//! Asking a PostgreSQL server what it contains.
//!
//! Everything here is a read of the system catalogues over the ordinary query
//! protocol. Four queries answer for a whole database — tables, columns,
//! constraints, unique indexes — rather than one per table, because a schema
//! with a few hundred tables would otherwise spend the first part of a
//! migration on round trips.
//!
//! `information_schema` is deliberately not used. It is defined in terms of the
//! standard, so it hides everything PostgreSQL added: array element types,
//! domains' base types, `typcategory`, identity columns and partitioning are
//! all either absent or reshaped. `pg_catalog` says what is actually there.

use super::pg_client::PgConnection;
use super::types::SourceColumn;

/// A table to be imported.
#[derive(Debug, Clone)]
pub struct SourceTable {
    pub schema: String,
    pub name: String,
    pub oid: u32,
    pub columns: Vec<SourceColumn>,
    /// The primary key's columns, in key order.
    pub primary_key: Vec<String>,
    pub unique_keys: Vec<UniqueKey>,
    pub foreign_keys: Vec<ForeignKey>,
    /// The planner's row estimate, when it has one. `None` on a table that has
    /// never been analysed. Used for progress only — the import reports what it
    /// actually copied.
    pub estimated_rows: Option<i64>,
    /// Anything about this table that the import could not carry over exactly.
    pub notes: Vec<String>,
}

impl SourceTable {
    /// `"schema"."table"`, for a statement sent to PostgreSQL.
    pub fn qualified(&self) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
    }
}

#[derive(Debug, Clone)]
pub struct UniqueKey {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    /// `a` no action, `r` restrict, `c` cascade, `n` set null, `d` set default.
    pub on_delete: char,
    pub on_update: char,
    /// `pg_constraint.convalidated`: false for a key added `NOT VALID` and
    /// never validated, whose rows PostgreSQL itself never checked.
    pub validated: bool,
}

/// What one schema of a PostgreSQL database holds.
#[derive(Debug, Default)]
pub struct SourceSchema {
    pub tables: Vec<SourceTable>,
    /// Things found in the source that no table carries: views, sequences,
    /// index kinds with no counterpart.
    pub notes: Vec<String>,
}

/// Reads a schema.
///
/// `include` and `exclude`, when non-empty, name tables — unqualified, as they
/// appear in the schema.
/// Asks each `json`/`jsonb` column whether every document it holds is an
/// object.
///
/// That is the only shape this engine's native JSON column can take, and the
/// answer decides whether the column is carried over as JSONB — which an
/// application can read as JSON — or as TEXT, which it cannot. One cheap query
/// per JSON column, and only where the source has any.
///
/// A column that cannot be probed stays `None`, which the planner reads as
/// TEXT: the mapping that cannot be wrong.
async fn probe_json_shapes(
    connection: &mut PgConnection,
    out: &mut SourceSchema,
    schema: &str,
) -> Result<(), String> {
    for table in out.tables.iter_mut() {
        for column in table.columns.iter_mut() {
            if !matches!(column.type_name.as_str(), "json" | "jsonb") {
                continue;
            }
            let sql = format!(
                "SELECT count(*) AS n FROM {}.{} WHERE {} IS NOT NULL \
                 AND {}_typeof({}::{}) NOT IN ('object', 'array')",
                quote_ident(schema),
                quote_ident(&table.name),
                quote_ident(&column.name),
                column.type_name,
                quote_ident(&column.name),
                column.type_name,
            );
            column.json_shreddable = match connection.simple_query(&sql).await {
                Ok(result) => result
                    .rows
                    .first()
                    .and_then(|row| result.get(row, "n"))
                    .and_then(|n| n.trim().parse::<i64>().ok())
                    .map(|n| n == 0),
                Err(e) => {
                    log::warn!(
                        "could not tell what shapes {}.{} holds, so it is carried over as \
                         TEXT: {}",
                        table.name,
                        column.name,
                        e
                    );
                    None
                }
            };
        }
    }
    Ok(())
}

pub async fn read_schema(
    connection: &mut PgConnection,
    schema: &str,
    include: &[String],
    exclude: &[String],
) -> Result<SourceSchema, String> {
    let mut out = SourceSchema::default();

    let mut tables = read_tables(connection, schema).await?;

    if !include.is_empty() {
        let wanted: Vec<String> = include.iter().map(|t| t.to_ascii_lowercase()).collect();
        let missing: Vec<&String> = include
            .iter()
            .filter(|want| {
                !tables
                    .iter()
                    .any(|t| t.name.eq_ignore_ascii_case(want.as_str()))
            })
            .collect();
        if !missing.is_empty() {
            // Named a table that is not there: almost always a typo, and
            // importing everything *except* it in silence is the wrong answer.
            return Err(format!(
                "no table called {} in schema {:?}",
                missing
                    .iter()
                    .map(|m| format!("{:?}", m))
                    .collect::<Vec<_>>()
                    .join(" or "),
                schema
            ));
        }
        tables.retain(|t| wanted.contains(&t.name.to_ascii_lowercase()));
    }

    if !exclude.is_empty() {
        let unwanted: Vec<String> = exclude.iter().map(|t| t.to_ascii_lowercase()).collect();
        tables.retain(|t| !unwanted.contains(&t.name.to_ascii_lowercase()));
    }

    if tables.is_empty() {
        return Err(format!("schema {:?} holds no tables to import", schema));
    }

    read_columns(connection, schema, &mut tables).await?;
    read_constraints(connection, schema, &mut tables).await?;
    read_unique_indexes(connection, schema, &mut tables).await?;
    note_other_objects(connection, schema, &mut out).await?;

    out.tables = tables;
    probe_json_shapes(connection, &mut out, schema).await?;
    Ok(out)
}

async fn read_tables(
    connection: &mut PgConnection,
    schema: &str,
) -> Result<Vec<SourceTable>, String> {
    // `relispartition` is excluded and `p` included, so a partitioned table is
    // read once through its parent rather than once per partition *and* again
    // through the parent — which would have doubled every row.
    let sql = format!(
        "SELECT c.oid::text AS oid, c.relname AS name, c.reltuples::bigint::text AS rows \
         FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = {} \
           AND c.relkind IN ('r', 'p') \
           AND NOT c.relispartition \
         ORDER BY c.relname",
        quote_literal(schema)
    );

    let result = connection.simple_query(&sql).await?;
    let mut tables = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let oid: u32 = result
            .get(row, "oid")
            .unwrap_or("0")
            .parse()
            .map_err(|_| "a relation oid was not a number".to_string())?;
        // `reltuples` is -1 on a table that has never been analysed, which is
        // not the same as an empty one — and reporting "0 rows" for a table
        // about to yield a million is a report nobody would trust twice.
        let estimated: Option<i64> = result
            .get(row, "rows")
            .and_then(|text| text.parse::<i64>().ok())
            .filter(|n| *n >= 0);
        tables.push(SourceTable {
            schema: schema.to_string(),
            name: result.get(row, "name").unwrap_or_default().to_string(),
            oid,
            columns: Vec::new(),
            primary_key: Vec::new(),
            unique_keys: Vec::new(),
            foreign_keys: Vec::new(),
            estimated_rows: estimated,
            notes: Vec::new(),
        });
    }
    Ok(tables)
}

async fn read_columns(
    connection: &mut PgConnection,
    schema: &str,
    tables: &mut [SourceTable],
) -> Result<(), String> {
    // The recursive term walks a domain down to whatever it is ultimately built
    // on, however many domains deep that is. Without it a column of a domain
    // over `numeric` would be reported as the domain's own name and fall
    // through to the unrecognised-type case, becoming `TEXT` — the values would
    // survive, but a number would have stopped being a number.
    let sql = format!(
        "WITH RECURSIVE resolved AS ( \
             SELECT t.oid AS start_oid, t.oid, t.typname, t.typtype, t.typcategory, \
                    t.typelem, t.typbasetype, 0 AS depth \
             FROM pg_type t \
             UNION ALL \
             SELECT r.start_oid, b.oid, b.typname, b.typtype, b.typcategory, \
                    b.typelem, b.typbasetype, r.depth + 1 \
             FROM resolved r \
             JOIN pg_type b ON b.oid = r.typbasetype \
             WHERE r.typtype = 'd' AND r.depth < 16 \
         ), base AS ( \
             SELECT DISTINCT ON (start_oid) * FROM resolved ORDER BY start_oid, depth DESC \
         ) \
         SELECT a.attrelid::text AS relid, \
                a.attname AS name, \
                a.attnum::text AS attnum, \
                a.attnotnull::text AS notnull, \
                a.atttypmod::text AS typmod, \
                base.typname AS typname, \
                base.typtype::text AS typtype, \
                base.typcategory::text AS typcategory, \
                elem.typname AS elemname, \
                format_type(a.atttypid, a.atttypmod) AS formatted, \
                pg_get_expr(d.adbin, d.adrelid) AS default_expr, \
                a.attidentity::text AS identity, \
                a.attgenerated::text AS generated \
         FROM pg_attribute a \
         JOIN pg_class c ON c.oid = a.attrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         JOIN base ON base.start_oid = a.atttypid \
         LEFT JOIN pg_type elem ON elem.oid = base.typelem \
         LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
         WHERE n.nspname = {} \
           AND c.relkind IN ('r', 'p') \
           AND a.attnum > 0 \
           AND NOT a.attisdropped \
         ORDER BY a.attrelid, a.attnum",
        quote_literal(schema)
    );

    let result = connection.simple_query(&sql).await?;

    for row in &result.rows {
        let relid: u32 = result.get(row, "relid").unwrap_or("0").parse().unwrap_or(0);
        let Some(table) = tables.iter_mut().find(|t| t.oid == relid) else {
            continue;
        };

        let name = result.get(row, "name").unwrap_or_default().to_string();
        let formatted = result.get(row, "formatted").unwrap_or_default().to_string();

        let identity = result.get(row, "identity").unwrap_or("").to_string();
        let generated = result.get(row, "generated").unwrap_or("").to_string();
        let mut default = result.get(row, "default_expr").map(|d| d.to_string());

        // A sequence default cannot be reproduced: this engine has no
        // sequences, and carrying the text across would leave a column whose
        // default is a call to something that does not exist. The *values*
        // still come over — they are ordinary data in the table.
        if default
            .as_deref()
            .is_some_and(|d| d.contains("nextval(") || !identity.is_empty())
        {
            table.notes.push(format!(
                "{}.{} is generated by a sequence; its values are imported but the column will \
                 not allocate new ones",
                table.name, name
            ));
            default = None;
        }
        if !generated.is_empty() {
            table.notes.push(format!(
                "{}.{} is a generated column; its stored values are imported as ordinary data",
                table.name, name
            ));
            default = None;
        }

        table.columns.push(SourceColumn {
            name,
            attnum: result
                .get(row, "attnum")
                .unwrap_or("0")
                .parse()
                .unwrap_or(0),
            type_name: result.get(row, "typname").unwrap_or("unknown").to_string(),
            type_kind: first_char(result.get(row, "typtype"), 'b'),
            type_category: first_char(result.get(row, "typcategory"), 'X'),
            element_type_name: result.get(row, "elemname").map(|e| e.to_string()),
            type_modifier: result
                .get(row, "typmod")
                .unwrap_or("-1")
                .parse()
                .unwrap_or(-1),
            not_null: is_true(result.get(row, "notnull")),
            default,
            formatted_type: formatted,
            // Answered by `probe_json_shapes` once the whole schema is read.
            json_shreddable: None,
        });
    }

    Ok(())
}

async fn read_constraints(
    connection: &mut PgConnection,
    schema: &str,
    tables: &mut [SourceTable],
) -> Result<(), String> {
    let sql = format!(
        "SELECT con.conrelid::text AS relid, \
                con.conname AS name, \
                con.contype::text AS kind, \
                con.conkey::text AS cols, \
                con.confkey::text AS refcols, \
                fr.relname AS reftable, \
                fn.nspname AS refschema, \
                con.confupdtype::text AS onupdate, \
                con.confdeltype::text AS ondelete, \
                con.convalidated::text AS validated \
         FROM pg_constraint con \
         JOIN pg_class rel ON rel.oid = con.conrelid \
         JOIN pg_namespace n ON n.oid = rel.relnamespace \
         LEFT JOIN pg_class fr ON fr.oid = con.confrelid \
         LEFT JOIN pg_namespace fn ON fn.oid = fr.relnamespace \
         WHERE n.nspname = {} AND con.contype IN ('p', 'u', 'f') \
         ORDER BY con.conname",
        quote_literal(schema)
    );

    let result = connection.simple_query(&sql).await?;

    for row in &result.rows {
        let relid: u32 = result.get(row, "relid").unwrap_or("0").parse().unwrap_or(0);
        let kind = first_char(result.get(row, "kind"), ' ');
        let name = result.get(row, "name").unwrap_or_default().to_string();

        // Resolved before the table is borrowed mutably, and against the
        // columns already read: `conkey` holds attribute *numbers*, and a
        // dropped column leaves a gap, so treating them as positions would
        // shift every key along by one.
        let columns = columns_by_number(tables, relid, result.get(row, "cols").unwrap_or("{}"));
        if columns.is_empty() {
            continue;
        }

        let referenced_table = result.get(row, "reftable").unwrap_or_default().to_string();
        let referenced_schema = result.get(row, "refschema").unwrap_or_default().to_string();
        let referenced_relid = tables
            .iter()
            .find(|t| t.name == referenced_table)
            .map(|t| t.oid)
            .unwrap_or(0);
        let referenced_columns = columns_by_number(
            tables,
            referenced_relid,
            result.get(row, "refcols").unwrap_or("{}"),
        );

        let Some(table) = tables.iter_mut().find(|t| t.oid == relid) else {
            continue;
        };

        match kind {
            'p' => table.primary_key = columns,
            'u' => table.unique_keys.push(UniqueKey { name, columns }),
            'f' => {
                if referenced_schema != schema {
                    // The target holds one schema's worth of tables, so a key
                    // pointing outside it has nothing to point at. Said rather
                    // than dropped in silence: the import is then known to
                    // enforce less than the source did.
                    table.notes.push(format!(
                        "foreign key {} references {}.{}, outside the schema being imported, and \
                         was not carried over",
                        name, referenced_schema, referenced_table
                    ));
                    continue;
                }
                if referenced_columns.is_empty() {
                    table.notes.push(format!(
                        "foreign key {} references a table that is not being imported, and was \
                         not carried over",
                        name
                    ));
                    continue;
                }
                table.foreign_keys.push(ForeignKey {
                    name,
                    columns,
                    referenced_table,
                    referenced_columns,
                    on_delete: first_char(result.get(row, "ondelete"), 'a'),
                    on_update: first_char(result.get(row, "onupdate"), 'a'),
                    // `true`/`t` as text; anything unreadable is taken as
                    // validated, which only means the target checks the rows.
                    validated: !matches!(result.get(row, "validated"), Some("false") | Some("f")),
                });
            }
            _ => {}
        }
    }

    Ok(())
}

/// The names of the columns a `{1,3}` attribute-number list refers to.
///
/// Empty when the relation is not one being imported, or when any number does
/// not resolve — a partly-resolved key would name the wrong columns, which is
/// worse than no key at all.
fn columns_by_number(tables: &[SourceTable], relid: u32, key: &str) -> Vec<String> {
    let Some(table) = tables.iter().find(|t| t.oid == relid) else {
        return Vec::new();
    };
    let numbers = parse_int_array(key);
    let mut out = Vec::with_capacity(numbers.len());
    for number in numbers {
        match table.columns.iter().find(|c| i64::from(c.attnum) == number) {
            Some(column) => out.push(column.name.clone()),
            None => return Vec::new(),
        }
    }
    out
}

async fn read_unique_indexes(
    connection: &mut PgConnection,
    schema: &str,
    tables: &mut [SourceTable],
) -> Result<(), String> {
    // Unique *indexes* that no constraint backs. Every ORM makes these —
    // `CREATE UNIQUE INDEX` rather than `ADD CONSTRAINT` — and leaving them out
    // would import a database that accepts duplicates the source refused.
    let sql = format!(
        "SELECT i.indrelid::text AS relid, \
                ix.relname AS name, \
                i.indkey::text AS cols, \
                (i.indpred IS NOT NULL)::text AS partial, \
                (i.indexprs IS NOT NULL)::text AS expression \
         FROM pg_index i \
         JOIN pg_class rel ON rel.oid = i.indrelid \
         JOIN pg_class ix ON ix.oid = i.indexrelid \
         JOIN pg_namespace n ON n.oid = rel.relnamespace \
         WHERE n.nspname = {} \
           AND i.indisunique \
           AND NOT i.indisprimary \
           AND NOT EXISTS ( \
                 SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid \
           ) \
         ORDER BY ix.relname",
        quote_literal(schema)
    );

    let result = connection.simple_query(&sql).await?;

    for row in &result.rows {
        let relid: u32 = result.get(row, "relid").unwrap_or("0").parse().unwrap_or(0);
        let name = result.get(row, "name").unwrap_or_default().to_string();

        // `indkey` is space-separated attribute numbers, not an array literal,
        // and a `0` in it stands for an expression rather than a column.
        let key = result.get(row, "cols").unwrap_or("").to_string();
        let numbers: Vec<i64> = key
            .split_whitespace()
            .map(|t| t.parse().unwrap_or(0))
            .collect();
        let partial = is_true(result.get(row, "partial"));
        let expression = is_true(result.get(row, "expression")) || numbers.iter().any(|n| *n <= 0);

        let Some(table) = tables.iter_mut().find(|t| t.oid == relid) else {
            continue;
        };

        // A partial or expression index constrains something other than "these
        // columns are unique together", which is the only shape this engine
        // has. Enforcing the nearest thing to it would be enforcing a different
        // constraint from the source's, so it is reported instead.
        if partial || expression {
            table.notes.push(format!(
                "unique index {} is partial or over an expression and was not carried over",
                name
            ));
            continue;
        }

        let mut columns = Vec::with_capacity(numbers.len());
        for number in &numbers {
            match table
                .columns
                .iter()
                .find(|c| i64::from(c.attnum) == *number)
            {
                Some(column) => columns.push(column.name.clone()),
                None => {
                    columns.clear();
                    break;
                }
            }
        }

        if columns.is_empty() {
            table.notes.push(format!(
                "unique index {} could not be read as a list of columns and was not carried over",
                name
            ));
            continue;
        }

        // The same columns may already be spoken for by the primary key or by
        // a constraint; declaring it twice would make the target refuse its own
        // schema.
        if table.primary_key == columns || table.unique_keys.iter().any(|k| k.columns == columns) {
            continue;
        }
        table.unique_keys.push(UniqueKey { name, columns });
    }

    Ok(())
}

/// Records what is in the schema that a table import does not carry.
async fn note_other_objects(
    connection: &mut PgConnection,
    schema: &str,
    out: &mut SourceSchema,
) -> Result<(), String> {
    let sql = format!(
        "SELECT c.relkind::text AS kind, count(*)::text AS n \
         FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = {} AND c.relkind IN ('v', 'm', 'S', 'f') \
         GROUP BY c.relkind",
        quote_literal(schema)
    );

    let result = connection.simple_query(&sql).await?;
    for row in &result.rows {
        let kind = first_char(result.get(row, "kind"), '?');
        let count = result.get(row, "n").unwrap_or("0");
        let what = match kind {
            'v' => "view",
            'm' => "materialised view",
            'S' => "sequence",
            'f' => "foreign table",
            _ => continue,
        };
        out.notes.push(if count == "1" {
            format!("1 {} in the source was not imported", what)
        } else {
            format!("{} {}s in the source were not imported", count, what)
        });
    }

    Ok(())
}

/// Parses `{1,3,7}` into its numbers.
fn parse_int_array(text: &str) -> Vec<i64> {
    text.trim()
        .trim_start_matches('{')
        .trim_end_matches('}')
        .split(',')
        .filter(|part| !part.trim().is_empty())
        .filter_map(|part| part.trim().parse().ok())
        .collect()
}

/// Reads a boolean out of a catalogue answer.
///
/// PostgreSQL renders a `boolean` as `t`/`f`, but `boolean::text` — which these
/// queries use, so that every column comes back as text — renders it as
/// `true`/`false`. Testing only for `t` therefore read every one of them as
/// false, which quietly dropped `NOT NULL` from every column and let a partial
/// unique index through as an ordinary one.
fn is_true(text: Option<&str>) -> bool {
    matches!(
        text,
        Some("t") | Some("true") | Some("TRUE") | Some("yes") | Some("1")
    )
}

fn first_char(text: Option<&str>, fallback: char) -> char {
    text.and_then(|t| t.chars().next()).unwrap_or(fallback)
}

/// Quotes a string for a statement sent to PostgreSQL.
pub fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Quotes an identifier for a statement sent to PostgreSQL.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_arrays_are_read() {
        assert_eq!(parse_int_array("{1,3,7}"), vec![1, 3, 7]);
        assert_eq!(parse_int_array("{}"), Vec::<i64>::new());
        assert_eq!(parse_int_array("{2}"), vec![2]);
    }

    /// The catalogue queries cast every column to text so one code path reads
    /// them all — and `boolean::text` is `true`/`false`, not the `t`/`f` a bare
    /// boolean column renders as. Reading only `t` silently dropped `NOT NULL`
    /// from every imported column and let a partial unique index through as an
    /// ordinary one, neither of which fails anything at the time.
    #[test]
    fn both_spellings_of_a_catalogue_boolean_are_read() {
        assert!(is_true(Some("t")));
        assert!(is_true(Some("true")));
        assert!(!is_true(Some("f")));
        assert!(!is_true(Some("false")));
        assert!(!is_true(None));
    }

    #[test]
    fn quoting_closes_the_obvious_holes() {
        assert_eq!(quote_literal("public"), "'public'");
        assert_eq!(quote_literal("it's"), "'it''s'");
        assert_eq!(quote_literal("' OR 1=1 --"), "''' OR 1=1 --'");
        assert_eq!(quote_ident("users"), "\"users\"");
        assert_eq!(quote_ident("od\"d"), "\"od\"\"d\"");
    }
}
