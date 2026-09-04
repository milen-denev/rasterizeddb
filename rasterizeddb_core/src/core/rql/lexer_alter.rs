//! `ALTER TABLE`, `DROP TABLE`, and the DDL the engine deliberately accepts
//! without acting on.
//!
//! Migrations are the first thing a real application runs, and they are mostly
//! DDL. A statement the engine cannot honour has exactly two acceptable
//! answers: do it, or say why not. Silently accepting one that changes nothing
//! is the failure mode to avoid — except where the engine's design makes the
//! statement genuinely unnecessary, which is the case for index DDL, and is
//! recorded here rather than left implicit.

use crate::core::db_type::DbType;

/// What an `ALTER TABLE` statement asks for.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterAction {
    AddColumn {
        name: String,
        db_type: DbType,
        nullable: bool,
        if_not_exists: bool,
        /// The `DEFAULT` expression, as written.
        default: Option<String>,
        /// Declared `JSON`/`JSONB`, so it is stored as a `UUID` pointing at a
        /// table of its own — the same mapping `CREATE TABLE` applies.
        ///
        /// `CREATE TABLE t (payload JSONB)` has worked since the JSON layer
        /// landed; `ALTER TABLE t ADD COLUMN payload JSONB` refused with "no
        /// JSON type", which was both wrong and inconsistent with the statement
        /// beside it.
        json: bool,
    },
    DropColumn {
        name: String,
        if_exists: bool,
    },
    RenameColumn {
        from: String,
        to: String,
    },
    /// A table-level constraint clause, as written.
    ///
    /// The executor decides what to do with it: `FOREIGN KEY` and `UNIQUE` are
    /// stored and enforced, while the rest — `CHECK`, `EXCLUDE` — are accepted
    /// so a migration can proceed and reported as unenforced rather than as
    /// done.
    ConstraintClause {
        /// Whether the statement drops the constraint rather than adding one.
        ///
        /// Both spellings lose their leading keyword here, so
        /// `ADD CONSTRAINT c UNIQUE (a)` and `DROP CONSTRAINT c` would
        /// otherwise arrive as the same `description` — and an `ADD` would be
        /// read as a request to remove the constraint it names.
        drop: bool,
        description: String,
    },
    /// `ALTER TABLE … RENAME TO`, which renames the table itself.
    ///
    /// Distinguished from `RENAME COLUMN` by what follows `RENAME`: a bare
    /// `TO` renames the relation, anything else names a column. Getting this
    /// wrong is not a parse detail — `ALTER TABLE role RENAME TO roles` was
    /// reported as "RENAME COLUMN must be followed by TO", which is both a
    /// refusal of valid DDL and a misleading message about a statement that
    /// mentions no column.
    RenameTable {
        to: String,
    },
    /// `ALTER COLUMN`, which changes a column in place.
    AlterColumn {
        name: String,
        detail: String,
    },
    /// `ALTER COLUMN … SET DEFAULT`, or `DROP DEFAULT` when the expression is
    /// `None`.
    SetDefault {
        name: String,
        expression: Option<String>,
    },
    /// Accepted and not acted on, with the reason.
    ///
    /// PostgreSQL storage tuning — `SET COMPRESSION`, `SET (autovacuum_…)`,
    /// `VALIDATE CONSTRAINT` — describes machinery this engine does not have.
    /// Failing them would stop a migration over a statement whose whole effect
    /// is advisory, and a real schema is full of them: 12 sites in the
    /// application this compatibility work is modelled on.
    NoOp {
        reason: &'static str,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTable {
    pub table_name: String,
    pub action: AlterAction,
}

/// Strips quoting and any schema qualification from a relation or column name.
fn bare_name(raw: &str) -> String {
    raw.trim()
        .trim_end_matches(&[',', ';'][..])
        .split('.')
        .next_back()
        .unwrap_or(raw)
        .trim_matches('"')
        .to_string()
}

/// Parses `ALTER TABLE …`.
/// The next whitespace-delimited token at or after `from`, with the offset
/// just past it. Offsets are kept because the action list has to be split on
/// the original text: joining whitespace-separated tokens back together loses
/// where one action ends and the next begins.
fn token_at(s: &str, from: usize) -> Option<(&str, usize)> {
    let start = s[from..].find(|c: char| !c.is_whitespace())? + from;
    let end = s[start..]
        .find(char::is_whitespace)
        .map(|i| i + start)
        .unwrap_or(s.len());
    Some((&s[start..end], end))
}

/// Parses an `ALTER TABLE` into one `AlterTable` per action.
///
/// PostgreSQL allows a comma-separated action list in a single statement, and
/// sea-orm emits one: a single `alter_table` carrying two `add_foreign_key`
/// calls becomes `ALTER TABLE t ADD CONSTRAINT …, ADD CONSTRAINT …`. Reading
/// only the first action would apply half the statement and report success,
/// so a list that cannot be fully accounted for is refused instead — the same
/// rule the `VALUES` clause follows.
///
/// The split is on top-level commas only: a comma inside `REFERENCES t (a, b)`
/// or inside a quoted identifier or default expression is data, not a
/// separator, which is why `split_definitions` does the work rather than
/// `split(',')`.
pub fn parse_alter_table_actions(sql: &str) -> Result<Vec<AlterTable>, String> {
    let trimmed = sql.trim().trim_end_matches(';');
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("ALTER TABLE") {
        return Err("not an ALTER TABLE statement".to_string());
    }

    // Walk `ALTER TABLE [IF EXISTS] [ONLY] name`, keeping the offset, so the
    // remainder is the action list as written.
    let no_relation = || "ALTER TABLE names no relation".to_string();
    let (_, pos) = token_at(trimmed, 0).ok_or_else(no_relation)?;
    let (_, pos) = token_at(trimmed, pos).ok_or_else(no_relation)?;
    let (mut table_token, mut pos) = token_at(trimmed, pos).ok_or_else(no_relation)?;
    if table_token.eq_ignore_ascii_case("IF") {
        let (_, after_exists) = token_at(trimmed, pos).ok_or_else(no_relation)?;
        let (name, after_name) = token_at(trimmed, after_exists).ok_or_else(no_relation)?;
        table_token = name;
        pos = after_name;
    }
    if table_token.eq_ignore_ascii_case("ONLY") {
        let (name, after_name) = token_at(trimmed, pos).ok_or_else(no_relation)?;
        table_token = name;
        pos = after_name;
    }

    let rest = &trimmed[pos..];
    // `split_definitions` drops a trailing empty segment, so a dangling comma
    // would otherwise pass as a well-formed list one action shorter.
    if rest.trim_end().ends_with(',') {
        return Err(
            "the ALTER TABLE action list could not be fully parsed (trailing comma); \
             refusing rather than applying the actions that were understood"
                .to_string(),
        );
    }

    let segments = crate::core::rql::lexer_ct::split_definitions(rest);
    if segments.len() <= 1 {
        return parse_alter_table(sql).map(|action| vec![action]);
    }

    let mut actions = Vec::with_capacity(segments.len());
    for segment in &segments {
        let segment = segment.trim();
        if segment.is_empty() {
            return Err(
                "the ALTER TABLE action list could not be fully parsed (empty action); \
                 refusing rather than applying the actions that were understood"
                    .to_string(),
            );
        }
        actions.push(parse_alter_table(&format!(
            "ALTER TABLE {} {}",
            table_token, segment
        ))?);
    }
    Ok(actions)
}

pub fn parse_alter_table(sql: &str) -> Result<AlterTable, String> {
    let sql = sql.trim().trim_end_matches(';');
    let upper = sql.to_ascii_uppercase();

    if !upper.starts_with("ALTER TABLE") {
        return Err("not an ALTER TABLE statement".to_string());
    }

    let mut tokens = sql[..].split_whitespace().skip(2).peekable();

    // `ALTER TABLE [IF EXISTS] [ONLY] name`
    let mut table_token = tokens.next().ok_or("ALTER TABLE names no relation")?;
    if table_token.eq_ignore_ascii_case("IF") {
        // IF EXISTS
        tokens.next();
        table_token = tokens.next().ok_or("ALTER TABLE names no relation")?;
    }
    if table_token.eq_ignore_ascii_case("ONLY") {
        table_token = tokens.next().ok_or("ALTER TABLE names no relation")?;
    }
    let table_name = bare_name(table_token);

    // The DEFAULT clause is removed before the action is read, so its
    // expression cannot be mistaken for a type or a constraint.
    let remainder: String = tokens.collect::<Vec<_>>().join(" ");
    let (remainder, default) = crate::core::rql::column_default::split_clause(&remainder);
    let rest: Vec<&str> = remainder.split_whitespace().collect();
    if rest.is_empty() {
        return Err("ALTER TABLE has no action".to_string());
    }
    let rest_upper: Vec<String> = rest.iter().map(|t| t.to_ascii_uppercase()).collect();

    let action = match rest_upper[0].as_str() {
        "ADD" => {
            let mut i = 1usize;
            if rest_upper.get(i).map(String::as_str) == Some("COLUMN") {
                i += 1;
            }

            // `ADD CONSTRAINT …`, `ADD FOREIGN KEY …`, `ADD PRIMARY KEY …`,
            // `ADD UNIQUE …` all declare constraints rather than columns.
            if matches!(
                rest_upper.get(i).map(String::as_str),
                Some("CONSTRAINT")
                    | Some("FOREIGN")
                    | Some("PRIMARY")
                    | Some("UNIQUE")
                    | Some("CHECK")
                    | Some("EXCLUDE")
            ) {
                return Ok(AlterTable {
                    table_name,
                    action: AlterAction::ConstraintClause {
                        drop: false,
                        description: rest[i..].join(" "),
                    },
                });
            }

            let mut if_not_exists = false;
            if rest_upper.get(i).map(String::as_str) == Some("IF")
                && rest_upper.get(i + 1).map(String::as_str) == Some("NOT")
                && rest_upper.get(i + 2).map(String::as_str) == Some("EXISTS")
            {
                if_not_exists = true;
                i += 3;
            }

            let name = bare_name(rest.get(i).ok_or("ADD COLUMN names no column")?);
            i += 1;

            let type_tokens: Vec<&str> = rest[i..].to_vec();
            // A JSON column is a `UUID` on this side of the mapping; the
            // shadow table is registered by the executor, as it is for CREATE.
            let json = type_tokens
                .first()
                .map(|t| {
                    let head = t.split('(').next().unwrap_or(t).to_ascii_uppercase();
                    head == "JSON" || head == "JSONB"
                })
                .unwrap_or(false);
            let (db_type, used) = if json {
                (DbType::UUID, 1)
            } else {
                parse_column_type(&type_tokens)?
            };
            let constraints = rest_upper[i + used..].join(" ");
            let nullable =
                !(constraints.contains("NOT NULL") || constraints.contains("PRIMARY KEY"));

            AlterAction::AddColumn {
                name,
                db_type,
                nullable,
                if_not_exists,
                default,
                json,
            }
        }
        "DROP" => {
            let mut i = 1usize;
            if rest_upper.get(i).map(String::as_str) == Some("COLUMN") {
                i += 1;
            }
            if matches!(rest_upper.get(i).map(String::as_str), Some("CONSTRAINT")) {
                return Ok(AlterTable {
                    table_name,
                    action: AlterAction::ConstraintClause {
                        drop: true,
                        description: rest[i..].join(" "),
                    },
                });
            }

            let mut if_exists = false;
            if rest_upper.get(i).map(String::as_str) == Some("IF")
                && rest_upper.get(i + 1).map(String::as_str) == Some("EXISTS")
            {
                if_exists = true;
                i += 2;
            }

            let name = bare_name(rest.get(i).ok_or("DROP COLUMN names no column")?);
            AlterAction::DropColumn { name, if_exists }
        }
        "RENAME" => {
            let mut i = 1usize;
            // `RENAME TO x` renames the table; `RENAME [COLUMN] a TO b` renames
            // a column. The keyword right after RENAME is what tells them apart.
            if rest_upper.get(i).map(String::as_str) == Some("TO") {
                let to = bare_name(rest.get(i + 1).ok_or("RENAME TO has no new name")?);
                return Ok(AlterTable {
                    table_name,
                    action: AlterAction::RenameTable { to },
                });
            }
            if rest_upper.get(i).map(String::as_str) == Some("COLUMN") {
                i += 1;
            }
            let from = bare_name(rest.get(i).ok_or("RENAME names no column")?);
            if rest_upper.get(i + 1).map(String::as_str) != Some("TO") {
                return Err("RENAME COLUMN must be followed by TO".to_string());
            }
            let to = bare_name(rest.get(i + 2).ok_or("RENAME has no new name")?);
            AlterAction::RenameColumn { from, to }
        }
        "ALTER" => {
            let mut i = 1usize;
            if rest_upper.get(i).map(String::as_str) == Some("COLUMN") {
                i += 1;
            }
            let name = bare_name(rest.get(i).ok_or("ALTER COLUMN names no column")?);
            let detail = rest[i + 1..].join(" ");
            let detail_upper = detail.to_ascii_uppercase();

            // `SET DEFAULT` arrives with the clause already split off, leaving
            // a bare `SET`; `DROP DEFAULT` keeps its keyword.
            if default.is_some() || detail_upper.starts_with("SET") {
                return Ok(AlterTable {
                    table_name,
                    action: AlterAction::SetDefault {
                        name,
                        expression: default,
                    },
                });
            }
            if detail_upper.starts_with("DROP DEFAULT") {
                return Ok(AlterTable {
                    table_name,
                    action: AlterAction::SetDefault {
                        name,
                        expression: None,
                    },
                });
            }

            AlterAction::AlterColumn { name, detail }
        }
        // Storage tuning and constraint validation: accepted, not acted on.
        // `SET` here is the *table-level* form — `ALTER COLUMN … SET DEFAULT`
        // was matched earlier and still does what it says.
        "SET" => AlterAction::NoOp {
            reason: "table storage options: RasterizedDB manages its own storage",
        },
        "RESET" => AlterAction::NoOp {
            reason: "table storage options: RasterizedDB manages its own storage",
        },
        "VALIDATE" => AlterAction::NoOp {
            reason: "constraints are validated when they are added",
        },
        "CLUSTER" | "OWNER" | "ENABLE" | "DISABLE" | "INHERIT" | "NO" => AlterAction::NoOp {
            reason: "PostgreSQL table machinery RasterizedDB does not have",
        },
        other => return Err(format!("ALTER TABLE ... {} is not supported yet", other)),
    };

    Ok(AlterTable { table_name, action })
}

/// Maps a SQL type phrase to a storage type, returning how many tokens it used.
pub fn parse_column_type(tokens: &[&str]) -> Result<(DbType, usize), String> {
    if tokens.is_empty() {
        return Err("column has no type".to_string());
    }

    let norm = |t: &str| {
        t.split(['(', '['])
            .next()
            .unwrap_or(t)
            .trim_matches(',')
            .to_ascii_uppercase()
    };

    // An array is written as brackets after the element type — `TEXT[]`,
    // `REAL[]`, `DOUBLE PRECISION[][]` — and the brackets may sit on any word
    // of a multi-word type name. One storage type serves every element type and
    // every dimension count, so all that is needed here is to notice them.
    let is_array = |count: usize| {
        tokens[..count.min(tokens.len())]
            .iter()
            .any(|t| t.contains('['))
    };

    // Multi-word type names, longest first.
    let two = if tokens.len() >= 2 {
        format!("{} {}", norm(tokens[0]), norm(tokens[1]))
    } else {
        String::new()
    };
    let four = if tokens.len() >= 4 {
        format!(
            "{} {} {} {}",
            norm(tokens[0]),
            norm(tokens[1]),
            norm(tokens[2]),
            norm(tokens[3])
        )
    } else {
        String::new()
    };

    if four == "TIMESTAMP WITH TIME ZONE" || four == "TIMESTAMP WITHOUT TIME ZONE" {
        return Ok((
            if is_array(4) {
                DbType::ARRAY
            } else {
                DbType::DATETIME
            },
            4,
        ));
    }
    // `TIME WITH TIME ZONE` is `timetz` and `TIME WITHOUT TIME ZONE` is `time`.
    // They were both reaching the one-word arm as `TIME`, so a column declared
    // with an offset lost it.
    if four == "TIME WITH TIME ZONE" {
        return Ok((
            if is_array(4) {
                DbType::ARRAY
            } else {
                DbType::TIMETZ
            },
            4,
        ));
    }
    if four == "TIME WITHOUT TIME ZONE" {
        return Ok((
            if is_array(4) {
                DbType::ARRAY
            } else {
                DbType::TIME
            },
            4,
        ));
    }
    if two == "DOUBLE PRECISION" {
        return Ok((
            if is_array(2) {
                DbType::ARRAY
            } else {
                DbType::F64
            },
            2,
        ));
    }
    if two == "CHARACTER VARYING" {
        return Ok((
            if is_array(2) {
                DbType::ARRAY
            } else {
                DbType::STRING
            },
            2,
        ));
    }
    if two == "BIT VARYING" {
        return Ok((
            if is_array(2) {
                DbType::ARRAY
            } else {
                DbType::VARBIT
            },
            2,
        ));
    }

    let one = norm(tokens[0]);
    // `CHAR(n)` for n above one is `bpchar`, a *string* of that width. Mapping
    // it to the engine's `CHAR` — a single codepoint — stored only the first
    // character and reported success: `'ABCDE'` in a `CHAR(5)` column came back
    // as `'ABCD'`. Only a bare `CHAR`, which Postgres reads as `CHAR(1)`, is
    // the one-character type.
    let char_width = declared_width(tokens[0]);

    let db_type = match one.as_str() {
        // Postgres spellings first, then the engine's own aliases. Unsigned
        // types and the explicit bit widths are not Postgres-native; they are
        // kept because the engine has always accepted them.
        "BIGINT" | "INT8" | "INT64" | "BIGSERIAL" | "SERIAL8" => DbType::I64,
        "INT" | "INTEGER" | "INT4" | "INT32" | "SERIAL" | "SERIAL4" => DbType::I32,
        "SMALLINT" | "INT2" | "INT16" | "SMALLSERIAL" | "SERIAL2" => DbType::I16,
        "TINYINT" | "INT1" | "INT8S" => DbType::I8,

        "UBIGINT" | "UINT64" | "UINT8" => DbType::U64,
        "UINT" | "UINT32" | "UINT4" => DbType::U32,
        "USMALLINT" | "UINT16" | "UINT2" => DbType::U16,
        "UTINYINT" | "UINT1" => DbType::U8,

        "REAL" | "FLOAT4" => DbType::F32,
        // Postgres treats a bare FLOAT as double precision.
        "FLOAT8" | "FLOAT" | "DOUBLE" => DbType::F64,
        "TEXT" | "VARCHAR" | "STRING" | "NAME" | "CITEXT" => DbType::STRING,
        "CHARACTER" | "BPCHAR" => DbType::STRING,
        "CHAR" => match char_width {
            Some(n) if n > 1 => DbType::STRING,
            _ => DbType::CHAR,
        },
        "NUMERIC" | "DECIMAL" | "MONEY" => DbType::DECIMAL,
        "BOOL" | "BOOLEAN" => DbType::BOOL,
        "UUID" => DbType::UUID,
        "INET" | "CIDR" => DbType::INET,
        // `DATE`, `TIME` and `TIMETZ` were all `DATETIME`, so each advertised
        // `timestamptz` on the wire and a client asking to decode a date or a
        // time of day refused the column. They are their own types now, stored
        // in the layout Postgres sends them in.
        "TIMESTAMP" | "TIMESTAMPTZ" | "DATETIME" => DbType::DATETIME,
        "DATE" => DbType::DATE,
        "TIME" => DbType::TIME,
        "TIMETZ" => DbType::TIMETZ,
        "INTERVAL" => DbType::INTERVAL,
        "MACADDR" => DbType::MACADDR,
        "MACADDR8" => DbType::MACADDR8,
        "BIT" => DbType::BIT,
        "VARBIT" => DbType::VARBIT,
        "POINT" => DbType::POINT,
        "LINE" => DbType::LINE,
        "LSEG" => DbType::LSEG,
        "BOX" => DbType::BOX,
        "PATH" => DbType::PATH,
        "POLYGON" => DbType::POLYGON,
        "CIRCLE" => DbType::CIRCLE,
        "INT4RANGE" => DbType::INT4RANGE,
        "INT8RANGE" => DbType::INT8RANGE,
        "NUMRANGE" => DbType::NUMRANGE,
        "TSRANGE" => DbType::TSRANGE,
        "TSTZRANGE" => DbType::TSTZRANGE,
        "DATERANGE" => DbType::DATERANGE,
        "BYTEA" => DbType::BYTEA,
        "ARRAY" => DbType::ARRAY,
        // A stored document. When `json_shadow_tables` is on, `CREATE TABLE`
        // and `ADD COLUMN` intercept the declaration before it reaches here and
        // build the shredded form instead, so this arm is the stored one.
        "JSON" | "JSONB" => DbType::JSONB,
        other => return Err(format!("column type '{}' is not supported yet", other)),
    };

    // The brackets turn any of the above into an array of it.
    if is_array(1) && db_type != DbType::ARRAY {
        return Ok((DbType::ARRAY, 1));
    }

    Ok((db_type, 1))
}

/// The `n` of a `TYPE(n)` or `TYPE(n, m)` declaration.
///
/// Only the first number is read: the callers here want a width, and the two
/// numbers of a `NUMERIC(p, s)` are parsed by
/// [`crate::core::rql::type_modifier::parse_numeric_modifier`] instead.
pub fn declared_width(token: &str) -> Option<u32> {
    let open = token.find('(')?;
    let close = token[open..].find(')')? + open;
    token[open + 1..close]
        .split(',')
        .next()?
        .trim()
        .parse()
        .ok()
}

/// The relation a `DROP TABLE` names, or `None` when the statement is malformed.
pub fn parse_drop_table(sql: &str) -> Result<(String, bool), String> {
    let sql = sql.trim().trim_end_matches(';');
    let upper = sql.to_ascii_uppercase();
    if !upper.starts_with("DROP TABLE") {
        return Err("not a DROP TABLE statement".to_string());
    }

    let mut tokens = sql.split_whitespace().skip(2).peekable();
    let mut if_exists = false;
    let mut token = tokens.next().ok_or("DROP TABLE names no relation")?;
    if token.eq_ignore_ascii_case("IF") {
        tokens.next(); // EXISTS
        if_exists = true;
        token = tokens.next().ok_or("DROP TABLE names no relation")?;
    }

    if sql.matches(',').count() > 0 {
        return Err("dropping several tables in one statement is not supported yet".to_string());
    }

    Ok((bare_name(token), if_exists))
}

/// Rewrites `CREATE UNIQUE INDEX name ON t (cols)` as the equivalent
/// `ALTER TABLE t ADD CONSTRAINT name UNIQUE (cols)`.
///
/// A unique index is a **constraint**, not merely an access path, so treating
/// it like other index DDL silently dropped a guarantee the schema asked for —
/// a duplicate insert succeeded after one had been created. Expressing it as
/// the constraint the engine already enforces keeps one implementation.
///
/// `None` when the statement is not one, or is a **partial** unique index
/// (`… WHERE …`). Partial uniqueness cannot be expressed here, and enforcing it
/// in full would reject rows PostgreSQL accepts — a stronger constraint is as
/// wrong as a weaker one, so those stay a declared no-op.
pub fn unique_index_as_constraint(sql: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';');
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("CREATE UNIQUE INDEX") {
        return None;
    }
    // A partial index keeps the old behaviour.
    if crate::core::rql::join::find_keyword(trimmed, "WHERE", 0).is_some() {
        return None;
    }

    let after = &trimmed["CREATE UNIQUE INDEX".len()..];
    // `IF NOT EXISTS` is carried into the constraint rather than dropped.
    // Losing it made a re-run of a migration fail on the second pass with
    // "constraint already exists" — the exact outcome the clause is written to
    // prevent, and the reason a migration that seeds data can be re-applied.
    let (after, if_not_exists) = {
        let t = after.trim_start();
        let upper = t.to_ascii_uppercase();
        if upper.starts_with("IF NOT EXISTS") {
            (&t["IF NOT EXISTS".len()..], true)
        } else {
            (t, false)
        }
    };

    let on_at = crate::core::rql::join::find_keyword(after, "ON", 0)?;
    let name = after[..on_at].trim().trim_matches('"');
    if name.is_empty() {
        return None;
    }

    let rest = after[on_at + "ON".len()..].trim_start();
    let open = rest.find('(')?;
    let table = rest[..open].trim().trim_matches('"');
    if table.is_empty() {
        return None;
    }
    let close = rest.rfind(')')?;
    let columns = rest[open + 1..close].trim();
    if columns.is_empty() {
        return None;
    }

    Some(format!(
        "ALTER TABLE {} ADD CONSTRAINT {}{} UNIQUE ({})",
        table,
        if if_not_exists { "IF NOT EXISTS " } else { "" },
        name,
        columns
    ))
}

/// Statements the engine accepts without acting on, with the reason.
///
/// Returns `None` when the statement is not one of them.
pub fn no_op_reason(sql: &str) -> Option<&'static str> {
    let upper = sql.trim_start().to_ascii_uppercase();

    // A non-partial `CREATE UNIQUE INDEX` is a constraint and is rewritten into
    // one; only the rest of index DDL is a no-op.
    if unique_index_as_constraint(sql).is_some() {
        return None;
    }
    // An index over a document path is a real statement — see
    // `path_index::parse_create`. Left to the executor rather than swallowed
    // here, which is where every other index DDL still goes.
    if crate::core::rql::path_index::parse_create(sql).is_some()
        || crate::core::rql::path_index::parse_drop(sql).is_some()
    {
        return None;
    }
    if upper.starts_with("CREATE INDEX")
        || upper.starts_with("CREATE UNIQUE INDEX")
        || upper.starts_with("DROP INDEX")
    {
        // Not a silent no-op: the engine maintains its own bit-sliced filter
        // index and leaf structures over every eligible column, so a declared
        // index has nothing to add.
        return Some("index DDL: RasterizedDB maintains its own indexes");
    }
    if upper.starts_with("CREATE EXTENSION") || upper.starts_with("DROP EXTENSION") {
        return Some("extensions: RasterizedDB has no extension mechanism");
    }
    if upper.starts_with("CREATE SCHEMA") || upper.starts_with("DROP SCHEMA") {
        return Some("schemas: RasterizedDB has a single namespace");
    }
    if upper.starts_with("COMMENT ON") {
        return Some("comments are not stored");
    }
    if upper.starts_with("ANALYZE") {
        return Some("statistics are maintained automatically");
    }
    // `VACUUM` is deliberately absent. The engine implements it — reclamation
    // and, with `FULL`, compaction — as `QueryPurpose::Vacuum`. Listing it here
    // made the PostgreSQL listener answer `CommandComplete("VACUUM")` without
    // running anything, while the same statement over the native protocol did
    // the work: a no-op reported as success, differing by which wire it arrived
    // on. Nothing in this list may name a statement the engine implements; see
    // `no_op_reason_never_shadows_a_real_statement`.

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Anything answered here is answered *instead of* the engine, so an entry
    /// naming a statement the engine implements silently replaces real work
    /// with a success report. `VACUUM` did exactly that.
    #[test]
    fn no_op_reason_never_shadows_a_real_statement() {
        for sql in [
            "CREATE INDEX i ON t (a)",
            "DROP INDEX i",
            "CREATE EXTENSION pgcrypto",
            "DROP EXTENSION pgcrypto",
            "CREATE SCHEMA s",
            "DROP SCHEMA s",
            "COMMENT ON TABLE t IS 'x'",
            "ANALYZE",
            "ANALYZE t",
            "VACUUM",
            "VACUUM FULL",
            "VACUUM t",
            "VACUUM FULL t",
        ] {
            let no_op = no_op_reason(sql).is_some();
            let implemented = crate::core::rql::lexer_s1::recognize_query_purpose(sql).is_some();
            assert!(
                !(no_op && implemented),
                "{sql:?} is treated as a no-op while the engine implements it"
            );
        }
    }

    #[test]
    fn splits_a_comma_separated_action_list() {
        // sea-orm emits exactly this for one `alter_table` with two
        // `add_foreign_key` calls.
        let actions = parse_alter_table_actions(
            "ALTER TABLE ccm ADD CONSTRAINT fk_a FOREIGN KEY (context_id) REFERENCES context (id) ON DELETE CASCADE ON UPDATE CASCADE, \
             ADD CONSTRAINT fk_b FOREIGN KEY (case_model_id) REFERENCES case_model (id) ON DELETE CASCADE ON UPDATE CASCADE",
        )
        .unwrap();
        assert_eq!(
            actions.len(),
            2,
            "both constraints must be parsed, not just the first"
        );
        assert!(actions.iter().all(|a| a.table_name == "ccm"));
        for action in &actions {
            match &action.action {
                AlterAction::ConstraintClause { drop, description } => {
                    assert!(!drop);
                    // The comma must not ride along into the action text; that
                    // is what made a supported CASCADE report as unsupported.
                    assert!(
                        !description.trim_end().ends_with(','),
                        "got {:?}",
                        description
                    );
                }
                other => panic!("expected a constraint clause, got {:?}", other),
            }
        }
    }

    #[test]
    fn a_comma_inside_parens_is_not_a_separator() {
        let actions = parse_alter_table_actions(
            "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (a, b) REFERENCES u (x, y)",
        )
        .unwrap();
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn a_comma_inside_a_quoted_default_is_not_a_separator() {
        let actions =
            parse_alter_table_actions("ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'a, b'").unwrap();
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn a_single_action_still_parses() {
        let actions =
            parse_alter_table_actions(r#"ALTER TABLE "users" ADD COLUMN "nickname" TEXT"#).unwrap();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].table_name, "users");
    }

    #[test]
    fn multiple_add_columns_are_all_parsed() {
        let actions =
            parse_alter_table_actions("ALTER TABLE t ADD COLUMN a INT, ADD COLUMN b TEXT").unwrap();
        assert_eq!(actions.len(), 2);
    }

    #[test]
    fn if_exists_and_only_prefixes_are_skipped() {
        let actions = parse_alter_table_actions(
            "ALTER TABLE IF EXISTS ONLY t ADD COLUMN a INT, ADD COLUMN b TEXT",
        )
        .unwrap();
        assert_eq!(actions.len(), 2);
        assert!(actions.iter().all(|a| a.table_name == "t"));
    }

    #[test]
    fn a_list_that_cannot_be_accounted_for_is_refused() {
        // Refusing beats applying the prefix that parsed: a partly-applied
        // ALTER reported as success is the failure mode being avoided.
        let trailing =
            parse_alter_table_actions("ALTER TABLE t ADD COLUMN a INT, ADD COLUMN b INT,");
        assert!(
            trailing.is_err(),
            "trailing comma must be refused, got {:?}",
            trailing
        );

        let empty = parse_alter_table_actions("ALTER TABLE t ADD COLUMN a INT, , ADD COLUMN b INT");
        assert!(
            empty.is_err(),
            "empty action must be refused, got {:?}",
            empty
        );
    }

    #[test]
    fn parses_add_column() {
        let a = parse_alter_table(r#"ALTER TABLE "users" ADD COLUMN "nickname" TEXT"#).unwrap();
        assert_eq!(a.table_name, "users");
        assert_eq!(
            a.action,
            AlterAction::AddColumn {
                name: "nickname".into(),
                db_type: DbType::STRING,
                // Postgres defaults a column to nullable.
                nullable: true,
                if_not_exists: false,
                default: None,
                json: false,
            }
        );
    }

    #[test]
    fn add_column_reads_its_default() {
        let a = parse_alter_table("ALTER TABLE t ADD COLUMN n INTEGER NOT NULL DEFAULT 0").unwrap();
        match a.action {
            AlterAction::AddColumn {
                default,
                nullable,
                db_type,
                ..
            } => {
                assert_eq!(default.as_deref(), Some("0"));
                assert!(
                    !nullable,
                    "NOT NULL must survive the DEFAULT being split off"
                );
                assert_eq!(db_type, DbType::I32);
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn set_and_drop_default_are_recognised() {
        assert_eq!(
            parse_alter_table("ALTER TABLE t ALTER COLUMN c SET DEFAULT 'x'")
                .unwrap()
                .action,
            AlterAction::SetDefault {
                name: "c".into(),
                expression: Some("'x'".into())
            }
        );
        assert_eq!(
            parse_alter_table("ALTER TABLE t ALTER COLUMN c DROP DEFAULT")
                .unwrap()
                .action,
            AlterAction::SetDefault {
                name: "c".into(),
                expression: None
            }
        );
    }

    #[test]
    fn add_column_honours_not_null() {
        let a = parse_alter_table(r#"ALTER TABLE users ADD COLUMN n INTEGER NOT NULL"#).unwrap();
        match a.action {
            AlterAction::AddColumn {
                nullable, db_type, ..
            } => {
                assert!(!nullable);
                assert_eq!(db_type, DbType::I32);
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn parses_multi_word_types() {
        for (sql, expected) in [
            (
                "ALTER TABLE t ADD COLUMN c TIMESTAMP WITH TIME ZONE",
                DbType::DATETIME,
            ),
            ("ALTER TABLE t ADD COLUMN c DOUBLE PRECISION", DbType::F64),
            (
                "ALTER TABLE t ADD COLUMN c CHARACTER VARYING",
                DbType::STRING,
            ),
            ("ALTER TABLE t ADD COLUMN c NUMERIC(10, 2)", DbType::DECIMAL),
            ("ALTER TABLE t ADD COLUMN c UUID", DbType::UUID),
        ] {
            match parse_alter_table(sql).unwrap().action {
                AlterAction::AddColumn { db_type, .. } => assert_eq!(db_type, expected, "{}", sql),
                other => panic!("{:?}", other),
            }
        }
    }

    #[test]
    fn parses_drop_and_rename_column() {
        assert_eq!(
            parse_alter_table(r#"ALTER TABLE "t" DROP COLUMN "c""#)
                .unwrap()
                .action,
            AlterAction::DropColumn {
                name: "c".into(),
                if_exists: false
            }
        );
        assert_eq!(
            parse_alter_table("ALTER TABLE t DROP COLUMN IF EXISTS c")
                .unwrap()
                .action,
            AlterAction::DropColumn {
                name: "c".into(),
                if_exists: true
            }
        );
        assert_eq!(
            parse_alter_table(r#"ALTER TABLE t RENAME COLUMN "a" TO "b""#)
                .unwrap()
                .action,
            AlterAction::RenameColumn {
                from: "a".into(),
                to: "b".into()
            }
        );
    }

    /// A constraint must be recognised as such, not parsed as a column named
    /// "CONSTRAINT".
    #[test]
    fn constraints_are_recognised_rather_than_read_as_columns() {
        for sql in [
            r#"ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES u (b)"#,
            r#"ALTER TABLE t ADD FOREIGN KEY (a) REFERENCES u (b)"#,
            r#"ALTER TABLE t ADD UNIQUE (a)"#,
            r#"ALTER TABLE t DROP CONSTRAINT fk"#,
        ] {
            assert!(
                matches!(
                    parse_alter_table(sql).unwrap().action,
                    AlterAction::ConstraintClause { .. }
                ),
                "{}",
                sql
            );
        }
    }

    #[test]
    fn schema_qualified_and_quoted_names_resolve() {
        let a = parse_alter_table(r#"ALTER TABLE "public"."users" ADD COLUMN c TEXT"#).unwrap();
        assert_eq!(a.table_name, "users");
        let a = parse_alter_table("ALTER TABLE IF EXISTS ONLY users DROP COLUMN c").unwrap();
        assert_eq!(a.table_name, "users");
    }

    /// `JSON`/`JSONB` used to be refused here, because a JSON column was only
    /// ever a `UUID` pointing at the table its document had been shredded into
    /// and the shredding was arranged by the `CREATE TABLE` path. A document is
    /// now stored as itself, so the name has a type of its own — and the
    /// shredded form, still available behind `--json-shadow-tables`, is
    /// intercepted before it reaches here.
    #[test]
    fn json_names_a_stored_type_while_arrays_and_bytes_are_typed() {
        assert_eq!(parse_column_type(&["JSONB"]).unwrap().0, DbType::JSONB);
        assert_eq!(parse_column_type(&["JSON"]).unwrap().0, DbType::JSONB);
        assert_eq!(parse_column_type(&["BYTEA"]).unwrap().0, DbType::BYTEA);
        assert_eq!(parse_column_type(&["TEXT[]"]).unwrap().0, DbType::ARRAY);
        assert_eq!(parse_column_type(&["UUID[]"]).unwrap().0, DbType::ARRAY);
    }

    #[test]
    fn parses_drop_table() {
        assert_eq!(
            parse_drop_table("DROP TABLE users").unwrap(),
            ("users".into(), false)
        );
        assert_eq!(
            parse_drop_table(r#"DROP TABLE IF EXISTS "public"."users""#).unwrap(),
            ("users".into(), true)
        );
        assert!(parse_drop_table("DROP TABLE a, b").is_err());
    }

    /// An index over an ordinary column is still a no-op — the engine keeps
    /// its own index over every eligible column, so declaring one adds
    /// nothing. Index DDL over a *document path* is not, and neither is any
    /// `DROP INDEX`: a drop names an index without saying what kind it is, and
    /// answering "accepted, did nothing" to the drop of a path index that does
    /// exist would be a silent lie. The executor takes every drop and answers
    /// harmlessly for names it has never stored.
    #[test]
    fn index_ddl_is_a_declared_no_op() {
        assert!(no_op_reason("CREATE INDEX idx ON t (a)").is_some());
        assert!(
            no_op_reason("DROP INDEX idx").is_none(),
            "a drop has to reach the executor, which owns path indexes"
        );
        assert!(
            no_op_reason("CREATE INDEX ix ON t ((doc->>'k'))").is_none(),
            "an index over a document path is a real statement"
        );
        assert!(no_op_reason("CREATE EXTENSION IF NOT EXISTS pgcrypto").is_some());
        // A statement that does something is not one of these.
        assert!(no_op_reason("SELECT 1").is_none());
        assert!(no_op_reason("CREATE TABLE t (a INT)").is_none());
    }

    #[test]
    fn a_unique_index_is_a_constraint_not_an_index() {
        // It is not a no-op: accepting it silently dropped a guarantee the
        // schema asked for, and a duplicate insert then succeeded.
        assert!(no_op_reason("CREATE UNIQUE INDEX idx ON t (a)").is_none());
        assert_eq!(
            unique_index_as_constraint("CREATE UNIQUE INDEX idx ON t (a, b)").as_deref(),
            Some("ALTER TABLE t ADD CONSTRAINT idx UNIQUE (a, b)")
        );
        // `IF NOT EXISTS` is carried through rather than dropped. Dropping it
        // made the second run of a migration fail with "constraint already
        // exists" — precisely what the clause is there to prevent, and what a
        // migration that seeds rows relies on to be re-runnable.
        assert_eq!(
            unique_index_as_constraint(
                "CREATE UNIQUE INDEX IF NOT EXISTS \"idx\" ON \"t\" (\"a\")"
            )
            .as_deref(),
            Some("ALTER TABLE t ADD CONSTRAINT IF NOT EXISTS idx UNIQUE (\"a\")")
        );
        // Without the clause it must stay intolerant: a name already in use is
        // an error, because the second constraint may cover different columns.
        assert!(
            !unique_index_as_constraint("CREATE UNIQUE INDEX idx ON t (a)")
                .unwrap()
                .contains("IF NOT EXISTS")
        );
    }

    #[test]
    fn a_partial_unique_index_stays_a_no_op() {
        // Uniqueness over a subset cannot be expressed here, and enforcing it
        // in full would reject rows PostgreSQL accepts — wrong in the other
        // direction. So it stays declared rather than becoming a lie.
        assert!(
            unique_index_as_constraint("CREATE UNIQUE INDEX i ON t (a) WHERE b IS NULL").is_none()
        );
        assert!(no_op_reason("CREATE UNIQUE INDEX i ON t (a) WHERE b IS NULL").is_some());
    }
}
