//! Turning what the source holds into statements this engine will run.
//!
//! Nothing here talks to either database. The plan is a value: a list of
//! statements and the conversion each column needs, which makes the decisions
//! testable without a server on either side, and makes `--pg-import-dry-run`
//! the same code path as a real import minus the execution.
//!
//! # Why the constraints are added last
//!
//! A `UNIQUE` constraint is checked here by scanning the table, so inserting
//! into a table that already has one costs time proportional to the rows
//! already in it — measured at 24 µs a row at 10 000 rows and 88 µs at 60 000,
//! climbing without limit. Loading a million-row table that way does not
//! finish.
//!
//! So a table is created with its constraints, they are dropped before the
//! data goes in, and they are added back afterwards — which validates the whole
//! table in one pass, 60 ms for those same 60 000 rows. Creating the table
//! *with* them and dropping them, rather than adding them at the end, is what
//! keeps the schema's record of which column is the primary key: that is
//! written when the table is created and there is no statement that sets it
//! later.

use super::source::{SourceSchema, SourceTable};
use super::types::{self, JsonMapping, Mapping, SourceColumn};

/// Columns one table-level constraint may span here.
const MAX_KEY_COLUMNS: usize = crate::core::rql::unique_key::MAX_COLUMNS;

/// Longest default expression the schema can store.
const MAX_DEFAULT_LEN: usize = crate::core::rql::column_default::MAX_EXPRESSION_LEN;

/// What to do with a `numeric` column whose declared scale is finer than this
/// engine's fixed nine decimal places.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WideNumeric {
    /// Refuse the import and name the column.
    #[default]
    Fail,
    /// Import it as `TEXT`, which keeps every digit.
    Text,
}

impl WideNumeric {
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "fail" | "refuse" => Some(WideNumeric::Fail),
            "text" => Some(WideNumeric::Text),
            _ => None,
        }
    }
}

/// Everything an import needs to know that is not the connection itself.
#[derive(Debug, Clone)]
pub struct ImportOptions {
    /// The schema to read. PostgreSQL's default is `public`.
    pub schema: String,
    /// Tables to import; empty means all of them.
    pub include: Vec<String>,
    /// Tables to leave out.
    pub exclude: Vec<String>,
    pub json: JsonMapping,
    pub wide_numeric: WideNumeric,
    /// Rows per transaction on the way in.
    pub batch_rows: usize,
    /// Leave primary keys, unique constraints and foreign keys off the target.
    pub skip_constraints: bool,
    /// Work out what would happen and report it, without creating anything.
    pub dry_run: bool,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            schema: "public".to_string(),
            include: Vec::new(),
            exclude: Vec::new(),
            json: JsonMapping::default(),
            wide_numeric: WideNumeric::default(),
            // Large enough that the per-commit costs — one log flush and one
            // outcome record — are spread over thousands of rows, small enough
            // that a failure loses seconds of work rather than minutes.
            batch_rows: 5_000,
            skip_constraints: false,
            dry_run: false,
        }
    }
}

/// A column, and what it becomes.
#[derive(Debug, Clone)]
pub struct PlannedColumn {
    pub source: SourceColumn,
    pub mapping: Mapping,
}

/// One table's worth of statements.
#[derive(Debug, Clone)]
pub struct TablePlan {
    /// `"public"."users"`, for the `COPY`.
    pub source_qualified: String,
    pub name: String,
    pub columns: Vec<PlannedColumn>,
    /// `CREATE TABLE …`, constraints included.
    pub create: String,
    /// The constraints, dropped before the data goes in.
    pub drop_constraints: Vec<String>,
    /// The same constraints, added back afterwards.
    pub add_constraints: Vec<String>,
    /// Foreign keys, added once every table holds its rows.
    pub foreign_keys: Vec<String>,
    /// `COPY (SELECT …) TO STDOUT`.
    pub copy: String,
    /// The source's row estimate, when it has one.
    pub estimated_rows: Option<i64>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Plan {
    pub tables: Vec<TablePlan>,
    pub notes: Vec<String>,
}

impl Plan {
    /// The estimated total, and whether any table could not be estimated.
    pub fn estimated_rows(&self) -> (i64, bool) {
        let known: i64 = self.tables.iter().filter_map(|t| t.estimated_rows).sum();
        let complete = self.tables.iter().all(|t| t.estimated_rows.is_some());
        (known, complete)
    }
}

/// Works out what the import will do, or why it cannot.
pub fn plan(schema: &SourceSchema, options: &ImportOptions) -> Result<Plan, String> {
    let mut plan = Plan {
        notes: schema.notes.clone(),
        ..Default::default()
    };

    check_names(&schema.tables)?;

    for table in &schema.tables {
        plan.tables.push(plan_table(table, options)?);
    }

    // Two differences that are true of a whole import rather than of any one
    // column, and that an operator will otherwise meet as a surprise the first
    // time they read a row back. The values are exact in both cases; it is how
    // they are *rendered* that changes.
    let has_decimal = plan.tables.iter().any(|t| {
        t.columns
            .iter()
            .any(|c| c.mapping.conversion == types::Conversion::Decimal)
    });
    if has_decimal {
        plan.notes.push(format!(
            "NUMERIC keeps the value but not the declared precision and scale — every one is \
             stored to {} decimal places and read back with the trailing zeroes trimmed, so \
             10.5000 comes back as 10.5",
            crate::core::value_codec::DECIMAL_SCALE
        ));
    }
    // Recorded on the column, but not refused on the way in. An operator who
    // assumes the target enforces what the source did would find out when an
    // application writes a null into a column the source would have rejected —
    // and the import is the last moment anyone is reading a list of
    // differences.
    let has_not_null = plan
        .tables
        .iter()
        .any(|t| t.columns.iter().any(|c| c.source.not_null));
    if has_not_null {
        plan.notes.push(
            "NOT NULL is carried over onto the columns but this engine does not yet refuse a \
             null on INSERT or UPDATE. Every imported row honours it, because the source did; \
             writes made afterwards are not checked."
                .to_string(),
        );
    }

    let has_timestamp = plan.tables.iter().any(|t| {
        t.columns
            .iter()
            .any(|c| c.mapping.conversion == types::Conversion::Timestamp)
    });
    if has_timestamp {
        plan.notes.push(
            "TIMESTAMP is reported to clients as `timestamptz` in UTC, so a `timestamp without \
             time zone` reads back with a +00 offset on the end. The instant is the same one."
                .to_string(),
        );
    }

    Ok(plan)
}

/// Refuses names this engine cannot address, all at once.
///
/// Reported together rather than one at a time: an operator who has to rename
/// three columns wants to know that on the first attempt, not the third.
fn check_names(tables: &[SourceTable]) -> Result<(), String> {
    let mut problems: Vec<String> = Vec::new();

    for table in tables {
        if !types::is_plain_identifier(&table.name) {
            problems.push(format!(
                "table {:?} needs quoting, which this engine does not accept everywhere a table \
                 is named",
                table.name
            ));
        }
        for column in &table.columns {
            if !types::is_plain_identifier(&column.name) {
                problems.push(format!(
                    "column {:?}.{:?} needs quoting",
                    table.name, column.name
                ));
            }
        }
        // Column names are compared case-insensitively here and
        // case-sensitively in PostgreSQL, so `Name` and `name` are one column
        // on this side. Two of them would silently become one, with whichever
        // value was written last winning.
        for (i, a) in table.columns.iter().enumerate() {
            for b in &table.columns[i + 1..] {
                if a.name.eq_ignore_ascii_case(&b.name) {
                    problems.push(format!(
                        "table {:?} has both {:?} and {:?}, which differ only in case; this \
                         engine treats them as one column",
                        table.name, a.name, b.name
                    ));
                }
            }
        }
    }

    if problems.is_empty() {
        return Ok(());
    }

    Err(format!(
        "the source cannot be imported as it stands:\n  {}\n\nRename them in the source, or leave \
         the tables out with EXCLUDE TABLES (…) / --pg-import-exclude-tables.",
        problems.join("\n  ")
    ))
}

fn plan_table(table: &SourceTable, options: &ImportOptions) -> Result<TablePlan, String> {
    let mut notes = table.notes.clone();
    let mut columns: Vec<PlannedColumn> = Vec::with_capacity(table.columns.len());

    for column in &table.columns {
        let mut mapping = types::map_column(column, options.json);

        // A `numeric(p, s)` is carried over at its declared scale: the column
        // is created as `NUMERIC(p, s)`, which records the scale in the table's
        // type modifiers, and its values are then stored and rendered with
        // exactly that many places. So `numeric(38,16)` is a number here, not
        // text.
        //
        // Only a scale past what the storage can hold is a problem, and that is
        // caught here — from the declared scale, rather than a million rows
        // into the copy. An unconstrained `numeric` declares nothing about its
        // values, so those are still checked one at a time as they arrive.
        if mapping.conversion == types::Conversion::Decimal {
            if let Some(scale) = types::numeric_scale(column.type_modifier) {
                if scale >= 0 && scale <= crate::core::rql::type_modifier::MAX_DECIMAL_SCALE as i32
                {
                    let precision = types::numeric_precision(column.type_modifier).unwrap_or(38);
                    mapping.declared = format!("NUMERIC({}, {})", precision, scale);
                    mapping.decimal_scale = Some(scale as u32);
                } else {
                    match options.wide_numeric {
                        WideNumeric::Fail => {
                            return Err(format!(
                                "{}.{} is {}, and this engine's DECIMAL keeps at most {} decimal places. \
                                 Import it as TEXT with --pg-import-wide-numeric text (every digit \
                                 is kept, but it stops being a number), or leave the table out.",
                                table.name,
                                column.name,
                                column.formatted_type,
                                crate::core::rql::type_modifier::MAX_DECIMAL_SCALE
                            ));
                        }
                        WideNumeric::Text => {
                            notes.push(format!(
                                "{}.{} is {} and became TEXT, which keeps every digit",
                                table.name, column.name, column.formatted_type
                            ));
                            mapping = Mapping {
                                db_type: crate::core::db_type::DbType::STRING,
                                declared: "TEXT".to_string(),
                                conversion: types::Conversion::Text,
                                select_cast: None,
                                note: None,
                                decimal_scale: None,
                            };
                        }
                    }
                }
            }
        }

        if let Some(note) = &mapping.note {
            notes.push(format!("{}.{}: {}", table.name, column.name, note));
        }

        columns.push(PlannedColumn {
            source: column.clone(),
            mapping,
        });
    }

    if columns.is_empty() {
        return Err(format!("table {:?} has no columns to import", table.name));
    }

    // ---- CREATE TABLE ----------------------------------------------------

    let mut definitions: Vec<String> = Vec::with_capacity(columns.len() + 4);
    for column in &columns {
        let mut definition = format!("{} {}", column.source.name, column.mapping.declared);
        if column.source.not_null {
            definition.push_str(" NOT NULL");
        }
        match translate_default(column) {
            Ok(Some(expression)) => {
                definition.push_str(" DEFAULT ");
                definition.push_str(&expression);
            }
            Ok(None) => {}
            Err(reason) => notes.push(format!("{}.{}: {}", table.name, column.source.name, reason)),
        }
        definitions.push(definition);
    }

    let mut drop_constraints = Vec::new();
    let mut add_constraints = Vec::new();

    if !options.skip_constraints {
        if !table.primary_key.is_empty() {
            if table.primary_key.len() > MAX_KEY_COLUMNS {
                notes.push(format!(
                    "the primary key of {} spans {} columns and this engine holds at most {}; it \
                     was not carried over",
                    table.name,
                    table.primary_key.len(),
                    MAX_KEY_COLUMNS
                ));
            } else {
                let columns = table.primary_key.join(", ");
                definitions.push(format!("PRIMARY KEY ({})", columns));
                // The name the engine gives an unnamed table-level key. It has
                // to match, because the constraint is dropped and re-added by
                // name.
                let name = crate::core::rql::unique_key::UniqueKey::default_name(
                    &table.name,
                    &table.primary_key,
                );
                drop_constraints.push(format!(
                    "ALTER TABLE {} DROP CONSTRAINT {}",
                    table.name, name
                ));
                add_constraints.push(format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY ({})",
                    table.name, name, columns
                ));
            }
        }

        let mut used_names: Vec<String> = Vec::new();
        for key in &table.unique_keys {
            if key.columns.len() > MAX_KEY_COLUMNS {
                notes.push(format!(
                    "unique constraint {} spans {} columns and this engine holds at most {}; it \
                     was not carried over",
                    key.name,
                    key.columns.len(),
                    MAX_KEY_COLUMNS
                ));
                continue;
            }
            let name = constraint_name(&key.name, &table.name, &key.columns, &mut used_names);
            let columns = key.columns.join(", ");
            definitions.push(format!("CONSTRAINT {} UNIQUE ({})", name, columns));
            drop_constraints.push(format!(
                "ALTER TABLE {} DROP CONSTRAINT {}",
                table.name, name
            ));
            add_constraints.push(format!(
                "ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({})",
                table.name, name, columns
            ));
        }
    } else if !table.primary_key.is_empty() || !table.unique_keys.is_empty() {
        notes.push(format!(
            "the keys on {} were not carried over, as asked",
            table.name
        ));
    }

    let create = format!("CREATE TABLE {} ({})", table.name, definitions.join(", "));

    // ---- foreign keys ----------------------------------------------------

    let mut foreign_keys = Vec::new();
    if !options.skip_constraints {
        let mut used_names: Vec<String> = Vec::new();
        for key in &table.foreign_keys {
            // This engine's foreign keys are single-column. A composite one
            // would have to become several, and several single-column keys
            // forbid different rows from what one composite key forbids — so it
            // is reported rather than approximated.
            if key.columns.len() != 1 || key.referenced_columns.len() != 1 {
                notes.push(format!(
                    "foreign key {} spans several columns, which this engine's foreign keys do \
                     not; it was not carried over",
                    key.name
                ));
                continue;
            }
            let name = constraint_name(&key.name, &table.name, &key.columns, &mut used_names);
            let mut statement = format!(
                "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})",
                table.name, name, key.columns[0], key.referenced_table, key.referenced_columns[0]
            );
            if let Some(action) = referential_action(key.on_delete) {
                statement.push_str(" ON DELETE ");
                statement.push_str(action);
            }
            if let Some(action) = referential_action(key.on_update) {
                statement.push_str(" ON UPDATE ");
                statement.push_str(action);
            }
            foreign_keys.push(statement);
        }
    }

    // ---- COPY ------------------------------------------------------------

    let projection: Vec<String> = columns
        .iter()
        .map(|c| {
            let quoted = super::source::quote_ident(&c.source.name);
            match c.mapping.select_cast {
                Some(cast) => format!("{}{}", quoted, cast),
                None => quoted,
            }
        })
        .collect();

    let copy = format!(
        "COPY (SELECT {} FROM {}) TO STDOUT WITH (FORMAT text)",
        projection.join(", "),
        table.qualified()
    );

    Ok(TablePlan {
        source_qualified: table.qualified(),
        name: table.name.clone(),
        columns,
        create,
        drop_constraints,
        add_constraints,
        foreign_keys,
        copy,
        estimated_rows: table.estimated_rows,
        notes,
    })
}

/// `ON DELETE`/`ON UPDATE`, or `None` for the default.
fn referential_action(code: char) -> Option<&'static str> {
    match code {
        'c' => Some("CASCADE"),
        'n' => Some("SET NULL"),
        'd' => Some("SET DEFAULT"),
        'r' => Some("RESTRICT"),
        // `a` is NO ACTION, which is the default and needs no clause.
        _ => None,
    }
}

/// A constraint name this engine will store and find again.
///
/// The schema strips everything but letters, digits and underscores from a
/// name before writing it, so a constraint written with a hyphen would be
/// stored under a different name than the one the statement used — and the
/// `DROP CONSTRAINT` that follows it would find nothing. Sanitising here means
/// what is written is what is stored.
fn constraint_name(
    preferred: &str,
    table: &str,
    columns: &[String],
    used: &mut Vec<String>,
) -> String {
    let mut name: String = preferred
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();

    if name.is_empty() || name.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        name = crate::core::rql::unique_key::UniqueKey::default_name(table, columns);
    }
    // The stored field is 128 bytes; a longer name would be truncated on the
    // way in and not match on the way out.
    if name.len() > 128 {
        name.truncate(128);
    }

    // Two source constraints can sanitise to the same name.
    let mut candidate = name.clone();
    let mut suffix = 2;
    while used.iter().any(|u| u.eq_ignore_ascii_case(&candidate)) {
        candidate = format!("{}_{}", name.trim_end_matches('_'), suffix);
        if candidate.len() > 128 {
            candidate.truncate(128);
        }
        suffix += 1;
    }
    used.push(candidate.clone());
    candidate
}

/// Turns a PostgreSQL default expression into one this engine can store, or
/// says why it cannot.
///
/// Only constants come across. Anything that calls into the source database —
/// a sequence, a function, another column — would be stored as text naming
/// something that does not exist here, and would fail the first time a row was
/// inserted without a value. The column's *data* is unaffected either way: an
/// import supplies every value explicitly.
fn translate_default(column: &PlannedColumn) -> Result<Option<String>, String> {
    let Some(raw) = column.source.default.as_deref() else {
        return Ok(None);
    };

    // `'x'::character varying` — the cast says what PostgreSQL parsed it as and
    // carries no information the target column does not already have.
    let expression = strip_cast(raw.trim()).trim().to_string();
    let upper = expression.to_ascii_uppercase();

    if upper == "NULL" {
        return Ok(None);
    }

    let acceptable = if upper == "TRUE" || upper == "FALSE" {
        true
    } else if matches!(
        upper.as_str(),
        "NOW()" | "CURRENT_TIMESTAMP" | "CURRENT_DATE"
    ) {
        // Rendered in the one spelling this engine's default evaluator knows.
        return Ok(Some("CURRENT_TIMESTAMP".to_string()));
    } else if expression.starts_with('\'') && expression.ends_with('\'') && expression.len() >= 2 {
        true
    } else {
        // A bare number, with or without a sign or a decimal point.
        !expression.is_empty() && expression.parse::<f64>().is_ok()
    };

    if !acceptable {
        return Err(format!(
            "the default {} calls into PostgreSQL and was not carried over",
            truncate(raw)
        ));
    }

    if expression.len() > MAX_DEFAULT_LEN {
        return Err(format!(
            "the default is longer than the {} bytes this engine stores and was not carried over",
            MAX_DEFAULT_LEN
        ));
    }

    Ok(Some(expression))
}

/// Removes a trailing `::type` cast, respecting quotes.
fn strip_cast(expression: &str) -> &str {
    let bytes = expression.as_bytes();
    let mut quote = false;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            quote = !quote;
            i += 1;
            continue;
        }
        if !quote && bytes[i] == b':' && bytes.get(i + 1) == Some(&b':') {
            return &expression[..i];
        }
        i += 1;
    }
    expression
}

fn truncate(value: &str) -> String {
    if value.len() <= 60 {
        return format!("{:?}", value);
    }
    let mut cut = 60;
    while cut > 0 && !value.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{:?}…", &value[..cut])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::migrate::source::{ForeignKey, UniqueKey};

    fn column(name: &str, type_name: &str, not_null: bool) -> SourceColumn {
        SourceColumn {
            name: name.to_string(),
            attnum: 1,
            type_name: type_name.to_string(),
            type_kind: 'b',
            type_category: 'X',
            element_type_name: None,
            type_modifier: -1,
            not_null,
            default: None,
            formatted_type: type_name.to_string(),
            json_shreddable: None,
        }
    }

    fn table(name: &str, columns: Vec<SourceColumn>) -> SourceTable {
        SourceTable {
            schema: "public".to_string(),
            name: name.to_string(),
            oid: 1,
            columns,
            primary_key: Vec::new(),
            unique_keys: Vec::new(),
            foreign_keys: Vec::new(),
            estimated_rows: None,
            notes: Vec::new(),
        }
    }

    fn plan_one(table: SourceTable) -> TablePlan {
        let schema = SourceSchema {
            tables: vec![table],
            notes: Vec::new(),
        };
        plan(&schema, &ImportOptions::default())
            .unwrap()
            .tables
            .remove(0)
    }

    #[test]
    fn a_plain_table_becomes_a_create_and_a_copy() {
        let plan = plan_one(table(
            "users",
            vec![
                column("id", "int8", true),
                column("email", "text", true),
                column("bio", "text", false),
            ],
        ));

        assert_eq!(
            plan.create,
            "CREATE TABLE users (id BIGINT NOT NULL, email TEXT NOT NULL, bio TEXT)"
        );
        assert_eq!(
            plan.copy,
            "COPY (SELECT \"id\", \"email\", \"bio\" FROM \"public\".\"users\") TO STDOUT \
             WITH (FORMAT text)"
        );
    }

    #[test]
    fn the_keys_are_declared_then_deferred() {
        let mut source = table(
            "users",
            vec![column("id", "int8", true), column("email", "text", true)],
        );
        source.primary_key = vec!["id".to_string()];
        source.unique_keys = vec![UniqueKey {
            name: "users_email_key".to_string(),
            columns: vec!["email".to_string()],
        }];
        let plan = plan_one(source);

        assert!(plan.create.contains("PRIMARY KEY (id)"), "{}", plan.create);
        assert!(
            plan.create
                .contains("CONSTRAINT users_email_key UNIQUE (email)"),
            "{}",
            plan.create
        );
        assert_eq!(
            plan.drop_constraints,
            vec![
                "ALTER TABLE users DROP CONSTRAINT users_id_key",
                "ALTER TABLE users DROP CONSTRAINT users_email_key",
            ]
        );
        assert_eq!(
            plan.add_constraints,
            vec![
                "ALTER TABLE users ADD CONSTRAINT users_id_key PRIMARY KEY (id)",
                "ALTER TABLE users ADD CONSTRAINT users_email_key UNIQUE (email)",
            ]
        );
    }

    /// Every dropped constraint has to be added back. A constraint left behind
    /// would be an import that accepts duplicates the source refused, and
    /// nothing would report it.
    #[test]
    fn every_dropped_constraint_is_added_back() {
        let mut source = table(
            "t",
            vec![
                column("a", "int4", true),
                column("b", "int4", true),
                column("c", "text", false),
            ],
        );
        source.primary_key = vec!["a".to_string(), "b".to_string()];
        source.unique_keys = vec![
            UniqueKey {
                name: "t_c_key".to_string(),
                columns: vec!["c".to_string()],
            },
            UniqueKey {
                name: "weird-name!".to_string(),
                columns: vec!["b".to_string()],
            },
        ];
        let plan = plan_one(source);

        assert_eq!(plan.drop_constraints.len(), plan.add_constraints.len());
        for drop in &plan.drop_constraints {
            let name = drop.rsplit(' ').next().unwrap();
            assert!(
                plan.add_constraints
                    .iter()
                    .any(|add| add.contains(&format!(" CONSTRAINT {} ", name))),
                "{} is dropped and never added back",
                name
            );
            assert!(
                plan.create.contains(name) || name.contains("_key"),
                "{} is dropped but the CREATE never declared it: {}",
                name,
                plan.create
            );
        }
    }

    #[test]
    fn a_constraint_name_the_schema_would_mangle_is_sanitised() {
        let mut used = Vec::new();
        assert_eq!(
            constraint_name("weird-name!", "t", &["a".to_string()], &mut used),
            "weirdname"
        );
        // A second constraint sanitising to the same thing gets its own name.
        assert_eq!(
            constraint_name("weird_name", "t", &["a".to_string()], &mut used),
            "weird_name"
        );
        assert_eq!(
            constraint_name("weird name", "t", &["a".to_string()], &mut used),
            "weirdname_2"
        );
        // Nothing usable left: fall back to the name the engine would pick.
        assert_eq!(
            constraint_name("!!!", "t", &["a".to_string()], &mut used),
            "t_a_key"
        );
    }

    #[test]
    fn foreign_keys_are_added_after_the_data() {
        let mut source = table(
            "child",
            vec![column("id", "int4", true), column("parent", "int8", false)],
        );
        source.foreign_keys = vec![ForeignKey {
            name: "child_parent_fkey".to_string(),
            columns: vec!["parent".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: 'c',
            on_update: 'a',
        }];
        let plan = plan_one(source);
        assert_eq!(
            plan.foreign_keys,
            vec![
                "ALTER TABLE child ADD CONSTRAINT child_parent_fkey FOREIGN KEY (parent) \
                 REFERENCES users (id) ON DELETE CASCADE"
            ]
        );
        assert!(!plan.create.contains("FOREIGN KEY"), "{}", plan.create);
    }

    #[test]
    fn a_composite_foreign_key_is_reported_rather_than_split() {
        let mut source = table(
            "child",
            vec![column("a", "int4", true), column("b", "int4", true)],
        );
        source.foreign_keys = vec![ForeignKey {
            name: "fk".to_string(),
            columns: vec!["a".to_string(), "b".to_string()],
            referenced_table: "parent".to_string(),
            referenced_columns: vec!["x".to_string(), "y".to_string()],
            on_delete: 'a',
            on_update: 'a',
        }];
        let plan = plan_one(source);
        assert!(plan.foreign_keys.is_empty());
        assert!(
            plan.notes
                .iter()
                .any(|n| n.contains("spans several columns")),
            "{:?}",
            plan.notes
        );
    }

    #[test]
    fn money_is_selected_as_an_exact_amount() {
        let plan = plan_one(table("t", vec![column("price", "money", false)]));
        assert!(plan.copy.contains("\"price\"::numeric"), "{}", plan.copy);
        assert!(plan.create.contains("price NUMERIC"), "{}", plan.create);
    }

    #[test]
    fn a_wide_numeric_is_carried_over_at_its_declared_scale() {
        // `numeric(38,16)` used to be refused as a number and offered only as
        // TEXT, because the engine kept nine decimal places for every DECIMAL.
        // A column now records its declared scale, so this is an ordinary
        // number that keeps all sixteen of its places.
        let mut c = column("quantity", "numeric", false);
        c.type_modifier = ((38 << 16) | 16) + 4;
        c.formatted_type = "numeric(38,16)".to_string();
        let schema = SourceSchema {
            tables: vec![table("t", vec![c])],
            notes: Vec::new(),
        };

        let planned = plan(&schema, &ImportOptions::default()).expect("wide numeric is a number");
        let mapping = &planned.tables[0].columns[0].mapping;
        assert_eq!(mapping.declared, "NUMERIC(38, 16)");
        assert_eq!(mapping.decimal_scale, Some(16));
        assert!(
            planned.tables[0].create.contains("NUMERIC(38, 16)"),
            "{}",
            planned.tables[0].create
        );
    }

    #[test]
    fn a_numeric_finer_than_the_storage_holds_is_refused_up_front() {
        let mut c = column("amount", "numeric", false);
        // Past `MAX_DECIMAL_SCALE`, which no type modifier can record.
        let scale = crate::core::rql::type_modifier::MAX_DECIMAL_SCALE as i32 + 1;
        c.type_modifier = ((60 << 16) | scale) + 4;
        c.formatted_type = format!("numeric(60,{})", scale);
        let schema = SourceSchema {
            tables: vec![table("t", vec![c.clone()])],
            notes: Vec::new(),
        };

        let error = plan(&schema, &ImportOptions::default()).unwrap_err();
        assert!(error.contains("decimal places"), "{}", error);
        assert!(error.contains("--pg-import-wide-numeric text"), "{}", error);

        let options = ImportOptions {
            wide_numeric: WideNumeric::Text,
            ..Default::default()
        };
        let planned = plan(&schema, &options).unwrap();
        assert_eq!(planned.tables[0].columns[0].mapping.declared, "TEXT");
    }

    #[test]
    fn a_numeric_the_engine_can_hold_is_left_alone() {
        let mut c = column("amount", "numeric", false);
        c.type_modifier = ((10 << 16) | 2) + 4; // numeric(10, 2)
        let plan = plan_one(table("t", vec![c]));
        assert!(plan.create.contains("amount NUMERIC"), "{}", plan.create);
    }

    #[test]
    fn defaults_that_are_constants_come_across_and_the_rest_are_reported() {
        let mut id = column("id", "int4", true);
        id.default = Some("nextval('t_id_seq'::regclass)".to_string());
        let mut flag = column("flag", "bool", false);
        flag.default = Some("true".to_string());
        let mut label = column("label", "text", false);
        label.default = Some("'none'::character varying".to_string());
        let mut at = column("at", "timestamptz", false);
        at.default = Some("now()".to_string());
        let mut computed = column("computed", "int4", false);
        computed.default = Some("(a + b)".to_string());

        let plan = plan_one(table("t", vec![id, flag, label, at, computed]));
        assert!(
            plan.create.contains("flag BOOLEAN DEFAULT true"),
            "{}",
            plan.create
        );
        assert!(
            plan.create.contains("label TEXT DEFAULT 'none'"),
            "{}",
            plan.create
        );
        assert!(
            plan.create
                .contains("at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
            "{}",
            plan.create
        );
        assert!(!plan.create.contains("nextval"), "{}", plan.create);
        assert!(!plan.create.contains("(a + b)"), "{}", plan.create);
        // Both the sequence call and the expression are reported rather than
        // dropped in silence. (A sequence default is also recognised earlier,
        // when the catalogue is read, which is where the note naming the
        // sequence comes from in a real import.)
        assert!(
            plan.notes
                .iter()
                .any(|n| n.starts_with("t.id:") && n.contains("nextval")),
            "{:?}",
            plan.notes
        );
        assert!(
            plan.notes
                .iter()
                .any(|n| n.starts_with("t.computed:") && n.contains("(a + b)")),
            "{:?}",
            plan.notes
        );
    }

    #[test]
    fn a_name_this_engine_cannot_address_stops_the_import() {
        let schema = SourceSchema {
            tables: vec![table("Odd Name", vec![column("id", "int4", true)])],
            notes: Vec::new(),
        };
        let error = plan(&schema, &ImportOptions::default()).unwrap_err();
        assert!(error.contains("needs quoting"), "{}", error);
    }

    #[test]
    fn columns_differing_only_in_case_stop_the_import() {
        let schema = SourceSchema {
            tables: vec![table(
                "t",
                vec![column("name", "text", false), column("Name", "text", false)],
            )],
            notes: Vec::new(),
        };
        let error = plan(&schema, &ImportOptions::default()).unwrap_err();
        assert!(error.contains("differ only in case"), "{}", error);
    }

    #[test]
    fn skipping_constraints_leaves_the_table_bare() {
        let mut source = table("users", vec![column("id", "int8", true)]);
        source.primary_key = vec!["id".to_string()];
        let schema = SourceSchema {
            tables: vec![source],
            notes: Vec::new(),
        };
        let options = ImportOptions {
            skip_constraints: true,
            ..Default::default()
        };
        let plan = plan(&schema, &options).unwrap();
        assert!(!plan.tables[0].create.contains("PRIMARY KEY"));
        assert!(plan.tables[0].drop_constraints.is_empty());
        assert!(plan.tables[0].add_constraints.is_empty());
    }

    #[test]
    fn casts_are_stripped_without_cutting_into_a_literal() {
        assert_eq!(strip_cast("'a'::text"), "'a'");
        assert_eq!(strip_cast("'a::b'::text"), "'a::b'");
        assert_eq!(strip_cast("42"), "42");
    }
}
