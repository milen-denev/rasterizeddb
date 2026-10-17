use crate::core::{db_type::DbType, rql::lexer_s1::QueryPurpose};

pub struct CreateColumnData {
    pub name: String,
    pub data_type: DbType,
}

/// Column constraints that follow the type in a `CREATE TABLE` definition.
///
/// Nullability and uniqueness are acted on. `REFERENCES` and `DEFAULT` are
/// handled elsewhere; `CHECK` and the rest are recognised so they do not break
/// parsing, but are not enforced.
#[derive(Debug, Clone, Copy, Default)]
struct ColumnConstraints {
    nullable: bool,
    unique: bool,
    /// Declared `PRIMARY KEY` on the column itself.
    ///
    /// Recorded separately from `unique` even though a primary key implies it:
    /// the two are enforced the same way but they are not the same statement,
    /// and a console addressing a row needs to know which column the schema
    /// calls the key rather than which columns happen to be unique.
    primary_key: bool,
}

fn parse_constraints(tokens: &[&str], is_serial: bool) -> ColumnConstraints {
    let upper: Vec<String> = tokens.iter().map(|t| t.to_ascii_uppercase()).collect();
    let joined = upper.join(" ");

    // Postgres defaults a column to nullable; `NOT NULL` and `PRIMARY KEY` are
    // what make it not. A `SERIAL` is `NOT NULL` by definition — the clause is
    // part of what the word expands to — and without this a `BIGSERIAL PRIMARY
    // KEY` column got a nullability companion whose presence made the insert
    // path treat the omitted column as NULL rather than as wanting its default.
    let not_null = is_serial || joined.contains("NOT NULL") || joined.contains("PRIMARY KEY");
    // A `PRIMARY KEY` is a `UNIQUE NOT NULL`, so both imply uniqueness. A
    // `REFERENCES … UNIQUE` would too, but the substring test is enough: the
    // word only appears in a column definition as a constraint on that column.
    let unique = joined.contains("UNIQUE") || joined.contains("PRIMARY KEY");
    ColumnConstraints {
        nullable: !not_null,
        unique,
        primary_key: joined.contains("PRIMARY KEY"),
    }
}

pub struct CreateTable {
    pub table_name: String,
    pub columns: Vec<CreateColumnData>,
    pub is_immutable: bool,
    /// Foreign keys declared in the body, either as table-level constraints or
    /// inline on a column.
    pub foreign_keys: Vec<crate::core::rql::foreign_key::ForeignKey>,
    /// `DEFAULT` expressions declared on columns.
    pub defaults: Vec<crate::core::rql::column_default::ColumnDefault>,
    /// Columns declared `JSON`/`JSONB`, each backed by its own table.
    pub json_columns: Vec<crate::core::rql::json_map::JsonColumn>,
    /// Columns constrained `UNIQUE` inline on the column definition.
    /// `PRIMARY KEY` implies it.
    pub unique_columns: Vec<String>,
    /// Declared precision and scale for columns that carry them.
    pub type_modifiers: Vec<crate::core::rql::type_modifier::TypeModifier>,
    /// Table-level `UNIQUE` and `PRIMARY KEY` constraints, single-column and
    /// composite alike. These go to the sidecar rather than onto a schema
    /// field, since a field flag has nowhere to record a tuple.
    pub unique_keys: Vec<crate::core::rql::unique_key::UniqueKey>,
    /// The column the table's `PRIMARY KEY` names, when it names exactly one.
    ///
    /// A composite key is left `None`: the schema header holds one name, and
    /// recording half of a two-column key would say something false. The
    /// constraint itself is still enforced — it lives in the unique-key
    /// sidecar like any other composite.
    pub primary_key: Option<String>,
    /// `CREATE TABLE IF NOT EXISTS`: an existing table is success, not an error.
    pub if_not_exists: bool,
    /// `GENERATED ALWAYS AS (…) STORED` expressions.
    pub generated: Vec<crate::core::rql::generated::GeneratedColumn>,
    /// `CHECK` constraints, both the column-level and the table-level kind.
    pub checks: Vec<crate::core::rql::check::CheckConstraint>,
}

/// Attempts to parse a QueryPurpose::CreateTable variant into a CreateTable struct.
/// Returns None if the QueryPurpose is not CreateTable or parsing fails.
pub fn create_table_from_query_purpose(qp: &QueryPurpose) -> Option<CreateTable> {
    create_table_with_reason(qp).0
}

/// Parses a `CREATE TABLE`, also returning why it was rejected.
///
/// The reason matters: without it an unsupported column type was reported to
/// the client as "Table already exists", because the executor could only see
/// that parsing produced nothing.
pub fn create_table_with_reason(qp: &QueryPurpose) -> (Option<CreateTable>, Option<String>) {
    let mut rejection: Option<String> = None;
    let parsed = parse_create_table(qp, &mut rejection);
    (parsed, rejection)
}

fn parse_create_table(qp: &QueryPurpose, rejection: &mut Option<String>) -> Option<CreateTable> {
    if let QueryPurpose::CreateTable(sql) = qp {
        let sql = sql.as_str();
        // Robustly extract table name after CREATE and TABLE, skipping arbitrary whitespace
        let sql_upper = sql.to_ascii_uppercase();
        let mut words = sql_upper.split_whitespace();
        if words.next()? != "CREATE" {
            return None;
        }
        if words.next()? != "TABLE" {
            return None;
        }
        // Find the start of the table name in the original string
        let mut after_create_table = sql;
        for _ in 0..2 {
            let idx = after_create_table.find(|c: char| !c.is_whitespace())?;
            after_create_table = &after_create_table[idx..];
            let ws = after_create_table
                .find(char::is_whitespace)
                .unwrap_or(after_create_table.len());
            after_create_table = &after_create_table[ws..];
        }
        let idx = after_create_table.find(|c: char| !c.is_whitespace())?;
        after_create_table = &after_create_table[idx..];

        // `CREATE TABLE IF NOT EXISTS t (…)`. Skipped rather than parsed,
        // because the table name is what follows it — without this the name came
        // back as "IF" and the create failed with "Failed to add table".
        //
        // This is not a corner: sea-orm's migrator emits
        // `CREATE TABLE IF NOT EXISTS "seaql_migrations" (…)` as its **first**
        // statement, before any user migration, so no sea-orm application could
        // bootstrap at all. `IF EXISTS` on `DROP TABLE` was already handled;
        // this is the same courtesy on the other side.
        let mut if_not_exists = false;
        {
            let upper = after_create_table.to_ascii_uppercase();
            if upper.starts_with("IF ") {
                let mut rest = after_create_table;
                let mut words = 0;
                for expected in ["IF", "NOT", "EXISTS"] {
                    let start = match rest.find(|c: char| !c.is_whitespace()) {
                        Some(i) => i,
                        None => break,
                    };
                    let tail = &rest[start..];
                    let end = tail.find(char::is_whitespace).unwrap_or(tail.len());
                    if !tail[..end].eq_ignore_ascii_case(expected) {
                        break;
                    }
                    rest = &tail[end..];
                    words += 1;
                }
                if words == 3 {
                    if_not_exists = true;
                    let idx = rest.find(|c: char| !c.is_whitespace())?;
                    after_create_table = &rest[idx..];
                }
            }
        }
        // Table name is up to the next whitespace or '('
        let paren_idx = after_create_table
            .find('(')
            .unwrap_or(after_create_table.len());
        let mut table_name = after_create_table[..paren_idx].trim();
        if let Some(ws_idx) = table_name.find(char::is_whitespace) {
            table_name = &table_name[..ws_idx];
        }
        let table_name = table_name.trim_matches('"').to_string();
        // Find the part between the first '(' and the last ')'
        let start = sql.find('(')?;
        let end = sql.rfind(')')?;
        let columns_str = &sql[start + 1..end];
        let mut columns = Vec::new();
        let mut nullable_columns: Vec<String> = Vec::new();
        let mut foreign_keys: Vec<crate::core::rql::foreign_key::ForeignKey> = Vec::new();
        let mut defaults: Vec<crate::core::rql::column_default::ColumnDefault> = Vec::new();
        let mut json_columns: Vec<crate::core::rql::json_map::JsonColumn> = Vec::new();
        let mut type_modifiers: Vec<crate::core::rql::type_modifier::TypeModifier> = Vec::new();
        let mut unique_columns: Vec<String> = Vec::new();
        let mut unique_keys: Vec<crate::core::rql::unique_key::UniqueKey> = Vec::new();
        let mut primary_key: Option<String> = None;
        let mut generated: Vec<crate::core::rql::generated::GeneratedColumn> = Vec::new();
        let mut checks: Vec<crate::core::rql::check::CheckConstraint> = Vec::new();
        for col_def in split_definitions(columns_str) {
            let col_def = col_def.trim();
            if col_def.is_empty() {
                continue;
            }

            // Table-level constraints sit in the same comma-separated list as
            // column definitions. They declare no column, so they are not
            // parsed as one — but a foreign key among them is captured rather
            // than discarded.
            if is_table_constraint(col_def) {
                // A table-level `CHECK (…)`, which may name several columns.
                let (_, table_check) = crate::core::rql::check::split_clause(col_def);
                if let Some(expression) = table_check {
                    checks.push(crate::core::rql::check::CheckConstraint {
                        name: constraint_name(col_def).unwrap_or_else(|| {
                            crate::core::rql::check::derived_name(
                                &table_name,
                                None,
                                checks.len() + 1,
                            )
                        }),
                        expression,
                        is_deleted: false,
                    });
                    continue;
                }
                if let Some(fk_clause) = foreign_key_clause(col_def) {
                    match crate::core::rql::foreign_key::parse_clause(
                        fk_clause,
                        constraint_name(col_def),
                        None,
                    ) {
                        Ok(fk) => foreign_keys.push(fk),
                        Err(reason) => {
                            rejection.replace(reason);
                            return None;
                        }
                    }
                }
                // Table-level `UNIQUE (...)` / `PRIMARY KEY (...)`, of any
                // arity. A composite constrains the tuple, so it goes to the
                // sidecar rather than onto the columns individually — flagging
                // each column would forbid repeats the constraint allows.
                match crate::core::rql::unique_key::parse_clause(col_def, &table_name) {
                    Ok(Some(key)) => {
                        if col_def.to_ascii_uppercase().contains("PRIMARY KEY")
                            && key.columns.len() == 1
                        {
                            primary_key = Some(key.columns[0].clone());
                        }
                        unique_keys.push(key);
                    }
                    Ok(None) => {}
                    Err(reason) => {
                        rejection.replace(reason);
                        return None;
                    }
                }
                continue;
            }

            // A column-level `CHECK (…)` comes out before anything else reads
            // the definition: its expression is a parenthesised group that the
            // type reader, the DEFAULT reader and the constraint reader would
            // each take a piece of.
            let (col_def_owned, check_expression) = crate::core::rql::check::split_clause(col_def);
            let col_def = col_def_owned.as_str();

            // `GENERATED ALWAYS AS (…) STORED` comes out first, because its
            // expression is a parenthesised group that the DEFAULT and
            // constraint readers would both take pieces of.
            //
            // It used to be left in and recognised only as a constraint
            // keyword, so the column was created and never given a value: every
            // row read NULL for it and no statement failed.
            let (col_def_owned, generated_expression) =
                crate::core::rql::generated::split_clause(col_def);
            let col_def = col_def_owned.as_str();

            // The DEFAULT clause is taken out before the type and constraints
            // are read, so its expression cannot be mistaken for either.
            let (col_def_owned, default_expression) =
                crate::core::rql::column_default::split_clause(col_def);
            let col_def = col_def_owned.as_str();

            // Split by whitespace to get name and (potentially multi-token) type
            let tokens: Vec<&str> = col_def.split_whitespace().collect();
            if tokens.len() < 2 {
                return None;
            }

            let name = tokens[0].trim_matches('"').to_string();
            if crate::core::rql::nullable::is_hidden(&name) {
                rejection.replace(format!(
                    "column names beginning with __null_ are reserved: '{}'",
                    name
                ));
                // The prefix is how the engine marks its own columns; letting a
                // user claim it would make a real column indistinguishable from
                // a nullability companion.
                return None;
            }
            let (type_phrase, tokens_used) = parse_type_phrase(&tokens[1..])?;
            // `SERIAL` is not a type but a shorthand: an integer column, `NOT
            // NULL`, defaulting to the next value of a sequence made with the
            // table. It is expanded here so everything downstream sees an
            // ordinary column with an ordinary default.
            let serial = crate::core::rql::sequence::serial_type(&type_phrase);
            let constraints = parse_constraints(&tokens[1 + tokens_used..], serial.is_some());

            // Map SQL type to DbType (Postgres-friendly mapping; extend as needed)
            // A JSON column holds a reference to the row of a table built from
            // the document, so its own storage is a UUID.
            let declared = type_phrase.split_whitespace().next().unwrap_or("");
            let is_json = matches!(
                declared
                    .split('(')
                    .next()
                    .unwrap_or(declared)
                    .to_ascii_uppercase()
                    .as_str(),
                "JSON" | "JSONB"
            );

            // `NUMERIC(38, 16)` says how many digits the column keeps. Read
            // from the *raw* tokens: `parse_type_phrase` strips the parentheses,
            // so by the time it returns the modifier is already gone. A space
            // inside them splits the type across tokens, so they are rejoined
            // until the parentheses balance.
            let raw_type = {
                let mut joined = String::new();
                for token in &tokens[1..] {
                    joined.push_str(token);
                    let opens = joined.matches('(').count();
                    let closes = joined.matches(')').count();
                    if opens == 0 || opens == closes {
                        break;
                    }
                    joined.push(' ');
                }
                joined
            };
            match crate::core::rql::type_modifier::parse_numeric_modifier(&raw_type) {
                Some(Ok((precision, scale))) => {
                    type_modifiers.push(crate::core::rql::type_modifier::TypeModifier {
                        column: name.clone(),
                        precision,
                        scale,
                        is_deleted: false,
                    });
                }
                Some(Err(reason)) => {
                    rejection.replace(reason);
                    return None;
                }
                None => {}
            }

            let data_type = if is_json && crate::json_shadow_tables() {
                // Shredded: the column holds a reference and the sidecar says
                // which table the document was taken apart into.
                json_columns.push(crate::core::rql::json_map::JsonColumn {
                    column: name.clone(),
                    shadow_table: crate::core::rql::json_map::shadow_table_name(&table_name, &name),
                    is_deleted: false,
                });
                DbType::UUID
            } else if is_json {
                // Stored: the column holds the document. No sidecar entry, and
                // that absence is what tells every later reader which of the
                // two kinds this column is.
                DbType::JSONB
            } else {
                match crate::core::rql::lexer_alter::parse_column_type(
                    &type_phrase.split_whitespace().collect::<Vec<_>>(),
                ) {
                    Ok((t, _)) => t,
                    Err(reason) => {
                        rejection.replace(reason);
                        return None;
                    }
                }
            };

            // `REFERENCES` written on the column itself.
            if let Some(fk_clause) = foreign_key_clause(col_def) {
                match crate::core::rql::foreign_key::parse_clause(fk_clause, None, Some(&name)) {
                    Ok(fk) => foreign_keys.push(fk),
                    Err(reason) => {
                        rejection.replace(reason);
                        return None;
                    }
                }
            }

            if let Some(expression) = default_expression {
                if let Err(reason) =
                    crate::core::rql::column_default::validate(&expression, &data_type)
                {
                    rejection.replace(reason);
                    return None;
                }
                defaults.push(crate::core::rql::column_default::ColumnDefault {
                    column: name.clone(),
                    expression,
                    is_deleted: false,
                });
            } else if serial.is_some() {
                // An explicit `DEFAULT` written alongside `SERIAL` wins, which
                // is why this is the `else`: PostgreSQL lets one override the
                // implicit `nextval`.
                defaults.push(crate::core::rql::column_default::ColumnDefault {
                    column: name.clone(),
                    expression: format!(
                        "nextval('{}')",
                        crate::core::rql::sequence::sequence_name(&table_name, &name)
                    ),
                    is_deleted: false,
                });
            }

            if let Some(expression) = check_expression {
                if expression.len() > crate::core::rql::check::MAX_EXPRESSION_LEN {
                    rejection.replace(format!(
                        "a check expression may be at most {} characters",
                        crate::core::rql::check::MAX_EXPRESSION_LEN
                    ));
                    return None;
                }
                checks.push(crate::core::rql::check::CheckConstraint {
                    name: crate::core::rql::check::derived_name(
                        &table_name,
                        Some(&name),
                        checks.len() + 1,
                    ),
                    expression,
                    is_deleted: false,
                });
            }

            if let Some(expression) = generated_expression {
                if expression.len() > crate::core::rql::generated::MAX_EXPRESSION_LEN {
                    rejection.replace(format!(
                        "a generation expression may be at most {} characters",
                        crate::core::rql::generated::MAX_EXPRESSION_LEN
                    ));
                    return None;
                }
                generated.push(crate::core::rql::generated::GeneratedColumn {
                    column: name.clone(),
                    expression,
                    is_deleted: false,
                });
            }

            if constraints.nullable {
                nullable_columns.push(name.clone());
            }
            if constraints.unique {
                unique_columns.push(name.clone());
            }
            if constraints.primary_key && primary_key.is_none() {
                primary_key = Some(name.clone());
            }
            columns.push(CreateColumnData { name, data_type });
        }

        // Companion columns come after every declared column, so the user's
        // columns keep write orders 0..n-1 and `SELECT *` needs no reordering.
        for column in nullable_columns {
            columns.push(CreateColumnData {
                name: crate::core::rql::nullable::flag_column_name(&column),
                data_type: crate::core::rql::nullable::FLAG_TYPE,
            });
        }

        Some(CreateTable {
            table_name,
            columns,
            is_immutable: false,
            foreign_keys,
            defaults,
            json_columns,
            unique_columns,
            type_modifiers,
            unique_keys,
            primary_key,
            if_not_exists,
            generated,
            checks,
        })
    } else {
        None
    }
}

/// Splits a `CREATE TABLE` body on commas that separate definitions, ignoring
/// commas inside parentheses or quotes.
///
/// A bare `split(',')` would cut `NUMERIC(10, 2)` in half and turn
/// `PRIMARY KEY (a, b)` into two fragments.
pub(crate) fn split_definitions(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;
    let mut quote: Option<char> = None;

    for c in body.chars() {
        match quote {
            Some(q) => {
                current.push(c);
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                '\'' | '"' => {
                    quote = Some(c);
                    current.push(c);
                }
                '(' => {
                    depth += 1;
                    current.push(c);
                }
                ')' => {
                    depth = depth.saturating_sub(1);
                    current.push(c);
                }
                ',' if depth == 0 => out.push(std::mem::take(&mut current)),
                _ => current.push(c),
            },
        }
    }
    if !current.trim().is_empty() {
        out.push(current);
    }
    out
}

/// The `FOREIGN KEY …` or `REFERENCES …` part of a definition, if it has one.
fn foreign_key_clause(def: &str) -> Option<&str> {
    let upper = def.to_ascii_uppercase();
    if let Some(at) = upper.find("FOREIGN KEY") {
        return Some(&def[at..]);
    }
    // An inline `REFERENCES` on a column definition.
    upper.find("REFERENCES").map(|at| &def[at..])
}

/// The name of a `CONSTRAINT <name> …` definition.
fn constraint_name(def: &str) -> Option<String> {
    let upper = def.trim_start().to_ascii_uppercase();
    if !upper.starts_with("CONSTRAINT") {
        return None;
    }
    def.trim_start()
        .split_whitespace()
        .nth(1)
        .map(|n| n.trim_matches('"').to_string())
}

/// True for a table-level constraint rather than a column definition.
///
/// The keyword has to be a whole word. Matched as a bare prefix, a *column*
/// whose name merely begins with one was read as a constraint and dropped from
/// the table — silently, since a constraint the engine does not keep is not an
/// error. `checksum_sha256 TEXT NOT NULL` became a `CHECK` constraint and the
/// column simply did not exist, which surfaced only when an insert named it.
/// `unique_id`, `constraint_name` and `excluded_at` are the same shape.
fn is_table_constraint(def: &str) -> bool {
    let upper = def.trim_start().to_ascii_uppercase();
    [
        "PRIMARY KEY",
        "FOREIGN KEY",
        "UNIQUE",
        "CHECK",
        "CONSTRAINT",
        "EXCLUDE",
    ]
    .iter()
    .any(|kw| {
        upper.strip_prefix(kw).is_some_and(|rest| {
            // A column definition always has a type after the name, so the
            // keyword is followed by something; what matters is that it is not
            // more of the same identifier.
            rest.chars()
                .next()
                .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'))
        })
    })
}

/// A type token, uppercased, **with its modifier left on**.
///
/// The modifier used to be stripped here, which cost `CHAR(5)` its width: the
/// phrase reached `parse_column_type` as a bare `CHAR`, was read as the
/// engine's one-codepoint type, and stored only the first character of every
/// value. Keeping the parentheses is what lets that parser tell `CHAR` from
/// `CHAR(5)`. Nothing downstream is troubled by them — every reader of a type
/// name already cuts at the first `(`.
fn normalize_token_upper(token: &str) -> String {
    token.to_ascii_uppercase()
}

/// Parses a SQL type phrase from a token stream that begins at the type.
/// Returns the normalized uppercase type phrase and number of tokens consumed.
fn parse_type_phrase(tokens: &[&str]) -> Option<(String, usize)> {
    if tokens.is_empty() {
        return None;
    }

    /// A token's type name with its modifier and array brackets removed, so
    /// `PRECISION[][]` matches `PRECISION` and `VARYING(32)` matches `VARYING`.
    ///
    /// Without this, `DOUBLE PRECISION[][]` failed the two-word test, fell back
    /// to the one-word `DOUBLE`, and left `PRECISION[][]` to be read as a
    /// *constraint*. The column became a scalar `F64` and the create still
    /// succeeded — the array-ness was lost with nothing reported.
    fn base(token: &str) -> String {
        token
            .split(['(', '['])
            .next()
            .unwrap_or(token)
            .trim_matches(',')
            .to_ascii_uppercase()
    }

    /// Whether a token carries array brackets.
    fn has_brackets(token: &str) -> bool {
        token.contains('[')
    }

    // Multi-word type names, longest first. Each entry is the words it is
    // spelled with and the canonical phrase they mean.
    const PHRASES: &[(&[&str], &str)] = &[
        (
            &["TIMESTAMP", "WITH", "TIME", "ZONE"],
            "TIMESTAMP WITH TIME ZONE",
        ),
        (
            &["TIMESTAMP", "WITHOUT", "TIME", "ZONE"],
            "TIMESTAMP WITHOUT TIME ZONE",
        ),
        (&["TIME", "WITH", "TIME", "ZONE"], "TIME WITH TIME ZONE"),
        (
            &["TIME", "WITHOUT", "TIME", "ZONE"],
            "TIME WITHOUT TIME ZONE",
        ),
        (&["DOUBLE", "PRECISION"], "DOUBLE PRECISION"),
        (&["CHARACTER", "VARYING"], "CHARACTER VARYING"),
        (&["BIT", "VARYING"], "BIT VARYING"),
    ];

    for (words, canonical) in PHRASES {
        if tokens.len() < words.len() {
            continue;
        }
        if words.iter().enumerate().all(|(i, w)| base(tokens[i]) == *w) {
            // The brackets may sit on any word of the phrase, and are carried
            // onto the canonical form so the caller sees an array.
            let array = tokens[..words.len()].iter().any(|t| has_brackets(t));
            let phrase = if array {
                format!("{}[]", canonical)
            } else {
                canonical.to_string()
            };
            return Some((phrase, words.len()));
        }
    }

    let token = normalize_token_upper(tokens[0]);
    Some((token, 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The columns the user declared, with the engine's nullability companions
    /// filtered out. Every column here is nullable by default — Postgres'
    /// rule — so each one adds a companion that these tests are not about.
    fn declared(result: &CreateTable) -> Vec<&CreateColumnData> {
        result
            .columns
            .iter()
            .filter(|c| !crate::core::rql::nullable::is_hidden(&c.name))
            .collect()
    }

    #[test]
    fn test_create_table_all_types() {
        let sql = r#"CREATE TABLE all_types (
            i8_col INT1,
            i16_col INT2,
            i32_col INT4,
            i64_col INT8,
            u8_col UINT1,
            u16_col UINT2,
            u32_col UINT4,
            u64_col UINT8,
            float_col FLOAT,
            real_col REAL,
            double_col DOUBLE PRECISION,
            char_col CHAR,
            text_col TEXT,
            varchar_col VARCHAR,
            string_col STRING,
            datetime_col DATETIME,
            timestamp_col TIMESTAMP,
            bool_col BOOL,
            boolean_col BOOLEAN
        )"#;
        let qp = QueryPurpose::CreateTable(sql.to_string());
        let result = create_table_from_query_purpose(&qp).expect("Should parse successfully");
        let types = [
            DbType::I8,
            DbType::I16,
            DbType::I32,
            DbType::I64,
            DbType::U8,
            DbType::U16,
            DbType::U32,
            DbType::U64,
            DbType::F64,
            DbType::F32,
            DbType::F64,
            DbType::CHAR,
            DbType::STRING,
            DbType::STRING,
            DbType::STRING,
            DbType::DATETIME,
            DbType::DATETIME,
            DbType::BOOL,
            DbType::BOOL,
        ];
        let names = [
            "i8_col",
            "i16_col",
            "i32_col",
            "i64_col",
            "u8_col",
            "u16_col",
            "u32_col",
            "u64_col",
            "float_col",
            "real_col",
            "double_col",
            "char_col",
            "text_col",
            "varchar_col",
            "string_col",
            "datetime_col",
            "timestamp_col",
            "bool_col",
            "boolean_col",
        ];
        assert_eq!(result.table_name, "all_types");
        assert_eq!(declared(&result).len(), types.len());
        for (i, col) in declared(&result).iter().enumerate() {
            assert_eq!(col.name, names[i]);
            assert_eq!(
                &col.data_type, &types[i],
                "Type mismatch for column {}",
                col.name
            );
        }
    }

    #[test]
    fn test_create_table_from_query_purpose() {
        let sql = r#"CREATE TABLE "users" (
            id INT,
            name TEXT,
            age INT
        )"#;
        let qp = QueryPurpose::CreateTable(sql.to_string());
        let result = create_table_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "users");
        assert_eq!(declared(&result).len(), 3);
        assert_eq!(declared(&result)[0].name, "id");
        assert_eq!(declared(&result)[0].data_type, DbType::I32);
        assert_eq!(declared(&result)[1].name, "name");
        assert_eq!(declared(&result)[1].data_type, DbType::STRING);
        assert_eq!(declared(&result)[2].name, "age");
        assert_eq!(declared(&result)[2].data_type, DbType::I32);
    }

    #[test]
    fn test_create_table_with_various_types_and_quotes() {
        let sql = r#"CREATE TABLE "complex_table" (
            "id" INT4,
            "username" VARCHAR,
            "score" DOUBLE PRECISION,
            "created_at" TIMESTAMP,
            "is_active" INT1,
            "desc" TEXT
        )"#;
        let qp = QueryPurpose::CreateTable(sql.to_string());
        let result = create_table_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "complex_table");
        assert_eq!(declared(&result).len(), 6);
        assert_eq!(declared(&result)[0].name, "id");
        assert_eq!(declared(&result)[0].data_type, DbType::I32);
        assert_eq!(declared(&result)[1].name, "username");
        assert_eq!(declared(&result)[1].data_type, DbType::STRING);
        assert_eq!(declared(&result)[2].name, "score");
        assert_eq!(declared(&result)[2].data_type, DbType::F64);
        assert_eq!(declared(&result)[3].name, "created_at");
        assert_eq!(declared(&result)[3].data_type, DbType::DATETIME);
        assert_eq!(declared(&result)[4].name, "is_active");
        assert_eq!(declared(&result)[4].data_type, DbType::I8);
        assert_eq!(declared(&result)[5].name, "desc");
        assert_eq!(declared(&result)[5].data_type, DbType::STRING);
    }

    #[test]
    fn test_create_table_with_extra_spaces_and_newlines() {
        let sql = r#"  CREATE   TABLE   test_space   (
            col1   INT  ,
            col2   TEXT
        )  "#;
        let qp = QueryPurpose::CreateTable(sql.to_string());
        let result = create_table_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "test_space");
        assert_eq!(declared(&result).len(), 2);
        assert_eq!(declared(&result)[0].name, "col1");
        assert_eq!(declared(&result)[0].data_type, DbType::I32);
        assert_eq!(declared(&result)[1].name, "col2");
        assert_eq!(declared(&result)[1].data_type, DbType::STRING);
    }

    #[test]
    #[should_panic(expected = "Should not parse successfully")]
    fn test_create_table_with_unknown_type() {
        let sql = r#"CREATE TABLE t1 (foo BAR)"#;
        let qp = QueryPurpose::CreateTable(sql.to_string());
        let _result = create_table_from_query_purpose(&qp).expect("Should not parse successfully");
    }

    #[test]
    fn test_create_table_with_no_columns() {
        let sql = r#"CREATE TABLE empty_table ()"#;
        let qp = QueryPurpose::CreateTable(sql.to_string());
        let result = create_table_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "empty_table");
        assert_eq!(declared(&result).len(), 0);
    }

    #[test]
    fn test_create_table_invalid_sql() {
        let sql = r#"CREATE TABLE missing_paren id INT, name TEXT"#;
        let qp = QueryPurpose::CreateTable(sql.to_string());
        assert!(create_table_from_query_purpose(&qp).is_none());
    }

    #[test]
    fn test_create_table_postgres_parameterized_and_timestamptz() {
        let sql = r#"CREATE TABLE t (
            name VARCHAR(32),
            tag CHARACTER VARYING(10),
            created TIMESTAMP WITH TIME ZONE,
            ok BOOLEAN
        )"#;
        let qp = QueryPurpose::CreateTable(sql.to_string());
        let result = create_table_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "t");
        assert_eq!(declared(&result).len(), 4);
        assert_eq!(declared(&result)[0].data_type, DbType::STRING);
        assert_eq!(declared(&result)[1].data_type, DbType::STRING);
        assert_eq!(declared(&result)[2].data_type, DbType::DATETIME);
        assert_eq!(declared(&result)[3].data_type, DbType::BOOL);
    }
}

#[cfg(test)]
mod constraint_keyword_tests {
    use super::is_table_constraint;

    #[test]
    fn a_column_whose_name_begins_with_a_keyword_is_still_a_column() {
        // Each of these was read as a table constraint and dropped from the
        // table without a word of complaint. `checksum_sha256` is the one that
        // was found in the wild: an import created the table without it, and
        // the first insert failed naming a column the schema had never had.
        for def in [
            "checksum_sha256 TEXT NOT NULL",
            "check_digit INT",
            "unique_id UUID NOT NULL",
            "constraint_name TEXT",
            "excluded_at TIMESTAMP",
            "uniqueness INT",
        ] {
            assert!(
                !is_table_constraint(def),
                "{} was taken for a constraint",
                def
            );
        }
    }

    #[test]
    fn a_real_constraint_is_still_recognised() {
        for def in [
            "PRIMARY KEY (id)",
            "FOREIGN KEY (a) REFERENCES b (id)",
            "UNIQUE (a, b)",
            "CHECK (a > 0)",
            "CONSTRAINT fk_x FOREIGN KEY (a) REFERENCES b (id)",
            "EXCLUDE USING gist (a WITH =)",
            "  unique (a)",
        ] {
            assert!(is_table_constraint(def), "{} was not recognised", def);
        }
    }
}
