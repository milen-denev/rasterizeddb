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
        // Before anything reads a body: the body below runs to the *last* `)`,
        // which swallowed a `PARTITION BY HASH (k)` trailer as constraint text
        // and created an ordinary table, reporting success.
        if let Some(reason) = partitioning_refusal(sql) {
            rejection.replace(reason);
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
        // Find the part between the first '(' and the last ')'.
        //
        // Not the column list's matching `)`, though `top_level_pieces` could
        // find it: the other trailers (`WITH (…)`, `TABLESPACE`, `ON
        // COMMIT`) keep what this has always done with them, which for a
        // parenthesised one is a separate latent bug, pinned by
        // `other_trailers_behave_as_they_did_before`.
        let start = sql.find('(')?;
        let end = sql.rfind(')')?;
        let columns_str = &sql[start + 1..end];
        let mut columns = Vec::new();
        let mut nullable_columns: Vec<String> = Vec::new();
        let mut foreign_keys: Vec<crate::core::rql::foreign_key::ForeignKey> = Vec::new();
        // Which of `foreign_keys` were declared without a name: named after
        // the loop, once every other constraint's name is known.
        let mut unnamed_foreign_keys: Vec<usize> = Vec::new();
        let mut defaults: Vec<crate::core::rql::column_default::ColumnDefault> = Vec::new();
        let mut json_columns: Vec<crate::core::rql::json_map::JsonColumn> = Vec::new();
        let mut type_modifiers: Vec<crate::core::rql::type_modifier::TypeModifier> = Vec::new();
        let mut unique_columns: Vec<String> = Vec::new();
        let mut unique_keys: Vec<crate::core::rql::unique_key::UniqueKey> = Vec::new();
        let mut primary_key: Option<String> = None;
        // The columns a table-level `PRIMARY KEY (…)` names, of any arity.
        let mut key_columns: Vec<String> = Vec::new();
        let mut generated: Vec<crate::core::rql::generated::GeneratedColumn> = Vec::new();
        let mut checks: Vec<crate::core::rql::check::CheckConstraint> = Vec::new();
        // Whether each check's name was written (`CONSTRAINT c CHECK …`)
        // rather than derived, for the duplicate rule below.
        let mut named_checks: Vec<bool> = Vec::new();
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
                    named_checks.push(constraint_name(col_def).is_some());
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
                    // Bare or quoted, as a UNIQUE's name is read: a quoted
                    // name keeps its spaces and non-ASCII characters.
                    let fk_name = match crate::core::rql::unique_key::split_constraint_name(col_def) {
                        Ok((name, _)) => name,
                        Err(reason) => {
                            rejection.replace(reason);
                            return None;
                        }
                    };
                    if fk_name.is_none() {
                        unnamed_foreign_keys.push(foreign_keys.len());
                    }
                    match crate::core::rql::foreign_key::parse_clause(
                        fk_clause,
                        &table_name,
                        fk_name,
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
                        if col_def.to_ascii_uppercase().contains("PRIMARY KEY") {
                            if key.columns.len() == 1 {
                                primary_key = Some(key.columns[0].clone());
                            }
                            key_columns.extend(key.columns.iter().cloned());
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
            let check_name = column_check_name(col_def);
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

            // The name (quoted, it may hold spaces), then the (potentially
            // multi-token) type and constraints, split by whitespace.
            let (name, name_end) = crate::core::sql_scan::leading_identifier(col_def)?;
            // Read whole now, but the SELECT list, ORDER BY, the WHERE
            // rewriter and tokenizer and ALTER still split names on
            // whitespace, so a column created with a space in its name could
            // be written and not read back by name. Refused by name until
            // every one of them uses this reader.
            if name.chars().any(char::is_whitespace) {
                rejection.replace(format!(
                    "a column name containing a space is not supported yet: {}",
                    crate::core::value_codec::double_quoted(&name)
                ));
                return None;
            }
            let mut tokens: Vec<&str> = vec![col_def[..name_end].trim()];
            tokens.extend(col_def[name_end..].split_whitespace());
            if tokens.len() < 2 {
                return None;
            }

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
            // `b INT UNIQUE NULLS NOT DISTINCT`: the flag above is a plain
            // UNIQUE, under which two NULLs never conflict.
            if tokens[1 + tokens_used..]
                .windows(3)
                .any(|w| w.iter().map(|t| t.to_ascii_uppercase()).eq(["NULLS", "NOT", "DISTINCT"]))
            {
                rejection.replace(format!(
                    "UNIQUE NULLS NOT DISTINCT is not supported: NULLs are always distinct here, so two rows \
                     with a NULL in column {} never conflict",
                    name
                ));
                return None;
            }

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
                // `col TYPE CONSTRAINT n REFERENCES …` names its key `n`.
                let inline_name = inline_constraint_name(col_def);
                if inline_name.is_none() {
                    unnamed_foreign_keys.push(foreign_keys.len());
                }
                match crate::core::rql::foreign_key::parse_clause(fk_clause, &table_name, inline_name, Some(&name)) {
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
                named_checks.push(check_name.is_some());
                checks.push(crate::core::rql::check::CheckConstraint {
                    // `a INT CONSTRAINT a_pos CHECK (…)` is named as written;
                    // the name used to be dropped for the derived one.
                    name: check_name.clone().unwrap_or_else(|| {
                        crate::core::rql::check::derived_name(&table_name, Some(&name), checks.len() + 1)
                    }),
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

        // A column's inline UNIQUE is named for the table and the column, cut
        // where long; two names that cut alike would be one constraint to
        // `DROP CONSTRAINT`, which would then drop one of them blindly.
        // PostgreSQL numbers the second; these names are derived, not stored,
        // so a number could not stay with its column. Refused instead.
        if let Some((a, b, name)) =
            crate::core::rql::unique_key::implicit_name_collision(&table_name, &unique_columns)
        {
            rejection.replace(format!(
                "UNIQUE columns \"{a}\" and \"{b}\" would both be constrained as \"{name}\"; two inline \
                 UNIQUE constraints whose names cut alike are not supported (declare one as a named \
                 table-level UNIQUE)"
            ));
            return None;
        }

        // A primary key's columns are NOT NULL, as PostgreSQL makes them,
        // whichever way the key is declared: a table-level `PRIMARY KEY (a,
        // b)` left both nullable, and `INSERT … VALUES (NULL, 1)` was stored.
        // Nullability is the companion column, so a key column gets none.
        nullable_columns.retain(|c| !key_columns.iter().any(|k| k == c));

        // An unnamed foreign key is `<table>_<column>_fkey`, numbered past
        // every other constraint name the statement declares, as
        // PostgreSQL's `ChooseConstraintName` numbers it: two unnamed keys on
        // one column are `t_c_fkey` and `t_c_fkey1`.
        if !unnamed_foreign_keys.is_empty() {
            let mut taken: Vec<String> = checks
                .iter()
                .map(|c| c.name.clone())
                .chain(unique_keys.iter().map(|k| k.name.clone()))
                .chain(unique_columns.iter().map(|c| crate::core::rql::unique_key::implicit_key_name(&table_name, c)))
                .chain(primary_key.iter().map(|_| crate::core::rql::unique_key::pkey_name(&table_name)))
                .chain(
                    foreign_keys
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| !unnamed_foreign_keys.contains(i))
                        .map(|(_, fk)| fk.name.clone()),
                )
                .collect();
            for i in unnamed_foreign_keys {
                let stem = crate::core::rql::foreign_key::key_stem(&table_name, &foreign_keys[i].column);
                match crate::core::rql::unique_key::choose_name(
                    &stem,
                    crate::core::rql::foreign_key::FKEY_LABEL,
                    |candidate| taken.iter().any(|t| t.eq_ignore_ascii_case(candidate)),
                ) {
                    Ok(name) => {
                        taken.push(name.clone());
                        foreign_keys[i].name = name;
                    }
                    Err(reason) => {
                        rejection.replace(reason);
                        return None;
                    }
                }
            }
        }

        // Companion columns come after every declared column, so the user's
        // columns keep write orders 0..n-1 and `SELECT *` needs no reordering.
        for column in nullable_columns {
            columns.push(CreateColumnData {
                name: crate::core::rql::nullable::flag_column_name(&column),
                data_type: crate::core::rql::nullable::FLAG_TYPE,
            });
        }

        // Two checks written with one name are PostgreSQL's 42710; the
        // second used to supersede the first when the table was loaded, so
        // one of them was silently not enforced. A derived name that meets
        // a written one is numbered on, as PostgreSQL chooses it.
        for i in 0..checks.len() {
            let clash = |j: usize| j != i && checks[j].name.eq_ignore_ascii_case(&checks[i].name);
            if named_checks[i] {
                if (0..i).any(|j| named_checks[j] && clash(j)) {
                    rejection.replace(format!(
                        "constraint \"{}\" for relation \"{}\" already exists",
                        checks[i].name, table_name
                    ));
                    return None;
                }
                continue;
            }
            if (0..checks.len()).any(clash) {
                let base = checks[i].name.clone();
                let free = (1..1000).map(|n| format!("{base}{n}")).find(|candidate| {
                    !checks.iter().any(|c| c.name.eq_ignore_ascii_case(candidate))
                });
                match free {
                    Some(name) => checks[i].name = name,
                    None => {
                        rejection.replace(format!("could not choose a name for a check on \"{}\"", table_name));
                        return None;
                    }
                }
            }
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

/// A piece of a `CREATE TABLE` at paren depth zero. Nothing inside a quote, a
/// comment or a parenthesised group is a word here, which is what keeps a
/// column called `partition` or a string saying `INHERITS` from counting.
#[derive(Debug, PartialEq)]
enum TopLevel {
    /// A bare word, uppercased.
    Word(String),
    /// A double-quoted identifier: always a name, never a keyword.
    Quoted,
    /// A parenthesised group, once it has closed.
    Group,
    /// A string literal, dollar-quoted or not.
    Str,
    /// Any other single character.
    Punct(u8),
}

/// The statement's top-level pieces, tracking nesting, quotes and comments the
/// way PostgreSQL's lexer does: a doubled quote is an escaped one, a backslash
/// escapes inside `E'…'`, `$tag$…$tag$` is a quote, and block comments nest.
///
/// The flag says whether the scan ended balanced: depth zero, no stray `)`, no
/// quote or comment left open. When it did not, what the scanner thinks is
/// top-level is a guess — a `(` it counted that PostgreSQL would not hides the
/// whole trailer — so the caller must not read "no trailer" from it.
fn top_level_pieces(sql: &str) -> (Vec<TopLevel>, bool) {
    let (spans, balanced) = top_level_spans(sql);
    (spans.into_iter().map(|(piece, _)| piece).collect(), balanced)
}

/// [`top_level_pieces`] with each piece's byte range in `sql`. A group's
/// range runs from its `(` through its `)`.
fn top_level_spans(sql: &str) -> (Vec<(TopLevel, std::ops::Range<usize>)>, bool) {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut balanced = true;
    let mut i = 0usize;
    let mut group_open = 0usize;
    // Bytes rather than chars: every delimiter is ASCII, and UTF-8 never puts
    // an ASCII byte inside a multi-byte sequence, so the word slices below
    // always fall on character boundaries.
    let is_word_byte = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80;
    // Only a `$` that does not continue a word opens a dollar quote: `a$$b`
    // is one identifier.
    let starts_word = |at: usize| at == 0 || !is_word_byte(bytes[at - 1]);
    while i < bytes.len() {
        let at = i;
        let c = bytes[i];
        if c == b'\'' || c == b'"' {
            // `E'…'`, the E a word of its own: `typE'…'` is a type and a
            // plain literal, where a backslash is just a byte.
            let escapes =
                c == b'\'' && i > 0 && matches!(bytes[i - 1], b'E' | b'e') && starts_word(i - 1);
            i += 1;
            let mut closed = false;
            while i < bytes.len() {
                if escapes && bytes[i] == b'\\' {
                    i += 2;
                    continue;
                }
                if bytes[i] == c {
                    if bytes.get(i + 1) == Some(&c) {
                        i += 2;
                        continue;
                    }
                    closed = true;
                    break;
                }
                i += 1;
            }
            balanced &= closed;
            i += 1;
            if depth == 0 {
                let piece = if c == b'"' {
                    TopLevel::Quoted
                } else {
                    TopLevel::Str
                };
                out.push((piece, at..i.min(bytes.len())));
            }
        } else if let Some(tag_len) = (c == b'$' && starts_word(i))
            .then(|| dollar_tag_len(&bytes[i..]))
            .flatten()
        {
            let tag = &bytes[i..i + tag_len];
            let body = i + tag_len;
            match bytes[body..].windows(tag_len).position(|w| w == tag) {
                Some(end) => i = body + end + tag_len,
                None => {
                    balanced = false;
                    i = bytes.len();
                }
            }
            if depth == 0 {
                out.push((TopLevel::Str, at..i));
            }
        } else if c == b'-' && bytes.get(i + 1) == Some(&b'-') {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
        } else if c == b'/' && bytes.get(i + 1) == Some(&b'*') {
            // Nested, as PostgreSQL's are: `/* /* */ ( */` is all comment.
            let mut nest = 1usize;
            i += 2;
            while i < bytes.len() && nest > 0 {
                match (bytes[i], bytes.get(i + 1)) {
                    (b'/', Some(b'*')) => {
                        nest += 1;
                        i += 2;
                    }
                    (b'*', Some(b'/')) => {
                        nest -= 1;
                        i += 2;
                    }
                    _ => i += 1,
                }
            }
            balanced &= nest == 0;
        } else if c == b'(' {
            if depth == 0 {
                group_open = at;
            }
            depth += 1;
            i += 1;
        } else if c == b')' {
            match depth {
                0 => {
                    balanced = false;
                    out.push((TopLevel::Punct(c), at..at + 1));
                }
                1 => out.push((TopLevel::Group, group_open..at + 1)),
                _ => {}
            }
            depth = depth.saturating_sub(1);
            i += 1;
        } else if depth > 0 || c.is_ascii_whitespace() {
            i += 1;
        } else if is_word_byte(c) {
            let start = i;
            while i < bytes.len() && is_word_byte(bytes[i]) {
                i += 1;
            }
            out.push((TopLevel::Word(sql[start..i].to_ascii_uppercase()), start..i));
        } else {
            out.push((TopLevel::Punct(c), at..at + 1));
            i += 1;
        }
    }
    (out, balanced && depth == 0)
}

/// The length of the dollar-quote delimiter `$tag$` (or `$$`) at the start of
/// `rest`, or `None` when `rest` does not start with one. The tag follows
/// identifier rules without `$`, so `$1` is a parameter, not a quote.
fn dollar_tag_len(rest: &[u8]) -> Option<usize> {
    let tag_byte = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80;
    if rest.get(1).is_some_and(|b| b.is_ascii_digit()) {
        return None;
    }
    let tag = rest[1..].iter().take_while(|&&b| tag_byte(b)).count();
    (rest.get(1 + tag) == Some(&b'$')).then_some(tag + 2)
}

/// Why a `CREATE TABLE` asking for partitioning or inheritance is refused, or
/// `None` when it does not ask for either.
///
/// Matched on whole words at depth zero, anywhere after `CREATE TABLE`:
/// `PARTITION BY` and `PARTITION OF` cannot be anything else there (`PARTITION
/// OF` may have no column list at all, so its `FOR VALUES (…)` would otherwise
/// be read as one), and neither can `INHERITS (…)` once a group has closed
/// before it, since a table may be named `inherits`. Not anchored on the name:
/// the parser below accepts names this could not shape-match (`t-1`, `'t'`),
/// and a missed shape would let the trailer be swallowed. Worded "not
/// supported" so the wire reports `0A000`, as for every other feature the
/// engine lacks.
///
/// A statement the scan cannot balance is refused as a syntax error, as
/// PostgreSQL refuses it: the parser below would take its body to the last
/// `)` whatever it was, swallowing a trailer this scan never saw.
fn partitioning_refusal(sql: &str) -> Option<String> {
    let (pieces, balanced) = top_level_pieces(sql);
    if !balanced {
        return Some(
            "syntax error in CREATE TABLE: an unclosed or unmatched parenthesis, quote or comment"
                .to_string(),
        );
    }
    let word = |at: usize, w: &str| matches!(pieces.get(at), Some(TopLevel::Word(x)) if x == w);
    let refuse = |clause: &str, what: &str| {
        Some(format!(
            "CREATE TABLE ... {} is not supported yet: RasterizedDB has no table {}",
            clause, what
        ))
    };
    let first_group = pieces.iter().position(|p| *p == TopLevel::Group);
    for at in 2..pieces.len() {
        if word(at, "PARTITION") && word(at + 1, "BY") {
            return refuse("PARTITION BY", "partitioning");
        }
        if word(at, "PARTITION") && word(at + 1, "OF") {
            return refuse("PARTITION OF", "partitioning");
        }
        if word(at, "INHERITS")
            && first_group.is_some_and(|g| g < at)
            && pieces.get(at + 1) == Some(&TopLevel::Group)
        {
            return refuse("INHERITS", "inheritance");
        }
    }
    None
}

/// `PARTITION BY HASH (c1 [, c2 [, c3]]) [PARTITIONS n]` on a `CREATE TABLE`
/// (`docs/partitioning.md` §2). Parsed beside [`CreateTable`], not as a field
/// of it: other crates build `CreateTable` literals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionClause {
    /// The key columns as written, unquoted, leading column first.
    pub columns: Vec<String>,
    /// `PARTITIONS n`, or `None` when the statement names no count.
    pub partitions: Option<u32>,
}

impl PartitionClause {
    /// The partitions to declare: the statement's count or the default.
    pub fn partitions_or_default(&self) -> u32 {
        self.partitions
            .unwrap_or(crate::core::partition::table::DEFAULT_PARTITIONS)
    }
}

/// The `CREATE TABLE` arm's parse: the table, its partition clause when it
/// has the one supported shape, or why it is refused.
///
/// [`create_table_with_reason`] keeps refusing every partitioning trailer:
/// its other callers cannot create a partitioned table, and taking the body
/// to the last `)` is what once swallowed this clause. Here the clause is cut
/// off at the column list's own `)` and the rest parses as a plain table, so
/// the two schemas come from one parser.
///
/// Only checks the statement alone can answer are made here (the shape, the
/// key's columns and their declared types, [`partition_key_refusal`]). What
/// depends on the database (nullability, uniqueness, foreign keys, the cap,
/// the storage) is `Database::create_partitioned_table`'s, for every caller.
pub fn create_table_with_partition(
    qp: &QueryPurpose,
) -> (Option<CreateTable>, Option<PartitionClause>, Option<String>) {
    let QueryPurpose::CreateTable(sql) = qp else {
        return (None, None, None);
    };
    match hash_partition_clause(sql) {
        Ok(None) => {
            let (parsed, rejection) = create_table_with_reason(qp);
            (parsed, None, rejection)
        }
        Err(reason) => (None, None, Some(reason)),
        Ok(Some((clause, head_end, name))) => {
            let head = QueryPurpose::CreateTable(sql[..head_end].to_string());
            let (parsed, rejection) = create_table_with_reason(&head);
            let Some(ct) = parsed else {
                return (None, None, rejection);
            };
            if rejection.is_some() {
                return (None, None, rejection);
            }
            // The plain parser reads the name its own way (up to a space or
            // `(`); a table must not be created under a name other than the
            // one the shape check saw.
            if ct.table_name != name {
                return (
                    None,
                    None,
                    Some(partition_shape_refusal(&format!(
                        "on the table name {name:?}, which reads back as {:?},",
                        ct.table_name
                    ))),
                );
            }
            if let Err(reason) = partition_key_refusal(&ct, &clause, sql) {
                return (None, None, Some(reason));
            }
            (Some(ct), Some(clause), None)
        }
    }
}

/// What a refused partitioned `CREATE TABLE` says. "not supported" makes the
/// wire report 0A000.
fn partition_shape_refusal(detail: &str) -> String {
    format!(
        "CREATE TABLE ... PARTITION BY {detail} is not supported: only CREATE TABLE name (columns) \
         PARTITION BY HASH (c1 [, c2 [, c3]]) [PARTITIONS n] is"
    )
}

/// Finds `PARTITION BY HASH (…) [PARTITIONS n]` straight after the column
/// list and nothing after it but an optional `;`. Returns the clause and the
/// byte offset just past the column list's `)` and the table's name, unquoted,
/// or `None` when the statement has no top-level `PARTITION BY` (the plain
/// path then refuses `PARTITION OF` and `INHERITS` itself). Any other statement with a `PARTITION BY` is
/// refused here, so none reaches a parser that would read the trailer as
/// part of the body.
fn hash_partition_clause(sql: &str) -> Result<Option<(PartitionClause, usize, String)>, String> {
    let (pieces, balanced) = top_level_spans(sql);
    if !balanced {
        // The plain path reports the syntax error.
        return Ok(None);
    }
    let word = |at: usize, w: &str| matches!(pieces.get(at), Some((TopLevel::Word(x), _)) if x == w);
    let Some(by) = (2..pieces.len()).find(|&at| word(at, "PARTITION") && word(at + 1, "BY")) else {
        return Ok(None);
    };
    // Inheritance or a partition of a parent, wherever they sit, keep their
    // own reasons.
    if let Some(reason) = partitioning_refusal(sql).filter(|r| !r.contains("PARTITION BY")) {
        return Err(reason);
    }
    if !(word(0, "CREATE") && word(1, "TABLE")) {
        return Err(partition_shape_refusal("on anything but a plain CREATE TABLE"));
    }
    let name_at = if word(2, "IF") && word(3, "NOT") && word(4, "EXISTS") {
        5
    } else {
        2
    };
    let name_ok = matches!(pieces.get(name_at), Some((TopLevel::Word(_) | TopLevel::Quoted, _)));
    let list_at = name_at + 1;
    let name = pieces
        .get(name_at)
        .map(|(_, span)| sql[span.clone()].trim_matches('"').to_string())
        .unwrap_or_default();
    let list = match pieces.get(list_at) {
        Some((TopLevel::Group, span)) if name_ok => span.clone(),
        _ => {
            return Err(partition_shape_refusal(
                "on a table name that is not one plain or quoted identifier followed by its column list",
            ));
        }
    };
    if by != list_at + 1 {
        return Err(partition_shape_refusal("after anything but the column list"));
    }
    match pieces.get(by + 2) {
        Some((TopLevel::Word(w), _)) if w == "HASH" => {}
        Some((TopLevel::Word(w), _)) if w == "RANGE" || w == "LIST" => {
            return Err(format!(
                "CREATE TABLE ... PARTITION BY {w} is not supported: RasterizedDB partitions tables only by HASH"
            ));
        }
        _ => return Err(partition_shape_refusal("without HASH")),
    }
    let key = match pieces.get(by + 3) {
        Some((TopLevel::Group, span)) => &sql[span.start + 1..span.end - 1],
        _ => return Err(partition_shape_refusal("HASH without a parenthesised column list")),
    };
    let columns = key_columns(key)?;

    let mut at = by + 4;
    let mut partitions = None;
    if word(at, "PARTITIONS") {
        let count = match pieces.get(at + 1) {
            Some((TopLevel::Word(n), _)) if n.bytes().all(|b| b.is_ascii_digit()) => n.parse::<u32>().ok(),
            _ => None,
        };
        let max = crate::core::partition::table::MAX_DECLARED_PER_DATABASE;
        match count {
            Some(n) if (1..=max).contains(&n) => partitions = Some(n),
            _ => {
                let written = pieces
                    .get(at + 1)
                    .map(|(_, span)| &sql[span.clone()])
                    .unwrap_or("");
                return Err(format!(
                    "PARTITIONS {written} is not supported: a partitioned table takes a whole number of \
                     partitions from 1 to {max}"
                ));
            }
        }
        at += 2;
    }
    if matches!(pieces.get(at), Some((TopLevel::Punct(b';'), _))) {
        at += 1;
    }
    if let Some((piece, span)) = pieces.get(at) {
        let text = &sql[span.clone()];
        return Err(match piece {
            TopLevel::Word(w) if w == "COLOCATE" => {
                "CREATE TABLE ... PARTITION BY HASH (...) COLOCATE WITH is not supported yet".to_string()
            }
            _ => partition_shape_refusal(&format!("HASH (...) followed by {text}")),
        });
    }
    Ok(Some((PartitionClause { columns, partitions }, list.end, name)))
}

/// The column names in `PARTITION BY HASH (…)`. Only bare or double-quoted
/// identifiers: PostgreSQL also takes expressions, operator classes and
/// `COLLATE` there, and each would hash something other than the column.
fn key_columns(key: &str) -> Result<Vec<String>, String> {
    let mut columns = Vec::new();
    for piece in split_definitions(key) {
        let piece = piece.trim();
        let quoted = piece.len() >= 2
            && piece.starts_with('"')
            && piece.ends_with('"')
            && !piece[1..piece.len() - 1].contains('"');
        let bare = piece
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_' || b >= 0x80)
            && piece
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80);
        if !(quoted || bare) {
            return Err(format!(
                "partition key element {piece:?} is not supported: a partition key is a list of column \
                 names, without expressions, operator classes or COLLATE"
            ));
        }
        columns.push(piece.trim_matches('"').to_string());
    }
    // A trailing or doubled comma leaves an empty piece out of the split.
    if columns.is_empty() || key.split(',').count() != columns.len() {
        return Err(partition_shape_refusal("HASH with an empty column in its list"));
    }
    Ok(columns)
}

/// Why `clause` cannot key `ct`, from what only the statement knows: which
/// columns exist, their declared types, and the declared type *text*, which
/// is gone once the table is created (§1.4). TEXT and VARCHAR are `STRING`,
/// but so are CITEXT, BPCHAR, CHARACTER, CHAR(n > 1) and NAME, without the
/// case folding, padding or truncation their equality has in PostgreSQL; so
/// a `STRING` key is allowed only when declared TEXT or VARCHAR, and never
/// with a `COLLATE`.
///
/// `sql` is the whole statement. Everything that depends on the database or
/// on the table's other declarations is `Database::create_partitioned_table`'s.
pub fn partition_key_refusal(ct: &CreateTable, clause: &PartitionClause, sql: &str) -> Result<(), String> {
    use crate::core::partition::key::{MAX_ARITY, family_of};
    use crate::core::rql::nullable;

    let key = &clause.columns;
    if key.is_empty() || key.len() > MAX_ARITY {
        return Err(format!(
            "a partition key of {} columns is not supported; it takes 1 to {MAX_ARITY}",
            key.len()
        ));
    }
    for (i, column) in key.iter().enumerate() {
        if key[..i].iter().any(|c| c.eq_ignore_ascii_case(column)) {
            return Err(format!(
                "partition key column \"{column}\" is named twice, which is not supported"
            ));
        }
    }
    let definitions = column_definitions(sql);
    for column in key {
        let Some(field) = ct
            .columns
            .iter()
            .find(|c| !nullable::is_hidden(&c.name) && c.name.eq_ignore_ascii_case(column))
        else {
            return Err(format!("partition key column \"{column}\" does not exist"));
        };
        // A shredded JSON column is declared UUID, which the allowlist takes.
        if ct.json_columns.iter().any(|j| j.column.eq_ignore_ascii_case(column)) {
            return Err(format!(
                "partition key column \"{column}\" is a JSON column, which is not supported as a partition key"
            ));
        }
        if let Err(why) = family_of(&field.data_type) {
            return Err(format!("partition key column \"{column}\" is not supported: {why}"));
        }
        let Some((type_phrase, rest)) = definitions
            .iter()
            .find(|(name, _, _)| name.eq_ignore_ascii_case(column))
            .map(|(_, t, r)| (t.as_str(), r.as_slice()))
        else {
            return Err(format!(
                "partition key column \"{column}\" has a declaration that could not be read back, so its \
                 type cannot be checked; this is not supported"
            ));
        };
        if rest.iter().any(|t| t.eq_ignore_ascii_case("COLLATE")) {
            return Err(format!(
                "a COLLATE on partition key column \"{column}\" is not supported: rows are placed by \
                 their bytes, which a collation's equality does not follow"
            ));
        }
        if field.data_type == DbType::STRING {
            let base = type_phrase.split('(').next().unwrap_or(type_phrase);
            if !matches!(base, "TEXT" | "VARCHAR" | "CHARACTER VARYING") {
                return Err(format!(
                    "partition key column \"{column}\" is declared {type_phrase}, which is not supported as a \
                     partition key: it is stored as text without its PostgreSQL equality (case folding, \
                     blank padding or truncation); declare it TEXT or VARCHAR"
                ));
            }
        }
    }
    Ok(())
}

/// Each column definition in a `CREATE TABLE`'s column list: its unquoted
/// name, its type phrase as [`parse_type_phrase`] reads it (uppercased,
/// modifier kept), and the tokens after the type, with the CHECK, GENERATED
/// and DEFAULT expressions taken out first, as the parser does.
fn column_definitions(sql: &str) -> Vec<(String, String, Vec<String>)> {
    let (pieces, _) = top_level_spans(sql);
    let Some(list) = pieces
        .iter()
        .find(|(p, _)| *p == TopLevel::Group)
        .map(|(_, span)| span.clone())
    else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for def in split_definitions(&sql[list.start + 1..list.end - 1]) {
        let def = def.trim();
        if def.is_empty() || is_table_constraint(def) {
            continue;
        }
        let (def, _) = crate::core::rql::check::split_clause(def);
        let (def, _) = crate::core::rql::generated::split_clause(&def);
        let (def, _) = crate::core::rql::column_default::split_clause(&def);
        let tokens: Vec<&str> = def.split_whitespace().collect();
        if tokens.len() < 2 {
            continue;
        }
        let Some((phrase, used)) = parse_type_phrase(&tokens[1..]) else {
            continue;
        };
        out.push((
            tokens[0].trim_matches('"').to_string(),
            phrase,
            tokens[1 + used..].iter().map(|t| t.to_string()).collect(),
        ));
    }
    out
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
    // Whole words outside quotes and parentheses: a constraint or column
    // named `user_preferences_…` holds "references".
    if let Some(at) = crate::core::rql::join::find_keyword(def, "FOREIGN", 0) {
        return Some(&def[at..]);
    }
    // An inline `REFERENCES` on a column definition.
    crate::core::rql::join::find_keyword(def, "REFERENCES", 0).map(|at| &def[at..])
}

/// The name a column definition gives its `REFERENCES` constraint:
/// `CONSTRAINT <name>` (bare or quoted) directly before `REFERENCES`. A
/// `CONSTRAINT` naming another of the column's constraints is not its.
fn inline_constraint_name(def: &str) -> Option<String> {
    let references = crate::core::rql::join::find_keyword(def, "REFERENCES", 0)?;
    // The last whole-word `CONSTRAINT` before it.
    let mut at = None;
    let mut from = 0;
    while let Some(found) = crate::core::rql::join::find_keyword(def, "CONSTRAINT", from).filter(|f| *f < references) {
        at = Some(found);
        from = found + 1;
    }
    let at = at?;
    match crate::core::rql::unique_key::split_constraint_name(&def[at..]) {
        Ok((Some(name), rest)) if rest.to_ascii_uppercase().starts_with("REFERENCES") => Some(name),
        _ => None,
    }
}

/// The name of a `CONSTRAINT <name> …` definition.
/// The name a column definition gives its `CHECK`: `CONSTRAINT <name>`
/// written directly before the keyword, as in `a INT CONSTRAINT a_pos CHECK
/// (a > 0)`.
fn column_check_name(def: &str) -> Option<String> {
    let upper = def.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    let word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut from = 0;
    let at = loop {
        let at = from + upper[from..].find("CHECK")?;
        let after = at + "CHECK".len();
        if (at == 0 || !word(bytes[at - 1])) && (after >= bytes.len() || !word(bytes[after])) {
            break at;
        }
        from = after;
    };
    let mut before = def[..at].split_whitespace().rev();
    let name = before.next()?;
    before.next().filter(|k| k.eq_ignore_ascii_case("CONSTRAINT"))?;
    Some(name.trim_matches('"').to_string())
}

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

#[cfg(test)]
mod partitioning_refusal_tests {
    use super::*;

    fn parse(sql: &str) -> (Option<CreateTable>, Option<String>) {
        create_table_with_reason(&QueryPurpose::CreateTable(sql.to_string()))
    }

    /// Every column, companions included, as `name:TYPE`: the whole shape a
    /// create registers, so a body cut in the wrong place shows up here.
    fn columns(sql: &str) -> Vec<String> {
        let (parsed, rejection) = parse(sql);
        let parsed = parsed.unwrap_or_else(|| panic!("{} did not parse: {:?}", sql, rejection));
        parsed
            .columns
            .iter()
            .map(|c| format!("{}:{:?}", c.name, c.data_type))
            .collect()
    }

    fn refusal(sql: &str) -> String {
        let (parsed, rejection) = parse(sql);
        assert!(parsed.is_none(), "{} produced a table", sql);
        rejection.unwrap_or_else(|| panic!("{} was rejected without a reason", sql))
    }

    /// Each of these was taken as an ordinary table, or failed on a type it
    /// never declared. The first, fourth and fifth reported success: the body
    /// ran to the *last* `)`, so the trailer became constraint text on the last
    /// column and was ignored.
    #[test]
    fn partitioning_and_inheritance_clauses_are_refused() {
        for (sql, clause) in [
            (
                "CREATE TABLE t (id INT, k INT NOT NULL) PARTITION BY HASH (k)",
                "PARTITION BY",
            ),
            (
                "CREATE TABLE t (id INT, k INT) PARTITION BY HASH (k)",
                "PARTITION BY",
            ),
            (
                "CREATE TABLE p PARTITION OF parent FOR VALUES WITH (MODULUS 4, REMAINDER 0)",
                "PARTITION OF",
            ),
            (
                "CREATE TABLE t (id INT, k INT, PRIMARY KEY (id)) PARTITION BY HASH (k)",
                "PARTITION BY",
            ),
            (
                "CREATE TABLE t (id INT NOT NULL) INHERITS (parent)",
                "INHERITS",
            ),
            (
                "CREATE TABLE t (id INT, k INT NOT NULL) PARTITION BY RANGE (k)",
                "PARTITION BY",
            ),
            (
                "create table if not exists t (id int not null) partition by list (id)",
                "PARTITION BY",
            ),
            (
                "CREATE TABLE IF NOT EXISTS p PARTITION OF parent (id NOT NULL) \
                 FOR VALUES WITH (MODULUS 4, REMAINDER 1)",
                "PARTITION OF",
            ),
            (
                "CREATE TABLE \"public\".\"t\"(id INT NOT NULL)\nPARTITION\n  BY HASH(id)",
                "PARTITION BY",
            ),
            (
                "CREATE TABLE t (id INT NOT NULL) INHERITS(parent) PARTITION BY HASH (id)",
                "INHERITS",
            ),
            // Names the parser below accepts but no name grammar would: the
            // refusal is not anchored on the name, so it cannot miss one.
            (
                "CREATE TABLE t-1 (id INT, k INT NOT NULL) PARTITION BY HASH (k)",
                "PARTITION BY",
            ),
            (
                "CREATE TABLE t#1 (k INT NOT NULL) PARTITION BY HASH (k)",
                "PARTITION BY",
            ),
            (
                "CREATE TABLE t@x (k INT NOT NULL) PARTITION BY HASH (k)",
                "PARTITION BY",
            ),
            (
                "CREATE TABLE 't' (k INT NOT NULL) PARTITION BY HASH (k)",
                "PARTITION BY",
            ),
            (
                "CREATE TABLE a.b.t (k INT NOT NULL) PARTITION BY HASH (k)",
                "PARTITION BY",
            ),
            (
                "CREATE TABLE tablé (k INT NOT NULL) PARTITION BY HASH (k)",
                "PARTITION BY",
            ),
            (
                "CREATE TABLE t-1 PARTITION OF parent FOR VALUES WITH (MODULUS 4, REMAINDER 0)",
                "PARTITION OF",
            ),
            // Not straight after the column list: every top-level word counts.
            (
                "CREATE TABLE t (k INT NOT NULL) USING heap PARTITION BY HASH (k)",
                "PARTITION BY",
            ),
            (
                "CREATE TABLE t (k INT NOT NULL) WITH (fillfactor = 70) PARTITION BY HASH (k)",
                "PARTITION BY",
            ),
            (
                "CREATE TABLE t (k INT NOT NULL) WITH (fillfactor = 70) INHERITS (parent)",
                "INHERITS",
            ),
        ] {
            let reason = refusal(sql);
            assert!(
                reason.contains(clause) && reason.contains("not supported"),
                "{} was refused for the wrong reason: {}",
                sql,
                reason
            );
        }
    }

    /// The scan that finds the column list's end has to see past every `)`
    /// that is not the one closing it: in a quoted name, in a string, in a
    /// nested group.
    #[test]
    fn the_refusal_finds_the_trailer_past_quotes_and_nested_parens() {
        for sql in [
            "CREATE TABLE t (\"x)\" INT, k INT NOT NULL) PARTITION BY HASH (k)",
            // An open paren in a quote would otherwise hold the list open to
            // the end of the statement, hiding the trailer.
            "CREATE TABLE t (\"x(\" INT, k INT NOT NULL) PARTITION BY HASH (k)",
            "CREATE TABLE t (k INT NOT NULL, s TEXT DEFAULT '(') PARTITION BY HASH (k)",
            "CREATE TABLE t (k INT NOT NULL, s TEXT DEFAULT 'a)b') PARTITION BY HASH (k)",
            "CREATE TABLE t (k INT NOT NULL, s TEXT DEFAULT 'it''s ) (') PARTITION BY HASH (k)",
            "CREATE TABLE t (k INT NOT NULL, CHECK (k > (0))) PARTITION BY HASH (k)",
            "CREATE TABLE t (k INT NOT NULL, n NUMERIC(10, 2)) PARTITION BY HASH (k)",
            "CREATE TABLE t (k INT NOT NULL /* ) */) /* ( */ PARTITION BY HASH (k)",
            "CREATE TABLE t (k INT NOT NULL) -- (\n PARTITION BY HASH (k)",
            // A backslash escapes the quote in an E string, so `'(` is inside it.
            "CREATE TABLE t (k INT NOT NULL, s TEXT NOT NULL, CHECK (s <> E'\\'(')) PARTITION BY HASH (k)",
            "CREATE TABLE t (k INT NOT NULL, s TEXT NOT NULL, CHECK (s <> e'\\\\')) PARTITION BY HASH (k)",
            // Dollar quotes, tagged or not, hold a paren and a quote.
            "CREATE TABLE t (k INT NOT NULL, s TEXT NOT NULL, CHECK (s <> $$($$)) PARTITION BY HASH (k)",
            "CREATE TABLE t (k INT NOT NULL, s TEXT DEFAULT $x$ ' ( $$ $x$) PARTITION BY HASH (k)",
            // Block comments nest: the `(` is still inside the outer one.
            "CREATE TABLE t (k INT NOT NULL) /* /* */ ( */ PARTITION BY HASH (k)",
            // `$` continues a word, non-ASCII included: neither `a$$` nor
            // `é$$` opens a quote, so the `'$$'` string is the only one.
            "CREATE TABLE t (k INT NOT NULL, a$$ INT, s TEXT DEFAULT '$$') PARTITION BY HASH (k)",
            "CREATE TABLE t (k INT NOT NULL, é$$ INT, s TEXT DEFAULT '$$') PARTITION BY HASH (k)",
            // `E` ending a longer word is no prefix, so `\\` is a plain byte.
            "CREATE TABLE t (k INT NOT NULL, s TEXT NOT NULL, CHECK (s <> nonE'\\')) PARTITION BY HASH (k)",
            // A positional parameter is not a dollar quote: a tag cannot start
            // with a digit, so `$1$1` is two parameters, as PostgreSQL lexes it.
            "CREATE TABLE t (k INT NOT NULL, CHECK (k <> $1)) PARTITION BY HASH (k)",
            "CREATE TABLE t (k INT NOT NULL, CHECK (k <> $1$1)) PARTITION BY HASH (k)",
        ] {
            assert!(
                refusal(sql).contains("PARTITION BY"),
                "{} was not refused as partitioned",
                sql
            );
        }
    }

    /// The words are matched whole, outside quotes and outside the column
    /// list. A column called `partition` or a table called `inherits` is data.
    #[test]
    fn names_and_strings_that_only_contain_the_words_are_not_refused() {
        assert_eq!(
            columns("CREATE TABLE t (id INT, partition INT)"),
            [
                "id:I32",
                "partition:I32",
                "__null_id:BOOL",
                "__null_partition:BOOL"
            ]
        );
        assert_eq!(
            columns("CREATE TABLE t (id INT, \"partition\" INT, \"by\" INT)"),
            [
                "id:I32",
                "partition:I32",
                "by:I32",
                "__null_id:BOOL",
                "__null_partition:BOOL",
                "__null_by:BOOL"
            ]
        );
        assert_eq!(
            columns("CREATE TABLE partition_log (id INT NOT NULL)"),
            ["id:I32"]
        );
        assert_eq!(
            columns("CREATE TABLE inherits (id INT NOT NULL)"),
            ["id:I32"]
        );
        assert_eq!(
            columns("CREATE TABLE partition (id INT NOT NULL)"),
            ["id:I32"]
        );
        let (parsed, _) =
            parse("CREATE TABLE t (id INT NOT NULL, s TEXT DEFAULT 'PARTITION BY HASH (id)')");
        let parsed = parsed.expect("a string that says PARTITION BY is a string");
        assert_eq!(parsed.defaults[0].expression, "'PARTITION BY HASH (id)'");
        assert_eq!(
            columns("CREATE TABLE t (id INT NOT NULL) TABLESPACE partition"),
            ["id:I32"]
        );
        assert_eq!(
            columns("CREATE TABLE t (id INT NOT NULL) TABLESPACE \"inherits\""),
            ["id:I32"]
        );
        // One word, not two: `partition_by` is not `PARTITION BY`.
        assert_eq!(
            columns("CREATE TABLE t (id INT NOT NULL) TABLESPACE partition_by"),
            ["id:I32"]
        );
        assert_eq!(
            columns("CREATE TABLE t (id INT NOT NULL) TABLESPACE inherits"),
            ["id:I32"]
        );
        // Inside the column list, a word followed by a group is an expression,
        // however deeply the groups before it nested.
        let (parsed, _) =
            parse("CREATE TABLE t (n NUMERIC(10,2) NOT NULL, CHECK ((n) > 0 OR inherits (n)))");
        let parsed = parsed.expect("a check calling a function named inherits");
        assert_eq!(parsed.checks[0].expression, "(n) > 0 OR inherits (n)");
        let (parsed, _) = parse("CREATE TABLE t (id INT NOT NULL, CHECK (inherits (id) > 0))");
        let parsed = parsed.expect("a check calling a function named inherits");
        assert_eq!(parsed.checks[0].expression, "inherits (id) > 0");
    }

    /// Trailers other than partitioning keep exactly what they did before the
    /// refusal was added, latent bugs included: a `WITH (…)` group still ends
    /// the body at the *last* `)`, so it is swallowed when the last column has
    /// a constraint and breaks the type when it does not. Pinned so the fix,
    /// when it comes, is a decision rather than a side effect.
    #[test]
    fn other_trailers_behave_as_they_did_before() {
        assert_eq!(
            columns("CREATE TABLE t (id INT NOT NULL) WITH (fillfactor = 70)"),
            ["id:I32"]
        );
        assert_eq!(
            refusal("CREATE TABLE t (id INT) WITH (fillfactor = 70)"),
            "column type 'INT)' is not supported yet"
        );
        assert_eq!(
            columns("CREATE TABLE t (id INT NOT NULL) TABLESPACE pg_default"),
            ["id:I32"]
        );
        assert_eq!(
            columns("CREATE TABLE t (id INT) TABLESPACE pg_default"),
            ["id:I32", "__null_id:BOOL"]
        );
        assert_eq!(
            columns("CREATE TABLE t (id INT NOT NULL) ON COMMIT DROP"),
            ["id:I32"]
        );
        assert_eq!(
            columns("CREATE TABLE t (id INT) ON COMMIT DROP"),
            ["id:I32", "__null_id:BOOL"]
        );
        assert_eq!(
            columns("CREATE TABLE t (id INT) WITHOUT OIDS"),
            ["id:I32", "__null_id:BOOL"]
        );
        // Not a table definition at all; still nothing, and no reason.
        assert!(matches!(
            parse("CREATE TABLE t AS SELECT * FROM u"),
            (None, None)
        ));
    }

    /// A statement the scan cannot balance used to skip the refusal: no group
    /// closed, so no trailer was seen, and the body ran from the first `(` to
    /// the last `)`, swallowing whatever followed. Each is a syntax error in
    /// PostgreSQL, and is one here.
    #[test]
    fn an_unbalanced_statement_is_a_syntax_error_not_a_table() {
        for sql in [
            "CREATE TABLE t (id INT NOT NULL PARTITION BY HASH (id)",
            "CREATE TABLE t (id INT NOT NULL, k INT NOT NULL PARTITION BY HASH (k)",
            "CREATE TABLE t (id INT NOT NULL, CHECK (id > 0) PARTITION BY HASH (id)",
            "CREATE TABLE t (id INT",
            "CREATE TABLE t (id INT NOT NULL)) PARTITION BY HASH (id)",
            "CREATE TABLE t (id INT NOT NULL, s TEXT DEFAULT 'x) PARTITION BY HASH (id)",
            "CREATE TABLE t (id INT NOT NULL) /* /* */ PARTITION BY HASH (id)",
            "CREATE TABLE t (id INT NOT NULL, s TEXT DEFAULT $$x) PARTITION BY HASH (id)",
            "CREATE TABLE t (id INT NOT NULL, s TEXT DEFAULT E'\\') PARTITION BY HASH (id)",
            // Open at depth zero, where only the open quote hides the trailer.
            "CREATE TABLE t (id INT NOT NULL) 'x PARTITION BY HASH (id)",
            "CREATE TABLE t (id INT NOT NULL) \"x PARTITION BY HASH (id)",
            "CREATE TABLE t (id INT NOT NULL) $$x PARTITION BY HASH (id)",
            "CREATE TABLE t (id INT NOT NULL) /* x PARTITION BY HASH (id)",
        ] {
            let reason = refusal(sql);
            assert!(
                reason.starts_with("syntax error"),
                "{} was refused for the wrong reason: {}",
                sql,
                reason
            );
        }
        // Closed once each comment is: the trailer is comment, as in PostgreSQL.
        assert_eq!(
            columns("CREATE TABLE t (id INT NOT NULL) /* /* */ PARTITION BY HASH (id) */"),
            ["id:I32"]
        );
        // A backslash in a plain literal is a byte: the string closes at its
        // second quote.
        assert_eq!(
            columns("CREATE TABLE t (id INT NOT NULL, s TEXT DEFAULT 'a\\')"),
            ["id:I32", "s:STRING", "__null_s:BOOL"]
        );
    }

    /// Bodies whose parentheses and quotes nest parse exactly as before.
    #[test]
    fn bodies_with_nested_parens_and_quotes_parse_as_before() {
        let (ct, _) = parse("CREATE TABLE t (a INT, CHECK (a > 0))");
        let ct = ct.expect("trailing CHECK");
        assert_eq!(
            ct.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["a", "__null_a"]
        );
        assert_eq!(ct.checks.len(), 1);
        assert_eq!(ct.checks[0].expression, "a > 0");

        // Refused before and after, for its expression, not its parentheses.
        assert!(
            refusal("CREATE TABLE t (a INT, ts TIMESTAMP DEFAULT (now()))")
                .starts_with("DEFAULT (now()) is not supported")
        );

        for (sql, expression) in [
            ("CREATE TABLE t (a INT, s TEXT DEFAULT 'a)b')", "'a)b'"),
            (
                "CREATE TABLE t (a INT, s TEXT DEFAULT 'it''s )')",
                "'it''s )'",
            ),
        ] {
            let (ct, _) = parse(sql);
            let ct = ct.expect(sql);
            assert_eq!(
                ct.columns
                    .iter()
                    .map(|c| format!("{}:{:?}", c.name, c.data_type))
                    .collect::<Vec<_>>(),
                ["a:I32", "s:STRING", "__null_a:BOOL", "__null_s:BOOL"]
            );
            assert_eq!(ct.defaults.len(), 1);
            assert_eq!(ct.defaults[0].column, "s");
            assert_eq!(ct.defaults[0].expression, expression);
        }

        assert_eq!(
            columns("CREATE TABLE t (\"x)\" INT, b INT)"),
            ["x):I32", "b:I32", "__null_x):BOOL", "__null_b:BOOL"]
        );

        let (ct, _) = parse("CREATE TABLE t (a INT, n NUMERIC(10,2))");
        let ct = ct.expect("numeric");
        assert_eq!(
            ct.columns
                .iter()
                .map(|c| format!("{}:{:?}", c.name, c.data_type))
                .collect::<Vec<_>>(),
            ["a:I32", "n:DECIMAL", "__null_a:BOOL", "__null_n:BOOL"]
        );
        assert_eq!(ct.type_modifiers.len(), 1);
        assert_eq!(
            (ct.type_modifiers[0].precision, ct.type_modifiers[0].scale),
            (10, 2)
        );

        let (ct, _) = parse("CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b))");
        let ct = ct.expect("composite key");
        assert_eq!(ct.unique_keys.len(), 1);
        assert_eq!(ct.unique_keys[0].columns, ["a", "b"]);
        assert_eq!(ct.primary_key, None);

        for sql in [
            "CREATE TABLE t (a INT, b INT, FOREIGN KEY (b) REFERENCES u(id))",
            "CREATE TABLE t (a INT, b INT REFERENCES u(id))",
        ] {
            let (ct, _) = parse(sql);
            let ct = ct.expect(sql);
            assert_eq!(ct.foreign_keys.len(), 1, "{}", sql);
            assert_eq!(ct.foreign_keys[0].column, "b");
            assert_eq!(ct.foreign_keys[0].parent_table, "u");
            assert_eq!(ct.foreign_keys[0].parent_column, "id");
        }

        // Phase 3 P4: an unnamed key is PostgreSQL's `<t>_<col>_fkey`,
        // numbered past every other name the statement declares; a named
        // one keeps its name as written, quoted, spaced and non-ASCII.
        let (ct, _) = parse(
            "CREATE TABLE t (a INT REFERENCES u(id), b INT, \
             FOREIGN KEY (a) REFERENCES v(id), CONSTRAINT \"外部 key\" FOREIGN KEY (b) REFERENCES u(id), \
             CONSTRAINT t_b_fkey CHECK (b > 0), FOREIGN KEY (b) REFERENCES v(id))",
        );
        let ct = ct.expect("named and unnamed keys");
        assert_eq!(
            ct.foreign_keys.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
            ["t_a_fkey", "t_a_fkey1", "外部 key", "t_b_fkey1"]
        );
        // A column's own `CONSTRAINT n REFERENCES …` names its key (P4 round
        // 2: the name was dropped and the key called `<t>_<col>_fkey`).
        let (ct, reason) = parse(
            "CREATE TABLE t (a INT CONSTRAINT a_to_u REFERENCES u(id), b INT NOT NULL CONSTRAINT \"b key\" \
             REFERENCES u(id), c INT CONSTRAINT c_nn NOT NULL REFERENCES u(id))",
        );
        let ct = ct.unwrap_or_else(|| panic!("inline names: {reason:?}"));
        assert_eq!(
            ct.foreign_keys.iter().map(|f| (f.name.as_str(), f.column.as_str())).collect::<Vec<_>>(),
            [("a_to_u", "a"), ("b key", "b"), ("t_c_fkey", "c")]
        );

        let (ct, _) =
            parse("CREATE TABLE t (a INT, b INT, c INT GENERATED ALWAYS AS (a + b) STORED)");
        let ct = ct.expect("generated");
        assert_eq!(
            ct.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["a", "b", "c", "__null_a", "__null_b", "__null_c"]
        );
        assert_eq!(ct.generated.len(), 1);
        assert_eq!(ct.generated[0].column, "c");
        assert_eq!(ct.generated[0].expression, "a + b");

        assert_eq!(
            columns("CREATE TABLE t(id INT)"),
            ["id:I32", "__null_id:BOOL"]
        );
    }
}

#[cfg(test)]
mod hash_partition_clause_tests {
    use super::*;

    fn parse(sql: &str) -> (Option<CreateTable>, Option<PartitionClause>, Option<String>) {
        create_table_with_partition(&QueryPurpose::CreateTable(sql.to_string()))
    }

    fn accepted(sql: &str) -> (CreateTable, PartitionClause) {
        match parse(sql) {
            (Some(ct), Some(clause), None) => (ct, clause),
            (_, clause, rejection) => panic!("{sql} was not accepted: {clause:?} {rejection:?}"),
        }
    }

    fn refused(sql: &str) -> String {
        let (ct, clause, rejection) = parse(sql);
        assert!(ct.is_none() && clause.is_none(), "{sql} produced a table");
        rejection.unwrap_or_else(|| panic!("{sql} was refused without a reason"))
    }

    /// The supported shape, spelled every way a client writes it. The body
    /// is the plain parser's, so the columns match the same table without
    /// the clause, companions included.
    #[test]
    fn the_supported_shape_parses_to_a_clause_and_the_plain_body() {
        for (sql, key, partitions) in [
            (
                "CREATE TABLE t (tenant_id BIGINT NOT NULL, n INT) PARTITION BY HASH (tenant_id)",
                vec!["tenant_id"],
                None,
            ),
            (
                "create table t (tenant_id bigint not null, g int not null, n int) \
                 partition by hash (tenant_id, g) partitions 16",
                vec!["tenant_id", "g"],
                Some(16),
            ),
            (
                "CREATE TABLE IF NOT EXISTS t (a INT NOT NULL, b UUID NOT NULL, c TEXT NOT NULL)\n\
                 PARTITION\n BY HASH(a,b , \"c\")PARTITIONS 2048;",
                vec!["a", "b", "c"],
                Some(2048),
            ),
            (
                "CREATE TABLE \"t\" (k VARCHAR(10) NOT NULL, s TEXT DEFAULT ')', CHECK (k <> '(')) \
                 PARTITION BY HASH (k) PARTITIONS 1",
                vec!["k"],
                Some(1),
            ),
        ] {
            let (ct, clause) = accepted(sql);
            assert_eq!(ct.table_name, "t", "{sql}");
            assert_eq!(clause.columns, key, "{sql}");
            assert_eq!(clause.partitions, partitions, "{sql}");
            let head_end = sql.to_ascii_uppercase().find("PARTITION").unwrap();
            let (plain, _) = create_table_with_reason(&QueryPurpose::CreateTable(sql[..head_end].to_string()));
            let plain = plain.expect("the body parses as a plain table");
            let shape = |c: &CreateTable| {
                c.columns.iter().map(|c| format!("{}:{:?}", c.name, c.data_type)).collect::<Vec<_>>()
            };
            assert_eq!(shape(&ct), shape(&plain), "{sql}");
        }
        assert_eq!(
            accepted("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (k)").1.partitions_or_default(),
            crate::core::partition::table::DEFAULT_PARTITIONS
        );
        assert_eq!(crate::core::partition::table::DEFAULT_PARTITIONS, 64);
    }

    /// The parser every other caller uses still refuses the clause: it takes
    /// the body to the last `)`, and only the CREATE arm creates partitions.
    #[test]
    fn the_other_callers_still_refuse_partition_by() {
        let (ct, reason) = create_table_with_reason(&QueryPurpose::CreateTable(
            "CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (k)".to_string(),
        ));
        assert!(ct.is_none());
        assert!(reason.is_some_and(|r| r.contains("PARTITION BY") && r.contains("not supported")));
    }

    /// Every partitioning shape but the one is refused, worded "not
    /// supported" (0A000), and names what it refused.
    #[test]
    fn every_other_partitioning_shape_is_refused_with_its_reason() {
        for (sql, names) in [
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY RANGE (k)", "RANGE"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY LIST (k)", "LIST"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH k", "parenthesised"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY (k)", "without HASH"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (k) COLOCATE WITH u", "COLOCATE WITH"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (k) WITH (fillfactor = 70)", "WITH"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (k) TABLESPACE fast", "TABLESPACE"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (k) PARTITIONS 4 PARTITIONS 8", "PARTITIONS"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (k) PARTITION BY HASH (k)", "PARTITION"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (k); DROP TABLE u", "DROP"),
            ("CREATE TABLE t (k INT NOT NULL) WITH (fillfactor = 70) PARTITION BY HASH (k)", "column list"),
            ("CREATE TABLE t (k INT NOT NULL) USING heap PARTITION BY HASH (k)", "column list"),
            ("CREATE TABLE t (k INT NOT NULL) INHERITS (p) PARTITION BY HASH (k)", "INHERITS"),
            (
                "CREATE TABLE c PARTITION OF p FOR VALUES WITH (MODULUS 2, REMAINDER 0) PARTITION BY HASH (k)",
                "PARTITION OF",
            ),
            ("CREATE TABLE a.t (k INT NOT NULL) PARTITION BY HASH (k)", "table name"),
            ("CREATE TABLE t-1 (k INT NOT NULL) PARTITION BY HASH (k)", "table name"),
            ("CREATE TABLE 't' (k INT NOT NULL) PARTITION BY HASH (k)", "table name"),
            ("CREATE TABLE \"my t\" (k INT NOT NULL) PARTITION BY HASH (k)", "table name"),
            ("CREATE TABLE t PARTITION BY HASH (k)", "table name"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (k) PARTITIONS 0", "PARTITIONS 0"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (k) PARTITIONS 2049", "PARTITIONS 2049"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (k) PARTITIONS 99999999999", "PARTITIONS"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (k) PARTITIONS many", "PARTITIONS many"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (k) PARTITIONS", "PARTITIONS"),
            // Expressions, operator classes, COLLATE and empty elements.
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH ((k + 1))", "column names"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (lower(k))", "column names"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (k int4_ops)", "column names"),
            ("CREATE TABLE t (k TEXT NOT NULL) PARTITION BY HASH (k COLLATE \"C\")", "column names"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH ()", "empty"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (k,)", "empty"),
            ("CREATE TABLE t (k INT NOT NULL, j INT NOT NULL) PARTITION BY HASH (k,,j)", "column names"),
        ] {
            let reason = refused(sql);
            assert!(
                reason.contains("not supported") && reason.contains(names),
                "{sql} was refused for the wrong reason: {reason}"
            );
        }
    }

    /// The key's columns: they exist, each once, at most three, and of a
    /// type on the allowlist (§1.4).
    #[test]
    fn key_columns_must_exist_once_and_be_allowed() {
        for (sql, names) in [
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (nope)", "does not exist"),
            // A nullability companion is the engine's column, not the user's.
            ("CREATE TABLE t (k INT) PARTITION BY HASH (__null_k)", "does not exist"),
            ("CREATE TABLE t (k INT NOT NULL) PARTITION BY HASH (k, K)", "twice"),
            (
                "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, c INT NOT NULL, d INT NOT NULL) \
                 PARTITION BY HASH (a, b, c, d)",
                "4 columns",
            ),
            ("CREATE TABLE t (k DOUBLE PRECISION NOT NULL) PARTITION BY HASH (k)", "F64"),
            ("CREATE TABLE t (k NUMERIC(10, 2) NOT NULL) PARTITION BY HASH (k)", "NUMERIC"),
            ("CREATE TABLE t (k BOOLEAN NOT NULL) PARTITION BY HASH (k)", "BOOL"),
            ("CREATE TABLE t (k JSONB NOT NULL) PARTITION BY HASH (k)", "JSON"),
            ("CREATE TABLE t (k INT[] NOT NULL) PARTITION BY HASH (k)", "ARRAY"),
            ("CREATE TABLE t (k TIMESTAMP NOT NULL) PARTITION BY HASH (k)", "not supported"),
            ("CREATE TABLE t (k BYTEA NOT NULL) PARTITION BY HASH (k)", "not supported"),
            ("CREATE TABLE t (k CHAR NOT NULL) PARTITION BY HASH (k)", "CHAR"),
        ] {
            let reason = refused(sql);
            assert!(reason.contains(names), "{sql} was refused for the wrong reason: {reason}");
            if names != "does not exist" {
                assert!(reason.contains("not supported"), "{sql}: {reason}");
            }
        }
    }

    /// TEXT and VARCHAR are `STRING`, and so are the aliases whose PostgreSQL
    /// equality is not byte equality. Only the declared text tells them
    /// apart, and only at CREATE (§1.4).
    #[test]
    fn text_keys_are_allowed_only_as_text_or_varchar() {
        for ty in ["TEXT", "text", "VARCHAR", "VARCHAR(10)", "varchar (10)", "CHARACTER VARYING(10)"] {
            let sql = format!("CREATE TABLE t (k {ty} NOT NULL) PARTITION BY HASH (k)");
            let (ct, _) = accepted(&sql);
            assert_eq!(ct.columns[0].data_type, DbType::STRING, "{sql}");
        }
        for (ty, names) in [
            ("CITEXT", "CITEXT"),
            ("BPCHAR", "BPCHAR"),
            ("NAME", "NAME"),
            ("CHARACTER(5)", "CHARACTER(5)"),
            ("CHAR(5)", "CHAR(5)"),
            ("VARCHAR(10) COLLATE \"C\"", "COLLATE"),
            // Not `COLLATE "default"`: the DEFAULT-clause split finds the
            // word inside the quotes, and the plain parser refuses it.
            ("TEXT COLLATE \"POSIX\"", "COLLATE"),
            ("text collate \"en_US\"", "COLLATE"),
        ] {
            let sql = format!("CREATE TABLE t (k {ty} NOT NULL) PARTITION BY HASH (k)");
            let (plain, _) = create_table_with_reason(&QueryPurpose::CreateTable(format!(
                "CREATE TABLE t (k {ty} NOT NULL)"
            )));
            // Each is accepted as a plain table, so only the alias scan stops it.
            assert!(plain.is_some(), "{ty} is not a type the plain parser takes");
            let reason = refused(&sql);
            assert!(
                reason.contains(names) && reason.contains("not supported"),
                "{sql} was refused for the wrong reason: {reason}"
            );
        }
        // A COLLATE on another column, or in a string, is not the key's.
        accepted("CREATE TABLE t (k TEXT NOT NULL, s TEXT COLLATE \"C\") PARTITION BY HASH (k)");
        accepted("CREATE TABLE t (k TEXT NOT NULL DEFAULT 'collate') PARTITION BY HASH (k)");
    }

    /// A shredded JSON column is declared UUID, which the allowlist takes;
    /// the JSON record is what refuses it.
    #[test]
    fn a_shredded_json_column_is_not_a_key() {
        let mut ct = accepted("CREATE TABLE t (k UUID NOT NULL) PARTITION BY HASH (k)").0;
        ct.json_columns.push(crate::core::rql::json_map::JsonColumn {
            column: "k".to_string(),
            shadow_table: "t__k".to_string(),
            is_deleted: false,
        });
        let clause = PartitionClause {
            columns: vec!["k".to_string()],
            partitions: None,
        };
        let reason = partition_key_refusal(&ct, &clause, "CREATE TABLE t (k UUID NOT NULL)").unwrap_err();
        assert!(reason.contains("JSON") && reason.contains("not supported"), "{reason}");
    }
}
