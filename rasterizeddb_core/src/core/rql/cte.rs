//! Common table expressions: `WITH name AS (SELECT …) <statement>`.
//!
//! Each CTE is **materialised** into a temporary table and the outer statement
//! rewritten to read from it. That composes machinery that already works —
//! `CREATE TABLE`, multi-row `INSERT`, `DROP TABLE` — instead of adding a
//! second way to produce rows, and it gives PostgreSQL's pre-12 semantics: a
//! CTE is an optimisation fence, evaluated once however many times it is
//! referenced. That is a position worth documenting rather than an accident.
//!
//! `WITH RECURSIVE` is **refused by keyword**, before anything else looks at
//! the statement. A recursive CTE run as a non-recursive one does not fail: it
//! computes the base case, stops, and returns a shorter answer to a well-formed
//! query. Silently wrong results are worse than an unsupported feature.

use rclite::Arc;

use crate::core::database::Database;
use crate::core::db_type::DbType;

/// One `name AS ( body )` definition.
#[derive(Debug)]
pub struct CteDef {
    pub name: String,
    pub body: String,
}

/// A statement whose CTEs have been materialised.
#[derive(Debug)]
pub struct Materialized {
    /// The statement with every CTE reference pointing at its temporary table.
    pub rewritten: String,
    /// Tables to drop once the statement is done, however it ends.
    pub temps: Vec<String>,
}

/// True when `sql` starts with the `WITH` keyword as a whole word.
fn starts_with_with(sql: &str) -> bool {
    let t = sql.trim_start();
    t.len() >= 4
        && t[..4].eq_ignore_ascii_case("WITH")
        && t[4..]
            .chars()
            .next()
            .is_some_and(|c| c.is_whitespace() || c == '(')
}

/// Index just past the parenthesis matching the one at `open`, ignoring
/// parentheses inside string literals.
fn matching_paren(s: &str, open: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                b'\'' | b'"' => quote = Some(c),
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            },
        }
        i += 1;
    }
    None
}

/// Splits `WITH a AS (…), b AS (…) <rest>` into its definitions and the rest.
///
/// `Ok(None)` when the statement has no `WITH` at all. `Err` when it has one
/// this cannot honour — including `WITH RECURSIVE`, which is refused rather
/// than approximated.
pub fn split_with(sql: &str) -> Result<Option<(Vec<CteDef>, String)>, String> {
    if !starts_with_with(sql) {
        return Ok(None);
    }
    let trimmed = sql.trim_start();
    let after_with = trimmed[4..].trim_start();

    // Refused by keyword, deliberately before any other check: run as a
    // non-recursive CTE this would return the base case and no error.
    if after_with.len() >= 9
        && after_with[..9].eq_ignore_ascii_case("RECURSIVE")
        && after_with[9..]
            .chars()
            .next()
            .is_some_and(|c| c.is_whitespace())
    {
        return Err(
            "WITH RECURSIVE is not supported yet; refusing rather than returning only the \
             base case"
                .to_string(),
        );
    }

    let mut rest = after_with;
    let mut defs: Vec<CteDef> = Vec::new();

    loop {
        // `name [(cols)] AS ( body )`
        let name_end = rest
            .find(|c: char| c.is_whitespace() || c == '(')
            .ok_or_else(|| "a WITH clause names no query".to_string())?;
        let name = rest[..name_end].trim().trim_matches('"').to_string();
        if name.is_empty() {
            return Err("a WITH clause names no query".to_string());
        }
        let after_name = rest[name_end..].trim_start();
        if after_name.starts_with('(') {
            return Err("a WITH clause that renames its columns is not supported yet".to_string());
        }
        if after_name.len() < 2 || !after_name[..2].eq_ignore_ascii_case("AS") {
            return Err(format!("expected AS after the WITH name '{}'", name));
        }
        let after_as = after_name[2..].trim_start();
        if !after_as.starts_with('(') {
            return Err(format!("expected ( after AS in the WITH clause '{}'", name));
        }
        let open = after_as.as_ptr() as usize - rest.as_ptr() as usize;
        let close = matching_paren(rest, open)
            .ok_or_else(|| format!("unterminated WITH body for '{}'", name))?;
        let body = rest[open + 1..close].trim().to_string();
        if body.is_empty() {
            return Err(format!("the WITH clause '{}' has an empty body", name));
        }
        defs.push(CteDef { name, body });

        let after_body = rest[close + 1..].trim_start();
        if let Some(next) = after_body.strip_prefix(',') {
            rest = next.trim_start();
            continue;
        }
        return Ok(Some((defs, after_body.to_string())));
    }
}

/// The output name of one select-list item, following PostgreSQL: an explicit
/// alias wins, otherwise a column keeps its own name.
///
/// `None` when the item has no name this can determine — refused rather than
/// invented, since the outer query refers to these by name.
fn item_label(item: &str) -> Option<String> {
    let item = item.trim();
    if item.is_empty() {
        return None;
    }
    // ` AS name` at the top level.
    let upper = item.to_ascii_uppercase();
    let mut depth = 0usize;
    let bytes = item.as_bytes();
    let mut i = 0usize;
    while i + 4 <= bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0 && upper[i..].starts_with(" AS ") && i + 4 <= item.len() {
            let name = item[i + 4..].trim().trim_matches('"');
            if !name.is_empty() {
                return Some(name.to_string());
            }
        }
        i += 1;
    }

    // A bare or qualified column keeps its own name: `d.id` is `id`.
    let bare = item.trim().trim_matches('"');
    if bare.contains('(') || bare.contains(' ') || bare == "*" {
        return None;
    }
    let last = bare.rsplit('.').next()?.trim_matches('"');
    if last.is_empty() || !last.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return None;
    }
    Some(last.to_string())
}

/// The select-list text of a `SELECT … FROM …`, for naming the columns.
fn select_list_of(body: &str) -> Option<&str> {
    let upper = body.to_ascii_uppercase();
    if !upper.trim_start().starts_with("SELECT") {
        return None;
    }
    let start = upper.find("SELECT")? + "SELECT".len();
    // Whole-word, not the literal `" FROM "`: a CTE body written across lines
    // puts `FROM` at the start of one, preceded by a newline rather than a
    // space, and the literal search then found no `FROM` at all — so the body
    // was reported as "must be a SELECT" when it plainly was one. Every ORM
    // and every Rust raw-string query formats across lines. `find_keyword`
    // already respects quoting and nesting, which is what this used to
    // hand-roll.
    let at = crate::core::rql::join::find_keyword(body, "FROM", start)?;
    Some(&body[start..at])
}

/// The constant literals a `RETURNING` clause lists, when every item is one.
///
/// `RETURNING 1` publishes one row of that constant per row written — the count
/// is the meaning, and no column of the table is involved. Resolving `1` against
/// the schema fails, so the clause is dropped and the constants are rebuilt
/// from the statement's row count instead.
fn returning_constants(body: &str) -> Option<Vec<String>> {
    let (_, tail) = crate::core::rql::lexer_ir::returning_split(body)?;
    let items = crate::core::rql::lexer_ct::split_definitions(tail);
    if items.is_empty() {
        return None;
    }
    let mut literals = Vec::with_capacity(items.len());
    for item in &items {
        let item = item.trim();
        let is_literal = item.starts_with('\'')
            || item
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_digit() || c == '-')
            || item.eq_ignore_ascii_case("NULL");
        if !is_literal {
            return None;
        }
        literals.push(item.to_string());
    }
    Some(literals)
}

/// The column names a data-modifying CTE exposes, taken from its `RETURNING`
/// clause.
///
/// `WITH x AS (INSERT … RETURNING id)` publishes `id` to the statement that
/// follows, exactly as a SELECT publishes its select list.
fn returning_labels(body: &str) -> Option<Vec<String>> {
    let (_, tail) = crate::core::rql::lexer_ir::returning_split(body)?;
    crate::core::rql::lexer_ct::split_definitions(tail)
        .iter()
        .map(|item| item_label(item))
        .collect::<Option<Vec<String>>>()
        .filter(|labels| !labels.is_empty())
}

/// The SQL type name that reads back as `db_type`, for the temporary table.
fn sql_type_of(db_type: &DbType) -> Option<&'static str> {
    Some(match db_type {
        DbType::I8 | DbType::I16 | DbType::I32 => "INTEGER",
        DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32 => "BIGINT",
        // Their own widths: as `BIGINT` a `UBIGINT` above `i64::MAX` read
        // back negative, a 16-byte integer was cut to eight, and the column
        // advertised `int8` where the base table's said `numeric`.
        DbType::U64 => "UBIGINT",
        DbType::I128 => "HUGEINT",
        DbType::U128 => "UHUGEINT",
        DbType::F32 | DbType::F64 => "DOUBLE",
        DbType::DECIMAL => "NUMERIC",
        DbType::BOOL => "BOOLEAN",
        DbType::UUID => "UUID",
        DbType::DATETIME => "TIMESTAMP",
        DbType::STRING | DbType::CHAR => "TEXT",
        // Every remaining stored type, named as `parse_column_type` accepts it.
        // A materialised subquery has to be able to hold whatever a select list
        // can produce: a `SELECT COUNT(*) FROM (SELECT …) AS sub_query` — which
        // is how a paginated count is written — carries every column of the
        // inner query through a real table, so a type missing here fails the
        // whole statement rather than just that column.
        DbType::JSONB | DbType::JSON => "JSONB",
        DbType::INET => "INET",
        DbType::BYTEA => "BYTEA",
        DbType::ARRAY => "ARRAY",
        // NULL is not a type, it is the absence of a value in the row that was
        // sampled. The caller resolves it by looking at further rows.
        DbType::NULL => return None,
        _ => return None,
    })
}

/// The prefix every materialised CTE and derived table's name carries.
///
/// Public because it is the one thing that identifies a relation as
/// statement-scoped from the outside. `Database::seed_in` reads it to give such
/// a table memory-backed storage instead of files, and `pool_place` reads it to
/// record no placement for one: a relation that is created and dropped inside a
/// single statement can never be read after that statement ends, so nothing it
/// writes has to reach a disk.
pub const TEMP_TABLE_PREFIX: &str = "__cte_";

/// A temporary table name that cannot collide with a user's table or with
/// another execution — including another session running the *same* statement.
///
/// The `__cte_` prefix keeps it out of the user's namespace, which is also what
/// makes a CTE that shadows a real table safe: references are rewritten to this
/// name, so the real table is untouched.
///
/// The name mixes in per-execution entropy as well as the statement text. It
/// used to hash the text alone, which made two sessions running one statement
/// agree on a name: both created the same table, and whichever finished first
/// dropped it out from under the other — "Table not found", then "could not
/// drop the temporary table … does not exist". A paginated count run twice at
/// once is enough to hit it. The name is derived once per execution and carried
/// through the rewrite and the drop, so it does not need to be reproducible.
fn temp_name(cte: &str, statement: &str, index: usize) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in statement.bytes().chain(cte.bytes()) {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash ^= fastrand::u64(..);
    let safe: String = cte
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .take(24)
        .collect();
    format!("{}{}_{}_{:x}", TEMP_TABLE_PREFIX, safe, index, hash)
}

/// Replaces references to a CTE *as a relation*, leaving same-named columns
/// alone.
///
/// A CTE may share its name with a column — `WITH domain_id AS (…) INSERT INTO
/// users (…, domain_id, …) SELECT …, domain_id.id, … FROM domain_id` is real
/// migration SQL. Replacing every occurrence rewrote the **column list** too,
/// shifting the insert's columns and surfacing as "column 'Zapier Integration'
/// not found in schema" — the select list read against the wrong columns.
///
/// A name is a relation reference when it follows `FROM`/`JOIN`, or when it
/// qualifies something (`domain_id.id`). A name *preceded* by a dot is a column
/// of something else and is left as it is.
fn replace_relation_references(sql: &str, from: &str, to: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let upper = sql.to_ascii_uppercase();
    let mut i = 0usize;
    let mut quote: Option<u8> = None;
    // The last word seen outside quotes, to spot `FROM x` / `JOIN x`.
    let mut previous_word = String::new();
    // Whether we are inside a `FROM` clause, where a comma separates relations.
    //
    // `FROM a, b` is an implicit cross join, so `b` is a relation position just
    // as `a` is — but a comma alone cannot mean that, or an unqualified select
    // item sharing a CTE's name would be rewritten too. The clause keywords
    // below are what end the list.
    let mut in_from = false;
    let mut relation_position = false;

    while i < bytes.len() {
        let c = bytes[i];
        if let Some(q) = quote {
            out.push(c as char);
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        if c == b'\'' || c == b'"' {
            quote = Some(c);
            out.push(c as char);
            i += 1;
            continue;
        }

        if c.is_ascii_alphanumeric() || c == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let word = &sql[start..i];
            let preceded_by_dot = start > 0 && bytes[start - 1] == b'.';
            let qualifies = bytes.get(i) == Some(&b'.');
            // `USING` names relations too — `DELETE … USING victims v` — but
            // only outside parentheses: in `JOIN b USING (id)` the word after
            // it is a column, and the paren is what tells the two apart.
            let preceded_by_paren = start > 0 && bytes[start - 1] == b'(';
            let after_relation_keyword = matches!(previous_word.as_str(), "FROM" | "JOIN")
                || (previous_word.as_str() == "USING" && !preceded_by_paren);

            if word.eq_ignore_ascii_case(from)
                && !preceded_by_dot
                && (qualifies || after_relation_keyword || relation_position)
            {
                out.push_str(to);
            } else {
                out.push_str(word);
            }
            previous_word = upper[start..i].to_string();

            // A word consumes the relation position a comma opened.
            relation_position = false;
            match previous_word.as_str() {
                "FROM" | "USING" => in_from = true,
                "WHERE" | "GROUP" | "HAVING" | "ORDER" | "LIMIT" | "OFFSET" | "UNION"
                | "INTERSECT" | "EXCEPT" | "RETURNING" | "SET" | "VALUES" => in_from = false,
                _ => {}
            }
            continue;
        }

        if c == b',' && in_from {
            relation_position = true;
        }
        out.push(c as char);
        i += 1;
    }
    out
}

/// Runs each CTE, stores it in a temporary table, and rewrites the statement to
/// read from those tables.
///
/// `Ok(None)` when there is no `WITH` clause. The caller **must** drop
/// [`Materialized::temps`] on every exit path: DDL is not transactional here,
/// so a failed outer statement leaves the temporary tables behind to collide
/// with the next run.
/// Whether a statement opens with a `WITH` clause.
///
/// Cheap enough to ask before deciding whether a statement needs a transaction
/// of its own, which is the one thing a caller must know before parsing.
pub fn has_with_clause(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    trimmed.len() >= 4
        && trimmed[..4].eq_ignore_ascii_case("WITH")
        && trimmed[4..].starts_with(|c: char| c.is_whitespace() || c == '(')
}

pub async fn materialize(
    sql: &str,
    database: &Arc<Database>,
    // The statement's transaction. A data-modifying CTE runs inside it, so its
    // writes stand or fall with the statement rather than committing on their
    // own — `FROZEN_TX` means there is none and each body autocommits.
    tx: crate::core::mvcc::transaction::TxId,
) -> Result<Option<Materialized>, String> {
    let mut temps: Vec<String> = Vec::new();
    // A statement with several CTEs can fail on a later one having already
    // materialised the earlier ones. The caller only drops what it is handed,
    // and an `Err` hands it nothing — so the tables created so far are dropped
    // here before the error propagates. Without this, one failing statement
    // leaks a full table per CTE that had already succeeded.
    match materialize_inner(sql, database, &mut temps, tx).await {
        Ok(result) => Ok(result),
        Err(e) => {
            drop_temps(database, &temps, tx).await;
            Err(e)
        }
    }
}

async fn materialize_inner(
    sql: &str,
    database: &Arc<Database>,
    temps: &mut Vec<String>,
    tx: crate::core::mvcc::transaction::TxId,
) -> Result<Option<Materialized>, String> {
    let Some((defs, rest)) = split_with(sql)? else {
        return Ok(None);
    };
    if rest.trim().is_empty() {
        return Err("a WITH clause with no statement after it".to_string());
    }

    let mut rewritten = rest;

    // Which temporary table each CTE published, by name.
    //
    // Deliberately not `temps` positionally zipped against `defs`: `temps` also
    // holds the tables a *derived table inside a body* needed, and a CTE that
    // publishes nothing pushes none at all — so position `i` is not CTE `i`.
    // It was, until a body containing `FROM (SELECT …) x` added an entry ahead
    // of its own, and every later CTE then read the derived table instead of
    // the CTE it named. The rows were real and the query looked fine.
    let mut published: Vec<(String, String)> = Vec::new();

    for (index, def) in defs.iter().enumerate() {
        // Earlier CTEs are visible to later ones, so rewrite the body too.
        let mut body = def.body.clone();
        for (name, temp) in &published {
            body = replace_relation_references(&body, name, temp);
        }

        // A CTE body may modify data — `WITH x AS (INSERT … RETURNING id)` —
        // in which case it runs for its effect and publishes the RETURNING
        // rows. PostgreSQL runs such a CTE exactly once, which is what
        // materialising in order already gives.
        let modifies = !matches!(
            body.trim_start()
                .get(..6)
                .map(|w| w.to_ascii_uppercase())
                .as_deref(),
            Some("SELECT")
        );

        // `RETURNING <constants>` names no column of the table, so the clause is
        // dropped and the constants rebuilt from the row count.
        if modifies {
            if let Some(literals) = returning_constants(&body) {
                let (head, _) = crate::core::rql::lexer_ir::returning_split(&body)
                    .ok_or_else(|| format!("the WITH clause '{}' lost its RETURNING", def.name))?;
                let affected =
                    crate::core::rql::executor::run_statement_affected(head, database, tx)
                        .await
                        .map_err(|e| format!("the WITH clause '{}' failed: {}", def.name, e))?;

                let labels: Vec<String> = (0..literals.len())
                    .map(|i| format!("col{}", i + 1))
                    .collect();
                let temp = temp_name(&def.name, sql, index);
                let columns: Vec<String> =
                    labels.iter().map(|name| format!("{} TEXT", name)).collect();
                crate::core::rql::executor::run_statement(
                    &format!("CREATE TABLE {} ({})", temp, columns.join(", ")),
                    database,
                    tx,
                )
                .await
                .map_err(|e| {
                    format!(
                        "could not materialise the WITH clause '{}': {}",
                        def.name, e
                    )
                })?;
                temps.push(temp.clone());

                if affected > 0 {
                    let tuple = format!("({})", literals.join(", "));
                    let tuples: Vec<String> = (0..affected).map(|_| tuple.clone()).collect();
                    crate::core::rql::executor::run_statement(
                        &format!(
                            "INSERT INTO {} ({}) VALUES {}",
                            temp,
                            labels.join(", "),
                            tuples.join(", ")
                        ),
                        database,
                        tx,
                    )
                    .await
                    .map_err(|e| format!("could not fill the WITH clause '{}': {}", def.name, e))?;
                }

                rewritten = replace_relation_references(&rewritten, &def.name, &temp);
                // Registered by name so a later CTE referencing this one finds
                // it, exactly as the ordinary path does.
                published.push((def.name.clone(), temp));
                continue;
            }
        }

        // A CTE body may itself select from a derived table. Those are
        // materialised in the statement path, which runs *after* this — so a
        // body reaching the scan with `FROM (SELECT …) x` still in it was
        // reported as `relation '(' does not exist`. Rewriting it here, before
        // the body runs, is the only point at which it can be caught.
        let derived = materialize_derived(&body, database, tx).await?;
        let body_owned;
        let body = match &derived {
            Some(d) => {
                body_owned = d.rewritten.clone();
                temps.extend(d.temps.iter().cloned());
                body_owned.as_str()
            }
            None => body.as_str(),
        };

        let rows = if modifies {
            crate::core::rql::executor::run_returning_rows(body, database, tx).await
        } else {
            crate::core::rql::executor::run_query_rows(body, database, tx).await
        }
        .map_err(|e| format!("the WITH clause '{}' failed: {}", def.name, e))?;

        // A data-modifying CTE with no `RETURNING` is legal and runs purely for
        // its effect. It publishes no columns, so it cannot be referenced and
        // needs no temporary table — running it is the whole job.
        if modifies && crate::core::rql::lexer_ir::returning_split(body).is_none() {
            crate::core::rql::executor::run_statement(body, database, tx)
                .await
                .map_err(|e| format!("the WITH clause '{}' failed: {}", def.name, e))?;
            continue;
        }

        let labels: Vec<String> = if modifies {
            returning_labels(&body).ok_or_else(|| {
                format!(
                    "the WITH clause '{}' modifies data but has no RETURNING clause to name \
                     its columns",
                    def.name
                )
            })?
        } else {
            let list = select_list_of(&body)
                .ok_or_else(|| format!("the WITH clause '{}' must be a SELECT", def.name))?;
            crate::core::rql::lexer_ct::split_definitions(list)
                .iter()
                .map(|item| item_label(item))
                .collect::<Option<Vec<String>>>()
                .ok_or_else(|| {
                    format!(
                        "the WITH clause '{}' has a column this cannot name; alias it with AS",
                        def.name
                    )
                })?
        };

        // Types come from the rows when there are any. An empty result still
        // needs a table for the outer statement to parse against, and nothing
        // is read from it, so TEXT is a safe placeholder.
        let types: Vec<&'static str> = match rows.first() {
            Some(row) => {
                // A scan fetches the columns its `WHERE` reads as well as the
                // selected ones, so a row may carry more than the select list
                // names. The visible columns come first, which is what
                // `visible_columns` trims to elsewhere.
                if row.columns.len() < labels.len() {
                    return Err(format!(
                        "the WITH clause '{}' returned {} columns but names {}",
                        def.name,
                        row.columns.len(),
                        labels.len()
                    ));
                }
                row.columns
                    .iter()
                    .take(labels.len())
                    .map(|c| sql_type_of(&c.column_type))
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| {
                        format!(
                            "the WITH clause '{}' returned a type that cannot be materialised",
                            def.name
                        )
                    })?
            }
            None => labels.iter().map(|_| "TEXT").collect(),
        };

        let temp = temp_name(&def.name, sql, index);
        let columns: Vec<String> = labels
            .iter()
            .zip(types.iter())
            .map(|(name, ty)| format!("{} {}", name, ty))
            .collect();

        // Timed from here rather than from the top of the loop: the body has
        // already run, and a body containing its own derived table has already
        // materialised that through `materialize_query`, which counts itself.
        // Starting the clock earlier would count the nested relation twice.
        #[cfg(feature = "query_stats")]
        let materialising = std::time::Instant::now();

        let create = format!("CREATE TABLE {} ({})", temp, columns.join(", "));
        crate::core::rql::executor::run_statement(&create, database, tx)
            .await
            .map_err(|e| {
                format!(
                    "could not materialise the WITH clause '{}': {}",
                    def.name, e
                )
            })?;
        // Recorded before the insert, so a failure part-way still gets cleaned
        // up by the caller.
        temps.push(temp.clone());

        if !rows.is_empty() {
            let mut tuples: Vec<String> = Vec::with_capacity(rows.len());
            for row in &rows {
                let mut parts: Vec<String> = Vec::with_capacity(labels.len());
                for column in row.columns.iter().take(labels.len()) {
                    let literal = crate::core::rql::subquery::value_to_literal(
                        &column.column_type,
                        column.data.into_slice(),
                    )
                    .ok_or_else(|| {
                        format!(
                            "the WITH clause '{}' returned a value that cannot be stored",
                            def.name
                        )
                    })?;
                    parts.push(literal);
                }
                tuples.push(format!("({})", parts.join(", ")));
            }
            let insert = format!(
                "INSERT INTO {} ({}) VALUES {}",
                temp,
                labels.join(", "),
                tuples.join(", ")
            );
            crate::core::rql::executor::run_statement(&insert, database, tx)
                .await
                .map_err(|e| format!("could not fill the WITH clause '{}': {}", def.name, e))?;
        }

        // One relation built. A CTE that failed part-way is not counted — the
        // `?` above carries the error out — which is the right answer: nothing
        // usable was produced, and the statement is recorded as the failure it
        // was.
        #[cfg(feature = "query_stats")]
        crate::core::query_stats::note_materialised(
            materialising.elapsed().as_micros().min(u64::MAX as u128) as u64,
        );

        rewritten = replace_relation_references(&rewritten, &def.name, &temp);
        published.push((def.name.clone(), temp));
    }

    Ok(Some(Materialized {
        rewritten,
        temps: std::mem::take(temps),
    }))
}

/// Materialises one query into a temporary table and returns its name.
///
/// The same mechanism a CTE uses, exposed for `UPDATE … FROM (subquery) alias`,
/// where the derived table has to become a real relation before the join can
/// read it. The caller owns the result and must drop it — see [`drop_temps`].
/// The declared type of each output column, for the columns whose real type a
/// scan cannot report.
///
/// `DbType::physical()` maps `JSONB`, `BYTEA` and `ARRAY` all to `STRING`, and
/// a scan types its cells physically — so sampling the returned rows says
/// `TEXT` for all three. Materialising a derived table on that sample created
/// the temp relation's column as `TEXT`, and the outer select then described
/// the column from the temp relation (text) while the DESCRIBE path resolved
/// the inner query properly and said `jsonb`. The document went out as raw
/// stored bytes under a `jsonb` description: "unsupported JSONB format version
/// 7", 7 being the encoded object's tag. `paginate()` wraps selects in derived
/// tables, so this is an ordinary shape rather than an exotic one.
///
/// Only those three types are recovered, and only when the inner query selects
/// bare columns from one real relation. Anything else keeps the sampled type —
/// in particular a rebuilt (shredded) document, whose declared type is the
/// `UUID` it is stored as and whose sampled `JSON` is the correct answer.
fn declared_wide_types(
    query: &str,
    labels: &[String],
    database: &Arc<Database>,
) -> Vec<Option<DbType>> {
    let none = vec![None; labels.len()];
    // A join has more than one relation behind its columns; this resolves only
    // the single-relation case rather than guessing which one a label meant.
    if crate::core::rql::join::statement_has_join(query) {
        return none;
    }
    let Some(at) = crate::core::rql::join::find_keyword(query, "FROM", 0) else {
        return none;
    };
    let rest = query[at + "FROM".len()..].trim_start();
    let name = rest
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_end_matches(|c: char| c == ';' || c == ',')
        .trim_matches('"');
    if name.is_empty() || name.contains('(') {
        return none;
    }
    let Some(table) = database.tables.get(name) else {
        return none;
    };
    labels
        .iter()
        .map(|label| {
            let field = table
                .schema
                .fields
                .iter()
                .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(label))?;
            match field.db_type {
                DbType::JSONB | DbType::BYTEA | DbType::ARRAY => Some(field.db_type.clone()),
                _ => None,
            }
        })
        .collect()
}

/// Which output columns of `query`, by position, are stored documents, for
/// the callers that compare its rows by value: `UNION`'s duplicate check and
/// `IN (subquery)`. A scan reports a stored document as text, so without this
/// `{"a":1}` and `{"a":1.0}` were two rows to `UNION`, and an `IN` list was
/// built from the stored bytes as if they were text.
///
/// The same resolution as [`declared_wide_types`]: bare columns of one
/// relation. Anything it cannot resolve is `false` and compares as before.
pub(crate) fn document_columns(query: &str, database: &Arc<Database>) -> Vec<bool> {
    let Some(list) = select_list_of(query) else {
        return Vec::new();
    };
    let list = list.trim_start();
    let list = match list.get(.."DISTINCT ".len()) {
        Some(head) if head.eq_ignore_ascii_case("DISTINCT ") => &list["DISTINCT ".len()..],
        _ => list,
    };
    let labels: Vec<String> = crate::core::rql::lexer_ct::split_definitions(list)
        .iter()
        .map(|item| item_label(item).unwrap_or_default())
        .collect();
    declared_wide_types(query, &labels, database)
        .into_iter()
        .map(|t| t == Some(DbType::JSONB))
        .collect()
}

pub async fn materialize_query(
    query: &str,
    alias: &str,
    statement: &str,
    database: &Arc<Database>,
    tx: crate::core::mvcc::transaction::TxId,
) -> Result<String, String> {
    // An explicit `return`, matching the same shape in `pgwire`: two cfg'd
    // blocks in tail position depend on cfg-stripping to decide which one is
    // the tail expression, and saying so outright costs nothing.
    #[cfg(not(feature = "query_stats"))]
    {
        return materialize_query_inner(query, alias, statement, database, tx).await;
    }

    // A wrapper rather than a timer threaded through the body, because the body
    // returns early through `?` in a dozen places and a count that a failure
    // path could skip is a count nobody can subtract from a duration.
    #[cfg(feature = "query_stats")]
    {
        let started = std::time::Instant::now();
        let result = materialize_query_inner(query, alias, statement, database, tx).await;
        // Only a relation that was actually built, which is the same rule the
        // `WITH` path follows: a materialisation that failed produced nothing
        // to have been slow *for*, and the statement carrying it is recorded as
        // the failure it was.
        if result.is_ok() {
            crate::core::query_stats::note_materialised(
                started.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );
        }
        result
    }
}

async fn materialize_query_inner(
    query: &str,
    alias: &str,
    statement: &str,
    database: &Arc<Database>,
    tx: crate::core::mvcc::transaction::TxId,
) -> Result<String, String> {
    let rows = crate::core::rql::executor::run_query_rows(query, database, tx)
        .await
        .map_err(|e| format!("the subquery for '{}' failed: {}", alias, e))?;

    let list = select_list_of(query)
        .ok_or_else(|| format!("the subquery for '{}' must be a SELECT", alias))?;
    let labels: Vec<String> = crate::core::rql::lexer_ct::split_definitions(list)
        .iter()
        .map(|item| item_label(item))
        .collect::<Option<Vec<String>>>()
        .ok_or_else(|| {
            format!(
                "the subquery for '{}' has a column this cannot name; alias it with AS",
                alias
            )
        })?;

    // The type of each column, taken from the first row that actually has a
    // value there.
    //
    // Sampling only `rows.first()` was not enough: a column that happens to be
    // NULL in the first row arrives as `DbType::NULL`, which is the absence of
    // a value rather than a type, and the whole statement failed on it. Every
    // row is consulted, and a column that is NULL in all of them becomes TEXT —
    // it holds nothing, so the choice cannot lose anything.
    let declared = declared_wide_types(query, &labels, database);
    let types: Vec<&'static str> = (0..labels.len())
        .map(|index| {
            let sampled = rows
                .iter()
                .filter_map(|row| row.columns.get(index))
                .find_map(|c| sql_type_of(&c.column_type));
            let widened = declared
                .get(index)
                .and_then(|w| w.as_ref())
                .and_then(sql_type_of);
            match sampled {
                // `TEXT` is what a scan reports for every variable-width type,
                // so it is the one answer worth second-guessing.
                Some("TEXT") | None => widened.unwrap_or("TEXT"),
                Some(other) => other,
            }
        })
        .collect();

    let temp = temp_name(alias, statement, 0);
    let columns: Vec<String> = labels
        .iter()
        .zip(types.iter())
        .map(|(name, ty)| format!("{} {}", name, ty))
        .collect();
    crate::core::rql::executor::run_statement(
        &format!("CREATE TABLE {} ({})", temp, columns.join(", ")),
        database,
        tx,
    )
    .await
    .map_err(|e| format!("could not materialise '{}': {}", alias, e))?;

    if !rows.is_empty() {
        let mut tuples: Vec<String> = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut parts: Vec<String> = Vec::with_capacity(labels.len());
            for (index, column) in row.columns.iter().take(labels.len()).enumerate() {
                // The literal has to be written in the same type the column was
                // created as. A scan reports a stored document as `STRING`, so
                // rendering it that way quoted the raw encoded bytes and the
                // insert into the (correctly) `JSONB` column was refused as a
                // schema mismatch — the type had to be recovered on both sides
                // or on neither.
                let as_type = match declared.get(index).and_then(|w| w.clone()) {
                    Some(wide) if column.column_type == DbType::STRING => wide,
                    _ => column.column_type.clone(),
                };
                parts.push(
                    crate::core::rql::subquery::value_to_literal(
                        &as_type,
                        column.data.into_slice(),
                    )
                    .ok_or_else(|| {
                        // Naming the type and the column is the difference
                        // between a five-minute fix and a hunt: the message
                        // without them says only that something, somewhere in
                        // the select list, has no literal form.
                        format!(
                            "the subquery for '{}' returned a {:?} value in column '{}' that cannot be stored",
                            alias,
                            as_type,
                            labels.get(index).map(String::as_str).unwrap_or("?"),
                        )
                    })?,
                );
            }
            tuples.push(format!("({})", parts.join(", ")));
        }
        crate::core::rql::executor::run_statement(
            &format!(
                "INSERT INTO {} ({}) VALUES {}",
                temp,
                labels.join(", "),
                tuples.join(", ")
            ),
            database,
            tx,
        )
        .await
        .map_err(|e| format!("could not fill '{}': {}", alias, e))?;
    }

    Ok(temp)
}

/// Drops the temporary tables a statement materialised. Best effort: a failure
/// here must not mask the statement's own result.
pub async fn drop_temps(
    database: &Arc<Database>,
    temps: &[String],
    tx: crate::core::mvcc::transaction::TxId,
) {
    for temp in temps {
        let sql = format!("DROP TABLE {}", temp);
        if let Err(e) = crate::core::rql::executor::run_statement(&sql, database, tx).await {
            log::warn!("could not drop the temporary table {}: {}", temp, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recursive_is_refused_by_keyword() {
        // The one that must never be approximated: run as non-recursive it
        // returns the base case and no error.
        let err = split_with("WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t").unwrap_err();
        assert!(err.contains("RECURSIVE"), "{}", err);
        // A CTE merely *named* something starting with "recursive" is fine.
        let ok =
            split_with("WITH recursive_totals AS (SELECT a FROM b) SELECT a FROM recursive_totals");
        assert!(ok.is_ok(), "{:?}", ok.err());
    }

    #[test]
    fn a_statement_without_with_is_left_alone() {
        assert!(split_with("SELECT 1 FROM t").unwrap().is_none());
        // `withdrawals` must not read as the keyword.
        assert!(split_with("SELECT x FROM withdrawals").unwrap().is_none());
    }

    #[test]
    fn splits_one_and_several_definitions() {
        let (defs, rest) = split_with("WITH a AS (SELECT id FROM t) SELECT id FROM a")
            .unwrap()
            .unwrap();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].name, "a");
        assert_eq!(defs[0].body, "SELECT id FROM t");
        assert_eq!(rest, "SELECT id FROM a");

        let (defs, rest) = split_with(
            "WITH a AS (SELECT id FROM t), b AS (SELECT id FROM u) INSERT INTO v (id) SELECT id FROM b",
        )
        .unwrap()
        .unwrap();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[1].name, "b");
        assert!(rest.starts_with("INSERT INTO v"));
    }

    #[test]
    fn a_nested_paren_in_the_body_does_not_end_it() {
        let (defs, _) = split_with(
            "WITH a AS (SELECT id FROM t WHERE x IN (SELECT y FROM u)) SELECT id FROM a",
        )
        .unwrap()
        .unwrap();
        assert_eq!(defs.len(), 1);
        assert!(defs[0].body.ends_with("(SELECT y FROM u))") || defs[0].body.contains("IN ("));
    }

    #[test]
    fn labels_follow_postgres_naming() {
        assert_eq!(item_label("d.id").as_deref(), Some("id"));
        assert_eq!(item_label("id").as_deref(), Some("id"));
        assert_eq!(item_label("\"id\"").as_deref(), Some("id"));
        assert_eq!(item_label("count(*) AS n").as_deref(), Some("n"));
        // Unnameable rather than invented: the outer query refers to these by
        // name, so guessing would bind the wrong column.
        assert_eq!(item_label("count(*)"), None);
        assert_eq!(item_label("*"), None);
    }

    #[test]
    fn only_relation_references_are_replaced() {
        let f = replace_relation_references;
        assert_eq!(f("SELECT a FROM cte", "cte", "__t"), "SELECT a FROM __t");
        assert_eq!(
            f("SELECT a FROM x JOIN cte ON 1", "cte", "__t"),
            "SELECT a FROM x JOIN __t ON 1"
        );
        // Qualifying something is a relation reference too.
        assert_eq!(
            f("SELECT cte.id FROM cte", "cte", "__t"),
            "SELECT __t.id FROM __t"
        );
        // A longer identifier merely starting with the name is untouched.
        assert_eq!(
            f("SELECT a FROM cte_extra", "cte", "__t"),
            "SELECT a FROM cte_extra"
        );
        // Inside a literal it is data.
        assert_eq!(
            f("SELECT 'cte' FROM cte", "cte", "__t"),
            "SELECT 'cte' FROM __t"
        );
    }

    #[test]
    fn a_column_sharing_the_ctes_name_is_left_alone() {
        // Real migration SQL: a CTE named `domain_id` beside a column of the
        // same name. Rewriting the column list shifted the insert's columns and
        // surfaced as a bogus "column not found" about a string literal.
        let sql = "INSERT INTO users (id, domain_id, name) \
                   SELECT domain_id.id, 'x' FROM domain_id";
        let out = replace_relation_references(sql, "domain_id", "__cte_d");
        assert!(
            out.contains("(id, domain_id, name)"),
            "column list changed: {}",
            out
        );
        assert!(
            out.contains("__cte_d.id"),
            "qualified ref not rewritten: {}",
            out
        );
        assert!(
            out.contains("FROM __cte_d"),
            "relation not rewritten: {}",
            out
        );
    }

    #[test]
    fn a_column_qualified_by_another_relation_is_left_alone() {
        // `users.domain_id` is a column of `users`, not the CTE.
        let out = replace_relation_references(
            "SELECT users.domain_id FROM domain_id",
            "domain_id",
            "__cte_d",
        );
        assert!(out.contains("users.domain_id"), "{}", out);
        assert!(out.contains("FROM __cte_d"), "{}", out);
    }

    #[test]
    fn temp_names_are_prefixed_and_never_shared() {
        let a = temp_name("roles", "WITH roles AS (SELECT 1) SELECT 1", 0);
        let b = temp_name("roles", "WITH roles AS (SELECT 2) SELECT 2", 0);
        assert!(a.starts_with("__cte_roles_0_"));
        assert_ne!(a, b);

        // The same statement twice must *also* differ. Two sessions running one
        // statement used to agree on a name, so one dropped the table the other
        // was reading — which is how a paginated count run concurrently failed
        // with "Table not found".
        let one = "WITH roles AS (SELECT 1) SELECT 1";
        assert_ne!(temp_name("roles", one, 0), temp_name("roles", one, 0));
    }

    #[test]
    fn a_with_clause_that_renames_its_columns_is_refused() {
        let err = split_with("WITH a (x, y) AS (SELECT 1, 2) SELECT x FROM a").unwrap_err();
        assert!(err.contains("renames its columns"), "{}", err);
    }
}

/// Materialises every derived table — `FROM (SELECT …) alias` — into a
/// temporary table and points the statement at it.
///
/// The same treatment a CTE gets, for the same reason: the scan reads one
/// relation at a time, so a subquery standing where a relation should be has to
/// become one. Without this the FROM parser took `(SELECT` for a relation name
/// and reported `relation '(SELECT' does not exist`.
///
/// Returns `None` when the statement has no derived table.
pub async fn materialize_derived(
    sql: &str,
    database: &Arc<Database>,
    tx: crate::core::mvcc::transaction::TxId,
) -> Result<Option<Materialized>, String> {
    let mut temps: Vec<String> = Vec::new();
    match materialize_derived_inner(sql, database, &mut temps, tx).await {
        Ok(result) => Ok(result),
        Err(e) => {
            // Whatever was created before the failure is dropped here: the
            // caller is handed nothing by an `Err` and so cannot clean up.
            drop_temps(database, &temps, tx).await;
            Err(e)
        }
    }
}

async fn materialize_derived_inner(
    sql: &str,
    database: &Arc<Database>,
    temps: &mut Vec<String>,
    tx: crate::core::mvcc::transaction::TxId,
) -> Result<Option<Materialized>, String> {
    let mut current = sql.to_string();
    let mut found_any = false;

    // Bounded: each pass replaces one derived table with a name, so the count
    // strictly decreases. The cap is a backstop against a rewrite that fails to
    // shrink rather than a real limit on how many a statement may have.
    for index in 0..16usize {
        let Some((start, end, inner, alias)) = next_derived_table(&current) else {
            break;
        };
        found_any = true;

        let temp = materialize_query(&inner, &alias, &current, database, tx).await?;
        temps.push(temp.clone());
        current = format!("{}{} {}{}", &current[..start], temp, alias, &current[end..]);
        let _ = index;
    }

    if !found_any {
        return Ok(None);
    }
    Ok(Some(Materialized {
        rewritten: current,
        temps: std::mem::take(temps),
    }))
}

/// Finds the first `FROM (SELECT …) alias` or `JOIN (SELECT …) alias`.
///
/// Returns the byte range covering `(…) alias`, the subquery text, and the
/// alias. A parenthesised *expression* is not a derived table, so the group has
/// to begin with `SELECT` to qualify.
/// The next derived table in , as .
///
/// Exposed so the describe path can resolve a derived table's columns without
/// materialising it — describe must have no side effects, and the alias's
/// schema is decided by the inner query's select list alone.
pub fn next_derived_table(sql: &str) -> Option<(usize, usize, String, String)> {
    for keyword in ["FROM", "JOIN"] {
        let mut from = 0usize;
        while let Some(at) = crate::core::rql::join::find_keyword(sql, keyword, from) {
            let after_keyword = at + keyword.len();
            let rest = &sql[after_keyword..];
            let lead = rest.len() - rest.trim_start().len();
            let open = after_keyword + lead;

            if sql.as_bytes().get(open) != Some(&b'(') {
                from = after_keyword;
                continue;
            }
            let inner_start = open + 1;
            let Some(close) = matching_paren(sql, open) else {
                from = after_keyword;
                continue;
            };
            let inner = sql[inner_start..close].trim();
            if !inner.to_ascii_uppercase().starts_with("SELECT")
                && !inner.to_ascii_uppercase().starts_with("WITH")
            {
                from = after_keyword;
                continue;
            }

            // `) alias` or `) AS alias`.
            let tail = &sql[close + 1..];
            let tail_trimmed = tail.trim_start();
            let lead_alias = tail.len() - tail_trimmed.len();
            let mut words = tail_trimmed.split_whitespace();
            let first = words.next().unwrap_or("");
            let (alias, alias_len) = if first.eq_ignore_ascii_case("AS") {
                let name = words.next().unwrap_or("");
                (name, first.len() + 1 + name.len())
            } else {
                (first, first.len())
            };
            let alias = alias.trim_matches('"');
            if alias.is_empty()
                || !alias
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            {
                // PostgreSQL requires one, and guessing a name would make the
                // rewritten statement refer to something the caller never wrote.
                return None;
            }

            return Some((
                open,
                close + 1 + lead_alias + alias_len,
                inner.to_string(),
                alias.to_string(),
            ));
        }
    }
    None
}

#[cfg(test)]
mod derived_tests {
    use super::*;

    #[test]
    fn a_derived_table_is_found_with_its_alias() {
        let (start, end, inner, alias) =
            next_derived_table("SELECT id FROM (SELECT id FROM t WHERE a = 1) ranked WHERE x")
                .expect("found");
        assert_eq!(inner, "SELECT id FROM t WHERE a = 1");
        assert_eq!(alias, "ranked");
        let sql = "SELECT id FROM (SELECT id FROM t WHERE a = 1) ranked WHERE x";
        assert_eq!(&sql[start..end], "(SELECT id FROM t WHERE a = 1) ranked");
    }

    #[test]
    fn the_as_spelling_is_accepted() {
        let (_, _, _, alias) = next_derived_table("SELECT * FROM (SELECT 1) AS r").expect("found");
        assert_eq!(alias, "r");
    }

    #[test]
    fn a_join_against_a_derived_table_is_found() {
        let (_, _, inner, alias) =
            next_derived_table("SELECT * FROM a JOIN (SELECT id FROM b) c ON a.id = c.id")
                .expect("found");
        assert_eq!(inner, "SELECT id FROM b");
        assert_eq!(alias, "c");
    }

    #[test]
    fn a_parenthesised_expression_is_not_a_derived_table() {
        // Only a group that begins with SELECT is one; `WHERE (a OR b)` and a
        // plain relation list must be left alone.
        assert!(next_derived_table("SELECT * FROM t WHERE (a = 1 OR b = 2)").is_none());
        assert!(next_derived_table("SELECT * FROM t").is_none());
    }

    #[test]
    fn a_derived_table_without_an_alias_is_not_rewritten() {
        // PostgreSQL requires one. Inventing a name would point the rewritten
        // statement at something nobody wrote.
        assert!(next_derived_table("SELECT * FROM (SELECT 1)").is_none());
    }
}
