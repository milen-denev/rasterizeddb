//! Enumerated types, `CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')`.
//!
//! # Storage
//!
//! A type is a row of the per-database system table [`TYPE_TABLE`]: its name
//! and its labels in their order. The row is written with SQL in the
//! statement's transaction, as a view's is (`views`), so `ROLLBACK` undoes
//! it and a migration may create a type and a table using it in one block.
//!
//! # Columns
//!
//! A column of an enum type is a `text` column holding the label, with a
//! `CHECK` that it is one: `CONSTRAINT "__enum_mood__m" CHECK (m IN ('sad',
//! 'ok', 'happy'))`. The constraint's name says which type it stands for,
//! so the columns of a type are found from the tables' own constraints,
//! whatever was renamed since; `ALTER TYPE … ADD VALUE` replaces each of
//! them. The `__` prefix keeps it out of what a client lists, as PostgreSQL
//! lists no constraint for an enum.
//!
//! # Reading
//!
//! Before anything else reads a statement, [`rewrite`] turns each use of a
//! type into its stored form: `::mood` and `CAST(… AS mood)` are `text` (a
//! literal that is no label refused 22P02, as PostgreSQL refuses it), a
//! column declared `mood` is `text` with its constraint, and a top-level
//! `ORDER BY` of an enum column sorts by the labels' order, as PostgreSQL
//! does, not by their text.
//!
//! Not done (documented in docs/postgres-compatibility.md): `<` and `>`
//! between enum values compare the labels' text; a row sent to the client
//! types the column `text`.

use std::sync::atomic::{AtomicBool, Ordering};

use rclite::Arc;

use crate::core::database::Database;
use crate::core::mvcc::TxId;
use crate::core::sql_scan::{is_identifier_byte, matching_close, skip_comment, skip_quoted, split_top_level};

/// The system table types are kept in.
pub const TYPE_TABLE: &str = "__rdb_types";

const TYPE_TABLE_DDL: &str = "CREATE TABLE __rdb_types (name TEXT PRIMARY KEY, labels TEXT NOT NULL)";

/// The prefix of the constraint that keeps an enum column to its labels.
pub const CONSTRAINT_PREFIX: &str = "__enum_";

pub type Refusal = (&'static str, String);

fn refusal(code: &'static str, message: impl Into<String>) -> Refusal {
    (code, message.into())
}

/// An enum type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumType {
    pub name: String,
    pub labels: Vec<String>,
}

impl EnumType {
    /// The oid it is listed under: stable for a name, in a range no
    /// built-in type or relation uses. Its array type's is the next one.
    pub fn oid(&self) -> i64 {
        oid_of(&self.name)
    }

    fn label_list(&self) -> String {
        self.labels.iter().map(|l| crate::auth::quote(l)).collect::<Vec<_>>().join(", ")
    }
}

pub fn oid_of(name: &str) -> i64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    900_000_000 + ((h % 50_000_000) as i64) * 2
}

/// The names of the types a database has had: a superset of those any
/// snapshot sees, so a statement naming none costs a few substring checks.
#[derive(Default)]
pub struct TypeRegistry {
    names: dashmap::DashMap<String, ()>,
    loaded: AtomicBool,
}

impl TypeRegistry {
    fn note(&self, name: &str) {
        self.names.insert(name.to_string(), ());
    }

    fn mentioned_in(&self, sql: &str) -> Vec<String> {
        self.names
            .iter()
            .filter(|e| crate::core::sql_scan::contains_ignore_ascii_case(sql, e.key().as_bytes()))
            .map(|e| e.key().clone())
            .collect()
    }
}

/// A type statement, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeStatement {
    Create { name: String, labels: Vec<String> },
    Drop { names: Vec<String>, if_exists: bool, cascade: bool },
    AddValue { name: String, label: String, if_not_exists: bool, place: Option<(bool, String)> },
    Rename { name: String, to: String },
    RenameValue { name: String, from: String, to: String },
}

/// Words of `sql` as a type statement reads them: identifiers folded
/// (`"Mood"` kept as written), string literals with their quotes.
fn words(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() || b == b';' {
            i += 1;
        } else if b == b'\'' || b == b'"' {
            let end = skip_quoted(bytes, i).unwrap_or(bytes.len());
            out.push(sql[i..end].to_string());
            i = end;
        } else if matches!(b, b'(' | b')' | b',' | b'.') {
            out.push((b as char).to_string());
            i += 1;
        } else {
            let start = i;
            while i < bytes.len() && !bytes[i].is_ascii_whitespace() && !matches!(bytes[i], b'(' | b')' | b',' | b';' | b'\'' | b'"' | b'.') {
                i += 1;
            }
            out.push(sql[start..i].to_string());
        }
    }
    out
}

/// An identifier as PostgreSQL folds it.
pub fn fold(word: &str) -> String {
    match word.strip_prefix('"').and_then(|w| w.strip_suffix('"')) {
        Some(q) => q.replace("\"\"", "\""),
        None => word.to_ascii_lowercase(),
    }
}

/// A string literal's text, `None` for anything else.
fn literal(word: &str) -> Option<String> {
    word.strip_prefix('\'').and_then(|w| w.strip_suffix('\'')).map(|w| w.replace("''", "'"))
}

fn kw(word: Option<&String>, k: &str) -> bool {
    word.is_some_and(|w| w.eq_ignore_ascii_case(k))
}

/// A type name at `w[*at..]`, `schema.name` or `name`, folded; `public` and
/// `pg_catalog` are the one schema here.
fn type_name(w: &[String], at: &mut usize) -> Option<String> {
    let first = w.get(*at)?;
    if first == "(" || first == ")" || first == "," || literal(first).is_some() {
        return None;
    }
    *at += 1;
    if w.get(*at).is_some_and(|d| d == ".") {
        let name = w.get(*at + 1)?;
        *at += 2;
        return Some(fold(name));
    }
    Some(fold(first))
}

/// `CREATE TYPE … AS ENUM`, `DROP TYPE`, `ALTER TYPE` parsed; `None` for any
/// other statement, an error for a type statement that is not an enum's.
pub fn parse_type_statement(sql: &str) -> Option<Result<TypeStatement, Refusal>> {
    let head = sql.trim_start();
    let starts = |k: &str| head.len() > k.len() && head.as_bytes()[..k.len()].eq_ignore_ascii_case(k.as_bytes());
    if !(starts("CREATE ") || starts("DROP ") || starts("ALTER ")) {
        return None;
    }
    let w = words(sql);
    if !kw(w.get(1), "TYPE") {
        return None;
    }
    Some(parse_words(&w))
}

fn parse_words(w: &[String]) -> Result<TypeStatement, Refusal> {
    let syntax = |what: &str| refusal("42601", format!("syntax error at or near \"{what}\""));
    let mut at = 2;
    if kw(w.first(), "CREATE") {
        let name = type_name(w, &mut at).ok_or_else(|| syntax("TYPE"))?;
        if !kw(w.get(at), "AS") {
            return Err(refusal("0A000", "CREATE TYPE is supported for enums only (CREATE TYPE name AS ENUM (…))"));
        }
        if !kw(w.get(at + 1), "ENUM") {
            return Err(refusal(
                "0A000",
                format!("CREATE TYPE … AS {} is not supported; enums are (AS ENUM (…))", w.get(at + 1).map(String::as_str).unwrap_or("")),
            ));
        }
        at += 2;
        if w.get(at).map(String::as_str) != Some("(") {
            return Err(syntax(w.get(at).map(String::as_str).unwrap_or("")));
        }
        at += 1;
        let mut labels = Vec::new();
        while w.get(at).map(String::as_str) != Some(")") {
            let word = w.get(at).ok_or_else(|| syntax("end of input"))?;
            let label = literal(word).ok_or_else(|| syntax(word))?;
            if label.len() > 63 {
                return Err(refusal("22023", format!("invalid enum label \"{label}\"")));
            }
            if labels.contains(&label) {
                return Err(refusal("42710", format!("enum label \"{label}\" used more than once")));
            }
            labels.push(label);
            at += 1;
            match w.get(at).map(String::as_str) {
                Some(",") => at += 1,
                Some(")") => {}
                other => return Err(syntax(other.unwrap_or("end of input"))),
            }
        }
        if at + 1 != w.len() {
            return Err(syntax(&w[at + 1]));
        }
        return Ok(TypeStatement::Create { name, labels });
    }
    if kw(w.first(), "DROP") {
        let if_exists = kw(w.get(at), "IF") && kw(w.get(at + 1), "EXISTS");
        if if_exists {
            at += 2;
        }
        let mut names = Vec::new();
        loop {
            names.push(type_name(w, &mut at).ok_or_else(|| syntax(w.get(at).map(String::as_str).unwrap_or("end of input")))?);
            if w.get(at).map(String::as_str) == Some(",") {
                at += 1;
            } else {
                break;
            }
        }
        let cascade = kw(w.get(at), "CASCADE");
        if cascade || kw(w.get(at), "RESTRICT") {
            at += 1;
        }
        if at != w.len() {
            return Err(syntax(&w[at]));
        }
        return Ok(TypeStatement::Drop { names, if_exists, cascade });
    }
    // ALTER TYPE
    let name = type_name(w, &mut at).ok_or_else(|| syntax("TYPE"))?;
    if kw(w.get(at), "ADD") && kw(w.get(at + 1), "VALUE") {
        at += 2;
        let if_not_exists = kw(w.get(at), "IF") && kw(w.get(at + 1), "NOT") && kw(w.get(at + 2), "EXISTS");
        if if_not_exists {
            at += 3;
        }
        let label = w.get(at).and_then(|l| literal(l)).ok_or_else(|| syntax(w.get(at).map(String::as_str).unwrap_or("end of input")))?;
        at += 1;
        let place = if kw(w.get(at), "BEFORE") || kw(w.get(at), "AFTER") {
            let before = kw(w.get(at), "BEFORE");
            let other = w.get(at + 1).and_then(|l| literal(l)).ok_or_else(|| syntax(w.get(at + 1).map(String::as_str).unwrap_or("end of input")))?;
            at += 2;
            Some((before, other))
        } else {
            None
        };
        if at != w.len() {
            return Err(syntax(&w[at]));
        }
        if label.len() > 63 {
            return Err(refusal("22023", format!("invalid enum label \"{label}\"")));
        }
        return Ok(TypeStatement::AddValue { name, label, if_not_exists, place });
    }
    if kw(w.get(at), "RENAME") && kw(w.get(at + 1), "TO") && w.len() == at + 3 {
        return Ok(TypeStatement::Rename { name, to: fold(&w[at + 2]) });
    }
    if kw(w.get(at), "RENAME") && kw(w.get(at + 1), "VALUE") && kw(w.get(at + 3), "TO") && w.len() == at + 5 {
        let from = literal(&w[at + 2]).ok_or_else(|| syntax(&w[at + 2]))?;
        let to = literal(&w[at + 4]).ok_or_else(|| syntax(&w[at + 4]))?;
        return Ok(TypeStatement::RenameValue { name, from, to });
    }
    Err(refusal("0A000", "ALTER TYPE supports ADD VALUE, RENAME TO and RENAME VALUE"))
}

/// Where a new label goes among `labels`.
pub fn place_label(labels: &[String], label: &str, place: &Option<(bool, String)>) -> Result<Vec<String>, Refusal> {
    let mut out = labels.to_vec();
    let at = match place {
        None => out.len(),
        Some((before, other)) => {
            let p = out
                .iter()
                .position(|l| l == other)
                .ok_or_else(|| refusal("22023", format!("\"{other}\" is not an existing enum label")))?;
            if *before { p } else { p + 1 }
        }
    };
    out.insert(at, label.to_string());
    Ok(out)
}

// ---------------------------------------------------------------------------
// Storage

fn run(
    database: &Arc<Database>,
    sql: &str,
    tx: Option<TxId>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>> {
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};
    let database = database.clone();
    let sql = sql.to_string();
    Box::pin(async move {
        let purpose = recognize_query_purpose(&sql).ok_or_else(|| format!("could not parse: {sql}"))?;
        let response = match tx {
            Some(tx) => executor::execute_in_txn(purpose, database, tx).await,
            None => tokio::spawn(executor::execute_as(crate::core::mvcc::owner::OwnerKind::Statement, purpose, database))
                .await
                .map_err(|e| format!("the type catalogue statement did not finish: {e}"))?,
        };
        if response.first() == Some(&3) {
            return Err(String::from_utf8_lossy(&response[1..]).to_string());
        }
        Ok(response)
    })
}

/// A type's name as PostgreSQL's messages show it: quoted unless it is a
/// plain lowercase identifier.
pub fn display_name(name: &str) -> String {
    let plain = name.bytes().next().is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    if plain { name.to_string() } else { crate::core::rql::views::quote_ident(name) }
}

/// A violated enum constraint's error as PostgreSQL reports a value that is
/// no label: 22P02, naming the type. The value is not in the engine's
/// message, so it is not in this one.
pub fn violation(message: &str) -> Option<String> {
    let at = message.find(&format!("violates check constraint \"{CONSTRAINT_PREFIX}"))?;
    let name_at = at + "violates check constraint \"".len() + CONSTRAINT_PREFIX.len();
    let rest = &message[name_at..];
    let name = &rest[..rest.find('"')?];
    let (ty, _) = name.rsplit_once("__")?;
    Some(format!("invalid input value for enum {}", display_name(ty)))
}

/// The registry, read from the table once per process.
pub async fn ensure_loaded(database: &Arc<Database>) {
    if database.types.loaded.load(Ordering::Acquire) {
        return;
    }
    if database.has_table(TYPE_TABLE) {
        if let Ok(all) = load(database, None, None).await {
            for t in all {
                database.types.note(&t.name);
            }
        }
    }
    database.types.loaded.store(true, Ordering::Release);
}

/// The types `tx` sees whose names are in `names`, or all of them.
pub async fn load(database: &Arc<Database>, names: Option<&[String]>, tx: Option<TxId>) -> Result<Vec<EnumType>, String> {
    if !database.has_table(TYPE_TABLE) {
        return Ok(Vec::new());
    }
    let filter = match names {
        Some([]) => return Ok(Vec::new()),
        Some(names) => format!(" WHERE name IN ({})", names.iter().map(|n| crate::auth::quote(n)).collect::<Vec<_>>().join(", ")),
        None => String::new(),
    };
    let response = run(database, &format!("SELECT name, labels FROM {TYPE_TABLE}{filter}"), tx).await?;
    if response.first() != Some(&2) {
        return Ok(Vec::new());
    }
    let rows = crate::core::row::row::vec_into_rows(&response[1..]).map_err(|e| e.to_string())?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let text = |i: usize| -> String {
                row.columns
                    .get(i)
                    .filter(|c| c.column_type != crate::core::db_type::DbType::NULL)
                    .map(|c| String::from_utf8_lossy(c.data.into_slice()).into_owned())
                    .unwrap_or_default()
            };
            let labels = text(1);
            EnumType {
                name: text(0),
                labels: if labels.is_empty() { Vec::new() } else { labels.split('\n').map(str::to_string).collect() },
            }
        })
        .collect())
}

/// Writes `def`, a new row or the one under `previous` replaced.
pub async fn store(database: &Arc<Database>, def: &EnumType, previous: Option<&str>, tx: Option<TxId>) -> Result<(), String> {
    use crate::auth::quote;
    if def.labels.iter().any(|l| l.contains('\n')) {
        return Err("an enum label may not hold a line break here".to_string());
    }
    if !database.has_table(TYPE_TABLE) {
        run(database, TYPE_TABLE_DDL, None).await?;
    }
    let labels = def.labels.join("\n");
    let sql = match previous {
        Some(old) => format!(
            "UPDATE {TYPE_TABLE} SET name = {}, labels = {} WHERE name = {}",
            quote(&def.name),
            quote(&labels),
            quote(old)
        ),
        None => format!("INSERT INTO {TYPE_TABLE} (name, labels) VALUES ({}, {})", quote(&def.name), quote(&labels)),
    };
    run(database, &sql, tx).await?;
    database.types.note(&def.name);
    Ok(())
}

pub async fn remove(database: &Arc<Database>, name: &str, tx: Option<TxId>) -> Result<(), String> {
    run(database, &format!("DELETE FROM {TYPE_TABLE} WHERE name = {}", crate::auth::quote(name)), tx)
        .await
        .map(|_| ())
}

/// Runs `sql` (DDL on a user table) in `tx`.
pub async fn run_ddl(database: &Arc<Database>, sql: &str, tx: Option<TxId>) -> Result<(), String> {
    run(database, sql, tx).await.map(|_| ())
}

/// Every enum column: (table, column, type), from the tables' constraints.
pub fn enum_columns(database: &Database) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for table in database.table_names() {
        let Some(view) = database.schema_view(&table) else { continue };
        for check in &view.schema().checks {
            if check.is_deleted {
                continue;
            }
            if let Some((ty, column)) = constraint_parts(&check.name, &check.expression) {
                out.push((table.clone(), column, ty));
            }
        }
    }
    out
}

/// The type and column an enum constraint stands for: its name is
/// `__enum_<type>__<column>` and its expression `<column> IN (…)`.
fn constraint_parts(name: &str, expression: &str) -> Option<(String, String)> {
    let rest = name.strip_prefix(CONSTRAINT_PREFIX)?;
    let expr = expression.trim();
    let expr = expr.strip_prefix('(').and_then(|e| e.strip_suffix(')')).map(str::trim).unwrap_or(expr);
    let column = match expr.strip_prefix("cardinality(") {
        Some(mut inner) => {
            while let Some(rest) = inner.strip_prefix("array_remove(") {
                inner = rest;
            }
            let end = if inner.starts_with('"') {
                crate::core::sql_scan::quoted_end(inner.as_bytes(), 0, b'"')
            } else {
                inner.bytes().take_while(|b| is_identifier_byte(*b)).count()
            };
            fold(&inner[..end])
        }
        None => {
            let in_at = crate::core::rql::join::find_keyword(expr, "IN", 0)?;
            fold(expr[..in_at].trim())
        }
    };
    let ty = rest.strip_suffix(&format!("__{column}"))?;
    Some((ty.to_string(), column))
}

/// The constraint that keeps `column` of an enum (or an array of one) to
/// its labels.
pub fn column_constraint(def: &EnumType, column: &str, array: bool) -> String {
    let name = format!("{CONSTRAINT_PREFIX}{}__{}", def.name, column);
    let col = crate::core::rql::views::quote_ident(column);
    // An array's elements: what is left once NULLs and every label are
    // removed is nothing (`<@` reads JSON containment in a CHECK here).
    let check = if array {
        let removed = def
            .labels
            .iter()
            .fold(format!("array_remove({col}, NULL)"), |inner, l| format!("array_remove({inner}, {})", crate::auth::quote(l)));
        format!("cardinality({removed}) = 0")
    } else {
        format!("{col} IN ({})", def.label_list())
    };
    format!("CONSTRAINT {} CHECK ({check})", crate::core::rql::views::quote_ident(&name))
}

// ---------------------------------------------------------------------------
// Rewriting statements

/// `sql` with each use of an enum type the statement names turned into its
/// stored form; `None` when it names none. `tx` is the statement's, so a
/// type created earlier in the block is seen.
pub async fn rewrite(database: &Arc<Database>, sql: &str, tx: Option<TxId>) -> Result<Option<String>, Refusal> {
    ensure_loaded(database).await;
    if database.types.names.is_empty() {
        return Ok(None);
    }
    let mentioned = database.types.mentioned_in(sql);
    let order_by = crate::core::sql_scan::contains_ignore_ascii_case(sql, b"order");
    if mentioned.is_empty() && !order_by {
        return Ok(None);
    }
    let mut defs = if mentioned.is_empty() {
        Vec::new()
    } else {
        load(database, Some(&mentioned), tx).await.map_err(|e| refusal("XX000", e))?
    };
    let columns = if order_by { enum_columns(database) } else { Vec::new() };
    if !columns.is_empty() {
        let mut wanted: Vec<String> = columns.iter().map(|(_, _, t)| t.clone()).filter(|t| !defs.iter().any(|d| d.name == *t)).collect();
        wanted.sort();
        wanted.dedup();
        if !wanted.is_empty() {
            defs.extend(load(database, Some(&wanted), tx).await.map_err(|e| refusal("XX000", e))?);
        }
    }
    if defs.is_empty() {
        return Ok(None);
    }
    rewrite_with(sql, &defs, &columns)
}

/// [`rewrite`] over the types `defs` and the enum columns `columns`.
pub fn rewrite_with(sql: &str, defs: &[EnumType], columns: &[(String, String, String)]) -> Result<Option<String>, Refusal> {
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let head = sql.trim_start();
    let is = |k: &str| head.len() > k.len() && head.as_bytes()[..k.len()].eq_ignore_ascii_case(k.as_bytes()) && !is_identifier_byte(head.as_bytes()[k.len()]);
    if is("CREATE") || is("ALTER") {
        column_types(sql, defs, &mut edits)?;
    }
    casts(sql, defs, &mut edits)?;
    if is("SELECT") || is("WITH") {
        order_by(sql, defs, columns, &mut edits);
    }
    if edits.is_empty() {
        return Ok(None);
    }
    edits.sort_by_key(|e| e.0);
    let mut out = String::with_capacity(sql.len() + 64);
    let mut copied = 0;
    for (start, end, text) in edits {
        if start < copied {
            continue;
        }
        out.push_str(&sql[copied..start]);
        out.push_str(&text);
        copied = end;
    }
    out.push_str(&sql[copied..]);
    Ok(Some(out))
}

/// A type name written at `sql[at..]` (`mood`, `"Mood"`, `public.mood`,
/// optionally `[]`): the type, whether an array, and where the name ends.
fn type_at<'d>(sql: &str, at: usize, defs: &'d [EnumType]) -> Option<(&'d EnumType, bool, usize)> {
    let bytes = sql.as_bytes();
    let read = |i: usize| -> Option<(String, usize)> {
        if bytes.get(i) == Some(&b'"') {
            let end = crate::core::sql_scan::quoted_end(bytes, i, b'"');
            Some((fold(&sql[i..end]), end))
        } else {
            let n = bytes[i..].iter().take_while(|b| is_identifier_byte(**b)).count();
            (n > 0 && !bytes[i].is_ascii_digit()).then(|| (sql[i..i + n].to_ascii_lowercase(), i + n))
        }
    };
    let (mut name, mut end) = read(at)?;
    if bytes.get(end) == Some(&b'.') && (name == "public" || name == "pg_catalog") {
        let (n, e) = read(end + 1)?;
        name = n;
        end = e;
    }
    if bytes.get(end).is_some_and(|b| is_identifier_byte(*b) || *b == b'.') {
        return None;
    }
    let def = defs.iter().find(|d| d.name == name)?;
    let rest = &sql[end..];
    let trimmed = rest.trim_start();
    if trimmed.starts_with("[]") {
        let close = end + (rest.len() - trimmed.len()) + 2;
        return Some((def, true, close));
    }
    Some((def, false, end))
}

/// `x::mood`, `CAST(x AS mood)`: the cast to `text` (`text[]` for an
/// array). A string literal cast so must be a label (22P02).
fn casts(sql: &str, defs: &[EnumType], edits: &mut Vec<(usize, usize, String)>) -> Result<(), Refusal> {
    let bytes = sql.as_bytes();
    let mut cast_parens: Vec<bool> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        match bytes[i] {
            b'(' => {
                let before = sql[..i].trim_end();
                let word_start = before.bytes().rposition(|b| !is_identifier_byte(b)).map_or(0, |p| p + 1);
                cast_parens.push(before[word_start..].eq_ignore_ascii_case("CAST"));
                i += 1;
                continue;
            }
            b')' => {
                cast_parens.pop();
                i += 1;
                continue;
            }
            b':' if bytes.get(i + 1) == Some(&b':') => {
                let mut at = i + 2;
                while at < bytes.len() && bytes[at].is_ascii_whitespace() {
                    at += 1;
                }
                if let Some((def, array, end)) = type_at(sql, at, defs) {
                    // A literal cast: one of the labels, or 22P02.
                    let operand = sql[..i].trim_end();
                    if !array && operand.ends_with('\'') {
                        let open = operand[..operand.len() - 1].rfind('\'').unwrap_or(0);
                        if let Some(value) = literal(&operand[open..]) {
                            if !operand[..open].ends_with('\'') && !def.labels.contains(&value) {
                                return Err(refusal("22P02", format!("invalid input value for enum {}: \"{value}\"", display_name(&def.name))));
                            }
                        }
                    }
                    edits.push((at, end, if array { "text[]" } else { "text" }.to_string()));
                    i = end;
                    continue;
                }
                i += 2;
                continue;
            }
            _ => {}
        }
        if cast_parens.last() == Some(&true) && is_identifier_byte(bytes[i]) && (i == 0 || !is_identifier_byte(bytes[i - 1])) {
            let n = bytes[i..].iter().take_while(|b| is_identifier_byte(**b)).count();
            if sql[i..i + n].eq_ignore_ascii_case("AS") {
                let mut at = i + n;
                while at < bytes.len() && bytes[at].is_ascii_whitespace() {
                    at += 1;
                }
                if let Some((_, array, end)) = type_at(sql, at, defs)
                    && sql[end..].trim_start().starts_with(')')
                {
                    edits.push((at, end, if array { "text[]" } else { "text" }.to_string()));
                    i = end;
                    continue;
                }
            }
            i += n;
            continue;
        }
        i += 1;
    }
    Ok(())
}

/// A column declared with an enum type in `CREATE TABLE … (…)` or `ALTER
/// TABLE … ADD [COLUMN]`: `text` with the constraint keeping it to the
/// labels. `ALTER COLUMN … TYPE mood` is refused (0A000).
fn column_types(sql: &str, defs: &[EnumType], edits: &mut Vec<(usize, usize, String)>) -> Result<(), Refusal> {
    use crate::core::rql::join::find_keyword;
    let head_at = sql.len() - sql.trim_start().len();
    let after_first = &sql[head_at + 6..];
    let word = after_first.trim_start();
    let is_table = word.len() > 5 && word.as_bytes()[..5].eq_ignore_ascii_case(b"TABLE") && !is_identifier_byte(word.as_bytes()[5]);
    let temp_table = ["TEMP", "TEMPORARY", "UNLOGGED"].iter().any(|k| {
        word.len() > k.len() && word.as_bytes()[..k.len()].eq_ignore_ascii_case(k.as_bytes()) && !is_identifier_byte(word.as_bytes()[k.len()])
    });
    if !(is_table || temp_table) {
        return Ok(());
    }
    let create = sql.trim_start().as_bytes()[..6].eq_ignore_ascii_case(b"CREATE");
    // Each column definition: `name type …`.
    let mut definitions: Vec<(usize, usize)> = Vec::new();
    if create {
        let Some(open) = sql.find('(') else { return Ok(()) };
        if find_keyword(&sql[..open], "AS", 0).is_some() {
            return Ok(());
        }
        let Some(close) = matching_close(sql, open) else { return Ok(()) };
        let list = &sql[open + 1..close];
        for item in split_top_level(list, b',') {
            let at = open + 1 + (item.as_ptr() as usize - list.as_ptr() as usize);
            definitions.push((at, at + item.len()));
        }
    } else {
        // ALTER TABLE t ADD [COLUMN] [IF NOT EXISTS] c mood, ALTER COLUMN c TYPE mood
        let mut from = 0;
        while let Some(p) = find_keyword(sql, "TYPE", from) {
            let mut at = p + 4;
            while at < sql.len() && sql.as_bytes()[at].is_ascii_whitespace() {
                at += 1;
            }
            if let Some((def, _, _)) = type_at(sql, at, defs) {
                return Err(refusal(
                    "0A000",
                    format!("changing a column's type to the enum {} is not supported; add a new column of it instead", def.name),
                ));
            }
            from = p + 4;
        }
        let mut from = 0;
        while let Some(p) = find_keyword(sql, "ADD", from) {
            let mut rest = &sql[p + 3..];
            for k in ["COLUMN", "IF NOT EXISTS"] {
                let t = rest.trim_start();
                if t.len() > k.len() && t.as_bytes()[..k.len()].eq_ignore_ascii_case(k.as_bytes()) && !is_identifier_byte(t.as_bytes()[k.len()]) {
                    rest = &t[k.len()..];
                }
            }
            let at = sql.len() - rest.len();
            let end = [find_keyword(sql, "ADD", at), sql[at..].find(',').map(|c| at + c)]
                .into_iter()
                .flatten()
                .min()
                .unwrap_or(sql.trim_end().trim_end_matches(';').len());
            definitions.push((at, end));
            from = p + 3;
        }
    }
    for (start, end) in definitions {
        let item = &sql[start..end];
        let lead = item.len() - item.trim_start().len();
        let name_at = start + lead;
        let bytes = sql.as_bytes();
        let name_end = if bytes.get(name_at) == Some(&b'"') {
            crate::core::sql_scan::quoted_end(bytes, name_at, b'"')
        } else {
            name_at + bytes[name_at..end].iter().take_while(|b| is_identifier_byte(**b)).count()
        };
        if name_end == name_at {
            continue;
        }
        let column = fold(&sql[name_at..name_end]);
        if ["constraint", "primary", "unique", "check", "foreign", "exclude"].contains(&column.as_str()) {
            continue;
        }
        let mut type_start = name_end;
        while type_start < end && bytes[type_start].is_ascii_whitespace() {
            type_start += 1;
        }
        if let Some((def, array, type_end)) = type_at(sql, type_start, defs) {
            edits.push((
                type_start,
                type_end,
                format!("{} {}", if array { "text[]" } else { "text" }, column_constraint(def, &column, array)),
            ));
        }
    }
    Ok(())
}

/// A top-level `ORDER BY` item that is an enum column, by its labels'
/// order: `array_position(ARRAY['sad', 'ok', 'happy']::text[], m)`. Only
/// where the column is certain: a name one relation of the statement has as
/// an enum column and no other has, not hidden by an output name.
fn order_by(sql: &str, defs: &[EnumType], columns: &[(String, String, String)], edits: &mut Vec<(usize, usize, String)>) {
    use crate::core::rql::join::{find_keyword, find_phrase};
    if columns.is_empty() {
        return;
    }
    let Some(order) = find_phrase(sql, &["ORDER", "BY"], 0) else { return };
    let by = find_keyword(sql, "BY", order).unwrap_or(order) + 2;
    let end = ["LIMIT", "OFFSET", "FETCH", "FOR"].iter().filter_map(|k| find_keyword(sql, k, by)).min().unwrap_or(sql.trim_end().trim_end_matches(';').len());
    // The relations a statement names, as words.
    let mentions = |table: &str| {
        let lower = sql.to_ascii_lowercase();
        let t = table.to_ascii_lowercase();
        lower.match_indices(&t).any(|(p, _)| {
            let before = lower.as_bytes().get(p.wrapping_sub(1)).copied();
            let after = lower.as_bytes().get(p + t.len()).copied();
            p > 0 && before.is_some_and(|b| !is_identifier_byte(b)) && after.is_none_or(|b| !is_identifier_byte(b))
        })
    };
    let named: Vec<&(String, String, String)> = columns.iter().filter(|(t, _, _)| mentions(t)).collect();
    if named.is_empty() {
        return;
    }
    let list = &sql[by..end];
    for item in split_top_level(list, b',') {
        let at = by + (item.as_ptr() as usize - list.as_ptr() as usize);
        let trimmed = item.trim_start();
        let item_at = at + (item.len() - trimmed.len());
        let n = trimmed.bytes().take_while(|b| is_identifier_byte(*b) || *b == b'.' || *b == b'"').count();
        let reference = &trimmed[..n];
        if reference.is_empty() || reference.as_bytes()[0].is_ascii_digit() {
            continue;
        }
        let tail = trimmed[n..].trim_start();
        let tail_ok = tail.is_empty()
            || ["ASC", "DESC", "NULLS"].iter().any(|k| {
                tail.len() >= k.len() && tail.as_bytes()[..k.len()].eq_ignore_ascii_case(k.as_bytes()) && tail.as_bytes().get(k.len()).is_none_or(|b| !is_identifier_byte(*b))
            });
        if !tail_ok {
            continue;
        }
        let column = fold(reference.rsplit('.').next().unwrap_or(reference));
        // An output name of that name would be what the ORDER BY reads.
        let alias = format!("AS {reference}");
        if crate::core::sql_scan::contains_ignore_ascii_case(&sql[..order], alias.as_bytes()) {
            continue;
        }
        let holders: Vec<&&(String, String, String)> = named.iter().filter(|(_, c, _)| *c == column).collect();
        if holders.len() != 1 {
            continue;
        }
        let Some(def) = defs.iter().find(|d| d.name == holders[0].2) else { continue };
        edits.push((item_at, item_at + n, format!("array_position(ARRAY[{}]::text[], {reference})", def.label_list())));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mood() -> Vec<EnumType> {
        vec![EnumType { name: "mood".into(), labels: vec!["sad".into(), "ok".into(), "happy".into()] }]
    }

    #[test]
    fn type_statements_parse() {
        assert_eq!(
            parse_type_statement("CREATE TYPE \"Mood\" AS ENUM ('sad', 'o''k');"),
            Some(Ok(TypeStatement::Create { name: "Mood".into(), labels: vec!["sad".into(), "o'k".into()] }))
        );
        assert_eq!(
            parse_type_statement("CREATE TYPE public.mood AS ENUM ()"),
            Some(Ok(TypeStatement::Create { name: "mood".into(), labels: vec![] }))
        );
        assert_eq!(parse_type_statement("CREATE TYPE p AS (x int)").unwrap().unwrap_err().0, "0A000");
        assert_eq!(parse_type_statement("CREATE TYPE m AS ENUM ('a', 'a')").unwrap().unwrap_err().0, "42710");
        assert_eq!(
            parse_type_statement("DROP TYPE IF EXISTS mood, \"X\" CASCADE"),
            Some(Ok(TypeStatement::Drop { names: vec!["mood".into(), "X".into()], if_exists: true, cascade: true }))
        );
        assert_eq!(
            parse_type_statement("ALTER TYPE mood ADD VALUE IF NOT EXISTS 'meh' BEFORE 'ok'"),
            Some(Ok(TypeStatement::AddValue { name: "mood".into(), label: "meh".into(), if_not_exists: true, place: Some((true, "ok".into())) }))
        );
        assert_eq!(
            parse_type_statement("ALTER TYPE mood RENAME VALUE 'ok' TO 'fine'"),
            Some(Ok(TypeStatement::RenameValue { name: "mood".into(), from: "ok".into(), to: "fine".into() }))
        );
        assert_eq!(parse_type_statement("CREATE TABLE t (a int)"), None);
        assert_eq!(parse_type_statement("ALTER TABLE t ADD COLUMN type int"), None);
    }

    #[test]
    fn a_label_is_placed() {
        let labels: Vec<String> = vec!["a".into(), "c".into()];
        assert_eq!(place_label(&labels, "b", &Some((true, "c".into()))).unwrap(), ["a", "b", "c"]);
        assert_eq!(place_label(&labels, "b", &Some((false, "a".into()))).unwrap(), ["a", "b", "c"]);
        assert_eq!(place_label(&labels, "d", &None).unwrap(), ["a", "c", "d"]);
        assert_eq!(place_label(&labels, "d", &Some((true, "x".into()))).unwrap_err().0, "22023");
    }

    #[test]
    fn uses_of_a_type_become_its_stored_form() {
        let d = mood();
        let r = |sql: &str| rewrite_with(sql, &d, &[]).unwrap();
        assert_eq!(
            r("CREATE TABLE t (id int, m mood NOT NULL DEFAULT 'ok'::mood, ms mood[])").as_deref(),
            Some(
                "CREATE TABLE t (id int, m text CONSTRAINT \"__enum_mood__m\" CHECK (\"m\" IN ('sad', 'ok', 'happy')) NOT NULL DEFAULT 'ok'::text, \
                 ms text[] CONSTRAINT \"__enum_mood__ms\" CHECK (cardinality(array_remove(array_remove(array_remove(array_remove(\"ms\", NULL), 'sad'), 'ok'), 'happy')) = 0))"
            )
        );
        assert_eq!(
            r("ALTER TABLE t ADD COLUMN \"M\" public.mood").as_deref(),
            Some("ALTER TABLE t ADD COLUMN \"M\" text CONSTRAINT \"__enum_mood__M\" CHECK (\"M\" IN ('sad', 'ok', 'happy'))")
        );
        assert_eq!(
            r("SELECT * FROM t WHERE m = $1::mood OR m = CAST('sad' AS mood)").as_deref(),
            Some("SELECT * FROM t WHERE m = $1::text OR m = CAST('sad' AS text)")
        );
        assert_eq!(rewrite_with("SELECT 'meh'::mood", &d, &[]).unwrap_err().0, "22P02");
        assert_eq!(rewrite_with("ALTER TABLE t ALTER COLUMN m TYPE mood", &d, &[]).unwrap_err().0, "0A000");
        // Not a use of the type: an alias, a column, a quoted text.
        assert_eq!(r("SELECT x AS mood, mood FROM t WHERE note = '::mood'"), None);
        assert_eq!(r("SELECT (SELECT 1 AS mood)"), None);
    }

    #[test]
    fn an_enum_column_sorts_by_its_labels() {
        let d = mood();
        let cols = vec![("t".to_string(), "m".to_string(), "mood".to_string())];
        let r = |sql: &str| rewrite_with(sql, &d, &cols).unwrap();
        assert_eq!(
            r("SELECT id FROM t ORDER BY m DESC, id LIMIT 3").as_deref(),
            Some("SELECT id FROM t ORDER BY array_position(ARRAY['sad', 'ok', 'happy']::text[], m) DESC, id LIMIT 3")
        );
        assert_eq!(
            r("SELECT id FROM t x ORDER BY x.m").as_deref(),
            Some("SELECT id FROM t x ORDER BY array_position(ARRAY['sad', 'ok', 'happy']::text[], x.m)")
        );
        assert_eq!(r("SELECT id FROM u ORDER BY m"), None);
        assert_eq!(r("SELECT upper(m) AS m FROM t ORDER BY m"), None);
        assert_eq!(r("SELECT id FROM t ORDER BY lower(m)"), None);
    }

    #[test]
    fn an_enum_constraint_names_its_type_and_column() {
        assert_eq!(
            constraint_parts("__enum_mood__m", "m IN ('sad', 'ok')"),
            Some(("mood".to_string(), "m".to_string()))
        );
        assert_eq!(
            constraint_parts("__enum_My__Type__\"Col\"", "\"Col\" IN ('a')"),
            None
        );
        assert_eq!(
            constraint_parts("__enum_My__Type__Col", "(\"Col\" IN ('a'))"),
            Some(("My__Type".to_string(), "Col".to_string()))
        );
        assert_eq!(constraint_parts("t_m_check", "m IN ('a')"), None);
        assert_eq!(
            constraint_parts("__enum_mood__ms", "cardinality(array_remove(array_remove(\"ms\", NULL), 'a')) = 0"),
            Some(("mood".to_string(), "ms".to_string()))
        );
        assert_eq!(
            violation("new row for relation \"t\" violates check constraint \"__enum_Status__st\"").as_deref(),
            Some("invalid input value for enum \"Status\"")
        );
        assert_eq!(violation("violates check constraint \"t_check\""), None);
    }
}
