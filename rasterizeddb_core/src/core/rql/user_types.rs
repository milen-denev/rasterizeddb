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
//! `<`, `>`, `min` and `max` over an enum column compare the labels' order
//! too ([`ordering`]), where the other side is a label or a column of the
//! same type.
//!
//! Not done (documented in docs/postgres-compatibility.md): a comparison
//! with a parameter or a text expression compares text; a row sent to the
//! client types the column `text`.

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

pub(crate) fn refusal(code: &'static str, message: impl Into<String>) -> Refusal {
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
    pub(crate) loaded: AtomicBool,
}

impl TypeRegistry {
    pub(crate) fn note(&self, name: &str) {
        self.names.insert(name.to_string(), ());
    }

    /// Whether the database has had a type of this name.
    pub(crate) fn contains(&self, name: &str) -> bool {
        self.names.contains_key(name)
    }

    pub(crate) fn mentioned_in(&self, sql: &str) -> Vec<String> {
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

pub(crate) fn run(
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
    // Domains first (`domains`): a domain over an enum is then the enum.
    let with_domains = crate::core::rql::domains::rewrite(database, sql, tx).await?;
    let sql_now = with_domains.as_deref().unwrap_or(sql);
    Ok(rewrite_enums(database, sql_now, tx).await?.or(with_domains))
}

async fn rewrite_enums(database: &Arc<Database>, sql: &str, tx: Option<TxId>) -> Result<Option<String>, Refusal> {
    ensure_loaded(database).await;
    if database.types.names.is_empty() {
        return Ok(None);
    }
    let mentioned = database.types.mentioned_in(sql);
    // What needs the enum columns: an ORDER BY, an ordering comparison, a
    // min or max.
    let order_by = crate::core::sql_scan::contains_ignore_ascii_case(sql, b"order")
        || sql.contains(['<', '>'])
        || crate::core::sql_scan::contains_ignore_ascii_case(sql, b"max(")
        || crate::core::sql_scan::contains_ignore_ascii_case(sql, b"min(");
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
    enum_functions(sql, defs, &mut edits)?;
    casts(sql, defs, &mut edits)?;
    if is("SELECT") || is("WITH") {
        order_by(sql, defs, columns, &mut edits);
    }
    if !(is("CREATE") || is("ALTER") || is("DROP")) {
        ordering(sql, defs, columns, &mut edits)?;
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

/// `enum_first(NULL::mood)`, `enum_last(…)`, `enum_range(…)` and
/// `enum_range(a, b)`, answered from the labels: their argument only says
/// which type (PostgreSQL ignores its value, except `enum_range(a, b)`'s
/// bounds, NULL meaning the first or last). They were refused ("function
/// 'enum_range' is not supported yet"). Written before [`casts`], whose
/// edits inside the call are then dropped as overlapping.
fn enum_functions(sql: &str, defs: &[EnumType], edits: &mut Vec<(usize, usize, String)>) -> Result<(), Refusal> {
    if !crate::core::sql_scan::contains_ignore_ascii_case(sql, b"enum_") {
        return Ok(());
    }
    let bytes = sql.as_bytes();
    // `NULL::mood` or `'ok'::mood`: the type, and the label (None for NULL).
    let argument = |arg: &str| -> Result<Option<(&EnumType, Option<String>)>, Refusal> {
        let arg = arg.trim();
        let Some(colons) = arg.rfind("::") else { return Ok(None) };
        let value = arg[..colons].trim();
        let ty_text = arg[colons + 2..].trim_start();
        let ty_at = arg.len() - ty_text.len();
        let Some((def, false, end)) = type_at(arg, ty_at, defs) else { return Ok(None) };
        if !arg[end..].trim().is_empty() {
            return Ok(None);
        }
        if value.eq_ignore_ascii_case("NULL") {
            return Ok(Some((def, None)));
        }
        let Some(label) = literal(value) else { return Ok(None) };
        if !def.labels.contains(&label) {
            return Err(refusal("22P02", format!("invalid input value for enum {}: \"{label}\"", display_name(&def.name))));
        }
        Ok(Some((def, Some(label))))
    };
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        let boundary = i == 0 || !(is_identifier_byte(bytes[i - 1]) || bytes[i - 1] == b'.');
        let name = ["enum_range", "enum_first", "enum_last"]
            .into_iter()
            .find(|f| boundary && bytes.len() > i + f.len() && bytes[i..i + f.len()].eq_ignore_ascii_case(f.as_bytes()));
        let Some(name) = name else {
            i += 1;
            continue;
        };
        let open = i + name.len() + (sql[i + name.len()..].len() - sql[i + name.len()..].trim_start().len());
        if bytes.get(open) != Some(&b'(') {
            i += name.len();
            continue;
        }
        let Some(close) = matching_close(sql, open) else { break };
        let args = split_top_level(&sql[open + 1..close], b',');
        let quoted = |labels: &[String]| labels.iter().map(|l| crate::auth::quote(l)).collect::<Vec<_>>().join(", ");
        let value = match (name, args.as_slice()) {
            ("enum_first" | "enum_last", [a]) => match argument(a)? {
                Some((def, _)) => {
                    let label = if name == "enum_first" { def.labels.first() } else { def.labels.last() };
                    label.map(|l| crate::auth::quote(l)).unwrap_or_else(|| "NULL".to_string())
                }
                None => {
                    i = close + 1;
                    continue;
                }
            },
            ("enum_range", [a]) => match argument(a)? {
                Some((def, _)) => format!("ARRAY[{}]::text[]", quoted(&def.labels)),
                None => {
                    i = close + 1;
                    continue;
                }
            },
            ("enum_range", [a, b]) => match (argument(a)?, argument(b)?) {
                (Some((def, low)), Some((other, high))) if def.name == other.name => {
                    let from = low.as_ref().and_then(|l| def.labels.iter().position(|x| x == l)).unwrap_or(0);
                    let to = high.as_ref().and_then(|l| def.labels.iter().position(|x| x == l)).map_or(def.labels.len(), |p| p + 1);
                    if from >= to {
                        "'{}'::text[]".to_string()
                    } else {
                        format!("ARRAY[{}]::text[]", quoted(&def.labels[from..to]))
                    }
                }
                _ => {
                    i = close + 1;
                    continue;
                }
            },
            _ => {
                i = close + 1;
                continue;
            }
        };
        let alias = if crate::core::sql_scan::whole_select_item(sql, i, close + 1) { format!(" AS {name}") } else { String::new() };
        edits.push((i, close + 1, format!("{value}{alias}")));
        i = close + 1;
    }
    Ok(())
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
/// Where each column definition of a `CREATE TABLE` (or each `ADD [COLUMN]`
/// of an `ALTER TABLE`) starts and ends, and each `ALTER COLUMN … TYPE`'s
/// type; `None` for any other statement. Shared with `domains`.
pub(crate) struct ColumnDefinitions {
    pub definitions: Vec<(usize, usize)>,
    pub type_changes: Vec<usize>,
}

pub(crate) fn column_definitions(sql: &str) -> Option<ColumnDefinitions> {
    use crate::core::rql::join::find_keyword;
    let head_at = sql.len() - sql.trim_start().len();
    let after_first = sql.get(head_at + 6..)?;
    let word = after_first.trim_start();
    let is_table = word.len() > 5 && word.as_bytes()[..5].eq_ignore_ascii_case(b"TABLE") && !is_identifier_byte(word.as_bytes()[5]);
    let temp_table = ["TEMP", "TEMPORARY", "UNLOGGED"].iter().any(|k| {
        word.len() > k.len() && word.as_bytes()[..k.len()].eq_ignore_ascii_case(k.as_bytes()) && !is_identifier_byte(word.as_bytes()[k.len()])
    });
    if !(is_table || temp_table) {
        return None;
    }
    let create = sql.trim_start().as_bytes()[..6].eq_ignore_ascii_case(b"CREATE");
    let mut out = ColumnDefinitions { definitions: Vec::new(), type_changes: Vec::new() };
    if create {
        let open = sql.find('(')?;
        if find_keyword(&sql[..open], "AS", 0).is_some() {
            return None;
        }
        let close = matching_close(sql, open)?;
        let list = &sql[open + 1..close];
        for item in split_top_level(list, b',') {
            let at = open + 1 + (item.as_ptr() as usize - list.as_ptr() as usize);
            out.definitions.push((at, at + item.len()));
        }
    } else {
        // ALTER TABLE t ADD [COLUMN] [IF NOT EXISTS] c mood, ALTER COLUMN c TYPE mood
        let mut from = 0;
        while let Some(p) = find_keyword(sql, "TYPE", from) {
            let mut at = p + 4;
            while at < sql.len() && sql.as_bytes()[at].is_ascii_whitespace() {
                at += 1;
            }
            out.type_changes.push(at);
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
            out.definitions.push((at, end));
            from = p + 3;
        }
    }
    Some(out)
}

/// A column definition's name (folded) and where its type starts; `None`
/// for a table constraint (`CONSTRAINT`, `PRIMARY KEY`, …).
pub(crate) fn column_name_and_type(sql: &str, start: usize, end: usize) -> Option<(String, usize)> {
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
        return None;
    }
    let column = fold(&sql[name_at..name_end]);
    if ["constraint", "primary", "unique", "check", "foreign", "exclude"].contains(&column.as_str()) {
        return None;
    }
    let mut type_start = name_end;
    while type_start < end && bytes[type_start].is_ascii_whitespace() {
        type_start += 1;
    }
    Some((column, type_start))
}

fn column_types(sql: &str, defs: &[EnumType], edits: &mut Vec<(usize, usize, String)>) -> Result<(), Refusal> {
    let Some(found) = column_definitions(sql) else { return Ok(()) };
    for at in &found.type_changes {
        if let Some((def, _, _)) = type_at(sql, *at, defs) {
            return Err(refusal(
                "0A000",
                format!("changing a column's type to the enum {} is not supported; add a new column of it instead", def.name),
            ));
        }
    }
    let definitions = found.definitions;
    for (start, end) in definitions {
        let Some((column, type_start)) = column_name_and_type(sql, start, end) else { continue };
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
/// an enum column and no other has.
///
/// An item naming an output column that is an enum (by position, by its
/// name or alias, a `max` or `min` of one) is sorted over the query as a
/// derived table, `SELECT * FROM (…) AS __rdb_o ORDER BY array_position(…,
/// __rdb_o.m)`: a grouped or `DISTINCT` query sorts only by its select
/// list, and the position of an output is not an expression. Every item is
/// then an output reference, or none is wrapped.
fn order_by(sql: &str, defs: &[EnumType], columns: &[(String, String, String)], edits: &mut Vec<(usize, usize, String)>) {
    use crate::core::rql::join::{find_keyword, find_phrase};
    if columns.is_empty() {
        return;
    }
    let Some(order) = find_phrase(sql, &["ORDER", "BY"], 0) else { return };
    let by = find_keyword(sql, "BY", order).unwrap_or(order) + 2;
    let stmt_end = sql.trim_end().trim_end_matches(';').trim_end().len();
    let end = ["LIMIT", "OFFSET", "FETCH", "FOR"].iter().filter_map(|k| find_keyword(sql, k, by)).min().unwrap_or(stmt_end);
    let named = named_columns(sql, columns);
    if named.is_empty() {
        return;
    }
    // The select list of the statement's (first) SELECT.
    let head = sql.trim_start();
    let select_at = sql.len() - head.len();
    let outputs = if head.len() > 6 && head.as_bytes()[..6].eq_ignore_ascii_case(b"SELECT") && !is_identifier_byte(head.as_bytes()[6]) {
        let list_start = select_at + 6;
        let list_end = find_keyword(sql, "FROM", list_start).filter(|f| *f < order).unwrap_or(order);
        crate::core::rql::outer_columns::select_items(&sql[list_start..list_end]).unwrap_or_default()
    } else {
        Vec::new()
    };
    // The enum an output's expression is: a column, or max/min of one.
    let output_enum = |expr: &str| -> Option<&EnumType> {
        let e = expr.trim();
        let inner = ["max(", "min("]
            .iter()
            .find(|f| e.len() > 4 && e.as_bytes()[..4].eq_ignore_ascii_case(f.as_bytes()) && e.ends_with(')'))
            .map(|_| e[4..e.len() - 1].trim())
            .unwrap_or(e);
        let plain = !inner.is_empty() && inner.bytes().all(|b| is_identifier_byte(b) || b == b'.' || b == b'"');
        if plain { enum_of(sql, &named, defs, inner) } else { None }
    };
    struct Item<'s> {
        at: usize,
        len: usize,
        reference: &'s str,
        tail: &'s str,
        /// The output it names, by index.
        output: Option<usize>,
    }
    let list = &sql[by..end];
    let mut items: Vec<Item> = Vec::new();
    for item in split_top_level(list, b',') {
        let at = by + (item.as_ptr() as usize - list.as_ptr() as usize);
        let trimmed = item.trim_start();
        let item_at = at + (item.len() - trimmed.len());
        let n = trimmed.bytes().take_while(|b| is_identifier_byte(*b) || *b == b'.' || *b == b'"').count();
        let reference = &trimmed[..n];
        let tail = trimmed[n..].trim();
        let tail_ok = tail.is_empty()
            || ["ASC", "DESC", "NULLS"].iter().any(|k| {
                tail.len() >= k.len() && tail.as_bytes()[..k.len()].eq_ignore_ascii_case(k.as_bytes()) && tail.as_bytes().get(k.len()).is_none_or(|b| !is_identifier_byte(*b))
            });
        let output = if reference.is_empty() || !tail_ok {
            None
        } else if reference.bytes().all(|b| b.is_ascii_digit()) {
            reference.parse::<usize>().ok().filter(|k| *k >= 1 && *k <= outputs.len()).map(|k| k - 1)
        } else if !reference.contains('.') {
            let name = fold(reference);
            let mut found = outputs.iter().enumerate().filter(|(_, (o, _))| *o == name);
            match (found.next(), found.next()) {
                (Some((k, _)), None) => Some(k),
                _ => None,
            }
        } else {
            None
        };
        items.push(Item { at: item_at, len: if tail_ok { n } else { 0 }, reference, tail, output });
    }
    let wraps = items.iter().any(|it| it.output.is_some_and(|k| output_enum(&outputs[k].1).is_some()));
    let unique = |k: usize| outputs.iter().filter(|(o, _)| *o == outputs[k].0).count() == 1;
    if wraps && select_at < order && items.iter().all(|it| it.output.is_some_and(unique)) {
        let terms: Vec<String> = items
            .iter()
            .map(|it| {
                let k = it.output.unwrap_or_default();
                let column = format!("__rdb_o.{}", crate::core::rql::views::quote_ident(&outputs[k].0));
                let term = match output_enum(&outputs[k].1) {
                    Some(def) => format!("array_position(ARRAY[{}]::text[], {column})", def.label_list()),
                    None => column,
                };
                if it.tail.is_empty() { term } else { format!("{term} {}", it.tail) }
            })
            .collect();
        let gap = if end < stmt_end { " " } else { "" };
        edits.push((select_at, select_at, "SELECT * FROM (".to_string()));
        edits.push((order, end, format!(") AS __rdb_o ORDER BY {}{gap}", terms.join(", "))));
        return;
    }
    for it in &items {
        if it.len == 0 {
            continue;
        }
        // An output is what an item of its name or position reads: sorted
        // in place by the column it is, when it is one (not a max or min,
        // which only the derived table can sort).
        let column = match it.output {
            Some(k) => outputs[k].1.trim(),
            None if it.reference.as_bytes()[0].is_ascii_digit() => continue,
            None => it.reference,
        };
        if column.contains('(') {
            continue;
        }
        let Some(def) = enum_of(sql, &named, defs, column) else { continue };
        edits.push((it.at, it.at + it.len, format!("array_position(ARRAY[{}]::text[], {column})", def.label_list())));
    }
}

/// The enum column `reference` names, by the holder rule of [`order_by`]:
/// one relation of the statement has it as an enum column and no other.
/// A qualified reference (`q.m`) only where `q` is that table or its alias.
fn enum_of<'d>(sql: &str, named: &[&(String, String, String)], defs: &'d [EnumType], reference: &str) -> Option<&'d EnumType> {
    let column = fold(reference.rsplit('.').next().unwrap_or(reference));
    let mut holders = named.iter().filter(|(_, c, _)| *c == column);
    let (table, _, ty) = holders.next()?;
    if holders.next().is_some() {
        return None;
    }
    if let Some((qualifier, _)) = reference.rsplit_once('.') {
        let qualifier = fold(qualifier.rsplit('.').next().unwrap_or(qualifier));
        if qualifier != *table && !aliases(sql, table).contains(&qualifier) {
            return None;
        }
    }
    defs.iter().find(|d| d.name == *ty)
}

/// The aliases `sql` gives `table`: the word after `table` or `table AS`.
fn aliases(sql: &str, table: &str) -> Vec<String> {
    const NOT_ALIASES: &[&str] = &[
        "where", "join", "on", "using", "left", "right", "full", "inner", "cross", "natural", "group", "order",
        "having", "limit", "offset", "union", "intersect", "except", "window", "fetch", "for", "set", "values",
        "returning", "lateral",
    ];
    let lower = sql.to_ascii_lowercase();
    let t = table.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut out = Vec::new();
    for (p, _) in lower.match_indices(&t) {
        let before_ok = p > 0 && !is_identifier_byte(bytes[p - 1]) && bytes[p - 1] != b'.';
        let end = p + t.len();
        if !before_ok || bytes.get(end).is_some_and(|b| is_identifier_byte(*b) || *b == b'.') {
            continue;
        }
        let mut rest = lower[end..].trim_start();
        if rest.starts_with("as") && rest.as_bytes().get(2).is_some_and(|b| b.is_ascii_whitespace()) {
            rest = rest[2..].trim_start();
        }
        let word: String = rest.bytes().take_while(|b| is_identifier_byte(*b)).map(|b| b as char).collect();
        if !word.is_empty() && !NOT_ALIASES.contains(&word.as_str()) {
            out.push(word);
        }
    }
    out
}

/// The tables of `columns` that `sql` names as words.
fn named_columns<'c>(sql: &str, columns: &'c [(String, String, String)]) -> Vec<&'c (String, String, String)> {
    let lower = sql.to_ascii_lowercase();
    columns
        .iter()
        .filter(|(table, _, _)| {
            let t = table.to_ascii_lowercase();
            lower.match_indices(&t).any(|(p, _)| {
                let before = lower.as_bytes().get(p.wrapping_sub(1)).copied();
                let after = lower.as_bytes().get(p + t.len()).copied();
                p > 0 && before.is_some_and(|b| !is_identifier_byte(b)) && after.is_none_or(|b| !is_identifier_byte(b))
            })
        })
        .collect()
}

/// An ordering comparison at the front of `text` (`<`, `<=`, `>`, `>=`),
/// its length; `None` for `<>`, `<@`, `<<` and anything else.
fn ordering_op(text: &str) -> Option<usize> {
    let b = text.as_bytes();
    match (b.first(), b.get(1)) {
        (Some(b'<' | b'>'), Some(b'=')) => Some(2),
        (Some(b'<'), Some(b'>' | b'@' | b'<' | b'-')) | (Some(b'>'), Some(b'>')) => None,
        (Some(b'<' | b'>'), _) => Some(1),
        _ => None,
    }
}

/// Whether `text` starts with the keyword `word`.
fn starts_word(text: &str, word: &str) -> bool {
    let t = text.trim_start();
    t.len() >= word.len()
        && t.as_bytes()[..word.len()].eq_ignore_ascii_case(word.as_bytes())
        && t.as_bytes().get(word.len()).is_none_or(|b| !is_identifier_byte(*b))
}

/// Where a parameter (`$1`) at `at` ends, without a cast after it.
fn parameter_end(sql: &str, at: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    if bytes.get(at) != Some(&b'$') {
        return None;
    }
    let end = at + 1 + bytes[at + 1..].iter().take_while(|b| b.is_ascii_digit()).count();
    (end > at + 1 && !sql[end..].trim_start().starts_with("::") && bytes.get(end).is_none_or(|b| !is_identifier_byte(*b))).then_some(end)
}

/// Enum values compared and folded by their labels' order, as PostgreSQL
/// compares them, not by their text: `m > 'sad'` is
/// `array_position(ARRAY['sad', 'ok', 'happy']::text[], m) > 1`, `m1 < m2`
/// compares both positions, and `max(m)` is the label at the greatest
/// position. Only an enum column certain by the [`order_by`] rule, compared
/// with a label literal or a column of its own type; anything else (a
/// parameter, a text column) still compares text. A literal that is no
/// label is PostgreSQL's 22P02.
fn ordering(
    sql: &str,
    defs: &[EnumType],
    columns: &[(String, String, String)],
    edits: &mut Vec<(usize, usize, String)>,
) -> Result<(), Refusal> {
    if columns.is_empty() {
        return Ok(());
    }
    let named = named_columns(sql, columns);
    if named.is_empty() {
        return Ok(());
    }
    let bytes = sql.as_bytes();
    let reference_at = |s: usize| -> usize {
        s + bytes[s..].iter().take_while(|b| is_identifier_byte(**b) || **b == b'.' || **b == b'"').count()
    };
    // The column a reference ending at `end` begins with, by scanning back.
    let reference_before = |end: usize| -> Option<(usize, &str)> {
        let s = sql[..end].bytes().rposition(|b| !(is_identifier_byte(b) || b == b'.' || b == b'"')).map_or(0, |p| p + 1);
        (s < end && !bytes[s].is_ascii_digit()).then(|| (s, &sql[s..end]))
    };
    let label_position = |def: &EnumType, at: usize| -> Result<Option<(usize, String)>, Refusal> {
        // A literal in parentheses, `('ok')`, as `EXECUTE` binds an argument.
        if bytes.get(at) == Some(&b'(')
            && let Some(close) = matching_close(sql, at)
        {
            let inner = sql[at + 1..close].trim();
            let inner_at = at + 1 + (sql[at + 1..close].len() - sql[at + 1..close].trim_start().len());
            let whole = inner.starts_with('\'') && crate::core::sql_scan::quoted_end(bytes, inner_at, b'\'') == inner_at + inner.len();
            if !whole {
                return Ok(None);
            }
            let value = inner[1..inner.len() - 1].replace("''", "'");
            return match def.labels.iter().position(|l| *l == value) {
                Some(p) if !sql[close + 1..].trim_start().starts_with("::") => Ok(Some((close + 1, (p + 1).to_string()))),
                Some(_) => Ok(None),
                None => Err(refusal("22P02", format!("invalid input value for enum {}: \"{value}\"", display_name(&def.name)))),
            };
        }
        if bytes.get(at) != Some(&b'\'') {
            return Ok(None);
        }
        let end = crate::core::sql_scan::quoted_end(bytes, at, b'\'');
        if sql[end..].trim_start().starts_with("::") {
            return Ok(None);
        }
        let value = sql[at + 1..end - 1].replace("''", "'");
        match def.labels.iter().position(|l| *l == value) {
            Some(p) => Ok(Some((end, (p + 1).to_string()))),
            None => Err(refusal("22P02", format!("invalid input value for enum {}: \"{value}\"", display_name(&def.name)))),
        }
    };
    let positions = |def: &EnumType, reference: &str| format!("array_position(ARRAY[{}]::text[], {reference})", def.label_list());
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        let c = bytes[i];
        let boundary = i == 0 || !(is_identifier_byte(bytes[i - 1]) || matches!(bytes[i - 1], b'.' | b'"' | b':' | b'$'));
        if !(boundary && (c.is_ascii_alphabetic() || c == b'_' || c == b'"')) {
            i += 1;
            continue;
        }
        let s = i;
        let e = reference_at(s);
        i = e.max(s + 1);
        let reference = &sql[s..e];
        if bytes.get(e) == Some(&b'(') || reference.ends_with('.') {
            continue;
        }
        let Some(def) = enum_of(sql, &named, defs, reference) else { continue };
        let after = sql[e..].trim_start();
        let after_at = e + (sql[e..].len() - after.len());
        let before = sql[..s].trim_end();
        // `max(m)`, `min(m)`.
        if after.starts_with(')') {
            let call = before.strip_suffix('(').map(str::trim_end);
            let word = call.and_then(|c| {
                let w = c.bytes().rposition(|b| !is_identifier_byte(b)).map_or(0, |p| p + 1);
                (c.len() - w == 3).then(|| (w, &c[w..]))
            });
            if let Some((word_at, name)) = word
                && (name.eq_ignore_ascii_case("max") || name.eq_ignore_ascii_case("min"))
            {
                let close = after_at;
                let folded = format!(
                    "(ARRAY[{}]::text[])[{name}({})]",
                    def.label_list(),
                    positions(def, reference)
                );
                let alias = if crate::core::sql_scan::whole_select_item(sql, word_at, close + 1) { format!(" AS {}", name.to_ascii_lowercase()) } else { String::new() };
                edits.push((word_at, close + 1, format!("{folded}{alias}")));
                continue;
            }
        }
        // `m [NOT] BETWEEN 'sad' AND 'ok'`.
        let between_at = if starts_word(after, "NOT") { after_at + 3 + (after[3..].len() - after[3..].trim_start().len()) } else { after_at };
        if starts_word(&sql[between_at..], "BETWEEN") {
            let low_at = between_at + 7 + (sql[between_at + 7..].len() - sql[between_at + 7..].trim_start().len());
            if let Some((low_end, low)) = label_position(def, low_at)?
                && starts_word(&sql[low_end..], "AND")
            {
                let and_at = low_end + (sql[low_end..].len() - sql[low_end..].trim_start().len());
                let high_at = and_at + 3 + (sql[and_at + 3..].len() - sql[and_at + 3..].trim_start().len());
                if let Some((high_end, high)) = label_position(def, high_at)? {
                    edits.push((low_at, low_end, low));
                    edits.push((high_at, high_end, high));
                    edits.push((s, e, positions(def, reference)));
                    continue;
                }
            }
        }
        // `m > 'sad'`, `m < n`, `m >= $1`.
        let mut wrap = false;
        if let Some(n) = ordering_op(after) {
            let other_at = after_at + n + (sql[after_at + n..].len() - sql[after_at + n..].trim_start().len());
            if let Some((end, position)) = label_position(def, other_at)? {
                edits.push((other_at, end, position));
                wrap = true;
            } else if let Some(end) = parameter_end(sql, other_at) {
                edits.push((other_at, end, positions(def, &sql[other_at..end])));
                wrap = true;
            } else {
                let other_end = reference_at(other_at);
                wrap = other_end > other_at
                    && bytes.get(other_end) != Some(&b'(')
                    && enum_of(sql, &named, defs, &sql[other_at..other_end]).is_some_and(|d| d.name == def.name);
            }
        }
        if !wrap {
            // `'sad' < m`, `n < m`.
            let op = [">=", "<=", ">", "<"].into_iter().find(|op| before.ends_with(op));
            let op_ok = op.is_some_and(|op| {
                let lead = before[..before.len() - op.len()].trim_end();
                !(lead.ends_with(['<', '>', '=', '!', '-', '@']) || (op.len() == 1 && before.ends_with("<>")))
            });
            if op_ok {
                let lead = before[..before.len() - op.unwrap().len()].trim_end();
                if lead.ends_with(')')
                    && let Some(open) = lead.rfind('(')
                    && let Some((end, position)) = label_position(def, open)?
                    && end == lead.len()
                {
                    edits.push((open, end, position));
                    wrap = true;
                } else if lead.ends_with('\'') {
                    let lit_at = sql[..lead.len() - 1].rfind('\'').unwrap_or(0);
                    // The literal's start: back over doubled quotes.
                    let mut start = lit_at;
                    while start > 0 && bytes[start - 1] == b'\'' {
                        start = sql[..start - 1].rfind('\'').unwrap_or(0);
                    }
                    if let Some((end, position)) = label_position(def, start)?
                        && end == lead.len()
                    {
                        edits.push((start, end, position));
                        wrap = true;
                    }
                } else if let Some(p) = lead.bytes().rposition(|b| !b.is_ascii_digit())
                    && p + 1 < lead.len()
                    && bytes[p] == b'$'
                    && (p == 0 || !is_identifier_byte(bytes[p - 1]))
                {
                    edits.push((p, lead.len(), positions(def, &lead[p..])));
                    wrap = true;
                } else if let Some((_, other)) = reference_before(lead.len()) {
                    wrap = enum_of(sql, &named, defs, other).is_some_and(|d| d.name == def.name);
                }
            }
        }
        if wrap {
            edits.push((s, e, positions(def, reference)));
        }
    }
    Ok(())
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
    fn enum_functions_answer_from_the_labels() {
        let d = mood();
        let r = |sql: &str| rewrite_with(sql, &d, &[]).unwrap();
        assert_eq!(
            r("SELECT enum_first(NULL::mood), enum_last(null::mood), enum_range(NULL::mood)").as_deref(),
            Some("SELECT 'sad' AS enum_first, 'happy' AS enum_last, ARRAY['sad', 'ok', 'happy']::text[] AS enum_range")
        );
        assert_eq!(
            r("SELECT enum_range('ok'::mood, NULL::mood), enum_range(NULL::mood, 'ok'::mood), enum_range('happy'::mood, 'sad'::mood)").as_deref(),
            Some("SELECT ARRAY['ok', 'happy']::text[] AS enum_range, ARRAY['sad', 'ok']::text[] AS enum_range, '{}'::text[] AS enum_range")
        );
        assert_eq!(rewrite_with("SELECT enum_first('glad'::mood)", &d, &[]).unwrap_err().0, "22P02");
        assert_eq!(r("SELECT enum_first(x)"), None);
    }

    #[test]
    fn enum_comparisons_and_extremes_follow_the_labels() {
        let d = mood();
        let cols = vec![("t".to_string(), "m".to_string(), "mood".to_string()), ("t".to_string(), "n".to_string(), "mood".to_string())];
        let r = |sql: &str| rewrite_with(sql, &d, &cols).unwrap();
        let p = |c: &str| format!("array_position(ARRAY['sad', 'ok', 'happy']::text[], {c})");
        assert_eq!(r("SELECT id FROM t WHERE m > 'sad'"), Some(format!("SELECT id FROM t WHERE {} > 1", p("m"))));
        assert_eq!(r("SELECT id FROM t WHERE 'ok' >= t.m"), Some(format!("SELECT id FROM t WHERE 2 >= {}", p("t.m"))));
        assert_eq!(r("SELECT id FROM t WHERE m<n"), Some(format!("SELECT id FROM t WHERE {}<{}", p("m"), p("n"))));
        assert_eq!(
            r("SELECT max(m), min(m) AS lo, count(*) FROM t"),
            Some(format!(
                "SELECT (ARRAY['sad', 'ok', 'happy']::text[])[max({})] AS max, (ARRAY['sad', 'ok', 'happy']::text[])[min({})] AS lo, count(*) FROM t",
                p("m"),
                p("m")
            ))
        );
        assert_eq!(
            r("SELECT coalesce(max(m), 'ok') FROM t"),
            Some(format!("SELECT coalesce((ARRAY['sad', 'ok', 'happy']::text[])[max({})], 'ok') FROM t", p("m")))
        );
        // Equality, a parameter, a text column, another table: text as before.
        assert_eq!(r("SELECT id FROM t WHERE m = 'sad' AND m <> 'ok'"), None);
        assert_eq!(r("SELECT id FROM t WHERE m > $1"), Some(format!("SELECT id FROM t WHERE {} > {}", p("m"), p("$1"))));
        assert_eq!(r("SELECT id FROM t WHERE $2 <= m"), Some(format!("SELECT id FROM t WHERE {} <= {}", p("$2"), p("m"))));
        assert_eq!(
            r("SELECT id FROM t WHERE m NOT BETWEEN 'sad' AND 'ok' AND id > 1"),
            Some(format!("SELECT id FROM t WHERE {} NOT BETWEEN 1 AND 2 AND id > 1", p("m")))
        );
        assert_eq!(r("SELECT id FROM t WHERE m > $1::text"), None);
        // `EXECUTE`'s bound argument.
        assert_eq!(r("SELECT id FROM t WHERE m >= ('ok')"), Some(format!("SELECT id FROM t WHERE {} >= 2", p("m"))));
        assert_eq!(r("SELECT id FROM t WHERE ('ok') < m"), Some(format!("SELECT id FROM t WHERE 2 < {}", p("m"))));
        assert_eq!(r("SELECT id FROM t WHERE m > (x)"), None);
        assert_eq!(r("SELECT id FROM t WHERE m > note"), None);
        assert_eq!(r("SELECT id FROM u WHERE m > 'sad'"), None);
        // A qualified reference that is another table's column.
        assert_eq!(r("SELECT * FROM t, u WHERE u.m > 'x'"), None);
        assert_eq!(r("SELECT * FROM t AS a WHERE a.m > 'sad'"), Some(format!("SELECT * FROM t AS a WHERE {} > 1", p("a.m"))));
        let err = rewrite_with("SELECT id FROM t WHERE m < 'glad'", &d, &cols).unwrap_err();
        assert_eq!(err, ("22P02", "invalid input value for enum mood: \"glad\"".to_string()));
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
        // An enum output: the query sorted as a derived table.
        let a = "ARRAY['sad', 'ok', 'happy']::text[]";
        assert_eq!(
            r("SELECT DISTINCT m FROM t ORDER BY m").unwrap(),
            format!("SELECT * FROM (SELECT DISTINCT m FROM t ) AS __rdb_o ORDER BY array_position({a}, __rdb_o.\"m\")")
        );
        assert_eq!(
            r("SELECT m AS z, count(*) FROM t GROUP BY m ORDER BY 1 DESC, count LIMIT 2;").unwrap(),
            format!("SELECT * FROM (SELECT m AS z, count(*) FROM t GROUP BY m ) AS __rdb_o ORDER BY array_position({a}, __rdb_o.\"z\") DESC, __rdb_o.\"count\" LIMIT 2;")
        );
        // An item that is no output: nothing wrapped, the column sorted in place.
        assert_eq!(r("SELECT m AS z FROM t ORDER BY z, id").unwrap(), format!("SELECT m AS z FROM t ORDER BY array_position({a}, m), id"));
        assert_eq!(r("SELECT m FROM t ORDER BY 1, id").unwrap(), format!("SELECT m FROM t ORDER BY array_position({a}, m), id"));
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
