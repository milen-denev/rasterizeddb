//! Views and materialized views.
//!
//! # Storage
//!
//! A view is a row of the per-database system table [`VIEW_TABLE`]: its name,
//! kind, query text (as the statement wrote it, after comments are stripped
//! and unquoted names folded), the names of its columns, and the relations
//! its query names. The row is written and deleted with ordinary SQL in the
//! statement's transaction, as the role catalogue is (`auth`), so a `CREATE
//! VIEW` or `DROP VIEW` inside a block is undone by its `ROLLBACK` and is
//! invisible to other sessions until it commits.
//!
//! # Reading
//!
//! A view is expanded where it is read: each reference in a `FROM`, `JOIN` or
//! `USING` list becomes the derived table `(<query>) AS <name>`, recursively,
//! before anything else plans the statement ([`expand`]). Every pass after
//! that already knows derived tables, so a view works wherever a subquery in
//! `FROM` does. A view is never stored expanded: replacing an inner view
//! changes every view built on it, as PostgreSQL's do.
//!
//! [`ViewRegistry`] keeps the names of every view this process has seen, a
//! superset of those any snapshot can see, so a statement that names none of
//! them costs a few substring checks and no read.
//!
//! # Materialized views
//!
//! A materialized view is a real table under its own name, filled by its
//! query (`CREATE TABLE … AS`), plus its row here. Reads go to the table.
//! Writes to it are refused (42809) except by `REFRESH MATERIALIZED VIEW`.

use std::sync::atomic::{AtomicU64, Ordering};

use rclite::Arc;

use crate::core::database::Database;
use crate::core::mvcc::TxId;
use crate::core::rql::lock_set::RelationRef;

/// The system table views are kept in. The `__` prefix keeps it out of the
/// catalogue relations a client lists (`catalog_views::is_internal`).
pub const VIEW_TABLE: &str = "__rdb_views";

const VIEW_TABLE_DDL: &str = "CREATE TABLE __rdb_views (name TEXT PRIMARY KEY, kind TEXT NOT NULL, \
     definition TEXT NOT NULL, columns TEXT NOT NULL, deps TEXT NOT NULL)";

/// How deep views may be nested in one another before expansion stops: a
/// cycle (which `CREATE` refuses, but a rename could make) ends here rather
/// than looping.
const MAX_DEPTH: usize = 32;

/// The view names a database's statements are checked for.
#[derive(Default)]
pub struct ViewRegistry {
    names: dashmap::DashMap<String, ()>,
    /// When the names were last read from the catalogue table, in
    /// milliseconds since the Unix epoch; 0 before the first read.
    loaded_at: AtomicU64,
}

/// How long the names read from the catalogue table are trusted. A view this
/// process creates is noted at once (`store`); one that arrives otherwise (a
/// replicated row from another node) is seen within this long.
const RELOAD_AFTER_MS: u64 = 2_000;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl ViewRegistry {
    fn note(&self, name: &str) {
        self.names.insert(name.to_string(), ());
    }

    /// Whether `sql` may name a view: one of the names appears in it.
    fn mentioned_in(&self, sql: &str) -> bool {
        self.names
            .iter()
            .any(|entry| crate::core::sql_scan::contains_ignore_ascii_case(sql, entry.key().as_bytes()))
    }
}

/// A view or a materialized view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewKind {
    View,
    Materialized,
}

impl ViewKind {
    fn code(self) -> &'static str {
        match self {
            ViewKind::View => "v",
            ViewKind::Materialized => "m",
        }
    }

    fn from_code(code: &str) -> Self {
        if code == "m" { ViewKind::Materialized } else { ViewKind::View }
    }

    /// The word PostgreSQL's messages use.
    pub fn noun(self) -> &'static str {
        match self {
            ViewKind::View => "view",
            ViewKind::Materialized => "materialized view",
        }
    }
}

/// A stored view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewDef {
    pub name: String,
    pub kind: ViewKind,
    /// The query, as stored: a column list given at `CREATE` is already
    /// applied by an outer select over it.
    pub definition: String,
    pub columns: Vec<String>,
    /// The relations the query names (views among them), lowercased.
    pub deps: Vec<String>,
}

/// A view statement, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewStatement {
    Create {
        name: String,
        columns: Option<Vec<String>>,
        query: String,
        or_replace: bool,
        kind: ViewKind,
        if_not_exists: bool,
        /// `WITH NO DATA` on a materialized view: created empty.
        with_data: bool,
    },
    Drop {
        names: Vec<String>,
        kind: ViewKind,
        if_exists: bool,
        cascade: bool,
    },
    Refresh {
        name: String,
        with_data: bool,
    },
    Rename {
        from: String,
        to: String,
        kind: ViewKind,
        if_exists: bool,
    },
}

/// A view statement's refusal: its SQLSTATE and message.
pub type Refusal = (&'static str, String);

fn refusal(code: &'static str, message: impl Into<String>) -> Refusal {
    (code, message.into())
}

/// A double-quoted identifier, so the name reads back exactly, case and all.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Reads keywords and names off the front of a statement.
struct Cursor<'a> {
    text: &'a str,
    at: usize,
}

impl<'a> Cursor<'a> {
    fn new(text: &'a str) -> Self {
        Cursor { text, at: 0 }
    }

    fn rest(&self) -> &'a str {
        &self.text[self.at..]
    }

    fn skip_ws(&mut self) {
        let rest = self.rest();
        self.at += rest.len() - rest.trim_start().len();
    }

    fn eat(&mut self, word: &str) -> bool {
        self.skip_ws();
        let rest = self.rest().as_bytes();
        let fits = rest.len() >= word.len() && rest[..word.len()].eq_ignore_ascii_case(word.as_bytes());
        let bounded = rest.get(word.len()).is_none_or(|b| !(b.is_ascii_alphanumeric() || *b == b'_'));
        if fits && bounded {
            self.at += word.len();
            true
        } else {
            false
        }
    }

    fn eat_all(&mut self, words: &[&str]) -> bool {
        let saved = self.at;
        if words.iter().all(|w| self.eat(w)) {
            true
        } else {
            self.at = saved;
            false
        }
    }

    /// One identifier: `"quoted"` (its `""` read as `"`) or a bare word.
    fn identifier(&mut self) -> Option<String> {
        let bytes = self.rest().as_bytes();
        if bytes.first() == Some(&b'"') {
            let end = crate::core::sql_scan::quoted_end(bytes, 0, b'"');
            if end < 2 || bytes.get(end - 1) != Some(&b'"') {
                return None;
            }
            let name = self.rest()[1..end - 1].replace("\"\"", "\"");
            self.at += end;
            return Some(name);
        }
        let len = bytes
            .iter()
            .position(|b| !(b.is_ascii_alphanumeric() || *b == b'_' || *b == b'$' || *b >= 0x80))
            .unwrap_or(bytes.len());
        if len == 0 || bytes[0].is_ascii_digit() {
            return None;
        }
        let name = self.rest()[..len].to_string();
        self.at += len;
        Some(name)
    }

    /// A possibly qualified name: its last segment, unquoted.
    fn name(&mut self) -> Option<String> {
        self.skip_ws();
        let mut name = self.identifier()?;
        while self.rest().starts_with('.') {
            self.at += 1;
            name = self.identifier()?;
        }
        (!name.is_empty()).then_some(name)
    }

    fn punct(&mut self, c: char) -> bool {
        self.skip_ws();
        if self.rest().starts_with(c) {
            self.at += c.len_utf8();
            true
        } else {
            false
        }
    }
}

/// Parses `CREATE [OR REPLACE] [MATERIALIZED] VIEW …`, `DROP [MATERIALIZED]
/// VIEW …`, `REFRESH MATERIALIZED VIEW …` and `ALTER [MATERIALIZED] VIEW …
/// RENAME TO …`; `None` for any other statement.
pub fn parse_view_statement(sql: &str) -> Option<Result<ViewStatement, Refusal>> {
    let sql = sql.trim().trim_end_matches(';').trim_end();
    let mut c = Cursor::new(sql);
    if c.eat("CREATE") {
        let or_replace = c.eat_all(&["OR", "REPLACE"]);
        let temporary = c.eat("TEMP") || c.eat("TEMPORARY");
        let recursive = c.eat("RECURSIVE");
        let materialized = c.eat("MATERIALIZED");
        if !c.eat("VIEW") {
            return None;
        }
        return Some(parse_create(c, or_replace, temporary, recursive, materialized));
    }
    if c.eat("DROP") {
        let materialized = c.eat("MATERIALIZED");
        if !c.eat("VIEW") {
            return None;
        }
        let kind = if materialized { ViewKind::Materialized } else { ViewKind::View };
        let if_exists = c.eat_all(&["IF", "EXISTS"]);
        let mut names = Vec::new();
        loop {
            let Some(name) = c.name() else {
                return Some(Err(refusal("42601", "syntax error in DROP VIEW: expected a view name")));
            };
            names.push(name);
            if !c.punct(',') {
                break;
            }
        }
        let cascade = c.eat("CASCADE");
        if !cascade {
            c.eat("RESTRICT");
        }
        c.skip_ws();
        if !c.rest().is_empty() {
            return Some(Err(refusal("42601", format!("syntax error at or near \"{}\"", c.rest()))));
        }
        return Some(Ok(ViewStatement::Drop { names, kind, if_exists, cascade }));
    }
    if c.eat("REFRESH") {
        if !c.eat_all(&["MATERIALIZED", "VIEW"]) {
            return None;
        }
        if c.eat("CONCURRENTLY") {
            return Some(Err(refusal(
                "0A000",
                "REFRESH MATERIALIZED VIEW CONCURRENTLY is not supported; REFRESH without it replaces the rows \
                 atomically",
            )));
        }
        let Some(name) = c.name() else {
            return Some(Err(refusal("42601", "syntax error in REFRESH MATERIALIZED VIEW: expected a name")));
        };
        let with_data = if c.eat_all(&["WITH", "NO", "DATA"]) {
            false
        } else {
            c.eat_all(&["WITH", "DATA"]);
            true
        };
        c.skip_ws();
        if !c.rest().is_empty() {
            return Some(Err(refusal("42601", format!("syntax error at or near \"{}\"", c.rest()))));
        }
        return Some(Ok(ViewStatement::Refresh { name, with_data }));
    }
    if c.eat("ALTER") {
        let materialized = c.eat("MATERIALIZED");
        if !c.eat("VIEW") {
            return None;
        }
        let kind = if materialized { ViewKind::Materialized } else { ViewKind::View };
        let if_exists = c.eat_all(&["IF", "EXISTS"]);
        let Some(from) = c.name() else {
            return Some(Err(refusal("42601", "syntax error in ALTER VIEW: expected a view name")));
        };
        if c.eat_all(&["RENAME", "TO"]) {
            let Some(to) = c.name() else {
                return Some(Err(refusal("42601", "syntax error in ALTER VIEW: expected the new name")));
            };
            return Some(Ok(ViewStatement::Rename { from, to, kind, if_exists }));
        }
        return Some(Err(refusal(
            "0A000",
            format!("ALTER {} supports only RENAME TO here", kind.noun().to_ascii_uppercase()),
        )));
    }
    None
}

fn parse_create(
    mut c: Cursor<'_>,
    or_replace: bool,
    temporary: bool,
    recursive: bool,
    materialized: bool,
) -> Result<ViewStatement, Refusal> {
    let kind = if materialized { ViewKind::Materialized } else { ViewKind::View };
    if temporary {
        return Err(refusal("0A000", "CREATE TEMPORARY VIEW is not supported yet"));
    }
    if recursive {
        return Err(refusal("0A000", "CREATE RECURSIVE VIEW is not supported; use WITH RECURSIVE in a view's query"));
    }
    if materialized && or_replace {
        return Err(refusal("42601", "syntax error at or near \"MATERIALIZED\": OR REPLACE is not allowed here"));
    }
    let if_not_exists = c.eat_all(&["IF", "NOT", "EXISTS"]);
    if if_not_exists && !materialized {
        return Err(refusal("42601", "syntax error at or near \"NOT\": CREATE VIEW has no IF NOT EXISTS"));
    }
    let name = c.name().ok_or_else(|| refusal("42601", "syntax error in CREATE VIEW: expected a name"))?;
    let mut columns = None;
    if c.punct('(') {
        let mut list = Vec::new();
        loop {
            let column = c.name().ok_or_else(|| refusal("42601", "syntax error in the view's column list"))?;
            list.push(column);
            if c.punct(',') {
                continue;
            }
            if c.punct(')') {
                break;
            }
            return Err(refusal("42601", "syntax error in the view's column list"));
        }
        columns = Some(list);
    }
    if c.eat("WITH") {
        // `WITH (security_barrier, check_option = …)`: storage options.
        return Err(refusal("0A000", format!("{} options (WITH (…)) are not supported", kind.noun())));
    }
    if c.eat("USING") || c.eat("TABLESPACE") {
        return Err(refusal("0A000", format!("{} storage clauses are not supported", kind.noun())));
    }
    if !c.eat("AS") {
        return Err(refusal("42601", "syntax error in CREATE VIEW: expected AS"));
    }
    let mut query = c.rest().trim();
    let mut with_data = true;
    if materialized {
        if let Some(rest) = strip_trailing_words(query, &["WITH", "NO", "DATA"]) {
            query = rest;
            with_data = false;
        } else if let Some(rest) = strip_trailing_words(query, &["WITH", "DATA"]) {
            query = rest;
        }
    } else {
        for option in [&["WITH", "CHECK", "OPTION"][..], &["WITH", "LOCAL", "CHECK", "OPTION"], &["WITH", "CASCADED", "CHECK", "OPTION"]] {
            if strip_trailing_words(query, option).is_some() {
                return Err(refusal(
                    "0A000",
                    "WITH CHECK OPTION is not supported: views here are read-only, so there is nothing for it to check",
                ));
            }
        }
    }
    let query = query.trim();
    let head = query.trim_start().as_bytes();
    let starts = |w: &str| head.len() >= w.len() && head[..w.len()].eq_ignore_ascii_case(w.as_bytes());
    if query.is_empty() || !(starts("SELECT") || starts("WITH") || starts("VALUES") || starts("TABLE") || starts("(")) {
        return Err(refusal("42601", "syntax error in CREATE VIEW: expected a query after AS"));
    }
    let query = crate::core::rql::ctas::table_as_select(query).unwrap_or_else(|| query.to_string());
    Ok(ViewStatement::Create { name, columns, query, or_replace, kind, if_not_exists, with_data })
}

/// `text` without the trailing whitespace-separated `words`, ignoring case;
/// `None` when it does not end with them.
fn strip_trailing_words<'t>(text: &'t str, words: &[&str]) -> Option<&'t str> {
    let mut rest = text.trim_end();
    for word in words.iter().rev() {
        let at = rest.len().checked_sub(word.len())?;
        if !rest.is_char_boundary(at) || !rest[at..].eq_ignore_ascii_case(word) {
            return None;
        }
        let before = rest[..at].as_bytes().last().copied();
        if before.is_some_and(|b| !b.is_ascii_whitespace()) {
            return None;
        }
        rest = rest[..at].trim_end();
    }
    Some(rest)
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

/// Runs `sql` as transaction `tx` (a block's) or as a statement of its own.
///
/// A statement of its own runs on a task of its own. Run inline it would
/// open and end an owner scope inside the statement that asked (the one
/// reading a view), and that statement's CTE pass then adopted the ended
/// transaction: its temporaries were invisible and it returned no rows.
fn run(
    database: &Arc<Database>,
    sql: &str,
    tx: Option<TxId>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>> {
    // Boxed as `dyn … + Send`: the executor reaches this module again (a
    // describe of a view's query expands it), so the future would otherwise
    // contain itself and its `Send` could not be proven.
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};
    let database = database.clone();
    let sql = sql.to_string();
    Box::pin(async move {
        let purpose = recognize_query_purpose(&sql).ok_or_else(|| format!("could not parse: {sql}"))?;
        let response = match tx {
            Some(tx) => executor::execute_in_txn(purpose, database, tx).await,
            None => tokio::spawn(executor::execute_as(crate::core::mvcc::owner::OwnerKind::Statement, purpose, database))
                .await
                .map_err(|e| format!("the view catalogue statement did not finish: {e}"))?,
        };
        if response.first() == Some(&3) {
            return Err(String::from_utf8_lossy(&response[1..]).to_string());
        }
        Ok(response)
    })
}

fn lines(text: &str) -> Vec<String> {
    text.split('\n').filter(|l| !l.is_empty()).map(str::to_string).collect()
}

/// The views `tx` sees whose names are in `names`, or all of them for `None`.
pub async fn load(
    database: &Arc<Database>,
    names: Option<&[String]>,
    tx: Option<TxId>,
) -> Result<Vec<ViewDef>, String> {
    if !database.has_table(VIEW_TABLE) {
        return Ok(Vec::new());
    }
    let filter = match names {
        Some([]) => return Ok(Vec::new()),
        Some(names) => format!(
            " WHERE name IN ({})",
            names.iter().map(|n| crate::auth::quote(n)).collect::<Vec<_>>().join(", ")
        ),
        None => String::new(),
    };
    let sql = format!("SELECT name, kind, definition, columns, deps FROM {VIEW_TABLE}{filter}");
    let response = run(database, &sql, tx).await?;
    if response.first() != Some(&2) {
        return Ok(Vec::new());
    }
    let rows = crate::core::row::row::vec_into_rows(&response[1..]).map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let text = |i: usize| -> String {
            row.columns
                .get(i)
                .filter(|c| c.column_type != crate::core::db_type::DbType::NULL)
                .map(|c| String::from_utf8_lossy(c.data.into_slice()).into_owned())
                .unwrap_or_default()
        };
        out.push(ViewDef {
            name: text(0),
            kind: ViewKind::from_code(&text(1)),
            definition: text(2),
            columns: lines(&text(3)),
            deps: lines(&text(4)),
        });
    }
    Ok(out)
}

/// The view `name`, as `tx` sees it.
pub async fn lookup(database: &Arc<Database>, name: &str, tx: Option<TxId>) -> Result<Option<ViewDef>, String> {
    Ok(load(database, Some(&[name.to_string()]), tx).await?.into_iter().next())
}

/// Every view `tx` sees whose query names `relation`.
pub async fn dependents(database: &Arc<Database>, relation: &str, tx: Option<TxId>) -> Result<Vec<ViewDef>, String> {
    let lower = relation.to_ascii_lowercase();
    Ok(load(database, None, tx).await?.into_iter().filter(|v| v.deps.iter().any(|d| *d == lower)).collect())
}

/// Writes `view` (a new row, or the one under its name replaced).
pub async fn store(database: &Arc<Database>, view: &ViewDef, replace: bool, tx: Option<TxId>) -> Result<(), String> {
    use crate::auth::quote;
    if !database.has_table(VIEW_TABLE) {
        // A statement of its own: an empty table outliving a rolled-back
        // block holds nothing.
        run(database, VIEW_TABLE_DDL, None).await?;
    }
    let columns = view.columns.join("\n");
    let deps = view.deps.join("\n");
    let sql = if replace {
        format!(
            "UPDATE {VIEW_TABLE} SET kind = {}, definition = {}, columns = {}, deps = {} WHERE name = {}",
            quote(view.kind.code()),
            quote(&view.definition),
            quote(&columns),
            quote(&deps),
            quote(&view.name)
        )
    } else {
        format!(
            "INSERT INTO {VIEW_TABLE} (name, kind, definition, columns, deps) VALUES ({}, {}, {}, {}, {})",
            quote(&view.name),
            quote(view.kind.code()),
            quote(&view.definition),
            quote(&columns),
            quote(&deps)
        )
    };
    run(database, &sql, tx).await?;
    database.views.note(&view.name);
    Ok(())
}

/// Deletes the view `name`'s row.
pub async fn remove(database: &Arc<Database>, name: &str, tx: Option<TxId>) -> Result<(), String> {
    let sql = format!("DELETE FROM {VIEW_TABLE} WHERE name = {}", crate::auth::quote(name));
    run(database, &sql, tx).await.map(|_| ())
}

/// Renames the view `from` to `to`, and points every view that names it at
/// the new name.
pub async fn rename(database: &Arc<Database>, from: &str, to: &str, tx: Option<TxId>) -> Result<(), String> {
    use crate::auth::quote;
    let sql = format!("UPDATE {VIEW_TABLE} SET name = {} WHERE name = {}", quote(to), quote(from));
    run(database, &sql, tx).await?;
    database.views.note(to);
    retarget_dependents(database, from, to, tx).await
}

/// Points every view whose query names the relation `from` at `to`: after a
/// table or a view is renamed, as PostgreSQL's views follow a relation they
/// hold by OID.
pub async fn retarget_dependents(database: &Arc<Database>, from: &str, to: &str, tx: Option<TxId>) -> Result<(), String> {
    for mut view in dependents(database, from, tx).await? {
        view.definition = retarget(&view.definition, from, to);
        let lower_from = from.to_ascii_lowercase();
        for dep in view.deps.iter_mut() {
            if *dep == lower_from {
                *dep = to.to_ascii_lowercase();
            }
        }
        store(database, &view, true, tx).await?;
    }
    Ok(())
}

/// `sql` with every reference to the relation `from` written as `to`; an
/// unaliased one keeps `from` as its alias, so a column qualified by the old
/// name still reads.
fn retarget(sql: &str, from: &str, to: &str) -> String {
    let mut refs: Vec<RelationRef> = crate::core::rql::lock_set::relation_references(sql)
        .into_iter()
        .filter(|r| r.name.eq_ignore_ascii_case(from))
        .collect();
    refs.sort_by_key(|r| std::cmp::Reverse(r.span.start));
    let mut out = sql.to_string();
    for r in refs {
        let replacement = if r.aliased {
            quote_ident(to)
        } else {
            format!("{} AS {}", quote_ident(to), quote_ident(&r.name))
        };
        out.replace_range(r.span.clone(), &replacement);
    }
    out
}

/// The relations `query` names, lowercased, once each; the names its own
/// CTEs define are not relations.
pub fn query_deps(query: &str) -> Vec<String> {
    let ctes = cte_names(query);
    let mut out: Vec<String> = Vec::new();
    for r in crate::core::rql::lock_set::relation_references(query) {
        let lower = r.name.to_ascii_lowercase();
        if !ctes.contains(&lower) && !out.contains(&lower) {
            out.push(lower);
        }
    }
    out
}

/// The names the statement's own `WITH` clauses define, lowercased (every
/// depth, read off the text: a CTE shadows a view of the same name).
fn cte_names(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(Some((defs, _))) = crate::core::rql::cte::split_with(sql) {
        out.extend(defs.into_iter().map(|d| d.name.to_ascii_lowercase()));
    }
    // A WITH inside a subquery (`( WITH x AS (…)`), found by the word; a
    // match inside a literal is a body `split_with` does not read, or a name
    // that only keeps a view of that name from being expanded there.
    let bytes = sql.as_bytes();
    let mut at = 1;
    while at + 4 <= bytes.len() {
        let word = bytes[at..at + 4].eq_ignore_ascii_case(b"WITH")
            && !(bytes[at - 1].is_ascii_alphanumeric() || bytes[at - 1] == b'_')
            && bytes.get(at + 4).is_none_or(|b| !(b.is_ascii_alphanumeric() || *b == b'_'));
        if word && let Ok(Some((defs, _))) = crate::core::rql::cte::split_with(&sql[at..]) {
            out.extend(defs.into_iter().map(|d| d.name.to_ascii_lowercase()));
        }
        at += 1;
    }
    out
}

// ---------------------------------------------------------------------------
// Expansion
// ---------------------------------------------------------------------------

/// Whether `sql` may name a view of `database`: the cheap check that keeps
/// [`expand`] off every statement that names none.
pub async fn may_name_a_view(database: &Arc<Database>, sql: &str) -> bool {
    if !database.has_table(VIEW_TABLE) {
        return false;
    }
    let loaded_at = database.views.loaded_at.load(Ordering::Acquire);
    let now = now_ms();
    // Before the first read every statement reads (none may go on with no
    // names); after it, one statement per interval does.
    let first = loaded_at == 0;
    if first
        || now.saturating_sub(loaded_at) >= RELOAD_AFTER_MS
            && database.views.loaded_at.compare_exchange(loaded_at, now, Ordering::AcqRel, Ordering::Acquire).is_ok()
    {
        // Every committed view, added to the names (never taken from them:
        // the set only has to hold every view some snapshot may see). One
        // statement reads it; the others go on with the names as they are.
        if let Ok(all) = load(database, None, None).await {
            for view in all {
                database.views.note(&view.name);
            }
            if first {
                database.views.loaded_at.store(now, Ordering::Release);
            }
        }
    }
    database.views.mentioned_in(sql)
}

/// Whether `sql` reads or writes a relation this process knows as a view,
/// without reading the catalogue: for the native protocol, which cannot
/// read one yet.
pub fn names_a_known_view(database: &Database, sql: &str) -> bool {
    database.has_table(VIEW_TABLE)
        && database.views.mentioned_in(sql)
        && crate::core::rql::lock_set::relation_references(sql)
            .iter()
            .any(|r| database.views.names.contains_key(&r.name))
}

/// What expansion was allowed to leave as a write: `REFRESH` filling its
/// own materialized view.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExpandOptions<'a> {
    /// The materialized view a `REFRESH` is writing.
    pub writing_materialized: Option<&'a str>,
}

/// `sql` with every view it reads written as the derived table it stands for,
/// recursively; `Ok(None)` when it names no view. A write to a view, and to a
/// materialized view other than by `REFRESH`, is refused.
pub async fn expand(
    database: &Arc<Database>,
    sql: &str,
    tx: Option<TxId>,
    options: ExpandOptions<'_>,
) -> Result<Option<String>, Refusal> {
    if !may_name_a_view(database, sql).await {
        return Ok(None);
    }
    let mut text = sql.to_string();
    let mut cache: std::collections::HashMap<String, Option<ViewDef>> = std::collections::HashMap::new();
    let mut changed = false;
    for _ in 0..MAX_DEPTH {
        let ctes = cte_names(&text);
        let refs: Vec<RelationRef> = crate::core::rql::lock_set::relation_references(&text)
            .into_iter()
            .filter(|r| !ctes.contains(&r.name.to_ascii_lowercase()))
            .filter(|r| database.views.names.contains_key(&r.name))
            .collect();
        if refs.is_empty() {
            return Ok(changed.then_some(text));
        }
        let missing: Vec<String> = refs.iter().map(|r| r.name.clone()).filter(|n| !cache.contains_key(n)).collect();
        if !missing.is_empty() {
            let found = load(database, Some(&missing), tx).await.map_err(|e| refusal("XX000", e))?;
            for name in missing {
                let def = found.iter().find(|v| v.name == name).cloned();
                cache.insert(name, def);
            }
        }
        let mut replaced: Vec<(std::ops::Range<usize>, String)> = Vec::new();
        for r in &refs {
            let Some(Some(view)) = cache.get(&r.name) else {
                continue;
            };
            let writes = r.mode != crate::core::mvcc::table_locks::TableMode::AccessShare;
            match view.kind {
                ViewKind::Materialized => {
                    if writes && options.writing_materialized != Some(view.name.as_str()) {
                        return Err(refusal("42809", format!("cannot change materialized view \"{}\"", view.name)));
                    }
                }
                ViewKind::View => {
                    if writes {
                        // PostgreSQL's words for a view it cannot write
                        // through (55000), by the verb.
                        let before = text[..r.span.start].trim_end();
                        let last = before.rsplit(|c: char| c.is_whitespace()).next().unwrap_or("");
                        let action = if last.eq_ignore_ascii_case("INTO") {
                            "insert into"
                        } else if last.eq_ignore_ascii_case("FROM") {
                            "delete from"
                        } else {
                            "update"
                        };
                        return Err(refusal(
                            "55000",
                            format!(
                                "cannot {action} view \"{}\": views are not automatically updatable here; write to \
                                 the table it reads",
                                view.name
                            ),
                        ));
                    }
                    let derived = if r.aliased {
                        format!("({})", view.definition)
                    } else {
                        format!("({}) AS {}", view.definition, quote_ident(&r.name))
                    };
                    replaced.push((r.span.clone(), derived));
                }
            }
        }
        if replaced.is_empty() {
            return Ok(changed.then_some(text));
        }
        replaced.sort_by_key(|(span, _)| std::cmp::Reverse(span.start));
        for (span, derived) in replaced {
            text.replace_range(span, &derived);
        }
        changed = true;
    }
    Err(refusal("42P17", "infinite recursion detected in the views this statement reads"))
}

/// The views among `names`, as `tx` sees them, by name.
pub async fn views_among(
    database: &Arc<Database>,
    names: &[String],
    tx: Option<TxId>,
) -> Result<Vec<ViewDef>, String> {
    if !database.has_table(VIEW_TABLE) {
        return Ok(Vec::new());
    }
    load(database, Some(names), tx).await
}

/// A table DDL statement that a view's name or its dependencies bear on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableDdl {
    Create { name: String, if_not_exists: bool },
    Drop { names: Vec<String>, if_exists: bool, cascade: bool },
    Alter { name: String, rename_to: Option<String> },
}

/// The target of `CREATE TABLE`, `DROP TABLE` or `ALTER TABLE`; `None` for
/// any other statement.
pub fn table_ddl(sql: &str) -> Option<TableDdl> {
    let sql = sql.trim().trim_end_matches(';').trim_end();
    let mut c = Cursor::new(sql);
    if c.eat("CREATE") {
        let _ = c.eat("GLOBAL") || c.eat("LOCAL");
        let _ = c.eat("TEMP") || c.eat("TEMPORARY") || c.eat("UNLOGGED");
        if !c.eat("TABLE") {
            return None;
        }
        let if_not_exists = c.eat_all(&["IF", "NOT", "EXISTS"]);
        return Some(TableDdl::Create { name: c.name()?, if_not_exists });
    }
    if c.eat("DROP") {
        if !c.eat("TABLE") {
            return None;
        }
        let if_exists = c.eat_all(&["IF", "EXISTS"]);
        let mut names = Vec::new();
        loop {
            names.push(c.name()?);
            if !c.punct(',') {
                break;
            }
        }
        let cascade = c.eat("CASCADE");
        return Some(TableDdl::Drop { names, if_exists, cascade });
    }
    if c.eat("ALTER") {
        if !c.eat("TABLE") {
            return None;
        }
        c.eat_all(&["IF", "EXISTS"]);
        c.eat("ONLY");
        let name = c.name()?;
        let rename_to = if c.eat_all(&["RENAME", "TO"]) { c.name() } else { None };
        return Some(TableDdl::Alter { name, rename_to });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(sql: &str) -> ViewStatement {
        parse_view_statement(sql).expect("a view statement").expect("parses")
    }

    #[test]
    fn view_statements_are_read() {
        assert_eq!(
            create("CREATE OR REPLACE VIEW public.\"V\" (a, b) AS SELECT x, y FROM t;"),
            ViewStatement::Create {
                name: "V".into(),
                columns: Some(vec!["a".into(), "b".into()]),
                query: "SELECT x, y FROM t".into(),
                or_replace: true,
                kind: ViewKind::View,
                if_not_exists: false,
                with_data: true,
            }
        );
        assert_eq!(
            create("create materialized view if not exists mv as select 1 with no data"),
            ViewStatement::Create {
                name: "mv".into(),
                columns: None,
                query: "select 1".into(),
                or_replace: false,
                kind: ViewKind::Materialized,
                if_not_exists: true,
                with_data: false,
            }
        );
        assert_eq!(
            create("CREATE VIEW v AS TABLE t"),
            ViewStatement::Create {
                name: "v".into(),
                columns: None,
                query: "SELECT * FROM t".into(),
                or_replace: false,
                kind: ViewKind::View,
                if_not_exists: false,
                with_data: true,
            }
        );
        assert_eq!(
            create("DROP VIEW IF EXISTS a, public.b CASCADE"),
            ViewStatement::Drop { names: vec!["a".into(), "b".into()], kind: ViewKind::View, if_exists: true, cascade: true }
        );
        assert_eq!(
            create("REFRESH MATERIALIZED VIEW mv WITH NO DATA"),
            ViewStatement::Refresh { name: "mv".into(), with_data: false }
        );
        assert_eq!(
            create("ALTER VIEW v RENAME TO w"),
            ViewStatement::Rename { from: "v".into(), to: "w".into(), kind: ViewKind::View, if_exists: false }
        );
        assert!(parse_view_statement("CREATE TABLE t (a INT)").is_none());
        assert!(parse_view_statement("DROP TABLE v").is_none());
        assert!(parse_view_statement("CREATE VIEW v AS SELECT 1 WITH CHECK OPTION").unwrap().is_err());
        assert!(parse_view_statement("CREATE TEMP VIEW v AS SELECT 1").unwrap().is_err());
        assert!(parse_view_statement("CREATE VIEW v").unwrap().is_err());
    }

    #[test]
    fn table_ddl_targets_are_read() {
        assert_eq!(
            table_ddl("CREATE TABLE IF NOT EXISTS public.\"T\" (a INT)"),
            Some(TableDdl::Create { name: "T".into(), if_not_exists: true })
        );
        assert_eq!(
            table_ddl("drop table if exists a, b cascade;"),
            Some(TableDdl::Drop { names: vec!["a".into(), "b".into()], if_exists: true, cascade: true })
        );
        assert_eq!(table_ddl("ALTER TABLE t RENAME TO u"), Some(TableDdl::Alter { name: "t".into(), rename_to: Some("u".into()) }));
        assert_eq!(table_ddl("ALTER TABLE t RENAME COLUMN a TO b"), Some(TableDdl::Alter { name: "t".into(), rename_to: None }));
        assert_eq!(table_ddl("CREATE INDEX i ON t (a)"), None);
    }

    #[test]
    fn a_reference_is_retargeted_and_keeps_its_name_as_alias() {
        assert_eq!(
            retarget("SELECT t.a FROM t JOIN u x ON x.id = t.id WHERE t.a IN (SELECT a FROM t)", "t", "t2"),
            "SELECT t.a FROM \"t2\" AS \"t\" JOIN u x ON x.id = t.id WHERE t.a IN (SELECT a FROM \"t2\" AS \"t\")"
        );
        assert_eq!(retarget("SELECT * FROM t AS z", "t", "t2"), "SELECT * FROM \"t2\" AS z");
    }

    #[test]
    fn a_ctes_name_is_no_dependency() {
        assert_eq!(query_deps("WITH v AS (SELECT * FROM t) SELECT * FROM v JOIN u ON true"), vec!["t", "u"]);
        assert_eq!(query_deps("SELECT * FROM (WITH w AS (SELECT 1) SELECT * FROM w) z, s"), vec!["s"]);
    }
}
