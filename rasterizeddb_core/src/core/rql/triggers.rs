//! `CREATE TRIGGER`, `DROP TRIGGER`, and the PL/pgSQL a trigger function is
//! written in.
//!
//! A trigger is a row of the system table [`TRIGGER_TABLE`], written in the
//! statement's transaction as a view's is. Firing is the PostgreSQL
//! listener's (`pgwire::trigger_exec`): a write to a table that has triggers
//! is run a row at a time, with each trigger's function interpreted here
//! ([`PlBlock`]) over the row's `OLD` and `NEW`.
//!
//! # The PL/pgSQL this reads
//!
//! `[DECLARE name type [:= expr]; …] BEGIN … END`, with these statements:
//! `NEW.col := expr` (or `=`), `var := expr`, `IF … THEN … [ELSIF … THEN …]
//! [ELSE …] END IF`, `RETURN NEW | OLD | NULL`, `RAISE [EXCEPTION | NOTICE |
//! WARNING | INFO | LOG | DEBUG] 'format' [, args]`, `PERFORM …`, `NULL`, and
//! `INSERT`, `UPDATE`, `DELETE` and `SELECT` statements. Anything else is
//! refused by name when the function is first fired, never skipped.

use std::sync::atomic::{AtomicU64, Ordering};

use rclite::Arc;

use crate::core::database::Database;
use crate::core::mvcc::TxId;

/// The system table triggers are kept in.
pub const TRIGGER_TABLE: &str = "__rdb_triggers";

const TRIGGER_TABLE_DDL: &str = "CREATE TABLE __rdb_triggers (id TEXT PRIMARY KEY, name TEXT NOT NULL, \
     tbl TEXT NOT NULL, timing TEXT NOT NULL, events TEXT NOT NULL, update_columns TEXT NOT NULL, \
     row_level BOOLEAN NOT NULL, when_condition TEXT NOT NULL, function TEXT NOT NULL, args TEXT NOT NULL)";

/// A refusal: its SQLSTATE and message.
pub type Refusal = (&'static str, String);

/// When a trigger fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timing {
    Before,
    After,
}

/// What a trigger fires on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    Insert,
    Update,
    Delete,
    Truncate,
}

impl Event {
    pub fn word(self) -> &'static str {
        match self {
            Event::Insert => "INSERT",
            Event::Update => "UPDATE",
            Event::Delete => "DELETE",
            Event::Truncate => "TRUNCATE",
        }
    }
    fn from_word(word: &str) -> Option<Event> {
        Some(match word.to_ascii_uppercase().as_str() {
            "INSERT" => Event::Insert,
            "UPDATE" => Event::Update,
            "DELETE" => Event::Delete,
            "TRUNCATE" => Event::Truncate,
            _ => return None,
        })
    }
}

/// A stored trigger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerDef {
    pub name: String,
    pub table: String,
    pub timing: Timing,
    pub events: Vec<Event>,
    /// `UPDATE OF a, b`: the columns whose assignment fires it; empty for any.
    pub update_columns: Vec<String>,
    pub row_level: bool,
    /// `WHEN (…)`, or "".
    pub when: String,
    pub function: String,
    pub args: Vec<String>,
}

impl TriggerDef {
    fn id(&self) -> String {
        format!("{}/{}", self.table, self.name)
    }
}

/// A trigger statement, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerStatement {
    Create { trigger: TriggerDef, or_replace: bool },
    Drop { name: String, table: String, if_exists: bool },
}

/// The tables that may have triggers, a superset, as `views::ViewRegistry`
/// keeps view names.
#[derive(Default)]
pub struct TriggerRegistry {
    tables: dashmap::DashMap<String, ()>,
    loaded_at: AtomicU64,
}

fn ident(word: &str) -> String {
    let w = word.trim();
    if w.starts_with('"') && w.ends_with('"') && w.len() >= 2 {
        w[1..w.len() - 1].replace("\"\"", "\"")
    } else {
        w.to_ascii_lowercase()
    }
}

fn name_of(word: &str) -> String {
    ident(crate::core::sql_scan::last_name_segment(word.trim()))
}

/// Splits a statement head into words, keeping quoted names, `( … )` groups
/// and string literals whole.
fn words(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let start = i;
        if b == b'(' {
            i = crate::core::sql_scan::matching_close(sql, i).map_or(bytes.len(), |c| c + 1);
        } else if b == b'"' {
            i = crate::core::sql_scan::quoted_end(bytes, i, b'"');
        } else if b == b'\'' {
            i = crate::core::sql_scan::string_literal_end(bytes, i, false);
        } else if b == b',' {
            i += 1;
        } else {
            while i < bytes.len() && !bytes[i].is_ascii_whitespace() && !matches!(bytes[i], b'(' | b',') {
                if bytes[i] == b'"' {
                    i = crate::core::sql_scan::quoted_end(bytes, i, b'"');
                } else {
                    i += 1;
                }
            }
        }
        out.push(sql[start..i].to_string());
    }
    out
}

/// Parses `CREATE [OR REPLACE] [CONSTRAINT] TRIGGER` and `DROP TRIGGER`;
/// `None` for any other statement.
pub fn parse_trigger_statement(sql: &str) -> Option<Result<TriggerStatement, Refusal>> {
    let text = sql.trim().trim_end_matches(';').trim_end();
    let w = words(text);
    let is = |i: usize, kw: &str| w.get(i).is_some_and(|x| x.eq_ignore_ascii_case(kw));
    if is(0, "DROP") && is(1, "TRIGGER") {
        let mut i = 2;
        let if_exists = is(i, "IF") && is(i + 1, "EXISTS");
        if if_exists {
            i += 2;
        }
        let Some(name) = w.get(i).map(|n| name_of(n)) else {
            return Some(Err(("42601", "syntax error in DROP TRIGGER: expected a name".to_string())));
        };
        if !is(i + 1, "ON") {
            return Some(Err(("42601", "syntax error in DROP TRIGGER: expected ON table".to_string())));
        }
        let Some(table) = w.get(i + 2).map(|t| name_of(t)) else {
            return Some(Err(("42601", "syntax error in DROP TRIGGER: expected a table".to_string())));
        };
        return Some(Ok(TriggerStatement::Drop { name, table, if_exists }));
    }
    if !is(0, "CREATE") {
        return None;
    }
    let mut i = 1;
    let mut or_replace = false;
    if is(i, "OR") && is(i + 1, "REPLACE") {
        or_replace = true;
        i += 2;
    }
    if is(i, "CONSTRAINT") {
        return Some(Err(("0A000", "constraint triggers are not supported".to_string())));
    }
    if !is(i, "TRIGGER") {
        return None;
    }
    Some(parse_create(&w, i + 1, or_replace))
}

fn parse_create(w: &[String], mut i: usize, or_replace: bool) -> Result<TriggerStatement, Refusal> {
    let is = |i: usize, kw: &str| w.get(i).is_some_and(|x| x.eq_ignore_ascii_case(kw));
    let syntax = |what: &str| ("42601", format!("syntax error in CREATE TRIGGER: {what}"));
    let name = w.get(i).map(|n| name_of(n)).ok_or_else(|| syntax("expected a name"))?;
    i += 1;
    let timing = if is(i, "BEFORE") {
        Timing::Before
    } else if is(i, "AFTER") {
        Timing::After
    } else if is(i, "INSTEAD") {
        return Err(("0A000", "INSTEAD OF triggers are not supported: views here are not updatable".to_string()));
    } else {
        return Err(syntax("expected BEFORE or AFTER"));
    };
    i += 1;
    let mut events = Vec::new();
    let mut update_columns = Vec::new();
    loop {
        let event = w.get(i).and_then(|e| Event::from_word(e)).ok_or_else(|| syntax("expected INSERT, UPDATE, DELETE or TRUNCATE"))?;
        i += 1;
        if event == Event::Update && is(i, "OF") {
            i += 1;
            while let Some(column) = w.get(i) {
                if column.eq_ignore_ascii_case("OR") || column.eq_ignore_ascii_case("ON") {
                    break;
                }
                if column != "," {
                    update_columns.push(ident(column));
                }
                i += 1;
            }
        }
        events.push(event);
        if is(i, "OR") {
            i += 1;
            continue;
        }
        break;
    }
    if !is(i, "ON") {
        return Err(syntax("expected ON table"));
    }
    let table = w.get(i + 1).map(|t| name_of(t)).ok_or_else(|| syntax("expected a table"))?;
    i += 2;
    if is(i, "FROM") || is(i, "NOT") || is(i, "DEFERRABLE") || is(i, "INITIALLY") {
        return Err(("0A000", "deferrable and constraint trigger options are not supported".to_string()));
    }
    if is(i, "REFERENCING") {
        return Err(("0A000", "transition tables (REFERENCING) are not supported yet".to_string()));
    }
    let mut row_level = false;
    if is(i, "FOR") {
        i += 1;
        if is(i, "EACH") {
            i += 1;
        }
        row_level = if is(i, "ROW") {
            true
        } else if is(i, "STATEMENT") {
            false
        } else {
            return Err(syntax("expected ROW or STATEMENT"));
        };
        i += 1;
    }
    let mut when = String::new();
    if is(i, "WHEN") {
        let group = w.get(i + 1).filter(|g| g.starts_with('(')).ok_or_else(|| syntax("WHEN needs a parenthesised condition"))?;
        when = group[1..group.len() - 1].trim().to_string();
        i += 2;
    }
    if !is(i, "EXECUTE") || !(is(i + 1, "FUNCTION") || is(i + 1, "PROCEDURE")) {
        return Err(syntax("expected EXECUTE FUNCTION"));
    }
    let function = w.get(i + 2).map(|f| name_of(f)).ok_or_else(|| syntax("expected a function"))?;
    let args = match w.get(i + 3) {
        Some(group) if group.starts_with('(') => crate::core::sql_scan::split_top_level(&group[1..group.len() - 1], b',')
            .into_iter()
            .map(|a| a.trim().trim_matches('\'').replace("''", "'"))
            .filter(|a| !a.is_empty())
            .collect(),
        _ => return Err(syntax("expected the function's argument list")),
    };
    if !row_level && !when.is_empty() && (when.to_ascii_uppercase().contains("NEW.") || when.to_ascii_uppercase().contains("OLD.")) {
        return Err(("42P17", "statement trigger's WHEN condition cannot reference column values".to_string()));
    }
    if events.contains(&Event::Truncate) && row_level {
        return Err(("0A000", "TRUNCATE FOR EACH ROW triggers are not supported".to_string()));
    }
    Ok(TriggerStatement::Create {
        trigger: TriggerDef { name, table, timing, events, update_columns, row_level, when, function, args },
        or_replace,
    })
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

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
                .map_err(|e| format!("the trigger catalogue statement did not finish: {e}"))?,
        };
        if response.first() == Some(&3) {
            return Err(String::from_utf8_lossy(&response[1..]).to_string());
        }
        Ok(response)
    })
}

fn joined(items: &[String]) -> String {
    items.join("\u{1}")
}

fn split_joined(text: &str) -> Vec<String> {
    if text.is_empty() { Vec::new() } else { text.split('\u{1}').map(str::to_string).collect() }
}

/// The triggers `tx` sees, all of them or `table`'s.
pub async fn load(database: &Arc<Database>, table: Option<&str>, tx: Option<TxId>) -> Result<Vec<TriggerDef>, String> {
    if !database.has_table(TRIGGER_TABLE) {
        return Ok(Vec::new());
    }
    let filter = table.map(|t| format!(" WHERE tbl = {}", crate::auth::quote(t))).unwrap_or_default();
    let sql = format!(
        "SELECT name, tbl, timing, events, update_columns, row_level, when_condition, function, args FROM {TRIGGER_TABLE}{filter}"
    );
    let response = run(database, &sql, tx).await?;
    if response.first() != Some(&2) {
        return Ok(Vec::new());
    }
    let rows = crate::core::row::row::vec_into_rows(&response[1..]).map_err(|e| e.to_string())?;
    let mut out: Vec<TriggerDef> = rows
        .into_iter()
        .map(|row| {
            let text = |i: usize| {
                row.columns.get(i).map(|c| String::from_utf8_lossy(c.data.into_slice()).into_owned()).unwrap_or_default()
            };
            let flag = |i: usize| row.columns.get(i).is_some_and(|c| c.data.into_slice().first().copied().unwrap_or(0) != 0);
            TriggerDef {
                name: text(0),
                table: text(1),
                timing: if text(2) == "before" { Timing::Before } else { Timing::After },
                events: split_joined(&text(3)).iter().filter_map(|e| Event::from_word(e)).collect(),
                update_columns: split_joined(&text(4)),
                row_level: flag(5),
                when: text(6),
                function: text(7),
                args: split_joined(&text(8)),
            }
        })
        .collect();
    // PostgreSQL fires a table's triggers of one kind in name order.
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// The triggers whose function is `function`.
pub async fn users_of(database: &Arc<Database>, function: &str, tx: Option<TxId>) -> Vec<TriggerDef> {
    load(database, None, tx).await.unwrap_or_default().into_iter().filter(|t| t.function == function).collect()
}

pub async fn store(database: &Arc<Database>, trigger: &TriggerDef, replace: bool, tx: Option<TxId>) -> Result<(), String> {
    use crate::auth::quote;
    if !database.has_table(TRIGGER_TABLE) {
        run(database, TRIGGER_TABLE_DDL, None).await?;
    }
    if replace {
        run(database, &format!("DELETE FROM {TRIGGER_TABLE} WHERE id = {}", quote(&trigger.id())), tx).await?;
    }
    let events: Vec<String> = trigger.events.iter().map(|e| e.word().to_string()).collect();
    let sql = format!(
        "INSERT INTO {TRIGGER_TABLE} (id, name, tbl, timing, events, update_columns, row_level, when_condition, function, args) \
         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
        quote(&trigger.id()),
        quote(&trigger.name),
        quote(&trigger.table),
        quote(if trigger.timing == Timing::Before { "before" } else { "after" }),
        quote(&joined(&events)),
        quote(&joined(&trigger.update_columns)),
        trigger.row_level,
        quote(&trigger.when),
        quote(&trigger.function),
        quote(&joined(&trigger.args))
    );
    run(database, &sql, tx).await?;
    database.triggers.tables.insert(trigger.table.clone(), ());
    Ok(())
}

pub async fn remove(database: &Arc<Database>, trigger: &TriggerDef, tx: Option<TxId>) -> Result<(), String> {
    let sql = format!("DELETE FROM {TRIGGER_TABLE} WHERE id = {}", crate::auth::quote(&trigger.id()));
    run(database, &sql, tx).await.map(|_| ())
}

/// Removes every trigger of `table`, for a table being dropped.
pub async fn remove_table(database: &Arc<Database>, table: &str, tx: Option<TxId>) -> Result<(), String> {
    if !database.has_table(TRIGGER_TABLE) {
        return Ok(());
    }
    let sql = format!("DELETE FROM {TRIGGER_TABLE} WHERE tbl = {}", crate::auth::quote(table));
    run(database, &sql, tx).await.map(|_| ())
}

/// Whether `table` may have triggers: the check that keeps a write to a
/// table with none on its ordinary path.
pub async fn may_have_triggers(database: &Arc<Database>, table: &str) -> bool {
    if !database.has_table(TRIGGER_TABLE) {
        return false;
    }
    let registry = &database.triggers;
    let loaded_at = registry.loaded_at.load(Ordering::Acquire);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let first = loaded_at == 0;
    if first
        || now.saturating_sub(loaded_at) >= 2_000
            && registry.loaded_at.compare_exchange(loaded_at, now, Ordering::AcqRel, Ordering::Acquire).is_ok()
    {
        if let Ok(all) = load(database, None, None).await {
            for trigger in all {
                registry.tables.insert(trigger.table, ());
            }
            if first {
                registry.loaded_at.store(now, Ordering::Release);
            }
        }
    }
    registry.tables.contains_key(table)
}

// ---------------------------------------------------------------------------
// PL/pgSQL
// ---------------------------------------------------------------------------

/// A function body, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlBlock {
    /// `DECLARE`d variables: name, type, initial value (an expression).
    pub declarations: Vec<(String, String, Option<String>)>,
    pub body: Vec<PlStmt>,
}

/// One PL/pgSQL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlStmt {
    /// `NEW.col := expr`, `OLD.col := expr` or `var := expr`: the target as
    /// written (lowercased), and the expression.
    Assign { target: String, expr: String },
    If { branches: Vec<(String, Vec<PlStmt>)>, otherwise: Vec<PlStmt> },
    /// `RETURN expr`, `None` for a bare `RETURN`.
    Return(Option<String>),
    Raise { level: String, format: String, args: Vec<String>, errcode: Option<String> },
    /// `PERFORM …`: a query run for its effects.
    Perform(String),
    /// An SQL statement.
    Sql(String),
    Null,
}

/// The statements of a block's text: split at `;` outside quotes,
/// parentheses and dollar quotes.
fn statements(text: &str) -> Vec<String> {
    crate::pgwire::split_statements(text)
}

/// The first word of `text`, uppercase.
fn head(text: &str) -> String {
    text.split(|c: char| c.is_whitespace() || c == '(' || c == ';').next().unwrap_or("").to_ascii_uppercase()
}

/// Where a top-level keyword `kw` starts in `text`, as a whole word, outside
/// quotes and parentheses.
fn keyword_at(text: &str, kw: &str) -> Option<usize> {
    crate::core::rql::join::find_keyword(text, kw, 0)
}

/// Parses a PL/pgSQL function body.
pub fn parse_plpgsql(body: &str) -> Result<PlBlock, Refusal> {
    let unsupported = |what: &str| ("0A000", format!("PL/pgSQL {what} is not supported yet"));
    let mut text = body.trim().trim_end_matches(';').trim_end().to_string();
    // A `<<label>>` before the block.
    if text.starts_with("<<") {
        if let Some(end) = text.find(">>") {
            text = text[end + 2..].trim_start().to_string();
        }
    }
    let mut declarations = Vec::new();
    if head(&text) == "DECLARE" {
        let begin = keyword_at(&text, "BEGIN").ok_or_else(|| ("42601", "DECLARE without BEGIN".to_string()))?;
        for decl in statements(&text[7..begin]) {
            let decl = decl.trim();
            if decl.is_empty() {
                continue;
            }
            let (name, rest) = decl.split_once(char::is_whitespace).ok_or_else(|| ("42601", format!("bad declaration: {decl}")))?;
            let rest = rest.trim();
            let (ty, init) = match rest.find(":=").map(|a| (a, 2)).or_else(|| keyword_at(rest, "DEFAULT").map(|a| (a, 7))) {
                Some((at, len)) => (rest[..at].trim(), Some(rest[at + len..].trim().to_string())),
                None => (rest, None),
            };
            if ty.to_ascii_uppercase().contains("RECORD") || ty.contains('%') {
                return Err(unsupported("RECORD and %TYPE variables"));
            }
            let ty = ty.trim_start_matches("CONSTANT ").trim_start_matches("constant ");
            declarations.push((ident(name), ty.trim().to_ascii_lowercase(), init));
        }
        text = text[begin..].to_string();
    }
    if head(&text) != "BEGIN" {
        return Err(("42601", "a PL/pgSQL body is BEGIN … END".to_string()));
    }
    let upper = text.to_ascii_uppercase();
    let end = upper.rfind("END").filter(|e| text[*e + 3..].trim().chars().all(|c| c.is_alphanumeric() || c == '_'))
        .ok_or_else(|| ("42601", "a PL/pgSQL body ends with END".to_string()))?;
    let inner = &text[5..end];
    let mut flat: Vec<String> = statements(inner).into_iter().map(|s| s.trim().to_string()).collect();
    flat.retain(|s| !s.is_empty());
    // An `EXCEPTION WHEN …` section opens a statement; the word inside
    // `RAISE EXCEPTION` does not.
    if flat.iter().any(|s| head(s) == "EXCEPTION" || keyword_at(s, "EXCEPTION").is_some_and(|at| s[..at].trim_end().to_ascii_uppercase().ends_with("END IF") || s[..at].trim().is_empty())) {
        return Err(unsupported("EXCEPTION blocks"));
    }
    let mut at = 0;
    let body = block(&flat, &mut at, &[])?;
    Ok(PlBlock { declarations, body })
}

/// Statements from `items[*at]` until one opening with a word of `stops`
/// (left for the caller), nesting `IF … END IF`.
fn block(items: &[String], at: &mut usize, stops: &[&str]) -> Result<Vec<PlStmt>, Refusal> {
    let mut out = Vec::new();
    while *at < items.len() {
        let item = items[*at].clone();
        let first = head(&item);
        if stops.iter().any(|s| first == *s || (first == "END" && item.to_ascii_uppercase().starts_with(s))) {
            return Ok(out);
        }
        *at += 1;
        // An `IF … THEN <first statement>`: the split at `;` keeps the first
        // statement of a branch with its condition.
        if first == "IF" {
            out.push(if_statement(item, items, at)?);
            continue;
        }
        out.push(simple(&item)?);
    }
    Ok(out)
}

fn if_statement(first: String, items: &[String], at: &mut usize) -> Result<PlStmt, Refusal> {
    let mut branches: Vec<(String, Vec<PlStmt>)> = Vec::new();
    let mut otherwise: Vec<PlStmt> = Vec::new();
    // The current piece: `IF cond THEN stmt`, `ELSIF cond THEN stmt`, `ELSE
    // stmt`, or `END IF`.
    let mut piece = first;
    let mut in_else = false;
    loop {
        let upper_head = head(&piece);
        let mut rest: Option<String> = None;
        if upper_head == "IF" || upper_head == "ELSIF" || upper_head == "ELSEIF" {
            let then = keyword_at(&piece, "THEN").ok_or_else(|| ("42601", format!("IF without THEN: {piece}")))?;
            let cond = piece[upper_head.len()..then].trim().to_string();
            branches.push((cond, Vec::new()));
            let tail = piece[then + 4..].trim();
            if !tail.is_empty() {
                rest = Some(tail.to_string());
            }
        } else if upper_head == "ELSE" {
            in_else = true;
            let tail = piece[4..].trim();
            if !tail.is_empty() {
                rest = Some(tail.to_string());
            }
        } else {
            return Err(("42601", format!("unexpected {piece} in IF")));
        }
        // The branch's statements: the one after THEN/ELSE, then the rest up
        // to the next ELSIF, ELSE or END IF.
        let mut stmts = Vec::new();
        if let Some(tail) = rest {
            if head(&tail) == "IF" {
                stmts.push(if_statement(tail, items, at)?);
            } else {
                // `IF c THEN x := 1 ELSE …` on one line is not a split point;
                // only `;` ends a statement.
                stmts.push(simple(&tail)?);
            }
        }
        stmts.extend(block(items, at, &["ELSIF", "ELSEIF", "ELSE", "END IF"])?);
        if in_else {
            otherwise = stmts;
        } else if let Some(last) = branches.last_mut() {
            last.1 = stmts;
        }
        let Some(next) = items.get(*at).cloned() else {
            return Err(("42601", "IF without END IF".to_string()));
        };
        *at += 1;
        if next.to_ascii_uppercase().starts_with("END IF") {
            return Ok(PlStmt::If { branches, otherwise });
        }
        piece = next;
    }
}

fn simple(item: &str) -> Result<PlStmt, Refusal> {
    let unsupported = |what: &str| ("0A000", format!("PL/pgSQL {what} is not supported yet"));
    let first = head(item);
    Ok(match first.as_str() {
        "RETURN" => {
            let rest = item[6..].trim();
            if rest.to_ascii_uppercase().starts_with("NEXT") || rest.to_ascii_uppercase().starts_with("QUERY") {
                return Err(unsupported("RETURN NEXT/QUERY"));
            }
            PlStmt::Return((!rest.is_empty()).then(|| rest.to_string()))
        }
        "RAISE" => raise(item)?,
        "PERFORM" => PlStmt::Perform(format!("SELECT {}", item[7..].trim())),
        "NULL" => PlStmt::Null,
        "INSERT" | "UPDATE" | "DELETE" | "SELECT" | "WITH" => {
            if first == "SELECT" && keyword_at(item, "INTO").is_some() {
                return Err(unsupported("SELECT … INTO"));
            }
            PlStmt::Sql(item.to_string())
        }
        // DDL and the other utility statements run as they are; `EXECUTE
        // expr` is the dynamic statement, whose text is computed when it
        // runs (`trigger_exec`).
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" | "GRANT" | "REVOKE" | "COMMENT" | "MERGE" | "NOTIFY" | "EXECUTE" => {
            PlStmt::Sql(item.to_string())
        }
        "LOOP" | "WHILE" | "FOR" | "FOREACH" | "EXIT" | "CONTINUE" | "CASE" | "GET" | "OPEN" | "FETCH"
        | "CLOSE" | "BEGIN" | "DECLARE" | "COMMIT" | "ROLLBACK" | "CALL" | "ASSERT" => return Err(unsupported(&first)),
        _ => {
            let (at, len) = match item.find(":=") {
                Some(at) => (at, 2),
                None => (item.find('=').ok_or_else(|| ("42601", format!("syntax error in PL/pgSQL: {item}")))?, 1),
            };
            let target = item[..at].trim().to_ascii_lowercase();
            if target.is_empty() || target.contains(' ') {
                return Err(("42601", format!("syntax error in PL/pgSQL: {item}")));
            }
            PlStmt::Assign { target, expr: item[at + len..].trim().to_string() }
        }
    })
}

fn raise(item: &str) -> Result<PlStmt, Refusal> {
    let rest = item[5..].trim();
    let levels = ["EXCEPTION", "NOTICE", "WARNING", "INFO", "LOG", "DEBUG"];
    let first = head(rest);
    let (level, rest) = match levels.iter().find(|l| **l == first) {
        Some(level) => (level.to_string(), rest[level.len()..].trim()),
        None => ("EXCEPTION".to_string(), rest),
    };
    let (main, using) = match keyword_at(rest, "USING") {
        Some(at) => (rest[..at].trim(), Some(rest[at + 5..].trim())),
        None => (rest, None),
    };
    let mut errcode = None;
    if let Some(using) = using {
        for option in crate::core::sql_scan::split_top_level(using, b',') {
            let (key, value) = option.split_once('=').unwrap_or((option, ""));
            if key.trim().eq_ignore_ascii_case("ERRCODE") {
                errcode = Some(value.trim().trim_matches('\'').to_string());
            }
        }
    }
    let parts = crate::core::sql_scan::split_top_level(main, b',');
    let format = parts.first().map(|f| f.trim()).unwrap_or("");
    if !format.starts_with('\'') {
        return Err(("0A000", "RAISE without a format string is not supported yet".to_string()));
    }
    let format = format[1..format.len().saturating_sub(1)].replace("''", "'");
    let args = parts.iter().skip(1).map(|a| a.trim().to_string()).collect();
    Ok(PlStmt::Raise { level, format, args, errcode })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_statements_are_read() {
        let TriggerStatement::Create { trigger, or_replace } = parse_trigger_statement(
            "CREATE OR REPLACE TRIGGER set_at BEFORE INSERT OR UPDATE OF a, b ON public.t FOR EACH ROW \
             WHEN (NEW.a IS NOT NULL) EXECUTE FUNCTION touch('x', 'y')",
        )
        .unwrap()
        .unwrap() else {
            panic!()
        };
        assert!(or_replace);
        assert_eq!(
            trigger,
            TriggerDef {
                name: "set_at".into(),
                table: "t".into(),
                timing: Timing::Before,
                events: vec![Event::Insert, Event::Update],
                update_columns: vec!["a".into(), "b".into()],
                row_level: true,
                when: "NEW.a IS NOT NULL".into(),
                function: "touch".into(),
                args: vec!["x".into(), "y".into()],
            }
        );
        assert_eq!(
            parse_trigger_statement("DROP TRIGGER IF EXISTS set_at ON t").unwrap().unwrap(),
            TriggerStatement::Drop { name: "set_at".into(), table: "t".into(), if_exists: true }
        );
        assert!(parse_trigger_statement("CREATE TRIGGER x INSTEAD OF INSERT ON v FOR EACH ROW EXECUTE FUNCTION f()").unwrap().is_err());
    }

    #[test]
    fn plpgsql_is_read() {
        let block = parse_plpgsql(
            "DECLARE n integer := 0; BEGIN \
               IF NEW.v IS NULL THEN RAISE EXCEPTION 'v is %', NEW.v USING ERRCODE = '23502'; \
               ELSIF TG_OP = 'UPDATE' THEN NEW.at := now(); n := n + 1; \
               ELSE NEW.at = NULL; END IF; \
               INSERT INTO log VALUES (NEW.id); \
               RETURN NEW; \
             END;",
        )
        .unwrap();
        assert_eq!(block.declarations, vec![("n".into(), "integer".into(), Some("0".into()))]);
        assert_eq!(block.body.len(), 3);
        let PlStmt::If { branches, otherwise } = &block.body[0] else { panic!() };
        assert_eq!(branches.len(), 2);
        assert_eq!(branches[1].1, vec![
            PlStmt::Assign { target: "new.at".into(), expr: "now()".into() },
            PlStmt::Assign { target: "n".into(), expr: "n + 1".into() },
        ]);
        assert_eq!(otherwise, &vec![PlStmt::Assign { target: "new.at".into(), expr: "NULL".into() }]);
        assert_eq!(block.body[2], PlStmt::Return(Some("NEW".into())));
        assert!(parse_plpgsql("BEGIN LOOP NULL; END LOOP; END").is_err());
    }
}
