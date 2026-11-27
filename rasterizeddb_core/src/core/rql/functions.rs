//! `CREATE FUNCTION`, `CREATE PROCEDURE`, `CALL`, and how a call is run.
//!
//! # Storage
//!
//! A routine is a row of the system table [`FUNCTION_TABLE`], written in the
//! statement's transaction as a view's is (`rql::views`). Routines are told
//! apart by name and argument count; `CREATE OR REPLACE` replaces the one
//! with the same count.
//!
//! # Running
//!
//! A `LANGUAGE sql` function is inlined where it is called, before anything
//! plans the statement ([`inline_calls`]): its arguments are put in for its
//! parameters (`$1`, or the parameter's name), each cast to the parameter's
//! type, and the call becomes
//!
//! - `CAST((<expression>) AS <type>)` for a body that is one `SELECT
//!   <expression>` with no clause after it,
//! - `CAST((<query>) AS <type>)`, a scalar subquery, for any other body, and
//! - `(<query>) AS <alias>`, a derived table, for a set-returning function
//!   called in a `FROM` list.
//!
//! A `LANGUAGE plpgsql` function whose body is `BEGIN RETURN <expression>;
//! END` is inlined the same way; any other plpgsql body is kept (a trigger's
//! function is run by `rql::triggers`) and refused if called as a value.
//!
//! A procedure's body is its statements, run in order by `CALL` in the
//! caller's transaction.

use std::sync::atomic::{AtomicU64, Ordering};

use rclite::Arc;

use crate::core::database::Database;
use crate::core::mvcc::TxId;

/// The system table routines are kept in.
pub const FUNCTION_TABLE: &str = "__rdb_functions";

const FUNCTION_TABLE_DDL: &str = "CREATE TABLE __rdb_functions (id TEXT PRIMARY KEY, name TEXT NOT NULL, \
     kind TEXT NOT NULL, arg_names TEXT NOT NULL, arg_types TEXT NOT NULL, return_type TEXT NOT NULL, \
     returns_set BOOLEAN NOT NULL, language TEXT NOT NULL, body TEXT NOT NULL)";

/// How deep calls may nest before inlining stops (a function calling
/// itself is refused here, as an infinite expansion).
const MAX_DEPTH: usize = 32;

/// A refusal: its SQLSTATE and message.
pub type Refusal = (&'static str, String);

/// A function or a procedure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutineKind {
    Function,
    Procedure,
}

impl RoutineKind {
    fn code(self) -> &'static str {
        match self {
            RoutineKind::Function => "f",
            RoutineKind::Procedure => "p",
        }
    }
    pub fn noun(self) -> &'static str {
        match self {
            RoutineKind::Function => "function",
            RoutineKind::Procedure => "procedure",
        }
    }
}

/// A stored routine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Routine {
    pub name: String,
    pub kind: RoutineKind,
    /// Each parameter's name, "" when it has none (`$n` only).
    pub arg_names: Vec<String>,
    pub arg_types: Vec<String>,
    pub return_type: String,
    pub returns_set: bool,
    pub language: String,
    pub body: String,
}

impl Routine {
    /// The key a routine is stored under: its name and argument count.
    fn id(&self) -> String {
        format!("{}/{}", self.name, self.arg_types.len())
    }
}

/// A routine statement, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutineStatement {
    /// `security_definer`: `SECURITY DEFINER`, kept as an ACL marker
    /// (`privileges::DEFINER`) rather than in the routine's row.
    Create { routine: Routine, or_replace: bool, security_definer: bool },
    Drop { kind: RoutineKind, targets: Vec<(String, Option<usize>)>, if_exists: bool },
    Call { name: String, args: Vec<String> },
}

/// The names of the routines a database may hold, a superset, as
/// `views::ViewRegistry` keeps view names.
#[derive(Default)]
pub struct FunctionRegistry {
    names: dashmap::DashMap<String, ()>,
    loaded_at: AtomicU64,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

const RELOAD_AFTER_MS: u64 = 2_000;

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// A quoted name as written, an unquoted one lowercased.
fn ident(word: &str) -> String {
    let w = word.trim();
    if w.starts_with('"') && w.ends_with('"') && w.len() >= 2 {
        w[1..w.len() - 1].replace("\"\"", "\"")
    } else {
        w.to_ascii_lowercase()
    }
}

/// The last segment of a possibly qualified name, as [`ident`] reads it.
fn routine_name(word: &str) -> String {
    ident(crate::core::sql_scan::last_name_segment(word.trim()))
}

/// A type as written, normalised for comparison: lowercase, single spaces.
fn type_text(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ").to_ascii_lowercase()
}

/// The words a parameter may open with that are not its name.
const ARG_MODES: &[&str] = &["in", "out", "inout", "variadic"];

/// `(a int, b text DEFAULT 'x')` read as names and types. A parameter with
/// one word is a type with no name.
fn parameters(list: &str) -> Result<(Vec<String>, Vec<String>), Refusal> {
    let mut names = Vec::new();
    let mut types = Vec::new();
    for piece in crate::core::sql_scan::split_top_level(list, b',') {
        let piece = piece.trim();
        if piece.is_empty() {
            continue;
        }
        let upper = piece.to_ascii_uppercase();
        if upper.contains(" DEFAULT ") || upper.contains('=') {
            return Err(("0A000", "parameter defaults are not supported yet".to_string()));
        }
        let mut words: Vec<&str> = piece.split_whitespace().collect();
        if words.first().is_some_and(|w| ARG_MODES.iter().any(|m| w.eq_ignore_ascii_case(m))) {
            let mode = words.remove(0).to_ascii_lowercase();
            if mode != "in" {
                return Err(("0A000", format!("{} parameters are not supported yet", mode.to_uppercase())));
            }
        }
        // `name type …` unless the first word is itself a type (`int`, or a
        // multi-word one such as `double precision`).
        let first_is_type = words.len() == 1
            || crate::core::rql::lexer_ct::column_type_span(&words).ok().flatten().is_some_and(|(_, _, used)| used == words.len());
        if first_is_type {
            names.push(String::new());
            types.push(type_text(&words.join(" ")));
        } else {
            names.push(ident(words[0]));
            types.push(type_text(&words[1..].join(" ")));
        }
    }
    Ok((names, types))
}

/// The body after `AS`: a dollar-quoted or single-quoted string's contents.
fn quoted_body(text: &str) -> Option<(String, usize)> {
    let bytes = text.as_bytes();
    if bytes.first() == Some(&b'$') {
        let tag = crate::core::sql_scan::dollar_tag_len(bytes, 0)?;
        let end = crate::core::sql_scan::dollar_quote_end(bytes, 0)?;
        if end < 2 * tag {
            return None;
        }
        return Some((text[tag..end - tag].to_string(), end));
    }
    if bytes.first() == Some(&b'\'') {
        let end = crate::core::sql_scan::string_literal_end(bytes, 0, false);
        if end < 2 {
            return None;
        }
        return Some((text[1..end - 1].replace("''", "'"), end));
    }
    None
}

/// Parses `CREATE [OR REPLACE] FUNCTION|PROCEDURE`, `DROP FUNCTION|PROCEDURE`
/// and `CALL`; `None` for any other statement.
pub fn parse_routine_statement(sql: &str) -> Option<Result<RoutineStatement, Refusal>> {
    let text = sql.trim().trim_end_matches(';').trim_end();
    let upper = text.to_ascii_uppercase();
    let mut words = upper.split_whitespace();
    match words.next()? {
        "CREATE" => {
            let mut second = words.next()?;
            let mut or_replace = false;
            if second == "OR" {
                if words.next()? != "REPLACE" {
                    return None;
                }
                or_replace = true;
                second = words.next()?;
            }
            let kind = match second {
                "FUNCTION" => RoutineKind::Function,
                "PROCEDURE" => RoutineKind::Procedure,
                _ => return None,
            };
            Some(parse_create(text, kind, or_replace))
        }
        "DROP" => {
            let kind = match words.next()? {
                "FUNCTION" => RoutineKind::Function,
                "PROCEDURE" => RoutineKind::Procedure,
                _ => return None,
            };
            Some(parse_drop(text, kind))
        }
        "CALL" => {
            let rest = text[4..].trim();
            let open = rest.find('(')?;
            let name = routine_name(&rest[..open]);
            let close = crate::core::sql_scan::matching_close(rest, open)?;
            let args = crate::core::sql_scan::split_top_level(&rest[open + 1..close], b',')
                .into_iter()
                .map(|a| a.trim().to_string())
                .filter(|a| !a.is_empty())
                .collect();
            Some(Ok(RoutineStatement::Call { name, args }))
        }
        _ => None,
    }
}

fn parse_create(text: &str, kind: RoutineKind, or_replace: bool) -> Result<RoutineStatement, Refusal> {
    let syntax = |what: &str| ("42601", format!("syntax error in CREATE {}: {what}", kind.noun().to_uppercase()));
    let keyword = kind.noun().to_ascii_uppercase();
    let at = text.to_ascii_uppercase().find(&keyword).ok_or_else(|| syntax("expected a name"))? + keyword.len();
    let rest = &text[at..];
    let open = rest.find('(').ok_or_else(|| syntax("expected a parameter list"))?;
    let name = routine_name(&rest[..open]);
    let close = crate::core::sql_scan::matching_close(rest, open).ok_or_else(|| syntax("unbalanced parameter list"))?;
    let (arg_names, arg_types) = parameters(&rest[open + 1..close])?;
    let mut tail = rest[close + 1..].trim();

    let mut return_type = String::new();
    let mut returns_set = false;
    let mut security_definer = false;
    let mut language: Option<String> = None;
    let mut body: Option<String> = None;
    while !tail.is_empty() {
        let upper = tail.to_ascii_uppercase();
        let word_end = upper.find(char::is_whitespace).unwrap_or(upper.len());
        let word = &upper[..word_end];
        match word {
            "RETURNS" => {
                tail = tail[word_end..].trim_start();
                let upper = tail.to_ascii_uppercase();
                if upper.starts_with("TABLE") {
                    let open = tail.find('(').ok_or_else(|| syntax("RETURNS TABLE needs its columns"))?;
                    let close = crate::core::sql_scan::matching_close(tail, open).ok_or_else(|| syntax("unbalanced RETURNS TABLE"))?;
                    return_type = format!("table({})", type_text(&tail[open + 1..close]));
                    returns_set = true;
                    tail = tail[close + 1..].trim_start();
                } else {
                    if upper.starts_with("SETOF ") {
                        returns_set = true;
                        tail = tail[6..].trim_start();
                    }
                    // The type runs to the next clause keyword.
                    let upper = tail.to_ascii_uppercase();
                    let end = [" LANGUAGE ", " AS ", " IMMUTABLE", " STABLE", " VOLATILE", " STRICT", " RETURN ", " BEGIN ", " SECURITY ", " COST ", " PARALLEL ", " CALLED ", " LEAKPROOF", " SET ", " RETURNS NULL"]
                        .iter()
                        .filter_map(|k| upper.find(k))
                        .min()
                        .unwrap_or(tail.len());
                    return_type = type_text(&tail[..end]);
                    tail = tail[end..].trim_start();
                }
            }
            "LANGUAGE" => {
                tail = tail[word_end..].trim_start();
                let end = tail.find(char::is_whitespace).unwrap_or(tail.len());
                language = Some(tail[..end].trim_matches(['\'', '"']).to_ascii_lowercase());
                tail = tail[end..].trim_start();
            }
            "AS" => {
                tail = tail[word_end..].trim_start();
                let (text, used) = quoted_body(tail).ok_or_else(|| syntax("expected the body as a quoted string"))?;
                body = Some(text);
                tail = tail[used..].trim_start();
                // `AS 'obj_file', 'link_symbol'`: a C function.
                if tail.starts_with(',') {
                    return Err(("0A000", "functions in C are not supported".to_string()));
                }
            }
            // The SQL-standard body: `RETURN <expression>` or `BEGIN ATOMIC
            // … END`, to the end of the statement.
            "RETURN" => {
                body = Some(format!("SELECT {}", tail[word_end..].trim()));
                language.get_or_insert_with(|| "sql".to_string());
                tail = "";
            }
            "BEGIN" if upper.starts_with("BEGIN ATOMIC") => {
                let inner = tail["BEGIN ATOMIC".len()..].trim();
                let inner = inner.strip_suffix("END").or_else(|| inner.strip_suffix("end")).unwrap_or(inner);
                body = Some(inner.trim().to_string());
                language.get_or_insert_with(|| "sql".to_string());
                tail = "";
            }
            "IMMUTABLE" | "STABLE" | "VOLATILE" | "STRICT" | "LEAKPROOF" => tail = tail[word_end..].trim_start(),
            "NOT" if upper.starts_with("NOT LEAKPROOF") => tail = tail["NOT LEAKPROOF".len()..].trim_start(),
            "CALLED" if upper.starts_with("CALLED ON NULL INPUT") => tail = tail["CALLED ON NULL INPUT".len()..].trim_start(),
            "SECURITY" => {
                security_definer = upper.starts_with("SECURITY DEFINER");
                let skip = if security_definer { "SECURITY DEFINER" } else { "SECURITY INVOKER" };
                tail = tail[skip.len().min(tail.len())..].trim_start();
            }
            "PARALLEL" | "COST" | "ROWS" => {
                let after = tail[word_end..].trim_start();
                let end = after.find(char::is_whitespace).unwrap_or(after.len());
                tail = after[end..].trim_start();
            }
            "WINDOW" => return Err(("0A000", "window functions defined by CREATE FUNCTION are not supported".to_string())),
            "SET" => return Err(("0A000", "a function's SET clause is not supported yet".to_string())),
            _ => return Err(syntax(&format!("unexpected \"{}\"", &tail[..word_end]))),
        }
    }
    let language = language.ok_or_else(|| ("42P13", "no language specified".to_string()))?;
    if !matches!(language.as_str(), "sql" | "plpgsql") {
        return Err(("42704", format!("language \"{language}\" does not exist")));
    }
    let body = body.ok_or_else(|| ("42P13", format!("no function body specified")))?;
    if kind == RoutineKind::Function && return_type.is_empty() {
        return Err(("42P13", "function result type must be specified".to_string()));
    }
    let routine = Routine {
        name,
        kind,
        arg_names,
        arg_types,
        return_type,
        returns_set,
        language,
        body: body.trim().to_string(),
    };
    Ok(RoutineStatement::Create { routine, or_replace, security_definer })
}

fn parse_drop(text: &str, kind: RoutineKind) -> Result<RoutineStatement, Refusal> {
    let keyword = kind.noun().to_ascii_uppercase();
    let at = text.to_ascii_uppercase().find(&keyword).map(|a| a + keyword.len()).unwrap_or(text.len());
    let mut rest = text[at..].trim();
    let mut if_exists = false;
    if rest.len() >= 9 && rest[..9].eq_ignore_ascii_case("IF EXISTS") {
        if_exists = true;
        rest = rest[9..].trim();
    }
    for suffix in ["CASCADE", "RESTRICT"] {
        if rest.len() >= suffix.len() && rest.as_bytes()[rest.len() - suffix.len()..].eq_ignore_ascii_case(suffix.as_bytes()) {
            rest = rest[..rest.len() - suffix.len()].trim();
        }
    }
    let mut targets = Vec::new();
    for piece in crate::core::sql_scan::split_top_level(rest, b',') {
        let piece = piece.trim();
        match piece.find('(') {
            Some(open) => {
                let close = crate::core::sql_scan::matching_close(piece, open)
                    .ok_or_else(|| ("42601", "unbalanced argument list in DROP".to_string()))?;
                let (_, types) = parameters(&piece[open + 1..close])?;
                targets.push((routine_name(&piece[..open]), Some(types.len())));
            }
            None => targets.push((routine_name(piece), None)),
        }
    }
    if targets.is_empty() {
        return Err(("42601", format!("DROP {keyword} names nothing")));
    }
    Ok(RoutineStatement::Drop { kind, targets, if_exists })
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
                .map_err(|e| format!("the routine catalogue statement did not finish: {e}"))?,
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

/// The routines `tx` sees named `name` (every one for `None`).
pub async fn load(database: &Arc<Database>, name: Option<&str>, tx: Option<TxId>) -> Result<Vec<Routine>, String> {
    if !database.has_table(FUNCTION_TABLE) {
        return Ok(Vec::new());
    }
    let filter = name.map(|n| format!(" WHERE name = {}", crate::auth::quote(n))).unwrap_or_default();
    let sql = format!(
        "SELECT name, kind, arg_names, arg_types, return_type, returns_set, language, body FROM {FUNCTION_TABLE}{filter}"
    );
    let response = run(database, &sql, tx).await?;
    if response.first() != Some(&2) {
        return Ok(Vec::new());
    }
    let rows = crate::core::row::row::vec_into_rows(&response[1..]).map_err(|e| e.to_string())?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let text = |i: usize| {
                row.columns.get(i).map(|c| String::from_utf8_lossy(c.data.into_slice()).into_owned()).unwrap_or_default()
            };
            let flag = |i: usize| row.columns.get(i).is_some_and(|c| c.data.into_slice().first().copied().unwrap_or(0) != 0);
            Routine {
                name: text(0),
                kind: if text(1) == "p" { RoutineKind::Procedure } else { RoutineKind::Function },
                arg_names: split_joined(&text(2)),
                arg_types: split_joined(&text(3)),
                return_type: text(4),
                returns_set: flag(5),
                language: text(6),
                body: text(7),
            }
        })
        .collect())
}

/// Writes `routine`, replacing one of its name and argument count.
pub async fn store(database: &Arc<Database>, routine: &Routine, replace: bool, tx: Option<TxId>) -> Result<(), String> {
    use crate::auth::quote;
    if !database.has_table(FUNCTION_TABLE) {
        run(database, FUNCTION_TABLE_DDL, None).await?;
    }
    if replace {
        run(database, &format!("DELETE FROM {FUNCTION_TABLE} WHERE id = {}", quote(&routine.id())), tx).await?;
    }
    let sql = format!(
        "INSERT INTO {FUNCTION_TABLE} (id, name, kind, arg_names, arg_types, return_type, returns_set, language, body) \
         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {})",
        quote(&routine.id()),
        quote(&routine.name),
        quote(routine.kind.code()),
        quote(&joined(&routine.arg_names)),
        quote(&joined(&routine.arg_types)),
        quote(&routine.return_type),
        routine.returns_set,
        quote(&routine.language),
        quote(&routine.body)
    );
    run(database, &sql, tx).await?;
    database.functions.names.insert(routine.name.clone(), ());
    Ok(())
}

/// Deletes `routine`'s row.
pub async fn remove(database: &Arc<Database>, routine: &Routine, tx: Option<TxId>) -> Result<(), String> {
    let sql = format!("DELETE FROM {FUNCTION_TABLE} WHERE id = {}", crate::auth::quote(&routine.id()));
    run(database, &sql, tx).await.map(|_| ())
}

/// Whether `sql` may call a routine of `database`, by name: the check that
/// keeps [`inline_calls`] off every statement that calls none.
pub async fn may_call_a_routine(database: &Arc<Database>, sql: &str) -> bool {
    if !database.has_table(FUNCTION_TABLE) {
        return false;
    }
    let registry = &database.functions;
    let loaded_at = registry.loaded_at.load(Ordering::Acquire);
    let now = now_ms();
    let first = loaded_at == 0;
    if first
        || now.saturating_sub(loaded_at) >= RELOAD_AFTER_MS
            && registry.loaded_at.compare_exchange(loaded_at, now, Ordering::AcqRel, Ordering::Acquire).is_ok()
    {
        if let Ok(all) = load(database, None, None).await {
            for routine in all {
                registry.names.insert(routine.name, ());
            }
            if first {
                registry.loaded_at.store(now, Ordering::Release);
            }
        }
    }
    registry
        .names
        .iter()
        .any(|e| crate::core::sql_scan::contains_ignore_ascii_case(sql, e.key().as_bytes()))
}

// ---------------------------------------------------------------------------
// Calls
// ---------------------------------------------------------------------------

/// A call of a known name in `sql`: where it is written (name to `)`), its
/// name and its argument texts.
struct Call {
    span: std::ops::Range<usize>,
    name: String,
    args: Vec<String>,
}

/// Every `name(…)` in `sql` outside literals, quoted identifiers and
/// comments, whose name is one of `names` and which is not qualified by
/// another relation (`t.f(…)` is no call here).
/// The names of the routines of `database` that `sql` calls.
pub fn called_routines(database: &Database, sql: &str) -> Vec<String> {
    let known = |n: &str| database.functions.names.contains_key(n);
    let mut out: Vec<String> = calls_in(sql, &known).into_iter().map(|c| c.name).collect();
    out.sort();
    out.dedup();
    out
}

fn calls_in(sql: &str, names: &dyn Fn(&str) -> bool) -> Vec<Call> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            i = crate::core::sql_scan::string_literal_end(bytes, i, false);
            continue;
        }
        if b == b'$' && let Some(end) = crate::core::sql_scan::dollar_quote_end(bytes, i) {
            i = end;
            continue;
        }
        if b == b'"' || b.is_ascii_alphabetic() || b == b'_' {
            let start = i;
            let (word, end) = if b == b'"' {
                let end = crate::core::sql_scan::quoted_end(bytes, i, b'"');
                (ident(&sql[start..end]), end)
            } else {
                let mut j = i;
                while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_' || bytes[j] == b'$') {
                    j += 1;
                }
                (sql[start..j].to_ascii_lowercase(), j)
            };
            i = end;
            // `public.f(…)` names the function in the one schema; `t.f(…)`
            // after any other qualifier is not a routine call.
            let qualified_by = start
                .checked_sub(1)
                .filter(|p| bytes[*p] == b'.')
                .map(|p| {
                    let mut q = p;
                    while q > 0 && (bytes[q - 1].is_ascii_alphanumeric() || bytes[q - 1] == b'_') {
                        q -= 1;
                    }
                    (q, sql[q..p].to_ascii_lowercase())
                });
            if qualified_by.as_ref().is_some_and(|(_, q)| q != "public") {
                continue;
            }
            let mut k = end;
            while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                k += 1;
            }
            if bytes.get(k) != Some(&b'(') || !names(&word) {
                continue;
            }
            let Some(close) = crate::core::sql_scan::matching_close(sql, k) else {
                continue;
            };
            let args = crate::core::sql_scan::split_top_level(&sql[k + 1..close], b',')
                .into_iter()
                .map(|a| a.trim().to_string())
                .filter(|a| !a.is_empty())
                .collect();
            let span_start = qualified_by.map_or(start, |(q, _)| q);
            out.push(Call { span: span_start..close + 1, name: word, args });
            continue;
        }
        i += 1;
    }
    out
}

/// `body` with each parameter replaced by its argument: `$n`, and a named
/// parameter wherever it stands as a bare word outside literals and quoted
/// identifiers (not after a `.`, which makes it a column of a relation).
fn substitute(body: &str, routine: &Routine, args: &[String]) -> String {
    let typed: Vec<String> = args
        .iter()
        .zip(&routine.arg_types)
        .map(|(arg, ty)| format!("CAST(({arg}) AS {ty})"))
        .collect();
    let bytes = body.as_bytes();
    let mut out = String::with_capacity(body.len() + 16);
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            let end = crate::core::sql_scan::string_literal_end(bytes, i, false);
            out.push_str(&body[i..end]);
            i = end;
            continue;
        }
        if b == b'"' {
            let end = crate::core::sql_scan::quoted_end(bytes, i, b'"');
            out.push_str(&body[i..end]);
            i = end;
            continue;
        }
        if b == b'$' && bytes.get(i + 1).is_some_and(u8::is_ascii_digit) {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            let n: usize = body[i + 1..j].parse().unwrap_or(0);
            match typed.get(n.wrapping_sub(1)) {
                Some(arg) => out.push_str(arg),
                None => out.push_str(&body[i..j]),
            }
            i = j;
            continue;
        }
        if b.is_ascii_alphabetic() || b == b'_' {
            let mut j = i;
            while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                j += 1;
            }
            let word = body[i..j].to_ascii_lowercase();
            let after_dot = i > 0 && bytes[i - 1] == b'.';
            let before_dot = bytes.get(j) == Some(&b'.');
            match routine.arg_names.iter().position(|n| !n.is_empty() && *n == word) {
                Some(at) if !after_dot && !before_dot => out.push_str(&typed[at]),
                _ => out.push_str(&body[i..j]),
            }
            i = j;
            continue;
        }
        let ch = body[i..].chars().next().expect("in bounds");
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// A plpgsql body of the one form inlined: `[DECLARE …] BEGIN RETURN
/// <expression>; END` with nothing else. The expression, or `None`.
fn plpgsql_return_expression(body: &str) -> Option<String> {
    let trimmed = body.trim().trim_end_matches(';').trim_end();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("BEGIN") || !upper.ends_with("END") {
        return None;
    }
    let inner = trimmed[5..trimmed.len() - 3].trim().trim_end_matches(';').trim();
    let inner_upper = inner.to_ascii_uppercase();
    if !inner_upper.starts_with("RETURN ") || inner.contains(';') {
        return None;
    }
    Some(inner[7..].trim().to_string())
}

/// What a call of `routine` with `args` is written as in a statement.
fn call_text(routine: &Routine, args: &[String], in_from: bool, alias: &str) -> Result<String, Refusal> {
    if args.len() != routine.arg_types.len() {
        return Err((
            "42883",
            format!("function {}({} arguments) does not exist", routine.name, args.len()),
        ));
    }
    let body = match routine.language.as_str() {
        "sql" => routine.body.trim().trim_end_matches(';').trim().to_string(),
        _ => match plpgsql_return_expression(&routine.body) {
            Some(expression) => format!("SELECT {expression}"),
            None if routine.return_type == "trigger" => {
                return Err((
                    "0A000",
                    format!("trigger functions can only be called as triggers: {}()", routine.name),
                ));
            }
            None => {
                return Err((
                    "0A000",
                    format!(
                        "PL/pgSQL function {}: only a body of BEGIN RETURN <expression>; END can be called yet",
                        routine.name
                    ),
                ));
            }
        },
    };
    if body.contains(';') {
        return Err(("0A000", format!("function {}: a body of several statements is not supported yet", routine.name)));
    }
    let body = substitute(&body, routine, args);
    if in_from {
        // Aliased by the statement (`f(1) AS x`), or by its own name.
        return Ok(if alias.is_empty() {
            format!("({body})")
        } else {
            format!("({body}) AS {}", crate::core::rql::views::quote_ident(alias))
        });
    }
    if routine.returns_set {
        return Err(("0A000", format!("set-returning function {} must be called in a FROM list here", routine.name)));
    }
    // `SELECT <expression>` alone: the expression itself.
    let lone_expression = {
        let upper = body.to_ascii_uppercase();
        upper.starts_with("SELECT ")
            && ["FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION", "INTERSECT", "EXCEPT", "OFFSET"]
                .iter()
                .all(|kw| crate::core::rql::join::find_keyword(&body, kw, 0).is_none())
            && crate::core::sql_scan::split_top_level(&body[7..], b',').len() == 1
    };
    let value = if lone_expression {
        let expression = body[7..].trim();
        // `SELECT x AS y`: the alias is the column's name, not the value.
        match crate::core::rql::join::find_keyword(expression, "AS", 0) {
            Some(at) => expression[..at].trim().to_string(),
            None => expression.to_string(),
        }
    } else {
        body
    };
    if routine.return_type == "void" {
        return Ok(format!("(({value}) IS NULL AND NULL)"));
    }
    Ok(format!("CAST(({value}) AS {})", routine.return_type))
}

/// `sql` with every call of a routine of `database` inlined, recursively;
/// `Ok(None)` when it calls none. See the module comment.
///
/// `role`, when given, must hold EXECUTE on each function called (R2;
/// `privileges::may_execute`); `None` checks nothing (a superuser, a
/// describe that runs nothing).
///
/// `invokers_only` leaves a `SECURITY DEFINER` function's call as written:
/// the caller's privileges are checked on the text before those are put in
/// (`privilege_statements::definer_refusal` checks their bodies as their
/// owners), so their bodies are inlined after, by a second call.
pub async fn inline_calls(
    database: &Arc<Database>,
    sql: &str,
    tx: Option<TxId>,
    role: Option<&str>,
    invokers_only: bool,
) -> Result<Option<String>, Refusal> {
    if !may_call_a_routine(database, sql).await {
        return Ok(None);
    }
    let mut acl: Option<std::sync::Arc<Vec<crate::core::rql::privileges::AclRow>>> = None;
    let mut cache: std::collections::HashMap<String, Vec<Routine>> = std::collections::HashMap::new();
    let mut text = sql.to_string();
    let mut changed = false;
    for _ in 0..MAX_DEPTH {
        let known = |n: &str| database.functions.names.contains_key(n);
        let found = calls_in(&text, &known);
        // The outermost calls this pass: one inside another's arguments is
        // put into that body and inlined by the next.
        let calls: Vec<Call> = found
            .iter()
            .filter(|c| !found.iter().any(|o| o.span != c.span && o.span.start <= c.span.start && c.span.end <= o.span.end))
            .map(|c| Call { span: c.span.clone(), name: c.name.clone(), args: c.args.clone() })
            .collect();
        if calls.is_empty() {
            return Ok(changed.then_some(text));
        }
        // The set-returning calls in a FROM list, by where they start.
        let from_items: Vec<(usize, bool)> = crate::core::rql::lock_set::function_references(&text)
            .into_iter()
            .map(|r| (r.span.start, r.aliased))
            .collect();
        let mut replaced: Vec<(std::ops::Range<usize>, String)> = Vec::new();
        for call in &calls {
            if !cache.contains_key(&call.name) {
                let found = load(database, Some(&call.name), tx).await.map_err(|e| ("XX000", e))?;
                cache.insert(call.name.clone(), found);
            }
            let routines = &cache[&call.name];
            let Some(routine) = routines
                .iter()
                .find(|r| r.kind == RoutineKind::Function && r.arg_types.len() == call.args.len())
                .or_else(|| routines.iter().find(|r| r.kind == RoutineKind::Function))
            else {
                continue;
            };
            if role.is_some() || invokers_only {
                if acl.is_none() {
                    acl = Some(crate::core::rql::privileges::load_shared(database, tx).await.map_err(|e| ("XX000", e))?);
                }
            }
            let key = format!("@function:{}", routine.name);
            if invokers_only
                && acl.as_deref().is_some_and(|rows| crate::core::rql::privileges::is_definer(rows, &key))
            {
                continue;
            }
            if let Some(role) = role {
                if !crate::core::rql::privileges::may_execute(acl.as_deref().map(Vec::as_slice).unwrap_or_default(), role, &key) {
                    return Err(("42501", format!("permission denied for function {}", routine.name)));
                }
            }
            let from = from_items.iter().find(|(start, _)| *start == call.span.start);
            let text_for = match from {
                Some((_, aliased)) => {
                    let alias = if *aliased { "" } else { call.name.as_str() };
                    call_text(routine, &call.args, true, alias)?
                }
                None => call_text(routine, &call.args, false, "")?,
            };
            replaced.push((call.span.clone(), text_for));
        }
        if replaced.is_empty() {
            return Ok(changed.then_some(text));
        }
        replaced.sort_by_key(|(span, _)| std::cmp::Reverse(span.start));
        for (span, value) in replaced {
            text.replace_range(span, &value);
        }
        changed = true;
    }
    Err(("54001", "stack depth limit exceeded: a function calls itself".to_string()))
}

/// A procedure's statements with `args` put in for its parameters, for
/// `CALL`.
pub fn procedure_statements(routine: &Routine, args: &[String]) -> Result<Vec<String>, Refusal> {
    if routine.language != "sql" {
        return Err(("0A000", format!("procedure {}: only LANGUAGE sql procedures can be called yet", routine.name)));
    }
    if args.len() != routine.arg_types.len() {
        return Err(("42883", format!("procedure {}({} arguments) does not exist", routine.name, args.len())));
    }
    Ok(crate::pgwire::split_statements(&substitute(&routine.body, routine, args)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(sql: &str) -> Routine {
        match parse_routine_statement(sql).expect("a routine statement").expect("parses") {
            RoutineStatement::Create { routine, .. } => routine,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn routines_are_read() {
        let r = create("CREATE OR REPLACE FUNCTION public.add(a integer, b integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT a + b $$");
        assert_eq!((r.name.as_str(), r.arg_names.clone(), r.arg_types.clone()), ("add", vec!["a".into(), "b".into()], vec!["integer".into(), "integer".into()]));
        assert_eq!((r.return_type.as_str(), r.language.as_str(), r.body.as_str()), ("integer", "sql", "SELECT a + b"));
        let r = create("CREATE FUNCTION f(int, text) RETURNS SETOF t AS 'SELECT * FROM t WHERE id = $1' LANGUAGE sql");
        assert_eq!((r.arg_names.clone(), r.returns_set, r.body.as_str()), (vec![String::new(), String::new()], true, "SELECT * FROM t WHERE id = $1"));
        let r = create("CREATE FUNCTION g(x double precision) RETURNS double precision RETURN x * 2");
        assert_eq!((r.arg_types[0].as_str(), r.body.as_str()), ("double precision", "SELECT x * 2"));
        let r = create("CREATE FUNCTION tr() RETURNS trigger AS $body$ BEGIN NEW.at := now(); RETURN NEW; END; $body$ LANGUAGE plpgsql");
        assert_eq!((r.return_type.as_str(), r.language.as_str()), ("trigger", "plpgsql"));
        assert_eq!(
            parse_routine_statement("DROP FUNCTION IF EXISTS add(int, int), f CASCADE").unwrap().unwrap(),
            RoutineStatement::Drop { kind: RoutineKind::Function, targets: vec![("add".into(), Some(2)), ("f".into(), None)], if_exists: true }
        );
        assert_eq!(
            parse_routine_statement("CALL p(1, 'a, b')").unwrap().unwrap(),
            RoutineStatement::Call { name: "p".into(), args: vec!["1".into(), "'a, b'".into()] }
        );
        assert!(parse_routine_statement("CREATE FUNCTION f() RETURNS int AS 'x' LANGUAGE c").unwrap().is_err());
        assert!(parse_routine_statement("CREATE TABLE f (a INT)").is_none());
    }

    #[test]
    fn a_call_is_its_body_with_its_arguments() {
        let r = create("CREATE FUNCTION add(a integer, b integer) RETURNS integer LANGUAGE sql AS $$ SELECT a + b + $1 $$");
        assert_eq!(
            call_text(&r, &["x.a".into(), "2".into()], false, "").unwrap(),
            "CAST((CAST((x.a) AS integer) + CAST((2) AS integer) + CAST((x.a) AS integer)) AS integer)"
        );
        let r = create("CREATE FUNCTION n() RETURNS bigint LANGUAGE sql AS 'SELECT count(*) FROM t'");
        assert_eq!(call_text(&r, &[], false, "").unwrap(), "CAST((SELECT count(*) FROM t) AS bigint)");
        let r = create("CREATE FUNCTION d(v text) RETURNS text LANGUAGE plpgsql AS $$ BEGIN RETURN upper(v); END $$");
        assert_eq!(call_text(&r, &["'q'".into()], false, "").unwrap(), "CAST((upper(CAST(('q') AS text))) AS text)");
        assert!(call_text(&r, &[], false, "").is_err());
    }

    #[test]
    fn calls_are_found_outside_literals() {
        let names = |n: &str| n == "f";
        let calls = calls_in("SELECT f(1, (2)), 'f(3)', t.f(4), public.f(5), \"f\"(6) FROM x", &names);
        let found: Vec<(String, Vec<String>)> = calls.into_iter().map(|c| (c.name, c.args)).collect();
        assert_eq!(found, [("f".into(), vec!["1".into(), "(2)".into()]), ("f".into(), vec!["5".into()]), ("f".into(), vec!["6".into()])]);
    }
}
