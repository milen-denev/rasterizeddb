//! `CREATE TEMP TABLE`: a table only its session sees, gone when the session
//! ends.
//!
//! The table is a real one under a name of the statement temporaries'
//! prefix (`__cte_tmp_<session>_<name>`), which gives it memory-backed
//! storage, no placement, and reaping at a restart (`cte::TEMP_TABLE_PREFIX`).
//! The session keeps the names it made ([`TempTable`]) and writes each
//! reference to one as the real name before anything plans the statement: a
//! temporary table shadows a permanent one of its name, as `pg_temp` comes
//! first on PostgreSQL's search path. `ON COMMIT DELETE ROWS` and `ON COMMIT
//! DROP` act when the transaction that holds them ends; `DISCARD TEMP` and
//! `DISCARD ALL` drop them all.

use super::*;

/// What happens to a temporary table at the end of each transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OnCommit {
    PreserveRows,
    DeleteRows,
    Drop,
}

/// One of the session's temporary tables.
#[derive(Debug, Clone)]
pub(super) struct TempTable {
    /// The name the session writes (lowercased unless it was quoted).
    pub(super) name: String,
    /// The table's real name.
    pub(super) real: String,
    pub(super) on_commit: OnCommit,
}

fn ident(word: &str) -> String {
    let w = word.trim();
    if w.starts_with('"') && w.ends_with('"') && w.len() >= 2 {
        w[1..w.len() - 1].replace("\"\"", "\"")
    } else {
        w.to_ascii_lowercase()
    }
}

/// An alias a reference can be written with that names `name` again: bare
/// when it is a plain lowercase identifier (this engine holds a quoted alias
/// to its quotes).
fn alias_of(name: &str) -> String {
    let plain = name.bytes().next().is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    if plain { name.to_string() } else { crate::core::rql::views::quote_ident(name) }
}

/// A parsed `CREATE TEMP TABLE`: its name, `IF NOT EXISTS`, `ON COMMIT`,
/// and the statement with `TEMP` and `ON COMMIT` taken out and `{}` where
/// the real name goes.
struct CreateTemp {
    name: String,
    if_not_exists: bool,
    on_commit: OnCommit,
    template: String,
}

fn parse_create_temp(stmt: &str) -> Option<Result<CreateTemp, (&'static str, String)>> {
    let text = stmt.trim().trim_end_matches(';').trim_end();
    let words: Vec<&str> = text.split_whitespace().collect();
    let is = |i: usize, kw: &str| words.get(i).is_some_and(|w| w.eq_ignore_ascii_case(kw));
    if !is(0, "CREATE") {
        return None;
    }
    let mut i = 1;
    if is(i, "GLOBAL") || is(i, "LOCAL") {
        i += 1;
    }
    if !(is(i, "TEMP") || is(i, "TEMPORARY")) || !is(i + 1, "TABLE") {
        return None;
    }
    // The text after TABLE.
    let mut at = 0;
    for _ in 0..=(i + 1) {
        let rest = &text[at..];
        let start = rest.len() - rest.trim_start().len();
        let end = rest[start..].find(char::is_whitespace).map_or(rest.len(), |e| start + e);
        at += end;
    }
    let mut rest = text[at..].trim_start();
    let mut if_not_exists = false;
    if rest.len() >= 13 && rest[..13].eq_ignore_ascii_case("IF NOT EXISTS") {
        if_not_exists = true;
        rest = rest[13..].trim_start();
    }
    let name_end = if rest.starts_with('"') {
        crate::core::sql_scan::quoted_end(rest.as_bytes(), 0, b'"')
    } else {
        rest.find(|c: char| c.is_whitespace() || c == '(').unwrap_or(rest.len())
    };
    let written = &rest[..name_end];
    if written.contains('.') && !written.to_ascii_lowercase().starts_with("pg_temp.") {
        return Some(Err(("42P16", "cannot create temporary relation in non-temporary schema".to_string())));
    }
    let name = ident(crate::core::sql_scan::last_name_segment(written));
    let mut body = rest[name_end..].to_string();
    let mut on_commit = OnCommit::PreserveRows;
    if let Some(on) = crate::core::rql::join::find_keyword(&body, "ON", 0) {
        let after = body[on + 2..].trim_start();
        if after.len() >= 6 && after[..6].eq_ignore_ascii_case("COMMIT") {
            let tail = after[6..].trim_start();
            let upper = tail.to_ascii_uppercase();
            let (mode, len) = if upper.starts_with("PRESERVE ROWS") {
                (OnCommit::PreserveRows, 13)
            } else if upper.starts_with("DELETE ROWS") {
                (OnCommit::DeleteRows, 11)
            } else if upper.starts_with("DROP") {
                (OnCommit::Drop, 4)
            } else {
                return Some(Err(("42601", "ON COMMIT takes PRESERVE ROWS, DELETE ROWS or DROP".to_string())));
            };
            on_commit = mode;
            let clause_end = body.len() - tail.len() + len;
            body = format!("{} {}", body[..on].trim_end(), body[clause_end..].trim_start());
        }
    }
    let template = format!("CREATE TABLE {{}} {}", body.trim());
    Some(Ok(CreateTemp { name, if_not_exists, on_commit, template }))
}

/// Forgets the temporary tables whose real table is gone (a rolled-back
/// create, a committed drop).
fn prune(database: &Database, state: &mut SessionState) {
    state.temp_tables.retain(|t| database.has_table(&t.real));
}

/// Handles `CREATE TEMP TABLE`; `Ok(false)` for any other statement.
pub(super) async fn handle_create<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    stmt: &str,
) -> io::Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(parsed) = parse_create_temp(stmt) else {
        return Ok(false);
    };
    let create = match parsed {
        Ok(create) => create,
        Err((code, message)) => {
            fail_statement(state, stream, code, &message).await?;
            return Ok(true);
        }
    };
    let read_only = state.transaction.as_ref().map(|t| t.read_only).unwrap_or(state.default_read_only);
    if read_only {
        // PostgreSQL lets a read-only transaction create a temporary table.
    }
    prune(database, state);
    if state.temp_tables.iter().any(|t| t.name == create.name) {
        if create.if_not_exists {
            crate::core::notice::push("NOTICE", "42P07", format!("relation \"{}\" already exists, skipping", create.name));
            send_command_complete(stream, "CREATE TABLE").await?;
        } else {
            fail_statement(state, stream, "42P07", &format!("relation \"{}\" already exists", create.name)).await?;
        }
        return Ok(true);
    }
    let real = format!("{}tmp_{:016x}_{}", crate::core::rql::cte::TEMP_TABLE_PREFIX, state.temp_session, create.name);
    let sql = create.template.replacen("{}", &crate::core::rql::views::quote_ident(&real), 1);
    let ran = crate::core::rql::cte::allowing_temporaries(super::view_statements::run_internal_ddl(database, state, &sql)).await?;
    match ran {
        Ok(tag) => {
            state.temp_tables.push(TempTable { name: create.name, real, on_commit: create.on_commit });
            let tag = if tag.starts_with("SELECT") { tag } else { "CREATE TABLE".to_string() };
            send_command_complete(stream, &tag).await?;
        }
        Err(e) => {
            let e = e.replace(&format!("\"{real}\""), &format!("\"{}\"", "<temporary>"));
            send_error_response(stream, sqlstate_for(&e), &e).await?;
        }
    }
    Ok(true)
}

/// `sql` with each reference to one of the session's temporary tables
/// written as its real name; `None` when it names none.
pub(super) fn rewrite(temps: &[TempTable], sql: &str) -> Option<String> {
    if temps.is_empty() || !temps.iter().any(|t| crate::core::sql_scan::contains_ignore_ascii_case(sql, t.name.as_bytes())) {
        return None;
    }
    let real_of = |name: &str| temps.iter().find(|t| t.name == name).map(|t| t.real.as_str());
    let mut edits: Vec<(std::ops::Range<usize>, String)> = Vec::new();
    for r in crate::core::rql::lock_set::relation_references(sql) {
        let Some(real) = real_of(&r.name) else { continue };
        // `public.t` names the permanent table; `pg_temp.t` or `t` the
        // temporary one.
        let written = &sql[r.span.clone()];
        if written.contains('.') && !written.to_ascii_lowercase().starts_with("pg_temp.") {
            continue;
        }
        let quoted = crate::core::rql::views::quote_ident(real);
        let reads = r.mode == crate::core::mvcc::table_locks::TableMode::AccessShare;
        let text = if reads && !r.aliased { format!("{quoted} AS {}", alias_of(&r.name)) } else { quoted };
        edits.push((r.span, text));
    }
    // Table DDL names its table where no relation list does.
    if edits.is_empty() {
        let upper = sql.trim_start().to_ascii_uppercase();
        let ddl = ["DROP TABLE", "ALTER TABLE", "TRUNCATE", "CREATE INDEX", "CREATE UNIQUE INDEX", "LOCK TABLE", "LOCK "]
            .iter()
            .any(|p| upper.starts_with(p));
        if ddl {
            let bytes = sql.as_bytes();
            let mut i = 0;
            while i < bytes.len() {
                let b = bytes[i];
                if b == b'\'' {
                    i = crate::core::sql_scan::string_literal_end(bytes, i, false);
                    continue;
                }
                if b == b'"' || b.is_ascii_alphabetic() || b == b'_' {
                    let start = i;
                    let end = if b == b'"' {
                        crate::core::sql_scan::quoted_end(bytes, i, b'"')
                    } else {
                        let mut j = i;
                        while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                            j += 1;
                        }
                        j
                    };
                    let mut span_start = start;
                    // `pg_temp.t`.
                    if start >= 8 && sql[..start].to_ascii_lowercase().ends_with("pg_temp.") {
                        span_start = start - 8;
                    }
                    let after_dot = span_start == start && start > 0 && bytes[start - 1] == b'.';
                    if !after_dot && bytes.get(end) != Some(&b'.') {
                        if let Some(real) = real_of(&ident(&sql[start..end])) {
                            edits.push((span_start..end, crate::core::rql::views::quote_ident(real)));
                        }
                    }
                    i = end;
                    continue;
                }
                i += 1;
            }
        }
    }
    if edits.is_empty() {
        return None;
    }
    edits.sort_by_key(|(span, _)| std::cmp::Reverse(span.start));
    let mut out = sql.to_string();
    for (span, text) in edits {
        out.replace_range(span, &text);
    }
    Some(out)
}

/// Drops the session's temporary tables (all of them, or those `which`
/// keeps), outside any block.
async fn drop_where(database: &Arc<Database>, state: &mut SessionState, which: impl Fn(&TempTable) -> bool) {
    let (dropped, kept): (Vec<TempTable>, Vec<TempTable>) = std::mem::take(&mut state.temp_tables).into_iter().partition(|t| which(t));
    state.temp_tables = kept;
    for t in dropped {
        let sql = format!("DROP TABLE IF EXISTS {}", crate::core::rql::views::quote_ident(&t.real));
        if let Err(e) = crate::core::rql::executor::run_statement(&sql, database, crate::core::mvcc::transaction::FROZEN_TX).await {
            log::warn!("temporary table {} could not be dropped: {e}", t.name);
        }
    }
}

/// Every one of the session's temporary tables, dropped: at its end, and for
/// `DISCARD TEMP` and `DISCARD ALL`.
pub(super) async fn drop_all(database: &Arc<Database>, state: &mut SessionState) {
    if !state.temp_tables.is_empty() {
        drop_where(database, state, |_| true).await;
    }
}

/// After a statement, with no transaction left open: `ON COMMIT DROP`
/// tables dropped, `ON COMMIT DELETE ROWS` ones emptied.
pub(super) async fn end_of_transaction(database: &Arc<Database>, state: &mut SessionState) {
    if state.temp_tables.is_empty() || state.transaction.is_some() {
        return;
    }
    prune(database, state);
    drop_where(database, state, |t| t.on_commit == OnCommit::Drop).await;
    let emptied: Vec<String> = state.temp_tables.iter().filter(|t| t.on_commit == OnCommit::DeleteRows).map(|t| t.real.clone()).collect();
    for real in emptied {
        let sql = format!("DELETE FROM {}", crate::core::rql::views::quote_ident(&real));
        let _ = crate::core::rql::executor::run_statement(&sql, database, crate::core::mvcc::transaction::FROZEN_TX).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_temp_create_is_read() {
        let c = parse_create_temp("CREATE TEMP TABLE IF NOT EXISTS Foo (a INT) ON COMMIT DROP").unwrap().unwrap();
        assert_eq!((c.name.as_str(), c.if_not_exists, c.on_commit), ("foo", true, OnCommit::Drop));
        assert_eq!(c.template, "CREATE TABLE {} (a INT)");
        let c = parse_create_temp("create local temporary table t ON COMMIT DELETE ROWS AS SELECT 1 AS a").unwrap().unwrap();
        assert_eq!((c.on_commit, c.template.as_str()), (OnCommit::DeleteRows, "CREATE TABLE {} AS SELECT 1 AS a"));
        assert!(parse_create_temp("CREATE TABLE t (a INT)").is_none());
        assert!(parse_create_temp("CREATE TEMP TABLE public.t (a INT)").unwrap().is_err());
    }

    #[test]
    fn references_are_written_as_the_real_name() {
        let temps = vec![TempTable { name: "t".into(), real: "__cte_tmp_1_t".into(), on_commit: OnCommit::PreserveRows }];
        assert_eq!(
            rewrite(&temps, "SELECT t.a FROM t JOIN u ON u.a = t.a").as_deref(),
            Some("SELECT t.a FROM \"__cte_tmp_1_t\" AS t JOIN u ON u.a = t.a")
        );
        assert_eq!(rewrite(&temps, "INSERT INTO t VALUES (1)").as_deref(), Some("INSERT INTO \"__cte_tmp_1_t\" VALUES (1)"));
        assert_eq!(rewrite(&temps, "SELECT * FROM public.t"), None);
        assert_eq!(rewrite(&temps, "DROP TABLE pg_temp.t").as_deref(), Some("DROP TABLE \"__cte_tmp_1_t\""));
        assert_eq!(rewrite(&temps, "TRUNCATE t").as_deref(), Some("TRUNCATE \"__cte_tmp_1_t\""));
        assert_eq!(rewrite(&temps, "SELECT 't' FROM x"), None);
    }
}
