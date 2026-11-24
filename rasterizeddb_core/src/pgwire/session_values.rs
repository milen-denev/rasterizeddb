//! What a statement reads of its session, as the literals they are for it:
//! `current_user`, `session_user`, `current_role`, `current_catalog`,
//! `current_database()`, `current_schemas(…)`, `pg_backend_pid()`,
//! `pg_is_in_recovery()` and `current_setting(…)`.
//!
//! Each was "column \"current_user\" does not exist" or "function … is not
//! supported yet": the row evaluator has no session to read. An ORM and a
//! pool ask for them as they connect (`SELECT current_database()`,
//! `current_setting('server_version_num')`), so they are read here, where the
//! session is, and put in the text before anything else reads it.
//!
//! A bare `user` is left alone: PostgreSQL reserves it, but a table here may
//! have a column created under that name, and reading the session's user in
//! place of that column would be a silent wrong answer.
//!
//! A value standing alone as a select item keeps PostgreSQL's column name
//! (`AS "current_user"`), which a client reading the row by name asks for.

use crate::core::rql::join::find_keyword;
use crate::core::sql_scan::{contains_ignore_ascii_case, is_identifier_byte, matching_close, skip_comment, skip_quoted};

/// The session a statement runs in.
pub(crate) struct Session<'a> {
    pub user: &'a str,
    pub database: &'a str,
    pub backend_pid: i32,
    /// A run-time parameter's value as `SHOW` reports it (`key` lowercase),
    /// `None` for one this server does not have.
    pub setting: &'a dyn Fn(&str) -> Option<String>,
}

/// Whether `sql` names a session value that [`rewrite`] would put in it, or
/// asks `current_setting` for a parameter: what the native protocol hands to
/// a pgwire session rather than run without one (`NativeSession::
/// needs_session_path`).
pub(crate) fn names_any(sql: &str) -> bool {
    let nothing = |_: &str| None;
    let session = Session { user: "", database: "", backend_pid: 0, setting: &nothing };
    !matches!(rewrite(sql, &session), Ok(None))
}

/// `sql` with each session value replaced; `None` when it names none, an
/// error for `current_setting` of a parameter that does not exist.
pub(crate) fn rewrite(sql: &str, session: &Session) -> Result<Option<String>, (&'static str, String)> {
    // Only a statement that runs now: a `DEFAULT current_user`, a view's or
    // a function's body is evaluated later, by whoever runs it, and a
    // literal there would keep the creator's name for everyone.
    let head = sql.trim_start();
    let runs_now = head.starts_with('(')
        || ["SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "VALUES", "MERGE", "TABLE"]
            .iter()
            .any(|w| head.len() > w.len() && head.as_bytes()[..w.len()].eq_ignore_ascii_case(w.as_bytes()) && !is_identifier_byte(head.as_bytes()[w.len()]));
    if !runs_now {
        return Ok(None);
    }
    if ![&b"current_"[..], b"session_user", b"pg_backend_pid", b"pg_is_in_recovery"]
        .iter()
        .any(|w| contains_ignore_ascii_case(sql, w))
    {
        return Ok(None);
    }
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 32);
    let mut copied = 0usize;
    let mut opens: Vec<usize> = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        match bytes[i] {
            b'(' => {
                opens.push(i);
                i += 1;
                continue;
            }
            b')' => {
                opens.pop();
                i += 1;
                continue;
            }
            _ => {}
        }
        let starts_word = is_identifier_byte(bytes[i])
            && !bytes[i].is_ascii_digit()
            && (i == 0 || !(is_identifier_byte(bytes[i - 1]) || bytes[i - 1] == b'.' || bytes[i - 1] == b'"' || bytes[i - 1] == b'$'));
        if !starts_word {
            i += 1;
            continue;
        }
        let mut j = i;
        while j < bytes.len() && is_identifier_byte(bytes[j]) {
            j += 1;
        }
        let word = sql[i..j].to_ascii_lowercase();
        let rest = &sql[j..];
        let next = rest.trim_start();
        let gap = rest.len() - next.len();
        let after_alias_keyword = previous_word(sql, i).is_some_and(|w| w.eq_ignore_ascii_case("AS"));
        if next.starts_with('.') || after_alias_keyword {
            i = j;
            continue;
        }
        let call = next.starts_with('(');
        let replaced: Option<(String, usize)> = match (word.as_str(), call) {
            ("current_user" | "current_role" | "session_user", false) => Some((literal(session.user), j)),
            ("current_catalog", false) => Some((literal(session.database), j)),
            ("current_database" | "pg_backend_pid" | "pg_is_in_recovery" | "current_schemas" | "current_setting", true) => {
                let open = j + gap;
                let Some(close) = matching_close(sql, open) else {
                    i = j;
                    continue;
                };
                let args = sql[open + 1..close].trim();
                let value = match word.as_str() {
                    "current_database" if args.is_empty() => Some(literal(session.database)),
                    "pg_backend_pid" if args.is_empty() => Some(session.backend_pid.to_string()),
                    // No server here replays another's log as a hot
                    // standby: a replica applies its peer's commits as
                    // ordinary transactions. `pg_dump` asks this first.
                    "pg_is_in_recovery" if args.is_empty() => Some("FALSE".to_string()),
                    "current_schemas" => match args.to_ascii_lowercase().as_str() {
                        "true" | "'t'" | "'true'" => Some("'{pg_catalog,public}'::text[]".to_string()),
                        "false" | "'f'" | "'false'" => Some("'{public}'::text[]".to_string()),
                        _ => None,
                    },
                    "current_setting" => current_setting(args, session)?,
                    _ => None,
                };
                value.map(|v| (v, close + 1))
            }
            _ => None,
        };
        let Some((value, end)) = replaced else {
            i = j;
            continue;
        };
        out.push_str(&sql[copied..i]);
        let segment_start = opens.last().map_or(0, |p| p + 1);
        if stands_alone_as_select_item(sql, segment_start, i, end, !opens.is_empty()) {
            out.push_str(&format!("{value} AS \"{word}\""));
        } else {
            out.push_str(&value);
        }
        copied = end;
        i = end;
    }
    if copied == 0 {
        return Ok(None);
    }
    out.push_str(&sql[copied..]);
    Ok(Some(out))
}

/// `current_setting('name' [, missing_ok])`: the value as text, `NULL` for a
/// missing one asked for with `missing_ok`, 42704 otherwise. `None` (left as
/// written) when the name is not a string literal.
fn current_setting(args: &str, session: &Session) -> Result<Option<String>, (&'static str, String)> {
    let parts = crate::core::sql_scan::split_top_level(args, b',');
    let name = parts.first().map(|p| p.trim()).unwrap_or_default();
    let Some(name) = name.strip_prefix('\'').and_then(|n| n.strip_suffix('\'')) else {
        return Ok(None);
    };
    if name.contains('\'') {
        return Ok(None);
    }
    let missing_ok = match parts.get(1).map(|p| p.trim().to_ascii_lowercase()) {
        None => false,
        Some(flag) => match flag.as_str() {
            "true" | "'t'" | "'true'" => true,
            "false" | "'f'" | "'false'" => false,
            _ => return Ok(None),
        },
    };
    if parts.len() > 2 {
        return Ok(None);
    }
    let key = name.to_ascii_lowercase();
    match (session.setting)(&key) {
        Some(value) => Ok(Some(literal(&value))),
        None if missing_ok => Ok(Some("NULL::text".to_string())),
        None => Err(("42704", format!("unrecognized configuration parameter \"{name}\""))),
    }
}

/// A string literal holding `value`.
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// The word just before `at`, if one ends there (after spaces).
fn previous_word(sql: &str, at: usize) -> Option<&str> {
    let before = sql[..at].trim_end();
    let start = before
        .bytes()
        .rposition(|b| !is_identifier_byte(b))
        .map_or(0, |p| p + 1);
    (start < before.len()).then(|| &before[start..])
}

/// Whether `sql[start..end]` is a whole item of a select list, so its value
/// may take a column name: the list is the segment's last `SELECT` with no
/// `FROM` yet, the item is preceded by that `SELECT` (or `DISTINCT`, `ALL`)
/// or a comma, and followed by a comma or what ends a select list.
fn stands_alone_as_select_item(sql: &str, segment_start: usize, start: usize, end: usize, nested: bool) -> bool {
    let segment = &sql[segment_start..start];
    let mut last_select = None;
    let mut from = 0;
    while let Some(p) = find_keyword(segment, "SELECT", from) {
        last_select = Some(p);
        from = p + "SELECT".len();
    }
    let Some(select) = last_select else { return false };
    if find_keyword(segment, "FROM", select).is_some() {
        return false;
    }
    let before = segment.trim_end();
    let opens_item = before.ends_with(',')
        || previous_word(sql, start).is_some_and(|w| ["SELECT", "DISTINCT", "ALL"].iter().any(|k| w.eq_ignore_ascii_case(k)));
    if !opens_item {
        return false;
    }
    let after = sql[end..].trim_start();
    if after.is_empty() || after.starts_with(',') || after.starts_with(';') || (nested && after.starts_with(')')) {
        return true;
    }
    let next: String = after.bytes().take_while(|b| is_identifier_byte(*b)).map(|b| b as char).collect();
    ["FROM", "WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "FETCH", "UNION", "INTERSECT", "EXCEPT", "INTO", "HAVING", "WINDOW", "FOR"]
        .iter()
        .any(|k| next.eq_ignore_ascii_case(k))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(sql: &str) -> Result<Option<String>, (&'static str, String)> {
        let setting = |key: &str| match key {
            "server_version_num" => Some("170000".to_string()),
            "application_name" => Some("it's".to_string()),
            _ => None,
        };
        rewrite(
            sql,
            &Session { user: "alice", database: "shop", backend_pid: 42, setting: &setting },
        )
    }

    #[test]
    fn a_session_value_is_its_literal_and_keeps_its_name() {
        assert_eq!(
            run("SELECT current_user, current_database()").unwrap().as_deref(),
            Some("SELECT 'alice' AS \"current_user\", 'shop' AS \"current_database\"")
        );
        assert_eq!(
            run("SELECT current_setting('server_version_num')::int >= 90000").unwrap().as_deref(),
            Some("SELECT '170000'::int >= 90000")
        );
        assert_eq!(
            run("SELECT x FROM t WHERE owner = session_user").unwrap().as_deref(),
            Some("SELECT x FROM t WHERE owner = 'alice'")
        );
        assert_eq!(
            run("SELECT coalesce(current_user, 'x') AS u").unwrap().as_deref(),
            Some("SELECT coalesce('alice', 'x') AS u")
        );
        assert_eq!(
            run("SELECT (SELECT pg_backend_pid()) FROM t").unwrap().as_deref(),
            Some("SELECT (SELECT 42 AS \"pg_backend_pid\") FROM t")
        );
        assert_eq!(
            run("SELECT current_setting('application_name')").unwrap().as_deref(),
            Some("SELECT 'it''s' AS \"current_setting\"")
        );
        assert_eq!(
            run("SELECT current_schemas(true)").unwrap().as_deref(),
            Some("SELECT '{pg_catalog,public}'::text[] AS \"current_schemas\"")
        );
    }

    #[test]
    fn what_is_not_a_session_value_is_left_alone() {
        for sql in [
            "SELECT 'current_user'",
            "SELECT \"current_user\" FROM t",
            "SELECT t.current_user FROM t",
            "SELECT 1 AS current_user",
            "SELECT current_schema()",
            "SELECT current_setting(name) FROM t",
            "SELECT user FROM t",
            "SELECT current_timestamp",
            "CREATE TABLE t (owner text DEFAULT current_user)",
            "CREATE VIEW v AS SELECT current_user",
        ] {
            assert_eq!(run(sql).unwrap(), None, "{sql}");
        }
    }

    #[test]
    fn a_missing_setting_is_42704_unless_missing_ok() {
        assert_eq!(run("SELECT current_setting('nope')").unwrap_err().0, "42704");
        assert_eq!(
            run("SELECT current_setting('nope', true)").unwrap().as_deref(),
            Some("SELECT NULL::text AS \"current_setting\"")
        );
    }
}
