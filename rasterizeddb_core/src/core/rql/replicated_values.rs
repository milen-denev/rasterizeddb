//! A write that replication ships as text carries the values of its
//! non-deterministic calls, not the calls.
//!
//! Replication sends the SQL a transaction ran and each peer runs it again
//! (docs/replication.md §2). `now()`, `random()`, `gen_random_uuid()` and the
//! like were evaluated a second time on every peer, so two nodes held
//! different rows for the same transaction, with nothing reporting it: the
//! one defence was a warning, logged once per process. On a node that is
//! replicating, a write is rewritten before it runs:
//!
//! * the clock functions (`now()`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`,
//!   `statement_timestamp()`, …) read the statement's clock, one instant for
//!   every row, so each becomes a literal of that instant wherever it stands;
//! * a function that answers anew at every call (`random()`,
//!   `gen_random_uuid()`, `clock_timestamp()`, `timeofday()`) becomes a
//!   literal of one call where it is evaluated once: a slot of `INSERT …
//!   VALUES`, one row per tuple. Anywhere else (`UPDATE … SET r = random()`,
//!   a `WHERE`, `INSERT … SELECT`) it is evaluated once per row, which no
//!   literal can stand for, and the statement is refused (0A000) rather than
//!   left to diverge.
//!
//! The statement then runs as rewritten, so the origin stores exactly what
//! the peers will. A `RETURNING` list is left alone: it changes no row. A
//! read is never rewritten, and a node that is not replicating runs every
//! statement as written.

/// What [`fold`] made of a statement.
#[derive(Debug, PartialEq, Eq)]
pub enum Fold {
    /// Nothing to replace.
    Unchanged,
    /// The statement with its calls replaced by their values.
    Rewritten(String),
    /// A call that cannot be given one value; the message says which.
    Refused(String),
}

/// How a function's value is fixed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// One value for the whole statement: the statement clock.
    Stable,
    /// A new value at every call.
    Volatile,
}

/// `(name, needs parentheses, kind, the type its literal is cast to)`.
/// The keyword forms (`CURRENT_TIMESTAMP`) take an optional precision.
const FUNCTIONS: &[(&str, bool, Kind, &str)] = &[
    ("now", true, Kind::Stable, "timestamptz"),
    ("transaction_timestamp", true, Kind::Stable, "timestamptz"),
    ("statement_timestamp", true, Kind::Stable, "timestamptz"),
    ("current_timestamp", false, Kind::Stable, "timestamptz"),
    ("localtimestamp", false, Kind::Stable, "timestamp"),
    ("current_date", false, Kind::Stable, "date"),
    ("current_time", false, Kind::Stable, "timetz"),
    ("localtime", false, Kind::Stable, "time"),
    ("random", true, Kind::Volatile, "float8"),
    ("gen_random_uuid", true, Kind::Volatile, "uuid"),
    ("clock_timestamp", true, Kind::Volatile, "timestamptz"),
    ("timeofday", true, Kind::Volatile, "text"),
];

/// One call found in the text.
struct Call {
    start: usize,
    end: usize,
    function: usize,
}

/// See the module documentation. `sql` is one statement.
pub fn fold(sql: &str) -> Fold {
    // Almost every statement mentions none of them: one cheap pass decides.
    let lower = sql.to_ascii_lowercase();
    if !FUNCTIONS.iter().any(|(name, ..)| lower.contains(name)) {
        return Fold::Unchanged;
    }
    let Some(kind) = write_kind(sql) else {
        return Fold::Unchanged;
    };
    let calls = calls_in(sql);
    if calls.is_empty() {
        return Fold::Unchanged;
    }
    let returning_at = top_level_keyword(sql, "RETURNING").unwrap_or(sql.len());
    let values = if kind == "INSERT" { values_region(sql) } else { None };

    let mut out = String::with_capacity(sql.len() + 32);
    let mut copied = 0usize;
    let mut changed = false;
    for call in &calls {
        if call.start >= returning_at {
            continue;
        }
        let (name, _, function_kind, cast) = FUNCTIONS[call.function];
        if function_kind == Kind::Volatile {
            let once = values.is_some_and(|(from, to)| call.start >= from && call.end <= to);
            if !once {
                return Fold::Refused(format!(
                    "{name}() in this {kind} would be evaluated again, to a different value, on every \
                     node replicating it: replication sends the statement, not the rows. Compute the \
                     value in the client, or write it in an INSERT … VALUES, where it is taken once"
                ));
            }
        }
        let text = &sql[call.start..call.end];
        let Some(mut value) = evaluate_as_text(text) else {
            // Something the evaluator does not take (an argument it refuses):
            // left as written, for the statement to report or run as before.
            continue;
        };
        if cast == "date" {
            // `CURRENT_DATE` renders as a midnight timestamp here; its date
            // is the value.
            value.truncate(value.find(' ').unwrap_or(value.len()));
        }
        out.push_str(&sql[copied..call.start]);
        out.push_str(&crate::core::value_codec::quote_sql(&value));
        out.push_str("::");
        out.push_str(cast);
        copied = call.end;
        changed = true;
    }
    if !changed {
        return Fold::Unchanged;
    }
    out.push_str(&sql[copied..]);
    Fold::Rewritten(out)
}

/// The statement's verb when it writes, `None` for anything else. A `WITH`
/// counts when a write follows its CTEs.
fn write_kind(sql: &str) -> Option<&'static str> {
    let first = first_word(sql)?;
    let upper = first.to_ascii_uppercase();
    match upper.as_str() {
        "INSERT" => Some("INSERT"),
        "UPDATE" => Some("UPDATE"),
        "DELETE" => Some("DELETE"),
        "MERGE" => Some("MERGE"),
        "CREATE" => {
            // CREATE TABLE … AS: its rows are computed here and shipped as
            // the statement.
            let rest = sql.trim_start()[first.len()..].to_ascii_uppercase();
            rest.contains(" AS ").then_some("CREATE TABLE AS")
        }
        "WITH" => ["INSERT", "UPDATE", "DELETE"]
            .iter()
            .find(|kw| top_level_keyword(sql, kw).is_some())
            .copied(),
        _ => None,
    }
}

fn first_word(sql: &str) -> Option<&str> {
    let t = sql.trim_start();
    let end = t.find(|c: char| !c.is_ascii_alphabetic()).unwrap_or(t.len());
    (end > 0).then(|| &t[..end])
}

/// Every call of one of [`FUNCTIONS`] outside quotes and comments, in order.
fn calls_in(sql: &str) -> Vec<Call> {
    let bytes = sql.as_bytes();
    let mut calls = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if let Some(end) = crate::core::sql_scan::skip_comment(bytes, i) {
            i = end;
            continue;
        }
        if c == b'\'' || c == b'"' || (c == b'$' && crate::core::sql_scan::dollar_tag_len(bytes, i).is_some()) {
            i = crate::core::sql_scan::skip_quoted(bytes, i).unwrap_or(bytes.len());
            continue;
        }
        let word_start = c.is_ascii_alphabetic()
            && (i == 0 || !(crate::core::sql_scan::is_identifier_byte(bytes[i - 1]) || bytes[i - 1] == b'.' || bytes[i - 1] == b'$'));
        if !word_start {
            i += 1;
            continue;
        }
        let mut j = i;
        while j < bytes.len() && crate::core::sql_scan::is_identifier_byte(bytes[j]) {
            j += 1;
        }
        let word = &sql[i..j];
        let found = FUNCTIONS.iter().position(|(name, ..)| word.eq_ignore_ascii_case(name));
        let Some(function) = found else {
            i = j;
            continue;
        };
        let (_, parens, ..) = FUNCTIONS[function];
        // What follows the name: `(…)`, required for a function and an
        // optional precision for a keyword. A name followed by `.` or by a
        // `(` it does not take is something else (a qualified column).
        let mut k = j;
        while k < bytes.len() && bytes[k].is_ascii_whitespace() {
            k += 1;
        }
        let end = if bytes.get(k) == Some(&b'(') {
            match matching_paren(bytes, k) {
                Some(close) => close + 1,
                None => {
                    i = j;
                    continue;
                }
            }
        } else if parens {
            i = j;
            continue;
        } else {
            j
        };
        if bytes.get(j) == Some(&b'.') {
            i = j;
            continue;
        }
        calls.push(Call { start: i, end, function });
        i = end;
    }
    calls
}

/// The `)` that closes the `(` at `open`, quotes and comments skipped.
fn matching_paren(bytes: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut i = open;
    while i < bytes.len() {
        let c = bytes[i];
        if let Some(end) = crate::core::sql_scan::skip_comment(bytes, i) {
            i = end;
            continue;
        }
        if c == b'\'' || c == b'"' || (c == b'$' && crate::core::sql_scan::dollar_tag_len(bytes, i).is_some()) {
            i = crate::core::sql_scan::skip_quoted(bytes, i)?;
            continue;
        }
        match c {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// The span of an `INSERT`'s `VALUES` tuples: from the keyword to the first
/// top-level `ON CONFLICT` or `RETURNING`. `None` when the rows come from a
/// query instead.
fn values_region(sql: &str) -> Option<(usize, usize)> {
    let from = top_level_keyword(sql, "VALUES")?;
    if top_level_keyword(sql, "SELECT").is_some_and(|at| at > from) {
        return None;
    }
    let to = ["ON", "RETURNING"]
        .iter()
        .filter_map(|kw| top_level_keyword(&sql[from..], kw).map(|at| from + at))
        .min()
        .unwrap_or(sql.len());
    Some((from, to))
}

/// Where `keyword` first stands as a word outside quotes, comments and
/// parentheses.
fn top_level_keyword(sql: &str, keyword: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if let Some(end) = crate::core::sql_scan::skip_comment(bytes, i) {
            i = end;
            continue;
        }
        if c == b'\'' || c == b'"' || (c == b'$' && crate::core::sql_scan::dollar_tag_len(bytes, i).is_some()) {
            i = crate::core::sql_scan::skip_quoted(bytes, i).unwrap_or(bytes.len());
            continue;
        }
        match c {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ if depth == 0
                && bytes.len() >= i + keyword.len()
                && bytes[i..i + keyword.len()].eq_ignore_ascii_case(keyword.as_bytes())
                && (i == 0 || !crate::core::sql_scan::is_identifier_byte(bytes[i - 1]))
                && bytes.get(i + keyword.len()).is_none_or(|b| !crate::core::sql_scan::is_identifier_byte(*b)) =>
            {
                return Some(i);
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// `call`'s value as PostgreSQL's text for it, evaluated now, under the
/// running statement's clock.
fn evaluate_as_text(call: &str) -> Option<String> {
    let expr = crate::core::rql::expr::parse(&format!("CAST({call} AS TEXT)"), &[]).ok()?;
    let empty = crate::core::row::row::Row {
        position: 0,
        length: 0,
        columns: smallvec::SmallVec::new(),
    };
    match crate::core::rql::expr::eval_output(&expr, &empty, &[]).ok()? {
        crate::core::rql::expr::Value::Data { bytes, .. } => {
            let text = String::from_utf8(bytes).ok()?;
            Some(text.trim_end_matches('\0').to_string())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewritten(sql: &str) -> String {
        match fold(sql) {
            Fold::Rewritten(s) => s,
            other => panic!("{sql}: {other:?}"),
        }
    }

    #[test]
    fn reads_and_plain_writes_are_untouched() {
        assert_eq!(fold("SELECT now(), random()"), Fold::Unchanged);
        assert_eq!(fold("INSERT INTO t (a) VALUES (1)"), Fold::Unchanged);
        assert_eq!(fold("UPDATE t SET note = 'now() and random()' WHERE id = 1"), Fold::Unchanged);
        assert_eq!(fold("UPDATE t SET now_at = 1 WHERE random_id = 2"), Fold::Unchanged);
        assert_eq!(fold("UPDATE t SET a = x.now WHERE x.random = 2"), Fold::Unchanged);
    }

    #[tokio::test]
    async fn clock_functions_become_the_statements_instant_everywhere() {
        let clock = crate::core::statement_clock::StatementClock {
            transaction_us: 1_700_000_000_123_456,
            statement_us: 1_700_000_005_000_000,
        };
        let sql = "UPDATE t SET at = now(), d = CURRENT_DATE, s = statement_timestamp() WHERE at < CURRENT_TIMESTAMP RETURNING now()";
        let out = crate::core::statement_clock::scope(clock, async { rewritten(sql) }).await;
        assert_eq!(
            out,
            "UPDATE t SET at = '2023-11-14 22:13:20.123456+00'::timestamptz, d = '2023-11-14'::date, \
             s = '2023-11-14 22:13:25+00'::timestamptz WHERE at < '2023-11-14 22:13:20.123456+00'::timestamptz RETURNING now()"
        );
    }

    #[test]
    fn a_per_call_function_is_taken_once_in_values_and_refused_elsewhere() {
        let out = rewritten("INSERT INTO t (id, r, u) VALUES (1, random(), gen_random_uuid()), (2, random(), gen_random_uuid())");
        assert!(!out.contains("random()") && !out.contains("gen_random_uuid()"), "{out}");
        assert_eq!(out.matches("::float8").count(), 2);
        assert_eq!(out.matches("::uuid").count(), 2);
        let parts: Vec<&str> = out.split("::uuid").collect();
        assert_ne!(parts[0].rsplit('\'').nth(1), parts[1].rsplit('\'').nth(1), "each slot is its own call");

        for sql in [
            "UPDATE t SET r = random() WHERE id < 10",
            "DELETE FROM t WHERE random() < 0.5",
            "INSERT INTO t (id, r) SELECT id, random() FROM u",
            "INSERT INTO t (id, r) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET r = random()",
            "WITH x AS (SELECT 1) UPDATE t SET u = gen_random_uuid()",
        ] {
            assert!(matches!(fold(sql), Fold::Refused(_)), "{sql}");
        }
        // RETURNING changes no row.
        assert!(matches!(fold("UPDATE t SET a = 1 RETURNING random()"), Fold::Unchanged));
    }
}
