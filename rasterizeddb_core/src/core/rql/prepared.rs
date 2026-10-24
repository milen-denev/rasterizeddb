//! A prepared `SELECT` parsed once and run many times.
//!
//! The engine reads statements as text. A prepared statement's values are
//! folded into the text (`pg_params::inline_params`) and the result is parsed
//! from scratch on every execution, although everything but the values is the
//! same each time. For a point lookup that parsing is most of the work.
//!
//! What can be kept is the parse, a `QueryRows`, with the `WHERE` clause turned
//! into a template the next execution's values are written into. It is never
//! *derived*, only *observed*. The first two executions run the ordinary text
//! path while it records what it did ([`Capture`]): which `QueryRows` the
//! executor built, from which table, under which column description, and that
//! it took the plain single-table branch exactly once, with nothing rewritten
//! around it.
//! - The first execution yields the template: each bound value's literal must
//!   occur exactly once in the recorded `WHERE` clause.
//! - The second, with different values, must reproduce it: its recorded
//!   `WHERE` clause must equal the template rendered with its values, and
//!   everything else it recorded must equal the first.
//!
//! Only then does a third execution skip the text path. A rewrite that treats
//! a value as more than an opaque literal (constant folding, `= NULL`, a
//! `LIMIT`) shows up as a mismatch and the statement stays on the text path for
//! good.
//!
//! Every reuse also requires each value to have the literal class it was
//! verified with (`literal_class`): an unquoted number, or a single-quoted
//! string with no quote or backslash inside. A `NULL` has no class, and always
//! takes the text path.
//!
//! A partitioned table learns the same way (`docs/partitioning.md` §4.1, S15).
//! [`PreparedRows::table_name`] is its logical name, and only the parse is
//! cached: each execution renders its own values into the template and the
//! executor routes that rendered `WHERE` afresh, then reads which partitions
//! are materialized after the execution's snapshot. So a learned parse
//! never pins the partition its first execution read (I11). pgwire runs a
//! learned, non-locking parse inside `statement_snapshot::scope`, as the text
//! path runs a read-only statement.

use std::cell::RefCell;

use crate::core::db_type::DbType;
use crate::core::rql::lexer_query::QueryRows;

/// A parse to run in place of the statement text.
#[derive(Debug)]
pub struct PreparedRows {
    pub table_name: String,
    pub query_row: QueryRows,
}

/// What the text path did for one statement, recorded while it ran.
#[derive(Default)]
pub struct Capture {
    /// Entries into the executor's `SELECT` arm. More than one means the
    /// statement ran others (a subquery, a union branch, a view), and its
    /// parse alone does not describe it.
    pub arm_entries: u32,
    /// Something rewrote the statement around the parse.
    pub invalid: bool,
    /// The plain single-table branch's parse, and its table.
    pub plain: Option<(String, QueryRows)>,
    /// The column description and scales the rows were sent with.
    pub description: Option<(Option<Vec<(String, DbType)>>, Option<Vec<u32>>)>,
}

tokio::task_local! {
    static CAPTURE: RefCell<Capture>;
}

/// Runs `fut` with a capture in scope, and returns what it recorded.
pub async fn capturing<F: std::future::Future>(fut: F) -> (F::Output, Capture) {
    CAPTURE
        .scope(RefCell::new(Capture::default()), async move {
            let out = fut.await;
            let captured = CAPTURE.with(|c| std::mem::take(&mut *c.borrow_mut()));
            (out, captured)
        })
        .await
}

fn with(f: impl FnOnce(&mut Capture)) {
    let _ = CAPTURE.try_with(|c| f(&mut c.borrow_mut()));
}

/// The executor entered its `SELECT` arm.
pub fn note_select_entry() {
    with(|c| c.arm_entries += 1);
}

/// The statement was rewritten around its parse (a CTE, a view, a subquery).
pub fn note_rewritten() {
    with(|c| c.invalid = true);
}

/// The plain single-table branch is about to run this parse.
pub fn note_plain(table_name: &str, query_row: &QueryRows) {
    with(|c| {
        if c.plain.is_some() {
            c.invalid = true;
        } else {
            c.plain = Some((table_name.to_string(), query_row.clone()));
        }
    });
}

/// The description the rows went out under.
pub fn note_description(fields: &Option<Vec<(String, DbType)>>, scales: &Option<Vec<u32>>) {
    with(|c| c.description = Some((fields.clone(), scales.clone())));
}

impl Capture {
    /// The plain parse, when it is the whole story of the statement.
    pub fn plain_only(self) -> Option<(String, QueryRows, Option<Vec<(String, DbType)>>, Option<Vec<u32>>)> {
        if self.invalid || self.arm_entries != 1 {
            return None;
        }
        let (table, query_row) = self.plain?;
        let (fields, scales) = self.description?;
        Some((table, query_row, fields, scales))
    }
}

/// The shape of a literal a template may be reused with: `Some(0)` for an
/// unquoted number, `Some(1)` for a plain single-quoted string. Anything else
/// (`NULL`, an escape string, a string holding a quote or backslash, a
/// keyword) is `None` and keeps its statement on the text path.
pub fn literal_class(literal: &str) -> Option<u8> {
    let b = literal.as_bytes();
    if b.len() >= 2 && b[0] == b'\'' && b[b.len() - 1] == b'\'' {
        let inner = &b[1..b.len() - 1];
        return (!inner.iter().any(|c| *c == b'\'' || *c == b'\\')).then_some(1);
    }
    let digits = b.strip_prefix(b"-").unwrap_or(b);
    let mut seen_digit = false;
    let mut seen_dot = false;
    for c in digits {
        match c {
            b'0'..=b'9' => seen_digit = true,
            b'.' if !seen_dot => seen_dot = true,
            _ => return None,
        }
    }
    seen_digit.then_some(0)
}

/// The column each placeholder is compared with, when every placeholder in
/// `sql` stands as `column <op> $n` with `op` one of `= <> != < <= > >=`, each
/// placeholder occurs once, and nothing in the statement rewrites a value by
/// what it holds (`LIKE` turns `'ab%'` into a prefix test and `'ab'` into an
/// equality, `IN` and `BETWEEN` expand). Indexed by parameter number - 1.
///
/// Deliberately narrow. Two executions verifying a template cannot rule out a
/// rewrite that only some values trigger; this leaves no rewrite to trigger.
pub fn compared_columns(sql: &str) -> Option<Vec<String>> {
    let b = sql.as_bytes();
    // A statement with no placeholders is the same text every time, so no
    // value can steer a rewrite and the rules below that exist for values do
    // not apply; the clock and function rules still do.
    let has_params = {
        let mut i = 0usize;
        let mut found = false;
        while i < b.len() {
            match b[i] {
                b'\'' | b'"' => {
                    let q = b[i];
                    i += 1;
                    while i < b.len() && b[i] != q {
                        i += 1;
                    }
                }
                b'$' if b.get(i + 1).is_some_and(|d| d.is_ascii_digit()) => {
                    found = true;
                    break;
                }
                _ => {}
            }
            i += 1;
        }
        found
    };
    // At any depth, outside quotes: a `LIKE` inside parentheses rewrites by
    // value as much as one outside.
    let mut i = 0usize;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' => {
                let q = b[i];
                i += 1;
                while i < b.len() && b[i] != q {
                    i += 1;
                }
                i += 1;
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                    i += 1;
                }
                let word = &sql[start..i];
                if has_params
                    && ["LIKE", "ILIKE", "SIMILAR", "IN", "BETWEEN", "ANY", "ALL", "ESCAPE", "SOME"]
                        .iter()
                        .any(|w| word.eq_ignore_ascii_case(w))
                {
                    return None;
                }
                // A value read from the clock or the session: the parse would
                // keep the first execution's answer for every later one.
                if [
                    "CURRENT_DATE",
                    "CURRENT_TIME",
                    "CURRENT_TIMESTAMP",
                    "LOCALTIME",
                    "LOCALTIMESTAMP",
                    "TRANSACTION_TIMESTAMP",
                    "CURRENT_USER",
                    "SESSION_USER",
                    "USER",
                    "CURRENT_SCHEMA",
                    "CURRENT_CATALOG",
                    "CURRENT_ROLE",
                ]
                .iter()
                .any(|w| word.eq_ignore_ascii_case(w))
                {
                    return None;
                }
                // A call. Aggregates fold the rows, which are read afresh each
                // time; anything else (`now()`, `random()`, `date_trunc(...)`)
                // may be evaluated into the parse.
                let mut after = i;
                while after < b.len() && b[after].is_ascii_whitespace() {
                    after += 1;
                }
                if b.get(after) == Some(&b'(')
                    && ![
                        "COUNT", "SUM", "AVG", "MIN", "MAX",
                        // Keywords a parenthesis follows, not calls.
                        "IN", "ANY", "ALL", "SOME", "AND", "OR", "NOT", "ON", "USING", "WHERE", "FROM",
                    ]
                    .iter()
                    .any(|w| word.eq_ignore_ascii_case(w))
                {
                    return None;
                }
            }
            _ => i += 1,
        }
    }
    let mut columns: Vec<Option<String>> = Vec::new();
    let mut i = 0usize;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' => {
                let q = b[i];
                i += 1;
                while i < b.len() && b[i] != q {
                    i += 1;
                }
                i += 1;
            }
            b'~' if has_params => return None,
            b'$' => {
                let mut j = i + 1;
                while j < b.len() && b[j].is_ascii_digit() {
                    j += 1;
                }
                if j == i + 1 {
                    return None;
                }
                let n: usize = sql[i + 1..j].parse().ok()?;
                if n == 0 || n > 64 {
                    return None;
                }
                // Forwards: nothing may combine with the value (`$1 + 1`,
                // `$1::date`, `$1 || 'x'`); only the clause going on.
                let mut f = j;
                while f < b.len() && b[f].is_ascii_whitespace() {
                    f += 1;
                }
                if f < b.len() && !(b[f].is_ascii_alphabetic() || matches!(b[f], b')' | b';' | b',')) {
                    return None;
                }
                // Backwards: the operator, then the column.
                let mut k = i;
                while k > 0 && b[k - 1].is_ascii_whitespace() {
                    k -= 1;
                }
                let op_end = k;
                while k > 0 && matches!(b[k - 1], b'=' | b'<' | b'>' | b'!' | b'@') {
                    k -= 1;
                }
                let op = &sql[k..op_end];
                // `@>` against a document: the literal is encoded, never
                // rewritten by what it holds (`class_for` admits it only for
                // `jsonb`).
                if !matches!(op, "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=" | "@>") {
                    return None;
                }
                while k > 0 && b[k - 1].is_ascii_whitespace() {
                    k -= 1;
                }
                let name_end = k;
                while k > 0 && (b[k - 1].is_ascii_alphanumeric() || b[k - 1] == b'_' || b[k - 1] == b'.') {
                    k -= 1;
                }
                let name = &sql[k..name_end];
                let column = name.rsplit('.').next().unwrap_or(name);
                if column.is_empty() || column.as_bytes()[0].is_ascii_digit() {
                    return None;
                }
                if columns.len() < n {
                    columns.resize(n, None);
                }
                if columns[n - 1].is_some() {
                    return None;
                }
                columns[n - 1] = Some(column.to_string());
                i = j;
            }
            _ => i += 1,
        }
    }
    if columns.is_empty() {
        return (!has_params).then(Vec::new);
    }
    columns.into_iter().collect()
}

/// The literal class a value compared with a column of `ty` may take, for
/// the types whose literals no rewrite reinterprets: numbers for numeric
/// columns, plain strings for text.
pub fn class_for(ty: &DbType) -> Option<u8> {
    use DbType::*;
    match ty {
        I8 | I16 | I32 | I64 | I128 | U8 | U16 | U32 | U64 | U128 | F32 | F64 | DECIMAL => Some(0),
        // Text, and a document's text: both a single-quoted literal the
        // parser encodes as it stands.
        STRING | JSONB => Some(1),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Piece {
    Text(String),
    Param(usize),
}

/// A `WHERE` clause with the places each value goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WhereTemplate {
    pieces: Vec<Piece>,
}

impl WhereTemplate {
    /// Splits `where_clause` at each literal. `None` unless every literal
    /// occurs exactly once, none inside another, so each place is certain.
    pub fn derive(where_clause: &str, literals: &[String]) -> Option<Self> {
        let mut spots: Vec<(usize, usize)> = Vec::with_capacity(literals.len());
        for (i, lit) in literals.iter().enumerate() {
            if lit.is_empty() {
                return None;
            }
            let mut found = where_clause.match_indices(lit.as_str());
            let (at, _) = found.next()?;
            if found.next().is_some() {
                return None;
            }
            spots.push((at, i));
        }
        spots.sort_unstable();
        let mut pieces = Vec::with_capacity(spots.len() * 2 + 1);
        let mut copied = 0usize;
        for (at, i) in spots {
            if at < copied {
                return None;
            }
            pieces.push(Piece::Text(where_clause[copied..at].to_string()));
            pieces.push(Piece::Param(i));
            copied = at + literals[i].len();
        }
        pieces.push(Piece::Text(where_clause[copied..].to_string()));
        Some(Self { pieces })
    }

    pub fn render(&self, literals: &[String]) -> String {
        let mut out = String::with_capacity(
            self.pieces
                .iter()
                .map(|p| match p {
                    Piece::Text(t) => t.len(),
                    Piece::Param(i) => literals.get(*i).map_or(0, |l| l.len()),
                })
                .sum(),
        );
        for p in &self.pieces {
            match p {
                Piece::Text(t) => out.push_str(t),
                Piece::Param(i) => out.push_str(literals.get(*i).map_or("", |l| l.as_str())),
            }
        }
        out
    }
}

/// `query_row` with its `WHERE` clause blanked, for comparing the rest.
pub fn shape_of(query_row: &QueryRows) -> String {
    let mut q = query_row.clone();
    q.where_clause = String::new();
    format!("{:?}", q)
}

/// How many times the executor parsed a single-table `SELECT` on each table,
/// for the tests that show a learned execution skips the parse. Keyed by
/// table name so tests running at once never see each other's.
#[cfg(test)]
pub(crate) mod test_counters {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    fn parses() -> &'static Mutex<HashMap<String, u64>> {
        static P: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();
        P.get_or_init(Default::default)
    }

    pub(crate) fn note_parse(table: &str) {
        *parses().lock().unwrap().entry(table.to_string()).or_default() += 1;
    }

    /// The `SELECT` parses on `table` so far.
    pub(crate) fn parses_of(table: &str) -> u64 {
        parses().lock().unwrap().get(table).copied().unwrap_or(0)
    }

    fn learned_runs() -> &'static Mutex<HashMap<String, Vec<bool>>> {
        static R: OnceLock<Mutex<HashMap<String, Vec<bool>>>> = OnceLock::new();
        R.get_or_init(Default::default)
    }

    /// A learned parse on `table` ran, inside `statement_snapshot::scope` or not.
    pub(crate) fn note_learned_run(table: &str, in_scope: bool) {
        learned_runs().lock().unwrap().entry(table.to_string()).or_default().push(in_scope);
    }

    /// Whether each learned run on `table` since the last call was scoped.
    pub(crate) fn take_learned_runs(table: &str) -> Vec<bool> {
        learned_runs().lock().unwrap().remove(table).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lits(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_template_renders_the_values_where_they_were() {
        let t = WhereTemplate::derive("k = 42 AND s = 'abc'", &lits(&["42", "'abc'"])).unwrap();
        assert_eq!(t.render(&lits(&["7", "'zz'"])), "k = 7 AND s = 'zz'");
        // Order of the values is by parameter, not by position.
        let t = WhereTemplate::derive("s = 'abc' AND k = 42", &lits(&["42", "'abc'"])).unwrap();
        assert_eq!(t.render(&lits(&["7", "'zz'"])), "s = 'zz' AND k = 7");
    }

    #[test]
    fn an_uncertain_place_is_refused() {
        // Twice, so which one is the value is a guess.
        assert!(WhereTemplate::derive("k = 5 AND j = 5", &lits(&["5"])).is_none());
        assert!(WhereTemplate::derive("k = 15 AND j = 5", &lits(&["5"])).is_none());
        // Not there at all: something rewrote it.
        assert!(WhereTemplate::derive("k IS NULL", &lits(&["NULL2"])).is_none());
        // Two values on the same text.
        assert!(WhereTemplate::derive("k = 3", &lits(&["3", "3"])).is_none());
    }

    #[test]
    fn only_plain_comparisons_qualify() {
        assert_eq!(compared_columns("SELECT v FROM kv WHERE k = $1"), Some(vec!["k".to_string()]));
        assert_eq!(compared_columns("SELECT count(*), max(v) FROM kv WHERE k = $1"), Some(vec!["k".to_string()]));
        assert_eq!(compared_columns("SELECT id FROM docs WHERE doc @> $1"), Some(vec!["doc".to_string()]));
        // No placeholders: the same text every time, LIKE and IN included.
        assert_eq!(compared_columns("SELECT * FROM t WHERE a = 1"), Some(vec![]));
        assert_eq!(compared_columns("SELECT * FROM t WHERE s LIKE 'ab%' AND a IN (1, 2)"), Some(vec![]));
        // ... but never the clock.
        assert_eq!(compared_columns("SELECT * FROM t WHERE at < now()"), None);
        assert_eq!(compared_columns("SELECT * FROM t WHERE at < CURRENT_DATE"), None);
        assert_eq!(
            compared_columns("SELECT * FROM t WHERE t.a >= $2 AND b<>$1 ORDER BY a LIMIT 5"),
            Some(vec!["b".to_string(), "a".to_string()])
        );
        for sql in [
            "SELECT * FROM t WHERE name LIKE $1",
            "SELECT * FROM t WHERE a IN ($1, $2)",
            "SELECT * FROM t WHERE a BETWEEN $1 AND $2",
            "SELECT * FROM t WHERE a = ANY($1)",
            "SELECT * FROM t WHERE (a = 1 OR b LIKE 'x%') AND c = $1",
            "SELECT * FROM t WHERE a = $1 + 1",
            "SELECT * FROM t WHERE a = $1::bigint",
            "SELECT * FROM t WHERE a = $1 || 'x'",
            "SELECT * FROM t WHERE a = $1 AND created >= CURRENT_DATE",
            "SELECT * FROM t WHERE a = $1 AND created >= now()",
            "SELECT * FROM t WHERE a = $1 AND created >= date_trunc('hour', created)",
            "SELECT now(), a FROM t WHERE a = $1",
            "SELECT a FROM t WHERE a = $1 AND owner = current_user",
            "SELECT * FROM t WHERE $1 = a",
            "SELECT * FROM t WHERE a = $1 OR b = $1",
            "SELECT * FROM t WHERE a = $1 LIMIT $2",
            "SELECT * FROM t WHERE a ~ $1",
        ] {
            assert_eq!(compared_columns(sql), None, "{sql}");
        }
    }

    #[test]
    fn literal_classes() {
        assert_eq!(literal_class("42"), Some(0));
        assert_eq!(literal_class("-4.5"), Some(0));
        assert_eq!(literal_class("'abc'"), Some(1));
        assert_eq!(literal_class("''"), Some(1));
        assert_eq!(literal_class("NULL"), None);
        assert_eq!(literal_class("'it''s'"), None);
        assert_eq!(literal_class("E'a\\nb'"), None);
        assert_eq!(literal_class("1e5"), None);
        assert_eq!(literal_class("-"), None);
        assert_eq!(literal_class("1.2.3"), None);
    }

    #[tokio::test]
    async fn the_capture_sees_only_its_own_scope() {
        note_select_entry(); // outside any scope: a no-op
        let ((), c) = capturing(async {
            note_select_entry();
            note_rewritten();
        })
        .await;
        assert_eq!(c.arm_entries, 1);
        assert!(c.invalid);
        assert!(c.plain_only().is_none());
    }
}
