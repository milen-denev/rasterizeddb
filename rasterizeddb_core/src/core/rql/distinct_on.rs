//! `SELECT DISTINCT ON (k, …)` over a join, or over a select list or an
//! `ORDER BY` with an aggregate or a window function, answered by the
//! single-table `DISTINCT ON` the scan already has, over a derived table.
//!
//! PostgreSQL evaluates such a statement in this order: `FROM` (the join),
//! `WHERE`, `GROUP BY`, `HAVING`, the windows and the select list, then
//! `ORDER BY` sorts, `DISTINCT ON` keeps the first row of each key, and
//! `LIMIT`/`OFFSET` page what is kept. Everything up to the select list is a
//! query of its own, so
//!
//! ```sql
//! SELECT DISTINCT ON (k) items FROM src … ORDER BY o LIMIT n
//! ```
//!
//! is answered as
//!
//! ```sql
//! SELECT DISTINCT ON (k') __don_c0 AS label0, …
//!   FROM (SELECT item0 AS __don_c0, …, k AS __don_k0, o AS __don_k1 FROM src …) __don
//!  ORDER BY o' LIMIT n
//! ```
//!
//! where each key and sort term is the select item it names (an ordinal, an
//! output name, or the same expression) or a column of its own. The inner
//! query computes every value exactly as the statement would before its
//! `DISTINCT ON`; the outer one only sorts, keeps and pages.
//!
//! A statement this cannot rewrite faithfully is left alone, and so keeps the
//! refusal it had: a `*` (its columns cannot be named here), a set operation,
//! a row lock, an ambiguous output name, an ordinal out of range.

use crate::core::rql::join::{find_keyword, find_phrase, pg_column_name};

/// The rewritten statement, or `None` when `sql` is not a `DISTINCT ON` that
/// needs it (a single-table one without aggregates or windows is answered
/// natively) or cannot be rewritten faithfully. A `WITH` statement has its
/// outer `SELECT` rewritten.
pub(crate) fn rewrite_statement(sql: &str) -> Option<String> {
    if !mentions_distinct(sql) {
        return None;
    }
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    if crate::core::rql::cte::has_with_clause(trimmed) {
        let (_, outer) = crate::core::rql::cte::split_with(trimmed).ok()??;
        let outer = outer.trim();
        if outer.is_empty() || !trimmed.ends_with(outer) {
            return None;
        }
        let prefix = &trimmed[..trimmed.len() - outer.len()];
        return Some(format!("{prefix}{}", rewrite_select(outer)?));
    }
    rewrite_select(trimmed)
}

fn mentions_distinct(sql: &str) -> bool {
    sql.as_bytes().windows(8).any(|w| w.eq_ignore_ascii_case(b"DISTINCT"))
}

/// `text` past a leading `word` (any case) and the whitespace after it.
fn strip_word<'a>(text: &'a str, word: &str) -> Option<&'a str> {
    let t = text.trim_start();
    let head = t.get(..word.len())?;
    if !head.eq_ignore_ascii_case(word) {
        return None;
    }
    let rest = &t[word.len()..];
    match rest.as_bytes().first() {
        Some(c) if c.is_ascii_alphanumeric() || *c == b'_' => None,
        _ => Some(rest.trim_start()),
    }
}

/// Where the phrase starting at `at` (`ORDER BY`, `GROUP BY`) ends.
fn phrase_end(sql: &str, at: usize, words: &[&str]) -> usize {
    let mut cursor = at;
    for (i, word) in words.iter().enumerate() {
        if i > 0 {
            cursor = sql.len() - sql[cursor..].trim_start().len();
        }
        cursor += word.len();
    }
    cursor
}

/// A comma at nesting depth zero, outside quotes: `FROM a, b`.
fn top_level_comma(text: &str) -> bool {
    crate::core::rql::lexer_ct::split_definitions(text).len() > 1
}

/// Whether `text` holds `word` as a whole word outside quotes.
fn has_word(text: &str, word: &str) -> bool {
    word_positions(text, word).next().is_some()
}

fn word_positions<'a>(text: &'a str, word: &'a str) -> impl Iterator<Item = usize> + 'a {
    let bytes = text.as_bytes();
    let mut quote: Option<u8> = None;
    let mut i = 0usize;
    std::iter::from_fn(move || {
        while i < bytes.len() {
            let c = bytes[i];
            match quote {
                Some(q) => {
                    if c == q {
                        quote = None;
                    }
                }
                None if c == b'\'' || c == b'"' => quote = Some(c),
                None => {
                    let ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
                    if (i == 0 || !ident(bytes[i - 1]))
                        && bytes.len() - i >= word.len()
                        && bytes[i..i + word.len()].eq_ignore_ascii_case(word.as_bytes())
                        && bytes.get(i + word.len()).is_none_or(|b| !ident(*b))
                    {
                        let at = i;
                        i += word.len();
                        return Some(at);
                    }
                }
            }
            i += 1;
        }
        None
    })
}

/// The aggregates PostgreSQL has, by name. A call to one in the select list or
/// the `ORDER BY` is what the native path refuses; naming one that is not an
/// aggregate here only costs the derived table.
const AGGREGATES: &[&str] = &[
    "count", "sum", "avg", "min", "max", "string_agg", "array_agg", "bool_and", "bool_or", "every", "json_agg",
    "jsonb_agg", "json_object_agg", "jsonb_object_agg", "stddev", "stddev_pop", "stddev_samp", "variance",
    "var_pop", "var_samp", "bit_and", "bit_or", "bit_xor", "percentile_cont", "percentile_disc", "mode",
];

fn calls_an_aggregate(text: &str) -> bool {
    AGGREGATES.iter().any(|name| {
        word_positions(text, name).any(|at| text[at + name.len()..].trim_start().starts_with('('))
    })
}

/// An expression's text compared as written: whitespace collapsed (and
/// dropped beside punctuation), letters folded and enclosing parentheses
/// removed, all outside quotes, so `(ts % 2)` and `ts%2` are one key and
/// `x = 'A'` and `x = 'a'` are two.
fn norm(text: &str) -> String {
    let mut t = text.trim();
    while t.starts_with('(') && crate::core::rql::expr::matching_paren(t, 0) == Some(t.len() - 1) {
        t = t[1..t.len() - 1].trim();
    }
    let mut out = String::with_capacity(t.len());
    let mut quote: Option<char> = None;
    let mut pending_space = false;
    let word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    for c in t.chars() {
        match quote {
            Some(q) => {
                out.push(c);
                if c == q {
                    quote = None;
                }
            }
            None if c.is_whitespace() => pending_space = true,
            None => {
                if pending_space && out.chars().last().is_some_and(word) && word(c) {
                    out.push(' ');
                }
                pending_space = false;
                if c == '\'' || c == '"' {
                    quote = Some(c);
                }
                out.push(c.to_ascii_lowercase());
            }
        }
    }
    out
}

/// PostgreSQL's name for an unaliased item: a window call or a filtered
/// aggregate is named after its function (`count(*) OVER ()` is `count`), so
/// the name is figured from the text before a top-level `OVER` or `FILTER`.
fn figure_name(expr: &str) -> String {
    let cut = ["OVER", "FILTER"]
        .iter()
        .filter_map(|w| word_positions(expr, w).find(|&at| depth_at(expr, at) == 0))
        .min();
    pg_column_name(cut.map_or(expr, |at| expr[..at].trim_end()))
}

/// A bare identifier, maybe quoted: what can name an output column.
fn as_identifier(text: &str) -> Option<(&str, bool)> {
    let t = text.trim();
    if t.len() >= 2 && t.starts_with('"') && t.ends_with('"') && !t[1..t.len() - 1].contains('"') {
        return Some((&t[1..t.len() - 1], true));
    }
    let mut chars = t.chars();
    let first = chars.next()?;
    (first.is_ascii_alphabetic() || first == '_')
        .then_some(())
        .filter(|_| chars.all(|c| c.is_ascii_alphanumeric() || c == '_'))
        .map(|_| (t, false))
}

/// `expr AS alias` split at its top-level `AS`, outside quotes; the alias as
/// written (quotes kept).
fn split_as(item: &str) -> (&str, Option<&str>) {
    let mut last = None;
    for at in word_positions(item, "AS") {
        if depth_at(item, at) == 0 {
            last = Some(at);
        }
    }
    match last {
        Some(at) if !item[at + 2..].trim().is_empty() => (item[..at].trim(), Some(item[at + 2..].trim())),
        _ => (item.trim(), None),
    }
}

/// Parenthesis depth at byte `at`, outside quotes.
fn depth_at(text: &str, at: usize) -> usize {
    let mut depth = 0usize;
    let mut quote: Option<u8> = None;
    for &c in &text.as_bytes()[..at] {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None if c == b'\'' || c == b'"' => quote = Some(c),
            None if c == b'(' => depth += 1,
            None if c == b')' => depth = depth.saturating_sub(1),
            None => {}
        }
    }
    depth
}

struct Item {
    expr: String,
    /// The output name as it is written into the outer select list.
    label: String,
    /// The output name a key or sort term can refer to, and whether it was
    /// quoted (compared exactly) or not (compared ignoring case).
    name: String,
    quoted: bool,
}

struct Columns {
    items: Vec<Item>,
    /// Keys and sort terms that are no select item: `(norm, text)`.
    extra: Vec<(String, String)>,
}

impl Columns {
    /// The derived table's column a key or sort term reads, or `None` when it
    /// cannot be resolved faithfully (an ordinal out of range, an ambiguous
    /// output name).
    fn resolve(&mut self, term: &str) -> Option<String> {
        let t = term.trim();
        if !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit()) {
            let n: usize = t.parse().ok()?;
            return (1..=self.items.len()).contains(&n).then(|| format!("__don_c{}", n - 1));
        }
        if let Some((ident, quoted)) = as_identifier(t) {
            let hits: Vec<usize> = (0..self.items.len())
                .filter(|&i| {
                    let item = &self.items[i];
                    if quoted || item.quoted { item.name == ident } else { item.name.eq_ignore_ascii_case(ident) }
                })
                .collect();
            match hits.as_slice() {
                [one] => return Some(format!("__don_c{one}")),
                [] => {}
                _ => return None,
            }
        }
        let key = norm(t);
        if let Some(i) = self.items.iter().position(|item| norm(&item.expr) == key) {
            return Some(format!("__don_c{i}"));
        }
        if let Some(j) = self.extra.iter().position(|(n, _)| *n == key) {
            return Some(format!("__don_k{j}"));
        }
        self.extra.push((key, t.to_string()));
        Some(format!("__don_k{}", self.extra.len() - 1))
    }
}

/// A sort term split into its expression and its `ASC`/`DESC` and `NULLS
/// FIRST`/`NULLS LAST`, rendered back as written (`""`, `" DESC NULLS
/// LAST"`). Read from the end, so an expression of any shape — a window call
/// with its own `ORDER BY`, a `CASE` — is kept whole.
fn sort_term(raw: &str) -> Option<(&str, String)> {
    let mut rest = raw.trim();
    let mut nulls = "";
    if let Some((before, w)) = last_word(rest) {
        if let ("FIRST" | "LAST", Some((before_nulls, n))) = (w.as_str(), last_word(before)) {
            if n == "NULLS" {
                nulls = if w == "FIRST" { " NULLS FIRST" } else { " NULLS LAST" };
                rest = before_nulls;
            }
        }
    }
    let mut direction = "";
    if let Some((before, w)) = last_word(rest) {
        if w == "DESC" || w == "ASC" {
            direction = if w == "DESC" { " DESC" } else { "" };
            rest = before;
        }
    }
    let expr = rest.trim();
    (!expr.is_empty()).then(|| (expr, format!("{direction}{nulls}")))
}

/// The last word of `text` when it is all letters and whitespace precedes
/// it: the text before it and the word, upper-cased.
fn last_word(text: &str) -> Option<(&str, String)> {
    let t = text.trim_end();
    let start = t.rfind(|c: char| !c.is_ascii_alphabetic()).map_or(0, |i| i + 1);
    let word = &t[start..];
    if word.is_empty() || !t[..start].ends_with(char::is_whitespace) {
        return None;
    }
    Some((t[..start].trim_end(), word.to_ascii_uppercase()))
}

fn rewrite_select(sql: &str) -> Option<String> {
    let after_select = strip_word(sql, "SELECT")?;
    let after_distinct = strip_word(after_select, "DISTINCT")?;
    let on = strip_word(after_distinct, "ON")?;
    if !on.starts_with('(') {
        return None;
    }
    let open = sql.len() - on.len();
    let close = crate::core::rql::expr::matching_paren(sql, open)?;
    let keys_text = &sql[open + 1..close];
    let from_at = find_keyword(sql, "FROM", close + 1)?;
    let list = sql[close + 1..from_at].trim();

    // A set operation or a row lock is not one select's rows.
    for word in ["UNION", "INTERSECT", "EXCEPT", "FOR"] {
        if find_keyword(sql, word, from_at).is_some() {
            return None;
        }
    }

    let order_at = find_phrase(sql, &["ORDER", "BY"], from_at);
    let after_order = order_at.map(|at| phrase_end(sql, at, &["ORDER", "BY"])).unwrap_or(from_at);
    let tail_at = ["LIMIT", "OFFSET", "FETCH"]
        .iter()
        .filter_map(|w| find_keyword(sql, w, after_order))
        .min()
        .unwrap_or(sql.len());
    let body_end = order_at.unwrap_or(tail_at).min(tail_at);
    let body = &sql[from_at..body_end];
    let order_text = order_at.filter(|&at| at < tail_at).map(|_| &sql[after_order..tail_at]);
    let tail = sql[tail_at..].trim();

    // Only what the native path refuses: a join (or a comma list) in FROM, a
    // GROUP BY or HAVING, an aggregate or a window function.
    let source_end = ["WHERE", "GROUP", "HAVING", "WINDOW"]
        .iter()
        .filter_map(|w| find_keyword(body, w, 0))
        .min()
        .unwrap_or(body.len());
    let source = &body["FROM".len()..source_end];
    let joined = find_keyword(source, "JOIN", 0).is_some() || top_level_comma(source);
    let grouped = find_keyword(body, "GROUP", 0).is_some() || find_keyword(body, "HAVING", 0).is_some();
    let order_str = order_text.unwrap_or("");
    let computed = has_word(list, "OVER")
        || has_word(order_str, "OVER")
        || calls_an_aggregate(list)
        || calls_an_aggregate(order_str);
    if !(joined || grouped || computed) {
        return None;
    }

    let mut columns = Columns { items: Vec::new(), extra: Vec::new() };
    for raw in crate::core::rql::lexer_ct::split_definitions(list) {
        let raw = raw.trim();
        if raw.is_empty() || raw == "*" || raw.ends_with(".*") {
            return None;
        }
        let (expr, alias) = split_as(raw);
        let (label, name, quoted) = match alias {
            Some(alias) => {
                let (name, quoted) = as_identifier(alias)?;
                (alias.to_string(), name.to_string(), quoted)
            }
            None => {
                let name = figure_name(expr);
                (format!("\"{name}\""), name, false)
            }
        };
        columns.items.push(Item { expr: expr.to_string(), label, name, quoted });
    }
    if columns.items.is_empty() {
        return None;
    }

    let mut keys = Vec::new();
    for key in crate::core::rql::lexer_ct::split_definitions(keys_text) {
        if key.trim().is_empty() {
            return None;
        }
        keys.push(columns.resolve(&key)?);
    }
    if keys.is_empty() {
        return None;
    }
    let mut order = Vec::new();
    if let Some(text) = order_text {
        for raw in crate::core::rql::lexer_ct::split_definitions(text) {
            let (expr, modifiers) = sort_term(&raw)?;
            let column = columns.resolve(expr)?;
            order.push(format!("{column}{modifiers}"));
        }
    }

    let mut inner: Vec<String> =
        columns.items.iter().enumerate().map(|(i, item)| format!("{} AS __don_c{i}", item.expr)).collect();
    inner.extend(columns.extra.iter().enumerate().map(|(j, (_, text))| format!("{text} AS __don_k{j}")));
    let outer: Vec<String> =
        columns.items.iter().enumerate().map(|(i, item)| format!("__don_c{i} AS {}", item.label)).collect();
    let mut out = format!(
        "SELECT DISTINCT ON ({}) {} FROM (SELECT {} {}) __don",
        keys.join(", "),
        outer.join(", "),
        inner.join(", "),
        body.trim()
    );
    if !order.is_empty() {
        out.push_str(" ORDER BY ");
        out.push_str(&order.join(", "));
    }
    if !tail.is_empty() {
        out.push(' ');
        out.push_str(tail);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::rewrite_statement as rw;

    #[test]
    fn a_single_table_distinct_on_is_left_to_the_scan() {
        assert_eq!(rw("SELECT DISTINCT ON (a) a, b FROM t ORDER BY a, b DESC"), None);
        assert_eq!(rw("SELECT DISTINCT a FROM t JOIN u ON t.id = u.id"), None);
        assert_eq!(rw("SELECT a FROM t"), None);
    }

    #[test]
    fn a_join_reads_a_derived_table() {
        assert_eq!(
            rw("SELECT DISTINCT ON (t.a) t.a, u.b AS bee FROM t JOIN u ON t.id = u.id WHERE u.b > 1 ORDER BY t.a, u.c DESC NULLS LAST LIMIT 3;")
                .unwrap(),
            "SELECT DISTINCT ON (__don_c0) __don_c0 AS \"a\", __don_c1 AS bee FROM (SELECT t.a AS __don_c0, u.b AS __don_c1, \
             u.c AS __don_k0 FROM t JOIN u ON t.id = u.id WHERE u.b > 1) __don ORDER BY __don_c0, __don_k0 DESC NULLS LAST LIMIT 3"
        );
        // A comma join, an ordinal and an output name.
        assert_eq!(
            rw("select distinct on (1) x.k, y.v as \"V\" from x, y order by 1, \"V\"").unwrap(),
            "SELECT DISTINCT ON (__don_c0) __don_c0 AS \"k\", __don_c1 AS \"V\" FROM (SELECT x.k AS __don_c0, y.v AS __don_c1 \
             from x, y) __don ORDER BY __don_c0, __don_c1"
        );
    }

    #[test]
    fn aggregates_and_windows_are_computed_inside() {
        assert_eq!(
            rw("SELECT DISTINCT ON (k) k, count(*) FROM t GROUP BY k, j ORDER BY k, count(*) DESC").unwrap(),
            "SELECT DISTINCT ON (__don_c0) __don_c0 AS \"k\", __don_c1 AS \"count\" FROM (SELECT k AS __don_c0, count(*) AS \
             __don_c1 FROM t GROUP BY k, j) __don ORDER BY __don_c0, __don_c1 DESC"
        );
        assert_eq!(
            rw("SELECT DISTINCT ON (k) k, row_number() OVER (ORDER BY v) AS rn FROM t ORDER BY k, rn").unwrap(),
            "SELECT DISTINCT ON (__don_c0) __don_c0 AS \"k\", __don_c1 AS rn FROM (SELECT k AS __don_c0, row_number() OVER \
             (ORDER BY v) AS __don_c1 FROM t) __don ORDER BY __don_c0, __don_c1"
        );
    }

    #[test]
    fn a_with_statement_has_its_outer_select_rewritten() {
        assert_eq!(
            rw("WITH s AS (SELECT 1 AS a) SELECT DISTINCT ON (s.a) s.a FROM s JOIN t ON t.a = s.a").unwrap(),
            "WITH s AS (SELECT 1 AS a) SELECT DISTINCT ON (__don_c0) __don_c0 AS \"a\" FROM (SELECT s.a AS __don_c0 FROM s \
             JOIN t ON t.a = s.a) __don"
        );
    }

    #[test]
    fn windows_and_filtered_aggregates_are_named_after_their_function() {
        assert_eq!(super::figure_name("count(*) OVER ()"), "count");
        assert_eq!(super::figure_name("max(id) OVER w"), "max");
        assert_eq!(super::figure_name("sum(x) FILTER (WHERE x > 0)"), "sum");
        assert_eq!(super::figure_name("coalesce(sum(x) OVER (), 0)"), "coalesce");
        assert_eq!(super::figure_name("t.a"), "a");
        assert_eq!(super::figure_name("a + 1"), "?column?");
    }

    #[test]
    fn sort_terms_keep_their_expression_whole() {
        use super::sort_term;
        assert_eq!(sort_term("rank() OVER (ORDER BY ts DESC) DESC"), Some(("rank() OVER (ORDER BY ts DESC)", " DESC".into())));
        assert_eq!(sort_term("x  desc  nulls   last"), Some(("x", " DESC NULLS LAST".into())));
        assert_eq!(sort_term("x NULLS FIRST"), Some(("x", " NULLS FIRST".into())));
        assert_eq!(sort_term("x ASC"), Some(("x", "".into())));
        assert_eq!(sort_term("CASE WHEN a THEN 1 ELSE 2 END"), Some(("CASE WHEN a THEN 1 ELSE 2 END", "".into())));
        assert_eq!(sort_term("x || 'DESC'"), Some(("x || 'DESC'", "".into())));
        assert_eq!(sort_term("lastname"), Some(("lastname", "".into())));
    }

    #[test]
    fn what_cannot_be_rewritten_faithfully_is_left_alone() {
        // `*`, a set operation, a row lock, an ambiguous name, an ordinal
        // out of range.
        for sql in [
            "SELECT DISTINCT ON (t.a) * FROM t JOIN u ON t.id = u.id",
            "SELECT DISTINCT ON (t.a) t.* FROM t JOIN u ON t.id = u.id",
            "SELECT DISTINCT ON (a) a FROM t JOIN u ON true UNION SELECT 1",
            "SELECT DISTINCT ON (a) a FROM t JOIN u ON true FOR UPDATE",
            "SELECT DISTINCT ON (id) t.id, u.id FROM t JOIN u ON true",
            "SELECT DISTINCT ON (3) t.id, u.id FROM t JOIN u ON true",
        ] {
            assert_eq!(rw(sql), None, "{sql}");
        }
    }

    #[test]
    fn keys_are_compared_outside_quotes_only() {
        // Two literals differing in case are two keys, not one.
        let sql = "SELECT DISTINCT ON (coalesce(t.a, 'A')) t.a FROM t JOIN u ON true ORDER BY coalesce(t.a, 'a')";
        let out = rw(sql).unwrap();
        assert!(out.contains("coalesce(t.a, 'A') AS __don_k0") && out.contains("coalesce(t.a, 'a') AS __don_k1"), "{out}");
        // and the same key written with other spacing and case is one key.
        let sql = "SELECT DISTINCT ON (COALESCE( t.a ,'A')) t.a FROM t JOIN u ON true ORDER BY coalesce(t.a, 'A')";
        let out = rw(sql).unwrap();
        assert!(out.ends_with("ORDER BY __don_k0") && !out.contains("__don_k1"), "{out}");
        // A word inside a literal is no join and no window.
        assert_eq!(rw("SELECT DISTINCT ON (a) a, 'x JOIN y OVER' FROM t ORDER BY a"), None);
    }
}
