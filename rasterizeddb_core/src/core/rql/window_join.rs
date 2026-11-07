//! A window function over a join, answered by the single-table windows the
//! scan already has, over a derived table.
//!
//! PostgreSQL evaluates `FROM` (the join) and `WHERE` before any window, so
//!
//! ```sql
//! SELECT a.id, rank() OVER (PARTITION BY a.g ORDER BY b.w) FROM a JOIN b ON … WHERE … ORDER BY 2
//! ```
//!
//! is answered as
//!
//! ```sql
//! SELECT __w0 AS "id", rank() OVER (PARTITION BY __w1 ORDER BY __w2) AS "rank"
//!   FROM (SELECT a.id AS __w0, a.g AS __w1, b.w AS __w2 FROM a JOIN b ON … WHERE …) __w
//!  ORDER BY 2
//! ```
//!
//! Every column the select list and the `ORDER BY` read is exposed by the
//! inner query once; the outer one computes the windows and keeps the output
//! names PostgreSQL gives the items. It was refused ("could not be read as an
//! expression over this join").
//!
//! Left alone, and so keeping the refusal it had: a `*`, a `GROUP BY` or
//! `HAVING` (windows over groups), a set operation, a row lock, a relation
//! whose columns are not known here (a CTE or a derived table, not yet
//! materialised), or a bare column name two relations share.

use crate::core::rql::join::{find_keyword, find_phrase, pg_column_name};

/// Words that are never a column here, whatever a table names its columns:
/// the window clause's own vocabulary and the operators a select item uses.
const KEYWORDS: &[&str] = &[
    "AND", "AS", "ASC", "BETWEEN", "BY", "CASE", "CAST", "CURRENT", "DESC", "DISTINCT", "ELSE", "END", "EXCLUDE",
    "FALSE", "FILTER", "FIRST", "FOLLOWING", "FROM", "GROUPS", "IN", "INTERVAL", "IS", "LAST", "LIKE", "ILIKE", "NOT",
    "NULL", "NULLS", "OR", "ORDER", "OTHERS", "OVER", "PARTITION", "PRECEDING", "RANGE", "ROW", "ROWS", "THEN", "TIES",
    "TRUE", "UNBOUNDED", "USING", "WHEN", "WHERE",
];

/// The rewritten statement, or `None` when `sql` is not a window over a join
/// or cannot be rewritten faithfully. `columns_of` names a relation's
/// columns, `None` for one it does not know.
pub(crate) fn rewrite_statement(sql: &str, columns_of: &dyn Fn(&str) -> Option<Vec<String>>) -> Option<String> {
    if find_keyword(sql, "OVER", 0).is_none() {
        return None;
    }
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    if crate::core::rql::cte::has_with_clause(trimmed) {
        // A CTE's columns are not known before it is materialised.
        return None;
    }
    rewrite_select(trimmed, columns_of)
}

fn strip_word<'a>(text: &'a str, word: &str) -> Option<&'a str> {
    let t = text.trim_start();
    let head = t.get(..word.len())?;
    if !head.eq_ignore_ascii_case(word) {
        return None;
    }
    let rest = &t[word.len()..];
    (rest.is_empty() || rest.starts_with(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))).then(|| rest.trim_start())
}

fn rewrite_select(sql: &str, columns_of: &dyn Fn(&str) -> Option<Vec<String>>) -> Option<String> {
    let after_select = strip_word(sql, "SELECT")?;
    let (distinct, after_distinct) = match strip_word(after_select, "DISTINCT") {
        Some(rest) if strip_word(rest, "ON").is_some() => return None,
        Some(rest) => (true, rest),
        None => (false, after_select),
    };
    let list_at = sql.len() - after_distinct.len();
    let from_at = find_keyword(sql, "FROM", list_at)?;
    let list = &sql[list_at..from_at];
    for word in ["UNION", "INTERSECT", "EXCEPT", "FOR", "GROUP", "HAVING", "WINDOW"] {
        if find_keyword(sql, word, from_at).is_some() {
            return None;
        }
    }
    let order_at = find_phrase(sql, &["ORDER", "BY"], from_at);
    let tail_at = ["LIMIT", "OFFSET", "FETCH"]
        .iter()
        .filter_map(|w| find_keyword(sql, w, order_at.unwrap_or(from_at)))
        .min()
        .unwrap_or(sql.len());
    let body_end = order_at.unwrap_or(tail_at).min(tail_at);
    let body = sql[from_at + "FROM".len()..body_end].trim();
    let order_text = order_at.filter(|&at| at < tail_at).map(|at| {
        let after = &sql[at + "ORDER".len()..];
        let by = after.len() - after.trim_start().len() + "BY".len();
        &sql[at + "ORDER".len() + by..tail_at]
    });
    let tail = sql[tail_at..].trim();
    if find_keyword(list, "OVER", 0).is_none() && order_text.is_none_or(|o| find_keyword(o, "OVER", 0).is_none()) {
        return None;
    }

    // The source: a join, every relation of it a table with known columns.
    let source_end = find_keyword(body, "WHERE", 0).unwrap_or(body.len());
    let source = &body[..source_end];
    if find_keyword(source, "JOIN", 0).is_none() && !crate::core::rql::lexer_ct::split_definitions(source).len().gt(&1) {
        return None;
    }
    let relations = crate::core::rql::join::parse_from_clause(source.trim()).ok()?;
    let mut known: Vec<(String, Vec<String>)> = Vec::with_capacity(relations.len());
    for r in &relations {
        known.push((r.label.clone(), columns_of(&r.table)?));
    }

    let items = crate::core::rql::lexer_ct::split_definitions(list);
    if items.iter().any(|i| {
        let i = i.trim();
        i == "*" || i.ends_with(".*")
    }) {
        return None;
    }
    let mut refs: Vec<String> = Vec::new();
    let mut outer_items = Vec::with_capacity(items.len());
    let mut output_names: Vec<String> = Vec::with_capacity(items.len());
    for raw in &items {
        let raw = raw.trim();
        let (expr, alias) = split_alias(raw);
        let rewritten = replace_references(expr, &known, &mut refs)?;
        let name = match alias {
            Some(alias) => {
                output_names.push(alias.trim_matches('"').to_string());
                alias.to_string()
            }
            None => {
                // A window or filtered aggregate is named after its function
                // (`rank`, `sum`), as PostgreSQL's FigureColname does.
                let cut = ["OVER", "FILTER"].iter().filter_map(|w| find_keyword(expr, w, 0)).min().unwrap_or(expr.len());
                let name = pg_column_name(&expr[..cut]);
                output_names.push(name.clone());
                quote(&name)
            }
        };
        outer_items.push(format!("{rewritten} AS {name}"));
    }
    // A sort term that is an output name alone names the output column, as
    // in PostgreSQL, even where an input column has that name too.
    let order = match order_text {
        Some(text) => {
            let mut terms = Vec::new();
            for term in crate::core::rql::lexer_ct::split_definitions(text) {
                let term = term.trim();
                let head_end = term.find(char::is_whitespace).unwrap_or(term.len());
                let (head, modifiers) = term.split_at(head_end);
                let only_modifiers = modifiers.split_whitespace().all(|w| {
                    ["ASC", "DESC", "NULLS", "FIRST", "LAST"].iter().any(|m| w.eq_ignore_ascii_case(m))
                });
                let names_output = output_names.iter().any(|n| n.eq_ignore_ascii_case(head.trim_matches('"')));
                terms.push(if names_output && only_modifiers {
                    term.to_string()
                } else {
                    replace_references(term, &known, &mut refs)?
                });
            }
            Some(terms.join(", "))
        }
        None => None,
    };
    if refs.is_empty() {
        return None;
    }
    let inner: Vec<String> = refs.iter().enumerate().map(|(i, r)| format!("{r} AS __w{i}")).collect();
    let mut out = format!(
        "SELECT {}{} FROM (SELECT {} FROM {}) __w",
        if distinct { "DISTINCT " } else { "" },
        outer_items.join(", "),
        inner.join(", "),
        body
    );
    if let Some(order) = order {
        out.push_str(" ORDER BY ");
        out.push_str(order.trim());
    }
    if !tail.is_empty() {
        out.push(' ');
        out.push_str(tail);
    }
    Some(out)
}

/// `expr AS name` or `expr name` as `(expr, Some(name))`.
fn split_alias(item: &str) -> (&str, Option<&str>) {
    if let Some(at) = last_top_level_keyword(item, "AS") {
        let name = item[at + 2..].trim();
        if is_identifier(name) {
            return (item[..at].trim(), Some(name));
        }
    }
    (item, None)
}

fn last_top_level_keyword(text: &str, word: &str) -> Option<usize> {
    let mut found = None;
    let mut from = 0;
    while let Some(at) = find_keyword(text, word, from) {
        found = Some(at);
        from = at + word.len();
    }
    found
}

fn is_identifier(text: &str) -> bool {
    (text.starts_with('"') && text.ends_with('"') && text.len() > 2)
        || (text.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
            && text.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$'))
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `text` with every column reference replaced by the inner query's name for
/// it (`__wN`), adding new ones to `refs` as `label.column`. `None` for a
/// bare name two relations share (PostgreSQL's 42702, left to the native
/// path), or an unknown qualifier.
fn replace_references(text: &str, known: &[(String, Vec<String>)], refs: &mut Vec<String>) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len() + 16);
    let mut i = 0usize;
    let mut copied = 0usize;
    let mut after_as = false;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if bytes.get(i + 1) == Some(&b'\'') {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
                continue;
            }
            b':' if bytes.get(i + 1) == Some(&b':') => {
                // A cast's type name is not a column.
                i += 2;
                while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || matches!(bytes[i], b'_' | b'[' | b']')) {
                    i += 1;
                }
                continue;
            }
            b'"' | b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                if i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || matches!(bytes[i - 1], b'_' | b'$')) {
                    i += 1;
                    continue;
                }
                let start = i;
                let (first, end) = read_identifier(text, i)?;
                let (second, end) = if bytes.get(end) == Some(&b'.') {
                    match read_identifier(text, end + 1) {
                        Some((name, e)) => (Some(name), e),
                        None => (None, end),
                    }
                } else {
                    (None, end)
                };
                i = end;
                let next = text[end..].trim_start();
                if next.starts_with('(') && second.is_none() {
                    // A function call.
                    after_as = false;
                    continue;
                }
                let word_is_as = second.is_none() && first.eq_ignore_ascii_case("AS");
                if after_as {
                    after_as = word_is_as;
                    continue;
                }
                after_as = word_is_as;
                let reference = match &second {
                    Some(column) => {
                        let (label, columns) = known.iter().find(|(l, _)| l.eq_ignore_ascii_case(&first))?;
                        let column = columns.iter().find(|c| c.eq_ignore_ascii_case(column))?;
                        format!("{label}.{column}")
                    }
                    None => {
                        if text.as_bytes()[start] != b'"' && KEYWORDS.iter().any(|k| first.eq_ignore_ascii_case(k)) {
                            continue;
                        }
                        let owners: Vec<(&String, &String)> = known
                            .iter()
                            .filter_map(|(l, cols)| cols.iter().find(|c| c.eq_ignore_ascii_case(&first)).map(|c| (l, c)))
                            .collect();
                        match owners.as_slice() {
                            [] => continue,
                            [(label, column)] => format!("{label}.{column}"),
                            _ => return None,
                        }
                    }
                };
                let at = refs.iter().position(|r| r.eq_ignore_ascii_case(&reference)).unwrap_or_else(|| {
                    refs.push(reference);
                    refs.len() - 1
                });
                out.push_str(&text[copied..start]);
                out.push_str(&format!("__w{at}"));
                copied = end;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    out.push_str(&text[copied..]);
    Some(out)
}

/// One identifier at `at`, bare or double-quoted: its name and its end.
fn read_identifier(text: &str, at: usize) -> Option<(String, usize)> {
    let bytes = text.as_bytes();
    if bytes.get(at) == Some(&b'"') {
        let close = at + 1 + text[at + 1..].find('"')?;
        return Some((text[at + 1..close].to_string(), close + 1));
    }
    let mut end = at;
    while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || matches!(bytes[end], b'_' | b'$')) {
        end += 1;
    }
    (end > at).then(|| (text[at..end].to_string(), end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns(table: &str) -> Option<Vec<String>> {
        match table {
            "a" => Some(vec!["id".into(), "g".into(), "first".into()]),
            "b" => Some(vec!["id".into(), "w".into()]),
            _ => None,
        }
    }

    #[test]
    fn a_window_over_a_join_reads_a_derived_table() {
        let got = rewrite_statement(
            "SELECT a.id, b.w, row_number() OVER (PARTITION BY a.id ORDER BY b.w) AS rn FROM a JOIN b ON a.id = b.id ORDER BY a.id, b.w",
            &columns,
        )
        .unwrap();
        assert_eq!(
            got,
            "SELECT __w0 AS \"id\", __w1 AS \"w\", row_number() OVER (PARTITION BY __w0 ORDER BY __w1) AS rn \
             FROM (SELECT a.id AS __w0, b.w AS __w1 FROM a JOIN b ON a.id = b.id) __w ORDER BY __w0, __w1"
        );
        // A bare column one relation has; keywords and casts left alone.
        let got = rewrite_statement(
            "SELECT g, sum(w) OVER (PARTITION BY g ORDER BY w ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::int AS s \
             FROM a, b WHERE a.id = b.id LIMIT 3",
            &columns,
        )
        .unwrap();
        assert_eq!(
            got,
            "SELECT __w0 AS \"g\", sum(__w1) OVER (PARTITION BY __w0 ORDER BY __w1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::int AS s \
             FROM (SELECT a.g AS __w0, b.w AS __w1 FROM a, b WHERE a.id = b.id) __w LIMIT 3"
        );
        // `NULLS FIRST` is not the column `first`.
        let got = rewrite_statement(
            "SELECT a.first, rank() OVER (ORDER BY b.w DESC NULLS FIRST) FROM a JOIN b ON a.id = b.id",
            &columns,
        )
        .unwrap();
        assert!(got.contains("ORDER BY __w1 DESC NULLS FIRST"), "{got}");
        assert!(got.contains("AS \"rank\""), "{got}");
        // An output name sorts by the output column, not the input `g`.
        let got = rewrite_statement(
            "SELECT b.w AS g, rank() OVER (ORDER BY a.g) FROM a JOIN b ON a.id = b.id ORDER BY g DESC",
            &columns,
        )
        .unwrap();
        assert!(got.ends_with("ORDER BY g DESC"), "{got}");
    }

    #[test]
    fn what_it_cannot_rewrite_faithfully_is_left_alone() {
        for sql in [
            "SELECT id, rank() OVER (ORDER BY w) FROM a JOIN b ON a.id = b.id",
            "SELECT a.id FROM a JOIN b ON a.id = b.id",
            "SELECT rank() OVER (ORDER BY id) FROM a",
            "SELECT *, rank() OVER (ORDER BY w) FROM a JOIN b ON a.id = b.id",
            "SELECT a.id, rank() OVER (ORDER BY b.w) FROM a JOIN c ON a.id = c.id",
            "SELECT a.g, count(*) OVER () FROM a JOIN b ON a.id = b.id GROUP BY a.g",
            "WITH x AS (SELECT 1) SELECT rank() OVER (ORDER BY b.w) FROM a JOIN b ON a.id = b.id",
        ] {
            assert!(rewrite_statement(sql, &columns).is_none(), "{sql}");
        }
    }
}
