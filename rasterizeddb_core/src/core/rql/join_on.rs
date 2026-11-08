//! A join condition whose operand is an expression, answered by the
//! column-to-column hash join the planner already has.
//!
//! The planner joins on equalities between two columns. An operand that is an
//! expression over one relation (`b.id::text`, `a.n + 1`, `lower(b.v)`) is
//! computed by a derived table standing in for that relation, so
//!
//! ```sql
//! SELECT * FROM t a JOIN u b ON a.g = b.id::text
//! ```
//!
//! is answered as
//!
//! ```sql
//! SELECT a."id", a."g", b."id", b."v" FROM t a
//!   JOIN (SELECT "id", "v", b.id::text AS __j0 FROM u b) b ON a.g = b.__j0
//! ```
//!
//! The derived table has the relation's rows and columns plus the computed
//! one, under the relation's own label, so every other reference reads as it
//! did. A `*` or `b.*` is expanded so the computed column is not shown. It was
//! refused ("column 'id::text' not found in relation 'b'").
//!
//! Left alone, keeping what it did before: an `OR`, an operand reading two
//! relations, a relation whose columns are not known here (a CTE, a derived
//! table), a comma-separated `FROM`, and a statement other than `SELECT`.

use crate::core::rql::join::{find_keyword, parse_from_clause, split_conjuncts};
use crate::core::rql::window_join::{is_bare_reference, matching_paren, replace_references};

const CLAUSE_ENDS: &[&str] = &[
    "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "FETCH", "FOR", "WINDOW", "UNION", "INTERSECT", "EXCEPT",
];

const JOIN_WORDS: &[&str] = &["LEFT", "RIGHT", "FULL", "INNER", "CROSS", "OUTER", "NATURAL"];

/// The rewritten statement, or `None` when `sql` has no join operand to
/// compute or cannot be rewritten faithfully.
pub(crate) fn rewrite_statement(sql: &str, columns_of: &dyn Fn(&str) -> Option<Vec<String>>) -> Option<String> {
    if !sql.to_ascii_uppercase().contains("JOIN") {
        return None;
    }
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    if crate::core::rql::cte::has_with_clause(trimmed) {
        return None;
    }
    rewrite_select(trimmed, columns_of)
}

fn starts_with_word(text: &str, word: &str) -> bool {
    let t = text.trim_start();
    t.len() >= word.len()
        && t[..word.len()].eq_ignore_ascii_case(word)
        && !t[word.len()..].starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_')
}

fn rewrite_select(sql: &str, columns_of: &dyn Fn(&str) -> Option<Vec<String>>) -> Option<String> {
    if !starts_with_word(sql, "SELECT") {
        return None;
    }
    let lead = sql.len() - sql.trim_start().len();
    let list_at = lead + "SELECT".len();
    let from_at = find_keyword(sql, "FROM", list_at)?;
    let source_at = from_at + "FROM".len();
    let source_end = CLAUSE_ENDS
        .iter()
        .filter_map(|w| find_keyword(sql, w, source_at))
        .min()
        .unwrap_or(sql.len());
    let source = &sql[source_at..source_end];

    // A `FROM` that is one derived table (a window over a join, rewritten
    // first): its own `SELECT` is where the join is.
    if find_keyword(source, "JOIN", 0).is_none() {
        let t = source.trim_start();
        if !t.starts_with('(') {
            return None;
        }
        let open = source.len() - t.len();
        let close = matching_paren(source, open)?;
        let inner = rewrite_select(&source[open + 1..close], columns_of)?;
        return Some(format!(
            "{}{}{}{}",
            &sql[..source_at],
            &source[..open + 1],
            inner,
            &sql[source_at + close..]
        ));
    }
    if crate::core::rql::lexer_ct::split_definitions(source).len() > 1 {
        return None;
    }

    // The relations, in order, and where each one's text and `ON` are.
    let relations = parse_from_clause(source.trim()).ok()?;
    let segments = segments(source)?;
    if segments.len() != relations.len() {
        return None;
    }
    let known: Vec<(String, Option<Vec<String>>)> =
        relations.iter().map(|r| (r.label.clone(), columns_of(&r.table))).collect();
    let all_known: Vec<(String, Vec<String>)> =
        known.iter().filter_map(|(l, c)| c.clone().map(|c| (l.clone(), c))).collect();

    // Each relation's computed operands, and each ON condition rewritten.
    let mut computed: Vec<Vec<String>> = vec![Vec::new(); relations.len()];
    let mut conditions: Vec<Option<String>> = vec![None; relations.len()];
    let mut next = 0usize;
    for (i, segment) in segments.iter().enumerate() {
        let Some((_, on)) = segment.on else { continue };
        let condition = &source[on.0..on.1];
        if find_keyword(condition, "OR", 0).is_some() {
            continue;
        }
        let mut terms = Vec::new();
        let mut changed = false;
        for term in split_conjuncts(condition) {
            let term = term.trim();
            let Some(eq) = top_level_equals(term) else {
                terms.push(term.to_string());
                continue;
            };
            let sides = [term[..eq].trim(), term[eq + 1..].trim()];
            let owners = sides.map(|side| owner(side, &all_known));
            let mut rewritten = [sides[0].to_string(), sides[1].to_string()];
            for k in 0..2 {
                let (Some(rel), Some(other)) = (owners[k], owners[1 - k]) else { continue };
                if rel == other || is_bare_reference(sides[k]) || known[rel].1.is_none() {
                    continue;
                }
                // Not a name a relation already has: one materialised from an
                // earlier rewrite carries its `__j0` (`lateral`, two items
                // keyed on one outer relation).
                let taken = |n: &str| all_known.iter().any(|(_, cols)| cols.iter().any(|c| c.eq_ignore_ascii_case(n)));
                let name = loop {
                    let candidate = format!("__j{next}");
                    next += 1;
                    if !taken(&candidate) {
                        break candidate;
                    }
                };
                computed[rel].push(format!("{} AS {name}", sides[k]));
                rewritten[k] = format!("{}.{name}", relations[rel].label);
                changed = true;
            }
            terms.push(format!("{} = {}", rewritten[0], rewritten[1]));
        }
        if changed {
            conditions[i] = Some(terms.join(" AND "));
        }
    }
    if computed.iter().all(|c| c.is_empty()) {
        return None;
    }

    // The source, each relation with computed operands as a derived table.
    let mut out_source = String::with_capacity(source.len() + 64);
    let mut copied = 0usize;
    for (i, segment) in segments.iter().enumerate() {
        if !computed[i].is_empty() {
            let columns = known[i].1.as_ref()?;
            let mut items: Vec<String> = columns.iter().map(|c| quote(c)).collect();
            items.extend(computed[i].iter().cloned());
            let span = &source[segment.relation.0..segment.relation.1];
            out_source.push_str(&source[copied..segment.relation.0]);
            out_source.push_str(&span[..span.len() - span.trim_start().len()]);
            out_source.push_str(&format!("(SELECT {} FROM {}) {}", items.join(", "), span.trim(), relations[i].label));
            out_source.push_str(&span[span.trim_end().len()..]);
            copied = segment.relation.1;
        }
        if let (Some(condition), Some((_, on))) = (&conditions[i], segment.on) {
            let span = &source[on.0..on.1];
            out_source.push_str(&source[copied..on.0]);
            out_source.push_str(&span[..span.len() - span.trim_start().len()]);
            out_source.push_str(condition);
            out_source.push_str(&span[span.trim_end().len()..]);
            copied = on.1;
        }
    }
    out_source.push_str(&source[copied..]);

    // `*` and `x.*` without the computed columns.
    let list = &sql[list_at..from_at];
    let items = crate::core::rql::lexer_ct::split_definitions(list);
    let mut out_items = Vec::with_capacity(items.len());
    for raw in &items {
        let raw = raw.trim();
        let qualifier = if raw == "*" {
            Some(None)
        } else {
            raw.strip_suffix(".*").map(|q| Some(q.trim().trim_matches('"')))
        };
        let Some(qualifier) = qualifier else {
            out_items.push(raw.to_string());
            continue;
        };
        let mut expanded = Vec::new();
        for (i, (label, columns)) in known.iter().enumerate() {
            if qualifier.is_some_and(|q| !q.eq_ignore_ascii_case(label)) {
                continue;
            }
            if computed[i].is_empty() && qualifier.is_some() {
                expanded.push(raw.to_string());
                continue;
            }
            for c in columns.as_ref()? {
                expanded.push(format!("{label}.{}", quote(c)));
            }
        }
        if expanded.is_empty() {
            return None;
        }
        out_items.push(expanded.join(", "));
    }
    Some(format!(
        "{} {} FROM {}{}{}",
        &sql[..list_at],
        out_items.join(", "),
        out_source.trim(),
        if source_end < sql.len() { " " } else { "" },
        sql[source_end..].trim_start()
    ))
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The one relation `side` reads, by index into `known`'s labels; `None` for
/// none, two or more, or a reference it cannot place.
fn owner(side: &str, known: &[(String, Vec<String>)]) -> Option<usize> {
    let mut refs = Vec::new();
    replace_references(side, known, &mut refs)?;
    let mut owner: Option<usize> = None;
    for r in &refs {
        let label = r.rsplit_once('.').map(|(l, _)| l)?;
        let at = known.iter().position(|(l, _)| l.eq_ignore_ascii_case(label))?;
        if owner.is_some_and(|o| o != at) {
            return None;
        }
        owner = Some(at);
    }
    owner
}

/// The position of a top-level `=` that is the equality operator itself.
fn top_level_equals(term: &str) -> Option<usize> {
    let bytes = term.as_bytes();
    let mut depth = 0usize;
    let mut quote: Option<u8> = None;
    let mut found = None;
    for (i, &c) in bytes.iter().enumerate() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None => match c {
                b'\'' | b'"' => quote = Some(c),
                b'(' => depth += 1,
                b')' => depth = depth.saturating_sub(1),
                b'=' if depth == 0 => {
                    let before = i.checked_sub(1).map(|p| bytes[p]);
                    let after = bytes.get(i + 1).copied();
                    if matches!(before, Some(b'<' | b'>' | b'!' | b'=')) || matches!(after, Some(b'=' | b'>')) {
                        return None;
                    }
                    if found.is_some() {
                        return None;
                    }
                    found = Some(i);
                }
                b'<' | b'>' if depth == 0 && bytes.get(i + 1) != Some(&b'=') => return None,
                _ => {}
            },
        }
    }
    found
}

/// Where one relation of a `FROM` is: its own text, and its `ON` condition
/// (the keyword's position and the condition's span).
struct Segment {
    relation: (usize, usize),
    on: Option<(usize, (usize, usize))>,
}

/// The relations of a join source, in order. `None` for a shape this does
/// not read (`USING`, `NATURAL`).
fn segments(source: &str) -> Option<Vec<Segment>> {
    // Each JOIN keyword and where its phrase (`LEFT OUTER JOIN`) begins.
    let mut joins: Vec<(usize, usize)> = Vec::new();
    let mut from = 0;
    while let Some(at) = find_keyword(source, "JOIN", from) {
        let mut start = at;
        loop {
            let head = source[..start].trim_end();
            let word_at = head.rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).map_or(0, |p| p + 1);
            let word = &head[word_at..];
            if JOIN_WORDS.iter().any(|w| word.eq_ignore_ascii_case(w)) {
                if word.eq_ignore_ascii_case("NATURAL") {
                    return None;
                }
                start = word_at;
            } else {
                break;
            }
        }
        joins.push((start, at + "JOIN".len()));
        from = at + "JOIN".len();
    }
    let mut out = Vec::with_capacity(joins.len() + 1);
    out.push(Segment { relation: (0, joins.first()?.0), on: None });
    for (k, &(_, body_at)) in joins.iter().enumerate() {
        let end = joins.get(k + 1).map_or(source.len(), |j| j.0);
        let body = &source[body_at..end];
        if find_keyword(body, "USING", 0).is_some() {
            return None;
        }
        match find_keyword(body, "ON", 0) {
            Some(on) => out.push(Segment {
                relation: (body_at, body_at + on),
                on: Some((body_at + on, (body_at + on + "ON".len(), end))),
            }),
            None => out.push(Segment { relation: (body_at, end), on: None }),
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns(table: &str) -> Option<Vec<String>> {
        match table {
            "t" => Some(vec!["id".into(), "g".into(), "n".into()]),
            "u" => Some(vec!["id".into(), "v".into()]),
            "w" => Some(vec!["k".into()]),
            _ => None,
        }
    }

    #[test]
    fn an_expression_operand_is_a_derived_table_column() {
        assert_eq!(
            rewrite_statement("SELECT a.id, b.id FROM t a JOIN u b ON a.g = b.id::text ORDER BY 1, 2", &columns).unwrap(),
            "SELECT a.id, b.id FROM t a JOIN (SELECT \"id\", \"v\", b.id::text AS __j0 FROM u b) b ON a.g = b.__j0 ORDER BY 1, 2"
        );
        // The left relation, an outer join, and a second key with a filter.
        assert_eq!(
            rewrite_statement(
                "SELECT * FROM t a LEFT JOIN u b ON a.n + 1 = b.id AND a.g = lower(b.v) AND b.v <> 'z' WHERE a.id > 1",
                &columns
            )
            .unwrap(),
            "SELECT a.\"id\", a.\"g\", a.\"n\", b.\"id\", b.\"v\" FROM (SELECT \"id\", \"g\", \"n\", a.n + 1 AS __j0 FROM t a) a \
             LEFT JOIN (SELECT \"id\", \"v\", lower(b.v) AS __j1 FROM u b) b ON a.__j0 = b.id AND a.g = b.__j1 AND b.v <> 'z' WHERE a.id > 1"
        );
        // `b.*` loses the computed column; `a.*` is left as written.
        assert_eq!(
            rewrite_statement("SELECT a.*, b.* FROM t a JOIN u b ON a.id = CAST(b.v AS int)", &columns).unwrap(),
            "SELECT a.*, b.\"id\", b.\"v\" FROM t a JOIN (SELECT \"id\", \"v\", CAST(b.v AS int) AS __j0 FROM u b) b ON a.id = b.__j0"
        );
        // A third relation after the rewritten one keeps its condition.
        assert_eq!(
            rewrite_statement("SELECT a.id FROM t a JOIN u b ON a.g = b.id::text JOIN w ON w.k = a.id", &columns).unwrap(),
            "SELECT a.id FROM t a JOIN (SELECT \"id\", \"v\", b.id::text AS __j0 FROM u b) b ON a.g = b.__j0 JOIN w ON w.k = a.id"
        );
    }

    #[test]
    fn what_it_cannot_rewrite_faithfully_is_left_alone() {
        for sql in [
            "SELECT a.id FROM t a JOIN u b ON a.id = b.id",
            "SELECT a.id FROM t a JOIN u b ON a.id = b.id::int OR a.g = b.v",
            "SELECT a.id FROM t a JOIN u b ON a.n + b.id = 3",
            "SELECT a.id FROM t a JOIN x b ON a.g = b.id::text",
            "SELECT a.id FROM t a JOIN u b USING (id)",
            "SELECT a.id FROM t a JOIN u b ON a.id < b.id + 1",
            "SELECT a.id FROM t a JOIN u b ON b.id::text = '3' AND a.id = b.id",
            "WITH c AS (SELECT 1) SELECT a.id FROM t a JOIN u b ON a.g = b.id::text",
            "UPDATE t SET n = 1 FROM u b WHERE t.g = b.id::text",
        ] {
            assert!(rewrite_statement(sql, &columns).is_none(), "{sql}");
        }
    }
}
