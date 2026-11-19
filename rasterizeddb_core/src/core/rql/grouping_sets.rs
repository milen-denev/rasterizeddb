//! `GROUP BY ROLLUP (…)`, `CUBE (…)` and `GROUPING SETS (…)`, as the
//! `UNION ALL` of one grouped query per grouping set they mean.
//!
//! `GROUP BY ROLLUP (a, b)` groups by `(a, b)`, by `(a)` and by nothing, and
//! the rows of each come out together; a key a set does not group by reads
//! NULL in that set's rows, and `GROUPING(a, …)` is the bitmask of the keys
//! that are not grouped. Each was "GROUP BY column 'rollup (k)' not found".
//!
//! Each branch is the statement grouped by its set: a key it does not group
//! by is written `max(CASE WHEN FALSE THEN key END)`, NULL of the key's own
//! type (so the branches' columns agree) and an aggregate (so it may stand
//! in a grouped select list); `GROUPING(…)` is the branch's constant. The
//! `WHERE` and `HAVING` stay with every branch; `ORDER BY`, `LIMIT` and
//! `OFFSET` apply to the whole, as PostgreSQL applies them.

use crate::core::rql::join::find_keyword;

/// `(sqlstate, message)`.
pub(crate) type Refusal = (&'static str, String);

/// At most this many sets: `CUBE` over ten keys is 1,024 branches.
const MAX_SETS: usize = 256;

fn normalize(expr: &str) -> String {
    expr.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

/// The text inside the parentheses opening at `open`, and the index past
/// the closing one.
fn parenthesised(text: &str, open: usize) -> Option<(&str, usize)> {
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut i = open;
    while i < bytes.len() {
        if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some((&text[open + 1..i], i + 1));
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// A set's items: `(a, b)` as `[a, b]`, `()` as none, `a` as `[a]`.
fn set_items(text: &str) -> Vec<String> {
    let t = text.trim();
    let inner = if t.starts_with('(') && parenthesised(t, 0).is_some_and(|(_, end)| end == t.len())
    {
        &t[1..t.len() - 1]
    } else {
        t
    };
    crate::core::sql_scan::split_top_level(inner, b',')
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// The grouping sets one element of a `GROUP BY` list stands for.
fn element_sets(element: &str) -> Result<Vec<Vec<String>>, Refusal> {
    let e = element.trim();
    let upper = e.to_ascii_uppercase();
    let call = |word: &str| -> Option<&str> {
        let rest = upper.strip_prefix(word)?;
        let skipped = rest.len() - rest.trim_start().len();
        let open = word.len() + skipped;
        (e.as_bytes().get(open) == Some(&b'('))
            .then(|| parenthesised(e, open))
            .flatten()
            .filter(|(_, end)| *end == e.len())
            .map(|(inner, _)| inner)
    };
    if let Some(inner) = call("ROLLUP") {
        let items: Vec<String> = crate::core::sql_scan::split_top_level(inner, b',')
            .into_iter()
            .map(|s| s.trim().to_string())
            .collect();
        // `ROLLUP (a, (b, c))`: each item may itself be a composite.
        let groups: Vec<Vec<String>> = items.iter().map(|i| set_items(i)).collect();
        return Ok((0..=groups.len())
            .rev()
            .map(|n| groups[..n].concat())
            .collect());
    }
    if let Some(inner) = call("CUBE") {
        let groups: Vec<Vec<String>> = crate::core::sql_scan::split_top_level(inner, b',')
            .into_iter()
            .map(|s| set_items(s.trim()))
            .collect();
        if groups.len() > 8 {
            return Err(("54000", "CUBE is limited to 8 elements here".to_string()));
        }
        let n = groups.len();
        // Largest sets first, as PostgreSQL orders them.
        let mut masks: Vec<u32> = (0..(1u32 << n)).collect();
        masks.sort_by_key(|m| std::cmp::Reverse(m.count_ones()));
        return Ok(masks
            .into_iter()
            .map(|m| {
                (0..n)
                    .filter(|i| m & (1 << (n - 1 - i)) != 0)
                    .flat_map(|i| groups[i].clone())
                    .collect()
            })
            .collect());
    }
    if upper.starts_with("GROUPING") && upper["GROUPING".len()..].trim_start().starts_with("SETS") {
        let at = e
            .find('(')
            .ok_or(("42601", "GROUPING SETS needs a list".to_string()))?;
        let (inner, _) =
            parenthesised(e, at).ok_or(("42601", "GROUPING SETS is not closed".to_string()))?;
        let mut out = Vec::new();
        for item in crate::core::sql_scan::split_top_level(inner, b',') {
            let item = item.trim();
            let u = item.to_ascii_uppercase();
            if u.starts_with("ROLLUP") || u.starts_with("CUBE") || u.starts_with("GROUPING") {
                out.extend(element_sets(item)?);
            } else {
                out.push(set_items(item));
            }
        }
        return Ok(out);
    }
    Ok(vec![vec![e.to_string()]])
}

/// The statement as the `UNION ALL` of its grouping sets' queries; `None`
/// when its `GROUP BY` has none of them.
pub(crate) fn rewrite_statement(sql: &str) -> Option<Result<String, Refusal>> {
    let has = |w: &[u8]| crate::core::sql_scan::contains_ignore_ascii_case(sql, w);
    if !(has(b"rollup") || has(b"cube") || has(b"grouping")) {
        return None;
    }
    let t = sql.trim().trim_end_matches(';').trim_end();
    if !t.get(..6).is_some_and(|w| w.eq_ignore_ascii_case("SELECT"))
        || find_keyword(t, "UNION", 0).is_some()
    {
        return None;
    }
    let group = find_keyword(t, "GROUP BY", 0)?;
    let after_group = &t[group + "GROUP BY".len()..];
    let group_end = [
        "HAVING", "ORDER", "LIMIT", "OFFSET", "WINDOW", "FETCH", "FOR",
    ]
    .iter()
    .filter_map(|k| find_keyword(after_group, k, 0))
    .min()
    .unwrap_or(after_group.len());
    let spec = after_group[..group_end].trim();
    let elements: Vec<&str> = crate::core::sql_scan::split_top_level(spec, b',');
    let special = |e: &str| {
        let u = e.trim().to_ascii_uppercase();
        (u.starts_with("ROLLUP") || u.starts_with("CUBE")) && u.contains('(')
            || u.starts_with("GROUPING") && u.contains("SETS")
    };
    if !elements.iter().any(|e| special(e)) {
        return None;
    }
    // The cross product of every element's sets.
    let mut sets: Vec<Vec<String>> = vec![Vec::new()];
    for element in &elements {
        let options = match element_sets(element) {
            Ok(o) => o,
            Err(e) => return Some(Err(e)),
        };
        let mut next = Vec::with_capacity(sets.len() * options.len());
        for base in &sets {
            for option in &options {
                let mut set = base.clone();
                for item in option {
                    if !set.iter().any(|s| normalize(s) == normalize(item)) {
                        set.push(item.clone());
                    }
                }
                next.push(set);
            }
        }
        if next.len() > MAX_SETS {
            return Some(Err((
                "54000",
                format!("more than {MAX_SETS} grouping sets"),
            )));
        }
        sets = next;
    }
    // Every key any set names.
    let mut keys: Vec<String> = Vec::new();
    for set in &sets {
        for item in set {
            if !keys.iter().any(|k| normalize(k) == normalize(item)) {
                keys.push(item.clone());
            }
        }
    }
    let rest = &after_group[group_end..];
    let having_end = ["ORDER", "LIMIT", "OFFSET", "FETCH", "FOR", "WINDOW"]
        .iter()
        .filter_map(|k| find_keyword(rest, k, 0))
        .min()
        .unwrap_or(rest.len());
    let having = rest[..having_end].trim();
    let tail = rest[having_end..].trim();
    let from = match find_keyword(t, "FROM", 0) {
        Some(f) if f < group => f,
        _ => return None,
    };
    let list = &t[6..from];
    let body = t[from..group].trim();
    let items: Vec<&str> = crate::core::sql_scan::split_top_level(list, b',');
    let mut branches = Vec::with_capacity(sets.len());
    for set in &sets {
        let grouped = |e: &str| set.iter().any(|s| normalize(s) == normalize(e));
        let rewrite = |text: &str| -> String {
            let mut out = text.to_string();
            // `GROUPING(a, b)`: bit set for each key this set does not group.
            while let Some(at) = find_keyword(&out, "GROUPING", 0) {
                let open = out[at + 8..].find('(').map(|p| at + 8 + p);
                let Some((inner, end)) = open.and_then(|o| parenthesised(&out, o)) else {
                    break;
                };
                let args: Vec<String> = crate::core::sql_scan::split_top_level(inner, b',')
                    .into_iter()
                    .map(|a| a.trim().to_string())
                    .collect();
                let mut bits = 0u32;
                for a in &args {
                    bits = (bits << 1) | u32::from(!grouped(a));
                }
                out = format!("{}{}{}", &out[..at], bits, &out[end..]);
            }
            out
        };
        let selected: Vec<String> = items
            .iter()
            .map(|item| {
                let (expr, alias) = crate::core::rql::lexer_query::split_alias(item.trim());
                let is_key = keys.iter().any(|k| normalize(k) == normalize(expr));
                if is_key && !grouped(expr) {
                    let name = alias
                        .clone()
                        .unwrap_or_else(|| crate::core::rql::join::pg_column_name(expr));
                    format!(
                        "max(CASE WHEN FALSE THEN {} END) AS {}",
                        expr.trim(),
                        crate::core::rql::views::quote_ident(&name)
                    )
                } else {
                    rewrite(item.trim())
                }
            })
            .collect();
        let mut branch = format!("SELECT {} {}", selected.join(", "), body);
        if !set.is_empty() {
            branch.push_str(&format!(" GROUP BY {}", set.join(", ")));
        }
        if !having.is_empty() {
            branch.push(' ');
            branch.push_str(&rewrite(having));
        }
        branches.push(branch);
    }
    let mut out = branches.join(" UNION ALL ");
    if !tail.is_empty() {
        out.push(' ');
        out.push_str(tail);
    }
    Some(Ok(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rw(sql: &str) -> String {
        rewrite_statement(sql).unwrap().unwrap()
    }

    #[test]
    fn rollup_is_its_prefixes() {
        assert_eq!(
            rw("SELECT k, count(*) FROM sa GROUP BY ROLLUP (k) ORDER BY k"),
            "SELECT k, count(*) FROM sa GROUP BY k UNION ALL \
             SELECT max(CASE WHEN FALSE THEN k END) AS \"k\", count(*) FROM sa ORDER BY k"
        );
        let two = rw("SELECT a, b, sum(n) FROM t GROUP BY ROLLUP (a, b)");
        assert_eq!(two.matches("UNION ALL").count(), 2);
        assert!(two.contains("GROUP BY a, b UNION ALL"), "{two}");
    }

    #[test]
    fn cube_and_sets_and_grouping() {
        let c = rw("SELECT a, b, GROUPING(a, b) FROM t GROUP BY CUBE (a, b)");
        assert_eq!(c.matches("UNION ALL").count(), 3);
        // The set grouping by `b` alone marks `a` not grouped: bitmask 2.
        assert!(
            c.contains("max(CASE WHEN FALSE THEN a END) AS \"a\", b, 2 FROM t GROUP BY b"),
            "{c}"
        );
        let g = rw("SELECT k, v, count(*) FROM sa GROUP BY GROUPING SETS ((k), (v), ())");
        assert_eq!(g.matches("UNION ALL").count(), 2);
        assert!(g.ends_with("count(*) FROM sa"), "{g}");
        // A plain key crossed with a rollup.
        let m = rw(
            "SELECT a, b, count(*) FROM t WHERE n > 0 GROUP BY a, ROLLUP (b) HAVING count(*) > 1",
        );
        assert_eq!(
            m,
            "SELECT a, b, count(*) FROM t WHERE n > 0 GROUP BY a, b HAVING count(*) > 1 UNION ALL \
                       SELECT a, max(CASE WHEN FALSE THEN b END) AS \"b\", count(*) FROM t WHERE n > 0 GROUP BY a HAVING count(*) > 1"
        );
        assert!(rewrite_statement("SELECT k FROM t GROUP BY k").is_none());
    }
}
