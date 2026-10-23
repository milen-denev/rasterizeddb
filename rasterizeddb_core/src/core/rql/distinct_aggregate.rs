//! `COUNT(DISTINCT e)` and the other aggregates over distinct values.
//!
//! The aggregate fold has no notion of DISTINCT: `COUNT(DISTINCT x)` was read
//! as a column named `DISTINCT x` and refused as 42601. A statement whose every
//! aggregate is over the distinct values of one expression is rewritten into
//! the plain form over a derived table that has already removed duplicates,
//! which the engine answers exactly as PostgreSQL does, NULLs included:
//!
//! ```text
//! SELECT g, COUNT(DISTINCT x) FROM t WHERE w GROUP BY g ORDER BY g
//! SELECT g AS g, COUNT(__distinct_value) AS count
//!   FROM (SELECT DISTINCT g AS g, x AS __distinct_value FROM t WHERE w) __distinct
//!   GROUP BY g ORDER BY g
//! ```
//!
//! `COUNT` skips the NULL that `DISTINCT` keeps once, and `SUM`, `AVG`, `MIN`
//! and `MAX` skip it anyway. A shape the rewrite cannot express (a DISTINCT
//! aggregate beside a plain one, over two different expressions, inside a
//! larger expression, or with `HAVING`) is refused as unsupported rather than
//! answered wrongly.

use crate::core::rql::join::find_keyword;

const VALUE: &str = "__distinct_value";
const AGGREGATES: [&str; 5] = ["COUNT", "SUM", "AVG", "MIN", "MAX"];

/// The rewritten statement, `Ok(None)` when `sql` has no aggregate over
/// DISTINCT values, or an error naming the shape that is not supported.
pub fn rewrite(sql: &str) -> Result<Option<String>, String> {
    let sql = sql.trim().trim_end_matches(';').trim_end();
    if !starts_with_word(sql, "SELECT") || !mentions_distinct_aggregate(sql) {
        return Ok(None);
    }
    let Some(from_at) = find_keyword(sql, "FROM", 0) else {
        return Ok(None);
    };
    let items_text = sql["SELECT".len()..from_at].trim();
    if starts_with_word(items_text, "DISTINCT") || starts_with_word(items_text, "ALL") {
        return Ok(None);
    }

    let mut inner_value: Option<String> = None;
    let mut items: Vec<Item> = Vec::new();
    for raw in split_top_level(items_text) {
        let (expression, alias) = split_alias(raw.trim());
        match distinct_call(expression) {
            Some((function, value)) => {
                match &inner_value {
                    Some(seen) if normalized(seen) != normalized(value) => {
                        return Err(unsupported("aggregates over the DISTINCT values of different expressions"));
                    }
                    _ => inner_value = Some(value.to_string()),
                }
                items.push(Item::Aggregate {
                    function,
                    original: expression.to_string(),
                    alias: alias.map(str::to_string),
                });
            }
            None => {
                if has_word(expression, "DISTINCT") {
                    return Err(unsupported("an aggregate over DISTINCT values inside a larger expression"));
                }
                if AGGREGATES.iter().any(|a| calls(expression, a)) {
                    return Err(unsupported("an aggregate over DISTINCT values beside another aggregate"));
                }
                items.push(Item::Key {
                    expression: expression.to_string(),
                    alias: alias.map(str::to_string),
                });
            }
        }
    }
    let Some(value) = inner_value else {
        return Ok(None);
    };

    let tail = &sql[from_at + "FROM".len()..];
    for word in ["HAVING", "WINDOW", "UNION", "INTERSECT", "EXCEPT"] {
        if find_keyword(tail, word, 0).is_some() {
            return Err(unsupported(&format!("an aggregate over DISTINCT values with {}", word)));
        }
    }
    let group_at = clause(tail, "GROUP");
    let order_at = clause(tail, "ORDER");
    let rest_at = ["LIMIT", "OFFSET", "FETCH"]
        .iter()
        .filter_map(|w| find_keyword(tail, w, 0))
        .min();
    let from_end = [group_at, order_at, rest_at].into_iter().flatten().min().unwrap_or(tail.len());
    let source = tail[..from_end].trim();
    let group_text = group_at.map(|at| {
        let end = [order_at, rest_at].into_iter().flatten().filter(|e| *e > at).min().unwrap_or(tail.len());
        // Past `GROUP` and its `BY`.
        let after = tail[at + "GROUP".len()..end].trim_start();
        after.get(2..).unwrap_or("").trim().to_string()
    });
    let ordering = [order_at, rest_at].into_iter().flatten().min().map(|at| tail[at..].trim()).unwrap_or("");

    // Every grouping key is a column reference, carried through the derived
    // table under its own bare name.
    let keys: Vec<String> = group_text
        .as_deref()
        .map(|g| split_top_level(g).into_iter().map(|k| k.trim().to_string()).collect())
        .unwrap_or_default();
    let mut bare_keys: Vec<String> = Vec::new();
    for key in &keys {
        let Some(bare) = bare_column(key) else {
            return Err(unsupported("an aggregate over DISTINCT values grouped by an expression"));
        };
        if bare_keys.iter().any(|b| b.eq_ignore_ascii_case(&bare)) {
            return Err(unsupported("an aggregate over DISTINCT values grouped by two columns of one name"));
        }
        bare_keys.push(bare);
    }
    let bare_of = |expression: &str| -> Option<String> {
        keys.iter()
            .position(|k| normalized(k) == normalized(expression))
            .map(|i| bare_keys[i].clone())
    };

    let mut outer_items: Vec<String> = Vec::new();
    let mut ordering = ordering.to_string();
    for item in &items {
        match item {
            Item::Aggregate { function, original, alias } => {
                let replacement = format!("{}({})", function, VALUE);
                ordering = ordering.replace(original.as_str(), &replacement);
                outer_items.push(format!(
                    "{} AS {}",
                    replacement,
                    alias.clone().unwrap_or_else(|| function.to_ascii_lowercase())
                ));
            }
            Item::Key { expression, alias } => {
                // Not grouped: left to the engine, which says what is wrong.
                let Some(bare) = bare_of(expression) else {
                    return Ok(None);
                };
                outer_items.push(format!("{} AS {}", bare, alias.clone().unwrap_or_else(|| bare.clone())));
            }
        }
    }
    for (key, bare) in keys.iter().zip(&bare_keys) {
        if key.contains('.') {
            ordering = ordering.replace(key.as_str(), bare);
        }
    }

    let mut inner_items: Vec<String> = keys
        .iter()
        .zip(&bare_keys)
        .map(|(key, bare)| format!("{} AS {}", key, bare))
        .collect();
    inner_items.push(format!("{} AS {}", value.trim(), VALUE));

    let mut out = format!(
        "SELECT {} FROM (SELECT DISTINCT {} FROM {}) __distinct",
        outer_items.join(", "),
        inner_items.join(", "),
        source
    );
    if !bare_keys.is_empty() {
        out.push_str(" GROUP BY ");
        out.push_str(&bare_keys.join(", "));
    }
    if !ordering.is_empty() {
        out.push(' ');
        out.push_str(&ordering);
    }
    Ok(Some(out))
}

enum Item {
    Aggregate {
        function: &'static str,
        original: String,
        alias: Option<String>,
    },
    Key {
        expression: String,
        alias: Option<String>,
    },
}

fn unsupported(what: &str) -> String {
    format!("{} is not supported", what)
}

/// `FUNC(DISTINCT value)` as the whole of `expression`: the function and
/// the value.
fn distinct_call(expression: &str) -> Option<(&'static str, &str)> {
    let open = expression.find('(')?;
    let name = expression[..open].trim();
    let function = AGGREGATES.iter().find(|a| a.eq_ignore_ascii_case(name))?;
    let close = open + matching_close(&expression[open..])?;
    if !expression[close + 1..].trim().is_empty() {
        return None;
    }
    let inside = expression[open + 1..close].trim();
    if !starts_with_word(inside, "DISTINCT") {
        return None;
    }
    let value = inside["DISTINCT".len()..].trim();
    (!value.is_empty()).then_some((*function, value))
}

/// Whether `sql` holds `FUNC(` `DISTINCT` for some aggregate, anywhere
/// outside quotes.
fn mentions_distinct_aggregate(sql: &str) -> bool {
    let upper = unquoted(sql).to_ascii_uppercase();
    AGGREGATES.iter().any(|a| {
        upper.match_indices(a).any(|(at, _)| {
            let after = upper[at + a.len()..].trim_start();
            after
                .strip_prefix('(')
                .is_some_and(|inside| starts_with_word(inside.trim_start(), "DISTINCT"))
        })
    })
}

/// Whether `expression` calls the function `name` outside quotes.
fn calls(expression: &str, name: &str) -> bool {
    let mut at = 0;
    while let Some(found) = find_keyword(expression, name, at) {
        if expression[found + name.len()..].trim_start().starts_with('(') {
            return true;
        }
        at = found + name.len();
    }
    false
}

fn has_word(text: &str, word: &str) -> bool {
    unquoted(text)
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .any(|w| w.eq_ignore_ascii_case(word))
}

/// `text` with every quoted string and identifier blanked, so a word inside
/// one is not mistaken for SQL.
fn unquoted(text: &str) -> String {
    let mut quote: Option<char> = None;
    text.chars()
        .map(|c| match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
                ' '
            }
            None => {
                if c == '\'' || c == '"' {
                    quote = Some(c);
                    ' '
                } else {
                    c
                }
            }
        })
        .collect()
}

fn starts_with_word(text: &str, word: &str) -> bool {
    text.len() >= word.len()
        && text[..word.len()].eq_ignore_ascii_case(word)
        && text[word.len()..].starts_with(|c: char| c.is_whitespace() || c == '(')
}

/// The clause starting with `word` and then `BY`, at the top level.
fn clause(text: &str, word: &str) -> Option<usize> {
    let mut at = 0;
    while let Some(found) = find_keyword(text, word, at) {
        if starts_with_word(text[found + word.len()..].trim_start(), "BY") {
            return Some(found);
        }
        at = found + word.len();
    }
    None
}

/// `expr AS alias`, or `expr` alone.
fn split_alias(item: &str) -> (&str, Option<&str>) {
    match find_keyword(item, "AS", 0) {
        Some(at) => (item[..at].trim(), Some(item[at + 2..].trim())),
        None => (item, None),
    }
}

/// The bare column name of `t.col`, `col` or `"Col"`; `None` for anything
/// else.
fn bare_column(expression: &str) -> Option<String> {
    let last = expression.rsplit('.').next()?.trim();
    let ident = |s: &str| {
        let s = s.trim();
        (s.starts_with('"') && s.ends_with('"') && s.len() > 2)
            || (!s.is_empty() && s.chars().all(|c| c.is_alphanumeric() || c == '_'))
    };
    expression.split('.').all(ident).then(|| last.to_string())
}

fn normalized(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ").to_ascii_lowercase()
}

/// `text` split at its top-level commas, outside quotes and parentheses.
fn split_top_level(text: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let (mut depth, mut start) = (0usize, 0usize);
    let mut quote: Option<char> = None;
    for (i, c) in text.char_indices() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None => match c {
                '\'' | '"' => quote = Some(c),
                '(' => depth += 1,
                ')' => depth = depth.saturating_sub(1),
                ',' if depth == 0 => {
                    parts.push(&text[start..i]);
                    start = i + 1;
                }
                _ => {}
            },
        }
    }
    parts.push(&text[start..]);
    parts
}

/// The offset of the `)` closing the `(` that `text` starts with.
fn matching_close(text: &str) -> Option<usize> {
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    for (i, c) in text.char_indices() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None => match c {
                '\'' | '"' => quote = Some(c),
                '(' => depth += 1,
                ')' => {
                    depth = depth.checked_sub(1)?;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            },
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::rewrite;

    fn rewritten(sql: &str) -> String {
        rewrite(sql).expect(sql).expect(sql)
    }

    #[test]
    fn a_lone_distinct_count_reads_a_derived_table() {
        assert_eq!(
            rewritten("SELECT COUNT(DISTINCT x) FROM t WHERE id > 1"),
            "SELECT COUNT(__distinct_value) AS count FROM (SELECT DISTINCT x AS __distinct_value FROM t WHERE id > 1) __distinct"
        );
    }

    #[test]
    fn grouping_keys_are_carried_through_by_their_bare_names() {
        assert_eq!(
            rewritten("select t.g, count(distinct t.x) as n from t group by t.g order by t.g desc limit 5"),
            "SELECT g AS g, COUNT(__distinct_value) AS n FROM (SELECT DISTINCT t.g AS g, t.x AS __distinct_value FROM t) __distinct GROUP BY g order by g desc limit 5"
        );
    }

    #[test]
    fn every_aggregate_over_one_expression_is_rewritten() {
        assert_eq!(
            rewritten("SELECT COUNT(DISTINCT lower(e)), MAX(DISTINCT lower(e)) FROM t"),
            "SELECT COUNT(__distinct_value) AS count, MAX(__distinct_value) AS max FROM (SELECT DISTINCT lower(e) AS __distinct_value FROM t) __distinct"
        );
    }

    #[test]
    fn statements_without_one_are_left_alone() {
        for sql in [
            "SELECT COUNT(x) FROM t",
            "SELECT DISTINCT x FROM t",
            "SELECT 'count(distinct x)' FROM t",
            "UPDATE t SET a = 1",
        ] {
            assert_eq!(rewrite(sql).unwrap(), None, "{sql}");
        }
    }

    #[test]
    fn shapes_it_cannot_express_are_refused() {
        for sql in [
            "SELECT COUNT(DISTINCT x), COUNT(*) FROM t",
            "SELECT COUNT(DISTINCT x), COUNT(DISTINCT y) FROM t",
            "SELECT COUNT(DISTINCT x) + 1 FROM t",
            "SELECT g, COUNT(DISTINCT x) FROM t GROUP BY g HAVING COUNT(DISTINCT x) > 1",
            "SELECT COUNT(DISTINCT x) FROM t GROUP BY lower(g)",
        ] {
            let error = rewrite(sql).unwrap_err();
            assert!(error.contains("not supported"), "{sql}: {error}");
        }
    }
}
