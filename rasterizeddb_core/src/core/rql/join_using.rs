//! `JOIN … USING (cols)` and `NATURAL JOIN`, as the `ON` joins they mean.
//!
//! `a JOIN b USING (id)` joins on `a.id = b.id` and merges the two `id`s into
//! one output column: `id` named bare is that column (the left side's for an
//! inner or LEFT join, the right side's for a RIGHT join, the first non-NULL
//! of the two for a FULL join), and `*` lists it once, first, followed by
//! the rest of the left side's columns and then the right side's. `NATURAL`
//! is `USING` over every column name the two sides share. Both were "JOIN of
//! 'b' is missing its ON condition".
//!
//! The rewrite keeps the statement a join this engine reads: the `ON`
//! equalities, `*` expanded into the merged list (each column under its own
//! name), and every bare reference to a merged column replaced by what it
//! is. Left alone: a `FROM` with parentheses, a statement other than a
//! SELECT, and one whose columns are not known here (a CTE's, a view's).

use crate::core::rql::join::find_keyword;

const CLAUSE_ENDS: &[&str] = &[
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "LIMIT",
    "OFFSET",
    "FETCH",
    "FOR",
    "WINDOW",
    "UNION",
    "INTERSECT",
    "EXCEPT",
];

/// A refusal PostgreSQL makes too: `(sqlstate, message)`.
pub(crate) type Refusal = (&'static str, String);

/// A name bare where it reads back as itself (lower case, a letter or `_`
/// first), quoted otherwise: a quoted qualifier (`"t".id`) is not always
/// matched to its alias here, so the bare form is the safer one.
fn quote(name: &str) -> String {
    let plain = name
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && !matches!(
            name,
            "user"
                | "order"
                | "group"
                | "select"
                | "from"
                | "where"
                | "table"
                | "end"
                | "all"
                | "and"
                | "or"
                | "not"
                | "as"
                | "on"
                | "in"
                | "is"
                | "null"
                | "true"
                | "false"
                | "case"
                | "when"
                | "then"
                | "else"
                | "limit"
                | "offset"
                | "union"
                | "join"
                | "left"
                | "right"
                | "full"
                | "inner"
                | "outer"
                | "using"
                | "natural"
                | "cross"
                | "having"
                | "distinct"
                | "default"
                | "check"
                | "column"
                | "primary"
                | "references"
                | "unique"
                | "constraint"
                | "desc"
                | "asc"
        );
    if plain {
        name.to_string()
    } else {
        crate::core::rql::views::quote_ident(name)
    }
}

fn unquote(name: &str) -> String {
    let t = name.trim();
    if t.len() >= 2 && t.starts_with('"') && t.ends_with('"') {
        t[1..t.len() - 1].replace("\"\"", "\"")
    } else {
        t.to_ascii_lowercase()
    }
}

/// One relation of the `FROM` list and how it joins.
struct Joined {
    table: String,
    label: String,
    /// The join words before `JOIN` (`LEFT`, `FULL OUTER`, …), upper case.
    kind: String,
    natural: bool,
    using: Option<Vec<String>>,
    /// Everything after the relation and its alias: `ON …`, or empty.
    rest: String,
}

/// The `FROM` list split into its relations, or `None` for one this does
/// not read (parentheses other than a USING list, a comma join).
fn split_from(from: &str) -> Option<Vec<Joined>> {
    let mut out: Vec<Joined> = Vec::new();
    let upper = from.to_ascii_uppercase();
    let bytes = from.as_bytes();
    // Every top-level `JOIN`, with the join words in front of it.
    let mut joins: Vec<usize> = Vec::new();
    let mut at = 0;
    while let Some(j) = find_keyword(from, "JOIN", at) {
        joins.push(j);
        at = j + 4;
    }
    let word_before = |end: usize| -> Option<(usize, String)> {
        let head = from[..end].trim_end();
        let start = head.char_indices().rev().find(|&(_, c)| c.is_whitespace()).map_or(0, |(p, c)| p + c.len_utf8());
        Some((start, head[start..].to_ascii_uppercase()))
    };
    // Where each join's words begin.
    let mut starts: Vec<(usize, String, bool)> = Vec::new();
    for &j in &joins {
        let mut begin = j;
        let mut words: Vec<String> = Vec::new();
        let mut natural = false;
        while let Some((s, w)) = word_before(begin) {
            match w.as_str() {
                "LEFT" | "RIGHT" | "FULL" | "INNER" | "OUTER" | "CROSS" => {
                    words.insert(0, w);
                    begin = s;
                }
                "NATURAL" => {
                    natural = true;
                    begin = s;
                }
                _ => break,
            }
        }
        starts.push((begin, words.join(" "), natural));
    }
    let first_end = starts.first().map_or(from.len(), |(b, _, _)| *b);
    let first = from[..first_end].trim();
    if first.contains(',') || first.contains('(') {
        return None;
    }
    let relation = |text: &str| -> Option<(String, String)> {
        let mut parts = text.split_whitespace();
        let table = parts.next()?.to_string();
        let alias = match parts.next() {
            Some(w) if w.eq_ignore_ascii_case("AS") => parts.next().map(str::to_string),
            other => other.map(str::to_string),
        };
        if parts.next().is_some() {
            return None;
        }
        let table_name = unquote(table.rsplit('.').next().unwrap_or(&table));
        Some((
            table_name.clone(),
            alias.map(|a| unquote(&a)).unwrap_or(table_name),
        ))
    };
    let (table, label) = relation(first)?;
    out.push(Joined {
        table,
        label,
        kind: String::new(),
        natural: false,
        using: None,
        rest: String::new(),
    });
    for (i, &j) in joins.iter().enumerate() {
        let end = starts.get(i + 1).map_or(from.len(), |(b, _, _)| *b);
        let body = from[j + 4..end].trim();
        let (relation_text, rest) =
            match find_keyword(body, "USING", 0).or_else(|| find_keyword(body, "ON", 0)) {
                Some(k) => (&body[..k], body[k..].trim()),
                None => (body, ""),
            };
        if relation_text.contains('(') || relation_text.contains(',') {
            return None;
        }
        let (table, label) = relation(relation_text.trim())?;
        let mut using = None;
        let mut rest_text = rest.to_string();
        if rest.len() >= 5 && rest[..5].eq_ignore_ascii_case("USING") {
            let list = rest[5..].trim();
            let close = list.find(')')?;
            if !list.starts_with('(') || !list[close + 1..].trim().is_empty() {
                return None;
            }
            using = Some(list[1..close].split(',').map(unquote).collect());
            rest_text = String::new();
        } else if rest.contains('(') && !rest.to_ascii_uppercase().starts_with("ON") {
            return None;
        }
        let _ = (&upper, bytes);
        out.push(Joined {
            table,
            label,
            kind: starts[i].1.clone(),
            natural: starts[i].2,
            using,
            rest: rest_text,
        });
    }
    Some(out)
}

/// `sql` with each bare word in `merged` replaced by its expression, outside
/// quotes, never a qualified name's part, a call's name or an alias after AS.
fn replace_bare(sql: &str, merged: &[(String, String)]) -> String {
    let bytes = sql.as_bytes();
    let ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut out = String::with_capacity(sql.len() + 16);
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            let end = crate::core::sql_scan::skip_quoted(bytes, i).unwrap_or(bytes.len());
            out.push_str(&sql[i..end]);
            i = end.max(i + 1);
            continue;
        }
        let quoted = b == b'"';
        if quoted
            || (ident(b)
                && !b.is_ascii_digit()
                && (i == 0 || !(ident(bytes[i - 1]) || bytes[i - 1] == b'.')))
        {
            let start = i;
            let end = if quoted {
                crate::core::sql_scan::quoted_end(bytes, i, b'"')
            } else {
                let mut e = i;
                while e < bytes.len() && ident(bytes[e]) {
                    e += 1;
                }
                e
            };
            let word = &sql[start..end];
            let name = unquote(word);
            let qualified = start > 0 && bytes[start - 1] == b'.';
            let qualifier = bytes.get(end) == Some(&b'.');
            let call = sql[end..].trim_start().starts_with('(');
            let after_as = {
                let before = sql[..start].trim_end();
                before.len() >= 2
                    && before.as_bytes()[before.len() - 2..].eq_ignore_ascii_case(b"AS")
                    && !before[..before.len() - 2]
                        .ends_with(|c: char| c.is_ascii_alphanumeric() || c == '_')
            };
            match merged.iter().find(|(n, _)| *n == name) {
                Some((_, expr)) if !qualified && !qualifier && !call && !after_as => {
                    out.push_str(expr)
                }
                _ => out.push_str(word),
            }
            i = end.max(i + 1);
            continue;
        }
        let ch = sql[i..].chars().next().expect("in bounds");
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// The statement with its `USING` and `NATURAL` joins as `ON` joins, `None`
/// when it has none or cannot be read here, `Some(Err)` for what PostgreSQL
/// refuses too.
pub(crate) fn rewrite_statement(
    sql: &str,
    columns_of: &dyn Fn(&str) -> Option<Vec<String>>,
) -> Option<Result<String, Refusal>> {
    let has = |w: &[u8]| crate::core::sql_scan::contains_ignore_ascii_case(sql, w);
    if !(has(b"using") || has(b"natural")) {
        return None;
    }
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    if !trimmed
        .get(..6)
        .is_some_and(|w| w.eq_ignore_ascii_case("SELECT"))
    {
        return None;
    }
    let from = find_keyword(trimmed, "FROM", 0)?;
    let after = &trimmed[from + 4..];
    let end = CLAUSE_ENDS
        .iter()
        .filter_map(|k| find_keyword(after, k, 0))
        .min()
        .unwrap_or(after.len());
    let from_text = &after[..end];
    if find_keyword(from_text, "USING", 0).is_none()
        && find_keyword(from_text, "NATURAL", 0).is_none()
    {
        return None;
    }
    let relations = split_from(from_text)?;
    // The output columns so far: each name and what it reads.
    let first = &relations[0];
    let mut columns: Vec<(String, String)> = columns_of(&first.table)?
        .into_iter()
        .map(|c| (c.clone(), format!("{}.{}", quote(&first.label), quote(&c))))
        .collect();
    let mut merged: Vec<(String, String)> = Vec::new();
    let mut from_out = format!("{} {}", quote(&first.table), quote(&first.label));
    for r in relations.iter().skip(1) {
        let right: Vec<String> = columns_of(&r.table)?;
        let right_ref = |c: &str| format!("{}.{}", quote(&r.label), quote(c));
        let using: Option<Vec<String>> = if r.natural {
            Some(
                columns
                    .iter()
                    .map(|(n, _)| n.clone())
                    .filter(|n| right.contains(n))
                    .collect(),
            )
        } else {
            r.using.clone()
        };
        let join_words = if r.kind.is_empty() {
            "JOIN".to_string()
        } else {
            format!("{} JOIN", r.kind)
        };
        match using {
            None => {
                from_out.push_str(&format!(
                    " {} {} {} {}",
                    join_words,
                    quote(&r.table),
                    quote(&r.label),
                    r.rest
                ));
                columns.extend(right.iter().map(|c| (c.clone(), right_ref(c))));
            }
            Some(using) => {
                let mut conditions = Vec::new();
                let mut front = Vec::new();
                for u in &using {
                    let left: Vec<&(String, String)> =
                        columns.iter().filter(|(n, _)| n == u).collect();
                    match left.len() {
                        0 => {
                            return Some(Err((
                                "42703",
                                format!(
                                    "column \"{u}\" specified in USING clause does not exist in left table"
                                ),
                            )));
                        }
                        1 => {}
                        _ => {
                            return Some(Err((
                                "42702",
                                format!(
                                    "common column name \"{u}\" appears more than once in left table"
                                ),
                            )));
                        }
                    }
                    if !right.contains(u) {
                        return Some(Err((
                            "42703",
                            format!(
                                "column \"{u}\" specified in USING clause does not exist in right table"
                            ),
                        )));
                    }
                    let left_expr = left[0].1.clone();
                    conditions.push(format!("{left_expr} = {}", right_ref(u)));
                    let kind = r.kind.to_ascii_uppercase();
                    let expr = if kind.starts_with("FULL") {
                        format!("COALESCE({left_expr}, {})", right_ref(u))
                    } else if kind.starts_with("RIGHT") {
                        right_ref(u)
                    } else {
                        left_expr
                    };
                    front.push((u.clone(), expr));
                }
                let on = if conditions.is_empty() {
                    "ON TRUE".to_string()
                } else {
                    format!("ON {}", conditions.join(" AND "))
                };
                let join_words = if conditions.is_empty() && r.kind.is_empty() {
                    "CROSS JOIN".to_string()
                } else {
                    join_words
                };
                if join_words == "CROSS JOIN" {
                    from_out.push_str(&format!(
                        " CROSS JOIN {} {}",
                        quote(&r.table),
                        quote(&r.label)
                    ));
                } else {
                    from_out.push_str(&format!(
                        " {} {} {} {}",
                        join_words,
                        quote(&r.table),
                        quote(&r.label),
                        on
                    ));
                }
                let rest: Vec<(String, String)> = columns
                    .iter()
                    .filter(|(n, _)| !using.contains(n))
                    .cloned()
                    .collect();
                let right_rest = right
                    .iter()
                    .filter(|c| !using.contains(c))
                    .map(|c| (c.clone(), right_ref(c)));
                merged.retain(|(n, _)| !using.contains(n));
                merged.extend(front.iter().cloned());
                columns = front.into_iter().chain(rest).chain(right_rest).collect();
            }
        }
    }
    // The select list: `*` as the merged list; a bare merged column under its
    // own name.
    let list_start = {
        let l = &trimmed[6..from];
        let skipped = l.len() - l.trim_start().len();
        6 + skipped
            + if l
                .trim_start()
                .get(..8)
                .is_some_and(|w| w.eq_ignore_ascii_case("DISTINCT"))
            {
                8
            } else {
                0
            }
    };
    let list = &trimmed[list_start..from];
    let items: Vec<String> = crate::core::sql_scan::split_top_level(list, b',')
        .into_iter()
        .map(|item| {
            let item = item.trim();
            if item == "*" {
                return columns
                    .iter()
                    .map(|(n, e)| format!("{e} AS {}", quote(n)))
                    .collect::<Vec<_>>()
                    .join(", ");
            }
            let (expr, alias) = crate::core::rql::lexer_query::split_alias(item);
            let name = unquote(expr);
            if alias.is_none()
                && merged.iter().any(|(n, _)| *n == name)
                && expr
                    .trim()
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'"')
            {
                let (_, e) = merged.iter().find(|(n, _)| *n == name).expect("found");
                return format!("{e} AS {}", quote(&name));
            }
            replace_bare(item, &merged)
        })
        .collect();
    let tail = replace_bare(&after[end..], &merged);
    Some(Ok(format!(
        "{} {} FROM {} {}",
        trimmed[..list_start].trim_end(),
        items.join(", "),
        from_out,
        tail
    )
    .trim_end()
    .to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(name: &str) -> Option<Vec<String>> {
        Some(
            match name {
                "sa" => vec!["id", "k", "v"],
                "sb" => vec!["id", "k", "w"],
                "sc" => vec!["k", "z"],
                _ => return None,
            }
            .into_iter()
            .map(String::from)
            .collect(),
        )
    }

    fn rw(sql: &str) -> String {
        rewrite_statement(sql, &cols).unwrap().unwrap()
    }

    #[test]
    fn using_becomes_on_and_merges_its_columns() {
        assert_eq!(
            rw("SELECT id, v, w FROM sa JOIN sb USING (id) ORDER BY id"),
            "SELECT sa.id AS id, v, w FROM sa sa JOIN sb sb ON sa.id = sb.id ORDER BY sa.id"
        );
        let star = rw("SELECT * FROM sa JOIN sb USING (id, k)");
        assert!(
            star.starts_with("SELECT sa.id AS id, sa.k AS k, sa.v AS v, sb.w AS w FROM"),
            "{star}"
        );
        let full = rw("SELECT id FROM sa FULL JOIN sb USING (id)");
        assert!(full.contains("COALESCE(sa.id, sb.id) AS id"), "{full}");
        assert!(full.contains("FULL JOIN sb sb ON sa.id = sb.id"), "{full}");
        let right = rw("SELECT id FROM sa RIGHT JOIN sb USING (id)");
        assert!(right.starts_with("SELECT sb.id AS id"), "{right}");
    }

    #[test]
    fn natural_joins_the_shared_names() {
        let n = rw("SELECT * FROM sa NATURAL JOIN sb");
        assert!(n.contains("ON sa.id = sb.id AND sa.k = sb.k"), "{n}");
        // Chained: the merged `k` meets the third table's.
        let c = rw("SELECT k, z FROM sa JOIN sb USING (id, k) JOIN sc USING (k)");
        assert!(c.contains("JOIN sc sc ON sa.k = sc.k"), "{c}");
        // With `sa.k` and `sb.k` both still there, `USING (k)` is ambiguous,
        // as PostgreSQL says.
        assert_eq!(
            rewrite_statement(
                "SELECT k FROM sa JOIN sb USING (id) JOIN sc USING (k)",
                &cols
            )
            .unwrap()
            .unwrap_err()
            .0,
            "42702"
        );
    }

    #[test]
    fn what_postgresql_refuses_is_refused() {
        assert_eq!(
            rewrite_statement("SELECT * FROM sa JOIN sb USING (nope)", &cols)
                .unwrap()
                .unwrap_err()
                .0,
            "42703"
        );
        assert!(rewrite_statement("SELECT 1 FROM sa", &cols).is_none());
        assert!(rewrite_statement("DELETE FROM sa USING sb WHERE sa.id = sb.id", &cols).is_none());
    }
}
