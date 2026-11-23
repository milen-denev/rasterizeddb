//! Correlated scalar subqueries — `(SELECT max(v) FROM u WHERE u.k = t.k)`
//! in a select list, a `WHERE`, or an `UPDATE`'s `SET` — rewritten into the
//! `LATERAL` join they are equivalent to.
//!
//! Both were refused (0A000, "write it as a join"): a value per outer row,
//! which nothing evaluated. `LATERAL` (`rql::lateral`) evaluates a subquery
//! once per distinct outer value and joins its rows back, so
//!
//! ```sql
//! SELECT id, (SELECT max(v) FROM u WHERE u.k = t.k) FROM t
//! -- is run as
//! SELECT id, __cs0.__v AS max FROM t LEFT JOIN LATERAL (SELECT max(v) AS __v FROM u WHERE u.k = t.k) __cs0 ON true
//! ```
//!
//! `LEFT … ON true` keeps an outer row whose subquery has no rows, with NULL,
//! as a scalar subquery gives; an item named with [`ALIAS_PREFIX`] is checked
//! to give at most one row per outer value (`lateral::materialize`), the
//! "more than one row returned by a subquery used as an expression" (21000)
//! PostgreSQL raises.
//!
//! An `UPDATE` reads its new values out of such a SELECT, keyed by the
//! target's identifying key, and writes them back through `UPDATE … FROM`.
//!
//! In a grouped SELECT the join runs before the grouping. A subquery in the
//! `WHERE`, an aggregate's argument or a `FILTER` is a value per row, as
//! there. Anywhere else a group evaluates it once: it may read only the outer
//! columns the query is grouped by (42803 otherwise, as PostgreSQL), so its
//! value is added to the `GROUP BY` without changing any group, and a
//! `HAVING` term testing it alone moves into the `WHERE` (`group_by_values`).
//!
//! In a set operation each branch is rewritten on its own and read as a
//! derived table (`set_operation`); the whole's `ORDER BY` and `LIMIT` stay
//! outside the last branch.
//!
//! Refused rather than guessed (0A000): in a grouped SELECT,
//! a `GROUP BY` over expressions, the value inside a larger select-list
//! expression, in the `ORDER BY`, or compared with an aggregate in one
//! `HAVING` term (what a grouped join cannot take); a subquery returning
//! more than one column (42601, as PostgreSQL); and whatever `LATERAL`
//! refuses (references to two outer items, an outer alias named bare).

use crate::core::rql::join::find_keyword;

/// The alias prefix of the `LATERAL` items this writes, which
/// `lateral::materialize` checks to yield at most one row per outer value.
pub(crate) const ALIAS_PREFIX: &str = "__cs";

/// The column every rewritten subquery's one value is named.
const VALUE: &str = "__v";

/// `name` as an identifier: bare when it is one already, quoted otherwise.
fn ident(name: &str) -> String {
    let bare = name.chars().next().is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if bare { name.to_string() } else { format!("\"{}\"", name.replace('"', "\"\"")) }
}

/// The clauses that end a `FROM` list.
const AFTER_FROM: &[&str] = &["WHERE", "GROUP", "HAVING", "WINDOW", "ORDER", "LIMIT", "OFFSET", "FETCH", "FOR"];

fn starts_with_word(text: &str, word: &str) -> bool {
    let t = text.trim_start();
    t.len() >= word.len()
        && t.as_bytes()[..word.len()].eq_ignore_ascii_case(word.as_bytes())
        && t.as_bytes().get(word.len()).is_none_or(|b| !(b.is_ascii_alphanumeric() || *b == b'_'))
}

/// Where the top-level `FROM` list of `sql` (a SELECT) ends.
fn from_list_end(sql: &str, from: usize) -> usize {
    AFTER_FROM
        .iter()
        .filter_map(|kw| find_keyword(sql, kw, from + 4))
        .min()
        .unwrap_or(sql.trim_end().len())
}

/// `sql`, a SELECT holding correlated scalar subqueries, as the `LATERAL`
/// joins they mean; `None` when it holds none (or is not a SELECT).
pub(crate) fn rewrite_select(sql: &str) -> Option<Result<String, String>> {
    // A set operation may open with a parenthesised branch.
    let set_operation =
        || sql.trim_start().starts_with('(') && ["UNION", "INTERSECT", "EXCEPT"].iter().any(|kw| find_keyword(sql, kw, 0).is_some());
    if !starts_with_word(sql, "SELECT") && !set_operation() {
        return None;
    }
    // A correlated `IN` subquery, as the `EXISTS` it means
    // (`executor::correlated_in_as_exists`): first, so its `NOT IN` form is
    // counted below like any other.
    // An EXISTS over a UNION, as its branches' EXISTS
    // (`executor::exists_over_set_operation`).
    let split = crate::core::rql::executor::exists_over_set_operation(sql);
    let sql = split.as_deref().unwrap_or(sql);
    let as_exists = crate::core::rql::executor::correlated_in_as_exists(sql).or_else(|| split.clone());
    let sql = as_exists.as_deref().unwrap_or(sql);
    // An EXISTS the `IN` form cannot say, as the count it asks about: a
    // correlated scalar subquery, placed with the rest below.
    let counted = crate::core::rql::executor::exists_as_counts(sql);
    let changed = counted.is_some() || as_exists.is_some();
    let sql = counted.as_deref().unwrap_or(sql);
    if outside_from(sql).is_none() {
        return changed.then(|| Ok(sql.to_string()));
    }
    Some(rewrite_select_inner(sql))
}

/// The first correlated scalar subquery of `sql` outside its top-level
/// `FROM` list: one in the list is a derived table or a `LATERAL` item,
/// which have passes of their own. Found in a copy with the list blanked, so
/// its offsets are `sql`'s.
fn outside_from(sql: &str) -> Option<crate::core::rql::subquery::InSubquery> {
    let masked = match find_keyword(sql, "FROM", 0) {
        Some(from) => {
            let end = from_list_end(sql, from);
            let mut bytes = sql.as_bytes().to_vec();
            for b in &mut bytes[from + 4..end] {
                *b = b' ';
            }
            String::from_utf8(bytes).ok()?
        }
        None => sql.to_string(),
    };
    crate::core::rql::subquery::find_correlated_scalar_subquery(&masked)
}

fn rewrite_select_inner(sql: &str) -> Result<String, String> {
    let refuse = |what: &str| {
        Err(format!(
            "a correlated scalar subquery in a SELECT with {what} is not supported yet; write it as a join"
        ))
    };
    if ["UNION", "INTERSECT", "EXCEPT"].iter().any(|kw| find_keyword(sql, kw, 0).is_some()) {
        return set_operation(sql);
    }
    let mut current = sql.trim().trim_end_matches(';').trim_end().to_string();
    // Every subquery replaced first, its join kept to append after: an
    // appended join's body is itself a correlated subquery the finder would
    // return next.
    let mut joins: Vec<String> = Vec::new();
    // Each subquery that a grouped query evaluates once per group, with the
    // outer columns it reads (see `group_by_values`).
    let mut per_group: Vec<(String, Vec<(String, String)>)> = Vec::new();
    // Whether one of them is part of a larger select-list expression.
    let mut in_select_expression = false;
    for n in 0..=32usize {
        let Some(sub) = outside_from(&current) else {
            break;
        };
        if n == 32 {
            return refuse("more than 32 correlated subqueries");
        }
        let Some(from) = find_keyword(&current, "FROM", 0) else {
            return refuse("no FROM");
        };
        let alias = format!("{ALIAS_PREFIX}{n}");
        let (body, label) = value_body(&sub.inner_sql)?;
        // A whole select item with no alias of its own keeps the subquery's
        // name for its column, as PostgreSQL names it (`max`).
        let before = current[..sub.start].trim_end();
        // `end` is past the closing parenthesis.
        let after = current[sub.end..].trim_start();
        let whole_item = sub.start < from
            && (before.ends_with(',') || starts_with_word_end(before, "SELECT") || starts_with_word_end(before, "DISTINCT"))
            && (after.starts_with(',') || starts_with_word(after, "FROM"));
        let replacement = if whole_item {
            format!("{alias}.{VALUE} AS \"{}\"", label.replace('"', "\"\""))
        } else {
            format!("{alias}.{VALUE}")
        };
        if !per_row(&current, sub.start) {
            per_group.push((alias.clone(), crate::core::rql::subquery::outer_refs(&sub.inner_sql)));
            in_select_expression |= sub.start < from && !alone_in_item(before, after);
        }
        current = format!("{}{}{}", &current[..sub.start], replacement, &current[sub.end..]);
        joins.push(format!("LEFT JOIN LATERAL ({body}) {alias} ON true"));
    }
    if joins.is_empty() {
        return Ok(current);
    }
    let from = find_keyword(&current, "FROM", 0).ok_or_else(|| "no FROM".to_string())?;
    let end = from_list_end(&current, from);
    let (head, tail) = current.split_at(end);
    let joined = format!("{} {} {}", head.trim_end(), joins.join(" "), tail.trim_start()).trim_end().to_string();
    group_by_values(joined, &per_group, in_select_expression)
}

/// Whether the subquery at `at` is evaluated once per row of the outer
/// query even when that query groups: in its `WHERE`, or inside an
/// aggregate's argument or `FILTER`. Anywhere else (the select list,
/// `HAVING`, `ORDER BY`) a grouped query evaluates it once per group.
fn per_row(sql: &str, at: usize) -> bool {
    if let Some(wh) = find_keyword(sql, "WHERE", 0) {
        let end = ["GROUP", "HAVING", "WINDOW", "ORDER", "LIMIT", "OFFSET", "FETCH", "FOR"]
            .iter()
            .filter_map(|kw| find_keyword(sql, kw, wh + 5))
            .min()
            .unwrap_or(sql.len());
        if wh < at && at < end {
            return true;
        }
    }
    // The parentheses open around `at`, each judged by the word before it.
    let bytes = sql.as_bytes();
    let mut open: Vec<usize> = Vec::new();
    let mut i = 0;
    while i < at {
        match bytes[i] {
            q @ (b'\'' | b'"') => {
                i += 1;
                while i < at && bytes[i] != q {
                    i += 1;
                }
            }
            b'(' => open.push(i),
            b')' => {
                open.pop();
            }
            _ => {}
        }
        i += 1;
    }
    open.iter().any(|&paren| {
        let before = sql[..paren].trim_end();
        let word_start = before
            .rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .map_or(0, |p| p + 1);
        let word = before[word_start..].to_ascii_lowercase();
        word == "filter" || crate::core::rql::distinct_on::AGGREGATES.contains(&word.as_str())
    })
}

/// `sql` with the value of each subquery in `per_group` added to its `GROUP
/// BY`, when the query groups: as PostgreSQL requires, a subquery a group
/// evaluates once reads only the outer columns the group is keyed by, so its
/// value is one per group and keying by it as well changes no group. One
/// reading another outer column is 42803, as there; a `GROUP BY` over
/// anything but plain columns is refused, not guessed.
fn group_by_values(
    sql: String,
    per_group: &[(String, Vec<(String, String)>)],
    in_select_expression: bool,
) -> Result<String, String> {
    let group = find_keyword(&sql, "GROUP", 0);
    let grouped = group.is_some() || find_keyword(&sql, "HAVING", 0).is_some() || aggregates_in_select_list(&sql)?;
    if !grouped || per_group.is_empty() {
        return Ok(sql);
    }
    let Some(group) = group else {
        // Grouped by an aggregate or HAVING alone: no column is a key.
        return Err(match per_group.iter().find_map(|(_, refs)| refs.first()) {
            Some((q, c)) => format!("subquery uses ungrouped column \"{q}.{c}\" from outer query"),
            None => "a correlated scalar subquery in a grouped SELECT is not supported yet; write it as a join".into(),
        });
    };
    let by = sql[group + 5..].trim_start();
    if !starts_with_word(by, "BY") {
        return Err("a correlated scalar subquery in a SELECT with GROUP BY is not supported here yet; write it as a join".into());
    }
    let list_start = sql.len() - by.len() + 2;
    let list_end = ["HAVING", "WINDOW", "ORDER", "LIMIT", "OFFSET", "FETCH", "FOR"]
        .iter()
        .filter_map(|kw| find_keyword(&sql, kw, list_start))
        .min()
        .unwrap_or(sql.len());
    let mut keys: Vec<(Option<String>, String)> = Vec::new();
    for key in crate::core::sql_scan::split_top_level(&sql[list_start..list_end], b',') {
        match plain_column(key.trim()) {
            Some(k) => keys.push(k),
            None => {
                return Err(format!(
                    "a correlated scalar subquery in a SELECT grouped by {} is not supported yet; write it as a join",
                    key.trim()
                ));
            }
        }
    }
    // A grouped join selects a joined column only on its own.
    if in_select_expression {
        return Err("a correlated scalar subquery inside an expression in a grouped SELECT is not supported yet; \
                    select it on its own"
            .into());
    }
    // A grouped join orders only by what it selects.
    if let Some(order) = find_keyword(&sql, "ORDER", 0)
        && per_group.iter().any(|(alias, _)| sql[order..].contains(&format!("{alias}.{VALUE}")))
    {
        return Err("a correlated scalar subquery in the ORDER BY of a grouped SELECT is not supported yet; \
                    select it and order by its name"
            .into());
    }
    let mut added = String::new();
    for (alias, refs) in per_group {
        for (q, c) in refs {
            let keyed = keys.iter().any(|(kq, kc)| kc == c && kq.as_ref().is_none_or(|kq| kq == q));
            if !keyed {
                return Err(format!("subquery uses ungrouped column \"{q}.{c}\" from outer query"));
            }
        }
        added.push_str(&format!(", {alias}.{VALUE}"));
    }
    let head = sql[..list_end].trim_end();
    let tail = &sql[list_end..];
    let grouped_sql = format!("{head}{added} {}", tail.trim_start()).trim_end().to_string();
    having_to_where(grouped_sql, per_group)
}

/// `sql` (grouped, with a `GROUP BY`) with each `HAVING` conjunct that reads
/// a per-group subquery's value and calls no aggregate moved into `WHERE`.
/// It reads group keys only, so it keeps or drops a group's rows together,
/// and filtering them before grouping leaves the same groups. A `HAVING`
/// over a join takes only aggregate tests, which is why it moves.
fn having_to_where(sql: String, per_group: &[(String, Vec<(String, String)>)]) -> Result<String, String> {
    let Some(having) = find_keyword(&sql, "HAVING", 0) else {
        return Ok(sql);
    };
    let start = having + "HAVING".len();
    let end = ["WINDOW", "ORDER", "LIMIT", "OFFSET", "FETCH", "FOR"]
        .iter()
        .filter_map(|kw| find_keyword(&sql, kw, start))
        .min()
        .unwrap_or(sql.len());
    let reads_value = |term: &str| per_group.iter().any(|(alias, _)| term.contains(&format!("{alias}.{VALUE}")));
    let (moved, kept): (Vec<_>, Vec<_>) = crate::core::rql::having::conjuncts(&sql[start..end])?
        .into_iter()
        .partition(|(term, aggregate)| !aggregate && reads_value(term));
    if kept.iter().any(|(term, aggregate)| *aggregate && reads_value(term)) {
        return Err("a correlated scalar subquery compared with an aggregate in HAVING is not supported yet; \
                    write it as a join"
            .into());
    }
    if moved.is_empty() {
        return Ok(sql);
    }
    let group = find_keyword(&sql, "GROUP", 0).ok_or_else(|| "no GROUP BY".to_string())?;
    let moved: Vec<String> = moved.into_iter().map(|(t, _)| format!("({t})")).collect();
    let filter = moved.join(" AND ");
    let before_group = match find_keyword(&sql[..group], "WHERE", 0) {
        Some(wh) => format!(
            "{} WHERE ({}) AND {filter}",
            sql[..wh].trim_end(),
            sql[wh + "WHERE".len()..group].trim()
        ),
        None => format!("{} WHERE {filter}", sql[..group].trim_end()),
    };
    let grouping = sql[group..having].trim_end();
    let having = if kept.is_empty() {
        String::new()
    } else {
        format!(" HAVING {}", kept.into_iter().map(|(t, _)| t).collect::<Vec<_>>().join(" AND "))
    };
    Ok(format!("{before_group} {grouping}{having} {}", sql[end..].trim_start()).trim_end().to_string())
}

/// Whether the top-level select list of `sql` calls an aggregate (which
/// makes the query grouped with no `GROUP BY`). A window function there is
/// refused: telling `sum(x) OVER ()` from `sum(x)` is not done here.
fn aggregates_in_select_list(sql: &str) -> Result<bool, String> {
    let Some(from) = find_keyword(sql, "FROM", 0) else {
        return Ok(false);
    };
    let list = &sql[..from];
    let calls = crate::core::rql::distinct_on::calls_an_aggregate(list);
    if calls && find_keyword(list, "OVER", 0).is_some() {
        return Err("a correlated scalar subquery beside a window function is not supported yet; write it as a join".into());
    }
    Ok(calls)
}

/// `name` or `qualifier.name`, folded as [`subquery::outer_refs`] folds
/// them; `None` for anything else (an expression, an ordinal).
fn plain_column(text: &str) -> Option<(Option<String>, String)> {
    let fold = |p: &str| -> Option<String> {
        if p.len() >= 2 && p.starts_with('"') && p.ends_with('"') {
            return Some(p[1..p.len() - 1].to_string());
        }
        let ok = p.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && p.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        ok.then(|| p.to_ascii_lowercase())
    };
    let parts: Vec<&str> = text.split('.').collect();
    match parts.as_slice() {
        [c] => Some((None, fold(c)?)),
        [q, c] => Some((Some(fold(q)?), fold(c)?)),
        _ => None,
    }
}

/// Whether the subquery between `before` and `after` is a whole select
/// item, `AS name` (or a bare name) after it allowed.
fn alone_in_item(before: &str, after: &str) -> bool {
    let starts_item = before.ends_with(',') || starts_with_word_end(before, "SELECT") || starts_with_word_end(before, "DISTINCT");
    let mut rest = after.trim_start();
    if starts_with_word(rest, "AS") {
        rest = rest[2..].trim_start();
    }
    if !(rest.starts_with(',') || starts_with_word(rest, "FROM")) {
        // A name: quoted, or a word that is not FROM.
        let name_len = if rest.starts_with('"') {
            rest[1..].find('"').map_or(0, |q| q + 2)
        } else {
            rest.find(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).unwrap_or(rest.len())
        };
        rest = rest[name_len..].trim_start();
    }
    starts_item && (rest.starts_with(',') || starts_with_word(rest, "FROM"))
}

/// A set operation whose branches hold correlated scalar subqueries: each
/// branch rewritten on its own, the operators and a trailing `ORDER BY` or
/// `LIMIT` (which belong to the whole) left where they were. A branch's
/// subquery reads only that branch's relations, so its lateral join is that
/// branch's. It was refused ("… in a SELECT with UNION is not supported").
fn set_operation(sql: &str) -> Result<String, String> {
    let text = sql.trim().trim_end_matches(';').trim_end();
    // Every top-level operator, with an `ALL` or `DISTINCT` after it.
    let mut cuts: Vec<(usize, usize)> = Vec::new();
    let mut at = 0;
    loop {
        let next = ["UNION", "INTERSECT", "EXCEPT"]
            .iter()
            .filter_map(|kw| find_keyword(text, kw, at).map(|p| (p, kw.len())))
            .min_by_key(|(p, _)| *p);
        let Some((p, len)) = next else { break };
        let mut end = p + len;
        let rest = &text[end..];
        for word in ["ALL", "DISTINCT"] {
            if starts_with_word(rest, word) {
                end += (rest.len() - rest.trim_start().len()) + word.len();
                break;
            }
        }
        cuts.push((p, end));
        at = end;
    }
    let mut out = String::with_capacity(text.len() + 64);
    let mut start = 0usize;
    let last = cuts.len();
    for (i, piece_end) in cuts.iter().map(|(p, _)| *p).chain(std::iter::once(text.len())).enumerate() {
        let mut piece = text[start..piece_end].trim();
        // The last branch carries the whole's `ORDER BY` and `LIMIT`, which
        // stay outside it.
        let mut tail = "";
        if i == last && !piece.starts_with('(') {
            if let Some(at) = ["ORDER", "LIMIT", "OFFSET", "FETCH"].iter().filter_map(|k| find_keyword(piece, k, 0)).min() {
                tail = &piece[at..];
                piece = piece[..at].trim_end();
            }
        }
        let (open, inner, close) = if piece.starts_with('(') && piece.ends_with(')') {
            ("(", &piece[1..piece.len() - 1], ")")
        } else {
            ("", piece, "")
        };
        // A branch with a correlated subquery as a derived table of its own:
        // its lateral join then reads that branch's relations alone. Joined
        // in place, a second branch's read the first's FROM ("missing
        // FROM-clause entry for table \"sb\"").
        let rewritten = if outside_from(inner).is_some() {
            // As written: the derived table's own pass rewrites it there.
            format!("SELECT * FROM ({inner}) {ALIAS_PREFIX}u{i}")
        } else {
            inner.to_string()
        };
        out.push_str(&format!(" {open}{rewritten}{close} {tail}"));
        if let Some((p, end)) = cuts.get(i) {
            out.push_str(&text[*p..*end]);
            start = *end;
        }
    }
    Ok(out.trim().to_string())
}

/// Whether `text` ends with the word `word`.
fn starts_with_word_end(text: &str, word: &str) -> bool {
    let t = text.trim_end();
    t.len() >= word.len()
        && t.as_bytes()[t.len() - word.len()..].eq_ignore_ascii_case(word.as_bytes())
        && (t.len() == word.len() || {
            let b = t.as_bytes()[t.len() - word.len() - 1];
            !(b.is_ascii_alphanumeric() || b == b'_')
        })
}

/// The subquery `inner` (`SELECT expr [AS a] FROM …`) with its one value
/// named [`VALUE`], and the name PostgreSQL gives its column.
fn value_body(inner: &str) -> Result<(String, String), String> {
    let inner = inner.trim();
    let Some(from) = find_keyword(inner, "FROM", 0) else {
        return Err("a correlated scalar subquery without FROM is not supported yet".to_string());
    };
    let select_end = 6;
    let mut list_start = select_end;
    if starts_with_word(&inner[select_end..], "DISTINCT") {
        let skipped = inner[select_end..].len() - inner[select_end..].trim_start().len();
        list_start = select_end + skipped + "DISTINCT".len();
    }
    let list = inner[list_start..from].trim();
    if crate::core::sql_scan::split_top_level(list, b',').len() != 1 {
        return Err("subquery must return only one column".to_string());
    }
    let (expr, alias) = crate::core::rql::lexer_query::split_alias(list);
    let label = alias.map(|a| a.trim_matches('"').to_string()).unwrap_or_else(|| crate::core::rql::join::pg_column_name(expr));
    let body = format!("{} {} AS {VALUE} {}", &inner[..list_start], expr.trim(), &inner[from..]);
    Ok((body, label))
}

/// `UPDATE t [alias] SET … WHERE …` whose SET reads correlated scalar
/// subqueries, as `UPDATE … FROM` over a SELECT computing each new value per
/// row, keyed by `key` (the target's identifying key's columns). `None` when
/// it reads none.
pub(crate) fn rewrite_update(sql: &str, key: &[String]) -> Option<Result<String, String>> {
    crate::core::rql::subquery::find_correlated_scalar_subquery(sql)?;
    Some(rewrite_update_inner(sql, key))
}

/// An `UPDATE` or `DELETE` whose `WHERE` holds a correlated scalar subquery
/// — what `executor::exists_as_counts` makes of an `EXISTS` comparing the
/// outer row by anything but `=`, `(SELECT count(*) FROM ev e2 WHERE e2.v >
/// ev.v) > 0` — as the `UPDATE … FROM` or `DELETE … USING` over a SELECT
/// computing each row's subquery values, keyed by the target's identifying
/// key:
///
/// ```sql
/// UPDATE ev SET v = 0 FROM (SELECT ev.id AS __k0, (SELECT count(*) …) AS __w0
///   FROM ev) __cw WHERE ev.id = __cw.__k0 AND ((__cw.__w0) > 0) RETURNING id
/// ```
///
/// The values are read before any row is written, under the statement's
/// snapshot, so a subquery over the table being written sees it as it was,
/// as PostgreSQL's does. `None` when the `WHERE` holds no correlated
/// subquery; refused when the `SET` holds one too, or the statement already
/// names a second relation.
pub(crate) fn rewrite_write_where(sql: &str, key: &[String], delete: bool) -> Option<Result<String, String>> {
    let sql = sql.trim().trim_end_matches(';').trim_end();
    let where_at = find_keyword(sql, "WHERE", 0)?;
    let (body_end, returning) = match find_keyword(sql, "RETURNING", where_at) {
        Some(at) => (at, Some(sql[at..].trim())),
        None => (sql.len(), None),
    };
    let condition = sql[where_at + "WHERE".len()..body_end].trim();
    // The target and its alias: `UPDATE t [AS] a SET`, `DELETE FROM t [AS] a WHERE`.
    let (head_end, target_text) = if delete {
        let from = find_keyword(sql, "FROM", 0)?;
        (where_at, sql[from + 4..where_at].trim())
    } else {
        let set_at = find_keyword(sql, "SET", 0)?;
        (set_at, sql[6..set_at].trim())
    };
    let mut words = target_text.split_whitespace();
    let table = words.next()?.to_string();
    let alias = match (words.next(), words.next()) {
        (Some(w), Some(a)) if w.eq_ignore_ascii_case("AS") => a.to_string(),
        (Some(a), None) => a.to_string(),
        (None, _) => table.clone(),
        _ => return None,
    };
    // A write that already names a second relation (`UPDATE … FROM`,
    // `DELETE … USING`) evaluates its correlated terms per joined row
    // itself (`executor::split_correlated_terms`): left to it.
    let second = if delete { find_keyword(sql, "USING", 0) } else { find_keyword(sql, "FROM", head_end) };
    if second.is_some_and(|at| at < where_at) {
        return None;
    }
    let probe = format!("SELECT 1 FROM {table} {alias} WHERE {condition}");
    crate::core::rql::subquery::find_correlated_scalar_subquery(&probe)?;
    Some(rewrite_write_where_inner(sql, key, delete, head_end, where_at, condition, returning, &table, &alias))
}

#[allow(clippy::too_many_arguments)]
fn rewrite_write_where_inner(
    sql: &str,
    key: &[String],
    delete: bool,
    head_end: usize,
    where_at: usize,
    condition: &str,
    returning: Option<&str>,
    table: &str,
    alias: &str,
) -> Result<String, String> {
    if key.is_empty() {
        return Err(format!(
            "a correlated subquery in the WHERE of an {} needs a primary key or a unique column on the table to \
             name each row by",
            if delete { "DELETE" } else { "UPDATE" }
        ));
    }
    if !delete {
        let assignments = &sql[head_end + 3..where_at];
        if crate::core::rql::subquery::find_correlated_scalar_subquery(assignments).is_some() {
            return Err(
                "an UPDATE with a correlated subquery in both its SET and its WHERE is not supported yet".to_string(),
            );
        }
    }
    // Each correlated subquery of the condition, out to the derived table,
    // and its place taken by the column holding its value.
    let prefix = format!("SELECT 1 FROM {table} {alias} WHERE ");
    let mut probe = format!("{prefix}{condition}");
    let mut values: Vec<String> = Vec::new();
    for n in 0..=32usize {
        let Some(sub) = crate::core::rql::subquery::find_correlated_scalar_subquery(&probe) else { break };
        if n == 32 {
            return Err("more than 32 correlated subqueries in one WHERE".to_string());
        }
        values.push(format!("{} AS __w{n}", &probe[sub.start..sub.end]));
        probe = format!("{}(__cw.__w{n}){}", &probe[..sub.start], &probe[sub.end..]);
    }
    let condition = &probe[prefix.len()..];
    let keys = key
        .iter()
        .enumerate()
        .map(|(n, k)| format!("{alias}.{} AS __k{n}", ident(k)))
        .collect::<Vec<_>>()
        .join(", ");
    let select = format!("SELECT {keys}, {} FROM {table} {alias}", values.join(", "));
    let select = match rewrite_select(&select) {
        Some(Ok(s)) => s,
        Some(Err(e)) => return Err(e),
        None => select,
    };
    let join = key
        .iter()
        .enumerate()
        .map(|(n, k)| format!("{alias}.{} = __cw.__k{n}", ident(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let alias_clause = if alias == table { String::new() } else { format!(" {alias}") };
    let returning = returning.map(|r| format!(" {r}")).unwrap_or_default();
    Ok(if delete {
        format!("DELETE FROM {table}{alias_clause} USING ({select}) __cw WHERE {join} AND ({condition}){returning}")
    } else {
        let assignments = sql[head_end + 3..where_at].trim();
        format!("UPDATE {table}{alias_clause} SET {assignments} FROM ({select}) __cw WHERE {join} AND ({condition}){returning}")
    })
}

fn rewrite_update_inner(sql: &str, key: &[String]) -> Result<String, String> {
    let sql = sql.trim().trim_end_matches(';').trim_end();
    let set_at = find_keyword(sql, "SET", 0).ok_or_else(|| "UPDATE without SET".to_string())?;
    if find_keyword(sql, "FROM", set_at).is_some() {
        return Err(
            "a correlated subquery in an UPDATE with FROM is not supported yet; compute it in the FROM list".to_string(),
        );
    }
    // `RETURNING` stays the statement's: carried onto the `UPDATE … FROM`
    // this becomes, which returns each written row. It was refused.
    let (sql, returning) = match find_keyword(sql, "RETURNING", set_at) {
        Some(at) => (sql[..at].trim_end(), Some(sql[at..].trim())),
        None => (sql, None),
    };
    let rewritten = rewrite_update_body(sql, set_at, key)?;
    Ok(match returning {
        Some(returning) => format!("{rewritten} {returning}"),
        None => rewritten,
    })
}

fn rewrite_update_body(sql: &str, set_at: usize, key: &[String]) -> Result<String, String> {
    if key.is_empty() {
        return Err(
            "a correlated subquery in UPDATE needs a primary key or a unique column on the table to name each \
             row by; write it as UPDATE … FROM"
                .to_string(),
        );
    }
    // `UPDATE t [AS] a`: the name the subqueries qualify the row with.
    let target = sql[6..set_at].trim();
    let mut words = target.split_whitespace();
    let table = words.next().unwrap_or("").to_string();
    let alias = match (words.next(), words.next()) {
        (Some(w), Some(a)) if w.eq_ignore_ascii_case("AS") => a.to_string(),
        (Some(a), None) => a.to_string(),
        _ => table.clone(),
    };
    let where_at = find_keyword(sql, "WHERE", set_at);
    let assignments_text = &sql[set_at + 3..where_at.unwrap_or(sql.len())];
    let condition = where_at.map(|w| sql[w + 5..].trim());
    let mut values: Vec<String> = Vec::new();
    let mut sets: Vec<String> = Vec::new();
    for (i, assignment) in crate::core::sql_scan::split_top_level(assignments_text, b',').into_iter().enumerate() {
        let Some(eq) = assignment.find('=') else {
            return Err(format!("syntax error in assignment '{assignment}'"));
        };
        let column = assignment[..eq].trim();
        let value = assignment[eq + 1..].trim();
        if crate::core::rql::subquery::find_correlated_scalar_subquery(value).is_some()
            || crate::core::rql::subquery::find_correlated_scalar_subquery(&format!("SELECT {value} FROM {table} {alias}"))
                .is_some()
        {
            values.push(format!("{value} AS __c{i}"));
            sets.push(format!("{column} = __cu.__c{i}"));
        } else {
            sets.push(format!("{column} = {value}"));
        }
    }
    let keys = key
        .iter()
        .enumerate()
        .map(|(n, k)| format!("{alias}.{} AS __k{n}", ident(k)))
        .collect::<Vec<_>>()
        .join(", ");
    // The values for the rows the UPDATE writes: filtered as it is, so a
    // subquery runs only for the outer values those rows hold.
    let filter = condition.map(|c| format!(" WHERE {c}")).unwrap_or_default();
    let select = format!("SELECT {keys}, {} FROM {table} {alias}{filter}", values.join(", "));
    let select = match rewrite_select(&select) {
        Some(Ok(s)) => s,
        Some(Err(e)) => return Err(e),
        None => select,
    };
    let join = key
        .iter()
        .enumerate()
        .map(|(n, k)| format!("{alias}.{} = __cu.__k{n}", ident(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let alias_clause = if alias == table { String::new() } else { format!(" {alias}") };
    Ok(format!(
        "UPDATE {table}{alias_clause} SET {} FROM ({select}) __cu WHERE {join}{}",
        sets.join(", "),
        condition.map(|c| format!(" AND ({c})")).unwrap_or_default()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_select_item_becomes_a_left_lateral_join() {
        let out = rewrite_select("SELECT id, (SELECT max(v) FROM cu WHERE cu.k = ct.k) FROM ct WHERE id > 1 ORDER BY id")
            .unwrap()
            .unwrap();
        assert_eq!(
            out,
            "SELECT id, __cs0.__v AS \"max\" FROM ct LEFT JOIN LATERAL (SELECT max(v) AS __v FROM cu WHERE cu.k = ct.k) __cs0 ON true WHERE id > 1 ORDER BY id"
        );
        assert!(rewrite_select("SELECT id FROM ct").is_none());
        assert_eq!(
            rewrite_select("SELECT k, (SELECT max(v) FROM cu WHERE cu.k = ct.k) FROM ct GROUP BY k").unwrap().unwrap(),
            "SELECT k, __cs0.__v AS \"max\" FROM ct LEFT JOIN LATERAL (SELECT max(v) AS __v FROM cu WHERE cu.k = ct.k) __cs0 ON true GROUP BY k, __cs0.__v"
        );
        // A grouped query evaluates it once per group: the outer columns it
        // reads must be keys (42803 otherwise, as PostgreSQL), and per row
        // in an aggregate's argument, which adds no key.
        assert!(rewrite_select("SELECT d, (SELECT max(v) FROM cu WHERE cu.k = ct.k) FROM ct GROUP BY d").unwrap().is_err());
        assert!(rewrite_select("SELECT count(*), (SELECT max(v) FROM cu WHERE cu.k = ct.k) FROM ct").unwrap().is_err());
        assert_eq!(
            rewrite_select("SELECT d, sum((SELECT max(v) FROM cu WHERE cu.k = ct.k)) FROM ct GROUP BY d").unwrap().unwrap(),
            "SELECT d, sum(__cs0.__v) FROM ct LEFT JOIN LATERAL (SELECT max(v) AS __v FROM cu WHERE cu.k = ct.k) __cs0 ON true GROUP BY d"
        );
        assert!(rewrite_select("SELECT k, (SELECT max(v) FROM cu WHERE cu.k = ct.k) FROM ct GROUP BY k + 1").unwrap().is_err());
        assert!(rewrite_select("SELECT (SELECT v, k FROM cu WHERE cu.k = ct.k) FROM ct").unwrap().is_err());
    }
}
