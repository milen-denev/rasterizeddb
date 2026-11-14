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
//! Refused rather than guessed (0A000): an outer `GROUP BY`/`HAVING`, whose
//! grouping the join would change; a set operation; a subquery returning more
//! than one column (42601, as PostgreSQL); and whatever `LATERAL` refuses
//! (references to two outer items, an outer alias named bare).

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
    if !starts_with_word(sql, "SELECT") {
        return None;
    }
    outside_from(sql)?;
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
    for kw in ["UNION", "INTERSECT", "EXCEPT"] {
        if find_keyword(sql, kw, 0).is_some() {
            return refuse(kw);
        }
    }
    if find_keyword(sql, "GROUP", 0).is_some() {
        return refuse("GROUP BY");
    }
    if find_keyword(sql, "HAVING", 0).is_some() {
        return refuse("HAVING");
    }
    let mut current = sql.trim().trim_end_matches(';').trim_end().to_string();
    // Every subquery replaced first, its join kept to append after: an
    // appended join's body is itself a correlated subquery the finder would
    // return next.
    let mut joins: Vec<String> = Vec::new();
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
        current = format!("{}{}{}", &current[..sub.start], replacement, &current[sub.end..]);
        joins.push(format!("LEFT JOIN LATERAL ({body}) {alias} ON true"));
    }
    if joins.is_empty() {
        return Ok(current);
    }
    let from = find_keyword(&current, "FROM", 0).ok_or_else(|| "no FROM".to_string())?;
    let end = from_list_end(&current, from);
    let (head, tail) = current.split_at(end);
    Ok(format!("{} {} {}", head.trim_end(), joins.join(" "), tail.trim_start()).trim_end().to_string())
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

fn rewrite_update_inner(sql: &str, key: &[String]) -> Result<String, String> {
    let sql = sql.trim().trim_end_matches(';').trim_end();
    let set_at = find_keyword(sql, "SET", 0).ok_or_else(|| "UPDATE without SET".to_string())?;
    for kw in ["FROM", "RETURNING"] {
        if find_keyword(sql, kw, set_at).is_some() {
            return Err(format!(
                "a correlated subquery in an UPDATE with {kw} is not supported yet; compute it in the FROM list"
            ));
        }
    }
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
        assert!(rewrite_select("SELECT k, (SELECT max(v) FROM cu WHERE cu.k = ct.k) FROM ct GROUP BY k").unwrap().is_err());
        assert!(rewrite_select("SELECT (SELECT v, k FROM cu WHERE cu.k = ct.k) FROM ct").unwrap().is_err());
    }
}
