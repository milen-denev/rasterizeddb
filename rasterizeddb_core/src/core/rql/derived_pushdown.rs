//! A derived table built only for the rows its statement can use.
//!
//! `FROM (SELECT … FROM big) x` is materialised whole before the statement
//! reads it (`cte::materialize_derived`). Two things a statement says about
//! `x` often decide all but a few of its rows, and both are pushed into its
//! body here, before it is built:
//!
//! - a join to a relation this statement has already materialised, on an
//!   equality: `small s JOIN (SELECT … FROM big) x ON s.id = x.k` builds `x`
//!   for `k IN (the ids of s)`. EF Core's `Take(10)` over a filtered set
//!   joined to a derived table of every variant, or of every image numbered
//!   by `ROW_NUMBER()`, built the whole table (154 ms and 2.3 s on the
//!   pilot's ERP copy) where the ten products' rows took 30 ms;
//! - the statement's own `WHERE x.k IN (…)` / `x.k = …` over literals, which
//!   is also what the first leaves on a derived table nested inside `x`.
//!
//! The predicate is added to the body's `WHERE`, on the expression its
//! output column is, and only where that cannot change which of the body's
//! rows the statement keeps:
//!
//! - the output column must be a plain column of the body's `FROM`;
//! - no `LIMIT`/`OFFSET`/`FETCH`, set operation, `DISTINCT ON` or `WINDOW`
//!   clause; aggregates only under a `GROUP BY` that groups by the column;
//!   every window `PARTITION BY` it (a filter on a partition's key removes
//!   whole partitions, and numbers the rest as before);
//! - the column must be one whose `IN` inside the body compares as the
//!   statement compares it outside: a `uuid`, an integer or text of a real
//!   relation, never `character(n)` (its padded comparison is not text's);
//!   for a join's keys, both sides of one kind, and text only under one
//!   collation on both (a case-insensitive key against a binary column
//!   would be matched by the join and not by the list, or the reverse);
//! - few keys.
//!
//! A row the predicate removes could not have joined (or passed the
//! `WHERE`), so the statement's own join and `WHERE`, which still run,
//! decide the same rows. Anything this cannot read is left as written.

use rclite::Arc;

use crate::core::database::Database;
use crate::core::db_type::DbType;
use crate::core::mvcc::transaction::TxId;
use crate::core::rql::join::find_keyword;

/// More join keys than this are not pushed: the list would cost more to
/// read than the rows it saves.
const MAX_KEYS: usize = 256;

fn normal(name: &str) -> String {
    name.chars().filter(|c| !c.is_whitespace() && *c != '"').collect::<String>().to_lowercase()
}

/// Whether `text` is one column reference: `c`, `q.c`, quoted or not.
fn is_column(text: &str) -> bool {
    let t = text.trim();
    !t.is_empty()
        && t.split('.').count() <= 2
        && t.split('.').all(|part| {
            let p = part.trim();
            if p.len() >= 2 && p.starts_with('"') && p.ends_with('"') {
                !p[1..p.len() - 1].contains('"')
            } else {
                !p.is_empty()
                    && p.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80)
                    && !p.as_bytes()[0].is_ascii_digit()
            }
        })
}

/// The top-level clause of `sql` after `keyword` (found at or after
/// `from`), up to the first of `ends` or the end.
fn clause<'a>(sql: &'a str, keyword: &str, from: usize, ends: &[&str]) -> Option<(usize, usize, &'a str)> {
    let at = find_keyword(sql, keyword, from)?;
    let body = at + keyword.len();
    let mut end = sql.trim_end().trim_end_matches(';').len();
    for word in ends {
        if let Some(found) = find_keyword(sql, word, body) {
            end = end.min(found);
        }
    }
    (body <= end).then(|| (body, end, &sql[body..end]))
}

const AFTER_WHERE: &[&str] =
    &["GROUP", "HAVING", "WINDOW", "ORDER", "LIMIT", "OFFSET", "FETCH", "UNION", "INTERSECT", "EXCEPT", "FOR"];

/// The expression the body's output column `column` is, when it is a plain
/// column reference (`c`, `q.c`) and the only output of that name.
fn output_expression(body: &str, column: &str) -> Option<String> {
    if !body.trim_start().get(..6).is_some_and(|h| h.eq_ignore_ascii_case("SELECT")) {
        return None;
    }
    let list = crate::core::rql::cte::select_list_of(body)?;
    let list_body = list_without_distinct(list)?;
    let want = normal(column);
    let mut expression: Option<String> = None;
    for item in crate::core::sql_scan::split_top_level(list_body, b',') {
        let (expr, alias) = crate::core::rql::join::split_expr_alias(item.trim());
        let expr = expr.trim();
        let name = match &alias {
            Some(a) => normal(a),
            None if is_column(expr) => normal(expr.rsplit('.').next().unwrap_or(expr)),
            None => continue,
        };
        if name == want {
            // Two outputs of one name: not decided here.
            if expression.is_some() {
                return None;
            }
            expression = Some(expr.to_string());
        }
    }
    expression.filter(|e| is_column(e))
}

/// A select list past its `DISTINCT`; `None` for `DISTINCT ON (…)`.
fn list_without_distinct(list: &str) -> Option<&str> {
    let trimmed = list.trim_start();
    match trimmed.get(..9) {
        Some(h) if h.eq_ignore_ascii_case("DISTINCT ") => {
            let rest = trimmed[9..].trim_start();
            if rest.get(..2).is_some_and(|o| o.eq_ignore_ascii_case("ON")) && rest[2..].trim_start().starts_with('(') {
                return None;
            }
            Some(rest)
        }
        _ => Some(trimmed),
    }
}

/// The expression the body's output column `column` is, when it is a plain
/// column of the body's FROM and a predicate on it may go in the body's
/// WHERE (module docs). `None` otherwise.
pub(crate) fn pushable_expression(body: &str, column: &str) -> Option<String> {
    for word in ["LIMIT", "OFFSET", "FETCH", "UNION", "INTERSECT", "EXCEPT", "WINDOW"] {
        if find_keyword(body, word, 0).is_some() {
            return None;
        }
    }
    let expression = output_expression(body, column)?;
    let list_body = list_without_distinct(crate::core::rql::cte::select_list_of(body)?)?;
    let key = normal(&expression);
    // Grouped: only by the column itself.
    let grouped = clause(body, "GROUP", 0, &["HAVING", "WINDOW", "ORDER"]).map(|(_, _, text)| text.to_string());
    match &grouped {
        Some(text) => {
            let by = text.trim_start();
            let by = by.get(..2).filter(|b| b.eq_ignore_ascii_case("BY")).map(|_| &by[2..])?;
            if !crate::core::sql_scan::split_top_level(by, b',').iter().any(|g| normal(g) == key) {
                return None;
            }
        }
        None => {
            // An aggregate with no GROUP BY folds every row: unless each
            // is a window's (checked below).
            let aggregates = crate::core::rql::lexer_query::find_aggregate_calls(list_body).len();
            if aggregates > 0 && find_word(list_body, "OVER").is_empty() {
                return None;
            }
        }
    }
    // Every window partitions by the column.
    for at in find_word(body, "OVER") {
        let rest = body[at + 4..].trim_start();
        if !rest.starts_with('(') {
            return None;
        }
        let open = body.len() - rest.len();
        let close = crate::core::rql::expr::matching_paren(body, open)?;
        let inside = &body[open + 1..close];
        let (_, _, partition) = clause(inside, "PARTITION", 0, &["ORDER", "ROWS", "RANGE", "GROUPS"])?;
        let by = partition.trim_start();
        let by = by.get(..2).filter(|b| b.eq_ignore_ascii_case("BY")).map(|_| &by[2..])?;
        if !crate::core::sql_scan::split_top_level(by, b',').iter().any(|p| normal(p) == key) {
            return None;
        }
    }
    Some(expression)
}

/// The byte offsets of the word `word` in `text`, outside quotes, at any
/// depth of its own expressions, and not inside a subquery (`(SELECT …)`,
/// `(WITH …)`), whose words are its own.
fn find_word(text: &str, word: &str) -> Vec<usize> {
    let bytes = text.as_bytes();
    let w = word.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        if bytes[i] == b'(' {
            let inside = text[i + 1..].trim_start();
            let subquery = ["SELECT", "WITH"].iter().any(|k| {
                inside.get(..k.len()).is_some_and(|h| h.eq_ignore_ascii_case(k))
                    && inside.as_bytes().get(k.len()).is_none_or(|b| !crate::core::sql_scan::is_identifier_byte(*b))
            });
            if subquery {
                match crate::core::rql::expr::matching_paren(text, i) {
                    Some(close) => {
                        i = close + 1;
                        continue;
                    }
                    None => return out,
                }
            }
        }
        let boundary = |k: usize| bytes.get(k).is_none_or(|b| !crate::core::sql_scan::is_identifier_byte(*b));
        if bytes.len() - i >= w.len()
            && bytes[i..i + w.len()].eq_ignore_ascii_case(w)
            && (i == 0 || boundary(i - 1))
            && boundary(i + w.len())
        {
            out.push(i);
            i += w.len();
            continue;
        }
        i += 1;
    }
    out
}

/// `body` with `predicate` ANDed into its top-level WHERE.
pub(crate) fn with_predicate(body: &str, predicate: &str) -> String {
    let trimmed = body.trim_end().trim_end_matches(';');
    match clause(trimmed, "WHERE", 0, AFTER_WHERE) {
        Some((start, end, text)) => {
            format!("{} ({}) AND {} {}", &trimmed[..start], text.trim(), predicate, &trimmed[end..])
        }
        None => {
            let mut at = trimmed.len();
            for word in AFTER_WHERE {
                if let Some(found) = find_keyword(trimmed, word, 0) {
                    at = at.min(found);
                }
            }
            format!("{} WHERE {} {}", trimmed[..at].trim_end(), predicate, &trimmed[at..])
        }
    }
}

/// Whether `list` (an IN list's inside, or one value) holds literals only:
/// quoted strings, numbers, and `CAST('…' AS type)` / `'…'::type` of them.
fn literals_only(list: &str) -> bool {
    let bytes = list.as_bytes();
    let mut i = 0;
    let mut any = false;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i = crate::core::sql_scan::quoted_end(bytes, i, b'\'');
                any = true;
            }
            b'0'..=b'9' => {
                any = true;
                i += 1;
            }
            b' ' | b'\t' | b'\n' | b'\r' | b',' | b'.' | b'-' | b'(' | b')' | b':' => i += 1,
            b if b.is_ascii_alphabetic() || b == b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let word = list[start..i].to_ascii_uppercase();
                const TYPES: &[&str] =
                    &["CAST", "AS", "UUID", "TEXT", "INTEGER", "INT", "BIGINT", "SMALLINT", "VARCHAR", "INT4", "INT8", "INT2"];
                if !TYPES.contains(&word.as_str()) {
                    return false;
                }
            }
            _ => return false,
        }
    }
    any
}

/// The statement's own top-level WHERE conjuncts `alias.column IN (literals)`
/// and `alias.column = literal`: (column, the list's SQL).
pub(crate) fn literal_predicates(statement: &str, after: usize, alias: &str) -> Vec<(String, String)> {
    let Some((_, _, text)) = clause(statement, "WHERE", after, AFTER_WHERE) else { return Vec::new() };
    let prefix = format!("{}.", normal(alias));
    let mut out = Vec::new();
    for term in crate::core::rql::join::split_conjuncts(text) {
        let term = term.trim();
        let upper = term.to_ascii_uppercase();
        let (left, list) = if let Some(at) = find_keyword(term, "IN", 0).filter(|_| !upper.contains(" NOT IN")) {
            let rest = term[at + 2..].trim();
            if !(rest.starts_with('(') && rest.ends_with(')')) {
                continue;
            }
            (term[..at].trim(), rest[1..rest.len() - 1].to_string())
        } else if let Some((l, r)) = term.split_once('=') {
            if l.ends_with(['<', '>', '!']) || r.starts_with('=') {
                continue;
            }
            (l.trim(), r.trim().to_string())
        } else {
            continue;
        };
        if !is_column(left) || !normal(left).starts_with(&prefix) || !literals_only(&list) {
            continue;
        }
        out.push((left.rsplit('.').next().unwrap_or(left).trim().trim_matches('"').to_string(), list));
    }
    out
}

/// The equalities of the derived table's own `ON` (it is a JOIN's right
/// side, at `start..end` of `statement`): (other item's label, its column,
/// the derived table's column). Empty when it is not joined so, or the
/// join is one that may keep its unmatched rows (RIGHT, FULL).
pub(crate) fn join_equalities(statement: &str, start: usize, end: usize, alias: &str) -> Vec<(String, String, String)> {
    let before = statement[..start].trim_end();
    let upper = before.to_ascii_uppercase();
    if !upper.ends_with("JOIN") {
        return Vec::new();
    }
    let head = upper[..upper.len() - 4].trim_end();
    let head = head.strip_suffix("OUTER").map(str::trim_end).unwrap_or(head);
    if head.ends_with("RIGHT") || head.ends_with("FULL") || head.ends_with("CROSS") || head.ends_with("NATURAL") {
        return Vec::new();
    }
    let rest = &statement[end..];
    let trimmed = rest.trim_start();
    if !trimmed.get(..2).is_some_and(|o| o.eq_ignore_ascii_case("ON"))
        || trimmed.as_bytes().get(2).is_some_and(|b| crate::core::sql_scan::is_identifier_byte(*b))
    {
        return Vec::new();
    }
    let on_at = end + (rest.len() - trimmed.len()) + 2;
    let mut stop = statement.trim_end().trim_end_matches(';').len();
    for word in
        ["JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "WHERE", "GROUP", "HAVING", "WINDOW", "ORDER", "LIMIT", "OFFSET", "FETCH", "UNION", "INTERSECT", "EXCEPT", "FOR"]
    {
        if let Some(found) = find_keyword(statement, word, on_at) {
            stop = stop.min(found);
        }
    }
    let condition = statement[on_at..stop].trim();
    let condition = match condition.strip_prefix('(') {
        Some(inner) if crate::core::rql::expr::matching_paren(condition, 0) == Some(condition.len() - 1) => {
            &inner[..inner.len() - 1]
        }
        _ => condition,
    };
    let mine = format!("{}.", normal(alias));
    let mut out = Vec::new();
    for term in crate::core::rql::join::split_conjuncts(condition) {
        let Some((l, r)) = term.split_once('=') else { continue };
        if l.ends_with(['<', '>', '!']) || r.starts_with('=') {
            continue;
        }
        let (l, r) = (l.trim(), r.trim());
        if !is_column(l) || !is_column(r) || !l.contains('.') || !r.contains('.') {
            continue;
        }
        let (other, own) = if normal(r).starts_with(&mine) && !normal(l).starts_with(&mine) {
            (l, r)
        } else if normal(l).starts_with(&mine) && !normal(r).starts_with(&mine) {
            (r, l)
        } else {
            continue;
        };
        let part = |t: &str, k: usize| t.split('.').nth(k).unwrap_or("").trim().trim_matches('"').to_string();
        out.push((part(other, 0), part(other, 1), part(own, 1)));
    }
    out
}

/// How a column compares, for deciding whether a predicate moved onto it
/// compares as it did where it was written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Uuid,
    Integer,
    /// Text that is not `character(n)`, under its collation's strength.
    Text(crate::core::rql::collation::Strength),
}

/// The kind of `field` among its relation's `fields`; `None` for a type a
/// predicate is not moved onto.
fn kind_of(field: &crate::core::row::schema::SchemaField, fields: &[crate::core::row::schema::SchemaField]) -> Option<Kind> {
    use crate::core::rql::type_modifier::TextTypmod;
    match field.db_type {
        DbType::UUID => Some(Kind::Uuid),
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32 => Some(Kind::Integer),
        DbType::STRING if !field.json_text && !matches!(field.text_typmod(), Some(TextTypmod::Bpchar(_))) => {
            Some(Kind::Text(crate::core::rql::collation::of(&field.name, fields)))
        }
        _ => None,
    }
}

/// The kind of the body's column `expression` (`c`, `q.c`), read from the
/// relation of the body's FROM it belongs to; `None` when that is not a
/// relation with a schema (a derived table, a function), or the name is
/// one two items could have.
pub(crate) fn body_column_kind(body: &str, expression: &str, database: &Arc<Database>) -> Option<Kind> {
    let (_, from_end, _) = clause(body, "FROM", 0, &["WHERE", "GROUP", "HAVING", "WINDOW", "ORDER"])?;
    let items = crate::core::rql::lateral::items_before(body, from_end);
    let (qualifier, column) = match expression.rsplit_once('.') {
        Some((q, c)) => (Some(normal(q)), c.trim().trim_matches('"').to_string()),
        None => (None, expression.trim().trim_matches('"').to_string()),
    };
    let relation = match &qualifier {
        Some(q) => items.iter().find(|(alias, _)| normal(alias) == *q).map(|(_, r)| r.clone())?,
        None => match items.as_slice() {
            [(_, only)] => only.clone(),
            _ => return None,
        },
    };
    // A derived table of the body's own: the kind of the column its output
    // is, inside it (its copy keeps a plain column's type and collation).
    let trimmed = relation.trim();
    if trimmed.starts_with('(') && trimmed.ends_with(')') {
        let inner = &trimmed[1..trimmed.len() - 1];
        let expression = output_expression(inner, &column)?;
        return body_column_kind(inner, &expression, database);
    }
    let view = database.schema_view(trimmed.trim_matches('"'))?;
    let fields = &view.schema().fields;
    let field = fields.iter().find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&column))?;
    kind_of(field, fields)
}

/// A join key's text as the literal it is compared as, for the exact
/// types whose text reads back as the same value: a quoted `uuid` or text,
/// an integer's digits. `None` for any other type, or a text that is not
/// one of its values.
fn key_literal(ty: &DbType, text: &str) -> Option<String> {
    match ty {
        DbType::UUID | DbType::STRING => Some(crate::core::value_codec::quote_sql(text)),
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32 => {
            let digits = text.strip_prefix('-').unwrap_or(text);
            (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())).then(|| text.to_string())
        }
        _ => None,
    }
}

/// The derived table `body` (aliased `alias`, at `start..end` of
/// `statement`) with what the statement says about it pushed into its
/// WHERE (module docs); `None` when nothing is.
///
/// `built` names the relations this statement has materialised so far: a
/// join key is read only from one of those.
pub(crate) async fn narrowed(
    statement: &str,
    start: usize,
    end: usize,
    body: &str,
    alias: &str,
    built: &[String],
    database: &Arc<Database>,
    tx: TxId,
) -> Option<String> {
    let mut out: Option<String> = None;
    // `wanted`: the kind the list's values compare as outside (a join's
    // key column); `None` for the statement's own literals, which compare
    // with the column as they did.
    let push = |current: &str, column: &str, list: &str, wanted: Option<Kind>| -> Option<String> {
        let expression = pushable_expression(current, column)?;
        let kind = body_column_kind(current, &expression, database)?;
        if wanted.is_some_and(|w| w != kind) {
            return None;
        }
        Some(with_predicate(current, &format!("{expression} IN ({list})")))
    };
    for (column, list) in literal_predicates(statement, end, alias) {
        let current = out.as_deref().unwrap_or(body).to_string();
        if let Some(next) = push(&current, &column, &list, None) {
            #[cfg(test)]
            test_hooks::record(&format!("{alias}:where"), 1);
            out = Some(next);
        }
    }
    let equalities = join_equalities(statement, start, end, alias);
    if !equalities.is_empty() {
        let items = crate::core::rql::lateral::items_before(statement, start);
        for (label, other_column, own_column) in equalities {
            let Some((_, relation)) = items.iter().find(|(a, _)| normal(a) == normal(&label)) else { continue };
            let relation = relation.trim_matches('"');
            if !built.iter().any(|b| b == relation) {
                continue;
            }
            let current = out.as_deref().unwrap_or(body).to_string();
            let Some(view) = database.schema_view(relation) else { continue };
            let key_fields = &view.schema().fields;
            let Some(field) = key_fields.iter().find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&other_column)).cloned()
            else {
                continue;
            };
            // Decided before the keys are read: nothing is, for a body
            // that cannot take them, or whose column compares otherwise
            // than the key does.
            let Some(key_kind) = kind_of(&field, key_fields) else { continue };
            let takes = pushable_expression(&current, &own_column)
                .and_then(|e| body_column_kind(&current, &e, database))
                .is_some_and(|k| k == key_kind);
            if !takes {
                continue;
            }
            let quoted = crate::core::rql::collation::ident(&field.name);
            let keys = crate::core::rql::lateral::text_rows(
                &format!("SELECT DISTINCT {quoted}::text FROM {relation}"),
                1,
                database,
                tx,
            )
            .await
            .ok()?;
            if keys.len() > MAX_KEYS {
                continue;
            }
            // A NULL key joins nothing.
            let texts: Vec<String> = keys.iter().filter_map(|row| row.first().cloned().flatten()).collect();
            let values: Vec<String> = texts.iter().filter_map(|v| key_literal(&field.db_type, v)).collect();
            if values.len() != texts.len() {
                continue;
            }
            // No key at all: no row of the body can join.
            let next = if values.is_empty() {
                Some(with_predicate(&current, "1 = 0"))
            } else {
                push(&current, &own_column, &values.join(", "), Some(key_kind))
            };
            if let Some(next) = next {
                #[cfg(test)]
                test_hooks::record(alias, values.len());
                out = Some(next);
            }
        }
    }
    out
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Each derived table's pushed join keys, by alias.
    static PUSHED: Mutex<Option<HashMap<String, usize>>> = Mutex::new(None);

    pub(crate) fn record(alias: &str, keys: usize) {
        PUSHED.lock().unwrap().get_or_insert_with(HashMap::new).insert(alias.to_string(), keys);
    }

    /// The join keys pushed into the derived table `alias`, forgotten.
    pub(crate) fn take(alias: &str) -> Option<usize> {
        PUSHED.lock().unwrap().as_mut().and_then(|m| m.remove(alias))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_output_column_takes_the_predicate() {
        let body = "SELECT d10.csscolor, d10.id, d10.productid FROM variants AS d10 WHERE d10.hidden = false AND d10.iscolor = true";
        assert_eq!(pushable_expression(body, "productid").as_deref(), Some("d10.productid"));
        assert_eq!(pushable_expression(body, "ProductId").as_deref(), Some("d10.productid"));
        assert_eq!(
            with_predicate(body, "d10.productid IN ('a')"),
            "SELECT d10.csscolor, d10.id, d10.productid FROM variants AS d10 WHERE (d10.hidden = false AND d10.iscolor = true) AND d10.productid IN ('a') "
        );
        // Renamed, and with no WHERE of its own, before its ORDER BY.
        let renamed = "SELECT v.pid AS k, v.n FROM v ORDER BY v.n";
        assert_eq!(pushable_expression(renamed, "k").as_deref(), Some("v.pid"));
        assert_eq!(with_predicate(renamed, "v.pid IN (1)"), "SELECT v.pid AS k, v.n FROM v WHERE v.pid IN (1) ORDER BY v.n");
    }

    #[test]
    fn what_could_change_the_rows_is_left() {
        // An expression, a limit, a set operation, DISTINCT ON, an
        // aggregate over everything, a group or a window by another key.
        for body in [
            "SELECT upper(v.k) AS k FROM v",
            "SELECT v.k FROM v LIMIT 5",
            "SELECT v.k FROM v ORDER BY v.n OFFSET 2",
            "SELECT v.k FROM v UNION SELECT w.k FROM w",
            "SELECT DISTINCT ON (v.n) v.k FROM v",
            "SELECT max(v.k) AS k FROM v",
            "SELECT v.k, count(*) FROM v GROUP BY v.n, v.other",
            "SELECT v.k, ROW_NUMBER() OVER (PARTITION BY v.n ORDER BY v.x) AS rn FROM v",
            "SELECT v.k, ROW_NUMBER() OVER (ORDER BY v.x) AS rn FROM v",
            "SELECT v.k, v.j AS k FROM v",
        ] {
            assert_eq!(pushable_expression(body, "k"), None, "{body}");
        }
        // Grouped or partitioned by the column itself: whole groups go.
        assert_eq!(pushable_expression("SELECT v.k, count(*) AS n FROM v GROUP BY v.k", "k").as_deref(), Some("v.k"));
        assert_eq!(
            pushable_expression(
                "SELECT d2.alt, d2.productid, ROW_NUMBER() OVER(PARTITION BY d2.productid ORDER BY d2.listingorder) AS rn FROM images AS d2",
                "productid"
            )
            .as_deref(),
            Some("d2.productid")
        );
        assert_eq!(pushable_expression("SELECT DISTINCT v.k FROM v", "k").as_deref(), Some("v.k"));
    }

    #[test]
    fn the_statement_s_join_and_where_are_read() {
        let sql = "SELECT d11.id FROM __cte_d11_0_ab d11 INNER JOIN (SELECT v.k FROM v) AS d14 ON d11.id = d14.k ORDER BY d11.id";
        let start = sql.find("(SELECT").unwrap();
        let end = sql.find(" ON ").unwrap();
        assert_eq!(join_equalities(sql, start, end, "d14"), vec![("d11".to_string(), "id".to_string(), "k".to_string())]);
        let left = sql.replace("INNER JOIN", "LEFT JOIN");
        assert_eq!(join_equalities(&left, start - 1, end - 1, "d14").len(), 1);
        // The side that keeps its unmatched rows is not narrowed.
        let right = sql.replace("INNER JOIN", "RIGHT JOIN");
        assert!(join_equalities(&right, start, end, "d14").is_empty());
        let full = sql.replace("INNER JOIN", "FULL OUTER JOIN");
        assert!(join_equalities(&full, start + 5, end + 5, "d14").is_empty());
        // Under an OR nothing is an equality of the join.
        let or = "SELECT 1 FROM t a JOIN (SELECT v.k FROM v) AS x ON a.id = x.k OR a.n = 1";
        assert!(join_equalities(or, or.find("(SELECT").unwrap(), or.find(" ON ").unwrap(), "x").is_empty());

        let w = "SELECT x.a FROM (SELECT v.a, v.k FROM v) AS x WHERE x.rn <= 1 AND x.k IN (CAST('a' AS UUID), CAST('b' AS UUID)) AND x.a = 'q' AND x.k <> 'z' AND x.j IN (SELECT 1)";
        let after = w.find(" AS x").unwrap();
        assert_eq!(
            literal_predicates(w, after, "x"),
            vec![("k".to_string(), "CAST('a' AS UUID), CAST('b' AS UUID)".to_string()), ("a".to_string(), "'q'".to_string())]
        );
    }
}
