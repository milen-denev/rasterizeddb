//! `LATERAL`: a `FROM` item that reads the items before it.
//!
//! `CROSS JOIN LATERAL (SELECT … WHERE o.cid = c.id ORDER BY … LIMIT 2) o`,
//! `LEFT JOIN LATERAL (…) o ON true`, `, LATERAL generate_series(1, c.n) g`
//! and `CROSS JOIN LATERAL (VALUES (c.a), (c.b)) v(x)` were all refused — the
//! subquery as "a correlated scalar subquery … is not supported", the
//! function as a relation called `LATERAL`. The top-N-per-group query is the
//! usual reason anyone writes one.
//!
//! The item is materialised, as a derived table is (`cte::materialize_derived`),
//! once per distinct value of the outer columns it reads: those values are
//! substituted into the item as typed constants, the item is run, and its rows
//! are tagged with the values as keys (`__lat_k1`, …). The statement then joins
//! the materialised relation back on the keys, which gives every outer row the
//! rows its own values produce, and none of anyone else's:
//!
//! - `CROSS JOIN LATERAL` and `, LATERAL` become an inner join on the keys;
//! - `JOIN LATERAL … ON c` and `LEFT JOIN LATERAL … ON c` keep `c`, joined to
//!   the keys by `AND`, so a `LEFT` join's outer row whose item has no rows
//!   is kept with NULLs, as PostgreSQL keeps it.
//!
//! A key is the value's text, `'v' || x::text`, or `'n'` for NULL, so a NULL
//! outer value still joins back to the rows its item produced (a `count(*)`
//! over nothing is one row, not none) rather than to nothing.
//!
//! What is refused (0A000), rather than answered otherwise than PostgreSQL
//! would: outer references to more than one preceding item; `RIGHT` or
//! `FULL JOIN LATERAL` (PostgreSQL's own 42P10 for a reference); `*` over the
//! statement, which would show the keys; more than [`MAX_TUPLES`] distinct
//! outer values. An outer column the item names without its qualifier is
//! resolved inside the item, as an unqualified name is resolved first there;
//! one the item cannot resolve is an unknown column, where PostgreSQL would
//! have looked outward.

use rclite::Arc;

use crate::core::database::Database;
use crate::core::mvcc::transaction::TxId;
use crate::core::rql::join::find_keyword;

/// The most distinct outer values an item is run for: each is one run of the
/// item, so this bounds the statement's work.
pub(crate) const MAX_TUPLES: usize = 100_000;

/// How a `LATERAL` item is joined to the items before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Form {
    /// `, LATERAL …`
    Comma,
    /// `CROSS JOIN LATERAL …`
    Cross,
    /// `[INNER] JOIN LATERAL … ON c`
    Inner,
    /// `LEFT [OUTER] JOIN LATERAL … ON c`
    Left,
}

/// One `LATERAL` item as written.
#[derive(Debug, Clone)]
pub(crate) struct Item {
    pub form: Form,
    /// Where the join's own words begin (the `,`, or `CROSS`, `LEFT`, `JOIN`).
    pub start: usize,
    /// Just past the alias and its column list.
    pub end: usize,
    /// The item as a query: the subquery itself, or `SELECT * FROM` over the
    /// `VALUES` list or function call.
    pub body: String,
    pub alias: String,
    /// `v(x, y)`: the item's columns renamed.
    pub columns: Option<Vec<String>>,
    /// `Inner` and `Left`: the condition after `ON`, and where it ends.
    pub on: Option<(String, usize)>,
}

const NOT_ALIASES: &[&str] = &[
    "ON", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "NATURAL", "USING", "WHERE", "GROUP", "HAVING",
    "ORDER", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "WINDOW", "FETCH", "FOR", "LATERAL", "AS",
];

/// Clause words that end an `ON` condition, or a `FROM` list.
const CLAUSE_ENDS: &[&str] = &[
    "JOIN", "LEFT", "RIGHT", "INNER", "FULL", "CROSS", "NATURAL", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT",
    "OFFSET", "UNION", "INTERSECT", "EXCEPT", "WINDOW", "FETCH", "FOR", "RETURNING",
];

fn is_word(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// The identifier at `at` (bare or `"quoted"`) and where it ends.
fn identifier_at(sql: &str, at: usize) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    match bytes.get(at)? {
        b'"' => {
            let end = crate::core::sql_scan::quoted_end(bytes, at, b'"');
            Some((sql[at + 1..end.saturating_sub(1)].to_string(), end))
        }
        b if b.is_ascii_alphabetic() || *b == b'_' => {
            let mut end = at;
            while end < bytes.len() && (is_word(bytes[end]) || bytes[end] == b'.') {
                end += 1;
            }
            Some((sql[at..end].to_string(), end))
        }
        _ => None,
    }
}

fn skip_ws(sql: &str, at: usize) -> usize {
    at + sql[at..].len() - sql[at..].trim_start().len()
}

fn matching_paren(sql: &str, open: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            q @ (b'\'' | b'"') => {
                i = crate::core::sql_scan::quoted_end(bytes, i, q);
                continue;
            }
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// The word ending just before `at`, and where it starts.
fn word_before(sql: &str, at: usize) -> Option<(String, usize)> {
    let head = sql[..at].trim_end();
    let bytes = head.as_bytes();
    let mut start = bytes.len();
    while start > 0 && is_word(bytes[start - 1]) {
        start -= 1;
    }
    (start < bytes.len()).then(|| (head[start..].to_ascii_uppercase(), start))
}

/// Where a top-level clause word or `,` first appears in `sql` at or after
/// `from`, else its end.
fn clause_end(sql: &str, from: usize) -> usize {
    let mut end = sql.len();
    for word in CLAUSE_ENDS {
        if let Some(at) = find_keyword(sql, word, from) {
            end = end.min(at);
        }
    }
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut i = from;
    while i < end {
        match bytes[i] {
            q @ (b'\'' | b'"') => {
                i = crate::core::sql_scan::quoted_end(bytes, i, q);
                continue;
            }
            b'(' => depth += 1,
            b')' if depth == 0 => return i,
            b')' => depth -= 1,
            b',' | b';' if depth == 0 => return i,
            _ => {}
        }
        i += 1;
    }
    end
}

/// `sql` with `LATERAL` written before the `FROM` item at `start`: a
/// function in `FROM` may read the items before it without the word, as
/// PostgreSQL's may (there it is "a noise word"); the table-function pass
/// hands such an item here this way.
pub(crate) fn mark_implicit(sql: &str, start: usize) -> String {
    format!("{}LATERAL {}", &sql[..start], &sql[start..])
}

/// `sql` with each leading `FROM` function item that reads an earlier
/// item's column marked `LATERAL` ([`mark_implicit`]), for a describe, which
/// materialises nothing; `None` when there is none.
pub(crate) fn with_implicit_marked(sql: &str) -> Option<String> {
    let mut text: Option<String> = None;
    for _ in 0..16usize {
        let current = text.as_deref().unwrap_or(sql);
        let Some(item) = crate::core::rql::table_functions::next_item(current) else { break };
        match crate::core::rql::table_functions::columns_of(&item) {
            Err(e) if is_outer_reference_refusal(&e) => text = Some(mark_implicit(current, item.start)),
            _ => break,
        }
    }
    text
}

/// Whether a table function's refusal is for reading another relation's
/// column, which [`mark_implicit`] makes a `LATERAL` item of.
pub(crate) fn is_outer_reference_refusal(message: &str) -> bool {
    message.contains("a LATERAL reference")
}

/// The first top-level `LATERAL` item of `sql`, `None` when it has none.
pub(crate) fn next_item(sql: &str) -> Option<Result<Item, String>> {
    let at = find_keyword(sql, "LATERAL", 0)?;
    Some(parse_item(sql, at))
}

fn parse_item(sql: &str, at: usize) -> Result<Item, String> {
    let unsupported = |what: &str| Err(format!("{what} is not supported yet"));
    // What joins it: `,`, or the words of a JOIN.
    let before = sql[..at].trim_end();
    let (form, start) = if before.ends_with(',') {
        (Form::Comma, before.len() - 1)
    } else {
        match word_before(sql, at) {
            Some((w, join_at)) if w == "JOIN" => match word_before(sql, join_at) {
                Some((w, s)) if w == "CROSS" => (Form::Cross, s),
                Some((w, s)) if w == "LEFT" => (Form::Left, s),
                Some((w, s)) if w == "OUTER" => match word_before(sql, s) {
                    Some((w, s)) if w == "LEFT" => (Form::Left, s),
                    _ => return unsupported("RIGHT or FULL JOIN LATERAL"),
                },
                Some((w, _)) if w == "RIGHT" || w == "FULL" => return unsupported("RIGHT or FULL JOIN LATERAL"),
                Some((w, s)) if w == "INNER" => (Form::Inner, s),
                _ => (Form::Inner, join_at),
            },
            _ => return unsupported("LATERAL outside a FROM list"),
        }
    };

    // The item: `( SELECT … )`, `( VALUES … )` or `name(args)`.
    let mut i = skip_ws(sql, at + "LATERAL".len());
    let (body_text, kind) = if sql.as_bytes().get(i) == Some(&b'(') {
        let close = matching_paren(sql, i).ok_or("LATERAL: unbalanced parentheses")?;
        let inner = sql[i + 1..close].trim().to_string();
        i = close + 1;
        let head = inner.split_whitespace().next().unwrap_or("").to_ascii_uppercase();
        match head.as_str() {
            "SELECT" | "WITH" => (inner, 's'),
            "VALUES" => (inner, 'v'),
            _ => return unsupported("this LATERAL item"),
        }
    } else {
        let (name, after) = identifier_at(sql, i).ok_or("LATERAL needs a subquery or a function call")?;
        let open = skip_ws(sql, after);
        if sql.as_bytes().get(open) != Some(&b'(') {
            return Err(format!("LATERAL {name}: a table is not a LATERAL item; write it without LATERAL"));
        }
        let close = matching_paren(sql, open).ok_or("LATERAL: unbalanced parentheses")?;
        let mut call_end = close + 1;
        // `WITH ORDINALITY` belongs to the call, not to the alias.
        let w = skip_ws(sql, call_end);
        if find_keyword(&sql[..sql.len()], "WITH", w) == Some(w) {
            let o = skip_ws(sql, w + 4);
            if sql.get(o..o + 10).is_some_and(|t| t.eq_ignore_ascii_case("ORDINALITY")) {
                call_end = o + 10;
            }
        }
        let call = sql[i..call_end].to_string();
        i = call_end;
        (call, 'f')
    };

    // `[AS] alias [(columns)]`.
    let mut j = skip_ws(sql, i);
    if sql[j..].len() >= 2 && sql[j..j + 2].eq_ignore_ascii_case("AS") && sql.as_bytes().get(j + 2).is_some_and(|b| !is_word(*b)) {
        j = skip_ws(sql, j + 2);
    }
    let (alias, mut end) = match identifier_at(sql, j) {
        Some((name, e)) if !NOT_ALIASES.iter().any(|w| w.eq_ignore_ascii_case(&name)) => (name, e),
        _ if kind == 'f' => {
            // A function's alias defaults to its name.
            let name = body_text.split('(').next().unwrap_or("").trim().to_string();
            (name, i)
        }
        _ => return Err("subquery in FROM must have an alias".to_string()),
    };
    let mut columns: Option<Vec<String>> = None;
    let k = skip_ws(sql, end);
    if end > i && sql.as_bytes().get(k) == Some(&b'(') {
        let close = matching_paren(sql, k).ok_or("LATERAL: unbalanced parentheses")?;
        columns = Some(sql[k + 1..close].split(',').map(|c| c.trim().trim_matches('"').to_string()).collect());
        end = close + 1;
    }

    // `ON c` for the joins that take one.
    let on = match form {
        Form::Inner | Form::Left => {
            let o = skip_ws(sql, end);
            if !(sql[o..].len() >= 2 && sql[o..o + 2].eq_ignore_ascii_case("ON")) {
                return Err("JOIN LATERAL needs an ON condition".to_string());
            }
            let from = o + 2;
            let stop = clause_end(sql, from);
            Some((sql[from..stop].trim().to_string(), stop))
        }
        _ => None,
    };

    let alias_list = columns.as_ref().map(|c| format!("({})", c.join(", "))).unwrap_or_default();
    let body = match kind {
        's' => body_text,
        'v' => format!("SELECT * FROM ({body_text}) AS {alias}{alias_list}"),
        _ => format!("SELECT * FROM {body_text} AS {alias}{alias_list}"),
    };
    // The renaming is in the body's own text for the computed forms.
    let columns = if kind == 's' { columns } else { None };
    Ok(Item { form, start, end, body, alias, columns, on })
}

/// The `FROM` items before `upto`: each alias (or bare relation name) and the
/// relation text it names.
fn items_before(sql: &str, upto: usize) -> Vec<(String, String)> {
    let Some(from) = find_keyword(&sql[..upto], "FROM", 0) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut i = from + "FROM".len();
    let bytes = sql.as_bytes();
    loop {
        i = skip_ws(sql, i);
        if i >= upto {
            break;
        }
        // The relation: a name, or a parenthesised derived table.
        let (relation, after) = if bytes[i] == b'(' {
            let Some(close) = matching_paren(sql, i) else { break };
            (sql[i..close + 1].to_string(), close + 1)
        } else if let Some((name, after)) = identifier_at(sql, i) {
            if name.eq_ignore_ascii_case("LATERAL") {
                break;
            }
            (name, after)
        } else {
            break;
        };
        let mut j = skip_ws(sql, after);
        if sql[j..].len() >= 2 && sql[j..j + 2].eq_ignore_ascii_case("AS") && bytes.get(j + 2).is_some_and(|b| !is_word(*b)) {
            j = skip_ws(sql, j + 2);
        }
        let alias = match identifier_at(sql, j) {
            Some((a, e)) if !NOT_ALIASES.iter().any(|w| w.eq_ignore_ascii_case(&a)) && !a.contains('.') => {
                j = e;
                a
            }
            _ => relation.rsplit('.').next().unwrap_or(&relation).to_string(),
        };
        out.push((alias, relation));
        // Past an `ON`/`USING` condition to the next item: a `,` or a JOIN.
        let mut k = j;
        let next = loop {
            k = skip_ws(sql, k);
            if k >= upto {
                break None;
            }
            if bytes[k] == b',' {
                break Some(k + 1);
            }
            if let Some(join) = find_keyword(&sql[..upto], "JOIN", k) {
                // Only a JOIN reached before the next `,` at this level.
                let comma = sql[k..join].find(',').filter(|c| {
                    let segment = &sql[k..k + c];
                    segment.matches('(').count() == segment.matches(')').count()
                });
                break Some(match comma {
                    Some(c) => k + c + 1,
                    None => join + "JOIN".len(),
                });
            }
            break None;
        };
        match next {
            Some(n) => i = n,
            None => break,
        }
    }
    out
}

/// The qualified references `X.col` in `body` (outside quotes, not a call,
/// not `X.*`), in order of first appearance, each once.
fn qualified_references(body: &str) -> Vec<(String, String)> {
    let bytes = body.as_bytes();
    let mut out: Vec<(String, String)> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            q @ (b'\'' | b'"') if q == b'\'' => {
                i = crate::core::sql_scan::quoted_end(bytes, i, q);
            }
            b if (b.is_ascii_alphabetic() || b == b'_') && (i == 0 || !(is_word(bytes[i - 1]) || bytes[i - 1] == b'.')) => {
                let start = i;
                while i < bytes.len() && is_word(bytes[i]) {
                    i += 1;
                }
                if bytes.get(i) == Some(&b'.') && bytes.get(i + 1).is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_') {
                    let qualifier = &body[start..i];
                    let c0 = i + 1;
                    let mut c1 = c0;
                    while c1 < bytes.len() && is_word(bytes[c1]) {
                        c1 += 1;
                    }
                    let called = body[c1..].trim_start().starts_with('(');
                    let column = &body[c0..c1];
                    if !called
                        && !out.iter().any(|(q, c)| q.eq_ignore_ascii_case(qualifier) && c.eq_ignore_ascii_case(column))
                    {
                        out.push((qualifier.to_string(), column.to_string()));
                    }
                    i = c1;
                }
            }
            _ => i += 1,
        }
    }
    out
}

/// The outer item the body reads, and the columns it reads of it: `None`
/// when it reads none. Refused when it reads two.
pub(crate) fn outer_references(sql: &str, item: &Item) -> Result<Option<(String, String, Vec<String>)>, String> {
    let outer = items_before(sql, item.start);
    // The body's own items shadow an outer alias, as names resolve inside it
    // first.
    let inner: Vec<String> = match find_keyword(&item.body, "FROM", 0) {
        Some(_) => items_before(&item.body, clause_end_of_from(&item.body)).into_iter().map(|(a, _)| a).collect(),
        None => Vec::new(),
    };
    let mut found: Option<(String, String, Vec<String>)> = None;
    for (qualifier, column) in qualified_references(&item.body) {
        if inner.iter().any(|a| a.eq_ignore_ascii_case(&qualifier)) {
            continue;
        }
        let Some((alias, relation)) = outer.iter().find(|(a, _)| a.eq_ignore_ascii_case(&qualifier)) else {
            continue;
        };
        match &mut found {
            None => found = Some((alias.clone(), relation.clone(), vec![column])),
            Some((a, _, columns)) if a.eq_ignore_ascii_case(alias) => {
                if !columns.iter().any(|c| c.eq_ignore_ascii_case(&column)) {
                    columns.push(column);
                }
            }
            Some((a, _, _)) => {
                return Err(format!(
                    "a LATERAL item reading two of the items before it ({a} and {alias}) is not supported yet"
                ))
            }
        }
    }
    Ok(found)
}

/// Where the body's top-level `FROM` list ends.
fn clause_end_of_from(body: &str) -> usize {
    let mut end = body.len();
    for word in ["WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "WINDOW", "FETCH"] {
        if let Some(at) = find_keyword(body, word, 0) {
            end = end.min(at);
        }
    }
    end
}

/// `body` with every `alias.column` of `columns` replaced by its constant.
pub(crate) fn substitute(body: &str, alias: &str, columns: &[String], constants: &[String]) -> String {
    let bytes = body.as_bytes();
    let mut out = String::with_capacity(body.len() + 32);
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                let end = crate::core::sql_scan::quoted_end(bytes, i, b'\'');
                out.push_str(&body[i..end]);
                i = end;
            }
            b if (b.is_ascii_alphabetic() || b == b'_') && (i == 0 || !(is_word(bytes[i - 1]) || bytes[i - 1] == b'.')) => {
                let start = i;
                while i < bytes.len() && is_word(bytes[i]) {
                    i += 1;
                }
                if body[start..i].eq_ignore_ascii_case(alias) && bytes.get(i) == Some(&b'.') {
                    let c0 = i + 1;
                    let mut c1 = c0;
                    while c1 < bytes.len() && is_word(bytes[c1]) {
                        c1 += 1;
                    }
                    if let Some(at) = columns.iter().position(|c| c.eq_ignore_ascii_case(&body[c0..c1])) {
                        if !body[c1..].trim_start().starts_with('(') {
                            out.push_str(&constants[at]);
                            i = c1;
                            continue;
                        }
                    }
                }
                out.push_str(&body[start..i]);
            }
            _ => {
                let ch = body[i..].chars().next().expect("in bounds");
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }
    out
}

/// `text` as a SQL string literal.
fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// The key a value joins back on: its text marked present, or `'n'`.
fn key_of(text: Option<&str>) -> String {
    match text {
        Some(t) => format!("v{t}"),
        None => "n".to_string(),
    }
}

/// The SQL type a column of `relation` has, as a cast can name it.
async fn column_types(
    database: &Arc<Database>,
    alias: &str,
    relation: &str,
    columns: &[String],
) -> Result<Vec<String>, String> {
    let probe = format!(
        "SELECT {} FROM {relation} {alias}",
        columns.iter().map(|c| format!("{alias}.{c}")).collect::<Vec<_>>().join(", ")
    );
    let described = Box::pin(crate::pgwire::describe_query(database, &probe))
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("LATERAL: the columns {} of {alias} cannot be typed", columns.join(", ")))?;
    described
        .0
        .iter()
        .map(|(_, ty)| {
            crate::core::rql::cte::sql_type_name(ty)
                .map(str::to_string)
                .ok_or_else(|| format!("LATERAL: a {ty:?} outer column is not supported yet"))
        })
        .collect()
}

/// The body with each outer reference a NULL of its type: what the item's
/// columns are described from, without running it.
pub(crate) async fn null_body(database: &Arc<Database>, item: &Item, outer: &(String, String, Vec<String>)) -> Result<String, String> {
    let (alias, relation, columns) = outer;
    let types = column_types(database, alias, relation, columns).await?;
    let nulls: Vec<String> = types.iter().map(|t| format!("CAST(NULL AS {t})")).collect();
    Ok(substitute(&item.body, alias, columns, &nulls))
}

/// The statement with the item replaced by `relation` under its alias, joined
/// as it was: on the keys for a materialised one (`keys` > 0), on its own
/// condition alone when it reads nothing outside.
pub(crate) fn rewrite(sql: &str, item: &Item, relation: &str, outer_alias: Option<&str>, outer_columns: &[String]) -> String {
    let keys: Vec<String> = outer_columns
        .iter()
        .enumerate()
        .map(|(n, c)| {
            format!(
                "{a}.__lat_k{} = coalesce('v' || {o}.{c}::text, 'n')",
                n + 1,
                a = item.alias,
                o = outer_alias.unwrap_or_default()
            )
        })
        .collect();
    let keys = keys.join(" AND ");
    let (end, joined) = match (&item.form, &item.on) {
        // Each ends in a space: what follows may be the next join's word,
        // which the `ON` it replaced ran up to.
        (Form::Comma, _) if keys.is_empty() => (item.end, format!(", {relation} {} ", item.alias)),
        (Form::Cross, _) if keys.is_empty() => (item.end, format!(" CROSS JOIN {relation} {} ", item.alias)),
        (Form::Comma | Form::Cross, _) => (item.end, format!(" JOIN {relation} {} ON {keys} ", item.alias)),
        (form, Some((condition, stop))) => {
            let join = if *form == Form::Left { "LEFT JOIN" } else { "JOIN" };
            // `ON true`, the usual LATERAL condition, says nothing the keys do
            // not: the join refuses a term that reads no column.
            let trivial = trivial_condition(condition);
            let on = match (keys.is_empty(), trivial) {
                (false, true) => keys.clone(),
                (false, false) => format!("{keys} AND ({condition})"),
                (true, _) => condition.clone(),
            };
            (*stop, format!(" {join} {relation} {} ON {on} ", item.alias))
        }
        (_, None) => (item.end, String::new()),
    };
    format!("{}{}{}", &sql[..item.start], joined, &sql[end..])
}

/// Whether an `ON` condition is `true`, which arrives as `(1=1)`
/// (`sql_scan`'s rewrite of a bare boolean).
fn trivial_condition(condition: &str) -> bool {
    let bare: String = condition.chars().filter(|c| !c.is_whitespace() && *c != '(' && *c != ')').collect();
    bare.eq_ignore_ascii_case("true") || bare == "1=1"
}

/// The item as a `CROSS JOIN`, its `ON` dropped, when it is `JOIN … ON true`
/// or `LEFT JOIN … ON true`: a join refuses a term that reads no column, and
/// without outer keys there is nothing else to join on.
fn as_cross_join(item: &Item) -> Option<Item> {
    let (condition, stop) = item.on.as_ref()?;
    if !trivial_condition(condition) {
        return None;
    }
    Some(Item { form: Form::Cross, end: *stop, on: None, ..item.clone() })
}

/// [`rewrite`] for a describe: the item a relation of its columns, joined as
/// written but without keys, a trivial `ON` as a `CROSS JOIN`.
pub(crate) fn rewrite_for_describe(sql: &str, item: &Item, relation: &str) -> String {
    match as_cross_join(item) {
        Some(cross) => rewrite(sql, &cross, relation, None, &[]),
        None => rewrite(sql, item, relation, None, &[]),
    }
}

/// Whether the statement's select list shows every column with `*`: over a
/// materialised item it would show the keys too.
fn selects_star(sql: &str, item: &Item) -> bool {
    let Some(from) = find_keyword(&sql[..item.start], "FROM", 0) else { return false };
    let list = &sql[..from];
    let mut depth = 0usize;
    let bytes = list.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            q @ (b'\'' | b'"') => {
                i = crate::core::sql_scan::quoted_end(bytes, i, q);
                continue;
            }
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b'*' if depth == 0 => {
                let before = list[..i].trim_end();
                let star = before.ends_with(',')
                    || before.to_ascii_uppercase().ends_with("SELECT")
                    || before.to_ascii_uppercase().ends_with("DISTINCT")
                    || before.to_ascii_lowercase().ends_with(&format!("{}.", item.alias.to_ascii_lowercase()));
                if star {
                    return true;
                }
            }
            _ => {}
        }
        i += 1;
    }
    false
}

/// Materialises every `LATERAL` item of `sql` (module docs), creating the
/// temporaries it names in `temps`, and returns the statement rewritten to
/// join them; `None` when it has none.
pub(crate) async fn materialize(
    sql: &str,
    database: &Arc<Database>,
    temps: &mut Vec<String>,
    tx: TxId,
) -> Result<Option<String>, String> {
    let mut current = sql.to_string();
    let mut found = false;
    for index in 0..16usize {
        let Some(item) = next_item(&current) else { break };
        let item = item?;
        found = true;
        let outer = outer_references(&current, &item)?;
        if outer.is_some() && selects_star(&current, &item) {
            return Err(format!(
                "SELECT * over the LATERAL item {} is not supported yet; name the columns",
                item.alias
            ));
        }
        let temp = crate::core::rql::cte::temp_name_for(&item.alias, &current, 200 + index);
        match &outer {
            None => {
                let built = crate::core::rql::cte::materialize_query(&item.body, &item.alias, &current, database, tx).await?;
                temps.push(built.clone());
                let renamed = rename_columns(&item, &built, database, tx).await?;
                if renamed != built {
                    temps.push(renamed.clone());
                }
                match as_cross_join(&item) {
                    Some(cross) => {
                        // `LEFT JOIN … ON true` over an item with no rows keeps
                        // every outer row with NULLs: the same item for each, so
                        // one row of NULLs, crossed, is that.
                        if item.form == Form::Left {
                            fill_one_null_row(&renamed, database, tx).await?;
                        }
                        current = rewrite(&current, &cross, &renamed, None, &[]);
                    }
                    None => current = rewrite(&current, &item, &renamed, None, &[]),
                }
            }
            Some(outer) => {
                let (alias, relation, columns) = outer;
                // The distinct outer values, as text: what is substituted and
                // what the keys are.
                let key_query = format!(
                    "SELECT DISTINCT {} FROM {relation} {alias}",
                    columns.iter().map(|c| format!("{alias}.{c}::text")).collect::<Vec<_>>().join(", ")
                );
                let tuples = text_rows(&key_query, columns.len(), database, tx).await?;
                if tuples.len() > MAX_TUPLES {
                    return Err(format!(
                        "LATERAL item {} would run for {} distinct outer values, more than the {MAX_TUPLES} this \
                         supports",
                        item.alias,
                        tuples.len()
                    ));
                }
                let types = column_types(database, alias, relation, columns).await?;
                let described = null_body(database, &item, outer).await?;
                let mut runs = Vec::with_capacity(tuples.len());
                for tuple in &tuples {
                    let constants: Vec<String> = tuple
                        .iter()
                        .zip(&types)
                        .map(|(v, t)| match v {
                            Some(v) => format!("CAST({} AS {t})", quote(v)),
                            None => format!("CAST(NULL AS {t})"),
                        })
                        .collect();
                    let body = substitute(&item.body, alias, columns, &constants);
                    let keys: Vec<String> = tuple.iter().map(|v| key_of(v.as_deref())).collect();
                    runs.push((body, keys));
                }
                let labels = item.columns.clone();
                crate::core::rql::cte::build_keyed_relation(
                    &described,
                    &temp,
                    labels.as_deref(),
                    &runs,
                    &item.alias,
                    database,
                    tx,
                    temps,
                )
                .await?;
                current = rewrite(&current, &item, &temp, Some(alias), columns);
                // The keys are expressions over the outer item, which a join
                // takes as a derived column (`join_on`). The executor's own
                // pass ran before this relation existed.
                let columns_of = |name: &str| crate::core::rql::cte::relation_labels(name, database);
                if let Some(joined) = crate::core::rql::join_on::rewrite_statement(&current, &columns_of) {
                    current = joined;
                }
                // That derived table, materialised now: the next item may
                // join to it, and `join_on` reads relations only.
                for _ in 0..16usize {
                    let Some((start, end, inner, derived)) = crate::core::rql::cte::next_derived_table(&current) else {
                        break;
                    };
                    let built = crate::core::rql::cte::materialize_query(&inner, &derived, &current, database, tx).await?;
                    temps.push(built.clone());
                    current = format!("{}{} {}{}", &current[..start], built, derived, &current[end..]);
                }
            }
        }
    }
    Ok(found.then_some(current))
}

/// A row of NULLs in `relation` when it has no rows.
async fn fill_one_null_row(relation: &str, database: &Arc<Database>, tx: TxId) -> Result<(), String> {
    let (rows, _) = crate::core::rql::executor::run_query_rows_scaled(&format!("SELECT count(*) FROM {relation}"), database, tx).await?;
    let empty = rows
        .first()
        .and_then(|r| r.columns.first())
        .and_then(|c| <[u8; 8]>::try_from(c.data.into_slice()).ok())
        .is_some_and(|b| i64::from_le_bytes(b) == 0);
    if !empty {
        return Ok(());
    }
    let labels = crate::core::rql::cte::relation_labels(relation, database)
        .ok_or_else(|| format!("LATERAL: {relation} cannot be read"))?;
    let nulls = vec!["NULL"; labels.len()].join(", ");
    crate::core::rql::executor::run_statement(
        &format!("INSERT INTO {relation} ({}) VALUES ({nulls})", labels.join(", ")),
        database,
        tx,
    )
    .await
}

/// The first `width` cells of each row `query` returns, as text: it selects
/// `::text` values only. `None` for a NULL.
async fn text_rows(query: &str, width: usize, database: &Arc<Database>, tx: TxId) -> Result<Vec<Vec<Option<String>>>, String> {
    let (rows, _) = crate::core::rql::executor::run_query_rows_scaled(query, database, tx).await?;
    Ok(rows
        .iter()
        .map(|row| {
            (0..width)
                .map(|i| match row.columns.get(i) {
                    Some(c) if c.column_type != crate::core::db_type::DbType::NULL => {
                        Some(String::from_utf8_lossy(c.data.into_slice()).into_owned())
                    }
                    _ => None,
                })
                .collect()
        })
        .collect())
}

/// A materialised item whose columns the alias renames, `(…) s(a, b)`, read
/// through a relation of its own under the new names. The body's names
/// otherwise.
async fn rename_columns(item: &Item, built: &str, database: &Arc<Database>, tx: TxId) -> Result<String, String> {
    let Some(names) = &item.columns else { return Ok(built.to_string()) };
    let labels = crate::core::rql::cte::relation_labels(built, database)
        .ok_or_else(|| format!("LATERAL item {}: its columns cannot be read", item.alias))?;
    if names.len() > labels.len() {
        return Err(format!(
            "table \"{}\" has {} columns available but {} columns specified",
            item.alias,
            labels.len(),
            names.len()
        ));
    }
    let list: Vec<String> = labels
        .iter()
        .enumerate()
        .map(|(n, l)| match names.get(n) {
            Some(new) => format!("{l} AS {new}"),
            None => l.clone(),
        })
        .collect();
    let query = format!("SELECT {} FROM {built}", list.join(", "));
    crate::core::rql::cte::materialize_query(&query, &item.alias, built, database, tx).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_form_is_read_with_its_alias() {
        let sql = "SELECT c.name, o.total FROM lc c CROSS JOIN LATERAL (SELECT total FROM lo WHERE lo.cid = c.id \
                   ORDER BY total DESC LIMIT 2) o ORDER BY 1";
        let item = next_item(sql).unwrap().unwrap();
        assert_eq!(item.form, Form::Cross);
        assert_eq!(item.alias, "o");
        assert!(item.body.starts_with("SELECT total FROM lo"));
        assert_eq!(&sql[item.start..item.start + 5], "CROSS");
        assert_eq!(sql[item.end..].trim_start(), "ORDER BY 1");

        let sql = "SELECT * FROM lc c LEFT OUTER JOIN LATERAL (SELECT 1 AS x) s ON true WHERE c.id > 1";
        let item = next_item(sql).unwrap().unwrap();
        assert_eq!(item.form, Form::Left);
        assert_eq!(item.on.as_ref().map(|(c, _)| c.as_str()), Some("true"));
        assert_eq!(sql[item.on.as_ref().unwrap().1..].trim_start(), "WHERE c.id > 1");

        let sql = "SELECT c.name, x FROM lc c, LATERAL generate_series(1, c.id) x ORDER BY 1";
        let item = next_item(sql).unwrap().unwrap();
        assert_eq!(item.form, Form::Comma);
        assert_eq!(item.body, "SELECT * FROM generate_series(1, c.id) AS x");

        let sql = "SELECT v FROM lc c CROSS JOIN LATERAL (VALUES (c.id * 10), (c.id)) t(v)";
        let item = next_item(sql).unwrap().unwrap();
        assert_eq!(item.body, "SELECT * FROM (VALUES (c.id * 10), (c.id)) AS t(v)");
        assert!(item.columns.is_none());

        assert!(next_item("SELECT * FROM a RIGHT JOIN LATERAL (SELECT 1) s ON true").unwrap().is_err());
        assert!(next_item("SELECT lateral FROM t").is_none() || next_item("SELECT lateral FROM t").unwrap().is_err());
    }

    #[test]
    fn outer_references_are_the_preceding_items_columns() {
        let sql = "SELECT 1 FROM lc AS c JOIN other d ON d.id = c.id CROSS JOIN LATERAL \
                   (SELECT total FROM lo WHERE lo.cid = c.id AND lo.region = c.region AND lo.x = 'c.id') o";
        let item = next_item(sql).unwrap().unwrap();
        let (alias, relation, columns) = outer_references(sql, &item).unwrap().unwrap();
        assert_eq!((alias.as_str(), relation.as_str()), ("c", "lc"));
        assert_eq!(columns, ["id", "region"]);

        // Two outer items: refused.
        let sql = "SELECT 1 FROM a x, b y, LATERAL (SELECT 1 FROM t WHERE t.a = x.a AND t.b = y.b) s";
        let item = next_item(sql).unwrap().unwrap();
        assert!(outer_references(sql, &item).is_err());

        // The body's own alias shadows an outer one.
        let sql = "SELECT 1 FROM t c, LATERAL (SELECT c.n FROM u c) s";
        let item = next_item(sql).unwrap().unwrap();
        assert!(outer_references(sql, &item).unwrap().is_none());
    }

    #[test]
    fn substitution_and_rewrite() {
        let body = "SELECT total FROM lo WHERE lo.cid = c.id AND note = 'c.id' AND c.idx = 1";
        assert_eq!(
            substitute(body, "c", &["id".to_string()], &["CAST('7' AS INTEGER)".to_string()]),
            "SELECT total FROM lo WHERE lo.cid = CAST('7' AS INTEGER) AND note = 'c.id' AND c.idx = 1"
        );
        let sql = "SELECT c.name, o.total FROM lc c LEFT JOIN LATERAL (SELECT 1 AS total) o ON o.total > 0 OR c.id = 1 ORDER BY 1";
        let item = next_item(sql).unwrap().unwrap();
        let words = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
        assert_eq!(
            words(&rewrite(sql, &item, "__t", Some("c"), &["id".to_string()])),
            "SELECT c.name, o.total FROM lc c LEFT JOIN __t o ON o.__lat_k1 = coalesce('v' || c.id::text, 'n') AND \
             (o.total > 0 OR c.id = 1) ORDER BY 1"
        );
        assert_eq!(key_of(None), "n");
        assert_eq!(key_of(Some("12")), "v12");
    }
}
