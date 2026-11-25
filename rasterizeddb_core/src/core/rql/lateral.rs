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
    /// A function call's or `VALUES` list's arguments, as a span of `body`:
    /// text with no relation of its own, where a bare name can only be an
    /// earlier item's column ([`qualify_outer_columns`]). `None` for a
    /// subquery.
    pub args: Option<(usize, usize)>,
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

/// Whether `word` (ASCII) is spelled at byte `at`, in any case. A
/// multibyte character there is not it, rather than a cut through it.
fn word_at(sql: &str, at: usize, word: &str) -> bool {
    sql.get(at..at + word.len()).is_some_and(|t| t.eq_ignore_ascii_case(word))
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
    let mut first = false;
    let (form, start) = if before.ends_with(',') {
        (Form::Comma, before.len() - 1)
    } else if word_before(sql, at).is_some_and(|(w, _)| w == "FROM") {
        // Nothing before it to read. Written so, or marked so (`FROM
        // unnest(r)`, whose argument is no constant): what it names is no
        // relation's (below).
        first = true;
        (Form::Comma, at)
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
    // The arguments' span within `body_text`.
    let mut args_span: Option<(usize, usize)> = None;
    let (body_text, kind) = if sql.as_bytes().get(i) == Some(&b'(') {
        let close = matching_paren(sql, i).ok_or("LATERAL: unbalanced parentheses")?;
        let inner = sql[i + 1..close].trim().to_string();
        i = close + 1;
        let head = inner.split_whitespace().next().unwrap_or("").to_ascii_uppercase();
        match head.as_str() {
            "SELECT" | "WITH" => (inner, 's'),
            "VALUES" => {
                args_span = Some((0, inner.len()));
                (inner, 'v')
            }
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
        args_span = Some((open + 1 - i, close - i));
        i = call_end;
        (call, 'f')
    };

    // `[AS] alias [(columns)]`.
    let mut j = skip_ws(sql, i);
    if word_at(sql, j, "AS") && sql.as_bytes().get(j + 2).is_some_and(|b| !is_word(*b)) {
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
            if !word_at(sql, o, "ON") {
                return Err("JOIN LATERAL needs an ON condition".to_string());
            }
            let from = o + 2;
            let stop = clause_end(sql, from);
            Some((sql[from..stop].trim().to_string(), stop))
        }
        _ => None,
    };

    let alias_list = columns.as_ref().map(|c| format!("({})", c.join(", "))).unwrap_or_default();
    let (body, prefix) = match kind {
        's' => (body_text, 0),
        'v' => (format!("SELECT * FROM ({body_text}) AS {alias}{alias_list}"), "SELECT * FROM (".len()),
        _ => (format!("SELECT * FROM {body_text} AS {alias}{alias_list}"), "SELECT * FROM ".len()),
    };
    let args = args_span.map(|(a, b)| (a + prefix, b + prefix));
    if first {
        // PostgreSQL's answer for the name no relation has.
        let arguments = args.map(|(a, b)| &body[a..b]).unwrap_or_default();
        return Err(match (bare_names(arguments).into_iter().next(), qualified_references(arguments).into_iter().next()) {
            (Some((name, _)), _) => format!("column \"{name}\" does not exist"),
            (None, Some((qualifier, _))) => format!("missing FROM-clause entry for table \"{qualifier}\""),
            (None, None) => "LATERAL as the first FROM item is not supported yet".to_string(),
        });
    }
    // The renaming is in the body's own text for the computed forms.
    let columns = if kind == 's' { columns } else { None };
    Ok(Item { form, start, end, body, alias, columns, on, args })
}

/// The names in `args` (a function's arguments, a `VALUES` list) that can
/// only be columns: bare words outside quotes, not qualified (`x.c`) or a
/// qualifier, not a call (`f(`), not a cast's or a typed literal's type
/// (`::int`, `AS int`, `DATE '…'`), not a keyword. Each with its offset.
fn bare_names(args: &str) -> Vec<(String, usize)> {
    const KEYWORDS: &[&str] = &[
        "AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "IS", "IN", "AS", "CASE", "WHEN", "THEN", "ELSE", "END", "ARRAY",
        "CAST", "DISTINCT", "BETWEEN", "LIKE", "ILIKE", "SIMILAR", "ESCAPE", "ANY", "ALL", "SOME", "VALUES", "WITH",
        "ORDINALITY", "INTERVAL", "COLLATE", "AT", "ZONE", "UNKNOWN", "FROM", "FOR", "SYMMETRIC", "ISNULL", "NOTNULL",
        "LOCAL", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "LOCALTIME", "LOCALTIMESTAMP", "CURRENT_USER",
        "SESSION_USER", "USER", "DEFAULT", "OVERLAPS", "EXISTS", "ROW",
    ];
    let bytes = args.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    // Inside a cast's type: after `::`, or `AS` within `CAST(`.
    let mut in_type = false;
    while i < bytes.len() {
        match bytes[i] {
            q @ (b'\'' | b'"') => {
                i = crate::core::sql_scan::quoted_end(bytes, i, q);
                in_type = false;
            }
            b':' if bytes.get(i + 1) == Some(&b':') => {
                in_type = true;
                i += 2;
            }
            b if b.is_ascii_alphabetic() || b == b'_' => {
                let start = i;
                while i < bytes.len() && is_word(bytes[i]) {
                    i += 1;
                }
                let word = &args[start..i];
                let after = skip_ws(args, i);
                let qualified = (start > 0 && bytes[start - 1] == b'.') || bytes.get(i) == Some(&b'.');
                let called = bytes.get(after) == Some(&b'(');
                let typed_literal = bytes.get(after) == Some(&b'\'');
                let preceded_by_digit = start > 0 && bytes[start - 1].is_ascii_digit();
                if word.eq_ignore_ascii_case("AS") {
                    in_type = true;
                } else if !(in_type
                    || qualified
                    || called
                    || typed_literal
                    || preceded_by_digit
                    || KEYWORDS.iter().any(|k| k.eq_ignore_ascii_case(word)))
                {
                    out.push((word.to_string(), start));
                }
            }
            b if b.is_ascii_whitespace() || b == b'[' || b == b']' || b.is_ascii_digit() => i += 1,
            b'(' if in_type => {
                // A type modifier: `numeric(10, 2)`.
                i = matching_paren(args, i).map_or(bytes.len(), |c| c + 1);
            }
            _ => {
                in_type = false;
                i += 1;
            }
        }
    }
    out
}

/// `item` with each bare name in its arguments ([`Item::args`]) that is a
/// column of exactly one earlier `FROM` item written `alias.name`, as
/// PostgreSQL resolves it: a function's arguments and a `VALUES` list have no
/// relation of their own, so the name can only be one of those. A name two
/// earlier items have is ambiguous, as there. `item` as it was when it has no
/// arguments, or reads nothing bare.
pub(crate) async fn qualify_outer_columns(sql: &str, item: &Item, database: &Arc<Database>) -> Result<Item, String> {
    let Some((a, b)) = item.args else { return Ok(item.clone()) };
    let names = bare_names(&item.body[a..b]);
    if names.is_empty() {
        return Ok(item.clone());
    }
    let mut outer_columns: Vec<(String, Vec<String>)> = Vec::new();
    for (alias, relation) in items_before(sql, item.start) {
        let probe = format!("SELECT * FROM {relation} {alias}");
        if let Ok(Some((fields, _))) = Box::pin(crate::pgwire::describe_query(database, &probe)).await {
            outer_columns.push((alias, fields.into_iter().map(|(name, _)| name).collect()));
        }
    }
    let mut body = item.body.clone();
    let mut shift = 0usize;
    for (name, offset) in names {
        let owners: Vec<&String> = outer_columns
            .iter()
            .filter(|(_, columns)| columns.iter().any(|c| c.eq_ignore_ascii_case(&name)))
            .map(|(alias, _)| alias)
            .collect();
        match owners.as_slice() {
            [] => {}
            [alias] => {
                let at = a + offset + shift;
                let qualifier = format!("{alias}.");
                body.insert_str(at, &qualifier);
                shift += qualifier.len();
            }
            _ => return Err(format!("column reference \"{name}\" is ambiguous")),
        }
    }
    let args = Some((a, b + shift));
    Ok(Item { body, args, ..item.clone() })
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
        if word_at(sql, j, "AS") && bytes.get(j + 2).is_some_and(|b| !is_word(*b)) {
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

/// An item reading two outer relations an INNER JOIN before it keys
/// together (`FROM v d JOIN p d0 ON d.pid = d0.id`, a subquery reading
/// `d0.id` and `d.id`): each reference to one side's key column is the
/// other side's, as every joined row has them equal, so the item reads one
/// relation (EF Core's correlated subqueries over a join). Only keys that
/// compare by their bytes (no text: a case-insensitive key's two sides may
/// differ in case), of one type on both sides, from an `ON` term conjoined
/// at its top level (not under `OR` or `NOT`) of an INNER JOIN (never a
/// LEFT/RIGHT/FULL one, whose NULL-extended rows break the equality), and
/// only when every reference to the dropped relation is such a key;
/// otherwise the item is left as it was (and refused).
pub(crate) fn fold_inner_join_keys(sql: &str, item: Item, database: &Arc<Database>) -> Item {
    let outer = items_before(sql, item.start);
    let inner: Vec<String> = match find_keyword(&item.body, "FROM", 0) {
        Some(_) => items_before(&item.body, clause_end_of_from(&item.body)).into_iter().map(|(a, _)| a).collect(),
        None => Vec::new(),
    };
    let refs: Vec<(String, String)> = qualified_references(&item.body)
        .into_iter()
        .filter(|(q, _)| !inner.iter().any(|a| a.eq_ignore_ascii_case(q)))
        .filter(|(q, _)| outer.iter().any(|(a, _)| a.eq_ignore_ascii_case(q)))
        .collect();
    let mut aliases: Vec<String> = Vec::new();
    for (q, _) in &refs {
        if !aliases.iter().any(|a| a.eq_ignore_ascii_case(q)) {
            aliases.push(q.clone());
        }
    }
    if aliases.len() != 2 {
        return item;
    }
    let equalities = inner_join_equalities(&sql[..item.start]);
    let relation_of = |alias: &str| outer.iter().find(|(a, _)| a.eq_ignore_ascii_case(alias)).map(|(_, r)| r.clone());
    // The column's type, when it compares by its bytes (no text).
    let byte_type = |alias: &str, column: &str| -> Option<crate::core::db_type::DbType> {
        let name = crate::core::sql_scan::bare_name(&relation_of(alias)?);
        let view = database.schema_view(&name)?;
        view.schema()
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(column))
            .filter(|f| !matches!(f.db_type, crate::core::db_type::DbType::STRING | crate::core::db_type::DbType::BPCHAR(_)))
            .map(|f| f.literal_type())
    };
    // Keep one alias; every reference to the other must map through a key.
    for (keep, drop) in [(0usize, 1usize), (1, 0)] {
        let (keep, drop) = (&aliases[keep], &aliases[drop]);
        let mut mapping: Vec<(String, String)> = Vec::new();
        let mut ok = true;
        for (q, column) in refs.iter().filter(|(q, _)| q.eq_ignore_ascii_case(drop)) {
            let partner = equalities.iter().find_map(|((la, lc), (ra, rc))| {
                if la.eq_ignore_ascii_case(drop) && lc.eq_ignore_ascii_case(column) && ra.eq_ignore_ascii_case(keep) {
                    Some(rc.clone())
                } else if ra.eq_ignore_ascii_case(drop) && rc.eq_ignore_ascii_case(column) && la.eq_ignore_ascii_case(keep) {
                    Some(lc.clone())
                } else {
                    None
                }
            });
            match partner {
                // Both sides one type: an `int = bigint` key would read
                // the other type in the subquery.
                Some(p) if byte_type(q, column).is_some() && byte_type(q, column) == byte_type(keep, &p) => {
                    mapping.push((column.clone(), p))
                }
                _ => {
                    ok = false;
                    break;
                }
            }
        }
        if ok && !mapping.is_empty() {
            let mut item = item.clone();
            item.body = replace_qualified(&item.body, drop, keep, &mapping);
            return item;
        }
    }
    item
}

/// The `a.x = b.y` terms of the `ON` conditions of the INNER JOINs in
/// `from_text` (a statement up to an item), each side as (alias, column).
fn inner_join_equalities(from_text: &str) -> Vec<((String, String), (String, String))> {
    let upper = from_text.to_ascii_uppercase();
    let mut out = Vec::new();
    let mut at = 0;
    while let Some(join) = find_keyword(&upper, "JOIN", at) {
        at = join + 4;
        let before = upper[..join].trim_end();
        if ["LEFT", "RIGHT", "FULL", "CROSS", "OUTER", "NATURAL"].iter().any(|k| before.ends_with(k)) {
            continue;
        }
        let Some(on) = find_keyword(&upper, "ON", at) else { continue };
        let end = ["JOIN", "WHERE", "GROUP", "ORDER", "LIMIT", "HAVING", "INNER", "LEFT", "RIGHT", "FULL", "CROSS"]
            .iter()
            .filter_map(|k| find_keyword(&upper, k, on + 2))
            .min()
            .unwrap_or(from_text.len());
        let cond = &from_text[on + 2..end];
        let cond_upper = cond.to_ascii_uppercase();
        let mut pieces = Vec::new();
        let mut from = 0;
        while let Some(and) = find_keyword(&cond_upper, "AND", from) {
            pieces.push(&cond[from..and]);
            from = and + 3;
        }
        pieces.push(&cond[from..]);
        for piece in pieces {
            let piece = piece.trim().trim_start_matches('(').trim_end_matches(')').trim();
            let Some((l, r)) = piece.split_once('=') else { continue };
            if l.ends_with(['<', '>', '!']) || r.starts_with('=') {
                continue;
            }
            let side = |t: &str| -> Option<(String, String)> {
                let t = t.trim();
                let (a, c) = t.split_once('.')?;
                let clean = |x: &str| x.trim().trim_matches('"').to_string();
                (!a.is_empty() && !c.contains('.') && !c.contains(' ')).then(|| (clean(a), clean(c)))
            };
            if let (Some(l), Some(r)) = (side(l), side(r)) {
                out.push((l, r));
            }
        }
    }
    out
}

/// `body` with each `drop.column` (outside quotes, as a whole reference)
/// written `keep.partner`.
fn replace_qualified(body: &str, drop: &str, keep: &str, mapping: &[(String, String)]) -> String {
    let bytes = body.as_bytes();
    let mut out = String::with_capacity(body.len());
    let mut copied = 0;
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i).filter(|_| bytes[i] == b'\'') {
            i = end.max(i + 1);
            continue;
        }
        let boundary = i == 0 || !(is_word(bytes[i - 1]) || bytes[i - 1] == b'.');
        // By bytes: `i` may sit inside a multibyte character (a dotted
        // name's `․`); a match starts on an ASCII byte of `drop`.
        if boundary
            && bytes.len() - i > drop.len()
            && bytes[i..i + drop.len()].eq_ignore_ascii_case(drop.as_bytes())
            && bytes.get(i + drop.len()) == Some(&b'.')
        {
            let col_start = i + drop.len() + 1;
            let mut col_end = col_start;
            while col_end < bytes.len() && is_word(bytes[col_end]) {
                col_end += 1;
            }
            let column = &body[col_start..col_end];
            if let Some((_, partner)) = mapping.iter().find(|(c, _)| c.eq_ignore_ascii_case(column)) {
                out.push_str(&body[copied..i]);
                out.push_str(&format!("{keep}.{partner}"));
                copied = col_end;
                i = col_end;
                continue;
            }
        }
        i += 1;
    }
    out.push_str(&body[copied..]);
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
    // An outer alias the body names as a bare word may be bound again in a
    // nested subquery (`(SELECT … FROM u c WHERE c.n …)`), where `c.n` is
    // that subquery's own column: substituting the outer value there would
    // answer another query. Refused rather than guessed.
    //
    // The outer relation read again under another alias binds nothing:
    // `FROM u7 x WHERE x.g = u7.g` hides no `u7`, and `u7.g` there is the
    // outer row's, as PostgreSQL reads it. It was refused with the rest.
    let rebinds = |alias: &str| binds_name(&item.body, alias);
    for (qualifier, column) in qualified_references(&item.body) {
        if inner.iter().any(|a| a.eq_ignore_ascii_case(&qualifier)) {
            continue;
        }
        let Some((alias, relation)) = outer.iter().find(|(a, _)| a.eq_ignore_ascii_case(&qualifier)) else {
            continue;
        };
        if rebinds(alias) {
            return Err(format!(
                "a LATERAL item that names its outer alias {alias} inside itself is not supported yet; give the \
                 inner relation another alias"
            ));
        }
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

/// Whether `body` may bind `name` as a relation's name or alias somewhere in
/// itself: any bare use of the word but one, a relation in a `FROM` or a
/// `JOIN` that is given another alias (`FROM name x`, `JOIN name AS x`),
/// which leaves `name` unbound there. Anything it cannot place counts as a
/// binding.
fn binds_name(body: &str, name: &str) -> bool {
    // What may follow a relation that is no alias for it.
    const AFTER_A_RELATION: &[&str] = &[
        "WHERE", "ON", "JOIN", "LEFT", "RIGHT", "INNER", "CROSS", "FULL", "NATURAL", "GROUP", "ORDER", "LIMIT",
        "OFFSET", "UNION", "INTERSECT", "EXCEPT", "HAVING", "WINDOW", "FETCH", "FOR", "USING", "TABLESAMPLE",
        "LATERAL", "RETURNING", "SET",
    ];
    let bytes = body.as_bytes();
    // The word starting at or after `at`, past whitespace: itself and its end.
    let word_at = |mut at: usize| -> Option<(&str, usize)> {
        while bytes.get(at).is_some_and(|b| b.is_ascii_whitespace()) {
            at += 1;
        }
        let start = at;
        if !bytes.get(at).is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_') {
            return None;
        }
        while at < bytes.len() && is_word(bytes[at]) {
            at += 1;
        }
        Some((&body[start..at], at))
    };
    let mut previous: Option<&str> = None;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            q @ (b'\'' | b'"') => {
                i = crate::core::sql_scan::quoted_end(bytes, i, q);
                previous = None;
            }
            b if b.is_ascii_alphabetic() || b == b'_' => {
                let start = i;
                while i < bytes.len() && is_word(bytes[i]) {
                    i += 1;
                }
                let word = &body[start..i];
                let qualified = (start > 0 && bytes[start - 1] == b'.') || bytes.get(i) == Some(&b'.');
                if !qualified && word.eq_ignore_ascii_case(name) {
                    let a_relation = previous.is_some_and(|p| p.eq_ignore_ascii_case("FROM") || p.eq_ignore_ascii_case("JOIN"));
                    let own_alias = match word_at(i) {
                        Some((w, end)) if w.eq_ignore_ascii_case("AS") => word_at(end).map(|(alias, _)| alias),
                        Some((w, _)) if !AFTER_A_RELATION.iter().any(|k| w.eq_ignore_ascii_case(k)) => Some(w),
                        _ => None,
                    };
                    if !(a_relation && own_alias.is_some_and(|alias| !alias.eq_ignore_ascii_case(name))) {
                        return true;
                    }
                }
                previous = Some(word);
            }
            b if b.is_ascii_whitespace() => i += 1,
            _ => {
                previous = None;
                i += 1;
            }
        }
    }
    false
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

/// `sql` with every quoted part of a qualified reference (`"d"."id"`,
/// `"d".id`, `d."id"`) unquoted where quoting changes nothing: a plain
/// lower-case identifier, which PostgreSQL reads the same either way. A part
/// that needs its quotes (upper case, a space, a dot, `""`) keeps them, and so
/// does a lone quoted name (`AS "d"`, a quoted column); string literals are
/// copied as they are.
pub(crate) fn unquote_plain_qualified(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let plain = |name: &str| {
        let mut chars = name.chars();
        chars.next().is_some_and(|c| c.is_ascii_lowercase() || c == '_')
            && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    };
    // One part at `i`: its end, its text as it may be written, and whether it
    // was quoted.
    let part = |i: usize| -> Option<(usize, String, bool)> {
        if bytes.get(i) == Some(&b'"') {
            let end = crate::core::sql_scan::quoted_end(bytes, i, b'"');
            let inner = sql.get(i + 1..end.checked_sub(1)?)?;
            if end > i + 1 && bytes.get(end - 1) == Some(&b'"') && plain(inner) {
                return Some((end, inner.to_string(), true));
            }
            return Some((end, sql[i..end].to_string(), false));
        }
        let mut j = i;
        while j < bytes.len() && is_word(bytes[j]) {
            j += 1;
        }
        (j > i).then(|| (j, sql[i..j].to_string(), false))
    };
    let mut out = String::with_capacity(sql.len());
    let mut copied = 0;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            i = crate::core::sql_scan::quoted_end(bytes, i, b'\'').max(i + 1);
            continue;
        }
        let starts = (b == b'"' || b.is_ascii_alphabetic() || b == b'_')
            && (i == 0 || !(is_word(bytes[i - 1]) || bytes[i - 1] == b'.' || bytes[i - 1] == b'"'));
        if !starts {
            i += 1;
            continue;
        }
        let Some((q_end, q_text, q_unquoted)) = part(i) else {
            i += 1;
            continue;
        };
        if bytes.get(q_end) != Some(&b'.') {
            i = q_end.max(i + 1);
            continue;
        }
        let Some((c_end, c_text, c_unquoted)) = part(q_end + 1) else {
            i = q_end + 1;
            continue;
        };
        let third = matches!(bytes.get(c_end), Some(b'.'));
        if (q_unquoted || c_unquoted) && !third {
            out.push_str(&sql[copied..i]);
            out.push_str(&q_text);
            out.push('.');
            out.push_str(&c_text);
            copied = c_end;
        }
        i = c_end.max(i + 1);
    }
    out.push_str(&sql[copied..]);
    out
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
        .map(|(_, ty)| cast_type_name(ty).ok_or_else(|| format!("LATERAL: a {ty:?} outer column is not supported yet")))
        .collect()
}

/// The name a `CAST(… AS name)` reads `ty` back as. Not
/// [`cte::sql_type_name`](crate::core::rql::cte::sql_type_name)'s, which
/// declares a column: `DOUBLE` and `ARRAY` declare one, and no cast takes
/// either (`type "double" does not exist`).
fn cast_type_name(ty: &crate::core::db_type::DbType) -> Option<String> {
    use crate::core::db_type::DbType;
    match ty {
        DbType::F64 => Some("DOUBLE PRECISION".to_string()),
        DbType::ARRAY => Some("TEXT[]".to_string()),
        DbType::ARRAYOF(code) => cast_type_name(&DbType::from_byte(*code)).map(|e| format!("{e}[]")),
        other => crate::core::rql::cte::sql_type_name(other).map(str::to_string),
    }
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
    // `"d"."id"` (EF Core, SeaORM) read as `d.id`: the scans below find and
    // substitute outer references only in that spelling, and a quoted one was
    // left in the item, where `d` is unbound ("missing FROM-clause entry for
    // table d", every correlated scalar subquery an ORM sends).
    let mut current = unquote_plain_qualified(sql);
    let mut found = false;
    for index in 0..16usize {
        let Some(item) = next_item(&current) else { break };
        let item = qualify_outer_columns(&current, &item?, database).await?;
        let item = fold_inner_join_keys(&current, item, database);
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
                // Only the outer rows the statement can keep need a run. The
                // item ran for every row of the outer relation: one customer
                // of a thousand cost a thousand runs, and an outer table past
                // `MAX_TUPLES` was refused whatever its WHERE kept.
                let tuples = match outer_only_filter(&current, &item, alias) {
                    Some(filter) => match text_rows(&format!("{key_query} WHERE {filter}"), columns.len(), database, tx).await {
                        Ok(tuples) => tuples,
                        // Read as written it failed; every outer row is
                        // still a superset of what the statement keeps.
                        Err(_) => text_rows(&key_query, columns.len(), database, tx).await?,
                    },
                    None => text_rows(&key_query, columns.len(), database, tx).await?,
                };
                #[cfg(test)]
                test_hooks::record_runs(relation, tuples.len());
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
                    columns.len(),
                    &item.alias,
                    database,
                    tx,
                    temps,
                )
                .await?;
                // A correlated scalar subquery's item (`rql::correlated`): one
                // row at most per outer value, or PostgreSQL's 21000.
                if item.alias.starts_with(crate::core::rql::correlated::ALIAS_PREFIX) {
                    let keys = (1..=columns.len()).map(|n| format!("__lat_k{n}")).collect::<Vec<_>>().join(" || '|' || ");
                    let counts = text_rows(
                        &format!("SELECT count(*), count(DISTINCT {keys}) FROM {temp}"),
                        2,
                        database,
                        tx,
                    )
                    .await?;
                    if let Some([Some(all), Some(distinct)]) = counts.first().map(|r| [r[0].clone(), r[1].clone()]) {
                        if all != distinct {
                            return Err("more than one row returned by a subquery used as an expression".to_string());
                        }
                    }
                }
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

/// The statement's own top-level `WHERE` conjuncts that read nothing but the
/// outer item `alias`'s columns, joined by `AND`: a row of that item they
/// refuse is not in the result, so the `LATERAL` item need not run for its
/// values. `None` when there is none, or when it is not safe to say:
///
/// - the outer item must be the first `FROM` item, and no `RIGHT` or `FULL`
///   join may pad it with NULLs, where a conjunct over it reads a row the
///   relation does not hold;
/// - a conjunct may hold only `alias.column`s, literals, operators, casts
///   and `AND OR NOT IS NULL IN BETWEEN LIKE ILIKE TRUE FALSE DISTINCT FROM`:
///   no call (a volatile one would answer differently twice), no subquery,
///   no bare or other item's column, no quoted name and no parameter.
///
/// Dropping a conjunct only keeps more rows, never fewer, so whatever is
/// left out is still decided by the statement's own `WHERE`.
fn outer_only_filter(sql: &str, item: &Item, alias: &str) -> Option<String> {
    let first = items_before(sql, item.start).into_iter().next()?;
    if !first.0.eq_ignore_ascii_case(alias) {
        return None;
    }
    let from = find_keyword(sql, "FROM", 0)?;
    if ["RIGHT", "FULL"].iter().any(|w| find_keyword(&sql[from..], w, 0).is_some()) {
        return None;
    }
    let at = find_keyword(sql, "WHERE", item.end)?;
    let body = at + "WHERE".len();
    let mut end = sql.len();
    for word in ["GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "WINDOW", "FETCH", "FOR", "RETURNING"] {
        if let Some(found) = find_keyword(sql, word, body) {
            end = end.min(found);
        }
    }
    let clause = sql[body..end].trim().trim_end_matches(';');
    let terms: Vec<String> = conjuncts(clause)
        .into_iter()
        .filter(|t| !t.is_empty() && (reads_only(t, alias) || exists_reads_only(t, alias)))
        .collect();
    (!terms.is_empty()).then(|| terms.join(" AND "))
}

/// `split_conjuncts`, with a `BETWEEN`'s own `AND` kept inside its term:
/// split there, `a BETWEEN 1 AND 2 OR b = 5` became `a BETWEEN 1` and
/// `2 OR b = 5`.
fn conjuncts(clause: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut open = false;
    for part in crate::core::rql::join::split_conjuncts(clause) {
        let part = part.trim().to_string();
        if open {
            if let Some(last) = out.last_mut() {
                last.push_str(" AND ");
                last.push_str(&part);
            }
        } else {
            out.push(part);
        }
        let last = out.last().map(String::as_str).unwrap_or("");
        // Open while the term's last BETWEEN has no AND after it yet.
        let mut at = 0;
        let mut between = None;
        while let Some(found) = find_keyword(last, "BETWEEN", at) {
            between = Some(found);
            at = found + "BETWEEN".len();
        }
        open = between.is_some_and(|b| find_keyword(last, "AND", b).is_none());
    }
    out
}

#[cfg(test)]
pub(crate) fn reads_only_for_tests(term: &str, alias: &str) -> bool {
    reads_only(term, alias)
}

/// Whether `term` reads only `alias`'s columns, by [`outer_only_filter`]'s
/// rule.
/// Whether `term` is `[NOT] EXISTS (SELECT …)` reading nothing but the
/// outer item `alias` and the relations it names in its own FROM and JOINs:
/// as deterministic as the statement's own WHERE, so the outer values it
/// refuses need no run. EF Core's filter of a product by a tag
/// (`EXISTS (SELECT 1 FROM tags t … WHERE p.id = t.product …)`) is most
/// of what its `WHERE` keeps out; left out of the filter, a correlated
/// subquery in the select list ran for every product of the table.
///
/// Refused when unsure: a call of any kind, a `$` parameter, a `;`, a
/// nested subquery, a qualifier the body does not bind and is not
/// `alias`, and an unqualified word that is neither a keyword nor a FROM
/// item's name or alias (it could be another item's column).
fn exists_reads_only(term: &str, alias: &str) -> bool {
    const KEYWORDS: &[&str] = &[
        "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IS", "NULL", "IN", "BETWEEN", "LIKE", "ILIKE", "TRUE",
        "FALSE", "DISTINCT", "JOIN", "INNER", "LEFT", "OUTER", "ON", "AS", "CROSS", "LIMIT", "TOP",
    ];
    let t = term.trim();
    let upper = t.to_ascii_uppercase();
    let rest = upper.strip_prefix("NOT ").map(str::trim_start).unwrap_or(&upper);
    let Some(after) = rest.strip_prefix("EXISTS") else { return false };
    let open = t.len() - after.trim_start().len();
    if !t[open..].starts_with('(') || matching_paren(t, open) != Some(t.len() - 1) {
        return false;
    }
    let body = &t[open + 1..t.len() - 1];
    if !body.trim_start().to_ascii_uppercase().starts_with("SELECT") {
        return false;
    }
    // Words and quoted names, in order.
    let bytes = body.as_bytes();
    let mut tokens: Vec<(String, bool, usize)> = Vec::new(); // (text, quoted, end)
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'\'' => i = crate::core::sql_scan::quoted_end(bytes, i, b'\''),
            b'"' => {
                let end = crate::core::sql_scan::quoted_end(bytes, i, b'"');
                tokens.push((body[i..end].to_string(), true, end));
                i = end;
            }
            b'$' | b';' => return false,
            _ if b.is_ascii_alphabetic() || b == b'_' || b >= 0x80 => {
                let start = i;
                while i < bytes.len() && (is_word(bytes[i]) || bytes[i] >= 0x80) {
                    i += 1;
                }
                tokens.push((body[start..i].to_string(), false, i));
            }
            _ => i += 1,
        }
    }
    // The names the body binds: each FROM / JOIN item's name and alias.
    let mut bound: Vec<String> = Vec::new();
    for (k, (word, quoted, _)) in tokens.iter().enumerate() {
        if !quoted && (word.eq_ignore_ascii_case("FROM") || word.eq_ignore_ascii_case("JOIN")) {
            if let Some((name, _, _)) = tokens.get(k + 1) {
                bound.push(name.trim_matches('"').to_ascii_lowercase());
                let mut j = k + 2;
                if tokens.get(j).is_some_and(|(w, q, _)| !q && w.eq_ignore_ascii_case("AS")) {
                    j += 1;
                }
                if let Some((a, q, _)) = tokens.get(j) {
                    if *q || !KEYWORDS.iter().any(|kw| kw.eq_ignore_ascii_case(a)) {
                        bound.push(a.trim_matches('"').to_ascii_lowercase());
                    }
                }
            }
        }
    }
    let binds = |name: &str| {
        let n = name.trim_matches('"').to_ascii_lowercase();
        n == alias.to_ascii_lowercase() || bound.contains(&n)
    };
    for (word, quoted, end) in &tokens {
        let next = body[*end..].trim_start();
        if next.starts_with('(') {
            // `x IN ('a', 'b')`, a list of values; `IN (SELECT …)` and any
            // call are not vouched for.
            let list = next[1..].trim_start().to_ascii_uppercase();
            if !quoted && word.eq_ignore_ascii_case("IN") && !list.starts_with("SELECT") && !list.starts_with("WITH") {
                continue;
            }
            return false;
        }
        // A qualifier: the outer alias or a name the body binds.
        if next.starts_with('.') {
            if !binds(word) {
                return false;
            }
            continue;
        }
        // A qualified column's name (after a `.`), or a cast's type
        // (after `::`).
        let before = body[..*end - word.len()].trim_end();
        if before.ends_with('.') || before.ends_with("::") {
            continue;
        }
        if *quoted || KEYWORDS.iter().any(|kw| kw.eq_ignore_ascii_case(word)) {
            continue;
        }
        if !binds(word) {
            return false;
        }
    }
    true
}

fn reads_only(term: &str, alias: &str) -> bool {
    const WORDS: &[&str] =
        &["AND", "OR", "NOT", "IS", "NULL", "IN", "BETWEEN", "LIKE", "ILIKE", "TRUE", "FALSE", "DISTINCT", "FROM"];
    let bytes = term.as_bytes();
    let mut column = false;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'\'' => {
                i = crate::core::sql_scan::quoted_end(bytes, i, b'\'');
                continue;
            }
            b'"' | b'$' | b';' => return false,
            b':' if bytes.get(i + 1) == Some(&b':') => {
                // `::type`: the type's one-word name is not a column. A
                // two-word type's second word is refused below, as any
                // unknown word is.
                i += 2;
                while i < bytes.len() && bytes[i] == b' ' {
                    i += 1;
                }
                while i < bytes.len() && is_word(bytes[i]) {
                    i += 1;
                }
                continue;
            }
            b'0'..=b'9' => {
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.' || bytes[i] == b'_') {
                    i += 1;
                }
                continue;
            }
            _ if b.is_ascii_alphabetic() || b == b'_' => {
                let start = i;
                while i < bytes.len() && is_word(bytes[i]) {
                    i += 1;
                }
                let word = &term[start..i];
                if bytes.get(i) == Some(&b'.') {
                    if !word.eq_ignore_ascii_case(alias) {
                        return false;
                    }
                    i += 1;
                    let c0 = i;
                    while i < bytes.len() && is_word(bytes[i]) {
                        i += 1;
                    }
                    if i == c0 || term[i..].trim_start().starts_with('(') {
                        return false;
                    }
                    column = true;
                    continue;
                }
                let called = term[i..].trim_start().starts_with('(');
                if !WORDS.iter().any(|w| w.eq_ignore_ascii_case(word)) || (called && !word.eq_ignore_ascii_case("IN")) {
                    return false;
                }
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    column
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
    fn bare_names_are_the_words_that_can_only_be_columns() {
        let names = |t: &str| bare_names(t).into_iter().map(|(n, _)| n).collect::<Vec<_>>();
        assert_eq!(names("r"), ["r"]);
        assert_eq!(names("1, s"), ["s"]);
        assert_eq!(names("t.r, x.y"), Vec::<String>::new());
        assert_eq!(names("ARRAY[f, f * 2]"), ["f", "f"]);
        assert_eq!(names("CAST(s AS double precision), n"), ["s", "n"]);
        assert_eq!(names("s::numeric(10, 2) + n"), ["s", "n"]);
        assert_eq!(names("lower(name), DATE '2026-01-01', 'lit', \"q\""), ["name"]);
        assert_eq!(names("VALUES (a), (b IS NULL)"), ["a", "b"]);
        assert_eq!(names("1e5, 2"), Vec::<String>::new());
    }

    #[test]
    fn a_function_item_knows_its_arguments_span() {
        let sql = "SELECT id, v FROM tl, LATERAL unnest(r) WITH ORDINALITY AS a(v, n)";
        let item = next_item(sql).unwrap().unwrap();
        let (a, b) = item.args.unwrap();
        assert_eq!(&item.body[a..b], "r");
        let sql = "SELECT x FROM tf, LATERAL (VALUES (f), (s)) v(x)";
        let item = next_item(sql).unwrap().unwrap();
        let (a, b) = item.args.unwrap();
        assert_eq!(&item.body[a..b], "VALUES (f), (s)");
        let sql = "SELECT x FROM tf CROSS JOIN LATERAL (SELECT f AS x) s";
        assert!(next_item(sql).unwrap().unwrap().args.is_none());
        // First in FROM, nothing before it has the name.
        assert_eq!(next_item("SELECT * FROM LATERAL unnest(r) g").unwrap().unwrap_err(), "column \"r\" does not exist");
    }

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

        // Bound again in a nested subquery: refused, not substituted there.
        let sql = "SELECT 1 FROM t c, LATERAL (SELECT x.a FROM u x WHERE x.k = c.k AND EXISTS \
                   (SELECT 1 FROM w c WHERE c.n = x.a)) s";
        let item = next_item(sql).unwrap().unwrap();
        assert!(outer_references(sql, &item).is_err());
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

#[cfg(test)]
mod binds_name_tests {
    use super::binds_name;

    /// A relation read under another alias leaves its name unbound; every
    /// other bare use of the name may bind it.
    #[test]
    fn a_relation_given_another_alias_does_not_bind_its_name() {
        for body in [
            "SELECT max(x.v) FROM u7 x WHERE x.g = u7.g",
            "SELECT count(*) FROM u7 AS x WHERE x.g = u7.g",
            "SELECT count(*) FROM u7 x JOIN u7 y ON y.id = x.id WHERE x.g = u7.g",
            "SELECT 1 FROM other o WHERE o.g = u7.g",
            "SELECT 'u7' FROM other o WHERE o.g = u7.g",
        ] {
            assert!(!binds_name(body, "u7"), "{body}");
        }
        for body in [
            // Unaliased, the name is the inner relation's.
            "SELECT count(*) FROM u7 WHERE u7.g = 1",
            "SELECT count(*) FROM u7",
            "SELECT 1 FROM other o JOIN u7 ON u7.id = o.id",
            // Bound as an alias, here or deeper.
            "SELECT 1 FROM other u7 WHERE u7.g = 1",
            "SELECT 1 FROM other AS u7",
            "SELECT (SELECT c.n FROM v u7 WHERE u7.n = 1) FROM other o",
            "SELECT 1 FROM u7 u7",
            // Not placed: after a comma, or anywhere else.
            "SELECT 1 FROM other o, u7 x WHERE x.g = u7.g",
            "SELECT u7 FROM other o",
        ] {
            assert!(binds_name(body, "u7"), "{body}");
        }
    }
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// The runs each outer relation's `LATERAL` items took, by relation name.
    static RUNS: Mutex<Option<HashMap<String, usize>>> = Mutex::new(None);

    pub(crate) fn record_runs(relation: &str, runs: usize) {
        *RUNS.lock().unwrap().get_or_insert_with(HashMap::new).entry(relation.to_string()).or_default() += runs;
    }

    /// The runs recorded so far for `relation`, and for the relations a
    /// statement whose outer item is `alias` materialised in its place (an
    /// item after the first reads the earlier ones' join, `__cte_<alias>_…`),
    /// and forgets them.
    pub(crate) fn take_runs(relation: &str, alias: &str) -> usize {
        let derived = format!("__cte_{alias}_");
        let mut runs = RUNS.lock().unwrap();
        let Some(map) = runs.as_mut() else { return 0 };
        let names: Vec<String> = map.keys().filter(|k| *k == relation || k.starts_with(&derived)).cloned().collect();
        names.iter().filter_map(|k| map.remove(k)).sum()
    }
}

#[cfg(test)]
mod multibyte_tests {
    use super::*;

    /// A multibyte character where `AS` or `ON` would be read — after the
    /// item, after a FROM relation — cut through it ("end byte index 17 is
    /// not a char boundary"). Every position, as the live sweep puts it.
    #[test]
    fn a_multibyte_character_where_a_keyword_is_read_is_not_cut() {
        let base = "SELECT * FROM t AS l LEFT JOIN LATERAL (SELECT b FROM u WHERE u.b = l.b) AS r ON true";
        for at in (0..=base.len()).filter(|&i| base.is_char_boundary(i)) {
            for ch in ["Δ", "😀"] {
                let sql = format!("{}{}{}", &base[..at], ch, &base[at..]);
                if let Some(Ok(item)) = next_item(&sql) {
                    let _ = outer_references(&sql, &item);
                }
            }
        }
        let item = next_item(base).expect("an item").expect("parses");
        assert!(outer_references(base, &item).expect("reads").is_some());
    }
}

#[cfg(test)]
mod unquote_plain_qualified_tests {
    use super::unquote_plain_qualified as u;

    #[test]
    fn plain_quoted_parts_are_unquoted_and_the_rest_kept() {
        assert_eq!(u(r#"SELECT "d"."id", (SELECT "p"."x" FROM pt AS "p" WHERE "d"."id" = "p"."tagsid") FROM tags AS "d""#),
            r#"SELECT d.id, (SELECT p.x FROM pt AS "p" WHERE d.id = p.tagsid) FROM tags AS "d""#);
        assert_eq!(u(r#""d".id + d."id" + d.id"#), "d.id + d.id + d.id");
        assert_eq!(u(r#""D"."id" + "d"."Id" + "d"."a b""#), r#""D".id + d."Id" + d."a b""#);
        assert_eq!(u(r#"'"d"."id"' || "d"."id""#), r#"'"d"."id"' || d.id"#);
        assert_eq!(u(r#""s"."t"."c" + "denevcloud․erp․x"."c""#), r#""s"."t"."c" + "denevcloud․erp․x".c"#);
        assert_eq!(u("no quotes at all"), "no quotes at all");
    }
}

#[cfg(test)]
mod exists_filter_tests {
    use super::exists_reads_only;

    /// An EXISTS over the outer item and its own relations filters which
    /// outer values run; anything it cannot vouch for does not.
    #[test]
    fn an_exists_over_its_own_relations_filters() {
        let tag = "EXISTS (SELECT 1 FROM \"producttags\" AS d0 INNER JOIN \"tags\" AS d1 ON d0.tagsid = d1.id \
                   WHERE d.id = d0.productsid AND d1.content = 'Realme C53')";
        assert!(exists_reads_only(tag, "d"));
        assert!(exists_reads_only("NOT EXISTS (SELECT 1 FROM t x WHERE x.k = d.id AND x.v = '1'::uuid)", "d"));
        // Another item's column, a call, a nested subquery, a parameter, a
        // bare word that could be any column.
        assert!(!exists_reads_only("EXISTS (SELECT 1 FROM t x WHERE x.k = e.id)", "d"));
        assert!(!exists_reads_only("EXISTS (SELECT 1 FROM t x WHERE x.k = d.id AND random() > 0.5)", "d"));
        assert!(!exists_reads_only("EXISTS (SELECT 1 FROM t x WHERE x.k IN (SELECT y.k FROM u y))", "d"));
        // A list of values is as decided as `=`.
        assert!(exists_reads_only("EXISTS (SELECT 1 FROM t x WHERE x.k = d.id AND x.v IN ('a', 'b'))", "d"));
        assert!(!exists_reads_only("EXISTS (SELECT 1 FROM t x WHERE x.k = $1)", "d"));
        assert!(!exists_reads_only("EXISTS (SELECT 1 FROM t x WHERE x.k = id)", "d"));
        assert!(!exists_reads_only("EXISTS (SELECT 1 FROM t x) OR d.id = 1", "d"));
        assert!(!exists_reads_only("d.id = 1", "d"));
    }
}
