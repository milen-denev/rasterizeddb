//! One quote-aware scanner for SQL text, and the normalisation that lets every
//! other scanner in the engine get away with knowing only standard strings.
//!
//! PostgreSQL has four ways to write a quoted run:
//!
//! * `'…'` — a standard string; `''` stands for one quote and a backslash is
//!   an ordinary character (`standard_conforming_strings` is on);
//! * `E'…'` — an escape string; a backslash escapes the character after it,
//!   so `\'` does *not* end the literal;
//! * `$tag$…$tag$` — a dollar-quoted string, inside which nothing is special;
//! * `"…"` — a quoted identifier; `""` stands for one quote.
//!
//! The engine has dozens of text scanners — clause splitters, keyword finders,
//! the tuple splitter, the comment stripper, the statement splitter, the
//! redaction and statistics normalisers — and nearly all of them know only the
//! first and the last. An escape string with an escaped quote inside therefore
//! ended early for all of them: `WHERE c LIKE E'it\'s%' ORDER BY id` read as a
//! literal `'it\'`, then the bare word `s%`, then a quote that swallowed the
//! `ORDER BY` — and the error named a column called `ORDER`.
//!
//! Teaching each scanner separately would leave the next one written to get it
//! wrong again. Instead [`normalize_string_literals`] rewrites escape strings
//! and dollar-quoted strings into the standard form they mean, once, where
//! statement text enters the engine. After that the only quoting any scanner
//! can meet is the one they all already handle. [`skip_quoted`] is the
//! scanner for code that sees text *before* that point, or that wants to be
//! right on its own.

use std::borrow::Cow;

/// A byte that continues an identifier. A quote or `$` right after one belongs
/// to (or follows) a name rather than opening something: `ELSE'x'` is the
/// keyword `ELSE` then a standard string, not an escape string.
#[inline]
pub fn is_identifier_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

/// `ix.indkey::text` and the catalogue's other vector columns cast to text,
/// as `array_to_string(ix.indkey, ' ')`: PostgreSQL's `int2vector` and
/// `oidvector` print their numbers apart by a space (`3 1`), and a catalogue
/// query splits them there (`string_to_array(ix.indkey::text, ' ')`). They
/// are arrays here, whose text is `{3,1}`. `None` when there is none.
pub fn vector_columns_as_text(sql: &str) -> Option<String> {
    const VECTORS: [&str; 6] = ["indkey", "indclass", "indoption", "indcollation", "proargtypes", "partattrs"];
    if !VECTORS.iter().any(|v| contains_ignore_ascii_case(sql, v.as_bytes())) || !sql.contains("::") {
        return None;
    }
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 32);
    let (mut copied, mut i) = (0usize, 0usize);
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        if !is_identifier_byte(bytes[i]) || (i > 0 && is_identifier_byte(bytes[i - 1])) {
            i += 1;
            continue;
        }
        // A name, with the qualifier before it if it has one.
        let start = i;
        let mut end = i + bytes[i..].iter().take_while(|b| is_identifier_byte(**b)).count();
        let mut column = &sql[start..end];
        if bytes.get(end) == Some(&b'.') {
            let next = end + 1 + bytes[end + 1..].iter().take_while(|b| is_identifier_byte(**b)).count();
            column = &sql[end + 1..next];
            end = next;
        }
        i = end;
        if !VECTORS.iter().any(|v| column.eq_ignore_ascii_case(v)) {
            continue;
        }
        let Some(cast) = sql[end..].trim_start().strip_prefix("::") else {
            continue;
        };
        let cast = cast.trim_start();
        if !starts_with_keyword(cast, "text") {
            continue;
        }
        let cast_end = sql.len() - cast.len() + 4;
        out.push_str(&sql[copied..start]);
        out.push_str(&format!("array_to_string({}, ' ')", &sql[start..end]));
        copied = cast_end;
        i = cast_end;
    }
    if copied == 0 {
        return None;
    }
    out.push_str(&sql[copied..]);
    Some(out)
}

/// Whether `text` begins with the keyword `keyword`, whatever its case, as a
/// whole word: `SELECT 1` and `select(1)` do, `selected_branches` does not.
///
/// What a scanner asks of the text after a `(` to tell a subquery from an
/// expression. Asked as a bare prefix (`starts_with("SELECT")`), a column
/// whose name begins with the keyword (`selected`, `selection`, `with_x`,
/// `values_n`) made `count(selected)` a subquery named "selected".
#[inline]
pub fn starts_with_keyword(text: &str, keyword: &str) -> bool {
    let (text, keyword) = (text.as_bytes(), keyword.as_bytes());
    text.len() >= keyword.len()
        && text[..keyword.len()].eq_ignore_ascii_case(keyword)
        && !text.get(keyword.len()).is_some_and(|b| is_identifier_byte(*b))
}

/// True when the `'` at `quote` opens an escape string: `E'…'` / `e'…'`, with
/// the `E` a word of its own rather than the end of an identifier.
#[inline]
pub fn is_escape_string_quote(bytes: &[u8], quote: usize) -> bool {
    quote >= 1
        && matches!(bytes[quote - 1], b'E' | b'e')
        && (quote < 2 || !(is_identifier_byte(bytes[quote - 2]) || bytes[quote - 2] == b'$'))
}

/// Index just past the string literal opening at `start` (or the end of the
/// text when it is unterminated). In an escape string a backslash escapes
/// whatever byte follows it.
pub fn string_literal_end(bytes: &[u8], start: usize, escapes: bool) -> usize {
    let mut i = start + 1;
    loop {
        let found = if escapes {
            memchr::memchr2(b'\'', b'\\', &bytes[i..])
        } else {
            memchr::memchr(b'\'', &bytes[i..])
        };
        let Some(p) = found else {
            return bytes.len();
        };
        i += p;
        if bytes[i] == b'\\' {
            i = (i + 2).min(bytes.len());
            continue;
        }
        // A quote: doubled, it is an escaped quote and the literal goes on.
        if bytes.get(i + 1) == Some(&b'\'') {
            i += 2;
            continue;
        }
        return i + 1;
    }
}

/// The catalogue spellings drivers and `psql` write, as this engine reads
/// them, outside quotes:
///
/// - a `pg_catalog.` qualifier dropped (`pg_catalog.format_type(…)`,
///   `::pg_catalog.text`): every built-in lives there, and a name resolves
///   to it unqualified;
/// - `OPERATOR(pg_catalog.~)` as the operator `~` it names;
/// - `COLLATE` dropped for the collations byte order already is (`"C"`,
///   `"POSIX"`, `"default"`, `pg_catalog.default`, `ucs_basic`); any other
///   is left, and refused as before;
/// - `pg_table_is_visible(…)`, `pg_function_is_visible(…)` and
///   `pg_type_is_visible(…)` as TRUE: everything here is in `public`, which
///   is on every search path;
/// - `::"char"` as `::text` and `::oid` as `::bigint`, the types those
///   columns have here.
///
/// Borrowed when the text holds none of them.
pub fn catalog_syntax(sql: &str) -> Cow<'_, str> {
    let has = |needle: &[u8]| contains_ignore_ascii_case(sql, needle);
    if !(has(b"pg_catalog.") || has(b"operator(") || has(b"collate") || has(b"_is_visible") || has(b"\"char\"") || has(b"::oid"))
    {
        return Cow::Borrowed(sql);
    }
    let bytes = sql.as_bytes();
    let at = |i: usize, word: &str| bytes.len() - i >= word.len() && bytes[i..i + word.len()].eq_ignore_ascii_case(word.as_bytes());
    let ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80;
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    // Whether the name at `i` starts a word: after a non-identifier byte, or
    // where a dropped qualifier ended.
    let mut word_start = true;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i) {
            out.push_str(&sql[i..end]);
            i = end.max(i + 1);
            word_start = false;
            continue;
        }
        let boundary = word_start || (i > 0 && !ident(bytes[i - 1]) && bytes[i - 1] != b'.');
        if boundary && at(i, "pg_catalog.") && bytes.get(i + 11).is_some_and(|&b| ident(b) || b == b'"') {
            i += 11;
            word_start = true;
            continue;
        }
        if boundary && at(i, "operator") {
            let open = i + 8 + sql[i + 8..].len() - sql[i + 8..].trim_start().len();
            if bytes.get(open) == Some(&b'(')
                && let Some(close) = sql[open..].find(')').map(|c| open + c)
            {
                let op = sql[open + 1..close].trim();
                let op = op.strip_prefix("pg_catalog.").or_else(|| op.strip_prefix("PG_CATALOG.")).unwrap_or(op);
                out.push(' ');
                out.push_str(op.trim());
                out.push(' ');
                i = close + 1;
                word_start = true;
                continue;
            }
        }
        if boundary && at(i, "collate") && bytes.get(i + 7).is_some_and(|&b| b.is_ascii_whitespace()) {
            let rest = &sql[i + 7..];
            let name_at = i + 7 + rest.len() - rest.trim_start().len();
            let name_end = if bytes.get(name_at) == Some(&b'"') {
                quoted_end(bytes, name_at, b'"')
            } else {
                let mut e = name_at;
                while e < bytes.len() && (ident(bytes[e]) || bytes[e] == b'.') {
                    e += 1;
                }
                e
            };
            let name = sql[name_at..name_end].trim_matches('"').to_ascii_lowercase();
            if matches!(name.as_str(), "c" | "posix" | "default" | "pg_catalog.default" | "ucs_basic") {
                i = name_end;
                word_start = true;
                continue;
            }
        }
        if boundary {
            let visible =
                ["pg_table_is_visible", "pg_function_is_visible", "pg_type_is_visible"].iter().find(|w| at(i, w)).copied();
            if let Some(word) = visible {
                let open = i + word.len() + sql[i + word.len()..].len() - sql[i + word.len()..].trim_start().len();
                if bytes.get(open) == Some(&b'(')
                    && let Some(close) = matching_paren_at(bytes, open)
                {
                    out.push_str("TRUE");
                    i = close + 1;
                    word_start = false;
                    continue;
                }
            }
        }
        if at(i, "::") {
            let rest = &sql[i + 2..];
            let mut ty_at = i + 2 + rest.len() - rest.trim_start().len();
            // `::pg_catalog.oid`, as psql's `\gdesc` writes it.
            if at(ty_at, "pg_catalog.") && (at(ty_at + 11, "\"char\"") || (at(ty_at + 11, "oid") && !bytes.get(ty_at + 14).is_some_and(|&b| ident(b)))) {
                ty_at += 11;
            }
            if at(ty_at, "\"char\"") {
                out.push_str("::text");
                i = ty_at + 6;
                word_start = false;
                continue;
            }
            if at(ty_at, "oid") && !bytes.get(ty_at + 3).is_some_and(|&b| ident(b)) {
                out.push_str("::bigint");
                i = ty_at + 3;
                word_start = false;
                continue;
            }
        }
        let ch = sql[i..].chars().next().expect("in bounds");
        word_start = !(ident(bytes[i]) || bytes[i] == b'.');
        out.push(ch);
        i += ch.len_utf8();
    }
    Cow::Owned(out)
}

/// The index of the parenthesis closing the one at `open`, past quotes.
fn matching_paren_at(bytes: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut i = open;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        match bytes[i] {
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

/// `[NOT] EXISTS (SELECT … [WHERE c])` whose `SELECT` reads no relation, as
/// the truth value it is: `TRUE` (`FALSE`) with no `WHERE`, else `((c) IS
/// TRUE)` (`IS NOT TRUE`), `c` reading the outer row as it did. A `SELECT`
/// with no `FROM` yields one row when its condition is true and none
/// otherwise (NULL is not true). Every `EXISTS` path wanted a relation
/// ("the SELECT inside EXISTS has no FROM clause", 42601), so `WHERE NOT
/// EXISTS (SELECT 1 WHERE $1 = 2)` failed whatever `$1` was.
///
/// Only that shape is rewritten, an allowlist: a select list calling no
/// function (an aggregate yields a row whatever the condition, a
/// set-returning function any number) and holding no subquery, and no
/// clause but `WHERE` (`LIMIT 0`, `HAVING` and a set operation change the
/// count). Anything else is left as written, to be refused as before.
pub fn from_less_exists(sql: &str) -> Option<String> {
    if !contains_ignore_ascii_case(sql, b"EXISTS") {
        return None;
    }
    let bytes = sql.as_bytes();
    let mut out = String::new();
    let mut copied = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end;
            continue;
        }
        let word = i + 6 <= bytes.len()
            && bytes[i..i + 6].eq_ignore_ascii_case(b"EXISTS")
            && (i == 0 || !is_identifier_byte(bytes[i - 1]))
            && bytes.get(i + 6).is_none_or(|b| !is_identifier_byte(*b));
        if !word {
            i += 1;
            continue;
        }
        let mut open = i + 6;
        while open < bytes.len() && bytes[open].is_ascii_whitespace() {
            open += 1;
        }
        let Some(close) = (bytes.get(open) == Some(&b'(')).then(|| matching_close(sql, open)).flatten() else {
            i += 6;
            continue;
        };
        let Some(condition) = from_less_condition(&sql[open + 1..close]) else {
            i += 6;
            continue;
        };
        // A `NOT` before it is the negation of this test, absorbed.
        let head = sql[copied..i].trim_end_matches(|c: char| c.is_ascii_whitespace());
        let negated = head.len() >= 3
            && head.as_bytes()[head.len() - 3..].eq_ignore_ascii_case(b"NOT")
            && (head.len() == 3 || !is_identifier_byte(head.as_bytes()[head.len() - 4]));
        let start = if negated { copied + head.len() - 3 } else { i };
        out.push_str(&sql[copied..start]);
        match condition {
            None => out.push_str(if negated { "FALSE" } else { "TRUE" }),
            Some(condition) => {
                // An `EXISTS` inside the condition, rewritten as well.
                let condition = from_less_exists(condition).map_or(Cow::Borrowed(condition), Cow::Owned);
                out.push_str("((");
                out.push_str(&condition);
                out.push_str(if negated { ") IS NOT TRUE)" } else { ") IS TRUE)" });
            }
        }
        copied = close + 1;
        i = close + 1;
    }
    if copied == 0 {
        return None;
    }
    out.push_str(&sql[copied..]);
    Some(out)
}

/// For [`from_less_exists`]: `Some(None)` when `inner` is a `SELECT` with
/// neither `FROM` nor `WHERE` (one row), `Some(Some(c))` when its only clause
/// is `WHERE c`, `None` for anything else.
fn from_less_condition(inner: &str) -> Option<Option<&str>> {
    use crate::core::rql::join::find_keyword;
    let inner = inner.trim();
    if !starts_with_keyword(inner, "SELECT") {
        return None;
    }
    const CLAUSES: [&str; 13] = [
        "FROM", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "FETCH", "UNION", "INTERSECT", "EXCEPT", "WINDOW",
        "INTO", "FOR",
    ];
    if CLAUSES.iter().any(|k| find_keyword(inner, k, 0).is_some()) {
        return None;
    }
    let where_at = find_keyword(inner, "WHERE", 0);
    let items = inner["SELECT".len()..where_at.unwrap_or(inner.len())].trim();
    // `SELECT *` with no table is PostgreSQL's error; a call or a subquery
    // may yield other than one row. Aggregates alone (`SELECT count(*) WHERE
    // false`) yield one row whatever the condition.
    const AGGREGATES: [&str; 12] = [
        "count", "sum", "avg", "min", "max", "bool_and", "bool_or", "every", "string_agg", "array_agg", "json_agg",
        "jsonb_agg",
    ];
    let b = items.as_bytes();
    let mut calls = 0usize;
    let mut aggregates = 0usize;
    for p in (0..b.len()).filter(|p| b[*p] == b'(') {
        let before = items[..p].trim_end();
        if !before.as_bytes().last().is_some_and(|c| is_identifier_byte(*c) || *c == b'"') {
            continue;
        }
        calls += 1;
        let name_at = before.bytes().rposition(|c| !is_identifier_byte(c)).map_or(0, |at| at + 1);
        if AGGREGATES.iter().any(|a| before.as_bytes()[name_at..].eq_ignore_ascii_case(a.as_bytes())) {
            aggregates += 1;
        }
    }
    if items.is_empty() || items == "*" || contains_ignore_ascii_case(items, b"SELECT") {
        return None;
    }
    if calls > 0 {
        return (calls == aggregates).then_some(None);
    }
    match where_at {
        None => Some(None),
        Some(at) => {
            let condition = inner[at + "WHERE".len()..].trim();
            (!condition.is_empty()).then_some(Some(condition))
        }
    }
}

/// `ARRAY(SELECT x FROM … [ORDER BY o])`, the array constructor over a
/// subquery, as `COALESCE((SELECT array_agg(x ORDER BY o) FROM …), '{}')`:
/// the same elements in the same order, and the empty array (not NULL) for
/// no rows. A subquery with its own `LIMIT`, `OFFSET`, `DISTINCT`, grouping
/// or set operation is aggregated over as a derived table instead. As a
/// scalar subquery it is correlated where it reads the outer row, which the
/// correlated pass then joins. It was "a subquery used as a value returned 2
/// rows".
pub fn array_subqueries(sql: &str) -> Cow<'_, str> {
    if !contains_ignore_ascii_case(sql, b"array") {
        return Cow::Borrowed(sql);
    }
    let mut text = sql.to_string();
    let mut changed = false;
    // Innermost first is not needed: each pass rewrites the first one and
    // the result is scanned again, an inner one included.
    for _ in 0..32 {
        let Some(next) = rewrite_first_array_subquery(&text) else { break };
        text = next;
        changed = true;
    }
    if changed { Cow::Owned(text) } else { Cow::Borrowed(sql) }
}

fn rewrite_first_array_subquery(sql: &str) -> Option<String> {
    use crate::core::rql::join::find_keyword;
    let bytes = sql.as_bytes();
    let ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        let word_start = i == 0 || !(ident(bytes[i - 1]) || bytes[i - 1] == b'.');
        if word_start && bytes.len() - i >= 5 && bytes[i..i + 5].eq_ignore_ascii_case(b"ARRAY") && !bytes.get(i + 5).is_some_and(|&b| ident(b)) {
            let after = &sql[i + 5..];
            let open = i + 5 + (after.len() - after.trim_start().len());
            if bytes.get(open) == Some(&b'(') && let Some(close) = matching_paren_at(bytes, open) {
                let inner = sql[open + 1..close].trim();
                if starts_with_keyword(inner, "SELECT") {
                    let replacement = array_aggregate(inner)?;
                    return Some(format!("{}{}{}", &sql[..i], replacement, &sql[close + 1..]));
                }
            }
        }
        i += 1;
    }
    let _ = find_keyword;
    None
}

/// The aggregate an `ARRAY(…)` subquery is.
fn array_aggregate(query: &str) -> Option<String> {
    use crate::core::rql::join::find_keyword;
    // Where the first branch of a set operation ends, if there is one.
    let set_at = ["UNION", "INTERSECT", "EXCEPT"].iter().filter_map(|k| find_keyword(query, k, 0)).min();
    let from = match find_keyword(query, "FROM", 0).filter(|f| set_at.is_none_or(|s| *f < s)) {
        Some(from) => from,
        None => {
            // A first select list with no FROM — `ARRAY(SELECT 2 UNION ALL
            // SELECT 1 ORDER BY 1)`, `ARRAY(SELECT 1)` — read as the
            // aggregate over the whole query, its one item named. It was
            // taken for a scalar subquery ("returned 2 rows, but must
            // return at most one").
            let end = ["WHERE", "GROUP", "HAVING", "WINDOW", "ORDER", "LIMIT", "OFFSET", "FETCH"]
                .iter()
                .filter_map(|k| find_keyword(query, k, 0))
                .chain(set_at)
                .min()
                .unwrap_or(query.len());
            let list = query[6..end].trim();
            let items = split_top_level(list, b',');
            if items.len() != 1 || list.get(..8).is_some_and(|w| w.eq_ignore_ascii_case("DISTINCT")) {
                return None;
            }
            let (expr, _) = crate::core::rql::lexer_query::split_alias(items[0].trim());
            let aliased = format!("SELECT {} AS __arr_v {}", expr.trim(), &query[end..]);
            return Some(format!("COALESCE((SELECT array_agg(__arr_v) FROM ({aliased}) __arr), '{{}}')"));
        }
    };
    let list = query[6..from].trim();
    let items = split_top_level(list, b',');
    if items.len() != 1 || list.get(..8).is_some_and(|w| w.eq_ignore_ascii_case("DISTINCT")) {
        return None;
    }
    let (expr, _) = crate::core::rql::lexer_query::split_alias(items[0].trim());
    let complex = ["LIMIT", "OFFSET", "FETCH", "GROUP", "HAVING", "UNION", "INTERSECT", "EXCEPT", "DISTINCT", "WINDOW"]
        .iter()
        .any(|k| find_keyword(query, k, 0).is_some());
    if complex {
        let aliased = format!("SELECT {} AS __arr_v {}", expr.trim(), &query[from..]);
        return Some(format!("COALESCE((SELECT array_agg(__arr_v) FROM ({aliased}) __arr), '{{}}')"));
    }
    let body = &query[from..];
    let (rest, order) = match find_keyword(body, "ORDER BY", 0) {
        Some(o) => (&body[..o], Some(body[o + "ORDER BY".len()..].trim())),
        None => (body, None),
    };
    let aggregate = match order {
        Some(o) => {
            // `ORDER BY 1` names the one select item. Inside the aggregate a
            // bare 1 is a constant and sorts nothing, so the order was lost.
            let keys: Vec<String> = split_top_level(o, b',')
                .iter()
                .map(|key| {
                    let key = key.trim();
                    let digits = key.bytes().take_while(u8::is_ascii_digit).count();
                    if &key[..digits] == "1" && !key[digits..].starts_with(|c: char| c == '.' || is_identifier_byte(c as u8)) {
                        format!("({}){}", expr.trim(), &key[digits..])
                    } else {
                        key.to_string()
                    }
                })
                .collect();
            format!("array_agg({} ORDER BY {})", expr.trim(), keys.join(", "))
        }
        None => format!("array_agg({})", expr.trim()),
    };
    Some(format!("COALESCE((SELECT {aggregate} {}), '{{}}')", rest.trim()))
}

/// Whether `item` calls an aggregate (`max(…)`, `count(*)`, …) outside
/// quotes: the item of a select list that makes it one aggregate row.
fn contains_aggregate_call(item: &str) -> bool {
    const AGGREGATES: &[&str] = &[
        "count", "sum", "avg", "min", "max", "array_agg", "string_agg", "bool_and", "bool_or", "every",
        "json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg", "stddev", "stddev_pop", "stddev_samp",
        "variance", "var_pop", "var_samp", "bit_and", "bit_or", "xmlagg",
    ];
    let bytes = item.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        if (i == 0 || !is_identifier_byte(bytes[i - 1])) && is_identifier_byte(bytes[i]) {
            let end = i + bytes[i..].iter().take_while(|b| is_identifier_byte(**b)).count();
            let word = &item[i..end];
            if item[end..].trim_start().starts_with('(') && AGGREGATES.iter().any(|a| word.eq_ignore_ascii_case(a)) {
                return true;
            }
            i = end;
            continue;
        }
        i += 1;
    }
    false
}

/// An `UPDATE`'s row assignments as one assignment per column:
/// `SET (a, b) = (x, y)` (or `= ROW(x, y)`) is `SET a = x, b = y`, and
/// `SET (a, b) = (SELECT p, q FROM …)` is `SET a = (SELECT p FROM …), b =
/// (SELECT q FROM …)`. Each subquery is read under the statement's one
/// snapshot, so the columns come from the same row; more than one row is the
/// error a scalar subquery's is, as PostgreSQL's is. A volatile function in
/// it runs once per column rather than once.
///
/// `Ok(None)` when the statement has none; 42601's message when a side has
/// a different number of items than the other, as PostgreSQL refuses it.
pub fn row_assignments(sql: &str) -> Result<Option<String>, String> {
    use crate::core::rql::join::find_keyword;
    let head = sql.trim_start();
    if !(head.len() > 7 && head.as_bytes()[..6].eq_ignore_ascii_case(b"UPDATE") && head.as_bytes()[6].is_ascii_whitespace()) {
        return Ok(None);
    }
    let Some(set) = find_keyword(sql, "SET", 0) else { return Ok(None) };
    let list_start = set + 3;
    let list_end = ["FROM", "WHERE", "RETURNING"]
        .iter()
        .filter_map(|k| find_keyword(sql, k, list_start))
        .min()
        .unwrap_or(sql.len());
    let list = &sql[list_start..list_end];
    if !list.trim_start().starts_with('(') && !list.contains(", (") && !list.contains(",(") {
        return Ok(None);
    }
    let mut changed = false;
    let mut out_items: Vec<String> = Vec::new();
    for item in split_top_level(list, b',') {
        if !item.starts_with('(') {
            out_items.push(item.to_string());
            continue;
        }
        let Some(close) = matching_close(item, 0) else { return Ok(None) };
        let columns = split_top_level(&item[1..close], b',');
        let rest = item[close + 1..].trim_start();
        let Some(rhs) = rest.strip_prefix('=') else { return Ok(None) };
        let mut rhs = rhs.trim();
        let row = rhs.len() > 3 && rhs.as_bytes()[..3].eq_ignore_ascii_case(b"ROW") && rhs[3..].trim_start().starts_with('(');
        if row {
            rhs = rhs[3..].trim_start();
        }
        // A value that is no row at all, `(a, b) = 1` (also `(a, b) = (1)`
        // once its parentheses were folded away): PostgreSQL's 42601.
        if !rhs.starts_with('(') {
            return Err("source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression".to_string());
        }
        if matching_close(rhs, 0) != Some(rhs.len() - 1) {
            return Ok(None);
        }
        let inner = rhs[1..rhs.len() - 1].trim();
        let is_query = inner.len() > 6 && inner.as_bytes()[..6].eq_ignore_ascii_case(b"SELECT") && !is_identifier_byte(inner.as_bytes()[6]);
        if is_query {
            let from = find_keyword(inner, "FROM", 0).unwrap_or(inner.len());
            let items = split_top_level(&inner[6..from], b',');
            if items.len() != columns.len() {
                return Err("number of columns does not match number of values".to_string());
            }
            let tail = &inner[from..];
            // A list mixing aggregates with plain values is one aggregate row
            // (`SELECT max(n), 'z' FROM t`): split, the plain item was read
            // once per row of t ("returned 2 rows") or not at all when t had
            // none. Read through a derived table instead, still once per
            // column, which keeps the row whole.
            let aggregate = |item: &str| contains_aggregate_call(item);
            let mixed = items.iter().any(|i| aggregate(i))
                && items.iter().any(|i| !aggregate(i))
                && find_keyword(tail, "GROUP", 0).is_none();
            if mixed {
                let named: Vec<String> = items
                    .iter()
                    .enumerate()
                    .map(|(n, item)| {
                        let value = match find_keyword(item, "AS", 0) {
                            Some(at) => item[..at].trim(),
                            None => item.trim(),
                        };
                        format!("{value} AS __rdb_row{}", n + 1)
                    })
                    .collect();
                for (n, column) in columns.iter().enumerate() {
                    out_items.push(format!(
                        "{column} = (SELECT __rdb_row{} FROM (SELECT {} {tail}) AS __rdb_row)",
                        n + 1,
                        named.join(", ")
                    ));
                }
                changed = true;
                continue;
            }
            for (column, value) in columns.iter().zip(items) {
                out_items.push(format!("{column} = (SELECT {value} {tail})"));
            }
        } else {
            let values = split_top_level(inner, b',');
            // `(1)` is one parenthesised value, not a row: PostgreSQL's 42601.
            if values.len() == 1 && !row {
                return Err("source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression".to_string());
            }
            if values.len() != columns.len() {
                return Err("number of columns does not match number of values".to_string());
            }
            for (column, value) in columns.iter().zip(values) {
                out_items.push(format!("{column} = {value}"));
            }
        }
        changed = true;
    }
    if !changed {
        return Ok(None);
    }
    Ok(Some(format!("{} {} {}", sql[..list_start].trim_end(), out_items.join(", "), sql[list_end..].trim_start())))
}

/// A `WINDOW` clause's named windows written into each `OVER` that names
/// one, and the clause dropped: `OVER w … WINDOW w AS (ORDER BY x)` is
/// `OVER (ORDER BY x)`, and `OVER (w ROWS …)` extends it as `OVER (ORDER BY
/// x ROWS …)`. A window may name an earlier one (`WINDOW a AS (PARTITION BY
/// p), b AS (a ORDER BY x)`). The clause of the outermost SELECT only.
///
/// `Ok(None)` when there is none; 42P20's message for an `OVER` naming a
/// window the clause does not define.
pub fn named_windows(sql: &str) -> Result<Option<String>, String> {
    use crate::core::rql::join::find_keyword;
    if !contains_ignore_ascii_case(sql, b"over") {
        return Ok(None);
    }
    // With no clause, an `OVER name` still names a window that does not
    // exist (42P20, as PostgreSQL).
    let (at, clause_end) = match find_keyword(sql, "WINDOW", 0) {
        Some(at) => (
            at,
            ["ORDER", "LIMIT", "OFFSET", "FETCH", "FOR", "UNION", "INTERSECT", "EXCEPT"]
                .iter()
                .filter_map(|k| find_keyword(sql, k, at + "WINDOW".len()))
                .min()
                .unwrap_or(sql.len()),
        ),
        None => (sql.len(), sql.len()),
    };
    let clause_start = (at + "WINDOW".len()).min(clause_end);
    let mut windows: Vec<(String, String)> = Vec::new();
    for definition in split_top_level(&sql[clause_start..clause_end], b',') {
        let name_len = definition.bytes().take_while(|b| is_identifier_byte(*b)).count();
        let name = definition[..name_len].to_ascii_lowercase();
        let rest = definition[name_len..].trim_start();
        let Some(rest) = rest.get(..2).filter(|w| w.eq_ignore_ascii_case("AS")).map(|_| rest[2..].trim_start()) else {
            return Ok(None);
        };
        if name.is_empty() || !rest.starts_with('(') || matching_close(rest, 0) != Some(rest.len() - 1) {
            return Ok(None);
        }
        let spec = expand_window_spec(rest[1..rest.len() - 1].trim(), &windows)?;
        windows.push((name, spec));
    }
    let body = if at == sql.len() {
        sql.to_string()
    } else {
        format!("{} {}", sql[..at].trim_end(), sql[clause_end..].trim_start())
    };
    let bytes = body.as_bytes();
    let mut out = String::with_capacity(body.len() + 64);
    let mut copied = 0;
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        // `OVER` after a call's `)`: a column may be named `over`. Read as
        // bytes: `i` steps through every byte, and slicing the text there
        // panicked inside a multi-byte character.
        let after_call = || bytes[..i].iter().rposition(|b| !b.is_ascii_whitespace()).is_some_and(|p| bytes[p] == b')');
        let at_word = i == 0 || !is_identifier_byte(bytes[i - 1]);
        if at_word && bytes.len() - i > 4 && bytes[i..i + 4].eq_ignore_ascii_case(b"OVER") && !is_identifier_byte(bytes[i + 4]) && after_call() {
            let after = &body[i + 4..];
            let gap = after.len() - after.trim_start().len();
            let start = i + 4 + gap;
            if bytes.get(start) == Some(&b'(') {
                let Some(close) = matching_close(&body, start) else { return Ok(None) };
                let spec = body[start + 1..close].trim();
                let expanded = expand_window_spec(spec, &windows)?;
                if expanded != spec {
                    out.push_str(&body[copied..start]);
                    out.push_str(&format!("({expanded})"));
                    copied = close + 1;
                }
                i = close + 1;
                continue;
            }
            let name_len = body[start..].bytes().take_while(|b| is_identifier_byte(*b)).count();
            if name_len > 0 {
                let name = body[start..start + name_len].to_ascii_lowercase();
                let Some((_, spec)) = windows.iter().find(|(n, _)| *n == name) else {
                    return Err(format!("window \"{name}\" does not exist"));
                };
                out.push_str(&body[copied..start]);
                out.push_str(&format!("({spec})"));
                copied = start + name_len;
                i = copied;
                continue;
            }
        }
        i += 1;
    }
    if at == sql.len() {
        // No clause and no name used: nothing to write.
        return Ok(None);
    }
    out.push_str(&body[copied..]);
    Ok(Some(out))
}

/// A window specification with the window it opens with, when it names one
/// of `windows`, written out in front of the rest.
fn expand_window_spec(spec: &str, windows: &[(String, String)]) -> Result<String, String> {
    let name_len = spec.bytes().take_while(|b| is_identifier_byte(*b)).count();
    if name_len == 0 {
        return Ok(spec.to_string());
    }
    let first = spec[..name_len].to_ascii_lowercase();
    if ["partition", "order", "rows", "range", "groups"].contains(&first.as_str()) {
        return Ok(spec.to_string());
    }
    match windows.iter().find(|(n, _)| *n == first) {
        Some((_, base)) => Ok(format!("{} {}", base, spec[name_len..].trim()).trim().to_string()),
        None => Err(format!("window \"{first}\" does not exist")),
    }
}

/// `ANY`/`ALL`/`SOME` over a string literal or an `ARRAY[…]` with an array
/// cast (`'{1,2}'::int[]`, `ARRAY[1,2]::bigint[]`), without the cast. `None`
/// when there is none.
fn quantified_array_casts(sql: &str) -> Option<String> {
    if !(contains_ignore_ascii_case(sql, b"any") || contains_ignore_ascii_case(sql, b"all") || contains_ignore_ascii_case(sql, b"some"))
        || !sql.contains("[]")
    {
        return None;
    }
    let bytes = sql.as_bytes();
    let mut out = String::new();
    let mut copied = 0;
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        let at_word = i == 0 || !is_identifier_byte(bytes[i - 1]);
        // Compared as bytes. Slicing the text at `i + n` panicked when that
        // fell inside a multi-byte character: this runs at every byte, so
        // `name = 'Δ…'` cut the `Δ` at the `=` (an ORM UPDATE of a row with
        // Greek text and `'[]'`, and "all" anywhere in the statement).
        let word_len = [3usize, 4]
            .into_iter()
            .find(|&n| {
                bytes.len() - i > n
                    && [&b"ANY"[..], b"ALL", b"SOME"].iter().any(|w| bytes[i..i + n].eq_ignore_ascii_case(w))
                    && !is_identifier_byte(bytes[i + n])
            });
        if let (true, Some(n)) = (at_word, word_len) {
            let after = &sql[i + n..];
            let open = i + n + (after.len() - after.trim_start().len());
            if bytes.get(open) == Some(&b'(')
                && let Some(close) = matching_close(sql, open)
            {
                let inner = &sql[open + 1..close];
                // The value ends where its cast starts: after the literal's
                // closing quote, or the `ARRAY[…]`'s bracket.
                let value_end = if inner.trim_start().starts_with('\'') {
                    let start = open + 1 + (inner.len() - inner.trim_start().len());
                    Some(skip_quoted(bytes, start).unwrap_or(start))
                } else {
                    let t = inner.trim_start();
                    let start = open + 1 + (inner.len() - t.len());
                    (t.len() > 5 && t.as_bytes()[..5].eq_ignore_ascii_case(b"ARRAY") && t[5..].trim_start().starts_with('['))
                        .then(|| {
                            let bracket = start + 5 + (t[5..].len() - t[5..].trim_start().len());
                            matching_close(sql, bracket).map(|c| c + 1)
                        })
                        .flatten()
                };
                if let Some(value_end) = value_end {
                    let cast = sql[value_end..close].trim();
                    let is_array_cast = cast.starts_with("::")
                        && cast.ends_with("[]")
                        && cast[2..cast.len() - 2]
                            .bytes()
                            .all(|b| is_identifier_byte(b) || b == b' ' || b == b'(' || b == b')' || b == b',' || b == b'"' || b == b'.');
                    if is_array_cast {
                        out.push_str(&sql[copied..value_end]);
                        copied = close;
                    }
                }
                i = open + 1;
                continue;
            }
        }
        i += 1;
    }
    if copied == 0 {
        return None;
    }
    out.push_str(&sql[copied..]);
    Some(out)
}

/// `CREATE UNLOGGED TABLE` and `SELECT … INTO t` (see `statement_shorthands`).
fn unlogged_and_select_into(sql: &str) -> Option<String> {
    use crate::core::rql::join::find_keyword;
    let head = sql.trim_start();
    let words: Vec<&str> = head.splitn(4, char::is_whitespace).collect();
    if words.len() >= 3 && words[0].eq_ignore_ascii_case("CREATE") && words[1].eq_ignore_ascii_case("UNLOGGED") && words[2].eq_ignore_ascii_case("TABLE") {
        let at = head.find(words[1])?;
        return Some(format!("{}{}", &head[..at], head[at + words[1].len()..].trim_start()));
    }
    if !(head.len() > 7 && head.as_bytes()[..6].eq_ignore_ascii_case(b"SELECT") && head.as_bytes()[6].is_ascii_whitespace()) {
        return None;
    }
    let into = find_keyword(head, "INTO", 0)?;
    let target_start = into + "INTO".len();
    let target_end = ["FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION", "INTERSECT", "EXCEPT", "WINDOW", "OFFSET", "FETCH", "FOR"]
        .iter()
        .filter_map(|k| find_keyword(head, k, target_start))
        .min()
        .unwrap_or(head.len());
    let mut target: Vec<&str> = head[target_start..target_end].split_whitespace().collect();
    let mut temp = false;
    while let Some(first) = target.first() {
        match first.to_ascii_uppercase().as_str() {
            "TEMP" | "TEMPORARY" => temp = true,
            "UNLOGGED" | "TABLE" => {}
            _ => break,
        }
        target.remove(0);
    }
    let [name] = target.as_slice() else { return None };
    Some(format!(
        "CREATE {}TABLE {} AS {} {}",
        if temp { "TEMP " } else { "" },
        name,
        head[..into].trim_end(),
        head[target_end..].trim_start()
    ).trim_end().to_string())
}

/// `ROW(a, b)` as `(a, b)` (see `statement_shorthands`). A one-item `ROW(x)`
/// is kept: without its keyword it would be the parenthesised value.
fn row_constructors(sql: &str) -> Option<String> {
    if !contains_ignore_ascii_case(sql, b"row") {
        return None;
    }
    let bytes = sql.as_bytes();
    let mut out = String::new();
    let mut copied = 0;
    let mut i = 0;
    while i + 3 < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        let at_word = i == 0 || !(is_identifier_byte(bytes[i - 1]) || bytes[i - 1] == b'.');
        if at_word && bytes[i..i + 3].eq_ignore_ascii_case(b"ROW") && !is_identifier_byte(bytes[i + 3]) {
            let after = &sql[i + 3..];
            let open = i + 3 + (after.len() - after.trim_start().len());
            if bytes.get(open) == Some(&b'(')
                && let Some(close) = matching_close(sql, open)
                && split_top_level(&sql[open + 1..close], b',').len() >= 2
            {
                out.push_str(&sql[copied..i]);
                copied = open;
                i = open + 1;
                continue;
            }
        }
        i += 1;
    }
    if copied == 0 {
        return None;
    }
    out.push_str(&sql[copied..]);
    Some(out)
}

/// PostgreSQL's shorthands, as the statements they stand for:
///
/// - `x = ANY('{1,2}'::int[])` is `x = ANY('{1,2}')`, and `ANY(ARRAY[…]::int[])`
///   is `ANY(ARRAY[…])`: the elements are read as the left side's type
///   either way (`quantified_array_casts`). The cast was refused in a
///   `WHERE`, which is where every driver's `= ANY($1)` with an array
///   parameter arrives;
/// - `INSERT INTO t DEFAULT VALUES` is `INSERT INTO t VALUES (DEFAULT)`,
///   every column its default either way;
/// - `CREATE UNLOGGED TABLE` is `CREATE TABLE`: a table here is always
///   written ahead, which an unlogged one is allowed to be;
/// - `SELECT … INTO [TEMP] [TABLE] t FROM …` is `CREATE [TEMP] TABLE t AS
///   SELECT … FROM …`;
/// - `ROW(a, b)` is `(a, b)`, the row constructor with its keyword;
/// - `a [NOT] SIMILAR TO p [ESCAPE e]` is `a [!]~ similar_to_escape(p [,
///   e])`, as PostgreSQL itself reads it (`check::lower_similar_to`);
/// - `TABLE t …` is `SELECT * FROM t …`;
/// - a CTE's `AS MATERIALIZED (…)` / `AS NOT MATERIALIZED (…)` is `AS (…)`:
///   a planner hint, and every CTE here is materialised once whichever is
///   asked for, which is what each spelling's answer is.
///
/// Each was a syntax error. Borrowed when there is none.
/// A derived table's column list, `FROM (SELECT a, b FROM t) AS s(x, y)`,
/// as the names its first select list gives: `(SELECT a AS x, b AS y FROM
/// t) AS s`. The list was not read: `s.x` was a "missing FROM-clause entry",
/// and without `AS` the statement a syntax error. Fewer names than columns
/// rename the first ones, as PostgreSQL does; more, or a `*` among those
/// renamed, are left as written (`VALUES` lists are read elsewhere).
pub fn derived_column_lists(sql: &str) -> Option<String> {
    use crate::core::rql::join::find_keyword;
    if !contains_ignore_ascii_case(sql, b"select") || memchr::memchr(b'(', sql.as_bytes()).is_none() {
        return None;
    }
    const NOT_ALIASES: &[&str] = &[
        "AND", "OR", "NOT", "IN", "IS", "AS", "ON", "USING", "JOIN", "LEFT", "RIGHT", "FULL", "INNER", "CROSS",
        "NATURAL", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "WINDOW",
        "FETCH", "FOR", "LATERAL", "THEN", "ELSE", "WHEN", "END", "SELECT", "VALUES", "RETURNING", "SET",
    ];
    let bytes = sql.as_bytes();
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        if bytes[i] != b'(' {
            i += 1;
            continue;
        }
        let open = i;
        i += 1;
        let inner = sql[open + 1..].trim_start();
        if !(inner.len() > 6 && inner.as_bytes()[..6].eq_ignore_ascii_case(b"SELECT") && !is_identifier_byte(inner.as_bytes()[6])) {
            continue;
        }
        // In a FROM list: after FROM, JOIN, LATERAL or a comma.
        let before = sql[..open].trim_end();
        let word_start = before.bytes().rposition(|b| !is_identifier_byte(b)).map_or(0, |p| p + 1);
        let word = &before[word_start..];
        if !(before.ends_with(',') || ["FROM", "JOIN", "LATERAL"].iter().any(|k| word.eq_ignore_ascii_case(k))) {
            continue;
        }
        let Some(close) = matching_close(sql, open) else { return None };
        // `[AS] alias (names)`.
        let mut rest = &sql[close + 1..];
        let mut at = close + 1;
        let skip_ws = |rest: &mut &str, at: &mut usize| {
            let t = rest.trim_start();
            *at += rest.len() - t.len();
            *rest = t;
        };
        skip_ws(&mut rest, &mut at);
        let clause_at = at;
        if rest.len() > 2 && rest.as_bytes()[..2].eq_ignore_ascii_case(b"AS") && !is_identifier_byte(rest.as_bytes()[2]) {
            rest = &rest[2..];
            at += 2;
            skip_ws(&mut rest, &mut at);
        }
        let alias_len = if rest.starts_with('"') {
            quoted_end(rest.as_bytes(), 0, b'"')
        } else {
            rest.bytes().take_while(|b| is_identifier_byte(*b)).count()
        };
        if alias_len == 0 || rest.as_bytes()[0].is_ascii_digit() {
            continue;
        }
        let alias = &rest[..alias_len];
        if NOT_ALIASES.iter().any(|k| alias.eq_ignore_ascii_case(k)) {
            continue;
        }
        let alias_end = at + alias_len;
        let mut list = &sql[alias_end..];
        let mut list_at = alias_end;
        skip_ws(&mut list, &mut list_at);
        if !list.starts_with('(') {
            continue;
        }
        let Some(list_close) = matching_close(sql, list_at) else { continue };
        let names: Vec<&str> = split_top_level(&sql[list_at + 1..list_close], b',').into_iter().map(str::trim).collect();
        let plain = |n: &&str| {
            (n.starts_with('"') && n.ends_with('"') && n.len() >= 2)
                || (!n.is_empty() && n.bytes().all(is_identifier_byte) && !n.as_bytes()[0].is_ascii_digit())
        };
        if names.is_empty() || !names.iter().all(plain) {
            continue;
        }
        // The first select list of the derived table.
        let body_at = open + 1 + (sql[open + 1..].len() - inner.len());
        let body = &sql[body_at..close];
        let list_start = 6;
        let list_end = ["FROM", "WHERE", "GROUP", "HAVING", "WINDOW", "ORDER", "LIMIT", "OFFSET", "FETCH", "UNION", "INTERSECT", "EXCEPT"]
            .iter()
            .filter_map(|k| find_keyword(body, k, list_start))
            .min()
            .unwrap_or(body.len());
        let select_list = &body[list_start..list_end];
        let mut items_text = select_list.trim_start();
        let mut lead = select_list.len() - items_text.len();
        for kw in ["DISTINCT", "ALL"] {
            if items_text.len() > kw.len() && items_text.as_bytes()[..kw.len()].eq_ignore_ascii_case(kw.as_bytes()) && !is_identifier_byte(items_text.as_bytes()[kw.len()]) {
                let after = items_text[kw.len()..].trim_start();
                let after = if kw == "DISTINCT" && after.len() > 2 && after.as_bytes()[..2].eq_ignore_ascii_case(b"ON") && after[2..].trim_start().starts_with('(') {
                    let on_open = after.len() - after[2..].trim_start().len();
                    let Some(on_close) = matching_close(after, on_open) else { break };
                    after[on_close + 1..].trim_start()
                } else {
                    after
                };
                lead += items_text.len() - after.len();
                items_text = after;
            }
        }
        let items = split_top_level(items_text, b',');
        if names.len() > items.len() {
            continue;
        }
        let mut renamed: Vec<String> = Vec::with_capacity(items.len());
        let mut ok = true;
        for (n, item) in items.iter().enumerate() {
            let item = item.trim();
            let Some(name) = names.get(n) else {
                renamed.push(item.to_string());
                continue;
            };
            if item == "*" || item.ends_with(".*") {
                ok = false;
                break;
            }
            let mut last_as = None;
            let mut from = 0;
            while let Some(p) = find_keyword(item, "AS", from) {
                last_as = Some(p);
                from = p + 2;
            }
            let expr = match last_as {
                Some(p) => item[..p].trim_end(),
                None => bare_alias_cut(item),
            };
            renamed.push(format!("{expr} AS {name}"));
        }
        if !ok {
            continue;
        }
        let items_at = body_at + list_start + lead;
        let gap = if list_end < body.len() { " " } else { "" };
        edits.push((items_at, body_at + list_end, format!("{}{gap}", renamed.join(", "))));
        edits.push((clause_at, list_close + 1, format!("AS {alias}")));
    }
    if edits.is_empty() {
        return None;
    }
    edits.sort_by_key(|e| e.0);
    let mut out = String::with_capacity(sql.len() + 16);
    let mut copied = 0;
    for (start, end, text) in edits {
        if start < copied {
            return None;
        }
        out.push_str(&sql[copied..start]);
        out.push_str(&text);
        copied = end;
    }
    out.push_str(&sql[copied..]);
    Some(out)
}

/// A select item without its alias when it ends in one written without
/// `AS` (`count(*) n`, `t.a x`); the item itself otherwise.
fn bare_alias_cut(item: &str) -> &str {
    const ENDS_AN_EXPRESSION: &[&str] = &["END", "NULL", "TRUE", "FALSE", "UNKNOWN", "DESC", "ASC", "FIRST", "LAST", "ZONE"];
    let trimmed = item.trim_end();
    let word_start = trimmed.bytes().rposition(|b| !is_identifier_byte(b)).map_or(0, |p| p + 1);
    let word = &trimmed[word_start..];
    if word_start == 0 || word.is_empty() || word.as_bytes()[0].is_ascii_digit() || ENDS_AN_EXPRESSION.iter().any(|k| word.eq_ignore_ascii_case(k)) {
        return trimmed;
    }
    let before = &trimmed[..word_start];
    // Only a space separates an alias from what it names.
    if !before.ends_with(char::is_whitespace) {
        return trimmed;
    }
    let head = before.trim_end();
    match head.bytes().last() {
        Some(b) if is_identifier_byte(b) || matches!(b, b')' | b'\'' | b'"' | b']') => {
            // `a IS x`, `x COLLATE y`: a keyword before the word makes it no alias.
            let prev_start = head.bytes().rposition(|b| !is_identifier_byte(b)).map_or(0, |p| p + 1);
            let prev = &head[prev_start..];
            if ["IS", "NOT", "COLLATE", "AT", "AND", "OR", "LIKE", "ILIKE", "IN", "BETWEEN", "SIMILAR", "ESCAPE", "THEN", "ELSE", "WHEN", "CASE", "DISTINCT", "INTERVAL"]
                .iter()
                .any(|k| prev.eq_ignore_ascii_case(k))
            {
                return trimmed;
            }
            head
        }
        _ => trimmed,
    }
}

pub fn statement_shorthands(sql: &str) -> Cow<'_, str> {
    let mut text = Cow::Borrowed(sql);
    if let Some(t) = quantified_array_casts(&text) {
        text = Cow::Owned(t);
    }
    if let Some(t) = unlogged_and_select_into(&text) {
        text = Cow::Owned(t);
    }
    if let Some(t) = row_constructors(&text) {
        text = Cow::Owned(t);
    }
    if let Some(t) = derived_column_lists(&text) {
        text = Cow::Owned(t);
    }
    if let Some(t) = text_operators(&text) {
        text = Cow::Owned(t);
    }
    // Only in a select list: a `WHERE`'s, `ON`'s or `HAVING`'s `SIMILAR TO`
    // is the predicate matcher's, which pads a `char(n)` value and refuses
    // a pattern too deep (2201B) or too costly (54001) as PostgreSQL does;
    // lowered, those went to the regular-expression reader and answered
    // differently.
    if contains_ignore_ascii_case(&text, b"similar") {
        let lowered = crate::core::rql::check::lower_similar_to_where(&text, &in_select_list);
        if lowered != *text {
            text = Cow::Owned(lowered);
        }
    }
    if contains_ignore_ascii_case(&text, b"default")
        && let Some(at) = crate::core::rql::join::find_phrase(&text, &["DEFAULT", "VALUES"], 0)
        && text.trim_start().get(..6).is_some_and(|w| w.eq_ignore_ascii_case("INSERT"))
    {
        let after = &text[at + "DEFAULT".len()..];
        let values_end = at + "DEFAULT".len() + (after.len() - after.trim_start().len()) + "VALUES".len();
        text = Cow::Owned(format!("{}VALUES (DEFAULT){}", &text[..at], &text[values_end..]));
    }
    let trimmed = text.trim_start();
    if trimmed.len() > 6
        && trimmed.as_bytes()[..5].eq_ignore_ascii_case(b"TABLE")
        && trimmed.as_bytes()[5].is_ascii_whitespace()
    {
        let rest = trimmed[6..].trim_start();
        text = Cow::Owned(format!("SELECT * FROM {rest}"));
    }
    if contains_ignore_ascii_case(&text, b"materialized") {
        let bytes = text.as_bytes();
        let mut out = String::with_capacity(text.len());
        let mut copied = 0;
        let mut i = 0;
        while i < bytes.len() {
            if let Some(end) = skip_quoted(bytes, i) {
                i = end.max(i + 1);
                continue;
            }
            let at_word = i == 0 || !is_identifier_byte(bytes[i - 1]);
            if at_word && bytes.len() - i >= 2 && bytes[i..i + 2].eq_ignore_ascii_case(b"AS") && bytes.get(i + 2).is_some_and(|b| b.is_ascii_whitespace()) {
                let mut j = i + 2;
                let skip_ws = |mut k: usize| {
                    while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                        k += 1;
                    }
                    k
                };
                j = skip_ws(j);
                let word = |k: usize, w: &[u8]| {
                    bytes.len() - k >= w.len()
                        && bytes[k..k + w.len()].eq_ignore_ascii_case(w)
                        && bytes.get(k + w.len()).is_none_or(|b| !is_identifier_byte(*b))
                };
                if word(j, b"NOT") {
                    j = skip_ws(j + 3);
                }
                if word(j, b"MATERIALIZED") {
                    let k = skip_ws(j + "MATERIALIZED".len());
                    if bytes.get(k) == Some(&b'(') {
                        out.push_str(&text[copied..i]);
                        out.push_str("AS ");
                        copied = k;
                        i = k;
                        continue;
                    }
                }
            }
            i += 1;
        }
        if copied > 0 {
            out.push_str(&text[copied..]);
            text = Cow::Owned(out);
        }
    }
    text
}

/// `sql` with every tab, line break, vertical tab and form feed outside
/// quotes made a space: whitespace to PostgreSQL, and one kind of it to the
/// parsers downstream, many of which test for a keyword followed by a space
/// (`CASE ` read `CASE` ending its line as a column named `CASE WHEN …`).
/// A string, a quoted identifier and a dollar-quoted body keep theirs, so a
/// routine's PL/pgSQL keeps its lines. Comments are expected gone already
/// (a line comment's end is its line break). Borrowed when nothing changes.
pub fn flatten_whitespace(sql: &str) -> Cow<'_, str> {
    let bytes = sql.as_bytes();
    let odd = |b: u8| matches!(b, b'\n' | b'\r' | b'\t' | 0x0b | 0x0c);
    if !bytes.iter().any(|&b| odd(b)) {
        return Cow::Borrowed(sql);
    }
    let mut out = bytes.to_vec();
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' || b == b'"' {
            i = quoted_end(bytes, i, b).max(i + 1);
            continue;
        }
        // A `$` after an identifier's character continues the identifier,
        // as PostgreSQL's lexer reads it; elsewhere it may open a body.
        if b == b'$' && !(i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_')) {
            if let Some(end) = dollar_quote_end(bytes, i) {
                i = end.max(i + 1);
                continue;
            }
        }
        if odd(b) {
            out[i] = b' ';
        }
        i += 1;
    }
    // Only ASCII bytes were replaced, by ASCII.
    Cow::Owned(String::from_utf8(out).expect("whitespace replaced by a space"))
}

/// Index just past the `quote`-delimited run opening at `start`, where a
/// doubled quote stands for itself (or the end of the text, unterminated).
pub fn quoted_end(bytes: &[u8], start: usize, quote: u8) -> usize {
    let mut i = start + 1;
    while let Some(p) = memchr::memchr(quote, &bytes[i..]) {
        i += p;
        if bytes.get(i + 1) == Some(&quote) {
            i += 2;
            continue;
        }
        return i + 1;
    }
    bytes.len()
}

/// If a dollar quote opens at `start`, the length of its tag (`$$` is 2,
/// `$tag$` is 5). `None` when this `$` opens nothing — a `$1` placeholder
/// among them, since a tag cannot begin with a digit.
pub fn dollar_tag_len(bytes: &[u8], start: usize) -> Option<usize> {
    let mut j = start + 1;
    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_' || bytes[j] >= 0x80) {
        if j == start + 1 && bytes[j].is_ascii_digit() {
            return None;
        }
        j += 1;
    }
    (j < bytes.len() && bytes[j] == b'$').then(|| j + 1 - start)
}

/// If a dollar-quoted string opens at `start`, the index just past its closing
/// tag — the end of the text when it is unterminated. `None` when this `$` is
/// not a dollar quote.
pub fn dollar_quote_end(bytes: &[u8], start: usize) -> Option<usize> {
    let tag_len = dollar_tag_len(bytes, start)?;
    let tag = &bytes[start..start + tag_len];
    let body = start + tag_len;
    Some(
        memchr::memmem::find(&bytes[body..], tag)
            .map_or(bytes.len(), |p| body + p + tag_len),
    )
}

/// Index just past the (possibly nested) block comment opening at `start`.
pub fn block_comment_end(bytes: &[u8], start: usize) -> usize {
    let mut depth = 1usize;
    let mut i = start + 2;
    while i < bytes.len() && depth > 0 {
        if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
            depth -= 1;
            i += 2;
        } else {
            i += 1;
        }
    }
    i
}

/// If a quoted run — string, escape string, dollar-quoted string or quoted
/// identifier — opens at `i`, the index just past it. Nothing inside one is
/// syntax.
#[inline]
pub fn skip_quoted(bytes: &[u8], i: usize) -> Option<usize> {
    match bytes[i] {
        b'\'' => Some(string_literal_end(bytes, i, is_escape_string_quote(bytes, i))),
        b'"' => Some(quoted_end(bytes, i, b'"')),
        b'$' if i == 0 || !is_identifier_byte(bytes[i - 1]) => dollar_quote_end(bytes, i),
        _ => None,
    }
}

/// The first quoted identifier in `sql` holding a doubled quote (`"a""b"`,
/// the name `a"b`), as written; `None` when there is none. Strings, escape
/// strings, dollar quotes and comments are skipped.
///
/// The engine's identifier readers each strip a name's quotes their own way,
/// and none reads `""` as a quote: `CREATE TABLE "t"" x ""y" (…)` created a
/// table named `t`, and such a column failed with an unrelated error. Until
/// they share one reader, a statement naming such an identifier is refused
/// where it enters, before anything is created under a different name.
pub fn identifier_with_doubled_quote(sql: &str) -> Option<&str> {
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_comment(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        if bytes[i] == b'"' {
            let end = quoted_end(bytes, i, b'"');
            // Between the opening quote and the closing one.
            let inner = &bytes[i + 1..end.saturating_sub(1).max(i + 1)];
            if memchr::memmem::find(inner, b"\"\"").is_some() {
                return Some(&sql[i..end]);
            }
            i = end;
            continue;
        }
        if let Some(end) = skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        i += 1;
    }
    None
}

/// The identifier `text` starts with (after whitespace), and the offset just
/// past it: a double-quoted one read whole, its spaces and case kept and
/// `""` read as `"`; a bare one up to the next whitespace, quotes trimmed, as
/// the readers that split on whitespace always took it. `None` for an empty
/// or unterminated name.
///
/// The one reader the statement parsers share for a name that may hold a
/// space: split on whitespace, `"c d" TEXT` was the column `c` of a type
/// `d" TEXT`, refused as "column type 'D"' is not supported yet".
pub fn leading_identifier(text: &str) -> Option<(String, usize)> {
    let bytes = text.as_bytes();
    let start = bytes.iter().position(|b| !b.is_ascii_whitespace())?;
    if bytes[start] == b'"' {
        let mut name = Vec::new();
        let mut i = start + 1;
        loop {
            match bytes.get(i)? {
                b'"' if bytes.get(i + 1) == Some(&b'"') => {
                    name.push(b'"');
                    i += 2;
                }
                b'"' => {
                    i += 1;
                    break;
                }
                b => {
                    name.push(*b);
                    i += 1;
                }
            }
        }
        let name = String::from_utf8(name).ok()?;
        return (!name.is_empty()).then_some((name, i));
    }
    let end = bytes[start..].iter().position(|b| b.is_ascii_whitespace()).map_or(bytes.len(), |n| start + n);
    let name = text[start..end].trim_matches('"').to_string();
    (!name.is_empty()).then_some((name, end))
}

/// Every qualified column reference in `text`: the byte span of its
/// qualifier including the final dot (`public.t.` in `public.t.id`, `t.` in
/// `t.*`), and the qualifier's names unquoted (`["public", "t"]`).
///
/// Quotes, comments and numbers are skipped, and so is a parenthesised
/// subquery, which has its own FROM. A schema-qualified call
/// (`pg_catalog.now()`) and what follows `::` are not column references.
fn qualified_references(text: &str) -> Vec<(usize, usize, Vec<String>)> {
    let bytes = text.as_bytes();
    // No dot, no qualifier: every statement's texts are asked, most of
    // them hold none, and the scan below is the per-statement cost.
    if memchr::memchr(b'.', bytes).is_none() {
        return Vec::new();
    }
    let ident_start = |b: u8| b.is_ascii_alphabetic() || b == b'_' || b == b'"' || b >= 0x80;
    let ident_byte = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80;
    // One name at `i`: (where it starts, the offset past it), read without
    // a copy; only a qualifier's names are made into strings.
    let read = |i: usize| -> Option<(usize, usize)> {
        if bytes.get(i) == Some(&b'"') {
            let end = quoted_end(bytes, i, b'"');
            text.get(i + 1..end.saturating_sub(1))?;
            return Some((i, end));
        }
        let mut j = i;
        while j < bytes.len() && ident_byte(bytes[j]) {
            j += 1;
        }
        (j > i).then_some((i, j))
    };
    // A name as a qualifier carries it: kept quoted, since a quoted name is
    // matched by its exact case.
    let name_of = |(start, end): (usize, usize)| -> String {
        if bytes[start] == b'"' {
            let inner = &text[start + 1..end.saturating_sub(1)];
            format!("\"{}\"", inner.replace("\"\"", "\""))
        } else {
            text[start..end].to_string()
        }
    };
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_comment(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        let b = bytes[i];
        if b == b'\'' || b == b'$' && dollar_tag_len(bytes, i).is_some() {
            i = skip_quoted(bytes, i).unwrap_or(i + 1).max(i + 1);
            continue;
        }
        if b == b'(' {
            // A subquery's names are its own.
            let inner = text[i + 1..].trim_start();
            if ["SELECT", "WITH", "VALUES"].iter().any(|keyword| starts_with_keyword(inner, keyword)) {
                i = matching_close(text, i).map_or(bytes.len(), |c| c + 1);
                continue;
            }
            i += 1;
            continue;
        }
        if b.is_ascii_digit() {
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.' || bytes[i] == b'_') {
                i += 1;
            }
            continue;
        }
        if b == b':' && bytes.get(i + 1) == Some(&b':') {
            // A cast's type name, which may be schema-qualified.
            i += 2;
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            while i < bytes.len() && (ident_byte(bytes[i]) || matches!(bytes[i], b'.' | b'"')) {
                i += 1;
            }
            continue;
        }
        if ident_start(b) && (i == 0 || !ident_byte(bytes[i - 1])) {
            // The whole dotted chain: each name's span.
            let mut names: smallvec::SmallVec<[(usize, usize); 4]> = smallvec::SmallVec::new();
            let mut j = i;
            let mut star = false;
            while let Some((start, end)) = read(j) {
                names.push((start, end));
                if bytes.get(end) == Some(&b'.') {
                    if bytes.get(end + 1) == Some(&b'*') {
                        star = true;
                        j = end + 2;
                        break;
                    }
                    if bytes.get(end + 1).is_some_and(|c| ident_start(*c)) {
                        j = end + 1;
                        continue;
                    }
                }
                j = end;
                break;
            }
            if names.is_empty() {
                i += 1;
                continue;
            }
            // `q.name(` is a schema-qualified function, not a column.
            let call = text[j..].trim_start().starts_with('(');
            let qualifiers = if star { names.len() } else { names.len() - 1 };
            if qualifiers > 0 && !call {
                let span_end = names[qualifiers - 1].1 + 1;
                out.push((i, span_end, names[..qualifiers].iter().map(|n| name_of(*n)).collect()));
            }
            i = j.max(i + 1);
            continue;
        }
        i += 1;
    }
    out
}

/// Whether the qualifier `names` is one of `allowed`, each a dotted path
/// (`t`, `public.t`), compared name by name without regard to ASCII case.
fn qualifier_is(names: &[String], allowed: &str) -> bool {
    let parts: Vec<&str> = allowed.split('.').collect();
    parts.len() == names.len()
        && parts.iter().zip(names).all(|(a, n)| {
            // A quoted name is its exact spelling (`"T"` is not `t`), a bare
            // one folds (`T` is `t`), as PostgreSQL reads them. An allowed
            // name written quoted is held to that; one written bare may be a
            // catalogue name some caller unquoted, so it matches either way.
            let a: &str = a;
            let quoted_n = n.strip_prefix('"').and_then(|n| n.strip_suffix('"'));
            match a.strip_prefix('"').and_then(|a| a.strip_suffix('"')) {
                Some(a) => match quoted_n {
                    Some(quoted) => quoted == a,
                    None => n.to_ascii_lowercase() == a,
                },
                None => match quoted_n {
                    Some(quoted) => quoted == a,
                    None => a.eq_ignore_ascii_case(n),
                },
            }
        })
}

/// A qualifier's names as a message shows them, unquoted.
fn written(names: &[String]) -> String {
    names.iter().map(|n| n.trim_matches('"')).collect::<Vec<_>>().join(".")
}

/// The first qualifier in `text` (`q` in `q.col`, `public.t` in
/// `public.t.col`) that is none of `allowed`, as written; `None` when every
/// one names a relation the statement has.
///
/// A single-table statement's readers take the last segment of `q.col` and
/// drop the qualifier, so a qualifier naming no relation was ignored:
/// `DELETE FROM t WHERE nosuch.id = 1` deleted t's row 1, and `WHERE
/// nosuch.v > 0 OR id = 2` read t's `v`. PostgreSQL refuses both ("missing
/// FROM-clause entry", 42P01). `allowed` is what [`relation_qualifiers`]
/// gives, plus what a statement adds (`EXCLUDED`).
pub fn unknown_qualifier(text: &str, allowed: &[&str]) -> Option<String> {
    qualified_references(text)
        .into_iter()
        .find(|(_, _, names)| !allowed.iter().any(|a| qualifier_is(names, a)))
        .map(|(_, _, names)| written(&names))
}

/// [`unknown_qualifier`], [`respell_qualifier`] and [`strip_qualifiers`] in
/// one scan, for a text an `ON CONFLICT DO UPDATE` rewrites every row:
/// `Err` with the first qualifier none of `allowed` names (as written);
/// otherwise the text with `respell_from`'s qualifier written `respell_to.`
/// and each of `strip`'s removed.
pub fn resolve_qualifiers(
    text: &str,
    allowed: &[&str],
    respell_from: &str,
    respell_to: &str,
    strip: &[&str],
) -> Result<String, String> {
    let refs = qualified_references(text);
    let mut out = String::with_capacity(text.len());
    let mut from = 0;
    for (start, end, names) in refs {
        if !allowed.iter().any(|a| qualifier_is(&names, a)) {
            return Err(written(&names));
        }
        if qualifier_is(&names, respell_from) {
            out.push_str(&text[from..start]);
            out.push_str(respell_to);
            out.push('.');
            from = end;
        } else if strip.iter().any(|s| qualifier_is(&names, s)) {
            out.push_str(&text[from..start]);
            from = end;
        }
    }
    out.push_str(&text[from..]);
    Ok(out)
}

/// Whether `text` is exactly `q.*` for a `q` in `allowed` (`t.*`,
/// `public.t.*`, `"x".*`).
pub fn is_qualified_star(text: &str, allowed: &[&str]) -> bool {
    let text = text.trim();
    let Some(head) = text.strip_suffix(".*") else { return false };
    let refs = qualified_references(text);
    refs.len() == 1
        && refs[0].0 == 0
        && refs[0].1 == head.len() + 1
        && allowed.iter().any(|a| qualifier_is(&refs[0].2, a))
}

/// `text` with each qualifier that is one of `strip` removed: `t0.v` as `v`,
/// `"public"."t"."id"` as `"id"`.
pub fn strip_qualifiers(text: &str, strip: &[&str]) -> String {
    let mut out = String::with_capacity(text.len());
    let mut from = 0;
    for (start, end, names) in qualified_references(text) {
        if strip.iter().any(|s| qualifier_is(&names, s)) {
            out.push_str(&text[from..start]);
            from = end;
        }
    }
    out.push_str(&text[from..]);
    out
}

/// `text` with each qualifier that is `from` written as `to.`: `"excluded"."v"`
/// as `EXCLUDED."v"`, so one spelling is left for a reader that looks for it.
pub fn respell_qualifier(text: &str, from: &str, to: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut from_at = 0;
    for (start, end, names) in qualified_references(text) {
        if qualifier_is(&names, from) {
            out.push_str(&text[from_at..start]);
            out.push_str(to);
            out.push('.');
            from_at = end;
        }
    }
    out.push_str(&text[from_at..]);
    out
}

/// The last segment of a possibly schema-qualified name as written
/// (`public.t` → `t`, `"public"."T"` → `"T"`, `"a.b"` → `"a.b"`): dots inside
/// double quotes do not split it. Quotes are kept for the caller to trim.
///
/// The catalogue has one namespace and every reader resolves a qualified
/// relation by its last segment, so a name that is stored must be stored the
/// same way: `CREATE TABLE "public"."q1"` once stored `public"."q1`, which no
/// reader, and not even `DROP TABLE`, could name again.
pub fn last_name_segment(raw: &str) -> &str {
    let bytes = raw.as_bytes();
    let mut i = 0;
    let mut start = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => i = quoted_end(bytes, i, b'"'),
            b'.' => {
                start = i + 1;
                i += 1;
            }
            _ => i += 1,
        }
    }
    let last = &raw[start..];
    if last.is_empty() {
        raw
    } else {
        last
    }
}

/// The qualifiers a single relation is known by: its alias alone when it has
/// one (PostgreSQL hides the name behind it), else its name and its name in
/// the default schema (`t`, `public.t`), and the schema-qualified name as
/// written when that is another schema.
pub fn relation_qualifiers(relation: &str, alias: Option<&str>) -> Vec<String> {
    if let Some(alias) = alias {
        // A quoted alias keeps its quotes: [`qualifier_is`] then holds it to
        // its exact spelling, as PostgreSQL does (`AS "X"` is not `x`).
        return vec![alias.to_string()];
    }
    let parts: Vec<String> = relation.split('.').map(|p| p.trim_matches('"').to_string()).collect();
    let name = parts.last().cloned().unwrap_or_default();
    let mut out = vec![name.clone(), format!("public.{name}")];
    if parts.len() > 1 {
        out.push(parts.join("."));
    }
    out
}

/// The refusal for a qualifier [`unknown_qualifier`] found, in PostgreSQL's
/// words (42P01).
pub fn missing_from_entry(qualifier: &str) -> String {
    format!("missing FROM-clause entry for table \"{qualifier}\"")
}

/// Whether `needle` occurs anywhere in `text`, ignoring ASCII case, quotes
/// and comments included: a scanner's cheap precondition, so one that can
/// only match where these bytes are can skip a statement that has none.
/// `needle` must not be empty.
pub fn contains_ignore_ascii_case(text: &str, needle: &[u8]) -> bool {
    let b = text.as_bytes();
    let first = needle[0];
    memchr::memchr2_iter(first.to_ascii_uppercase(), first.to_ascii_lowercase(), b)
        .any(|i| b.len() - i >= needle.len() && b[i..i + needle.len()].eq_ignore_ascii_case(needle))
}

/// If a comment opens at `i`, the index just past it (a line comment ends
/// before its newline).
#[inline]
pub fn skip_comment(bytes: &[u8], i: usize) -> Option<usize> {
    match (bytes[i], bytes.get(i + 1)) {
        (b'-', Some(b'-')) => {
            Some(memchr::memchr(b'\n', &bytes[i..]).map_or(bytes.len(), |p| i + p))
        }
        (b'/', Some(b'*')) => Some(block_comment_end(bytes, i)),
        _ => None,
    }
}

/// The typed literals [`typed_literals_as_casts`] rewrites: the words of the
/// type, longest spelling first, and the cast it becomes.
const TYPED_LITERAL_TYPES: &[(&[&str], &str)] = &[
    (&["TIMESTAMP", "WITH", "TIME", "ZONE"], "timestamptz"),
    (&["TIMESTAMP", "WITHOUT", "TIME", "ZONE"], "timestamp"),
    (&["TIME", "WITHOUT", "TIME", "ZONE"], "time"),
    (&["TIME", "WITH", "TIME", "ZONE"], "timetz"),
    (&["BIT", "VARYING"], "varbit"),
    (&["DOUBLE", "PRECISION"], "float8"),
    (&["TIMESTAMPTZ"], "timestamptz"),
    (&["TIMESTAMP"], "timestamp"),
    (&["TIME"], "time"),
    (&["DATE"], "date"),
    (&["INTERVAL"], "interval"),
    (&["NUMERIC"], "numeric"),
    (&["DECIMAL"], "numeric"),
    (&["INTEGER"], "integer"),
    (&["INT"], "integer"),
    (&["INT4"], "integer"),
    (&["INT2"], "smallint"),
    (&["SMALLINT"], "smallint"),
    (&["INT8"], "bigint"),
    (&["BIGINT"], "bigint"),
    (&["BOOLEAN"], "boolean"),
    (&["BOOL"], "boolean"),
    (&["REAL"], "real"),
    (&["FLOAT4"], "real"),
    (&["FLOAT8"], "float8"),
    (&["TEXT"], "text"),
    (&["VARCHAR"], "varchar"),
    (&["UUID"], "uuid"),
    (&["JSON"], "json"),
    (&["JSONB"], "jsonb"),
    // The network, geometric, range and binary types: `inet '10.0.0.5'`,
    // `point '(1,2)'`, `int4range '[1,5)'` were read as a column of that
    // name ("column \"inet '10.0.0.5'\" does not exist").
    (&["TIMETZ"], "timetz"),
    (&["INET"], "inet"),
    (&["CIDR"], "cidr"),
    (&["MACADDR8"], "macaddr8"),
    (&["MACADDR"], "macaddr"),
    (&["POINT"], "point"),
    (&["LINE"], "line"),
    (&["LSEG"], "lseg"),
    (&["BOX"], "box"),
    (&["PATH"], "path"),
    (&["POLYGON"], "polygon"),
    (&["CIRCLE"], "circle"),
    (&["INT4RANGE"], "int4range"),
    (&["INT8RANGE"], "int8range"),
    (&["NUMRANGE"], "numrange"),
    (&["DATERANGE"], "daterange"),
    (&["TSRANGE"], "tsrange"),
    (&["TSTZRANGE"], "tstzrange"),
    (&["BYTEA"], "bytea"),
    (&["VARBIT"], "varbit"),
];

/// SQL's typed literal — `DATE '2024-01-02'`, `NUMERIC '1.50'`, `TIMESTAMP
/// WITH TIME ZONE '…'` — rewritten as the cast it means (`'2024-01-02'::date`),
/// which every reader of a value already handles; `None` when there is none.
///
/// Read as written, the prefix form reached a text column as its own source
/// characters (`DATE '2024-01-02'` stored as `DATE '2024-01-02'`, silently),
/// was refused by a NUMERIC or BOOLEAN column, and failed to plan against a
/// DATE column in a `WHERE`. Strings, quoted identifiers, dollar quotes and
/// comments are skipped; a type word is one only at a word boundary and
/// directly before a standard string, so `AT TIME ZONE 'UTC'` and a column
/// named `date` are left alone, and so is `INTERVAL '1' DAY` (a field
/// qualifier the cast form has no place for).
pub fn typed_literals_as_casts(sql: &str) -> Option<String> {
    if !sql.contains('\'') {
        return None;
    }
    let bytes = sql.as_bytes();
    let mut out: Option<String> = None;
    let mut copied = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        let boundary = i == 0 || !(is_identifier_byte(bytes[i - 1]) || matches!(bytes[i - 1], b'.' | b':' | b'"'));
        if !(boundary && bytes[i].is_ascii_alphabetic()) {
            i += 1;
            continue;
        }
        let word_end = |from: usize| bytes[from..].iter().position(|b| !is_identifier_byte(*b)).map_or(bytes.len(), |p| from + p);
        let found = TYPED_LITERAL_TYPES.iter().find_map(|(words, cast)| {
            let mut at = i;
            for (n, word) in words.iter().enumerate() {
                if n > 0 {
                    let gap = bytes[at..].iter().take_while(|b| b.is_ascii_whitespace()).count();
                    if gap == 0 {
                        return None;
                    }
                    at += gap;
                }
                let end = word_end(at);
                if !sql[at..end].eq_ignore_ascii_case(word) {
                    return None;
                }
                at = end;
            }
            let gap = bytes[at..].iter().take_while(|b| b.is_ascii_whitespace()).count();
            let quote = at + gap;
            // A standard string directly after the type, not an escape string
            // (`E'…'` begins with a letter, so it never matches here).
            if gap == 0 || bytes.get(quote) != Some(&b'\'') {
                return None;
            }
            let close = string_literal_end(bytes, quote, false);
            if close > bytes.len() || bytes[close - 1] != b'\'' || close == quote + 1 {
                return None;
            }
            if *cast == "interval" {
                let after = sql[close..].trim_start();
                let next = &after[..after.bytes().position(|b| !is_identifier_byte(b)).unwrap_or(after.len())];
                if ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"].iter().any(|f| next.eq_ignore_ascii_case(f)) {
                    return None;
                }
            }
            Some((quote, close, *cast))
        });
        match found {
            Some((quote, close, cast)) => {
                let o = out.get_or_insert_with(|| String::with_capacity(sql.len() + 16));
                o.push_str(&sql[copied..i]);
                o.push_str(&sql[quote..close]);
                o.push_str("::");
                o.push_str(cast);
                copied = close;
                i = close;
            }
            None => i = word_end(i),
        }
    }
    out.map(|mut o| {
        o.push_str(&sql[copied..]);
        o
    })
}

/// `x = ANY (SELECT …)` and `x = SOME (SELECT …)` as `x IN (SELECT …)`,
/// and `x <> ALL (SELECT …)` as `x NOT IN (SELECT …)`: the standard defines
/// them as the same predicates, NULLs included, so the rewrite holds in
/// every context. Read as they were, the subquery reached the scalar
/// substitution and failed as "a subquery used as a value returned 2 rows".
///
/// Any other operator with a quantified subquery (`> ANY`, `= ALL`, `<> ANY`)
/// is refused as not supported: answering it through `min` or `max` holds
/// in a WHERE but not under `NOT` when the subquery yields a NULL. `ANY`
/// over an array (`= ANY (ARRAY[…])`, `= ANY ('{…}')`) is left alone.
/// `Ok(None)` when there is nothing to rewrite.
pub fn quantified_subqueries(sql: &str) -> Result<Option<String>, String> {
    let bytes = sql.as_bytes();
    let mut out: Option<String> = None;
    let mut copied = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        let boundary = i == 0 || !(is_identifier_byte(bytes[i - 1]) || matches!(bytes[i - 1], b'.' | b'"'));
        if !(boundary && bytes[i].is_ascii_alphabetic()) {
            i += 1;
            continue;
        }
        let word_end = bytes[i..].iter().position(|b| !is_identifier_byte(*b)).map_or(bytes.len(), |p| i + p);
        let word = &sql[i..word_end];
        let quantifier = ["ANY", "SOME", "ALL"].into_iter().find(|q| word.eq_ignore_ascii_case(q));
        let Some(quantifier) = quantifier else {
            i = word_end;
            continue;
        };
        // `(` then `SELECT` or `WITH`: a subquery, not an array.
        let open = word_end + bytes[word_end..].iter().take_while(|b| b.is_ascii_whitespace()).count();
        let subquery = bytes.get(open) == Some(&b'(') && {
            let inner = sql[open + 1..].trim_start();
            let head = &inner[..inner.bytes().position(|b| !is_identifier_byte(b)).unwrap_or(inner.len())];
            head.eq_ignore_ascii_case("SELECT") || head.eq_ignore_ascii_case("WITH")
        };
        if !subquery {
            i = word_end;
            continue;
        }
        // The comparison operator just before the quantifier.
        let before = sql[..i].trim_end();
        let op = ["<>", "!=", "<=", ">=", "=", "<", ">"].into_iter().find(|op| before.ends_with(op));
        let Some(op) = op else {
            i = word_end;
            continue;
        };
        let op_at = before.len() - op.len();
        let replacement = match (op, quantifier.eq_ignore_ascii_case("ALL")) {
            ("=", false) => "IN",
            ("<>" | "!=", true) => "NOT IN",
            // Answered with the subquery's values in hand, by the executor
            // (`find_quantified_subquery`).
            _ => {
                i = word_end;
                continue;
            }
        };
        let o = out.get_or_insert_with(|| String::with_capacity(sql.len() + 8));
        o.push_str(&sql[copied..op_at]);
        // `x=ANY(` has no space before the operator to keep.
        if op_at > 0 && !bytes[op_at - 1].is_ascii_whitespace() {
            o.push(' ');
        }
        o.push_str(replacement);
        o.push(' ');
        copied = open;
        i = open;
    }
    Ok(out.map(|mut o| {
        o.push_str(&sql[copied..]);
        o
    }))
}

/// A comparison with a quantified subquery, `x > ANY (SELECT …)`: where its
/// operand begins, the operator, whether it is `ALL`, where the subquery's
/// parenthesis closes (exclusive end), and the subquery.
#[derive(Debug, Clone, PartialEq)]
pub struct QuantifiedSubquery {
    pub start: usize,
    pub operand: String,
    pub op: &'static str,
    pub all: bool,
    pub end: usize,
    pub inner_sql: String,
}

/// The first `x op ANY|SOME|ALL (SELECT …)` of `sql` outside quotes, or
/// `None`. `Err` when its operand cannot be told apart from what precedes it.
pub fn find_quantified_subquery(sql: &str) -> Option<Result<QuantifiedSubquery, String>> {
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        let boundary = i == 0 || !(is_identifier_byte(bytes[i - 1]) || matches!(bytes[i - 1], b'.' | b'"'));
        if !(boundary && bytes[i].is_ascii_alphabetic()) {
            i += 1;
            continue;
        }
        let word_end = bytes[i..].iter().position(|b| !is_identifier_byte(*b)).map_or(bytes.len(), |p| i + p);
        let word = &sql[i..word_end];
        let Some(quantifier) = ["ANY", "SOME", "ALL"].into_iter().find(|q| word.eq_ignore_ascii_case(q)) else {
            i = word_end;
            continue;
        };
        let open = word_end + bytes[word_end..].iter().take_while(|b| b.is_ascii_whitespace()).count();
        let subquery = bytes.get(open) == Some(&b'(') && {
            let inner = sql[open + 1..].trim_start();
            let head = &inner[..inner.bytes().position(|b| !is_identifier_byte(b)).unwrap_or(inner.len())];
            head.eq_ignore_ascii_case("SELECT") || head.eq_ignore_ascii_case("WITH")
        };
        let before = sql[..i].trim_end();
        let op = ["<>", "!=", "<=", ">=", "=", "<", ">"].into_iter().find(|op| before.ends_with(op));
        let (true, Some(op)) = (subquery, op) else {
            i = word_end;
            continue;
        };
        let Some(close) = matching_close(sql, open) else {
            return Some(Err("a quantified subquery is missing its closing parenthesis".to_string()));
        };
        let op_at = before.len() - op.len();
        let Some(start) = operand_start(sql, op_at) else {
            return Some(Err(format!(
                "the left operand of {op} {} (subquery) could not be read; parenthesise it",
                quantifier.to_ascii_uppercase()
            )));
        };
        return Some(Ok(QuantifiedSubquery {
            start,
            operand: sql[start..op_at].trim().to_string(),
            op: if op == "!=" { "<>" } else { op },
            all: quantifier.eq_ignore_ascii_case("ALL"),
            end: close + 1,
            inner_sql: sql[open + 1..close].trim().to_string(),
        }));
    }
    None
}

/// Whether `sql[at..]` stands in a select list: the nearest clause keyword
/// before it, at its own query level, is `SELECT` (not `FROM`, `WHERE`,
/// `ON`, `HAVING`, …). Parentheses that are no subquery (a call, a CASE's
/// group) are looked through; a subquery's own opening ends the search.
pub(crate) fn in_select_list(sql: &str, at: usize) -> bool {
    const ENDS: &[&str] = &[
        "FROM", "WHERE", "ON", "HAVING", "GROUP", "ORDER", "LIMIT", "OFFSET", "SET", "VALUES", "RETURNING", "USING",
        "JOIN", "CHECK", "WINDOW", "FETCH", "INTO",
    ];
    let bytes = sql.as_bytes();
    // Which bytes are inside a quoted run or a comment.
    let mut quoted = vec![false; bytes.len()];
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            let end = end.max(i + 1).min(bytes.len());
            quoted[i..end].iter_mut().for_each(|q| *q = true);
            i = end;
            continue;
        }
        i += 1;
    }
    let mut depth = 0i32;
    let mut k = at.min(bytes.len());
    while k > 0 {
        k -= 1;
        if quoted[k] {
            continue;
        }
        match bytes[k] {
            b')' => depth += 1,
            b'(' => {
                if depth > 0 {
                    depth -= 1;
                } else {
                    // An enclosing parenthesis: a subquery's ends the level.
                    let inner = sql[k + 1..].trim_start();
                    let head: String = inner.bytes().take_while(|b| is_identifier_byte(*b)).map(|b| b as char).collect();
                    if head.eq_ignore_ascii_case("SELECT") || head.eq_ignore_ascii_case("WITH") {
                        return false;
                    }
                }
            }
            b if depth == 0 && is_identifier_byte(b) && (k == 0 || !is_identifier_byte(bytes[k - 1])) => {
                let end = k + bytes[k..].iter().take_while(|b| is_identifier_byte(**b)).count();
                let word = &sql[k..end];
                if word.eq_ignore_ascii_case("SELECT") {
                    return true;
                }
                if ENDS.iter().any(|e| word.eq_ignore_ascii_case(e)) {
                    return false;
                }
            }
            _ => {}
        }
    }
    false
}

/// PostgreSQL's `text ^@ prefix` and `(s1, e1) OVERLAPS (s2, e2)`, written
/// with what the engine reads: `starts_with(text, prefix)` and the `CASE`
/// PostgreSQL's `overlaps_timestamp` computes. Both were refused ("column
/// "'a' ^@ 'b'" does not exist"). `None` when `sql` has neither.
///
/// `OVERLAPS` follows PostgreSQL: each pair is ordered first, a pair's end
/// may be an interval (`(start, INTERVAL '1 day')`, read off the text),
/// periods sharing their start overlap, and a missing end gives NULL where
/// the answer depends on it.
pub fn text_operators(sql: &str) -> Option<String> {
    if !sql.contains("^@") && !contains_ignore_ascii_case(sql, b"overlaps") {
        return None;
    }
    let mut text = sql.to_string();
    let mut changed = false;
    // Each round rewrites the first one found; bounded for a statement that
    // keeps one we cannot read.
    for _ in 0..64 {
        let Some((start, end, replacement)) = next_text_operator(&text) else { break };
        text.replace_range(start..end, &replacement);
        changed = true;
    }
    changed.then_some(text)
}

fn next_text_operator(sql: &str) -> Option<(usize, usize, String)> {
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        if bytes[i] == b'^' && bytes.get(i + 1) == Some(&b'@') {
            if let Some(start) = operand_start(sql, i)
                && let Some(end) = operand_end(sql, i + 2)
            {
                let left = sql[start..i].trim();
                let right = sql[i + 2..end].trim();
                // PostgreSQL names an operator's column `?column?`.
                let name = if whole_select_item(sql, start, end) { " AS \"?column?\"" } else { "" };
                return Some((start, end, format!("starts_with({left}, {right}){name}")));
            }
            i += 2;
            continue;
        }
        let boundary = i == 0 || !is_identifier_byte(bytes[i - 1]);
        if boundary
            && bytes.len() >= i + 8
            && bytes[i..i + 8].eq_ignore_ascii_case(b"OVERLAPS")
            && bytes.get(i + 8).is_none_or(|b| !is_identifier_byte(*b))
            && let Some(rewritten) = overlaps_at(sql, i)
        {
            return Some(rewritten);
        }
        i += 1;
    }
    None
}

/// A query without the parentheses around a branch that group nothing:
/// `(SELECT …) EXCEPT (SELECT …) ORDER BY 1` as `SELECT … EXCEPT SELECT …
/// ORDER BY 1`, and `(SELECT …)` alone as the SELECT. Borrowed when there is
/// nothing to remove.
///
/// The planners downstream read a branch as written from its `SELECT`, and a
/// parenthesised one took a narrower path: a correlated subquery or an
/// `EXISTS` in it was refused, where the same branch without parentheses ran.
/// A branch keeps its parentheses when they do group: it has an `ORDER BY`,
/// `LIMIT`, `OFFSET`, `FETCH` or `FOR` of its own. One that is itself a set
/// operation, whose order of evaluation they decide, is written as the
/// derived table it means (`(a UNION b) INTERSECT c` read its group as a
/// relation with no name).
pub fn plain_branch_parentheses(sql: &str) -> std::borrow::Cow<'_, str> {
    use crate::core::rql::join::find_keyword;
    use std::borrow::Cow;
    const OPERATORS: [&str; 3] = ["UNION", "INTERSECT", "EXCEPT"];
    const OWN_TAIL: [&str; 5] = ["ORDER", "LIMIT", "OFFSET", "FETCH", "FOR"];
    let head = sql.trim_start();
    let opens = head.starts_with('(');
    // A point read has no parenthesis at all; anything else is a query
    // with a parenthesis and, unless it opens with one, a set operation.
    if !opens
        && !(starts_with_keyword(head, "SELECT")
            && memchr::memchr(b'(', sql.as_bytes()).is_some()
            && OPERATORS.iter().any(|kw| contains_ignore_ascii_case(sql, kw.as_bytes())))
    {
        return Cow::Borrowed(sql);
    }
    let text = sql.trim().trim_end_matches(';').trim_end();
    let operator = |text: &str, from: usize| {
        OPERATORS.iter().filter_map(|kw| find_keyword(text, kw, from).map(|p| (p, kw.len()))).min_by_key(|(p, _)| *p)
    };
    // The branches: what lies between the top-level operators.
    let mut pieces: Vec<(usize, usize)> = Vec::new();
    let mut at = 0usize;
    loop {
        let Some((p, len)) = operator(text, at) else {
            pieces.push((at, text.len()));
            break;
        };
        pieces.push((at, p));
        let mut end = p + len;
        let rest = &text[end..];
        for word in ["ALL", "DISTINCT"] {
            if starts_with_keyword(rest.trim_start(), word) {
                end += (rest.len() - rest.trim_start().len()) + word.len();
                break;
            }
        }
        at = end;
    }
    let alone = pieces.len() == 1;
    let mut out = String::with_capacity(text.len() + 32);
    let mut copied = 0usize;
    for (number, (start, end)) in pieces.into_iter().enumerate() {
        let lead = text[start..end].len() - text[start..end].trim_start().len();
        let open = start + lead;
        if text.as_bytes().get(open) != Some(&b'(') {
            continue;
        }
        let Some(close) = matching_close(text, open).filter(|close| *close < end) else {
            continue;
        };
        // What follows a branch's parenthesis is the whole's `ORDER BY` or
        // `LIMIT` (after the last branch) or nothing.
        let after = text[close + 1..end].trim();
        if !after.is_empty() && !OWN_TAIL.iter().any(|kw| starts_with_keyword(after, kw)) {
            continue;
        }
        // Parentheses in parentheses group what the inner ones do, and the
        // group may be branches in parentheses of their own.
        let mut inner = text[open + 1..close].trim();
        while inner.starts_with('(') && matching_close(inner, 0) == Some(inner.len() - 1) {
            inner = inner[1..inner.len() - 1].trim();
        }
        let within = plain_branch_parentheses(inner);
        let inner = within.as_ref();
        if !starts_with_keyword(inner, "SELECT") {
            continue;
        }
        let set_operation = operator(inner, 0).is_some();
        let own_tail = OWN_TAIL.iter().any(|kw| find_keyword(inner, kw, 0).is_some());
        let replacement = if alone && (after.is_empty() || !own_tail) {
            // The whole query: the same without them.
            inner.to_string()
        } else if alone || own_tail {
            continue;
        } else if set_operation {
            format!("SELECT * FROM ({inner}) __rdb_branch{number}")
        } else {
            inner.to_string()
        };
        out.push_str(&text[copied..open]);
        out.push_str(&replacement);
        copied = close + 1;
    }
    if copied == 0 {
        return Cow::Borrowed(sql);
    }
    out.push_str(&text[copied..]);
    Cow::Owned(out)
}

/// `text` without the parentheses around the whole of it, however many:
/// `((SELECT 1))` is `SELECT 1`, and `SELECT 9, round(2.5, 1)` is itself.
/// Trimming every leading `(` and trailing `)` took the last call's own
/// parenthesis with them ("trailing text after 'round(2.567, 1)'").
pub fn without_outer_parentheses(text: &str) -> &str {
    let mut text = text.trim();
    while text.starts_with('(') && matching_close(text, 0) == Some(text.len() - 1) {
        text = text[1..text.len() - 1].trim();
    }
    text
}

/// [`plain_branch_parentheses`] for every query written inside parentheses,
/// at any depth: `x IN ((SELECT 1) UNION (SELECT 2))`, `EXISTS ((SELECT …)
/// EXCEPT (SELECT …))`, `((SELECT 1) UNION (SELECT 1))` as a value, a
/// derived table, a CTE's body. Borrowed when nothing changes.
///
/// A subquery's readers take its text from the `SELECT` after the
/// parenthesis: one that opened with another parenthesis was "column
/// \"SELECT\" does not exist" or "EXISTS must contain a SELECT".
pub fn nested_branch_parentheses(sql: &str) -> std::borrow::Cow<'_, str> {
    use std::borrow::Cow;
    // Only a set operation has branches to write in parentheses.
    if !["UNION", "INTERSECT", "EXCEPT"].iter().any(|kw| contains_ignore_ascii_case(sql, kw.as_bytes())) {
        return Cow::Borrowed(sql);
    }
    let mut out: Option<String> = None;
    let mut i = 0usize;
    loop {
        let text = out.as_deref().unwrap_or(sql);
        let bytes = text.as_bytes();
        if i >= bytes.len() {
            break;
        }
        if bytes[i] != b'(' {
            i = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)).map_or(i + 1, |end| end.max(i + 1));
            continue;
        }
        // A group that opens with a parenthesis or with a query; anything
        // else is stepped into without looking for where it closes.
        let first = text[i + 1..].trim_start();
        if !(first.starts_with('(') || starts_with_keyword(first, "SELECT")) {
            i += 1;
            continue;
        }
        let Some(close) = matching_close(text, i) else {
            i += 1;
            continue;
        };
        if let Cow::Owned(plain) = plain_branch_parentheses(&text[i + 1..close]) {
            if starts_with_keyword(plain.trim_start(), "SELECT") {
                out = Some(format!("{}{}{}", &text[..=i], plain, &text[close..]));
            }
        }
        // On into the group, as it now reads.
        i += 1;
    }
    out.map_or(Cow::Borrowed(sql), Cow::Owned)
}

/// `INSERT INTO t [(columns)] (SELECT …)` and `CREATE TABLE t AS (SELECT …)`
/// with the query that supplies their rows read as the top level reads one
/// ([`plain_branch_parentheses`]). Borrowed when the query is not in
/// parentheses.
///
/// A parenthesised query there was read as a value: "a subquery used as a
/// value must select exactly one column", or with one column and one row, a
/// table of that one value.
pub fn source_query_parentheses(sql: &str) -> std::borrow::Cow<'_, str> {
    use crate::core::rql::join::find_keyword;
    use std::borrow::Cow;
    let lead = sql.len() - sql.trim_start().len();
    let head = &sql[lead..];
    let bytes = sql.as_bytes();
    let skip_ws = |mut at: usize| {
        while bytes.get(at).is_some_and(|b| b.is_ascii_whitespace()) {
            at += 1;
        }
        at
    };
    // Where the query starts and where it ends.
    let (start, end) = if starts_with_keyword(head, "INSERT") {
        let mut at = skip_ws(lead + 6);
        if !starts_with_keyword(&sql[at..], "INTO") {
            return Cow::Borrowed(sql);
        }
        at = skip_ws(at + 4);
        // The table's name, quoted or not, to a space or a parenthesis.
        while let Some(&b) = bytes.get(at) {
            if b == b'"' {
                match skip_quoted(bytes, at) {
                    Some(end) => at = end,
                    None => return Cow::Borrowed(sql),
                }
            } else if b.is_ascii_whitespace() || b == b'(' {
                break;
            } else {
                at += 1;
            }
        }
        at = skip_ws(at);
        if starts_with_keyword(&sql[at..], "AS") {
            return Cow::Borrowed(sql);
        }
        if bytes.get(at) != Some(&b'(') {
            return Cow::Borrowed(sql);
        }
        let Some(close) = matching_close(sql, at) else {
            return Cow::Borrowed(sql);
        };
        // A column list holds names; a query opens with its keyword or with
        // a parenthesis of its own.
        let first = sql[at + 1..close].trim_start();
        let is_query = |text: &str| text.starts_with('(') || starts_with_keyword(text, "SELECT") || starts_with_keyword(text, "WITH");
        if !is_query(first) {
            at = skip_ws(close + 1);
            if starts_with_keyword(&sql[at..], "OVERRIDING") {
                let Some(value) = find_keyword(sql, "VALUE", at) else {
                    return Cow::Borrowed(sql);
                };
                at = skip_ws(value + 5);
            }
            if bytes.get(at) != Some(&b'(') {
                return Cow::Borrowed(sql);
            }
            let Some(close) = matching_close(sql, at) else {
                return Cow::Borrowed(sql);
            };
            if !is_query(sql[at + 1..close].trim_start()) {
                return Cow::Borrowed(sql);
            }
        }
        // `ON CONFLICT` and `RETURNING` are the statement's, after its query.
        let end = ["ON", "RETURNING"].iter().filter_map(|kw| find_keyword(sql, kw, at)).min().unwrap_or(sql.len());
        (at, end)
    } else if starts_with_keyword(head, "CREATE") {
        let Some(parsed) = crate::core::rql::ctas::parse(sql) else {
            return Cow::Borrowed(sql);
        };
        if !sql[parsed.query_span.clone()].trim_start().starts_with('(') {
            return Cow::Borrowed(sql);
        }
        (parsed.query_span.start, parsed.query_span.end)
    } else {
        return Cow::Borrowed(sql);
    };
    let query = sql[start..end].trim_end().trim_end_matches(';');
    match plain_branch_parentheses(query) {
        Cow::Owned(plain) if !plain.trim_start().starts_with('(') => {
            Cow::Owned(format!("{}{} {}", &sql[..start], plain, sql[start + query.len()..].trim_start()).trim_end().to_string())
        }
        _ => Cow::Borrowed(sql),
    }
}

/// Whether `sql[start..end]` is a whole item of a select list: at depth
/// zero after the nearest `SELECT`, between it or a comma and a comma,
/// `FROM` or the end.
pub(crate) fn whole_select_item(sql: &str, start: usize, end: usize) -> bool {
    let before = sql[..start].trim_end();
    let after = sql[end..].trim_start();
    let after_ok = after.is_empty()
        || after.starts_with([',', ';'])
        || (after.len() >= 4 && after.as_bytes()[..4].eq_ignore_ascii_case(b"FROM") && after.as_bytes().get(4).is_none_or(|b| !is_identifier_byte(*b)));
    if !after_ok || !(before.ends_with(',') || before.to_ascii_uppercase().ends_with("SELECT")) {
        return false;
    }
    let upper = before.to_ascii_uppercase();
    let Some(select) = upper.rfind("SELECT") else { return false };
    let between = &before[select + 6..];
    let mut depth = 0i32;
    let b = between.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if let Some(e) = skip_quoted(b, i) {
            i = e.max(i + 1);
            continue;
        }
        match b[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        i += 1;
    }
    if depth != 0 {
        return false;
    }
    let words = between.to_ascii_uppercase();
    !["FROM", "WHERE", " BY", "HAVING", "WHEN", "THEN", " ON "].iter().any(|k| words.contains(k))
}

/// `(a, b) OVERLAPS (c, d)` around the keyword at `at`.
fn overlaps_at(sql: &str, at: usize) -> Option<(usize, usize, String)> {
    let before = sql[..at].trim_end();
    if !before.ends_with(')') {
        return None;
    }
    let start = operand_start(sql, before.len())?;
    if sql.as_bytes()[start] != b'(' || matching_close(sql, start)? + 1 != before.len() {
        return None;
    }
    let after = at + 8;
    let open = after + (sql[after..].len() - sql[after..].trim_start().len());
    if sql.as_bytes().get(open) != Some(&b'(') {
        return None;
    }
    let close = matching_close(sql, open)?;
    let pair = |inner: &str| -> Option<(String, String)> {
        let parts = split_top_level(inner, b',');
        let [a, b] = parts.as_slice() else { return None };
        let (a, b) = (format!("({a})"), b.to_string());
        let interval = {
            let t = b.trim_start();
            (t.len() > 8 && t.as_bytes()[..8].eq_ignore_ascii_case(b"INTERVAL") && !is_identifier_byte(t.as_bytes()[8]))
                || contains_ignore_ascii_case(&b, b"::interval")
        };
        let b = if interval { format!("({a} + ({b}))") } else { format!("({b})") };
        // Ordered as PostgreSQL orders a pair: the earlier first; a missing
        // start takes the end's place and leaves the end missing.
        let s = format!("(CASE WHEN {a} > {b} THEN {b} ELSE coalesce({a}, {b}) END)");
        let e = format!("(CASE WHEN {a} > {b} THEN {a} WHEN {a} <= {b} THEN {b} END)");
        Some((s, e))
    };
    let (s1, e1) = pair(&sql[start + 1..before.len() - 1])?;
    let (s2, e2) = pair(&sql[open + 1..close])?;
    let unknown = format!("{e1} IS NULL OR {e2} IS NULL");
    let text = format!(
        "(CASE WHEN {s1} > {s2} THEN (CASE WHEN {s1} < {e2} THEN true WHEN {unknown} THEN NULL ELSE false END) \
         WHEN {s1} < {s2} THEN (CASE WHEN {s2} < {e1} THEN true WHEN {unknown} THEN NULL ELSE false END) \
         WHEN {s1} = {s2} THEN (CASE WHEN {unknown} THEN NULL ELSE true END) END)"
    );
    let name = if whole_select_item(sql, start, close + 1) { " AS overlaps" } else { "" };
    Some((start, close + 1, format!("{text}{name}")))
}

/// Where the operand starting at `from` ends: a constant, a typed literal
/// (`DATE '…'`), a column, a call or a group, with its casts and
/// subscripts. `None` when none starts there.
fn operand_end(sql: &str, from: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let skip_ws = |mut i: usize| {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        i
    };
    let mut i = skip_ws(from);
    if bytes.get(i) == Some(&b'-') {
        i = skip_ws(i + 1);
    }
    let c = *bytes.get(i)?;
    i = match c {
        b'(' => matching_close(sql, i)? + 1,
        b'\'' => quoted_end(bytes, i, b'\''),
        b'0'..=b'9' | b'.' => i + bytes[i..].iter().take_while(|b| b.is_ascii_digit() || **b == b'.').count(),
        b'$' => i + 1 + bytes[i + 1..].iter().take_while(|b| b.is_ascii_digit()).count(),
        c if c == b'"' || is_identifier_byte(c) => {
            // A dotted name, then a call's arguments or a typed literal.
            let mut k = i;
            loop {
                k = if bytes.get(k) == Some(&b'"') {
                    quoted_end(bytes, k, b'"')
                } else {
                    k + bytes[k..].iter().take_while(|b| is_identifier_byte(**b)).count()
                };
                if bytes.get(k) == Some(&b'.') {
                    k += 1;
                    continue;
                }
                break;
            }
            let next = skip_ws(k);
            match bytes.get(next) {
                Some(b'(') if next == k => matching_close(sql, next)? + 1,
                Some(b'\'') if bytes[i..k].iter().all(|b| b.is_ascii_alphabetic()) => quoted_end(bytes, next, b'\''),
                _ => k,
            }
        }
        _ => return None,
    };
    // `::type`, `::type(n)`, `[i]`.
    loop {
        let next = skip_ws(i);
        if bytes.get(next) == Some(&b':') && bytes.get(next + 1) == Some(&b':') {
            let mut k = skip_ws(next + 2);
            k += bytes[k..].iter().take_while(|b| is_identifier_byte(**b) || **b == b'.').count();
            if bytes.get(k) == Some(&b'(') {
                k = matching_close(sql, k)? + 1;
            }
            while bytes.get(k) == Some(&b'[') && bytes.get(k + 1) == Some(&b']') {
                k += 2;
            }
            i = k;
        } else if bytes.get(next) == Some(&b'[') && next == i {
            i = matching_close(sql, next)? + 1;
        } else {
            return Some(i);
        }
    }
}

/// `FROM t [[AS] x] TABLESAMPLE BERNOULLI (p)` (or `SYSTEM`) as the derived
/// table `(SELECT * FROM t WHERE random() * 100 < (p)) AS x`: each row kept
/// with chance `p` percent. It was refused ("TABLESAMPLE is not supported").
/// `SYSTEM` samples pages in PostgreSQL and rows here; either is a sample of
/// that size. PostgreSQL's errors: an unknown method (42704), a literal
/// percentage outside 0..=100 (2202H). `REPEATABLE (seed)` is refused
/// (0A000): a per-row `random()` cannot draw the same sample twice.
pub fn tablesample(sql: &str) -> Result<Option<String>, (&'static str, String)> {
    if !contains_ignore_ascii_case(sql, b"tablesample") {
        return Ok(None);
    }
    let mut text = sql.to_string();
    let mut changed = false;
    for _ in 0..32 {
        let Some(at) = crate::core::rql::join::find_keyword(&text, "TABLESAMPLE", 0) else { break };
        let (start, end, replacement) = tablesample_at(&text, at)?;
        text.replace_range(start..end, &replacement);
        changed = true;
    }
    Ok(changed.then_some(text))
}

fn tablesample_at(sql: &str, at: usize) -> Result<(usize, usize, String), (&'static str, String)> {
    let syntax = || ("42601", "syntax error at or near \"TABLESAMPLE\"".to_string());
    let bytes = sql.as_bytes();
    // The words before the keyword: `t`, `t x`, `t AS x`.
    let word_before = |end: usize| -> Option<(usize, usize)> {
        let e = sql[..end].trim_end().len();
        let s = sql[..e].bytes().rposition(|b| !(is_identifier_byte(b) || b == b'.' || b == b'"')).map_or(0, |p| p + 1);
        (s < e).then_some((s, e))
    };
    let (w1s, w1e) = word_before(at).ok_or_else(syntax)?;
    let is_kw = |s: usize, e: usize, k: &str| sql[s..e].eq_ignore_ascii_case(k);
    let opens_list = |s: usize| {
        let before = sql[..s].trim_end();
        before.ends_with(',')
            || ["FROM", "JOIN"].iter().any(|k| {
                let Some(w) = before.len().checked_sub(k.len()) else { return false };
                // Bytes: `w` can fall inside a character before the keyword.
                before.as_bytes()[w..].eq_ignore_ascii_case(k.as_bytes()) && (w == 0 || !is_identifier_byte(before.as_bytes()[w - 1]))
            })
    };
    let (relation, alias, start) = if opens_list(w1s) {
        (&sql[w1s..w1e], None, w1s)
    } else {
        let (w2s, w2e) = word_before(w1s).ok_or_else(syntax)?;
        if is_kw(w2s, w2e, "AS") {
            let (rs, re) = word_before(w2s).ok_or_else(syntax)?;
            (&sql[rs..re], Some(&sql[w1s..w1e]), rs)
        } else {
            (&sql[w2s..w2e], Some(&sql[w1s..w1e]), w2s)
        }
    };
    // `METHOD (args) [REPEATABLE (seed)]`.
    let mut i = at + "TABLESAMPLE".len();
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let method_end = i + bytes[i..].iter().take_while(|b| is_identifier_byte(**b)).count();
    let method = &sql[i..method_end];
    if !(method.eq_ignore_ascii_case("BERNOULLI") || method.eq_ignore_ascii_case("SYSTEM")) {
        return Err(("42704", format!("tablesample method {} does not exist", method.to_ascii_lowercase())));
    }
    let open = method_end + (sql[method_end..].len() - sql[method_end..].trim_start().len());
    if bytes.get(open) != Some(&b'(') {
        return Err(syntax());
    }
    let close = matching_close(sql, open).ok_or_else(syntax)?;
    let arg = sql[open + 1..close].trim();
    if arg.is_empty() || split_top_level(arg, b',').len() != 1 {
        return Err(("42883", format!("tablesample method {} requires 1 argument, not {}", method.to_ascii_lowercase(), split_top_level(arg, b',').len())));
    }
    if let Ok(p) = arg.parse::<f64>()
        && !(0.0..=100.0).contains(&p)
    {
        return Err(("2202H", "sample percentage must be between 0 and 100".to_string()));
    }
    let after = sql[close + 1..].trim_start();
    if after.len() >= 10 && after.as_bytes()[..10].eq_ignore_ascii_case(b"REPEATABLE") {
        return Err((
            "0A000",
            "TABLESAMPLE … REPEATABLE is not supported: the sample is drawn anew on every run".to_string(),
        ));
    }
    let name = alias.map(str::to_string).unwrap_or_else(|| relation.rsplit('.').next().unwrap_or(relation).to_string());
    Ok((start, close + 1, format!("(SELECT * FROM {relation} WHERE random() * 100 < ({arg})) AS {name}")))
}

/// Where the operand ending at `end` begins: a column, a constant, a call,
/// a parenthesised expression, a cast, and arithmetic over them, back to the
/// keyword, comma, comparison or unmatched `(` before it. `None` when there
/// is no operand.
fn operand_start(sql: &str, end: usize) -> Option<usize> {
    const STOP_WORDS: &[&str] = &[
        "AND", "OR", "NOT", "WHERE", "ON", "WHEN", "THEN", "ELSE", "HAVING", "SELECT", "CASE", "IS", "IN", "LIKE",
        "ILIKE", "BETWEEN", "RETURNING", "SET", "BY", "AS", "DISTINCT", "END", "ALL", "ANY", "SOME",
    ];
    let bytes = sql.as_bytes();
    let mut i = end;
    let mut start = None;
    loop {
        while i > 0 && bytes[i - 1].is_ascii_whitespace() {
            i -= 1;
        }
        if i == 0 {
            break;
        }
        match bytes[i - 1] {
            b')' => {
                // Back to the matching `(`, outside quotes.
                let mut depth = 0usize;
                let mut k = i;
                let mut quote: Option<u8> = None;
                loop {
                    if k == 0 {
                        return None;
                    }
                    k -= 1;
                    let c = bytes[k];
                    match quote {
                        Some(q) if c == q => quote = None,
                        Some(_) => {}
                        None => match c {
                            b'\'' | b'"' => quote = Some(c),
                            b')' => depth += 1,
                            b'(' => {
                                depth -= 1;
                                if depth == 0 {
                                    break;
                                }
                            }
                            _ => {}
                        },
                    }
                }
                i = k;
                start = Some(i);
            }
            q @ (b'\'' | b'"') => {
                let open = sql[..i - 1].rfind(q as char)?;
                i = open;
                start = Some(i);
            }
            b'+' | b'-' | b'*' | b'/' | b'%' | b'^' | b'|' | b':' | b'.' | b'[' | b']' => {
                i -= 1;
            }
            c if is_identifier_byte(c) => {
                let mut k = i;
                while k > 0 && is_identifier_byte(bytes[k - 1]) {
                    k -= 1;
                }
                let word = &sql[k..i];
                if STOP_WORDS.iter().any(|w| word.eq_ignore_ascii_case(w)) {
                    break;
                }
                i = k;
                start = Some(i);
            }
            _ => break,
        }
    }
    start
}

/// Splits `list` on `delimiter` where it stands at nesting depth zero —
/// outside every quoted run, comment, `(…)` and `[…]` — returning each piece
/// trimmed and borrowed from `list`. A trailing empty piece is dropped
/// (`1, ` is one piece) while an empty one between delimiters is kept.
///
/// This is the shape every list splitter needs and most got wrong: a
/// splitter that tracks only quotes cuts `f(1, 2)`, `ARRAY[1,2]` and
/// `numeric(10,2)` into pieces, and when the piece count happens to match
/// what was expected the pieces are used as values.
pub fn split_top_level(list: &str, delimiter: u8) -> Vec<&str> {
    let bytes = list.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end;
            continue;
        }
        match bytes[i] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth = depth.saturating_sub(1),
            c if c == delimiter && depth == 0 => {
                out.push(list[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    let last = list[start..].trim();
    if !last.is_empty() {
        out.push(last);
    }
    out
}

/// `doc #>> ARRAY['a', '0']` and `doc #> ARRAY[…]` as the path literal
/// `doc #>> '{a,0}'`, which every path reader here takes. A path written as an
/// `ARRAY[…]` constructor failed in each of them: "unexpected character '['"
/// in a `WHERE`, "Column '… #>> ARRAY[…]' not found" in a select list, and
/// "function '' is not supported" once a driver's parameters were in it.
///
/// Only a constructor whose every element is a constant is rewritten: a
/// string or an integer, optionally cast (`'a'::text`, `CAST('a' AS text)`,
/// as SQL `EXECUTE` and parameter inlining write them). A path element that
/// is a column or an expression, or `NULL`, is left as written. Not a `->`
/// chain: in a path, `'0'` indexes into an array, where `-> '0'` would not.
pub fn json_path_arrays(sql: &str) -> Cow<'_, str> {
    if !sql.contains('#') || !contains_ignore_ascii_case(sql, b"ARRAY") {
        return Cow::Borrowed(sql);
    }
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut copied = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' || b == b'"' {
            i = quoted_end(bytes, i, b).max(i + 1);
            continue;
        }
        if b == b'$' && !(i > 0 && is_identifier_byte(bytes[i - 1])) {
            if let Some(end) = dollar_quote_end(bytes, i) {
                i = end.max(i + 1);
                continue;
            }
        }
        if b == b'#' && bytes.get(i + 1) == Some(&b'>') {
            let operator_end = if bytes.get(i + 2) == Some(&b'>') { i + 3 } else { i + 2 };
            let mut at = operator_end;
            while bytes.get(at).is_some_and(|c| c.is_ascii_whitespace()) {
                at += 1;
            }
            let keyword = at + 5;
            if sql.get(at..keyword).is_some_and(|w| w.eq_ignore_ascii_case("ARRAY")) {
                let mut open = keyword;
                while bytes.get(open).is_some_and(|c| c.is_ascii_whitespace()) {
                    open += 1;
                }
                if bytes.get(open) == Some(&b'[')
                    && let Some(close) = matching_close(sql, open)
                    && let Some(path) = constant_path(&sql[open + 1..close])
                {
                    out.push_str(&sql[copied..operator_end]);
                    out.push(' ');
                    out.push_str(&path);
                    copied = close + 1;
                    i = close + 1;
                    continue;
                }
            }
            i = operator_end;
            continue;
        }
        i += 1;
    }
    if copied == 0 {
        return Cow::Borrowed(sql);
    }
    out.push_str(&sql[copied..]);
    Cow::Owned(out)
}

/// The path literal (`'{a,"b c",0}'`) for an `ARRAY[…]`'s elements, or `None`
/// when one is not a constant.
fn constant_path(elements: &str) -> Option<String> {
    let mut keys = Vec::new();
    for element in split_top_level(elements, b',') {
        keys.push(constant_key(element)?);
    }
    if keys.is_empty() {
        return None;
    }
    let quoted: Vec<String> = keys
        .iter()
        .map(|k| {
            let plain = !k.is_empty()
                && !k.eq_ignore_ascii_case("NULL")
                && !k.bytes().any(|c| c.is_ascii_whitespace() || matches!(c, b',' | b'{' | b'}' | b'"' | b'\\'));
            if plain { k.clone() } else { format!("\"{}\"", k.replace('\\', "\\\\").replace('"', "\\\"")) }
        })
        .collect();
    Some(format!("'{{{}}}'", quoted.join(",").replace('\'', "''")))
}

/// One path element's value: a string or integer constant, without the
/// casts a driver or SQL `EXECUTE` wraps it in.
fn constant_key(element: &str) -> Option<String> {
    let mut e = element.trim();
    loop {
        // `( … )` around the whole element.
        if e.starts_with('(') && matching_close(e, 0) == Some(e.len() - 1) {
            e = e[1..e.len() - 1].trim();
            continue;
        }
        // `CAST(x AS type)`.
        if e.len() > 5 && e[..4].eq_ignore_ascii_case("CAST") && e[4..].trim_start().starts_with('(') {
            let open = e.len() - e[4..].trim_start().len();
            if matching_close(e, open) == Some(e.len() - 1) {
                let inner = &e[open + 1..e.len() - 1];
                let upper = inner.to_ascii_uppercase();
                let mut cut = None;
                let ib = inner.as_bytes();
                let mut k = 0usize;
                while k < ib.len() {
                    if ib[k] == b'\'' || ib[k] == b'"' {
                        k = quoted_end(ib, k, ib[k]).max(k + 1);
                        continue;
                    }
                    if upper[k..].starts_with(" AS ") {
                        cut = Some(k);
                    }
                    k += 1;
                }
                e = inner[..cut?].trim();
                continue;
            }
        }
        // `x::type`, the last cast outside quotes.
        let eb = e.as_bytes();
        let mut k = 0usize;
        let mut cast = None;
        while k + 1 < eb.len() {
            if eb[k] == b'\'' || eb[k] == b'"' {
                k = quoted_end(eb, k, eb[k]).max(k + 1);
                continue;
            }
            if eb[k] == b':' && eb[k + 1] == b':' {
                cast = Some(k);
                k += 2;
                continue;
            }
            k += 1;
        }
        if let Some(at) = cast {
            let ty = e[at + 2..].trim();
            if ty.is_empty() || !ty.bytes().all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b' ' | b'(' | b')')) {
                return None;
            }
            e = e[..at].trim();
            continue;
        }
        break;
    }
    if e.len() >= 2 && e.starts_with('\'') && quoted_end(e.as_bytes(), 0, b'\'') == e.len() {
        return Some(e[1..e.len() - 1].replace("''", "'"));
    }
    let digits = e.strip_prefix('-').or_else(|| e.strip_prefix('+')).unwrap_or(e);
    if !digits.is_empty() && digits.bytes().all(|c| c.is_ascii_digit()) {
        return Some(e.trim_start_matches('+').to_string());
    }
    None
}

/// Index of the bracket closing the one at `open` (`(` or `[`), skipping
/// quoted runs and comments; `None` when it never closes.
pub fn matching_close(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    while i < bytes.len() {
        // Only these bytes can open a quoted run or a comment or change the
        // depth; every other one is stepped over without the two probes,
        // which ran at each byte of every statement's several scans.
        if !matches!(bytes[i], b'(' | b'[' | b')' | b']' | b'\'' | b'"' | b'$' | b'-' | b'/') {
            i += 1;
            continue;
        }
        if i > open {
            if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
                i = end;
                continue;
            }
        }
        match bytes[i] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => {
                depth = depth.checked_sub(1)?;
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

/// [`fold_unquoted_identifiers`] for a statement whose names are the
/// engine's own (a query, DML, table and index DDL, VACUUM, LOCK, TRUNCATE);
/// borrowed and untouched for any other, whose words are its own (a KV key,
/// a setting, a backup's or a database's name).
///
/// Applied where a client's statement enters, the wire layer's execute and
/// describe, before the rewrites that read the text first (CTEs, derived
/// tables, joins), so they see the names the parsers do. Never to the SQL
/// the engine writes itself: that splices in names as stored (a JSON
/// shadow's `Date` beside `date`, a CTE column from a quoted `"ID"`, a table
/// imported from PostgreSQL), and folding one would name another column.
pub fn fold_core_statement(sql: &str) -> Cow<'_, str> {
    let mut words = sql.trim_start().split(|c: char| c.is_ascii_whitespace() || c == '(').filter(|w| !w.is_empty());
    let first = words.next().unwrap_or("");
    let is = |w: &str, k: &str| w.eq_ignore_ascii_case(k);
    let core = sql.trim_start().starts_with('(')
        || ["SELECT", "INSERT", "UPDATE", "DELETE", "WITH", "VALUES", "VACUUM", "LOCK", "TRUNCATE"]
            .iter()
            .any(|k| is(first, k))
        || match (first, words.next().unwrap_or(""), words.next().unwrap_or("")) {
            (c, t, _) if is(c, "CREATE") && (is(t, "TABLE") || is(t, "INDEX")) => true,
            (c, u, i) if is(c, "CREATE") && is(u, "UNIQUE") && is(i, "INDEX") => true,
            (a, t, _) if is(a, "ALTER") && is(t, "TABLE") => true,
            (d, t, _) if is(d, "DROP") && (is(t, "TABLE") || is(t, "INDEX")) => true,
            _ => false,
        };
    if core { fold_unquoted_identifiers(sql) } else { Cow::Borrowed(sql) }
}

/// PostgreSQL's reserved keywords (and the operator words among its
/// type-or-function ones): words that can never be an unquoted identifier,
/// so [`fold_unquoted_identifiers`] leaves them as written. Folding them
/// would change no name; leaving them keeps the declarations stored as text
/// (a CHECK, a partial index's predicate, a generated column) reading as
/// written, `deleted_at IS NULL` rather than `deleted_at is null`.
const RESERVED_WORDS: &[&str] = &[
    "ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "ASYMMETRIC", "BETWEEN", "BOTH", "CASE",
    "CAST", "CHECK", "COLLATE", "COLUMN", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_CATALOG", "CURRENT_DATE",
    "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DEFAULT", "DEFERRABLE", "DESC",
    "DISTINCT", "DO", "ELSE", "END", "EXCEPT", "EXISTS", "FALSE", "FETCH", "FOR", "FOREIGN", "FROM", "FULL",
    "GRANT", "GROUP", "HAVING", "ILIKE", "IN", "INITIALLY", "INNER", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN",
    "LATERAL", "LEADING", "LIKE", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "NATURAL", "NOT", "NOTNULL", "NULL",
    "OFFSET", "ON", "ONLY", "OR", "ORDER", "OUTER", "OVERLAPS", "PLACING", "PRIMARY", "REFERENCES", "RETURNING",
    "SELECT", "SESSION_USER", "SIMILAR", "SOME", "SYMMETRIC", "SYSTEM_USER", "TABLE", "THEN", "TO", "TRAILING",
    "TRUE", "UNION", "UNIQUE", "USER", "USING", "VARIADIC", "WHEN", "WHERE", "WINDOW", "WITH",
];

/// `sql` with every unquoted identifier folded to lowercase, as PostgreSQL's
/// lexer folds it: `CREATE TABLE Acct (Name text)` declares `acct` and
/// `name`, and `SELECT ID AS Ident` is labelled `ident`. Quoted identifiers
/// (`"Name"`), strings of every kind (`'Name'`, `E'…'`, `$$…$$`), comments,
/// a word written directly against a string (the prefix of `X'FF'`, `E'…'`,
/// `U&'…'`) and the [`RESERVED_WORDS`] keep their case. Only ASCII letters
/// change, so every byte keeps its offset and no two tokens can run
/// together. Borrowed when there is nothing to fold.
pub fn fold_unquoted_identifiers(sql: &str) -> Cow<'_, str> {
    let bytes = sql.as_bytes();
    let mut out: Option<Vec<u8>> = None;
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        match bytes[i] {
            // A word: an identifier or a keyword. PostgreSQL lets one hold
            // non-ASCII letters, and folds only the ASCII ones.
            b if b.is_ascii_alphabetic() || b == b'_' || b >= 0x80 => {
                let start = i;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'$' || bytes[i] >= 0x80)
                {
                    i += 1;
                }
                let word = &bytes[start..i];
                // A string prefix (`X'…'`, `E'…'`, `U&'…'`) is part of the
                // literal, not a name.
                let prefix = bytes.get(i) == Some(&b'\'')
                    || (bytes.get(i) == Some(&b'&') && matches!(bytes.get(i + 1), Some(b'\'' | b'"')));
                if !prefix
                    && word.iter().any(u8::is_ascii_uppercase)
                    && !RESERVED_WORDS.iter().any(|r| r.as_bytes().eq_ignore_ascii_case(word))
                {
                    let buf = out.get_or_insert_with(|| bytes.to_vec());
                    buf[start..i].make_ascii_lowercase();
                }
            }
            // A number: its letters (`1E5`, `0x1F`) are never an identifier.
            b if b.is_ascii_digit() => {
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'.') {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }
    match out {
        // Only ASCII letters changed, so the bytes are still UTF-8.
        Some(buf) => Cow::Owned(String::from_utf8(buf).expect("ASCII case folding keeps UTF-8")),
        None => Cow::Borrowed(sql),
    }
}

/// Rewrites every escape string (`E'…'`) and dollar-quoted string
/// (`$tag$…$tag$`) into the standard string it means, leaving everything else
/// — standard strings, quoted identifiers, comments, `$1` placeholders — byte
/// for byte as it was.
///
/// Borrowed when there is nothing to rewrite, which is nearly every statement.
/// An escape that does not decode — a byte sequence that is not UTF-8, a NUL,
/// a `\u` that names no character — is an error, as it is in PostgreSQL.
pub fn normalize_string_literals(sql: &str) -> Result<Cow<'_, str>, String> {
    let bytes = sql.as_bytes();
    if !needs_normalizing(bytes) {
        return Ok(zone_free_timestamp_literals(sql));
    }
    escapes_resolved(sql).map(|out| match zone_free_timestamp_literals(&out) {
        Cow::Borrowed(_) => Cow::Owned(out),
        Cow::Owned(rewritten) => Cow::Owned(rewritten),
    })
}

/// [`normalize_string_literals`]'s escape and dollar-quote rewrite.
fn escapes_resolved(sql: &str) -> Result<String, String> {
    let bytes = sql.as_bytes();

    let mut out = String::with_capacity(sql.len() + 8);
    let mut copied = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        // Jump to the next byte that can open anything.
        match memchr::memchr3(b'\'', b'$', b'"', &bytes[i..]) {
            Some(p) => {
                // A comment can hide a quote; find whether one opens first.
                let next = i + p;
                if let Some(c) = first_comment_start(bytes, i, next) {
                    i = skip_comment(bytes, c).expect("a comment opens here");
                    continue;
                }
                i = next;
            }
            None => break,
        }
        match bytes[i] {
            b'\'' if is_escape_string_quote(bytes, i) => {
                let Some(end) = terminated_escape_literal_end(bytes, i) else {
                    // Unterminated: left for the parser to report.
                    i = bytes.len();
                    continue;
                };
                let body = &bytes[i + 1..end - 1];
                let decoded = decode_escape_body(body)?;
                // Drop the `E` already copied with the run before the quote.
                out.push_str(&sql[copied..i - 1]);
                push_standard_literal(&mut out, &decoded);
                i = end;
                copied = end;
            }
            b'\'' => i = string_literal_end(bytes, i, false),
            b'"' => i = quoted_end(bytes, i, b'"'),
            b'$' => {
                let opens = i == 0 || !is_identifier_byte(bytes[i - 1]);
                match opens.then(|| dollar_tag_len(bytes, i)).flatten() {
                    Some(tag_len) => {
                        let tag = &bytes[i..i + tag_len];
                        let body_start = i + tag_len;
                        match memchr::memmem::find(&bytes[body_start..], tag) {
                            Some(p) => {
                                let body = &sql[body_start..body_start + p];
                                out.push_str(&sql[copied..i]);
                                push_standard_literal(&mut out, body);
                                i = body_start + p + tag_len;
                                copied = i;
                            }
                            // Unterminated: left as written.
                            None => i = bytes.len(),
                        }
                    }
                    None => i += 1,
                }
            }
            _ => i += 1,
        }
    }
    out.push_str(&sql[copied..]);
    Ok(out)
}

/// Every string literal the statement types as `timestamp` or `timestamptz`
/// (`'…'::timestamptz`, `TIMESTAMP '…'`, `CAST('…' AS timestamp with time
/// zone)`, a precision allowed) whose text carries a zone, rewritten with no
/// zone left to read two ways and the same meaning: the wall-clock time for
/// `timestamp`, which discards the zone, and the UTC time with `+00` for
/// `timestamptz`, which converts by it (the session time zone is UTC).
///
/// The two types read the same text differently, and the cast or type name
/// that says which does not reach every reader of the literal: the write path
/// drops a cast on a literal, the predicate rewrite drops one before the
/// tokenizer sees it, the typed-literal prefix is stripped before a value is
/// encoded, and the evaluator's casts name one storage type for both. So a
/// row written with `'… 10:00+02'::timestamptz` into a `timestamp` column was
/// stored as 10:00 (PostgreSQL: 08:00) and a predicate spelled the same way
/// looked for something else. With no zone left, every reader agrees.
///
/// Borrowed when the statement has no such literal: the cheap test is that
/// it never mentions `timestamp`.
pub fn zone_free_timestamp_literals(sql: &str) -> Cow<'_, str> {
    let bytes = sql.as_bytes();
    if !contains_ignore_ascii_case(sql, b"timestamp") {
        return Cow::Borrowed(sql);
    }
    let mut out: Option<String> = None;
    let mut copied = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let Some(p) = memchr::memchr3(b'\'', b'"', b'$', &bytes[i..]).map(|p| i + p) else {
            break;
        };
        if let Some(c) = first_comment_start(bytes, i, p) {
            i = skip_comment(bytes, c).expect("a comment opens here");
            continue;
        }
        i = p;
        if bytes[i] != b'\'' || is_escape_string_quote(bytes, i) {
            i = skip_quoted(bytes, i).unwrap_or(i + 1);
            continue;
        }
        let end = string_literal_end(bytes, i, false);
        if end > bytes.len() || end < i + 2 || bytes[end - 1] != b'\'' {
            break;
        }
        let body = &sql[i + 1..end - 1];
        let zoned = (!body.contains('\''))
            .then(|| timestamp_type_of_literal(sql, i, end))
            .flatten()
            .and_then(|with_zone| zone_free(body, with_zone));
        if let Some(text) = zoned {
            let o = out.get_or_insert_with(|| String::with_capacity(sql.len()));
            o.push_str(&sql[copied..i + 1]);
            o.push_str(&text);
            copied = end - 1;
        }
        i = end;
    }
    match out {
        None => Cow::Borrowed(sql),
        Some(mut o) => {
            o.push_str(&sql[copied..]);
            Cow::Owned(o)
        }
    }
}

/// The number inside `text` when `text` is only a number in parentheses,
/// `(0098)` or `((1e1))`: the constant as written, without them.
pub fn parenthesised_number(text: &str) -> Option<&str> {
    let mut inner = text.trim();
    let mut stripped = false;
    while let Some(rest) = inner.strip_prefix('(').and_then(|r| r.strip_suffix(')')) {
        inner = rest.trim();
        stripped = true;
    }
    (stripped && !inner.contains(['(', ')']) && crate::pg_params::is_numeric_literal(inner)).then_some(inner)
}

/// Whether `text` holds a string literal that reads as a timestamp with a
/// zone in it (`'… 09:00+05'`): one that `timestamp` and `timestamptz` read as
/// two different values.
pub fn has_zoned_timestamp_literal(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut i = 0usize;
    while let Some(p) = memchr::memchr(b'\'', &bytes[i..]).map(|p| i + p) {
        let end = string_literal_end(bytes, p, is_escape_string_quote(bytes, p));
        if end >= p + 2 {
            let body = &text[p + 1..end - 1];
            if zone_free(body, true).is_some() {
                return true;
            }
        }
        i = end.max(p + 1);
        if i >= bytes.len() {
            break;
        }
    }
    false
}

/// `body` with its zone resolved away, or `None` when it has none to resolve
/// (or is not a timestamp, or would not read back as the same instant).
fn zone_free(body: &str, with_zone: bool) -> Option<String> {
    use crate::core::value_codec::{format_timestamp_us, parse_timestamp_local_us, parse_timestamp_us};
    let (utc, wall) = (parse_timestamp_us(body)?, parse_timestamp_local_us(body)?);
    if utc == wall {
        return None;
    }
    let us = if with_zone { utc } else { wall };
    // A `timestamptz` keeps an explicit `+00`, which every reader takes as
    // the same instant and which is how PostgreSQL shows it as text: written
    // into a text column, `'… 10:00+02'::timestamptz` is `… 08:00:00+00`.
    let mut text = format_timestamp_us(us);
    if with_zone {
        text.push_str("+00");
    }
    (parse_timestamp_us(&text) == Some(us)).then_some(text)
}

/// Whether the literal at `start..end` is typed `timestamptz` (`Some(true)`)
/// or `timestamp` (`Some(false)`) by a cast after it, a type name before it,
/// or `CAST(… AS …)` around it; `None` when it is not typed either way.
fn timestamp_type_of_literal(sql: &str, start: usize, end: usize) -> Option<bool> {
    let after = sql[end..].trim_start();
    // `('…')::timestamp`, `(('…'))::timestamp`: a literal alone in
    // parentheses (not a call's argument, `f('…')::timestamp`), cast as one.
    let (mut inner_after, mut inner_before) = (after, sql[..start].trim_end());
    while let (Some(rest), Some(open)) = (inner_after.strip_prefix(')'), inner_before.strip_suffix('(')) {
        let open = open.trim_end();
        if open.as_bytes().last().is_some_and(|b| is_identifier_byte(*b)) {
            break;
        }
        (inner_after, inner_before) = (rest.trim_start(), open);
        if let Some(cast) = inner_after.strip_prefix("::") {
            return timestamp_type_at(cast.trim_start()).map(|(tz, _)| tz);
        }
    }
    if let Some(cast) = after.strip_prefix("::") {
        return timestamp_type_at(cast.trim_start()).map(|(tz, _)| tz);
    }
    let before = sql[..start].trim_end();
    if let Some(open) = before.strip_suffix('(') {
        let head = open.trim_end();
        let cast_word = head.len() >= 4 && head.as_bytes()[head.len() - 4..].eq_ignore_ascii_case(b"CAST");
        let word_start = head.len().saturating_sub(4);
        let bounded = word_start == 0 || !is_identifier_byte(head.as_bytes()[word_start - 1]);
        if cast_word && bounded {
            let rest = after;
            let as_word = rest.get(..2).is_some_and(|w| w.eq_ignore_ascii_case("AS"))
                && rest.as_bytes().get(2).is_some_and(|b| !is_identifier_byte(*b));
            if as_word {
                return timestamp_type_at(rest[2..].trim_start()).map(|(tz, _)| tz);
            }
        }
        return None;
    }
    timestamp_type_before(before)
}

/// A timestamp type name at the start of `text`: whether it carries a zone,
/// and its length. `timestamptz`, `timestamp`, either with a `(p)`, and
/// `timestamp[(p)] with[out] time zone`.
fn timestamp_type_at(text: &str) -> Option<(bool, usize)> {
    let word = |t: &str, w: &str| {
        t.get(..w.len()).is_some_and(|h| h.eq_ignore_ascii_case(w))
            && t.as_bytes().get(w.len()).is_none_or(|b| !is_identifier_byte(*b))
    };
    let precision = |t: &str| -> usize {
        let rest = t.trim_start();
        let lead = t.len() - rest.len();
        match rest.strip_prefix('(') {
            Some(inner) => match inner.find(')') {
                Some(close) if inner[..close].trim().bytes().all(|b| b.is_ascii_digit()) => lead + 1 + close + 1,
                _ => 0,
            },
            None => 0,
        }
    };
    if word(text, "timestamptz") {
        let n = "timestamptz".len();
        return Some((true, n + precision(&text[n..])));
    }
    if !word(text, "timestamp") {
        return None;
    }
    let mut n = "timestamp".len();
    n += precision(&text[n..]);
    let rest = &text[n..];
    let words: Vec<&str> = rest.split_whitespace().take(3).collect();
    let zone = |kind: &str| {
        words.len() == 3
            && words[0].eq_ignore_ascii_case(kind)
            && words[1].eq_ignore_ascii_case("time")
            && words[2].len() >= 4
            && words[2].as_bytes()[..4].eq_ignore_ascii_case(b"zone")
            && words[2].as_bytes().get(4).is_none_or(|b| !is_identifier_byte(*b))
    };
    if zone("with") {
        return Some((true, n));
    }
    Some((false, n))
}

/// A timestamp type name ending `before` (whitespace-trimmed), the type of a
/// typed literal `TIMESTAMP '…'`: whether it carries a zone.
fn timestamp_type_before(before: &str) -> Option<bool> {
    // The last few words, read backwards: identifier runs, with a `(p)`
    // precision folded into the word before it.
    let b = before.as_bytes();
    let mut words: Vec<String> = Vec::new();
    let mut k = b.len();
    let mut first_at = k;
    while words.len() < 4 && k > 0 {
        while k > 0 && b[k - 1].is_ascii_whitespace() {
            k -= 1;
        }
        let mut stop = k;
        if k > 0 && b[k - 1] == b')' {
            let mut j = k - 1;
            while j > 0 && (b[j - 1].is_ascii_digit() || b[j - 1] == b' ') {
                j -= 1;
            }
            if j == 0 || b[j - 1] != b'(' {
                break;
            }
            stop = j - 1;
            while stop > 0 && b[stop - 1].is_ascii_whitespace() {
                stop -= 1;
            }
        }
        let mut j = stop;
        while j > 0 && is_identifier_byte(b[j - 1]) {
            j -= 1;
        }
        if j == stop {
            break;
        }
        words.push(before[j..stop].to_ascii_lowercase());
        first_at = j;
        k = j;
    }
    words.reverse();
    // The type must be a word of its own: not a qualified name (`t.timestamp`),
    // a quoted identifier, or the tail of a cast (`x::timestamp '…'`).
    let lead = |at: usize| at == 0 || !matches!(b[at - 1], b'.' | b'"' | b':');
    let ty = |w: &str| match w {
        "timestamptz" => Some(true),
        "timestamp" => Some(false),
        _ => None,
    };
    let n = words.len();
    if n >= 4 && words[n - 2] == "time" && words[n - 1] == "zone" && ty(&words[n - 4]) == Some(false) {
        // `first_at` is where the earliest word read starts; with four words
        // read that is the type's.
        if n == 4 && !lead(first_at) {
            return None;
        }
        return match words[n - 3].as_str() {
            "with" => Some(true),
            "without" => Some(false),
            _ => None,
        };
    }
    let last = words.last()?;
    // The last word's start: the text before it must let it stand alone.
    let last_start = {
        let trimmed = before.trim_end();
        let tb = trimmed.as_bytes();
        let mut j = tb.len();
        if j > 0 && tb[j - 1] == b')' {
            while j > 0 && tb[j - 1] != b'(' {
                j -= 1;
            }
            j = j.saturating_sub(1);
            while j > 0 && tb[j - 1].is_ascii_whitespace() {
                j -= 1;
            }
        }
        while j > 0 && is_identifier_byte(tb[j - 1]) {
            j -= 1;
        }
        j
    };
    if !lead(last_start) {
        return None;
    }
    ty(last)
}

/// [`string_literal_end`] for an escape string, `None` when it never closes.
fn terminated_escape_literal_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start + 1;
    loop {
        i += memchr::memchr2(b'\'', b'\\', &bytes[i..])?;
        if bytes[i] == b'\\' {
            i += 2;
            if i > bytes.len() {
                return None;
            }
            continue;
        }
        if bytes.get(i + 1) == Some(&b'\'') {
            i += 2;
            continue;
        }
        return Some(i + 1);
    }
}

/// Cheap pre-check: is there any `E'` or `$tag$` at all? A false positive only
/// costs the full pass.
fn needs_normalizing(bytes: &[u8]) -> bool {
    for p in memchr::memchr2_iter(b'\'', b'$', bytes) {
        match bytes[p] {
            b'\'' if is_escape_string_quote(bytes, p) => return true,
            b'$' if (p == 0 || !is_identifier_byte(bytes[p - 1]))
                && dollar_tag_len(bytes, p).is_some() =>
            {
                return true;
            }
            _ => {}
        }
    }
    false
}

/// The first `--` or `/*` in `bytes[from..to]`, if any.
fn first_comment_start(bytes: &[u8], from: usize, to: usize) -> Option<usize> {
    let window = &bytes[from..to];
    let mut at = 0usize;
    while let Some(p) = memchr::memchr2(b'-', b'/', &window[at..]) {
        let k = from + at + p;
        if skip_comment(bytes, k).is_some() {
            return Some(k);
        }
        at += p + 1;
    }
    None
}

/// `'` + `body` with every quote doubled + `'`.
fn push_standard_literal(out: &mut String, body: &str) {
    out.reserve(body.len() + 2);
    out.push('\'');
    let mut start = 0usize;
    for at in memchr::memchr_iter(b'\'', body.as_bytes()) {
        out.push_str(&body[start..=at]);
        out.push('\'');
        start = at + 1;
    }
    out.push_str(&body[start..]);
    out.push('\'');
}

/// Decodes the body of an escape string the way PostgreSQL does: `\b \f \n \r
/// \t`, octal `\o`–`\ooo`, hex `\xh`–`\xhh`, `\uXXXX` and `\UXXXXXXXX`
/// (surrogate pairs combined), `''` as one quote, and any other escaped
/// character as itself.
fn decode_escape_body(body: &[u8]) -> Result<String, String> {
    let mut out: Vec<u8> = Vec::with_capacity(body.len());
    let mut i = 0usize;
    let hex = |b: u8| (b as char).to_digit(16);
    while i < body.len() {
        let b = body[i];
        if b == b'\'' {
            // Only a doubled quote can sit inside the body.
            out.push(b'\'');
            i += 2;
            continue;
        }
        if b != b'\\' {
            out.push(b);
            i += 1;
            continue;
        }
        let Some(&e) = body.get(i + 1) else {
            out.push(b'\\');
            break;
        };
        i += 2;
        match e {
            b'b' => out.push(0x08),
            b'f' => out.push(0x0c),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'0'..=b'7' => {
                let mut v = u32::from(e - b'0');
                let mut n = 1;
                while n < 3 && i < body.len() && (b'0'..=b'7').contains(&body[i]) {
                    v = v * 8 + u32::from(body[i] - b'0');
                    i += 1;
                    n += 1;
                }
                out.push((v & 0xff) as u8);
            }
            b'x' if body.get(i).and_then(|b| hex(*b)).is_some() => {
                let mut v = hex(body[i]).unwrap();
                i += 1;
                if let Some(d) = body.get(i).and_then(|b| hex(*b)) {
                    v = v * 16 + d;
                    i += 1;
                }
                out.push(v as u8);
            }
            b'u' | b'U' => {
                let width = if e == b'u' { 4 } else { 8 };
                let mut code = read_hex(body, i, width)
                    .ok_or_else(|| "invalid Unicode escape: \\u needs 4 hex digits, \\U 8".to_string())?;
                i += width;
                if (0xD800..0xDC00).contains(&code) {
                    // A high surrogate must be followed by `\uDC00`–`\uDFFF`.
                    let low = (body.get(i) == Some(&b'\\') && body.get(i + 1) == Some(&b'u'))
                        .then(|| read_hex(body, i + 2, 4))
                        .flatten()
                        .filter(|l| (0xDC00..0xE000).contains(l))
                        .ok_or_else(|| "invalid Unicode surrogate pair".to_string())?;
                    i += 6;
                    code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                }
                let c = char::from_u32(code)
                    .ok_or_else(|| format!("invalid Unicode escape value {:#x}", code))?;
                let mut buf = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
            other => out.push(other),
        }
    }
    if out.contains(&0) {
        return Err("invalid byte sequence for encoding \"UTF8\": 0x00".to_string());
    }
    String::from_utf8(out).map_err(|e| {
        format!(
            "invalid byte sequence for encoding \"UTF8\" in an escape string at byte {}",
            e.utf8_error().valid_up_to()
        )
    })
}

fn read_hex(body: &[u8], at: usize, width: usize) -> Option<u32> {
    let digits = body.get(at..at + width)?;
    let mut v = 0u32;
    for d in digits {
        v = v * 16 + (*d as char).to_digit(16)?;
    }
    Some(v)
}

#[cfg(test)]
mod tests {

    #[test]
    fn starts_with_operator_is_the_function() {
        assert_eq!(
            text_operators("SELECT 'alphabet' ^@ 'alph', name ^@ 'a' FROM t WHERE t.name::text ^@ lower('A')").as_deref(),
            Some("SELECT starts_with('alphabet', 'alph') AS \"?column?\", starts_with(name, 'a') AS \"?column?\" FROM t WHERE starts_with(t.name::text, lower('A'))")
        );
        assert_eq!(text_operators("SELECT '^@', \"a^@b\" FROM t"), None);
    }

    #[test]
    fn a_catalog_qualified_oid_cast_is_bigint() {
        assert_eq!(
            catalog_syntax("SELECT n FROM (VALUES ('id', '23'::pg_catalog.oid, -1)) s(n, tp, tpm)"),
            "SELECT n FROM (VALUES ('id', '23'::bigint, -1)) s(n, tp, tpm)"
        );
        assert_eq!(catalog_syntax("SELECT x::pg_catalog.oidvector"), "SELECT x::oidvector");
    }

    #[test]
    fn tablesample_is_a_sampled_derived_table() {
        assert_eq!(
            tablesample("SELECT count(*) FROM t TABLESAMPLE BERNOULLI (10)").unwrap().as_deref(),
            Some("SELECT count(*) FROM (SELECT * FROM t WHERE random() * 100 < (10)) AS t")
        );
        assert_eq!(
            tablesample("SELECT x.id FROM public.t AS x TABLESAMPLE system(50) JOIN u y TABLESAMPLE SYSTEM (5) ON x.id = y.id").unwrap().as_deref(),
            Some(
                "SELECT x.id FROM (SELECT * FROM public.t WHERE random() * 100 < (50)) AS x JOIN (SELECT * FROM u WHERE random() * 100 < (5)) AS y ON x.id = y.id"
            )
        );
        assert_eq!(tablesample("SELECT 'TABLESAMPLE' FROM t").unwrap(), None);
        assert_eq!(tablesample("SELECT * FROM t TABLESAMPLE foo (1)").unwrap_err().0, "42704");
        assert_eq!(tablesample("SELECT * FROM t TABLESAMPLE BERNOULLI (101)").unwrap_err().0, "2202H");
        assert_eq!(tablesample("SELECT * FROM t TABLESAMPLE BERNOULLI (1) REPEATABLE (7)").unwrap_err().0, "0A000");
    }

    #[test]
    fn similar_to_is_lowered_only_in_a_select_list() {
        let s = |sql: &str| statement_shorthands(sql).into_owned();
        assert_eq!(s("SELECT 'abc' SIMILAR TO 'a%'"), "SELECT 'abc'  ~ similar_to_escape('a%') ");
        assert_eq!(
            s("SELECT coalesce(v SIMILAR TO 'x', false) FROM t WHERE k SIMILAR TO 'a  '"),
            "SELECT coalesce(v  ~ similar_to_escape('x') , false) FROM t WHERE k SIMILAR TO 'a  '"
        );
        assert_eq!(s("SELECT 1 FROM t JOIN u ON u.a SIMILAR TO t.b"), "SELECT 1 FROM t JOIN u ON u.a SIMILAR TO t.b");
        assert_eq!(
            s("SELECT (SELECT 1 FROM u WHERE u.a SIMILAR TO 'q') FROM t"),
            "SELECT (SELECT 1 FROM u WHERE u.a SIMILAR TO 'q') FROM t"
        );
        assert!(in_select_list("SELECT CASE WHEN a SIMILAR TO 'x' THEN 1 END FROM t", 21));
    }

    #[test]
    fn overlaps_is_postgresql_s_case() {
        let out = text_operators("SELECT (DATE '2001-02-16', DATE '2001-12-21') OVERLAPS (DATE '2001-10-30', DATE '2002-10-30') AS o").unwrap();
        assert!(out.starts_with("SELECT (CASE WHEN (CASE WHEN (DATE '2001-02-16') > (DATE '2001-12-21')"), "{out}");
        assert!(out.ends_with(" END) AS o"), "{out}");
        assert!(!out.contains("OVERLAPS"), "{out}");
        let interval = text_operators("SELECT 1 WHERE (s, INTERVAL '1 day') OVERLAPS (x, y)").unwrap();
        assert!(interval.contains("((s) + (INTERVAL '1 day'))"), "{interval}");
        assert_eq!(text_operators("SELECT overlaps FROM t"), None);
    }

    #[test]
    fn the_last_name_segment_ignores_dots_inside_quotes() {
        use super::last_name_segment as l;
        assert_eq!(l("t"), "t");
        assert_eq!(l("public.t"), "t");
        assert_eq!(l("\"public\".\"Q1\""), "\"Q1\"");
        assert_eq!(l("app.\"a.b\""), "\"a.b\"");
        assert_eq!(l("\"a.b\""), "\"a.b\"");
        assert_eq!(l("\"x\"\".y\".z"), "z");
        assert_eq!(l("db.public.t"), "t");
        assert_eq!(l("t."), "t.");
    }

    #[test]
    fn a_quantified_subquery_is_an_in_list() {
        use super::quantified_subqueries as q;
        assert_eq!(q("SELECT id FROM a WHERE id = ANY (SELECT id FROM b)").unwrap().as_deref(),
                   Some("SELECT id FROM a WHERE id IN (SELECT id FROM b)"));
        assert_eq!(q("SELECT 1 FROM a WHERE x=some(select y from b) AND z <> ALL (WITH w AS (SELECT 1) SELECT * FROM w)").unwrap().as_deref(),
                   Some("SELECT 1 FROM a WHERE x IN (select y from b) AND z NOT IN (WITH w AS (SELECT 1) SELECT * FROM w)"));
        assert!(q("SELECT id FROM a WHERE id = ANY (ARRAY[1, 2])").unwrap().is_none());
        assert!(q("SELECT id FROM a WHERE id = ANY ('{1,2}')").unwrap().is_none());
        assert!(q("SELECT 'x = ANY (SELECT 1)' FROM a").unwrap().is_none());
        assert!(q("SELECT company FROM a").unwrap().is_none());
        // Any other operator is answered by the executor, with the values.
        for left in ["id > ANY (SELECT 1)", "id = ALL (SELECT 1)", "id <> ANY (SELECT 1)", "id <= SOME (SELECT 1)"] {
            assert!(q(&format!("SELECT 1 FROM a WHERE {left}")).unwrap().is_none(), "{left}");
        }
    }

    #[test]
    fn a_quantified_comparison_is_found_with_its_operand() {
        use super::find_quantified_subquery as f;
        let sql = "SELECT id FROM a WHERE x = 1 AND n + 1 >= ALL (SELECT m FROM b WHERE (m) > 0) ORDER BY id";
        let q = f(sql).unwrap().unwrap();
        assert_eq!((q.operand.as_str(), q.op, q.all), ("n + 1", ">=", true));
        assert_eq!(&sql[q.start..q.end], "n + 1 >= ALL (SELECT m FROM b WHERE (m) > 0)");
        assert_eq!(q.inner_sql, "SELECT m FROM b WHERE (m) > 0");
        for (sql, operand, op) in [
            ("SELECT id, lower(a.g)::text<SOME(SELECT g FROM b) FROM a", "lower(a.g)::text", "<"),
            ("SELECT 1 FROM a WHERE NOT \"N\" != ANY (SELECT 1)", "\"N\"", "<>"),
            ("SELECT 1 FROM a WHERE (n) > ANY (SELECT 1)", "(n)", ">"),
            ("UPDATE a SET g = 'x' WHERE n > ALL (SELECT 1)", "n", ">"),
            ("SELECT 1 FROM a WHERE 'it''s' < ANY (SELECT g FROM b)", "'it''s'", "<"),
        ] {
            let q = f(sql).unwrap().unwrap();
            assert_eq!((q.operand.as_str(), q.op), (operand, op), "{sql}");
        }
        for none in ["SELECT 1 FROM a WHERE n = ANY (ARRAY[1])", "SELECT 'n > ANY (SELECT 1)' FROM a", "SELECT 1 UNION ALL (SELECT 2)"] {
            assert!(f(none).is_none(), "{none}");
        }
    }


    #[test]
    fn typed_literals_become_casts() {
        use super::typed_literals_as_casts as t;
        assert_eq!(
            t("INSERT INTO x VALUES (DATE '2024-01-02', numeric '1.50', TIMESTAMP WITH TIME ZONE '2024-01-02 03:04:05+02')").as_deref(),
            Some("INSERT INTO x VALUES ('2024-01-02'::date, '1.50'::numeric, '2024-01-02 03:04:05+02'::timestamptz)")
        );
        assert_eq!(
            t("SELECT 1 WHERE d = DATE  'it''s' AND f = DOUBLE PRECISION '1.5'").as_deref(),
            Some("SELECT 1 WHERE d = 'it''s'::date AND f = '1.5'::float8")
        );
        // Left alone: no typed literal, a type word in a string, a column
        // named like a type, AT TIME ZONE, an interval field qualifier, an
        // escape string, a cast already, a comment, a quoted identifier.
        for sql in [
            "SELECT a FROM t",
            "SELECT 'DATE ''x''' FROM t",
            "SELECT date, text FROM t WHERE date > '2024-01-01'",
            "SELECT ts AT TIME ZONE 'UTC' FROM t",
            "SET TIME ZONE 'UTC'",
            "SELECT INTERVAL '1' DAY",
            "SELECT DATE E'2024-01-02'",
            "SELECT '1'::int, x::date",
            "SELECT 1 -- DATE '2024'\n",
            "SELECT \"date\" FROM t WHERE x = 'y'",
            "SELECT t.date FROM t WHERE x = 'y'",
        ] {
            assert_eq!(t(sql), None, "{sql}");
        }
    }
    /// Each spelling PostgreSQL types a literal with, rewritten to the value
    /// that type reads; everything else left byte for byte.
    #[test]
    fn a_typed_timestamp_literal_loses_its_zone_and_keeps_its_meaning() {
        use super::zone_free_timestamp_literals as z;
        let t = "'2024-01-01 10:00:00+02'";
        for (sql, want) in [
            (format!("SELECT {t}::timestamptz"), "SELECT '2024-01-01 08:00:00+00'::timestamptz"),
            (format!("SELECT {t} :: TIMESTAMPTZ(3)"), "SELECT '2024-01-01 08:00:00+00' :: TIMESTAMPTZ(3)"),
            (format!("SELECT {t}::timestamp"), "SELECT '2024-01-01 10:00:00'::timestamp"),
            (format!("SELECT {t}::timestamp with time zone"), "SELECT '2024-01-01 08:00:00+00'::timestamp with time zone"),
            (format!("SELECT {t}::timestamp(0) without time zone"), "SELECT '2024-01-01 10:00:00'::timestamp(0) without time zone"),
            (format!("VALUES (TIMESTAMPTZ {t})"), "VALUES (TIMESTAMPTZ '2024-01-01 08:00:00+00')"),
            (format!("VALUES (timestamp {t})"), "VALUES (timestamp '2024-01-01 10:00:00')"),
            (format!("VALUES(TIMESTAMP(3) {t})"), "VALUES(TIMESTAMP(3) '2024-01-01 10:00:00')"),
            (format!("VALUES (TIMESTAMP WITH TIME ZONE {t})"), "VALUES (TIMESTAMP WITH TIME ZONE '2024-01-01 08:00:00+00')"),
            (format!("VALUES (CAST({t} AS timestamptz))"), "VALUES (CAST('2024-01-01 08:00:00+00' AS timestamptz))"),
            (format!("VALUES (cast ( {t} as timestamp))"), "VALUES (cast ( '2024-01-01 10:00:00' as timestamp))"),
            (format!("VALUES (({t})::timestamp)"), "VALUES (('2024-01-01 10:00:00')::timestamp)"),
            (format!("VALUES (( {t} ) :: timestamptz)"), "VALUES (( '2024-01-01 08:00:00+00' ) :: timestamptz)"),
        ] {
            assert_eq!(z(&sql), want, "{sql}");
        }
        // Not typed, not a timestamp, no zone, or not a literal at all.
        for sql in [
            format!("SELECT * FROM t WHERE timestamp_col = {t}"),
            format!("SELECT {t}::text FROM timestamps"),
            format!("SELECT x AT TIME ZONE {t}"),
            "SELECT 'now'::timestamptz".to_string(),
            "SELECT '2024-01-01 10:00:00'::timestamptz".to_string(),
            "SELECT '2024-01-01 10:00:00Z'::timestamp".to_string(),
            format!("SELECT \"{t}\"::timestamptz"),
            format!("SELECT t.timestamp {t}"),
            format!("SELECT f({t})::timestamp"),
            format!("SELECT 1 -- {t}::timestamptz\n"),
            format!("SELECT E{t}::text, $$x$$::timestamp"),
        ] {
            assert_eq!(z(&sql), sql.as_str(), "{sql}");
        }
        // Through the entry point every statement takes.
        assert_eq!(
            super::normalize_string_literals(&format!("SELECT E'a', {t}::timestamptz")).unwrap(),
            "SELECT 'a', '2024-01-01 08:00:00+00'::timestamptz"
        );
        assert!(super::has_zoned_timestamp_literal("a > '2024-01-01 09:00:00+05'"));
        assert!(!super::has_zoned_timestamp_literal("a > '2024-01-01 09:00:00'"));
    }

    #[test]
    fn a_doubled_quote_in_an_identifier_is_found_and_nowhere_else() {
        use super::identifier_with_doubled_quote as found;
        assert_eq!(found(r#"CREATE TABLE "t"" x ""y" (id INT)"#), Some(r#""t"" x ""y""#));
        assert_eq!(found(r#"SELECT "a""b" FROM t"#), Some(r#""a""b""#));
        assert_eq!(found(r#"SELECT "a""" FROM t"#), Some(r#""a""""#));
        assert_eq!(found(r#"SELECT "a" FROM "b""#), None);
        assert_eq!(found(r#"SELECT "" FROM t"#), None);
        assert_eq!(found(r#"SELECT 'x""y', $$""$$ FROM t -- "a""b""#), None);
        assert_eq!(found(r#"SELECT 1 /* "a""b" */"#), None);
    }

    use super::*;

    /// `#>> ARRAY[…]` and `#> ARRAY[…]` of constants read as path literals;
    /// anything that is not a constant path stays as written.
    #[test]
    fn a_json_path_array_of_constants_is_a_path_literal() {
        let r = |s: &str| json_path_arrays(s).into_owned();
        assert_eq!(r("SELECT data #>> ARRAY['0','related_row_id'] FROM t"), "SELECT data #>> '{0,related_row_id}' FROM t");
        assert_eq!(r("SELECT d #> array[ 'a' , 1 ] FROM t"), "SELECT d #> '{a,1}' FROM t");
        assert_eq!(
            r("SELECT 1 FROM t WHERE d#>>ARRAY[CAST('0' AS text), 'k'::text] IN ('x') AND d #>> ARRAY[('b')::varchar] = 'y'"),
            "SELECT 1 FROM t WHERE d#>> '{0,k}' IN ('x') AND d #>> '{b}' = 'y'"
        );
        // Keys that need quoting inside the array literal, and a quote.
        assert_eq!(r(r#"SELECT d #>> ARRAY['a b', 'c,d', 'it''s', '', 'x"y'] FROM t"#), r#"SELECT d #>> '{"a b","c,d",it''s,"","x\"y"}' FROM t"#);
        // Not constants, or not this operator: unchanged.
        for sql in [
            "SELECT d #>> ARRAY[k, 'a'] FROM t",
            "SELECT d #>> ARRAY[$1, 'a'] FROM t",
            "SELECT d #>> ARRAY[NULL, 'a'] FROM t",
            "SELECT d #>> '{a,b}' FROM t",
            "SELECT ARRAY['a'] FROM t",
            "SELECT '#>> ARRAY[''a'']' FROM t",
            "SELECT d #- ARRAY['a'] FROM t",
        ] {
            assert_eq!(r(sql), sql, "{sql}");
        }
    }

    #[test]
    fn unquoted_words_fold_and_quoted_text_keeps_its_case() {
        let fold = |s: &str| fold_unquoted_identifiers(s).into_owned();
        assert_eq!(
            fold("SELECT ID AS Ident, \"Quoted\", 'Str''S' FROM Acct WHERE x = 1E5 AND Y IS NULL"),
            "SELECT id AS ident, \"Quoted\", 'Str''S' FROM acct WHERE x = 1E5 AND y IS NULL"
        );
        // A string's prefix belongs to the literal; an escape string's `\'`
        // and a dollar quote's body are inside it; comments are left alone.
        assert_eq!(fold("SELECT X'FF', B'01', U&'A', U&\"B\" FROM T"), "SELECT X'FF', B'01', U&'A', U&\"B\" FROM t");
        assert_eq!(
            fold("SELECT E'It\\'S', $Tg$ Body $Tg$, Col -- Don't Fold\n FROM T /* X */"),
            "SELECT E'It\\'S', $Tg$ Body $Tg$, col -- Don't Fold\n FROM t /* X */"
        );
        // Non-ASCII letters stay; the ASCII ones around them fold.
        assert_eq!(fold("SELECT naïveName FROM Ü"), "SELECT naïvename FROM Ü");
        assert_eq!(fold("UPDATE t SET A = $1, \"B\"\"C\" = 'x'"), "update t set a = $1, \"B\"\"C\" = 'x'");
        // Nothing to fold: borrowed.
        assert!(matches!(fold_unquoted_identifiers("SELECT a FROM \"T\" WHERE b = 'X'"), Cow::Borrowed(_)));
        // Offsets are kept.
        let text = "INSERT INTO Tbl (A) VALUES ('Q')";
        assert_eq!(fold(text), "insert INTO tbl (a) values ('Q')");
        assert_eq!(fold(text).len(), text.len());
    }

    #[test]
    fn a_relation_star_is_read_as_its_columns() {
        let allowed = ["t", "public.t"];
        assert!(is_qualified_star("t.*", &allowed));
        assert!(is_qualified_star("public.t.*", &allowed));
        assert!(is_qualified_star("T.*", &allowed));
        assert!(!is_qualified_star("u.*", &allowed));
        assert!(!is_qualified_star("t.*, id", &allowed));
        assert!(!is_qualified_star("\"T\".*", &allowed));
    }

    #[test]
    fn a_quoted_alias_is_its_exact_spelling() {
        let quoted = relation_qualifiers("t", Some("\"X\""));
        let quoted: Vec<&str> = quoted.iter().map(String::as_str).collect();
        assert_eq!(unknown_qualifier("\"X\".v + 1", &quoted), None);
        assert_eq!(unknown_qualifier("x.v + 1", &quoted).as_deref(), Some("x"));
        assert_eq!(unknown_qualifier("X.v + 1", &quoted).as_deref(), Some("X"));
        let lower = relation_qualifiers("t", Some("\"x\""));
        let lower: Vec<&str> = lower.iter().map(String::as_str).collect();
        assert_eq!(unknown_qualifier("X.v + x.v + \"x\".v", &lower), None);
        let bare = relation_qualifiers("t", Some("x"));
        let bare: Vec<&str> = bare.iter().map(String::as_str).collect();
        assert_eq!(unknown_qualifier("X.v + x.v", &bare), None);
    }

    fn norm(sql: &str) -> String {
        normalize_string_literals(sql).unwrap().into_owned()
    }

    #[test]
    fn catalog_spellings_are_read_as_this_engine_reads_them() {
        use super::catalog_syntax;
        assert_eq!(
            catalog_syntax("SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) FROM pg_catalog.pg_attribute a"),
            "SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a"
        );
        assert_eq!(
            catalog_syntax("WHERE c.relname OPERATOR(pg_catalog.~) '^(t)$' COLLATE pg_catalog.default AND pg_catalog.pg_table_is_visible(c.oid)"),
            "WHERE c.relname  ~  '^(t)$'  AND TRUE"
        );
        assert_eq!(catalog_syntax("SELECT x COLLATE \"C\", y COLLATE \"de_DE\""), "SELECT x , y COLLATE \"de_DE\"");
        assert_eq!(catalog_syntax("SELECT 'r'::\"char\", c.oid::oid, t.oidx"), "SELECT 'r'::text, c.oid::bigint, t.oidx");
        // Inside a string or a quoted identifier: untouched.
        assert_eq!(catalog_syntax("SELECT 'pg_catalog.x', \"pg_catalog.y\""), "SELECT 'pg_catalog.x', \"pg_catalog.y\"");
        // A column named like the qualifier's tail is not a qualifier.
        assert_eq!(catalog_syntax("SELECT my_pg_catalog.x FROM t"), "SELECT my_pg_catalog.x FROM t");
        assert!(matches!(catalog_syntax("SELECT 1"), Cow::Borrowed(_)));
    }

    #[test]
    fn array_subqueries_become_aggregates() {
        use super::array_subqueries;
        assert_eq!(
            array_subqueries("SELECT id, ARRAY(SELECT w FROM sb WHERE sb.id <= sa.id ORDER BY w) FROM sa"),
            "SELECT id, COALESCE((SELECT array_agg(w ORDER BY w) FROM sb WHERE sb.id <= sa.id), '{}') FROM sa"
        );
        assert_eq!(
            array_subqueries("SELECT ARRAY(SELECT w FROM sb LIMIT 2)"),
            "SELECT COALESCE((SELECT array_agg(__arr_v) FROM (SELECT w AS __arr_v FROM sb LIMIT 2) __arr), '{}')"
        );
        // The constructor over elements, and a column named like it: untouched.
        assert_eq!(array_subqueries("SELECT ARRAY[1, 2], my_array(x), 'ARRAY(SELECT 1)'"), "SELECT ARRAY[1, 2], my_array(x), 'ARRAY(SELECT 1)'");
    }

    #[test]
    fn line_breaks_outside_quotes_become_spaces() {
        use super::flatten_whitespace;
        assert_eq!(flatten_whitespace("SELECT CASE\nWHEN a\tTHEN 1 END\r\nFROM t"), "SELECT CASE WHEN a THEN 1 END  FROM t");
        // Kept inside a string, a quoted identifier and a dollar-quoted body.
        assert_eq!(flatten_whitespace("SELECT 'a\nb', \"x\ny\"\nFROM t"), "SELECT 'a\nb', \"x\ny\" FROM t");
        assert_eq!(
            flatten_whitespace("CREATE FUNCTION f() AS $$\nBEGIN\n  RETURN 1;\nEND $$\nLANGUAGE plpgsql"),
            "CREATE FUNCTION f() AS $$\nBEGIN\n  RETURN 1;\nEND $$ LANGUAGE plpgsql"
        );
        // A `$` inside an identifier opens nothing; a placeholder neither.
        assert_eq!(flatten_whitespace("SELECT a$b$\nFROM t WHERE x = $1\n"), "SELECT a$b$ FROM t WHERE x = $1 ");
        assert!(matches!(flatten_whitespace("SELECT 1"), Cow::Borrowed(_)));
    }

    #[test]
    fn escape_strings_become_standard_strings() {
        assert_eq!(norm(r"SELECT E'it\'s%'"), "SELECT 'it''s%'");
        assert_eq!(norm(r"x = e'a\\b'"), r"x = 'a\b'");
        assert_eq!(norm(r"E'\n\t\x41\101é\U0001F642'"), "'\n\tAAé🙂'");
        assert_eq!(norm(r"E'a''b'"), "'a''b'");
        assert_eq!(norm(r"E'🙂'"), "'🙂'");
        assert_eq!(norm(r"E'\q'"), "'q'");
        // Unterminated (the last quote is escaped): left as written.
        assert_eq!(norm(r"x = E'abc\'"), r"x = E'abc\'");
        // Not an escape-string prefix: the E ends a word.
        assert_eq!(norm(r"CASE WHEN x THEN 1 ELSE'\' END"), r"CASE WHEN x THEN 1 ELSE'\' END");
        // Inside a standard string, a comment or an identifier: untouched.
        assert_eq!(norm(r"'E''x' -- E'\'' "), r"'E''x' -- E'\'' ");
        assert_eq!(norm(r#"/* E'\' */ "E'" "#), r#"/* E'\' */ "E'" "#);
    }

    #[test]
    fn dollar_quotes_become_standard_strings() {
        assert_eq!(norm("SELECT $$a, 'b$$"), "SELECT 'a, ''b'");
        assert_eq!(norm("x = $t$ $$ $1 $t$, $1"), "x = ' $$ $1 ', $1");
        // Placeholders and identifiers with `$` are left alone.
        assert_eq!(norm("WHERE a$b$ = $1 AND c = $2"), "WHERE a$b$ = $1 AND c = $2");
        // Unterminated: as written.
        assert_eq!(norm("SELECT $$abc"), "SELECT $$abc");
    }

    #[test]
    fn bad_escapes_are_errors() {
        assert!(normalize_string_literals(r"E'\0'").is_err());
        assert!(normalize_string_literals(r"E'\xff'").is_err());
        assert!(normalize_string_literals(r"E'\u12'").is_err());
        assert!(normalize_string_literals(r"E'\uD83D'").is_err());
    }

    #[test]
    fn nothing_to_do_borrows() {
        assert!(matches!(
            normalize_string_literals("SELECT 'x' FROM t WHERE a = $1"),
            Ok(Cow::Borrowed(_))
        ));
    }

    #[test]
    fn top_level_split_tracks_nesting() {
        assert_eq!(
            split_top_level(r"1, f(2, 3), ARRAY[4,5], '6,7'::text, E'8\',9', $$1,0$$, ", b','),
            vec!["1", "f(2, 3)", "ARRAY[4,5]", "'6,7'::text", r"E'8\',9'", "$$1,0$$"]
        );
        assert_eq!(split_top_level("a,,b", b','), vec!["a", "", "b"]);
        assert_eq!(matching_close("(a, (b), ')', E'\\')') x", 0), Some(20));
    }
}

#[cfg(test)]
mod derived_column_list_tests {
    use super::derived_column_lists;

    #[test]
    fn a_derived_table_s_column_list_names_its_select_list() {
        let cases = [
            (
                "SELECT s.k FROM (SELECT customer_id, total FROM ord) AS s(k, t)",
                "SELECT s.k FROM (SELECT customer_id AS k, total AS t FROM ord) AS s",
            ),
            ("SELECT k FROM (SELECT a FROM t) s(k) ORDER BY 1", "SELECT k FROM (SELECT a AS k FROM t) AS s ORDER BY 1"),
            // Fewer names rename the first columns; an alias is replaced.
            (
                "SELECT * FROM (SELECT DISTINCT a AS x, count(*) n, b FROM t GROUP BY a, b) AS d (p, q)",
                "SELECT * FROM (SELECT DISTINCT a AS p, count(*) AS q, b FROM t GROUP BY a, b) AS d",
            ),
            // A set operation is named by its first branch.
            (
                "SELECT * FROM (SELECT 1 UNION SELECT 2) u(n)",
                "SELECT * FROM (SELECT 1 AS n UNION SELECT 2) AS u",
            ),
            (
                "SELECT * FROM t JOIN (SELECT a, b FROM u) AS j(\"X\", y) ON j.y = t.y",
                "SELECT * FROM t JOIN (SELECT a AS \"X\", b AS y FROM u) AS j ON j.y = t.y",
            ),
            (
                "SELECT * FROM (SELECT x IS NULL, CASE WHEN a THEN 1 END FROM t) AS z(n, c)",
                "SELECT * FROM (SELECT x IS NULL AS n, CASE WHEN a THEN 1 END AS c FROM t) AS z",
            ),
        ];
        for (sql, want) in cases {
            assert_eq!(derived_column_lists(sql).as_deref(), Some(want), "{sql}");
        }
    }

    #[test]
    fn what_is_no_derived_column_list_is_left_alone() {
        for sql in [
            "SELECT * FROM (SELECT a FROM t) s",
            "SELECT * FROM (SELECT a FROM t) AS s",
            "SELECT * FROM (SELECT * FROM t) AS s(a, b)",
            "SELECT * FROM (SELECT a FROM t) AS s(a, b)",
            "SELECT 1 WHERE x IN (SELECT a FROM t) AND (y = 1)",
            "SELECT * FROM (VALUES (1, 2)) AS v(a, b)",
            "SELECT '(SELECT a FROM t) s(k)'",
        ] {
            assert_eq!(derived_column_lists(sql), None, "{sql}");
        }
    }
}

#[cfg(test)]
mod statement_shorthand_tests {
    use super::statement_shorthands;

    #[test]
    fn shorthands_are_the_statements_they_stand_for() {
        assert_eq!(statement_shorthands("TABLE t2 ORDER BY id LIMIT 2"), "SELECT * FROM t2 ORDER BY id LIMIT 2");
        assert_eq!(
            statement_shorthands("SELECT id FROM t WHERE id = ANY('{1,2}'::int[]) AND s <> ALL (ARRAY['a', 'b']::varchar(3)[])"),
            "SELECT id FROM t WHERE id = ANY('{1,2}') AND s <> ALL (ARRAY['a', 'b'])"
        );
        assert_eq!(statement_shorthands("SELECT '{1}'::int[] FROM t"), "SELECT '{1}'::int[] FROM t");
        assert_eq!(statement_shorthands("INSERT INTO t DEFAULT  VALUES RETURNING id"), "INSERT INTO t VALUES (DEFAULT) RETURNING id");
        assert_eq!(statement_shorthands("SELECT 'DEFAULT VALUES' FROM t"), "SELECT 'DEFAULT VALUES' FROM t");
        assert_eq!(statement_shorthands("CREATE UNLOGGED TABLE d (a int)"), "CREATE TABLE d (a int)");
        assert_eq!(
            statement_shorthands("SELECT id INTO TEMP d21 FROM t1 WHERE id < 3"),
            "CREATE TEMP TABLE d21 AS SELECT id FROM t1 WHERE id < 3"
        );
        assert_eq!(statement_shorthands("SELECT 1 INTO x"), "CREATE TABLE x AS SELECT 1");
        assert_eq!(statement_shorthands("SELECT ROW(1,2) < ROW(1,3), row(4)"), "SELECT (1,2) < (1,3), row(4)");
        assert_eq!(statement_shorthands("SELECT t.row FROM t"), "SELECT t.row FROM t");
        assert_eq!(
            statement_shorthands("WITH x AS MATERIALIZED (SELECT 1), y AS NOT MATERIALIZED (SELECT 2) SELECT * FROM x, y"),
            "WITH x AS (SELECT 1), y AS (SELECT 2) SELECT * FROM x, y"
        );
        for same in ["SELECT 'AS MATERIALIZED (' FROM t", "SELECT tablename FROM t", "SELECT 1 AS materialized FROM t", "CREATE MATERIALIZED VIEW v AS SELECT 1"] {
            assert_eq!(statement_shorthands(same), same);
        }
    }
}

#[cfg(test)]
mod row_assignment_tests {
    use super::row_assignments;

    #[test]
    fn a_row_assignment_is_one_per_column() {
        assert_eq!(
            row_assignments("UPDATE d SET (a, b) = (a + 10, upper(b)) WHERE a = 1").unwrap().as_deref(),
            Some("UPDATE d SET a = a + 10, b = upper(b) WHERE a = 1")
        );
        assert_eq!(
            row_assignments("UPDATE d SET c = 1, (a, b) = ROW(2, 'x')").unwrap().as_deref(),
            Some("UPDATE d SET c = 1, a = 2, b = 'x' ")
        );
        assert_eq!(
            row_assignments("UPDATE d SET (a, b) = (SELECT p, q FROM e WHERE e.id = d.id) RETURNING a").unwrap().as_deref(),
            Some("UPDATE d SET a = (SELECT p FROM e WHERE e.id = d.id), b = (SELECT q FROM e WHERE e.id = d.id) RETURNING a")
        );
        assert_eq!(
            row_assignments("UPDATE d SET (a, b) = (1)").unwrap_err(),
            "source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression"
        );
        // A list mixing an aggregate with a plain value stays one row.
        assert_eq!(
            row_assignments("UPDATE d SET (a, b) = (SELECT max(n) AS m, 'z' FROM e WHERE e.k > d.id) WHERE id = 1").unwrap().as_deref(),
            Some(
                "UPDATE d SET a = (SELECT __rdb_row1 FROM (SELECT max(n) AS __rdb_row1, 'z' AS __rdb_row2 FROM e WHERE e.k > d.id) AS __rdb_row), \
                 b = (SELECT __rdb_row2 FROM (SELECT max(n) AS __rdb_row1, 'z' AS __rdb_row2 FROM e WHERE e.k > d.id) AS __rdb_row) WHERE id = 1"
            )
        );
        assert!(super::contains_aggregate_call("COUNT (*) + 1") && !super::contains_aggregate_call("'max(x)'") && !super::contains_aggregate_call("maxim(x)"));
        assert_eq!(
            row_assignments("UPDATE oc SET (n, s) = 1 WHERE TRUE").unwrap_err(),
            "source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression"
        );
        assert_eq!(
            row_assignments("UPDATE oc SET (n, s) = (1) WHERE TRUE").unwrap_err(),
            "source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression"
        );
        assert_eq!(
            row_assignments("UPDATE d SET (a, b) = ROW(1)").unwrap_err(),
            "number of columns does not match number of values"
        );
        assert_eq!(row_assignments("UPDATE d SET a = (1 + 2) WHERE b = 1").unwrap(), None);
        assert_eq!(row_assignments("SELECT (a, b) = (1, 2) FROM d").unwrap(), None);
    }
}

#[cfg(test)]
mod named_window_tests {
    use super::named_windows;

    #[test]
    fn a_named_window_is_written_where_it_is_used() {
        assert_eq!(
            named_windows("SELECT id, rank() OVER w, dense_rank() OVER w FROM t1 WINDOW w AS (ORDER BY sm) ORDER BY id LIMIT 5").unwrap().as_deref(),
            Some("SELECT id, rank() OVER (ORDER BY sm), dense_rank() OVER (ORDER BY sm) FROM t1 ORDER BY id LIMIT 5")
        );
        assert_eq!(
            named_windows("SELECT sum(x) OVER (b ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t WINDOW a AS (PARTITION BY p), b AS (a ORDER BY x)").unwrap().as_deref(),
            Some("SELECT sum(x) OVER (PARTITION BY p ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t ")
        );
        assert!(named_windows("SELECT rank() OVER nope FROM t WINDOW w AS (ORDER BY x)").is_err());
        assert_eq!(named_windows("SELECT rank() OVER (ORDER BY x) FROM t").unwrap(), None);
        assert!(named_windows("SELECT rank() OVER nope FROM t").is_err());
        assert_eq!(named_windows("SELECT over AS x FROM t").unwrap(), None);
    }
}

#[cfg(test)]
mod multibyte_text_tests {
    use super::*;

    /// Statements that wake each pass: quantified array casts, row
    /// assignments, named windows, derived column lists, shorthands,
    /// operators, samples, typed literals, string forms, catalogue casts.
    const TRIGGERS: &[&str] = &[
        "SELECT * FROM t WHERE a = ANY('{1,2}'::int[]) AND b <> ALL(ARRAY[1,2]::bigint[]) OR c = SOME('{x}'::text[])",
        "UPDATE t SET (a, b) = (1, 'x') WHERE c = 'y' RETURNING a",
        "SELECT sum(a) OVER w, rank() OVER (w ORDER BY c) FROM t WINDOW w AS (PARTITION BY b)",
        "SELECT d.x FROM (SELECT a, b FROM t) AS d(x, y)",
        "TABLE t",
        "CREATE UNLOGGED TABLE t (a INT, b TEXT)",
        "SELECT a INTO t2 FROM t WHERE b = 'z'",
        "WITH x AS MATERIALIZED (SELECT 1) SELECT * FROM x",
        "SELECT ROW(1, 'a') = ROW(1, 'a'), (a, b) IN (SELECT 1, 2) FROM t",
        "SELECT a || 'x', a ~ 'b', a !~* 'c', a ^@ 'd' FROM t",
        "SELECT * FROM t TABLESAMPLE BERNOULLI (10) REPEATABLE (1)",
        "SELECT DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00+02', INTERVAL '1 day'",
        "SELECT E'a\\nb', U&'d\\0061t', $$q$$, $t$q$t$ FROM t",
        "SELECT ARRAY(SELECT a FROM t), 't'::regclass, to_regclass('t')",
        "SELECT * FROM t WHERE a > ANY (SELECT b FROM u) AND a < ALL (SELECT c FROM v)",
        "SELECT \"MixedCase\", Lower FROM T -- note\nWHERE /* c */ x = 1",
        "UPDATE stores SET name = 'Probe', guest_buttons = '[{\"label\":\"Call the waiter\"}]', basic_guest_buttons = '[]' WHERE id = 's1' RETURNING id",        "SELECT 1 WHERE NOT EXISTS (SELECT 'x' WHERE a = 'y') AND EXISTS (SELECT 1) OR EXISTS (SELECT 2 WHERE b > 3)",
    ];

    /// Every pass over every trigger statement with a 2-, 3- and 4-byte
    /// character put at every position: no pass may slice text inside a
    /// character. One did (`quantified_array_casts`), and the server handler
    /// panicked on an ORM UPDATE of a row with Greek text in it.
    #[test]
    fn no_pass_cuts_a_multibyte_character() {
        type Pass = fn(&str);
        let passes: &[(&str, Pass)] = &[
            ("catalog_syntax", |s| drop(catalog_syntax(s))),
            ("array_subqueries", |s| drop(array_subqueries(s))),
            ("row_assignments", |s| drop(row_assignments(s))),
            ("named_windows", |s| drop(named_windows(s))),
            ("derived_column_lists", |s| drop(derived_column_lists(s))),
            ("statement_shorthands", |s| drop(statement_shorthands(s))),
            ("flatten_whitespace", |s| drop(flatten_whitespace(s))),
            ("identifier_with_doubled_quote", |s| { let _ = identifier_with_doubled_quote(s); }),
            ("typed_literals_as_casts", |s| drop(typed_literals_as_casts(s))),
            ("quantified_subqueries", |s| drop(quantified_subqueries(s))),
            ("find_quantified_subquery", |s| drop(find_quantified_subquery(s))),
            ("text_operators", |s| drop(text_operators(s))),
            ("tablesample", |s| drop(tablesample(s))),
            ("fold_core_statement", |s| drop(fold_core_statement(s))),
            ("fold_unquoted_identifiers", |s| drop(fold_unquoted_identifiers(s))),
            ("normalize_string_literals", |s| drop(normalize_string_literals(s))),
            ("zone_free_timestamp_literals", |s| drop(zone_free_timestamp_literals(s))),
            ("from_less_exists", |s| drop(from_less_exists(s))),
        ];
        let mut failures = Vec::new();
        for base in TRIGGERS {
            for at in (0..=base.len()).filter(|&i| base.is_char_boundary(i)) {
                for ch in ["Δ", "€", "😀"] {
                    let sql = format!("{}{}{}", &base[..at], ch, &base[at..]);
                    for (name, pass) in passes {
                        if std::panic::catch_unwind(|| pass(&sql)).is_err() {
                            failures.push(format!("{name}: {sql}"));
                        }
                    }
                }
            }
        }
        assert!(failures.is_empty(), "{} panics, first: {:#?}", failures.len(), &failures[..failures.len().min(10)]);
    }
}

#[cfg(test)]
mod keyword_prefix_tests {
    use super::starts_with_keyword;

    /// A keyword is a whole word. `count(selected_branches)` was a subquery
    /// named "selected_branches" while only the prefix was asked.
    #[test]
    fn a_keyword_is_a_whole_word() {
        for text in ["SELECT 1", "select\n1", "Select(1)", "SELECT", "SELECT*", "select'x'", "SELECT\"a\""] {
            assert!(starts_with_keyword(text, "SELECT"), "{text}");
        }
        for text in ["selected", "selected_branches", "selection = 1", "SELECT_1", "select9", "selecté", "sel", "", " SELECT 1"] {
            assert!(!starts_with_keyword(text, "SELECT"), "{text}");
        }
        assert!(starts_with_keyword("with q AS (SELECT 1)", "WITH"));
        assert!(!starts_with_keyword("with_x + 1", "WITH"));
        assert!(!starts_with_keyword("withdrawn", "WITH"));
        assert!(starts_with_keyword("VALUES (1)", "VALUES"));
        assert!(!starts_with_keyword("values_n", "VALUES"));
    }
}

#[cfg(test)]
mod branch_parentheses_tests {
    use super::*;

    fn plain(sql: &str) -> String {
        plain_branch_parentheses(sql).into_owned()
    }

    #[test]
    fn parentheses_that_group_nothing_are_removed() {
        assert_eq!(plain("(SELECT a FROM t)"), "SELECT a FROM t");
        assert_eq!(plain("(SELECT a FROM t) EXCEPT (SELECT 1) ORDER BY 1"), "SELECT a FROM t EXCEPT SELECT 1 ORDER BY 1");
        assert_eq!(plain("SELECT 9 UNION ALL (SELECT a FROM t WHERE EXISTS (SELECT 1))"), "SELECT 9 UNION ALL SELECT a FROM t WHERE EXISTS (SELECT 1)");
        assert_eq!(plain("((SELECT 1)) UNION ALL ((SELECT 2));"), "SELECT 1 UNION ALL SELECT 2");
        // The whole query alone: the same without them, whatever it ends in.
        assert_eq!(plain("(SELECT a FROM t ORDER BY a LIMIT 1)"), "SELECT a FROM t ORDER BY a LIMIT 1");
        assert_eq!(plain("(SELECT a FROM t) ORDER BY a LIMIT 1"), "SELECT a FROM t ORDER BY a LIMIT 1");
        assert_eq!(plain("(SELECT 1 UNION SELECT 2) ORDER BY 1"), "SELECT 1 UNION SELECT 2 ORDER BY 1");
    }

    #[test]
    fn parentheses_that_group_are_kept_or_made_a_derived_table() {
        // A branch's own ORDER BY and LIMIT are its own.
        for sql in [
            "(SELECT a FROM t ORDER BY a LIMIT 1) UNION ALL (SELECT a FROM t ORDER BY a DESC LIMIT 1)",
            "(SELECT a FROM t ORDER BY a LIMIT 2) ORDER BY a DESC",
            "SELECT 1 UNION (SELECT a FROM t LIMIT 1)",
        ] {
            assert!(matches!(plain_branch_parentheses(sql), Cow::Borrowed(_)), "{sql}");
        }
        // A group decides the order of evaluation: a derived table.
        assert_eq!(
            plain("(SELECT 1 UNION SELECT 2) INTERSECT SELECT 2"),
            "SELECT * FROM (SELECT 1 UNION SELECT 2) __rdb_branch0 INTERSECT SELECT 2"
        );
        assert_eq!(
            plain("((SELECT 1) UNION (SELECT 2)) INTERSECT (SELECT 2)"),
            "SELECT * FROM (SELECT 1 UNION SELECT 2) __rdb_branch0 INTERSECT SELECT 2"
        );
        assert_eq!(
            plain("SELECT 1 EXCEPT ALL (SELECT 1 UNION SELECT 2)"),
            "SELECT 1 EXCEPT ALL SELECT * FROM (SELECT 1 UNION SELECT 2) __rdb_branch1"
        );
    }

    #[test]
    fn anything_else_is_left_as_written() {
        for sql in [
            "SELECT a FROM t WHERE id = 1",
            "SELECT a FROM t WHERE b IN ('x', 'union') UNION ALL SELECT 7",
            "SELECT '(SELECT 1) UNION (SELECT 2)'",
            "SELECT a FROM (SELECT 1 AS a) d UNION SELECT 2",
            "INSERT INTO t VALUES (1)",
            "(VALUES (1), (2))",
            // Multibyte text beside the parentheses.
            "SELECT 'é(ü' UNION ALL SELECT 'ß)'",
        ] {
            assert!(matches!(plain_branch_parentheses(sql), Cow::Borrowed(_)), "{sql}");
        }
        assert_eq!(plain("(SELECT 'é) UNION (ü' AS t) UNION ALL (SELECT 'ß')"), "SELECT 'é) UNION (ü' AS t UNION ALL SELECT 'ß'");
    }

    #[test]
    fn only_the_parentheses_around_the_whole_are_removed() {
        assert_eq!(without_outer_parentheses(" ((SELECT 1)) "), "SELECT 1");
        assert_eq!(without_outer_parentheses("SELECT 9, round(2.5, 1)"), "SELECT 9, round(2.5, 1)");
        assert_eq!(without_outer_parentheses("(SELECT 9, CAST(NULL AS numeric))"), "SELECT 9, CAST(NULL AS numeric)");
        assert_eq!(without_outer_parentheses("(SELECT 1) UNION (SELECT 2)"), "(SELECT 1) UNION (SELECT 2)");
        assert_eq!(without_outer_parentheses("(SELECT ')'"), "(SELECT ')'");
    }

    #[test]
    fn a_query_inside_parentheses_is_read_the_same_way() {
        let nested = |sql: &str| nested_branch_parentheses(sql).into_owned();
        assert_eq!(nested("SELECT a FROM t WHERE a IN ((SELECT 1) UNION (SELECT 2))"), "SELECT a FROM t WHERE a IN (SELECT 1 UNION SELECT 2)");
        assert_eq!(nested("SELECT EXISTS ((SELECT 1) EXCEPT (SELECT 1))"), "SELECT EXISTS (SELECT 1 EXCEPT SELECT 1)");
        assert_eq!(nested("SELECT ((SELECT 1) UNION (SELECT 1)) AS one"), "SELECT (SELECT 1 UNION SELECT 1) AS one");
        assert_eq!(nested("SELECT a FROM t WHERE a IN (SELECT 1 UNION (SELECT b FROM u))"), "SELECT a FROM t WHERE a IN (SELECT 1 UNION SELECT b FROM u)");
        // A group inside a group.
        assert_eq!(
            nested("SELECT a FROM t WHERE a IN ((SELECT 1) EXCEPT ((SELECT 2) UNION (SELECT 3)))"),
            "SELECT a FROM t WHERE a IN (SELECT 1 EXCEPT SELECT * FROM (SELECT 2 UNION SELECT 3) __rdb_branch1)"
        );
        for sql in [
            "SELECT a FROM t WHERE (a = 1) AND ((b = 2) OR (c = 3))",
            "SELECT ((SELECT 1) + (SELECT 2)) UNION ALL SELECT 3",
            "SELECT ((SELECT 1), (SELECT 2)) UNION ALL SELECT (3, 4)",
            "SELECT '((SELECT 1) UNION (SELECT 2))' UNION ALL SELECT 'x'",
            "SELECT a FROM t WHERE a IN (SELECT b FROM u UNION SELECT c FROM v)",
            "SELECT a FROM t WHERE a IN ((SELECT b FROM u ORDER BY b LIMIT 1) UNION ALL (SELECT c FROM v ORDER BY c LIMIT 1))",
        ] {
            assert!(matches!(nested_branch_parentheses(sql), Cow::Borrowed(_)), "{sql}");
        }
    }

    #[test]
    fn a_source_query_in_parentheses_is_read_as_a_query() {
        let source = |sql: &str| source_query_parentheses(sql).into_owned();
        assert_eq!(source("INSERT INTO t (a, b) (SELECT 1, 2)"), "INSERT INTO t (a, b) SELECT 1, 2");
        assert_eq!(source("INSERT INTO t (SELECT a, b FROM s)"), "INSERT INTO t SELECT a, b FROM s");
        assert_eq!(source("insert into \"T x\" ((SELECT 1))"), "insert into \"T x\" SELECT 1");
        assert_eq!(
            source("INSERT INTO t (a) (SELECT 1) UNION ALL (SELECT 2) ON CONFLICT (a) DO NOTHING RETURNING a"),
            "INSERT INTO t (a) SELECT 1 UNION ALL SELECT 2 ON CONFLICT (a) DO NOTHING RETURNING a"
        );
        assert_eq!(
            source("INSERT INTO t (a) OVERRIDING SYSTEM VALUE (SELECT 1) RETURNING a"),
            "INSERT INTO t (a) OVERRIDING SYSTEM VALUE SELECT 1 RETURNING a"
        );
        assert_eq!(source("CREATE TABLE c AS (SELECT a FROM s) WITH NO DATA"), "CREATE TABLE c AS SELECT a FROM s WITH NO DATA");
        assert_eq!(source("CREATE TABLE c (x) AS (SELECT 1) UNION ALL (SELECT 2)"), "CREATE TABLE c (x) AS SELECT 1 UNION ALL SELECT 2");
        for sql in [
            "INSERT INTO t (a, b) VALUES (1, (SELECT 2))",
            "INSERT INTO t (a, b) SELECT 1, 2",
            "INSERT INTO t VALUES ((SELECT 1))",
            "INSERT INTO t (a) VALUES ('(SELECT 1)')",
            "INSERT INTO t DEFAULT VALUES",
            "CREATE TABLE c (a int, b text)",
            "CREATE TABLE c AS SELECT 1",
            "UPDATE t SET a = (SELECT 1)",
        ] {
            assert!(matches!(source_query_parentheses(sql), Cow::Borrowed(_)), "{sql}");
        }
    }
}

#[cfg(test)]
mod from_less_exists_tests {
    use super::from_less_exists;

    #[test]
    fn a_from_less_exists_is_its_condition_and_nothing_else_is_rewritten() {
        for (sql, expected) in [
            ("SELECT EXISTS (SELECT 1)", "SELECT TRUE"),
            ("SELECT 1 WHERE NOT EXISTS (SELECT count(*), max(1) WHERE false)", "SELECT 1 WHERE FALSE"),
            ("SELECT NOT EXISTS (SELECT 1)", "SELECT FALSE"),
            ("SELECT 1 WHERE EXISTS (SELECT 1 WHERE false)", "SELECT 1 WHERE ((false) IS TRUE)"),
            ("SELECT 1 WHERE NOT EXISTS (SELECT 1 WHERE $1 = 2)", "SELECT 1 WHERE (($1 = 2) IS NOT TRUE)"),
            (
                "SELECT id FROM t WHERE exists(select 'é' where t.n > 5) AND n IS NULL",
                "SELECT id FROM t WHERE ((t.n > 5) IS TRUE) AND n IS NULL",
            ),
            (
                "SELECT 1 WHERE EXISTS (SELECT 1 WHERE NOT EXISTS (SELECT 2 WHERE a = 1))",
                "SELECT 1 WHERE ((((a = 1) IS NOT TRUE)) IS TRUE)",
            ),
        ] {
            assert_eq!(from_less_exists(sql).as_deref(), Some(expected), "{sql}");
        }
        // A relation, a clause that changes the count, a call or a subquery
        // in the select list, `*`, or no EXISTS at all: left as written.
        for sql in [
            "SELECT 1 WHERE EXISTS (SELECT 1 FROM t)",
            "SELECT 1 WHERE EXISTS (SELECT 1 LIMIT 0)",
            "SELECT 1 WHERE EXISTS (SELECT 1 WHERE a = 1 OFFSET 1)",
            "SELECT 1 WHERE EXISTS (SELECT count(*), lower('x') WHERE false)",
            "SELECT 1 WHERE EXISTS (SELECT generate_series(1, 0))",
            "SELECT 1 WHERE EXISTS (SELECT (SELECT 1) WHERE true)",
            "SELECT 1 WHERE EXISTS (SELECT 1 UNION SELECT 2)",
            "SELECT 1 WHERE EXISTS (SELECT 1 GROUP BY 1 HAVING false)",
            "SELECT 1 WHERE EXISTS (SELECT *)",
            "SELECT 'EXISTS (SELECT 1)'",
            "SELECT not_exists (SELECT 1)",
            "SELECT exists_x FROM t",
        ] {
            assert_eq!(from_less_exists(sql), None, "{sql}");
        }
    }
}
