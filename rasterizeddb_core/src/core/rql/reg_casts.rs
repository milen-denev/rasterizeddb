//! A literal cast to `regclass` or `regtype` outside a catalogue statement:
//! `SELECT 'items'::regclass`, `'items'::regclass::oid`, `to_regclass('x')`,
//! `'int4'::regtype`. Each failed ("cast to 'regclass' is not supported
//! yet"); a catalogue statement's casts were read (`catalog_views`).
//!
//! PostgreSQL's `regclass` is an oid that prints as the relation's name. So a
//! cast that is shown (a whole select item, or cast on to `text`) is the name
//! as `regclass` prints it, and one that is compared or cast on to `oid` is
//! the oid the catalogue views give the relation (`catalog_model::oid_of`).
//! `regtype` likewise: the type's name as `format_type` writes it shown,
//! the oid otherwise.
//!
//! Left as written: a cast inside the calls that read the literal
//! themselves (`nextval('s'::regclass)`, `rdb_index_check('t'::regclass)`,
//! the size functions) and every DDL statement, whose `DEFAULT
//! nextval('t_id_seq'::regclass)` is kept as text. A name no relation has
//! is not refused here (a sequence's name is not known without reading its
//! table); `to_regclass` of one is NULL only where the name is no table or
//! view.

use crate::core::sql_scan::{is_identifier_byte, matching_close, quoted_end, skip_comment, skip_quoted};

/// Calls whose `regclass` argument is read off the text by their own code.
const READS_ITS_ARGUMENT: &[&str] = &[
    "nextval",
    "currval",
    "setval",
    "rdb_index_checkpoint",
    "rdb_index_check",
    "pg_relation_size",
    "pg_total_relation_size",
    "pg_table_size",
    "pg_indexes_size",
    "pg_get_serial_sequence",
];

/// A relation name as `regclass` reads a literal: `public.Items` is `items`,
/// `"Items"` is `Items`; `None` for a schema other than `public` or
/// `pg_catalog`, or for no name at all.
fn relation_name(text: &str) -> Option<String> {
    let parts: Vec<String> = split_name(text.trim())?;
    let (schema, name) = match parts.as_slice() {
        [name] => (None, name.clone()),
        [schema, name] => (Some(schema.clone()), name.clone()),
        _ => return None,
    };
    match schema.as_deref() {
        None | Some("public") | Some("pg_catalog") | Some("information_schema") => Some(name),
        Some(_) => None,
    }
}

/// `a.b` and `"A"."b"` into their parts, folded as PostgreSQL folds them.
fn split_name(text: &str) -> Option<Vec<String>> {
    let bytes = text.as_bytes();
    let mut parts = Vec::new();
    let mut i = 0;
    loop {
        if bytes.get(i) == Some(&b'"') {
            let end = quoted_end(bytes, i, b'"');
            if end < i + 2 || bytes.get(end - 1) != Some(&b'"') {
                return None;
            }
            parts.push(text[i + 1..end - 1].replace("\"\"", "\""));
            i = end;
        } else {
            let end = i + bytes[i..].iter().take_while(|b| is_identifier_byte(**b)).count();
            if end == i {
                return None;
            }
            parts.push(text[i..end].to_ascii_lowercase());
            i = end;
        }
        match bytes.get(i) {
            None => return Some(parts),
            Some(b'.') => i += 1,
            Some(_) => return None,
        }
    }
}

/// A name as `regclass` prints it: bare when it needs no quotes.
pub fn regclass_text(name: &str) -> String {
    let plain = name.bytes().next().is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    if plain { name.to_string() } else { crate::core::rql::views::quote_ident(name) }
}

fn sql_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// The word of the call whose argument list encloses `at`, if any.
fn enclosing_call(sql: &str, at: usize) -> Option<&str> {
    let bytes = sql.as_bytes();
    let mut depth = 0i32;
    let mut i = at;
    while i > 0 {
        i -= 1;
        match bytes[i] {
            b')' => depth += 1,
            b'(' if depth == 0 => {
                let before = sql[..i].trim_end();
                let start = before.bytes().rposition(|b| !is_identifier_byte(b)).map_or(0, |p| p + 1);
                return Some(&before[start..]);
            }
            b'(' => depth -= 1,
            _ => {}
        }
    }
    None
}

/// `sql` with its literal `regclass` / `regtype` casts and `to_regclass`
/// calls written as the engine reads them; `None` when it has none.
/// `relation_exists` answers for `to_regclass`.
pub(crate) fn rewrite(sql: &str, relation_exists: &dyn Fn(&str) -> bool) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("reg") {
        return None;
    }
    let head = lower.trim_start();
    if ["create", "alter", "comment", "drop"].iter().any(|k| head.starts_with(k)) {
        return None;
    }
    let bytes = sql.as_bytes();
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        // `to_regclass('name')`.
        if (i == 0 || !is_identifier_byte(bytes[i - 1]))
            && bytes.len() >= i + 11
            && bytes[i..i + 11].eq_ignore_ascii_case(b"to_regclass")
        {
            let open = i + 11 + (sql[i + 11..].len() - sql[i + 11..].trim_start().len());
            if bytes.get(open) == Some(&b'(')
                && let Some(close) = matching_close(sql, open)
            {
                let inner = sql[open + 1..close].trim();
                let inner_at = open + 1 + (sql[open + 1..close].len() - sql[open + 1..close].trim_start().len());
                if inner.starts_with('\'') && quoted_end(bytes, inner_at, b'\'') == inner_at + inner.len() {
                    let text = inner[1..inner.len() - 1].replace("''", "'");
                    let value = match relation_name(&text) {
                        Some(name) if relation_exists(&name) => sql_literal(&regclass_text(&name)),
                        _ => "NULL".to_string(),
                    };
                    edits.push((i, close + 1, value));
                    i = close + 1;
                    continue;
                }
            }
        }
        if bytes[i] != b'\'' {
            if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
                i = end.max(i + 1);
            } else {
                i += 1;
            }
            continue;
        }
        let lit_end = quoted_end(bytes, i, b'\'');
        let lit_start = i;
        i = lit_end.max(i + 1);
        // `'x'::regclass` or `CAST('x' AS regclass)`.
        let after = &sql[lit_end..];
        let (kind, cast_end, span_start) = if let Some(rest) = after.trim_start().strip_prefix("::") {
            let rest_t = rest.trim_start();
            let at = sql.len() - rest_t.len();
            let word: String = rest_t.bytes().take_while(|b| is_identifier_byte(*b)).map(|b| b as char).collect();
            (word.to_ascii_lowercase(), at + word.len(), lit_start)
        } else if let Some(call) = cast_call(sql, lit_start, lit_end) {
            call
        } else {
            continue;
        };
        if kind != "regclass" && kind != "regtype" {
            continue;
        }
        if enclosing_call(sql, span_start).is_some_and(|f| READS_ITS_ARGUMENT.iter().any(|r| f.eq_ignore_ascii_case(r))) {
            continue;
        }
        let text = sql[lit_start + 1..lit_end - 1].replace("''", "'");
        // A cast on to `oid` (dropped: the value is one) or to `text`.
        let next = sql[cast_end..].trim_start();
        // Whether the value is shown, where the casts end, and the column
        // name PostgreSQL gives the last of them.
        let (shown, end, column) = match next.strip_prefix("::") {
            Some(rest) => {
                let t = rest.trim_start();
                let word: String = t.bytes().take_while(|b| is_identifier_byte(*b)).map(|b| b as char).collect();
                let w = word.to_ascii_lowercase();
                let word_end = sql.len() - t.len() + word.len();
                match w.as_str() {
                    "oid" => (false, word_end, w),
                    "text" | "varchar" | "name" => (true, word_end, w),
                    _ => (false, cast_end, kind.clone()),
                }
            }
            None => (crate::core::sql_scan::whole_select_item(sql, span_start, cast_end), cast_end, kind.clone()),
        };
        // `pg_typeof(x) = 'date'::regtype`: `pg_typeof` answers the type's
        // name here, so the type it is compared with is its name too. As an
        // oid it was "operator does not exist: text = integer".
        let shown = shown || (kind == "regtype" && compared_with_pg_typeof(sql, span_start, end));
        let replacement = if kind == "regclass" {
            let Some(name) = relation_name(&text) else { continue };
            if let Ok(n) = text.trim().parse::<i64>() {
                n.to_string()
            } else if shown {
                sql_literal(&regclass_text(&name))
            } else {
                crate::core::rql::catalog_model::oid_of(&name).to_string()
            }
        } else {
            let oid = match text.trim().parse::<i64>() {
                Ok(n) => n,
                Err(_) => crate::core::rql::catalog_model::type_oid_by_name(&text),
            };
            if oid == 0 {
                continue;
            }
            if shown {
                match crate::core::rql::catalog_model::format_type(oid, None) {
                    Some(name) => sql_literal(&name),
                    None => continue,
                }
            } else {
                oid.to_string()
            }
        };
        // A whole select item keeps PostgreSQL's column name.
        let alias = if crate::core::sql_scan::whole_select_item(sql, span_start, end) {
            format!(" AS {column}")
        } else {
            String::new()
        };
        edits.push((span_start, end, format!("{replacement}{alias}")));
        i = end;
    }
    if edits.is_empty() {
        return None;
    }
    let mut out = String::with_capacity(sql.len());
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

/// Whether the value at `start..end` is compared with a `pg_typeof(…)`
/// call: `pg_typeof(x) = <value>`, `<value> <> pg_typeof(x)`, or an item of
/// `pg_typeof(x) [NOT] IN (…, <value>, …)`.
fn compared_with_pg_typeof(sql: &str, start: usize, end: usize) -> bool {
    const OPERATORS: [&str; 3] = ["<>", "!=", "="];
    // The text before `text`'s closing parenthesis, when `text` ends in a
    // call of `pg_typeof`.
    let ends_in_the_call = |text: &str| -> bool {
        let text = text.trim_end();
        if !text.ends_with(')') {
            return false;
        }
        let mut depth = 0usize;
        for (at, byte) in text.bytes().enumerate().rev() {
            match byte {
                b')' => depth += 1,
                b'(' => {
                    depth -= 1;
                    if depth == 0 {
                        let name = text[..at].trim_end();
                        let word = name.len() - name.bytes().rev().take_while(|b| is_identifier_byte(*b)).count();
                        return name[word..].eq_ignore_ascii_case("pg_typeof");
                    }
                }
                _ => {}
            }
        }
        false
    };
    let before = sql[..start].trim_end();
    if OPERATORS.iter().any(|op| before.strip_suffix(op).is_some_and(ends_in_the_call)) {
        return true;
    }
    let after = sql[end..].trim_start();
    if OPERATORS.iter().any(|op| {
        after.strip_prefix(op).is_some_and(|rest| {
            let rest = rest.trim_start();
            // By bytes: `rest` may open with a character wider than one.
            rest.as_bytes().get(..9).is_some_and(|word| word.eq_ignore_ascii_case(b"pg_typeof"))
                && rest[9..].trim_start().starts_with('(')
        })
    }) {
        return true;
    }
    // An item of an `IN` list: the list's opening parenthesis, then `IN`.
    let mut depth = 0usize;
    for (at, byte) in before.bytes().enumerate().rev() {
        match byte {
            b')' => depth += 1,
            b'(' if depth > 0 => depth -= 1,
            b'(' => {
                let head = before[..at].trim_end();
                let Some(head) = strip_word(head, "IN") else {
                    return false;
                };
                return ends_in_the_call(strip_word(head, "NOT").unwrap_or(head));
            }
            _ => {}
        }
    }
    false
}

/// `text` without the keyword `word` it ends in.
fn strip_word<'a>(text: &'a str, word: &str) -> Option<&'a str> {
    let text = text.trim_end();
    let at = text.len().checked_sub(word.len())?;
    (text.is_char_boundary(at)
        && text[at..].eq_ignore_ascii_case(word)
        && !text[..at].bytes().last().is_some_and(is_identifier_byte))
    .then(|| text[..at].trim_end())
}

/// `CAST('x' AS regclass)` around the literal at `lit_start..lit_end`: the
/// type, where the call ends, and where it begins.
fn cast_call(sql: &str, lit_start: usize, lit_end: usize) -> Option<(String, usize, usize)> {
    let before = sql[..lit_start].trim_end();
    let open = before.len().checked_sub(1)?;
    if !before.ends_with('(') {
        return None;
    }
    let word_end = sql[..open].trim_end().len();
    let word_start = word_end.checked_sub(4)?;
    if !sql.as_bytes()[word_start..word_end].eq_ignore_ascii_case(b"CAST") || (word_start > 0 && is_identifier_byte(sql.as_bytes()[word_start - 1])) {
        return None;
    }
    let close = matching_close(sql, open)?;
    let rest = sql[lit_end..close].trim();
    let ty = rest.strip_prefix("AS ").or_else(|| rest.strip_prefix("as "))?.trim();
    if !ty.bytes().all(is_identifier_byte) {
        return None;
    }
    Some((ty.to_ascii_lowercase(), close + 1, word_start))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(sql: &str) -> Option<String> {
        rewrite(sql, &|name| name == "items" || name == "Items")
    }

    #[test]
    fn a_shown_cast_is_the_name_a_compared_one_the_oid() {
        let oid = crate::core::rql::catalog_model::oid_of("items");
        assert_eq!(r("SELECT 'items'::regclass").as_deref(), Some("SELECT 'items' AS regclass"));
        assert_eq!(r("SELECT 'public.ITEMS'::regclass::text, 1").as_deref(), Some("SELECT 'items' AS text, 1"));
        assert_eq!(r("SELECT '\"Items\"'::regclass").as_deref(), Some("SELECT '\"Items\"' AS regclass"));
        assert_eq!(r("SELECT 'items'::regclass::oid").as_deref(), Some(format!("SELECT {oid} AS oid").as_str()));
        assert_eq!(
            r("SELECT 1 FROM t WHERE rel = 'items'::regclass").as_deref(),
            Some(format!("SELECT 1 FROM t WHERE rel = {oid}").as_str())
        );
        assert_eq!(r("SELECT CAST('items' AS regclass)").as_deref(), Some("SELECT 'items' AS regclass"));
        assert_eq!(r("SELECT 'int4'::regtype, 'integer'::regtype::oid").as_deref(), Some("SELECT 'integer' AS regtype, 23 AS oid"));
    }

    #[test]
    fn to_regclass_is_the_name_or_null() {
        assert_eq!(r("SELECT to_regclass('items'), to_regclass('nosuch')").as_deref(), Some("SELECT 'items', NULL"));
    }

    #[test]
    fn calls_and_ddl_that_read_the_literal_are_left() {
        assert_eq!(r("SELECT nextval('s'::regclass)"), None);
        assert_eq!(r("SELECT pg_total_relation_size('items'::regclass)"), None);
        assert_eq!(r("CREATE TABLE t (id int DEFAULT nextval('t_id_seq'::regclass))"), None);
        assert_eq!(r("SELECT 'x::regclass'"), None);
        assert_eq!(r("SELECT 'other.items'::regclass"), None);
    }
}

#[cfg(test)]
mod pg_typeof_tests {
    use super::rewrite;

    /// Compared with `pg_typeof(…)`, a regtype is the name that call
    /// answers; anywhere else compared, the oid it was.
    #[test]
    fn a_regtype_beside_pg_typeof_is_the_type_name() {
        let r = |sql: &str| rewrite(sql, &|_| true).unwrap_or_else(|| sql.to_string());
        assert_eq!(r("SELECT 1 WHERE pg_typeof(d) = 'date'::regtype"), "SELECT 1 WHERE pg_typeof(d) = 'date'");
        assert_eq!(r("SELECT 1 WHERE 'int4'::regtype <> PG_TYPEOF (n + (1))"), "SELECT 1 WHERE 'integer' <> PG_TYPEOF (n + (1))");
        assert_eq!(
            r("SELECT 1 WHERE pg_typeof(n) NOT IN ('bigint'::regtype, 'int4'::regtype)"),
            "SELECT 1 WHERE pg_typeof(n) NOT IN ('bigint', 'integer')"
        );
        assert_eq!(r("SELECT 1 WHERE typ = 'date'::regtype"), "SELECT 1 WHERE typ = 1082");
        assert_eq!(r("SELECT 1 WHERE x IN ('date'::regtype)"), "SELECT 1 WHERE x IN (1082)");
        // A character wider than a byte right after the operator.
        assert_eq!(r("SELECT 1 WHERE 'date'::regtype = é"), "SELECT 1 WHERE 1082 = é");
    }
}
