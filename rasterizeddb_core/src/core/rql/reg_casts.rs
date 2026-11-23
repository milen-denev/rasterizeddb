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
pub(crate) fn rewrite(
    sql: &str,
    relation_exists: &dyn Fn(&str) -> bool,
    user_type: &dyn Fn(&str) -> bool,
) -> Result<Option<String>, (&'static str, String)> {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("reg") {
        return Ok(None);
    }
    let head = lower.trim_start();
    if ["create", "alter", "comment", "drop"].iter().any(|k| head.starts_with(k)) {
        return Ok(None);
    }
    // `23::regtype`: an oid written as a number, read as the quoted form
    // below reads it. It was "cast to 'regtype' is not supported yet".
    let numbered = quote_numeric_operands(sql);
    let rewritten = rewrite_literals(numbered.as_deref().unwrap_or(sql), relation_exists, user_type)?.or(numbered);
    // What is left is a cast of something other than a literal.
    let named = column_regtype_casts(rewritten.as_deref().unwrap_or(sql))?;
    Ok(named.or(rewritten))
}

/// Why a `regtype` value cannot be an `ORDER BY` key of a grouped query or
/// a set operation: PostgreSQL sorts it by the type's oid, and a key of such
/// a query has to be in its result, which carries the name. Sorted by the
/// name it was a wrong order with no error.
fn regtype_order_refusal() -> (&'static str, String) {
    (
        "0A000",
        "ORDER BY a regtype value in a grouped query or a set operation is not supported: it sorts by the type's \
         oid, which the result does not carry; select the name as text (x::regtype::text) and sort by that"
            .to_string(),
    )
}

/// One `ORDER BY` clause: where its keys are written, and whether the query
/// it sorts is grouped or a set operation.
struct OrderByClause {
    keys: std::ops::Range<usize>,
    grouped: bool,
    /// The statement's own, not a subquery's, a window's or an aggregate's.
    top: bool,
}

/// Every `ORDER BY` of `sql`, at any depth: a query's, a window's
/// (`OVER (ORDER BY …)`) and an aggregate's (`array_agg(x ORDER BY y)`).
fn order_by_clauses(sql: &str) -> Vec<OrderByClause> {
    use crate::core::rql::join::find_keyword;
    const ENDS: [&str; 10] = ["LIMIT", "OFFSET", "FETCH", "FOR", "UNION", "INTERSECT", "EXCEPT", "ROWS", "RANGE", "GROUPS"];
    let bytes = sql.as_bytes();
    // Open parentheses, and the clauses still open: (depth, keys start).
    let mut parens: Vec<usize> = Vec::new();
    let mut open: Vec<(usize, usize)> = Vec::new();
    // (depth, the parenthesis the clause is in, keys).
    let mut found: Vec<(usize, Option<usize>, std::ops::Range<usize>)> = Vec::new();
    let close_at = |open: &mut Vec<(usize, usize)>, found: &mut Vec<(usize, Option<usize>, std::ops::Range<usize>)>, parens: &[usize], at: usize| {
        while open.last().is_some_and(|(depth, _)| *depth == parens.len()) {
            let (depth, start) = open.pop().expect("checked");
            found.push((depth, parens.last().copied(), start..at));
        }
    };
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        match bytes[i] {
            b'(' => parens.push(i),
            b')' => {
                close_at(&mut open, &mut found, &parens, i);
                parens.pop();
            }
            b';' => close_at(&mut open, &mut found, &parens, i),
            b if is_identifier_byte(b) && (i == 0 || !is_identifier_byte(bytes[i - 1])) => {
                let end = i + bytes[i..].iter().take_while(|b| is_identifier_byte(**b)).count();
                let word = &sql[i..end];
                if word.eq_ignore_ascii_case("ORDER") {
                    let rest = sql[end..].trim_start();
                    if crate::core::sql_scan::starts_with_keyword(rest, "BY") {
                        open.push((parens.len(), sql.len() - rest.len() + 2));
                    }
                } else if ENDS.iter().any(|k| word.eq_ignore_ascii_case(k)) {
                    close_at(&mut open, &mut found, &parens, i);
                }
                i = end;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    while let Some((depth, start)) = open.pop() {
        found.push((depth, None, start..sql.len()));
    }
    found
        .into_iter()
        .map(|(depth, paren, keys)| {
            // The query the clause sorts: what its parenthesis holds, or the
            // whole statement.
            let region = match paren {
                Some(at) if depth > 0 => &sql[at + 1..matching_close(sql, at).unwrap_or(sql.len())],
                _ => sql,
            };
            let grouped = ["GROUP", "UNION", "INTERSECT", "EXCEPT"].iter().any(|kw| find_keyword(region, kw, 0).is_some());
            OrderByClause { keys, grouped, top: depth == 0 }
        })
        .collect()
}

/// `sql` with each key of its own `ORDER BY` that names a `regtype` item of
/// its select list, by position (`ORDER BY 1`) or by alias (`ORDER BY n`),
/// written as the column that item casts: the oid, which is what PostgreSQL
/// sorts a `regtype` by. `Ok(None)` when it has none; refused in a grouped
/// query or a set operation ([`regtype_order_refusal`]).
fn regtype_order_keys(sql: &str) -> Result<Option<String>, (&'static str, String)> {
    use crate::core::rql::join::find_keyword;
    use crate::core::sql_scan::{split_top_level, starts_with_keyword};
    let Some(select) = find_keyword(sql, "SELECT", 0) else {
        return Ok(None);
    };
    let Some(order) = order_by_clauses(sql).into_iter().filter(|c| c.top && c.keys.start > select).last() else {
        return Ok(None);
    };
    let list_start = select + "SELECT".len();
    let list_end = find_keyword(sql, "FROM", list_start).filter(|at| *at < order.keys.start).unwrap_or(order.keys.start);
    // The select list's regtype items: position, the column cast, its name.
    let mut items: Vec<(usize, &str, &str)> = Vec::new();
    for (at, item) in split_top_level(&sql[list_start..list_end], b',').into_iter().enumerate() {
        let item = if at == 0 && starts_with_keyword(item, "DISTINCT") { item[8..].trim_start() } else { item };
        let Some((operand, rest)) = item.split_once("::") else {
            continue;
        };
        let operand = operand.trim_end();
        let name = operand.bytes().all(|b| is_identifier_byte(b) || b == b'.' || b == b'"')
            && operand.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_' || c == '"');
        let rest = rest.trim_start();
        if !name || !starts_with_keyword(rest, "regtype") {
            continue;
        }
        let tail = rest["regtype".len()..].trim();
        if tail.starts_with("::") {
            continue;
        }
        let alias = if starts_with_keyword(tail, "AS") { tail[2..].trim() } else { tail };
        let alias = if alias.is_empty() { operand.rsplit('.').next().unwrap_or(operand) } else { alias };
        items.push((at + 1, operand, alias));
    }
    if items.is_empty() {
        return Ok(None);
    }
    let mut edits: Vec<(std::ops::Range<usize>, &str)> = Vec::new();
    for key in split_top_level(&sql[order.keys.clone()], b',') {
        // The key without its direction.
        let mut core = key.trim_end();
        while let Some(at) = core.rfind(|c: char| c.is_ascii_whitespace()) {
            let word = &core[at + 1..];
            if ["ASC", "DESC", "NULLS", "FIRST", "LAST"].iter().any(|w| word.eq_ignore_ascii_case(w)) {
                core = core[..at].trim_end();
            } else {
                break;
            }
        }
        let item = match core.parse::<usize>() {
            Ok(position) => items.iter().find(|(at, _, _)| *at == position),
            Err(_) => items.iter().find(|(_, _, alias)| alias.eq_ignore_ascii_case(core)),
        };
        let Some((_, operand, _)) = item else {
            continue;
        };
        if order.grouped {
            return Err(regtype_order_refusal());
        }
        let start = core.as_ptr() as usize - sql.as_ptr() as usize;
        edits.push((start..start + core.len(), operand));
    }
    if edits.is_empty() {
        return Ok(None);
    }
    let mut out = String::with_capacity(sql.len() + 16);
    let mut copied = 0usize;
    for (range, operand) in edits {
        out.push_str(&sql[copied..range.start]);
        out.push_str(operand);
        copied = range.end;
    }
    out.push_str(&sql[copied..]);
    Ok(Some(out))
}

/// `sql` with each `column::regtype` as the type's name, `None` when it has
/// none: `atttypid::regtype`, `t.oid_col::regtype::text`. With `::oid` after
/// it the value is the column itself. It was "cast to 'regtype' is not
/// supported yet" outside a catalogue statement. A whole select item keeps
/// the column's name, as PostgreSQL names a cast column.
fn column_regtype_casts(sql: &str) -> Result<Option<String>, (&'static str, String)> {
    const CAST: &str = "regtype";
    if !crate::core::sql_scan::contains_ignore_ascii_case(sql, CAST.as_bytes()) {
        return Ok(None);
    }
    // A key that names a regtype item by position or alias, as the column.
    let keyed = regtype_order_keys(sql)?;
    let sql = keyed.as_deref().unwrap_or(sql);
    let order_by = order_by_clauses(sql);
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 32);
    let (mut copied, mut i) = (0usize, 0usize);
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        // By bytes: `i` may be inside a character wider than one.
        if !bytes[i..].starts_with(b"::") {
            i += 1;
            continue;
        }
        let after = sql[i + 2..].trim_start();
        if !crate::core::sql_scan::starts_with_keyword(after, CAST) {
            i += 2;
            continue;
        }
        let cast_end = sql.len() - after.len() + CAST.len();
        // The operand: a name, qualified or quoted, right before the cast.
        let before = sql[..i].trim_end();
        let start = before
            .bytes()
            .rposition(|b| !(is_identifier_byte(b) || b == b'.' || b == b'"'))
            .map_or(0, |p| p + 1);
        let operand = &before[start..];
        let is_name = operand.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_' || c == '"')
            && !operand.ends_with('.')
            // A quoted name with a space in it is not read from its end.
            && operand.bytes().filter(|b| *b == b'"').count() % 2 == 0
            && start >= copied;
        if !is_name {
            i = cast_end;
            continue;
        }
        // `::oid` after it, or a cast to an integer (an earlier pass writes
        // `oid` as one): the column is the oid. So it is when it is compared
        // with another type, which the literal pass has made an oid too
        // (`t::regtype = 'integer'::regtype`).
        let next = sql[cast_end..].trim_start();
        let keyword = |text: &str, words: &[&str]| words.iter().any(|w| crate::core::sql_scan::starts_with_keyword(text, w));
        // The order of two regtypes is their oids' too (`<`, `>=`).
        let compared = ["=", "<", ">", "!="].iter().any(|op| next.starts_with(op) || before[..start].trim_end().ends_with(op))
            || keyword(next, &["IN"])
            || (keyword(next, &["NOT"]) && keyword(next[3..].trim_start(), &["IN"]));
        // An `ORDER BY` key, a query's, a window's or an aggregate's:
        // PostgreSQL sorts a regtype by its oid. As the name it sorted
        // `99999`, `boolean`, `integer`, where PostgreSQL has `boolean`
        // (16), `integer` (23), … `99999`.
        let sorted = order_by.iter().find(|clause| clause.keys.contains(&start));
        let (replacement, end) = match next.strip_prefix("::").map(str::trim_start) {
            Some(rest) if keyword(rest, &["oid"]) => (operand.to_string(), sql.len() - rest.len() + 3),
            Some(rest) if keyword(rest, &["bigint", "int8", "integer", "int4", "int"]) => (operand.to_string(), cast_end),
            Some(_) => (format!("__regtype_name({operand})"), cast_end),
            None if compared => (operand.to_string(), cast_end),
            None if sorted.is_some_and(|clause| clause.grouped) => return Err(regtype_order_refusal()),
            None if sorted.is_some() => (operand.to_string(), cast_end),
            None => (format!("__regtype_name({operand})"), cast_end),
        };
        let alias = if crate::core::sql_scan::whole_select_item(sql, start, end) {
            let name = operand.rsplit('.').next().unwrap_or(operand);
            format!(" AS {name}")
        } else {
            String::new()
        };
        out.push_str(&sql[copied..start]);
        out.push_str(&replacement);
        out.push_str(&alias);
        copied = end;
        i = end;
    }
    if copied == 0 {
        return Ok(keyed);
    }
    out.push_str(&sql[copied..]);
    Ok(Some(out))
}

/// `sql` with each bare integer cast to `regtype` or `regclass` quoted
/// (`23::regtype` as `'23'::regtype`), `None` when it has none.
fn quote_numeric_operands(sql: &str) -> Option<String> {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 8);
    let (mut copied, mut i) = (0usize, 0usize);
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        let starts = bytes[i].is_ascii_digit() && (i == 0 || !(is_identifier_byte(bytes[i - 1]) || bytes[i - 1] == b'.'));
        if !starts {
            i += 1;
            continue;
        }
        let end = i + bytes[i..].iter().take_while(|b| b.is_ascii_digit()).count();
        let names_reg = |c: &str| ["regtype", "regclass"].iter().any(|word| crate::core::sql_scan::starts_with_keyword(c, word));
        let after = sql[end..].trim_start();
        // `23::regtype`, or `CAST(23 AS regtype)`.
        let reg = after.strip_prefix("::").map(str::trim_start).is_some_and(names_reg)
            || (crate::core::sql_scan::starts_with_keyword(after, "AS")
                && names_reg(after[2..].trim_start())
                && sql[..i].trim_end().strip_suffix('(').is_some_and(|head| strip_word(head, "CAST").is_some()));
        if reg {
            out.push_str(&sql[copied..i]);
            out.push('\'');
            out.push_str(&sql[i..end]);
            out.push('\'');
            copied = end;
        }
        i = end.max(i + 1);
    }
    if copied == 0 {
        return None;
    }
    out.push_str(&sql[copied..]);
    Some(out)
}

fn rewrite_literals(
    sql: &str,
    relation_exists: &dyn Fn(&str) -> bool,
    user_type: &dyn Fn(&str) -> bool,
) -> Result<Option<String>, (&'static str, String)> {
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
            // An enum or a domain: its own name, or the oid it is listed
            // under. `user_type` answers for every name the database has
            // had, so a dropped type still reads as one here.
            let named = relation_name(&text).filter(|name| oid == 0 && user_type(name));
            // A name no type has: PostgreSQL's 42704. Left as written, it
            // was "cast to 'regtype' is not supported yet", naming the cast.
            if oid == 0 && named.is_none() {
                return Err(("42704", format!("type \"{}\" does not exist", text.trim())));
            }
            if let Some(name) = named {
                if shown { sql_literal(&regclass_text(&name)) } else { crate::core::rql::user_types::oid_of(&name).to_string() }
            } else if shown {
                // An oid no type has is shown as the number, as PostgreSQL
                // shows it.
                sql_literal(&crate::core::rql::catalog_model::format_type(oid, None).unwrap_or_else(|| oid.to_string()))
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
        return Ok(None);
    }
    let mut out = String::with_capacity(sql.len());
    let mut copied = 0;
    for (start, end, text) in edits {
        if start < copied {
            return Ok(None);
        }
        out.push_str(&sql[copied..start]);
        out.push_str(&text);
        copied = end;
    }
    out.push_str(&sql[copied..]);
    Ok(Some(out))
}

/// `pg_partition_tree('t'::regclass)` in a FROM list, as the rows it
/// answers here: a table's partitions are not relations of their own in this
/// engine, so every table, partitioned or not, is its own one leaf, which is
/// what PostgreSQL answers for a table with no partitions. `relid` is the
/// relation's oid, as a comparison with `pg_class.oid` needs it.
///
/// Before [`rewrite`], which would make the argument an oid and lose the
/// name: a relation that does not exist is 42P01, as the cast's failure is.
/// An argument that is no literal is left as written.
pub(crate) fn partition_tree(
    sql: &str,
    relation_exists: &dyn Fn(&str) -> bool,
) -> Result<Option<String>, (&'static str, String)> {
    const NAME: &[u8] = b"pg_partition_tree";
    if !crate::core::sql_scan::contains_ignore_ascii_case(sql, NAME) {
        return Ok(None);
    }
    // What may follow a FROM item that is no alias for it.
    const NOT_AN_ALIAS: &[&str] = &[
        "WHERE", "ON", "JOIN", "LEFT", "RIGHT", "INNER", "CROSS", "FULL", "NATURAL", "GROUP", "ORDER", "LIMIT", "OFFSET",
        "UNION", "INTERSECT", "EXCEPT", "HAVING", "WINDOW", "FETCH", "FOR", "USING", "LOOP", "RETURNING",
    ];
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 64);
    let (mut copied, mut i) = (0usize, 0usize);
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end.max(i + 1);
            continue;
        }
        let at_word = (i == 0 || !(is_identifier_byte(bytes[i - 1]) || bytes[i - 1] == b'.'))
            && bytes.len() >= i + NAME.len()
            && bytes[i..i + NAME.len()].eq_ignore_ascii_case(NAME);
        if !at_word {
            i += 1;
            continue;
        }
        let open = i + NAME.len() + (sql[i + NAME.len()..].len() - sql[i + NAME.len()..].trim_start().len());
        let Some(close) = (bytes.get(open) == Some(&b'(')).then(|| matching_close(sql, open)).flatten() else {
            i += NAME.len();
            continue;
        };
        // `'name'`, `'name'::regclass`, `CAST('name' AS regclass)`, or an oid.
        let argument = sql[open + 1..close].trim();
        let argument = argument
            .strip_prefix("CAST(")
            .or_else(|| argument.strip_prefix("cast("))
            .and_then(|inner| inner.strip_suffix(')'))
            .map(|inner| inner.rsplit_once(" AS ").or_else(|| inner.rsplit_once(" as ")).map_or(inner, |(v, _)| v).trim())
            .unwrap_or(argument);
        let oid = if let Ok(oid) = argument.parse::<i64>() {
            oid
        } else if argument.starts_with('\'') {
            let end = quoted_end(argument.as_bytes(), 0, b'\'');
            let rest = argument[end..].trim();
            if !(rest.is_empty() || rest.eq_ignore_ascii_case("::regclass")) {
                i = close + 1;
                continue;
            }
            let text = argument[1..end - 1].replace("''", "'");
            let name = relation_name(&text).filter(|name| relation_exists(name));
            let Some(name) = name else {
                return Err(("42P01", format!("relation \"{text}\" does not exist")));
            };
            crate::core::rql::catalog_model::oid_of(&name)
        } else {
            i = close + 1;
            continue;
        };
        let after = sql[close + 1..].trim_start();
        let word: String = after.bytes().take_while(|b| is_identifier_byte(*b)).map(|b| b as char).collect();
        let aliased = !word.is_empty() && !NOT_AN_ALIAS.iter().any(|k| word.eq_ignore_ascii_case(k));
        out.push_str(&sql[copied..i]);
        out.push_str(&format!(
            "(SELECT {oid} AS relid, CAST(NULL AS integer) AS parentrelid, TRUE AS isleaf, 0 AS level){}",
            if aliased { "" } else { " AS pg_partition_tree" }
        ));
        copied = close + 1;
        i = close + 1;
    }
    if copied == 0 {
        return Ok(None);
    }
    out.push_str(&sql[copied..]);
    Ok(Some(out))
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

    #[test]
    fn a_number_a_modifier_and_an_enum_are_read_and_an_unknown_name_refused() {
        assert_eq!(r("SELECT 23::regtype").as_deref(), Some("SELECT 'integer' AS regtype"));
        assert_eq!(r("SELECT 1082::regtype::text, 16 :: regtype::oid").as_deref(), Some("SELECT 'date' AS text, 16 AS oid"));
        assert_eq!(r("SELECT CAST(20 AS regtype)").as_deref(), Some("SELECT 'bigint' AS regtype"));
        assert_eq!(r("SELECT 'numeric(10,2)'::regtype").as_deref(), Some("SELECT 'numeric' AS regtype"));
        assert_eq!(
            r("SELECT 'timestamp(3) with time zone'::regtype").as_deref(),
            Some("SELECT 'timestamp with time zone' AS regtype")
        );
        // An oid no type has is shown as the number.
        assert_eq!(r("SELECT 99999::regtype").as_deref(), Some("SELECT '99999' AS regtype"));
        // An enum: its name shown, its own oid compared.
        assert_eq!(r("SELECT 'mood'::regtype").as_deref(), Some("SELECT 'mood' AS regtype"));
        let oid = crate::core::rql::user_types::oid_of("mood");
        assert_eq!(r("SELECT 1 WHERE t = 'public.mood'::regtype").as_deref(), Some(format!("SELECT 1 WHERE t = {oid}").as_str()));
        // Numbers that are not cast to one of the two are left alone.
        assert_eq!(r("SELECT 23::int, 1.5::regtype, '23::regtype' FROM regs"), None);
        // A column's cast is the type's name, under the column's own name.
        assert_eq!(
            r("SELECT x1::regtype, t.\"Ty\"::regtype::text, y::regtype::oid FROM regs t WHERE y::regtype = 'integer'").as_deref(),
            Some("SELECT __regtype_name(x1) AS x1, __regtype_name(t.\"Ty\")::text, y AS y FROM regs t WHERE y = 'integer'")
        );
        // Compared with a type, both sides are the oid.
        assert_eq!(
            r("SELECT 1 FROM regs WHERE y::regtype = 'integer'::regtype AND 'text'::regtype <> x1::regtype AND y::regtype IN (23, 25)").as_deref(),
            Some("SELECT 1 FROM regs WHERE y = 23 AND 25 <> x1 AND y IN (23, 25)")
        );
        assert_eq!(r("SELECT y::regtype::bigint FROM regs").as_deref(), Some("SELECT y::bigint FROM regs"));
        // Sorted, a regtype is its oid: as a key, by position, by alias, in
        // a window and in an aggregate; the name where it is shown.
        assert_eq!(
            r("SELECT id, y::regtype FROM regs ORDER BY y::regtype DESC NULLS LAST, id LIMIT 2").as_deref(),
            Some("SELECT id, __regtype_name(y) AS y FROM regs ORDER BY y DESC NULLS LAST, id LIMIT 2")
        );
        assert_eq!(
            r("SELECT t.y::regtype AS n, id FROM regs t ORDER BY 1 DESC, 2").as_deref(),
            Some("SELECT __regtype_name(t.y) AS n, id FROM regs t ORDER BY t.y DESC, 2")
        );
        assert_eq!(
            r("SELECT y::regtype AS n FROM regs ORDER BY N").as_deref(),
            Some("SELECT __regtype_name(y) AS n FROM regs ORDER BY y")
        );
        assert_eq!(
            r("SELECT rank() OVER (PARTITION BY g ORDER BY y::regtype), array_agg(id ORDER BY y::regtype) FROM regs").as_deref(),
            Some("SELECT rank() OVER (PARTITION BY g ORDER BY y), array_agg(id ORDER BY y) FROM regs")
        );
        assert_eq!(
            r("SELECT 1 FROM regs WHERE y::regtype < 'text'::regtype AND 'bool'::regtype <= y::regtype").as_deref(),
            Some("SELECT 1 FROM regs WHERE y < 25 AND 16 <= y")
        );
        // By its name where that is asked for, and other keys untouched.
        assert_eq!(
            r("SELECT y::regtype::text AS n, id FROM regs ORDER BY 1, id").as_deref(),
            Some("SELECT __regtype_name(y)::text AS n, id FROM regs ORDER BY 1, id")
        );
        assert_eq!(
            r("SELECT id, y::regtype FROM regs ORDER BY 1 LIMIT 3").as_deref(),
            Some("SELECT id, __regtype_name(y) AS y FROM regs ORDER BY 1 LIMIT 3")
        );
        // Grouped, or a set operation: refused rather than sorted by name.
        for sql in [
            "SELECT y::regtype, count(*) FROM regs GROUP BY y::regtype ORDER BY 1",
            "SELECT y::regtype AS n, count(*) FROM regs GROUP BY 1 ORDER BY n",
            "SELECT y::regtype, count(*) FROM regs GROUP BY y::regtype ORDER BY y::regtype",
            "SELECT y::regtype FROM regs UNION SELECT x1::regtype FROM regs ORDER BY 1",
        ] {
            assert_eq!(rewrite(sql, &|_| true, &|_| false).map_err(|(code, _)| code), Err("0A000"), "{sql}");
        }
        // A grouped query sorted by something else is not.
        assert!(rewrite("SELECT y::regtype, count(*) FROM regs GROUP BY y::regtype ORDER BY 2", &|_| true, &|_| false).is_ok());
        // Characters wider than a byte around the cast.
        assert_eq!(
            r("SELECT 'é', y::regtype AS \"ü\" FROM regs WHERE n = 'ß€' AND 7::regtype = 7").as_deref(),
            Some("SELECT 'é', __regtype_name(y) AS \"ü\" FROM regs WHERE n = 'ß€' AND 7 = 7")
        );
        assert_eq!(r("SELECT é::regtype::text, ünï FROM regs"), None);
        assert_eq!(r("SELECT (a + 1)::regtype, f(a)::regtype, \"my ty\"::regtype FROM regs"), None);
        let refused = rewrite("SELECT 'nosuchtype'::regtype", &|_| true, &|_| false);
        assert_eq!(refused, Err(("42704", "type \"nosuchtype\" does not exist".to_string())));
    }

    fn r(sql: &str) -> Option<String> {
        rewrite(sql, &|name| name == "items" || name == "Items", &|name| name == "mood").unwrap()
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
        let r = |sql: &str| rewrite(sql, &|_| true, &|_| false).unwrap().unwrap_or_else(|| sql.to_string());
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
