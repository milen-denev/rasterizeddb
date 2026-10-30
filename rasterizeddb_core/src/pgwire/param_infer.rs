//! The types of a prepared statement's parameters, for `ParameterDescription`.
//!
//! A client that declares no parameter types at Parse — tokio-postgres, and
//! anything else that leaves the OIDs at 0 — learns them from the statement
//! describe, and then encodes each value as the type it was told. Answering
//! with the declared OIDs (none) told such a client the statement took no
//! parameters at all, so it refused to bind any. PostgreSQL infers each
//! unspecified parameter's type from where it stands; this does the same for
//! the contexts that carry nearly every bound value:
//!
//! * a comparison with a column — `col <op> $n`, `$n <op> col`, `col LIKE $n`,
//!   `col IN ($1, $2)`, `col BETWEEN $1 AND $2` — and so `UPDATE … SET col = $n`;
//! * an `INSERT`'s `VALUES` tuples, by position against its column list;
//! * `LIMIT` / `OFFSET`: `int8`;
//! * a cast on the placeholder, `$n::type`: that type.
//!
//! A declared OID always wins. Anything else is `text`, which is what
//! PostgreSQL resolves an unknown-typed parameter to.
//!
//! Only types the Bind path can decode in **binary** are ever inferred: a
//! client that is told a type sends that type's binary form, and a type the
//! parameter decoder had no arm for would turn a working text bind into a
//! refused one. Anything outside [`decodable`] is reported as `text`, which
//! the client can always send.

use std::collections::HashMap;

use crate::core::database::Database;
use crate::core::db_type::DbType;
use crate::core::sql_scan;

const TEXT: i32 = 25;
const INT8: i32 = 20;

/// Types the Bind path decodes in binary (`pg_params::binary_param_literal`)
/// into a literal the column takes.
fn decodable(oid: i32) -> bool {
    matches!(
        oid,
        16 | 17 | 20 | 21 | 23 | 25 | 114 | 700 | 701 | 1042 | 1043 | 1114 | 1184 | 1700 | 2950 | 3802
            | 1000 | 1005 | 1007 | 1009 | 1016 | 1021 | 1022 | 1231 | 2951
            // Date and time, and the types stored in PostgreSQL's wire layout.
            | 1082 | 1083 | 1266 | 1186 | 869 | 650 | 829 | 774 | 790 | 1560 | 1562
            | 600 | 601 | 602 | 603 | 604 | 628 | 718
            | 3904 | 3906 | 3908 | 3910 | 3912 | 3926
    )
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// A name, possibly qualified: `(qualifier, name)`, unquoted as written.
    Ident(Option<String>, String),
    Param(usize),
    Op(String),
    Cast,
    Open,
    Close,
    Comma,
    Other,
}

impl Tok {
    fn keyword(&self, kw: &str) -> bool {
        matches!(self, Tok::Ident(None, n) if n.eq_ignore_ascii_case(kw))
    }
}

fn unquote_ident(raw: &str) -> String {
    match raw.strip_prefix('"').and_then(|r| r.strip_suffix('"')) {
        Some(inner) => inner.replace("\"\"", "\""),
        None => raw.to_string(),
    }
}

fn tokenize(sql: &str) -> Vec<Tok> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if let Some(end) = sql_scan::skip_comment(bytes, i) {
            i = end;
            continue;
        }
        if c == b'$' && bytes.get(i + 1).is_some_and(u8::is_ascii_digit) {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            out.push(Tok::Param(sql[i + 1..j].parse().unwrap_or(0)));
            i = j;
            continue;
        }
        if c == b'\'' || (c == b'$' && sql_scan::dollar_tag_len(bytes, i).is_some()) {
            i = sql_scan::skip_quoted(bytes, i).unwrap_or(bytes.len());
            out.push(Tok::Other);
            continue;
        }
        if c == b'"' || c == b'_' || c.is_ascii_alphabetic() || c >= 0x80 {
            // A name, with its qualifier: `a.b`, `"A"."b"`.
            let mut parts: Vec<String> = Vec::new();
            loop {
                let start = i;
                if bytes.get(i) == Some(&b'"') {
                    i = sql_scan::quoted_end(bytes, i, b'"');
                } else {
                    while i < bytes.len() && (sql_scan::is_identifier_byte(bytes[i]) || bytes[i] == b'$') {
                        i += 1;
                    }
                }
                if i == start {
                    break;
                }
                parts.push(unquote_ident(&sql[start..i]));
                if bytes.get(i) == Some(&b'.')
                    && bytes
                        .get(i + 1)
                        .is_some_and(|b| *b == b'"' || *b == b'_' || b.is_ascii_alphabetic() || *b >= 0x80)
                {
                    i += 1;
                    continue;
                }
                break;
            }
            let name = parts.pop().unwrap_or_default();
            out.push(Tok::Ident(parts.pop(), name));
            continue;
        }
        match c {
            b'(' => out.push(Tok::Open),
            b')' => out.push(Tok::Close),
            b',' => out.push(Tok::Comma),
            b':' if bytes.get(i + 1) == Some(&b':') => {
                out.push(Tok::Cast);
                i += 2;
                continue;
            }
            b'<' | b'>' | b'=' | b'!' | b'~' | b'+' | b'-' | b'*' | b'/' | b'%' | b'|' | b'&' => {
                let start = i;
                while i < bytes.len()
                    && matches!(bytes[i], b'<' | b'>' | b'=' | b'!' | b'~' | b'+' | b'-' | b'*' | b'/' | b'%' | b'|' | b'&')
                {
                    i += 1;
                }
                out.push(Tok::Op(sql[start..i].to_string()));
                continue;
            }
            _ => out.push(Tok::Other),
        }
        i += 1;
    }
    out
}

/// For each parameter `$1 … $count`, whether every place it stands in is
/// the whole of a value: a slot of a list or tuple, one side of a
/// comparison or of a `SET` assignment, or a side of `||` (whose operands
/// PostgreSQL resolves as text). A parameter that is an operand of any other
/// expression there (`$1 + 1`, `-$1`) is not: its type is the expression's
/// to decide, so it is not typed by the column it lands in.
pub(super) fn whole_value_params(sql: &str, count: usize) -> Vec<bool> {
    let toks = tokenize(sql);
    let mut whole = vec![true; count];
    let comparison = |t: &Tok| matches!(t, Tok::Op(op) if matches!(op.as_str(), "=" | "<>" | "!=" | "<" | ">" | "<=" | ">=" | "||"));
    for (i, t) in toks.iter().enumerate() {
        let Tok::Param(n) = t else { continue };
        let before = i.checked_sub(1).and_then(|j| toks.get(j));
        let after = toks.get(i + 1);
        let opens = matches!(before, None | Some(Tok::Open | Tok::Comma | Tok::Ident(..))) || before.is_some_and(comparison);
        let closes = matches!(after, None | Some(Tok::Close | Tok::Comma | Tok::Ident(..) | Tok::Cast)) || after.is_some_and(comparison);
        if !(opens && closes) {
            if let Some(w) = n.checked_sub(1).and_then(|k| whole.get_mut(k)) {
                *w = false;
            }
        }
    }
    whole
}

/// The OID a cast's type name stands for, `None` when it is not one of these.
fn cast_oid(name: &str, array: bool) -> Option<i32> {
    let base = match name.to_ascii_lowercase().as_str() {
        "int" | "int4" | "integer" => 23,
        "int8" | "bigint" => 20,
        "int2" | "smallint" => 21,
        "text" | "varchar" | "char" | "bpchar" | "name" => 25,
        "bool" | "boolean" => 16,
        "uuid" => 2950,
        "numeric" | "decimal" => 1700,
        "float8" => 701,
        "float4" | "real" => 700,
        "timestamptz" => 1184,
        "timestamp" => 1114,
        "json" => 114,
        "jsonb" => 3802,
        "bytea" => 17,
        "date" => 1082,
        "time" => 1083,
        "timetz" => 1266,
        "interval" => 1186,
        "inet" => 869,
        "cidr" => 650,
        "macaddr" => 829,
        "macaddr8" => 774,
        "money" => 790,
        "bit" => 1560,
        "varbit" => 1562,
        "point" => 600,
        "lseg" => 601,
        "path" => 602,
        "box" => 603,
        "polygon" => 604,
        "line" => 628,
        "circle" => 718,
        "int4range" => 3904,
        "int8range" => 3926,
        "numrange" => 3906,
        "tsrange" => 3908,
        "tstzrange" => 3910,
        "daterange" => 3912,
        _ => return None,
    };
    if !array {
        return Some(base);
    }
    Some(match base {
        23 => 1007,
        20 => 1016,
        21 => 1005,
        25 => 1009,
        16 => 1000,
        2950 => 2951,
        1700 => 1231,
        701 => 1022,
        700 => 1021,
        _ => return None,
    })
}

fn is_comparison(t: &Tok) -> bool {
    match t {
        Tok::Op(op) => matches!(
            op.as_str(),
            "=" | "<>" | "!=" | "<" | ">" | "<=" | ">=" | "+" | "-" | "*" | "/" | "%" | "~~" | "!~~"
        ),
        other => other.keyword("LIKE") || other.keyword("ILIKE"),
    }
}

const RESERVED: &[&str] = &[
    "WHERE", "SET", "VALUES", "ON", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "GROUP",
    "ORDER", "LIMIT", "OFFSET", "RETURNING", "USING", "AS", "SELECT", "FROM", "AND", "OR", "NOT",
    "DEFAULT", "UNION", "HAVING", "FOR", "OUTER", "NATURAL",
];

struct Relations {
    /// Each relation by its name and by its alias.
    by_name: HashMap<String, String>,
    /// The tables, in the order they appear.
    tables: Vec<String>,
}

fn relations(toks: &[Tok]) -> Relations {
    let mut rel = Relations {
        by_name: HashMap::new(),
        tables: Vec::new(),
    };
    for (i, t) in toks.iter().enumerate() {
        let introduces = ["FROM", "JOIN", "UPDATE", "INTO", "USING"]
            .iter()
            .any(|kw| t.keyword(kw));
        if !introduces {
            continue;
        }
        let Some(Tok::Ident(_, table)) = toks.get(i + 1) else {
            continue;
        };
        rel.tables.push(table.clone());
        rel.by_name.insert(table.to_ascii_lowercase(), table.clone());
        let mut k = i + 2;
        if toks.get(k).is_some_and(|t| t.keyword("AS")) {
            k += 1;
        }
        if let Some(Tok::Ident(None, alias)) = toks.get(k) {
            if !RESERVED.iter().any(|r| alias.eq_ignore_ascii_case(r)) {
                rel.by_name.insert(alias.to_ascii_lowercase(), table.clone());
            }
        }
    }
    rel
}

/// The OID a column is advertised as, if `(qualifier, name)` names one.
fn column_oid(database: &Database, rel: &Relations, qualifier: Option<&str>, name: &str) -> Option<i32> {
    let candidates: Vec<String> = match qualifier {
        Some(q) => vec![rel.by_name.get(&q.to_ascii_lowercase())?.clone()],
        None => rel.tables.clone(),
    };
    for table in candidates {
        let Some(t) = database.schema_view(&table) else {
            continue;
        };
        let schema = t.schema();
        if let Some(field) = schema.fields.iter().find(|f| {
            !f.is_deleted
                && !crate::core::rql::nullable::is_hidden(&f.name)
                && f.name.eq_ignore_ascii_case(name)
        }) {
            let ty: DbType = super::reported_type_of(field, &schema.json_columns);
            return Some(ty.pg_oid() as i32);
        }
    }
    None
}

fn column_at(database: &Database, rel: &Relations, t: Option<&Tok>) -> Option<i32> {
    match t? {
        Tok::Ident(q, name) => column_oid(database, rel, q.as_deref(), name),
        _ => None,
    }
}

/// One OID per parameter `$1 … $N`, `N` being the larger of the highest
/// placeholder and the number declared. See the module documentation.
pub(super) fn infer(database: &Database, sql: &str, declared: &[i32]) -> Vec<i32> {
    infer_where_placed(database, sql, declared)
        .into_iter()
        .map(|o| o.unwrap_or(TEXT))
        .collect()
}

/// [`infer`] without its fallback: `None` for a parameter whose position
/// says nothing (an operand of arithmetic, a function argument), where
/// [`infer`] answers `text`.
pub(super) fn infer_where_placed(database: &Database, sql: &str, declared: &[i32]) -> Vec<Option<i32>> {
    let toks = tokenize(sql);
    let highest = toks
        .iter()
        .filter_map(|t| match t {
            Tok::Param(n) => Some(*n),
            _ => None,
        })
        .max()
        .unwrap_or(0);
    let count = highest.max(declared.len());
    let mut oids: Vec<Option<i32>> = (0..count)
        .map(|i| declared.get(i).copied().filter(|o| *o != 0))
        .collect();
    if oids.iter().all(Option::is_some) {
        return oids;
    }

    let rel = relations(&toks);
    let insert_columns = insert_columns(database, &toks);

    let set = |n: usize, oid: Option<i32>, oids: &mut Vec<Option<i32>>| {
        if n == 0 || n > oids.len() || oids[n - 1].is_some() {
            return;
        }
        if let Some(oid) = oid {
            oids[n - 1] = Some(if decodable(oid) { oid } else { TEXT });
        }
    };

    // INSERT … (cols) VALUES (…), (…): by position at the top of each tuple,
    // or alone in parentheses there (`(($1))`).
    if let Some((columns, values_at)) = &insert_columns {
        let mut depth = 0usize;
        let mut position = 0usize;
        // The slot's one parameter, while it holds nothing but it and
        // parentheses; `Err` once it holds anything else.
        let mut alone: Result<Option<usize>, ()> = Ok(None);
        let place = |alone: Result<Option<usize>, ()>, position: usize, oids: &mut Vec<Option<i32>>| {
            if let Ok(Some(n)) = alone {
                let oid = columns.get(position).and_then(|c| column_oid(database, &rel, None, c));
                set(n, oid, oids);
            }
        };
        for t in &toks[*values_at..] {
            match t {
                Tok::Open => {
                    depth += 1;
                    if depth == 1 {
                        position = 0;
                        alone = Ok(None);
                    }
                }
                Tok::Close => {
                    if depth == 1 {
                        place(alone, position, &mut oids);
                        alone = Err(());
                    }
                    depth = depth.saturating_sub(1);
                }
                Tok::Comma if depth == 1 => {
                    place(alone, position, &mut oids);
                    alone = Ok(None);
                    position += 1;
                }
                Tok::Param(n) if depth == 1 => {
                    let oid = columns
                        .get(position)
                        .and_then(|c| column_oid(database, &rel, None, c));
                    set(*n, oid, &mut oids);
                    alone = Err(());
                }
                Tok::Param(n) if depth > 1 => {
                    alone = match alone {
                        Ok(None) => Ok(Some(*n)),
                        _ => Err(()),
                    };
                }
                t if depth == 0 && (t.keyword("RETURNING") || t.keyword("ON")) => break,
                _ => {
                    if depth >= 1 {
                        alone = Err(());
                    }
                }
            }
        }
    }

    for (i, t) in toks.iter().enumerate() {
        let Tok::Param(n) = t else { continue };
        let n = *n;
        let prev = |k: usize| i.checked_sub(k).and_then(|j| toks.get(j));

        // `$n::type` — the cast is what the client asked for.
        if toks.get(i + 1) == Some(&Tok::Cast) {
            if let Some(Tok::Ident(_, ty)) = toks.get(i + 2) {
                let array = matches!(toks.get(i + 3), Some(Tok::Other));
                set(n, cast_oid(ty, array), &mut oids);
                continue;
            }
        }
        // LIMIT / OFFSET.
        if prev(1).is_some_and(|p| p.keyword("LIMIT") || p.keyword("OFFSET")) {
            set(n, Some(INT8), &mut oids);
            continue;
        }
        // A parameter alone in parentheses stands where they do:
        // `SET v = ($1)`, `WHERE v = (($1))` (not a call's argument, `f($1)`).
        let (mut lo, mut hi) = (i, i);
        while lo > 0
            && toks.get(lo - 1) == Some(&Tok::Open)
            && toks.get(hi + 1) == Some(&Tok::Close)
            && !(lo >= 2 && matches!(toks[lo - 2], Tok::Ident(..)) && !is_comparison(&toks[lo - 2]))
        {
            lo -= 1;
            hi += 1;
        }
        let before = |k: usize| lo.checked_sub(k).and_then(|j| toks.get(j));
        // `col <op> $n`, `col NOT LIKE $n`.
        if before(1).is_some_and(is_comparison) {
            let col = if before(2).is_some_and(|p| p.keyword("NOT")) { before(3) } else { before(2) };
            if let Some(oid) = column_at(database, &rel, col) {
                set(n, Some(oid), &mut oids);
                continue;
            }
        }
        // `$n <op> col`.
        if toks.get(hi + 1).is_some_and(is_comparison) {
            if let Some(oid) = column_at(database, &rel, toks.get(hi + 2)) {
                set(n, Some(oid), &mut oids);
                continue;
            }
        }
        // `col BETWEEN $1 AND $2`.
        let between_col = if prev(1).is_some_and(|p| p.keyword("BETWEEN")) {
            if prev(2).is_some_and(|p| p.keyword("NOT")) { prev(3) } else { prev(2) }
        } else if prev(1).is_some_and(|p| p.keyword("AND")) && prev(3).is_some_and(|p| p.keyword("BETWEEN")) {
            if prev(4).is_some_and(|p| p.keyword("NOT")) { prev(5) } else { prev(4) }
        } else {
            None
        };
        if let Some(oid) = column_at(database, &rel, between_col) {
            set(n, Some(oid), &mut oids);
            continue;
        }
        // `col [NOT] IN (…, $n, …)`: back over the list to its parenthesis.
        let mut k = i;
        while k > 0 && matches!(toks[k - 1], Tok::Param(_) | Tok::Comma | Tok::Other) {
            k -= 1;
        }
        if k >= 2 && toks[k - 1] == Tok::Open && toks[k - 2].keyword("IN") {
            let col = if k >= 4 && toks[k - 3].keyword("NOT") { toks.get(k - 4) } else { toks.get(k - 3) };
            if let Some(oid) = column_at(database, &rel, col) {
                set(n, Some(oid), &mut oids);
            }
        }
    }

    oids
}

/// For `INSERT INTO t (a, b) VALUES …`: the column names, and the index of the
/// `VALUES` token.
///
/// `INSERT INTO t VALUES …` names no columns, and its tuple fills the table's
/// writable columns in order, so those are the names. The order and the
/// filter are the ones the executor uses when it supplies the list
/// (`supply_insert_column_list`): left out here, every parameter of such an
/// insert was described as `text`, and a client that encodes by the described
/// type refused to bind an integer to it.
fn insert_columns(database: &Database, toks: &[Tok]) -> Option<(Vec<String>, usize)> {
    if !toks.first()?.keyword("INSERT") {
        return None;
    }
    let into = toks.iter().position(|t| t.keyword("INTO"))?;
    let values = toks.iter().position(|t| t.keyword("VALUES"))?;
    if values == into + 2 {
        let Some(Tok::Ident(_, table)) = toks.get(into + 1) else {
            return None;
        };
        let entry = database.schema_view(table)?;
        let schema = entry.schema();
        let mut fields: Vec<_> = schema
            .fields
            .iter()
            .filter(|f| {
                !f.is_deleted
                    && !crate::core::rql::nullable::is_hidden(&f.name)
                    && crate::core::rql::generated::expression_for(&f.name, &schema.generated).is_none()
            })
            .collect();
        fields.sort_by_key(|f| f.write_order);
        return Some((fields.iter().map(|f| f.name.clone()).collect(), values));
    }
    if toks.get(into + 2) != Some(&Tok::Open) {
        return None;
    }
    let mut columns = Vec::new();
    for t in &toks[into + 3..values] {
        match t {
            Tok::Ident(_, name) => columns.push(name.clone()),
            Tok::Close => break,
            _ => {}
        }
    }
    Some((columns, values))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_see_through_quotes_and_comments() {
        let toks = tokenize(r#"SELECT "a"."B" FROM t WHERE x = $1 -- $9
            AND y = 'it''s $2' AND z::text = $3::int8"#);
        assert!(toks.contains(&Tok::Ident(Some("a".into()), "B".into())));
        let params: Vec<usize> = toks
            .iter()
            .filter_map(|t| match t {
                Tok::Param(n) => Some(*n),
                _ => None,
            })
            .collect();
        assert_eq!(params, vec![1, 3]);
    }

    #[test]
    fn casts_name_their_type() {
        assert_eq!(cast_oid("BIGINT", false), Some(20));
        assert_eq!(cast_oid("uuid", true), Some(2951));
        assert_eq!(cast_oid("inet", false), Some(869));
        assert_eq!(cast_oid("geometry", false), None);
    }
}
