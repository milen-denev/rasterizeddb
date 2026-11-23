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
    /// A quoted literal, PostgreSQL's `unknown`.
    Str,
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
            let quoted = c == b'\'';
            i = sql_scan::skip_quoted(bytes, i).unwrap_or(bytes.len());
            out.push(if quoted { Tok::Str } else { Tok::Other });
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

/// The columns of a relation that exists only for one statement — a CTE, or
/// a derived table under its alias — as `(relation, [(column, oid)])`. They
/// are resolved by the describe path (`pgwire::param_scratch_relations`),
/// which types each body without running it.
pub(super) type Scratch = [(String, Vec<(String, i32)>)];

struct Relation {
    /// The table, CTE or derived table's name (an alias for a derived table).
    name: String,
    /// The parentheses the relation is introduced in, as token indices
    /// `(open, close)`; the whole statement for one at its top level.
    scope: (usize, usize),
}

struct Relations {
    /// Each relation by its name and by its alias.
    by_name: HashMap<String, String>,
    /// The relations, in the order they appear.
    tables: Vec<Relation>,
    /// For each token, the parentheses that enclose it.
    enclosing: Vec<(usize, usize)>,
}

impl Relations {
    /// The relations an unqualified column at token `at` may belong to, the
    /// nearest first: those introduced in the parentheses around it, the
    /// innermost first, and then every other one in the order written.
    ///
    /// In order of appearance alone, a CTE's body came first, so in `WITH c
    /// AS (SELECT count(*) AS id FROM t …) SELECT … FROM c WHERE id = $1` the
    /// parameter was typed by `t.id`, not by the `bigint` it is compared with.
    fn candidates(&self, at: Option<usize>) -> Vec<&str> {
        let Some(scope_of_column) = at.and_then(|k| self.enclosing.get(k)) else {
            return self.tables.iter().map(|r| r.name.as_str()).collect();
        };
        let contains = |r: &Relation| r.scope.0 <= scope_of_column.0 && scope_of_column.1 <= r.scope.1;
        let mut near: Vec<&Relation> = self.tables.iter().filter(|r| contains(r)).collect();
        near.sort_by_key(|r| r.scope.1 - r.scope.0);
        near.iter()
            .map(|r| r.name.as_str())
            .chain(self.tables.iter().filter(|r| !contains(r)).map(|r| r.name.as_str()))
            .collect()
    }
}

/// For each token, the `(open, close)` of the parentheses around it, or
/// `(0, len)` at the top level.
fn enclosing(toks: &[Tok]) -> Vec<(usize, usize)> {
    let top = (0usize, toks.len());
    let mut close_of = vec![toks.len(); toks.len()];
    let mut stack: Vec<usize> = Vec::new();
    for (k, t) in toks.iter().enumerate() {
        match t {
            Tok::Open => stack.push(k),
            Tok::Close => {
                if let Some(open) = stack.pop() {
                    close_of[open] = k;
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::with_capacity(toks.len());
    let mut stack: Vec<usize> = Vec::new();
    for (k, t) in toks.iter().enumerate() {
        if *t == Tok::Close {
            stack.pop();
        }
        out.push(stack.last().map_or(top, |&open| (open, close_of[open])));
        if *t == Tok::Open {
            stack.push(k);
        }
    }
    out
}

fn relations(toks: &[Tok]) -> Relations {
    let enclosing = enclosing(toks);
    let mut rel = Relations {
        by_name: HashMap::new(),
        tables: Vec::new(),
        enclosing,
    };
    for (i, t) in toks.iter().enumerate() {
        let introduces = ["FROM", "JOIN", "UPDATE", "INTO", "USING"]
            .iter()
            .any(|kw| t.keyword(kw));
        if !introduces {
            continue;
        }
        let scope = rel.enclosing[i];
        // `FROM (SELECT …) d`: the derived table goes by its alias.
        if toks.get(i + 1) == Some(&Tok::Open) {
            let Some(close) = close_after(toks, i + 1) else { continue };
            let mut k = close + 1;
            if toks.get(k).is_some_and(|t| t.keyword("AS")) {
                k += 1;
            }
            if let Some(Tok::Ident(None, alias)) = toks.get(k) {
                if !RESERVED.iter().any(|r| alias.eq_ignore_ascii_case(r)) {
                    rel.tables.push(Relation { name: alias.clone(), scope });
                    rel.by_name.insert(alias.to_ascii_lowercase(), alias.clone());
                }
            }
            continue;
        }
        let Some(Tok::Ident(_, table)) = toks.get(i + 1) else {
            continue;
        };
        rel.tables.push(Relation { name: table.clone(), scope });
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

/// The `)` matching the `(` at `open`.
fn close_after(toks: &[Tok], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (k, t) in toks.iter().enumerate().skip(open) {
        match t {
            Tok::Open => depth += 1,
            Tok::Close => {
                depth -= 1;
                if depth == 0 {
                    return Some(k);
                }
            }
            _ => {}
        }
    }
    None
}

/// The OID a column is advertised as, if `(qualifier, name)` names one.
/// `at` is the column's token, which decides the nearest relations for an
/// unqualified name. A statement's own relations (`scratch`) shadow tables
/// of the same name, as a CTE does.
fn column_oid(
    database: &Database,
    rel: &Relations,
    scratch: &Scratch,
    qualifier: Option<&str>,
    name: &str,
    at: Option<usize>,
) -> Option<i32> {
    let candidates: Vec<&str> = match qualifier {
        Some(q) => vec![rel.by_name.get(&q.to_ascii_lowercase())?.as_str()],
        None => rel.candidates(at),
    };
    for table in candidates {
        if let Some((_, columns)) = scratch.iter().find(|(n, _)| n.eq_ignore_ascii_case(table)) {
            match columns.iter().find(|(c, _)| c.eq_ignore_ascii_case(name)) {
                Some((_, oid)) => return Some(*oid),
                None => continue,
            }
        }
        let Some(t) = database.schema_view(table) else {
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

fn column_at(database: &Database, rel: &Relations, scratch: &Scratch, toks: &[Tok], at: Option<usize>) -> Option<i32> {
    match toks.get(at?)? {
        Tok::Ident(q, name) => column_oid(database, rel, scratch, q.as_deref(), name, at),
        _ => None,
    }
}

/// One OID per parameter `$1 … $N`, `N` being the larger of the highest
/// placeholder and the number declared. See the module documentation.
pub(super) fn infer(database: &Database, sql: &str, declared: &[i32], scratch: &Scratch) -> Vec<i32> {
    infer_placed(database, sql, declared, scratch)
        .into_iter()
        .map(|o| o.unwrap_or(TEXT))
        .collect()
}

/// [`infer`] without its fallback: `None` for a parameter whose position
/// says nothing (an operand of arithmetic, a function argument), where
/// [`infer`] answers `text`.
pub(super) fn infer_where_placed(database: &Database, sql: &str, declared: &[i32]) -> Vec<Option<i32>> {
    infer_placed(database, sql, declared, &[])
}

fn infer_placed(database: &Database, sql: &str, declared: &[i32], scratch: &Scratch) -> Vec<Option<i32>> {
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
                let oid = columns.get(position).and_then(|c| column_oid(database, &rel, scratch, None, c, None));
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
                        .and_then(|c| column_oid(database, &rel, scratch, None, c, None));
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
            let col = if before(2).is_some_and(|p| p.keyword("NOT")) { lo.checked_sub(3) } else { lo.checked_sub(2) };
            if let Some(oid) = column_at(database, &rel, scratch, &toks, col) {
                set(n, Some(oid), &mut oids);
                continue;
            }
            // `f(…) <op> $n` with `f` one that returns text (`md5(v) = $1`,
            // `lower(s) LIKE $1`): the parameter is text, as PostgreSQL
            // resolves an unknown beside text. Inlined as the number its
            // text looked like, it met the text as `text = integer` (42883).
            if before(2) == Some(&Tok::Close) && text_call_before(&toks, lo - 2) {
                set(n, Some(TEXT), &mut oids);
                continue;
            }
            // `'x' <op> $n`, `$m <op> $n`: an unknown beside an unknown is
            // text, as PostgreSQL resolves it. Inlined as a number, `'Abc'
            // >= $1` bound to -1 compared 'Abc' with an integer.
            if matches!(before(1), Some(Tok::Op(op)) if matches!(op.as_str(), "=" | "<>" | "!=" | "<" | ">" | "<=" | ">="))
                && matches!(before(2), Some(Tok::Str | Tok::Param(_)))
            {
                set(n, Some(TEXT), &mut oids);
                continue;
            }
        }
        // `$n <op> 'x'`, `$n <op> $m`, as above.
        if matches!(toks.get(hi + 1), Some(Tok::Op(op)) if matches!(op.as_str(), "=" | "<>" | "!=" | "<" | ">" | "<=" | ">="))
            && matches!(toks.get(hi + 2), Some(Tok::Str | Tok::Param(_)))
            && !toks.get(hi + 3).is_some_and(|t| t == &Tok::Cast)
        {
            set(n, Some(TEXT), &mut oids);
            continue;
        }
        // `$n <op> col`.
        if toks.get(hi + 1).is_some_and(is_comparison) {
            if let Some(oid) = column_at(database, &rel, scratch, &toks, Some(hi + 2)) {
                set(n, Some(oid), &mut oids);
                continue;
            }
        }
        // `col BETWEEN $1 AND $2`.
        let back = |k: usize| i.checked_sub(k);
        let between_col = if prev(1).is_some_and(|p| p.keyword("BETWEEN")) {
            if prev(2).is_some_and(|p| p.keyword("NOT")) { back(3) } else { back(2) }
        } else if prev(1).is_some_and(|p| p.keyword("AND")) && prev(3).is_some_and(|p| p.keyword("BETWEEN")) {
            if prev(4).is_some_and(|p| p.keyword("NOT")) { back(5) } else { back(4) }
        } else {
            None
        };
        if let Some(oid) = column_at(database, &rel, scratch, &toks, between_col) {
            set(n, Some(oid), &mut oids);
            continue;
        }
        // `col [NOT] IN (…, $n, …)`: back over the list to its parenthesis.
        let mut k = i;
        while k > 0 && matches!(toks[k - 1], Tok::Param(_) | Tok::Comma | Tok::Other | Tok::Str) {
            k -= 1;
        }
        if k >= 2 && toks[k - 1] == Tok::Open && toks[k - 2].keyword("IN") {
            let col = if k >= 4 && toks[k - 3].keyword("NOT") { k - 4 } else { k - 3 };
            if let Some(oid) = column_at(database, &rel, scratch, &toks, Some(col)) {
                set(n, Some(oid), &mut oids);
            }
        }
    }

    oids
}

/// Whether the `)` at `close` ends a call of a function returning text.
fn text_call_before(toks: &[Tok], close: usize) -> bool {
    let mut depth = 0usize;
    let mut k = close;
    loop {
        match toks.get(k) {
            Some(Tok::Close) => depth += 1,
            Some(Tok::Open) => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            None => return false,
            _ => {}
        }
        if k == 0 {
            return false;
        }
        k -= 1;
    }
    const TEXT_FUNCTIONS: [&str; 30] = [
        "LOWER", "UPPER", "MD5", "TRIM", "LTRIM", "RTRIM", "BTRIM", "LPAD", "RPAD", "CONCAT", "CONCAT_WS", "SUBSTR",
        "SUBSTRING", "LEFT", "RIGHT", "REPLACE", "REVERSE", "INITCAP", "REPEAT", "SPLIT_PART", "TRANSLATE", "TO_CHAR",
        "FORMAT", "CHR", "TO_HEX", "REGEXP_REPLACE", "QUOTE_LITERAL", "QUOTE_IDENT", "OVERLAY", "STRING_AGG",
    ];
    if k.checked_sub(1).and_then(|j| toks.get(j)).is_some_and(|t| TEXT_FUNCTIONS.iter().any(|f| t.keyword(f))) {
        return true;
    }
    // A parenthesised group rather than a call: `(lower(s)) < $1` is the
    // call inside, and `(s || a) < $1` a concatenation, text either way.
    const CLAUSE_WORDS: [&str; 16] = [
        "WHERE", "AND", "OR", "NOT", "ON", "HAVING", "SELECT", "WHEN", "THEN", "ELSE", "CASE", "SET", "RETURNING",
        "BY", "USING", "DISTINCT",
    ];
    if k.checked_sub(1)
        .and_then(|j| toks.get(j))
        .is_some_and(|t| matches!(t, Tok::Ident(..)) && !CLAUSE_WORDS.iter().any(|w| t.keyword(w)))
    {
        // Another function's call.
        return false;
    }
    let mut depth = 0usize;
    for t in &toks[k + 1..close] {
        match t {
            Tok::Open => depth += 1,
            Tok::Close => depth = depth.saturating_sub(1),
            Tok::Op(op) if depth == 0 && op == "||" => return true,
            _ => {}
        }
    }
    close >= 1
        && toks.get(close - 1) == Some(&Tok::Close)
        && matches!(toks.get(k + 1), Some(Tok::Ident(..)))
        && toks.get(k + 2) == Some(&Tok::Open)
        && open_of(toks, close - 1) == Some(k + 2)
        && text_call_before(toks, close - 1)
}

/// The `(` matching the `)` at `close`.
fn open_of(toks: &[Tok], close: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut k = close;
    loop {
        match toks.get(k)? {
            Tok::Close => depth += 1,
            Tok::Open => {
                depth -= 1;
                if depth == 0 {
                    return Some(k);
                }
            }
            _ => {}
        }
        k = k.checked_sub(1)?;
    }
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
    fn an_unqualified_column_looks_in_its_own_parentheses_first() {
        let sql = "WITH c AS (SELECT count(*) AS id FROM t WHERE id > $1) SELECT id FROM c JOIN (SELECT 1 AS k) d ON true WHERE id = $2";
        let toks = tokenize(sql);
        let rel = relations(&toks);
        let names: Vec<&str> = rel.tables.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["t", "c", "d"]);
        let params: Vec<usize> = toks
            .iter()
            .enumerate()
            .filter(|(_, t)| matches!(t, Tok::Param(_)))
            .map(|(k, _)| k)
            .collect();
        // `id > $1` inside the body: `t` first. `id = $2` outside: `c` and
        // the derived table first, `t` only as a last resort.
        assert_eq!(rel.candidates(Some(params[0] - 2)), ["t", "c", "d"]);
        assert_eq!(rel.candidates(Some(params[1] - 2)), ["c", "d", "t"]);
        assert_eq!(rel.candidates(None), ["t", "c", "d"]);
    }

    #[test]
    fn casts_name_their_type() {
        assert_eq!(cast_oid("BIGINT", false), Some(20));
        assert_eq!(cast_oid("uuid", true), Some(2951));
        assert_eq!(cast_oid("inet", false), Some(869));
        assert_eq!(cast_oid("geometry", false), None);
    }
}
