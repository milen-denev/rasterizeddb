//! `OPENJSON(json [, path]) [WITH (column type [path] [AS JSON], …)]`, over
//! JSON text the statement carries: a literal, a parameter or a variable.
//!
//! EF Core passes a list this way. `ids.Contains(x)` is
//! `x IN (SELECT [p].[value] FROM OPENJSON(@ids) WITH ([value] int '$') AS [p])`
//! with `@ids` the list as a JSON array (EF Core 8 and 9 always, 10 when
//! parameters are kept whole), and a join against a list reads the bare
//! form's `key` and `value`. The whole of it was refused.
//!
//! A statement is translated with its parameters' values in hand, and not
//! kept (`conn::Scope`), so the JSON is read here and the rows written into
//! the statement: a `VALUES` relation, each value cast to its column's type,
//! or, for the `IN (SELECT … FROM OPENJSON …)` shape, the list itself, which
//! the engine can answer from an index where a subquery is run and compared.
//!
//! JSON read from a row (`CROSS APPLY OPENJSON([t].[Tags]) WITH (…)`, which
//! EF Core writes for a collection kept in a JSON column) is not known here:
//! the APPLY becomes a `LATERAL` join over the engine's
//! `jsonb_array_elements`, each column read from an element by its path.
//! Inside a subquery (`(SELECT COUNT(*) FROM OPENJSON([t].[Tags]) AS [x])`)
//! it is that relation, correlated; `EXISTS (SELECT … FROM OPENJSON(col) …)`
//! and `v IN (SELECT x.c FROM OPENJSON(col) WITH (…) AS x)`, which EF Core
//! writes for `Any` and `Contains`, are counted (`(SELECT COUNT(*) …) > 0`),
//! the form of them the engine correlates over a function. `NOT IN` is
//! refused: a NULL element makes it unknown, which a count does not say.
//! This form reads a JSON array only (an object is the engine's "cannot
//! extract elements" error, where SQL Server reads it as one row or as its
//! members) and gives a nested object's members in the engine's order.
//!
//! The JSON is read by a parse of its own ([`Json`]), which keeps an object's
//! members in the order written, as SQL Server gives them: `serde_json`'s
//! value sorts them by name. What differs from SQL Server: a number past an
//! `f64` or `u64` is read as the nearest one, and an object or an array
//! shown as text is written compactly, not as it was spaced.

use super::lex::{Tok, Token, matching_paren, split_commas};
use super::translate::{Env, TsqlError, Value, is_clause_word, map_type, sql_str};
use crate::sqlcompat::{ident_sql, kw};

/// One `WITH` column.
struct Column {
    name: String,
    /// Its type, in the engine's words.
    ty: String,
    /// Where its value is in a row's JSON; `$.name` when not given.
    path: Option<String>,
    /// `AS JSON`: an object or an array, as its text.
    as_json: bool,
}

/// One call, with what follows it.
struct Call {
    json: Source,
    path: Option<String>,
    columns: Option<Vec<Column>>,
    alias: Option<String>,
    /// The token after the call, its `WITH` and its alias.
    end: usize,
}

/// Where a call's JSON is.
enum Source {
    /// Text the statement carries; `None` for NULL.
    Text(Option<String>),
    /// An expression over a row, as written.
    Row(Vec<Token>),
}

/// One value of a row.
#[derive(Debug, PartialEq)]
enum Cell {
    Null,
    Text(String),
    Number(String),
    Bool(bool),
}

fn is_call(tokens: &[Token], at: usize) -> bool {
    kw(tokens, at, "OPENJSON")
        && tokens.get(at + 1).is_some_and(|t| t.is_op("("))
        && !at.checked_sub(1).is_some_and(|p| tokens[p].is_op("."))
}

/// Every `OPENJSON` of `tokens` as the rows it reads.
pub(crate) fn pass(tokens: Vec<Token>, env: &dyn Env) -> Result<Vec<Token>, TsqlError> {
    if !(0..tokens.len()).any(|i| is_call(&tokens, i)) {
        return Ok(tokens);
    }
    let mut out: Vec<Token> = Vec::with_capacity(tokens.len());
    let mut i = 0usize;
    while i < tokens.len() {
        if let Some((list, next)) = in_list(&tokens, i, env)? {
            out.extend(list);
            i = next;
            continue;
        }
        if let Some(next) = counted(&tokens, i, &mut out, env)? {
            i = next;
            continue;
        }
        if is_call(&tokens, i) {
            let call = parse(&tokens, i, env)?;
            if let Source::Row(json) = &call.json {
                let (before, after) = row_relation(&call)?;
                let (before, after) = match out.as_slice() {
                    // `CROSS APPLY` / `OUTER APPLY` before it, taken back.
                    [.., form, apply] if apply.is_word("APPLY") && (form.is_word("CROSS") || form.is_word("OUTER")) => {
                        let outer = form.is_word("OUTER");
                        out.truncate(out.len() - 2);
                        (
                            format!("{} JOIN LATERAL {before}", if outer { "LEFT" } else { "CROSS" }),
                            format!("{after}{}", if outer { " ON 1 = 1" } else { "" }),
                        )
                    }
                    // A subquery's own relation, reading the row outside it.
                    [.., from] if from.is_word("FROM") && depth_of(&tokens, i) > 0 => (before, after),
                    _ => return Err(TsqlError::unsupported("OPENJSON over a column or an expression, other than after APPLY or as a subquery's FROM")),
                };
                let mut head = Token::raw(before);
                head.space = true;
                out.push(head);
                out.extend(json.iter().cloned());
                out.push(Token::raw(after));
                i = call.end;
                continue;
            }
            let mut relation = Token::raw(relation_sql(&call)?);
            relation.space = tokens[i].space;
            out.push(relation);
            i = call.end;
            continue;
        }
        out.push(tokens[i].clone());
        i += 1;
    }
    Ok(out)
}

/// `IN (SELECT [p.]c FROM OPENJSON(json) WITH (c type path) [AS] p)` at
/// `at` (the `IN`), as `IN (v, …)` and the token after it. `None` for any
/// other shape, and for no rows: `x NOT IN ()` of nothing is true, which no
/// list says.
fn in_list(tokens: &[Token], at: usize, env: &dyn Env) -> Result<Option<(Vec<Token>, usize)>, TsqlError> {
    if !(kw(tokens, at, "IN") && tokens.get(at + 1).is_some_and(|t| t.is_op("(")) && kw(tokens, at + 2, "SELECT")) {
        return Ok(None);
    }
    let Some(close) = matching_paren(tokens, at + 1) else { return Ok(None) };
    // `c FROM` or `p . c FROM`
    let mut i = at + 3;
    let mut qualifier: Option<&str> = None;
    if tokens.get(i + 1).is_some_and(|t| t.is_op(".")) {
        qualifier = tokens[i].ident();
        if qualifier.is_none() {
            return Ok(None);
        }
        i += 2;
    }
    let Some(selected) = tokens.get(i).and_then(|t| t.ident()) else { return Ok(None) };
    if !(kw(tokens, i + 1, "FROM") && is_call(tokens, i + 2)) {
        return Ok(None);
    }
    let call = parse(&tokens[..close], i + 2, env)?;
    if matches!(call.json, Source::Row(_)) {
        return Ok(None);
    }
    let Some([column]) = call.columns.as_deref() else { return Ok(None) };
    if call.end != close
        || !column.name.eq_ignore_ascii_case(selected)
        || qualifier.is_some_and(|q| !call.alias.as_deref().is_some_and(|a| a.eq_ignore_ascii_case(q)))
    {
        return Ok(None);
    }
    let rows = rows(&call)?;
    if rows.is_empty() {
        return Ok(None);
    }
    let list: Vec<String> = rows.iter().map(|r| list_value(&r[0], &column.ty)).collect();
    let mut head = tokens[at].clone();
    head.tok = Tok::Word("IN".into());
    Ok(Some((vec![head, Token::raw(format!("({})", list.join(", ")))], close + 1)))
}

/// `tokens` with each parameter as its value. The engine runs a subquery
/// over an outer row once for each row, from its text, where a `$n` is not
/// bound: the value is written in, a number bare and anything else quoted.
fn inlined(tokens: Vec<Token>, env: &dyn Env) -> Vec<Token> {
    tokens
        .into_iter()
        .map(|t| {
            let Tok::UserVar(name) = &t.tok else { return t };
            let Some(text) = env.param_text(name) else { return t };
            let string = env.param(name).is_none_or(|p| p.string);
            let mut value = match text {
                None => Token::word("NULL"),
                Some(v) if !string && v.parse::<f64>().is_ok() => Token::new(Tok::Num(v)),
                Some(v) => Token::new(Tok::Str(v.into_bytes())),
            };
            value.space = t.space;
            value
        })
        .collect()
}

/// How many parentheses are open at `at`.
fn depth_of(tokens: &[Token], at: usize) -> usize {
    let mut depth = 0usize;
    for t in &tokens[..at] {
        if t.is_op("(") {
            depth += 1;
        } else if t.is_op(")") {
            depth = depth.saturating_sub(1);
        }
    }
    depth
}

/// `EXISTS (SELECT … FROM OPENJSON(row) … [WHERE c])` and
/// `v IN (SELECT [x.]c FROM OPENJSON(row) WITH (…) [AS] x [WHERE c])` at
/// `at` (the `EXISTS` or the `IN`), over JSON read from a row: written to
/// `out` as a count of the rows compared with zero, and the token after it
/// returned. `None` for any other shape.
fn counted(tokens: &[Token], at: usize, out: &mut Vec<Token>, env: &dyn Env) -> Result<Option<usize>, TsqlError> {
    let exists = kw(tokens, at, "EXISTS");
    if !(exists || kw(tokens, at, "IN")) || !tokens.get(at + 1).is_some_and(|t| t.is_op("(")) || !kw(tokens, at + 2, "SELECT") {
        return Ok(None);
    }
    let Some(close) = matching_paren(tokens, at + 1) else { return Ok(None) };
    let inner = &tokens[at + 2..close];
    let Some(from) = crate::sqlcompat::find_top(inner, 1, &["FROM"]) else { return Ok(None) };
    if !is_call(inner, from + 1) {
        return Ok(None);
    }
    let call = parse(inner, from + 1, env)?;
    let Source::Row(json) = &call.json else { return Ok(None) };
    // Nothing beside the relation but a filter.
    let rest = &inner[call.end..];
    if !(rest.is_empty() || rest[0].is_word("WHERE")) || crate::sqlcompat::find_top(rest, 0, &["GROUP", "HAVING", "ORDER", "UNION", "EXCEPT", "INTERSECT", "OFFSET"]).is_some() {
        return Ok(None);
    }
    let (before, after) = row_relation(&call)?;
    let mut filter: Vec<Token> = inlined(pass(rest.to_vec(), env)?, env);
    let mut compared: Option<(String, Vec<Token>)> = None;
    if !exists {
        // The one selected column, and the value before `IN`.
        let list = &inner[1..from];
        let column = match list {
            [c] => c.ident(),
            [q, dot, c] if dot.is_op(".") && q.ident().is_some_and(|q| call.alias.as_deref().is_some_and(|a| a.eq_ignore_ascii_case(q))) => c.ident(),
            _ => None,
        };
        let Some(column) = column else { return Ok(None) };
        let refused = |what: &str| TsqlError::unsupported(format!("{what} (SELECT … FROM OPENJSON over a column)"));
        if out.last().is_some_and(|t| t.is_word("NOT")) {
            return Err(refused("NOT IN"));
        }
        let simple = |t: &Token| matches!(t.tok, Tok::Word(_) | Tok::Quoted(_) | Tok::Str(_) | Tok::Num(_) | Tok::UserVar(_) | Tok::Param(_));
        let take = match out.as_slice() {
            [.., a, dot, b] if dot.is_op(".") && a.ident().is_some() && b.ident().is_some() => 3,
            [.., v] if simple(v) && !super::translate::is_clause_word(v) => 1,
            _ => return Err(refused("an expression IN")),
        };
        let value = inlined(out.split_off(out.len() - take), env);
        let alias = ident_sql(call.alias.as_deref().unwrap_or("openjson"));
        compared = Some((format!("{alias}.{}", ident_sql(column)), value));
    }
    let mut head = Token::raw(format!("((SELECT COUNT(*) FROM {before}"));
    head.space = true;
    out.push(head);
    out.extend(json.iter().cloned());
    out.push(Token::raw(after));
    if let Some((column, value)) = compared {
        if filter.is_empty() {
            out.push(Token::raw(format!("WHERE {column} =")));
            out.extend(value);
        } else {
            // `WHERE c` becomes `WHERE (c) AND column = value`.
            filter.remove(0);
            out.push(Token::raw("WHERE ("));
            out.extend(filter);
            out.push(Token::raw(format!(") AND {column} =")));
            out.extend(value);
        }
    } else {
        out.extend(filter);
    }
    out.push(Token::raw(") > 0)"));
    Ok(Some(close + 1))
}

fn parse(tokens: &[Token], at: usize, env: &dyn Env) -> Result<Call, TsqlError> {
    let close = matching_paren(tokens, at + 1).ok_or_else(|| TsqlError::syntax("unbalanced parentheses"))?;
    let args = split_commas(&tokens[at + 2..close]);
    if args.is_empty() || args.len() > 2 || args[0].is_empty() {
        return Err(TsqlError::new(174, "The openjson function requires 1 to 2 arguments."));
    }
    let json = match text_of(&args[0], env)? {
        Some(text) => Source::Text(text),
        None => Source::Row(args[0].clone()),
    };
    let path = match args.get(1) {
        None => None,
        Some(p) => Some(
            text_of(p, env)?
                .ok_or_else(|| TsqlError::unsupported("an OPENJSON path that is not a literal, a parameter or a variable"))?
                .ok_or_else(|| TsqlError::new(13608, "Property cannot be found on the specified JSON path."))?,
        ),
    };
    let mut end = close + 1;
    let mut columns = None;
    if kw(tokens, end, "WITH") && tokens.get(end + 1).is_some_and(|t| t.is_op("(")) {
        let with_close = matching_paren(tokens, end + 1).ok_or_else(|| TsqlError::syntax("unbalanced WITH"))?;
        let mut list = Vec::new();
        for part in split_commas(&tokens[end + 2..with_close]) {
            list.push(column(&part)?);
        }
        if list.is_empty() {
            return Err(TsqlError::syntax("Incorrect syntax near ')'."));
        }
        columns = Some(list);
        end = with_close + 1;
    }
    let mut alias = None;
    let named = |i: usize| tokens.get(i).filter(|t| !is_clause_word(t)).and_then(|t| t.ident());
    if kw(tokens, end, "AS") {
        alias = Some(named(end + 1).ok_or_else(|| TsqlError::syntax("an alias was expected after AS"))?.to_string());
        end += 2;
    } else if let Some(name) = named(end) {
        alias = Some(name.to_string());
        end += 1;
    }
    Ok(Call { json, path, columns, alias, end })
}

/// The text an argument is, when the statement carries it: a literal's, a
/// parameter's or a variable's (the inner `None` for NULL). `None` for
/// anything read from a row.
fn text_of(arg: &[Token], env: &dyn Env) -> Result<Option<Option<String>>, TsqlError> {
    let [t] = arg else { return Ok(None) };
    match &t.tok {
        Tok::Str(b) => Ok(Some(Some(String::from_utf8_lossy(b).into_owned()))),
        Tok::Word(w) if w.eq_ignore_ascii_case("NULL") => Ok(Some(None)),
        Tok::UserVar(name) => {
            if let Some(text) = env.param_text(name) {
                return Ok(Some(text));
            }
            match env.variable(name) {
                Some(Value::Null) => Ok(Some(None)),
                Some(v) => Ok(Some(v.text())),
                None => Err(TsqlError::new(137, format!("Must declare the scalar variable \"@{name}\"."))),
            }
        }
        _ => Ok(None),
    }
}

/// `name type ['path'] [AS JSON]`.
fn column(part: &[Token]) -> Result<Column, TsqlError> {
    let name = part
        .first()
        .and_then(|t| t.ident())
        .ok_or_else(|| TsqlError::syntax("a column name was expected in OPENJSON's WITH"))?
        .to_string();
    let mut end = part.len();
    let as_json = end >= 4 && kw(part, end - 2, "AS") && kw(part, end - 1, "JSON");
    if as_json {
        end -= 2;
    }
    let mut path = None;
    if end > 2 {
        if let Tok::Str(b) = &part[end - 1].tok {
            path = Some(String::from_utf8_lossy(b).into_owned());
            end -= 1;
        }
    }
    if end < 2 {
        return Err(TsqlError::syntax(format!("a type was expected for OPENJSON's column '{name}'")));
    }
    Ok(Column { name, ty: map_type(&part[1..end])?, path, as_json })
}

/// One step of a JSON path.
enum Step {
    Key(String),
    Index(usize),
}

/// `$`, `$.a.b`, `$."a b"[0]`, with or without `lax`/`strict` before it.
fn steps(path: &str) -> Result<Vec<Step>, TsqlError> {
    let bad = || TsqlError::new(13607, format!("JSON path is not properly formatted: '{path}'."));
    let mut rest = path.trim();
    for mode in ["lax", "strict"] {
        if let Some(r) = rest.strip_prefix(mode).filter(|r| r.starts_with(char::is_whitespace)) {
            rest = r.trim_start();
        }
    }
    let mut rest = rest.strip_prefix('$').ok_or_else(bad)?;
    let mut out = Vec::new();
    while !rest.is_empty() {
        if let Some(r) = rest.strip_prefix('.') {
            if let Some(quoted) = r.strip_prefix('"') {
                let close = quoted.find('"').ok_or_else(bad)?;
                out.push(Step::Key(quoted[..close].to_string()));
                rest = &quoted[close + 1..];
            } else {
                let len = r.find(['.', '[']).unwrap_or(r.len());
                if len == 0 {
                    return Err(bad());
                }
                out.push(Step::Key(r[..len].to_string()));
                rest = &r[len..];
            }
        } else if let Some(r) = rest.strip_prefix('[') {
            let close = r.find(']').ok_or_else(bad)?;
            out.push(Step::Index(r[..close].trim().parse().map_err(|_| bad())?));
            rest = &r[close + 1..];
        } else {
            return Err(bad());
        }
    }
    Ok(out)
}

/// A JSON value, an object's members in the order written.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Json {
    Null,
    Bool(bool),
    /// As `serde_json` writes the number it read.
    Number(String),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    pub(crate) fn parse(text: &str) -> Option<Json> {
        serde_json::from_str(text).ok()
    }

    /// A member by name: the first, where a name is written twice.
    fn member(&self, name: &str) -> Option<&Json> {
        match self {
            Json::Object(members) => members.iter().find(|(k, _)| k == name).map(|(_, v)| v),
            _ => None,
        }
    }

    fn write(&self, out: &mut String) {
        let quoted = |s: &str| serde_json::Value::String(s.to_string()).to_string();
        match self {
            Json::Null => out.push_str("null"),
            Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Json::Number(n) => out.push_str(n),
            Json::String(s) => out.push_str(&quoted(s)),
            Json::Array(items) => {
                out.push('[');
                for (i, v) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    v.write(out);
                }
                out.push(']');
            }
            Json::Object(members) => {
                out.push('{');
                for (i, (k, v)) in members.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(&quoted(k));
                    out.push(':');
                    v.write(out);
                }
                out.push('}');
            }
        }
    }

    /// The value as JSON text, compact.
    pub(crate) fn text(&self) -> String {
        let mut out = String::new();
        self.write(&mut out);
        out
    }
}

impl<'de> serde::Deserialize<'de> for Json {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Json, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Json;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a JSON value")
            }
            fn visit_unit<E>(self) -> Result<Json, E> {
                Ok(Json::Null)
            }
            fn visit_bool<E>(self, v: bool) -> Result<Json, E> {
                Ok(Json::Bool(v))
            }
            fn visit_i64<E>(self, v: i64) -> Result<Json, E> {
                Ok(Json::Number(v.to_string()))
            }
            fn visit_u64<E>(self, v: u64) -> Result<Json, E> {
                Ok(Json::Number(v.to_string()))
            }
            fn visit_f64<E>(self, v: f64) -> Result<Json, E> {
                Ok(serde_json::Number::from_f64(v).map_or(Json::Null, |n| Json::Number(n.to_string())))
            }
            fn visit_str<E>(self, v: &str) -> Result<Json, E> {
                Ok(Json::String(v.to_string()))
            }
            fn visit_string<E>(self, v: String) -> Result<Json, E> {
                Ok(Json::String(v))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Json, A::Error> {
                let mut items = Vec::new();
                while let Some(v) = seq.next_element()? {
                    items.push(v);
                }
                Ok(Json::Array(items))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Json, A::Error> {
                let mut members = Vec::new();
                while let Some(entry) = map.next_entry::<String, Json>()? {
                    members.push(entry);
                }
                Ok(Json::Object(members))
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

fn at_path<'v>(root: &'v Json, path: &str) -> Result<Option<&'v Json>, TsqlError> {
    let mut v = root;
    for step in steps(path)? {
        let next = match step {
            Step::Key(k) => v.member(&k),
            Step::Index(i) => match v {
                Json::Array(a) => a.get(i),
                _ => None,
            },
        };
        match next {
            Some(n) => v = n,
            None => return Ok(None),
        }
    }
    Ok(Some(v))
}

/// The rows the call reads: a `WITH`'s columns, or `key`, `value`, `type`.
fn rows(call: &Call) -> Result<Vec<Vec<Cell>>, TsqlError> {
    let Source::Text(Some(json)) = &call.json else { return Ok(Vec::new()) };
    let malformed = || TsqlError::new(13609, "JSON text is not properly formatted.");
    let root = Json::parse(json).ok_or_else(malformed)?;
    let Some(root) = at_path(&root, call.path.as_deref().unwrap_or("$"))? else { return Ok(Vec::new()) };
    use Json as J;
    let Some(columns) = &call.columns else {
        let row = |key: String, v: &J| {
            let (value, kind) = match v {
                J::Null => (Cell::Null, 0),
                J::String(s) => (Cell::Text(s.clone()), 1),
                J::Number(n) => (Cell::Text(n.clone()), 2),
                J::Bool(b) => (Cell::Text(b.to_string()), 3),
                J::Array(_) => (Cell::Text(v.text()), 4),
                J::Object(_) => (Cell::Text(v.text()), 5),
            };
            vec![Cell::Text(key), value, Cell::Number(kind.to_string())]
        };
        return match root {
            J::Array(a) => Ok(a.iter().enumerate().map(|(i, v)| row(i.to_string(), v)).collect()),
            J::Object(o) => Ok(o.iter().map(|(k, v)| row(k.clone(), v)).collect()),
            _ => Err(malformed()),
        };
    };
    // An array is a row for each element; an object is one row.
    let elements: Vec<&J> = match root {
        J::Array(a) => a.iter().collect(),
        J::Object(_) => vec![root],
        _ => Vec::new(),
    };
    let mut out = Vec::with_capacity(elements.len());
    for element in elements {
        let mut row = Vec::with_capacity(columns.len());
        for c in columns {
            let found = match &c.path {
                Some(p) => at_path(element, p)?,
                None => element.member(&c.name),
            };
            row.push(match found {
                None | Some(J::Null) => Cell::Null,
                Some(v @ (J::Array(_) | J::Object(_))) if c.as_json => Cell::Text(v.text()),
                // A scalar is no `AS JSON` value, and an object or an array
                // no scalar: NULL, as SQL Server's lax mode gives.
                Some(_) if c.as_json => Cell::Null,
                Some(J::Array(_) | J::Object(_)) => Cell::Null,
                Some(J::String(s)) => Cell::Text(s.clone()),
                Some(J::Number(n)) => Cell::Number(n.clone()),
                Some(J::Bool(b)) => Cell::Bool(*b),
            });
        }
        out.push(row);
    }
    Ok(out)
}

/// A value as its column's type.
fn typed(cell: &Cell, ty: &str) -> String {
    match cell {
        Cell::Null => format!("CAST(NULL AS {ty})"),
        Cell::Text(s) => format!("CAST({} AS {ty})", sql_str(s)),
        Cell::Number(n) => format!("CAST({n} AS {ty})"),
        // `true` is 1 to a number and `true` to text, as SQL Server reads it.
        Cell::Bool(b) if ty == "boolean" => (if *b { "TRUE" } else { "FALSE" }).to_string(),
        Cell::Bool(b) if is_text(ty) => format!("CAST({} AS {ty})", sql_str(if *b { "true" } else { "false" })),
        Cell::Bool(b) => format!("CAST({} AS {ty})", u8::from(*b)),
    }
}

fn is_text(ty: &str) -> bool {
    ty == "text" || ty.starts_with("varchar")
}

/// A value in an `IN` list: written as a literal of its column's type where
/// one is (the comparison then reads it as the other side's type, and a key
/// can be looked up), cast otherwise.
fn list_value(cell: &Cell, ty: &str) -> String {
    let number = matches!(ty, "utinyint" | "smallint" | "integer" | "bigint" | "real" | "double precision")
        || ty.starts_with("numeric");
    match cell {
        Cell::Null => "NULL".to_string(),
        Cell::Text(s) if is_text(ty) || ty == "uuid" => sql_str(s),
        Cell::Number(n) if number => n.clone(),
        other => typed(other, ty),
    }
}

/// The call as a relation: `(VALUES (…), …) AS alias(columns)`.
fn relation_sql(call: &Call) -> Result<String, TsqlError> {
    let rows = rows(call)?;
    let columns: Vec<(String, String)> = match &call.columns {
        Some(list) => list.iter().map(|c| (c.name.clone(), c.ty.clone())).collect(),
        None => vec![("key".into(), "text".into()), ("value".into(), "text".into()), ("type".into(), "integer".into())],
    };
    let names: Vec<String> = columns.iter().map(|(n, _)| ident_sql(n)).collect();
    let alias = ident_sql(call.alias.as_deref().unwrap_or("openjson"));
    if rows.is_empty() {
        // `VALUES` has no empty form: one row of typed NULLs, none kept.
        let nulls: Vec<String> = columns.iter().map(|(_, ty)| typed(&Cell::Null, ty)).collect();
        return Ok(format!(
            "(SELECT * FROM (VALUES ({})) AS openjson_none({names}) WHERE 1 = 0) AS {alias}",
            nulls.join(", "),
            names = names.join(", ")
        ));
    }
    let mut sql = String::from("(VALUES ");
    for (r, row) in rows.iter().enumerate() {
        if r > 0 {
            sql.push_str(", ");
        }
        sql.push('(');
        for (c, cell) in row.iter().enumerate() {
            if c > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&typed(cell, &columns[c].1));
        }
        sql.push(')');
    }
    sql.push_str(&format!(") AS {alias}({})", names.join(", ")));
    Ok(sql)
}

/// A JSON path as the engine's `#>` takes one: `'{a,0,"b c"}'`.
fn engine_path(path: &str) -> Result<String, TsqlError> {
    let parts: Vec<String> = steps(path)?
        .into_iter()
        .map(|step| match step {
            Step::Index(i) => i.to_string(),
            Step::Key(k) if !k.is_empty() && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') && !k.eq_ignore_ascii_case("null") => k,
            Step::Key(k) => format!("\"{}\"", k.replace('\\', "\\\\").replace('"', "\\\"")),
        })
        .collect();
    Ok(sql_str(&format!("{{{}}}", parts.join(","))))
}

/// A call over a row's JSON as a relation, `(SELECT … FROM
/// jsonb_array_elements…(CAST(<json> AS jsonb)…) AS e(…)) AS alias`: the
/// text before the JSON expression and the text after it.
fn row_relation(call: &Call) -> Result<(String, String), TsqlError> {
    let alias = ident_sql(call.alias.as_deref().unwrap_or("openjson"));
    let within = match &call.path {
        Some(p) if !steps(p)?.is_empty() => format!(" #> {}", engine_path(p)?),
        _ => String::new(),
    };
    // An element whole is its text (`…_text`); a member of it is read by
    // path, from the element as JSON. The engine has no empty path
    // (`#>> '{}'`), so beside a member the whole element's text is read
    // back out of an array of it.
    const WHOLE: &str = "jsonb_build_array(e.value) ->> 0";
    let whole = |c: &Column| c.path.as_deref().is_some_and(|p| steps(p).is_ok_and(|s| s.is_empty()));
    let (function, list, elements) = match &call.columns {
        None => (
            "jsonb_array_elements",
            format!(
                "CAST(e.n - 1 AS text) AS key, {WHOLE} AS value, CASE jsonb_typeof(e.value) WHEN 'null' THEN 0 WHEN 'string' THEN 1 \
                 WHEN 'number' THEN 2 WHEN 'boolean' THEN 3 WHEN 'array' THEN 4 ELSE 5 END AS type"
            ),
            " WITH ORDINALITY AS e(value, n)",
        ),
        Some(columns) if columns.iter().all(|c| whole(c) && !c.as_json) => (
            "jsonb_array_elements_text",
            columns.iter().map(|c| format!("CAST(e.value AS {}) AS {}", c.ty, ident_sql(&c.name))).collect::<Vec<_>>().join(", "),
            " AS e(value)",
        ),
        Some(columns) => {
            let mut items = Vec::with_capacity(columns.len());
            for c in columns {
                let name = ident_sql(&c.name);
                if whole(c) {
                    items.push(if c.as_json {
                        format!("CAST(e.value AS text) AS {name}")
                    } else {
                        format!("CAST({WHOLE} AS {}) AS {name}", c.ty)
                    });
                    continue;
                }
                let path = match &c.path {
                    Some(p) => engine_path(p)?,
                    None => engine_path(&format!("$.\"{}\"", c.name))?,
                };
                items.push(if c.as_json {
                    format!("CAST(e.value #> {path} AS text) AS {name}")
                } else {
                    format!("CAST(e.value #>> {path} AS {}) AS {name}", c.ty)
                });
            }
            ("jsonb_array_elements", items.join(", "), " AS e(value)")
        }
    };
    Ok((format!("(SELECT {list} FROM {function}(CAST("), format!(" AS jsonb){within}){elements}) AS {alias}")))
}
