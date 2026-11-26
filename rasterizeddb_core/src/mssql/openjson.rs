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
//! JSON read from a column (`CROSS APPLY OPENJSON(t.tags)`) stays refused:
//! its rows are not known here.
//!
//! What differs from SQL Server: an object's members come in name order,
//! not as written (the parsed value keeps no order), and a number past an
//! `f64` or `u64` is read as the nearest one.

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
    /// The JSON text; `None` for NULL.
    json: Option<String>,
    path: Option<String>,
    columns: Option<Vec<Column>>,
    alias: Option<String>,
    /// The token after the call, its `WITH` and its alias.
    end: usize,
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
        if is_call(&tokens, i) {
            let call = parse(&tokens, i, env)?;
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

fn parse(tokens: &[Token], at: usize, env: &dyn Env) -> Result<Call, TsqlError> {
    let close = matching_paren(tokens, at + 1).ok_or_else(|| TsqlError::syntax("unbalanced parentheses"))?;
    let args = split_commas(&tokens[at + 2..close]);
    if args.is_empty() || args.len() > 2 || args[0].is_empty() {
        return Err(TsqlError::new(174, "The openjson function requires 1 to 2 arguments."));
    }
    let json = text_of(&args[0], env)?;
    let path = match args.get(1) {
        None => None,
        Some(p) => Some(text_of(p, env)?.ok_or_else(|| TsqlError::new(13608, "Property cannot be found on the specified JSON path."))?),
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

/// The text an argument is: a literal's, a parameter's or a variable's;
/// `None` for NULL.
fn text_of(arg: &[Token], env: &dyn Env) -> Result<Option<String>, TsqlError> {
    let refused = || TsqlError::unsupported("OPENJSON over a column or an expression");
    let [t] = arg else { return Err(refused()) };
    match &t.tok {
        Tok::Str(b) => Ok(Some(String::from_utf8_lossy(b).into_owned())),
        Tok::Word(w) if w.eq_ignore_ascii_case("NULL") => Ok(None),
        Tok::UserVar(name) => {
            if let Some(text) = env.param_text(name) {
                return Ok(text);
            }
            match env.variable(name) {
                Some(Value::Null) => Ok(None),
                Some(v) => Ok(v.text()),
                None => Err(TsqlError::new(137, format!("Must declare the scalar variable \"@{name}\"."))),
            }
        }
        _ => Err(refused()),
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

fn at_path<'v>(root: &'v serde_json::Value, path: &str) -> Result<Option<&'v serde_json::Value>, TsqlError> {
    let mut v = root;
    for step in steps(path)? {
        let next = match step {
            Step::Key(k) => v.as_object().and_then(|o| o.get(&k)),
            Step::Index(i) => v.as_array().and_then(|a| a.get(i)),
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
    let Some(json) = call.json.as_deref() else { return Ok(Vec::new()) };
    let malformed = || TsqlError::new(13609, "JSON text is not properly formatted.");
    let root: serde_json::Value = serde_json::from_str(json).map_err(|_| malformed())?;
    let Some(root) = at_path(&root, call.path.as_deref().unwrap_or("$"))? else { return Ok(Vec::new()) };
    use serde_json::Value as J;
    let Some(columns) = &call.columns else {
        let row = |key: String, v: &J| {
            let (value, kind) = match v {
                J::Null => (Cell::Null, 0),
                J::String(s) => (Cell::Text(s.clone()), 1),
                J::Number(n) => (Cell::Text(n.to_string()), 2),
                J::Bool(b) => (Cell::Text(b.to_string()), 3),
                J::Array(_) => (Cell::Text(v.to_string()), 4),
                J::Object(_) => (Cell::Text(v.to_string()), 5),
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
                None => element.as_object().and_then(|o| o.get(&c.name)),
            };
            row.push(match found {
                None | Some(J::Null) => Cell::Null,
                Some(v @ (J::Array(_) | J::Object(_))) if c.as_json => Cell::Text(v.to_string()),
                // A scalar is no `AS JSON` value, and an object or an array
                // no scalar: NULL, as SQL Server's lax mode gives.
                Some(_) if c.as_json => Cell::Null,
                Some(J::Array(_) | J::Object(_)) => Cell::Null,
                Some(J::String(s)) => Cell::Text(s.clone()),
                Some(J::Number(n)) => Cell::Number(n.to_string()),
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
