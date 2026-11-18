//! Stored procedures in MySQL's language: CREATE PROCEDURE's parts, the
//! body as statements ([`S`]), and splitting a query into statements without
//! cutting a body at the `;`s inside it.
//!
//! The engine has no procedures; the session runs them ([`super::conn`]),
//! with their definitions kept in the database (the table the SQL Server
//! front end keeps its own in, by dialect).

use super::lex::{Tok, Token};
use super::translate::MyError;
use crate::sqlcompat::{kw, matching_paren, split_commas};

pub(crate) const ROUTINE_TABLE: &str = "__rdb_routines";
pub(crate) const CREATE_ROUTINE_TABLE: &str = "CREATE TABLE IF NOT EXISTS __rdb_routines \
     (name text PRIMARY KEY, dialect text NOT NULL, definition text NOT NULL)";

/// MySQL's default max_sp_recursion_depth is 0; nesting other procedures is
/// allowed this deep.
pub(crate) const MAX_DEPTH: u32 = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ParamMode {
    In,
    Out,
    InOut,
}

#[derive(Clone, Debug)]
pub(crate) struct Param {
    pub(crate) mode: ParamMode,
    pub(crate) name: String,
}

#[derive(Clone, Debug)]
pub(crate) struct ProcDef {
    pub(crate) name: String,
    pub(crate) if_not_exists: bool,
    pub(crate) params: Vec<Param>,
    pub(crate) body: Vec<Token>,
}

/// A condition a handler is for.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Condition {
    NotFound,
    Exception,
    Warning,
    State(String),
    Code(u16),
}

impl Condition {
    pub(crate) fn matches(&self, code: u16, state: &str) -> bool {
        match self {
            Condition::NotFound => state.starts_with("02"),
            Condition::Warning => state.starts_with("01"),
            Condition::Exception => !state.starts_with("00") && !state.starts_with("01") && !state.starts_with("02"),
            Condition::State(s) => s == state,
            Condition::Code(c) => *c == code,
        }
    }
}

/// A statement of a procedure's body.
#[derive(Clone, Debug)]
pub(crate) enum S {
    Sql(Vec<Token>),
    Block { label: Option<String>, body: Vec<S> },
    Declare { names: Vec<String>, default: Option<Vec<Token>> },
    Cursor { name: String, select: Vec<Token> },
    Handler { exit: bool, conditions: Vec<Condition>, body: Box<S> },
    /// SET's assignments: (target as written, value).
    Set(Vec<(Vec<Token>, Vec<Token>)>),
    If { branches: Vec<(Vec<Token>, Vec<S>)>, otherwise: Option<Vec<S>> },
    Case { operand: Option<Vec<Token>>, whens: Vec<(Vec<Token>, Vec<S>)>, otherwise: Option<Vec<S>> },
    While { label: Option<String>, condition: Vec<Token>, body: Vec<S> },
    Repeat { label: Option<String>, body: Vec<S>, until: Vec<Token> },
    Loop { label: Option<String>, body: Vec<S> },
    Leave(String),
    Iterate(String),
    Open(String),
    Fetch { cursor: String, into: Vec<String> },
    Close(String),
    Signal { state: String, message: Option<Vec<Token>>, code: Option<u16> },
    Resignal,
}

fn word(tokens: &[Token], i: usize) -> String {
    tokens.get(i).and_then(|t| t.upper()).unwrap_or_default()
}

fn ident_lower(tokens: &[Token], i: usize) -> Result<String, MyError> {
    tokens
        .get(i)
        .and_then(|t| t.ident())
        .map(str::to_lowercase)
        .ok_or_else(|| MyError::syntax("a name was expected in the procedure"))
}

/// Whether a block-opening word at `i` opens a block (rather than being the
/// IF() or REPEAT() function, or the word after END).
fn opens_block(tokens: &[Token], i: usize) -> bool {
    let w = word(tokens, i);
    let prev = if i > 0 { word(tokens, i - 1) } else { String::new() };
    if prev == "END" {
        return false;
    }
    let call = tokens.get(i + 1).is_some_and(|t| t.is_op("("));
    match w.as_str() {
        "BEGIN" | "CASE" | "WHILE" | "LOOP" => true,
        "REPEAT" => !call,
        "IF" => {
            // A statement IF starts a statement.
            i == 0
                || tokens[i - 1].is_op(";")
                || tokens[i - 1].is_op(":")
                || matches!(prev.as_str(), "BEGIN" | "THEN" | "ELSE" | "DO" | "LOOP" | "REPEAT")
        }
        _ => false,
    }
}

/// Splits a query into statements at `;`, keeping a CREATE PROCEDURE /
/// FUNCTION / TRIGGER / EVENT whole (its body's statements end in `;` too).
pub(crate) fn split(tokens: Vec<Token>) -> Vec<Vec<Token>> {
    let mut out = Vec::new();
    let mut current: Vec<Token> = Vec::new();
    let mut depth = 0i32;
    let mut routine = false;
    let mut body_from = usize::MAX;
    for token in tokens {
        if current.is_empty() {
            routine = false;
            depth = 0;
            body_from = usize::MAX;
        }
        if token.is_op(";") && depth <= 0 {
            if !current.is_empty() {
                out.push(std::mem::take(&mut current));
            }
            continue;
        }
        current.push(token);
        let n = current.len();
        if n >= 2 && kw(&current, 0, "CREATE") && !routine {
            let w = word(&current, n - 1);
            if matches!(w.as_str(), "PROCEDURE" | "FUNCTION" | "TRIGGER" | "EVENT") {
                routine = true;
            }
        }
        if routine && body_from == usize::MAX {
            // The body starts after the parameter list (or, for a trigger
            // or event, at its first BEGIN).
            if current[n - 1].is_op(")") && matching_paren(&current, current.iter().position(|t| t.is_op("(")).unwrap_or(0)) == Some(n - 1) {
                body_from = n;
            } else if current[n - 1].is_word("BEGIN") {
                body_from = n - 1;
            }
        }
        if routine && n > body_from {
            // A word opens a block depending on what follows it (IF( and
            // REPEAT( are functions), so it is decided a token later; END
            // closes at once.
            let i = n - 1;
            if i >= 1 && i > body_from && opens_block(&current, i - 1) {
                depth += 1;
            }
            if current[i].is_word("END") {
                depth -= 1;
            }
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// `CREATE [DEFINER = user] PROCEDURE [IF NOT EXISTS] name (params)
/// [characteristics] body`; `None` when the statement is something else.
pub(crate) fn parse_create(tokens: &[Token]) -> Option<Result<ProcDef, MyError>> {
    if !kw(tokens, 0, "CREATE") {
        return None;
    }
    let mut i = 1;
    if kw(tokens, i, "DEFINER") {
        // DEFINER = user[@host] | CURRENT_USER[()]
        i += 2;
        while i < tokens.len() && !matches!(word(tokens, i).as_str(), "PROCEDURE" | "FUNCTION" | "TRIGGER" | "EVENT" | "VIEW" | "SQL" | "ALGORITHM") {
            i += 1;
        }
    }
    match word(tokens, i).as_str() {
        "PROCEDURE" => {}
        "FUNCTION" => return Some(Err(MyError::unsupported("stored functions (CREATE FUNCTION)"))),
        "TRIGGER" => return Some(Err(MyError::unsupported("triggers"))),
        "EVENT" => return Some(Err(MyError::unsupported("events"))),
        _ => return None,
    }
    Some(parse_definition(tokens, i + 1))
}

fn parse_definition(tokens: &[Token], mut i: usize) -> Result<ProcDef, MyError> {
    let mut if_not_exists = false;
    if kw(tokens, i, "IF") && kw(tokens, i + 1, "NOT") && kw(tokens, i + 2, "EXISTS") {
        if_not_exists = true;
        i += 3;
    }
    let mut name = ident_lower(tokens, i)?;
    i += 1;
    if tokens.get(i).is_some_and(|t| t.is_op(".")) {
        name = ident_lower(tokens, i + 1)?;
        i += 2;
    }
    if !tokens.get(i).is_some_and(|t| t.is_op("(")) {
        return Err(MyError::syntax("CREATE PROCEDURE needs a parameter list"));
    }
    let close = matching_paren(tokens, i).ok_or_else(|| MyError::syntax("unbalanced parameter list"))?;
    let mut params = Vec::new();
    for part in split_commas(&tokens[i + 1..close]) {
        if part.is_empty() {
            continue;
        }
        let (mode, at) = match word(&part, 0).as_str() {
            "IN" => (ParamMode::In, 1),
            "OUT" => (ParamMode::Out, 1),
            "INOUT" => (ParamMode::InOut, 1),
            _ => (ParamMode::In, 0),
        };
        params.push(Param { mode, name: ident_lower(&part, at)? });
    }
    i = close + 1;
    // Characteristics: they change nothing here.
    loop {
        match word(tokens, i).as_str() {
            "COMMENT" => i += 2,
            "LANGUAGE" => i += 2,
            "NOT" if kw(tokens, i + 1, "DETERMINISTIC") => i += 2,
            "DETERMINISTIC" => i += 1,
            "CONTAINS" | "NO" => i += 2,
            "READS" | "MODIFIES" => i += 3,
            "SQL" if kw(tokens, i + 1, "SECURITY") => i += 3,
            _ => break,
        }
    }
    if i >= tokens.len() {
        return Err(MyError::syntax("CREATE PROCEDURE needs a body"));
    }
    let body = tokens[i..].to_vec();
    // A body that does not parse is refused now.
    parse_body(&body)?;
    Ok(ProcDef { name, if_not_exists, params, body })
}

/// A procedure's body: a BEGIN … END block or one statement.
pub(crate) fn parse_body(tokens: &[Token]) -> Result<Vec<S>, MyError> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        if tokens[i].is_op(";") {
            i += 1;
            continue;
        }
        let (s, next) = parse_stmt(tokens, i)?;
        out.push(s);
        i = next;
    }
    Ok(out)
}

/// The statements from `i` until a word in `stops` (not consumed).
fn parse_list(tokens: &[Token], mut i: usize, stops: &[&str]) -> Result<(Vec<S>, usize), MyError> {
    let mut out = Vec::new();
    loop {
        while tokens.get(i).is_some_and(|t| t.is_op(";")) {
            i += 1;
        }
        if i >= tokens.len() {
            return Err(MyError::syntax(format!("{} was expected in the procedure", stops.join(" or "))));
        }
        if stops.contains(&word(tokens, i).as_str()) {
            return Ok((out, i));
        }
        let (s, next) = parse_stmt(tokens, i)?;
        out.push(s);
        i = next;
    }
}

/// From `i` to the first top-level word of `stops` (outside parentheses
/// and CASE … END).
fn until_word(tokens: &[Token], i: usize, stops: &[&str]) -> Result<usize, MyError> {
    let mut depth = 0i32;
    let mut case = 0i32;
    for (j, t) in tokens.iter().enumerate().skip(i) {
        if t.is_op("(") {
            depth += 1;
        } else if t.is_op(")") {
            depth -= 1;
        } else if depth == 0 {
            let w = t.upper().unwrap_or_default();
            if w == "CASE" {
                case += 1;
            } else if w == "END" && case > 0 {
                case -= 1;
            } else if case == 0 && stops.contains(&w.as_str()) {
                return Ok(j);
            }
        }
    }
    Err(MyError::syntax(format!("{} was expected in the procedure", stops.join(" or "))))
}

/// `END word [label]` at `i`; the index after it (and its `;`).
fn expect_end(tokens: &[Token], mut i: usize, what: &str) -> Result<usize, MyError> {
    if !kw(tokens, i, "END") {
        return Err(MyError::syntax(format!("END {what} was expected")));
    }
    i += 1;
    if !what.is_empty() {
        if !kw(tokens, i, what) {
            return Err(MyError::syntax(format!("END {what} was expected")));
        }
        i += 1;
    }
    // An optional closing label.
    if tokens.get(i).is_some_and(|t| t.ident().is_some() && !t.is_op(";")) && !tokens.get(i).is_some_and(|t| matches!(t.tok, Tok::Word(ref w) if is_keyword(w))) {
        i += 1;
    }
    if tokens.get(i).is_some_and(|t| t.is_op(";")) {
        i += 1;
    }
    Ok(i)
}

fn is_keyword(w: &str) -> bool {
    matches!(
        w.to_ascii_uppercase().as_str(),
        "END" | "ELSE" | "ELSEIF" | "WHEN" | "UNTIL" | "IF" | "CASE" | "WHILE" | "LOOP" | "REPEAT" | "BEGIN"
            | "DECLARE" | "SET" | "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "OPEN" | "CLOSE" | "FETCH" | "LEAVE"
            | "ITERATE" | "CALL" | "SIGNAL" | "RESIGNAL" | "RETURN" | "REPLACE" | "CREATE" | "DROP" | "ALTER"
            | "COMMIT" | "ROLLBACK" | "START" | "TRUNCATE" | "WITH" | "DO"
    )
}

/// To the `;` that ends a simple statement (or the end).
fn to_semicolon(tokens: &[Token], i: usize) -> usize {
    let mut depth = 0i32;
    for (j, t) in tokens.iter().enumerate().skip(i) {
        if t.is_op("(") {
            depth += 1;
        } else if t.is_op(")") {
            depth -= 1;
        } else if depth == 0 && t.is_op(";") {
            return j;
        }
    }
    tokens.len()
}

fn parse_stmt(tokens: &[Token], i: usize) -> Result<(S, usize), MyError> {
    // `label:` before BEGIN / WHILE / REPEAT / LOOP.
    let (label, i) = if tokens.get(i).is_some_and(|t| t.ident().is_some()) && tokens.get(i + 1).is_some_and(|t| t.is_op(":")) {
        (Some(ident_lower(tokens, i)?), i + 2)
    } else {
        (None, i)
    };
    let head = word(tokens, i);
    match head.as_str() {
        "BEGIN" => {
            let mut j = i + 1;
            if kw(tokens, j, "NOT") && kw(tokens, j + 1, "ATOMIC") {
                j += 2;
            }
            let (body, end) = parse_list(tokens, j, &["END"])?;
            Ok((S::Block { label, body }, expect_end(tokens, end, "")?))
        }
        "IF" => {
            let mut branches = Vec::new();
            let mut otherwise = None;
            let mut j = i + 1;
            loop {
                let then = until_word(tokens, j, &["THEN"])?;
                let condition = tokens[j..then].to_vec();
                let (body, at) = parse_list(tokens, then + 1, &["ELSEIF", "ELSE", "END"])?;
                branches.push((condition, body));
                match word(tokens, at).as_str() {
                    "ELSEIF" => j = at + 1,
                    "ELSE" => {
                        let (body, end) = parse_list(tokens, at + 1, &["END"])?;
                        otherwise = Some(body);
                        j = end;
                        break;
                    }
                    _ => {
                        j = at;
                        break;
                    }
                }
            }
            Ok((S::If { branches, otherwise }, expect_end(tokens, j, "IF")?))
        }
        "CASE" => {
            let first = until_word(tokens, i + 1, &["WHEN"])?;
            let operand = (first > i + 1).then(|| tokens[i + 1..first].to_vec());
            let mut whens = Vec::new();
            let mut otherwise = None;
            let mut j = first;
            loop {
                let then = until_word(tokens, j + 1, &["THEN"])?;
                let condition = tokens[j + 1..then].to_vec();
                let (body, at) = parse_list(tokens, then + 1, &["WHEN", "ELSE", "END"])?;
                whens.push((condition, body));
                match word(tokens, at).as_str() {
                    "WHEN" => j = at,
                    "ELSE" => {
                        let (body, end) = parse_list(tokens, at + 1, &["END"])?;
                        otherwise = Some(body);
                        j = end;
                        break;
                    }
                    _ => {
                        j = at;
                        break;
                    }
                }
            }
            Ok((S::Case { operand, whens, otherwise }, expect_end(tokens, j, "CASE")?))
        }
        "WHILE" => {
            let d = until_word(tokens, i + 1, &["DO"])?;
            let (body, end) = parse_list(tokens, d + 1, &["END"])?;
            Ok((S::While { label, condition: tokens[i + 1..d].to_vec(), body }, expect_end(tokens, end, "WHILE")?))
        }
        "REPEAT" => {
            let (body, at) = parse_list(tokens, i + 1, &["UNTIL"])?;
            let end = until_word(tokens, at + 1, &["END"])?;
            Ok((S::Repeat { label, body, until: tokens[at + 1..end].to_vec() }, expect_end(tokens, end, "REPEAT")?))
        }
        "LOOP" => {
            let (body, end) = parse_list(tokens, i + 1, &["END"])?;
            Ok((S::Loop { label, body }, expect_end(tokens, end, "LOOP")?))
        }
        _ if label.is_some() => Err(MyError::syntax("a label goes before BEGIN, WHILE, REPEAT or LOOP")),
        "LEAVE" | "ITERATE" => {
            let l = ident_lower(tokens, i + 1)?;
            let end = to_semicolon(tokens, i);
            Ok((if head == "LEAVE" { S::Leave(l) } else { S::Iterate(l) }, end + 1))
        }
        "DECLARE" => {
            let end = to_semicolon(tokens, i);
            let s = parse_declare(tokens, i, end)?;
            // A handler's statement may be a block, which has its own end.
            Ok(s)
        }
        "OPEN" | "CLOSE" => {
            let name = ident_lower(tokens, i + 1)?;
            let end = to_semicolon(tokens, i);
            Ok((if head == "OPEN" { S::Open(name) } else { S::Close(name) }, end + 1))
        }
        "FETCH" => {
            let end = to_semicolon(tokens, i);
            let mut j = i + 1;
            if kw(tokens, j, "NEXT") {
                j += 1;
            }
            if kw(tokens, j, "FROM") {
                j += 1;
            }
            let cursor = ident_lower(tokens, j)?;
            if !kw(tokens, j + 1, "INTO") {
                return Err(MyError::syntax("FETCH … INTO was expected"));
            }
            let mut into = Vec::new();
            for part in split_commas(&tokens[j + 2..end]) {
                into.push(ident_lower(&part, 0)?);
            }
            Ok((S::Fetch { cursor, into }, end + 1))
        }
        "SET" => {
            let end = to_semicolon(tokens, i);
            let mut out = Vec::new();
            for part in split_commas(&tokens[i + 1..end]) {
                let eq = part
                    .iter()
                    .position(|t| t.is_op("=") || t.is_op(":="))
                    .ok_or_else(|| MyError::syntax("SET needs '='"))?;
                out.push((part[..eq].to_vec(), part[eq + 1..].to_vec()));
            }
            Ok((S::Set(out), end + 1))
        }
        "SIGNAL" => {
            let end = to_semicolon(tokens, i);
            let mut j = i + 1;
            if !kw(tokens, j, "SQLSTATE") {
                return Err(MyError::unsupported("SIGNAL of a named condition"));
            }
            j += 1;
            if kw(tokens, j, "VALUE") {
                j += 1;
            }
            let state = match tokens.get(j).map(|t| &t.tok) {
                Some(Tok::Str(s)) => String::from_utf8_lossy(s).into_owned(),
                _ => return Err(MyError::syntax("SIGNAL SQLSTATE needs a string")),
            };
            let mut message = None;
            let mut code = None;
            if kw(tokens, j + 1, "SET") {
                for part in split_commas(&tokens[j + 2..end]) {
                    let eq = part.iter().position(|t| t.is_op("=")).ok_or_else(|| MyError::syntax("SIGNAL … SET needs '='"))?;
                    match word(&part, 0).as_str() {
                        "MESSAGE_TEXT" => message = Some(part[eq + 1..].to_vec()),
                        "MYSQL_ERRNO" => code = part.get(eq + 1).and_then(|t| match &t.tok {
                            Tok::Num(n) => n.parse().ok(),
                            _ => None,
                        }),
                        _ => {}
                    }
                }
            }
            Ok((S::Signal { state, message, code }, end + 1))
        }
        "RESIGNAL" => Ok((S::Resignal, to_semicolon(tokens, i) + 1)),
        "RETURN" => Err(MyError::syntax("RETURN is only allowed in a FUNCTION")),
        _ => {
            let end = to_semicolon(tokens, i);
            Ok((S::Sql(tokens[i..end].to_vec()), end + 1))
        }
    }
}

fn parse_declare(tokens: &[Token], i: usize, end: usize) -> Result<(S, usize), MyError> {
    let w1 = word(tokens, i + 1);
    if matches!(w1.as_str(), "CONTINUE" | "EXIT" | "UNDO") {
        if w1 == "UNDO" {
            return Err(MyError::unsupported("UNDO handlers"));
        }
        if !(kw(tokens, i + 2, "HANDLER") && kw(tokens, i + 3, "FOR")) {
            return Err(MyError::syntax("DECLARE … HANDLER FOR was expected"));
        }
        // Conditions, comma-separated, then the handler's statement.
        let mut j = i + 4;
        let mut conditions = Vec::new();
        loop {
            match word(tokens, j).as_str() {
                "NOT" if kw(tokens, j + 1, "FOUND") => {
                    conditions.push(Condition::NotFound);
                    j += 2;
                }
                "SQLEXCEPTION" => {
                    conditions.push(Condition::Exception);
                    j += 1;
                }
                "SQLWARNING" => {
                    conditions.push(Condition::Warning);
                    j += 1;
                }
                "SQLSTATE" => {
                    j += 1;
                    if kw(tokens, j, "VALUE") {
                        j += 1;
                    }
                    match tokens.get(j).map(|t| &t.tok) {
                        Some(Tok::Str(s)) => conditions.push(Condition::State(String::from_utf8_lossy(s).into_owned())),
                        _ => return Err(MyError::syntax("SQLSTATE needs a string")),
                    }
                    j += 1;
                }
                _ => match tokens.get(j).map(|t| &t.tok) {
                    Some(Tok::Num(n)) => {
                        conditions.push(Condition::Code(n.parse().map_err(|_| MyError::syntax("an error code was expected"))?));
                        j += 1;
                    }
                    _ => return Err(MyError::unsupported("handlers for named conditions")),
                },
            }
            if tokens.get(j).is_some_and(|t| t.is_op(",")) {
                j += 1;
                continue;
            }
            break;
        }
        let (body, next) = parse_stmt(tokens, j)?;
        return Ok((S::Handler { exit: w1 == "EXIT", conditions, body: Box::new(body) }, next));
    }
    let cursor_at = (i + 2..end).find(|&k| kw(tokens, k, "CURSOR"));
    if let Some(c) = cursor_at.filter(|&c| c == i + 2) {
        if !kw(tokens, c + 1, "FOR") {
            return Err(MyError::syntax("DECLARE … CURSOR FOR was expected"));
        }
        return Ok((S::Cursor { name: ident_lower(tokens, i + 1)?, select: tokens[c + 2..end].to_vec() }, end + 1));
    }
    if kw(tokens, i + 2, "CONDITION") {
        return Err(MyError::unsupported("DECLARE … CONDITION"));
    }
    // DECLARE a, b type [DEFAULT expr]
    let default_at = (i + 1..end).find(|&k| kw(tokens, k, "DEFAULT"));
    let decl_end = default_at.unwrap_or(end);
    let mut names = Vec::new();
    let mut k = i + 1;
    while k < decl_end {
        names.push(ident_lower(tokens, k)?);
        if tokens.get(k + 1).is_some_and(|t| t.is_op(",")) {
            k += 2;
        } else {
            break;
        }
    }
    let default = default_at.map(|d| tokens[d + 1..end].to_vec());
    Ok((S::Declare { names, default }, end + 1))
}

/// Each local variable's name, where it stands for its value, as its value.
pub(crate) fn substitute(tokens: &[Token], value_of: &dyn Fn(&str) -> Option<Token>) -> Vec<Token> {
    let mut out = Vec::with_capacity(tokens.len());
    for (k, t) in tokens.iter().enumerate() {
        let local = match &t.tok {
            Tok::Word(w) => Some(w.to_lowercase()),
            Tok::Quoted(q) => Some(q.to_lowercase()),
            _ => None,
        };
        if let Some(name) = local {
            let after_dot = k > 0 && tokens[k - 1].is_op(".");
            let qualifier = tokens.get(k + 1).is_some_and(|n| n.is_op(".") || n.is_op("("));
            let alias = k > 0 && tokens[k - 1].is_word("AS");
            if !after_dot && !qualifier && !alias {
                if let Some(mut v) = value_of(&name) {
                    v.space = t.space;
                    v.span = t.span;
                    out.push(v);
                    continue;
                }
            }
        }
        out.push(t.clone());
    }
    out
}

/// `SELECT … INTO a, b [FROM …]`: the query without its INTO, and the
/// variables.
pub(crate) fn select_into(tokens: &[Token]) -> Option<(Vec<Token>, Vec<String>)> {
    if !kw(tokens, 0, "SELECT") {
        return None;
    }
    let into = crate::sqlcompat::find_top(tokens, 1, &["INTO"])?;
    let end = crate::sqlcompat::find_top(tokens, into, &["FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "FOR", "UNION"])
        .unwrap_or(tokens.len());
    let names = split_commas(&tokens[into + 1..end])
        .iter()
        .map(|p| match p.first().map(|t| &t.tok) {
            Some(Tok::UserVar(u)) => format!("@{}", u.to_lowercase()),
            _ => p.first().and_then(|t| t.ident()).unwrap_or("").to_lowercase(),
        })
        .collect();
    let mut query = tokens[..into].to_vec();
    query.extend(tokens[end..].iter().cloned());
    Some((query, names))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::lex::{Mode, lex};

    fn t(sql: &str) -> Vec<Token> {
        lex(sql.as_bytes(), Mode::default()).unwrap()
    }

    #[test]
    fn splits_around_bodies() {
        let sql = "DROP PROCEDURE IF EXISTS p; CREATE DEFINER=`root`@`localhost` PROCEDURE p(IN a INT, OUT b INT)
            BEGIN
              DECLARE i INT DEFAULT 0;
              IF (a > 1) THEN SET b = 1; ELSEIF a = 0 THEN SET b = 0; ELSE SET b = CASE WHEN a < 0 THEN -1 END; END IF;
              l1: WHILE i < a DO SET i = i + 1; IF i > 5 THEN LEAVE l1; END IF; END WHILE l1;
              REPEAT SET i = i - 1; UNTIL i <= 0 END REPEAT;
              SELECT IF(a > 0, 'pos', 'neg'), REPEAT('x', 2);
            END; SELECT 1";
        let parts = split(t(sql));
        assert_eq!(parts.len(), 3, "{parts:?}");
        let def = parse_create(&parts[1]).unwrap().unwrap();
        assert_eq!(def.name, "p");
        assert_eq!(def.params.len(), 2);
        assert_eq!(def.params[1].mode, ParamMode::Out);
        let body = parse_body(&def.body).unwrap();
        let S::Block { body, .. } = &body[0] else { panic!("{body:?}") };
        assert_eq!(body.len(), 5, "{body:?}");
        assert!(matches!(&body[1], S::If { branches, otherwise: Some(_) } if branches.len() == 2));
        assert!(matches!(&body[2], S::While { label: Some(l), .. } if l == "l1"));
        assert!(matches!(&body[3], S::Repeat { .. }));
        assert!(matches!(&body[4], S::Sql(_)));
    }

    #[test]
    fn cursors_and_handlers() {
        let body = parse_body(&t("BEGIN
            DECLARE done INT DEFAULT FALSE;
            DECLARE v INT;
            DECLARE c CURSOR FOR SELECT id FROM t ORDER BY id;
            DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
            DECLARE EXIT HANDLER FOR SQLEXCEPTION, SQLSTATE '23000' BEGIN ROLLBACK; RESIGNAL; END;
            OPEN c;
            read_loop: LOOP
              FETCH c INTO v;
              IF done THEN LEAVE read_loop; END IF;
              CASE v WHEN 1 THEN SELECT 'one'; ELSE SELECT 'other'; END CASE;
            END LOOP;
            CLOSE c;
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'stop', MYSQL_ERRNO = 5001;
          END")).unwrap();
        let S::Block { body, .. } = &body[0] else { panic!() };
        assert!(matches!(&body[2], S::Cursor { name, .. } if name == "c"));
        assert!(matches!(&body[3], S::Handler { exit: false, conditions, .. } if conditions == &[Condition::NotFound]));
        assert!(matches!(&body[4], S::Handler { exit: true, conditions, body } if conditions.len() == 2 && matches!(**body, S::Block { .. })));
        assert!(matches!(&body[6], S::Loop { label: Some(_), body } if body.len() == 3));
        assert!(matches!(&body[8], S::Signal { state, code: Some(5001), .. } if state == "45000"));
        assert!(Condition::Exception.matches(1062, "23000"));
        assert!(!Condition::Exception.matches(1329, "02000"));
    }

    #[test]
    fn variables_and_select_into() {
        let v = |n: &str| (n == "p_id").then(|| Token::new(Tok::Num("7".into())));
        let out = substitute(&t("SELECT p_id AS p_id, t.p_id, p_id(1) FROM t WHERE id = p_id"), &v);
        assert_eq!(crate::sqlcompat::render_text(&out), "SELECT 7 AS p_id, t.p_id, p_id(1) FROM t WHERE id = 7");
        let (q, names) = select_into(&t("SELECT a, b INTO x, @y FROM t WHERE c = 1")).unwrap();
        assert_eq!(crate::sqlcompat::render_text(&q), "SELECT a, b FROM t WHERE c = 1");
        assert_eq!(names, ["x", "@y"]);
    }
}
