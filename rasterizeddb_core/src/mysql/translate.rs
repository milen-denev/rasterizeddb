//! MySQL statements, rewritten into the dialect the engine speaks.
//!
//! Token-level, never text substitution: a rewrite sees what the lexer saw,
//! so a keyword inside a string or a quoted name is never touched.
//!
//! **Identifiers.** MySQL's column names are case-insensitive; PostgreSQL
//! folds unquoted names to lower case and keeps quoted ones exact. Every
//! name — bare or `` `quoted` `` — is therefore sent folded to lower case, so
//! `` `UserId` ``, `userid` and `USERID` are one column, as in MySQL. What
//! MySQL shows as written is a result column's *name*, so the select list
//! keeps it: an unaliased item is named by its text as written
//! (`COUNT(*)`, `UserId`), and an alias keeps its case. The one place the
//! difference shows is `SELECT *`, whose names are the lower-cased ones the
//! table was created with.
//!
//! **Division.** MySQL's `/` never truncates (`7 / 2` is `3.5000`), where the
//! engine's integer division does. `a / b` is sent as `a * 1.0 / b`: the
//! multiplication has `/`'s precedence and associativity, so whatever `a`
//! was, it is the same operand, now exact.
//!
//! **Booleans.** MySQL's `TRUE` is the integer 1; a `BOOLEAN` column is a
//! `TINYINT(1)`. Such columns are created as `boolean` (the engine compares
//! and stores integers into them), and `TRUE`/`FALSE` in expressions are
//! sent as `1`/`0`, which is what MySQL computes with.

use rclite::Arc;

use super::lex::{Mode, Tok, Token, matching_paren, split_commas};
pub(crate) use crate::sqlcompat::{call, find_top, ident_sql, kw, literal_str, quote_ident, string_sql, wrap};

/// Tokens back to text. Never fails today; kept fallible so a rewrite that
/// leaves something unrenderable can say so.
pub(crate) fn render(tokens: &[Token]) -> Result<String, MyError> {
    Ok(crate::sqlcompat::render_text(tokens))
}
use crate::core::database::Database;

/// A value MySQL's session holds: a system or user variable.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Value {
    Null,
    Int(i64),
    Str(String),
}

impl Value {
    pub(crate) fn token(&self) -> Token {
        match self {
            Value::Null => Token::word("NULL"),
            Value::Int(n) => Token::new(Tok::Num(n.to_string())),
            Value::Str(s) => Token::new(Tok::Str(s.as_bytes().to_vec())),
        }
    }

    pub(crate) fn text(&self) -> Option<String> {
        match self {
            Value::Null => None,
            Value::Int(n) => Some(n.to_string()),
            Value::Str(s) => Some(s.clone()),
        }
    }
}

/// An error in MySQL's terms: its number, SQLSTATE and message.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MyError {
    pub(crate) code: u16,
    pub(crate) state: &'static str,
    pub(crate) message: String,
}

impl MyError {
    pub(crate) fn new(code: u16, state: &'static str, message: impl Into<String>) -> Self {
        MyError {
            code,
            state,
            message: message.into(),
        }
    }

    /// ER_PARSE_ERROR.
    pub(crate) fn syntax(message: impl Into<String>) -> Self {
        MyError::new(1064, "42000", message)
    }

    /// ER_NOT_SUPPORTED_YET.
    pub(crate) fn unsupported(what: impl Into<String>) -> Self {
        MyError::new(
            1235,
            "42000",
            format!("This version of RasterizedDB doesn't yet support '{}'", what.into()),
        )
    }
}

/// What the translation reads from the connection.
pub(crate) trait Env {
    fn system_variable(&self, name: &str) -> Option<Value>;
    fn user_variable(&self, name: &str) -> Value;
    fn database(&self) -> &Arc<Database>;
    fn database_name(&self) -> &str;
    fn last_insert_id(&self) -> u64;
    fn found_rows(&self) -> u64;
    fn row_count(&self) -> i64;
    fn connection_id(&self) -> u32;
    fn user(&self) -> &str;
    fn mode(&self) -> Mode;
}

/// What kind of answer a forwarded statement gives.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Kind {
    /// Rows.
    Query,
    /// An INSERT or REPLACE. `auto` is the auto-increment column a
    /// `RETURNING` was added for, whose first value is the insert id.
    Insert { auto: Option<String> },
    /// An UPDATE, DELETE or anything else answered with a count.
    Write,
    /// DDL: commits an open transaction first, as MySQL does.
    Ddl,
    /// Anything else answered with OK.
    Other,
}

/// A statement in the engine's dialect, ready to run.
#[derive(Clone, Debug)]
pub(crate) struct Forward {
    /// What to run, in order; the answer is the last one's (DDL that became
    /// a table and its indexes runs them all and answers OK).
    pub(crate) statements: Vec<String>,
    pub(crate) kind: Kind,
    /// `SQL_CALC_FOUND_ROWS`: the count query `FOUND_ROWS()` then reports.
    pub(crate) found_rows: Option<String>,
    /// How many `?` the statement had.
    pub(crate) params: usize,
    /// Foreign keys held back while `foreign_key_checks` is off, to be
    /// added when it is switched on again (a dump creates its tables in an
    /// order its keys do not follow).
    pub(crate) deferred: Vec<String>,
}

/// The words that never start an implicit alias.
const NOT_ALIAS: &[&str] = &[
    "END", "NULL", "TRUE", "FALSE", "FROM", "AS", "AND", "OR", "NOT", "IS", "IN", "LIKE", "THEN", "ELSE", "WHEN",
    "CASE", "BETWEEN", "DESC", "ASC", "DISTINCT", "UNKNOWN", "INTERVAL", "BINARY", "COLLATE", "ESCAPE",
];

/// Where the select list ends.
const SELECT_LIST_END: &[&str] = &[
    "FROM", "INTO", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "FOR", "UNION", "WINDOW", "LOCK", "EXCEPT",
    "INTERSECT",
];

const SELECT_MODIFIERS: &[&str] = &[
    "SQL_NO_CACHE",
    "SQL_CACHE",
    "HIGH_PRIORITY",
    "STRAIGHT_JOIN",
    "SQL_SMALL_RESULT",
    "SQL_BIG_RESULT",
    "SQL_BUFFER_RESULT",
];

pub(crate) struct Cx<'a> {
    pub(crate) env: &'a dyn Env,
    /// `?` becomes `$n` (a prepared statement); otherwise it is refused.
    pub(crate) placeholders: bool,
    /// Inside CREATE/ALTER: `TRUE`/`FALSE` stay what they are.
    ddl: bool,
    /// Select-list aliases, as written: a later reference keeps their case.
    aliases: Vec<String>,
}

impl<'a> Cx<'a> {
    pub(crate) fn new(env: &'a dyn Env, placeholders: bool) -> Self {
        Cx {
            env,
            placeholders,
            ddl: false,
            aliases: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

/// The unit of `INTERVAL n unit` in the engine's words.
fn interval_unit(word: &str) -> Option<(&'static str, i64)> {
    Some(match word.to_ascii_uppercase().as_str() {
        "MICROSECOND" => ("microseconds", 1),
        "SECOND" => ("seconds", 1),
        "MINUTE" => ("minutes", 1),
        "HOUR" => ("hours", 1),
        "DAY" => ("days", 1),
        "WEEK" => ("days", 7),
        "MONTH" => ("months", 1),
        "QUARTER" => ("months", 3),
        "YEAR" => ("years", 1),
        _ => return None,
    })
}

/// `n unit` (after `INTERVAL`) as an interval expression.
fn interval_expr(amount: Vec<Token>, unit: &Token) -> Result<Vec<Token>, MyError> {
    let word = unit.ident().unwrap_or("");
    let Some((unit, factor)) = interval_unit(word) else {
        return Err(MyError::unsupported(format!("INTERVAL unit {word}")));
    };
    // A plain literal becomes one interval literal; anything else is a
    // multiple of one unit.
    if let [Token { tok: Tok::Num(n), .. }] = amount.as_slice() {
        if let Ok(v) = n.parse::<i64>() {
            return Ok(vec![Token::raw(format!("interval '{} {}'", v * factor, unit))]);
        }
    }
    if let [Token { tok: Tok::Str(s), .. }] = amount.as_slice() {
        if let Ok(v) = String::from_utf8_lossy(s).trim().parse::<i64>() {
            return Ok(vec![Token::raw(format!("interval '{} {}'", v * factor, unit))]);
        }
    }
    let mut out = wrap(amount);
    out.push(Token::op("*"));
    out.push(Token::raw(format!("interval '{} {}'", factor, unit)));
    Ok(wrap(out))
}

/// Where the operand ending just before `i` starts.
fn operand_start(tokens: &[Token], i: usize) -> Option<usize> {
    if i == 0 {
        return None;
    }
    let mut j = i - 1;
    if tokens[j].is_op(")") {
        let mut depth = 0i32;
        loop {
            if tokens[j].is_op(")") {
                depth += 1;
            } else if tokens[j].is_op("(") {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            if j == 0 {
                return None;
            }
            j -= 1;
        }
        // A function call: its name is part of the operand.
        if j > 0 && matches!(tokens[j - 1].tok, Tok::Word(_) | Tok::Raw(_)) && !tokens[j - 1].is_word("AND") && !tokens[j - 1].is_word("OR") {
            j -= 1;
        }
    }
    // Qualified names: `a.b.c`.
    while j >= 2 && tokens[j - 1].is_op(".") {
        j -= 2;
    }
    Some(j)
}

/// Where the operand starting at `i` ends (inclusive).
fn operand_end(tokens: &[Token], i: usize) -> Option<usize> {
    let mut j = i;
    let first = tokens.get(j)?;
    if first.is_op("-") || first.is_op("+") {
        j += 1;
    }
    let t = tokens.get(j)?;
    if t.is_op("(") {
        return matching_paren(tokens, j);
    }
    if matches!(t.tok, Tok::Word(_) | Tok::Raw(_)) && tokens.get(j + 1).is_some_and(|n| n.is_op("(")) {
        return matching_paren(tokens, j + 1);
    }
    while tokens.get(j + 1).is_some_and(|n| n.is_op(".")) && tokens.get(j + 2).is_some() {
        j += 2;
    }
    Some(j)
}

/// A MySQL JSON path (`$.a.b[0]`) as a chain of `->` steps.
fn json_path_steps(path: &str) -> Result<Vec<String>, MyError> {
    let bad = || MyError::new(3143, "42000", format!("Invalid JSON path expression '{path}'"));
    let mut rest = path.trim().strip_prefix('$').ok_or_else(bad)?;
    let mut steps = Vec::new();
    while !rest.is_empty() {
        if let Some(r) = rest.strip_prefix('.') {
            if let Some(r) = r.strip_prefix('"') {
                let end = r.find('"').ok_or_else(bad)?;
                steps.push(format!("'{}'", r[..end].replace('\'', "''")));
                rest = &r[end + 1..];
            } else {
                let end = r.find(['.', '[']).unwrap_or(r.len());
                let key = &r[..end];
                if key.is_empty() || key == "*" {
                    return Err(MyError::unsupported("JSON path wildcards"));
                }
                steps.push(format!("'{}'", key.replace('\'', "''")));
                rest = &r[end..];
            }
        } else if let Some(r) = rest.strip_prefix('[') {
            let end = r.find(']').ok_or_else(bad)?;
            let index: i64 = r[..end].trim().parse().map_err(|_| MyError::unsupported("JSON path wildcards or ranges"))?;
            steps.push(index.to_string());
            rest = &r[end + 1..];
        } else {
            return Err(bad());
        }
    }
    Ok(steps)
}

fn json_chain(doc: Vec<Token>, path: &str, unquote: bool) -> Result<Vec<Token>, MyError> {
    let steps = json_path_steps(path)?;
    let mut out = wrap(doc);
    if steps.is_empty() {
        if unquote {
            out.push(Token::raw("#>> '{}'"));
        }
        return Ok(wrap(out));
    }
    let last = steps.len() - 1;
    for (i, step) in steps.into_iter().enumerate() {
        out.push(Token::op(if unquote && i == last { "->>" } else { "->" }));
        out.push(Token::raw(step));
    }
    Ok(wrap(out))
}

fn str_of(tokens: &[Token]) -> Option<String> {
    match tokens {
        [Token { tok: Tok::Str(s), .. }] => Some(String::from_utf8_lossy(s).into_owned()),
        _ => None,
    }
}

/// MySQL's `DATE_FORMAT` pattern as a `to_char` one.
fn date_format_pattern(fmt: &str) -> Result<String, MyError> {
    let mut out = String::new();
    let mut literal = String::new();
    // Punctuation stands for itself in a to_char template; only text with
    // letters (which could read as a pattern) is quoted.
    let flush = |out: &mut String, literal: &mut String| {
        if literal.is_empty() {
            return;
        }
        if literal.chars().any(|c| c.is_alphanumeric() || c == '"' || c == '\\') {
            out.push('"');
            out.push_str(&literal.replace('"', "\\\""));
            out.push('"');
        } else {
            out.push_str(literal);
        }
        literal.clear();
    };
    let mut chars = fmt.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            literal.push(c);
            continue;
        }
        let Some(spec) = chars.next() else {
            literal.push('%');
            break;
        };
        let pg = match spec {
            'Y' => "YYYY",
            'y' => "YY",
            'm' => "MM",
            'c' => "FMMM",
            'd' => "DD",
            'e' => "FMDD",
            'H' => "HH24",
            'k' => "FMHH24",
            'h' | 'I' => "HH12",
            'l' => "FMHH12",
            'i' => "MI",
            's' | 'S' => "SS",
            'f' => "US",
            'p' => "AM",
            'M' => "FMMonth",
            'b' => "Mon",
            'W' => "FMDay",
            'a' => "Dy",
            'j' => "DDD",
            'T' => "HH24:MI:SS",
            'r' => "HH12:MI:SS AM",
            '%' => {
                literal.push('%');
                continue;
            }
            other => return Err(MyError::unsupported(format!("DATE_FORMAT specifier %{other}"))),
        };
        flush(&mut out, &mut literal);
        out.push_str(pg);
    }
    flush(&mut out, &mut literal);
    Ok(out)
}

/// A MySQL type name (`CAST(x AS …)`, a column's type) in the engine's words.
/// `tokens` is the type as written; returns the type and whether it is an
/// unsigned or zerofill one (already accounted for).
pub(crate) fn map_type(tokens: &[Token], for_cast: bool) -> Result<String, MyError> {
    let name = tokens
        .first()
        .and_then(|t| t.ident())
        .ok_or_else(|| MyError::syntax("a type name was expected"))?
        .to_ascii_uppercase();
    // Parenthesised arguments, as text.
    let mut args: Vec<String> = Vec::new();
    let mut idx = 1;
    if tokens.get(1).is_some_and(|t| t.is_op("(")) {
        let close = matching_paren(tokens, 1).ok_or_else(|| MyError::syntax("unbalanced type arguments"))?;
        for part in split_commas(&tokens[2..close]) {
            args.push(render(&part)?);
        }
        idx = close + 1;
    }
    let rest: Vec<String> = tokens[idx..].iter().filter_map(|t| t.upper()).collect();
    let unsigned = rest.iter().any(|w| w == "UNSIGNED");
    let arg = |i: usize| args.get(i).cloned();
    let precision = |default_p: &str, default_s: &str| -> String {
        match (arg(0), arg(1)) {
            (Some(p), Some(s)) => format!("numeric({p},{s})"),
            (Some(p), None) => format!("numeric({p},0)"),
            _ => format!("numeric({default_p},{default_s})"),
        }
    };
    Ok(match name.as_str() {
        // CAST targets.
        "SIGNED" | "UNSIGNED" if for_cast => "bigint".into(),
        "CHAR" | "NCHAR" | "VARCHAR" if for_cast => match arg(0) {
            Some(n) => format!("varchar({n})"),
            None => "text".into(),
        },
        "BINARY" | "VARBINARY" if for_cast => "bytea".into(),
        // Integers. Unsigned widens to the next type that holds its range;
        // BIGINT UNSIGNED stays bigint (values past 2^63 are refused).
        "TINYINT" if args.first().map(|a| a == "1").unwrap_or(false) && !unsigned => "boolean".into(),
        "BOOL" | "BOOLEAN" => "boolean".into(),
        "TINYINT" => "smallint".into(),
        "SMALLINT" => {
            if unsigned { "integer".into() } else { "smallint".into() }
        }
        "MEDIUMINT" => "integer".into(),
        "INT" | "INTEGER" | "INT4" => {
            if unsigned { "bigint".into() } else { "integer".into() }
        }
        "BIGINT" | "INT8" => "bigint".into(),
        "SERIAL" => "bigserial".into(),
        "DECIMAL" | "NUMERIC" | "DEC" | "FIXED" => precision("10", "0"),
        "FLOAT" => match arg(0).and_then(|p| p.parse::<u32>().ok()) {
            Some(p) if p > 24 => "double precision".into(),
            _ => "real".into(),
        },
        "DOUBLE" | "REAL" | "FLOAT8" => "double precision".into(),
        "FLOAT4" => "real".into(),
        "BIT" => match arg(0).as_deref() {
            None | Some("1") => "boolean".into(),
            _ => "bigint".into(),
        },
        // Strings. CHAR is varchar: MySQL strips CHAR's padding on read,
        // where a padded type would keep it.
        "CHAR" | "NCHAR" | "CHARACTER" => match arg(0) {
            Some(n) => format!("varchar({n})"),
            None => "varchar(1)".into(),
        },
        "VARCHAR" | "NVARCHAR" | "VARCHARACTER" => match arg(0) {
            Some(n) => format!("varchar({n})"),
            None => "text".into(),
        },
        "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" | "LONG" => "text".into(),
        "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => "bytea".into(),
        "ENUM" | "SET" => "text".into(),
        // Time.
        "DATE" => "date".into(),
        "DATETIME" => "timestamp".into(),
        "TIMESTAMP" => "timestamptz".into(),
        "TIME" => "time".into(),
        "YEAR" => "smallint".into(),
        "JSON" => "jsonb".into(),
        "UUID" => "uuid".into(),
        "DOUBLE PRECISION" => "double precision".into(),
        other => return Err(MyError::unsupported(format!("the {other} type"))),
    })
}

/// Rewrites one expression-level token list.
pub(crate) fn rewrite_expr(tokens: Vec<Token>, cx: &mut Cx) -> Result<Vec<Token>, MyError> {
    let mut out: Vec<Token> = Vec::with_capacity(tokens.len() + 4);
    let mut i = 0usize;
    let n = tokens.len();
    while i < n {
        let t = &tokens[i];
        // Function calls: a word directly followed by `(`.
        if let Tok::Word(word) = &t.tok {
            if tokens.get(i + 1).is_some_and(|p| p.is_op("(")) && !out.last().is_some_and(|p| p.is_op(".")) {
                let close = matching_paren(&tokens, i + 1).ok_or_else(|| MyError::syntax("unbalanced parentheses"))?;
                if let Some(replacement) = rewrite_call(word, &tokens[i + 2..close], cx)? {
                    out.extend(replacement);
                    i = close + 1;
                    continue;
                }
                // Not one of ours: the name stays, the arguments are
                // rewritten.
                out.push(t.clone());
                let mut open = tokens[i + 1].clone();
                open.space = false;
                out.push(open);
                let inner = rewrite_expr(tokens[i + 2..close].to_vec(), cx)?;
                out.extend(inner);
                out.push(tokens[close].clone());
                i = close + 1;
                continue;
            }
        }
        match &t.tok {
            Tok::Param(_) if !cx.placeholders => {
                return Err(MyError::syntax(
                    "a '?' placeholder is only allowed in a prepared statement",
                ));
            }
            Tok::SysVar(name) => {
                let value = cx
                    .env
                    .system_variable(name)
                    .ok_or_else(|| MyError::new(1193, "HY000", format!("Unknown system variable '{name}'")))?;
                let mut v = value.token();
                v.space = t.space;
                out.push(v);
            }
            Tok::UserVar(name) => {
                if tokens.get(i + 1).is_some_and(|p| p.is_op(":=")) {
                    return Err(MyError::unsupported("assigning a user variable inside a statement"));
                }
                let mut v = cx.env.user_variable(name).token();
                v.space = t.space;
                out.push(v);
            }
            Tok::Op(o) => match o.as_str() {
                "<=>" => out.push(Token::raw("IS NOT DISTINCT FROM")),
                "||" if !cx.env.mode().pipes_as_concat => out.push(Token::word("OR")),
                "&&" => out.push(Token::word("AND")),
                "!" => out.push(Token::word("NOT")),
                "/" => {
                    out.push(Token::op("*"));
                    out.push(Token::new(Tok::Num("1.0".into())));
                    out.push(Token::op("/"));
                }
                "->" | "->>" => {
                    // `col->'$.a'`: the path is a MySQL JSON path.
                    let Some(path) = tokens.get(i + 1).and_then(|p| str_of(std::slice::from_ref(p))) else {
                        out.push(t.clone());
                        i += 1;
                        continue;
                    };
                    let start = operand_start(&out, out.len()).ok_or_else(|| MyError::syntax("'->' needs a column"))?;
                    let doc: Vec<Token> = out.drain(start..).collect();
                    out.extend(json_chain(doc, &path, o == "->>")?);
                    i += 2;
                    continue;
                }
                ":=" => return Err(MyError::unsupported("':=' outside SET")),
                _ => out.push(t.clone()),
            },
            Tok::Word(w) => {
                let upper = w.to_ascii_uppercase();
                match upper.as_str() {
                    "TRUE" | "FALSE" if !cx.ddl && !matches!(out.last(), Some(p) if p.is_word("IS") || p.is_word("NOT")) => {
                        let mut v = Token::new(Tok::Num(if upper == "TRUE" { "1" } else { "0" }.into()));
                        v.space = t.space;
                        out.push(v);
                    }
                    "DIV" => {
                        let start = operand_start(&out, out.len()).ok_or_else(|| MyError::syntax("DIV needs operands"))?;
                        let left: Vec<Token> = out.drain(start..).collect();
                        let rest = rewrite_expr(tokens[i + 1..].to_vec(), cx)?;
                        let end = operand_end(&rest, 0).ok_or_else(|| MyError::syntax("DIV needs operands"))?;
                        let right: Vec<Token> = rest[..=end].to_vec();
                        // MySQL's DIV is a BIGINT whatever its operands.
                        let mut quotient = call("div", vec![left, right]);
                        quotient.push(Token::raw("::bigint"));
                        out.extend(wrap(quotient));
                        out.extend(rest[end + 1..].iter().cloned());
                        return Ok(out);
                    }
                    "MOD" => out.push(Token::op("%")),
                    "XOR" => return Err(MyError::unsupported("XOR")),
                    "REGEXP" | "RLIKE" => {
                        if out.last().is_some_and(|p| p.is_word("NOT")) {
                            out.pop();
                            out.push(Token::op("!~"));
                        } else {
                            out.push(Token::op("~"));
                        }
                    }
                    "BINARY" if tokens.get(i + 1).is_some_and(|p| !p.is_op("(") && !p.is_op(",") && !p.is_op(")")) => {}
                    "COLLATE" => {
                        // The collation name goes too: comparisons here are
                        // binary whatever is asked.
                        i += 2;
                        continue;
                    }
                    "INTERVAL" => {
                        let end = operand_end(&tokens, i + 1).ok_or_else(|| MyError::syntax("INTERVAL needs a value"))?;
                        let amount = rewrite_expr(tokens[i + 1..=end].to_vec(), cx)?;
                        let unit = tokens.get(end + 1).ok_or_else(|| MyError::syntax("INTERVAL needs a unit"))?;
                        out.extend(interval_expr(amount, unit)?);
                        i = end + 2;
                        continue;
                    }
                    "CURRENT_TIME" => out.push(Token::raw("(now()::time)")),
                    "CURRENT_USER" => out.push(literal_str(&format!("{}@%", cx.env.user()))),
                    "UNKNOWN" if out.last().is_some_and(|p| p.is_word("IS")) => out.push(Token::word("NULL")),
                    "DUAL" if out.last().is_some_and(|p| p.is_word("FROM")) => {
                        out.pop();
                    }
                    "LOCK" if kw(&tokens, i + 1, "IN") && kw(&tokens, i + 2, "SHARE") && kw(&tokens, i + 3, "MODE") => {
                        out.push(Token::raw("FOR SHARE"));
                        i += 4;
                        continue;
                    }
                    "USE" | "FORCE" | "IGNORE"
                        if (kw(&tokens, i + 1, "INDEX") || kw(&tokens, i + 1, "KEY"))
                            && matches!(out.last().map(|p| &p.tok), Some(Tok::Word(_) | Tok::Quoted(_))) =>
                    {
                        // An index hint: `USE INDEX [FOR JOIN] (…)`.
                        let mut j = i + 2;
                        if kw(&tokens, j, "FOR") {
                            j += 1;
                            if kw(&tokens, j, "ORDER") || kw(&tokens, j, "GROUP") {
                                j += 2;
                            } else {
                                j += 1;
                            }
                        }
                        if tokens.get(j).is_some_and(|p| p.is_op("(")) {
                            let close = matching_paren(&tokens, j).ok_or_else(|| MyError::syntax("unbalanced index hint"))?;
                            i = close + 1;
                            continue;
                        }
                        out.push(t.clone());
                    }
                    "STRAIGHT_JOIN" => out.push(Token::word("JOIN")),
                    "MATCH" => return Err(MyError::unsupported("MATCH … AGAINST (full-text search)")),
                    _ => {
                        if let Some(alias) = cx.aliases.iter().find(|a| a.eq_ignore_ascii_case(w)) {
                            if !out.last().is_some_and(|p| p.is_op(".")) && !tokens.get(i + 1).is_some_and(|p| p.is_op(".")) {
                                let mut v = Token::raw(quote_ident(alias));
                                v.space = t.space;
                                out.push(v);
                                i += 1;
                                continue;
                            }
                        }
                        out.push(t.clone());
                    }
                }
            }
            Tok::Quoted(name) => {
                if let Some(alias) = cx.aliases.iter().find(|a| a.eq_ignore_ascii_case(name)) {
                    if !out.last().is_some_and(|p| p.is_op(".")) && !tokens.get(i + 1).is_some_and(|p| p.is_op(".")) {
                        let mut v = Token::raw(quote_ident(alias));
                        v.space = t.space;
                        out.push(v);
                        i += 1;
                        continue;
                    }
                }
                out.push(t.clone());
            }
            _ => out.push(t.clone()),
        }
        i += 1;
    }
    Ok(out)
}

/// One function call, rewritten; `None` when the name is not one MySQL and
/// the engine spell differently.
fn rewrite_call(name: &str, inner: &[Token], cx: &mut Cx) -> Result<Option<Vec<Token>>, MyError> {
    let upper = name.to_ascii_uppercase();
    let raw_args = || split_commas(inner);
    let args = |cx: &mut Cx| -> Result<Vec<Vec<Token>>, MyError> {
        let mut out = Vec::new();
        for a in split_commas(inner) {
            out.push(rewrite_expr(a, cx)?);
        }
        Ok(out)
    };
    let arity = |args: &Vec<Vec<Token>>, want: &[usize]| -> Result<(), MyError> {
        if want.contains(&args.len()) {
            Ok(())
        } else {
            Err(MyError::new(
                1582,
                "42000",
                format!("Incorrect parameter count in the call to native function '{name}'"),
            ))
        }
    };
    let extract = |field: &str, args: Vec<Vec<Token>>| -> Vec<Token> {
        let mut out = vec![Token::raw(format!("(extract({field} from"))];
        out.extend(wrap(args.into_iter().next().unwrap_or_default()));
        out.push(Token::raw(")::int)"));
        out
    };
    Ok(Some(match upper.as_str() {
        "IFNULL" | "NVL" => call("coalesce", args(cx)?),
        "IF" => {
            let a = args(cx)?;
            arity(&a, &[3])?;
            let mut it = a.into_iter();
            let (c, x, y) = (it.next().unwrap(), it.next().unwrap(), it.next().unwrap());
            let mut out = vec![Token::raw("(CASE WHEN")];
            out.extend(c);
            out.push(Token::word("THEN"));
            out.extend(x);
            out.push(Token::word("ELSE"));
            out.extend(y);
            out.push(Token::raw("END)"));
            out
        }
        "ISNULL" => {
            let a = args(cx)?;
            arity(&a, &[1])?;
            let mut out = wrap(a.into_iter().next().unwrap());
            out.push(Token::raw("IS NULL"));
            wrap(out)
        }
        "NOW" | "CURRENT_TIMESTAMP" | "LOCALTIME" | "LOCALTIMESTAMP" => vec![Token::raw("now()")],
        "SYSDATE" => vec![Token::raw("clock_timestamp()")],
        "CURDATE" | "CURRENT_DATE" => vec![Token::raw("current_date")],
        "CURTIME" | "CURRENT_TIME" => vec![Token::raw("(now()::time)")],
        "UTC_TIMESTAMP" => vec![Token::raw("(now() at time zone 'UTC')")],
        "UTC_DATE" => vec![Token::raw("((now() at time zone 'UTC')::date)")],
        "UTC_TIME" => vec![Token::raw("((now() at time zone 'UTC')::time)")],
        "UNIX_TIMESTAMP" => {
            let a = args(cx)?;
            arity(&a, &[0, 1])?;
            match a.into_iter().next() {
                None => vec![Token::raw("(floor(extract(epoch from now()))::bigint)")],
                Some(x) => {
                    let mut out = vec![Token::raw("(floor(extract(epoch from")];
                    out.extend(wrap(x));
                    out.push(Token::raw("))::bigint)"));
                    out
                }
            }
        }
        "FROM_UNIXTIME" => {
            let a = args(cx)?;
            arity(&a, &[1, 2])?;
            let mut it = a.into_iter();
            let mut ts = vec![Token::raw("(to_timestamp")];
            ts.extend(wrap(it.next().unwrap()));
            ts.push(Token::raw("at time zone 'UTC')"));
            match it.next() {
                None => ts,
                Some(fmt) => {
                    let pattern = str_of(&fmt).ok_or_else(|| MyError::unsupported("a FROM_UNIXTIME format that is not a literal"))?;
                    call("to_char", vec![ts, vec![literal_str(&date_format_pattern(&pattern)?)]])
                }
            }
        }
        "LAST_INSERT_ID" => {
            if !inner.is_empty() {
                return Err(MyError::unsupported("LAST_INSERT_ID(expr)"));
            }
            vec![Token::raw(format!("({}::bigint)", cx.env.last_insert_id().to_string()))]
        }
        "DATABASE" | "SCHEMA" => vec![literal_str(cx.env.database_name())],
        "VERSION" => vec![literal_str(super::SERVER_VERSION)],
        "CONNECTION_ID" => vec![Token::raw(format!("({}::bigint)", cx.env.connection_id().to_string()))],
        "USER" | "CURRENT_USER" | "SESSION_USER" | "SYSTEM_USER" => {
            vec![literal_str(&format!("{}@%", cx.env.user()))]
        }
        "FOUND_ROWS" => vec![Token::raw(format!("({}::bigint)", cx.env.found_rows().to_string()))],
        "ROW_COUNT" => vec![Token::raw(format!("({}::bigint)", cx.env.row_count().to_string()))],
        "LCASE" => call("lower", args(cx)?),
        "UCASE" => call("upper", args(cx)?),
        "LENGTH" | "OCTET_LENGTH" => call("octet_length", args(cx)?),
        "CHARACTER_LENGTH" => call("char_length", args(cx)?),
        "MID" | "SUBSTR" => call("substr", args(cx)?),
        "SUBSTRING" if !inner.iter().any(|t| t.is_word("FROM")) => call("substr", args(cx)?),
        "LOCATE" => {
            let a = args(cx)?;
            arity(&a, &[2, 3])?;
            let mut it = a.into_iter();
            let (sub, s) = (it.next().unwrap(), it.next().unwrap());
            match it.next() {
                None => call("strpos", vec![s, sub]),
                Some(pos) => {
                    // strpos of the tail, moved back to a position in `s`.
                    let tail = call("substr", vec![s, pos.clone()]);
                    let found = call("strpos", vec![tail, sub]);
                    let mut out = vec![Token::raw("(CASE WHEN")];
                    out.extend(found.clone());
                    out.push(Token::raw("= 0 THEN 0 ELSE"));
                    out.extend(found);
                    out.push(Token::op("+"));
                    out.extend(wrap(pos));
                    out.push(Token::raw("- 1 END)"));
                    out
                }
            }
        }
        "INSTR" => {
            let a = args(cx)?;
            arity(&a, &[2])?;
            call("strpos", a)
        }
        "RAND" => {
            if !inner.is_empty() {
                return Err(MyError::unsupported("RAND(seed)"));
            }
            vec![Token::raw("random()")]
        }
        "TRUNCATE" => call("trunc", args(cx)?),
        "LOG" => {
            let a = args(cx)?;
            if a.len() == 1 { call("ln", a) } else { call("log", a) }
        }
        "LOG2" => {
            let mut a = args(cx)?;
            arity(&a, &[1])?;
            a.insert(0, vec![Token::new(Tok::Num("2".into()))]);
            call("log", a)
        }
        "LOG10" => call("log", args(cx)?),
        "POW" => call("power", args(cx)?),
        "SPACE" => {
            let mut a = args(cx)?;
            arity(&a, &[1])?;
            a.insert(0, vec![literal_str(" ")]);
            call("repeat", a)
        }
        "UUID" => vec![Token::raw("(gen_random_uuid()::text)")],
        "ANY_VALUE" => call("min", args(cx)?),
        "STRCMP" => {
            let a = args(cx)?;
            arity(&a, &[2])?;
            let (x, y) = (wrap(a[0].clone()), wrap(a[1].clone()));
            let mut out = vec![Token::raw("(CASE WHEN")];
            out.extend(x.clone());
            out.push(Token::op("<"));
            out.extend(y.clone());
            out.push(Token::raw("THEN -1 WHEN"));
            out.extend(x);
            out.push(Token::op(">"));
            out.extend(y);
            out.push(Token::raw("THEN 1 ELSE 0 END)"));
            out
        }
        "GROUP_CONCAT" => {
            // GROUP_CONCAT([DISTINCT] e, … [ORDER BY …] [SEPARATOR 's'])
            let mut body: Vec<Token> = inner.to_vec();
            let mut separator = ",".to_string();
            if let Some(at) = find_top(&body, 0, &["SEPARATOR"]) {
                separator = str_of(&body[at + 1..]).ok_or_else(|| MyError::syntax("SEPARATOR needs a string"))?;
                body.truncate(at);
            }
            let mut order: Vec<Token> = Vec::new();
            if let Some(at) = find_top(&body, 0, &["ORDER"]) {
                order = rewrite_expr(body[at..].to_vec(), cx)?;
                body.truncate(at);
            }
            let distinct = body.first().is_some_and(|t| t.is_word("DISTINCT"));
            if distinct {
                body.remove(0);
            }
            let mut parts = Vec::new();
            for part in split_commas(&body) {
                parts.push(rewrite_expr(part, cx)?);
            }
            let value = if parts.len() == 1 {
                parts.pop().unwrap()
            } else {
                call("concat", parts)
            };
            let mut arg = Vec::new();
            if distinct {
                arg.push(Token::word("DISTINCT"));
            }
            let mut cast = wrap(value);
            cast.push(Token::raw("::text"));
            arg.extend(cast);
            let mut out = vec![Token::raw("string_agg"), Token::op("(")];
            out[1].space = false;
            out.extend(arg);
            out.push(Token::op(","));
            out.push(literal_str(&separator));
            out.extend(order);
            out.push(Token::op(")"));
            out
        }
        "DATE" => {
            let a = args(cx)?;
            arity(&a, &[1])?;
            let mut out = wrap(a.into_iter().next().unwrap());
            out.push(Token::raw("::date"));
            wrap(out)
        }
        "TIME" if !inner.is_empty() => {
            let a = args(cx)?;
            arity(&a, &[1])?;
            let mut out = wrap(a.into_iter().next().unwrap());
            out.push(Token::raw("::time"));
            wrap(out)
        }
        "YEAR" => extract("year", args(cx)?),
        "MONTH" => extract("month", args(cx)?),
        "DAY" | "DAYOFMONTH" => extract("day", args(cx)?),
        "HOUR" => extract("hour", args(cx)?),
        "MINUTE" => extract("minute", args(cx)?),
        "SECOND" => extract("second", args(cx)?),
        "QUARTER" => extract("quarter", args(cx)?),
        "DAYOFYEAR" => extract("doy", args(cx)?),
        "DAYOFWEEK" => {
            let mut out = extract("dow", args(cx)?);
            out.push(Token::raw("+ 1"));
            wrap(out)
        }
        "DATEDIFF" => {
            let a = args(cx)?;
            arity(&a, &[2])?;
            let mut it = a.into_iter();
            let mut out = wrap(it.next().unwrap());
            out.push(Token::raw("::date -"));
            out.extend(wrap(it.next().unwrap()));
            out.push(Token::raw("::date"));
            wrap(out)
        }
        "DATE_ADD" | "ADDDATE" | "DATE_SUB" | "SUBDATE" => {
            let raw = raw_args();
            if raw.len() != 2 {
                return Err(MyError::syntax(format!("{name} takes a date and an interval")));
            }
            let date = rewrite_expr(raw[0].clone(), cx)?;
            let second = &raw[1];
            let interval = if second.first().is_some_and(|t| t.is_word("INTERVAL")) {
                rewrite_expr(second.clone(), cx)?
            } else {
                // `ADDDATE(d, n)`: n days.
                interval_expr(rewrite_expr(second.clone(), cx)?, &Token::word("DAY"))?
            };
            let mut out = wrap(date);
            out.push(Token::op(if upper.contains("SUB") { "-" } else { "+" }));
            out.extend(interval);
            wrap(out)
        }
        "TIMESTAMPDIFF" => {
            let raw = raw_args();
            if raw.len() != 3 {
                return Err(MyError::syntax("TIMESTAMPDIFF takes a unit and two times"));
            }
            let unit = raw[0].first().and_then(|t| t.upper()).unwrap_or_default();
            let divisor = match unit.as_str() {
                "MICROSECOND" => "0.000001",
                "SECOND" => "1",
                "MINUTE" => "60",
                "HOUR" => "3600",
                "DAY" => "86400",
                "WEEK" => "604800",
                other => return Err(MyError::unsupported(format!("TIMESTAMPDIFF by {other}"))),
            };
            let a = rewrite_expr(raw[1].clone(), cx)?;
            let b = rewrite_expr(raw[2].clone(), cx)?;
            let mut out = vec![Token::raw("(trunc(extract(epoch from")];
            let mut diff = wrap(b);
            diff.push(Token::raw("::timestamp -"));
            diff.extend(wrap(a));
            diff.push(Token::raw("::timestamp"));
            out.extend(wrap(diff));
            out.push(Token::raw(format!(") / {divisor})::bigint)")));
            out
        }
        "DATE_FORMAT" => {
            let a = args(cx)?;
            arity(&a, &[2])?;
            let pattern = str_of(&a[1]).ok_or_else(|| MyError::unsupported("a DATE_FORMAT format that is not a literal"))?;
            // MySQL takes a date in a string too.
            let mut date = wrap(a[0].clone());
            date.push(Token::raw("::timestamp"));
            call("to_char", vec![date, vec![literal_str(&date_format_pattern(&pattern)?)]])
        }
        "STR_TO_DATE" => {
            let a = args(cx)?;
            arity(&a, &[2])?;
            let pattern = str_of(&a[1]).ok_or_else(|| MyError::unsupported("a STR_TO_DATE format that is not a literal"))?;
            let mut out = call("to_timestamp", vec![a[0].clone(), vec![literal_str(&date_format_pattern(&pattern)?)]]);
            out.push(Token::raw("at time zone 'UTC'"));
            wrap(out)
        }
        "JSON_EXTRACT" => {
            let a = args(cx)?;
            arity(&a, &[2])?;
            let path = str_of(&a[1]).ok_or_else(|| MyError::unsupported("a JSON path that is not a literal"))?;
            json_chain(a[0].clone(), &path, false)?
        }
        "JSON_UNQUOTE" => {
            let raw = raw_args();
            if raw.len() == 1 {
                let first = &raw[0];
                if first.first().is_some_and(|t| t.is_word("JSON_EXTRACT")) && first.get(1).is_some_and(|t| t.is_op("(")) {
                    let close = matching_paren(first, 1).ok_or_else(|| MyError::syntax("unbalanced parentheses"))?;
                    let inner_args = split_commas(&first[2..close]);
                    if inner_args.len() == 2 {
                        let doc = rewrite_expr(inner_args[0].clone(), cx)?;
                        let path = str_of(&inner_args[1]).ok_or_else(|| MyError::unsupported("a JSON path that is not a literal"))?;
                        return Ok(Some(json_chain(doc, &path, true)?));
                    }
                }
                let mut out = wrap(rewrite_expr(first.clone(), cx)?);
                out.push(Token::raw("#>> '{}'"));
                wrap(out)
            } else {
                return Err(MyError::syntax("JSON_UNQUOTE takes one argument"));
            }
        }
        "JSON_OBJECT" => call("jsonb_build_object", args(cx)?),
        "JSON_ARRAY" => call("jsonb_build_array", args(cx)?),
        "CAST" => {
            let at = find_top(inner, 0, &["AS"]).ok_or_else(|| MyError::syntax("CAST needs AS"))?;
            let value = rewrite_expr(inner[..at].to_vec(), cx)?;
            let target = map_type(&inner[at + 1..], true)?;
            let mut out = vec![Token::raw("CAST"), Token::op("(")];
            out[1].space = false;
            out.extend(value);
            out.push(Token::raw(format!("AS {target})")));
            out
        }
        "CONVERT" => {
            if let Some(at) = find_top(inner, 0, &["USING"]) {
                wrap(rewrite_expr(inner[..at].to_vec(), cx)?)
            } else {
                let raw = raw_args();
                if raw.len() != 2 {
                    return Err(MyError::syntax("CONVERT takes a value and a type"));
                }
                let value = rewrite_expr(raw[0].clone(), cx)?;
                let target = map_type(&raw[1], true)?;
                let mut out = vec![Token::raw("CAST"), Token::op("(")];
                out[1].space = false;
                out.extend(value);
                out.push(Token::raw(format!("AS {target})")));
                out
            }
        }
        "VALUES" => {
            // `VALUES(col)` in ON DUPLICATE KEY UPDATE.
            let a = raw_args();
            if a.len() == 1 && a[0].len() == 1 && a[0][0].ident().is_some() {
                vec![Token::raw(format!("excluded.{}", ident_sql(a[0][0].ident().unwrap())))]
            } else {
                return Ok(None);
            }
        }
        "MATCH" => return Err(MyError::unsupported("MATCH … AGAINST (full-text search)")),
        _ => return Ok(None),
    }))
}

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

/// The text a select-list item is named by in MySQL: as written.
fn written_text(sql: &[u8], item: &[Token]) -> String {
    let (Some(first), Some(last)) = (item.first(), item.last()) else {
        return String::new();
    };
    if first.span.1 == 0 || last.span.1 < first.span.0 || last.span.1 > sql.len() {
        return String::new();
    }
    String::from_utf8_lossy(&sql[first.span.0..last.span.1]).into_owned()
}

/// Rewrites a SELECT (or the SELECT of an INSERT … SELECT / UNION branch).
/// `name_items` names unaliased items as MySQL would (the outermost select).
pub(crate) fn rewrite_select(
    sql: &[u8],
    tokens: Vec<Token>,
    cx: &mut Cx,
    name_items: bool,
    calc_found_rows: &mut bool,
) -> Result<Vec<Token>, MyError> {
    let mut out = Vec::new();
    let mut i = 0usize;
    // Leading `(`, then SELECT.
    while tokens.get(i).is_some_and(|t| t.is_op("(")) {
        out.push(tokens[i].clone());
        i += 1;
    }
    if !kw(&tokens, i, "SELECT") {
        return rewrite_expr(tokens, cx);
    }
    out.push(tokens[i].clone());
    i += 1;
    // Modifiers.
    loop {
        let Some(t) = tokens.get(i) else { break };
        if t.is_word("SQL_CALC_FOUND_ROWS") {
            *calc_found_rows = true;
            i += 1;
        } else if SELECT_MODIFIERS.iter().any(|m| t.is_word(m)) {
            i += 1;
        } else if t.is_word("DISTINCT") || t.is_word("ALL") || t.is_word("DISTINCTROW") {
            out.push(if t.is_word("DISTINCTROW") { Token::word("DISTINCT") } else { t.clone() });
            i += 1;
        } else {
            break;
        }
    }
    let end = find_top(&tokens, i, SELECT_LIST_END).unwrap_or(tokens.len());
    // Collect the aliases first: they may be referred to later.
    let items = split_commas(&tokens[i..end]);
    let mut prepared: Vec<(Vec<Token>, Option<String>)> = Vec::new();
    for item in items {
        let (expr, alias) = split_alias(&item);
        if let Some(a) = &alias {
            cx.aliases.push(a.clone());
        }
        prepared.push((expr, alias.or_else(|| {
            if !name_items {
                return None;
            }
            implicit_name(sql, &item)
        })));
    }
    let saved = std::mem::take(&mut cx.aliases);
    for (k, (expr, name)) in prepared.into_iter().enumerate() {
        if k > 0 {
            out.push(Token::op(","));
        }
        let rewritten = rewrite_expr(expr, cx)?;
        out.extend(rewritten);
        if let Some(name) = name {
            out.push(Token::word("AS"));
            out.push(Token::raw(quote_ident(&name)));
        }
    }
    cx.aliases = saved;
    let mut rest: Vec<Token> = tokens[end..].to_vec();
    // `LIMIT a, b` → `LIMIT b OFFSET a`, at depth 0 of this select.
    if let Some(at) = find_top(&rest, 0, &["LIMIT"]) {
        let tail = rest.split_off(at + 1);
        let parts = split_commas(&tail);
        if parts.len() == 2 {
            rest.extend(parts[1].clone());
            rest.push(Token::word("OFFSET"));
            rest.extend(parts[0].clone());
        } else {
            rest.extend(tail);
        }
    }
    // UNION branches: each is a select of its own, not named.
    if let Some(u) = find_top(&rest, 0, &["UNION", "EXCEPT", "INTERSECT"]) {
        let mut j = u + 1;
        while kw(&rest, j, "ALL") || kw(&rest, j, "DISTINCT") {
            j += 1;
        }
        let head = rewrite_expr(rest[..j].to_vec(), cx)?;
        let mut found = false;
        let tail = rewrite_select(sql, rest[j..].to_vec(), cx, false, &mut found)?;
        out.extend(head);
        out.extend(tail);
        cx.aliases.clear();
        return Ok(out);
    }
    let rest = rewrite_expr(rest, cx)?;
    out.extend(rest);
    cx.aliases.clear();
    Ok(out)
}

/// An item's expression and its explicit alias, if it has one.
fn split_alias(item: &[Token]) -> (Vec<Token>, Option<String>) {
    let n = item.len();
    if n >= 3 && item[n - 2].is_word("AS") {
        if let Some(name) = alias_name(&item[n - 1]) {
            return (item[..n - 2].to_vec(), Some(name));
        }
    }
    if n >= 2 {
        let last = &item[n - 1];
        let before = &item[n - 2];
        let operand_before = matches!(before.tok, Tok::Word(_) | Tok::Quoted(_) | Tok::Num(_) | Tok::Str(_) | Tok::SysVar(_) | Tok::UserVar(_))
            || before.is_op(")");
        let implicit = match &last.tok {
            Tok::Quoted(_) | Tok::Str(_) => true,
            Tok::Word(w) => !NOT_ALIAS.iter().any(|k| k.eq_ignore_ascii_case(w)),
            _ => false,
        };
        let before_is_keyword = before.upper().is_some_and(|w| NOT_ALIAS.contains(&w.as_str()) && w != "END" && w != "NULL" && w != "TRUE" && w != "FALSE");
        if implicit && operand_before && !before_is_keyword && !before.is_op(".") {
            if let Some(name) = alias_name(last) {
                return (item[..n - 1].to_vec(), Some(name));
            }
        }
    }
    (item.to_vec(), None)
}

fn alias_name(t: &Token) -> Option<String> {
    match &t.tok {
        Tok::Word(w) | Tok::Quoted(w) => Some(w.clone()),
        Tok::Str(s) => Some(String::from_utf8_lossy(s).into_owned()),
        _ => None,
    }
}

/// What MySQL names an unaliased item: a column by its name as written, any
/// other expression by its text. `None` when the engine already gives the
/// same name (a lower-case column) or the item is a `*`.
fn implicit_name(sql: &[u8], item: &[Token]) -> Option<String> {
    if item.last().is_some_and(|t| t.is_op("*")) {
        return None;
    }
    // `col`, `t.col`, `` `t`.`col` ``.
    let is_column = item.iter().enumerate().all(|(k, t)| {
        if k % 2 == 1 { t.is_op(".") } else { matches!(t.tok, Tok::Word(_) | Tok::Quoted(_)) }
    }) && item.len() % 2 == 1;
    if is_column {
        let name = item.last()?.ident()?.to_string();
        if name == name.to_lowercase() {
            return None;
        }
        return Some(name);
    }
    let text = written_text(sql, item);
    if text.is_empty() {
        return None;
    }
    // MySQL caps a generated name at 64 characters.
    Some(text.chars().take(64).collect())
}

// ---------------------------------------------------------------------------
// INSERT / REPLACE / UPDATE / DELETE
// ---------------------------------------------------------------------------

/// The table's auto-increment column: one whose default draws a sequence.
pub(crate) fn auto_increment_column(database: &Database, table: &str) -> Option<String> {
    let view = database.schema_view(table)?;
    let schema = view.schema();
    schema
        .defaults
        .iter()
        .filter(|d| !d.is_deleted)
        .find(|d| d.expression.trim_start().to_ascii_lowercase().starts_with("nextval("))
        .map(|d| d.column.clone())
}

fn table_columns(database: &Database, table: &str) -> Option<Vec<String>> {
    let view = database.schema_view(table)?;
    Some(
        view.schema()
            .fields
            .iter()
            .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
            .map(|f| f.name.clone())
            .collect(),
    )
}

fn primary_key(database: &Database, table: &str) -> Option<String> {
    let view = database.schema_view(table)?;
    view.schema().primary_key.clone()
}

/// The table name a statement names at `tokens[i]` (`db.t` keeps `t`), and
/// the index after it.
fn table_name_at(tokens: &[Token], i: usize) -> Option<(String, usize)> {
    let first = tokens.get(i)?.ident()?;
    if tokens.get(i + 1).is_some_and(|t| t.is_op(".")) {
        let second = tokens.get(i + 2)?.ident()?;
        return Some((second.to_lowercase(), i + 3));
    }
    Some((first.to_lowercase(), i + 1))
}

fn rewrite_insert(sql: &[u8], tokens: Vec<Token>, cx: &mut Cx, replace: bool) -> Result<Forward, MyError> {
    let mut i = 1usize;
    let mut ignore = false;
    while let Some(t) = tokens.get(i) {
        if t.is_word("LOW_PRIORITY") || t.is_word("DELAYED") || t.is_word("HIGH_PRIORITY") {
            i += 1;
        } else if t.is_word("IGNORE") {
            ignore = true;
            i += 1;
        } else {
            break;
        }
    }
    if kw(&tokens, i, "INTO") {
        i += 1;
    }
    let (table, mut j) = table_name_at(&tokens, i).ok_or_else(|| MyError::syntax("INSERT needs a table"))?;
    let table_tokens = tokens[i..j].to_vec();
    if kw(&tokens, j, "PARTITION") {
        return Err(MyError::unsupported("INSERT … PARTITION"));
    }
    let database = cx.env.database().clone();
    let mut columns: Option<Vec<String>> = None;
    if tokens.get(j).is_some_and(|t| t.is_op("(")) {
        let close = matching_paren(&tokens, j).ok_or_else(|| MyError::syntax("unbalanced column list"))?;
        // `INSERT INTO t (SELECT …)` is a parenthesised query, not columns.
        if !kw(&tokens, j + 1, "SELECT") {
            let mut list = Vec::new();
            for part in split_commas(&tokens[j + 1..close]) {
                let name = part.last().and_then(|t| t.ident()).ok_or_else(|| MyError::syntax("a column name was expected"))?;
                list.push(name.to_lowercase());
            }
            columns = Some(list);
            j = close + 1;
        }
    }
    let mut out: Vec<Token> = vec![Token::word("INSERT"), Token::word("INTO")];
    out.extend(table_tokens);

    // The body: VALUES, SET, or a query; then ON DUPLICATE KEY UPDATE.
    let dup_at = (j..tokens.len()).find(|&k| kw(&tokens, k, "ON") && kw(&tokens, k + 1, "DUPLICATE"));
    let returning_at = find_top(&tokens, j, &["RETURNING"]);
    let body_end = dup_at.or(returning_at).unwrap_or(tokens.len());
    let mut body: Vec<Token> = tokens[j..body_end].to_vec();
    // `VALUES (…) AS new [(a, b)]`: the alias names the inserted row.
    let mut row_alias: Option<String> = None;
    if let Some(at) = find_top(&body, 0, &["AS"]) {
        if body.first().is_some_and(|t| t.is_word("VALUES") || t.is_word("VALUE")) {
            row_alias = body.get(at + 1).and_then(|t| t.ident()).map(|s| s.to_lowercase());
            if body.get(at + 2).is_some_and(|t| t.is_op("(")) {
                return Err(MyError::unsupported("column aliases on an INSERT row alias"));
            }
            body.truncate(at);
        }
    }
    if body.first().is_some_and(|t| t.is_word("SET")) {
        // `INSERT INTO t SET a = 1, b = 2`.
        let mut names = Vec::new();
        let mut values = Vec::new();
        for part in split_commas(&body[1..]) {
            let eq = part.iter().position(|t| t.is_op("=")).ok_or_else(|| MyError::syntax("SET needs col = value"))?;
            let name = part[..eq].last().and_then(|t| t.ident()).ok_or_else(|| MyError::syntax("a column name was expected"))?;
            names.push(name.to_lowercase());
            values.push(rewrite_expr(part[eq + 1..].to_vec(), cx)?);
        }
        out.push(Token::raw(format!(
            "({})",
            names.iter().map(|c| ident_sql(c)).collect::<Vec<_>>().join(", ")
        )));
        out.push(Token::word("VALUES"));
        let mut row = Vec::new();
        for (k, v) in values.into_iter().enumerate() {
            if k > 0 {
                row.push(Token::op(","));
            }
            row.extend(v);
        }
        out.extend(wrap(row));
        columns = Some(names);
    } else {
        if let Some(cols) = &columns {
            out.push(Token::raw(format!(
                "({})",
                cols.iter().map(|c| ident_sql(c)).collect::<Vec<_>>().join(", ")
            )));
        }
        if body.first().is_some_and(|t| t.is_word("VALUE")) {
            body[0] = Token::word("VALUES");
        }
        if body.first().is_some_and(|t| t.is_word("VALUES")) {
            out.push(Token::word("VALUES"));
            // `VALUES ROW(…)` is MySQL 8's row constructor.
            let rows: Vec<Token> = body[1..].iter().filter(|t| !t.is_word("ROW")).cloned().collect();
            out.extend(rewrite_expr(rows, cx)?);
        } else {
            let mut found = false;
            out.extend(rewrite_select(sql, body, cx, false, &mut found)?);
        }
    }

    if replace {
        // REPLACE: the row that collides is replaced. Every column the
        // statement gives is overwritten; one it does not give would keep
        // its old value where MySQL resets it to its default, so that form
        // is refused rather than answered differently.
        let all = table_columns(&database, &table).ok_or_else(|| no_table(&table))?;
        let given = columns.clone().unwrap_or_else(|| all.clone());
        let missing: Vec<&String> = all.iter().filter(|c| !given.contains(c)).collect();
        let auto = auto_increment_column(&database, &table);
        if missing.iter().any(|c| Some(*c) != auto.as_ref()) {
            return Err(MyError::unsupported(
                "REPLACE that leaves columns out (name every column, or use INSERT … ON DUPLICATE KEY UPDATE)",
            ));
        }
        out.push(Token::raw(format!(
            "ON CONFLICT{} DO UPDATE SET {}",
            conflict_target(&database, &table, Some(&given)),
            given
                .iter()
                .map(|c| format!("{0} = excluded.{0}", ident_sql(c)))
                .collect::<Vec<_>>()
                .join(", ")
        )));
    } else if let Some(at) = dup_at {
        // ON DUPLICATE KEY UPDATE a = VALUES(a), b = new.b, c = c + 1
        let mut k = at + 4;
        if !kw(&tokens, at + 2, "KEY") || !kw(&tokens, at + 3, "UPDATE") {
            return Err(MyError::syntax("ON DUPLICATE KEY UPDATE was expected"));
        }
        let end = returning_at.unwrap_or(tokens.len());
        let mut assignments: Vec<Token> = Vec::new();
        while k < end {
            let t = &tokens[k];
            if let (Some(alias), Some(name)) = (&row_alias, t.ident()) {
                if name.eq_ignore_ascii_case(alias) && tokens.get(k + 1).is_some_and(|d| d.is_op(".")) {
                    assignments.push(Token::raw("excluded"));
                    k += 1;
                    continue;
                }
            }
            assignments.push(t.clone());
            k += 1;
        }
        // Targets are bare names in the engine's SET: `t.a = …` → `a = …`.
        let mut parts = Vec::new();
        for part in split_commas(&assignments) {
            let eq = part.iter().position(|t| t.is_op("=")).ok_or_else(|| MyError::syntax("UPDATE needs col = value"))?;
            let name = part[..eq].last().and_then(|t| t.ident()).ok_or_else(|| MyError::syntax("a column name was expected"))?;
            let mut p = vec![Token::raw(ident_sql(name)), Token::op("=")];
            p.extend(rewrite_expr(part[eq + 1..].to_vec(), cx)?);
            parts.push(p);
        }
        out.push(Token::raw(format!("ON CONFLICT{} DO UPDATE SET", conflict_target(&database, &table, columns.as_deref()))));
        for (n, p) in parts.into_iter().enumerate() {
            if n > 0 {
                out.push(Token::op(","));
            }
            out.extend(p);
        }
    } else if ignore {
        out.push(Token::raw("ON CONFLICT DO NOTHING"));
    }

    let mut auto = None;
    if let Some(at) = returning_at {
        out.extend(rewrite_expr(tokens[at..].to_vec(), cx)?);
    } else if let Some(column) = auto_increment_column(&database, &table) {
        out.push(Token::raw(format!("RETURNING {}", ident_sql(&column))));
        auto = Some(column);
    }
    Ok(Forward {
        statements: vec![render(&out)?],
        kind: if returning_at.is_some() { Kind::Query } else { Kind::Insert { auto } },
        found_rows: None,
        params: 0,
        deferred: Vec::new(),
    })
}

/// Which key an upsert's conflict is on. MySQL's ON DUPLICATE KEY UPDATE
/// fires on whichever unique key collides; the engine wants it named when a
/// table has more than one. The key named is the first (primary first) whose
/// columns the statement gives values for — a key it gives no value for can
/// only collide through its default, which for an auto-increment key never
/// happens. Empty when there is no such key, which leaves the choice to the
/// engine.
fn conflict_target(database: &Database, table: &str, given: Option<&[String]>) -> String {
    let Some(view) = database.schema_view(table) else {
        return String::new();
    };
    let schema = view.schema();
    let given: Vec<String> = match given {
        Some(g) => g.to_vec(),
        None => schema
            .fields
            .iter()
            .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
            .map(|f| f.name.clone())
            .collect(),
    };
    let mut keys: Vec<Vec<String>> = Vec::new();
    if let Some(pk) = &schema.primary_key {
        keys.push(vec![pk.clone()]);
    }
    for key in schema.unique_keys.iter().filter(|k| !k.is_deleted && k.predicate.is_none()) {
        keys.push(key.columns.clone());
    }
    for field in schema.fields.iter().filter(|f| f.is_unique && !f.is_deleted) {
        keys.push(vec![field.name.clone()]);
    }
    keys.into_iter()
        .find(|cols| cols.iter().all(|c| given.contains(c)))
        .map(|cols| format!(" ({})", cols.iter().map(|c| ident_sql(c)).collect::<Vec<_>>().join(", ")))
        .unwrap_or_default()
}

fn no_table(table: &str) -> MyError {
    MyError::new(1146, "42S02", format!("Table '{table}' doesn't exist"))
}

/// `… ORDER BY … LIMIT n` on an UPDATE or DELETE, which the engine has no
/// form of: the rows are chosen by primary key in a subquery.
fn limited_by_key(
    tokens: &[Token],
    table: &str,
    where_at: Option<usize>,
    tail_at: usize,
    cx: &mut Cx,
) -> Result<Vec<Token>, MyError> {
    let database = cx.env.database().clone();
    let Some(key) = primary_key(&database, table) else {
        return Err(MyError::unsupported("UPDATE or DELETE with LIMIT on a table without a primary key"));
    };
    let key = ident_sql(&key);
    let mut sub = vec![Token::raw(format!("{key} IN (SELECT {key} FROM {}", ident_sql(table)))];
    if let Some(w) = where_at {
        sub.extend(rewrite_expr(tokens[w..tail_at].to_vec(), cx)?);
    }
    let mut tail = tokens[tail_at..].to_vec();
    if let Some(l) = find_top(&tail, 0, &["LIMIT"]) {
        if split_commas(&tail[l + 1..]).len() > 1 {
            return Err(MyError::syntax("UPDATE and DELETE take LIMIT n, not LIMIT offset, n"));
        }
    }
    sub.extend(rewrite_expr(std::mem::take(&mut tail), cx)?);
    sub.push(Token::op(")"));
    Ok(sub)
}

fn rewrite_update(tokens: Vec<Token>, cx: &mut Cx) -> Result<Forward, MyError> {
    let mut i = 1usize;
    while tokens.get(i).is_some_and(|t| t.is_word("LOW_PRIORITY") || t.is_word("IGNORE")) {
        i += 1;
    }
    let set_at = find_top(&tokens, i, &["SET"]).ok_or_else(|| MyError::syntax("UPDATE needs SET"))?;
    let target = &tokens[i..set_at];
    let where_at = find_top(&tokens, set_at, &["WHERE"]);
    let tail_at = find_top(&tokens, set_at, &["ORDER", "LIMIT"]);
    let assign_end = where_at.or(tail_at).unwrap_or(tokens.len());

    // `UPDATE a JOIN b ON c SET …`: the engine's `UPDATE a SET … FROM b
    // WHERE c`.
    let join_at = find_top(target, 0, &["JOIN", "INNER"]);
    let mut out = vec![Token::word("UPDATE")];
    let (table, mut from_clause, mut join_condition) = if let Some(jat) = join_at {
        let main = target[..jat].to_vec();
        let (table, _) = table_name_at(&main, 0).ok_or_else(|| MyError::syntax("UPDATE needs a table"))?;
        let mut k = jat;
        if target[k].is_word("INNER") {
            k += 1;
        }
        if !target.get(k).is_some_and(|t| t.is_word("JOIN")) {
            return Err(MyError::unsupported("this form of multi-table UPDATE"));
        }
        let on_at = find_top(target, k, &["ON"]).ok_or_else(|| MyError::unsupported("a multi-table UPDATE without ON"))?;
        if find_top(target, k + 1, &["JOIN"]).is_some_and(|x| x < on_at) || find_top(target, on_at, &["JOIN"]).is_some() {
            return Err(MyError::unsupported("UPDATE across more than two tables"));
        }
        out.extend(rewrite_expr(main, cx)?);
        (
            table,
            Some(rewrite_expr(target[k + 1..on_at].to_vec(), cx)?),
            Some(rewrite_expr(target[on_at + 1..].to_vec(), cx)?),
        )
    } else {
        let (table, _) = table_name_at(target, 0).ok_or_else(|| MyError::syntax("UPDATE needs a table"))?;
        out.extend(rewrite_expr(target.to_vec(), cx)?);
        (table, None, None)
    };
    out.push(Token::word("SET"));
    for (n, part) in split_commas(&tokens[set_at + 1..assign_end]).into_iter().enumerate() {
        let eq = part.iter().position(|t| t.is_op("=")).ok_or_else(|| MyError::syntax("SET needs col = value"))?;
        let name = part[..eq].last().and_then(|t| t.ident()).ok_or_else(|| MyError::syntax("a column name was expected"))?;
        if n > 0 {
            out.push(Token::op(","));
        }
        out.push(Token::raw(ident_sql(name)));
        out.push(Token::op("="));
        out.extend(rewrite_expr(part[eq + 1..].to_vec(), cx)?);
    }
    if let Some(from) = from_clause.take() {
        out.push(Token::word("FROM"));
        out.extend(from);
    }
    let mut conditions: Vec<Vec<Token>> = Vec::new();
    if let Some(c) = join_condition.take() {
        conditions.push(c);
    }
    if let Some(t) = tail_at {
        if join_at.is_some() {
            return Err(MyError::unsupported("ORDER BY or LIMIT on a multi-table UPDATE"));
        }
        conditions.push(limited_by_key(&tokens, &table, where_at.map(|w| w + 1), t, cx)?);
    } else if let Some(w) = where_at {
        conditions.push(rewrite_expr(tokens[w + 1..].to_vec(), cx)?);
    }
    if !conditions.is_empty() {
        out.push(Token::word("WHERE"));
        for (n, c) in conditions.into_iter().enumerate() {
            if n > 0 {
                out.push(Token::word("AND"));
            }
            out.extend(wrap(c));
        }
    }
    Ok(Forward {
        statements: vec![render(&out)?],
        kind: Kind::Write,
        found_rows: None,
        params: 0,
        deferred: Vec::new(),
    })
}

fn rewrite_delete(tokens: Vec<Token>, cx: &mut Cx) -> Result<Forward, MyError> {
    let mut i = 1usize;
    while tokens.get(i).is_some_and(|t| t.is_word("LOW_PRIORITY") || t.is_word("QUICK") || t.is_word("IGNORE")) {
        i += 1;
    }
    let mut out = vec![Token::word("DELETE")];
    if !kw(&tokens, i, "FROM") {
        // `DELETE t1 FROM t1 JOIN t2 ON c WHERE w` →
        // `DELETE FROM t1 USING t2 WHERE c AND w`.
        let from_at = find_top(&tokens, i, &["FROM"]).ok_or_else(|| MyError::syntax("DELETE needs FROM"))?;
        let targets = split_commas(&tokens[i..from_at]);
        if targets.len() != 1 {
            return Err(MyError::unsupported("DELETE from more than one table"));
        }
        let where_at = find_top(&tokens, from_at, &["WHERE"]);
        let sources = &tokens[from_at + 1..where_at.unwrap_or(tokens.len())];
        let join_at = find_top(sources, 0, &["JOIN", "INNER"]).ok_or_else(|| MyError::unsupported("this form of multi-table DELETE"))?;
        let mut k = join_at;
        if sources[k].is_word("INNER") {
            k += 1;
        }
        let on_at = find_top(sources, k, &["ON"]).ok_or_else(|| MyError::unsupported("a multi-table DELETE without ON"))?;
        if find_top(sources, on_at, &["JOIN"]).is_some() {
            return Err(MyError::unsupported("DELETE across more than two tables"));
        }
        let main = &sources[..join_at];
        let target = targets[0].last().and_then(|t| t.ident()).unwrap_or("").to_lowercase();
        let main_names: Vec<String> = main.iter().filter_map(|t| t.ident().map(|s| s.to_lowercase())).collect();
        if !main_names.contains(&target) {
            return Err(MyError::unsupported("a multi-table DELETE whose target is not the first table"));
        }
        out.push(Token::word("FROM"));
        out.extend(rewrite_expr(main.to_vec(), cx)?);
        out.push(Token::word("USING"));
        out.extend(rewrite_expr(sources[k + 1..on_at].to_vec(), cx)?);
        out.push(Token::word("WHERE"));
        out.extend(wrap(rewrite_expr(sources[on_at + 1..].to_vec(), cx)?));
        if let Some(w) = where_at {
            out.push(Token::word("AND"));
            out.extend(wrap(rewrite_expr(tokens[w + 1..].to_vec(), cx)?));
        }
        return Ok(Forward {
            statements: vec![render(&out)?],
            kind: Kind::Write,
            found_rows: None,
            params: 0,
            deferred: Vec::new(),
        });
    }
    let (table, after) = table_name_at(&tokens, i + 1).ok_or_else(|| MyError::syntax("DELETE needs a table"))?;
    let where_at = find_top(&tokens, after, &["WHERE"]);
    let tail_at = find_top(&tokens, after, &["ORDER", "LIMIT"]);
    let head_end = where_at.or(tail_at).unwrap_or(tokens.len());
    out.extend(rewrite_expr(tokens[i..head_end].to_vec(), cx)?);
    if let Some(t) = tail_at {
        out.push(Token::word("WHERE"));
        out.extend(limited_by_key(&tokens, &table, where_at.map(|w| w + 1), t, cx)?);
    } else if let Some(w) = where_at {
        out.extend(rewrite_expr(tokens[w..].to_vec(), cx)?);
    }
    Ok(Forward {
        statements: vec![render(&out)?],
        kind: Kind::Write,
        found_rows: None,
        params: 0,
        deferred: Vec::new(),
    })
}

// ---------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------

pub(crate) use crate::sqlcompat::{index_name, plain_index_name};

/// A column list for an index, prefix lengths (`name(10)`) and ASC/DESC
/// stripped of what the engine does not take.
fn index_columns(tokens: &[Token]) -> Result<Vec<String>, MyError> {
    let mut out = Vec::new();
    for part in split_commas(tokens) {
        let name = part.first().and_then(|t| t.ident()).ok_or_else(|| {
            MyError::unsupported("an index on an expression")
        })?;
        let mut column = ident_sql(name);
        if part.last().is_some_and(|t| t.is_word("DESC")) {
            column.push_str(" DESC");
        }
        out.push(column);
    }
    Ok(out)
}

/// One column definition: name, then type, then attributes. Returns the
/// engine's definition and any CHECK an ENUM adds.
fn column_definition(def: &[Token], cx: &mut Cx) -> Result<(String, Vec<String>), MyError> {
    let name = def.first().and_then(|t| t.ident()).ok_or_else(|| MyError::syntax("a column name was expected"))?;
    let column = ident_sql(name);
    // The type runs to the first attribute keyword.
    const ATTRS: &[&str] = &[
        "NOT", "NULL", "DEFAULT", "AUTO_INCREMENT", "PRIMARY", "UNIQUE", "KEY", "COMMENT", "CHARACTER", "CHARSET",
        "COLLATE", "ON", "REFERENCES", "CHECK", "CONSTRAINT", "GENERATED", "AS", "VISIBLE", "INVISIBLE",
        "COLUMN_FORMAT", "STORAGE", "SRID",
    ];
    let mut k = 1usize;
    let mut type_end = def.len();
    while k < def.len() {
        if def[k].is_op("(") {
            k = matching_paren(def, k).ok_or_else(|| MyError::syntax("unbalanced type"))? + 1;
            continue;
        }
        if ATTRS.iter().any(|a| def[k].is_word(a)) {
            type_end = k;
            break;
        }
        k += 1;
    }
    let type_tokens = &def[1..type_end];
    let type_name = type_tokens.first().and_then(|t| t.upper()).unwrap_or_default();
    let mut checks = Vec::new();
    let mut pg_type = map_type(type_tokens, false)?;
    if type_name == "ENUM" {
        if let Some(close) = matching_paren(type_tokens, 1) {
            let values: Vec<String> = split_commas(&type_tokens[2..close])
                .iter()
                .map(|v| render(v))
                .collect::<Result<_, _>>()?;
            let longest = split_commas(&type_tokens[2..close])
                .iter()
                .filter_map(|v| str_of(v))
                .map(|s| s.chars().count())
                .max()
                .unwrap_or(1);
            pg_type = format!("varchar({})", longest.max(1));
            checks.push(format!("CHECK ({column} IN ({}))", values.join(", ")));
        }
    }
    let mut attrs: Vec<String> = Vec::new();
    let mut auto_increment = false;
    let mut k = type_end;
    while k < def.len() {
        let t = &def[k];
        let upper = t.upper().unwrap_or_default();
        match upper.as_str() {
            "NOT" if kw(def, k + 1, "NULL") => {
                attrs.push("NOT NULL".into());
                k += 2;
            }
            "NULL" => {
                attrs.push("NULL".into());
                k += 1;
            }
            "DEFAULT" => {
                // The default runs to the next attribute.
                let mut e = k + 1;
                if def.get(e).is_some_and(|t| t.is_op("(")) {
                    e = matching_paren(def, e).ok_or_else(|| MyError::syntax("unbalanced DEFAULT"))? + 1;
                } else {
                    e += 1;
                    if def.get(e).is_some_and(|t| t.is_op("(")) {
                        e = matching_paren(def, e).ok_or_else(|| MyError::syntax("unbalanced DEFAULT"))? + 1;
                    }
                }
                let value = &def[k + 1..e];
                let rendered = match value {
                    [v, open, ..] if (v.is_word("CURRENT_TIMESTAMP") || v.is_word("NOW") || v.is_word("LOCALTIMESTAMP")) && open.is_op("(") => {
                        "CURRENT_TIMESTAMP".to_string()
                    }
                    [v] if v.is_word("CURRENT_TIMESTAMP") || v.is_word("LOCALTIMESTAMP") || v.is_word("NOW") => "CURRENT_TIMESTAMP".to_string(),
                    _ => {
                        let previous = std::mem::replace(&mut cx.ddl, true);
                        let r = render(&rewrite_expr(value.to_vec(), cx)?);
                        cx.ddl = previous;
                        r?
                    }
                };
                attrs.push(format!("DEFAULT {rendered}"));
                k = e;
            }
            "AUTO_INCREMENT" => {
                auto_increment = true;
                k += 1;
            }
            "PRIMARY" => {
                attrs.push("PRIMARY KEY".into());
                k += if kw(def, k + 1, "KEY") { 2 } else { 1 };
            }
            "UNIQUE" => {
                attrs.push("UNIQUE".into());
                k += if kw(def, k + 1, "KEY") { 2 } else { 1 };
            }
            "KEY" => {
                attrs.push("PRIMARY KEY".into());
                k += 1;
            }
            "COMMENT" | "COLUMN_FORMAT" | "STORAGE" | "SRID" => k += 2,
            "CHARACTER" => k += 3,
            "CHARSET" | "COLLATE" => k += 2,
            "VISIBLE" | "INVISIBLE" => k += 1,
            "ON" if kw(def, k + 1, "UPDATE") => {
                return Err(MyError::unsupported("ON UPDATE CURRENT_TIMESTAMP (set the column in the UPDATE)"));
            }
            "REFERENCES" | "CHECK" | "CONSTRAINT" => {
                let previous = std::mem::replace(&mut cx.ddl, true);
                let r = render(&rewrite_expr(def[k..].to_vec(), cx)?);
                cx.ddl = previous;
                attrs.push(r?);
                k = def.len();
            }
            "GENERATED" | "AS" => return Err(MyError::unsupported("generated columns in MySQL's syntax")),
            _ => return Err(MyError::syntax(format!("unexpected '{}' in the definition of column {name}", render(std::slice::from_ref(t))?))),
        }
    }
    if auto_increment {
        pg_type = match pg_type.as_str() {
            "smallint" => "smallserial".into(),
            "integer" => "serial".into(),
            "bigint" | "bigserial" => "bigserial".into(),
            other => return Err(MyError::unsupported(format!("AUTO_INCREMENT on a {other} column"))),
        };
        attrs.retain(|a| !a.starts_with("DEFAULT"));
    }
    let mut out = format!("{column} {pg_type}");
    for a in attrs {
        out.push(' ');
        out.push_str(&a);
    }
    Ok((out, checks))
}

/// Table options after the column list: dropped, they describe storage the
/// engine chooses for itself.
fn check_table_options(tokens: &[Token]) -> Result<(), MyError> {
    if find_top(tokens, 0, &["PARTITION"]).is_some() {
        return Err(MyError::unsupported("MySQL's PARTITION BY (use the PostgreSQL form)"));
    }
    if tokens.iter().any(|t| t.is_word("SELECT")) {
        return Err(MyError::unsupported("CREATE TABLE … SELECT"));
    }
    Ok(())
}

fn rewrite_create_table(tokens: Vec<Token>, cx: &mut Cx) -> Result<Forward, MyError> {
    cx.ddl = true;
    let mut i = 1usize;
    let temporary = kw(&tokens, i, "TEMPORARY");
    if temporary {
        i += 1;
    }
    i += 1; // TABLE
    let mut if_not_exists = false;
    if kw(&tokens, i, "IF") && kw(&tokens, i + 1, "NOT") && kw(&tokens, i + 2, "EXISTS") {
        if_not_exists = true;
        i += 3;
    }
    let (table, after) = table_name_at(&tokens, i).ok_or_else(|| MyError::syntax("CREATE TABLE needs a name"))?;
    if kw(&tokens, after, "LIKE") {
        return Err(MyError::unsupported("CREATE TABLE … LIKE"));
    }
    if !tokens.get(after).is_some_and(|t| t.is_op("(")) {
        return Err(MyError::syntax("CREATE TABLE needs a column list"));
    }
    let close = matching_paren(&tokens, after).ok_or_else(|| MyError::syntax("unbalanced column list"))?;
    check_table_options(&tokens[close + 1..])?;

    let mut defs: Vec<String> = Vec::new();
    let mut after_statements: Vec<String> = Vec::new();
    let mut deferred: Vec<String> = Vec::new();
    for part in split_commas(&tokens[after + 1..close]) {
        let Some(first) = part.first() else { continue };
        let upper = first.upper().unwrap_or_default();
        let mut p = 0usize;
        let mut constraint_name: Option<String> = None;
        if upper == "CONSTRAINT" {
            if let Some(n) = part.get(1).and_then(|t| t.ident()) {
                if !["PRIMARY", "UNIQUE", "FOREIGN", "CHECK"].iter().any(|k| n.eq_ignore_ascii_case(k)) || matches!(part[1].tok, Tok::Quoted(_)) {
                    constraint_name = Some(n.to_lowercase());
                    p = 2;
                } else {
                    p = 1;
                }
            }
        }
        let head = part.get(p).and_then(|t| t.upper()).unwrap_or_default();
        let is_column = p == 0 && !matches!(head.as_str(), "PRIMARY" | "UNIQUE" | "KEY" | "INDEX" | "FOREIGN" | "FULLTEXT" | "SPATIAL" | "CHECK")
            || (p == 0 && matches!(first.tok, Tok::Quoted(_)));
        if is_column {
            let (def, checks) = column_definition(&part, cx)?;
            defs.push(def);
            defs.extend(checks);
            continue;
        }
        let paren = part.iter().position(|t| t.is_op("(")).ok_or_else(|| MyError::syntax("a key needs a column list"))?;
        let pclose = matching_paren(&part, paren).ok_or_else(|| MyError::syntax("unbalanced key"))?;
        match head.as_str() {
            "PRIMARY" => {
                defs.push(format!("PRIMARY KEY ({})", index_columns(&part[paren + 1..pclose])?.join(", ")));
            }
            "UNIQUE" => {
                // UNIQUE [KEY|INDEX] [name] (cols)
                let mut q = p + 1;
                if kw(&part, q, "KEY") || kw(&part, q, "INDEX") {
                    q += 1;
                }
                let name = if q < paren { part[q].ident().map(|s| s.to_lowercase()) } else { None }.or(constraint_name);
                let cols = index_columns(&part[paren + 1..pclose])?;
                match name {
                    Some(n) => defs.push(format!("CONSTRAINT {} UNIQUE ({})", ident_sql(&index_name(&table, &n)), cols.join(", "))),
                    None => defs.push(format!("UNIQUE ({})", cols.join(", "))),
                }
            }
            "KEY" | "INDEX" => {
                let name = if p + 1 < paren { part[p + 1].ident().map(|s| s.to_lowercase()) } else { None };
                let cols = index_columns(&part[paren + 1..pclose])?;
                let name = name.unwrap_or_else(|| cols[0].trim_matches('"').split(' ').next().unwrap_or("idx").to_string());
                after_statements.push(format!(
                    "CREATE INDEX {}{} ON {} ({})",
                    if if_not_exists { "IF NOT EXISTS " } else { "" },
                    ident_sql(&index_name(&table, &name)),
                    ident_sql(&table),
                    cols.join(", ")
                ));
            }
            "FULLTEXT" | "SPATIAL" => {
                return Err(MyError::unsupported(format!("{head} indexes")));
            }
            "FOREIGN" => {
                // FOREIGN KEY [index name] (cols) REFERENCES …
                let cols = index_columns(&part[paren + 1..pclose])?;
                let rest = render(&rewrite_expr(part[pclose + 1..].to_vec(), cx)?)?;
                let named = constraint_name.map(|n| format!("CONSTRAINT {} ", ident_sql(&n))).unwrap_or_default();
                if cx.env.system_variable("foreign_key_checks") == Some(Value::Int(0)) {
                    deferred.push(format!(
                        "ALTER TABLE {} ADD {named}FOREIGN KEY ({}) {rest}",
                        ident_sql(&table),
                        cols.join(", ")
                    ));
                } else {
                    defs.push(format!("{named}FOREIGN KEY ({}) {rest}", cols.join(", ")));
                }
            }
            "CHECK" => {
                let named = constraint_name.map(|n| format!("CONSTRAINT {} ", ident_sql(&n))).unwrap_or_default();
                defs.push(format!("{named}{}", render(&rewrite_expr(part[p..].to_vec(), cx)?)?));
            }
            _ => return Err(MyError::syntax(format!("unexpected '{head}' in CREATE TABLE"))),
        }
    }
    let mut statements = vec![format!(
        "CREATE {}TABLE {}{} ({})",
        if temporary { "TEMPORARY " } else { "" },
        if if_not_exists { "IF NOT EXISTS " } else { "" },
        ident_sql(&table),
        defs.join(", ")
    )];
    statements.extend(after_statements);
    Ok(Forward {
        statements,
        kind: Kind::Ddl,
        found_rows: None,
        params: 0,
        deferred,
    })
}

fn rewrite_alter_table(tokens: Vec<Token>, cx: &mut Cx) -> Result<Forward, MyError> {
    cx.ddl = true;
    let mut i = 1usize;
    if kw(&tokens, i, "IGNORE") || kw(&tokens, i, "ONLINE") {
        i += 1;
    }
    i += 1; // TABLE
    let (table, after) = table_name_at(&tokens, i).ok_or_else(|| MyError::syntax("ALTER TABLE needs a name"))?;
    let t = ident_sql(&table);
    let database = cx.env.database().clone();
    let mut statements = Vec::new();
    for spec in split_commas(&tokens[after..]) {
        let Some(first) = spec.first() else { continue };
        let head = first.upper().unwrap_or_default();
        match head.as_str() {
            "ADD" => {
                let mut k = 1usize;
                let mut constraint_name: Option<String> = None;
                if kw(&spec, k, "CONSTRAINT") {
                    if let Some(n) = spec.get(k + 1).and_then(|t| t.ident()) {
                        if !["PRIMARY", "UNIQUE", "FOREIGN", "CHECK"].iter().any(|w| n.eq_ignore_ascii_case(w)) {
                            constraint_name = Some(n.to_lowercase());
                            k += 2;
                        } else {
                            k += 1;
                        }
                    }
                }
                let what = spec.get(k).and_then(|t| t.upper()).unwrap_or_default();
                match what.as_str() {
                    "INDEX" | "KEY" | "UNIQUE" | "PRIMARY" | "FULLTEXT" | "SPATIAL" | "FOREIGN" | "CHECK" => {
                        let paren = spec.iter().position(|t| t.is_op("(")).ok_or_else(|| MyError::syntax("a key needs a column list"))?;
                        let pclose = matching_paren(&spec, paren).ok_or_else(|| MyError::syntax("unbalanced key"))?;
                        match what.as_str() {
                            "INDEX" | "KEY" => {
                                let name = if k + 1 < paren { spec[k + 1].ident().map(|s| s.to_lowercase()) } else { None };
                                let cols = index_columns(&spec[paren + 1..pclose])?;
                                let name = name.unwrap_or_else(|| cols[0].trim_matches('"').split(' ').next().unwrap_or("idx").to_string());
                                statements.push(format!("CREATE INDEX {} ON {t} ({})", ident_sql(&index_name(&table, &name)), cols.join(", ")));
                            }
                            "UNIQUE" => {
                                let mut q = k + 1;
                                if kw(&spec, q, "KEY") || kw(&spec, q, "INDEX") {
                                    q += 1;
                                }
                                let name = if q < paren { spec[q].ident().map(|s| s.to_lowercase()) } else { None }.or(constraint_name);
                                let cols = index_columns(&spec[paren + 1..pclose])?;
                                let name = name.unwrap_or_else(|| cols[0].trim_matches('"').to_string());
                                statements.push(format!(
                                    "ALTER TABLE {t} ADD CONSTRAINT {} UNIQUE ({})",
                                    ident_sql(&index_name(&table, &name)),
                                    cols.join(", ")
                                ));
                            }
                            "PRIMARY" => {
                                statements.push(format!("ALTER TABLE {t} ADD PRIMARY KEY ({})", index_columns(&spec[paren + 1..pclose])?.join(", ")));
                            }
                            "FOREIGN" => {
                                let cols = index_columns(&spec[paren + 1..pclose])?;
                                let rest = render(&rewrite_expr(spec[pclose + 1..].to_vec(), cx)?)?;
                                let named = constraint_name.map(|n| format!("CONSTRAINT {} ", ident_sql(&n))).unwrap_or_default();
                                statements.push(format!("ALTER TABLE {t} ADD {named}FOREIGN KEY ({}) {rest}", cols.join(", ")));
                            }
                            "CHECK" => {
                                let named = constraint_name.map(|n| format!("CONSTRAINT {} ", ident_sql(&n))).unwrap_or_default();
                                statements.push(format!("ALTER TABLE {t} ADD {named}{}", render(&rewrite_expr(spec[k..].to_vec(), cx)?)?));
                            }
                            other => return Err(MyError::unsupported(format!("{other} indexes"))),
                        }
                    }
                    _ => {
                        // ADD [COLUMN] def [FIRST | AFTER x], or ADD (def, …)
                        let mut k = 1usize;
                        if kw(&spec, k, "COLUMN") {
                            k += 1;
                        }
                        let mut defs: Vec<Vec<Token>> = Vec::new();
                        if spec.get(k).is_some_and(|t| t.is_op("(")) {
                            let close = matching_paren(&spec, k).ok_or_else(|| MyError::syntax("unbalanced column list"))?;
                            defs.extend(split_commas(&spec[k + 1..close]));
                        } else {
                            let mut def = spec[k..].to_vec();
                            strip_position(&mut def);
                            defs.push(def);
                        }
                        for def in defs {
                            let (column, checks) = column_definition(&def, cx)?;
                            statements.push(format!("ALTER TABLE {t} ADD COLUMN {column}"));
                            for c in checks {
                                statements.push(format!("ALTER TABLE {t} ADD {c}"));
                            }
                        }
                    }
                }
            }
            "DROP" => {
                let what = spec.get(1).and_then(|t| t.upper()).unwrap_or_default();
                match what.as_str() {
                    "INDEX" | "KEY" => {
                        let name = spec.get(2).and_then(|t| t.ident()).ok_or_else(|| MyError::syntax("DROP INDEX needs a name"))?.to_lowercase();
                        statements.push(drop_index_statement(&database, &table, &name));
                    }
                    "FOREIGN" | "CONSTRAINT" | "CHECK" => {
                        let at = if what == "FOREIGN" { 3 } else { 2 };
                        let name = spec.get(at).and_then(|t| t.ident()).ok_or_else(|| MyError::syntax("a constraint name was expected"))?;
                        statements.push(format!("ALTER TABLE {t} DROP CONSTRAINT {}", ident_sql(name)));
                    }
                    "PRIMARY" => return Err(MyError::unsupported("DROP PRIMARY KEY")),
                    _ => {
                        let at = if what == "COLUMN" { 2 } else { 1 };
                        let name = spec.get(at).and_then(|t| t.ident()).ok_or_else(|| MyError::syntax("a column name was expected"))?;
                        statements.push(format!("ALTER TABLE {t} DROP COLUMN {}", ident_sql(name)));
                    }
                }
            }
            "MODIFY" | "CHANGE" => {
                let mut k = 1usize;
                if kw(&spec, k, "COLUMN") {
                    k += 1;
                }
                let old = spec.get(k).and_then(|t| t.ident()).ok_or_else(|| MyError::syntax("a column name was expected"))?.to_lowercase();
                if head == "CHANGE" {
                    k += 1;
                }
                let mut def = spec[k..].to_vec();
                strip_position(&mut def);
                let new = def.first().and_then(|t| t.ident()).unwrap_or(&old).to_lowercase();
                if new != old {
                    statements.push(format!("ALTER TABLE {t} RENAME COLUMN {} TO {}", ident_sql(&old), ident_sql(&new)));
                }
                let (column, checks) = column_definition(&def, cx)?;
                // `name type attrs…` → TYPE, then NULL-ness and default.
                let without_name = column.splitn(2, ' ').nth(1).unwrap_or("").to_string();
                let (pg_type, attrs) = split_type_attrs(&without_name);
                statements.push(format!("ALTER TABLE {t} ALTER COLUMN {} TYPE {pg_type}", ident_sql(&new)));
                if attrs.contains("NOT NULL") {
                    statements.push(format!("ALTER TABLE {t} ALTER COLUMN {} SET NOT NULL", ident_sql(&new)));
                } else {
                    statements.push(format!("ALTER TABLE {t} ALTER COLUMN {} DROP NOT NULL", ident_sql(&new)));
                }
                if let Some(at) = attrs.find("DEFAULT ") {
                    let value = attrs[at + 8..].split(" NOT NULL").next().unwrap_or("").split(" NULL").next().unwrap_or("");
                    statements.push(format!("ALTER TABLE {t} ALTER COLUMN {} SET DEFAULT {}", ident_sql(&new), value.trim()));
                }
                for c in checks {
                    statements.push(format!("ALTER TABLE {t} ADD {c}"));
                }
            }
            "ALTER" => {
                let mut k = 1usize;
                if kw(&spec, k, "COLUMN") {
                    k += 1;
                }
                let name = spec.get(k).and_then(|t| t.ident()).ok_or_else(|| MyError::syntax("a column name was expected"))?;
                let rest = render(&rewrite_expr(spec[k + 1..].to_vec(), cx)?)?;
                statements.push(format!("ALTER TABLE {t} ALTER COLUMN {} {rest}", ident_sql(name)));
            }
            "RENAME" => {
                if kw(&spec, 1, "COLUMN") {
                    let a = spec.get(2).and_then(|t| t.ident()).ok_or_else(|| MyError::syntax("a column name was expected"))?;
                    let b = spec.get(4).and_then(|t| t.ident()).ok_or_else(|| MyError::syntax("a column name was expected"))?;
                    statements.push(format!("ALTER TABLE {t} RENAME COLUMN {} TO {}", ident_sql(a), ident_sql(b)));
                } else if kw(&spec, 1, "INDEX") || kw(&spec, 1, "KEY") {
                    return Err(MyError::unsupported("RENAME INDEX"));
                } else {
                    let mut k = 1;
                    if kw(&spec, k, "TO") || kw(&spec, k, "AS") {
                        k += 1;
                    }
                    let (new, _) = table_name_at(&spec, k).ok_or_else(|| MyError::syntax("a table name was expected"))?;
                    statements.push(format!("ALTER TABLE {t} RENAME TO {}", ident_sql(&new)));
                }
            }
            // Storage options: the engine chooses its own. DISABLE/ENABLE
            // KEYS (mysqldump) defers index maintenance in MySQL; indexes
            // here are always maintained.
            "DISABLE" | "ENABLE" | "ENGINE" | "DEFAULT" | "CHARACTER" | "CHARSET" | "COLLATE" | "CONVERT" | "AUTO_INCREMENT" | "COMMENT"
            | "ROW_FORMAT" | "ALGORITHM" | "LOCK" | "FORCE" | "KEY_BLOCK_SIZE" | "STATS_PERSISTENT" => {}
            other => return Err(MyError::unsupported(format!("ALTER TABLE … {other}"))),
        }
    }
    if statements.is_empty() {
        // Only storage options: nothing to change.
        statements.push(String::new());
    }
    Ok(Forward {
        statements,
        kind: Kind::Ddl,
        found_rows: None,
        params: 0,
        deferred: Vec::new(),
    })
}

/// `FIRST` / `AFTER col` at the end of a column definition: column order is
/// the engine's, so the position is dropped.
fn strip_position(def: &mut Vec<Token>) {
    if def.last().is_some_and(|t| t.is_word("FIRST")) {
        def.pop();
    } else if def.len() >= 2 && def[def.len() - 2].is_word("AFTER") {
        def.truncate(def.len() - 2);
    }
}

fn split_type_attrs(text: &str) -> (String, String) {
    for marker in [" NOT NULL", " NULL", " DEFAULT ", " PRIMARY KEY", " UNIQUE", " CHECK", " REFERENCES"] {
        if let Some(at) = text.find(marker) {
            let (ty, attrs) = text.split_at(at);
            let mut first = ty.to_string();
            let mut rest = attrs.to_string();
            // The earliest marker wins.
            for m in [" NOT NULL", " NULL", " DEFAULT ", " PRIMARY KEY", " UNIQUE", " CHECK", " REFERENCES"] {
                if let Some(a) = text.find(m) {
                    if a < first.len() {
                        first = text[..a].to_string();
                        rest = text[a..].to_string();
                    }
                }
            }
            return (first.trim().to_string(), rest);
        }
    }
    (text.trim().to_string(), String::new())
}

/// The statement that drops index `name` of `table`: its prefixed name if it
/// was made here, a unique constraint if that is what it is.
fn drop_index_statement(database: &Database, table: &str, name: &str) -> String {
    let mangled = index_name(table, name);
    if let Some(view) = database.schema_view(table) {
        let schema = view.schema();
        for key in schema.unique_keys.iter().filter(|k| !k.is_deleted) {
            if key.name == mangled || key.name == name {
                return format!("ALTER TABLE {} DROP CONSTRAINT {}", ident_sql(table), ident_sql(&key.name));
            }
        }
        for index in schema.secondary_indexes.iter().filter(|x| !x.is_deleted) {
            if index.name == mangled {
                return format!("DROP INDEX {}", ident_sql(&mangled));
            }
        }
    }
    format!("DROP INDEX {}", ident_sql(name))
}

fn rewrite_create_index(tokens: Vec<Token>, cx: &mut Cx) -> Result<Forward, MyError> {
    // CREATE [UNIQUE|FULLTEXT|SPATIAL] INDEX name [USING x] ON t (cols) [USING x] [options]
    let mut i = 1usize;
    let unique = kw(&tokens, i, "UNIQUE");
    if unique {
        i += 1;
    }
    if kw(&tokens, i, "FULLTEXT") || kw(&tokens, i, "SPATIAL") {
        return Err(MyError::unsupported("FULLTEXT and SPATIAL indexes"));
    }
    i += 1; // INDEX
    let mut if_not_exists = false;
    if kw(&tokens, i, "IF") && kw(&tokens, i + 1, "NOT") && kw(&tokens, i + 2, "EXISTS") {
        if_not_exists = true;
        i += 3;
    }
    let name = tokens.get(i).and_then(|t| t.ident()).ok_or_else(|| MyError::syntax("CREATE INDEX needs a name"))?.to_lowercase();
    let on = find_top(&tokens, i, &["ON"]).ok_or_else(|| MyError::syntax("CREATE INDEX needs ON"))?;
    let (table, after) = table_name_at(&tokens, on + 1).ok_or_else(|| MyError::syntax("CREATE INDEX needs a table"))?;
    let paren = (after..tokens.len()).find(|&k| tokens[k].is_op("(")).ok_or_else(|| MyError::syntax("CREATE INDEX needs columns"))?;
    let close = matching_paren(&tokens, paren).ok_or_else(|| MyError::syntax("unbalanced column list"))?;
    let cols = index_columns(&tokens[paren + 1..close])?;
    let _ = cx;
    let statement = if unique {
        format!(
            "ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({})",
            ident_sql(&table),
            ident_sql(&index_name(&table, &name)),
            cols.join(", ")
        )
    } else {
        format!(
            "CREATE INDEX {}{} ON {} ({})",
            if if_not_exists { "IF NOT EXISTS " } else { "" },
            ident_sql(&index_name(&table, &name)),
            ident_sql(&table),
            cols.join(", ")
        )
    };
    Ok(Forward {
        statements: vec![statement],
        kind: Kind::Ddl,
        found_rows: None,
        params: 0,
        deferred: Vec::new(),
    })
}

fn rewrite_drop(tokens: Vec<Token>, cx: &mut Cx) -> Result<Forward, MyError> {
    let what = tokens.get(1).and_then(|t| t.upper()).unwrap_or_default();
    let database = cx.env.database().clone();
    let statements = match what.as_str() {
        "TABLE" | "TEMPORARY" => {
            let mut i = if what == "TEMPORARY" { 3 } else { 2 };
            let mut if_exists = false;
            if kw(&tokens, i, "IF") && kw(&tokens, i + 1, "EXISTS") {
                if_exists = true;
                i += 2;
            }
            let mut end = tokens.len();
            if tokens.last().is_some_and(|t| t.is_word("CASCADE") || t.is_word("RESTRICT")) {
                end -= 1;
            }
            let mut names = Vec::new();
            for part in split_commas(&tokens[i..end]) {
                let (name, _) = table_name_at(&part, 0).ok_or_else(|| MyError::syntax("a table name was expected"))?;
                names.push(ident_sql(&name));
            }
            names
                .into_iter()
                .map(|n| format!("DROP TABLE {}{n}", if if_exists { "IF EXISTS " } else { "" }))
                .collect()
        }
        "INDEX" => {
            // DROP INDEX name ON t
            let name = tokens.get(2).and_then(|t| t.ident()).ok_or_else(|| MyError::syntax("DROP INDEX needs a name"))?.to_lowercase();
            let on = find_top(&tokens, 3, &["ON"]).ok_or_else(|| MyError::syntax("DROP INDEX needs ON table"))?;
            let (table, _) = table_name_at(&tokens, on + 1).ok_or_else(|| MyError::syntax("a table name was expected"))?;
            vec![drop_index_statement(&database, &table, &name)]
        }
        "DATABASE" | "SCHEMA" => {
            let mut i = 2;
            let mut if_exists = false;
            if kw(&tokens, i, "IF") && kw(&tokens, i + 1, "EXISTS") {
                if_exists = true;
                i += 2;
            }
            let name = tokens.get(i).and_then(|t| t.ident()).ok_or_else(|| MyError::syntax("a database name was expected"))?;
            vec![format!("DROP DATABASE {}{}", if if_exists { "IF EXISTS " } else { "" }, ident_sql(name))]
        }
        "VIEW" => vec![render(&rewrite_expr(tokens, cx)?)?],
        other => return Err(MyError::unsupported(format!("DROP {other}"))),
    };
    Ok(Forward {
        statements,
        kind: Kind::Ddl,
        found_rows: None,
        params: 0,
        deferred: Vec::new(),
    })
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Translates one statement (already split) for the engine. Session
/// statements (SET, SHOW, USE, transaction control) are not handled here.
pub(crate) fn translate(sql: &[u8], tokens: Vec<Token>, env: &dyn Env, placeholders: bool) -> Result<Forward, MyError> {
    let mut cx = Cx::new(env, placeholders);
    // MySQL's information_schema calls a database a schema: TABLE_SCHEMA
    // holds the database's name, which the engine's views keep in
    // table_catalog (their table_schema is the namespace, `public`).
    let tokens = if tokens.iter().any(|t| t.ident().is_some_and(|i| i.eq_ignore_ascii_case("information_schema"))) {
        tokens
            .into_iter()
            .map(|t| match t.ident() {
                Some(i) if i.eq_ignore_ascii_case("table_schema") => Token {
                    tok: Tok::Word("table_catalog".into()),
                    ..t
                },
                _ => t,
            })
            .collect()
    } else {
        tokens
    };
    let params = tokens.iter().filter(|t| matches!(t.tok, Tok::Param(_))).count();
    let head = tokens.first().and_then(|t| t.upper()).unwrap_or_default();
    let mut forward = match head.as_str() {
        _ if tokens.first().is_some_and(|t| t.is_op("(")) => select_forward(sql, tokens, &mut cx)?,
        "SELECT" => select_forward(sql, tokens, &mut cx)?,
        "WITH" => Forward {
            statements: vec![render(&rewrite_expr(tokens, &mut cx)?)?],
            kind: Kind::Query,
            found_rows: None,
            params: 0,
            deferred: Vec::new(),
        },
        "INSERT" => rewrite_insert(sql, tokens, &mut cx, false)?,
        "REPLACE" => rewrite_insert(sql, tokens, &mut cx, true)?,
        "UPDATE" => rewrite_update(tokens, &mut cx)?,
        "DELETE" => rewrite_delete(tokens, &mut cx)?,
        "CREATE" => {
            let second = tokens.get(1).and_then(|t| t.upper()).unwrap_or_default();
            match second.as_str() {
                "TABLE" | "TEMPORARY" => rewrite_create_table(tokens, &mut cx)?,
                "INDEX" | "UNIQUE" | "FULLTEXT" | "SPATIAL" => rewrite_create_index(tokens, &mut cx)?,
                "DATABASE" | "SCHEMA" => {
                    let mut i = 2;
                    let mut if_not_exists = false;
                    if kw(&tokens, i, "IF") && kw(&tokens, i + 1, "NOT") && kw(&tokens, i + 2, "EXISTS") {
                        if_not_exists = true;
                        i += 3;
                    }
                    let name = tokens.get(i).and_then(|t| t.ident()).ok_or_else(|| MyError::syntax("a database name was expected"))?;
                    Forward {
                        statements: vec![format!(
                            "CREATE DATABASE {}{}",
                            if if_not_exists { "IF NOT EXISTS " } else { "" },
                            ident_sql(name)
                        )],
                        kind: Kind::Ddl,
                        found_rows: None,
                        params: 0,
                        deferred: Vec::new(),
                    }
                }
                "VIEW" | "OR" => {
                    cx.ddl = true;
                    let at = find_top(&tokens, 2, &["AS"]).ok_or_else(|| MyError::syntax("CREATE VIEW needs AS"))?;
                    let mut out = rewrite_expr(tokens[..=at].to_vec(), &mut cx)?;
                    cx.ddl = false;
                    let mut found = false;
                    out.extend(rewrite_select(sql, tokens[at + 1..].to_vec(), &mut cx, false, &mut found)?);
                    Forward {
                        statements: vec![render(&out)?],
                        kind: Kind::Ddl,
                        found_rows: None,
                        params: 0,
                        deferred: Vec::new(),
                    }
                }
                other => return Err(MyError::unsupported(format!("CREATE {other}"))),
            }
        }
        "ALTER" => {
            let second = tokens.get(1).and_then(|t| t.upper()).unwrap_or_default();
            if second == "TABLE" || second == "IGNORE" || second == "ONLINE" {
                rewrite_alter_table(tokens, &mut cx)?
            } else {
                return Err(MyError::unsupported(format!("ALTER {second}")));
            }
        }
        "DROP" => rewrite_drop(tokens, &mut cx)?,
        "TRUNCATE" => {
            let i = if kw(&tokens, 1, "TABLE") { 2 } else { 1 };
            let (table, _) = table_name_at(&tokens, i).ok_or_else(|| MyError::syntax("TRUNCATE needs a table"))?;
            Forward {
                statements: vec![format!("TRUNCATE TABLE {}", ident_sql(&table))],
                kind: Kind::Ddl,
                found_rows: None,
                params: 0,
                deferred: Vec::new(),
            }
        }
        "RENAME" => {
            // RENAME TABLE a TO b, c TO d
            let mut statements = Vec::new();
            for part in split_commas(&tokens[2..]) {
                let to = part.iter().position(|t| t.is_word("TO")).ok_or_else(|| MyError::syntax("RENAME TABLE needs TO"))?;
                let (a, _) = table_name_at(&part, 0).ok_or_else(|| MyError::syntax("a table name was expected"))?;
                let (b, _) = table_name_at(&part, to + 1).ok_or_else(|| MyError::syntax("a table name was expected"))?;
                statements.push(format!("ALTER TABLE {} RENAME TO {}", ident_sql(&a), ident_sql(&b)));
            }
            Forward {
                statements,
                kind: Kind::Ddl,
                found_rows: None,
                params: 0,
                deferred: Vec::new(),
            }
        }
        "EXPLAIN" | "DESCRIBE" | "DESC" => {
            let mut out = vec![Token::word("EXPLAIN")];
            let mut rest = tokens[1..].to_vec();
            while rest.first().is_some_and(|t| t.is_word("FORMAT") || t.is_word("EXTENDED") || t.is_word("PARTITIONS")) {
                if rest.first().is_some_and(|t| t.is_word("FORMAT")) {
                    rest.drain(..3.min(rest.len()));
                } else {
                    rest.remove(0);
                }
            }
            let mut found = false;
            if rest.first().is_some_and(|t| t.is_word("SELECT")) {
                out.extend(rewrite_select(sql, rest, &mut cx, false, &mut found)?);
            } else {
                out.extend(rewrite_expr(rest, &mut cx)?);
            }
            Forward {
                statements: vec![render(&out)?],
                kind: Kind::Query,
                found_rows: None,
                params: 0,
                deferred: Vec::new(),
            }
        }
        _ => Forward {
            statements: vec![render(&rewrite_expr(tokens, &mut cx)?)?],
            kind: Kind::Other,
            found_rows: None,
            params: 0,
            deferred: Vec::new(),
        },
    };
    forward.params = params;
    Ok(forward)
}

fn select_forward(sql: &[u8], tokens: Vec<Token>, cx: &mut Cx) -> Result<Forward, MyError> {
    let mut calc = false;
    let original = tokens.clone();
    let out = rewrite_select(sql, tokens, cx, true, &mut calc)?;
    let found_rows = if calc {
        // The same select without its LIMIT and ORDER BY, counted.
        let mut stripped = original;
        let cut = find_top(&stripped, 0, &["ORDER", "LIMIT"]);
        if let Some(c) = cut {
            stripped.truncate(c);
        }
        stripped.retain(|t| !t.is_word("SQL_CALC_FOUND_ROWS"));
        let mut ignored = false;
        let inner = rewrite_select(sql, stripped, cx, false, &mut ignored)?;
        Some(format!("SELECT count(*) FROM ({}) AS found_rows", render(&inner)?))
    } else {
        None
    };
    Ok(Forward {
        statements: vec![render(&out)?],
        kind: Kind::Query,
        found_rows,
        params: 0,
        deferred: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::lex::lex;

    struct TestEnv {
        database: Arc<Database>,
    }

    impl Env for TestEnv {
        fn system_variable(&self, name: &str) -> Option<Value> {
            match name {
                "version_comment" => Some(Value::Str("RasterizedDB".into())),
                "autocommit" => Some(Value::Int(1)),
                _ => None,
            }
        }
        fn user_variable(&self, _name: &str) -> Value {
            Value::Null
        }
        fn database(&self) -> &Arc<Database> {
            &self.database
        }
        fn database_name(&self) -> &str {
            "app"
        }
        fn last_insert_id(&self) -> u64 {
            42
        }
        fn found_rows(&self) -> u64 {
            0
        }
        fn row_count(&self) -> i64 {
            0
        }
        fn connection_id(&self) -> u32 {
            7
        }
        fn user(&self) -> &str {
            "root"
        }
        fn mode(&self) -> Mode {
            Mode::default()
        }
    }

    fn env() -> TestEnv {
        let catalog = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(crate::core::database::catalog::DatabaseCatalog::open(None, "app"))
            .unwrap();
        TestEnv { database: catalog.get("app").unwrap() }
    }

    fn tr(env: &TestEnv, sql: &str) -> String {
        let tokens = lex(sql.as_bytes(), Mode::default()).unwrap();
        translate(sql.as_bytes(), tokens, env, true).unwrap().statements.join("; ")
    }

    fn err(env: &TestEnv, sql: &str) -> MyError {
        let tokens = lex(sql.as_bytes(), Mode::default()).unwrap();
        translate(sql.as_bytes(), tokens, env, true).unwrap_err()
    }

    #[test]
    fn select_names_and_dialect() {
        let e = env();
        assert_eq!(tr(&e, "SELECT `UserId`, name FROM `Users`"), r#"SELECT userid AS "UserId", name FROM users"#);
        assert_eq!(tr(&e, "SELECT COUNT(*) FROM t"), r#"SELECT COUNT(*) AS "COUNT(*)" FROM t"#);
        assert_eq!(tr(&e, "SELECT a AS Total FROM t ORDER BY total"), r#"SELECT a AS "Total" FROM t ORDER BY "Total""#);
        assert_eq!(tr(&e, "SELECT * FROM t LIMIT 5, 10"), "SELECT * FROM t LIMIT 10 OFFSET 5");
        assert_eq!(tr(&e, "SELECT IFNULL(a, 'x') v FROM t"), r#"SELECT coalesce(a, 'x') AS "v" FROM t"#);
        assert_eq!(tr(&e, "SELECT @@version_comment LIMIT 1"), r#"SELECT 'RasterizedDB' AS "@@version_comment" LIMIT 1"#);
        assert_eq!(tr(&e, "SELECT 7 / 2 AS q"), r#"SELECT 7 * 1.0 / 2 AS "q""#);
        assert_eq!(tr(&e, "SELECT a FROM t WHERE b <=> NULL && c"), "SELECT a FROM t WHERE b IS NOT DISTINCT FROM NULL AND c");
        assert_eq!(tr(&e, "SELECT a FROM t WHERE ok = TRUE"), "SELECT a FROM t WHERE ok = 1");
        assert_eq!(tr(&e, "SELECT a FROM t WHERE x = ? AND y = ?"), "SELECT a FROM t WHERE x = $1 AND y = $2");
        assert_eq!(tr(&e, "SELECT LAST_INSERT_ID() AS id"), r#"SELECT (42::bigint) AS "id""#);
        assert_eq!(tr(&e, "SELECT 1 FROM DUAL"), r#"SELECT 1 AS "1""#);
        assert_eq!(
            tr(&e, "SELECT IF(a > 1, 'y', 'n') AS r FROM t"),
            r#"SELECT (CASE WHEN a > 1 THEN 'y' ELSE 'n' END) AS "r" FROM t"#
        );
        assert_eq!(
            tr(&e, "SELECT GROUP_CONCAT(DISTINCT name ORDER BY name SEPARATOR '; ') AS g FROM t"),
            r#"SELECT string_agg(DISTINCT (name)::text, '; ' ORDER BY name) AS "g" FROM t"#
        );
        assert_eq!(
            tr(&e, "SELECT * FROM t WHERE d > NOW() - INTERVAL 1 DAY"),
            "SELECT * FROM t WHERE d > now() - interval '1 days'"
        );
        assert_eq!(tr(&e, "SELECT doc->>'$.a.b' AS v FROM t"), r#"SELECT ((doc) -> 'a' ->> 'b') AS "v" FROM t"#);
        assert_eq!(tr(&e, "SELECT 7 DIV 2 AS q"), r#"SELECT (div(7, 2)::bigint) AS "q""#);
        assert_eq!(tr(&e, "SELECT CAST(a AS SIGNED) x FROM t"), r#"SELECT CAST(a AS bigint) AS "x" FROM t"#);
        assert_eq!(
            tr(&e, "SELECT DATE_FORMAT(d, '%Y-%m-%d %H:%i') f FROM t"),
            r#"SELECT to_char((d)::timestamp, 'YYYY-MM-DD HH24:MI') AS "f" FROM t"#
        );
        assert_eq!(tr(&e, "SELECT * FROM t USE INDEX (i) WHERE a = 1"), "SELECT * FROM t WHERE a = 1");
        assert_eq!(tr(&e, "SELECT * FROM t LOCK IN SHARE MODE"), "SELECT * FROM t FOR SHARE");
        assert_eq!(
            tr(&e, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'app'"),
            "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'app'"
        );
    }

    #[test]
    fn found_rows_query() {
        let e = env();
        let tokens = lex(b"SELECT SQL_CALC_FOUND_ROWS * FROM t WHERE a = 1 ORDER BY a LIMIT 10", Mode::default()).unwrap();
        let f = translate(b"", tokens, &e, false).unwrap();
        assert_eq!(f.statements[0], "SELECT * FROM t WHERE a = 1 ORDER BY a LIMIT 10");
        assert_eq!(f.found_rows.as_deref(), Some("SELECT count(*) FROM (SELECT * FROM t WHERE a = 1) AS found_rows"));
    }

    #[test]
    fn inserts() {
        let e = env();
        assert_eq!(tr(&e, "INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')"), "INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')");
        assert_eq!(tr(&e, "INSERT t SET a = 1, b = 'x'"), "INSERT INTO t (a, b) VALUES (1, 'x')");
        assert_eq!(tr(&e, "INSERT IGNORE INTO t (a) VALUES (1)"), "INSERT INTO t (a) VALUES (1) ON CONFLICT DO NOTHING");
        assert_eq!(
            tr(&e, "INSERT INTO t (a, b) VALUES (1, 2) ON DUPLICATE KEY UPDATE b = VALUES(b), c = c + 1"),
            "INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT DO UPDATE SET b = excluded.b, c = c + 1"
        );
        assert_eq!(
            tr(&e, "INSERT INTO t (a, b) VALUES (1, 2) AS new ON DUPLICATE KEY UPDATE b = new.b"),
            "INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT DO UPDATE SET b = excluded.b"
        );
        assert_eq!(tr(&e, "INSERT INTO t (a) VALUES (x'00ff')"), r"INSERT INTO t (a) VALUES ('\x00ff'::bytea)");
    }

    #[test]
    fn updates_and_deletes() {
        let e = env();
        assert_eq!(tr(&e, "UPDATE t SET a = 1 WHERE b = 2"), "UPDATE t SET a = 1 WHERE (b = 2)");
        assert_eq!(
            tr(&e, "UPDATE a JOIN b ON a.id = b.id SET a.x = b.y WHERE b.z = 1"),
            "UPDATE a SET x = b.y FROM b WHERE (a.id = b.id) AND (b.z = 1)"
        );
        assert_eq!(tr(&e, "DELETE FROM t WHERE a = 1"), "DELETE FROM t WHERE a = 1");
        assert_eq!(
            tr(&e, "DELETE a FROM a JOIN b ON a.id = b.id WHERE b.z = 1"),
            "DELETE FROM a USING b WHERE (a.id = b.id) AND (b.z = 1)"
        );
    }

    #[test]
    fn create_table() {
        let e = env();
        let sql = "CREATE TABLE IF NOT EXISTS `Users` (
            `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            `Email` VARCHAR(255) NOT NULL COMMENT 'login',
            `active` TINYINT(1) NOT NULL DEFAULT 1,
            `role` ENUM('admin','user') DEFAULT 'user',
            `created_at` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
            `bio` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
            PRIMARY KEY (`id`),
            UNIQUE KEY `users_email_unique` (`email`),
            KEY `idx_created` (`created_at`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";
        let out = tr(&e, sql);
        assert_eq!(
            out,
            "CREATE TABLE IF NOT EXISTS users (id bigserial NOT NULL, email varchar(255) NOT NULL, \
             active boolean NOT NULL DEFAULT 1, role varchar(5) DEFAULT 'user', CHECK (role IN ('admin', 'user')), \
             created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP, bio text, PRIMARY KEY (id), \
             CONSTRAINT users__users_email_unique UNIQUE (email)); \
             CREATE INDEX IF NOT EXISTS users__idx_created ON users (created_at)"
        );
        assert_eq!(err(&e, "CREATE TABLE t (a INT, FULLTEXT KEY f (a))").code, 1235);
        assert_eq!(err(&e, "CREATE TABLE t (a TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)").code, 1235);
    }

    #[test]
    fn alter_table() {
        let e = env();
        assert_eq!(
            tr(&e, "ALTER TABLE t ADD COLUMN c INT NOT NULL DEFAULT 0 AFTER b, ADD INDEX ic (c), ENGINE=InnoDB"),
            "ALTER TABLE t ADD COLUMN c integer NOT NULL DEFAULT 0; CREATE INDEX t__ic ON t (c)"
        );
        assert_eq!(
            tr(&e, "ALTER TABLE t MODIFY c BIGINT NULL"),
            "ALTER TABLE t ALTER COLUMN c TYPE bigint; ALTER TABLE t ALTER COLUMN c DROP NOT NULL"
        );
        assert_eq!(tr(&e, "RENAME TABLE a TO b"), "ALTER TABLE a RENAME TO b");
        assert_eq!(tr(&e, "DROP TABLE IF EXISTS a, `b`"), "DROP TABLE IF EXISTS a; DROP TABLE IF EXISTS b");
    }
}
