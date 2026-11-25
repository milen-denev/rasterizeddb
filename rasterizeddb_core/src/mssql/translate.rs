//! T-SQL, rewritten into the dialect the engine speaks.
//!
//! **Batches.** A T-SQL batch is statements one after another, with `;`
//! optional. [`parse_batch`] finds where each ends by where the next one
//! starts, and builds the control flow (IF/ELSE, BEGIN…END, TRY…CATCH,
//! WHILE) the session runs.
//!
//! **Names.** SQL Server's default collation compares names
//! case-insensitively; names are folded to lower case as for MySQL, and
//! `dbo.` is the engine's one schema. A column is named in a result as it was
//! written in the select list.
//!
//! **`+`.** T-SQL's `+` concatenates strings and adds numbers, by the
//! operands' types. It is sent as `||` when one side is plainly a string (a
//! literal, a string function, a string parameter or variable); two string
//! columns added together are refused by the engine rather than guessed at.

use rclite::Arc;

use super::lex::{Tok, Token, matching_paren, split_commas};
use crate::core::database::Database;
use crate::sqlcompat::{
    call, find_top, ident_sql, kw, literal_str, quote_ident, render_text, wrap,
};

/// A local variable's, or a function's, value.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Value {
    Null,
    Int(i64),
    /// A decimal, as text.
    Num(String),
    Str(String),
    Bytes(Vec<u8>),
}

impl Value {
    pub(crate) fn token(&self) -> Token {
        match self {
            Value::Null => Token::word("NULL"),
            Value::Int(n) => Token::new(Tok::Num(n.to_string())),
            Value::Num(n) => Token::new(Tok::Num(n.clone())),
            Value::Str(s) => Token::new(Tok::Str(s.as_bytes().to_vec())),
            Value::Bytes(b) => Token::new(Tok::Hex(b.clone())),
        }
    }

    pub(crate) fn text(&self) -> Option<String> {
        match self {
            Value::Null => None,
            Value::Int(n) => Some(n.to_string()),
            Value::Num(n) | Value::Str(n) => Some(n.clone()),
            Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        }
    }

    fn is_string(&self) -> bool {
        matches!(self, Value::Str(_))
    }
}

/// An error in SQL Server's terms.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TsqlError {
    pub(crate) number: i32,
    pub(crate) class: u8,
    pub(crate) state: u8,
    pub(crate) message: String,
}

impl TsqlError {
    pub(crate) fn new(number: i32, message: impl Into<String>) -> Self {
        TsqlError {
            number,
            class: 16,
            state: 1,
            message: message.into(),
        }
    }

    pub(crate) fn syntax(message: impl Into<String>) -> Self {
        TsqlError::new(102, message)
    }

    pub(crate) fn unsupported(what: impl Into<String>) -> Self {
        TsqlError::new(
            40000,
            format!(
                "'{}' is not supported by RasterizedDB's SQL Server protocol",
                what.into()
            ),
        )
    }
}

/// A parameter of the statement being translated (sp_executesql, sp_prepare).
#[derive(Clone, Debug)]
pub(crate) struct ParamSlot {
    pub(crate) index: usize,
    pub(crate) string: bool,
}

/// What the translation reads from the session.
pub(crate) trait Env {
    fn database(&self) -> &Arc<Database>;
    fn database_name(&self) -> &str;
    fn user(&self) -> &str;
    fn databases(&self) -> Vec<String> {
        vec![self.database_name().to_string()]
    }
    /// The catalogue's default database: SQL Server's `master` here.
    fn default_database(&self) -> Option<String> {
        None
    }
    fn variable(&self, name: &str) -> Option<Value>;
    fn param(&self, name: &str) -> Option<ParamSlot>;
    fn system(&self, name: &str) -> Option<Value>;
    fn function(&self, name: &str) -> Option<Value>;
    /// The database's stored procedures: (name, definition).
    fn routines(&self) -> Vec<(String, String)> {
        Vec::new()
    }
    /// The table a `#table` of this connection is kept in.
    fn temp_name(&self, name: &str) -> String {
        name.to_lowercase()
    }
    /// A parameter's value as text, where the statement is translated with
    /// its values in hand (`sp_executesql`): `Some(None)` for NULL.
    fn param_text(&self, _name: &str) -> Option<Option<String>> {
        None
    }
    /// Whether a text column declared with no `COLLATE` compares without
    /// case (`MsConfig::case_insensitive`).
    fn case_insensitive(&self) -> bool {
        false
    }
    /// The spelling a relation (`column` None) or its column was declared
    /// with over this protocol (`NAMES_TABLE`); `None` when unknown.
    fn shown(&self, _relation: &str, _column: Option<&str>) -> Option<String> {
        None
    }
}

/// What a statement answers with.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Kind {
    Query,
    /// An INSERT; `identity` is the column whose generated value is
    /// captured (a RETURNING the client does not see).
    Insert {
        identity: Option<String>,
        /// `(table, column)` when the statement writes the identity column
        /// itself (`SET IDENTITY_INSERT … ON`, EF Core's seeding): the
        /// counter then moves past the values written, as SQL Server's does.
        written: Option<(String, String)>,
    },
    /// UPDATE / DELETE, and an INSERT whose OUTPUT returns rows (`Query`
    /// then).
    Write,
    Ddl,
    Other,
}

#[derive(Clone, Debug)]
pub(crate) struct Forward {
    pub(crate) statements: Vec<String>,
    pub(crate) kind: Kind,
    /// A column added after the engine's, row by row (a MERGE's source
    /// position, which an INSERT's RETURNING cannot name): its name and
    /// each row's value.
    pub(crate) extra_column: Option<(String, Vec<String>)>,
    /// The top-level select list's explicit aliases, exactly as written:
    /// shown as typed, where a column read as it is shows its declared
    /// spelling (`[n] AS [n]` over a column `[N]` is `n`).
    pub(crate) aliases: Vec<String>,
    /// A CREATE (`true`) or DROP DATABASE's name as written: its spelling
    /// is kept, as SQL Server shows it (`DB_NAME()`, sys.databases).
    pub(crate) database: Option<(String, bool)>,
}

/// A statement of a batch, with T-SQL's control flow.
#[derive(Clone, Debug)]
pub(crate) enum Stmt {
    Simple(Vec<Token>),
    Block(Vec<Stmt>),
    If {
        condition: Vec<Token>,
        then: Box<Stmt>,
        otherwise: Option<Box<Stmt>>,
    },
    While {
        condition: Vec<Token>,
        body: Box<Stmt>,
    },
    Try {
        body: Vec<Stmt>,
        catch: Vec<Stmt>,
    },
}

// ---------------------------------------------------------------------------
// The batch parser
// ---------------------------------------------------------------------------

const STARTS: &[&str] = &[
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "SET",
    "DECLARE",
    "PRINT",
    "EXEC",
    "EXECUTE",
    "CREATE",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SAVE",
    "RETURN",
    "RAISERROR",
    "THROW",
    "IF",
    "WHILE",
    "USE",
    "MERGE",
    "WITH",
    "GRANT",
    "DENY",
    "REVOKE",
    "BREAK",
    "CONTINUE",
    "END",
    "ELSE",
    "GOTO",
    "WAITFOR",
    "OPEN",
    "CLOSE",
    "FETCH",
    "DEALLOCATE",
    "CHECKPOINT",
    "DBCC",
    "KILL",
    "BACKUP",
    "RESTORE",
];

/// Where the simple statement starting at `start` ends (exclusive).
fn statement_end(tokens: &[Token], start: usize) -> usize {
    let head = tokens[start].upper().unwrap_or_default();
    let mut depth = 0i32;
    let mut case_depth = 0i32;
    let mut seen_set = false;
    let mut main_verb = head != "WITH";
    let mut insert_body = false;
    let mut j = start + 1;
    while j < tokens.len() {
        let t = &tokens[j];
        if t.is_op("(") {
            depth += 1;
        } else if t.is_op(")") {
            depth -= 1;
        } else if depth == 0 {
            if t.is_op(";") {
                return j;
            }
            if let Some(w) = t.upper() {
                let prev = tokens[j - 1].upper().unwrap_or_default();
                let prev_op = tokens[j - 1].is_op(")") || tokens[j - 1].is_op(",");
                let in_case = case_depth > 0;
                match w.as_str() {
                    "CASE" => case_depth += 1,
                    "END" if in_case => case_depth -= 1,
                    _ => {}
                }
                if !STARTS.contains(&w.as_str()) {
                    if w == "VALUES" || w == "DEFAULT" {
                        insert_body = true;
                    }
                    j += 1;
                    continue;
                }
                let continues = match w.as_str() {
                    "SELECT" => {
                        matches!(
                            prev.as_str(),
                            "UNION" | "ALL" | "EXCEPT" | "INTERSECT" | "AS" | "FOR"
                        ) || (head == "INSERT" && !insert_body)
                            || (head == "WITH" && !main_verb)
                            || (head == "CREATE" && prev == "AS")
                    }
                    "SET" => {
                        (!seen_set
                            && (head == "UPDATE" || main_verb_is(tokens, start, j, "UPDATE")))
                            || prev == "UPDATE"
                            || (head == "ALTER" && kw(tokens, start + 1, "DATABASE"))
                    }
                    "INSERT" | "UPDATE" | "DELETE" => {
                        matches!(prev.as_str(), "ON" | "FOR" | "OF" | "THEN" | "INSTEAD")
                            || prev_op && head == "CREATE"
                            || (head == "WITH" && !main_verb)
                    }
                    "DROP" | "ALTER" => head == "ALTER",
                    "IF" => matches!(
                        prev.as_str(),
                        "TABLE"
                            | "VIEW"
                            | "INDEX"
                            | "DATABASE"
                            | "SCHEMA"
                            | "PROCEDURE"
                            | "FUNCTION"
                            | "TRIGGER"
                            | "COLUMN"
                            | "CONSTRAINT"
                    ),
                    "WITH" => {
                        // A table hint or options, not a CTE. A full-text
                        // index's options may go unparenthesised
                        // (`WITH CHANGE_TRACKING AUTO`).
                        (matches!(head.as_str(), "CREATE" | "ALTER") && kw(tokens, start + 1, "FULLTEXT"))
                            || tokens.get(j + 1).is_some_and(|n| n.is_op("("))
                            || matches!(prev.as_str(), "ROLLUP" | "CUBE")
                            || tokens.get(j + 1).is_some_and(|n| {
                                n.is_word("CHECK") || n.is_word("TIES") || n.is_word("GRANT")
                            })
                            // `ALTER DATABASE d SET SINGLE_USER WITH ROLLBACK
                            // IMMEDIATE` / `WITH NO_WAIT`.
                            || (head == "ALTER"
                                && kw(tokens, start + 1, "DATABASE")
                                && tokens.get(j + 1).is_some_and(|n| {
                                    n.is_word("ROLLBACK") || n.is_word("NO_WAIT")
                                }))
                    }
                    "END" | "ELSE" => in_case,
                    // `OFFSET 5 ROWS FETCH NEXT …`: part of a query.
                    "FETCH" => matches!(prev.as_str(), "ROWS" | "ROW"),
                    // A cursor statement names a cursor; otherwise the word
                    // is a column (`SET close = 1`, `SELECT open, close`).
                    "OPEN" | "CLOSE" | "DEALLOCATE" => {
                        let names_cursor =
                            tokens.get(j + 1).is_some_and(|n| {
                                (n.ident().is_some() && !n.is_op(",")) || n.is_word("GLOBAL")
                            }) && !tokens.get(j + 1).and_then(|n| n.upper()).is_some_and(|w| {
                                matches!(
                                    w.as_str(),
                                    "FROM"
                                        | "AS"
                                        | "AND"
                                        | "OR"
                                        | "IS"
                                        | "IN"
                                        | "WHERE"
                                        | "ORDER"
                                        | "GROUP"
                                        | "THEN"
                                        | "END"
                                        | "ELSE"
                                        | "WHEN"
                                )
                            });
                        let after_expr_start = matches!(&tokens[j - 1].tok, Tok::Op(o) if o != ")")
                            || matches!(
                                prev.as_str(),
                                "SELECT"
                                    | "BY"
                                    | "SET"
                                    | "AS"
                                    | "AND"
                                    | "OR"
                                    | "ON"
                                    | "WHERE"
                                    | "THEN"
                                    | "ELSE"
                                    | "WHEN"
                                    | "DISTINCT"
                                    | "TOP"
                            );
                        !names_cursor || after_expr_start
                    }
                    "BEGIN" => false,
                    "ROLLBACK" | "COMMIT" => prev == "ON" || (prev == "WITH" && head == "ALTER"),
                    _ => false,
                };
                if w == "SET" && continues {
                    seen_set = true;
                }
                if matches!(
                    w.as_str(),
                    "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "MERGE"
                ) && head == "WITH"
                {
                    main_verb = true;
                }
                if !continues {
                    return j;
                }
            }
        }
        j += 1;
    }
    tokens.len()
}

fn main_verb_is(tokens: &[Token], start: usize, end: usize, verb: &str) -> bool {
    let head = tokens[start].upper().unwrap_or_default();
    head == "WITH" && find_top(&tokens[start..end], 0, &[verb]).is_some()
}

/// Parses a batch into statements.
pub(crate) fn parse_batch(tokens: &[Token]) -> Result<Vec<Stmt>, TsqlError> {
    let mut i = 0usize;
    let mut out = Vec::new();
    while i < tokens.len() {
        if tokens[i].is_op(";") {
            i += 1;
            continue;
        }
        let (stmt, next) = parse_stmt(tokens, i)?;
        out.push(stmt);
        i = next;
    }
    Ok(out)
}

fn parse_stmt(tokens: &[Token], i: usize) -> Result<(Stmt, usize), TsqlError> {
    let head = tokens[i].upper().unwrap_or_default();
    let next = tokens
        .get(i + 1)
        .and_then(|t| t.upper())
        .unwrap_or_default();
    match head.as_str() {
        "BEGIN" if next == "TRY" => {
            let (body, j) = parse_until(tokens, i + 2, "TRY")?;
            if !(kw(tokens, j, "BEGIN") && kw(tokens, j + 1, "CATCH")) {
                return Err(TsqlError::syntax(
                    "BEGIN TRY needs a BEGIN CATCH after END TRY",
                ));
            }
            let (catch, k) = parse_until(tokens, j + 2, "CATCH")?;
            Ok((Stmt::Try { body, catch }, k))
        }
        "BEGIN"
            if !matches!(
                next.as_str(),
                "TRAN" | "TRANSACTION" | "DISTRIBUTED" | "DIALOG" | "CONVERSATION"
            ) =>
        {
            let (body, j) = parse_until(tokens, i + 1, "")?;
            Ok((Stmt::Block(body), j))
        }
        "IF" => {
            let cond_end = condition_end(tokens, i + 1);
            let condition = tokens[i + 1..cond_end].to_vec();
            if condition.is_empty() || cond_end >= tokens.len() {
                return Err(TsqlError::syntax("IF needs a condition and a statement"));
            }
            let (then, mut j) = parse_stmt(tokens, cond_end)?;
            while tokens.get(j).is_some_and(|t| t.is_op(";")) {
                j += 1;
            }
            let mut otherwise = None;
            if kw(tokens, j, "ELSE") {
                let (e, k) = parse_stmt(tokens, j + 1)?;
                otherwise = Some(Box::new(e));
                j = k;
            }
            Ok((
                Stmt::If {
                    condition,
                    then: Box::new(then),
                    otherwise,
                },
                j,
            ))
        }
        "WHILE" => {
            let cond_end = condition_end(tokens, i + 1);
            let condition = tokens[i + 1..cond_end].to_vec();
            let (body, j) = parse_stmt(tokens, cond_end)?;
            Ok((
                Stmt::While {
                    condition,
                    body: Box::new(body),
                },
                j,
            ))
        }
        "END" | "ELSE" => Err(TsqlError::syntax(format!(
            "Incorrect syntax near the keyword '{head}'."
        ))),
        _ => {
            let end = statement_end(tokens, i);
            Ok((Stmt::Simple(tokens[i..end].to_vec()), end))
        }
    }
}

/// Statements up to the END [suffix] that closes a block; returns them and
/// the index after it.
fn parse_until(
    tokens: &[Token],
    mut i: usize,
    suffix: &str,
) -> Result<(Vec<Stmt>, usize), TsqlError> {
    let mut out = Vec::new();
    loop {
        while tokens.get(i).is_some_and(|t| t.is_op(";")) {
            i += 1;
        }
        if i >= tokens.len() {
            return Err(TsqlError::syntax("BEGIN without END"));
        }
        if tokens[i].is_word("END") {
            let mut j = i + 1;
            if !suffix.is_empty() {
                if !kw(tokens, j, suffix) {
                    return Err(TsqlError::syntax(format!("END {suffix} was expected")));
                }
                j += 1;
            }
            return Ok((out, j));
        }
        let (s, j) = parse_stmt(tokens, i)?;
        out.push(s);
        i = j;
    }
}

/// Where an IF/WHILE condition ends: at the first statement keyword at
/// depth 0 after it began.
fn condition_end(tokens: &[Token], start: usize) -> usize {
    let mut depth = 0i32;
    let mut case_depth = 0i32;
    for (j, t) in tokens.iter().enumerate().skip(start) {
        if t.is_op("(") {
            depth += 1;
        } else if t.is_op(")") {
            depth -= 1;
        } else if depth == 0 && j > start {
            if let Some(w) = t.upper() {
                match w.as_str() {
                    "CASE" => case_depth += 1,
                    "END" if case_depth > 0 => {
                        case_depth -= 1;
                        continue;
                    }
                    _ => {}
                }
                if case_depth == 0 && STARTS.contains(&w.as_str()) && w != "END" && w != "ELSE" {
                    return j;
                }
            }
        }
    }
    tokens.len()
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A T-SQL type (`tokens` from its name on) in the engine's words.
pub(crate) fn map_type(tokens: &[Token]) -> Result<String, TsqlError> {
    let name = tokens
        .first()
        .and_then(|t| t.ident())
        .ok_or_else(|| TsqlError::syntax("a type name was expected"))?
        .to_ascii_uppercase();
    let mut args: Vec<String> = Vec::new();
    if tokens.get(1).is_some_and(|t| t.is_op("(")) {
        let close = matching_paren(tokens, 1)
            .ok_or_else(|| TsqlError::syntax("unbalanced type arguments"))?;
        for part in split_commas(&tokens[2..close]) {
            args.push(render_text(&part));
        }
    }
    let arg = |i: usize| args.get(i).cloned();
    let max = arg(0).is_some_and(|a| a.eq_ignore_ascii_case("max"));
    Ok(match name.as_str() {
        "BIT" => "boolean".into(),
        // 0..=255, as SQL Server's: the engine's unsigned byte, which
        // the front end sends back as a TDS tinyint (`tds::column_type`).
        "TINYINT" => "utinyint".into(),
        "SMALLINT" => "smallint".into(),
        "INT" | "INTEGER" => "integer".into(),
        "BIGINT" => "bigint".into(),
        "DECIMAL" | "NUMERIC" | "DEC" => match (arg(0), arg(1)) {
            (Some(p), Some(s)) => format!("numeric({p},{s})"),
            (Some(p), None) => format!("numeric({p},0)"),
            _ => "numeric(18,0)".into(),
        },
        "MONEY" => "numeric(19,4)".into(),
        "SMALLMONEY" => "numeric(10,4)".into(),
        "FLOAT" => match arg(0).and_then(|p| p.parse::<u32>().ok()) {
            Some(p) if p <= 24 => "real".into(),
            _ => "double precision".into(),
        },
        "REAL" => "real".into(),
        "DATE" => "date".into(),
        "DATETIME" | "DATETIME2" | "SMALLDATETIME" => "timestamp".into(),
        "DATETIMEOFFSET" => "timestamptz".into(),
        "TIME" => "time".into(),
        "CHAR" | "NCHAR" | "VARCHAR" | "NVARCHAR" | "CHARACTER" => {
            if max || arg(0).is_none() && matches!(name.as_str(), "VARCHAR" | "NVARCHAR") {
                "text".into()
            } else {
                format!("varchar({})", arg(0).unwrap_or_else(|| "1".into()))
            }
        }
        "TEXT" | "NTEXT" | "XML" | "SYSNAME" => "text".into(),
        "BINARY" | "VARBINARY" | "IMAGE" => "bytea".into(),
        "UNIQUEIDENTIFIER" => "uuid".into(),
        "TIMESTAMP" | "ROWVERSION" => return Err(TsqlError::unsupported("rowversion columns")),
        other => return Err(TsqlError::unsupported(format!("the {other} type"))),
    })
}

fn is_string_type(tokens: &[Token]) -> bool {
    tokens.first().and_then(|t| t.upper()).is_some_and(|n| {
        matches!(
            n.as_str(),
            "CHAR" | "NCHAR" | "VARCHAR" | "NVARCHAR" | "TEXT" | "NTEXT" | "SYSNAME" | "XML"
        )
    })
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

pub(crate) struct Cx<'a> {
    pub(crate) env: &'a dyn Env,
    aliases: Vec<String>,
    /// The call being rewritten is followed by OVER.
    window_next: bool,
    /// Past a select list (WHERE, GROUP BY, HAVING, ORDER BY): COUNT is
    /// left uncast there, where its type is no column's and the engine
    /// does not take a cast inside HAVING.
    past_select_list: bool,
    /// The FOR XML being rewritten is read back as text with
    /// `.value('.', …)`: its text is unescaped.
    xml_value: bool,
    /// A CREATE TABLE's column `COLLATE` is honoured ([`engine_collation`]);
    /// elsewhere it is dropped, as before.
    collations: bool,
    /// The top-level select list's explicit aliases ([`Forward::aliases`]).
    top_aliases: Vec<String>,
}

impl<'a> Cx<'a> {
    pub(crate) fn new(env: &'a dyn Env) -> Self {
        Cx {
            env,
            aliases: Vec::new(),
            window_next: false,
            past_select_list: false,
            xml_value: false,
            collations: false,
            top_aliases: Vec::new(),
        }
    }
}

const STRING_FUNCTIONS: &[&str] = &[
    "CONCAT",
    "CONCAT_WS",
    "UPPER",
    "LOWER",
    "LTRIM",
    "RTRIM",
    "TRIM",
    "SUBSTRING",
    "LEFT",
    "RIGHT",
    "REPLACE",
    "REPLICATE",
    "REVERSE",
    "STR",
    "FORMAT",
    "DB_NAME",
    "USER_NAME",
    "SUSER_SNAME",
    "SUSER_NAME",
    "HOST_NAME",
    "APP_NAME",
    "QUOTENAME",
    "STRING_AGG",
    "CHAR",
    "NCHAR",
    "SPACE",
    "OBJECT_NAME",
    "SCHEMA_NAME",
    "NEWID",
];

/// Whether the operand in `tokens[start..=end]` is plainly a string.
fn stringish(tokens: &[Token], start: usize, end: usize, cx: &Cx) -> bool {
    let first = &tokens[start];
    match &first.tok {
        Tok::Str(_) => return true,
        Tok::UserVar(name) => {
            if let Some(p) = cx.env.param(name) {
                return p.string;
            }
            return cx.env.variable(name).is_some_and(|v| v.is_string());
        }
        Tok::Raw(r) => return r.starts_with('\''),
        Tok::Word(w) if end > start && tokens[start + 1].is_op("(") => {
            let upper = w.to_ascii_uppercase();
            if STRING_FUNCTIONS.contains(&upper.as_str()) {
                return true;
            }
            if upper == "CAST" || upper == "TRY_CAST" {
                if let Some(at) = find_top(&tokens[start + 2..end], 0, &["AS"]) {
                    return is_string_type(&tokens[start + 2 + at + 1..end]);
                }
            }
            if upper == "CONVERT" || upper == "TRY_CONVERT" {
                return is_string_type(&tokens[start + 2..end]);
            }
            if upper == "ISNULL" || upper == "COALESCE" {
                let args = split_commas(&tokens[start + 2..end]);
                return args
                    .iter()
                    .any(|a| !a.is_empty() && stringish(a, 0, a.len() - 1, cx));
            }
        }
        Tok::Op(o) if o == "(" => {
            if let Some(close) = matching_paren(tokens, start) {
                if close == end && close > start + 1 {
                    let inner = &tokens[start + 1..close];
                    // A parenthesised concatenation is a string.
                    return inner.iter().any(|t| matches!(t.tok, Tok::Str(_)))
                        && !inner.iter().any(|t| t.is_word("SELECT"));
                }
            }
        }
        _ => {}
    }
    false
}

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
    if matches!(t.tok, Tok::Word(_)) && tokens.get(j + 1).is_some_and(|n| n.is_op("(")) {
        return matching_paren(tokens, j + 1);
    }
    while tokens.get(j + 1).is_some_and(|n| n.is_op(".")) && tokens.get(j + 2).is_some() {
        j += 2;
    }
    Some(j)
}

fn operand_start(tokens: &[Token], end: usize) -> Option<usize> {
    let mut j = end;
    if tokens.get(j)?.is_op(")") {
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
        if j > 0 && matches!(tokens[j - 1].tok, Tok::Word(_) | Tok::Raw(_)) {
            j -= 1;
        }
    }
    while j >= 2 && tokens[j - 1].is_op(".") {
        j -= 2;
    }
    Some(j)
}

fn interval_unit(part: &str) -> Option<(&'static str, i64)> {
    Some(match part.to_ascii_lowercase().as_str() {
        "year" | "yy" | "yyyy" => ("years", 1),
        "quarter" | "qq" | "q" => ("months", 3),
        "month" | "mm" | "m" => ("months", 1),
        "dayofyear" | "dy" | "y" | "day" | "dd" | "d" | "weekday" | "dw" | "w" => ("days", 1),
        "week" | "wk" | "ww" => ("days", 7),
        "hour" | "hh" => ("hours", 1),
        "minute" | "mi" | "n" => ("minutes", 1),
        "second" | "ss" | "s" => ("seconds", 1),
        "millisecond" | "ms" => ("milliseconds", 1),
        "microsecond" | "mcs" => ("microseconds", 1),
        _ => return None,
    })
}

fn extract_field(part: &str) -> Option<&'static str> {
    Some(match part.to_ascii_lowercase().as_str() {
        "year" | "yy" | "yyyy" => "year",
        "quarter" | "qq" | "q" => "quarter",
        "month" | "mm" | "m" => "month",
        "dayofyear" | "dy" | "y" => "doy",
        "day" | "dd" | "d" => "day",
        "weekday" | "dw" => "dow",
        "hour" | "hh" => "hour",
        "minute" | "mi" | "n" => "minute",
        "second" | "ss" | "s" => "second",
        "millisecond" | "ms" => "milliseconds",
        "microsecond" | "mcs" => "microseconds",
        _ => return None,
    })
}

/// A stable object id for a table name.
pub(crate) fn object_id(name: &str) -> i64 {
    let mut h: u32 = 0x811c9dc5;
    for b in name.to_lowercase().bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    (h & 0x3fff_ffff) as i64 + 1000
}

/// `[dbo].[t]`, `dbo.t`, `t` written in a string (OBJECT_ID's argument).
fn object_name_in(text: &str) -> String {
    super::lex::last_name_part(text).to_lowercase()
}

/// `CROSS APPLY (…) [AS] t` is `CROSS JOIN LATERAL (…) t`; `OUTER APPLY`
/// is `LEFT JOIN LATERAL (…) t ON 1 = 1`, which keeps an outer row the
/// subquery has nothing for. EF Core writes these for `Take`/`First` inside
/// a projection or an `Include`.
fn apply_to_lateral(tokens: Vec<Token>) -> Result<Vec<Token>, TsqlError> {
    if !tokens.iter().any(|t| t.is_word("APPLY")) {
        return Ok(tokens);
    }
    let mut out = Vec::with_capacity(tokens.len() + 8);
    let mut on_at: Vec<usize> = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        let t = &tokens[i];
        let outer = t.is_word("OUTER");
        if (outer || t.is_word("CROSS")) && kw(&tokens, i + 1, "APPLY") {
            if !tokens.get(i + 2).is_some_and(|p| p.is_op("(")) {
                return Err(TsqlError::unsupported("APPLY of a table-valued function"));
            }
            let close = matching_paren(&tokens, i + 2)
                .ok_or_else(|| TsqlError::syntax("unbalanced parentheses"))?;
            let mut end = close + 1;
            if kw(&tokens, end, "AS") {
                end += 1;
            }
            if tokens
                .get(end)
                .is_some_and(|a| a.ident().is_some() && !is_clause_word(a))
            {
                end += 1;
            } else {
                return Err(TsqlError::syntax("APPLY needs an alias"));
            }
            let mut first = Token::word(if outer { "LEFT" } else { "CROSS" });
            first.space = t.space;
            out.push(first);
            out.push(Token::word("JOIN"));
            out.push(Token::word("LATERAL"));
            if outer {
                on_at.push(end);
            }
            i += 2;
            continue;
        }
        out.push(t.clone());
        i += 1;
        if on_at.last() == Some(&i) {
            on_at.pop();
            out.push(Token::word("ON"));
            out.push(Token::new(Tok::Num("1".into())));
            out.push(Token::op("="));
            out.push(Token::new(Tok::Num("1".into())));
        }
    }
    Ok(out)
}

pub(crate) fn rewrite_expr(tokens: Vec<Token>, cx: &mut Cx) -> Result<Vec<Token>, TsqlError> {
    let tokens = apply_to_lateral(tokens)?;
    let mut out: Vec<Token> = Vec::with_capacity(tokens.len() + 4);
    let mut i = 0usize;
    let n = tokens.len();
    while i < n {
        let t = &tokens[i];
        // `db.dbo.t`, `dbo.t`: the engine has one schema.
        if let Some(name) = t.ident() {
            if tokens.get(i + 1).is_some_and(|d| d.is_op("."))
                && !out.last().is_some_and(|p| p.is_op("."))
            {
                let schema_next = tokens
                    .get(i + 2)
                    .and_then(|x| x.ident())
                    .is_some_and(|s| s.eq_ignore_ascii_case("dbo"))
                    && tokens.get(i + 3).is_some_and(|d| d.is_op("."));
                if name.eq_ignore_ascii_case("dbo")
                    && tokens.get(i + 2).is_some_and(|x| x.ident().is_some())
                {
                    i += 2;
                    continue;
                }
                if schema_next {
                    i += 4;
                    continue;
                }
                if tokens.get(i + 2).is_some_and(|d| d.is_op(".")) {
                    // `db..t`
                    i += 3;
                    continue;
                }
                // sys.tables, INFORMATION_SCHEMA.COLUMNS: views of the
                // catalogue.
                if let Some(view) = tokens.get(i + 2).and_then(|x| x.ident()) {
                    if let Some(sql) = catalog_view(name, view, cx)? {
                        out.push(Token::raw(sql));
                        let alias_given = tokens.get(i + 3).is_some_and(|a| {
                            a.is_word("AS")
                                || (matches!(a.tok, Tok::Word(_) | Tok::Quoted(_))
                                    && !is_clause_word(a))
                        });
                        if !alias_given {
                            out.push(Token::raw(quote_ident(&view.to_lowercase())));
                        }
                        i += 3;
                        continue;
                    }
                }
            }
        }
        // A subquery: a query of its own (TOP, FOR JSON, …).
        if t.is_op("(") && kw(&tokens, i + 1, "SELECT") {
            let close = matching_paren(&tokens, i)
                .ok_or_else(|| TsqlError::syntax("unbalanced parentheses"))?;
            // `(SELECT … FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)')`
            let value_call = tokens.get(close + 1).is_some_and(|d| d.is_op("."))
                && tokens.get(close + 2).is_some_and(|w| w.is_word("value"))
                && tokens.get(close + 3).is_some_and(|o| o.is_op("("));
            let outer_xml = std::mem::replace(&mut cx.xml_value, value_call);
            let inner = rewrite_select("", tokens[i + 1..close].to_vec(), cx, false);
            cx.xml_value = outer_xml;
            out.push(t.clone());
            out.extend(inner?);
            out.push(tokens[close].clone());
            i = close + 1;
            if value_call {
                i = matching_paren(&tokens, close + 3)
                    .ok_or_else(|| TsqlError::syntax("unbalanced .value()"))?
                    + 1;
            }
            continue;
        }
        // A table hint, WITH (NOLOCK), or query hints, OPTION (…): dropped.
        if (t.is_word("WITH")
            && matches!(
                out.last().map(|p| &p.tok),
                Some(Tok::Word(_) | Tok::Quoted(_) | Tok::Raw(_))
            )
            && !out.last().is_some_and(|p| {
                p.upper()
                    .is_some_and(|w| matches!(w.as_str(), "AS" | "SELECT"))
            }))
            || t.is_word("OPTION")
        {
            if tokens.get(i + 1).is_some_and(|p| p.is_op("(")) {
                let close = matching_paren(&tokens, i + 1)
                    .ok_or_else(|| TsqlError::syntax("unbalanced hint"))?;
                i = close + 1;
                continue;
            }
        }
        if let Tok::Word(word) = &t.tok {
            if tokens.get(i + 1).is_some_and(|p| p.is_op("("))
                && !out.last().is_some_and(|p| p.is_op("."))
                && !is_clause_word(t)
                // `THEN (SELECT TOP(1) …)`, `AND (SELECT …)`: a keyword
                // before a subquery, not a call, whose subquery is then
                // translated (its TOP went to the engine as written).
                && !kw(&tokens, i + 2, "SELECT")
            {
                let close = matching_paren(&tokens, i + 1)
                    .ok_or_else(|| TsqlError::syntax("unbalanced parentheses"))?;
                cx.window_next = kw(&tokens, close + 1, "OVER");
                if let Some(replacement) = rewrite_call(word, &tokens[i + 2..close], cx)? {
                    out.extend(replacement);
                    // A table-valued full-text function may go without an
                    // alias in SQL Server; the derived table it is needs one.
                    if (word.eq_ignore_ascii_case("CONTAINSTABLE") || word.eq_ignore_ascii_case("FREETEXTTABLE"))
                        && !tokens.get(close + 1).is_some_and(|a| {
                            a.is_word("AS") || (matches!(a.tok, Tok::Word(_) | Tok::Quoted(_)) && !is_clause_word(a))
                        })
                    {
                        out.push(Token::raw(format!("AS {}", word.to_ascii_lowercase())));
                    }
                    i = close + 1;
                    continue;
                }
                out.push(t.clone());
                let mut open = tokens[i + 1].clone();
                open.space = false;
                out.push(open);
                out.extend(rewrite_expr(tokens[i + 2..close].to_vec(), cx)?);
                out.push(tokens[close].clone());
                i = close + 1;
                continue;
            }
        }
        match &t.tok {
            Tok::UserVar(name) => {
                if let Some(p) = cx.env.param(name) {
                    let mut v = Token::new(Tok::Param(p.index));
                    v.space = t.space;
                    out.push(v);
                } else if let Some(v) = cx.env.variable(name) {
                    let mut v = v.token();
                    v.space = t.space;
                    out.push(v);
                } else {
                    return Err(TsqlError::new(
                        137,
                        format!("Must declare the scalar variable \"@{name}\"."),
                    ));
                }
            }
            Tok::SysVar(name) => {
                let v = cx
                    .env
                    .system(name)
                    .ok_or_else(|| TsqlError::unsupported(format!("@@{}", name.to_uppercase())))?;
                out.push(v.token());
            }
            Tok::Op(o) if o == "+" => {
                // Concatenation, when either side is plainly a string.
                let left_string = out.last().is_some_and(|p| p.is_op("||"))
                    || operand_start(&out, out.len().saturating_sub(1))
                        .is_some_and(|s| !out.is_empty() && stringish(&out, s, out.len() - 1, cx));
                let right_string =
                    operand_end(&tokens, i + 1).is_some_and(|e| stringish(&tokens, i + 1, e, cx));
                let unary = out
                    .last()
                    .is_none_or(|p| matches!(&p.tok, Tok::Op(o) if o != ")"));
                if (left_string || right_string) && !unary {
                    out.push(Token::op("||"));
                } else {
                    out.push(t.clone());
                }
            }
            Tok::Op(o) if o == "!<" => out.push(Token::op(">=")),
            Tok::Op(o) if o == "!>" => out.push(Token::op("<=")),
            Tok::Word(w) => {
                let upper = w.to_ascii_uppercase();
                match upper.as_str() {
                    "COLLATE" => {
                        i += 2;
                        continue;
                    }
                    "CURRENT_TIMESTAMP" => out.push(Token::raw("localtimestamp")),
                    "CURRENT_USER" | "SESSION_USER" | "SYSTEM_USER" | "USER"
                        if !tokens.get(i + 1).is_some_and(|p| p.is_op("(")) =>
                    {
                        out.push(literal_str(cx.env.user()));
                    }
                    "WITH"
                        if tokens.get(i + 1).is_some_and(|p| p.is_op("("))
                            && matches!(
                                out.last().map(|p| &p.tok),
                                Some(Tok::Word(_) | Tok::Quoted(_) | Tok::Raw(_))
                            ) =>
                    {
                        // A table hint: WITH (NOLOCK).
                        let close = matching_paren(&tokens, i + 1)
                            .ok_or_else(|| TsqlError::syntax("unbalanced hint"))?;
                        i = close + 1;
                        continue;
                    }
                    "OPTION" if tokens.get(i + 1).is_some_and(|p| p.is_op("(")) => {
                        let close = matching_paren(&tokens, i + 1)
                            .ok_or_else(|| TsqlError::syntax("unbalanced OPTION"))?;
                        i = close + 1;
                        continue;
                    }
                    "NOLOCK" | "READUNCOMMITTED" | "ROWLOCK" | "UPDLOCK" | "HOLDLOCK"
                    | "READPAST" | "TABLOCK" | "TABLOCKX"
                        if out.last().is_some_and(|p| p.is_op("("))
                            && tokens.get(i + 1).is_some_and(|p| p.is_op(")")) =>
                    {
                        // An old-style hint: t (NOLOCK).
                        out.pop();
                        i += 2;
                        continue;
                    }
                    _ => {
                        if let Some(alias) = cx.aliases.iter().find(|a| a.eq_ignore_ascii_case(w)) {
                            if !out.last().is_some_and(|p| p.is_op("."))
                                && !tokens.get(i + 1).is_some_and(|p| p.is_op("."))
                            {
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
                    if !out.last().is_some_and(|p| p.is_op("."))
                        && !tokens.get(i + 1).is_some_and(|p| p.is_op("."))
                    {
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

fn is_clause_word(t: &Token) -> bool {
    t.upper().is_some_and(|w| {
        matches!(
            w.as_str(),
            "WHERE"
                | "FROM"
                | "LATERAL"
                | "JOIN"
                | "INNER"
                | "LEFT"
                | "RIGHT"
                | "FULL"
                | "CROSS"
                | "ON"
                | "GROUP"
                | "ORDER"
                | "HAVING"
                | "UNION"
                | "OUTER"
                | "WITH"
                | "OPTION"
                | "FOR"
                | "EXCEPT"
                | "INTERSECT"
                | "SELECT"
                | "INSERT"
                | "UPDATE"
                | "DELETE"
                | "SET"
                | "DECLARE"
                | "PRINT"
                | "EXEC"
                | "IF"
                | "END"
                | "ELSE"
                | "BEGIN"
                | "OFFSET"
                | "FETCH"
        )
    })
}

/// The columns of a full-text predicate's first argument: `col`, `t.col`,
/// or `(col, …)`, each as SQL. `*` names the full-text index's columns,
/// which only the table-valued forms know the table of.
fn fulltext_columns(arg: &[Token], cx: &mut Cx) -> Result<Vec<String>, TsqlError> {
    let list: Vec<Vec<Token>> = if arg.first().is_some_and(|t| t.is_op("(")) && matching_paren(arg, 0) == Some(arg.len() - 1) {
        split_commas(&arg[1..arg.len() - 1])
    } else {
        vec![arg.to_vec()]
    };
    let mut out = Vec::new();
    for part in list {
        if part.len() == 1 && part[0].is_op("*") {
            return Err(TsqlError::unsupported("CONTAINS or FREETEXT over * (name the columns)"));
        }
        let reference = part.iter().enumerate().all(|(k, t)| if k % 2 == 1 { t.is_op(".") } else { t.ident().is_some() });
        if part.is_empty() || part.len() % 2 == 0 || !reference {
            return Err(TsqlError::syntax("a full-text predicate names columns"));
        }
        out.push(render_text(&rewrite_expr(part, cx)?));
    }
    Ok(out)
}

/// A full-text predicate's search string, from a literal, a variable or a
/// parameter of the statement.
fn fulltext_search(arg: &[Token], cx: &Cx) -> Result<String, TsqlError> {
    super::fulltext::search_text(arg, |name| {
        if cx.env.param(name).is_some() {
            return cx.env.param_text(name);
        }
        cx.env.variable(name).map(|v| match v {
            Value::Null => None,
            Value::Str(s) => Some(s),
            Value::Int(i) => Some(i.to_string()),
            Value::Num(n) => Some(n),
            Value::Bytes(b) => Some(String::from_utf8_lossy(&b).into_owned()),
        })
    })
}

/// The columns a table's full-text index covers, from its word index.
fn fulltext_index_columns(database: &Database, table: &str) -> Option<Vec<String>> {
    let view = database.schema_view(table)?;
    let schema = view.schema();
    let [words, _] = super::fulltext::index_names(table);
    let decl = schema
        .secondary_indexes
        .iter()
        .find(|d| !d.is_deleted && d.name.eq_ignore_ascii_case(&words) && d.method.is_text())?;
    decl.columns
        .iter()
        .map(|wo| {
            schema
                .fields
                .iter()
                .find(|f| !f.is_deleted && f.write_order as u32 == *wo)
                .map(|f| ident_sql(&f.name))
        })
        .collect()
}

/// SQL Server's system databases and their ids. The catalogue's default
/// database answers for all of them (`resolve_database`): it is `master`.
const SYSTEM_DATABASES: &[(&str, i64)] = &[("master", 1), ("tempdb", 2), ("model", 3), ("msdb", 4)];

/// A database's id (`DB_ID`, `sys.databases.database_id`): the system
/// databases' own, the default database `master`'s, and otherwise one made
/// from its folded name, so it is the same in every session, after a
/// restart, and whatever other databases come and go. It was the
/// database's place in the list, and `DB_ID(x)` was 5 for any `x`.
pub(crate) fn database_id(name: &str, default: Option<&str>) -> i64 {
    let folded = name.to_lowercase();
    if let Some(&(_, id)) = SYSTEM_DATABASES.iter().find(|(n, _)| *n == folded) {
        return id;
    }
    if default.is_some_and(|d| d.eq_ignore_ascii_case(&folded)) {
        return 1;
    }
    // FNV-1a over the folded name, past the system ids, within an `int`.
    let mut h: u64 = 0xcbf29ce484222325;
    for b in folded.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100000001b3);
    }
    5 + (h % 2_000_000_000) as i64
}

/// Every database a session can name, with its id: the system databases,
/// then the catalogue's others (the default one is `master`).
fn database_ids(env: &dyn Env) -> Vec<(String, i64)> {
    let default = env.default_database();
    let mut out: Vec<(String, i64)> = SYSTEM_DATABASES.iter().map(|(n, id)| (n.to_string(), *id)).collect();
    for name in env.databases() {
        let id = database_id(&name, default.as_deref());
        if id > 4 {
            out.push((name, id));
        }
    }
    out
}

/// A CAST's or CONVERT's target type: a character type without a length
/// is 30 long there, as SQL Server's (`CAST(x AS varchar)` keeps 30
/// characters), where a column declared so is 1.
fn cast_type(ty: &[Token]) -> Vec<Token> {
    match ty {
        [t] if t.upper().is_some_and(|w| matches!(w.as_str(), "VARCHAR" | "NVARCHAR" | "CHAR" | "NCHAR")) => {
            vec![t.clone(), Token::op("("), Token::new(Tok::Num("30".into())), Token::op(")")]
        }
        _ => ty.to_vec(),
    }
}

/// `TRY_CAST(v AS t)` / `TRY_CONVERT(t, v)`: NULL where the cast would
/// fail. Guarded by whether `v`'s text reads as a `t`
/// (`pg_input_is_valid`), so a value converted from another type is judged
/// by its text: `TRY_CAST(1.5 AS int)` is NULL here, 1 in SQL Server.
fn try_cast(value: &[Token], target: &str) -> Vec<Token> {
    let v = render_text(value);
    vec![Token::raw(format!(
        "CASE WHEN pg_input_is_valid(CAST(({v}) AS text), {}) THEN CAST(({v}) AS {target}) END",
        sql_str(target)
    ))]
}

fn rewrite_call(name: &str, inner: &[Token], cx: &mut Cx) -> Result<Option<Vec<Token>>, TsqlError> {
    let upper = name.to_ascii_uppercase();
    if matches!(upper.as_str(), "CONTAINS" | "FREETEXT") {
        use super::fulltext::{Predicate, condition};
        let a = split_commas(inner);
        if !(2..=3).contains(&a.len()) {
            return Err(TsqlError::syntax(format!("{upper} takes columns and a search condition")));
        }
        let predicate = if upper == "CONTAINS" { Predicate::Contains } else { Predicate::Freetext };
        let columns = fulltext_columns(&a[0], cx)?;
        let search = fulltext_search(&a[1], cx)?;
        return Ok(Some(vec![Token::raw(condition(predicate, &columns, &search)?)]));
    }
    if matches!(upper.as_str(), "CONTAINSTABLE" | "FREETEXTTABLE") {
        use super::fulltext::{Predicate, table};
        let a = split_commas(inner);
        if !(3..=5).contains(&a.len()) {
            return Err(TsqlError::syntax(format!("{upper} takes a table, columns and a search condition")));
        }
        let predicate = if upper == "CONTAINSTABLE" { Predicate::Contains } else { Predicate::Freetext };
        let (name, _) = table_name_at(&a[0], 0).ok_or_else(|| TsqlError::syntax(format!("{upper} needs a table")))?;
        let name = name.to_lowercase();
        let database = cx.env.database().clone();
        let key = primary_key(&database, &name).ok_or_else(|| {
            TsqlError::new(7601, format!("Cannot use a CONTAINS or FREETEXT predicate on table '{name}' because it is not full-text indexed."))
        })?;
        let columns = if a[1].len() == 1 && a[1][0].is_op("*") {
            fulltext_index_columns(&database, &name).ok_or_else(|| {
                TsqlError::new(7601, format!("Cannot use a CONTAINS or FREETEXT predicate on table '{name}' because it is not full-text indexed."))
            })?
        } else {
            fulltext_columns(&a[1], cx)?
        };
        let search = fulltext_search(&a[2], cx)?;
        // `LANGUAGE x` is set aside; a number is top_n_by_rank.
        let top = a[3..].iter().find_map(|x| match x.as_slice() {
            [Token { tok: Tok::Num(n), .. }] => n.parse::<u64>().ok(),
            _ => None,
        });
        return Ok(Some(vec![Token::raw(table(predicate, &name, &key, &columns, &search, top)?)]));
    }
    let raw = || split_commas(inner);
    let args = |cx: &mut Cx| -> Result<Vec<Vec<Token>>, TsqlError> {
        let mut out = Vec::new();
        for a in split_commas(inner) {
            out.push(rewrite_expr(a, cx)?);
        }
        Ok(out)
    };
    let arity = |a: &Vec<Vec<Token>>, want: &[usize]| -> Result<(), TsqlError> {
        if want.contains(&a.len()) {
            Ok(())
        } else {
            Err(TsqlError::new(
                174,
                format!("The {upper} function requires {} argument(s).", want[0]),
            ))
        }
    };
    let text_arg = |a: &[Token]| -> Option<String> {
        match a {
            [
                Token {
                    tok: Tok::Str(s), ..
                },
            ] => Some(String::from_utf8_lossy(s).into_owned()),
            [
                Token {
                    tok: Tok::Word(w), ..
                },
            ] => Some(w.clone()),
            _ => None,
        }
    };
    if let Some(v) = cx.env.function(&upper) {
        if inner.is_empty() {
            return Ok(Some(vec![v.token()]));
        }
    }
    Ok(Some(match upper.as_str() {
        // SQL Server's COUNT is an int; COUNT_BIG is the bigint. A window
        // (COUNT(*) OVER …) is left as it is: the cast cannot sit between.
        "COUNT" if !cx.window_next && !cx.past_select_list => {
            let mut inner_tokens = vec![Token::raw("count"), Token::op("(")];
            inner_tokens[1].space = false;
            inner_tokens.extend(rewrite_expr(inner.to_vec(), cx)?);
            inner_tokens.push(Token::op(")"));
            let mut out = wrap(inner_tokens);
            out.push(Token::raw("::integer"));
            wrap(out)
        }
        "COUNT_BIG" => {
            let mut out = vec![Token::raw("count"), Token::op("(")];
            out[1].space = false;
            out.extend(rewrite_expr(inner.to_vec(), cx)?);
            out.push(Token::op(")"));
            out
        }
        "ISNULL" => call("coalesce", args(cx)?),
        "LEN" => {
            let a = args(cx)?;
            arity(&a, &[1])?;
            let mut cast = vec![Token::raw("CAST"), Token::op("(")];
            cast[1].space = false;
            cast.extend(a.into_iter().next().unwrap());
            cast.push(Token::raw("AS text)"));
            call("char_length", vec![call("rtrim", vec![cast])])
        }
        "DATALENGTH" => call("octet_length", args(cx)?),
        "GETDATE" | "SYSDATETIME" => vec![Token::raw("localtimestamp")],
        "GETUTCDATE" | "SYSUTCDATETIME" => vec![Token::raw("(now() at time zone 'UTC')")],
        "SYSDATETIMEOFFSET" => vec![Token::raw("now()")],
        "NEWID" => vec![Token::raw("gen_random_uuid()")],
        "NEWSEQUENTIALID" => vec![Token::raw("uuidv7()")],
        "IIF" => {
            let a = args(cx)?;
            arity(&a, &[3])?;
            let mut it = a.into_iter();
            let mut out = vec![Token::raw("(CASE WHEN")];
            out.extend(it.next().unwrap());
            out.push(Token::word("THEN"));
            out.extend(it.next().unwrap());
            out.push(Token::word("ELSE"));
            out.extend(it.next().unwrap());
            out.push(Token::raw("END)"));
            out
        }
        "CHOOSE" => {
            let a = args(cx)?;
            if a.len() < 2 {
                return Err(TsqlError::new(
                    174,
                    "The CHOOSE function requires at least 2 arguments.",
                ));
            }
            let mut it = a.into_iter();
            let mut out = vec![Token::raw("(CASE")];
            out.extend(wrap(it.next().unwrap()));
            for (k, v) in it.enumerate() {
                out.push(Token::raw(format!("WHEN {} THEN", k + 1)));
                out.extend(v);
            }
            out.push(Token::raw("END)"));
            out
        }
        "CHARINDEX" => {
            let a = args(cx)?;
            arity(&a, &[2, 3])?;
            let mut it = a.into_iter();
            let (sub, s) = (it.next().unwrap(), it.next().unwrap());
            match it.next() {
                None => call("strpos", vec![s, sub]),
                Some(pos) => {
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
        "SUBSTRING" => call("substr", args(cx)?),
        "REPLICATE" => call("repeat", args(cx)?),
        "SPACE" => {
            let mut a = args(cx)?;
            a.insert(0, vec![literal_str(" ")]);
            call("repeat", a)
        }
        "SQUARE" => {
            let a = args(cx)?;
            arity(&a, &[1])?;
            let x = wrap(a.into_iter().next().unwrap());
            let mut out = x.clone();
            out.push(Token::op("*"));
            out.extend(x);
            wrap(out)
        }
        "LOG" => {
            let a = args(cx)?;
            if a.len() == 1 {
                call("ln", a)
            } else {
                let mut a = a;
                a.swap(0, 1);
                call("log", a)
            }
        }
        "LOG10" => call("log", args(cx)?),
        "RAND" => vec![Token::raw("random()")],
        "ROUND" => {
            let a = args(cx)?;
            if a.len() == 3 {
                let truncate =
                    !matches!(a[2].as_slice(), [Token { tok: Tok::Num(n), .. }] if n == "0");
                let mut a = a;
                a.truncate(2);
                if truncate {
                    call("trunc", a)
                } else {
                    call("round", a)
                }
            } else {
                call("round", a)
            }
        }
        "STR" => {
            let a = args(cx)?;
            let mut cast = vec![Token::raw("CAST"), Token::op("(")];
            cast[1].space = false;
            cast.extend(a.into_iter().next().unwrap_or_default());
            cast.push(Token::raw("AS text)"));
            cast
        }
        "QUOTENAME" => {
            let a = args(cx)?;
            let mut out = vec![Token::raw("('[' ||")];
            out.extend(call(
                "replace",
                vec![
                    a.into_iter().next().unwrap_or_default(),
                    vec![literal_str("]")],
                    vec![literal_str("]]")],
                ],
            ));
            out.push(Token::raw("|| ']')"));
            out
        }
        "STRING_AGG" => {
            // STRING_AGG(x, sep) [WITHIN GROUP (ORDER BY …)] — the WITHIN
            // GROUP is handled by the caller seeing it after the call.
            let a = args(cx)?;
            arity(&a, &[2])?;
            let mut it = a.into_iter();
            let mut value = wrap(it.next().unwrap());
            value.push(Token::raw("::text"));
            call("string_agg", vec![value, it.next().unwrap()])
        }
        "DATEADD" => {
            let r = raw();
            if r.len() != 3 {
                return Err(TsqlError::new(
                    174,
                    "The DATEADD function requires 3 argument(s).",
                ));
            }
            let part = text_arg(&r[0]).unwrap_or_default();
            let (unit, factor) = interval_unit(&part).ok_or_else(|| {
                TsqlError::new(155, format!("'{part}' is not a recognized dateadd option."))
            })?;
            let amount = rewrite_expr(r[1].clone(), cx)?;
            let date = rewrite_expr(r[2].clone(), cx)?;
            let mut out = wrap(date);
            out.push(Token::op("+"));
            let mut step = wrap(amount);
            step.push(Token::raw(format!("* interval '{factor} {unit}'")));
            out.extend(wrap(step));
            wrap(out)
        }
        "DATEDIFF" | "DATEDIFF_BIG" => {
            let r = raw();
            if r.len() != 3 {
                return Err(TsqlError::new(
                    174,
                    "The DATEDIFF function requires 3 argument(s).",
                ));
            }
            let part = text_arg(&r[0]).unwrap_or_default().to_ascii_lowercase();
            let a = render_text(&rewrite_expr(r[1].clone(), cx)?);
            let b = render_text(&rewrite_expr(r[2].clone(), cx)?);
            // Boundaries crossed, as SQL Server counts.
            let sql = match part.as_str() {
                "year" | "yy" | "yyyy" => format!(
                    "(extract(year from ({b})::timestamp) - extract(year from ({a})::timestamp))::int"
                ),
                "quarter" | "qq" | "q" => format!(
                    "((extract(year from ({b})::timestamp) - extract(year from ({a})::timestamp)) * 4 + extract(quarter from ({b})::timestamp) - extract(quarter from ({a})::timestamp))::int"
                ),
                "month" | "mm" | "m" => format!(
                    "((extract(year from ({b})::timestamp) - extract(year from ({a})::timestamp)) * 12 + extract(month from ({b})::timestamp) - extract(month from ({a})::timestamp))::int"
                ),
                "day" | "dd" | "d" | "dayofyear" | "dy" | "y" => {
                    format!("(({b})::date - ({a})::date)")
                }
                "hour" | "hh" => format!(
                    "(extract(epoch from date_trunc('hour', ({b})::timestamp) - date_trunc('hour', ({a})::timestamp)) / 3600)::int"
                ),
                "minute" | "mi" | "n" => format!(
                    "(extract(epoch from date_trunc('minute', ({b})::timestamp) - date_trunc('minute', ({a})::timestamp)) / 60)::int"
                ),
                "second" | "ss" | "s" => format!(
                    "(extract(epoch from date_trunc('second', ({b})::timestamp) - date_trunc('second', ({a})::timestamp)))::bigint"
                ),
                "millisecond" | "ms" => format!(
                    "(extract(epoch from ({b})::timestamp - ({a})::timestamp) * 1000)::bigint"
                ),
                other => return Err(TsqlError::unsupported(format!("DATEDIFF by {other}"))),
            };
            vec![Token::raw(sql)]
        }
        "DATEPART" | "DATENAME" => {
            let r = raw();
            if r.len() != 2 {
                return Err(TsqlError::new(
                    174,
                    format!("The {upper} function requires 2 argument(s)."),
                ));
            }
            let part = text_arg(&r[0]).unwrap_or_default();
            let field = extract_field(&part)
                .ok_or_else(|| TsqlError::unsupported(format!("{upper} of {part}")))?;
            let d = rewrite_expr(r[1].clone(), cx)?;
            let mut out = vec![Token::raw(format!("(extract({field} from"))];
            let mut ts = wrap(d);
            ts.push(Token::raw("::timestamp"));
            out.extend(wrap(ts));
            out.push(Token::raw(if field == "dow" {
                ")::int + 1)"
            } else {
                ")::int)"
            }));
            if upper == "DATENAME" {
                let mut cast = vec![Token::raw("CAST(")];
                cast.extend(out);
                cast.push(Token::raw("AS text)"));
                cast
            } else {
                out
            }
        }
        "YEAR" | "MONTH" | "DAY" => {
            let a = args(cx)?;
            arity(&a, &[1])?;
            let mut out = vec![Token::raw(format!(
                "(extract({} from",
                upper.to_lowercase()
            ))];
            let mut ts = wrap(a.into_iter().next().unwrap());
            ts.push(Token::raw("::timestamp"));
            out.extend(wrap(ts));
            out.push(Token::raw(")::int)"));
            out
        }
        "EOMONTH" => {
            let a = args(cx)?;
            arity(&a, &[1])?;
            let d = render_text(&a[0]);
            vec![Token::raw(format!(
                "((date_trunc('month', ({d})::timestamp) + interval '1 month' - interval '1 day')::date)"
            ))]
        }
        "DATEFROMPARTS" => {
            let a = args(cx)?;
            arity(&a, &[3])?;
            let parts: Vec<String> = a.iter().map(|x| render_text(x)).collect();
            vec![Token::raw(format!(
                "CAST(concat({}, '-', {}, '-', {}) AS date)",
                parts[0], parts[1], parts[2]
            ))]
        }
        "CAST" | "TRY_CAST" => {
            let at =
                find_top(inner, 0, &["AS"]).ok_or_else(|| TsqlError::syntax("CAST needs AS"))?;
            let value = rewrite_expr(inner[..at].to_vec(), cx)?;
            let target = map_type(&cast_type(&inner[at + 1..]))?;
            if upper == "TRY_CAST" {
                try_cast(&value, &target)
            } else {
                let mut out = vec![Token::raw("CAST"), Token::op("(")];
                out[1].space = false;
                out.extend(value);
                out.push(Token::raw(format!("AS {target})")));
                out
            }
        }
        "CONVERT" | "TRY_CONVERT" => {
            let r = raw();
            if r.len() < 2 {
                return Err(TsqlError::new(
                    174,
                    "The CONVERT function requires at least 2 argument(s).",
                ));
            }
            let target = map_type(&cast_type(&r[0]))?;
            let value = rewrite_expr(r[1].clone(), cx)?;
            let style = r.get(2).and_then(|s| render_text(s).parse::<u32>().ok());
            let pattern = match (style, target.as_str()) {
                (Some(s), t) if t == "text" || t.starts_with("varchar") => match s {
                    23 => Some("YYYY-MM-DD"),
                    120 | 20 => Some("YYYY-MM-DD HH24:MI:SS"),
                    121 | 21 | 126 | 127 => Some(if s >= 126 {
                        "YYYY-MM-DD\"T\"HH24:MI:SS.MS"
                    } else {
                        "YYYY-MM-DD HH24:MI:SS.MS"
                    }),
                    112 => Some("YYYYMMDD"),
                    101 => Some("MM/DD/YYYY"),
                    103 => Some("DD/MM/YYYY"),
                    104 => Some("DD.MM.YYYY"),
                    108 | 8 => Some("HH24:MI:SS"),
                    _ => None,
                },
                _ => None,
            };
            if let Some(p) = pattern {
                let mut ts = wrap(value);
                ts.push(Token::raw("::timestamp"));
                call("to_char", vec![ts, vec![literal_str(p)]])
            } else if upper == "TRY_CONVERT" {
                try_cast(&value, &target)
            } else {
                let mut out = vec![Token::raw("CAST"), Token::op("(")];
                out[1].space = false;
                out.extend(value);
                out.push(Token::raw(format!("AS {target})")));
                out
            }
        }
        "OBJECT_ID" => {
            let r = raw();
            let name = r
                .first()
                .and_then(|a| text_arg(a))
                .map(|t| object_name_in(&t));
            // OBJECT_ID('name', 'U' | 'P' | …): tables and procedures.
            let kind = r
                .get(1)
                .and_then(|a| text_arg(a))
                .map(|k| k.trim().to_ascii_uppercase());
            let name = name.map(|n| {
                if n.starts_with('#') {
                    cx.env.temp_name(&n)
                } else {
                    n
                }
            });
            let exists = name.as_deref().is_some_and(|n| {
                let table = cx.env.database().has_table(n);
                let routine = cx.env.routines().iter().any(|(r, _)| r == n);
                match kind.as_deref() {
                    Some("U") => table,
                    Some("P") => routine,
                    Some(_) => false,
                    None => table || routine,
                }
            });
            vec![match name {
                Some(n) if exists => Token::new(Tok::Num(object_id(&n).to_string())),
                _ => Token::raw("CAST(NULL AS integer)"),
            }]
        }
        "OBJECT_NAME" => {
            let r = raw();
            let id: Option<i64> = r
                .first()
                .and_then(|a| text_arg(a))
                .and_then(|t| t.parse().ok());
            let name = id.and_then(|id| {
                cx.env
                    .database()
                    .table_names()
                    .into_iter()
                    .chain(cx.env.routines().into_iter().map(|(n, _)| n))
                    .find(|n| object_id(n) == id)
            });
            vec![match name {
                Some(n) => literal_str(&n),
                None => Token::raw("CAST(NULL AS text)"),
            }]
        }
        "SCHEMA_ID" => {
            let r = raw();
            let name = r
                .first()
                .and_then(|a| text_arg(a))
                .unwrap_or_else(|| "dbo".into());
            vec![if name.eq_ignore_ascii_case("dbo") {
                Token::new(Tok::Num("1".into()))
            } else {
                Token::raw("CAST(NULL AS integer)")
            }]
        }
        "SCHEMA_NAME" => vec![literal_str("dbo")],
        // An id per database, the same in every session and across
        // restarts (`database_id`); NULL for a name or id no database has.
        "DB_NAME" if !inner.is_empty() => {
            let id = render_text(&rewrite_expr(inner.to_vec(), cx)?);
            let arms: String = database_ids(cx.env)
                .into_iter()
                .map(|(name, id)| format!(" WHEN {id} THEN {}", sql_str(&name)))
                .collect();
            vec![Token::raw(format!("CAST(CASE ({id}){arms} END AS text)"))]
        }
        "DB_NAME" => vec![literal_str(cx.env.database_name())],
        "DB_ID" if !inner.is_empty() => {
            let name = render_text(&rewrite_expr(inner.to_vec(), cx)?);
            let arms: String = database_ids(cx.env)
                .into_iter()
                .map(|(n, id)| format!(" WHEN {} THEN {id}", sql_str(&n.to_lowercase())))
                .collect();
            vec![Token::raw(format!("CAST(CASE lower({name}){arms} END AS integer)"))]
        }
        "DB_ID" => {
            let id = database_id(cx.env.database_name(), cx.env.default_database().as_deref());
            vec![Token::raw(format!("CAST({id} AS integer)"))]
        }
        "USER_NAME" | "SUSER_NAME" | "SUSER_SNAME" | "ORIGINAL_LOGIN" | "USER_ID"
            if inner.is_empty() || upper != "USER_ID" =>
        {
            vec![literal_str(cx.env.user())]
        }
        "COLUMNPROPERTY" | "OBJECTPROPERTY" | "OBJECTPROPERTYEX" | "INDEXPROPERTY"
        | "COLLATIONPROPERTY" => vec![Token::raw("CAST(NULL AS integer)")],
        "SERVERPROPERTY" | "DATABASEPROPERTYEX" => {
            let r = raw();
            let prop = r.last().and_then(|a| text_arg(a)).unwrap_or_default();
            let v = server_property(&prop);
            vec![v.token()]
        }
        // HAS_PERMS_BY_NAME(securable, class, permission): the privilege
        // gates are the engine's; the sys schema is not readable as tables.
        "HAS_PERMS_BY_NAME" => {
            let r = raw();
            let securable = r
                .first()
                .and_then(|a| text_arg(a))
                .unwrap_or_default()
                .to_ascii_lowercase();
            let sys = securable.contains("[sys]")
                || securable.starts_with("sys.")
                || securable.contains(".sys.");
            vec![Token::new(Tok::Num(if sys { "0" } else { "1" }.into()))]
        }
        // STUFF(s, start, length, replacement)
        "STUFF" => {
            let a = args(cx)?;
            arity(&a, &[4])?;
            let text = |k: usize| render_text(&a[k]);
            vec![Token::raw(format!(
                "(substring({s}, 1, ({start}) - 1) || coalesce({r}, '') || substring({s}, ({start}) + ({len})))",
                s = text(0),
                start = text(1),
                len = text(2),
                r = text(3)
            ))]
        }
        "STRING_SPLIT" | "OPENJSON" | "OPENXML" | "OPENQUERY" | "OPENROWSET" | "PATINDEX"
        | "FORMAT" | "TRANSLATE" | "SOUNDEX" | "DIFFERENCE" => {
            return Err(TsqlError::unsupported(upper));
        }
        "ERROR_MESSAGE" | "ERROR_NUMBER" | "ERROR_SEVERITY" | "ERROR_STATE" | "ERROR_LINE"
        | "ERROR_PROCEDURE" | "XACT_STATE" | "SCOPE_IDENTITY" | "IDENT_CURRENT"
        | "ROWCOUNT_BIG" | "HOST_NAME" | "APP_NAME" | "IS_SRVROLEMEMBER" | "IS_MEMBER"
        | "HAS_DBACCESS" | "SESSIONPROPERTY" => {
            let v = cx.env.function(&upper).unwrap_or(Value::Null);
            vec![v.token()]
        }
        _ => return Ok(None),
    }))
}

/// What SERVERPROPERTY tells a client.
pub(crate) fn server_property(name: &str) -> Value {
    let s = |v: &str| Value::Str(v.into());
    match name.to_ascii_lowercase().as_str() {
        "productversion" => s("16.0.4000.0"),
        "productlevel" => s("RTM"),
        "productmajorversion" => s("16"),
        "productminorversion" => s("0"),
        "edition" => s("Developer Edition (64-bit)"),
        "engineedition" => Value::Int(3),
        "servername" | "machinename" => s("rasterizeddb"),
        "instancename" => Value::Null,
        "collation" => s("Latin1_General_BIN2"),
        "isclustered"
        | "isfulltextinstalled"
        | "isintegratedsecurityonly"
        | "ishadrenabled"
        | "isxtpsupported" => Value::Int(0),
        "issingleuser" => Value::Int(0),
        "status" => s("ONLINE"),
        "updateability" => s("READ_WRITE"),
        "recovery" => s("FULL"),
        "useraccess" => s("MULTI_USER"),
        "version" => Value::Int(957),
        _ => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Catalogue views
// ---------------------------------------------------------------------------

pub(crate) fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// A `sys.` or `INFORMATION_SCHEMA.` view as a derived table built now, or
/// `None` when `schema.view` is not one.
fn catalog_view(schema: &str, view: &str, cx: &Cx) -> Result<Option<String>, TsqlError> {
    // Names are listed with their dots (`lex::NAME_DOT`): the view is a
    // VALUES list, where every name is a string.
    Ok(catalog_view_stored(schema, view, cx)?.map(|sql| super::lex::decode_dots(&sql).into_owned()))
}

fn catalog_view_stored(schema: &str, view: &str, cx: &Cx) -> Result<Option<String>, TsqlError> {
    let schema = schema.to_ascii_lowercase();
    let view = view.to_ascii_lowercase();
    if schema != "sys" && schema != "information_schema" {
        return Ok(None);
    }
    let database = cx.env.database();
    let mut tables: Vec<String> = database
        .table_names()
        .into_iter()
        .filter(|n| {
            // `__` marks the engine's own relations; a table this front
            // end made under such a name (EF Core's `__EFMigrationsHistory`)
            // is the client's, known by its recorded declared name.
            (!n.starts_with("__") || cx.env.shown(n, None).is_some())
                && n != crate::auth::ROLE_TABLE
                && !n.starts_with(crate::core::rql::json_map::SHADOW_TABLE_PREFIX)
        })
        .collect();
    tables.sort();
    let db = cx.env.database_name().to_string();
    // Names as they were declared over this protocol (`NAMES_TABLE`).
    let shown_table = |t: &str| cx.env.shown(t, None).unwrap_or_else(|| t.to_string());
    let shown_column = |t: &str, c: &str| cx.env.shown(t, Some(c)).unwrap_or_else(|| c.to_string());
    let empty = |cols: &[&str]| -> String {
        format!(
            "(SELECT {} WHERE 1 = 0)",
            cols.iter()
                .map(|c| format!("CAST(NULL AS text) AS {}", ident_sql(c)))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let values = |rows: Vec<String>, cols: &[&str], fallback: String| -> String {
        if rows.is_empty() {
            fallback
        } else {
            format!(
                "(SELECT * FROM (VALUES {}) AS v({}))",
                rows.join(", "),
                cols.iter()
                    .map(|c| ident_sql(c))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    };
    let columns_of = |table: &str| -> Vec<(String, String, bool, Option<String>, bool)> {
        // (name, type, nullable, default, identity)
        let Some(view) = database.schema_view(table) else {
            return Vec::new();
        };
        let schema = view.schema();
        schema
            .fields
            .iter()
            .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
            .map(|f| {
                let default = schema
                    .defaults
                    .iter()
                    .filter(|d| !d.is_deleted && d.column == f.name)
                    .map(|d| d.expression.clone())
                    .next_back();
                let identity = default
                    .as_deref()
                    .is_some_and(|d| d.to_ascii_lowercase().starts_with("nextval("));
                let pk = schema.primary_key.as_deref() == Some(f.name.as_str());
                // A declared length: `nvarchar(40)`, `nchar(3)` (both kinds
                // of text are the engine's one: shown as SQL Server's
                // Unicode ones).
                let ty = match (f.db_type == crate::core::db_type::DbType::STRING && !f.json_text, f.text_typmod()) {
                    (true, Some(crate::core::rql::type_modifier::TextTypmod::Varchar(n))) if n > 0 => format!("nvarchar({n})"),
                    (true, Some(crate::core::rql::type_modifier::TextTypmod::Bpchar(n))) if n > 0 => format!("nchar({n})"),
                    _ => tsql_type_name(&f.db_type, f.without_time_zone, f.json_text),
                };
                (f.name.clone(), ty, !pk, default, identity)
            })
            .collect()
    };
    let sql = match (schema.as_str(), view.as_str()) {
        ("sys", "tables") | ("sys", "objects") | ("sys", "all_objects") | ("sys", "procedures") => {
            let cols = ["name", "object_id", "principal_id", "schema_id", "parent_object_id", "type", "type_desc", "create_date", "modify_date", "is_ms_shipped"];
            let mut rows: Vec<String> = Vec::new();
            if view != "procedures" {
                rows.extend(tables.iter().map(|t| format!("({}, {}, NULL::integer, 1, 0, 'U', 'USER_TABLE', localtimestamp, localtimestamp, false)", sql_str(&shown_table(t)), object_id(t))));
            }
            if view != "tables" {
                rows.extend(cx.env.routines().iter().map(|(n, _)| format!("({}, {}, NULL::integer, 1, 0, 'P', 'SQL_STORED_PROCEDURE', localtimestamp, localtimestamp, false)", sql_str(n), object_id(n))));
            }
            values(rows, &cols, empty(&cols))
        }
        ("sys", "sql_modules") | ("sys", "all_sql_modules") => {
            let cols = ["object_id", "definition", "uses_ansi_nulls", "uses_quoted_identifier", "is_schema_bound"];
            let rows = cx.env.routines().iter().map(|(n, d)| format!("({}, {}, true, true, false)", object_id(n), sql_str(d))).collect();
            values(rows, &cols, empty(&cols))
        }
        ("sys", "views") | ("sys", "triggers") | ("sys", "sequences") | ("sys", "synonyms") => {
            empty(&["name", "object_id", "schema_id", "type", "type_desc"])
        }
        ("sys", "schemas") => "(SELECT * FROM (VALUES ('dbo', 1, 1), ('sys', 4, 4), ('INFORMATION_SCHEMA', 3, 4)) AS v(name, schema_id, principal_id))".into(),
        ("sys", "databases") => {
            let mut rows = Vec::new();
            for (name, id) in database_ids(cx.env) {
                rows.push(format!("({}, {id}, 'Latin1_General_BIN2', 0, 'ONLINE', 160)", sql_str(&name)));
            }
            format!("(SELECT * FROM (VALUES {}) AS v(name, database_id, collation_name, state, state_desc, compatibility_level))", rows.join(", "))
        }
        ("sys", "columns") | ("sys", "all_columns") => {
            let cols = ["object_id", "name", "column_id", "system_type_id", "user_type_id", "max_length", "precision", "scale", "is_nullable", "is_identity", "is_computed"];
            let mut rows = Vec::new();
            for t in &tables {
                for (k, (name, ty, nullable, _, identity)) in columns_of(t).into_iter().enumerate() {
                    let (type_id, max_len) = type_ids(&ty);
                    // A declared length, in bytes as SQL Server counts
                    // them (two per character of nvarchar / nchar).
                    let max_len = ty
                        .split_once('(')
                        .and_then(|(_, r)| r.trim_end_matches(')').parse::<i32>().ok())
                        .map_or(max_len, |n| n * 2);
                    rows.push(format!(
                        "({}, {}, {}, {type_id}, {type_id}, {max_len}, 0, 0, {nullable}, {identity}, false)",
                        object_id(t),
                        sql_str(&shown_column(t, &name)),
                        k + 1
                    ));
                }
            }
            values(rows, &cols, empty(&cols))
        }
        ("sys", "types") => {
            let rows: Vec<String> = [
                ("bit", 104), ("tinyint", 48), ("smallint", 52), ("int", 56), ("bigint", 127), ("decimal", 106), ("numeric", 108),
                ("money", 60), ("float", 62), ("real", 59), ("date", 40), ("time", 41), ("datetime2", 42), ("datetimeoffset", 43),
                ("datetime", 61), ("char", 175), ("varchar", 167), ("nchar", 239), ("nvarchar", 231), ("text", 35), ("ntext", 99),
                ("binary", 173), ("varbinary", 165), ("image", 34), ("uniqueidentifier", 36), ("xml", 241), ("sysname", 231),
            ]
            .iter()
            .map(|(n, id)| format!("({}, {id}, {id}, 4, false)", sql_str(n)))
            .collect();
            values(rows, &["name", "system_type_id", "user_type_id", "schema_id", "is_user_defined"], String::new())
        }
        ("sys", "indexes") => {
            let cols = ["object_id", "name", "index_id", "type", "type_desc", "is_unique", "is_primary_key", "is_unique_constraint"];
            let mut rows = Vec::new();
            for t in &tables {
                let Some(view) = database.schema_view(t) else { continue };
                let schema = view.schema();
                let mut id = 1;
                if schema.primary_key.is_some() {
                    rows.push(format!("({}, {}, 1, 1, 'CLUSTERED', true, true, false)", object_id(t), sql_str(&format!("PK_{t}"))));
                    id += 1;
                }
                for k in schema.unique_keys.iter().filter(|k| !k.is_deleted) {
                    rows.push(format!("({}, {}, {id}, 2, 'NONCLUSTERED', true, false, true)", object_id(t), sql_str(&k.name)));
                    id += 1;
                }
                for x in schema.secondary_indexes.iter().filter(|x| !x.is_deleted) {
                    rows.push(format!(
                        "({}, {}, {id}, 2, 'NONCLUSTERED', {}, false, false)",
                        object_id(t),
                        sql_str(crate::sqlcompat::plain_index_name(t, &x.name)),
                        x.unique
                    ));
                    id += 1;
                }
            }
            values(rows, &cols, empty(&cols))
        }
        // No rows (the engine's defaults, checks and keys are not named
        // objects a script can drop one by one), but each with the columns
        // scripts read: EF Core's DropColumn / AlterColumn joins
        // `default_constraints` to `columns` on `parent_column_id`.
        ("sys", view @ ("foreign_keys" | "key_constraints" | "check_constraints" | "default_constraints" | "index_columns" | "foreign_key_columns" | "extended_properties")) => {
            let cols: &[&str] = match view {
                "default_constraints" => &["object_id", "name", "parent_object_id", "parent_column_id", "definition", "type", "type_desc", "is_system_named", "schema_id"],
                "check_constraints" => &["object_id", "name", "parent_object_id", "parent_column_id", "definition", "type", "type_desc", "is_disabled", "schema_id"],
                "key_constraints" => &["object_id", "name", "parent_object_id", "type", "type_desc", "unique_index_id", "schema_id"],
                "foreign_keys" => &["object_id", "name", "parent_object_id", "referenced_object_id", "key_index_id", "delete_referential_action", "update_referential_action", "type", "type_desc", "is_disabled", "schema_id"],
                "index_columns" => &["object_id", "index_id", "index_column_id", "column_id", "key_ordinal", "partition_ordinal", "is_descending_key", "is_included_column"],
                "foreign_key_columns" => &["constraint_object_id", "constraint_column_id", "parent_object_id", "parent_column_id", "referenced_object_id", "referenced_column_id"],
                _ => &["class", "class_desc", "major_id", "minor_id", "name", "value"],
            };
            let numeric = |c: &str| {
                c.ends_with("_id")
                    || c.starts_with("is_")
                    || matches!(c, "class" | "key_ordinal" | "partition_ordinal" | "delete_referential_action" | "update_referential_action")
            };
            format!(
                "(SELECT {} WHERE 1 = 0)",
                cols.iter()
                    .map(|c| format!("CAST(NULL AS {}) AS {}", if numeric(c) { "integer" } else { "text" }, ident_sql(c)))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        ("information_schema", "tables") => {
            let cols = ["table_catalog", "table_schema", "table_name", "table_type"];
            let rows = tables.iter().map(|t| format!("({}, 'dbo', {}, 'BASE TABLE')", sql_str(&db), sql_str(&shown_table(t)))).collect();
            values(rows, &cols, empty(&cols))
        }
        ("information_schema", "columns") => {
            // SQL Server's columns, in its order: tools read the numeric,
            // datetime and collation ones (`DATA_TYPE + '(' +
            // CAST(DATETIME_PRECISION …)`), and a name it lacked failed.
            let cols = [
                "table_catalog", "table_schema", "table_name", "column_name", "ordinal_position", "column_default",
                "is_nullable", "data_type", "character_maximum_length", "character_octet_length", "numeric_precision",
                "numeric_precision_radix", "numeric_scale", "datetime_precision", "character_set_catalog",
                "character_set_schema", "character_set_name", "collation_catalog", "collation_schema", "collation_name",
                "domain_catalog", "domain_schema", "domain_name",
            ];
            let int = |v: Option<i64>| v.map(|v| v.to_string()).unwrap_or_else(|| "CAST(NULL AS integer)".into());
            let text = |v: Option<&str>| v.map(sql_str).unwrap_or_else(|| "CAST(NULL AS text)".into());
            let mut rows = Vec::new();
            for t in &tables {
                let view = database.schema_view(t);
                let schema = view.as_ref().map(|v| v.schema());
                for (k, (name, ty, nullable, default, _)) in columns_of(t).into_iter().enumerate() {
                    let base = ty.split('(').next().unwrap_or(&ty).to_string();
                    let len = ty.split_once('(').and_then(|(_, r)| r.trim_end_matches(')').parse::<i64>().ok());
                    let field = schema.and_then(|s| s.fields.iter().find(|f| !f.is_deleted && f.name == name));
                    let is_text = base.starts_with("nvarchar") || base.starts_with("varchar") || base.starts_with("nchar");
                    let max_len = len.or_else(|| (ty.ends_with("(max)")).then_some(-1));
                    let octets = match max_len {
                        Some(-1) => Some(-1),
                        Some(n) if is_text => Some(n * 2),
                        other => other,
                    };
                    let (precision, radix, scale) = match base.as_str() {
                        "tinyint" => (Some(3), Some(10), Some(0)),
                        "smallint" => (Some(5), Some(10), Some(0)),
                        "int" => (Some(10), Some(10), Some(0)),
                        "bigint" => (Some(19), Some(10), Some(0)),
                        "real" => (Some(24), Some(2), None),
                        "float" => (Some(53), Some(2), None),
                        "decimal" => {
                            let mods = schema.map(|s| &s.type_modifiers[..]).unwrap_or(&[]);
                            let p = crate::core::rql::type_modifier::precision_of(&name, mods).unwrap_or(38);
                            let sc = field.and_then(|f| f.scale).unwrap_or(crate::core::value_codec::DECIMAL_SCALE);
                            (Some(i64::from(p)), Some(10), Some(i64::from(sc)))
                        }
                        _ => (None, None, None),
                    };
                    // The fractional digits the column keeps: a declared
                    // precision, else the engine's microseconds.
                    let datetime = matches!(base.as_str(), "datetime2" | "datetimeoffset" | "time" | "date").then(|| {
                        if base == "date" {
                            return 0;
                        }
                        match field.and_then(|f| f.text_typmod()) {
                            Some(crate::core::rql::type_modifier::TextTypmod::Precision(p)) => i64::from(p),
                            _ => 6,
                        }
                    });
                    let collation = is_text.then(|| {
                        let binary = schema.is_some_and(|s| crate::core::rql::collation::of(&name, &s.fields).is_binary());
                        if binary { "Latin1_General_BIN2" } else { "SQL_Latin1_General_CP1_CI_AS" }
                    });
                    rows.push(format!(
                        "({}, 'dbo', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                        sql_str(&db),
                        sql_str(&shown_table(t)),
                        sql_str(&shown_column(t, &name)),
                        k + 1,
                        default.map(|d| sql_str(&d)).unwrap_or_else(|| "CAST(NULL AS text)".into()),
                        sql_str(if nullable { "YES" } else { "NO" }),
                        sql_str(&base),
                        int(max_len),
                        int(octets),
                        int(precision),
                        int(radix),
                        int(scale),
                        int(datetime),
                        text(None),
                        text(None),
                        text(is_text.then_some("UNICODE")),
                        text(None),
                        text(None),
                        text(collation),
                        text(None),
                        text(None),
                        text(None),
                    ));
                }
            }
            values(rows, &cols, empty(&cols))
        }
        ("information_schema", "schemata") => format!("(SELECT * FROM (VALUES ({}, 'dbo', 'dbo')) AS v(catalog_name, schema_name, schema_owner))", sql_str(&db)),
        ("information_schema", "routines") => {
            let cols = ["specific_catalog", "specific_schema", "specific_name", "routine_catalog", "routine_schema", "routine_name", "routine_type", "routine_definition", "created", "last_altered"];
            let rows = cx.env.routines().iter().map(|(n, d)| format!("({db}, 'dbo', {n}, {db}, 'dbo', {n}, 'PROCEDURE', {d}, localtimestamp, localtimestamp)", db = sql_str(&db), n = sql_str(n), d = sql_str(d))).collect();
            values(rows, &cols, empty(&cols))
        }
        ("information_schema", "views") | ("information_schema", "table_constraints") | ("information_schema", "key_column_usage") | ("information_schema", "referential_constraints") => {
            empty(&["table_catalog", "table_schema", "table_name"])
        }
        _ => return Err(TsqlError::new(208, format!("Invalid object name '{schema}.{view}'."))),
    };
    Ok(Some(sql))
}

/// The SQL Server name of an engine column type.
fn tsql_type_name(t: &crate::core::db_type::DbType, without_tz: bool, json: bool) -> String {
    use crate::core::db_type::DbType as D;
    match t {
        D::BOOL => "bit".into(),
        D::I8 | D::U8 => "tinyint".into(),
        D::I16 => "smallint".into(),
        D::I32 | D::U16 => "int".into(),
        D::I64 | D::U32 => "bigint".into(),
        D::F32 => "real".into(),
        D::F64 => "float".into(),
        D::DECIMAL | D::I128 | D::U64 | D::U128 => "decimal".into(),
        D::DATETIME if without_tz => "datetime2".into(),
        D::DATETIME => "datetimeoffset".into(),
        D::TIMESTAMP => "datetime2".into(),
        D::UUID => "uniqueidentifier".into(),
        D::BYTEA => "varbinary(max)".into(),
        D::STRING if json => "nvarchar(max)".into(),
        _ => "nvarchar(max)".into(),
    }
}

fn type_ids(ty: &str) -> (i32, i32) {
    match ty.split('(').next().unwrap_or(ty) {
        "bit" => (104, 1),
        "tinyint" => (48, 1),
        "smallint" => (52, 2),
        "int" => (56, 4),
        "bigint" => (127, 8),
        "real" => (59, 4),
        "float" => (62, 8),
        "decimal" => (106, 17),
        "datetime2" => (42, 8),
        "datetimeoffset" => (43, 10),
        "uniqueidentifier" => (36, 16),
        "varbinary" => (165, -1),
        "nchar" => (239, -1),
        _ => (231, -1),
    }
}

// ---------------------------------------------------------------------------
// Statements
// ---------------------------------------------------------------------------

fn table_name_at(tokens: &[Token], i: usize) -> Option<(String, usize)> {
    let mut j = i;
    let mut name = tokens.get(j)?.ident()?.to_string();
    while tokens.get(j + 1).is_some_and(|t| t.is_op(".")) {
        match tokens.get(j + 2).and_then(|t| t.ident()) {
            Some(next) => {
                name = next.to_string();
                j += 2;
            }
            None => {
                // `db..t`
                j += 1;
            }
        }
    }
    Some((name.to_lowercase(), j + 1))
}

pub(crate) fn identity_column(database: &Database, table: &str) -> Option<String> {
    let view = database.schema_view(table)?;
    view.schema()
        .defaults
        .iter()
        .filter(|d| !d.is_deleted)
        .find(|d| {
            d.expression
                .trim_start()
                .to_ascii_lowercase()
                .starts_with("nextval(")
        })
        .map(|d| d.column.clone())
}

fn primary_key(database: &Database, table: &str) -> Option<String> {
    database.schema_view(table)?.schema().primary_key.clone()
}

/// `TOP (n)` / `TOP n` right after `SELECT [DISTINCT]`: removed, and its
/// count returned.
fn take_top(
    tokens: &mut Vec<Token>,
    at: usize,
    cx: &mut Cx,
) -> Result<Option<Vec<Token>>, TsqlError> {
    if !kw(tokens, at, "TOP") {
        return Ok(None);
    }
    let (count, end) = if tokens.get(at + 1).is_some_and(|t| t.is_op("(")) {
        let close =
            matching_paren(tokens, at + 1).ok_or_else(|| TsqlError::syntax("unbalanced TOP"))?;
        (tokens[at + 2..close].to_vec(), close + 1)
    } else {
        (tokens.get(at + 1).cloned().into_iter().collect(), at + 2)
    };
    if kw(tokens, end, "PERCENT") || (kw(tokens, end, "WITH") && kw(tokens, end + 1, "TIES")) {
        return Err(TsqlError::unsupported("TOP … PERCENT / WITH TIES"));
    }
    tokens.drain(at..end);
    Ok(Some(rewrite_expr(count, cx)?))
}

/// Rewrites a SELECT. `name_items` names unaliased columns as written.
fn rewrite_select(
    sql: &str,
    tokens: Vec<Token>,
    cx: &mut Cx,
    name_items: bool,
) -> Result<Vec<Token>, TsqlError> {
    // A query has its own select list, wherever it sits.
    let outer = std::mem::replace(&mut cx.past_select_list, false);
    let r = match find_top(&tokens, 1, &["FOR"]) {
        Some(f) if kw(&tokens, f + 1, "JSON") => for_json(tokens, f, cx, name_items),
        Some(f) if kw(&tokens, f + 1, "XML") => for_xml(tokens, f, cx, name_items),
        _ => rewrite_select_inner(sql, tokens, cx, name_items),
    };
    cx.past_select_list = outer;
    r
}

/// FOR JSON / FOR XML's result column, as SQL Server names it.
const JSON_COLUMN: &str = "JSON_F52E2B61-18A1-11d1-B105-00805F49916B";
const XML_COLUMN: &str = "XML_F52E2B61-18A1-11d1-B105-00805F49916B";

/// A FOR JSON / FOR XML query's parts: its select items (expression, key)
/// and what follows them, split at ORDER BY.
struct Shaped {
    items: Vec<(Vec<Token>, String)>,
    tail: Vec<Token>,
    order: Option<Vec<Token>>,
    /// DISTINCT, TOP, GROUP BY, aggregates, OFFSET, UNION: the rows are
    /// those of the query as written, read through a derived table.
    derived: bool,
    /// An item holds a subquery. The engine answers a correlated subquery
    /// wrongly inside a derived table and refuses one beside HAVING, so
    /// neither is used then.
    subquery: bool,
}

fn shape_for(tokens: &[Token], cx: &Cx, what: &str) -> Result<Shaped, TsqlError> {
    let mut i = 1;
    let mut derived = false;
    if kw(tokens, i, "DISTINCT") || kw(tokens, i, "TOP") {
        derived = true;
    }
    if kw(tokens, i, "DISTINCT") || kw(tokens, i, "ALL") {
        i += 1;
    }
    if kw(tokens, i, "TOP") {
        i += 1;
        if tokens.get(i).is_some_and(|t| t.is_op("(")) {
            i = matching_paren(tokens, i).ok_or_else(|| TsqlError::syntax("unbalanced TOP"))? + 1;
        } else {
            i += 1;
        }
    }
    let end = find_top(
        tokens,
        i,
        &[
            "FROM",
            "WHERE",
            "GROUP",
            "HAVING",
            "ORDER",
            "UNION",
            "EXCEPT",
            "INTERSECT",
            "OPTION",
            "OFFSET",
        ],
    )
    .unwrap_or(tokens.len());
    let mut items = Vec::new();
    for item in split_commas(&tokens[i..end]) {
        if item.len() == 1 && item[0].is_op("*") {
            // SELECT * FROM one table: its columns.
            let from = find_top(tokens, end, &["FROM"])
                .ok_or_else(|| TsqlError::unsupported(format!("FOR {what} of SELECT *")))?;
            let (table, after) = table_name_at(tokens, from + 1)
                .ok_or_else(|| TsqlError::unsupported(format!("FOR {what} of SELECT *")))?;
            if find_top(tokens, after, &["JOIN"]).is_some()
                || tokens.get(after).is_some_and(|t| t.is_op(","))
            {
                return Err(TsqlError::unsupported(format!(
                    "FOR {what} of SELECT * over a join"
                )));
            }
            let view =
                cx.env.database().schema_view(&table).ok_or_else(|| {
                    TsqlError::new(208, format!("Invalid object name '{table}'."))
                })?;
            for f in view
                .schema()
                .fields
                .iter()
                .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
            {
                items.push((
                    vec![Token::new(Tok::Quoted(f.name.clone()))],
                    f.name.clone(),
                ));
            }
            continue;
        }
        let (expr, alias) = if item.len() >= 3
            && item[1].is_op("=")
            && matches!(item[0].tok, Tok::Word(_) | Tok::Quoted(_) | Tok::Str(_))
        {
            let name = match &item[0].tok {
                Tok::Word(w) | Tok::Quoted(w) => w.clone(),
                Tok::Str(b) => String::from_utf8_lossy(b).into_owned(),
                _ => unreachable!(),
            };
            (item[2..].to_vec(), Some(name))
        } else {
            split_alias(&item)
        };
        let column = expr.len() % 2 == 1
            && expr.iter().enumerate().all(|(k, t)| {
                if k % 2 == 1 {
                    t.is_op(".")
                } else {
                    matches!(t.tok, Tok::Word(_) | Tok::Quoted(_))
                }
            });
        let key = match alias {
            Some(a) => a,
            None if column => expr
                .last()
                .and_then(|t| t.ident())
                .unwrap_or_default()
                .to_string(),
            None => String::new(),
        };
        let aggregate = expr.iter().enumerate().any(|(k, t)| {
            t.upper().is_some_and(|w| {
                matches!(
                    w.as_str(),
                    "COUNT" | "COUNT_BIG" | "SUM" | "AVG" | "MIN" | "MAX" | "STRING_AGG"
                )
            }) && expr.get(k + 1).is_some_and(|o| o.is_op("("))
        });
        derived |= aggregate;
        items.push((expr, key));
    }
    let mut tail = tokens[end..].to_vec();
    if find_top(
        &tail,
        0,
        &["GROUP", "HAVING", "OFFSET", "UNION", "EXCEPT", "INTERSECT"],
    )
    .is_some()
    {
        derived = true;
    }
    let mut order = None;
    if let Some(o) = find_top(&tail, 0, &["ORDER"]) {
        if kw(&tail, o + 1, "BY") && !derived {
            order = Some(tail[o + 2..].to_vec());
            tail.truncate(o);
        }
    }
    let subquery = items.iter().any(|(e, _)| {
        e.windows(2)
            .any(|w| w[0].is_op("(") && w[1].is_word("SELECT"))
    });
    if derived && subquery {
        return Err(TsqlError::unsupported(format!(
            "FOR {what} with a subquery in a query with DISTINCT, TOP, GROUP BY or aggregates"
        )));
    }
    Ok(Shaped {
        items,
        tail,
        order,
        derived,
        subquery,
    })
}

/// The options after FOR JSON / FOR XML: (mode, its argument, ROOT,
/// other words).
fn for_options(tokens: &[Token]) -> (String, Option<String>, Option<String>, Vec<String>) {
    let mut mode = String::new();
    let mut arg = None;
    let mut root = None;
    let mut words = Vec::new();
    for (k, part) in split_commas(tokens).into_iter().enumerate() {
        let word = part.first().and_then(|t| t.upper()).unwrap_or_default();
        let inner = if part.get(1).is_some_and(|t| t.is_op("(")) {
            part.get(2).and_then(|t| match &t.tok {
                Tok::Str(b) => Some(String::from_utf8_lossy(b).into_owned()),
                _ => None,
            })
        } else {
            None
        };
        if k == 0 {
            mode = word;
            arg = inner;
        } else if word == "ROOT" {
            root = Some(inner.unwrap_or_else(|| "root".into()));
        } else {
            words.push(word);
        }
    }
    (mode, arg, root, words)
}

/// A JSON string's text, as SQL text: `"key"` escaped, then quoted.
fn json_key_sql(key: &str) -> String {
    let mut j = String::from("\"");
    for c in key.chars() {
        match c {
            '"' => j.push_str("\\\""),
            '\\' => j.push_str("\\\\"),
            c if (c as u32) < 0x20 => j.push_str(&format!("\\u{:04x}", c as u32)),
            c => j.push(c),
        }
    }
    j.push('"');
    sql_str(&j)
}

/// Whether an item's expression is JSON already (a FOR JSON subquery,
/// JSON_QUERY), which goes in as it is rather than as a string.
fn is_json_already(expr: &[Token]) -> bool {
    (expr.first().is_some_and(|t| t.is_op("("))
        && kw(expr, 1, "SELECT")
        && matching_paren(expr, 0) == Some(expr.len() - 1)
        && find_top(&expr[1..expr.len() - 1], 1, &["FOR"])
            .is_some_and(|f| kw(&expr[1..], f + 1, "JSON")))
        || (kw(expr, 0, "JSON_QUERY") && expr.get(1).is_some_and(|t| t.is_op("(")))
}

/// One row's JSON object, from (key path, value SQL, already JSON).
fn json_object_sql(members: &[(Vec<String>, String, bool)], include_nulls: bool) -> String {
    // Nested by the key paths' first parts, in the order they appear.
    let mut groups: Vec<(String, Vec<(Vec<String>, String, bool)>)> = Vec::new();
    let mut parts = Vec::new();
    for (path, value, raw) in members {
        if path.len() > 1 {
            match groups.iter_mut().find(|(g, _)| *g == path[0]) {
                Some((_, v)) => v.push((path[1..].to_vec(), value.clone(), *raw)),
                None => {
                    groups.push((
                        path[0].clone(),
                        vec![(path[1..].to_vec(), value.clone(), *raw)],
                    ));
                    parts.push(format!("\u{0}{}", path[0]));
                }
            }
            continue;
        }
        let key = json_key_sql(&format!("{}", path[0]));
        let json = if *raw {
            format!("({value})")
        } else {
            format!("(to_jsonb({value}))::text")
        };
        parts.push(if include_nulls {
            format!("{key} || ':' || coalesce({json}, 'null')")
        } else {
            format!("CASE WHEN ({value}) IS NULL THEN NULL ELSE {key} || ':' || {json} END")
        });
    }
    let parts: Vec<String> = parts
        .into_iter()
        .map(|p| match p.strip_prefix('\u{0}') {
            Some(group) => {
                let members = &groups.iter().find(|(g, _)| g == group).unwrap().1;
                format!(
                    "{} || ':' || {}",
                    json_key_sql(group),
                    json_object_sql(members, include_nulls)
                )
            }
            None => p,
        })
        .collect();
    format!(
        "('{{' || coalesce(concat_ws(',', {}), '') || '}}')",
        parts.join(", ")
    )
}

fn for_json(
    tokens: Vec<Token>,
    at: usize,
    cx: &mut Cx,
    top_level: bool,
) -> Result<Vec<Token>, TsqlError> {
    let (mode, _, root, words) = for_options(&tokens[at + 2..]);
    if mode != "PATH" && mode != "AUTO" {
        return Err(TsqlError::syntax("FOR JSON needs PATH or AUTO"));
    }
    let query = &tokens[..at];
    if mode == "AUTO" && find_top(query, 1, &["JOIN"]).is_some() {
        return Err(TsqlError::unsupported(
            "FOR JSON AUTO over a join (its nesting by table); use FOR JSON PATH",
        ));
    }
    let include_nulls = words.iter().any(|w| w == "INCLUDE_NULL_VALUES");
    let unwrapped = words.iter().any(|w| w == "WITHOUT_ARRAY_WRAPPER");
    let shaped = shape_for(query, cx, "JSON")?;
    if let Some((_, _)) = shaped.items.iter().find(|(_, k)| k.is_empty()) {
        return Err(TsqlError::new(
            13605,
            "Column expressions and data sources without names or aliases cannot be formatted as JSON text using FOR JSON clause. Add alias to the unnamed column or table.",
        ));
    }
    let path = |key: &str| -> Vec<String> {
        if mode == "PATH" {
            // A bracketed alias's dots arrive as `lex::NAME_DOT`.
            super::lex::decode_dots(&key).split('.').map(str::to_string).collect()
        } else {
            vec![key.to_string()]
        }
    };
    let (members, from_sql) = if shaped.derived {
        // The query as written, its items named c0, c1, …, read through a
        // derived table.
        let mut inner = vec![tokens[0].clone()];
        let select_end = find_top(
            query,
            1,
            &[
                "FROM",
                "WHERE",
                "GROUP",
                "HAVING",
                "ORDER",
                "UNION",
                "EXCEPT",
                "INTERSECT",
                "OPTION",
                "OFFSET",
            ],
        )
        .unwrap_or(query.len());
        let first_item = (1..select_end)
            .find(|&k| !(kw(query, k, "DISTINCT") || kw(query, k, "ALL") || kw(query, k, "TOP")))
            .unwrap_or(1);
        let mut k = 1;
        while k < first_item {
            inner.push(query[k].clone());
            k += 1;
        }
        // TOP (n): keep its parenthesized count.
        if kw(query, first_item.saturating_sub(1), "TOP")
            && query.get(first_item).is_some_and(|t| t.is_op("("))
        {
            let close = matching_paren(query, first_item).unwrap_or(first_item);
            inner.extend(query[first_item..=close].iter().cloned());
        } else if kw(query, first_item.saturating_sub(1), "TOP") {
            inner.push(query[first_item].clone());
        }
        for (n, (expr, _)) in shaped.items.iter().enumerate() {
            if n > 0 {
                inner.push(Token::op(","));
            }
            inner.extend(expr.iter().cloned());
            inner.push(Token::word("AS"));
            inner.push(Token::new(Tok::Quoted(format!("c{n}"))));
        }
        inner.extend(shaped.tail.iter().cloned());
        let body = render_text(&rewrite_select_inner("", inner, cx, false)?);
        let members = shaped
            .items
            .iter()
            .enumerate()
            .map(|(n, (expr, key))| {
                (
                    path(key),
                    format!("__json.{}", quote_ident(&format!("c{n}"))),
                    is_json_already(expr),
                )
            })
            .collect::<Vec<_>>();
        (members, format!("FROM ({body}) AS __json"))
    } else {
        let mut members = Vec::new();
        for (expr, key) in &shaped.items {
            let value = render_text(&rewrite_expr(expr.clone(), cx)?);
            members.push((path(key), value, is_json_already(expr)));
        }
        let outer = std::mem::replace(&mut cx.past_select_list, true);
        let tail = rewrite_expr(shaped.tail.clone(), cx);
        cx.past_select_list = outer;
        (members, render_text(&tail?))
    };
    let object = json_object_sql(&members, include_nulls);
    let order = match &shaped.order {
        Some(o) => {
            let outer = std::mem::replace(&mut cx.past_select_list, true);
            let r = rewrite_expr(o.clone(), cx);
            cx.past_select_list = outer;
            format!(" ORDER BY {}", render_text(&r?))
        }
        None => String::new(),
    };
    let rows = format!("string_agg({object}, ','{order})");
    let mut value = if unwrapped {
        rows
    } else {
        format!("('[' || {rows} || ']')")
    };
    if let Some(r) = root {
        value = format!(
            "('{{' || {} || ':' || coalesce({value}, '[]') || '}}')",
            json_key_sql(&r)
        );
    }
    // At the top a query of no rows answers no rows; as a subquery, NULL.
    // (With a subquery in its items: a row holding NULL.)
    let having = if top_level && !shaped.subquery {
        " HAVING count(*) > 0"
    } else {
        ""
    };
    Ok(vec![Token::raw(format!(
        "SELECT {value} AS {} {from_sql}{having}",
        quote_ident(JSON_COLUMN)
    ))])
}

fn xml_escape_sql(value: &str) -> String {
    format!(
        "replace(replace(replace(CAST({value} AS text), '&', '&amp;'), '<', '&lt;'), '>', '&gt;')"
    )
}

/// FOR XML PATH: rows as elements (`PATH('')`: none), columns as elements
/// named by their aliases (an unnamed column: its text alone), ROOT.
fn for_xml(
    tokens: Vec<Token>,
    at: usize,
    cx: &mut Cx,
    top_level: bool,
) -> Result<Vec<Token>, TsqlError> {
    let (mode, arg, root, words) = for_options(&tokens[at + 2..]);
    if mode != "PATH" {
        return Err(TsqlError::unsupported(format!(
            "FOR XML {} (FOR XML PATH is)",
            mode.to_lowercase()
        )));
    }
    if words.iter().any(|w| w != "TYPE") {
        return Err(TsqlError::unsupported(format!(
            "FOR XML … {}",
            words.join(", ")
        )));
    }
    let row = arg.unwrap_or_else(|| "row".into());
    let shaped = shape_for(&tokens[..at], cx, "XML")?;
    if shaped.derived {
        return Err(TsqlError::unsupported(
            "FOR XML over DISTINCT, TOP, GROUP BY or aggregates",
        ));
    }
    let escape = !cx.xml_value;
    let mut parts = Vec::new();
    for (expr, key) in &shaped.items {
        let value = render_text(&rewrite_expr(expr.clone(), cx)?);
        let text = if escape {
            xml_escape_sql(&value)
        } else {
            format!("CAST({value} AS text)")
        };
        // A column named by an alias is an element; an unnamed one (or
        // one aliased [*]) is its text.
        let named = !key.is_empty() && key != "*";
        parts.push(if named {
            format!(
                "CASE WHEN ({value}) IS NULL THEN NULL ELSE {} || {text} || {} END",
                sql_str(&format!("<{key}>")),
                sql_str(&format!("</{key}>"))
            )
        } else {
            text
        });
    }
    let mut object = format!("concat({})", parts.join(", "));
    if !row.is_empty() {
        object = format!(
            "({} || {object} || {})",
            sql_str(&format!("<{row}>")),
            sql_str(&format!("</{row}>"))
        );
    }
    let outer = std::mem::replace(&mut cx.past_select_list, true);
    let tail = rewrite_expr(shaped.tail.clone(), cx);
    let order = match &shaped.order {
        Some(o) => rewrite_expr(o.clone(), cx).map(|o| format!(" ORDER BY {}", render_text(&o))),
        None => Ok(String::new()),
    };
    cx.past_select_list = outer;
    let (tail, order) = (render_text(&tail?), order?);
    let mut value = format!("string_agg({object}, ''{order})");
    if let Some(r) = root {
        value = format!(
            "({} || coalesce({value}, '') || {})",
            sql_str(&format!("<{r}>")),
            sql_str(&format!("</{r}>"))
        );
    }
    let having = if top_level && !shaped.subquery {
        " HAVING count(*) > 0"
    } else {
        ""
    };
    Ok(vec![Token::raw(format!(
        "SELECT {value} AS {} {tail}{having}",
        quote_ident(XML_COLUMN)
    ))])
}

fn rewrite_select_inner(
    sql: &str,
    tokens: Vec<Token>,
    cx: &mut Cx,
    name_items: bool,
) -> Result<Vec<Token>, TsqlError> {
    let mut tokens = tokens;
    let mut i = 0usize;
    let mut out = Vec::new();
    while tokens.get(i).is_some_and(|t| t.is_op("(")) {
        out.push(tokens[i].clone());
        i += 1;
    }
    if !kw(&tokens, i, "SELECT") {
        return rewrite_expr(tokens, cx);
    }
    out.push(tokens[i].clone());
    i += 1;
    if kw(&tokens, i, "DISTINCT") || kw(&tokens, i, "ALL") {
        out.push(tokens[i].clone());
        i += 1;
    }
    let top = take_top(&mut tokens, i, cx)?;
    let end = find_top(
        &tokens,
        i,
        &[
            "FROM",
            "INTO",
            "WHERE",
            "GROUP",
            "HAVING",
            "ORDER",
            "UNION",
            "EXCEPT",
            "INTERSECT",
            "OPTION",
            "FOR",
            "OFFSET",
        ],
    )
    .unwrap_or(tokens.len());
    let items = split_commas(&tokens[i..end]);
    let mut prepared = Vec::new();
    for item in items {
        // T-SQL's `alias = expr`.
        let (expr, alias) = if item.len() >= 3
            && item[1].is_op("=")
            && matches!(item[0].tok, Tok::Word(_) | Tok::Quoted(_) | Tok::Str(_))
        {
            let name = match &item[0].tok {
                Tok::Word(w) | Tok::Quoted(w) => w.clone(),
                Tok::Str(s) => String::from_utf8_lossy(s).into_owned(),
                _ => unreachable!(),
            };
            (item[2..].to_vec(), Some(name))
        } else {
            split_alias(&item)
        };
        if let Some(a) = &alias {
            cx.aliases.push(a.clone());
            if name_items {
                cx.top_aliases.push(a.clone());
            }
        }
        let blank = name_items && alias.is_none() && !is_column_reference(&expr);
        let name = alias.or_else(|| {
            if name_items {
                implicit_name(sql, &expr)
            } else {
                None
            }
        });
        // SQL Server leaves an unaliased expression's column unnamed
        // (`SELECT COUNT_BIG(*)`), as tools keyed on names expect; the
        // engine would label it `count`. `?column?` is sent as no name.
        let name = name.or_else(|| blank.then(|| BLANK_NAME.to_string()));
        prepared.push((expr, name));
    }
    let saved = std::mem::take(&mut cx.aliases);
    // Each item's rewritten text and alias: an ORDER BY of the same
    // expression is sent as the alias, which the engine resolves where it
    // would refuse a grouped expression it does not see in the list.
    let mut item_texts: Vec<(String, String)> = Vec::new();
    for (k, (expr, name)) in prepared.into_iter().enumerate() {
        if k > 0 {
            out.push(Token::op(","));
        }
        // A whole-number literal is an `int` in T-SQL (EF Core selects
        // `1 AS [c]` and reads it with GetInt32); the engine's is a bigint.
        let int_literal = matches!(&expr[..], [t] if matches!(&t.tok, Tok::Num(n)
            if n.parse::<i32>().is_ok()));
        let mut rewritten = rewrite_expr(expr, cx)?;
        if int_literal {
            let n = render_text(&rewritten);
            rewritten = vec![Token::raw(format!("CAST({n} AS integer)"))];
        }
        if let Some(n) = name.as_ref().filter(|n| *n != BLANK_NAME) {
            item_texts.push((render_text(&rewritten), n.clone()));
        }
        out.extend(rewritten);
        if let Some(name) = name {
            out.push(Token::word("AS"));
            out.push(Token::raw(quote_ident(&name)));
        }
    }
    cx.aliases = saved;
    let mut rest: Vec<Token> = tokens[end..].to_vec();
    if let Some(order) = find_top(&rest, 0, &["ORDER"]) {
        if kw(&rest, order + 1, "BY") && !item_texts.is_empty() {
            let stop = find_top(
                &rest,
                order + 2,
                &["OFFSET", "OPTION", "FOR", "UNION", "EXCEPT", "INTERSECT"],
            )
            .unwrap_or(rest.len());
            let mut terms = Vec::new();
            for term in split_commas(&rest[order + 2..stop]) {
                let mut body = term.clone();
                let mut direction = Vec::new();
                if body
                    .last()
                    .is_some_and(|t| t.is_word("ASC") || t.is_word("DESC"))
                {
                    direction.push(body.pop().unwrap());
                }
                let mut probe = Cx::new(cx.env);
                let text = render_text(&rewrite_expr(body.clone(), &mut probe)?);
                match item_texts.iter().find(|(t, _)| *t == text) {
                    Some((_, alias)) => {
                        let mut replaced = vec![Token::raw(quote_ident(alias))];
                        replaced.extend(direction);
                        terms.push(replaced);
                    }
                    None => terms.push(term),
                }
            }
            let mut rebuilt: Vec<Token> = rest[..order + 2].to_vec();
            for (n, t) in terms.into_iter().enumerate() {
                if n > 0 {
                    rebuilt.push(Token::op(","));
                }
                rebuilt.extend(t);
            }
            rebuilt.extend(rest[stop..].iter().cloned());
            rest = rebuilt;
        }
    }
    // OFFSET n ROWS FETCH NEXT m ROWS ONLY
    let mut limit: Option<Vec<Token>> = top;
    let mut offset: Option<Vec<Token>> = None;
    if let Some(at) = find_top(&rest, 0, &["OFFSET"]) {
        let rows_at = find_top(&rest, at, &["ROWS", "ROW"])
            .ok_or_else(|| TsqlError::syntax("OFFSET needs ROWS"))?;
        offset = Some(rest[at + 1..rows_at].to_vec());
        let mut tail_start = rows_at + 1;
        if kw(&rest, tail_start, "FETCH") {
            let rows2 = find_top(&rest, tail_start, &["ROWS", "ROW"])
                .ok_or_else(|| TsqlError::syntax("FETCH needs ROWS ONLY"))?;
            limit = Some(rest[tail_start + 2..rows2].to_vec());
            tail_start = rows2 + 1;
            if kw(&rest, tail_start, "ONLY") {
                tail_start += 1;
            }
        }
        let tail: Vec<Token> = rest[tail_start..].to_vec();
        rest.truncate(at);
        rest.extend(tail);
    }
    if find_top(&rest, 0, &["UNION", "EXCEPT", "INTERSECT"]).is_some() && limit.is_some() {
        return Err(TsqlError::unsupported("TOP or OFFSET in a UNION branch"));
    }
    if let Some(u) = find_top(&rest, 0, &["UNION", "EXCEPT", "INTERSECT"]) {
        let mut j = u + 1;
        if kw(&rest, j, "ALL") {
            j += 1;
        }
        let outer = std::mem::replace(&mut cx.past_select_list, true);
        let tail = rewrite_expr(rest[..j].to_vec(), cx);
        cx.past_select_list = outer;
        out.extend(tail?);
        out.extend(rewrite_select(sql, rest[j..].to_vec(), cx, false)?);
        cx.aliases.clear();
        return Ok(out);
    }
    // FOR JSON / FOR XML / FOR UPDATE.
    if let Some(f) = find_top(&rest, 0, &["FOR"]) {
        if kw(&rest, f + 1, "JSON") || kw(&rest, f + 1, "XML") || kw(&rest, f + 1, "BROWSE") {
            return Err(TsqlError::unsupported("FOR JSON / FOR XML"));
        }
    }
    let rest = if reads_catalog(&rest) { fold_catalog_names(rest) } else { rest };
    let outer = std::mem::replace(&mut cx.past_select_list, true);
    let tail = rewrite_expr(rest, cx);
    cx.past_select_list = outer;
    out.extend(tail?);
    if let Some(l) = limit {
        out.push(Token::word("LIMIT"));
        out.extend(rewrite_expr(l, cx)?);
    }
    if let Some(o) = offset {
        out.push(Token::word("OFFSET"));
        out.extend(rewrite_expr(o, cx)?);
    }
    cx.aliases.clear();
    Ok(out)
}

fn split_alias(item: &[Token]) -> (Vec<Token>, Option<String>) {
    let n = item.len();
    let name_of = |t: &Token| match &t.tok {
        Tok::Word(w) | Tok::Quoted(w) => Some(w.clone()),
        Tok::Str(s) => Some(String::from_utf8_lossy(s).into_owned()),
        _ => None,
    };
    if n >= 3 && item[n - 2].is_word("AS") {
        if let Some(a) = name_of(&item[n - 1]) {
            return (item[..n - 2].to_vec(), Some(a));
        }
    }
    if n >= 2 {
        let last = &item[n - 1];
        let before = &item[n - 2];
        const NOT_ALIAS: &[&str] = &[
            "END", "NULL", "AS", "AND", "OR", "NOT", "IS", "IN", "LIKE", "THEN", "ELSE", "WHEN",
            "CASE", "BETWEEN", "DESC", "ASC", "DISTINCT", "COLLATE",
        ];
        let implicit = match &last.tok {
            Tok::Quoted(_) | Tok::Str(_) => true,
            Tok::Word(w) => !NOT_ALIAS.iter().any(|k| k.eq_ignore_ascii_case(w)),
            _ => false,
        };
        let operand_before = matches!(
            before.tok,
            Tok::Word(_)
                | Tok::Quoted(_)
                | Tok::Num(_)
                | Tok::Str(_)
                | Tok::UserVar(_)
                | Tok::SysVar(_)
        ) || before.is_op(")");
        let before_keyword = before
            .upper()
            .is_some_and(|w| NOT_ALIAS.contains(&w.as_str()) && w != "END" && w != "NULL");
        if implicit && operand_before && !before_keyword && !before.is_op(".") {
            if let Some(a) = name_of(last) {
                return (item[..n - 1].to_vec(), Some(a));
            }
        }
    }
    (item.to_vec(), None)
}

/// Whether `tokens` read a catalogue view (`sys.x`, `INFORMATION_SCHEMA.x`).
fn reads_catalog(tokens: &[Token]) -> bool {
    tokens.windows(2).any(|w| {
        w[1].is_op(".")
            && w[0].ident().is_some_and(|n| n.eq_ignore_ascii_case("sys") || n.eq_ignore_ascii_case("INFORMATION_SCHEMA"))
    })
}

/// A catalogue view's name column compared with one value (`name =
/// N'orders'`, `TABLE_NAME = @t`, `name LIKE …`), compared without case, as
/// SQL Server's catalogue compares under the database's collation: the views
/// list names as they were declared (`NAMES_TABLE`), and a lookup spelled
/// otherwise found nothing.
fn fold_catalog_names(tokens: Vec<Token>) -> Vec<Token> {
    const NAMES: &[&str] = &[
        "name", "table_name", "column_name", "constraint_name", "routine_name", "specific_name", "type_desc",
    ];
    let is_name = |t: &Token| t.ident().is_some_and(|n| NAMES.iter().any(|k| n.eq_ignore_ascii_case(k)));
    let single = |t: &Token| matches!(t.tok, Tok::Str(_) | Tok::UserVar(_) | Tok::Param(_));
    let mut out: Vec<Token> = Vec::with_capacity(tokens.len() + 8);
    let mut i = 0;
    while i < tokens.len() {
        // `[q.]name <op> value`
        let (start, end) = if is_name(&tokens[i]) {
            (i, i + 1)
        } else {
            out.push(tokens[i].clone());
            i += 1;
            continue;
        };
        let qualified = out.last().is_some_and(|t| t.is_op(".")) && out.len() >= 2;
        let op = tokens.get(end);
        let compares = op.is_some_and(|o| o.is_op("=") || o.is_op("<>") || o.is_op("!=") || o.is_word("LIKE"));
        let like = op.is_some_and(|o| o.is_word("LIKE"));
        let literal = tokens.get(end + 1).is_some_and(|t| matches!(t.tok, Tok::Str(_)));
        // LIKE takes no call on its pattern side: only a literal pattern,
        // lowercased here, folds.
        if compares
            && (literal || !like)
            && tokens.get(end + 1).is_some_and(single)
            && !tokens.get(end + 2).is_some_and(|t| t.is_op("."))
        {
            // The qualifier already written goes inside the call.
            let mut left: Vec<Token> = Vec::new();
            if qualified {
                let dot = out.pop().unwrap();
                let q = out.pop().unwrap();
                left.push(q);
                left.push(dot);
            }
            left.extend(tokens[start..end].iter().cloned());
            let mut call = Token::raw("lower");
            call.space = left.first().is_some_and(|t| t.space);
            out.push(call);
            let mut open = Token::op("(");
            open.space = false;
            out.push(open);
            for (k, t) in left.into_iter().enumerate() {
                let mut t = t;
                if k == 0 {
                    t.space = false;
                }
                out.push(t);
            }
            out.push(Token::op(")"));
            out.push(tokens[end].clone());
            let mut value = tokens[end + 1].clone();
            if let Tok::Str(text) = &mut value.tok {
                *text = String::from_utf8_lossy(text).to_lowercase().into_bytes();
                out.push(value);
            } else {
                out.push(Token::raw("lower"));
                let mut open = Token::op("(");
                open.space = false;
                out.push(open);
                value.space = false;
                out.push(value);
                out.push(Token::op(")"));
            }
            i = end + 2;
            continue;
        }
        out.push(tokens[i].clone());
        i += 1;
    }
    out
}

/// The name an unaliased expression item is given, which the front end
/// sends as none (`conn::exchange`).
const BLANK_NAME: &str = "?column?";

/// Whether a select item is a (qualified) column, or a `*`.
fn is_column_reference(item: &[Token]) -> bool {
    if item.last().is_some_and(|t| t.is_op("*")) {
        return true;
    }
    item.len() % 2 == 1
        && item.iter().enumerate().all(|(k, t)| {
            if k % 2 == 1 {
                t.is_op(".")
            } else {
                matches!(t.tok, Tok::Word(_) | Tok::Quoted(_)) && !t.is_word("NULL") && !t.is_word("TRUE") && !t.is_word("FALSE")
            }
        })
}

fn implicit_name(_sql: &str, item: &[Token]) -> Option<String> {
    if item.last().is_some_and(|t| t.is_op("*")) {
        return None;
    }
    let is_column = item.len() % 2 == 1
        && item.iter().enumerate().all(|(k, t)| {
            if k % 2 == 1 {
                t.is_op(".")
            } else {
                matches!(t.tok, Tok::Word(_) | Tok::Quoted(_))
            }
        });
    if is_column {
        let name = item.last()?.ident()?.to_string();
        if name == name.to_lowercase() {
            return None;
        }
        return Some(name);
    }
    None
}

/// `OUTPUT INSERTED.a, DELETED.b` as a RETURNING list.
fn output_clause(
    tokens: &[Token],
    at: usize,
    end: usize,
    deleted_ok: bool,
    cx: &mut Cx,
) -> Result<Vec<Token>, TsqlError> {
    let mut items = Vec::new();
    for part in split_commas(&tokens[at + 1..end]) {
        if part.iter().any(|t| t.is_word("INTO")) {
            return Err(TsqlError::unsupported("OUTPUT … INTO"));
        }
        let mut cleaned = Vec::new();
        let mut k = 0;
        while k < part.len() {
            let t = &part[k];
            if let Some(w) = t.ident() {
                let lw = w.to_ascii_lowercase();
                if (lw == "inserted" || lw == "deleted")
                    && part.get(k + 1).is_some_and(|d| d.is_op("."))
                {
                    if lw == "deleted" && !deleted_ok {
                        return Err(TsqlError::unsupported("OUTPUT DELETED in an UPDATE"));
                    }
                    k += 2;
                    continue;
                }
            }
            cleaned.push(t.clone());
            k += 1;
        }
        let (expr, alias) = split_alias(&cleaned);
        let name = alias.or_else(|| expr.last().and_then(|t| t.ident()).map(str::to_string));
        let mut item = rewrite_expr(expr, cx)?;
        if let Some(n) = name {
            if n != "*" {
                item.push(Token::word("AS"));
                item.push(Token::raw(quote_ident(&n)));
            }
        }
        items.push(item);
    }
    let mut out = vec![Token::word("RETURNING")];
    for (k, item) in items.into_iter().enumerate() {
        if k > 0 {
            out.push(Token::op(","));
        }
        out.extend(item);
    }
    Ok(out)
}

fn rewrite_insert(sql: &str, tokens: Vec<Token>, cx: &mut Cx) -> Result<Forward, TsqlError> {
    let mut i = 1usize;
    if kw(&tokens, i, "TOP") {
        return Err(TsqlError::unsupported("INSERT TOP"));
    }
    if kw(&tokens, i, "INTO") {
        i += 1;
    }
    let (table, mut j) =
        table_name_at(&tokens, i).ok_or_else(|| TsqlError::syntax("INSERT needs a table"))?;
    let mut out = vec![
        Token::word("INSERT"),
        Token::word("INTO"),
        Token::raw(ident_sql(&table)),
    ];
    let mut explicit_columns: Vec<String> = Vec::new();
    if kw(&tokens, j, "WITH") && tokens.get(j + 1).is_some_and(|t| t.is_op("(")) {
        j = matching_paren(&tokens, j + 1).ok_or_else(|| TsqlError::syntax("unbalanced hint"))? + 1;
    }
    if tokens.get(j).is_some_and(|t| t.is_op("(")) {
        let close = matching_paren(&tokens, j)
            .ok_or_else(|| TsqlError::syntax("unbalanced column list"))?;
        if !kw(&tokens, j + 1, "SELECT") {
            let names: Vec<String> = split_commas(&tokens[j + 1..close])
                .iter()
                .filter_map(|p| p.last().and_then(|t| t.ident()).map(ident_sql))
                .collect();
            out.push(Token::raw(format!("({})", names.join(", "))));
            explicit_columns = names;
            j = close + 1;
        }
    }
    let mut returning = None;
    if kw(&tokens, j, "OUTPUT") {
        let end = find_top(
            &tokens,
            j,
            &["VALUES", "SELECT", "DEFAULT", "EXEC", "EXECUTE"],
        )
        .ok_or_else(|| TsqlError::syntax("OUTPUT needs VALUES or SELECT after it"))?;
        returning = Some(output_clause(&tokens, j, end, false, cx)?);
        j = end;
    }
    if kw(&tokens, j, "DEFAULT") && kw(&tokens, j + 1, "VALUES") {
        out.push(Token::raw("DEFAULT VALUES"));
    } else if kw(&tokens, j, "VALUES") {
        out.push(Token::word("VALUES"));
        out.extend(rewrite_expr(tokens[j + 1..].to_vec(), cx)?);
    } else if kw(&tokens, j, "EXEC") || kw(&tokens, j, "EXECUTE") {
        return Err(TsqlError::unsupported("INSERT … EXEC"));
    } else {
        out.extend(rewrite_select(sql, tokens[j..].to_vec(), cx, false)?);
    }
    let database = cx.env.database().clone();
    let kind = match returning {
        Some(r) => {
            out.extend(r);
            Kind::Query
        }
        None => match identity_column(&database, &table) {
            Some(col) => {
                out.push(Token::raw(format!("RETURNING {}", ident_sql(&col))));
                Kind::Insert {
                    identity: Some(col),
                    written: None,
                }
            }
            None => Kind::Insert { identity: None, written: None },
        },
    };
    let kind = match kind {
        Kind::Insert { identity: Some(col), .. } => {
            let written = explicit_columns
                .iter()
                .any(|c| c.eq_ignore_ascii_case(&ident_sql(&col)))
                .then(|| (table.clone(), col.clone()));
            Kind::Insert { identity: Some(col), written }
        }
        other => other,
    };
    Ok(Forward {
        statements: vec![render_text(&out)],
        kind,
        extra_column: None,
        aliases: Vec::new(),
        database: None,
    })
}

/// `… TOP (n)` on UPDATE / DELETE: the rows chosen by primary key.
fn limited(
    table: &str,
    where_sql: Option<String>,
    count: Vec<Token>,
    cx: &mut Cx,
) -> Result<String, TsqlError> {
    let key = primary_key(cx.env.database(), table).ok_or_else(|| {
        TsqlError::unsupported("UPDATE/DELETE TOP on a table without a primary key")
    })?;
    let key = ident_sql(&key);
    Ok(format!(
        "{key} IN (SELECT {key} FROM {}{} LIMIT {})",
        ident_sql(table),
        where_sql.map(|w| format!(" WHERE {w}")).unwrap_or_default(),
        render_text(&count)
    ))
}

fn rewrite_update(tokens: Vec<Token>, cx: &mut Cx) -> Result<Forward, TsqlError> {
    let mut tokens = tokens;
    let top = take_top(&mut tokens, 1, cx)?;
    let set_at =
        find_top(&tokens, 1, &["SET"]).ok_or_else(|| TsqlError::syntax("UPDATE needs SET"))?;
    let target = tokens[1..set_at].to_vec();
    let (target_name, _) =
        table_name_at(&target, 0).ok_or_else(|| TsqlError::syntax("UPDATE needs a table"))?;
    let output_at = find_top(&tokens, set_at, &["OUTPUT"]);
    let from_at = find_top(&tokens, set_at, &["FROM"]);
    let where_at = find_top(&tokens, set_at, &["WHERE"]);
    let assign_end = [output_at, from_at, where_at]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(tokens.len());

    // `UPDATE a SET … FROM t a JOIN b ON …`: the target is an alias.
    let mut table = target_name.clone();
    let mut alias: Option<String> = None;
    let mut from_items: Vec<Token> = Vec::new();
    let mut conditions: Vec<Vec<Token>> = Vec::new();
    if let Some(f) = from_at {
        let from_end = where_at.unwrap_or(tokens.len());
        let from = tokens[f + 1..from_end].to_vec();
        let first_end = find_top(
            &from,
            0,
            &["JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS"],
        )
        .unwrap_or(from.len());
        let first = split_commas(&from[..first_end]);
        let (first_table, after) =
            table_name_at(&first[0], 0).ok_or_else(|| TsqlError::syntax("FROM needs a table"))?;
        let first_alias = first[0]
            .get(after)
            .and_then(|t| {
                if t.is_word("AS") {
                    first[0].get(after + 1)
                } else {
                    Some(t)
                }
            })
            .and_then(|t| t.ident())
            .map(|s| s.to_lowercase());
        let target_is_first =
            first_alias.as_deref() == Some(target_name.as_str()) || first_table == target_name;
        let mut others: Vec<Vec<Token>> = Vec::new();
        if target_is_first {
            table = first_table;
            alias = first_alias.filter(|a| *a != table);
            others.extend(first[1..].iter().cloned());
        } else {
            others.extend(first.iter().cloned());
        }
        // JOINs: their tables to FROM, their ON to WHERE (inner joins only).
        let mut k = first_end;
        while k < from.len() {
            let mut s = k;
            if from[s].is_word("INNER") {
                s += 1;
            }
            if !from[s].is_word("JOIN") {
                return Err(TsqlError::unsupported("an outer join in UPDATE … FROM"));
            }
            let on =
                find_top(&from, s, &["ON"]).ok_or_else(|| TsqlError::syntax("JOIN needs ON"))?;
            let next = find_top(
                &from,
                on,
                &["JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS"],
            )
            .unwrap_or(from.len());
            others.push(from[s + 1..on].to_vec());
            conditions.push(rewrite_expr(from[on + 1..next].to_vec(), cx)?);
            k = next;
        }
        for (n, o) in others.into_iter().enumerate() {
            if n > 0 {
                from_items.push(Token::op(","));
            }
            from_items.extend(rewrite_expr(o, cx)?);
        }
    }
    let mut out = vec![Token::word("UPDATE"), Token::raw(ident_sql(&table))];
    if let Some(a) = &alias {
        out.push(Token::word("AS"));
        out.push(Token::raw(ident_sql(a)));
    }
    out.push(Token::word("SET"));
    for (n, part) in split_commas(&tokens[set_at + 1..assign_end])
        .into_iter()
        .enumerate()
    {
        let eq = part
            .iter()
            .position(|t| t.is_op("=") || t.is_op("+=") || t.is_op("-="))
            .ok_or_else(|| TsqlError::syntax("SET needs col = value"))?;
        if part
            .first()
            .is_some_and(|t| matches!(t.tok, Tok::UserVar(_)))
        {
            return Err(TsqlError::unsupported("assigning a variable in UPDATE"));
        }
        let name = part[..eq]
            .last()
            .and_then(|t| t.ident())
            .ok_or_else(|| TsqlError::syntax("a column name was expected"))?;
        if n > 0 {
            out.push(Token::op(","));
        }
        out.push(Token::raw(ident_sql(name)));
        out.push(Token::op("="));
        if part[eq].is_op("+=") || part[eq].is_op("-=") {
            out.push(Token::raw(ident_sql(name)));
            out.push(Token::op(if part[eq].is_op("+=") { "+" } else { "-" }));
            out.extend(wrap(rewrite_expr(part[eq + 1..].to_vec(), cx)?));
        } else {
            out.extend(rewrite_expr(part[eq + 1..].to_vec(), cx)?);
        }
    }
    if !from_items.is_empty() {
        out.push(Token::word("FROM"));
        out.extend(from_items);
    }
    let where_sql = match where_at {
        Some(w) => Some(render_text(&rewrite_expr(tokens[w + 1..].to_vec(), cx)?)),
        None => None,
    };
    if let Some(count) = top {
        conditions.push(vec![Token::raw(limited(
            &table,
            where_sql.clone(),
            count,
            cx,
        )?)]);
    } else if let Some(w) = where_sql {
        conditions.push(vec![Token::raw(w)]);
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
    let mut kind = Kind::Write;
    if let Some(o) = output_at {
        let end = [from_at, where_at]
            .into_iter()
            .flatten()
            .filter(|x| *x > o)
            .min()
            .unwrap_or(tokens.len());
        out.extend(output_clause(&tokens, o, end, false, cx)?);
        kind = Kind::Query;
    }
    Ok(Forward {
        statements: vec![render_text(&out)],
        kind,
        extra_column: None,
        aliases: Vec::new(),
        database: None,
    })
}

fn rewrite_delete(tokens: Vec<Token>, cx: &mut Cx) -> Result<Forward, TsqlError> {
    let mut tokens = tokens;
    let top = take_top(&mut tokens, 1, cx)?;
    let mut i = 1;
    if kw(&tokens, i, "FROM") {
        i += 1;
    }
    let (target, after) =
        table_name_at(&tokens, i).ok_or_else(|| TsqlError::syntax("DELETE needs a table"))?;
    let output_at = find_top(&tokens, after, &["OUTPUT"]);
    let from_at = find_top(&tokens, after, &["FROM"]);
    let where_at = find_top(&tokens, after, &["WHERE"]);
    let mut table = target.clone();
    let mut alias: Option<String> = None;
    let mut using_first: Vec<Vec<Token>> = Vec::new();
    let mut conditions: Vec<Vec<Token>> = Vec::new();
    let mut from: Vec<Token> = Vec::new();
    let mut first_end = 0usize;
    if let Some(f) = from_at {
        // DELETE p FROM t AS p JOIN u ON … → DELETE FROM t AS p USING u WHERE …
        from = tokens[f + 1..where_at.unwrap_or(tokens.len())].to_vec();
        first_end = find_top(&from, 0, &["JOIN", "INNER"]).unwrap_or(from.len());
        for part in split_commas(&from[..first_end]) {
            let Some((name, after)) = table_name_at(&part, 0) else {
                continue;
            };
            let part_alias = part
                .get(after)
                .and_then(|t| {
                    if t.is_word("AS") {
                        part.get(after + 1)
                    } else {
                        Some(t)
                    }
                })
                .and_then(|t| t.ident())
                .map(|s| s.to_lowercase());
            if name == target || part_alias.as_deref() == Some(target.as_str()) {
                table = name;
                alias = part_alias.filter(|a| *a != table);
            } else {
                using_first.push(part);
            }
        }
    }
    let mut out = vec![
        Token::word("DELETE"),
        Token::word("FROM"),
        Token::raw(ident_sql(&table)),
    ];
    if let Some(a) = &alias {
        out.push(Token::word("AS"));
        out.push(Token::raw(ident_sql(a)));
    }
    let target = table.clone();
    if from_at.is_some() {
        let mut using: Vec<Vec<Token>> = using_first;
        let mut k = first_end;
        while k < from.len() {
            let mut s = k;
            if from[s].is_word("INNER") {
                s += 1;
            }
            let on =
                find_top(&from, s, &["ON"]).ok_or_else(|| TsqlError::syntax("JOIN needs ON"))?;
            let next = find_top(&from, on, &["JOIN", "INNER"]).unwrap_or(from.len());
            using.push(from[s + 1..on].to_vec());
            conditions.push(rewrite_expr(from[on + 1..next].to_vec(), cx)?);
            k = next;
        }
        if !using.is_empty() {
            out.push(Token::word("USING"));
            for (n, u) in using.into_iter().enumerate() {
                if n > 0 {
                    out.push(Token::op(","));
                }
                out.extend(rewrite_expr(u, cx)?);
            }
        }
    }
    let where_sql = match where_at {
        Some(w) => Some(render_text(&rewrite_expr(tokens[w + 1..].to_vec(), cx)?)),
        None => None,
    };
    if let Some(count) = top {
        conditions.push(vec![Token::raw(limited(
            &target,
            where_sql.clone(),
            count,
            cx,
        )?)]);
    } else if let Some(w) = where_sql {
        conditions.push(vec![Token::raw(w)]);
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
    let mut kind = Kind::Write;
    if let Some(o) = output_at {
        let end = [from_at, where_at]
            .into_iter()
            .flatten()
            .filter(|x| *x > o)
            .min()
            .unwrap_or(tokens.len());
        out.extend(output_clause(&tokens, o, end, true, cx)?);
        kind = Kind::Query;
    }
    Ok(Forward {
        statements: vec![render_text(&out)],
        kind,
        extra_column: None,
        aliases: Vec::new(),
        database: None,
    })
}

/// A default's expression, its redundant parentheses (`((0))`) removed.
fn default_expr(tokens: &[Token], cx: &mut Cx) -> Result<String, TsqlError> {
    let mut t = tokens;
    while t.len() >= 2 && t[0].is_op("(") && matching_paren(t, 0) == Some(t.len() - 1) {
        t = &t[1..t.len() - 1];
    }
    match t {
        [w, open, close]
            if open.is_op("(")
                && close.is_op(")")
                && w.upper().is_some_and(|u| {
                    matches!(
                        u.as_str(),
                        "GETDATE"
                            | "SYSDATETIME"
                            | "GETUTCDATE"
                            | "SYSUTCDATETIME"
                            | "SYSDATETIMEOFFSET"
                    )
                }) =>
        {
            Ok("CURRENT_TIMESTAMP".into())
        }
        [w] if w.is_word("CURRENT_TIMESTAMP") => Ok("CURRENT_TIMESTAMP".into()),
        _ => Ok(render_text(&rewrite_expr(t.to_vec(), cx)?)),
    }
}

/// A column definition; returns it and any index made with it.
fn column_definition(def: &[Token], cx: &mut Cx) -> Result<String, TsqlError> {
    let name = def
        .first()
        .and_then(|t| t.ident())
        .ok_or_else(|| TsqlError::syntax("a column name was expected"))?;
    const ATTRS: &[&str] = &[
        "NOT",
        "NULL",
        "DEFAULT",
        "IDENTITY",
        "PRIMARY",
        "UNIQUE",
        "CONSTRAINT",
        "COLLATE",
        "REFERENCES",
        "CHECK",
        "ROWGUIDCOL",
        "SPARSE",
        "FOREIGN",
        "INDEX",
        "AS",
        "FILESTREAM",
        "MASKED",
        "ENCRYPTED",
        "GENERATED",
    ];
    let mut k = 1;
    let mut type_end = def.len();
    while k < def.len() {
        if def[k].is_op("(") {
            k = matching_paren(def, k).ok_or_else(|| TsqlError::syntax("unbalanced type"))? + 1;
            continue;
        }
        if ATTRS.iter().any(|a| def[k].is_word(a)) {
            type_end = k;
            break;
        }
        k += 1;
    }
    if type_end == 1 && kw(def, 1, "AS") {
        return Err(TsqlError::unsupported("computed columns"));
    }
    let mut ty = map_type(&def[1..type_end])?;
    let mut attrs: Vec<String> = Vec::new();
    let mut identity = false;
    let mut explicit_collation = false;
    let mut k = type_end;
    while k < def.len() {
        let w = def[k].upper().unwrap_or_default();
        match w.as_str() {
            "NOT" if kw(def, k + 1, "NULL") => {
                attrs.push("NOT NULL".into());
                k += 2;
            }
            "NULL" => {
                attrs.push("NULL".into());
                k += 1;
            }
            "CONSTRAINT" => k += 2,
            "DEFAULT" => {
                let mut e = k + 1;
                if def.get(e).is_some_and(|t| t.is_op("(")) {
                    e = matching_paren(def, e)
                        .ok_or_else(|| TsqlError::syntax("unbalanced DEFAULT"))?
                        + 1;
                } else {
                    e += 1;
                    if def.get(e).is_some_and(|t| t.is_op("(")) {
                        e = matching_paren(def, e)
                            .ok_or_else(|| TsqlError::syntax("unbalanced DEFAULT"))?
                            + 1;
                    }
                }
                attrs.push(format!("DEFAULT {}", default_expr(&def[k + 1..e], cx)?));
                k = e;
            }
            "IDENTITY" => {
                identity = true;
                k += 1;
                if def.get(k).is_some_and(|t| t.is_op("(")) {
                    k = matching_paren(def, k)
                        .ok_or_else(|| TsqlError::syntax("unbalanced IDENTITY"))?
                        + 1;
                }
            }
            "PRIMARY" => {
                attrs.push("PRIMARY KEY".into());
                k += 2;
                while kw(def, k, "CLUSTERED") || kw(def, k, "NONCLUSTERED") {
                    k += 1;
                }
            }
            "UNIQUE" => {
                attrs.push("UNIQUE".into());
                k += 1;
                while kw(def, k, "CLUSTERED") || kw(def, k, "NONCLUSTERED") {
                    k += 1;
                }
            }
            "COLLATE" => {
                explicit_collation = true;
                // A case-insensitive collation is the engine's own (the
                // column gets its key companion); a case-sensitive or binary
                // one is the bytes, which is what a column compares by.
                if cx.collations
                    && let Some(engine) = def.get(k + 1).and_then(|t| t.ident()).and_then(engine_collation)
                {
                    // Right after the type: after a DEFAULT it would read
                    // as part of the default's expression.
                    attrs.insert(0, format!("COLLATE {engine}"));
                }
                k += 2;
            }
            "ROWGUIDCOL" | "SPARSE" => k += 1,
            "REFERENCES" | "CHECK" | "FOREIGN" => {
                attrs.push(render_text(&rewrite_expr(def[k..].to_vec(), cx)?));
                k = def.len();
            }
            _ => {
                return Err(TsqlError::syntax(format!(
                    "Incorrect syntax near '{}'.",
                    render_text(std::slice::from_ref(&def[k]))
                )));
            }
        }
    }
    // SQL Server's default collation compares text without case: a text
    // column that names no collation is given the engine's.
    if cx.collations
        && !explicit_collation
        && cx.env.case_insensitive()
        && is_string_type(&def[1..type_end])
        && !kw(def, 1, "XML")
    {
        attrs.insert(0, format!("COLLATE {}", crate::core::rql::collation::CI_NAME));
    }
    if identity {
        ty = match ty.as_str() {
            "smallint" => "smallserial".into(),
            "integer" => "serial".into(),
            "bigint" | "numeric(18,0)" | "numeric(19,0)" | "numeric(38,0)" => "bigserial".into(),
            other => {
                return Err(TsqlError::unsupported(format!(
                    "IDENTITY on a {other} column"
                )));
            }
        };
        attrs.retain(|a| !a.starts_with("DEFAULT"));
    }
    let mut out = format!("{} {ty}", ident_sql(name));
    for a in attrs {
        out.push(' ');
        out.push_str(&a);
    }
    Ok(out)
}

fn index_columns(tokens: &[Token]) -> Result<Vec<String>, TsqlError> {
    let mut out = Vec::new();
    for part in split_commas(tokens) {
        let name = part
            .first()
            .and_then(|t| t.ident())
            .ok_or_else(|| TsqlError::unsupported("an index on an expression"))?;
        let mut c = ident_sql(name);
        if part.last().is_some_and(|t| t.is_word("DESC")) {
            c.push_str(" DESC");
        }
        out.push(c);
    }
    Ok(out)
}

/// A table constraint (PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK) or inline
/// INDEX. Returns the definition, or the statement an index becomes.
fn table_constraint(
    part: &[Token],
    table: &str,
    cx: &mut Cx,
) -> Result<(Option<String>, Option<String>), TsqlError> {
    let mut p = 0;
    let mut name = None;
    if part[0].is_word("CONSTRAINT") {
        name = part.get(1).and_then(|t| t.ident()).map(str::to_string);
        p = 2;
    }
    let head = part.get(p).and_then(|t| t.upper()).unwrap_or_default();
    let named = name
        .as_ref()
        .map(|n| format!("CONSTRAINT {} ", ident_sql(n)))
        .unwrap_or_default();
    let paren = part.iter().position(|t| t.is_op("("));
    match head.as_str() {
        "PRIMARY" | "UNIQUE" => {
            let open = paren.ok_or_else(|| TsqlError::syntax("a key needs columns"))?;
            let close =
                matching_paren(part, open).ok_or_else(|| TsqlError::syntax("unbalanced key"))?;
            let cols = index_columns(&part[open + 1..close])?;
            let kind = if head == "PRIMARY" {
                "PRIMARY KEY"
            } else {
                "UNIQUE"
            };
            Ok((Some(format!("{named}{kind} ({})", cols.join(", "))), None))
        }
        "FOREIGN" => {
            let open = paren.ok_or_else(|| TsqlError::syntax("a key needs columns"))?;
            let close =
                matching_paren(part, open).ok_or_else(|| TsqlError::syntax("unbalanced key"))?;
            let cols = index_columns(&part[open + 1..close])?;
            let mut rest = part[close + 1..].to_vec();
            // NOT FOR REPLICATION
            if let Some(at) = rest.iter().position(|t| t.is_word("NOT")) {
                if kw(&rest, at + 1, "FOR") {
                    rest.truncate(at);
                }
            }
            let rest = render_text(&rewrite_expr(rest, cx)?);
            Ok((
                Some(format!("{named}FOREIGN KEY ({}) {rest}", cols.join(", "))),
                None,
            ))
        }
        "CHECK" => Ok((
            Some(format!(
                "{named}{}",
                render_text(&rewrite_expr(part[p..].to_vec(), cx)?)
            )),
            None,
        )),
        "INDEX" => {
            let index = part
                .get(p + 1)
                .and_then(|t| t.ident())
                .ok_or_else(|| TsqlError::syntax("INDEX needs a name"))?;
            let open = paren.ok_or_else(|| TsqlError::syntax("an index needs columns"))?;
            let close =
                matching_paren(part, open).ok_or_else(|| TsqlError::syntax("unbalanced index"))?;
            let unique = part[p..open].iter().any(|t| t.is_word("UNIQUE"));
            let cols = index_columns(&part[open + 1..close])?;
            Ok((
                None,
                Some(format!(
                    "CREATE {}INDEX {} ON {} ({})",
                    if unique { "UNIQUE " } else { "" },
                    ident_sql(&crate::sqlcompat::index_name(table, index)),
                    ident_sql(table),
                    cols.join(", ")
                )),
            ))
        }
        _ => Err(TsqlError::syntax(format!(
            "Incorrect syntax near '{head}'."
        ))),
    }
}

fn is_constraint_start(part: &[Token]) -> bool {
    part.first().is_some_and(|t| {
        matches!(t.tok, Tok::Word(_))
            && t.upper().is_some_and(|w| {
                matches!(
                    w.as_str(),
                    "CONSTRAINT" | "PRIMARY" | "UNIQUE" | "FOREIGN" | "CHECK" | "INDEX"
                )
            })
    })
}

/// Storage clauses after a table or index: `ON [PRIMARY]`, `WITH (…)`,
/// `TEXTIMAGE_ON …`, dropped.
fn strip_storage(tokens: &[Token]) -> Result<(), TsqlError> {
    let mut k = 0;
    while k < tokens.len() {
        let w = tokens[k].upper().unwrap_or_default();
        match w.as_str() {
            "ON" | "TEXTIMAGE_ON" | "FILESTREAM_ON" => k += 2,
            "WITH" => {
                if tokens.get(k + 1).is_some_and(|t| t.is_op("(")) {
                    k = matching_paren(tokens, k + 1)
                        .ok_or_else(|| TsqlError::syntax("unbalanced WITH"))?
                        + 1;
                } else {
                    k += 1;
                }
            }
            "INCLUDE" => {
                k = matching_paren(tokens, k + 1)
                    .ok_or_else(|| TsqlError::syntax("unbalanced INCLUDE"))?
                    + 1;
            }
            _ if tokens[k].is_op("(") || tokens[k].is_op(")") || tokens[k].is_op(",") => k += 1,
            _ => k += 1,
        }
    }
    Ok(())
}

/// The engine collation a SQL Server collation means, `None` for the bytes:
/// `…_CI_AI` ignores case and accents, any other `…_CI…` case only;
/// `…_CS…` and `…_BIN`/`…_BIN2` compare by the bytes.
pub(crate) fn engine_collation(name: &str) -> Option<&'static str> {
    let n = name.to_ascii_uppercase();
    if !n.contains("_CI") {
        None
    } else if n.contains("_CI_AI") {
        Some(crate::core::rql::collation::CIA_NAME)
    } else {
        Some(crate::core::rql::collation::CI_NAME)
    }
}

/// Where the SQL Server front end keeps the spellings tables and columns
/// were declared with: names are folded to lower case for the engine (SQL
/// Server compares them without case), and shown as declared, which
/// SqlBulkCopy's column mappings match exactly.
pub(crate) const NAMES_TABLE: &str = "__rdb_tds_names";

/// The engine's key for a declared name: folded and fitted as `ident_sql`
/// stores it.
fn name_key(name: &str) -> String {
    crate::sqlcompat::fit_name(name.to_lowercase())
}

/// The statements recording `relation`'s declared spellings: its own
/// (`shown`, when given) and its columns'. With `replace`, what was
/// recorded for the relation before goes first (a CREATE TABLE).
pub(crate) fn record_names(
    relation: &str,
    shown: Option<&str>,
    columns: &[String],
    objects: &[(String, String)],
    replace: bool,
) -> Vec<String> {
    let lit = |s: &str| sql_str(&super::lex::decode_dots(s));
    let key = |s: &str| sql_str(s);
    let relation = &name_key(relation);
    let mut rows: Vec<String> = Vec::new();
    if let Some(shown) = shown {
        rows.push(format!("({}, '', {})", key(relation), lit(shown)));
    }
    for c in columns {
        rows.push(format!("({}, {}, {})", key(relation), key(&name_key(c)), lit(c)));
    }
    // Constraints and indexes, by the name the engine holds them under
    // (`OBJECT_MARK` + it): the names its errors quote.
    for (engine, shown) in objects {
        rows.push(format!("({}, {}, {})", key(relation), key(&format!("{OBJECT_MARK}{}", name_key(engine))), lit(shown)));
    }
    let mut out = vec![format!(
        "CREATE TABLE IF NOT EXISTS {NAMES_TABLE} (rel text NOT NULL, col text NOT NULL, shown text NOT NULL, PRIMARY KEY (rel, col))"
    )];
    if replace {
        out.push(format!("DELETE FROM {NAMES_TABLE} WHERE rel = {}", key(relation)));
    }
    if !rows.is_empty() {
        out.push(format!(
            "INSERT INTO {NAMES_TABLE} (rel, col, shown) VALUES {} ON CONFLICT (rel, col) DO UPDATE SET shown = excluded.shown",
            rows.join(", ")
        ));
    }
    out
}

/// The statements keeping `NAMES_TABLE` in step with an sp_rename of
/// `parts` (the old name's parts, folded, `dbo` left out) to `new` (dots
/// encoded): a column's, an index's or a table's (`kind` empty or OBJECT).
pub(crate) fn rename_records(parts: &[String], new: &str, kind: &str) -> Vec<String> {
    let names = NAMES_TABLE;
    if kind == "COLUMN" && parts.len() >= 2 {
        let table = &parts[parts.len() - 2];
        // The old spelling goes first: a rename in case only
        // keeps its key.
        let mut r = record_names(table, None, &[], &[], false);
        r.push(format!(
            "DELETE FROM {names} WHERE rel = {} AND col = {}",
            sql_str(table),
            sql_str(&parts[parts.len() - 1])
        ));
        r.extend(record_names(table, None, &[new.to_string()], &[], false));
        r
    } else if kind == "INDEX" && parts.len() >= 2 {
        let table = &parts[parts.len() - 2];
        let object = (crate::sqlcompat::index_name(table, new), new.to_string());
        record_names(table, None, &[], &[object], false)
    } else if kind.is_empty() || kind == "OBJECT" {
        let old = parts.last().cloned().unwrap_or_default();
        let mut r = record_names(new, None, &[], &[], false);
        r.push(format!(
            "UPDATE {names} SET rel = {} WHERE rel = {}",
            sql_str(&name_key(new)),
            sql_str(&old)
        ));
        r.extend(record_names(new, Some(new), &[], &[], false));
        r
    } else {
        Vec::new()
    }
}

/// The statements recording the declared names a T-SQL script would make
/// (`EXEC sp_rdb_record_names @script = N'…'`), none of its statements
/// run: for databases made before names were recorded, from the script
/// that made them (`dotnet ef migrations script --idempotent`). Its CREATE
/// TABLE, ALTER TABLE … ADD, CREATE INDEX and sp_rename, in order, also
/// inside IF / BEGIN … END and `EXEC(N'…')`.
pub(crate) fn names_from_script(script: &str, env: &dyn Env) -> Result<Vec<String>, TsqlError> {
    // `GO` separates a script's batches for its client.
    let text: String = script
        .lines()
        .filter(|l| !l.trim().eq_ignore_ascii_case("GO"))
        .collect::<Vec<_>>()
        .join("\n");
    let tokens = super::lex::lex(&text).map_err(|e| TsqlError::syntax(e.0))?;
    let mut out = Vec::new();
    fn walk(stmts: &[Stmt], env: &dyn Env, out: &mut Vec<String>) -> Result<(), TsqlError> {
        for stmt in stmts {
            match stmt {
                Stmt::Simple(tokens) => simple(tokens, env, out)?,
                Stmt::Block(b) => walk(b, env, out)?,
                Stmt::If { then, otherwise, .. } => {
                    walk(std::slice::from_ref(then.as_ref()), env, out)?;
                    if let Some(o) = otherwise {
                        walk(std::slice::from_ref(o.as_ref()), env, out)?;
                    }
                }
                Stmt::While { body, .. } => walk(std::slice::from_ref(body.as_ref()), env, out)?,
                Stmt::Try { body, catch } => {
                    walk(body, env, out)?;
                    walk(catch, env, out)?;
                }
            }
        }
        Ok(())
    }
    fn simple(tokens: &[Token], env: &dyn Env, out: &mut Vec<String>) -> Result<(), TsqlError> {
        let head = tokens.first().and_then(|t| t.upper()).unwrap_or_default();
        let second = tokens.get(1).and_then(|t| t.upper()).unwrap_or_default();
        let strings = |from: usize| -> Vec<String> {
            tokens[from..]
                .iter()
                .filter_map(|t| match &t.tok {
                    Tok::Str(b) => Some(String::from_utf8_lossy(b).into_owned()),
                    _ => None,
                })
                .collect()
        };
        match (head.as_str(), second.as_str()) {
            ("CREATE", "TABLE") | ("CREATE", "INDEX" | "UNIQUE" | "CLUSTERED" | "NONCLUSTERED") | ("ALTER", "TABLE") => {
                // What the statement would record, not the statement.
                if let Ok(f) = translate(&render_text(tokens), tokens.to_vec(), env) {
                    out.extend(f.statements.into_iter().filter(|s| {
                        s.contains(NAMES_TABLE)
                            && (s.starts_with("CREATE TABLE IF NOT EXISTS")
                                || s.starts_with("INSERT INTO")
                                || s.starts_with("DELETE FROM"))
                    }));
                }
            }
            ("EXEC" | "EXECUTE", _) => {
                let name = tokens.get(1).and_then(|t| t.ident()).unwrap_or("").to_ascii_lowercase();
                if name == "sp_rename" {
                    let args = strings(2);
                    if args.len() >= 2 {
                        let parts: Vec<String> = super::lex::name_parts(&args[0])
                            .into_iter()
                            .map(|p| p.to_lowercase())
                            .filter(|p| p != "dbo")
                            .collect();
                        let new = super::lex::encode_dots(args[1].trim_matches(['[', ']']).to_string());
                        let kind = args.get(2).map(|k| k.to_ascii_uppercase()).unwrap_or_default();
                        out.extend(rename_records(&parts, &new, &kind));
                    }
                } else if name.is_empty() || name == "sp_executesql" {
                    // `EXEC(N'…')`, `EXEC sp_executesql N'…'`: the text run.
                    if let Some(inner) = strings(1).into_iter().next() {
                        let inner_tokens = super::lex::lex(&inner).map_err(|e| TsqlError::syntax(e.0))?;
                        walk(&parse_batch(&inner_tokens)?, env, out)?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
    walk(&parse_batch(&tokens)?, env, &mut out)?;
    Ok(out)
}

/// Marks a recorded constraint's or index's name in `NAMES_TABLE`'s `col`.
pub(crate) const OBJECT_MARK: char = '#';

/// A table constraint's or inline index's name: the engine's and as
/// written.
fn constraint_object(part: &[Token], table: &str) -> Option<(String, String)> {
    if part.first()?.is_word("CONSTRAINT") {
        let name = part.get(1)?.ident()?;
        return Some((name.to_string(), name.to_string()));
    }
    if part.first()?.is_word("INDEX") {
        let name = part.get(1)?.ident()?;
        return Some((crate::sqlcompat::index_name(table, name), name.to_string()));
    }
    None
}

/// The last part of the name at `i`, as written.
fn shown_name_at(tokens: &[Token], i: usize) -> Option<String> {
    let mut j = i;
    let mut name = tokens.get(j)?.ident()?.to_string();
    while tokens.get(j + 1).is_some_and(|t| t.is_op(".")) {
        match tokens.get(j + 2).and_then(|t| t.ident()) {
            Some(next) => {
                name = next.to_string();
                j += 2;
            }
            None => j += 1,
        }
    }
    Some(name)
}

fn rewrite_create_table(tokens: Vec<Token>, cx: &mut Cx) -> Result<Forward, TsqlError> {
    cx.collations = true;
    let (table, after) =
        table_name_at(&tokens, 2).ok_or_else(|| TsqlError::syntax("CREATE TABLE needs a name"))?;
    let temporary = table.starts_with('#');
    if !tokens.get(after).is_some_and(|t| t.is_op("(")) {
        return Err(TsqlError::syntax("CREATE TABLE needs a column list"));
    }
    let close = matching_paren(&tokens, after)
        .ok_or_else(|| TsqlError::syntax("unbalanced column list"))?;
    strip_storage(&tokens[close + 1..])?;
    let mut defs = Vec::new();
    let mut later = Vec::new();
    let mut declared: Vec<String> = Vec::new();
    let mut objects: Vec<(String, String)> = Vec::new();
    for part in split_commas(&tokens[after + 1..close]) {
        if part.is_empty() {
            continue;
        }
        if is_constraint_start(&part) {
            objects.extend(constraint_object(&part, &table));
            let (def, statement) = table_constraint(&part, &table, cx)?;
            defs.extend(def);
            later.extend(statement);
        } else {
            if let Some(name) = part.first().and_then(|t| t.ident()) {
                declared.push(name.to_string());
            }
            defs.push(column_definition(&part, cx)?);
        }
    }
    let mut statements = vec![format!(
        "CREATE {}TABLE {} ({})",
        if temporary { "TEMPORARY " } else { "" },
        ident_sql(&table),
        defs.join(", ")
    )];
    statements.extend(later);
    if !temporary {
        let shown = shown_name_at(&tokens, 2);
        statements.extend(record_names(&table, shown.as_deref(), &declared, &objects, true));
    }
    Ok(Forward {
        statements,
        kind: Kind::Ddl,
        extra_column: None,
        aliases: Vec::new(),
        database: None,
    })
}

fn rewrite_alter_table(tokens: Vec<Token>, cx: &mut Cx) -> Result<Forward, TsqlError> {
    let (table, after) =
        table_name_at(&tokens, 2).ok_or_else(|| TsqlError::syntax("ALTER TABLE needs a name"))?;
    let t = ident_sql(&table);
    let mut statements = Vec::new();
    let mut k = after;
    // WITH CHECK / WITH NOCHECK before ADD CONSTRAINT.
    if kw(&tokens, k, "WITH") && (kw(&tokens, k + 1, "CHECK") || kw(&tokens, k + 1, "NOCHECK")) {
        k += 2;
    }
    let verb = tokens.get(k).and_then(|x| x.upper()).unwrap_or_default();
    match verb.as_str() {
        "ADD" => {
            // A column added takes collations as a created one does.
            cx.collations = true;
            for part in split_commas(&tokens[k + 1..]) {
                if part.is_empty() {
                    continue;
                }
                if is_constraint_start(&part) {
                    let (def, statement) = table_constraint(&part, &table, cx)?;
                    if let Some(d) = def {
                        statements.push(format!("ALTER TABLE {t} ADD {d}"));
                    }
                    statements.extend(statement);
                    if !table.starts_with('#') {
                        if let Some(object) = constraint_object(&part, &table) {
                            statements.extend(record_names(&table, None, &[], &[object], false));
                        }
                    }
                } else {
                    let mut p = part;
                    if p.first().is_some_and(|x| x.is_word("COLUMN")) {
                        p.remove(0);
                    }
                    statements.push(format!(
                        "ALTER TABLE {t} ADD COLUMN {}",
                        column_definition(&p, cx)?
                    ));
                    if !table.starts_with('#') {
                        if let Some(name) = p.first().and_then(|x| x.ident()) {
                            statements.extend(record_names(&table, None, &[name.to_string()], &[], false));
                        }
                    }
                }
            }
        }
        "DROP" => {
            let mut rest = tokens[k + 1..].to_vec();
            let what = rest.first().and_then(|x| x.upper()).unwrap_or_default();
            let constraint = what == "CONSTRAINT";
            if what == "COLUMN" || constraint {
                rest.remove(0);
            }
            let mut if_exists = false;
            if kw(&rest, 0, "IF") && kw(&rest, 1, "EXISTS") {
                if_exists = true;
                rest.drain(..2);
            }
            for part in split_commas(&rest) {
                let name = part
                    .first()
                    .and_then(|x| x.ident())
                    .ok_or_else(|| TsqlError::syntax("a name was expected"))?;
                statements.push(format!(
                    "ALTER TABLE {t} DROP {} {}{}",
                    if constraint { "CONSTRAINT" } else { "COLUMN" },
                    if if_exists { "IF EXISTS " } else { "" },
                    ident_sql(name)
                ));
            }
        }
        "ALTER" => {
            // ALTER COLUMN c type [NULL | NOT NULL]
            let def = &tokens[k + 2..];
            let name = def
                .first()
                .and_then(|x| x.ident())
                .ok_or_else(|| TsqlError::syntax("a column name was expected"))?;
            let null_at = def
                .iter()
                .position(|x| x.is_word("NULL") || x.is_word("NOT") || x.is_word("COLLATE"))
                .unwrap_or(def.len());
            let ty = map_type(&def[1..null_at])?;
            statements.push(format!(
                "ALTER TABLE {t} ALTER COLUMN {} TYPE {ty}",
                ident_sql(name)
            ));
            if def[null_at..].iter().any(|x| x.is_word("NOT")) {
                statements.push(format!(
                    "ALTER TABLE {t} ALTER COLUMN {} SET NOT NULL",
                    ident_sql(name)
                ));
            } else if def[null_at..].iter().any(|x| x.is_word("NULL")) {
                statements.push(format!(
                    "ALTER TABLE {t} ALTER COLUMN {} DROP NOT NULL",
                    ident_sql(name)
                ));
            }
        }
        "CHECK" | "NOCHECK" | "ENABLE" | "DISABLE" | "SET" | "REBUILD" | "SWITCH" => {}
        other => return Err(TsqlError::unsupported(format!("ALTER TABLE … {other}"))),
    }
    if statements.is_empty() {
        statements.push(String::new());
    }
    Ok(Forward {
        statements,
        kind: Kind::Ddl,
        extra_column: None,
        aliases: Vec::new(),
        database: None,
    })
}

/// The columns of a filter made only of `col IS NOT NULL` joined by `AND`
/// (parentheses allowed), lower-cased; `None` for any other filter.
fn not_null_filter(tokens: &[Token]) -> Option<Vec<String>> {
    let body: Vec<&Token> = tokens
        .iter()
        .filter(|t| !t.is_op("(") && !t.is_op(")"))
        .collect();
    let mut out = Vec::new();
    let mut i = 0;
    loop {
        let col = body.get(i)?.ident()?.to_lowercase();
        if !(body.get(i + 1)?.is_word("IS")
            && body.get(i + 2)?.is_word("NOT")
            && body.get(i + 3)?.is_word("NULL"))
        {
            return None;
        }
        out.push(col);
        i += 4;
        match body.get(i) {
            None => return Some(out),
            Some(t) if t.is_word("AND") => i += 1,
            Some(_) => return None,
        }
    }
}

fn rewrite_create_index(tokens: Vec<Token>, cx: &mut Cx) -> Result<Forward, TsqlError> {
    let _ = cx;
    let unique = tokens.iter().take(4).any(|t| t.is_word("UNIQUE"));
    let at = tokens
        .iter()
        .position(|t| t.is_word("INDEX"))
        .ok_or_else(|| TsqlError::syntax("CREATE INDEX was expected"))?;
    let name = tokens
        .get(at + 1)
        .and_then(|t| t.ident())
        .ok_or_else(|| TsqlError::syntax("CREATE INDEX needs a name"))?;
    let on =
        find_top(&tokens, at, &["ON"]).ok_or_else(|| TsqlError::syntax("CREATE INDEX needs ON"))?;
    let (table, after) = table_name_at(&tokens, on + 1)
        .ok_or_else(|| TsqlError::syntax("CREATE INDEX needs a table"))?;
    let open = (after..tokens.len())
        .find(|&k| tokens[k].is_op("("))
        .ok_or_else(|| TsqlError::syntax("CREATE INDEX needs columns"))?;
    let close =
        matching_paren(&tokens, open).ok_or_else(|| TsqlError::syntax("unbalanced column list"))?;
    let cols = index_columns(&tokens[open + 1..close])?;
    // From past the column list: started on its `)`, the search ran at
    // depth -1 and never found the WHERE, so every filtered index was built
    // whole, a unique one refusing rows its filter left out.
    if let Some(w) = find_top(&tokens, close + 1, &["WHERE"]) {
        // A filter only narrows which rows the index holds. Not unique, the
        // index over every row answers the same queries. Unique, a filter of
        // `k IS NOT NULL [AND …]` over key columns leaves out only rows whose
        // key holds a NULL, which a unique key here never compares equal
        // anyway (EF Core puts one on every unique index over a nullable
        // column). Any other filter would constrain different rows.
        if unique {
            let keys: Vec<String> = split_commas(&tokens[open + 1..close])
                .iter()
                .filter_map(|p| p.first().and_then(|t| t.ident()).map(str::to_lowercase))
                .collect();
            let end = find_top(&tokens, w + 1, &["WITH", "ON"]).unwrap_or(tokens.len());
            match not_null_filter(&tokens[w + 1..end]) {
                Some(filtered) if filtered.iter().all(|c| keys.contains(c)) => {}
                _ => {
                    return Err(TsqlError::unsupported(
                        "a filtered unique index other than `key IS NOT NULL [AND …]`",
                    ))
                }
            }
        }
    }
    let mut statements = vec![format!(
        "CREATE {}INDEX {} ON {} ({})",
        if unique { "UNIQUE " } else { "" },
        ident_sql(&crate::sqlcompat::index_name(&table, name)),
        ident_sql(&table),
        cols.join(", ")
    )];
    if !table.starts_with('#') {
        let object = (crate::sqlcompat::index_name(&table, name), name.to_string());
        statements.extend(record_names(&table, None, &[], &[object], false));
    }
    Ok(Forward {
        statements,
        kind: Kind::Ddl,
        extra_column: None,
        aliases: Vec::new(),
        database: None,
    })
}

fn rewrite_drop(tokens: Vec<Token>, cx: &mut Cx) -> Result<Forward, TsqlError> {
    let mut dropped_database = None;
    let what = tokens.get(1).and_then(|t| t.upper()).unwrap_or_default();
    let mut i = 2;
    let mut if_exists = false;
    if kw(&tokens, i, "IF") && kw(&tokens, i + 1, "EXISTS") {
        if_exists = true;
        i += 2;
    }
    let ie = if if_exists { "IF EXISTS " } else { "" };
    let statements = match what.as_str() {
        "TABLE" => split_commas(&tokens[i..])
            .iter()
            .map(|p| {
                table_name_at(p, 0)
                    .map(|(n, _)| format!("DROP TABLE {ie}{}", ident_sql(&n)))
                    .ok_or_else(|| TsqlError::syntax("a table name was expected"))
            })
            .collect::<Result<Vec<_>, _>>()?,
        "INDEX" => {
            // DROP INDEX ix ON t, or DROP INDEX t.ix
            let mut out = Vec::new();
            for part in split_commas(&tokens[i..]) {
                let (table, name) = if let Some(on) = part.iter().position(|t| t.is_word("ON")) {
                    let n = part
                        .first()
                        .and_then(|t| t.ident())
                        .unwrap_or("")
                        .to_string();
                    let (t, _) = table_name_at(&part, on + 1)
                        .ok_or_else(|| TsqlError::syntax("a table name was expected"))?;
                    (t, n)
                } else {
                    let idents: Vec<&str> = part.iter().filter_map(|t| t.ident()).collect();
                    if idents.len() < 2 {
                        return Err(TsqlError::syntax("DROP INDEX needs ON table"));
                    }
                    (
                        idents[idents.len() - 2].to_lowercase(),
                        idents[idents.len() - 1].to_string(),
                    )
                };
                out.push(format!(
                    "DROP INDEX {ie}{}",
                    ident_sql(&crate::sqlcompat::index_name(&table, &name))
                ));
            }
            out
        }
        "DATABASE" => {
            let name = tokens
                .get(i)
                .and_then(|t| t.ident())
                .ok_or_else(|| TsqlError::syntax("a database name was expected"))?;
            dropped_database = Some((name.to_string(), false));
            vec![format!("DROP DATABASE {ie}{}", ident_sql(name))]
        }
        "VIEW" => vec![render_text(&rewrite_expr(tokens, cx)?)],
        other => return Err(TsqlError::unsupported(format!("DROP {other}"))),
    };
    Ok(Forward {
        statements,
        kind: Kind::Ddl,
        extra_column: None,
        aliases: Vec::new(),
        database: dropped_database,
    })
}

/// The one MERGE drivers write to insert several rows and learn their keys
/// (EF Core's batched insert):
///
/// ```sql
/// MERGE t USING (VALUES (…, 0), (…, 1)) AS i (a, b, _Position) ON 1=0
/// WHEN NOT MATCHED THEN INSERT (a, b) VALUES (i.a, i.b)
/// OUTPUT INSERTED.Id, i._Position;
/// ```
///
/// It is an INSERT of those rows. The source columns OUTPUT names cannot
/// be RETURNED, so each row's are put back by position: the engine returns
/// an INSERT's rows in the order of its VALUES.
/// `CREATE | DROP | ALTER FULLTEXT CATALOG | INDEX …` ([`super::fulltext`]).
fn fulltext_ddl(tokens: Vec<Token>, _cx: &mut Cx) -> Result<Forward, TsqlError> {
    let verb = tokens[0].upper().unwrap_or_default();
    let nothing = || Forward {
        statements: vec![String::new()],
        kind: Kind::Ddl,
        extra_column: None,
        aliases: Vec::new(),
        database: None,
    };
    if kw(&tokens, 2, "CATALOG") || kw(&tokens, 2, "STOPLIST") {
        // A catalog groups indexes for SQL Server's own administration; there
        // is nothing to keep for one here.
        return Ok(nothing());
    }
    if !kw(&tokens, 2, "INDEX") || !kw(&tokens, 3, "ON") {
        return Err(TsqlError::syntax(format!("{verb} FULLTEXT INDEX ON table …")));
    }
    let (table, after) =
        table_name_at(&tokens, 4).ok_or_else(|| TsqlError::syntax("a full-text index needs a table"))?;
    let table = table.to_lowercase();
    let statements = match verb.as_str() {
        "CREATE" => {
            if !tokens.get(after).is_some_and(|t| t.is_op("(")) {
                return Err(TsqlError::syntax("CREATE FULLTEXT INDEX ON table (columns) KEY INDEX name"));
            }
            let close = matching_paren(&tokens, after).ok_or_else(|| TsqlError::syntax("unbalanced column list"))?;
            if !kw(&tokens, close + 1, "KEY") {
                return Err(TsqlError::syntax("CREATE FULLTEXT INDEX needs KEY INDEX"));
            }
            let columns = super::fulltext::index_columns(&tokens[after + 1..close])?;
            super::fulltext::index_statements(&table, &columns)
        }
        "DROP" => super::fulltext::drop_statements(&table),
        _ => {
            // ENABLE, DISABLE, SET CHANGE_TRACKING, START/STOP … POPULATION:
            // the index is kept current with every write.
            if tokens[after..].iter().any(|t| t.is_word("ADD") || t.is_word("DROP")) {
                return Err(TsqlError::unsupported(
                    "ALTER FULLTEXT INDEX … ADD / DROP (drop the full-text index and create it again)",
                ));
            }
            return Ok(nothing());
        }
    };
    Ok(Forward {
        statements,
        kind: Kind::Ddl,
        extra_column: None,
        aliases: Vec::new(),
        database: None,
    })
}

fn merge_refused() -> TsqlError {
    TsqlError::unsupported("MERGE (only the insert-only form with ON 1 = 0 is translated)")
}

fn rewrite_merge_insert(tokens: Vec<Token>, cx: &mut Cx) -> Result<Forward, TsqlError> {
    let mut i = 1;
    if kw(&tokens, i, "INTO") {
        i += 1;
    }
    let (table, mut j) = table_name_at(&tokens, i).ok_or_else(merge_refused)?;
    if kw(&tokens, j, "WITH") && tokens.get(j + 1).is_some_and(|t| t.is_op("(")) {
        j = matching_paren(&tokens, j + 1).ok_or_else(merge_refused)? + 1;
    }
    if kw(&tokens, j, "AS") {
        j += 2;
    } else if tokens
        .get(j)
        .is_some_and(|t| t.ident().is_some() && !t.is_word("USING"))
    {
        j += 1;
    }
    if !kw(&tokens, j, "USING")
        || !tokens.get(j + 1).is_some_and(|t| t.is_op("("))
        || !kw(&tokens, j + 2, "VALUES")
    {
        return Err(merge_refused());
    }
    let close = matching_paren(&tokens, j + 1).ok_or_else(merge_refused)?;
    let rows: Vec<Vec<Vec<Token>>> = split_commas(&tokens[j + 3..close])
        .into_iter()
        .map(|row| {
            let inner = if row.first().is_some_and(|t| t.is_op("("))
                && row.last().is_some_and(|t| t.is_op(")"))
            {
                row[1..row.len() - 1].to_vec()
            } else {
                row
            };
            split_commas(&inner)
        })
        .collect();
    let mut k = close + 1;
    if kw(&tokens, k, "AS") {
        k += 1;
    }
    let alias = tokens
        .get(k)
        .and_then(|t| t.ident())
        .ok_or_else(merge_refused)?
        .to_lowercase();
    k += 1;
    let source_close = matching_paren(&tokens, k).ok_or_else(merge_refused)?;
    let source_cols: Vec<String> = split_commas(&tokens[k + 1..source_close])
        .iter()
        .map(|c| {
            c.first()
                .and_then(|t| t.ident())
                .unwrap_or("")
                .to_lowercase()
        })
        .collect();
    let on = source_close + 1;
    let always_false = kw(&tokens, on, "ON")
        && matches!((tokens.get(on + 1), tokens.get(on + 2), tokens.get(on + 3)), (Some(a), Some(eq), Some(b)) if eq.is_op("=") && matches!((&a.tok, &b.tok), (Tok::Num(x), Tok::Num(y)) if x != y));
    if !always_false {
        return Err(merge_refused());
    }
    let insert = find_top(&tokens, on, &["INSERT"]).ok_or_else(merge_refused)?;
    if find_top(&tokens, on, &["MATCHED"]).is_none()
        || find_top(&tokens, on, &["UPDATE", "DELETE"]).is_some_and(|u| u < insert)
    {
        return Err(merge_refused());
    }
    let cols_open = insert + 1;
    let cols_close = matching_paren(&tokens, cols_open).ok_or_else(merge_refused)?;
    let target_cols: Vec<String> = split_commas(&tokens[cols_open + 1..cols_close])
        .iter()
        .map(|c| {
            c.last()
                .and_then(|t| t.ident())
                .unwrap_or("")
                .to_lowercase()
        })
        .collect();
    if !kw(&tokens, cols_close + 1, "VALUES") {
        return Err(merge_refused());
    }
    let values_close = matching_paren(&tokens, cols_close + 2).ok_or_else(merge_refused)?;
    // Each inserted value is a source column: which one.
    let mut picks = Vec::new();
    for v in split_commas(&tokens[cols_close + 3..values_close]) {
        let name = v
            .last()
            .and_then(|t| t.ident())
            .unwrap_or("")
            .to_lowercase();
        let at = source_cols
            .iter()
            .position(|c| *c == name)
            .ok_or_else(merge_refused)?;
        picks.push(at);
    }
    // OUTPUT: inserted columns, then source columns put back by position.
    let output = find_top(&tokens, values_close + 1, &["OUTPUT"]).ok_or_else(merge_refused)?;
    let mut returning = Vec::new();
    let mut extra: Option<(String, usize)> = None;
    for part in split_commas(&tokens[output + 1..]) {
        let names: Vec<String> = part
            .iter()
            .filter_map(|t| t.ident().map(|s| s.to_lowercase()))
            .collect();
        if names.first().map(String::as_str) == Some(alias.as_str()) {
            if extra.is_some() {
                return Err(merge_refused());
            }
            let col = names.get(1).cloned().unwrap_or_default();
            let at = source_cols
                .iter()
                .position(|c| *c == col)
                .ok_or_else(merge_refused)?;
            let written = part
                .last()
                .and_then(|t| t.ident())
                .unwrap_or(&col)
                .to_string();
            extra = Some((written, at));
        } else if extra.is_some() {
            return Err(merge_refused());
        } else {
            let written = part
                .last()
                .and_then(|t| t.ident())
                .unwrap_or("")
                .to_string();
            let col = names.last().cloned().unwrap_or_default();
            returning.push(format!("{} AS {}", ident_sql(&col), quote_ident(&written)));
        }
    }
    let mut row_sql = Vec::new();
    let mut positions = Vec::new();
    for row in &rows {
        let mut vals = Vec::new();
        for &p in &picks {
            let v = row.get(p).cloned().ok_or_else(merge_refused)?;
            vals.push(render_text(&rewrite_expr(v, cx)?));
        }
        row_sql.push(format!("({})", vals.join(", ")));
        if let Some((_, at)) = &extra {
            let v = row.get(*at).cloned().ok_or_else(merge_refused)?;
            positions.push(render_text(&rewrite_expr(v, cx)?));
        }
    }
    let sql = format!(
        "INSERT INTO {} ({}) VALUES {}{}",
        ident_sql(&table),
        target_cols
            .iter()
            .map(|c| ident_sql(c))
            .collect::<Vec<_>>()
            .join(", "),
        row_sql.join(", "),
        if returning.is_empty() {
            String::new()
        } else {
            format!(" RETURNING {}", returning.join(", "))
        }
    );
    Ok(Forward {
        statements: vec![sql],
        kind: if returning.is_empty() && extra.is_none() {
            Kind::Insert { identity: None, written: None }
        } else {
            Kind::Query
        },
        extra_column: extra.map(|(name, _)| (name, positions)),
        aliases: Vec::new(),
        database: None,
    })
}

/// Translates one data statement.
pub(crate) fn translate(
    sql: &str,
    tokens: Vec<Token>,
    env: &dyn Env,
) -> Result<Forward, TsqlError> {
    let mut cx = Cx::new(env);
    let mut forward = translate_in(sql, tokens, &mut cx)?;
    forward.aliases = std::mem::take(&mut cx.top_aliases);
    Ok(forward)
}

fn translate_in(sql: &str, tokens: Vec<Token>, cx: &mut Cx) -> Result<Forward, TsqlError> {
    let head = tokens.first().and_then(|t| t.upper()).unwrap_or_default();
    match head.as_str() {
        "SELECT" => {
            // SELECT … INTO t FROM …
            if let Some(into) = find_top(&tokens, 1, &["INTO"]) {
                let (table, after) = table_name_at(&tokens, into + 1)
                    .ok_or_else(|| TsqlError::syntax("INTO needs a table"))?;
                let mut select = tokens[..into].to_vec();
                select.extend(tokens[after..].iter().cloned());
                let body = rewrite_select(sql, select, cx, false)?;
                return Ok(Forward {
                    statements: vec![format!(
                        "CREATE {}TABLE {} AS {}",
                        if table.starts_with('#') {
                            "TEMPORARY "
                        } else {
                            ""
                        },
                        ident_sql(&table),
                        render_text(&body)
                    )],
                    kind: Kind::Write,
                    extra_column: None,
                    aliases: Vec::new(),
                    database: None,
                });
            }
            Ok(Forward {
                statements: vec![render_text(&rewrite_select(sql, tokens, cx, true)?)],
                kind: Kind::Query,
                extra_column: None,
                aliases: Vec::new(),
                database: None,
            })
        }
        _ if tokens.first().is_some_and(|t| t.is_op("(")) => Ok(Forward {
            statements: vec![render_text(&rewrite_select(sql, tokens, cx, true)?)],
            kind: Kind::Query,
            extra_column: None,
            aliases: Vec::new(),
            database: None,
        }),
        "WITH" => {
            let main = find_top(&tokens, 1, &["SELECT", "INSERT", "UPDATE", "DELETE"]);
            let kind = match main.and_then(|m| tokens[m].upper()).as_deref() {
                Some("SELECT") => Kind::Query,
                _ => Kind::Write,
            };
            Ok(Forward {
                statements: vec![render_text(&rewrite_expr(tokens, cx)?)],
                kind,
                extra_column: None,
                aliases: Vec::new(),
                database: None,
            })
        }
        "INSERT" => rewrite_insert(sql, tokens, cx),
        "UPDATE" => rewrite_update(tokens, cx),
        "DELETE" => rewrite_delete(tokens, cx),
        "MERGE" => rewrite_merge_insert(tokens, cx),
        "CREATE" | "DROP" | "ALTER" if kw(&tokens, 1, "FULLTEXT") => fulltext_ddl(tokens, cx),
        "CREATE" => {
            let second = tokens.get(1).and_then(|t| t.upper()).unwrap_or_default();
            match second.as_str() {
                "TABLE" => rewrite_create_table(tokens, cx),
                "INDEX" | "UNIQUE" | "CLUSTERED" | "NONCLUSTERED" => {
                    rewrite_create_index(tokens, cx)
                }
                "DATABASE" => {
                    let name = tokens
                        .get(2)
                        .and_then(|t| t.ident())
                        .ok_or_else(|| TsqlError::syntax("a database name was expected"))?;
                    Ok(Forward {
                        statements: vec![format!("CREATE DATABASE {}", ident_sql(name))],
                        kind: Kind::Ddl,
                        extra_column: None,
                        aliases: Vec::new(),
                        database: Some((name.to_string(), true)),
                    })
                }
                "SCHEMA" => {
                    let name = tokens.get(2).and_then(|t| t.ident()).unwrap_or("");
                    if name.eq_ignore_ascii_case("dbo") {
                        Ok(Forward {
                            statements: vec![String::new()],
                            kind: Kind::Ddl,
                            extra_column: None,
                            aliases: Vec::new(),
                            database: None,
                        })
                    } else {
                        Err(TsqlError::unsupported("schemas other than dbo"))
                    }
                }
                "VIEW" | "OR" => {
                    let at = find_top(&tokens, 2, &["AS"])
                        .ok_or_else(|| TsqlError::syntax("CREATE VIEW needs AS"))?;
                    let mut out = rewrite_expr(tokens[..=at].to_vec(), cx)?;
                    out.extend(rewrite_select(
                        sql,
                        tokens[at + 1..].to_vec(),
                        cx,
                        false,
                    )?);
                    Ok(Forward {
                        statements: vec![render_text(&out)],
                        kind: Kind::Ddl,
                        extra_column: None,
                        aliases: Vec::new(),
                        database: None,
                    })
                }
                other => Err(TsqlError::unsupported(format!("CREATE {other}"))),
            }
        }
        "ALTER" => {
            if kw(&tokens, 1, "TABLE") {
                rewrite_alter_table(tokens, cx)
            } else if kw(&tokens, 1, "DATABASE") && kw(&tokens, 3, "SET") {
                // Options that tune SQL Server itself, which change no answer
                // here: user access (EnsureDeleted's SINGLE_USER WITH
                // ROLLBACK IMMEDIATE before DROP DATABASE), snapshot reads
                // (MVCC already reads committed snapshots), statistics,
                // recovery. Any other option is refused.
                let option = tokens.get(4).and_then(|t| t.upper()).unwrap_or_default();
                if matches!(
                    option.as_str(),
                    "SINGLE_USER" | "MULTI_USER" | "RESTRICTED_USER" | "READ_COMMITTED_SNAPSHOT"
                        | "ALLOW_SNAPSHOT_ISOLATION" | "COMPATIBILITY_LEVEL" | "RECOVERY"
                        | "AUTO_CLOSE" | "AUTO_SHRINK" | "AUTO_UPDATE_STATISTICS"
                        | "AUTO_UPDATE_STATISTICS_ASYNC" | "AUTO_CREATE_STATISTICS" | "PAGE_VERIFY"
                        | "QUERY_STORE" | "TARGET_RECOVERY_TIME"
                ) {
                    Ok(Forward {
                        statements: vec![String::new()],
                        kind: Kind::Ddl,
                        extra_column: None,
                        aliases: Vec::new(),
                        database: None,
                    })
                } else {
                    Err(TsqlError::unsupported(format!("ALTER DATABASE … SET {option}")))
                }
            } else {
                Err(TsqlError::unsupported(format!(
                    "ALTER {}",
                    tokens.get(1).and_then(|t| t.upper()).unwrap_or_default()
                )))
            }
        }
        "DROP" => rewrite_drop(tokens, cx),
        "TRUNCATE" => {
            let (table, _) = table_name_at(&tokens, 2)
                .ok_or_else(|| TsqlError::syntax("TRUNCATE TABLE needs a table"))?;
            Ok(Forward {
                statements: vec![format!("TRUNCATE TABLE {}", ident_sql(&table))],
                kind: Kind::Ddl,
                extra_column: None,
                aliases: Vec::new(),
                database: None,
            })
        }
        _ => Ok(Forward {
            statements: vec![render_text(&rewrite_expr(tokens, cx)?)],
            kind: Kind::Other,
            extra_column: None,
            aliases: Vec::new(),
            database: None,
        }),
    }
}

/// Rewrites a condition (IF, WHILE) as a query answering 1 or 0.
pub(crate) fn condition_query(tokens: Vec<Token>, env: &dyn Env) -> Result<String, TsqlError> {
    let mut cx = Cx::new(env);
    let cond = render_text(&rewrite_expr(tokens, &mut cx)?);
    Ok(format!("SELECT CASE WHEN {cond} THEN 1 ELSE 0 END"))
}

/// Rewrites an expression (a variable's value) as a query of one value.
pub(crate) fn value_query(tokens: Vec<Token>, env: &dyn Env) -> Result<String, TsqlError> {
    let mut cx = Cx::new(env);
    let value = render_text(&rewrite_expr(tokens, &mut cx)?);
    Ok(format!("SELECT {value} AS v"))
}

/// A `SELECT @a = x, @b = y FROM …` as a query of the values, and the
/// variables in order; `None` when the select assigns nothing.
pub(crate) fn assignment_select(
    sql: &str,
    tokens: &[Token],
    env: &dyn Env,
) -> Result<Option<(String, Vec<String>, bool)>, TsqlError> {
    let mut i = 1;
    if kw(tokens, i, "TOP") {
        return Ok(None);
    }
    if kw(tokens, i, "DISTINCT") {
        i += 1;
    }
    if !matches!(tokens.get(i).map(|t| &t.tok), Some(Tok::UserVar(_)))
        || !tokens.get(i + 1).is_some_and(|t| t.is_op("="))
    {
        return Ok(None);
    }
    let end = find_top(
        tokens,
        i,
        &[
            "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "UNION", "OPTION",
        ],
    )
    .unwrap_or(tokens.len());
    let mut names = Vec::new();
    let mut select = vec![tokens[0].clone()];
    let parts = split_commas(&tokens[i..end]);
    let assigned: Vec<&str> = parts
        .iter()
        .filter_map(|p| match p.first().map(|t| &t.tok) {
            Some(Tok::UserVar(n)) => Some(n.as_str()),
            _ => None,
        })
        .collect();
    // `SELECT @s = @s + x FROM t`: folded row by row in SQL Server.
    let accumulates = end < tokens.len()
        && parts.iter().any(|p| {
            p.iter()
                .skip(2)
                .any(|t| matches!(&t.tok, Tok::UserVar(n) if assigned.contains(&n.as_str())))
        });
    for (k, part) in parts.into_iter().enumerate() {
        let Some(Tok::UserVar(name)) = part.first().map(|t| &t.tok) else {
            return Err(TsqlError::new(
                141,
                "A SELECT statement that assigns a value to a variable must not be combined with data-retrieval operations.",
            ));
        };
        names.push(name.clone());
        if k > 0 {
            select.push(Token::op(","));
        }
        select.extend(part[2..].iter().cloned());
    }
    select.extend(tokens[end..].iter().cloned());
    let mut cx = Cx::new(env);
    let body = rewrite_select(sql, select, &mut cx, false)?;
    Ok(Some((render_text(&body), names, accumulates)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mssql::lex::lex;

    struct TestEnv {
        database: Arc<Database>,
    }

    impl Env for TestEnv {
        fn database(&self) -> &Arc<Database> {
            &self.database
        }
        fn database_name(&self) -> &str {
            "app"
        }
        fn user(&self) -> &str {
            "sa"
        }
        fn variable(&self, name: &str) -> Option<Value> {
            match name {
                "n" => Some(Value::Int(5)),
                "s" => Some(Value::Str("x".into())),
                _ => None,
            }
        }
        fn param(&self, name: &str) -> Option<ParamSlot> {
            match name {
                "p0" => Some(ParamSlot {
                    index: 0,
                    string: false,
                }),
                "p1" => Some(ParamSlot {
                    index: 1,
                    string: true,
                }),
                _ => None,
            }
        }
        fn system(&self, name: &str) -> Option<Value> {
            match name {
                "version" => Some(Value::Str("Microsoft SQL Server 2022".into())),
                "rowcount" => Some(Value::Int(1)),
                _ => None,
            }
        }
        fn function(&self, name: &str) -> Option<Value> {
            match name {
                "SCOPE_IDENTITY" => Some(Value::Num("7".into())),
                _ => None,
            }
        }
    }

    fn env() -> TestEnv {
        let catalog = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(crate::core::database::catalog::DatabaseCatalog::open(
                None, "app",
            ))
            .unwrap();
        TestEnv {
            database: catalog.get("app").unwrap(),
        }
    }

    fn tr(e: &TestEnv, sql: &str) -> String {
        tr_named(e, sql).replace(r#" AS "?column?""#, "")
    }

    /// The statements before the declared names are recorded.
    fn before_names(sql: String) -> String {
        sql.split("; CREATE TABLE IF NOT EXISTS __rdb_tds_names").next().unwrap().to_string()
    }

    /// As translated, unaliased expressions' blank names (`BLANK_NAME`) kept.
    fn tr_named(e: &TestEnv, sql: &str) -> String {
        let tokens = lex(sql).unwrap();
        translate(sql, tokens, e).unwrap().statements.join("; ")
    }

    #[test]
    fn batches_split_without_semicolons() {
        let sql = "SET NOCOUNT ON SELECT 1 INSERT INTO t (a) SELECT b FROM u UPDATE t SET a = 1 WHERE a = 2 IF @n > 1 BEGIN SELECT 2 END ELSE SELECT 3";
        let stmts = parse_batch(&lex(sql).unwrap()).unwrap();
        assert_eq!(stmts.len(), 5, "{stmts:?}");
        assert!(matches!(
            &stmts[4],
            Stmt::If {
                otherwise: Some(_),
                ..
            }
        ));
        let sql = "CREATE TABLE a (id int, b int REFERENCES c (id) ON DELETE CASCADE) SELECT CASE WHEN 1 = 1 THEN 1 ELSE 0 END";
        assert_eq!(parse_batch(&lex(sql).unwrap()).unwrap().len(), 2);
        let sql = "WITH c AS (SELECT 1 AS x) SELECT x FROM c; DROP TABLE IF EXISTS t";
        assert_eq!(parse_batch(&lex(sql).unwrap()).unwrap().len(), 2);
        // Cursor statements stand alone; `open`/`close` columns do not.
        let sql = "DECLARE c CURSOR FOR SELECT a FROM t ORDER BY a OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY OPEN c FETCH NEXT FROM c INTO @a WHILE @@FETCH_STATUS = 0 FETCH NEXT FROM c INTO @a CLOSE c DEALLOCATE c";
        let stmts = parse_batch(&lex(sql).unwrap()).unwrap();
        assert_eq!(stmts.len(), 6, "{stmts:?}");
        let sql = "UPDATE prices SET close = 1, open = 2 WHERE close > open SELECT open, close FROM prices";
        assert_eq!(parse_batch(&lex(sql).unwrap()).unwrap().len(), 2);
    }

    #[test]
    fn selects() {
        let e = env();
        assert_eq!(
            tr(
                &e,
                "SELECT TOP (5) [Name] FROM [dbo].[Users] WITH (NOLOCK) ORDER BY [Name]"
            ),
            r#"SELECT name AS "Name" FROM users ORDER BY "Name" LIMIT 5"#
        );
        assert_eq!(
            tr(
                &e,
                "SELECT a FROM t ORDER BY a OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY"
            ),
            "SELECT a FROM t ORDER BY a LIMIT 5 OFFSET 10"
        );
        assert_eq!(
            tr(&e, "SELECT Total = COUNT(*) FROM t"),
            r#"SELECT ((count(*))::integer) AS "Total" FROM t"#
        );
        assert_eq!(
            tr(
                &e,
                "SELECT a, COUNT(*) AS n FROM t GROUP BY a HAVING COUNT(*) > 1 AND a IN (SELECT a FROM u WHERE b > (SELECT COUNT(*) FROM v))"
            ),
            r#"SELECT a, ((count(*))::integer) AS "n" FROM t GROUP BY a HAVING COUNT(*) > 1 AND a IN (SELECT a FROM u WHERE b > (SELECT ((count(*))::integer) FROM v))"#
        );
        assert_eq!(
            tr(&e, "SELECT 'a' + name + N'b' FROM t"),
            "SELECT 'a' || name || 'b' FROM t"
        );
        assert_eq!(
            tr(&e, "SELECT a + 1 FROM t WHERE x = @p0 AND y = @p1 + 'z'"),
            "SELECT a + 1 FROM t WHERE x = $1 AND y = $2 || 'z'"
        );
        assert_eq!(
            tr(&e, "SELECT ISNULL(a, 0), LEN(b), GETDATE() FROM t"),
            "SELECT coalesce(a, 0), char_length(rtrim(CAST(b AS text))), localtimestamp FROM t"
        );
        assert_eq!(
            tr(&e, "SELECT @@VERSION"),
            "SELECT 'Microsoft SQL Server 2022'"
        );
        assert_eq!(tr(&e, "SELECT SCOPE_IDENTITY()"), "SELECT 7");
        assert_eq!(
            tr(&e, "SELECT DATEADD(day, 1, d) FROM t"),
            "SELECT ((d) + ((1) * interval '1 days')) FROM t"
        );
        assert_eq!(
            tr(&e, "SELECT CONVERT(varchar(10), d, 23) FROM t"),
            "SELECT to_char((d)::timestamp, 'YYYY-MM-DD') FROM t"
        );
        assert_eq!(
            tr(&e, "SELECT IIF(a > 1, 'y', 'n') FROM t"),
            "SELECT (CASE WHEN a > 1 THEN 'y' ELSE 'n' END) FROM t"
        );
    }

    #[test]
    fn writes() {
        let e = env();
        assert_eq!(
            tr(
                &e,
                "INSERT INTO [t] ([a], [b]) OUTPUT INSERTED.[Id] VALUES (@p0, @p1)"
            ),
            r#"INSERT INTO t (a, b) VALUES ($1, $2) RETURNING id AS "Id""#
        );
        assert_eq!(
            tr(
                &e,
                "UPDATE [t] SET [a] = @p0 OUTPUT INSERTED.[v] WHERE [id] = @p1"
            ),
            r#"UPDATE t SET a = $1 WHERE (id = $2) RETURNING v AS "v""#
        );
        assert_eq!(
            tr(&e, "DELETE FROM [t] WHERE [id] = 3"),
            "DELETE FROM t WHERE (id = 3)"
        );
        assert_eq!(
            tr(
                &e,
                "UPDATE a SET a.x = b.y FROM t a INNER JOIN u b ON a.id = b.id WHERE b.z = 1"
            ),
            "UPDATE t AS a SET x = b.y FROM u b WHERE (a.id = b.id) AND (b.z = 1)"
        );
    }

    #[test]
    fn ef_core_merge_insert() {
        let e = env();
        let sql = "MERGE [Blogs] USING (VALUES (@p0, @p1, 0), (@p0, @p1, 1)) AS i ([Created], [Url], _Position) ON 1=0 \
                   WHEN NOT MATCHED THEN INSERT ([Created], [Url]) VALUES (i.[Created], i.[Url]) OUTPUT INSERTED.[Id], i._Position;";
        let tokens = lex(sql).unwrap();
        let stmts = parse_batch(&tokens).unwrap();
        let Stmt::Simple(t) = &stmts[0] else { panic!() };
        let f = translate(sql, t.clone(), &e)
            .map_err(|e| e.message)
            .unwrap();
        assert_eq!(
            f.statements[0],
            r#"INSERT INTO blogs (created, url) VALUES ($1, $2), ($1, $2) RETURNING id AS "Id""#
        );
        assert_eq!(
            f.extra_column,
            Some((
                "_Position".to_string(),
                vec!["0".to_string(), "1".to_string()]
            ))
        );
    }

    #[test]
    fn ddl() {
        let e = env();
        let sql = "CREATE TABLE [dbo].[Blogs] (
            [Id] int NOT NULL IDENTITY(1,1),
            [Url] nvarchar(max) NULL,
            [Rating] decimal(18,2) NOT NULL CONSTRAINT [DF_r] DEFAULT ((0)),
            [Created] datetime2 NOT NULL DEFAULT (getdate()),
            [Active] bit NOT NULL DEFAULT CAST(1 AS bit),
            CONSTRAINT [PK_Blogs] PRIMARY KEY CLUSTERED ([Id] ASC)
        ) ON [PRIMARY]";
        // The statement, then the spellings it was declared with
        // (`record_names`).
        let all = tr(&e, sql);
        assert!(
            all.contains("('blogs', '', 'Blogs'), ('blogs', 'id', 'Id'), ('blogs', 'url', 'Url')"),
            "{all}"
        );
        assert_eq!(
            all.split("; CREATE TABLE IF NOT EXISTS __rdb_tds_names").next().unwrap(),
            "CREATE TABLE blogs (id serial NOT NULL, url text NULL, rating numeric(18,2) NOT NULL DEFAULT 0, \
             created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, active boolean NOT NULL DEFAULT CAST(1 AS boolean), \
             CONSTRAINT pk_blogs PRIMARY KEY (id))"
        );
        let index = tr(&e, "CREATE NONCLUSTERED INDEX [IX_Posts_BlogId] ON [Posts] ([BlogId] ASC) WITH (ONLINE = ON)");
        assert_eq!(before_names(index.clone()), "CREATE INDEX posts__ix_posts_blogid ON posts (blogid)");
        // Its name as declared, keyed by the engine's: what errors quote.
        assert!(index.contains("('posts', '#posts__ix_posts_blogid', 'IX_Posts_BlogId')"), "{index}");
        assert_eq!(
            tr(&e, "ALTER TABLE [t] ADD [c] int NULL").split("; CREATE TABLE IF NOT EXISTS __rdb_tds_names").next().unwrap(),
            "ALTER TABLE t ADD COLUMN c integer NULL"
        );
    }

    #[test]
    fn apply_is_a_lateral_join() {
        let e = env();
        assert_eq!(
            tr(&e, "SELECT [b].[Url], [p0].[Title] FROM [Blogs] AS [b] OUTER APPLY (SELECT TOP(1) [p].[Title] FROM [Posts] AS [p] WHERE [b].[Id] = [p].[BlogId] ORDER BY [p].[Title]) AS [p0] ORDER BY [b].[Url]"),
            r#"SELECT b.url AS "Url", p0.title AS "Title" FROM blogs AS b LEFT JOIN LATERAL (SELECT p.title FROM posts AS p WHERE b.id = p.blogid ORDER BY p.title LIMIT 1) AS p0 ON 1 = 1 ORDER BY "Url""#
        );
        assert_eq!(
            tr(&e, "SELECT b.id, x.n FROM b CROSS APPLY (SELECT TOP 2 p.n FROM p WHERE p.b = b.id) x"),
            r#"SELECT b.id, x.n FROM b CROSS JOIN LATERAL (SELECT p.n FROM p WHERE p.b = b.id LIMIT 2) x"#
        );
        // An APPLY of a function is not a subquery.
        let tokens = lex("SELECT * FROM t CROSS APPLY OPENJSON(t.j) AS o").unwrap();
        assert!(translate("", tokens, &e).is_err());
    }

    #[test]
    fn a_derived_tables_top_is_its_limit() {
        let e = env();
        assert_eq!(
            tr(&e, "SELECT x.id FROM (SELECT TOP(1) b.id FROM blogs AS b ORDER BY b.url) AS x"),
            r#"SELECT x.id FROM (SELECT b.id FROM blogs AS b ORDER BY b.url LIMIT 1) AS x"#
        );
    }

    #[test]
    fn an_integer_literal_item_is_an_int() {
        let e = env();
        assert_eq!(tr(&e, "SELECT 1 AS [c], 5000000000 AS d"), r#"SELECT CAST(1 AS integer) AS "c", 5000000000 AS "d""#);
    }

    #[test]
    fn plus_over_a_function_result_concatenates() {
        let e = env();
        assert_eq!(
            tr(&e, "SELECT DATA_TYPE + ISNULL('(' + CAST(DATETIME_PRECISION AS varchar) + ')','') FROM t"),
            "SELECT DATA_TYPE || coalesce('(' || CAST(DATETIME_PRECISION AS varchar(30)) || ')', '') FROM t"
        );
        assert_eq!(tr(&e, "SELECT CONVERT(nvarchar, 5)"), "SELECT CAST(5 AS varchar(30))");
        assert_eq!(tr(&e, "SELECT CAST(x AS nvarchar(max)) FROM t"), "SELECT CAST(x AS text) FROM t");
    }

    #[test]
    fn names_are_read_from_a_script() {
        let e = env();
        let script = "IF NOT EXISTS (SELECT * FROM [__EFMigrationsHistory] WHERE [MigrationId] = N'1')\nBEGIN\n    CREATE TABLE [DenevCloud.ERP.Units] ([Id] int NOT NULL IDENTITY, [Old] int NULL, CONSTRAINT [PK_U] PRIMARY KEY ([Id]));\nEND;\nGO\nIF NOT EXISTS (SELECT 1)\nBEGIN\n    EXEC sp_rename N'[DenevCloud.ERP.Units].[Old]', N'Factor', 'COLUMN';\n    EXEC(N'CREATE UNIQUE INDEX [IX_Code] ON [DenevCloud.ERP.Units] ([Factor]) WHERE [Factor] IS NOT NULL');\nEND;\nGO";
        let all = names_from_script(script, &e).unwrap().join(";\n");
        // Recorded, nothing else: no CREATE TABLE of the relation itself.
        assert!(!all.contains("CREATE TABLE \""), "{all}");
        for want in ["'Id'", "'PK_U'", "'Factor'", "'IX_Code'", "'DenevCloud.ERP.Units'"] {
            assert!(all.contains(want), "{want}: {all}");
        }
    }

    #[test]
    fn a_not_null_filter_on_a_unique_key_is_the_key() {
        let e = env();
        assert_eq!(
            before_names(tr(&e, "CREATE UNIQUE INDEX [IX_U] ON [AspNetUsers] ([NormalizedUserName]) WHERE [NormalizedUserName] IS NOT NULL")),
            "CREATE UNIQUE INDEX aspnetusers__ix_u ON aspnetusers (normalizedusername)"
        );
        assert_eq!(
            before_names(tr(&e, "CREATE UNIQUE INDEX [IX_P] ON [P] ([Sku], [Name]) WHERE ([Sku] IS NOT NULL AND [Name] IS NOT NULL)")),
            "CREATE UNIQUE INDEX p__ix_p ON p (sku, name)"
        );
        // Not unique: the index over every row answers the same.
        assert_eq!(before_names(tr(&e, "CREATE INDEX [IX_D] ON [P] ([Sku]) WHERE [Deleted] = 0")), "CREATE INDEX p__ix_d ON p (sku)");
        for sql in [
            "CREATE UNIQUE INDEX [IX_D] ON [P] ([Sku]) WHERE [Deleted] = 0",
            "CREATE UNIQUE INDEX [IX_D] ON [P] ([Sku]) WHERE [Other] IS NOT NULL",
            "CREATE UNIQUE INDEX [IX_D] ON [P] ([Sku]) WHERE [Sku] IS NOT NULL OR [Name] IS NOT NULL",
        ] {
            assert!(translate(sql, lex(sql).unwrap(), &e).is_err(), "{sql}");
        }
    }

    #[test]
    fn alter_database_set_is_one_statement() {
        let e = env();
        let sql = "IF SERVERPROPERTY('EngineEdition') <> 5 BEGIN ALTER DATABASE [efmig] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; END; DROP DATABASE [efmig];";
        let stmts = parse_batch(&lex(sql).unwrap()).unwrap();
        assert_eq!(stmts.len(), 2, "{stmts:?}");
        assert_eq!(tr(&e, "ALTER DATABASE [d] SET READ_COMMITTED_SNAPSHOT ON"), "");
        let sql = "ALTER DATABASE [d] SET ANSI_NULLS OFF";
        assert!(translate(sql, lex(sql).unwrap(), &e).is_err());
    }

    #[test]
    fn a_long_dotted_index_name_fits_and_is_found_again() {
        let e = env();
        let create = tr(
            &e,
            "CREATE INDEX [IX_DenevCloud.ERP.ClientSpecifics.UGCS.PublicationCategories_ContentCategoryId] \
             ON [dbo].[DenevCloud.ERP.ClientSpecifics.UGCS.PublicationCategories] ([ContentCategoryId])",
        );
        let name = create.split_whitespace().nth(2).unwrap().trim_matches('"').to_string();
        assert!(name.len() <= crate::sqlcompat::NAME_BYTES, "{name} is {} bytes", name.len());
        let drop = tr(
            &e,
            "DROP INDEX [IX_DenevCloud.ERP.ClientSpecifics.UGCS.PublicationCategories_ContentCategoryId] \
             ON [dbo].[DenevCloud.ERP.ClientSpecifics.UGCS.PublicationCategories]",
        );
        assert!(drop.contains(&name), "{drop} does not drop {name}");
        // Short names are as they were.
        assert_eq!(crate::sqlcompat::fit_name("ix_a".into()), "ix_a");
        let long = "é".repeat(100);
        let fitted = crate::sqlcompat::fit_name(long.clone());
        assert!(fitted.len() <= crate::sqlcompat::NAME_BYTES);
        assert_eq!(fitted, crate::sqlcompat::fit_name(long));
        assert_ne!(fitted, crate::sqlcompat::fit_name("é".repeat(101)));
    }

    #[test]
    fn a_subquery_after_a_keyword_is_translated() {
        let e = env();
        let sql = tr(
            &e,
            "SELECT CASE WHEN 1 = 1 THEN (SELECT TOP(1) [x].[a] FROM [t] AS [x]) ELSE (SELECT TOP(1) [y].[a] FROM [t] AS [y]) END FROM [u] WHERE [u].[k] = 1 AND (SELECT TOP(1) [z].[a] FROM [t] AS [z]) = 2",
        );
        assert!(!sql.to_ascii_uppercase().contains("TOP"), "{sql}");
        assert_eq!(sql.matches("LIMIT 1").count(), 3, "{sql}");
    }

    #[test]
    fn an_unaliased_expression_goes_unnamed() {
        let e = env();
        assert_eq!(
            tr_named(&e, "SELECT COUNT_BIG(*), [a], [t].[b], 1 + 2, [c] AS [n] FROM [t]"),
            r#"SELECT count(*) AS "?column?", a, t.b, 1 + 2 AS "?column?", c AS "n" FROM t"#
        );
        // Not inside a subquery or a derived table, whose names the outer
        // query reads.
        assert!(!tr_named(&e, "SELECT x.n FROM (SELECT COUNT(*) AS n FROM t) AS x WHERE EXISTS (SELECT 1 FROM u)").contains("?column?"));
    }
}
