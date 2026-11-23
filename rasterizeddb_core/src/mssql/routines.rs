//! Stored procedures, table variables and cursors: the T-SQL objects the
//! engine has no counterpart for, kept by this front end.
//!
//! A procedure's definition (its whole `CREATE PROCEDURE` text) is stored in
//! the database, in [`ROUTINE_TABLE`], so it survives restarts, replicates
//! and is shared by every connection; it is parsed again at each `EXEC`.
//! Names starting with `__` are hidden from the catalogue views, so the
//! table is not listed among the user's.

use super::lex::{Tok, Token, split_commas};
use super::translate::TsqlError;
use crate::sqlcompat::kw;

/// Where routine definitions are kept, one row per routine.
pub(crate) const ROUTINE_TABLE: &str = "__rdb_routines";

pub(crate) const CREATE_ROUTINE_TABLE: &str = "CREATE TABLE IF NOT EXISTS __rdb_routines \
     (name text PRIMARY KEY, dialect text NOT NULL, definition text NOT NULL)";

/// SQL Server's limit on nested procedure calls.
pub(crate) const MAX_NESTING: u32 = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    Create,
    CreateOrAlter,
    Alter,
}

#[derive(Clone, Debug)]
pub(crate) struct ProcParam {
    /// Without the `@`, lower case.
    pub(crate) name: String,
    /// The declared type's name, upper case (`NVARCHAR`, `INT`, …).
    pub(crate) ty: String,
    pub(crate) default: Option<Vec<Token>>,
    pub(crate) output: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct ProcDef {
    pub(crate) name: String,
    pub(crate) mode: Mode,
    pub(crate) params: Vec<ProcParam>,
    /// Where the body starts in the tokens given to [`parse_create`].
    pub(crate) body_start: usize,
}

/// Whether a type name holds text (assigned values stay strings) or numbers
/// (assigned strings are read as numbers).
pub(crate) fn type_flags(ty: &str) -> (bool, bool) {
    let string = matches!(
        ty,
        "CHAR"
            | "NCHAR"
            | "VARCHAR"
            | "NVARCHAR"
            | "TEXT"
            | "NTEXT"
            | "SYSNAME"
            | "XML"
            | "UNIQUEIDENTIFIER"
    );
    let numeric = matches!(
        ty,
        "INT"
            | "BIGINT"
            | "SMALLINT"
            | "TINYINT"
            | "BIT"
            | "DECIMAL"
            | "NUMERIC"
            | "MONEY"
            | "SMALLMONEY"
            | "FLOAT"
            | "REAL"
    );
    (string, numeric)
}

/// A dotted name at `i` (`p`, `dbo.p`, `[db].[dbo].[p]`): its last part in
/// lower case, and the index after it.
pub(crate) fn object_name_at(tokens: &[Token], mut i: usize) -> Option<(String, usize)> {
    loop {
        let name = tokens.get(i)?.ident()?.to_lowercase();
        i += 1;
        if tokens.get(i).is_some_and(|t| t.is_op(".")) {
            i += 1;
            continue;
        }
        return Some((name, i));
    }
}

/// `CREATE [OR ALTER] PROC[EDURE]` / `ALTER PROC[EDURE]` at the start of a
/// batch: `None` when the batch is something else.
pub(crate) fn parse_create(tokens: &[Token]) -> Option<Result<ProcDef, TsqlError>> {
    let proc_at = |i: usize| kw(tokens, i, "PROC") || kw(tokens, i, "PROCEDURE");
    let (mode, at) = if kw(tokens, 0, "CREATE") && proc_at(1) {
        (Mode::Create, 2)
    } else if kw(tokens, 0, "CREATE") && kw(tokens, 1, "OR") && kw(tokens, 2, "ALTER") && proc_at(3)
    {
        (Mode::CreateOrAlter, 4)
    } else if kw(tokens, 0, "ALTER") && proc_at(1) {
        (Mode::Alter, 2)
    } else {
        return None;
    };
    Some(parse_definition(tokens, mode, at))
}

fn parse_definition(tokens: &[Token], mode: Mode, at: usize) -> Result<ProcDef, TsqlError> {
    let (name, mut i) = object_name_at(tokens, at)
        .ok_or_else(|| TsqlError::syntax("CREATE PROCEDURE needs a name"))?;
    if tokens.get(i).is_some_and(|t| t.is_op(";")) {
        return Err(TsqlError::unsupported("numbered procedures"));
    }
    // The parameter list, bare or in parentheses, ends at WITH, FOR or AS.
    let mut end = i;
    let mut depth = 0i32;
    while end < tokens.len() {
        let t = &tokens[end];
        if t.is_op("(") {
            depth += 1;
        } else if t.is_op(")") {
            depth -= 1;
        } else if depth == 0 && (t.is_word("AS") || t.is_word("WITH") || t.is_word("FOR")) {
            // `@p AS int` names a type: AS ends the list only when what
            // follows it is not a type after a parameter.
            let after_param = end > i && matches!(tokens[end - 1].tok, Tok::UserVar(_));
            if !(t.is_word("AS") && after_param) {
                break;
            }
        }
        end += 1;
    }
    let mut list = &tokens[i..end];
    if list.first().is_some_and(|t| t.is_op("("))
        && crate::sqlcompat::matching_paren(list, 0) == Some(list.len() - 1)
    {
        list = &list[1..list.len() - 1];
    }
    let mut params = Vec::new();
    for part in split_commas(list) {
        if part.is_empty() {
            continue;
        }
        params.push(parse_param(&part)?);
    }
    i = end;
    // WITH RECOMPILE / ENCRYPTION / EXECUTE AS …, FOR REPLICATION: options
    // that do not change what the procedure does here.
    while i < tokens.len() && !tokens[i].is_word("AS") {
        if tokens[i].is_word("EXECUTE") || tokens[i].is_word("EXEC") {
            // EXECUTE AS CALLER/OWNER/SELF/'user': skip its AS and the
            // principal.
            i += 3;
            continue;
        }
        i += 1;
    }
    if i >= tokens.len() {
        return Err(TsqlError::syntax(
            "CREATE PROCEDURE needs AS before its body",
        ));
    }
    let body_start = i + 1;
    if body_start >= tokens.len() {
        return Err(TsqlError::syntax("CREATE PROCEDURE needs a body"));
    }
    Ok(ProcDef {
        name,
        mode,
        params,
        body_start,
    })
}

fn parse_param(part: &[Token]) -> Result<ProcParam, TsqlError> {
    let Some(Tok::UserVar(name)) = part.first().map(|t| &t.tok) else {
        return Err(TsqlError::syntax("a procedure parameter must start with @"));
    };
    let mut k = 1;
    if kw(part, k, "AS") {
        k += 1;
    }
    let ty = part
        .get(k)
        .and_then(|t| t.upper())
        .ok_or_else(|| TsqlError::syntax(format!("parameter @{name} needs a type")))?;
    k += 1;
    if part.get(k).is_some_and(|t| t.is_op("(")) {
        k = crate::sqlcompat::matching_paren(part, k)
            .map(|c| c + 1)
            .unwrap_or(part.len());
    }
    if kw(part, k, "VARYING") {
        k += 1;
    }
    let mut default = None;
    let mut output = false;
    if part.get(k).is_some_and(|t| t.is_op("=")) {
        let start = k + 1;
        let mut end = part.len();
        while end > start
            && part[end - 1]
                .upper()
                .is_some_and(|w| matches!(w.as_str(), "OUTPUT" | "OUT" | "READONLY"))
        {
            end -= 1;
        }
        default = Some(part[start..end].to_vec());
        k = end;
    }
    for t in &part[k..] {
        match t.upper().as_deref() {
            Some("OUTPUT") | Some("OUT") => output = true,
            Some("READONLY") => return Err(TsqlError::unsupported("table-valued parameters")),
            _ => {
                return Err(TsqlError::syntax(format!(
                    "Incorrect syntax in parameter @{name}."
                )));
            }
        }
    }
    if ty == "TABLE" || ty == "CURSOR" {
        return Err(TsqlError::unsupported(format!(
            "{} parameters",
            ty.to_lowercase()
        )));
    }
    Ok(ProcParam {
        name: name.clone(),
        ty,
        default,
        output,
    })
}

/// `DECLARE name CURSOR …` / `DECLARE name INSENSITIVE SCROLL CURSOR …`.
#[derive(Clone, Debug)]
pub(crate) struct CursorDecl {
    pub(crate) name: String,
    pub(crate) scroll: bool,
    pub(crate) local: bool,
    pub(crate) select: Vec<Token>,
}

pub(crate) fn parse_cursor(tokens: &[Token]) -> Option<Result<CursorDecl, TsqlError>> {
    if !kw(tokens, 0, "DECLARE") {
        return None;
    }
    let name = match tokens.get(1).map(|t| &t.tok) {
        Some(Tok::Word(w)) => w.to_lowercase(),
        Some(Tok::Quoted(q)) => q.to_lowercase(),
        _ => return None,
    };
    let cursor = (2..tokens.len().min(12)).find(|&k| kw(tokens, k, "CURSOR"))?;
    let mut scroll = false;
    let mut local = false;
    let mut k = 2;
    while k < tokens.len() && !kw(tokens, k, "FOR") {
        match tokens[k].upper().as_deref() {
            Some("SCROLL") | Some("STATIC") | Some("KEYSET") | Some("INSENSITIVE")
            | Some("DYNAMIC") => scroll |= !kw(tokens, k, "DYNAMIC"),
            Some("LOCAL") => local = true,
            _ => {}
        }
        k += 1;
    }
    let _ = cursor;
    if k >= tokens.len() {
        return Some(Err(TsqlError::syntax(
            "DECLARE CURSOR needs FOR and a query",
        )));
    }
    let mut select = tokens[k + 1..].to_vec();
    // A trailing FOR UPDATE [OF …] / FOR READ ONLY.
    if let Some(f) = crate::sqlcompat::find_top(&select, 1, &["FOR"]) {
        if kw(&select, f + 1, "UPDATE") || kw(&select, f + 1, "READ") {
            if kw(&select, f + 1, "UPDATE") {
                return Some(Err(TsqlError::unsupported(
                    "updatable cursors (FOR UPDATE)",
                )));
            }
            select.truncate(f);
        }
    }
    if !kw(&select, 0, "SELECT") && !kw(&select, 0, "WITH") {
        return Some(Err(TsqlError::syntax("a cursor's FOR needs a SELECT")));
    }
    Some(Ok(CursorDecl {
        name,
        scroll,
        local,
        select,
    }))
}

/// The row a FETCH moves to, for `rows` rows and the cursor at `pos`
/// (-1 before the first): the new position, which is out of range when
/// there is no row (-1, or `rows`).
pub(crate) fn fetch_target(pos: i64, rows: i64, how: &str, n: i64) -> i64 {
    let target = match how {
        "FIRST" => 0,
        "LAST" => rows - 1,
        "PRIOR" => pos - 1,
        "ABSOLUTE" if n > 0 => n - 1,
        "ABSOLUTE" if n < 0 => rows + n,
        "ABSOLUTE" => -1,
        "RELATIVE" => pos + n,
        _ => pos + 1,
    };
    target.clamp(-1, rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mssql::lex::lex;

    #[test]
    fn create_procedure() {
        let t = lex("CREATE PROCEDURE dbo.[Get Orders] @id int, @name nvarchar(50) = N'x', @n AS int = 5 OUTPUT AS SELECT 1").unwrap();
        let d = parse_create(&t).unwrap().unwrap();
        assert_eq!(d.name, "get orders");
        assert_eq!(d.mode, Mode::Create);
        assert_eq!(d.params.len(), 3);
        assert_eq!(d.params[1].ty, "NVARCHAR");
        assert!(d.params[1].default.is_some());
        assert!(d.params[2].output && d.params[2].default.as_ref().unwrap().len() == 1);
        assert!(t[d.body_start].is_word("SELECT"));

        let t = lex("create or alter proc p (@a int OUT) with recompile, execute as owner as begin return 1 end").unwrap();
        let d = parse_create(&t).unwrap().unwrap();
        assert_eq!(d.mode, Mode::CreateOrAlter);
        assert!(d.params[0].output);
        assert!(t[d.body_start].is_word("BEGIN"));

        let t = lex("ALTER PROCEDURE p AS SELECT 2").unwrap();
        assert_eq!(parse_create(&t).unwrap().unwrap().mode, Mode::Alter);
        assert!(parse_create(&lex("CREATE TABLE p (a int)").unwrap()).is_none());
        assert!(
            parse_create(&lex("CREATE PROCEDURE p @t dbo.Ids READONLY AS SELECT 1").unwrap())
                .unwrap()
                .is_err()
        );
    }

    #[test]
    fn cursors() {
        let t =
            lex("DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT a, b FROM t ORDER BY a").unwrap();
        let c = parse_cursor(&t).unwrap().unwrap();
        assert_eq!(c.name, "c");
        assert!(c.local && !c.scroll);
        assert_eq!(c.select.len(), 9);
        let t = lex("DECLARE c SCROLL CURSOR FOR SELECT a FROM t FOR READ ONLY").unwrap();
        let c = parse_cursor(&t).unwrap().unwrap();
        assert!(c.scroll);
        assert_eq!(c.select.len(), 4);
        assert!(parse_cursor(&lex("DECLARE @a int").unwrap()).is_none());
        assert!(
            parse_cursor(&lex("DECLARE c CURSOR FOR SELECT a FROM t FOR UPDATE").unwrap())
                .unwrap()
                .is_err()
        );
        assert_eq!(fetch_target(-1, 3, "NEXT", 0), 0);
        assert_eq!(fetch_target(2, 3, "NEXT", 0), 3);
        assert_eq!(fetch_target(0, 3, "PRIOR", 0), -1);
        assert_eq!(fetch_target(0, 3, "ABSOLUTE", -1), 2);
        assert_eq!(fetch_target(0, 3, "RELATIVE", 5), 3);
    }
}
