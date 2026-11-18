//! T-SQL's MERGE, which the engine does not have, as statements it does.
//!
//! The statements are built as T-SQL and go through the translation like
//! any other, then run in one transaction (a savepoint inside the client's):
//!
//! 1. a target row matched by more than one source row is error 8672;
//! 2. the source rows WHEN NOT MATCHED inserts are read first, while the
//!    target is as the MERGE found it;
//! 3. WHEN NOT MATCHED BY SOURCE updates or deletes the target rows no source
//!    row matches;
//! 4. WHEN MATCHED updates or deletes the matched ones;
//! 5. the rows read in step 2 are inserted.
//!
//! A clause's rows are those its condition holds for and no clause of its
//! kind before it took. Steps 3 and 4 work on disjoint rows, both as the
//! source matched them before anything changed, unless an UPDATE assigns a
//! column the ON condition reads: that is refused. A derived source
//! (`(VALUES …)`, a subquery) is first copied to a temporary table, which
//! the engine can correlate with.

use super::lex::{Tok, Token, lex, matching_paren, split_commas};
use super::translate::TsqlError;
use crate::sqlcompat::kw;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum When {
    Matched,
    NotMatched,
    NotMatchedBySource,
}

#[derive(Clone, Debug)]
pub(crate) enum Action {
    /// The SET list.
    Update(Vec<Token>),
    Delete,
    Insert {
        /// Inside the column list's parentheses.
        columns: Option<Vec<Token>>,
        /// Inside VALUES' parentheses; `None` for DEFAULT VALUES.
        values: Option<Vec<Token>>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct Clause {
    pub(crate) when: When,
    pub(crate) condition: Option<Vec<Token>>,
    pub(crate) action: Action,
}

#[derive(Clone, Debug)]
pub(crate) struct Merge {
    /// The target's name as written (`dbo.T`, `[T]`).
    pub(crate) target: Vec<Token>,
    /// Its table name, lower case.
    pub(crate) target_name: String,
    pub(crate) alias: String,
    /// The source as written, without its alias: a table name or a
    /// parenthesized query / VALUES.
    pub(crate) source: Vec<Token>,
    pub(crate) source_alias: String,
    /// `(a, b)` after the source alias, if any.
    pub(crate) source_columns: Option<Vec<Token>>,
    pub(crate) on: Vec<Token>,
    pub(crate) clauses: Vec<Clause>,
}

impl Merge {
    pub(crate) fn derived_source(&self) -> bool {
        self.source.first().is_some_and(|t| t.is_op("("))
    }
}

fn refused(what: &str) -> TsqlError {
    TsqlError::unsupported(format!("MERGE {what}"))
}

/// Where a top-level word (outside parentheses and CASE … END) from `words`
/// is, from `from`.
pub(crate) fn find_clause_word(tokens: &[Token], from: usize, words: &[&str]) -> Option<usize> {
    let mut depth = 0i32;
    let mut case = 0i32;
    for (j, t) in tokens.iter().enumerate().skip(from) {
        if t.is_op("(") {
            depth += 1;
        } else if t.is_op(")") {
            depth -= 1;
        } else if depth == 0 {
            if t.is_word("CASE") {
                case += 1;
            } else if t.is_word("END") && case > 0 {
                case -= 1;
            } else if case == 0 && t.upper().is_some_and(|w| words.contains(&w.as_str())) {
                return Some(j);
            }
        }
    }
    None
}

/// A dotted name at `i`: (its tokens, last part lower case, index after).
fn name_at(tokens: &[Token], mut i: usize) -> Option<(Vec<Token>, String, usize)> {
    let start = i;
    loop {
        let part = tokens.get(i)?.ident()?.to_lowercase();
        i += 1;
        if tokens.get(i).is_some_and(|t| t.is_op(".")) {
            i += 1;
            continue;
        }
        return Some((tokens[start..i].to_vec(), part, i));
    }
}

/// `[AS] alias` at `i`, unless the word there is one of `stop`.
fn alias_at(tokens: &[Token], i: usize, stop: &[&str]) -> (Option<String>, usize) {
    let mut j = i;
    if kw(tokens, j, "AS") {
        j += 1;
    }
    match tokens.get(j) {
        Some(t) if t.ident().is_some() && !t.upper().is_some_and(|w| stop.contains(&w.as_str())) => {
            (Some(t.ident().unwrap().to_lowercase()), j + 1)
        }
        _ => (None, i),
    }
}

/// EF Core's batched insert, `MERGE … USING (VALUES …) ON 1 = 0 WHEN NOT
/// MATCHED THEN INSERT … OUTPUT …`, which the translation turns into one
/// INSERT … RETURNING.
pub(crate) fn is_insert_only(tokens: &[Token]) -> bool {
    let Some(on) = find_clause_word(tokens, 1, &["ON"]) else {
        return false;
    };
    matches!(
        (tokens.get(on + 1).map(|t| &t.tok), tokens.get(on + 2), tokens.get(on + 3).map(|t| &t.tok)),
        (Some(Tok::Num(a)), Some(eq), Some(Tok::Num(b))) if eq.is_op("=") && a != b
    )
}

pub(crate) fn parse(tokens: &[Token]) -> Result<Merge, TsqlError> {
    let mut i = 1;
    if kw(tokens, i, "TOP") {
        return Err(refused("TOP"));
    }
    if kw(tokens, i, "INTO") {
        i += 1;
    }
    let (target, target_name, mut j) =
        name_at(tokens, i).ok_or_else(|| TsqlError::syntax("MERGE needs a target table"))?;
    if kw(tokens, j, "WITH") && tokens.get(j + 1).is_some_and(|t| t.is_op("(")) {
        j = matching_paren(tokens, j + 1).ok_or_else(|| TsqlError::syntax("unbalanced hint"))? + 1;
    }
    let (alias, j) = alias_at(tokens, j, &["USING"]);
    let alias = alias.unwrap_or_else(|| target_name.clone());
    if !kw(tokens, j, "USING") {
        return Err(TsqlError::syntax("MERGE needs USING"));
    }
    let k = j + 1;
    let (source, after) = if tokens.get(k).is_some_and(|t| t.is_op("(")) {
        let close = matching_paren(tokens, k).ok_or_else(|| TsqlError::syntax("unbalanced USING"))?;
        (tokens[k..=close].to_vec(), close + 1)
    } else {
        let (name, _, after) =
            name_at(tokens, k).ok_or_else(|| TsqlError::syntax("MERGE … USING needs a source"))?;
        let mut after = after;
        if kw(tokens, after, "WITH") && tokens.get(after + 1).is_some_and(|t| t.is_op("(")) {
            after = matching_paren(tokens, after + 1).ok_or_else(|| TsqlError::syntax("unbalanced hint"))? + 1;
        }
        (name, after)
    };
    let (source_alias, mut k) = alias_at(tokens, after, &["ON"]);
    let source_alias = match source_alias {
        Some(a) => a,
        None if !source.first().is_some_and(|t| t.is_op("(")) => {
            source.last().and_then(|t| t.ident()).unwrap_or("source").to_lowercase()
        }
        None => return Err(TsqlError::syntax("a derived table in MERGE … USING needs an alias")),
    };
    let mut source_columns = None;
    if tokens.get(k).is_some_and(|t| t.is_op("(")) {
        let close = matching_paren(tokens, k).ok_or_else(|| TsqlError::syntax("unbalanced column list"))?;
        source_columns = Some(tokens[k + 1..close].to_vec());
        k = close + 1;
    }
    if !kw(tokens, k, "ON") {
        return Err(TsqlError::syntax("MERGE needs ON"));
    }
    let first_when = find_clause_word(tokens, k + 1, &["WHEN"])
        .ok_or_else(|| TsqlError::syntax("MERGE needs a WHEN clause"))?;
    let on = tokens[k + 1..first_when].to_vec();
    let end = find_clause_word(tokens, first_when, &["OUTPUT", "OPTION"]).unwrap_or(tokens.len());
    if kw(tokens, end, "OUTPUT") {
        return Err(refused("… OUTPUT (other than EF Core's insert form)"));
    }
    let mut clauses = Vec::new();
    let mut at = first_when;
    while at < end {
        let next = find_clause_word(tokens, at + 1, &["WHEN"]).filter(|&n| n < end).unwrap_or(end);
        clauses.push(parse_clause(&tokens[at..next])?);
        at = next;
    }
    let count = |w: When| clauses.iter().filter(|c| c.when == w).count();
    if count(When::NotMatched) > 1 {
        return Err(TsqlError::new(
            10714,
            "An action of type 'WHEN NOT MATCHED' cannot appear more than once in a 'UPDATE' clause of a MERGE statement.",
        ));
    }
    if count(When::Matched) > 2 || count(When::NotMatchedBySource) > 2 {
        return Err(TsqlError::syntax("MERGE allows at most two WHEN MATCHED and two WHEN NOT MATCHED BY SOURCE clauses"));
    }
    Ok(Merge {
        target,
        target_name,
        alias,
        source,
        source_alias,
        source_columns,
        on,
        clauses,
    })
}

fn parse_clause(tokens: &[Token]) -> Result<Clause, TsqlError> {
    // WHEN [NOT] MATCHED [BY TARGET|SOURCE] [AND cond] THEN action
    let mut i = 1;
    let not = kw(tokens, i, "NOT");
    if not {
        i += 1;
    }
    if !kw(tokens, i, "MATCHED") {
        return Err(TsqlError::syntax("WHEN [NOT] MATCHED was expected"));
    }
    i += 1;
    let mut when = if not { When::NotMatched } else { When::Matched };
    if kw(tokens, i, "BY") {
        if kw(tokens, i + 1, "SOURCE") && not {
            when = When::NotMatchedBySource;
        } else if !(kw(tokens, i + 1, "TARGET") && not) {
            return Err(TsqlError::syntax("WHEN NOT MATCHED BY TARGET or BY SOURCE was expected"));
        }
        i += 2;
    }
    let then = find_clause_word(tokens, i, &["THEN"]).ok_or_else(|| TsqlError::syntax("WHEN … needs THEN"))?;
    let condition = if kw(tokens, i, "AND") {
        Some(tokens[i + 1..then].to_vec())
    } else if then == i {
        None
    } else {
        return Err(TsqlError::syntax("WHEN … AND condition THEN was expected"));
    };
    let mut body = &tokens[then + 1..];
    while body.last().is_some_and(|t| t.is_op(";")) {
        body = &body[..body.len() - 1];
    }
    let action = if kw(body, 0, "UPDATE") && kw(body, 1, "SET") {
        Action::Update(body[2..].to_vec())
    } else if kw(body, 0, "DELETE") && body.len() == 1 {
        Action::Delete
    } else if kw(body, 0, "INSERT") {
        let mut k = 1;
        let mut columns = None;
        if body.get(k).is_some_and(|t| t.is_op("(")) {
            let close = matching_paren(body, k).ok_or_else(|| TsqlError::syntax("unbalanced column list"))?;
            columns = Some(body[k + 1..close].to_vec());
            k = close + 1;
        }
        let values = if kw(body, k, "DEFAULT") && kw(body, k + 1, "VALUES") {
            None
        } else if kw(body, k, "VALUES") && body.get(k + 1).is_some_and(|t| t.is_op("(")) {
            let close = matching_paren(body, k + 1).ok_or_else(|| TsqlError::syntax("unbalanced VALUES"))?;
            Some(body[k + 2..close].to_vec())
        } else {
            return Err(TsqlError::syntax("INSERT in MERGE needs VALUES or DEFAULT VALUES"));
        };
        Action::Insert { columns, values }
    } else {
        return Err(TsqlError::syntax("WHEN … THEN UPDATE SET, DELETE or INSERT was expected"));
    };
    match (when, &action) {
        (When::NotMatched, Action::Insert { .. }) => {}
        (When::NotMatched, _) => return Err(TsqlError::syntax("WHEN NOT MATCHED [BY TARGET] can only INSERT")),
        (_, Action::Insert { .. }) => return Err(TsqlError::syntax("only WHEN NOT MATCHED [BY TARGET] can INSERT")),
        _ => {}
    }
    Ok(Clause { when, condition, action })
}

/// T-SQL text as tokens, for the statements built here.
pub(crate) fn t(text: &str) -> Vec<Token> {
    lex(text).expect("fixed T-SQL")
}

fn ident(name: &str) -> Token {
    Token::new(Tok::Quoted(name.to_string()))
}

fn paren(inner: &[Token]) -> Vec<Token> {
    let mut v = vec![Token::op("(")];
    v.extend(inner.iter().cloned());
    v.push(Token::op(")"));
    v
}

/// `AND CASE WHEN (c1) THEN 1 WHEN (c2) THEN 1 ELSE 0 END = 0`: none of the
/// earlier clauses' conditions is true.
fn not_taken(earlier: &[&Vec<Token>]) -> Vec<Token> {
    if earlier.is_empty() {
        return Vec::new();
    }
    let mut v = t("AND CASE");
    for c in earlier {
        v.extend(t("WHEN"));
        v.extend(paren(c));
        v.extend(t("THEN 1"));
    }
    v.extend(t("ELSE 0 END = 0"));
    v
}

/// The statements a MERGE runs, built from its parts. `source` is what the
/// statements read the source as (the table it was copied to, or as
/// written), with its alias.
pub(crate) struct Steps {
    /// SELECT of target keys matched more than once (empty: none).
    pub(crate) duplicates: Option<Vec<Token>>,
    /// SELECT of the values WHEN NOT MATCHED inserts, and the columns.
    pub(crate) to_insert: Option<(Vec<Token>, Option<Vec<String>>, bool)>,
    /// UPDATE / DELETE statements, in order.
    pub(crate) writes: Vec<Vec<Token>>,
}

pub(crate) fn steps(m: &Merge, source: &[Token], primary_key: Option<&str>) -> Result<Steps, TsqlError> {
    let target = || m.target.clone();
    let alias = || ident(&m.alias);
    let on = paren(&m.on);
    // Columns the ON condition reads on the target side.
    let mut on_columns = Vec::new();
    for (k, tok) in m.on.iter().enumerate() {
        if tok.ident().is_some_and(|a| a.eq_ignore_ascii_case(&m.alias))
            && m.on.get(k + 1).is_some_and(|d| d.is_op("."))
        {
            if let Some(c) = m.on.get(k + 2).and_then(|c| c.ident()) {
                on_columns.push(c.to_lowercase());
            }
        }
    }
    let mut writes = Vec::new();
    let mut to_insert = None;
    let mut by_source_conds: Vec<&Vec<Token>> = Vec::new();
    let mut matched_conds: Vec<&Vec<Token>> = Vec::new();
    let mut by_source_writes = Vec::new();
    let mut matched_writes = Vec::new();
    for c in &m.clauses {
        if let Action::Update(set) = &c.action {
            for part in split_commas(set) {
                let mut col = part.first().and_then(|t| t.ident()).map(str::to_lowercase);
                if part.get(1).is_some_and(|d| d.is_op(".")) {
                    col = part.get(2).and_then(|t| t.ident()).map(str::to_lowercase);
                }
                if col.as_ref().is_some_and(|c| on_columns.contains(c)) {
                    return Err(refused("that updates a column its ON condition reads"));
                }
            }
        }
        let cond = c.condition.as_ref().map(|cond| {
            let mut v = t("AND");
            v.extend(paren(cond));
            v
        });
        match c.when {
            When::NotMatched => {
                let Action::Insert { columns, values } = &c.action else { unreachable!() };
                let mut select = t("SELECT");
                match values {
                    Some(v) => select.extend(v.iter().cloned()),
                    None => select.extend(t("1")),
                }
                select.extend(t("FROM"));
                select.extend(source.iter().cloned());
                select.extend(t("WHERE NOT EXISTS (SELECT 1 FROM"));
                select.extend(target());
                select.extend(t("AS"));
                select.push(alias());
                select.extend(t("WHERE"));
                select.extend(on.clone());
                select.extend(t(")"));
                if let Some(cond) = &cond {
                    select.extend(cond.iter().cloned());
                }
                let names = columns.as_ref().map(|cols| {
                    split_commas(cols)
                        .iter()
                        .map(|p| p.last().and_then(|t| t.ident()).unwrap_or("").to_lowercase())
                        .collect()
                });
                to_insert = Some((select, names, values.is_none()));
            }
            When::NotMatchedBySource => {
                let mut w = match &c.action {
                    Action::Update(set) => {
                        let mut w = t("UPDATE");
                        w.push(alias());
                        w.extend(t("SET"));
                        w.extend(set.iter().cloned());
                        w.extend(t("FROM"));
                        w
                    }
                    _ => {
                        let mut w = t("DELETE");
                        w.push(alias());
                        w.extend(t("FROM"));
                        w
                    }
                };
                w.extend(target());
                w.extend(t("AS"));
                w.push(alias());
                w.extend(t("WHERE NOT EXISTS (SELECT 1 FROM"));
                w.extend(source.iter().cloned());
                w.extend(t("WHERE"));
                w.extend(on.clone());
                w.extend(t(")"));
                if let Some(cond) = &cond {
                    w.extend(cond.iter().cloned());
                }
                w.extend(not_taken(&by_source_conds));
                by_source_writes.push(w);
                if let Some(cond) = &c.condition {
                    by_source_conds.push(cond);
                }
            }
            When::Matched => {
                let w = match &c.action {
                    Action::Update(set) => {
                        let mut w = t("UPDATE");
                        w.push(alias());
                        w.extend(t("SET"));
                        w.extend(set.iter().cloned());
                        w.extend(t("FROM"));
                        w.extend(target());
                        w.extend(t("AS"));
                        w.push(alias());
                        w.extend(t("JOIN"));
                        w.extend(source.iter().cloned());
                        w.extend(t("ON"));
                        w.extend(on.clone());
                        w.extend(t("WHERE 1 = 1"));
                        if let Some(cond) = &cond {
                            w.extend(cond.iter().cloned());
                        }
                        w.extend(not_taken(&matched_conds));
                        w
                    }
                    _ => {
                        let mut w = t("DELETE");
                        w.push(alias());
                        w.extend(t("FROM"));
                        w.extend(target());
                        w.extend(t("AS"));
                        w.push(alias());
                        w.extend(t("WHERE EXISTS (SELECT 1 FROM"));
                        w.extend(source.iter().cloned());
                        w.extend(t("WHERE"));
                        w.extend(on.clone());
                        if let Some(cond) = &cond {
                            w.extend(cond.iter().cloned());
                        }
                        w.extend(not_taken(&matched_conds));
                        w.extend(t(")"));
                        w
                    }
                };
                matched_writes.push(w);
                if let Some(cond) = &c.condition {
                    matched_conds.push(cond);
                }
            }
        }
    }
    writes.extend(by_source_writes);
    writes.extend(matched_writes);
    let duplicates = match primary_key {
        Some(pk) if m.clauses.iter().any(|c| c.when == When::Matched) => {
            let mut d = t("SELECT");
            d.push(alias());
            d.push(Token::op("."));
            d.push(ident(pk));
            d.extend(t("FROM"));
            d.extend(target());
            d.extend(t("AS"));
            d.push(alias());
            d.extend(t("JOIN"));
            d.extend(source.iter().cloned());
            d.extend(t("ON"));
            d.extend(on.clone());
            d.extend(t("GROUP BY"));
            d.push(alias());
            d.push(Token::op("."));
            d.push(ident(pk));
            d.extend(t("HAVING COUNT(*) > 1"));
            Some(d)
        }
        _ => None,
    };
    Ok(Steps {
        duplicates,
        to_insert,
        writes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlcompat::render_text;

    #[test]
    fn parses_a_merge() {
        let toks = t("MERGE INTO dbo.Stock WITH (HOLDLOCK) AS t
            USING (VALUES (1, 5), (2, 0)) AS s (Id, Qty) ON t.Id = s.Id
            WHEN MATCHED AND s.Qty = 0 THEN DELETE
            WHEN MATCHED THEN UPDATE SET Qty = t.Qty + s.Qty, Note = CASE WHEN s.Qty > 3 THEN N'big' ELSE N'small' END
            WHEN NOT MATCHED BY TARGET THEN INSERT (Id, Qty) VALUES (s.Id, s.Qty)
            WHEN NOT MATCHED BY SOURCE THEN DELETE;");
        let m = parse(&toks).unwrap();
        assert_eq!(m.target_name, "stock");
        assert_eq!(m.alias, "t");
        assert_eq!(m.source_alias, "s");
        assert!(m.derived_source());
        assert_eq!(m.clauses.len(), 4);
        assert_eq!(m.clauses[2].when, When::NotMatched);
        assert_eq!(m.clauses[3].when, When::NotMatchedBySource);
        let source = t("#src AS s");
        let s = steps(&m, &source, Some("id")).unwrap();
        assert!(s.duplicates.is_some());
        assert_eq!(s.writes.len(), 3);
        // The by-source delete first, then the matched delete, then the
        // update, guarded against the delete's rows.
        assert!(render_text(&s.writes[0]).contains("NOT EXISTS"));
        let update = render_text(&s.writes[2]);
        assert!(update.contains("CASE WHEN (s.Qty = 0) THEN 1 ELSE 0 END = 0"), "{update}");
        let (insert, cols, default) = s.to_insert.unwrap();
        assert_eq!(cols, Some(vec!["id".to_string(), "qty".to_string()]));
        assert!(!default);
        assert!(render_text(&insert).starts_with("SELECT s.Id, s.Qty FROM"));
    }

    #[test]
    fn refuses_what_would_answer_differently() {
        let m = parse(&t("MERGE T USING S ON T.k = S.k WHEN MATCHED THEN UPDATE SET k = S.k2")).unwrap();
        assert_eq!(m.alias, "t");
        assert!(steps(&m, &t("S AS s"), None).is_err());
        assert!(parse(&t("MERGE T USING S ON T.k = S.k WHEN MATCHED THEN DELETE OUTPUT $action")).is_err());
        assert!(parse(&t("MERGE TOP (5) T USING S ON T.k = S.k WHEN MATCHED THEN DELETE")).is_err());
        assert!(parse(&t("MERGE T USING S ON T.k = S.k WHEN NOT MATCHED THEN DELETE")).is_err());
    }
}
