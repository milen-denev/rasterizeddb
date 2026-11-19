//! SQL Server full-text search on the engine's: `CREATE FULLTEXT INDEX`,
//! `CONTAINS`, `FREETEXT`, `CONTAINSTABLE` and `FREETEXTTABLE`.
//!
//! A full-text index over `(a, b)` is two GIN indexes over the same document,
//! `coalesce(a, '') || ' ' || coalesce(b, '')`: one under `to_tsvector('simple',
//! …)` (words as written, without case) for `CONTAINS`, one under `'english'`
//! (stemmed, stop words out) for `FREETEXT` and `FORMSOF(INFLECTIONAL, …)`.
//! Each predicate reads the document the same way, so the engine answers it
//! from the index (`core::index::gin`). A catalog (`CREATE FULLTEXT CATALOG`)
//! holds nothing here and is accepted as such.
//!
//! # CONTAINS
//!
//! A search condition is made of words (`apple`, `"apple"`), phrases
//! (`"apple pie"`), prefixes (`"app*"`; in a phrase every word is one),
//! `AND` / `&`, `OR` / `|`, `AND NOT` / `&!`, parentheses, `FORMSOF
//! (INFLECTIONAL, …)` (stemmed forms), `FORMSOF (THESAURUS, …)` (read as the
//! words: there is no thesaurus), `NEAR((a, b), n [, ordered])` (within `n`
//! words; `n` at most 20, or no limit), `a NEAR b` (both present) and
//! `ISABOUT (…)` (any of the terms; weights rank only). Each part becomes a
//! `@@` and the parts are joined by the condition's own `AND` / `OR` /
//! `AND NOT`; neighbouring parts of one kind are folded into one query.
//!
//! What differs from SQL Server: there is no stop list for `CONTAINS`
//! (`"the"` finds rows containing "the", where SQL Server's noise words are
//! never indexed), words are split at every character that is not a letter
//! or a digit, and `RANK` is `ts_rank` scaled to SQL Server's 0–1000, not
//! its own statistic.

use super::lex::{Tok, Token, split_commas};
use super::translate::TsqlError;
use crate::sqlcompat::ident_sql;

/// The two indexes a table's full-text index is: words, and stems.
pub(crate) fn index_names(table: &str) -> [String; 2] {
    [format!("{}__ftx", table.to_lowercase()), format!("{}__ftx_stem", table.to_lowercase())]
}

/// The statements `CREATE FULLTEXT INDEX ON table (columns)` is.
pub(crate) fn index_statements(table: &str, columns: &[String]) -> Vec<String> {
    let document = crate::sqlcompat::fulltext_document(&columns.iter().map(|c| ident_sql(c)).collect::<Vec<_>>());
    let [words, stems] = index_names(table);
    vec![
        format!("CREATE INDEX {} ON {} USING gin (to_tsvector('simple', {document}))", ident_sql(&words), ident_sql(table)),
        format!("CREATE INDEX {} ON {} USING gin (to_tsvector('english', {document}))", ident_sql(&stems), ident_sql(table)),
    ]
}

/// The statements `DROP FULLTEXT INDEX ON table` is.
pub(crate) fn drop_statements(table: &str) -> Vec<String> {
    index_names(table).iter().map(|n| format!("DROP INDEX IF EXISTS {}", ident_sql(n))).collect()
}

/// The columns of a `CREATE FULLTEXT INDEX ON t (…)` list: each column's
/// name, its `TYPE COLUMN`, `LANGUAGE` and `STATISTICAL_SEMANTICS` set
/// aside (one word breaker serves every language here).
pub(crate) fn index_columns(list: &[Token]) -> Result<Vec<String>, TsqlError> {
    let mut out = Vec::new();
    for part in split_commas(list) {
        let name = part
            .first()
            .and_then(|t| t.ident())
            .ok_or_else(|| TsqlError::syntax("a full-text index lists columns"))?;
        out.push(name.to_string());
    }
    if out.is_empty() {
        return Err(TsqlError::syntax("a full-text index lists columns"));
    }
    Ok(out)
}

/// The words of `text` as the engine's full-text splitter makes them.
fn words(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for c in text.chars() {
        if c.is_alphanumeric() || (c == '\'' && !current.is_empty()) {
            current.push(c);
        } else if !current.is_empty() {
            out.push(std::mem::take(&mut current).to_lowercase());
        }
    }
    if !current.is_empty() {
        out.push(current.to_lowercase());
    }
    out
}

fn quoted(word: &str) -> String {
    format!("'{}'", word.replace('\'', "''"))
}

/// A word for an `english` query: bare, so `to_tsquery` stems it (the
/// engine takes a quoted one as an already-made lexeme). A word with an
/// apostrophe in it is quoted, unstemmed, as it cannot go bare.
fn stemmable(word: &str) -> String {
    if word.contains('\'') { quoted(word) } else { word.to_string() }
}

/// A search condition, read.
#[derive(Debug, Clone, PartialEq)]
enum Cond {
    /// A `to_tsquery` text, under `english` (stems) or `simple`.
    Leaf { stems: bool, query: String },
    And(Box<Cond>, Box<Cond>),
    Or(Box<Cond>, Box<Cond>),
    AndNot(Box<Cond>, Box<Cond>),
}

#[derive(Debug, Clone, PartialEq)]
enum Lexed {
    Quoted(String),
    Word(String),
    Open,
    Close,
    Comma,
    And,
    Or,
    Not,
    /// `~`, the old proximity operator.
    Near,
}

fn lex(text: &str) -> Result<Vec<Lexed>, TsqlError> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '"' => {
                let start = i + 1;
                let mut j = start;
                while j < chars.len() && chars[j] != '"' {
                    j += 1;
                }
                if j >= chars.len() {
                    return Err(full_text_syntax(text));
                }
                out.push(Lexed::Quoted(chars[start..j].iter().collect()));
                i = j + 1;
            }
            '(' => {
                out.push(Lexed::Open);
                i += 1;
            }
            ')' => {
                out.push(Lexed::Close);
                i += 1;
            }
            ',' => {
                out.push(Lexed::Comma);
                i += 1;
            }
            '&' => {
                out.push(Lexed::And);
                i += 1;
                if chars.get(i) == Some(&'!') {
                    out.push(Lexed::Not);
                    i += 1;
                }
            }
            '|' => {
                out.push(Lexed::Or);
                i += 1;
            }
            '!' => {
                out.push(Lexed::Not);
                i += 1;
            }
            '~' => {
                out.push(Lexed::Near);
                i += 1;
            }
            _ => {
                let start = i;
                while i < chars.len() && !chars[i].is_whitespace() && !matches!(chars[i], '"' | '(' | ')' | ',' | '&' | '|' | '!' | '~')
                {
                    i += 1;
                }
                let w: String = chars[start..i].iter().collect();
                out.push(match w.to_ascii_uppercase().as_str() {
                    "AND" => Lexed::And,
                    "OR" => Lexed::Or,
                    "NOT" => Lexed::Not,
                    _ => Lexed::Word(w),
                });
            }
        }
    }
    Ok(out)
}

/// SQL Server's message for a search condition it cannot read (7630).
fn full_text_syntax(text: &str) -> TsqlError {
    TsqlError::new(7630, format!("Syntax error near '{text}' in the full-text search condition '{text}'."))
}

struct Parser<'a> {
    toks: Vec<Lexed>,
    at: usize,
    text: &'a str,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Lexed> {
        self.toks.get(self.at)
    }

    fn bad(&self) -> TsqlError {
        full_text_syntax(self.text)
    }

    fn expect(&mut self, want: Lexed) -> Result<(), TsqlError> {
        if self.peek() == Some(&want) {
            self.at += 1;
            Ok(())
        } else {
            Err(self.bad())
        }
    }

    fn or(&mut self) -> Result<Cond, TsqlError> {
        let mut left = self.and()?;
        while self.peek() == Some(&Lexed::Or) {
            self.at += 1;
            let right = self.and()?;
            left = Cond::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn and(&mut self) -> Result<Cond, TsqlError> {
        let mut left = self.near()?;
        while self.peek() == Some(&Lexed::And) {
            self.at += 1;
            if self.peek() == Some(&Lexed::Not) {
                self.at += 1;
                let right = self.near()?;
                left = Cond::AndNot(Box::new(left), Box::new(right));
            } else {
                let right = self.near()?;
                left = Cond::And(Box::new(left), Box::new(right));
            }
        }
        Ok(left)
    }

    /// `a NEAR b`, `a ~ b`: both present.
    fn near(&mut self) -> Result<Cond, TsqlError> {
        let mut left = self.term()?;
        loop {
            let infix = match self.peek() {
                Some(Lexed::Near) => true,
                Some(Lexed::Word(w)) if w.eq_ignore_ascii_case("NEAR") && self.toks.get(self.at + 1) != Some(&Lexed::Open) => true,
                _ => false,
            };
            if !infix {
                return Ok(left);
            }
            self.at += 1;
            let right = self.term()?;
            left = Cond::And(Box::new(left), Box::new(right));
        }
    }

    /// A word or phrase as a query of words as written (`stems`: words for
    /// `english` to stem), or `None` when it has no word.
    fn phrase(text: &str, stems: bool) -> Option<String> {
        let prefix = text.trim_end().ends_with('*');
        let ws = words(text.trim_end().trim_end_matches('*'));
        if ws.is_empty() {
            return None;
        }
        let suffix = if prefix { ":*" } else { "" };
        let word = |w: &String| if stems { stemmable(w) } else { quoted(w) };
        Some(ws.iter().map(|w| format!("{}{suffix}", word(w))).collect::<Vec<_>>().join(" <-> "))
    }

    fn simple_term(&mut self) -> Result<String, TsqlError> {
        self.term_of(false)
    }

    /// A word or phrase, for `english` (`stems`) or `simple`.
    fn term_of(&mut self, stems: bool) -> Result<String, TsqlError> {
        let text = match self.peek() {
            Some(Lexed::Quoted(q)) => q.clone(),
            Some(Lexed::Word(w)) => w.trim_end_matches('*').to_string(),
            _ => return Err(self.bad()),
        };
        self.at += 1;
        Self::phrase(&text, stems).ok_or_else(|| self.bad())
    }

    /// A comma-separated list of terms.
    fn term_list(&mut self) -> Result<Vec<String>, TsqlError> {
        self.terms_of(false)
    }

    fn terms_of(&mut self, stems: bool) -> Result<Vec<String>, TsqlError> {
        let mut out = vec![self.term_of(stems)?];
        while self.peek() == Some(&Lexed::Comma) {
            self.at += 1;
            out.push(self.term_of(stems)?);
        }
        Ok(out)
    }

    fn term(&mut self) -> Result<Cond, TsqlError> {
        match self.peek().cloned() {
            Some(Lexed::Open) => {
                self.at += 1;
                let inner = self.or()?;
                self.expect(Lexed::Close)?;
                Ok(inner)
            }
            Some(Lexed::Not) => Err(TsqlError::new(
                7630,
                "NOT is allowed only after AND in a full-text search condition (AND NOT, &!).",
            )),
            Some(Lexed::Word(w)) if self.toks.get(self.at + 1) == Some(&Lexed::Open) => {
                let name = w.to_ascii_uppercase();
                self.at += 2;
                let out = match name.as_str() {
                    "FORMSOF" => {
                        let kind = match self.peek() {
                            Some(Lexed::Word(k)) => k.to_ascii_uppercase(),
                            _ => return Err(self.bad()),
                        };
                        self.at += 1;
                        self.expect(Lexed::Comma)?;
                        let stems = kind == "INFLECTIONAL";
                        let terms = self.terms_of(stems)?;
                        Cond::Leaf { stems, query: terms.join(" | ") }
                    }
                    "ISABOUT" => {
                        let mut terms = Vec::new();
                        loop {
                            terms.push(self.simple_term()?);
                            // `WEIGHT (0.5)` ranks only.
                            if matches!(self.peek(), Some(Lexed::Word(w)) if w.eq_ignore_ascii_case("WEIGHT")) {
                                self.at += 1;
                                self.expect(Lexed::Open)?;
                                while !matches!(self.peek(), Some(Lexed::Close) | None) {
                                    self.at += 1;
                                }
                                self.expect(Lexed::Close)?;
                            }
                            if self.peek() == Some(&Lexed::Comma) {
                                self.at += 1;
                            } else {
                                break;
                            }
                        }
                        Cond::Leaf { stems: false, query: terms.join(" | ") }
                    }
                    "NEAR" => self.near_call()?,
                    _ => return Err(self.bad()),
                };
                self.expect(Lexed::Close)?;
                Ok(out)
            }
            Some(Lexed::Quoted(_) | Lexed::Word(_)) => Ok(Cond::Leaf {
                stems: false,
                query: self.simple_term()?,
            }),
            _ => Err(self.bad()),
        }
    }

    /// `NEAR((a, b), n [, TRUE|FALSE])` after its `(`.
    fn near_call(&mut self) -> Result<Cond, TsqlError> {
        let terms = if self.peek() == Some(&Lexed::Open) {
            self.at += 1;
            let t = self.term_list()?;
            self.expect(Lexed::Close)?;
            t
        } else {
            self.term_list()?
        };
        let mut distance: Option<u32> = None;
        let mut ordered = false;
        if self.peek() == Some(&Lexed::Comma) {
            self.at += 1;
            match self.peek() {
                Some(Lexed::Word(w)) if w.eq_ignore_ascii_case("MAX") => {}
                Some(Lexed::Word(w)) => distance = Some(w.parse().map_err(|_| self.bad())?),
                _ => return Err(self.bad()),
            }
            self.at += 1;
            if self.peek() == Some(&Lexed::Comma) {
                self.at += 1;
                ordered = matches!(self.peek(), Some(Lexed::Word(w)) if w.eq_ignore_ascii_case("TRUE"));
                self.at += 1;
            }
        }
        let all = Cond::Leaf { stems: false, query: terms.iter().map(|t| format!("({t})")).collect::<Vec<_>>().join(" & ") };
        let Some(n) = distance else { return Ok(all) };
        if terms.len() != 2 || terms.iter().any(|t| t.contains("<->")) {
            return Err(TsqlError::unsupported("NEAR with a distance over other than two single words"));
        }
        if n > 20 {
            return Err(TsqlError::unsupported("NEAR with a distance above 20"));
        }
        // Within `n` words: `n` terms between them at most, so a gap of
        // 1..=n+1 positions, either way round unless ordered.
        let mut ways = Vec::new();
        for gap in 1..=n + 1 {
            ways.push(format!("{} <{gap}> {}", terms[0], terms[1]));
            if !ordered {
                ways.push(format!("{} <{gap}> {}", terms[1], terms[0]));
            }
        }
        Ok(Cond::Leaf { stems: false, query: ways.join(" | ") })
    }
}

/// Folds neighbouring parts of one kind into one query, so a condition of
/// words alone is one `@@` the index answers in one lookup.
fn fold(c: Cond) -> Cond {
    let join = |a: Cond, b: Cond, op: &str, make: fn(Box<Cond>, Box<Cond>) -> Cond| match (fold(a), fold(b)) {
        (Cond::Leaf { stems: s1, query: q1 }, Cond::Leaf { stems: s2, query: q2 }) if s1 == s2 => Cond::Leaf {
            stems: s1,
            query: format!("({q1}) {op} ({q2})"),
        },
        (a, b) => make(Box::new(a), Box::new(b)),
    };
    match c {
        Cond::And(a, b) => join(*a, *b, "&", Cond::And),
        Cond::Or(a, b) => join(*a, *b, "|", Cond::Or),
        Cond::AndNot(a, b) => join(*a, *b, "& !", Cond::AndNot),
        leaf => leaf,
    }
}

fn configuration(stems: bool) -> &'static str {
    if stems { "english" } else { "simple" }
}

fn render(c: &Cond, document: &str) -> String {
    match c {
        Cond::Leaf { stems, query } => {
            let cfg = configuration(*stems);
            format!("(to_tsvector('{cfg}', {document}) @@ to_tsquery('{cfg}', '{}'))", query.replace('\'', "''"))
        }
        Cond::And(a, b) => format!("({} AND {})", render(a, document), render(b, document)),
        Cond::Or(a, b) => format!("({} OR {})", render(a, document), render(b, document)),
        Cond::AndNot(a, b) => format!("({} AND NOT {})", render(a, document), render(b, document)),
    }
}

/// A rank for a condition: the sum of each positive part's `ts_rank`.
fn rank(c: &Cond, document: &str) -> String {
    match c {
        Cond::Leaf { stems, query } => {
            let cfg = configuration(*stems);
            format!("ts_rank(to_tsvector('{cfg}', {document}), to_tsquery('{cfg}', '{}'))", query.replace('\'', "''"))
        }
        Cond::And(a, b) | Cond::Or(a, b) => format!("{} + {}", rank(a, document), rank(b, document)),
        Cond::AndNot(a, _) => rank(a, document),
    }
}

/// A `CONTAINS` search condition, read and folded.
fn contains_cond(search: &str) -> Result<Cond, TsqlError> {
    let mut p = Parser { toks: lex(search)?, at: 0, text: search };
    if p.toks.is_empty() {
        return Err(TsqlError::new(7645, "Null or empty full-text predicate."));
    }
    let c = p.or()?;
    if p.at != p.toks.len() {
        return Err(p.bad());
    }
    Ok(fold(c))
}

/// A `FREETEXT` string: any of its words, stemmed.
fn freetext_cond(search: &str) -> Result<Cond, TsqlError> {
    let ws = words(search);
    if ws.is_empty() {
        return Err(TsqlError::new(7645, "Null or empty full-text predicate."));
    }
    Ok(Cond::Leaf {
        stems: true,
        query: ws.iter().map(|w| stemmable(w)).collect::<Vec<_>>().join(" | "),
    })
}

/// Which predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Predicate {
    Contains,
    Freetext,
}

fn cond(predicate: Predicate, search: &str) -> Result<Cond, TsqlError> {
    match predicate {
        Predicate::Contains => contains_cond(search),
        Predicate::Freetext => freetext_cond(search),
    }
}

/// `CONTAINS (columns, search)` / `FREETEXT (…)` as a condition over the
/// document of `columns` (each already SQL).
pub(crate) fn condition(predicate: Predicate, columns: &[String], search: &str) -> Result<String, TsqlError> {
    let document = crate::sqlcompat::fulltext_document(columns);
    Ok(render(&cond(predicate, search)?, &document))
}

/// `CONTAINSTABLE (table, columns, search [, top_n])` / `FREETEXTTABLE`: a
/// derived table of the matching rows' `KEY` (the table's key column) and
/// `RANK` (0 to 1000), the best `top_n` when given.
pub(crate) fn table(
    predicate: Predicate,
    table: &str,
    key: &str,
    columns: &[String],
    search: &str,
    top: Option<u64>,
) -> Result<String, TsqlError> {
    let document = crate::sqlcompat::fulltext_document(columns);
    let c = cond(predicate, search)?;
    let limit = top.map(|n| format!(" ORDER BY \"RANK\" DESC LIMIT {n}")).unwrap_or_default();
    Ok(format!(
        "(SELECT {} AS \"KEY\", least(1000, round(({}) * 1000))::int AS \"RANK\" FROM {} WHERE {}{limit})",
        ident_sql(key),
        rank(&c, &document),
        ident_sql(table),
        render(&c, &document)
    ))
}

/// The search string argument: a literal, or a variable or parameter the
/// statement is run with (`value`).
pub(crate) fn search_text(arg: &[Token], value: impl Fn(&str) -> Option<Option<String>>) -> Result<String, TsqlError> {
    match arg {
        [Token { tok: Tok::Str(s), .. }] => Ok(String::from_utf8_lossy(s).into_owned()),
        [Token { tok: Tok::UserVar(name), .. }] => match value(name) {
            Some(Some(text)) => Ok(text),
            Some(None) => Err(TsqlError::new(7645, "Null or empty full-text predicate.")),
            None => Err(TsqlError::unsupported("a full-text search condition that is not a literal, variable or parameter")),
        },
        _ => Err(TsqlError::unsupported("a full-text search condition that is not a literal, variable or parameter")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(s: &str) -> String {
        render(&contains_cond(s).unwrap(), "d")
    }

    #[test]
    fn contains_conditions() {
        assert_eq!(q("apple"), "(to_tsvector('simple', d) @@ to_tsquery('simple', '''apple'''))");
        assert_eq!(
            q("\"apple pie\" AND NOT crust"),
            "(to_tsvector('simple', d) @@ to_tsquery('simple', '(''apple'' <-> ''pie'') & ! (''crust'')'))"
        );
        assert_eq!(q("\"app*\" | pear"), "(to_tsvector('simple', d) @@ to_tsquery('simple', '(''app'':*) | (''pear'')'))");
        assert_eq!(
            q("FORMSOF(INFLECTIONAL, run) AND shoe"),
            "((to_tsvector('english', d) @@ to_tsquery('english', 'run')) AND (to_tsvector('simple', d) @@ to_tsquery('simple', '''shoe''')))"
        );
        assert_eq!(
            q("NEAR((fox, dog), 1, TRUE)"),
            "(to_tsvector('simple', d) @@ to_tsquery('simple', '''fox'' <1> ''dog'' | ''fox'' <2> ''dog'''))"
        );
        assert_eq!(q("NEAR((fox, dog))"), "(to_tsvector('simple', d) @@ to_tsquery('simple', '(''fox'') & (''dog'')'))");
        assert_eq!(
            q("ISABOUT(fox WEIGHT(.8), dog WEIGHT(.2))"),
            "(to_tsvector('simple', d) @@ to_tsquery('simple', '''fox'' | ''dog'''))"
        );
        assert!(contains_cond("NOT fox").is_err());
        assert!(contains_cond("\"open").is_err());
        assert!(contains_cond("").is_err());
    }

    #[test]
    fn freetext_and_tables() {
        assert_eq!(
            render(&freetext_cond("running shoes").unwrap(), "d"),
            "(to_tsvector('english', d) @@ to_tsquery('english', 'running | shoes'))"
        );
        let t = table(Predicate::Contains, "docs", "id", &["\"body\"".into()], "fox", Some(5)).unwrap();
        assert!(t.starts_with("(SELECT id AS \"KEY\", least(1000, round((ts_rank("), "{t}");
        assert!(t.ends_with("ORDER BY \"RANK\" DESC LIMIT 5)"), "{t}");
    }
}
