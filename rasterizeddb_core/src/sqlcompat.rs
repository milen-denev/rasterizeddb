//! What the MySQL and SQL Server front ends share: a statement as tokens,
//! and those tokens rendered back as text in the engine's dialect.
//!
//! Each front end has its own lexer (their grammars differ in quoting,
//! comments, literals and variables) producing these tokens, and its own
//! rewrites over them. Rendering is common: names are folded to lower case
//! (both dialects compare names case-insensitively), strings are standard
//! literals, and binary strings are `bytea`.

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Tok {
    /// A bare word: an identifier or a keyword, as written.
    Word(String),
    /// A quoted identifier, unescaped.
    Quoted(String),
    /// A string literal's value.
    Str(Vec<u8>),
    /// A numeric literal, as written.
    Num(String),
    /// `x'…'` / `0x…`: a binary string.
    Hex(Vec<u8>),
    /// `?`, numbered from 0 in the order written.
    Param(usize),
    /// `@@name`, `@@session.name`: the name, lowercased, scope dropped.
    SysVar(String),
    /// `@name`: the name, lowercased.
    UserVar(String),
    /// Punctuation and operators.
    Op(String),
    /// Text already in the engine's dialect, put in by a rewrite.
    Raw(String),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Token {
    pub(crate) tok: Tok,
    /// Whitespace (or a comment) came before it in the source.
    pub(crate) space: bool,
    /// Its bytes in the source, for naming a result column as written.
    pub(crate) span: (usize, usize),
}

impl Token {
    pub(crate) fn new(tok: Tok) -> Self {
        Token {
            tok,
            space: true,
            span: (0, 0),
        }
    }

    pub(crate) fn raw(text: impl Into<String>) -> Self {
        Token::new(Tok::Raw(text.into()))
    }

    pub(crate) fn op(text: &str) -> Self {
        Token::new(Tok::Op(text.to_string()))
    }

    pub(crate) fn word(text: &str) -> Self {
        Token::new(Tok::Word(text.to_string()))
    }

    /// Whether this is the bare word `kw`, case-insensitively.
    pub(crate) fn is_word(&self, kw: &str) -> bool {
        matches!(&self.tok, Tok::Word(w) if w.eq_ignore_ascii_case(kw))
    }

    pub(crate) fn is_op(&self, op: &str) -> bool {
        matches!(&self.tok, Tok::Op(o) if o == op)
    }

    /// The identifier this token names, bare or quoted.
    pub(crate) fn ident(&self) -> Option<&str> {
        match &self.tok {
            Tok::Word(w) | Tok::Quoted(w) => Some(w),
            _ => None,
        }
    }

    /// The upper-cased word, for keyword matching.
    pub(crate) fn upper(&self) -> Option<String> {
        match &self.tok {
            Tok::Word(w) => Some(w.to_ascii_uppercase()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LexError(pub(crate) String);

/// Splits tokens into statements at `;`. Empty statements are dropped.
pub(crate) fn split_statements(tokens: Vec<Token>) -> Vec<Vec<Token>> {
    let mut out = Vec::new();
    let mut current = Vec::new();
    for token in tokens {
        if token.is_op(";") {
            if !current.is_empty() {
                out.push(std::mem::take(&mut current));
            }
        } else {
            current.push(token);
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// The index of the `)` matching the `(` at `open`, if any.
pub(crate) fn matching_paren(tokens: &[Token], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (i, t) in tokens.iter().enumerate().skip(open) {
        if t.is_op("(") {
            depth += 1;
        } else if t.is_op(")") {
            depth -= 1;
            if depth == 0 {
                return Some(i);
            }
        }
    }
    None
}

/// Splits `tokens` at depth-0 commas.
pub(crate) fn split_commas(tokens: &[Token]) -> Vec<Vec<Token>> {
    let mut out = Vec::new();
    let mut current = Vec::new();
    let mut depth = 0i32;
    for t in tokens {
        if t.is_op("(") {
            depth += 1;
        } else if t.is_op(")") {
            depth -= 1;
        } else if depth == 0 && t.is_op(",") {
            out.push(std::mem::take(&mut current));
            continue;
        }
        current.push(t.clone());
    }
    if !current.is_empty() || !out.is_empty() {
        out.push(current);
    }
    out
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A name as the engine is sent it: folded to lower case, quoted when it
/// would not survive unquoted.
pub(crate) fn ident_sql(name: &str) -> String {
    let lower = name.to_lowercase();
    let plain = !lower.is_empty()
        && lower
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && lower
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !is_pg_reserved(&lower);
    if plain { lower } else { quote_ident(&lower) }
}

fn is_pg_reserved(word: &str) -> bool {
    matches!(
        word,
        "all"
            | "analyse"
            | "analyze"
            | "and"
            | "any"
            | "array"
            | "as"
            | "asc"
            | "asymmetric"
            | "both"
            | "case"
            | "cast"
            | "check"
            | "collate"
            | "column"
            | "constraint"
            | "create"
            | "current_catalog"
            | "current_date"
            | "current_role"
            | "current_time"
            | "current_timestamp"
            | "current_user"
            | "default"
            | "deferrable"
            | "desc"
            | "distinct"
            | "do"
            | "else"
            | "end"
            | "except"
            | "false"
            | "fetch"
            | "for"
            | "foreign"
            | "from"
            | "grant"
            | "group"
            | "having"
            | "in"
            | "initially"
            | "intersect"
            | "into"
            | "lateral"
            | "leading"
            | "limit"
            | "localtime"
            | "localtimestamp"
            | "not"
            | "null"
            | "offset"
            | "on"
            | "only"
            | "or"
            | "order"
            | "placing"
            | "primary"
            | "references"
            | "returning"
            | "select"
            | "session_user"
            | "some"
            | "symmetric"
            | "system_user"
            | "table"
            | "then"
            | "to"
            | "trailing"
            | "true"
            | "union"
            | "unique"
            | "user"
            | "using"
            | "variadic"
            | "when"
            | "where"
            | "window"
            | "with"
    )
}

/// Words a `(` after them is not a call of: kept apart, as the engine reads
/// `SELECT(` as one unknown word.
fn is_keyword_before_paren(word: &str) -> bool {
    matches!(
        word.to_ascii_uppercase().as_str(),
        "SELECT"
            | "IN"
            | "VALUES"
            | "AND"
            | "OR"
            | "NOT"
            | "EXISTS"
            | "WHERE"
            | "ON"
            | "AS"
            | "FROM"
            | "JOIN"
            | "USING"
            | "WHEN"
            | "THEN"
            | "ELSE"
            | "CASE"
            | "BY"
            | "ALL"
            | "ANY"
            | "SOME"
            | "INTO"
            | "TABLE"
            | "LIKE"
            | "IS"
            | "BETWEEN"
            | "DISTINCT"
            | "UNION"
            | "RETURNING"
            | "SET"
            | "LIMIT"
            | "OFFSET"
            | "HAVING"
            | "WITH"
            | "RECURSIVE"
            | "LATERAL"
            | "INTERSECT"
            | "EXCEPT"
            | "KEY"
            | "UNIQUE"
            | "CHECK"
            | "REFERENCES"
            | "FOREIGN"
            | "PRIMARY"
            | "DEFAULT"
            | "OVER"
            | "PARTITION"
            | "FILTER"
            | "WITHIN"
    )
}

pub(crate) fn string_sql(value: &[u8]) -> String {
    match std::str::from_utf8(value) {
        Ok(text) if !text.contains('\0') => format!("'{}'", text.replace('\'', "''")),
        _ => bytea_sql(value),
    }
}

pub(crate) fn bytea_sql(value: &[u8]) -> String {
    let mut out = String::with_capacity(value.len() * 2 + 12);
    out.push_str("'\\x");
    for b in value {
        out.push_str(&format!("{:02x}", b));
    }
    out.push_str("'::bytea");
    out
}

/// Tokens back to text.
pub(crate) fn render_text(tokens: &[Token]) -> String {
    let mut out = String::new();
    let mut prev: Option<&Token> = None;
    for t in tokens {
        if let Some(p) = prev {
            let glue = p.is_op("(")
                || p.is_op(".")
                || t.is_op(",")
                || t.is_op(")")
                || t.is_op(".")
                || matches!(&t.tok, Tok::Raw(r) if r.starts_with("::"));
            let call = t.is_op("(")
                && !t.space
                && match &p.tok {
                    Tok::Word(w) => !is_keyword_before_paren(w),
                    Tok::Raw(_) => true,
                    _ => false,
                };
            if !glue && !call {
                out.push(' ');
            }
        }
        match &t.tok {
            Tok::Word(w) => {
                if w.chars().next().is_some_and(|c| c.is_ascii_digit())
                    || w.contains('$')
                    || !w.is_ascii()
                {
                    out.push_str(&quote_ident(&w.to_lowercase()));
                } else {
                    out.push_str(w);
                }
            }
            Tok::Quoted(name) => out.push_str(&ident_sql(name)),
            Tok::Str(value) => out.push_str(&string_sql(value)),
            Tok::Num(n) => out.push_str(n),
            Tok::Hex(bytes) => out.push_str(&bytea_sql(bytes)),
            Tok::Param(i) => out.push_str(&format!("${}", i + 1)),
            Tok::SysVar(name) => out.push_str(&format!("@@{name}")),
            Tok::UserVar(name) => out.push_str(&format!("@{name}")),
            Tok::Op(o) => out.push_str(o),
            Tok::Raw(r) => out.push_str(r),
        }
        prev = Some(t);
    }
    out
}

/// Session temporary tables. The engine has none, so the MySQL and SQL
/// Server front ends keep them as real tables named per connection (names
/// starting with `__` are hidden from both catalogues) and drop them when
/// their scope or connection ends. The boot id keeps the names of a run
/// apart from tables a crashed run left behind.
pub(crate) const TEMP_PREFIX: &str = "__rdb_tmp_";

pub(crate) fn temp_table_prefix(protocol: &str, connection: u64) -> String {
    static BOOT: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    let boot = *BOOT.get_or_init(|| fastrand::u32(..));
    format!("{TEMP_PREFIX}{protocol}_{boot:08x}_{connection}_")
}

/// A number for each connection of this process.
pub(crate) fn next_connection_id() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

pub(crate) fn kw(tokens: &[Token], i: usize, word: &str) -> bool {
    tokens.get(i).is_some_and(|t| t.is_word(word))
}

/// The index of the next depth-0 word among `words`, from `from`.
pub(crate) fn find_top(tokens: &[Token], from: usize, words: &[&str]) -> Option<usize> {
    let mut depth = 0i32;
    for (i, t) in tokens.iter().enumerate().skip(from) {
        if t.is_op("(") {
            depth += 1;
        } else if t.is_op(")") {
            depth -= 1;
        } else if depth == 0 && words.iter().any(|w| t.is_word(w)) {
            return Some(i);
        }
    }
    None
}

pub(crate) fn wrap(inner: Vec<Token>) -> Vec<Token> {
    let mut out = Vec::with_capacity(inner.len() + 2);
    out.push(Token::op("("));
    out.extend(inner);
    out.push(Token::op(")"));
    out
}

pub(crate) fn call(name: &str, args: Vec<Vec<Token>>) -> Vec<Token> {
    let mut out = vec![Token::raw(name)];
    let mut open = Token::op("(");
    open.space = false;
    out.push(open);
    for (i, arg) in args.into_iter().enumerate() {
        if i > 0 {
            out.push(Token::op(","));
        }
        out.extend(arg);
    }
    out.push(Token::op(")"));
    out
}

pub(crate) fn literal_str(text: &str) -> Token {
    Token::new(Tok::Str(text.as_bytes().to_vec()))
}

/// MySQL's and SQL Server's index names are per table; the engine's are per database. An
/// index made here is named `<table>__<name>`, which [`plain_index_name`]
/// takes apart again.
pub(crate) fn index_name(table: &str, name: &str) -> String {
    format!("{}__{}", table.to_lowercase(), name.to_lowercase())
}

/// An index name as MySQL shows it.
pub(crate) fn plain_index_name<'a>(table: &str, name: &'a str) -> &'a str {
    name.strip_prefix(&format!("{}__", table.to_lowercase()))
        .unwrap_or(name)
}

/// The full-text document a front end's full-text key indexes and its
/// search predicates read, over `columns` (each already SQL): each column
/// NULL-safe, joined by a space, which ends a word, so the engine keys a row
/// by each column's own lexemes (`core::index::ddl::tsvector_expression`)
/// and answers a search over any of them from the same index.
pub(crate) fn fulltext_document(columns: &[String]) -> String {
    columns
        .iter()
        .map(|c| format!("coalesce({c}, '')"))
        .collect::<Vec<_>>()
        .join(" || ' ' || ")
}
