//! `CREATE TABLE [IF NOT EXISTS] name [(column, …)] AS query [WITH [NO] DATA]`.
//!
//! The query runs once, inside the statement's transaction, through the same
//! materialisation a CTE body takes (`cte::create_table_as`): its rows are
//! copied out and the table is created typed by the query's description and
//! filled with them, by statements of its own. Those are what replication
//! records, so a replica stores the values this node computed rather than
//! running the query again (`now()`, a sequence, a row a peer has not yet
//! received).
//!
//! As in PostgreSQL, constraints are not copied, NOT NULL included: the new
//! table has the query's columns and types only.

/// A parsed `CREATE TABLE … AS`. Names are as written: the table's goes into
/// the generated `CREATE TABLE` and `INSERT` verbatim, so it resolves exactly
/// as it would in a plain `CREATE TABLE`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CreateTableAs {
    pub name: String,
    pub if_not_exists: bool,
    /// The column names the statement lists, unquoted; may be fewer than the
    /// query's columns, which keep their own names past the list's end.
    pub columns: Vec<String>,
    pub query: String,
    /// Where `query` is in the statement, for a rewrite around it.
    pub query_span: std::ops::Range<usize>,
    pub with_data: bool,
}

fn skip_ws(s: &[u8], mut at: usize) -> usize {
    while at < s.len() && s[at].is_ascii_whitespace() {
        at += 1;
    }
    at
}

/// `word` at `at`, case-insensitively and ending at a word boundary: the
/// position after it.
fn keyword(s: &[u8], at: usize, word: &str) -> Option<usize> {
    let end = at + word.len();
    if end > s.len() || !s[at..end].eq_ignore_ascii_case(word.as_bytes()) {
        return None;
    }
    match s.get(end) {
        Some(b) if b.is_ascii_alphanumeric() || *b == b'_' || *b == b'$' => None,
        _ => Some(end),
    }
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
}

/// One identifier, bare or double-quoted: its end and its text unquoted.
fn identifier(sql: &str, at: usize) -> Option<(usize, String)> {
    let s = sql.as_bytes();
    if s.get(at) == Some(&b'"') {
        let mut out = String::new();
        let mut i = at + 1;
        let mut run = i;
        loop {
            let q = run + sql[run..].find('"')?;
            out.push_str(&sql[i..q]);
            if s.get(q + 1) == Some(&b'"') {
                out.push('"');
                i = q + 2;
                run = i;
                continue;
            }
            if out.is_empty() {
                return None;
            }
            return Some((q + 1, out));
        }
    }
    let start = at;
    let mut i = at;
    while i < s.len() && is_ident_byte(s[i]) {
        i += 1;
    }
    if i == start || s[start].is_ascii_digit() {
        return None;
    }
    Some((i, sql[start..i].to_string()))
}

/// A possibly qualified name (`t`, `public.t`, `"Odd"."T"`): its end.
fn qualified_name(sql: &str, at: usize) -> Option<usize> {
    let (mut end, _) = identifier(sql, at)?;
    while sql.as_bytes().get(end) == Some(&b'.') {
        end = identifier(sql, end + 1)?.0;
    }
    Some(end)
}

/// The parsed statement, or `None` for anything that is not a `CREATE TABLE
/// … AS` (a plain `CREATE TABLE` with column definitions among them).
pub(crate) fn parse(sql: &str) -> Option<CreateTableAs> {
    let s = sql.as_bytes();
    let mut at = skip_ws(s, 0);
    at = skip_ws(s, keyword(s, at, "CREATE")?);
    at = skip_ws(s, keyword(s, at, "TABLE")?);
    let mut if_not_exists = false;
    if let Some(after_if) = keyword(s, at, "IF") {
        let not = skip_ws(s, after_if);
        if let Some(after_not) = keyword(s, not, "NOT") {
            let exists = skip_ws(s, after_not);
            if let Some(after_exists) = keyword(s, exists, "EXISTS") {
                if_not_exists = true;
                at = skip_ws(s, after_exists);
            }
        }
    }
    let name_end = qualified_name(sql, at)?;
    let name = sql[at..name_end].to_string();
    at = skip_ws(s, name_end);
    let mut columns = Vec::new();
    if s.get(at) == Some(&b'(') {
        at = skip_ws(s, at + 1);
        loop {
            // Only bare names: a definition (`a INT`) makes this a plain
            // CREATE TABLE, which is not this statement.
            let (end, column) = identifier(sql, at)?;
            columns.push(column);
            at = skip_ws(s, end);
            match s.get(at) {
                Some(b',') => at = skip_ws(s, at + 1),
                Some(b')') => {
                    at = skip_ws(s, at + 1);
                    break;
                }
                _ => return None,
            }
        }
    }
    let after_as = keyword(s, at, "AS")?;
    let after_as = after_as + (sql[after_as..].len() - sql[after_as..].trim_start().len());
    let mut query = sql[after_as..].trim();
    while let Some(stripped) = query.strip_suffix(';') {
        query = stripped.trim_end();
    }
    let mut with_data = true;
    for (tail, data) in [(&["WITH", "NO", "DATA"][..], false), (&["WITH", "DATA"][..], true)] {
        if let Some(rest) = strip_trailing_words(query, tail) {
            query = rest;
            with_data = data;
            break;
        }
    }
    if query.is_empty() {
        return None;
    }
    let query_span = after_as..after_as + query.len();
    Some(CreateTableAs { name, if_not_exists, columns, query: query.to_string(), query_span, with_data })
}

/// `text` without the trailing whitespace-separated `words`, matched
/// case-insensitively, or `None` when it does not end with them. A quoted or
/// commented `WITH DATA` ends in a quote or newline, never the bare word.
fn strip_trailing_words<'a>(text: &'a str, words: &[&str]) -> Option<&'a str> {
    let mut rest = text;
    for word in words.iter().rev() {
        let trimmed = rest.trim_end();
        let start = trimmed.len().checked_sub(word.len())?;
        if !trimmed.is_char_boundary(start) || !trimmed[start..].eq_ignore_ascii_case(word) {
            return None;
        }
        let before = &trimmed[..start];
        if !before.ends_with(|c: char| c.is_ascii_whitespace()) {
            return None;
        }
        rest = before;
    }
    Some(rest.trim_end())
}

/// `TABLE name` as `SELECT * FROM name`, or `None` for any other query.
pub(crate) fn table_as_select(query: &str) -> Option<String> {
    let s = query.as_bytes();
    let at = skip_ws(s, keyword(s, skip_ws(s, 0), "TABLE")?);
    let end = qualified_name(query, at)?;
    query[end..].trim().is_empty().then(|| format!("SELECT * FROM {}", &query[at..end]))
}

/// `name` as an identifier in generated SQL: bare when it is one, quoted
/// otherwise (`?column?`, a space, a quote).
pub(crate) fn quote_ident(name: &str) -> String {
    let bare = name
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
    if bare {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(sql: &str) -> Option<CreateTableAs> {
        parse(sql)
    }

    #[test]
    fn recognises_every_form_and_nothing_else() {
        let c = p("CREATE TABLE t AS SELECT 1").unwrap();
        assert_eq!((c.name.as_str(), c.if_not_exists, c.query.as_str(), c.with_data), ("t", false, "SELECT 1", true));
        assert!(c.columns.is_empty());
        let c = p("create table if not exists public.t (a, \"B\") as select x, y from s with no data;").unwrap();
        assert_eq!(c.name, "public.t");
        assert!(c.if_not_exists);
        assert_eq!(c.columns, vec!["a".to_string(), "B".to_string()]);
        assert_eq!(c.query, "select x, y from s");
        assert_eq!(&"create table if not exists public.t (a, \"B\") as select x, y from s with no data;"[c.query_span.clone()], c.query);
        assert!(!c.with_data);
        let c = p("CREATE TABLE \"Odd\"\"Name\" AS WITH w AS (SELECT 1) SELECT * FROM w WITH DATA").unwrap();
        assert_eq!(c.name, "\"Odd\"\"Name\"");
        assert_eq!(c.query, "WITH w AS (SELECT 1) SELECT * FROM w");
        assert!(c.with_data);
        // A literal or alias that merely contains the words is the query's.
        let c = p("CREATE TABLE t AS SELECT 'WITH NO DATA'").unwrap();
        assert_eq!(c.query, "SELECT 'WITH NO DATA'");
        assert!(c.with_data);
        assert!(p("CREATE TABLE t (a INT, b TEXT)").is_none());
        assert!(p("CREATE TABLE t (a INT) AS SELECT 1").is_none());
        assert!(p("CREATE TABLE asx (a INT)").is_none());
        assert!(p("CREATE TABLE t AS").is_none());
        assert!(p("CREATE INDEX i ON t (a)").is_none());
        assert!(p("CREATE TABLE t PARTITION BY HASH (a) AS SELECT 1").is_none());
    }

    #[test]
    fn a_table_query_is_a_select_of_everything() {
        assert_eq!(table_as_select("TABLE src").as_deref(), Some("SELECT * FROM src"));
        assert_eq!(table_as_select(" table public.\"S\" ").as_deref(), Some("SELECT * FROM public.\"S\""));
        assert!(table_as_select("TABLE src WHERE x").is_none());
        assert!(table_as_select("SELECT * FROM src").is_none());
        assert!(table_as_select("tables").is_none());
    }

    #[test]
    fn quotes_what_is_not_a_bare_identifier() {
        assert_eq!(quote_ident("id"), "id");
        assert_eq!(quote_ident("?column?"), "\"?column?\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_ident("1a"), "\"1a\"");
    }
}
