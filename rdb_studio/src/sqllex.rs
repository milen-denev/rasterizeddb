//! The one lexer for everything that reads SQL while it is being typed: the
//! highlighter colours its tokens, the linter reasons over them, and running
//! the statement under the cursor splits on its semicolons.
//!
//! # Why it works a line at a time
//!
//! The editor re-highlights from the first line that changed, one line at a
//! time, and never hands the highlighter the whole buffer. So the lexer takes a
//! line and the state the previous one ended in — inside a block comment, a
//! string, a quoted identifier or a dollar-quoted body — and returns the state
//! this one ends in. The linter drives the same function over every line, so
//! the colours and the diagnostics can never disagree about where a string
//! ends.
//!
//! Positions are **byte** offsets within a line, which is what both the
//! editor's highlight ranges and its cursor column are measured in. Every
//! delimiter is ASCII and every byte of a multi-byte character is lexed as part
//! of a word, so a token boundary never falls inside a character.

/// What a line ends inside of, and so what the next one starts inside of.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Carry {
    #[default]
    None,
    /// Inside `/* … */`, at this nesting depth — PostgreSQL nests them.
    Comment(u32),
    /// Inside `'…'`. `escapes` for an `E'…'` string, where a backslash escapes
    /// the next character, including a quote.
    String { escapes: bool },
    /// Inside `"…"`.
    Quoted,
    /// Inside `$tag$ … $tag$`; the tag without its dollars.
    Dollar(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A bare word: a keyword, a name, a type.
    Word,
    /// `"a quoted identifier"`.
    Quoted,
    /// A string literal of any flavour, dollar-quoted bodies included.
    String,
    Number,
    Comment,
    /// `=`, `<>`, `::`, `||`, `->>` and the rest.
    Operator,
    /// `(`, `)`, `,`, `;`, `.`, `[`, `]`, and anything unrecognised.
    Punct,
    /// `$1`, a bind parameter.
    Param,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Token {
    pub kind: Kind,
    pub start: usize,
    pub end: usize,
}

/// Lexes one line that begins in `carry`, and returns its tokens and the state
/// it ends in. Whitespace produces no token.
pub fn lex_line(line: &str, carry: &Carry) -> (Vec<Token>, Carry) {
    let bytes = line.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0usize;

    // Finish whatever the previous line left open.
    let mut carry = carry.clone();
    if carry != Carry::None {
        let kind = match &carry {
            Carry::Comment(_) => Kind::Comment,
            Carry::Quoted => Kind::Quoted,
            _ => Kind::String,
        };
        let (end, next) = continue_carry(bytes, 0, carry);
        push(&mut tokens, kind, 0, end);
        i = end;
        carry = next;
        if carry != Carry::None {
            return (tokens, carry);
        }
    }

    while i < bytes.len() {
        let c = bytes[i];

        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }

        if c == b'-' && bytes.get(i + 1) == Some(&b'-') {
            push(&mut tokens, Kind::Comment, i, bytes.len());
            return (tokens, Carry::None);
        }

        if c == b'/' && bytes.get(i + 1) == Some(&b'*') {
            let (end, next) = continue_carry(bytes, i + 2, Carry::Comment(1));
            push(&mut tokens, Kind::Comment, i, end);
            if next != Carry::None {
                return (tokens, next);
            }
            i = end;
            continue;
        }

        // A one-letter prefix glued to a quote is part of the literal:
        // E'…' (with escapes), B'…', X'…', N'…'.
        let prefixed = matches!(c, b'E' | b'e' | b'B' | b'b' | b'X' | b'x' | b'N' | b'n')
            && bytes.get(i + 1) == Some(&b'\'');
        if c == b'\'' || prefixed {
            let escapes = prefixed && matches!(c, b'E' | b'e');
            let open = if prefixed { i + 2 } else { i + 1 };
            let (end, next) = continue_carry(bytes, open, Carry::String { escapes });
            push(&mut tokens, Kind::String, i, end);
            if next != Carry::None {
                return (tokens, next);
            }
            i = end;
            continue;
        }

        if c == b'"' {
            let (end, next) = continue_carry(bytes, i + 1, Carry::Quoted);
            push(&mut tokens, Kind::Quoted, i, end);
            if next != Carry::None {
                return (tokens, next);
            }
            i = end;
            continue;
        }

        if c == b'$' {
            // `$1` is a parameter; `$$` and `$tag$` open a dollar-quoted body.
            if bytes.get(i + 1).is_some_and(u8::is_ascii_digit) {
                let mut end = i + 1;
                while end < bytes.len() && bytes[end].is_ascii_digit() {
                    end += 1;
                }
                push(&mut tokens, Kind::Param, i, end);
                i = end;
                continue;
            }
            let mut tag_end = i + 1;
            while tag_end < bytes.len()
                && (bytes[tag_end].is_ascii_alphanumeric() || bytes[tag_end] == b'_')
            {
                tag_end += 1;
            }
            let tag_ok = tag_end == i + 1 || !bytes[i + 1].is_ascii_digit();
            if tag_ok && bytes.get(tag_end) == Some(&b'$') {
                let tag = line[i + 1..tag_end].to_string();
                let (end, next) = continue_carry(bytes, tag_end + 1, Carry::Dollar(tag));
                push(&mut tokens, Kind::String, i, end);
                if next != Carry::None {
                    return (tokens, next);
                }
                i = end;
                continue;
            }
            push(&mut tokens, Kind::Punct, i, i + 1);
            i += 1;
            continue;
        }

        if c.is_ascii_digit()
            || (c == b'.' && bytes.get(i + 1).is_some_and(u8::is_ascii_digit))
        {
            let end = number_end(bytes, i);
            push(&mut tokens, Kind::Number, i, end);
            i = end;
            continue;
        }

        if is_word_start(c) {
            let mut end = i + 1;
            while end < bytes.len() && is_word_byte(bytes[end]) {
                end += 1;
            }
            push(&mut tokens, Kind::Word, i, end);
            i = end;
            continue;
        }

        if c == b':' && bytes.get(i + 1) == Some(&b':') {
            push(&mut tokens, Kind::Operator, i, i + 2);
            i += 2;
            continue;
        }

        if is_operator(c) {
            let mut end = i + 1;
            while end < bytes.len() && is_operator(bytes[end]) {
                // An operator never swallows the start of a comment.
                if (bytes[end] == b'-' && bytes.get(end + 1) == Some(&b'-'))
                    || (bytes[end] == b'/' && bytes.get(end + 1) == Some(&b'*'))
                {
                    break;
                }
                end += 1;
            }
            push(&mut tokens, Kind::Operator, i, end);
            i = end;
            continue;
        }

        push(&mut tokens, Kind::Punct, i, i + 1);
        i += 1;
    }

    (tokens, Carry::None)
}

/// Scans from `from` for the end of whatever `carry` is inside. Returns where
/// the token ends and the carry left over: `Carry::None` if it closed on this
/// line, the (possibly re-nested) carry if it runs past the end.
fn continue_carry(bytes: &[u8], from: usize, carry: Carry) -> (usize, Carry) {
    let mut i = from;
    match carry {
        Carry::None => (from, Carry::None),
        Carry::Comment(mut depth) => {
            while i < bytes.len() {
                if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
                    depth += 1;
                    i += 2;
                } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                    depth -= 1;
                    i += 2;
                    if depth == 0 {
                        return (i, Carry::None);
                    }
                } else {
                    i += 1;
                }
            }
            (bytes.len(), Carry::Comment(depth))
        }
        Carry::String { escapes } => {
            while i < bytes.len() {
                if escapes && bytes[i] == b'\\' {
                    i += 2;
                    continue;
                }
                if bytes[i] == b'\'' {
                    // A doubled quote is an escaped quote, not the end.
                    if bytes.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    return (i + 1, Carry::None);
                }
                i += 1;
            }
            (bytes.len(), Carry::String { escapes })
        }
        Carry::Quoted => {
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if bytes.get(i + 1) == Some(&b'"') {
                        i += 2;
                        continue;
                    }
                    return (i + 1, Carry::None);
                }
                i += 1;
            }
            (bytes.len(), Carry::Quoted)
        }
        Carry::Dollar(tag) => {
            let close = format!("${tag}$");
            let close = close.as_bytes();
            while i + close.len() <= bytes.len() {
                if &bytes[i..i + close.len()] == close {
                    return (i + close.len(), Carry::None);
                }
                i += 1;
            }
            (bytes.len(), Carry::Dollar(tag))
        }
    }
}

fn number_end(bytes: &[u8], from: usize) -> usize {
    let digits = |mut i: usize| {
        while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'_') {
            i += 1;
        }
        i
    };
    let mut i = digits(from);
    if bytes.get(i) == Some(&b'.') && bytes.get(i + 1) != Some(&b'.') {
        i = digits(i + 1);
    }
    if matches!(bytes.get(i), Some(b'e' | b'E')) {
        let sign = usize::from(matches!(bytes.get(i + 1), Some(b'+' | b'-')));
        if bytes.get(i + 1 + sign).is_some_and(u8::is_ascii_digit) {
            i = digits(i + 1 + sign);
        }
    }
    i
}

fn push(tokens: &mut Vec<Token>, kind: Kind, start: usize, end: usize) {
    if end > start {
        tokens.push(Token { kind, start, end });
    }
}

fn is_word_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_' || c >= 0x80
}

fn is_word_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80
}

fn is_operator(c: u8) -> bool {
    matches!(
        c,
        b'+' | b'-' | b'*' | b'/' | b'<' | b'>' | b'=' | b'~' | b'!' | b'@' | b'#' | b'%' | b'^'
            | b'&' | b'|' | b'`' | b'?'
    )
}

// ---------------------------------------------------------------------------
// The whole buffer
// ---------------------------------------------------------------------------

/// A token with the line it is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Located {
    pub line: usize,
    pub token: Token,
}

/// Every token in a buffer, and whatever the buffer ends still inside of.
#[derive(Debug, Default)]
pub struct Lexed<'a> {
    /// The buffer's lines, without their endings — the editor's own lines.
    pub lines: Vec<&'a str>,
    pub tokens: Vec<Located>,
    /// Where an unterminated string, comment, quoted identifier or dollar body
    /// opened, if the buffer ends inside one.
    pub open: Option<(usize, usize, Carry)>,
}

impl<'a> Lexed<'a> {
    pub fn text(&self, token: &Located) -> &'a str {
        &self.lines[token.line][token.token.start..token.token.end]
    }
}

/// Splits the buffer into the editor's lines: on `\n`, with a `\r` before it
/// dropped, so a CRLF file lexes the same as an LF one.
pub fn lines(text: &str) -> Vec<&str> {
    text.split('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        .collect()
}

pub fn lex(text: &str) -> Lexed<'_> {
    let lines = lines(text);
    let mut tokens = Vec::new();
    let mut carry = Carry::None;
    let mut open: Option<(usize, usize, Carry)> = None;

    for (index, line) in lines.iter().enumerate() {
        let started_inside = carry != Carry::None;
        let (line_tokens, next) = lex_line(line, &carry);
        if next != Carry::None {
            // The last token is the open one. It opened here unless it is the
            // continuation the line began with.
            let last = line_tokens.last().copied();
            // An empty line inside a block comment has no token at all.
            let continuation = started_inside && line_tokens.len() <= 1;
            if !continuation || open.is_none() {
                let column = match last {
                    Some(token) if !continuation => token.start,
                    _ => 0,
                };
                open = Some((index, column, next.clone()));
            }
        } else {
            open = None;
        }
        tokens.extend(line_tokens.into_iter().map(|token| Located { line: index, token }));
        carry = next;
    }

    Lexed { lines, tokens, open }
}

// ---------------------------------------------------------------------------
// Vocabulary
// ---------------------------------------------------------------------------

/// Reserved and structural words, upper-cased. A name that collides with one
/// of these is coloured as the keyword, which is also how the server reads it
/// unquoted.
pub const KEYWORDS: &[&str] = &[
    "ABORT", "ADD", "ALL", "ALTER", "ANALYZE", "AND", "ANY", "ARCHIVE", "ARRAY", "AS", "ASC",
    "ASYMMETRIC", "AT", "BACKUP", "BEGIN", "BETWEEN", "BOTH", "BY", "CASCADE", "CASE", "CAST",
    "CHECK", "CHECKPOINT", "COLLATE", "COLUMN", "COLUMNS", "COMMIT", "CONCURRENTLY", "CONFIG",
    "CONFLICT", "CONSTRAINT", "COPY", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE",
    "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DATABASES",
    "DEALLOCATE", "DECLARE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DESCRIBE",
    "DISABLE", "DISCARD", "DISTINCT", "DO", "DROP", "EACH", "ELSE", "ENABLE", "END", "ESCAPE",
    "EXCEPT", "EXCLUDE", "EXECUTE", "EXISTS", "EXPLAIN", "EXTENSION", "FALSE", "FETCH",
    "FILTER", "FIRST", "FOLLOWING", "FOR", "FOREIGN", "FROM", "FULL", "FUNCTION", "GRANT",
    "GROUP", "GROUPS", "HAVING", "IF", "ILIKE", "IMMEDIATE", "IMPORT", "IN", "INCLUDE",
    "INDEX", "INHERITS", "INITIALLY", "INNER", "INSERT", "INTERSECT", "INTO", "IS", "ISNULL",
    "JOIN", "KEY", "KEYS", "LAST", "LATERAL", "LEADING", "LEFT", "LIKE", "LIMIT", "LISTEN",
    "LOCAL", "LOCK", "MATCHED", "MATERIALIZED", "MERGE", "MOVE", "NATURAL", "NEXT", "NO",
    "NOT", "NOTHING", "NOTIFY", "NOTNULL", "NOWAIT", "NULL", "NULLS", "OF", "OFFSET", "ON",
    "ONLY", "OR", "ORDER", "ORDINALITY", "OTHERS", "OUTER", "OVER", "OVERLAPS", "OWNED",
    "PARTITION", "PAUSE", "PEER", "PRECEDING", "PREPARE", "PRIMARY", "PROCEDURE", "PRUNE",
    "QUERIES", "QUERY", "RANGE", "REBALANCE", "RECURSIVE", "REFERENCES", "REFRESH", "REINDEX",
    "RELEASE", "RENAME", "REPLACE", "REPLICATION", "RESET", "RESTART", "RESTORE", "RESTRICT",
    "RESUME", "RESYNC", "RETURNING", "REVOKE", "RIGHT", "ROLLBACK", "ROW", "ROWS", "SAVEPOINT",
    "SCHEMA", "SELECT", "SEQUENCE", "SESSION_USER", "SET", "SETOF", "SHARE", "SHOW", "SIMILAR",
    "SKIP", "SOME", "START", "STATS", "STATUS", "STORE", "SYMMETRIC", "TABLE", "TABLES",
    "TEMP", "TEMPORARY", "THEN", "TIES", "TO", "TRAILING", "TRANSACTION", "TRIGGER", "TRUE",
    "TRUNCATE", "TYPE", "UNBOUNDED", "UNION", "UNIQUE", "UNLISTEN", "UNLOGGED", "UNTIL",
    "UPDATE", "USE", "USER", "USING", "VACUUM", "VALUES", "VARIADIC", "VERBOSE", "VIEW",
    "WAL", "WHEN", "WHERE", "WINDOW", "WITH", "WITHIN", "WITHOUT", "WORK", "ZONE",
];

/// Type names, upper-cased.
pub const TYPES: &[&str] = &[
    "BIGINT", "BIGSERIAL", "BIT", "BOOL", "BOOLEAN", "BOX", "BYTEA", "CHAR", "CHARACTER",
    "CIDR", "CIRCLE", "DATE", "DATERANGE", "DECIMAL", "DOUBLE", "FLOAT", "FLOAT4", "FLOAT8",
    "INET", "INT", "INT1", "INT16", "INT2", "INT4", "INT8", "INT4RANGE", "INT8RANGE", "INTEGER",
    "INTERVAL", "JSON", "JSONB", "LINE", "LSEG", "MACADDR", "MACADDR8", "MONEY", "NUMERIC",
    "NUMRANGE", "OID", "PATH", "POINT", "POLYGON", "PRECISION", "REAL", "REGCLASS",
    "SERIAL", "SMALLINT", "SMALLSERIAL", "STRING", "TEXT", "TIME", "TIMESTAMP",
    "TIMESTAMPTZ", "TIMETZ", "TINYINT", "TSRANGE", "TSTZRANGE", "TSQUERY", "TSVECTOR",
    "U128", "U16", "U32", "U64", "U8", "I128", "I16", "I32", "I64", "I8", "F32", "F64",
    "UUID", "VARBIT", "VARCHAR", "VARYING", "XML",
];

/// Keywords that are also the names of functions, and are coloured as one
/// when a `(` follows: `LEFT(name, 3)` against `LEFT JOIN`.
pub const FUNCTION_KEYWORDS: &[&str] = &[
    "CAST", "COALESCE", "EXTRACT", "GREATEST", "LEAST", "LEFT", "NULLIF",
    "OVERLAY", "POSITION", "REPLACE", "RIGHT", "ROW", "SUBSTRING", "TRIM",
];

pub fn is_keyword(upper: &str) -> bool {
    KEYWORDS.binary_search(&upper).is_ok()
}

pub fn is_type(upper: &str) -> bool {
    TYPES.contains(&upper)
}

/// The words `NULL`, `TRUE` and `FALSE`: values, not structure.
pub fn is_constant(upper: &str) -> bool {
    matches!(upper, "NULL" | "TRUE" | "FALSE")
}

/// An identifier as the server would look it up: a quoted one without its
/// quotes and with doubled quotes undone, a bare one lower-cased.
pub fn identifier(kind: Kind, text: &str) -> String {
    match kind {
        Kind::Quoted => text
            .strip_prefix('"')
            .map(|inner| inner.strip_suffix('"').unwrap_or(inner))
            .unwrap_or(text)
            .replace("\"\"", "\""),
        _ => text.to_lowercase(),
    }
}

// ---------------------------------------------------------------------------
// Statements
// ---------------------------------------------------------------------------

/// The statement the cursor is in, as text, for running just that one.
///
/// Statements end at a `;` that is not inside a string, comment or quoted
/// identifier. A cursor after the last `;` with nothing but whitespace or
/// comments behind it means the statement just finished — `SELECT 1;|` runs
/// `SELECT 1`, which is what someone who just typed the semicolon wants.
pub fn statement_at(text: &str, line: usize, column: usize) -> Option<String> {
    let lexed = lex(text);
    let starts = line_starts(text);
    let offset = |line: usize, column: usize| -> usize {
        starts.get(line).map_or(text.len(), |start| (start + column).min(text.len()))
    };
    let cursor = offset(line, column);

    // Every statement as a byte range, with whether it has anything in it
    // other than comments.
    let mut ranges: Vec<(usize, usize, bool)> = Vec::new();
    let mut start = 0usize;
    let mut significant = false;
    for token in &lexed.tokens {
        let at = offset(token.line, token.token.start);
        if token.token.kind == Kind::Punct && lexed.text(token) == ";" {
            ranges.push((start, at + 1, significant));
            start = at + 1;
            significant = false;
        } else if token.token.kind != Kind::Comment {
            significant = true;
        }
    }
    ranges.push((start, text.len(), significant));

    let index = ranges
        .iter()
        .position(|&(from, to, _)| cursor >= from && cursor <= to && from != to)
        .unwrap_or(ranges.len() - 1);
    let chosen = ranges[..=index].iter().rev().find(|range| range.2)?;
    let sql = text[chosen.0..chosen.1].trim();
    let sql = sql.strip_suffix(';').unwrap_or(sql).trim_end();
    (!sql.is_empty()).then(|| sql.to_string())
}

fn line_starts(text: &str) -> Vec<usize> {
    let mut starts = vec![0];
    starts.extend(text.match_indices('\n').map(|(index, _)| index + 1));
    starts
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(line: &str) -> Vec<(Kind, &str)> {
        lex_line(line, &Carry::None)
            .0
            .into_iter()
            .map(|token| (token.kind, &line[token.start..token.end]))
            .collect()
    }

    #[test]
    fn keyword_list_is_sorted_for_binary_search() {
        let mut sorted = KEYWORDS.to_vec();
        sorted.sort_unstable();
        assert_eq!(sorted, KEYWORDS, "KEYWORDS must stay sorted");
    }

    #[test]
    fn a_plain_statement_lexes_into_its_parts() {
        assert_eq!(
            kinds("select a, 'x''y' from \"T\" where n >= 1.5e3 -- done"),
            vec![
                (Kind::Word, "select"),
                (Kind::Word, "a"),
                (Kind::Punct, ","),
                (Kind::String, "'x''y'"),
                (Kind::Word, "from"),
                (Kind::Quoted, "\"T\""),
                (Kind::Word, "where"),
                (Kind::Word, "n"),
                (Kind::Operator, ">="),
                (Kind::Number, "1.5e3"),
                (Kind::Comment, "-- done"),
            ]
        );
    }

    #[test]
    fn casts_params_and_prefixed_strings() {
        assert_eq!(
            kinds("$1::int || E'a\\'b'"),
            vec![
                (Kind::Param, "$1"),
                (Kind::Operator, "::"),
                (Kind::Word, "int"),
                (Kind::Operator, "||"),
                (Kind::String, "E'a\\'b'"),
            ]
        );
    }

    /// An operator run must stop at a comment, or `a=-- note` hides the note
    /// inside an operator.
    #[test]
    fn an_operator_does_not_swallow_a_comment() {
        assert_eq!(
            kinds("a=-- note"),
            vec![(Kind::Word, "a"), (Kind::Operator, "="), (Kind::Comment, "-- note")]
        );
    }

    #[test]
    fn a_block_comment_carries_across_lines_and_nests() {
        let (first, carry) = lex_line("select /* a /* b */", &Carry::None);
        assert_eq!(carry, Carry::Comment(1));
        assert_eq!(first.last().unwrap().kind, Kind::Comment);

        let (second, carry) = lex_line("still */ from t", &carry);
        assert_eq!(carry, Carry::None);
        assert_eq!(second[0], Token { kind: Kind::Comment, start: 0, end: 8 });
        assert_eq!(second[1].kind, Kind::Word);
    }

    #[test]
    fn strings_and_dollar_bodies_carry_across_lines() {
        let (_, carry) = lex_line("select 'one", &Carry::None);
        assert_eq!(carry, Carry::String { escapes: false });
        let (tokens, carry) = lex_line("two' as x", &carry);
        assert_eq!(carry, Carry::None);
        assert_eq!(tokens[0], Token { kind: Kind::String, start: 0, end: 4 });

        let (_, carry) = lex_line("do $body$ begin", &Carry::None);
        assert_eq!(carry, Carry::Dollar("body".into()));
        let (tokens, carry) = lex_line("select 1; end $body$;", &carry);
        assert_eq!(carry, Carry::None);
        assert_eq!(tokens[0].kind, Kind::String);
        assert_eq!(tokens.len(), 2, "the `;` inside the body is part of it: {tokens:?}");
    }

    #[test]
    fn multibyte_text_keeps_char_boundaries() {
        let line = "select 'привет', naïve from t";
        for token in lex_line(line, &Carry::None).0 {
            assert!(line.is_char_boundary(token.start) && line.is_char_boundary(token.end));
        }
    }

    #[test]
    fn the_buffer_remembers_where_an_unclosed_literal_opened() {
        let lexed = lex("select 1;\nselect 'oops\nmore\nlines");
        assert_eq!(lexed.open, Some((1, 7, Carry::String { escapes: false })));

        let lexed = lex("select 'closed'");
        assert_eq!(lexed.open, None);
    }

    #[test]
    fn the_statement_under_the_cursor() {
        let text = "select 1;\nselect 2\n  from t;\n-- trailing\n";
        assert_eq!(statement_at(text, 0, 3).as_deref(), Some("select 1"));
        assert_eq!(statement_at(text, 2, 4).as_deref(), Some("select 2\n  from t"));
        // Just after a semicolon: the statement that semicolon finished.
        assert_eq!(statement_at(text, 0, 9).as_deref(), Some("select 1"));
        // After the last one, in a comment: still the last real statement.
        assert_eq!(statement_at(text, 3, 5).as_deref(), Some("select 2\n  from t"));
    }

    #[test]
    fn a_semicolon_in_a_literal_does_not_split() {
        let text = "select ';' as a; select 2";
        assert_eq!(statement_at(text, 0, 2).as_deref(), Some("select ';' as a"));
        assert_eq!(statement_at(text, 0, 20).as_deref(), Some("select 2"));
    }

    #[test]
    fn an_empty_buffer_has_no_statement() {
        assert_eq!(statement_at("", 0, 0), None);
        assert_eq!(statement_at("-- only a note", 0, 3), None);
    }
}
