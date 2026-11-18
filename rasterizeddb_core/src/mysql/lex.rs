//! MySQL's lexical grammar: what a statement's bytes are, before any of it is
//! translated.
//!
//! What differs from PostgreSQL's, and is why this exists rather than the
//! engine's own scanner:
//!
//! - `` `name` `` quotes an identifier; `"…"` is a *string* unless
//!   `ANSI_QUOTES` is in `sql_mode`.
//! - Backslash escapes in strings (`\n`, `\'`, `\0`, …) unless
//!   `NO_BACKSLASH_ESCAPES`; `''` doubling works too.
//! - `#` starts a comment, and `--` only does when followed by whitespace.
//! - `/*! … */` is *executed*: its contents are statement text (mysqldump
//!   writes its `SET`s that way). `/*+ … */` is an optimizer hint, dropped.
//! - `x'…'`, `0x…`, `b'…'`, `0b…` literals; `@@name` system variables;
//!   `@name` user variables; `?` placeholders.
//!
//! Strings are kept as bytes: a client that interpolates binary data into a
//! statement (PHP's PDO, for one) sends bytes that are not UTF-8, and those
//! are a binary string, not an error.

/// What `sql_mode` changes about lexing and operators.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Mode {
    pub(crate) ansi_quotes: bool,
    pub(crate) pipes_as_concat: bool,
    pub(crate) no_backslash_escapes: bool,
}

impl Mode {
    pub(crate) fn from_sql_mode(sql_mode: &str) -> Self {
        let mut mode = Mode::default();
        for part in sql_mode.split(',') {
            match part.trim().to_ascii_uppercase().as_str() {
                "ANSI_QUOTES" => mode.ansi_quotes = true,
                "PIPES_AS_CONCAT" => mode.pipes_as_concat = true,
                "NO_BACKSLASH_ESCAPES" => mode.no_backslash_escapes = true,
                // ANSI is a combination mode that includes both.
                "ANSI" => {
                    mode.ansi_quotes = true;
                    mode.pipes_as_concat = true;
                }
                _ => {}
            }
        }
        mode
    }
}

pub(crate) use crate::sqlcompat::{LexError, Tok, Token, matching_paren, split_commas, split_statements};

/// Splits `sql` into tokens. Placeholders are numbered across the whole text.
pub(crate) fn lex(sql: &[u8], mode: Mode) -> Result<Vec<Token>, LexError> {
    let mut out: Vec<Token> = Vec::new();
    let mut i = 0usize;
    let n = sql.len();
    let mut space = false;
    let mut params = 0usize;
    // Inside `/*! … */`: its closing `*/` is skipped, not lexed.
    let mut exec_comment = 0usize;

    while i < n {
        let c = sql[i];
        // Whitespace.
        if c.is_ascii_whitespace() {
            space = true;
            i += 1;
            continue;
        }
        // Comments.
        if c == b'#' {
            while i < n && sql[i] != b'\n' {
                i += 1;
            }
            space = true;
            continue;
        }
        if c == b'-' && i + 1 < n && sql[i + 1] == b'-' && (i + 2 >= n || sql[i + 2].is_ascii_whitespace() || sql[i + 2] < 0x20) {
            while i < n && sql[i] != b'\n' {
                i += 1;
            }
            space = true;
            continue;
        }
        if c == b'/' && i + 1 < n && sql[i + 1] == b'*' {
            if i + 2 < n && sql[i + 2] == b'!' {
                // Executable comment: skip `/*!` and an optional version.
                i += 3;
                let mut digits = 0;
                while i < n && sql[i].is_ascii_digit() && digits < 6 {
                    i += 1;
                    digits += 1;
                }
                exec_comment += 1;
                space = true;
                continue;
            }
            // A plain comment or an optimizer hint: dropped.
            let start = i;
            i += 2;
            loop {
                if i + 1 >= n {
                    return Err(LexError(format!(
                        "unterminated comment starting at byte {start}"
                    )));
                }
                if sql[i] == b'*' && sql[i + 1] == b'/' {
                    i += 2;
                    break;
                }
                i += 1;
            }
            space = true;
            continue;
        }
        if exec_comment > 0 && c == b'*' && i + 1 < n && sql[i + 1] == b'/' {
            exec_comment -= 1;
            i += 2;
            space = true;
            continue;
        }

        let start = i;
        let tok = match c {
            b'\'' => {
                let (value, end) = string_body(sql, i, b'\'', mode)?;
                i = end;
                Tok::Str(value)
            }
            b'"' if !mode.ansi_quotes => {
                let (value, end) = string_body(sql, i, b'"', mode)?;
                i = end;
                Tok::Str(value)
            }
            b'"' => {
                let (value, end) = quoted_ident(sql, i, b'"')?;
                i = end;
                Tok::Quoted(value)
            }
            b'`' => {
                let (value, end) = quoted_ident(sql, i, b'`')?;
                i = end;
                Tok::Quoted(value)
            }
            b'?' => {
                i += 1;
                params += 1;
                Tok::Param(params - 1)
            }
            b'@' => {
                if i + 1 < n && sql[i + 1] == b'@' {
                    i += 2;
                    let mut name = read_word(sql, &mut i);
                    // `@@session.x`, `@@global.x`, `@@local.x`.
                    let lower = name.to_ascii_lowercase();
                    if (lower == "session" || lower == "global" || lower == "local" || lower == "persist")
                        && i < n
                        && sql[i] == b'.'
                    {
                        i += 1;
                        name = read_word(sql, &mut i);
                    }
                    if name.is_empty() {
                        return Err(LexError("a system variable needs a name after @@".into()));
                    }
                    Tok::SysVar(name.to_ascii_lowercase())
                } else {
                    i += 1;
                    let name = if i < n && (sql[i] == b'\'' || sql[i] == b'"' || sql[i] == b'`') {
                        let quote = sql[i];
                        let (value, end) = if quote == b'`' {
                            let (v, e) = quoted_ident(sql, i, quote)?;
                            (v.into_bytes(), e)
                        } else {
                            string_body(sql, i, quote, mode)?
                        };
                        i = end;
                        String::from_utf8_lossy(&value).into_owned()
                    } else {
                        read_word(sql, &mut i)
                    };
                    if name.is_empty() {
                        return Err(LexError("a user variable needs a name after @".into()));
                    }
                    Tok::UserVar(name.to_ascii_lowercase())
                }
            }
            b'0' if i + 1 < n && (sql[i + 1] == b'x') && i + 2 < n && sql[i + 2].is_ascii_hexdigit() => {
                i += 2;
                let s = i;
                while i < n && sql[i].is_ascii_hexdigit() {
                    i += 1;
                }
                if i < n && is_word_byte(sql[i]) {
                    // `0xg…` is an identifier in MySQL; too odd to support.
                    return Err(LexError("malformed hexadecimal literal".into()));
                }
                Tok::Hex(hex_bytes(&sql[s..i]))
            }
            b'0' if i + 1 < n && sql[i + 1] == b'b' && i + 2 < n && (sql[i + 2] == b'0' || sql[i + 2] == b'1') => {
                i += 2;
                let s = i;
                while i < n && (sql[i] == b'0' || sql[i] == b'1') {
                    i += 1;
                }
                Tok::Num(bits_value(&sql[s..i])?.to_string())
            }
            b'x' | b'X' if i + 1 < n && sql[i + 1] == b'\'' => {
                i += 2;
                let s = i;
                while i < n && sql[i] != b'\'' {
                    i += 1;
                }
                if i >= n {
                    return Err(LexError("unterminated hexadecimal literal".into()));
                }
                let digits = &sql[s..i];
                i += 1;
                if digits.len() % 2 != 0 || !digits.iter().all(|b| b.is_ascii_hexdigit()) {
                    return Err(LexError("malformed hexadecimal literal".into()));
                }
                Tok::Hex(hex_bytes(digits))
            }
            b'b' | b'B' if i + 1 < n && sql[i + 1] == b'\'' => {
                i += 2;
                let s = i;
                while i < n && sql[i] != b'\'' {
                    i += 1;
                }
                if i >= n {
                    return Err(LexError("unterminated bit literal".into()));
                }
                let digits = &sql[s..i];
                i += 1;
                Tok::Num(bits_value(digits)?.to_string())
            }
            b'n' | b'N' if i + 1 < n && sql[i + 1] == b'\'' => {
                // National string: the same as a string here.
                let (value, end) = string_body(sql, i + 1, b'\'', mode)?;
                i = end;
                Tok::Str(value)
            }
            c if c.is_ascii_digit() || (c == b'.' && i + 1 < n && sql[i + 1].is_ascii_digit() && !prev_is_operand(&out)) => {
                let s = i;
                while i < n && sql[i].is_ascii_digit() {
                    i += 1;
                }
                if i < n && sql[i] == b'.' {
                    i += 1;
                    while i < n && sql[i].is_ascii_digit() {
                        i += 1;
                    }
                }
                if i < n && (sql[i] == b'e' || sql[i] == b'E') {
                    let mut j = i + 1;
                    if j < n && (sql[j] == b'+' || sql[j] == b'-') {
                        j += 1;
                    }
                    if j < n && sql[j].is_ascii_digit() {
                        i = j;
                        while i < n && sql[i].is_ascii_digit() {
                            i += 1;
                        }
                    }
                }
                if i < n && is_word_byte(sql[i]) && !sql[s..i].contains(&b'.') {
                    // `1abc` is an identifier in MySQL (names may start with
                    // a digit).
                    while i < n && is_word_byte(sql[i]) {
                        i += 1;
                    }
                    Tok::Word(String::from_utf8_lossy(&sql[s..i]).into_owned())
                } else {
                    Tok::Num(String::from_utf8_lossy(&sql[s..i]).into_owned())
                }
            }
            c if is_word_byte(c) => {
                let word = read_word(sql, &mut i);
                // A charset introducer (`_utf8mb4'x'`, `_binary'…'`) before a
                // string: dropped, the string stands as it is.
                if word.starts_with('_') && is_introducer(&word) {
                    let mut j = i;
                    while j < n && sql[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    if j < n && (sql[j] == b'\'' || (sql[j] == b'"' && !mode.ansi_quotes) || ((sql[j] == b'x' || sql[j] == b'X') && j + 1 < n && sql[j + 1] == b'\'')) {
                        i = j;
                        space = true;
                        continue;
                    }
                }
                Tok::Word(word)
            }
            _ => {
                let op = operator_at(sql, i);
                i += op.len();
                Tok::Op(op.to_string())
            }
        };
        out.push(Token {
            tok,
            space,
            span: (start, i),
        });
        space = false;
    }
    if exec_comment > 0 {
        return Err(LexError("unterminated /*! comment".into()));
    }
    Ok(out)
}

fn is_introducer(word: &str) -> bool {
    matches!(
        word.to_ascii_lowercase().as_str(),
        "_utf8mb4" | "_utf8mb3" | "_utf8" | "_latin1" | "_binary" | "_ascii" | "_ucs2" | "_utf16" | "_utf32"
    )
}

fn prev_is_operand(out: &[Token]) -> bool {
    match out.last().map(|t| &t.tok) {
        Some(Tok::Word(_)) | Some(Tok::Quoted(_)) => true,
        Some(Tok::Op(o)) => o == ")",
        _ => false,
    }
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
}

fn read_word(sql: &[u8], i: &mut usize) -> String {
    let s = *i;
    while *i < sql.len() && is_word_byte(sql[*i]) {
        *i += 1;
    }
    String::from_utf8_lossy(&sql[s..*i]).into_owned()
}

fn hex_bytes(digits: &[u8]) -> Vec<u8> {
    let digit = |b: u8| match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        _ => b - b'A' + 10,
    };
    // An odd count has an implied leading zero, as MySQL reads `0xabc`.
    let mut padded;
    let digits = if digits.len() % 2 == 1 {
        padded = vec![b'0'];
        padded.extend_from_slice(digits);
        &padded[..]
    } else {
        digits
    };
    digits
        .chunks(2)
        .map(|pair| digit(pair[0]) << 4 | digit(pair[1]))
        .collect()
}

fn bits_value(digits: &[u8]) -> Result<u64, LexError> {
    if digits.len() > 64 || !digits.iter().all(|b| *b == b'0' || *b == b'1') {
        return Err(LexError("malformed bit literal".into()));
    }
    Ok(digits.iter().fold(0u64, |acc, b| acc << 1 | (b - b'0') as u64))
}

/// A string literal starting at the opening quote `sql[start]`: its value
/// and the index after the closing quote. Adjacent literals (`'a' 'b'`) are
/// concatenated, as MySQL does.
fn string_body(sql: &[u8], start: usize, quote: u8, mode: Mode) -> Result<(Vec<u8>, usize), LexError> {
    let mut value = Vec::new();
    let mut i = start + 1;
    let n = sql.len();
    loop {
        if i >= n {
            return Err(LexError("unterminated string literal".into()));
        }
        let c = sql[i];
        if c == quote {
            if i + 1 < n && sql[i + 1] == quote {
                value.push(quote);
                i += 2;
                continue;
            }
            i += 1;
            break;
        }
        if c == b'\\' && !mode.no_backslash_escapes && i + 1 < n {
            let e = sql[i + 1];
            match e {
                b'0' => value.push(0),
                b'n' => value.push(b'\n'),
                b'r' => value.push(b'\r'),
                b't' => value.push(b'\t'),
                b'b' => value.push(8),
                b'Z' => value.push(26),
                // `\%` and `\_` keep their backslash: they are LIKE escapes.
                b'%' | b'_' => {
                    value.push(b'\\');
                    value.push(e);
                }
                other => value.push(other),
            }
            i += 2;
            continue;
        }
        value.push(c);
        i += 1;
    }
    // `'a' 'b'` is one literal.
    let mut j = i;
    while j < n && sql[j].is_ascii_whitespace() {
        j += 1;
    }
    if j > i && j < n && sql[j] == quote && quote == b'\'' {
        let (more, end) = string_body(sql, j, quote, mode)?;
        value.extend_from_slice(&more);
        return Ok((value, end));
    }
    Ok((value, i))
}

fn quoted_ident(sql: &[u8], start: usize, quote: u8) -> Result<(String, usize), LexError> {
    let mut value = Vec::new();
    let mut i = start + 1;
    loop {
        if i >= sql.len() {
            return Err(LexError("unterminated quoted identifier".into()));
        }
        if sql[i] == quote {
            if i + 1 < sql.len() && sql[i + 1] == quote {
                value.push(quote);
                i += 2;
                continue;
            }
            i += 1;
            break;
        }
        value.push(sql[i]);
        i += 1;
    }
    match String::from_utf8(value) {
        Ok(name) => Ok((name, i)),
        Err(_) => Err(LexError("an identifier is not valid UTF-8".into())),
    }
}

fn operator_at(sql: &[u8], i: usize) -> &'static str {
    const OPS: [&str; 25] = [
        "<=>", "->>", "<=", ">=", "<>", "!=", "||", "&&", "<<", ">>", ":=", "->", "(", ")", ",", ";", ".", "=",
        "<", ">", "+", "-", "*", "/", "%",
    ];
    for op in OPS {
        if sql[i..].starts_with(op.as_bytes()) {
            return op;
        }
    }
    match sql[i] {
        b'!' => "!",
        b'~' => "~",
        b'^' => "^",
        b'&' => "&",
        b'|' => "|",
        b':' => ":",
        b'{' => "{",
        b'}' => "}",
        b'[' => "[",
        b']' => "]",
        _ => "\u{FFFD}",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(sql: &str) -> Vec<Tok> {
        lex(sql.as_bytes(), Mode::default()).unwrap().into_iter().map(|t| t.tok).collect()
    }

    #[test]
    fn strings_escapes_and_quotes() {
        assert_eq!(toks(r"'it\'s'"), vec![Tok::Str(b"it's".to_vec())]);
        assert_eq!(toks("'it''s'"), vec![Tok::Str(b"it's".to_vec())]);
        assert_eq!(toks(r#""a\nb""#), vec![Tok::Str(b"a\nb".to_vec())]);
        assert_eq!(toks("'a' 'b'"), vec![Tok::Str(b"ab".to_vec())]);
        assert_eq!(toks(r"'\0'"), vec![Tok::Str(vec![0])]);
        assert_eq!(toks(r"'50\%'"), vec![Tok::Str(br"50\%".to_vec())]);
        assert_eq!(toks("`my``col`"), vec![Tok::Quoted("my`col".into())]);
        let ansi = Mode::from_sql_mode("ANSI_QUOTES");
        assert_eq!(
            lex(br#""col""#, ansi).unwrap()[0].tok,
            Tok::Quoted("col".into())
        );
        let raw = Mode::from_sql_mode("NO_BACKSLASH_ESCAPES");
        assert_eq!(lex(br"'a\n'", raw).unwrap()[0].tok, Tok::Str(br"a\n".to_vec()));
    }

    #[test]
    fn comments_and_executable_comments() {
        assert_eq!(
            toks("SELECT 1 # c\n, 2 -- d\n /* e */"),
            vec![Tok::Word("SELECT".into()), Tok::Num("1".into()), Tok::Op(",".into()), Tok::Num("2".into())]
        );
        assert_eq!(
            toks("/*!40101 SET x = 1 */"),
            vec![Tok::Word("SET".into()), Tok::Word("x".into()), Tok::Op("=".into()), Tok::Num("1".into())]
        );
        // `--x` is minus minus x, not a comment.
        assert_eq!(toks("1--1"), vec![Tok::Num("1".into()), Tok::Op("-".into()), Tok::Op("-".into()), Tok::Num("1".into())]);
    }

    #[test]
    fn literals_and_variables() {
        assert_eq!(toks("x'0aFF'"), vec![Tok::Hex(vec![0x0a, 0xff])]);
        assert_eq!(toks("0xABC"), vec![Tok::Hex(vec![0x0a, 0xbc])]);
        assert_eq!(toks("b'101'"), vec![Tok::Num("5".into())]);
        assert_eq!(toks("@@session.SQL_MODE"), vec![Tok::SysVar("sql_mode".into())]);
        assert_eq!(toks("@x"), vec![Tok::UserVar("x".into())]);
        assert_eq!(toks("? ?"), vec![Tok::Param(0), Tok::Param(1)]);
        assert_eq!(toks("_utf8mb4'x'"), vec![Tok::Str(b"x".to_vec())]);
        assert_eq!(toks("1.5e3 .5"), vec![Tok::Num("1.5e3".into()), Tok::Num(".5".into())]);
        assert_eq!(toks("t.c"), vec![Tok::Word("t".into()), Tok::Op(".".into()), Tok::Word("c".into())]);
        assert_eq!(toks("a<=>b"), vec![Tok::Word("a".into()), Tok::Op("<=>".into()), Tok::Word("b".into())]);
    }

    #[test]
    fn statements_split_at_semicolons() {
        let parts = split_statements(lex(b"SELECT 1; ; SELECT ';'", Mode::default()).unwrap());
        assert_eq!(parts.len(), 2);
    }
}
