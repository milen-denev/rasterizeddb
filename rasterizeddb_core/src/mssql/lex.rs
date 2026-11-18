//! T-SQL's lexical grammar.
//!
//! `[name]` and `"name"` quote identifiers (QUOTED_IDENTIFIER is on, as every
//! modern client sets it); strings are `'…'` with `''` and no backslash
//! escapes, `N'…'` alike; `--` comments need no space after them, and block
//! comments nest; `@name` is a local variable or parameter, `@@name` a
//! system function; `#t` and `##t` are temporary tables.

pub(crate) use crate::sqlcompat::{LexError, Tok, Token, matching_paren, split_commas};

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b == b'@' || b == b'#' || b >= 0x80
}

pub(crate) fn lex(sql: &str) -> Result<Vec<Token>, LexError> {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut space = false;
    let mut params = 0usize;
    while i < n {
        let c = b[i];
        if c.is_ascii_whitespace() {
            space = true;
            i += 1;
            continue;
        }
        if c == b'-' && i + 1 < n && b[i + 1] == b'-' {
            while i < n && b[i] != b'\n' {
                i += 1;
            }
            space = true;
            continue;
        }
        if c == b'/' && i + 1 < n && b[i + 1] == b'*' {
            let mut depth = 0usize;
            loop {
                if i + 1 >= n {
                    return Err(LexError("unterminated comment".into()));
                }
                if b[i] == b'/' && b[i + 1] == b'*' {
                    depth += 1;
                    i += 2;
                } else if b[i] == b'*' && b[i + 1] == b'/' {
                    depth -= 1;
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            space = true;
            continue;
        }
        let start = i;
        let tok = match c {
            b'\'' => {
                let (v, e) = quoted(b, i, b'\'', b'\'')?;
                i = e;
                Tok::Str(v.into_bytes())
            }
            b'N' | b'n' if i + 1 < n && b[i + 1] == b'\'' => {
                let (v, e) = quoted(b, i + 1, b'\'', b'\'')?;
                i = e;
                Tok::Str(v.into_bytes())
            }
            b'[' => {
                let (v, e) = quoted(b, i, b'[', b']')?;
                i = e;
                Tok::Quoted(v)
            }
            b'"' => {
                let (v, e) = quoted(b, i, b'"', b'"')?;
                i = e;
                Tok::Quoted(v)
            }
            b'0' if i + 1 < n && (b[i + 1] == b'x' || b[i + 1] == b'X') => {
                i += 2;
                let s = i;
                while i < n && b[i].is_ascii_hexdigit() {
                    i += 1;
                }
                let digits = &b[s..i];
                let mut padded = Vec::new();
                if digits.len() % 2 == 1 {
                    padded.push(b'0');
                }
                padded.extend_from_slice(digits);
                let bytes = padded
                    .chunks(2)
                    .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap_or("00"), 16).unwrap_or(0))
                    .collect();
                Tok::Hex(bytes)
            }
            b'?' => {
                i += 1;
                params += 1;
                Tok::Param(params - 1)
            }
            c if c.is_ascii_digit() || (c == b'.' && i + 1 < n && b[i + 1].is_ascii_digit()) => {
                let s = i;
                while i < n && b[i].is_ascii_digit() {
                    i += 1;
                }
                if i < n && b[i] == b'.' {
                    i += 1;
                    while i < n && b[i].is_ascii_digit() {
                        i += 1;
                    }
                }
                if i < n && (b[i] == b'e' || b[i] == b'E') {
                    let mut j = i + 1;
                    if j < n && (b[j] == b'+' || b[j] == b'-') {
                        j += 1;
                    }
                    if j < n && b[j].is_ascii_digit() {
                        i = j;
                        while i < n && b[i].is_ascii_digit() {
                            i += 1;
                        }
                    }
                }
                Tok::Num(sql[s..i].to_string())
            }
            c if is_word_byte(c) => {
                let s = i;
                while i < n && is_word_byte(b[i]) {
                    i += 1;
                }
                let word = &sql[s..i];
                if let Some(name) = word.strip_prefix("@@") {
                    Tok::SysVar(name.to_ascii_lowercase())
                } else if let Some(name) = word.strip_prefix('@') {
                    Tok::UserVar(name.to_ascii_lowercase())
                } else if word.starts_with('#') {
                    Tok::Quoted(word.to_string())
                } else {
                    Tok::Word(word.to_string())
                }
            }
            _ => {
                const OPS: [&str; 22] = [
                    "<>", "!=", "<=", ">=", "!<", "!>", "+=", "-=", "*=", "/=", "%=", "&=", "|=", "^=", "::", "(", ")",
                    ",", ";", ".", "=", "<",
                ];
                let op = OPS
                    .iter()
                    .find(|op| sql[i..].starts_with(*op))
                    .copied()
                    .unwrap_or_else(|| match c {
                        b'>' => ">",
                        b'+' => "+",
                        b'-' => "-",
                        b'*' => "*",
                        b'/' => "/",
                        b'%' => "%",
                        b'&' => "&",
                        b'|' => "|",
                        b'^' => "^",
                        b'~' => "~",
                        b'!' => "!",
                        b':' => ":",
                        b'{' => "{",
                        b'}' => "}",
                        _ => "\u{FFFD}",
                    });
                i += op.len().max(1);
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
    Ok(out)
}

/// A quoted run starting at the opening `open`; `close` doubled escapes it.
fn quoted(b: &[u8], start: usize, open: u8, close: u8) -> Result<(String, usize), LexError> {
    let _ = open;
    let mut value = Vec::new();
    let mut i = start + 1;
    loop {
        if i >= b.len() {
            return Err(LexError(if close == b'\'' {
                "Unclosed quotation mark after the character string".into()
            } else {
                "unterminated quoted identifier".into()
            }));
        }
        if b[i] == close {
            if i + 1 < b.len() && b[i + 1] == close {
                value.push(close);
                i += 2;
                continue;
            }
            i += 1;
            break;
        }
        value.push(b[i]);
        i += 1;
    }
    Ok((String::from_utf8_lossy(&value).into_owned(), i))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(sql: &str) -> Vec<Tok> {
        lex(sql).unwrap().into_iter().map(|t| t.tok).collect()
    }

    #[test]
    fn tsql_tokens() {
        assert_eq!(toks("[my]]col]"), vec![Tok::Quoted("my]col".into())]);
        assert_eq!(toks("N'it''s'"), vec![Tok::Str(b"it's".to_vec())]);
        assert_eq!(toks(r"'a\n'"), vec![Tok::Str(br"a\n".to_vec())]);
        assert_eq!(toks("@p1 @@IDENTITY #tmp"), vec![Tok::UserVar("p1".into()), Tok::SysVar("identity".into()), Tok::Quoted("#tmp".into())]);
        assert_eq!(toks("/* a /* b */ c */ 1--x"), vec![Tok::Num("1".into())]);
        assert_eq!(toks("0x0aFF"), vec![Tok::Hex(vec![0x0a, 0xff])]);
    }
}
