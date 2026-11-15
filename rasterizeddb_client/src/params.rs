//! `$n` placeholders, rendered into literals the server's lexer reads back as
//! exactly the value given.

use std::fmt::Write as _;

use crate::{Error, Result};

/// One value for a `$n` placeholder.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Param<'a> {
    Null,
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    /// Rendered as a single-quoted literal. Must not contain a NUL, which no
    /// text value can hold.
    Text(&'a str),
    /// Rendered as a `'\x…'` hex literal, which the server decodes as
    /// PostgreSQL's `byteain` does.
    Bytes(&'a [u8]),
}

impl<'a> From<&'a str> for Param<'a> {
    fn from(s: &'a str) -> Self {
        Param::Text(s)
    }
}

impl<'a> From<&'a [u8]> for Param<'a> {
    fn from(b: &'a [u8]) -> Self {
        Param::Bytes(b)
    }
}

impl From<i64> for Param<'_> {
    fn from(v: i64) -> Self {
        Param::I64(v)
    }
}

impl From<i32> for Param<'_> {
    fn from(v: i32) -> Self {
        Param::I32(v)
    }
}

impl From<i16> for Param<'_> {
    fn from(v: i16) -> Self {
        Param::I16(v)
    }
}

impl From<bool> for Param<'_> {
    fn from(v: bool) -> Self {
        Param::Bool(v)
    }
}

impl<'a, T: Into<Param<'a>>> From<Option<T>> for Param<'a> {
    fn from(v: Option<T>) -> Self {
        v.map_or(Param::Null, Into::into)
    }
}

/// `s` as a single-quoted string literal: `'` doubled, everything else as it
/// is. A backslash is an ordinary byte in such a literal (only `E'…'` reads
/// escapes), so nothing else needs escaping.
///
/// Refuses a NUL: a text value cannot hold one, and the server would store
/// something other than what was given.
pub fn quote_text(s: &str) -> Result<String> {
    let mut out = String::with_capacity(s.len() + 2);
    push_text(&mut out, s)?;
    Ok(out)
}

/// `bytes` as a `bytea` hex literal, `'\x…'` with lower-case digits.
pub fn quote_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2 + 4);
    push_bytes(&mut out, bytes);
    out
}

fn push_text(out: &mut String, s: &str) -> Result<()> {
    if s.contains('\0') {
        return Err(Error::Param(
            "a text value cannot contain a NUL character".to_string(),
        ));
    }
    out.reserve(s.len() + 2);
    out.push('\'');
    let mut start = 0;
    for (at, _) in s.match_indices('\'') {
        out.push_str(&s[start..=at]);
        out.push('\'');
        start = at + 1;
    }
    out.push_str(&s[start..]);
    out.push('\'');
    Ok(())
}

fn push_bytes(out: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.reserve(bytes.len() * 2 + 4);
    out.push_str("'\\x");
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out.push('\'');
}

/// A number, parenthesised when negative. Spliced after a `-` (`x-$1`), a bare
/// `-3` would make `x--3`, and `--` starts a comment that swallows the rest of
/// the line, the `WHERE` clause included.
fn push_int(out: &mut String, v: i64) {
    if v < 0 {
        let _ = write!(out, "({v})");
    } else {
        let _ = write!(out, "{v}");
    }
}

fn push_param(out: &mut String, param: &Param<'_>) -> Result<()> {
    match *param {
        Param::Null => out.push_str("NULL"),
        Param::Bool(true) => out.push_str("true"),
        Param::Bool(false) => out.push_str("false"),
        Param::I16(v) => push_int(out, v.into()),
        Param::I32(v) => push_int(out, v.into()),
        Param::I64(v) => push_int(out, v),
        Param::Text(s) => push_text(out, s)?,
        Param::Bytes(b) => push_bytes(out, b),
    }
    Ok(())
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
}

/// `sql` with every `$n` placeholder replaced by the literal for `params[n-1]`.
///
/// Placeholders are recognised where the server's lexer would see a token:
/// not inside a single-quoted string (`E'…'` with its backslash escapes
/// included), a double-quoted identifier, a dollar-quoted string, or a `--` or
/// `/* */` comment, and not as the tail of an identifier (`a$1`). A
/// placeholder numbered past the end of `params`, or `$0`, is an error; a
/// parameter no placeholder uses is not.
pub fn render(sql: &str, params: &[Param<'_>]) -> Result<String> {
    if params.is_empty() {
        // Still scanned: a `$1` with nothing to fill it is a mistake to report,
        // not a statement to send.
        if let Some(n) = first_placeholder(sql) {
            return Err(Error::Param(format!(
                "${n} has no value: the statement was given no parameters"
            )));
        }
        return Ok(sql.to_string());
    }

    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + params.len() * 8);
    let mut copied = 0usize;
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => i = skip_quoted(bytes, i, b'\'', escape_string_at(bytes, i)),
            b'"' => i = skip_quoted(bytes, i, b'"', false),
            b'-' if bytes.get(i + 1) == Some(&b'-') => i = skip_line_comment(bytes, i),
            b'/' if bytes.get(i + 1) == Some(&b'*') => i = skip_block_comment(bytes, i),
            b'$' if i == 0 || !is_ident_byte(bytes[i - 1]) => {
                let digits = bytes[i + 1..]
                    .iter()
                    .take_while(|b| b.is_ascii_digit())
                    .count();
                if digits > 0 {
                    let text = &sql[i + 1..i + 1 + digits];
                    let n: usize = text
                        .parse()
                        .map_err(|_| Error::Param(format!("${text} is not a usable number")))?;
                    if n == 0 || n > params.len() {
                        return Err(Error::Param(format!(
                            "${n} has no value: {} parameter(s) were given",
                            params.len()
                        )));
                    }
                    out.push_str(&sql[copied..i]);
                    push_param(&mut out, &params[n - 1])?;
                    i += 1 + digits;
                    copied = i;
                } else if let Some(end) = dollar_quote_end(bytes, i) {
                    i = end;
                } else {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }
    out.push_str(&sql[copied..]);
    Ok(out)
}

/// The number of the first placeholder `render` would replace, if any.
fn first_placeholder(sql: &str) -> Option<String> {
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => i = skip_quoted(bytes, i, b'\'', escape_string_at(bytes, i)),
            b'"' => i = skip_quoted(bytes, i, b'"', false),
            b'-' if bytes.get(i + 1) == Some(&b'-') => i = skip_line_comment(bytes, i),
            b'/' if bytes.get(i + 1) == Some(&b'*') => i = skip_block_comment(bytes, i),
            b'$' if i == 0 || !is_ident_byte(bytes[i - 1]) => {
                let digits = bytes[i + 1..]
                    .iter()
                    .take_while(|b| b.is_ascii_digit())
                    .count();
                if digits > 0 {
                    return Some(sql[i + 1..i + 1 + digits].to_string());
                }
                i = dollar_quote_end(bytes, i).unwrap_or(i + 1);
            }
            _ => i += 1,
        }
    }
    None
}

/// Whether the quote at `quote` opens an `E'…'` string, whose backslashes
/// escape the next character: an `e` or `E` right before it that is not
/// itself the end of a longer identifier.
fn escape_string_at(bytes: &[u8], quote: usize) -> bool {
    quote >= 1
        && matches!(bytes[quote - 1], b'e' | b'E')
        && (quote == 1 || !is_ident_byte(bytes[quote - 2]))
}

/// The index just past the closing `delim` of the quoted run starting at
/// `start`. A doubled delimiter is an escaped one. An unterminated run ends at
/// the end of the text: nothing after an unclosed quote is a placeholder.
fn skip_quoted(bytes: &[u8], start: usize, delim: u8, backslash_escapes: bool) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        let b = bytes[i];
        if backslash_escapes && b == b'\\' {
            i += 2;
            continue;
        }
        if b == delim {
            if bytes.get(i + 1) == Some(&delim) {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

fn skip_line_comment(bytes: &[u8], start: usize) -> usize {
    match bytes[start..].iter().position(|&b| b == b'\n') {
        Some(n) => start + n + 1,
        None => bytes.len(),
    }
}

/// Block comments nest, as PostgreSQL's do.
fn skip_block_comment(bytes: &[u8], start: usize) -> usize {
    let mut depth = 0usize;
    let mut i = start;
    while i + 1 < bytes.len() {
        if bytes[i] == b'/' && bytes[i + 1] == b'*' {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'*' && bytes[i + 1] == b'/' {
            depth -= 1;
            i += 2;
            if depth == 0 {
                return i;
            }
        } else {
            i += 1;
        }
    }
    bytes.len()
}

/// The index past a dollar-quoted string (`$$…$$`, `$tag$…$tag$`) opening at
/// `start`, or `None` if no tag opens there.
fn dollar_quote_end(bytes: &[u8], start: usize) -> Option<usize> {
    let rest = &bytes[start + 1..];
    let tag_len = rest.iter().position(|&b| b == b'$')?;
    let tag = &rest[..tag_len];
    let valid_tag = tag
        .first()
        .is_none_or(|b| b.is_ascii_alphabetic() || *b == b'_' || *b >= 0x80)
        && tag
            .iter()
            .all(|&b| b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80);
    if !valid_tag {
        return None;
    }
    let delimiter = &bytes[start..start + tag_len + 2];
    let body = start + tag_len + 2;
    let close = bytes[body..]
        .windows(delimiter.len())
        .position(|w| w == delimiter);
    Some(match close {
        Some(n) => body + n + delimiter.len(),
        None => bytes.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(sql: &str, params: &[Param<'_>]) -> String {
        render(sql, params).unwrap()
    }

    #[test]
    fn values_become_literals() {
        assert_eq!(
            r(
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[
                    Param::Null,
                    Param::Bool(true),
                    Param::I16(5),
                    Param::I32(-6),
                    Param::I64(i64::MIN),
                    Param::Text("it's"),
                    Param::Bytes(&[0x00, 0xff, 0x10]),
                ]
            ),
            "VALUES (NULL, true, 5, (-6), (-9223372036854775808), 'it''s', '\\x00ff10')"
        );
    }

    #[test]
    fn a_negative_after_a_minus_is_not_a_comment() {
        let sql = r("SELECT x-$1 FROM t WHERE k = 1", &[Param::I64(-3)]);
        assert_eq!(sql, "SELECT x-(-3) FROM t WHERE k = 1");
        assert!(!sql.contains("--"));
    }

    #[test]
    fn backslashes_and_comment_starters_stay_inside_the_literal() {
        assert_eq!(
            r(
                "SELECT $1",
                &[Param::Text("a\\b -- c /* d */ '; DROP TABLE t; --")]
            ),
            "SELECT 'a\\b -- c /* d */ ''; DROP TABLE t; --'"
        );
    }

    #[test]
    fn placeholders_in_strings_identifiers_and_comments_are_left_alone() {
        let sql =
            "SELECT '$1', \"$1\", E'\\'$1', $$ $1 $$, $q$ $1 $q$ -- $1\n /* $1 /* $1 */ $1 */ $1";
        assert_eq!(
            r(sql, &[Param::I64(9)]),
            "SELECT '$1', \"$1\", E'\\'$1', $$ $1 $$, $q$ $1 $q$ -- $1\n /* $1 /* $1 */ $1 */ 9"
        );
    }

    #[test]
    fn a_doubled_quote_does_not_end_the_string() {
        assert_eq!(
            r("SELECT 'a''$1', $1", &[Param::I64(1)]),
            "SELECT 'a''$1', 1"
        );
    }

    #[test]
    fn an_identifier_tail_is_not_a_placeholder() {
        assert_eq!(r("SELECT a$1, $1", &[Param::I64(2)]), "SELECT a$1, 2");
    }

    #[test]
    fn multi_digit_placeholders_are_one_number() {
        let params: Vec<Param<'_>> = (1..=12).map(|n| Param::I64(n * 100)).collect();
        assert_eq!(r("SELECT $1, $12, $10", &params), "SELECT 100, 1200, 1000");
    }

    #[test]
    fn a_missing_value_is_an_error() {
        assert!(render("SELECT $2", &[Param::I64(1)]).is_err());
        assert!(render("SELECT $0", &[Param::I64(1)]).is_err());
        assert!(render("SELECT $1", &[]).is_err());
        assert_eq!(render("SELECT '$1'", &[]).unwrap(), "SELECT '$1'");
    }

    #[test]
    fn a_nul_is_refused() {
        assert!(render("SELECT $1", &[Param::Text("a\0b")]).is_err());
        assert!(quote_text("a\0").is_err());
    }

    #[test]
    fn unicode_passes_through() {
        assert_eq!(r("SELECT $1", &[Param::Text("ж€😀'")]), "SELECT 'ж€😀'''");
        assert_eq!(r("SELECT 'ж', $1", &[Param::I64(1)]), "SELECT 'ж', 1");
    }

    #[test]
    fn an_unterminated_quote_hides_the_rest() {
        assert_eq!(
            r("SELECT $1, 'oops $1", &[Param::I64(1)]),
            "SELECT 1, 'oops $1"
        );
    }
}
