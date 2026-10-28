//! One quote-aware scanner for SQL text, and the normalisation that lets every
//! other scanner in the engine get away with knowing only standard strings.
//!
//! PostgreSQL has four ways to write a quoted run:
//!
//! * `'…'` — a standard string; `''` stands for one quote and a backslash is
//!   an ordinary character (`standard_conforming_strings` is on);
//! * `E'…'` — an escape string; a backslash escapes the character after it,
//!   so `\'` does *not* end the literal;
//! * `$tag$…$tag$` — a dollar-quoted string, inside which nothing is special;
//! * `"…"` — a quoted identifier; `""` stands for one quote.
//!
//! The engine has dozens of text scanners — clause splitters, keyword finders,
//! the tuple splitter, the comment stripper, the statement splitter, the
//! redaction and statistics normalisers — and nearly all of them know only the
//! first and the last. An escape string with an escaped quote inside therefore
//! ended early for all of them: `WHERE c LIKE E'it\'s%' ORDER BY id` read as a
//! literal `'it\'`, then the bare word `s%`, then a quote that swallowed the
//! `ORDER BY` — and the error named a column called `ORDER`.
//!
//! Teaching each scanner separately would leave the next one written to get it
//! wrong again. Instead [`normalize_string_literals`] rewrites escape strings
//! and dollar-quoted strings into the standard form they mean, once, where
//! statement text enters the engine. After that the only quoting any scanner
//! can meet is the one they all already handle. [`skip_quoted`] is the
//! scanner for code that sees text *before* that point, or that wants to be
//! right on its own.

use std::borrow::Cow;

/// A byte that continues an identifier. A quote or `$` right after one belongs
/// to (or follows) a name rather than opening something: `ELSE'x'` is the
/// keyword `ELSE` then a standard string, not an escape string.
#[inline]
pub fn is_identifier_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

/// True when the `'` at `quote` opens an escape string: `E'…'` / `e'…'`, with
/// the `E` a word of its own rather than the end of an identifier.
#[inline]
pub fn is_escape_string_quote(bytes: &[u8], quote: usize) -> bool {
    quote >= 1
        && matches!(bytes[quote - 1], b'E' | b'e')
        && (quote < 2 || !(is_identifier_byte(bytes[quote - 2]) || bytes[quote - 2] == b'$'))
}

/// Index just past the string literal opening at `start` (or the end of the
/// text when it is unterminated). In an escape string a backslash escapes
/// whatever byte follows it.
pub fn string_literal_end(bytes: &[u8], start: usize, escapes: bool) -> usize {
    let mut i = start + 1;
    loop {
        let found = if escapes {
            memchr::memchr2(b'\'', b'\\', &bytes[i..])
        } else {
            memchr::memchr(b'\'', &bytes[i..])
        };
        let Some(p) = found else {
            return bytes.len();
        };
        i += p;
        if bytes[i] == b'\\' {
            i = (i + 2).min(bytes.len());
            continue;
        }
        // A quote: doubled, it is an escaped quote and the literal goes on.
        if bytes.get(i + 1) == Some(&b'\'') {
            i += 2;
            continue;
        }
        return i + 1;
    }
}

/// Index just past the `quote`-delimited run opening at `start`, where a
/// doubled quote stands for itself (or the end of the text, unterminated).
pub fn quoted_end(bytes: &[u8], start: usize, quote: u8) -> usize {
    let mut i = start + 1;
    while let Some(p) = memchr::memchr(quote, &bytes[i..]) {
        i += p;
        if bytes.get(i + 1) == Some(&quote) {
            i += 2;
            continue;
        }
        return i + 1;
    }
    bytes.len()
}

/// If a dollar quote opens at `start`, the length of its tag (`$$` is 2,
/// `$tag$` is 5). `None` when this `$` opens nothing — a `$1` placeholder
/// among them, since a tag cannot begin with a digit.
pub fn dollar_tag_len(bytes: &[u8], start: usize) -> Option<usize> {
    let mut j = start + 1;
    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_' || bytes[j] >= 0x80) {
        if j == start + 1 && bytes[j].is_ascii_digit() {
            return None;
        }
        j += 1;
    }
    (j < bytes.len() && bytes[j] == b'$').then(|| j + 1 - start)
}

/// If a dollar-quoted string opens at `start`, the index just past its closing
/// tag — the end of the text when it is unterminated. `None` when this `$` is
/// not a dollar quote.
pub fn dollar_quote_end(bytes: &[u8], start: usize) -> Option<usize> {
    let tag_len = dollar_tag_len(bytes, start)?;
    let tag = &bytes[start..start + tag_len];
    let body = start + tag_len;
    Some(
        memchr::memmem::find(&bytes[body..], tag)
            .map_or(bytes.len(), |p| body + p + tag_len),
    )
}

/// Index just past the (possibly nested) block comment opening at `start`.
pub fn block_comment_end(bytes: &[u8], start: usize) -> usize {
    let mut depth = 1usize;
    let mut i = start + 2;
    while i < bytes.len() && depth > 0 {
        if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
            depth -= 1;
            i += 2;
        } else {
            i += 1;
        }
    }
    i
}

/// If a quoted run — string, escape string, dollar-quoted string or quoted
/// identifier — opens at `i`, the index just past it. Nothing inside one is
/// syntax.
#[inline]
pub fn skip_quoted(bytes: &[u8], i: usize) -> Option<usize> {
    match bytes[i] {
        b'\'' => Some(string_literal_end(bytes, i, is_escape_string_quote(bytes, i))),
        b'"' => Some(quoted_end(bytes, i, b'"')),
        b'$' if i == 0 || !is_identifier_byte(bytes[i - 1]) => dollar_quote_end(bytes, i),
        _ => None,
    }
}

/// The first quoted identifier in `sql` holding a doubled quote (`"a""b"`,
/// the name `a"b`), as written; `None` when there is none. Strings, escape
/// strings, dollar quotes and comments are skipped.
///
/// The engine's identifier readers each strip a name's quotes their own way,
/// and none reads `""` as a quote: `CREATE TABLE "t"" x ""y" (…)` created a
/// table named `t`, and such a column failed with an unrelated error. Until
/// they share one reader, a statement naming such an identifier is refused
/// where it enters, before anything is created under a different name.
pub fn identifier_with_doubled_quote(sql: &str) -> Option<&str> {
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = skip_comment(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        if bytes[i] == b'"' {
            let end = quoted_end(bytes, i, b'"');
            // Between the opening quote and the closing one.
            let inner = &bytes[i + 1..end.saturating_sub(1).max(i + 1)];
            if memchr::memmem::find(inner, b"\"\"").is_some() {
                return Some(&sql[i..end]);
            }
            i = end;
            continue;
        }
        if let Some(end) = skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        i += 1;
    }
    None
}

/// Whether `needle` occurs anywhere in `text`, ignoring ASCII case, quotes
/// and comments included: a scanner's cheap precondition, so one that can
/// only match where these bytes are can skip a statement that has none.
/// `needle` must not be empty.
pub fn contains_ignore_ascii_case(text: &str, needle: &[u8]) -> bool {
    let b = text.as_bytes();
    let first = needle[0];
    memchr::memchr2_iter(first.to_ascii_uppercase(), first.to_ascii_lowercase(), b)
        .any(|i| b.len() - i >= needle.len() && b[i..i + needle.len()].eq_ignore_ascii_case(needle))
}

/// If a comment opens at `i`, the index just past it (a line comment ends
/// before its newline).
#[inline]
pub fn skip_comment(bytes: &[u8], i: usize) -> Option<usize> {
    match (bytes[i], bytes.get(i + 1)) {
        (b'-', Some(b'-')) => {
            Some(memchr::memchr(b'\n', &bytes[i..]).map_or(bytes.len(), |p| i + p))
        }
        (b'/', Some(b'*')) => Some(block_comment_end(bytes, i)),
        _ => None,
    }
}

/// Splits `list` on `delimiter` where it stands at nesting depth zero —
/// outside every quoted run, comment, `(…)` and `[…]` — returning each piece
/// trimmed and borrowed from `list`. A trailing empty piece is dropped
/// (`1, ` is one piece) while an empty one between delimiters is kept.
///
/// This is the shape every list splitter needs and most got wrong: a
/// splitter that tracks only quotes cuts `f(1, 2)`, `ARRAY[1,2]` and
/// `numeric(10,2)` into pieces, and when the piece count happens to match
/// what was expected the pieces are used as values.
pub fn split_top_level(list: &str, delimiter: u8) -> Vec<&str> {
    let bytes = list.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
            i = end;
            continue;
        }
        match bytes[i] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth = depth.saturating_sub(1),
            c if c == delimiter && depth == 0 => {
                out.push(list[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    let last = list[start..].trim();
    if !last.is_empty() {
        out.push(last);
    }
    out
}

/// Index of the bracket closing the one at `open` (`(` or `[`), skipping
/// quoted runs and comments; `None` when it never closes.
pub fn matching_close(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    while i < bytes.len() {
        // Only these bytes can open a quoted run or a comment or change the
        // depth; every other one is stepped over without the two probes,
        // which ran at each byte of every statement's several scans.
        if !matches!(bytes[i], b'(' | b'[' | b')' | b']' | b'\'' | b'"' | b'$' | b'-' | b'/') {
            i += 1;
            continue;
        }
        if i > open {
            if let Some(end) = skip_quoted(bytes, i).or_else(|| skip_comment(bytes, i)) {
                i = end;
                continue;
            }
        }
        match bytes[i] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Rewrites every escape string (`E'…'`) and dollar-quoted string
/// (`$tag$…$tag$`) into the standard string it means, leaving everything else
/// — standard strings, quoted identifiers, comments, `$1` placeholders — byte
/// for byte as it was.
///
/// Borrowed when there is nothing to rewrite, which is nearly every statement.
/// An escape that does not decode — a byte sequence that is not UTF-8, a NUL,
/// a `\u` that names no character — is an error, as it is in PostgreSQL.
pub fn normalize_string_literals(sql: &str) -> Result<Cow<'_, str>, String> {
    let bytes = sql.as_bytes();
    if !needs_normalizing(bytes) {
        return Ok(Cow::Borrowed(sql));
    }

    let mut out = String::with_capacity(sql.len() + 8);
    let mut copied = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        // Jump to the next byte that can open anything.
        match memchr::memchr3(b'\'', b'$', b'"', &bytes[i..]) {
            Some(p) => {
                // A comment can hide a quote; find whether one opens first.
                let next = i + p;
                if let Some(c) = first_comment_start(bytes, i, next) {
                    i = skip_comment(bytes, c).expect("a comment opens here");
                    continue;
                }
                i = next;
            }
            None => break,
        }
        match bytes[i] {
            b'\'' if is_escape_string_quote(bytes, i) => {
                let Some(end) = terminated_escape_literal_end(bytes, i) else {
                    // Unterminated: left for the parser to report.
                    i = bytes.len();
                    continue;
                };
                let body = &bytes[i + 1..end - 1];
                let decoded = decode_escape_body(body)?;
                // Drop the `E` already copied with the run before the quote.
                out.push_str(&sql[copied..i - 1]);
                push_standard_literal(&mut out, &decoded);
                i = end;
                copied = end;
            }
            b'\'' => i = string_literal_end(bytes, i, false),
            b'"' => i = quoted_end(bytes, i, b'"'),
            b'$' => {
                let opens = i == 0 || !is_identifier_byte(bytes[i - 1]);
                match opens.then(|| dollar_tag_len(bytes, i)).flatten() {
                    Some(tag_len) => {
                        let tag = &bytes[i..i + tag_len];
                        let body_start = i + tag_len;
                        match memchr::memmem::find(&bytes[body_start..], tag) {
                            Some(p) => {
                                let body = &sql[body_start..body_start + p];
                                out.push_str(&sql[copied..i]);
                                push_standard_literal(&mut out, body);
                                i = body_start + p + tag_len;
                                copied = i;
                            }
                            // Unterminated: left as written.
                            None => i = bytes.len(),
                        }
                    }
                    None => i += 1,
                }
            }
            _ => i += 1,
        }
    }
    out.push_str(&sql[copied..]);
    Ok(Cow::Owned(out))
}

/// [`string_literal_end`] for an escape string, `None` when it never closes.
fn terminated_escape_literal_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start + 1;
    loop {
        i += memchr::memchr2(b'\'', b'\\', &bytes[i..])?;
        if bytes[i] == b'\\' {
            i += 2;
            if i > bytes.len() {
                return None;
            }
            continue;
        }
        if bytes.get(i + 1) == Some(&b'\'') {
            i += 2;
            continue;
        }
        return Some(i + 1);
    }
}

/// Cheap pre-check: is there any `E'` or `$tag$` at all? A false positive only
/// costs the full pass.
fn needs_normalizing(bytes: &[u8]) -> bool {
    for p in memchr::memchr2_iter(b'\'', b'$', bytes) {
        match bytes[p] {
            b'\'' if is_escape_string_quote(bytes, p) => return true,
            b'$' if (p == 0 || !is_identifier_byte(bytes[p - 1]))
                && dollar_tag_len(bytes, p).is_some() =>
            {
                return true;
            }
            _ => {}
        }
    }
    false
}

/// The first `--` or `/*` in `bytes[from..to]`, if any.
fn first_comment_start(bytes: &[u8], from: usize, to: usize) -> Option<usize> {
    let window = &bytes[from..to];
    let mut at = 0usize;
    while let Some(p) = memchr::memchr2(b'-', b'/', &window[at..]) {
        let k = from + at + p;
        if skip_comment(bytes, k).is_some() {
            return Some(k);
        }
        at += p + 1;
    }
    None
}

/// `'` + `body` with every quote doubled + `'`.
fn push_standard_literal(out: &mut String, body: &str) {
    out.reserve(body.len() + 2);
    out.push('\'');
    let mut start = 0usize;
    for at in memchr::memchr_iter(b'\'', body.as_bytes()) {
        out.push_str(&body[start..=at]);
        out.push('\'');
        start = at + 1;
    }
    out.push_str(&body[start..]);
    out.push('\'');
}

/// Decodes the body of an escape string the way PostgreSQL does: `\b \f \n \r
/// \t`, octal `\o`–`\ooo`, hex `\xh`–`\xhh`, `\uXXXX` and `\UXXXXXXXX`
/// (surrogate pairs combined), `''` as one quote, and any other escaped
/// character as itself.
fn decode_escape_body(body: &[u8]) -> Result<String, String> {
    let mut out: Vec<u8> = Vec::with_capacity(body.len());
    let mut i = 0usize;
    let hex = |b: u8| (b as char).to_digit(16);
    while i < body.len() {
        let b = body[i];
        if b == b'\'' {
            // Only a doubled quote can sit inside the body.
            out.push(b'\'');
            i += 2;
            continue;
        }
        if b != b'\\' {
            out.push(b);
            i += 1;
            continue;
        }
        let Some(&e) = body.get(i + 1) else {
            out.push(b'\\');
            break;
        };
        i += 2;
        match e {
            b'b' => out.push(0x08),
            b'f' => out.push(0x0c),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'0'..=b'7' => {
                let mut v = u32::from(e - b'0');
                let mut n = 1;
                while n < 3 && i < body.len() && (b'0'..=b'7').contains(&body[i]) {
                    v = v * 8 + u32::from(body[i] - b'0');
                    i += 1;
                    n += 1;
                }
                out.push((v & 0xff) as u8);
            }
            b'x' if body.get(i).and_then(|b| hex(*b)).is_some() => {
                let mut v = hex(body[i]).unwrap();
                i += 1;
                if let Some(d) = body.get(i).and_then(|b| hex(*b)) {
                    v = v * 16 + d;
                    i += 1;
                }
                out.push(v as u8);
            }
            b'u' | b'U' => {
                let width = if e == b'u' { 4 } else { 8 };
                let mut code = read_hex(body, i, width)
                    .ok_or_else(|| "invalid Unicode escape: \\u needs 4 hex digits, \\U 8".to_string())?;
                i += width;
                if (0xD800..0xDC00).contains(&code) {
                    // A high surrogate must be followed by `\uDC00`–`\uDFFF`.
                    let low = (body.get(i) == Some(&b'\\') && body.get(i + 1) == Some(&b'u'))
                        .then(|| read_hex(body, i + 2, 4))
                        .flatten()
                        .filter(|l| (0xDC00..0xE000).contains(l))
                        .ok_or_else(|| "invalid Unicode surrogate pair".to_string())?;
                    i += 6;
                    code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                }
                let c = char::from_u32(code)
                    .ok_or_else(|| format!("invalid Unicode escape value {:#x}", code))?;
                let mut buf = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
            other => out.push(other),
        }
    }
    if out.contains(&0) {
        return Err("invalid byte sequence for encoding \"UTF8\": 0x00".to_string());
    }
    String::from_utf8(out).map_err(|e| {
        format!(
            "invalid byte sequence for encoding \"UTF8\" in an escape string at byte {}",
            e.utf8_error().valid_up_to()
        )
    })
}

fn read_hex(body: &[u8], at: usize, width: usize) -> Option<u32> {
    let digits = body.get(at..at + width)?;
    let mut v = 0u32;
    for d in digits {
        v = v * 16 + (*d as char).to_digit(16)?;
    }
    Some(v)
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_doubled_quote_in_an_identifier_is_found_and_nowhere_else() {
        use super::identifier_with_doubled_quote as found;
        assert_eq!(found(r#"CREATE TABLE "t"" x ""y" (id INT)"#), Some(r#""t"" x ""y""#));
        assert_eq!(found(r#"SELECT "a""b" FROM t"#), Some(r#""a""b""#));
        assert_eq!(found(r#"SELECT "a""" FROM t"#), Some(r#""a""""#));
        assert_eq!(found(r#"SELECT "a" FROM "b""#), None);
        assert_eq!(found(r#"SELECT "" FROM t"#), None);
        assert_eq!(found(r#"SELECT 'x""y', $$""$$ FROM t -- "a""b""#), None);
        assert_eq!(found(r#"SELECT 1 /* "a""b" */"#), None);
    }

    use super::*;

    fn norm(sql: &str) -> String {
        normalize_string_literals(sql).unwrap().into_owned()
    }

    #[test]
    fn escape_strings_become_standard_strings() {
        assert_eq!(norm(r"SELECT E'it\'s%'"), "SELECT 'it''s%'");
        assert_eq!(norm(r"x = e'a\\b'"), r"x = 'a\b'");
        assert_eq!(norm(r"E'\n\t\x41\101é\U0001F642'"), "'\n\tAAé🙂'");
        assert_eq!(norm(r"E'a''b'"), "'a''b'");
        assert_eq!(norm(r"E'🙂'"), "'🙂'");
        assert_eq!(norm(r"E'\q'"), "'q'");
        // Unterminated (the last quote is escaped): left as written.
        assert_eq!(norm(r"x = E'abc\'"), r"x = E'abc\'");
        // Not an escape-string prefix: the E ends a word.
        assert_eq!(norm(r"CASE WHEN x THEN 1 ELSE'\' END"), r"CASE WHEN x THEN 1 ELSE'\' END");
        // Inside a standard string, a comment or an identifier: untouched.
        assert_eq!(norm(r"'E''x' -- E'\'' "), r"'E''x' -- E'\'' ");
        assert_eq!(norm(r#"/* E'\' */ "E'" "#), r#"/* E'\' */ "E'" "#);
    }

    #[test]
    fn dollar_quotes_become_standard_strings() {
        assert_eq!(norm("SELECT $$a, 'b$$"), "SELECT 'a, ''b'");
        assert_eq!(norm("x = $t$ $$ $1 $t$, $1"), "x = ' $$ $1 ', $1");
        // Placeholders and identifiers with `$` are left alone.
        assert_eq!(norm("WHERE a$b$ = $1 AND c = $2"), "WHERE a$b$ = $1 AND c = $2");
        // Unterminated: as written.
        assert_eq!(norm("SELECT $$abc"), "SELECT $$abc");
    }

    #[test]
    fn bad_escapes_are_errors() {
        assert!(normalize_string_literals(r"E'\0'").is_err());
        assert!(normalize_string_literals(r"E'\xff'").is_err());
        assert!(normalize_string_literals(r"E'\u12'").is_err());
        assert!(normalize_string_literals(r"E'\uD83D'").is_err());
    }

    #[test]
    fn nothing_to_do_borrows() {
        assert!(matches!(
            normalize_string_literals("SELECT 'x' FROM t WHERE a = $1"),
            Ok(Cow::Borrowed(_))
        ));
    }

    #[test]
    fn top_level_split_tracks_nesting() {
        assert_eq!(
            split_top_level(r"1, f(2, 3), ARRAY[4,5], '6,7'::text, E'8\',9', $$1,0$$, ", b','),
            vec!["1", "f(2, 3)", "ARRAY[4,5]", "'6,7'::text", r"E'8\',9'", "$$1,0$$"]
        );
        assert_eq!(split_top_level("a,,b", b','), vec!["a", "", "b"]);
        assert_eq!(matching_close("(a, (b), ')', E'\\')') x", 0), Some(20));
    }
}
