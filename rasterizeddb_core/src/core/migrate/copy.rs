//! Reading `COPY … TO STDOUT` text.
//!
//! The text format is one line a row, tab between columns, `\N` for a null,
//! and backslash escapes for the handful of bytes that would otherwise be
//! structural. Undoing that is the whole of this module, and it is kept apart
//! from the import so it can be tested against the cases that matter — a tab
//! inside a value, a newline inside a value, a literal backslash, an empty
//! string, and a value that is the two characters `\N` rather than a null.
//!
//! Binary format was considered and rejected. It would need a decoder per type
//! — a second implementation of everything `value_codec` already does, kept in
//! step by hand — where text needs one unescape and hands the value to the
//! parser the engine itself uses.

/// Splits one `COPY` line into its columns, undoing the escaping.
///
/// `None` in the result is a SQL null, which is `\N` on the wire and is
/// therefore distinguishable from the two-character string `\N`, written
/// `\\N`.
pub fn split_row(line: &[u8]) -> Result<Vec<Option<String>>, String> {
    let mut out = Vec::new();
    for field in line.split(|b| *b == b'\t') {
        out.push(unescape_field(field)?);
    }
    Ok(out)
}

/// Whether a line is the end-of-data marker rather than a row.
///
/// PostgreSQL does not send `\.` over version 3 of the protocol — `CopyDone`
/// ends the stream — but a server that did would otherwise have its marker
/// stored as a row. A value that really is `\.` arrives escaped as `\\.`, so
/// this cannot swallow data.
pub fn is_terminator(line: &[u8]) -> bool {
    line == b"\\."
}

fn unescape_field(field: &[u8]) -> Result<Option<String>, String> {
    if field == b"\\N" {
        return Ok(None);
    }

    // The overwhelming majority of values carry no escape at all, and walking
    // them byte by byte to discover that is the single hottest loop in an
    // import.
    if !field.contains(&b'\\') {
        return std::str::from_utf8(field)
            .map(|s| Some(s.to_string()))
            .map_err(|_| "a value was not valid UTF-8".to_string());
    }

    let mut out: Vec<u8> = Vec::with_capacity(field.len());
    let mut i = 0usize;
    while i < field.len() {
        if field[i] != b'\\' {
            out.push(field[i]);
            i += 1;
            continue;
        }

        i += 1;
        let Some(&c) = field.get(i) else {
            // A trailing backslash is not something COPY produces.
            return Err("a value ended in a lone backslash".to_string());
        };
        i += 1;

        match c {
            b'b' => out.push(0x08),
            b'f' => out.push(0x0c),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'v' => out.push(0x0b),
            b'\\' => out.push(b'\\'),
            b'x' | b'X' => {
                // One or two hex digits.
                let mut value = 0u8;
                let mut digits = 0;
                while digits < 2 {
                    let Some(d) = field.get(i).and_then(|b| hex_digit(*b)) else {
                        break;
                    };
                    value = value * 16 + d;
                    i += 1;
                    digits += 1;
                }
                if digits == 0 {
                    // `\x` with nothing after it stands for `x`.
                    out.push(b'x');
                } else {
                    out.push(value);
                }
            }
            b'0'..=b'7' => {
                // One to three octal digits, the first already consumed.
                let mut value = u32::from(c - b'0');
                let mut digits = 1;
                while digits < 3 {
                    let Some(d) = field.get(i).filter(|b| (b'0'..=b'7').contains(b)) else {
                        break;
                    };
                    value = value * 8 + u32::from(d - b'0');
                    i += 1;
                    digits += 1;
                }
                out.push(value as u8);
            }
            // "Any other backslashed character represents itself", which is
            // what PostgreSQL's own reader does.
            other => out.push(other),
        }
    }

    String::from_utf8(out)
        .map(Some)
        .map_err(|_| "a value was not valid UTF-8 once unescaped".to_string())
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(line: &str) -> Vec<Option<String>> {
        split_row(line.as_bytes()).unwrap()
    }

    fn one(field: &str) -> Option<String> {
        let mut r = row(field);
        assert_eq!(r.len(), 1);
        r.remove(0)
    }

    #[test]
    fn columns_are_split_on_tabs() {
        assert_eq!(
            row("1\tabc\t2026-01-01"),
            vec![
                Some("1".to_string()),
                Some("abc".to_string()),
                Some("2026-01-01".to_string())
            ]
        );
    }

    #[test]
    fn a_null_is_not_the_two_character_string() {
        assert_eq!(one("\\N"), None);
        assert_eq!(one("\\\\N"), Some("\\N".to_string()));
    }

    #[test]
    fn an_empty_value_is_an_empty_string_not_a_null() {
        assert_eq!(row("1\t\t2"), vec![Some("1".into()), Some("".into()), Some("2".into())]);
    }

    #[test]
    fn the_structural_bytes_survive_inside_a_value() {
        assert_eq!(one("a\\tb"), Some("a\tb".to_string()));
        assert_eq!(one("a\\nb"), Some("a\nb".to_string()));
        assert_eq!(one("a\\rb"), Some("a\rb".to_string()));
        assert_eq!(one("a\\\\b"), Some("a\\b".to_string()));
        assert_eq!(one("\\b\\f\\v"), Some("\u{8}\u{c}\u{b}".to_string()));
    }

    #[test]
    fn a_tab_inside_a_value_does_not_split_the_row() {
        assert_eq!(
            row("first\tone\\ttwo\tlast"),
            vec![
                Some("first".to_string()),
                Some("one\ttwo".to_string()),
                Some("last".to_string())
            ]
        );
    }

    #[test]
    fn octal_and_hex_escapes_are_read() {
        assert_eq!(one("\\101"), Some("A".to_string()));
        assert_eq!(one("\\x41"), Some("A".to_string()));
        assert_eq!(one("\\x4a"), Some("J".to_string()));
        assert_eq!(one("\\1"), Some("\u{1}".to_string()));
    }

    #[test]
    fn non_ascii_text_is_carried_through_unchanged() {
        assert_eq!(one("café ünïcødé 😀"), Some("café ünïcødé 😀".to_string()));
    }

    #[test]
    fn bytea_hex_arrives_with_one_backslash() {
        // `COPY` escapes the backslash of `\x48…`, so the value on the wire is
        // `\\x48…`.
        assert_eq!(one("\\\\x48656c6c6f"), Some("\\x48656c6c6f".to_string()));
    }

    #[test]
    fn the_end_marker_is_recognised_and_a_value_like_it_is_not() {
        assert!(is_terminator(b"\\."));
        assert!(!is_terminator(b"\\\\."));
        assert_eq!(one("\\\\."), Some("\\.".to_string()));
    }

    #[test]
    fn a_lone_trailing_backslash_is_refused() {
        assert!(split_row(b"abc\\").is_err());
    }
}
