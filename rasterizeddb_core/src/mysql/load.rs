//! `LOAD DATA LOCAL INFILE`: the statement, and the records of the file the
//! client sends, read as MySQL reads them.
//!
//! Fields end at FIELDS TERMINATED BY (a tab), records at LINES TERMINATED
//! BY (a newline). A field may be ENCLOSED BY a character (none by
//! default; OPTIONALLY changes nothing on input), which a doubled one or
//! the escape character stands for inside it. The ESCAPED BY character (a
//! backslash) makes `\0 \b \n \r \t \Z` those characters, `\N` NULL, and
//! any other character itself; with an enclosure set, an unenclosed `NULL`
//! is NULL too. LINES STARTING BY skips up to its prefix, and lines without
//! it.

use super::lex::{Tok, Token};
use super::translate::MyError;
use crate::sqlcompat::{kw, split_commas};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Duplicates {
    /// LOCAL without REPLACE: a duplicate key skips the row, as IGNORE.
    Ignore,
    Replace,
    /// Neither, and not LOCAL: a duplicate key is an error.
    Error,
}

#[derive(Clone, Debug)]
pub(crate) struct LoadSpec {
    pub(crate) local: bool,
    pub(crate) file: String,
    pub(crate) duplicates: Duplicates,
    pub(crate) table: String,
    pub(crate) format: Format,
    pub(crate) ignore_lines: u64,
    /// The column each field goes to; `None` for a `@variable` (dropped).
    pub(crate) columns: Option<Vec<Option<String>>>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Format {
    pub(crate) field_end: Vec<u8>,
    pub(crate) enclosed: Option<u8>,
    pub(crate) escape: Option<u8>,
    pub(crate) line_start: Vec<u8>,
    pub(crate) line_end: Vec<u8>,
}

impl Default for Format {
    fn default() -> Self {
        Format {
            field_end: b"\t".to_vec(),
            enclosed: None,
            escape: Some(b'\\'),
            line_start: Vec::new(),
            line_end: b"\n".to_vec(),
        }
    }
}

fn string_at(tokens: &[Token], i: usize) -> Result<Vec<u8>, MyError> {
    match tokens.get(i).map(|t| &t.tok) {
        Some(Tok::Str(s)) => Ok(s.clone()),
        _ => Err(MyError::syntax("a string was expected in LOAD DATA")),
    }
}

pub(crate) fn parse(tokens: &[Token]) -> Result<LoadSpec, MyError> {
    let mut i = 2; // LOAD DATA
    while kw(tokens, i, "LOW_PRIORITY") || kw(tokens, i, "CONCURRENT") {
        i += 1;
    }
    let local = kw(tokens, i, "LOCAL");
    if local {
        i += 1;
    }
    if !kw(tokens, i, "INFILE") {
        return Err(MyError::syntax("LOAD DATA … INFILE was expected"));
    }
    let file = String::from_utf8_lossy(&string_at(tokens, i + 1)?).into_owned();
    i += 2;
    let mut duplicates = if local { Duplicates::Ignore } else { Duplicates::Error };
    if kw(tokens, i, "REPLACE") {
        duplicates = Duplicates::Replace;
        i += 1;
    } else if kw(tokens, i, "IGNORE") {
        duplicates = Duplicates::Ignore;
        i += 1;
    }
    if !(kw(tokens, i, "INTO") && kw(tokens, i + 1, "TABLE")) {
        return Err(MyError::syntax("LOAD DATA … INTO TABLE was expected"));
    }
    i += 2;
    let mut table = tokens
        .get(i)
        .and_then(|t| t.ident())
        .ok_or_else(|| MyError::syntax("LOAD DATA needs a table"))?
        .to_lowercase();
    i += 1;
    if tokens.get(i).is_some_and(|t| t.is_op(".")) {
        table = tokens
            .get(i + 1)
            .and_then(|t| t.ident())
            .ok_or_else(|| MyError::syntax("LOAD DATA needs a table"))?
            .to_lowercase();
        i += 2;
    }
    if kw(tokens, i, "PARTITION") {
        return Err(MyError::unsupported("LOAD DATA … PARTITION"));
    }
    let mut format = Format::default();
    let mut ignore_lines = 0;
    let mut columns = None;
    while i < tokens.len() {
        let w = tokens[i].upper().unwrap_or_default();
        match w.as_str() {
            "CHARACTER" if kw(tokens, i + 1, "SET") => {
                let cs = tokens.get(i + 2).and_then(|t| t.ident()).unwrap_or("").to_ascii_lowercase();
                if !matches!(cs.as_str(), "utf8" | "utf8mb4" | "utf8mb3" | "binary" | "ascii") {
                    return Err(MyError::unsupported(format!("LOAD DATA … CHARACTER SET {cs}")));
                }
                i += 3;
            }
            "CHARSET" => i += 2,
            "FIELDS" | "COLUMNS" => {
                i += 1;
                loop {
                    if kw(tokens, i, "TERMINATED") && kw(tokens, i + 1, "BY") {
                        format.field_end = string_at(tokens, i + 2)?;
                        i += 3;
                    } else if kw(tokens, i, "OPTIONALLY") && kw(tokens, i + 1, "ENCLOSED") {
                        i += 1;
                    } else if kw(tokens, i, "ENCLOSED") && kw(tokens, i + 1, "BY") {
                        format.enclosed = string_at(tokens, i + 2)?.first().copied();
                        i += 3;
                    } else if kw(tokens, i, "ESCAPED") && kw(tokens, i + 1, "BY") {
                        format.escape = string_at(tokens, i + 2)?.first().copied();
                        i += 3;
                    } else {
                        break;
                    }
                }
            }
            "LINES" => {
                i += 1;
                loop {
                    if kw(tokens, i, "STARTING") && kw(tokens, i + 1, "BY") {
                        format.line_start = string_at(tokens, i + 2)?;
                        i += 3;
                    } else if kw(tokens, i, "TERMINATED") && kw(tokens, i + 1, "BY") {
                        format.line_end = string_at(tokens, i + 2)?;
                        i += 3;
                    } else {
                        break;
                    }
                }
            }
            "IGNORE" => {
                ignore_lines = match tokens.get(i + 1).map(|t| &t.tok) {
                    Some(Tok::Num(n)) => n.parse().map_err(|_| MyError::syntax("IGNORE needs a number"))?,
                    _ => return Err(MyError::syntax("IGNORE needs a number of lines")),
                };
                i += 3; // IGNORE n LINES|ROWS
            }
            _ if tokens[i].is_op("(") => {
                let close = crate::sqlcompat::matching_paren(tokens, i)
                    .ok_or_else(|| MyError::syntax("unbalanced column list"))?;
                let mut cols = Vec::new();
                for part in split_commas(&tokens[i + 1..close]) {
                    match part.first().map(|t| &t.tok) {
                        Some(Tok::UserVar(_)) => cols.push(None),
                        _ => cols.push(Some(
                            part.last()
                                .and_then(|t| t.ident())
                                .ok_or_else(|| MyError::syntax("a column name was expected"))?
                                .to_lowercase(),
                        )),
                    }
                }
                columns = Some(cols);
                i = close + 1;
            }
            "SET" => return Err(MyError::unsupported("LOAD DATA … SET")),
            _ => {
                return Err(MyError::syntax(format!(
                    "unexpected '{}' in LOAD DATA",
                    crate::sqlcompat::render_text(&tokens[i..i + 1])
                )));
            }
        }
    }
    if format.line_end.is_empty() {
        return Err(MyError::unsupported("LOAD DATA with an empty LINES TERMINATED BY"));
    }
    Ok(LoadSpec {
        local,
        file,
        duplicates,
        table,
        format,
        ignore_lines,
        columns,
    })
}

/// Reads records from bytes as they arrive: [`Reader::feed`] the file's
/// chunks, take the complete records, [`Reader::finish`] at the end.
pub(crate) struct Reader {
    format: Format,
    buf: Vec<u8>,
    to_skip: u64,
}

impl Reader {
    pub(crate) fn new(format: Format, ignore_lines: u64) -> Self {
        Reader {
            format,
            buf: Vec::new(),
            to_skip: ignore_lines,
        }
    }

    pub(crate) fn feed(&mut self, chunk: &[u8]) -> Vec<Vec<Option<Vec<u8>>>> {
        self.buf.extend_from_slice(chunk);
        self.take(false)
    }

    pub(crate) fn finish(&mut self) -> Vec<Vec<Option<Vec<u8>>>> {
        self.take(true)
    }

    fn take(&mut self, at_end: bool) -> Vec<Vec<Option<Vec<u8>>>> {
        let mut out = Vec::new();
        let mut pos = 0;
        while let Some((record, next)) = self.record(pos, at_end) {
            pos = next;
            if self.to_skip > 0 {
                self.to_skip -= 1;
                continue;
            }
            if let Some(r) = record {
                out.push(r);
            }
        }
        self.buf.drain(..pos);
        out
    }

    /// The record from `pos`: (its fields, or `None` for a line skipped
    /// for want of LINES STARTING BY; where the next begins). `None` when
    /// the record is not complete yet.
    #[allow(clippy::type_complexity)]
    fn record(&self, pos: usize, at_end: bool) -> Option<(Option<Vec<Option<Vec<u8>>>>, usize)> {
        let b = &self.buf;
        let f = &self.format;
        if pos >= b.len() {
            return None;
        }
        let mut i = pos;
        if !f.line_start.is_empty() {
            // Up to the prefix, which must come before the line ends.
            let line_end = find(b, i, &f.line_end);
            match find(b, i, &f.line_start) {
                Some(p) if line_end.is_none_or(|e| p < e) => i = p + f.line_start.len(),
                _ => {
                    return match line_end {
                        Some(e) => Some((None, e + f.line_end.len())),
                        None if at_end => Some((None, b.len())),
                        None => None,
                    };
                }
            }
        }
        let mut fields = Vec::new();
        loop {
            // One field.
            let mut value = Vec::new();
            let mut enclosed = false;
            let mut escaped_null = false;
            if f.enclosed.is_some_and(|q| b.get(i) == Some(&q)) {
                enclosed = true;
                i += 1;
                let q = f.enclosed.unwrap();
                loop {
                    let Some(&c) = b.get(i) else {
                        // The file ends inside the enclosure: the field is
                        // what came, not a record lost.
                        if !at_end {
                            return None;
                        }
                        fields.push(field_value(value, true, false, f));
                        return Some((Some(fields), i));
                    };
                    if Some(c) == f.escape && i + 1 < b.len() {
                        value.push(unescape(b[i + 1]));
                        i += 2;
                    } else if Some(c) == f.escape {
                        if at_end {
                            value.push(c);
                            i += 1;
                        } else {
                            return None;
                        }
                    } else if c == q {
                        if b.get(i + 1) == Some(&q) {
                            value.push(q);
                            i += 2;
                        } else if i + 1 == b.len() && !at_end {
                            return None;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        value.push(c);
                        i += 1;
                    }
                }
            }
            // To the field or line end.
            loop {
                if starts(b, i, &f.field_end) {
                    i += f.field_end.len();
                    fields.push(field_value(value, enclosed, escaped_null, f));
                    break;
                }
                if starts(b, i, &f.line_end) {
                    fields.push(field_value(value, enclosed, escaped_null, f));
                    return Some((Some(fields), i + f.line_end.len()));
                }
                if i >= b.len() {
                    if !at_end {
                        return None;
                    }
                    if fields.is_empty() && value.is_empty() && !enclosed && i == pos {
                        return None;
                    }
                    fields.push(field_value(value, enclosed, escaped_null, f));
                    return Some((Some(fields), i));
                }
                // A terminator may be cut by the chunk's end.
                if !at_end && (prefix_cut(b, i, &f.field_end) || prefix_cut(b, i, &f.line_end)) {
                    return None;
                }
                let c = b[i];
                if Some(c) == f.escape {
                    match b.get(i + 1) {
                        Some(b'N') if value.is_empty() && !enclosed => {
                            escaped_null = true;
                            i += 2;
                        }
                        Some(&n) => {
                            value.push(unescape(n));
                            i += 2;
                        }
                        None if at_end => {
                            value.push(c);
                            i += 1;
                        }
                        None => return None,
                    }
                } else {
                    value.push(c);
                    i += 1;
                }
            }
        }
    }
}

fn field_value(value: Vec<u8>, enclosed: bool, escaped_null: bool, f: &Format) -> Option<Vec<u8>> {
    if escaped_null && value.is_empty() {
        return None;
    }
    if !enclosed && f.enclosed.is_some() && value == b"NULL" {
        return None;
    }
    Some(value)
}

fn unescape(c: u8) -> u8 {
    match c {
        b'0' => 0,
        b'b' => 8,
        b'n' => b'\n',
        b'r' => b'\r',
        b't' => b'\t',
        b'Z' => 0x1a,
        other => other,
    }
}

fn starts(b: &[u8], i: usize, s: &[u8]) -> bool {
    !s.is_empty() && b.len() >= i + s.len() && &b[i..i + s.len()] == s
}

/// Whether the bytes from `i` to the end are a proper prefix of `s`.
fn prefix_cut(b: &[u8], i: usize, s: &[u8]) -> bool {
    let rest = &b[i..];
    !rest.is_empty() && rest.len() < s.len() && s.starts_with(rest)
}

fn find(b: &[u8], from: usize, s: &[u8]) -> Option<usize> {
    if s.is_empty() || from >= b.len() {
        return None;
    }
    b[from..].windows(s.len()).position(|w| w == s).map(|p| p + from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::lex::lex;
    use crate::mysql::lex::Mode;

    fn spec(sql: &str) -> LoadSpec {
        parse(&lex(sql.as_bytes(), Mode::default()).unwrap()).unwrap()
    }

    fn s(v: &[u8]) -> Option<Vec<u8>> {
        Some(v.to_vec())
    }

    #[test]
    fn statement() {
        let l = spec(
            r#"LOAD DATA LOCAL INFILE '/tmp/x.csv' REPLACE INTO TABLE db.t CHARACTER SET utf8mb4
               FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
               LINES TERMINATED BY '\r\n' IGNORE 1 LINES (a, @skip, `B`)"#,
        );
        assert!(l.local);
        assert_eq!(l.file, "/tmp/x.csv");
        assert_eq!(l.duplicates, Duplicates::Replace);
        assert_eq!(l.table, "t");
        assert_eq!(l.format.field_end, b",");
        assert_eq!(l.format.enclosed, Some(b'"'));
        assert_eq!(l.format.line_end, b"\r\n");
        assert_eq!(l.ignore_lines, 1);
        assert_eq!(l.columns, Some(vec![Some("a".into()), None, Some("b".into())]));
        assert_eq!(spec("LOAD DATA INFILE 'f' INTO TABLE t").duplicates, Duplicates::Error);
        assert_eq!(spec("LOAD DATA LOCAL INFILE 'f' INTO TABLE t").duplicates, Duplicates::Ignore);
    }

    #[test]
    fn records_as_mysql_reads_them() {
        // Defaults: tabs, newlines, backslash escapes, \N.
        let mut r = Reader::new(Format::default(), 0);
        let rows = r.feed(b"1\ta\\tb\t\\N\n2\t\\\\\tx");
        assert_eq!(rows, vec![vec![s(b"1"), s(b"a\tb"), None]]);
        assert_eq!(r.finish(), vec![vec![s(b"2"), s(b"\\"), s(b"x")]]);

        // CSV with an enclosure, chunked anywhere.
        let csv = b"id,name\r\n1,\"Ann, \"\"A\"\"\"\r\n2,NULL\r\n3,\"\"\r\n";
        let format = Format {
            field_end: b",".to_vec(),
            enclosed: Some(b'"'),
            escape: Some(b'\\'),
            line_start: Vec::new(),
            line_end: b"\r\n".to_vec(),
        };
        let want = vec![
            vec![s(b"1"), s(b"Ann, \"A\"")],
            vec![s(b"2"), None],
            vec![s(b"3"), s(b"")],
        ];
        for cut in 1..csv.len() {
            let mut r = Reader::new(format.clone(), 1);
            let mut got = r.feed(&csv[..cut]);
            got.extend(r.feed(&csv[cut..]));
            got.extend(r.finish());
            assert_eq!(got, want, "cut at {cut}");
        }

        // LINES STARTING BY skips lines without the prefix.
        let mut r = Reader::new(
            Format {
                line_start: b"xxx".to_vec(),
                ..Format::default()
            },
            0,
        );
        let mut got = r.feed(b"xxxa\tb\nnope\nzzxxxc\td\n");
        got.extend(r.finish());
        assert_eq!(got, vec![vec![s(b"a"), s(b"b")], vec![s(b"c"), s(b"d")]]);
    }
}
