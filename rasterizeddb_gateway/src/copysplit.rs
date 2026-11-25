//! `COPY t FROM STDIN` into a sharded table, split by the shard each row
//! goes to (sharding-plan.md §8.5).
//!
//! The gateway answers the client's COPY itself (CopyInResponse), reads its
//! rows as they come, takes each row's key field, hashes it as every router
//! does (`route::param_block`, the key's text) and appends the row, as
//! written, to that shard's buffer. A shard's own `COPY` opens on its first
//! row and gets its rows in CopyData of up to [`FLUSH_BYTES`]. At CopyDone
//! each shard's COPY ends and the counts are summed.
//!
//! Under `two_phase` the whole load is one transaction (the client's, or one
//! of the gateway's own, committed in two phases): a bad row anywhere loads
//! nothing. Without it, `SET rdb.split_bulk_writes = on` allows a split that
//! is not atomic: each shard's COPY commits alone, and a failure names the
//! shards that loaded.
//!
//! Text and CSV formats (`DELIMITER`, `NULL`, `HEADER`, `QUOTE`, `ESCAPE`);
//! not binary. A CSV header line goes to every shard as its first line, so
//! each skips it as the client asked.

use std::collections::BTreeMap;

use rasterizeddb_router::KeyFamily;

/// A shard's buffered rows are sent once they pass this.
pub const FLUSH_BYTES: usize = 256 << 10;

/// A `COPY … FROM STDIN` statement, as far as splitting it needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyIn {
    pub table: String,
    /// The column list as written, when there is one.
    pub columns: Option<Vec<String>>,
    pub csv: bool,
    pub delimiter: u8,
    pub quote: u8,
    pub escape: u8,
    pub header: bool,
    pub null: Vec<u8>,
}

fn unquote_ident(s: &str) -> String {
    let s = s.trim();
    match s.strip_prefix('"').and_then(|r| r.strip_suffix('"')) {
        Some(q) => q.replace("\"\"", "\""),
        None => s.to_ascii_lowercase(),
    }
}

fn single_char(v: &str, what: &str) -> Result<u8, String> {
    let t = v.trim();
    let t = t.strip_prefix(['E', 'e']).unwrap_or(t);
    let inner = t.strip_prefix('\'').and_then(|r| r.strip_suffix('\'')).ok_or_else(|| format!("0A000: COPY {what} takes a quoted character"))?;
    let inner = inner.replace("''", "'");
    let inner = match inner.as_str() {
        "\\t" => "\t".to_string(),
        other => other.to_string(),
    };
    match inner.as_bytes() {
        [c] => Ok(*c),
        _ => Err(format!("0A000: COPY {what} must be a single one-byte character")),
    }
}

fn quoted_text(v: &str, what: &str) -> Result<Vec<u8>, String> {
    let t = v.trim();
    let inner = t.strip_prefix('\'').and_then(|r| r.strip_suffix('\'')).ok_or_else(|| format!("0A000: COPY {what} takes a quoted string"))?;
    Ok(inner.replace("''", "'").into_bytes())
}

/// Splits `s` at top-level commas (outside quotes).
fn split_options(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let (mut cur, mut quoted) = (String::new(), false);
    for c in s.chars() {
        match c {
            '\'' => {
                quoted = !quoted;
                cur.push(c);
            }
            ',' if !quoted => {
                out.push(std::mem::take(&mut cur));
            }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

/// `COPY t [(cols)] FROM STDIN [[WITH] (options) | legacy options]`; `None`
/// for any other statement (a COPY TO, a COPY from a file or program),
/// `Some(Err)` for one this does not split.
pub fn parse(sql: &str) -> Option<Result<CopyIn, String>> {
    let t = sql.trim().trim_end_matches(';').trim();
    if !t.get(..4)?.eq_ignore_ascii_case("copy") || !t.as_bytes().get(4)?.is_ascii_whitespace() {
        return None;
    }
    let rest = t[4..].trim_start();
    // The table, up to its column list or FROM.
    let upper = rest.to_ascii_uppercase();
    let from_at = upper.find(" FROM ")?;
    let target = rest[..from_at].trim();
    let (table, columns) = match target.find('(') {
        Some(open) => {
            let close = target.rfind(')')?;
            let cols = target[open + 1..close].split(',').map(unquote_ident).collect();
            (unquote_ident(&target[..open]), Some(cols))
        }
        None => (unquote_ident(target), None),
    };
    let after = rest[from_at + 6..].trim_start();
    if !after.get(..5)?.eq_ignore_ascii_case("stdin") {
        return None;
    }
    let mut c = CopyIn { table, columns, csv: false, delimiter: b'\t', quote: b'"', escape: b'"', header: false, null: b"\\N".to_vec() };
    let mut opts = after[5..].trim();
    if opts.len() >= 4 && opts[..4].eq_ignore_ascii_case("with") {
        opts = opts[4..].trim_start();
    }
    let mut explicit_null = None;
    let mut explicit_delim = None;
    let mut explicit_escape = None;
    if let Some(inner) = opts.strip_prefix('(') {
        let inner = inner.strip_suffix(')').unwrap_or(inner);
        for opt in split_options(inner) {
            let opt = opt.trim();
            let (name, value) = opt.split_once(char::is_whitespace).map(|(n, v)| (n, v.trim())).unwrap_or((opt, ""));
            match name.to_ascii_uppercase().as_str() {
                "FORMAT" => match value.trim_matches('\'').to_ascii_lowercase().as_str() {
                    "text" => c.csv = false,
                    "csv" => c.csv = true,
                    "binary" => return Some(Err("0A000: a binary COPY into a sharded table is not split across shards; use text or csv".into())),
                    other => return Some(Err(format!("22023: COPY format \"{other}\" not recognized"))),
                },
                "DELIMITER" => explicit_delim = Some(match single_char(value, "DELIMITER") { Ok(v) => v, Err(e) => return Some(Err(e)) }),
                "NULL" => explicit_null = Some(match quoted_text(value, "NULL") { Ok(v) => v, Err(e) => return Some(Err(e)) }),
                "HEADER" => c.header = !matches!(value.trim_matches('\'').to_ascii_lowercase().as_str(), "false" | "off" | "0"),
                "QUOTE" => c.quote = match single_char(value, "QUOTE") { Ok(v) => v, Err(e) => return Some(Err(e)) },
                "ESCAPE" => explicit_escape = Some(match single_char(value, "ESCAPE") { Ok(v) => v, Err(e) => return Some(Err(e)) }),
                "ENCODING" if value.trim_matches('\'').eq_ignore_ascii_case("utf8") || value.trim_matches('\'').eq_ignore_ascii_case("utf-8") => {}
                "FREEZE" => {}
                other => return Some(Err(format!("0A000: COPY option {other} is not split across shards"))),
            }
        }
    } else {
        // The legacy spelling: CSV, HEADER, DELIMITER [AS] 'x', NULL [AS] 'x'.
        let words: Vec<&str> = opts.split_whitespace().collect();
        let mut i = 0;
        while i < words.len() {
            let w = words[i].to_ascii_uppercase();
            let value = |i: usize| {
                let j = if words.get(i + 1).is_some_and(|w| w.eq_ignore_ascii_case("as")) { i + 2 } else { i + 1 };
                (words.get(j).copied().unwrap_or(""), j)
            };
            match w.as_str() {
                "CSV" => c.csv = true,
                "HEADER" => c.header = true,
                "BINARY" => return Some(Err("0A000: a binary COPY into a sharded table is not split across shards; use text or csv".into())),
                "DELIMITER" | "NULL" | "QUOTE" | "ESCAPE" => {
                    let (v, j) = value(i);
                    match w.as_str() {
                        "DELIMITER" => explicit_delim = Some(match single_char(v, "DELIMITER") { Ok(v) => v, Err(e) => return Some(Err(e)) }),
                        "NULL" => explicit_null = Some(match quoted_text(v, "NULL") { Ok(v) => v, Err(e) => return Some(Err(e)) }),
                        "QUOTE" => c.quote = match single_char(v, "QUOTE") { Ok(v) => v, Err(e) => return Some(Err(e)) },
                        _ => explicit_escape = Some(match single_char(v, "ESCAPE") { Ok(v) => v, Err(e) => return Some(Err(e)) }),
                    }
                    i = j;
                }
                other => return Some(Err(format!("0A000: COPY option {other} is not split across shards"))),
            }
            i += 1;
        }
    }
    if c.csv {
        c.delimiter = explicit_delim.unwrap_or(b',');
        c.null = explicit_null.unwrap_or_default();
        c.escape = explicit_escape.unwrap_or(c.quote);
    } else {
        if c.header {
            return Some(Err("0A000: COPY HEADER is available only in CSV mode".into()));
        }
        c.delimiter = explicit_delim.unwrap_or(b'\t');
        c.null = explicit_null.unwrap_or_else(|| b"\\N".to_vec());
    }
    Some(Ok(c))
}

/// One row cut out of the stream: its bytes (with its line end) and its
/// fields' raw extents.
pub struct Rows {
    pending: Vec<u8>,
    /// The end-of-data line (`\.`) was read: the rest is ignored.
    pub ended: bool,
}

impl Rows {
    pub fn new() -> Rows {
        Rows { pending: Vec::new(), ended: false }
    }

    /// Feeds `data`; calls `row` with each complete row (its line end
    /// included). A row split across CopyData messages waits for the rest.
    pub fn feed(&mut self, c: &CopyIn, data: &[u8], mut row: impl FnMut(&[u8]) -> Result<(), String>) -> Result<(), String> {
        if self.ended {
            return Ok(());
        }
        self.pending.extend_from_slice(data);
        let mut start = 0;
        let mut i = 0;
        let mut quoted = false;
        let b = std::mem::take(&mut self.pending);
        while i < b.len() {
            let ch = b[i];
            if c.csv {
                if quoted {
                    if ch == c.escape && c.escape != c.quote && b.get(i + 1).is_some_and(|n| *n == c.quote || *n == c.escape) {
                        i += 2;
                        continue;
                    }
                    if ch == c.quote {
                        quoted = false;
                    }
                    i += 1;
                    continue;
                }
                if ch == c.quote {
                    quoted = true;
                    i += 1;
                    continue;
                }
            } else if ch == b'\\' {
                i += 2;
                continue;
            }
            if ch == b'\n' {
                let line = &b[start..=i];
                let content = line.strip_suffix(b"\n").unwrap_or(line);
                let content = content.strip_suffix(b"\r").unwrap_or(content);
                if content == b"\\." {
                    self.ended = true;
                    return Ok(());
                }
                row(line)?;
                start = i + 1;
            }
            i += 1;
        }
        self.pending = b[start..].to_vec();
        Ok(())
    }

    /// At CopyDone: a last row without a line end.
    pub fn finish(&mut self, c: &CopyIn, mut row: impl FnMut(&[u8]) -> Result<(), String>) -> Result<(), String> {
        if self.ended || self.pending.is_empty() {
            return Ok(());
        }
        let mut last = std::mem::take(&mut self.pending);
        if last.as_slice() == b"\\." || last.as_slice() == b"\\.\r" {
            return Ok(());
        }
        let _ = c;
        last.push(b'\n');
        row(&last)
    }
}

impl Default for Rows {
    fn default() -> Self {
        Rows::new()
    }
}

/// Field `k` of `line` as text (`None`: NULL).
pub fn field(c: &CopyIn, line: &[u8], k: usize) -> Result<Option<Vec<u8>>, String> {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let mut idx = 0;
    let mut i = 0;
    loop {
        // One field from `i`.
        let mut raw = Vec::new();
        let mut was_quoted = false;
        if c.csv {
            let mut quoted = false;
            while i < line.len() {
                let ch = line[i];
                if quoted {
                    if ch == c.escape && line.get(i + 1).is_some_and(|n| *n == c.quote || (*n == c.escape && c.escape != c.quote)) {
                        raw.push(line[i + 1]);
                        i += 2;
                        continue;
                    }
                    if ch == c.quote {
                        quoted = false;
                        i += 1;
                        continue;
                    }
                    raw.push(ch);
                } else if ch == c.quote {
                    quoted = true;
                    was_quoted = true;
                } else if ch == c.delimiter {
                    break;
                } else {
                    raw.push(ch);
                }
                i += 1;
            }
        } else {
            while i < line.len() && line[i] != c.delimiter {
                if line[i] == b'\\' && i + 1 < line.len() {
                    raw.push(b'\\');
                    raw.push(line[i + 1]);
                    i += 2;
                    continue;
                }
                raw.push(line[i]);
                i += 1;
            }
        }
        if idx == k {
            if !was_quoted && raw == c.null {
                return Ok(None);
            }
            return Ok(Some(if c.csv { raw } else { unescape_text(&raw) }));
        }
        if i >= line.len() {
            return Err(format!("22P04: missing data for column {} of the shard key", k + 1));
        }
        i += 1;
        idx += 1;
    }
}

/// A text-format field's backslash escapes resolved.
fn unescape_text(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] != b'\\' || i + 1 >= raw.len() {
            out.push(raw[i]);
            i += 1;
            continue;
        }
        let n = raw[i + 1];
        i += 2;
        match n {
            b'b' => out.push(8),
            b'f' => out.push(12),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'v' => out.push(11),
            b'0'..=b'7' => {
                let mut v = (n - b'0') as u32;
                let mut k = 0;
                while k < 2 && i < raw.len() && (b'0'..=b'7').contains(&raw[i]) {
                    v = v * 8 + (raw[i] - b'0') as u32;
                    i += 1;
                    k += 1;
                }
                out.push(v as u8);
            }
            b'x' => {
                let mut v = 0u32;
                let mut k = 0;
                while k < 2 && i < raw.len() && raw[i].is_ascii_hexdigit() {
                    v = v * 16 + (raw[i] as char).to_digit(16).unwrap();
                    i += 1;
                    k += 1;
                }
                out.push(v as u8);
            }
            other => out.push(other),
        }
    }
    out
}

/// What a split COPY in progress holds.
pub struct Split {
    pub copy: CopyIn,
    /// The statement each shard's COPY runs (the client's own).
    pub statement: String,
    pub key: usize,
    pub family: KeyFamily,
    pub rows: Rows,
    /// The CSV header line, sent to each shard first.
    pub header_line: Option<Vec<u8>>,
    pub header_pending: bool,
    /// Each shard's unsent rows, and the shards whose COPY is open.
    pub buffers: BTreeMap<u32, Vec<u8>>,
    pub opened: Vec<u32>,
    /// One transaction over every shard (`two_phase`), and whether it is the
    /// gateway's own.
    pub atomic: bool,
    pub implicit: bool,
    /// The first failure, answered at CopyDone (the client keeps sending
    /// until then).
    pub failure: Option<(String, String)>,
    pub columns: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_statements_are_read() {
        let c = parse("COPY orders (id, \"Store\", v) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ';', NULL 'NA')").unwrap().unwrap();
        assert_eq!(c.table, "orders");
        assert_eq!(c.columns, Some(vec!["id".into(), "Store".into(), "v".into()]));
        assert!(c.csv && c.header);
        assert_eq!((c.delimiter, c.null.as_slice()), (b';', &b"NA"[..]));
        let c = parse("copy t from stdin").unwrap().unwrap();
        assert_eq!((c.csv, c.delimiter, c.null.as_slice()), (false, b'\t', &b"\\N"[..]));
        let c = parse("COPY t FROM STDIN CSV HEADER DELIMITER AS '|'").unwrap().unwrap();
        assert!(c.csv && c.header && c.delimiter == b'|');
        assert!(parse("COPY t TO STDOUT").is_none());
        assert!(parse("COPY t FROM '/etc/passwd'").is_none());
        assert!(parse("COPY t FROM STDIN (FORMAT binary)").unwrap().is_err());
        assert!(parse("COPY t FROM STDIN (HEADER)").unwrap().is_err());
    }

    #[test]
    fn rows_are_cut_across_messages_and_quotes() {
        let c = parse("COPY t FROM STDIN (FORMAT csv)").unwrap().unwrap();
        let mut rows = Rows::new();
        let mut got = Vec::new();
        rows.feed(&c, b"1,\"a\nb\",x\n2,", |r| {
            got.push(r.to_vec());
            Ok(())
        })
        .unwrap();
        rows.feed(&c, b"\"c,d\"\n3,e", |r| {
            got.push(r.to_vec());
            Ok(())
        })
        .unwrap();
        rows.finish(&c, |r| {
            got.push(r.to_vec());
            Ok(())
        })
        .unwrap();
        assert_eq!(got, vec![b"1,\"a\nb\",x\n".to_vec(), b"2,\"c,d\"\n".to_vec(), b"3,e\n".to_vec()]);
        assert_eq!(field(&c, &got[1], 1).unwrap(), Some(b"c,d".to_vec()));
        assert_eq!(field(&c, b",x\n", 0).unwrap(), None);
        assert_eq!(field(&c, b"\"\",x\n", 0).unwrap(), Some(Vec::new()));
    }

    #[test]
    fn text_fields_unescape_and_the_end_marker_stops() {
        let c = parse("COPY t FROM STDIN").unwrap().unwrap();
        let mut rows = Rows::new();
        let mut got = Vec::new();
        rows.feed(&c, b"a\\tb\t\\N\t12\n\\.\nignored\n", |r| {
            got.push(r.to_vec());
            Ok(())
        })
        .unwrap();
        assert_eq!(got.len(), 1);
        assert!(rows.ended);
        assert_eq!(field(&c, &got[0], 0).unwrap(), Some(b"a\tb".to_vec()));
        assert_eq!(field(&c, &got[0], 1).unwrap(), None);
        assert_eq!(field(&c, &got[0], 2).unwrap(), Some(b"12".to_vec()));
        assert!(field(&c, &got[0], 3).is_err());
    }
}
