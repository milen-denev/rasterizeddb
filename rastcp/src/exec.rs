//! The `\x01EXEC;` request: a statement's text with its `$n` placeholders,
//! sent apart from its values.
//!
//! A native statement is otherwise sent as text with its values written in,
//! which the server then takes apart again to find the statement's shape
//! (`pgwire::text_template`) before it can answer from a parse it learned for
//! that shape. Sent this way, the text *is* the shape and the values arrive
//! already apart, so neither side writes them in nor finds them again.
//!
//! What each value is sent as is the SQL literal the client would have
//! written in its place (`'ada'`, `42`, `NULL`, `0.1::real`), and where each
//! placeholder sits is the byte range the client's own scanner found for it
//! (strings, quoted names, dollar quotes and comments skipped). The server
//! does not scan: [`Exec::render`] splices the literals into those ranges, so
//! the text it runs when it has nothing learned is byte for byte the text the
//! client would have sent. One definition of what a placeholder is and of
//! what a value becomes, the client's, and no second one on the server to
//! keep in step with it.
//!
//! This module knows nothing of SQL: it frames and checks bytes.
//!
//! Layout, after [`PREFIX`], all integers little endian:
//!
//! ```text
//! u32 sql_len, sql (UTF-8)
//! u16 n_literals, per literal: u32 len, bytes (UTF-8)
//! u16 n_spans,    per span:    u32 start, u32 end, u16 literal
//! ```
//!
//! Spans are ascending and disjoint, each within the text and on character
//! boundaries, each naming a literal that exists. Nothing may follow the
//! last. A request that breaks any of that is refused whole ([`decode`]
//! answers `Some(Err(_))`), never run as something else.

/// What an EXEC request starts with. A server without the form reads it as
/// a statement beginning with a control character and refuses it, which a
/// client takes as "unsupported".
pub const PREFIX: &[u8] = b"\x01EXEC;";

/// The longest statement an EXEC request may render to: the longest
/// request a server reads by default ([`crate::common::DEFAULT_MAX_MESSAGE_SIZE`]),
/// so no EXEC request runs a statement longer than one the client could
/// have sent as text.
///
/// Without it the request's size did not bound what it rendered to: a span
/// may name the same literal as every other, so a 1 MB literal named by
/// 65,535 one-byte placeholders asked for a 64 GB statement, and the
/// allocation aborted the server. A client with a longer statement sends it
/// as text.
pub const MAX_RENDERED: usize = crate::common::DEFAULT_MAX_MESSAGE_SIZE;

/// Where one placeholder sits in the text, and which literal fills it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    /// The placeholder's first byte.
    pub start: u32,
    /// Just past its last byte.
    pub end: u32,
    /// Index into [`Exec::literals`].
    pub literal: u16,
}

/// A decoded EXEC request, borrowing from the request's bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exec<'a> {
    /// The statement with its placeholders, as the client wrote it.
    pub sql: &'a str,
    /// Per parameter, by number (`$n`'s at `n - 1`, pgwire's Bind order),
    /// its value as an SQL literal, up to the highest one named. An empty
    /// literal is a parameter no placeholder names; no value renders as
    /// empty, and no span may name one.
    pub literals: Vec<&'a str>,
    /// Every placeholder, in order of position.
    pub spans: Vec<Span>,
}

impl Exec<'_> {
    /// The statement as it would have been sent as text: each span replaced
    /// by its literal.
    pub fn render(&self) -> String {
        let extra: usize = self.spans.iter().map(|s| self.literals[s.literal as usize].len()).sum();
        let mut out = String::with_capacity(self.sql.len() + extra);
        let mut copied = 0usize;
        for span in &self.spans {
            out.push_str(&self.sql[copied..span.start as usize]);
            out.push_str(self.literals[span.literal as usize]);
            copied = span.end as usize;
        }
        out.push_str(&self.sql[copied..]);
        out
    }
}

/// The request for `sql` with `literals` filling `spans`. The caller's spans
/// are written as given; [`decode`] is what checks them, so a client that
/// gets them wrong is refused rather than misread.
pub fn encode(sql: &str, literals: &[&str], spans: &[Span]) -> Result<Vec<u8>, String> {
    let too_long = |what: &str| format!("an EXEC request's {what} is too long for its length field");
    let sql_len = u32::try_from(sql.len()).map_err(|_| too_long("text"))?;
    let n_literals = u16::try_from(literals.len()).map_err(|_| too_long("parameter list"))?;
    let n_spans = u16::try_from(spans.len()).map_err(|_| too_long("placeholder list"))?;
    let size = PREFIX.len() + 4 + sql.len() + 2 + literals.iter().map(|l| 4 + l.len()).sum::<usize>() + 2 + spans.len() * 10;
    let mut out = Vec::with_capacity(size);
    out.extend_from_slice(PREFIX);
    out.extend_from_slice(&sql_len.to_le_bytes());
    out.extend_from_slice(sql.as_bytes());
    out.extend_from_slice(&n_literals.to_le_bytes());
    for literal in literals {
        let len = u32::try_from(literal.len()).map_err(|_| too_long("parameter"))?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(literal.as_bytes());
    }
    out.extend_from_slice(&n_spans.to_le_bytes());
    for span in spans {
        out.extend_from_slice(&span.start.to_le_bytes());
        out.extend_from_slice(&span.end.to_le_bytes());
        out.extend_from_slice(&span.literal.to_le_bytes());
    }
    Ok(out)
}

/// `None` when `request` is not an EXEC request at all; `Some(Err)` when it
/// is one but malformed, which is to be refused, not run as text.
pub fn decode(request: &[u8]) -> Option<Result<Exec<'_>, String>> {
    let body = request.strip_prefix(PREFIX)?;
    Some(decode_body(body))
}

fn decode_body(body: &[u8]) -> Result<Exec<'_>, String> {
    fn take<'b>(body: &'b [u8], at: &mut usize, n: usize, what: &str) -> Result<&'b [u8], String> {
        let end = at
            .checked_add(n)
            .filter(|e| *e <= body.len())
            .ok_or_else(|| format!("an EXEC request ends inside its {what}"))?;
        let slice = &body[*at..end];
        *at = end;
        Ok(slice)
    }
    fn text<'b>(b: &'b [u8], what: &str) -> Result<&'b str, String> {
        std::str::from_utf8(b).map_err(|_| format!("an EXEC request's {what} is not UTF-8"))
    }
    let u32_at = |b: &[u8]| u32::from_le_bytes(b.try_into().expect("four bytes")) as usize;
    let u16_at = |b: &[u8]| u16::from_le_bytes(b.try_into().expect("two bytes")) as usize;
    let mut at = 0usize;
    let mut take = |n: usize, what: &str| take(body, &mut at, n, what);

    let sql_len = u32_at(take(4, "text length")?);
    let sql = text(take(sql_len, "text")?, "text")?;
    let n_literals = u16_at(take(2, "parameter count")?);
    let mut literals = Vec::with_capacity(n_literals);
    for _ in 0..n_literals {
        let len = u32_at(take(4, "parameter length")?);
        literals.push(text(take(len, "parameter")?, "parameter")?);
    }
    let n_spans = u16_at(take(2, "placeholder count")?);
    let mut spans = Vec::with_capacity(n_spans);
    let mut previous_end = 0usize;
    for _ in 0..n_spans {
        let b = take(10, "placeholder")?;
        let span = Span {
            start: u32::from_le_bytes(b[0..4].try_into().expect("four bytes")),
            end: u32::from_le_bytes(b[4..8].try_into().expect("four bytes")),
            literal: u16::from_le_bytes(b[8..10].try_into().expect("two bytes")),
        };
        let (start, end) = (span.start as usize, span.end as usize);
        if start < previous_end || start >= end || end > sql.len() {
            return Err("an EXEC request's placeholders are out of order, empty or past its text".to_string());
        }
        if !sql.is_char_boundary(start) || !sql.is_char_boundary(end) {
            return Err("an EXEC request's placeholder splits a character".to_string());
        }
        if span.literal as usize >= literals.len() {
            return Err(format!(
                "an EXEC request's placeholder names parameter {} of {}",
                span.literal as usize + 1,
                literals.len()
            ));
        }
        if literals[span.literal as usize].is_empty() {
            return Err(format!(
                "an EXEC request's placeholder names parameter {}, which it sent as unnamed",
                span.literal as usize + 1
            ));
        }
        previous_end = end;
        spans.push(span);
    }
    drop(take);
    if at != body.len() {
        return Err("an EXEC request has bytes past its last placeholder".to_string());
    }
    // What `render` would allocate, worked out before anything is: the text
    // less each placeholder, plus each placeholder's literal. Every span
    // lies within the text and they are disjoint, so the subtraction cannot
    // pass zero; the sum is checked all the same.
    let rendered = spans.iter().try_fold(sql.len(), |len, span| {
        len.checked_sub((span.end - span.start) as usize)?.checked_add(literals[span.literal as usize].len())
    });
    match rendered {
        Some(len) if len <= MAX_RENDERED => {}
        _ => {
            return Err(format!(
                "an EXEC request renders to a statement longer than {MAX_RENDERED} bytes: send it as text"
            ));
        }
    }
    Ok(Exec { sql, literals, spans })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(start: u32, end: u32, literal: u16) -> Span {
        Span { start, end, literal }
    }

    /// A request that would render past [`MAX_RENDERED`] is refused before
    /// anything is allocated for it, however small the request itself is:
    /// one literal named by every placeholder multiplies.
    #[test]
    fn a_request_that_renders_too_long_is_refused_unrendered() {
        let literal = "9".repeat(4096);
        let n: u16 = 4096; // 4096 * 4096 = 16 MB rendered, from a ~45 KB request
        let sql: String = (0..n).map(|_| "$1,").collect();
        let spans: Vec<Span> = (0..n as u32).map(|i| span(i * 3, i * 3 + 2, 0)).collect();
        let request = encode(&sql, &[&literal], &spans).unwrap();
        assert!(request.len() < 64 * 1024, "the request itself is small: {}", request.len());
        let refused = decode(&request).unwrap().unwrap_err();
        assert!(refused.contains("send it as text"), "{refused}");
        // The same shape within the limit still decodes and renders.
        let fits: Vec<Span> = spans[..1024].to_vec();
        let sql_fits: String = (0..1024).map(|_| "$1,").collect();
        let exec_request = encode(&sql_fits, &[&literal], &fits).unwrap();
        let exec = decode(&exec_request).unwrap().unwrap();
        assert_eq!(exec.render().len(), 1024 * (4096 + 1));
    }

    #[test]
    fn a_request_reads_back_as_it_was_written_and_renders_its_text() {
        let sql = "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $1";
        let spans = [span(26, 28, 0), span(37, 39, 1), span(48, 50, 0)];
        let request = encode(sql, &["42", "'it''s'"], &spans).unwrap();
        let exec = decode(&request).unwrap().unwrap();
        assert_eq!(exec, Exec { sql, literals: vec!["42", "'it''s'"], spans: spans.to_vec() });
        assert_eq!(exec.render(), "SELECT * FROM t WHERE a = 42 AND b = 'it''s' AND c = 42");
    }

    #[test]
    fn no_placeholders_and_multibyte_text_survive() {
        for (sql, literals, spans) in [
            ("SELECT 1", vec![], vec![]),
            ("SELECT 'ü' || $1", vec!["'é'"], vec![span(15, 17, 0)]),
            ("", vec!["1"], vec![]),
        ] {
            let request = encode(sql, &literals, &spans).unwrap();
            let exec = decode(&request).unwrap().unwrap();
            assert_eq!((exec.sql, exec.literals.clone(), exec.spans.clone()), (sql, literals, spans));
        }
        let request = encode("SELECT 'ü' || $1", &["'é'"], &[span(15, 17, 0)]).unwrap();
        let exec = decode(&request).unwrap().unwrap();
        assert_eq!(exec.render(), "SELECT 'ü' || 'é'");
    }

    #[test]
    fn other_requests_are_not_exec_requests() {
        for request in [&b"SELECT 1"[..], b"\x01SIMPLE;SELECT 1", b"\x01EXEC", b"\x01EXECX;", b""] {
            assert!(decode(request).is_none(), "{request:?}");
        }
    }

    /// A malformed request is refused whole, whichever field is wrong, and
    /// never read as something shorter that happens to parse.
    #[test]
    fn a_malformed_request_is_refused() {
        let sql = "SELECT $1, $2";
        let good = encode(sql, &["1", "2"], &[span(7, 9, 0), span(11, 13, 1)]).unwrap();
        assert!(decode(&good).unwrap().is_ok());
        // Every truncation, and one byte too many.
        for cut in PREFIX.len()..good.len() {
            assert!(decode(&good[..cut]).unwrap().is_err(), "cut at {cut}");
        }
        let mut long = good.clone();
        long.push(0);
        assert!(decode(&long).unwrap().is_err());
        // Spans out of order, overlapping, empty, past the end, splitting a
        // character, naming a missing literal.
        for spans in [
            vec![span(11, 13, 1), span(7, 9, 0)],
            vec![span(7, 12, 0), span(11, 13, 1)],
            vec![span(7, 7, 0)],
            vec![span(11, 14, 0)],
            vec![span(7, 9, 2)],
        ] {
            let request = encode(sql, &["1", "2"], &spans).unwrap();
            assert!(decode(&request).unwrap().is_err(), "{spans:?}");
        }
        let request = encode(sql, &["1", ""], &[span(7, 9, 0), span(11, 13, 1)]).unwrap();
        assert!(decode(&request).unwrap().is_err(), "a span naming an unnamed parameter");
        let request = encode(sql, &["", "2"], &[span(11, 13, 1)]).unwrap();
        assert!(decode(&request).unwrap().is_ok(), "an unnamed parameter no span names");
        let request = encode("SELECT 'ü'", &["1"], &[span(9, 10, 0)]).unwrap();
        assert!(decode(&request).unwrap().is_err(), "a span inside a character");
        // Text and literals that are not UTF-8.
        let mut bad = PREFIX.to_vec();
        bad.extend_from_slice(&1u32.to_le_bytes());
        bad.push(0xff);
        bad.extend_from_slice(&0u16.to_le_bytes());
        bad.extend_from_slice(&0u16.to_le_bytes());
        assert!(decode(&bad).unwrap().is_err());
        // A length that claims more than the request holds.
        let mut huge = PREFIX.to_vec();
        huge.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode(&huge).unwrap().is_err());
    }
}
