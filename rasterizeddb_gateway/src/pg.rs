//! The few pgwire messages the gateway reads or writes (§4.9 G4).
//!
//! The gateway reads framing (a type byte and a length) for every message,
//! and the contents of only: the startup packet, Query, Parse, Bind,
//! Close, SASL responses, the ReadyForQuery status and an ErrorResponse's
//! SQLSTATE. Everything else, rows included, is copied as bytes.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const SSL_REQUEST: i32 = 80_877_103;
pub const GSSENC_REQUEST: i32 = 80_877_104;
pub const CANCEL_REQUEST: i32 = 80_877_102;
pub const PROTOCOL_3: i32 = 196_608;

/// A buffered reader of frontend messages.
pub struct MsgReader {
    buf: Vec<u8>,
    start: usize,
    end: usize,
    max: usize,
}

impl MsgReader {
    pub fn new(capacity: usize, max: usize) -> MsgReader {
        MsgReader { buf: vec![0; capacity.max(512)], start: 0, end: 0, max }
    }

    /// Bytes buffered but not yet taken.
    pub fn buffered(&self) -> usize {
        self.end - self.start
    }

    /// Gives back a buffer a large message grew, while nothing is buffered
    /// (G8): called between batches.
    pub fn trim(&mut self) {
        if self.start == self.end {
            self.start = 0;
            self.end = 0;
            if self.buf.len() > 64 << 10 {
                self.buf.truncate(16 << 10);
                self.buf.shrink_to(16 << 10);
            }
        }
    }

    async fn fill<S: AsyncRead + Unpin>(&mut self, s: &mut S, need: usize) -> io::Result<()> {
        if self.start == self.end {
            self.start = 0;
            self.end = 0;
        }
        while self.end - self.start < need {
            if self.start > 0 && (self.buf.len() - self.end) < need {
                self.buf.copy_within(self.start..self.end, 0);
                self.end -= self.start;
                self.start = 0;
            }
            if self.buf.len() < need {
                self.buf.resize(need.next_power_of_two(), 0);
            }
            let n = s.read(&mut self.buf[self.end..]).await?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "client closed the connection"));
            }
            self.end += n;
        }
        Ok(())
    }

    /// The startup packet: `len:i32 ‖ code:i32 ‖ rest`, returned as
    /// `(code, rest)`.
    pub async fn startup<S: AsyncRead + Unpin>(&mut self, s: &mut S) -> io::Result<(i32, Vec<u8>)> {
        self.fill(s, 8).await?;
        let len = i32::from_be_bytes(self.buf[self.start..self.start + 4].try_into().unwrap());
        if !(8..=10_000).contains(&len) {
            return Err(protocol("invalid startup packet length"));
        }
        let len = len as usize;
        self.fill(s, len).await?;
        let code = i32::from_be_bytes(self.buf[self.start + 4..self.start + 8].try_into().unwrap());
        let rest = self.buf[self.start + 8..self.start + len].to_vec();
        self.start += len;
        Ok((code, rest))
    }

    /// The next message, whole: `(type, the message's bytes including type
    /// and length)`. Valid until the next call.
    pub async fn next<S: AsyncRead + Unpin>(&mut self, s: &mut S) -> io::Result<(u8, &[u8])> {
        self.fill(s, 5).await?;
        let t = self.buf[self.start];
        let len = i32::from_be_bytes(self.buf[self.start + 1..self.start + 5].try_into().unwrap());
        if len < 4 || len as usize > self.max {
            return Err(protocol(&format!("message of {len} bytes refused (max_message_bytes is {})", self.max)));
        }
        let total = 1 + len as usize;
        self.fill(s, total).await?;
        let at = self.start;
        self.start += total;
        Ok((t, &self.buf[at..at + total]))
    }

    /// Takes the bytes the client sent after a startup packet that turned
    /// into TLS: none may be pending, or the client sent plaintext early.
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
}

pub fn protocol(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

/// A message's body (after type and length).
pub fn body(msg: &[u8]) -> &[u8] {
    &msg[5..]
}

/// A NUL-terminated string at the start of `b`, and what follows it.
pub fn cstr(b: &[u8]) -> Option<(&str, &[u8])> {
    let nul = b.iter().position(|&c| c == 0)?;
    Some((std::str::from_utf8(&b[..nul]).ok()?, &b[nul + 1..]))
}

/// The startup packet's parameters.
pub fn startup_params(rest: &[u8]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut b = rest;
    while let Some((k, r)) = cstr(b) {
        if k.is_empty() {
            break;
        }
        let Some((v, r)) = cstr(r) else { break };
        out.push((k.to_string(), v.to_string()));
        b = r;
    }
    out
}

/// A Parse message: `(statement name, query text, raw message)`.
pub fn parse_parse(msg: &[u8]) -> Option<(&str, &str)> {
    let (name, rest) = cstr(body(msg))?;
    let (query, _) = cstr(rest)?;
    Some((name, query))
}

/// A Bind message, as much as routing needs.
pub struct Bind<'a> {
    pub statement: &'a str,
    formats: &'a [u8],
    nformats: usize,
    params: &'a [u8],
    nparams: usize,
}

pub fn parse_bind(msg: &[u8]) -> Option<Bind<'_>> {
    let (_portal, rest) = cstr(body(msg))?;
    let (statement, rest) = cstr(rest)?;
    let nformats = i16::from_be_bytes(rest.get(0..2)?.try_into().ok()?) as usize;
    let formats = rest.get(2..2 + 2 * nformats)?;
    let rest = &rest[2 + 2 * nformats..];
    let nparams = i16::from_be_bytes(rest.get(0..2)?.try_into().ok()?) as usize;
    Some(Bind { statement, formats, nformats, params: &rest[2..], nparams })
}

impl<'a> Bind<'a> {
    /// Parameter `i` (0-based): `(binary, bytes)`, `None` bytes for NULL;
    /// `None` when there is no such parameter or the message is malformed.
    pub fn param(&self, i: usize) -> Option<(bool, Option<&'a [u8]>)> {
        if i >= self.nparams {
            return None;
        }
        let mut at = 0usize;
        for j in 0..=i {
            let len = i32::from_be_bytes(self.params.get(at..at + 4)?.try_into().ok()?);
            at += 4;
            let value = if len < 0 {
                None
            } else {
                let v = self.params.get(at..at + len as usize)?;
                at += len as usize;
                Some(v)
            };
            if j == i {
                let fmt = match self.nformats {
                    0 => 0,
                    1 => i16::from_be_bytes(self.formats[0..2].try_into().ok()?),
                    _ => i16::from_be_bytes(self.formats.get(2 * i..2 * i + 2)?.try_into().ok()?),
                };
                return Some((fmt == 1, value));
            }
        }
        None
    }
}

/// A Close message: `(b'S' or b'P', name)`.
pub fn parse_close(msg: &[u8]) -> Option<(u8, &str)> {
    let b = body(msg);
    let kind = *b.first()?;
    let (name, _) = cstr(&b[1..])?;
    Some((kind, name))
}

/// A Query message's text.
pub fn parse_query(msg: &[u8]) -> Option<&str> {
    cstr(body(msg)).map(|(q, _)| q)
}

// ------------------------------------------------------------ backend side

/// Appends a backend message `t ‖ len ‖ body`.
pub fn put(out: &mut Vec<u8>, t: u8, body: &[u8]) {
    out.push(t);
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
}

pub fn ready(out: &mut Vec<u8>, status: u8) {
    put(out, b'Z', &[status]);
}

pub fn command_complete(out: &mut Vec<u8>, tag: &str) {
    let mut b = tag.as_bytes().to_vec();
    b.push(0);
    put(out, b'C', &b);
}

pub fn parameter_status(out: &mut Vec<u8>, k: &str, v: &str) {
    let mut b = Vec::with_capacity(k.len() + v.len() + 2);
    b.extend_from_slice(k.as_bytes());
    b.push(0);
    b.extend_from_slice(v.as_bytes());
    b.push(0);
    put(out, b'S', &b);
}

/// An ErrorResponse (`severity` ERROR or FATAL).
pub fn error(out: &mut Vec<u8>, severity: &str, sqlstate: &str, message: &str) {
    let mut b = Vec::with_capacity(32 + message.len());
    for (f, v) in [(b'S', severity), (b'V', severity), (b'C', sqlstate), (b'M', message)] {
        b.push(f);
        b.extend_from_slice(v.as_bytes());
        b.push(0);
    }
    b.push(0);
    put(out, b'E', &b);
}

pub fn notice(out: &mut Vec<u8>, message: &str) {
    let mut b = Vec::new();
    for (f, v) in [(b'S', "NOTICE"), (b'V', "NOTICE"), (b'C', "00000"), (b'M', message)] {
        b.push(f);
        b.extend_from_slice(v.as_bytes());
        b.push(0);
    }
    b.push(0);
    put(out, b'N', &b);
}

/// A RowDescription of text columns.
pub fn row_description(out: &mut Vec<u8>, names: &[&str]) {
    let mut b = (names.len() as i16).to_be_bytes().to_vec();
    for n in names {
        b.extend_from_slice(n.as_bytes());
        b.push(0);
        b.extend_from_slice(&0i32.to_be_bytes()); // table oid
        b.extend_from_slice(&0i16.to_be_bytes()); // attnum
        b.extend_from_slice(&25i32.to_be_bytes()); // text
        b.extend_from_slice(&(-1i16).to_be_bytes());
        b.extend_from_slice(&(-1i32).to_be_bytes());
        b.extend_from_slice(&0i16.to_be_bytes()); // text format
    }
    put(out, b'T', &b);
}

/// A DataRow of text values.
pub fn data_row(out: &mut Vec<u8>, values: &[Option<&str>]) {
    let mut b = (values.len() as i16).to_be_bytes().to_vec();
    for v in values {
        match v {
            Some(s) => {
                b.extend_from_slice(&(s.len() as i32).to_be_bytes());
                b.extend_from_slice(s.as_bytes());
            }
            None => b.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    put(out, b'D', &b);
}

/// The backend messages in `b`, as `(type, whole message)`; stops at a
/// partial one.
pub fn messages(b: &[u8]) -> impl Iterator<Item = (u8, &[u8])> {
    let mut at = 0usize;
    std::iter::from_fn(move || {
        let t = *b.get(at)?;
        let len = i32::from_be_bytes(b.get(at + 1..at + 5)?.try_into().ok()?);
        if len < 4 {
            return None;
        }
        let end = at + 1 + len as usize;
        let m = b.get(at..end)?;
        at = end;
        Some((t, m))
    })
}

/// An ErrorResponse's SQLSTATE.
pub fn error_sqlstate(msg: &[u8]) -> Option<&str> {
    let mut b = body(msg);
    while let Some(&f) = b.first() {
        if f == 0 {
            break;
        }
        let (v, rest) = cstr(&b[1..])?;
        if f == b'C' {
            return Some(v);
        }
        b = rest;
    }
    None
}

/// An ErrorResponse's message text.
pub fn error_message(msg: &[u8]) -> Option<String> {
    let mut b = body(msg);
    while let Some(&f) = b.first() {
        if f == 0 {
            break;
        }
        let (v, rest) = cstr(&b[1..])?;
        if f == b'M' {
            return Some(v.to_string());
        }
        b = rest;
    }
    None
}

/// A DataRow's single column value, for COUNT sums: `(binary?, bytes)` is
/// decided by the caller from the RowDescription.
pub fn data_row_first(msg: &[u8]) -> Option<Option<&[u8]>> {
    let b = body(msg);
    let n = i16::from_be_bytes(b.get(0..2)?.try_into().ok()?);
    if n < 1 {
        return None;
    }
    let len = i32::from_be_bytes(b.get(2..6)?.try_into().ok()?);
    if len < 0 {
        return Some(None);
    }
    Some(Some(b.get(6..6 + len as usize)?))
}

/// The format code of a RowDescription's first column.
pub fn row_description_first_format(msg: &[u8]) -> Option<i16> {
    let b = body(msg);
    let (_, rest) = cstr(b.get(2..)?)?;
    // table oid 4, attnum 2, type 4, size 2, modifier 4, format 2
    Some(i16::from_be_bytes(rest.get(16..18)?.try_into().ok()?))
}

/// The row count in a CommandComplete tag (`SELECT 5`, `UPDATE 3`,
/// `INSERT 0 2`), and the tag with it replaced.
pub fn tag_count(msg: &[u8]) -> Option<(String, u64)> {
    let (tag, _) = cstr(body(msg))?;
    let (head, n) = tag.rsplit_once(' ')?;
    Some((head.to_string(), n.parse().ok()?))
}

pub async fn write_all<S: AsyncWrite + Unpin>(s: &mut S, b: &[u8]) -> io::Result<()> {
    s.write_all(b).await?;
    s.flush().await
}
