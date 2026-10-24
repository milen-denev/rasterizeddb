//! A frame-level PostgreSQL v3 client for replaying a driver's exact message
//! sequences (slice B8, `docs/persistent-indexes.md` §13, V10 case 14 and the
//! driver gate).
//!
//! It writes the frontend frames a driver writes (`P`, `B`, `D`, `E`, `S`,
//! `H`, `C`, `Q`), in one `write_all` when the driver pipelines them, and
//! reads every backend message back in order. A reply is rendered as a short
//! token so two servers' answers can be compared exactly:
//!
//! - `1` ParseComplete, `2` BindComplete, `3` CloseComplete, `n` NoData,
//!   `s` PortalSuspended, `I` EmptyQueryResponse;
//! - `t[23,25]` ParameterDescription (type OIDs);
//! - `T[id/23/1,a/25/0]` RowDescription (name / type OID / format code);
//! - `D[00000001|'x'|NULL]` DataRow (a cell of printable ASCII as `'text'`,
//!   anything else as hex: a binary int4 1 is `00000001`);
//! - `C(SELECT 1)` CommandComplete;
//! - `E(23505)` ErrorResponse by sqlstate; for 0A000 the message and the
//!   routine field too (`R=`), since pgjdbc decides whether to re-prepare by
//!   the routine (`QueryExecutorBase.willHealViaReparse`);
//! - `Z(I)` ReadyForQuery with its transaction status.
//!
//! Left out of the rendering, as documented differences between this engine
//! and PostgreSQL: ParameterStatus (`S`) and BackendKeyData (`K`) at startup
//! and after `SET`, NoticeResponse (`N`), a RowDescription's table OID,
//! column number, type size and modifier, and an error's message text other
//! than 0A000's.
//!
//! Startup speaks `trust` only (the harness's server and a PostgreSQL 17
//! container started with `POSTGRES_HOST_AUTH_METHOD=trust`). Every read is
//! bounded so a server that stops answering fails the test, never hangs it.

#![allow(dead_code)]

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// One read's bound: past it the server is taken to have stopped answering.
pub const READ_LIMIT: Duration = Duration::from_secs(10);

/// A frontend frame, already encoded.
#[derive(Clone)]
pub struct Frame(pub Vec<u8>);

fn frame(tag: u8, body: &[u8]) -> Frame {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
    Frame(out)
}

fn cstr(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
}

/// Parse: statement name (`""` is the unnamed statement), text, declared
/// parameter type OIDs.
pub fn parse(name: &str, sql: &str, oids: &[i32]) -> Frame {
    let mut b = Vec::new();
    cstr(&mut b, name);
    cstr(&mut b, sql);
    b.extend_from_slice(&(oids.len() as i16).to_be_bytes());
    for o in oids {
        b.extend_from_slice(&o.to_be_bytes());
    }
    frame(b'P', &b)
}

/// A parameter value as a driver sends it.
#[derive(Clone)]
pub enum Param {
    /// Text format.
    Text(String),
    /// Binary int4, as pgjdbc's `setInt` and npgsql's `int` send it.
    Int4(i32),
    /// Binary text (the UTF-8 bytes), as npgsql sends a `string`.
    BinText(String),
    Null,
}

impl Param {
    fn format(&self) -> i16 {
        match self {
            Param::Text(_) | Param::Null => 0,
            Param::Int4(_) | Param::BinText(_) => 1,
        }
    }
}

/// Bind: portal, statement, the parameters (each with its own format code),
/// and the result format codes exactly as the driver sends them (`[]` means
/// text for every column, `[1]` binary for every column, one per column
/// otherwise).
pub fn bind(portal: &str, stmt: &str, params: &[Param], result_formats: &[i16]) -> Frame {
    let mut b = Vec::new();
    cstr(&mut b, portal);
    cstr(&mut b, stmt);
    b.extend_from_slice(&(params.len() as i16).to_be_bytes());
    for p in params {
        b.extend_from_slice(&p.format().to_be_bytes());
    }
    b.extend_from_slice(&(params.len() as i16).to_be_bytes());
    for p in params {
        match p {
            Param::Null => b.extend_from_slice(&(-1i32).to_be_bytes()),
            Param::Text(s) | Param::BinText(s) => {
                b.extend_from_slice(&(s.len() as i32).to_be_bytes());
                b.extend_from_slice(s.as_bytes());
            }
            Param::Int4(v) => {
                b.extend_from_slice(&4i32.to_be_bytes());
                b.extend_from_slice(&v.to_be_bytes());
            }
        }
    }
    b.extend_from_slice(&(result_formats.len() as i16).to_be_bytes());
    for f in result_formats {
        b.extend_from_slice(&f.to_be_bytes());
    }
    frame(b'B', &b)
}

/// Describe of a statement.
pub fn describe_s(name: &str) -> Frame {
    let mut b = vec![b'S'];
    cstr(&mut b, name);
    frame(b'D', &b)
}

/// Describe of a portal.
pub fn describe_p(name: &str) -> Frame {
    let mut b = vec![b'P'];
    cstr(&mut b, name);
    frame(b'D', &b)
}

/// Execute a portal, `max_rows` 0 meaning all.
pub fn execute(portal: &str, max_rows: i32) -> Frame {
    let mut b = Vec::new();
    cstr(&mut b, portal);
    b.extend_from_slice(&max_rows.to_be_bytes());
    frame(b'E', &b)
}

/// Close a statement.
pub fn close_s(name: &str) -> Frame {
    let mut b = vec![b'S'];
    cstr(&mut b, name);
    frame(b'C', &b)
}

pub fn sync() -> Frame {
    frame(b'S', &[])
}

pub fn flush() -> Frame {
    frame(b'H', &[])
}

/// A simple query.
pub fn query(sql: &str) -> Frame {
    let mut b = Vec::new();
    cstr(&mut b, sql);
    frame(b'Q', &b)
}

/// One backend message, decoded.
#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    ParseComplete,
    BindComplete,
    CloseComplete,
    NoData,
    PortalSuspended,
    EmptyQuery,
    ParamDesc(Vec<i32>),
    /// (name, type OID, format code) per column.
    RowDesc(Vec<(String, i32, i16)>),
    DataRow(Vec<Option<Vec<u8>>>),
    Complete(String),
    Error { code: String, message: String, routine: Option<String> },
    Notice(String),
    Ready(u8),
    /// ParameterStatus, BackendKeyData and anything else: kept for a test
    /// that wants it, never rendered.
    Other(u8),
}

fn render_cell(cell: &Option<Vec<u8>>) -> String {
    match cell {
        None => "NULL".to_string(),
        Some(b) if !b.is_empty() && b.iter().all(|c| (0x20..0x7f).contains(c)) => {
            format!("'{}'", String::from_utf8_lossy(b))
        }
        Some(b) if b.is_empty() => "''".to_string(),
        Some(b) => b.iter().map(|c| format!("{c:02x}")).collect(),
    }
}

impl Reply {
    /// The token this reply is compared by, or `None` for one left out.
    pub fn token(&self) -> Option<String> {
        Some(match self {
            Reply::ParseComplete => "1".into(),
            Reply::BindComplete => "2".into(),
            Reply::CloseComplete => "3".into(),
            Reply::NoData => "n".into(),
            Reply::PortalSuspended => "s".into(),
            Reply::EmptyQuery => "I".into(),
            Reply::ParamDesc(o) => format!(
                "t[{}]",
                o.iter().map(|o| o.to_string()).collect::<Vec<_>>().join(",")
            ),
            Reply::RowDesc(cols) => format!(
                "T[{}]",
                cols.iter()
                    .map(|(n, o, f)| format!("{n}/{o}/{f}"))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Reply::DataRow(cells) => format!(
                "D[{}]",
                cells.iter().map(render_cell).collect::<Vec<_>>().join("|")
            ),
            Reply::Complete(tag) => format!("C({tag})"),
            Reply::Error { code, message, routine } if code == "0A000" => format!(
                "E(0A000 M={message} R={})",
                routine.as_deref().unwrap_or("-")
            ),
            Reply::Error { code, .. } => format!("E({code})"),
            Reply::Ready(s) => format!("Z({})", *s as char),
            Reply::Notice(_) | Reply::Other(_) => return None,
        })
    }
}

/// The compared rendering of a run of replies, space-separated.
pub fn render(replies: &[Reply]) -> String {
    replies.iter().filter_map(Reply::token).collect::<Vec<_>>().join(" ")
}

fn decode(tag: u8, body: &[u8]) -> Reply {
    let i16_at = |at: usize| i16::from_be_bytes([body[at], body[at + 1]]);
    let i32_at = |at: usize| i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
    match tag {
        b'1' => Reply::ParseComplete,
        b'2' => Reply::BindComplete,
        b'3' => Reply::CloseComplete,
        b'n' => Reply::NoData,
        b's' => Reply::PortalSuspended,
        b'I' => Reply::EmptyQuery,
        b't' => {
            let n = i16_at(0) as usize;
            Reply::ParamDesc((0..n).map(|i| i32_at(2 + 4 * i)).collect())
        }
        b'T' => {
            let n = i16_at(0) as usize;
            let mut at = 2;
            let mut cols = Vec::with_capacity(n);
            for _ in 0..n {
                let end = at + body[at..].iter().position(|b| *b == 0).unwrap();
                let name = String::from_utf8_lossy(&body[at..end]).into_owned();
                // table oid (4), column number (2), type oid (4), size (2),
                // modifier (4), format (2)
                let oid = i32_at(end + 1 + 6);
                let format = i16_at(end + 1 + 16);
                cols.push((name, oid, format));
                at = end + 1 + 18;
            }
            Reply::RowDesc(cols)
        }
        b'D' => {
            let n = i16_at(0) as usize;
            let mut at = 2;
            let mut cells = Vec::with_capacity(n);
            for _ in 0..n {
                let len = i32_at(at);
                at += 4;
                if len < 0 {
                    cells.push(None);
                } else {
                    cells.push(Some(body[at..at + len as usize].to_vec()));
                    at += len as usize;
                }
            }
            assert_eq!(at, body.len(), "a DataRow's cells do not account for its body");
            Reply::DataRow(cells)
        }
        b'C' => Reply::Complete(String::from_utf8_lossy(&body[..body.len().saturating_sub(1)]).into_owned()),
        b'E' | b'N' => {
            let mut code = String::new();
            let mut message = String::new();
            let mut routine = None;
            for f in body.split(|b| *b == 0) {
                match f.first() {
                    Some(b'C') => code = String::from_utf8_lossy(&f[1..]).into_owned(),
                    Some(b'M') => message = String::from_utf8_lossy(&f[1..]).into_owned(),
                    Some(b'R') => routine = Some(String::from_utf8_lossy(&f[1..]).into_owned()),
                    _ => {}
                }
            }
            if tag == b'E' {
                Reply::Error { code, message, routine }
            } else {
                Reply::Notice(code)
            }
        }
        b'Z' => Reply::Ready(body[0]),
        other => Reply::Other(other),
    }
}

/// One connection.
pub struct Wire {
    stream: TcpStream,
    addr: String,
    /// BackendKeyData, for a CancelRequest.
    pub pid: i32,
    pub secret: i32,
}

impl Wire {
    /// Startup with `params` (user and database included) and `trust`.
    pub async fn connect(addr: &str, params: &[(&str, &str)]) -> Wire {
        let stream = tokio::time::timeout(READ_LIMIT, TcpStream::connect(addr))
            .await
            .expect("connect timed out")
            .expect("connect");
        stream.set_nodelay(true).ok();
        let mut w = Wire { stream, addr: addr.to_string(), pid: 0, secret: 0 };
        let mut body = 196608i32.to_be_bytes().to_vec();
        for (k, v) in params {
            cstr(&mut body, k);
            cstr(&mut body, v);
        }
        body.push(0);
        let mut msg = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        msg.extend_from_slice(&body);
        w.stream.write_all(&msg).await.expect("startup");
        loop {
            let (tag, body) = w.read_raw().await;
            match tag {
                b'R' => {
                    let code = i32::from_be_bytes(body[..4].try_into().unwrap());
                    assert_eq!(code, 0, "only trust is spoken here (authentication request {code})");
                }
                b'K' => {
                    w.pid = i32::from_be_bytes(body[..4].try_into().unwrap());
                    w.secret = i32::from_be_bytes(body[4..8].try_into().unwrap());
                }
                b'E' => panic!("startup refused: {:?}", decode(tag, &body)),
                b'Z' => break,
                _ => {}
            }
        }
        w
    }

    async fn read_raw(&mut self) -> (u8, Vec<u8>) {
        tokio::time::timeout(READ_LIMIT, self.read_raw_unbounded())
            .await
            .expect("the server stopped answering")
    }

    async fn read_raw_unbounded(&mut self) -> (u8, Vec<u8>) {
        let tag = self.stream.read_u8().await.expect("read tag");
        let len = self.stream.read_i32().await.expect("read length") as usize;
        let mut body = vec![0u8; len - 4];
        self.stream.read_exact(&mut body).await.expect("read body");
        (tag, body)
    }

    /// Writes every frame in one write, as a pipelining driver does.
    pub async fn write(&mut self, frames: &[Frame]) {
        let mut buf = Vec::new();
        for f in frames {
            buf.extend_from_slice(&f.0);
        }
        self.stream.write_all(&buf).await.expect("write");
        self.stream.flush().await.expect("flush");
    }

    /// The next backend message, bounded by [`READ_LIMIT`].
    pub async fn next(&mut self) -> Reply {
        let (tag, body) = self.read_raw().await;
        decode(tag, &body)
    }

    /// The next backend message if one arrives within `d`. A message cut by
    /// the timeout would desynchronise the stream, so a test only uses this
    /// to show that nothing arrived, then reads on with [`Wire::next`].
    pub async fn next_within(&mut self, d: Duration) -> Option<Reply> {
        match tokio::time::timeout(d, self.stream.read_u8()).await {
            Err(_) => None,
            Ok(tag) => {
                let tag = tag.expect("read tag");
                let len = self.stream.read_i32().await.expect("read length") as usize;
                let mut body = vec![0u8; len - 4];
                self.stream.read_exact(&mut body).await.expect("read body");
                Some(decode(tag, &body))
            }
        }
    }

    /// Every message up to and including ReadyForQuery.
    pub async fn until_ready(&mut self) -> Vec<Reply> {
        let mut out = Vec::new();
        loop {
            let r = self.next().await;
            let done = matches!(r, Reply::Ready(_));
            out.push(r);
            if done {
                return out;
            }
        }
    }

    /// Every message up to and including the first one matching `stop` (for
    /// a pipeline with no Sync yet, answered by a Flush).
    pub async fn until(&mut self, stop: impl Fn(&Reply) -> bool) -> Vec<Reply> {
        let mut out = Vec::new();
        loop {
            let r = self.next().await;
            let done = stop(&r);
            out.push(r);
            if done {
                return out;
            }
        }
    }

    /// Writes `frames` and reads to ReadyForQuery.
    pub async fn exchange(&mut self, frames: &[Frame]) -> Vec<Reply> {
        self.write(frames).await;
        self.until_ready().await
    }

    /// A simple query, its replies.
    pub async fn simple(&mut self, sql: &str) -> Vec<Reply> {
        self.exchange(&[query(sql)]).await
    }

    /// A simple query that must not fail.
    pub async fn simple_ok(&mut self, sql: &str) -> Vec<Reply> {
        let replies = self.simple(sql).await;
        if let Some(e) = replies.iter().find(|r| matches!(r, Reply::Error { .. })) {
            panic!("{sql}: {e:?}");
        }
        replies
    }

    /// A CancelRequest for this connection's running statement, from a
    /// connection of its own.
    pub async fn cancel(&self) {
        let mut s = TcpStream::connect(&self.addr).await.expect("cancel connect");
        let mut msg = 16i32.to_be_bytes().to_vec();
        msg.extend_from_slice(&80877102i32.to_be_bytes());
        msg.extend_from_slice(&self.pid.to_be_bytes());
        msg.extend_from_slice(&self.secret.to_be_bytes());
        s.write_all(&msg).await.expect("cancel request");
        // The server closes it without a reply.
        let mut sink = Vec::new();
        let _ = tokio::time::timeout(READ_LIMIT, s.read_to_end(&mut sink)).await;
    }
}

/// The first DataRow's cells as text (a test reading back a count or ids).
pub fn text_rows(replies: &[Reply]) -> Vec<Vec<Option<String>>> {
    replies
        .iter()
        .filter_map(|r| match r {
            Reply::DataRow(cells) => Some(
                cells
                    .iter()
                    .map(|c| c.as_ref().map(|b| String::from_utf8_lossy(b).into_owned()))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

/// The sqlstate of the first error among `replies`, if any.
pub fn error_code(replies: &[Reply]) -> Option<String> {
    replies.iter().find_map(|r| match r {
        Reply::Error { code, .. } => Some(code.clone()),
        _ => None,
    })
}
