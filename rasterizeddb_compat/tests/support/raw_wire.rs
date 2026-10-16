//! A bare pgwire client that reports the shape of what came back.
//!
//! sqlx and tokio-postgres both size a row by its RowDescription and never
//! look at how many fields a DataRow carried, so a row holding one column too
//! many decodes cleanly through either of them and only libpq notices ("unexpected
//! field count in D message"). This reads the frames itself and keeps every
//! field of every DataRow, so a test can compare the two counts.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// What one statement returned: the field count its RowDescription declared,
/// and every DataRow's fields as text (`None` for NULL).
#[derive(Debug)]
pub struct Shape {
    pub described: Option<usize>,
    pub rows: Vec<Vec<Option<String>>>,
}

pub struct RawConn {
    stream: TcpStream,
}

/// Every read is bounded, so a server that stops answering fails the test
/// instead of hanging it.
const READ_LIMIT: Duration = Duration::from_secs(30);

impl RawConn {
    /// Startup as `rasterized` on `postgres`, which the harness serves with
    /// `trust`, then wait for the first ReadyForQuery.
    pub async fn connect(port: u16) -> RawConn {
        let stream = TcpStream::connect(("127.0.0.1", port)).await.expect("connect");
        let mut conn = RawConn { stream };
        let mut body = Vec::new();
        body.extend_from_slice(&196608i32.to_be_bytes());
        for s in ["user", "rasterized", "database", "postgres"] {
            body.extend_from_slice(s.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut msg = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        msg.extend_from_slice(&body);
        conn.stream.write_all(&msg).await.expect("startup");
        loop {
            let (tag, body) = conn.read_msg().await;
            match tag {
                b'Z' => break,
                b'E' => panic!("startup refused: {}", error_text(&body)),
                _ => {}
            }
        }
        conn
    }

    async fn send(&mut self, tag: u8, body: &[u8]) {
        let mut msg = vec![tag];
        msg.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        msg.extend_from_slice(body);
        self.stream.write_all(&msg).await.expect("send");
    }

    async fn read_msg(&mut self) -> (u8, Vec<u8>) {
        tokio::time::timeout(READ_LIMIT, async {
            let tag = self.stream.read_u8().await.expect("read tag");
            let len = self.stream.read_i32().await.expect("read length") as usize;
            let mut body = vec![0u8; len - 4];
            self.stream.read_exact(&mut body).await.expect("read body");
            (tag, body)
        })
        .await
        .expect("the server stopped answering")
    }

    /// Reads up to ReadyForQuery, collecting the description and the rows.
    async fn collect(&mut self) -> Result<Shape, String> {
        let mut shape = Shape { described: None, rows: Vec::new() };
        let mut error = None;
        loop {
            let (tag, body) = self.read_msg().await;
            match tag {
                b'T' => {
                    shape.described = Some(i16::from_be_bytes([body[0], body[1]]) as usize);
                }
                b'D' => shape.rows.push(data_row(&body)),
                b'E' => error = Some(error_text(&body)),
                b'Z' => break,
                _ => {}
            }
        }
        match error {
            Some(e) => Err(e),
            None => Ok(shape),
        }
    }

    /// One statement over the simple protocol.
    pub async fn simple(&mut self, sql: &str) -> Result<Shape, String> {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        self.send(b'Q', &body).await;
        self.collect().await
    }

    /// Parses `sql` once as the named statement `name`.
    pub async fn parse(&mut self, name: &str, sql: &str) -> Result<(), String> {
        let mut body = Vec::new();
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(sql.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i16.to_be_bytes());
        self.send(b'P', &body).await;
        self.send(b'S', &[]).await;
        self.collect().await.map(|_| ())
    }

    /// Bind, Describe(portal), Execute, Sync of a parsed statement with no
    /// parameters, in text format: the sequence `pgbench -M prepared` sends.
    pub async fn execute(&mut self, name: &str) -> Result<Shape, String> {
        let mut bind = Vec::new();
        bind.push(0); // unnamed portal
        bind.extend_from_slice(name.as_bytes());
        bind.push(0);
        bind.extend_from_slice(&0i16.to_be_bytes()); // parameter formats
        bind.extend_from_slice(&0i16.to_be_bytes()); // parameters
        bind.extend_from_slice(&0i16.to_be_bytes()); // result formats: text
        self.send(b'B', &bind).await;
        self.send(b'D', &[b'P', 0]).await;
        let mut execute = vec![0u8];
        execute.extend_from_slice(&0i32.to_be_bytes());
        self.send(b'E', &execute).await;
        self.send(b'S', &[]).await;
        self.collect().await
    }
}

fn data_row(body: &[u8]) -> Vec<Option<String>> {
    let count = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut at = 2;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let len = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
        at += 4;
        if len < 0 {
            fields.push(None);
        } else {
            let len = len as usize;
            fields.push(Some(String::from_utf8_lossy(&body[at..at + len]).into_owned()));
            at += len;
        }
    }
    assert_eq!(at, body.len(), "a DataRow's fields do not account for its body");
    fields
}

fn error_text(body: &[u8]) -> String {
    body.split(|b| *b == 0)
        .find(|f| f.first() == Some(&b'M'))
        .map(|f| String::from_utf8_lossy(&f[1..]).into_owned())
        .unwrap_or_else(|| "unknown error".to_string())
}
