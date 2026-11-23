//! Frontend message lengths are capped as PostgreSQL caps them, and checked
//! before anything is allocated.
//!
//! After authentication the length a client sent used to be allocated as
//! asked, up to 2 GB per message. Now: Query, Parse, Bind, FunctionCall and
//! CopyData may be up to `PQ_LARGE_MESSAGE_LIMIT` (0x3fffffff); every other
//! message up to `PQ_SMALL_MESSAGE_LIMIT` (10,000). Past that the server sends
//! FATAL 08P01 "invalid message length" and closes the connection. Within the
//! limit the buffer grows with the bytes that actually arrive, so a client
//! claiming a huge message and sending nothing pins next to nothing.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

fn rss_mib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.split_whitespace().next()?.parse::<u64>().ok())
        .unwrap_or(0)
        / 1024
}

/// A trust-authenticated raw session, read up to its first ReadyForQuery.
fn session(server: &TestServer) -> TcpStream {
    let url = server.url();
    let database = url.rsplit('/').next().unwrap().split('?').next().unwrap().to_string();
    let mut s = TcpStream::connect(("127.0.0.1", server.port())).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut params = Vec::new();
    for (k, v) in [("user", "rasterized"), ("database", database.as_str())] {
        params.extend_from_slice(k.as_bytes());
        params.push(0);
        params.extend_from_slice(v.as_bytes());
        params.push(0);
    }
    params.push(0);
    let mut startup = ((params.len() + 8) as i32).to_be_bytes().to_vec();
    startup.extend_from_slice(&196_608i32.to_be_bytes());
    startup.extend_from_slice(&params);
    s.write_all(&startup).unwrap();
    loop {
        let (tag, _) = message(&mut s).expect("startup reply");
        if tag == b'Z' {
            return s;
        }
    }
}

fn message(s: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    let mut head = [0u8; 5];
    s.read_exact(&mut head).ok()?;
    let len = i32::from_be_bytes(head[1..5].try_into().unwrap()) as usize - 4;
    let mut body = vec![0u8; len];
    s.read_exact(&mut body).ok()?;
    Some((head[0], body))
}

/// Sends a header claiming `len`, no body, and returns what came back before
/// the server closed the connection.
fn claim(server: &TestServer, tag: u8, len: i32) -> (Vec<(u8, Vec<u8>)>, bool) {
    let mut s = session(server);
    let mut head = vec![tag];
    head.extend_from_slice(&len.to_be_bytes());
    s.write_all(&head).unwrap();
    let mut got = Vec::new();
    while let Some(m) = message(&mut s) {
        got.push(m);
    }
    // `message` returns None on EOF (closed) or on the read timeout (open).
    let mut probe = [0u8; 1];
    let closed = matches!(s.read(&mut probe), Ok(0));
    (got, closed)
}

fn is_fatal_08p01(m: &(u8, Vec<u8>)) -> bool {
    let text = String::from_utf8_lossy(&m.1);
    m.0 == b'E' && text.contains("FATAL") && text.contains("08P01") && text.contains("invalid message length")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_messages_are_refused_before_allocation() {
    let server = TestServer::start().await.expect("start");

    // One byte over each limit: refused, FATAL 08P01, closed. The body is
    // never sent, so a server that waited to read it would time out instead.
    for (tag, len) in [
        (b'Q', 0x3fff_ffff + 4 + 1),
        (b'P', 0x3fff_ffff + 4 + 1),
        (b'S', 10_000 + 4 + 1),
        (b'E', 10_000 + 4 + 1),
        (b'D', 10_000 + 4 + 1),
        (b'Q', 3),
    ] {
        let (got, closed) = tokio::task::block_in_place(|| claim(&server, tag, len));
        assert!(
            got.iter().any(is_fatal_08p01),
            "{} with length {len}: expected FATAL 08P01, got {:?}",
            tag as char,
            got.iter().map(|m| (m.0 as char, String::from_utf8_lossy(&m.1).to_string())).collect::<Vec<_>>()
        );
        assert!(closed, "{} with length {len}: the connection stayed open", tag as char);
    }

    // A legal but huge claim with nothing behind it pins next to nothing:
    // the buffer grows with the bytes that arrive, not with the claim.
    let before = rss_mib();
    let mut held = Vec::new();
    for _ in 0..4 {
        let mut s = tokio::task::block_in_place(|| session(&server));
        let mut head = vec![b'Q'];
        head.extend_from_slice(&(900 * 1024 * 1024i32).to_be_bytes());
        s.write_all(&head).unwrap();
        held.push(s);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let grew = rss_mib().saturating_sub(before);
    assert!(grew < 200, "four 900 MB claims with no body grew RSS by {grew} MiB");
    drop(held);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ten_megabyte_insert_still_works() {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    conn.execute("CREATE TABLE big_text (id INT NOT NULL, t TEXT NOT NULL)").await.expect("create");
    let payload = "x".repeat(10 * 1024 * 1024);
    let sql = format!("INSERT INTO big_text (id, t) VALUES (1, '{payload}')");
    assert!(sql.len() > 10_000_000);
    conn.execute(AssertSqlSafe(sql)).await.expect("a 10 MB statement is legal");
    let row = sqlx::query("SELECT t FROM big_text WHERE id = 1")
        .fetch_one(&mut conn)
        .await
        .expect("read back");
    let t: String = row.try_get(0).expect("text");
    assert!(t == payload, "the 10 MB value came back {} bytes long", t.len());
}
