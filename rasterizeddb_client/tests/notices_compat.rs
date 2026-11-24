//! A client that asks for notices, against servers that do and do not know
//! the request. No database: a rastcp server on a unix socket plays the
//! server, answering as each kind of server answers, so what is tested is the
//! client's side of the wire.
//!
//! - A server from before marked requests runs `\x01NOTICES;` as two
//!   statements and refuses it whole (a native error, `3`).
//! - A server with marked requests but not this one refuses it as unknown
//!   (a simple-query error, `4`).
//! - A server that knows it answers `SET`, then carries notices in front of
//!   the answers that have some (`5`), on that connection only.
//!
//! The client connects and runs statements against each; only the last
//! gives it notices.

#![cfg(unix)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rastcp::server::{ConnectionInfo, TcpServerBuilder};
use rastcp::session::Principal;
use rasterizeddb_client::{Client, Executor, Notice, Options};

const TEST_LIMIT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, PartialEq)]
enum Server {
    /// Refuses the request as two statements, as servers before marked
    /// requests do.
    BeforeMarks,
    /// Refuses it as an unknown marked request.
    BeforeNotices,
    /// Knows it.
    WithNotices,
}

/// A simple-query error answer, as the server writes one.
fn simple_error(code: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    for (field, value) in [(b'S', "ERROR"), (b'V', "ERROR"), (b'C', code), (b'M', message)] {
        body.push(field);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut out = vec![4u8, b'E'];
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(&body);
    out.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
    out
}

fn simple_complete(tag: &str) -> Vec<u8> {
    let mut out = vec![4u8, b'C'];
    out.extend_from_slice(&((tag.len() + 5) as i32).to_be_bytes());
    out.extend_from_slice(tag.as_bytes());
    out.push(0);
    out.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
    out
}

/// `answer` with one notice in front, as a server that knows notices sends
/// it to a connection that asked.
fn with_notice(message: &str, answer: &[u8]) -> Vec<u8> {
    let mut out = vec![5u8];
    out.extend_from_slice(&1u32.to_le_bytes());
    for field in ["NOTICE", "00000", message] {
        out.extend_from_slice(field.as_bytes());
        out.push(0);
    }
    out.extend_from_slice(answer);
    out
}

/// A count answer: `1` and a `u64`.
fn count(n: u64) -> Vec<u8> {
    let mut out = vec![1u8];
    out.extend_from_slice(&n.to_le_bytes());
    out
}

async fn serve(kind: Server, path: std::path::PathBuf) -> Arc<rastcp::server::TcpServer> {
    let server = Arc::new(TcpServerBuilder::unix(&path).build().await.expect("binds"));
    let running = server.clone();
    tokio::spawn(async move {
        running
            .run_with_session(
                kind,
                |_: &Server, _: &Arc<Principal>, _: &ConnectionInfo| AtomicBool::new(false),
                |kind: Server, _principal, asked: Arc<AtomicBool>, request: Vec<u8>| async move {
                    if request.first() == Some(&1) {
                        return match kind {
                            Server::BeforeMarks => b"\x03the native protocol runs one statement per request".to_vec(),
                            Server::BeforeNotices => simple_error(
                                "0A000",
                                "this server does not know the marked request (a request starting with byte 1)",
                            ),
                            Server::WithNotices if request == b"\x01NOTICES;" => {
                                asked.store(true, Ordering::Relaxed);
                                simple_complete("SET")
                            }
                            Server::WithNotices => simple_error("0A000", "unknown marked request"),
                        };
                    }
                    let answer = if request.starts_with(b"FAIL") {
                        b"\x03it failed".to_vec()
                    } else {
                        count(3)
                    };
                    if asked.load(Ordering::Relaxed) {
                        with_notice("said", &answer)
                    } else {
                        answer
                    }
                },
            )
            .await
    });
    server
}

async fn run_against(kind: Server) -> (Vec<Notice>, Vec<Notice>, bool) {
    let dir = std::env::temp_dir().join(format!(
        "rdb_n6f_compat_{}_{}",
        std::process::id(),
        kind as u8
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("s.sock");
    let _server = serve(kind, path.clone()).await;
    let client = Client::connect(
        &format!("unix:{}", path.display()),
        Options {
            timeout: Some(Duration::from_secs(10)),
            ..Options::default()
        },
    )
    .await
    .expect("the client connects whatever the server says about notices");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let keep = seen.clone();
    client.set_notice_handler(move |n| keep.lock().unwrap().push(n.clone()));

    let rows = client.run("UPDATE x SET y = 1").await.expect("a statement runs");
    assert_eq!(rows.rows_affected(), Some(3));
    let on_rows = rows.notices().to_vec();
    let failed = client.run("FAIL now").await.unwrap_err();
    assert_eq!(failed.server_message(), Some("it failed"));

    // A transaction's connection asks too.
    let tx = client.begin().await.expect("a transaction opens");
    let in_tx = tx.run("UPDATE x SET y = 2").await.expect("a statement in it runs");
    assert_eq!(in_tx.rows_affected(), Some(3));
    let tx_has = !in_tx.notices().is_empty();
    drop(tx);

    let handled = std::mem::take(&mut *seen.lock().unwrap());
    let _ = std::fs::remove_dir_all(&dir);
    (on_rows, handled, tx_has)
}

#[tokio::test]
async fn a_server_from_before_marked_requests_sends_no_notices() {
    let (rows, handled, tx) = tokio::time::timeout(TEST_LIMIT, run_against(Server::BeforeMarks))
        .await
        .expect("bounded");
    assert!(rows.is_empty() && handled.is_empty() && !tx);
}

#[tokio::test]
async fn a_server_from_before_notices_sends_none() {
    let (rows, handled, tx) = tokio::time::timeout(TEST_LIMIT, run_against(Server::BeforeNotices))
        .await
        .expect("bounded");
    assert!(rows.is_empty() && handled.is_empty() && !tx);
}

#[tokio::test]
async fn a_server_with_notices_sends_them_to_a_client_that_asked() {
    let (rows, handled, tx) = tokio::time::timeout(TEST_LIMIT, run_against(Server::WithNotices))
        .await
        .expect("bounded");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].message, "said");
    assert_eq!(rows[0].severity, "NOTICE");
    assert_eq!(rows[0].sqlstate, "00000");
    // The statement's, the failed statement's (its error has no room for
    // them), BEGIN's and the transaction statement's.
    assert_eq!(handled.len(), 4, "{handled:?}");
    assert!(tx);
}
