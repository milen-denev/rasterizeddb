//! PostgreSQL's rule for an error in the extended query protocol.
//!
//! From the protocol documentation: when an error occurs, the server "discards
//! messages until a Sync message is reached, then issues ReadyForQuery and
//! returns to normal message processing". A server that keeps answering the
//! rest of the batch sends replies the client never asked for — and a client
//! reading them as answers to its *next* query decodes whatever arrives
//! against the wrong column list. For a 4-byte type that is a panic inside
//! `bytes`, not an error.
//!
//! This talks the protocol directly rather than through sqlx, so what the
//! server sends can be inspected rather than inferred.

use rasterizeddb_compat::TestServer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn msg(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn cstr(s: &str) -> Vec<u8> {
    let mut v = s.as_bytes().to_vec();
    v.push(0);
    v
}

async fn startup(stream: &mut TcpStream) {
    let mut body = 196608i32.to_be_bytes().to_vec(); // protocol 3.0
    body.extend_from_slice(&cstr("user"));
    body.extend_from_slice(&cstr("postgres"));
    body.extend_from_slice(&cstr("database"));
    body.extend_from_slice(&cstr("postgres"));
    body.push(0);
    let mut out = ((body.len() + 4) as i32).to_be_bytes().to_vec();
    out.extend_from_slice(&body);
    stream.write_all(&out).await.expect("startup");

    // Read until ReadyForQuery.
    loop {
        let tag = read_tag(stream).await.expect("startup reply");
        if tag.0 == b'Z' {
            break;
        }
    }
}

/// Reads one message, returning its tag and body.
async fn read_tag(stream: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    let mut t = [0u8; 1];
    stream.read_exact(&mut t).await.ok()?;
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.ok()?;
    let n = i32::from_be_bytes(len) as usize;
    let mut body = vec![0u8; n.saturating_sub(4)];
    if !body.is_empty() {
        stream.read_exact(&mut body).await.ok()?;
    }
    Some((t[0], body))
}

/// A batch whose Bind fails, with Describe and Execute queued behind it.
#[tokio::test]
async fn messages_after_an_error_are_discarded_until_sync() {
    let server = TestServer::start().await.expect("server should start");
    let mut stream = TcpStream::connect(("127.0.0.1", server.port()))
        .await
        .expect("connect");
    startup(&mut stream).await;

    // A statement taking one parameter.
    let mut parse = cstr("s1");
    parse.extend_from_slice(&cstr("SELECT $1::int AS n"));
    parse.extend_from_slice(&0i16.to_be_bytes()); // no type hints
    let mut batch = msg(b'P', &parse);

    // Bind supplying ZERO parameters for a statement that needs one: an error.
    let mut bind = cstr("");
    bind.extend_from_slice(&cstr("s1"));
    bind.extend_from_slice(&0i16.to_be_bytes()); // format codes
    bind.extend_from_slice(&0i16.to_be_bytes()); // parameter count = 0
    bind.extend_from_slice(&0i16.to_be_bytes()); // result formats
    batch.extend_from_slice(&msg(b'B', &bind));

    // Queued behind the failing Bind — PostgreSQL never answers these.
    let mut describe = vec![b'P'];
    describe.extend_from_slice(&cstr(""));
    batch.extend_from_slice(&msg(b'D', &describe));

    let mut execute = cstr("");
    execute.extend_from_slice(&0i32.to_be_bytes());
    batch.extend_from_slice(&msg(b'E', &execute));

    // A second, perfectly valid statement queued behind the failing one.
    // PostgreSQL discards it: it is after the error and before the Sync.
    let mut parse2 = cstr("s2");
    parse2.extend_from_slice(&cstr("SELECT 1 AS ok"));
    parse2.extend_from_slice(&0i16.to_be_bytes());
    batch.extend_from_slice(&msg(b'P', &parse2));

    let mut bind2 = cstr("p2");
    bind2.extend_from_slice(&cstr("s2"));
    bind2.extend_from_slice(&0i16.to_be_bytes());
    bind2.extend_from_slice(&0i16.to_be_bytes());
    bind2.extend_from_slice(&0i16.to_be_bytes());
    batch.extend_from_slice(&msg(b'B', &bind2));

    let mut execute2 = cstr("p2");
    execute2.extend_from_slice(&0i32.to_be_bytes());
    batch.extend_from_slice(&msg(b'E', &execute2));

    batch.extend_from_slice(&msg(b'S', &[]));

    stream.write_all(&batch).await.expect("send batch");

    // Collect everything up to and including ReadyForQuery.
    let mut tags = Vec::new();
    loop {
        match read_tag(&mut stream).await {
            Some((tag, _)) => {
                tags.push(tag as char);
                if tag == b'Z' {
                    break;
                }
            }
            None => break,
        }
    }

    println!("server sent after the failing Bind: {:?}", tags);

    let error_at = tags.iter().position(|c| *c == 'E');
    assert!(error_at.is_some(), "the Bind must fail: {:?}", tags);
    let after: Vec<char> = tags[error_at.unwrap() + 1..].to_vec();

    // Everything between the error and ReadyForQuery must be nothing at all.
    assert_eq!(
        after,
        vec!['Z'],
        "after an error the server must discard messages until Sync and send only \
         ReadyForQuery, but it also sent {:?} — a client reads these as answers to its \
         next query",
        after.iter().filter(|c| **c != 'Z').collect::<Vec<_>>()
    );
}

/// A simple query that fails must not open a discard window for the batch
/// after it.
///
/// A simple `Query` is its own unit: it ends with `ReadyForQuery` whether it
/// failed or not, and the protocol's discard-until-`Sync` rule is about the
/// extended protocol only. When the failure was left recorded as a batch
/// error, the client's *next* extended batch was swallowed whole —
/// `Parse`, `Bind`, `Execute` dropped without a reply, then `ReadyForQuery`
/// at the `Sync` — so a valid query came back as no rows at all.
#[tokio::test]
async fn a_failed_simple_query_does_not_swallow_the_next_batch() {
    use sqlx::{Connection, Executor};

    let server = TestServer::start().await.expect("server should start");
    let mut c = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    c.execute("CREATE TABLE after_simple_error (id INT PRIMARY KEY)").await.unwrap();
    c.execute("INSERT INTO after_simple_error (id) VALUES (7)").await.unwrap();

    // `execute` with a bare string and no arguments is a simple query.
    c.execute("SELECT * FROM no_such_table_here")
        .await
        .expect_err("the table does not exist");

    // `query_scalar` always goes through Parse/Bind/Execute/Sync.
    let id: i32 = sqlx::query_scalar("SELECT id FROM after_simple_error")
        .fetch_one(&mut c)
        .await
        .expect("the batch after a failed simple query must be answered");
    assert_eq!(id, 7);
}
