//! Connection-level bounds, driven over the wire: the session cap, the
//! handshake timeout, TCP keepalive, and the two idle timeouts.
//!
//! Each of these defends the server against a client that does too little
//! rather than too much — opens sockets and never finishes them, opens a
//! transaction and walks away — so most of these tests are about what happens
//! while nothing is sent. The timeouts are a few hundred milliseconds so the
//! waits stay short; every wait on the server is itself bounded, so a server
//! that never acts fails the test instead of hanging it.

use std::time::Duration;

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::{
    auth::AuthMethod,
    core::database::DEFAULT_DATABASE_NAME,
    pgwire::{ConnectionLimits, TlsMode},
};
use sqlx::{Connection, Executor, PgConnection, Row};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// How long a test waits for the server to do something it should do within
/// a few hundred milliseconds. Long enough to absorb a loaded machine, short
/// enough that a server that never acts fails promptly.
const BOUND: Duration = Duration::from_secs(5);

fn limits() -> ConnectionLimits {
    ConnectionLimits::default()
}

/// The SQLSTATE of a failed statement or connect, or a description of why
/// there is none.
fn sqlstate<T: std::fmt::Debug>(result: Result<T, sqlx::Error>) -> String {
    match result {
        Ok(v) => format!("succeeded: {:?}", v),
        Err(e) => match e.as_database_error() {
            Some(db) => db.code().map(|c| c.to_string()).unwrap_or_else(|| format!("no code: {}", db)),
            None => format!("not a database error: {}", e),
        },
    }
}

/// Connects one session, retrying a refusal for a short while.
///
/// `TestServer::start` probes the port with a bare connect that it drops at
/// once, and the server holds that probe's slot until it reads the EOF. A
/// test with a cap of one or two could otherwise lose a slot to it.
async fn connect_admitted(url: &str) -> PgConnection {
    for _ in 0..100 {
        match PgConnection::connect(url).await {
            Ok(c) => return c,
            Err(e) if e.as_database_error().and_then(|d| d.code()).as_deref() == Some("53300") => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(e) => panic!("connect failed: {}", e),
        }
    }
    panic!("a slot never came free");
}

/// Runs a trivial statement and checks the answer, so a session that has
/// been closed or wedged fails here rather than later.
///
/// A simple query, read in text: the builtin `SELECT 1` answers in text
/// whatever format an extended-protocol client asks for, so a binary decode
/// of it would read the byte `'1'` as 49.
async fn served(c: &mut PgConnection, why: &str) {
    let row = c.fetch_one(sqlx::raw_sql("SELECT 1")).await.expect(why);
    assert_eq!(row.get::<i32, _>(0), 1, "{}", why);
}

/// A v3 StartupMessage for the default database.
fn startup_packet(user: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&196608i32.to_be_bytes());
    for (k, v) in [("user", user), ("database", DEFAULT_DATABASE_NAME)] {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut packet = ((body.len() + 4) as i32).to_be_bytes().to_vec();
    packet.extend_from_slice(&body);
    packet
}

/// Reads until the server closes the socket, returning how long that took
/// and whatever it sent first. A reset counts as closed.
async fn time_until_closed(stream: &mut tokio::net::TcpStream) -> (Duration, Vec<u8>) {
    let started = std::time::Instant::now();
    let mut sent = Vec::new();
    let mut buf = [0u8; 512];
    loop {
        match tokio::time::timeout(BOUND, stream.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) => return (started.elapsed(), sent),
            Ok(Ok(n)) => sent.extend_from_slice(&buf[..n]),
            Err(_) => panic!("the server kept the socket open for {:?}", BOUND),
        }
    }
}

// --- max_connections -------------------------------------------------------

/// Over the cap, a connection is refused with PostgreSQL's code and message —
/// which is what pools recognise and back off on — and a slot freed by a
/// closing session is usable again.
#[tokio::test(flavor = "multi_thread")]
async fn a_connection_over_the_cap_is_refused_with_53300() {
    let server = TestServer::start_with_limits(ConnectionLimits { max_connections: 2, ..limits() })
        .await
        .expect("server should start");

    let first = connect_admitted(&server.url()).await;
    let mut second = connect_admitted(&server.url()).await;

    let refused = PgConnection::connect(&server.url()).await;
    let err = refused.expect_err("a third session must be refused under a cap of two");
    let db = err.as_database_error().expect("the refusal must be a server error, not a dropped socket");
    assert_eq!(db.code().as_deref(), Some("53300"), "{}", db);
    assert_eq!(db.message(), "sorry, too many clients already");

    // The two admitted sessions are unaffected by the refusal.
    served(&mut second, "still served").await;

    first.close().await.expect("close");
    let mut third = connect_admitted(&server.url()).await;
    served(&mut third, "served").await;
}

/// The refusal waits for the TLS handshake a client asked for, so the client
/// reads it as an error rather than as a garbled answer to its `SSLRequest`.
#[tokio::test(flavor = "multi_thread")]
async fn the_refusal_reaches_a_tls_client_as_an_error() {
    let server = TestServer::start_configured(
        TlsMode::Require,
        AuthMethod::Trust,
        None,
        ConnectionLimits { max_connections: 1, ..limits() },
    )
    .await
    .expect("server should start");

    let _held = connect_admitted(&server.url()).await;
    let err = PgConnection::connect(&server.url())
        .await
        .expect_err("a second session must be refused under a cap of one");
    assert_eq!(sqlstate::<()>(Err(err)), "53300");
}

/// Every way a connection can end gives its slot back. A cap of two, after
/// several rounds of each ending, must still admit two sessions at once.
///
/// Two servers, because the endings need different set-ups: a refused
/// password needs authentication, and the SCRAM exchange is slow enough in a
/// debug build under a parallel suite to overrun the short handshake timeout
/// the other endings need.
#[tokio::test(flavor = "multi_thread")]
async fn every_way_a_connection_ends_returns_its_slot() {
    let authenticating = TestServer::start_configured(
        TlsMode::Disable,
        AuthMethod::ScramSha256,
        Some("s3cret"),
        ConnectionLimits { max_connections: 2, ..limits() },
    )
    .await
    .expect("server should start");
    let good = authenticating.url_as("rasterized", Some("s3cret"));
    for _ in 0..3 {
        // A wrong password.
        let refused =
            PgConnection::connect(&authenticating.url_as("rasterized", Some("wrong"))).await;
        assert_eq!(sqlstate(refused), "28P01");
    }
    let mut a = connect_admitted(&good).await;
    let mut b = connect_admitted(&good).await;
    served(&mut a, "served").await;
    served(&mut b, "served").await;

    let server = TestServer::start_with_limits(ConnectionLimits {
        max_connections: 2,
        handshake_timeout: Duration::from_millis(200),
        ..limits()
    })
    .await
    .expect("server should start");
    let good = server.url();

    for _ in 0..3 {
        // A clean close.
        let c = connect_admitted(&good).await;
        c.close().await.expect("close");

        // A socket dropped halfway through its startup packet.
        let mut raw = tokio::net::TcpStream::connect(("127.0.0.1", server.port())).await.unwrap();
        raw.write_all(&startup_packet("rasterized")[..6]).await.unwrap();
        drop(raw);

        // A socket that times out of its handshake.
        let mut raw = tokio::net::TcpStream::connect(("127.0.0.1", server.port())).await.unwrap();
        time_until_closed(&mut raw).await;

        // A session that dies of its idle timeout inside a block.
        let mut c = connect_admitted(&good).await;
        c.execute("SET idle_in_transaction_session_timeout = 100").await.unwrap();
        c.execute("BEGIN").await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(sqlstate(c.execute("SELECT 1").await), "25P03");
    }

    let mut a = connect_admitted(&good).await;
    let mut b = connect_admitted(&good).await;
    for c in [&mut a, &mut b] {
        served(c, "served").await;
    }
}

/// `max_connections` is reported and cannot be changed by a session.
#[tokio::test(flavor = "multi_thread")]
async fn max_connections_is_shown_and_read_only() {
    let server = TestServer::start_with_limits(ConnectionLimits { max_connections: 7, ..limits() })
        .await
        .expect("server should start");
    let mut c = connect_admitted(&server.url()).await;

    let shown: String = sqlx::query_scalar("SHOW max_connections").fetch_one(&mut c).await.unwrap();
    assert_eq!(shown, "7");
    assert_eq!(sqlstate(c.execute("SET max_connections = 1000").await), "55P02");
    let shown: String = sqlx::query_scalar("SHOW max_connections").fetch_one(&mut c).await.unwrap();
    assert_eq!(shown, "7", "a refused SET must change nothing");
}

// --- handshake timeout -------------------------------------------------------

/// A socket that connects and sends nothing is closed once the handshake
/// timeout passes, instead of holding a descriptor and a slot for ever.
#[tokio::test(flavor = "multi_thread")]
async fn a_silent_socket_is_closed_after_the_handshake_timeout() {
    let server = TestServer::start_with_limits(ConnectionLimits {
        handshake_timeout: Duration::from_millis(200),
        ..limits()
    })
    .await
    .expect("server should start");

    let mut raw = tokio::net::TcpStream::connect(("127.0.0.1", server.port())).await.unwrap();
    let (waited, _) = time_until_closed(&mut raw).await;
    assert!(waited >= Duration::from_millis(150), "closed before the timeout: {:?}", waited);
}

/// The timeout covers authentication too: a client that sends its startup
/// packet and then never answers the password challenge is closed.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_never_authenticates_is_closed() {
    let server = TestServer::start_configured(
        TlsMode::Disable,
        AuthMethod::ScramSha256,
        Some("s3cret"),
        ConnectionLimits { handshake_timeout: Duration::from_millis(200), ..limits() },
    )
    .await
    .expect("server should start");

    let mut raw = tokio::net::TcpStream::connect(("127.0.0.1", server.port())).await.unwrap();
    raw.write_all(&startup_packet("rasterized")).await.unwrap();
    // The SASL challenge arrives, and is never answered.
    let mut tag = [0u8; 1];
    tokio::time::timeout(BOUND, raw.read_exact(&mut tag)).await.expect("challenge").unwrap();
    assert_eq!(tag[0], b'R', "expected an authentication request");
    let (_, sent) = time_until_closed(&mut raw).await;
    // Told why, and not as a wrong password: PostgreSQL's code for it.
    let sent = String::from_utf8_lossy(&sent);
    assert!(sent.contains("57014"), "expected the timeout's SQLSTATE in {:?}", sent);
    assert!(!sent.contains("28P01"), "a timeout is not a failed password: {:?}", sent);
}

/// And TLS: a client that asks for it and then never sends its ClientHello
/// is closed, rather than parking the TLS acceptor for ever.
#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_tls_handshake_is_closed() {
    let server = TestServer::start_configured(
        TlsMode::Require,
        AuthMethod::Trust,
        None,
        ConnectionLimits { handshake_timeout: Duration::from_millis(200), ..limits() },
    )
    .await
    .expect("server should start");

    let mut raw = tokio::net::TcpStream::connect(("127.0.0.1", server.port())).await.unwrap();
    let mut ssl_request = 8i32.to_be_bytes().to_vec();
    ssl_request.extend_from_slice(&80877103i32.to_be_bytes());
    raw.write_all(&ssl_request).await.unwrap();
    let mut answer = [0u8; 1];
    tokio::time::timeout(BOUND, raw.read_exact(&mut answer)).await.expect("answer").unwrap();
    assert_eq!(answer[0], b'S');
    time_until_closed(&mut raw).await;
}

/// The handshake deadline ends with the handshake: an established session
/// that stays quiet for longer than it is not touched. Over TLS, because the
/// deadline sits underneath the TLS stream there.
///
/// `trust`, not SCRAM: the SCRAM exchange in a debug build under a parallel
/// suite can itself take longer than a timeout short enough to test with.
#[tokio::test(flavor = "multi_thread")]
async fn an_established_session_outlives_the_handshake_timeout() {
    let server = TestServer::start_configured(
        TlsMode::Require,
        AuthMethod::Trust,
        None,
        ConnectionLimits { handshake_timeout: Duration::from_millis(300), ..limits() },
    )
    .await
    .expect("server should start");

    let mut c = connect_admitted(&server.url()).await;
    tokio::time::sleep(Duration::from_millis(900)).await;
    served(&mut c, "still served").await;

    let shown: String =
        sqlx::query_scalar("SHOW authentication_timeout").fetch_one(&mut c).await.unwrap();
    assert_eq!(shown, "300ms");
}

// --- startup packet length ---------------------------------------------------

/// A startup packet's length is capped at PostgreSQL's 10,000 bytes and
/// checked before anything is allocated: a client that has not authenticated
/// chose it, and up to 2 GiB of buffer was its to ask for. Out of range, the
/// client is told why — `08P01`, in PostgreSQL's words — and the connection
/// is closed at once rather than left to the handshake timeout.
#[tokio::test(flavor = "multi_thread")]
async fn an_out_of_range_startup_packet_is_refused_and_closed() {
    let server = TestServer::start().await.expect("server should start");

    for len in [i32::MAX, 10_001, 7, -1] {
        let mut raw = tokio::net::TcpStream::connect(("127.0.0.1", server.port())).await.unwrap();
        raw.write_all(&len.to_be_bytes()).await.unwrap();
        let (_, sent) = time_until_closed(&mut raw).await;
        assert_eq!(sent.first(), Some(&b'E'), "length {}: expected an ErrorResponse, got {:?}", len, sent);
        let text = String::from_utf8_lossy(&sent);
        assert!(text.contains("FATAL") && text.contains("08P01"), "length {}: {:?}", len, text);
        assert!(text.contains("invalid length of startup packet"), "length {}: {:?}", len, text);
    }

    // The largest legal length is still read as a startup packet: padded with
    // NULs after the parameters, it authenticates like any other.
    let mut packet = startup_packet("rasterized");
    packet.resize(10_000, 0);
    packet[..4].copy_from_slice(&10_000i32.to_be_bytes());
    let mut raw = tokio::net::TcpStream::connect(("127.0.0.1", server.port())).await.unwrap();
    raw.write_all(&packet).await.unwrap();
    let (tags, _) = read_until_ready(&mut raw).await;
    assert_eq!(tags.first(), Some(&'R'), "expected authentication, got {:?}", tags);

    // And the server is unharmed by any of it.
    let mut c = connect_admitted(&server.url()).await;
    served(&mut c, "served after the refusals").await;
}

// --- TCP keepalive -----------------------------------------------------------

/// The server's end of an established connection carries a keepalive timer
/// set to the short idle time, not the two-hour system default.
///
/// Read from `/proc/net/tcp`, because nothing on the wire shows a socket
/// option: the `tr` column is 2 while a keepalive timer is pending and 0 for an
/// idle socket without one, and `tm->when` is the time left on it in
/// hundredths of a second.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn accepted_sockets_have_keepalive() {
    let server = TestServer::start().await.expect("server should start");
    let mut c = connect_admitted(&server.url()).await;
    served(&mut c, "served").await;

    let port = format!("{:04X}", server.port());
    let established_server_side = |line: &str| -> Option<(u32, u64)> {
        let cols: Vec<&str> = line.split_whitespace().collect();
        let local = cols.get(1)?;
        let state = cols.get(3)?;
        if !local.ends_with(&format!(":{}", port)) || *state != "01" {
            return None;
        }
        let (timer, when) = cols.get(5)?.split_once(':')?;
        Some((u32::from_str_radix(timer, 16).ok()?, u64::from_str_radix(when, 16).ok()?))
    };

    // The reply's ACK may still be outstanding, during which the
    // retransmit timer shows instead; poll until the socket is quiet.
    let mut seen = Vec::new();
    for _ in 0..100 {
        let table = std::fs::read_to_string("/proc/net/tcp").expect("read /proc/net/tcp");
        seen = table.lines().filter_map(established_server_side).collect();
        if seen.iter().any(|(timer, _)| *timer == 2) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (_, when) = seen
        .iter()
        .find(|(timer, _)| *timer == 2)
        .unwrap_or_else(|| panic!("no keepalive timer on the server's socket: {:?}", seen));
    assert!(
        *when <= 60 * 100,
        "the keepalive fires in {} hundredths of a second; the idle time was not applied",
        when
    );
}

// --- idle_in_transaction_session_timeout -------------------------------------

/// A session idle inside a block past the timeout is ended with 25P03, and
/// its transaction is rolled back: another session can write the same key at
/// once, which it could not while the block held it.
#[tokio::test(flavor = "multi_thread")]
async fn an_idle_transaction_is_terminated_and_rolled_back() {
    let server = TestServer::start().await.expect("server should start");
    let mut other = connect_admitted(&server.url()).await;
    other.execute("CREATE TABLE idle_tx (id INT PRIMARY KEY, v TEXT)").await.unwrap();

    let mut c = connect_admitted(&server.url()).await;
    c.execute("SET idle_in_transaction_session_timeout = 200").await.unwrap();
    let shown: String = sqlx::query_scalar("SHOW idle_in_transaction_session_timeout")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(shown, "200ms");

    c.execute("BEGIN").await.unwrap();
    c.execute("INSERT INTO idle_tx (id, v) VALUES (1, 'abandoned')").await.unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;

    assert_eq!(sqlstate(c.execute("SELECT 1").await), "25P03");

    let rows = other.fetch_all("SELECT v FROM idle_tx").await.unwrap();
    assert!(rows.is_empty(), "the abandoned insert must not be visible");
    other
        .execute("INSERT INTO idle_tx (id, v) VALUES (1, 'kept')")
        .await
        .expect("the key must be free once the idle block is rolled back");
    let v: String = other.fetch_one("SELECT v FROM idle_tx WHERE id = 1").await.unwrap().get(0);
    assert_eq!(v, "kept");
}

/// The timer measures idleness, not the age of the block: a block that keeps
/// sending statements lives as long as it likes, and a session outside a
/// block is not subject to it at all.
#[tokio::test(flavor = "multi_thread")]
async fn activity_keeps_a_block_alive() {
    let server = TestServer::start().await.expect("server should start");
    let mut c = connect_admitted(&server.url()).await;
    c.execute("SET idle_in_transaction_session_timeout = '300ms'").await.unwrap();

    c.execute("BEGIN").await.unwrap();
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        served(&mut c, "alive").await;
    }
    c.execute("COMMIT").await.expect("a block that kept talking commits");

    tokio::time::sleep(Duration::from_millis(600)).await;
    served(&mut c, "idle outside a block is not idle in transaction").await;

    // And the timeout was live all along: the same pause inside a block ends it.
    c.execute("BEGIN").await.unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(sqlstate(c.execute("SELECT 1").await), "25P03");
}

/// The server default applies to new sessions; `SET` overrides it, `0`
/// disables it, and `RESET` returns to the server's value rather than to zero.
#[tokio::test(flavor = "multi_thread")]
async fn the_server_default_applies_and_reset_returns_to_it() {
    let server = TestServer::start_with_limits(ConnectionLimits {
        idle_in_transaction_session_timeout: Duration::from_millis(200),
        ..limits()
    })
    .await
    .expect("server should start");
    let mut c = connect_admitted(&server.url()).await;

    async fn show(c: &mut PgConnection) -> String {
        sqlx::query_scalar::<_, String>("SHOW idle_in_transaction_session_timeout")
            .fetch_one(c)
            .await
            .unwrap()
    }
    assert_eq!(show(&mut c).await, "200ms");
    c.execute("SET idle_in_transaction_session_timeout TO '1min'").await.unwrap();
    assert_eq!(show(&mut c).await, "1min");
    c.execute("SET idle_in_transaction_session_timeout = 0").await.unwrap();
    assert_eq!(show(&mut c).await, "0");

    c.execute("BEGIN").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    c.execute("COMMIT").await.expect("0 disables the timeout");

    c.execute("RESET idle_in_transaction_session_timeout").await.unwrap();
    assert_eq!(show(&mut c).await, "200ms");
    c.execute("BEGIN").await.unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(sqlstate(c.execute("SELECT 1").await), "25P03");
}

/// `SET LOCAL` lasts for the block it was made in.
#[tokio::test(flavor = "multi_thread")]
async fn set_local_ends_with_the_block() {
    let server = TestServer::start().await.expect("server should start");
    let mut c = connect_admitted(&server.url()).await;

    c.execute("BEGIN").await.unwrap();
    c.execute("SET LOCAL idle_in_transaction_session_timeout = '5s'").await.unwrap();
    let inside: String = sqlx::query_scalar("SHOW idle_in_transaction_session_timeout")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(inside, "5s");
    c.execute("COMMIT").await.unwrap();
    let after: String = sqlx::query_scalar("SHOW idle_in_transaction_session_timeout")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(after, "0");
}

/// A value that is not a time is refused, not read as zero — which would
/// quietly disable the bound the client asked for.
#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_timeout_is_refused() {
    let server = TestServer::start().await.expect("server should start");
    let mut c = connect_admitted(&server.url()).await;
    c.execute("SET idle_in_transaction_session_timeout = 250").await.unwrap();

    for bad in [
        "SET idle_in_transaction_session_timeout = 'soon'",
        "SET idle_in_transaction_session_timeout = -5",
        "SET idle_in_transaction_session_timeout = '10 parsecs'",
        "SET idle_in_transaction_session_timeout = '1us'",
        "SET idle_session_timeout = '99999999999'",
    ] {
        assert_eq!(sqlstate(c.execute(bad).await), "22023", "{}", bad);
    }
    let shown: String = sqlx::query_scalar("SHOW idle_in_transaction_session_timeout")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(shown, "250ms", "a refused value must leave the setting as it was");
}

/// One frontend message: tag, length, body.
fn message(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// Reads backend messages up to and including `ReadyForQuery`, returning
/// their tags and the text of the whole exchange.
async fn read_until_ready(stream: &mut tokio::net::TcpStream) -> (Vec<char>, String) {
    let mut tags = Vec::new();
    let mut text = Vec::new();
    for _ in 0..64 {
        let mut header = [0u8; 5];
        tokio::time::timeout(BOUND, stream.read_exact(&mut header))
            .await
            .expect("the server must answer")
            .expect("the connection must stay open");
        let len = i32::from_be_bytes(header[1..5].try_into().unwrap()) as usize - 4;
        let mut body = vec![0u8; len];
        stream.read_exact(&mut body).await.expect("message body");
        tags.push(header[0] as char);
        text.extend_from_slice(&body);
        if header[0] == b'Z' {
            return (tags, String::from_utf8_lossy(&text).into_owned());
        }
    }
    panic!("no ReadyForQuery in 64 messages: {:?}", tags);
}

/// A session between `Parse` and `Sync` is in the middle of a request, not
/// idle: PostgreSQL arms the idle timeouts only when it sends
/// `ReadyForQuery`, and so does this. A client that pauses between building a
/// batch and sending its `Sync` must get its answer, not a termination.
#[tokio::test(flavor = "multi_thread")]
async fn a_pause_inside_an_extended_batch_is_not_idleness() {
    let server = TestServer::start().await.expect("server should start");
    let mut raw = tokio::net::TcpStream::connect(("127.0.0.1", server.port())).await.unwrap();
    raw.write_all(&startup_packet("rasterized")).await.unwrap();
    read_until_ready(&mut raw).await;

    let mut set = b"SET idle_session_timeout = 200".to_vec();
    set.push(0);
    raw.write_all(&message(b'Q', &set)).await.unwrap();
    let (tags, _) = read_until_ready(&mut raw).await;
    assert_eq!(tags, vec!['C', 'Z']);

    // Parse and Bind, then a pause well past the timeout, then the rest.
    let mut parse = b"\0SHOW idle_session_timeout\0".to_vec();
    parse.extend_from_slice(&0i16.to_be_bytes());
    let mut bind = b"\0\0".to_vec();
    bind.extend_from_slice(&0i16.to_be_bytes());
    bind.extend_from_slice(&0i16.to_be_bytes());
    bind.extend_from_slice(&0i16.to_be_bytes());
    let mut batch = message(b'P', &parse);
    batch.extend_from_slice(&message(b'B', &bind));
    raw.write_all(&batch).await.unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;

    let mut execute = b"\0".to_vec();
    execute.extend_from_slice(&0i32.to_be_bytes());
    let mut rest = message(b'E', &execute);
    rest.extend_from_slice(&message(b'S', &[]));
    raw.write_all(&rest).await.unwrap();
    let (tags, text) = read_until_ready(&mut raw).await;
    assert!(!tags.contains(&'E'), "the batch must be answered, not terminated: {}", text);
    assert!(text.contains("200ms"), "expected the SHOW's value in {:?}", text);

    // Once the batch is answered the session is idle, and the timeout runs.
    let (_, sent) = time_until_closed(&mut raw).await;
    assert!(String::from_utf8_lossy(&sent).contains("57P05"));
}

// --- idle_session_timeout ----------------------------------------------------

/// A session idle outside a block past `idle_session_timeout` is ended with
/// 57P05; inside a block that timeout does not apply.
#[tokio::test(flavor = "multi_thread")]
async fn an_idle_session_is_terminated() {
    let server = TestServer::start().await.expect("server should start");

    let mut in_block = connect_admitted(&server.url()).await;
    in_block.execute("SET idle_session_timeout = 200").await.unwrap();
    in_block.execute("BEGIN").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    in_block.execute("COMMIT").await.expect("idle_session_timeout does not apply inside a block");

    let mut c = connect_admitted(&server.url()).await;
    c.execute("SET idle_session_timeout = '200ms'").await.unwrap();
    let shown: String =
        sqlx::query_scalar("SHOW idle_session_timeout").fetch_one(&mut c).await.unwrap();
    assert_eq!(shown, "200ms");
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(sqlstate(c.execute("SELECT 1").await), "57P05");
}
