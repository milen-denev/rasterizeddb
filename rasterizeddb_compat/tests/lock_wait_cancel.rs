//! A statement waiting for a lock must still obey its own bound.
//!
//! `SELECT … FOR UPDATE` and `LOCK TABLE` wait for a holder by polling, with a
//! fixed 10 s ceiling of their own. That loop used to look at nothing else, so
//! `statement_timeout = 200` meant ten seconds for any statement that met a
//! held lock, and a client that disconnected mid-wait kept its statement queued
//! for the full ten. Both have to end the wait as the cancellation every other
//! statement reports: `57014`, "canceling statement …".

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use std::time::{Duration, Instant};

async fn seeded(server: &TestServer) -> sqlx::PgConnection {
    let mut conn = sqlx::PgConnection::connect(&server.url())
        .await
        .expect("connect");
    conn.execute(AssertSqlSafe(
        "CREATE TABLE jobs (id INT PRIMARY KEY, state TEXT NOT NULL)",
    ))
    .await
    .expect("create");
    conn.execute(AssertSqlSafe(
        "INSERT INTO jobs (id, state) VALUES (1, 'pending'), (2, 'pending')",
    ))
    .await
    .expect("insert");
    conn
}

/// Asserts `outcome` is a statement-timeout cancellation that arrived well
/// before the lock wait's own 10 s ceiling.
fn assert_cancelled_by_timeout<T: std::fmt::Debug>(
    outcome: Result<T, sqlx::Error>,
    took: Duration,
    what: &str,
) {
    let e = match outcome {
        Ok(v) => panic!("{what} should have been cancelled, got {v:?}"),
        Err(e) => e,
    };
    let message = e.to_string();
    assert!(
        message.contains("canceling statement due to statement timeout"),
        "{what}: expected the statement-timeout cancellation, got: {message}"
    );
    assert_eq!(
        e.as_database_error().and_then(|d| d.code()).as_deref(),
        Some("57014"),
        "{what}: a cancelled wait must carry query_canceled"
    );
    // 200 ms asked for; anything near the 10 s lock ceiling means the wait
    // ignored the statement's bound. Generous for a loaded box.
    assert!(
        took < Duration::from_secs(3),
        "{what} took {took:?}; statement_timeout = 200ms was ignored by the lock wait"
    );
    assert!(
        took >= Duration::from_millis(150),
        "{what} returned after {took:?}, before the timeout could have expired"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_update_wait_obeys_statement_timeout() {
    let server = TestServer::start().await.expect("start");
    let mut holder = seeded(&server).await;
    holder.execute(AssertSqlSafe("BEGIN")).await.expect("begin");
    let held = sqlx::query(AssertSqlSafe("SELECT id FROM jobs WHERE id = 1 FOR UPDATE"))
        .fetch_all(&mut holder)
        .await
        .expect("holder locks row 1");
    assert_eq!(held.len(), 1);

    let mut waiter = sqlx::PgConnection::connect(&server.url())
        .await
        .expect("connect");
    waiter
        .execute(AssertSqlSafe("SET statement_timeout = 200"))
        .await
        .expect("set");
    let started = Instant::now();
    let outcome = sqlx::query(AssertSqlSafe("SELECT id FROM jobs WHERE id = 1 FOR UPDATE"))
        .fetch_all(&mut waiter)
        .await
        .map(|rows| rows.len());
    assert_cancelled_by_timeout(outcome, started.elapsed(), "SELECT … FOR UPDATE");

    // The waiter's session survives, and once the holder is gone the same
    // statement takes the lock — the cancellation damaged nothing.
    holder.execute(AssertSqlSafe("COMMIT")).await.expect("commit");
    waiter
        .execute(AssertSqlSafe("SET statement_timeout = 0"))
        .await
        .expect("reset");
    let rows = sqlx::query(AssertSqlSafe("SELECT id FROM jobs WHERE id = 1 FOR UPDATE"))
        .fetch_all(&mut waiter)
        .await
        .expect("lock is free now");
    assert_eq!(rows.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lock_table_wait_obeys_statement_timeout() {
    let server = TestServer::start().await.expect("start");
    let mut holder = seeded(&server).await;
    holder.execute(AssertSqlSafe("BEGIN")).await.expect("begin");
    holder
        .execute(AssertSqlSafe("LOCK TABLE jobs IN ACCESS EXCLUSIVE MODE"))
        .await
        .expect("holder locks the table");

    let mut waiter = sqlx::PgConnection::connect(&server.url())
        .await
        .expect("connect");
    waiter.execute(AssertSqlSafe("BEGIN")).await.expect("begin");
    waiter
        .execute(AssertSqlSafe("SET statement_timeout = 200"))
        .await
        .expect("set");
    let started = Instant::now();
    let outcome = waiter
        .execute(AssertSqlSafe("LOCK TABLE jobs IN ACCESS EXCLUSIVE MODE"))
        .await
        .map(|r| r.rows_affected());
    assert_cancelled_by_timeout(outcome, started.elapsed(), "LOCK TABLE");
    let _ = waiter.execute(AssertSqlSafe("ROLLBACK")).await;
    holder.execute(AssertSqlSafe("COMMIT")).await.expect("commit");
}

/// Speaks the wire protocol on a raw socket, sends `sql` as a simple query and
/// returns the socket without reading the answer. Dropping it is what a killed
/// client looks like; sqlx does not close promptly when its task is dropped.
fn send_and_abandon(server: &TestServer, sql: &'static str) -> std::net::TcpStream {
    use std::io::{Read, Write};
    let port = server.port();
    let url = server.url();
    let database = url
        .rsplit('/')
        .next()
        .and_then(|tail| tail.split('?').next())
        .expect("database name in the URL")
        .to_string();
    let socket = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    socket.set_nodelay(true).expect("nodelay");
    let mut params = Vec::new();
    for (key, value) in [("user", "rasterized"), ("database", database.as_str())] {
        params.extend_from_slice(key.as_bytes());
        params.push(0);
        params.extend_from_slice(value.as_bytes());
        params.push(0);
    }
    params.push(0);
    let mut startup = Vec::new();
    startup.extend_from_slice(&((params.len() + 8) as i32).to_be_bytes());
    startup.extend_from_slice(&196_608i32.to_be_bytes());
    startup.extend_from_slice(&params);
    (&socket).write_all(&startup).expect("startup");

    let mut seen: Vec<u8> = Vec::new();
    let mut buffer = [0u8; 1024];
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    while !seen.contains(&b'Z') {
        match (&socket).read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(n) => seen.extend_from_slice(&buffer[..n]),
        }
    }
    assert!(seen.contains(&b'Z'), "raw client never reached ReadyForQuery");

    let mut query = vec![b'Q'];
    query.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
    query.extend_from_slice(sql.as_bytes());
    query.push(0);
    (&socket).write_all(&query).expect("query");
    socket
}

/// A client that disconnects while its `FOR UPDATE` waits must have the
/// statement stop as "client closed", not sit out the 10 s lock ceiling and
/// then report a lock timeout to nobody.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_update_wait_stops_when_the_client_leaves() {
    const WAIT: &str = "SELECT id FROM jobs WHERE id = 2 AND state <> 'abandoned_waiter' FOR UPDATE";
    let server = TestServer::start().await.expect("start");
    let mut holder = seeded(&server).await;
    holder.execute(AssertSqlSafe("BEGIN")).await.expect("begin");
    sqlx::query(AssertSqlSafe("SELECT id FROM jobs WHERE id = 2 FOR UPDATE"))
        .fetch_all(&mut holder)
        .await
        .expect("holder locks row 2");

    let doomed = send_and_abandon(&server, WAIT);
    // Let the statement reach the wait before the client vanishes.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let dropped_at = Instant::now();
    drop(doomed);

    let mut observer = sqlx::PgConnection::connect(&server.url())
        .await
        .expect("connect");
    let mut last_seen = String::from("(the abandoned statement is not in the log)");
    while dropped_at.elapsed() < Duration::from_secs(4) {
        let log = sqlx::query(AssertSqlSafe("SHOW QUERIES"))
            .fetch_all(&mut observer)
            .await
            .expect("show queries");
        for row in log
            .iter()
            .filter(|r| r.get::<String, _>("statement").contains("abandoned_waiter"))
        {
            let status: String = row.get("status");
            let error: String = row.get("error");
            if status != "ok" {
                assert!(
                    error.contains("client closed the connection"),
                    "the wait ended, but not because the client left: {error}"
                );
                holder.execute(AssertSqlSafe("COMMIT")).await.expect("commit");
                return;
            }
            last_seen = format!("status={status:?} error={error:?}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!(
        "the abandoned FOR UPDATE was still waiting 4 s after its client left; \
         last seen: {last_seen}"
    );
}
