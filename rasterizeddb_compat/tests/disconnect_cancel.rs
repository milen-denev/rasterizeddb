//! A client that goes away mid-scan must have its statement stopped.
//!
//! This is what OOM-killed the deployment twice: a killed client left the
//! server executing forever, and several of those together reached 21 GB and
//! six cores with nothing attached. The statement is *asked* to stop rather
//! than dropped, so it unwinds through the ordinary scan-error path and
//! discharges its cleanup on the way out.
//!
//! **What this can and cannot interrupt.** The check runs at the scan's batch
//! boundaries, which is where a runaway statement spends its time and its
//! memory. A long *synchronous* phase — sorting the rows already gathered —
//! yields to nothing and so is not interruptible; `statement_timeout` has the
//! same boundary, for the same reason. The statement below is therefore chosen
//! to be scan-bound: a substring match that no index can prune and that returns
//! nothing, so the time is all scanning and none of it is sorting or writing.

use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use rasterizeddb_compat::TestServer;
use std::time::{Duration, Instant};

const ROWS: i64 = 100_000;
/// Matches nothing, and `%…%` cannot be answered from the zone maps, so every
/// row is read and no output is produced until the end.
const SCAN: &str = "SELECT id FROM p WHERE tag LIKE '%zzzqqq%'";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_disconnected_client_stops_its_statement() {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    conn.execute(AssertSqlSafe(
        "CREATE TABLE p (id INT PRIMARY KEY, tag TEXT NOT NULL, n BIGINT NOT NULL)",
    ))
    .await
    .expect("create");
    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(5_000) {
        let values = chunk
            .iter()
            .map(|i| format!("({i}, 'tag{}', {i})", i % 977))
            .collect::<Vec<_>>()
            .join(",");
        conn.execute(AssertSqlSafe(format!(
            "INSERT INTO p (id, tag, n) VALUES {values}"
        )))
        .await
        .expect("insert");
    }

    // Baseline: how long the scan takes when nobody interrupts it.
    let started = Instant::now();
    let rows = sqlx::query(AssertSqlSafe(SCAN))
        .fetch_all(&mut conn)
        .await
        .expect("baseline");
    let baseline = started.elapsed();
    assert!(rows.is_empty());
    assert!(
        baseline > Duration::from_millis(200),
        "the scan is too quick ({baseline:?}) for this test to distinguish anything"
    );

    // Run it again on a connection that vanishes partway through.
    //
    // Spoken directly rather than through sqlx: sqlx does not close its socket
    // promptly when its task is dropped, so an aborted sqlx task leaves the
    // server with a connection that is still open and nothing to notice. A raw
    // socket closes when it is dropped, which is what a killed client does.
    let port = server.port();
    // The database this server actually serves, taken from its own URL rather
    // than assumed: the user is `rasterized` and the database is not.
    let url = server.url();
    let database = url
        .rsplit('/')
        .next()
        .and_then(|tail| tail.split('?').next())
        .expect("database name in the URL")
        .to_string();
    let doomed = std::thread::spawn(move || {
        let socket = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
        socket.set_nodelay(true).expect("nodelay");

        // StartupMessage: length, protocol 3.0, then key/value pairs.
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
        {
            use std::io::Write;
            let mut socket = &socket;
            socket.write_all(&startup).expect("startup");
            socket.flush().expect("flush");
        }

        // Read until ReadyForQuery ('Z'), so the query goes to a live session.
        {
            use std::io::Read;
            let mut socket = &socket;
            let mut seen: Vec<u8> = Vec::new();
            let mut buffer = [0u8; 1024];
            let mut ready = false;
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("read timeout");
            while !ready {
                match socket.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        seen.extend_from_slice(&buffer[..n]);
                        ready = seen.contains(&b'Z');
                    }
                }
            }
            assert!(
                ready,
                "the raw client never reached ReadyForQuery; the handshake failed \
                 (server said: {:?})",
                String::from_utf8_lossy(&seen)
            );
            socket.set_read_timeout(None).expect("clear timeout");
        }

        // Simple query, then vanish without reading the answer.
        let mut query = vec![b'Q'];
        query.extend_from_slice(&((SCAN.len() + 5) as i32).to_be_bytes());
        query.extend_from_slice(SCAN.as_bytes());
        query.push(0);
        {
            use std::io::Write;
            let mut socket = &socket;
            socket.write_all(&query).expect("query");
            socket.flush().expect("flush");
        }
        socket
    })
    .join()
    .expect("client thread");

    // Closed straight away, before the server can finish. Deliberately not a
    // race: the property under test is "a statement whose client is gone must
    // stop", and timing the disconnect to land inside the scan would make the
    // test measure scheduling rather than the mechanism.
    drop(doomed);

    // The engine's own statement log is the oracle: the abandoned statement has
    // to be recorded as a failure naming the client going away. A server that
    // ignored the disconnect would record it as a success, having run it to
    // completion for nobody.
    let deadline = Instant::now() + baseline * 4;
    let mut last_seen = String::from("(no second run in the log)");
    while Instant::now() < deadline {
        let log = sqlx::query(AssertSqlSafe("SHOW QUERIES"))
            .fetch_all(&mut conn)
            .await
            .expect("show queries");
        // The baseline run is in the log too, so look for one that failed.
        for row in log
            .iter()
            .filter(|r| r.get::<String, _>("statement").contains("zzzqqq"))
        {
            let status: String = row.get("status");
            let error: String = row.get("error");
            if status != "ok" {
                assert!(
                    error.contains("client closed the connection"),
                    "the statement failed, but not because the client left: {error}"
                );
                return;
            }
            last_seen = format!("status={status:?} error={error:?}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!(
        "the abandoned statement was never recorded as cancelled within {:?} \
         (baseline {baseline:?}); last seen: {last_seen}",
        baseline * 4
    );
}
