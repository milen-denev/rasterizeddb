//! `statement_timeout`, which used to be accepted and thrown away.
//!
//! The deployed server was OOM-killed twice by statements that nothing could
//! stop, so what matters here is not only that a long statement ends, but that
//! it ends as a *failure*. A cancelled scan that returned the rows it had
//! reached would look like a smaller result set, which is worse than a hang.

use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use rasterizeddb_compat::TestServer;

/// Enough rows that a full scan spans many batches, and bounded so the test
/// cannot fill a shared /tmp.
const ROWS: usize = 60_000;

async fn seeded() -> (TestServer, sqlx::PgConnection) {
    let server = TestServer::start().await.expect("server should start");
    // One pinned connection: `SET` is per-session, and a pool could hand a
    // later statement a different connection that never saw it.
    let mut conn = sqlx::PgConnection::connect(&server.url())
        .await
        .expect("should connect");
    conn.execute(AssertSqlSafe(
        "CREATE TABLE wide (id INT PRIMARY KEY, tag TEXT NOT NULL, n BIGINT NOT NULL)",
    ))
    .await
    .expect("create");

    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(1_000) {
        let values = chunk
            .iter()
            .map(|i| format!("({i}, 'tag{i}', {i})"))
            .collect::<Vec<_>>()
            .join(",");
        conn.execute(AssertSqlSafe(format!(
            "INSERT INTO wide (id, tag, n) VALUES {values}"
        )))
        .await
        .expect("insert");
    }
    (server, conn)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timeout_cancels_with_an_error_rather_than_a_short_answer() {
    let (_server, mut conn) = seeded().await;

    // What SHOW says must be what was set. It used to answer "0" regardless,
    // telling an operator statements were unbounded when they had just been
    // bounded — and telling a client its SET had not taken.
    conn.execute(AssertSqlSafe("SET statement_timeout = '250ms'"))
        .await
        .expect("set");
    let shown: String = sqlx::query(AssertSqlSafe("SHOW statement_timeout"))
        .fetch_one(&mut conn)
        .await
        .expect("show")
        .try_get(0)
        .expect("value");
    assert_eq!(shown, "250ms", "SHOW must report what was SET");

    // A timeout small enough that a full scan cannot finish inside it.
    conn.execute(AssertSqlSafe("SET statement_timeout = '1ms'"))
        .await
        .expect("set");
    let outcome = sqlx::query(AssertSqlSafe(
        "SELECT id, tag, n FROM wide ORDER BY tag DESC",
    ))
    .fetch_all(&mut conn)
    .await;

    match outcome {
        Ok(rows) => panic!(
            "the statement was allowed to finish, returning {} rows — a timeout must \
             cancel, and must never return a partial result as a success",
            rows.len()
        ),
        Err(e) => {
            let message = e.to_string();
            assert!(
                message.contains("canceling statement"),
                "expected a cancellation, got: {message}"
            );
            if let Some(db) = e.as_database_error() {
                assert_eq!(
                    db.code().as_deref(),
                    Some("57014"),
                    "a cancelled statement must carry query_canceled, not a generic code"
                );
            }
        }
    }

    // The session survives its own cancellation, and lifting the limit lets the
    // same statement complete in full — the cancellation was the timeout, not
    // damage to the connection or the table.
    conn.execute(AssertSqlSafe("SET statement_timeout = 0"))
        .await
        .expect("reset");
    let shown: String = sqlx::query(AssertSqlSafe("SHOW statement_timeout"))
        .fetch_one(&mut conn)
        .await
        .expect("show")
        .try_get(0)
        .expect("value");
    assert_eq!(shown, "0ms", "zero is no limit");

    let rows = sqlx::query(AssertSqlSafe("SELECT id FROM wide"))
        .fetch_all(&mut conn)
        .await
        .expect("the same statement should complete with no limit");
    assert_eq!(rows.len(), ROWS, "every row should come back");
}

/// A generous timeout must not cancel an ordinary statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_generous_timeout_leaves_a_statement_alone() {
    let (_server, mut conn) = seeded().await;
    conn.execute(AssertSqlSafe("SET statement_timeout = '5min'"))
        .await
        .expect("set");
    let rows = sqlx::query(AssertSqlSafe("SELECT id FROM wide"))
        .fetch_all(&mut conn)
        .await
        .expect("should not be cancelled");
    assert_eq!(rows.len(), ROWS);
}

/// The timeout a driver sets at connection time, not with a `SET`.
///
/// sea-orm's `ConnectOptions::statement_timeout` becomes libpq's
/// `options=-c statement_timeout=…` in the StartupMessage, explicitly to avoid
/// a roundtrip. Honouring only `SET` would leave every sea-orm application
/// unprotected while `psql` showed the feature working.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_startup_parameter_sets_the_timeout() {
    let (server, seeder) = seeded().await;
    // Close the seeding connection so the table is all that survives it.
    drop(seeder);

    // The shape sqlx builds from `.options([("statement_timeout", "…")])`.
    let url = format!("{}&options=-c%20statement_timeout%3D1", server.url());
    let mut conn = sqlx::PgConnection::connect(&url)
        .await
        .expect("should connect with a startup options parameter");

    let shown: String = sqlx::query(AssertSqlSafe("SHOW statement_timeout"))
        .fetch_one(&mut conn)
        .await
        .expect("show")
        .try_get(0)
        .expect("value");
    assert_eq!(shown, "1ms", "the startup parameter must reach the session");

    let outcome = sqlx::query(AssertSqlSafe(
        "SELECT id, tag, n FROM wide ORDER BY tag DESC",
    ))
    .fetch_all(&mut conn)
    .await;
    match outcome {
        Ok(rows) => panic!(
            "a startup-set timeout did not cancel: {} rows came back",
            rows.len()
        ),
        Err(e) => assert!(
            e.to_string().contains("canceling statement"),
            "expected a cancellation, got: {e}"
        ),
    }
}
