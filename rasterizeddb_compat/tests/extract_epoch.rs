//! `EXTRACT(EPOCH FROM …)`.
//!
//! It used to fail with "relation does not exist" — a message naming nothing in
//! the statement, because `EPOCH FROM x` was handed to the expression parser as
//! one argument and `EPOCH` read as a column. PMS uses it in five places, all in
//! the three dashboard queries that also needed `FILTER` over a join.

use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use rasterizeddb_compat::TestServer;

async fn seeded() -> (TestServer, sqlx::PgConnection) {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    for sql in [
        "CREATE TABLE ev (id INT PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL)",
        // Exactly one hour apart, so the epoch of the difference is 3600.
        "INSERT INTO ev (id, created_at, updated_at) VALUES (1, '2026-01-01 00:00:00+00', '2026-01-01 01:00:00+00')",
        // Ninety seconds apart.
        "INSERT INTO ev (id, created_at, updated_at) VALUES (2, '2026-01-01 00:00:00+00', '2026-01-01 00:01:30+00')",
    ] {
        conn.execute(AssertSqlSafe(sql)).await.expect(sql);
    }
    (server, conn)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn epoch_of_an_interval_is_its_seconds() {
    let (_server, mut conn) = seeded().await;

    let rows = sqlx::query(AssertSqlSafe(
        "SELECT id, EXTRACT(EPOCH FROM (updated_at - created_at)) AS seconds \
         FROM ev ORDER BY id",
    ))
    .fetch_all(&mut conn)
    .await
    .expect("EXTRACT(EPOCH FROM interval)");

    let got: Vec<(i32, f64)> = rows
        .iter()
        .map(|r| (r.try_get("id").expect("id"), r.try_get("seconds").expect("seconds")))
        .collect();
    assert_eq!(got, vec![(1, 3600.0), (2, 90.0)]);
}

/// The epoch of an instant is its seconds since 1970.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn epoch_of_a_timestamp_is_seconds_since_1970() {
    let (_server, mut conn) = seeded().await;
    let seconds: f64 = sqlx::query(AssertSqlSafe(
        "SELECT EXTRACT(EPOCH FROM created_at) AS s FROM ev WHERE id = 1",
    ))
    .fetch_one(&mut conn)
    .await
    .expect("EXTRACT(EPOCH FROM timestamp)")
    .try_get("s")
    .expect("s");
    // 2026-01-01T00:00:00Z.
    assert_eq!(seconds, 1_767_225_600.0);
}

/// A field this cannot compute must say so, rather than return a number.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unsupported_field_is_refused() {
    let (_server, mut conn) = seeded().await;
    let outcome = sqlx::query(AssertSqlSafe(
        "SELECT EXTRACT(YEAR FROM created_at) AS y FROM ev",
    ))
    .fetch_all(&mut conn)
    .await;
    let error = outcome.err().map(|e| e.to_string()).unwrap_or_default();
    // Refused, not answered. The message currently comes from the select-list
    // parser's generic "function is not supported yet" rather than the specific
    // reason EXTRACT itself gives, which is worth improving but is not the
    // property under test: a field this cannot compute must not return a number.
    assert!(
        error.contains("not supported yet"),
        "an unsupported field must be refused: {error}"
    );
}
