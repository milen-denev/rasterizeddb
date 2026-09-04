//! The row-count fast path must agree with the ordinary aggregate path.
//!
//! A `COUNT(*)` with no grouping, no `HAVING` and no filter is answered by
//! counting matched rows instead of materialising them. The guard that decides
//! eligibility is an **allowlist over `AggregatePlan`**, so every field it does
//! not read is a way for a query to take a path that ignores it — `limit` and
//! `offset` were two, and `COUNT(*) … LIMIT 0` returned a row where the
//! ordinary path correctly returns none.
//!
//! Each shape below is checked against the same query made *ineligible* by a
//! `FILTER`, so the two paths are compared against each other rather than
//! against an assumption.

use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use rasterizeddb_compat::TestServer;

async fn seeded() -> (TestServer, sqlx::PgConnection) {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    conn.execute(AssertSqlSafe(
        "CREATE TABLE c (id INT PRIMARY KEY, age INT NOT NULL)",
    ))
    .await
    .expect("create");
    for i in 1..=10 {
        conn.execute(AssertSqlSafe(format!(
            "INSERT INTO c (id, age) VALUES ({i}, {i})"
        )))
        .await
        .expect("insert");
    }
    (server, conn)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paging_applies_to_a_counted_result() {
    let (_server, mut conn) = seeded().await;

    // `LIMIT 0` is not exotic: drivers issue it to fetch column metadata
    // without rows.
    for (label, sql, want_rows) in [
        ("plain", "SELECT COUNT(*) AS n FROM c", 1usize),
        ("limit 0", "SELECT COUNT(*) AS n FROM c LIMIT 0", 0),
        ("offset 1", "SELECT COUNT(*) AS n FROM c OFFSET 1", 0),
        ("limit 1", "SELECT COUNT(*) AS n FROM c LIMIT 1", 1),
        ("limit 5", "SELECT COUNT(*) AS n FROM c LIMIT 5", 1),
    ] {
        let rows = sqlx::query(AssertSqlSafe(sql))
            .fetch_all(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("{label}: {sql}: {e}"));
        assert_eq!(rows.len(), want_rows, "{label}: {sql}");
        if want_rows == 1 {
            assert_eq!(rows[0].try_get::<i64, _>("n").expect("n"), 10, "{label}");
        }
    }
}

/// The fast path and the ordinary path must give the same answer, including
/// how many rows come back. A `FILTER` makes the same query ineligible.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_two_paths_agree() {
    let (_server, mut conn) = seeded().await;

    for tail in ["", " LIMIT 0", " OFFSET 1", " LIMIT 1"] {
        let fast = sqlx::query(AssertSqlSafe(format!(
            "SELECT COUNT(*) AS n FROM c WHERE age > 5{tail}"
        )))
        .fetch_all(&mut conn)
        .await
        .unwrap_or_else(|e| panic!("fast '{tail}': {e}"));

        // Ineligible for the fast path: the filter is per-aggregate, so this
        // one folds rows the ordinary way.
        let slow = sqlx::query(AssertSqlSafe(format!(
            "SELECT COUNT(*) FILTER (WHERE age > 5) AS n FROM c{tail}"
        )))
        .fetch_all(&mut conn)
        .await
        .unwrap_or_else(|e| panic!("slow '{tail}': {e}"));

        assert_eq!(
            fast.len(),
            slow.len(),
            "row count differs between the two paths for '{tail}'"
        );
        if !fast.is_empty() {
            assert_eq!(
                fast[0].try_get::<i64, _>("n").expect("fast"),
                slow[0].try_get::<i64, _>("n").expect("slow"),
                "value differs between the two paths for '{tail}'"
            );
            assert_eq!(fast[0].try_get::<i64, _>("n").expect("fast"), 5);
        }
    }
}
