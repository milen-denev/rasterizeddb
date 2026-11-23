//! `WHERE a >= $x OR <outer side> >= $x` over a LEFT JOIN.
//!
//! Reported as a bare `OperandSizeMismatch` with no context. The suspicion is
//! that it is the same defect as the COALESCE one: an unmatched outer row's
//! columns arrived as their declared type holding *no bytes*, so comparing eight
//! bytes of timestamp against zero bytes is a size mismatch rather than a
//! comparison against NULL.

use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use rasterizeddb_compat::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_or_against_the_outer_side_of_a_left_join() {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    for sql in [
        "CREATE TABLE spc (id INT PRIMARY KEY, case_id INT NOT NULL, assigned_at TIMESTAMPTZ)",
        "CREATE TABLE c (id INT PRIMARY KEY, closed_at TIMESTAMPTZ)",
        // 1: assigned late, has a case row closed late.
        "INSERT INTO c (id, closed_at) VALUES (1, '2026-06-01 00:00:00+00')",
        "INSERT INTO spc (id, case_id, assigned_at) VALUES (10, 1, '2026-06-02 00:00:00+00')",
        // 2: assigned late, but NO matching case row — the unmatched side.
        "INSERT INTO spc (id, case_id, assigned_at) VALUES (11, 99, '2026-06-03 00:00:00+00')",
        // 3: assigned early, no matching case row: must not qualify.
        "INSERT INTO spc (id, case_id, assigned_at) VALUES (12, 98, '2020-01-01 00:00:00+00')",
    ] {
        conn.execute(AssertSqlSafe(sql)).await.expect(sql);
    }

    let ids: Vec<i32> = sqlx::query(AssertSqlSafe(
        "SELECT spc.id AS id FROM spc LEFT JOIN c ON c.id = spc.case_id \
         WHERE spc.assigned_at >= '2026-01-01 00:00:00+00' \
            OR c.closed_at >= '2026-01-01 00:00:00+00' \
         ORDER BY spc.id",
    ))
    .fetch_all(&mut conn)
    .await
    .expect("an OR against the outer side must not fail")
    .iter()
    .map(|r| r.try_get::<i32, _>("id").expect("id"))
    .collect();

    // 10 and 11 are assigned in 2026; 12 is not, and its unmatched `closed_at`
    // is NULL rather than a value that could satisfy the second arm.
    assert_eq!(ids, vec![10, 11], "the unmatched side must read as NULL, not as zero bytes");
}
