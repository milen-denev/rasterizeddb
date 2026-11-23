//! A signed numeric literal in a predicate.
//!
//! The tokenizer emits the sign as its own operator, so the parser met a factor
//! beginning with `Op("-")` and refused the statement — while the identical
//! positive literal parsed. Found against `pg_stat_statements`, whose `queryid`
//! is a signed 64-bit hash and so is negative about half the time.

use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use rasterizeddb_compat::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_negative_literal_compares() {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    for sql in [
        "CREATE TABLE q (id INT PRIMARY KEY, queryid BIGINT NOT NULL, delta INT NOT NULL)",
        "INSERT INTO q (id, queryid, delta) VALUES (1, -8455582427799197754, -5)",
        "INSERT INTO q (id, queryid, delta) VALUES (2, 8455582427799197754, 5)",
        "INSERT INTO q (id, queryid, delta) VALUES (3, 0, 0)",
    ] {
        conn.execute(AssertSqlSafe(sql)).await.expect(sql);
    }

    for (label, sql, want) in [
        ("negative bigint", "SELECT id FROM q WHERE queryid = -8455582427799197754", vec![1]),
        ("positive bigint", "SELECT id FROM q WHERE queryid = 8455582427799197754", vec![2]),
        ("negative int", "SELECT id FROM q WHERE delta = -5", vec![1]),
        ("less than a negative", "SELECT id FROM q WHERE delta < -1", vec![1]),
        ("greater than a negative", "SELECT id FROM q WHERE delta > -1", vec![2, 3]),
        ("explicit plus", "SELECT id FROM q WHERE delta = +5", vec![2]),
    ] {
        let mut got: Vec<i32> = sqlx::query(AssertSqlSafe(sql))
            .fetch_all(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("{label}: {sql}: {e}"))
            .iter()
            .map(|r| r.try_get::<i32, _>("id").expect("id"))
            .collect();
        got.sort_unstable();
        assert_eq!(got, want, "{label}: {sql}");
    }
}
