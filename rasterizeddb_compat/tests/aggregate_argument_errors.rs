//! An aggregate argument that fails to evaluate on one row fails the
//! statement, as PostgreSQL's does.
//!
//! The fold logged the error and accumulated the row as "no value":
//! `sum(1/(b-7))` answered the other rows' sum, `count(…)` one fewer, and a
//! FILTER that failed counted as rejecting the row — numbers that look like
//! answers.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor};

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_aggregate_argument_fails_the_statement() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE ae (id BIGINT PRIMARY KEY, b BIGINT, g BIGINT);
         INSERT INTO ae VALUES (1, 100, 1), (2, 7, 1), (3, 1, 2), (4, 2, 2);",
    ))
    .await
    .unwrap();
    for sql in [
        "SELECT sum(1/(b-7)) FROM ae",
        "SELECT count(1/(b-7)) FROM ae",
        "SELECT avg(10/(b-7)) FROM ae",
        "SELECT g, sum(1/(b-7)) FROM ae GROUP BY g",
        "SELECT count(*) FILTER (WHERE 1/(b-7) > 0) FROM ae",
        "SELECT g FROM ae GROUP BY g HAVING sum(1/(b-7)) > 0",
    ] {
        let e = sqlx::query(AssertSqlSafe(sql)).fetch_all(&pool).await.expect_err(sql);
        assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("22012"), "{sql}: {e}");
    }
    // A cast that cannot read a row's text is the input function's 22P02,
    // inside an aggregate and out.
    pool.execute(sqlx::raw_sql("CREATE TABLE at (id BIGINT PRIMARY KEY, t TEXT); INSERT INTO at VALUES (1, '5'), (2, 'x');"))
        .await
        .unwrap();
    for sql in ["SELECT sum(t::int) FROM at", "SELECT t::int FROM at ORDER BY id", "SELECT count(CAST(t AS BIGINT)) FROM at"] {
        let e = sqlx::query(AssertSqlSafe(sql)).fetch_all(&pool).await.expect_err(sql);
        assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("22P02"), "{sql}: {e}");
    }
    // Rows that evaluate are unaffected.
    let s: rust_decimal::Decimal = sqlx::query_scalar("SELECT sum(1/(b-6)) FROM ae").fetch_one(&pool).await.unwrap();
    assert_eq!(s, rust_decimal::Decimal::from(1));
    let n: i64 = sqlx::query_scalar("SELECT count(1/(b-6)) FROM ae").fetch_one(&pool).await.unwrap();
    assert_eq!(n, 4);
}
