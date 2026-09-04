//! Aggregates over a cast argument, checked against known truth.
//!
//! `SUM(CAST(x AS NUMERIC))` returned **0** — not an error, a number. The cast
//! from an integer to a decimal was refused, the aggregate swallowed that
//! refusal as a NULL, and `SUM` of no values is zero. On a chart over a quiet
//! table nobody would question it.
//!
//! Every expectation here is computed independently and written out, because
//! the failure mode is a plausible value: a test that only checked "does it
//! run" passed throughout, and one that compared against the engine's own
//! answer would have agreed with it.

use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use rasterizeddb_compat::TestServer;
use rust_decimal::Decimal;

/// `co` is 1..=10, so: sum 55, sum of (co + 1) 65, avg 5.5, max 10.
/// `big` is co * 100, so: sum 5500, max 1000.
async fn seeded() -> (TestServer, sqlx::PgConnection) {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    conn.execute(AssertSqlSafe(
        "CREATE TABLE t (id INT PRIMARY KEY, co INT NOT NULL, big BIGINT NOT NULL)",
    ))
    .await
    .expect("create");
    for i in 1..=10i64 {
        conn.execute(AssertSqlSafe(format!(
            "INSERT INTO t (id, co, big) VALUES ({i}, {i}, {})",
            i * 100
        )))
        .await
        .expect("insert");
    }
    (server, conn)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cast_argument_aggregates_to_its_real_value() {
    let (_server, mut conn) = seeded().await;

    for (sql, expected) in [
        ("SELECT SUM(CAST(co AS NUMERIC)) AS v FROM t", "55"),
        ("SELECT AVG(CAST(co AS NUMERIC)) AS v FROM t", "5.5"),
        ("SELECT MIN(CAST(co AS NUMERIC)) AS v FROM t", "1"),
        ("SELECT MAX(CAST(big AS NUMERIC)) AS v FROM t", "1000"),
        ("SELECT SUM(CAST(big AS NUMERIC)) AS v FROM t", "5500"),
        // The `::` spelling must agree with the CAST(…) one.
        ("SELECT SUM(co::numeric) AS v FROM t", "55"),
    ] {
        let got: Decimal = sqlx::query(AssertSqlSafe(sql))
            .fetch_one(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .try_get("v")
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(
            got,
            expected.parse::<Decimal>().expect("expected"),
            "{sql} — a zero here means the cast failed and was swallowed"
        );
    }
}

/// The integer path must keep working, and agree with the decimal one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn integer_and_decimal_paths_agree() {
    let (_server, mut conn) = seeded().await;

    let plain: i64 = sqlx::query(AssertSqlSafe("SELECT SUM(co) AS v FROM t"))
        .fetch_one(&mut conn)
        .await
        .expect("plain sum")
        .try_get("v")
        .expect("v");
    assert_eq!(plain, 55);

    let arithmetic: i64 = sqlx::query(AssertSqlSafe("SELECT SUM(co + 1) AS v FROM t"))
        .fetch_one(&mut conn)
        .await
        .expect("arithmetic argument")
        .try_get("v")
        .expect("v");
    assert_eq!(arithmetic, 65, "ten rows, each one greater");

    let widened: i64 = sqlx::query(AssertSqlSafe("SELECT SUM(CAST(co AS BIGINT)) AS v FROM t"))
        .fetch_one(&mut conn)
        .await
        .expect("widening cast")
        .try_get("v")
        .expect("v");
    assert_eq!(widened, plain, "a widening cast must not change the sum");
}
