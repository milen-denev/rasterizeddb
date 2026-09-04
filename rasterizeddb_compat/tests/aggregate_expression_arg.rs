//! An aggregate whose argument is computed rather than read.
//!
//! `SUM(CAST(NULLIF(data ->> 'y', '') AS NUMERIC))` is how PMS aggregates a
//! JSONB field for its Insights charts. It failed with "Column '…' not found in
//! schema", naming the whole expression as though it were a column.

use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use rasterizeddb_compat::TestServer;
use rust_decimal::Decimal;

async fn seeded() -> (TestServer, sqlx::PgConnection) {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    conn.execute(AssertSqlSafe(
        "CREATE TABLE rows_t (id INT PRIMARY KEY, grp INT NOT NULL, n INT NOT NULL, data JSONB)",
    ))
    .await
    .expect("create");
    for (id, grp, n, doc) in [
        (1, 1, 10, r#"{"y": "5"}"#),
        (2, 1, 20, r#"{"y": "7"}"#),
        // An empty string, which NULLIF turns into NULL: it must be skipped,
        // not counted as a zero.
        (3, 1, 30, r#"{"y": ""}"#),
        (4, 2, 40, r#"{"y": "100"}"#),
    ] {
        conn.execute(AssertSqlSafe(format!(
            "INSERT INTO rows_t (id, grp, n, data) VALUES ({id}, {grp}, {n}, '{doc}')"
        )))
        .await
        .expect("insert");
    }
    (server, conn)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_aggregate_over_a_document_field() {
    let (_server, mut conn) = seeded().await;

    let total: Decimal = sqlx::query(AssertSqlSafe(
        "SELECT SUM(CAST(NULLIF(data ->> 'y', '') AS NUMERIC)) AS total FROM rows_t",
    ))
    .fetch_one(&mut conn)
    .await
    .expect("SUM over a document field")
    .try_get("total")
    .expect("total");
    // 5 + 7 + 100. The empty string is NULL and contributes nothing — a zero
    // there would make this 112 either way, so the average below is the test
    // that actually distinguishes them.
    assert_eq!(total, Decimal::from(112), "the empty value must be skipped");

    let average: Decimal = sqlx::query(AssertSqlSafe(
        "SELECT AVG(CAST(NULLIF(data ->> 'y', '') AS NUMERIC)) AS a FROM rows_t WHERE grp = 1",
    ))
    .fetch_one(&mut conn)
    .await
    .expect("AVG over a document field")
    .try_get("a")
    .expect("a");
    // (5 + 7) / 2, not / 3: a NULL is not a zero.
    assert_eq!(average, Decimal::from(6), "a NULL must not count in the divisor");
}

/// Grouped, and beside an ordinary aggregate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_computed_argument_beside_a_plain_one() {
    let (_server, mut conn) = seeded().await;
    let rows = sqlx::query(AssertSqlSafe(
        "SELECT grp, COUNT(*) AS c, \
                MIN(CAST(NULLIF(data ->> 'y', '') AS NUMERIC)) AS lo, \
                MAX(CAST(NULLIF(data ->> 'y', '') AS NUMERIC)) AS hi, \
                SUM(n) AS sum_n \
         FROM rows_t GROUP BY grp ORDER BY grp",
    ))
    .fetch_all(&mut conn)
    .await
    .expect("a computed argument beside a plain one");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].try_get::<i64, _>("c").expect("c"), 3);
    assert_eq!(rows[0].try_get::<Decimal, _>("lo").expect("lo"), Decimal::from(5));
    assert_eq!(rows[0].try_get::<Decimal, _>("hi").expect("hi"), Decimal::from(7));
    assert_eq!(rows[0].try_get::<i64, _>("sum_n").expect("sum_n"), 60);
    assert_eq!(rows[1].try_get::<Decimal, _>("hi").expect("hi"), Decimal::from(100));
}
