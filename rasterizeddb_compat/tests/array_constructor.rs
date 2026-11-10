//! `ARRAY[…]`, the array constructor, against PostgreSQL 17's answers.
//!
//! The select-list splitters cut it at its commas and the evaluator had no
//! constructor, so every use failed: `SELECT ARRAY[a, b]` with "Column
//! 'ARRAY[a' not found", `array_length(ARRAY[a, b], 1)` and `INSERT … VALUES
//! (ARRAY[3, 4])` likewise.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::raw_sql("CREATE TABLE sp (a INT, b INT, c TEXT); INSERT INTO sp VALUES (1, 2, 'x'), (3, 4, 'y');")
        .execute(&pool)
        .await
        .expect("setup");
    (server, pool)
}

/// Each row's columns as text, over the simple protocol.
async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_constructor_builds_an_array_wherever_a_value_goes() {
    let (_s, pool) = server().await;
    assert_eq!(rows(&pool, "SELECT ARRAY[a, b] FROM sp ORDER BY a").await, ["{1,2}", "{3,4}"]);
    assert_eq!(rows(&pool, "SELECT ARRAY[a, b] AS ab, c FROM sp ORDER BY a").await, ["{1,2}|x", "{3,4}|y"]);
    assert_eq!(rows(&pool, "SELECT c, ARRAY[a, b, 7] FROM sp ORDER BY a").await, ["x|{1,2,7}", "y|{3,4,7}"]);
    assert_eq!(rows(&pool, "SELECT ARRAY[1,2,3]").await, ["{1,2,3}"]);
    assert_eq!(rows(&pool, "SELECT ARRAY[1.5, 2]").await, ["{1.5,2}"]);
    assert_eq!(rows(&pool, "SELECT ARRAY['p', c] FROM sp ORDER BY a").await, ["{p,x}", "{p,y}"]);
    assert_eq!(rows(&pool, "SELECT ARRAY[a, NULL, b] FROM sp ORDER BY a").await, ["{1,NULL,2}", "{3,NULL,4}"]);
    assert_eq!(
        rows(&pool, "SELECT ARRAY[c, 'q\"r', 'a b'] FROM sp ORDER BY a").await,
        ["{x,\"q\\\"r\",\"a b\"}", "{y,\"q\\\"r\",\"a b\"}"]
    );
    assert_eq!(rows(&pool, "SELECT array_length(ARRAY[a, b], 1) FROM sp ORDER BY a").await, ["2", "2"]);
    assert_eq!(rows(&pool, "SELECT ARRAY[a, b] = ARRAY[1, 2] FROM sp ORDER BY a").await, ["t", "f"]);
    assert_eq!(rows(&pool, "SELECT ARRAY[]::int[]").await, ["{}"]);
    sqlx::raw_sql("CREATE TABLE ia (x INT[]); INSERT INTO ia VALUES (ARRAY[3, 04]);").execute(&pool).await.expect("insert");
    assert_eq!(rows(&pool, "SELECT x FROM ia").await, ["{3,4}"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_constructed_array_orders_as_its_elements_type() {
    let (_s, pool) = server().await;
    sqlx::raw_sql("INSERT INTO sp VALUES (10, 0, 'z')").execute(&pool).await.expect("insert");
    assert_eq!(
        rows(&pool, "SELECT x FROM (SELECT ARRAY[a, b] AS x FROM sp) s ORDER BY x DESC").await,
        ["{10,0}", "{3,4}", "{1,2}"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unnamed_constructor_is_called_array() {
    let (_s, pool) = server().await;
    let row = sqlx::query("SELECT ARRAY[a, b] FROM sp WHERE a = 1").fetch_one(&pool).await.expect("select");
    use sqlx::Column;
    assert_eq!(row.columns()[0].name(), "array");
}
