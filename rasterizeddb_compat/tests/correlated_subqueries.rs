//! Correlated scalar subqueries, against PostgreSQL 17's answers.
//!
//! `SELECT id, (SELECT max(v) FROM cu WHERE cu.k = ct.k) FROM ct` and
//! `UPDATE ct SET x = (SELECT …  WHERE cu.k = ct.k)` were refused (0A000,
//! "write it as a join"). Both are run now as the `LATERAL` join they mean
//! (`rql::correlated`).

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::raw_sql(
        "CREATE TABLE ct (id INT PRIMARY KEY, k INT, x INT, n INT);
         CREATE TABLE cu (k INT, v INT);
         INSERT INTO ct VALUES (1, 1, 0, 0), (2, 2, 0, 0), (3, 9, 0, 0);
         INSERT INTO cu VALUES (1, 10), (1, 30), (2, 5);",
    )
    .execute(&pool)
    .await
    .expect("setup");
    (server, pool)
}

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

async fn code_of(pool: &sqlx::PgPool, sql: &str) -> String {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).execute(pool).await.expect_err(sql);
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_correlated_subquery_has_a_value_per_outer_row() {
    let (_s, pool) = server().await;
    assert_eq!(
        rows(&pool, "SELECT id, (SELECT max(v) FROM cu WHERE cu.k = ct.k) FROM ct ORDER BY id").await,
        ["1|30", "2|5", "3|NULL"]
    );
    assert_eq!(
        rows(
            &pool,
            "SELECT id, (SELECT max(v) FROM cu WHERE cu.k = ct.k) AS mx, (SELECT count(*) FROM cu WHERE cu.k = ct.k) \
             FROM ct ORDER BY id"
        )
        .await,
        ["1|30|2", "2|5|1", "3|NULL|0"]
    );
    assert_eq!(rows(&pool, "SELECT id FROM ct WHERE (SELECT count(*) FROM cu WHERE cu.k = ct.k) > 1 ORDER BY id").await, ["1"]);
    assert_eq!(rows(&pool, "SELECT t.id, (SELECT min(v) FROM cu WHERE cu.k = t.k) FROM ct t ORDER BY 1").await, ["1|10", "2|5", "3|NULL"]);
    // More than one row for an outer row is PostgreSQL's cardinality error.
    assert_eq!(code_of(&pool, "SELECT id, (SELECT v FROM cu WHERE cu.k = ct.k) FROM ct ORDER BY id").await, "21000");
    // Over the extended protocol, described and typed: the column is named
    // as PostgreSQL names it.
    let row = sqlx::query("SELECT id, (SELECT max(v) FROM cu WHERE cu.k = ct.k) FROM ct WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("prepared");
    use sqlx::Column;
    assert_eq!(row.columns()[1].name(), "max");
    assert_eq!(row.try_get::<Option<i32>, _>(1).expect("int"), Some(30));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_update_sets_each_row_from_its_own_subquery() {
    let (_s, pool) = server().await;
    sqlx::raw_sql("UPDATE ct SET x = (SELECT max(v) FROM cu WHERE cu.k = ct.k)").execute(&pool).await.expect("update");
    assert_eq!(rows(&pool, "SELECT id, x FROM ct ORDER BY id").await, ["1|30", "2|5", "3|NULL"]);
    sqlx::raw_sql("UPDATE ct SET n = (SELECT count(*) FROM cu WHERE cu.k = ct.k), x = x + 1 WHERE id < 3")
        .execute(&pool)
        .await
        .expect("update");
    assert_eq!(rows(&pool, "SELECT id, x, n FROM ct ORDER BY id").await, ["1|31|2", "2|6|1", "3|NULL|0"]);
    assert_eq!(code_of(&pool, "UPDATE ct SET x = (SELECT v FROM cu WHERE cu.k = ct.k) WHERE id = 1").await, "21000");
    assert_eq!(rows(&pool, "SELECT x FROM ct WHERE id = 1").await, ["31"]);
    let done = sqlx::query("UPDATE ct SET x = (SELECT min(v) FROM cu WHERE cu.k = ct.k) WHERE id = 2")
        .execute(&pool)
        .await
        .expect("prepared update");
    assert_eq!(done.rows_affected(), 1);
    assert_eq!(rows(&pool, "SELECT id, x FROM ct ORDER BY id").await, ["1|31", "2|5", "3|NULL"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn shapes_not_supported_yet_are_refused() {
    let (_s, pool) = server().await;
    assert_eq!(code_of(&pool, "SELECT k, (SELECT max(v) FROM cu WHERE cu.k = ct.k) FROM ct GROUP BY k").await, "0A000");
    assert_eq!(code_of(&pool, "SELECT (SELECT v, k FROM cu WHERE cu.k = ct.k) FROM ct").await, "42601");
}
