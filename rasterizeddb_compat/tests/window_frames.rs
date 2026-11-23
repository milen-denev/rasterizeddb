//! Window frames and FILTER, against PostgreSQL 17's answers: `GROUPS`,
//! `RANGE` with an offset, `EXCLUDE`, and `FILTER (WHERE …)` on an aggregate
//! window call. Each was refused before ("'groups' is not a sort direction",
//! "a RANGE frame with an offset is not supported yet", "'EXCLUDE …' is not
//! part of a window frame", "'__agg_0 over (…)' is neither a column nor a
//! value").

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::raw_sql(
        "CREATE TABLE w (id INT, g INT, v INT);
         INSERT INTO w VALUES (1,1,10),(2,1,20),(3,1,20),(4,2,5),(5,2,NULL),(6,2,7);",
    )
    .execute(&pool)
    .await
    .expect("setup");
    (server, pool)
}

/// The second column of each row, in id order, NULL as `-`.
async fn col(pool: &sqlx::PgPool, sql: &str) -> String {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    rows.iter()
        .map(|r| r.try_get_unchecked::<Option<String>, _>(1).expect("text").unwrap_or_else(|| "-".into()))
        .collect::<Vec<_>>()
        .join(" ")
}

async fn code_of(pool: &sqlx::PgPool, sql: &str) -> String {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).execute(pool).await.expect_err(sql);
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread")]
async fn frames_answer_as_postgresql_does() {
    let (_s, pool) = server().await;
    for (sql, want) in [
        ("SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) FROM w ORDER BY id", "20 30 25 20 12 -"),
        ("SELECT id, sum(v) OVER (ORDER BY v GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w ORDER BY id", "17 50 50 5 40 12"),
        ("SELECT id, sum(v) OVER (ORDER BY v RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING) FROM w ORDER BY id", "22 40 40 22 - 22"),
        ("SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM w ORDER BY id", "10 30 50 55 55 62"),
        ("SELECT id, sum(v) OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP) FROM w ORDER BY id", "12 22 22 - 62 5"),
        ("SELECT id, sum(v) OVER (ORDER BY v DESC RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) FROM w ORDER BY id", "10 40 40 12 - 17"),
        ("SELECT id, sum(id) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM w ORDER BY id", "3 6 9 12 15 11"),
    ] {
        assert_eq!(col(&pool, sql).await, want, "{sql}");
    }
    assert_eq!(code_of(&pool, "SELECT sum(v) OVER (GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w").await, "42P20");
    assert_eq!(code_of(&pool, "SELECT sum(v) OVER (ORDER BY g, id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w").await, "42P20");
}

#[tokio::test(flavor = "multi_thread")]
async fn filter_leaves_rows_out_of_a_window_aggregate() {
    let (_s, pool) = server().await;
    for (sql, want) in [
        ("SELECT id, sum(v) FILTER (WHERE v > 5) OVER (PARTITION BY g ORDER BY id) FROM w ORDER BY id", "10 30 50 - - 7"),
        ("SELECT id, count(*) FILTER (WHERE v IS NULL) OVER () FROM w ORDER BY id", "1 1 1 1 1 1"),
        ("SELECT id, count(*) FILTER (WHERE v > 6) OVER () FROM w ORDER BY id", "4 4 4 4 4 4"),
    ] {
        assert_eq!(col(&pool, sql).await, want, "{sql}");
    }
    assert_eq!(code_of(&pool, "SELECT row_number() FILTER (WHERE v > 1) OVER (ORDER BY id) FROM w").await, "0A000");
}
