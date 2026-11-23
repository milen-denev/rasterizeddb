//! Three gaps found side by side with PostgreSQL 17, each an explicit error:
//!
//! - an UPDATE or DELETE whose WHERE holds an EXISTS comparing the row by
//!   anything but `=` ("a correlated EXISTS can compare an outer column only
//!   with `=`") — SELECT answered it, the writes did not;
//! - `UPDATE … SET v = (correlated subquery) RETURNING …` ("a correlated
//!   subquery in an UPDATE with RETURNING is not supported yet");
//! - `date + timetz` ("operator does not exist").
//!
//! Every expectation is PostgreSQL 17's, the self-reading writes included:
//! the subquery reads the table as the statement found it, not the rows it
//! has already written.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn start(setup: &str) -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    if !setup.is_empty() {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(setup.to_string()))).await.expect("setup");
    }
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

async fn sorted(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let mut out = rows(pool, sql).await;
    out.sort();
    out
}

async fn code(pool: &sqlx::PgPool, sql: &str) -> String {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_all(pool).await.expect_err(sql);
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| format!("no code: {e}"))
}

fn v(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_whose_exists_compares_by_more_than_equality() {
    let (_server, pool) = start(
        "CREATE TABLE ev (id int PRIMARY KEY, g int, v int); \
         INSERT INTO ev VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 30), (5, 3, NULL)",
    )
    .await;
    assert_eq!(
        sorted(&pool, "UPDATE ev SET v = 0 WHERE EXISTS (SELECT 1 FROM ev e2 WHERE e2.v > ev.v) RETURNING id").await,
        v(&["1", "2"])
    );
    assert_eq!(rows(&pool, "SELECT id, v FROM ev ORDER BY id").await, v(&["1|0", "2|0", "3|30", "4|30", "5|NULL"]));
    assert_eq!(
        sorted(&pool, "DELETE FROM ev WHERE EXISTS (SELECT 1 FROM ev e2 WHERE e2.id > ev.id) RETURNING id").await,
        v(&["1", "2", "3", "4"])
    );
    assert_eq!(rows(&pool, "SELECT id FROM ev ORDER BY id").await, v(&["5"]));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_subquery_reads_the_table_as_the_write_found_it() {
    let (_server, pool) = start(
        "CREATE TABLE w (id int PRIMARY KEY, g int, v int); \
         INSERT INTO w VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40), (5, 3, NULL)",
    )
    .await;
    // Every row but the largest gains 100, judged on the values before any.
    assert_eq!(
        sorted(&pool, "UPDATE w SET v = v + 100 WHERE EXISTS (SELECT 1 FROM w w2 WHERE w2.v > w.v) RETURNING id, v")
            .await,
        v(&["1|110", "2|120", "3|130"])
    );
    // NOT EXISTS, an alias, and another condition beside it.
    pool.execute(
        "UPDATE w AS a SET v = 1 WHERE NOT EXISTS (SELECT 1 FROM w b WHERE b.g = a.g AND b.id < a.id) AND a.g < 3",
    )
    .await
    .unwrap();
    assert_eq!(rows(&pool, "SELECT id, v FROM w ORDER BY id").await, v(&["1|1", "2|120", "3|1", "4|40", "5|NULL"]));
    pool.execute("DELETE FROM w WHERE g = 2 AND EXISTS (SELECT 1 FROM w x WHERE x.g = w.g AND x.id <> w.id AND x.v >= w.v)")
        .await
        .unwrap();
    assert_eq!(rows(&pool, "SELECT id FROM w ORDER BY id").await, v(&["1", "2", "4", "5"]));
    assert_eq!(
        rows(&pool, "DELETE FROM w d WHERE NOT EXISTS (SELECT 1 FROM w x WHERE x.id < d.id) RETURNING id").await,
        v(&["1"])
    );
    assert_eq!(rows(&pool, "SELECT id FROM w ORDER BY id").await, v(&["2", "4", "5"]));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_update_setting_a_correlated_subquery_returns_its_rows() {
    let (_server, pool) =
        start("CREATE TABLE u7 (id int PRIMARY KEY, g int, v int); INSERT INTO u7 VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)")
            .await;
    assert_eq!(
        sorted(&pool, "UPDATE u7 SET v = (SELECT max(x.v) FROM u7 x WHERE x.g = u7.g) RETURNING id, v").await,
        v(&["1|20", "2|20", "3|30"])
    );
    assert_eq!(rows(&pool, "SELECT id, v FROM u7 ORDER BY id").await, v(&["1|20", "2|20", "3|30"]));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_date_plus_a_time_with_its_zone_is_an_instant() {
    let (_server, pool) = start("").await;
    assert_eq!(rows(&pool, "SELECT date '2026-01-02' + timetz '10:00+02'").await, v(&["2026-01-02 08:00:00+00"]));
    assert_eq!(rows(&pool, "SELECT timetz '23:30-05' + date '2026-01-02'").await, v(&["2026-01-03 04:30:00+00"]));
    assert_eq!(
        rows(&pool, "SELECT pg_typeof(date '2026-01-02' + timetz '10:00+02')").await,
        v(&["timestamp with time zone"])
    );
    assert_eq!(code(&pool, "SELECT date '2026-01-02' - timetz '10:00+02'").await, "42883");
}
