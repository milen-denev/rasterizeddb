//! `ALTER TABLE … ADD` keeps what it declares.
//!
//! Three forms reported success and stored nothing (found by
//! rasterizeddb-eb's power-loss harness): `ADD [CONSTRAINT c] CHECK (…)`,
//! after which a violating insert succeeded; `ADD COLUMN m NUMERIC(p, s)`,
//! which stored every value at scale 9; and `ADD COLUMN g … GENERATED ALWAYS
//! AS (…) STORED`, which was never given a value. Expected answers are
//! PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn exec(pool: &sqlx::PgPool, sql: &str) {
    sqlx::query(AssertSqlSafe(sql)).execute(pool).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn refused(pool: &sqlx::PgPool, sql: &str, code: &str) {
    match sqlx::query(AssertSqlSafe(sql)).execute(pool).await {
        Ok(_) => panic!("{sql}: PostgreSQL refuses this with {code}, but it succeeded"),
        Err(e) => assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some(code), "{sql}: {e}"),
    }
}

async fn texts(pool: &sqlx::PgPool, sql: &str) -> Vec<Option<String>> {
    sqlx::query(AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.get::<Option<String>, _>(0))
        .collect()
}

fn some(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some(v.to_string())).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn add_check_is_enforced() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE t (id INT NOT NULL, a INT)").await;
    exec(&pool, "INSERT INTO t VALUES (1, 5)").await;

    exec(&pool, "ALTER TABLE t ADD CHECK (a > 0)").await;
    refused(&pool, "INSERT INTO t VALUES (2, -1)", "23514").await;
    exec(&pool, "ALTER TABLE t ADD CONSTRAINT a_small CHECK (a < 100)").await;
    refused(&pool, "INSERT INTO t VALUES (3, 500)", "23514").await;
    // NULL is not a violation.
    exec(&pool, "INSERT INTO t VALUES (4, NULL)").await;
    // A row already there that violates it refuses the ALTER.
    refused(&pool, "ALTER TABLE t ADD CONSTRAINT a_big CHECK (a > 10)", "23514").await;
    exec(&pool, "INSERT INTO t VALUES (5, 2)").await;
    // And a dropped one stops applying.
    exec(&pool, "ALTER TABLE t DROP CONSTRAINT a_small").await;
    exec(&pool, "INSERT INTO t VALUES (6, 500)").await;
    assert_eq!(texts(&pool, "SELECT CAST(id AS TEXT) FROM t ORDER BY id").await, some(&["1", "4", "5", "6"]));
}

#[tokio::test(flavor = "multi_thread")]
async fn add_numeric_column_keeps_its_scale() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    // On an empty table, and on one that holds rows, which is rebuilt.
    exec(&pool, "CREATE TABLE e (id INT NOT NULL)").await;
    exec(&pool, "ALTER TABLE e ADD COLUMN m NUMERIC(12,3)").await;
    exec(&pool, "INSERT INTO e VALUES (1, 2.5)").await;
    assert_eq!(texts(&pool, "SELECT CAST(m AS TEXT) FROM e").await, some(&["2.500"]));

    exec(&pool, "CREATE TABLE p (id INT NOT NULL)").await;
    exec(&pool, "INSERT INTO p VALUES (1)").await;
    exec(&pool, "ALTER TABLE p ADD COLUMN m NUMERIC(12, 3)").await;
    exec(&pool, "ALTER TABLE p ADD COLUMN d NUMERIC(10,2) DEFAULT 1.5").await;
    exec(&pool, "INSERT INTO p (id, m) VALUES (2, 1.23456)").await;
    assert_eq!(texts(&pool, "SELECT CAST(m AS TEXT) FROM p WHERE id = 2").await, some(&["1.235"]));
    assert_eq!(texts(&pool, "SELECT CAST(id AS TEXT) FROM p WHERE m = 1.235").await, some(&["2"]));
    assert_eq!(texts(&pool, "SELECT CAST(d AS TEXT) FROM p ORDER BY id").await, some(&["1.50", "1.50"]));
}

#[tokio::test(flavor = "multi_thread")]
async fn add_generated_column_is_computed() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE e (id INT NOT NULL, a INT)").await;
    exec(&pool, "ALTER TABLE e ADD COLUMN g INT GENERATED ALWAYS AS (a * 2) STORED").await;
    exec(&pool, "INSERT INTO e (id, a) VALUES (1, 4)").await;
    assert_eq!(texts(&pool, "SELECT CAST(g AS TEXT) FROM e").await, some(&["8"]));

    exec(&pool, "CREATE TABLE p (id INT NOT NULL, a INT)").await;
    exec(&pool, "INSERT INTO p VALUES (1, 5), (2, NULL)").await;
    exec(&pool, "ALTER TABLE p ADD COLUMN g INT GENERATED ALWAYS AS (a * 2) STORED").await;
    assert_eq!(
        texts(&pool, "SELECT CAST(g AS TEXT) FROM p ORDER BY id").await,
        vec![Some("10".to_string()), None]
    );
    exec(&pool, "INSERT INTO p (id, a) VALUES (3, 7)").await;
    exec(&pool, "UPDATE p SET a = 6 WHERE id = 1").await;
    assert_eq!(
        texts(&pool, "SELECT CAST(g AS TEXT) FROM p ORDER BY id").await,
        vec![Some("12".to_string()), None, Some("14".to_string())]
    );

    refused(&pool, "ALTER TABLE p ADD COLUMN h INT DEFAULT 1 GENERATED ALWAYS AS (a) STORED", "42601").await;
}
