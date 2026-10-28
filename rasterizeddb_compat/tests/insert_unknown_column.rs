//! A write naming a column its table does not have is 42703, before anything
//! is written.
//!
//! `INSERT INTO pt ("no such column") VALUES (1)` answered 23502 about another
//! column, or XX000 "Cannot insert row due to schema mismatch" when the other
//! columns were nullable; `INSERT … SELECT` from an empty source reported
//! success. No row was ever written without the value, but a client branching
//! on 42703 (a migration probe, an ORM's schema check) never saw one.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor};

fn code(e: &sqlx::Error) -> String {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| e.to_string())
}

struct Root(String);

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn unknown_columns(partitioning: &str) {
    let root = std::env::temp_dir().join(format!("insert_unknown_column_{}_{}", std::process::id(), partitioning.len()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("root");
    let root = Root(root.to_string_lossy().into_owned());
    let server = TestServer::start_file_backed(&root.0).await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let p = partitioning;
    pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE TABLE pt (id BIGINT PRIMARY KEY, a BIGINT NOT NULL, b TEXT){p};
         CREATE TABLE nn (id BIGINT PRIMARY KEY, a BIGINT, b TEXT){p};
         CREATE TABLE src (id BIGINT PRIMARY KEY, a BIGINT);
         INSERT INTO src VALUES (5, 5);
         INSERT INTO nn VALUES (1, 1, 'x');"
    ))))
    .await
    .unwrap();
    for sql in [
        r#"INSERT INTO pt ("no such column") VALUES (1)"#,
        r#"INSERT INTO nn ("no such column") VALUES (1)"#,
        "INSERT INTO nn (id, zz) VALUES (2, 5)",
        "INSERT INTO nn (id, a, zz) VALUES (2, 3, 'x')",
        "INSERT INTO nn (id, zz) VALUES (2, 1), (3, 2)",
        "INSERT INTO nn (id, zz) SELECT id, a FROM src",
        "INSERT INTO nn (id, zz) SELECT id, a FROM src WHERE id < 0",
        "INSERT INTO nn (id, zz) VALUES (1, 2) ON CONFLICT (id) DO NOTHING",
        "INSERT INTO nn (id, a) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET zz = 3",
        "UPDATE nn SET zz = 1",
        "UPDATE nn SET a = 9, zz = 2 WHERE id = 1",
        "UPDATE nn SET zz = src.a FROM src WHERE nn.id = 1 AND src.id = 5",
    ] {
        let e = sqlx::query(AssertSqlSafe(sql)).execute(&pool).await.expect_err(sql);
        assert_eq!(code(&e), "42703", "{p}: {sql}: {e}");
    }
    let rows: Vec<(i64, Option<i64>, Option<String>)> =
        sqlx::query_as("SELECT id, a, b FROM nn ORDER BY id").fetch_all(&pool).await.unwrap();
    assert_eq!(rows, [(1, Some(1), Some("x".to_string()))], "{p}: a refused write left a row");
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pt").fetch_one(&pool).await.unwrap();
    assert_eq!(n, 0, "{p}");
    // A quoted name that does exist is still one.
    sqlx::query(r#"INSERT INTO nn (id, "a") VALUES (2, 2)"#).execute(&pool).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_column_is_42703_on_a_plain_table() {
    unknown_columns("").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_column_is_42703_on_a_hash_partitioned_table() {
    unknown_columns(" PARTITION BY HASH (id) PARTITIONS 4").await;
}
