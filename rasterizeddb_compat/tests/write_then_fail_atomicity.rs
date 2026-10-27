//! An autocommit statement that fails after its table write leaves nothing.
//!
//! An autocommit statement may overwrite a row in place, which its abort
//! cannot undo. `UPDATE t2 SET v = 7 WHERE id = 1 RETURNING 10/(v-7)` failed
//! with 22012 (RETURNING is evaluated after the write) and stored v = 7.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

struct Root(String);

impl Root {
    fn new(tag: &str) -> Self {
        let r = std::env::temp_dir().join(format!("write_then_fail_atomicity_{}_{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&r);
        std::fs::create_dir_all(&r).expect("root");
        Root(r.to_string_lossy().into_owned())
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn code(e: &sqlx::Error) -> String {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_default()
}

async fn values(pool: &sqlx::PgPool, table: &str) -> Vec<(i64, i64)> {
    sqlx::query(AssertSqlSafe(format!("SELECT id, v FROM {table} ORDER BY id")))
        .fetch_all(pool)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<i64, _>(0), r.get::<i64, _>(1)))
        .collect()
}

async fn returning_after_the_write(partitioning: &str) {
    let dir = Root::new(if partitioning.is_empty() { "plain" } else { "hash" });
    let server = TestServer::start_file_backed(&dir.0).await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE TABLE t2 (id BIGINT PRIMARY KEY, v BIGINT, CHECK (v < 1000)){partitioning};
         INSERT INTO t2 VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8);"
    ))))
    .await
    .unwrap();
    let before = values(&pool, "t2").await;
    for (sql, want) in [
        ("UPDATE t2 SET v = 7 WHERE id = 1 RETURNING 10/(v-7)", "22012"),
        ("UPDATE t2 SET v = v + 10 RETURNING 10/(v-17)", "22012"),
        ("INSERT INTO t2 VALUES (1, 7) ON CONFLICT (id) DO UPDATE SET v = 7 RETURNING 10/(v-7)", "22012"),
        // Fanned out: the row past the CHECK may be in a later partition
        // than rows already written.
        ("UPDATE t2 SET v = v * 200", "23514"),
    ] {
        let e = pool.execute(AssertSqlSafe(sql)).await.expect_err(sql);
        assert_eq!(code(&e), want, "{partitioning} {sql}: {e}");
        assert_eq!(values(&pool, "t2").await, before, "{partitioning} {sql}: its write survived its failure");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_statement_leaves_no_write_on_a_plain_table() {
    returning_after_the_write("").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_statement_leaves_no_write_on_a_hash_partitioned_table() {
    returning_after_the_write(" PARTITION BY HASH (id) PARTITIONS 4").await;
}
