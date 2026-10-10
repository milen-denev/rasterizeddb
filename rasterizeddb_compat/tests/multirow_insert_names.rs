//! A multi-row `INSERT` whose table or column name contains `values`.
//!
//! The keyword search accepted `VALUES` inside `raw_values` because `_` was not
//! counted as part of a name. The statement was split at the column list, the
//! tuple splitter gave up, and the single-row fallback stored the first tuple
//! and silently dropped the rest.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

#[tokio::test(flavor = "multi_thread")]
async fn every_tuple_is_stored_when_a_name_contains_values() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");

    for (table, column) in [
        ("order_values", "v"),
        ("plain", "raw_values"),
        ("plain2", "values_x"),
    ] {
        sqlx::query(AssertSqlSafe(format!(
            "CREATE TABLE {} (id INTEGER NOT NULL, {} INTEGER NOT NULL)",
            table, column
        )))
        .execute(&pool)
        .await
        .unwrap_or_else(|e| panic!("CREATE {} failed: {}", table, e));

        let done = sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {} (id, {}) VALUES (1, 10), (2, 20), (3, 30)",
            table, column
        )))
        .execute(&pool)
        .await
        .unwrap_or_else(|e| panic!("INSERT into {} failed: {}", table, e));
        assert_eq!(done.rows_affected(), 3, "{}.{}: rows reported", table, column);

        let ids: Vec<i32> = sqlx::query(AssertSqlSafe(format!(
            "SELECT id FROM {} ORDER BY id",
            table
        )))
        .fetch_all(&pool)
        .await
        .expect("select")
        .iter()
        .map(|r| r.get::<i32, _>(0))
        .collect();
        assert_eq!(ids, vec![1, 2, 3], "{}.{}: rows stored", table, column);
    }
}
