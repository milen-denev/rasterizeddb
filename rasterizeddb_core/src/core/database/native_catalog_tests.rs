//! The catalogue relations over the native protocol: `pg_tables`,
//! `information_schema.tables` and `pg_class` answer a native statement as
//! they answer one over pgwire, built per statement and dropped after it.
//! Native clients (oxyredox's store, `rasterizeddb_client`) ask them whether
//! a table exists before creating it; they used to get "Table not found" for
//! the view itself.

use rclite::Arc;

use super::*;

fn rows(response: &[u8], sql: &str) -> usize {
    assert_eq!(response.first(), Some(&2), "{sql}: {}", String::from_utf8_lossy(response));
    crate::core::row::row::vec_into_rows(&response[1..]).expect("rows").len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_statements_read_the_catalogue_views() {
    let dir = tempfile::tempdir().expect("dir");
    let database = Arc::new(Database::new(dir.path().to_str().expect("utf-8")).await);
    let created = run_native_query_for_tests("CREATE TABLE charges (id BIGINT PRIMARY KEY, amount BIGINT)", database.clone()).await;
    assert_ne!(created.first(), Some(&3), "{}", String::from_utf8_lossy(&created));
    for (sql, expected) in [
        ("SELECT tablename FROM pg_tables WHERE tablename = 'charges'", 1),
        ("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = 'charges'", 1),
        ("SELECT table_name FROM information_schema.tables WHERE table_name = 'charges'", 1),
        ("SELECT relname FROM pg_class WHERE relname = 'charges'", 1),
        ("SELECT column_name FROM information_schema.columns WHERE table_name = 'charges'", 2),
        ("SELECT tablename FROM pg_tables WHERE tablename = 'missing'", 0),
    ] {
        let response = run_native_query_for_tests(sql, database.clone()).await;
        assert_eq!(rows(&response, sql), expected, "{sql}");
    }
    // Nothing of the statement's own relations is left behind.
    assert!(
        database.table_names().iter().all(|n| !n.starts_with("__cat_")),
        "{:?}",
        database.table_names()
    );
    // An ordinary statement is untouched.
    let plain = run_native_query_for_tests("SELECT id FROM charges", database.clone()).await;
    assert_eq!(rows(&plain, "SELECT id FROM charges"), 0);
}
