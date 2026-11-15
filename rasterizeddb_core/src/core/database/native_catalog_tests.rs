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

fn context(database: &Arc<Database>, authentication_required: bool) -> NativeContext {
    NativeContext { database: database.clone(), authentication_required }
}

async fn native(context: &NativeContext, superuser: bool, sql: &str) -> Vec<u8> {
    process_incoming_queries(
        context.clone(),
        std::sync::Arc::new(rastcp::session::Principal::new("svc", superuser)),
        sql.as_bytes().to_vec(),
    )
    .await
}

fn the_line(response: &[u8]) -> String {
    let rows = crate::core::row::row::vec_into_rows(&response[1..]).expect("rows");
    assert_eq!(rows.len(), 1);
    String::from_utf8(rows[0].columns[0].data.into_slice().to_vec()).expect("utf-8")
}

/// `SELECT rdb_index_checkpoint('t')` over the native protocol, as pgwire
/// answers it: one row saying how many file sets were left clean, the
/// superuser gate where callers authenticate, and the parse errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_statements_checkpoint_a_table() {
    let dir = tempfile::tempdir().expect("dir");
    let database = Arc::new(Database::new(dir.path().to_str().expect("utf-8")).await);
    let open = context(&database, false);
    for sql in ["CREATE TABLE charges (id BIGINT PRIMARY KEY, amount BIGINT)", "INSERT INTO charges (id, amount) VALUES (1, 5), (2, 7)"] {
        let r = native(&open, false, sql).await;
        assert_ne!(r.first(), Some(&3), "{sql}: {}", String::from_utf8_lossy(&r));
    }
    let r = native(&open, false, "SELECT rdb_index_checkpoint('charges')").await;
    assert_eq!(r.first(), Some(&2), "{}", String::from_utf8_lossy(&r));
    assert_eq!(the_line(&r), "1 of 1 clean");
    // Sealed already: still clean, nothing new to write.
    let again = native(&open, false, "select rdb_index_checkpoint('charges'::regclass);").await;
    assert_eq!(the_line(&again), "1 of 1 clean");

    let r = native(&open, false, "SELECT rdb_index_checkpoint('missing')").await;
    assert_eq!(r.first(), Some(&3));
    assert!(String::from_utf8_lossy(&r).contains("does not exist"), "{}", String::from_utf8_lossy(&r));
    let r = native(&open, false, "SELECT rdb_index_checkpoint(charges)").await;
    assert_eq!(r.first(), Some(&3));
    assert!(String::from_utf8_lossy(&r).contains("string literal"), "{}", String::from_utf8_lossy(&r));

    // Where callers authenticate, a superuser's statement only.
    let gated = context(&database, true);
    let r = native(&gated, false, "SELECT rdb_index_checkpoint('charges')").await;
    assert_eq!(r.first(), Some(&3));
    assert!(String::from_utf8_lossy(&r).contains("requires superuser"), "{}", String::from_utf8_lossy(&r));
    let r = native(&gated, true, "SELECT rdb_index_checkpoint('charges')").await;
    assert_eq!(the_line(&r), "1 of 1 clean");
}
