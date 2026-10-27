//! A failing UPDATE that overwrote rows in place, with no write-ahead log.
//!
//! Without a log an in-place overwrite goes straight to the data file. An
//! UPDATE that overwrote rows and then failed on a later one (a CHECK) left
//! the earlier rows changed, and the filter, whose maintenance is staged
//! until the commit, still described their old values: `WHERE n = <new>`
//! pruned rows the table holds. PostgreSQL changes no row.
//!
//! Its own binary because the log switch is process-wide (set once).

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS, WAL_ENABLED,
    core::{database::Database, rql::{executor::execute, lexer_s1::recognize_query_purpose}},
};
use rclite::Arc;

fn on_a_big_stack<F: std::future::Future<Output = ()> + Send + 'static>(body: F) {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("a tokio runtime")
                .block_on(body);
        })
        .expect("a test thread")
        .join()
        .expect("the test thread panicked");
}

async fn run(db: &Arc<Database>, q: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(q).unwrap_or_else(|| panic!("unparsed: {q}"));
    execute(purpose, db.clone()).await
}

fn count_of(response: &[u8]) -> u64 {
    assert_eq!(response.first(), Some(&2u8), "not a row set: {:?}", String::from_utf8_lossy(&response[1..]));
    let rows = rasterizeddb_core::core::row::row::vec_into_rows(&response[1..]).expect("rows");
    let cell = rows[0].columns[0].data.into_slice();
    u64::from_le_bytes(cell[..8].try_into().unwrap())
}

#[test]
fn a_failing_in_place_update_without_a_log_changes_no_row() {
    MAX_PERMITS_THREADS.get_or_init(|| 4);
    BATCH_SIZE.get_or_init(|| 1_000);
    let _ = WAL_ENABLED.set(false);
    rasterizeddb_core::core::filter::settings::settings().update_table("ipc", |o| o.min_table_rows = Some(0));
    on_a_big_stack(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let db = Arc::new(Database::new_with_backend(&path, false).await);
        // A constant assignment (what may overwrite in place) that only the
        // last row's CHECK refuses.
        assert_eq!(run(&db, "CREATE TABLE ipc (id BIGINT NOT NULL, n BIGINT NOT NULL, m BIGINT NOT NULL, CHECK (n + m < 100))").await[0], 0);
        let values: Vec<String> = (0..300).map(|i| format!("({i}, {}, {})", i % 10, i % 10)).collect();
        assert_ne!(run(&db, &format!("INSERT INTO ipc (id, n, m) VALUES {}", values.join(", "))).await[0], 3);
        assert_ne!(run(&db, "INSERT INTO ipc (id, n, m) VALUES (99999, 0, 60)").await[0], 3);
        let table = db.table_for_tests("ipc").unwrap();
        let before = table.in_place_column_updates();
        let response = run(&db, "UPDATE ipc SET n = 50").await;
        assert_eq!(response[0], 3, "the update was expected to fail on its last row");
        assert!(table.in_place_column_updates() > before, "nothing was overwritten in place, so this tests nothing");
        for (v, want) in [(50u64, 0u64), (0, 31), (1, 30), (9, 30)] {
            let scanned = count_of(&run(&db, &format!("SELECT COUNT(*) FROM ipc WHERE n + 0 = {v}")).await);
            assert_eq!(scanned, want, "n = {v}: a failed UPDATE left rows changed");
            let filtered = count_of(&run(&db, &format!("SELECT COUNT(*) FROM ipc WHERE n = {v}")).await);
            assert_eq!(filtered, scanned, "WHERE n = {v}: the filter answered {filtered}, the table holds {scanned}");
        }
        // And it still overwrites in place when nothing fails.
        assert_ne!(run(&db, "UPDATE ipc SET n = 7 WHERE id = 3").await[0], 3);
        assert_eq!(count_of(&run(&db, "SELECT COUNT(*) FROM ipc WHERE n = 7").await), 31);
    });
}
