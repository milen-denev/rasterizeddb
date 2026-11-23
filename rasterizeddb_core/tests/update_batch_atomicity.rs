//! A failing UPDATE changes nothing, however many log batches it ran.
//!
//! A long statement commits its storage transaction in batches once the log
//! passes its checkpoint threshold (`wal::should_break_transaction`), so the
//! log can be reclaimed. A version written in a committed batch is still
//! undone by the statement's abort, which makes it invisible. An in-place
//! overwrite is not: it kept no previous version. So an UPDATE that
//! overwrote rows in place, broke a batch, then failed on a later row left
//! the earlier rows changed. Reported by rasterizeddb-eb. PostgreSQL rejects
//! the whole statement and changes no row.
//!
//! Its own binary because the threshold is process-wide (set once).

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS, WAL_CHECKPOINT_BYTES,
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
fn a_failing_update_changes_no_row_across_batches() {
    MAX_PERMITS_THREADS.get_or_init(|| 4);
    BATCH_SIZE.get_or_init(|| 1_000);
    let _ = WAL_CHECKPOINT_BYTES.set(8 * 1024);
    on_a_big_stack(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let db = Arc::new(Database::new_with_backend(&path, false).await);
        assert_eq!(run(&db, "CREATE TABLE t (id BIGINT NOT NULL, n BIGINT NOT NULL)").await[0], 0);
        for batch in 0..20 {
            let values: Vec<String> = (0..100).map(|i| format!("({}, 10)", batch * 100 + i)).collect();
            let r = run(&db, &format!("INSERT INTO t (id, n) VALUES {}", values.join(", "))).await;
            assert_ne!(r[0], 3, "{}", String::from_utf8_lossy(&r[1..]));
        }
        // The last row makes the statement fail.
        assert_ne!(run(&db, "INSERT INTO t (id, n) VALUES (99999, 3)").await[0], 3);

        let response = run(&db, "UPDATE t SET n = 100 / (n - 3)").await;
        assert_eq!(response[0], 3, "the update was expected to fail on its last row");

        let unchanged = count_of(&run(&db, "SELECT COUNT(*) FROM t WHERE n = 10").await);
        assert_eq!(unchanged, 2000, "a failed UPDATE left {} of 2000 rows changed", 2000 - unchanged);

        // A failure found while writing: a CHECK only the last row breaks.
        assert_eq!(run(&db, "CREATE TABLE c (id BIGINT NOT NULL, n BIGINT NOT NULL CHECK (n < 50))").await[0], 0);
        for batch in 0..20 {
            let values: Vec<String> = (0..100).map(|i| format!("({}, 10)", batch * 100 + i)).collect();
            assert_ne!(run(&db, &format!("INSERT INTO c (id, n) VALUES {}", values.join(", "))).await[0], 3);
        }
        assert_ne!(run(&db, "INSERT INTO c (id, n) VALUES (99999, 49)").await[0], 3);
        let response = run(&db, "UPDATE c SET n = n + 1").await;
        assert_eq!(response[0], 3, "the update was expected to fail on its last row");
        let unchanged = count_of(&run(&db, "SELECT COUNT(*) FROM c WHERE n = 10").await);
        assert_eq!(unchanged, 2000, "a failed UPDATE left {} of 2000 rows changed", 2000 - unchanged);

        // A constant assignment, which may go in place, failing on the last
        // row through a CHECK over another column.
        assert_eq!(run(&db, "CREATE TABLE k (id BIGINT NOT NULL, n BIGINT NOT NULL, m BIGINT NOT NULL, CHECK (n < m))").await[0], 0);
        for batch in 0..20 {
            let values: Vec<String> = (0..100).map(|i| format!("({}, 1, 100)", batch * 100 + i)).collect();
            assert_ne!(run(&db, &format!("INSERT INTO k (id, n, m) VALUES {}", values.join(", "))).await[0], 3);
        }
        assert_ne!(run(&db, "INSERT INTO k (id, n, m) VALUES (99999, 1, 5)").await[0], 3);
        // A constant assignment goes in place, row by row, so this is the
        // case that left earlier rows overwritten: it must be refused, and
        // refused before any row changes.
        for sql in ["UPDATE k SET n = 7", "UPDATE k SET n = n + 6"] {
            let response = run(&db, sql).await;
            assert_eq!(response[0], 3, "{sql}: the last row violates CHECK (n < m)");
            assert!(String::from_utf8_lossy(&response[1..]).contains("violates check constraint"), "{sql}");
            let unchanged = count_of(&run(&db, "SELECT COUNT(*) FROM k WHERE n = 1").await);
            assert_eq!(unchanged, 2001, "{sql}: a failed UPDATE left {} of 2001 rows changed", 2001 - unchanged);
        }
        // Rows the check allows still update, in place or not.
        assert_eq!(run(&db, "UPDATE k SET n = 4").await[0], 0);
        assert_eq!(count_of(&run(&db, "SELECT COUNT(*) FROM k WHERE n = 4").await), 2001);
    });
}
