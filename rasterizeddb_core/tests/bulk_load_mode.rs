//! Rows appended under bulk-load mode are there after the final sync.
//!
//! The mode drops the per-append log record and flush for one directory, on
//! the promise that the loader syncs everything once at the end. This checks
//! that promise can be kept: what was written without a record is readable
//! throughout, survives the sync, and is still there after the database is
//! reopened — which is the only thing a sync is for.

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        bulk_load,
        database::Database,
        rql::{executor::execute, lexer_s1::recognize_query_purpose},
    },
};
use rclite::Arc;

/// Runs a test body on a thread with a stack big enough for the engine.
///
/// `execute()` builds a very large async state machine, and in a debug build
/// one statement's frame overruns the 2 MiB a thread gets by default — the
/// server itself runs on a deliberately-set 32 MiB stack for exactly this
/// reason (`main.rs`, `STACK_SIZE`). A `#[tokio::test]` gets no such favour,
/// and `block_on` drives the future on the *calling* thread, so raising only
/// the runtime's `thread_stack_size` would not help: the thread that overflows
/// is the test's own. Both are raised here, so the test passes for anyone who
/// runs it rather than only with `RUST_MIN_STACK` set in the environment.
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

fn init_globals() {
    MAX_PERMITS_THREADS.get_or_init(|| 4);
    BATCH_SIZE.get_or_init(|| 1_000);
}

async fn run(db: &Arc<Database>, q: &str) -> String {
    String::from_utf8_lossy(&run_bytes(db, q).await).to_string()
}

/// The response as it came back. A lossy `String` mangles anything that is not
/// UTF-8, which a stored document is.
async fn run_bytes(db: &Arc<Database>, q: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(q).expect("recognized");
    let out = execute(purpose, db.clone()).await;
    assert_ne!(
        out.first(),
        Some(&3),
        "{} failed: {}",
        q,
        String::from_utf8_lossy(&out[1..])
    );
    out
}

#[test]
fn rows_loaded_without_a_log_record_survive_the_final_sync_and_a_reopen() {
    on_a_big_stack(rows_loaded_without_a_log_record_survive_the_final_sync_and_a_reopen_body());
}

async fn rows_loaded_without_a_log_record_survive_the_final_sync_and_a_reopen_body() {
    init_globals();
    let dir = format!("/tmp/rdb_bulk_load_mode_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");

    {
        let db = Arc::new(Database::new_with_backend(&dir, false).await);
        run(
            &db,
            "CREATE TABLE t (id INT NOT NULL, tag TEXT NULL, doc JSONB NULL)",
        )
        .await;

        let guard = bulk_load::begin(&dir);
        assert!(bulk_load::is_active(&dir));
        for i in 0..200 {
            run(
                &db,
                &format!(
                    r#"INSERT INTO t (id, tag, doc) VALUES ({}, 'row-{}', '{{"n":{}}}')"#,
                    i, i, i
                ),
            )
            .await;
        }
        // Readable while the mode is on: the bytes are in the file, only the
        // record and the flush are missing.
        let mid = run(&db, "SELECT tag FROM t WHERE id = 150").await;
        assert!(
            mid.contains("row-150"),
            "a row written under bulk load is not readable: {}",
            mid
        );

        drop(guard);
        assert!(!bulk_load::is_active(&dir));
        bulk_load::sync_all(&dir).expect("the final sync");
        db.sync_durable().expect("durable");
        db.transactions.flush().expect("flushed");
    }

    let db = Arc::new(Database::new_with_backend(&dir, false).await);
    let tags = run(&db, "SELECT tag FROM t").await;
    let count = tags.matches("row-").count();
    assert_eq!(count, 200, "expected 200 rows after reopening");
    // A JSONB column stores the document as bytes, so what has to survive the
    // reopen is exactly what the encoder produced.
    let doc = run_bytes(&db, "SELECT doc FROM t WHERE id = 199").await;
    let expected = rasterizeddb_core::core::jsonb::encode(&serde_json::json!({"n": 199}));
    assert!(
        doc.windows(expected.len())
            .any(|w| w == expected.as_slice()),
        "document lost across the reopen"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
