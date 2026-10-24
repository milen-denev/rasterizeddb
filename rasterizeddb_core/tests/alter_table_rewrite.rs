//! `ALTER TABLE … ADD COLUMN` rewrites every row, and the rewrite has to put
//! each value back under the column it came from.
//!
//! It used to match values to columns by `write_order`. That holds only while
//! the old numbering has no gaps — and `DROP COLUMN` leaves one. After
//! dropping `b` from `(a, b, c)` the surviving fields still read `a=0, c=2`,
//! while the rewritten table numbers them `a=0, c=1, d=2`, so `c`'s bytes were
//! written into `d` and every later column shifted with it. The statement
//! reported success and the row was corrupt, which is the failure mode this
//! file exists to keep out.

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        database::Database,
        db_type::DbType,
        row::row::{ColumnWritePayload, RowWrite},
        rql::{
            executor::execute,
            lexer_ct::{CreateColumnData, CreateTable},
            lexer_s1::recognize_query_purpose,
        },
    },
    memory_pool::{MEMORY_POOL, MemoryBlock},
};
use rclite::Arc;
use smallvec::smallvec;

/// Runs a test body on a thread with a stack big enough for the engine.
///
/// `execute` builds a very large async state machine, and in a debug build one
/// statement's frame overruns the 2 MiB a thread gets by default — the server
/// itself runs on a deliberately-set 32 MiB stack for exactly this reason
/// (`main.rs`, `STACK_SIZE`). A `#[tokio::test]` gets no such favour, and
/// `block_on` drives the future on the *calling* thread, so raising only the
/// runtime's `thread_stack_size` would not help: the thread that overflows is
/// the test's own. Both are raised here, so the test passes for anyone who runs
/// it rather than only with `RUST_MIN_STACK` set in the environment.
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

fn mb(bytes: &[u8]) -> MemoryBlock {
    let mut data = MEMORY_POOL.acquire(bytes.len());
    data.into_slice_mut().copy_from_slice(bytes);
    data
}

fn payload(bytes: &[u8], write_order: u32, ty: DbType) -> ColumnWritePayload {
    ColumnWritePayload {
        data: mb(bytes),
        write_order,
        column_type: ty.clone(),
        size: ty.get_size(),
    }
}

async fn run(db: &Arc<Database>, q: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(q).expect("recognized");
    execute(purpose, db.clone()).await
}

#[test]
fn a_column_added_after_a_drop_does_not_take_the_previous_column_s_bytes() {
    on_a_big_stack(a_column_added_after_a_drop_does_not_take_the_previous_column_s_bytes_body());
}

async fn a_column_added_after_a_drop_does_not_take_the_previous_column_s_bytes_body() {
    init_globals();
    let dir = format!("/tmp/rdb_alter_rewrite_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");

    let db = Arc::new(Database::new_with_backend(&dir, false).await);
    db.add_table(CreateTable {
        table_name: "wo".to_string(),
        columns: vec![
            CreateColumnData {
                name: "a".into(),
                data_type: DbType::I32,
            },
            CreateColumnData {
                name: "b".into(),
                data_type: DbType::I32,
            },
            CreateColumnData {
                name: "c".into(),
                data_type: DbType::STRING,
            },
        ],
        is_immutable: false,
        foreign_keys: Vec::new(),
        defaults: Vec::new(),
        primary_key: None,
        json_columns: Vec::new(),
        unique_columns: Vec::new(),
        unique_keys: Vec::new(),
        type_modifiers: Vec::new(),
        generated: Vec::new(),
        checks: Vec::new(),
        if_not_exists: false,
    })
    .await
    .expect("create table");

    let t = db.table_for_tests("wo").expect("table");
    t.insert_row(RowWrite {
        columns_writing_data: smallvec![
            payload(&10i32.to_le_bytes(), 0, DbType::I32),
            payload(&20i32.to_le_bytes(), 1, DbType::I32),
            payload(b"hello", 2, DbType::STRING),
        ],
    })
    .await
    .expect("insert");

    let dropped = run(&db, "ALTER TABLE wo DROP COLUMN b").await;
    assert!(
        !String::from_utf8_lossy(&dropped).contains("ERROR"),
        "drop failed: {}",
        String::from_utf8_lossy(&dropped)
    );

    // The gap in the numbering is now open. Adding a column rewrites the row.
    let added = run(&db, "ALTER TABLE wo ADD COLUMN d INT").await;
    let text = String::from_utf8_lossy(&added).to_string();
    assert!(
        !text.to_lowercase().contains("cannot rewrite"),
        "the rewrite mismatched a column: {}",
        text
    );

    let out = run(&db, "SELECT a, c FROM wo").await;
    let rendered = String::from_utf8_lossy(&out).to_string();
    assert!(
        rendered.contains("hello"),
        "'c' lost its value across the rewrite: {:?}",
        rendered
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_insert_after_a_drop_still_fills_the_dropped_column_s_slot() {
    on_a_big_stack(an_insert_after_a_drop_still_fills_the_dropped_column_s_slot_body());
}

async fn an_insert_after_a_drop_still_fills_the_dropped_column_s_slot_body() {
    // A dropped column keeps its slot in the row layout — reclaiming the bytes
    // would move every column after it. So a row written without that slot is
    // short, and every later column reads at the wrong offset. The insert path
    // used to skip tombstones when filling omitted columns, so every `INSERT`
    // after a `DROP COLUMN` produced a malformed row while reporting success:
    // `count(*)` counted them, the projection returned the wrong values or
    // dropped the row entirely.
    init_globals();
    let dir = format!("/tmp/rdb_alter_insert_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");

    let db = Arc::new(Database::new_with_backend(&dir, false).await);
    let created = run(
        &db,
        "CREATE TABLE t (id INT NOT NULL, gone TEXT NOT NULL, keep TEXT NOT NULL, n INT NOT NULL)",
    )
    .await;
    assert!(
        !String::from_utf8_lossy(&created)
            .to_lowercase()
            .contains("error"),
        "create failed: {}",
        String::from_utf8_lossy(&created)
    );

    run(
        &db,
        "INSERT INTO t (id, gone, keep, n) VALUES (1, 'gone', 'stays', 11)",
    )
    .await;
    run(&db, "ALTER TABLE t DROP COLUMN gone").await;
    run(&db, "INSERT INTO t (id, keep, n) VALUES (2, 'after', 22)").await;

    let out = run(&db, "SELECT id, keep, n FROM t").await;
    let rendered = String::from_utf8_lossy(&out).to_string();
    assert!(
        rendered.contains("stays") && rendered.contains("after"),
        "a row inserted after the drop lost its values: {:?}",
        rendered
    );

    let _ = std::fs::remove_dir_all(&dir);
}
