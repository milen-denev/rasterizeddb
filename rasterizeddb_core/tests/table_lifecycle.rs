//! A dropped or rebuilt table releases its files.
//!
//! `ALTER TABLE … ADD COLUMN` on a populated table rebuilds it: the old
//! `Table` is dropped and a new one opened on fresh files. The old one's
//! maintenance task used to run forever, holding every storage provider it
//! was given — so each rebuild leaked a full set of descriptors, and a JSON
//! column whose documents kept bringing new keys walked the process to the
//! descriptor limit. The count below is the proof that it lets go.

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
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

async fn run(db: &Arc<Database>, q: &str) {
    let purpose = recognize_query_purpose(q).expect("recognized");
    let out = execute(purpose, db.clone()).await;
    assert_ne!(out.first(), Some(&3), "{} failed: {}", q, String::from_utf8_lossy(&out[1..]));
}

fn open_descriptors() -> usize {
    std::fs::read_dir("/proc/self/fd").map(|d| d.count()).unwrap_or(0)
}

#[test]
fn a_rebuilt_table_releases_the_previous_generation_s_files() {
    on_a_big_stack(a_rebuilt_table_releases_the_previous_generation_s_files_body());
}

async fn a_rebuilt_table_releases_the_previous_generation_s_files_body() {
    if !std::path::Path::new("/proc/self/fd").exists() {
        return; // the count needs procfs
    }
    init_globals();
    let dir = format!("/tmp/rdb_table_lifecycle_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");

    let db = Arc::new(Database::new_with_backend(&dir, false).await);
    run(&db, "CREATE TABLE t (id INT NOT NULL, c0 INT NULL)").await;
    run(&db, "INSERT INTO t (id, c0) VALUES (1, 1)").await;

    // One rebuild to settle any one-off opens, then measure from there.
    run(&db, "ALTER TABLE t ADD COLUMN warm INT NULL").await;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let before = open_descriptors();

    const REBUILDS: usize = 20;
    for i in 0..REBUILDS {
        run(&db, &format!("ALTER TABLE t ADD COLUMN c{} INT NULL", i + 1)).await;
    }
    // The release is gated on the slowest watcher, the filter-maintenance
    // task, which looks at the flag once per checkpoint interval (10 s by
    // default). Poll rather than sleep for the worst case.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(40);
    let mut after = open_descriptors();
    while after > before + 20 && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        after = open_descriptors();
    }

    // A generation holds several handles per file across five files; twenty
    // leaked generations would be well over a hundred descriptors. A handful
    // of difference is the live table and timing, not a leak.
    assert!(
        after <= before + 20,
        "descriptors grew from {} to {} over {} rebuilds: dropped tables are not releasing \
         their files",
        before,
        after,
        REBUILDS
    );

    let _ = std::fs::remove_dir_all(&dir);
}
