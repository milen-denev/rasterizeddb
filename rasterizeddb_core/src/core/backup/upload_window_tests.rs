//! What a `DROP`, a `RENAME TABLE` and a `MOVE` do to a snapshot whose upload
//! is in progress.
//!
//! The deletion gate is held only while a snapshot fixes where its copy
//! begins, so these statements can run between the snapshot listing its files
//! and opening them. A drop deleted a listed file and the snapshot failed on
//! it. A rename or a move did the same, and could not simply be skipped: their
//! copies write no log record, so a file they took away is in no log a
//! restore could rebuild it from. A drop's file is left out; a rename or a
//! move waits for the upload.
//!
//! Each test parks the snapshot between its listing and its first open (see
//! [`super::upload_hooks`]), runs the statement there, and restores the
//! snapshot through the in-process store.

use std::path::Path;
use std::time::Duration;

use rclite::Arc;

use super::manifest::RecoveryTarget;
use super::s3::{Credentials, Endpoint};
use super::{BackupConfig, BackupService, SnapshotReport, upload_hooks};
use crate::core::database::Database;
use crate::core::row::row::vec_into_rows;
use crate::core::rql::executor::execute;
use crate::core::rql::lexer_s1::recognize_query_purpose;

fn init_globals() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(1024);
}

/// A configuration on the in-process store, under a prefix of its own.
fn config(test: &str, incremental_min_bytes: u64) -> BackupConfig {
    BackupConfig {
        bucket: "rdbtest".to_string(),
        prefix: format!("upload-window/{:016x}/{test}", fastrand::u64(..)),
        region: "us-east-1".to_string(),
        endpoint: Endpoint::parse(&super::fake_s3::shared().endpoint()).unwrap(),
        credentials: Credentials {
            access_key: "fake".to_string(),
            secret_key: "fake".to_string(),
            session_token: None,
        },
        path_style: true,
        snapshot_level: 3,
        segment_level: 3,
        compression_threads: 1,
        long_range: false,
        part_size: 5 * 1024 * 1024,
        upload_concurrency: 2,
        max_attempts: 3,
        snapshot_interval_secs: 0,
        wal_flush_secs: 0,
        wal_bundle_bytes: 0,
        rollover_interval_secs: 0,
        retain_snapshots: 0,
        staging_max_bytes: 0,
        incremental_min_bytes,
    }
}

fn path_str(p: &Path) -> String {
    p.to_str().unwrap().to_string()
}

/// The statement's response; an error is code 3.
async fn run(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("unrecognised: {sql}"));
    execute(purpose, db.clone()).await
}

async fn ok(db: &Arc<Database>, sql: &str) {
    let response = run(db, sql).await;
    assert_ne!(
        response.first(),
        Some(&3),
        "{sql} failed: {}",
        String::from_utf8_lossy(&response[1..])
    );
}

async fn count(db: &Arc<Database>, sql: &str) -> usize {
    let response = run(db, sql).await;
    assert_eq!(
        response.first(),
        Some(&2),
        "{sql} returned no rows: {}",
        String::from_utf8_lossy(response.get(1..).unwrap_or_default())
    );
    vec_into_rows(&response[1..]).expect("rows decode").len()
}

async fn table_with_rows(db: &Arc<Database>, table: &str, rows: usize) {
    ok(db, &format!("CREATE TABLE {table} (id INT, note STRING)")).await;
    for i in 0..rows {
        ok(db, &format!("INSERT INTO {table} (id, note) VALUES ({i}, '{table}_{i}')")).await;
    }
}

/// The table's files in `dir` that hold any byte.
fn nonempty_files_of(dir: &Path, table: &str) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut found: Vec<String> = entries
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_str()?.to_string();
            let stem = name.split(['_', '.']).next()?.to_string();
            (stem == table && e.metadata().ok()?.len() > 0).then_some(name)
        })
        .collect();
    found.sort();
    found
}

/// Starts a snapshot of `live` and returns once it is parked between listing
/// its files and copying them, with what releases it.
async fn parked_snapshot(
    service: &std::sync::Arc<BackupService>,
    live: &str,
) -> (
    tokio::task::JoinHandle<std::io::Result<SnapshotReport>>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (reached, release) = upload_hooks::park_next(live);
    let snapshot = tokio::spawn({
        let service = service.clone();
        async move { service.snapshot_now().await }
    });
    tokio::time::timeout(Duration::from_secs(60), reached)
        .await
        .expect("the snapshot never reached its copy")
        .expect("the snapshot dropped its park");
    (snapshot, release)
}

async fn finished(
    snapshot: tokio::task::JoinHandle<std::io::Result<SnapshotReport>>,
) -> SnapshotReport {
    tokio::time::timeout(Duration::from_secs(60), snapshot)
        .await
        .expect("the snapshot never finished")
        .unwrap()
        .expect("the snapshot failed")
}

/// Returns once a rename or a move of `table` waits for its permit, and
/// fails if `work` finished first: it did not wait for the upload.
async fn relocation_parked<T>(table: &str, work: &tokio::task::JoinHandle<T>) {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if crate::core::database::PARKED_RELOCATIONS
                .lock()
                .iter()
                .any(|t| t == table)
            {
                return;
            }
            assert!(
                !work.is_finished(),
                "{table} was renamed or moved while a snapshot was uploading"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the rename or move never reached the snapshot");
}

/// Restores `report`'s snapshot exactly into `target` and opens it.
async fn restored(config: &BackupConfig, report: &SnapshotReport, target: &Path) -> Arc<Database> {
    super::restore_from(config, target, RecoveryTarget::Snapshot(report.id.clone()))
        .await
        .expect("restore");
    Arc::new(Database::new(&path_str(target)).await)
}

/// A `DROP TABLE` between the listing and the copy: the snapshot completes
/// without the dropped files, and its restore holds every other table whole
/// and no trace of the dropped one.
async fn a_drop_during_the_upload(incremental_min_bytes: u64) {
    init_globals();
    let base = tempfile::tempdir().unwrap();
    let live = base.path().join("live");
    std::fs::create_dir_all(&live).unwrap();
    let live_str = path_str(&live);
    let db = Arc::new(Database::new(&live_str).await);
    table_with_rows(&db, "kept", 12).await;
    table_with_rows(&db, "doomed", 9).await;

    let config = config("drop", incremental_min_bytes);
    let service = BackupService::connect(config.clone(), &live_str).await.expect("connect");
    let (snapshot, release) = parked_snapshot(&service, &live_str).await;

    // A drop waits only while a snapshot fixes its start, never for its copy.
    tokio::time::timeout(Duration::from_secs(20), ok(&db, "DROP TABLE doomed"))
        .await
        .expect("a drop waited for a snapshot's upload");
    assert_eq!(nonempty_files_of(&live, "doomed"), Vec::<String>::new());
    assert!(!live.join("doomed.db").exists(), "the drop left its files");
    let _ = release.send(());
    let report = finished(snapshot).await;

    let target = base.path().join("restored");
    let back = restored(&config, &report, &target).await;
    assert_eq!(count(&back, "SELECT * FROM kept").await, 12, "the restore lost a kept row");
    assert!(!back.tables.contains_key("doomed"), "the restore brought the dropped table back");
    assert_eq!(
        nonempty_files_of(&target, "doomed"),
        Vec::<String>::new(),
        "the restore holds bytes of the dropped table"
    );
    // Nothing of it in the way of the name either.
    ok(&back, "CREATE TABLE doomed (id INT, note STRING)").await;
    assert_eq!(count(&back, "SELECT * FROM doomed").await, 0);

    drop(back);
    drop(db);
    crate::core::wal::remove_for(&path_str(&target));
    crate::core::wal::remove_for(&live_str);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drop_during_the_upload_is_left_out_of_the_archive() {
    a_drop_during_the_upload(0).await;
}

/// The same with every non-empty file stored as an object of its own, which
/// reads each one's length first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drop_during_the_upload_is_left_out_of_the_objects() {
    a_drop_during_the_upload(1).await;
}

/// A `RENAME TABLE` in the same window waits for the upload, then succeeds,
/// and the snapshot holds the table under its old name with every row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rename_during_the_upload_waits_for_it() {
    init_globals();
    let base = tempfile::tempdir().unwrap();
    let live = base.path().join("live");
    std::fs::create_dir_all(&live).unwrap();
    let live_str = path_str(&live);
    let db = Arc::new(Database::new(&live_str).await);
    table_with_rows(&db, "wren", 11).await;

    let config = config("rename", 0);
    let service = BackupService::connect(config.clone(), &live_str).await.expect("connect");
    let (snapshot, release) = parked_snapshot(&service, &live_str).await;

    let renaming = tokio::spawn({
        let db = db.clone();
        async move { run(&db, "ALTER TABLE wren RENAME TO wren_renamed").await }
    });
    relocation_parked("wren", &renaming).await;
    assert!(live.join("wren.db").exists(), "the rename deleted the files the snapshot listed");
    assert!(!live.join("wren_renamed.db").exists(), "the rename copied during the upload");

    let _ = release.send(());
    let report = finished(snapshot).await;
    let response = tokio::time::timeout(Duration::from_secs(60), renaming)
        .await
        .expect("the rename never finished once the upload had")
        .unwrap();
    assert_ne!(response.first(), Some(&3), "{}", String::from_utf8_lossy(&response[1..]));
    assert_eq!(count(&db, "SELECT * FROM wren_renamed").await, 11);

    let target = base.path().join("restored");
    let back = restored(&config, &report, &target).await;
    assert_eq!(count(&back, "SELECT * FROM wren").await, 11, "the snapshot lost the table");
    assert!(!back.tables.contains_key("wren_renamed"));

    drop(back);
    drop(db);
    crate::core::wal::remove_for(&path_str(&target));
    crate::core::wal::remove_for(&live_str);
}

/// A `MOVE` in the same window waits for the upload, then succeeds, and the
/// snapshot holds the table where it was with every row.
#[cfg(feature = "storage_pools")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_move_during_the_upload_waits_for_it() {
    use crate::core::storage::LocationRole;
    init_globals();
    let base = tempfile::tempdir().unwrap();
    let live = base.path().join("live");
    let disk = base.path().join("disk");
    std::fs::create_dir_all(&live).unwrap();
    std::fs::create_dir_all(&disk).unwrap();
    let live_str = path_str(&live);
    let db = Arc::new(Database::new(&live_str).await);
    db.storage
        .add_location(&path_str(&disk), LocationRole::Primary, 1, true)
        .unwrap();
    db.storage.pin("mover", &live_str).unwrap();
    table_with_rows(&db, "mover", 10).await;
    assert!(live.join("mover.db").exists(), "the table is not on the root");

    let config = config("move", 0);
    let service = BackupService::connect(config.clone(), &live_str).await.expect("connect");
    let (snapshot, release) = parked_snapshot(&service, &live_str).await;

    let moving = tokio::spawn({
        let db = db.clone();
        let disk = path_str(&disk);
        async move { db.move_table_to("mover", &disk).await }
    });
    relocation_parked("mover", &moving).await;
    assert!(live.join("mover.db").exists(), "the move deleted the files the snapshot listed");
    assert!(!disk.join("mover.db").exists(), "the move copied during the upload");

    let _ = release.send(());
    let report = finished(snapshot).await;
    tokio::time::timeout(Duration::from_secs(60), moving)
        .await
        .expect("the move never finished once the upload had")
        .unwrap()
        .expect("the move failed");
    assert!(disk.join("mover.db").exists() && !live.join("mover.db").exists());
    assert_eq!(count(&db, "SELECT * FROM mover").await, 10);

    let target = base.path().join("restored");
    let back = restored(&config, &report, &target).await;
    assert_eq!(count(&back, "SELECT * FROM mover").await, 10, "the snapshot lost the table");

    drop(back);
    drop(db);
    crate::core::wal::remove_for(&path_str(&target));
    crate::core::wal::remove_for(&live_str);
    crate::core::wal::remove_for(&path_str(&disk));
}
