//! What `SHOW BACKUP STATUS` reports of the service's attempts, against an
//! in-process store that can be told to refuse writes, as a bucket whose
//! policy stopped allowing this server does.

use std::path::Path;
use std::time::Duration;

use rclite::Arc;

use super::activity::{Next, Operation};
use super::s3::{Credentials, Endpoint};
use super::{BackupConfig, BackupService, fake_s3};
use crate::core::database::Database;
use crate::core::rql::executor::execute;
use crate::core::rql::lexer_s1::recognize_query_purpose;

pub(super) fn init_globals() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(1024);
}

pub(super) fn config(s3: &fake_s3::FakeS3, snapshot_interval_secs: u64) -> BackupConfig {
    BackupConfig {
        bucket: "rdbtest".to_string(),
        prefix: format!("activity/{:016x}", fastrand::u64(..)),
        region: "us-east-1".to_string(),
        endpoint: Endpoint::parse(&s3.endpoint()).unwrap(),
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
        max_attempts: 1,
        snapshot_interval_secs,
        wal_flush_secs: 0,
        wal_bundle_bytes: 0,
        rollover_interval_secs: 0,
        retain_snapshots: 0,
        staging_max_bytes: 0,
        incremental_min_bytes: 0,
    }
}

pub(super) fn path_str(p: &Path) -> String {
    p.to_str().unwrap().to_string()
}

pub(super) async fn ok(db: &Arc<Database>, sql: &str) {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("unrecognised: {sql}"));
    let response = execute(purpose, db.clone()).await;
    assert_ne!(
        response.first(),
        Some(&3),
        "{sql} failed: {}",
        String::from_utf8_lossy(&response[1..])
    );
}

/// The row `metric` of `SHOW BACKUP STATUS`'s activity rows, as (value, detail).
fn status(service: &BackupService, metric: &str) -> (String, String) {
    crate::core::introspect::backup_activity_rows(&service.activity(), service.config())
        .into_iter()
        .find(|(m, _, _)| m == metric)
        .map(|(_, value, detail)| (value, detail))
        .unwrap_or_else(|| panic!("no {metric} row"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn attempts_outcomes_and_the_last_error_are_reported() {
    init_globals();
    let s3 = fake_s3::start();
    let base = tempfile::tempdir().unwrap();
    let live = base.path().join("live");
    std::fs::create_dir_all(&live).unwrap();
    let live_str = path_str(&live);
    let db = Arc::new(Database::new(&live_str).await);
    ok(&db, "CREATE TABLE t (id INT, note STRING)").await;
    ok(&db, "INSERT INTO t (id, note) VALUES (1, 'a')").await;

    let service = BackupService::connect(config(&s3, 0), &live_str).await.expect("connect");

    // Nothing tried yet.
    assert_eq!(status(&service, "last_snapshot_attempt_at").0, "none");
    assert_eq!(status(&service, "last_wal_flush_outcome").0, "none");
    assert_eq!(status(&service, "last_error").0, "none");
    let (next, why) = status(&service, "next_snapshot_at");
    assert_eq!(next, "not scheduled");
    assert!(why.contains("snapshot_interval_secs is 0"), "{why}");

    // A snapshot that works: its id is in the outcome, and the log upload it
    // made on its way is recorded too.
    let report = service.snapshot_now().await.expect("snapshot");
    let (outcome, summary) = status(&service, "last_snapshot_outcome");
    assert_eq!(outcome, "ok");
    assert!(summary.contains(&report.id), "{summary}");
    assert!(status(&service, "last_snapshot_attempt_at").1.starts_with("finished "));
    assert_eq!(status(&service, "last_wal_flush_outcome").0, "ok");
    assert_eq!(status(&service, "last_error").0, "none");

    // The bucket stops accepting writes. The catalogue still reads as before;
    // the attempts say what is happening.
    s3.store.lock().refuse_writes = true;
    ok(&db, "INSERT INTO t (id, note) VALUES (2, 'b')").await;
    let flushed = service.archive_wal_now().await;
    assert!(flushed.is_err(), "an upload to a refusing bucket succeeded");
    let (outcome, message) = status(&service, "last_wal_flush_outcome");
    assert_eq!(outcome, "failed");
    assert!(message.contains("403") || message.contains("AccessDenied"), "{message}");
    let (error, operation) = status(&service, "last_error");
    assert_eq!(error, message);
    assert_eq!(operation, "wal_flush");

    let snapshot = service.snapshot_now().await;
    assert!(snapshot.is_err(), "a snapshot to a refusing bucket succeeded");
    assert_eq!(status(&service, "last_snapshot_outcome").0, "failed");
    assert_eq!(status(&service, "last_error").1, "snapshot");
    assert_eq!(status(&service, "last_error_at").1, "snapshot");
    assert_ne!(status(&service, "last_error_at").0, "none");

    // Writes work again: the flush turns green, and the last error stays
    // readable until another one replaces it.
    s3.store.lock().refuse_writes = false;
    service.archive_wal_now().await.expect("archive after recovery");
    assert_eq!(status(&service, "last_wal_flush_outcome").0, "ok");
    assert_eq!(status(&service, "last_error").1, "snapshot");
    assert_eq!(service.activity().rollover, None);

    drop(db);
    crate::core::wal::remove_for(&live_str);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_next_scheduled_snapshot_is_reported() {
    init_globals();
    let s3 = fake_s3::start();
    let base = tempfile::tempdir().unwrap();
    let live = base.path().join("live");
    std::fs::create_dir_all(&live).unwrap();
    let live_str = path_str(&live);
    let db = Arc::new(Database::new(&live_str).await);
    ok(&db, "CREATE TABLE t (id INT)").await;

    let service = BackupService::connect(config(&s3, 3600), &live_str).await.expect("connect");
    let before = super::timeutil::now_unix();
    service.spawn_background();

    // No base snapshot: the task takes one at once, then waits an interval.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let next = loop {
        let activity = service.activity();
        if let Next::At(at) = activity.next_snapshot
            && activity.snapshot.as_ref().is_some_and(|a| a.outcome.is_some())
        {
            break at;
        }
        assert!(std::time::Instant::now() < deadline, "no next snapshot: {activity:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(
        service.activity().snapshot.and_then(|a| a.outcome).map(|o| o.is_ok()),
        Some(true)
    );
    assert!(
        (before + 3600..=super::timeutil::now_unix() + 3601).contains(&next),
        "next snapshot at {next}, started at {before}"
    );
    assert_eq!(
        status(&service, "next_snapshot_at").0,
        super::timeutil::format_rfc3339(next)
    );
    // No log interval: never scheduled, and the row says why.
    assert_eq!(service.activity().next(Operation::WalFlush), Next::NotScheduled);
    assert!(status(&service, "next_wal_flush_at").1.contains("wal_flush_secs is 0"));

    drop(db);
    crate::core::wal::remove_for(&live_str);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_base_snapshot_reports_its_retry() {
    init_globals();
    let s3 = fake_s3::start();
    let base = tempfile::tempdir().unwrap();
    let live = base.path().join("live");
    std::fs::create_dir_all(&live).unwrap();
    let live_str = path_str(&live);
    let db = Arc::new(Database::new(&live_str).await);
    ok(&db, "CREATE TABLE t (id INT)").await;

    let service = BackupService::connect(config(&s3, 3600), &live_str).await.expect("connect");
    s3.store.lock().refuse_writes = true;
    service.spawn_background();

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let activity = service.activity();
        if matches!(activity.next_snapshot, Next::At(_))
            && activity.snapshot.as_ref().is_some_and(|a| a.outcome.is_some())
        {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "no retry scheduled: {activity:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(status(&service, "last_snapshot_outcome").0, "failed");
    let (_, why) = status(&service, "next_snapshot_at");
    assert!(why.contains("retrying the base snapshot"), "{why}");

    drop(db);
    crate::core::wal::remove_for(&live_str);
}
