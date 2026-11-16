//! The staging ceiling when uploads fail, which is the case it exists for.
//!
//! Segments pile up in staging while the bucket refuses them; past
//! `staging_max_bytes` the service stops staging so a full disk cannot stop
//! the database. The ceiling used to be checked only after a flush that
//! succeeded, and by the bundle loop only while staging was below the bundle
//! size, so a bucket that kept refusing filled the disk instead.
//!
//! And a flush that fails part-way has already deleted the staged files of
//! the bundles it did upload: those bundles have to reach the published
//! catalogue, which is all a restore reads.

use std::time::Duration;

use rclite::Arc;

use super::activity_tests::{config, init_globals, ok, path_str};
use super::manifest::{MANIFEST_KEY, Manifest};
use super::{BackupService, fake_s3};
use crate::core::database::Database;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bucket_that_keeps_refusing_suspends_archiving_at_the_ceiling() {
    init_globals();
    let s3 = fake_s3::start();
    let base = tempfile::tempdir().unwrap();
    let live = base.path().join("live");
    std::fs::create_dir_all(&live).unwrap();
    let live_str = path_str(&live);
    let db = Arc::new(Database::new(&live_str).await);
    ok(&db, "CREATE TABLE t (id INT, note STRING)").await;

    let mut cfg = config(&s3, 0);
    cfg.staging_max_bytes = 1;
    let service = BackupService::connect(cfg, &live_str).await.expect("connect");

    s3.store.lock().refuse_writes = true;
    ok(&db, "INSERT INTO t (id, note) VALUES (1, 'a')").await;
    assert!(service.archive_wal_now().await.is_err(), "an upload to a refusing bucket succeeded");
    assert!(service.staged_bytes() >= 1, "the failed flush left nothing staged");
    assert!(
        service.archiving_is_suspended(),
        "staging is past its ceiling ({} bytes) and archiving was not suspended",
        service.staged_bytes()
    );

    // The bucket comes back: the next flush drains staging and archiving
    // resumes.
    s3.store.lock().refuse_writes = false;
    service.archive_wal_now().await.expect("archive after recovery");
    assert_eq!(service.staged_bytes(), 0);
    assert!(!service.archiving_is_suspended(), "drained, and still suspended");

    drop(db);
    crate::core::wal::remove_for(&live_str);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_ceiling_is_watched_without_a_bundle_trigger() {
    init_globals();
    let s3 = fake_s3::start();
    let base = tempfile::tempdir().unwrap();
    let live = base.path().join("live");
    std::fs::create_dir_all(&live).unwrap();
    let live_str = path_str(&live);
    let db = Arc::new(Database::new(&live_str).await);
    ok(&db, "CREATE TABLE t (id INT, note STRING)").await;

    // No interval and no bundle trigger: nothing flushes on its own. A base
    // snapshot first, so the snapshot task has nothing to do either and the
    // only thing that can suspend archiving is the ceiling's own check.
    let mut cfg = config(&s3, 0);
    cfg.staging_max_bytes = 1;
    let service = BackupService::connect(cfg, &live_str).await.expect("connect");
    service.snapshot_now().await.expect("base snapshot");
    service.spawn_background();

    ok(&db, "INSERT INTO t (id, note) VALUES (1, 'a')").await;
    service.checkpoint_all_logs().await.expect("cut");
    assert!(service.staged_bytes() >= 1, "the cut staged nothing");

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !service.archiving_is_suspended() {
        assert!(
            std::time::Instant::now() < deadline,
            "{} bytes staged past a ceiling of 1 and archiving was never suspended",
            service.staged_bytes()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    drop(db);
    crate::core::wal::remove_for(&live_str);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flush_that_fails_part_way_publishes_what_it_uploaded() {
    init_globals();
    let s3 = fake_s3::start();
    let base = tempfile::tempdir().unwrap();
    let live = base.path().join("live");
    std::fs::create_dir_all(&live).unwrap();
    let live_str = path_str(&live);
    let db = Arc::new(Database::new(&live_str).await);
    ok(&db, "CREATE TABLE t (id INT, note STRING)").await;

    // One segment per bundle, so two staged segments are two uploads.
    let mut cfg = config(&s3, 0);
    cfg.wal_bundle_bytes = 1;
    let manifest_key = format!("{}/{}", cfg.bucket, cfg.key(MANIFEST_KEY));
    let service = BackupService::connect(cfg, &live_str).await.expect("connect");

    for id in 1..=2 {
        ok(&db, &format!("INSERT INTO t (id, note) VALUES ({id}, 'n')")).await;
        service.checkpoint_all_logs().await.expect("cut");
    }
    assert!(service.staged_segments().unwrap().len() >= 2, "two cuts staged fewer than two segments");

    // The first bundle (sequence 0) goes through; the second is refused.
    s3.store.lock().refuse_writes_containing = Some("wal/0000000000000001-".to_string());
    let flushed = service.archive_wal_now().await;
    assert!(flushed.is_err(), "the refused bundle was reported as uploaded");

    // Its staged files are gone, so only the catalogue can still lead a
    // restore to it.
    let published = s3
        .store
        .lock()
        .objects
        .get(&manifest_key)
        .cloned()
        .expect("no catalogue was published");
    let published = Manifest::decode(&published).expect("the catalogue decodes");
    assert!(
        published.segments.iter().any(|s| s.sequence == 0),
        "bundle 0 was uploaded and its staged files deleted, but the published catalogue does not list it: {:?}",
        published.segments.iter().map(|s| s.sequence).collect::<Vec<_>>()
    );

    drop(db);
    crate::core::wal::remove_for(&live_str);
}
