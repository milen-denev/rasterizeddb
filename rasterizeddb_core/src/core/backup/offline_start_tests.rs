//! `--s3-start-offline`: a server whose bucket does not answer at boot
//! starts anyway, holds the log locally, writes nothing to the bucket, and
//! picks the catalogue up once it answers.
//!
//! The hazard is the placeholder it runs on meanwhile: an empty catalogue
//! that, published, would replace every snapshot and bundle the real one
//! lists. Each test seeds a real catalogue first and checks it is untouched.

use std::time::Duration;

use rclite::Arc;

use super::activity_tests::{config, init_globals, ok, path_str};
use super::manifest::{MANIFEST_KEY, Manifest};
use super::{BackupConfig, BackupService, CatalogueState, fake_s3, read_or_create_instance_id};
use crate::core::database::Database;

/// A catalogue already in the bucket for `instance_id`, with its sequence
/// counter moved on so a bundle archived on top of it is recognisable.
fn seed_catalogue(s3: &fake_s3::FakeS3, cfg: &BackupConfig, instance_id: &str) -> Vec<u8> {
    let mut manifest = Manifest::new(instance_id.to_string());
    manifest.segment_sequence_high = 7;
    let bytes = manifest.encode();
    s3.store
        .lock()
        .objects
        .insert(format!("{}/{}", cfg.bucket, cfg.key(MANIFEST_KEY)), bytes.clone());
    bytes
}

fn published(s3: &fake_s3::FakeS3, cfg: &BackupConfig) -> Option<Vec<u8>> {
    s3.store
        .lock()
        .objects
        .get(&format!("{}/{}", cfg.bucket, cfg.key(MANIFEST_KEY)))
        .cloned()
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !done() {
        assert!(std::time::Instant::now() < deadline, "{what} never happened");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_without_the_bucket_holds_the_log_and_catches_up() {
    init_globals();
    let s3 = fake_s3::start();
    let base = tempfile::tempdir().unwrap();
    let live = base.path().join("live");
    std::fs::create_dir_all(&live).unwrap();
    let live_str = path_str(&live);
    let db = Arc::new(Database::new(&live_str).await);
    ok(&db, "CREATE TABLE t (id INT, note STRING)").await;

    let cfg = config(&s3, 0);
    let instance = read_or_create_instance_id(&live_str).unwrap();
    let seeded = seed_catalogue(&s3, &cfg, &instance);
    s3.store.lock().unavailable = true;

    // Without the flag the start is refused, as before.
    assert!(
        BackupService::connect(cfg.clone(), &live_str).await.is_err(),
        "a strict start went ahead without its bucket"
    );

    let service = BackupService::connect_waiting(cfg.clone(), &live_str)
        .await
        .expect("an offline start");
    assert!(
        matches!(service.catalogue_state(), CatalogueState::Unreachable { attempts: 1, .. }),
        "{:?}",
        service.catalogue_state()
    );
    assert!(
        service.activity().catalogue.and_then(|a| a.outcome).is_some_and(|o| o.is_err()),
        "the failed read is not in the status"
    );

    // Writes go on, and their log is held locally. The bucket answers again
    // but the catalogue has not been re-read, so each write path has only its
    // own gate between the placeholder and the bucket.
    ok(&db, "INSERT INTO t (id, note) VALUES (1, 'held')").await;
    s3.store.lock().unavailable = false;
    let archived = service.archive_wal_now().await;
    let refusal = archived.expect_err("an archive ran without a catalogue").to_string();
    assert!(refusal.contains("has not been read"), "{refusal}");
    assert!(service.staged_bytes() > 0, "the log was not held in staging");
    assert!(service.snapshot_now().await.is_err(), "a snapshot ran without a catalogue");
    assert!(service.prune_keeping(1).await.is_err(), "a prune ran without a catalogue");
    assert!(service.rollover_now().await.is_err(), "a rollover ran without a catalogue");
    assert!(service.publish_manifest().await.is_err(), "the placeholder could be published");
    assert_eq!(published(&s3, &cfg), Some(seeded.clone()), "the real catalogue was overwritten");
    let written: Vec<String> = s3
        .keys(&cfg.bucket, &cfg.normalized_prefix())
        .into_iter()
        .filter(|k| !k.ends_with(MANIFEST_KEY))
        .collect();
    assert!(written.is_empty(), "objects were written without a catalogue: {written:?}");

    // The retry reads the real catalogue and the schedules take over.
    service.spawn_background();
    until("the catalogue load", || service.catalogue_state() == CatalogueState::Loaded).await;
    assert!(service.activity().catalogue.and_then(|a| a.outcome).is_some_and(|o| o.is_ok()));

    // The held log goes up on top of the real catalogue: sequence 7, not 0.
    service.archive_wal_now().await.expect("archive once loaded");
    assert_eq!(service.staged_bytes(), 0, "the held log was not uploaded");
    let keys = s3.keys(&cfg.bucket, &cfg.normalized_prefix());
    assert!(
        keys.iter().any(|k| k.contains("wal/0000000000000007-")),
        "the held log was not archived on top of the real catalogue: {keys:?}"
    );
    let now = Manifest::decode(&published(&s3, &cfg).expect("a catalogue")).unwrap();
    assert_eq!(now.instance_id, instance);
    assert!(now.segments.iter().any(|s| s.sequence == 7), "{:?}", now.segments);

    drop(db);
    crate::core::wal::remove_for(&live_str);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bucket_holding_another_database_still_refuses_the_start() {
    init_globals();
    let s3 = fake_s3::start();
    let base = tempfile::tempdir().unwrap();
    let live = base.path().join("live");
    std::fs::create_dir_all(&live).unwrap();
    let live_str = path_str(&live);
    let db = Arc::new(Database::new(&live_str).await);

    let cfg = config(&s3, 0);
    seed_catalogue(&s3, &cfg, "someone-else");
    let refused = BackupService::connect_waiting(cfg, &live_str).await;
    let message = refused.err().expect("a start on another database's prefix went ahead").to_string();
    assert!(message.contains("already holds backups for instance someone-else"), "{message}");

    drop(db);
    crate::core::wal::remove_for(&live_str);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_database_found_later_stops_backups_and_writes_nothing() {
    init_globals();
    let s3 = fake_s3::start();
    let base = tempfile::tempdir().unwrap();
    let live = base.path().join("live");
    std::fs::create_dir_all(&live).unwrap();
    let live_str = path_str(&live);
    let db = Arc::new(Database::new(&live_str).await);
    ok(&db, "CREATE TABLE t (id INT)").await;

    let cfg = config(&s3, 0);
    let theirs = seed_catalogue(&s3, &cfg, "someone-else");
    s3.store.lock().unavailable = true;
    let service = BackupService::connect_waiting(cfg.clone(), &live_str).await.expect("offline start");
    service.spawn_background();
    s3.store.lock().unavailable = false;

    until("the refusal", || matches!(service.catalogue_state(), CatalogueState::Refused { .. })).await;
    ok(&db, "INSERT INTO t (id) VALUES (1)").await;
    let refusal = service.archive_wal_now().await.expect_err("archived into another database's prefix");
    assert!(refusal.to_string().contains("backups are stopped"), "{refusal}");
    assert_eq!(published(&s3, &cfg), Some(theirs), "another database's catalogue was overwritten");
    let wal: Vec<String> = s3
        .keys(&cfg.bucket, &cfg.normalized_prefix())
        .into_iter()
        .filter(|k| k.contains("/wal/"))
        .collect();
    assert!(wal.is_empty(), "log bundles were written into another database's prefix: {wal:?}");

    drop(db);
    crate::core::wal::remove_for(&live_str);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_ceiling_holds_while_the_bucket_is_unreachable() {
    init_globals();
    let s3 = fake_s3::start();
    let base = tempfile::tempdir().unwrap();
    let live = base.path().join("live");
    std::fs::create_dir_all(&live).unwrap();
    let live_str = path_str(&live);
    let db = Arc::new(Database::new(&live_str).await);
    ok(&db, "CREATE TABLE t (id INT)").await;

    let mut cfg = config(&s3, 0);
    cfg.staging_max_bytes = 1;
    s3.store.lock().unavailable = true;
    let service = BackupService::connect_waiting(cfg, &live_str).await.expect("offline start");
    service.spawn_background();

    ok(&db, "INSERT INTO t (id) VALUES (1)").await;
    service.checkpoint_all_logs().await.expect("cut");
    assert!(service.staged_bytes() >= 1);
    until("suspension at the ceiling", || service.archiving_is_suspended()).await;

    drop(db);
    crate::core::wal::remove_for(&live_str);
}
