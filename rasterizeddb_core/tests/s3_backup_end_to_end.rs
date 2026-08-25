//! Backup and restore a real database, through a real object store.
//!
//! Skipped unless `RDB_S3_TEST_ENDPOINT` is set. See `s3_backup_minio.rs` for
//! how to bring one up.
//!
//! These are the tests that justify the feature: a database is written to while
//! a snapshot is taken, the snapshot and the archived log are restored into a
//! fresh directory, and the restored directory is *opened as a database* and
//! queried. Anything short of that — checking bytes, checking file counts —
//! would not catch a backup that produces files the engine cannot open.

#![cfg(feature = "s3_backup")]

use std::path::Path;

use rasterizeddb_core::core::backup::manifest::RecoveryTarget;
use rasterizeddb_core::core::backup::s3::{Credentials, Endpoint};
use rasterizeddb_core::core::backup::{BackupConfig, BackupService};
use rasterizeddb_core::core::database::Database;
use rasterizeddb_core::core::row::row::vec_into_rows;
use rasterizeddb_core::core::rql::executor::execute;
use rasterizeddb_core::core::rql::lexer_s1::recognize_query_purpose;

use rclite::Arc;

/// Identifies this run of the suite.
fn run_id() -> &'static str {
    static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ID.get_or_init(|| format!("{:016x}", fastrand::u64(..)))
}

/// A raw client against the same bucket, for tests that need to reach past the
/// service and disturb what it wrote.
fn client_for(config: &BackupConfig) -> rasterizeddb_core::core::backup::s3::S3Client {
    use rasterizeddb_core::core::backup::s3::{S3Client, S3Config};
    S3Client::new(S3Config {
        endpoint: config.endpoint.clone(),
        region: config.region.clone(),
        bucket: config.bucket.clone(),
        credentials: config.credentials.clone(),
        path_style: config.path_style,
        max_attempts: config.max_attempts,
    })
    .expect("client")
}

/// Seeds the process-wide knobs that `main` normally sets.
///
/// The query processor reads its worker count from a `OnceLock` that only the
/// binary populates, so an integration test has to stand in for it.
fn init_engine_globals() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rasterizeddb_core::MAX_PERMITS_THREADS.set(4);
        let _ = rasterizeddb_core::BATCH_SIZE.set(10_000);
    });
}

/// A configuration pointed at the test bucket under a prefix of its own, so
/// tests do not see each other's catalogues.
fn config(prefix: &str) -> Option<BackupConfig> {
    let endpoint = std::env::var("RDB_S3_TEST_ENDPOINT").ok()?;
    Some(BackupConfig {
        bucket: std::env::var("RDB_S3_TEST_BUCKET").unwrap_or_else(|_| "rdbtest".to_string()),
        // Unique per run as well as per test: each run builds a fresh database
        // in a fresh temp directory, and a prefix already holding a *different*
        // database's catalogue is refused — correctly, but it would make the
        // second run of the suite fail for the wrong reason.
        prefix: format!("e2e/{}/{prefix}", run_id()),
        region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        endpoint: Endpoint::parse(&endpoint).expect("endpoint must be a URL"),
        credentials: Credentials::from_env().expect("AWS credentials must be set"),
        path_style: true,
        // Low levels: these tests are about correctness, and a high level would
        // spend seconds proving something the compression round-trip test in
        // the unit suite already covers.
        snapshot_level: 3,
        segment_level: 3,
        compression_threads: 1,
        long_range: true,
        part_size: 5 * 1024 * 1024,
        upload_concurrency: 2,
        max_attempts: 3,
        // Every schedule off: these tests drive each step explicitly, so a
        // background flush firing mid-assertion could not confuse them.
        snapshot_interval_secs: 0,
        wal_flush_secs: 0,
        wal_bundle_bytes: 0,
        rollover_interval_secs: 0,
        retain_snapshots: 0,
        staging_max_bytes: 0,
    })
}

/// The catalogue's key under a config's prefix.
///
/// `BackupConfig::key` is private and widening it so a test can reach the
/// manifest would be the wrong trade — the prefix rule is one line.
fn manifest_key(config: &BackupConfig) -> String {
    format!(
        "{}{}",
        config.normalized_prefix(),
        rasterizeddb_core::core::backup::manifest::MANIFEST_KEY
    )
}

async fn run(database: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql)
        .unwrap_or_else(|| panic!("could not recognise {sql:?}"));
    let response = execute(purpose, database.clone()).await;
    assert_ne!(
        response.first(),
        Some(&3u8),
        "{sql} failed: {}",
        String::from_utf8_lossy(&response[1..])
    );
    response
}

/// Runs a statement inside an explicit transaction.
async fn run_in_txn(
    database: &Arc<Database>,
    sql: &str,
    tx: rasterizeddb_core::core::mvcc::transaction::TxId,
) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql)
        .unwrap_or_else(|| panic!("could not recognise {sql:?}"));
    let response =
        rasterizeddb_core::core::rql::executor::execute_in_txn(purpose, database.clone(), tx).await;
    assert_ne!(
        response.first(),
        Some(&3u8),
        "{sql} failed: {}",
        String::from_utf8_lossy(&response[1..])
    );
    response
}

/// Rows a `SELECT` returned.
async fn count_rows(database: &Arc<Database>, sql: &str) -> usize {
    let response = run(database, sql).await;
    assert_eq!(response.first(), Some(&2u8), "{sql} returned no result set");
    vec_into_rows(&response[1..]).expect("rows decode").len()
}

/// Opens a restored directory as a database and counts what survived.
async fn count_in_restored(dir: &Path, sql: &str) -> usize {
    let database = Arc::new(Database::new(dir.to_str().unwrap()).await);
    count_rows(&database, sql).await
}

macro_rules! e2e_test {
    ($name:ident, $config:ident, $body:block) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() {
            let Some($config) = config(stringify!($name)) else {
                eprintln!("skipping {}: RDB_S3_TEST_ENDPOINT is not set", stringify!($name));
                return;
            };
            init_engine_globals();
            $body
        }
    };
}

e2e_test!(restores_a_database_that_can_be_opened_and_queried, config, {
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE people (id INT, name STRING)").await;
    for i in 0..50 {
        run(
            &database,
            &format!("INSERT INTO people (id, name) VALUES ({i}, 'person{i}')"),
        )
        .await;
    }
    assert_eq!(count_rows(&database, "SELECT * FROM people").await, 50);

    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");
    service.snapshot_now().await.expect("snapshot");

    let target = root.join("restored");
    rasterizeddb_core::core::backup::restore_from(&config, &target, RecoveryTarget::Latest)
        .await
        .expect("restore");

    assert_eq!(
        count_in_restored(&target, "SELECT * FROM people").await,
        50,
        "the restored database must hold every row the original did"
    );
});

e2e_test!(captures_writes_that_happened_after_the_snapshot, config, {
    // The snapshot is taken, then more rows are written, then the log is
    // archived. A restore must pick up both halves — this is the whole point
    // of archiving the log rather than only snapshotting.
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE events (id INT, note STRING)").await;
    for i in 0..20 {
        run(&database, &format!("INSERT INTO events (id, note) VALUES ({i}, 'before')")).await;
    }

    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");
    service.snapshot_now().await.expect("snapshot");

    for i in 20..45 {
        run(&database, &format!("INSERT INTO events (id, note) VALUES ({i}, 'after')")).await;
    }
    service.archive_wal_now().await.expect("archive");

    let target = root.join("restored");
    rasterizeddb_core::core::backup::restore_from(&config, &target, RecoveryTarget::Latest)
        .await
        .expect("restore");

    assert_eq!(
        count_in_restored(&target, "SELECT * FROM events").await,
        45,
        "rows written after the snapshot must come back from the archived log"
    );
});

e2e_test!(recovers_to_a_point_in_time, config, {
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE ledger (id INT, note STRING)").await;

    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");
    service.snapshot_now().await.expect("snapshot");

    for i in 0..30 {
        run(&database, &format!("INSERT INTO ledger (id, note) VALUES ({i}, 'good')")).await;
    }
    // The barrier: everything up to here is recoverable, and the commit
    // sequence at this instant is the point a recovery can be asked for.
    service.archive_wal_now().await.expect("archive");
    let good_lsn = rasterizeddb_core::core::wal::wal_for(live.to_str().unwrap())
        .expect("the log is installed")
        .commit_lsn();

    // Then the mistake everyone wants to undo.
    for i in 30..60 {
        run(&database, &format!("INSERT INTO ledger (id, note) VALUES ({i}, 'oops')")).await;
    }
    service.archive_wal_now().await.expect("archive");

    let full = root.join("restored-full");
    rasterizeddb_core::core::backup::restore_from(&config, &full, RecoveryTarget::Latest)
        .await
        .expect("restore latest");
    assert_eq!(count_in_restored(&full, "SELECT * FROM ledger").await, 60);

    let rewound = root.join("restored-pit");
    let report = rasterizeddb_core::core::backup::restore_from(
        &config,
        &rewound,
        RecoveryTarget::Lsn(good_lsn),
    )
    .await
    .expect("restore to a point in time");
    assert_eq!(report.recovered_to_lsn, Some(good_lsn));

    assert_eq!(
        count_in_restored(&rewound, "SELECT * FROM ledger").await,
        30,
        "recovery to LSN {good_lsn} must stop before the later writes"
    );
});

e2e_test!(a_snapshot_taken_under_concurrent_writes_still_restores, config, {
    // The fuzzy-copy claim, against a live engine: rows keep being inserted for
    // the whole duration of the snapshot, so the files it copies are captured
    // at inconsistent moments and only replay can reconcile them.
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE busy (id INT, payload STRING)").await;
    for i in 0..100 {
        run(&database, &format!("INSERT INTO busy (id, payload) VALUES ({i}, 'seed')")).await;
    }

    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");

    let writer_db = database.clone();
    let writer = tokio::spawn(async move {
        for i in 100..400 {
            let sql = format!("INSERT INTO busy (id, payload) VALUES ({i}, 'concurrent')");
            let purpose = recognize_query_purpose(&sql).expect("parses");
            execute(purpose, writer_db.clone()).await;
        }
    });

    service.snapshot_now().await.expect("snapshot");
    writer.await.expect("writer finished");

    // Everything the writer did, whether it landed before, during or after the
    // copy, is now archived.
    service.archive_wal_now().await.expect("archive");

    let target = root.join("restored");
    rasterizeddb_core::core::backup::restore_from(&config, &target, RecoveryTarget::Latest)
        .await
        .expect("restore");

    let restored = count_in_restored(&target, "SELECT * FROM busy").await;
    let original = count_rows(&database, "SELECT * FROM busy").await;
    assert_eq!(original, 400);
    assert_eq!(
        restored, original,
        "a snapshot taken under concurrent writes must restore to the same row count"
    );
});

e2e_test!(the_sql_surface_drives_the_same_operations, config, {
    // `BACKUP` / `SHOW BACKUPS` / `RESTORE` go through the ordinary statement
    // dispatch, so this also confirms the new grammar does not shadow anything
    // and that its results decode as a result set.
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE notes (id INT, body STRING)").await;
    for i in 0..10 {
        run(&database, &format!("INSERT INTO notes (id, body) VALUES ({i}, 'n{i}')")).await;
    }

    // `install` rather than `connect`: the statement handler reaches the
    // service through the process-wide slot the server sets at start-up.
    rasterizeddb_core::core::backup::install(config.clone(), live.to_str().unwrap())
        .await
        .expect("install");

    let response = run(&database, "BACKUP DATABASE TO S3").await;
    assert_eq!(response.first(), Some(&0u8), "BACKUP should report a count");
    let uploaded = u64::from_le_bytes(response[1..9].try_into().unwrap());
    assert!(uploaded > 0, "the snapshot object should not be empty");

    for i in 10..20 {
        run(&database, &format!("INSERT INTO notes (id, body) VALUES ({i}, 'n{i}')")).await;
    }
    run(&database, "BACKUP WAL").await;

    // One row per snapshot and one per archived bundle.
    let listed = count_rows(&database, "SHOW BACKUPS").await;
    assert!(listed >= 2, "SHOW BACKUPS listed {listed} rows");

    let target = root.join("restored");
    let response = run(
        &database,
        &format!("RESTORE DATABASE FROM S3 TO '{}'", target.display()),
    )
    .await;
    assert_eq!(response.first(), Some(&2u8), "RESTORE should report a row");

    assert_eq!(count_in_restored(&target, "SELECT * FROM notes").await, 20);

    // Restoring over the directory the server is serving from is refused, and
    // says what to do instead.
    let sql = format!("RESTORE DATABASE FROM S3 TO '{}'", live.display());
    let purpose = recognize_query_purpose(&sql).expect("parses");
    let response = execute(purpose, database.clone()).await;
    assert_eq!(response.first(), Some(&3u8));
    let message = String::from_utf8_lossy(&response[1..]);
    assert!(message.contains("--restore-from-s3"), "{message}");
});

e2e_test!(a_pruned_catalogue_still_restores, config, {
    // Retention is the one operation whose bug is silent: deleting a segment a
    // surviving snapshot still needs produces a catalogue that looks healthy
    // and a restore that cannot complete. So the check is not "did it delete
    // the right keys" but "does what is left still rebuild the database".
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE t (id INT, tag STRING)").await;

    let mut config = config;
    config.retain_snapshots = 2;
    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");

    // Four generations, each a snapshot with writes and an archive flush after
    // it, so retention has both snapshots and segments to consider.
    for generation in 0..4 {
        for i in 0..10 {
            run(
                &database,
                &format!("INSERT INTO t (id, tag) VALUES ({}, 'g{generation}')", generation * 10 + i),
            )
            .await;
        }
        service.snapshot_now().await.expect("snapshot");
        service.archive_wal_now().await.expect("archive");
    }

    let manifest = service.manifest_snapshot().await;
    assert_eq!(
        manifest.snapshots.len(),
        2,
        "retention should have kept exactly the two newest snapshots"
    );

    // Everything the surviving catalogue points at must still be fetchable, and
    // the result must be the whole database.
    let target = root.join("restored");
    rasterizeddb_core::core::backup::restore_from(&config, &target, RecoveryTarget::Latest)
        .await
        .expect("restore after pruning");
    assert_eq!(count_in_restored(&target, "SELECT * FROM t").await, 40);

    // And the older of the two survivors must still be usable on its own,
    // which is the case retention is most likely to have broken.
    let oldest = manifest
        .snapshots_newest_first()
        .last()
        .expect("two survivors")
        .id
        .clone();
    let rewound = root.join("restored-oldest");
    rasterizeddb_core::core::backup::restore_from(
        &config,
        &rewound,
        RecoveryTarget::Snapshot(oldest.clone()),
    )
    .await
    .unwrap_or_else(|e| panic!("the oldest surviving snapshot {oldest} no longer restores: {e}"));
    assert_eq!(count_in_restored(&rewound, "SELECT * FROM t").await, 30);
});

e2e_test!(a_failed_restore_leaves_nothing_behind_and_can_be_retried, config, {
    // Without this the target directory would hold a half-unpacked database
    // after a failure, and the emptiness check would then refuse the retry —
    // a trap rather than a safeguard. The restore is therefore assembled in a
    // sibling directory and moved into place only once it is complete.
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE t (id INT, body STRING)").await;
    for i in 0..30 {
        run(&database, &format!("INSERT INTO t (id, body) VALUES ({i}, 'row{i}')")).await;
    }

    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");
    let good = service.snapshot_now().await.expect("snapshot");

    // Corrupt the snapshot object so unpacking fails part way through, which is
    // what a truncated upload or bit-rot would look like.
    let client = client_for(&config);
    let original = client.get_object(&good.key).await.expect("fetch snapshot");
    let mut damaged = original.clone();
    let midpoint = damaged.len() / 2;
    damaged[midpoint] ^= 0xff;
    client.put_object(&good.key, &damaged).await.expect("overwrite");

    let target = root.join("restored");
    let err = rasterizeddb_core::core::backup::restore_from(
        &config,
        &target,
        RecoveryTarget::Latest,
    )
    .await
    .expect_err("a corrupted snapshot must not restore");
    eprintln!("corrupted restore failed as expected: {err}");

    assert!(
        !target.exists(),
        "a failed restore must not leave a directory behind"
    );
    assert!(
        !root.join(".restored.restore-partial").exists(),
        "the staging directory must be cleaned up too"
    );

    // Put the good object back; the same target must now be usable.
    client.put_object(&good.key, &original).await.expect("restore object");
    rasterizeddb_core::core::backup::restore_from(&config, &target, RecoveryTarget::Latest)
        .await
        .expect("the retry must succeed");
    assert_eq!(count_in_restored(&target, "SELECT * FROM t").await, 30);
});

e2e_test!(the_background_task_archives_without_being_asked, config, {
    // The scheduled flush is what turns the configured interval into an actual
    // recovery point objective. Untested, a mistake in the loop would show up
    // as "we thought we had five-minute coverage" after an incident.
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE t (id INT)").await;

    let mut config = config;
    config.wal_flush_secs = 1;
    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");
    service.snapshot_now().await.expect("snapshot");
    service.spawn_background();

    for i in 0..25 {
        run(&database, &format!("INSERT INTO t (id) VALUES ({i})")).await;
    }

    // Nothing is flushed by hand from here on; the loop has to do it.
    let before = service.manifest_snapshot().await.segments.len();
    let mut archived = false;
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        if service.manifest_snapshot().await.segments.len() > before {
            archived = true;
            break;
        }
    }
    assert!(archived, "the scheduled flush never archived anything");

    let target = root.join("restored");
    rasterizeddb_core::core::backup::restore_from(&config, &target, RecoveryTarget::Latest)
        .await
        .expect("restore");
    assert_eq!(
        count_in_restored(&target, "SELECT * FROM t").await,
        25,
        "what the background task archived must be enough to restore"
    );
});

e2e_test!(archiving_suspends_when_staging_fills_and_resumes_when_it_drains, config, {
    // The failure this guards against is an outage, not data loss: uploads stop
    // working, checkpoints keep handing segments to a directory nobody drains,
    // and the disk fills until the database stops.
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE t (id INT)").await;

    let mut config = config;
    config.staging_max_bytes = 64 * 1024;
    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");

    assert!(!service.archiving_is_suspended());

    // Stand in for a backlog of segments that failed to upload.
    let ballast = service.staging_dir().join("00000000000000ff.wseg");
    std::fs::create_dir_all(service.staging_dir()).unwrap();
    std::fs::write(&ballast, vec![0u8; 128 * 1024]).unwrap();

    service.enforce_staging_ceiling();
    assert!(
        service.archiving_is_suspended(),
        "archiving should suspend once staging passes its ceiling"
    );

    // And checkpoints must actually stop staging, or the disk keeps filling.
    let wal = rasterizeddb_core::core::wal::wal_for(live.to_str().unwrap()).expect("wal");
    assert!(!wal.is_archiving(), "a suspended service must detach from the log");

    for i in 0..20 {
        run(&database, &format!("INSERT INTO t (id) VALUES ({i})")).await;
    }
    wal.checkpoint().expect("checkpoint");
    assert_eq!(
        std::fs::read_dir(service.staging_dir()).unwrap().count(),
        1,
        "a suspended service must not accumulate new segments"
    );

    // Drain it, and the service must come back on its own.
    std::fs::remove_file(&ballast).unwrap();
    service.enforce_staging_ceiling();
    assert!(!service.archiving_is_suspended(), "archiving should resume once drained");
    assert!(wal.is_archiving(), "the log must be re-attached on resume");
});

e2e_test!(transaction_outcomes_survive_a_restore, config, {
    // The engine is MVCC: a row's visibility depends on how its transaction
    // ended, and that outcome lives in `db_transactions.status` rather than in
    // the row. A snapshot that captured the rows but not the status log would
    // restore into a database that cannot tell a committed row from a rolled
    // back one — and the rows would still be *there*, so it would look fine.
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE ledger (id INT, note STRING)").await;

    // `BEGIN`/`COMMIT`/`ROLLBACK` are session state in the pgwire layer rather
    // than statements the recogniser knows, so the block is driven through the
    // transaction manager directly — which is what that layer does underneath.
    // `commit` records the outcome in memory; the flush after it is what makes
    // it durable, and it is a separate call on purpose — a statement inside the
    // block pays for its own rows and its outcome record with one `fdatasync`
    // rather than two. The pgwire layer calls both (`flush_outcomes`), and a
    // test that drove only the first would be asserting durability against a
    // path that never claimed it.
    let committed = database.transactions.begin();
    for i in 0..10 {
        run_in_txn(&database, &format!("INSERT INTO ledger (id, note) VALUES ({i}, 'kept')"), committed).await;
    }
    database.transactions.commit(committed);
    database.transactions.flush().expect("the commit outcome must be durable");

    let rolled_back = database.transactions.begin();
    for i in 100..110 {
        run_in_txn(&database, &format!("INSERT INTO ledger (id, note) VALUES ({i}, 'undone')"), rolled_back).await;
    }
    database.transactions.abort(rolled_back);
    database.transactions.flush().expect("the rollback outcome must be durable");

    let live_rows = count_rows(&database, "SELECT * FROM ledger").await;
    assert_eq!(live_rows, 10, "the rollback should have left nothing behind");

    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");
    service.snapshot_now().await.expect("snapshot");

    let target = root.join("restored");
    rasterizeddb_core::core::backup::restore_from(&config, &target, RecoveryTarget::Latest)
        .await
        .expect("restore");

    // The status log must have come along, or the rolled-back rows reappear.
    assert!(
        target.join("db_transactions.status").exists(),
        "the transaction status log must be part of the snapshot"
    );
    assert_eq!(
        count_in_restored(&target, "SELECT * FROM ledger").await,
        10,
        "a restored database must agree with the original about which \
         transactions committed"
    );
});

e2e_test!(a_snapshot_whose_log_cannot_be_archived_is_discarded, config, {
    // `Wal::checkpoint` returns `Ok(())` when it declines to run — and it can
    // decline in two places, the second of them under the writer lock, after
    // the point where staging happens. A snapshot that inferred "my window was
    // cut" from that `Ok` would record an `end_lsn` whose records were never
    // archived: a catalogue entry that looks healthy and a restore that stops
    // short of consistency and says nothing.
    //
    // So the snapshot checks the condition instead, and discards itself if it
    // does not hold. Losing a snapshot costs a retry; recording an
    // unrestorable one costs whatever depended on it.
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE t (id INT)").await;
    for i in 0..10 {
        run(&database, &format!("INSERT INTO t (id) VALUES ({i})")).await;
    }

    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");

    // Hold a transaction with a pending in-place write open. That is exactly
    // the condition that makes every checkpoint decline. It targets a scratch
    // file rather than a real one, so committing it later disturbs nothing.
    let wal = rasterizeddb_core::core::wal::wal_for(live.to_str().unwrap()).expect("wal");
    let scratch = live.join("scratch.db");
    std::fs::write(&scratch, vec![0u8; 64]).unwrap();
    let scratch_id = rasterizeddb_core::core::wal::file_id_for_name("scratch.db");
    wal.register_file(scratch_id, &scratch);

    let pending = wal.begin();
    wal.log_in_place(pending, scratch_id, 0, b"held").expect("log in place");

    let err = service
        .snapshot_now()
        .await
        .expect_err("a snapshot whose log cannot be archived must not be recorded");
    assert!(
        err.to_string().contains("could not be archived"),
        "unexpected error: {err}"
    );
    eprintln!("snapshot correctly discarded: {err}");

    assert!(
        service.manifest_snapshot().await.snapshots.is_empty(),
        "a discarded snapshot must leave no catalogue entry behind"
    );

    // With the transaction closed, checkpoints work again and the retry lands.
    wal.commit_txn(pending).expect("commit");
    service.snapshot_now().await.expect("the retry should succeed");
    assert_eq!(service.manifest_snapshot().await.snapshots.len(), 1);

    let target = root.join("restored");
    rasterizeddb_core::core::backup::restore_from(&config, &target, RecoveryTarget::Latest)
        .await
        .expect("restore");
    assert_eq!(count_in_restored(&target, "SELECT * FROM t").await, 10);
});

e2e_test!(refuses_to_restore_over_an_existing_directory, config, {
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE t (id INT)").await;
    run(&database, "INSERT INTO t (id) VALUES (1)").await;

    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");
    service.snapshot_now().await.expect("snapshot");

    // Merging a restore into files that are already a database would leave one
    // that is neither the old nor the new.
    let err = rasterizeddb_core::core::backup::restore_from(&config, &live, RecoveryTarget::Latest)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists, "{err}");
});

e2e_test!(a_prefix_belonging_to_another_database_is_refused, config, {
    let root = temp_testdir::TempDir::default();
    let first = root.join("first");
    let second = root.join("second");
    std::fs::create_dir_all(&first).unwrap();
    std::fs::create_dir_all(&second).unwrap();

    let db_one = Arc::new(Database::new(first.to_str().unwrap()).await);
    run(&db_one, "CREATE TABLE t (id INT)").await;
    let service = BackupService::connect(config.clone(), first.to_str().unwrap())
        .await
        .expect("connect");
    service.snapshot_now().await.expect("snapshot");

    // Two databases sharing a prefix would overwrite each other's catalogue and
    // leave both unrestorable, so the second one is stopped at start-up.
    let _db_two = Arc::new(Database::new(second.to_str().unwrap()).await);
    let err = match BackupService::connect(config.clone(), second.to_str().unwrap()).await {
        Err(e) => e,
        Ok(_) => panic!("a second database must not be allowed to share the prefix"),
    };
    assert!(err.to_string().contains("already holds backups"), "{err}");
});

e2e_test!(a_prefix_with_no_snapshot_gets_its_base_without_being_asked, config, {
    // The bug this pins: the snapshot loop slept a whole interval before doing
    // anything, so a new prefix held nothing restorable for a day — while the
    // archive loop happily filled it with segments that had no snapshot to
    // replay onto. Every deployment restarted inside the interval never got a
    // base at all, and the only thing that ever produced one was somebody
    // pressing BACKUP NOW.
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE t (id INT)").await;
    for i in 0..12 {
        run(&database, &format!("INSERT INTO t (id) VALUES ({i})")).await;
    }

    let mut config = config;
    // A day away: on the old behaviour nothing whatsoever happens in the next
    // ten seconds.
    config.snapshot_interval_secs = 86_400;
    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");
    assert!(
        service.manifest_snapshot().await.snapshots.is_empty(),
        "the prefix must start empty for this to be testing anything"
    );

    // Nothing is snapshotted by hand from here on.
    service.spawn_background();

    let mut took_one = false;
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        if !service.manifest_snapshot().await.snapshots.is_empty() {
            took_one = true;
            break;
        }
    }
    assert!(took_one, "a prefix with no snapshot must not wait out the interval");

    // And it is a real one: file counts and a successful upload have both lied
    // before, so the assertion is a restore.
    let target = root.join("restored");
    rasterizeddb_core::core::backup::restore_from(&config, &target, RecoveryTarget::Latest)
        .await
        .expect("restore");
    assert_eq!(count_in_restored(&target, "SELECT * FROM t").await, 12);
});

e2e_test!(an_overdue_snapshot_is_taken_at_start_not_an_interval_later, config, {
    // The second half of the same bug. A catalogue that already holds a
    // snapshot used to have its next one scheduled from process start, so a
    // server restarted more often than the interval never reached one —
    // silently, since the loop is running and the log keeps being archived.
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE t (id INT)").await;
    run(&database, "INSERT INTO t (id) VALUES (1)").await;

    let mut config = config;
    config.snapshot_interval_secs = 86_400;
    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");
    service.snapshot_now().await.expect("snapshot");

    // Reach past the service and age the catalogue by two days, which is what a
    // host that has been restarting inside the interval looks like.
    let client = client_for(&config);
    let key = manifest_key(&config);
    let mut manifest = rasterizeddb_core::core::backup::manifest::Manifest::decode(
        &client.get_object(&key).await.expect("manifest"),
    )
    .expect("decode");
    let two_days = 2 * 86_400;
    for snapshot in &mut manifest.snapshots {
        snapshot.started_at = snapshot.started_at.saturating_sub(two_days);
        snapshot.finished_at = snapshot.finished_at.saturating_sub(two_days);
    }
    let before = manifest.snapshots.len();
    client.put_object(&key, &manifest.encode()).await.expect("put manifest");

    // A fresh service over the aged catalogue — a restart, in other words.
    let restarted = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("reconnect");
    restarted.spawn_background();

    let mut caught_up = false;
    for _ in 0..40 {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        if restarted.manifest_snapshot().await.snapshots.len() > before {
            caught_up = true;
            break;
        }
    }
    assert!(
        caught_up,
        "a snapshot two days overdue must be taken at start, not an interval later"
    );
});

e2e_test!(a_rollover_records_when_it_happened, config, {
    // A rollover leaves an ordinary snapshot behind, so without this stamp
    // nothing in the catalogue says one ever ran — and the schedule would have
    // to start from process start again, which is the bug two tests up.
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE t (id INT)").await;
    run(&database, "INSERT INTO t (id) VALUES (1)").await;

    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");
    service.snapshot_now().await.expect("first snapshot");
    assert_eq!(
        service.manifest_snapshot().await.last_rollover_at,
        0,
        "an ordinary snapshot is not a rollover"
    );

    service.rollover_now().await.expect("rollover");
    let stamped = service.manifest_snapshot().await.last_rollover_at;
    assert_ne!(stamped, 0, "a rollover must record when it happened");

    // And it is on the object, not just in memory: the point of the stamp is
    // that the next process reads it.
    let client = client_for(&config);
    let key = manifest_key(&config);
    let published = rasterizeddb_core::core::backup::manifest::Manifest::decode(
        &client.get_object(&key).await.expect("manifest"),
    )
    .expect("decode");
    assert_eq!(published.last_rollover_at, stamped);

    // The rollover kept exactly one generation, and it restores.
    assert_eq!(published.snapshots.len(), 1);
    let target = root.join("restored");
    rasterizeddb_core::core::backup::restore_from(&config, &target, RecoveryTarget::Latest)
        .await
        .expect("restore");
    assert_eq!(count_in_restored(&target, "SELECT * FROM t").await, 1);
});

e2e_test!(a_rollover_removes_the_bundles_behind_it, config, {
    // What a rollover promises, checked against the bucket rather than against
    // the catalogue: an entry dropped from the manifest while its object stays
    // behind is the same bill at the end of the month, and the same prefix that
    // never stops growing.
    //
    // The rule that decides which bundles those are lives in
    // `manifest::segment_needed_by`, and the case that made a live rollover
    // keep every one of them — a surviving snapshot whose `start_lsn` is 0
    // because the commit sequence restarted with the log — is pinned in the
    // unit tests there. This one holds the end of it: what is deleted is gone,
    // and what is left still restores every row.
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE t (id INT, tag STRING)").await;

    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");

    // Two generations before the rollover, each with its own archived window.
    for generation in 0..2 {
        for i in 0..5 {
            run(
                &database,
                &format!("INSERT INTO t (id, tag) VALUES ({}, 'g{generation}')", generation * 5 + i),
            )
            .await;
        }
        service.snapshot_now().await.expect("snapshot");
        service.archive_wal_now().await.expect("archive");
    }

    let before = service.manifest_snapshot().await;
    let superseded: Vec<String> = before
        .snapshots
        .iter()
        .map(|s| s.key.clone())
        .chain(before.segments.iter().map(|s| s.key.clone()))
        .collect();
    assert!(
        before.segments.len() >= 2,
        "the test needs bundles to supersede, got {:?}",
        before.segments
    );

    // Writes the rollover's own snapshot has to carry.
    for i in 10..15 {
        run(&database, &format!("INSERT INTO t (id, tag) VALUES ({i}, 'after')")).await;
    }

    let (report, removed) = service.rollover_now().await.expect("rollover");
    assert!(removed > 0, "the rollover deleted nothing");

    let after = service.manifest_snapshot().await;
    assert_eq!(after.snapshots.len(), 1, "a rollover keeps exactly one generation");
    assert_eq!(after.snapshots[0].key, report.key);

    let client = client_for(&config);
    for key in &superseded {
        assert!(
            !after.segments.iter().any(|s| &s.key == key)
                && !after.snapshots.iter().any(|s| &s.key == key),
            "{key} is still in the catalogue after a rollover"
        );
        assert!(
            client.get_object(key).await.is_err(),
            "{key} was dropped from the catalogue but left in the bucket"
        );
    }

    // What remains is self-contained: every row, from the new snapshot and the
    // bundles the rollover kept.
    let target = root.join("restored");
    rasterizeddb_core::core::backup::restore_from(&config, &target, RecoveryTarget::Latest)
        .await
        .expect("restore after a rollover");
    assert_eq!(count_in_restored(&target, "SELECT * FROM t").await, 15);
});

e2e_test!(a_created_database_is_archived_and_restored, config, {
    // Every database made with `CREATE DATABASE` is a subdirectory with a log
    // of its own, and the service used to attach to the root's log only. Its
    // checkpoints therefore discarded records that reached no archive — and
    // since the snapshot excludes live logs at every depth, those records were
    // in neither the copy nor the log that reconciles it. The backup reported
    // success over what was left.
    //
    // Both ways in are exercised: a database that exists before the service
    // starts, and one created while it is already running.
    use rasterizeddb_core::core::database::catalog::DatabaseCatalog;

    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let live_str = live.to_str().unwrap().to_string();

    let catalogue = DatabaseCatalog::open(Some(&live_str), "master")
        .await
        .expect("catalogue");
    let master = catalogue.get("master").expect("the default database");
    run(&master, "CREATE TABLE ledger (id INT)").await;
    run(&master, "INSERT INTO ledger (id) VALUES (1)").await;

    let early = catalogue.create("early").await.expect("create early");
    run(&early, "CREATE TABLE t (id INT, tag STRING)").await;
    run(&early, "INSERT INTO t (id, tag) VALUES (1, 'before the service')").await;

    let service = BackupService::connect(config.clone(), &live_str)
        .await
        .expect("connect");

    // Created after the service attached to what existed: nothing re-runs the
    // attach, so this is the case that needs the catalogue's hook.
    let late = catalogue.create("late").await.expect("create late");
    run(&late, "CREATE TABLE t (id INT, tag STRING)").await;
    for i in 0..5 {
        run(&late, &format!("INSERT INTO t (id, tag) VALUES ({i}, 'before the snapshot')")).await;
    }
    run(&early, "INSERT INTO t (id, tag) VALUES (2, 'before the snapshot')").await;

    service.snapshot_now().await.expect("snapshot");

    // Writes after the snapshot: these exist only in the archived log, so they
    // are what proves the log of a created database is being archived at all.
    for i in 5..10 {
        run(&late, &format!("INSERT INTO t (id, tag) VALUES ({i}, 'after the snapshot')")).await;
    }
    run(&early, "INSERT INTO t (id, tag) VALUES (3, 'after the snapshot')").await;
    run(&master, "INSERT INTO ledger (id) VALUES (2)").await;
    service.archive_wal_now().await.expect("archive");

    let manifest = service.manifest_snapshot().await;
    let snapshot = manifest.snapshots_newest_first()[0];
    for database in ["", "early", "late"] {
        assert!(
            snapshot.window_for(database).is_some(),
            "the snapshot recorded no window for {database:?}"
        );
        assert!(
            !manifest.segments_for(database).is_empty(),
            "no log of {database:?} was archived"
        );
    }
    // One database's records must never end up in another's bundle.
    for segment in &manifest.segments {
        let expected = if segment.database.is_empty() {
            "wal/".to_string()
        } else {
            format!("wal/{}/", segment.database)
        };
        assert!(
            segment.key.contains(&expected),
            "{} does not name the log it came from",
            segment.key
        );
    }

    let target = root.join("restored");
    rasterizeddb_core::core::backup::restore_from(&config, &target, RecoveryTarget::Latest)
        .await
        .expect("restore");

    // Each database is opened from its own directory, which is where a restore
    // has to put it for the catalogue to find it again.
    assert_eq!(count_in_restored(&target, "SELECT * FROM ledger").await, 2);
    assert_eq!(count_in_restored(&target.join("early"), "SELECT * FROM t").await, 3);
    assert_eq!(
        count_in_restored(&target.join("late"), "SELECT * FROM t").await,
        10,
        "the writes after the snapshot exist only in the archived log"
    );
});


e2e_test!(a_restored_copy_reads_its_own_directory, config, {
    // A restore that answers queries from the *source* directory is the worst
    // shape a backup bug can take: the verification query passes, and it passes
    // by consulting the very thing the copy was supposed to replace. On a host
    // where the source is gone — the case a restore exists for — the same
    // database comes back empty.
    //
    // The mechanism was `db_storage.cfg`. The snapshot carries it like any
    // other file, and it names absolute paths: the source's directory as a
    // storage location, and every table placed on it. Restored elsewhere, it
    // still named the source, so the copy's own files were never opened. A
    // write against the clone would have landed in the original.
    //
    // So the source is taken away before the copy is asked anything.
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let database = Arc::new(Database::new(live.to_str().unwrap()).await);
    run(&database, "CREATE TABLE ledger (id INT, note STRING)").await;
    for i in 0..25 {
        run(&database, &format!("INSERT INTO ledger (id, note) VALUES ({i}, 'kept')")).await;
    }

    let service = BackupService::connect(config.clone(), live.to_str().unwrap())
        .await
        .expect("connect");
    service.snapshot_now().await.expect("snapshot");

    let target = root.join("restored");
    rasterizeddb_core::core::backup::restore_from(&config, &target, RecoveryTarget::Latest)
        .await
        .expect("restore");

    // Nothing may point back at the source.
    let pool = std::fs::read_to_string(target.join("db_storage.cfg")).unwrap_or_default();
    assert!(
        !pool.contains(live.to_str().unwrap()),
        "the restored storage pool still names the source directory:\n{pool}"
    );

    // And the copy has to stand on its own with the source gone.
    drop(database);
    std::fs::remove_dir_all(&live).expect("remove the source");

    assert_eq!(
        count_in_restored(&target, "SELECT * FROM ledger").await,
        25,
        "the restored copy must hold its own data, not read the source's"
    );
});
