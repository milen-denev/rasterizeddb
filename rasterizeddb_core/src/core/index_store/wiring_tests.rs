//! The gate wired in (slice B11), on a real database: the census (V3) over
//! every write path from a forced clean point, ticket ownership and
//! lifetime, the single flight of `DIRTY`, `VACUUM FULL`'s durable epoch
//! (also through crash images), T1 through crash images of a real `INSERT`,
//! and one word per partition.

use std::path::{Path, PathBuf};
use std::sync::Arc as StdArc;

use parking_lot::Mutex;
use rclite::Arc;

use super::census::{Census, Scope, check, holds_on};
use super::epoch_log::{self, EPOCH_LOG_FILE, NO_PID};
use super::file_gate::FileGate;
use super::gate_word::GateState;
use super::marker::{self, LogState};
use crate::core::database::Database;
use crate::core::power_loss::failpoint;
use crate::core::power_loss::image::{CrashPoint, Recorder};
use crate::core::row::table::Table;
use crate::core::storage_providers::backend::DbStorageProvider;

type DbTable = Table<DbStorageProvider>;

async fn open(dir: &Path, name: &str) -> Arc<Database> {
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(
        Database::try_new_named(&dir.to_string_lossy(), false, name)
            .await
            .expect("open the database"),
    )
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&dir.to_string_lossy());
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap_or_else(|| panic!("parse {sql}"));
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    assert!(!out.is_empty() && out[0] != 3, "{sql}: {}", String::from_utf8_lossy(&out));
    out
}

async fn count(db: &Arc<Database>, table: &str) -> usize {
    let out = exec(db, &format!("SELECT id FROM {table} WHERE 1 = 1")).await;
    crate::core::row::row::vec_into_rows(&out[1..]).map(|r| r.len()).unwrap_or(0)
}

fn table(db: &Database, name: &str) -> Arc<DbTable> {
    db.table_for_tests(name).expect("the table")
}

/// A table with a primary key (an index) and filtered columns, holding
/// `rows` rows, its filter backfilled and its indexes built.
async fn seeded(db: &Arc<Database>, name: &str, rows: i64) -> Arc<DbTable> {
    exec(db, &format!("CREATE TABLE {name} (id BIGINT PRIMARY KEY, v BIGINT, s TEXT)")).await;
    if rows > 0 {
        let values: Vec<String> = (0..rows).map(|i| format!("({i}, {}, 's{i}')", i * 10)).collect();
        exec(db, &format!("INSERT INTO {name} (id, v, s) VALUES {}", values.join(", "))).await;
    }
    let t = table(db, name);
    t.wait_filters_ready().await.unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while t.indexes().unwrap().is_some_and(|ix| !ix.is_ready()) {
        assert!(std::time::Instant::now() < deadline, "the index build never finished");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    t
}

/// What the census attributes to `t`: its gate, structures and files.
fn scope_files(t: &DbTable) -> (Vec<usize>, Vec<PathBuf>) {
    (t.census_structures(), t.census_files())
}

/// `_flt.db` is written by the filter's runtime checkpoint loop on its own
/// task, under no ticket (B14 retires the sidecar; B12's `GatedIo` gates it
/// meanwhile). It still needs the durable `DIRTY`, which the census checks.
const EXEMPT: &[&str] = &["_flt.db"];

fn assert_census(events: &[super::census::Event], t: &DbTable, began_clean: bool) -> super::census::Summary {
    let (structures, files) = scope_files(t);
    let summary = check(
        events,
        &Scope {
            gate: t.gate().census_id(),
            began_clean,
            structures: &structures,
            files: &files,
            exempt: EXEMPT,
        },
    )
    .unwrap_or_else(|e| panic!("census: {e}"));
    if began_clean {
        assert_eq!(summary.dirty_durable, 1, "DIRTY was made durable {} times", summary.dirty_durable);
    }
    summary
}

fn log_of(t: &DbTable) -> marker::MarkerScan {
    marker::scan(&std::fs::read(t.gate().ixm_path().unwrap()).unwrap()).unwrap()
}

/// INSERT, UPDATE (a relocating and a point one), DELETE and an upsert from
/// a forced clean point: a durable `DIRTY` and a ticket held on the table's
/// gate before every mutation and every write; the family's ticket given
/// back when each statement's owner is released.
#[tokio::test(flavor = "multi_thread")]
async fn the_census_over_insert_update_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "census_dml").await;
    let t = seeded(&db, "c", 20).await;
    t.gate().force_clean_for_tests();
    assert_eq!(t.gate().state(), GateState::Clean);

    let census = Census::start();
    exec(&db, "INSERT INTO c (id, v, s) VALUES (100, 1000, 'new')").await;
    exec(&db, "UPDATE c SET s = 'longer than it was before' WHERE id = 3").await;
    exec(&db, "UPDATE c SET v = 31 WHERE id = 3").await;
    exec(&db, "DELETE FROM c WHERE id = 4").await;
    exec(&db, "INSERT INTO c (id, v, s) VALUES (5, 1, 'x') ON CONFLICT (id) DO UPDATE SET v = 55").await;
    let events = census.finish();

    let s = assert_census(&events, &t, true);
    assert!(s.writes > 0 && s.ticket_mutations > 0, "the census saw nothing: {s:?}");
    for m in ["TableFilters::on_insert", "TableIndexes::on_insert"] {
        assert!(s.mutators.contains(&m), "{m} was not seen: {:?}", s.mutators);
    }
    assert_eq!(t.gate().word().load().count(), 0, "a ticket outlived its statement's owner");
    assert_eq!(t.gate().state(), GateState::Dirty);
    let log = log_of(&t);
    assert_eq!((log.state(), log.records.len(), log.trailing()), (LogState::Dirty, 3, 0));
    assert_eq!(count(&db, "c").await, 20);
    close(dir.path(), db);
}

/// One ticket per (family, file set), kept on the lock owner's record
/// through the family's statements and until its owner is released after
/// the outcome is final: not at the end of the statement that took it.
#[tokio::test(flavor = "multi_thread")]
async fn a_family_keeps_one_ticket_until_its_owner_is_released() {
    use crate::core::mvcc::owner::{LockOwner, OwnerKind, under};
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "census_family").await;
    let t = seeded(&db, "f", 5).await;
    let gate = t.gate().clone();
    for commit in [true, false] {
        let tx = db.transactions.begin();
        for i in 0..3 {
            let sql = format!("INSERT INTO f (id, v, s) VALUES ({}, 1, 'b')", 100 + i + if commit { 0 } else { 10 });
            let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&sql).unwrap();
            let out = under(
                &db.transactions,
                tx,
                OwnerKind::Block,
                crate::core::rql::executor::execute_in_txn(purpose, db.clone(), tx),
            )
            .await;
            assert_ne!(out.first(), Some(&3), "{}", String::from_utf8_lossy(&out));
            // Between statements, its outcome not final: still held, once.
            assert!(db.transactions.family_tickets().keeps_on(tx, &gate), "the family gave its ticket back early");
            assert_eq!(db.transactions.family_tickets().kept_by(tx), 1);
            assert_eq!(gate.word().load().count(), 1, "one ticket per family and file set");
        }
        if commit {
            db.commit_written_transaction(tx).unwrap();
        } else {
            db.abort_transaction(tx);
        }
        // The outcome is final, the owner not yet released: still held.
        assert_eq!(gate.word().load().count(), 1);
        db.release_owner(LockOwner::block(tx).unwrap());
        assert_eq!(gate.word().load().count(), 0, "released with the owner");
        assert!(!db.transactions.family_tickets().keeps_on(tx, &gate));
    }
    assert_eq!(count(&db, "f").await, 8);
    close(dir.path(), db);
}

/// Many first writers after a clean point single-flight the slow path:
/// `DIRTY` is appended and fdatasynced once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn many_first_writers_make_dirty_durable_once() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "census_flight").await;
    let t = seeded(&db, "m", 0).await;
    t.gate().force_clean_for_tests();
    let watch = crate::core::sync_audit::watch(dir.path());
    // The first slow path lingers between its write and its fdatasync, so
    // the other writers arrive while it is in flight.
    let _linger = failpoint::arm(
        "dirty_append_before_fsync",
        dir.path(),
        1,
        StdArc::new(|_: &failpoint::Hit<'_>| {
            std::thread::sleep(std::time::Duration::from_millis(150));
            failpoint::Outcome::Continue
        }),
    );
    let barrier = StdArc::new(tokio::sync::Barrier::new(16));
    let census = Census::start();
    let mut tasks = Vec::new();
    for i in 0..16 {
        let db = db.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            exec(&db, &format!("INSERT INTO m (id, v, s) VALUES ({i}, {i}, 'w')")).await;
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    // One slow path ran its I/O; every other writer waited for it rather
    // than reading its DIRTY back before it was durable and writing on.
    assert_census(&census.finish(), &t, true);
    let marker_syncs = watch
        .take()
        .into_iter()
        .filter(|(tag, _)| *tag == super::file_gate::SYNC_MARKER)
        .count();
    assert_eq!(marker_syncs, 1, "DIRTY was fdatasynced {marker_syncs} times");
    let log = log_of(&t);
    assert_eq!(log.records.len(), 3, "DIRTY written more than once: {:?}", log.records);
    assert_eq!(count(&db, "m").await, 16);
    close(dir.path(), db);
}

/// VACUUM from a clean point: the forgets and the filter clears it applies
/// are made under a ticket on the table's own gate, after a durable `DIRTY`.
#[tokio::test(flavor = "multi_thread")]
async fn the_census_over_vacuum_and_its_filter_clears() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "census_vacuum").await;
    let t = seeded(&db, "v", 40).await;
    exec(&db, "DELETE FROM v WHERE id < 20").await;
    t.gate().force_clean_for_tests();

    let census = Census::start();
    let report = t.vacuum_now().await.unwrap();
    let events = census.finish();
    assert!(report.dead_rows > 0, "nothing was dead: {report:?}");
    let s = assert_census(&events, &t, true);
    assert!(s.holds > 0, "the sweep took no ticket");
    for m in ["TableFilters::on_delete_runs_at", "TableIndexes::forget"] {
        assert!(s.mutators.contains(&m), "{m} was not applied: {:?}", s.mutators);
    }
    assert_eq!(t.gate().word().load().count(), 0);
    close(dir.path(), db);
}

/// RV2: a reopen followed by a vacuum pass with nothing new to apply takes
/// no ticket, so the table stays clean. In B11 an open rebuilds the filter
/// and the indexes from every pointer, dead versions included, so the first
/// pass after a reopen has real forgets and clears to make and takes its one
/// ticket; the pass after it has nothing new, and takes none. (B14 and B17
/// load the structures from a snapshot instead, with the dead versions
/// already cleared and forgotten, and the first pass is then the empty one.)
#[tokio::test(flavor = "multi_thread")]
async fn a_reopen_then_a_vacuum_with_nothing_to_apply_takes_no_ticket() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open(dir.path(), "census_reopen").await;
        let t = seeded(&db, "r", 40).await;
        exec(&db, "DELETE FROM r WHERE id < 20").await;
        t.vacuum_now().await.unwrap();
        drop(t);
        close(dir.path(), db);
    }
    let db = open(dir.path(), "census_reopen").await;
    let t = table(&db, "r");
    t.wait_filters_ready().await.unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while t.indexes().unwrap().is_some_and(|ix| !ix.is_ready()) {
        assert!(std::time::Instant::now() < deadline, "the index build never finished");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    // What the rebuild put back for the dead versions goes once.
    t.vacuum_now().await.unwrap();
    t.gate().force_clean_for_tests();
    let census = Census::start();
    let report = t.vacuum_now().await.unwrap();
    let events = census.finish();
    assert!(report.dead_rows > 0, "the pass had tombstones to look at: {report:?}");
    assert_eq!(holds_on(&events, t.gate().census_id()), 0, "a pass with nothing to apply took a ticket");
    assert_eq!(t.gate().state(), GateState::Clean);
    let log = log_of(&t);
    assert_eq!(log.proven_clean().is_some(), true, "the pass wrote to the marker log");
    close(dir.path(), db);
}

/// An UPDATE that overwrites in place (no new version) passes the gate too.
#[tokio::test(flavor = "multi_thread")]
async fn the_census_over_an_in_place_update() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "census_inplace").await;
    let t = seeded(&db, "p", 10).await;
    t.gate().force_clean_for_tests();
    let before = t.in_place_column_updates();
    let census = Census::start();
    exec(&db, "UPDATE p SET v = 77 WHERE id = 2").await;
    let events = census.finish();
    assert!(t.in_place_column_updates() > before, "the update did not take the in-place path");
    let s = assert_census(&events, &t, true);
    assert!(s.writes > 0, "{s:?}");
    close(dir.path(), db);
}

/// `VACUUM FULL` bumps the slot epoch durably before its first effect: the
/// census sees the ticket first, `rdb.ixe` holds the new epoch, and a reopen
/// recovers it.
#[tokio::test(flavor = "multi_thread")]
async fn vacuum_full_bumps_the_slot_epoch_durably() {
    let dir = tempfile::tempdir().unwrap();
    let incarnation;
    {
        let db = open(dir.path(), "census_full").await;
        let t = seeded(&db, "x", 40).await;
        incarnation = t.schema.incarnation;
        exec(&db, "DELETE FROM x WHERE id < 30").await;
        assert_eq!(t.gate().slot_epoch(), 0);
        t.gate().force_clean_for_tests();
        let census = Census::start();
        exec(&db, "VACUUM FULL x").await;
        let events = census.finish();
        assert_census(&events, &t, true);
        assert_eq!(t.gate().slot_epoch(), 1);
        exec(&db, "VACUUM FULL x").await;
        assert_eq!(t.gate().slot_epoch(), 2);
        drop(t);
        close(dir.path(), db);
    }
    let log = epoch_log::decode(&std::fs::read(dir.path().join(EPOCH_LOG_FILE)).unwrap()).unwrap();
    assert_eq!(log.epoch_of(incarnation, NO_PID), Some(2));
    let db = open(dir.path(), "census_full").await;
    let t = table(&db, "x");
    assert_eq!((t.gate().slot_epoch(), t.gate().epoch_proven()), (2, true));
    assert_eq!(count(&db, "x").await, 10);
    close(dir.path(), db);
}

fn arm_cuts(
    rec: &StdArc<Recorder>,
    name: &str,
    scope: &Path,
    hits: u64,
    sink: &StdArc<Mutex<Vec<Result<CrashPoint, String>>>>,
) -> Vec<failpoint::Guard> {
    (1..=hits)
        .map(|nth| failpoint::arm(name, scope, nth, rec.cut_action(format!("{name} #{nth}"), sink.clone())))
        .collect()
}

fn snapshot_files(dir: &Path, names: &[String]) -> Vec<Vec<u8>> {
    names.iter().map(|n| std::fs::read(dir.join(n)).unwrap_or_default()).collect()
}

/// `VACUUM FULL`'s epoch in crash images: in every image whose replayed
/// pointer file shows compaction's work, `rdb.ixe` holds the bump.
#[tokio::test(flavor = "multi_thread")]
async fn vacuum_full_s_epoch_precedes_its_effects_in_every_crash_image() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    std::fs::create_dir(&dir).unwrap();
    let db = open(&dir, "full_crash").await;
    let t = seeded(&db, "y", 60).await;
    let incarnation = t.schema.incarnation;
    exec(&db, "DELETE FROM y WHERE id % 3 <> 0").await;
    db.sync_durable().unwrap();
    let names = vec!["y_pointers.db".to_string(), "y.db".to_string()];
    let rec = Recorder::start(&dir);
    let before = rec.cut_quiescent("before VACUUM FULL").unwrap();
    let sink = StdArc::new(Mutex::new(Vec::new()));
    let _cuts = arm_cuts(&rec, "sync:wal.log", &dir, 8, &sink);
    let _data = arm_cuts(&rec, "sync:wal.checkpoint.data_file", &dir, 4, &sink);
    exec(&db, "VACUUM FULL y").await;
    drop(_cuts);
    drop(_data);
    let mut points: Vec<CrashPoint> = sink.lock().drain(..).filter_map(Result::ok).collect();
    points.push(rec.cut_quiescent("after VACUUM FULL").unwrap());
    drop(t);
    close(&dir, db);
    drop(rec);
    // The pre-compaction bytes, as an open of an image cut before the
    // statement leaves them.
    let img = base.path().join("image");
    before.write_image(0, &img);
    let db = open(&img, "full_crash").await;
    let baseline = snapshot_files(&img, &names);
    close(&img, db);
    let _ = std::fs::remove_dir_all(&img);
    let mut effects = 0;
    for p in &points {
        for seed in 0..6 {
            p.write_image(seed, &img);
            let db = open(&img, "full_crash").await;
            assert_eq!(count(&db, "y").await, 20, "{} seed {seed}: rows lost", p.label);
            let files = snapshot_files(&img, &names);
            close(&img, db);
            let epoch = std::fs::read(img.join(EPOCH_LOG_FILE))
                .ok()
                .and_then(|b| epoch_log::decode(&b).ok())
                .and_then(|l| l.epoch_of(incarnation, NO_PID))
                .unwrap_or(0);
            if files != baseline {
                effects += 1;
                assert_eq!(epoch, 1, "{} seed {seed}: compaction's effects without its epoch", p.label);
            }
            let _ = std::fs::remove_dir_all(&img);
        }
    }
    assert!(effects > 0, "no image held compaction's effects");
}

/// T1 on a real `INSERT`: in every image cut around the slow path and the
/// statement's syncs, a table whose marker log still ends `CLEAN` replays
/// to exactly its rows and pointers before the statement.
#[tokio::test(flavor = "multi_thread")]
async fn t1_an_insert_after_a_clean_point_in_crash_images() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    std::fs::create_dir(&dir).unwrap();
    let db = open(&dir, "t1_crash").await;
    let t = seeded(&db, "w", 30).await;
    db.sync_durable().unwrap();
    t.gate().force_clean_for_tests();
    let names = vec!["w_pointers.db".to_string(), "w.db".to_string()];
    let rec = Recorder::start(&dir);
    let before = rec.cut_quiescent("before the INSERT").unwrap();
    let sink = StdArc::new(Mutex::new(Vec::new()));
    let _a = arm_cuts(&rec, "dirty_append_before_fsync", &dir, 1, &sink);
    let _b = arm_cuts(&rec, "dirty_after_fsync", &dir, 1, &sink);
    let _c = arm_cuts(&rec, "sync:wal.log", &dir, 3, &sink);
    exec(&db, "INSERT INTO w (id, v, s) VALUES (1000, 1, 'after the clean point')").await;
    drop((_a, _b, _c));
    let mut points: Vec<CrashPoint> = sink.lock().drain(..).filter_map(Result::ok).collect();
    points.push(rec.cut_quiescent("after the INSERT").unwrap());
    assert!(points.len() >= 3, "cuts were lost: {}", points.len());
    drop(t);
    close(&dir, db);
    drop(rec);
    let img = base.path().join("image");
    before.write_image(0, &img);
    let db = open(&img, "t1_crash").await;
    let baseline = snapshot_files(&img, &names);
    close(&img, db);
    let _ = std::fs::remove_dir_all(&img);
    let (mut still_clean, mut written) = (0, 0);
    for p in &points {
        for seed in 0..8 {
            p.write_image(seed, &img);
            let log = std::fs::read(img.join("w.ixm")).unwrap_or_default();
            let clean = marker::scan(&log).map(|s| s.state() == LogState::Clean).unwrap_or(false);
            let db = open(&img, "t1_crash").await;
            let n = count(&db, "w").await;
            let files = snapshot_files(&img, &names);
            close(&img, db);
            let _ = std::fs::remove_dir_all(&img);
            if clean {
                still_clean += 1;
                assert_eq!(n, 30, "{} seed {seed}: a row beside a log still ending CLEAN", p.label);
                assert!(files == baseline, "{} seed {seed}: the table's files changed beside a log still ending CLEAN", p.label);
            } else if n == 31 {
                written += 1;
            }
        }
    }
    assert!(still_clean > 0 && written > 0, "the cuts missed one side: {still_clean} clean, {written} written");
}

/// `Unknown` at open (an unreadable log) is the slow path's other branch: the
/// first write rewrites the log as `HEADER` + `DIRTY` before anything else.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_log_is_rewritten_before_the_first_write() {
    let dir = tempfile::tempdir().unwrap();
    let incarnation;
    {
        let db = open(dir.path(), "census_unknown").await;
        let t = seeded(&db, "u", 5).await;
        incarnation = t.schema.incarnation;
        // A checkpoint the reopen loads: otherwise its filter backfill
        // checkpoints `_flt.db`, which B12 gates like any write (the release
        // slow path, under its exemption), and that write, not the INSERT,
        // would be the first to rewrite the log.
        t.checkpoint_filters().await.unwrap();
        drop(t);
        close(dir.path(), db);
    }
    std::fs::write(dir.path().join("u.ixm"), vec![0xee; 50]).unwrap();
    let db = open(dir.path(), "census_unknown").await;
    let t = table(&db, "u");
    assert_eq!(t.gate().state(), GateState::Unknown);
    t.wait_filters_ready().await.unwrap();
    let census = Census::start();
    exec(&db, "INSERT INTO u (id, v, s) VALUES (50, 1, 'z')").await;
    let events = census.finish();
    assert_census(&events, &t, true);
    assert_eq!(std::fs::read(dir.path().join("u.ixm")).unwrap(), marker::compacted_log(incarnation));
    close(dir.path(), db);
}

/// A reload keeps the slot's word; each partition of a partitioned table is
/// its own file set with its own word.
#[tokio::test(flavor = "multi_thread")]
async fn a_reload_keeps_the_word_and_each_partition_has_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "census_slots").await;
    let t = seeded(&db, "k", 5).await;
    let word = t.gate().clone();
    let slot = db.slot("k").unwrap();
    assert!(StdArc::ptr_eq(&slot.gate(), &word), "the slot holds another word");
    exec(&db, "CREATE INDEX k_v ON k (v)").await;
    let reloaded = table(&db, "k");
    assert!(!Arc::ptr_eq(&reloaded, &t), "CREATE INDEX did not reload");
    assert!(StdArc::ptr_eq(reloaded.gate(), &word), "a reload derived a new word");
    assert!(StdArc::ptr_eq(&slot.gate(), &word));

    exec(
        &db,
        "CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT, v BIGINT) PARTITION BY HASH (tenant_id) PARTITIONS 4",
    )
    .await;
    let values: Vec<String> = (0..64).map(|i| format!("({i}, {i}, {i})")).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await;
    let pt = db.partitioned_table("pt").unwrap();
    let epoch = pt.epoch().unwrap();
    let parts = pt.materialized(&epoch, &crate::core::partition::Route::All).unwrap();
    assert!(parts.len() >= 2, "too few partitions materialized: {}", parts.len());
    let mut paths = Vec::new();
    for (pid, part) in &parts {
        let g = part.gate();
        assert!(g.has_files());
        assert!(
            parts.iter().filter(|(_, o)| StdArc::ptr_eq(o.gate(), g)).count() == 1,
            "partition {pid} shares a word"
        );
        paths.push(g.ixm_path().unwrap());
    }
    paths.sort();
    paths.dedup();
    assert_eq!(paths.len(), parts.len(), "partitions share a marker log");
    let _ = FileGate::detached();
    close(dir.path(), db);
}
