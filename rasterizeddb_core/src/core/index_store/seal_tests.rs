//! Seal and publish on a real database (slice B13): the published
//! snapshot's identity against the files it describes, durability through
//! crash images around every step, a writer cancelling an optional seal and
//! queueing behind a mandatory one, failures before and after `CLEAN`, the
//! three clean points, and the word carried across RENAME.

use std::path::Path;
use std::sync::Arc as StdArc;
use std::time::Duration;

use parking_lot::Mutex;
use rclite::Arc;

use super::*;
use crate::core::database::Database;
use crate::core::filter::{FilterConfig, TableFilters};
use crate::core::index_store::gate_word::{GateState, SealKind};
use crate::core::index_store::identity::VersionTuple;
use crate::core::index_store::marker::{LogState, MarkerRecord};
use crate::core::power_loss::failpoint::{self, Outcome};
use crate::core::power_loss::image::{CrashPoint, Recorder};
use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
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

async fn ready(t: &DbTable) {
    t.wait_filters_ready().await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while t.indexes().unwrap().is_some_and(|ix| !ix.is_ready()) {
        assert!(std::time::Instant::now() < deadline, "the index build never finished");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A table with a primary key and filtered columns holding `rows` rows,
/// its filter backfilled and its index built.
async fn seeded(db: &Arc<Database>, name: &str, rows: i64) -> Arc<DbTable> {
    exec(db, &format!("CREATE TABLE {name} (id BIGINT PRIMARY KEY, v BIGINT, s TEXT)")).await;
    if rows > 0 {
        let values: Vec<String> = (0..rows).map(|i| format!("({i}, {}, 's{i}')", i * 10)).collect();
        exec(db, &format!("INSERT INTO {name} (id, v, s) VALUES {}", values.join(", "))).await;
    }
    let t = table(db, name);
    ready(&t).await;
    t
}

async fn checkpoint(db: &Arc<Database>, name: &str) -> Vec<crate::core::database::index_checkpoint::Sealed> {
    db.index_checkpoint(name, None).await.expect("rdb_index_checkpoint").sealed
}

fn scan_of(path: &Path) -> Option<MarkerScan> {
    std::fs::read(path).ok().and_then(|b| marker::scan(&b).ok())
}

/// The `CLEAN` `stem`'s log ends in (proven: nothing after it), checked
/// against the `.ixs` it names: length, footer, identity, a decodable
/// snapshot with a FILTER section.
fn proven(dir: &Path, stem: &str) -> CleanRecord {
    let scan = scan_of(&dir.join(format!("{stem}.ixm"))).expect("a readable marker log");
    let clean = *scan.proven_clean().expect("the log proves a snapshot");
    let ixs = std::fs::read(dir.join(format!("{stem}.ixs"))).expect("the snapshot the CLEAN names");
    let snap = snapshot::decode(&ixs).expect("a whole snapshot");
    assert_eq!(clean.snapshot_len, ixs.len() as u64, "the CLEAN's length");
    assert_eq!(clean.footer, snap.footer, "the CLEAN's footer");
    assert_eq!(clean.identity, snap.header.identity, "the CLEAN's identity against the snapshot's");
    clean
}

/// The identity against the files as an open finds them (after recovery)
/// and the reopened table: lengths, slot count, incarnation, epoch,
/// versions, shape; and the FILTER section reads back as the filter's base
/// image, complete over every slot.
fn identity_matches(dir: &Path, stem: &str, t: &DbTable, id: &Identity) {
    let pointers = std::fs::metadata(dir.join(format!("{stem}_pointers.db"))).unwrap().len();
    let rows = std::fs::metadata(dir.join(format!("{stem}.db"))).unwrap().len();
    assert_eq!(id.pointers_len, pointers, "pointers length");
    assert_eq!(id.rows_len, rows, "rows length");
    assert_eq!(id.n_slots, pointers / ROW_POINTER_RECORD_LEN as u64, "slot count");
    assert_eq!(id.incarnation, t.schema.incarnation, "incarnation");
    assert_eq!(id.slot_epoch, t.gate().slot_epoch(), "slot epoch");
    assert_eq!(id.versions, VersionTuple::current(crate::core::filter::persist::FORMAT_VER));
    assert_eq!(id.shape_hash, crate::core::index::shape_of(&t.schema), "shape hash");
    let ixs = std::fs::read(dir.join(format!("{stem}.ixs"))).unwrap();
    let snap = snapshot::decode(&ixs).unwrap();
    let (_, filter) = snap
        .sections
        .iter()
        .find(|(e, _)| e.kind == SectionKind::Filter)
        .expect("a FILTER section");
    let f = TableFilters::from_checkpoint(filter, &t.schema.fields, FilterConfig::default(), id.n_slots)
        .unwrap_or_else(|e| panic!("the FILTER section does not load: {e:?}"));
    // As the live filter has it: a compaction leaves the frontier at the
    // old high-water mark, past the slots that remain.
    assert!(f.resume_slot() >= id.n_slots, "the section's frontier stops short of the data: {} < {}", f.resume_slot(), id.n_slots);
    assert!(f.is_complete());
}

/// Publish, then the identity against the files a reopen finds; a second
/// checkpoint has nothing to add; any later write, and the next open sees
/// `Dirty`.
#[tokio::test(flavor = "multi_thread")]
async fn a_published_clean_matches_the_files_and_any_later_write_reopens_dirty() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = open(dir.path(), "seal_identity").await;
        let t = seeded(&db, "s", 300).await;
        exec(&db, "DELETE FROM s WHERE id < 40").await;
        exec(&db, "VACUUM FULL s").await;
        ready(&t).await;
        let t = table(&db, "s");
        ready(&t).await;
        assert_eq!(t.gate().slot_epoch(), 1);
        exec(&db, "INSERT INTO s (id, v, s) VALUES (1000, 1, 'after the compaction')").await;
        // Vacuum's next sweep would legitimately take a ticket for the
        // compaction's leftovers and dirty the table again: stopped, as
        // shutdown stops it, so the assertions below see only the seal.
        t.quiesce_for_shutdown().await;
        let sealed = checkpoint(&db, "s").await;
        assert_eq!(sealed.len(), 1);
        assert!(matches!(sealed[0].outcome, SealOutcome::Published), "{:?}", sealed[0].outcome);
        assert_eq!(t.gate().state(), GateState::Clean);
        assert!(t.gate().vouched());
        let clean = proven(dir.path(), "s");
        assert_eq!(clean.identity.slot_epoch, 1);
        let again = checkpoint(&db, "s").await;
        assert!(matches!(again[0].outcome, SealOutcome::AlreadyClean), "{:?}", again[0].outcome);
        assert!(!dir.path().join("s.ixs.tmp").exists() && !dir.path().join("s.ixm.tmp").exists());
        drop(t);
        close(dir.path(), db);
    }
    {
        // Read before the open: the reopen's first vacuum sweep may take a
        // ticket of its own (B11: a rebuild re-adds what vacuum forgot) and
        // append DIRTY right after it.
        let clean = proven(dir.path(), "s");
        let db = open(dir.path(), "seal_identity").await;
        let t = table(&db, "s");
        assert_ne!(t.gate().state(), GateState::Unknown);
        identity_matches(dir.path(), "s", &t, &clean.identity);
        assert_eq!(count(&db, "s").await, 261);
        // An in-place UPDATE changes no length: only DIRTY says it happened.
        exec(&db, "UPDATE s SET v = 7 WHERE id = 50").await;
        assert_eq!(t.gate().state(), GateState::Dirty);
        drop(t);
        close(dir.path(), db);
    }
    let db = open(dir.path(), "seal_identity").await;
    let t = table(&db, "s");
    assert_eq!(t.gate().state(), GateState::Dirty, "a write after the CLEAN, and the open sees Clean");
    assert_eq!(scan_of(&dir.path().join("s.ixm")).unwrap().proven_clean(), None);
    drop(t);
    close(dir.path(), db);
}

fn arm_cuts(
    rec: &StdArc<Recorder>,
    names: &[&str],
    scope: &Path,
    sink: &StdArc<Mutex<Vec<Result<CrashPoint, String>>>>,
) -> Vec<failpoint::Guard> {
    names
        .iter()
        .map(|name| failpoint::arm(name, scope, 1, rec.cut_action(name.to_string(), sink.clone())))
        .collect()
}

const SEAL_STEPS: &[&str] = &[
    "snapshot_before_fsync",
    "snapshot_after_fsync",
    "snapshot_after_rename",
    "snapshot_after_dir_fsync",
    "marker_compaction_after_rename",
    "clean_append_before_fsync",
    "clean_after_fsync",
];

/// Every crash image cut around a seal's steps: a log that proves a
/// snapshot names a whole `.ixs` whose length, footer and identity it
/// records; cut before the `CLEAN` was written, the log never proves one.
/// Run for a first seal (no log yet: the compaction path, whose directory
/// sync covers the rename) and a second one after a write (a log ending
/// `DIRTY`: nothing after the snapshot's rename syncs the directory but its
/// own fsync, and an old `.ixs` sits under the name).
#[tokio::test(flavor = "multi_thread")]
async fn crash_images_around_every_seal_step_never_prove_a_missing_or_partial_snapshot() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    std::fs::create_dir(&dir).unwrap();
    let db = open(&dir, "seal_crash").await;
    let t = seeded(&db, "c", 400).await;
    db.sync_durable().unwrap();
    let (mut proving, mut not, mut cut_before_clean) = (0, 0, 0);
    for round in 0..2 {
        if round == 1 {
            exec(&db, "INSERT INTO c (id, v, s) VALUES (5000, 1, 'between the seals')").await;
            exec(&db, "UPDATE c SET s = 'moved' WHERE id < 100").await;
            db.sync_durable().unwrap();
        }
        let rec = Recorder::start(&dir);
        let sink = StdArc::new(Mutex::new(Vec::new()));
        let cuts = arm_cuts(&rec, SEAL_STEPS, &dir, &sink);
        let sealed = checkpoint(&db, "c").await;
        assert!(matches!(sealed[0].outcome, SealOutcome::Published), "round {round}: {:?}", sealed[0].outcome);
        drop(cuts);
        let mut points: Vec<CrashPoint> = sink.lock().drain(..).filter_map(Result::ok).collect();
        points.push(rec.cut_quiescent("after the seal").unwrap());
        assert!(points.len() >= 4, "round {round}: cuts were lost: {}", points.len());
        let published = snapshot::decode(&std::fs::read(dir.join("c.ixs")).unwrap()).unwrap().footer;
        for p in &points {
            // Cut labels start with the failpoint's name.
            let before_clean = SEAL_STEPS[..5].iter().any(|step| p.label.starts_with(step));
            if before_clean {
                cut_before_clean += 1;
            }
            // From the directory fsync after the rename on, the new `.ixs`
            // is durable under its name in every image: no seed may undo
            // the rename (in round 1 that would bring back the old `.ixs`
            // under a `CLEAN` written after it).
            let renamed_durably = !SEAL_STEPS[..3].iter().any(|step| p.label.starts_with(step));
            for seed in 0..24 {
                let img = p.image(seed);
                if renamed_durably {
                    let ixs = img.get(Path::new("c.ixs")).unwrap_or_else(|| {
                        panic!("round {round}, {} seed {seed}: the durable rename was undone (no c.ixs)", p.label)
                    });
                    let footer = snapshot::decode(ixs).map(|s| s.footer);
                    assert_eq!(
                        footer.ok(),
                        Some(published),
                        "round {round}, {} seed {seed}: the durable rename was undone (another or a torn c.ixs)",
                        p.label
                    );
                }
                let log = img.get(Path::new("c.ixm")).cloned().unwrap_or_default();
                let Some(clean) = marker::scan(&log).ok().and_then(|s| s.proven_clean().copied()) else {
                    not += 1;
                    continue;
                };
                assert!(!before_clean, "round {round}, {} seed {seed}: a CLEAN before it was written", p.label);
                proving += 1;
                let ixs = img
                    .get(Path::new("c.ixs"))
                    .unwrap_or_else(|| panic!("round {round}, {} seed {seed}: CLEAN over no .ixs", p.label));
                assert_eq!(ixs.len() as u64, clean.snapshot_len, "round {round}, {} seed {seed}: a partial .ixs", p.label);
                let snap = snapshot::decode(ixs)
                    .unwrap_or_else(|e| panic!("round {round}, {} seed {seed}: CLEAN over a torn .ixs: {e}", p.label));
                assert_eq!(snap.footer, clean.footer, "round {round}, {} seed {seed}: another .ixs", p.label);
                assert_eq!(snap.header.identity, clean.identity, "round {round}, {} seed {seed}", p.label);
            }
        }
        drop(rec);
    }
    assert!(proving > 0 && not > 0, "the cuts missed one side: {proving} proving, {not} not");
    assert!(cut_before_clean >= 6, "too few cuts before the CLEAN: {cut_before_clean}");
    drop(t);
    close(&dir, db);
}

/// Parks the next hit of `name` under `scope` until released: `reached`
/// says it got there.
fn park(name: &str, scope: &Path) -> (failpoint::Guard, std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (reached_tx, reached) = std::sync::mpsc::channel::<()>();
    let (go, go_rx) = std::sync::mpsc::channel::<()>();
    let go_rx = std::sync::Mutex::new(go_rx);
    let reached_tx = std::sync::Mutex::new(reached_tx);
    let guard = failpoint::arm(
        name,
        scope,
        1,
        StdArc::new(move |_: &failpoint::Hit<'_>| {
            let _ = reached_tx.lock().unwrap().send(());
            let _ = go_rx.lock().unwrap().recv_timeout(Duration::from_secs(30));
            Outcome::Continue
        }),
    );
    (guard, reached, go)
}

async fn wait_reached(reached: &std::sync::mpsc::Receiver<()>) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        if reached.try_recv().is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    false
}

/// A writer during an optional seal cancels it and goes ahead without
/// waiting for the sealer; the seal publishes nothing and leaves `Dirty`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_cancels_an_optional_seal_and_no_clean_is_written() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "seal_optional").await;
    let t = seeded(&db, "o", 50).await;
    let (guard, reached, go) = park("snapshot_after_fsync", dir.path());
    let sealer = {
        let t = t.clone();
        tokio::spawn(async move { t.seal_snapshot(SealKind::Optional).await })
    };
    assert!(wait_reached(&reached).await, "the seal never reached its snapshot fsync");
    let insert = tokio::time::timeout(
        Duration::from_secs(5),
        exec(&db, "INSERT INTO o (id, v, s) VALUES (900, 1, 'a writer preempts')"),
    )
    .await;
    assert!(insert.is_ok(), "the writer waited for an optional seal");
    let _ = go.send(());
    let outcome = sealer.await.unwrap();
    drop(guard);
    assert!(matches!(outcome, SealOutcome::Skipped(Skip::Cancelled)), "{outcome:?}");
    assert_eq!(t.gate().state(), GateState::Dirty);
    let w = t.gate().word().load();
    assert!(!w.sealed() && !w.cancelled() && !w.publishing(), "flags left on the word: {w:?}");
    assert!(scan_of(&dir.path().join("o.ixm")).is_none_or(|s| s.state() == LogState::Dirty), "a CLEAN was written");
    assert!(!dir.path().join("o.ixs.tmp").exists(), "the abandoned .ixs.tmp was left");
    assert_eq!(count(&db, "o").await, 51);
    close(dir.path(), db);
}

/// A writer during a mandatory seal queues behind it, and is admitted after
/// the `CLEAN`, through `DIRTY`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_queues_behind_a_mandatory_seal_and_dirties_it_after() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "seal_mandatory").await;
    let t = seeded(&db, "m", 50).await;
    let (guard, reached, go) = park("snapshot_after_fsync", dir.path());
    let sealer = {
        let t = t.clone();
        tokio::spawn(async move { t.seal_snapshot(SealKind::Mandatory).await })
    };
    assert!(wait_reached(&reached).await, "the seal never reached its snapshot fsync");
    let writer = {
        let db = db.clone();
        tokio::spawn(async move {
            exec(&db, "INSERT INTO m (id, v, s) VALUES (900, 1, 'queued')").await;
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!writer.is_finished(), "a writer went ahead of a mandatory seal");
    let _ = go.send(());
    let outcome = sealer.await.unwrap();
    drop(guard);
    assert!(matches!(outcome, SealOutcome::Published), "{outcome:?}");
    tokio::time::timeout(Duration::from_secs(10), writer).await.expect("the writer never ran").unwrap();
    assert_eq!(t.gate().state(), GateState::Dirty);
    let log = scan_of(&dir.path().join("m.ixm")).unwrap();
    let kinds: Vec<&str> = log
        .records
        .iter()
        .map(|r| match r.record {
            MarkerRecord::Header { .. } => "HEADER",
            MarkerRecord::Dirty => "DIRTY",
            MarkerRecord::Clean(_) => "CLEAN",
        })
        .collect();
    assert_eq!(kinds, ["HEADER", "DIRTY", "CLEAN", "DIRTY"]);
    assert_eq!(count(&db, "m").await, 51);
    close(dir.path(), db);
}

/// A barrier that fails publishes nothing (§3 step 4): the state stays
/// `Dirty` and no `.ixs` is left.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_barrier_publishes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "seal_barrier").await;
    let t = seeded(&db, "b", 20).await;
    for name in ["barrier_root_sync_fails", "barrier_dir_sync_fails"] {
        let _f = failpoint::arm(
            name,
            dir.path(),
            1,
            StdArc::new(|_: &failpoint::Hit<'_>| Outcome::Fail(std::io::Error::other("injected log sync failure"))),
        );
        let outcome = t.seal_snapshot(SealKind::Mandatory).await;
        assert!(matches!(outcome, SealOutcome::Failed(_)), "{name}: {outcome:?}");
        assert_eq!(t.gate().state(), GateState::Dirty, "{name}");
        assert!(!dir.path().join("b.ixs").exists() && !dir.path().join("b.ixs.tmp").exists(), "{name}");
        assert!(scan_of(&dir.path().join("b.ixm")).is_none_or(|s| s.proven_clean().is_none()), "{name}");
    }
    close(dir.path(), db);
}

/// `CLEAN`'s fdatasync fails once its write was issued: the state is
/// `Unknown`, never `Dirty`, and the next write rewrites the log first.
#[tokio::test(flavor = "multi_thread")]
async fn a_clean_fsync_that_fails_leaves_unknown_and_the_next_write_rewrites_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "seal_unknown").await;
    let t = seeded(&db, "u", 20).await;
    {
        let _f = failpoint::arm(
            "clean_fsync_fails",
            dir.path(),
            1,
            StdArc::new(|_: &failpoint::Hit<'_>| Outcome::Fail(std::io::Error::from_raw_os_error(5))),
        );
        let outcome = t.seal_snapshot(SealKind::Mandatory).await;
        assert_eq!(t.gate().state(), GateState::Unknown, "a failed CLEAN fsync left the word trusting the log");
        assert!(matches!(outcome, SealOutcome::FailedAfterClean(_)), "{outcome:?}");
    }
    exec(&db, "UPDATE u SET v = 3 WHERE id = 4").await;
    assert_eq!(t.gate().state(), GateState::Dirty);
    assert_eq!(
        std::fs::read(dir.path().join("u.ixm")).unwrap(),
        marker::compacted_log(t.schema.incarnation),
        "the write did not rewrite the log before it was issued"
    );
    close(dir.path(), db);
}

/// `rdb_index_checkpoint` on a partitioned table seals each materialized
/// partition; inside a block that has written the table it seals nothing
/// and says so at once rather than waiting `seal_timeout` on its own ticket.
#[tokio::test(flavor = "multi_thread")]
async fn rdb_index_checkpoint_fans_out_over_partitions_and_skips_its_own_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "seal_fanout").await;
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
    for (_, p) in &parts {
        ready(p).await;
    }
    let report = db.index_checkpoint("pt", None).await.unwrap();
    assert_eq!(report.sealed.len(), parts.len(), "{report:?}");
    for (pid, p) in &parts {
        assert_eq!(p.gate().state(), GateState::Clean, "partition {pid}");
        let stem = p.gate().stem().unwrap().to_string();
        proven(p.gate().dir().unwrap(), &stem);
    }
    assert!(db.index_checkpoint("no_such_table", None).await.is_err());

    // Inside a block that wrote `w`: reported at once, nothing sealed.
    let w = seeded(&db, "w", 10).await;
    use crate::core::mvcc::owner::{LockOwner, OwnerKind, under};
    let tx = db.transactions.begin();
    let sql = "INSERT INTO w (id, v, s) VALUES (100, 1, 'in the block')";
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap();
    let out = under(
        &db.transactions,
        tx,
        OwnerKind::Block,
        crate::core::rql::executor::execute_in_txn(purpose, db.clone(), tx),
    )
    .await;
    assert_ne!(out.first(), Some(&3), "{}", String::from_utf8_lossy(&out));
    let started = std::time::Instant::now();
    let report = db.index_checkpoint("w", Some(tx)).await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(2), "it waited on its own ticket");
    assert_eq!(report.own_writes, vec!["w".to_string()], "{report:?}");
    assert!(report.sealed.is_empty());
    db.commit_written_transaction(tx).unwrap();
    db.release_owner(LockOwner::block(tx).unwrap());
    assert_eq!(w.gate().state(), GateState::Dirty);
    close(dir.path(), db);
}

/// Shutdown seals every file set: plain tables and every partition.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_seals_every_table_and_partition() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "seal_shutdown").await;
    let a = seeded(&db, "a", 30).await;
    let b = seeded(&db, "b", 0).await;
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
    for (_, p) in &parts {
        ready(p).await;
    }
    let report = db.seal_for_shutdown(Duration::from_secs(60)).await;
    assert!(report.skipped.is_empty(), "{report:?}");
    for s in &report.sealed {
        assert!(s.outcome.is_clean(), "{}: {}", s.name, s.outcome.describe());
    }
    let mut gates = vec![("a".to_string(), a.gate().clone()), ("b".to_string(), b.gate().clone())];
    for (pid, p) in &parts {
        gates.push((format!("partition {pid}"), p.gate().clone()));
    }
    assert_eq!(report.sealed.len(), gates.len(), "{report:?}");
    for (name, g) in &gates {
        assert_eq!(g.state(), GateState::Clean, "{name} was not sealed");
        proven(g.dir().unwrap(), g.stem().unwrap());
    }
    assert!(a.is_retired(), "vacuum and the filter's loop still run after shutdown's quiesce");
    close(dir.path(), db);
}

/// The idle checkpoint's clock: due only after the whole threshold with the
/// word `Dirty`, unsealed and unticketed and the mutation counter still.
#[test]
fn the_idle_clock_restarts_on_any_ticket_seal_or_mutation() {
    use crate::core::index_store::gate_word::{SEALED, Word};
    use crate::core::row::table::table_seal::IdleWatch;
    let t0 = tokio::time::Instant::now();
    let th = Duration::from_secs(10);
    let dirty = Word::new(GateState::Dirty);
    let s = |n| t0 + Duration::from_secs(n);
    let mut w = IdleWatch::new(7, t0);
    assert!(!w.due(s(5), 7, dirty, th));
    assert!(w.due(s(10), 7, dirty, th));
    // A committed mutation restarts it.
    assert!(!w.due(s(11), 8, dirty, th));
    assert!(!w.due(s(20), 8, dirty, th));
    assert!(w.due(s(21), 8, dirty, th));
    // A ticket held at a look restarts it.
    assert!(!w.due(s(22), 8, Word(dirty.0 + 1), th));
    assert!(!w.due(s(31), 8, dirty, th));
    assert!(w.due(s(32), 8, dirty, th));
    // So does a seal running, and a word that is not Dirty.
    assert!(!w.due(s(33), 8, Word(dirty.0 | SEALED), th));
    assert!(!w.due(s(43), 8, Word::new(GateState::Clean), th));
    assert!(!w.due(s(52), 8, dirty, th));
    assert!(w.due(s(53), 8, dirty, th));
}

/// The idle checkpoint seals a table left alone, and again after a write.
#[tokio::test(flavor = "multi_thread")]
async fn the_idle_checkpoint_seals_an_idle_table() {
    use crate::core::row::table::table_seal::idle_override;
    let dir = tempfile::tempdir().unwrap();
    let _idle = idle_override::set(dir.path(), Duration::from_millis(400));
    let db = open(dir.path(), "seal_idle").await;
    let t = seeded(&db, "i", 25).await;
    let wait_clean = |t: Arc<DbTable>| async move {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while t.gate().state() != GateState::Clean {
            assert!(std::time::Instant::now() < deadline, "the idle checkpoint never sealed the table");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    wait_clean(t.clone()).await;
    proven(dir.path(), "i");
    exec(&db, "INSERT INTO i (id, v, s) VALUES (500, 1, 'after idle')").await;
    assert_eq!(t.gate().state(), GateState::Dirty);
    wait_clean(t.clone()).await;
    let clean = proven(dir.path(), "i");
    assert_eq!(clean.identity.n_slots, 26);
    close(dir.path(), db);
}

/// RENAME carries the pair and the word (B11 deviation 2): the renamed table
/// shares the word, its log is the old one's with the rename's `DIRTY`
/// last, nothing is left under the old stem, and it seals under its new
/// name. Renaming back, after a write, finds no `CLEAN`. DROP and CREATE of
/// the name start from no log.
#[tokio::test(flavor = "multi_thread")]
async fn rename_carries_the_pair_and_the_word_and_drop_create_inherits_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let db = open(d, "seal_rename").await;
    let a = seeded(&db, "ra", 20).await;
    let incarnation = a.schema.incarnation;
    checkpoint(&db, "ra").await;
    let a_clean = proven(d, "ra");
    let word = a.gate().word().clone();
    drop(a);

    exec(&db, "ALTER TABLE ra RENAME TO rb").await;
    let b = table(&db, "rb");
    ready(&b).await;
    assert!(StdArc::ptr_eq(b.gate().word(), &word), "the renamed table opened with a fresh word");
    assert_eq!(b.schema.incarnation, incarnation);
    for ext in [".ixm", ".ixs", ".ixm.tmp", ".ixs.tmp"] {
        assert!(!d.join(format!("ra{ext}")).exists(), "ra{ext} was left under the old stem");
    }
    let log = scan_of(&d.join("rb.ixm")).expect("the marker log was carried");
    assert_eq!(log.incarnation, incarnation);
    assert_eq!(log.state(), LogState::Dirty, "the rename's ACCESS EXCLUSIVE ticket made it DIRTY");
    assert!(
        log.records.iter().any(|r| r.record == MarkerRecord::Clean(a_clean)),
        "the carried log is not the old one"
    );
    assert!(d.join("rb.ixs").exists(), "the snapshot was not carried");
    assert_eq!(b.gate().state(), GateState::Dirty);
    checkpoint(&db, "rb").await;
    let clean = proven(d, "rb");
    assert_eq!(clean.identity.incarnation, incarnation);

    // Written under the new name, then renamed back: no CLEAN comes home.
    exec(&db, "UPDATE rb SET v = 1 WHERE id = 3").await;
    drop(b);
    exec(&db, "ALTER TABLE rb RENAME TO ra").await;
    let a = table(&db, "ra");
    assert!(StdArc::ptr_eq(a.gate().word(), &word));
    assert_ne!(a.gate().state(), GateState::Clean);
    assert_eq!(scan_of(&d.join("ra.ixm")).unwrap().proven_clean(), None);
    assert!(!d.join("rb.ixm").exists() && !d.join("rb.ixs").exists());
    drop(a);

    // DROP and CREATE under the name: a new incarnation, no log.
    exec(&db, "DROP TABLE ra").await;
    for ext in [".ixm", ".ixs"] {
        assert!(!d.join(format!("ra{ext}")).exists(), "DROP left ra{ext}");
    }
    let fresh = seeded(&db, "ra", 3).await;
    assert_ne!(fresh.schema.incarnation, incarnation);
    assert!(!StdArc::ptr_eq(fresh.gate().word(), &word));
    assert_eq!(fresh.gate().state(), GateState::Dirty);
    close(d, db);
}

/// A table in a storage pool's directory: its first `.ixm` sets
/// `index_store_v1` there and in the database's root, the one directory
/// `incompat::check` reads at open (B11 deviation 4).
#[cfg(all(feature = "storage_pools", feature = "persistent_indexes"))]
#[tokio::test(flavor = "multi_thread")]
async fn a_pool_tables_first_marker_log_sets_the_flag_in_the_root_too() {
    use crate::core::database::incompat::{INDEX_STORE_V1, flags};
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    let disk = base.path().join("disk");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&disk).unwrap();
    let db = open(&root, "seal_pool").await;
    db.storage
        .add_location(&disk.to_string_lossy(), crate::core::storage::LocationRole::Primary, 1, true)
        .unwrap();
    db.storage.pin("pooled", &disk.to_string_lossy()).unwrap();
    let t = seeded(&db, "pooled", 10).await;
    assert_eq!(t.gate().dir(), Some(disk.as_path()), "the table is not in the pool directory");
    assert!(flags(&root).unwrap().is_empty() && flags(&disk).unwrap().is_empty());
    let sealed = checkpoint(&db, "pooled").await;
    assert!(matches!(sealed[0].outcome, SealOutcome::Published), "{:?}", sealed[0].outcome);
    assert!(disk.join("pooled.ixm").exists());
    assert_eq!(flags(&disk).unwrap(), vec![INDEX_STORE_V1.to_string()], "the pool directory");
    assert_eq!(flags(&root).unwrap(), vec![INDEX_STORE_V1.to_string()], "the root, which check reads");
    drop(t);
    close(&root, db);
    crate::core::wal::remove_for(&disk.to_string_lossy());
}

/// MOVE carries the pair and the word to the destination directory, like
/// RENAME: the moved table shares the word, its log is the old one's, the
/// source directory keeps neither file, and it seals where it now lives.
#[cfg(feature = "storage_pools")]
#[tokio::test(flavor = "multi_thread")]
async fn move_carries_the_pair_and_the_word_to_the_destination() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    let disk = base.path().join("disk");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&disk).unwrap();
    let db = open(&root, "seal_move").await;
    db.storage
        .add_location(&disk.to_string_lossy(), crate::core::storage::LocationRole::Primary, 1, true)
        .unwrap();
    let t = seeded(&db, "mv", 15).await;
    let incarnation = t.schema.incarnation;
    checkpoint(&db, "mv").await;
    let clean = proven(&root, "mv");
    let word = t.gate().word().clone();
    drop(t);
    exec(&db, &format!("STORAGE MOVE TABLE mv TO '{}'", disk.to_string_lossy())).await;
    let moved = table(&db, "mv");
    ready(&moved).await;
    assert_eq!(moved.gate().dir(), Some(disk.as_path()));
    assert!(StdArc::ptr_eq(moved.gate().word(), &word), "the moved table opened with a fresh word");
    assert!(!root.join("mv.ixm").exists() && !root.join("mv.ixs").exists(), "the source kept its pair");
    let log = scan_of(&disk.join("mv.ixm")).expect("the marker log was carried");
    assert_eq!(log.incarnation, incarnation);
    assert!(log.records.iter().any(|r| r.record == MarkerRecord::Clean(clean)));
    assert_ne!(moved.gate().state(), GateState::Clean, "MOVE's ACCESS EXCLUSIVE ticket made it DIRTY");
    let sealed = checkpoint(&db, "mv").await;
    assert!(matches!(sealed[0].outcome, SealOutcome::Published), "{:?}", sealed[0].outcome);
    proven(&disk, "mv");
    assert_eq!(count(&db, "mv").await, 15);
    drop(moved);
    close(&root, db);
    crate::core::wal::remove_for(&disk.to_string_lossy());
}

/// N26 is a database's own: a commit revoked in another database's status
/// log (in a test run, another test's) leaves this database's seals alone,
/// and one revoked in this database's own log after the table opened still
/// refuses its seal (`Skip::Revoked`).
///
/// Mutations: the seal comparing the process-wide count again (the first
/// checkpoint skips); `StatusLog::record` not recording its latest
/// revocation (the second publishes).
#[tokio::test(flavor = "multi_thread")]
async fn a_revocation_refuses_only_its_own_databases_seals() {
    use crate::core::mvcc::TxOutcome;
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "seal_revoked").await;
    let t = seeded(&db, "r", 30).await;

    // Another database's log revokes a commit of its own.
    let other = crate::core::mvcc::status::StatusLog::in_memory();
    other.record(5, TxOutcome::Committed);
    other.record(5, TxOutcome::Aborted);
    for s in checkpoint(&db, "r").await {
        assert!(s.outcome.is_clean(), "another database's revocation refused {}: {}", s.name, s.outcome.describe());
    }

    // Dirty again, then this database revokes one of its own commits. A
    // transaction held open keeps the log from settling past it.
    exec(&db, "INSERT INTO r (id, v, s) VALUES (1000, 1, 'late')").await;
    ready(&t).await;
    let held = db.transactions.begin();
    let tx = db.transactions.begin();
    db.transactions.commit(tx);
    db.transactions.status().record(tx, TxOutcome::Aborted);
    let sealed = checkpoint(&db, "r").await;
    assert!(
        sealed.iter().any(|s| matches!(s.outcome, SealOutcome::Skipped(Skip::Revoked))),
        "a revocation in the table's own database did not refuse its seal: {:?}",
        sealed.iter().map(|s| s.outcome.describe()).collect::<Vec<_>>()
    );
    db.transactions.abort(held);
    close(dir.path(), db);
}
