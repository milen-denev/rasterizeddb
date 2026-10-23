//! A `VACUUM FULL` that fails part-way through renumbering slots must not leave
//! the filter or the secondary indexes answering by the old slot numbers.
//!
//! `Table::compact` packs pointer records towards slot 0 in committed batches.
//! When a later batch fails, the batches before it stay done: rows sit at new,
//! lower slots. Every in-memory structure keyed by slot (the filter planes and
//! the secondary indexes) still describes the old numbering, and the filter's
//! checkpoint could write that numbering to disk as clean. Each case compares a
//! filtered or indexed query against a whole-table scan (no predicate, so no
//! filter and no index) of the same table: the twin.
//!
//! The failure is injected with `compact_test_hooks::FAIL_PHASE2_AFTER_SLOT_MOVES`,
//! which commits the moves made so far and then fails, as ENOSPC on the next
//! batch would.

use std::path::Path;
use std::time::{Duration, Instant};

use rclite::Arc;

use crate::core::database::Database;
use crate::core::index_store::gate::{Proof, test_mint};
use crate::core::row::table::compact_test_hooks::FAIL_PHASE2_AFTER_SLOT_MOVES;

/// Rows per table. Over the filter's default `min_table_rows` (4096), so a
/// filter loaded from its checkpoint (which uses the default config) is
/// consulted after a reopen too.
const ROWS: i32 = 4400;
/// Rows deleted at the front, so phase 2 has slots to close.
const DELETED: i32 = 100;
/// Pointer records moved before the injected failure: ids `DELETED..DELETED+MOVED`
/// end up at slots `0..MOVED`.
const MOVED: u64 = 50;

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

fn label(id: i32) -> String {
    format!("L{id:05}")
}

async fn open(root: &Path) -> Arc<Database> {
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::new_named(&path_str(root), false, "compactfail").await)
}

async fn run(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql)
        .unwrap_or_else(|| panic!("could not parse {sql}"));
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    assert!(
        !out.is_empty() && out[0] != 3,
        "{sql} failed: {}",
        String::from_utf8_lossy(&out)
    );
    out
}

/// Every `L#####` label in a result, sorted, duplicates kept.
fn labels_in(out: &[u8]) -> Vec<String> {
    let mut found = Vec::new();
    let mut i = 0;
    while i + 6 <= out.len() {
        if out[i] == b'L'
            && out[i + 1..i + 6].iter().all(u8::is_ascii_digit)
            && out.get(i + 6).is_none_or(|b| !b.is_ascii_digit())
        {
            found.push(String::from_utf8_lossy(&out[i..i + 6]).into_owned());
            i += 6;
        } else {
            i += 1;
        }
    }
    found.sort();
    found
}

async fn create_and_fill(db: &Arc<Database>, table: &str, with_index: bool) {
    run(db, &format!("CREATE TABLE {table} (id INTEGER, k INTEGER, label TEXT)")).await;
    if with_index {
        run(db, &format!("CREATE INDEX {table}_k ON {table} (k)")).await;
    }
    let mut id = 0;
    while id < ROWS {
        let end = (id + 200).min(ROWS);
        let values: Vec<String> = (id..end)
            .map(|i| format!("({i}, {}, '{}')", i * 10, label(i)))
            .collect();
        run(
            db,
            &format!("INSERT INTO {table} (id, k, label) VALUES {}", values.join(", ")),
        )
        .await;
        id = end;
    }
    run(db, &format!("DELETE FROM {table} WHERE id < {DELETED}")).await;
}

async fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Fails a compaction of `table` after `MOVED` slot moves.
async fn fail_compaction(db: &Arc<Database>, table: &str) {
    let t = db.tables.get(table).unwrap().clone();
    FAIL_PHASE2_AFTER_SLOT_MOVES.with(|h| h.set(Some(MOVED)));
    let outcome = t.compact().await;
    FAIL_PHASE2_AFTER_SLOT_MOVES.with(|h| h.set(None));
    assert!(outcome.is_err(), "the injected phase-2 failure did not fail the compaction");
}

/// The rows a predicate query returns, against what the whole-table scan says
/// the same predicate selects. `ids` are the rows the predicate matches.
async fn assert_matches_twin(db: &Arc<Database>, table: &str, predicate: &str, ids: &[i32], when: &str) {
    let all = labels_in(&run(db, &format!("SELECT label FROM {table}")).await);
    let wanted: Vec<String> = ids.iter().map(|&i| label(i)).collect();
    let mut twin: Vec<String> = all.iter().filter(|l| wanted.contains(l)).cloned().collect();
    twin.sort();
    // The scan itself must hold each matching row exactly once, or the twin
    // proves nothing about the predicate query.
    let mut expect = wanted.clone();
    expect.sort();
    assert_eq!(twin, expect, "{when}: the whole-table scan lost or duplicated rows");
    let got = labels_in(&run(db, &format!("SELECT label FROM {table} WHERE {predicate}")).await);
    assert_eq!(
        got, twin,
        "{when}: `WHERE {predicate}` disagrees with the whole-table scan"
    );
}

/// Predicates over rows the failed compaction moved, plus one it did not.
async fn assert_all_match(db: &Arc<Database>, table: &str, when: &str) {
    for id in [DELETED, DELETED + 7, DELETED + 20, DELETED + MOVED as i32 - 1, 3000] {
        assert_matches_twin(db, table, &format!("k = {}", id * 10), &[id], when).await;
        assert_matches_twin(db, table, &format!("label = '{}'", label(id)), &[id], when).await;
    }
}

/// (a) Right after the failure, in the same process: neither the index on `k`
/// nor the filter may prune by the old numbering.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_compaction_leaves_queries_answering_by_the_new_slots() {
    let base = tempfile::tempdir().unwrap();
    let db = open(base.path()).await;
    create_and_fill(&db, "cf_a", true).await;
    let t = db.tables.get("cf_a").unwrap().clone();
    let ix = t.indexes.clone().expect("the CREATE INDEX gave the table an index");
    wait_until("the index build", || ix.is_ready()).await;
    t.wait_filters_ready().await;
    drop(t);

    assert_all_match(&db, "cf_a", "before the compaction").await;
    fail_compaction(&db, "cf_a").await;
    assert_all_match(&db, "cf_a", "after a compaction failed in phase 2").await;

    drop(db);
    crate::core::wal::remove_for(&path_str(base.path()));
}

/// (d) The failed compaction is followed by a write and a filter checkpoint,
/// as the maintenance task would run one, and the database is reopened. The
/// sidecar must not come back trusted with the old numbering.
#[tokio::test(flavor = "multi_thread")]
async fn a_checkpoint_after_a_failed_compaction_does_not_persist_the_old_numbering() {
    let base = tempfile::tempdir().unwrap();
    let db = open(base.path()).await;
    create_and_fill(&db, "cf_d", false).await;
    let t = db.tables.get("cf_d").unwrap().clone();
    t.wait_filters_ready().await;
    // A checkpoint exists, so the table is in the state a long-running one is.
    t.checkpoint_filters().await;
    drop(t);

    fail_compaction(&db, "cf_d").await;
    run(&db, &format!("DELETE FROM cf_d WHERE id = {}", ROWS - 1)).await;
    let t = db.tables.get("cf_d").unwrap().clone();
    t.checkpoint_filters().await;
    drop(t);
    assert_all_match(&db, "cf_d", "after the failure and a checkpoint").await;

    drop(db);
    crate::core::wal::remove_for(&path_str(base.path()));
    let db = open(base.path()).await;
    assert_all_match(&db, "cf_d", "after reopening").await;
    // And once the open-time backfill has finished with the table.
    let t = db.tables.get("cf_d").unwrap().clone();
    t.wait_filters_ready().await;
    drop(t);
    assert_all_match(&db, "cf_d", "after reopening and the backfill").await;

    drop(db);
    crate::core::wal::remove_for(&path_str(base.path()));
}

// ---------------------------------------------------------------------------
// The epoch fences, directly. A build or backfill that started before a
// compaction (they do not take its gate) must not publish what it read once
// slots have started or stopped moving.
// ---------------------------------------------------------------------------

fn x_filter(n: u64) -> (crate::core::filter::TableFilters, Vec<crate::core::row::schema::SchemaField>) {
    use crate::core::db_type::DbType;
    use crate::core::filter::{FilterConfig, TableFilters};
    use crate::core::row::schema::SchemaField;
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    let fields = vec![SchemaField::new("x".to_string(), DbType::I64, 8, 0, 0, false)];
    let f = TableFilters::new(&fields, FilterConfig::default());
    f.backfill_batch(proof, &rows(n));
    f.clear_unindexed_prefix(proof);
    (f, fields)
}

fn rows(n: u64) -> Vec<(u64, smallvec::SmallVec<[(u32, Vec<u8>); 32]>)> {
    (0..n)
        .map(|slot| {
            let mut c: smallvec::SmallVec<[(u32, Vec<u8>); 32]> = smallvec::SmallVec::new();
            c.push((0, (slot as i64).to_le_bytes().to_vec()));
            (slot, c)
        })
        .collect()
}

#[test]
fn a_renumbering_fences_off_work_that_read_slots_before_or_during_it() {
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    let (f, fields) = x_filter(100);
    assert!(f.is_complete());
    let before = f.renumber_epoch();

    f.begin_renumbering(proof, 100);
    assert!(!f.is_complete(), "the filter still vouches for slots being renumbered");
    assert_eq!(f.resume_slot(), 0);
    let during = f.renumber_epoch();
    assert!(!f.backfill_batch_at(proof, before, &rows(10)), "a stale backfill batch was applied");
    assert!(!f.clear_unindexed_prefix_at(proof, before), "a stale backfill cleared the prefix");
    assert!(!f.on_delete_runs_at(proof, before, &[(3, 2)]), "a stale vacuum clear was applied");
    assert!(!f.is_complete());

    // A checkpoint taken now does not vouch for the renumbered slots.
    let bytes = f.checkpoint_bytes(100);
    let loaded = crate::core::filter::TableFilters::from_checkpoint(
        &bytes,
        &fields,
        crate::core::filter::FilterConfig::default(),
        100,
    )
    .expect("the checkpoint loads");
    assert!(!loaded.is_complete(), "a checkpoint of a demoted filter came back trusted");
    assert_eq!(loaded.resume_slot(), 0, "a reopened table would not backfill the demoted prefix");

    f.end_renumbering(proof);
    assert!(!f.clear_unindexed_prefix_at(proof, during), "work that overlapped the renumbering cleared the prefix");
    let after = f.renumber_epoch();
    assert!(f.backfill_batch_at(proof, after, &rows(100)));
    assert!(f.clear_unindexed_prefix_at(proof, after));
    assert!(f.is_complete());
}

#[test]
fn an_index_build_from_before_a_renumbering_cannot_mark_it_ready() {
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    let ix = crate::core::index::TableIndexes::new(Vec::new());
    let before = ix.epoch();
    ix.set_ready(proof, true);
    let during = ix.invalidate(proof);
    assert!(!ix.is_ready(), "invalidate left the indexes ready");
    assert!(!ix.set_ready_at(proof, before), "a build from before the renumbering marked ready");
    let after = ix.invalidate(proof);
    assert!(!ix.set_ready_at(proof, during), "a build that overlapped the renumbering marked ready");
    assert!(!ix.is_ready());
    assert!(ix.set_ready_at(proof, after));
    assert!(ix.is_ready());
    assert_eq!(ix.epoch(), after, "marking ready changed the epoch");
}

// ---------------------------------------------------------------------------
// Work already in flight when a compaction starts. Neither a filter checkpoint
// nor a vacuum sweep waits for the compaction to begin before reading slots.
// ---------------------------------------------------------------------------

/// Lets a parked task go once `compact` has finished, or once it has been
/// blocked for a while waiting on `stalled` (a fix that makes it wait for the
/// parked task). Without a fix the compaction runs straight through and the
/// parked task resumes only after it, which is the interleaving under test.
fn release_after_compaction(
    release: impl FnOnce() + Send + 'static,
    done: std::sync::Arc<std::sync::atomic::AtomicBool>,
    stalled: impl Fn() -> bool + Send + 'static,
) -> tokio::task::JoinHandle<()> {
    use std::sync::atomic::Ordering;
    tokio::spawn(async move {
        let mut stalled_since: Option<Instant> = None;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if done.load(Ordering::Acquire) {
                break;
            }
            if stalled() {
                let since = *stalled_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= Duration::from_millis(300) {
                    break;
                }
            } else {
                stalled_since = None;
            }
            assert!(Instant::now() < deadline, "the compaction neither finished nor stalled");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        release();
    })
}

/// Finding: a checkpoint planned before the compaction began (a base image of
/// the old numbering, `unknown_below = 0`) lands after it began and marks the
/// sidecar clean, erasing the compaction's dirty mark. The compaction then
/// fails in phase 2 and the database is reopened with no further checkpoint.
#[tokio::test(flavor = "multi_thread")]
async fn a_checkpoint_in_flight_as_a_compaction_starts_does_not_persist_the_old_numbering() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let base = tempfile::tempdir().unwrap();
    let db = open(base.path()).await;
    create_and_fill(&db, "cf_e", false).await;
    let t = db.tables.get("cf_e").unwrap().clone();
    t.wait_filters_ready().await;
    t.checkpoint_filters().await;
    // Marks the sidecar dirty, so the next checkpoint plans a base image.
    run(&db, &format!("DELETE FROM cf_e WHERE id = {}", ROWS - 1)).await;

    let filters = t.filters.clone().expect("the table has a filter");
    let park = crate::core::filter::checkpoint_test_hooks::arm(&filters);
    let cp_table = t.clone();
    let checkpoint = tokio::spawn(async move { cp_table.checkpoint_filters().await });
    tokio::time::timeout(Duration::from_secs(30), park.planned.notified())
        .await
        .expect("the checkpoint never planned a write");

    let done = std::sync::Arc::new(AtomicBool::new(false));
    // Without a fix nothing waits on the parked checkpoint, so only the
    // compaction finishing releases it; with one the compaction waits for it.
    let releaser = {
        let park = park.clone();
        release_after_compaction(move || park.release.notify_one(), done.clone(), || true)
    };
    drop(t);
    drop(filters);
    fail_compaction(&db, "cf_e").await;
    done.store(true, Ordering::Release);
    releaser.await.unwrap();
    checkpoint.await.unwrap();

    drop(db);
    crate::core::wal::remove_for(&path_str(base.path()));
    let db = open(base.path()).await;
    assert_all_match(&db, "cf_e", "after reopening").await;
    let t = db.tables.get("cf_e").unwrap().clone();
    t.wait_filters_ready().await;
    drop(t);
    assert_all_match(&db, "cf_e", "after reopening and the backfill").await;

    drop(db);
    crate::core::wal::remove_for(&path_str(base.path()));
}

/// Runs a `vacuum_now` of `table` that parks at `point`, a full compaction
/// while it is parked, and then lets the sweep finish.
async fn vacuum_overlapping_compaction(db: &Arc<Database>, table: &str, point: u8) {
    use crate::core::row::table::compact_test_hooks::GATE_PROBE;
    use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
    let t = db.tables.get(table).unwrap().clone();
    let park = crate::core::vacuum::run_once_test_hooks::arm(&t.free_space, point);
    let vt = t.clone();
    let sweep = tokio::spawn(async move { vt.vacuum_now().await.map(|r| r.dead_rows) });
    tokio::time::timeout(Duration::from_secs(30), park.parked.notified())
        .await
        .expect("the vacuum sweep never reached its park point");

    let probe = std::sync::Arc::new(AtomicU8::new(0));
    let done = std::sync::Arc::new(AtomicBool::new(false));
    let releaser = {
        let park = park.clone();
        let probe = probe.clone();
        release_after_compaction(move || park.release.notify_one(), done.clone(), move || {
            probe.load(Ordering::Acquire) == 1
        })
    };
    GATE_PROBE.with(|p| *p.borrow_mut() = Some(probe.clone()));
    let outcome = t.compact().await;
    GATE_PROBE.with(|p| *p.borrow_mut() = None);
    done.store(true, Ordering::Release);
    outcome.expect("the compaction failed");
    releaser.await.unwrap();
    let dead = sweep.await.unwrap().expect("the vacuum sweep failed");
    assert!(dead >= DELETED as u64, "the parked sweep found {dead} reclaimable rows, not the deleted ones");
}

/// Finding: a sweep that scanned before a compaction forgets index entries by
/// its old slot numbers and row positions after the compaction rebuilt the
/// index, removing the entries of the live rows that now sit there.
#[tokio::test(flavor = "multi_thread")]
async fn a_vacuum_sweep_overlapping_a_compaction_leaves_the_index_whole() {
    let base = tempfile::tempdir().unwrap();
    let db = open(base.path()).await;
    create_and_fill(&db, "cf_v", true).await;
    let t = db.tables.get("cf_v").unwrap().clone();
    let ix = t.indexes.clone().expect("the CREATE INDEX gave the table an index");
    wait_until("the index build", || ix.is_ready()).await;
    t.wait_filters_ready().await;
    drop(t);
    assert_all_match(&db, "cf_v", "before the compaction").await;

    vacuum_overlapping_compaction(&db, "cf_v", crate::core::vacuum::run_once_test_hooks::AFTER_SCAN).await;
    wait_until("the index rebuild", || ix.is_ready()).await;
    assert_all_match(&db, "cf_v", "after a sweep overlapped the compaction").await;

    drop(ix);
    drop(db);
    crate::core::wal::remove_for(&path_str(base.path()));
}

/// Finding: the same sweep, parked after its scan but before it publishes,
/// hands writers extents of the old row layout, which after the compaction
/// hold live rows.
#[tokio::test(flavor = "multi_thread")]
async fn a_vacuum_sweep_overlapping_a_compaction_publishes_no_old_extents() {
    let base = tempfile::tempdir().unwrap();
    let db = open(base.path()).await;
    create_and_fill(&db, "cf_p", false).await;
    let t = db.tables.get("cf_p").unwrap().clone();
    t.wait_filters_ready().await;
    drop(t);

    vacuum_overlapping_compaction(&db, "cf_p", crate::core::vacuum::run_once_test_hooks::BEFORE_PUBLISH).await;
    // Rows the same size as the moved ones, which a stale extent would take.
    let added: Vec<i32> = (5000..5000 + DELETED).collect();
    let values: Vec<String> = added
        .iter()
        .map(|&i| format!("({i}, {}, '{}')", i * 10, label(i)))
        .collect();
    run(&db, &format!("INSERT INTO cf_p (id, k, label) VALUES {}", values.join(", "))).await;

    let got = labels_in(&run(&db, "SELECT label FROM cf_p").await);
    let mut want: Vec<String> = (DELETED..ROWS).chain(added.iter().copied()).map(label).collect();
    want.sort();
    assert_eq!(got.len(), want.len(), "the table lost or gained rows");
    assert_eq!(got, want, "an insert overwrote a live row moved by the compaction");

    drop(db);
    crate::core::wal::remove_for(&path_str(base.path()));
}

/// Vacuum's deferred clears yield each column's lock every `CLEAR_CHUNK`
/// slots. Runs that straddle those boundaries must clear exactly the slots a
/// slot-at-a-time clear would, no more and no fewer.
#[test]
fn chunked_run_clears_match_clearing_slot_by_slot() {
    use crate::core::db_type::DbType;
    use crate::core::filter::{FilterConfig, TableFilters};
    use crate::core::row::schema::SchemaField;
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    const N: u64 = 10_000;
    // A text column: fingerprinted, so a cleared slot changes its plane.
    let fields = vec![SchemaField::new("s".to_string(), DbType::STRING, 12, 0, 0, false)];
    let f = TableFilters::new(&fields, FilterConfig::default());
    let text_rows: Vec<(u64, smallvec::SmallVec<[(u32, Vec<u8>); 32]>)> = (0..N)
        .map(|slot| {
            let mut c: smallvec::SmallVec<[(u32, Vec<u8>); 32]> = smallvec::SmallVec::new();
            c.push((0, format!("v{slot:08}").into_bytes()));
            (slot, c)
        })
        .collect();
    f.backfill_batch(proof, &text_rows);
    f.clear_unindexed_prefix(proof);
    let image = f.checkpoint_bytes(N);
    let load = || TableFilters::from_checkpoint(&image, &fields, FilterConfig::default(), N).unwrap();
    let (one_by_one, in_runs, one_short) = (load(), load(), load());
    // The boundary counts slots cleared, not slot numbers: single slots, a
    // run the first boundary falls inside (slot 4104, the 4096th cleared), a
    // run that ends exactly on the second (8192 cleared), and runs after it.
    let runs: Vec<(u64, u64)> = vec![(0, 1), (2, 1), (10, 4100), (5000, 4090), (9095, 1), (9100, 50)];
    assert!(runs.iter().map(|r| r.1).sum::<u64>() > 2 * 4096, "the runs no longer cross two boundaries");
    for &(first, count) in &runs {
        for slot in first..first + count {
            one_by_one.on_delete(proof, slot);
            if slot != 4104 {
                one_short.on_delete(proof, slot);
            }
        }
    }
    assert!(
        one_by_one.checkpoint_bytes(N) != one_short.checkpoint_bytes(N),
        "one slot's clear does not show in the checkpoint, so the comparison proves nothing"
    );
    assert!(in_runs.on_delete_runs_at(proof, in_runs.renumber_epoch(), &runs));
    assert!(
        one_by_one.checkpoint_bytes(N) == in_runs.checkpoint_bytes(N),
        "clearing in chunked runs cleared different slots than clearing one at a time"
    );
}

// ---------------------------------------------------------------------------
// Follow-ups to the review of 595acf5 and to finding (h).
// ---------------------------------------------------------------------------

/// Finding (h): a sweep that cannot publish, because an older query is still
/// running, quarantines what it found. A compaction then packs live rows into
/// exactly those bytes. A later sweep publishes every quarantined batch whose
/// barrier has passed, and an insert given one of those extents overwrites a
/// moved row.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_quarantined_before_a_compaction_is_never_published() {
    use crate::core::vacuum::{ReclaimContext, VacuumConfig};
    let base = tempfile::tempdir().unwrap();
    let db = open(base.path()).await;
    create_and_fill(&db, "cf_q", false).await;
    let t = db.tables.get("cf_q").unwrap().clone();
    t.wait_filters_ready().await;

    // Stands in for a query that started before the sweep and is still running.
    let ticket = t.readers.enter();
    let config = VacuumConfig {
        drain_timeout: Duration::from_millis(50),
        ..Default::default()
    };
    let horizon = t.version_horizon();
    let cleared_below = std::sync::atomic::AtomicU64::new(0);
    let ctx = ReclaimContext {
        horizon: &horizon,
        filters: None,
        indexes: None,
        cleared_below: &cleared_below,
        compaction_gate: None,
    };
    let report = crate::core::vacuum::run_once(
        &*t.io_pointers,
        &*t.io_rows,
        &t.free_space,
        &t.readers,
        &ctx,
        &config,
        true,
    )
    .await
    .unwrap();
    assert!(report.deferred, "the sweep published while an older read was live");
    assert!(
        t.free_space.quarantined_extents() > 0,
        "nothing was quarantined, so the case tests nothing: {report:?}"
    );
    drop(ticket);

    let compacted = t.compact().await.expect("the compaction failed");
    assert!(compacted.rows_moved > 0, "the compaction moved no rows: {compacted:?}");
    // Nothing older is running now, so this publishes whatever is quarantined.
    t.vacuum_now().await.unwrap();
    drop(t);

    // Rows the same size as the deleted ones, which a stale extent would take.
    let added: Vec<i32> = (5000..5000 + DELETED).collect();
    let values: Vec<String> = added
        .iter()
        .map(|&i| format!("({i}, {}, '{}')", i * 10, label(i)))
        .collect();
    run(&db, &format!("INSERT INTO cf_q (id, k, label) VALUES {}", values.join(", "))).await;

    let got = labels_in(&run(&db, "SELECT label FROM cf_q").await);
    let mut want: Vec<String> = (DELETED..ROWS).chain(added.iter().copied()).map(label).collect();
    want.sort();
    assert_eq!(got.len(), want.len(), "the table lost or gained rows");
    assert_eq!(got, want, "an insert overwrote a live row moved by the compaction");

    drop(db);
    crate::core::wal::remove_for(&path_str(base.path()));
}

/// Review S3: query-cache entries hold row pointers. A compaction that fails
/// once rows have moved durably, in phase 1 (row bytes) or phase 2 (slots),
/// leaves an entry from before it naming bytes another row may now occupy.
/// The cache serves an entry only while the table's cache generation is the
/// one it was stored under (see `query_cache::lookup`), so the failed
/// compaction must have moved it on. The write generation too: the pointer
/// re-read `DELETE` and `UPDATE` fall back on, and the vacuum service's
/// "nothing changed" test, both read it.
///
/// The generations are asserted rather than a cached `SELECT`: these tests
/// run with the query cache switched off process-wide (`open` sets
/// `ENABLE_CACHE` once), and `lookup` is a plain comparison against them.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_compaction_retires_cached_row_pointers() {
    use crate::core::row::table::compact_test_hooks::FAIL_PHASE1_AFTER_ROW_MOVES;
    let base = tempfile::tempdir().unwrap();
    let db = open(base.path()).await;
    create_and_fill(&db, "cf_c", false).await;
    let t = db.tables.get("cf_c").unwrap().clone();
    t.wait_filters_ready().await;
    drop(t);

    for phase in [1, 2] {
        let t = db.tables.get("cf_c").unwrap().clone();
        let (cache_before, writes_before) = (t.cache_generation(), t.write_generation());
        if phase == 1 {
            FAIL_PHASE1_AFTER_ROW_MOVES.with(|h| h.set(Some(MOVED)));
            let outcome = t.compact().await;
            FAIL_PHASE1_AFTER_ROW_MOVES.with(|h| h.set(None));
            assert!(outcome.is_err(), "the injected phase-1 failure did not fail the compaction");
        } else {
            fail_compaction(&db, "cf_c").await;
        }
        assert_ne!(
            t.cache_generation(),
            cache_before,
            "a cached answer from before a compaction that failed in phase {phase} would still be served"
        );
        assert_ne!(
            t.write_generation(),
            writes_before,
            "a compaction that failed in phase {phase} left the write generation where it was"
        );
        drop(t);
        assert_all_match(&db, "cf_c", &format!("after a compaction failed in phase {phase}")).await;
    }
    drop(db);
    crate::core::wal::remove_for(&path_str(base.path()));
}

/// Review follow-up: the renumber epoch is odd exactly while a renumbering
/// runs, however begins and ends are paired. A compaction dropped before its
/// renumbering began still ends one (its guard is built first), and one that
/// began while another had not ended must still read as running.
#[test]
fn the_renumber_epoch_is_odd_exactly_while_renumbering() {
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    use crate::core::filter::renumbering;
    let (f, _) = x_filter(100);
    let mut seen = vec![f.renumber_epoch()];
    assert!(!renumbering(seen[0]), "a fresh filter reads as renumbering");
    for cycle in 0..2 {
        f.begin_renumbering(proof, 100);
        assert!(renumbering(f.renumber_epoch()), "cycle {cycle}: not renumbering after a begin");
        seen.push(f.renumber_epoch());
        f.end_renumbering(proof);
        assert!(!renumbering(f.renumber_epoch()), "cycle {cycle}: still renumbering after an end");
        seen.push(f.renumber_epoch());
    }
    f.end_renumbering(proof);
    assert!(!renumbering(f.renumber_epoch()), "an end with no begin left the epoch renumbering");
    seen.push(f.renumber_epoch());
    f.begin_renumbering(proof, 100);
    seen.push(f.renumber_epoch());
    f.begin_renumbering(proof, 100);
    assert!(renumbering(f.renumber_epoch()), "a begin during a renumbering ended it");
    seen.push(f.renumber_epoch());
    f.end_renumbering(proof);
    assert!(!renumbering(f.renumber_epoch()));
    seen.push(f.renumber_epoch());
    assert!(
        seen.windows(2).all(|w| w[1] > w[0]),
        "a begin or an end left the epoch unchanged, so work across it is not fenced: {seen:?}"
    );
}

/// Review follow-up: a compaction future dropped while it waits for the
/// sidecar's dirty mark, after its renumbering began, must not leave the
/// epoch reading "renumbering" for good. Every backfill and prefix clear
/// would be refused, and the filter would stay demoted until a restart.
#[tokio::test(flavor = "multi_thread")]
async fn a_compaction_dropped_as_its_renumbering_begins_ends_it() {
    use crate::core::filter::renumbering;
    let base = tempfile::tempdir().unwrap();
    let db = open(base.path()).await;
    create_and_fill(&db, "cf_x", false).await;
    let t = db.tables.get("cf_x").unwrap().clone();
    t.wait_filters_ready().await;
    // A checkpoint exists, so the renumbering writes the dirty mark.
    t.checkpoint_filters().await;
    let filters = t.filters.clone().expect("the table has a filter");

    let marking = filters.dirty_marking.lock().await;
    let mut compaction = Box::pin(t.compact());
    let deadline = Instant::now() + Duration::from_secs(30);
    while !renumbering(filters.renumber_epoch()) {
        assert!(Instant::now() < deadline, "the compaction never began its renumbering");
        tokio::select! {
            biased;
            outcome = &mut compaction => panic!(
                "the compaction ran past the dirty mark it could not take: {:?}",
                outcome.map(|_| ())
            ),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
    // A cancelled statement drops its future here.
    drop(compaction);
    drop(marking);
    assert!(
        !renumbering(filters.renumber_epoch()),
        "a dropped compaction left the epoch renumbering, refusing every backfill"
    );
    t.backfill_filters_from(0).await.expect("the backfill was refused");
    assert!(filters.is_complete(), "the backfill could not restore the filter");
    drop(filters);
    drop(t);
    assert_all_match(&db, "cf_x", "after a compaction was dropped").await;

    drop(db);
    crate::core::wal::remove_for(&path_str(base.path()));
}

/// Review S4: work that read the renumber epoch *during* a renumbering, and
/// writes before it ends, saw an unchanged epoch and was let through. Its
/// prefix clear then outlives a compaction that fails part-way.
#[test]
fn work_that_read_the_epoch_during_a_renumbering_cannot_publish() {
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    let (f, _) = x_filter(100);
    f.begin_renumbering(proof, 100);
    let during = f.renumber_epoch();
    assert!(!f.backfill_batch_at(proof, during, &rows(10)), "a batch read mid-renumbering was applied");
    assert!(!f.on_delete_runs_at(proof, during, &[(3, 2)]), "a clear read mid-renumbering was applied");
    assert!(!f.clear_unindexed_prefix_at(proof, during), "a backfill mid-renumbering cleared the prefix");
    assert!(!f.is_complete(), "the demotion was cleared while slots were still moving");
    f.end_renumbering(proof);
    let after = f.renumber_epoch();
    assert_ne!(after, during);
    assert!(f.backfill_batch_at(proof, after, &rows(100)));
    assert!(f.clear_unindexed_prefix_at(proof, after));
    assert!(f.is_complete());
}

/// Review S4, through the table: the open-time backfill does not take the
/// compaction gate, so it can start after a compaction began and finish
/// while that compaction is still moving slots. Its prefix clear must not
/// land; if it did, a compaction failing afterwards leaves the filter
/// trusting slots that hold moved rows it never indexed.
#[tokio::test(flavor = "multi_thread")]
async fn a_backfill_that_starts_during_a_renumbering_leaves_the_filter_demoted() {
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
    use crate::core::storage_providers::traits::StorageIO;
    let base = tempfile::tempdir().unwrap();
    let db = open(base.path()).await;
    create_and_fill(&db, "cf_b", false).await;
    let t = db.tables.get("cf_b").unwrap().clone();
    t.wait_filters_ready().await;
    let filters = t.filters.clone().expect("the table has a filter");
    let n_slots = t.io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64;

    // What `compact` does before its first move; it is still running.
    filters.begin_renumbering(proof, n_slots);
    assert!(!filters.is_complete());
    let _ = t.backfill_filters_from(0).await;
    assert!(
        !filters.is_complete(),
        "a backfill that read slots while they moved cleared the renumbering's demotion"
    );
    filters.end_renumbering(proof);

    drop(filters);
    drop(t);
    drop(db);
    crate::core::wal::remove_for(&path_str(base.path()));
}

/// Review S5, directly: a demoted prefix must stop the zone map licensing
/// the `ORDER BY … LIMIT` slot ceiling. The frontier is left where it was.
#[test]
fn a_demoted_column_is_not_reported_monotonic() {
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    let (f, _) = x_filter(2048);
    assert!(f.column_is_monotonic(0, 2048), "the fixture is not monotonic, so this proves nothing");
    f.begin_renumbering(proof, 2048);
    assert!(!f.column_is_monotonic(0, 2048), "a demoted column still licenses the slot ceiling");
}

/// Review S5, through the table. Values ascend across zones and descend
/// inside each one, the case the ceiling rounds up to whole zones for. Zone 0
/// keeps ten rows; the compaction moves 502 records from zone 1 into its
/// slots and then fails. The zone map still says zone 0 holds the smallest
/// values, so `ORDER BY v LIMIT 20` bounded to zone 0 finds 512 live rows
/// there and never looks at the unmoved rows of zone 1, which hold 512..521.
///
/// Through `Table::query_row`, the row path that applies the slot ceiling
/// (the executor's bytes path for a plain `SELECT` does not).
#[tokio::test(flavor = "multi_thread")]
async fn order_by_limit_after_a_failed_compaction_matches_a_full_sort() {
    const ZONE: i32 = crate::core::filter::kernels::ZONE_ROWS as i32;
    const N: i32 = 9 * ZONE;
    const KEPT: i32 = 10;
    let value = |slot: i32| {
        if slot < ZONE {
            slot
        } else {
            (slot / ZONE) * ZONE + (ZONE - 1 - slot % ZONE)
        }
    };
    let base = tempfile::tempdir().unwrap();
    let db = open(base.path()).await;
    run(&db, "CREATE TABLE cf_m (id INTEGER, v INTEGER, label TEXT)").await;
    let mut id = 0;
    while id < N {
        let end = (id + 256).min(N);
        let values: Vec<String> = (id..end)
            .map(|i| format!("({i}, {}, '{}')", value(i), label(value(i))))
            .collect();
        run(&db, &format!("INSERT INTO cf_m (id, v, label) VALUES {}", values.join(", "))).await;
        id = end;
    }
    run(&db, &format!("DELETE FROM cf_m WHERE id >= {KEPT} AND id < {ZONE}")).await;
    let t = db.tables.get("cf_m").unwrap().clone();
    t.wait_filters_ready().await;

    let v = t.schema.fields.iter().find(|f| f.name == "v").unwrap().write_order as u32;
    let all: Vec<u32> = t.schema.fields.iter().map(|f| f.write_order as u32).collect();
    let fetch = crate::core::row::table::Table::<crate::core::storage_providers::backend::DbStorageProvider>::build_fetch_for(
        &t.schema.fields,
        &all,
    );
    let fields = t.schema.fields.to_vec();
    let top_20 = async || -> Vec<i32> {
        let rows = t
            .query_row("", &fields, fetch.clone(), fetch.clone(), Some(20), Some("v".to_string()))
            .await
            .unwrap();
        let mut got: Vec<i32> = rows
            .iter()
            .map(|r| {
                let c = r.columns.iter().find(|c| c.schema_id as u32 == v).unwrap();
                i32::from_le_bytes(c.data.into_slice().try_into().unwrap())
            })
            .collect();
        got.sort();
        got
    };
    let want: Vec<i32> = (0..KEPT).chain(ZONE..ZONE + 20 - KEPT).collect();
    assert_eq!(top_20().await, want, "wrong before the compaction");

    // Otherwise the slot ceiling never applies and the case proves nothing.
    let filters = t.filters.clone().expect("the table has a filter");
    assert!(
        filters.column_is_monotonic(v, N as u64),
        "the zone map does not license the slot ceiling for `v`, so this tests nothing"
    );

    FAIL_PHASE2_AFTER_SLOT_MOVES.with(|h| h.set(Some((ZONE - KEPT) as u64)));
    let outcome = t.compact().await;
    FAIL_PHASE2_AFTER_SLOT_MOVES.with(|h| h.set(None));
    assert!(outcome.is_err(), "the injected phase-2 failure did not fail the compaction");

    assert_eq!(
        top_20().await,
        want,
        "`ORDER BY v LIMIT 20` disagrees with a full sort after a failed compaction"
    );

    drop(filters);
    drop(t);
    drop(db);
    crate::core::wal::remove_for(&path_str(base.path()));
}
