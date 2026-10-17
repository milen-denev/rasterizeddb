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
    let fields = vec![SchemaField::new("x".to_string(), DbType::I64, 8, 0, 0, false)];
    let f = TableFilters::new(&fields, FilterConfig::default());
    f.backfill_batch(&rows(n));
    f.clear_unindexed_prefix();
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
    let (f, fields) = x_filter(100);
    assert!(f.is_complete());
    let before = f.renumber_epoch();

    f.begin_renumbering(100);
    assert!(!f.is_complete(), "the filter still vouches for slots being renumbered");
    assert_eq!(f.resume_slot(), 0);
    let during = f.renumber_epoch();
    assert!(!f.backfill_batch_at(before, &rows(10)), "a stale backfill batch was applied");
    assert!(!f.clear_unindexed_prefix_at(before), "a stale backfill cleared the prefix");
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

    f.end_renumbering();
    assert!(!f.clear_unindexed_prefix_at(during), "work that overlapped the renumbering cleared the prefix");
    let after = f.renumber_epoch();
    assert!(f.backfill_batch_at(after, &rows(100)));
    assert!(f.clear_unindexed_prefix_at(after));
    assert!(f.is_complete());
}

#[test]
fn an_index_build_from_before_a_renumbering_cannot_mark_it_ready() {
    let ix = crate::core::index::TableIndexes::new(Vec::new());
    let before = ix.epoch();
    ix.set_ready(true);
    let during = ix.invalidate();
    assert!(!ix.is_ready(), "invalidate left the indexes ready");
    assert!(!ix.set_ready_at(before), "a build from before the renumbering marked ready");
    let after = ix.invalidate();
    assert!(!ix.set_ready_at(during), "a build that overlapped the renumbering marked ready");
    assert!(!ix.is_ready());
    assert!(ix.set_ready_at(after));
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
