//! A statement's snapshot must be taken before its candidate slots are.
//!
//! A writer appends a version (the pointer file grows), then indexes it — in
//! the filter, then in any secondary index — then commits. A statement that
//! samples the table length after the append but asks the index or the filter
//! before the entry lands leaves the new slot out of its candidates. If it
//! only then takes its snapshot, the snapshot can see the now-committed row
//! the candidates already excluded: the row is visible and silently not
//! matched. Taken first, the snapshot either predates the commit (the row is
//! invisible, so excluding it is right) or follows it, in which case the
//! entry landed before the commit and the lookup names the slot.
//!
//! Two shapes of the window, one per [`Mode`]: a B-tree entry missing on a
//! table with a UNIQUE key, and a filter fingerprint missing on a table with
//! filters and no index at all. Each runs the same interleaving on a twin
//! whose candidates cannot miss the slot (no index; in filter mode no filter
//! either), whose answer is exactly what its snapshot sees, and the two must
//! agree. Both directions are asserted: a commit after the statement's
//! snapshot is not affected or returned (`unseen`), and a commit before it is
//! (`seen`) — a statement affects or returns the row if and only if its
//! snapshot sees it.
//!
//! The interleaving is forced, not raced for: the row is written and its
//! entry withdrawn (the state between the writer's append and its index
//! update), and the failpoint at the end of [`Table::build_candidates`] parks
//! the statement once the index and the filter have been asked, while the
//! writer indexes and commits.
//!
//! The point UPDATE case pins ordering, not a race reachable today: an INSERT
//! holds its key's `unique_lock` stripe from before its append until after
//! its index update, and a point UPDATE takes that same stripe before it
//! snapshots, so a same-key inserter can never sit inside its window. The
//! test reaches that state only by withdrawing the entry behind the stripe's
//! back. It guards the snapshot order in `update_rows_locked`, which the
//! point path shares with DELETE's and the full UPDATE path's exposure, both
//! of which are reachable (they take `write_lock`, which inserts never take).

use std::collections::HashMap;
use std::sync::Arc as StdArc;
use std::sync::{Mutex, OnceLock};

use itertools::Itertools;
use rclite::Arc;
use smallvec::SmallVec;
use tokio::sync::oneshot;

use crate::core::db_type::DbType;
use crate::core::index_store::gate::{Proof, test_mint};
use crate::core::mvcc::TransactionManager;
use crate::core::row::row::{ColumnFetchingData, ColumnWritePayload, RowFetch, RowWrite};
use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
use crate::core::row::schema::{SchemaCalculator, SchemaField};
use crate::core::row::table::Table;
use crate::core::rql::lexer_ct::CreateColumnData;
use crate::core::storage_providers::file_sync::LocalStorageProvider;
use crate::core::storage_providers::traits::StorageIO;
use crate::memory_pool::MEMORY_POOL;

// ---------------------------------------------------------------------------
// Failpoint
// ---------------------------------------------------------------------------

struct Armed {
    arrived: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

fn armed() -> &'static Mutex<HashMap<String, Armed>> {
    static ARMED: OnceLock<Mutex<HashMap<String, Armed>>> = OnceLock::new();
    ARMED.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Called at the end of [`Table::build_candidates`], on every return path,
/// once the index and the filter have been asked. Parks the statement when a
/// test armed it for this table; a no-op otherwise. Keyed by table name, so parallel tests (each with
/// its own table) never see each other's arming. One-shot.
pub(super) async fn candidates_built(table: &str) {
    let hit = armed().lock().unwrap().remove(table);
    if let Some(Armed { arrived, release }) = hit {
        let _ = arrived.send(());
        let _ = release.await;
    }
}

/// Arms the failpoint for `table`: the first receiver fires when a statement
/// reaches it, and the statement stays parked until the sender is used.
fn arm(table: &str) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (arrived_tx, arrived_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    armed().lock().unwrap().insert(
        table.to_string(),
        Armed {
            arrived: arrived_tx,
            release: release_rx,
        },
    );
    (arrived_rx, release_tx)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type T = Table<LocalStorageProvider>;

fn init() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
}

/// The two places a writer's entry can be missing from a statement's lookup.
#[derive(Clone, Copy, Debug)]
enum Mode {
    /// `id` UNIQUE, so a B-tree index answers; the twin has no index.
    Index,
    /// Filters on, with the size gate lifted so this small table uses the
    /// mask, and no index; the twin has neither filters nor an index.
    Filter,
}

fn payload(bytes: &[u8], write_order: u32, column_type: DbType) -> ColumnWritePayload {
    let mut mb = MEMORY_POOL.acquire(bytes.len());
    mb.into_slice_mut().copy_from_slice(bytes);
    let size = column_type.get_size();
    ColumnWritePayload {
        data: mb,
        write_order,
        column_type,
        size,
    }
}

fn row(id: u64, cat: i32) -> RowWrite {
    let mut rw = RowWrite {
        columns_writing_data: SmallVec::new(),
    };
    rw.columns_writing_data.push(payload(&id.to_le_bytes(), 0, DbType::U64));
    rw.columns_writing_data.push(payload(&cat.to_le_bytes(), 1, DbType::I32));
    rw
}

fn full_fetch(fields: &SmallVec<[SchemaField; 20]>) -> RowFetch {
    let calc = SchemaCalculator;
    let mut cols = SmallVec::new();
    for f in fields.iter().sorted_by(|a, b| a.write_order.cmp(&b.write_order)) {
        let (column_offset, schema_id) = calc.calculate_schema_offset(&f.name, fields);
        cols.push(ColumnFetchingData {
            column_offset,
            column_type: f.db_type.clone(),
            size: f.size,
            schema_id,
        });
    }
    RowFetch {
        columns_fetching_data: cols,
    }
}

fn set_cat(t: &T, cat: i32) -> SmallVec<[ColumnWritePayload; 32]> {
    let f = t.schema.fields.iter().find(|f| f.name == "cat").unwrap();
    let mut sv: SmallVec<[ColumnWritePayload; 32]> = SmallVec::new();
    sv.push(payload(&cat.to_le_bytes(), f.write_order as u32, f.db_type.clone()));
    sv
}

/// A versioned table over `id U64, cat I32`, with rows 1..=3 committed.
/// `unique` makes `id` UNIQUE, which gives it an implicit B-tree index and the
/// point-update path. `filters`: `Some(true)` lifts the filter's size gate for
/// this table, `Some(false)` turns its filter off, `None` leaves the defaults.
async fn open(
    dir: &std::path::Path,
    name: &str,
    unique: bool,
    filters: Option<bool>,
) -> (StdArc<T>, StdArc<TransactionManager>) {
    init();
    let settings = crate::core::filter::settings::settings();
    match filters {
        Some(true) => settings.update_table(name, |o| o.min_table_rows = Some(0)),
        Some(false) => settings.update_table(name, |o| o.indexed = Some(false)),
        None => {}
    }
    let io = Arc::new(LocalStorageProvider::new(dir.to_str().unwrap(), Some(name)).await);
    let unique_cols: Vec<String> = if unique { vec!["id".to_string()] } else { vec![] };
    let t = Table::new_with_constraints(
        name,
        io,
        vec![
            CreateColumnData {
                name: "id".into(),
                data_type: DbType::U64,
            },
            CreateColumnData {
                name: "cat".into(),
                data_type: DbType::I32,
            },
        ],
        &unique_cols,
    )
    .await;
    let manager = StdArc::new(TransactionManager::in_memory());
    t.enable_versioning(manager.clone());
    for id in 1..=3u64 {
        t.insert_row(row(id, 1)).await.unwrap();
    }
    t.wait_filters_ready().await;
    if unique {
        let ix = t.indexes.as_ref().expect("a UNIQUE column is indexed");
        let ready = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !ix.is_ready() {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        })
        .await;
        assert!(ready.is_ok(), "index build never finished");
    } else {
        assert!(t.indexes.is_none(), "{name}: no UNIQUE key, so no index");
    }
    match filters {
        Some(true) => {
            let f = t.filters.as_ref().expect("filters on");
            assert_eq!(f.config().min_table_rows, 0, "{name}: the size gate is lifted");
        }
        Some(false) => assert!(t.filters.is_none(), "{name}: the filter is off"),
        None => {}
    }
    (StdArc::new(t), manager)
}

/// The table under test and its twin, for a mode.
async fn open_pair(
    subject_dir: &std::path::Path,
    twin_dir: &std::path::Path,
    tag: &str,
    mode: Mode,
) -> ((StdArc<T>, StdArc<TransactionManager>), (StdArc<T>, StdArc<TransactionManager>)) {
    match mode {
        Mode::Index => (
            open(subject_dir, &format!("so_{tag}_ix"), true, None).await,
            open(twin_dir, &format!("so_{tag}_ix_twin"), false, None).await,
        ),
        Mode::Filter => (
            open(subject_dir, &format!("so_{tag}_fl"), false, Some(true)).await,
            open(twin_dir, &format!("so_{tag}_fl_twin"), false, Some(false)).await,
        ),
    }
}

#[derive(Clone, Copy, Debug)]
enum Stmt {
    Delete,
    /// `id >= k AND id <= k`: no equality on the key, so the full path.
    UpdateRange,
    /// `id = k`: the point path on a table with a UNIQUE key, the full path
    /// (through the filter's fingerprint) on one without.
    UpdatePoint,
    Select,
    SelectBytes,
    Count,
}

#[derive(Debug, PartialEq)]
enum Answer {
    Affected(u64),
    Rows(usize),
    Bytes(Vec<u8>),
    Count(u64),
}

/// The WHERE clause a statement uses for key `k`.
fn clause(stmt: Stmt, k: u64) -> String {
    match stmt {
        Stmt::UpdateRange => format!("id >= {k} AND id <= {k}"),
        _ => format!("id = {k}"),
    }
}

async fn run_stmt(t: &T, stmt: Stmt, k: u64, tx: u64) -> Answer {
    let fields = t.schema.fields.to_vec();
    let fetch = full_fetch(&t.schema.fields);
    let w = clause(stmt, k);
    match stmt {
        Stmt::Delete => Answer::Affected(t.delete_rows_in_txn(&w, fetch, None, tx).await.unwrap()),
        Stmt::UpdateRange | Stmt::UpdatePoint => Answer::Affected(
            t.update_rows_with_expressions(&w, fetch, set_cat(t, 9), &[], None, tx)
                .await
                .unwrap(),
        ),
        Stmt::Select => Answer::Rows(
            t.query_row_in_txn(&w, &fields, fetch.clone(), fetch, None, None, tx)
                .await
                .unwrap()
                .len(),
        ),
        Stmt::SelectBytes => Answer::Bytes(
            t.query_row_bytes_in_txn(&w, &fields, fetch.clone(), fetch, None, None, tx)
                .await
                .unwrap(),
        ),
        Stmt::Count => Answer::Count(t.count_rows_in_txn(&w, &fields, fetch, tx).await.unwrap()),
    }
}

/// `(id, cat)` of every row a fresh snapshot sees, sorted.
async fn contents(t: &T) -> Vec<(u64, i32)> {
    let fetch = full_fetch(&t.schema.fields);
    let rows = t
        .query_row("", &t.schema.fields.to_vec(), fetch.clone(), fetch, None, None)
        .await
        .unwrap();
    let col = |r: &crate::core::row::row::Row, sid: u64| -> Vec<u8> {
        r.columns
            .iter()
            .find(|c| u64::from(c.schema_id) == sid)
            .unwrap()
            .data
            .into_slice()
            .to_vec()
    };
    let mut out: Vec<(u64, i32)> = rows
        .iter()
        .map(|r| {
            (
                u64::from_le_bytes(col(r, 0).try_into().unwrap()),
                i32::from_le_bytes(col(r, 1).try_into().unwrap()),
            )
        })
        .collect();
    out.sort();
    out
}

/// What a withdrawn entry was withdrawn from, so it can be put back.
enum Withdrawn {
    Nothing,
    Index(StdArc<crate::core::index::TableIndexes>),
    Filter(StdArc<crate::core::filter::TableFilters>),
}

/// One table, one statement, under the interleaving: the writer's row (key
/// `k`, `cat` 7) is appended but uncommitted when the statement starts, and
/// on the subject its entry is withdrawn; once the statement has asked its
/// index and filter, the writer puts the entry back and commits; then the
/// statement finishes. `subject` picks what is withdrawn: per `mode`, the
/// B-tree entry or the filter's fingerprint; the twin withdraws nothing.
async fn interleave_unseen(
    t: StdArc<T>,
    manager: StdArc<TransactionManager>,
    stmt: Stmt,
    mode: Mode,
    subject: bool,
    k: u64,
) -> (Answer, Vec<(u64, i32)>) {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let writer = manager.begin();
    t.insert_row_in_txn(row(k, 7), writer).await.unwrap();
    let slot = t.io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64 - 1;

    // The state between the writer's append and its index update.
    let withdrawn = match (subject, mode) {
        (false, _) => Withdrawn::Nothing,
        (true, Mode::Index) => {
            let ix = t.indexes.clone().expect("indexed");
            let rw = row(k, 7);
            assert!(ix.holds(slot), "the insert indexed the slot it appended");
            let get = |wo: u32| {
                rw.columns_writing_data
                    .iter()
                    .find(|c| c.write_order == wo)
                    .map(|c| c.data.into_slice())
            };
            ix.forget(proof, slot, Some(&get));
            assert!(!ix.holds(slot), "the entry is withdrawn");
            Withdrawn::Index(ix)
        }
        (true, Mode::Filter) => {
            let f = t.filters.clone().expect("filtered");
            let w = clause(stmt, k);
            let before = t.build_candidates(&w, None).await.expect("the filter masks this clause");
            assert!(before.get(slot), "the insert fingerprinted the slot it appended");
            f.on_delete(proof, slot);
            let after = t.build_candidates(&w, None).await.expect("the filter masks this clause");
            assert!(
                !after.get(slot),
                "{stmt:?}: with its fingerprint withdrawn the mask leaves the slot out, \
                 as it does a slot the filter has not been told about"
            );
            Withdrawn::Filter(f)
        }
    };

    let stmt_tx = manager.begin();
    let (arrived, release) = arm(&t.schema.name);
    let runner = {
        let t = t.clone();
        tokio::spawn(async move { run_stmt(&t, stmt, k, stmt_tx).await })
    };

    let reached = tokio::time::timeout(std::time::Duration::from_secs(10), arrived).await;
    assert!(
        matches!(reached, Ok(Ok(()))),
        "{:?} on {} never reached the failpoint",
        stmt,
        t.schema.name
    );
    // The writer finishes: entry first, then the commit, as
    // `insert_row_in_txn` orders them.
    match withdrawn {
        Withdrawn::Nothing => {}
        Withdrawn::Index(ix) => ix.on_insert(proof, slot, &row(k, 7)),
        Withdrawn::Filter(f) => f.on_insert(proof, slot, &row(k, 7)),
    }
    manager.commit(writer);
    release.send(()).unwrap();

    let answer = tokio::time::timeout(std::time::Duration::from_secs(10), runner)
        .await
        .expect("statement finished")
        .expect("statement task");
    manager.commit(stmt_tx);
    (answer, contents(&t).await)
}

/// The other direction: the writer's row (key `k`, `cat` 7) is inserted,
/// indexed and committed before the statement begins, so its snapshot sees it.
async fn interleave_seen(t: StdArc<T>, manager: StdArc<TransactionManager>, stmt: Stmt, k: u64) -> (Answer, Vec<(u64, i32)>) {
    let writer = manager.begin();
    t.insert_row_in_txn(row(k, 7), writer).await.unwrap();
    manager.commit(writer);

    let stmt_tx = manager.begin();
    let answer = tokio::time::timeout(std::time::Duration::from_secs(10), run_stmt(&t, stmt, k, stmt_tx))
        .await
        .expect("statement finished");
    manager.commit(stmt_tx);
    (answer, contents(&t).await)
}

/// Which branch of `update_rows_with_expressions` the statement takes, so a
/// change to `point_update_stripe` cannot quietly turn one case into another.
fn assert_update_path(subject: &T, twin: &T, stmt: Stmt, mode: Mode, k: u64) {
    let w = clause(stmt, k);
    let point = |t: &T| t.point_update_stripe(&w, &set_cat(t, 9), &[]).is_some();
    match (stmt, mode) {
        (Stmt::UpdatePoint, Mode::Index) => {
            assert!(point(subject), "`{w}` on a UNIQUE key takes the point path");
            assert!(!point(twin), "the twin has no UNIQUE key, so the full path");
        }
        (Stmt::UpdatePoint | Stmt::UpdateRange, _) => {
            assert!(!point(subject), "`{w}` on {mode:?} takes the full path");
            assert!(!point(twin), "`{w}` on the twin takes the full path");
        }
        _ => {}
    }
}

/// Key the `unseen` interleaving writes; `seen` writes [`SEEN_KEY`].
const UNSEEN_KEY: u64 = 100;
const SEEN_KEY: u64 = 200;

async fn case(stmt: Stmt, mode: Mode) {
    let subject_dir = tempfile::tempdir().unwrap();
    let twin_dir = tempfile::tempdir().unwrap();
    let tag = format!("{:?}", stmt).to_lowercase();
    let ((subject, sm), (twin, tm)) = open_pair(subject_dir.path(), twin_dir.path(), &tag, mode).await;

    // Unseen: the commit lands after the statement's snapshot.
    assert_update_path(&subject, &twin, stmt, mode, UNSEEN_KEY);
    let (twin_answer, twin_after) =
        interleave_unseen(twin.clone(), tm.clone(), stmt, mode, false, UNSEEN_KEY).await;
    let (answer, after) = interleave_unseen(subject.clone(), sm.clone(), stmt, mode, true, UNSEEN_KEY).await;
    assert_eq!(
        answer, twin_answer,
        "{stmt:?}/{mode:?} unseen: the subject answered differently from its twin, so its \
         candidates disagreed with its snapshot"
    );
    assert_eq!(after, twin_after, "{stmt:?}/{mode:?} unseen: tables diverged");
    let unseen = match stmt {
        Stmt::Delete | Stmt::UpdateRange | Stmt::UpdatePoint => Answer::Affected(0),
        Stmt::Select => Answer::Rows(0),
        Stmt::Count => Answer::Count(0),
        // No row serialized is no bytes at all.
        Stmt::SelectBytes => Answer::Bytes(Vec::new()),
    };
    assert_eq!(answer, unseen, "{stmt:?}/{mode:?}: the snapshot predates the commit");
    assert!(after.contains(&(UNSEEN_KEY, 7)), "{stmt:?}/{mode:?}: {after:?}");

    // Seen: the commit lands before the statement begins.
    assert_update_path(&subject, &twin, stmt, mode, SEEN_KEY);
    let (twin_answer, twin_after) = interleave_seen(twin, tm, stmt, SEEN_KEY).await;
    let (answer, after) = interleave_seen(subject, sm, stmt, SEEN_KEY).await;
    assert_eq!(answer, twin_answer, "{stmt:?}/{mode:?} seen: the subject answered differently from its twin");
    assert_eq!(after, twin_after, "{stmt:?}/{mode:?} seen: tables diverged");
    match stmt {
        Stmt::Delete => {
            assert_eq!(answer, Answer::Affected(1), "{stmt:?}/{mode:?}: a committed row is deleted");
            assert!(!after.iter().any(|(id, _)| *id == SEEN_KEY), "{stmt:?}/{mode:?}: {after:?}");
        }
        Stmt::UpdateRange | Stmt::UpdatePoint => {
            assert_eq!(answer, Answer::Affected(1), "{stmt:?}/{mode:?}: a committed row is updated");
            assert!(after.contains(&(SEEN_KEY, 9)), "{stmt:?}/{mode:?}: {after:?}");
            assert!(!after.contains(&(SEEN_KEY, 7)), "{stmt:?}/{mode:?}: {after:?}");
        }
        Stmt::Select => assert_eq!(answer, Answer::Rows(1), "{stmt:?}/{mode:?}"),
        Stmt::Count => assert_eq!(answer, Answer::Count(1), "{stmt:?}/{mode:?}"),
        Stmt::SelectBytes => {
            assert!(matches!(&answer, Answer::Bytes(b) if !b.is_empty()), "{stmt:?}/{mode:?}: {answer:?}")
        }
    }
    if !matches!(stmt, Stmt::Delete | Stmt::UpdateRange | Stmt::UpdatePoint) {
        assert!(after.contains(&(SEEN_KEY, 7)), "{stmt:?}/{mode:?}: {after:?}");
    }
}

// A B-tree entry missing from the lookup.

#[tokio::test(flavor = "multi_thread")]
async fn delete_matches_through_a_snapshot_taken_before_its_candidates() {
    case(Stmt::Delete, Mode::Index).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_matches_through_a_snapshot_taken_before_its_candidates() {
    case(Stmt::UpdateRange, Mode::Index).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn point_update_matches_through_a_snapshot_taken_before_its_candidates() {
    case(Stmt::UpdatePoint, Mode::Index).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn select_reads_through_a_snapshot_taken_before_its_candidates() {
    case(Stmt::Select, Mode::Index).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bytes_select_reads_through_a_snapshot_taken_before_its_candidates() {
    case(Stmt::SelectBytes, Mode::Index).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn count_reads_through_a_snapshot_taken_before_its_candidates() {
    case(Stmt::Count, Mode::Index).await;
}

// A filter fingerprint missing from the mask, on a table with no index. Only
// equality clauses: a withdrawn fingerprint leaves the zone's bounds wide
// (bounds only widen), so a range clause cannot be made to miss the slot this
// way; the full UPDATE path is covered by `UpdatePoint`, which has no point
// path without a UNIQUE key.

#[tokio::test(flavor = "multi_thread")]
async fn delete_matches_past_a_missing_fingerprint() {
    case(Stmt::Delete, Mode::Filter).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn update_matches_past_a_missing_fingerprint() {
    case(Stmt::UpdatePoint, Mode::Filter).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn select_reads_past_a_missing_fingerprint() {
    case(Stmt::Select, Mode::Filter).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bytes_select_reads_past_a_missing_fingerprint() {
    case(Stmt::SelectBytes, Mode::Filter).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn count_reads_past_a_missing_fingerprint() {
    case(Stmt::Count, Mode::Filter).await;
}
