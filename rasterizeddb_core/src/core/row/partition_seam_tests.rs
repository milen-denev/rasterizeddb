//! A partition `Table` (design §3.4, §3.5, §4.5, §8): its range guard (I4),
//! its shared row-id sequence (I12), its own cache scope (I20), and the
//! `_with_visibility` read variants a fan-out statement needs (I7).
//!
//! Partitions are built here directly with `Table::open_partition`, over
//! files laid out as `core::partition::files` names them; no SQL reaches
//! them yet.

use std::sync::Arc as StdArc;

use rclite::Arc;
use smallvec::SmallVec;

use crate::core::db_type::DbType;
use crate::core::mvcc::TransactionManager;
use crate::core::partition::files;
use crate::core::partition::guard::{KEY_CHANGE_REFUSAL, RangeGuard};
use crate::core::partition::{KeySpec, PointRange, RangeMap};
use crate::core::row::row::{ColumnWritePayload, Row, RowFetch, RowWrite};
use crate::core::row::row_pointer::{ROW_POINTER_RECORD_LEN, RowPointer};
use crate::core::row::schema::TableSchema;
use crate::core::row::table::{IdSource, PartitionOpen, Table};
use crate::core::rql::lexer_ct::CreateColumnData;
use crate::core::storage_providers::file_sync::LocalStorageProvider;
use crate::core::storage_providers::traits::StorageIO;
use crate::memory_pool::MEMORY_POOL;

type P = LocalStorageProvider;

const UID: u64 = 0x0000_5eed_0000_0001;

fn init_threads() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
}

fn init() {
    init_threads();
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
}

/// The logical table every partition here belongs to: its schema, the shell's
/// schema provider and the one id sequence.
struct Logical {
    seed: Arc<P>,
    io_schema: Arc<P>,
    schema: TableSchema,
    ids: StdArc<IdSource<P>>,
}

fn columns() -> Vec<CreateColumnData> {
    vec![
        CreateColumnData {
            name: "tenant_id".into(),
            data_type: DbType::I64,
        },
        CreateColumnData {
            name: "n".into(),
            data_type: DbType::I32,
        },
        CreateColumnData {
            name: "label".into(),
            data_type: DbType::STRING,
        },
    ]
}

async fn create_logical(dir: &std::path::Path) -> Logical {
    let seed = Arc::new(P::new(dir.to_str().unwrap(), Some("seed.db")).await);
    let io_schema = Arc::new(seed.create_new(format!("{}.db", files::schema_stem(UID))).await);
    let schema = Table::create_schema_files(io_schema.clone(), "acct", columns(), &[])
        .await
        .unwrap();
    let ids_io = Arc::new(seed.create_new(files::ids_file(UID)).await);
    let ids = StdArc::new(IdSource::create(ids_io).await.unwrap());
    Logical {
        seed,
        io_schema,
        schema,
        ids,
    }
}

fn spec(schema: &TableSchema) -> KeySpec {
    KeySpec::from_schema(schema, &["tenant_id"]).unwrap()
}

fn point_of(schema: &TableSchema, tenant: i64) -> u64 {
    spec(schema).point_of(&[&tenant.to_le_bytes()]).unwrap()
}

async fn open(l: &Logical, pid: u32, range: PointRange) -> Table<P> {
    let physical = files::data_stem(UID, pid);
    Table::open_partition(PartitionOpen {
        cache_scope: format!("1\u{0}{}", physical),
        physical,
        seed: l.seed.clone(),
        io_schema: l.io_schema.clone(),
        schema: l.schema.clone(),
        ids: l.ids.clone(),
        guard: RangeGuard::new(spec(&l.schema), range),
        mode: crate::core::partition::maintenance::MaintenanceMode::Own,
        watch: None,
    })
    .await
    .unwrap()
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

fn wo(t: &Table<P>, name: &str) -> u32 {
    t.schema.fields.iter().find(|f| f.name == name).unwrap().write_order as u32
}

fn row(t: &Table<P>, tenant: i64, n: i32, label: &str) -> RowWrite {
    let mut w = RowWrite {
        columns_writing_data: SmallVec::new(),
    };
    w.columns_writing_data.push(payload(&tenant.to_le_bytes(), wo(t, "tenant_id"), DbType::I64));
    w.columns_writing_data.push(payload(&n.to_le_bytes(), wo(t, "n"), DbType::I32));
    w.columns_writing_data.push(payload(label.as_bytes(), wo(t, "label"), DbType::STRING));
    w
}

fn set(t: &Table<P>, assignments: &[(&str, &[u8])]) -> SmallVec<[ColumnWritePayload; 32]> {
    let mut sv = SmallVec::new();
    for (name, bytes) in assignments {
        let f = t.schema.fields.iter().find(|f| f.name == *name).unwrap();
        sv.push(payload(bytes, f.write_order as u32, f.db_type.clone()));
    }
    sv
}

fn fetch(t: &Table<P>) -> RowFetch {
    Table::<P>::build_full_row_fetch(&t.schema.fields)
}

async fn rows(t: &Table<P>, w: &str) -> Vec<Row> {
    t.query_row(w, &t.schema.fields.to_vec(), fetch(t), fetch(t), None, None)
        .await
        .unwrap()
}

fn i64_of(t: &Table<P>, r: &Row, name: &str) -> i64 {
    let w = wo(t, name);
    let c = r.columns.iter().find(|c| c.schema_id as u32 == w).unwrap();
    i64::from_le_bytes(c.data.into_slice().try_into().unwrap())
}

fn i32_of(t: &Table<P>, r: &Row, name: &str) -> i32 {
    let w = wo(t, name);
    let c = r.columns.iter().find(|c| c.schema_id as u32 == w).unwrap();
    i32::from_le_bytes(c.data.into_slice().try_into().unwrap())
}

fn str_of(t: &Table<P>, r: &Row, name: &str) -> String {
    let w = wo(t, name);
    let c = r.columns.iter().find(|c| c.schema_id as u32 == w).unwrap();
    String::from_utf8_lossy(c.data.into_slice()).to_string()
}

async fn pointers(t: &Table<P>) -> Vec<RowPointer> {
    let io = t.io_pointers().unwrap();
    let len = io.get_len().await;
    let mut out = Vec::new();
    let mut pos = 0u64;
    while pos + ROW_POINTER_RECORD_LEN as u64 <= len {
        let mut buf = [0u8; ROW_POINTER_RECORD_LEN];
        let mut p = pos;
        io.read_data_into_buffer(&mut p, &mut buf).await.unwrap();
        out.push(RowPointer::from_slice(&buf));
        pos += ROW_POINTER_RECORD_LEN as u64;
    }
    out
}

// ---------------------------------------------------------------------------
// I4: the range guard.
// ---------------------------------------------------------------------------

/// An insert whose point lies outside the partition's range fails as an
/// internal error and stores nothing; one inside succeeds. This is what turns
/// a routing bug into a failed statement instead of a row no routed query
/// finds.
#[tokio::test(flavor = "multi_thread")]
async fn an_insert_outside_its_range_errors() {
    init();
    let dir = tempfile::tempdir().unwrap();
    let l = create_logical(dir.path()).await;
    // Partition 1 of 4: some tenant lands there and some other does not.
    let map = RangeMap::initial(4).unwrap();
    let pid = map.pid_of(point_of(&l.schema, 42));
    let range = map.range_of(pid).unwrap();
    let outsider = (1..10_000i64)
        .find(|t| !range.contains(point_of(&l.schema, *t)))
        .unwrap();
    let t = open(&l, pid, range).await;

    let e = t.insert_row(row(&t, outsider, 1, "stray")).await.unwrap_err().to_string();
    assert!(e.contains("internal routing error"), "{e}");
    // XX000 in pgwire: not a phrase mapped to a client error class.
    assert!(!e.contains("not supported") && !e.contains("out of range"), "{e}");
    assert!(pointers(&t).await.is_empty(), "the refused row was written");

    t.insert_row(row(&t, 42, 1, "home")).await.unwrap();
    let found = rows(&t, "").await;
    assert_eq!(found.len(), 1);
    assert_eq!(i64_of(&t, &found[0], "tenant_id"), 42);
}

/// A write-ahead log for `dir` until the guard drops. An in-place overwrite
/// is taken only where the write waits for its commit
/// (`StorageIO::defers_in_place_writes`), which for a file means one with a
/// log: the tests of that path run under one.
struct Logged(String);

impl Drop for Logged {
    fn drop(&mut self) {
        crate::core::wal::remove_for(&self.0);
    }
}

fn logged(dir: &std::path::Path) -> Logged {
    let location = dir.to_str().unwrap().to_string();
    let wal = std::sync::Arc::new(
        crate::core::wal::Wal::open(dir.join(crate::core::wal::DEFAULT_WAL_NAME)).unwrap(),
    );
    crate::core::wal::install_for(&location, wal);
    Logged(location)
}

/// A partition owning the whole space, so only the "point unchanged" rule can
/// refuse an update, never the range.
async fn quiet_partition(dir: &std::path::Path) -> Table<P> {
    init();
    let l = create_logical(dir).await;
    let t = open(&l, 0, PointRange::FULL).await;
    for (tenant, n, label) in [(42i64, 1i32, "aaaa"), (7, 2, "bbbb")] {
        t.insert_row(row(&t, tenant, n, label)).await.unwrap();
    }
    t.wait_filters_ready().await.unwrap();
    t
}

async fn assert_untouched(t: &Table<P>) {
    let found = rows(t, "n = 1").await;
    assert_eq!(found.len(), 1, "the row was lost or duplicated");
    assert_eq!(i64_of(t, &found[0], "tenant_id"), 42, "the key changed");
    assert_eq!(str_of(t, &found[0], "label"), "aaaa", "the row was rewritten");
    assert!(rows(t, "tenant_id = 43").await.is_empty(), "a row moved to tenant 43");
}

/// The fixed-width fast path writes bytes in place and is taken whenever
/// nothing else runs, so a guard on the relocating branch alone would let
/// `SET tenant_id = 43` rewrite tenant 42's row where it stands. Refused with
/// 0A000, the in-place counter unmoved, the row still tenant 42's.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_change_on_the_in_place_path_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let _log = logged(dir.path());
    let t = quiet_partition(dir.path()).await;
    let fast = !cfg!(feature = "enable_long_row");

    // The fast path is open on this table: a non-key fixed-width SET takes it.
    let before = t.in_place_column_updates();
    let n = t.update_rows("n = 2", fetch(&t), set(&t, &[("n", &3i32.to_le_bytes())]), None).await.unwrap();
    assert_eq!(n, 1);
    assert_eq!(t.in_place_column_updates(), before + fast as u64, "the fast path was not open");

    let before = t.in_place_column_updates();
    let e = t
        .update_rows("n = 1", fetch(&t), set(&t, &[("tenant_id", &43i64.to_le_bytes())]), None)
        .await
        .unwrap_err();
    assert_eq!(e.to_string(), KEY_CHANGE_REFUSAL);
    assert!(!e.to_string().ends_with(Table::<P>::CONFLICT_RETRYABLE), "a key change was marked retryable");
    assert_eq!(t.in_place_column_updates(), before, "the key change was written in place");
    assert_untouched(&t).await;
}

/// A longer string forces relocation: refused all the same, nothing appended.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_change_by_relocation_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let t = quiet_partition(dir.path()).await;
    let slots = pointers(&t).await.len();
    let e = t
        .update_rows(
            "n = 1",
            fetch(&t),
            set(&t, &[("tenant_id", &43i64.to_le_bytes()), ("label", b"a much longer label that relocates")]),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(e.to_string(), KEY_CHANGE_REFUSAL);
    assert_eq!(pointers(&t).await.len(), slots, "a relocated version was appended");
    assert_untouched(&t).await;
}

/// A same-length string outside a transaction overwrites the row where it
/// stands (`relocated == None`), the other branch that never relocates.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_change_by_in_place_overwrite_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let t = quiet_partition(dir.path()).await;
    let slots = pointers(&t).await.len();
    let e = t
        .update_rows(
            "n = 1",
            fetch(&t),
            set(&t, &[("tenant_id", &43i64.to_le_bytes()), ("label", b"cccc")]),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(e.to_string(), KEY_CHANGE_REFUSAL);
    assert_eq!(pointers(&t).await.len(), slots);
    assert_untouched(&t).await;
}

/// ORMs assign every column, usually its current value: assigning a key the
/// value it holds keeps the point and is an ordinary update, on every branch.
#[tokio::test(flavor = "multi_thread")]
async fn a_same_value_key_assignment_is_ordinary() {
    let dir = tempfile::tempdir().unwrap();
    let t = quiet_partition(dir.path()).await;
    let key = 42i64.to_le_bytes();
    // Fast path.
    let n = t
        .update_rows("n = 1", fetch(&t), set(&t, &[("tenant_id", &key), ("n", &5i32.to_le_bytes())]), None)
        .await
        .unwrap();
    assert_eq!(n, 1);
    // Relocation.
    let n = t
        .update_rows("n = 5", fetch(&t), set(&t, &[("tenant_id", &key), ("label", b"grown to relocate")]), None)
        .await
        .unwrap();
    assert_eq!(n, 1);
    let found = rows(&t, "tenant_id = 42").await;
    assert_eq!(found.len(), 1);
    assert_eq!(i32_of(&t, &found[0], "n"), 5);
    assert_eq!(str_of(&t, &found[0], "label"), "grown to relocate");
}

/// A multi-row statement that reaches a key change part way through fails as
/// a whole, and the row whose key it would have changed keeps it. (Outside a
/// transaction, as here, rows before it that kept their point may already be
/// rewritten. Under the executor's statement transaction they are new
/// versions the abort discards, because a statement assigning a key never
/// takes the in-place fast path: `a_refused_key_change_leaves_nothing_after_abort`.)
#[tokio::test(flavor = "multi_thread")]
async fn a_key_change_part_way_through_a_statement_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let t = quiet_partition(dir.path()).await;
    // Every row gets tenant 42: the first row matched keeps its point, the
    // second (tenant 7) does not.
    let e = t
        .update_rows("n >= 1", fetch(&t), set(&t, &[("tenant_id", &42i64.to_le_bytes())]), None)
        .await
        .unwrap_err();
    assert_eq!(e.to_string(), KEY_CHANGE_REFUSAL);
    assert_eq!(rows(&t, "tenant_id = 7").await.len(), 1, "tenant 7's row changed key");
    assert_eq!(rows(&t, "").await.len(), 2);
}

/// The executor's shape: an autocommit transaction, aborted when the statement
/// fails. The first row matched keeps its point and is written before the
/// second is refused; had it gone through the fixed-width fast path, which
/// admits statement transactions, its bytes would be overwritten in place and
/// the abort could not take them back. The client is told the statement
/// failed, so no row may show its change.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_key_change_leaves_nothing_after_abort() {
    let dir = tempfile::tempdir().unwrap();
    let _log = logged(dir.path());
    let t = quiet_partition(dir.path()).await;
    let manager = StdArc::new(TransactionManager::in_memory());
    t.enable_versioning(manager.clone());
    let fast = !cfg!(feature = "enable_long_row");

    // The fast path is open to an autocommit statement here: a same-value
    // non-key SET takes it.
    let before = t.in_place_column_updates();
    let tx = manager.begin_autocommit();
    let n = t
        .update_rows_in_txn("n = 2", fetch(&t), set(&t, &[("n", &2i32.to_le_bytes())]), None, tx)
        .await
        .unwrap();
    manager.commit(tx);
    assert_eq!(n, 1);
    assert_eq!(t.in_place_column_updates(), before + fast as u64, "the fast path was not open");

    let tx = manager.begin_autocommit();
    let e = t
        .update_rows_in_txn(
            "n >= 1",
            fetch(&t),
            set(&t, &[("tenant_id", &42i64.to_le_bytes()), ("n", &9i32.to_le_bytes())]),
            None,
            tx,
        )
        .await
        .unwrap_err();
    assert_eq!(e.to_string(), KEY_CHANGE_REFUSAL);
    manager.abort(tx);
    assert!(rows(&t, "n = 9").await.is_empty(), "a refused, aborted statement left a row changed");
    assert_eq!(rows(&t, "").await.len(), 2);
    assert_untouched(&t).await;
}

/// A refusal after rows were written still ends the statement's storage
/// transaction: left open, it holds the log's checkpoint off for good. Only a
/// partition whose directory has a log can show it; without one the
/// transaction is token 0.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_key_change_ends_its_storage_transaction() {
    struct Uninstall(String);
    impl Drop for Uninstall {
        fn drop(&mut self) {
            crate::core::wal::remove_for(&self.0);
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let location = dir.path().to_str().unwrap().to_string();
    let wal = std::sync::Arc::new(
        crate::core::wal::Wal::open(dir.path().join(crate::core::wal::DEFAULT_WAL_NAME)).unwrap(),
    );
    crate::core::wal::install_for(&location, wal.clone());
    let _uninstall = Uninstall(location);

    let t = quiet_partition(dir.path()).await;
    assert_eq!(wal.pending_transactions(), 0);
    // Tenant 42's row is rewritten in place under the transaction (same
    // length, no versioning), then tenant 7's is refused.
    let e = t
        .update_rows("n >= 1", fetch(&t), set(&t, &[("tenant_id", &42i64.to_le_bytes())]), None)
        .await
        .unwrap_err();
    assert_eq!(e.to_string(), KEY_CHANGE_REFUSAL);
    assert_eq!(wal.pending_transactions(), 0, "the refused statement left its transaction open");
    assert_eq!(rows(&t, "tenant_id = 7").await.len(), 1);
    drop(t);
}

/// A statement the range guard refuses on a later row leaves no trace of its
/// earlier rows, even in storage: their pending in-place overwrites are
/// dropped with the statement's storage transaction, as a refused row check
/// drops them, rather than committed durably ahead of the error. Checked live
/// and in the files, through a partition reopened over them.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_key_change_leaves_no_earlier_row_in_storage() {
    struct Uninstall(String);
    impl Drop for Uninstall {
        fn drop(&mut self) {
            crate::core::wal::remove_for(&self.0);
        }
    }
    init();
    let dir = tempfile::tempdir().unwrap();
    let location = dir.path().to_str().unwrap().to_string();
    let wal = std::sync::Arc::new(
        crate::core::wal::Wal::open(dir.path().join(crate::core::wal::DEFAULT_WAL_NAME)).unwrap(),
    );
    crate::core::wal::install_for(&location, wal.clone());
    let _uninstall = Uninstall(location);

    let l = create_logical(dir.path()).await;
    let t = open(&l, 0, PointRange::FULL).await;
    for (tenant, n, label) in [(42i64, 1i32, "aaaa"), (7, 2, "bbbb")] {
        t.insert_row(row(&t, tenant, n, label)).await.unwrap();
    }
    t.wait_filters_ready().await.unwrap();
    let slots = pointers(&t).await.len();

    // Tenant 42's row keeps its point and is overwritten in place (same
    // length) under the statement's storage transaction; tenant 7's row is
    // then refused.
    let e = t
        .update_rows(
            "n >= 1",
            fetch(&t),
            set(&t, &[("tenant_id", &42i64.to_le_bytes()), ("label", b"cccc")]),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(e.to_string(), KEY_CHANGE_REFUSAL);
    assert_eq!(wal.pending_transactions(), 0, "the refused statement left its transaction open");
    assert_untouched(&t).await;
    assert_eq!(pointers(&t).await.len(), slots);
    drop(t);

    let t = open(&l, 0, PointRange::FULL).await;
    assert_untouched(&t).await;
    assert_eq!(rows(&t, "label = 'cccc'").await.len(), 0, "the refused statement's row is in the files");
    assert_eq!(rows(&t, "").await.len(), 2);
    assert_eq!(pointers(&t).await.len(), slots, "the refused statement's row is in the files");
}

fn expression_set(
    t: &Table<P>,
    column: &str,
    text: &str,
) -> (SmallVec<[ColumnWritePayload; 32]>, Vec<(u32, crate::core::rql::expr::Expr)>) {
    let f = t.schema.fields.iter().find(|f| f.name == column).unwrap();
    let parsed = crate::core::rql::expr::parse(text, t.schema.fields.as_slice()).unwrap();
    // As `lexer_ur` builds an expression assignment: an empty placeholder the
    // evaluated value replaces per row.
    let mut sv = SmallVec::new();
    sv.push(ColumnWritePayload {
        data: crate::memory_pool::MemoryBlock::default(),
        write_order: f.write_order as u32,
        column_type: f.db_type.physical(),
        size: f.size,
    });
    (sv, vec![(f.write_order as u32, parsed)])
}

/// The guard reads the row after expressions are applied: before that the key
/// holds an empty placeholder. `SET tenant_id = tenant_id + 1` is a key change
/// (0A000, not an internal error); `SET tenant_id = tenant_id`, which ORMs
/// send, keeps the point and is ordinary, as is an expression on another
/// column.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_change_by_expression_is_refused() {
    use crate::core::mvcc::transaction::FROZEN_TX;
    let dir = tempfile::tempdir().unwrap();
    let t = quiet_partition(dir.path()).await;

    let (u, x) = expression_set(&t, "tenant_id", "tenant_id + 1");
    let e = t
        .update_rows_with_expressions("n = 1", fetch(&t), u, &x, None, FROZEN_TX)
        .await
        .unwrap_err();
    assert_eq!(e.to_string(), KEY_CHANGE_REFUSAL);
    assert_untouched(&t).await;

    let (u, x) = expression_set(&t, "tenant_id", "tenant_id");
    let n = t
        .update_rows_with_expressions("n = 1", fetch(&t), u, &x, None, FROZEN_TX)
        .await
        .unwrap();
    assert_eq!(n, 1);
    assert_untouched(&t).await;

    let (u, x) = expression_set(&t, "n", "n + 1");
    let n = t
        .update_rows_with_expressions("tenant_id = 42", fetch(&t), u, &x, None, FROZEN_TX)
        .await
        .unwrap();
    assert_eq!(n, 1);
    let found = rows(&t, "tenant_id = 42").await;
    assert_eq!(found.len(), 1);
    assert_eq!(i32_of(&t, &found[0], "n"), 2);
}

/// Open files under `dir` whose name contains `stem`.
#[cfg(target_os = "linux")]
fn open_fds_naming(dir: &std::path::Path, stem: &str) -> usize {
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|e| std::fs::read_link(e.ok()?.path()).ok())
        .filter(|target| target.starts_with(dir) && target.to_string_lossy().contains(stem))
        .count()
}

/// A partition that fails to open lets its providers go: the service task it
/// spawned is stopped, not left polling with the files open for the life of
/// the process, once per retry.
#[tokio::test(flavor = "current_thread")]
#[cfg(target_os = "linux")]
async fn a_failed_open_releases_its_files() {
    init();
    let dir = tempfile::tempdir().unwrap();
    let l = create_logical(dir.path()).await;
    let physical = files::data_stem(UID, 3);
    crate::core::row::table::partition_test_hooks::FAIL_POINTER_OPEN.with(|f| f.set(true));
    let opened = Table::open_partition(PartitionOpen {
        cache_scope: format!("1\u{0}{}", physical),
        physical: physical.clone(),
        seed: l.seed.clone(),
        io_schema: l.io_schema.clone(),
        schema: l.schema.clone(),
        ids: l.ids.clone(),
        guard: RangeGuard::new(spec(&l.schema), PointRange::FULL),
        mode: crate::core::partition::maintenance::MaintenanceMode::Own,
        watch: None,
    })
    .await;
    crate::core::row::table::partition_test_hooks::FAIL_POINTER_OPEN.with(|f| f.set(false));
    assert!(opened.is_err(), "the injected failure did not reach the caller");
    assert!(open_fds_naming(dir.path(), &physical) > 0, "the count cannot see the partition's files");
    // The service task checks its signal every two seconds.
    tokio::time::sleep(std::time::Duration::from_millis(4500)).await;
    assert_eq!(open_fds_naming(dir.path(), &physical), 0, "a failed open kept its files open");
}

/// A create whose write never became durable fails, rather than reporting a
/// table that `open_strict` refuses at every start after.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_ids_create_is_an_error() {
    struct Uninstall(String);
    impl Drop for Uninstall {
        fn drop(&mut self) {
            crate::core::wal::remove_for(&self.0);
        }
    }
    init();
    let dir = tempfile::tempdir().unwrap();
    let location = dir.path().to_str().unwrap().to_string();
    let wal = std::sync::Arc::new(
        crate::core::wal::Wal::open(dir.path().join(crate::core::wal::DEFAULT_WAL_NAME)).unwrap(),
    );
    crate::core::wal::install_for(&location, wal);
    let _uninstall = Uninstall(location);
    let seed = Arc::new(P::new(dir.path().to_str().unwrap(), Some("seed.db")).await);
    let ids_io = Arc::new(seed.create_new(files::ids_file(UID)).await);

    crate::core::wal::test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(true));
    let created = IdSource::create(ids_io).await;
    crate::core::wal::test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(false));
    assert!(created.is_err(), "a create whose commit failed reported success");
}

// ---------------------------------------------------------------------------
// I12: one id sequence per logical table.
// ---------------------------------------------------------------------------

/// Two partitions drawing ids concurrently never hand out the same one, and
/// after a reopen the sequence is past every id either holds. `exclude_id`
/// and `locate_row_by_id` assume ids are unique across the table.
#[tokio::test(flavor = "multi_thread")]
async fn partitions_share_one_id_sequence() {
    init();
    let dir = tempfile::tempdir().unwrap();
    let l = create_logical(dir.path()).await;
    let a = StdArc::new(open(&l, 0, PointRange::FULL).await);
    let b = StdArc::new(open(&l, 1, PointRange::FULL).await);

    let mut tasks = Vec::new();
    for (t, tenant) in [(a.clone(), 1i64), (b.clone(), 2i64)] {
        for chunk in 0..10 {
            let t = t.clone();
            tasks.push(tokio::spawn(async move {
                for i in 0..50 {
                    t.insert_row(row(&t, tenant, chunk * 50 + i, "x")).await.unwrap();
                }
            }));
        }
    }
    for task in tasks {
        task.await.unwrap();
    }

    let mut ids: Vec<u64> = Vec::new();
    for t in [&a, &b] {
        ids.extend(pointers(t).await.iter().filter(|p| !p.deleted).map(|p| p.row_id()));
    }
    assert_eq!(ids.len(), 1000);
    let max = *ids.iter().max().unwrap();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 1000, "two partitions handed out the same row id");

    // Reopen the sequence from its file, as a restart would.
    drop((a, b));
    let ids_io = Arc::new(l.seed.create_new(files::ids_file(UID)).await);
    let reopened = IdSource::open_strict(ids_io).await.unwrap();
    assert!(reopened.handed_out() > max, "the sequence reopened at {} with id {} in use", reopened.handed_out(), max);
    let l2 = Logical {
        ids: StdArc::new(reopened),
        ..l
    };
    let a = open(&l2, 0, PointRange::FULL).await;
    a.insert_row(row(&a, 1, 9999, "after")).await.unwrap();
    let after = pointers(&a).await.iter().map(|p| p.row_id()).max().unwrap();
    assert!(after > max, "a row inserted after reopening took id {} <= {}", after, max);
}

/// A partitioned table's sequence never falls back: no partition's last
/// record knows what ids the others took. A file the provider had to create
/// (so empty) or a short one is refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_partition_without_its_ids_file_is_refused() {
    init();
    let dir = tempfile::tempdir().unwrap();
    let seed = Arc::new(P::new(dir.path().to_str().unwrap(), Some("seed.db")).await);

    // Missing: the provider creates it empty, and the open refuses that.
    let missing = Arc::new(seed.create_new(files::ids_file(UID)).await);
    assert!(IdSource::open_strict(missing).await.is_err(), "an empty ids file was accepted");

    // Seven bytes: torn, refused.
    let short = files::ids_file(UID + 1);
    std::fs::write(dir.path().join(&short), [0xffu8; 7]).unwrap();
    let short = Arc::new(seed.create_new(short).await);
    let e = IdSource::open_strict(short).await.err().expect("a 7-byte ids file was accepted").to_string();
    assert!(e.contains("7 bytes"), "{e}");

    // Eight bytes: the reservation, exactly.
    let whole = files::ids_file(UID + 2);
    std::fs::write(dir.path().join(&whole), 1234u64.to_le_bytes()).unwrap();
    let whole = Arc::new(seed.create_new(whole).await);
    assert_eq!(IdSource::open_strict(whole).await.unwrap().handed_out(), 1234);

    // And a create never reuses a file that holds anything.
    let used = Arc::new(seed.create_new(files::ids_file(UID + 2)).await);
    assert!(IdSource::create(used).await.is_err(), "create reused a populated ids file");
}

// ---------------------------------------------------------------------------
// I20: distinct cache scopes.
// ---------------------------------------------------------------------------

const CACHE_CHILD_ENV: &str = "RDB_PARTITION_CACHE_SCOPE_CHILD";

/// Two partitions share a schema and its name but hold different rows. With
/// the query cache on, one `WHERE` run twice on each returns each partition's
/// own rows. Run in a child process: the cache switch is process-wide and
/// every other library test runs with it off.
#[test]
#[cfg(unix)]
fn partitions_have_distinct_cache_scopes() {
    if std::env::var(CACHE_CHILD_ENV).is_ok() {
        return;
    }
    let exe = std::env::current_exe().expect("test binary");
    let out = std::process::Command::new(exe)
        .args(["--exact", "core::row::partition_seam_tests::cache_scope_child", "--nocapture", "--test-threads=1"])
        .env(CACHE_CHILD_ENV, "1")
        .output()
        .expect("spawn the cache child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success() && stdout.contains("1 passed"),
        "cache child failed or did not run (status {:?})\n--- stdout\n{}\n--- stderr\n{}",
        out.status,
        stdout,
        stderr.chars().rev().take(4000).collect::<String>().chars().rev().collect::<String>()
    );
}

/// The child half of [`partitions_have_distinct_cache_scopes`]. A no-op
/// unless the parent set the environment variable.
#[tokio::test(flavor = "multi_thread")]
async fn cache_scope_child() {
    if std::env::var(CACHE_CHILD_ENV).is_err() {
        return;
    }
    init_threads();
    crate::cache::query_cache::init(1024);
    let _ = crate::cache::query_cache::ENABLE_CACHE.set(true);
    assert!(crate::cache::query_cache::enabled(), "the child could not turn the cache on");

    let dir = tempfile::tempdir().unwrap();
    let l = create_logical(dir.path()).await;
    let mut a = open(&l, 0, PointRange::FULL).await;
    let mut b = open(&l, 1, PointRange::FULL).await;
    // What `Database` calls on every table it holds. A partition keeps its
    // own scope through it.
    a.scope_cache_to_database(1);
    b.scope_cache_to_database(1);
    assert_ne!(a.cache_key().0, b.cache_key().0);

    // A: three matching rows. B: five, longer, so positions differ too.
    for i in 0..3 {
        a.insert_row(row(&a, 1, i, "a")).await.unwrap();
    }
    for i in 0..5 {
        b.insert_row(row(&b, 2, i, "bbbbbbbbbbbbbbbb")).await.unwrap();
    }
    // Generations are what retire entries; a shared scope only shows once
    // they coincide. Bring them level with rows the clause does not match.
    while a.cache_key().1 < b.cache_key().1 {
        a.insert_row(row(&a, 1, -1, "filler")).await.unwrap();
    }
    while b.cache_key().1 < a.cache_key().1 {
        b.insert_row(row(&b, 2, -1, "filler")).await.unwrap();
    }
    assert_eq!(a.cache_key().1, b.cache_key().1);
    a.wait_filters_ready().await.unwrap();
    b.wait_filters_ready().await.unwrap();

    let clause = "n >= 0";
    let tenants = |t: &Table<P>, found: &[Row]| found.iter().map(|r| i64_of(t, r, "tenant_id")).collect::<Vec<_>>();

    let (hits0, _, _) = crate::cache::query_cache::stats();
    for _ in 0..2 {
        let found = rows(&a, clause).await;
        assert_eq!(tenants(&a, &found), vec![1; 3], "partition A answered with other rows");
    }
    let (hits1, _, stores1) = crate::cache::query_cache::stats();
    assert!(stores1 > 0 && hits1 > hits0, "the cache never took part; the test proves nothing");

    // A count is served from an entry without reading a row back, so it is
    // where a shared scope shows first: B would count A's three pointers.
    // Before B's own row scans, which would replace A's entry.
    let fields = b.schema.fields.to_vec();
    let count = b
        .count_rows_in_txn(clause, &fields, fetch(&b), crate::core::mvcc::transaction::FROZEN_TX)
        .await
        .unwrap();
    assert_eq!(count, 5, "partition B counted A's cached rows");

    for _ in 0..2 {
        let found = rows(&b, clause).await;
        assert_eq!(tenants(&b, &found), vec![2; 5], "partition B was served A's cached rows");
    }

    // `cache == false` neither reads nor fills it.
    let (hits2, misses2, stores2) = crate::cache::query_cache::stats();
    let vis = a.visibility_for(crate::core::mvcc::transaction::FROZEN_TX);
    let found = a
        .query_row_with_visibility_cached(clause, &a.schema.fields.to_vec(), fetch(&a), fetch(&a), None, None, vis.clone(), false)
        .await
        .unwrap();
    assert_eq!(found.len(), 3);
    assert_eq!(crate::cache::query_cache::stats(), (hits2, misses2, stores2), "an uncached read touched the cache");

    // Nor do the count and the bytes reads, which the fan-out uses (§8).
    let fields = a.schema.fields.to_vec();
    let count = a
        .count_rows_with_visibility(clause, &fields, fetch(&a), vis.clone(), false)
        .await
        .unwrap();
    assert_eq!(count, 3);
    assert_eq!(crate::cache::query_cache::stats(), (hits2, misses2, stores2), "an uncached count touched the cache");
    let bytes = a
        .query_row_bytes_with_visibility(clause, &fields, fetch(&a), fetch(&a), None, None, vis.clone(), false)
        .await
        .unwrap();
    assert_eq!(crate::cache::query_cache::stats(), (hits2, misses2, stores2), "an uncached bytes read touched the cache");
    let cached_bytes = a
        .query_row_bytes_in_txn(clause, &fields, fetch(&a), fetch(&a), None, None, crate::core::mvcc::transaction::FROZEN_TX)
        .await
        .unwrap();
    assert_eq!(bytes, cached_bytes, "the uncached bytes read answered differently");
    let after_bytes = crate::cache::query_cache::stats();
    assert_ne!(after_bytes, (hits2, misses2, stores2), "a cached bytes read did not touch the cache; the next check proves nothing");
    // Uncached again, after the cached read above stamped an entry it could hit.
    let again = a
        .query_row_bytes_with_visibility(clause, &fields, fetch(&a), fetch(&a), None, None, vis, false)
        .await
        .unwrap();
    assert_eq!(again, cached_bytes);
    assert_eq!(crate::cache::query_cache::stats(), after_bytes, "an uncached bytes read touched the cache");
}

// ---------------------------------------------------------------------------
// The `_with_visibility` variants (§8).
// ---------------------------------------------------------------------------

/// Given the snapshot `_in_txn` would take, each variant answers exactly as
/// `_in_txn` does; given an older snapshot, it answers as of that one, which
/// is what lets a fan-out read every partition through one.
#[tokio::test(flavor = "multi_thread")]
async fn with_visibility_variants_match_in_txn() {
    init();
    let dir = tempfile::tempdir().unwrap();
    let l = create_logical(dir.path()).await;
    let t = open(&l, 0, PointRange::FULL).await;
    let manager = StdArc::new(TransactionManager::in_memory());
    t.enable_versioning(manager.clone());

    let committed = manager.begin();
    for i in 0..6 {
        t.insert_row_in_txn(row(&t, 1 + (i % 2) as i64, i, "c"), committed).await.unwrap();
    }
    manager.commit(committed);
    // In flight: visible to its own transaction only.
    let open_tx = manager.begin();
    for i in 10..13 {
        t.insert_row_in_txn(row(&t, 1, i, "u"), open_tx).await.unwrap();
    }
    let reader = manager.begin();
    let fields = t.schema.fields.to_vec();

    for clause in ["", "n >= 2", "tenant_id = 1", "n = 11", "n < 0"] {
        for tx in [reader, open_tx] {
            let expect_count = t.count_rows_in_txn(clause, &fields, fetch(&t), tx).await.unwrap();
            let expect_bytes = t
                .query_row_bytes_reporting_in_txn(clause, &fields, fetch(&t), fetch(&t), None, None, tx)
                .await
                .unwrap();
            for cache in [true, false] {
                let count = t
                    .count_rows_with_visibility(clause, &fields, fetch(&t), t.visibility_for(tx), cache)
                    .await
                    .unwrap();
                assert_eq!(count, expect_count, "count for {clause:?} as {tx}, cache {cache}");
                let bytes = t
                    .query_row_bytes_reporting_with_visibility(
                        clause,
                        &fields,
                        fetch(&t),
                        fetch(&t),
                        None,
                        None,
                        t.visibility_for(tx),
                        cache,
                    )
                    .await
                    .unwrap();
                assert_eq!(bytes, expect_bytes, "bytes for {clause:?} as {tx}, cache {cache}");
                let plain = t
                    .query_row_bytes_with_visibility(clause, &fields, fetch(&t), fetch(&t), None, None, t.visibility_for(tx), cache)
                    .await
                    .unwrap();
                assert_eq!(plain, expect_bytes.0);
            }
        }
    }

    // A snapshot taken before a commit keeps answering as of then.
    let early = t.visibility_for(reader);
    let before = t.count_rows_with_visibility("", &fields, fetch(&t), early.clone(), true).await.unwrap();
    assert_eq!(before, 6);
    manager.commit(open_tx);
    let late_reader = manager.begin();
    assert_eq!(t.count_rows_in_txn("", &fields, fetch(&t), late_reader).await.unwrap(), 9);
    assert_eq!(
        t.count_rows_with_visibility("", &fields, fetch(&t), early.clone(), true).await.unwrap(),
        6,
        "the count took a fresh snapshot instead of the one it was given"
    );
    let bytes = t
        .query_row_bytes_with_visibility("", &fields, fetch(&t), fetch(&t), None, None, early, true)
        .await
        .unwrap();
    let before_bytes = t
        .query_row_bytes_in_txn("n < 10", &fields, fetch(&t), fetch(&t), None, None, late_reader)
        .await
        .unwrap();
    assert_eq!(bytes.len(), before_bytes.len(), "the bytes read took a fresh snapshot instead of the one it was given");
}
