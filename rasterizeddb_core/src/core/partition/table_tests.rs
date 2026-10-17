//! `PartitionedTable` (§1.1, §3.2, §4.3, §12): insert and query agree on a
//! value's point (I2), materializing follows §3.2 once per partition, a
//! materialized partition's missing file refuses the table (I9), and a
//! reopen keeps rows and the materialized set. Built over real files in tiny
//! temp directories; no SQL reaches the table yet.

use std::path::Path;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicBool, Ordering};

use rclite::Arc;

use super::files::{self, PartFile};
use super::key::KeySpec;
use super::log::{self, FRAME_LEN, Record, ReshapeBegin, ReshapeTarget};
use super::map::RangeMap;
use super::route::Route;
use super::table::{
    DEFAULT_PARTITIONS, MAX_DECLARED_PER_DATABASE, MirrorsConfigured, OpenCtx, PartitionedTable, check_declared_cap,
    declared_in,
};
use crate::core::db_type::DbType;
use crate::core::mvcc::TransactionManager;
use crate::core::mvcc::transaction::{FROZEN_TX, TxId};
use crate::core::row::row::{Row, RowWrite};
use crate::core::row::schema::TableSchema;
use crate::core::row::table::Table;
use crate::core::rql::lexer_ct::CreateColumnData;
use crate::core::rql::lexer_s1::{InsertRowData, QueryPurpose};
use crate::core::storage_providers::backend::DbStorageProvider;
use crate::core::storage_providers::traits::StorageIO;
use crate::pg_params::{BoundParam, OID_INT4, OID_INT8, OID_TEXT, inline_params};

type P = DbStorageProvider;

const UID: u64 = 0x0000_7ab1_e000_0001;

fn init() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
}

struct Fixture {
    dir: tempfile::TempDir,
    seed: Arc<P>,
    tx: StdArc<TransactionManager>,
    /// What the pool reports about backup mirrors, flipped at runtime.
    mirrors: StdArc<AtomicBool>,
}

impl Fixture {
    fn mirrored(&self) -> MirrorsConfigured {
        let m = self.mirrors.clone();
        StdArc::new(move || m.load(Ordering::SeqCst))
    }
}

fn unmirrored() -> MirrorsConfigured {
    StdArc::new(|| false)
}

async fn fixture() -> Fixture {
    init();
    let dir = tempfile::tempdir().unwrap();
    let seed = Arc::new(P::new_file(dir.path().to_str().unwrap(), Some("seed.db")).await);
    Fixture {
        dir,
        seed,
        tx: StdArc::new(TransactionManager::in_memory()),
        mirrors: StdArc::new(AtomicBool::new(false)),
    }
}

fn col(name: &str, data_type: DbType) -> CreateColumnData {
    CreateColumnData {
        name: name.into(),
        data_type,
    }
}

fn columns(key_type: DbType) -> Vec<CreateColumnData> {
    vec![col("tenant_id", key_type), col("n", DbType::I32), col("label", DbType::STRING)]
}

/// S4's CREATE order, minus the catalogue: parent files, the schema, then
/// the map log and ids file.
async fn create(f: &Fixture, uid: u64, key_type: DbType, n: u32) -> PartitionedTable {
    files::create_parent_files(f.dir.path(), uid).unwrap();
    let io_schema = Arc::new(f.seed.create_new(format!("{}.db", files::schema_stem(uid))).await);
    let schema = Table::create_schema_files(io_schema, "acct", columns(key_type), &[]).await.unwrap();
    let spec = KeySpec::from_schema(&schema, &["tenant_id"]).unwrap();
    PartitionedTable::create_files(&f.seed, false, uid, &spec, n).await.unwrap();
    open(f, uid).await
}

async fn open(f: &Fixture, uid: u64) -> PartitionedTable {
    let io_schema = Arc::new(f.seed.create_new(format!("{}.db", files::schema_stem(uid))).await);
    let schema = TableSchema::load(io_schema.clone()).await.unwrap();
    let shell = Arc::new(Table::partition_shell(io_schema, schema));
    let ctx = OpenCtx {
        seed: &f.seed,
        db_id: 1,
        transactions: &f.tx,
        mirrored: f.mirrored(),
    };
    PartitionedTable::open(ctx, "acct", uid, shell).await
}

/// The row `INSERT INTO acct … VALUES (<tenant>, n, label)` writes, through
/// the real insert parser.
fn insert_row(t: &PartitionedTable, tenant: &str, n: i32, label: &str) -> Option<RowWrite> {
    let qp = QueryPurpose::InsertRow(InsertRowData {
        table_name: "acct".to_string(),
        query: format!("INSERT INTO acct (tenant_id, n, label) VALUES ({tenant}, {n}, '{label}')"),
        on_conflict_do_nothing: false,
        on_conflict_update: None,
    });
    crate::core::rql::lexer_ir::insert_row_with_defaults(&qp, &t.shell().schema.fields, &[], &[], 0)
}

fn row(t: &PartitionedTable, tenant: i64, n: i32) -> RowWrite {
    insert_row(t, &tenant.to_string(), n, "x").expect("the insert parser refused a plain row")
}

async fn insert(t: &PartitionedTable, tenant: i64, n: i32) {
    t.insert_row_in_txn(row(t, tenant, n), FROZEN_TX).await.unwrap();
}

fn point(t: &PartitionedTable, tenant: i64) -> u64 {
    t.point_of_write(&row(t, tenant, 0)).unwrap()
}

fn pid_of(t: &PartitionedTable, tenant: i64) -> u32 {
    t.epoch().unwrap().map.pid_of(point(t, tenant))
}

/// `clause` as the executor hands it to a table: `IN`, `BETWEEN` and the
/// like lowered by the rewriter.
fn lowered(t: &PartitionedTable, clause: &str) -> String {
    try_lowered(t, clause).pop().expect("the rewriter refused the clause")
}

/// [`lowered`], with the clauses the rewriter refuses (and so the executor
/// never routes) left out.
fn try_lowered(t: &PartitionedTable, clause: &str) -> Vec<String> {
    crate::core::tokenizer::predicate_rewrite::rewrite_where(clause, &t.shell().schema.fields)
        .into_iter()
        .collect()
}

/// Rows matching `clause` across the partitions it routes to, or the
/// evaluator's error.
async fn try_select(t: &PartitionedTable, clause: &str) -> std::io::Result<Vec<Row>> {
    let clause = try_lowered(t, clause)
        .pop()
        .ok_or_else(|| std::io::Error::other("the rewriter refused the clause"))?;
    let epoch = t.epoch().unwrap();
    let route = t.route_where(&clause).unwrap();
    let mut out = Vec::new();
    for (_, p) in t.materialized(&epoch, &route).unwrap() {
        let fetch = Table::<P>::build_full_row_fetch(&p.schema.fields);
        out.extend(
            p.query_row(&clause, &p.schema.fields.to_vec(), fetch.clone(), fetch, None, None)
                .await?,
        );
    }
    Ok(out)
}

async fn select(t: &PartitionedTable, clause: &str) -> Vec<Row> {
    try_select(t, clause).await.unwrap()
}

/// Every file of `uid` in `dir` that names a partition's data, by pid.
fn data_files_in(dir: &Path, uid: u64) -> Vec<(u32, String)> {
    let mut v: Vec<(u32, String)> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let name = e.unwrap().file_name().into_string().unwrap();
            match files::parse_name(&name)? {
                PartFile::Data { uid: u, pid } if u == uid => Some((pid, name)),
                _ => None,
            }
        })
        .collect();
    v.sort();
    v
}

/// The records in the map log's main file, read straight from disk.
fn records(dir: &Path, uid: u64) -> Vec<Record> {
    let buf = std::fs::read(dir.join(files::map_file(uid, 0))).unwrap();
    let mut at = 0;
    let mut out = Vec::new();
    while at + FRAME_LEN < buf.len() {
        let len = u32::from_le_bytes(buf[at..at + 4].try_into().unwrap()) as usize;
        out.push(log::decode(buf[at + FRAME_LEN], &buf[at + FRAME_LEN + 1..at + FRAME_LEN + len]).unwrap());
        at += FRAME_LEN + len;
    }
    out
}

fn materializes(dir: &Path, uid: u64) -> Vec<u32> {
    records(dir, uid)
        .into_iter()
        .filter_map(|r| match r {
            Record::Materialize { pid, .. } => Some(pid),
            _ => None,
        })
        .collect()
}

/// Deterministic values covering both signs, zero and the 32-bit edges.
fn values(count: usize) -> Vec<i64> {
    let mut v = vec![0, 1, -1, 42, i32::MAX as i64, i32::MIN as i64, 1 << 40, -(1 << 40)];
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    while v.len() < count {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        // Mostly 32-bit values, so the INT spellings apply to them too.
        v.push(if x % 4 == 0 { x as i64 } else { (x as i32) as i64 });
    }
    v
}

fn text_param(v: &str, oid: i32) -> BoundParam {
    BoundParam {
        value: Some(v.as_bytes().to_vec()),
        format: 0,
        oid,
    }
}

/// `tenant_id = <spelling>` for every spelling the evaluator calls equal to
/// the integer `v`, the prepared path's included.
fn int_spellings(v: i64) -> Vec<String> {
    let mut s = vec![
        format!("{v}"),
        format!("'{v}'"),
        format!("{v}::bigint"),
        format!("CAST({v} AS BIGINT)"),
    ];
    if i32::try_from(v).is_ok() {
        s.push(format!("{v}::int"));
    }
    for param in [
        text_param(&v.to_string(), 0),
        text_param(&v.to_string(), OID_INT8),
        text_param(&v.to_string(), OID_TEXT),
        BoundParam {
            value: Some(v.to_be_bytes().to_vec()),
            format: 1,
            oid: OID_INT8,
        },
    ] {
        s.push(inline_params("$1", &[param]).unwrap());
    }
    if let Ok(v32) = i32::try_from(v) {
        s.push(inline_params("$1", &[text_param(&v.to_string(), OID_INT4)]).unwrap());
        s.push(
            inline_params(
                "$1",
                &[BoundParam {
                    value: Some(v32.to_be_bytes().to_vec()),
                    format: 1,
                    oid: OID_INT4,
                }],
            )
            .unwrap(),
        );
    }
    s
}

// ---------------------------------------------------------------------------
// I2: insert and query agree on a value's point.
// ---------------------------------------------------------------------------

/// Checks every spelling of each value routes to a set holding the point the
/// inserted row has, and that the plain spelling narrows (a router that
/// answered `All` to everything would pass the first half vacuously).
/// Returns how many routes narrowed.
fn check_int_key(t: &PartitionedTable, vals: &[i64], insert_spellings: &[fn(i64) -> String]) -> usize {
    let mut narrowed = 0;
    for &v in vals {
        let p = point(t, v);
        // The insert parser takes every spelling to the same point too.
        for spell in insert_spellings {
            if let Some(w) = insert_row(t, &spell(v), 0, "x") {
                assert_eq!(t.point_of_write(&w).unwrap(), p, "INSERT of {} moved the point", spell(v));
            }
        }
        for s in int_spellings(v) {
            // Raw, and as the executor will hand it over.
            let raw = format!("tenant_id = {s}");
            for clause in try_lowered(t, &raw).into_iter().chain([raw]) {
                let route = t.route_where(&clause).unwrap();
                assert!(route.contains(p), "{clause} routes to {route:?}, missing the row's point {p:#018x}");
                if route != Route::All {
                    narrowed += 1;
                }
            }
        }
        let plain = t.route_where(&format!("tenant_id = {v}")).unwrap();
        assert!(matches!(plain, Route::Points(_)), "tenant_id = {v} did not narrow: {plain:?}");
    }
    narrowed
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_and_query_agree_on_points_for_every_spelling() {
    let f = fixture().await;
    let vals = values(200);

    let big = create(&f, UID, DbType::I64, 64).await;
    let narrowed = check_int_key(
        &big,
        &vals,
        &[|v| format!("'{v}'"), |v| format!("{v}::bigint"), |v| {
            if i32::try_from(v).is_ok() { format!("{v}::int") } else { format!("{v}") }
        }],
    );
    println!("BIGINT key: {narrowed} spellings narrowed");

    // INT key column: the BIGINT spellings of an in-range value meet it.
    let int = create(&f, UID + 1, DbType::I32, 64).await;
    let small: Vec<i64> = vals.iter().copied().filter(|v| i32::try_from(*v).is_ok()).collect();
    let narrowed = check_int_key(&int, &small, &[|v| format!("'{v}'"), |v| format!("{v}::bigint")]);
    println!("INT key: {narrowed} spellings narrowed");

    // TEXT key.
    let text = create(&f, UID + 2, DbType::STRING, 64).await;
    for v in vals.iter().take(50) {
        let name = format!("tenant {v} O''Brien");
        let w = insert_row(&text, &format!("'{name}'"), 0, "x").unwrap();
        let p = text.point_of_write(&w).unwrap();
        let unescaped = name.replace("''", "'");
        let mut spellings = vec![format!("'{name}'"), format!("'{name}'::text"), format!("CAST('{name}' AS TEXT)")];
        for oid in [0, OID_TEXT] {
            spellings.push(inline_params("$1", &[text_param(&unescaped, oid)]).unwrap());
        }
        for s in spellings {
            let raw = format!("tenant_id = {s}");
            for clause in try_lowered(&text, &raw).into_iter().chain([raw]) {
                let route = text.route_where(&clause).unwrap();
                assert!(route.contains(p), "{clause} routes to {route:?}, missing {p:#018x}");
            }
        }
        let plain = text.route_where(&format!("tenant_id = '{name}'")).unwrap();
        assert!(matches!(plain, Route::Points(_)), "text equality did not narrow: {plain:?}");
    }

    // End to end: rows stored through the table are found through the route.
    for &v in vals.iter().take(6) {
        big.insert_row_in_txn(row(&big, v, 7), FROZEN_TX).await.unwrap();
    }
    // A spelling the evaluator itself rejects is no routing question; the
    // plain one must be answered.
    let mut answered = 0;
    for &v in vals.iter().take(6) {
        assert_eq!(select(&big, &format!("tenant_id = {v}")).await.len(), 1, "tenant_id = {v}");
        for s in int_spellings(v) {
            if let Ok(found) = try_select(&big, &format!("tenant_id = {s}")).await {
                assert_eq!(found.len(), 1, "tenant_id = {s} found {} rows", found.len());
                answered += 1;
            }
        }
    }
    println!("end to end: {answered} spellings answered");
}

// ---------------------------------------------------------------------------
// Materializing (§3.2, §4.3).
// ---------------------------------------------------------------------------

/// The first write to a partition creates exactly its three files (each with
/// its staging sibling) and one `MATERIALIZE`, in the log's main file at
/// once; later writes create nothing; other partitions have no files and
/// are skipped by `materialized` until written.
#[tokio::test(flavor = "multi_thread")]
async fn materializing_creates_three_files_and_one_record() {
    let f = fixture().await;
    let d = f.dir.path();
    let t = create(&f, UID, DbType::I64, 8).await;
    assert!(data_files_in(d, UID).is_empty(), "CREATE made partition files");
    assert!(materializes(d, UID).is_empty());
    let epoch = t.epoch().unwrap();
    assert!(t.materialized(&epoch, &Route::All).unwrap().is_empty());

    let pid = pid_of(&t, 42);
    insert(&t, 42, 1).await;
    let mut want: Vec<(u32, String)> = files::data_files(UID, pid)
        .into_iter()
        .flat_map(|n| [(pid, n.clone()), (pid, format!("{n}.tmp"))])
        .collect();
    want.sort();
    assert_eq!(data_files_in(d, UID), want);
    assert_eq!(materializes(d, UID), vec![pid]);

    // A second write to the partition, by another tenant, creates nothing.
    let neighbour = (1..10_000i64).find(|v| *v != 42 && pid_of(&t, *v) == pid).unwrap();
    insert(&t, 42, 2).await;
    insert(&t, neighbour, 3).await;
    assert_eq!(data_files_in(d, UID), want);
    assert_eq!(materializes(d, UID), vec![pid]);

    let all: Vec<u32> = t.materialized(&epoch, &Route::All).unwrap().into_iter().map(|(p, _)| p).collect();
    assert_eq!(all, vec![pid]);
    let elsewhere = (1..10_000i64).find(|v| pid_of(&t, *v) != pid).unwrap();
    let route = t.route_where(&format!("tenant_id = {elsewhere}")).unwrap();
    assert!(t.materialized(&epoch, &route).unwrap().is_empty());
    assert!(select(&t, &format!("tenant_id = {elsewhere}")).await.is_empty());
    assert_eq!(select(&t, "tenant_id = 42").await.len(), 2);

    let totals = t.totals().await.unwrap();
    assert_eq!((totals.materialized, totals.declared, totals.slots), (1, 8, 3));
    assert_eq!(totals.ids_handed_out, 3);
    let infos = t.partitions().await.unwrap();
    assert_eq!(infos.len(), 8);
    for info in infos {
        let mine = info.pid == pid;
        assert_eq!(info.state, if mine { "materialized" } else { "unmaterialized" });
        assert_eq!(info.location.is_some(), mine);
        assert_eq!(info.slots, if mine { 3 } else { 0 });
    }
}

/// Concurrent first writes to one partition materialize it once: one
/// record, and every row lands.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_first_writes_materialize_once() {
    let f = fixture().await;
    let t = StdArc::new(create(&f, UID, DbType::I64, 8).await);
    let pid = pid_of(&t, 1);
    let tenants: Vec<i64> = (1..100_000i64).filter(|v| pid_of(&t, *v) == pid).take(16).collect();
    let mut tasks = Vec::new();
    for tenant in tenants.clone() {
        let t = t.clone();
        tasks.push(tokio::spawn(async move {
            t.insert_row_in_txn(row(&t, tenant, 1), FROZEN_TX).await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert_eq!(materializes(f.dir.path(), UID), vec![pid]);
    let found = select(&t, &format!("tenant_id IN ({})", tenants.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", "))).await;
    assert_eq!(found.len(), 16);
}

/// A partition whose `MATERIALIZE` was logged but whose open failed opens its
/// own files on the next write: no second record, which replay would refuse
/// at every later start, and no files deleted.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_open_after_materialize_is_retried_without_a_second_record() {
    let f = fixture().await;
    let d = f.dir.path();
    let t = create(&f, UID, DbType::I64, 8).await;
    let pid = pid_of(&t, 42);

    crate::core::row::table::partition_test_hooks::FAIL_POINTER_OPEN.with(|c| c.set(true));
    let first = t.insert_row_in_txn(row(&t, 42, 1), FROZEN_TX).await;
    crate::core::row::table::partition_test_hooks::FAIL_POINTER_OPEN.with(|c| c.set(false));
    assert!(first.is_err(), "the injected open failure did not reach the caller");
    assert_eq!(materializes(d, UID), vec![pid]);

    insert(&t, 42, 2).await;
    assert_eq!(materializes(d, UID), vec![pid]);
    assert_eq!(select(&t, "tenant_id = 42").await.len(), 1);
    drop(t);
    let t = open(&f, UID).await;
    assert!(t.broken().is_none(), "{:?}", t.broken());
    assert_eq!(select(&t, "tenant_id = 42").await.len(), 1);
}

// ---------------------------------------------------------------------------
// Opening: reopen and I9.
// ---------------------------------------------------------------------------

/// A reopen opens exactly the materialized partitions, with their rows, and
/// the id sequence continues past every id taken.
#[tokio::test(flavor = "multi_thread")]
async fn reopen_keeps_rows_and_the_materialized_set() {
    let f = fixture().await;
    let t = create(&f, UID, DbType::I64, 16).await;
    for tenant in 1..=20i64 {
        insert(&t, tenant, tenant as i32).await;
        insert(&t, tenant, -(tenant as i32)).await;
    }
    let before = t.partitions().await.unwrap();
    let ids = t.totals().await.unwrap().ids_handed_out;
    drop(t);

    let t = open(&f, UID).await;
    assert!(t.broken().is_none(), "{:?}", t.broken());
    assert_eq!(t.partitions().await.unwrap(), before);
    for tenant in 1..=20i64 {
        assert_eq!(select(&t, &format!("tenant_id = {tenant}")).await.len(), 2, "tenant {tenant}");
    }
    assert_eq!(select(&t, "").await.len(), 40);
    assert!(t.totals().await.unwrap().ids_handed_out >= ids);
    insert(&t, 99, 0).await;
    assert_eq!(select(&t, "").await.len(), 41);
}

/// A materialized partition's missing pointer file, a missing ids file or a
/// missing map log refuses the table with an error naming the file, and the
/// open does not create it (I9).
#[tokio::test(flavor = "multi_thread")]
async fn a_missing_file_refuses_the_table_and_is_not_recreated() {
    let f = fixture().await;
    let d = f.dir.path();
    let t = create(&f, UID, DbType::I64, 8).await;
    insert(&t, 42, 1).await;
    let pid = pid_of(&t, 42);
    drop(t);

    let pointers = files::data_files(UID, pid)[1].clone();
    for missing in [pointers, files::ids_file(UID), files::map_file(UID, 0)] {
        let saved = std::fs::read(d.join(&missing)).unwrap();
        std::fs::remove_file(d.join(&missing)).unwrap();

        let t = open(&f, UID).await;
        let why = t.broken().unwrap_or_else(|| panic!("opened without {missing}")).to_string();
        assert!(why.contains(&missing), "{why} does not name {missing}");
        assert!(!d.join(&missing).exists(), "opening recreated {missing}");
        let e = t.insert_row_in_txn(row(&t, 42, 2), FROZEN_TX).await.unwrap_err().to_string();
        assert!(e.contains(&missing), "{e}");
        drop(t);

        std::fs::write(d.join(&missing), saved).unwrap();
        let t = open(&f, UID).await;
        assert!(t.broken().is_none(), "{:?}", t.broken());
    }
}

/// A broken table refuses every data call with its reason; its shell still
/// answers schema questions.
#[tokio::test(flavor = "multi_thread")]
async fn a_broken_table_refuses_every_data_call() {
    let f = fixture().await;
    let t = create(&f, UID, DbType::I64, 8).await;
    insert(&t, 42, 1).await;
    let shape = row(&t, 42, 1);
    let epoch = t.epoch().unwrap();
    drop(t);
    std::fs::remove_file(f.dir.path().join(files::ids_file(UID))).unwrap();

    let t = open(&f, UID).await;
    let why = t.broken().expect("opened without its ids file").to_string();
    let same = |e: std::io::Error| assert_eq!(e.to_string(), why);
    same(t.insert_row_in_txn(shape, FROZEN_TX).await.unwrap_err());
    same(t.route_where("tenant_id = 42").unwrap_err());
    same(t.epoch().unwrap_err());
    same(t.spec().unwrap_err());
    same(t.materialized(&epoch, &Route::All).err().unwrap());
    same(t.partition_for_point(0).await.err().unwrap());
    same(t.totals().await.unwrap_err());
    same(t.partitions().await.unwrap_err());
    same(t.check().unwrap_err());
    assert_eq!(t.shell().schema.name, "acct");
    // The log replayed, so the count is known even though the table is broken.
    assert_eq!(t.declared(), Some(8));
}

/// Phase 2a partitions plain files only: an in-memory database's seed is
/// refused at CREATE and leaves an open broken.
#[tokio::test(flavor = "multi_thread")]
async fn in_memory_storage_is_refused() {
    init();
    let seed = Arc::new(P::new_memory("mem"));
    let schema = {
        let io = Arc::new(seed.create_new("s.db".to_string()).await);
        Table::create_schema_files(io, "acct", columns(DbType::I64), &[]).await.unwrap()
    };
    let spec = KeySpec::from_schema(&schema, &["tenant_id"]).unwrap();
    let e = PartitionedTable::create_files(&seed, false, UID, &spec, 8).await.unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::Unsupported, "{e}");
    assert!(e.to_string().contains("not supported"), "{e}");

    let io_schema = Arc::new(seed.create_new("s.db".to_string()).await);
    let shell = Arc::new(Table::partition_shell(io_schema, schema));
    let tx = StdArc::new(TransactionManager::in_memory());
    let t = PartitionedTable::open(
        OpenCtx {
            seed: &seed,
            db_id: 1,
            transactions: &tx,
            mirrored: unmirrored(),
        },
        "acct",
        UID,
        shell,
    )
    .await;
    assert!(t.broken().is_some_and(|w| w.contains("not supported")), "{:?}", t.broken());
}

/// DROP's file steps remove every file of the uid, leave other tables'
/// alone, and refuse data calls after; a broken table drops too.
#[tokio::test(flavor = "multi_thread")]
async fn drop_files_removes_every_file_of_the_uid() {
    let f = fixture().await;
    let d = f.dir.path();
    let t = create(&f, UID, DbType::I64, 8).await;
    let other = create(&f, UID + 1, DbType::I64, 8).await;
    insert(&t, 42, 1).await;
    insert(&other, 42, 1).await;

    t.drop_files().await.unwrap();
    let left: Vec<String> = std::fs::read_dir(d)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.starts_with(&format!("__pt_{UID:016x}_")))
        .collect();
    assert!(left.is_empty(), "left behind: {left:?}");
    assert!(t.insert_row_in_txn(row(&t, 42, 2), FROZEN_TX).await.is_err(), "a dropped table took a row");
    assert!(t.check().is_err());
    assert_eq!(select(&other, "tenant_id = 42").await.len(), 1);

    // A broken table: its map log is gone, so no DROPPED is written, and its
    // other files still go.
    drop(other);
    std::fs::remove_file(d.join(files::map_file(UID + 1, 0))).unwrap();
    let broken = open(&f, UID + 1).await;
    assert!(broken.broken().is_some());
    broken.drop_files().await.unwrap();
    assert!(std::fs::read_dir(d).unwrap().all(|e| {
        files::parse_name(e.unwrap().file_name().to_str().unwrap()).is_none_or(|p| p.uid() != UID + 1)
    }));
}

// ---------------------------------------------------------------------------
// Routing and the cap.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn route_where_narrows_only_on_key_predicates() {
    let f = fixture().await;
    let t = create(&f, UID, DbType::I64, DEFAULT_PARTITIONS).await;
    let map = t.epoch().unwrap().map.clone();
    let pids = |clause: &str| t.route_where(clause).unwrap().pids(&map);

    assert_eq!(t.route_where("1 = 0").unwrap(), Route::Zero);
    assert_eq!(t.route_where("tenant_id = 1 AND 1 = 0").unwrap(), Route::Zero);
    for clause in ["", "n = 5", "label = 'x'", "tenant_id > 5", "tenant_id = 1 OR n = 2", "tenant_id = ("] {
        assert_eq!(t.route_where(clause).unwrap(), Route::All, "{clause}");
    }
    assert_eq!(pids("tenant_id = 42"), vec![pid_of(&t, 42)]);
    assert_eq!(pids("tenant_id = 42 AND n = 3"), vec![pid_of(&t, 42)]);
    let list = [1i64, 2, 3, 500, 77_777];
    let mut want: Vec<u32> = list.iter().map(|v| pid_of(&t, *v)).collect();
    want.sort_unstable();
    want.dedup();
    assert_eq!(pids(&lowered(&t, "tenant_id IN (1, 2, 3, 500, 77777)")), want);
    assert_eq!(pids("tenant_id = 1 OR tenant_id = 500"), {
        let mut w = vec![pid_of(&t, 1), pid_of(&t, 500)];
        w.sort_unstable();
        w.dedup();
        w
    });
}

#[test]
fn the_declared_partition_cap_is_per_database_and_inclusive() {
    assert_eq!(MAX_DECLARED_PER_DATABASE, 2048);
    assert_eq!(DEFAULT_PARTITIONS, 64);
    check_declared_cap(0, 2048).unwrap();
    check_declared_cap(1024, 1024).unwrap();
    check_declared_cap(2048 - 64, 64).unwrap();
    for (declared, adding) in [(0, 2049), (2048 - 63, 64), (2048, 1), (u64::MAX, 1)] {
        let e = check_declared_cap(declared, adding).unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::Unsupported);
        assert!(e.to_string().contains("not supported"), "{e}");
    }
    // A map of the default size fits 32 times.
    let mut declared = 0u64;
    for _ in 0..32 {
        check_declared_cap(declared, DEFAULT_PARTITIONS).unwrap();
        declared += DEFAULT_PARTITIONS as u64;
    }
    assert!(check_declared_cap(declared, 1).is_err());
    let _ = RangeMap::initial(DEFAULT_PARTITIONS).unwrap();
}

/// A torn map-log tail is cut and the table opens, but says so: reaping
/// must stay off while the evidence file exists (§12).
#[tokio::test(flavor = "multi_thread")]
async fn a_torn_map_tail_opens_and_says_so() {
    let f = fixture().await;
    let d = f.dir.path();
    let t = create(&f, UID, DbType::I64, 8).await;
    assert!(t.torn().is_none());
    insert(&t, 42, 1).await;
    drop(t);
    {
        use std::io::Write;
        let mut log = std::fs::OpenOptions::new().append(true).open(d.join(files::map_file(UID, 0))).unwrap();
        log.write_all(&[7, 0, 0, 0, 1, 2]).unwrap();
    }
    let t = open(&f, UID).await;
    assert!(t.broken().is_none(), "{:?}", t.broken());
    assert!(t.torn().is_some(), "a torn tail went unreported");
    assert_eq!(select(&t, "tenant_id = 42").await.len(), 1);
}

/// A map header whose key the schema no longer has with the same family
/// leaves the table broken: routing on it would put rows where no query
/// looks.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_the_schema_disagrees_with_refuses_the_table() {
    let f = fixture().await;
    let t = create(&f, UID, DbType::I64, 8).await;
    drop(t);
    let io = Arc::new(f.seed.create_new("other_schema.db".to_string()).await);
    let schema = Table::create_schema_files(io.clone(), "acct", columns(DbType::STRING), &[]).await.unwrap();
    let shell = Arc::new(Table::partition_shell(io, schema));
    let ctx = OpenCtx {
        seed: &f.seed,
        db_id: 1,
        transactions: &f.tx,
        mirrored: f.mirrored(),
    };
    let t = PartitionedTable::open(ctx, "acct", UID, shell).await;
    let why = t.broken().expect("opened with a key of another family");
    assert!(why.contains("disagrees"), "{why}");
}

// ---------------------------------------------------------------------------
// A torn map log (§12): leftovers prove nothing.
// ---------------------------------------------------------------------------

/// Flips the last byte of the map log's main file: its final record then
/// fails the CRC, which replay cannot tell from a torn tail.
fn damage_last_record(dir: &Path, uid: u64) {
    let path = dir.join(files::map_file(uid, 0));
    let mut buf = std::fs::read(&path).unwrap();
    *buf.last_mut().unwrap() ^= 0xff;
    std::fs::write(&path, buf).unwrap();
}

fn file_bytes(dir: &Path, uid: u64, pid: u32) -> Vec<Vec<u8>> {
    files::data_files(uid, pid).iter().map(|n| std::fs::read(dir.join(n)).unwrap()).collect()
}

fn set_file_bytes(dir: &Path, uid: u64, pid: u32, bytes: &[Vec<u8>]) {
    for (n, b) in files::data_files(uid, pid).iter().zip(bytes) {
        std::fs::write(dir.join(n), b).unwrap();
    }
}

/// Media damage to the last record reads as a torn tail, so a materialized
/// partition can lose its `MATERIALIZE` while its rows are committed. The
/// table must refuse, not skip the files as rowless (a subset) nor delete
/// them at the next first write, and keep refusing while the evidence file
/// is there.
#[tokio::test(flavor = "multi_thread")]
async fn a_torn_log_refuses_a_partition_with_data_but_no_materialize() {
    let f = fixture().await;
    let d = f.dir.path();
    let t = create(&f, UID, DbType::I64, 8).await;
    insert(&t, 42, 1).await;
    let pid = pid_of(&t, 42);
    drop(t);
    assert!(matches!(records(d, UID).last(), Some(Record::Materialize { pid: p, .. }) if *p == pid));
    let before = file_bytes(d, UID, pid);
    assert!(before.iter().any(|b| !b.is_empty()), "the partition's files hold nothing");
    damage_last_record(d, UID);

    for _ in 0..2 {
        let t = open(&f, UID).await;
        assert!(t.torn().is_some());
        let why = t.broken().expect("a torn log hid a partition with rows").to_string();
        assert!(why.contains("torn") && why.contains(&format!("partition {pid} ")), "{why}");
        assert!(t.insert_row_in_txn(row(&t, 42, 2), FROZEN_TX).await.is_err());
        assert_eq!(file_bytes(d, UID, pid), before, "the partition's files were changed");
    }
}

/// The write path refuses on its own: files that hold data when a first
/// write comes, after an open that saw them empty, are not deleted. Empty
/// leftovers, a crash between creating the files and logging the record,
/// are recreated as usual.
#[tokio::test(flavor = "multi_thread")]
async fn a_torn_log_keeps_materialize_from_deleting_files_with_data() {
    let f = fixture().await;
    let d = f.dir.path();
    let t = create(&f, UID, DbType::I64, 8).await;
    insert(&t, 42, 1).await;
    let pid = pid_of(&t, 42);
    drop(t);
    damage_last_record(d, UID);
    let saved = file_bytes(d, UID, pid);
    let empty = vec![Vec::new(); 3];
    set_file_bytes(d, UID, pid, &empty);

    let t = open(&f, UID).await;
    assert!(t.broken().is_none(), "{:?}", t.broken());
    assert!(t.torn().is_some());
    set_file_bytes(d, UID, pid, &saved);
    let e = t.insert_row_in_txn(row(&t, 42, 2), FROZEN_TX).await.unwrap_err().to_string();
    assert!(e.contains("torn") && e.contains(&format!("partition {pid} ")), "{e}");
    assert_eq!(file_bytes(d, UID, pid), saved, "materialize deleted files with data");
    assert!(materializes(d, UID).is_empty());

    set_file_bytes(d, UID, pid, &empty);
    insert(&t, 42, 3).await;
    assert_eq!(materializes(d, UID), vec![pid]);
    assert_eq!(select(&t, "tenant_id = 42").await.len(), 1);
}

// ---------------------------------------------------------------------------
// Logs this phase cannot open: only open_live refuses them, since replay
// accepts each.
// ---------------------------------------------------------------------------

/// Opens `uid`'s map log, appends what `recs` builds from its replayed state,
/// then opens the table.
async fn reopen_after(f: &Fixture, uid: u64, recs: impl FnOnce(&log::MapState) -> Vec<Record>) -> PartitionedTable {
    let io = StdArc::new(f.seed.create_new(files::map_file(uid, 0)).await);
    let (map_log, replay) = files::MapLogFile::open(io).await.unwrap();
    for rec in recs(&replay.state) {
        map_log.append_durable(&rec).await.unwrap();
    }
    drop(map_log);
    open(f, uid).await
}

/// `t` is broken for a reason containing `want`, and every data call returns
/// exactly that reason.
async fn assert_refused(t: &PartitionedTable, want: &str) {
    let why = t.broken().unwrap_or_else(|| panic!("opened a table that should be refused for {want:?}")).to_string();
    assert!(why.contains(want), "{why}");
    let same = |e: std::io::Error| assert_eq!(e.to_string(), why);
    same(t.insert_row_in_txn(row(t, 42, 1), FROZEN_TX).await.unwrap_err());
    same(t.route_where("tenant_id = 42").unwrap_err());
    same(t.epoch().unwrap_err());
    same(t.partition_for_point(0).await.err().unwrap());
    same(t.totals().await.unwrap_err());
    same(t.partitions().await.unwrap_err());
    same(t.check().unwrap_err());
}

/// A copied or foreign map log would route rows by another table's map.
#[tokio::test(flavor = "multi_thread")]
async fn a_map_log_of_another_uid_refuses_the_table() {
    let f = fixture().await;
    let d = f.dir.path();
    drop(create(&f, UID, DbType::I64, 8).await);
    drop(create(&f, UID + 1, DbType::I64, 8).await);
    std::fs::copy(d.join(files::map_file(UID + 1, 0)), d.join(files::map_file(UID, 0))).unwrap();
    assert_refused(&open(&f, UID).await, &format!("belongs to table {:016x}", UID + 1)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_map_log_refuses_the_table() {
    let f = fixture().await;
    let t = create(&f, UID, DbType::I64, 8).await;
    insert(&t, 42, 1).await;
    drop(t);
    let t = reopen_after(&f, UID, |_| vec![Record::Dropped]).await;
    assert_refused(&t, "dropped").await;
}

/// A later phase's reshape, open, abandoned or committed: routing by the
/// placed map would put rows where the log says no partition is.
#[tokio::test(flavor = "multi_thread")]
async fn a_reshaped_map_log_refuses_the_table() {
    let f = fixture().await;
    let split = |state: &log::MapState| {
        Record::ReshapeBegin(ReshapeBegin {
            id: 1,
            sources: vec![0],
            targets: vec![ReshapeTarget {
                lo: state.map().range_of(0).unwrap().lo,
                pid: 8,
                loc: 0,
            }],
        })
    };
    let cases: [(&str, Box<dyn Fn(&log::MapState) -> Vec<Record>>); 3] = [
        // Open: a reshape listed, and next_pid past the map.
        ("open", Box::new(move |s| vec![split(s)])),
        // Abandoned: only next_pid says so.
        ("aborted", Box::new(move |s| vec![split(s), Record::ReshapeAbort { id: 1 }])),
        // Committed: pid 0 retired and pid 8 in its place.
        ("committed", Box::new(move |s| vec![split(s), Record::ReshapeCommit { id: 1, pins: vec![(8, None)] }])),
    ];
    for (i, (case, recs)) in cases.into_iter().enumerate() {
        let uid = UID + i as u64;
        drop(create(&f, uid, DbType::I64, 8).await);
        let t = reopen_after(&f, uid, |s| recs(s)).await;
        println!("{case}: {:?}", t.broken());
        assert_refused(&t, "reshaped").await;
    }
}

/// Phase 2a opens partitions only at the root: one recorded elsewhere would
/// be opened from the wrong directory, or not at all.
#[tokio::test(flavor = "multi_thread")]
async fn a_partition_outside_the_root_refuses_the_table() {
    let f = fixture().await;
    drop(create(&f, UID, DbType::I64, 8).await);
    let t = reopen_after(&f, UID, |s| {
        let mut locations = s.locations().to_vec();
        locations.push("/elsewhere".to_string());
        vec![Record::Locations(locations), Record::Materialize { pid: 3, floor: 0, loc: 1 }]
    })
    .await;
    assert_refused(&t, "partition 3 is at location 1, outside the root").await;
}

// ---------------------------------------------------------------------------
// MVCC: every partition is versioned and registered (§3.2 step 3).
// ---------------------------------------------------------------------------

/// Rows matching `clause` that `reader`'s snapshot sees, through each
/// partition's own visibility.
async fn count_as(t: &PartitionedTable, clause: &str, reader: TxId) -> usize {
    let epoch = t.epoch().unwrap();
    let route = t.route_where(clause).unwrap();
    let mut n = 0;
    for (_, p) in t.materialized(&epoch, &route).unwrap() {
        let fetch = Table::<P>::build_full_row_fetch(&p.schema.fields);
        n += p
            .query_row_in_txn(clause, &p.schema.fields.to_vec(), fetch.clone(), fetch, None, None, reader)
            .await
            .unwrap()
            .len();
    }
    n
}

fn assert_registered(t: &PartitionedTable) {
    let all = t.materialized(&t.epoch().unwrap(), &Route::All).unwrap();
    assert!(!all.is_empty());
    for (pid, p) in all {
        assert!(p.forget_horizon().is_some(), "partition {pid} is not in the forget registry");
    }
}

/// A row written under a transaction is invisible to others until it
/// commits and to everyone once it aborts, on a freshly materialized
/// partition and after a reopen; each partition holds a forget handle.
#[tokio::test(flavor = "multi_thread")]
async fn partitions_are_versioned_and_registered() {
    let f = fixture().await;
    let t = create(&f, UID, DbType::I64, 8).await;
    let q = "tenant_id = 42";

    let writer = f.tx.begin();
    t.insert_row_in_txn(row(&t, 42, 1), writer).await.unwrap();
    assert_registered(&t);
    let other = f.tx.begin();
    assert_eq!(count_as(&t, q, writer).await, 1, "the writer cannot see its own row");
    assert_eq!(count_as(&t, q, other).await, 0, "an uncommitted row is visible to another transaction");
    f.tx.abort(writer);
    assert_eq!(count_as(&t, q, other).await, 0, "a rolled-back row is visible");
    f.tx.commit(other);

    let writer = f.tx.begin();
    t.insert_row_in_txn(row(&t, 42, 2), writer).await.unwrap();
    f.tx.commit(writer);
    let reader = f.tx.begin();
    assert_eq!(count_as(&t, q, reader).await, 1, "a committed row is missing");
    f.tx.commit(reader);
    drop(t);

    let t = open(&f, UID).await;
    assert!(t.broken().is_none(), "{:?}", t.broken());
    assert_registered(&t);
    let writer = f.tx.begin();
    t.insert_row_in_txn(row(&t, 42, 3), writer).await.unwrap();
    let reader = f.tx.begin();
    assert_eq!(count_as(&t, q, reader).await, 1, "after reopen, an uncommitted row is visible");
    f.tx.abort(writer);
    f.tx.commit(reader);
}

// ---------------------------------------------------------------------------
// Mirrors and the cap's inputs.
// ---------------------------------------------------------------------------

/// Pools with backup mirrors are refused (phase 2a), as the pool reports
/// them at each call: a mirror added at runtime leaves the root seed a plain
/// file provider, and a partition materialized then would go unmirrored.
#[tokio::test(flavor = "multi_thread")]
async fn backup_mirrors_are_refused_even_when_added_at_runtime() {
    let f = fixture().await;
    let d = f.dir.path();
    let unsupported = |e: std::io::Error| {
        assert_eq!(e.kind(), std::io::ErrorKind::Unsupported, "{e}");
        assert!(e.to_string().contains("not supported"), "{e}");
    };

    files::create_parent_files(d, UID).unwrap();
    let io_schema = Arc::new(f.seed.create_new(format!("{}.db", files::schema_stem(UID))).await);
    let schema = Table::create_schema_files(io_schema, "acct", columns(DbType::I64), &[]).await.unwrap();
    let spec = KeySpec::from_schema(&schema, &["tenant_id"]).unwrap();
    unsupported(PartitionedTable::create_files(&f.seed, true, UID, &spec, 8).await.unwrap_err());
    assert_eq!(std::fs::metadata(d.join(files::map_file(UID, 0))).unwrap().len(), 0);

    let t = create(&f, UID + 1, DbType::I64, 8).await;
    f.mirrors.store(true, Ordering::SeqCst);
    assert_eq!(f.seed.kind(), "file");
    unsupported(t.insert_row_in_txn(row(&t, 42, 1), FROZEN_TX).await.unwrap_err());
    assert!(data_files_in(d, UID + 1).is_empty(), "a partition was created unmirrored");
    assert!(materializes(d, UID + 1).is_empty());
    drop(t);

    let t = open(&f, UID + 1).await;
    assert!(t.broken().is_some_and(|w| w.contains("not supported")), "{:?}", t.broken());
    drop(t);
    f.mirrors.store(false, Ordering::SeqCst);
    let t = open(&f, UID + 1).await;
    insert(&t, 42, 1).await;
    assert_eq!(select(&t, "tenant_id = 42").await.len(), 1);
}

/// The cap counts replicated state only: a table whose map log did not
/// replay has an unknown count, never 0, and the database's sum refuses
/// while one is unknown.
#[tokio::test(flavor = "multi_thread")]
async fn an_unreplayed_map_log_leaves_the_declared_count_unknown() {
    let f = fixture().await;
    let a = create(&f, UID, DbType::I64, 8).await;
    let b = create(&f, UID + 1, DbType::I64, 16).await;
    assert_eq!((a.declared(), b.declared()), (Some(8), Some(16)));
    assert_eq!(declared_in([&a, &b]).unwrap(), 24);
    drop(b);

    std::fs::remove_file(f.dir.path().join(files::map_file(UID + 1, 0))).unwrap();
    let b = open(&f, UID + 1).await;
    assert!(b.broken().is_some());
    assert_eq!(b.declared(), None);
    let e = declared_in([&a, &b]).unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::Unsupported, "{e}");
    assert!(e.to_string().contains("not supported"), "{e}");
}
