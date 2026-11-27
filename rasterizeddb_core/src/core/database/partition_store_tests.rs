//! Partitions on a key/value store (docs/kv-partitions.md §1), API level:
//! a table placed with `STORAGE PLACE TABLE t AT 'kv:<store>'` before its
//! CREATE keeps its partitions' row and pointer files on the store (a local
//! directory store) and its parent files in the root; a restart, an idle
//! close and a reopen find them there; a partition file missing from the
//! store refuses the table instead of reading as empty (I9); DROP drops them
//! from the store; the startup reaper drops a store leftover no
//! `MATERIALIZE` names; and a store holding partitions cannot be removed.

use std::path::{Path, PathBuf};
use std::time::Duration;

use rclite::Arc;

use super::*;
use crate::core::mvcc::transaction::FROZEN_TX;
use crate::core::partition::files::{self, PartFile};
use crate::core::partition::route::Route;
use crate::core::partition::table::PartitionedTable;
use crate::core::rql::lexer_ct::CreateColumnData;
use crate::core::rql::lexer_s1::{InsertRowData, QueryPurpose};

fn init() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
}

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

/// A database directory and a store directory, apart.
struct Dirs {
    _base: tempfile::TempDir,
    root: PathBuf,
    kv: PathBuf,
}

fn dirs() -> Dirs {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    let kv = base.path().join("kv");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&kv).unwrap();
    Dirs { _base: base, root, kv }
}

async fn open(d: &Dirs) -> Arc<Database> {
    init();
    Arc::new(Database::new_named(&path_str(&d.root), false, "kvparttests").await)
}

fn close(db: Arc<Database>, d: &Dirs) {
    drop(db);
    crate::core::wal::remove_for(&path_str(&d.root));
}

async fn storage(db: &Arc<Database>, sql: &str) -> Result<Vec<String>, String> {
    crate::core::storage::sql::execute(sql, db).await
}

/// The store `logs` over `d.kv`, and table `name` placed on it.
async fn place(db: &Arc<Database>, d: &Dirs, name: &str) {
    storage(db, &format!("STORAGE ADD STORE logs DIRECTORY WITH (PATH = '{}', PREFIX = 'logs')", path_str(&d.kv)))
        .await
        .unwrap();
    storage(db, &format!("STORAGE PLACE TABLE {name} AT 'kv:logs'")).await.unwrap();
}

fn col(name: &str, data_type: DbType) -> CreateColumnData {
    CreateColumnData { name: name.into(), data_type }
}

fn acct(name: &str) -> CreateTable {
    CreateTable {
        table_name: name.to_string(),
        columns: vec![col("tenant_id", DbType::I64), col("n", DbType::I32), col("label", DbType::STRING)],
        is_immutable: false,
        special: crate::core::row::schema::SpecialTable::None,
        ledger_policy: Vec::new(),
        foreign_keys: Vec::new(),
        defaults: Vec::new(),
        json_columns: Vec::new(),
        unique_columns: Vec::new(),
        type_modifiers: Vec::new(),
        unique_keys: Vec::new(),
        primary_key: None,
        if_not_exists: false,
        generated: Vec::new(),
        checks: Vec::new(),
    }
}

async fn create(db: &Database, name: &str, partitions: u32) -> u64 {
    db.create_partitioned_table(acct(name), &["tenant_id"], partitions)
        .await
        .unwrap_or_else(|e| panic!("creating {name}: {e}"));
    partitioned(db, name).uid()
}

fn partitioned(db: &Database, name: &str) -> Arc<PartitionedTable> {
    match db.relation(name) {
        Some(Relation::Partitioned(t)) => t,
        Some(Relation::Plain(_)) => panic!("{name} resolved as a plain table"),
        None => panic!("{name} does not resolve"),
    }
}

fn row(t: &PartitionedTable, tenant: i64, n: i32) -> crate::core::row::row::RowWrite {
    let qp = QueryPurpose::InsertRow(InsertRowData {
        table_name: t.name().to_string(),
        query: format!("INSERT INTO {} (tenant_id, n, label) VALUES ({tenant}, {n}, 'x')", t.name()),
        on_conflict_do_nothing: false,
        on_conflict_target: Default::default(),
        on_conflict_update: None,
    });
    crate::core::rql::lexer_ir::insert_row_with_defaults(&qp, &t.shell().schema.fields, &[], &[], 0)
        .expect("the insert parser refused a plain row")
}

async fn fill(db: &Database, t: &PartitionedTable, tenants: std::ops::Range<i64>) {
    for tenant in tenants {
        t.insert_row_in_txn(row(t, tenant, tenant as i32), FROZEN_TX).await.unwrap();
    }
    db.sync_durable().unwrap();
}

/// Rows with `n >= 0` across every materialized partition.
async fn count(t: &PartitionedTable) -> usize {
    let epoch = t.epoch().unwrap();
    let mut total = 0;
    for (_, p) in t.materialized(&epoch, &Route::All).await.unwrap() {
        let fetch = Table::<DbStorageProvider>::build_full_row_fetch(&p.schema.fields);
        total += p
            .query_row("n >= 0", &p.schema.fields.to_vec(), fetch.clone(), fetch, None, None)
            .await
            .unwrap()
            .len();
    }
    total
}

fn files_in(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = match std::fs::read_dir(dir) {
        Ok(listing) => listing.map(|e| e.unwrap().file_name().into_string().unwrap()).collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

/// The pids with a data file named in `dir` (a directory's files, or a
/// directory store's per-file key prefixes).
fn data_pids(dir: &Path, uid: u64) -> Vec<u32> {
    let mut pids: Vec<u32> = files_in(dir)
        .iter()
        .filter_map(|n| match files::parse_name(n)? {
            PartFile::Data { uid: u, pid } if u == uid => Some(pid),
            _ => None,
        })
        .collect();
    pids.sort();
    pids.dedup();
    pids
}

/// Where the store keeps its files' objects: one directory per file name.
fn store_files(d: &Dirs) -> PathBuf {
    d.kv.join("logs")
}

/// Whether `dir` holds an object anywhere under it: a directory store leaves
/// the empty directories of a swept file behind.
fn holds_objects(dir: &Path) -> bool {
    std::fs::read_dir(dir).into_iter().flatten().flatten().any(|e| {
        let p = e.path();
        if p.is_dir() { holds_objects(&p) } else { true }
    })
}

/// The pids of `uid` with a data file holding objects in the store.
fn store_pids(d: &Dirs, uid: u64) -> Vec<u32> {
    let mut pids: Vec<u32> = files_in(&store_files(d))
        .iter()
        .filter(|n| holds_objects(&store_files(d).join(n)))
        .filter_map(|n| match files::parse_name(n)? {
            PartFile::Data { uid: u, pid } if u == uid => Some(pid),
            _ => None,
        })
        .collect();
    pids.sort();
    pids.dedup();
    pids
}

/// Waits (bounded) until `cond` holds.
async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !cond() {
        assert!(std::time::Instant::now() < deadline, "{what}: not within 30 s");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn materialized_locations(t: &PartitionedTable) -> Vec<(u32, String)> {
    t.partitions().await.unwrap().into_iter().filter_map(|p| p.location.map(|l| (p.pid, l))).collect()
}

/// The partitions' two files live on the store and nowhere else; the parent
/// files stay in the root; a close, a reopen and a restart read every row
/// back from the store.
#[tokio::test(flavor = "multi_thread")]
async fn placed_partitions_live_on_the_store_across_a_restart() {
    let d = dirs();
    let db = open(&d).await;
    place(&db, &d, "acct").await;
    let uid = create(&db, "acct", 4).await;
    let t = partitioned(&db, "acct");
    fill(&db, &t, 0..64).await;
    assert_eq!(count(&t).await, 64);

    let placed = materialized_locations(&t).await;
    assert_eq!(placed.len(), 4, "64 tenants did not reach all 4 partitions: {placed:?}");
    assert!(placed.iter().all(|(_, l)| l == "kv:logs"), "a partition is not on the store: {placed:?}");
    assert_eq!(data_pids(&d.root, uid), Vec::<u32>::new(), "partition files in the root");
    assert_eq!(store_pids(&d, uid), vec![0, 1, 2, 3], "partition files missing from the store");
    let root_files = files_in(&d.root);
    for parent in [files::map_file(uid, 0), files::ids_file(uid), format!("{}.db", files::schema_stem(uid))] {
        assert!(root_files.contains(&parent), "parent file {parent} is not in the root: {root_files:?}");
    }

    // Closed and reopened through the store.
    assert!(t.close_idle().await.unwrap() > 0, "nothing was open to close");
    assert_eq!(t.open_count(), 0);
    assert_eq!(count(&t).await, 64, "a reopen from the store lost rows");
    let totals = t.totals().await.unwrap();
    assert_eq!(totals.materialized, 4);
    assert!(totals.data > 0 && totals.pointers > 0, "{totals:?}");

    drop(t);
    close(db, &d);
    let db = open(&d).await;
    let t = partitioned(&db, "acct");
    assert!(t.broken().is_none(), "{:?}", t.broken());
    assert_eq!(count(&t).await, 64, "the restart lost rows");
    assert!(materialized_locations(&t).await.iter().all(|(_, l)| l == "kv:logs"));
    // Writes after the restart land on the store too.
    fill(&db, &t, 64..80).await;
    assert_eq!(count(&t).await, 80);
    assert_eq!(data_pids(&d.root, uid), Vec::<u32>::new(), "partition files in the root after the restart");
    drop(t);
    close(db, &d);
    let db = open(&d).await;
    assert_eq!(count(&partitioned(&db, "acct")).await, 80, "the second restart lost rows");
}

/// A materialized partition whose file is gone from the store refuses the
/// table at open, named, rather than opening it as an empty file (I9).
#[tokio::test(flavor = "multi_thread")]
async fn a_partition_file_missing_from_the_store_refuses_the_table() {
    let d = dirs();
    let db = open(&d).await;
    place(&db, &d, "acct").await;
    let uid = create(&db, "acct", 4).await;
    let t = partitioned(&db, "acct");
    fill(&db, &t, 0..64).await;
    drop(t);
    close(db, &d);

    let lost = format!("{}_pointers.db", files::data_stem(uid, 2));
    std::fs::remove_dir_all(store_files(&d).join(&lost)).unwrap();
    let db = open(&d).await;
    let t = partitioned(&db, "acct");
    let why = t.broken().unwrap_or_else(|| panic!("the table opened without {lost}"));
    assert!(why.contains(&lost) && why.contains("missing from key/value store 'logs'"), "{why}");
}

/// DROP drops every partition file from the store and every parent file
/// from the root, and forgets the name's placement.
#[tokio::test(flavor = "multi_thread")]
async fn drop_removes_the_partitions_from_the_store() {
    let d = dirs();
    let db = open(&d).await;
    place(&db, &d, "acct").await;
    let uid = create(&db, "acct", 4).await;
    let t = partitioned(&db, "acct");
    fill(&db, &t, 0..64).await;
    drop(t);
    assert_eq!(store_pids(&d, uid).len(), 4);
    assert!(db.drop_partitioned("acct").await.unwrap());
    let prefix = format!("{}{uid:016x}_", files::PREFIX);
    eventually("the store's partition files go", || {
        files_in(&store_files(&d)).iter().all(|n| !n.starts_with(&prefix) || !holds_objects(&store_files(&d).join(n)))
    })
    .await;
    assert!(files_in(&d.root).iter().all(|n| !n.starts_with(&prefix)), "{:?}", files_in(&d.root));
    assert_eq!(super::pool_spec_of(&db.storage, "acct"), None, "the placement outlived the table");
    // And the name is free again, back on the root with no placement.
    let uid2 = create(&db, "acct", 2).await;
    let t = partitioned(&db, "acct");
    fill(&db, &t, 0..8).await;
    assert_eq!(count(&t).await, 8);
    assert!(!data_pids(&d.root, uid2).is_empty(), "a table with no placement wrote its partitions elsewhere");
}

/// The startup reaper drops a partition file on the store that no
/// `MATERIALIZE` names (a materialize a crash cut short), and keeps the
/// materialized ones.
#[tokio::test(flavor = "multi_thread")]
async fn the_reaper_drops_a_store_leftover() {
    let d = dirs();
    let db = open(&d).await;
    place(&db, &d, "acct").await;
    let uid = create(&db, "acct", 8).await;
    let t = partitioned(&db, "acct");
    // One tenant: one partition materialized, seven not.
    fill(&db, &t, 0..1).await;
    let materialized = t.materialized_pids().unwrap();
    assert_eq!(materialized.len(), 1);
    let unplaced = (0..8u32).find(|p| !materialized.contains(p)).unwrap();
    // A leftover of an unmaterialized partition, with a byte in it.
    {
        let placement = db.partition_placement();
        let store = placement.store("kv:logs").await.unwrap();
        let name = format!("{}.db", files::data_stem(uid, unplaced));
        let file = store.create_fresh(&name).await.unwrap();
        file.write_data(0, b"leftover").await;
        // And an index-snapshot sidecar whose file is gone (§2): of another
        // unmaterialized partition, with no data file beside it.
        let orphan_pid = (0..8u32).rev().find(|p| !materialized.contains(p) && *p != unplaced).unwrap();
        let orphan = format!("{}.db.ixs", files::data_stem(uid, orphan_pid));
        let side = store.create_fresh(&orphan).await.unwrap();
        side.write_data(0, b"stale snapshot").await;
    }
    let orphan_dir = || {
        files_in(&store_files(&d))
            .into_iter()
            .any(|n| n.ends_with(".db.ixs") && holds_objects(&store_files(&d).join(&n)))
    };
    assert!(orphan_dir(), "the orphan sidecar was not stored");
    assert!(store_pids(&d, uid).contains(&unplaced));
    drop(t);
    close(db, &d);

    let db = open(&d).await;
    eventually("the leftover is dropped", || !store_pids(&d, uid).contains(&unplaced)).await;
    eventually("the orphan sidecar is dropped", || !orphan_dir()).await;
    assert_eq!(store_pids(&d, uid), materialized, "the reaper dropped a materialized partition");
    let t = partitioned(&db, "acct");
    assert_eq!(count(&t).await, 1);
}

/// A store holding a partition cannot be removed, and a CREATE after a
/// directory placement is still refused by name.
#[tokio::test(flavor = "multi_thread")]
async fn a_store_with_partitions_is_not_dropped_and_directory_placements_still_refuse() {
    let d = dirs();
    let db = open(&d).await;
    place(&db, &d, "acct").await;
    create(&db, "acct", 2).await;
    let t = partitioned(&db, "acct");
    fill(&db, &t, 0..4).await;
    let refused = storage(&db, "STORAGE DROP STORE logs").await.unwrap_err();
    assert!(refused.contains("acct"), "{refused}");

    storage(&db, &format!("STORAGE PLACE TABLE dirt AT '{}'", path_str(&d.root))).await.unwrap();
    let e = db.create_partitioned_table(acct("dirt"), &["tenant_id"], 2).await.unwrap_err();
    assert!(e.to_string().contains("has a storage placement recorded"), "{e}");
}

/// A partition materialized on the store with no row written yet (its first
/// write failed, or a crash came between the `MATERIALIZE` and the row)
/// still stands after a restart: its files were stored empty, so the open
/// finds them rather than refusing the table for a missing file.
#[tokio::test(flavor = "multi_thread")]
async fn a_partition_materialized_with_no_row_survives_a_restart() {
    let d = dirs();
    let db = open(&d).await;
    place(&db, &d, "acct").await;
    let uid = create(&db, "acct", 4).await;
    let t = partitioned(&db, "acct");
    let point = t.point_of_write(&row(&t, 7, 7)).unwrap();
    t.partition_for_point(point).await.unwrap();
    let pids = t.materialized_pids().unwrap();
    assert_eq!(pids.len(), 1);
    drop(t);
    close(db, &d);

    assert_eq!(store_pids(&d, uid), pids, "the empty partition's files were not stored");
    let db = open(&d).await;
    let t = partitioned(&db, "acct");
    assert!(t.broken().is_none(), "{:?}", t.broken());
    assert_eq!(t.materialized_pids().unwrap(), pids);
    fill(&db, &t, 7..8).await;
    assert_eq!(count(&t).await, 1);
}

/// A snapshot's cold copy does not copy store partitions, and says so: the
/// table is recorded as not contained, never silently left out.
#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_records_store_partitions_as_not_contained() {
    let d = dirs();
    let db = open(&d).await;
    place(&db, &d, "acct").await;
    let uid = create(&db, "acct", 4).await;
    let t = partitioned(&db, "acct");
    fill(&db, &t, 0..8).await;
    let copy = crate::core::backup::cold::copy_cold_tables(&d.root, || 0).await;
    let label = format!("partitioned table {uid:016x}");
    let entry = copy.uncovered.iter().find(|u| u.table == label).unwrap_or_else(|| {
        panic!("{label} is not recorded as uncovered: {:?}", copy.uncovered.iter().map(|u| &u.table).collect::<Vec<_>>())
    });
    assert!(entry.reason.contains("not copied by backups yet") && entry.reason.contains("'logs'"), "{}", entry.reason);
    assert!(copy.files.iter().all(|(name, _)| !name.contains("__pt_")), "a partition file was copied");
}

/// Runs `sql` through the executor, panicking on an error frame.
async fn sql(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap_or_else(|| panic!("{sql}"));
    let r = crate::core::rql::executor::execute(purpose, db.clone()).await;
    assert_ne!(r.first(), Some(&3), "{sql}: {}", String::from_utf8_lossy(&r));
    r
}

/// The rows a SELECT answers.
async fn rows_of(db: &Arc<Database>, query: &str) -> usize {
    let r = sql(db, query).await;
    assert_eq!(r.first(), Some(&2), "{query}");
    crate::core::row::row::vec_into_rows(&r[1..]).expect("rows").len()
}

/// Every partition of `t` opened, its index load or build finished, with
/// how its indexes came to be.
async fn origins(t: &PartitionedTable) -> Vec<(u32, crate::core::index_store::load::Origin)> {
    let mut out = Vec::new();
    for (pid, part) in t.all_partitions().await.unwrap() {
        part.index_load_settled().await;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while part.index_origin() == crate::core::index_store::load::Origin::Pending {
            assert!(std::time::Instant::now() < deadline, "partition {pid}'s indexes never settled");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        out.push((pid, part.index_origin()));
    }
    out.sort_by_key(|(pid, _)| *pid);
    out
}

/// docs/kv-partitions.md §2: a store partition sealed (here by
/// `rdb_index_checkpoint`) loads its FILTER and INDEX sections at the next
/// open instead of rebuilding them, validated by its files' store states. A
/// partition written after its seal recovers other states and rebuilds,
/// and the row written then is found through its index; a seal with
/// nothing written since writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sealed_store_partitions_load_their_indexes_and_a_later_write_rebuilds() {
    use crate::core::index_store::load::{Origin, settings::IndexStorage, storage_override};
    use crate::core::index_store::seal::SealOutcome;
    let d = dirs();
    let db = open(&d).await;
    place(&db, &d, "acct").await;
    let uid = create(&db, "acct", 4).await;
    let _snapshot = storage_override::set_stem(&format!("__pt_{uid:016x}"), IndexStorage::Snapshot);
    sql(&db, "CREATE INDEX acct_n ON acct (n)").await;
    for tenant in 0..96 {
        sql(&db, &format!("INSERT INTO acct (tenant_id, n, label) VALUES ({tenant}, {}, 'x')", tenant * 10)).await;
    }
    let t = partitioned(&db, "acct");
    assert_eq!(t.materialized_pids().unwrap().len(), 4);
    let report = db.index_checkpoint("acct", None).await.unwrap();
    assert_eq!(report.sealed.len(), 4, "{:?}", report.sealed.iter().map(|s| &s.name).collect::<Vec<_>>());
    for s in &report.sealed {
        assert!(matches!(s.outcome, SealOutcome::Published), "{}: {}", s.name, s.outcome.describe());
    }
    // Nothing written since: nothing to write.
    let again = db.index_checkpoint("acct", None).await.unwrap();
    for s in &again.sealed {
        assert!(matches!(s.outcome, SealOutcome::AlreadyClean), "{}: {}", s.name, s.outcome.describe());
    }
    drop(t);
    close(db, &d);

    let db = open(&d).await;
    let t = partitioned(&db, "acct");
    let loaded = origins(&t).await;
    assert!(loaded.iter().all(|(_, o)| *o == Origin::Loaded), "{loaded:?}");
    assert_eq!(rows_of(&db, "SELECT tenant_id FROM acct WHERE n = 470").await, 1);
    assert_eq!(rows_of(&db, "SELECT tenant_id FROM acct").await, 96);
    // A reopen with no write in between still matches: the same states.
    let reloaded = db.index_checkpoint("acct", None).await.unwrap();
    for s in &reloaded.sealed {
        assert!(matches!(s.outcome, SealOutcome::AlreadyClean), "{}: {}", s.name, s.outcome.describe());
    }

    // One row written after the seal, and no seal after it.
    sql(&db, "INSERT INTO acct (tenant_id, n, label) VALUES (5000, 77777, 'late')").await;
    let written = t.partition_for_point(t.point_of_write(&row(&t, 5000, 77777)).unwrap()).await.unwrap();
    let written_pid = t
        .open_partitions_now()
        .into_iter()
        .find(|(_, p)| std::ptr::eq(&**p, &*written))
        .map(|(pid, _)| pid)
        .expect("the written partition is open");
    drop(written);
    drop(t);
    close(db, &d);

    let db = open(&d).await;
    let t = partitioned(&db, "acct");
    for (pid, origin) in origins(&t).await {
        if pid == written_pid {
            assert_eq!(origin, Origin::Built, "partition {pid} was written after its seal and must rebuild");
        } else {
            assert_eq!(origin, Origin::Loaded, "partition {pid}");
        }
    }
    assert_eq!(rows_of(&db, "SELECT tenant_id FROM acct WHERE n = 77777").await, 1, "the late row");
    assert_eq!(rows_of(&db, "SELECT tenant_id FROM acct").await, 97);

    // Sealed again, then a row deleted, the table compacted and a row of
    // the same size put back with another value: both files end at the
    // lengths the seal recorded, the slots renumbered under them, so only
    // the store states tell the snapshot is stale. Loaded anyway, the index
    // would map the old value to whatever row now sits at its slot.
    for s in &db.index_checkpoint("acct", None).await.unwrap().sealed {
        assert!(s.outcome.is_clean(), "{}: {}", s.name, s.outcome.describe());
    }
    let lens = |t: &PartitionedTable| -> Vec<(u32, u64)> {
        t.open_partitions_now().into_iter().map(|(pid, p)| (pid, p.data().unwrap().io_pointers.kv_state_now().unwrap().len)).collect()
    };
    let before = lens(&t);
    sql(&db, "DELETE FROM acct WHERE tenant_id = 7").await;
    sql(&db, "VACUUM FULL acct").await;
    sql(&db, "INSERT INTO acct (tenant_id, n, label) VALUES (7, 99999, 'x')").await;
    let in_place = lens(&t) == before;
    drop(t);
    close(db, &d);
    let db = open(&d).await;
    let t = partitioned(&db, "acct");
    let rebuilt: Vec<u32> = origins(&t).await.into_iter().filter(|(_, o)| *o != Origin::Loaded).map(|(p, _)| p).collect();
    // VACUUM FULL wrote every partition (each compaction stores its slot
    // moves and its truncation), so every one rebuilds.
    assert_eq!(rebuilt.len(), 4, "the compacted partitions rebuild: {rebuilt:?}");
    assert_eq!(rows_of(&db, "SELECT tenant_id FROM acct WHERE n = 99999").await, 1, "the updated row");
    assert_eq!(rows_of(&db, "SELECT tenant_id FROM acct WHERE n = 70").await, 0, "its old value");
    assert!(in_place, "the lengths moved: this no longer tests the store states alone");
    drop(t);
    close(db, &d);
}

/// A partition's sidecar goes when its files do: DROP leaves no `.ixs` on
/// the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_store_partition_takes_its_snapshot_with_it() {
    let d = dirs();
    let db = open(&d).await;
    place(&db, &d, "acct").await;
    let uid = create(&db, "acct", 2).await;
    for tenant in 0..16 {
        sql(&db, &format!("INSERT INTO acct (tenant_id, n, label) VALUES ({tenant}, {tenant}, 'x')")).await;
    }
    db.index_checkpoint("acct", None).await.unwrap();
    let sidecars = |d: &Dirs| -> Vec<String> {
        files_in(&store_files(d))
            .into_iter()
            .filter(|n| n.ends_with(".ixs") && n.contains(&format!("{uid:016x}")) && holds_objects(&store_files(d).join(n)))
            .collect()
    };
    assert!(!sidecars(&d).is_empty(), "the seal stored no sidecar");
    sql(&db, "DROP TABLE acct").await;
    eventually("the sidecars are swept", || sidecars(&d).is_empty()).await;
    close(db, &d);
}

/// A plain table on a store is sealed and loaded the same way (§2 is the
/// key/value driver's, not the partitions'), and DROP takes its sidecar.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_store_table_loads_its_snapshot_after_a_restart() {
    use crate::core::index_store::load::{Origin, settings::IndexStorage, storage_override};
    let d = dirs();
    let db = open(&d).await;
    place(&db, &d, "plain").await;
    let _snapshot = storage_override::set_stem("plain", IndexStorage::Snapshot);
    sql(&db, "CREATE TABLE plain (id BIGINT PRIMARY KEY, n INT, label TEXT)").await;
    sql(&db, "CREATE INDEX plain_n ON plain (n)").await;
    for i in 0..64 {
        sql(&db, &format!("INSERT INTO plain (id, n, label) VALUES ({i}, {}, 'x')", i % 8)).await;
    }
    let report = db.index_checkpoint("plain", None).await.unwrap();
    assert_eq!(report.sealed.len(), 1);
    assert!(report.sealed[0].outcome.is_clean(), "{}", report.sealed[0].outcome.describe());
    // As a server stops: the store's service compacts a file at 64 deltas,
    // and one after the checkpoint above would leave its snapshot stale; the
    // shutdown seal pauses the service and seals again.
    let shutdown = db.seal_for_shutdown(Duration::from_secs(60)).await;
    assert!(shutdown.skipped.is_empty(), "{:?}", shutdown.skipped);
    for s in &shutdown.sealed {
        assert!(s.outcome.is_clean(), "{}: {}", s.name, s.outcome.describe());
    }
    close(db, &d);

    let db = open(&d).await;
    let table = match db.relation("plain") {
        Some(Relation::Plain(t)) => t,
        _ => panic!("plain is not a plain table"),
    };
    table.index_load_settled().await;
    assert_eq!(table.index_origin(), Origin::Loaded);
    assert_eq!(rows_of(&db, "SELECT id FROM plain WHERE n = 3").await, 8);
    drop(table);
    let sidecar = || holds_objects(&store_files(&d).join("plain.db.ixs"));
    assert!(sidecar(), "no sidecar at {:?}: {:?}", store_files(&d), files_in(&store_files(&d)));
    sql(&db, "DROP TABLE plain").await;
    eventually("the sidecar is swept", || !sidecar()).await;
    close(db, &d);
}
