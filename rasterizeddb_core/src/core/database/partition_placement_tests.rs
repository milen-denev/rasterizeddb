//! Phase 4 placement (docs/partitioning.md §6.1, I21): partitions
//! materialize on the storage pool's locations by its policy, counted across
//! tables, recorded in `MATERIALIZE`, and found there again after a restart;
//! the pool's guards, DROP and the startup reaper reach every location; a
//! map naming a directory the pool does not is refused, never read.
//!
//! API level, as `partitioned_tests` is, in small temp directories: a root
//! and a second primary location, both on the same filesystem.

use std::path::{Path, PathBuf};

use rclite::Arc;

use super::*;
use crate::core::mvcc::transaction::FROZEN_TX;
use crate::core::partition::files::{self, PartFile};
use crate::core::partition::route::Route;
use crate::core::partition::table::PartitionedTable;
use crate::core::rql::lexer_ct::CreateColumnData;
use crate::core::rql::lexer_s1::{InsertRowData, QueryPurpose};
use crate::core::storage::{LocationRole, PlacementPolicy};

fn init() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
}

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

/// A root and a second primary location under one temp directory.
struct Dirs {
    _base: tempfile::TempDir,
    root: PathBuf,
    disk: PathBuf,
}

fn dirs() -> Dirs {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    let disk = base.path().join("disk");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&disk).unwrap();
    Dirs { _base: base, root, disk }
}

async fn open(d: &Dirs) -> Database {
    init();
    Database::new_named(&path_str(&d.root), false, "placetests").await
}

/// Opens the root and adds the second location under `policy`.
async fn open_pool(d: &Dirs, policy: PlacementPolicy) -> Database {
    let db = open(d).await;
    db.storage.add_location(&path_str(&d.disk), LocationRole::Primary, 1, true).unwrap();
    db.storage.set_policy(policy);
    db
}

/// Closes `db` so both directories can be opened again in this process.
fn close(db: Database, d: &Dirs) {
    drop(db);
    crate::core::wal::remove_for(&path_str(&d.root));
    crate::core::wal::remove_for(&path_str(&d.disk));
}

fn col(name: &str, data_type: DbType) -> CreateColumnData {
    CreateColumnData {
        name: name.into(),
        data_type,
    }
}

fn acct(name: &str) -> CreateTable {
    CreateTable {
        table_name: name.to_string(),
        columns: vec![col("tenant_id", DbType::I64), col("n", DbType::I32), col("label", DbType::STRING)],
        is_immutable: false,
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
        on_conflict_target: Vec::new(),
        on_conflict_update: None,
    });
    crate::core::rql::lexer_ir::insert_row_with_defaults(&qp, &t.shell().schema.fields, &[], &[], 0)
        .expect("the insert parser refused a plain row")
}

/// One row per tenant in `tenants`, durable on return.
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
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    v.sort();
    v
}

fn uid_files(dir: &Path, uid: u64) -> Vec<String> {
    files_in(dir)
        .into_iter()
        .filter(|n| n.starts_with(&format!("{}{uid:016x}_", files::PREFIX)))
        .collect()
}

/// The pids with a data file in `dir`.
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

/// Where each materialized partition is, by pid, as the table reports it.
async fn locations(t: &PartitionedTable) -> Vec<(u32, String)> {
    t.partitions()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|p| p.location.map(|l| (p.pid, crate::core::partition::files::dir_key(&l))))
        .collect()
}

/// Round-robin spreads one table's partitions evenly over both locations,
/// ties to the root, with their data files where the map says; a restart
/// finds every partition where it was and reads every row back.
#[tokio::test(flavor = "multi_thread")]
async fn round_robin_spreads_partitions_and_a_restart_finds_them() {
    let d = dirs();
    let db = open_pool(&d, PlacementPolicy::RoundRobin).await;
    let uid = create(&db, "acct", 4).await;
    let t = partitioned(&db, "acct");
    let cfg = d.root.join(crate::core::storage::POOL_FILE);
    let pool_file = std::fs::read(&cfg).unwrap();
    fill(&db, &t, 0..64).await;
    assert_eq!(std::fs::read(&cfg).unwrap(), pool_file, "a materialization wrote the pool file (I21)");
    let placed = locations(&t).await;
    assert_eq!(placed.len(), 4, "64 tenants did not reach all 4 partitions: {placed:?}");
    let (root, disk) = (files::dir_key(&path_str(&d.root)), files::dir_key(&path_str(&d.disk)));
    let on = |dir: &str| placed.iter().filter(|(_, l)| l == dir).map(|(p, _)| *p).collect::<Vec<u32>>();
    assert_eq!((on(&root).len(), on(&disk).len()), (2, 2), "not spread evenly: {placed:?}");
    assert_eq!(data_pids(&d.root, uid), on(&root), "root data files are not the root's partitions");
    assert_eq!(data_pids(&d.disk, uid), on(&disk), "second location's data files are not its partitions");
    // Only partitions' files leave the root.
    assert!(
        uid_files(&d.disk, uid).iter().all(|n| matches!(files::parse_name(n), Some(PartFile::Data { .. }))),
        "a parent file went to the second location: {:?}",
        uid_files(&d.disk, uid)
    );
    let report = db.storage.locations();
    let counted: Vec<u64> = report.iter().map(|l| l.partitions).collect();
    assert_eq!(counted, vec![2, 2], "SHOW STORAGE's partition counts");
    assert_eq!(count(&t).await, 64);
    drop(t);
    close(db, &d);

    let db = open(&d).await;
    let t = partitioned(&db, "acct");
    assert!(t.broken().is_none(), "the reopen refused: {:?}", t.broken());
    assert_eq!(t.totals().await.unwrap().materialized, 4);
    assert_eq!(count(&t).await, 64, "rows lost across the restart");
    assert_eq!(locations(&t).await, placed, "a partition moved across the restart");
    let counted: Vec<u64> = db.storage.locations().iter().map(|l| l.partitions).collect();
    assert_eq!(counted, vec![2, 2], "the reopen did not register its partitions with the dealer");
    drop(t);
    close(db, &d);
}

/// The dealer is cross-table: a second table's first partition goes where
/// the first table left fewer, not to the root again.
#[tokio::test(flavor = "multi_thread")]
async fn the_dealer_counts_every_table() {
    let d = dirs();
    let db = open_pool(&d, PlacementPolicy::RoundRobin).await;
    let a = create(&db, "a", 1).await;
    let t = partitioned(&db, "a");
    fill(&db, &t, 0..1).await;
    assert_eq!(data_pids(&d.root, a), vec![0], "the first partition of all goes to the root (a tie)");
    let b = create(&db, "b", 1).await;
    let u = partitioned(&db, "b");
    fill(&db, &u, 0..1).await;
    assert_eq!(data_pids(&d.disk, b), vec![0], "the second table's partition did not see the first's");
    drop((t, u));
    close(db, &d);
}

/// A location holding partitions cannot leave the pool or become a backup;
/// after the DROP every location is free of the table's files and can.
#[tokio::test(flavor = "multi_thread")]
async fn guards_hold_a_location_with_partitions_and_drop_clears_every_location() {
    let d = dirs();
    let db = open_pool(&d, PlacementPolicy::RoundRobin).await;
    let uid = create(&db, "acct", 2).await;
    let t = partitioned(&db, "acct");
    fill(&db, &t, 0..32).await;
    assert_eq!(data_pids(&d.disk, uid).len(), 1, "no partition on the second location");
    let disk = path_str(&d.disk);
    let removed = db.storage.drop_location(&disk).expect_err("dropped a location holding a partition");
    assert!(removed.contains("partition"), "{removed}");
    let demoted = db.storage.set_role(&disk, LocationRole::Backup).expect_err("made a partition's location a backup");
    assert!(demoted.contains("partition"), "{demoted}");
    drop(t);

    assert!(db.drop_partitioned("acct").await.unwrap());
    assert_eq!(uid_files(&d.disk, uid), Vec::<String>::new(), "DROP left files on the second location");
    assert_eq!(uid_files(&d.root, uid), Vec::<String>::new(), "DROP left files in the root");
    db.storage.drop_location(&disk).expect("the location is empty after the DROP");
    close(db, &d);
}

/// The startup reaper deletes partition files on the second location that
/// no map places there: a materialized partition's stray copy, and an
/// unknown table's file. The real ones stay and every row reads back.
#[tokio::test(flavor = "multi_thread")]
async fn the_reaper_clears_strays_on_every_location() {
    let d = dirs();
    let db = open_pool(&d, PlacementPolicy::RoundRobin).await;
    let uid = create(&db, "acct", 2).await;
    let t = partitioned(&db, "acct");
    fill(&db, &t, 0..32).await;
    let on_root = data_pids(&d.root, uid);
    let on_disk = data_pids(&d.disk, uid);
    assert_eq!((on_root.len(), on_disk.len()), (1, 1));
    drop(t);
    close(db, &d);

    // A copy of the root partition on the second location, as a crash in a
    // materialize dealt there before it was dealt again could leave, and a
    // file of a table no catalogue row names.
    let stray = format!("{}.db", files::data_stem(uid, on_root[0]));
    std::fs::write(d.disk.join(&stray), b"stray").unwrap();
    let orphan = format!("{}.db", files::data_stem(0x0123_4567_89ab_cdef, 0));
    std::fs::write(d.disk.join(&orphan), b"orphan").unwrap();

    let db = open(&d).await;
    let names = files_in(&d.disk);
    assert!(!names.contains(&stray), "the stray copy survived: {names:?}");
    assert!(!names.contains(&orphan), "the unknown table's file survived: {names:?}");
    assert_eq!(data_pids(&d.disk, uid), on_disk, "the reaper took a real partition");
    let t = partitioned(&db, "acct");
    assert_eq!(count(&t).await, 32);
    drop(t);
    close(db, &d);
}

/// A map naming a directory the pool does not (copied, restored elsewhere,
/// or the location removed from the pool file by hand) refuses its table
/// with the path, and the files there are left as they are.
#[tokio::test(flavor = "multi_thread")]
async fn a_partition_outside_the_pool_refuses_its_table() {
    let d = dirs();
    let db = open_pool(&d, PlacementPolicy::RoundRobin).await;
    let uid = create(&db, "acct", 2).await;
    let t = partitioned(&db, "acct");
    fill(&db, &t, 0..32).await;
    let before = uid_files(&d.disk, uid);
    assert!(!before.is_empty());
    drop(t);
    close(db, &d);

    let cfg = d.root.join(crate::core::storage::POOL_FILE);
    let disk = path_str(&d.disk);
    let text = std::fs::read_to_string(&cfg).unwrap();
    let kept: String = text.lines().filter(|l| !l.ends_with(&disk)).map(|l| format!("{l}\n")).collect();
    assert_ne!(kept, text, "the pool file did not name the location");
    std::fs::write(&cfg, kept).unwrap();

    let db = open(&d).await;
    let t = partitioned(&db, "acct");
    let why = t.broken().expect("opened a partition in a directory the pool does not name").to_string();
    assert!(why.contains("not a storage location") && why.contains(&disk), "{why}");
    assert_eq!(uid_files(&d.disk, uid), before, "the refused table's files were touched");
    drop(t);
    close(db, &d);
}

/// I21: 1024 materializations over three locations are within one of each
/// other under round-robin; under free-space they follow the weights; a
/// disabled location gets none; a failed one is taken back; `root_only`
/// keeps every one in the root.
#[tokio::test(flavor = "multi_thread")]
async fn the_dealer_balances_by_policy() {
    let d = dirs();
    let third = d.root.parent().unwrap().join("third");
    std::fs::create_dir_all(&third).unwrap();
    let db = open_pool(&d, PlacementPolicy::RoundRobin).await;
    db.storage.add_location(&path_str(&third), LocationRole::Primary, 1, true).unwrap();
    let keys: Vec<String> = [&d.root, &d.disk, &third].iter().map(|p| files::dir_key(&path_str(p))).collect();
    let tally = |dealt: &[String]| keys.iter().map(|k| dealt.iter().filter(|d| *d == k).count()).collect::<Vec<_>>();

    let dealt: Vec<String> = (0..1024).map(|_| db.storage.deal_partition(1, false).unwrap()).collect();
    let n = tally(&dealt);
    assert!(n.iter().max().unwrap() - n.iter().min().unwrap() <= 1, "round-robin is not within one: {n:?}");

    // A location added mid-run gets its share of later ones at once.
    db.storage.register_partitions(1, std::collections::HashMap::new());
    db.storage.drop_location(&path_str(&third)).unwrap();
    let dealt: Vec<String> = (0..100).map(|_| db.storage.deal_partition(1, false).unwrap()).collect();
    assert_eq!(tally(&dealt)[2], 0, "a location outside the pool was dealt");
    db.storage.add_location(&path_str(&third), LocationRole::Primary, 1, true).unwrap();
    let dealt: Vec<String> = (0..150).map(|_| db.storage.deal_partition(1, false).unwrap()).collect();
    let n = tally(&dealt);
    // It catches up to 50 first, then takes a third of the rest.
    assert_eq!(n, vec![34, 33, 83], "the new location did not get its share at once");

    // Disabled: none.
    db.storage.register_partitions(1, std::collections::HashMap::new());
    db.storage.set_enabled(&path_str(&third), false).unwrap();
    let dealt: Vec<String> = (0..60).map(|_| db.storage.deal_partition(1, false).unwrap()).collect();
    assert_eq!(tally(&dealt), vec![30, 30, 0], "a disabled location was dealt");
    db.storage.set_enabled(&path_str(&third), true).unwrap();

    // Free-space: in proportion to weight on one filesystem, within slack
    // for free space moving while the test runs.
    db.storage.register_partitions(1, std::collections::HashMap::new());
    db.storage.set_policy(PlacementPolicy::FreeSpace);
    db.storage.set_weight(&path_str(&third), 2).unwrap();
    let dealt: Vec<String> = (0..400).map(|_| db.storage.deal_partition(1, false).unwrap()).collect();
    let n = tally(&dealt);
    assert!(n[0].abs_diff(100) <= 3 && n[1].abs_diff(100) <= 3 && n[2].abs_diff(200) <= 3, "not 1:1:2: {n:?}");

    // Taken back.
    let before = db.storage.locations().iter().map(|l| l.partitions).sum::<u64>();
    let one = db.storage.deal_partition(1, false).unwrap();
    db.storage.undeal_partition(1, &one);
    assert_eq!(db.storage.locations().iter().map(|l| l.partitions).sum::<u64>(), before);

    // Root only.
    let dealt: Vec<String> = (0..10).map(|_| db.storage.deal_partition(2, true).unwrap()).collect();
    assert_eq!(tally(&dealt), vec![10, 0, 0], "root_only left the root");
    close(db, &d);
    crate::core::wal::remove_for(&path_str(&third));
}
