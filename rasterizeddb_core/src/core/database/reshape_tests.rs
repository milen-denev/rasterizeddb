//! Reshape (docs/partitioning.md §6.2, phase 5, "P7 as built"): `SPLIT
//! PARTITION`, `MERGE PARTITIONS`, `ISOLATE KEY`, `SPLIT KEY` and `STORAGE
//! MOVE PARTITION` keep every version a snapshot can see, route new writes
//! to the new partitions, survive a restart, and leave nothing behind when
//! they fail or a crash cuts them short on either side of the commit.
//!
//! The table's fingerprint is every row in key order, as the wire would
//! carry it, plus routed reads and aggregates: compared byte for byte before
//! and after each reshape.

use std::path::{Path, PathBuf};

use rclite::Arc;

use super::Database;
use crate::core::partition::files::{self, PartFile};
use crate::core::partition::table::reshape_hooks;

async fn open(dir: &Path) -> Arc<Database> {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::try_new_named(&dir.to_string_lossy(), false, "reshape").await.expect("open the database"))
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&dir.to_string_lossy());
}

async fn try_exec(db: &Arc<Database>, sql: &str) -> Result<Vec<u8>, String> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("parse {sql}"))?;
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    if out.is_empty() || out[0] == 3 {
        return Err(format!("{sql}: {}", String::from_utf8_lossy(out.get(1..).unwrap_or_default())));
    }
    Ok(out)
}

async fn exec_in(db: &Arc<Database>, tx: crate::core::mvcc::transaction::TxId, sql: &str) {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap_or_else(|| panic!("cannot parse {sql}"));
    let out = Box::pin(crate::core::rql::executor::execute_with_context(purpose, db.clone(), 0, tx)).await;
    assert!(!out.is_empty() && out[0] != 3, "{sql}: {}", String::from_utf8_lossy(&out));
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    try_exec(db, sql).await.unwrap_or_else(|e| panic!("{e}"))
}

/// What a client can tell about the table: every row in key order, a
/// routed read per probed tenant, and aggregates.
async fn fingerprint(db: &Arc<Database>) -> Vec<Vec<u8>> {
    let mut out = vec![
        exec(db, "SELECT tenant_id, grp, id, v, s FROM pt ORDER BY tenant_id, grp, id").await,
        exec(db, "SELECT COUNT(*), SUM(v), MIN(s), MAX(s) FROM pt").await,
        exec(db, "SELECT grp, COUNT(*) FROM pt GROUP BY grp ORDER BY grp").await,
    ];
    for t in [1, 7, 13, 19] {
        out.push(exec(db, &format!("SELECT id, v FROM pt WHERE tenant_id = {t} ORDER BY grp, id")).await);
        out.push(exec(db, &format!("SELECT COUNT(*) FROM pt WHERE tenant_id = {t} AND grp = 2")).await);
    }
    out
}

/// 20 tenants × 3 groups × 5 ids, then updates (relocating and in place),
/// deletes and a rolled-back block, so every partition holds dead and
/// aborted versions beside live ones.
async fn populated(dir: &Path) -> Arc<Database> {
    let db = open(dir).await;
    exec(
        &db,
        "CREATE TABLE pt (tenant_id BIGINT NOT NULL, grp BIGINT NOT NULL, id BIGINT NOT NULL, v BIGINT, s TEXT, \
         PRIMARY KEY (tenant_id, grp, id)) PARTITION BY HASH (tenant_id, grp) PARTITIONS 4",
    )
    .await;
    for t in 1..=20 {
        let mut values = Vec::new();
        for g in 1..=3 {
            for i in 1..=5 {
                values.push(format!("({t}, {g}, {i}, {}, 's{t}-{g}-{i}')", t * 100 + g * 10 + i));
            }
        }
        exec(&db, &format!("INSERT INTO pt (tenant_id, grp, id, v, s) VALUES {}", values.join(", "))).await;
    }
    exec(&db, "UPDATE pt SET v = v + 1 WHERE id = 2").await;
    exec(&db, "UPDATE pt SET s = 'longer text that relocates the row' WHERE id = 3").await;
    exec(&db, "DELETE FROM pt WHERE id = 5 AND grp = 1").await;
    let x = db.transactions.begin();
    exec_in(&db, x, "INSERT INTO pt (tenant_id, grp, id, v, s) VALUES (7, 2, 99, 1, 'rolled back')").await;
    exec_in(&db, x, "UPDATE pt SET v = 0 WHERE tenant_id = 7").await;
    db.abort_transaction(x);
    db
}

fn layout(db: &Database) -> Vec<(u32, u64)> {
    let pt = db.partitioned_table("pt").expect("pt");
    let epoch = pt.epoch().expect("an epoch");
    epoch.map.entries().iter().map(|&(lo, pid)| (pid, lo)).collect()
}

/// The pids with data files in `dir`.
fn pids_on_disk(dir: &Path, uid: u64) -> Vec<u32> {
    let mut pids: Vec<u32> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter_map(|n| match files::parse_name(&n) {
            Some(PartFile::Data { uid: u, pid }) if u == uid => Some(pid),
            _ => None,
        })
        .collect();
    pids.sort_unstable();
    pids.dedup();
    pids
}

/// Every materialized partition in the map has files, and no other pid does.
async fn assert_files_match(db: &Database, dir: &Path, at: &str) {
    let pt = db.partitioned_table("pt").expect("pt");
    let mut live = pt.materialized_pids().expect("pids");
    live.sort_unstable();
    assert_eq!(pids_on_disk(dir, pt.uid()), live, "{at}: files on disk vs materialized partitions");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_reshape_keeps_every_row_and_routes_new_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = populated(&dir).await;
    let before = fingerprint(&db).await;
    assert_eq!(layout(&db).len(), 4);

    // SPLIT PARTITION: the partition holding tenant 7's block, cut in two.
    exec(&db, "ALTER TABLE pt SPLIT PARTITION FOR (tenant_id = 7)").await;
    assert_eq!(layout(&db).len(), 5, "split makes one more");
    assert_eq!(fingerprint(&db).await, before, "after SPLIT PARTITION");
    assert_files_match(&db, &dir, "after SPLIT PARTITION").await;

    // MERGE PARTITIONS: the two halves again.
    let lay = layout(&db);
    let pt = db.partitioned_table("pt").unwrap();
    let block = pt.route_where("tenant_id = 7").unwrap();
    let crate::core::partition::route::Route::Points(r) = block else { panic!("tenant 7 routes to a block") };
    let at = lay.iter().position(|&(_, lo)| lo > r[0].lo).unwrap_or(lay.len()) - 1;
    // Its neighbour, by a tenant whose block lands there: any key in the
    // next partition's range will do, found by trying tenants.
    let mut other = None;
    for t in 1..2000 {
        let Ok(crate::core::partition::route::Route::Points(rr)) = pt.route_where(&format!("tenant_id = {t}")) else {
            continue;
        };
        let owner = lay.iter().rposition(|&(_, lo)| lo <= rr[0].lo).unwrap();
        if owner == at + 1 || (at > 0 && owner == at - 1) {
            other = Some(t);
            break;
        }
    }
    let other = other.expect("a tenant in a neighbouring partition");
    drop(pt);
    exec(&db, &format!("ALTER TABLE pt MERGE PARTITIONS FOR (tenant_id = 7), FOR (tenant_id = {other})")).await;
    assert_eq!(layout(&db).len(), 4, "merge makes one fewer");
    assert_eq!(fingerprint(&db).await, before, "after MERGE PARTITIONS");
    assert_files_match(&db, &dir, "after MERGE PARTITIONS").await;

    // ISOLATE KEY: tenant 13's block alone (up to two cuts).
    let n = layout(&db).len();
    exec(&db, "ALTER TABLE pt ISOLATE KEY (tenant_id = 13)").await;
    let pt = db.partitioned_table("pt").unwrap();
    let crate::core::partition::route::Route::Points(r13) = pt.route_where("tenant_id = 13").unwrap() else {
        panic!("tenant 13 routes to a block")
    };
    let epoch = pt.epoch().unwrap();
    assert_eq!(epoch.map.sole_pid(r13[0].lo, r13[0].hi).map(|p| epoch.map.range_of(p).unwrap()), Some(r13[0]), "isolated");
    assert!(layout(&db).len() > n);
    drop((pt, epoch));
    assert_eq!(fingerprint(&db).await, before, "after ISOLATE KEY");
    let refused = try_exec(&db, "ALTER TABLE pt ISOLATE KEY (tenant_id = 13)").await.unwrap_err();
    assert!(refused.contains("already has partition"), "{refused}");

    // SPLIT KEY: tenant 13's block in four, by grp.
    let n = layout(&db).len();
    exec(&db, "ALTER TABLE pt SPLIT KEY (tenant_id = 13) INTO 4").await;
    assert_eq!(layout(&db).len(), n + 3);
    assert_eq!(fingerprint(&db).await, before, "after SPLIT KEY");
    assert_files_match(&db, &dir, "after SPLIT KEY").await;

    // New writes go to the new partitions, and read back routed and not.
    exec(&db, "INSERT INTO pt (tenant_id, grp, id, v, s) VALUES (13, 2, 50, 5, 'new'), (7, 1, 50, 6, 'new')").await;
    exec(&db, "UPDATE pt SET v = v * 2 WHERE tenant_id = 13 AND grp = 2").await;
    exec(&db, "DELETE FROM pt WHERE tenant_id = 7 AND id = 4").await;
    let after_writes = fingerprint(&db).await;
    assert_ne!(after_writes, before);
    assert!(try_exec(&db, "INSERT INTO pt (tenant_id, grp, id, v) VALUES (13, 2, 50, 1)").await.is_err(), "the key is unique");

    // A restart reads the same.
    close(&dir, db);
    let db = open(&dir).await;
    assert_eq!(fingerprint(&db).await, after_writes, "after a restart");
    assert_files_match(&db, &dir, "after a restart").await;

    // Refusals: a plain table, a one-point split, nonsense counts.
    exec(&db, "CREATE TABLE plain (k BIGINT PRIMARY KEY)").await;
    assert!(try_exec(&db, "ALTER TABLE plain SPLIT PARTITION FOR (k = 1)").await.unwrap_err().contains("not one"));
    assert!(try_exec(&db, "ALTER TABLE pt SPLIT KEY (tenant_id = 13) INTO 3").await.is_err());
    assert!(try_exec(&db, "ALTER TABLE pt SPLIT KEY (tenant_id = 1, grp = 1) INTO 2").await.is_err());
    assert!(try_exec(&db, "ALTER TABLE pt SPLIT PARTITION FOR (grp = 1)").await.is_err(), "a non-leading column");
    assert_eq!(fingerprint(&db).await, after_writes);
    close(&dir, db);
}

/// A failure at either injected point leaves the table as it was, its map
/// unchanged and no target file behind; the table stays writable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_reshape_changes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = populated(&dir).await;
    let before = fingerprint(&db).await;
    let lay = layout(&db);
    for point in ["before_copy", "before_commit"] {
        reshape_hooks::arm(&dir, point);
        let e = try_exec(&db, "ALTER TABLE pt SPLIT PARTITION FOR (tenant_id = 7)").await.unwrap_err();
        assert!(e.contains("injected"), "{point}: {e}");
        assert_eq!(layout(&db), lay, "{point}: the map");
        assert_eq!(fingerprint(&db).await, before, "{point}: the rows");
        assert_files_match(&db, &dir, point).await;
    }
    // Still writable, and reshapable.
    exec(&db, "INSERT INTO pt (tenant_id, grp, id, v, s) VALUES (7, 3, 77, 1, 'after')").await;
    exec(&db, "DELETE FROM pt WHERE tenant_id = 7 AND id = 77").await;
    exec(&db, "ALTER TABLE pt SPLIT PARTITION FOR (tenant_id = 7)").await;
    assert_eq!(fingerprint(&db).await, before);
    close(&dir, db);
    let db = open(&dir).await;
    assert_eq!(fingerprint(&db).await, before, "after a restart");
    assert_files_match(&db, &dir, "after a restart").await;
    close(&dir, db);
}

/// A crash before the commit: the next open aborts the reshape, the reaper
/// deletes its targets' files, the rows are the sources'. A crash right
/// after it: the next open records the sources gone, the reaper deletes
/// their files, the rows are the targets'.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_on_either_side_of_the_commit_resolves_at_the_next_open() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = populated(&dir).await;
    let before = fingerprint(&db).await;
    let lay = layout(&db);
    let uid = db.partitioned_table("pt").unwrap().uid();

    reshape_hooks::arm(&dir, "crash_before_commit");
    assert!(try_exec(&db, "ALTER TABLE pt SPLIT PARTITION FOR (tenant_id = 7)").await.is_err());
    let leftover = pids_on_disk(&dir, uid);
    assert!(leftover.len() > lay.len(), "the targets' files are left for the next open: {leftover:?}");
    close(&dir, db);
    let db = open(&dir).await;
    assert_eq!(layout(&db), lay, "rolled back");
    assert_eq!(fingerprint(&db).await, before, "rolled back: the rows");
    assert_files_match(&db, &dir, "rolled back").await;

    reshape_hooks::arm(&dir, "crash_after_commit");
    exec(&db, "ALTER TABLE pt SPLIT PARTITION FOR (tenant_id = 7)").await;
    let split = layout(&db);
    assert_eq!(split.len(), lay.len() + 1);
    assert!(pids_on_disk(&dir, uid).len() > split.len(), "the sources' files are left for the next open");
    close(&dir, db);
    let db = open(&dir).await;
    assert_eq!(layout(&db), split, "rolled forward");
    assert_eq!(fingerprint(&db).await, before, "rolled forward: the rows");
    assert_files_match(&db, &dir, "rolled forward").await;
    // The retired pids are gone from the map log: a third open is clean.
    let state_parts = {
        let pt = db.partitioned_table("pt").unwrap();
        let board = pt.pin_board().unwrap();
        let state = board.state.lock().await;
        state.parts().filter(|(_, p)| p.retired).count()
    };
    assert_eq!(state_parts, 0, "RETIRED_GONE recorded");
    close(&dir, db);
}

/// `STORAGE MOVE PARTITION` to a second storage location, and back; a
/// directory the pool does not hold is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn move_partition_follows_the_storage_locations() {
    let tmp = tempfile::tempdir().unwrap();
    let root: PathBuf = tmp.path().join("root");
    let disk: PathBuf = tmp.path().join("disk");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&disk).unwrap();
    let db = populated(&root).await;
    let before = fingerprint(&db).await;
    let uid = db.partitioned_table("pt").unwrap().uid();
    let refused = try_exec(&db, &format!("STORAGE MOVE PARTITION pt FOR (tenant_id = 7) TO '{}'", disk.display()))
        .await
        .unwrap_err();
    assert!(refused.contains("not a storage location"), "{refused}");
    db.storage
        .add_location(&disk.to_string_lossy(), crate::core::storage::LocationRole::Primary, 1, true)
        .unwrap();
    exec(&db, &format!("STORAGE MOVE PARTITION pt FOR (tenant_id = 7) TO '{}'", disk.display())).await;
    assert_eq!(pids_on_disk(&disk, uid).len(), 1, "one partition on the disk");
    assert_eq!(fingerprint(&db).await, before, "after the move");
    close(&root, db);
    let db = open(&root).await;
    assert_eq!(fingerprint(&db).await, before, "after a restart");
    assert_eq!(pids_on_disk(&disk, uid).len(), 1, "still there");
    exec(&db, &format!("STORAGE MOVE PARTITION pt FOR (tenant_id = 7) TO '{}'", root.display())).await;
    assert!(pids_on_disk(&disk, uid).is_empty(), "moved back");
    assert_eq!(fingerprint(&db).await, before, "after moving back");
    assert_files_match(&db, &root, "after moving back").await;
    close(&root, db);
}

