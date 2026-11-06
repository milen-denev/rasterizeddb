//! Row moves (docs/partitioning.md §9, phase 6, "P9 as built"): an `UPDATE`
//! that changes a partition-key value moves the row to the partition its new
//! point routes to, in the statement's transaction: once per row however the
//! partitions are visited, its unique keys checked where it lands, all or
//! nothing, and `RETURNING` showing the new row. `ON CONFLICT DO UPDATE`
//! still refuses a move, as PostgreSQL does.

use std::path::Path;

use rclite::Arc;

use super::Database;
use crate::core::mvcc::transaction::TxId;

async fn open(dir: &Path) -> Arc<Database> {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::try_new_named(&dir.to_string_lossy(), false, "rowmove").await.expect("open the database"))
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

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    try_exec(db, sql).await.unwrap_or_else(|e| panic!("{e}"))
}

async fn try_exec_in(db: &Arc<Database>, tx: TxId, sql: &str) -> Result<Vec<u8>, String> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).ok_or_else(|| format!("parse {sql}"))?;
    let out = Box::pin(crate::core::rql::executor::execute_with_context(purpose, db.clone(), 0, tx)).await;
    if out.is_empty() || out[0] == 3 {
        return Err(format!("{sql}: {}", String::from_utf8_lossy(out.get(1..).unwrap_or_default())));
    }
    Ok(out)
}

/// The `i64` columns of every row `sql` returns, row by row.
async fn ints(db: &Arc<Database>, sql: &str) -> Vec<Vec<i64>> {
    let out = exec(db, sql).await;
    assert_eq!(out[0], 2, "not a row set: {sql}");
    crate::core::row::row::vec_into_rows(&out[1..])
        .unwrap()
        .iter()
        .map(|r| r.columns.iter().map(|c| i64::from_le_bytes(c.data.into_slice().try_into().unwrap())).collect())
        .collect()
}

async fn rows(db: &Arc<Database>) -> Vec<Vec<i64>> {
    ints(db, "SELECT tenant_id, grp, id, v FROM pt ORDER BY tenant_id, grp, id").await
}

/// 20 tenants × 3 groups × 4 ids over 8 partitions.
async fn populated(dir: &Path) -> Arc<Database> {
    let db = open(dir).await;
    exec(
        &db,
        "CREATE TABLE pt (tenant_id BIGINT NOT NULL, grp BIGINT NOT NULL, id BIGINT NOT NULL, v BIGINT NOT NULL, \
         PRIMARY KEY (tenant_id, grp, id)) PARTITION BY HASH (tenant_id, grp) PARTITIONS 8",
    )
    .await;
    for t in 1..=20 {
        let mut values = Vec::new();
        for g in 1..=3 {
            for i in 1..=4 {
                values.push(format!("({t}, {g}, {i}, {})", t * 100 + g * 10 + i));
            }
        }
        exec(&db, &format!("INSERT INTO pt (tenant_id, grp, id, v) VALUES {}", values.join(", "))).await;
    }
    db
}

/// One row moves: routed reads find it under its new key only, in the
/// partition that key routes to, and a restart keeps it there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_moved_row_is_found_under_its_new_key_only() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = populated(&dir).await;
    let before = rows(&db).await;
    let out = ints(&db, "UPDATE pt SET tenant_id = 99 WHERE tenant_id = 7 AND grp = 2 AND id = 3 RETURNING tenant_id, grp, id, v").await;
    assert_eq!(out, vec![vec![99, 2, 3, 723]], "RETURNING shows the new row");
    assert_eq!(ints(&db, "SELECT id FROM pt WHERE tenant_id = 99").await, vec![vec![3]]);
    assert!(ints(&db, "SELECT id FROM pt WHERE tenant_id = 7 AND grp = 2 AND id = 3").await.is_empty());
    let mut want = before.clone();
    for r in want.iter_mut() {
        if r[..3] == [7, 2, 3] {
            r[0] = 99;
        }
    }
    want.sort();
    assert_eq!(rows(&db).await, want);
    close(&dir, db);
    let db = open(&dir).await;
    assert_eq!(rows(&db).await, want, "after a restart");
    assert_eq!(ints(&db, "SELECT v FROM pt WHERE tenant_id = 99 AND grp = 2 AND id = 3").await, vec![vec![723]]);
    close(&dir, db);
}

/// Every row moves once, wherever the partition it moves to comes in the
/// statement's visiting order: a moved row inserted before a later
/// partition's visit would be matched again and moved twice.
///
/// Mutation: inserting each partition's moved rows right after its visit
/// (not after the last one): rows move by 2000 or more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_row_moves_once_whatever_the_visiting_order() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = populated(&dir).await;
    let before = rows(&db).await;
    let n = before.len() as i64;
    exec(&db, "UPDATE pt SET tenant_id = tenant_id + 1000").await;
    let after = rows(&db).await;
    assert_eq!(after.len() as i64, n, "every row once");
    let mut want: Vec<Vec<i64>> = before.iter().map(|r| vec![r[0] + 1000, r[1], r[2], r[3]]).collect();
    want.sort();
    assert_eq!(after, want, "each moved by exactly 1000");
    // A second level too: grp changes, tenant_id kept.
    exec(&db, "UPDATE pt SET grp = grp + 10 WHERE tenant_id = 1007").await;
    assert_eq!(ints(&db, "SELECT COUNT(*) FROM pt WHERE tenant_id = 1007 AND grp > 10").await, vec![vec![12]]);
    assert_eq!(ints(&db, "SELECT COUNT(*) FROM pt").await, vec![vec![n]]);
    close(&dir, db);
}

/// A move onto a key that exists fails the statement with 23505's text,
/// and nothing of it stays: the rows it had already retired come back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_move_onto_an_existing_key_changes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = populated(&dir).await;
    let before = rows(&db).await;
    // Tenant 3's rows go to tenant 4, which already holds every (grp, id).
    let e = try_exec(&db, "UPDATE pt SET tenant_id = 4 WHERE tenant_id = 3").await.unwrap_err();
    assert!(e.contains("duplicate") || e.contains("unique"), "{e}");
    assert_eq!(rows(&db).await, before, "nothing changed");
    // And in a block, rolled back by the caller: nothing.
    let x = db.transactions.begin();
    try_exec_in(&db, x, "UPDATE pt SET tenant_id = 500 WHERE tenant_id = 5").await.unwrap();
    db.abort_transaction(x);
    assert_eq!(rows(&db).await, before, "rolled back");
    close(&dir, db);
}

/// A second writer of a row another transaction moved and has not
/// committed fails rather than moving it again; after the first commits,
/// the row is where it moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_concurrent_writer_of_a_moving_row_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = populated(&dir).await;
    let x = db.transactions.begin();
    try_exec_in(&db, x, "UPDATE pt SET tenant_id = 300 WHERE tenant_id = 9 AND grp = 1 AND id = 1").await.unwrap();
    let y = db.transactions.begin();
    let refused = try_exec_in(&db, y, "UPDATE pt SET tenant_id = 301 WHERE tenant_id = 9 AND grp = 1 AND id = 1").await;
    assert!(refused.is_err(), "the second mover went through: {refused:?}");
    db.abort_transaction(y);
    db.commit_written_transaction(x).unwrap();
    assert_eq!(ints(&db, "SELECT v FROM pt WHERE tenant_id = 300").await, vec![vec![911]]);
    assert!(ints(&db, "SELECT v FROM pt WHERE tenant_id = 301").await.is_empty());
    assert!(ints(&db, "SELECT v FROM pt WHERE tenant_id = 9 AND grp = 1 AND id = 1").await.is_empty());
    close(&dir, db);
}

/// `ON CONFLICT DO UPDATE` refuses a move, as PostgreSQL does, and writes
/// nothing; one that keeps the point runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_conflict_do_update_refuses_a_move() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let db = populated(&dir).await;
    let before = rows(&db).await;
    let e = try_exec(
        &db,
        "INSERT INTO pt (tenant_id, grp, id, v) VALUES (2, 1, 1, 0) \
         ON CONFLICT (tenant_id, grp, id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id + 100",
    )
    .await
    .unwrap_err();
    assert!(e.contains("invalid ON UPDATE specification") && e.contains("not supported"), "{e}");
    assert_eq!(rows(&db).await, before);
    exec(
        &db,
        "INSERT INTO pt (tenant_id, grp, id, v) VALUES (2, 1, 1, 0) \
         ON CONFLICT (tenant_id, grp, id) DO UPDATE SET v = 5",
    )
    .await;
    assert_eq!(ints(&db, "SELECT v FROM pt WHERE tenant_id = 2 AND grp = 1 AND id = 1").await, vec![vec![5]]);
    close(&dir, db);
}
