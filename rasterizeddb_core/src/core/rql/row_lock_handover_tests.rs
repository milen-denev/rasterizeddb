//! A writer's row mark holds its row while the writer's commit is recorded
//! but not yet visible.
//!
//! `TransactionManager::commit` records the outcome, then retires the
//! transaction from the running set, and only a snapshot taken after the
//! retirement sees the commit. A locker that counted the writer as gone at
//! the recorded outcome kept the row's lock in between; its locking read then
//! scanned again at a snapshot that still could not see the commit, and,
//! finding the row locked by itself already, returned it as scanned. The
//! `SKIP LOCKED` job queue handed a finished job to a second worker, which
//! marked it again by key (compat `composite_row_locks.rs`, "job (4,25)
//! claimed by workers 4 and 5").
//!
//! Each test parks a claimer's `COMMIT` in that window
//! (`commit_test_hooks::after_record`) and runs the second claim inside it.

use std::sync::{Mutex, mpsc};
use std::time::Duration;

use super::*;
use crate::core::mvcc::owner::LockOwner;
use crate::core::mvcc::transaction::commit_test_hooks;

const BOUND: Duration = Duration::from_secs(20);

const CLAIM: &str = "SELECT tenant_id, id FROM q WHERE state = 'new' LIMIT 1 FOR UPDATE SKIP LOCKED";

async fn queue() -> Arc<Database> {
    let db = crate::replication::test_support::in_memory_database().await;
    for sql in [
        "CREATE TABLE q (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, state TEXT NOT NULL, \
         claimed_by BIGINT, PRIMARY KEY (tenant_id, id))",
        "INSERT INTO q (tenant_id, id, state) VALUES (1, 1, 'new'), (1, 2, 'new')",
    ] {
        let response = execute(recognize_query_purpose(sql).unwrap(), db.clone()).await;
        assert_ne!(response.first(), Some(&3), "{sql}: {}", String::from_utf8_lossy(&response));
    }
    db
}

async fn in_block(db: &Arc<Database>, tx: TxId, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("{sql}: not recognised"));
    let response = execute_in_txn(purpose, db.clone(), tx).await;
    assert_ne!(response.first(), Some(&3), "{sql}: {}", String::from_utf8_lossy(&response));
    response
}

fn int(column: &crate::core::row::row::Column) -> i64 {
    let bytes = column.data.into_slice();
    i64::from_le_bytes(bytes[..8].try_into().expect("a BIGINT cell"))
}

/// The (tenant_id, id) pairs a claim returned.
async fn claim(db: &Arc<Database>, tx: TxId) -> Vec<(i64, i64)> {
    let response = in_block(db, tx, CLAIM).await;
    assert_eq!(response.first(), Some(&2), "{}", String::from_utf8_lossy(&response));
    crate::core::row::row::vec_into_rows(&response[1..])
        .unwrap()
        .iter()
        .map(|row| (int(&row.columns[0]), int(&row.columns[1])))
        .collect()
}

async fn mark(db: &Arc<Database>, tx: TxId, worker: i64, (tenant, id): (i64, i64)) {
    in_block(
        db,
        tx,
        &format!("UPDATE q SET state = 'done', claimed_by = {worker} WHERE tenant_id = {tenant} AND id = {id}"),
    )
    .await;
}

fn end(db: &Arc<Database>, tx: TxId) {
    db.commit_transaction(tx);
    db.release_owner(LockOwner::block(tx).unwrap());
}

/// (state, claimed_by) of every row, by key. Every row is claimed by the
/// time this is read, so `claimed_by` is never NULL.
async fn stored(db: &Arc<Database>) -> Vec<((i64, i64), String, i64)> {
    let sql = "SELECT tenant_id, id, state, claimed_by FROM q ORDER BY tenant_id, id";
    let response = execute(recognize_query_purpose(sql).unwrap(), db.clone()).await;
    assert_eq!(response.first(), Some(&2), "{}", String::from_utf8_lossy(&response));
    crate::core::row::row::vec_into_rows(&response[1..])
        .unwrap()
        .iter()
        .map(|row| {
            let state = String::from_utf8_lossy(row.columns[2].data.into_slice()).to_string();
            ((int(&row.columns[0]), int(&row.columns[1])), state, int(&row.columns[3]))
        })
        .collect()
}

/// Commits `tx` on a thread of its own and parks it between its recorded
/// outcome and its retirement; returns the thread and the sender that lets
/// it go on. Waits until it is parked.
fn park_commit(db: &Arc<Database>, tx: TxId) -> (std::thread::JoinHandle<()>, mpsc::Sender<()>) {
    let (reached, parked) = mpsc::channel::<()>();
    let (go, resume) = mpsc::channel::<()>();
    let resume = Mutex::new(resume);
    commit_test_hooks::after_record(&db.transactions, tx, move || {
        reached.send(()).unwrap();
        resume.lock().unwrap().recv_timeout(BOUND).expect("the test never let the commit go on");
    });
    let committer = {
        let db = db.clone();
        std::thread::spawn(move || end(&db, tx))
    };
    parked.recv_timeout(BOUND).expect("the commit never reached the hook");
    (committer, go)
}

/// Worker A claims job (1,1) and marks it done; its `COMMIT` is parked
/// with the outcome recorded and A still running. Worker B claims inside
/// that window. A's write marked (1,1), and the mark holds until A is
/// retired, so B skips it and is handed (1,2); and what is stored names A
/// for (1,1).
///
/// Mutation: `acquire_row_lock`'s `owner_live` is `owner_live` alone (as it
/// was): B takes A's lock (A's outcome is no longer `InProgress`) and
/// ignores its mark, its locking read scans again at a snapshot that cannot
/// see A's commit, returns (1,1) as `'new'`, and B's mark would overwrite
/// A's: the assertion on B's claim fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lock_is_not_handed_on_before_its_holders_commit_is_visible() {
    let db = queue().await;
    let a = db.transactions.begin();
    assert_eq!(claim(&db, a).await, vec![(1, 1)]);
    mark(&db, a, 1, (1, 1)).await;

    let (committer, go) = park_commit(&db, a);
    let b = db.transactions.begin();
    let got = claim(&db, b).await;
    // B marks what it was handed only once A is retired: a write of a row
    // A wrote would wait on A, which is parked.
    go.send(()).unwrap();
    committer.join().expect("A's commit");
    for job in &got {
        mark(&db, b, 2, *job).await;
    }
    end(&db, b);

    assert_eq!(got, vec![(1, 2)], "B was handed the job A had finished");
    assert_eq!(
        stored(&db).await,
        vec![((1, 1), "done".to_string(), 1), ((1, 2), "done".to_string(), 2)]
    );
}

/// The same window, met through a writer's row mark rather than its lock:
/// A writes (1,1) with no row locked anywhere, so it takes no lock and only
/// marks the row (`RowLocks::mark_row_writes`). Its `COMMIT` is parked as
/// above, and B's claim inside the window skips (1,1), whose writer is
/// still running.
///
/// Mutation: `acquire_row_lock`'s `owner_live` is `owner_live` alone (as it
/// was): B ignores A's mark, and returns (1,1) as `'new'` after A wrote it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writers_mark_holds_its_row_until_its_commit_is_visible() {
    let db = queue().await;
    let a = db.transactions.begin();
    assert!(db.row_locks.is_empty(), "the fast path needs no row locked");
    mark(&db, a, 1, (1, 1)).await;
    assert!(db.row_locks.is_empty(), "A took a row lock: this is not the marked path");

    let (committer, go) = park_commit(&db, a);
    let b = db.transactions.begin();
    let got = claim(&db, b).await;
    // B marks what it was handed only once A is retired: a write of a row
    // A wrote would wait on A, which is parked.
    go.send(()).unwrap();
    committer.join().expect("A's commit");
    for job in &got {
        mark(&db, b, 2, *job).await;
    }
    end(&db, b);

    assert_eq!(got, vec![(1, 2)], "B was handed the row A had written");
    assert_eq!(
        stored(&db).await,
        vec![((1, 1), "done".to_string(), 1), ((1, 2), "done".to_string(), 2)]
    );
}
