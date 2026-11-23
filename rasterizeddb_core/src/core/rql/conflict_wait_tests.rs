//! Conflict retries wait on the transaction they met (slice B6, finding L1),
//! at each of the three retry sites, and an autocommit reader is one whose
//! owner has written nothing (finding L8).
//!
//! No assertion here reads a clock: a wait is shown by the per-site counters
//! in `RowLocks::conflict_waits` (a wait begun on a live blocker, a wait
//! ended by its end) and by the wait-for edge in the graph.

use std::time::Duration;

use rclite::Arc;

use super::*;
use crate::core::mvcc::owner::{LockOwner, OwnerKind};
use crate::core::mvcc::table_locks::{Request, TableMode, WaitRole};
use crate::core::rql::executor::{execute, execute_in_txn};
use crate::core::rql::lexer_s1::recognize_query_purpose;

/// The bound on anything that should happen.
const BOUND: Duration = Duration::from_secs(20);

async fn db() -> Arc<Database> {
    let db = crate::replication::test_support::in_memory_database().await;
    for sql in [
        "CREATE TABLE cw (id INT PRIMARY KEY, v TEXT)",
        "INSERT INTO cw (id, v) VALUES (1, 'a'), (2, 'b')",
    ] {
        let response = execute(recognize_query_purpose(sql).unwrap(), db.clone()).await;
        assert_ne!(response.first(), Some(&3), "{sql}: {}", String::from_utf8_lossy(&response));
    }
    db
}

fn purpose(sql: &str) -> crate::core::rql::lexer_s1::QueryPurpose {
    recognize_query_purpose(sql).unwrap_or_else(|| panic!("{sql}: not recognised"))
}

/// Runs `sql` in the block `tx`, expecting success.
async fn in_block(db: &Arc<Database>, tx: TxId, sql: &str) -> Vec<u8> {
    let response = execute_in_txn(purpose(sql), db.clone(), tx).await;
    assert_ne!(response.first(), Some(&3), "{sql}: {}", String::from_utf8_lossy(&response));
    response
}

fn end_block(db: &Arc<Database>, tx: TxId, commit: bool) {
    if commit {
        db.commit_transaction(tx);
    } else {
        db.abort_transaction(tx);
    }
    db.release_owner(LockOwner::block(tx).unwrap());
}

/// Waits (bounded) until `site` has begun at least `n` waits on a live
/// blocker.
async fn until_waiting(db: &Arc<Database>, site: Site, n: usize) {
    tokio::time::timeout(BOUND, async {
        while db.row_locks.conflict_waits.at(site as usize).0 < n {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{site:?}: no conflict wait began on the live blocker"));
}

fn rows_of(response: &[u8]) -> u64 {
    match response.first() {
        Some(0) | Some(1) if response.len() >= 9 => {
            let mut n = [0u8; 8];
            n.copy_from_slice(&response[1..9]);
            u64::from_le_bytes(n)
        }
        other => panic!("not a row count: {other:?} {}", String::from_utf8_lossy(response)),
    }
}

async fn value_of(db: &Arc<Database>, id: i32) -> Option<String> {
    let response = execute(purpose(&format!("SELECT v FROM cw WHERE id = {id}")), db.clone()).await;
    assert_eq!(response.first(), Some(&2), "{}", String::from_utf8_lossy(&response));
    let rows = crate::core::row::row::vec_into_rows(&response[1..]).unwrap();
    rows.first().map(|row| String::from_utf8_lossy(row.columns[0].data.into_slice()).to_string())
}

/// The autocommit loop: an UPDATE of a row a block has updated waits on that
/// block (a wait begun on a live blocker, counted at the autocommit site,
/// and ended by the block's end), and replays the moment the block commits, applying its write over the
/// block's.
///
/// Mutation: `execute_autocommit` passes no blocker to `pause` (the blind
/// back-off): no wait is counted at the site and the test fails waiting for
/// one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_autocommit_conflict_waits_on_its_blocker_and_proceeds_when_it_commits() {
    let db = db().await;
    let block = db.transactions.begin();
    in_block(&db, block, "UPDATE cw SET v = 'block' WHERE id = 1").await;

    let (db2, before) = (db.clone(), db.row_locks.conflict_waits.at(Site::Autocommit as usize));
    let writer = tokio::spawn(async move {
        execute(purpose("UPDATE cw SET v = 'auto' WHERE id = 1"), db2).await
    });
    until_waiting(&db, Site::Autocommit, before.0 + 1).await;
    assert!(!writer.is_finished(), "the writer finished while its blocker ran");
    end_block(&db, block, true);
    let response = tokio::time::timeout(BOUND, writer).await.expect("the writer never woke").unwrap();
    assert_eq!(rows_of(&response), 1, "{}", String::from_utf8_lossy(&response));
    let after = db.row_locks.conflict_waits.at(Site::Autocommit as usize);
    assert!(after.1 > before.1, "the wait did not end because its blocker did: {before:?} -> {after:?}");
    assert_eq!(value_of(&db, 1).await.as_deref(), Some("auto"));
}

/// The block loop: a block's UPDATE of a row another block updated waits on
/// it, with a wait-for edge from its own root to the other block once
/// `deadlock_timeout` has passed (well inside the retry budget), and replays when it commits
/// (`READ COMMITTED`: the replay reads the committed version and writes over
/// it).
///
/// Mutation: `execute_in_txn_inner` passes no blocker to `pause`: no wait is
/// counted at the block site.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_conflict_waits_on_its_blocker_with_an_edge() {
    let db = db().await;
    let first = db.transactions.begin();
    in_block(&db, first, "UPDATE cw SET v = 'first' WHERE id = 2").await;
    let second = db.transactions.begin();
    let before = db.row_locks.conflict_waits.at(Site::Block as usize);
    let db2 = db.clone();
    let writer = tokio::spawn(async move {
        execute_in_txn(purpose("UPDATE cw SET v = 'second' WHERE id = 2"), db2, second).await
    });
    until_waiting(&db, Site::Block, before.0 + 1).await;
    tokio::time::timeout(BOUND, async {
        while !db.row_locks.waiting_on(second).contains(&first) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the block's conflict wait never entered an edge to the other block");
    end_block(&db, first, true);
    let response = tokio::time::timeout(BOUND, writer).await.expect("the block never woke").unwrap();
    assert_eq!(rows_of(&response), 1, "{}", String::from_utf8_lossy(&response));
    assert!(db.row_locks.conflict_waits.at(Site::Block as usize).1 > before.1);
    end_block(&db, second, true);
    assert_eq!(value_of(&db, 2).await.as_deref(), Some("second"));
}

/// The batch loop: a multi-row INSERT whose second tuple's key another
/// block is writing waits on that block at the tuple, and once it rolls
/// back every tuple lands (the first one stays written throughout: the
/// statement as a whole could not be replayed).
///
/// Mutation: `insert_values_tuples` passes no blocker to `pause`: no wait is
/// counted at the batch site.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_insert_waits_at_the_tuple_on_the_key_in_flight() {
    let db = db().await;
    let other = db.transactions.begin();
    in_block(&db, other, "INSERT INTO cw (id, v) VALUES (5, 'other')").await;
    let mine = db.transactions.begin();
    let before = db.row_locks.conflict_waits.at(Site::Batch as usize);
    let db2 = db.clone();
    let writer = tokio::spawn(async move {
        execute_in_txn(
            purpose("INSERT INTO cw (id, v) VALUES (4, 'm'), (5, 'm'), (6, 'm')"),
            db2,
            mine,
        )
        .await
    });
    until_waiting(&db, Site::Batch, before.0 + 1).await;
    assert!(!writer.is_finished());
    end_block(&db, other, false);
    let response = tokio::time::timeout(BOUND, writer).await.expect("the batch never woke").unwrap();
    assert_eq!(rows_of(&response), 3, "{}", String::from_utf8_lossy(&response));
    assert!(db.row_locks.conflict_waits.at(Site::Batch as usize).1 > before.1);
    end_block(&db, mine, true);
    for id in [4, 5, 6] {
        assert_eq!(value_of(&db, id).await.as_deref(), Some("m"), "row {id}");
    }
}

/// A blocker of the waiter's own family is never waited on: a block's later
/// savepoint writing a key its earlier savepoint holds in flight would wait
/// for itself. It is reported at once as the duplicate it is, with no wait.
///
/// Mutation: `pause` drops the own-family filter: the statement waits out
/// the budget (a wait is counted).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflict_with_the_waiters_own_family_is_not_waited_on() {
    let db = db().await;
    let root = db.transactions.begin();
    let sp1 = db.transactions.begin_savepoint(root);
    in_block(&db, sp1, "INSERT INTO cw (id, v) VALUES (7, 'x')").await;
    let sp2 = db.transactions.begin_savepoint(sp1);
    let before = db.row_locks.conflict_waits.at(Site::Block as usize);
    let response = tokio::time::timeout(
        BOUND,
        execute_in_txn(purpose("INSERT INTO cw (id, v) VALUES (7, 'y')"), db.clone(), sp2),
    )
    .await
    .expect("the own-family conflict hung");
    assert_eq!(response.first(), Some(&3), "{}", String::from_utf8_lossy(&response));
    assert_eq!(db.row_locks.conflict_waits.at(Site::Block as usize).0, before.0, "it waited on itself");
    end_block(&db, root, false);
}

/// Finding L8: an autocommit statement's queued ACCESS SHARE is a reader
/// (never a victim) while its owner has written nothing, and `Other`
/// (eligible) once an attempt acting for it has written.
///
/// Mutation: `refine_role` returns the provisional role (mode-based, as
/// before L8): the writer's request stays a reader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_autocommit_owner_that_wrote_is_no_reader() {
    let db = db().await;
    let holder = LockOwner::block(db.transactions.begin()).unwrap();
    assert!(matches!(
        db.row_locks.table_locks().request(holder, "cw", TableMode::AccessExclusive, WaitRole::Other),
        Request::Granted
    ));
    for wrote in [false, true] {
        let (transactions, row_locks) = (db.transactions.clone(), db.row_locks.clone());
        let db2 = db.clone();
        let role = crate::core::mvcc::owner::statement(&transactions, &row_locks, OwnerKind::Statement, async move {
            let owner = crate::core::mvcc::owner::ensure().unwrap();
            let attempt = db2.transactions.begin_autocommit();
            let _alias = db2.row_locks.owner_alias(attempt, owner.owner);
            if wrote {
                db2.transactions.mark_wrote(attempt);
            }
            let role = {
                let wait = db2.lock_table("cw", Some(owner), TableMode::AccessShare);
                tokio::pin!(wait);
                tokio::time::timeout(BOUND, async {
                    loop {
                        tokio::select! {
                            out = &mut wait => panic!("granted beside a held ACCESS EXCLUSIVE: {out:?}"),
                            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                        }
                        if let Some((_, role)) = db2.row_locks.table_locks().waiting_for(owner.owner) {
                            break role;
                        }
                    }
                })
                .await
                .expect("the request never queued")
            };
            db2.abort_transaction(attempt);
            role
        })
        .await;
        let want = if wrote { WaitRole::Other } else { WaitRole::AutocommitReader };
        assert_eq!(role, want, "wrote = {wrote}");
    }
    db.release_owner(holder);
}
