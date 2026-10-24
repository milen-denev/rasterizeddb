//! Lock owners (slice B2): O's lifetime and the status log, aliases and
//! release by space, the scope's every exit path, every sub-execution of an
//! engine statement under one owner, both retry loops, and the named owners
//! of the native protocol and login.
//!
//! The wire's pieces (CTE, derived and catalog bodies and their drops, the
//! `WITH` transaction as O, a block's owner at every block end) are in
//! `pgwire/owner_tests.rs`; replication apply's and the importer's beside
//! their code.

use std::collections::HashSet;
use std::sync::Arc as StdArc;
use std::time::Duration;

use rclite::Arc;

use super::probe::{self, Record, Site};
use super::*;
use crate::core::database::Database;
use crate::core::mvcc::TxOutcome;
use crate::core::mvcc::row_locks::{Acquired, TableLockMode};
use crate::core::mvcc::status::StatusLog;
use crate::core::mvcc::transaction::FROZEN_TX;
use crate::core::rql::executor;
use crate::core::rql::lexer_s1::recognize_query_purpose;

// ------------------------------------------------------------- fixtures

async fn db() -> Arc<Database> {
    crate::replication::test_support::in_memory_database().await
}

fn purpose(sql: &str) -> crate::core::rql::lexer_s1::QueryPurpose {
    recognize_query_purpose(sql).unwrap_or_else(|| panic!("could not parse {sql}"))
}

fn assert_ok(sql: &str, out: &[u8]) {
    assert!(
        !out.is_empty() && out[0] != 3,
        "{sql} failed: {}",
        String::from_utf8_lossy(out)
    );
}

/// One autocommit statement, as the wire's engine path runs it.
async fn run(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let out = executor::execute_with_sync(purpose(sql), db.clone(), false).await;
    assert_ok(sql, &out);
    out
}

/// One statement inside `tx`, which stays open.
async fn run_in(db: &Arc<Database>, tx: TxId, sql: &str) -> Vec<u8> {
    let out = executor::execute_in_txn(purpose(sql), db.clone(), tx).await;
    assert_ok(sql, &out);
    out
}

/// One autocommit statement, with what every probe inside it recorded.
async fn captured(db: &Arc<Database>, sql: &str) -> (Vec<u8>, Vec<Record>) {
    probe::capture(executor::execute_with_sync(purpose(sql), db.clone(), false)).await
}

const LIVE: &dyn Fn(TxId) -> bool = &|_| true;

/// Every record ran under one owner of `kind`, which read `InProgress` at
/// each site, and every transaction a piece ran under acts for it. Returns it.
fn one_owner(what: &str, records: &[Record], kind: OwnerKind) -> LockOwner {
    assert!(
        !records.is_empty(),
        "{what}: no piece of the statement was recorded"
    );
    let first = records[0]
        .owner
        .unwrap_or_else(|| panic!("{what}: {:?} ran under no owner", records[0].site));
    assert_eq!(
        first.kind, kind,
        "{what}: the owner acts for the wrong kind"
    );
    for r in records {
        let o = r
            .owner
            .unwrap_or_else(|| panic!("{what}: {:?} ran under no owner", r.site));
        assert_eq!(
            o, first,
            "{what}: {:?} ran under {:?}, the statement under {:?}",
            r.site, o, first
        );
        assert!(
            r.owner_in_progress,
            "{what}: the owner was not in progress at {:?}",
            r.site
        );
        if r.tx != FROZEN_TX {
            assert_eq!(
                r.owner_of_tx,
                first.owner.id(),
                "{what}: {:?} ran under tx {}, which acts for {} rather than the statement's owner {}",
                r.site,
                r.tx,
                r.owner_of_tx,
                first.owner.id()
            );
        }
    }
    first.owner
}

fn sites(records: &[Record]) -> HashSet<Site> {
    records.iter().map(|r| r.site).collect()
}

/// O was ended and holds nothing: the scope's end ran.
fn assert_ended(db: &Arc<Database>, what: &str, o: LockOwner) {
    assert_ne!(
        db.transactions.outcome(o.id()),
        TxOutcome::InProgress,
        "{what}: the owner {} was never ended",
        o.id()
    );
    assert!(
        !db.row_locks.owner_holds_tables(o.id()),
        "{what}: the owner still holds a table lock"
    );
    assert_eq!(
        db.row_locks.alias_len(),
        0,
        "{what}: an alias outlived its statement"
    );
}

// ------------------------------------------- O and the transaction manager

/// O reads `InProgress` from its begin to its end, however many
/// transactions start and end around it and however far the status log
/// settles and forgets meanwhile.
#[test]
fn o_reads_in_progress_through_settling_and_forgetting() {
    let m = TransactionManager::new(StatusLog::in_memory_with(true));
    let table = m.register_table();
    // Enough history below O for the log to settle some of it, so a horizon
    // that stops at O is seen to stop there rather than never to move.
    for _ in 0..10_000 {
        let tx = m.begin_autocommit();
        m.commit(tx);
    }
    let o = m.begin_owner();
    for round in 0..3 {
        for _ in 0..10_000 {
            let tx = m.begin_autocommit();
            if tx % 7 == 0 {
                m.abort(tx);
            } else {
                m.commit(tx);
            }
        }
        m.settle_now();
        table.advance(m.settle_bound());
        m.try_forget();
        assert_eq!(
            m.outcome(o),
            TxOutcome::InProgress,
            "round {round}: O settled or forgotten while running"
        );
    }
    assert!(
        m.status().settled_below() > 0,
        "the log settled nothing, so the test proved nothing"
    );
    assert!(m.status().settled_below() <= o, "the log settled past O");
    m.end_owner(o);
    assert_eq!(m.outcome(o), TxOutcome::Committed);
    assert!(!m.is_owner_only(o));
}

/// O holds no snapshot, so the in-place gate looks past it; a real reader
/// still closes it.
#[test]
fn the_in_place_gate_looks_past_an_owner() {
    let m = StdArc::new(TransactionManager::in_memory());
    let o = m.begin_owner();
    let attempt = m.begin_autocommit();
    assert!(m.is_owner_only(o) && !m.is_owner_only(attempt));
    assert!(
        m.try_begin_in_place_for(attempt).is_some(),
        "the statement's own O made the in-place gate decline"
    );
    let reader = m.begin_autocommit();
    assert!(
        m.try_begin_in_place_for(attempt).is_none(),
        "a real reader must close the gate"
    );
    m.commit(reader);
    m.commit(attempt);
    m.end_owner(o);
}

// --------------------------------------------- aliases, release by space

fn row_locks(m: &StdArc<TransactionManager>) -> StdArc<RowLocks> {
    StdArc::new(RowLocks::for_transactions(m.clone()))
}

/// Finding L4: an attempt's end frees its row locks and keeps its owner's
/// table locks; the owner's release frees those.
#[test]
fn an_attempts_end_keeps_its_owners_table_locks() {
    let m = StdArc::new(TransactionManager::in_memory());
    let locks = row_locks(&m);
    let o = LockOwner::autocommit(m.begin_owner()).unwrap();
    let other = m.begin();

    let a1 = m.begin_autocommit();
    let alias = locks.owner_alias(a1, o);
    assert_eq!(locks.owner_of(a1), o.id());
    assert_eq!(locks.txn_of(a1), a1);
    assert_eq!(
        locks.acquire_table("t", a1, TableLockMode::Exclusive, LIVE),
        Acquired::Yes
    );
    assert_eq!(
        locks.acquire(
            "t",
            b"k",
            a1,
            crate::core::rql::row_lock::LockStrength::Update,
            LIVE
        ),
        Acquired::Yes
    );
    assert!(
        locks.owner_holds_tables(o.id()),
        "the attempt's table lock is O's"
    );
    assert!(
        locks.txn_holds_rows(a1),
        "the attempt's row lock is its own"
    );

    // Attempt 1 conflicts and is aborted.
    locks.release_rows(a1);
    drop(alias);
    assert!(
        !locks.txn_holds_rows(a1),
        "attempt 1's row lock carried past its end"
    );
    assert_eq!(
        locks.table_conflict("t", other, true, LIVE),
        Some(o.id()),
        "attempt 1's abort released O's table lock"
    );

    // Attempt 2 acts for the same O: the lock is already its own.
    let a2 = m.begin_autocommit();
    let alias = locks.owner_alias(a2, o);
    assert_eq!(locks.owner_of(a2), o.id(), "attempt 2 is not aliased to O");
    assert_eq!(
        locks.acquire_table("t", a2, TableLockMode::Exclusive, LIVE),
        Acquired::Yes
    );
    assert_eq!(
        locks.table_conflict("t", a2, true, LIVE),
        None,
        "O's own lock conflicted with its attempt"
    );
    locks.release_rows(a2);
    drop(alias);
    assert_eq!(locks.owner_of(a2), a2, "the alias outlived its guard");

    locks.release_owner(o);
    assert!(!locks.owner_holds_tables(o.id()));
    assert_eq!(locks.table_conflict("t", other, true, LIVE), None);
    assert_eq!(locks.alias_len(), 0);
}

/// A block is its own owner: release by space changes nothing for it except
/// when, and its row locks still end with it.
#[test]
fn a_blocks_rows_end_with_it_and_its_table_locks_with_its_owner() {
    let m = StdArc::new(TransactionManager::in_memory());
    let locks = row_locks(&m);
    let block = m.begin();
    let other = m.begin();
    assert_eq!(
        locks.acquire_table("t", block, TableLockMode::Shared, LIVE),
        Acquired::Yes
    );
    assert_eq!(
        locks.acquire(
            "t",
            b"k",
            block,
            crate::core::rql::row_lock::LockStrength::Update,
            LIVE
        ),
        Acquired::Yes
    );
    locks.release_rows(block);
    assert!(!locks.txn_holds_rows(block));
    assert!(
        locks.owner_holds_tables(block),
        "the commit released the block's table lock before durability"
    );
    assert_eq!(locks.table_conflict("t", other, true, LIVE), Some(block));
    locks.release_owner(LockOwner::block(block).unwrap());
    assert_eq!(locks.table_conflict("t", other, true, LIVE), None);
}

// ------------------------------------------------------------ the scope

/// A lazy scope begins O once, on the first ask; a nested statement joins it;
/// its end releases O's locks and ends O.
#[tokio::test]
async fn a_statement_scope_mints_once_joins_nested_and_ends_o() {
    let db = db().await;
    let (t, l) = (db.transactions.clone(), db.row_locks.clone());
    let (o, nested) = owner_statement(&t, &l, async {
        assert!(in_scope());
        assert_eq!(current(), None, "O was begun before anything asked for it");
        let o = ensure().unwrap();
        assert_eq!(ensure().unwrap(), o, "O was begun twice");
        assert_eq!(o.kind, OwnerKind::Statement);
        assert_eq!(db.transactions.outcome(o.owner.id()), TxOutcome::InProgress);
        let nested = statement(&t, &l, OwnerKind::Native, async { ensure().unwrap() }).await;
        db.row_locks
            .acquire_table("t", o.owner.id(), TableLockMode::Exclusive, LIVE);
        (o, nested)
    })
    .await;
    assert_eq!(nested, o, "a nested statement opened a second owner");
    assert_ended(&db, "a finished statement", o.owner);
    assert_eq!(db.transactions.outcome(o.owner.id()), TxOutcome::Committed);
}

fn owner_statement<'a, F: std::future::Future + 'a>(
    t: &'a StdArc<TransactionManager>,
    l: &'a StdArc<RowLocks>,
    fut: F,
) -> impl std::future::Future<Output = F::Output> + 'a {
    statement(t, l, OwnerKind::Statement, fut)
}

/// A statement that is never asked for its owner costs no id.
#[tokio::test]
async fn a_statement_that_takes_no_lock_begins_no_owner() {
    let db = db().await;
    let before = db.transactions.next_id();
    statement(
        &db.transactions,
        &db.row_locks,
        OwnerKind::Statement,
        async {},
    )
    .await;
    assert_eq!(db.transactions.next_id(), before);
}

/// A block's scope resolves to its root and releases nothing at its end:
/// the block outlives the statement, and its end releases the owner.
#[tokio::test]
async fn a_block_scope_is_its_root_and_outlives_the_statement() {
    let db = db().await;
    let root = db.transactions.begin();
    let release = OwnerRelease::for_root(&db.row_locks, root);
    let o = under(&db.transactions, root, OwnerKind::Block, async {
        let o = ensure().unwrap();
        db.row_locks
            .acquire_table("t", root, TableLockMode::Exclusive, LIVE);
        o
    })
    .await;
    assert_eq!(o.owner, LockOwner::block(root).unwrap());
    assert_eq!(o.kind, OwnerKind::Block);
    assert!(
        db.row_locks.owner_holds_tables(root),
        "a statement's end released its block's lock"
    );
    db.commit_transaction(root);
    assert!(
        db.row_locks.owner_holds_tables(root),
        "the commit released the owner before durability (L4)"
    );
    drop(release);
    assert!(!db.row_locks.owner_holds_tables(root));
    // `FROZEN_TX` has no owner at all.
    let none = under(&db.transactions, FROZEN_TX, OwnerKind::Block, async {
        ensure()
    })
    .await;
    assert_eq!(none, None);
}

/// A `WITH` statement's transaction is O: adopted, it is a regular
/// transaction the statement ends, and the scope only releases it.
#[tokio::test]
async fn an_adopted_o_is_the_statements_transaction() {
    let db = db().await;
    let (o, tx) = statement(
        &db.transactions,
        &db.row_locks,
        OwnerKind::Statement,
        async {
            let tx = adopt_transaction().expect("an unminted autocommit scope adopts");
            assert_eq!(adopt_transaction(), None, "adopted twice");
            let o = ensure().unwrap();
            assert!(
                !db.transactions.is_owner_only(tx),
                "an adopted O reads and writes"
            );
            db.row_locks
                .acquire_table("t", tx, TableLockMode::Exclusive, LIVE);
            (o, tx)
        },
    )
    .await;
    assert_eq!(o.owner.id(), tx);
    assert!(
        !db.row_locks.owner_holds_tables(tx),
        "the scope's end did not release an adopted O"
    );
    assert_eq!(
        db.transactions.outcome(tx),
        TxOutcome::InProgress,
        "the scope ended a transaction the statement owns"
    );
    db.commit_transaction(tx);
}

// ------------------------------------------ the engine's sub-executions

async fn fixture(db: &Arc<Database>) {
    for sql in [
        "CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)",
        "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE CASCADE)",
        "CREATE TABLE u (id INT PRIMARY KEY, v TEXT)",
        "INSERT INTO parent (id, name) VALUES (1, 'a')",
        "INSERT INTO parent (id, name) VALUES (2, 'b')",
        "INSERT INTO child (id, pid) VALUES (10, 1)",
        "INSERT INTO u (id, v) VALUES (1, 'x')",
    ] {
        run(db, sql).await;
    }
}

/// Every piece of one statement the engine runs (ON CONFLICT's update, a
/// foreign-key check, a cascade, RETURNING, a multi-row insert's tuples, a
/// subquery) runs under the statement's one owner, which reads in progress
/// throughout and ends with the statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_sub_execution_of_an_engine_statement_runs_under_its_owner() {
    let db = db().await;
    fixture(&db).await;
    let cases: &[(&str, &[Site])] = &[
        (
            "INSERT INTO u (id, v) VALUES (1, 'y') ON CONFLICT (id) DO UPDATE SET v = 'z'",
            &[Site::ConflictUpdate],
        ),
        (
            "INSERT INTO child (id, pid) VALUES (11, 2)",
            &[Site::ForeignKeyCheck],
        ),
        (
            "DELETE FROM parent WHERE id = 1",
            &[Site::Dispatch { depth: 1 }],
        ),
        (
            "UPDATE u SET v = 'r' WHERE id = 1 RETURNING v",
            &[Site::Returning],
        ),
        (
            "INSERT INTO u (id, v) VALUES (5, 'p'), (6, 'q')",
            &[Site::BatchTuple],
        ),
        (
            "SELECT id FROM u WHERE id IN (SELECT id FROM parent)",
            &[Site::Subquery],
        ),
    ];
    for (sql, wanted) in cases {
        let (out, records) = captured(&db, sql).await;
        assert_ok(sql, &out);
        let o = one_owner(sql, &records, OwnerKind::Statement);
        let seen = sites(&records);
        if wanted.contains(&Site::ConflictUpdate) {
            // The INSERT and the UPDATE it re-dispatches: a re-dispatch that
            // left the statement's task (and so its owner) records nothing.
            let dispatches = records
                .iter()
                .filter(|r| matches!(r.site, Site::Dispatch { .. }))
                .count();
            assert!(
                dispatches >= 2,
                "{sql}: the conflict update's re-dispatch was not seen ({dispatches})"
            );
        }
        for site in *wanted {
            assert!(
                seen.contains(site),
                "{sql}: {site:?} was not recorded (saw {seen:?})"
            );
        }
        assert!(seen.contains(&Site::Attempt), "{sql}: no attempt recorded");
        assert!(
            records
                .iter()
                .filter(|r| r.site == Site::Attempt)
                .all(|r| r.tx > o.id()),
            "{sql}: O is not older than its attempts"
        );
        assert_ended(&db, sql, o);
    }
}

/// The autocommit retry loop: every attempt is a fresh transaction aliased
/// to the same O, which reads in progress across all of them (and through
/// each attempt's abort) and ends once, after the last.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_autocommit_attempt_acts_for_the_same_o() {
    let db = db().await;
    fixture(&db).await;
    let block = db.transactions.begin();
    run_in(&db, block, "UPDATE u SET v = 'held' WHERE id = 1").await;

    // The statement's scope opened as the wire opens it, with O holding a
    // table lock, so an attempt's abort that released O's locks would show.
    let minted = StdArc::new(parking_lot::Mutex::new(None));
    let racer = {
        let db = db.clone();
        let minted = minted.clone();
        tokio::spawn(async move {
            probe::capture(statement(
                &db.transactions,
                &db.row_locks,
                OwnerKind::Statement,
                async {
                    let o = ensure().unwrap().owner;
                    db.row_locks
                        .acquire_table("t", o.id(), TableLockMode::Exclusive, LIVE);
                    *minted.lock() = Some(o);
                    executor::execute_with_sync(
                        purpose("UPDATE u SET v = 'won' WHERE id = 1"),
                        db.clone(),
                        false,
                    )
                    .await
                },
            ))
            .await
        })
    };
    // Long enough for a few attempts to conflict on the block's version.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let held = minted.lock().expect("the statement began its owner");
    assert!(
        db.row_locks.owner_holds_tables(held.id()),
        "an attempt's abort released its statement's table lock (finding L4)"
    );
    db.commit_transaction(block);
    let (out, records) = tokio::time::timeout(Duration::from_secs(20), racer)
        .await
        .expect("the retried statement finished")
        .unwrap();
    assert_ok("the retried UPDATE", &out);
    let o = one_owner("the retried UPDATE", &records, OwnerKind::Statement);
    assert_eq!(o, held);
    let attempts: Vec<TxId> = records
        .iter()
        .filter(|r| r.site == Site::Attempt)
        .map(|r| r.tx)
        .collect();
    assert!(
        attempts.len() >= 2,
        "the statement never retried ({attempts:?}), so nothing was tested"
    );
    let distinct: HashSet<TxId> = attempts.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        attempts.len(),
        "two attempts shared a transaction"
    );
    assert_ended(&db, "the retried UPDATE", o);
}

/// The in-transaction retry loop: a block's statement retries under the
/// block's own transaction, so under its root, every attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_in_block_attempt_acts_for_the_blocks_owner() {
    let db = db().await;
    fixture(&db).await;
    let holder = db.transactions.begin();
    run_in(&db, holder, "UPDATE u SET v = 'held' WHERE id = 1").await;
    let block = db.transactions.begin();
    let racer = {
        let db = db.clone();
        tokio::spawn(async move {
            probe::capture(executor::execute_in_txn(
                purpose("UPDATE u SET v = 'won' WHERE id = 1"),
                db.clone(),
                block,
            ))
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(80)).await;
    db.commit_transaction(holder);
    let (out, records) = tokio::time::timeout(Duration::from_secs(20), racer)
        .await
        .expect("the retried statement finished")
        .unwrap();
    assert_ok("the block's retried UPDATE", &out);
    let o = one_owner("the block's retried UPDATE", &records, OwnerKind::Block);
    assert_eq!(o, LockOwner::block(block).unwrap());
    let attempts = records
        .iter()
        .filter(|r| r.site == Site::InTxnAttempt)
        .count();
    assert!(
        attempts >= 2,
        "the block's statement never retried, so nothing was tested"
    );
    db.commit_transaction(block);
}

/// Finding L4 end to end: a block's `LOCK TABLE` survives another
/// statement's conflict-retry storm (each attempt's abort releases rows only),
/// and goes when the block's owner is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blocks_table_lock_survives_another_statements_retries() {
    let db = db().await;
    fixture(&db).await;
    run(&db, "CREATE TABLE locked (id INT PRIMARY KEY, v TEXT)").await;
    run(&db, "INSERT INTO locked (id, v) VALUES (1, 'a')").await;
    let block = db.transactions.begin();
    let release = OwnerRelease::for_root(&db.row_locks, block);
    run_in(&db, block, "LOCK TABLE locked IN EXCLUSIVE MODE").await;
    run_in(&db, block, "UPDATE u SET v = 'held' WHERE id = 1").await;

    // Retries: each conflicts on the block's version of u's row and aborts.
    let storm = {
        let db = db.clone();
        tokio::spawn(async move {
            executor::execute_with_sync(
                purpose("UPDATE u SET v = 'storm' WHERE id = 1"),
                db.clone(),
                false,
            )
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        db.row_locks.owner_holds_tables(block),
        "another statement's retries released the block's lock"
    );
    let refused = executor::execute_with_sync(
        purpose("UPDATE locked SET v = 'b' WHERE id = 1"),
        db.clone(),
        false,
    )
    .await;
    assert_eq!(
        refused.first(),
        Some(&3),
        "a write got past the block's EXCLUSIVE lock"
    );

    db.commit_transaction(block);
    drop(release);
    let out = tokio::time::timeout(Duration::from_secs(20), storm)
        .await
        .expect("storm ended")
        .unwrap();
    assert_ok("the storm's UPDATE", &out);
    assert!(!db.row_locks.owner_holds_tables(block));
    run(&db, "UPDATE locked SET v = 'c' WHERE id = 1").await;
}

/// The scope ends O on every way out: success, error, cancellation (the
/// statement's own timeout or a departed client), and its future dropped
/// mid-wait. O is made to hold a table lock first, so only its release frees
/// it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o_is_released_and_ended_on_every_exit_path() {
    let db = db().await;
    fixture(&db).await;

    // A statement scope, as the wire opens one, holding a table lock as O.
    async fn statement_holding<F: std::future::Future>(
        db: &Arc<Database>,
        body: impl FnOnce() -> F,
    ) -> (F::Output, LockOwner) {
        statement(
            &db.transactions,
            &db.row_locks,
            OwnerKind::Statement,
            async {
                let o = ensure().unwrap().owner;
                assert_eq!(
                    db.row_locks
                        .acquire_table("t", o.id(), TableLockMode::Exclusive, LIVE),
                    Acquired::Yes
                );
                (body().await, o)
            },
        )
        .await
    }

    // Success and error.
    let (out, o) = statement_holding(&db, || {
        executor::execute_with_sync(purpose("SELECT id FROM u WHERE id = 1"), db.clone(), false)
    })
    .await;
    assert_ok("success", &out);
    assert_ended(&db, "success", o);
    let (out, o) = statement_holding(&db, || {
        executor::execute_with_sync(purpose("SELECT nope FROM missing_table"), db.clone(), false)
    })
    .await;
    assert_eq!(out.first(), Some(&3));
    assert_ended(&db, "error", o);

    // A row lock wait the statement cannot win: a block holds the row.
    let block = db.transactions.begin();
    run_in(&db, block, "SELECT id FROM u WHERE id = 1 FOR UPDATE").await;
    let waiting = "SELECT id FROM u WHERE id = 1 FOR UPDATE";

    for reason in [
        crate::core::cancel::CancelReason::UserRequest,
        crate::core::cancel::CancelReason::ClientWentAway,
    ] {
        let cancel = std::sync::Arc::new(crate::core::cancel::StatementCancel::new(None));
        let trip = {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(60)).await;
                cancel.flag(reason);
            })
        };
        let (out, o) = crate::core::cancel::STATEMENT_CANCEL
            .scope(
                cancel,
                statement_holding(&db, || {
                    executor::execute_with_sync(purpose(waiting), db.clone(), false)
                }),
            )
            .await;
        trip.await.unwrap();
        assert_eq!(
            out.first(),
            Some(&3),
            "{reason:?}: the wait was not cancelled"
        );
        assert_ended(&db, &format!("{reason:?}"), o);
    }

    // The future dropped mid-wait: no cleanup code of the statement runs, only
    // drops.
    let minted = StdArc::new(parking_lot::Mutex::new(None));
    let seen = minted.clone();
    let dropped = tokio::time::timeout(
        Duration::from_millis(80),
        statement(
            &db.transactions,
            &db.row_locks,
            OwnerKind::Statement,
            async {
                let o = ensure().unwrap().owner;
                *seen.lock() = Some(o);
                db.row_locks
                    .acquire_table("t", o.id(), TableLockMode::Exclusive, LIVE);
                executor::execute_with_sync(purpose(waiting), db.clone(), false).await
            },
        ),
    )
    .await;
    assert!(
        dropped.is_err(),
        "the wait finished, so nothing was dropped"
    );
    let o = minted.lock().expect("the statement began its owner");
    assert_ended(&db, "a dropped statement", o);

    db.commit_transaction(block);
}

/// The native protocol and login role lookups run as named owners, never
/// unowned: one owner per statement, of their kind.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_native_protocol_and_login_run_under_named_owners() {
    let db = db().await;
    fixture(&db).await;

    let (out, records) = probe::capture(crate::core::database::run_native_query_for_tests(
        "UPDATE u SET v = 'native' WHERE id = 1",
        db.clone(),
    ))
    .await;
    assert_ok("native UPDATE", &out);
    let o = one_owner("native UPDATE", &records, OwnerKind::Native);
    assert_ended(&db, "native UPDATE", o);

    let (seeded, records) = probe::capture(crate::auth::ensure_role_catalogue(
        &db,
        "boot",
        Some("secret"),
        crate::auth::AuthMethod::ScramSha256,
    ))
    .await;
    seeded.expect("the role catalogue was seeded");
    assert!(!records.is_empty(), "the bootstrap recorded nothing");
    for r in &records {
        let o = r
            .owner
            .unwrap_or_else(|| panic!("bootstrap: {:?} ran under no owner", r.site));
        assert_eq!(
            o.kind,
            OwnerKind::Auth,
            "bootstrap: {:?} ran as {:?}",
            r.site,
            o.kind
        );
        assert!(r.owner_in_progress);
    }

    let (role, records) = probe::capture(crate::auth::lookup_role(&db, "boot")).await;
    assert!(role.is_some(), "the bootstrap role was not found");
    let o = one_owner("login lookup", &records, OwnerKind::Auth);
    assert_ended(&db, "login lookup", o);
}

// ------------------------------------------------------------- the cost

/// What O costs an autocommit statement (Q4 is B3b's gate; this is the
/// observation B2 reports): ids per statement, and the time of O's own work
/// (begin, alias, release, end) beside a whole point `SELECT`. Ignored: a
/// measurement, not an invariant. `cargo test --release -p rasterizeddb_core
/// --lib owner_cost -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn owner_cost() {
    let db = db().await;
    fixture(&db).await;
    let n: u64 = std::env::var("RDB_OWNER_COST_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000);
    let select = "SELECT id, v FROM u WHERE id = 1";
    for _ in 0..1_000 {
        run(&db, select).await;
    }

    let before = db.transactions.next_id();
    let started = std::time::Instant::now();
    for _ in 0..n {
        executor::execute_with_sync(purpose(select), db.clone(), false).await;
    }
    let statement = started.elapsed();
    let ids = db.transactions.next_id() - before;

    // O's own work, alone: begin it, alias one attempt, release, end.
    let attempt = db.transactions.begin_autocommit();
    let started = std::time::Instant::now();
    for _ in 0..n {
        let o = LockOwner::autocommit(db.transactions.begin_owner()).unwrap();
        let alias = db.row_locks.owner_alias(attempt, o);
        drop(alias);
        db.row_locks.release_owner(o);
        db.transactions.end_owner(o.id());
    }
    let owner = started.elapsed();
    db.transactions.commit(attempt);

    // And one plain autocommit id, the cost each attempt already paid.
    let started = std::time::Instant::now();
    for _ in 0..n {
        let tx = db.transactions.begin_autocommit();
        db.transactions.commit(tx);
    }
    let plain = started.elapsed();

    let per = |d: Duration| d.as_nanos() as f64 / n as f64;
    eprintln!(
        "owner_cost n={n}: ids per point SELECT {:.2}; point SELECT {:.0} ns; O's work {:.0} ns ({:.1}% of it); \
         one plain autocommit id {:.0} ns",
        ids as f64 / n as f64,
        per(statement),
        per(owner),
        100.0 * per(owner) / per(statement),
        per(plain)
    );
    assert_eq!(
        ids,
        2 * n,
        "an autocommit statement takes its attempt's id and O's"
    );
}
