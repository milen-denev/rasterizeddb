//! Errors end blocks, and conflict retries are waits in the graph (slice
//! B6, docs/persistent-indexes.md §7 "As built (B6)"): an error in a block
//! with no savepoint aborts it at once (R3), and a cycle through an
//! autocommit conflict retry and a block's DDL fails the DDL alone, after
//! which the writer goes on (V10 case 9; finding L1).
//!
//! Driven through `execute_recorded` as `table_lock_tests` is.

use std::time::Duration;

use super::owner_tests::{Session, ok, send, session};
use super::*;
use crate::core::mvcc::TxOutcome;
use tokio::io::AsyncReadExt;

const BOUND: Duration = Duration::from_secs(20);

async fn db() -> Arc<Database> {
    crate::replication::test_support::in_memory_database().await
}

async fn seeded(db: &Arc<Database>) -> Session {
    let mut s = session(db);
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)",
        "INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b')",
    ] {
        ok(db, &mut s, sql).await;
    }
    s
}

fn sqlstate_of(error: &str) -> &str {
    error.split('\0').find_map(|field| field.strip_prefix('C')).unwrap_or("")
}

/// Runs `sql` and returns its CommandComplete tag, or the error.
async fn tag(db: &Arc<Database>, s: &mut Session, sql: &str) -> Result<String, String> {
    Box::pin(execute_recorded(db, &mut s.state, &mut s.server, sql, &[], false, None))
        .await
        .unwrap();
    loop {
        let kind = s.client.read_u8().await.unwrap();
        let len = s.client.read_i32().await.unwrap() as usize;
        let mut body = vec![0u8; len - 4];
        s.client.read_exact(&mut body).await.unwrap();
        match kind {
            b'C' => return Ok(String::from_utf8_lossy(&body).trim_end_matches('\0').to_string()),
            b'E' => return Err(String::from_utf8_lossy(&body).to_string()),
            _ => {}
        }
    }
}

fn root_of(s: &Session) -> TxId {
    s.state.transaction.as_ref().expect("a block is open").id
}

/// R3: with no savepoint open, an error ends the block before it is
/// reported. The block's transaction reads aborted at once (its own view,
/// before any ROLLBACK), and its ROW EXCLUSIVE on `t` is gone (another
/// session's `LOCK TABLE … NOWAIT` is granted); later statements, a
/// SAVEPOINT among them, get 25P02; COMMIT reports ROLLBACK; nothing of the
/// block survives; ROLLBACK after that is a no-op.
///
/// Mutation: `fail_statement` only marks the block failed (no
/// `abort_now`): the transaction still reads in progress and the NOWAIT is
/// refused by the block's ROW EXCLUSIVE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_error_ends_the_block_at_once() {
    let db = db().await;
    let mut reader = seeded(&db).await;
    let mut s = session(&db);
    ok(&db, &mut s, "BEGIN").await;
    ok(&db, &mut s, "INSERT INTO t (id, v) VALUES (10, 'mine')").await;
    let root = root_of(&s);
    let e = send(&db, &mut s, "INSERT INTO t (id, v) VALUES (1, 'dup')").await.expect_err("a duplicate key");
    assert_eq!(sqlstate_of(&e), "23505", "{e}");

    assert_eq!(db.transactions.outcome(root), TxOutcome::Aborted, "the block was not ended at its error");
    let mut probe = session(&db);
    ok(&db, &mut probe, "BEGIN").await;
    let nowait = send(&db, &mut probe, "LOCK TABLE t IN ACCESS EXCLUSIVE MODE NOWAIT").await;
    assert!(nowait.is_ok(), "the failed block still holds its lock: {nowait:?}");
    ok(&db, &mut probe, "COMMIT").await;

    for sql in ["SELECT id FROM t", "SAVEPOINT a", "INSERT INTO t (id, v) VALUES (11, 'x')"] {
        let e = send(&db, &mut s, sql).await.expect_err(sql);
        assert_eq!(sqlstate_of(&e), "25P02", "{sql}: {e}");
    }
    assert_eq!(tag(&db, &mut s, "COMMIT").await.unwrap(), "ROLLBACK");
    assert_eq!(ok(&db, &mut reader, "SELECT id FROM t WHERE id = 10").await, 0, "the failed block's row committed");
    assert_eq!(ok(&db, &mut reader, "SELECT id FROM t WHERE id = 11").await, 0);

    // The same through ROLLBACK, which finds nothing left to abort.
    ok(&db, &mut s, "BEGIN").await;
    let root = root_of(&s);
    let _ = send(&db, &mut s, "SELECT id / 0 FROM t WHERE id = 1").await.expect_err("division by zero");
    assert_eq!(db.transactions.outcome(root), TxOutcome::Aborted);
    assert_eq!(tag(&db, &mut s, "ROLLBACK").await.unwrap(), "ROLLBACK");
    assert!(s.state.transaction.is_none());
    ok(&db, &mut s, "SELECT id FROM t WHERE id = 1").await;
}

/// Under an open savepoint the error only fails the block, as before:
/// `ROLLBACK TO` revives it, its earlier work commits.
///
/// Mutation: `fail_statement` aborts whatever the savepoints: the block's
/// transaction reads aborted and its first row is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn under_a_savepoint_an_error_only_fails_the_block() {
    let db = db().await;
    let mut reader = seeded(&db).await;
    let mut s = session(&db);
    ok(&db, &mut s, "BEGIN").await;
    ok(&db, &mut s, "INSERT INTO t (id, v) VALUES (20, 'kept')").await;
    let root = root_of(&s);
    ok(&db, &mut s, "SAVEPOINT a").await;
    let _ = send(&db, &mut s, "INSERT INTO t (id, v) VALUES (1, 'dup')").await.expect_err("a duplicate key");
    assert_eq!(db.transactions.outcome(root), TxOutcome::InProgress);
    ok(&db, &mut s, "ROLLBACK TO a").await;
    ok(&db, &mut s, "INSERT INTO t (id, v) VALUES (21, 'after')").await;
    assert_eq!(tag(&db, &mut s, "COMMIT").await.unwrap(), "COMMIT");
    assert_eq!(ok(&db, &mut reader, "SELECT id FROM t WHERE id = 20 OR id = 21").await, 2);
}

/// V10 case 9 (finding L1): `BEGIN; UPDATE t … WHERE id = 1` in one
/// session, an autocommit UPDATE of row 1 in another (it conflicts and waits
/// on the block, holding ROW EXCLUSIVE on `t`), then the block's CREATE
/// INDEX, whose ACCESS EXCLUSIVE waits on that ROW EXCLUSIVE. One 40P01, to
/// the DDL; the block is then aborted at once (R3), so the writer's blocker
/// ends and its UPDATE lands: no 40001.
///
/// The DDL is sent 600 ms into the writer's wait, so its own
/// `deadlock_timeout` would come round only after the writer's retry budget
/// (1.5 s from its conflict) had run out: the cycle is broken in time only
/// because the writer's check fails the DDL's wait itself.
///
/// Mutations, each failing it: the conflict wait enters no edge (no cycle:
/// the writer gives up with 40001 and the DDL then succeeds);
/// `conflict_deadlock_victim` leaves the table victim to its own check (the
/// writer's budget ends first: 40001); R3 off (the DDL fails but the block
/// keeps the row until ROLLBACK: the writer's budget ends, 40001).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cycle_through_a_conflict_retry_and_a_ddl_fails_only_the_ddl() {
    use crate::core::rql::conflict_wait::Site;
    let db = db().await;
    let mut reader = seeded(&db).await;
    let mut block = session(&db);
    ok(&db, &mut block, "BEGIN").await;
    ok(&db, &mut block, "UPDATE t SET v = 'block' WHERE id = 1").await;

    let before = db.row_locks.conflict_waits.at(Site::Autocommit as usize).0;
    let writer = {
        let db = db.clone();
        tokio::spawn(async move {
            let mut w = session(&db);
            send(&db, &mut w, "UPDATE t SET v = 'writer' WHERE id = 1").await
        })
    };
    tokio::time::timeout(BOUND, async {
        while db.row_locks.conflict_waits.at(Site::Autocommit as usize).0 <= before {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the writer never waited on the block");
    tokio::time::sleep(Duration::from_millis(600)).await;

    let ddl = {
        let db = db.clone();
        tokio::spawn(async move {
            let out = send(&db, &mut block, "CREATE INDEX t_v ON t (v)").await;
            (block, out)
        })
    };
    let (mut block, ddl_out) = tokio::time::timeout(BOUND, ddl).await.expect("the DDL never finished").unwrap();
    let writer_out = tokio::time::timeout(BOUND, writer).await.expect("the writer never finished").unwrap();
    let e = ddl_out.expect_err("the DDL was not the victim");
    assert_eq!(sqlstate_of(&e), "40P01", "{e}");
    writer_out.unwrap_or_else(|e| panic!("the writer failed ({}): {e}", sqlstate_of(&e)));
    let e = send(&db, &mut block, "SELECT id FROM t").await.expect_err("the victim's block went on");
    assert_eq!(sqlstate_of(&e), "25P02", "{e}");
    assert_eq!(tag(&db, &mut block, "ROLLBACK").await.unwrap(), "ROLLBACK");
    assert_eq!(ok(&db, &mut reader, "SELECT id FROM t WHERE v = 'writer'").await, 1);
}
