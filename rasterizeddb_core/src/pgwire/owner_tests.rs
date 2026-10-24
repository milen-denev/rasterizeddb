//! Lock owners on the wire (slice B2, docs/persistent-indexes.md §7 Owner):
//! one owner for every piece of a statement the session runs (CTE, derived
//! and catalog bodies and their drops, subqueries, the engine's attempts),
//! the `WITH` transaction as that owner across the in-transaction retry
//! loop, and a block's owner released at every way a block ends.
//!
//! Driven through `execute_recorded`, the one entry every statement takes.

use super::*;
use crate::core::mvcc::TxOutcome;
use crate::core::mvcc::owner::probe::{self, Record, Site};
use crate::core::mvcc::owner::{LockOwner, OwnerKind};
use tokio::io::AsyncReadExt;

struct Session {
    state: SessionState,
    server: tokio::io::DuplexStream,
    client: tokio::io::DuplexStream,
}

fn session(db: &Arc<Database>) -> Session {
    let catalog = DatabaseCatalog::single(db.clone());
    let auth = AuthConfig {
        method: crate::auth::AuthMethod::Trust,
        ..AuthConfig::default()
    };
    let principal = Principal {
        name: "rasterized".to_string(),
        superuser: true,
        authenticated: false,
    };
    let state = SessionState::new(
        catalog,
        db.name.clone(),
        auth,
        principal,
        ConnectionLimits::default(),
    );
    let (server, client) = tokio::io::duplex(1 << 20);
    Session {
        state,
        server,
        client,
    }
}

/// Runs one statement and reads its reply up to `CommandComplete` or an
/// error: `Ok(rows)` or `Err(message)`.
async fn send(db: &Arc<Database>, s: &mut Session, sql: &str) -> Result<usize, String> {
    Box::pin(execute_recorded(
        db,
        &mut s.state,
        &mut s.server,
        sql,
        &[],
        false,
        None,
    ))
    .await
    .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut rows = 0;
    loop {
        let kind = s.client.read_u8().await.unwrap();
        let len = s.client.read_i32().await.unwrap() as usize;
        let mut body = vec![0u8; len - 4];
        s.client.read_exact(&mut body).await.unwrap();
        match kind {
            b'D' => rows += 1,
            b'C' => return Ok(rows),
            b'E' => return Err(String::from_utf8_lossy(&body).to_string()),
            _ => {}
        }
    }
}

async fn ok(db: &Arc<Database>, s: &mut Session, sql: &str) -> usize {
    send(db, s, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

async fn captured(db: &Arc<Database>, s: &mut Session, sql: &str) -> (usize, Vec<Record>) {
    let (out, records) = probe::capture(send(db, s, sql)).await;
    (out.unwrap_or_else(|e| panic!("{sql}: {e}")), records)
}

/// Every record ran under one owner of `kind` that read in progress, and
/// every transaction a piece ran under acts for it.
fn one_owner(what: &str, records: &[Record], kind: OwnerKind) -> LockOwner {
    assert!(
        !records.is_empty(),
        "{what}: no piece of the statement was recorded"
    );
    let first = records[0]
        .owner
        .unwrap_or_else(|| panic!("{what}: {:?} ran under no owner", records[0].site));
    assert_eq!(first.kind, kind, "{what}");
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
        if r.tx != crate::core::mvcc::transaction::FROZEN_TX {
            assert_eq!(
                r.owner_of_tx,
                first.owner.id(),
                "{what}: {:?} ran under tx {}, which acts for {}, a second owner beside {}",
                r.site,
                r.tx,
                r.owner_of_tx,
                first.owner.id()
            );
        }
    }
    first.owner
}

fn assert_ended(db: &Arc<Database>, what: &str, o: LockOwner) {
    assert_ne!(
        db.transactions.outcome(o.id()),
        TxOutcome::InProgress,
        "{what}: the owner was never ended"
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

async fn db() -> Arc<Database> {
    crate::replication::test_support::in_memory_database().await
}

async fn tables(db: &Arc<Database>, s: &mut Session) {
    for sql in [
        "CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)",
        "CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT, v TEXT)",
        "INSERT INTO t1 (id, name) VALUES (1, 't2')",
        "INSERT INTO t1 (id, name) VALUES (2, 'b')",
        "INSERT INTO t2 (id, t1_id, v) VALUES (10, 1, 'x')",
        "INSERT INTO t2 (id, t1_id, v) VALUES (11, 2, 'y')",
    ] {
        ok(db, s, sql).await;
    }
}

/// One owner per statement (V10 case 15, early form using `LOCK TABLE`): a
/// derived-table join, a `WITH … SELECT` and a `pg_class` join, run while a
/// block holds SHARE on the inner table, each finish (no self-deadlock),
/// and every piece of each (bodies, catalog relations, their drops, the
/// engine's attempt) runs under the statement's one owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_piece_of_a_wire_statement_runs_under_one_owner() {
    let db = db().await;
    let mut a = session(&db);
    tables(&db, &mut a).await;
    let mut blocker = session(&db);
    ok(&db, &mut blocker, "BEGIN").await;
    ok(&db, &mut blocker, "LOCK TABLE t2 IN SHARE MODE").await;

    let cases: &[(&str, &[Site])] = &[
        (
            "SELECT t1.name, d.v FROM t1 JOIN (SELECT t1_id, v FROM t2) d ON d.t1_id = t1.id",
            &[Site::Derived, Site::DropTemps, Site::Attempt],
        ),
        (
            "WITH c AS (SELECT id, v FROM t2) SELECT id, v FROM c",
            &[Site::Cte, Site::DropTemps],
        ),
        (
            "SELECT c.relname FROM pg_class c JOIN t1 ON c.relname = t1.name",
            &[Site::Catalog, Site::DropTemps],
        ),
        (
            "SELECT id FROM t1 WHERE id IN (SELECT t1_id FROM t2)",
            &[Site::Subquery, Site::Attempt],
        ),
    ];
    for (sql, wanted) in cases {
        let (_, records) = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            captured(&db, &mut a, sql),
        )
        .await
        .unwrap_or_else(|_| panic!("{sql}: did not finish beside the block's SHARE lock"));
        let o = one_owner(sql, &records, OwnerKind::Statement);
        let seen: std::collections::HashSet<Site> = records.iter().map(|r| r.site).collect();
        for site in *wanted {
            assert!(
                seen.contains(site),
                "{sql}: {site:?} not recorded (saw {seen:?})"
            );
        }
        assert_ended(&db, sql, o);
    }
    ok(&db, &mut blocker, "COMMIT").await;
}

/// The `WITH` transaction is O, never a second owner (Q1), and the
/// in-transaction retry loop keeps every attempt of a data-modifying CTE
/// body under it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_with_transaction_is_o_through_in_transaction_retries() {
    let db = db().await;
    let mut a = session(&db);
    tables(&db, &mut a).await;
    let mut holder = session(&db);
    ok(&db, &mut holder, "BEGIN").await;
    ok(&db, &mut holder, "UPDATE t2 SET v = 'held' WHERE id = 10").await;

    let sql = "WITH w AS (UPDATE t2 SET v = 'won' WHERE id = 10 RETURNING id) SELECT id FROM w";
    let db2 = db.clone();
    let racer = tokio::spawn(async move {
        let mut a = a;
        let got = captured(&db2, &mut a, sql).await;
        (got, a)
    });
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    ok(&db, &mut holder, "COMMIT").await;
    let ((rows, records), _a) = tokio::time::timeout(std::time::Duration::from_secs(20), racer)
        .await
        .expect("the WITH statement finished")
        .unwrap();
    assert_eq!(rows, 1);
    let o = one_owner(sql, &records, OwnerKind::Statement);
    let in_txn: Vec<&Record> = records
        .iter()
        .filter(|r| r.site == Site::InTxnAttempt)
        .collect();
    assert!(
        in_txn.len() >= 2,
        "the CTE body never retried, so the loop was not tested"
    );
    assert!(
        in_txn.iter().all(|r| r.tx == o.id()),
        "a CTE body ran under a transaction other than O"
    );
    assert!(records.iter().any(|r| r.site == Site::ReturningBody));
    assert_ended(&db, sql, o);
}

/// A block's owner is released at every way the block ends: COMMIT of a
/// block that read or wrote, COMMIT of a failed block, ROLLBACK, and the
/// session dropped with the block open. A block statement runs under it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blocks_owner_is_released_at_every_block_end() {
    let db = db().await;
    let mut a = session(&db);
    tables(&db, &mut a).await;

    let ends: &[(&str, &[&str], &str)] = &[
        ("read-only COMMIT", &[], "COMMIT"),
        (
            "written COMMIT",
            &["INSERT INTO t1 (id, name) VALUES (50, 'w')"],
            "COMMIT",
        ),
        (
            "ROLLBACK",
            &["INSERT INTO t1 (id, name) VALUES (51, 'r')"],
            "ROLLBACK",
        ),
    ];
    for (what, body, end) in ends {
        ok(&db, &mut a, "BEGIN").await;
        let root = a.state.transaction.as_ref().unwrap().id;
        ok(&db, &mut a, "LOCK TABLE t2 IN ACCESS EXCLUSIVE MODE").await;
        let (_, records) = captured(&db, &mut a, "SELECT id FROM t1 WHERE id = 1").await;
        let o = one_owner(what, &records, OwnerKind::Block);
        assert_eq!(
            o,
            LockOwner::block(root).unwrap(),
            "{what}: a block statement ran under another owner"
        );
        for sql in *body {
            ok(&db, &mut a, sql).await;
        }
        assert!(
            db.row_locks.owner_holds_tables(root),
            "{what}: the lock went before the block ended"
        );
        ok(&db, &mut a, end).await;
        assert!(
            !db.row_locks.owner_holds_tables(root),
            "{what}: the block's owner was not released"
        );
    }

    // A failed block: COMMIT reports ROLLBACK and still releases.
    ok(&db, &mut a, "BEGIN").await;
    let root = a.state.transaction.as_ref().unwrap().id;
    ok(&db, &mut a, "LOCK TABLE t2 IN ACCESS EXCLUSIVE MODE").await;
    assert!(
        send(&db, &mut a, "SELECT nope FROM missing_table")
            .await
            .is_err()
    );
    ok(&db, &mut a, "COMMIT").await;
    assert!(
        !db.row_locks.owner_holds_tables(root),
        "a failed block's owner was not released"
    );

    // The session goes with the block open.
    let mut gone = session(&db);
    ok(&db, &mut gone, "BEGIN").await;
    let root = gone.state.transaction.as_ref().unwrap().id;
    ok(&db, &mut gone, "LOCK TABLE t2 IN ACCESS EXCLUSIVE MODE").await;
    drop(gone);
    assert!(
        !db.row_locks.owner_holds_tables(root),
        "a dropped session's block kept its owner's locks"
    );
}
