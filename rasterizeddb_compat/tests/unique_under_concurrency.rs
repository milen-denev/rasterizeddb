//! `UNIQUE` has to hold when several connections insert the same keys at once.
//!
//! It did not. `check_unique` read through the inserting transaction's
//! snapshot, so a row another transaction had written but not yet committed was
//! invisible to it: two concurrent inserts of one key each looked, each found
//! nothing, and each wrote. Measured on four connections inserting the same 500
//! keys, the table ended with **1,994 rows holding 500 distinct keys** — 499 of
//! them duplicated, some four times. No error was raised.
//!
//! The write lock around the check did not help, and neither did widening the
//! check to every *committed* row: the window is `write → unlock → commit`, and
//! a row that has been written but not committed is in none of those sets.
//!
//! Two things made it hard to see. Without `ON CONFLICT` the same concurrency
//! stays clean, because a loser hits the violation on its *first* row and
//! aborts, never getting a second chance — so the clause looks guilty when it
//! only multiplies the exposure by the batch size. And the corruption is
//! silent: the rows are committed and readable, and only counting distinct keys
//! reveals it.
//!
//! The fix lets the check see a value another transaction is still writing and
//! replay the statement rather than guess, which is sound because the check runs
//! before the write and so rejects a statement that has applied nothing.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

fn many_values(prefix: &str, n: usize) -> String {
    let mut sql = String::with_capacity(n * 24 + 64);
    sql.push_str("INSERT INTO t (k, v) VALUES ");
    for i in 1..=n {
        if i > 1 {
            sql.push(',');
        }
        sql.push_str(&format!("('{prefix}{i}','v')"));
    }
    sql
}

/// Every connection inserts the same keys. Whatever order they interleave in,
/// the table may hold each key exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_inserts_of_one_key_set_cannot_duplicate_it() {
    const CLIENTS: usize = 4;
    const ROWS: usize = 300;

    let server = TestServer::start().await.expect("start");
    let setup = server.connect_sqlx().await.expect("connect");
    sqlx::query(AssertSqlSafe("CREATE TABLE t (k VARCHAR NOT NULL UNIQUE, v VARCHAR)"))
        .execute(&setup)
        .await
        .expect("create");

    // `ON CONFLICT DO NOTHING` deliberately: without it a loser aborts on its
    // first row, which hides the race by cutting the exposure to one attempt.
    let statement = format!("{} ON CONFLICT (k) DO NOTHING", many_values("k", ROWS));

    let mut clients = Vec::with_capacity(CLIENTS);
    for _ in 0..CLIENTS {
        let pool = server.connect_sqlx().await.expect("connect");
        let sql = statement.clone();
        clients.push(tokio::spawn(async move {
            sqlx::query(AssertSqlSafe(sql)).execute(&pool).await.map(|_| ()).map_err(|e| e.to_string())
        }));
    }
    for client in clients {
        // Every one must *finish*. A retryable conflict that escaped to the
        // client would be a different bug wearing this one's clothes.
        client
            .await
            .expect("client task")
            .unwrap_or_else(|e| panic!("a concurrent insert failed outright: {e}"));
    }

    // Counted by reading the keys rather than by `COUNT(DISTINCT …)`, so the
    // assertion does not depend on the same engine paths it is testing.
    let keys: Vec<String> = sqlx::query(AssertSqlSafe("SELECT k FROM t"))
        .fetch_all(&setup)
        .await
        .expect("read back")
        .into_iter()
        .map(|row| row.try_get::<String, _>(0).expect("k"))
        .collect();

    let mut distinct = keys.clone();
    distinct.sort();
    distinct.dedup();

    assert_eq!(
        keys.len(),
        distinct.len(),
        "UNIQUE did not hold: {} rows for {} distinct keys",
        keys.len(),
        distinct.len()
    );
    assert_eq!(distinct.len(), ROWS, "some keys were lost entirely");
}

/// The same concurrency without the clause. This passed before the fix too —
/// it is here so that a regression which "fixes" the race by making every
/// concurrent insert fail cannot pass by only satisfying the test above.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_plain_inserts_still_land_exactly_once() {
    const CLIENTS: usize = 3;
    const ROWS: usize = 200;

    let server = TestServer::start().await.expect("start");
    let setup = server.connect_sqlx().await.expect("connect");
    sqlx::query(AssertSqlSafe("CREATE TABLE t (k VARCHAR NOT NULL UNIQUE, v VARCHAR)"))
        .execute(&setup)
        .await
        .expect("create");

    let statement = many_values("p", ROWS);
    let mut clients = Vec::with_capacity(CLIENTS);
    for _ in 0..CLIENTS {
        let pool = server.connect_sqlx().await.expect("connect");
        let sql = statement.clone();
        clients.push(tokio::spawn(async move {
            // Losers are *expected* to fail here; only the row set matters.
            let _ = sqlx::query(AssertSqlSafe(sql)).execute(&pool).await;
        }));
    }
    for client in clients {
        client.await.expect("client task");
    }

    let keys: Vec<String> = sqlx::query(AssertSqlSafe("SELECT k FROM t"))
        .fetch_all(&setup)
        .await
        .expect("read back")
        .into_iter()
        .map(|row| row.try_get::<String, _>(0).expect("k"))
        .collect();
    let mut distinct = keys.clone();
    distinct.sort();
    distinct.dedup();

    assert_eq!(keys.len(), distinct.len(), "UNIQUE did not hold without ON CONFLICT either");
}

/// The cases the fix must not have broken, all sequential.
///
/// Re-inserting a deleted key is the one that matters most: the obvious way to
/// let the check see uncommitted rows is `Visibility::All`, and that is wrong
/// precisely here — `deleted` is set by vacuum rather than by the delete, so
/// `All` still sees a committed-deleted row and the key could never be reused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_ordinary_uniqueness_answers_are_unchanged() {
    let server = TestServer::start().await.expect("start");
    let pool = server.connect_sqlx().await.expect("connect");
    for sql in [
        "CREATE TABLE t (k VARCHAR NOT NULL UNIQUE, v VARCHAR)",
        "INSERT INTO t (k,v) VALUES ('a','1')",
    ] {
        sqlx::query(AssertSqlSafe(sql)).execute(&pool).await.expect(sql);
    }

    // A committed duplicate is still an error.
    let err = sqlx::query(AssertSqlSafe("INSERT INTO t (k,v) VALUES ('a','2')"))
        .execute(&pool)
        .await
        .expect_err("a committed duplicate was accepted");
    assert!(err.to_string().contains("duplicate"), "unexpected error: {err}");

    // And still absorbed by the clause.
    sqlx::query(AssertSqlSafe("INSERT INTO t (k,v) VALUES ('a','2') ON CONFLICT (k) DO NOTHING"))
        .execute(&pool)
        .await
        .expect("ON CONFLICT DO NOTHING should absorb a committed duplicate");

    // Deleting a key must free it. This is the `Visibility::All` trap.
    sqlx::query(AssertSqlSafe("DELETE FROM t WHERE k = 'a'"))
        .execute(&pool)
        .await
        .expect("delete");
    sqlx::query(AssertSqlSafe("INSERT INTO t (k,v) VALUES ('a','3')"))
        .execute(&pool)
        .await
        .expect("a deleted key must be reusable");

    // A duplicate inside one statement is this transaction's own doing, so it
    // is a hard error rather than something to wait out — replaying it would
    // find the same row again, forever.
    let err = sqlx::query(AssertSqlSafe("INSERT INTO t (k,v) VALUES ('b','1'),('b','2')"))
        .execute(&pool)
        .await
        .expect_err("a duplicate within one statement was accepted");
    assert!(err.to_string().contains("duplicate"), "unexpected error: {err}");
}

/// The same race, but inside an explicit `BEGIN … COMMIT`.
///
/// This is a *second* bug wearing the first one's clothes, and it survived the
/// fix above because that test uses autocommit. There are two retry loops:
/// `execute_with_sync` for autocommit and `execute_in_txn` for a client's own
/// block, and only the first one had been taught that an insert is replayable.
/// A statement inside a transaction therefore got no retry at all and the
/// conflict went straight to the client — which is how it reached PMS and
/// aborted a tier migration that had already copied 165,000 rows.
///
/// Three details all had to be present to see it, and each one alone hides it:
/// an explicit transaction, more than one tuple, and `ON CONFLICT`. Reproducing
/// it with bind parameters and a multi-row insert but no `BEGIN` gives four
/// clean clients, which is what made the first two attempts report success.
///
/// The retry that fixes it lives at the *tuple*, not at the statement: by the
/// time tuple 200 conflicts, tuples 1..=199 are written, so replaying the
/// statement would meet its own rows rather than "having applied nothing".
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_inserts_inside_a_transaction_are_replayed_not_surfaced() {
    const CLIENTS: usize = 4;
    const ROWS: usize = 300;

    let server = TestServer::start().await.expect("start");
    let setup = server.connect_sqlx().await.expect("connect");
    sqlx::query(AssertSqlSafe("CREATE TABLE t (k VARCHAR NOT NULL UNIQUE, v VARCHAR)"))
        .execute(&setup)
        .await
        .expect("create");

    let statement = format!("{} ON CONFLICT (k) DO NOTHING", many_values("k", ROWS));

    let mut clients = Vec::with_capacity(CLIENTS);
    for _ in 0..CLIENTS {
        let pool = server.connect_sqlx().await.expect("connect");
        let sql = statement.clone();
        clients.push(tokio::spawn(async move {
            let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
            sqlx::query(AssertSqlSafe(sql))
                .execute(&mut *tx)
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())
        }));
    }
    for client in clients {
        client
            .await
            .expect("client task")
            .unwrap_or_else(|e| panic!("an insert inside a transaction failed outright: {e}"));
    }

    let keys: Vec<String> = sqlx::query(AssertSqlSafe("SELECT k FROM t"))
        .fetch_all(&setup)
        .await
        .expect("read back")
        .into_iter()
        .map(|row| row.try_get::<String, _>(0).expect("k"))
        .collect();
    let mut distinct = keys.clone();
    distinct.sort();
    distinct.dedup();

    assert_eq!(
        keys.len(),
        distinct.len(),
        "UNIQUE did not hold inside a transaction: {} rows for {} distinct keys",
        keys.len(),
        distinct.len()
    );
    assert_eq!(distinct.len(), ROWS, "some keys were lost entirely");
}

/// Waiting must not turn a real violation into a silent success.
///
/// Without `ON CONFLICT` the losers are *supposed* to fail. The risk in a retry
/// that waits for the winner is that it converts "another transaction is
/// writing this" into a swallowed conflict; what it must do instead is wait,
/// see the now-committed row, and report the ordinary duplicate. So this
/// asserts on the error text as well as the row set: the message a client gets
/// has to be the permanent violation, not the transient one it could retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_transaction_that_genuinely_violates_unique_still_fails() {
    const CLIENTS: usize = 3;
    const ROWS: usize = 200;

    let server = TestServer::start().await.expect("start");
    let setup = server.connect_sqlx().await.expect("connect");
    sqlx::query(AssertSqlSafe("CREATE TABLE t (k VARCHAR NOT NULL UNIQUE, v VARCHAR)"))
        .execute(&setup)
        .await
        .expect("create");

    let statement = many_values("p", ROWS);
    let mut clients = Vec::with_capacity(CLIENTS);
    for _ in 0..CLIENTS {
        let pool = server.connect_sqlx().await.expect("connect");
        let sql = statement.clone();
        clients.push(tokio::spawn(async move {
            let mut tx = pool.begin().await.expect("begin");
            let outcome = sqlx::query(AssertSqlSafe(sql))
                .execute(&mut *tx)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string());
            if outcome.is_ok() {
                let _ = tx.commit().await;
            }
            outcome
        }));
    }

    let mut failures = Vec::new();
    for client in clients {
        if let Err(text) = client.await.expect("client task") {
            failures.push(text);
        }
    }
    assert!(
        !failures.is_empty(),
        "every concurrent transaction succeeded, so UNIQUE was not enforced"
    );
    for text in &failures {
        assert!(
            text.contains("duplicate"),
            "a loser was told the transient conflict rather than the violation: {text}"
        );
        assert!(
            !text.contains("being written by another transaction"),
            "the retryable conflict escaped to a client instead of being waited out: {text}"
        );
    }

    let keys: Vec<String> = sqlx::query(AssertSqlSafe("SELECT k FROM t"))
        .fetch_all(&setup)
        .await
        .expect("read back")
        .into_iter()
        .map(|row| row.try_get::<String, _>(0).expect("k"))
        .collect();
    let mut distinct = keys.clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(keys.len(), distinct.len(), "UNIQUE did not hold");
}

/// A multi-row `UPDATE` that hits a conflict applies **nothing**.
///
/// This is the invariant that makes replaying an `UPDATE` safe, and it is not
/// obvious from the code: `update_rows_with_expressions` writes each matched
/// row as it goes and commits the batch on the way out, so reading it suggests
/// that a conflict on the fifth row leaves four rows updated. If that were
/// true, a replay would re-apply them and `n = n + 1` would count twice.
///
/// It was measured rather than assumed, and it holds in every arrangement
/// tried: the conflicting row first or last, and the caller in autocommit or
/// in its own `BEGIN … COMMIT` that afterwards commits cleanly. In all four,
/// every row keeps its original value.
///
/// The assignment is deliberately non-idempotent (`n + 1`, `k || '!'`) because
/// an idempotent one could not tell a single application from a double one.
///
/// So this test guards a property other code depends on, rather than
/// reproducing a known bug — if a change ever starts leaving partial work
/// behind, the replay paths in `executor.rs` become unsafe and this is what
/// should fail first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflicting_update_applies_nothing() {
    for conflicting_key in ["a!", "c!"] {
        let server = TestServer::start().await.expect("start");
        let pool = server.connect_sqlx().await.expect("connect");
        for sql in [
            "CREATE TABLE u (id INT, k VARCHAR NOT NULL UNIQUE, n INT)",
            "INSERT INTO u (id,k,n) VALUES (1,'a',0),(2,'b',0),(3,'c',0)",
        ] {
            sqlx::query(AssertSqlSafe(sql)).execute(&pool).await.expect(sql);
        }

        // Held uncommitted, so whether row 1 (or row 3) duplicates anything
        // depends on a transaction that has not ended.
        let holder = server.connect_sqlx().await.expect("connect");
        let mut held = holder.begin().await.expect("begin");
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO u (id,k,n) VALUES (9,'{conflicting_key}',0)"
        )))
        .execute(&mut *held)
        .await
        .expect("in-flight insert");

        let writer = server.connect_sqlx().await.expect("connect");
        let mut wtx = writer.begin().await.expect("begin writer");
        let outcome = sqlx::query(AssertSqlSafe(
            "UPDATE u SET k = k || '!', n = n + 1 WHERE id IN (1,2,3)",
        ))
        .execute(&mut *wtx)
        .await;
        assert!(
            outcome.is_err(),
            "the UPDATE reported success while {conflicting_key} was still in flight"
        );
        // Commits cleanly: the point is that there is nothing to commit, not
        // that the transaction was poisoned.
        wtx.commit().await.expect("commit after a failed statement");
        held.rollback().await.expect("rollback");

        for (id, original) in [(1, "a"), (2, "b"), (3, "c")] {
            let row = sqlx::query(AssertSqlSafe(format!("SELECT k, n FROM u WHERE id = {id}")))
                .fetch_one(&pool)
                .await
                .expect("read back");
            assert_eq!(
                row.try_get::<String, _>(0).expect("k"),
                original,
                "row {id} was modified by a statement that failed \
                 (conflict on {conflicting_key})"
            );
            assert_eq!(
                row.try_get::<i32, _>(1).expect("n"),
                0,
                "row {id} was counted by a statement that failed \
                 (conflict on {conflicting_key})"
            );
        }
    }
}
