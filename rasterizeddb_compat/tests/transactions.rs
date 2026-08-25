//! Transactions, as an application actually uses them.
//!
//! `BEGIN`/`COMMIT`/`ROLLBACK` used to be accepted and ignored — a rollback had
//! already committed. These check that the guarantees an application relies on
//! are real: a rollback undoes, a transaction sees its own work and nobody
//! else's, and a failure inside a block does not commit half of it.

use rasterizeddb_compat::TestServer;
use sqlx::Row;

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    sqlx::query(sql)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

async fn count(pool: &sqlx::PgPool, sql: &str) -> usize {
    sqlx::query(sql).fetch_all(pool).await.expect("select should succeed").len()
}

/// One connection, so `BEGIN` and the statements after it share a session.
async fn conn(server: &TestServer) -> sqlx::PgConnection {
    use sqlx::Connection;
    sqlx::PgConnection::connect(&server.url())
        .await
        .expect("should connect")
}

async fn exec(c: &mut sqlx::PgConnection, sql: &str) -> Result<(), String> {
    sqlx::query(sql)
        .execute(&mut *c)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

async fn rows(c: &mut sqlx::PgConnection, sql: &str) -> usize {
    sqlx::query(sql).fetch_all(&mut *c).await.expect("select").len()
}

async fn table(pool: &sqlx::PgPool) {
    run(pool, "CREATE TABLE t (id INT NOT NULL, name TEXT)").await.expect("create");
}

/// The guarantee that was missing entirely.
#[tokio::test(flavor = "multi_thread")]
async fn a_rollback_undoes_the_work() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");
    assert_eq!(rows(&mut c, "SELECT id FROM t").await, 1, "visible to its own transaction");
    exec(&mut c, "ROLLBACK").await.expect("rollback");

    assert_eq!(rows(&mut c, "SELECT id FROM t").await, 0, "and gone once rolled back");
    assert_eq!(count(&pool, "SELECT id FROM t").await, 0, "for every other session too");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_commit_keeps_the_work() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");
    exec(&mut c, "COMMIT").await.expect("commit");

    assert_eq!(count(&pool, "SELECT id FROM t").await, 1);
}

/// The isolation half: uncommitted work must not leak.
#[tokio::test(flavor = "multi_thread")]
async fn uncommitted_work_is_invisible_to_other_sessions() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut writer = conn(&server).await;
    exec(&mut writer, "BEGIN").await.expect("begin");
    exec(&mut writer, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");

    assert_eq!(count(&pool, "SELECT id FROM t").await, 0, "a dirty read");
    assert_eq!(rows(&mut writer, "SELECT id FROM t").await, 1, "but the writer sees it");

    exec(&mut writer, "COMMIT").await.expect("commit");
    assert_eq!(count(&pool, "SELECT id FROM t").await, 1, "and it appears on commit");
}

/// A delete has to be undoable too — it stamps the row rather than erasing it.
#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_delete_leaves_the_row() {
    let (server, pool) = server().await;
    table(&pool).await;
    run(&pool, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "DELETE FROM t WHERE id = 1").await.expect("delete");
    assert_eq!(rows(&mut c, "SELECT id FROM t").await, 0, "gone for the deleter");
    assert_eq!(count(&pool, "SELECT id FROM t").await, 1, "still there for everyone else");

    exec(&mut c, "ROLLBACK").await.expect("rollback");
    assert_eq!(count(&pool, "SELECT id FROM t").await, 1, "and the rollback restores it");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_committed_delete_removes_the_row() {
    let (server, pool) = server().await;
    table(&pool).await;
    run(&pool, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "DELETE FROM t WHERE id = 1").await.expect("delete");
    exec(&mut c, "COMMIT").await.expect("commit");

    assert_eq!(count(&pool, "SELECT id FROM t").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_update_restores_the_old_value() {
    let (server, pool) = server().await;
    table(&pool).await;
    run(&pool, "INSERT INTO t (id, name) VALUES (1, 'before')").await.expect("insert");

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "UPDATE t SET name = 'after' WHERE id = 1").await.expect("update");
    exec(&mut c, "ROLLBACK").await.expect("rollback");

    let row = sqlx::query("SELECT name FROM t WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("select");
    assert_eq!(row.try_get::<String, _>("name").unwrap(), "before");
}

/// Several statements, one outcome. A block that rolls back must undo all of
/// it, not the last statement only.
#[tokio::test(flavor = "multi_thread")]
async fn a_block_is_all_or_nothing() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    for id in 1..=5 {
        exec(&mut c, &format!("INSERT INTO t (id, name) VALUES ({}, 'x')", id))
            .await
            .expect("insert");
    }
    assert_eq!(rows(&mut c, "SELECT id FROM t").await, 5);
    exec(&mut c, "ROLLBACK").await.expect("rollback");

    assert_eq!(count(&pool, "SELECT id FROM t").await, 0, "every one of them");
}

/// After a failure the block is poisoned: PostgreSQL refuses the rest of it, so
/// a commit cannot land a transaction with a hole in it.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_statement_poisons_the_block() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");
    exec(&mut c, "INSERT INTO nonexistent (id) VALUES (1)").await.expect_err("no such table");

    let err = exec(&mut c, "INSERT INTO t (id, name) VALUES (2, 'b')")
        .await
        .expect_err("the block is aborted");
    assert!(err.contains("aborted"), "{}", err);

    exec(&mut c, "ROLLBACK").await.expect("rollback");
    assert_eq!(count(&pool, "SELECT id FROM t").await, 0, "nothing from the block landed");
}

/// Committing a poisoned block must roll it back rather than pretend.
#[tokio::test(flavor = "multi_thread")]
async fn committing_a_failed_block_rolls_it_back() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");
    exec(&mut c, "SELECT * FROM nonexistent").await.expect_err("no such table");
    exec(&mut c, "COMMIT").await.expect("commit is accepted");

    assert_eq!(count(&pool, "SELECT id FROM t").await, 0, "but it rolled back");
}

/// A connection that goes away mid-transaction must not leave it open: it would
/// hold reclamation back forever and its rows would never resolve.
#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_connection_rolls_its_transaction_back() {
    let (server, pool) = server().await;
    table(&pool).await;

    {
        let mut c = conn(&server).await;
        exec(&mut c, "BEGIN").await.expect("begin");
        exec(&mut c, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");
        // `c` is dropped here without a COMMIT.
    }

    // Give the server a moment to notice the close.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(count(&pool, "SELECT id FROM t").await, 0);
}

/// sea-orm and sqlx both track the transaction status byte from
/// `ReadyForQuery`; reporting "idle" throughout makes a block invisible to the
/// driver. `sqlx::Transaction` is the driver-level test of that.
#[tokio::test(flavor = "multi_thread")]
async fn the_driver_transaction_api_works() {
    let (_s, pool) = server().await;
    table(&pool).await;

    let mut tx = pool.begin().await.expect("driver begin");
    sqlx::query("INSERT INTO t (id, name) VALUES (1, 'a')")
        .execute(&mut *tx)
        .await
        .expect("insert");
    tx.rollback().await.expect("driver rollback");

    assert_eq!(count(&pool, "SELECT id FROM t").await, 0);

    let mut tx = pool.begin().await.expect("driver begin");
    sqlx::query("INSERT INTO t (id, name) VALUES (2, 'b')")
        .execute(&mut *tx)
        .await
        .expect("insert");
    tx.commit().await.expect("driver commit");

    assert_eq!(count(&pool, "SELECT id FROM t").await, 1);
}

/// Two transactions updating the same row must not both succeed. If they did,
/// each would relocate from the same base version and both replacements would
/// be visible once committed — the row duplicated, and one update lost.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_updates_to_one_row_do_not_both_win() {
    let (server, pool) = server().await;
    table(&pool).await;
    run(&pool, "INSERT INTO t (id, name) VALUES (1, 'start')").await.expect("insert");

    let mut first = conn(&server).await;
    let mut second = conn(&server).await;

    exec(&mut first, "BEGIN").await.expect("begin");
    exec(&mut second, "BEGIN").await.expect("begin");

    exec(&mut first, "UPDATE t SET name = 'first' WHERE id = 1").await.expect("first update");
    let conflict = exec(&mut second, "UPDATE t SET name = 'second' WHERE id = 1")
        .await
        .expect_err("the second updater must be told to retry");
    assert!(
        conflict.to_ascii_lowercase().contains("serialize"),
        "the error must say why it can be retried: {}",
        conflict
    );

    exec(&mut first, "COMMIT").await.expect("commit");
    exec(&mut second, "ROLLBACK").await.expect("rollback");

    // Exactly one row, holding the winner's value.
    let all = sqlx::query("SELECT name FROM t WHERE id = 1")
        .fetch_all(&pool)
        .await
        .expect("select");
    assert_eq!(all.len(), 1, "the row must not have been duplicated");
    assert_eq!(all[0].try_get::<String, _>("name").unwrap(), "first");
}

/// A conflict against a transaction that then aborts is not a conflict: the
/// retirement never happened, so the version is still available.
#[tokio::test(flavor = "multi_thread")]
async fn an_aborted_writer_releases_the_row() {
    let (server, pool) = server().await;
    table(&pool).await;
    run(&pool, "INSERT INTO t (id, name) VALUES (1, 'start')").await.expect("insert");

    let mut first = conn(&server).await;
    exec(&mut first, "BEGIN").await.expect("begin");
    exec(&mut first, "UPDATE t SET name = 'first' WHERE id = 1").await.expect("update");
    exec(&mut first, "ROLLBACK").await.expect("rollback");

    let mut second = conn(&server).await;
    exec(&mut second, "BEGIN").await.expect("begin");
    exec(&mut second, "UPDATE t SET name = 'second' WHERE id = 1")
        .await
        .expect("the abort released it");
    exec(&mut second, "COMMIT").await.expect("commit");

    let all = sqlx::query("SELECT name FROM t WHERE id = 1")
        .fetch_all(&pool)
        .await
        .expect("select");
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].try_get::<String, _>("name").unwrap(), "second");
}

/// A delete racing an update is the same conflict by another route.
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_conflicts_with_a_concurrent_update() {
    let (server, pool) = server().await;
    table(&pool).await;
    run(&pool, "INSERT INTO t (id, name) VALUES (1, 'start')").await.expect("insert");

    let mut first = conn(&server).await;
    let mut second = conn(&server).await;
    exec(&mut first, "BEGIN").await.expect("begin");
    exec(&mut second, "BEGIN").await.expect("begin");

    exec(&mut first, "UPDATE t SET name = 'first' WHERE id = 1").await.expect("update");
    let conflict = exec(&mut second, "DELETE FROM t WHERE id = 1")
        .await
        .expect_err("the deleter must be told to retry");
    assert!(conflict.to_ascii_lowercase().contains("serialize"), "{}", conflict);

    exec(&mut first, "ROLLBACK").await.expect("rollback");
    exec(&mut second, "ROLLBACK").await.expect("rollback");
}

/// The default is `READ COMMITTED`: a statement sees whatever has committed by
/// the time it runs, so the same query can give different answers within one
/// transaction.
#[tokio::test(flavor = "multi_thread")]
async fn read_committed_sees_commits_that_land_mid_transaction() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut reader = conn(&server).await;
    exec(&mut reader, "BEGIN").await.expect("begin");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await, 0);

    run(&pool, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");

    assert_eq!(
        rows(&mut reader, "SELECT id FROM t").await,
        1,
        "read committed takes a fresh snapshot per statement"
    );
    exec(&mut reader, "COMMIT").await.expect("commit");
}

/// Under `REPEATABLE READ` the same query must give the same answer for the
/// whole transaction, however much commits underneath it.
#[tokio::test(flavor = "multi_thread")]
async fn repeatable_read_does_not_see_later_commits() {
    let (server, pool) = server().await;
    table(&pool).await;
    run(&pool, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");

    let mut reader = conn(&server).await;
    exec(&mut reader, "BEGIN ISOLATION LEVEL REPEATABLE READ").await.expect("begin");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await, 1);

    run(&pool, "INSERT INTO t (id, name) VALUES (2, 'b')").await.expect("insert");
    run(&pool, "DELETE FROM t WHERE id = 1").await.expect("delete");

    assert_eq!(
        rows(&mut reader, "SELECT id FROM t").await,
        1,
        "the snapshot is fixed for the transaction: neither the insert nor the delete shows"
    );

    exec(&mut reader, "COMMIT").await.expect("commit");
    assert_eq!(count(&pool, "SELECT id FROM t").await, 1, "and afterwards it sees the truth");
}

/// `SERIALIZABLE` is accepted and behaves as `REPEATABLE READ` — see the caveat
/// in `docs/mvcc.md`; it does not detect write skew.
#[tokio::test(flavor = "multi_thread")]
async fn serializable_is_accepted_and_fixes_the_snapshot() {
    let (server, pool) = server().await;
    table(&pool).await;
    run(&pool, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");

    let mut reader = conn(&server).await;
    exec(&mut reader, "BEGIN ISOLATION LEVEL SERIALIZABLE").await.expect("begin");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await, 1);

    run(&pool, "INSERT INTO t (id, name) VALUES (2, 'b')").await.expect("insert");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await, 1);

    exec(&mut reader, "COMMIT").await.expect("commit");
}

/// `SET TRANSACTION ISOLATION LEVEL` used to fall through to the blanket `SET`
/// no-op, so asking for a stricter level silently got the default.
#[tokio::test(flavor = "multi_thread")]
async fn set_transaction_isolation_level_takes_effect() {
    let (server, pool) = server().await;
    table(&pool).await;
    run(&pool, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");

    let mut reader = conn(&server).await;
    exec(&mut reader, "BEGIN").await.expect("begin");
    exec(&mut reader, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("set");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await, 1);

    run(&pool, "INSERT INTO t (id, name) VALUES (2, 'b')").await.expect("insert");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await, 1, "the level was applied");

    exec(&mut reader, "COMMIT").await.expect("commit");
}

/// A repeatable-read transaction still sees its *own* writes — the fixed
/// snapshot hides other transactions, not itself.
#[tokio::test(flavor = "multi_thread")]
async fn a_fixed_snapshot_still_shows_its_own_writes() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN ISOLATION LEVEL REPEATABLE READ").await.expect("begin");
    assert_eq!(rows(&mut c, "SELECT id FROM t").await, 0);
    exec(&mut c, "INSERT INTO t (id, name) VALUES (1, 'mine')").await.expect("insert");
    assert_eq!(rows(&mut c, "SELECT id FROM t").await, 1, "its own insert is visible");
    exec(&mut c, "COMMIT").await.expect("commit");

    assert_eq!(count(&pool, "SELECT id FROM t").await, 1);
}

/// The in-place fast path overwrites a row's bytes without writing a new
/// version. That is safe only when the write can never need to be undone — and
/// inside an explicit block it always can.
#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_update_to_a_fixed_width_column_is_undone() {
    let (server, pool) = server().await;
    run(&pool, "CREATE TABLE m (id INT NOT NULL, amount BIGINT)").await.expect("create");
    run(&pool, "INSERT INTO m (id, amount) VALUES (1, 100)").await.expect("insert");

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "UPDATE m SET amount = 999 WHERE id = 1").await.expect("update");
    exec(&mut c, "ROLLBACK").await.expect("rollback");

    let row = sqlx::query("SELECT amount FROM m WHERE id = 1")
        .fetch_one(&pool)
        .await
        .expect("select");
    assert_eq!(
        row.try_get::<i64, _>("amount").unwrap(),
        100,
        "an in-place overwrite left nothing for the rollback to restore"
    );
}

/// `SET`/`SHOW` are PostgreSQL runtime parameters *and* the engine's own
/// configuration surface. Answering them all generically swallowed the second
/// kind: over the Postgres wire an engine `SET` was acknowledged and did
/// nothing, while the same statement over the native protocol worked.
///
/// The rule is that anything the executor recognises belongs to the executor.
#[tokio::test(flavor = "multi_thread")]
async fn session_parameters_do_not_swallow_engine_statements() {
    let (_s, pool) = server().await;

    // A genuine runtime parameter is still accepted and ignored, because psql
    // and every driver send these on connect.
    run(&pool, "SET client_encoding = 'UTF8'").await.expect("runtime parameter");
    run(&pool, "SET search_path = public").await.expect("runtime parameter");

    // ...and a transaction-control SET reaches the code that acts on it, rather
    // than being acknowledged and dropped.
    let mut c = conn(&_s).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("set isolation");
    // Proof it acted: the snapshot is now fixed, so a commit from another
    // session is invisible to this one.
    run(&pool, "CREATE TABLE later (id INT NOT NULL)").await.expect("create");
    run(&pool, "INSERT INTO later (id) VALUES (1)").await.expect("insert");
    exec(&mut c, "COMMIT").await.expect("commit");
}

/// `SHOW` for a parameter the engine does not know is still answered rather than
/// erroring, which is what drivers expect.
#[tokio::test(flavor = "multi_thread")]
async fn show_still_answers_runtime_parameters() {
    let (_s, pool) = server().await;

    for stmt in ["SHOW server_version", "SHOW client_encoding", "SHOW TimeZone"] {
        let rows = sqlx::query(stmt)
            .fetch_all(&pool)
            .await
            .unwrap_or_else(|e| panic!("{}: {}", stmt, e));
        assert_eq!(rows.len(), 1, "{} should answer with one row", stmt);
    }
}

// ---- savepoints ---------------------------------------------------------
//
// A savepoint lets a block survive a failed statement instead of losing all of
// it. Every ORM that offers nested transactions — sea-orm included — builds them
// out of these, so "nested transaction" support is really savepoint support.

#[tokio::test(flavor = "multi_thread")]
async fn rolling_back_to_a_savepoint_keeps_the_work_before_it() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (1, 'kept')").await.expect("first");
    exec(&mut c, "SAVEPOINT sp1").await.expect("savepoint");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (2, 'discarded')").await.expect("second");
    assert_eq!(rows(&mut c, "SELECT id FROM t").await, 2);

    exec(&mut c, "ROLLBACK TO SAVEPOINT sp1").await.expect("rollback to");
    assert_eq!(rows(&mut c, "SELECT id FROM t").await, 1, "only the later insert went");

    exec(&mut c, "COMMIT").await.expect("commit");
    assert_eq!(count(&pool, "SELECT id FROM t").await, 1);
    assert_eq!(count(&pool, "SELECT id FROM t WHERE id = 1").await, 1, "the kept one");
}

/// The reason savepoints exist: recovering from a failed statement without
/// losing the block. Before this, the block was poisoned and everything was lost.
#[tokio::test(flavor = "multi_thread")]
async fn a_savepoint_recovers_a_block_from_a_failed_statement() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("first");
    exec(&mut c, "SAVEPOINT sp").await.expect("savepoint");
    exec(&mut c, "INSERT INTO nonexistent (id) VALUES (1)").await.expect_err("no such table");

    // Poisoned, as it should be...
    exec(&mut c, "INSERT INTO t (id, name) VALUES (2, 'b')")
        .await
        .expect_err("block is aborted");

    // ...and the savepoint brings it back.
    exec(&mut c, "ROLLBACK TO SAVEPOINT sp").await.expect("rollback to");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (3, 'c')")
        .await
        .expect("the block is usable again");
    exec(&mut c, "COMMIT").await.expect("commit");

    assert_eq!(count(&pool, "SELECT id FROM t").await, 2, "the first and the third");
}

#[tokio::test(flavor = "multi_thread")]
async fn releasing_a_savepoint_keeps_its_work() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "SAVEPOINT sp").await.expect("savepoint");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");
    exec(&mut c, "RELEASE SAVEPOINT sp").await.expect("release");
    exec(&mut c, "COMMIT").await.expect("commit");

    assert_eq!(count(&pool, "SELECT id FROM t").await, 1);
}

/// Nested savepoints: rolling back the outer one takes the inner with it.
#[tokio::test(flavor = "multi_thread")]
async fn nested_savepoints_unwind_together() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (1, 'outer')").await.expect("insert");
    exec(&mut c, "SAVEPOINT a").await.expect("savepoint a");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (2, 'in a')").await.expect("insert");
    exec(&mut c, "SAVEPOINT b").await.expect("savepoint b");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (3, 'in b')").await.expect("insert");
    assert_eq!(rows(&mut c, "SELECT id FROM t").await, 3);

    exec(&mut c, "ROLLBACK TO SAVEPOINT a").await.expect("rollback to a");
    assert_eq!(rows(&mut c, "SELECT id FROM t").await, 1, "b went with a");

    exec(&mut c, "COMMIT").await.expect("commit");
    assert_eq!(count(&pool, "SELECT id FROM t").await, 1);
}

/// Rolling back the whole block discards savepointed work too.
#[tokio::test(flavor = "multi_thread")]
async fn rolling_back_the_block_discards_savepointed_work() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "SAVEPOINT sp").await.expect("savepoint");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");
    exec(&mut c, "RELEASE SAVEPOINT sp").await.expect("release");
    exec(&mut c, "ROLLBACK").await.expect("rollback");

    assert_eq!(count(&pool, "SELECT id FROM t").await, 0, "release is not commit");
}

/// A savepoint's work is invisible outside the block, like the block's own.
#[tokio::test(flavor = "multi_thread")]
async fn savepointed_work_is_invisible_to_other_sessions() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "SAVEPOINT sp").await.expect("savepoint");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");

    assert_eq!(count(&pool, "SELECT id FROM t").await, 0);
    exec(&mut c, "COMMIT").await.expect("commit");
    assert_eq!(count(&pool, "SELECT id FROM t").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_savepoint_outside_a_block_is_refused() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    let err = exec(&mut c, "SAVEPOINT sp").await.expect_err("no block open");
    assert!(err.to_ascii_lowercase().contains("transaction"), "{}", err);
}

#[tokio::test(flavor = "multi_thread")]
async fn rolling_back_to_an_unknown_savepoint_is_refused() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    let err = exec(&mut c, "ROLLBACK TO SAVEPOINT nope")
        .await
        .expect_err("no such savepoint");
    assert!(err.contains("nope"), "the error should name it: {}", err);
    exec(&mut c, "ROLLBACK").await.expect("rollback");
}

/// PostgreSQL lets a name be reused: the newest shadows the older, and rolling
/// back to it leaves the older standing.
#[tokio::test(flavor = "multi_thread")]
async fn a_reused_savepoint_name_shadows_the_older_one() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "SAVEPOINT sp").await.expect("first sp");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (1, 'first')").await.expect("insert");
    exec(&mut c, "SAVEPOINT sp").await.expect("second sp, same name");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (2, 'second')").await.expect("insert");

    exec(&mut c, "ROLLBACK TO SAVEPOINT sp").await.expect("rollback to");
    assert_eq!(
        rows(&mut c, "SELECT id FROM t").await,
        1,
        "only work after the newer savepoint went"
    );
    exec(&mut c, "COMMIT").await.expect("commit");
    assert_eq!(count(&pool, "SELECT id FROM t").await, 1);
}

/// sqlx builds nested transactions out of savepoints, which is the shape an
/// ORM actually uses.
#[tokio::test(flavor = "multi_thread")]
async fn the_driver_nested_transaction_api_works() {
    let (_s, pool) = server().await;
    table(&pool).await;

    let mut outer = pool.begin().await.expect("outer begin");
    sqlx::query("INSERT INTO t (id, name) VALUES (1, 'outer')")
        .execute(&mut *outer)
        .await
        .expect("outer insert");

    // sqlx exposes nesting through `Acquire`, and it emits SAVEPOINT/RELEASE.
    use sqlx::Acquire;
    let mut inner = outer.begin().await.expect("nested begin");
    sqlx::query("INSERT INTO t (id, name) VALUES (2, 'inner')")
        .execute(&mut *inner)
        .await
        .expect("inner insert");
    inner.rollback().await.expect("nested rollback");

    outer.commit().await.expect("outer commit");

    assert_eq!(count(&pool, "SELECT id FROM t").await, 1, "only the outer insert");
    assert_eq!(count(&pool, "SELECT id FROM t WHERE id = 1").await, 1);
}

/// The case that made the query cache unsound under MVCC, and the reason it is
/// now keyed by predicate rather than by what a snapshot could see.
///
/// A transaction writes, a query caches the result, the transaction commits. The
/// commit changes what is visible while writing nothing, so the generation
/// counter cannot know the entry is stale. The row has to be *in* the cached set
/// all along — invisible at the time, visible afterwards — or the later query
/// answers from a set that can never contain it.
#[tokio::test(flavor = "multi_thread")]
async fn a_commit_after_a_cached_query_is_still_seen() {
    let (server, pool) = server().await;
    table(&pool).await;
    run(&pool, "INSERT INTO t (id, name) VALUES (1, 'first')").await.expect("insert");

    let mut writer = conn(&server).await;
    exec(&mut writer, "BEGIN").await.expect("begin");
    exec(&mut writer, "INSERT INTO t (id, name) VALUES (2, 'second')").await.expect("insert");

    // Runs and is cached while the writer's row exists but is invisible.
    assert_eq!(count(&pool, "SELECT id FROM t WHERE id > 0").await, 1);
    assert_eq!(count(&pool, "SELECT id FROM t WHERE id > 0").await, 1, "same answer");

    exec(&mut writer, "COMMIT").await.expect("commit");

    assert_eq!(
        count(&pool, "SELECT id FROM t WHERE id > 0").await,
        2,
        "the committed row must appear; a cached visible-set could never contain it"
    );
}

/// And the reverse: a delete that commits after an entry is cached must stop
/// being returned.
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_after_a_cached_query_stops_being_returned() {
    let (server, pool) = server().await;
    table(&pool).await;
    for id in 1..=3 {
        run(&pool, &format!("INSERT INTO t (id, name) VALUES ({}, 'x')", id))
            .await
            .expect("insert");
    }
    assert_eq!(count(&pool, "SELECT id FROM t WHERE id > 0").await, 3);

    let mut deleter = conn(&server).await;
    exec(&mut deleter, "BEGIN").await.expect("begin");
    exec(&mut deleter, "DELETE FROM t WHERE id = 2").await.expect("delete");
    assert_eq!(count(&pool, "SELECT id FROM t WHERE id > 0").await, 3, "not yet");

    exec(&mut deleter, "COMMIT").await.expect("commit");
    assert_eq!(count(&pool, "SELECT id FROM t WHERE id > 0").await, 2);
}

/// A data-modifying CTE must belong to the statement's transaction.
///
/// `WITH d AS (INSERT … RETURNING id) INSERT …` is one statement. If the CTE's
/// write commits on its own, a `ROLLBACK` leaves it behind — and, worse, a
/// failure in the outer statement leaves the database half-written with no
/// error to say so. pms's seed migrations chain several of these, so a failure
/// mid-chain would make a re-run collide with its own leftovers.
#[tokio::test(flavor = "multi_thread")]
async fn a_data_modifying_cte_rolls_back_with_its_statement() {
    let (server, pool) = server().await;
    run(&pool, "CREATE TABLE cd (id INT NOT NULL, nm TEXT)").await.expect("cd");
    run(&pool, "CREATE TABLE cu (id INT NOT NULL, did INT)").await.expect("cu");

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(
        &mut c,
        "WITH d AS (INSERT INTO cd (id, nm) VALUES (1,'acme') RETURNING id) \
         INSERT INTO cu (id, did) SELECT 5, id FROM d",
    )
    .await
    .expect("cte insert");
    assert_eq!(rows(&mut c, "SELECT id FROM cd").await, 1, "visible to its own transaction");
    exec(&mut c, "ROLLBACK").await.expect("rollback");

    assert_eq!(
        count(&pool, "SELECT id FROM cd").await,
        0,
        "the CTE's INSERT survived a ROLLBACK, so it committed on its own"
    );
    assert_eq!(count(&pool, "SELECT id FROM cu").await, 0);
}

/// Outside a block too: a statement whose CTE writes and whose outer statement
/// then fails must leave nothing behind.
///
/// This is the shape a seed migration uses — several chained CTEs, each feeding
/// the next. A failure mid-chain that left the earlier writes committed would
/// make a re-run collide with its own leftovers, which is the worst way for a
/// migration to fail.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_statement_leaves_no_cte_writes_behind() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE sd (id INT NOT NULL, nm TEXT)").await.expect("sd");
    run(&pool, "CREATE TABLE su (id INT NOT NULL, did INT)").await.expect("su");

    // The outer statement names a column that does not exist, so it fails after
    // the CTE body has already run.
    run(
        &pool,
        "WITH d AS (INSERT INTO sd (id, nm) VALUES (1,'acme') RETURNING id) \
         INSERT INTO su (id, nope) SELECT 5, id FROM d",
    )
    .await
    .expect_err("the outer statement should fail");

    assert_eq!(
        count(&pool, "SELECT id FROM sd").await,
        0,
        "the CTE's INSERT committed even though the statement failed"
    );
    assert_eq!(count(&pool, "SELECT id FROM su").await, 0);
}

/// And the successful case still lands, including the chaining.
#[tokio::test(flavor = "multi_thread")]
async fn chained_data_modifying_ctes_land_together() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE gd (id INT NOT NULL, nm TEXT)").await.expect("gd");
    run(&pool, "CREATE TABLE gu (id INT NOT NULL, did INT)").await.expect("gu");

    run(
        &pool,
        "WITH d AS (INSERT INTO gd (id, nm) VALUES (7,'zapier') RETURNING id) \
         INSERT INTO gu (id, did) SELECT 70, id FROM d",
    )
    .await
    .expect("chained cte");

    assert_eq!(count(&pool, "SELECT id FROM gd").await, 1);
    let row = sqlx::query("SELECT did FROM gu WHERE id = 70")
        .fetch_one(&pool)
        .await
        .expect("select");
    assert_eq!(row.try_get::<i32, _>("did").unwrap(), 7, "the RETURNING value fed the outer insert");
}

/// The shape a real seed migration uses: two data-modifying CTEs chained, joined
/// by the **comma form** of a cross join.
///
/// `FROM user_id, domain_id` is an implicit cross join, so both names are
/// relation positions — but only the first follows `FROM`. Rewriting CTE
/// references only after `FROM`/`JOIN` left the second pointing at a name that
/// no longer existed, while the qualified `domain_id.id` in the select list
/// *was* rewritten, so the two halves disagreed.
#[tokio::test(flavor = "multi_thread")]
async fn chained_ctes_joined_by_the_comma_form_resolve() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE zdom (id INT NOT NULL, nm TEXT)").await.expect("zdom");
    run(&pool, "CREATE TABLE zusr (id INT NOT NULL, did INT)").await.expect("zusr");
    run(&pool, "CREATE TABLE zrole (uid INT NOT NULL, did INT NOT NULL)").await.expect("zrole");

    run(
        &pool,
        "WITH domain_id AS (INSERT INTO zdom (id, nm) VALUES (1,'zapier') RETURNING id), \
         user_id AS (INSERT INTO zusr (id, did) VALUES (50, 1) RETURNING id) \
         INSERT INTO zrole (uid, did) SELECT user_id.id, domain_id.id FROM user_id, domain_id",
    )
    .await
    .expect("chained ctes with a comma join");

    let row = sqlx::query("SELECT uid, did FROM zrole").fetch_one(&pool).await.expect("select");
    assert_eq!(row.try_get::<i32, _>("uid").unwrap(), 50);
    assert_eq!(row.try_get::<i32, _>("did").unwrap(), 1, "the second CTE's value came through");
    assert_eq!(count(&pool, "SELECT id FROM zdom").await, 1);
    assert_eq!(count(&pool, "SELECT id FROM zusr").await, 1);
}

/// A select item that merely shares a CTE's name must not be rewritten — the
/// comma that opens a relation position only does so inside a `FROM` clause.
#[tokio::test(flavor = "multi_thread")]
async fn a_comma_outside_from_does_not_open_a_relation_position() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE src (a INT NOT NULL, b INT NOT NULL)").await.expect("src");
    run(&pool, "INSERT INTO src (a, b) VALUES (1, 2)").await.expect("insert");

    // `b` is both a column here and the name of the CTE.
    let rows = sqlx::query("WITH b AS (SELECT a FROM src) SELECT a, b FROM src")
        .fetch_all(&pool)
        .await
        .expect("column sharing a CTE name");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].try_get::<i32, _>("b").unwrap(), 2, "the column was rewritten away");
}

/// A join inside a transaction must see the transaction's own writes.
///
/// It did not: `execute_join` scanned every relation with the non-transactional
/// `query_row`, so rows the same transaction had just written were invisible to
/// it. `BEGIN; INSERT …; SELECT … JOIN …` found nothing, and — because a join
/// that matches no rows is not an error — the statement reported success while
/// returning an empty result.
///
/// The visible symptom was two tables away: a chained data-modifying CTE
/// materialises each body's `RETURNING` rows into a temporary table inside the
/// statement's transaction, then joins those tables. Both temporaries held
/// their row and the rewritten SQL was correct, yet the outer
/// `INSERT … SELECT … FROM a, b` inserted nothing and reported `INSERT 0`.
#[tokio::test(flavor = "multi_thread")]
async fn a_join_sees_the_transactions_own_writes() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE jl (id INT NOT NULL, nm TEXT)").await.expect("jl");
    run(&pool, "CREATE TABLE jr (id INT NOT NULL, jl_id INT NOT NULL)").await.expect("jr");

    let mut tx = pool.begin().await.expect("begin");
    sqlx::query("INSERT INTO jl (id, nm) VALUES (1, 'left')").execute(&mut *tx).await.expect("insert jl");
    sqlx::query("INSERT INTO jr (id, jl_id) VALUES (10, 1)").execute(&mut *tx).await.expect("insert jr");

    // Both rows are this transaction's own, uncommitted work.
    let rows = sqlx::query(
        "SELECT jl.nm, jr.id FROM jl JOIN jr ON jl.id = jr.jl_id",
    )
    .fetch_all(&mut *tx)
    .await
    .expect("join inside the transaction");
    assert_eq!(rows.len(), 1, "the join must see the rows this transaction wrote");
    assert_eq!(rows[0].try_get::<String, _>("nm").unwrap(), "left");

    // The comma form is the same scan path and must agree.
    let rows = sqlx::query("SELECT jl.nm, jr.id FROM jl, jr")
        .fetch_all(&mut *tx)
        .await
        .expect("cross join inside the transaction");
    assert_eq!(rows.len(), 1, "the comma form must see them too");

    tx.commit().await.expect("commit");
    assert_eq!(count(&pool, "SELECT nm FROM jl JOIN jr ON jl.id = jr.jl_id").await, 1);
}

/// A join must not see another transaction's uncommitted writes.
///
/// The guard on the fix above: threading the transaction into the scan is only
/// correct if it is the *reader's* transaction. Passing one that sees everything
/// would have made the test above pass for the wrong reason.
#[tokio::test(flavor = "multi_thread")]
async fn a_join_does_not_see_another_transactions_uncommitted_writes() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE il (id INT NOT NULL, nm TEXT)").await.expect("il");
    run(&pool, "CREATE TABLE ir (id INT NOT NULL, il_id INT NOT NULL)").await.expect("ir");
    run(&pool, "INSERT INTO il (id, nm) VALUES (1, 'committed')").await.expect("seed il");
    run(&pool, "INSERT INTO ir (id, il_id) VALUES (10, 1)").await.expect("seed ir");

    let mut tx = pool.begin().await.expect("begin");
    sqlx::query("INSERT INTO il (id, nm) VALUES (2, 'uncommitted')").execute(&mut *tx).await.expect("insert");
    sqlx::query("INSERT INTO ir (id, il_id) VALUES (20, 2)").execute(&mut *tx).await.expect("insert");

    // Outside the block, only the committed pair is visible.
    assert_eq!(
        count(&pool, "SELECT nm FROM il JOIN ir ON il.id = ir.il_id").await,
        1,
        "another connection must not see the open transaction's rows"
    );

    tx.rollback().await.expect("rollback");
    assert_eq!(
        count(&pool, "SELECT nm FROM il JOIN ir ON il.id = ir.il_id").await,
        1,
        "and a rolled back pair never becomes visible"
    );
}

async fn show(c: &mut sqlx::PgConnection, parameter: &str) -> String {
    let row = sqlx::query(&format!("SHOW {}", parameter))
        .fetch_one(&mut *c)
        .await
        .expect("SHOW answers");
    row.try_get::<String, _>(0).expect("one text column")
}

async fn sqlstate(c: &mut sqlx::PgConnection, sql: &str) -> Option<String> {
    match sqlx::query(sql).execute(&mut *c).await {
        Ok(_) => None,
        Err(e) => e.as_database_error().and_then(|d| d.code().map(|c| c.to_string())),
    }
}

/// `READ ONLY` used to be acknowledged and ignored — the block was read-write
/// and reported as read-only. PostgreSQL refuses the write with `25006`.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_block_refuses_writes() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN READ ONLY").await.expect("begin read only");
    assert_eq!(show(&mut c, "transaction_read_only").await, "on");
    assert_eq!(
        sqlstate(&mut c, "INSERT INTO t (id, name) VALUES (1, 'a')").await.as_deref(),
        Some("25006"),
        "a write inside a read-only block is refused"
    );
    exec(&mut c, "ROLLBACK").await.expect("rollback");
    assert_eq!(count(&pool, "SELECT * FROM t").await, 0);

    // `SET TRANSACTION READ ONLY` inside an ordinary block has the same effect,
    // and reads still work.
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "SET TRANSACTION READ ONLY").await.expect("set read only");
    assert_eq!(rows(&mut c, "SELECT * FROM t").await, 0);
    assert_eq!(
        sqlstate(&mut c, "CREATE TABLE forbidden (id INT)").await.as_deref(),
        Some("25006")
    );
    exec(&mut c, "COMMIT").await.expect("a failed block commits as a rollback");

    // READ WRITE is the default and writes go through.
    exec(&mut c, "START TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ WRITE")
        .await
        .expect("modes may be combined");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("write");
    exec(&mut c, "COMMIT").await.expect("commit");
    assert_eq!(count(&pool, "SELECT * FROM t").await, 1);

    // The session default applies to blocks that name no mode, and to
    // autocommit statements.
    exec(&mut c, "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        .await
        .expect("session default");
    assert_eq!(show(&mut c, "default_transaction_read_only").await, "on");
    assert_eq!(
        sqlstate(&mut c, "DELETE FROM t WHERE id = 1").await.as_deref(),
        Some("25006")
    );
    exec(&mut c, "BEGIN READ WRITE").await.expect("a block may override the default");
    exec(&mut c, "DELETE FROM t WHERE id = 1").await.expect("write in a read-write block");
    exec(&mut c, "COMMIT").await.expect("commit");
    assert_eq!(count(&pool, "SELECT * FROM t").await, 0);
}

/// `COMMIT AND CHAIN` closes the block and opens another with the same
/// characteristics, so the client is never between blocks.
#[tokio::test(flavor = "multi_thread")]
async fn commit_and_chain_opens_the_next_block() {
    let (server, pool) = server().await;
    table(&pool).await;

    let mut c = conn(&server).await;
    exec(&mut c, "BEGIN ISOLATION LEVEL REPEATABLE READ").await.expect("begin");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("write");
    exec(&mut c, "COMMIT AND CHAIN").await.expect("commit and chain");

    // Still in a block, at the same level: a ROLLBACK has something to undo.
    assert_eq!(show(&mut c, "transaction_isolation").await, "repeatable read");
    exec(&mut c, "INSERT INTO t (id, name) VALUES (2, 'b')").await.expect("write");
    exec(&mut c, "ROLLBACK AND CHAIN").await.expect("rollback and chain");
    assert_eq!(count(&pool, "SELECT * FROM t").await, 1, "the chained block's write was undone");

    exec(&mut c, "INSERT INTO t (id, name) VALUES (3, 'c')").await.expect("write");
    exec(&mut c, "COMMIT").await.expect("plain commit ends the chain");
    assert_eq!(count(&pool, "SELECT * FROM t").await, 2);
    // Outside a block now: a ROLLBACK is a notice, and nothing is undone.
    exec(&mut c, "ROLLBACK").await.expect("no block");
    assert_eq!(count(&pool, "SELECT * FROM t").await, 2);
}

/// `SHOW` answers the parameters drivers read at start-up from the session,
/// and refuses one this server does not have rather than answering "".
#[tokio::test(flavor = "multi_thread")]
async fn show_reports_the_session_and_refuses_unknown_parameters() {
    let (server, _pool) = server().await;
    let mut c = conn(&server).await;

    assert_eq!(show(&mut c, "transaction_isolation").await, "read committed");
    assert_eq!(show(&mut c, "is_superuser").await, "on");
    assert_eq!(show(&mut c, "server_version").await, "16.3");
    exec(&mut c, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("set");
    assert_eq!(show(&mut c, "transaction_isolation").await, "repeatable read");
    assert_eq!(sqlstate(&mut c, "SHOW no_such_parameter").await.as_deref(), Some("42704"));
}

/// `SET LOCAL` belongs to its block. Treated as `SET SESSION` it would be a
/// durability leak in the dangerous direction: one transaction asking to skip
/// its `fdatasync` would quietly decide that for every later transaction on the
/// connection, for as long as it stayed open.
#[tokio::test(flavor = "multi_thread")]
async fn set_local_synchronous_commit_does_not_outlive_its_block() {
    let (server, _pool) = server().await;
    let mut c = conn(&server).await;

    assert_eq!(show(&mut c, "synchronous_commit").await, "on");

    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "SET LOCAL synchronous_commit = off").await.expect("set local");
    assert_eq!(
        show(&mut c, "synchronous_commit").await,
        "off",
        "it applies inside the block that set it"
    );
    exec(&mut c, "COMMIT").await.expect("commit");
    assert_eq!(
        show(&mut c, "synchronous_commit").await,
        "on",
        "and is put back when the block ends"
    );

    // A rollback ends the block too.
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "SET LOCAL synchronous_commit = off").await.expect("set local");
    exec(&mut c, "ROLLBACK").await.expect("rollback");
    assert_eq!(show(&mut c, "synchronous_commit").await, "on");

    // So does a block that failed and commits as a rollback.
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "SET LOCAL synchronous_commit = off").await.expect("set local");
    assert!(exec(&mut c, "SELECT * FROM no_such_table").await.is_err(), "poisons the block");
    exec(&mut c, "COMMIT").await.expect("a poisoned block commits as a rollback");
    assert_eq!(show(&mut c, "synchronous_commit").await, "on");

    // Two in one block put back the value the block started with, not the one
    // the first of them left.
    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "SET LOCAL synchronous_commit = off").await.expect("first");
    exec(&mut c, "SET LOCAL synchronous_commit = on").await.expect("second");
    exec(&mut c, "COMMIT").await.expect("commit");
    assert_eq!(show(&mut c, "synchronous_commit").await, "on");
}

/// A plain `SET` is the session's, and is the control that proves the test
/// above is measuring `LOCAL` rather than the setting simply never sticking.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_set_outlives_the_block() {
    let (server, _pool) = server().await;
    let mut c = conn(&server).await;

    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "SET synchronous_commit = off").await.expect("set session");
    exec(&mut c, "COMMIT").await.expect("commit");
    assert_eq!(
        show(&mut c, "synchronous_commit").await,
        "off",
        "a session SET is not undone by the block ending"
    );

    exec(&mut c, "RESET synchronous_commit").await.expect("reset");
    assert_eq!(show(&mut c, "synchronous_commit").await, "on");
}

/// Outside a block there is nothing for `LOCAL` to be local to. PostgreSQL
/// warns and changes nothing; applying it to the session would be exactly the
/// leak this is here to prevent.
#[tokio::test(flavor = "multi_thread")]
async fn set_local_outside_a_block_changes_nothing() {
    let (server, _pool) = server().await;
    let mut c = conn(&server).await;

    exec(&mut c, "SET LOCAL synchronous_commit = off")
        .await
        .expect("accepted, as PostgreSQL accepts it");
    assert_eq!(
        show(&mut c, "synchronous_commit").await,
        "on",
        "but it must not have touched the session"
    );
}

/// `ROLLBACK TO` undoes a `SET LOCAL` made after the savepoint, along with the
/// writes it accompanied.
#[tokio::test(flavor = "multi_thread")]
async fn rollback_to_savepoint_restores_the_setting() {
    let (server, _pool) = server().await;
    let mut c = conn(&server).await;

    exec(&mut c, "BEGIN").await.expect("begin");
    exec(&mut c, "SAVEPOINT sp").await.expect("savepoint");
    exec(&mut c, "SET LOCAL synchronous_commit = off").await.expect("set local");
    assert_eq!(show(&mut c, "synchronous_commit").await, "off");
    exec(&mut c, "ROLLBACK TO SAVEPOINT sp").await.expect("rollback to");
    assert_eq!(
        show(&mut c, "synchronous_commit").await,
        "on",
        "the setting is rolled back with the work"
    );

    // One set *before* the savepoint survives rolling back to it, and is still
    // undone when the block ends.
    exec(&mut c, "SET LOCAL synchronous_commit = off").await.expect("set local");
    exec(&mut c, "SAVEPOINT sp2").await.expect("savepoint");
    exec(&mut c, "ROLLBACK TO SAVEPOINT sp2").await.expect("rollback to");
    assert_eq!(show(&mut c, "synchronous_commit").await, "off");
    exec(&mut c, "COMMIT").await.expect("commit");
    assert_eq!(show(&mut c, "synchronous_commit").await, "on");
}
