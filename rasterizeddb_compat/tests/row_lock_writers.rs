//! Writers and `FOR UPDATE`: which column a lock is keyed by, and who owns a
//! lock taken inside a savepoint.
//!
//! `SELECT … FOR UPDATE` keys its locks by the table's identifying column —
//! a column-level `UNIQUE`, else the primary key, else a single-column table
//! `UNIQUE`. The writer-side check picked only the first of those, so on a table
//! identified any other way an `UPDATE` from another session looked for locks
//! under no column at all and overwrote a row somebody held.
//!
//! A lock taken inside a savepoint was recorded under the savepoint's id. The
//! transaction's own later write then met it as somebody else's lock, and its
//! `COMMIT` never released it.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor, Row};

async fn connect(server: &TestServer) -> sqlx::PgConnection {
    sqlx::PgConnection::connect(&server.url())
        .await
        .expect("connect")
}

async fn exec(conn: &mut sqlx::PgConnection, sql: &str) -> Result<(), String> {
    conn.execute(AssertSqlSafe(sql.to_string()))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

async fn state(conn: &mut sqlx::PgConnection, table: &str) -> String {
    sqlx::query(AssertSqlSafe(format!("SELECT state FROM {} WHERE id = 1", table)))
        .fetch_one(&mut *conn)
        .await
        .expect("select")
        .get::<String, _>(0)
}

async fn held_row_is_not_overwritten(create: &str, table: &str) {
    let server = TestServer::start().await.expect("server");
    let mut a = connect(&server).await;
    exec(&mut a, create).await.expect("create");
    exec(&mut a, &format!("INSERT INTO {} (id, state) VALUES (1, 'pending')", table))
        .await
        .expect("insert");

    exec(&mut a, "BEGIN").await.expect("begin");
    exec(&mut a, &format!("SELECT id FROM {} WHERE id = 1 FOR UPDATE", table))
        .await
        .expect("lock");

    let mut b = connect(&server).await;
    let outcome = exec(
        &mut b,
        &format!("UPDATE {} SET state = 'stolen' WHERE id = 1", table),
    )
    .await;
    exec(&mut a, "COMMIT").await.expect("commit");

    assert!(
        outcome.is_err(),
        "{}: an UPDATE overwrote a row another transaction holds FOR UPDATE",
        table
    );
    assert_eq!(state(&mut a, table).await, "pending", "{}", table);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_honours_a_lock_on_a_table_keyed_by_a_table_level_primary_key() {
    held_row_is_not_overwritten(
        "CREATE TABLE q_pk (id INT NOT NULL, state TEXT NOT NULL, PRIMARY KEY (id))",
        "q_pk",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_honours_a_lock_on_a_table_keyed_by_a_table_level_unique() {
    held_row_is_not_overwritten(
        "CREATE TABLE q_uq (id INT NOT NULL, state TEXT NOT NULL, UNIQUE (id))",
        "q_uq",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_honours_a_lock_on_a_table_keyed_by_a_column_unique() {
    held_row_is_not_overwritten(
        "CREATE TABLE q_col (id INT PRIMARY KEY, state TEXT NOT NULL)",
        "q_col",
    )
    .await;
}

/// The claim idiom with a savepoint between the claim and the write.
#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_may_write_a_row_it_locked_from_inside_a_savepoint() {
    let server = TestServer::start().await.expect("server");
    let mut a = connect(&server).await;
    exec(&mut a, "CREATE TABLE jobs (id INT PRIMARY KEY, state TEXT NOT NULL)")
        .await
        .expect("create");
    exec(&mut a, "INSERT INTO jobs (id, state) VALUES (1, 'pending')")
        .await
        .expect("insert");

    exec(&mut a, "BEGIN").await.expect("begin");
    exec(&mut a, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE")
        .await
        .expect("claim");
    exec(&mut a, "SAVEPOINT s").await.expect("savepoint");
    exec(&mut a, "UPDATE jobs SET state = 'done' WHERE id = 1")
        .await
        .expect("a transaction is not locked out by its own lock");
    exec(&mut a, "COMMIT").await.expect("commit");
    assert_eq!(state(&mut a, "jobs").await, "done");
}

/// A lock taken inside a savepoint is released when its transaction ends.
#[tokio::test(flavor = "multi_thread")]
async fn a_lock_taken_inside_a_savepoint_is_released_at_commit() {
    let server = TestServer::start().await.expect("server");
    let mut a = connect(&server).await;
    exec(&mut a, "CREATE TABLE jobs (id INT PRIMARY KEY, state TEXT NOT NULL)")
        .await
        .expect("create");
    exec(&mut a, "INSERT INTO jobs (id, state) VALUES (1, 'pending')")
        .await
        .expect("insert");

    exec(&mut a, "BEGIN").await.expect("begin");
    exec(&mut a, "SAVEPOINT s").await.expect("savepoint");
    exec(&mut a, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE")
        .await
        .expect("claim");
    assert_eq!(server.database().row_locks.len(), 1, "the claim holds one lock");
    exec(&mut a, "COMMIT").await.expect("commit");
    assert_eq!(
        server.database().row_locks.len(),
        0,
        "COMMIT must release a lock taken inside a savepoint, not leave it in the table"
    );
}
