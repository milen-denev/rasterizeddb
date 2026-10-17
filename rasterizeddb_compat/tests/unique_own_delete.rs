//! A transaction's own delete frees a unique key for that transaction.
//!
//! The uniqueness check counts a row version as holding its key until a
//! *committed* transaction removes it, so that a concurrent delete still in
//! flight is not mistaken for a free key. It applied that rule to the
//! inserting transaction's own deletes too, and a transaction that deleted
//! (or soft-deleted, or re-keyed) a row and then wrote its key again was
//! refused with 23505. PostgreSQL accepts it: a transaction sees its own
//! delete. That is the ordinary "replace" shape, and with a partial unique
//! index over `deleted_at IS NULL` it is how every soft-delete-and-recreate
//! inside a transaction looks.
//!
//! Every expected answer is PostgreSQL 17's, from the same statements.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn exec(pool: &sqlx::PgPool, sql: &str) {
    sqlx::query(AssertSqlSafe(sql))
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn sqlstate(e: &sqlx::Error) -> Option<String> {
    match e {
        sqlx::Error::Database(d) => d.code().map(|c| c.to_string()),
        _ => None,
    }
}

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    sqlx::query(AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.get::<String, _>(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_key_the_transaction_freed_is_free_to_it() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE one (id INT, k TEXT UNIQUE)").await;
    exec(&pool, "CREATE TABLE two (id INT, a INT, b TEXT, UNIQUE (a, b))").await;
    exec(&pool, "INSERT INTO one VALUES (1, 'a'), (2, 'b')").await;
    exec(&pool, "INSERT INTO two VALUES (1, 1, 'x'), (2, 1, 'y')").await;

    let mut tx = pool.begin().await.expect("begin");
    for sql in [
        // A column-level key, freed by a delete and by a re-key.
        "DELETE FROM one WHERE id = 1",
        "INSERT INTO one VALUES (3, 'a')",
        "UPDATE one SET k = 'c' WHERE id = 2",
        "INSERT INTO one VALUES (4, 'b')",
        // A composite key, which is checked by its own lookup.
        "DELETE FROM two WHERE id = 1",
        "INSERT INTO two VALUES (3, 1, 'x')",
    ] {
        tx.execute(sql).await.unwrap_or_else(|e| panic!("{sql}: PostgreSQL accepts this: {e}"));
    }
    tx.commit().await.expect("commit");
    // PostgreSQL: 2|c 3|a 4|b, and 2|1|y 3|1|x
    assert_eq!(rows(&pool, "SELECT k FROM one ORDER BY id").await, ["c", "a", "b"]);
    assert_eq!(rows(&pool, "SELECT b FROM two ORDER BY id").await, ["y", "x"]);

    // A key the transaction did not free is still taken.
    let e = sqlx::query("INSERT INTO one VALUES (5, 'a')")
        .execute(&pool)
        .await
        .expect_err("PostgreSQL: 23505");
    assert_eq!(sqlstate(&e).as_deref(), Some("23505"), "{e}");
}

/// A savepoint is a sub-transaction with an id of its own, so "deleted by
/// this transaction" has to mean the whole family, and a delete undone by
/// `ROLLBACK TO` must hold the key again.
#[tokio::test(flavor = "multi_thread")]
async fn savepoints_free_and_restore_the_key() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE one (id INT, k TEXT UNIQUE)").await;
    exec(&pool, "INSERT INTO one VALUES (3, 'a')").await;

    // Deleted before the savepoint, written again inside it.
    let mut tx = pool.begin().await.expect("begin");
    tx.execute("DELETE FROM one WHERE id = 3").await.expect("delete");
    tx.execute("SAVEPOINT s").await.expect("savepoint");
    tx.execute("INSERT INTO one VALUES (6, 'a')").await.expect("PostgreSQL accepts this");
    tx.commit().await.expect("commit");
    assert_eq!(rows(&pool, "SELECT k FROM one ORDER BY id").await, ["a"]);

    // Deleted inside a savepoint that is rolled back: the row is back, and so
    // is its key.
    let mut tx = pool.begin().await.expect("begin");
    tx.execute("SAVEPOINT s").await.expect("savepoint");
    tx.execute("DELETE FROM one WHERE id = 6").await.expect("delete");
    tx.execute("ROLLBACK TO SAVEPOINT s").await.expect("rollback to");
    let e = tx.execute("INSERT INTO one VALUES (7, 'a')").await.expect_err("PostgreSQL: 23505");
    assert_eq!(sqlstate(&e).as_deref(), Some("23505"), "{e}");
    drop(tx);
    // PostgreSQL: 6|a
    assert_eq!(rows(&pool, "SELECT k FROM one ORDER BY id").await, ["a"]);
}
