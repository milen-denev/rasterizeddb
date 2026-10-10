//! A savepoint inside a `REPEATABLE READ` transaction.
//!
//! The savepoint is a sub-transaction with an id of its own, and it shares its
//! parent's pinned snapshot so that it does not start seeing new commits. The
//! shared snapshot was the parent's exactly — its "mine" set held the parent's
//! id and not the savepoint's — so a row written inside the savepoint carried
//! an id the snapshot counted as someone else's, started after it, and the
//! transaction could not see its own insert.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor};

async fn exec(conn: &mut sqlx::PgConnection, sql: &str) {
    conn.execute(AssertSqlSafe(sql.to_string()))
        .await
        .unwrap_or_else(|e| panic!("{}: {}", sql, e));
}

async fn count(conn: &mut sqlx::PgConnection, sql: &str) -> usize {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(&mut *conn)
        .await
        .expect("select")
        .len()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_savepoint_sees_its_own_writes_under_repeatable_read() {
    let server = TestServer::start().await.expect("server");
    let mut c = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    exec(&mut c, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)").await;
    exec(&mut c, "INSERT INTO t (id, name) VALUES (1, 'a')").await;

    exec(&mut c, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
    assert_eq!(count(&mut c, "SELECT id FROM t").await, 1);
    exec(&mut c, "SAVEPOINT s").await;
    exec(&mut c, "INSERT INTO t (id, name) VALUES (2, 'b')").await;
    assert_eq!(
        count(&mut c, "SELECT id FROM t").await,
        2,
        "the savepoint's own insert is invisible to it"
    );
    exec(&mut c, "DELETE FROM t WHERE id = 1").await;
    assert_eq!(count(&mut c, "SELECT id FROM t").await, 1, "and its own delete");
    exec(&mut c, "RELEASE SAVEPOINT s").await;
    assert_eq!(count(&mut c, "SELECT id FROM t").await, 1, "after release too");
    exec(&mut c, "COMMIT").await;
    assert_eq!(count(&mut c, "SELECT id FROM t WHERE id = 2").await, 1);
}
