//! A writer that meets another transaction's update of its row, under READ
//! COMMITTED, as PostgreSQL 17 answers it (measured beside this engine,
//! `.claude/reviews/pgdiff/conc.sh`): it waits for that transaction however
//! long it runs, then applies its update to the row as the other left it, or
//! updates nothing when the row no longer satisfies its condition or was
//! deleted. Under REPEATABLE READ it is a serialization failure (40001).
//!
//! Before: every wait past a fixed 1.5 s ended in 40001, and a replay that
//! found the row changed out of its condition, or gone, reported 40001 too.

use rasterizeddb_compat::TestServer;
use sqlx::{Connection, Executor, Row};
use std::time::Duration;

async fn setup() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&server.url())
        .await
        .expect("pool");
    for sql in [
        "CREATE TABLE cc (id INT PRIMARY KEY, v INT NOT NULL, w INT NOT NULL)",
        "INSERT INTO cc (id, v, w) VALUES (1, 100, 0), (2, 200, 0)",
    ] {
        pool.execute(sql).await.expect(sql);
    }
    (server, pool)
}

async fn row(pool: &sqlx::PgPool, id: i32) -> Option<(i32, i32)> {
    sqlx::query("SELECT v, w FROM cc WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .expect("read")
        .map(|r| (r.get(0), r.get(1)))
}

/// Runs `blocker` in a transaction held open for `hold`, while `waiter` runs
/// autocommit on another connection; returns the waiter's answer.
async fn while_held(
    pool: &sqlx::PgPool,
    blocker: &'static str,
    waiter: &'static str,
    hold: Duration,
) -> Result<Vec<i32>, String> {
    let mut a = pool.acquire().await.expect("a");
    a.execute("BEGIN").await.expect("begin a");
    a.execute(blocker).await.expect(blocker);
    let p = pool.clone();
    let w = tokio::spawn(async move {
        sqlx::query(waiter)
            .fetch_all(&p)
            .await
            .map(|rows| rows.iter().map(|r| r.get::<i32, _>(0)).collect::<Vec<_>>())
            .map_err(|e| e.to_string())
    });
    tokio::time::sleep(hold).await;
    if w.is_finished() {
        let early = w.await.expect("join");
        panic!("{waiter}: answered {early:?} while {blocker} still held the row");
    }
    a.execute("COMMIT").await.expect("commit a");
    tokio::time::timeout(Duration::from_secs(20), w).await.expect("waiter ends").expect("join")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_waits_as_long_as_its_blocker_and_applies_on_top() {
    let (_s, pool) = setup().await;
    // Past the 1.5 s the wait used to be bounded by.
    let got = while_held(&pool, "UPDATE cc SET v = v + 5 WHERE id = 1", "UPDATE cc SET v = v + 10 WHERE id = 1 RETURNING v", Duration::from_millis(2500)).await;
    assert_eq!(got, Ok(vec![115]));
    assert_eq!(row(&pool, 1).await, Some((115, 0)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_row_moved_out_of_the_condition_is_not_updated() {
    let (_s, pool) = setup().await;
    let got = while_held(&pool, "UPDATE cc SET v = 50 WHERE id = 1", "UPDATE cc SET w = 1 WHERE id = 1 AND v > 80 RETURNING v", Duration::from_millis(300)).await;
    assert_eq!(got, Ok(vec![]), "PostgreSQL re-checks the condition on the new version and updates nothing");
    assert_eq!(row(&pool, 1).await, Some((50, 0)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_row_deleted_under_the_writer_is_not_updated() {
    let (_s, pool) = setup().await;
    let got = while_held(&pool, "DELETE FROM cc WHERE id = 1", "UPDATE cc SET w = 9 WHERE id = 1 RETURNING id", Duration::from_millis(300)).await;
    assert_eq!(got, Ok(vec![]));
    assert_eq!(row(&pool, 1).await, None);
    assert_eq!(row(&pool, 2).await, Some((200, 0)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_writer_waits_at_the_held_row_and_updates_both() {
    let (_s, pool) = setup().await;
    let got = while_held(&pool, "UPDATE cc SET v = v + 5 WHERE id = 2", "UPDATE cc SET v = v + 1 RETURNING v", Duration::from_millis(300)).await;
    let mut got = got.expect("updates both");
    got.sort();
    assert_eq!(got, vec![101, 206]);
}

/// The same waits inside an explicit READ COMMITTED block: each statement
/// reads a fresh snapshot, so the replay sees the blocker's commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inside_a_read_committed_block_the_statement_waits_and_proceeds() {
    let (_s, pool) = setup().await;
    let mut a = pool.acquire().await.expect("a");
    a.execute("BEGIN").await.expect("begin a");
    a.execute("UPDATE cc SET v = 50 WHERE id = 1").await.expect("a writes");
    let p = pool.clone();
    let b = tokio::spawn(async move {
        let mut b = p.acquire().await.expect("b");
        b.execute("BEGIN").await.expect("begin b");
        b.execute("UPDATE cc SET w = 7 WHERE id = 2").await.expect("b writes first");
        let n = b.execute("UPDATE cc SET w = 1 WHERE v > 80").await.map(|r| r.rows_affected());
        b.execute("COMMIT").await.expect("commit b");
        n.map_err(|e| e.to_string())
    });
    tokio::time::sleep(Duration::from_millis(2000)).await;
    a.execute("COMMIT").await.expect("commit a");
    let n = tokio::time::timeout(Duration::from_secs(20), b).await.expect("b ends").expect("join");
    assert_eq!(n, Ok(1), "only row 2 still has v > 80");
    assert_eq!(row(&pool, 1).await, Some((50, 0)));
    assert_eq!(row(&pool, 2).await, Some((200, 1)));
}

/// REPEATABLE READ is not READ COMMITTED: the version its snapshot sees was
/// replaced, so the write is a serialization failure, as in PostgreSQL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeatable_read_still_fails_with_40001() {
    let (s, pool) = setup().await;
    let mut b = sqlx::PgConnection::connect(&s.url()).await.expect("b");
    b.execute("BEGIN ISOLATION LEVEL REPEATABLE READ").await.expect("begin b");
    let _ = sqlx::query("SELECT v FROM cc WHERE id = 1").fetch_one(&mut b).await.expect("snapshot");
    pool.execute("UPDATE cc SET v = v + 5 WHERE id = 1").await.expect("a commits");
    let e = b.execute("UPDATE cc SET v = v + 10 WHERE id = 1").await.expect_err("40001");
    let code = e.as_database_error().and_then(|d| d.code().map(|c| c.to_string()));
    assert_eq!(code.as_deref(), Some("40001"), "{e}");
    let _ = b.execute("ROLLBACK").await;
    assert_eq!(row(&pool, 1).await, Some((105, 0)));
}
