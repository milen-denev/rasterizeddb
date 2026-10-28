//! A writer that found no row locked and a `FOR UPDATE` that lands before
//! the writer's write.
//!
//! `UPDATE` and `DELETE` honour row locks by asking for each row's lock
//! before writing — unless no row in the database is locked, the fast path,
//! which skips the scan. A writer W that took the fast path and a locker L
//! that locked and read the row before W wrote it both went on: L held a lock
//! on a row W then changed and committed, and L's own update, computed from
//! what it had read, overwrote W's. A lost update.
//!
//! Deterministic: W is parked at the `test.row_write_fast_path` failpoint,
//! right after its check, while L locks the row.

use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::core::power_loss::failpoint::{Outcome, arm};
use sqlx::{Connection, Executor};

#[tokio::test(flavor = "multi_thread")]
async fn a_locker_waits_for_a_fast_path_writer_it_raced() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE fast_path_race (id BIGINT PRIMARY KEY, v BIGINT); INSERT INTO fast_path_race VALUES (1, 0);",
    ))
    .await
    .unwrap();

    let (at, at_rx) = mpsc::channel::<()>();
    let (go, go_rx) = mpsc::channel::<()>();
    let go_rx = Mutex::new(go_rx);
    let _armed = arm(
        "test.row_write_fast_path",
        std::path::Path::new("fast_path_race"),
        1,
        Arc::new(move |_| {
            let _ = at.send(());
            // Bounded, and off the worker: parked in place, the server's
            // other tasks queued on this worker (the locker's connection
            // among them) waited for it.
            tokio::task::block_in_place(|| {
                let _ = go_rx.lock().unwrap().recv_timeout(Duration::from_secs(10));
            });
            Outcome::Continue
        }),
    );

    // W: the fast-path writer, parked after its check.
    let writer_pool = pool.clone();
    let writer = tokio::spawn(async move {
        sqlx::query("UPDATE fast_path_race SET v = v + 1 WHERE id = 1").execute(&writer_pool).await
    });
    tokio::task::spawn_blocking(move || at_rx.recv_timeout(Duration::from_secs(10)))
        .await
        .unwrap()
        .expect("the writer never reached its check");

    // L: locks the row W has checked but not yet written.
    let mut locker = sqlx::PgConnection::connect(&server.url()).await.expect("locker");
    locker.execute("BEGIN").await.unwrap();
    let read = tokio::spawn(async move {
        let v: Result<i64, sqlx::Error> =
            sqlx::query_scalar("SELECT v FROM fast_path_race WHERE id = 1 FOR UPDATE").fetch_one(&mut locker).await;
        (locker, v)
    });
    // Long enough for an unguarded FOR UPDATE to have locked and read.
    tokio::time::sleep(Duration::from_millis(500)).await;
    go.send(()).unwrap();

    writer.await.unwrap().expect("the writer");
    let (mut locker, v) = read.await.unwrap();
    let v = v.expect("the locker's FOR UPDATE");
    // What the locker read is what it holds: the writer's committed value.
    assert_eq!(v, 1, "FOR UPDATE read a row a writer was about to change");
    sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE fast_path_race SET v = {} WHERE id = 1", v + 10)))
        .execute(&mut locker)
        .await
        .expect("the locker's update");
    locker.execute("COMMIT").await.unwrap();

    let v: i64 = sqlx::query_scalar("SELECT v FROM fast_path_race WHERE id = 1").fetch_one(&pool).await.unwrap();
    assert_eq!(v, 11, "an update was lost");
}

/// An explicit block's writer took the fast path and is still open: its
/// write is uncommitted until COMMIT, so `FOR UPDATE` waits for the block
/// rather than lock and read the version the block is replacing. It read the
/// old value at once, and the block's commit then changed the row under its
/// lock.
#[tokio::test(flavor = "multi_thread")]
async fn a_locker_waits_for_an_open_blocks_fast_path_write() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE fast_path_block (id BIGINT PRIMARY KEY, v BIGINT); INSERT INTO fast_path_block VALUES (1, 0);",
    ))
    .await
    .unwrap();

    let mut writer = sqlx::PgConnection::connect(&server.url()).await.expect("writer");
    writer.execute("BEGIN").await.unwrap();
    writer.execute("UPDATE fast_path_block SET v = v + 1 WHERE id = 1").await.unwrap();

    let mut locker = sqlx::PgConnection::connect(&server.url()).await.expect("locker");
    // Not waiting: refused while the block is open.
    locker.execute("BEGIN").await.unwrap();
    let e = sqlx::query("SELECT v FROM fast_path_block WHERE id = 1 FOR UPDATE NOWAIT")
        .fetch_one(&mut locker)
        .await
        .expect_err("NOWAIT took a row an open block is writing");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("55P03"), "{e}");
    locker.execute("ROLLBACK").await.unwrap();

    locker.execute("BEGIN").await.unwrap();
    let read = tokio::spawn(async move {
        let v: Result<i64, sqlx::Error> =
            sqlx::query_scalar("SELECT v FROM fast_path_block WHERE id = 1 FOR UPDATE").fetch_one(&mut locker).await;
        (locker, v)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!read.is_finished(), "FOR UPDATE did not wait for the open block's write");
    writer.execute("COMMIT").await.unwrap();

    let (mut locker, v) = read.await.unwrap();
    let v = v.expect("the locker's FOR UPDATE");
    assert_eq!(v, 1, "FOR UPDATE read the version the block replaced");
    sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE fast_path_block SET v = {} WHERE id = 1", v + 10)))
        .execute(&mut locker)
        .await
        .unwrap();
    locker.execute("COMMIT").await.unwrap();
    let v: i64 = sqlx::query_scalar("SELECT v FROM fast_path_block WHERE id = 1").fetch_one(&pool).await.unwrap();
    assert_eq!(v, 11, "an update was lost");
}
