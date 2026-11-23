//! A writer that found no row locked, and a `FOR UPDATE` that lands before
//! the writer's write.
//!
//! `UPDATE` and `DELETE` honour row locks by asking for each row's lock
//! before writing — unless no row in the database is locked, the fast path,
//! which skips the scan. A writer W that took the fast path and a locker L
//! that locked and read the row before W wrote it both went on: L held a lock
//! on a row W then changed and committed, and L's own update, computed from
//! what it had read, overwrote W's. A lost update.
//!
//! Each row is now marked as being written just before it is stamped, under
//! the row-lock mutex: W meets L's lock there (refused, or replayed once L
//! ends when it had applied nothing), or L meets W's mark and waits for W's
//! owner to end. Row by row, so a lock on another row — a `SKIP LOCKED`
//! queue claim above all — is never held up by W.
//!
//! The race is made deterministic by parking W at the
//! `test.row_write_fast_path` failpoint, right after its check.

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

    let (mut locker, v) = read.await.unwrap();
    let v = v.expect("the locker's FOR UPDATE");
    sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE fast_path_race SET v = {} WHERE id = 1", v + 10)))
        .execute(&mut locker)
        .await
        .expect("the locker's update");
    locker.execute("COMMIT").await.unwrap();
    let written = writer.await.unwrap();

    // Either the writer met the lock as it marked the row and was refused
    // (55P03, the locker having read the row before any write), or it
    // wrote and the locker read its write: never both writes with one lost.
    let final_v: i64 = sqlx::query_scalar("SELECT v FROM fast_path_race WHERE id = 1").fetch_one(&pool).await.unwrap();
    match written {
        Ok(_) => assert_eq!(final_v, 11, "an update was lost (the locker read {v})"),
        Err(e) => {
            assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("55P03"), "{e}");
            assert_eq!((v, final_v), (0, 10));
        }
    }
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

async fn queue(server: &TestServer) -> sqlx::PgPool {
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE q (id BIGINT PRIMARY KEY, status TEXT, v BIGINT);
         INSERT INTO q VALUES (1, 'pending', 0), (2, 'pending', 0), (3, 'pending', 0), (100, 'other', 0);",
    ))
    .await
    .unwrap();
    pool
}

const CLAIM: &str = "UPDATE q SET status = 'running' WHERE id = \
    (SELECT id FROM q WHERE status = 'pending' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING id";

async fn in_time(sql: &'static str, l: &mut sqlx::PgConnection) -> Result<Vec<i64>, sqlx::Error> {
    tokio::time::timeout(Duration::from_secs(3), sqlx::query_scalar::<_, i64>(sql).fetch_all(l)).await.expect(sql)
}

fn code(e: &sqlx::Error) -> Option<String> {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string())
}

/// The job-queue claim. A table-wide mark taken by the claim's outer check,
/// before its subquery locked anything, made every overlapping claim skip
/// every row: `[]` with rows pending.
#[tokio::test(flavor = "multi_thread")]
async fn a_queue_claim_gets_a_row_while_another_claim_is_open() {
    let server = TestServer::start().await.expect("server");
    let pool = queue(&server).await;
    let mut a = sqlx::PgConnection::connect(&server.url()).await.unwrap();
    a.execute("BEGIN").await.unwrap();
    let first: Vec<i64> = sqlx::query_scalar(CLAIM).fetch_all(&mut a).await.unwrap();
    assert_eq!(first, [1]);
    // A second claim, while the first is open: the next row, at once.
    let mut b = sqlx::PgConnection::connect(&server.url()).await.unwrap();
    b.execute("BEGIN").await.unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), sqlx::query_scalar::<_, i64>(CLAIM).fetch_all(&mut b))
        .await
        .expect("the second claim waited")
        .unwrap();
    assert_eq!(second, [2]);
    // And an autocommit claim beside both.
    let third: Vec<i64> = sqlx::query_scalar(CLAIM).fetch_all(&pool).await.unwrap();
    assert_eq!(third, [3]);
    a.execute("COMMIT").await.unwrap();
    b.execute("COMMIT").await.unwrap();
}

/// A claim while a plain writer of another row is parked between its check
/// and its write: the writer marks only the rows it stamps.
#[tokio::test(flavor = "multi_thread")]
async fn a_queue_claim_is_not_held_up_by_a_writer_of_another_row() {
    let server = TestServer::start().await.expect("server");
    let pool = queue(&server).await;
    // The table name scopes the failpoint to this test's writer.
    pool.execute("CREATE TABLE fast_path_other (id BIGINT PRIMARY KEY, v BIGINT); INSERT INTO fast_path_other VALUES (1, 0);")
        .await
        .unwrap();
    let (at, at_rx) = mpsc::channel::<()>();
    let (go, go_rx) = mpsc::channel::<()>();
    let go_rx = Mutex::new(go_rx);
    let _armed = arm(
        "test.row_write_fast_path",
        std::path::Path::new("q"),
        1,
        Arc::new(move |_| {
            let _ = at.send(());
            tokio::task::block_in_place(|| {
                let _ = go_rx.lock().unwrap().recv_timeout(Duration::from_secs(10));
            });
            Outcome::Continue
        }),
    );
    let writer_pool = pool.clone();
    let writer =
        tokio::spawn(async move { sqlx::query("UPDATE q SET v = v + 1 WHERE id = 100").execute(&writer_pool).await });
    tokio::task::spawn_blocking(move || at_rx.recv_timeout(Duration::from_secs(10))).await.unwrap().expect("parked");
    let claimed = tokio::time::timeout(Duration::from_secs(3), sqlx::query_scalar::<_, i64>(CLAIM).fetch_all(&pool))
        .await
        .expect("the claim waited for a writer of another row")
        .unwrap();
    assert_eq!(claimed, [1]);
    go.send(()).unwrap();
    writer.await.unwrap().unwrap();
}

/// An open block that wrote row 100 holds up lockers of row 100 only.
#[tokio::test(flavor = "multi_thread")]
async fn an_open_blocks_write_holds_up_only_its_own_rows() {
    let server = TestServer::start().await.expect("server");
    let pool = queue(&server).await;
    let mut w = sqlx::PgConnection::connect(&server.url()).await.unwrap();
    w.execute("BEGIN").await.unwrap();
    w.execute("UPDATE q SET v = 1 WHERE id = 100").await.unwrap();

    let mut l = sqlx::PgConnection::connect(&server.url()).await.unwrap();
    l.execute("BEGIN").await.unwrap();
    assert_eq!(in_time("SELECT id FROM q WHERE id = 3 FOR UPDATE", &mut l).await.unwrap(), [3]);
    assert_eq!(in_time("SELECT id FROM q WHERE id = 2 FOR UPDATE NOWAIT", &mut l).await.unwrap(), [2]);
    assert_eq!(in_time("SELECT id FROM q ORDER BY id FOR UPDATE SKIP LOCKED", &mut l).await.unwrap(), [1, 2, 3]);
    let e = in_time("SELECT id FROM q WHERE id = 100 FOR UPDATE NOWAIT", &mut l).await.expect_err("row 100 is being written");
    assert_eq!(code(&e).as_deref(), Some("55P03"), "{e}");
    l.execute("ROLLBACK").await.unwrap();

    // Row 100 itself waits for the block, then reads what it committed.
    l.execute("BEGIN").await.unwrap();
    let read = tokio::spawn(async move {
        let v: Result<i64, sqlx::Error> = sqlx::query_scalar("SELECT v FROM q WHERE id = 100 FOR UPDATE").fetch_one(&mut l).await;
        (l, v)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!read.is_finished(), "FOR UPDATE of row 100 did not wait for the block writing it");
    w.execute("COMMIT").await.unwrap();
    let (mut l, v) = read.await.unwrap();
    assert_eq!(v.unwrap(), 1);
    l.execute("COMMIT").await.unwrap();
    let _ = pool;
}

/// W writes row 1, L locks row 2, W locks row 2 and waits for L. A table
/// mark made L wait on W too, and one of them was failed as a deadlock
/// where PostgreSQL completes both.
#[tokio::test(flavor = "multi_thread")]
async fn locking_another_row_than_a_writer_wrote_is_no_deadlock() {
    let server = TestServer::start().await.expect("server");
    let _pool = queue(&server).await;
    let mut w = sqlx::PgConnection::connect(&server.url()).await.unwrap();
    let mut l = sqlx::PgConnection::connect(&server.url()).await.unwrap();
    w.execute("BEGIN").await.unwrap();
    w.execute("UPDATE q SET v = 1 WHERE id = 1").await.unwrap();
    l.execute("BEGIN").await.unwrap();
    let got: Vec<i64> = tokio::time::timeout(
        Duration::from_secs(3),
        sqlx::query_scalar("SELECT id FROM q WHERE id = 2 FOR UPDATE").fetch_all(&mut l),
    )
    .await
    .expect("L waited for W's write of another row")
    .unwrap();
    assert_eq!(got, [2]);
    let waiting = tokio::spawn(async move {
        let r: Result<Vec<i64>, sqlx::Error> =
            sqlx::query_scalar("SELECT id FROM q WHERE id = 2 FOR UPDATE").fetch_all(&mut w).await;
        (w, r)
    });
    tokio::time::sleep(Duration::from_millis(1500)).await;
    l.execute("COMMIT").await.expect("L was failed as a deadlock");
    let (mut w, r) = waiting.await.unwrap();
    assert_eq!(r.expect("W was failed"), [2]);
    w.execute("COMMIT").await.unwrap();
}
