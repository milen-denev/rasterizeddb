//! Crossed lock waits end in about a second, with one party failed.
//!
//! Every wait — `FOR UPDATE`, `LOCK TABLE`, a foreign key's key-share lock —
//! used to poll for its full 10 s ceiling and then fail, so in a cycle every
//! party waited 10 s and every party failed. Now a wait that has lasted 1 s
//! (PostgreSQL's `deadlock_timeout`) looks for a cycle in the wait-for graph;
//! exactly one participant fails with `40P01 deadlock detected` and, once its
//! client rolls back, the rest get their locks.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor};
use std::time::{Duration, Instant};

async fn connect(server: &TestServer) -> sqlx::PgConnection {
    sqlx::PgConnection::connect(&server.url())
        .await
        .expect("connect")
}

async fn exec(conn: &mut sqlx::PgConnection, sql: &str) -> Result<(), sqlx::Error> {
    conn.execute(AssertSqlSafe(sql.to_string())).await.map(|_| ())
}

fn code(e: &sqlx::Error) -> Option<String> {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string())
}

async fn seeded(server: &TestServer, rows: i32) {
    let mut c = connect(server).await;
    exec(&mut c, "CREATE TABLE jobs (id INT PRIMARY KEY, state TEXT NOT NULL)")
        .await
        .expect("create");
    for i in 1..=rows {
        exec(&mut c, &format!("INSERT INTO jobs (id, state) VALUES ({}, 'x')", i))
            .await
            .expect("insert");
    }
}

/// Runs a cycle of `n` sessions: session i holds row i and then asks for row
/// i+1 (the last asks for row 1). Returns each session's outcome of the
/// second request, and how long the slowest took.
async fn cycle(server: &TestServer, n: i32) -> (Vec<Result<(), sqlx::Error>>, Duration) {
    let mut sessions = Vec::new();
    for i in 1..=n {
        let mut c = connect(server).await;
        exec(&mut c, "BEGIN").await.expect("begin");
        exec(&mut c, &format!("SELECT id FROM jobs WHERE id = {} FOR UPDATE", i))
            .await
            .expect("first lock");
        sessions.push(c);
    }
    let start = Instant::now();
    let mut handles = Vec::new();
    for (i, mut c) in sessions.into_iter().enumerate() {
        let want = (i as i32 + 1) % n + 1;
        handles.push(tokio::spawn(async move {
            let outcome = exec(
                &mut c,
                &format!("SELECT id FROM jobs WHERE id = {} FOR UPDATE", want),
            )
            .await;
            let elapsed = start.elapsed();
            // The victim's client rolls back, which is what frees the others;
            // the rest commit.
            let _ = exec(&mut c, if outcome.is_err() { "ROLLBACK" } else { "COMMIT" }).await;
            (outcome, elapsed)
        }));
    }
    let mut outcomes = Vec::new();
    let mut slowest = Duration::ZERO;
    for h in handles {
        let (outcome, elapsed) = h.await.expect("join");
        slowest = slowest.max(elapsed);
        outcomes.push(outcome);
    }
    (outcomes, slowest)
}

fn assert_one_victim(outcomes: &[Result<(), sqlx::Error>], slowest: Duration, what: &str) {
    let failed: Vec<&sqlx::Error> = outcomes.iter().filter_map(|o| o.as_ref().err()).collect();
    assert_eq!(
        failed.len(),
        1,
        "{}: exactly one party to the cycle fails; got {:?}",
        what,
        outcomes
    );
    assert_eq!(code(failed[0]).as_deref(), Some("40P01"), "{}: {}", what, failed[0]);
    assert!(failed[0].to_string().contains("deadlock detected"), "{}", failed[0]);
    assert!(
        slowest < Duration::from_secs(5),
        "{}: the cycle took {:?} to resolve; the 10 s lock timeout was what ended it",
        what,
        slowest
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_two_session_cycle_fails_one_in_about_a_second() {
    let server = TestServer::start().await.expect("server");
    seeded(&server, 2).await;
    let (outcomes, slowest) = cycle(&server, 2).await;
    assert_one_victim(&outcomes, slowest, "two sessions");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_three_session_cycle_fails_one_in_about_a_second() {
    let server = TestServer::start().await.expect("server");
    seeded(&server, 3).await;
    let (outcomes, slowest) = cycle(&server, 3).await;
    assert_one_victim(&outcomes, slowest, "three sessions");
}

/// A wait with no cycle is not a deadlock, however long it lasts.
#[tokio::test(flavor = "multi_thread")]
async fn a_plain_wait_past_a_second_is_not_a_deadlock() {
    let server = TestServer::start().await.expect("server");
    seeded(&server, 1).await;
    let mut holder = connect(&server).await;
    exec(&mut holder, "BEGIN").await.expect("begin");
    exec(&mut holder, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE").await.expect("lock");

    let mut waiter = connect(&server).await;
    exec(&mut waiter, "BEGIN").await.expect("begin");
    let handle = tokio::spawn(async move {
        let r = exec(&mut waiter, "SELECT id FROM jobs WHERE id = 1 FOR UPDATE").await;
        let _ = exec(&mut waiter, "COMMIT").await;
        r
    });
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    exec(&mut holder, "COMMIT").await.expect("commit");
    handle.await.expect("join").expect("the waiter gets the lock, not a deadlock error");
}

/// The cycle can run through a foreign key's key-share lock and a LOCK TABLE.
#[tokio::test(flavor = "multi_thread")]
async fn a_cycle_through_a_key_share_lock_and_a_table_lock_is_detected() {
    let server = TestServer::start().await.expect("server");
    let mut setup = connect(&server).await;
    exec(&mut setup, "CREATE TABLE p (id INT PRIMARY KEY)").await.expect("p");
    exec(
        &mut setup,
        "CREATE TABLE c (id INT PRIMARY KEY, pid INT, \
         CONSTRAINT fk_c FOREIGN KEY (pid) REFERENCES p (id))",
    )
    .await
    .expect("c");
    exec(&mut setup, "CREATE TABLE other (id INT PRIMARY KEY)").await.expect("other");
    exec(&mut setup, "INSERT INTO p (id) VALUES (1)").await.expect("parent");

    // A holds a key-share lock on parent 1; B holds `other` exclusively.
    let mut a = connect(&server).await;
    exec(&mut a, "BEGIN").await.expect("begin");
    exec(&mut a, "INSERT INTO c (id, pid) VALUES (10, 1)").await.expect("child");
    let mut b = connect(&server).await;
    exec(&mut b, "BEGIN").await.expect("begin");
    exec(&mut b, "LOCK TABLE other IN ACCESS EXCLUSIVE MODE").await.expect("lock table");

    let start = Instant::now();
    // B wants to delete parent 1 (waits for A's key share) ...
    let hb = tokio::spawn(async move {
        let r = exec(&mut b, "DELETE FROM p WHERE id = 1").await;
        let _ = exec(&mut b, "ROLLBACK").await;
        (r, start.elapsed())
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    // ... while A wants `other` (waits for B).
    let ha = tokio::spawn(async move {
        let r = exec(&mut a, "LOCK TABLE other IN ACCESS EXCLUSIVE MODE").await;
        let _ = exec(&mut a, if r.is_err() { "ROLLBACK" } else { "COMMIT" }).await;
        (r, start.elapsed())
    });
    let (ra, ta) = ha.await.expect("join");
    let (rb, tb) = hb.await.expect("join");
    let outcomes = vec![ra, rb];
    assert_one_victim(&outcomes, ta.max(tb), "key share + table lock");
}
