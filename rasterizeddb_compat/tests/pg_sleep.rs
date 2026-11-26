//! `pg_sleep(seconds)`, `pg_sleep_for(interval)`, `pg_sleep_until(timestamptz)`
//! as PostgreSQL answers them: one row whose value is empty (`void`), after
//! the wait. The wait is async (other sessions run meanwhile, on a server
//! with one runtime worker) and ends early, with 57014, on `statement_timeout`
//! or a cancel request. They were "function pg_sleep(1 arguments) does not
//! exist".

use std::time::{Duration, Instant};

use rasterizeddb_compat::TestServer;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

async fn connect(server: &TestServer) -> Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), NoTls).await.expect("connect");
    tokio::spawn(conn);
    c
}

async fn values(c: &Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap_or("NULL").to_string()),
            _ => None,
        })
        .collect()
}

async fn code(c: &Client, sql: &str) -> String {
    let e = c.simple_query(sql).await.expect_err(sql);
    let message = e.as_db_error().map(|d| d.message().to_string()).unwrap_or_default();
    let code = e.code().map(|s| s.code().to_string()).unwrap_or_else(|| panic!("{sql}: {e}"));
    eprintln!("{sql}: {code} {message}");
    code
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn the_three_sleeps_wait_and_answer_one_empty_row() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    for sql in [
        "SELECT pg_sleep(0.3)",
        "SELECT pg_sleep_for('300 milliseconds')",
        "SELECT pg_sleep_for(interval '0.3 seconds')",
        "SELECT pg_sleep_until(now() + interval '300 milliseconds')",
    ] {
        let at = Instant::now();
        assert_eq!(values(&c, sql).await, [""], "{sql}");
        let took = at.elapsed();
        assert!(took >= Duration::from_millis(280), "{sql} answered after {took:?}");
        assert!(took < Duration::from_secs(5), "{sql} answered after {took:?}");
    }
    // Nothing to wait for: a zero, a negative, a NULL, an instant gone by,
    // and no row at all.
    for sql in [
        "SELECT pg_sleep(0)",
        "SELECT pg_sleep(-5)",
        "SELECT pg_sleep(NULL)",
        "SELECT pg_sleep_until('2000-01-01 00:00:00+00')",
        "SELECT pg_sleep(30) WHERE false",
    ] {
        let at = Instant::now();
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert!(at.elapsed() < Duration::from_secs(2), "{sql} waited {:?}", at.elapsed());
    }
    // Over the extended protocol, with the seconds as a parameter.
    let at = Instant::now();
    let rows = c.query("SELECT pg_sleep($1)", &[&0.3f64]).await.expect("prepared pg_sleep");
    assert_eq!(rows.len(), 1);
    assert!(at.elapsed() >= Duration::from_millis(280), "the prepared sleep took {:?}", at.elapsed());
}

/// The wait holds no runtime worker: with one, another session's statements
/// are answered while a sleep runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn a_sleep_does_not_hold_up_another_session() {
    let server = TestServer::start().await.expect("server");
    let sleeper = connect(&server).await;
    let other = connect(&server).await;
    let sleeping = tokio::spawn(async move { sleeper.simple_query("SELECT pg_sleep(2)").await.map(|_| ()) });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let at = Instant::now();
    for _ in 0..5 {
        assert_eq!(values(&other, "SELECT 1").await, ["1"]);
    }
    assert!(at.elapsed() < Duration::from_millis(1500), "another session waited {:?} behind a sleep", at.elapsed());
    assert!(!sleeping.is_finished(), "the sleep ended early");
    sleeping.await.unwrap().expect("the sleep");
}

#[tokio::test(flavor = "multi_thread")]
async fn statement_timeout_ends_a_sleep() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.simple_query("SET statement_timeout = 200").await.expect("set");
    for sql in ["SELECT pg_sleep(30)", "SELECT pg_sleep_for('30 seconds')", "SELECT pg_sleep_until(now() + interval '30 seconds')"] {
        let at = Instant::now();
        assert_eq!(code(&c, sql).await, "57014", "{sql}");
        assert!(at.elapsed() < Duration::from_secs(5), "{sql} ran {:?} past a 200 ms timeout", at.elapsed());
    }
    // The session goes on.
    c.simple_query("SET statement_timeout = 0").await.expect("reset");
    assert_eq!(values(&c, "SELECT pg_sleep(0.05)").await, [""]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cancel_request_ends_a_sleep() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    let token = c.cancel_token();
    let canceller = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        token.cancel_query(NoTls).await
    });
    let at = Instant::now();
    assert_eq!(code(&c, "SELECT pg_sleep(30)").await, "57014");
    assert!(at.elapsed() < Duration::from_secs(5), "the cancelled sleep ran {:?}", at.elapsed());
    canceller.await.unwrap().expect("cancel request");
    assert_eq!(values(&c, "SELECT 1").await, ["1"]);
}

/// Anywhere but an item of a SELECT with no FROM it would have to block a
/// worker per row, so it is refused (0A000) rather than answered unwaited.
#[tokio::test(flavor = "multi_thread")]
async fn a_sleep_over_rows_is_refused_not_skipped() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute("CREATE TABLE sl (id INT PRIMARY KEY); INSERT INTO sl VALUES (1), (2)").await.expect("table");
    assert_eq!(code(&c, "SELECT pg_sleep(0.1) FROM sl").await, "0A000");
    assert_eq!(code(&c, "SELECT id FROM sl WHERE pg_sleep(0.1) IS NULL").await, "0A000");
}
