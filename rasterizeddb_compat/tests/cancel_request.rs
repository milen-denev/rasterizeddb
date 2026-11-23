//! CancelRequest: a second connection carrying the (pid, key) a session was
//! given in BackendKeyData stops that session's running statement with 57014.
//! It used to be logged and dropped, and the key sent was never stored.

use rasterizeddb_compat::TestServer;
use std::time::Duration;

async fn tp(server: &TestServer) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connects");
    tokio::spawn(conn);
    client
}

/// Bounded so the test cannot fill a shared /tmp.
const ROWS: i32 = 20_000;

async fn seed(client: &tokio_postgres::Client) {
    client
        .batch_execute("CREATE TABLE cr (id INT PRIMARY KEY, n INT NOT NULL)")
        .await
        .expect("create");
    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(1_000) {
        let values = chunk
            .iter()
            .map(|i| format!("({i}, {i})"))
            .collect::<Vec<_>>()
            .join(",");
        client
            .batch_execute(&format!("INSERT INTO cr (id, n) VALUES {values}"))
            .await
            .expect("insert");
    }
}

/// Every outer row is compared against every inner value: a statement that
/// runs for a long time while scanning, which is where cancellation is
/// checked.
const SLOW: &str = "SELECT COUNT(*) FROM cr WHERE n NOT IN (SELECT n FROM cr)";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_request_stops_the_running_statement() {
    let server = TestServer::start().await.expect("server");
    let client = tp(&server).await;
    seed(&client).await;
    let token = client.cancel_token();

    let running = tokio::spawn(async move {
        let outcome = client.query_one(SLOW, &[]).await;
        (client, outcome)
    });
    // Keep asking until the statement ends; one landing before it starts
    // finds nothing running and does nothing.
    let mut sent = 0;
    while !running.is_finished() && sent < 400 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        token
            .cancel_query(tokio_postgres::NoTls)
            .await
            .expect("cancel request sent");
        sent += 1;
    }
    let (client, outcome) = tokio::time::timeout(Duration::from_secs(60), running)
        .await
        .expect("the statement ends")
        .expect("task");
    let err = outcome.expect_err("a cancelled statement fails rather than answering");
    let db = err.as_db_error().expect("a server error");
    assert_eq!(db.code().code(), "57014", "{db:?}");
    assert!(
        db.message().contains("user request"),
        "named as a user cancel: {}",
        db.message()
    );

    // The session survives, and the next statement is not cancelled by any
    // request still in flight from above.
    let n: i64 = client
        .query_one("SELECT COUNT(*) FROM cr", &[])
        .await
        .expect("the next statement runs")
        .get(0);
    assert_eq!(n, i64::from(ROWS));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_after_the_statement_finished_changes_nothing() {
    let server = TestServer::start().await.expect("server");
    let client = tp(&server).await;
    seed(&client).await;
    let token = client.cancel_token();

    let n: i64 = client.query_one("SELECT COUNT(*) FROM cr", &[]).await.unwrap().get(0);
    assert_eq!(n, i64::from(ROWS));
    // Nothing is running: the request finds no statement.
    token.cancel_query(tokio_postgres::NoTls).await.expect("sent");
    tokio::time::sleep(Duration::from_millis(50)).await;
    for _ in 0..3 {
        let n: i64 = client
            .query_one("SELECT COUNT(*) FROM cr WHERE n >= 0", &[])
            .await
            .expect("a later statement is not cancelled")
            .get(0);
        assert_eq!(n, i64::from(ROWS));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_sessions_key_cancels_nothing() {
    let server = TestServer::start().await.expect("server");
    let a = tp(&server).await;
    let b = tp(&server).await;
    seed(&a).await;
    // B's token stops B's statements, never A's.
    let b_token = b.cancel_token();
    let running = tokio::spawn(async move { a.query_one("SELECT COUNT(*) FROM cr", &[]).await });
    for _ in 0..5 {
        b_token.cancel_query(tokio_postgres::NoTls).await.expect("sent");
    }
    let row = running.await.expect("task").expect("A was not cancelled by B's key");
    assert_eq!(row.get::<_, i64>(0), i64::from(ROWS));
}
