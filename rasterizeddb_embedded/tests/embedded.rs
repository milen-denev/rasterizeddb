//! The embedded engine end to end: start, query through the client SDK,
//! stop, and reopen a directory to find what was written.

use rasterizeddb_embedded::Builder;
use rasterizeddb_embedded::client::Executor;

fn builder(b: Builder) -> Builder {
    // Off unix the endpoint is a loopback port: these tests use a password,
    // which also exercises the SCRAM login.
    if cfg!(unix) { b } else { b.password("app", "embedded-test-password") }
}

async fn count(client: &rasterizeddb_embedded::Client, sql: &str) -> i64 {
    let rows = client.query(sql, &[]).await.unwrap();
    assert_eq!(rows.len(), 1, "{sql}");
    rows.iter().next().unwrap().get::<i64>(0).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_memory_round_trip() {
    let db = builder(Builder::in_memory()).worker_threads(2).start().await.unwrap();
    let client = db.connect().await.unwrap();
    client.execute("CREATE TABLE notes (id BIGINT, body TEXT)", &[]).await.unwrap();
    for i in 0..10i64 {
        client
            .execute(&format!("INSERT INTO notes VALUES ({i}, 'note {i}')"), &[])
            .await
            .unwrap();
    }
    assert_eq!(count(&client, "SELECT COUNT(*) FROM notes").await, 10);
    let rows = client.query("SELECT body FROM notes WHERE id = 7", &[]).await.unwrap();
    assert_eq!(rows.iter().next().unwrap().get::<String>(0).unwrap(), "note 7");
    drop(client);
    db.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_directory_survives_shutdown_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = builder(Builder::open(dir.path())).worker_threads(2).start().await.unwrap();
        let client = db.connect().await.unwrap();
        client.execute("CREATE TABLE kept (id BIGINT, v TEXT)", &[]).await.unwrap();
        client.execute("INSERT INTO kept VALUES (1, 'a'), (2, 'b'), (3, 'c')", &[]).await.unwrap();
        drop(client);
        db.shutdown().await.unwrap();
    }
    {
        let db = builder(Builder::open(dir.path())).worker_threads(2).start().await.unwrap();
        let client = db.connect().await.unwrap();
        assert_eq!(count(&client, "SELECT COUNT(*) FROM kept").await, 3);
        assert_eq!(count(&client, "SELECT SUM(id) FROM kept").await, 6);
        drop(client);
        // Stopped by the drop this time.
        drop(db);
    }
    let db = builder(Builder::open(dir.path())).worker_threads(2).start().await.unwrap();
    let client = db.connect().await.unwrap();
    assert_eq!(count(&client, "SELECT COUNT(*) FROM kept").await, 3);
    drop(client);
    db.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_rolls_back() {
    let db = builder(Builder::in_memory()).worker_threads(2).start().await.unwrap();
    let client = db.connect().await.unwrap();
    client.execute("CREATE TABLE t (id BIGINT)", &[]).await.unwrap();
    client.execute("INSERT INTO t VALUES (1)", &[]).await.unwrap();
    let tx = client.begin().await.unwrap();
    tx.execute("INSERT INTO t VALUES (2)", &[]).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(count(&client, "SELECT COUNT(*) FROM t").await, 1);
    drop(client);
    db.shutdown().await.unwrap();
}

#[test]
fn starts_and_stops_without_a_runtime() {
    let db = builder(Builder::in_memory()).worker_threads(1).start_blocking().unwrap();
    assert!(!db.endpoint().is_empty());
    db.shutdown_blocking().unwrap();
}

#[cfg(not(unix))]
#[test]
fn loopback_without_a_password_is_refused() {
    let err = Builder::in_memory().start_blocking().err().expect("refused");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}
