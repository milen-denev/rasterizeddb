//! `SET partition_trace = on` (docs/partitioning.md §2): a NOTICE per
//! statement on a partitioned table, naming how many partitions it routed to
//! out of how many, and why. The design documented it; the engine
//! acknowledged the `SET` and dropped it, so the one tool for telling a routed
//! query from a fan-out said nothing.

use std::sync::{Arc, Mutex};

use rasterizeddb_compat::TestServer;
use tokio_postgres::Client;

struct OnDisk(std::path::PathBuf);

impl Drop for OnDisk {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

type Notices = Arc<Mutex<Vec<(String, String, String)>>>;

/// A client whose notices are collected: (severity, SQLSTATE, message).
async fn client_with_notices(server: &TestServer) -> (Client, Notices) {
    let (client, mut connection) =
        tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    let notices: Notices = Arc::default();
    let sink = notices.clone();
    tokio::spawn(async move {
        while let Some(message) = std::future::poll_fn(|cx| connection.poll_message(cx)).await {
            if let Ok(tokio_postgres::AsyncMessage::Notice(n)) = message {
                sink.lock().unwrap().push((n.severity().to_string(), n.code().code().to_string(), n.message().to_string()));
            }
        }
    });
    (client, notices)
}

/// The trace lines a statement raised, by message.
async fn traced(c: &Client, notices: &Notices, sql: &str) -> Vec<String> {
    notices.lock().unwrap().clear();
    c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let taken = std::mem::take(&mut *notices.lock().unwrap());
    for (severity, code, message) in &taken {
        if message.starts_with("partition_trace: ") {
            assert_eq!((severity.as_str(), code.as_str()), ("NOTICE", "00000"), "{message}");
        }
    }
    taken.into_iter().map(|(_, _, m)| m).filter(|m| m.starts_with("partition_trace: ")).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn partition_trace_says_where_each_statement_looked() {
    let root = std::env::temp_dir().join(format!("partition_trace_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let _dir = OnDisk(root.clone());
    let server = TestServer::start_file_backed(&root.to_string_lossy()).await.expect("server");
    let (c, notices) = client_with_notices(&server).await;
    c.simple_query("CREATE TABLE p (tenant_id BIGINT NOT NULL, v INT) PARTITION BY HASH (tenant_id) PARTITIONS 4")
        .await
        .unwrap();
    c.simple_query("CREATE TABLE plain (v INT)").await.unwrap();

    // Off by default: nothing said.
    assert!(traced(&c, &notices, "SELECT * FROM p WHERE tenant_id = 1").await.is_empty());

    c.simple_query("SET partition_trace = on").await.unwrap();
    let shown = c.simple_query("SHOW partition_trace").await.unwrap();
    let value = shown.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
        _ => None,
    });
    assert_eq!(value.as_deref(), Some("on"));

    // One line per statement for the rows an INSERT routed, not one per row.
    let insert = traced(&c, &notices, "INSERT INTO p VALUES (1, 10), (1, 11), (2, 20), (2, 21), (3, 30), (3, 31)").await;
    assert_eq!(insert.len(), 1, "{insert:?}");
    assert!(insert[0].starts_with("partition_trace: \"p\": INSERT of 6 rows into "), "{insert:?}");
    assert!(insert[0].ends_with(" of 4 partitions (routed by tenant_id)"), "{insert:?}");

    let cases: &[(&str, &str)] = &[
        ("SELECT * FROM p WHERE tenant_id = 1", "partition_trace: \"p\": read 1 of 4 partitions (routed by tenant_id)"),
        ("SELECT * FROM p WHERE v = 10", "partition_trace: \"p\": read 4 of 4 partitions (the WHERE clause does not pin tenant_id)"),
        ("SELECT * FROM p", "partition_trace: \"p\": read 4 of 4 partitions (no WHERE clause)"),
        ("UPDATE p SET v = 99 WHERE tenant_id = 2", "partition_trace: \"p\": write 1 of 4 partitions (routed by tenant_id)"),
        ("DELETE FROM p WHERE v = 31", "partition_trace: \"p\": write 4 of 4 partitions (the WHERE clause does not pin tenant_id)"),
    ];
    for (sql, want) in cases {
        let got = traced(&c, &notices, sql).await;
        assert!(got.iter().any(|m| m == want), "{sql}: {got:?}");
    }
    // An IN list routes to the partitions its values are in: at most three.
    let listed = traced(&c, &notices, "SELECT * FROM p WHERE tenant_id IN (1, 2, 3)").await;
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert!(listed[0].ends_with(" of 4 partitions (routed by tenant_id)"), "{listed:?}");

    // A prepared statement is routed too (by its bound value when the
    // statement's shape is cached).
    notices.lock().unwrap().clear();
    for _ in 0..3 {
        c.query("SELECT v FROM p WHERE tenant_id = $1", &[&1i64]).await.unwrap();
    }
    let prepared: Vec<String> = std::mem::take(&mut *notices.lock().unwrap())
        .into_iter()
        .map(|(_, _, m)| m)
        .filter(|m| m.starts_with("partition_trace: "))
        .collect();
    assert_eq!(prepared.len(), 3, "{prepared:?}");
    assert!(prepared.iter().all(|m| m.starts_with("partition_trace: \"p\": read 1 of 4 partitions (routed by ")), "{prepared:?}");

    // A plain table says nothing.
    assert!(traced(&c, &notices, "SELECT * FROM plain").await.is_empty());

    // RESET turns it off; a bad value is refused.
    c.simple_query("RESET partition_trace").await.unwrap();
    assert!(traced(&c, &notices, "SELECT * FROM p WHERE tenant_id = 1").await.is_empty());
    let e = c.simple_query("SET partition_trace = sometimes").await.expect_err("not a boolean");
    assert_eq!(e.code().map(|c| c.code()), Some("22023"), "{e}");
}
