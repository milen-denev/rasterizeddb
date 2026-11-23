//! A replicated write carries the values of its non-deterministic calls.
//!
//! Replication ships the statement text and each peer runs it again, so
//! `now()`, `random()` and `gen_random_uuid()` were evaluated a second time
//! there: measured on two nodes, every such value differed between them,
//! `now()` included (by the milliseconds between the runs). Over the wire,
//! on a node that replicates, they are now taken once and shipped as values,
//! and a per-row call no single value can stand for is refused.

use rclite::Arc;
use std::time::Duration;

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::core::database::Database;
use rasterizeddb_core::replication::config::{PeerConfig, ReplicationConfig};
use tokio_postgres::{Client, SimpleQueryMessage};

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    client
}

fn listener() -> (std::net::TcpListener, u16) {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    (l, port)
}

fn replicate(database: &Arc<Database>, node_id: u64, bound: (std::net::TcpListener, u16), peer_port: u16) {
    let (socket, port) = bound;
    let mut config = ReplicationConfig {
        node_id,
        listen_port: port,
        peers: vec![PeerConfig::new("other", format!("127.0.0.1:{peer_port}"))],
        ..Default::default()
    };
    config.listen_addr = "127.0.0.1".to_string();
    config.batch_linger = Duration::from_micros(200);
    config.reconnect_base = Duration::from_millis(25);
    config.reconnect_max = Duration::from_millis(200);
    database.replication.set_config(config).expect("config");
    database.replication.serve_on(socket);
    Database::start_replication(database.clone()).expect("replication starts");
}

async fn rows(c: &Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"))
            }
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_nodes_hold_the_same_values_for_now_random_and_uuids() {
    let (one, two) = (TestServer::start().await.expect("one"), TestServer::start().await.expect("two"));
    let (bound_one, bound_two) = (listener(), listener());
    let (port_one, port_two) = (bound_one.1, bound_two.1);
    replicate(one.database(), 1, bound_one, port_two);
    replicate(two.database(), 2, bound_two, port_one);
    let (a, b) = (client(&one).await, client(&two).await);

    a.batch_execute("CREATE TABLE nd (id INT, t TIMESTAMPTZ, d DATE, r FLOAT8, u UUID, note TEXT)").await.expect("create");
    a.batch_execute(
        "INSERT INTO nd (id, t, d, r, u, note) VALUES \
         (1, now(), CURRENT_DATE, random(), gen_random_uuid(), 'one'), \
         (2, CURRENT_TIMESTAMP, CURRENT_DATE, random(), gen_random_uuid(), 'two')",
    )
    .await
    .expect("insert");
    a.batch_execute("UPDATE nd SET t = now(), note = 'stamped' WHERE id = 2").await.expect("update with now()");

    // A per-row call has no one value to ship: refused, not left to diverge.
    for sql in ["UPDATE nd SET r = random()", "DELETE FROM nd WHERE random() < 0.5"] {
        let e = a.simple_query(sql).await.expect_err(sql);
        let db = e.as_db_error().unwrap_or_else(|| panic!("{sql}: {e}"));
        assert_eq!(db.code().code(), "0A000", "{sql}: {}", db.message());
        assert!(db.message().contains("on every node replicating it"), "{}", db.message());
    }

    let query = "SELECT id, t, d, r, u, note FROM nd ORDER BY id";
    let on_one = rows(&a, query).await;
    let mut on_two = Vec::new();
    for _ in 0..200 {
        on_two = rows(&b, query).await;
        if on_two.len() == 2 && on_two[1].ends_with("stamped") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(on_one.len(), 2, "{on_one:?}");
    assert_eq!(on_one, on_two, "the two nodes must hold the same values");
    let first: Vec<&str> = on_one[0].split('|').collect();
    let second: Vec<&str> = on_one[1].split('|').collect();
    assert_ne!(first[4], second[4], "each VALUES slot took its own uuid");
    assert_ne!(first[3], second[3], "and its own random()");
}

/// A server that does not replicate runs every statement as written.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_does_not_replicate_is_unchanged() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute("CREATE TABLE nd (id INT, r FLOAT8); INSERT INTO nd VALUES (1, 0), (2, 0); UPDATE nd SET r = random()")
        .await
        .expect("a per-row random() is fine without replication");
    let distinct = rows(&c, "SELECT count(DISTINCT r) FROM nd").await;
    assert_eq!(distinct, ["2"], "random() still answers anew for each row");
}
