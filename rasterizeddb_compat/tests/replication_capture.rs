//! What replication carries, statement kind by statement kind: an audit
//! found every write the engine supports reaching the peer, partitioned
//! tables and the role catalogue included, and one silent gap, a database
//! made with `CREATE DATABASE` (replication carries the default one only).
//! Statements that are each node's own now say so to the client.

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
    match c.simple_query(sql).await {
        Ok(messages) => messages
            .into_iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => {
                    Some((0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"))
                }
                _ => None,
            })
            .collect(),
        Err(e) => vec![format!("ERR {}", e.as_db_error().map(|d| d.message().to_string()).unwrap_or(e.to_string()))],
    }
}

/// A client whose server notices are collected.
async fn noticing(server: &TestServer) -> (Client, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
    let (client, mut connection) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    let notices = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = notices.clone();
    tokio::spawn(async move {
        while let Some(message) = std::future::poll_fn(|cx| connection.poll_message(cx)).await {
            if let Ok(tokio_postgres::AsyncMessage::Notice(n)) = message {
                sink.lock().unwrap().push(n.message().to_string());
            }
        }
    });
    (client, notices)
}

/// One statement of each kind the engine supports, run on node one of a
/// replicating pair, and the answer node two gives afterwards. Every write
/// reaches the peer, partitioned tables and the role catalogue included; a
/// statement that is each node's own (`VACUUM`, `CREATE DATABASE`) stays
/// local and says so. Pinned so a statement kind whose capture changes, or
/// one added without a decision (`executor::replication_of`), is noticed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_kind_of_write_reaches_the_peer_and_local_ones_say_so() {
    let root = std::env::temp_dir().join(format!("replication_capture_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let (one, two) = (
        TestServer::start_file_backed(&root.join("one").to_string_lossy()).await.expect("one"),
        TestServer::start_file_backed(&root.join("two").to_string_lossy()).await.expect("two"),
    );
    let (bound_one, bound_two) = (listener(), listener());
    let (port_one, port_two) = (bound_one.1, bound_two.1);
    replicate(one.database(), 1, bound_one, port_two);
    replicate(two.database(), 2, bound_two, port_one);
    let ((a, notices), b) = (noticing(&one).await, client(&two).await);

    // (kind, statements on node one, a query whose answer on node two shows it)
    let replicated: Vec<(&str, &str, &str)> = vec![
        ("CREATE TABLE", "CREATE TABLE m1 (id INT PRIMARY KEY, v TEXT)", "SELECT count(*) FROM m1"),
        ("INSERT", "INSERT INTO m1 VALUES (1, 'a'), (2, 'b')", "SELECT count(*) FROM m1"),
        ("UPDATE", "UPDATE m1 SET v = 'u' WHERE id = 1", "SELECT v FROM m1 WHERE id = 1"),
        ("DELETE", "DELETE FROM m1 WHERE id = 2", "SELECT count(*) FROM m1"),
        ("UPSERT", "INSERT INTO m1 VALUES (1, 'up') ON CONFLICT (id) DO UPDATE SET v = excluded.v", "SELECT v FROM m1 WHERE id = 1"),
        ("ADD COLUMN", "ALTER TABLE m1 ADD COLUMN w INT DEFAULT 7", "SELECT w FROM m1 WHERE id = 1"),
        ("RENAME COLUMN", "ALTER TABLE m1 RENAME COLUMN w TO w2", "SELECT w2 FROM m1 WHERE id = 1"),
        ("DROP COLUMN", "ALTER TABLE m1 DROP COLUMN w2", "SELECT count(*) FROM information_schema.columns WHERE table_name = 'm1'"),
        ("CREATE INDEX", "CREATE INDEX m1_v ON m1 (v)", "SELECT count(*) FROM pg_indexes WHERE indexname = 'm1_v'"),
        ("DROP INDEX", "DROP INDEX m1_v", "SELECT count(*) FROM pg_indexes WHERE indexname = 'm1_v'"),
        ("CREATE TABLE AS", "CREATE TABLE m2 AS SELECT id, v FROM m1", "SELECT count(*) FROM m2"),
        ("UPDATE FROM", "UPDATE m1 SET v = m2.v || '!' FROM m2 WHERE m1.id = m2.id", "SELECT v FROM m1 WHERE id = 1"),
        ("INSERT SELECT", "INSERT INTO m2 SELECT id + 10, v FROM m1", "SELECT count(*) FROM m2"),
        ("TRUNCATE", "TRUNCATE m2", "SELECT count(*) FROM m2"),
        ("SERIAL", "CREATE TABLE m3 (id SERIAL PRIMARY KEY, v TEXT); INSERT INTO m3 (v) VALUES ('x'), ('y')", "SELECT string_agg(id::text, ',' ORDER BY id) FROM m3"),
        ("CREATE ROLE", "CREATE ROLE m_role LOGIN PASSWORD 'x'", "SELECT count(*) FROM rdb_roles WHERE name = 'm_role'"),
        ("ALTER ROLE", "ALTER ROLE m_role PASSWORD 'y'", "SELECT md5(password) FROM rdb_roles WHERE name = 'm_role'"),
        ("DROP ROLE", "DROP ROLE m_role", "SELECT count(*) FROM rdb_roles WHERE name = 'm_role'"),
        ("PARTITIONED", "CREATE TABLE mp (k INT NOT NULL, v TEXT) PARTITION BY HASH (k) PARTITIONS 4; INSERT INTO mp VALUES (1,'a'),(2,'b'),(3,'c')", "SELECT count(*) FROM mp"),
        ("PARTITION WRITES", "UPDATE mp SET v = 'z' WHERE k = 2; DELETE FROM mp WHERE k = 3", "SELECT string_agg(k::text || v, ',' ORDER BY k) FROM mp"),
        ("JSONB", "CREATE TABLE mj (id INT, d JSONB); INSERT INTO mj VALUES (1, '{\"a\": 1}'); UPDATE mj SET d = jsonb_set(d, '{b}', '2')", "SELECT d::text FROM mj"),
        ("BLOCK", "BEGIN; INSERT INTO m1 VALUES (5, 'blk'); UPDATE m1 SET v = 'blk2' WHERE id = 5; COMMIT", "SELECT v FROM m1 WHERE id = 5"),
        ("ROLLBACK", "BEGIN; INSERT INTO m1 VALUES (6, 'rb'); ROLLBACK", "SELECT count(*) FROM m1 WHERE id = 6"),
        ("DROP TABLE", "DROP TABLE m3", "SELECT count(*) FROM information_schema.tables WHERE table_name = 'm3'"),
    ];
    let mut wrong = Vec::new();
    for (kind, sql, check) in replicated {
        if let Err(e) = a.batch_execute(sql).await {
            wrong.push(format!("{kind}: failed on node one: {e:?}"));
            continue;
        }
        let want = rows(&a, check).await;
        let mut got = Vec::new();
        for _ in 0..100 {
            got = rows(&b, check).await;
            if got == want {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if got != want {
            wrong.push(format!("{kind}: node one {want:?}, node two {got:?}"));
        }
    }
    assert!(notices.lock().unwrap().iter().all(|n| !n.starts_with("not replicated")), "{:?}", notices.lock().unwrap());

    // Each node's own: done here, not shipped, and the client is told.
    for (kind, sql) in [("VACUUM", "VACUUM m1"), ("CREATE DATABASE", "CREATE DATABASE m_db")] {
        notices.lock().unwrap().clear();
        a.batch_execute(sql).await.unwrap_or_else(|e| panic!("{kind}: {e}"));
        let said = notices.lock().unwrap().clone();
        if !said.iter().any(|n| n.starts_with("not replicated")) {
            wrong.push(format!("{kind}: no 'not replicated' warning ({said:?})"));
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(rows(&b, "SELECT count(*) FROM pg_database WHERE datname = 'm_db'").await, ["0"]);
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    drop((one, two));
    let _ = std::fs::remove_dir_all(&root);
}
