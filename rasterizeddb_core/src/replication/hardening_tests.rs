//! What replication must hold under concurrency, crashes and bad links.
//!
//! `tests.rs` shows that a write on one node turns up on another. These show
//! the properties that make that trustworthy: nothing is lost, nothing is
//! applied twice, and both nodes end with the same rows whatever the order
//! their writers raced in.

use std::time::Duration;

use rclite::Arc;

use crate::core::database::Database;
use crate::core::rql::executor::execute;
use crate::core::rql::lexer_s1::recognize_query_purpose;

use super::config::{PeerConfig, ReplicationConfig};
use super::protocol::{ApplyStatus, TxnRecord};
use super::test_support::listener;

fn brisk(config: &mut ReplicationConfig) {
    config.listen_addr = "127.0.0.1".to_string();
    config.batch_linger = Duration::from_micros(200);
    config.reconnect_base = Duration::from_millis(25);
    config.reconnect_max = Duration::from_millis(200);
    config.connect_timeout = Duration::from_secs(2);
    config.request_timeout = Duration::from_secs(5);
    config.ack_timeout = Duration::from_secs(5);
}

async fn node(
    node_id: u64,
    bound: (std::net::TcpListener, u16),
    peers: Vec<PeerConfig>,
) -> Arc<Database> {
    let (socket, port) = bound;
    let database = super::test_support::in_memory_database().await;
    let mut config = ReplicationConfig {
        node_id,
        listen_port: port,
        peers,
        ..Default::default()
    };
    brisk(&mut config);
    database.replication.set_config(config).expect("a valid configuration");
    database.replication.serve_on(socket);
    Database::start_replication(database.clone()).expect("replication should start");
    database
}

async fn pair() -> (Arc<Database>, Arc<Database>) {
    let (bound_one, bound_two) = (listener(), listener());
    let (port_one, port_two) = (bound_one.1, bound_two.1);
    let one = node(1, bound_one, vec![PeerConfig::new("two", format!("127.0.0.1:{port_two}"))]).await;
    let two = node(2, bound_two, vec![PeerConfig::new("one", format!("127.0.0.1:{port_one}"))]).await;
    (one, two)
}

async fn run(database: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("cannot parse `{sql}`"));
    execute(purpose, database.clone()).await
}

async fn run_ok(database: &Arc<Database>, sql: &str) {
    let response = run(database, sql).await;
    assert_ne!(
        response.first(),
        Some(&3),
        "`{sql}` failed: {}",
        String::from_utf8_lossy(&response[1..])
    );
}

/// Every row of `sql`'s result as its columns' text, sorted. `None` when the
/// statement failed (the table is not there yet).
async fn rows(database: &Arc<Database>, sql: &str) -> Option<Vec<Vec<String>>> {
    let response = run(database, sql).await;
    if response.first() != Some(&2) {
        return None;
    }
    let rows = crate::core::row::row::vec_into_rows(&response[1..]).ok()?;
    let mut out: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            row.columns
                .iter()
                .map(|column| {
                    let bytes = column.data.into_slice();
                    match bytes.len() {
                        4 if !bytes.iter().all(|b| b.is_ascii_graphic()) => {
                            i32::from_le_bytes(bytes.try_into().unwrap()).to_string()
                        }
                        _ => String::from_utf8_lossy(bytes).trim_end_matches('\0').to_string(),
                    }
                })
                .collect()
        })
        .collect();
    out.sort();
    Some(out)
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    const LIMIT: Duration = Duration::from_secs(30);
    let deadline = tokio::time::Instant::now() + LIMIT;
    loop {
        if check().await {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out after {LIMIT:?} waiting for {what}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Both nodes have applied everything the other originated.
async fn settled(one: &Arc<Database>, two: &Arc<Database>) {
    eventually("both nodes to hold each other's whole stream", || async {
        two.replication.applied().high_water(1) == one.replication.local_seq()
            && one.replication.applied().high_water(2) == two.replication.local_seq()
    })
    .await;
}

/// Writers racing for one row on one node: whatever order they committed in
/// there is the order the other node must apply them in. The last writer's
/// value is the row's value on both.
///
/// A sequence number handed out after the row lock is released lets the
/// writer that waited publish first; the other node then applies the two in
/// the wrong order and keeps the older value, and nothing reports it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writers_racing_for_one_row_leave_both_nodes_with_the_same_value() {
    let (one, two) = pair().await;
    run_ok(&one, "CREATE TABLE repl_race (id INTEGER, name VARCHAR)").await;
    run_ok(&one, "INSERT INTO repl_race (id, name) VALUES (1, 'start')").await;

    const WRITERS: usize = 8;
    const ROUNDS: usize = 60;
    let mut tasks = Vec::new();
    for writer in 0..WRITERS {
        let one = one.clone();
        tasks.push(tokio::spawn(async move {
            for round in 0..ROUNDS {
                let sql = format!("UPDATE repl_race SET name = 'w{writer}r{round}' WHERE id = 1");
                // A writer that loses a conflict tries again: the test is
                // about the order of the ones that commit.
                for _ in 0..50 {
                    if run(&one, &sql).await.first() != Some(&3) {
                        break;
                    }
                }
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    settled(&one, &two).await;
    let on_one = rows(&one, "SELECT name FROM repl_race WHERE id = 1").await;
    let on_two = rows(&two, "SELECT name FROM repl_race WHERE id = 1").await;
    assert_eq!(on_one, on_two, "the nodes disagree about the last write");
}

/// The window, held open: a transaction that has committed and not yet been
/// handed its place in the stream, and a second one that read its row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_that_saw_another_is_shipped_after_it() {
    let (one, two) = pair().await;
    run_ok(&one, "CREATE TABLE repl_race (id INTEGER, name VARCHAR)").await;

    let tx = one.transactions.begin();
    let insert = recognize_query_purpose("INSERT INTO repl_race (id, name) VALUES (1, 'first')").unwrap();
    let response = crate::core::rql::executor::execute_in_txn(insert, one.clone(), tx).await;
    assert_ne!(response.first(), Some(&3));
    let family = one.commit_written_transaction(tx).expect("commit");
    one.transactions.flush().expect("flush");

    run_ok(&one, "UPDATE repl_race SET name = 'second' WHERE id = 1").await;
    one.publish_transaction(family).await;

    settled(&one, &two).await;
    assert_eq!(
        rows(&two, "SELECT name FROM repl_race WHERE id = 1").await,
        rows(&one, "SELECT name FROM repl_race WHERE id = 1").await,
    );
}

/// One record reaching a node twice at the same moment (a link that
/// reconnected while its old request was still being served, a resync during
/// a push) is applied once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_arriving_twice_at_once_is_applied_once() {
    let database = super::test_support::in_memory_database().await;
    database
        .replication
        .set_config(ReplicationConfig { node_id: 1, ..Default::default() })
        .unwrap();
    database.replication.open_storage(None).unwrap();
    database.replication.set_active(true);
    run_ok(&database, "CREATE TABLE repl_twice (id INTEGER, name VARCHAR)").await;

    for seq in 1..=40u64 {
        let record = TxnRecord {
            origin: 2,
            seq,
            commit_hlc: seq * 1000,
            flags: 0,
            statements: vec![format!("INSERT INTO repl_twice (id, name) VALUES ({seq}, 'x')")],
            tables: vec!["repl_twice".to_string()],
        };
        let mut arrivals = Vec::new();
        for _ in 0..4 {
            let database = database.clone();
            let record = record.clone();
            arrivals.push(tokio::spawn(async move {
                let replication = database.replication.clone();
                super::server::apply_record(&database, &replication, record, 0).await.status
            }));
        }
        let mut applied = 0;
        for arrival in arrivals {
            if arrival.await.unwrap() == ApplyStatus::Applied {
                applied += 1;
            }
        }
        assert_eq!(applied, 1, "sequence {seq} was applied {applied} times");
    }
    let held = rows(&database, "SELECT id FROM repl_twice WHERE 1 = 1").await.unwrap();
    assert_eq!(held.len(), 40, "a record applied twice left a second row");
}
