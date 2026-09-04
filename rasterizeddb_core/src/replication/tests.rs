//! Two instances, replicating for real.
//!
//! Everything here runs the whole stack: two `Database`s, each with its own
//! `rastcp` listener on a loopback port, connected to each other as peers. The
//! statements go through the ordinary executor and the assertions read the
//! other node through the ordinary query path, so what is being tested is the
//! system rather than a mock of it.
//!
//! The unit-level behaviour — framing, the log, the conflict rules — is tested
//! next to the code it belongs to. What is left for here is the property none
//! of those can show on its own: that a write on one node turns up on another.

use std::time::Duration;

use rclite::Arc;

use crate::core::database::Database;
use crate::core::rql::executor::execute;
use crate::core::rql::lexer_s1::recognize_query_purpose;

use super::config::{Compression, CompressionMode, PeerConfig, ReplicationConfig, SyncMode};

use super::test_support::free_port;

/// Settings that make a test converge in a moment rather than in the seconds a
/// production default would take.
fn brisk(config: &mut ReplicationConfig) {
    config.listen_addr = "127.0.0.1".to_string();
    config.batch_linger = Duration::from_micros(200);
    config.reconnect_base = Duration::from_millis(25);
    config.reconnect_max = Duration::from_millis(200);
    config.connect_timeout = Duration::from_secs(2);
    config.request_timeout = Duration::from_secs(5);
    config.ack_timeout = Duration::from_secs(5);
}

/// Builds an in-memory node and starts its replication.
async fn node(node_id: u64, port: u16, peers: Vec<PeerConfig>) -> Arc<Database> {
    let database = super::test_support::in_memory_database().await;

    let mut config = ReplicationConfig {
        node_id,
        listen_port: port,
        peers,
        ..Default::default()
    };
    brisk(&mut config);

    database
        .replication
        .set_config(config)
        .expect("the test configuration should be valid");
    Database::start_replication(database.clone()).expect("replication should start");

    database
}

async fn run(database: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("cannot parse `{}`", sql));
    execute(purpose, database.clone()).await
}

/// Runs a statement and fails the test with the engine's own message if it did
/// not work — otherwise a replication assertion fails later for a reason that
/// has nothing to do with replication.
async fn run_ok(database: &Arc<Database>, sql: &str) {
    let response = run(database, sql).await;
    assert_ne!(
        response.first(),
        Some(&3),
        "`{}` failed: {}",
        sql,
        String::from_utf8_lossy(&response[1..])
    );
}

/// Rows in `repl_users`, or `None` when the table is not there yet.
async fn users(database: &Arc<Database>) -> Option<Vec<String>> {
    let response = run(database, "SELECT id, name FROM repl_users WHERE 1 = 1").await;
    if response.first() != Some(&2) {
        return None;
    }
    let rows = crate::core::row::row::vec_into_rows(&response[1..]).ok()?;

    let mut names: Vec<String> = rows
        .iter()
        .filter_map(|row| row.columns.get(1))
        .map(|column| {
            String::from_utf8_lossy(column.data.into_slice())
                .trim_end_matches('\0')
                .to_string()
        })
        .collect();
    names.sort();
    Some(names)
}

/// Waits for `check` to hold, or gives up.
///
/// Replication is asynchronous, so a test either waits for it or races it. The
/// generous ceiling costs nothing when things work — it returns as soon as the
/// condition holds — and turns a flake into a failure when they do not.
async fn eventually<F, Fut>(what: &str, mut check: F) -> ()
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    const LIMIT: Duration = Duration::from_secs(20);
    let deadline = tokio::time::Instant::now() + LIMIT;

    loop {
        if check().await {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out after {:?} waiting for {}", LIMIT, what);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Two nodes, each configured with the other.
async fn pair() -> (Arc<Database>, Arc<Database>) {
    let (port_one, port_two) = (free_port(), free_port());

    let one = node(
        1,
        port_one,
        vec![PeerConfig::new("two", format!("127.0.0.1:{}", port_two))],
    )
    .await;
    let two = node(
        2,
        port_two,
        vec![PeerConfig::new("one", format!("127.0.0.1:{}", port_one))],
    )
    .await;

    (one, two)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_on_one_node_turns_up_on_the_other() {
    let (one, two) = pair().await;

    run_ok(&one, "CREATE TABLE repl_users (id INTEGER, name VARCHAR)").await;
    run_ok(&one, "INSERT INTO repl_users (id, name) VALUES (1, 'ada')").await;

    eventually("the row to reach node two", || async {
        users(&two).await.as_deref() == Some(&["ada".to_string()][..])
    })
    .await;
}

/// The point of multi-primary: both ends take writes, and both ends see both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn both_nodes_accept_writes_and_converge() {
    let (one, two) = pair().await;

    run_ok(&one, "CREATE TABLE repl_users (id INTEGER, name VARCHAR)").await;
    eventually("the table to reach node two", || async {
        users(&two).await.is_some()
    })
    .await;

    run_ok(&one, "INSERT INTO repl_users (id, name) VALUES (1, 'ada')").await;
    run_ok(
        &two,
        "INSERT INTO repl_users (id, name) VALUES (2, 'grace')",
    )
    .await;

    let expected = vec!["ada".to_string(), "grace".to_string()];
    eventually("node one to hold both rows", || async {
        users(&one).await.as_ref() == Some(&expected)
    })
    .await;
    eventually("node two to hold both rows", || async {
        users(&two).await.as_ref() == Some(&expected)
    })
    .await;
}

/// The failure this design has to rule out: a mesh where each node
/// re-originates what it was sent, and the two bounce it forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replicated_transaction_is_not_echoed_back() {
    let (one, two) = pair().await;

    run_ok(&one, "CREATE TABLE repl_users (id INTEGER, name VARCHAR)").await;
    run_ok(&one, "INSERT INTO repl_users (id, name) VALUES (1, 'ada')").await;

    eventually("the row to reach node two", || async {
        users(&two).await.as_deref() == Some(&["ada".to_string()][..])
    })
    .await;

    // Give an echo every chance to happen before looking.
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        two.replication.local_seq(),
        0,
        "node two originated nothing, so its sequence must not have moved"
    );
    assert_eq!(
        one.replication.local_seq(),
        2,
        "node one originated exactly the CREATE and the INSERT"
    );
    assert_eq!(
        users(&two).await.as_deref(),
        Some(&["ada".to_string()][..]),
        "an echo would have inserted the row twice"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_multi_statement_transaction_arrives_whole() {
    let (one, two) = pair().await;

    run_ok(&one, "CREATE TABLE repl_users (id INTEGER, name VARCHAR)").await;
    for (id, name) in [(1, "ada"), (2, "grace"), (3, "edsger")] {
        run_ok(
            &one,
            &format!(
                "INSERT INTO repl_users (id, name) VALUES ({}, '{}')",
                id, name
            ),
        )
        .await;
    }

    let expected = vec!["ada".to_string(), "edsger".to_string(), "grace".to_string()];
    eventually("all three rows to reach node two", || async {
        users(&two).await.as_ref() == Some(&expected)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_update_and_a_delete_replicate_as_well_as_an_insert() {
    let (one, two) = pair().await;

    run_ok(&one, "CREATE TABLE repl_users (id INTEGER, name VARCHAR)").await;
    run_ok(&one, "INSERT INTO repl_users (id, name) VALUES (1, 'ada')").await;
    run_ok(
        &one,
        "INSERT INTO repl_users (id, name) VALUES (2, 'grace')",
    )
    .await;

    eventually("both rows to reach node two", || async {
        users(&two).await.map(|rows| rows.len()) == Some(2)
    })
    .await;

    run_ok(&one, "UPDATE repl_users SET name = 'ada l.' WHERE id = 1").await;
    run_ok(&one, "DELETE FROM repl_users WHERE id = 2").await;

    eventually("the update and the delete to reach node two", || async {
        users(&two).await.as_deref() == Some(&["ada l.".to_string()][..])
    })
    .await;
}

/// A node that was not there when the write happened has to be told about it
/// when it comes back, which is what the log and the catch-up are for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_that_was_paused_catches_up_when_it_resumes() {
    let (one, two) = pair().await;

    run_ok(&one, "CREATE TABLE repl_users (id INTEGER, name VARCHAR)").await;
    eventually("the table to reach node two", || async {
        users(&two).await.is_some()
    })
    .await;

    let link = one
        .replication
        .peer("two")
        .expect("node one knows node two");
    link.set_paused(true);
    // Let the pause take effect before writing anything behind it.
    tokio::time::sleep(Duration::from_millis(100)).await;

    for id in 1..=5 {
        run_ok(
            &one,
            &format!(
                "INSERT INTO repl_users (id, name) VALUES ({}, 'user{}')",
                id, id
            ),
        )
        .await;
    }

    assert_eq!(
        users(&two).await.map(|rows| rows.len()),
        Some(0),
        "a paused peer should not have received anything"
    );

    link.set_paused(false);

    eventually("the backlog to drain after resuming", || async {
        users(&two).await.map(|rows| rows.len()) == Some(5)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transactions_replicate_uncompressed_when_compression_is_off() {
    let (port_one, port_two) = (free_port(), free_port());

    let plain = Compression {
        mode: CompressionMode::Off,
        codec: super::config::Codec::None,
        ..Default::default()
    };

    let build = |node_id: u64, port: u16, peer_port: u16| async move {
        let database = super::test_support::in_memory_database().await;
        let mut config = ReplicationConfig {
            node_id,
            listen_port: port,
            compression: plain,
            peers: vec![PeerConfig::new("other", format!("127.0.0.1:{}", peer_port))],
            ..Default::default()
        };
        brisk(&mut config);
        database.replication.set_config(config).unwrap();
        Database::start_replication(database.clone()).unwrap();
        database
    };

    let one = build(1, port_one, port_two).await;
    let two = build(2, port_two, port_one).await;

    run_ok(&one, "CREATE TABLE repl_users (id INTEGER, name VARCHAR)").await;
    run_ok(&one, "INSERT INTO repl_users (id, name) VALUES (1, 'ada')").await;

    eventually("the row to reach node two uncompressed", || async {
        users(&two).await.as_deref() == Some(&["ada".to_string()][..])
    })
    .await;

    let stats = one.replication.stats();
    let raw = stats.bytes_raw.load(std::sync::atomic::Ordering::Relaxed);
    let wire = stats.bytes_wire.load(std::sync::atomic::Ordering::Relaxed);
    assert!(raw > 0, "something should have been sent");
    assert!(
        wire >= raw,
        "with compression off the wire carries the payload plus a header: {} vs {}",
        wire,
        raw
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "replication_compression")]
async fn a_batch_of_statements_is_compressed_on_the_wire() {
    let (port_one, port_two) = (free_port(), free_port());

    let squeeze = Compression {
        mode: CompressionMode::Always,
        codec: super::config::Codec::Zstd,
        // Compress everything, so one batch of a test's size is enough to show
        // the effect rather than falling under the production floor.
        min_bytes: 0,
        ..Default::default()
    };

    let build = |node_id: u64, port: u16, peer_port: u16| async move {
        let database = super::test_support::in_memory_database().await;
        let mut config = ReplicationConfig {
            node_id,
            listen_port: port,
            compression: squeeze,
            peers: vec![PeerConfig::new("other", format!("127.0.0.1:{}", peer_port))],
            ..Default::default()
        };
        brisk(&mut config);
        // A long linger, so the inserts below travel as one batch and the
        // compressor has something repetitive to work with.
        config.batch_linger = Duration::from_millis(50);
        database.replication.set_config(config).unwrap();
        Database::start_replication(database.clone()).unwrap();
        database
    };

    let one = build(1, port_one, port_two).await;
    let two = build(2, port_two, port_one).await;

    run_ok(&one, "CREATE TABLE repl_users (id INTEGER, name VARCHAR)").await;
    for id in 1..=200 {
        run_ok(
            &one,
            &format!(
                "INSERT INTO repl_users (id, name) VALUES ({}, 'a fairly repetitive value {}')",
                id, id
            ),
        )
        .await;
    }

    eventually("all two hundred rows to reach node two", || async {
        users(&two).await.map(|rows| rows.len()) == Some(200)
    })
    .await;

    let stats = one.replication.stats();
    let raw = stats.bytes_raw.load(std::sync::atomic::Ordering::Relaxed);
    let wire = stats.bytes_wire.load(std::sync::atomic::Ordering::Relaxed);

    assert!(raw > 0);
    assert!(
        wire < raw,
        "repetitive SQL should compress: {} bytes of payload went out as {}",
        raw,
        wire
    );
    assert!(
        stats.compression_saving().unwrap_or(0.0) > 0.0,
        "the saving should be reported in the status view"
    );
}

/// A record's own node must not apply it a second time when it comes back, and
/// a resend must not double-write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resending_the_same_transaction_changes_nothing() {
    let (one, two) = pair().await;

    run_ok(&one, "CREATE TABLE repl_users (id INTEGER, name VARCHAR)").await;
    run_ok(&one, "INSERT INTO repl_users (id, name) VALUES (1, 'ada')").await;

    eventually("the row to reach node two", || async {
        users(&two).await.as_deref() == Some(&["ada".to_string()][..])
    })
    .await;

    // Force the link down and back up, which replays the catch-up from the log
    // over records node two already has.
    let link = one
        .replication
        .peer("two")
        .expect("node one knows node two");
    link.set_paused(true);
    tokio::time::sleep(Duration::from_millis(100)).await;
    link.set_paused(false);

    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        users(&two).await.as_deref(),
        Some(&["ada".to_string()][..]),
        "a resend must be recognised as a duplicate"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_status_view_reports_a_connected_peer() {
    let (one, _two) = pair().await;

    eventually("node one to report its peer as up", || async {
        let lines = super::sql::execute("SHOW REPLICATION PEERS", &one)
            .await
            .expect("the status view should render");
        lines
            .iter()
            .any(|line| line.contains("two") && line.contains("up"))
    })
    .await;
}

/// A cluster name is a misconfiguration guard, and it has to actually guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nodes_in_different_clusters_do_not_replicate() {
    let (port_one, port_two) = (free_port(), free_port());

    let build = |node_id: u64, port: u16, peer_port: u16, cluster: &str| {
        let cluster = cluster.to_string();
        async move {
            let database = super::test_support::in_memory_database().await;
            let mut config = ReplicationConfig {
                node_id,
                listen_port: port,
                cluster,
                peers: vec![PeerConfig::new("other", format!("127.0.0.1:{}", peer_port))],
                ..Default::default()
            };
            brisk(&mut config);
            database.replication.set_config(config).unwrap();
            Database::start_replication(database.clone()).unwrap();
            database
        }
    };

    let one = build(1, port_one, port_two, "east").await;
    let two = build(2, port_two, port_one, "west").await;

    run_ok(&one, "CREATE TABLE repl_users (id INTEGER, name VARCHAR)").await;
    run_ok(&one, "INSERT INTO repl_users (id, name) VALUES (1, 'ada')").await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        users(&two).await.is_none(),
        "a node in another cluster must not have been written to"
    );
}

/// The whole surface, driven the way an operator would drive it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cluster_can_be_built_entirely_from_sql() {
    let (port_one, port_two) = (free_port(), free_port());

    let bare = |node_id: u64, port: u16| async move {
        let database = super::test_support::in_memory_database().await;
        let mut config = ReplicationConfig {
            node_id,
            listen_port: port,
            ..Default::default()
        };
        brisk(&mut config);
        database.replication.set_config(config).unwrap();
        database
    };

    let one = bare(1, port_one).await;
    let two = bare(2, port_two).await;

    for (database, peer_port) in [(&one, port_two), (&two, port_one)] {
        run_ok(
            database,
            &format!(
                "REPLICATION ADD PEER 'other' ADDRESS '127.0.0.1:{}'",
                peer_port
            ),
        )
        .await;
        run_ok(database, "REPLICATION ENABLE").await;
    }

    run_ok(&one, "CREATE TABLE repl_users (id INTEGER, name VARCHAR)").await;
    run_ok(&one, "INSERT INTO repl_users (id, name) VALUES (1, 'ada')").await;

    eventually("the row to reach the node added over SQL", || async {
        users(&two).await.as_deref() == Some(&["ada".to_string()][..])
    })
    .await;

    // And it can be taken apart the same way.
    run_ok(&one, "REPLICATION DROP PEER 'other'").await;
    assert!(one.replication.peer("other").is_none());
}

// ---------------------------------------------------------------------------
// The gaps these tests exist to keep closed
// ---------------------------------------------------------------------------

/// A listener that cannot bind must say so to whoever asked, and must leave the
/// node able to try again. Reporting success and logging the failure inside a
/// spawned task leaves an operator with a node that says it is replicating and
/// is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_listener_that_cannot_bind_says_so_and_stays_retryable() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    let port = occupied.local_addr().expect("a bound address").port();

    let database = super::test_support::in_memory_database().await;
    let mut config = ReplicationConfig {
        node_id: 1,
        listen_port: port,
        ..Default::default()
    };
    brisk(&mut config);
    database.replication.set_config(config).unwrap();

    let error = Database::start_replication(database.clone())
        .expect_err("the port is held, so this must fail");
    assert!(
        error.to_string().contains("bind"),
        "the failure should name what went wrong: {}",
        error
    );

    // The claim on the listener has to have been given back, or the node can
    // never bind for the rest of the process's life.
    drop(occupied);
    Database::start_replication(database.clone())
        .expect("with the port free, a second attempt should succeed");
}

/// `REPLICATION RESYNC` has to drain the backlog, not take the first answer.
/// The peer flags that it has more; ignoring the flag resynchronises one batch
/// and stops without saying so.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resync_pulls_every_batch_and_not_just_the_first() {
    let (port_source, port_puller) = (free_port(), free_port());

    // The source has no peers, so it never pushes: everything the puller gets,
    // it got by pulling.
    let source = node(1, port_source, Vec::new()).await;

    let puller = super::test_support::in_memory_database().await;
    let mut config = ReplicationConfig {
        node_id: 2,
        listen_port: port_puller,
        // Two records per answer, so ten records take several rounds.
        batch_max_records: 2,
        peers: vec![PeerConfig::new(
            "source",
            format!("127.0.0.1:{}", port_source),
        )],
        ..Default::default()
    };
    brisk(&mut config);
    puller.replication.set_config(config).unwrap();
    Database::start_replication(puller.clone()).unwrap();

    run_ok(
        &source,
        "CREATE TABLE repl_users (id INTEGER, name VARCHAR)",
    )
    .await;
    for id in 1..=9 {
        run_ok(
            &source,
            &format!(
                "INSERT INTO repl_users (id, name) VALUES ({}, 'user{}')",
                id, id
            ),
        )
        .await;
    }
    assert_eq!(
        source.replication.local_seq(),
        10,
        "one CREATE plus nine INSERTs"
    );

    // Nothing should have arrived on its own.
    assert!(
        users(&puller).await.is_none(),
        "the source has no peers, so it pushes nothing"
    );

    // Wait for the link so the peer's node id is known, which RESYNC needs.
    eventually("the puller's link to the source", || async {
        puller
            .replication
            .peer("source")
            .map(|p| p.remote_node() != 0)
            .unwrap_or(false)
    })
    .await;

    let report = super::sql::execute("REPLICATION RESYNC PEER 'source' FROM 0", &puller)
        .await
        .expect("the resync should run");

    assert_eq!(
        users(&puller).await.map(|rows| rows.len()),
        Some(9),
        "every batch should have been pulled, not just the first: {}",
        report.join(" | ")
    );
    assert_eq!(puller.replication.applied().high_water(1), 10);
}

/// Relaying: a node that cannot see the origin gets the transaction through the
/// node in the middle. Also the first test with three nodes at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_chain_carries_a_transaction_to_a_node_the_origin_cannot_see() {
    let (port_a, port_b, port_c) = (free_port(), free_port(), free_port());

    // A ── B ── C, and A has never heard of C.
    let a = node(
        1,
        port_a,
        vec![PeerConfig::new("b", format!("127.0.0.1:{}", port_b))],
    )
    .await;

    let b = super::test_support::in_memory_database().await;
    let mut config = ReplicationConfig {
        node_id: 2,
        listen_port: port_b,
        relay: true,
        peers: vec![
            PeerConfig::new("a", format!("127.0.0.1:{}", port_a)),
            PeerConfig::new("c", format!("127.0.0.1:{}", port_c)),
        ],
        ..Default::default()
    };
    brisk(&mut config);
    b.replication.set_config(config).unwrap();
    Database::start_replication(b.clone()).unwrap();

    let c = node(
        3,
        port_c,
        vec![PeerConfig::new("b", format!("127.0.0.1:{}", port_b))],
    )
    .await;

    // Let every link finish its handshake before anything is written, so the
    // relay knows which peer is which.
    eventually("the chain to come up", || async {
        b.replication.peers().iter().all(|p| p.remote_node() != 0)
    })
    .await;

    run_ok(&a, "CREATE TABLE repl_users (id INTEGER, name VARCHAR)").await;
    run_ok(&a, "INSERT INTO repl_users (id, name) VALUES (1, 'ada')").await;

    eventually("the row to reach the far end of the chain", || async {
        users(&c).await.as_deref() == Some(&["ada".to_string()][..])
    })
    .await;

    // And the relay must not have sent it back where it came from and had it
    // applied twice.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        users(&a).await.map(|rows| rows.len()),
        Some(1),
        "the origin"
    );
    assert_eq!(
        users(&b).await.map(|rows| rows.len()),
        Some(1),
        "the middle"
    );
    assert_eq!(
        users(&c).await.map(|rows| rows.len()),
        Some(1),
        "the far end"
    );
    assert_eq!(
        c.replication.applied().high_water(1),
        2,
        "the far end should attribute the transactions to the origin, not to the relay"
    );
}

/// A synchronous commit returns only once a peer has actually taken the record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_synchronous_commit_waits_for_the_peer() {
    let (port_one, port_two) = (free_port(), free_port());

    let one = super::test_support::in_memory_database().await;
    let mut config = ReplicationConfig {
        node_id: 1,
        listen_port: port_one,
        sync_mode: SyncMode::Quorum(1),
        peers: vec![PeerConfig::new("two", format!("127.0.0.1:{}", port_two))],
        ..Default::default()
    };
    brisk(&mut config);
    one.replication.set_config(config).unwrap();
    Database::start_replication(one.clone()).unwrap();

    let two = node(
        2,
        port_two,
        vec![PeerConfig::new("one", format!("127.0.0.1:{}", port_one))],
    )
    .await;

    eventually("the link to come up", || async {
        one.replication
            .peer("two")
            .map(|p| p.is_connected())
            .unwrap_or(false)
    })
    .await;

    run_ok(&one, "CREATE TABLE repl_users (id INTEGER, name VARCHAR)").await;
    run_ok(&one, "INSERT INTO repl_users (id, name) VALUES (1, 'ada')").await;

    // The commit already waited for the acknowledgement, so this needs no
    // polling: if it is not there now, the wait did not mean anything.
    assert_eq!(
        users(&two).await.as_deref(),
        Some(&["ada".to_string()][..]),
        "a synchronous commit must not return before the peer has the record"
    );
    assert_eq!(
        one.replication
            .stats()
            .ack_timeouts
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "nothing should have timed out"
    );
}

/// A synchronous commit against a peer that is not there must not hang: the
/// local commit has happened, and the wait is bounded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_synchronous_commit_gives_up_on_a_peer_that_is_not_there() {
    let port = free_port();

    let one = super::test_support::in_memory_database().await;
    let mut config = ReplicationConfig {
        node_id: 1,
        listen_port: port,
        sync_mode: SyncMode::Quorum(1),
        // Nothing is listening there, and never will be.
        peers: vec![PeerConfig::new("ghost", "127.0.0.1:1".to_string())],
        ..Default::default()
    };
    brisk(&mut config);
    config.ack_timeout = Duration::from_millis(300);
    one.replication.set_config(config).unwrap();
    Database::start_replication(one.clone()).unwrap();

    let started = std::time::Instant::now();
    run_ok(&one, "CREATE TABLE repl_users (id INTEGER, name VARCHAR)").await;
    run_ok(&one, "INSERT INTO repl_users (id, name) VALUES (1, 'ada')").await;
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(5),
        "the wait is bounded by ack_timeout, not by the peer coming back: {:?}",
        elapsed
    );
    assert_eq!(
        users(&one).await.map(|rows| rows.len()),
        Some(1),
        "the local write stands regardless of what the peer did"
    );
    assert!(
        one.replication
            .stats()
            .ack_timeouts
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0,
        "and the give-up should be visible in the statistics"
    );
}

/// A commit becomes visible to the cluster only once its outcome is durable.
///
/// The autocommit path commits, flushes the outcome record, and reports failure
/// if that flush did not work — rolling the transaction back. Publishing inside
/// the commit put the transaction on the wire *before* that check, so a flush
/// failure left the peers holding a write this node had disowned, with no way to
/// recall it. The ordering is the fix, and this pins it: committing alone must
/// publish nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committing_publishes_nothing_until_the_outcome_is_durable() {
    let database = super::test_support::in_memory_database().await;
    let mut config = ReplicationConfig {
        node_id: 1,
        listen_port: free_port(),
        ..Default::default()
    };
    brisk(&mut config);
    database.replication.set_config(config).unwrap();
    database.replication.open_storage(None).unwrap();
    database.replication.set_active(true);

    run_ok(
        &database,
        "CREATE TABLE repl_users (id INTEGER, name VARCHAR)",
    )
    .await;
    let published_before = database.replication.local_seq();

    // A write in a transaction the caller owns, as an explicit block does.
    let tx = database.transactions.begin();
    let purpose =
        recognize_query_purpose("INSERT INTO repl_users (id, name) VALUES (1, 'ada')").unwrap();
    let response = crate::core::rql::executor::execute_in_txn(purpose, database.clone(), tx).await;
    assert_ne!(response.first(), Some(&3), "the insert should have worked");

    let family = database.commit_transaction(tx);
    assert_eq!(
        database.replication.local_seq(),
        published_before,
        "committing must not put the transaction on the wire — the outcome is not durable yet"
    );

    database.publish_transaction(family).await;
    assert_eq!(
        database.replication.local_seq(),
        published_before + 1,
        "publishing is what hands it to the cluster"
    );
}

/// And the other half: a transaction that is aborted publishes nothing, so the
/// rollback the durability check performs cannot leak a write to the peers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_aborted_transaction_is_never_published() {
    let database = super::test_support::in_memory_database().await;
    let mut config = ReplicationConfig {
        node_id: 1,
        listen_port: free_port(),
        ..Default::default()
    };
    brisk(&mut config);
    database.replication.set_config(config).unwrap();
    database.replication.open_storage(None).unwrap();
    database.replication.set_active(true);

    run_ok(
        &database,
        "CREATE TABLE repl_users (id INTEGER, name VARCHAR)",
    )
    .await;
    let published_before = database.replication.local_seq();

    let tx = database.transactions.begin();
    let purpose =
        recognize_query_purpose("INSERT INTO repl_users (id, name) VALUES (1, 'ada')").unwrap();
    crate::core::rql::executor::execute_in_txn(purpose, database.clone(), tx).await;

    // What the durability check does when the flush fails.
    let family = database.commit_transaction(tx);
    database.abort_transaction(tx);
    database.publish_transaction(family).await;

    assert_eq!(
        database.replication.local_seq(),
        published_before,
        "an aborted transaction must leave nothing for the peers, even if publish is called"
    );
    assert_eq!(
        database.replication.open_captures(),
        0,
        "and no buffer behind"
    );
}

/// A block with savepoints is several transaction ids, and all of them have to
/// travel.
///
/// Writes made inside a `SAVEPOINT` carry the sub-transaction's id, not the
/// block's. Publishing on the outer id alone shipped the statements written
/// outside the savepoints and silently dropped the ones written inside — the
/// replica ended up holding a subset of a transaction that committed whole,
/// with nothing anywhere reporting a problem.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_inside_a_savepoint_replicate_with_the_block() {
    let (one, two) = pair().await;

    run_ok(&one, "CREATE TABLE repl_users (id INTEGER, name VARCHAR)").await;
    eventually("the table to reach node two", || async {
        users(&two).await.is_some()
    })
    .await;

    // A block that writes, opens a savepoint, writes inside it, and commits.
    let tx = one.transactions.begin();
    let outer =
        recognize_query_purpose("INSERT INTO repl_users (id, name) VALUES (1, 'outer')").unwrap();
    crate::core::rql::executor::execute_in_txn(outer, one.clone(), tx).await;

    let sub = one.transactions.begin_savepoint(tx);
    let inner =
        recognize_query_purpose("INSERT INTO repl_users (id, name) VALUES (2, 'inner')").unwrap();
    crate::core::rql::executor::execute_in_txn(inner, one.clone(), sub).await;
    one.transactions.release_savepoint(sub);

    let family = one.commit_transaction(tx);
    one.transactions.flush().unwrap();
    one.publish_transaction(family).await;

    let expected = vec!["inner".to_string(), "outer".to_string()];
    eventually(
        "both the outer and the savepoint write to reach node two",
        || async { users(&two).await.as_ref() == Some(&expected) },
    )
    .await;
}

/// And the other half: what a `ROLLBACK TO SAVEPOINT` discarded must not travel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_rolled_back_to_a_savepoint_are_not_replicated() {
    let (one, two) = pair().await;

    run_ok(&one, "CREATE TABLE repl_users (id INTEGER, name VARCHAR)").await;
    eventually("the table to reach node two", || async {
        users(&two).await.is_some()
    })
    .await;

    let tx = one.transactions.begin();
    let kept =
        recognize_query_purpose("INSERT INTO repl_users (id, name) VALUES (1, 'kept')").unwrap();
    crate::core::rql::executor::execute_in_txn(kept, one.clone(), tx).await;

    let sub = one.transactions.begin_savepoint(tx);
    let discarded =
        recognize_query_purpose("INSERT INTO repl_users (id, name) VALUES (2, 'discarded')")
            .unwrap();
    crate::core::rql::executor::execute_in_txn(discarded, one.clone(), sub).await;
    one.transactions.rollback_savepoint(sub);

    let family = one.commit_transaction(tx);
    one.transactions.flush().unwrap();
    one.publish_transaction(family).await;

    let expected = vec!["kept".to_string()];
    eventually("the surviving write to reach node two", || async {
        users(&two).await.as_ref() == Some(&expected)
    })
    .await;

    // Give a leak every chance to show before concluding it did not happen.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        users(&two).await.as_ref(),
        Some(&expected),
        "a rolled-back savepoint's write must never reach a replica"
    );
    assert_eq!(
        users(&one).await.as_ref(),
        Some(&expected),
        "and it is not on the origin either"
    );
    assert_eq!(
        one.replication.open_captures(),
        0,
        "no capture buffer should be left behind"
    );
}
