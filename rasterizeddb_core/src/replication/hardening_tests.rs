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

/// One transaction is one record and travels in one frame. A frame bigger
/// than the transport's default limit used to end the link on every attempt,
/// for good.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_larger_than_the_transports_default_frame_replicates() {
    let (one, two) = pair().await;
    // Incompressible enough that the frame stays large on the wire.
    one.replication
        .update_config(|config| {
            config.compression = super::config::Compression {
                mode: super::config::CompressionMode::Off,
                ..config.compression
            };
            Ok(())
        })
        .unwrap();
    run_ok(&one, "CREATE TABLE repl_big (id INTEGER, body VARCHAR)").await;
    let body = "x".repeat(9 * 1024 * 1024);
    run_ok(&one, &format!("INSERT INTO repl_big (id, body) VALUES (1, '{body}')")).await;
    run_ok(&one, "INSERT INTO repl_big (id, body) VALUES (2, 'small')").await;

    settled(&one, &two).await;
    assert_eq!(
        rows(&two, "SELECT id FROM repl_big WHERE 1 = 1").await.map(|r| r.len()),
        Some(2)
    );
}

/// A peer that refuses a transaction is offered it again, never sent what
/// follows as if it had taken it; once the cause is gone the stream carries
/// on by itself, in order, and nothing is missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_transaction_holds_the_stream_and_is_applied_once_it_can_be() {
    let (one, two) = pair().await;
    run_ok(&one, "CREATE TABLE repl_hold (id INTEGER PRIMARY KEY, name VARCHAR)").await;
    settled(&one, &two).await;

    // Node two already holds the key node one is about to insert. Written
    // behind replication's back, so it does not travel to node one.
    two.replication.set_active(false);
    run_ok(&two, "INSERT INTO repl_hold (id, name) VALUES (1, 'in the way')").await;
    two.replication.set_active(true);

    run_ok(&one, "INSERT INTO repl_hold (id, name) VALUES (1, 'first')").await;
    run_ok(&one, "INSERT INTO repl_hold (id, name) VALUES (2, 'second')").await;

    eventually("the link to report the stall", || async {
        one.replication
            .peer("two")
            .is_some_and(|peer| peer.snapshot(0).state == super::peer::PeerState::Stalled)
    })
    .await;
    assert_eq!(
        two.replication.applied().high_water(1),
        1,
        "nothing past the refused transaction may be applied"
    );
    assert_eq!(
        rows(&two, "SELECT name FROM repl_hold WHERE id = 2").await.map(|r| r.len()),
        Some(0)
    );

    // The operator removes what was in the way.
    two.replication.set_active(false);
    run_ok(&two, "DELETE FROM repl_hold WHERE id = 1").await;
    two.replication.set_active(true);

    settled(&one, &two).await;
    assert_eq!(
        rows(&two, "SELECT name FROM repl_hold WHERE 1 = 1").await,
        rows(&one, "SELECT name FROM repl_hold WHERE 1 = 1").await,
    );
    eventually("the link to report streaming again", || async {
        one.replication
            .peer("two")
            .is_some_and(|peer| peer.snapshot(0).state == super::peer::PeerState::Streaming)
    })
    .await;
}

/// Many writers on both nodes at once, on separate keys: every row ends up
/// on both, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_on_both_nodes_converge_with_nothing_lost_or_doubled() {
    let (one, two) = pair().await;
    run_ok(&one, "CREATE TABLE repl_many (id INTEGER, who VARCHAR)").await;
    settled(&one, &two).await;

    const WRITERS: usize = 6;
    const ROWS: usize = 80;
    let mut tasks = Vec::new();
    for (node_at, node) in [one.clone(), two.clone()].into_iter().enumerate() {
        for writer in 0..WRITERS {
            let node = node.clone();
            tasks.push(tokio::spawn(async move {
                for row in 0..ROWS {
                    let id = node_at * 1_000_000 + writer * 1_000 + row;
                    run_ok(&node, &format!("INSERT INTO repl_many (id, who) VALUES ({id}, 'n{node_at}w{writer}')")).await;
                }
            }));
        }
    }
    for task in tasks {
        task.await.unwrap();
    }

    settled(&one, &two).await;
    let on_one = rows(&one, "SELECT id FROM repl_many WHERE 1 = 1").await.unwrap();
    let on_two = rows(&two, "SELECT id FROM repl_many WHERE 1 = 1").await.unwrap();
    assert_eq!(on_one.len(), 2 * WRITERS * ROWS, "node one lost or doubled a row");
    assert_eq!(on_one, on_two);
    for node in [&one, &two] {
        let stats = node.replication.stats();
        assert_eq!(stats.gaps.load(std::sync::atomic::Ordering::Relaxed), 0, "the stream is sent in order");
    }
}

/// A node on disk, started on `bound` with `peers`.
async fn disk_node(
    dir: &std::path::Path,
    node_id: u64,
    bound: (std::net::TcpListener, u16),
    peers: Vec<PeerConfig>,
) -> Arc<Database> {
    super::test_support::init_globals();
    let (socket, port) = bound;
    let database = Arc::new(
        Database::try_new_named(&dir.to_string_lossy(), false, "repl_disk")
            .await
            .expect("the database should open"),
    );
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

/// A node that stops and starts again takes up its stream where it left it,
/// sends what it committed while its peer could not hear, and applies what
/// its peer committed meanwhile exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_node_misses_nothing_and_repeats_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (bound_one, bound_two) = (listener(), listener());
    let (port_one, port_two) = (bound_one.1, bound_two.1);
    let one = disk_node(
        dir.path(),
        1,
        bound_one,
        vec![PeerConfig::new("two", format!("127.0.0.1:{port_two}"))],
    )
    .await;
    let two = node(2, bound_two, vec![PeerConfig::new("one", format!("127.0.0.1:{port_one}"))]).await;

    // No unique key: anything applied twice shows as a second row.
    run_ok(&one, "CREATE TABLE repl_restart (id INTEGER, who VARCHAR)").await;
    for id in 0..20 {
        run_ok(&one, &format!("INSERT INTO repl_restart (id, who) VALUES ({id}, 'one')")).await;
    }
    settled(&one, &two).await;
    for id in 100..120 {
        run_ok(&two, &format!("INSERT INTO repl_restart (id, who) VALUES ({id}, 'two')")).await;
    }
    settled(&one, &two).await;
    let seq_before = one.replication.local_seq();
    let applied_before = one.replication.applied().high_water(2);
    assert_eq!(applied_before, 20);

    // Node one stops. Its peer cannot reach it, and keeps taking writes.
    let link = two.replication.peer("one").unwrap();
    link.set_paused(true);
    for peer in one.replication.peers() {
        peer.stop();
    }
    one.replication.set_active(false);
    one.replication.flush();
    drop(one);
    crate::core::wal::remove_for(&dir.path().to_string_lossy());
    for id in 120..140 {
        run_ok(&two, &format!("INSERT INTO repl_restart (id, who) VALUES ({id}, 'two')")).await;
    }

    // It starts again, on a new port, and takes writes before the link is up.
    let bound_again = listener();
    let port_again = bound_again.1;
    let one = disk_node(
        dir.path(),
        1,
        bound_again,
        vec![PeerConfig::new("two", format!("127.0.0.1:{port_two}"))],
    )
    .await;
    assert_eq!(one.replication.local_seq(), seq_before, "the stream resumes where it stopped");
    assert_eq!(one.replication.applied().high_water(2), applied_before);
    for id in 20..40 {
        run_ok(&one, &format!("INSERT INTO repl_restart (id, who) VALUES ({id}, 'one')")).await;
    }
    link.set_address(format!("127.0.0.1:{port_again}"));
    link.set_paused(false);

    settled(&one, &two).await;
    let on_one = rows(&one, "SELECT id FROM repl_restart WHERE 1 = 1").await.unwrap();
    let on_two = rows(&two, "SELECT id FROM repl_restart WHERE 1 = 1").await.unwrap();
    assert_eq!(on_one.len(), 80, "node one holds a row twice or not at all");
    assert_eq!(on_one, on_two);
}

/// The files an earlier build left: a log of plain records and a state file
/// of `(origin, seq)` pairs. A node running this build for the first time
/// opens them, resumes its stream and keeps its positions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_files_of_an_earlier_build_open_and_carry_on() {
    let dir = tempfile::tempdir().unwrap();
    {
        // A record without a transaction id is framed as it always was.
        let log = super::log::ReplicationLog::open(dir.path(), 1 << 20).unwrap();
        for seq in 1..=3u64 {
            log.append(&TxnRecord {
                origin: 1,
                seq,
                commit_hlc: seq * 1000,
                flags: 0,
                statements: vec![format!("INSERT INTO old_t (id) VALUES ({seq})")],
                tables: vec!["old_t".to_string()],
            })
            .unwrap();
        }
        log.flush().unwrap();
        let mut pairs = Vec::new();
        for (origin, seq) in [(2u64, 4u64), (2, 5), (3, 9)] {
            pairs.extend_from_slice(&origin.to_le_bytes());
            pairs.extend_from_slice(&seq.to_le_bytes());
        }
        std::fs::write(dir.path().join(super::log::STATE_FILE), pairs).unwrap();
    }

    let one = disk_node(dir.path(), 1, listener(), Vec::new()).await;
    assert_eq!(one.replication.local_seq(), 3);
    assert_eq!(one.replication.released_through(), 3);
    assert_eq!(one.replication.applied().high_water(2), 5);
    assert_eq!(one.replication.applied().high_water(3), 9);
    assert!(one.replication.log().unwrap().is_logged());

    run_ok(&one, "CREATE TABLE repl_new (id INTEGER)").await;
    let (records, _) = one.replication.log().unwrap().read_after(1, 0, 10, 1 << 20).unwrap();
    assert_eq!(records.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![1, 2, 3, 4]);

    for peer in one.replication.peers() {
        peer.stop();
    }
    drop(one);
    crate::core::wal::remove_for(&dir.path().to_string_lossy());
}

/// And with nothing there at all, which is how a node seeded from a restore
/// starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_with_no_replication_files_starts_from_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let one = disk_node(dir.path(), 1, listener(), Vec::new()).await;
    assert_eq!(one.replication.local_seq(), 0);
    run_ok(&one, "CREATE TABLE repl_new (id INTEGER)").await;
    assert_eq!(one.replication.released_through(), 1);
    drop(one);
    crate::core::wal::remove_for(&dir.path().to_string_lossy());
}

/// A peer that holds more of this node's stream than this node's own log
/// does: the node was restored to an earlier point, or lost its log. Nothing
/// is sent, because the peer would drop every new transaction numbered below
/// what it holds as one it already has. An operator who accepts the
/// difference says so, and the stream carries on above it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_ahead_of_this_nodes_log_is_not_sent_to_until_an_operator_adopts_it() {
    let (one, two) = pair().await;
    run_ok(&one, "CREATE TABLE repl_ahead (id INTEGER, name VARCHAR)").await;
    run_ok(&one, "INSERT INTO repl_ahead (id, name) VALUES (1, 'before')").await;
    settled(&one, &two).await;

    // Node two comes to hold node one's stream up to 50, as it would if node
    // one had been put back to an earlier point.
    let link = one.replication.peer("two").unwrap();
    link.set_paused(true);
    eventually("the link to drop", || async { !link.snapshot(0).connected }).await;
    two.replication.applied().set(1, 50).unwrap();
    link.set_paused(false);

    eventually("the link to refuse", || async {
        link.snapshot(0).state == super::peer::PeerState::NeedsReseed && link.ahead() == 50
    })
    .await;
    run_ok(&one, "INSERT INTO repl_ahead (id, name) VALUES (2, 'not sent')").await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        rows(&two, "SELECT id FROM repl_ahead WHERE 1 = 1").await.map(|r| r.len()),
        Some(1),
        "a transaction numbered below what the peer holds must not be offered as new"
    );
    assert!(
        super::sql::execute("REPLICATION ADOPT PEER 'nobody'", &one).await.is_err(),
        "an unknown peer"
    );

    let said = super::sql::execute("REPLICATION ADOPT PEER 'two'", &one).await.unwrap();
    assert!(said[0].contains("to 50"), "{said:?}");
    assert_eq!(one.replication.local_seq(), 50);
    assert!(
        super::sql::execute("REPLICATION ADOPT PEER 'two'", &one).await.is_err(),
        "nothing left to adopt"
    );

    run_ok(&one, "INSERT INTO repl_ahead (id, name) VALUES (3, 'after')").await;
    eventually("the stream to carry on above what the peer held", || async {
        rows(&two, "SELECT id FROM repl_ahead WHERE id = 3").await.map(|r| r.len()) == Some(1)
    })
    .await;
    assert_eq!(two.replication.applied().high_water(1), 51);
    assert_eq!(link.snapshot(0).state, super::peer::PeerState::Streaming);
}

/// A node that replicated, was backed up before its log moved on to further
/// files and archived after, restores to a copy whose replication files open
/// and hold its stream up to where it was archived. Every file of the log is
/// appended through the write-ahead log from its first byte, which is what
/// lets a restore rebuild one the snapshot never saw. (Written by the review
/// of this branch, which found that a log compacted by copying came back
/// empty.)
#[cfg(feature = "s3_backup")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replicating_node_restores_with_its_whole_stream() {
    use crate::core::backup::{BackupConfig, BackupService};
    use crate::core::backup::manifest::RecoveryTarget;
    use crate::core::backup::s3::{Credentials, Endpoint};
    let dir = tempfile::tempdir().unwrap();
    let (bound_one, bound_two) = (listener(), listener());
    let (port_one, port_two) = (bound_one.1, bound_two.1);
    super::test_support::init_globals();
    let (socket, port) = bound_one;
    let one = Arc::new(
        Database::try_new_named(&dir.path().to_string_lossy(), false, "repl_bk_one")
            .await
            .expect("the database should open"),
    );
    let mut config = ReplicationConfig {
        node_id: 1,
        listen_port: port,
        peers: vec![PeerConfig::new("two", format!("127.0.0.1:{port_two}"))],
        log_retention_bytes: 16 * 1024,
        ..Default::default()
    };
    brisk(&mut config);
    one.replication.set_config(config).expect("a valid configuration");
    one.replication.serve_on(socket);
    Database::start_replication(one.clone()).expect("replication should start");
    let two = node(2, bound_two, vec![PeerConfig::new("one", format!("127.0.0.1:{port_one}"))]).await;

    run_ok(&one, "CREATE TABLE repl_bk (id INTEGER, who VARCHAR)").await;
    for id in 0..5 {
        run_ok(&one, &format!("INSERT INTO repl_bk (id, who) VALUES ({id}, 'before')")).await;
    }
    settled(&one, &two).await;

    let config = BackupConfig {
        bucket: "rdbtest".to_string(),
        prefix: format!("review-repl/{:016x}", fastrand::u64(..)),
        region: "us-east-1".to_string(),
        endpoint: Endpoint::parse(&crate::core::backup::fake_s3::shared().endpoint()).unwrap(),
        credentials: Credentials { access_key: "fake".to_string(), secret_key: "fake".to_string(), session_token: None },
        path_style: true,
        snapshot_level: 3,
        segment_level: 3,
        compression_threads: 1,
        long_range: false,
        part_size: 5 * 1024 * 1024,
        upload_concurrency: 2,
        max_attempts: 3,
        snapshot_interval_secs: 0,
        wal_flush_secs: 0,
        wal_bundle_bytes: 0,
        rollover_interval_secs: 0,
        retain_snapshots: 0,
        staging_max_bytes: 0,
        incremental_min_bytes: 0,
    };
    let service = BackupService::connect(config.clone(), &dir.path().to_string_lossy()).await.expect("connect");
    service.snapshot_now().await.expect("snapshot");

    let pad = "x".repeat(400);
    for id in 5..200 {
        run_ok(&one, &format!("INSERT INTO repl_bk (id, who) VALUES ({id}, '{pad}')")).await;
    }
    settled(&one, &two).await;
    let names: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("db_replication."))
        .collect();
    eprintln!("live replication files: {names:?}");
    assert!(
        names.iter().any(|n| n.starts_with("db_replication.log.")),
        "the log moved on to another file after the snapshot: {names:?}"
    );
    let seq = one.replication.local_seq();
    service.archive_wal_now().await.expect("archive");

    let target = dir.path().parent().unwrap().join(format!("restored-{:x}", fastrand::u64(..)));
    let report = crate::core::backup::restore_from(&config, &target, RecoveryTarget::Latest).await.expect("restore");
    assert_eq!(report.records_unresolved, 0);
    let restored_names: Vec<String> = std::fs::read_dir(&target)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("db_replication."))
        .collect();
    eprintln!("restored replication files: {restored_names:?}");

    let copy = Arc::new(
        Database::try_new_named(&target.to_string_lossy(), false, "repl_restored")
            .await
            .expect("the restored copy opens"),
    );
    copy.open_replication_storage().expect("the restored replication files open");
    let log = copy.replication.log().expect("a log");
    assert_eq!(log.highest(1), seq, "the restored log holds the stream up to where it was archived");
    let n = rows(&copy, "SELECT id FROM repl_bk WHERE 1 = 1").await.unwrap().len();
    assert_eq!(n, 200);
    let _ = std::fs::remove_dir_all(&target);
}

/// The server opens replication's files at start, before the replication
/// flags have been applied, to settle what a crash left. What the log keeps
/// is still the configuration's to say: opened first with the default budget,
/// a restarted node grew one file to an eighth of 256 MiB whatever
/// `--replication-retention-mb` said.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_opened_before_the_configuration_takes_its_retention_from_it() {
    let dir = tempfile::tempdir().unwrap();
    super::test_support::init_globals();
    let database = Arc::new(
        Database::try_new_named(&dir.path().to_string_lossy(), false, "repl_keep")
            .await
            .expect("the database should open"),
    );
    // As `main` does when it finds the files: open first, configure after.
    database.open_replication_storage().unwrap();
    let mut config = ReplicationConfig {
        node_id: 1,
        listen_port: listener().1,
        log_retention_bytes: 64 * 1024,
        ..Default::default()
    };
    brisk(&mut config);
    database.replication.set_config(config).unwrap();
    database.replication.serve_on(listener().0);
    database.open_replication_storage().unwrap();
    database.replication.set_active(true);
    assert_eq!(database.replication.log().unwrap().retention(), 64 * 1024);

    run_ok(&database, "CREATE TABLE repl_keep (id INTEGER, body VARCHAR)").await;
    let body = "y".repeat(1000);
    for id in 0..200 {
        run_ok(&database, &format!("INSERT INTO repl_keep (id, body) VALUES ({id}, '{body}')")).await;
    }
    let held = database.replication.log().unwrap().bytes();
    assert!(held <= 64 * 1024 + 16 * 1024, "the log holds {held} bytes against a 64 KiB budget");
    drop(database);
    crate::core::wal::remove_for(&dir.path().to_string_lossy());
}

/// `REPLICATION CHECK` says two nodes hold the same rows when they do, and
/// names the table when they do not: a row missing, a row extra, and a row
/// that is there on both with different contents, none of which the
/// sequence numbers show.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_check_finds_rows_that_differ_between_two_nodes_that_report_in_step() {
    let (one, two) = pair().await;
    run_ok(&one, "CREATE TABLE repl_same (id INTEGER, name VARCHAR)").await;
    run_ok(&one, "CREATE TABLE repl_lost (id INTEGER, name VARCHAR)").await;
    run_ok(&one, "CREATE TABLE repl_changed (id INTEGER, name VARCHAR)").await;
    for table in ["repl_same", "repl_lost", "repl_changed"] {
        for id in 0..50 {
            run_ok(&one, &format!("INSERT INTO {table} (id, name) VALUES ({id}, 'row {id}')")).await;
        }
    }
    run_ok(&two, "INSERT INTO repl_same (id, name) VALUES (1000, 'from two')").await;
    settled(&one, &two).await;

    let peer = one.replication.peer("two").unwrap();
    let (verdict, lines) = super::check::check(&one, &peer, &[]).await.unwrap();
    assert!(verdict.in_step, "{lines:?}");
    assert_eq!(verdict.compared, 3, "{lines:?}");
    assert!(verdict.different.is_empty() && verdict.missing.is_empty() && verdict.unread.is_empty(), "{lines:?}");
    assert!(lines[1].contains("the same rows on both"), "{lines:?}");

    // Behind replication's back on node two, as a divergence would be: the
    // streams still report both nodes in step.
    two.replication.set_active(false);
    run_ok(&two, "DELETE FROM repl_lost WHERE id = 7").await;
    run_ok(&two, "UPDATE repl_changed SET name = 'not the same' WHERE id = 7").await;
    run_ok(&two, "CREATE TABLE repl_only_two (id INTEGER)").await;
    two.replication.set_active(true);
    assert_eq!(two.replication.applied().high_water(1), one.replication.local_seq());

    let (verdict, lines) = super::check::check(&one, &peer, &[]).await.unwrap();
    assert!(verdict.in_step, "{lines:?}");
    assert_eq!(verdict.different.len(), 2, "{lines:?}");
    assert!(verdict.different.iter().any(|d| d.starts_with("repl_lost: 50 rows here, 49 on two")), "{lines:?}");
    assert!(
        verdict.different.iter().any(|d| d.starts_with("repl_changed: 50 rows on both, and they are not the same rows")),
        "{lines:?}"
    );
    assert_eq!(verdict.missing, vec!["repl_only_two: on two, not here".to_string()], "{lines:?}");

    // One table by name, through the statement, from the other side.
    let said = super::sql::execute("REPLICATION CHECK PEER 'one' TABLE repl_changed", &two).await.unwrap();
    assert!(said.iter().any(|line| line.starts_with("DIFFERENT  repl_changed")), "{said:?}");
    let said = super::sql::execute("REPLICATION CHECK TABLE repl_same", &two).await.unwrap();
    assert!(said.iter().any(|line| line.contains("the same rows on both")), "{said:?}");
    assert!(super::sql::execute("REPLICATION CHECK PEER 'nobody'", &one).await.is_err());
}

/// A comparison made while one node is behind is reported as not
/// comparable, not as a difference.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_check_of_nodes_that_are_not_in_step_says_so() {
    let (one, two) = pair().await;
    run_ok(&one, "CREATE TABLE repl_behind (id INTEGER, name VARCHAR)").await;
    settled(&one, &two).await;

    // Node two stops hearing node one, which carries on writing.
    let link = one.replication.peer("two").unwrap();
    link.set_paused(true);
    run_ok(&one, "INSERT INTO repl_behind (id, name) VALUES (1, 'not there yet')").await;

    let peer = two.replication.peer("one").unwrap();
    let (verdict, lines) = super::check::check(&two, &peer, &[]).await.unwrap();
    assert!(!verdict.in_step, "{lines:?}");
    assert!(lines[0].contains("NOT in step"), "{lines:?}");

    link.set_paused(false);
    settled(&one, &two).await;
    let (verdict, lines) = super::check::check(&two, &peer, &[]).await.unwrap();
    assert!(verdict.in_step && verdict.different.is_empty(), "{lines:?}");
}

/// The same rows held in different bytes are the same rows. Two nodes can
/// both be right and lay a row out differently: what lies under a `NULL` is
/// whatever the last writer left, and a column dropped on one node at a
/// different point is still in that node's layout. A check that compared
/// stored bytes would raise the alarm on a healthy pair.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_check_compares_values_and_not_how_each_node_happens_to_store_them() {
    let (one, two) = pair().await;
    // Each node builds the same tables its own way, so nothing here travels.
    one.replication.set_active(false);
    two.replication.set_active(false);

    // A NULL written as one, against a NULL that was a value first.
    run_ok(&one, "CREATE TABLE repl_nulls (id INTEGER NOT NULL, name VARCHAR)").await;
    run_ok(&one, "INSERT INTO repl_nulls (id, name) VALUES (1, NULL)").await;
    run_ok(&one, "INSERT INTO repl_nulls (id, name) VALUES (2, 'kept')").await;
    run_ok(&two, "CREATE TABLE repl_nulls (id INTEGER NOT NULL, name VARCHAR)").await;
    run_ok(&two, "INSERT INTO repl_nulls (id, name) VALUES (1, 'was here once')").await;
    run_ok(&two, "INSERT INTO repl_nulls (id, name) VALUES (2, 'kept')").await;
    run_ok(&two, "UPDATE repl_nulls SET name = NULL WHERE id = 1").await;

    // A column that one node never had and the other had and dropped.
    run_ok(&one, "CREATE TABLE repl_shape (id INTEGER NOT NULL, name VARCHAR)").await;
    run_ok(&one, "INSERT INTO repl_shape (id, name) VALUES (1, 'ada')").await;
    run_ok(&two, "CREATE TABLE repl_shape (id INTEGER NOT NULL, junk VARCHAR, name VARCHAR)").await;
    run_ok(&two, "INSERT INTO repl_shape (id, junk, name) VALUES (1, 'in the way', 'ada')").await;
    run_ok(&two, "ALTER TABLE repl_shape DROP COLUMN junk").await;

    // And a real difference under a NULL-able column, which must still show:
    // a NULL here against an empty string there.
    run_ok(&one, "CREATE TABLE repl_really (id INTEGER NOT NULL, name VARCHAR)").await;
    run_ok(&one, "INSERT INTO repl_really (id, name) VALUES (1, NULL)").await;
    run_ok(&two, "CREATE TABLE repl_really (id INTEGER NOT NULL, name VARCHAR)").await;
    run_ok(&two, "INSERT INTO repl_really (id, name) VALUES (1, '')").await;

    one.replication.set_active(true);
    two.replication.set_active(true);
    let peer = one.replication.peer("two").unwrap();
    let (verdict, lines) = super::check::check(&one, &peer, &[]).await.unwrap();
    assert!(verdict.in_step, "{lines:?}");
    assert!(verdict.unread.is_empty() && verdict.missing.is_empty(), "{lines:?}");
    assert_eq!(verdict.different.len(), 1, "{lines:?}");
    assert!(verdict.different[0].starts_with("repl_really:"), "{lines:?}");
}
