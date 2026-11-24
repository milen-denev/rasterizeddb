//! Every database replicates, not only the node's default one.
//!
//! Two nodes, each a real catalogue on disk with its own listener. A
//! database is created on one, and the other is expected to create it, take
//! its schema and rows, send its own back, keep it apart from the default
//! database's, and drop it when the first one drops it. Each database's
//! stream is its own: its positions are its own, as its log is.

use std::time::Duration;

use rclite::Arc;

use crate::core::database::Database;
use crate::core::database::catalog::DatabaseCatalog;
use crate::core::rql::executor::execute;
use crate::core::rql::lexer_s1::recognize_query_purpose;

use super::config::{PeerConfig, ReplicationConfig};
use super::test_support::{init_globals, listener};

const DEFAULT: &str = "postgres";

async fn node(
    node_id: u64,
    root: &std::path::Path,
    bound: (std::net::TcpListener, u16),
    peers: Vec<PeerConfig>,
) -> Arc<DatabaseCatalog> {
    init_globals();
    let (socket, port) = bound;
    let catalog = DatabaseCatalog::open(Some(&root.to_string_lossy()), DEFAULT).await.expect("a catalogue");
    let default = catalog.get(DEFAULT).expect("the default database");
    let mut config = ReplicationConfig { node_id, listen_port: port, peers, ..Default::default() };
    config.listen_addr = "127.0.0.1".to_string();
    config.batch_linger = Duration::from_micros(200);
    config.reconnect_base = Duration::from_millis(25);
    config.reconnect_max = Duration::from_millis(200);
    config.connect_timeout = Duration::from_secs(2);
    config.request_timeout = Duration::from_secs(5);
    config.ack_timeout = Duration::from_secs(5);
    default.replication.set_config(config).expect("a valid configuration");
    default.replication.serve_on(socket);
    Database::start_replication_with(default, Some(catalog.clone())).expect("replication should start");
    catalog
}

async fn run_ok(database: &Arc<Database>, sql: &str) {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("cannot parse `{sql}`"));
    let response = execute(purpose, database.clone()).await;
    assert_ne!(response.first(), Some(&3), "`{sql}` failed: {}", String::from_utf8_lossy(&response[1..]));
}

/// The `v` of every row of `notes`, sorted; `None` while there is no table.
async fn notes(catalog: &Arc<DatabaseCatalog>, database: &str) -> Option<Vec<String>> {
    let database = catalog.get(database)?;
    let purpose = recognize_query_purpose("SELECT id, v FROM notes WHERE 1 = 1")?;
    let response = execute(purpose, database.clone()).await;
    if response.first() != Some(&2) {
        return None;
    }
    let rows = crate::core::row::row::vec_into_rows(&response[1..]).ok()?;
    let mut out: Vec<String> = rows
        .iter()
        .filter_map(|row| row.columns.get(1))
        .map(|c| String::from_utf8_lossy(c.data.into_slice()).trim_end_matches('\0').to_string())
        .collect();
    out.sort();
    Some(out)
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !check().await {
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for {what}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| v.to_string()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_database_created_on_one_node_replicates_with_everything_in_it() {
    let (dir_one, dir_two) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (bound_one, bound_two) = (listener(), listener());
    let (port_one, port_two) = (bound_one.1, bound_two.1);
    let one = node(1, dir_one.path(), bound_one, vec![PeerConfig::new("two", format!("127.0.0.1:{port_two}"))]).await;
    let two = node(2, dir_two.path(), bound_two, vec![PeerConfig::new("one", format!("127.0.0.1:{port_one}"))]).await;

    // The default database replicates as it always did, and a table of the
    // same name in another database stays apart from it.
    let one_default = one.get(DEFAULT).unwrap();
    run_ok(&one_default, "CREATE TABLE notes (id INTEGER, v VARCHAR)").await;
    run_ok(&one_default, "INSERT INTO notes (id, v) VALUES (1, 'default')").await;
    eventually("the default database's row on node two", || async {
        notes(&two, DEFAULT).await == Some(strings(&["default"]))
    })
    .await;

    one.create("sales").await.expect("CREATE DATABASE");
    eventually("node two to create sales", || async { two.get("sales").is_some() }).await;

    let one_sales = one.get("sales").unwrap();
    run_ok(&one_sales, "CREATE TABLE notes (id INTEGER, v VARCHAR)").await;
    run_ok(&one_sales, "INSERT INTO notes (id, v) VALUES (1, 'sales from one')").await;
    eventually("sales' row on node two", || async {
        notes(&two, "sales").await == Some(strings(&["sales from one"]))
    })
    .await;

    // Both ways: node two writes into its sales, and node one gets it.
    let two_sales = two.get("sales").unwrap();
    run_ok(&two_sales, "INSERT INTO notes (id, v) VALUES (2, 'sales from two')").await;
    eventually("node two's sales row on node one", || async {
        notes(&one, "sales").await == Some(strings(&["sales from one", "sales from two"]))
    })
    .await;

    // Apart: the default databases hold only their own row.
    assert_eq!(notes(&one, DEFAULT).await, Some(strings(&["default"])));
    assert_eq!(notes(&two, DEFAULT).await, Some(strings(&["default"])));

    // Each stream its own: sales' two records (CREATE TABLE, INSERT) on
    // node one, against the default's three (CREATE TABLE, INSERT, and the
    // CREATE DATABASE it carried).
    assert_eq!(one_sales.replication.local_seq(), 2);
    assert_eq!(one_default.replication.local_seq(), 3);
    assert_eq!(two_sales.replication.applied().high_water(1), 2);
    assert_eq!(two.get(DEFAULT).unwrap().replication.applied().high_water(1), 3);
    // Nothing came back: node two originated one sales record, its own row.
    assert_eq!(two_sales.replication.local_seq(), 1);
    assert_eq!(two.get(DEFAULT).unwrap().replication.local_seq(), 0);

    drop((one_sales, two_sales, one_default));
    one.drop_database("sales", DEFAULT).await.expect("DROP DATABASE");
    eventually("node two to drop sales", || async { two.get("sales").is_none() }).await;
    assert!(one.get("sales").is_none());
    // Not created again by anything still in flight.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(two.get("sales").is_none() && one.get("sales").is_none());

    drop((one, two));
    for dir in [dir_one.path(), dir_two.path()] {
        crate::core::wal::remove_for(&dir.to_string_lossy());
        crate::core::wal::remove_for(&dir.join("sales").to_string_lossy());
    }
}

/// A greeting that names no database is the default database's, as every
/// greeting from a node before this was; one naming a database the node does
/// not have is refused, and the link keeps trying until it has it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_link_for_a_database_the_peer_does_not_have_waits_for_it() {
    let (dir_one, dir_two) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (bound_one, bound_two) = (listener(), listener());
    let (port_one, port_two) = (bound_one.1, bound_two.1);
    let one = node(1, dir_one.path(), bound_one, vec![PeerConfig::new("two", format!("127.0.0.1:{port_two}"))]).await;
    let two = node(2, dir_two.path(), bound_two, vec![PeerConfig::new("one", format!("127.0.0.1:{port_one}"))]).await;

    // Made on node one before replication knew of it: not recorded, so node
    // two never hears of it. Its link to node two is refused, and keeps
    // trying; its writes stay in its log.
    let local = one.create_unrecorded("local").await.expect("a database");
    let config = one.get(DEFAULT).unwrap().replication.config();
    Database::start_routed_replication(local.clone(), "local", &config).expect("replication starts");
    run_ok(&local, "CREATE TABLE notes (id INTEGER, v VARCHAR)").await;
    run_ok(&local, "INSERT INTO notes (id, v) VALUES (1, 'waits')").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(two.get("local").is_none());

    // Once node two has it (as a base copy would give it), the link is
    // taken and the stream arrives from its start.
    let theirs = two.create_unrecorded("local").await.expect("a database");
    let config = two.get(DEFAULT).unwrap().replication.config();
    Database::start_routed_replication(theirs, "local", &config).expect("replication starts");
    eventually("the waiting stream to arrive", || async {
        notes(&two, "local").await == Some(strings(&["waits"]))
    })
    .await;

    drop((local, one, two));
    for dir in [dir_one.path(), dir_two.path()] {
        crate::core::wal::remove_for(&dir.to_string_lossy());
        crate::core::wal::remove_for(&dir.join("local").to_string_lossy());
    }
}

async fn pair(dir_one: &std::path::Path, dir_two: &std::path::Path) -> (Arc<DatabaseCatalog>, Arc<DatabaseCatalog>) {
    let (bound_one, bound_two) = (listener(), listener());
    let (port_one, port_two) = (bound_one.1, bound_two.1);
    let one = node(1, dir_one, bound_one, vec![PeerConfig::new("two", format!("127.0.0.1:{port_two}"))]).await;
    let two = node(2, dir_two, bound_two, vec![PeerConfig::new("one", format!("127.0.0.1:{port_one}"))]).await;
    (one, two)
}

fn release(dirs: &[&std::path::Path], names: &[&str]) {
    for dir in dirs {
        crate::core::wal::remove_for(&dir.to_string_lossy());
        for name in names {
            crate::core::wal::remove_for(&dir.join(name).to_string_lossy());
        }
    }
}

/// A stop between a catalogue change and its record leaves the change on
/// this node alone; the marker written before the change says so, and the
/// next start records it. For a CREATE, a DROP, and a DROP the stop
/// interrupted before the database was gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_catalogue_change_a_crash_left_unrecorded_is_recorded_at_the_next_start() {
    use crate::core::database::catalog::test_hooks::crash_before_record;
    let (dir_one, dir_two) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (one, two) = pair(dir_one.path(), dir_two.path()).await;

    crash_before_record("crashed");
    one.create("crashed").await.expect("CREATE DATABASE");
    let marker = dir_one.path().join("crashed").join("db_replication.pending_create");
    assert!(marker.exists(), "the CREATE's marker outlives the crash");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(two.get("crashed").is_none(), "nothing was recorded, so node two has not heard of it");

    // What the next start runs.
    one.reconcile_replication().await;
    eventually("node two to create the database", || async { two.get("crashed").is_some() }).await;
    assert!(!marker.exists(), "recorded: the marker is gone");
    let mine = one.get("crashed").unwrap();
    run_ok(&mine, "CREATE TABLE notes (id INTEGER, v VARCHAR)").await;
    run_ok(&mine, "INSERT INTO notes (id, v) VALUES (1, 'after the crash')").await;
    eventually("its rows on node two", || async {
        notes(&two, "crashed").await == Some(strings(&["after the crash"]))
    })
    .await;
    drop(mine);

    // A DROP that stopped before its record.
    crash_before_record("crashed");
    one.drop_database("crashed", DEFAULT).await.expect("DROP DATABASE");
    let tombstone = dir_one.path().join("db_replication.pending_drop.crashed");
    assert!(tombstone.exists(), "the DROP's tombstone outlives the crash");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(two.get("crashed").is_some(), "nothing was recorded, so node two keeps it");
    one.reconcile_replication().await;
    eventually("node two to drop the database", || async { two.get("crashed").is_none() }).await;
    assert!(!tombstone.exists());

    // A DROP the stop interrupted before the database went: finished, then
    // recorded.
    one.create("half").await.expect("CREATE DATABASE");
    eventually("node two to create half", || async { two.get("half").is_some() }).await;
    std::fs::File::create(dir_one.path().join("db_replication.pending_drop.half")).unwrap();
    one.reconcile_replication().await;
    assert!(one.get("half").is_none(), "the interrupted DROP is finished");
    eventually("node two to drop half", || async { two.get("half").is_none() }).await;

    // A database with no marker is never announced: one made before
    // replication knew of it waits for a base copy rather than being
    // created empty on the peers.
    one.create_unrecorded("quiet").await.expect("a database");
    one.reconcile_replication().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(two.get("quiet").is_none());

    drop((one, two));
    release(&[dir_one.path(), dir_two.path()], &["crashed", "half", "quiet"]);
}

/// REPLICATION DISABLE on the default database stops every database, as
/// ENABLE starts them; SHOW REPLICATION STATUS and STATS report each other
/// database on rows of their own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disable_and_enable_cover_every_database_and_status_reports_each() {
    let (dir_one, dir_two) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (one, two) = pair(dir_one.path(), dir_two.path()).await;
    one.create("sales").await.expect("CREATE DATABASE");
    eventually("node two to create sales", || async { two.get("sales").is_some() }).await;
    let default = one.get(DEFAULT).unwrap();
    let sales = one.get("sales").unwrap();
    run_ok(&sales, "CREATE TABLE notes (id INTEGER, v VARCHAR)").await;
    eventually("sales' table on node two", || async { notes(&two, "sales").await.is_some() }).await;

    let status = super::sql::execute("SHOW REPLICATION STATUS", &default).await.unwrap();
    let line = status.iter().find(|l| l.starts_with("database sales:")).unwrap_or_else(|| panic!("{status:#?}"));
    assert!(line.contains("enabled") && line.contains("local sequence 1") && line.contains("two "), "{line}");
    let stats = super::sql::execute_structured("SHOW REPLICATION STATS", &default).unwrap();
    let rows = crate::core::row::row::vec_into_rows(&stats[1..]).unwrap();
    let metrics: Vec<String> =
        rows.iter().map(|r| String::from_utf8_lossy(r.columns[0].data.into_slice()).to_string()).collect();
    for metric in ["database.sales.active", "database.sales.local_sequence", "database.sales.peers_connected"] {
        assert!(metrics.iter().any(|m| m == metric), "{metric} in {metrics:?}");
    }

    let disabled = super::sql::execute("REPLICATION DISABLE", &default).await.unwrap();
    assert!(disabled[0].contains("1 other"), "{disabled:?}");
    assert!(!default.replication.is_active() && !sales.replication.is_active());
    assert!(sales.replication.peers().is_empty());
    // A write now is this node's alone.
    run_ok(&sales, "INSERT INTO notes (id, v) VALUES (1, 'while disabled')").await;

    super::sql::execute("REPLICATION ENABLE", &default).await.unwrap();
    assert!(default.replication.is_active() && sales.replication.is_active());
    run_ok(&sales, "INSERT INTO notes (id, v) VALUES (2, 'after enable')").await;
    eventually("the write after ENABLE on node two", || async {
        notes(&two, "sales").await.is_some_and(|rows| rows.contains(&"after enable".to_string()))
    })
    .await;

    drop((default, sales, one, two));
    release(&[dir_one.path(), dir_two.path()], &["sales"]);
}
