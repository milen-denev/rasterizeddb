//! The policy, and the task end to end over a heartbeat-shaped table.

use super::*;

#[test]
fn a_table_is_due_past_the_floor_and_the_ratio() {
    let m = |slots, live| Measure { slots, live };
    // The heartbeat: 8 live rows in 38 030 slots.
    assert!(m(38_030, 8).due(20_000, 8));
    // Below the floor, whatever the ratio.
    assert!(!m(19_999, 1).due(20_000, 8));
    // A table that mostly inserts: slots near its live rows.
    assert!(!m(472_468, 400_000).due(20_000, 8));
    assert!(m(472_468, 1_012).due(20_000, 8));
    // An empty table counts as one live row, never divides by zero.
    assert!(m(20_000, 0).due(20_000, 8));
}

/// A table updated in place of growing reaches the threshold, the task
/// compacts it through `VACUUM FULL`, and every row reads back the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_update_heavy_table_is_compacted_and_keeps_its_rows() {
    use crate::core::rql::lexer_s1::recognize_query_purpose;
    let dir = tempfile::tempdir().expect("dir");
    let database = rclite::Arc::new(Database::new(dir.path().to_str().expect("utf-8")).await);
    let run = |sql: &str| {
        let database = database.clone();
        let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("{sql}"));
        async move { crate::core::rql::executor::execute(purpose, database).await }
    };
    assert_ne!(run("CREATE TABLE hb (id INT PRIMARY KEY, n INT, s TEXT)").await.first(), Some(&3));
    assert_ne!(run("INSERT INTO hb (id, n) VALUES (1, 0), (2, 0), (3, 0)").await.first(), Some(&3));
    for i in 0..600 {
        // A text column relocates the row, a new slot each time, as every
        // update does on a server taking concurrent traffic (the in-place
        // path needs no other transaction running anywhere).
        let r = run(&format!("UPDATE hb SET n = {i}, s = 'beat {i}'")).await;
        assert_ne!(r.first(), Some(&3), "{}", String::from_utf8_lossy(&r));
    }
    // A complete sweep measures the live rows.
    let table = match database.relation("hb").expect("hb") {
        crate::core::database::Relation::Plain(t) => t,
        _ => panic!("plain"),
    };
    table.vacuum_now().await.expect("sweep");
    let before = measure(&database, "hb").await.expect("measured");
    assert_eq!(before.live, 3);
    assert!(before.slots >= 1_800, "{before:?}");
    assert!(before.due(1_000, 8));

    let mut last = HashMap::new();
    // The policy below the defaults, as a small table needs.
    assert_eq!(tick_with(&database, &mut last, small_policy()).await, 1, "the due table was compacted");
    let after = measure(&database, "hb").await.expect("measured");
    assert!(after.slots < 100, "{after:?}");
    // Within the cooldown, not again.
    assert_eq!(tick_with(&database, &mut last, small_policy()).await, 0);
    let r = run("SELECT id, n FROM hb ORDER BY id").await;
    assert_eq!(r.first(), Some(&2));
    let rows = crate::core::row::row::vec_into_rows(&r[1..]).expect("rows");
    assert_eq!(rows.len(), 3);
}

/// The policy the tests compact small tables under.
fn small_policy() -> Policy {
    Policy {
        min_slots: 1_000,
        ratio: 8,
        cooldown: Duration::from_secs(600),
    }
}

/// A table whose lock is held is skipped, not waited for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_busy_table_is_skipped() {
    use crate::core::rql::lexer_s1::recognize_query_purpose;
    let dir = tempfile::tempdir().expect("dir");
    let database = rclite::Arc::new(Database::new(dir.path().to_str().expect("utf-8")).await);
    let purpose = recognize_query_purpose("CREATE TABLE busy (id INT PRIMARY KEY)").expect("create");
    crate::core::rql::executor::execute(purpose, database.clone()).await;
    // A block's LOCK TABLE holds ACCESS EXCLUSIVE to the block's end.
    let tx = database.transactions.begin();
    let lock = recognize_query_purpose("LOCK TABLE busy IN ACCESS EXCLUSIVE MODE").expect("lock");
    let locked = crate::core::rql::executor::execute_in_txn(lock, database.clone(), tx).await;
    assert_ne!(locked.first(), Some(&3), "{}", String::from_utf8_lossy(&locked));
    let started = Instant::now();
    let outcome = compact(&database, "busy").await;
    let waited = started.elapsed();
    database.abort_transaction(tx);
    assert!(matches!(outcome, Outcome::Busy), "a held table is reported busy");
    assert!(waited < Duration::from_secs(2), "it did not queue behind the holder: {waited:?}");
}

async fn run(database: &rclite::Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap_or_else(|| panic!("{sql}"));
    let r = crate::core::rql::executor::execute(purpose, database.clone()).await;
    assert_ne!(r.first(), Some(&3), "{sql}: {}", String::from_utf8_lossy(&r));
    r
}

fn partitioned(database: &Database, name: &str) -> rclite::Arc<crate::core::partition::table::PartitionedTable> {
    match database.relation(name) {
        Some(crate::core::database::Relation::Partitioned(t)) => t,
        _ => panic!("{name} is not a partitioned table"),
    }
}

/// A partitioned table updated in place of growing: each of its partitions
/// becomes due on its own, one pass compacts each alone (docs/kv-partitions.md
/// §3), the cooldown holds per partition, and every row reads back the same.
/// Returns the database for a caller's own checks.
async fn update_heavy_partitions(dir: &std::path::Path, place: Option<&std::path::Path>) -> rclite::Arc<Database> {
    let database = rclite::Arc::new(Database::new(dir.to_str().expect("utf-8")).await);
    #[cfg(all(feature = "storage_pools", feature = "kv_storage"))]
    if let Some(kv) = place {
        for sql in [
            format!(
                "STORAGE ADD STORE logs DIRECTORY WITH (PATH = '{}', PREFIX = 'logs', CHUNK_SIZE = '16KiB', \
                 PAGE_SIZE = '4KiB', MAX_DELTAS = '4', COMPACT_CHUNK_BYTES = '8KiB')",
                kv.display()
            ),
            "STORAGE PLACE TABLE ph AT 'kv:logs'".to_string(),
        ] {
            crate::core::storage::sql::execute(&sql, &database).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }
    #[cfg(not(all(feature = "storage_pools", feature = "kv_storage")))]
    assert!(place.is_none(), "a store needs storage_pools and kv_storage");
    run(&database, "CREATE TABLE ph (id INT PRIMARY KEY, n INT, s TEXT) PARTITION BY HASH (id) PARTITIONS 2").await;
    run(&database, "INSERT INTO ph (id, n) VALUES (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0)").await;
    // The hash puts 2 of the 6 rows in one partition: 600 versions each
    // pass the 1 000-slot floor there too.
    for i in 0..600 {
        run(&database, &format!("UPDATE ph SET n = {i}, s = 'beat {i}'")).await;
    }
    let pt = partitioned(&database, "ph");
    // A complete sweep of each partition measures its live rows.
    for (_, part) in pt.open_partitions_now() {
        part.vacuum_now().await.expect("sweep");
    }
    let before = measure_partitions(&database, "ph").await;
    assert_eq!(before.len(), 2, "both partitions are open and measured: {before:?}");
    for (pid, m) in &before {
        assert!(m.due(1_000, 8), "partition {pid}: {m:?}");
    }
    // The plain measure does not see a partitioned table.
    assert!(measure(&database, "ph").await.is_none());

    // Every version is on the store before the pass (its flush is 200 ms).
    #[cfg(all(feature = "storage_pools", feature = "kv_storage"))]
    if let Some(kv) = place {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let bytes = tree_bytes(kv);
        assert!(bytes > 2 * STORE_BOUND, "the store holds only {bytes} bytes before the compaction");
    }
    let mut last = HashMap::new();
    assert_eq!(tick_with(&database, &mut last, small_policy()).await, 2, "each due partition was compacted");
    let after = measure_partitions(&database, "ph").await;
    assert_eq!(after.len(), 2, "{after:?}");
    for (pid, m) in &after {
        assert!(m.slots < 100, "partition {pid} after: {m:?}");
    }
    // Within the cooldown, neither again.
    assert_eq!(tick_with(&database, &mut last, small_policy()).await, 0);

    let r = run(&database, "SELECT id, n, s FROM ph ORDER BY id").await;
    assert_eq!(r.first(), Some(&2));
    let rows = crate::core::row::row::vec_into_rows(&r[1..]).expect("rows");
    assert_eq!(rows.len(), 6);
    let r = run(&database, "SELECT count(*) FROM ph WHERE n = 599 AND s = 'beat 599'").await;
    let rows = crate::core::row::row::vec_into_rows(&r[1..]).expect("rows");
    assert_eq!(rows.len(), 1);
    database
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_heavy_partitions_are_compacted_one_by_one() {
    let dir = tempfile::tempdir().expect("dir");
    let database = update_heavy_partitions(dir.path(), None).await;
    // Writes after the compaction land and read back.
    run(&database, "UPDATE ph SET n = 1000 WHERE id = 3").await;
    let r = run(&database, "SELECT id FROM ph WHERE n = 1000").await;
    assert_eq!(crate::core::row::row::vec_into_rows(&r[1..]).expect("rows").len(), 1);
}

/// Bytes held by every object under `dir`.
#[cfg(all(feature = "storage_pools", feature = "kv_storage"))]
fn tree_bytes(dir: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else { return 0 };
    entries
        .flatten()
        .map(|e| {
            let p = e.path();
            if p.is_dir() { tree_bytes(&p) } else { e.metadata().map_or(0, |m| m.len()) }
        })
        .sum()
}

/// What the store may hold after the compaction: the chunk straddling each
/// of the four files' new end keeps its tail (`truncate_to` deletes only the
/// chunks wholly past it), one 16 KiB chunk per file, plus metadata and the
/// last deltas.
#[cfg(all(feature = "storage_pools", feature = "kv_storage"))]
const STORE_BOUND: u64 = 4 * 16 * 1024 + 8 * 1024;

/// The same on a key/value store: the compaction's truncation deletes the
/// chunk objects past each partition's new end, so the store holds at most a
/// chunk per file.
#[cfg(all(feature = "storage_pools", feature = "kv_storage"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_heavy_partitions_on_a_store_give_their_objects_back() {
    let dir = tempfile::tempdir().expect("dir");
    let kv = tempfile::tempdir().expect("kv");
    let database = update_heavy_partitions(dir.path(), Some(kv.path())).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let bytes = tree_bytes(kv.path());
        if bytes <= STORE_BOUND {
            break;
        }
        assert!(Instant::now() < deadline, "the store still holds {bytes} bytes 30 s after the compaction");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(database);
    crate::core::wal::remove_for(dir.path().to_str().expect("utf-8"));
}

/// A due partition whose table is locked is skipped, not waited for; so is
/// one an open transaction wrote, whose slots are still in its write set
/// (its ROW EXCLUSIVE holds off the compaction's ACCESS EXCLUSIVE).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_busy_partitioned_table_is_skipped() {
    let dir = tempfile::tempdir().expect("dir");
    let database = rclite::Arc::new(Database::new(dir.path().to_str().expect("utf-8")).await);
    run(&database, "CREATE TABLE pb (id INT PRIMARY KEY, n INT) PARTITION BY HASH (id) PARTITIONS 2").await;
    run(&database, "INSERT INTO pb (id, n) VALUES (1, 0), (2, 0), (3, 0), (4, 0)").await;
    let parts = partitioned(&database, "pb").open_partitions_now();
    assert_eq!(parts.len(), 2);
    for holder in [
        "LOCK TABLE pb IN ACCESS EXCLUSIVE MODE",
        "UPDATE pb SET n = 1",
    ] {
        let tx = database.transactions.begin();
        let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(holder).expect(holder);
        let held = crate::core::rql::executor::execute_in_txn(purpose, database.clone(), tx).await;
        assert_ne!(held.first(), Some(&3), "{holder}: {}", String::from_utf8_lossy(&held));
        for (pid, _) in &parts {
            let started = Instant::now();
            let outcome = compact_part(&database, "pb", *pid).await;
            let waited = started.elapsed();
            assert!(matches!(outcome, Outcome::Busy), "{holder}: partition {pid} is reported busy");
            assert!(waited < Duration::from_secs(2), "{holder}: it did not queue behind the holder: {waited:?}");
        }
        database.abort_transaction(tx);
    }
    // Free again, each partition compacts, and the aborted update left no row
    // changed.
    for (pid, _) in &parts {
        assert!(matches!(compact_part(&database, "pb", *pid).await, Outcome::Done));
    }
    let r = run(&database, "SELECT id FROM pb WHERE n = 0").await;
    assert_eq!(crate::core::row::row::vec_into_rows(&r[1..]).expect("rows").len(), 4);
}
