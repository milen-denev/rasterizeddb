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
    let runs = RUNS.load(Ordering::Relaxed);
    // The policy as set below the defaults, as a small table needs.
    let _ = crate::AUTO_COMPACT_MIN_SLOTS.set(1_000);
    if crate::auto_compact_min_slots() == 1_000 {
        tick(&database, &mut last).await;
        assert_eq!(RUNS.load(Ordering::Relaxed), runs + 1, "the due table was compacted");
        let after = measure(&database, "hb").await.expect("measured");
        assert!(after.slots < 100, "{after:?}");
        // Within the cooldown, not again.
        tick(&database, &mut last).await;
        assert_eq!(RUNS.load(Ordering::Relaxed), runs + 1);
    } else {
        // Another test set the floor first; compact through the statement the
        // task runs, which is what is under test either way.
        assert!(matches!(compact(&database, "hb").await, Outcome::Done));
    }
    let r = run("SELECT id, n FROM hb ORDER BY id").await;
    assert_eq!(r.first(), Some(&2));
    let rows = crate::core::row::row::vec_into_rows(&r[1..]).expect("rows");
    assert_eq!(rows.len(), 3);
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
