//! A multi-row INSERT into a table with a primary key costs the same per batch
//! whether the table holds ten thousand rows or a few hundred thousand.
//!
//! The uniqueness check has to find whether the key already exists. Done by
//! scanning, that is O(table) per row and O(n²) to load a table — which is
//! how a 2M-row load in `disconnect_cancel` took 37 minutes and then lost its
//! connection. Bounded: the table stops at `ROWS`, and each batch has a
//! generous ceiling, so a regression fails here in seconds instead of hanging
//! the suite.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor};
use std::time::{Duration, Instant};

const BATCH: i64 = 5_000;
const ROWS: i64 = 300_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_key_inserts_do_not_slow_down_as_the_table_grows() {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    conn.execute("CREATE TABLE p (id INT PRIMARY KEY, tag TEXT NOT NULL, n BIGINT NOT NULL)")
        .await
        .expect("create");

    let mut times: Vec<Duration> = Vec::new();
    let mut start = 0i64;
    while start < ROWS {
        let values = (start..start + BATCH)
            .map(|i| format!("({i}, 'tag{}', {i})", i % 977))
            .collect::<Vec<_>>()
            .join(",");
        let t = Instant::now();
        conn.execute(AssertSqlSafe(format!("INSERT INTO p (id, tag, n) VALUES {values}")))
            .await
            .unwrap_or_else(|e| panic!("insert at {start}: {e}"));
        let took = t.elapsed();
        assert!(
            took < Duration::from_secs(20),
            "a {BATCH}-row batch at {start} rows took {took:?}"
        );
        times.push(took);
        start += BATCH;
    }
    let early: Duration = times[1..6].iter().sum::<Duration>() / 5;
    let late: Duration = times[times.len() - 5..].iter().sum::<Duration>() / 5;
    println!("5k-row batch: early {early:?}, late (at ~{ROWS} rows) {late:?}");
    // Linear growth per row would make the late batches ~60x the early ones
    // here; a per-row cost independent of the table size keeps them close.
    assert!(
        late < early * 4 + Duration::from_millis(200),
        "batches slowed from {early:?} to {late:?} as the table grew"
    );
}
