//! Multi-row INSERT into a PRIMARY KEY table must cost the same per batch as
//! the table grows. A uniqueness check that looks at existing rows one by one
//! makes each batch O(table) and a bulk load quadratic: `disconnect_cancel`'s
//! load of 2M rows took 37 minutes that way.
//!
//! The regular test is bounded (a few hundred thousand rows, a time cap) and
//! asserts the last batches are not much slower than the first ones. The
//! ignored one prints the curve out to two million rows.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor};
use std::time::{Duration, Instant};

const BATCH: i64 = 5_000;

async fn load(rows: i64, report_every: i64, cap: Duration) -> Vec<(i64, Duration)> {
    let server = TestServer::start().await.expect("start");
    load_into(&server, rows, report_every, cap).await
}

async fn load_into(
    server: &TestServer,
    rows: i64,
    report_every: i64,
    cap: Duration,
) -> Vec<(i64, Duration)> {
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");
    conn.execute(AssertSqlSafe(
        "CREATE TABLE g (id INT PRIMARY KEY, tag TEXT NOT NULL, n BIGINT NOT NULL)",
    ))
    .await
    .expect("create");
    let started = Instant::now();
    let mut curve = Vec::new();
    let mut loaded = 0i64;
    while loaded < rows && started.elapsed() < cap {
        let values = (loaded..loaded + BATCH)
            .map(|i| format!("({i}, 'tag{}', {i})", i % 977))
            .collect::<Vec<_>>()
            .join(",");
        let t = Instant::now();
        conn.execute(AssertSqlSafe(format!("INSERT INTO g (id, tag, n) VALUES {values}")))
            .await
            .unwrap_or_else(|e| panic!("insert at {loaded}: {e}"));
        let took = t.elapsed();
        loaded += BATCH;
        if loaded % report_every == 0 || loaded == BATCH {
            curve.push((loaded, took));
        }
    }
    curve
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_costs_the_same_as_the_table_grows() {
    // 150k rows, bounded in time. A per-row check linear in the table took
    // the 150k batch from 67 ms to 8.2 s (release); flat, it stays within
    // noise of the cheapest batch.
    let curve = load(150_000, 25_000, Duration::from_secs(300)).await;
    for (rows, took) in &curve {
        println!("{rows:>8} rows: batch of {BATCH} took {took:?}");
    }
    let (rows, _) = curve.last().copied().expect("one batch");
    assert!(rows >= 150_000, "the load did not finish within its time cap ({rows} rows)");
    // The cheapest batch against the dearest: a flat curve stays within a
    // small factor (single batches are noisy); a per-row cost linear in the
    // table makes the last batch ~30x the first.
    let cheapest = curve.iter().map(|(_, t)| *t).min().unwrap();
    let (at, dearest) = curve.iter().copied().max_by_key(|(_, t)| *t).unwrap();
    assert!(
        dearest < cheapest * 10 + Duration::from_millis(500),
        "a batch at {at} rows took {dearest:?} against {cheapest:?} for the cheapest: \
         the per-row cost grows with the table"
    );
}

#[ignore = "measurement: prints the per-batch cost out to 2M rows"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_cost_curve() {
    for (rows, took) in load(2_000_000, 200_000, Duration::from_secs(1800)).await {
        println!("{rows:>8} rows: batch of {BATCH} took {took:?}");
    }
}

#[ignore = "measurement: the same curve on a file-backed server, to 500k rows"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_backed_insert_cost_curve() {
    let root = format!("/var/tmp/insert_growth_cost_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&root);
    let server = TestServer::start_file_backed(&root).await.expect("start");
    for (rows, took) in load_into(&server, 500_000, 50_000, Duration::from_secs(1200)).await {
        println!("{rows:>8} rows: batch of {BATCH} took {took:?}");
    }
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}
