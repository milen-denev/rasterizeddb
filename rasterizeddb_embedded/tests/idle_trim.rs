//! Freed memory goes back to the operating system once the embedded engine
//! is idle. glibc only: elsewhere there is no trimmer to run.
#![cfg(all(target_os = "linux", target_env = "gnu"))]

use std::time::{Duration, Instant};

use rasterizeddb_embedded::{Builder, engine};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_engine_trims_once_a_burst() {
    let db = Builder::in_memory().worker_threads(2).start().await.unwrap();
    let client = db.connect().await.unwrap();
    client.execute("CREATE TABLE t (id BIGINT, body TEXT)", &[]).await.unwrap();
    for i in 0..20i64 {
        client.execute(&format!("INSERT INTO t VALUES ({i}, 'row {i}')"), &[]).await.unwrap();
    }
    let rows = client.query("SELECT COUNT(*) FROM t", &[]).await.unwrap();
    assert_eq!(rows.iter().next().unwrap().get::<i64>(0).unwrap(), 20);

    // A trim follows between one and two ticks after the last statement.
    let tick = engine::idle_trim::TICK;
    let deadline = Instant::now() + tick * 4;
    while engine::idle_trim::trims() == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(engine::idle_trim::trims(), 1, "no trim within {:?} of the last statement", tick * 4);

    // Nothing ran since: an idle engine is not trimmed again and again.
    tokio::time::sleep(tick * 2 + Duration::from_millis(500)).await;
    assert_eq!(engine::idle_trim::trims(), 1);

    drop(client);
    db.shutdown().await.unwrap();
}
