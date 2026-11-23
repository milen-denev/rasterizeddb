//! Where the cost of `ORDER BY … LIMIT` actually goes.
//!
//! Diagnostic, not a regression test: it prints timings so the paths can be
//! compared. The deployed report was 8.1s and ~7 GB to return three rows from
//! 940k, against 0.1s for the same shape ordered by a NOT NULL column.

use sqlx::{AssertSqlSafe, Connection, Executor};
use rasterizeddb_compat::TestServer;
use std::time::Instant;

/// Peak resident set of this process, in MiB.
///
/// The server runs in-process here, so its high-water mark is this process's.
/// Peak rather than current: an allocator need not return freed pages to the
/// OS, so what a query *needed* only shows in the high-water mark.
fn peak_rss_mib() -> f64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            if let Some(kb) = rest.split_whitespace().next() {
                return kb.parse::<f64>().unwrap_or(0.0) / 1024.0;
            }
        }
    }
    0.0
}

const ROWS: usize = 120_000;

// A measurement harness, not a regression test — it prints timings and peak RSS
// and asserts nothing, and seeding 120k rows costs ~140s. Run it deliberately:
//   cargo test -p rasterizeddb_compat --test sort_cost -- --ignored --nocapture
#[ignore = "measurement harness; run deliberately with --ignored --nocapture"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn where_order_by_limit_spends_its_time() {
    let server = TestServer::start().await.expect("start");
    let mut conn = sqlx::PgConnection::connect(&server.url()).await.expect("connect");

    // `ord` is NOT NULL and `maybe` is nullable, and they carry the same
    // values — so any difference between them is the path, not the data.
    conn.execute(AssertSqlSafe(
        "CREATE TABLE big (id INT PRIMARY KEY, ord BIGINT NOT NULL, maybe BIGINT, tag TEXT NOT NULL)",
    ))
    .await
    .expect("create");

    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(2_000) {
        let values = chunk
            .iter()
            .map(|i| {
                let v = (i * 7919) % ROWS;
                format!("({i}, {v}, {v}, 'tag{v}')")
            })
            .collect::<Vec<_>>()
            .join(",");
        conn.execute(AssertSqlSafe(format!(
            "INSERT INTO big (id, ord, maybe, tag) VALUES {values}"
        )))
        .await
        .expect("insert");
    }

    // The comparison that matters: the same ordering with and without a cap.
    // A capped one is what the worker-side top-N bounds; an uncapped one still
    // has to hold every row, and is the shape the capped one used to have.
    for (label, sql) in [
        ("narrow + NOT NULL, lim 3 ", "SELECT id FROM big ORDER BY ord DESC LIMIT 3"),
        ("wide + NOT NULL, limit 3 ", "SELECT id, ord, maybe, tag FROM big ORDER BY ord DESC LIMIT 3"),
        ("order by text, limit 3   ", "SELECT id FROM big ORDER BY tag DESC LIMIT 3"),
        ("wide + nullable, limit 3 ", "SELECT id, ord, maybe, tag FROM big ORDER BY maybe DESC LIMIT 3"),
        ("wide, NO limit (uncapped)", "SELECT id, ord, maybe, tag FROM big ORDER BY ord DESC"),
    ] {
        let before = peak_rss_mib();
        let started = Instant::now();
        let rows = sqlx::query(AssertSqlSafe(sql)).fetch_all(&mut conn).await;
        let elapsed = started.elapsed();
        let grew = peak_rss_mib() - before;
        match rows {
            Ok(r) => println!(
                "  {label} {:>9.1?}  peak +{grew:>7.1} MiB  {} row(s)",
                elapsed,
                r.len()
            ),
            Err(e) => println!("  {label} {:>9.1?}  ERROR {}", elapsed, e.to_string().lines().next().unwrap_or("")),
        }
    }
}
