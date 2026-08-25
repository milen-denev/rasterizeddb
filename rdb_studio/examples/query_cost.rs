//! Splits a query's cost into the exchange and the client's own work.
//!
//! The question this answers is "is the console adding to it": it times the
//! whole `Session::query` — network, server, driver decode and the `Grid` the
//! console builds — so the figure can be put beside `psql`'s for the same
//! statement against the same server. Whatever the difference is, that is what
//! the console costs.
//!
//! ```sh
//! RDB_STUDIO_TEST_RDB=postgresql://user@host:5432/db?sslmode=disable \
//!   cargo run -p rdb_studio --release --example query_cost -- "SELECT 1"
//! ```

use rdb_studio::model::{Connection, Flavor, TlsMode};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let sql = std::env::args().nth(1).unwrap_or_else(|| "SELECT 1".to_string());
    let runs: usize = std::env::args().nth(2).and_then(|a| a.parse().ok()).unwrap_or(20);

    let url = std::env::var("RDB_STUDIO_TEST_RDB").expect("set RDB_STUDIO_TEST_RDB");
    let rest = url.trim_start_matches("postgresql://");
    let (credentials, rest) = rest.split_once('@').unwrap_or(("", rest));
    let (user, password) = credentials.split_once(':').unwrap_or((credentials, ""));
    let (host_port, tail) = rest.split_once('/').unwrap_or((rest, ""));
    let (host, port) = host_port.split_once(':').unwrap_or((host_port, "5432"));

    let session = rdb_studio::conn::connect(Connection {
        name: "bench".into(),
        flavor: Flavor::Rasterized,
        host: host.into(),
        port: port.parse().unwrap_or(5432),
        database: tail.split('?').next().unwrap_or("postgres").into(),
        user: user.into(),
        password: password.into(),
        save_password: false,
        tls: TlsMode::Disable,
    })
    .await
    .expect("could not connect");

    // A warm-up that is not measured: the first exchange on a connection pays
    // for whatever the server does once.
    let _ = session.query(&sql).await;

    let mut timings = Vec::with_capacity(runs);
    let mut rows = 0usize;
    for _ in 0..runs {
        let started = std::time::Instant::now();
        let grid = session.query(&sql).await.expect("query failed");
        timings.push(started.elapsed());
        rows = grid.rows.len();
    }

    timings.sort();
    let total: std::time::Duration = timings.iter().sum();
    println!(
        "{} runs, {} rows: min {:.2} ms, median {:.2} ms, max {:.2} ms, mean {:.2} ms",
        runs,
        rows,
        timings[0].as_secs_f64() * 1000.0,
        timings[runs / 2].as_secs_f64() * 1000.0,
        timings[runs - 1].as_secs_f64() * 1000.0,
        total.as_secs_f64() * 1000.0 / runs as f64,
    );
}
