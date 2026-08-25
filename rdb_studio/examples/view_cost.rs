//! Measures what one frame of the view tree costs.
//!
//! `view()` runs on every redraw, so anything it allocates is paid sixty times
//! a second, and anything it leaks is paid forever. This builds the tree in a
//! loop against a synthetic result set and reports the time and the resident
//! set, which is the only way to see either.
//!
//! ```sh
//! cargo run -p rdb_studio --release --example view_cost
//! ```

use rdb_studio::app::{Studio, Tab};
use rdb_studio::model::{Connection, Flavor, Grid, TlsMode};

/// Resident set in KiB, from the kernel rather than from an allocator hook.
fn rss_kib() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .and_then(|field| field.parse().ok())
        .unwrap_or(0);
    pages * 4
}

fn grid(rows: usize, columns: usize) -> Grid {
    Grid {
        columns: (0..columns).map(|c| format!("column_{c}")).collect(),
        rows: (0..rows)
            .map(|r| {
                (0..columns)
                    .map(|c| {
                        if c == 3 {
                            None
                        } else {
                            Some(format!("value {r}-{c} with some length to it"))
                        }
                    })
                    .collect()
            })
            .collect(),
        tag: format!("{rows} rows"),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let frames: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(600);

    // A real session, because `view` renders the connection screen without one
    // — which is not the screen worth measuring.
    let url = std::env::var("RDB_STUDIO_TEST_RDB")
        .expect("set RDB_STUDIO_TEST_RDB to a server to measure against");
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

    let (mut studio, _) = Studio::new(None);
    studio.session = Some(session);
    studio.data = grid(200, 12);
    studio.tab = Tab::Data;
    studio.selected_table = Some("measurements".to_string());

    // Populate every screen so each can be measured against real content.
    studio.tables = (0..60)
        .map(|i| rdb_studio::model::TableInfo {
            name: format!("table_{i}"),
            row_slots: 1000 + i,
            columns: 12,
            data_bytes: 4096 * (i + 1),
            index_bytes: 2048,
            total_bytes: 8192 * (i + 1),
            reclaimable_bytes: i * 7,
            primary_key: "id".into(),
            immutable: false,
            indexed: true,
        })
        .collect();
    studio.db_stats = (0..40)
        .map(|i| rdb_studio::model::Stat {
            metric: format!("filter_metric_{i}"),
            value: format!("{}", i * 1000),
            detail: "what this metric means, at some length".into(),
        })
        .collect();
    // PostgreSQL reports about this many settings, and the screen draws an
    // input for every hot one.
    studio.config = (0..350)
        .map(|i| rdb_studio::model::ConfigEntry {
            section: format!("section_{}", i / 40),
            key: format!("setting_{i}"),
            value: format!("value_{i}"),
            hot: i % 3 != 0,
            set_with: "ALTER SYSTEM SET, then pg_reload_conf()".into(),
            help: "a sentence about what this setting does".into(),
        })
        .collect();
    for series in 0..14 {
        let metric = rdb_studio::model::SERIES[series % rdb_studio::model::SERIES.len()].metric;
        for step in 0..600 {
            studio.metrics.series.entry(metric.to_string()).or_default().push_back(step as f64);
        }
    }
    studio.history = (0..80)
        .map(|i| rdb_studio::model::HistoryEntry {
            sql: format!("SELECT * FROM table_{i} WHERE id = {i} AND name = 'something'"),
            elapsed: std::time::Duration::from_micros(4300),
            at: std::time::Instant::now(),
            outcome: rdb_studio::model::RunOutcome::Ok { statements: 1, rows: 12, affected: 0 },
        })
        .collect();
    studio.results = vec![rdb_studio::model::Outcome::Rows(grid(200, 12))];
    studio.columns = (0..12)
        .map(|i| rdb_studio::model::ColumnInfo {
            position: i,
            name: format!("column_{i}"),
            data_type: "TEXT".into(),
            nullable: true,
            unique: false,
            primary_key: i == 0,
            default: String::new(),
            indexed: false,
            json_table: String::new(),
        })
        .collect();

    for tab in [
        Tab::Dashboard,
        Tab::Overview,
        Tab::Tables,
        Tab::Data,
        Tab::Query,
        Tab::Config,
    ] {
        studio.tab = tab;
        // One pass first, so lazily-initialised state is not charged to the
        // measurement.
        drop(studio.view());

        let before = rss_kib();
        let started = std::time::Instant::now();
        for _ in 0..frames {
            // Dropped immediately, exactly as the runtime does between frames.
            drop(studio.view());
        }
        let elapsed = started.elapsed();
        let after = rss_kib();

        println!(
            "{:<10} {:>6.2} ms/frame   RSS {:+6} KiB over {} frames ({:.3} KiB/frame)",
            tab.label(),
            elapsed.as_secs_f64() * 1000.0 / frames as f64,
            after as i64 - before as i64,
            frames,
            (after as i64 - before as i64) as f64 / frames as f64,
        );
    }
}
