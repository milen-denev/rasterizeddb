//! The benchmark runner, against a real server.
//!
//! Skipped unless pointed at one, like `live_dialect`:
//!
//! ```sh
//! RDB_STUDIO_TEST_BENCH=postgresql://user:pw@127.0.0.1:5432/db \
//!   cargo test -p rdb_studio --test live_bench -- --nocapture
//! ```
//!
//! Every run here is a second or two of `SELECT`s, and the stop test is
//! bounded by its own timeout, so a broken runner fails rather than hangs.

use std::time::Duration;

use iced::futures::StreamExt;
use rdb_studio::bench::{self, Event, Phase, Plan, Protocol, Report};
use rdb_studio::model::{Connection, Flavor, TlsMode};

fn profile() -> Option<Connection> {
    let url = std::env::var("RDB_STUDIO_TEST_BENCH").ok()?;
    let rest = url
        .trim_start_matches("postgresql://")
        .trim_start_matches("postgres://");
    let (credentials, rest) = rest.split_once('@').unwrap_or(("", rest));
    let (user, password) = credentials.split_once(':').unwrap_or((credentials, ""));
    let (host_port, rest) = rest.split_once('/').unwrap_or((rest, ""));
    let (host, port) = host_port.split_once(':').unwrap_or((host_port, "5432"));
    let database = rest.split('?').next().unwrap_or("postgres");
    Some(Connection {
        name: "bench".to_string(),
        flavor: Flavor::Rasterized,
        host: host.to_string(),
        port: port.parse().unwrap_or(5432),
        database: database.to_string(),
        user: if user.is_empty() { "postgres".to_string() } else { user.to_string() },
        password: password.to_string(),
        save_password: false,
        tls: TlsMode::Disable,
    })
}

fn plan(profile: Connection, sql: &str, protocol: Protocol) -> Plan {
    Plan {
        sql: sql.to_string(),
        connections: 3,
        duration: Duration::from_secs(2),
        warmup: Duration::from_millis(300),
        protocol,
        profile,
    }
}

/// Drives a run to its end, returning the phases it reported and the result.
async fn drive(
    plan: Plan,
    stop: tokio::sync::watch::Receiver<bool>,
) -> (Vec<Phase>, Result<Box<Report>, String>) {
    let mut phases = Vec::new();
    let mut events = Box::pin(bench::run(plan, stop));
    while let Some(event) = events.next().await {
        match event {
            Event::Progress(p) => {
                if phases.last() != Some(&p.phase) {
                    phases.push(p.phase);
                }
            }
            Event::Finished(result) => return (phases, result),
        }
    }
    panic!("the stream ended without a report");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_reports_consistent_figures() {
    let Some(profile) = profile() else { return };
    for protocol in Protocol::ALL {
        let (_stop, stopped) = tokio::sync::watch::channel(false);
        let (phases, result) = drive(plan(profile.clone(), "SELECT 1", protocol), stopped).await;
        let report = result.unwrap_or_else(|e| panic!("{} run failed: {}", protocol, e));
        println!("{}", report.to_text());

        assert_eq!(phases.first(), Some(&Phase::Connecting), "{:?}", phases);
        assert!(phases.contains(&Phase::Measuring), "{:?}", phases);
        assert!(report.completed > 10, "{} queries in two seconds", report.completed);
        assert_eq!(report.errors, 0, "{:?}", report.first_error);
        assert!(!report.stopped);
        assert_eq!(report.connections, 3);
        assert_eq!(report.rows, report.completed, "SELECT 1 is one row each time");

        // The window was two seconds; finishing the in-flight statements can
        // add a little, never take any away.
        assert!(report.elapsed >= Duration::from_secs(2), "{:?}", report.elapsed);
        assert!(report.elapsed < Duration::from_secs(3), "{:?}", report.elapsed);
        assert_eq!(report.per_second.len(), 2);
        // Every statement lands in some second, and only the trailing
        // part-second is dropped, so the bars cannot exceed the total.
        assert!(report.per_second.iter().sum::<u64>() <= report.completed);

        let l = &report.latency;
        assert!(l.min <= l.p50 && l.p50 <= l.p90 && l.p90 <= l.p95);
        assert!(l.p95 <= l.p99 && l.p99 <= l.p999 && l.p999 <= l.max);
        assert!(l.min <= l.mean && l.mean <= l.max);
        assert_eq!(
            report.distribution.iter().map(|b| b.2).sum::<u64>(),
            report.completed,
            "the histogram must hold every statement"
        );

        // Little's law, loosely: three clients each waiting `mean` per
        // statement cannot finish more than 3 / mean of them a second.
        let ceiling = 3.0 / l.mean.as_secs_f64();
        assert!(report.qps() <= ceiling * 1.05, "{} q/s over {}", report.qps(), ceiling);
    }
}

/// A typing mistake is one error, before any connection is benchmarked.
#[tokio::test(flavor = "multi_thread")]
async fn a_bad_statement_fails_up_front() {
    let Some(profile) = profile() else { return };
    let (_stop, stopped) = tokio::sync::watch::channel(false);
    let (phases, result) =
        drive(plan(profile, "SELEC 1", Protocol::Simple), stopped).await;
    assert!(result.is_err(), "a parse error must fail the run");
    assert!(!phases.contains(&Phase::Measuring), "{:?}", phases);
}

/// STOP ends a long run promptly and still reports what finished.
#[tokio::test(flavor = "multi_thread")]
async fn stop_ends_a_run_early() {
    let Some(profile) = profile() else { return };
    let mut long = plan(profile, "SELECT 1", Protocol::Simple);
    long.duration = Duration::from_secs(600);
    long.warmup = Duration::ZERO;
    let (stop, stopped) = tokio::sync::watch::channel(false);

    let started = std::time::Instant::now();
    let run = tokio::spawn(drive(long, stopped));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    stop.send(true).unwrap();
    let (_, result) = tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("STOP did not end the run")
        .unwrap();
    let report = result.unwrap();
    assert!(report.stopped);
    assert!(report.completed > 0);
    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(report.elapsed < Duration::from_secs(5), "{:?}", report.elapsed);
}
