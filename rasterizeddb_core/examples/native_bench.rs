//! Point queries through the real native listener, pooled against shared
//! connections.
//!
//! The transport's own benchmark (`rastcp/examples/echo_bench.rs`) answers
//! requests with an echo, which shows what the transport costs and nothing
//! about what that is worth next to a query. This runs the database's own
//! handler behind the same listener the server binary starts.
//!
//! ```text
//! native_bench server <dir> <port> <threads>
//! native_bench setup  <host:port> <rows>
//! native_bench pooled <host:port> <callers> <seconds> <threads> <rows>
//! native_bench shared <host:port> <callers> <seconds> <threads> <rows> <connections>
//! ```
//!
//! Server and clients are separate processes so each can be confined to its
//! own cores.

use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rasterizeddb_core::client::DbClient;
use rasterizeddb_core::core::database::{Database, NativeListen};
use rasterizeddb_core::core::processor::concurrent_processor::ENABLE_CACHE;
use rasterizeddb_core::core::support_types::QueryExecutionResult;
use rasterizeddb_core::native_auth::NativeAuthConfig;
use rasterizeddb_core::{BATCH_SIZE, MAX_PERMITS_THREADS};
use rclite::Arc;

fn runtime(threads: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.max(1))
        .enable_all()
        .build()
        .expect("runtime")
}

fn arg<T: std::str::FromStr>(args: &[String], index: usize, default: T) -> T {
    args.get(index)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let target = args.get(2).cloned().unwrap_or_default();

    match args.get(1).map(String::as_str) {
        Some("server") => server(target, arg(&args, 3, 61999u16), arg(&args, 4, 4usize)),
        Some("setup") => setup(target, arg(&args, 3, 1000u64)),
        Some(mode @ ("pooled" | "shared")) => load(
            mode == "shared",
            target,
            arg(&args, 3, 64usize),
            arg(&args, 4, 5u64),
            arg(&args, 5, 4usize),
            arg(&args, 6, 1000u64),
            arg(&args, 7, 2usize),
        ),
        _ => {
            eprintln!("usage: native_bench server <dir> <port> <threads>");
            eprintln!("       native_bench setup  <host:port> <rows>");
            eprintln!(
                "       native_bench pooled <host:port> <callers> <seconds> <threads> <rows>"
            );
            eprintln!(
                "       native_bench shared <host:port> <callers> <seconds> <threads> <rows> <connections>"
            );
            std::process::exit(2);
        }
    }
}

fn server(dir: String, port: u16, threads: usize) {
    let _ = MAX_PERMITS_THREADS.set(threads.max(1));
    let _ = BATCH_SIZE.set(10_000);
    let _ = ENABLE_CACHE.set(false);

    runtime(threads).block_on(async move {
        let database = Arc::new(Database::new_with_backend(&dir, false).await);
        println!("serving {dir} on 127.0.0.1:{port} with {threads} worker thread(s)");

        Database::start_db_with_auth_on(
            database,
            NativeAuthConfig::default(),
            NativeListen {
                addr: "127.0.0.1".to_string(),
                port,
            },
        )
        .await
        .expect("the native listener");
    });
}

fn setup(addr: String, rows: u64) {
    runtime(2).block_on(async move {
        let client = DbClient::new(Some(&addr)).await.expect("connect");

        let created = client
            .execute_query("CREATE TABLE bench (id UBIGINT, name VARCHAR, salary REAL, age INTEGER);")
            .await;
        println!("create table: {}", describe(&created));

        for id in 1..=rows {
            let insert = format!(
                "INSERT INTO bench (id, name, salary, age) VALUES ({id}, 'employee {id}', {}.5, {});",
                1000 + id % 9000,
                20 + id % 40
            );
            if let Err(e) = client.execute_query(&insert).await {
                panic!("insert {id} failed: {e}");
            }
        }

        let indexed = client.execute_query("CREATE INDEX bench_id ON bench (id);").await;
        println!("create index: {}", describe(&indexed));

        let probe = client
            .execute_query(&format!("SELECT id, salary, age, name FROM bench WHERE id = {rows}"))
            .await;
        println!("probe: {}", describe(&probe));
    });
}

fn describe(result: &std::io::Result<QueryExecutionResult>) -> String {
    match result {
        Ok(QueryExecutionResult::Ok) => "ok".to_string(),
        Ok(QueryExecutionResult::RowsAffected(n)) => format!("{n} row(s) affected"),
        Ok(QueryExecutionResult::RowsResult(bytes)) => format!("rows, {} bytes", bytes.len()),
        Ok(QueryExecutionResult::Error(message)) => format!("server error: {message}"),
        Err(e) => format!("transport error: {e}"),
    }
}

/// Microsecond buckets up to 100 ms; anything slower lands in the last one.
const BUCKETS: usize = 100_000;

fn percentile(histogram: &[u64], total: u64, fraction: f64) -> usize {
    let target = (total as f64 * fraction).ceil() as u64;
    let mut seen = 0u64;

    for (micros, count) in histogram.iter().enumerate() {
        seen += count;
        if seen >= target {
            return micros;
        }
    }

    histogram.len() - 1
}

fn load(
    shared: bool,
    addr: String,
    callers: usize,
    seconds: u64,
    threads: usize,
    rows: u64,
    connections: usize,
) {
    runtime(threads).block_on(async move {
        let client = if shared {
            DbClient::multiplexed(Some(&addr), connections, Duration::from_secs(30), None).await
        } else {
            DbClient::with_pool_settings(Some(&addr), callers, callers).await
        };
        let client = StdArc::new(client.expect("connect"));

        let stop = StdArc::new(AtomicBool::new(false));
        let measuring = StdArc::new(AtomicBool::new(false));
        let total = StdArc::new(AtomicU64::new(0));
        let failed = StdArc::new(AtomicU64::new(0));
        let mut tasks = Vec::with_capacity(callers);

        for caller in 0..callers {
            let client = client.clone();
            let stop = stop.clone();
            let measuring = measuring.clone();
            let total = total.clone();
            let failed = failed.clone();

            tasks.push(tokio::spawn(async move {
                let mut histogram = vec![0u64; BUCKETS];
                let mut done = 0u64;
                // A different key every time, and a different sequence for
                // every caller, without a random number generator in the loop.
                let mut key = (caller as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;

                while !stop.load(Ordering::Relaxed) {
                    key ^= key << 13;
                    key ^= key >> 7;
                    key ^= key << 17;
                    let id = 1 + key % rows;
                    let query = format!("SELECT id, salary, age, name FROM bench WHERE id = {id}");

                    let started = Instant::now();
                    let answered = matches!(
                        client.execute_query(&query).await,
                        Ok(QueryExecutionResult::RowsResult(_))
                    );

                    if measuring.load(Ordering::Relaxed) {
                        if answered {
                            let micros = started.elapsed().as_micros() as usize;
                            histogram[micros.min(BUCKETS - 1)] += 1;
                            done += 1;
                        } else {
                            failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                total.fetch_add(done, Ordering::Relaxed);
                histogram
            }));
        }

        tokio::time::sleep(Duration::from_millis(2000)).await;
        measuring.store(true, Ordering::Relaxed);
        let started = Instant::now();
        tokio::time::sleep(Duration::from_secs(seconds)).await;
        measuring.store(false, Ordering::Relaxed);
        let elapsed = started.elapsed().as_secs_f64();
        stop.store(true, Ordering::Relaxed);

        let mut histogram = vec![0u64; BUCKETS];
        for task in tasks {
            for (slot, count) in histogram.iter_mut().zip(task.await.expect("task")) {
                *slot += count;
            }
        }

        let done = total.load(Ordering::Relaxed);
        let (open, of) = client.get_pool_stats();

        println!(
            "{} queries_per_sec={:.0} p50_us={} p99_us={} failed={} callers={callers} connections={open}/{of}",
            if shared { "shared" } else { "pooled" },
            done as f64 / elapsed,
            percentile(&histogram, done, 0.50),
            percentile(&histogram, done, 0.99),
            failed.load(Ordering::Relaxed),
        );
    });
}
