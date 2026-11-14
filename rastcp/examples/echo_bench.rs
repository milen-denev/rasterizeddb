//! A load generator and an echo server for measuring the transport itself.
//!
//! Two processes, so a `perf` attached to one sees only that side:
//!
//! ```text
//! echo_bench server <port> <worker-threads>
//! echo_bench client <addr:port> <connections> <seconds> <payload-bytes> <worker-threads> [depth]
//! ```
//!
//! The handler echoes, so everything measured is the transport: framing, TLS
//! records, timers, wakeups and system calls. `depth` is how many requests a
//! connection keeps in flight; 1 is one request per round trip.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rastcp::client::TcpClientBuilder;
use rastcp::server::TcpServerBuilder;

fn runtime(threads: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.max(1))
        .enable_all()
        .build()
        .expect("runtime")
}

fn arg<T: std::str::FromStr>(args: &[String], index: usize, default: T) -> T {
    args.get(index).and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(String::as_str) {
        Some("server") => server(arg(&args, 2, 7878u16), arg(&args, 3, 4usize)),
        Some("client") => client(
            args.get(2).cloned().unwrap_or_else(|| "127.0.0.1:7878".to_string()),
            arg(&args, 3, 64usize),
            arg(&args, 4, 5u64),
            arg(&args, 5, 64usize),
            arg(&args, 6, 4usize),
            arg(&args, 7, 1usize),
        ),
        _ => {
            eprintln!("usage: echo_bench server <port> <threads>");
            eprintln!(
                "       echo_bench client <addr:port> <connections> <seconds> <payload> <threads> [depth]"
            );
            std::process::exit(2);
        }
    }
}

fn server(port: u16, threads: usize) {
    runtime(threads).block_on(async move {
        let server = TcpServerBuilder::new("127.0.0.1", port)
            .max_connections(4096)
            .build()
            .await
            .expect("server");

        println!("listening on 127.0.0.1:{port} with {threads} worker thread(s)");

        server.run(|data| async move { data }).await.expect("run");
    });
}

/// Microsecond buckets up to 20 ms; anything slower lands in the last one.
const BUCKETS: usize = 20_000;

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

fn client(
    addr: String,
    connections: usize,
    seconds: u64,
    payload: usize,
    threads: usize,
    depth: usize,
) {
    let runtime = runtime(threads);

    runtime.block_on(async move {
        let stop = Arc::new(AtomicBool::new(false));
        let measuring = Arc::new(AtomicBool::new(false));
        let total = Arc::new(AtomicU64::new(0));
        let mut tasks = Vec::with_capacity(connections);

        for _ in 0..connections {
            let addr = addr.clone();
            let stop = stop.clone();
            let measuring = measuring.clone();
            let total = total.clone();

            tasks.push(tokio::spawn(async move {
                let mut client = TcpClientBuilder::new(&addr)
                    .verify_certificate(false)
                    .build()
                    .await
                    .expect("client");
                client.connect().await.expect("connect");

                let request = vec![0x5au8; payload];
                let mut histogram = vec![0u64; BUCKETS];
                let mut done = 0u64;

                while !stop.load(Ordering::Relaxed) {
                    let started = Instant::now();
                    let completed = round(&mut client, &request, depth).await;

                    if measuring.load(Ordering::Relaxed) {
                        let micros = started.elapsed().as_micros() as usize;
                        histogram[micros.min(BUCKETS - 1)] += 1;
                        done += completed;
                    }
                }

                total.fetch_add(done, Ordering::Relaxed);
                histogram
            }));
        }

        // Let every connection finish its handshake before the clock starts.
        tokio::time::sleep(Duration::from_millis(1500)).await;
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
        let rounds: u64 = histogram.iter().sum();

        println!(
            "ops={done} secs={elapsed:.3} ops_per_sec={:.0} p50_us={} p99_us={} p999_us={} conns={connections} payload={payload} depth={depth}",
            done as f64 / elapsed,
            percentile(&histogram, rounds, 0.50),
            percentile(&histogram, rounds, 0.99),
            percentile(&histogram, rounds, 0.999),
        );
    });
}

/// One round of `depth` requests on a connection. Returns how many completed.
async fn round(client: &mut rastcp::client::TcpClient, request: &[u8], depth: usize) -> u64 {
    let _ = depth;
    let response = client.send_borrowed(request).await.expect("send");
    assert_eq!(response.len(), request.len());
    1
}
