//! Throughput and latency of the broker's native path.
//!
//! ```text
//! cargo run --release --example broker_bench -- ADDR USER PASSWORD \
//!     [records=1000000] [size=100] [partitions=6] [ack=written|durable] [inflight=200000] [batch=262144] [linger_ms=5]
//! ```
//!
//! Creates a fresh topic, produces `records` records of `size` bytes through
//! one batching [`Producer`] (records spread over the partitions), then
//! consumes them all with an ordered subscription from the earliest offset.
//! Prints the rates and the send-to-ack latency percentiles.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rasterizeddb_client::broker::*;

fn arg<T: std::str::FromStr>(args: &[String], name: &str, default: T) -> T {
    args.iter()
        .find_map(|a| a.strip_prefix(&format!("{name}=")).and_then(|v| v.parse().ok()))
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let (addr, user, pw) = (args[1].clone(), args[2].clone(), args[3].clone());
    let records: u64 = arg(&args, "records", 1_000_000);
    let size: usize = arg(&args, "size", 100);
    let partitions: u32 = arg(&args, "partitions", 6);
    let inflight: usize = arg(&args, "inflight", 200_000);
    let batch: usize = arg(&args, "batch", 256 * 1024);
    let linger: u64 = arg(&args, "linger_ms", 5);
    let ack = match arg(&args, "ack", "written".to_string()).as_str() {
        "durable" => Ack::Durable,
        _ => Ack::Written,
    };
    let durability = match arg(&args, "durability", "async".to_string()).as_str() {
        "sync" => Durability::Sync,
        "memory" => Durability::Memory,
        _ => Durability::Async,
    };
    let broker = Broker::connect(&addr, Options::password(&user, &pw)).await?;
    let name = format!("bench-{}", std::process::id());
    let topic = broker
        .create_topic(&name, &TopicConfig { partitions, durability, ..Default::default() }, false)
        .await?;

    let producer = broker.producer(
        topic.clone(),
        ProducerOptions { batch_bytes: batch, linger: Duration::from_millis(linger), ack, idempotent: false },
    );
    let value = vec![b'x'; size];
    let lat: Arc<std::sync::Mutex<Vec<u32>>> = Arc::new(std::sync::Mutex::new(Vec::with_capacity(records as usize)));
    let sem = Arc::new(tokio::sync::Semaphore::new(inflight));
    let errors = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..records {
        let permit = sem.clone().acquire_owned().await?;
        let t0 = Instant::now();
        let fut = producer.send(None, &value, &[]);
        let (lat, errors) = (lat.clone(), errors.clone());
        handles.push(tokio::spawn(async move {
            match fut.await {
                Ok(_) => lat.lock().unwrap().push(t0.elapsed().as_micros() as u32),
                Err(_) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            drop(permit);
        }));
        if handles.len() >= 100_000 {
            for h in handles.drain(..) {
                h.await?;
            }
        }
    }
    producer.flush();
    for h in handles {
        h.await?;
    }
    let took = start.elapsed();
    let mut l = lat.lock().unwrap().clone();
    l.sort_unstable();
    let pct = |p: f64| l.get(((l.len() as f64 - 1.0) * p) as usize).copied().unwrap_or(0) as f64 / 1000.0;
    println!(
        "native produce: {records} x {size} B in {:.2}s = {:.0} rec/s, {:.1} MB/s; ack={ack:?} durability={durability:?}; \
         latency ms p50 {:.2} p99 {:.2} p99.9 {:.2}; errors {}",
        took.as_secs_f64(),
        records as f64 / took.as_secs_f64(),
        (records as f64 * size as f64) / took.as_secs_f64() / 1e6,
        pct(0.5),
        pct(0.99),
        pct(0.999),
        errors.load(Ordering::Relaxed)
    );

    // Consume everything back, pushed.
    let start = Instant::now();
    let config = SubConfig { mode: SubMode::Ordered, start: Start::Earliest, ..Default::default() };
    let mut sub = broker.subscribe(&topic, "bench-reader", config, 500_000).await?;
    let mut n = 0u64;
    let mut bytes = 0u64;
    while n < records {
        match tokio::time::timeout(Duration::from_secs(10), sub.next()).await {
            Ok(Some(Ok(d))) => {
                for r in d.records() {
                    n += 1;
                    bytes += r.value.len() as u64;
                }
            }
            _ => break,
        }
    }
    let took = start.elapsed();
    println!(
        "native consume: {n} records in {:.2}s = {:.0} rec/s, {:.1} MB/s",
        took.as_secs_f64(),
        n as f64 / took.as_secs_f64(),
        bytes as f64 / took.as_secs_f64() / 1e6
    );
    drop(sub);
    broker.delete_topic(&name, true).await?;
    Ok(())
}
