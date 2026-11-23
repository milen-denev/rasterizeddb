//! Cost of `LocalStorageProvider`'s hot atomics, padded or packed.
//!
//! Readers do what a row fetch does — a short `read_data_into_buffer` at a
//! random offset of a resident file — while, in the `mixed` mode, one writer
//! stages appends, which bumps the staged-length counters every read loads.
//! That is the case packing could lose: read-mostly fields sharing a line with
//! counters another core keeps writing.
//!
//! Run on tmpfs, release:
//!   cargo run --release -p rasterizeddb_core --example storage_padding_bench -- <dir> [readers] [secs]
//! The file it writes is bounded: 4 MiB of base data, and the writer stops at
//! 32 MiB staged.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rasterizeddb_core::core::storage_providers::file_sync::LocalStorageProvider;
use rasterizeddb_core::core::storage_providers::traits::StorageIO;

const BASE: usize = 4 << 20;
const STAGED_CAP: u64 = 32 << 20;

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("a directory, on tmpfs");
    let readers: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(8);
    let secs: u64 = args.next().map(|s| s.parse().unwrap()).unwrap_or(2);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(readers + 1)
        .enable_all()
        .build()
        .unwrap();
    let _ = rasterizeddb_core::MAX_PERMITS_THREADS.set(4);

    rt.block_on(async move {
        println!(
            "size_of::<LocalStorageProvider>() = {}",
            std::mem::size_of::<LocalStorageProvider>()
        );
        for mode in ["read-only", "mixed"] {
            let sub = format!("{dir}/{mode}");
            let _ = std::fs::remove_dir_all(&sub);
            std::fs::create_dir_all(&sub).unwrap();
            let io = Arc::new(LocalStorageProvider::new(&sub, Some("bench.db")).await);
            let base: Vec<u8> = (0..BASE).map(|i| (i % 251) as u8).collect();
            io.append_data(&base, true).await;

            let stop = Arc::new(AtomicBool::new(false));
            let done = Arc::new(AtomicU64::new(0));
            let mut tasks = Vec::new();
            for r in 0..readers {
                let io = io.clone();
                let stop = stop.clone();
                let done = done.clone();
                tasks.push(tokio::spawn(async move {
                    let mut x: u64 = 0x9E37_79B9_7F4A_7C15 ^ r as u64;
                    let mut buf = [0u8; 64];
                    let mut n = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        for _ in 0..256 {
                            x ^= x << 13;
                            x ^= x >> 7;
                            x ^= x << 17;
                            let mut at = x % (BASE as u64 - 64);
                            io.read_data_into_buffer(&mut at, &mut buf).await.unwrap();
                            n += 1;
                        }
                        tokio::task::yield_now().await;
                    }
                    done.fetch_add(n, Ordering::Relaxed);
                }));
            }
            let writer = if mode == "mixed" {
                let io = io.clone();
                let stop = stop.clone();
                // Paced to spread STAGED_CAP over the whole run, so the
                // counters keep moving under the readers until the end.
                let per_append = Duration::from_secs_f64(secs as f64 * 64.0 / STAGED_CAP as f64);
                Some(tokio::spawn(async move {
                    let row = [0xCDu8; 64];
                    let mut staged = 0u64;
                    let begun = Instant::now();
                    while !stop.load(Ordering::Relaxed) && staged < STAGED_CAP {
                        io.append_data(&row, false).await;
                        staged += 64;
                        let due = per_append * (staged / 64) as u32;
                        while begun.elapsed() < due && !stop.load(Ordering::Relaxed) {
                            tokio::task::yield_now().await;
                        }
                    }
                    staged
                }))
            } else {
                None
            };

            let started = Instant::now();
            tokio::time::sleep(Duration::from_secs(secs)).await;
            stop.store(true, Ordering::Relaxed);
            for t in tasks {
                t.await.unwrap();
            }
            let elapsed = started.elapsed().as_secs_f64();
            let staged = match writer {
                Some(w) => w.await.unwrap(),
                None => 0,
            };
            let reads = done.load(Ordering::Relaxed);
            println!(
                "{mode:>9}: {:>7.2} M reads/s ({readers} readers), {:.1} MiB staged",
                reads as f64 / elapsed / 1e6,
                staged as f64 / (1 << 20) as f64
            );
            drop(io);
            let _ = std::fs::remove_dir_all(&sub);
        }
    });
}
