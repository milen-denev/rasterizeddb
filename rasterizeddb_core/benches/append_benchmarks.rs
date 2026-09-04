//! Append/durability cost.
//!
//! Every immediate append issues a write followed by a full `fsync`, and an
//! INSERT performs two of them (row bytes, then the pointer record). On tmpfs an
//! fsync is ~1 us and hides this completely, so this benchmark deliberately runs
//! on a real filesystem where a flush actually reaches the device.
//!
//! Set `RASTDB_BENCH_DIR` to choose where; it defaults to /var/tmp so the
//! numbers mean something.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use tempfile::TempDir;

use rasterizeddb_core::core::storage_providers::file_sync::LocalStorageProvider;
use rasterizeddb_core::core::storage_providers::traits::StorageIO;

fn bench_dir() -> TempDir {
    let base = std::env::var("RASTDB_BENCH_DIR").unwrap_or_else(|_| "/var/tmp".to_string());
    tempfile::Builder::new()
        .prefix("rastdb_append_")
        .tempdir_in(base)
        .unwrap()
}

/// `RASTDB_BENCH_WAL=1` routes durability through the write-ahead log so the
/// two modes can be compared on the same machine and filesystem.
fn maybe_install_wal(dir: &std::path::Path) {
    if std::env::var("RASTDB_BENCH_WAL").as_deref() != Ok("1") {
        return;
    }
    let dir_str = dir.to_string_lossy().to_string();
    let path = dir.join(rasterizeddb_core::core::wal::DEFAULT_WAL_NAME);
    let wal = rasterizeddb_core::core::wal::Wal::open(&path).unwrap();
    rasterizeddb_core::core::wal::install_for(&dir_str, std::sync::Arc::new(wal));
}

fn bench_appends(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    // A single database directory, so one log covers every file below.
    let db_dir = bench_dir();
    maybe_install_wal(db_dir.path());

    let mut group = c.benchmark_group("append");
    group.sample_size(10);

    // A pointer record is 21 bytes; a small row is on the order of 64.
    let record = vec![0xABu8; 21];
    let row = vec![0xCDu8; 64];

    // Sequential appends: one client inserting in a loop.
    {
        let dir = &db_dir;
        let provider = rt.block_on(LocalStorageProvider::new(
            dir.path().to_string_lossy().as_ref(),
            Some("seq.db"),
        ));
        group.bench_function("sequential_200_immediate", |b| {
            b.iter(|| {
                rt.block_on(async {
                    for _ in 0..200 {
                        provider.append_data(black_box(&record), true).await;
                    }
                })
            })
        });
    }

    // Two appends per logical insert, which is what write_row actually does.
    {
        let dir = &db_dir;
        let rows_io = rt.block_on(LocalStorageProvider::new(
            dir.path().to_string_lossy().as_ref(),
            Some("rows.db"),
        ));
        let ptrs_io = rt.block_on(LocalStorageProvider::new(
            dir.path().to_string_lossy().as_ref(),
            Some("ptrs.db"),
        ));
        // Mirrors `write_row`: two appends, one commit. With a log installed
        // that is one flush per row instead of two.
        group.bench_function("insert_shape_100_rows", |b| {
            b.iter(|| {
                rt.block_on(async {
                    for _ in 0..100 {
                        // One transaction across both files, exactly as
                        // `write_row` does it.
                        let txn = rows_io.begin_txn();
                        let _ = rows_io.append_data_txn(txn, black_box(&row)).await;
                        let (_, token) = ptrs_io.append_data_txn(txn, black_box(&record)).await;
                        ptrs_io.commit_durable(token).await.unwrap();
                    }
                })
            })
        });
    }

    // Concurrent appenders: the server's actual shape, and the case where a
    // shared flush can cover more than one waiter.
    for tasks in [4usize, 16] {
        let dir = &db_dir;
        let provider = std::sync::Arc::new(rt.block_on(LocalStorageProvider::new(
            dir.path().to_string_lossy().as_ref(),
            Some("conc.db"),
        )));
        group.bench_function(format!("concurrent_{}x50_immediate", tasks), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut handles = Vec::with_capacity(tasks);
                    for _ in 0..tasks {
                        let p = provider.clone();
                        let rec = record.clone();
                        handles.push(tokio::spawn(async move {
                            for _ in 0..50 {
                                p.append_data(&rec, true).await;
                            }
                        }));
                    }
                    for h in handles {
                        let _ = h.await;
                    }
                })
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_appends);
criterion_main!(benches);
