//! Large-mmap access patterns.
//!
//! Row reads are served by copying out of a memory map, so once a table no
//! longer fits in TLB reach the cost of *finding* a page starts to compete with
//! the cost of copying it. The working set here (1 GiB) is deliberately larger
//! than this CPU's L2 TLB can cover with 4 KiB pages but well inside what it
//! covers with 2 MiB pages, so the two page sizes are distinguishable.
//!
//! The file is warmed before measuring, so these numbers are about page-table
//! walks and page-cache copies, not disk.

use std::hint::black_box;
use std::io::Write;

use criterion::{Criterion, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};
use tempfile::TempDir;

use rasterizeddb_core::core::row::row_pointer::{
    ROW_POINTER_RECORD_LEN, RowPointer, RowPointerIterator,
};
use rasterizeddb_core::core::storage_providers::file_sync::LocalStorageProvider;
use rasterizeddb_core::core::storage_providers::traits::StorageIO;

const FILE_BYTES: usize = 1024 * 1024 * 1024; // 1 GiB
const POINTER_RECORD: usize = 92; // RowPointer::TOTAL_LENGTH
const SCAN_CHUNK: usize = POINTER_RECORD * 3047; // ~280 KiB, the iterator's chunk

fn write_file(path: &std::path::Path, bytes: usize) {
    let mut rng = StdRng::seed_from_u64(0xBEEF);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap();

    const CHUNK: usize = 4 * 1024 * 1024;
    let mut remaining = bytes;
    let mut buf = vec![0u8; CHUNK];
    while remaining > 0 {
        let n = remaining.min(CHUNK);
        rng.fill_bytes(&mut buf[..n]);
        file.write_all(&buf[..n]).unwrap();
        remaining -= n;
    }
    file.flush().unwrap();
    file.sync_all().unwrap();
}

fn bench_mmap_patterns(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let dir = TempDir::new().unwrap();
    let file_name = "big_table.db";
    write_file(&dir.path().join(file_name), FILE_BYTES);

    let provider = rt.block_on(LocalStorageProvider::new(
        dir.path().to_string_lossy().as_ref(),
        Some(file_name),
    ));

    // Warm the page cache and the mapping so we measure steady state.
    {
        let mut warm = vec![0u8; SCAN_CHUNK];
        let mut pos = 0u64;
        while (pos as usize) + SCAN_CHUNK <= FILE_BYTES {
            let mut p = pos;
            let _ = rt.block_on(provider.read_data_into_buffer(&mut p, &mut warm));
            pos += SCAN_CHUNK as u64;
        }
    }

    let mut group = c.benchmark_group("mmap_patterns");
    group.sample_size(10);

    // Full sequential scan, the shape RowPointerIterator uses.
    group.bench_function("seq_scan_full_1gib", |b| {
        let mut buf = vec![0u8; SCAN_CHUNK];
        b.iter(|| {
            let mut pos = 0u64;
            let mut total = 0usize;
            while (pos as usize) + SCAN_CHUNK <= FILE_BYTES {
                let mut p = pos;
                if provider.try_read_vectored_sync(&mut [(p, &mut buf[..])]) {
                    total += SCAN_CHUNK;
                } else {
                    let _ = rt.block_on(provider.read_data_into_buffer(&mut p, &mut buf));
                    total += SCAN_CHUNK;
                }
                pos += SCAN_CHUNK as u64;
            }
            black_box(total)
        })
    });

    group.sample_size(100);

    // Random pointer-record reads spread over the whole file: the access
    // pattern that punishes a 4 KiB-page TLB the hardest.
    let offsets: Vec<u64> = {
        let mut rng = StdRng::seed_from_u64(7);
        (0..4096)
            .map(|_| {
                let max = (FILE_BYTES - POINTER_RECORD) as u64;
                (rng.next_u64() % max) & !0x3
            })
            .collect()
    };

    group.bench_function("random_pointer_reads_x1024", |b| {
        let mut buf = [0u8; POINTER_RECORD];
        let mut i = 0usize;
        b.iter(|| {
            let mut ok = 0usize;
            for _ in 0..1024 {
                let off = offsets[i & (offsets.len() - 1)];
                i += 1;
                if provider.try_read_vectored_sync(&mut [(off, &mut buf[..])]) {
                    ok += 1;
                }
            }
            black_box(ok)
        })
    });

    // Row-fetch shape: one vectored call gathering many small columns at
    // scattered offsets.
    group.bench_function("random_vectored_16x64b_x64", |b| {
        let mut bufs: Vec<[u8; 64]> = vec![[0u8; 64]; 16];
        let mut i = 0usize;
        b.iter(|| {
            let mut ok = 0usize;
            for _ in 0..64 {
                let mut reads: Vec<(u64, &mut [u8])> = Vec::with_capacity(16);
                for (k, buf) in bufs.iter_mut().enumerate() {
                    let off = offsets[(i + k) & (offsets.len() - 1)];
                    reads.push((off, &mut buf[..]));
                }
                i += 16;
                if provider.try_read_vectored_sync(&mut reads) {
                    ok += 1;
                }
            }
            black_box(ok)
        })
    });

    group.finish();
}

/// End-to-end pointer scan: what a full-table WHERE actually walks.
///
/// `RowPointerIterator` copies a ~64 KiB chunk out of the memory map into an
/// owned buffer and then parses records from that copy. This measures the whole
/// thing, so the copy's share of the scan is visible.
fn bench_pointer_scan(c: &mut Criterion) {
    const POINTERS: usize = 8 * 1024 * 1024; // ~8M rows

    let _ = rasterizeddb_core::BATCH_SIZE.set(16384);
    let _ = rasterizeddb_core::MAX_PERMITS_THREADS.set(8);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let dir = TempDir::new().unwrap();
    let file_name = "pointers.db";

    // Build a valid pointer file: id, length, position, not-deleted.
    {
        let path = dir.path().join(file_name);
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let mut buf: Vec<u8> = Vec::with_capacity(POINTERS * ROW_POINTER_RECORD_LEN);
        for i in 0..POINTERS as u64 {
            buf.extend_from_slice(&i.to_le_bytes()); // id
            buf.extend_from_slice(&64u32.to_le_bytes()); // length
            buf.extend_from_slice(&(i * 64).to_le_bytes()); // position
            buf.push(0); // deleted = false
        }
        file.write_all(&buf).unwrap();
        file.flush().unwrap();
        file.sync_all().unwrap();
    }

    let provider = rt.block_on(LocalStorageProvider::new(
        dir.path().to_string_lossy().as_ref(),
        Some(file_name),
    ));
    let io = rclite::Arc::new(provider);

    let mut group = c.benchmark_group("pointer_scan");
    group.sample_size(20);

    group.bench_function("full_scan_8M_pointers", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut it = RowPointerIterator::new(io.clone()).await.unwrap();
                let mut out: Vec<RowPointer> = Vec::new();
                let mut total = 0usize;
                loop {
                    it.next_row_pointers_into(&mut out).await.unwrap();
                    if out.is_empty() {
                        break;
                    }
                    total += out.len();
                }
                black_box(total)
            })
        })
    });

    group.finish();
}

criterion_group!(benches, bench_mmap_patterns, bench_pointer_scan);
criterion_main!(benches);
