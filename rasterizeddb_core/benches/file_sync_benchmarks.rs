use std::hint::black_box;
use std::io::Write;
use std::sync::atomic::Ordering;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::{RngCore, SeedableRng, rngs::StdRng};
use tempfile::TempDir;

use rasterizeddb_core::core::storage_providers::file_sync::LocalStorageProvider;
use rasterizeddb_core::core::storage_providers::traits::StorageIO;

fn write_random_file(path: &std::path::Path, bytes: usize, seed: u64) {
    let mut rng = StdRng::seed_from_u64(seed);

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap();

    // Write in chunks to avoid a single huge allocation.
    const CHUNK: usize = 1 * 1024 * 1024;
    let mut remaining = bytes;
    let mut buf = vec![0u8; CHUNK];

    while remaining > 0 {
        let n = remaining.min(CHUNK);
        rng.fill_bytes(&mut buf[..n]);
        file.write_all(&buf[..n]).unwrap();
        remaining -= n;
    }

    file.flush().unwrap();
}

fn append_random_bytes(path: &std::path::Path, bytes: usize, seed: u64) {
    let mut rng = StdRng::seed_from_u64(seed);

    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .unwrap();

    const CHUNK: usize = 1 * 1024 * 1024;
    let mut remaining = bytes;
    let mut buf = vec![0u8; CHUNK];

    while remaining > 0 {
        let n = remaining.min(CHUNK);
        rng.fill_bytes(&mut buf[..n]);
        file.write_all(&buf[..n]).unwrap();
        remaining -= n;
    }

    file.flush().unwrap();
}

fn make_provider(rt: &tokio::runtime::Runtime, dir: &TempDir, file_name: &str) -> LocalStorageProvider {
    rt.block_on(LocalStorageProvider::new(
        dir.path().to_string_lossy().as_ref(),
        Some(file_name),
    ))
}

fn bench_file_sync_read_data_into_buffer(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("file_sync_read_data_into_buffer");

    // -------- Case 1: Hot reads fully satisfied by mmap --------
    {
        let dir = tempfile::tempdir().unwrap();
        let file_name = "bench_mmap.db";
        let file_path = dir.path().join(file_name);

        let file_size = 64 * 1024 * 1024; // 64 MiB
        write_random_file(&file_path, file_size, 0xA11CE);

        let provider = make_provider(&rt, &dir, file_name);
        let read_size = 4 * 1024;

        group.throughput(Throughput::Bytes(read_size as u64));
        group.bench_with_input(BenchmarkId::new("mmap_hot_sequential", read_size), &read_size, |b, &read_size| {
            let mut buf = vec![0u8; read_size];
            let mut pos = 0u64;
            let max_start = (file_size - read_size) as u64;

            b.iter(|| {
                // Wrap around to keep reads in-bounds.
                if pos > max_start {
                    pos = 0;
                }

                let mut local_pos = pos;
                rt.block_on(provider.read_data_into_buffer(&mut local_pos, &mut buf))
                    .unwrap();

                // Advance for next iteration.
                pos = local_pos;

                black_box(&buf);
                black_box(local_pos);
            })
        });
    }

    // -------- Case 1b: Random reads within a 128MiB mmap --------
    // This approximates OLTP-style point reads scattered across the file.
    {
        let dir = tempfile::tempdir().unwrap();
        let file_name = "bench_mmap_random_128mb.db";
        let file_path = dir.path().join(file_name);

        let file_size = 128 * 1024 * 1024; // 128 MiB
        write_random_file(&file_path, file_size, 0x5151_5151);

        let provider = make_provider(&rt, &dir, file_name);

        let read_size = 4 * 1024;
        let max_start = (file_size - read_size) as u64;

        // Precompute offsets to avoid RNG overhead inside the measurement loop.
        // NOTE: Align offsets to the read size. DB/page workloads are typically page-aligned,
        // and unaligned 4KiB reads can straddle two OS pages which changes the bottleneck.
        let mut rng = StdRng::seed_from_u64(0xC0DE_CAFE);
        let offsets_len = 16 * 1024;
        let mut offsets = Vec::with_capacity(offsets_len);
        for _ in 0..offsets_len {
            let raw = rng.next_u64() % (max_start + 1);
            let aligned = (raw / read_size as u64) * read_size as u64;
            offsets.push(aligned);
        }

        group.throughput(Throughput::Bytes(read_size as u64));
        group.bench_with_input(
            BenchmarkId::new("mmap_random_positions_128mb", read_size),
            &read_size,
            |b, &read_size| {
                let mut buf = vec![0u8; read_size];
                let mut i = 0usize;

                b.iter(|| {
                    let offset = offsets[i];
                    i += 1;
                    if i == offsets.len() {
                        i = 0;
                    }

                    let mut pos = offset;
                    rt.block_on(provider.read_data_into_buffer(&mut pos, &mut buf))
                        .unwrap();

                    black_box(&buf);
                    black_box(pos);
                })
            },
        );
    }

    // -------- Case 1c: Random 16KiB page reads within a 128MiB mmap --------
    // This measures DB-style page reads (16KiB) at page-aligned offsets.
    {
        let dir = tempfile::tempdir().unwrap();
        let file_name = "bench_mmap_random_128mb_16k.db";
        let file_path = dir.path().join(file_name);

        let file_size = 128 * 1024 * 1024; // 128 MiB
        write_random_file(&file_path, file_size, 0x5151_5152);

        let provider = make_provider(&rt, &dir, file_name);

        let read_size = 16 * 1024;
        let max_start = (file_size - read_size) as u64;

        let mut rng = StdRng::seed_from_u64(0xC0DE_F00D);
        let offsets_len = 8 * 1024;
        let mut offsets = Vec::with_capacity(offsets_len);
        for _ in 0..offsets_len {
            let raw = rng.next_u64() % (max_start + 1);
            let aligned = (raw / read_size as u64) * read_size as u64;
            offsets.push(aligned);
        }

        group.throughput(Throughput::Bytes(read_size as u64));
        group.bench_with_input(
            BenchmarkId::new("mmap_random_pages_128mb", read_size),
            &read_size,
            |b, &read_size| {
                let mut buf = vec![0u8; read_size];
                let mut i = 0usize;

                b.iter(|| {
                    let offset = offsets[i];
                    i += 1;
                    if i == offsets.len() {
                        i = 0;
                    }

                    let mut pos = offset;
                    rt.block_on(provider.read_data_into_buffer(&mut pos, &mut buf))
                        .unwrap();

                    black_box(&buf);
                    black_box(pos);
                })
            },
        );
    }

    // -------- Case 2: Forced fallback to pread beyond current mmap --------
    // We extend the underlying file *without* calling update_memory_map() so mmap stays smaller,
    // but we do publish the new length via the atomic.
    {
        let dir = tempfile::tempdir().unwrap();
        let file_name = "bench_pread.db";
        let file_path = dir.path().join(file_name);

        let initial = 8 * 1024 * 1024; // 8 MiB mapped at creation
        let extra = 8 * 1024 * 1024; // 8 MiB appended after creation
        write_random_file(&file_path, initial, 0xBEEF);

        let provider = make_provider(&rt, &dir, file_name);

        append_random_bytes(&file_path, extra, 0xC0FFEE);
        provider.file_len.fetch_add(extra as u64, Ordering::Release);

        let read_size = 4 * 1024;
        group.throughput(Throughput::Bytes(read_size as u64));
        group.bench_with_input(
            BenchmarkId::new("pread_fallback_beyond_mmap", read_size),
            &read_size,
            |b, &read_size| {
                let mut buf = vec![0u8; read_size];

                // Read from the middle of the appended region so it cannot be served by the old mmap.
                let start = (initial + (extra / 2)) as u64;

                b.iter(|| {
                    let mut pos = start;
                    rt.block_on(provider.read_data_into_buffer(&mut pos, &mut buf))
                        .unwrap();

                    black_box(&buf);
                    black_box(pos);
                })
            },
        );
    }

    // -------- Case 3: Cross-boundary read (main file tail + staged temp head) --------
    {
        let dir = tempfile::tempdir().unwrap();
        let file_name = "bench_temp_boundary.db";
        let file_path = dir.path().join(file_name);

        let main_size = 4 * 1024 * 1024; // 4 MiB main
        write_random_file(&file_path, main_size, 0x1234);

        let provider = make_provider(&rt, &dir, file_name);

        // Stage bytes into temp (not flushed), so reads may span main->temp.
        let staged_size = 4 * 1024 * 1024;
        let mut staged = vec![0u8; staged_size];
        StdRng::seed_from_u64(0xFEEDFACE).fill_bytes(&mut staged);
        rt.block_on(StorageIO::append_data(&provider, &staged, false));

        let read_size = 4 * 1024;
        group.throughput(Throughput::Bytes(read_size as u64));
        group.bench_with_input(
            BenchmarkId::new("main_to_temp_boundary", read_size),
            &read_size,
            |b, &read_size| {
                let mut buf = vec![0u8; read_size];
                // Start 2 KiB before end of main, so the read spans into temp.
                let start = (main_size as u64).saturating_sub(2 * 1024);

                b.iter(|| {
                    let mut pos = start;
                    rt.block_on(provider.read_data_into_buffer(&mut pos, &mut buf))
                        .unwrap();

                    black_box(&buf);
                    black_box(pos);
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_file_sync_read_data_into_buffer);
criterion_main!(benches);
