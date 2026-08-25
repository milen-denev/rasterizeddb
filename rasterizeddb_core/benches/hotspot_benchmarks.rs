//! Micro-benchmarks for the specific hot-path functions targeted by the
//! performance work:
//!
//!  1. `AtomicGenericCache::{get, contains_key, insert}` — the query-result
//!     cache and the memory pool's heap-reuse cache both live here.
//!  2. `MemoryPool::acquire` + `MemoryBlock::drop` — every column read, every
//!     math intermediate and every string materialization goes through this.
//!  3. `simd_compare_strings(Contains)` for short needles — the `LIKE '%x%'`
//!     path used by WHERE filters.
//!
//! Each group is written so the *same* benchmark ids exist before and after the
//! optimization, so `cargo bench` reports a direct before/after delta.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use rasterizeddb_core::cache::atomic_cache::AtomicGenericCache;
use rasterizeddb_core::core::processor::transformer::ComparerOperation;
use rasterizeddb_core::core::row::common::simd_compare_strings;
use rasterizeddb_core::memory_pool::MEMORY_POOL;

// ---------------------------------------------------------------------------
// 1. AtomicGenericCache lookup
// ---------------------------------------------------------------------------

/// Cache sizes that matter in practice:
///  - 64   : the memory pool's heap-reuse cache (`AtomicGenericCache::new(64, 1024)`)
///  - 1024 : the floor `main.rs` applies to `--cache-size`
///  - 16384 / 131072 : realistic query-result cache sizes
///    (`--cache-size 100000` rounds up to 131072 slots)
const CACHE_SIZES: [usize; 4] = [64, 1024, 16384, 131072];

fn bench_cache_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("atomic_cache_get");

    for &cap in CACHE_SIZES.iter() {
        // Fill the cache to ~50% so lookups have to walk realistic occupancy.
        let cache = AtomicGenericCache::<u64, u64>::new(cap, cap * 4);
        let fill = cap / 2;
        for i in 0..fill as u64 {
            let _ = cache.insert(i, i * 3 + 1);
        }

        // Hit: a key that exists.
        let hit_key = (fill / 2) as u64;
        group.bench_with_input(BenchmarkId::new("hit", cap), &cap, |b, _| {
            b.iter(|| black_box(cache.get(black_box(&hit_key))))
        });

        // Miss: a key that does not exist (worst case for a linear scan —
        // it must touch every single slot before giving up).
        let miss_key = u64::MAX - 7;
        group.bench_with_input(BenchmarkId::new("miss", cap), &cap, |b, _| {
            b.iter(|| black_box(cache.get(black_box(&miss_key))))
        });

        group.bench_with_input(BenchmarkId::new("contains_key_hit", cap), &cap, |b, _| {
            b.iter(|| black_box(cache.contains_key(black_box(&hit_key))))
        });
    }

    group.finish();
}

fn bench_cache_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("atomic_cache_insert");

    for &cap in [64usize, 1024, 16384].iter() {
        let cache = AtomicGenericCache::<u64, u64>::new(cap, cap * 4);
        let fill = cap / 2;
        for i in 0..fill as u64 {
            let _ = cache.insert(i, i);
        }

        // Overwrite of an existing key: exercises `try_update_existing`.
        let existing = (fill / 2) as u64;
        group.bench_with_input(BenchmarkId::new("update_existing", cap), &cap, |b, _| {
            b.iter(|| black_box(cache.insert(black_box(existing), black_box(42u64))))
        });
    }

    group.finish();
}

/// Multi-threaded lookup: the query cache is read concurrently by every worker
/// and the memory pool's reuse cache by every allocating thread, so contention
/// behaviour matters as much as single-thread latency.
fn bench_cache_get_concurrent(c: &mut Criterion) {
    use std::sync::Arc as StdArc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let mut group = c.benchmark_group("atomic_cache_get_concurrent");
    group.sample_size(30);

    for &cap in [1024usize, 16384].iter() {
        let cache = AtomicGenericCache::<u64, u64>::new(cap, cap * 4);
        let fill = cap / 2;
        for i in 0..fill as u64 {
            let _ = cache.insert(i, i);
        }

        let stop = StdArc::new(AtomicBool::new(false));
        let mut handles = Vec::new();
        for t in 0..3 {
            let cache = cache.clone();
            let stop = stop.clone();
            handles.push(std::thread::spawn(move || {
                let mut k = t as u64;
                while !stop.load(Ordering::Relaxed) {
                    let _ = black_box(cache.get(&k));
                    k = (k + 1) % fill as u64;
                }
            }));
        }

        let hit_key = (fill / 2) as u64;
        group.bench_with_input(BenchmarkId::new("hit_3_readers", cap), &cap, |b, _| {
            b.iter(|| black_box(cache.get(black_box(&hit_key))))
        });

        stop.store(true, Ordering::Relaxed);
        for h in handles {
            let _ = h.join();
        }
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. MemoryPool acquire / release
// ---------------------------------------------------------------------------

fn bench_memory_pool(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_pool");

    // Inline path (<= 16 bytes): every i32/i64/f64 column value.
    for &size in [4usize, 8, 16].iter() {
        group.bench_with_input(BenchmarkId::new("acquire_inline", size), &size, |b, &s| {
            b.iter(|| {
                let mb = MEMORY_POOL.acquire(black_box(s));
                black_box(mb.len())
            })
        });
    }

    // Heap path: string columns and row buffers. A tight acquire/drop loop is
    // exactly what the row-fetch path does per column per row.
    for &size in [32usize, 64, 256, 4096].iter() {
        group.bench_with_input(BenchmarkId::new("acquire_drop_heap", size), &size, |b, &s| {
            b.iter(|| {
                let mb = MEMORY_POOL.acquire(black_box(s));
                black_box(mb.len())
            })
        });
    }

    // Several live blocks at once: the single-slot reuse cache can only satisfy
    // one of them, so this measures the mixed reuse-hit / mi_malloc path that a
    // real row fetch (N columns held simultaneously) sees.
    group.bench_function("acquire_drop_heap_8_live_64b", |b| {
        b.iter(|| {
            let blocks: [_; 8] = std::array::from_fn(|_| MEMORY_POOL.acquire(64));
            black_box(blocks[0].len())
        })
    });

    // Clone is the deep-copy path used by `Row::clone_row` / `Column::deep_clone`.
    let src = MEMORY_POOL.acquire(64);
    group.bench_function("clone_heap_64b", |b| {
        b.iter(|| black_box(black_box(&src).clone()))
    });

    let src_inline = MEMORY_POOL.acquire(8);
    group.bench_function("clone_inline_8b", |b| {
        b.iter(|| black_box(black_box(&src_inline).clone()))
    });

    group.finish();
}

/// Multi-threaded allocation. The heap-reuse cache is a single global structure,
/// so cross-thread contention is the thing to watch here.
fn bench_memory_pool_concurrent(c: &mut Criterion) {
    use std::sync::Arc as StdArc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let mut group = c.benchmark_group("memory_pool_concurrent");
    group.sample_size(30);

    let stop = StdArc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for _ in 0..3 {
        let stop = stop.clone();
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let mb = MEMORY_POOL.acquire(64);
                black_box(mb.len());
            }
        }));
    }

    group.bench_function("acquire_drop_heap_64b_3_allocators", |b| {
        b.iter(|| {
            let mb = MEMORY_POOL.acquire(black_box(64));
            black_box(mb.len())
        })
    });

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. simd_compare_strings — Contains with short needles
// ---------------------------------------------------------------------------

fn bench_string_contains(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_contains");

    // Realistic column values: an email, a short description, a long note.
    let email = b"jonathan.doe.the.third@some-company-mail.example.com".as_slice(); // 52
    let description =
        b"Senior backend engineer working on distributed storage systems and query engines"
            .as_slice(); // 80
    let long_note = b"This row records a transaction that was settled through the clearing house on the third business day following the trade date, with no exceptions raised by the compliance desk during review.".as_slice(); // ~190

    // Needles of 2..=8 bytes hit `contains_small`, which is the fully scalar path.
    let cases: [(&str, &[u8], &[u8], bool); 9] = [
        ("email_n2_hit", email, b"@s".as_slice(), true),
        ("email_n4_hit", email, b"mail".as_slice(), true),
        ("email_n8_hit", email, b"example.".as_slice(), true),
        ("email_n4_miss", email, b"zzzz".as_slice(), false),
        ("desc_n3_hit", description, b"and".as_slice(), true),
        ("desc_n6_hit", description, b"engine".as_slice(), true),
        ("desc_n6_miss", description, b"qqqqqq".as_slice(), false),
        ("note_n5_hit", long_note, b"trade".as_slice(), true),
        ("note_n5_miss", long_note, b"xxxxx".as_slice(), false),
    ];

    for (id, hay, needle, expected) in cases {
        group.bench_with_input(BenchmarkId::new("small_needle", id), &id, |b, _| {
            b.iter(|| {
                let r = simd_compare_strings(
                    black_box(hay),
                    black_box(needle),
                    &ComparerOperation::Contains,
                );
                debug_assert_eq!(r, expected);
                black_box(r)
            })
        });
    }

    // Medium needles (9..=32) already take a SIMD path — kept as a control so a
    // regression there is visible.
    group.bench_function("medium_needle/desc_n12_hit", |b| {
        b.iter(|| {
            black_box(simd_compare_strings(
                black_box(description),
                black_box(b"distributed ".as_slice()),
                &ComparerOperation::Contains,
            ))
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_cache_get,
    bench_cache_insert,
    bench_cache_get_concurrent,
    bench_memory_pool,
    bench_memory_pool_concurrent,
    bench_string_contains
);
criterion_main!(benches);
