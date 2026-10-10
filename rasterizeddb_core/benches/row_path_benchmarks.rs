//! Round-two micro-benchmarks: the per-row work a SELECT does once it has
//! decided a row matches.
//!
//!  1. `Row::extend_serialized` — every returned row in bytes mode goes through
//!     this on its way to the wire.
//!  2. `Row::clone_row` / `Column::deep_clone` — the row-mode and cache-hit paths.
//!  3. String equality for short values — `WHERE name = 'x'` is the most common
//!     string predicate and lands in the <=16-byte path.
//!  4. `CONTAINS` with medium/large needles — the paths the round-one work left
//!     alone.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::processor::transformer::ComparerOperation;
use rasterizeddb_core::core::row::common::simd_compare_strings;
use rasterizeddb_core::core::row::row::{Column, Row};
use rasterizeddb_core::memory_pool::{MEMORY_POOL, MemoryBlock};

fn mb(bytes: &[u8]) -> MemoryBlock {
    let mut b = MEMORY_POOL.acquire(bytes.len());
    b.into_slice_mut().copy_from_slice(bytes);
    b
}

/// A row shaped like a typical record: a few fixed-width columns and a couple
/// of strings.
fn make_row(n_string_cols: usize, string_len: usize) -> Row {
    let mut columns = smallvec::SmallVec::new();

    columns.push(Column {
        schema_id: 0,
        data: mb(&42i64.to_le_bytes()),
        column_type: DbType::I64,
    });
    columns.push(Column {
        schema_id: 1,
        data: mb(&7i32.to_le_bytes()),
        column_type: DbType::I32,
    });
    columns.push(Column {
        schema_id: 2,
        data: mb(&1234.5f64.to_le_bytes()),
        column_type: DbType::F64,
    });
    columns.push(Column {
        schema_id: 3,
        data: mb(&1u8.to_le_bytes()),
        column_type: DbType::U8,
    });

    let filler: Vec<u8> = std::iter::repeat(b'x').take(string_len).collect();
    for i in 0..n_string_cols {
        columns.push(Column {
            schema_id: 4 + i as u32,
            data: mb(&filler),
            column_type: DbType::STRING,
        });
    }

    Row {
        position: 0,
        columns,
        length: 0,
    }
}

// ---------------------------------------------------------------------------
// 1. Row serialization
// ---------------------------------------------------------------------------

fn bench_extend_serialized(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_serialize");

    for (label, n_str, str_len) in [
        ("4num_0str", 0usize, 0usize),
        ("4num_2str_16b", 2, 16),
        ("4num_2str_64b", 2, 64),
        ("4num_8str_32b", 8, 32),
    ] {
        let row = make_row(n_str, str_len);

        // Fresh buffer each iteration: models a worker's first row into an
        // empty accumulator, where capacity growth is on the critical path.
        group.bench_with_input(BenchmarkId::new("into_fresh_vec", label), &label, |b, _| {
            b.iter(|| {
                let mut out: Vec<u8> = Vec::new();
                black_box(&row).extend_serialized(&mut out);
                black_box(out.len())
            })
        });

        // Reused buffer: models the steady state, where the accumulator has
        // already grown and only the per-column writes remain.
        group.bench_with_input(
            BenchmarkId::new("into_reused_vec", label),
            &label,
            |b, _| {
                let mut out: Vec<u8> = Vec::with_capacity(64 * 1024);
                b.iter(|| {
                    out.clear();
                    black_box(&row).extend_serialized(&mut out);
                    black_box(out.len())
                })
            },
        );

        // A whole batch of rows into one buffer — closest to what a worker does.
        group.bench_with_input(BenchmarkId::new("batch_64_rows", label), &label, |b, _| {
            let mut out: Vec<u8> = Vec::with_capacity(256 * 1024);
            b.iter(|| {
                out.clear();
                for _ in 0..64 {
                    black_box(&row).extend_serialized(&mut out);
                }
                black_box(out.len())
            })
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Row cloning
// ---------------------------------------------------------------------------

fn bench_clone_row(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_clone");

    for (label, n_str, str_len) in [
        ("4num_0str", 0usize, 0usize),
        ("4num_2str_16b", 2, 16),
        ("4num_2str_64b", 2, 64),
    ] {
        let row = make_row(n_str, str_len);
        group.bench_with_input(BenchmarkId::new("clone_row", label), &label, |b, _| {
            b.iter(|| black_box(Row::clone_row(black_box(&row))))
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Short-string equality (`WHERE name = 'x'`)
// ---------------------------------------------------------------------------

fn bench_string_equals(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_equals");

    // Values at and below the 16-byte boundary take the byte-at-a-time path.
    let cases: [(&str, &[u8], &[u8]); 8] = [
        ("len4_match", b"John".as_slice(), b"John".as_slice()),
        ("len4_diff_last", b"John".as_slice(), b"Johx".as_slice()),
        ("len8_match", b"Jonathan".as_slice(), b"Jonathan".as_slice()),
        (
            "len8_diff_last",
            b"Jonathan".as_slice(),
            b"Jonathax".as_slice(),
        ),
        (
            "len16_match",
            b"Engineering-Dept".as_slice(),
            b"Engineering-Dept".as_slice(),
        ),
        (
            "len16_diff_last",
            b"Engineering-Dept".as_slice(),
            b"Engineering-Depx".as_slice(),
        ),
        (
            "len12_match",
            b"Engineering2".as_slice(),
            b"Engineering2".as_slice(),
        ),
        ("len1_match", b"A".as_slice(), b"A".as_slice()),
    ];

    for (id, a, bb) in cases {
        group.bench_with_input(BenchmarkId::new("simd_equals", id), &id, |b, _| {
            b.iter(|| {
                black_box(simd_compare_strings(
                    black_box(a),
                    black_box(bb),
                    &ComparerOperation::Equals,
                ))
            })
        });
    }

    // Control: a length that already goes through the SSE path.
    let long_a = b"0123456789abcdef0123456789abcdef".as_slice();
    group.bench_function("simd_equals/len32_match", |b| {
        b.iter(|| {
            black_box(simd_compare_strings(
                black_box(long_a),
                black_box(long_a),
                &ComparerOperation::Equals,
            ))
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. CONTAINS with medium (9..=32) and large (>32) needles
// ---------------------------------------------------------------------------

fn bench_contains_bigger_needles(c: &mut Criterion) {
    let mut group = c.benchmark_group("contains_bigger");

    // A note field with many occurrences of the needle's first byte, which is
    // what makes a first-byte-only filter do redundant verification work.
    let note = b"the trade the term the total the theory the threshold the transfer the trigger the trailing the tranche the treasury the trustee the transcript".as_slice();
    let long_note: Vec<u8> = note.iter().chain(note.iter()).copied().collect();

    let cases: [(&str, &[u8], &[u8], bool); 6] = [
        ("med_n10_hit", note, b"threshold ".as_slice(), true),
        ("med_n10_miss", note, b"thresholdX".as_slice(), false),
        ("med_n16_hit", note, b"the transcript".as_slice(), true),
        ("med_n16_miss", note, b"the transcripX".as_slice(), false),
        (
            "large_n40_miss",
            &long_note,
            b"the trade the term the total the theoryX".as_slice(),
            false,
        ),
        (
            "large_n40_hit",
            &long_note,
            b"the trade the term the total the theory ".as_slice(),
            true,
        ),
    ];

    for (id, hay, needle, expected) in cases {
        group.bench_with_input(BenchmarkId::new("contains", id), &id, |b, _| {
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

    group.finish();
}

criterion_group!(
    benches,
    bench_extend_serialized,
    bench_clone_row,
    bench_string_equals,
    bench_contains_bigger_needles
);
criterion_main!(benches);
