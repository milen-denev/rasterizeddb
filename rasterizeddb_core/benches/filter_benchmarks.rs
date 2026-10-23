use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::db_type::DbType as Ty;
use rasterizeddb_core::core::filter::column::{ColumnFilter, bitmap_words};
use rasterizeddb_core::core::filter::hash::{HashSeed, canonical_hash, fingerprint};
use rasterizeddb_core::core::filter::kernels::*;
use rasterizeddb_core::core::filter::{FilterConfig, SargOp, TableFilters};
use rasterizeddb_core::core::index_store::gate::{Proof, test_mint};
use rasterizeddb_core::core::row::schema::SchemaField;

/// One zone probe = 512 rows. Reported as rows/second.
fn bench_probe_kernel(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter/probe_zone");
    group.throughput(Throughput::Elements(ZONE_ROWS as u64));

    for f in [4u8, 8, 12, 16] {
        let planes: Vec<u64> = (0..f as usize * ZONE_WORDS)
            .map(|_| fastrand::u64(..))
            .collect();
        let g = fingerprint(fastrand::u64(..), f);
        let mut out = [0u64; ZONE_WORDS];

        group.bench_function(format!("scalar/f{}", f), |b| {
            b.iter(|| {
                probe_zone_scalar(black_box(&planes), f, black_box(g), &mut out);
                black_box(out[0])
            })
        });

        let probe = probe_fn();
        group.bench_function(format!("{}/f{}", active_kernel(), f), |b| {
            b.iter(|| {
                probe(black_box(&planes), f, black_box(g), &mut out);
                black_box(out[0])
            })
        });
    }
    group.finish();
}

/// Full-column sweep: the cost of turning a query value into a candidate bitmap.
fn bench_column_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter/column_sweep");
    const N: u64 = 1_000_000;
    group.throughput(Throughput::Elements(N));

    for f in [8u8, 16] {
        let cfg = FilterConfig {
            min_f: f,
            default_f: f,
            max_f: f,
            ..Default::default()
        };
        let mut col = ColumnFilter::new(0, DbType::U64, HashSeed::default(), cfg);
        for slot in 0..N {
            col.set(slot, Some(&fastrand::u64(..).to_le_bytes()));
        }
        let mut bm = vec![0u64; bitmap_words(N)];

        group.bench_function(format!("f{}/{}", f, active_kernel()), |b| {
            b.iter(|| {
                col.probe_hash_into(black_box(fastrand::u64(..)), N, &mut bm);
                black_box(bm[0])
            })
        });
    }
    group.finish();
}

fn bench_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter/canonical_hash");
    let seed = HashSeed::default();

    let v = 0x0123_4567_89AB_CDEFu64.to_le_bytes();
    group.bench_function("u64", |b| {
        b.iter(|| black_box(canonical_hash(&DbType::U64, black_box(&v), seed)))
    });

    let f = 3.14159f64.to_le_bytes();
    group.bench_function("f64", |b| {
        b.iter(|| black_box(canonical_hash(&DbType::F64, black_box(&f), seed)))
    });

    for len in [8usize, 32, 128] {
        let s: Vec<u8> = (0..len).map(|_| fastrand::u8(b'a'..=b'z')).collect();
        group.bench_function(format!("string/{}b", len), |b| {
            b.iter(|| black_box(canonical_hash(&DbType::STRING, black_box(&s), seed)))
        });
    }
    group.finish();
}

/// Insert-path cost: what maintaining the index adds to a row write.
fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter/insert");
    group.throughput(Throughput::Elements(1));

    for f in [8u8, 16] {
        let cfg = FilterConfig {
            min_f: f,
            default_f: f,
            max_f: f,
            ..Default::default()
        };
        group.bench_function(format!("set_slot/f{}", f), |b| {
            b.iter_batched_ref(
                || ColumnFilter::new(0, DbType::U64, HashSeed::default(), cfg),
                |col| {
                    for slot in 0..1024u64 {
                        col.set(slot, Some(&slot.to_le_bytes()));
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

/// Insert maintenance under concurrent writers.
///
/// This is the question the "lock-free open zone" item exists to answer: how
/// much of insert maintenance is the per-column lock, and how badly does it
/// scale? Reported as rows/sec per thread count so the shape is visible rather
/// than a single number.
fn report_insert_contention(_c: &mut Criterion) {
    use rasterizeddb_core::core::filter::TableFilters;
    use rasterizeddb_core::core::row::row::{ColumnWritePayload, RowWrite};
    use rasterizeddb_core::core::row::schema::SchemaField;
    use rasterizeddb_core::memory_pool::MEMORY_POOL;
    use smallvec::SmallVec;
    use std::sync::Arc;

    const PER_THREAD: u64 = 200_000;
    let spec = [
        ("id", DbType::U64),
        ("cat", DbType::I32),
        ("label", DbType::STRING),
        ("score", DbType::F64),
    ];
    let fields: SmallVec<[SchemaField; 20]> = spec
        .iter()
        .enumerate()
        .map(|(i, (n, t))| {
            SchemaField::new(n.to_string(), t.clone(), t.get_size(), 0, i as u64, false)
        })
        .collect();

    fn payload(bytes: &[u8], write_order: u32, column_type: DbType) -> ColumnWritePayload {
        let mut mb = MEMORY_POOL.acquire(bytes.len().max(1));
        mb.into_slice_mut()[..bytes.len()].copy_from_slice(bytes);
        let size = column_type.get_size();
        ColumnWritePayload {
            data: mb,
            write_order,
            column_type,
            size,
        }
    }

    // Row construction is pooled allocation and would be timed as contention it
    // is not. Each thread cycles a small pre-built set instead, so what the
    // clock sees is maintenance and nothing else.
    let prebuilt: Vec<RowWrite> = (0..64u64)
        .map(|k| {
            let mut rw = RowWrite {
                columns_writing_data: SmallVec::new(),
            };
            rw.columns_writing_data
                .push(payload(&k.to_le_bytes(), 0, DbType::U64));
            rw.columns_writing_data
                .push(payload(&((k % 97) as i32).to_le_bytes(), 1, DbType::I32));
            rw.columns_writing_data
                .push(payload(format!("l{}", k).as_bytes(), 2, DbType::STRING));
            rw.columns_writing_data
                .push(payload(&(k as f64).to_le_bytes(), 3, DbType::F64));
            rw
        })
        .collect();
    let prebuilt = &prebuilt;
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);

    for batch in [1usize, 64] {
        for threads in [1usize, 2, 4, 8] {
            let filters = Arc::new(TableFilters::new(&fields, FilterConfig::default()));
            let start = std::time::Instant::now();
            std::thread::scope(|s| {
                for t in 0..threads {
                    let filters = filters.clone();
                    s.spawn(move || {
                        let mut run: Vec<(u64, &RowWrite)> = Vec::with_capacity(batch);
                        for i in 0..PER_THREAD {
                            let slot = (t as u64) * PER_THREAD + i;
                            if batch == 1 {
                                filters.on_insert(proof, slot, &prebuilt[(i % 64) as usize]);
                            } else {
                                run.push((slot, &prebuilt[(i % 64) as usize]));
                                if run.len() == batch {
                                    filters.on_insert_batch(proof, &run);
                                    run.clear();
                                }
                            }
                        }
                        if !run.is_empty() {
                            filters.on_insert_batch(proof, &run);
                        }
                    });
                }
            });
            let secs = start.elapsed().as_secs_f64();
            let rows = PER_THREAD * threads as u64;
            println!(
                "insert contention: batch={:<3} {} thread(s)  {:>9.0} rows/s  {:>6.1} ns/row",
                batch,
                threads,
                rows as f64 / secs,
                secs * 1e9 / rows as f64,
            );
        }
    }
}

/// What the L0/L1 span summary is worth, at both ends of its cost model.
///
/// The summary costs 0.25 bits/row and rules out a whole 16 384-row superzone
/// in eight word tests. Whether that beats reading the planes depends entirely
/// on how many distinct values a span holds, so both regimes are reported —
/// the point is to make "turn it on for a column like this" a measurement
/// rather than an assertion.
fn report_span_summary(_c: &mut Criterion) {
    use rasterizeddb_core::core::filter::bloom::SUPERZONE_ROWS;
    use rasterizeddb_core::core::filter::column::bitmap_words;

    const N: u64 = 1_000_000;
    let f = 8u8;
    let cfg = FilterConfig {
        min_f: f,
        default_f: f,
        max_f: f,
        ..Default::default()
    };

    for (label, distinct_per_span) in [
        ("low-cardinality", 250u64),
        ("all-distinct", SUPERZONE_ROWS),
    ] {
        let mut built = Vec::new();
        for summary in [false, true] {
            let mut col = ColumnFilter::with_options(
                0,
                DbType::U64,
                HashSeed::default(),
                cfg,
                false,
                summary,
            );
            for slot in 0..N {
                let span = slot / SUPERZONE_ROWS;
                let v = span * 1_000_000_000 + (slot % distinct_per_span);
                col.set(slot, Some(&v.to_le_bytes()));
            }
            built.push(col);
        }

        let mut bm = vec![0u64; bitmap_words(N)];
        let mut timings = Vec::new();
        for col in &built {
            // Absent values: the case the summary exists to short-circuit.
            let reps = 200;
            let start = std::time::Instant::now();
            for i in 0..reps {
                let absent = 800_000_000_000u64 + i;
                col.probe_hash_into(
                    rasterizeddb_core::core::filter::canonical_hash(
                        &DbType::U64,
                        &absent.to_le_bytes(),
                        HashSeed::default(),
                    )
                    .unwrap(),
                    N,
                    &mut bm,
                );
            }
            timings.push(start.elapsed().as_secs_f64() / reps as f64);
        }

        println!(
            "span summary [{}]: absent-value probe {:.0} us -> {:.0} us ({:.2}x),              storage {} -> {} bytes (+{:.3} bits/row)",
            label,
            timings[0] * 1e6,
            timings[1] * 1e6,
            timings[0] / timings[1],
            built[0].bytes(),
            built[1].bytes(),
            (built[1].bytes() - built[0].bytes()) as f64 * 8.0 / N as f64,
        );
    }
}

/// Storage overhead actually observed, in bits per row per column.
fn report_overhead(_c: &mut Criterion) {
    const N: u64 = 1_000_000;
    for f in [4u8, 8, 12, 16] {
        let cfg = FilterConfig {
            min_f: f,
            default_f: f,
            max_f: f,
            ..Default::default()
        };
        let mut col = ColumnFilter::new(0, DbType::U64, HashSeed::default(), cfg);
        for slot in 0..N {
            col.set(slot, Some(&fastrand::u64(..).to_le_bytes()));
        }
        println!(
            "overhead f={:2}: {} bytes for {} rows = {:.3} bits/row (kernel: {})",
            f,
            col.bytes(),
            N,
            col.bytes() as f64 * 8.0 / N as f64,
            active_kernel()
        );
    }
}

/// Checkpoint round-trip: what persisting and reloading the index costs.
fn bench_checkpoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter/checkpoint");
    const N: u64 = 1_000_000;

    let fields: smallvec::SmallVec<[SchemaField; 20]> = smallvec::smallvec![
        SchemaField::new("id".into(), Ty::U64, 8, 0, 0, false),
        SchemaField::new("k".into(), Ty::I32, 4, 0, 1, false),
    ];
    let filters = TableFilters::new(&fields, FilterConfig::default());
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    for slot in 0..N {
        let cols: smallvec::SmallVec<[(u32, Vec<u8>); 32]> = smallvec::smallvec![
            (0u32, fastrand::u64(..).to_le_bytes().to_vec()),
            (1u32, (fastrand::i32(..)).to_le_bytes().to_vec()),
        ];
        filters.backfill_row(proof, slot, &cols);
    }

    let bytes = filters.checkpoint_bytes(N);
    println!(
        "checkpoint: {} bytes for {} rows x 2 columns = {:.3} bits/row/col",
        bytes.len(),
        N,
        bytes.len() as f64 * 8.0 / (N as f64 * 2.0)
    );

    group.throughput(Throughput::Bytes(bytes.len() as u64));
    group.bench_function("serialize", |b| {
        b.iter(|| black_box(filters.checkpoint_bytes(black_box(N))).len())
    });
    group.bench_function("deserialize", |b| {
        b.iter(|| {
            // `now_slots` is the pointer file's current slot count, which a
            // checkpoint may not exceed. The blob was serialised claiming `N`,
            // so `N` is what a file it actually describes would hold.
            let f = TableFilters::from_checkpoint(
                black_box(&bytes),
                &fields,
                FilterConfig::default(),
                N,
            )
            .unwrap();
            black_box(f.resume_slot())
        })
    });
    group.finish();
}

/// Range pruning via zone bounds, and what the bounds cost.
fn bench_zonemap(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter/zonemap");
    const N: u64 = 1_000_000;
    group.throughput(Throughput::Elements(N));

    let cfg = FilterConfig::default();

    // Append-ordered column: the case zone maps exist for.
    let mut ordered = ColumnFilter::new(0, Ty::I64, HashSeed::default(), cfg);
    for slot in 0..N {
        ordered.set(slot, Some(&(slot as i64).to_le_bytes()));
    }
    // Random column: bounds overlap, so almost nothing prunes.
    let mut random = ColumnFilter::new(0, Ty::I64, HashSeed::default(), cfg);
    for slot in 0..N {
        random.set(slot, Some(&fastrand::i64(..).to_le_bytes()));
    }

    let mut bm = vec![0u64; bitmap_words(N)];
    let pivot = ((N as i64) * 9) / 10;

    ordered.probe_pred_into(SargOp::Gt, &pivot.to_le_bytes(), N, &mut bm);
    let ordered_survivors: u64 = bm.iter().map(|w| w.count_ones() as u64).sum();
    random.probe_pred_into(SargOp::Gt, &pivot.to_le_bytes(), N, &mut bm);
    let random_survivors: u64 = bm.iter().map(|w| w.count_ones() as u64).sum();

    let fp_only = ColumnFilter::new(0, Ty::STRING, HashSeed::default(), cfg);
    println!(
        "zonemap: `col > {}` over {} rows -> ordered data leaves {} candidates ({:.4}), \
random data leaves {} ({:.4}); bounds add {} bytes = {:.3} bits/row (string col has zonemap: {})",
        pivot,
        N,
        ordered_survivors,
        ordered_survivors as f64 / N as f64,
        random_survivors,
        random_survivors as f64 / N as f64,
        ordered.zonemap().map_or(0, |z| z.bytes()),
        ordered.zonemap().map_or(0, |z| z.bytes()) as f64 * 8.0 / N as f64,
        fp_only.has_zonemap(),
    );

    group.bench_function("range_sweep/ordered", |b| {
        b.iter(|| {
            ordered.probe_pred_into(SargOp::Gt, black_box(&pivot.to_le_bytes()), N, &mut bm);
            black_box(bm[0])
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_probe_kernel,
    bench_column_sweep,
    bench_hash,
    bench_insert,
    bench_checkpoint,
    bench_zonemap,
    report_overhead,
    report_insert_contention,
    report_span_summary
);
criterion_main!(benches);
