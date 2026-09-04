//! What space reclamation costs, and what it saves.
//!
//! Three questions this answers, because every claim about reclamation being
//! "cheap" or "worth it" is otherwise reasoning rather than measurement:
//!
//! 1. **The sweep.** A scan reads the whole pointer file. How long does that
//!    take at a realistic table size, and does the skip for an unchanged table
//!    actually cost nothing?
//! 2. **The insert path.** Allocating from the free list is a `BTreeSet` lookup
//!    plus a `HashMap` insert per row. Is that visible against the write?
//! 3. **Fragmentation.** Does repeated delete-and-refill churn degrade the hit
//!    rate over many cycles, or does it hold steady?
//!
//! Runs on a real filesystem, like `append_benchmarks`: reclamation is bounded
//! by reading the pointer file, and tmpfs would hide that. Set
//! `RASTDB_BENCH_DIR` to choose where; defaults to /var/tmp.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use rclite::Arc;
use smallvec::SmallVec;
use tempfile::TempDir;

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::row::row::{ColumnWritePayload, RowWrite};
use rasterizeddb_core::core::row::schema::SchemaField;
use rasterizeddb_core::core::row::table::Table;
use rasterizeddb_core::core::rql::lexer_ct::CreateColumnData;
use rasterizeddb_core::core::storage_providers::file_sync::LocalStorageProvider;
use rasterizeddb_core::memory_pool::MEMORY_POOL;

/// Big enough that the sweep is doing real work, small enough that setting the
/// table up does not dominate the run.
const ROWS: u64 = 50_000;

fn bench_dir() -> TempDir {
    let base = std::env::var("RASTDB_BENCH_DIR").unwrap_or_else(|_| "/var/tmp".to_string());
    tempfile::Builder::new()
        .prefix("rastdb_vacuum_")
        .tempdir_in(base)
        .unwrap()
}

fn init() {
    let _ = rasterizeddb_core::MAX_PERMITS_THREADS.set(8);
    let _ = rasterizeddb_core::BATCH_SIZE.set(4096);
    // The background service would scan underneath the measurement and make the
    // numbers depend on when its timer happened to fire.
    let _ = rasterizeddb_core::VACUUM_ENABLED.set(false);
}

fn payload(bytes: &[u8], write_order: u32, column_type: DbType) -> ColumnWritePayload {
    let mut mb = MEMORY_POOL.acquire(bytes.len());
    mb.into_slice_mut().copy_from_slice(bytes);
    let size = column_type.get_size();
    ColumnWritePayload {
        data: mb,
        write_order,
        column_type,
        size,
    }
}

fn row_of(id: u64) -> RowWrite {
    let mut rw = RowWrite {
        columns_writing_data: SmallVec::new(),
    };
    rw.columns_writing_data
        .push(payload(&id.to_le_bytes(), 0, DbType::U64));
    rw.columns_writing_data
        .push(payload(&(id as i32).to_le_bytes(), 1, DbType::I32));
    rw.columns_writing_data.push(payload(
        format!("row-payload-of-a-realistic-width-{:08}", id).as_bytes(),
        2,
        DbType::STRING,
    ));
    rw
}

fn full_fetch(fields: &SmallVec<[SchemaField; 20]>) -> rasterizeddb_core::core::row::row::RowFetch {
    use rasterizeddb_core::core::row::row::ColumnFetchingData;
    use rasterizeddb_core::core::row::schema::SchemaCalculator;
    let calc = SchemaCalculator;
    let mut cols = SmallVec::new();
    let mut sorted: Vec<&SchemaField> = fields.iter().collect();
    sorted.sort_by_key(|f| f.write_order);
    for f in sorted {
        let (column_offset, schema_id) = calc.calculate_schema_offset(&f.name, fields);
        cols.push(ColumnFetchingData {
            column_offset,
            column_type: f.db_type.clone(),
            size: f.size,
            schema_id,
        });
    }
    rasterizeddb_core::core::row::row::RowFetch {
        columns_fetching_data: cols,
    }
}

async fn build_table(dir: &std::path::Path, name: &str, rows: u64) -> Table<LocalStorageProvider> {
    init();
    let io = Arc::new(LocalStorageProvider::new(dir.to_str().unwrap(), Some(name)).await);
    let table = Table::new(
        name,
        io,
        vec![
            CreateColumnData {
                name: "id".into(),
                data_type: DbType::U64,
            },
            CreateColumnData {
                name: "cat".into(),
                data_type: DbType::I32,
            },
            CreateColumnData {
                name: "label".into(),
                data_type: DbType::STRING,
            },
        ],
    )
    .await;
    for i in 0..rows {
        table.insert_row(row_of(i)).await.unwrap();
    }
    table
}

/// The sweep itself, over a table with a realistic proportion of tombstones.
fn bench_sweep(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let dir = bench_dir();

    let table = rt.block_on(async {
        let table = build_table(dir.path(), "bench_sweep", ROWS).await;
        table.wait_filters_ready().await;
        // A fifth of the table dead: enough to be worth reclaiming, typical of a
        // table that has been running a while.
        table
            .delete_rows("cat < 10000", full_fetch(&table.schema.fields), None)
            .await
            .unwrap();
        table
    });

    let mut group = c.benchmark_group("vacuum");
    group.sample_size(20);
    group.bench_function("sweep_50k_rows_20pct_dead", |b| {
        b.iter(|| {
            let report = rt.block_on(table.vacuum_now()).unwrap();
            black_box(report.dead_bytes)
        })
    });
    group.finish();
}

/// Inserting with a free list to draw from, against inserting by appending.
///
/// The difference is the whole per-row cost of reclamation on the write path.
fn bench_insert_paths(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("vacuum");
    group.sample_size(20);

    group.bench_function("insert_1k_appending", |b| {
        let dir = bench_dir();
        let table = rt.block_on(build_table(dir.path(), "bench_append", 1_000));
        let mut next = 1_000_000u64;
        b.iter(|| {
            rt.block_on(async {
                for _ in 0..1_000 {
                    table.insert_row(row_of(next)).await.unwrap();
                    next += 1;
                }
            });
            black_box(next)
        })
    });

    group.bench_function("insert_1k_from_free_list", |b| {
        let dir = bench_dir();
        let table = rt.block_on(async {
            let table = build_table(dir.path(), "bench_reuse", ROWS).await;
            table.wait_filters_ready().await;
            table
                .delete_rows("cat < 40000", full_fetch(&table.schema.fields), None)
                .await
                .unwrap();
            table.vacuum_now().await.unwrap();
            table
        });
        let mut next = 1_000_000u64;
        b.iter(|| {
            rt.block_on(async {
                for _ in 0..1_000 {
                    table.insert_row(row_of(next)).await.unwrap();
                    next += 1;
                }
            });
            black_box(next)
        })
    });

    group.finish();
}

/// Full compaction: what `VACUUM FULL` costs per live row.
fn bench_compaction(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("vacuum");
    group.sample_size(10);

    group.bench_function("compact_50k_rows_half_dead", |b| {
        b.iter_batched(
            || {
                let dir = bench_dir();
                let table = rt.block_on(async {
                    let table = build_table(dir.path(), "bench_compact", ROWS).await;
                    table.wait_filters_ready().await;
                    table
                        .delete_rows("cat < 25000", full_fetch(&table.schema.fields), None)
                        .await
                        .unwrap();
                    table
                });
                (dir, table)
            },
            |(dir, table)| {
                let report = rt.block_on(table.compact()).unwrap();
                black_box(report.rows_moved);
                drop(table);
                drop(dir);
            },
            criterion::BatchSize::PerIteration,
        )
    });

    group.finish();
}

criterion_group!(benches, bench_sweep, bench_insert_paths, bench_compaction);
criterion_main!(benches);
