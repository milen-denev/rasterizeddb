//! The filter index and the monotonic-order shortcut against PostgreSQL's
//! float order, on a real table: NaN is above every number (+Infinity
//! included) and -0 equals +0.
//!
//! Zone bounds used to leave NaN out, which made two things wrong at once: a
//! zone holding NaN looked as if nothing in it could satisfy `f > x`, and a
//! float column with a NaN early in the table still looked ascending, so
//! `ORDER BY f LIMIT k` answered from the leading slots and returned the NaN
//! among the smallest values.

use rclite::Arc;
use smallvec::SmallVec;

use crate::core::db_type::DbType;
use crate::core::row::row::{ColumnFetchingData, ColumnWritePayload, Row, RowFetch, RowWrite};
use crate::core::row::schema::{SchemaCalculator, SchemaField};
use crate::core::row::table::Table;
use crate::core::rql::lexer_ct::CreateColumnData;
use crate::core::storage_providers::file_sync::LocalStorageProvider;
use crate::memory_pool::MEMORY_POOL;

fn init() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
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

fn full_fetch(fields: &SmallVec<[SchemaField; 20]>) -> RowFetch {
    let calc = SchemaCalculator::default();
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
    RowFetch {
        columns_fetching_data: cols,
    }
}

async fn table_of(
    dir: &std::path::Path,
    values: &[f64],
) -> Table<LocalStorageProvider> {
    init();
    let io = Arc::new(LocalStorageProvider::new(dir.to_str().unwrap(), Some("fl")).await);
    let table = Table::new(
        "fl",
        io,
        vec![
            CreateColumnData {
                name: "id".into(),
                data_type: DbType::U64,
            },
            CreateColumnData {
                name: "f".into(),
                data_type: DbType::F64,
            },
        ],
    )
    .await;
    for (i, v) in values.iter().enumerate() {
        let mut rw = RowWrite {
            columns_writing_data: SmallVec::new(),
        };
        rw.columns_writing_data
            .push(payload(&(i as u64).to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data
            .push(payload(&v.to_le_bytes(), 1, DbType::F64));
        table.insert_row(rw).await.unwrap();
    }
    table
}

async fn query(
    table: &Table<LocalStorageProvider>,
    where_clause: &str,
    limit: Option<u64>,
    order_by: Option<&str>,
) -> Vec<u64> {
    let rows: Vec<Row> = table
        .query_row(
            where_clause,
            &table.schema.fields.to_vec(),
            full_fetch(&table.schema.fields),
            full_fetch(&table.schema.fields),
            limit,
            order_by.map(|s| s.to_string()),
        )
        .await
        .unwrap();
    rows.iter()
        .map(|r| {
            let c = r.columns.iter().find(|c| c.schema_id == 0).unwrap();
            u64::from_le_bytes(c.data.into_slice().try_into().unwrap())
        })
        .collect()
}

fn pg_cmp(a: f64, b: f64) -> std::cmp::Ordering {
    crate::core::processor::concurrent_processor::pg_float_cmp(a, b)
}

/// Every float predicate on a table big enough to prune returns exactly the
/// rows PostgreSQL's order says it should — NaN, ±Infinity and ±0 placed at
/// zone boundaries and inside zones whose other values are far from the probe.
#[tokio::test(flavor = "multi_thread")]
async fn float_predicates_never_lose_a_row_to_the_index() {
    let dir = tempfile::tempdir().unwrap();
    const N: usize = 6000;
    // Distinct per row (a low-cardinality column is gated out of the index
    // and prunes nothing), but each zone a narrow band: zone z holds
    // [z, z + 0.5).
    let mut values: Vec<f64> = (0..N)
        .map(|i| (i / 512) as f64 + (i % 512) as f64 / 1024.0)
        .collect();
    for (at, v) in [
        (0, f64::NAN),
        (511, f64::NAN),
        (512, f64::INFINITY),
        (1023, f64::NEG_INFINITY),
        (1024, -0.0),
        (2047, 0.0),
        (3000, f64::from_bits(0xfff8_0000_0000_0001)),
        (5999, f64::NAN),
    ] {
        values[at] = v;
    }
    let table = table_of(dir.path(), &values).await;
    let mut wrong = Vec::new();
    let mut pruned = 0;
    for (clause, probe, op) in [
        ("f > 10.5", 10.5, ">"),
        ("f >= 11", 11.0, ">="),
        ("f > 'Infinity'", f64::INFINITY, ">"),
        ("f >= 'Infinity'", f64::INFINITY, ">="),
        ("f = 'NaN'", f64::NAN, "="),
        ("f >= 'NaN'", f64::NAN, ">="),
        ("f < 0", 0.0, "<"),
        ("f <= 0", 0.0, "<="),
        ("f = 0", 0.0, "="),
        ("f = '-0'", -0.0, "="),
        ("f < '-Infinity'", f64::NEG_INFINITY, "<"),
        ("f <= '-Infinity'", f64::NEG_INFINITY, "<="),
        ("f = 3.25", 3.25, "="),
        ("f > 1 AND f < 2", 0.0, "range"),
    ] {
        use std::cmp::Ordering::*;
        let mut want: Vec<u64> = values
            .iter()
            .enumerate()
            .filter(|(_, v)| {
                let o = pg_cmp(**v, probe);
                match op {
                    ">" => o == Greater,
                    ">=" => o != Less,
                    "<" => o == Less,
                    "<=" => o != Greater,
                    "=" => o == Equal,
                    _ => pg_cmp(**v, 1.0) == Greater && pg_cmp(**v, 2.0) == Less,
                }
            })
            .map(|(i, _)| i as u64)
            .collect();
        want.sort();
        let mut got = query(&table, clause, None, None).await;
        got.sort();
        // The index has to have pruned something, or this proves nothing.
        if let Some(filters) = table.filters.as_ref() {
            let tokens = crate::core::tokenizer::query_tokenizer::tokenize(clause, &table.schema.fields);
            if let Some(program) = super::sarg::extract(&tokens, filters) {
                if let Some(mask) = filters.evaluate(&program, N as u64) {
                    if mask.count() < N as u64 {
                        pruned += 1;
                    }
                }
            }
        }
        if got != want {
            wrong.push(format!("{clause}: {} rows, want {}", got.len(), want.len()));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    assert!(pruned >= 8, "the index pruned for only {pruned} predicates");
}

/// `ORDER BY f LIMIT k` over a column that ascends with slot position except
/// for NaN: NaN sorts last, so it never belongs among the smallest.
#[tokio::test(flavor = "multi_thread")]
async fn a_nan_does_not_make_a_float_column_look_ascending() {
    let dir = tempfile::tempdir().unwrap();
    const N: usize = 6000;
    let mut values: Vec<f64> = (0..N).map(|i| i as f64).collect();
    values[2] = f64::NAN;
    values[700] = f64::NAN;
    let table = table_of(dir.path(), &values).await;
    assert_eq!(query(&table, "", Some(3), Some("f")).await, vec![0, 1, 3]);
    let mut top = query(&table, "", Some(3), Some("f DESC")).await;
    top[..2].sort();
    assert_eq!(top, vec![2, 700, N as u64 - 1]);

    // Without a NaN the shortcut still answers, and answers right.
    let dir2 = tempfile::tempdir().unwrap();
    let plain: Vec<f64> = (0..N).map(|i| i as f64).collect();
    let table2 = table_of(dir2.path(), &plain).await;
    assert_eq!(query(&table2, "", Some(3), Some("f")).await, vec![0, 1, 2]);
}

/// Zones that ascend against each other say nothing about the order *inside*
/// a zone. Values descending within each 512-row zone, ascending across them:
/// the three smallest are at slots 511, 510, 509, not 0, 1, 2.
#[tokio::test(flavor = "multi_thread")]
async fn ascending_zones_do_not_order_the_rows_inside_one() {
    let dir = tempfile::tempdir().unwrap();
    const N: usize = 6000;
    let values: Vec<f64> = (0..N)
        .map(|i| ((i / 512) * 512 + (511 - i % 512)) as f64)
        .collect();
    let table = table_of(dir.path(), &values).await;
    assert_eq!(query(&table, "", Some(3), Some("f")).await, vec![511, 510, 509]);
    assert_eq!(query(&table, "", Some(600), Some("f")).await.len(), 600);
    let first_600: Vec<u64> = query(&table, "", Some(600), Some("f")).await;
    let mut want: Vec<(f64, u64)> = values.iter().enumerate().map(|(i, v)| (*v, i as u64)).collect();
    want.sort_by(|a, b| pg_cmp(a.0, b.0));
    let want: Vec<u64> = want.iter().take(600).map(|(_, i)| *i).collect();
    assert_eq!(first_600, want);
}

/// A float range scan over 1M rows with zone pruning against the same scan
/// with float ranges unpruned (what NaN-blind bounds forced).
///
/// `cargo test --release -p rasterizeddb_core --lib -- --ignored float_range_scan_bench --nocapture`
/// Builds its ~100 MB table in a temporary directory (tmpfs: this measures
/// the scan's CPU, and per-insert fsync on disk made loading a million rows
/// take tens of minutes), removed afterwards.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn float_range_scan_bench() {
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    const N: usize = 1_000_000;
    let values: Vec<f64> = (0..N)
        .map(|i| if i % 100_003 == 7 { f64::NAN } else { i as f64 * 0.5 })
        .collect();
    let t = std::time::Instant::now();
    let table = table_of(root, &values).await;
    println!("loaded {N} rows in {:?}", t.elapsed());
    for clause in ["f > 499000", "f >= 250000 AND f < 250500"] {
        for round in 0..3 {
            for refuse in [true, false] {
                super::sarg::REFUSE_FLOAT_RANGES.store(refuse, Ordering::Relaxed);
                let t = std::time::Instant::now();
                let rows = query(&table, clause, None, None).await.len();
                println!(
                    "{clause:28} round {round} {:>9}: {:>10.2?} ({rows} rows)",
                    if refuse { "unpruned" } else { "pruned" },
                    t.elapsed()
                );
            }
        }
    }
    super::sarg::REFUSE_FLOAT_RANGES.store(false, Ordering::Relaxed);
    drop(table);
    drop(dir);
}
