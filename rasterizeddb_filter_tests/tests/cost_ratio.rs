//! What a masked-out slot costs against what a visited one costs.
//!
//! Two constants in the scan planner rest on this single ratio, in the same
//! direction, and neither was measured:
//!
//! - `min_pruning_ratio` declines a mask that leaves more than that share of the
//!   table as candidates, on the grounds that a bitmap test per record costs
//!   more than the fetches it saves. Where that line falls depends entirely on
//!   the ratio. It was 0.5 until this measurement moved it to 0.90.
//! - the leading-run override in `build_candidates` keeps a mask whose dead
//!   prefix is worth at least one batch, which trades the same two costs.
//!
//! Guessing here is how a heuristic ends up tuned around a mis-measured
//! constant, where the tuning absorbs the error and hides it. So this measures
//! it instead. Ignored by default because it is a measurement, not an
//! assertion — run it with:
//!
//! ```text
//! cargo test --release -p rasterizeddb_filter_tests --test cost_ratio -- --ignored --nocapture
//! ```

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::filter::settings::settings;
use rasterizeddb_core::core::row::row::RowWrite;
use rasterizeddb_core::core::row::table::Table;
use rasterizeddb_core::core::storage_providers::file_sync::LocalStorageProvider;
use rasterizeddb_filter_tests::*;
use smallvec::SmallVec;

const N: u64 = 200_000;

/// Payload bytes per row. The ratio depends on how expensive a fetch is, and a
/// fetch gets more expensive as the row file stops fitting in page cache — so
/// measuring one narrow table would report the most favourable case for
/// *declining* a mask, which is the wrong direction to be optimistic in.
const WIDTHS: [usize; 3] = [8, 400, 2000];

async fn build(dir: &std::path::Path, name: &str, width: usize) -> Table<LocalStorageProvider> {
    let t = open_table(
        dir,
        name,
        &[("id", DbType::U64), ("name", DbType::STRING)],
    )
    .await;
    for i in 0..N {
        let mut name_bytes = format!("n{}", i);
        // Padded on the right, so the value stays unique and no probe matches.
        while name_bytes.len() < width {
            name_bytes.push('x');
        }
        let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
        rw.columns_writing_data.push(payload(&i.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data.push(payload(name_bytes.as_bytes(), 1, DbType::STRING));
        t.insert_row(rw).await.unwrap();
    }
    t
}

/// Wall time for a scan that walks every row and matches none.
async fn scan_ns_per_row(t: &Table<LocalStorageProvider>) -> f64 {
    let mut best = f64::MAX;
    for _ in 0..5 {
        let start = std::time::Instant::now();
        let rows = t
            .query_row(
                "name = 'no-such-value-anywhere'",
                &t.schema.fields.to_vec(),
                full_fetch(&t.schema.fields),
                full_fetch(&t.schema.fields),
                None,
                None,
            )
            .await
            .unwrap();
        assert!(rows.is_empty(), "the probe value must match nothing");
        best = best.min(start.elapsed().as_secs_f64());
    }
    best * 1e9 / N as f64
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement, not an assertion"]
async fn bit_test_versus_row_fetch() {
    let dir = tempfile::tempdir().unwrap();
    init();
    settings().reset_all();

    println!("\n--- cost per row, {} rows, 0 matches ---", N);
    for width in WIDTHS {
        // Filtered: the mask for an absent value is all zeros, so every slot
        // takes the skip path.
        let on_name = format!("cost_on_{}", width);
        let filtered = build(dir.path(), &on_name, width).await;
        assert!(filtered.filters.is_some(), "the filtered table must have filters");
        let masked = scan_ns_per_row(&filtered).await;

        // Unfiltered: same rows, same query, every record decoded and fetched.
        let off_name = format!("cost_off_{}", width);
        run_sql(&format!("SET FILTER indexed = off FOR TABLE {}", off_name));
        let plain = build(dir.path(), &off_name, width).await;
        assert!(plain.filters.is_none(), "the unfiltered table must have no filters");
        let visited = scan_ns_per_row(&plain).await;

        println!(
            "  row ~{:>4} B  |  masked {:6.2} ns/row  visited {:7.2} ns/row  \
             ratio {:6.1}x  |  break-even pruning ratio {:.4}",
            width,
            masked,
            visited,
            visited / masked,
            1.0 - masked / visited,
        );
    }
    settings().reset_all();
    println!();
}

fn run_sql(s: &str) {
    rasterizeddb_core::core::filter::sql::execute(s).unwrap();
}

