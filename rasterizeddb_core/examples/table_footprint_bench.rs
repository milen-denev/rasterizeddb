//! Resident memory per open table.
//!
//! Opens `n` tables (each: rows, pointers, schema, filters and sequence
//! providers, plus its schema) over the file backend in `dir`, and reports the
//! growth in resident set size per table. Everything it creates is deleted
//! when it finishes.
//!
//!   cargo run --release -p rasterizeddb_core --example table_footprint_bench -- <dir> [n]

use rclite::Arc;

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::row::table::Table;
use rasterizeddb_core::core::rql::lexer_ct::CreateColumnData;
use rasterizeddb_core::core::storage_providers::file_sync::LocalStorageProvider;

fn rss() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
    let pages: u64 = statm.split_whitespace().nth(1).unwrap().parse().unwrap();
    pages * 4096
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("a directory, on tmpfs");
    let n: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(1000);
    let _ = rasterizeddb_core::MAX_PERMITS_THREADS.set(4);
    let _ = rasterizeddb_core::BATCH_SIZE.set(512);

    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let root = format!("{dir}/footprint");
                let _ = std::fs::remove_dir_all(&root);
                std::fs::create_dir_all(&root).unwrap();
                println!(
                    "size_of: LocalStorageProvider {} B, TableSchema {} B, Table {} B",
                    std::mem::size_of::<LocalStorageProvider>(),
                    std::mem::size_of::<rasterizeddb_core::core::row::schema::TableSchema>(),
                    std::mem::size_of::<Table<LocalStorageProvider>>()
                );
                // Warm-up table, so one-time allocations are not counted.
                let mut tables = Vec::with_capacity(n + 1);
                tables.push(open(&root, "warm").await);
                let before = rss();
                for i in 0..n {
                    tables.push(open(&root, &format!("t{i}")).await);
                }
                let after = rss();
                println!(
                    "{n} tables: RSS +{:.2} MiB, {:.0} B per table",
                    (after - before) as f64 / (1 << 20) as f64,
                    (after - before) as f64 / n as f64
                );
                drop(tables);
                let _ = std::fs::remove_dir_all(&root);
            });
            rt.shutdown_timeout(std::time::Duration::from_secs(1));
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn open(root: &str, name: &str) -> Table<LocalStorageProvider> {
    let io = Arc::new(LocalStorageProvider::new(root, Some(&format!("{name}_init.db"))).await);
    Table::new(
        name,
        io,
        vec![
            CreateColumnData {
                name: "id".into(),
                data_type: DbType::U64,
            },
            CreateColumnData {
                name: "label".into(),
                data_type: DbType::STRING,
            },
        ],
    )
    .await
}
