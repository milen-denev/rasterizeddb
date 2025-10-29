use std::sync::Arc;
use clap::Parser;

use log::LevelFilter;
use rasterizeddb_core::{core::database::Database, BATCH_SIZE, MAX_PERMITS_THREADS};
use tokio::runtime::Builder;

use rasterizeddb_core::configuration::Configuration;

#[derive(Parser, Debug)]
#[command(name = "rasterizeddb_core", version, about = "RasterizedDB Core Server")]
struct Args {
    /// Database location directory
    #[arg(long, value_name = "PATH")]
    location: Option<String>,

    /// Batch size for processing
    #[arg(long = "batch-size", alias = "batch_size", value_name = "N")]
    batch_size: Option<usize>,

    /// Number of concurrent worker threads
    #[arg(long = "concurrent-threads", alias = "concurrent_threads", value_name = "N")]
    concurrent_threads: Option<usize>,

    /// Logging level (off, error, warn, info, debug, trace)
    #[arg(long = "log-level", alias = "log_level", value_name = "LEVEL")]
    log_level: Option<LevelFilter>,
}

fn main() -> std::io::Result<()> {
    // 32 MiB
    const STACK_SIZE: usize = 32 * 1024 * 1024;

    // Parse command line arguments with clap
    let args = Args::parse();

    let config = Configuration {
        location: args.location.clone(),
        batch_size: args.batch_size,
        concurrent_threads: args.concurrent_threads,
    };

    MAX_PERMITS_THREADS.set(config.concurrent_threads.unwrap_or(16)).unwrap();
    BATCH_SIZE.set(config.batch_size.unwrap_or(1024 * 64)).unwrap();

    let rt = Builder::new_multi_thread()
        .worker_threads(config.concurrent_threads.unwrap_or(*MAX_PERMITS_THREADS.get().unwrap()))
        .thread_stack_size(STACK_SIZE)
        .enable_all()
        .build()?;

    rt.block_on(async {
        let level = args.log_level.unwrap_or(LevelFilter::Info);
        env_logger::Builder::new()
            .filter_level(level)
            .init();

        #[cfg(debug_assertions)]
        let db_location = config.location.as_deref().unwrap_or("G:/Databases/Production/");
        
        #[cfg(not(debug_assertions))]
        let db_location = config.location.as_deref().expect("Database location must be provided with --location <path>");
        
        let database = Database::new(db_location).await;
        let arc_database = Arc::new(database);
        _ = Database::start_db(arc_database).await;
    });

    Ok(())
}
