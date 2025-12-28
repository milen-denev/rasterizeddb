use std::{env, sync::Arc};
use clap::Parser;

use log::LevelFilter;
use rasterizeddb_core::{BATCH_SIZE, ENABLE_SEMANTICS, MAX_PERMITS_THREADS, cache::atomic_cache::AtomicGenericCache, core::{database::Database, processor::concurrent_processor::{ATOMIC_CACHE, ENABLE_CACHE}, row::row_pointer::RowPointer}};
use tokio::runtime::Builder;

use rasterizeddb_core::configuration::Configuration;

#[derive(Parser, Debug)]
#[command(name = "rasterizeddb_core", version, about = "RasterizedDB Core Server")]
struct Args {
    /// Database location directory path (default: none, required)
    #[arg(long, value_name = "PATH")]
    location: Option<String>,

    /// Batch size for processing (default: 65536)
    #[arg(long = "batch-size", alias = "batch_size", value_name = "N")]
    batch_size: Option<usize>,

    /// Number of concurrent worker threads (default: 16)
    #[arg(long = "concurrent-threads", alias = "concurrent_threads", value_name = "N")]
    concurrent_threads: Option<usize>,

    /// Logging level off, error, warn, info, debug, trace (default: error)
    #[arg(long = "log-level", alias = "log_level", value_name = "LEVEL")]
    log_level: Option<LevelFilter>,

    /// Cache size in number of entries (default: 100_000)
    #[arg(long = "cache-size", alias = "cache_size", value_name = "N")]
    cache_size: Option<usize>,

    /// Enable or disable cache (default: true)
    #[arg(long = "enable-cache", alias = "enable_cache", value_name = "BOOL")]
    enable_cache: Option<bool>,

    /// Enable or disable SME semantics (default: true)
    #[arg(long = "enable-semantics", alias = "enable_semantics", value_name = "BOOL")]
    enable_semantics: Option<bool>,
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

    let cache_size = args.cache_size.unwrap_or(100_000);
    let min_size = if cache_size < 1024 { 1024 } else { cache_size };

    //#[cfg(debug_assertions)]
    let enable_cache = args.enable_cache.unwrap_or(false);

    //#[cfg(not(debug_assertions))]
    //let enable_cache = args.enable_cache.unwrap_or(true);

    if enable_cache {
        ATOMIC_CACHE.get_or_init(|| {
            AtomicGenericCache::<u64, rclite::Arc<Vec<RowPointer>>>::new(min_size, cache_size)
        });

        ENABLE_CACHE.get_or_init(|| {
            true
        });
    } else {
        ENABLE_CACHE.get_or_init(|| {
            false
        });
    }

    let enable_semantics = args.enable_semantics.unwrap_or(true);
    ENABLE_SEMANTICS.get_or_init(|| enable_semantics);

    let rt = Builder::new_multi_thread()
        .worker_threads(config.concurrent_threads.unwrap_or(*MAX_PERMITS_THREADS.get().unwrap()))
        .thread_stack_size(STACK_SIZE)
        .enable_all()
        .build()?;

    rt.block_on(async {
        unsafe { env::set_var("RUST_BACKTRACE", "0"); }
        let level = args.log_level.unwrap_or(LevelFilter::Error);
        env_logger::Builder::new()
            .filter_level(level)
            .init();

        //#[cfg(debug_assertions)]
        let db_location = config.location.as_deref().unwrap_or("G:/Databases/Production/");
        
        //#[cfg(not(debug_assertions))]
        //let db_location = config.location.as_deref().expect("Database location must be provided with --location <path>");
        
        let database = Database::new(db_location).await;
        let arc_database = Arc::new(database);
        _ = Database::start_db(arc_database).await;
    });

    Ok(())
}