use std::env;

use rclite::Arc;
use clap::Parser;

use log::LevelFilter;
use rasterizeddb_core::{BATCH_SIZE, ENABLE_SEMANTICS, MAX_PERMITS_THREADS, cache::atomic_cache::AtomicGenericCache, core::{database::Database, processor::concurrent_processor::{ATOMIC_CACHE, ENABLE_CACHE}, row::row_pointer::RowPointer}};
use tokio::runtime::Builder;

use rasterizeddb_core::configuration::Configuration;
use clap::ValueEnum;

#[derive(Clone, Debug, ValueEnum)]
enum PgTlsMode {
    /// No TLS. Uses cleartext only.
    Disable,
    /// Use TLS if client requests it (SSLRequest), otherwise allow cleartext.
    Prefer,
    /// Require TLS (reject cleartext StartupMessage).
    Require,
}
#[derive(Clone, Debug, ValueEnum)]
enum Protocol {
    /// Custom rastcp protocol (existing default)
    Rastcp,
    /// PostgreSQL wire protocol (pgwire) for network-level Postgres compatibility
    Pgwire,
}

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

    /// Server protocol to use: rastcp or pgwire (default: rastcp)
    #[arg(long, value_enum, default_value_t = Protocol::Rastcp)]
    protocol: Protocol,

    /// pgwire listen address (default: 127.0.0.1)
    #[arg(long = "pg-addr", default_value = "127.0.0.1")]
    pg_addr: String,

    /// pgwire listen port (default: 5432)
    #[arg(long = "pg-port", default_value_t = 5432)]
    pg_port: u16,

    /// pgwire TLS mode (default: require)
    #[arg(long = "pg-tls", value_enum, default_value_t = PgTlsMode::Require)]
    pg_tls: PgTlsMode,

    /// Path to PEM-encoded TLS certificate chain for pgwire (optional)
    #[arg(long = "pg-cert", value_name = "PATH")]
    pg_cert: Option<String>,

    /// Path to PEM-encoded TLS private key for pgwire (optional)
    #[arg(long = "pg-key", value_name = "PATH")]
    pg_key: Option<String>,
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

    #[cfg(debug_assertions)]
    let enable_cache = args.enable_cache.unwrap_or(false);

    #[cfg(not(debug_assertions))]
    let enable_cache = args.enable_cache.unwrap_or(true);

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

    let enable_semantics = args.enable_semantics.unwrap_or(false);
    ENABLE_SEMANTICS.get_or_init(|| enable_semantics);

    let rt = Builder::new_multi_thread()
        .worker_threads(config.concurrent_threads.unwrap_or(*MAX_PERMITS_THREADS.get().unwrap()))
        .thread_stack_size(STACK_SIZE)
        .enable_all()
        .build()?;

    rt.block_on(async {
        unsafe { env::set_var("RUST_BACKTRACE", "full"); }
        let level = args.log_level.unwrap_or(LevelFilter::Trace);
        env_logger::Builder::new()
            .filter_level(level)
            .init();

        #[cfg(debug_assertions)]
        let db_location = config.location.as_deref().unwrap_or("G:/Databases/Production_2/");
        
        #[cfg(not(debug_assertions))]
        let db_location = config.location.as_deref().expect("Database location must be provided with --location <path>");
        
        let database = Database::new(db_location).await;
        let arc_database = Arc::new(database);

        match args.protocol {
            Protocol::Rastcp => {
                _ = Database::start_db(arc_database).await;
            }
            Protocol::Pgwire => {
                // Run a PostgreSQL-wire-compatible server.
                let tls_mode = match args.pg_tls {
                    PgTlsMode::Disable => rasterizeddb_core::pgwire::TlsMode::Disable,
                    PgTlsMode::Prefer => rasterizeddb_core::pgwire::TlsMode::Prefer,
                    PgTlsMode::Require => rasterizeddb_core::pgwire::TlsMode::Require,
                };

                let tls = rasterizeddb_core::pgwire::TlsConfig {
                    mode: tls_mode,
                    cert_path: args.pg_cert.clone(),
                    key_path: args.pg_key.clone(),
                };

                _ = rasterizeddb_core::pgwire::start_pgwire(arc_database, &args.pg_addr, args.pg_port, tls).await;
            }
        }
    });

    Ok(())
}