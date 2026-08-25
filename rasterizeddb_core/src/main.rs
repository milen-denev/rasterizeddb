use std::env;

use clap::Parser;

use log::LevelFilter;
use rasterizeddb_core::{BATCH_SIZE, MAX_PERMITS_THREADS, cache::query_cache, core::database::Database};
use tokio::runtime::Builder;

use rasterizeddb_core::configuration::Configuration;
use clap::ValueEnum;

/// S3 backup and restore flags. Off by default and absent from the binary
/// entirely without the feature.
#[cfg(feature = "s3_backup")]
mod backup_args;

/// PostgreSQL import flags. Off by default and absent from the binary entirely
/// without the feature.
#[cfg(feature = "pg_import")]
mod pg_import_args;

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

    /// Batch size for processing (default: 10_000)
    #[arg(long = "batch-size", alias = "batch_size", value_name = "N")]
    batch_size: Option<usize>,

    /// Number of concurrent worker threads (default: 16)
    #[arg(long = "concurrent-threads", alias = "concurrent_threads", value_name = "N")]
    concurrent_threads: Option<usize>,

    /// Whether a commit waits for its fdatasync (default: on)
    ///
    /// PostgreSQL's `synchronous_commit`, and the same trade. `off` lets a
    /// commit return once its records are in the page cache, leaving the sync
    /// to the log writer.
    ///
    /// The bytes are in the kernel's cache, not the server's, so this process
    /// dying loses nothing — only an OS crash or a power loss can, and then
    /// only the last `--wal-writer-delay-ms` of transactions. It cannot corrupt
    /// the database either way: what is lost is whole transactions off the end
    /// of the log, never part of one.
    ///
    /// A session can override it for itself with `SET synchronous_commit = off`,
    /// and it can be changed on a running server with
    /// `SET CONFIG wal.synchronous_commit = off`.
    #[arg(long = "synchronous-commit", alias = "synchronous_commit", value_name = "on|off")]
    synchronous_commit: Option<String>,

    /// How long an asynchronous commit may go unsynced, in milliseconds
    /// (default: 200)
    ///
    /// The bound on what `--synchronous-commit off` can lose. Ignored while
    /// synchronous commit is on, because nothing is then left for the writer.
    #[arg(long = "wal-writer-delay-ms", alias = "wal_writer_delay_ms", value_name = "MS")]
    wal_writer_delay_ms: Option<u64>,

    /// Logging level off, error, warn, info, debug, trace (default: error)
    #[arg(long = "log-level", alias = "log_level", value_name = "LEVEL")]
    log_level: Option<LevelFilter>,

    /// Cache size in number of entries (default: 100_000)
    #[arg(long = "cache-size", alias = "cache_size", value_name = "N")]
    cache_size: Option<usize>,

    /// Enable or disable cache (default: false)
    #[arg(long = "enable-cache", alias = "enable_cache", value_name = "BOOL")]
    enable_cache: Option<bool>,

    /// Run an in-memory database (no files are created).
    /// Master switch for the bit-level filter index.
    #[arg(long = "filter", alias = "filter_enabled", value_name = "BOOL")]
    filter_enabled: Option<bool>,

    /// Index rows that predate the filter when a table opens.
    #[arg(long = "filter-backfill", alias = "filter_backfill", value_name = "BOOL")]
    filter_backfill: Option<bool>,

    /// Skip the candidate mask when it would prune less than this fraction.
    #[arg(long = "filter-min-pruning-ratio", value_name = "0..1")]
    filter_min_pruning_ratio: Option<f64>,

    /// Seconds between filter checkpoints by the maintenance task.
    #[arg(long = "filter-checkpoint-secs", value_name = "N")]
    filter_checkpoint_secs: Option<u64>,

    /// Floor on fingerprint bits per row (4..16).
    #[arg(long = "filter-min-f", value_name = "N")]
    filter_min_f: Option<u8>,

    /// Fingerprint bits used before a column's cardinality is known.
    #[arg(long = "filter-default-f", value_name = "N")]
    filter_default_f: Option<u8>,

    /// Ceiling on fingerprint bits per row (4..16). Higher costs storage and
    /// buys a lower false-positive rate.
    #[arg(long = "filter-max-f", value_name = "N")]
    filter_max_f: Option<u8>,

    /// Largest row fraction an equality predicate may match and still be worth
    /// indexing.
    #[arg(long = "filter-min-selectivity", value_name = "0..1")]
    filter_min_selectivity: Option<f64>,

    /// Tables smaller than this are not indexed.
    #[arg(long = "filter-min-table-rows", value_name = "N")]
    filter_min_table_rows: Option<u64>,

    /// Maintain per-zone min/max bounds for range pruning.
    #[arg(long = "filter-zone-bounds", value_name = "BOOL")]
    filter_zone_bounds: Option<bool>,

    /// Maintain a per-superzone span summary. Cuts how many index bytes a probe
    /// streams; does not change how many rows it fetches.
    #[arg(long = "filter-bloom", value_name = "BOOL")]
    filter_bloom: Option<bool>,

    /// Store a JSON/JSONB column by taking its documents apart into a table of
    /// their own, instead of storing each document as bytes. Default: off.
    ///
    /// Shredding makes a document's fields ordinary typed columns, which the
    /// filter index can prune on directly — worth having when documents share a
    /// small, stable set of keys. It cannot hold documents whose *keys are
    /// data*: the table grows a column per distinct key and each new one
    /// rewrites every row, so such a column loads quadratically.
    ///
    /// Only affects columns created from now on; what an existing column is, is
    /// recorded in its schema.
    #[arg(long = "json-shadow-tables", alias = "json_shadow_tables", value_name = "BOOL")]
    json_shadow_tables: Option<bool>,

    #[arg(long = "in-memory", alias = "in_memory", default_value_t = false)]
    in_memory: bool,

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

    /// How pgwire connections authenticate: trust, password, md5 or
    /// scram-sha-256 (default).
    #[arg(long = "pg-auth", default_value = "scram-sha-256")]
    pg_auth: String,

    /// Database a connection reaches when it names none, and the one that lives
    /// in the location directory itself rather than a subdirectory of it.
    #[arg(long = "pg-default-db", default_value = "postgres")]
    pg_default_db: String,

    /// Role created when the role catalogue is empty, so a fresh database can
    /// be connected to at all.
    #[arg(long = "pg-user", default_value = "rasterized")]
    pg_user: String,

    /// Password for that role. Without it the role can only be reached under
    /// `--pg-auth trust`, rather than being a passwordless superuser.
    ///
    /// Prefer the `RASTERIZEDDB_PG_PASSWORD` environment variable, which this
    /// falls back to: a password given on the command line is visible in `ps`
    /// to every other user on the machine.
    #[arg(long = "pg-password", value_name = "PASSWORD")]
    pg_password: Option<String>,

    // --- native (rastcp) protocol authentication --------------------------
    /// How native connections authenticate: `scram-sha-256` (default) or
    /// `trust`.
    ///
    /// `md5` and cleartext are not offered here — they exist in the PostgreSQL
    /// listener only for drivers that speak nothing else, and nothing but this
    /// crate speaks the native protocol.
    #[arg(long = "rdb-auth", default_value = "scram-sha-256")]
    rdb_auth: String,

    /// Address the native (rastcp) listener binds (default: 127.0.0.1).
    ///
    /// Binding an address other machines can reach is refused unless the
    /// listener actually authenticates — set `--rdb-password`, or have a role
    /// with a password, before opening it up.
    #[arg(long = "rdb-addr", value_name = "IP")]
    rdb_addr: Option<String>,

    /// Port the native (rastcp) listener binds (default: 61170).
    ///
    /// The replication listener's default is the *built-in* native port plus
    /// one and does not follow this; set `--replication-port` too when running
    /// more than one node on a host.
    #[arg(long = "rdb-port", value_name = "PORT")]
    rdb_port: Option<u16>,

    /// Role created for the native protocol when the role catalogue is empty.
    #[arg(long = "rdb-user", default_value = "rasterized")]
    rdb_user: String,

    /// Password for that role.
    ///
    /// Prefer the `RASTERIZEDDB_PASSWORD` environment variable, which this
    /// falls back to: a password given on the command line is visible in `ps`
    /// to every other user on the machine.
    ///
    /// With neither given, and no account in the database holding a password,
    /// the loopback listener accepts any caller and says so at start-up. It
    /// starts demanding a password as soon as there is one to demand.
    #[arg(long = "rdb-password", value_name = "PASSWORD")]
    rdb_password: Option<String>,

    // --- storage placement -------------------------------------------------
    /// An extra directory this database may write into, as
    /// `PATH`, `PATH:ROLE` or `PATH:ROLE:WEIGHT`. Repeatable.
    ///
    /// `ROLE` is `primary` (holds tables; the default) or `backup` (receives a
    /// mirrored copy of every write). `WEIGHT` multiplies a primary's share
    /// under the `free-space` policy.
    ///
    /// Applied on top of what the database's own pool file records, never
    /// replacing it: where each table's files are is persisted, and a flag that
    /// reset the pool would leave tables on a second disk unfindable.
    #[cfg(feature = "storage_pools")]
    #[arg(long = "storage-location", value_name = "PATH[:ROLE[:WEIGHT]]")]
    storage_location: Vec<String>,

    /// How a new table's directory is chosen: `primary` (default),
    /// `round-robin` or `free-space`.
    #[cfg(feature = "storage_pools")]
    #[arg(long = "storage-policy", value_name = "POLICY")]
    storage_policy: Option<String>,

    /// A location with less free space than this is not chosen for new tables.
    /// Written as `512MB`, `2GiB` or a plain byte count (default: 256MiB).
    #[cfg(feature = "storage_pools")]
    #[arg(long = "storage-min-free", value_name = "SIZE")]
    storage_min_free: Option<String>,

    // --- replication ------------------------------------------------------
    /// Replicate committed transactions to and from the configured peers.
    /// Every node accepts writes; see `--node-id` and `--peer`.
    #[arg(long = "replication", default_value_t = false)]
    replication: bool,

    /// This node's id. Must be unique across the cluster and non-zero, since it
    /// is half of every transaction's global identifier.
    #[arg(long = "node-id", alias = "node_id", value_name = "N")]
    node_id: Option<u64>,

    /// Cluster name. Nodes refuse to talk to peers naming a different one.
    #[arg(long = "replication-cluster", value_name = "NAME")]
    replication_cluster: Option<String>,

    /// Shared secret every member presents in its handshake.
    #[arg(long = "replication-token", value_name = "SECRET")]
    replication_token: Option<String>,

    /// Address the replication listener binds (default: 127.0.0.1).
    #[arg(long = "replication-addr", value_name = "IP")]
    replication_addr: Option<String>,

    /// Port the replication listener binds (default: the SQL port plus one).
    #[arg(long = "replication-port", value_name = "PORT")]
    replication_port: Option<u16>,

    /// A peer, as `name@host:port` or bare `host:port`. Repeatable.
    #[arg(long = "peer", value_name = "NAME@HOST:PORT")]
    peer: Vec<String>,

    /// How transactions are compressed on the wire: `none`, `zstd[:level]` or
    /// `auto[:zstd[:level]]` (default: `auto:zstd:1`).
    #[arg(long = "replication-compression", value_name = "SPEC")]
    replication_compression: Option<String>,

    /// How long a commit waits for its peers: `async` (default), `sync`,
    /// `quorum:N` or `all`.
    #[arg(long = "replication-mode", value_name = "MODE")]
    replication_mode: Option<String>,

    /// How long a synchronous commit waits before returning anyway.
    #[arg(long = "replication-ack-timeout-ms", value_name = "MS")]
    replication_ack_timeout_ms: Option<u64>,

    /// What to do with a transaction that will not apply: `abort` (default),
    /// `skip` or `last-writer-wins`.
    #[arg(long = "replication-conflict", value_name = "POLICY")]
    replication_conflict: Option<String>,

    /// Records in one outbound batch (default: 256).
    #[arg(long = "replication-batch-records", value_name = "N")]
    replication_batch_records: Option<usize>,

    /// Uncompressed bytes in one outbound batch (default: 1 MiB).
    #[arg(long = "replication-batch-bytes", value_name = "N")]
    replication_batch_bytes: Option<usize>,

    /// How long a sender waits for more records before sending a partial
    /// batch, in microseconds (default: 500).
    #[arg(long = "replication-linger-us", value_name = "US")]
    replication_linger_us: Option<u64>,

    /// Records a peer's queue holds before overflowing to the log (default:
    /// 8192).
    #[arg(long = "replication-queue", value_name = "N")]
    replication_queue: Option<usize>,

    /// Forward transactions received from one peer to the others. Off by
    /// default, which is right for a full mesh.
    #[arg(long = "replication-relay", default_value_t = false)]
    replication_relay: bool,

    /// History kept for peers that fall behind, in MiB (default: 256).
    #[arg(long = "replication-retention-mb", value_name = "N")]
    replication_retention_mb: Option<u64>,

    /// How peers prove they belong to the cluster: `insecure`, `pinned`
    /// (default when certificates are given) or `ca`.
    ///
    /// `insecure` encrypts but does not authenticate, so anyone able to
    /// intercept the link can impersonate a peer. It is refused on any listen
    /// address other than loopback.
    #[arg(long = "replication-auth", value_name = "MODE")]
    replication_auth: Option<String>,

    /// PEM certificate this node presents to its peers, in both directions.
    #[arg(long = "replication-cert", value_name = "PATH")]
    replication_cert: Option<String>,

    /// PEM private key for `--replication-cert`.
    #[arg(long = "replication-key", value_name = "PATH")]
    replication_key: Option<String>,

    /// PEM certificate authority for the cluster, under `--replication-auth ca`.
    #[arg(long = "replication-ca", value_name = "PATH")]
    replication_ca: Option<String>,

    /// SHA-256 fingerprint of a peer's certificate, under
    /// `--replication-auth pinned`. Repeatable; list every node.
    #[arg(long = "replication-peer-fingerprint", value_name = "HEX")]
    replication_peer_fingerprint: Vec<String>,

    /// Generate a self-signed replication identity into this directory, print
    /// its fingerprint, and exit. Run once per node.
    #[arg(long = "replication-generate-identity", value_name = "DIR")]
    replication_generate_identity: Option<String>,

    #[cfg(feature = "s3_backup")]
    #[command(flatten)]
    backup: backup_args::BackupArgs,

    #[cfg(feature = "pg_import")]
    #[command(flatten)]
    pg_import: pg_import_args::PgImportArgs,
}

/// Applies `--storage-location`, `--storage-policy` and `--storage-min-free`
/// to the database's pool.
///
/// Additive by design. The pool is *persisted* — unlike replication, where a
/// flag is the whole configuration and a runtime change is deliberately
/// forgotten at restart — because a location holds files. Re-adding a location
/// that is already there is therefore not an error worth stopping for: the
/// usual case is a service unit that names its disks on every start.
#[cfg(feature = "storage_pools")]
fn apply_storage_flags(config: &Configuration, location: &str) -> Result<(), String> {
    use rasterizeddb_core::core::storage::{PlacementPolicy, StorageManager, parse_bytes};

    if config.storage_locations.is_empty()
        && config.storage_policy.is_none()
        && config.storage_min_free.is_none()
    {
        return Ok(());
    }

    // A pool opened purely to rewrite its file, then dropped. The database that
    // opens a moment later reads the result — which is the only ordering that
    // gets the catalogue's own files mirrored from the first start.
    let pool = StorageManager::open(location);

    for spec in &config.storage_locations {
        let (path, role, weight) = parse_location_spec(spec)?;
        match pool.add_location(&path, role, weight, true) {
            Ok(added) => log::info!(
                "storage location {} ({}, weight {})",
                added,
                role.as_str(),
                weight
            ),
            // Already present, or overlapping one that is. Reported at info
            // rather than refused: the flag describes intent, and the intent is
            // already satisfied.
            Err(e) => log::info!("storage location {}: {}", path, e),
        }
    }

    if let Some(policy) = &config.storage_policy {
        let policy = PlacementPolicy::parse(policy).ok_or_else(|| {
            format!(
                "unknown --storage-policy {:?}: expected one of {}",
                policy,
                PlacementPolicy::names()
            )
        })?;
        pool.set_policy(policy);
    }

    if let Some(size) = &config.storage_min_free {
        let bytes = parse_bytes(size).ok_or_else(|| {
            format!(
                "--storage-min-free {:?} is not a size: write it as 512MB, 2GiB or a byte count",
                size
            )
        })?;
        pool.set_min_free_bytes(bytes);
    }

    Ok(())
}

/// Splits `PATH`, `PATH:ROLE` or `PATH:ROLE:WEIGHT`.
///
/// Read from the right, and each optional field only claimed when it parses as
/// what it should be — so an absolute Windows path or any path containing a
/// colon is not mistaken for a role.
#[cfg(feature = "storage_pools")]
fn parse_location_spec(
    spec: &str,
) -> Result<(String, rasterizeddb_core::core::storage::LocationRole, u32), String> {
    use rasterizeddb_core::core::storage::LocationRole;

    let mut parts: Vec<&str> = spec.split(':').collect();
    let mut weight = 1u32;
    let mut role = LocationRole::Primary;

    if parts.len() >= 2 {
        if let Ok(parsed) = parts[parts.len() - 1].trim().parse::<u32>() {
            if parsed == 0 {
                return Err(format!("{}: weight must be at least 1", spec));
            }
            weight = parsed;
            parts.pop();
        }
    }
    if parts.len() >= 2 {
        if let Some(parsed) = LocationRole::parse(parts[parts.len() - 1]) {
            role = parsed;
            parts.pop();
        }
    }

    let path = parts.join(":");
    if path.trim().is_empty() {
        return Err(format!("--storage-location {:?} has no path", spec));
    }
    Ok((path.trim().to_string(), role, weight))
}

/// Builds the replication configuration from the flags, or explains what is
/// wrong with them.
fn replication_config(
    args: &Args,
) -> Result<rasterizeddb_core::replication::ReplicationConfig, String> {
    use rasterizeddb_core::replication::{
        AuthMode, Compression, ConflictPolicy, PeerConfig, ReplicationConfig, SyncMode,
    };
    use std::time::Duration;

    let mut config = ReplicationConfig {
        node_id: args.node_id.unwrap_or(0),
        ..Default::default()
    };

    config.tls.cert_path = args.replication_cert.clone();
    config.tls.key_path = args.replication_key.clone();
    config.tls.ca_path = args.replication_ca.clone();
    config.tls.peer_fingerprints = args.replication_peer_fingerprint.clone();

    // Naming the mode wins. Otherwise it is inferred from what was supplied, so
    // an operator who went to the trouble of providing certificates gets them
    // used instead of silently running unauthenticated.
    config.auth = match args.replication_auth.as_deref() {
        Some(text) => AuthMode::parse(text).ok_or_else(|| {
            format!("--replication-auth {:?}: expected insecure, pinned or ca", text)
        })?,
        None if args.replication_ca.is_some() => AuthMode::Ca,
        None if args.replication_cert.is_some() => AuthMode::Pinned,
        None => AuthMode::Insecure,
    };

    if let Some(cluster) = args.replication_cluster.clone() {
        config.cluster = cluster;
    }
    if let Some(token) = args.replication_token.clone() {
        config.token = token;
    }
    if let Some(addr) = args.replication_addr.clone() {
        config.listen_addr = addr;
    }
    if let Some(port) = args.replication_port {
        config.listen_port = port;
    }
    if let Some(spec) = args.replication_compression.as_deref() {
        config.compression = Compression::parse(spec).ok_or_else(|| {
            format!(
                "--replication-compression {:?}: expected none, zstd[:level] or auto[:zstd[:level]]",
                spec
            )
        })?;
    }
    if let Some(mode) = args.replication_mode.as_deref() {
        config.sync_mode = SyncMode::parse(mode).ok_or_else(|| {
            format!("--replication-mode {:?}: expected async, sync, quorum:N or all", mode)
        })?;
    }
    if let Some(ms) = args.replication_ack_timeout_ms {
        config.ack_timeout = Duration::from_millis(ms);
    }
    if let Some(policy) = args.replication_conflict.as_deref() {
        config.conflict = ConflictPolicy::parse(policy).ok_or_else(|| {
            format!(
                "--replication-conflict {:?}: expected abort, skip or last-writer-wins",
                policy
            )
        })?;
    }
    if let Some(n) = args.replication_batch_records {
        config.batch_max_records = n;
    }
    if let Some(n) = args.replication_batch_bytes {
        config.batch_max_bytes = n;
    }
    if let Some(us) = args.replication_linger_us {
        config.batch_linger = Duration::from_micros(us);
    }
    if let Some(n) = args.replication_queue {
        config.queue_capacity = n;
    }
    config.relay = args.replication_relay;
    if let Some(mb) = args.replication_retention_mb {
        config.log_retention_bytes = mb * 1024 * 1024;
    }

    for flag in &args.peer {
        config.peers.push(PeerConfig::parse_flag(flag)?);
    }

    config.validate()?;
    Ok(config)
}

fn main() -> std::io::Result<()> {
    // 32 MiB
    const STACK_SIZE: usize = 32 * 1024 * 1024;

    // Parse command line arguments with clap
    let args = Args::parse();

    // Before anything opens a database: this writes two files and exits, so a
    // node can be given an identity without also being started.
    if let Some(dir) = args.replication_generate_identity.as_deref() {
        let mut names = vec!["localhost".to_string()];
        let addr = args
            .replication_addr
            .clone()
            .unwrap_or_else(|| "127.0.0.1".to_string());
        if !names.contains(&addr) {
            names.push(addr);
        }

        match rastcp::cert::write_self_signed_identity(std::path::Path::new(dir), &names) {
            Ok((cert_path, key_path, fingerprint)) => {
                println!("wrote {}", cert_path.display());
                println!("wrote {} (mode 0600)", key_path.display());
                println!();
                println!("fingerprint: {}", rastcp::tls::format_fingerprint(&fingerprint));
                println!();
                println!("Start this node with:");
                println!(
                    "  --replication-auth pinned --replication-cert {} --replication-key {}",
                    cert_path.display(),
                    key_path.display()
                );
                println!("and give every *other* node:");
                println!(
                    "  --replication-peer-fingerprint {}",
                    rastcp::tls::format_fingerprint(&fingerprint)
                );
                return Ok(());
            }
            Err(e) => {
                eprintln!("could not generate a replication identity in {}: {}", dir, e);
                std::process::exit(2);
            }
        }
    }

    let config = Configuration {
        location: args.location.clone(),
        batch_size: args.batch_size,
        concurrent_threads: args.concurrent_threads,
        #[cfg(feature = "storage_pools")]
        storage_locations: args.storage_location.clone(),
        #[cfg(not(feature = "storage_pools"))]
        storage_locations: Vec::new(),
        #[cfg(feature = "storage_pools")]
        storage_policy: args.storage_policy.clone(),
        #[cfg(not(feature = "storage_pools"))]
        storage_policy: None,
        #[cfg(feature = "storage_pools")]
        storage_min_free: args.storage_min_free.clone(),
        #[cfg(not(feature = "storage_pools"))]
        storage_min_free: None,
    };

    MAX_PERMITS_THREADS.set(config.concurrent_threads.unwrap_or(16)).unwrap();
    BATCH_SIZE.set(config.batch_size.unwrap_or(10_000)).unwrap();

    // Durability. These seed the same runtime state `SET synchronous_commit` and
    // `SET CONFIG wal.*` write, so a flag is the starting value rather than a
    // second mechanism — the same shape the filter flags below use.
    if let Some(spec) = args.synchronous_commit.as_deref() {
        match rasterizeddb_core::parse_synchronous_commit(spec) {
            Some(on) => rasterizeddb_core::set_synchronous_commit(on),
            None => {
                return Err(std::io::Error::other(format!(
                    "--synchronous-commit {:?}: expected on or off",
                    spec
                )));
            }
        }
    }
    if let Some(ms) = args.wal_writer_delay_ms {
        rasterizeddb_core::set_wal_writer_delay_ms(ms);
    }

    // Filter tuning. These seed the same runtime state `SET FILTER ...` writes,
    // so a flag is just the starting value rather than a separate mechanism.
    {
        let f = rasterizeddb_core::filter_settings();
        if let Some(v) = args.filter_enabled {
            f.set_enabled(v);
        }
        if let Some(v) = args.filter_backfill {
            f.set_backfill_on_open(v);
        }
        if let Some(v) = args.filter_min_pruning_ratio {
            f.set_min_pruning_ratio(v);
        }
        if let Some(v) = args.filter_checkpoint_secs {
            f.set_checkpoint_interval_secs(v);
        }
        f.update_global(|o| {
            if let Some(v) = args.filter_min_f {
                o.min_f = Some(v);
            }
            if let Some(v) = args.filter_default_f {
                o.default_f = Some(v);
            }
            if let Some(v) = args.filter_max_f {
                o.max_f = Some(v);
            }
            if let Some(v) = args.filter_min_selectivity {
                o.min_selectivity = Some(v);
            }
            if let Some(v) = args.filter_min_table_rows {
                o.min_table_rows = Some(v);
            }
            if let Some(v) = args.filter_bloom {
                o.bloom = Some(v);
            }
            if let Some(v) = args.filter_zone_bounds {
                o.zone_bounds = Some(v);
            }
        });
    }

    if let Some(v) = args.json_shadow_tables {
        let _ = rasterizeddb_core::JSON_SHADOW_TABLES.set(v);
        if v {
            log::info!(
                "JSON columns will be shredded into shadow tables; documents whose keys are \
                 data load quadratically under this setting"
            );
        }
    }

    let cache_size = args.cache_size.unwrap_or(100_000);
    let enable_cache = args.enable_cache.unwrap_or(false);

    // `init` sizes the slot array itself; the cache is only reachable once the
    // flag is set, so building it first keeps a query from ever seeing a
    // half-configured cache.
    if enable_cache {
        query_cache::init(cache_size);
    }

    query_cache::ENABLE_CACHE.get_or_init(|| enable_cache);

    let rt = Builder::new_multi_thread()
        .worker_threads(config.concurrent_threads.unwrap_or(rasterizeddb_core::max_permits_threads()))
        .thread_stack_size(STACK_SIZE)
        .enable_all()
        .build()?;

    rt.block_on(async {
        unsafe { env::set_var("RUST_BACKTRACE", "full"); }

        let level = args.log_level.unwrap_or(LevelFilter::Error);

        env_logger::Builder::new()
            .filter_level(level)
            .init();

        #[cfg(debug_assertions)]
        let db_location = if args.in_memory {
            ""
        } else {
            config.location.as_deref().unwrap_or("/mnt/Volume4/test_db")
        };

        #[cfg(not(debug_assertions))]
        let db_location = if args.in_memory {
            ""
        } else {
            config
                .location
                .as_deref()
                .expect("Database location must be provided with --location <path> unless --in-memory is set")
        };

        // Before anything opens a table: a restore rewrites the files the
        // engine is about to memory-map, and the WAL recovery that
        // `Database::new_with_backend` runs would otherwise replay the old
        // log over the newly restored data.
        #[cfg(feature = "s3_backup")]
        if args.backup.wants_restore() {
            match backup_args::run_restore(&args.backup, db_location).await {
                Ok(true) => return,
                Ok(false) => {}
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(2);
                }
            }
        }

        // Storage locations before the catalogue opens, not after. A backup
        // location decides whether a file is mirrored *when that file is
        // opened*, so a location applied to an already-open database would
        // leave the catalogue's own files unmirrored until the next restart —
        // and a backup directory holding every table but not the list of them
        // does not open as a database.
        //
        // Refused rather than warned about: an operator who asked for a second
        // disk and silently got one directory finds out when it fills.
        #[cfg(feature = "storage_pools")]
        if !args.in_memory {
            if let Err(e) = apply_storage_flags(&config, db_location) {
                eprintln!("{e}");
                std::process::exit(2);
            }
        }

        // One catalogue, holding the default database and every other database
        // found under the location. The root directory *is* the default
        // database, so a data directory written by a single-database build
        // opens as a catalogue of one with nothing moved.
        let catalog = match rasterizeddb_core::core::database::catalog::DatabaseCatalog::open(
            (!args.in_memory).then_some(db_location),
            &args.pg_default_db,
        )
        .await
        {
            Ok(catalog) => catalog,
            Err(e) => {
                eprintln!("could not open the database catalogue: {e}");
                std::process::exit(2);
            }
        };

        // An import builds a database and stops. Run here, after the catalogue
        // is open — it needs somewhere to create the new database — and before
        // replication, backups or a listener start, none of which have anything
        // to do with a one-shot migration.
        #[cfg(feature = "pg_import")]
        if args.pg_import.wants_import() {
            match pg_import_args::run_import(&args.pg_import, &catalog).await {
                Ok(true) => return,
                Ok(false) => {}
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(2);
                }
            }
        }

        // Replication, backups and the rastcp protocol are single-database and
        // act on the default one. Only the PostgreSQL listener routes by name.
        let arc_database = catalog
            .get(&args.pg_default_db)
            .expect("the catalogue always holds its default database");

        // Started before the SQL listener, so a node is ready to take its
        // peers' transactions by the time it is ready to take a client's.
        // Configuration is applied either way: a node started without
        // `--replication` can still be brought up later with
        // `REPLICATION ENABLE`, and the flags are what it comes up as.
        if args.node_id.is_some() || args.replication || !args.peer.is_empty() {
            match replication_config(&args) {
                Ok(replication) => {
                    if let Err(e) = arc_database.replication.set_config(replication) {
                        eprintln!("replication configuration rejected: {e}");
                        std::process::exit(2);
                    }
                    if args.replication {
                        if let Err(e) = Database::start_replication(arc_database.clone()) {
                            eprintln!("replication could not start: {e}");
                            std::process::exit(2);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(2);
                }
            }
        }

        // After the database is open, so the log exists for checkpoints to
        // hand their segments to, and so a first snapshot describes a
        // recovered database rather than one mid-replay.
        #[cfg(feature = "s3_backup")]
        if args.backup.enabled() {
            match args.backup.to_config() {
                Ok(config) => {
                    match rasterizeddb_core::core::backup::install(config, db_location).await {
                        Ok(service) => {
                            log::info!(
                                "Backing up to s3://{}/{} as instance {}",
                                service.config().bucket,
                                service.config().normalized_prefix(),
                                service.instance_id()
                            );
                            // Flush what is still staged before the process
                            // leaves, so a clean stop costs no recovery window.
                            let on_exit = service.clone();
                            rasterizeddb_core::shutdown::on_termination(move || {
                                let service = on_exit.clone();
                                tokio::spawn(async move { service.shutdown().await });
                            });
                            // Only on top of a catalogue that already holds
                            // one. An empty prefix has its base snapshot on the
                            // way from the service itself, and taking one here
                            // as well would upload the whole database twice on
                            // a first boot.
                            if args.backup.backup_now
                                && !service.manifest_snapshot().await.snapshots.is_empty()
                            {
                                if let Err(e) = service.snapshot_now().await {
                                    eprintln!("initial snapshot failed: {e}");
                                }
                            }
                        }
                        // Refusing to start: an operator who asked for backups
                        // and silently got none is worse off than one who is
                        // told why at boot.
                        Err(e) => {
                            eprintln!("could not start backups: {e}");
                            std::process::exit(2);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(2);
                }
            }
        }

        match args.protocol {
            Protocol::Rastcp => {
                let Some(method) = rasterizeddb_core::auth::AuthMethod::parse(&args.rdb_auth)
                else {
                    eprintln!(
                        "unknown --rdb-auth {:?}: expected trust or scram-sha-256",
                        args.rdb_auth
                    );
                    std::process::exit(2);
                };

                let auth = rasterizeddb_core::native_auth::NativeAuthConfig {
                    method,
                    bootstrap_user: args.rdb_user.clone(),
                    // The environment variable keeps the password out of `ps`.
                    bootstrap_password: args
                        .rdb_password
                        .clone()
                        .or_else(|| std::env::var("RASTERIZEDDB_PASSWORD").ok())
                        .filter(|password| !password.is_empty()),
                };

                if let Err(e) = auth.validate() {
                    eprintln!("{e}");
                    std::process::exit(2);
                }

                let mut listen =
                    rasterizeddb_core::core::database::NativeListen::default();
                if let Some(addr) = args.rdb_addr.clone() {
                    listen.addr = addr;
                }
                if let Some(port) = args.rdb_port {
                    listen.port = port;
                }

                if let Err(e) =
                    Database::start_db_with_auth_on(arc_database, auth, listen).await
                {
                    eprintln!("{e}");
                    std::process::exit(2);
                }
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
                    // With neither path given a self-signed certificate is
                    // generated at start-up, so an encrypted listener needs no
                    // preparation.
                    cert_path: args.pg_cert.clone(),
                    key_path: args.pg_key.clone(),
                };

                let Some(method) = rasterizeddb_core::auth::AuthMethod::parse(&args.pg_auth) else {
                    eprintln!(
                        "unknown --pg-auth {:?}: expected trust, password, md5 or scram-sha-256",
                        args.pg_auth
                    );
                    std::process::exit(2);
                };

                let auth = rasterizeddb_core::pgwire::AuthConfig {
                    method,
                    bootstrap_user: args.pg_user.clone(),
                    bootstrap_password: args
                        .pg_password
                        .clone()
                        .or_else(|| std::env::var("RASTERIZEDDB_PG_PASSWORD").ok())
                        .filter(|p| !p.is_empty()),
                };

                // The native listener refuses a public bind it cannot
                // authenticate; this one always authenticates, but a mode that
                // lets a client downgrade to cleartext puts the SCRAM exchange
                // — and every row after it — on the wire in the clear. Said
                // loudly rather than refused: an operator may terminate TLS in
                // front of the process.
                let public = args
                    .pg_addr
                    .parse::<std::net::IpAddr>()
                    .map(|ip| !ip.is_loopback())
                    .unwrap_or(true);
                if public && tls_mode != rasterizeddb_core::pgwire::TlsMode::Require {
                    log::warn!(
                        "pgwire is bound to {} with --pg-tls {:?}: clients may connect \
                         unencrypted over a network interface; use --pg-tls require \
                         unless TLS is terminated in front of this process",
                        args.pg_addr,
                        tls_mode
                    );
                }

                _ = rasterizeddb_core::pgwire::start_pgwire_with_catalog(
                    catalog,
                    &args.pg_addr,
                    args.pg_port,
                    tls,
                    auth,
                )
                .await;
            }
        }

        // Both servers return once they have drained, so this is the last thing
        // the process does: flush what the WAL still holds so the next start-up
        // has nothing to replay.
        rasterizeddb_core::shutdown::finalize();
    });

    Ok(())
}