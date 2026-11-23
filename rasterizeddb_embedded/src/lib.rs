//! RasterizedDB inside your process.
//!
//! [`Builder::start`] opens a database (a directory, or memory) and runs the
//! full server engine on a thread of its own, the way the `rasterizeddb_core`
//! binary runs it. It then serves the native protocol on a private endpoint,
//! and [`Embedded::connect`] hands back a [`rasterizeddb_client::Client`]
//! for it. Everything the client SDK offers works here unchanged: typed
//! queries, transactions, the ORM, migrations and the key/value API.
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use rasterizeddb_embedded::Builder;
//!
//! let db = Builder::open("./data").start().await?;
//! let client = db.connect().await?;
//! client.execute("CREATE TABLE IF NOT EXISTS notes (id BIGINT, body TEXT)", &[]).await?;
//! client.execute("INSERT INTO notes VALUES (1, 'hello')", &[]).await?;
//! let rows = client.query("SELECT body FROM notes WHERE id = 1", &[]).await?;
//! # let _ = rows;
//! db.shutdown().await?;
//! # Ok(()) }
//! ```
//!
//! # Why a thread and an endpoint of its own
//!
//! - **Stacks.** The engine runs every statement on 32 MiB stacks, as the
//!   server binary does; on tokio's 2 MiB default a nested statement can
//!   overflow. Its own runtime gives it those stacks whatever runtime (or
//!   none) the application uses.
//! - **One code path.** Statements reach the engine through the same native
//!   session layer a network client's do (transactions, views, sequences,
//!   functions, privileges), so an embedded database answers exactly as a
//!   server would.
//! - **Signals stay yours.** Nothing here installs a Ctrl+C or SIGTERM
//!   handler. Stop the engine with [`Embedded::shutdown`]: it drains the
//!   requests in flight, seals the index snapshots and checkpoints the
//!   write-ahead log, so the next open has nothing to replay. Dropping an
//!   [`Embedded`] does the same, blocking the dropping thread until it is
//!   done.
//!
//! # The endpoint
//!
//! On unix it is a socket in a fresh `0700` directory under the temporary
//! directory, with the socket file `0600`. Only processes of the same
//! operating-system user can open it, so no password is needed. On other
//! platforms (Windows) there are no unix sockets, and the endpoint is a TLS
//! listener on `127.0.0.1` with a port the system picks. Any local process can
//! reach that port, so [`Builder::password`] is required there unless
//! [`Builder::allow_unauthenticated_loopback`] says otherwise.
//!
//! # CPU
//!
//! On x86-64 the engine needs AVX2 (an x86-64-v3 CPU: Intel Haswell, AMD
//! Excavator or later). [`Builder::start`] refuses on a CPU without it rather
//! than fail later with an illegal instruction. Build with
//! `-C target-cpu=x86-64-v3` (as the server binary is) so the vector paths
//! inline. aarch64 needs nothing extra.
//!
//! # Settings
//!
//! Everything the server binary takes as a flag, [`Builder`] takes as a
//! method: memory ([`Builder::low_memory`], [`Builder::malloc_arenas`],
//! [`Builder::index_residency`], [`Builder::join_memory_mb`],
//! [`Builder::kv_max_memory`], …), durability
//! ([`Builder::synchronous_commit`], [`Builder::wal_checkpoint_bytes`], …),
//! threads, and through [`Builder::config`] every key `SHOW CONFIG KEYS`
//! lists.
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use rasterizeddb_embedded::{Builder, IndexResidency};
//!
//! let db = Builder::open("./data")
//!     .worker_threads(4)
//!     .query_threads(4)
//!     .malloc_arenas(4)
//!     .index_residency(IndexResidency::Disk)
//!     .join_memory_mb(256)
//!     .synchronous_commit(false)
//!     .config("auto_compact.interval_secs", "300")
//!     .start()
//!     .await?;
//! # let _ = db;
//! # Ok(()) }
//! ```
//!
//! The keys come in two kinds, as on a server:
//!
//! - **Hot keys** (`wal.synchronous_commit`, `auto_compact.*`, `filter.*`,
//!   `partition.*`, `index_snapshot.*`, `query_stats.*`, `vacuum.enabled`)
//!   can also be changed on the running engine, with `SET CONFIG key = value`
//!   through any client. A Builder value is only where they start.
//! - **Startup keys** (`server.*`, `index.*`, `wal.checkpoint_bytes`,
//!   `wal.prealloc_kb`, `cache.enabled`) are read once. `SET CONFIG` on one
//!   saves it to `rasterizeddb.conf` in the database directory, and the next
//!   [`Builder::start`] on that directory applies it. A value given to the
//!   Builder wins over a saved one, as a server's flag does; `SHOW CONFIG`
//!   shows each value's source (a Builder value reads `command line`).
//!
//! **Settings are the process's, not one database's.** The engine keeps them
//! in process-wide state, so two [`Embedded`] in one process share them: the
//! second start's values replace the first's for both. A startup value that
//! cannot change any more, because an engine already runs in this process
//! with another, makes [`Builder::start`] fail rather than run with a value
//! nobody asked for.
//!
//! Freed memory goes back to the operating system once the engine has been
//! idle for a few seconds ([`Builder::idle_trim`], on by default, glibc
//! only).

mod settings;

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rasterizeddb_core::auth::AuthMethod;
use rasterizeddb_core::core::database::catalog::DatabaseCatalog;
use rasterizeddb_core::core::database::{DEFAULT_DATABASE_NAME, Database, NativeListen};
use rasterizeddb_core::native_auth::NativeAuthConfig;
use settings::{Deferred, Settings};
use tokio::sync::oneshot;

pub use rasterizeddb_client as client;
pub use rasterizeddb_client::{Client, Login, Options};
/// The engine itself, for what the client cannot express (counters, internal
/// state). Its API is the server's internals and changes between releases.
pub use rasterizeddb_core as engine;
/// Where indexes keep their entries; see [`Builder::index_residency`].
pub use rasterizeddb_core::core::index::paged::Residency as IndexResidency;
/// What an open does with the indexes; see [`Builder::index_storage`].
pub use rasterizeddb_core::core::index_store::load::settings::IndexStorage;
/// What the key/value store drops when full; see [`Builder::kv_eviction`].
pub use rasterizeddb_core::core::kv_cache::EvictionPolicy as KvEviction;
/// A vector instruction set; see [`Builder::simd`].
pub use rasterizeddb_core::simds::cpu::SimdLevel;

/// The stack every engine thread gets, as in the server binary.
const ENGINE_STACK: usize = 32 * 1024 * 1024;

/// The role a fresh database is given when [`Builder::password`] names none.
const DEFAULT_USER: &str = "rasterized";

enum Location {
    Memory,
    Directory(PathBuf),
}

/// What to open and how to serve it. Start with [`Builder::open`] or
/// [`Builder::in_memory`].
pub struct Builder {
    location: Location,
    worker_threads: Option<usize>,
    credentials: Option<(String, String)>,
    allow_unauthenticated_loopback: bool,
    request_timeout: Option<Duration>,
    settings: Settings,
}

impl Builder {
    /// A database kept in `directory`, created if it does not exist. A
    /// directory written by the server binary opens here, and the reverse.
    /// One process at a time may open a directory.
    pub fn open(directory: impl Into<PathBuf>) -> Self {
        Self::new(Location::Directory(directory.into()))
    }

    /// A database held in memory only, gone when the engine stops.
    pub fn in_memory() -> Self {
        Self::new(Location::Memory)
    }

    fn new(location: Location) -> Self {
        Builder {
            location,
            worker_threads: None,
            credentials: None,
            allow_unauthenticated_loopback: false,
            request_timeout: None,
            settings: Settings::default(),
        }
    }

    /// Threads the engine's runtime runs on (default: one per CPU, as
    /// tokio chooses). How many of them one query may use is
    /// [`Builder::query_threads`].
    pub fn worker_threads(mut self, threads: usize) -> Self {
        self.worker_threads = Some(threads.max(1));
        self
    }

    /// Require a SCRAM-SHA-256 login as `user`. A fresh database gets that
    /// role with this password; an existing one must already have it.
    /// Required off unix (see the crate documentation).
    pub fn password(mut self, user: impl Into<String>, password: impl Into<String>) -> Self {
        self.credentials = Some((user.into(), password.into()));
        self
    }

    /// Off unix, serve the loopback endpoint without authentication, so that
    /// any local process can use the database. Ignored on unix, whose socket
    /// admits this user only.
    pub fn allow_unauthenticated_loopback(mut self, allow: bool) -> Self {
        self.allow_unauthenticated_loopback = allow;
        self
    }

    /// How long one statement may run before it is abandoned (default
    /// 30 s, the server's). A connection idle this long is closed too; the
    /// client reopens it before its next statement.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }


    // ----------------------------------------------------------------
    // Settings. See the crate documentation, "Settings": they are
    // process-wide, and each method names the server flag it stands for.
    // ----------------------------------------------------------------

    /// Sets any key `SHOW CONFIG KEYS` lists, as `SET CONFIG key = value`
    /// spells it: `config("auto_compact.enabled", "off")`,
    /// `config("filter.max_f", "14")`, `config("index.residency", "disk")`.
    /// The typed methods below are this with the key filled in, so the last
    /// call for a key wins whichever of them made it.
    ///
    /// A hot key is set before the database opens (`replication.*` and
    /// `vacuum.enabled`, which act on the open database, right after) and
    /// stays changeable with `SET CONFIG`. An unknown key or a value the
    /// key refuses makes [`Builder::start`] fail.
    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.config.push((key.into(), value.into()));
        self
    }

    /// Trades throughput for memory, for a small machine or a process that
    /// shares its memory with an application: four allocator arenas
    /// ([`Builder::malloc_arenas`]), indexes on disk behind a 64 MiB cache
    /// ([`Builder::index_residency`]) and 256 MiB a join
    /// ([`Builder::join_memory_mb`]). A call after it overrides its choice
    /// for that setting.
    pub fn low_memory(self) -> Self {
        self.malloc_arenas(4).index_residency(IndexResidency::Disk).join_memory_mb(256)
    }

    /// Workers one query may use (`--concurrent-threads`, default 16).
    /// Read once.
    pub fn query_threads(self, threads: usize) -> Self {
        self.config("server.concurrent_threads", threads.max(1).to_string())
    }

    /// Row pointers a scan fetches per batch (`--batch-size`, default
    /// 10 000). Read once.
    pub fn batch_size(self, rows: usize) -> Self {
        self.config("server.batch_size", rows.to_string())
    }

    /// Caps glibc's malloc arenas (`--malloc-arenas`; 0, the default, is
    /// glibc's own 8 per core). Fewer arenas keep less freed memory per
    /// thread and cost throughput under concurrency: measured at 16 clients,
    /// 4 arenas cost 20% of point reads and 12% of inserts for 16% less
    /// resident memory, and 1 arena 61% and 53% for 11%.
    ///
    /// It is the whole process's allocator, the application's threads
    /// included, and it binds only arenas not yet made: start the engine
    /// before the application's own thread pools for the full effect. A
    /// limit once set is not lifted. Ignored (with a logged warning) where
    /// the allocator is not glibc's.
    pub fn malloc_arenas(self, arenas: usize) -> Self {
        self.config("server.malloc_arenas", arenas.to_string())
    }

    /// Keeps large buffers out of the kernel's hands between statements
    /// (`--tune-allocator`, default off; glibc only). Worth 1.4x on a single
    /// large scan and nothing on point lookups; costs about 15% when several
    /// clients run large scans at once, and holds more memory.
    pub fn tune_allocator(mut self, on: bool) -> Self {
        self.settings.tune_allocator = on;
        self
    }

    /// Hands freed memory back to the operating system once no statement
    /// has run for a few seconds (default on; glibc only, where a process
    /// otherwise holds its high-water mark until it exits). `false` leaves
    /// the allocator alone, for an application that manages it itself.
    pub fn idle_trim(mut self, on: bool) -> Self {
        self.settings.idle_trim = on;
        self
    }

    /// Where B-tree, hash, path and gin indexes keep their entries
    /// (`--index-residency`, default [`IndexResidency::Memory`], every entry
    /// in RAM). `Hybrid` and `Disk` keep them in sorted files beside the
    /// table, searched through a block cache, with only recent changes in
    /// memory. Applies to tables opened afterwards.
    pub fn index_residency(self, residency: IndexResidency) -> Self {
        self.config("index.residency", residency.name())
    }

    /// The paged indexes' block cache in MiB, shared by every table
    /// (`--index-cache-mb`; 0, the default, is the residency's own: 1024
    /// for hybrid, 64 for disk).
    pub fn index_cache_mb(self, mib: u64) -> Self {
        self.config("index.cache_mb", mib.to_string())
    }

    /// The memory part each paged index holds before writing it out, in MiB
    /// (`--index-delta-mb`; 0, the default, is the residency's own: 256 for
    /// hybrid, 16 for disk).
    pub fn index_delta_mb(self, mib: u64) -> Self {
        self.config("index.delta_mb", mib.to_string())
    }

    /// What an open does with the B-tree, hash and path indexes
    /// (`--index-storage`): rebuild them from rows (the default), or load
    /// the snapshot a clean shutdown wrote. Hot:
    /// `index_snapshot.index_storage`.
    pub fn index_storage(self, storage: IndexStorage) -> Self {
        self.config("index_snapshot.index_storage", storage.name())
    }

    /// The most memory one join may hold for its tuples, in MiB (the
    /// server's `RDB_JOIN_MEMORY_MB`, default 1024). A join that would grow
    /// past it fails with SQLSTATE 53200 instead of taking the process
    /// down. Read once.
    pub fn join_memory_mb(mut self, mib: usize) -> Self {
        self.settings.join_memory_mb = Some(mib);
        self
    }

    /// Serves repeated queries from a remembered position (`--enable-cache`,
    /// default off). Read once.
    pub fn query_cache(self, on: bool) -> Self {
        self.config("cache.enabled", if on { "true" } else { "false" })
    }

    /// Entries the query cache holds when it is on (`--cache-size`, default
    /// 100 000). The first engine in a process to turn the cache on sizes
    /// it.
    pub fn query_cache_entries(self, entries: usize) -> Self {
        self.config("cache.size", entries.to_string())
    }

    /// Bytes the key/value store may hold, as Redis's `maxmemory`
    /// (`--kv-max-memory`; 0, the default, is unbounded).
    pub fn kv_max_memory(mut self, bytes: usize) -> Self {
        self.settings.kv_max_memory = Some(bytes);
        self
    }

    /// What goes when the key/value store is full, as Redis's
    /// `maxmemory-policy` (`--kv-eviction`, default
    /// [`KvEviction::NoEviction`]: writes fail instead).
    pub fn kv_eviction(mut self, policy: KvEviction) -> Self {
        self.settings.kv_eviction = Some(policy);
        self
    }

    /// Saves the key/value store periodically and at a clean shutdown
    /// (`--kv-persist`, default off: it is a cache).
    pub fn kv_persist(mut self, on: bool) -> Self {
        self.settings.kv_persist = Some(on);
        self
    }

    /// Time between two periodic saves under [`Builder::kv_persist`]
    /// (`--kv-save-interval-secs`); at least one second.
    pub fn kv_save_interval(mut self, interval: Duration) -> Self {
        self.settings.kv_save_interval = Some(interval);
        self
    }

    /// Partitions open at once in this process, across tables
    /// (`partition.open_budget`, hot). 0, the default, derives it from the
    /// process's open-file limit: a soft limit of 1024 leaves 16, and
    /// [`Builder::raise_descriptor_limit`] is the other way out.
    pub fn partition_open_budget(self, partitions: u64) -> Self {
        self.config("partition.open_budget", partitions.to_string())
    }

    /// The most partitions a derived open budget allows (`partition.max_open`,
    /// `--partition-max-open`, default 4096). With
    /// [`Builder::raise_descriptor_limit`], the open-file limit is raised for
    /// this many, ten descriptors each. Read once.
    pub fn partition_max_open(self, partitions: usize) -> Self {
        self.config("partition.max_open", partitions.to_string())
    }

    /// Raises this process's soft open-file limit toward its hard limit, as
    /// far as the largest partition budget needs, the way the server binary
    /// does at start (default off: the limit is the application's). Unix
    /// only. Leave it off in a process that still uses `select(2)`, which
    /// cannot watch a descriptor above 1023.
    pub fn raise_descriptor_limit(mut self, raise: bool) -> Self {
        self.settings.raise_descriptor_limit = raise;
        self
    }

    /// Whether a commit waits for its `fdatasync` (`--synchronous-commit`,
    /// default on; hot: `wal.synchronous_commit`). Off, a commit returns
    /// once its records are in the page cache: an operating-system crash or
    /// power loss can then lose the last [`Builder::wal_writer_delay`] of
    /// transactions, whole ones off the end of the log, never part of one.
    /// A crash of the process alone loses nothing.
    pub fn synchronous_commit(self, on: bool) -> Self {
        self.config("wal.synchronous_commit", if on { "on" } else { "off" })
    }

    /// How long an asynchronous commit may go unsynced
    /// (`--wal-writer-delay-ms`, default 200 ms; hot:
    /// `wal.writer_delay_ms`). Unused while commits are synchronous.
    pub fn wal_writer_delay(self, delay: Duration) -> Self {
        self.config("wal.writer_delay_ms", delay.as_millis().to_string())
    }

    /// Log bytes between checkpoints (`--wal-checkpoint-bytes`, default
    /// 64 MiB). Larger means fewer checkpoints and a longer replay after a
    /// crash. Read once.
    pub fn wal_checkpoint_bytes(self, bytes: u64) -> Self {
        self.config("wal.checkpoint_bytes", bytes.to_string())
    }

    /// KiB of zeros the log is extended by ahead of its appends
    /// (`--wal-prealloc-kb`, default 1024; 0 turns it off). It about halves
    /// a commit's sync on ext4 and costs up to this much disk per log. Read
    /// once.
    pub fn wal_prealloc_kb(self, kib: u64) -> Self {
        self.config("wal.prealloc_kb", kib.to_string())
    }

    /// Compacts tables and partitions whose pointer slots are mostly dead
    /// without being asked (`--auto-compact`, default on; hot:
    /// `auto_compact.enabled`, and the other `auto_compact.*` keys through
    /// [`Builder::config`]).
    pub fn auto_compact(self, on: bool) -> Self {
        self.config("auto_compact.enabled", if on { "on" } else { "off" })
    }

    /// Records what each statement cost, for `SHOW QUERIES` and
    /// `SHOW QUERY STATS` (`--query-stats`, default on; hot:
    /// `query_stats.enabled`). Off, a statement skips the record and the one
    /// lock every statement otherwise takes for it.
    pub fn query_stats(self, on: bool) -> Self {
        self.config("query_stats.enabled", if on { "on" } else { "off" })
    }

    /// The instruction set the vector kernels use (`--simd`; default: the
    /// best this CPU supports). A level the CPU cannot run makes
    /// [`Builder::start`] fail rather than fall back.
    pub fn simd(mut self, level: SimdLevel) -> Self {
        self.settings.simd = Some(level);
        self
    }

    /// Drops rows a `WHERE` clause cannot match before fetching them
    /// (`--scan-prefilter`, default on).
    pub fn scan_prefilter(mut self, on: bool) -> Self {
        self.settings.scan_prefilter = Some(on);
        self
    }

    /// Stores a new JSON/JSONB column by taking its documents apart into a
    /// table of their own instead of as bytes (`--json-shadow-tables`,
    /// default off). Only for documents that share a small, stable set of
    /// keys: with keys that are data, such a column loads quadratically.
    /// Columns that already exist keep what they are. Read once.
    pub fn json_shadow_tables(mut self, on: bool) -> Self {
        self.settings.json_shadow_tables = Some(on);
        self
    }

    /// Settles long-finished transaction outcomes so the status log's
    /// memory stops growing with every transaction ever run
    /// (`--settle-status-at-open`, default on; `false` is the kill switch).
    pub fn settle_status(mut self, on: bool) -> Self {
        self.settings.settle_status = Some(on);
        self
    }

    /// Opens the database and starts serving it. Callable from any async
    /// runtime: the engine runs on threads of its own.
    pub async fn start(self) -> io::Result<Embedded> {
        let (ready_rx, embedded) = self.launch()?;
        match ready_rx.await {
            Ok(Ok(ready)) => Ok(embedded.ready(ready)),
            Ok(Err(e)) => Err(embedded.failed(e)),
            Err(_) => Err(embedded.failed(io::Error::other("the engine thread ended before it started"))),
        }
    }

    /// [`Builder::start`] for a caller outside any async runtime. Must not be
    /// called from inside one (it blocks until the engine is up).
    pub fn start_blocking(self) -> io::Result<Embedded> {
        let (ready_rx, embedded) = self.launch()?;
        match ready_rx.blocking_recv() {
            Ok(Ok(ready)) => Ok(embedded.ready(ready)),
            Ok(Err(e)) => Err(embedded.failed(e)),
            Err(_) => Err(embedded.failed(io::Error::other("the engine thread ended before it started"))),
        }
    }

    fn launch(self) -> io::Result<(oneshot::Receiver<io::Result<Ready>>, Embedded)> {
        let missing = rasterizeddb_core::simds::cpu::features().baseline_missing();
        if !missing.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "RasterizedDB needs an x86-64-v3 CPU (AVX2, BMI1/2, FMA, LZCNT, MOVBE); this one lacks: {}",
                    missing.join(", ")
                ),
            ));
        }
        if rasterizeddb_core::TEST_ONLY_FEATURES.first().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "rasterizeddb_core was built with the test-only `{}` feature, which a database must not run with",
                    rasterizeddb_core::TEST_ONLY_FEATURES[0]
                ),
            ));
        }

        let root = match &self.location {
            Location::Memory => None,
            Location::Directory(dir) => {
                std::fs::create_dir_all(dir)?;
                Some(
                    dir.to_str()
                        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "the database path is not UTF-8"))?
                        .to_string(),
                )
            }
        };

        let (auth, login) = match &self.credentials {
            Some((user, password)) => (
                NativeAuthConfig {
                    method: AuthMethod::ScramSha256,
                    bootstrap_user: user.clone(),
                    bootstrap_password: Some(password.clone()),
                },
                Login::password(user.clone(), password.clone()),
            ),
            None => {
                if cfg!(not(unix)) && !self.allow_unauthenticated_loopback {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "off unix the embedded endpoint is a loopback TCP port any local process can reach: \
                         set Builder::password, or Builder::allow_unauthenticated_loopback(true)",
                    ));
                }
                (
                    NativeAuthConfig {
                        method: AuthMethod::Trust,
                        bootstrap_user: DEFAULT_USER.to_string(),
                        bootstrap_password: None,
                    },
                    Login::Trust,
                )
            }
        };

        // After the checks on this Builder's own arguments, so a start they
        // refuse leaves the process's settings alone; before any engine
        // thread exists.
        let deferred = self
            .settings
            .apply(root.as_deref().map(std::path::Path::new))?;

        let private_dir = private_socket_dir()?;
        let listen = NativeListen {
            addr: "127.0.0.1".to_string(),
            port: 0,
            tcp: private_dir.is_none(),
            socket: private_dir.as_ref().map(|dir| {
                let mut socket = rasterizeddb_core::core::database::NativeSocket::new(dir.join("native.sock"));
                socket.mode = 0o600;
                socket
            }),
            request_timeout: self.request_timeout,
            certificate: None,
        };

        let (ready_tx, ready_rx) = oneshot::channel();
        let (stop_tx, stop_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();
        let workers = self.worker_threads;
        let spawned = std::thread::Builder::new()
            .name("rasterizeddb-engine".to_string())
            .stack_size(ENGINE_STACK)
            .spawn(move || {
                let result = engine_thread(root, auth, listen, workers, deferred, ready_tx, stop_rx);
                let _ = done_tx.send(result);
            });
        let thread = match spawned {
            Ok(thread) => thread,
            Err(e) => {
                remove_private_dir(&private_dir);
                return Err(e);
            }
        };
        Ok((
            ready_rx,
            Embedded {
                endpoint: String::new(),
                login,
                catalog: None,
                stop: Some(stop_tx),
                done: Some(done_rx),
                thread: Some(thread),
                private_dir,
            },
        ))
    }
}

struct Ready {
    endpoint: String,
    catalog: rclite::Arc<DatabaseCatalog>,
}

fn engine_thread(
    root: Option<String>,
    auth: NativeAuthConfig,
    listen: NativeListen,
    workers: Option<usize>,
    deferred: Deferred,
    ready_tx: oneshot::Sender<io::Result<Ready>>,
    stop_rx: oneshot::Receiver<()>,
) -> io::Result<()> {
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime
        .thread_name("rasterizeddb-worker")
        .thread_stack_size(ENGINE_STACK)
        .enable_all();
    if let Some(workers) = workers {
        runtime.worker_threads(workers);
    }
    let runtime = match runtime.build() {
        Ok(runtime) => runtime,
        Err(e) => {
            let _ = ready_tx.send(Err(io::Error::new(e.kind(), e.to_string())));
            return Err(e);
        }
    };
    let result = runtime.block_on(async move {
        let started = async {
            use rasterizeddb_core::core::introspect::set_config_at_start;
            // Named by its key: the engine's own message often is not
            // ("`x` is not an integer").
            let refused = |key: &str, value: &str, e: String| {
                io::Error::new(io::ErrorKind::InvalidInput, format!("{key} = {value}: {e}"))
            };
            // The hot keys, where the server's flags leave them: set before
            // a database opens, so the opens see them.
            for (key, value) in &deferred.before_open {
                set_config_at_start(None, key, value).await.map_err(|e| refused(key, value, e))?;
            }
            let catalog = DatabaseCatalog::open(root.as_deref(), DEFAULT_DATABASE_NAME)
                .await
                .map_err(|e| io::Error::other(format!("could not open the database: {e}")))?;
            if !deferred.after_open.is_empty() {
                let database = catalog
                    .get(catalog.default_name())
                    .ok_or_else(|| io::Error::other("the catalogue holds no default database"))?;
                for (key, value) in &deferred.after_open {
                    set_config_at_start(Some(&database), key, value)
                        .await
                        .map_err(|e| refused(key, value, e))?;
                }
            }
            if deferred.idle_trim {
                rasterizeddb_core::idle_trim::spawn();
            }
            let server = Database::bind_native_in(catalog.clone(), auth, listen).await?;
            let endpoint = server
                .socket_endpoint()
                .or_else(|| server.tcp_endpoint())
                .ok_or_else(|| io::Error::other("the engine bound no endpoint"))?;
            io::Result::Ok((catalog, server, endpoint))
        };
        let (catalog, server, endpoint) = match started.await {
            Ok(started) => started,
            Err(e) => {
                let _ = ready_tx.send(Err(io::Error::new(e.kind(), e.to_string())));
                return Err(e);
            }
        };
        let handles = server.shutdown_handles();
        let serving = tokio::spawn(server.run());
        let _ = ready_tx.send(Ok(Ready {
            endpoint,
            catalog: catalog.clone(),
        }));

        // A stop request, or the Embedded gone without one: either way, stop.
        let _ = stop_rx.await;
        for handle in &handles {
            handle.shutdown();
        }
        let served = match serving.await {
            Ok(result) => result,
            Err(e) => Err(io::Error::other(format!("the native listener panicked: {e}"))),
        };
        // The listeners have drained: the clean point the server binary
        // seals and checkpoints at.
        rasterizeddb_core::shutdown::seal_all(&catalog).await;
        rasterizeddb_core::shutdown::finalize();
        served
    });
    // Connection tasks may still be parked; bounded so a client that never
    // disconnects cannot hold the host's shutdown.
    runtime.shutdown_timeout(Duration::from_secs(5));
    result
}

/// A running embedded database. Stop it with [`Embedded::shutdown`] (or by
/// dropping it).
pub struct Embedded {
    endpoint: String,
    login: Login,
    catalog: Option<rclite::Arc<DatabaseCatalog>>,
    stop: Option<oneshot::Sender<()>>,
    done: Option<oneshot::Receiver<io::Result<()>>>,
    thread: Option<std::thread::JoinHandle<()>>,
    private_dir: Option<PathBuf>,
}

impl Embedded {
    fn ready(mut self, ready: Ready) -> Self {
        self.endpoint = ready.endpoint;
        self.catalog = Some(ready.catalog);
        self
    }

    /// The thread is ending (or never started): reap it and its directory.
    fn failed(mut self, e: io::Error) -> io::Error {
        self.stop.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        remove_private_dir(&self.private_dir.take());
        e
    }

    /// The address a [`Client`] connects to: `unix:/…/native.sock`, or
    /// `127.0.0.1:<port>` off unix.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The [`Options`] [`Embedded::connect`] uses: the client's defaults with
    /// this database's login. Adjust and pass to [`Embedded::connect_with`].
    pub fn options(&self) -> Options {
        Options {
            login: self.login.clone(),
            ..Options::default()
        }
    }

    /// A client for this database, with [`Embedded::options`]. Clients are
    /// cheap to clone and share; one usually serves a whole application.
    pub async fn connect(&self) -> Result<Client, rasterizeddb_client::Error> {
        Client::connect(&self.endpoint, self.options()).await
    }

    /// A client with `options` (connections, timeouts, migrations, …).
    /// Their `login` is used as given.
    pub async fn connect_with(&self, options: Options) -> Result<Client, rasterizeddb_client::Error> {
        Client::connect(&self.endpoint, options).await
    }

    /// The engine's catalogue of databases, for what SQL cannot reach.
    pub fn catalog(&self) -> &rclite::Arc<DatabaseCatalog> {
        self.catalog.as_ref().expect("a started Embedded has its catalogue")
    }

    /// Stops the engine: no new connections, requests in flight finish,
    /// index snapshots are sealed and the write-ahead log is checkpointed.
    /// Returns what the listener ended with.
    pub async fn shutdown(mut self) -> io::Result<()> {
        self.stop.take().map(|stop| stop.send(()));
        let result = match self.done.take() {
            Some(done) => done
                .await
                .unwrap_or_else(|_| Err(io::Error::other("the engine thread panicked"))),
            None => Ok(()),
        };
        // It has sent its result, so it is about to end; joining is brief.
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        remove_private_dir(&self.private_dir.take());
        result
    }

    /// [`Embedded::shutdown`] for a caller outside any async runtime.
    pub fn shutdown_blocking(mut self) -> io::Result<()> {
        self.stop.take().map(|stop| stop.send(()));
        let result = match self.done.take() {
            Some(done) => done
                .blocking_recv()
                .unwrap_or_else(|_| Err(io::Error::other("the engine thread panicked"))),
            None => Ok(()),
        };
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        remove_private_dir(&self.private_dir.take());
        result
    }
}

impl Drop for Embedded {
    fn drop(&mut self) {
        // Dropping the sender is the stop request; the join waits for the
        // seal and checkpoint so the directory is left clean.
        self.stop.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        remove_private_dir(&self.private_dir.take());
    }
}

/// A fresh `0700` directory for the socket, short enough for a socket path
/// (at most 107 bytes). `None` off unix.
#[cfg(unix)]
fn private_socket_dir() -> io::Result<Option<PathBuf>> {
    use std::os::unix::fs::DirBuilderExt;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let base = std::env::temp_dir();
    let base = if base.as_os_str().len() > 80 { PathBuf::from("/tmp") } else { base };
    for _ in 0..16 {
        let dir = base.join(format!(
            "rdb-embedded-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match std::fs::DirBuilder::new().mode(0o700).create(&dir) {
            Ok(()) => return Ok(Some(dir)),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::other("could not create a private directory for the embedded socket"))
}

#[cfg(not(unix))]
fn private_socket_dir() -> io::Result<Option<PathBuf>> {
    let _ = (&AtomicU64::new(0), Ordering::Relaxed);
    Ok(None)
}

fn remove_private_dir(dir: &Option<PathBuf>) {
    if let Some(dir) = dir {
        let _ = std::fs::remove_dir_all(dir);
    }
}
