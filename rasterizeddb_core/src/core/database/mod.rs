pub mod catalog;
#[cfg(test)]
mod native_authz_tests;
#[cfg(test)]
mod rename_tests;
#[cfg(test)]
mod recovery_refusal_tests;
#[cfg(test)]
mod drop_recreate_replay_tests;

#[cfg(test)]
pub(crate) mod test_hooks {
    thread_local! {
        /// Fails the next catalogue tombstone on this thread before it is
        /// written: the first step of a drop failing.
        pub static FAIL_TOMBSTONE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
}

use std::{fmt, io, sync::atomic::AtomicU64};

use cacheguard::CacheGuard;
use rclite::Arc;
use std::sync::Arc as StdArc;

use crate::core::mvcc::TransactionManager;
use crate::core::mvcc::status::StatusLog;

use dashmap::DashMap;
use log::{debug, error, info, warn};
use rastcp::server::{TcpServer, TcpServerBuilder};
use smallvec::{SmallVec, smallvec};

use crate::{
    SERVER_PORT,
    core::{
        db_type::DbType,
        processor::concurrent_processor::ConcurrentProcessor,
        row::{
            row::{ColumnFetchingData, ColumnWritePayload, RowFetch, RowWrite},
            row_pointer::{RowPointer, RowPointerIterator},
            schema::{SchemaCalculator, SchemaField, TableSchema},
            table::Table,
        },
        rql::{executor::execute, lexer_ct::CreateTable, lexer_s1::recognize_query_purpose},
        storage_providers::{backend::DbStorageProvider, traits::StorageIO},
    },
    memory_pool::MEMORY_POOL,
};

/// Where the native listener binds.
///
/// Loopback: the native protocol is what this crate's own client speaks, and
/// exposing it further is a deliberate act that should also come with
/// credentials — see [`Database::start_db_with_auth`].
pub const NATIVE_LISTEN_ADDR: &str = "127.0.0.1";

/// Where the native listener binds.
///
/// Defaults to [`NATIVE_LISTEN_ADDR`]`:`[`SERVER_PORT`]. Both halves are
/// configurable so a second node on one host, or a node other machines must
/// reach, does not need a rebuild — but see
/// [`Database::start_db_with_auth_on`] for the one combination it refuses.
#[derive(Debug, Clone)]
pub struct NativeListen {
    pub addr: String,
    pub port: u16,
}

impl Default for NativeListen {
    fn default() -> Self {
        Self {
            addr: NATIVE_LISTEN_ADDR.to_string(),
            port: SERVER_PORT,
        }
    }
}

impl NativeListen {
    pub fn new(addr: impl Into<String>, port: u16) -> Self {
        Self {
            addr: addr.into(),
            port,
        }
    }

    /// Whether this address is reachable only from the machine itself.
    ///
    /// A hostname cannot be judged without resolving it, and resolving during
    /// validation would make start-up depend on DNS, so anything but a
    /// loopback literal or `localhost` is treated as reachable — the answer
    /// that fails safe.
    pub fn is_loopback(&self) -> bool {
        match self.addr.parse::<std::net::IpAddr>() {
            Ok(ip) => ip.is_loopback(),
            Err(_) => self.addr.eq_ignore_ascii_case("localhost"),
        }
    }
}

impl fmt::Display for NativeListen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.addr, self.port)
    }
}

/// Binds the native listener, retrying a port a predecessor is still draining.
///
/// A predecessor that was killed mid-connection can keep the port busy for a
/// moment while its sockets drain. SO_REUSEADDR covers the usual case, so a
/// bind that still fails after a few retries means someone else holds the port
/// - say so instead of dumping a backtrace on an unwrap.
async fn bind_native_listener(
    listen: &NativeListen,
    authenticator: Option<StdArc<dyn rastcp::session::ServerAuthenticator>>,
) -> TcpServer {
    const BIND_ATTEMPTS: u32 = 5;
    const BIND_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);

    let mut last_error = None;

    for attempt in 1..=BIND_ATTEMPTS {
        let mut builder = TcpServerBuilder::new(&listen.addr, listen.port);
        if let Some(authenticator) = authenticator.clone() {
            builder = builder.authenticator(authenticator);
        }

        match builder.build().await {
            Ok(server) => return server,
            Err(e) => {
                error!(
                    "Could not bind {} (attempt {}/{}): {}",
                    listen, attempt, BIND_ATTEMPTS, e
                );
                last_error = Some(e);

                if attempt < BIND_ATTEMPTS {
                    tokio::time::sleep(BIND_RETRY_DELAY).await;
                }
            }
        }
    }

    panic!(
        "Could not bind {listen} after {BIND_ATTEMPTS} attempts: {error}. \
         Another instance is most likely still running - check with `ss -ltnp | grep {port}`.",
        port = listen.port,
        error = last_error.expect("a failed bind always records an error"),
    );
}

/// The database a client reaches when its connection names none, and the one
/// that lives at the catalogue root rather than in a subdirectory.
///
/// `postgres` rather than something of our own, because it is the name every
/// PostgreSQL client and connection string defaults to.
pub const DEFAULT_DATABASE_NAME: &str = "postgres";

/// Hands out [`Database::id`]. Process-wide, never reused, and never persisted:
/// its only job is to keep the query cache's namespaces apart for the life of
/// the process.
fn next_database_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// The transaction ids a commit turned out to consist of, and whether each
/// one's writes survived.
///
/// A block with savepoints is several ids — writes inside a `SAVEPOINT` carry
/// the sub-transaction's — so this is what has to be published, not the outer
/// id alone. It is deliberately a value produced by
/// [`Database::commit_transaction`] and consumed by
/// [`Database::publish_transaction`]: the set is only reliably answerable at
/// commit, and threading it through makes that impossible to get wrong.
#[derive(Debug, Clone)]
pub struct TransactionFamily(Vec<(crate::core::mvcc::TxId, bool)>);

impl TransactionFamily {
    /// Every id, whether or not its writes survived.
    pub fn ids(&self) -> Vec<crate::core::mvcc::TxId> {
        self.0.iter().map(|(id, _)| *id).collect()
    }
}

/// Every file whose name is derived from the table's, and which therefore has
/// to move when the table does.
///
/// Getting this list short is the failure mode that matters. An early sketch of
/// this feature listed four entries and omitted `.db` — the row data itself —
/// which would have moved the schema and the pointers while orphaning every
/// row, leaving pointers resolving into a file that no longer existed. The list
/// is asserted against the files a real table produces by
/// `the_rename_file_list_matches_what_a_table_creates`, so a sidecar added
/// later fails that test rather than being silently left behind.
///
/// That guard was itself short for a while, and the three entries it missed say
/// what to watch for: several sidecars are opened only when a schema is
/// *loaded*, so a table that has been created and never reopened has fewer
/// files on disk than the same table after a restart. Checking a
/// freshly-created table is checking the smaller set. `CHK`, `GEN` and `PIX`
/// sat outside this list that way — a rename dropped a table's CHECK
/// constraints, its generated columns and its JSON path indexes, and left the
/// files under the old name for the next table to inherit.
const RENAMED_SUFFIXES: &[&str] = &[
    // Row data. First deliberately: it is the one whose absence is silent.
    ".db",
    "_pointers.db",
    "_schema.db",
    // Sidecars, each named from the schema file rather than the table.
    "_schema_FIELDS.db",
    "_schema_FK.db",
    "_schema_DEF.db",
    "_schema_UK.db",
    "_schema_JSON.db",
    "_schema_TYP.db",
    "_schema_CHK.db",
    "_schema_GEN.db",
    "_schema_PIX.db",
    // `CREATE INDEX` declarations. The indexes themselves are rebuilt in
    // memory at open; this is what says which ones to build.
    "_schema_IDX.db",
    // Derived: the filter rebuilds from the pointers and the sequence from
    // `load_id_sequence`, so a failure to copy either is recoverable. They are
    // copied anyway because copying is cheaper than rebuilding.
    "_flt.db",
    "_seq.db",
];

/// Every file a table of `name` may own: what its log records can name.
fn table_file_names(name: &str) -> impl Iterator<Item = String> + '_ {
    RENAMED_SUFFIXES
        .iter()
        .map(move |suffix| format!("{}{}", name, suffix))
}

/// Name of the marker recording a rename that has begun but not finished.
const RENAME_INTENT_FILE: &str = "__rename.intent";

// ---------------------------------------------------------------------------
// Storage placement
// ---------------------------------------------------------------------------
//
// A database may write into more than one directory — see
// [`crate::core::storage`]. Placement is consulted from a dozen points in this
// file (open, create, drop, rename, reload, sync), and none of them should have
// to know whether the build carries the feature. So the pool is a type alias
// that collapses to `()` without it, and every question asked of it is a free
// function with two definitions. The compiler erases the whole thing in a build
// that has the feature off: `()` is zero-sized and each shim is a one-liner.

/// A database's storage pool, or `()` in a build without `storage_pools`.
#[cfg(feature = "storage_pools")]
pub type StoragePool = StdArc<crate::core::storage::StorageManager>;
/// A database's storage pool, or `()` in a build without `storage_pools`.
#[cfg(not(feature = "storage_pools"))]
pub type StoragePool = ();

/// The key/value stores a database's tables may be placed on, or `()` in a
/// build without `kv_storage`.
#[cfg(all(feature = "kv_storage", feature = "storage_pools"))]
pub type KvStores = StdArc<crate::core::storage::kv_registry::KvRegistry>;
/// The key/value stores a database's tables may be placed on, or `()` in a
/// build without `kv_storage`.
///
/// Also `()` without `storage_pools`: the registry lives inside `core::storage`,
/// and without a placement map there is nothing to place on it. The driver
/// itself is unaffected — it is a provider, and needs neither feature.
#[cfg(not(all(feature = "kv_storage", feature = "storage_pools")))]
pub type KvStores = ();

#[cfg(all(feature = "kv_storage", feature = "storage_pools"))]
fn open_kv_stores(location: &str, in_memory: bool) -> KvStores {
    StdArc::new(if in_memory {
        crate::core::storage::kv_registry::KvRegistry::inert()
    } else {
        crate::core::storage::kv_registry::KvRegistry::open(location)
    })
}

#[cfg(not(all(feature = "kv_storage", feature = "storage_pools")))]
fn open_kv_stores(_location: &str, _in_memory: bool) -> KvStores {}

/// The seed provider each non-directory table is opened from, keyed by the
/// backend spec *and* the table name.
///
/// Cached because these backends have identity: an in-memory table's buffers
/// live in the registry its seed created, and a key/value table's length and
/// page cache live in its family. Building a fresh seed on the second call
/// would not reopen the table — it would make a second, empty one.
///
/// The spec is part of the key rather than only the name, because a name can
/// outlive the backend it was cached against: drop a table on `kv:archive`, pin
/// the same name to `memory:`, recreate it, and a name-keyed entry would hand
/// back the old key/value seed. The table would then be built on the store the
/// placement no longer names — silently, and correctly as far as every later
/// read is concerned. Keying on the spec makes that a miss instead of relying on
/// every site that changes a placement to remember to invalidate this.
type SeedCache = DashMap<(String, String), Arc<DbStorageProvider>, ahash::RandomState>;

#[cfg(feature = "storage_pools")]
fn open_pool(location: &str, in_memory: bool) -> Result<StoragePool, String> {
    Ok(StdArc::new(if in_memory {
        crate::core::storage::StorageManager::inert()
    } else {
        crate::core::storage::StorageManager::try_open(location)?
    }))
}

#[cfg(not(feature = "storage_pools"))]
fn open_pool(_location: &str, _in_memory: bool) -> Result<StoragePool, String> {
    Ok(())
}

/// The backup directories every write is mirrored into.
#[cfg(feature = "storage_pools")]
fn pool_mirrors(pool: &StoragePool) -> Vec<String> {
    pool.mirrors()
}

#[cfg(not(feature = "storage_pools"))]
fn pool_mirrors(_pool: &StoragePool) -> Vec<String> {
    Vec::new()
}

/// Opens one of the database's own files, mirrored when the pool says so.
#[cfg(feature = "storage_pools")]
async fn open_root_file(location: &str, backups: &[String], name: &str) -> DbStorageProvider {
    DbStorageProvider::new_mirrored(location, backups, name).await
}

#[cfg(not(feature = "storage_pools"))]
async fn open_root_file(location: &str, _backups: &[String], name: &str) -> DbStorageProvider {
    DbStorageProvider::new_file(location, Some(name)).await
}

/// The provider a table's files are created from.
///
/// The database's own provider when the table has no recorded placement, which
/// is every table in a single-directory database — so this call is free there
/// and the code paths do not fork.
/// Resolves a table onto a backend that is not a directory, if it is on one.
///
/// Separate from the directory path so that a build without `storage_pools` —
/// which has no placement map at all — still compiles, and so the common case
/// stays one map lookup.
async fn seed_on_other_backend(
    _io_pointers: &Arc<DbStorageProvider>,
    _pool: &StoragePool,
    _kv: &KvStores,
    _seeds: &SeedCache,
    _table_name: &str,
) -> Option<Arc<DbStorageProvider>> {
    #[cfg(feature = "storage_pools")]
    {
        use crate::core::storage::TableBackend;
        let backend = _pool.backend_of(_table_name)?;
        if backend.is_file_based() {
            return None;
        }
        let key = (backend.as_spec(), _table_name.to_string());
        if let Some(existing) = _seeds.get(&key) {
            return Some(existing.clone());
        }
        let seed: Arc<DbStorageProvider> = match &backend {
            TableBackend::Memory => Arc::new(DbStorageProvider::new_memory(&format!(
                "{_table_name}_pointers.db"
            ))),
            #[cfg(feature = "kv_storage")]
            TableBackend::KeyValue(store) => match _kv.family(store).await {
                Ok(family) => match DbStorageProvider::new_key_value(
                    family,
                    &format!("{_table_name}_pointers.db"),
                )
                .await
                {
                    Ok(provider) => Arc::new(provider),
                    Err(e) => {
                        error!("table {_table_name}: key/value store '{store}' failed: {e}");
                        return Some(fallback_seed(_table_name));
                    }
                },
                Err(e) => {
                    error!("table {_table_name}: key/value store '{store}' failed: {e}");
                    return Some(fallback_seed(_table_name));
                }
            },
            other => {
                error!(
                    "table {_table_name} is placed on a {} backend this build cannot open",
                    other.kind()
                );
                return Some(fallback_seed(_table_name));
            }
        };
        _seeds.insert(key, seed.clone());
        return Some(seed);
    }
    #[cfg(not(feature = "storage_pools"))]
    None
}

/// Whether `table_name`'s recorded backend is one this server can actually open.
///
/// Reports what is wrong and where, because the operator's fix — restore the
/// store's entry in `db_kv.cfg`, or rebuild this server with the feature — is
/// not guessable from "table does not exist".
fn backend_is_openable(_pool: &StoragePool, _kv: &KvStores, _table_name: &str) -> bool {
    #[cfg(feature = "storage_pools")]
    {
        use crate::core::storage::TableBackend;
        match _pool.backend_of(_table_name) {
            Some(TableBackend::KeyValue(store)) => {
                #[cfg(all(feature = "kv_storage", feature = "storage_pools"))]
                {
                    if _kv.has(&store) {
                        return true;
                    }
                    error!(
                        "table {_table_name} is placed on key/value store '{store}', which \
                         {} does not configure; the table is unavailable until it is restored",
                        crate::core::storage::kv_registry::KV_FILE
                    );
                    return false;
                }
                #[cfg(not(all(feature = "kv_storage", feature = "storage_pools")))]
                {
                    error!(
                        "table {_table_name} is placed on key/value store '{store}', and this \
                         server was built without the kv_storage feature; the table is \
                         unavailable"
                    );
                    return false;
                }
            }
            _ => true,
        }
    }
    #[cfg(not(feature = "storage_pools"))]
    true
}

/// What a table gets when its backend cannot be opened.
///
/// In memory, deliberately. The database refuses to open when a placement names
/// a store that is not configured, so reaching this means the configuration
/// changed under a running server. Falling back to the database *directory*
/// would scatter that table's files where a later, correct run would find them
/// as orphans; this way the damage cannot outlive the process.
fn fallback_seed(table_name: &str) -> Arc<DbStorageProvider> {
    Arc::new(DbStorageProvider::new_memory(&format!(
        "{table_name}_pointers.db"
    )))
}

/// Memory-backed storage for a relation that cannot outlive its statement.
///
/// A CTE and a derived table are both turned into real relations before the
/// scan can read them — see [`crate::core::rql::cte::materialize_query`] — and
/// they are dropped again before the statement returns. Nothing can read one
/// afterwards: the name carries per-execution entropy, so it is not even
/// reproducible by the session that made it, and a crash mid-statement leaves
/// files whose only possible reader has already gone away.
///
/// Given files, each one costs five `create_new` calls (`_pointers`, the rows
/// file, `_schema`, `_flt`, `_seq`) and five deletions, every one of them
/// durable. Measured against the deployment on 192.168.1.7, that is a flat
/// ~130 ms per materialisation, independent of row count, column count and
/// whether the outer query aggregates:
///
/// ```text
/// SELECT COUNT(*) FROM t                              0.08 ms   (server-side)
/// SELECT COUNT(*) FROM (SELECT id FROM t) AS s      126.0  ms
/// WITH s AS (SELECT id FROM t) SELECT COUNT(*) FROM s 129.0  ms
/// ```
///
/// It dominated that server: `SELECT COUNT(*) … FROM (…) AS sub_query` is what
/// sea-orm's `paginate()` emits, so every paginated list view paid it once, and
/// the single most expensive statement on the node — two CTEs feeding an
/// `UPDATE … RETURNING`, scanning four rows per call — spent about half of its
/// 594 ms mean here before doing any work of its own.
///
/// The durability that costs those milliseconds buys nothing, so it is not
/// bought. Everything else about the relation is unchanged: it is a real table
/// with a real schema, created and dropped through the ordinary paths.
fn temp_seed(table_name: &str) -> Option<Arc<DbStorageProvider>> {
    table_name
        .starts_with(crate::core::rql::cte::TEMP_TABLE_PREFIX)
        .then(|| Arc::new(DbStorageProvider::new_memory(table_name)))
}

#[cfg(feature = "storage_pools")]
async fn seed_in(
    io_pointers: &Arc<DbStorageProvider>,
    pool: &StoragePool,
    kv: &KvStores,
    seeds: &SeedCache,
    table_name: &str,
) -> Arc<DbStorageProvider> {
    // A statement-scoped relation never reaches a disk. See `temp_seed`.
    if let Some(seed) = temp_seed(table_name) {
        return seed;
    }

    // Boxed, so opening a key/value store does not enlarge this frame for the
    // tables that will never take that branch. `seed_in` is on `add_table`'s
    // await chain, which already sits close to a test thread's 2 MiB stack, and
    // an inlined future here would be paid for by every table.
    if let Some(seed) = Box::pin(seed_on_other_backend(
        io_pointers,
        pool,
        kv,
        seeds,
        table_name,
    ))
    .await
    {
        return seed;
    }
    let Some(root) = pool.root() else {
        return io_pointers.clone(); // in memory: one place, and it is not a directory
    };

    let at = pool
        .location_of(table_name)
        .unwrap_or_else(|| root.to_string());
    // Read here rather than inherited from `io_pointers`, so a backup location
    // added while the server is running applies to the next table created —
    // the catalogue's own providers were built before it existed and cannot be
    // swapped underneath a live database, but a new table's need not be.
    let backups = pool.mirrors();

    if at == root && backups.is_empty() {
        return io_pointers.clone();
    }

    seed_at(pool, &at, table_name).await
}

/// The provider a table's files are created from, in a named directory.
///
/// `seed_in` resolves the directory from the placement map; this takes it as an
/// argument, which is what a move needs — it has to open the destination
/// *before* the placement says the table is there.
#[cfg(feature = "storage_pools")]
async fn seed_at(pool: &StoragePool, location: &str, table_name: &str) -> Arc<DbStorageProvider> {
    Arc::new(
        DbStorageProvider::new_mirrored(
            location,
            &pool.mirrors(),
            &format!("{}_pointers.db", table_name),
        )
        .await,
    )
}

/// The provider a table's files are created from, on a named *backend*.
///
/// The generalisation of [`seed_at`] that a move between backends needs: it has
/// to open the destination before the placement says the table is there, and
/// the destination may not be a directory at all.
///
/// Deliberately fallible where `seed_at` is not. A directory that cannot be
/// written fails at the first write and the caller finds out; a key/value store
/// that cannot be opened would otherwise fall back to memory — see
/// [`fallback_seed`] — and a move into it would report success having copied
/// the table into a buffer that dies with the process.
#[cfg(feature = "storage_pools")]
async fn seed_for_backend(
    pool: &StoragePool,
    _kv: &KvStores,
    backend: &crate::core::storage::TableBackend,
    table_name: &str,
) -> io::Result<Arc<DbStorageProvider>> {
    use crate::core::storage::TableBackend;

    match backend {
        TableBackend::Directory(path) => Ok(seed_at(pool, path, table_name).await),
        TableBackend::Memory => Ok(Arc::new(DbStorageProvider::new_memory(&format!(
            "{table_name}_pointers.db"
        )))),
        #[cfg(feature = "kv_storage")]
        TableBackend::KeyValue(store) => {
            let family = _kv.family(store).await?;
            let provider =
                DbStorageProvider::new_key_value(family, &format!("{table_name}_pointers.db"))
                    .await?;
            Ok(Arc::new(provider))
        }
        #[cfg(not(feature = "kv_storage"))]
        other => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("this server cannot open a {} backend", other.kind()),
        )),
    }
}

#[cfg(not(feature = "storage_pools"))]
async fn seed_in(
    io_pointers: &Arc<DbStorageProvider>,
    _pool: &StoragePool,
    _kv: &KvStores,
    _seeds: &SeedCache,
    table_name: &str,
) -> Arc<DbStorageProvider> {
    // A statement-scoped relation never reaches a disk. See `temp_seed`.
    if let Some(seed) = temp_seed(table_name) {
        return seed;
    }
    io_pointers.clone()
}

/// The provider a table's files are resolved from, for a test that wants to
/// read one of them directly.
///
/// `seed_in` is private and takes four of a `Database`'s fields; a test that
/// reached for them individually would go stale the moment one moves.
#[cfg(test)]
pub(crate) async fn seed_for_tests(
    database: &Database,
    table_name: &str,
) -> Arc<DbStorageProvider> {
    seed_in(
        &database.db_io_pointers,
        &database.storage,
        &database.kv,
        &database.seeds,
        table_name,
    )
    .await
}

/// Chooses and records where a table about to be created goes.
#[cfg(feature = "storage_pools")]
fn pool_place(pool: &StoragePool, table_name: &str) {
    // A statement-scoped relation is in memory and so is in no directory —
    // recording a placement for it would name one its files are not in. It
    // would also be the expensive half of this: `place` and `forget` each take
    // the pool's write lock and `persist()` the whole placement map, so a
    // materialised CTE rewrote that file twice, and every concurrent statement
    // with a CTE in it queued behind the same lock. See `temp_seed`.
    if temp_seed(table_name).is_some() {
        return;
    }
    pool.place(table_name);
}

#[cfg(not(feature = "storage_pools"))]
fn pool_place(_pool: &StoragePool, _table_name: &str) {}

/// Forgets a dropped table's placement.
#[cfg(feature = "storage_pools")]
fn pool_forget(pool: &StoragePool, table_name: &str) {
    // Nothing to forget: `pool_place` records no placement for one of these.
    if temp_seed(table_name).is_some() {
        return;
    }
    pool.forget(table_name);
}

#[cfg(not(feature = "storage_pools"))]
fn pool_forget(_pool: &StoragePool, _table_name: &str) {}

/// Carries a placement across a rename, which does not move a table.
#[cfg(feature = "storage_pools")]
fn pool_rename(pool: &StoragePool, old_name: &str, new_name: &str) {
    // Recorded before the copy rather than after it, so that a crash midway
    // leaves the new name pointing at the directory its half-written files are
    // actually in. The other order strands them somewhere nothing looks.
    // The spec, not the directory: a table on another backend has no directory,
    // and losing its placement here would have the renamed table resolve to the
    // database directory and open empty.
    if let Some(spec) = pool.spec_of(old_name) {
        let _ = pool.pin_backend(new_name, &crate::core::storage::TableBackend::parse(&spec));
    }
}

#[cfg(not(feature = "storage_pools"))]
fn pool_rename(_pool: &StoragePool, _old_name: &str, _new_name: &str) {}

/// Every directory this database writes into, root first.
#[cfg(feature = "storage_pools")]
fn pool_paths(pool: &StoragePool) -> Vec<String> {
    pool.all_paths()
}

#[cfg(not(feature = "storage_pools"))]
fn pool_paths(_pool: &StoragePool) -> Vec<String> {
    Vec::new()
}

pub struct Database {
    pub tables: DashMap<String, Arc<Table<DbStorageProvider>>, ahash::RandomState>,
    pub db_io_pointers: Arc<DbStorageProvider>,
    pub db_io_rows: Arc<DbStorageProvider>,
    pub db_io_schema: Arc<DbStorageProvider>,
    /// Assigns transaction ids and records how they ended.
    ///
    /// One per database rather than one per table: a transaction spans tables,
    /// so an id that meant different things in different tables would make a
    /// cross-table snapshot meaningless.
    pub transactions: StdArc<TransactionManager>,
    /// Rows held by `SELECT … FOR UPDATE`, keyed by table and row key.
    ///
    /// Lives beside the transaction manager because a lock's lifetime is a
    /// transaction's: every one it holds is released when it ends, whichever
    /// way it ends.
    pub row_locks: StdArc<crate::core::mvcc::row_locks::RowLocks>,

    /// Bumped whenever a table's shape changes.
    ///
    /// A client that prepares a statement caches the column list it was
    /// described, and nothing on the wire tells it to look again — so an
    /// `ALTER TABLE` between the prepare and the execute leaves it decoding
    /// rows against a shape that no longer exists. PostgreSQL answers that with
    /// "cached plan must not change result type", which every driver handles by
    /// re-preparing. This counter is how the wire layer knows to look.
    pub schema_generation: StdArc<CacheGuard<std::sync::atomic::AtomicU64>>,
    /// Ships committed transactions to the other instances, and applies theirs.
    ///
    /// Always present and inert until configured, so the write path can hook
    /// into it unconditionally and pay one relaxed atomic load when a database
    /// is not replicating. See [`crate::replication`].
    pub replication: StdArc<crate::replication::Replication>,
    /// Distinguishes this database from the others in the process.
    ///
    /// Not the name: a name can be reused after a `DROP DATABASE`, and the
    /// process-wide query cache must not hand a recreated database the entries
    /// its namesake left behind. Handed to every table this database owns, which
    /// is the only thing that reads it.
    pub id: u64,
    /// What clients connect to this database as.
    pub name: String,
    /// Where this database lives, or `None` when it is in memory.
    ///
    /// Replication needs it to place its log beside the data, and it is decided
    /// at construction rather than when replication starts.
    pub location: Option<String>,
    /// The directories this database may write into, and which of them each
    /// table's files are in.
    ///
    /// Always present and inert for a database that has only ever known one
    /// directory, so the placement call sites need no branch — the same shape
    /// `replication` uses. See [`crate::core::storage`].
    pub storage: StoragePool,

    /// The key/value stores this database's tables may be placed on.
    ///
    /// Read from `db_kv.cfg` at open and empty for a database that has none, so
    /// the call sites need no branch. See
    /// [`crate::core::storage::kv_registry`].
    pub kv: KvStores,

    /// The seed provider each table on a non-directory backend was opened from.
    ///
    /// Not a cache in the usual sense — it is identity. Two lookups of the same
    /// in-memory table have to reach the same buffers, and two lookups of the
    /// same key/value table the same family. See [`SeedCache`].
    seeds: SeedCache,

    /// Names whose files were kept because a log's records for them could not
    /// be voided (a rename's old name; see [`forget_logged_files`]), until the
    /// rename intent that also names them is finished at the next open. A
    /// table created under one would open those files as its own, and the
    /// next open would sweep them from under it, so `CREATE TABLE` and
    /// `RENAME … TO` refuse them, and no further rename starts meanwhile (it
    /// would overwrite the intent that finishes the job).
    held_names: parking_lot::Mutex<Vec<String>>,

    /// The Redis-compatible key/value store (`KV …` statements and the RESP
    /// port). Built lazily: a database that never uses it allocates nothing.
    #[cfg(feature = "kv_cache")]
    pub kv_cache: crate::core::kv_cache::KvCacheHandle,
}

impl Database {
    pub async fn new(location: &str) -> Self {
        Self::new_with_backend(location, false).await
    }

    pub async fn new_with_backend(location: &str, in_memory: bool) -> Self {
        Self::new_named(location, in_memory, DEFAULT_DATABASE_NAME).await
    }

    /// Opens a database that clients address by `name`.
    ///
    /// The name is not derived from the location: the default database lives at
    /// the catalogue root, whose directory is named for the whole installation
    /// rather than for that one database.
    ///
    /// Panics where [`Database::try_new_named`] errs: startup and tests have
    /// nobody to hand the refusal to, and that is the fatal-startup convention.
    pub async fn new_named(location: &str, in_memory: bool, name: &str) -> Self {
        Self::try_new_named(location, in_memory, name)
            .await
            .unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`Database::new_named`], refusing with an error rather than a panic.
    ///
    /// For callers that have somewhere to report it: `CREATE DATABASE` adopting
    /// a directory, where a panic would drop the client's connection instead of
    /// answering it, and the catalogue at startup. Every refusal comes before
    /// anything is opened, and a root log installed here is uninstalled again,
    /// so a refused open leaves nothing behind but the log it would not replay.
    pub async fn try_new_named(location: &str, in_memory: bool, name: &str) -> Result<Self, String> {
        let mut installed_root_log = false;
        if !in_memory {
            let dir_exists = tokio::fs::metadata(location).await.is_ok();

            if !dir_exists {
                tokio::fs::create_dir_all(location).await.unwrap();
            }

            // Replay before anything opens a table. A log on disk means writes
            // that may not have reached their data files, and every reader below
            // would otherwise observe the pre-recovery state. This runs whether
            // or not logging is currently enabled, so switching the feature off
            // cannot strand un-checkpointed writes.
            //
            // A log recovery refuses is left on disk, and opening without it is
            // the one thing that must not happen: writes would land on the bytes
            // its records describe, and the replay that runs once the cause is
            // cleared would put the older records back over them, silently. So
            // the open is refused, as the other fatal startup paths are.
            match crate::core::wal::open_and_recover(location) {
                Ok((w, stats)) => {
                    if stats.applied > 0 || stats.rolled_back > 0 {
                        info!(
                            "WAL recovery: applied {}, rolled back {}",
                            stats.applied, stats.rolled_back
                        );
                    }
                    if crate::wal_enabled() {
                        crate::core::wal::install_for(location, w);
                        installed_root_log = true;
                    }
                }
                Err(e) => {
                    return Err(format!(
                        "refusing to open the database at {location}: WAL recovery failed ({e}). \
                         Its log was left as it was; the writes it holds are not on disk until \
                         it replays. Resolve the cause and restart — deleting the log discards \
                         those writes."
                    ));
                }
            }
        }

        // Before any file is opened: the pool decides which directories exist
        // for this database, recovers their logs, and says whether the
        // catalogue itself is mirrored.
        let storage = match open_pool(location, in_memory) {
            Ok(storage) => storage,
            Err(e) => {
                // Nothing will use the root's log, and leaving it registered
                // would make a later open of this directory skip its recovery.
                if installed_root_log {
                    let _ = crate::core::wal::remove_for(location);
                }
                return Err(format!("refusing to open the database at {location}: {e}"));
            }
        };
        let kv_stores = open_kv_stores(location, in_memory);
        let seeds: SeedCache = DashMap::default();
        let backups = pool_mirrors(&storage);

        // The catalogue, the row store and the schema are mirrored along with
        // everything else. A backup directory that held every table but not the
        // list of them would not open as a database, which is the one thing a
        // backup copy is for.
        let open_root = async |name: &str| open_root_file(location, &backups, name).await;

        let io_pointers = if in_memory {
            Arc::new(DbStorageProvider::new_memory("db_data_pointers.db"))
        } else {
            Arc::new(open_root("db_data_pointers.db").await)
        };

        let io_rows = if in_memory {
            Arc::new(DbStorageProvider::new_memory("db_data_rows.db"))
        } else {
            Arc::new(open_root("db_data_rows.db").await)
        };

        let io_schema = if in_memory {
            Arc::new(DbStorageProvider::new_memory("db_data_schema.db"))
        } else {
            Arc::new(open_root("db_data_schema.db").await)
        };

        let io_pointers_clone = io_pointers.clone();
        let io_rows_clone = io_rows.clone();
        let io_schema_clone = io_schema.clone();

        tokio::spawn(async move {
            tokio::join!(
                io_rows_clone.start_service(),
                io_pointers_clone.start_service(),
                io_schema_clone.start_service()
            );
        });

        let schema = load_db_data_schema(io_schema.clone()).await;

        let mut iterator = RowPointerIterator::new(io_pointers.clone()).await.unwrap();

        // Before the table list, because what it resolves is which names that
        // list should contain.
        let held = recover_interrupted_rename(
            &io_pointers,
            &io_rows,
            &io_schema,
            &storage,
            &kv_stores,
            &seeds,
        )
        .await;

        let tables_names: Vec<(String, u64)> =
            load_db_tables(&schema, &mut iterator, io_rows.clone()).await;

        debug!(
            "Loaded {} tables from database: {:?}",
            tables_names.len(),
            tables_names
        );

        let tables = DashMap::with_hasher(ahash::RandomState::new());

        // Outcomes are durable for a file-backed database and need not be for
        // an in-memory one: nothing there outlives the process, so there is
        // nobody left to ask how a transaction ended.
        let status = if in_memory {
            StatusLog::in_memory()
        } else {
            match StatusLog::open(std::path::Path::new(location).join("db_transactions.status")) {
                Ok(log) => log,
                Err(e) => {
                    // Refusing to start would be worse than running unversioned,
                    // but running versioned on a log that cannot be written
                    // would report commits that do not survive a restart.
                    error!(
                        "Could not open the transaction status log: {}. \
                            Transactions will not be durable.",
                        e
                    );
                    StatusLog::in_memory()
                }
            }
        };
        // Through the log, so a commit record survives whatever the data files
        // do. Nothing to attach for an in-memory database.
        if let Some(wal) = crate::core::wal::wal_for(location)
            && let Err(e) = status.attach_wal(wal)
        {
            error!("The transaction status log is not covered by the WAL: {}", e);
        }
        let transactions = StdArc::new(TransactionManager::new(status));

        // On the log writer's round, so a commit that did not wait for its own
        // sync still gets one shortly. Only for a database with files: an
        // in-memory one has no log and nothing a crash could lose.
        if !in_memory {
            crate::core::log_writer::register(location, &transactions);
        }

        // The id has to be settled before any table is built: a table's cache
        // scope is fixed at construction.
        let id = next_database_id();

        for (table_name, _) in tables_names {
            // A statement-scoped relation that survived to an open is garbage,
            // always, and is reaped here rather than reported.
            //
            // `materialize_query` creates one, uses it, and drops it inside a
            // single statement, and its name carries per-execution entropy — so
            // nothing can refer to one after the process that made it is gone,
            // not even the session that made it. Reaching this loop means a
            // crash or a kill landed between the create and the drop.
            //
            // Before this, each such leftover was logged as "its schema could
            // not be read" on every subsequent start, forever: on the
            // 192.168.1.7 node, 13 of them produced 32 error lines at each boot
            // and had done since 2026-09-01. That is noise that trains an
            // operator to skim exactly the part of the log where a real
            // unreadable table would appear.
            //
            // Files first, then the catalogue row, matching `drop_table`: a row
            // tombstoned first would leave the files with nothing naming them.
            // `io_pointers` rather than `seed_in`, deliberately — `temp_seed`
            // now answers with memory-backed storage for these names, which
            // would discard nothing. These leftovers predate that and are real
            // files in the database's own directory.
            if table_name.starts_with(crate::core::rql::cte::TEMP_TABLE_PREFIX) {
                // Kept files keep their row too, so the next start tries again.
                if !discard_files(&io_pointers, &storage, Some(location), &table_name).await {
                    continue;
                }
                pool_forget(&storage, &table_name);
                if let Err(e) =
                    tombstone_catalogue_row(&io_pointers, &io_rows, &io_schema, &table_name).await
                {
                    // Worth a line each: it means the reaper cannot make
                    // progress and the entry will be back at the next start.
                    error!(
                        "could not reap the leftover temporary table '{}': {}",
                        table_name, e
                    );
                }
                continue;
            }

            // A table whose backend is not configured is left out of the live
            // map entirely, so a query for it says the table does not exist.
            // Opening it anyway would answer that query with no rows, which is
            // the same shape as an empty table and reads as data loss.
            if !backend_is_openable(&storage, &kv_stores, &table_name) {
                continue;
            }
            // A table placed on a second disk is opened from a seed rooted
            // there. `seed_in` returns the database's own provider when the
            // table has no placement, so a single-directory database opens
            // exactly as it did before.
            let seed = seed_in(&io_pointers, &storage, &kv_stores, &seeds, &table_name).await;

            let mut table = Table::new(&table_name, seed, vec![]).await;
            // Its schema did not load, so every column is missing. Left out of
            // the live map for the same reason an unopenable backend is: a
            // query then says the table does not exist, where opening it would
            // answer `SELECT *` with rows carrying no fields.
            if table.schema_unreadable {
                error!(
                    "table '{}' is left unopened: its schema could not be read",
                    table_name
                );
                continue;
            }
            table.scope_cache_to_database(id);
            table.enable_versioning(transactions.clone());
            tables.insert(table_name, Arc::new(table));
        }

        Ok(Database {
            id,
            name: name.to_string(),
            tables,
            db_io_pointers: io_pointers,
            db_io_rows: io_rows,
            db_io_schema: io_schema,
            transactions: StdArc::clone(&transactions),
            // Resolves a savepoint's locks to its transaction, which is what
            // releases them.
            row_locks: StdArc::new(crate::core::mvcc::row_locks::RowLocks::for_transactions(
                StdArc::clone(&transactions),
            )),
            schema_generation: StdArc::new(std::sync::atomic::AtomicU64::new(0).into()),
            replication: StdArc::new(crate::replication::Replication::new()),
            location: (!in_memory).then(|| location.to_string()),
            storage,
            kv: kv_stores,
            seeds,
            held_names: parking_lot::Mutex::new(held.into_iter().collect()),
            #[cfg(feature = "kv_cache")]
            kv_cache: {
                let handle = crate::core::kv_cache::KvCacheHandle::new((!in_memory).then_some(location));
                handle.load_snapshot_if_present();
                handle
            },
        })
    }

    /// Releases the cached seed for a table that is gone.
    ///
    /// Correctness does not depend on this — the spec is part of the cache key,
    /// so a stale entry can never be served for a different backend. It is here
    /// so that dropping an in-memory table also drops its buffers, and dropping
    /// a key/value table releases its open file, rather than holding both for
    /// the life of the process.
    fn forget_seed(&self, table_name: &str) {
        self.seeds.retain(|(_, name), _| name != table_name);
    }

    /// Ends `tx` successfully, on this node.
    ///
    /// Deliberately does **not** publish to the cluster. A commit is not final
    /// until its outcome record is durable, and if that flush fails the caller
    /// aborts and reports failure — but a transaction already handed to the
    /// peers cannot be recalled, so they would apply a write this node has
    /// disowned. Publishing is [`Database::publish_transaction`], called once
    /// durability is established.
    /// Makes every write of this database durable.
    ///
    /// Append-only commits defer their `fdatasync` so that one sync can cover
    /// many of them — see [`crate::core::wal::Wal::sync_pending`]. This is where
    /// that debt is paid, and it must be called before anything reports success
    /// to a client: an autocommit statement pays it at the end of the
    /// statement, an explicit block at `COMMIT`.
    ///
    /// A failure here means the writes are *not* durable, so the caller has to
    /// report the statement as failed rather than swallow it.
    pub fn sync_durable(&self) -> std::io::Result<()> {
        let Some(location) = self.location.as_deref() else {
            return Ok(()); // in-memory: nothing to sync
        };

        // Every directory this database writes into, not just its own. A
        // statement that touched a table on a second disk logged that write to
        // *that* directory's log, and the root's `sync_pending` knows nothing
        // about it — so reporting success after syncing only the root would
        // report durability for bytes still in the page cache.
        //
        // The other directories go first and the root goes last, because the
        // root's log is also where the transaction *outcome* record lives. An
        // outcome that reached the disk before the rows it validates would,
        // after a crash in between, name a transaction as committed whose rows
        // are missing — a write reported as successful that silently is not
        // there. Within one log the records are already in that order, and
        // replay stops at the first one it cannot read whole, so a torn tail
        // can only ever lose the outcome and never keep it.
        //
        // The first failure is returned and the rest are still attempted:
        // stopping early would leave logs unflushed on directories that were
        // fine, and the caller's answer is the same either way.
        let mut outcome = Ok(());

        for path in pool_paths(&self.storage) {
            if path == location {
                continue;
            }
            if let Some(wal) = crate::core::wal::wal_for(&path) {
                if let Err(e) = wal.sync_pending() {
                    error!("storage location {} could not be flushed: {}", path, e);
                    if outcome.is_ok() {
                        outcome = Err(e);
                    }
                }
            }
        }

        if let Some(wal) = crate::core::wal::wal_for(location) {
            if let Err(e) = wal.sync_pending() {
                error!("storage location {} could not be flushed: {}", location, e);
                if outcome.is_ok() {
                    outcome = Err(e);
                }
            }
        }

        // Outcome bytes that `flush_deferred` staged may be copied into the
        // status file now, and not before: the log record justifying them is
        // durable as of the sync above. The file is never fsynced here — the
        // log is what makes it recoverable — so a copy taken any earlier could
        // reach the disk on its own and outlive the rows it speaks for.
        if outcome.is_ok() {
            if let Err(e) = self.transactions.apply_staged() {
                error!(
                    "transaction outcomes could not be written to the status file: {}",
                    e
                );
                outcome = Err(e);
            }
        }

        outcome
    }
    pub fn commit_transaction(&self, tx: crate::core::mvcc::TxId) -> TransactionFamily {
        // Read *before* the commit. A rolled-back savepoint already resolves as
        // aborted while its parent runs, so the answer is correct here — and
        // asking afterwards would depend on the manager keeping its parent map
        // past the commit, which it happens to do and is not obliged to.
        let family = self.family_of(tx);
        self.transactions.commit(tx);
        // A lock outlives nothing: whatever this transaction held is another
        // transaction's the moment it ends.
        self.row_locks.release_all(tx);
        family
    }

    /// Hands a committed, durable transaction to the peers.
    ///
    /// Assigning its sequence number happens here rather than at commit, for
    /// the same reason: a number burned on a transaction that then failed would
    /// leave a hole in this node's stream that peers would wait forever to have
    /// filled.
    ///
    /// In the default asynchronous mode this is a channel send per peer, so a
    /// peer that is slow or gone cannot hold a commit up. Under a synchronous
    /// mode it waits for acknowledgements, bounded by `ack_timeout`.
    pub async fn publish_transaction(&self, family: TransactionFamily) {
        if let Some(record) = self.replication.commit_record_family(&family.0) {
            self.replication.publish(record).await;
        }
    }

    /// Every id a transaction turned out to consist of, paired with whether its
    /// writes survived.
    ///
    /// A block containing savepoints is not one transaction id: writes made
    /// inside a `SAVEPOINT` carry the sub-transaction's id, and only ids in the
    /// family are ever asked about. Publishing on the outer id alone would ship
    /// the statements written outside the savepoints and silently drop the ones
    /// written inside — the replica would end up with a subset of a transaction
    /// that committed whole.
    ///
    /// `false` marks a sub-transaction that was rolled back to; its writes are
    /// invisible here and must not travel. `TransactionManager::outcome`
    /// resolves that through the parent chain, so this does not need to know how
    /// savepoints nest.
    fn family_of(&self, tx: crate::core::mvcc::TxId) -> TransactionFamily {
        use crate::core::mvcc::TxOutcome;

        // Only when replication is on: `family_of` walks the manager's parent
        // map, and a database that is not replicating should not pay for a
        // question whose answer it will throw away.
        if !self.replication.is_active() {
            return TransactionFamily(vec![(tx, true)]);
        }

        TransactionFamily(
            self.transactions
                .family_of(tx)
                .into_iter()
                .map(|id| (id, self.transactions.outcome(id) != TxOutcome::Aborted))
                .collect(),
        )
    }

    /// Ends `tx` without effect and discards what replication buffered for it.
    pub fn abort_transaction(&self, tx: crate::core::mvcc::TxId) {
        // Read before the abort: nothing prunes the parent map today, but
        // relying on that would make this quietly wrong the day something does.
        let family = self.family_of(tx);
        self.transactions.abort(tx);
        self.row_locks.release_all(tx);
        // Every id, including savepoints: their buffers would otherwise sit in
        // the capture map for the life of the process.
        self.replication.on_abort_family(&family.ids());
    }

    /// Brings replication up: opens its log, starts each peer, and starts the
    /// listener.
    ///
    /// Idempotent in the part that matters — the listener binds once per
    /// process, so calling this again after a `REPLICATION DISABLE` restarts
    /// the peers without trying to bind a port that is already held.
    ///
    /// Deliberately not `async`: everything it does is synchronous — opening
    /// files, spawning tasks — and a `REPLICATION ENABLE` statement reaches it
    /// through the executor, which would make the two futures mutually
    /// recursive if this one were awaited.
    pub fn start_replication(database: Arc<Database>) -> io::Result<()> {
        let replication = database.replication.clone();
        let config = replication.config();

        config
            .validate()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        replication.open_storage(database.location.as_deref().map(std::path::Path::new))?;
        replication.set_active(true);

        for peer_config in &config.peers {
            if replication.peer(&peer_config.name).is_none() {
                replication.insert_peer(crate::replication::peer::spawn(
                    replication.clone(),
                    peer_config,
                ));
            }
        }

        if replication.claim_listener() {
            // Bound here rather than inside the task, so a port that is already
            // held is reported to whoever asked for the listener instead of
            // being logged into the void behind a success.
            let server = match crate::replication::server::bind(&replication) {
                Ok(server) => server,
                Err(e) => {
                    replication.release_listener();
                    return Err(e);
                }
            };

            let listener_database = database.clone();
            tokio::spawn(async move {
                if let Err(e) = crate::replication::server::serve(listener_database, server).await {
                    error!("Replication listener stopped with an error: {}", e);
                }
            });
        }

        Ok(())
    }

    /// Removes a table and releases its storage.
    ///
    /// The row and pointer files are dropped with the `Table`, so the space is
    /// reclaimed rather than merely being made unreachable.
    ///
    /// The catalogue row `add_table` wrote is tombstoned too. An earlier comment
    /// here claimed that row was "only read to seed the pointer sequence" and
    /// that leaving it made no observable difference. That was wrong:
    /// `load_db_tables` queries the catalogue to *enumerate* tables and
    /// `Database::new` builds one `Table` per row it returns, so a surviving row
    /// resurrects the table on the next open — with no data, because the files
    /// really were released. Schema comes back, rows do not, and nothing
    /// reports an error at any point.
    pub async fn drop_table(&self, table_name: &str) -> io::Result<bool> {
        let _changed = SchemaChanged::unless_internal(&self.schema_generation, table_name);
        self.drop_table_to_depth(table_name, 0).await
    }

    /// `drop_table`, tracking how far into a chain of JSON shadow tables it has
    /// recursed.
    ///
    /// The depth is a backstop, not a mechanism: nesting is already bounded at
    /// creation by `json_map::MAX_NESTING_DEPTH`, so reaching the cap here means
    /// a sidecar names a shadow that names it back. Stopping is right in that
    /// case — leaking a table is recoverable, recursing forever is not.
    async fn drop_table_to_depth(&self, table_name: &str, depth: usize) -> io::Result<bool> {
        // A held rename's intent names this table, and the next open settles
        // the kept files by which of its two names the catalogue still holds.
        // Dropped now, it would hold neither, the open would leave the kept
        // files, and a table created under their name would take their rows.
        if self.tables.contains_key(table_name) && self.held_names.lock().iter().any(|h| h == table_name) {
            return Err(io::Error::other(format!(
                "\"{table_name}\" is part of a rename whose old files could not be released yet; \
                 restart before dropping it"
            )));
        }
        let Some((_, table)) = self.tables.remove(table_name) else {
            return Ok(false);
        };

        // Before the files go: a failure here must leave the table intact rather
        // than half-dropped, and the catalogue is the half that decides whether
        // it exists at all.
        if let Err(e) = self.forget_in_catalogue(table_name).await {
            self.tables.insert(table_name.to_string(), table);
            error!("Could not remove {} from the catalogue: {}", table_name, e);
            return Err(e);
        }

        // Before any file goes, and after the catalogue row: see
        // `forget_logged_files`. Records name files by a hash of the file
        // name, so the next table of this name would otherwise have this one's
        // records replayed into it at the next open, crash or not.
        let mut unconfirmed = None;
        if let Err(failure) = self.forget_dropped_files(&table.io_rows, table_name) {
            if !failure.maybe_logged {
                // No barrier reached any log: the row goes back, so a failed
                // drop leaves the table as it was.
                error!(
                    "Could not void the log's records for {}, so it was not dropped: {}",
                    table_name, failure.error
                );
                if let Err(restore) = self.remember_in_catalogue(table_name).await {
                    error!(
                        "... and could not put its catalogue row back ({}): it is open now \
                         but will not be after a restart, though its files remain",
                        restore
                    );
                }
                self.tables.insert(table_name.to_string(), table);
                return Err(failure.error);
            }
            // A barrier may be durable (the log failed past its append and is
            // poisoned now), so the table cannot come back: a restart would
            // open it with its unflushed records void. It is dropped, as
            // asked, files included. Unlike feat/partitioning, which keeps
            // the files and relies on its leftover-file name guard, this
            // branch has no such guard, and a later table of this name would
            // open kept files as its own. With the files gone, records a lost
            // barrier left live resolve to nothing at the next open (replay
            // resolves ids from the directory listing).
            //
            // And it is reported as the drop it is, unless the database's own
            // log is poisoned: then not even the catalogue tombstone is known
            // to be durable, and nothing after it can commit. An error here
            // otherwise told the `ALTER` rewrites, which drop and then create
            // again, that nothing happened, so they stopped between the two
            // and lost the rows they held in memory, though a backup's log
            // refusing its barrier while the database's own log took it is no
            // reason to stop the create that follows.
            error!(
                "Dropped {} but could not make the void of its log records durable: {}",
                table_name, failure.error
            );
            if self.own_log_poisoned() {
                unconfirmed = Some(failure.error);
            }
        }

        table.io_rows.drop_io();
        table.io_pointers.drop_io();
        table.io_schema.drop_io();
        table.io_filters.drop_io();
        // Including the id sequence: a table recreated under the same name would
        // otherwise inherit the old one's floor and start numbering from it.
        table.io_sequence.drop_io();

        // The five handles above are the five files a `Table` holds open. A
        // table owns ten: the `_schema_*` sidecars are opened lazily, so they
        // were never in a handle to release and survived every drop. Sweeping
        // the full list closes that, and uses the same list the rename moves —
        // one place to update when a sidecar is added, and one guard test
        // covering both directions.
        //
        // Harmless for the files just released: `sweep_files` tolerates a name
        // that is not there, which is also what makes it usable for cleaning up
        // a rename that never finished. The log was told above.
        sweep_files_durably(
            &seed_in(
                &self.db_io_pointers,
                &self.storage,
                &self.kv,
                &self.seeds,
                table_name,
            )
            .await,
            &self.storage,
            self.location.as_deref(),
            table_name,
        )
        .await;

        // After the files, never before: a placement dropped first would send
        // the sweep above to the database directory, where it would find
        // nothing and report a clean drop over a table still on disk.
        pool_forget(&self.storage, table_name);
        self.forget_seed(table_name);

        // A shadow table is storage the engine created to back a JSON column,
        // owned by the parent's column rather than by the user. Nothing will
        // ever drop it by name, so leaving it behind is not a leak that a later
        // `DROP` tidies up — it is permanent, and it stays visible in the
        // catalogue as a table nobody can account for.
        if depth < crate::core::rql::json_map::MAX_NESTING_DEPTH {
            for json in &table.schema.json_columns {
                if json.is_deleted {
                    continue;
                }
                // A shadow can itself hold JSON columns, so this walks the chain.
                if let Err(e) =
                    Box::pin(self.drop_table_to_depth(&json.shadow_table, depth + 1)).await
                {
                    // The parent is already gone; failing here would report a
                    // drop that did happen as one that did not.
                    error!(
                        "Dropped {} but could not drop its JSON shadow table {}: {}",
                        table_name, json.shadow_table, e
                    );
                }
            }
        } else {
            warn!(
                "Stopped dropping JSON shadow tables under {} at depth {}: the sidecars                  describe a cycle",
                table_name, depth
            );
        }

        if let Some(e) = unconfirmed {
            return Err(e);
        }
        info!("Dropped table {}", table_name);
        Ok(true)
    }

    /// Whether the database's own log, which holds the catalogue, is poisoned.
    fn own_log_poisoned(&self) -> bool {
        self.location
            .as_deref()
            .and_then(crate::core::wal::wal_for)
            .is_some_and(|wal| wal.is_poisoned())
    }

    /// [`forget_logged_files`] for a table being dropped, once its catalogue
    /// tombstone is durable: a barrier that outlived the tombstone in a crash
    /// would void the records of a table that still exists.
    ///
    /// The tombstone is an in-place write, whose commit already syncs the
    /// database's own log, so the sync here is normally a no-op; it is asked
    /// for rather than inherited because the barriers depend on it. Skipped
    /// when the first barrier goes to the database's own log, whose commit
    /// sync covers everything before it there anyway. Nothing at all for a
    /// table with no log, which covers every statement-scoped relation: those
    /// are dropped once per CTE, and a sync each is the cost their memory
    /// backing exists to avoid.
    fn forget_dropped_files(
        &self,
        io: &DbStorageProvider,
        table_name: &str,
    ) -> Result<(), ForgetFailure> {
        let logs = logs_holding(io, &self.storage, self.location.as_deref());
        if logs.is_empty() {
            return Ok(());
        }
        if !self.leads_with_own_log(&logs) {
            self.sync_own_log().map_err(|error| ForgetFailure {
                error,
                maybe_logged: false,
            })?;
        }
        forget_logged_files(&logs, table_name)
    }

    /// Whether the first of `logs` is the database's own log, whose barrier
    /// commit then syncs every catalogue change logged before it.
    fn leads_with_own_log(&self, logs: &[StdArc<crate::core::wal::Wal>]) -> bool {
        let own = self.location.as_deref().and_then(crate::core::wal::wal_for);
        matches!((logs.first(), own), (Some(first), Some(own)) if StdArc::ptr_eq(first, &own))
    }

    /// Makes the database's own log durable: the catalogue's changes.
    fn sync_own_log(&self) -> std::io::Result<()> {
        let Some(location) = self.location.as_deref() else {
            return Ok(()); // in-memory: nothing to sync
        };
        if let Some(wal) = crate::core::wal::wal_for(location) {
            if let Err(e) = wal.sync_pending() {
                error!("storage location {} could not be flushed: {}", location, e);
                return Err(e);
            }
        }
        Ok(())
    }

    /// Refuses `name` when a directory of this database already holds a
    /// non-empty file the new table would take as its own.
    ///
    /// A drop's unlinks reach the disk only with a later directory sync (see
    /// [`sweep_files_durably`]), so a power cut just after a drop can bring the
    /// dropped table's files back while its catalogue row and the log's
    /// barrier stay gone. A table created under the name opened them and
    /// showed the dropped rows. The same holds for the files a rename that
    /// could not finish leaves. Empty files are reused: a sweep creates each
    /// missing file before deleting it, and an empty one holds nothing.
    fn refuse_leftover_files(&self, name: &str) -> io::Result<()> {
        // A statement-scoped relation lives in memory, and is created often.
        if name.starts_with(crate::core::rql::cte::TEMP_TABLE_PREFIX) {
            return Ok(());
        }
        let mut directories = pool_paths(&self.storage);
        directories.extend(self.location.clone());
        directories.sort();
        directories.dedup();
        for directory in &directories {
            for file in table_file_names(name) {
                for candidate in [file.clone(), format!("{file}.tmp")] {
                    let path = std::path::Path::new(directory).join(&candidate);
                    match std::fs::symlink_metadata(&path) {
                        Ok(meta) if meta.len() == 0 && meta.is_file() => {}
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                        Ok(_) => {
                            return Err(io::Error::new(
                                io::ErrorKind::AlreadyExists,
                                format!(
                                    "table \"{name}\" would take over {}, which already exists \
                                     and belongs to no table (left by a dropped or renamed one \
                                     after a crash?); move it aside if it is not needed",
                                    path.display()
                                ),
                            ));
                        }
                        Err(e) => {
                            return Err(io::Error::other(format!(
                                "table \"{name}\" cannot be checked for leftover files at {}: {e}",
                                path.display()
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Refuses `name` while files kept under it still have live log records
    /// (see `held_names`).
    fn refuse_held_name(&self, name: &str) -> io::Result<()> {
        if self.held_names.lock().iter().any(|held| held == name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "the files of a table formerly named \"{name}\" are still on disk with log \
                     records that could not be voided; the name is free again after a restart"
                ),
            ));
        }
        Ok(())
    }

    /// Tombstones a table's row in the database catalogue.
    ///
    /// The catalogue is an ordinary rows-and-pointers pair, so removing an entry
    /// is the same tombstone a `DELETE` writes: `RowPointerIterator` skips it on
    /// the next open and the table is not rebuilt. Rewriting the catalogue file
    /// would be the alternative and is not worth it — a tombstone is one record
    /// in place.
    ///
    /// Returns `Ok(false)` when no row named the table, which is not an error:
    /// a table created before the catalogue existed, or one already forgotten,
    /// should still drop cleanly.
    async fn forget_in_catalogue(&self, table_name: &str) -> io::Result<bool> {
        #[cfg(test)]
        if test_hooks::FAIL_TOMBSTONE.with(|f| f.replace(false)) {
            return Err(io::Error::other("injected catalogue tombstone failure"));
        }
        tombstone_catalogue_row(
            &self.db_io_pointers,
            &self.db_io_rows,
            &self.db_io_schema,
            table_name,
        )
        .await
    }

    /// Rebuilds a table's in-memory handle from its persisted schema.
    ///
    /// `Table` owns its `TableSchema` by value, so a schema change is applied
    /// by writing it to storage and then constructing the table afresh — which
    /// is what `Table::new` does when a schema file already exists.
    /// Renames a table, moving its files and re-keying its catalogue row.
    ///
    /// # Why this copies instead of moving
    ///
    /// `StorageIO` has no rename primitive, so the alternative would be adding
    /// one to every provider. Copying turns out to be the better answer anyway:
    /// the original stays intact and readable until the catalogue flips, so
    /// every crash before that point leaves the old table working and the new
    /// files merely orphaned. Orphans are invisible — `load_db_tables`
    /// enumerates from the catalogue, so nothing opens a file the catalogue
    /// does not name — which makes a crash a space leak rather than a
    /// corruption. Resumable per-file moves, by contrast, have a window where
    /// the table is genuinely half-moved and neither name is complete.
    ///
    /// The cost is O(table size) I/O rather than O(1).
    ///
    /// # Why the catalogue row is inserted before the old one is tombstoned
    ///
    /// This ordering is counterintuitive and it is the important part.
    /// Tombstoning first leaves a window in which *neither* row exists: the
    /// table is invisible while all of its data sits on disk, which reads to an
    /// operator as data loss. Inserting first leaves a window in which *both*
    /// exist — a visible duplicate, where both names open and read correctly
    /// because the copy gave them independent file sets.
    ///
    /// A duplicate you can see beats a table you cannot find.
    ///
    /// # JSON columns
    ///
    /// A table with JSON columns keeps its shadow tables under their old names.
    /// That is deliberate and it is correct rather than merely tolerable:
    /// `JsonColumn::shadow_table` is *persisted*, not recomputed from the parent
    /// name, so the sidecar moves with the schema and goes on pointing at
    /// tables that still exist. Renaming the shadow tables would mean rehashing
    /// every name and rewriting every sidecar, recursively, to end up somewhere
    /// no more correct and briefly much less so. The names read as stale; the
    /// data resolves.
    pub async fn rename_table(&self, old_name: &str, new_name: &str) -> io::Result<()> {
        // The table's handle is dropped before the reopened one registers;
        // the database's forgotten horizon must not move in that gap, or the
        // reopened files could carry ids it forgot. See `ForgetRegistry`.
        let _forget_pause = self.transactions.forget_pause();
        let _changed = SchemaChanged::unless_internal(&self.schema_generation, old_name)
            .or_else(|| SchemaChanged::unless_internal(&self.schema_generation, new_name));
        if old_name == new_name {
            return Ok(());
        }
        if new_name.is_empty() || new_name.len() > 128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "table names are limited to 128 bytes, which is what the schema record holds",
            ));
        }
        if crate::core::rql::json_map::is_shadow_table(new_name) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "that prefix is reserved for tables backing JSON columns",
            ));
        }
        if !self.tables.contains_key(old_name) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Table not found"));
        }
        if self.tables.contains_key(new_name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "a table of that name already exists",
            ));
        }
        self.refuse_held_name(new_name)?;
        self.refuse_leftover_files(new_name)?;
        // A rename whose old files are kept still has its intent on disk, and
        // the next open finishes it from there; a new intent would replace it.
        if let Some(held) = self.held_names.lock().first() {
            return Err(io::Error::other(format!(
                "an earlier rename has not finished (the files of \"{held}\" are kept until \
                 the next open discards them); restart before renaming again"
            )));
        }

        // Recorded before anything moves, so a crash anywhere below is
        // recognisable on the next open rather than merely non-destructive.
        self.record_rename_intent(old_name, new_name).await?;

        // The new name inherits the old one's directory. Done here, before the
        // copy, so every path below — the copy's destination, the cleanup of a
        // failed one, and the recovery sweep after a crash — resolves the new
        // name to the directory its files are actually in.
        pool_rename(&self.storage, old_name, new_name);

        // The handle goes first so its maintenance task stops before the files
        // are read: a background flush landing mid-copy would put bytes in the
        // source that the copy has already passed.
        let table = self.tables.remove(old_name).map(|(_, t)| t);

        let copied = self.copy_table_files(old_name, new_name).await;
        if let Err(e) = copied {
            // Nothing has been committed, so the cleanest state is the one we
            // started in. The partial copies are dropped rather than left for
            // recovery: no catalogue row names them.
            self.abandon_copy(old_name, new_name).await;
            if let Some(table) = table {
                self.tables.insert(old_name.to_string(), table);
            }
            error!("Rename of {} to {}: copy failed: {}", old_name, new_name, e);
            return Err(e);
        }

        if let Err(e) = self.rewrite_schema_name(new_name).await {
            self.abandon_copy(old_name, new_name).await;
            if let Some(table) = table {
                self.tables.insert(old_name.to_string(), table);
            }
            error!(
                "Rename of {} to {}: schema rewrite failed: {}",
                old_name, new_name, e
            );
            return Err(e);
        }

        // The commit point. After this the new name is the real one.
        if let Err(e) = self.remember_in_catalogue(new_name).await {
            self.abandon_copy(old_name, new_name).await;
            if let Some(table) = table {
                self.tables.insert(old_name.to_string(), table);
            }
            error!(
                "Rename of {} to {}: catalogue insert failed: {}",
                old_name, new_name, e
            );
            return Err(e);
        }

        // Past the commit point, a failure here is not worth unwinding: both
        // rows exist and both tables read, which is the benign window the
        // ordering was chosen to produce. Recovery finishes it on the next open.
        if let Err(e) = self.forget_in_catalogue(old_name).await {
            error!(
                "Renamed {} to {} but could not tombstone the old catalogue row: {}. \
                 Both names will be visible until the next open completes the rename.",
                old_name, new_name, e
            );
            self.open_renamed_table(new_name).await;
            return Ok(());
        }

        // The barriers below void the old name's records, which only a
        // durable catalogue makes safe: one that outlived the catalogue rows in
        // a crash would leave the old name the table, its unflushed writes
        // void. The old row's tombstone is an in-place write whose commit
        // syncs the database's log, so this normally finds nothing owed; it
        // is asked for rather than inherited because the barriers depend on
        // it. Every other location first, so the catalogue never names a copy
        // whose records are not durable. When the old name's first log is the
        // database's own, the barrier's commit sync covers it all anyway.
        //
        // Without the barriers the old name's records stay live in the log
        // after its files are deleted, and the next open replays them into
        // whatever table takes the name next: `RENAME src TO dst; CREATE
        // TABLE src` came back from a restart with `src` holding the old rows.
        let old_seed = seed_in(
            &self.db_io_pointers,
            &self.storage,
            &self.kv,
            &self.seeds,
            old_name,
        )
        .await;
        let logs = logs_holding(&old_seed, &self.storage, self.location.as_deref());
        if !logs.is_empty() && !self.leads_with_own_log(&logs) {
            if let Err(e) = self.sync_durable() {
                // The old files stay and so does the intent: once the
                // catalogue is durable, the next open finishes the rename.
                error!(
                    "Renamed {} to {} but could not make it durable, so the old files \
                     were kept: {}",
                    old_name, new_name, e
                );
                self.hold_intent(old_name, new_name);
                self.open_renamed_table(new_name).await;
                return Err(e);
            }
        }

        if discard_files(
            &old_seed,
            &self.storage,
            self.location.as_deref(),
            old_name,
        )
        .await
        {
            // Only now: until the old files are gone, the old name still
            // describes something real, and a placement dropped earlier would
            // send the sweep above to the wrong directory.
            pool_forget(&self.storage, old_name);
            self.clear_rename_intent().await;
        } else {
            // The intent stays, and the placement with it, so the next open
            // finds the old files and discards them, barriers first. Until
            // then the old name is refused: a table created under it would
            // open the kept files as its own.
            self.hold_intent(old_name, new_name);
        }
        self.open_renamed_table(new_name).await;

        info!("Renamed table {} to {}", old_name, new_name);
        Ok(())
    }

    /// Drops a rename's copy that never reached the catalogue. If its files
    /// cannot be released the intent stays, so the next open discards them.
    async fn abandon_copy(&self, old_name: &str, new_name: &str) {
        if self.discard_table_files(new_name).await {
            pool_forget(&self.storage, new_name);
            self.clear_rename_intent().await;
        } else {
            self.hold_intent(old_name, new_name);
        }
    }

    /// Holds both names of a rename intent the next open has to finish: the
    /// kept files' name is refused to `CREATE TABLE` and `RENAME … TO`, and
    /// the table under the other is refused to `DROP`, since the open decides
    /// what the kept files are by which of the two the catalogue holds.
    fn hold_intent(&self, old_name: &str, new_name: &str) {
        let mut held = self.held_names.lock();
        for name in [old_name, new_name] {
            if !held.iter().any(|h| h == name) {
                held.push(name.to_string());
            }
        }
    }
    /// Copies every file derived from `old_name` to one derived from `new_name`.
    ///
    /// A file that does not exist copies as a no-op: the sidecars are created
    /// lazily, so a table with no foreign keys has no `_FK.db` and that is not
    /// an error.
    async fn copy_table_files(&self, old_name: &str, new_name: &str) -> io::Result<()> {
        // A rename does not move a table, so both names resolve to the same
        // directory — but they are resolved separately rather than assumed
        // equal, because that assumption is only true while `pool_rename` runs
        // before this does.
        let from = seed_in(
            &self.db_io_pointers,
            &self.storage,
            &self.kv,
            &self.seeds,
            old_name,
        )
        .await;
        let to = seed_in(
            &self.db_io_pointers,
            &self.storage,
            &self.kv,
            &self.seeds,
            new_name,
        )
        .await;

        for suffix in RENAMED_SUFFIXES {
            let source = from.create_new(format!("{}{}", old_name, suffix)).await;
            let len = source.get_len().await;
            if len == 0 {
                continue;
            }

            let destination = to.create_new(format!("{}{}", new_name, suffix)).await;

            // Chunked rather than one allocation: a table is not bounded by
            // memory and a rename of a large one should not be.
            const CHUNK: usize = 1 << 20;
            let mut position = 0u64;
            let mut buffer = vec![0u8; CHUNK.min(len as usize)];
            while position < len {
                let remaining = (len - position) as usize;
                let take = remaining.min(buffer.len());
                let mut cursor = position;
                source
                    .read_data_into_buffer(&mut cursor, &mut buffer[..take])
                    .await?;
                destination.append_data(&buffer[..take], false).await;
                position += take as u64;
            }

            // Durable before the catalogue names it: a file the catalogue
            // points at but the page cache has not written is the one state
            // this whole ordering exists to avoid.
            let token = destination.begin_txn();
            destination
                .commit_durable(token)
                .await
                .map_err(|e| io::Error::other(format!("{e:?}")))?;

            let written = destination.get_len().await;
            if written != len {
                return Err(io::Error::other(format!(
                    "copying {}{} produced {} bytes from {}",
                    new_name, suffix, written, len
                )));
            }
        }
        Ok(())
    }

    /// Total bytes a table occupies, across every file it owns.
    ///
    /// What a rebalance sorts on: moving the largest table off a full disk
    /// frees the most space for the fewest copies.
    #[cfg(feature = "storage_pools")]
    pub async fn table_bytes(&self, table_name: &str) -> u64 {
        let seed = seed_in(
            &self.db_io_pointers,
            &self.storage,
            &self.kv,
            &self.seeds,
            table_name,
        )
        .await;
        let mut total = 0u64;
        for suffix in RENAMED_SUFFIXES {
            let io = seed.create_new(format!("{}{}", table_name, suffix)).await;
            total = total.saturating_add(io.get_len().await);
        }
        total
    }

    /// Moves a table's bytes onto `destination`, which may be a storage
    /// location, another key/value store, or memory.
    ///
    /// Returns the number of bytes copied.
    ///
    /// # Ordering, and what a crash leaves behind
    ///
    /// Copy, then flip the placement, then delete the originals. Every window
    /// between those steps leaves a *leak* rather than a loss:
    ///
    /// * crash after the copy — the placement still names the old backend, the
    ///   table opens from it unchanged, and the new copies are orphans that
    ///   nothing reads. `db_storage.cfg` is the only thing that decides where a
    ///   table is, so bytes it does not name are invisible.
    /// * crash after the flip — the table opens from the new backend and the
    ///   originals are the orphans.
    ///
    /// The placement flip is a rename over `db_storage.cfg`, so there is no
    /// state in which it is half-applied. That is the whole reason the pool
    /// file is rewritten atomically rather than appended to.
    ///
    /// # Why this works for every backend
    ///
    /// The copy is `StorageIO` to `StorageIO`, one named file at a time. It
    /// never reads a directory entry, an object key or a buffer address, so a
    /// directory, an in-memory registry and a key/value family are all just a
    /// provider to it — which is what makes file to memory to key/value to S3
    /// one operation instead of twelve.
    ///
    /// The two things that are *not* backend-agnostic are handled by the
    /// caller: a directory destination has to be a location of this database,
    /// and a store destination has to be configured. Both are checked here
    /// before anything is copied.
    #[cfg(feature = "storage_pools")]
    pub async fn move_table_to(&self, table_name: &str, destination: &str) -> io::Result<u64> {
        let backend = crate::core::storage::TableBackend::parse(destination);
        self.move_table_to_backend(table_name, &backend).await
    }

    /// [`Self::move_table_to`], with the destination already resolved.
    #[cfg(feature = "storage_pools")]
    pub async fn move_table_to_backend(
        &self,
        table_name: &str,
        destination: &crate::core::storage::TableBackend,
    ) -> io::Result<u64> {
        use crate::core::storage::{LocationRole, TableBackend};
        // The table's handle is dropped before the reopened one registers;
        // the database's forgotten horizon must not move in that gap, or the
        // reopened files could carry ids it forgot. See `ForgetRegistry`.
        let _forget_pause = self.transactions.forget_pause();

        let root = self
            .storage
            .root()
            .ok_or_else(|| io::Error::other("this database is in memory"))?
            .to_string();

        // A directory has to be one this database writes into; the other
        // backends have their own answer to "does this exist", and both are
        // asked before a byte is copied.
        match destination {
            TableBackend::Directory(path) => match self.storage.role_of(path) {
                Some(LocationRole::Primary) => {}
                Some(LocationRole::Backup) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "{} is a backup location: its contents are written by mirroring, \
                             and a table moved into it would be overwritten",
                            path
                        ),
                    ));
                }
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("{} is not a storage location of this database", path),
                    ));
                }
            },
            TableBackend::Memory => {}
            #[cfg(feature = "kv_storage")]
            TableBackend::KeyValue(store) => {
                if !self.kv.has(store) {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "no key/value store named '{}' in {}",
                            store,
                            crate::core::storage::kv_registry::KV_FILE
                        ),
                    ));
                }
            }
            #[cfg(not(feature = "kv_storage"))]
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("this server cannot open a {} backend", other.kind()),
                ));
            }
        }

        let current = self
            .storage
            .backend_of(table_name)
            .unwrap_or_else(|| TableBackend::Directory(root.clone()));
        if current == *destination {
            return Ok(0);
        }

        if !self.tables.contains_key(table_name) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Table not found"));
        }

        // Every write this table has made has to be on the source before the
        // source is read. The handle's own maintenance stops when it is removed
        // below, but a write-ahead log record still only in the page cache is
        // not something dropping a handle flushes — and the originals are
        // deleted at the end of this, so anything left there is lost rather
        // than merely late.
        if let Err(e) = self.sync_durable() {
            warn!("could not flush before moving {}: {}", table_name, e);
        }

        // The handle goes first so its maintenance task stops before the bytes
        // are read: a background flush landing mid-copy would put bytes in the
        // source that the copy has already passed. Same reasoning as
        // `rename_table`.
        let table = self.tables.remove(table_name).map(|(_, t)| t);

        let from = seed_in(
            &self.db_io_pointers,
            &self.storage,
            &self.kv,
            &self.seeds,
            table_name,
        )
        .await;
        // Built for the destination directly rather than from this database's
        // own provider: whether a file is mirrored is settled when it is
        // opened, and the catalogue's provider may predate the backup location.
        // Deriving from it would move a mirrored table onto a disk and quietly
        // stop mirroring it.
        //
        // Boxed for the reason `seed_in` boxes its own: this is on a statement's
        // await chain and the key/value arm is much the largest of the three.
        let to = match Box::pin(seed_for_backend(
            &self.storage,
            &self.kv,
            destination,
            table_name,
        ))
        .await
        {
            Ok(to) => to,
            Err(e) => {
                if let Some(table) = table {
                    self.tables.insert(table_name.to_string(), table);
                }
                return Err(e);
            }
        };

        let copied = copy_between(&from, &to, table_name).await;

        let copied = match copied {
            Ok(bytes) => bytes,
            Err(e) => {
                // The destination's primary only. Its backup copy is the same
                // file in the backup directory that the table's surviving
                // copies mirror into, and deleting it would take the mirror of
                // a table that is still there. Whatever partial bytes it holds
                // are repaired at the next open, which rebuilds any backup
                // whose length disagrees with its primary.
                discard_files(
                &Arc::new(to.without_backups()),
                &self.storage,
                self.location.as_deref(),
                table_name,
            )
            .await;
                if let Some(table) = table {
                    self.tables.insert(table_name.to_string(), table);
                }
                error!(
                    "Could not move {} to {}: {}",
                    table_name,
                    destination.as_spec(),
                    e
                );
                return Err(e);
            }
        };

        // The flip. Everything before it is reversible by deleting copies;
        // everything after it reads from the new backend.
        if let Err(e) = self.storage.pin_backend(table_name, destination) {
            discard_files(
                &Arc::new(to.without_backups()),
                &self.storage,
                self.location.as_deref(),
                table_name,
            )
            .await;
            if let Some(table) = table {
                self.tables.insert(table_name.to_string(), table);
            }
            return Err(io::Error::other(e));
        }

        // The seed cache is keyed on `(spec, table)`, so the destination's
        // entry is a different one — and it has to be *written*, not left to be
        // built later. An in-memory backend's identity is the registry its seed
        // created: rebuilding a seed for `memory:` would make a second, empty
        // one and the rows just copied would be unreachable. The source's entry
        // goes for the opposite reason: it holds the bytes this is about to
        // delete, and a later move back to that backend must not find them.
        self.seeds
            .remove(&(current.as_spec(), table_name.to_string()));
        if !destination.is_file_based() {
            self.seeds
                .insert((destination.as_spec(), table_name.to_string()), to.clone());
        }

        // The old handle is dropped rather than reused: it holds file handles
        // and a memory map of the *old* backend, and reading through it after
        // the flip would serve rows from bytes this is about to delete.
        drop(table);

        let mut reopened = Table::new(table_name, to.clone(), Vec::new()).await;
        if reopened.schema_unreadable {
            // Publishing it would be a table with no columns, which is what the
            // guard exists to prevent. But *returning here* used to leave the
            // table in neither place: removed from the live map, and with the
            // placement already flipped, so the running server could not see it
            // at all until a restart. An operator watching a move fail would
            // find the table gone.
            //
            // The source bytes are still there — `discard_files` is below this
            // point — so the whole move can be undone: put the placement back,
            // reopen from the source, and delete the bad copy. That turns a
            // failed move into a no-op, which is what it should always have
            // been.
            error!(
                "table '{}' could not be read at {} after the copy; rolling the move back",
                table_name,
                destination.as_spec()
            );
            let rolled_back = self.storage.pin_backend(table_name, &current).is_ok();
            self.seeds
                .remove(&(destination.as_spec(), table_name.to_string()));
            if !current.is_file_based() {
                self.seeds
                    .insert((current.as_spec(), table_name.to_string()), from.clone());
            }
            discard_files(
                &Arc::new(to.without_backups()),
                &self.storage,
                self.location.as_deref(),
                table_name,
            )
            .await;

            let mut restored = Table::new(table_name, from, Vec::new()).await;
            if rolled_back && !restored.schema_unreadable {
                restored.scope_cache_to_database(self.id);
                restored.enable_versioning(self.transactions.clone());
                self.tables
                    .insert(table_name.to_string(), Arc::new(restored));
            } else {
                // Both copies unreadable, or the placement would not go back.
                // Nothing to publish, and saying so is the only honest answer —
                // but the bytes are still under the placement that is recorded.
                error!(
                    "table '{}' could not be restored to {} either; it is absent from this \
                     server until it is restarted",
                    table_name,
                    current.as_spec()
                );
            }
            return Err(io::Error::other(format!(
                "table '{}' was copied to {} but its schema could not be read there; the \
                 move was rolled back and the table is where it was",
                table_name,
                destination.as_spec()
            )));
        }
        reopened.scope_cache_to_database(self.id);
        reopened.enable_versioning(self.transactions.clone());
        self.tables
            .insert(table_name.to_string(), Arc::new(reopened));

        // The old primary's files, and only those — see `without_backups`.
        discard_files(
                &Arc::new(from.without_backups()),
                &self.storage,
                self.location.as_deref(),
                table_name,
            )
            .await;

        // Make the destination durable before reporting success.
        //
        // `copy_between` commits each file it writes, but reopening the table
        // writes more — a schema header, an id sequence — and those are
        // *non-transactional*, so a key/value backend leaves them to its
        // background service and its flush interval. A server that stopped
        // inside that window came back with the table on the new store and
        // none of its rows: the move had reported success and the data was
        // gone. It showed up as a restart in a test losing 12 rows of 12,
        // intermittently, which is exactly how long that window is.
        //
        // The file backend has no equivalent gap here — its own commit path
        // already ran — so this is asked of the provider rather than branched
        // on the backend, and is a no-op where there is nothing to flush.
        #[cfg(feature = "kv_storage")]
        if let Some(family) = to.kv_family()
            && let Err(e) = family.flush_all().await
        {
            // The bytes are copied and the placement is flipped, so this is a
            // durability warning rather than a failed move: the background
            // service will still write them out. Reported because "moved" must
            // not imply "survives a crash from this instant" when it does not.
            warn!(
                "moved {} to {} but could not flush the store: {}. The move is applied; \
                 its last writes are durable once the store's flush interval elapses",
                table_name,
                destination.as_spec(),
                e
            );
        }

        info!(
            "Moved {} from {} to {} ({} bytes)",
            table_name,
            current.as_spec(),
            destination.as_spec(),
            copied
        );
        Ok(copied)
    }

    /// Releases every file derived from `name`, and says whether they went
    /// (see [`discard_files`]).
    ///
    /// Used both to clean up a failed copy and to drop the original once the
    /// rename has committed, so it must tolerate files that are not there.
    async fn discard_table_files(&self, name: &str) -> bool {
        discard_files(
            &seed_in(
                &self.db_io_pointers,
                &self.storage,
                &self.kv,
                &self.seeds,
                name,
            )
            .await,
            &self.storage,
            self.location.as_deref(),
            name,
        )
        .await
    }

    /// Rewrites the table name embedded in a schema record's header.
    ///
    /// The name occupies the first 128 bytes of the header, which sits at
    /// offset 0, so this is an in-place overwrite of one fixed-size record
    /// rather than a rewrite of the file. Leaving it stale would not break a
    /// lookup — the catalogue is what enumerates tables — but `Table` carries
    /// `schema.name` into its cache scope, so two tables would end up sharing
    /// a cache namespace.
    async fn rewrite_schema_name(&self, new_name: &str) -> io::Result<()> {
        let schema_io = Arc::new(
            seed_in(
                &self.db_io_pointers,
                &self.storage,
                &self.kv,
                &self.seeds,
                new_name,
            )
            .await
            .create_new(format!("{}_schema.db", new_name))
            .await,
        );

        let mut schema = TableSchema::load(schema_io.clone())
            .await
            .map_err(|e| io::Error::other(format!("could not read the schema to rename: {e}")))?;
        schema.name = new_name.to_string();
        let header = schema.into_vec();

        schema_io.write_data(0, &header).await;
        let token = schema_io.begin_txn();
        schema_io
            .commit_durable(token)
            .await
            .map_err(|e| io::Error::other(format!("{e:?}")))
    }

    /// Opens the renamed table and puts it in the live map under its new name.
    async fn open_renamed_table(&self, new_name: &str) {
        let seed = seed_in(
            &self.db_io_pointers,
            &self.storage,
            &self.kv,
            &self.seeds,
            new_name,
        )
        .await;
        let mut table = Table::new(new_name, seed, Vec::new()).await;
        if table.schema_unreadable {
            error!(
                "renamed table '{}' is left unopened: its schema could not be read",
                new_name
            );
            return;
        }
        table.scope_cache_to_database(self.id);
        table.enable_versioning(self.transactions.clone());
        self.tables.insert(new_name.to_string(), Arc::new(table));
    }
    /// Writes the catalogue row that makes a table enumerable on the next open.
    ///
    /// This is `add_table`'s row write, factored out: a rename needs exactly the
    /// same record under a different name, and a second copy of it would be a
    /// second thing to keep in step with the catalogue's schema.
    async fn remember_in_catalogue(&self, table_name: &str) -> io::Result<()> {
        let mut name_mb = MEMORY_POOL.acquire(table_name.len());
        name_mb
            .into_slice_mut()
            .copy_from_slice(table_name.as_bytes());

        let mut clusters_mb = MEMORY_POOL.acquire(8);
        clusters_mb.into_slice_mut().copy_from_slice(&[0u8; 8]);

        let row_write = RowWrite {
            columns_writing_data: smallvec![
                ColumnWritePayload {
                    column_type: DbType::STRING,
                    size: 8 + 4,
                    data: name_mb,
                    write_order: 0
                },
                ColumnWritePayload {
                    column_type: DbType::U64,
                    size: 8,
                    data: clusters_mb,
                    write_order: 1
                }
            ],
        };

        let mut pointer_iterator = RowPointerIterator::new(self.db_io_pointers.clone())
            .await
            .map_err(|e| io::Error::other(format!("{e:?}")))?;

        let (last_id, table_length) = match pointer_iterator.read_last().await {
            // `id` widens to u128 under `enable_long_row` but is allocated from
            // a u64, so it never exceeds one. The cast is a no-op by default and
            // a real narrowing under that feature, which is why it stays.
            #[allow(clippy::unnecessary_cast)]
            Some(pointer) => (pointer.id as u64, pointer.position),
            None => (0, 0),
        };

        RowPointer::write_row(
            self.db_io_pointers.clone(),
            self.db_io_rows.clone(),
            &AtomicU64::new(last_id),
            &AtomicU64::new(table_length),
            #[cfg(feature = "enable_long_row")]
            0,
            &row_write,
        )
        .await
        .map_err(|e| io::Error::other(format!("{e:?}")))?;

        Ok(())
    }

    /// Whether the catalogue still names a table.
    ///
    /// Used by recovery to work out how far a rename got, which is why it reads
    /// the catalogue rather than the live map: the map is rebuilt from the
    /// catalogue, so consulting it would be circular.
    #[cfg(test)]
    async fn catalogue_contains(&self, table_name: &str) -> bool {
        catalogue_has_row(
            &self.db_io_pointers,
            &self.db_io_rows,
            &self.db_io_schema,
            table_name,
        )
        .await
    }

    async fn record_rename_intent(&self, old_name: &str, new_name: &str) -> io::Result<()> {
        let io = self
            .db_io_pointers
            .create_new(RENAME_INTENT_FILE.to_string())
            .await;
        io.truncate(0)
            .await
            .map_err(|e| io::Error::other(format!("{e:?}")))?;
        io.append_data(encode_intent(old_name, new_name).as_bytes(), true)
            .await;
        let token = io.begin_txn();
        io.commit_durable(token)
            .await
            .map_err(|e| io::Error::other(format!("{e:?}")))
    }

    async fn clear_rename_intent(&self) {
        clear_intent(&self.db_io_pointers).await
    }

    pub async fn add_table(&self, create_table: CreateTable) -> io::Result<()> {
        let _changed =
            SchemaChanged::unless_internal(&self.schema_generation, &create_table.table_name);
        let table_name = &create_table.table_name;
        let columns = create_table.columns;
        let unique_columns = create_table.unique_columns;

        if self.tables.contains_key(table_name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "Table already exists",
            ));
        }
        self.refuse_held_name(table_name)?;
        // Before the catalogue row: a refusal after it would leave a row
        // naming a table that was never built.
        self.refuse_leftover_files(table_name)?;

        let mut name_mb = MEMORY_POOL.acquire(table_name.len());
        let name_data = name_mb.into_slice_mut();
        name_data.copy_from_slice(table_name.as_bytes());

        let mut clusters_mb = MEMORY_POOL.acquire(8);
        let clusters_data = clusters_mb.into_slice_mut();
        clusters_data.copy_from_slice(&[0u8; 8]);

        let row_write = RowWrite {
            columns_writing_data: smallvec![
                ColumnWritePayload {
                    column_type: DbType::STRING,
                    size: 8 + 4,
                    data: name_mb,
                    write_order: 0
                },
                ColumnWritePayload {
                    column_type: DbType::U64,
                    size: 8,
                    data: clusters_mb,
                    write_order: 1
                }
            ],
        };

        let mut pointer_iterator = RowPointerIterator::new(self.db_io_pointers.clone())
            .await
            .unwrap();

        if let Some(row_pointer) = pointer_iterator.read_last().await {
            // `id` widens to u128 under `enable_long_row`, but it is always
            // allocated from this same AtomicU64, so it never exceeds u64.
            let atomic_last_id = AtomicU64::new(row_pointer.id as u64);

            let table_length = row_pointer.position;
            let atomic_table_length = AtomicU64::new(table_length);

            #[cfg(feature = "enable_long_row")]
            let cluster = 0;

            let result = RowPointer::write_row(
                self.db_io_pointers.clone(),
                self.db_io_rows.clone(),
                &atomic_last_id,
                &atomic_table_length,
                #[cfg(feature = "enable_long_row")]
                cluster,
                &row_write,
            )
            .await;

            if result.is_err() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Failed to write row pointer.",
                ));
            }
        } else {
            let atomic_last_id = AtomicU64::new(0);
            let atomic_table_length = AtomicU64::new(0);

            #[cfg(feature = "enable_long_row")]
            let cluster = 0;

            let result = RowPointer::write_row(
                self.db_io_pointers.clone(),
                self.db_io_rows.clone(),
                &atomic_last_id,
                &atomic_table_length,
                #[cfg(feature = "enable_long_row")]
                cluster,
                &row_write,
            )
            .await;

            if result.is_err() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Failed to write initial row pointer.",
                ));
            }
        }

        // The directory is decided and recorded before a single file is
        // created there. A crash between the two leaves a placement pointing at
        // a half-written table, which the next `CREATE TABLE` of that name
        // reuses; the other order leaves the files somewhere nothing looks.
        pool_place(&self.storage, table_name);
        let seed = seed_in(
            &self.db_io_pointers,
            &self.storage,
            &self.kv,
            &self.seeds,
            table_name,
        )
        .await;

        let mut table =
            Table::new_with_constraints(&table_name, seed, columns, &unique_columns).await;
        table.scope_cache_to_database(self.id);

        // The declared `PRIMARY KEY`, recorded in the schema header rather than
        // left implicit in the column's `UNIQUE NOT NULL` flags.
        //
        // Enforcement does not need it — a primary key is a unique constraint
        // and is already enforced as one — but every client that addresses a
        // single row does: "the column this table calls its key" and "some
        // column that happens to be unique" are different questions, and only
        // the first has one answer.
        if let Some(primary_key) = &create_table.primary_key {
            table.schema.set_primary_key(primary_key.clone());
            if let Err(e) = table.schema.rewrite_header(table.io_schema.clone()).await {
                error!("Failed to record primary key for {}: {}", table_name, e);
                return Err(e);
            }
        }

        // Written after the table exists so its sidecar sits beside the schema
        // it belongs to, and kept in the loaded schema so the first statement
        // after `CREATE TABLE` is already constrained.
        for fk in &create_table.foreign_keys {
            if let Err(e) = crate::core::rql::foreign_key::append(table.io_schema.clone(), fk).await
            {
                error!("Failed to persist foreign key {}: {}", fk.name, e);
                return Err(e);
            }
            table.schema.foreign_keys.push(fk.clone());
        }

        for entry in &create_table.defaults {
            if let Err(e) =
                crate::core::rql::column_default::append(table.io_schema.clone(), entry).await
            {
                error!("Failed to persist default for {}: {}", entry.column, e);
                return Err(e);
            }
            table.schema.defaults.push(entry.clone());
        }

        // Every column a constraint names has to exist, or the constraint would
        // sit in the sidecar matching nothing and the table would look
        // constrained while allowing duplicates.
        for key in &create_table.unique_keys {
            for column in &key.columns {
                if !table
                    .schema
                    .fields
                    .iter()
                    .any(|f| f.name.eq_ignore_ascii_case(column) && !f.is_deleted)
                {
                    error!(
                        "UNIQUE constraint {} names unknown column {}",
                        key.name, column
                    );
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "UNIQUE constraint {:?} names column {:?}, which the table does not have",
                            key.name, column
                        ),
                    ));
                }
            }
            if let Err(e) = crate::core::rql::unique_key::append(table.io_schema.clone(), key).await
            {
                error!("Failed to persist unique constraint {}: {}", key.name, e);
                return Err(e);
            }
            table.register_unique_key(key.clone());
        }

        for entry in &create_table.type_modifiers {
            if let Err(e) =
                crate::core::rql::type_modifier::append(table.io_schema.clone(), entry).await
            {
                error!(
                    "Failed to persist type modifier for {}: {}",
                    entry.column, e
                );
                return Err(e);
            }
            table.schema.type_modifiers.push(entry.clone());
            // Onto the field as well as into the sidecar. A table created in
            // this process is never re-read from disk, so `TableSchema::load`'s
            // copy of this never runs for it — and a planner that did not know
            // the column's scale compared every literal against it a power of
            // ten away, silently.
            if let Some(field) = table
                .schema
                .fields
                .iter_mut()
                .find(|f| f.name.eq_ignore_ascii_case(&entry.column))
            {
                field.scale = Some(entry.scale as u32);
            }
        }

        for entry in &create_table.generated {
            if let Err(e) =
                crate::core::rql::generated::append(table.io_schema.clone(), entry).await
            {
                error!("Failed to persist generated column {}: {}", entry.column, e);
                return Err(e);
            }
            table.schema.generated.push(entry.clone());
        }

        for entry in &create_table.checks {
            if let Err(e) = crate::core::rql::check::append(table.io_schema.clone(), entry).await {
                error!("Failed to persist check constraint {}: {}", entry.name, e);
                return Err(e);
            }
            table.schema.checks.push(entry.clone());
        }

        for entry in &create_table.json_columns {
            if let Err(e) = crate::core::rql::json_map::append(table.io_schema.clone(), entry).await
            {
                error!("Failed to register JSON column {}: {}", entry.column, e);
                return Err(e);
            }
            table.schema.json_columns.push(entry.clone());
        }

        table.enable_versioning(self.transactions.clone());
        // The table's indexes were built when it opened, from a schema that
        // did not have its composite keys yet: they are registered above. A
        // composite PRIMARY KEY or UNIQUE had no index until the next open, so
        // pg_indexes listed none and its uniqueness checks scanned. Reopened
        // from the sidecars just written, it has them; the table is empty, so
        // the reopen builds nothing.
        let reopen = (!create_table.unique_keys.is_empty()).then(|| create_table.table_name.clone());
        self.tables.insert(create_table.table_name, Arc::new(table));
        if let Some(name) = reopen {
            self.reload_table(&name).await?;
        }

        Ok(())
    }

    /// Reclaims dead space across every open table, now rather than at the
    /// background service's next tick.
    ///
    /// Returns one report per table, named. A table whose report has `deferred`
    /// set had a query older than the scan still running and published nothing;
    /// the next run picks it up.
    pub async fn vacuum_all(&self) -> Vec<(String, crate::core::vacuum::VacuumReport)> {
        let tables: Vec<_> = self
            .tables
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        let mut reports = Vec::with_capacity(tables.len());
        for (name, table) in tables {
            match table.vacuum_now().await {
                Ok(report) => reports.push((name, report)),
                Err(e) => error!("Vacuum failed for {}: {}", name, e),
            }
        }
        reports
    }

    /// Serves the native protocol, with authentication configured by default.
    ///
    /// See [`Database::start_db_with_auth`] for what "by default" resolves to.
    pub async fn start_db(database: Arc<Database>) -> io::Result<()> {
        Self::start_db_with_auth(database, crate::native_auth::NativeAuthConfig::default()).await
    }

    /// Serves the native protocol under an explicit authentication policy.
    ///
    /// # What happens when no password is configured
    ///
    /// Requiring SCRAM with no credential to check against would refuse every
    /// connection, including the first one an operator would use to create an
    /// account — a database nobody can open is not a secure database, it is a
    /// broken one, and the reflex it teaches is `--rdb-auth trust`. So a
    /// listener that was given no bootstrap password, on a database that holds
    /// no account with a password either, serves the loopback address without
    /// authentication and says so loudly at start-up.
    ///
    /// The moment either of those becomes untrue — a password is configured, or
    /// an account with one exists — authentication is required and stays
    /// required. Security ratchets on and never silently off.
    pub async fn start_db_with_auth(
        database: Arc<Database>,
        auth: crate::native_auth::NativeAuthConfig,
    ) -> io::Result<()> {
        Self::start_db_with_auth_on(database, auth, NativeListen::default()).await
    }

    /// As [`Database::start_db_with_auth`], on an explicit address and port.
    ///
    /// # The combination this refuses
    ///
    /// Binding an address other machines can reach while accepting any caller
    /// would put an unauthenticated database on the network, which is the one
    /// outcome no default should be able to produce by accident. That is
    /// refused at start-up rather than warned about, the same way replication
    /// refuses `--replication-auth insecure` off loopback.
    ///
    /// The loopback exemption in [`Database::start_db_with_auth`] therefore
    /// stays exactly that: an exemption for the address only the machine
    /// itself can reach.
    pub async fn start_db_with_auth_on(
        database: Arc<Database>,
        auth: crate::native_auth::NativeAuthConfig,
        listen: NativeListen,
    ) -> io::Result<()> {
        use crate::native_auth::RoleAuthenticator;

        info!("Starting database");

        if let Err(e) = auth.validate() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, e));
        }

        // Whether anyone *could* authenticate, which decides whether demanding
        // it would lock everyone out.
        let credential_exists =
            auth.bootstrap_password.is_some() || crate::auth::has_login_role(&database).await;

        let authenticator = if auth.authenticates() && credential_exists {
            if let Err(e) = crate::native_auth::seed_roles(&database, &auth).await {
                return Err(io::Error::other(format!(
                    "could not prepare the role catalogue: {}",
                    e
                )));
            }
            info!(
                "Native protocol requires {} authentication",
                auth.method.name()
            );
            Some(
                StdArc::new(RoleAuthenticator::new(database.clone(), auth.method))
                    as StdArc<dyn rastcp::session::ServerAuthenticator>,
            )
        } else {
            if auth.authenticates() {
                warn!(
                    "No password is configured and no role has one, so {} accepts any caller. \
                     Pass --rdb-password, or run CREATE USER ... PASSWORD to require one.",
                    listen
                );
            }
            None
        };

        let authentication_required = authenticator.is_some();

        // The listener is about to accept anyone, so it may only do so where
        // only this machine can reach it.
        if !authentication_required && !listen.is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "refusing to serve the native protocol on {} with authentication off: \
                     anyone who can reach that address would have unauthenticated access. \
                     Set --rdb-password (or create a role with one), or bind loopback.",
                    listen
                ),
            ));
        }

        let server = Arc::new(bind_native_listener(&listen, authenticator).await);

        // Ctrl+C and SIGTERM stop the accept loop and let in-flight queries
        // finish, rather than dropping every connection where it stands.
        let shutdown = server.shutdown_handle();
        crate::shutdown::on_termination(move || shutdown.shutdown());

        let context = NativeContext {
            database,
            authentication_required,
        };

        let fut = async move {
            info!("Database started, listening on {}", listen);
            if let Err(e) = server
                .run_with_context_and_principal(context, process_incoming_queries)
                .await
            {
                error!("Server stopped with an error: {}", e);
            }
        };

        if let Err(e) = tokio::spawn(fut).await {
            warn!("Server task did not stop cleanly: {}", e);
        }

        info!("Database stopped");

        Ok(())
    }

    pub async fn reload_table(&self, table_name: &str) -> io::Result<()> {
        // The table's handle is dropped before the reopened one registers;
        // the database's forgotten horizon must not move in that gap, or the
        // reopened files could carry ids it forgot. See `ForgetRegistry`.
        let _forget_pause = self.transactions.forget_pause();
        if !self.tables.contains_key(table_name) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Table not found"));
        }
        let _changed = SchemaChanged(&self.schema_generation);
        // Every schema change comes through here, so this is where a client's
        // cached column list becomes stale. See `schema_generation`.
        // Bumped again once the reload is done: see `SchemaChanged`.
        self.schema_generation
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        // The previous handle is dropped first so its maintenance task stops
        // before a second one starts on the same files.
        self.tables.remove(table_name);
        let seed = seed_in(
            &self.db_io_pointers,
            &self.storage,
            &self.kv,
            &self.seeds,
            table_name,
        )
        .await;
        let mut table = Table::new(table_name, seed, Vec::new()).await;
        if table.schema_unreadable {
            // The old handle has already been removed, so the table is now
            // absent rather than stale. That is the safe end of this: a query
            // says it does not exist, instead of reading it with no columns.
            return Err(io::Error::other(format!(
                "table '{}' could not be reopened: its schema could not be read",
                table_name
            )));
        }
        table.scope_cache_to_database(self.id);
        table.enable_versioning(self.transactions.clone());
        self.tables.insert(table_name.to_string(), Arc::new(table));
        Ok(())
    }
}

async fn load_db_data_schema<S: StorageIO>(schema_io: Arc<S>) -> TableSchema {
    let schema = {
        if let Ok(schema) = TableSchema::load(schema_io.clone()).await {
            info!("Loaded schema: {:?}", schema);
            schema
        } else {
            info!("Schema not found, creating new schema");
            let mut table = TableSchema::new("db_data".to_string(), false);

            table
                .add_field(
                    schema_io.clone(),
                    "table_name".to_string(),
                    DbType::STRING,
                    true,
                )
                .await;
            table
                .add_field(
                    schema_io.clone(),
                    "clusters".to_string(),
                    DbType::U64,
                    false,
                )
                .await;

            table.save(schema_io).await.unwrap();

            info!("Created new schema and saved it");

            table
        }
    };

    schema
}

/// Whether the catalogue still names a table.
///
/// A free function rather than a method because rename recovery runs before the
/// `Database` exists — it decides which names the table list should contain, so
/// it cannot be built from that list.
/// Tombstones a table's row in the database catalogue.
///
/// Free-standing for the same reason as `catalogue_has_row`: rename recovery
/// needs it before there is a `Database` to call it on.
/// Removes the marker recording a rename in progress, voiding its log records
/// first.
///
/// The marker is rewritten at offset 0 by every rename, so its records from
/// one rename, left live, replayed under the next one's at the next open: a
/// longer earlier intent showed through a shorter later one, and recovery
/// acted on a name neither rename had. Callers clear it only once whatever the
/// intent protected is durable, which the barrier's own sync also covers for
/// everything this log holds before it.
async fn clear_intent(io_pointers: &Arc<DbStorageProvider>) {
    if let Some(wal) = io_pointers
        .get_location()
        .and_then(|dir| crate::core::wal::wal_for(&dir))
        && let Err(e) =
            wal.forget_files(&[crate::core::wal::file_id_for_name(RENAME_INTENT_FILE)])
    {
        // Deleted anyway: the next intent is checked whole when it is read
        // (see `parse_intent`), so a stale record under it is refused, not
        // acted on.
        error!("Could not void the log's records for the rename intent: {}", e);
    }
    let io = io_pointers.create_new(RENAME_INTENT_FILE.to_string()).await;
    io.drop_io();
}

/// The rename intent's bytes: both names and a checksum over them, each line
/// terminated, so a record that is not exactly one intent does not parse.
fn encode_intent(old_name: &str, new_name: &str) -> String {
    let body = format!("{old_name}\n{new_name}");
    format!("{body}\n{:016x}\n", crate::core::wal::file_id_for_name(&body))
}

/// The names an intent written by [`encode_intent`] holds, or by an older
/// build (two lines and nothing else), or `None` for anything else: stale log
/// records replayed under it, a torn write.
fn parse_intent(text: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = text.split('\n').collect();
    let (old_name, new_name) = match parts.as_slice() {
        [old_name, new_name, sum, ""] => {
            let body = format!("{old_name}\n{new_name}");
            if *sum != format!("{:016x}", crate::core::wal::file_id_for_name(&body)) {
                return None;
            }
            (*old_name, *new_name)
        }
        [old_name, new_name] => (*old_name, *new_name),
        _ => return None,
    };
    let (old_name, new_name) = (old_name.trim(), new_name.trim());
    if old_name.is_empty() || new_name.is_empty() {
        return None;
    }
    Some((old_name.to_string(), new_name.to_string()))
}

/// Finishes or abandons a rename that a crash interrupted.
///
/// Called before the table list is built, because what it decides is which
/// names that list should contain. The catalogue is the authority: it
/// recorded the commit, so what it holds says which side of the commit the
/// crash fell on.
///
/// Deliberately not an error path — a database whose rename cannot be
/// resolved should still open, with both names visible, rather than refuse
/// to start.
async fn recover_interrupted_rename(
    io_pointers: &Arc<DbStorageProvider>,
    io_rows: &Arc<DbStorageProvider>,
    io_schema: &Arc<DbStorageProvider>,
    pool: &StoragePool,
    kv: &KvStores,
    seeds: &SeedCache,
) -> Vec<String> {
    let io = io_pointers.create_new(RENAME_INTENT_FILE.to_string()).await;
    let len = io.get_len().await;
    if len == 0 {
        return Vec::new();
    }

    let mut buffer = vec![0u8; len as usize];
    let mut position = 0u64;
    if io
        .read_data_into_buffer(&mut position, &mut buffer)
        .await
        .is_err()
    {
        warn!("A rename intent record exists but could not be read; leaving it in place");
        return Vec::new();
    }

    let text = String::from_utf8_lossy(&buffer);
    let Some((old_name, new_name)) = parse_intent(&text) else {
        warn!("Malformed rename intent record {:?}; discarding it", text);
        clear_intent(io_pointers).await;
        return Vec::new();
    };
    let (old_name, new_name) = (old_name.as_str(), new_name.as_str());
    let held = || vec![old_name.to_string(), new_name.to_string()];

    let has_new = catalogue_has_row(io_pointers, io_rows, io_schema, new_name).await;
    let has_old = catalogue_has_row(io_pointers, io_rows, io_schema, old_name).await;
    let root = io_pointers.get_location();

    match (has_old, has_new) {
        // Committed: the new row is there. Finish the tidy-up.
        (true, true) => {
            info!(
                "Completing an interrupted rename of {} to {}: both catalogue rows exist",
                old_name, new_name
            );
            if tombstone_catalogue_row(io_pointers, io_rows, io_schema, old_name)
                .await
                .is_ok()
                // Durable before the old name's records are voided below, on
                // whichever log holds them: see `forget_logged_files`.
                && root
                    .as_deref()
                    .and_then(crate::core::wal::wal_for)
                    .is_none_or(|wal| wal.sync_pending().is_ok())
            {
                // Files kept keep the intent, so the next open tries again.
                if !discard_files(
                    &seed_in(io_pointers, pool, kv, seeds, old_name).await,
                    pool,
                    root.as_deref(),
                    old_name,
                )
                .await
                {
                    return held();
                }
                clear_intent(io_pointers).await;
            }
        }
        (false, true) => {
            // Committed and tombstoned; only the old files remain.
            if !discard_files(
                &seed_in(io_pointers, pool, kv, seeds, old_name).await,
                pool,
                root.as_deref(),
                old_name,
            )
            .await
            {
                return held();
            }
            clear_intent(io_pointers).await;
        }
        // Never committed: the catalogue still names only the original, so
        // the copy is incomplete and belongs to nobody.
        (true, false) => {
            info!(
                "Abandoning an interrupted rename of {} to {}: it never reached the catalogue",
                old_name, new_name
            );
            if !discard_files(
                &seed_in(io_pointers, pool, kv, seeds, new_name).await,
                pool,
                root.as_deref(),
                new_name,
            )
            .await
            {
                return held();
            }
            clear_intent(io_pointers).await;
        }
        // Neither: nothing to reconcile against, so the safest act is none.
        // Leaving both file sets costs space; deleting the wrong one costs
        // the table.
        (false, false) => {
            warn!(
                "A rename of {} to {} was interrupted and the catalogue names neither; \
                 leaving both file sets in place",
                old_name, new_name
            );
            clear_intent(io_pointers).await;
        }
    }
    Vec::new()
}

async fn tombstone_catalogue_row(
    io_pointers: &Arc<DbStorageProvider>,
    io_rows: &Arc<DbStorageProvider>,
    io_schema: &Arc<DbStorageProvider>,
    table_name: &str,
) -> io::Result<bool> {
    let schema = load_db_data_schema(io_schema.clone()).await;
    let fetch = create_db_data_row_fetch(&schema.fields);

    let mut iterator = RowPointerIterator::new(io_pointers.clone())
        .await
        .map_err(|e| io::Error::other(format!("{e:?}")))?;

    while let Ok(Some((record_pos, mut pointer))) = iterator.next_row_pointer_with_offset().await {
        let Ok(row) = pointer.fetch_row(io_rows.clone(), &fetch).await else {
            continue;
        };
        let Some(name_column) = row.columns.iter().find(|c| c.schema_id == 0) else {
            continue;
        };
        if String::from_utf8_lossy(name_column.data.into_slice()) != table_name {
            continue;
        }

        pointer
            .delete(
                io_pointers.clone(),
                record_pos,
                None,
                crate::core::mvcc::transaction::FROZEN_TX,
            )
            .await
            .map_err(|e| io::Error::other(format!("{e:?}")))?;
        return Ok(true);
    }

    Ok(false)
}

async fn catalogue_has_row(
    io_pointers: &Arc<DbStorageProvider>,
    io_rows: &Arc<DbStorageProvider>,
    io_schema: &Arc<DbStorageProvider>,
    table_name: &str,
) -> bool {
    let schema = load_db_data_schema(io_schema.clone()).await;
    let fetch = create_db_data_row_fetch(&schema.fields);

    let Ok(mut iterator) = RowPointerIterator::new(io_pointers.clone()).await else {
        return false;
    };

    while let Ok(Some((_, pointer))) = iterator.next_row_pointer_with_offset().await {
        let Ok(row) = pointer.fetch_row(io_rows.clone(), &fetch).await else {
            continue;
        };
        let Some(name_column) = row.columns.iter().find(|c| c.schema_id == 0) else {
            continue;
        };
        if String::from_utf8_lossy(name_column.data.into_slice()) == table_name {
            return true;
        }
    }
    false
}

/// Copies every file `name` owns from one seed provider to another, under the
/// same name.
///
/// The rename's copy is a sibling of this and differs in one way that matters:
/// it copies *between names* in one directory, this copies *between
/// directories* under one name. Merging them would take four arguments and read
/// worse than two functions do.
#[cfg(feature = "storage_pools")]
async fn copy_between(
    from: &Arc<DbStorageProvider>,
    to: &Arc<DbStorageProvider>,
    name: &str,
) -> io::Result<u64> {
    const CHUNK: usize = 1 << 20;
    let mut total = 0u64;

    for suffix in RENAMED_SUFFIXES {
        let file = format!("{}{}", name, suffix);
        let source = from.create_new(file.clone()).await;
        let len = source.get_len().await;
        if len == 0 {
            // Sidecars are created lazily, so a table with no foreign keys has
            // no `_FK.db`. Not an error, and not worth an empty file.
            continue;
        }

        let destination = to.create_new(file.clone()).await;
        // A destination that already holds bytes is a leftover from a move that
        // did not finish. Appending to it would interleave two copies of the
        // table; emptying it first is the only safe reading.
        let existing = destination.get_len().await;
        if existing > 0 {
            warn!(
                "{} already exists at the destination ({} bytes); replacing it",
                file, existing
            );
            destination
                .truncate(0)
                .await
                .map_err(|e| io::Error::other(format!("could not empty {}: {e:?}", file)))?;
        }

        let mut position = 0u64;
        let mut buffer = vec![0u8; CHUNK.min(len as usize)];
        while position < len {
            let take = ((len - position) as usize).min(buffer.len());
            let mut cursor = position;
            source
                .read_data_into_buffer(&mut cursor, &mut buffer[..take])
                .await?;
            destination.append_data(&buffer[..take], false).await;
            position += take as u64;
        }

        let token = destination.begin_txn();
        destination
            .commit_durable(token)
            .await
            .map_err(|e| io::Error::other(format!("{e:?}")))?;

        // Verified rather than assumed. A short copy that nobody checked is how
        // a table becomes a file full of rows the pointers cannot resolve.
        let written = destination.get_len().await;
        if written != len {
            return Err(io::Error::other(format!(
                "copying {} produced {} bytes from {}",
                file, written, len
            )));
        }
        total = total.saturating_add(len);
    }

    Ok(total)
}

/// Why [`forget_logged_files`] failed, and whether a barrier may be in a log
/// anyway: one of several already written, or this one, if the failure came
/// after its append. Only a caller told `maybe_logged: false` may put the
/// table back, since a durable barrier voids its unflushed records.
struct ForgetFailure {
    error: io::Error,
    maybe_logged: bool,
}

/// The logs that can hold records for the files `io` reaches: its own
/// directory's, and for a mirrored provider each backup directory's, since a
/// backup copy's writes are logged there under the same ids and a sweep
/// through `io` deletes those copies too. The database's own log (`own`, a
/// directory) comes first when it is one of them; see
/// [`Database::forget_dropped_files`]. Empty without a log anywhere: in
/// memory, a statement-scoped relation, another backend.
fn logs_holding(
    io: &DbStorageProvider,
    pool: &StoragePool,
    own: Option<&str>,
) -> Vec<StdArc<crate::core::wal::Wal>> {
    let dirs = dirs_reached(io, pool);
    let mut logs: Vec<StdArc<crate::core::wal::Wal>> = Vec::new();
    for dir in &dirs {
        if let Some(wal) = crate::core::wal::wal_for(dir)
            && !logs.iter().any(|l| StdArc::ptr_eq(l, &wal))
        {
            logs.push(wal);
        }
    }
    if let Some(own) = own.and_then(crate::core::wal::wal_for)
        && let Some(at) = logs.iter().position(|l| StdArc::ptr_eq(l, &own))
    {
        logs.swap(0, at);
    }
    logs
}

/// The directories `io` reaches: its own, and for a mirrored provider each
/// backup directory, whose copies a sweep through `io` deletes too.
fn dirs_reached(io: &DbStorageProvider, pool: &StoragePool) -> Vec<String> {
    let mut dirs: Vec<String> = io.get_location().into_iter().collect();
    #[cfg(feature = "storage_pools")]
    {
        if matches!(io, DbStorageProvider::Mirror(_)) {
            dirs.extend(pool.mirrors());
        }
    }
    #[cfg(not(feature = "storage_pools"))]
    let _ = pool;
    dirs
}

/// Voids the records for every file a table of `name` owns in each of
/// `logs` (see [`logs_holding`]), durably, in order. See
/// [`crate::core::wal::Wal::forget_files`].
///
/// Records name files by a hash of the file name, so the next table of this
/// name, or this one moved or renamed back, has the ids these records carry.
/// Unvoided, the next open (a crash is not needed: every open replays the
/// log) writes them into its files. Call it before the files are deleted and
/// after the catalogue no longer names the table *durably*: voided while the
/// table still exists, a crash would replay it without the writes its log was
/// holding.
fn forget_logged_files(
    logs: &[StdArc<crate::core::wal::Wal>],
    name: &str,
) -> Result<(), ForgetFailure> {
    let ids: Vec<u64> = table_file_names(name)
        .map(|file| crate::core::wal::file_id_for_name(&file))
        .collect();
    let mut logged = false;
    for wal in logs {
        let was_poisoned = wal.is_poisoned();
        if let Err(error) = wal.forget_files(&ids) {
            // Poisoned by this call means it failed past the append, at the
            // marker or the sync, and the barrier may be durable. Either
            // other failure leaves nothing that can commit.
            let maybe_logged = logged || (!was_poisoned && wal.is_poisoned());
            return Err(ForgetFailure {
                error,
                maybe_logged,
            });
        }
        logged = true;
    }
    Ok(())
}

/// Releases every file derived from `name`, voiding the logs' records for them
/// first (see [`forget_logged_files`]). Returns whether the files went.
///
/// A failure to void keeps the files: deleted with their records live, the
/// next open would replay those into a later table of that name. Kept, the
/// caller holds the name (`Database::held_names`) and leaves whatever intent
/// lets the next open try again: a space leak, not a corruption. The caller
/// must already have made durable whatever stops the files being a live
/// table's (see [`Database::rename_table`]).
async fn discard_files(
    io_pointers: &Arc<DbStorageProvider>,
    pool: &StoragePool,
    own: Option<&str>,
    name: &str,
) -> bool {
    if let Err(failure) = forget_logged_files(&logs_holding(io_pointers, pool, own), name) {
        error!(
            "Could not void the log's records for {}'s files, so they were kept: {}",
            name, failure.error
        );
        return false;
    }
    sweep_files_durably(io_pointers, pool, own, name).await;
    true
}

/// [`sweep_files`], then a sync of each directory swept, so the deletions are
/// on disk before the caller goes on.
///
/// An unlink reaches the disk with the directory's next sync, and the barrier
/// and catalogue tombstone before it are already durable. A power cut in
/// between kept the files, and the next table of the name opened them as its
/// own. Only where a log reaches: a directory without one is in memory or
/// another backend, and statement-scoped relations, dropped once per CTE,
/// must not pay a sync each.
async fn sweep_files_durably(
    io_pointers: &Arc<DbStorageProvider>,
    pool: &StoragePool,
    own: Option<&str>,
    name: &str,
) {
    sweep_files(io_pointers, name).await;
    if logs_holding(io_pointers, pool, own).is_empty() {
        return;
    }
    for dir in dirs_reached(io_pointers, pool) {
        // Named by a swept file: `sync_parent_dir` syncs the directory a
        // path is in.
        let swept = std::path::Path::new(&dir).join(format!("{name}{}", RENAMED_SUFFIXES[0]));
        if let Err(e) = crate::core::wal::sync_parent_dir(&swept) {
            error!(
                "Deleted {}'s files in {} but could not sync the directory: {}",
                name, dir, e
            );
        }
    }
}

/// Deletes every file derived from `name`, the log left as it is.
async fn sweep_files(io_pointers: &Arc<DbStorageProvider>, name: &str) {
    for suffix in RENAMED_SUFFIXES {
        let io = io_pointers.create_new(format!("{}{}", name, suffix)).await;
        io.drop_io();
    }
}

async fn load_db_tables<S: StorageIO>(
    schema: &TableSchema,
    row_pointer_iterator: &mut RowPointerIterator<S>,
    io_rows: Arc<S>,
) -> Vec<(String, u64)> {
    info!("Loading database tables");

    let mut tables = Vec::new();

    let schema_fields = &schema.fields;

    let row_fetch = create_db_data_row_fetch(schema_fields);
    let row_fetch2 = create_db_data_row_fetch(schema_fields);

    let concurrent_processor = ConcurrentProcessor::new();

    let all_rows = concurrent_processor
        .process(
            &schema.name,
            &format!(
                r##"
            1 = 1
        "##
            ),
            row_fetch,
            row_fetch2,
            &schema_fields.to_vec(),
            io_rows,
            row_pointer_iterator,
            None,
            None,
            // Catalog enumeration, not a user query: no table handle exists yet
            // to read a generation from, and its clause would otherwise share a
            // cache entry with a real query against the same table.
            None,
            // The catalogue scan, which reads the table list rather than user
            // rows and so is not subject to any statement's snapshot.
            crate::core::mvcc::Visibility::All,
        )
        .await
        .expect("failed to enumerate tables from the database catalog");

    let small_vec_fields = SmallVec::from_vec(schema_fields.to_vec());

    info!("Found {} tables in database", all_rows.len());

    for row in all_rows {
        // Process each row and extract table information
        if let Some(table_name) = row.get_column("table_name", &small_vec_fields) {
            if let Some(cluster_id) = row.get_column("clusters", &small_vec_fields) {
                let table_name = String::from_utf8(table_name.into_slice().to_vec()).unwrap();
                let cluster_id = u64::from_le_bytes(cluster_id.into_slice().try_into().unwrap());
                info!("Found table: {}, cluster: {}", table_name, cluster_id);
                tables.push((table_name.clone(), cluster_id.clone()));
            } else {
                panic!("Cluster ID not found in row: {:?}", row);
            }
        } else {
            panic!("Table name not found in row: {:?}", row);
        }
    }

    tables
}

fn create_db_data_row_fetch(schema_fields: &SmallVec<[SchemaField; 20]>) -> RowFetch {
    let schema_calculator = SchemaCalculator::default();

    RowFetch {
        columns_fetching_data: smallvec::smallvec![
            ColumnFetchingData {
                column_offset: schema_calculator
                    .calculate_schema_offset("table_name", schema_fields)
                    .0,
                column_type: DbType::STRING,
                size: 8 + 4,
                schema_id: 0,
            },
            ColumnFetchingData {
                column_offset: schema_calculator
                    .calculate_schema_offset("clusters", schema_fields)
                    .0,
                column_type: DbType::U64,
                size: 8,
                schema_id: 1,
            },
        ],
    }
}

/// What every request on the native listener is served with.
#[derive(Clone)]
pub(crate) struct NativeContext {
    database: Arc<Database>,
    /// Whether callers had to prove who they are. When they did not, every
    /// principal is anonymous, and gating privileged statements on
    /// `superuser` would refuse the very statement that creates the first
    /// account — so the gate is applied only where it means something.
    authentication_required: bool,
}

/// Encodes an error the way the executor does, so a caller needs no special
/// case for refusals.
fn error_response(message: &str) -> Vec<u8> {
    let mut result = Vec::with_capacity(1 + message.len());
    result.push(3u8);
    result.extend_from_slice(message.as_bytes());
    result
}

pub(crate) async fn process_incoming_queries(
    context: NativeContext,
    principal: StdArc<rastcp::session::Principal>,
    request_vec: Vec<u8>,
) -> Vec<u8> {
    let query = String::from_utf8_lossy(&request_vec);

    // Role management is answered by the auth module rather than the RQL
    // executor: the catalogue is an ordinary table, but a password has to be
    // turned into a verifier instead of being stored as written.
    //
    // Recognised before anything is logged, because the text of one of these
    // contains a password in the clear and the log does not need it.
    if let Some(parsed) = crate::auth::parse_role_statement(&query) {
        return match parsed {
            Err(e) => error_response(&e),
            Ok(statement) => {
                let verb = match statement {
                    crate::auth::RoleStatement::Create { .. } => "CREATE ROLE",
                    crate::auth::RoleStatement::AlterPassword { .. } => "ALTER ROLE",
                    crate::auth::RoleStatement::Drop { .. } => "DROP ROLE",
                };
                info!("Received {} from {}", verb, principal);

                if context.authentication_required && !principal.superuser {
                    return error_response(&format!(
                        "permission denied: {} is not a superuser",
                        principal
                    ));
                }

                let method = crate::auth::AuthMethod::ScramSha256;
                match crate::auth::execute_role_statement(&context.database, statement, method)
                    .await
                {
                    Ok(()) => vec![0u8],
                    Err(e) => error_response(&e),
                }
            }
        };
    }

    // Redacted before it is written: role statements are handled above, before
    // reaching here, but the store grammar carries S3 keys and this line would
    // otherwise put them in the server log in the clear — a file that is
    // rotated, shipped and backed up, where the statement log at least stays in
    // memory and bounded.
    info!(
        "Received query from {}: {}",
        principal,
        crate::core::redact::statement(&query)
    );

    #[cfg(not(feature = "query_stats"))]
    {
        run_native_query(&query, context.database).await
    }

    // Recorded for `SHOW QUERIES` the same way a statement arriving over the
    // wire protocol is. The role statements handled above are not: they return
    // before this, and what they carry is a password.
    #[cfg(feature = "query_stats")]
    {
        let Some(recording) = crate::core::query_stats::begin() else {
            return run_native_query(&query, context.database).await;
        };
        let database_name = context.database.name.clone();
        let response = crate::core::query_stats::observe(recording.probe(), async {
            let response = run_native_query(&query, context.database).await;
            // Counted inside the scope: outside it the statement's counters are
            // no longer the ones this task can reach.
            crate::core::query_stats::note_rows(rows_of(&response));
            response
        })
        .await;
        let failure = match response.first() {
            Some(3) => Some(String::from_utf8_lossy(&response[1..]).to_string()),
            _ => None,
        };
        crate::core::query_stats::finish(
            recording,
            &query,
            &database_name,
            &principal.user,
            failure.as_deref(),
        );
        response
    }
}

/// Runs one recognised statement from the native protocol.
async fn run_native_query(query: &str, database: Arc<Database>) -> Vec<u8> {
    if let Some(query_purpose) = recognize_query_purpose(query) {
        info!("Recognized query purpose: {:?}", query_purpose);
        execute(query_purpose, database).await
    } else {
        error!("Could not recognize query purpose");
        error_response("Unrecognized query")
    }
}

/// Rows a native-protocol response reports: the count a write carries, or the
/// rows a read returned.
///
/// The tags are the executor's: 0 is an acknowledgement that may carry a count,
/// 1 is a count, 2 is a result set, 3 is a failure.
#[cfg(feature = "query_stats")]
fn rows_of(response: &[u8]) -> u64 {
    match response.first() {
        Some(0) | Some(1) if response.len() >= 9 => {
            u64::from_le_bytes(response[1..9].try_into().unwrap_or([0; 8]))
        }
        Some(2) => crate::core::row::row::count_serialized_rows(&response[1..]),
        _ => 0,
    }
}

/// Bumps the schema generation when dropped — that is, once the change it
/// guards has finished, on every path out of it.
///
/// The bump at the *start* of `reload_table` is what tells a prepared
/// statement's next `Execute` to look again. It is not enough for anything that
/// caches a description keyed by the generation (`pgwire`'s describe cache):
/// a describe that reads the generation after that first bump but the table
/// before it is replaced would file the old shape under the new generation.
/// A bump after the change closes that, since nothing computed during the
/// change can match a lookup made after it.
///
/// Creating, dropping and renaming a table bump it too. None of them alters a
/// table's shape, but each changes what a name resolves to — `DROP` then
/// `CREATE` under the same name gives a statement a different column list
/// without any `ALTER`. The engine's own temporary relations (a CTE's
/// `__cte_…` tables, a JSON column's shadow tables) are created and dropped by
/// ordinary statements constantly and are never named by a client, so they do
/// not count.
struct SchemaChanged<'a>(&'a CacheGuard<std::sync::atomic::AtomicU64>);

impl<'a> SchemaChanged<'a> {
    fn unless_internal(
        generation: &'a CacheGuard<std::sync::atomic::AtomicU64>,
        table_name: &str,
    ) -> Option<Self> {
        let internal = table_name.starts_with(crate::core::rql::cte::TEMP_TABLE_PREFIX)
            || crate::core::rql::json_map::is_shadow_table(table_name);
        (!internal).then(|| SchemaChanged(generation))
    }
}

impl Drop for SchemaChanged<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
}
