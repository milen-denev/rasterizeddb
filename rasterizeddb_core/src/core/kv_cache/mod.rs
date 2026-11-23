//! An in-process key/value store that speaks Redis.
//!
//! Two ways in, one store:
//!
//! - **The Redis protocol** on `--kv-port`. RESP2 and RESP3, pipelining,
//!   `MULTI`/`EXEC`/`WATCH`, pub/sub, blocking list pops. Any Redis client
//!   connects unchanged. See [`resp`].
//! - **SQL** on the PostgreSQL port: `KV SET session:42 '{"user":7}' EX 3600`,
//!   `KV GET session:42`, and every other command after a leading `KV`,
//!   answered as rows. See [`sql`].
//!
//! # What it is built on
//!
//! [`store`] is 1024 lock-striped shards of a hashbrown table. Redis runs one
//! thread, so it never pays for a lock, but it also never uses a second core.
//! This store lets every core in at once. A read takes one shard's read lock,
//! and a write takes one shard's write lock. Nothing is shared across shards
//! on the hot path: no global counter, no global clock read per command, no
//! allocation beyond the key and value themselves. Every command is atomic
//! on its key. Commands on several keys (`MSET`, `RENAME`, `SINTERSTORE`, …)
//! and `EXEC` take every shard they touch, in index order, so they are atomic
//! too and cannot deadlock.
//!
//! # Memory
//!
//! Unbounded by default, like Redis. `maxmemory` (`--kv-max-memory`,
//! `CONFIG SET maxmemory`, `SET CONFIG kv.max_memory`) bounds it. Each shard
//! enforces its share, `maxmemory / 1024`, under its own lock, so the bound
//! costs no shared state. `maxmemory-policy` chooses what goes when a shard is
//! full, from Redis's set: `noeviction` (refuse writes with `OOM`),
//! `allkeys-lru`, `volatile-lru`, `allkeys-random`, `volatile-random`,
//! `volatile-ttl`. Eviction samples five keys and takes the best by the
//! policy, as Redis does. One value larger than a shard's whole share is
//! still stored, into an otherwise emptied shard. The split never refuses
//! what the store as a whole has room for.
//!
//! # Lifetimes
//!
//! An expiry is an absolute millisecond timestamp on the key. A read treats an
//! expired key as absent. A write, or the background sweep (ten times a
//! second, only over shards that hold keys with a lifetime), removes it.
//! `TTL` is exact; `DBSIZE` may count keys that have expired and not yet been
//! swept, as Redis's does.
//!
//! # Persistence
//!
//! Off by default: this is a cache. `SAVE`/`BGSAVE` (or `KV SAVE`) writes every
//! database to `db_kv_cache.snap` beside the database's files, and an open that
//! finds the file loads it. With `kv.persist` on, the sweeper also saves every
//! `kv.save_interval_secs` when anything changed, and a clean shutdown saves.
//! That is Redis's RDB without the AOF: a crash loses what changed since the
//! last save.

pub mod blocking;
pub mod commands;
pub mod glob;
pub mod pubsub;
pub mod reply;
pub mod resp;
pub mod server;
pub mod snapshot;
pub mod sql;
pub mod store;
pub mod values;
#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use log::{error, info, warn};
use parking_lot::Mutex;

pub use store::KvCache;

/// Databases a client may `SELECT`, as Redis's default `databases 16`.
pub const DATABASES: usize = 16;

/// Where a database keeps its snapshot, beside its tables.
pub const SNAPSHOT_FILE: &str = "db_kv_cache.snap";

/// The Redis version this server answers as in `INFO` and `HELLO`. Clients
/// gate features on it (Sidekiq refuses anything below 6.2), and every
/// command they may then send is implemented.
pub const REDIS_VERSION: &str = "7.4.0";

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

static MAX_MEMORY: AtomicUsize = AtomicUsize::new(0);
static EVICTION: AtomicU8 = AtomicU8::new(EvictionPolicy::NoEviction as u8);
static PERSIST: AtomicBool = AtomicBool::new(false);
static SAVE_INTERVAL_SECS: AtomicU64 = AtomicU64::new(300);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum EvictionPolicy {
    NoEviction = 0,
    AllKeysLru = 1,
    VolatileLru = 2,
    AllKeysRandom = 3,
    VolatileRandom = 4,
    VolatileTtl = 5,
}

impl EvictionPolicy {
    pub fn name(self) -> &'static str {
        match self {
            EvictionPolicy::NoEviction => "noeviction",
            EvictionPolicy::AllKeysLru => "allkeys-lru",
            EvictionPolicy::VolatileLru => "volatile-lru",
            EvictionPolicy::AllKeysRandom => "allkeys-random",
            EvictionPolicy::VolatileRandom => "volatile-random",
            EvictionPolicy::VolatileTtl => "volatile-ttl",
        }
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        Ok(match text.trim().to_ascii_lowercase().as_str() {
            "noeviction" => EvictionPolicy::NoEviction,
            "allkeys-lru" => EvictionPolicy::AllKeysLru,
            "volatile-lru" => EvictionPolicy::VolatileLru,
            "allkeys-random" => EvictionPolicy::AllKeysRandom,
            "volatile-random" => EvictionPolicy::VolatileRandom,
            "volatile-ttl" => EvictionPolicy::VolatileTtl,
            lfu @ ("allkeys-lfu" | "volatile-lfu") => {
                return Err(format!(
                    "{lfu} is not supported: access frequency is not tracked; use allkeys-lru or volatile-lru"
                ));
            }
            other => {
                return Err(format!(
                    "`{other}` is not an eviction policy; use noeviction, allkeys-lru, volatile-lru, \
                     allkeys-random, volatile-random or volatile-ttl"
                ));
            }
        })
    }

    fn from_u8(v: u8) -> Self {
        match v {
            1 => EvictionPolicy::AllKeysLru,
            2 => EvictionPolicy::VolatileLru,
            3 => EvictionPolicy::AllKeysRandom,
            4 => EvictionPolicy::VolatileRandom,
            5 => EvictionPolicy::VolatileTtl,
            _ => EvictionPolicy::NoEviction,
        }
    }
}

/// Bytes the store may hold; 0 is unbounded.
pub fn max_memory() -> usize {
    MAX_MEMORY.load(Ordering::Relaxed)
}

pub fn set_max_memory(bytes: usize) {
    MAX_MEMORY.store(bytes, Ordering::Relaxed);
}

pub fn eviction_policy() -> EvictionPolicy {
    EvictionPolicy::from_u8(EVICTION.load(Ordering::Relaxed))
}

pub fn set_eviction_policy(policy: EvictionPolicy) {
    EVICTION.store(policy as u8, Ordering::Relaxed);
}

pub fn persist() -> bool {
    PERSIST.load(Ordering::Relaxed)
}

pub fn set_persist(on: bool) {
    PERSIST.store(on, Ordering::Relaxed);
}

pub fn save_interval_secs() -> u64 {
    SAVE_INTERVAL_SECS.load(Ordering::Relaxed)
}

pub fn set_save_interval_secs(secs: u64) -> Result<(), String> {
    if secs == 0 {
        return Err("kv.save_interval_secs must be at least 1; turn saving off with kv.persist = off".into());
    }
    SAVE_INTERVAL_SECS.store(secs, Ordering::Relaxed);
    Ok(())
}

/// A byte size as Redis writes one: `100`, `64mb`, `1gb`, `512k`.
pub fn parse_bytes(text: &str) -> Result<usize, String> {
    let t = text.trim().to_ascii_lowercase();
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (digits, unit) = t.split_at(split);
    let n: usize = digits
        .parse()
        .map_err(|_| format!("`{text}` is not a size (e.g. 0, 1048576, 512mb, 2gb)"))?;
    let mult: usize = match unit {
        "" | "b" => 1,
        "k" => 1000,
        "kb" => 1024,
        "m" => 1000 * 1000,
        "mb" => 1024 * 1024,
        "g" => 1000 * 1000 * 1000,
        "gb" => 1024 * 1024 * 1024,
        _ => return Err(format!("`{text}` has an unknown unit; use b, kb, mb or gb")),
    };
    n.checked_mul(mult).ok_or_else(|| format!("`{text}` is too large"))
}

pub fn human_bytes(n: usize) -> String {
    const UNITS: [&str; 5] = ["B", "K", "M", "G", "T"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    format!("{v:.2}{}", UNITS[u])
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// What a command can fail with. Rendered with Redis's prefixes and wording,
/// so a client ported from Redis recognises them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvError {
    WrongType,
    NotInteger,
    NotFloat,
    Overflow,
    OutOfMemory,
    NoSuchKey,
    /// `SAVE` on a database that has no directory.
    NoLocation,
    /// Rendered as `ERR <message>`.
    Other(String),
}

impl std::fmt::Display for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KvError::WrongType => write!(f, "WRONGTYPE Operation against a key holding the wrong kind of value"),
            KvError::NotInteger => write!(f, "ERR value is not an integer or out of range"),
            KvError::NotFloat => write!(f, "ERR value is not a valid float"),
            KvError::Overflow => write!(f, "ERR increment or decrement would overflow"),
            KvError::OutOfMemory => write!(f, "OOM command not allowed when used memory > 'maxmemory'."),
            KvError::NoSuchKey => write!(f, "ERR no such key"),
            KvError::NoLocation => write!(
                f,
                "ERR this database is in memory and has no directory to save a snapshot into"
            ),
            KvError::Other(message) => write!(f, "ERR {message}"),
        }
    }
}

// ---------------------------------------------------------------------------
// The handle a database holds
// ---------------------------------------------------------------------------

/// Counters the RESP server keeps for `INFO`. The command count is striped by
/// thread so counting is not a line every core writes.
#[derive(Default)]
pub struct ServerStats {
    pub connected: AtomicUsize,
    pub connections_total: AtomicU64,
    pub blocked: AtomicUsize,
    pub rejected: AtomicU64,
    commands: [cacheguard::CacheGuard<AtomicU64>; 32],
}

impl ServerStats {
    #[inline]
    pub fn count_commands(&self, n: u64) {
        thread_local! {
            static SLOT: usize = {
                static NEXT: AtomicUsize = AtomicUsize::new(0);
                NEXT.fetch_add(1, Ordering::Relaxed) % 32
            };
        }
        SLOT.with(|slot| self.commands[*slot].fetch_add(n, Ordering::Relaxed));
    }

    pub fn commands(&self) -> u64 {
        self.commands.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }

    pub fn reset(&self) {
        for c in &self.commands {
            c.store(0, Ordering::Relaxed);
        }
        self.connections_total.store(0, Ordering::Relaxed);
        self.rejected.store(0, Ordering::Relaxed);
    }
}

pub struct Inner {
    dbs: [OnceLock<Arc<KvCache>>; DATABASES],
    location: Option<PathBuf>,
    pub pubsub: pubsub::PubSub,
    pub stats: ServerStats,
    /// Unix seconds of the last successful save.
    pub last_save: AtomicU64,
    pub saving: AtomicBool,
    pub last_save_ok: AtomicBool,
    pub started: std::time::Instant,
    /// The RESP port, for `INFO` and `CONFIG GET port` (0 when not listening).
    pub port: AtomicU64,
    /// Serialises saves, so a `SAVE` and the periodic one never write the
    /// same temporary file at once.
    save_lock: Mutex<()>,
    sweeper: OnceLock<()>,
}

/// The database's key/value store, built on first use.
///
/// The stores are created lazily, so a database that never issues a `KV`
/// statement or opens the RESP port never allocates one. Cloning is cheap; it
/// is one `Arc`.
#[derive(Clone)]
pub struct KvCacheHandle {
    inner: Arc<Inner>,
}

impl Default for KvCacheHandle {
    fn default() -> Self {
        Self::new(None)
    }
}

/// Every handle in the process, for the save on shutdown.
static ALL: Mutex<Vec<Weak<Inner>>> = Mutex::new(Vec::new());

impl KvCacheHandle {
    pub fn new(location: Option<&str>) -> Self {
        let inner = Arc::new(Inner {
            dbs: std::array::from_fn(|_| OnceLock::new()),
            location: location.map(PathBuf::from),
            pubsub: pubsub::PubSub::default(),
            stats: ServerStats::default(),
            last_save: AtomicU64::new(store::now_ms() / 1000),
            saving: AtomicBool::new(false),
            last_save_ok: AtomicBool::new(true),
            started: std::time::Instant::now(),
            port: AtomicU64::new(0),
            save_lock: Mutex::new(()),
            sweeper: OnceLock::new(),
        });
        {
            // Every database opened registers here, so drop the ones that are
            // gone: a long-running test suite opens thousands.
            let mut all = ALL.lock();
            all.retain(|w| w.strong_count() > 0);
            all.push(Arc::downgrade(&inner));
        }
        KvCacheHandle { inner }
    }

    pub fn inner(&self) -> &Arc<Inner> {
        &self.inner
    }

    /// Database `db`, building it if this is its first use.
    pub fn db(&self, db: usize) -> &Arc<KvCache> {
        let store = self.inner.dbs[db].get_or_init(|| Arc::new(KvCache::new(db)));
        self.start_sweeper();
        store
    }

    /// Database `db` if it has been built.
    pub fn existing(&self, db: usize) -> Option<&Arc<KvCache>> {
        self.inner.dbs.get(db)?.get()
    }

    pub fn snapshot_path(&self) -> Result<PathBuf, KvError> {
        self.inner
            .location
            .as_ref()
            .map(|dir| dir.join(SNAPSHOT_FILE))
            .ok_or(KvError::NoLocation)
    }

    /// Writes every database to the snapshot file. Returns how many keys.
    pub fn save(&self) -> Result<usize, KvError> {
        let _one_at_a_time = self.inner.save_lock.lock();
        let result = snapshot::save(self);
        self.inner.last_save_ok.store(result.is_ok(), Ordering::Relaxed);
        if result.is_ok() {
            self.inner
                .last_save
                .store(store::now_ms() / 1000, Ordering::Relaxed);
        }
        result
    }

    /// Whether anything changed since the last save.
    pub fn is_dirty(&self) -> bool {
        self.inner.dbs.iter().filter_map(|d| d.get()).any(|store| {
            store.total_version() != store.saved_version.load(Ordering::Relaxed)
        })
    }

    /// Writes since the last save, summed over every database, for `INFO`.
    pub fn changes_since_save(&self) -> u64 {
        self.inner
            .dbs
            .iter()
            .filter_map(|d| d.get())
            .map(|store| {
                store
                    .total_version()
                    .wrapping_sub(store.saved_version.load(Ordering::Relaxed))
            })
            .sum()
    }

    /// Loads the snapshot beside the database, if there is one. A snapshot
    /// that fails to load is reported and left where it is; the store starts
    /// empty.
    pub fn load_snapshot_if_present(&self) {
        let Ok(path) = self.snapshot_path() else {
            return;
        };
        if !path.exists() {
            return;
        }
        match snapshot::load(self, &path) {
            Ok((loaded, skipped)) => info!(
                "kv cache: loaded {loaded} keys from {}{}",
                path.display(),
                if skipped > 0 {
                    format!(" ({skipped} had expired)")
                } else {
                    String::new()
                }
            ),
            Err(e) => warn!(
                "kv cache: could not load {}: {e}; starting empty and leaving the file",
                path.display()
            ),
        }
    }

    /// The sweeper: active expiry, the LRU clock, and the periodic save. One
    /// per handle, started with the first store; ends when the handle goes.
    fn start_sweeper(&self) {
        self.inner.sweeper.get_or_init(|| {
            let Ok(runtime) = tokio::runtime::Handle::try_current() else {
                // No runtime (a unit test of the store): reads still check
                // lifetimes themselves, so only memory reclaim is lost.
                return;
            };
            let weak = Arc::downgrade(&self.inner);
            runtime.spawn(async move {
                let mut since_save = std::time::Instant::now();
                loop {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let Some(inner) = weak.upgrade() else {
                        break;
                    };
                    store::tick_lru_clock();
                    for store in inner.dbs.iter().filter_map(|d| d.get()) {
                        // Keep going while a pass finds a quarter of what it
                        // examines expired: a burst of keys that expire
                        // together goes together, not over the next minute.
                        for _ in 0..16 {
                            let (removed, examined) = store.sweep(64 * 1024);
                            if examined == 0 || removed * 4 < examined {
                                break;
                            }
                        }
                    }
                    if persist()
                        && inner.location.is_some()
                        && since_save.elapsed().as_secs() >= save_interval_secs()
                    {
                        since_save = std::time::Instant::now();
                        let handle = KvCacheHandle { inner: inner.clone() };
                        if handle.is_dirty() && !inner.saving.swap(true, Ordering::AcqRel) {
                            let outcome = tokio::task::spawn_blocking(move || handle.save()).await;
                            inner.saving.store(false, Ordering::Release);
                            match outcome {
                                Ok(Ok(keys)) => info!("kv cache: saved {keys} keys"),
                                Ok(Err(e)) => error!("kv cache: periodic save failed: {e}"),
                                Err(e) => error!("kv cache: periodic save panicked: {e}"),
                            }
                        }
                    }
                }
            });
        });
    }
}

/// Saves every persisting store, for the process's clean shutdown. Only with
/// `kv.persist` on: a store never asked to persist is a cache, and a cache
/// that writes a file on exit is a surprise on the next start.
pub fn save_all_on_exit() {
    if !persist() {
        return;
    }
    let handles: Vec<KvCacheHandle> = ALL
        .lock()
        .iter()
        .filter_map(Weak::upgrade)
        .map(|inner| KvCacheHandle { inner })
        .collect();
    for handle in handles {
        if handle.inner.location.is_none() || !handle.is_dirty() {
            continue;
        }
        match handle.save() {
            Ok(keys) => info!("kv cache: saved {keys} keys on shutdown"),
            Err(e) => error!("kv cache: save on shutdown failed: {e}"),
        }
    }
}
