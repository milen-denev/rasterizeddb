//! An in-process key/value cache, driven from SQL: `KV SET`, `KV GET`, …
//!
//! ```text
//! KV SET session:42 '{"user":7}' EX 3600      -- a string, gone in an hour
//! KV GET session:42                            -- one row, or none
//! KV INCR hits:/home                           -- an atomic counter
//! KV HSET user:7 name 'Ada' plan 'pro'         -- a hash
//! KV LPUSH queue:mail job-1 job-2              -- a list
//! KV SADD tags:7 rust db                       -- a set
//! KV KEYS 'session:*'  /  KV SCAN 0 COUNT 100  -- the keyspace
//! KV INFO  /  KV SAVE  /  KV FLUSHALL
//! ```
//!
//! The statement set is Redis's, keyed by the leading `KV` so it can share a
//! connection with SQL. [`sql`] parses and answers them; this module is the
//! store they run against, and [`snapshot`] is how it survives a restart when
//! asked to.
//!
//! # Why this is a separate thing from the `kv:` table backend
//!
//! [`crate::core::storage_providers::kv`] keeps *tables* in an object store:
//! bytes at offsets, chunked and committed. This keeps *keys*: a flat space of
//! names, each holding a string, a counter, a hash, a list or a set, each with
//! an optional lifetime. The two share a name because they answer the same
//! question — "can this database be my key/value store?" — from opposite ends:
//! that one makes S3 hold a table, this one makes the process hold a cache.
//!
//! # What it is built on, and what that decides
//!
//! Every key lives in one [`AtomicGenericCache`]: an open-addressed table of
//! cache-line-sized slots, each an `ArcSwap` over the entry, with a 16-slot
//! probe window and no locks anywhere on the read path. A `GET` is a hash, at
//! most sixteen line reads, and one refcount bump. That is the whole reason to
//! build on it, and it fixes three properties that the rest of the design is
//! shaped around:
//!
//! * **The table has a fixed size.** It is allocated once, `max_keys × 2`
//!   slots so the load factor stays under a half, and never grows — 64 bytes a
//!   slot, so a million keys of capacity is 128 MiB before a single value.
//!   It is allocated lazily, on the first `KV` statement, so a database that
//!   never issues one pays nothing. `max_keys` is a start-up setting.
//! * **A full probe window evicts.** When the sixteen slots a key hashes to
//!   are all live, storing it displaces the least recently *written* of them.
//!   That is the policy Redis calls `allkeys-lru` in spirit — an approximation
//!   sampled from the neighbourhood rather than from the whole keyspace — and
//!   it is on by default because a cache that refuses is a cache that stops
//!   caching. `kv.eviction = refuse` turns it into Redis's `noeviction`: a
//!   `SET` into a full store is an error. Either way [`KvCache::info`]
//!   reports every eviction, because a key that silently vanished is the
//!   worst thing a store can do.
//! * **A slot matches on a 64-bit hash.** Two keys with the same hash would
//!   read as one, so every entry carries its key and every read compares it.
//!   A collision is answered as a refusal to store the second key — counted,
//!   and loud — not as the second key's value under the first key's name.
//!
//! What the table does *not* give is atomic read-modify-write. `INCR`, `SET NX`
//! and `HSET` all read the entry and write back what they computed, and two of
//! them racing on one key would each store a version that forgot the other.
//! Worse, the table's own writer protocol lets two concurrent inserts of the
//! *same* key land in two slots, after which reads answer from whichever slot
//! comes first and a `DEL` removes one of them. So every write to a key takes a
//! lock striped by the key's hash — one uncontended mutex acquisition, some
//! twenty nanoseconds, on the write path and nothing on the read path — and
//! that lock is what makes each command atomic per key, which is the guarantee
//! Redis gives by having one thread.
//!
//! # Lifetimes
//!
//! An expiry is a timestamp on the entry. A read of an expired key answers
//! "absent" and removes it; a background sweep walks the table a few thousand
//! slots at a time and removes the rest, staying on a stretch while it keeps
//! finding expired keys, the way Redis's active expiry does. `DBSIZE` may
//! therefore be slightly high — it counts what has not been swept yet — and
//! `TTL` is exact.
//!
//! # Persistence
//!
//! Off by default; this is a cache. `KV SAVE` writes the whole keyspace to
//! `db_kv_cache.snap` beside the database, and a database that finds that file
//! at open loads it, dropping whatever expired in between. With
//! `kv.persist = on` the sweep also saves every `kv.save_interval_secs` and
//! the process saves on a clean shutdown. That is Redis's RDB, without the
//! append-only log: what is lost in a crash is everything since the last save.

pub mod snapshot;
pub mod sql;
#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ahash::RandomState;
use cacheguard::CacheGuard;
use log::{error, info, warn};
use parking_lot::{Mutex, RwLock};

use crate::cache::atomic_cache::{AtomicGenericCache, Placed};

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

/// Keys the store is sized for. Read when the store is first built, which is
/// the first `KV` statement — so a `SET CONFIG` before that takes effect and
/// one after it does not, and `SHOW CONFIG` says which.
static MAX_KEYS: AtomicUsize = AtomicUsize::new(1 << 20);
/// `true` refuses a store into a full window; `false` evicts its oldest key.
static EVICTION_REFUSES: AtomicBool = AtomicBool::new(false);
/// Whether the sweep saves on an interval and the process saves on exit.
static PERSIST: AtomicBool = AtomicBool::new(false);
static SAVE_INTERVAL_SECS: AtomicU64 = AtomicU64::new(60);

pub fn max_keys() -> usize {
    MAX_KEYS.load(Ordering::Relaxed)
}

/// Sizes the *next* store to be built. Refuses zero, because a table with no
/// slots would evict every key on arrival and report it faithfully.
pub fn set_max_keys(keys: usize) -> Result<(), String> {
    if keys == 0 {
        return Err("kv.max_keys must be at least 1".to_string());
    }
    MAX_KEYS.store(keys, Ordering::Relaxed);
    Ok(())
}

pub fn eviction() -> &'static str {
    if EVICTION_REFUSES.load(Ordering::Relaxed) {
        "refuse"
    } else {
        "evict"
    }
}

pub fn set_eviction(policy: &str) -> Result<(), String> {
    match policy.trim().to_ascii_lowercase().as_str() {
        "evict" | "allkeys-lru" | "lru" => EVICTION_REFUSES.store(false, Ordering::Relaxed),
        "refuse" | "noeviction" | "error" => EVICTION_REFUSES.store(true, Ordering::Relaxed),
        other => {
            return Err(format!(
                "`{other}` is not an eviction policy; use evict (the oldest key in a full \
                 window makes room) or refuse (a SET into a full store is an error)"
            ));
        }
    }
    Ok(())
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
        return Err("kv.save_interval_secs must be at least 1; turn saving off with kv.persist = off"
            .to_string());
    }
    SAVE_INTERVAL_SECS.store(secs, Ordering::Relaxed);
    Ok(())
}

/// Where a database keeps its snapshot, beside its tables.
pub const SNAPSHOT_FILE: &str = "db_kv_cache.snap";

/// Lock stripes. Sixteen threads writing random keys share a stripe about as
/// often as they share a core, and the array is 64 KiB padded — small enough
/// to stay in L2.
const STRIPES: usize = 1024;

/// Slots the sweep examines per pass. A pass is a few microseconds, and it
/// runs ten times a second, so a table of two million slots is walked in
/// about a minute — or much faster while it keeps finding work.
const SWEEP_SLOTS: usize = 2048;
const SWEEP_INTERVAL: Duration = Duration::from_millis(100);

/// The bytes an entry is assumed to cost beyond its own key and value: the
/// slot, the `Arc`, the boxes. A round figure, and `KV INFO` says it is one.
const ENTRY_OVERHEAD: usize = 128;
const ELEMENT_OVERHEAD: usize = 32;

// ---------------------------------------------------------------------------
// Entries
// ---------------------------------------------------------------------------

/// What a key holds.
///
/// Strings are immutable and replaced whole — a `SET` is a new entry. The
/// structures are mutated in place under their own lock, so `HSET` on a
/// hash of ten thousand fields is one insertion and not a copy of ten
/// thousand.
pub enum Value {
    Str(Box<[u8]>),
    /// A string that `INCR` produced. Kept as a number so the next `INCR` is
    /// an add and not a parse; rendered as decimal wherever a string is read.
    Int(i64),
    Hash(RwLock<HashMap<Box<[u8]>, Box<[u8]>, RandomState>>),
    List(RwLock<VecDeque<Box<[u8]>>>),
    Set(RwLock<HashSet<Box<[u8]>, RandomState>>),
}

impl Default for Value {
    fn default() -> Self {
        Value::Str(Box::default())
    }
}

impl Value {
    /// The type name `KV TYPE` reports, which is Redis's.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Str(_) | Value::Int(_) => "string",
            Value::Hash(_) => "hash",
            Value::List(_) => "list",
            Value::Set(_) => "set",
        }
    }

    /// Bytes this value is charged for. Walks a structure, so it is asked
    /// when an entry comes or goes and not per element.
    fn footprint(&self) -> usize {
        match self {
            Value::Str(s) => s.len(),
            Value::Int(_) => 8,
            Value::Hash(h) => h
                .read()
                .iter()
                .map(|(f, v)| f.len() + v.len() + ELEMENT_OVERHEAD)
                .sum(),
            Value::List(l) => l.read().iter().map(|v| v.len() + ELEMENT_OVERHEAD).sum(),
            Value::Set(s) => s.read().iter().map(|v| v.len() + ELEMENT_OVERHEAD).sum(),
        }
    }
}

/// One key and what it holds.
#[derive(Default)]
pub struct Entry {
    /// Kept so a read can tell this key from another with the same hash.
    pub key: Box<[u8]>,
    /// Milliseconds since the epoch, or zero for a key that does not expire.
    /// Atomic so `EXPIRE` and `PERSIST` change it in place rather than
    /// rebuilding the entry — which for a structure would mean copying it.
    pub expires_at: AtomicU64,
    pub value: Value,
}

impl Entry {
    fn new(key: &[u8], value: Value, expires_at: u64) -> Arc<Self> {
        Arc::new(Entry {
            key: key.into(),
            expires_at: AtomicU64::new(expires_at),
            value,
        })
    }

    pub fn expires_at(&self) -> u64 {
        self.expires_at.load(Ordering::Relaxed)
    }

    fn is_expired(&self, now: u64) -> bool {
        let at = self.expires_at();
        at != 0 && now >= at
    }

    fn footprint(&self) -> usize {
        ENTRY_OVERHEAD + self.key.len() + self.value.footprint()
    }

    /// The string form of a string-typed entry, or the type that it is.
    pub fn as_string(&self) -> Result<Vec<u8>, KvError> {
        match &self.value {
            Value::Str(s) => Ok(s.to_vec()),
            Value::Int(n) => Ok(n.to_string().into_bytes()),
            _ => Err(KvError::WrongType),
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// What a command can fail with. The messages are Redis's where Redis has
/// one, so a client ported from it recognises them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvError {
    WrongType,
    NotInteger,
    Overflow,
    /// The store is full and the policy is to refuse rather than evict.
    Full { max_keys: usize },
    /// Another key already occupies this key's hash.
    Collision,
    NoSuchKey,
    /// `SAVE` on a database that has no directory.
    NoLocation,
    Other(String),
}

impl std::fmt::Display for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KvError::WrongType => {
                write!(f, "WRONGTYPE Operation against a key holding the wrong kind of value")
            }
            KvError::NotInteger => write!(f, "ERR value is not an integer or out of range"),
            KvError::Overflow => write!(f, "ERR increment or decrement would overflow"),
            KvError::Full { max_keys } => write!(
                f,
                "OOM the key/value store is full (kv.max_keys = {max_keys}); DEL some keys, \
                 restart with a larger --kv-max-keys, or SET CONFIG kv.eviction = evict"
            ),
            KvError::Collision => write!(
                f,
                "ERR this key's 64-bit hash is already taken by a different key, so it cannot \
                 be stored; rename it"
            ),
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
// The store
// ---------------------------------------------------------------------------

/// How a `SET` decides whether to store, and what lifetime to give the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SetOptions {
    pub ttl: Ttl,
    pub condition: Condition,
    /// Return the previous value (`SET … GET`).
    pub get: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Ttl {
    /// The key does not expire, and a lifetime it had is cleared — which is
    /// what a plain `SET` does in Redis.
    #[default]
    None,
    /// Expires `ms` from now.
    Ms(u64),
    /// Expires at this absolute millisecond timestamp.
    At(u64),
    /// Keeps whatever lifetime the key already had (`KEEPTTL`).
    Keep,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Condition {
    #[default]
    Always,
    /// Only if the key does not exist (`NX`).
    IfAbsent,
    /// Only if it does (`XX`).
    IfPresent,
}

/// What a `SET` did.
pub struct SetOutcome {
    pub stored: bool,
    /// The value the key held before, when `SetOptions::get` asked for it.
    pub previous: Option<Vec<u8>>,
}

#[repr(align(64))]
struct Stripe(Mutex<()>);

/// Counters `KV INFO` reports.
///
/// The three the read and write paths touch are each on their own line, for
/// the reason the query cache's are: a hit counted on one core must not stall
/// a miss being counted on another.
struct Stats {
    hits: CacheGuard<AtomicU64>,
    misses: CacheGuard<AtomicU64>,
    writes: CacheGuard<AtomicU64>,
    evicted: AtomicU64,
    expired: AtomicU64,
    refused: AtomicU64,
    collisions: AtomicU64,
    /// Approximate bytes held; see [`ENTRY_OVERHEAD`].
    memory: AtomicI64,
}

pub struct KvCache {
    slots: Arc<AtomicGenericCache<u64, Arc<Entry>>>,
    stripes: Box<[Stripe]>,
    hasher: RandomState,
    max_keys: usize,
    /// Where `SAVE` writes, or `None` for a database in memory.
    location: Option<PathBuf>,
    stats: Stats,
    sweep_cursor: AtomicUsize,
    /// Bumped by every write; the snapshot records what it saw, so an
    /// interval with no writes saves nothing.
    generation: AtomicU64,
    saved_generation: AtomicU64,
    created: SystemTime,
}

/// Milliseconds since the epoch, which is what every lifetime is measured in.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl KvCache {
    /// Builds a store for `max_keys` keys, beside `location` if it has one.
    ///
    /// The table is the only large allocation and it is made here, in full:
    /// `2 × max_keys` slots rounded to a power of two, 64 bytes each.
    pub fn new(max_keys: usize, location: Option<PathBuf>) -> Arc<Self> {
        let max_keys = max_keys.max(1);
        let slots = AtomicGenericCache::new(max_keys.saturating_mul(2), 0);
        let stripes: Vec<Stripe> = (0..STRIPES).map(|_| Stripe(Mutex::new(()))).collect();
        Arc::new(KvCache {
            slots,
            stripes: stripes.into_boxed_slice(),
            hasher: RandomState::new(),
            max_keys,
            location,
            stats: Stats {
                hits: AtomicU64::new(0).into(),
                misses: AtomicU64::new(0).into(),
                writes: AtomicU64::new(0).into(),
                evicted: AtomicU64::new(0),
                expired: AtomicU64::new(0),
                refused: AtomicU64::new(0),
                collisions: AtomicU64::new(0),
                memory: AtomicI64::new(0),
            },
            sweep_cursor: AtomicUsize::new(0),
            generation: AtomicU64::new(0),
            saved_generation: AtomicU64::new(0),
            created: SystemTime::now(),
        })
    }

    pub fn max_keys(&self) -> usize {
        self.max_keys
    }

    /// Slots in the table, which is the number of keys it could hold if every
    /// probe window filled evenly — it will not, so `max_keys` is the figure
    /// to plan on.
    pub fn capacity(&self) -> usize {
        self.slots.capacity()
    }

    pub fn location(&self) -> Option<&PathBuf> {
        self.location.as_ref()
    }

    pub fn snapshot_path(&self) -> Result<PathBuf, KvError> {
        self.location
            .as_ref()
            .map(|dir| dir.join(SNAPSHOT_FILE))
            .ok_or(KvError::NoLocation)
    }

    #[inline]
    fn hash(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    #[inline]
    fn stripe(&self, hash: u64) -> &Mutex<()> {
        &self.stripes[(hash as usize) & (STRIPES - 1)].0
    }

    /// The live entry for `key`, or `None` if it is absent, expired, or the
    /// slot belongs to a different key with the same hash. Does not count
    /// hits and misses — that is `get`'s job, and the write paths that call
    /// this are not reads.
    #[inline]
    fn live(&self, hash: u64, key: &[u8], now: u64) -> Option<Arc<Entry>> {
        let entry = self.slots.get_settled(&hash)?;
        if *entry.key != *key {
            return None;
        }
        if entry.is_expired(now) {
            return None;
        }
        Some(entry)
    }

    /// Whatever the slot holds for `hash`, expired or not, if it is `key`.
    #[inline]
    fn resident(&self, hash: u64, key: &[u8]) -> Option<Arc<Entry>> {
        let entry = self.slots.get_settled(&hash)?;
        (*entry.key == *key).then_some(entry)
    }

    /// Whether the slot for `hash` is held by a key that is not `key`.
    #[inline]
    fn collides(&self, hash: u64, key: &[u8]) -> bool {
        self.slots
            .get_settled(&hash)
            .is_some_and(|entry| *entry.key != *key)
    }

    /// Stores `entry` under `hash`, applying the eviction policy and the
    /// accounting. Caller holds the stripe and has checked for a collision.
    fn store(&self, hash: u64, entry: Arc<Entry>, replaced: Option<&Arc<Entry>>) -> Result<(), KvError> {
        let added = entry.footprint() as i64;
        let removed = replaced.map_or(0, |old| old.footprint() as i64);
        if replaced.is_none()
            && EVICTION_REFUSES.load(Ordering::Relaxed)
            && self.slots.len() >= self.max_keys
        {
            self.stats.refused.fetch_add(1, Ordering::Relaxed);
            return Err(KvError::Full {
                max_keys: self.max_keys,
            });
        }
        match self.slots.insert_evicting(hash, entry) {
            Ok(Placed::Updated) | Ok(Placed::Inserted) => {}
            Ok(Placed::Evicted(victim)) => {
                self.stats.evicted.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .memory
                    .fetch_sub(victim.footprint() as i64, Ordering::Relaxed);
            }
            Ok(Placed::Failed) => {
                return Err(KvError::Other(
                    "the store could not place the key after several attempts; try again"
                        .to_string(),
                ));
            }
            Err(e) => return Err(KvError::Other(e.to_string())),
        }
        self.stats
            .memory
            .fetch_add(added - removed, Ordering::Relaxed);
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        self.generation.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Removes `key`'s entry if the slot holds it. Caller holds the stripe.
    fn erase(&self, hash: u64, key: &[u8]) -> Option<Arc<Entry>> {
        let entry = self.resident(hash, key)?;
        let removed = self.slots.remove(&hash)?;
        self.stats
            .memory
            .fetch_sub(removed.footprint() as i64, Ordering::Relaxed);
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        self.generation.fetch_add(1, Ordering::Relaxed);
        debug_assert!(Arc::ptr_eq(&entry, &removed) || *removed.key == *key);
        Some(removed)
    }

    /// Removes `key` if it is still the expired entry a reader just saw.
    fn expire_now(&self, hash: u64, key: &[u8], seen: &Arc<Entry>) {
        let _stripe = self.stripe(hash).lock();
        if let Some(current) = self.resident(hash, key) {
            if Arc::ptr_eq(&current, seen) && current.is_expired(now_ms()) {
                self.erase(hash, key);
                self.stats.expired.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    // ---- strings ----------------------------------------------------------

    /// The live entry for `key`. The one read that takes no lock.
    pub fn get(&self, key: &[u8]) -> Option<Arc<Entry>> {
        let hash = self.hash(key);
        let now = now_ms();
        match self.slots.get_settled(&hash) {
            Some(entry) if *entry.key == *key => {
                if entry.is_expired(now) {
                    self.stats.misses.fetch_add(1, Ordering::Relaxed);
                    self.expire_now(hash, key, &entry);
                    None
                } else {
                    self.stats.hits.fetch_add(1, Ordering::Relaxed);
                    Some(entry)
                }
            }
            _ => {
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// `GET`: the string at `key`, `None` if absent, an error if it is a
    /// structure.
    pub fn get_string(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.get(key).map(|entry| entry.as_string()).transpose()
    }

    /// `SET` and every variant of it.
    pub fn set(&self, key: &[u8], value: Vec<u8>, options: SetOptions) -> Result<SetOutcome, KvError> {
        let hash = self.hash(key);
        let now = now_ms();
        let _stripe = self.stripe(hash).lock();
        if self.collides(hash, key) {
            self.stats.collisions.fetch_add(1, Ordering::Relaxed);
            return Err(KvError::Collision);
        }
        let existing = self.live(hash, key, now);
        let previous = if options.get {
            match &existing {
                Some(entry) => Some(entry.as_string()?),
                None => None,
            }
        } else {
            None
        };
        let stored = match options.condition {
            Condition::Always => true,
            Condition::IfAbsent => existing.is_none(),
            Condition::IfPresent => existing.is_some(),
        };
        if !stored {
            return Ok(SetOutcome {
                stored: false,
                previous,
            });
        }
        let expires_at = match options.ttl {
            Ttl::None => 0,
            Ttl::Ms(ms) => now.saturating_add(ms),
            Ttl::At(at) => at,
            Ttl::Keep => existing.as_ref().map_or(0, |e| e.expires_at()),
        };
        // `existing` is the *live* entry; the slot may hold an expired one,
        // which the store replaces and must account for.
        let replaced = self.resident(hash, key);
        let entry = Entry::new(key, Value::Str(value.into_boxed_slice()), expires_at);
        self.store(hash, entry, replaced.as_ref())?;
        Ok(SetOutcome {
            stored: true,
            previous,
        })
    }

    /// `MSET`: every pair, each atomically; not atomic as a whole.
    pub fn mset(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), KvError> {
        for (key, value) in pairs {
            self.set(&key, value, SetOptions::default())?;
        }
        Ok(())
    }

    /// `DEL`: how many of `keys` existed.
    pub fn delete(&self, keys: &[Vec<u8>]) -> u64 {
        let now = now_ms();
        let mut removed = 0;
        for key in keys {
            let hash = self.hash(key);
            let _stripe = self.stripe(hash).lock();
            let was_live = self.live(hash, key, now).is_some();
            if self.erase(hash, key).is_some() && was_live {
                removed += 1;
            }
        }
        removed
    }

    /// `GETDEL`.
    pub fn get_del(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        let hash = self.hash(key);
        let _stripe = self.stripe(hash).lock();
        let Some(entry) = self.live(hash, key, now_ms()) else {
            return Ok(None);
        };
        let value = entry.as_string()?;
        self.erase(hash, key);
        Ok(Some(value))
    }

    /// `EXISTS`: how many of `keys` exist, counting a key given twice twice.
    pub fn exists(&self, keys: &[Vec<u8>]) -> u64 {
        let now = now_ms();
        keys.iter()
            .filter(|key| self.live(self.hash(key), key, now).is_some())
            .count() as u64
    }

    /// `INCRBY` / `DECRBY`: the value after adding `delta`, from zero for an
    /// absent key. Keeps the key's lifetime, as Redis does.
    pub fn incr_by(&self, key: &[u8], delta: i64) -> Result<i64, KvError> {
        let hash = self.hash(key);
        let _stripe = self.stripe(hash).lock();
        if self.collides(hash, key) {
            self.stats.collisions.fetch_add(1, Ordering::Relaxed);
            return Err(KvError::Collision);
        }
        let existing = self.live(hash, key, now_ms());
        let (current, expires_at) = match &existing {
            Some(entry) => (integer_of(&entry.value)?, entry.expires_at()),
            None => (0, 0),
        };
        let next = current.checked_add(delta).ok_or(KvError::Overflow)?;
        let replaced = self.resident(hash, key);
        self.store(hash, Entry::new(key, Value::Int(next), expires_at), replaced.as_ref())?;
        Ok(next)
    }

    /// `APPEND`: the length after appending.
    pub fn append(&self, key: &[u8], suffix: &[u8]) -> Result<usize, KvError> {
        let hash = self.hash(key);
        let _stripe = self.stripe(hash).lock();
        if self.collides(hash, key) {
            self.stats.collisions.fetch_add(1, Ordering::Relaxed);
            return Err(KvError::Collision);
        }
        let existing = self.live(hash, key, now_ms());
        let (mut bytes, expires_at) = match &existing {
            Some(entry) => (entry.as_string()?, entry.expires_at()),
            None => (Vec::new(), 0),
        };
        bytes.extend_from_slice(suffix);
        let length = bytes.len();
        let replaced = self.resident(hash, key);
        self.store(
            hash,
            Entry::new(key, Value::Str(bytes.into_boxed_slice()), expires_at),
            replaced.as_ref(),
        )?;
        Ok(length)
    }

    /// `STRLEN`: zero for an absent key.
    pub fn strlen(&self, key: &[u8]) -> Result<usize, KvError> {
        match self.get(key) {
            Some(entry) => match &entry.value {
                Value::Str(s) => Ok(s.len()),
                Value::Int(n) => Ok(n.to_string().len()),
                _ => Err(KvError::WrongType),
            },
            None => Ok(0),
        }
    }

    // ---- lifetimes ---------------------------------------------------------

    /// Sets when `key` expires (`0` never); `false` if the key is absent.
    pub fn expire_at(&self, key: &[u8], at_ms: u64) -> bool {
        let hash = self.hash(key);
        let _stripe = self.stripe(hash).lock();
        match self.live(hash, key, now_ms()) {
            Some(entry) => {
                entry.expires_at.store(at_ms, Ordering::Relaxed);
                self.generation.fetch_add(1, Ordering::Relaxed);
                true
            }
            None => false,
        }
    }

    /// `PTTL`: milliseconds left, `-1` for no lifetime, `-2` for no key.
    pub fn ttl_ms(&self, key: &[u8]) -> i64 {
        let now = now_ms();
        match self.live(self.hash(key), key, now) {
            None => -2,
            Some(entry) => match entry.expires_at() {
                0 => -1,
                at => at.saturating_sub(now) as i64,
            },
        }
    }

    /// `TYPE`: Redis's type names, and `none` for an absent key.
    pub fn type_of(&self, key: &[u8]) -> &'static str {
        match self.live(self.hash(key), key, now_ms()) {
            Some(entry) => entry.value.type_name(),
            None => "none",
        }
    }

    /// `RENAME`: moves the entry, lifetime included, over whatever `to` held.
    pub fn rename(&self, from: &[u8], to: &[u8]) -> Result<(), KvError> {
        if from == to {
            return match self.live(self.hash(from), from, now_ms()) {
                Some(_) => Ok(()),
                None => Err(KvError::NoSuchKey),
            };
        }
        let (from_hash, to_hash) = (self.hash(from), self.hash(to));
        // Both stripes, in address order so two renames crossing each other
        // cannot deadlock; one lock when they share a stripe.
        let (first, second) = (self.stripe(from_hash), self.stripe(to_hash));
        let _guards = if std::ptr::eq(first, second) {
            (first.lock(), None)
        } else if (first as *const Mutex<()>) < (second as *const Mutex<()>) {
            (first.lock(), Some(second.lock()))
        } else {
            let second_guard = second.lock();
            (first.lock(), Some(second_guard))
        };
        let entry = self
            .live(from_hash, from, now_ms())
            .ok_or(KvError::NoSuchKey)?;
        if self.collides(to_hash, to) {
            // Whatever holds `to`'s hash is not `to` (that case is a live
            // entry, replaced below). Refused for the reason `SET` refuses.
            self.stats.collisions.fetch_add(1, Ordering::Relaxed);
            return Err(KvError::Collision);
        }
        let moved = Arc::new(Entry {
            key: to.into(),
            expires_at: AtomicU64::new(entry.expires_at()),
            value: take_value(&entry.value),
        });
        let replaced = self.resident(to_hash, to);
        self.store(to_hash, moved, replaced.as_ref())?;
        self.erase(from_hash, from);
        Ok(())
    }

    // ---- keyspace ----------------------------------------------------------

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// `FLUSHALL`: how many keys went.
    pub fn flush(&self) -> usize {
        // Every stripe, so no write is half-done when the table empties
        // under it — a `SET` that had checked for a collision and not yet
        // stored would otherwise land in the emptied table, which is fine,
        // or race the clear's state transitions, which is not.
        let _guards: Vec<_> = self.stripes.iter().map(|s| s.0.lock()).collect();
        let cleared = self.slots.clear();
        self.stats.memory.store(0, Ordering::Relaxed);
        self.generation.fetch_add(1, Ordering::Relaxed);
        cleared
    }

    /// `KEYS`: every live key matching `pattern`, a whole-table walk.
    pub fn keys(&self, pattern: &[u8]) -> Vec<Box<[u8]>> {
        let now = now_ms();
        let mut out = Vec::new();
        let all = pattern == b"*";
        self.slots.scan_slots(0, self.capacity(), |_, entry, _| {
            if !entry.is_expired(now) && (all || glob_match(pattern, &entry.key)) {
                out.push(entry.key.clone());
            }
        });
        out
    }

    /// `SCAN`: keys from slot `cursor` on, and the cursor to continue from —
    /// zero when the walk is over.
    ///
    /// `count` is how many keys to gather before stopping; the walk also
    /// stops after a bounded number of slots so a sparse table cannot make one
    /// call read the whole thing. A page can therefore be empty with a
    /// non-zero cursor, which is what Redis's contract allows too.
    pub fn scan(&self, cursor: u64, pattern: Option<&[u8]>, count: usize) -> (u64, Vec<Box<[u8]>>) {
        let now = now_ms();
        let count = count.max(1);
        let capacity = self.capacity();
        let mut at = (cursor as usize).min(capacity);
        let budget = count.saturating_mul(64).max(65_536);
        let mut examined = 0usize;
        let mut out = Vec::with_capacity(count.min(1024));
        while at < capacity && out.len() < count && examined < budget {
            let step = SWEEP_SLOTS.min(capacity - at);
            self.slots.scan_slots(at, step, |_, entry, _| {
                if out.len() < count
                    && !entry.is_expired(now)
                    && pattern.is_none_or(|p| glob_match(p, &entry.key))
                {
                    out.push(entry.key.clone());
                }
            });
            at += step;
            examined += step;
        }
        let next = if at >= capacity { 0 } else { at as u64 };
        (next, out)
    }

    /// Every live entry, for the snapshot.
    pub(crate) fn for_each_live<F: FnMut(&Arc<Entry>)>(&self, mut visit: F) {
        let now = now_ms();
        self.slots.scan_slots(0, self.capacity(), |_, entry, _| {
            if !entry.is_expired(now) {
                visit(&entry);
            }
        });
    }

    /// `INFO`: what the store is, and what it has done.
    pub fn info(&self) -> Vec<(&'static str, String)> {
        let hits = self.stats.hits.load(Ordering::Relaxed);
        let misses = self.stats.misses.load(Ordering::Relaxed);
        let uptime = self.created.elapsed().unwrap_or_default().as_secs();
        vec![
            ("keys", self.len().to_string()),
            ("max_keys", self.max_keys.to_string()),
            ("slots", self.capacity().to_string()),
            (
                "used_memory_bytes",
                self.stats.memory.load(Ordering::Relaxed).max(0).to_string(),
            ),
            (
                "table_bytes",
                (self.capacity() * std::mem::size_of::<u64>() * 8).to_string(),
            ),
            ("eviction", eviction().to_string()),
            ("keyspace_hits", hits.to_string()),
            ("keyspace_misses", misses.to_string()),
            (
                "hit_rate",
                if hits + misses == 0 {
                    "0".to_string()
                } else {
                    format!("{:.4}", hits as f64 / (hits + misses) as f64)
                },
            ),
            ("writes", self.stats.writes.load(Ordering::Relaxed).to_string()),
            ("evicted_keys", self.stats.evicted.load(Ordering::Relaxed).to_string()),
            ("expired_keys", self.stats.expired.load(Ordering::Relaxed).to_string()),
            ("refused_writes", self.stats.refused.load(Ordering::Relaxed).to_string()),
            ("hash_collisions", self.stats.collisions.load(Ordering::Relaxed).to_string()),
            ("persist", persist().to_string()),
            ("save_interval_secs", save_interval_secs().to_string()),
            (
                "snapshot",
                self.snapshot_path()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|_| "(in-memory database; SAVE is refused)".to_string()),
            ),
            (
                "unsaved_writes",
                self.generation
                    .load(Ordering::Relaxed)
                    .saturating_sub(self.saved_generation.load(Ordering::Relaxed))
                    .to_string(),
            ),
            ("uptime_secs", uptime.to_string()),
        ]
    }

    /// The number of writes since the store was built, for the snapshot to
    /// record what it saw.
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    pub(crate) fn mark_saved(&self, generation: u64) {
        self.saved_generation.store(generation, Ordering::Relaxed);
    }

    /// Whether anything changed since the last save.
    pub fn is_dirty(&self) -> bool {
        self.generation.load(Ordering::Relaxed) != self.saved_generation.load(Ordering::Relaxed)
    }

    /// Restores one entry from a snapshot. Refuses rather than evicts when the
    /// snapshot holds more than the store: a snapshot taken under a larger
    /// `max_keys` should lose its tail loudly, not its head silently.
    pub(crate) fn restore(&self, entry: Arc<Entry>) -> Result<(), KvError> {
        let hash = self.hash(&entry.key);
        let _stripe = self.stripe(hash).lock();
        if self.collides(hash, &entry.key) {
            self.stats.collisions.fetch_add(1, Ordering::Relaxed);
            return Err(KvError::Collision);
        }
        if self.slots.len() >= self.max_keys {
            return Err(KvError::Full {
                max_keys: self.max_keys,
            });
        }
        let replaced = self.resident(hash, &entry.key);
        self.store(hash, entry, replaced.as_ref())
    }

    // ---- hashes ------------------------------------------------------------

    /// The live structure at `key`, made if absent by `make`, or the type it
    /// actually is. Caller holds the stripe.
    fn structure_for_write(
        &self,
        hash: u64,
        key: &[u8],
        make: fn() -> Value,
        is: fn(&Value) -> bool,
    ) -> Result<Arc<Entry>, KvError> {
        if self.collides(hash, key) {
            self.stats.collisions.fetch_add(1, Ordering::Relaxed);
            return Err(KvError::Collision);
        }
        if let Some(entry) = self.live(hash, key, now_ms()) {
            return if is(&entry.value) {
                Ok(entry)
            } else {
                Err(KvError::WrongType)
            };
        }
        let replaced = self.resident(hash, key);
        let entry = Entry::new(key, make(), 0);
        self.store(hash, entry.clone(), replaced.as_ref())?;
        Ok(entry)
    }

    /// The live structure at `key` for reading, `None` if absent.
    fn structure_for_read(&self, key: &[u8], is: fn(&Value) -> bool) -> Result<Option<Arc<Entry>>, KvError> {
        match self.get(key) {
            Some(entry) if is(&entry.value) => Ok(Some(entry)),
            Some(_) => Err(KvError::WrongType),
            None => Ok(None),
        }
    }

    /// Records a change inside a structure: the bytes and the write.
    fn mutated(&self, delta: i64) {
        self.stats.memory.fetch_add(delta, Ordering::Relaxed);
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    /// `HSET`: how many of `pairs` were new fields.
    pub fn hset(&self, key: &[u8], pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<u64, KvError> {
        let hash = self.hash(key);
        let _stripe = self.stripe(hash).lock();
        let entry = self.structure_for_write(hash, key, new_hash, is_hash)?;
        let Value::Hash(map) = &entry.value else {
            return Err(KvError::WrongType);
        };
        let mut map = map.write();
        let mut added = 0;
        let mut delta = 0i64;
        for (field, value) in pairs {
            delta += value.len() as i64;
            match map.insert(field.into_boxed_slice(), value.into_boxed_slice()) {
                Some(old) => delta -= old.len() as i64,
                None => {
                    added += 1;
                    delta += ELEMENT_OVERHEAD as i64;
                }
            }
        }
        drop(map);
        self.mutated(delta);
        Ok(added)
    }

    pub fn hget(&self, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        Ok(self
            .structure_for_read(key, is_hash)?
            .and_then(|entry| match &entry.value {
                Value::Hash(map) => map.read().get(field).map(|v| v.to_vec()),
                _ => None,
            }))
    }

    pub fn hmget(&self, key: &[u8], fields: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, KvError> {
        let entry = self.structure_for_read(key, is_hash)?;
        Ok(match entry.as_ref().map(|e| &e.value) {
            Some(Value::Hash(map)) => {
                let map = map.read();
                fields
                    .iter()
                    .map(|f| map.get(f.as_slice()).map(|v| v.to_vec()))
                    .collect()
            }
            _ => fields.iter().map(|_| None).collect(),
        })
    }

    pub fn hgetall(&self, key: &[u8]) -> Result<Vec<(Box<[u8]>, Box<[u8]>)>, KvError> {
        let entry = self.structure_for_read(key, is_hash)?;
        Ok(match entry.as_ref().map(|e| &e.value) {
            Some(Value::Hash(map)) => {
                let mut pairs: Vec<_> = map
                    .read()
                    .iter()
                    .map(|(f, v)| (f.clone(), v.clone()))
                    .collect();
                // A hash has no order; a stable one is a kindness to a test
                // and to anyone reading the output.
                pairs.sort();
                pairs
            }
            _ => Vec::new(),
        })
    }

    /// `HDEL`: how many of `fields` existed. Deletes the key when the last
    /// field goes, as Redis does.
    pub fn hdel(&self, key: &[u8], fields: &[Vec<u8>]) -> Result<u64, KvError> {
        let hash = self.hash(key);
        let _stripe = self.stripe(hash).lock();
        let Some(entry) = self.live(hash, key, now_ms()) else {
            return Ok(0);
        };
        let Value::Hash(map) = &entry.value else {
            return Err(KvError::WrongType);
        };
        let mut map = map.write();
        let mut removed = 0;
        let mut delta = 0i64;
        for field in fields {
            if let Some(old) = map.remove(field.as_slice()) {
                removed += 1;
                delta -= (field.len() + old.len() + ELEMENT_OVERHEAD) as i64;
            }
        }
        let empty = map.is_empty();
        drop(map);
        if removed > 0 {
            self.mutated(delta);
        }
        if empty {
            self.erase(hash, key);
        }
        Ok(removed)
    }

    pub fn hexists(&self, key: &[u8], field: &[u8]) -> Result<bool, KvError> {
        Ok(self.hget(key, field)?.is_some())
    }

    pub fn hlen(&self, key: &[u8]) -> Result<u64, KvError> {
        Ok(self
            .structure_for_read(key, is_hash)?
            .map_or(0, |entry| match &entry.value {
                Value::Hash(map) => map.read().len() as u64,
                _ => 0,
            }))
    }

    pub fn hkeys(&self, key: &[u8]) -> Result<Vec<Box<[u8]>>, KvError> {
        Ok(self.hgetall(key)?.into_iter().map(|(f, _)| f).collect())
    }

    pub fn hvals(&self, key: &[u8]) -> Result<Vec<Box<[u8]>>, KvError> {
        Ok(self.hgetall(key)?.into_iter().map(|(_, v)| v).collect())
    }

    pub fn hincr_by(&self, key: &[u8], field: &[u8], delta: i64) -> Result<i64, KvError> {
        let hash = self.hash(key);
        let _stripe = self.stripe(hash).lock();
        let entry = self.structure_for_write(hash, key, new_hash, is_hash)?;
        let Value::Hash(map) = &entry.value else {
            return Err(KvError::WrongType);
        };
        let mut map = map.write();
        let current = match map.get(field) {
            Some(bytes) => parse_integer(bytes)?,
            None => 0,
        };
        let next = current.checked_add(delta).ok_or(KvError::Overflow)?;
        let rendered = next.to_string().into_bytes().into_boxed_slice();
        let mut memory = rendered.len() as i64;
        match map.insert(field.into(), rendered) {
            Some(old) => memory -= old.len() as i64,
            None => memory += (field.len() + ELEMENT_OVERHEAD) as i64,
        }
        drop(map);
        self.mutated(memory);
        Ok(next)
    }

    // ---- lists -------------------------------------------------------------

    /// `LPUSH` / `RPUSH`: the list's length afterwards. Values are pushed in
    /// order, so `LPUSH k a b` leaves `b` at the head, as Redis does.
    pub fn push(&self, key: &[u8], values: Vec<Vec<u8>>, left: bool) -> Result<u64, KvError> {
        let hash = self.hash(key);
        let _stripe = self.stripe(hash).lock();
        let entry = self.structure_for_write(hash, key, new_list, is_list)?;
        let Value::List(list) = &entry.value else {
            return Err(KvError::WrongType);
        };
        let mut list = list.write();
        let mut delta = 0i64;
        for value in values {
            delta += (value.len() + ELEMENT_OVERHEAD) as i64;
            let value = value.into_boxed_slice();
            if left {
                list.push_front(value);
            } else {
                list.push_back(value);
            }
        }
        let len = list.len() as u64;
        drop(list);
        self.mutated(delta);
        Ok(len)
    }

    /// `LPOP` / `RPOP`: up to `count` values. Deletes the key when the list
    /// empties.
    pub fn pop(&self, key: &[u8], count: usize, left: bool) -> Result<Vec<Box<[u8]>>, KvError> {
        let hash = self.hash(key);
        let _stripe = self.stripe(hash).lock();
        let Some(entry) = self.live(hash, key, now_ms()) else {
            return Ok(Vec::new());
        };
        let Value::List(list) = &entry.value else {
            return Err(KvError::WrongType);
        };
        let mut list = list.write();
        let mut out = Vec::with_capacity(count.min(list.len()));
        let mut delta = 0i64;
        for _ in 0..count {
            let Some(value) = (if left { list.pop_front() } else { list.pop_back() }) else {
                break;
            };
            delta -= (value.len() + ELEMENT_OVERHEAD) as i64;
            out.push(value);
        }
        let empty = list.is_empty();
        drop(list);
        if !out.is_empty() {
            self.mutated(delta);
        }
        if empty {
            self.erase(hash, key);
        }
        Ok(out)
    }

    pub fn llen(&self, key: &[u8]) -> Result<u64, KvError> {
        Ok(self
            .structure_for_read(key, is_list)?
            .map_or(0, |entry| match &entry.value {
                Value::List(list) => list.read().len() as u64,
                _ => 0,
            }))
    }

    /// `LRANGE`, with Redis's inclusive, negative-from-the-end indexes.
    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Box<[u8]>>, KvError> {
        let Some(entry) = self.structure_for_read(key, is_list)? else {
            return Ok(Vec::new());
        };
        let Value::List(list) = &entry.value else {
            return Ok(Vec::new());
        };
        let list = list.read();
        let len = list.len() as i64;
        let norm = |i: i64| if i < 0 { (len + i).max(0) } else { i.min(len) };
        let (start, stop) = (norm(start), norm(stop).min(len - 1));
        if len == 0 || start > stop {
            return Ok(Vec::new());
        }
        Ok(list
            .range(start as usize..=stop as usize)
            .cloned()
            .collect())
    }

    pub fn lindex(&self, key: &[u8], index: i64) -> Result<Option<Box<[u8]>>, KvError> {
        let Some(entry) = self.structure_for_read(key, is_list)? else {
            return Ok(None);
        };
        let Value::List(list) = &entry.value else {
            return Ok(None);
        };
        let list = list.read();
        let len = list.len() as i64;
        let index = if index < 0 { len + index } else { index };
        if index < 0 || index >= len {
            return Ok(None);
        }
        Ok(list.get(index as usize).cloned())
    }

    // ---- sets --------------------------------------------------------------

    pub fn sadd(&self, key: &[u8], members: Vec<Vec<u8>>) -> Result<u64, KvError> {
        let hash = self.hash(key);
        let _stripe = self.stripe(hash).lock();
        let entry = self.structure_for_write(hash, key, new_set, is_set)?;
        let Value::Set(set) = &entry.value else {
            return Err(KvError::WrongType);
        };
        let mut set = set.write();
        let mut added = 0;
        let mut delta = 0i64;
        for member in members {
            let size = member.len();
            if set.insert(member.into_boxed_slice()) {
                added += 1;
                delta += (size + ELEMENT_OVERHEAD) as i64;
            }
        }
        drop(set);
        if added > 0 {
            self.mutated(delta);
        }
        Ok(added)
    }

    pub fn srem(&self, key: &[u8], members: &[Vec<u8>]) -> Result<u64, KvError> {
        let hash = self.hash(key);
        let _stripe = self.stripe(hash).lock();
        let Some(entry) = self.live(hash, key, now_ms()) else {
            return Ok(0);
        };
        let Value::Set(set) = &entry.value else {
            return Err(KvError::WrongType);
        };
        let mut set = set.write();
        let mut removed = 0;
        let mut delta = 0i64;
        for member in members {
            if set.remove(member.as_slice()) {
                removed += 1;
                delta -= (member.len() + ELEMENT_OVERHEAD) as i64;
            }
        }
        let empty = set.is_empty();
        drop(set);
        if removed > 0 {
            self.mutated(delta);
        }
        if empty {
            self.erase(hash, key);
        }
        Ok(removed)
    }

    pub fn smembers(&self, key: &[u8]) -> Result<Vec<Box<[u8]>>, KvError> {
        let Some(entry) = self.structure_for_read(key, is_set)? else {
            return Ok(Vec::new());
        };
        let Value::Set(set) = &entry.value else {
            return Ok(Vec::new());
        };
        let mut members: Vec<_> = set.read().iter().cloned().collect();
        members.sort();
        Ok(members)
    }

    pub fn sismember(&self, key: &[u8], member: &[u8]) -> Result<bool, KvError> {
        Ok(self
            .structure_for_read(key, is_set)?
            .is_some_and(|entry| match &entry.value {
                Value::Set(set) => set.read().contains(member),
                _ => false,
            }))
    }

    pub fn scard(&self, key: &[u8]) -> Result<u64, KvError> {
        Ok(self
            .structure_for_read(key, is_set)?
            .map_or(0, |entry| match &entry.value {
                Value::Set(set) => set.read().len() as u64,
                _ => 0,
            }))
    }

    // ---- sweeping ----------------------------------------------------------

    /// One pass of active expiry: examines `SWEEP_SLOTS` slots from the
    /// cursor and removes what has expired. Returns how many it removed and
    /// how many it examined, so the caller can decide whether to go straight
    /// round again.
    pub fn sweep_once(&self) -> (usize, usize) {
        let now = now_ms();
        let capacity = self.capacity();
        let start = self.sweep_cursor.load(Ordering::Relaxed) % capacity;
        let mut expired: Vec<(u64, Arc<Entry>)> = Vec::new();
        let mut seen = 0usize;
        let examined = self.slots.scan_slots(start, SWEEP_SLOTS, |hash, entry, _| {
            seen += 1;
            if entry.is_expired(now) {
                expired.push((hash, entry));
            }
        });
        let next = if start + examined >= capacity {
            0
        } else {
            start + examined
        };
        self.sweep_cursor.store(next, Ordering::Relaxed);
        let mut removed = 0;
        for (_, entry) in expired {
            let hash = self.hash(&entry.key);
            self.expire_now(hash, &entry.key, &entry);
            removed += 1;
        }
        (removed, seen)
    }

    /// Runs the sweep, and the periodic save, for as long as the store lives.
    ///
    /// Holds a `Weak`, so dropping the last `Arc` — the database going away —
    /// ends the task. Needs a runtime; a store built outside one (a unit test
    /// of the store itself) simply has no active expiry, and reads still
    /// answer correctly because they check the lifetime themselves.
    pub fn spawn_sweeper(this: &Arc<Self>) {
        let weak: Weak<Self> = Arc::downgrade(this);
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        handle.spawn(async move {
            let mut last_save = std::time::Instant::now();
            loop {
                tokio::time::sleep(SWEEP_INTERVAL).await;
                let Some(store) = weak.upgrade() else {
                    break;
                };
                // Stay on it while it keeps finding work: a burst of keys
                // that expire together should go together, not over the
                // next minute.
                for _ in 0..16 {
                    let (removed, seen) = store.sweep_once();
                    if seen == 0 || removed * 4 < seen {
                        break;
                    }
                }
                if persist()
                    && store.location.is_some()
                    && last_save.elapsed().as_secs() >= save_interval_secs()
                {
                    last_save = std::time::Instant::now();
                    if store.is_dirty() {
                        let store = store.clone();
                        let outcome =
                            tokio::task::spawn_blocking(move || snapshot::save(&store)).await;
                        match outcome {
                            Ok(Ok(keys)) => info!("kv cache: saved {keys} keys"),
                            Ok(Err(e)) => error!("kv cache: periodic save failed: {e}"),
                            Err(e) => error!("kv cache: periodic save panicked: {e}"),
                        }
                    }
                }
            }
        });
    }
}

fn new_hash() -> Value {
    Value::Hash(RwLock::new(HashMap::with_hasher(RandomState::new())))
}
fn new_list() -> Value {
    Value::List(RwLock::new(VecDeque::new()))
}
fn new_set() -> Value {
    Value::Set(RwLock::new(HashSet::with_hasher(RandomState::new())))
}
fn is_hash(v: &Value) -> bool {
    matches!(v, Value::Hash(_))
}
fn is_list(v: &Value) -> bool {
    matches!(v, Value::List(_))
}
fn is_set(v: &Value) -> bool {
    matches!(v, Value::Set(_))
}

/// Moves a structure out of an entry that is about to be erased, for
/// `RENAME`. A string is cloned; a structure is swapped for an empty one,
/// which the erased entry then carries to its grave.
fn take_value(value: &Value) -> Value {
    match value {
        Value::Str(s) => Value::Str(s.clone()),
        Value::Int(n) => Value::Int(*n),
        Value::Hash(map) => Value::Hash(RwLock::new(std::mem::replace(
            &mut *map.write(),
            HashMap::with_hasher(RandomState::new()),
        ))),
        Value::List(list) => Value::List(RwLock::new(std::mem::take(&mut *list.write()))),
        Value::Set(set) => Value::Set(RwLock::new(std::mem::replace(
            &mut *set.write(),
            HashSet::with_hasher(RandomState::new()),
        ))),
    }
}

fn integer_of(value: &Value) -> Result<i64, KvError> {
    match value {
        Value::Int(n) => Ok(*n),
        Value::Str(s) => parse_integer(s),
        _ => Err(KvError::WrongType),
    }
}

/// Redis's integer: an optional sign and decimal digits, nothing else.
fn parse_integer(bytes: &[u8]) -> Result<i64, KvError> {
    let text = std::str::from_utf8(bytes).map_err(|_| KvError::NotInteger)?;
    if text.is_empty() || text.starts_with('+') || text.contains(char::is_whitespace) {
        return Err(KvError::NotInteger);
    }
    text.parse::<i64>().map_err(|_| KvError::NotInteger)
}

/// Redis's glob: `*`, `?`, `[abc]`, `[a-z]`, `[^a]`, and `\` to escape.
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let (mut p, mut t) = (0usize, 0usize);
    // Where the last `*` was, and where in the text it had matched up to, so
    // a failure further on can retry with the star eating one more byte.
    let mut star: Option<(usize, usize)> = None;
    while t < text.len() {
        if p < pattern.len() {
            match pattern[p] {
                b'*' => {
                    star = Some((p, t));
                    p += 1;
                    continue;
                }
                b'?' => {
                    p += 1;
                    t += 1;
                    continue;
                }
                b'[' => {
                    if let Some((matched, next)) = class_match(&pattern[p..], text[t]) {
                        if matched {
                            p += next;
                            t += 1;
                            continue;
                        }
                    }
                }
                b'\\' if p + 1 < pattern.len() => {
                    if pattern[p + 1] == text[t] {
                        p += 2;
                        t += 1;
                        continue;
                    }
                }
                c => {
                    if c == text[t] {
                        p += 1;
                        t += 1;
                        continue;
                    }
                }
            }
        }
        match star {
            Some((sp, st)) => {
                p = sp + 1;
                t = st + 1;
                star = Some((sp, st + 1));
            }
            None => return false,
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

/// Matches `c` against the class starting at `class[0] == b'['`. Returns
/// whether it matched and how many pattern bytes the class spans, or `None`
/// for an unterminated class, which then matches nothing.
fn class_match(class: &[u8], c: u8) -> Option<(bool, usize)> {
    let mut i = 1;
    let negate = class.get(i) == Some(&b'^');
    if negate {
        i += 1;
    }
    let mut matched = false;
    let mut first = true;
    loop {
        let ch = *class.get(i)?;
        if ch == b']' && !first {
            break;
        }
        first = false;
        let lo = if ch == b'\\' {
            i += 1;
            *class.get(i)?
        } else {
            ch
        };
        if class.get(i + 1) == Some(&b'-') && class.get(i + 2).is_some_and(|&h| h != b']') {
            let hi = class[i + 2];
            if lo <= c && c <= hi {
                matched = true;
            }
            i += 3;
        } else {
            if lo == c {
                matched = true;
            }
            i += 1;
        }
    }
    Some((matched != negate, i + 1))
}

// ---------------------------------------------------------------------------
// The handle a database holds
// ---------------------------------------------------------------------------

/// The database's store, built on first use.
///
/// `OnceLock` rather than a field on `Database`, because the table is the
/// only large allocation in this module and a database that never issues a
/// `KV` statement should never make it. The one exception is a database that
/// finds a snapshot at open: it builds the store then, to load it.
#[derive(Default)]
pub struct KvCacheHandle {
    store: OnceLock<Arc<KvCache>>,
    location: Option<PathBuf>,
}

/// Every store built in this process, for the save on shutdown.
static ALL: Mutex<Vec<Weak<KvCache>>> = Mutex::new(Vec::new());

impl KvCacheHandle {
    pub fn new(location: Option<&str>) -> Self {
        KvCacheHandle {
            store: OnceLock::new(),
            location: location.map(PathBuf::from),
        }
    }

    /// The store, building it if this is the first call.
    pub fn get_or_create(&self) -> &Arc<KvCache> {
        self.store.get_or_init(|| {
            let store = KvCache::new(max_keys(), self.location.clone());
            info!(
                "kv cache: built for {} keys ({} slots, {} MiB of table){}",
                store.max_keys(),
                store.capacity(),
                store.capacity() * 64 / (1024 * 1024),
                match &self.location {
                    Some(dir) => format!(", snapshots in {}", dir.display()),
                    None => String::new(),
                }
            );
            KvCache::spawn_sweeper(&store);
            ALL.lock().push(Arc::downgrade(&store));
            store
        })
    }

    /// The store if it has been built, without building it.
    pub fn get(&self) -> Option<&Arc<KvCache>> {
        self.store.get()
    }

    /// Loads the snapshot beside the database, if there is one.
    ///
    /// Called at open. A snapshot that fails to load is reported and left in
    /// place: the store starts empty, and the file is there to look at.
    pub fn load_snapshot_if_present(&self) {
        let Some(dir) = &self.location else {
            return;
        };
        let path = dir.join(SNAPSHOT_FILE);
        if !path.exists() {
            return;
        }
        let store = self.get_or_create();
        match snapshot::load(store, &path) {
            Ok((loaded, skipped)) => info!(
                "kv cache: loaded {loaded} keys from {}{}",
                path.display(),
                if skipped > 0 {
                    format!(" ({skipped} expired or did not fit)")
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
}

/// Saves every persisting store, for the process's clean shutdown.
///
/// Only those with `kv.persist` on: a store that was never asked to persist
/// is a cache, and a cache that writes a file on exit is a surprise on the
/// next start.
pub fn save_all_on_exit() {
    if !persist() {
        return;
    }
    let stores: Vec<Arc<KvCache>> = ALL.lock().iter().filter_map(Weak::upgrade).collect();
    for store in stores {
        if store.location().is_none() || !store.is_dirty() {
            continue;
        }
        match snapshot::save(&store) {
            Ok(keys) => info!("kv cache: saved {keys} keys on shutdown"),
            Err(e) => error!("kv cache: save on shutdown failed: {e}"),
        }
    }
}
