//! The keyspace: 1024 lock-striped shards of a hashbrown table.
//!
//! # Why shards and not one lock-free table
//!
//! Redis is one thread, so it never pays for a lock; it also never uses a
//! second core. A store that wants to beat it on a 16-core machine has to let
//! sixteen threads in at once without making each of them pay more than
//! Redis pays for its whole command. A shard is a `parking_lot` read-write
//! lock over its own table. A `GET` is a hash, one atomic add to take the
//! lock, one probe, a copy of the value into the reply, and the release.
//! With 1024 shards two random keys share a lock about one time in a thousand.
//!
//! Everything a write has to account (bytes held, keys with a lifetime,
//! the version `WATCH` compares) is a plain field of the shard, changed under
//! the write lock. No counter is shared between shards. A process-wide atomic
//! that every write touched would be the one cache line every core fights
//! over, and it would cap the store at whatever that line can sustain.
//!
//! # One allocation per string
//!
//! A slot holds its key and, for a string, the value in one boxed slice:
//! `key ++ value`. Small keys and values are the common case, so `SET` makes
//! one allocation instead of two and a `GET` touches one line of heap after
//! the table.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

use ahash::RandomState;
use hashbrown::HashTable;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use smallvec::SmallVec;

use super::values::{HashVal, ListVal, SetVal, ZSet};
use super::{EvictionPolicy, KvError};

/// Shards per store. A power of two; the shard index takes bits 40.. of the
/// hash, which hashbrown uses for neither its bucket index (low bits) nor its
/// control byte (top seven).
pub const SHARDS: usize = 1024;
const SHARD_SHIFT: u32 = 40;

/// Charged per key beyond its bytes: the slot, the table's control byte and
/// load-factor slack, and the allocation's header.
pub const SLOT_OVERHEAD: usize = std::mem::size_of::<Slot>() + 24;

#[inline]
pub fn shard_of(hash: u64) -> usize {
    ((hash >> SHARD_SHIFT) as usize) & (SHARDS - 1)
}

// ---------------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------------

static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
/// Seconds since the process started using the store, refreshed by the
/// sweeper. The LRU stamp a read leaves on a key.
static LRU_CLOCK: AtomicU32 = AtomicU32::new(0);

pub fn lru_clock() -> u32 {
    LRU_CLOCK.load(Ordering::Relaxed)
}

pub fn tick_lru_clock() {
    let epoch = EPOCH.get_or_init(Instant::now);
    LRU_CLOCK.store(epoch.elapsed().as_secs() as u32, Ordering::Relaxed);
}

/// Milliseconds since the Unix epoch: what every lifetime is measured in.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Slots
// ---------------------------------------------------------------------------

/// What a key holds. A string's bytes live in the slot's buffer after the
/// key, so `Str` carries nothing.
pub enum Value {
    Str,
    /// A string `INCR` produced, kept as a number so the next `INCR` is an add
    /// rather than a parse.
    Int(i64),
    Hash(Box<HashVal>),
    List(Box<ListVal>),
    Set(Box<SetVal>),
    ZSet(Box<ZSet>),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Str | Value::Int(_) => "string",
            Value::Hash(_) => "hash",
            Value::List(_) => "list",
            Value::Set(_) => "set",
            Value::ZSet(_) => "zset",
        }
    }

    fn heap_bytes(&self) -> usize {
        match self {
            Value::Str | Value::Int(_) => 0,
            Value::Hash(h) => h.bytes(),
            Value::List(l) => l.bytes(),
            Value::Set(s) => s.bytes(),
            Value::ZSet(z) => z.bytes(),
        }
    }
}

pub struct Slot {
    data: Box<[u8]>,
    klen: u32,
    lru: AtomicU32,
    /// Milliseconds since the epoch, or zero for no lifetime.
    pub expires_at: u64,
    /// The shard's version when this key was last written: what `WATCH`
    /// compares.
    pub version: u64,
    pub value: Value,
}

impl Slot {
    pub fn string(key: &[u8], value: &[u8], expires_at: u64) -> Slot {
        let mut data = Vec::with_capacity(key.len() + value.len());
        data.extend_from_slice(key);
        data.extend_from_slice(value);
        Slot {
            data: data.into_boxed_slice(),
            klen: key.len() as u32,
            lru: AtomicU32::new(lru_clock()),
            expires_at,
            version: 0,
            value: Value::Str,
        }
    }

    pub fn with_value(key: &[u8], value: Value, expires_at: u64) -> Slot {
        Slot {
            data: key.into(),
            klen: key.len() as u32,
            lru: AtomicU32::new(lru_clock()),
            expires_at,
            version: 0,
            value,
        }
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.data[..self.klen as usize]
    }

    /// A string's bytes; empty for anything else (check `value` first).
    #[inline]
    pub fn str_bytes(&self) -> &[u8] {
        &self.data[self.klen as usize..]
    }

    /// Replaces a string's bytes, keeping the key.
    pub fn set_str(&mut self, value: &[u8]) {
        let klen = self.klen as usize;
        if self.data.len() == klen + value.len() {
            self.data[klen..].copy_from_slice(value);
        } else {
            let mut data = Vec::with_capacity(klen + value.len());
            data.extend_from_slice(&self.data[..klen]);
            data.extend_from_slice(value);
            self.data = data.into_boxed_slice();
        }
        self.value = Value::Str;
    }

    /// Makes this slot an integer, dropping any string bytes.
    pub fn set_int(&mut self, n: i64) {
        if self.data.len() != self.klen as usize {
            self.data = self.data[..self.klen as usize].into();
        }
        self.value = Value::Int(n);
    }

    /// Replaces the value with a structure (or an integer), dropping string bytes.
    pub fn set_value(&mut self, value: Value) {
        if self.data.len() != self.klen as usize {
            self.data = self.data[..self.klen as usize].into();
        }
        self.value = value;
    }

    #[inline]
    pub fn is_expired(&self, now: u64) -> bool {
        self.expires_at != 0 && now >= self.expires_at
    }

    /// Leaves the LRU stamp; a store only when the clock has moved, so a hot
    /// key read from every core is not a line every core writes.
    #[inline]
    pub fn touch(&self) {
        let now = lru_clock();
        if self.lru.load(Ordering::Relaxed) != now {
            self.lru.store(now, Ordering::Relaxed);
        }
    }

    pub fn idle_secs(&self) -> u32 {
        lru_clock().saturating_sub(self.lru.load(Ordering::Relaxed))
    }

    /// The same value under another key, for `RENAME`.
    pub fn rekey(self, key: &[u8]) -> Slot {
        let expires_at = self.expires_at;
        match self.value {
            Value::Str => Slot::string(key, self.str_bytes(), expires_at),
            value => Slot::with_value(key, value, expires_at),
        }
    }

    /// A deep copy under another key, for `COPY`.
    pub fn copy_as(&self, key: &[u8]) -> Slot {
        let value = match &self.value {
            Value::Str => return Slot::string(key, self.str_bytes(), self.expires_at),
            Value::Int(n) => Value::Int(*n),
            Value::Hash(h) => Value::Hash(h.clone()),
            Value::List(l) => Value::List(l.clone()),
            Value::Set(x) => Value::Set(x.clone()),
            Value::ZSet(z) => Value::ZSet(z.clone()),
        };
        Slot::with_value(key, value, self.expires_at)
    }

    /// Bytes this key is charged.
    #[inline]
    pub fn cost(&self) -> usize {
        SLOT_OVERHEAD + self.data.len() + self.value.heap_bytes()
    }

    /// The string form of a string-typed value.
    pub fn string_value(&self) -> Option<std::borrow::Cow<'_, [u8]>> {
        match &self.value {
            Value::Str => Some(std::borrow::Cow::Borrowed(self.str_bytes())),
            Value::Int(n) => Some(std::borrow::Cow::Owned(n.to_string().into_bytes())),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Shards
// ---------------------------------------------------------------------------

pub struct Shard {
    pub(crate) table: HashTable<Slot>,
    /// Bytes charged to the keys in this shard.
    pub used: usize,
    /// Keys here with a lifetime, so the sweeper can skip shards without any.
    pub ttl_keys: usize,
    /// Bumped by every write; a slot records the value it was written at.
    pub version: u64,
    pub evicted: u64,
    pub expired: u64,
    pub sweep_cursor: usize,
    rng: u64,
    hasher: RandomState,
}

impl Shard {
    fn new(hasher: RandomState, seed: u64) -> Shard {
        Shard {
            table: HashTable::new(),
            used: 0,
            ttl_keys: 0,
            version: 0,
            evicted: 0,
            expired: 0,
            sweep_cursor: 0,
            rng: seed | 1,
            hasher,
        }
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// The slot for `key`, expired or not.
    #[inline]
    pub fn find(&self, h: u64, key: &[u8]) -> Option<&Slot> {
        self.table.find(h, |s| s.key() == key)
    }

    /// The slot for `key` unless it has expired. Expired slots stay until a
    /// write or the sweeper removes them; a reader holds only a read lock.
    #[inline]
    pub fn live(&self, h: u64, key: &[u8], now: u64) -> Option<&Slot> {
        self.find(h, key).filter(|s| !s.is_expired(now))
    }

    /// The live slot for `key`, removing it first if it has expired.
    pub fn live_mut(&mut self, h: u64, key: &[u8], now: u64) -> Option<&mut Slot> {
        match self.table.find_entry(h, |s| s.key() == key) {
            Ok(entry) => {
                if entry.get().is_expired(now) {
                    let (slot, _) = entry.remove();
                    self.used -= slot.cost();
                    if slot.expires_at != 0 {
                        self.ttl_keys -= 1;
                    }
                    self.expired += 1;
                    self.version += 1;
                    None
                } else {
                    Some(entry.into_mut())
                }
            }
            Err(_) => None,
        }
    }

    #[inline]
    fn next_version(&mut self) -> u64 {
        self.version += 1;
        self.version
    }

    /// Stores a new key. The caller has made sure it is absent (or removed
    /// the old one).
    pub fn insert(&mut self, h: u64, mut slot: Slot) {
        slot.version = self.next_version();
        self.used += slot.cost();
        if slot.expires_at != 0 {
            self.ttl_keys += 1;
        }
        let hasher = &self.hasher;
        self.table
            .insert_unique(h, slot, |s| hasher.hash_one(s.key()));
    }

    /// Removes `key`, live or expired.
    pub fn remove(&mut self, h: u64, key: &[u8]) -> Option<Slot> {
        let entry = self.table.find_entry(h, |s| s.key() == key).ok()?;
        let (slot, _) = entry.remove();
        self.used -= slot.cost();
        if slot.expires_at != 0 {
            self.ttl_keys -= 1;
        }
        self.version += 1;
        Some(slot)
    }

    /// Changes the live slot for `key` in place with `f`, keeping the byte
    /// charge, the lifetime count and the version right whatever `f` does.
    /// `None` when the key is absent or expired.
    pub fn update<T>(
        &mut self,
        h: u64,
        key: &[u8],
        now: u64,
        f: impl FnOnce(&mut Slot) -> T,
    ) -> Option<T> {
        let entry = self.table.find_entry(h, |s| s.key() == key).ok()?;
        if entry.get().is_expired(now) {
            // Expired: gone, and the caller sees it as absent.
            let (slot, _) = entry.remove();
            self.used -= slot.cost();
            self.ttl_keys -= 1;
            self.expired += 1;
            self.version += 1;
            return None;
        }
        self.version += 1;
        let slot = entry.into_mut();
        let (before, had_ttl) = (slot.cost(), slot.expires_at != 0);
        let out = f(slot);
        slot.version = self.version;
        let (after, has_ttl) = (slot.cost(), slot.expires_at != 0);
        self.used = self.used + after - before;
        match (had_ttl, has_ttl) {
            (false, true) => self.ttl_keys += 1,
            (true, false) => self.ttl_keys -= 1,
            _ => {}
        }
        Some(out)
    }

    /// Stores `slot` under its key, replacing whatever was there.
    pub fn put(&mut self, h: u64, slot: Slot) -> Option<Slot> {
        let old = self.remove(h, slot.key());
        self.insert(h, slot);
        old
    }

    /// Removes and returns a removable entry when the shard is over its
    /// budget, choosing like Redis: sample five, take the best by policy.
    /// `None` when the policy finds nothing it may evict.
    fn evict_one(&mut self, policy: EvictionPolicy) -> Option<Slot> {
        let buckets = self.table.num_buckets();
        if self.table.is_empty() {
            return None;
        }
        let volatile = matches!(
            policy,
            EvictionPolicy::VolatileLru | EvictionPolicy::VolatileRandom | EvictionPolicy::VolatileTtl
        );
        if volatile && self.ttl_keys == 0 {
            return None;
        }
        let mut best: Option<(usize, u64)> = None; // (bucket, badness: higher evicts first)
        let mut found = 0;
        let mut tries = 0;
        while found < 5 && tries < 256 {
            tries += 1;
            self.rng ^= self.rng << 13;
            self.rng ^= self.rng >> 7;
            self.rng ^= self.rng << 17;
            let index = (self.rng as usize) % buckets;
            let Some(slot) = self.table.get_bucket(index) else {
                continue;
            };
            if volatile && slot.expires_at == 0 {
                continue;
            }
            found += 1;
            let badness = match policy {
                EvictionPolicy::AllKeysLru | EvictionPolicy::VolatileLru => slot.idle_secs() as u64,
                EvictionPolicy::VolatileTtl => u64::MAX - slot.expires_at,
                _ => 0,
            };
            if best.is_none_or(|(_, b)| badness > b) {
                best = Some((index, badness));
            }
        }
        if best.is_none() {
            // Sampling missed (a sparse table, or few volatile keys): fall
            // back to the first eligible key in bucket order.
            for index in 0..buckets {
                if let Some(slot) = self.table.get_bucket(index) {
                    if !volatile || slot.expires_at != 0 {
                        best = Some((index, 0));
                        break;
                    }
                }
            }
        }
        let (index, _) = best?;
        let entry = self.table.get_bucket_entry(index).ok()?;
        let (slot, _) = entry.remove();
        self.used -= slot.cost();
        if slot.expires_at != 0 {
            self.ttl_keys -= 1;
        }
        self.evicted += 1;
        self.version += 1;
        Some(slot)
    }

    /// Makes room for `need` more bytes under `budget`, evicting by `policy`.
    /// An error when the policy is `noeviction` (or finds nothing to evict) and
    /// the write would take the shard past its budget.
    pub fn make_room(&mut self, need: usize, budget: usize, policy: EvictionPolicy) -> Result<(), KvError> {
        if budget == 0 || self.used + need <= budget {
            return Ok(());
        }
        if policy == EvictionPolicy::NoEviction {
            return Err(KvError::OutOfMemory);
        }
        while self.used + need > budget {
            if self.evict_one(policy).is_none() {
                // Nothing left to evict. A value larger than the shard's
                // whole budget is stored into an empty shard rather than
                // refused, so the shard split never refuses what the store
                // as a whole has room for.
                if self.table.is_empty() {
                    return Ok(());
                }
                return Err(KvError::OutOfMemory);
            }
        }
        Ok(())
    }

    /// Removes up to `limit` expired keys from the next `buckets` buckets.
    /// Returns (removed, examined).
    pub fn sweep(&mut self, now: u64, buckets: usize) -> (usize, usize) {
        let total = self.table.num_buckets();
        if total == 0 || self.ttl_keys == 0 {
            return (0, 0);
        }
        let start = self.sweep_cursor % total;
        let end = (start + buckets).min(total);
        let mut removed = 0;
        for index in start..end {
            let expired = self
                .table
                .get_bucket(index)
                .is_some_and(|s| s.is_expired(now));
            if expired {
                if let Ok(entry) = self.table.get_bucket_entry(index) {
                    let (slot, _) = entry.remove();
                    self.used -= slot.cost();
                    self.ttl_keys -= 1;
                    self.expired += 1;
                    self.version += 1;
                    removed += 1;
                }
            }
        }
        self.sweep_cursor = if end >= total { 0 } else { end };
        (removed, end - start)
    }

    pub fn clear(&mut self) -> usize {
        let n = self.table.len();
        self.table = HashTable::new();
        self.used = 0;
        self.ttl_keys = 0;
        self.version += 1;
        n
    }

    /// Every slot, in bucket order from `start`, until `visit` says stop.
    /// Returns the bucket to resume from (0 when the walk is complete).
    pub fn walk(&self, start: usize, mut visit: impl FnMut(&Slot) -> bool) -> usize {
        let total = self.table.num_buckets();
        let mut index = start;
        while index < total {
            if let Some(slot) = self.table.get_bucket(index) {
                if !visit(slot) {
                    return index + 1;
                }
            }
            index += 1;
        }
        0
    }

    pub fn num_buckets(&self) -> usize {
        self.table.num_buckets()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Slot> {
        self.table.iter()
    }

    /// A random live key, by sampling buckets.
    pub fn random_key(&mut self, now: u64) -> Option<Vec<u8>> {
        if self.table.is_empty() {
            return None;
        }
        let buckets = self.table.num_buckets();
        for _ in 0..512 {
            self.rng ^= self.rng << 13;
            self.rng ^= self.rng >> 7;
            self.rng ^= self.rng << 17;
            if let Some(slot) = self.table.get_bucket((self.rng as usize) % buckets) {
                if !slot.is_expired(now) {
                    return Some(slot.key().to_vec());
                }
            }
        }
        self.table
            .iter()
            .find(|s| !s.is_expired(now))
            .map(|s| s.key().to_vec())
    }

    pub fn next_random(&mut self) -> u64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        self.rng
    }
}

/// A shard and the read counters that live on its line.
#[repr(align(128))]
pub struct ShardCell {
    pub lock: RwLock<Shard>,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}

// ---------------------------------------------------------------------------
// The store
// ---------------------------------------------------------------------------

pub struct KvCache {
    cells: Box<[ShardCell]>,
    hasher: RandomState,
    /// The database number (`SELECT`), for `INFO keyspace`.
    pub db: usize,
    pub waiters: super::blocking::Waiters,
    /// Bumped by `FLUSHALL`/`FLUSHDB`, so every `WATCH` taken before fails.
    pub flush_epoch: AtomicU64,
    /// Total version count at the last save, to tell whether a save is due.
    pub saved_version: AtomicU64,
    /// The shard the next sweep pass starts at, so a pass that runs out of
    /// budget is continued where it stopped rather than from shard 0 again.
    sweep_shard: std::sync::atomic::AtomicUsize,
}

impl KvCache {
    pub fn new(db: usize) -> KvCache {
        let hasher = RandomState::new();
        let mut seed = now_ms() ^ 0x9E37_79B9_7F4A_7C15;
        let cells: Vec<ShardCell> = (0..SHARDS)
            .map(|i| {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(i as u64 | 1);
                ShardCell {
                    lock: RwLock::new(Shard::new(hasher.clone(), seed)),
                    hits: AtomicU64::new(0),
                    misses: AtomicU64::new(0),
                }
            })
            .collect();
        tick_lru_clock();
        KvCache {
            cells: cells.into_boxed_slice(),
            hasher,
            db,
            waiters: super::blocking::Waiters::default(),
            flush_epoch: AtomicU64::new(0),
            saved_version: AtomicU64::new(0),
            sweep_shard: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    #[inline]
    pub fn hash(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    #[inline]
    pub fn cell(&self, h: u64) -> &ShardCell {
        &self.cells[shard_of(h)]
    }

    #[inline]
    pub fn cell_at(&self, index: usize) -> &ShardCell {
        &self.cells[index]
    }

    pub fn cells(&self) -> &[ShardCell] {
        &self.cells
    }

    /// The byte budget of one shard, from the process-wide `maxmemory`.
    #[inline]
    pub fn shard_budget(&self) -> usize {
        super::max_memory() / SHARDS
    }

    pub fn len(&self) -> usize {
        self.cells.iter().map(|c| c.lock.read().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.cells.iter().all(|c| c.lock.read().is_empty())
    }

    /// Sum of every shard's version: changes whenever anything was written.
    pub fn total_version(&self) -> u64 {
        self.cells
            .iter()
            .map(|c| c.lock.read().version)
            .fold(0u64, u64::wrapping_add)
    }

    pub fn flush(&self) -> usize {
        // Every shard at once, in order, so no command sees half a flush.
        let mut guards: Vec<_> = self.cells.iter().map(|c| c.lock.write()).collect();
        let n = guards.iter_mut().map(|g| g.clear()).sum();
        self.flush_epoch.fetch_add(1, Ordering::Relaxed);
        n
    }

    /// One pass of active expiry over every shard with keys that expire.
    /// Spends at most `budget` bucket examinations; returns (removed, examined).
    pub fn sweep(&self, budget: usize) -> (usize, usize) {
        let now = now_ms();
        let mut removed = 0;
        let mut examined = 0;
        let start = self.sweep_shard.load(Ordering::Relaxed) % SHARDS;
        let mut visited = 0;
        while visited < SHARDS && examined < budget {
            let cell = &self.cells[(start + visited) % SHARDS];
            visited += 1;
            // A read to see if there is anything to do, so a shard without
            // lifetimes never takes the write lock.
            if cell.lock.read().ttl_keys == 0 {
                continue;
            }
            let mut shard = cell.lock.write();
            let (r, e) = shard.sweep(now, 256);
            removed += r;
            examined += e;
        }
        self.sweep_shard
            .store((start + visited) % SHARDS, Ordering::Relaxed);
        (removed, examined)
    }

    pub fn stats(&self) -> StoreStats {
        let mut s = StoreStats::default();
        for cell in self.cells.iter() {
            s.hits += cell.hits.load(Ordering::Relaxed);
            s.misses += cell.misses.load(Ordering::Relaxed);
            let shard = cell.lock.read();
            s.keys += shard.len();
            s.expires += shard.ttl_keys;
            s.used += shard.used;
            s.evicted += shard.evicted;
            s.expired += shard.expired;
        }
        s
    }

    pub fn reset_stats(&self) {
        for cell in self.cells.iter() {
            cell.hits.store(0, Ordering::Relaxed);
            cell.misses.store(0, Ordering::Relaxed);
            let mut shard = cell.lock.write();
            shard.evicted = 0;
            shard.expired = 0;
        }
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct StoreStats {
    pub keys: usize,
    pub expires: usize,
    pub used: usize,
    pub hits: u64,
    pub misses: u64,
    pub evicted: u64,
    pub expired: u64,
}

// ---------------------------------------------------------------------------
// Access: how a command reaches its shards
// ---------------------------------------------------------------------------

/// How a command reaches the shards its keys live in.
///
/// Outside a transaction, [`Direct`] takes each shard's lock for the duration
/// of one closure. Inside `EXEC`, [`Pinned`] already holds every shard the
/// transaction touches, and hands them out without locking again. Commands
/// are written once, generic over this, and monomorphised for each.
pub trait Access {
    fn store(&self) -> &KvCache;
    /// The time this command runs at, read once per batch of commands.
    fn now(&self) -> u64;
    fn read<T>(&mut self, h: u64, f: impl FnOnce(&Shard) -> T) -> T;
    fn write<T>(&mut self, h: u64, f: impl FnOnce(&mut Shard) -> T) -> T;
    /// Every shard `hashes` fall in, write-locked together, for a command that
    /// changes several keys and must not be seen half done.
    fn write_many<T>(&mut self, hashes: &[u64], f: impl FnOnce(&mut ShardsMut<'_>) -> T) -> T;
    /// Every shard `hashes` fall in, read-locked together.
    fn read_many<T>(&mut self, hashes: &[u64], f: impl FnOnce(&ShardsRef<'_>) -> T) -> T;
    /// Whether a blocking command may wait (not inside `EXEC`).
    fn may_block(&self) -> bool;
    /// Every shard in index order, read-locked one at a time, until `f`
    /// returns `false`. For `KEYS`, `SCAN`, `DBSIZE`.
    fn each_shard(&mut self, f: impl FnMut(usize, &Shard) -> bool);
    /// Every shard in index order, write-locked one at a time.
    fn each_shard_mut(&mut self, f: impl FnMut(usize, &mut Shard));

    #[inline]
    fn hash(&self, key: &[u8]) -> u64 {
        self.store().hash(key)
    }

    #[inline]
    fn count_read(&self, h: u64, hit: bool) {
        let cell = self.store().cell(h);
        if hit {
            cell.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            cell.misses.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Several shards held at once, by index.
pub struct ShardsMut<'a> {
    held: SmallVec<[(usize, &'a mut Shard); 8]>,
}

impl ShardsMut<'_> {
    #[inline]
    pub fn get(&mut self, h: u64) -> &mut Shard {
        let index = shard_of(h);
        let at = self
            .held
            .binary_search_by_key(&index, |(i, _)| *i)
            .expect("a command reached a shard it did not lock: its key spec is wrong");
        &mut *self.held[at].1
    }
}

pub struct ShardsRef<'a> {
    held: SmallVec<[(usize, &'a Shard); 8]>,
}

impl ShardsRef<'_> {
    #[inline]
    pub fn get(&self, h: u64) -> &Shard {
        let index = shard_of(h);
        let at = self
            .held
            .binary_search_by_key(&index, |(i, _)| *i)
            .expect("a command reached a shard it did not lock: its key spec is wrong");
        self.held[at].1
    }
}

fn sorted_indexes(hashes: &[u64]) -> SmallVec<[usize; 8]> {
    let mut idx: SmallVec<[usize; 8]> = hashes.iter().map(|h| shard_of(*h)).collect();
    idx.sort_unstable();
    idx.dedup();
    idx
}

/// Locks per call: the path every command outside `EXEC` takes.
pub struct Direct<'s> {
    pub store: &'s KvCache,
    pub now: u64,
}

impl<'s> Direct<'s> {
    pub fn new(store: &'s KvCache) -> Self {
        Direct { store, now: now_ms() }
    }
}

impl Access for Direct<'_> {
    #[inline]
    fn store(&self) -> &KvCache {
        self.store
    }
    #[inline]
    fn now(&self) -> u64 {
        self.now
    }
    #[inline]
    fn read<T>(&mut self, h: u64, f: impl FnOnce(&Shard) -> T) -> T {
        let guard = self.store.cell(h).lock.read();
        f(&guard)
    }
    #[inline]
    fn write<T>(&mut self, h: u64, f: impl FnOnce(&mut Shard) -> T) -> T {
        let mut guard = self.store.cell(h).lock.write();
        f(&mut guard)
    }
    fn write_many<T>(&mut self, hashes: &[u64], f: impl FnOnce(&mut ShardsMut<'_>) -> T) -> T {
        let idx = sorted_indexes(hashes);
        let mut guards: SmallVec<[(usize, RwLockWriteGuard<'_, Shard>); 8]> = idx
            .iter()
            .map(|&i| (i, self.store.cell_at(i).lock.write()))
            .collect();
        let mut shards = ShardsMut {
            held: guards.iter_mut().map(|(i, g)| (*i, &mut **g)).collect(),
        };
        f(&mut shards)
    }
    fn read_many<T>(&mut self, hashes: &[u64], f: impl FnOnce(&ShardsRef<'_>) -> T) -> T {
        let idx = sorted_indexes(hashes);
        let guards: SmallVec<[(usize, RwLockReadGuard<'_, Shard>); 8]> = idx
            .iter()
            .map(|&i| (i, self.store.cell_at(i).lock.read()))
            .collect();
        let shards = ShardsRef {
            held: guards.iter().map(|(i, g)| (*i, &**g)).collect(),
        };
        f(&shards)
    }
    fn may_block(&self) -> bool {
        true
    }
    fn each_shard(&mut self, mut f: impl FnMut(usize, &Shard) -> bool) {
        for i in 0..SHARDS {
            let guard = self.store.cell_at(i).lock.read();
            if !f(i, &guard) {
                break;
            }
        }
    }
    fn each_shard_mut(&mut self, mut f: impl FnMut(usize, &mut Shard)) {
        for i in 0..SHARDS {
            let mut guard = self.store.cell_at(i).lock.write();
            f(i, &mut guard);
        }
    }
}

/// Every shard a transaction touches, locked up front in index order.
pub struct Pinned<'s> {
    store: &'s KvCache,
    now: u64,
    guards: Vec<(usize, RwLockWriteGuard<'s, Shard>)>,
}

impl<'s> Pinned<'s> {
    /// Locks every shard, for a transaction that runs a whole-store command.
    pub fn lock_all(store: &'s KvCache) -> Pinned<'s> {
        Pinned {
            store,
            now: now_ms(),
            guards: (0..SHARDS).map(|i| (i, store.cell_at(i).lock.write())).collect(),
        }
    }

    /// Locks the shards `hashes` fall in, in ascending order, which is the
    /// order every multi-shard lock in the store takes, so two transactions
    /// cannot deadlock.
    pub fn lock(store: &'s KvCache, hashes: &[u64]) -> Pinned<'s> {
        let mut idx: Vec<usize> = hashes.iter().map(|h| shard_of(*h)).collect();
        idx.sort_unstable();
        idx.dedup();
        Pinned {
            store,
            now: now_ms(),
            guards: idx.into_iter().map(|i| (i, store.cell_at(i).lock.write())).collect(),
        }
    }

    fn guard(&mut self, h: u64) -> &mut Shard {
        let index = shard_of(h);
        match self.guards.binary_search_by_key(&index, |(i, _)| *i) {
            Ok(at) => &mut self.guards[at].1,
            Err(_) => panic!("a transaction reached a shard it did not lock: a key spec is wrong"),
        }
    }

    /// The live slot's version for `key`, for `WATCH`.
    pub fn version_of(&mut self, key: &[u8]) -> Option<u64> {
        let h = self.store.hash(key);
        self.guard(h).find(h, key).map(|s| s.version)
    }
}

impl Access for Pinned<'_> {
    fn store(&self) -> &KvCache {
        self.store
    }
    fn now(&self) -> u64 {
        self.now
    }
    fn read<T>(&mut self, h: u64, f: impl FnOnce(&Shard) -> T) -> T {
        f(self.guard(h))
    }
    fn write<T>(&mut self, h: u64, f: impl FnOnce(&mut Shard) -> T) -> T {
        f(self.guard(h))
    }
    fn write_many<T>(&mut self, hashes: &[u64], f: impl FnOnce(&mut ShardsMut<'_>) -> T) -> T {
        let need = sorted_indexes(hashes);
        let mut shards = ShardsMut {
            held: self
                .guards
                .iter_mut()
                .filter(|(i, _)| need.binary_search(i).is_ok())
                .map(|(i, g)| (*i, &mut **g))
                .collect(),
        };
        f(&mut shards)
    }
    fn read_many<T>(&mut self, hashes: &[u64], f: impl FnOnce(&ShardsRef<'_>) -> T) -> T {
        let need = sorted_indexes(hashes);
        let shards = ShardsRef {
            held: self
                .guards
                .iter()
                .filter(|(i, _)| need.binary_search(i).is_ok())
                .map(|(i, g)| (*i, &**g))
                .collect(),
        };
        f(&shards)
    }
    fn may_block(&self) -> bool {
        false
    }
    fn each_shard(&mut self, mut f: impl FnMut(usize, &Shard) -> bool) {
        assert_eq!(
            self.guards.len(),
            SHARDS,
            "a whole-store command ran in a transaction that did not lock every shard"
        );
        for (i, guard) in self.guards.iter() {
            if !f(*i, guard) {
                break;
            }
        }
    }
    fn each_shard_mut(&mut self, mut f: impl FnMut(usize, &mut Shard)) {
        assert_eq!(
            self.guards.len(),
            SHARDS,
            "a whole-store command ran in a transaction that did not lock every shard"
        );
        for (i, guard) in self.guards.iter_mut() {
            f(*i, guard);
        }
    }
}
