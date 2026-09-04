//! # Query-result cache
//!
//! Maps `(table, WHERE clause)` to the row pointers that matched it, so a
//! repeated query can skip the scan and the predicate evaluation and go
//! straight to fetching rows.
//!
//! Three invariants make the entries safe to serve:
//!
//!  * **The stored set is always the complete match set.** A scan cut short by
//!    `LIMIT` is never cached, so one entry answers the same `WHERE` at any
//!    limit and in any order — the caller re-applies `ORDER BY`/`LIMIT` on top.
//!    Keying on the limit instead would both fragment the cache and hand a
//!    truncated answer to the unlimited form of the query.
//!  * **Every entry carries the cache generation its scan observed.** A write
//!    bumps the table's generation, which strands every entry produced before
//!    it — including one from a scan that was still in flight when the write
//!    committed. Invalidation is therefore a single atomic increment rather
//!    than a sweep of the whole slot array.
//!  * **Every entry carries its own key.** The slot array matches on a 64-bit
//!    hash alone, so the table and clause are compared again on the way out.
//!    A collision costs a miss instead of serving another query's rows.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use cacheguard::CacheGuard;

use crc::{CRC_64_ECMA_182, Crc};
use rclite::Arc;

use crate::cache::atomic_cache::AtomicGenericCache;
use crate::core::row::row_pointer::RowPointer;

const CRC_64: Crc<u64> = Crc::<u64>::new(&CRC_64_ECMA_182);

/// One cached answer. Shared behind an `Arc` so a hit is a refcount bump
/// rather than a copy of the pointer vector.
#[derive(Debug, Default)]
pub struct QueryCacheEntry {
    /// Table the clause was evaluated against.
    pub table: Box<str>,
    /// The WHERE clause verbatim, as it was handed to the processor.
    pub where_clause: Box<str>,
    /// Cache generation of the table when the scan *started*.
    pub generation: u64,
    /// The limit this entry was produced under.
    ///
    /// `None` means `pointers` is the *complete* match set, which can answer
    /// any limit by truncation. `Some(n)` means the scan stopped early after
    /// `n` matches, so the set is a prefix and may only answer that same limit.
    /// It is part of the cache key for exactly that reason: a prefix stored
    /// under the bare clause would answer the unlimited form with a truncated
    /// result.
    pub limit: Option<u64>,
    /// Every pointer that matched, or the first `limit` of them.
    pub pointers: Vec<RowPointer>,
}

pub type QueryCacheValue = Arc<QueryCacheEntry>;
pub type QueryCache = AtomicGenericCache<u64, QueryCacheValue>;

pub static QUERY_CACHE: OnceLock<std::sync::Arc<QueryCache>> = OnceLock::new();
pub static ENABLE_CACHE: OnceLock<bool> = OnceLock::new();

/// Answered from the cache.
///
/// The three counters are each on their own cache line. Every query increments
/// exactly one of them, from whatever thread it runs on, and as plain adjacent
/// statics all three shared one line — so a hit on one core stalled a miss being
/// counted on another, on the hottest path in the process, to maintain
/// statistics nothing reads until someone asks for them.
static HITS: CacheGuard<AtomicU64> = CacheGuard::new(AtomicU64::new(0));
/// Consulted the cache and had to scan anyway — no entry, a stale generation,
/// a key that did not match the slot, or an entry whose pointers no longer read
/// back. Not counted for queries that opt out of the cache entirely.
static MISSES: CacheGuard<AtomicU64> = CacheGuard::new(AtomicU64::new(0));
/// Complete match sets handed to `store`. A run where this stays far below
/// `misses` is one where the cache cannot warm up: either the queries are all
/// `LIMIT`ed, or writes are retiring entries faster than reads produce them.
static STORES: CacheGuard<AtomicU64> = CacheGuard::new(AtomicU64::new(0));

/// `(hits, misses, stores)` since process start.
pub fn stats() -> (u64, u64, u64) {
    (
        HITS.load(Ordering::Relaxed),
        MISSES.load(Ordering::Relaxed),
        STORES.load(Ordering::Relaxed),
    )
}

/// Records a hit that the caller had to throw away — the entry's pointers no
/// longer read back, so the query rescans. Counted as a miss, because that is
/// what it cost.
pub fn record_abandoned_hit() {
    HITS.fetch_sub(1, Ordering::Relaxed);
    MISSES.fetch_add(1, Ordering::Relaxed);
}

/// Builds the query cache with room for `max_entries` live entries.
///
/// The slot array is sized at twice that. The probe window is 16 slots wide, so
/// a table run near its entry limit would spend most inserts evicting a live
/// neighbour; the headroom keeps the load factor around 50% instead.
pub fn init(max_entries: usize) -> &'static std::sync::Arc<QueryCache> {
    QUERY_CACHE
        .get_or_init(|| QueryCache::new(max_entries.saturating_mul(2).max(1024), max_entries))
}

/// Whether caching is on. Unset means off — a library embedder that never
/// touches these globals gets a working database, not a panic.
#[inline]
pub fn enabled() -> bool {
    ENABLE_CACHE.get().copied().unwrap_or(false) && QUERY_CACHE.get().is_some()
}

/// Slot hash for a `(table, clause)` pair.
///
/// Both halves go through one CRC digest with the table length prefixed, so
/// the pair is distinguished by more than the concatenation of its parts. The
/// previous `crc(table) ^ crc(clause)` was symmetric in its two arguments and
/// collided across any two pairs with a matching XOR.
#[inline]
fn slot_hash(table: &str, where_clause: &str, limit: Option<u64>) -> u64 {
    let mut digest = CRC_64.digest();
    digest.update(&(table.len() as u64).to_le_bytes());
    digest.update(table.as_bytes());
    digest.update(where_clause.as_bytes());
    // A prefix and a complete set are different answers to the same clause, so
    // they must not share a slot.
    match limit {
        None => digest.update(&[0u8]),
        Some(n) => {
            digest.update(&[1u8]);
            digest.update(&n.to_le_bytes());
        }
    }
    digest.finalize()
}

/// Looks up the complete match set for `(table, where_clause)` as of
/// `generation`.
///
/// Returns `None` when caching is off, nothing is stored, the stored entry
/// predates the caller's generation, or the slot turned out to hold a
/// different key.
pub fn lookup(
    table: &str,
    where_clause: &str,
    generation: u64,
    limit: Option<u64>,
) -> Option<QueryCacheValue> {
    if !enabled() {
        return None;
    }

    let probe = |key_limit: Option<u64>| {
        QUERY_CACHE
            .get()
            .and_then(|c| c.get(&slot_hash(table, where_clause, key_limit)))
            .filter(|entry: &QueryCacheValue| {
                entry.generation == generation
                    && &*entry.table == table
                    && &*entry.where_clause == where_clause
                    && entry.limit == key_limit
            })
    };

    // The complete set first: it answers any limit by truncation, and is the
    // better entry whenever both exist. Only then the prefix stored under this
    // caller's exact limit.
    let hit = probe(None).or_else(|| limit.and_then(|_| probe(limit)));

    match &hit {
        Some(_) => HITS.fetch_add(1, Ordering::Relaxed),
        None => MISSES.fetch_add(1, Ordering::Relaxed),
    };

    hit
}

/// Stores a complete match set.
///
/// `generation` must be the value read *before* the scan began: a write that
/// lands mid-scan bumps the counter, and tagging the entry with the older
/// value is what keeps the now-stale pointers from ever being served.
///
/// Uses `force_insert`, so a full cache evicts its oldest entries instead of
/// quietly refusing to admit anything new for the rest of the process.
pub fn store(
    table: &str,
    where_clause: &str,
    generation: u64,
    limit: Option<u64>,
    pointers: Vec<RowPointer>,
) {
    if !enabled() {
        return;
    }

    let Some(cache) = QUERY_CACHE.get() else {
        return;
    };

    let entry = Arc::new(QueryCacheEntry {
        table: table.into(),
        where_clause: where_clause.into(),
        generation,
        limit,
        pointers,
    });

    match cache.force_insert(slot_hash(table, where_clause, limit), entry) {
        Ok((true, _evicted)) => {
            STORES.fetch_add(1, Ordering::Relaxed);
        }
        Ok((false, _)) => {
            log::warn!(
                "Query cache refused an entry for '{}' even after eviction",
                table
            );
        }
        Err(e) => log::error!("Failed to insert entries into the query cache: {}", e),
    }
}

/// Drops every entry. Correctness never depends on this — generations already
/// strand stale entries — but tests and benchmarks need a cold cache on demand.
pub fn clear() {
    if let Some(cache) = QUERY_CACHE.get() {
        let _ = cache.clear();
    }
}
