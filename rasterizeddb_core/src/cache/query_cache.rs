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

use crc::{CRC_64_ECMA_182, Crc, Table};
use rclite::Arc;

use crate::cache::atomic_cache::AtomicGenericCache;
use crate::core::row::row_pointer::RowPointer;

/// Slice-by-16 rather than `crc`'s default byte-at-a-time table: the same
/// values (pinned by `slot_hash_matches_the_byte_table`), several times the
/// throughput on clauses longer than a few words. In-memory only, so the
/// implementation can change freely; the value never did.
const CRC_64: Crc<u64, Table<16>> = Crc::<u64, Table<16>>::new(&CRC_64_ECMA_182);

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
    /// Whether this entry's bytes were added to the cache's total.
    counted: bool,
    /// Every pointer that matched, or the first `limit` of them.
    ///
    /// A boxed slice rather than the scan's `Vec`: the accumulating vector has
    /// up to half its capacity spare, which an entry kept for the life of the
    /// process would hold for nothing.
    pub pointers: Box<[RowPointer]>,
}

impl QueryCacheEntry {
    /// Heap bytes this entry holds, as counted against [`byte_budget`].
    pub fn heap_bytes(&self) -> u64 {
        (self.pointers.len() * std::mem::size_of::<RowPointer>()
            + self.table.len()
            + self.where_clause.len()
            + std::mem::size_of::<Self>()) as u64
    }
}

impl Drop for QueryCacheEntry {
    // Counted down when the last reference goes — eviction, replacement,
    // `clear`, or a reader that held a hit past its eviction — so the total is
    // what is really resident, whichever path let go of it. Only entries that
    // were counted in (`store`) are counted out.
    fn drop(&mut self) {
        if self.counted {
            CACHED_BYTES.fetch_sub(self.heap_bytes(), Ordering::Relaxed);
        }
    }
}

pub type QueryCacheValue = std::sync::Arc<QueryCacheEntry>;

/// What a cache slot holds: the entry behind a swappable pointer.
///
/// The slot array never drops a value it removes — `remove`, `remove_oldest`,
/// `clear` and a stranded generation all leave the old value in the slot until
/// the slot is reused — so an entry held directly kept its whole pointer set
/// resident after eviction, and no byte budget could be enforced. Emptying the
/// holder frees the entry as soon as no reader holds it, whatever the slot
/// array does with the holder.
#[derive(Default)]
pub struct CacheSlot {
    entry: arc_swap::ArcSwapOption<QueryCacheEntry>,
}

impl CacheSlot {
    /// Lets go of the entry; a reader already holding it keeps its copy.
    fn release(&self) {
        self.entry.store(None);
    }
}

pub type QueryCache = AtomicGenericCache<u64, Arc<CacheSlot>>;

pub static QUERY_CACHE: OnceLock<std::sync::Arc<QueryCache>> = OnceLock::new();
pub static ENABLE_CACHE: OnceLock<bool> = OnceLock::new();

/// Heap bytes held by live entries.
static CACHED_BYTES: AtomicU64 = AtomicU64::new(0);

/// The byte budget, when one was set; [`DEFAULT_BYTE_BUDGET`] otherwise.
static BYTE_BUDGET: AtomicU64 = AtomicU64::new(0);

/// 256 MiB. The entry-count bound alone (100 000 by default) let a cache of
/// large match sets grow without limit: a pointer is 48 bytes, so one cached
/// 5M-row match is 240 MB, and a handful of them was gigabytes.
pub const DEFAULT_BYTE_BUDGET: u64 = 256 * 1024 * 1024;

/// An entry larger than this share of the budget is not cached at all. Its
/// rescan is long anyway, and admitting it would evict most of the cache to
/// keep one answer.
const MAX_ENTRY_SHARE: u64 = 8;

/// Sets the cache's byte budget. `0` restores the default.
pub fn set_byte_budget(bytes: u64) {
    BYTE_BUDGET.store(bytes, Ordering::Relaxed);
}

/// The byte budget in force.
pub fn byte_budget() -> u64 {
    match BYTE_BUDGET.load(Ordering::Relaxed) {
        0 => DEFAULT_BYTE_BUDGET,
        b => b,
    }
}

/// Heap bytes held by live entries now.
pub fn cached_bytes() -> u64 {
    CACHED_BYTES.load(Ordering::Relaxed)
}

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
        let slot = QUERY_CACHE
            .get()
            .and_then(|c| c.get(&slot_hash(table, where_clause, key_limit)))?;
        let entry = slot.entry.load_full()?;
        let same_key = &*entry.table == table
            && &*entry.where_clause == where_clause
            && entry.limit == key_limit;
        if same_key && entry.generation < generation {
            // Stranded by a write: generations only grow, so nothing can be
            // answered from it again. Freed now rather than when the slot is
            // reused, which may be never.
            slot.release();
            return None;
        }
        (same_key && entry.generation == generation).then_some(entry)
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
    store_into(cache, table, where_clause, generation, limit, pointers, byte_budget());
}

/// [`store`] into a given cache under a given budget.
fn store_into(
    cache: &QueryCache,
    table: &str,
    where_clause: &str,
    generation: u64,
    limit: Option<u64>,
    pointers: Vec<RowPointer>,
    budget: u64,
) {
    let mut entry = QueryCacheEntry {
        table: table.into(),
        where_clause: where_clause.into(),
        generation,
        limit,
        counted: false,
        pointers: pointers.into_boxed_slice(),
    };
    let bytes = entry.heap_bytes();
    if bytes > budget / MAX_ENTRY_SHARE {
        return;
    }
    CACHED_BYTES.fetch_add(bytes, Ordering::Relaxed);
    entry.counted = true;
    let slot = Arc::new(CacheSlot {
        entry: arc_swap::ArcSwapOption::new(Some(std::sync::Arc::new(entry))),
    });

    match cache.force_insert(slot_hash(table, where_clause, limit), slot.clone()) {
        Ok((true, evicted)) => {
            STORES.fetch_add(1, Ordering::Relaxed);
            for (_, old, _, _) in evicted {
                old.release();
            }
        }
        Ok((false, _)) => {
            slot.release();
            log::warn!(
                "Query cache refused an entry for '{}' even after eviction",
                table
            );
        }
        Err(e) => log::error!("Failed to insert entries into the query cache: {}", e),
    }

    // Over budget: drop the oldest entries until under it. Bounded by the
    // entry count, so a cache whose total cannot fall (every entry still held
    // by a reader) stops rather than spins.
    let mut rounds = 0;
    while CACHED_BYTES.load(Ordering::Relaxed) > budget && rounds < 64 {
        let live = cache.len();
        if live == 0 {
            break;
        }
        let removed = cache.remove_oldest((live / 16).max(1));
        if removed.is_empty() {
            break;
        }
        for (_, old, _, _) in removed {
            old.release();
        }
        rounds += 1;
    }
}

/// Drops every entry. Correctness never depends on this — generations already
/// strand stale entries — but tests and benchmarks need a cold cache on demand.
pub fn clear() {
    if let Some(cache) = QUERY_CACHE.get() {
        clear_cache(cache);
    }
}

/// Empties `cache`, releasing every entry rather than leaving each resident in
/// its slot.
pub fn clear_cache(cache: &QueryCache) {
    for (_, slot, _, _) in cache.drain() {
        slot.release();
    }
}

#[cfg(test)]
mod budget_tests {
    use super::*;

    fn pointers(n: usize) -> Vec<RowPointer> {
        let mut v = Vec::with_capacity(n * 2);
        let blank = [0u8; crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN];
        v.resize_with(n, || RowPointer::from_slice(&blank));
        v
    }

    /// The only test in the library that stores entries, so the process-wide
    /// byte counter is this test's alone (library tests run with the global
    /// cache off).
    #[test]
    fn the_byte_budget_bounds_what_is_cached() {
        let per = std::mem::size_of::<RowPointer>() as u64;
        let cache = QueryCache::new(2048, 1024);
        let base = cached_bytes();
        // Budget: room for about four 10 000-pointer entries.
        let budget = 4 * 10_000 * per + 4096;

        // An entry over 1/8 of the budget is refused outright.
        store_into(&cache, "t", "a = 1", 1, None, pointers(10_000), budget);
        assert_eq!(cache.len(), 0, "an entry above budget/8 is not cached");
        assert_eq!(cached_bytes(), base);

        // Entries within the share are admitted, and the total never stays
        // above the budget: the oldest go first.
        for i in 0..40 {
            store_into(&cache, "t", &format!("b = {}", i), 1, None, pointers(4_000), budget);
            assert!(cached_bytes() - base <= budget, "over budget after {} stores", i);
        }
        assert!(cache.len() > 0 && cache.len() < 40, "len {}", cache.len());
        // The newest survived.
        let newest = cache
            .get(&slot_hash("t", "b = 39", None))
            .and_then(|slot| slot.entry.load_full())
            .expect("newest kept");
        // Spare capacity is not kept: the scan's Vec had room for 8 000.
        assert_eq!(newest.pointers.len(), 4_000);
        assert_eq!(newest.heap_bytes(), 4_000 * per + 1 + 6 + std::mem::size_of::<QueryCacheEntry>() as u64);
        drop(newest);

        // Every path out counts the bytes back down — and frees them, which the
        // slot array's own `clear` alone did not.
        clear_cache(&cache);
        assert_eq!(cached_bytes(), base);
        assert_eq!(cache.len(), 0);
    }
}

#[cfg(test)]
mod slot_hash_tests {
    use super::*;

    /// The byte-at-a-time table this used, as the reference.
    const BYTE_TABLE: Crc<u64> = Crc::<u64>::new(&CRC_64_ECMA_182);

    #[test]
    fn slot_hash_matches_the_byte_table() {
        let mut rng = 0x2545_F491_4F6C_DD1Du64;
        for len in 0..=1024usize {
            let data: Vec<u8> = (0..len)
                .map(|_| {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    rng as u8
                })
                .collect();
            assert_eq!(CRC_64.checksum(&data), BYTE_TABLE.checksum(&data), "len {}", len);
        }
        assert_eq!(CRC_64.checksum(b"123456789"), 0x6C40_DF5F_0B49_7347);
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored slot_hash_throughput --nocapture`
    #[test]
    #[ignore]
    fn slot_hash_throughput() {
        for size in [24usize, 64, 256] {
            let data: Vec<u8> = (0..size).map(|i| (i * 13 + 1) as u8).collect();
            let rounds = 20_000_000 / size.max(1) * 8;
            for round in 0..3 {
                let mut sink = 0u64;
                let t = std::time::Instant::now();
                for _ in 0..rounds {
                    sink ^= BYTE_TABLE.checksum(std::hint::black_box(&data));
                }
                let old = t.elapsed();
                let t = std::time::Instant::now();
                for _ in 0..rounds {
                    sink ^= CRC_64.checksum(std::hint::black_box(&data));
                }
                let new = t.elapsed();
                println!(
                    "size {:>4} round {}: table1 {:>6.1} ns/hash  table16 {:>6.1} ns/hash ({})",
                    size,
                    round,
                    old.as_nanos() as f64 / rounds as f64,
                    new.as_nanos() as f64 / rounds as f64,
                    sink
                );
            }
        }
    }
}
