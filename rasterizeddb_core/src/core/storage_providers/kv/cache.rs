//! The resident base chunks, shared by every file in one database.
//!
//! Without this the driver is unusable rather than merely slow: a scan reads
//! row pointers densely and rows randomly, so every row would otherwise be a
//! round trip to the object store. The cache is what makes the *second* pass
//! over a table cost nothing, and read-ahead is what makes the first one cost
//! one request per few megabytes instead of one per row.
//!
//! Budgeted in bytes and shared across files, because a database's working set
//! is not evenly spread over its tables — a per-file budget would hold cold
//! chunks of an idle table while evicting the hot table's.
//!
//! # Fetches in flight
//!
//! The cache also owns the fetches that will fill it, so a miss on something
//! already being fetched waits for that request instead of issuing its own.
//! Without this, a scan worker missing a chunk the read-ahead is still
//! downloading paid a second GET for it, and N workers missing one chunk paid N.
//! See [`ChunkCache::fetch`].

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use parking_lot::{Mutex, RwLock};

/// Identifies one chunk: which file, and which chunk of it.
///
/// The generation is part of the key so a `swap_temp` cannot serve the old
/// file's bytes for the new file's chunk — the two have different generations
/// and simply miss rather than collide.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ChunkId {
    pub file: u64,
    pub generation: u64,
    pub index: u64,
}

/// One request to the store that is filling the cache: a whole chunk object,
/// or a single page of one as a ranged read.
///
/// `index` is the chunk number for a whole-chunk fetch and the page number for
/// a page fetch; `whole` keeps the two namespaces apart.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct FetchKey {
    pub file: u64,
    pub generation: u64,
    pub index: u64,
    pub whole: bool,
}

/// The pages a fetch produced, as `(page, bytes)` in page order. Empty when
/// the object does not exist — a hole, which a reader fills with zeros.
pub type FetchedPages = Arc<Vec<(u64, Arc<Vec<u8>>)>>;

/// What every waiter on one fetch receives. The error is shared because
/// `io::Error` is not `Clone` and every waiter needs it.
pub type FetchOutcome = Result<FetchedPages, Arc<io::Error>>;

/// A fetch any number of readers can await. Cloned per waiter.
pub type PendingFetch = Shared<BoxFuture<'static, FetchOutcome>>;

struct Entry {
    data: Arc<Vec<u8>>,
    /// CLOCK's reference bit: 1 once hit since the hand last passed. Atomic so
    /// a hit needs only the read lock, and written only when it changes, so a
    /// page every reader is hitting is not a line every reader writes.
    used: AtomicU64,
    /// Where this entry sits in `Inner::ring`.
    slot: usize,
}

struct Inner {
    entries: HashMap<ChunkId, Entry>,
    /// Every resident chunk once, in no particular order: the CLOCK face.
    ring: Vec<ChunkId>,
    /// The CLOCK hand, an index into `ring`.
    hand: usize,
    bytes: u64,
    /// Per file, how many times its cached bytes have been declared stale.
    ///
    /// A fetch notes this before its request goes out and inserts what it got
    /// only if the count has not moved. Otherwise a request that left before a
    /// compaction rewrote a chunk, and landed after the compaction invalidated
    /// it, would put the *old* bytes back — and with the overlay that covered
    /// them already folded away, every later read of those pages would return
    /// them. Per file, so a busy table compacting does not stop another
    /// table's fetches from being kept.
    epochs: HashMap<u64, u64>,
}

struct InFlight {
    /// Distinguishes this fetch from a later one for the same key, so a fetch
    /// that finishes after an invalidation replaced it removes only itself.
    token: u64,
    fetch: PendingFetch,
}

pub struct ChunkCache {
    inner: RwLock<Inner>,
    /// Lock order: `in_flight` before `inner`, never the reverse.
    in_flight: Mutex<HashMap<FetchKey, InFlight>>,
    next_token: AtomicU64,
    budget: u64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl ChunkCache {
    pub fn new(budget_bytes: u64) -> Self {
        Self {
            inner: RwLock::new(Inner {
                entries: HashMap::new(),
                ring: Vec::new(),
                hand: 0,
                bytes: 0,
                epochs: HashMap::new(),
            }),
            in_flight: Mutex::new(HashMap::new()),
            next_token: AtomicU64::new(0),
            budget: budget_bytes.max(1),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, id: ChunkId) -> Option<Arc<Vec<u8>>> {
        let mut found = None;
        self.get_each([id], |_, data| found = data);
        found
    }

    /// Looks up several chunks under one read of the lock, handing `each` what
    /// is resident and `None` for what is not. Counted as one hit or miss per
    /// chunk, the same as calling [`ChunkCache::get`] for each.
    ///
    /// The batch read path's lookups go through here: one lock acquisition and
    /// one counter update per batch rather than per page, because the lock word
    /// and the counters are shared by every reader of the database and eight
    /// scan workers were spending most of a resident read bouncing them.
    ///
    /// `each` runs under the lock and must not call back into the cache.
    pub fn get_each(
        &self,
        ids: impl IntoIterator<Item = ChunkId>,
        mut each: impl FnMut(ChunkId, Option<Arc<Vec<u8>>>),
    ) {
        let (mut hits, mut misses) = (0u64, 0u64);
        {
            let inner = self.inner.read();
            for id in ids {
                match inner.entries.get(&id) {
                    Some(entry) => {
                        entry.mark_used();
                        hits += 1;
                        each(id, Some(entry.data.clone()));
                    }
                    None => {
                        misses += 1;
                        each(id, None);
                    }
                }
            }
        }
        if hits > 0 {
            self.hits.fetch_add(hits, Ordering::Relaxed);
        }
        if misses > 0 {
            self.misses.fetch_add(misses, Ordering::Relaxed);
        }
    }

    /// Runs `f` over the bytes of every chunk in `ids`, in order, all under one
    /// read of the lock — or, if any of them is not resident, returns `None`
    /// without running it and without counting a miss, as
    /// [`ChunkCache::contains`] does not.
    ///
    /// For the synchronous read path, which has to know everything is here
    /// before it copies anything. Borrowing under the lock rather than cloning
    /// each page's `Arc` keeps a hot page from being a reference count every
    /// worker writes. A hit is counted per chunk in `ids`.
    ///
    /// `f` runs under the lock and must not call back into the cache.
    pub fn read_resident<R>(&self, ids: &[ChunkId], f: impl FnOnce(&[&[u8]]) -> R) -> Option<R> {
        let inner = self.inner.read();
        let mut pages: smallvec::SmallVec<[&[u8]; 8]> = smallvec::SmallVec::with_capacity(ids.len());
        for id in ids {
            let entry = inner.entries.get(id)?;
            entry.mark_used();
            pages.push(entry.data.as_slice());
        }
        let result = f(&pages);
        drop(pages);
        drop(inner);
        if !ids.is_empty() {
            self.hits.fetch_add(ids.len() as u64, Ordering::Relaxed);
        }
        Some(result)
    }

    /// Whether the chunk is resident, without counting a hit or a miss.
    ///
    /// The synchronous read fast path asks this to decide whether it can serve a
    /// batch without blocking; recording that as a miss would make the counters
    /// describe probes rather than reads.
    pub fn contains(&self, id: ChunkId) -> bool {
        self.inner.read().entries.contains_key(&id)
    }

    pub fn insert(&self, id: ChunkId, data: Arc<Vec<u8>>) {
        let mut inner = self.inner.write();
        Self::insert_locked(&mut inner, self.budget, id, data);
    }

    /// Inserts, then evicts by CLOCK until the budget holds again.
    ///
    /// This used to scan every entry for the least recently used one, per
    /// eviction, under the write lock that every reader waits on — 4,096
    /// entries at the default geometry, not the "few hundred" its comment
    /// assumed, about 4 µs per insert into a full cache. CLOCK approximates
    /// LRU in amortised O(1): the hand clears reference bits until it finds an
    /// entry not hit since its last pass, and each entry is passed at most
    /// twice per eviction. The byte budget is still exact: `bytes` never stays
    /// above it once this returns.
    fn insert_locked(inner: &mut Inner, budget: u64, id: ChunkId, data: Arc<Vec<u8>>) {
        let size = data.len() as u64;
        if size > budget {
            // A chunk larger than the whole budget would evict everything and
            // then itself. Serve it and forget it.
            return;
        }
        if let Some(entry) = inner.entries.get_mut(&id) {
            inner.bytes -= entry.data.len() as u64;
            entry.data = data;
            entry.used.store(1, Ordering::Relaxed);
        } else {
            let slot = inner.ring.len();
            inner.ring.push(id);
            inner.entries.insert(
                id,
                Entry {
                    data,
                    // Starts referenced: what was just fetched gets one pass
                    // of the hand before it can go.
                    used: AtomicU64::new(1),
                    slot,
                },
            );
        }
        inner.bytes += size;
        while inner.bytes > budget && !inner.ring.is_empty() {
            if inner.hand >= inner.ring.len() {
                inner.hand = 0;
            }
            let candidate = inner.ring[inner.hand];
            let entry = inner.entries.get(&candidate).expect("ring and map agree");
            if entry.used.load(Ordering::Relaxed) != 0 {
                entry.used.store(0, Ordering::Relaxed);
                inner.hand += 1;
            } else {
                // The entry swapped into this slot is looked at next.
                Self::remove_locked(inner, candidate);
            }
        }
    }

    /// Removes one entry from the map and the ring, keeping both in step.
    fn remove_locked(inner: &mut Inner, id: ChunkId) {
        let Some(entry) = inner.entries.remove(&id) else {
            return;
        };
        inner.bytes -= entry.data.len() as u64;
        inner.ring.swap_remove(entry.slot);
        if let Some(&moved) = inner.ring.get(entry.slot) {
            inner.entries.get_mut(&moved).expect("ring and map agree").slot = entry.slot;
        }
    }

    /// The fetch for `key` already in flight, if any.
    pub fn in_flight(&self, key: FetchKey) -> Option<PendingFetch> {
        self.in_flight.lock().get(&key).map(|f| f.fetch.clone())
    }

    /// Joins the fetch for `key` if one is in flight, or starts `request`.
    ///
    /// `request` is the store round trip plus splitting its answer into pages;
    /// it is not polled until the fetch runs. A started fetch runs as its own
    /// task, so:
    ///
    /// - every waiter gets the same pages, and the store is asked once;
    /// - a waiter that is cancelled — a statement timing out, a client going
    ///   away — strands nobody, because none of them is driving the request;
    /// - the entry is removed when the request ends, success or failure, so a
    ///   failed fetch is retried by the next reader rather than remembered.
    ///
    /// What it fetched goes into the cache before the entry is removed, so a
    /// reader that finds neither has a genuine miss.
    pub fn fetch<F>(self: &Arc<Self>, key: FetchKey, request: F) -> PendingFetch
    where
        F: Future<Output = io::Result<Vec<(u64, Arc<Vec<u8>>)>>> + Send + 'static,
    {
        let mut in_flight = self.in_flight.lock();
        if let Some(existing) = in_flight.get(&key) {
            return existing.fetch.clone();
        }
        // Noted before the request can leave: see `Inner::epochs`. Taken under
        // `in_flight`, which `invalidate*` also holds while bumping it, so a
        // fetch is either registered before an invalidation (and dropped by it)
        // or sees the bumped count.
        let epoch = self.inner.read().epochs.get(&key.file).copied().unwrap_or(0);
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        let cache = self.clone();
        let work = async move {
            // Removes the entry however the fetch ends, unwinding included.
            let _registered = Registered {
                cache: cache.clone(),
                key,
                token,
            };
            let pages = request.await?;
            cache.insert_fetched(key, epoch, &pages);
            Ok::<_, io::Error>(Arc::new(pages))
        };
        let fetch: PendingFetch = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                let task = runtime.spawn(work);
                async move {
                    match task.await {
                        Ok(Ok(pages)) => Ok(pages),
                        Ok(Err(e)) => Err(Arc::new(e)),
                        Err(join) => Err(Arc::new(io::Error::other(format!(
                            "kv fetch task failed: {join}"
                        )))),
                    }
                }
                .boxed()
                .shared()
            }
            // No runtime to run it on — `tokio::spawn` would panic here. The
            // waiters drive it themselves instead, which still makes it one
            // request; only the cancellation guarantee above is weaker, since
            // a waiter that goes away stops polling it until another arrives.
            Err(_) => async move { work.await.map_err(Arc::new) }.boxed().shared(),
        };
        in_flight.insert(
            key,
            InFlight {
                token,
                fetch: fetch.clone(),
            },
        );
        fetch
    }

    /// Inserts a completed fetch's pages, unless the file was invalidated since
    /// the fetch started.
    fn insert_fetched(&self, key: FetchKey, epoch: u64, pages: &[(u64, Arc<Vec<u8>>)]) {
        let mut inner = self.inner.write();
        if inner.epochs.get(&key.file).copied().unwrap_or(0) != epoch {
            return;
        }
        for (page, data) in pages {
            Self::insert_locked(
                &mut inner,
                self.budget,
                ChunkId {
                    file: key.file,
                    generation: key.generation,
                    index: *page,
                },
                data.clone(),
            );
        }
    }

    fn retire(&self, key: FetchKey, token: u64) {
        let mut in_flight = self.in_flight.lock();
        if in_flight.get(&key).is_some_and(|f| f.token == token) {
            in_flight.remove(&key);
        }
    }

    /// Declares `file`'s cached bytes stale: bumps its epoch and forgets its
    /// fetches in flight, so nothing that left before this point is cached or
    /// joined afterwards. Called with both locks held.
    fn stale_locked(
        in_flight: &mut HashMap<FetchKey, InFlight>,
        inner: &mut Inner,
        file: u64,
    ) {
        *inner.epochs.entry(file).or_insert(0) += 1;
        in_flight.retain(|k, _| k.file != file);
    }

    /// Drops every chunk of one file generation. Called after compaction
    /// publishes a new generation, and when a file is dropped.
    pub fn invalidate_file(&self, file: u64) {
        let mut in_flight = self.in_flight.lock();
        let mut inner = self.inner.write();
        Self::stale_locked(&mut in_flight, &mut inner, file);
        let doomed: Vec<ChunkId> = inner
            .entries
            .keys()
            .filter(|k| k.file == file)
            .copied()
            .collect();
        for id in doomed {
            Self::remove_locked(&mut inner, id);
        }
    }

    pub fn invalidate(&self, id: ChunkId) {
        let mut in_flight = self.in_flight.lock();
        let mut inner = self.inner.write();
        Self::stale_locked(&mut in_flight, &mut inner, id.file);
        Self::remove_locked(&mut inner, id);
    }

    /// (hits, misses, resident bytes).
    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.inner.read().bytes,
        )
    }

    /// Fetches currently in flight. For tests.
    #[cfg(test)]
    pub fn fetches_in_flight(&self) -> usize {
        self.in_flight.lock().len()
    }
}

impl Entry {
    #[inline]
    fn mark_used(&self) {
        if self.used.load(Ordering::Relaxed) == 0 {
            self.used.store(1, Ordering::Relaxed);
        }
    }
}

/// Removes a fetch's in-flight entry when its task ends, by any route.
struct Registered {
    cache: Arc<ChunkCache>,
    key: FetchKey,
    token: u64,
}

impl Drop for Registered {
    fn drop(&mut self) {
        self.cache.retire(self.key, self.token);
    }
}

#[cfg(test)]
mod eviction_tests {
    use super::*;

    fn id(i: u64) -> ChunkId {
        ChunkId { file: i % 7, generation: 1, index: i }
    }

    /// Map, ring and byte count must agree after any mix of inserts,
    /// replacements, invalidations and hits, and the budget must hold.
    #[test]
    fn eviction_keeps_the_books_and_the_budget() {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        let budget = 10_000u64;
        let cache = ChunkCache::new(budget);
        for _ in 0..50_000 {
            match next(10) {
                0 => cache.invalidate(id(next(200))),
                1 => cache.invalidate_file(next(7)),
                2..=4 => {
                    let _ = cache.get(id(next(200)));
                }
                _ => cache.insert(id(next(200)), Arc::new(vec![0u8; 1 + next(900) as usize])),
            }
            let inner = cache.inner.read();
            assert!(inner.bytes <= budget, "over budget: {}", inner.bytes);
            assert_eq!(inner.ring.len(), inner.entries.len());
            let mut sum = 0;
            for (slot, k) in inner.ring.iter().enumerate() {
                let e = &inner.entries[k];
                assert_eq!(e.slot, slot);
                sum += e.data.len() as u64;
            }
            assert_eq!(sum, inner.bytes);
        }
    }

    /// A page being read keeps its place while cold pages stream through.
    #[test]
    fn a_hot_page_survives_a_scan() {
        let cache = ChunkCache::new(100 * 10);
        for i in 0..10 {
            cache.insert(id(i), Arc::new(vec![0u8; 100]));
        }
        for i in 100..1_000 {
            assert!(cache.get(id(3)).is_some(), "hot page evicted at {i}");
            cache.insert(id(i), Arc::new(vec![0u8; 100]));
        }
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// kv_cache_insert_bench --nocapture`. Inserts into a full cache at the
    /// default geometry (256 MiB of 64 KiB pages = 4,096 entries), with a
    /// read of a few resident pages between inserts.
    #[test]
    #[ignore]
    fn kv_cache_insert_bench() {
        const PAGE: usize = 64 * 1024;
        let cache = ChunkCache::new(256 * 1024 * 1024);
        let page = Arc::new(vec![0u8; PAGE]);
        for i in 0..4096 {
            cache.insert(id(i), page.clone());
        }
        for round in 0..3 {
            let n = 20_000u64;
            let base = 10_000 + round * n;
            let t = std::time::Instant::now();
            for i in base..base + n {
                cache.insert(id(i), page.clone());
                let _ = cache.get(id(i - 3));
            }
            println!(
                "round {round}: {:.2} us per insert into a full cache",
                t.elapsed().as_secs_f64() * 1e6 / n as f64
            );
        }
    }
}
