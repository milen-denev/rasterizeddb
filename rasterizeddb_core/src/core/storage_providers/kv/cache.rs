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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

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

struct Entry {
    data: Arc<Vec<u8>>,
    /// Tick of the last hit, for eviction.
    used: u64,
}

struct Inner {
    entries: HashMap<ChunkId, Entry>,
    bytes: u64,
}

pub struct ChunkCache {
    inner: Mutex<Inner>,
    budget: u64,
    clock: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl ChunkCache {
    pub fn new(budget_bytes: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                bytes: 0,
            }),
            budget: budget_bytes.max(1),
            clock: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, id: ChunkId) -> Option<Arc<Vec<u8>>> {
        let tick = self.clock.fetch_add(1, Ordering::Relaxed);
        let mut inner = self.inner.lock();
        match inner.entries.get_mut(&id) {
            Some(entry) => {
                entry.used = tick;
                let data = entry.data.clone();
                drop(inner);
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(data)
            }
            None => {
                drop(inner);
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Whether the chunk is resident, without counting a hit or a miss.
    ///
    /// The synchronous read fast path asks this to decide whether it can serve a
    /// batch without blocking; recording that as a miss would make the counters
    /// describe probes rather than reads.
    pub fn contains(&self, id: ChunkId) -> bool {
        self.inner.lock().entries.contains_key(&id)
    }

    pub fn insert(&self, id: ChunkId, data: Arc<Vec<u8>>) {
        let tick = self.clock.fetch_add(1, Ordering::Relaxed);
        let size = data.len() as u64;
        if size > self.budget {
            // A chunk larger than the whole budget would evict everything and
            // then itself. Serve it and forget it.
            return;
        }
        let mut inner = self.inner.lock();
        if let Some(old) = inner.entries.insert(id, Entry { data, used: tick }) {
            inner.bytes -= old.data.len() as u64;
        }
        inner.bytes += size;
        while inner.bytes > self.budget {
            // Least-recently-used by scan. The table is budget/chunk_size
            // entries — a few hundred — and eviction only runs when the cache is
            // full, so a scan costs less than the bookkeeping an intrusive list
            // would add to every hit.
            let victim = inner
                .entries
                .iter()
                .min_by_key(|(_, e)| e.used)
                .map(|(&k, _)| k);
            let Some(victim) = victim else { break };
            if let Some(entry) = inner.entries.remove(&victim) {
                inner.bytes -= entry.data.len() as u64;
            }
        }
    }

    /// Drops every chunk of one file generation. Called after compaction
    /// publishes a new generation, and when a file is dropped.
    pub fn invalidate_file(&self, file: u64) {
        let mut inner = self.inner.lock();
        let doomed: Vec<ChunkId> = inner
            .entries
            .keys()
            .filter(|k| k.file == file)
            .copied()
            .collect();
        for id in doomed {
            if let Some(entry) = inner.entries.remove(&id) {
                inner.bytes -= entry.data.len() as u64;
            }
        }
    }

    pub fn invalidate(&self, id: ChunkId) {
        let mut inner = self.inner.lock();
        if let Some(entry) = inner.entries.remove(&id) {
            inner.bytes -= entry.data.len() as u64;
        }
    }

    /// (hits, misses, resident bytes).
    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.inner.lock().bytes,
        )
    }
}
