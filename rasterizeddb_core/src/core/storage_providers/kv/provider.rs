//! A [`StorageIO`] over an object store.
//!
//! # The shape of the thing
//!
//! Three layers answer every read, newest first:
//!
//! ```text
//!   live overlay    writes since the last compaction started
//!   frozen overlay  writes the compaction in flight is folding
//!   base chunks     objects in the store, `chunk_size` each
//! ```
//!
//! and one path answers every write: append the change to the *live* overlay,
//! and on commit put a single object describing it. The base chunks are never
//! touched on the commit path. That is the whole design — everything else here
//! is bookkeeping to make it crash-safe and to keep the overlay from growing
//! without bound.
//!
//! # Why it is shaped for S3 specifically
//!
//! An object store charges for requests and answers each one after a latency
//! floor that has nothing to do with size. So:
//!
//! - **One PUT per commit**, whatever the commit touched. Row bytes and pointer
//!   bytes in the same transaction are one object each — one per file — not one
//!   per changed chunk and not one per row.
//! - **Nothing is read to be written.** A read-modify-write of a 4 MiB chunk to
//!   flip a row's status byte would be the obvious implementation and it would
//!   be unusable; the delta carries the byte.
//! - **Objects are big, reads are small.** A chunk is an object; a *page* is the
//!   unit of caching and of ranged reads. A point read fetches one page, a scan
//!   fetches whole chunks, and the read path picks between them by counting how
//!   much of a chunk a batch actually wants.
//! - **Everything fans out.** A vectored read issues its page and chunk fetches
//!   concurrently, because one connection to an object store is latency-bound
//!   long before it is bandwidth-bound.
//!
//! # Durability
//!
//! [`StorageIO::commit_durable`] returns only after the delta object is stored,
//! so a committed transaction survives the process. Writes made outside a
//! transaction — schema headers, the id sequence — are flushed by the background
//! service within `flush_interval`, which is the same bargain the file backend
//! makes with the page cache, and the same window.
//!
//! Uncommitted records may be written out early to bound memory during a long
//! statement. Nothing applies them: a batch names the transactions it declares
//! committed, and recovery keeps a record only if some batch named its
//! transaction. This is the write-ahead log's contract expressed in objects.
//!
//! # What this does not do
//!
//! One writer per database. The append allocator is an in-process atomic and an
//! object store offers no compare-and-swap to replace it with, so two servers
//! pointed at one prefix will interleave offsets and corrupt each other. There
//! is no lease here yet; the deployment has to guarantee it.

use std::collections::HashMap;
use std::io::{self, ErrorKind, SeekFrom};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arc_swap::ArcSwap;
use log::{error, warn};
use parking_lot::Mutex;

use super::super::traits::{BorrowedBytes, IOResult, StorageIO};
use super::cache::{ChunkCache, ChunkId};
use super::layout::{self, DeltaBatch, Meta, Record, RecordKind};
use super::overlay::Overlay;
use super::store::KvStore;

/// How the provider trades requests, bandwidth and memory against each other.
#[derive(Clone, Debug)]
pub struct KvConfig {
    /// Key prefix every object of this database lives under.
    pub root: String,
    /// Bytes per base-chunk object. Bigger means fewer requests for a scan and
    /// more bytes rewritten when a chunk is finally folded.
    pub chunk_size: u64,
    /// Cache and ranged-read granularity. Must divide `chunk_size`. A point read
    /// costs one page of bandwidth; below about 32 KiB the saving is invisible
    /// next to the round trip.
    pub page_size: u64,
    /// Resident base data across the whole database.
    pub cache_bytes: u64,
    /// Pages of one chunk a batch must want before the whole chunk object is
    /// fetched in a single request instead of one request per page.
    pub chunk_fetch_pages: u64,
    /// Chunks to fetch ahead once reads look sequential.
    pub readahead_chunks: u64,
    /// Delta objects tolerated before compaction is forced.
    pub max_deltas: usize,
    /// Overlay bytes inside one chunk that make folding it worthwhile.
    pub compact_chunk_bytes: u64,
    /// How often the background service flushes non-transactional writes and
    /// considers compacting.
    pub flush_interval_ms: u64,
    /// Unwritten record bytes held before they are spilled to a delta object.
    /// Bounds the copy a long statement keeps; the overlay still holds the data.
    pub spill_bytes: u64,
}

impl KvConfig {
    pub fn new(root: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            chunk_size: 4 * 1024 * 1024,
            page_size: 64 * 1024,
            cache_bytes: 256 * 1024 * 1024,
            chunk_fetch_pages: 4,
            readahead_chunks: 2,
            max_deltas: 64,
            compact_chunk_bytes: 1024 * 1024,
            flush_interval_ms: 200,
            spill_bytes: 8 * 1024 * 1024,
        }
    }

    fn validate(&self) -> io::Result<()> {
        if self.page_size == 0 || self.chunk_size == 0 {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "kv sizes must be non-zero",
            ));
        }
        if self.chunk_size % self.page_size != 0 {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "kv chunk_size {} is not a multiple of page_size {}",
                    self.chunk_size, self.page_size
                ),
            ));
        }
        if self.root.is_empty() || self.root.ends_with('/') {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "kv root must be a non-empty prefix with no trailing slash",
            ));
        }
        Ok(())
    }

    #[inline]
    fn pages_per_chunk(&self) -> u64 {
        self.chunk_size / self.page_size
    }
}

/// What the startup scan found for one file, waiting for it to be opened.
struct FileRecovery {
    meta: Meta,
    /// Delta batches in sequence order, already filtered for the ones a
    /// compaction retired.
    batches: Vec<DeltaBatch>,
}

/// Everything one database's files share: the store, the page budget, the
/// registry that makes two handles to the same name the same bytes — and the
/// transactions, which is the part that matters most.
///
/// # Why a commit is a family operation
///
/// The engine begins a transaction on one file and commits it on another. An
/// INSERT writes row bytes through `rows`, the pointer through `pointers`, and
/// calls `commit_durable` on `pointers` alone; the file backend gets away with
/// that because both files share one write-ahead log. Committing per file here
/// would make the pointer durable and the row bytes not — a pointer naming
/// bytes that were never stored, which is the exact failure the engine's own
/// comment at that call site says the transaction exists to prevent.
///
/// So a commit flushes every file holding records for the transaction, and only
/// the last object written declares it committed. Recovery unions those
/// declarations across the whole database before applying anything, so the
/// transaction is either everywhere or nowhere: if the final put fails, no
/// object names the transaction and every file discards its half.
pub struct KvFamily {
    store: Arc<dyn KvStore>,
    config: Arc<KvConfig>,
    cache: Arc<ChunkCache>,
    files: dashmap::DashMap<String, Arc<KvFile>>,
    next_file_id: AtomicU64,
    next_txn: AtomicU64,
    /// Drained as files are opened.
    recovered: Mutex<HashMap<String, FileRecovery>>,
    /// Transactions some batch declared committed, across every file.
    committed: Arc<std::collections::HashSet<u64>>,
    /// Names whose objects a dropped file is still sweeping.
    ///
    /// `drop_io` is a synchronous trait method and deleting is a round trip, so
    /// the erase is spawned. Its comment says reopening the name meanwhile
    /// "gets a fresh file, which is what unlinking a path does" — but unlinking
    /// is synchronous and this is not, and `erase` deletes the file's whole key
    /// prefix, which is the *name*. So a name reopened inside that window had
    /// the new file's objects swept by the old file's task: bytes that read
    /// back correctly from the overlay for the life of the process and are
    /// simply absent after a restart.
    ///
    /// Reached by anything that drops a name and recreates it — `DROP TABLE`
    /// then `CREATE TABLE` of the same name, and `STORAGE MOVE TABLE` moving a
    /// table off a store and back. [`KvFamily::open_file`] waits on this, which
    /// is what makes the reopen behave the way the comment claimed.
    erasing: dashmap::DashMap<String, Arc<tokio::sync::Semaphore>>,
}

impl KvFamily {
    /// Opens a database in `store`, reading back whatever is already there.
    ///
    /// One listing and one get per delta object, for the whole database, before
    /// any file is opened — a per-file scan could not see the commit
    /// declarations that live in another file's deltas.
    pub async fn open(store: Arc<dyn KvStore>, config: KvConfig) -> io::Result<Arc<Self>> {
        config.validate()?;
        let prefix = format!("{}/", config.root);
        let listed = store.list(&prefix).await?;

        let mut meta_keys: Vec<(String, String)> = Vec::new();
        let mut delta_keys: Vec<(String, u64, String)> = Vec::new();
        for object in listed {
            let Some(rest) = object.key.strip_prefix(&prefix) else {
                continue;
            };
            let Some((name, tail)) = rest.split_once('/') else {
                continue;
            };
            if tail == "META" {
                meta_keys.push((name.to_string(), object.key));
            } else if let Some(seq_text) = tail.strip_prefix("d/")
                && let Ok(seq) = u64::from_str_radix(seq_text, 16)
            {
                delta_keys.push((name.to_string(), seq, object.key));
            }
        }

        let width = store.concurrency().max(1);
        // Borrowed, not moved: the same store serves every batch in the loop.
        let store_ref = &store;
        let mut metas: HashMap<String, Meta> = HashMap::new();
        for batch in meta_keys.chunks(width) {
            let fetched = futures::future::join_all(
                batch
                    .iter()
                    .map(|(name, key)| async move { (name.clone(), store_ref.get(key).await) }),
            )
            .await;
            for (name, result) in fetched {
                if let Some(bytes) = result?
                    && let Some(meta) = Meta::decode(&bytes)?
                {
                    metas.insert(name, meta);
                }
            }
        }

        delta_keys.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        let mut per_file: HashMap<String, Vec<DeltaBatch>> = HashMap::new();
        let mut committed: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut max_txn = 0u64;
        let mut max_seq: HashMap<String, u64> = HashMap::new();
        let mut stale: Vec<String> = Vec::new();

        for group in delta_keys.chunks(width) {
            let fetched = futures::future::join_all(group.iter().map(|(name, seq, key)| {
                let name = name.clone();
                let key = key.clone();
                async move {
                    let bytes = store_ref.get(&key).await;
                    (name, *seq, key, bytes)
                }
            }))
            .await;
            for (name, seq, key, result) in fetched {
                let entry = max_seq.entry(name.clone()).or_insert(0);
                *entry = (*entry).max(seq + 1);
                let retired = metas.get(&name).is_some_and(|m| seq < m.compacted_upto);
                if retired {
                    // Folded into a base chunk already; the delete that should
                    // have removed it did not land before the crash.
                    stale.push(key);
                    continue;
                }
                let Some(bytes) = result? else { continue };
                match DeltaBatch::decode(&bytes)? {
                    Some(batch) => {
                        committed.extend(batch.committed.iter().copied());
                        for record in &batch.records {
                            max_txn = max_txn.max(record.txn);
                        }
                        per_file.entry(name).or_default().push(batch);
                    }
                    None => {
                        warn!(
                            "kv: delta {key} is not intact; its transactions are treated as uncommitted"
                        );
                        stale.push(key);
                    }
                }
            }
        }
        if !stale.is_empty() {
            let _ = store.delete(stale).await;
        }
        max_txn = max_txn.max(committed.iter().copied().max().unwrap_or(0));

        let mut recovered: HashMap<String, FileRecovery> = HashMap::new();
        for (name, batches) in per_file {
            let meta = metas.remove(&name).unwrap_or_default();
            recovered.insert(name, FileRecovery { meta, batches });
        }
        for (name, meta) in metas {
            recovered.insert(
                name,
                FileRecovery {
                    meta,
                    batches: Vec::new(),
                },
            );
        }
        for (name, next) in max_seq {
            if let Some(entry) = recovered.get_mut(&name) {
                entry.meta.compacted_upto = entry.meta.compacted_upto.max(next);
            }
        }

        Ok(Arc::new(Self {
            store,
            cache: Arc::new(ChunkCache::new(config.cache_bytes)),
            config: Arc::new(config),
            files: dashmap::DashMap::new(),
            next_file_id: AtomicU64::new(1),
            // Resumed past everything the store has seen. Restarting the counter
            // would let a fresh transaction take an id some surviving batch has
            // already declared committed, and its half-written records would
            // then apply on the next recovery.
            next_txn: AtomicU64::new(max_txn + 1),
            recovered: Mutex::new(recovered),
            committed: Arc::new(committed),
            erasing: dashmap::DashMap::new(),
        }))
    }

    pub fn store(&self) -> &Arc<dyn KvStore> {
        &self.store
    }

    pub fn config(&self) -> &KvConfig {
        &self.config
    }

    /// (cache hits, misses, resident bytes).
    pub fn cache_stats(&self) -> (u64, u64, u64) {
        self.cache.stats()
    }

    /// Files opened so far, for a flush or a compaction across the database.
    fn open_files(&self) -> Vec<Arc<KvFile>> {
        self.files.iter().map(|e| e.value().clone()).collect()
    }

    /// Makes every file's half of `txn` durable.
    ///
    /// `last` — the file the caller committed on — goes last and is the only one
    /// whose object declares the transaction committed. Everything before it is
    /// stored but inert until that declaration lands.
    async fn commit(&self, txn: u64, last: &Arc<KvFile>) -> io::Result<()> {
        let others: Vec<Arc<KvFile>> = self
            .open_files()
            .into_iter()
            .filter(|f| !Arc::ptr_eq(f, last) && f.holds(txn))
            .collect();

        // Concurrently: they are independent objects and the commit's latency is
        // one round trip plus the slowest of these, not their sum.
        let results = futures::future::join_all(others.iter().map(|f| f.flush(None))).await;
        for result in results {
            result?;
        }
        let outcome = last.flush(Some(txn)).await;
        if outcome.is_ok() {
            // The transaction is over everywhere, so no file's compaction has to
            // keep avoiding the range it was writing. Every open file, not just
            // the ones flushed here: one that spilled its records earlier is
            // still holding the transaction open.
            for file in self.open_files() {
                file.state.lock().open_txns.remove(&txn);
            }
        }
        outcome
    }

    /// Writes out every file's unstored records. For a clean shutdown, which the
    /// trait has no hook for.
    pub async fn flush_all(&self) -> io::Result<()> {
        let files = self.open_files();
        let results = futures::future::join_all(files.iter().map(|f| f.flush(None))).await;
        for result in results {
            result?;
        }
        Ok(())
    }

    async fn open_file(self: &Arc<Self>, name: &str) -> io::Result<Arc<KvFile>> {
        if let Some(existing) = self.files.get(name) {
            return Ok(existing.clone());
        }
        // A previous incarnation of this name may still be sweeping its
        // objects. Opening now would build a file whose first writes that task
        // is about to delete, so wait for it — the gate is closed rather than
        // signalled, which wakes every waiter and cannot be missed by one that
        // arrives late.
        let pending = self.erasing.get(name).map(|gate| gate.clone());
        if let Some(gate) = pending {
            let _ = gate.acquire().await;
        }
        let file = KvFile::open(self, name).await?;
        // Another task may have opened the same name while we were reading the
        // store. Whoever registered first wins, so both handles see one file.
        Ok(match self.files.entry(name.to_string()) {
            dashmap::Entry::Occupied(e) => e.get().clone(),
            dashmap::Entry::Vacant(e) => {
                e.insert(file.clone());
                file
            }
        })
    }
}

/// A file's mutable state. Everything here is touched under one lock, held only
/// for memory work — never across a request to the store.
struct State {
    /// Writes since the last freeze.
    live: Overlay,
    /// Writes a compaction is currently folding into base chunks.
    frozen: Option<Overlay>,
    /// Records not yet in any delta object.
    pending: Vec<Record>,
    pending_bytes: u64,
    /// Transactions to declare committed in the next delta object.
    committing: Vec<u64>,
    /// Open transactions, each mapped to the lowest offset it has written.
    /// Compaction may not fold a chunk that reaches one of these.
    open_txns: HashMap<u64, u64>,
    /// Delta object sequences the store holds, ascending.
    deltas: Vec<u64>,
    /// Deltas below this have been folded into base chunks.
    compacted_upto: u64,
    /// Sequence reserved by a freeze for the batch that will carry whatever the
    /// compaction could not fold. Set while a compaction is in flight.
    reserved_seq: Option<u64>,
}

pub(crate) struct KvFile {
    store: Arc<dyn KvStore>,
    config: Arc<KvConfig>,
    cache: Arc<ChunkCache>,
    id: u64,
    name: String,
    /// Key prefix of this file. Swapped, not copied, by `swap_temp`.
    root: ArcSwap<String>,
    generation: AtomicU64,
    /// Logical length, and the append allocator.
    len: AtomicU64,
    /// How much of the file the base chunks describe.
    base_len: AtomicU64,
    /// Shared with any scratch file made from this one, so the two never take
    /// the same delta sequence — they write into one key space.
    next_seq: Arc<AtomicU64>,
    /// A scratch file from `create_temp`. It shares the real file's key prefix
    /// and differs only by generation, so erasing it must not reach past its
    /// own generation.
    is_temp: bool,
    state: Mutex<State>,
    /// Serialises delta writes so sequence order is store order.
    flush: tokio::sync::Mutex<()>,
    compacting: AtomicBool,
    service: AtomicBool,
    dropped: AtomicBool,
    /// End of the last read, for spotting a sequential scan.
    read_cursor: AtomicU64,
    /// Highest offset read-ahead has already asked for.
    prefetched_to: AtomicU64,
}

impl KvFile {
    #[inline]
    fn root(&self) -> Arc<String> {
        self.root.load_full()
    }

    #[inline]
    fn page_of(&self, offset: u64) -> u64 {
        offset / self.config.page_size
    }

    #[inline]
    fn chunk_of_page(&self, page: u64) -> u64 {
        page / self.config.pages_per_chunk()
    }

    // ---- open and recover --------------------------------------------------

    async fn open(family: &Arc<KvFamily>, name: &str) -> io::Result<Arc<Self>> {
        let recovery = family.recovered.lock().remove(name);
        let (meta, batches) = match recovery {
            Some(r) => (r.meta, r.batches),
            None => (Meta::default(), Vec::new()),
        };

        let config = family.config.clone();
        let root = format!("{}/{}", config.root, name);

        // A file written with a different chunk size is readable only with that
        // size; every offset in it is derived from it. Adopting it is right, and
        // silently using the configured one would misplace every byte.
        let chunk_size = if meta.chunk_size == 0 {
            config.chunk_size
        } else {
            meta.chunk_size
        };
        let config = if chunk_size == config.chunk_size {
            config
        } else {
            let mut adjusted = (*config).clone();
            adjusted.chunk_size = chunk_size;
            if chunk_size % adjusted.page_size != 0 {
                adjusted.page_size = gcd(chunk_size, adjusted.page_size).max(4096);
            }
            adjusted.validate()?;
            Arc::new(adjusted)
        };

        let mut state = State {
            live: Overlay::new(),
            frozen: None,
            pending: Vec::new(),
            pending_bytes: 0,
            committing: Vec::new(),
            open_txns: HashMap::new(),
            deltas: Vec::new(),
            compacted_upto: meta.compacted_upto,
            reserved_seq: None,
        };

        let generation = meta.base_gen;
        let mut len = meta.base_len;
        let mut base_len = meta.base_len;
        let mut next_seq = meta.compacted_upto;

        // A record counts only once some batch — in this file or in any other
        // file of the database — has named its transaction. That is what keeps
        // an INSERT that died between its two files from leaving a pointer to
        // bytes that were never stored.
        for batch in &batches {
            next_seq = next_seq.max(batch.seq + 1);
            if batch.base_gen != generation {
                // Describes bytes of a generation that no longer exists.
                continue;
            }
            state.deltas.push(batch.seq);
            for record in &batch.records {
                if record.txn != 0 && !family.committed.contains(&record.txn) {
                    continue;
                }
                match record.kind {
                    RecordKind::Write => {
                        state.live.insert(record.offset, &record.payload);
                        len = len.max(record.offset + record.payload.len() as u64);
                    }
                    RecordKind::Truncate => {
                        state.live.truncate(record.offset);
                        len = record.offset;
                        base_len = base_len.min(record.offset);
                    }
                }
            }
        }

        let id = family.next_file_id.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(Self {
            store: family.store.clone(),
            config,
            cache: family.cache.clone(),
            id,
            name: name.to_string(),
            root: ArcSwap::from_pointee(root),
            generation: AtomicU64::new(generation),
            len: AtomicU64::new(len),
            base_len: AtomicU64::new(base_len),
            next_seq: Arc::new(AtomicU64::new(next_seq)),
            is_temp: false,
            state: Mutex::new(state),
            flush: tokio::sync::Mutex::new(()),
            compacting: AtomicBool::new(false),
            service: AtomicBool::new(false),
            dropped: AtomicBool::new(false),
            read_cursor: AtomicU64::new(u64::MAX),
            prefetched_to: AtomicU64::new(0),
        }))
    }

    /// Whether this file is holding unstored records for `txn`.
    fn holds(&self, txn: u64) -> bool {
        let state = self.state.lock();
        state.pending.iter().any(|r| r.txn == txn)
    }

    // ---- reads -------------------------------------------------------------

    /// Fills `dst` from the base chunks alone, zeroing anything past `base_len`.
    async fn fill_from_base(&self, position: u64, dst: &mut [u8]) -> io::Result<()> {
        if dst.is_empty() {
            return Ok(());
        }
        let base_len = self.base_len.load(Ordering::Acquire);
        let end = position + dst.len() as u64;

        if position >= base_len {
            dst.fill(0);
            return Ok(());
        }
        if end > base_len {
            let from = (base_len - position) as usize;
            dst[from..].fill(0);
        }
        let covered_end = end.min(base_len);

        let page_size = self.config.page_size;
        let first = self.page_of(position);
        let last = self.page_of(covered_end - 1);
        let pages: Vec<u64> = (first..=last).collect();
        let resident = self.pages(&pages, base_len).await?;

        for page in first..=last {
            let page_start = page * page_size;
            let copy_start = page_start.max(position);
            let copy_end = (page_start + page_size).min(covered_end);
            if copy_end <= copy_start {
                continue;
            }
            let dst_at = (copy_start - position) as usize;
            let n = (copy_end - copy_start) as usize;
            match resident.get(&page) {
                Some(bytes) => {
                    let src_at = (copy_start - page_start) as usize;
                    let available = bytes.len().saturating_sub(src_at);
                    let copied = n.min(available);
                    dst[dst_at..dst_at + copied].copy_from_slice(&bytes[src_at..src_at + copied]);
                    if copied < n {
                        // The object is shorter than the page: a hole, which a
                        // file would read back as zeros.
                        dst[dst_at + copied..dst_at + n].fill(0);
                    }
                }
                None => dst[dst_at..dst_at + n].fill(0),
            }
        }
        Ok(())
    }

    /// Resolves `wanted` pages, fetching whatever the cache is missing.
    ///
    /// The one decision worth making here is per chunk: a batch that wants a few
    /// scattered pages of a chunk fetches them as ranged reads, and a batch that
    /// wants most of one fetches the object whole. Getting this backwards is the
    /// difference between a scan costing one request per 4 MiB and one per page.
    async fn pages(&self, wanted: &[u64], base_len: u64) -> io::Result<HashMap<u64, Arc<Vec<u8>>>> {
        let generation = self.generation.load(Ordering::Acquire);
        let mut out: HashMap<u64, Arc<Vec<u8>>> = HashMap::with_capacity(wanted.len());
        let mut missing_by_chunk: HashMap<u64, Vec<u64>> = HashMap::new();

        for &page in wanted {
            let id = ChunkId {
                file: self.id,
                generation,
                index: page,
            };
            match self.cache.get(id) {
                Some(bytes) => {
                    out.insert(page, bytes);
                }
                None => missing_by_chunk
                    .entry(self.chunk_of_page(page))
                    .or_default()
                    .push(page),
            }
        }
        if missing_by_chunk.is_empty() {
            return Ok(out);
        }

        let root = self.root();
        let page_size = self.config.page_size;
        let pages_per_chunk = self.config.pages_per_chunk();

        enum Fetch {
            /// The whole chunk object, splitting into pages on arrival.
            Chunk { chunk: u64 },
            /// One page, as a ranged read.
            Page { chunk: u64, page: u64 },
        }

        let mut work: Vec<Fetch> = Vec::new();
        for (chunk, mut pages) in missing_by_chunk {
            pages.sort_unstable();
            if pages.len() as u64 >= self.config.chunk_fetch_pages.min(pages_per_chunk) {
                work.push(Fetch::Chunk { chunk });
            } else {
                for page in pages {
                    work.push(Fetch::Page { chunk, page });
                }
            }
        }

        let width = self.store.concurrency().max(1);
        for batch in work.chunks(width) {
            let futures = batch.iter().map(|item| {
                let root = root.clone();
                async move {
                    match *item {
                        Fetch::Chunk { chunk } => {
                            let key = layout::chunk_key(&root, generation, chunk);
                            let bytes = self.store.get(&key).await?;
                            Ok::<_, io::Error>((chunk, None, bytes))
                        }
                        Fetch::Page { chunk, page } => {
                            let key = layout::chunk_key(&root, generation, chunk);
                            let within = (page - chunk * pages_per_chunk) * page_size;
                            let bytes = self
                                .store
                                .get_range(&key, within, page_size as usize)
                                .await?;
                            Ok((chunk, Some(page), bytes))
                        }
                    }
                }
            });
            for result in futures::future::join_all(futures).await {
                let (chunk, page, bytes) = result?;
                let Some(bytes) = bytes else { continue };
                match page {
                    Some(page) => {
                        let bytes = Arc::new(bytes);
                        self.cache.insert(
                            ChunkId {
                                file: self.id,
                                generation,
                                index: page,
                            },
                            bytes.clone(),
                        );
                        out.insert(page, bytes);
                    }
                    None => {
                        // Split the object into pages so a later point read of a
                        // neighbouring row is a cache hit rather than a request.
                        let base = chunk * pages_per_chunk;
                        for (i, slice) in bytes.chunks(page_size as usize).enumerate() {
                            let page = base + i as u64;
                            if page * page_size >= base_len {
                                break;
                            }
                            let bytes = Arc::new(slice.to_vec());
                            self.cache.insert(
                                ChunkId {
                                    file: self.id,
                                    generation,
                                    index: page,
                                },
                                bytes.clone(),
                            );
                            out.insert(page, bytes);
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// Reads into `dst`, returning how many bytes the file actually had.
    async fn read_at(&self, position: u64, dst: &mut [u8]) -> io::Result<usize> {
        self.read_at_inner(position, dst, true).await
    }

    /// `note` drives read-ahead. A vectored batch has already put everything it
    /// needs in flight, so letting each of its reads also trigger a prefetch
    /// would queue the same chunks many times over.
    async fn read_at_inner(&self, position: u64, dst: &mut [u8], note: bool) -> io::Result<usize> {
        if dst.is_empty() {
            return Ok(0);
        }
        let len = self.len.load(Ordering::Acquire);
        if position >= len {
            return Ok(0);
        }
        let n = dst.len().min((len - position) as usize);
        let dst = &mut dst[..n];
        let end = position + n as u64;

        // The common case for a freshly appended row: the live overlay has all
        // of it, so nothing is asked of the store.
        {
            let state = self.state.lock();
            if state.frozen.is_none() && state.live.covers(position, end) {
                state.live.apply(position, dst);
                return Ok(n);
            }
        }

        self.fill_from_base(position, dst).await?;
        {
            let state = self.state.lock();
            if let Some(frozen) = &state.frozen {
                frozen.apply(position, dst);
            }
            state.live.apply(position, dst);
        }
        if note {
            self.note_read(position, end);
        }
        Ok(n)
    }

    /// Serves a batch of reads with one round of fetches instead of one per read.
    async fn read_many(&self, reads: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        if reads.is_empty() {
            return Ok(());
        }
        let len = self.len.load(Ordering::Acquire);
        let base_len = self.base_len.load(Ordering::Acquire);

        // One pass to work out every page the batch could want, so they are all
        // in flight together rather than discovered one read at a time.
        let mut wanted: Vec<u64> = Vec::new();
        {
            let state = self.state.lock();
            for (position, buf) in reads.iter() {
                if *position >= len || buf.is_empty() {
                    continue;
                }
                let n = buf.len().min((len - *position) as usize) as u64;
                let end = *position + n;
                if state.frozen.is_none() && state.live.covers(*position, end) {
                    continue;
                }
                let covered_end = end.min(base_len);
                if *position >= covered_end {
                    continue;
                }
                let first = self.page_of(*position);
                let last = self.page_of(covered_end - 1);
                wanted.extend(first..=last);
            }
        }
        wanted.sort_unstable();
        wanted.dedup();
        if !wanted.is_empty() {
            self.pages(&wanted, base_len).await?;
        }

        // Every page the batch needs is resident now, so each read is memory work.
        let mut first_error = None;
        for (position, buf) in reads.iter_mut() {
            match self.read_at_inner(*position, buf, false).await {
                Ok(got) if got == buf.len() => {}
                Ok(_) => {
                    if first_error.is_none() {
                        first_error = Some(io::Error::new(
                            ErrorKind::UnexpectedEof,
                            "Not enough bytes to fill buffer",
                        ));
                    }
                }
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Serves a batch only if it needs nothing from the store.
    fn try_read_many_sync(&self, reads: &mut [(u64, &mut [u8])]) -> bool {
        let len = self.len.load(Ordering::Acquire);
        let base_len = self.base_len.load(Ordering::Acquire);
        let generation = self.generation.load(Ordering::Acquire);
        let state = self.state.lock();

        // Checked in full before anything is copied, so the ordinary "not
        // resident" answer leaves the caller's buffers untouched. Only an
        // eviction racing the second pass can copy and then still return false,
        // and the caller's fallback overwrites what that wrote.
        for (position, buf) in reads.iter() {
            if buf.is_empty() {
                continue;
            }
            let end = *position + buf.len() as u64;
            if end > len {
                return false;
            }
            if state.frozen.is_none() && state.live.covers(*position, end) {
                continue;
            }
            let covered_end = end.min(base_len);
            if *position >= covered_end {
                continue;
            }
            let first = self.page_of(*position);
            let last = self.page_of(covered_end - 1);
            for page in first..=last {
                if !self.cache.contains(ChunkId {
                    file: self.id,
                    generation,
                    index: page,
                }) {
                    return false;
                }
            }
        }

        for (position, buf) in reads.iter_mut() {
            if buf.is_empty() {
                continue;
            }
            let end = *position + buf.len() as u64;
            let covered_end = end.min(base_len);
            if *position < covered_end {
                let page_size = self.config.page_size;
                let first = self.page_of(*position);
                let last = self.page_of(covered_end - 1);
                for page in first..=last {
                    let page_start = page * page_size;
                    let copy_start = page_start.max(*position);
                    let copy_end = (page_start + page_size).min(covered_end);
                    if copy_end <= copy_start {
                        continue;
                    }
                    let dst_at = (copy_start - *position) as usize;
                    let n = (copy_end - copy_start) as usize;
                    let id = ChunkId {
                        file: self.id,
                        generation,
                        index: page,
                    };
                    match self.cache.get(id) {
                        Some(bytes) => {
                            let src_at = (copy_start - page_start) as usize;
                            let available = bytes.len().saturating_sub(src_at);
                            let copied = n.min(available);
                            buf[dst_at..dst_at + copied]
                                .copy_from_slice(&bytes[src_at..src_at + copied]);
                            if copied < n {
                                buf[dst_at + copied..dst_at + n].fill(0);
                            }
                        }
                        // Evicted between the check and here; the caller falls
                        // back to the async path, which will refetch it.
                        None => return false,
                    }
                }
            }
            if end > covered_end {
                let from = (covered_end.max(*position) - *position) as usize;
                buf[from..].fill(0);
            }
            if let Some(frozen) = &state.frozen {
                frozen.apply(*position, buf);
            }
            state.live.apply(*position, buf);
        }
        true
    }

    /// Notices a scan and puts the next chunks in flight behind it.
    fn note_read(&self, position: u64, end: u64) {
        if self.config.readahead_chunks == 0 {
            return;
        }
        let previous = self.read_cursor.swap(end, Ordering::Relaxed);
        // Sequential means "starts where the last read ended", with a page of
        // slack for a caller that reads a header and then a body.
        if previous == u64::MAX
            || position < previous
            || position > previous + self.config.page_size
        {
            return;
        }
        let chunk_size = self.config.chunk_size;
        let ahead = end + self.config.readahead_chunks * chunk_size;
        let target = ahead.min(self.base_len.load(Ordering::Acquire));
        let already = self.prefetched_to.load(Ordering::Relaxed);
        if target <= already.max(end) {
            return;
        }
        if self
            .prefetched_to
            .compare_exchange(already, target, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let from = end.max(already);
        let first_chunk = from / chunk_size;
        let last_chunk = (target - 1) / chunk_size;
        let generation = self.generation.load(Ordering::Acquire);
        let root = self.root();
        let store = self.store.clone();
        let cache = self.cache.clone();
        let file = self.id;
        let pages_per_chunk = self.config.pages_per_chunk();
        let page_size = self.config.page_size;
        let base_len = self.base_len.load(Ordering::Acquire);

        tokio::spawn(async move {
            for chunk in first_chunk..=last_chunk {
                if cache.contains(ChunkId {
                    file,
                    generation,
                    index: chunk * pages_per_chunk,
                }) {
                    continue;
                }
                let key = layout::chunk_key(&root, generation, chunk);
                let Ok(Some(bytes)) = store.get(&key).await else {
                    continue;
                };
                let base = chunk * pages_per_chunk;
                for (i, slice) in bytes.chunks(page_size as usize).enumerate() {
                    let page = base + i as u64;
                    if page * page_size >= base_len {
                        break;
                    }
                    cache.insert(
                        ChunkId {
                            file,
                            generation,
                            index: page,
                        },
                        Arc::new(slice.to_vec()),
                    );
                }
            }
        });
    }

    // ---- writes ------------------------------------------------------------

    /// Records a change and returns the transaction it belongs to, which is the
    /// token [`StorageIO::commit_durable`] takes.
    fn stage(&self, txn: u64, record: Record) -> u64 {
        let mut state = self.state.lock();
        if record.kind == RecordKind::Write {
            state.live.insert(record.offset, &record.payload);
        }
        if txn != 0 {
            let low = state.open_txns.entry(txn).or_insert(u64::MAX);
            *low = (*low).min(record.offset);
        }
        state.pending_bytes += record.weight();
        state.pending.push(record);
        txn
    }

    fn append(&self, txn: u64, bytes: &[u8]) -> u64 {
        // The offset has to come from the same atomic step that claims it, or
        // two appends describe each other's bytes. See `StorageIO`.
        let offset = self.len.fetch_add(bytes.len() as u64, Ordering::AcqRel);
        if !bytes.is_empty() {
            self.stage(txn, Record::write(txn, offset, bytes.to_vec()));
        }
        offset
    }

    fn write_at(&self, txn: u64, position: u64, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let end = position + bytes.len() as u64;
        self.stage(txn, Record::write(txn, position, bytes.to_vec()));
        self.len.fetch_max(end, Ordering::AcqRel);
    }

    /// Writes one delta object holding whatever is pending, and declares
    /// `commit` — and anything committed since the last flush — durable.
    async fn flush(&self, commit: Option<u64>) -> io::Result<()> {
        let _serialised = self.flush.lock().await;
        self.flush_locked(commit).await
    }

    /// As [`Self::flush`], for a caller already holding the flush lock.
    async fn flush_locked(&self, commit: Option<u64>) -> io::Result<()> {
        let (records, committed, seq, base_gen) = {
            let mut state = self.state.lock();
            if let Some(txn) = commit {
                state.committing.push(txn);
                state.open_txns.remove(&txn);
            }
            if state.pending.is_empty() && state.committing.is_empty() {
                return Ok(());
            }
            let records = std::mem::take(&mut state.pending);
            state.pending_bytes = 0;
            let committed = std::mem::take(&mut state.committing);
            // Allocated under the flush lock, so sequence order is store order.
            let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
            (
                records,
                committed,
                seq,
                self.generation.load(Ordering::Acquire),
            )
        };

        let batch = DeltaBatch {
            seq,
            base_gen,
            committed,
            records,
        };
        let key = layout::delta_key(&self.root(), seq);
        let encoded = batch.encode();
        match self.store.put(&key, encoded).await {
            Ok(()) => {
                self.state.lock().deltas.push(seq);
                Ok(())
            }
            Err(e) => {
                // Nothing was made durable, so the records go back on the queue
                // and the transactions go back to being uncommitted. Reporting
                // success here is the failure mode this whole trait exists to
                // prevent.
                let mut state = self.state.lock();
                let mut restored = batch.records;
                restored.append(&mut state.pending);
                state.pending_bytes = restored.iter().map(|r| r.weight()).sum();
                state.pending = restored;
                let mut committing = batch.committed;
                committing.append(&mut state.committing);
                state.committing = committing;
                Err(e)
            }
        }
    }

    /// Writes pending records out without committing anything, to bound the
    /// copy a long statement holds. Nothing applies them until a later batch
    /// names their transaction.
    async fn spill(&self) {
        if let Err(e) = self.flush(None).await {
            warn!("kv: spilling {} failed: {e}", self.name);
        }
    }

    // ---- compaction --------------------------------------------------------

    /// Folds overlay bytes into base chunks and retires the deltas they came
    /// from. Runs in the background; a failure leaves the file readable and
    /// simply postpones the work.
    async fn compact(self: &Arc<Self>) -> io::Result<()> {
        if self.compacting.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let result = self.compact_inner().await;
        self.compacting.store(false, Ordering::Release);
        result
    }

    async fn compact_inner(self: &Arc<Self>) -> io::Result<()> {
        let chunk_size = self.config.chunk_size;

        // ---- freeze ----
        //
        // The live overlay is set aside and writes carry on against a fresh one,
        // so nothing below has to reason about a range changing underneath it.
        // The sequence for the leftovers is reserved here, before any write can
        // take a higher one, so the batch it eventually names sorts *before*
        // everything written during the compaction.
        let (safe_end, generation, base_len, len, reserved_seq) = {
            let _serialised = self.flush.lock().await;
            let mut state = self.state.lock();
            if state.frozen.is_some() || state.live.is_empty() {
                return Ok(());
            }
            let len = self.len.load(Ordering::Acquire);
            let lowest_open = state.open_txns.values().copied().min().unwrap_or(u64::MAX);
            // Only what is committed is frozen. An open transaction's bytes stay
            // in the live overlay, where they are still visible to their own
            // writer and still absent from every object that outlives a crash.
            let frozen = state.live.split_below(lowest_open);
            if frozen.is_empty() {
                return Ok(());
            }
            state.frozen = Some(frozen);
            let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
            state.reserved_seq = Some(seq);
            (
                len.min(lowest_open),
                self.generation.load(Ordering::Acquire),
                self.base_len.load(Ordering::Acquire),
                len,
                seq,
            )
        };

        // ---- choose ----
        let mut candidates: Vec<u64> = Vec::new();
        {
            let state = self.state.lock();
            let frozen = state.frozen.as_ref().expect("just installed");
            let mut seen: Vec<u64> = Vec::new();
            for (start, end) in frozen.span_bounds() {
                let first = start / chunk_size;
                let last = (end - 1) / chunk_size;
                for chunk in first..=last {
                    if !seen.contains(&chunk) {
                        seen.push(chunk);
                    }
                }
            }
            for chunk in seen {
                let start = chunk * chunk_size;
                let end = start + chunk_size;
                // A chunk is foldable only when nothing uncommitted reaches into
                // it: an open transaction's bytes must not become base data.
                let foldable = end <= safe_end || (safe_end >= len && end >= len);
                if !foldable {
                    continue;
                }
                let overlay_bytes = frozen.bytes_in(start, end.min(len));
                if overlay_bytes == 0 {
                    continue;
                }
                let forced = state.deltas.len() >= self.config.max_deltas;
                if forced || overlay_bytes >= self.config.compact_chunk_bytes || end <= base_len {
                    candidates.push(chunk);
                }
            }
            candidates.sort_unstable();
        }

        // ---- rewrite ----
        let root = self.root();
        let mut new_base_len = base_len;
        for chunk in &candidates {
            let start = chunk * chunk_size;
            let chunk_end = (start + chunk_size).min(len);
            if chunk_end <= start {
                continue;
            }
            let mut buffer = vec![0u8; (chunk_end - start) as usize];
            self.fill_from_base(start, &mut buffer).await?;
            {
                let state = self.state.lock();
                if let Some(frozen) = &state.frozen {
                    frozen.apply(start, &mut buffer);
                }
            }
            let key = layout::chunk_key(&root, generation, *chunk);
            self.store.put(&key, buffer).await?;
            new_base_len = new_base_len.max(chunk_end);

            // The pages this chunk covers now hold stale bytes.
            let pages_per_chunk = self.config.pages_per_chunk();
            for page in chunk * pages_per_chunk..(chunk + 1) * pages_per_chunk {
                self.cache.invalidate(ChunkId {
                    file: self.id,
                    generation,
                    index: page,
                });
            }
            self.state
                .lock()
                .frozen
                .as_mut()
                .expect("held")
                .remove_range(start, chunk_end);
        }

        // ---- publish ----
        //
        // Order is load-bearing. The leftovers go out first, then the metadata
        // that makes them authoritative, and only then are the old deltas
        // deleted. Dying anywhere in here leaves a state that recovers: base
        // chunks written but not published are simply re-covered by the deltas
        // that are still there, because a record is physical redo and applying
        // it twice is applying it once.
        let _serialised = self.flush.lock().await;
        let leftovers = {
            let mut state = self.state.lock();
            let frozen = state.frozen.take().expect("held");
            state.reserved_seq = None;
            frozen
        };

        if !leftovers.is_empty() {
            // Transaction 0: already committed, by definition — everything in
            // the frozen overlay was committed before the freeze took it.
            let records: Vec<Record> = leftovers
                .spans_iter()
                .map(|(start, bytes)| Record::write(0, start, bytes.to_vec()))
                .collect();
            let batch = DeltaBatch {
                seq: reserved_seq,
                base_gen: generation,
                committed: Vec::new(),
                records,
            };
            self.store
                .put(&layout::delta_key(&root, reserved_seq), batch.encode())
                .await?;
        }

        self.base_len.store(new_base_len, Ordering::Release);
        let retire = self.state.lock().open_txns.is_empty();
        let meta = Meta {
            chunk_size,
            base_gen: generation,
            base_len: new_base_len,
            // Only claim the older deltas are gone if they are about to be. A
            // watermark ahead of the deletes would have recovery skip records
            // that are still the only copy.
            compacted_upto: if retire {
                reserved_seq
            } else {
                self.state.lock().compacted_upto
            },
        };
        self.store
            .put(&layout::meta_key(&root), meta.encode())
            .await?;

        let retired: Vec<String> = {
            let mut state = self.state.lock();
            // An older delta may still hold records of a transaction that is
            // open right now. Deleting it would leave a commit declaration with
            // nothing to commit, so retirement waits for a quiet moment; the
            // chunks and the leftovers are published either way, so the pass
            // still made progress.
            let retire = state.open_txns.is_empty();
            let (old, keep): (Vec<u64>, Vec<u64>) = if retire {
                state.deltas.iter().partition(|&&s| s < reserved_seq)
            } else {
                (Vec::new(), state.deltas.clone())
            };
            state.deltas = keep;
            if !leftovers.is_empty() {
                state.deltas.push(reserved_seq);
            }
            state.deltas.sort_unstable();
            state.deltas.dedup();
            if retire {
                state.compacted_upto = reserved_seq;
            }
            // Whatever could not be folded stays readable: it goes back to the
            // live overlay, under anything written since the freeze.
            if !leftovers.is_empty() {
                let live = std::mem::replace(&mut state.live, Overlay::new());
                let mut rebuilt = leftovers;
                // Live spans go in last so a write made during the compaction
                // still wins over the older bytes it replaced.
                for (start, bytes) in live.spans_iter() {
                    rebuilt.insert(start, bytes);
                }
                state.live = rebuilt;
            }
            old.into_iter()
                .map(|s| layout::delta_key(&root, s))
                .collect()
        };
        if !retired.is_empty() {
            self.store.delete(retired).await?;
        }
        Ok(())
    }

    // ---- lifecycle ---------------------------------------------------------

    async fn truncate_to(&self, new_len: u64) -> io::Result<()> {
        let current = self.len.load(Ordering::Acquire);
        if new_len > current {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("truncate to {new_len} would grow a {current}-byte file"),
            ));
        }
        if new_len == current {
            return Ok(());
        }
        let generation = self.generation.load(Ordering::Acquire);
        let root = self.root();
        let chunk_size = self.config.chunk_size;
        let base_len = self.base_len.load(Ordering::Acquire);

        {
            let mut state = self.state.lock();
            state.live.truncate(new_len);
            if let Some(frozen) = &mut state.frozen {
                frozen.truncate(new_len);
            }
            state
                .pending
                .retain(|r| r.kind != RecordKind::Write || r.offset < new_len);
            state.pending_bytes = state.pending.iter().map(|r| r.weight()).sum();
            state.pending.push(Record::truncate(0, new_len));
            state.pending_bytes += 21;
        }
        self.len.store(new_len, Ordering::Release);
        self.base_len
            .store(base_len.min(new_len), Ordering::Release);
        self.flush(None).await?;

        // Objects wholly past the new end are dead. The one straddling it keeps
        // its tail — nothing reads past `len`, and rewriting it would cost a
        // request to change bytes no one can see.
        if base_len > new_len {
            let first_dead = new_len.div_ceil(chunk_size);
            let last = (base_len - 1) / chunk_size;
            if first_dead <= last {
                let keys: Vec<String> = (first_dead..=last)
                    .map(|c| layout::chunk_key(&root, generation, c))
                    .collect();
                self.store.delete(keys).await?;
            }
        }
        self.cache.invalidate_file(self.id);
        Ok(())
    }

    /// Removes every object this file owns.
    /// The next unused base generation for this key prefix.
    fn next_generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire) + 1
    }

    /// Makes `temp`'s generation the live one.
    ///
    /// The metadata object is written *before* the old generation's chunks are
    /// deleted. Dying in between orphans objects, which costs storage; dying the
    /// other way round would delete the chunks the live metadata still points
    /// at, which costs the table.
    async fn adopt(&self, temp: &Arc<KvFile>) -> io::Result<()> {
        let _one = self.flush.lock().await;
        let _two = temp.flush.lock().await;

        // Anything the scratch file has not written out yet has to land first;
        // it is about to become the only copy.
        temp.flush_locked(None).await?;

        let old_generation = self.generation.load(Ordering::Acquire);
        let new_generation = temp.generation.load(Ordering::Acquire);
        let base_len = temp.base_len.load(Ordering::Acquire);
        let root = self.root();

        let meta = Meta {
            chunk_size: self.config.chunk_size,
            base_gen: new_generation,
            base_len,
            compacted_upto: temp.state.lock().compacted_upto,
        };
        self.store
            .put(&layout::meta_key(&root), meta.encode())
            .await?;

        // Read before the swap, or this names the scratch file's deltas — the
        // ones that must survive — instead of the originals.
        let dead_deltas: Vec<u64> = self.state.lock().deltas.clone();

        self.generation.store(new_generation, Ordering::Release);
        self.len
            .store(temp.len.load(Ordering::Acquire), Ordering::Release);
        self.base_len.store(base_len, Ordering::Release);
        {
            let mut mine = self.state.lock();
            let mut theirs = temp.state.lock();
            std::mem::swap(&mut *mine, &mut *theirs);
        }
        self.cache.invalidate_file(self.id);
        self.cache.invalidate_file(temp.id);

        // The old generation is unreachable now: its chunks are dead, and so are
        // the delta batches that describe them.
        let listed = self
            .store
            .list(&layout::generation_prefix(&root, old_generation))
            .await?;
        let mut doomed: Vec<String> = listed.into_iter().map(|o| o.key).collect();
        doomed.extend(
            dead_deltas
                .into_iter()
                .map(|seq| layout::delta_key(&root, seq)),
        );
        if !doomed.is_empty() {
            self.store.delete(doomed).await?;
        }
        Ok(())
    }

    async fn erase(&self) -> io::Result<()> {
        let root = self.root();
        let prefix = if self.is_temp {
            // Scratch shares the real file's prefix. Sweeping it would take the
            // live file's objects with it.
            layout::generation_prefix(&root, self.generation.load(Ordering::Acquire))
        } else {
            format!("{root}/")
        };
        let listed = self.store.list(&prefix).await?;
        let keys: Vec<String> = listed.into_iter().map(|o| o.key).collect();
        if !keys.is_empty() {
            self.store.delete(keys).await?;
        }
        self.cache.invalidate_file(self.id);
        Ok(())
    }
}

fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 { a } else { gcd(b, a % b) }
}

// ---------------------------------------------------------------------------
// The provider
// ---------------------------------------------------------------------------

/// One file of a database kept in an object store.
///
/// Cloning is cheap and every clone is the same file, the way two handles onto
/// one path are — the schema writer opens its own handle and must see what the
/// table wrote through a different one.
pub struct KvStorageProvider {
    family: Arc<KvFamily>,
    file: Arc<KvFile>,
    hash: u32,
}

impl Clone for KvStorageProvider {
    fn clone(&self) -> Self {
        Self {
            family: self.family.clone(),
            file: self.file.clone(),
            hash: self.hash,
        }
    }
}

impl KvStorageProvider {
    /// Opens `name` in `family`, recovering whatever the store already holds.
    pub async fn open(family: Arc<KvFamily>, name: &str) -> io::Result<Self> {
        let file = family.open_file(name).await?;
        let hash = super::super::CRC.checksum(name.as_bytes());
        Ok(Self { family, file, hash })
    }

    /// The family this file belongs to, for cache statistics and for opening
    /// siblings without going through the trait.
    pub fn family(&self) -> &Arc<KvFamily> {
        &self.family
    }

    /// Makes every write so far durable, committed or not.
    ///
    /// The background service does this on a timer; call it directly before a
    /// clean shutdown, which the trait has no hook for.
    pub async fn flush(&self) -> io::Result<()> {
        self.file.flush(None).await
    }

    /// Folds the overlay into base chunks now rather than when the service gets
    /// to it. Exposed for tests and for maintenance commands.
    pub async fn compact_now(&self) -> io::Result<()> {
        self.file.compact().await
    }

    /// Writes unstored records out early once they have piled up.
    ///
    /// Bounds the second copy a long statement keeps — the records and the
    /// overlay hold the same bytes until a batch is written. Nothing becomes
    /// visible to a restart: a spilled record still waits for a batch to name
    /// its transaction.
    /// Puts an untransacted write into the store before returning.
    ///
    /// A write that carries no transaction is recorded under transaction 0,
    /// which recovery treats as already committed — but only if the bytes ever
    /// reached the store. `maybe_spill` alone does not get them there: it
    /// spills to bound memory, so a write smaller than the threshold stays
    /// resident, and nothing flushes on drop because `Drop` cannot await.
    ///
    /// Behind a file there is an OS page cache that a clean shutdown writes
    /// out; behind this there is nothing, so "written" has to mean "put". A
    /// table's schema, its foreign-key sidecars and its sequences all take this
    /// path, and losing them silently is how a KV-backed table came back from a
    /// clean restart with its rows intact and **zero columns**.
    ///
    /// The cost lands on metadata writes only. Row data goes through the
    /// transaction API, which batches a whole statement into one put.
    async fn persist_untransacted(&self, what: &str) {
        if let Err(e) = self.file.flush(None).await {
            // Reported rather than swallowed: the caller's API returns nothing,
            // so a lost metadata write would otherwise be invisible until the
            // next restart, which is exactly the failure this exists to stop.
            log::error!(
                "kv: {} on '{}' could not be made durable: {}",
                what,
                self.file.name,
                e
            );
        }
    }

    async fn maybe_spill(&self) {
        let over = {
            let state = self.file.state.lock();
            state.pending_bytes > self.file.config.spill_bytes
        };
        if over {
            self.file.spill().await;
        }
    }
}

impl StorageIO for KvStorageProvider {
    async fn verify_data(&self, position: u64, buffer: &[u8]) -> bool {
        if buffer.is_empty() {
            return true;
        }
        let mut scratch = vec![0u8; buffer.len()];
        match self.file.read_at(position, &mut scratch).await {
            Ok(n) if n == buffer.len() => scratch == buffer,
            _ => false,
        }
    }

    async fn write_data(&self, position: u64, buffer: &[u8]) {
        self.file.write_at(0, position, buffer);
        self.maybe_spill().await;
        self.persist_untransacted("write_data").await;
    }

    async fn write_data_seek(&self, seek: SeekFrom, buffer: &[u8]) {
        match seek {
            SeekFrom::Start(index) => self.write_data(index, buffer).await,
            // Matching the other providers: a relative seek has no meaning for a
            // handle that has no cursor, and quietly writing at 0 would be worse
            // than refusing.
            SeekFrom::Current(_) | SeekFrom::End(_) => panic!("Not supported"),
        }
    }

    async fn read_data_into_buffer(&self, position: &mut u64, buffer: &mut [u8]) -> io::Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }
        if *position >= self.file.len.load(Ordering::Acquire) {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "Position is past end of buffer",
            ));
        }
        let got = self.file.read_at(*position, buffer).await?;
        *position += got as u64;
        if got == buffer.len() {
            Ok(())
        } else {
            Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "Not enough bytes to fill buffer",
            ))
        }
    }

    async fn read_vectored(&self, reads: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        self.file.read_many(reads).await
    }

    fn try_read_vectored_sync(&self, reads: &mut [(u64, &mut [u8])]) -> bool {
        self.file.try_read_many_sync(reads)
    }

    async fn append_data(&self, buffer: &[u8], immediate: bool) {
        self.file.append(0, buffer);
        self.maybe_spill().await;
        if immediate {
            self.persist_untransacted("append_data").await;
        }
    }

    async fn append_data_returning_offset(&self, buffer: &[u8], immediate: bool) -> u64 {
        let offset = self.file.append(0, buffer);
        self.maybe_spill().await;
        if immediate {
            self.persist_untransacted("append_data_returning_offset")
                .await;
        }
        offset
    }

    fn begin_txn(&self) -> u64 {
        self.family.next_txn.fetch_add(1, Ordering::AcqRel)
    }

    async fn append_data_txn(&self, txn: u64, buffer: &[u8]) -> (u64, u64) {
        let offset = self.file.append(txn, buffer);
        if txn == 0 {
            // A token of 0 tells the caller the bytes are already durable, so
            // they have to be. Nothing in the engine takes this path — it begins
            // a transaction first — but the contract is the contract.
            if let Err(e) = self.file.flush(None).await {
                error!(
                    "kv: untransacted append to {} could not be stored: {e}",
                    self.file.name
                );
            }
            return (offset, 0);
        }
        self.maybe_spill().await;
        (offset, txn)
    }

    async fn write_data_txn(&self, txn: u64, position: u64, buffer: &[u8]) -> u64 {
        self.file.write_at(txn, position, buffer);
        if txn == 0 {
            if let Err(e) = self.file.flush(None).await {
                error!(
                    "kv: untransacted write to {} could not be stored: {e}",
                    self.file.name
                );
            }
            return 0;
        }
        self.maybe_spill().await;
        txn
    }

    async fn commit_durable(&self, token: u64) -> IOResult<()> {
        if token == 0 {
            return Ok(());
        }
        // Not `self.file.flush`: the transaction's other half is in another
        // file, and committing only this one would store a pointer whose row
        // bytes are not there. See [`KvFamily`].
        self.family.commit(token, &self.file).await
    }

    fn try_borrow_range(&self, position: u64, len: usize) -> Option<BorrowedBytes> {
        if len == 0 {
            return None;
        }
        let end = position.checked_add(len as u64)?;
        if end > self.file.len.load(Ordering::Acquire) {
            return None;
        }
        // A borrow hands out a pointer into a cached page, so the range has to
        // live entirely in one page *and* be exactly what the store holds. Any
        // overlay touching it means the page is stale for these bytes.
        let page_size = self.file.config.page_size;
        let page = position / page_size;
        if end > (page + 1) * page_size {
            return None;
        }
        if end > self.file.base_len.load(Ordering::Acquire) {
            return None;
        }
        {
            let state = self.file.state.lock();
            if state.live.intersects(position, end) {
                return None;
            }
            if state
                .frozen
                .as_ref()
                .is_some_and(|f| f.intersects(position, end))
            {
                return None;
            }
        }
        let generation = self.file.generation.load(Ordering::Acquire);
        let bytes = self.file.cache.get(ChunkId {
            file: self.file.id,
            generation,
            index: page,
        })?;
        let at = (position - page * page_size) as usize;
        if at + len > bytes.len() {
            return None;
        }
        let ptr = unsafe { bytes.as_ptr().add(at) };
        let owner: Arc<dyn std::any::Any + Send + Sync> = bytes;
        // SAFETY: `owner` is the Arc holding the page, so the bytes stay alive
        // and unmoved for as long as the guard does, and nothing mutates a page
        // in place — a changed page is replaced in the cache, not edited.
        Some(unsafe { BorrowedBytes::new(owner, ptr, len) })
    }

    async fn truncate(&self, len: u64) -> IOResult<()> {
        self.file.truncate_to(len).await
    }

    async fn get_len(&self) -> u64 {
        self.file.len.load(Ordering::Acquire)
    }

    /// Always true.
    ///
    /// The trait makes this a static method, so there is no store to ask — and
    /// nothing in the engine calls it. Answering "no" would be a guess that
    /// reads as fact; answering "yes" matches the in-memory provider and leaves
    /// the caller to open the file and find it empty, which is the same thing.
    fn exists(_location: &str, _table_name: &str) -> bool {
        true
    }

    async fn create_temp(&self) -> Self {
        // A new *generation* of the same file rather than a differently named
        // one. An object store has no rename, so a scratch file under its own
        // name could only be adopted by copying every object into place — or,
        // worse, by leaving the live data under a name nothing reopens, which
        // is a table that comes back empty after a restart.
        //
        // Sharing the prefix is safe because a delta batch records the
        // generation it describes and recovery skips the ones that do not match
        // the live generation. Before a swap the scratch batches are inert;
        // after it, the originals are.
        let file = self.file.clone();
        let generation = file.next_generation();
        let temp = Arc::new(KvFile {
            store: file.store.clone(),
            config: file.config.clone(),
            cache: file.cache.clone(),
            id: self.family.next_file_id.fetch_add(1, Ordering::Relaxed),
            name: file.name.clone(),
            root: ArcSwap::from_pointee((*file.root()).clone()),
            generation: AtomicU64::new(generation),
            len: AtomicU64::new(0),
            base_len: AtomicU64::new(0),
            next_seq: file.next_seq.clone(),
            is_temp: true,
            state: Mutex::new(State {
                live: Overlay::new(),
                frozen: None,
                pending: Vec::new(),
                pending_bytes: 0,
                committing: Vec::new(),
                open_txns: HashMap::new(),
                deltas: Vec::new(),
                compacted_upto: 0,
                reserved_seq: None,
            }),
            flush: tokio::sync::Mutex::new(()),
            compacting: AtomicBool::new(false),
            service: AtomicBool::new(false),
            dropped: AtomicBool::new(false),
            read_cursor: AtomicU64::new(u64::MAX),
            prefetched_to: AtomicU64::new(0),
        });
        // Unregistered, like the in-memory provider's: scratch is not a name
        // anyone should be able to reopen.
        Self {
            family: self.family.clone(),
            file: temp,
            hash: self.hash,
        }
    }

    async fn swap_temp(&self, temp_io_sync: &mut Self) {
        if Arc::ptr_eq(&self.file, &temp_io_sync.file) {
            return;
        }
        if let Err(e) = self.file.adopt(&temp_io_sync.file).await {
            // No error channel in the trait, and the alternative — swapping the
            // in-memory state anyway — would leave the live file describing
            // bytes no object holds.
            error!(
                "kv: could not publish the scratch generation of {}: {e}",
                self.file.name
            );
        }
    }

    /// None.
    ///
    /// There is no directory, and the write-ahead log keys itself by one — so
    /// this file has no log, which is correct: its durability is the delta
    /// object, not a log the log's own replay would write into local files.
    fn get_location(&self) -> Option<String> {
        None
    }

    #[allow(refining_impl_trait)]
    async fn create_new(&self, name: String) -> Self {
        match KvStorageProvider::open(self.family.clone(), &name).await {
            Ok(provider) => provider,
            Err(e) => {
                error!("kv: could not open {name}: {e}");
                self.clone()
            }
        }
    }

    #[allow(refining_impl_trait)]
    async fn create_new_at(&self, _location: &str, name: String) -> Self {
        // A store has one namespace; placing a table on a different disk is a
        // filesystem idea with nothing to correspond to here.
        self.create_new(name).await
    }

    fn drop_io(&self) {
        if self.file.dropped.swap(true, Ordering::AcqRel) {
            return;
        }
        self.family.files.remove(&self.file.name);
        let file = self.file.clone();
        // Registered before the task is spawned, so a reopen that races this
        // sees the gate rather than an empty map.
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        self.family.erasing.insert(file.name.clone(), gate.clone());
        let family = self.family.clone();
        // Sync in the trait, and deleting is a round trip. Reopening the name
        // meanwhile gets a fresh file, which is what unlinking a path does.
        //
        // Guarded because `tokio::spawn` panics with no runtime, and a provider
        // dropped during shutdown may have outlived one. Leaving the objects
        // behind costs storage; panicking here would take the caller with it.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    if let Err(e) = file.erase().await {
                        error!("kv: could not erase {}: {e}", file.name);
                    }
                    // Closed first, then removed: a waiter holding the gate
                    // wakes either way, and one that reads the map after the
                    // removal finds nothing to wait for because there is not.
                    gate.close();
                    family.erasing.remove(&file.name);
                });
            }
            Err(_) => {
                // No runtime, so nothing will sweep and nothing must wait.
                gate.close();
                family.erasing.remove(&file.name);
                warn!(
                    "kv: {} was dropped with no runtime to delete its objects; they are \
                     orphaned",
                    file.name
                )
            }
        }
    }

    fn get_hash(&self) -> u32 {
        self.hash
    }

    async fn start_service(&self) {
        if self.file.service.swap(true, Ordering::AcqRel) {
            return;
        }
        let file = Arc::downgrade(&self.file);
        let interval = self.file.config.flush_interval_ms.max(10);
        let max_deltas = self.file.config.max_deltas;
        let compact_bytes = self.file.config.compact_chunk_bytes;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(interval)).await;
                // A weak handle, so the service does not keep a dropped table's
                // file alive forever.
                let Some(file) = file.upgrade() else { return };
                if file.dropped.load(Ordering::Acquire) {
                    return;
                }
                let (has_pending, deltas, overlay_bytes) = {
                    let state = file.state.lock();
                    (
                        !state.pending.is_empty(),
                        state.deltas.len(),
                        state.live.bytes(),
                    )
                };
                if has_pending && let Err(e) = file.flush(None).await {
                    error!("kv: periodic flush of {} failed: {e}", file.name);
                }
                if deltas >= max_deltas || overlay_bytes >= compact_bytes.saturating_mul(2) {
                    if let Err(e) = file.compact().await {
                        warn!("kv: compaction of {} failed: {e}", file.name);
                    }
                }
            }
        });
    }

    fn get_name(&self) -> String {
        self.file.name.clone()
    }
}
