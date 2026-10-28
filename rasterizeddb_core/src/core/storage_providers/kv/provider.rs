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
use parking_lot::{Mutex, RwLock};

use super::super::traits::{BorrowedBytes, IOResult, StorageIO};
use super::cache::{ChunkCache, ChunkId, FetchKey, PendingFetch};
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

    /// Whether this configuration can actually open a file.
    ///
    /// Public because the statement that *defines* a store has to answer this
    /// before the store exists. It used to be checked only when a family was
    /// opened, which is long after `STORAGE ADD STORE` has reported success —
    /// so a `CHUNK_SIZE` that is not a multiple of `PAGE_SIZE` produced a store
    /// that was listed, placeable, and could never be opened. See
    /// [`crate::core::storage::kv_registry::check_tuning`].
    pub fn validate(&self) -> io::Result<()> {
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
    /// One past the highest delta sequence the store holds for this name,
    /// retired or not. Kept apart from `meta.compacted_upto`, which it used to
    /// be folded into: the watermark then claimed every delta below it was
    /// folded, and the next compaction that did not retire wrote that claim
    /// into the META — and the restart after it skipped deltas that were still
    /// the only copy of their records.
    next_seq: u64,
    /// Highest base generation anything in the store names for this file: the
    /// META, a delta batch, a chunk object. A scratch file must take a
    /// generation above it, or the deltas an abandoned scratch file left
    /// behind describe the new one and replay into it once it is adopted.
    max_gen: u64,
}

/// Which unretired delta objects hold records of each transaction, across the
/// whole database.
///
/// A transaction is declared committed in one file's delta only, and the
/// records of its other half live in another file's. Retiring the declaring
/// delta — its file compacted, or was replaced by `adopt` — deleted the
/// declaration while the other file's records still depended on it, and the
/// restart after that dropped them: the row bytes of a committed INSERT, gone.
/// A file that retires a declaration consults this first and carries forward
/// the ones some other delta still needs.
#[derive(Default)]
struct Ledger {
    holders: Mutex<HashMap<u64, Vec<(Arc<str>, u64)>>>,
}

impl Ledger {
    /// `seq` of `name` holds records of each of `txns` (0 excluded).
    fn register(&self, name: &Arc<str>, seq: u64, txns: &[u64]) {
        if txns.is_empty() {
            return;
        }
        let mut holders = self.holders.lock();
        for &txn in txns {
            holders.entry(txn).or_default().push((name.clone(), seq));
        }
    }

    /// `seq` of `name` is gone; it no longer holds records of `txns`.
    fn retire(&self, name: &str, seq: u64, txns: &[u64]) {
        if txns.is_empty() {
            return;
        }
        let mut holders = self.holders.lock();
        for txn in txns {
            if let Some(list) = holders.get_mut(txn) {
                list.retain(|(n, s)| !(&**n == name && *s == seq));
                if list.is_empty() {
                    holders.remove(txn);
                }
            }
        }
    }

    /// Of `declared`, the transactions some delta other than the ones
    /// `retiring` describes still holds records of.
    fn still_needed(
        &self,
        declared: impl IntoIterator<Item = u64>,
        retiring: impl Fn(&str, u64) -> bool,
    ) -> Vec<u64> {
        let holders = self.holders.lock();
        let mut out: Vec<u64> = declared
            .into_iter()
            .filter(|txn| {
                holders
                    .get(txn)
                    .is_some_and(|list| list.iter().any(|(n, s)| !retiring(n, *s)))
            })
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }
}

/// What one delta object this file holds says about transactions.
#[derive(Clone, Default)]
struct DeltaTxns {
    /// Transactions with records in it (0 excluded), registered in the
    /// [`Ledger`].
    records: Vec<u64>,
    /// Transactions it declares committed.
    declared: Vec<u64>,
    /// Rollbacks it declares, `(txn, in_doubt_seq)`. See `DeltaBatch::aborted`.
    aborted: Vec<(u64, u64)>,
}

impl DeltaTxns {
    fn of(batch: &DeltaBatch) -> Self {
        let mut records: Vec<u64> = batch
            .records
            .iter()
            .map(|r| r.txn)
            .filter(|&t| t != 0)
            .collect();
        records.sort_unstable();
        records.dedup();
        Self {
            records,
            declared: batch.committed.clone(),
            aborted: batch.aborted.clone(),
        }
    }
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
/// A family's identity: its store and the prefix within it. See
/// [`KvFamily::identity`].
pub(crate) fn identity_of(store: &dyn KvStore, root: &str) -> String {
    format!("{} @ {}", store.describe(), root)
}

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
    /// Shared with every file. See [`Ledger`].
    ledger: Arc<Ledger>,
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
    /// Read by everything that rewrites or deletes an object of this family —
    /// compaction, a truncation's deletes, adopting a scratch generation, a
    /// dropped file's sweep. Written by [`KvFamily::pause_maintenance`], so a
    /// reader working from the objects listed at one moment finds every one
    /// still there and unchanged. Appends and commits never take it: they
    /// add objects, never change one.
    maintenance: Arc<tokio::sync::RwLock<()>>,
    /// Read for a whole commit, written by [`KvFamily::hold_commits`]. A
    /// listing taken under the write sees every commit whole or not at all;
    /// without it a listing could pass a commit's first object before it was
    /// stored and then find the declaring one.
    declaring: Arc<tokio::sync::RwLock<()>>,
}

impl KvFamily {
    /// Opens a database in `store`, reading back whatever is already there.
    ///
    /// One listing and one get per delta object, for the whole database, before
    /// any file is opened — a per-file scan could not see the commit
    /// declarations that live in another file's deltas.
    pub async fn open(store: Arc<dyn KvStore>, config: KvConfig) -> io::Result<Arc<Self>> {
        Self::open_with(store, config, false).await
    }

    /// The family as a restart would recover it from the objects in the store
    /// now, for reading only: a crash-consistent copy of every file, with no
    /// object written or deleted to get it. A live family on the same prefix
    /// should be paused around it ([`super::export`]).
    pub async fn open_snapshot(store: Arc<dyn KvStore>, config: KvConfig) -> io::Result<Arc<Self>> {
        Self::open_with(store, config, true).await
    }

    async fn open_with(store: Arc<dyn KvStore>, config: KvConfig, read_only: bool) -> io::Result<Arc<Self>> {
        config.validate()?;
        let prefix = format!("{}/", config.root);
        let listed = store.list(&prefix).await?;

        let mut meta_keys: Vec<(String, String)> = Vec::new();
        let mut delta_keys: Vec<(String, u64, String)> = Vec::new();
        let mut max_gen: HashMap<String, u64> = HashMap::new();
        for object in listed {
            let Some(rest) = object.key.strip_prefix(&prefix) else {
                continue;
            };
            let Some((name, tail)) = rest.split_once('/') else {
                continue;
            };
            if tail == "META" {
                meta_keys.push((name.to_string(), object.key));
            } else if let Some(gen_text) = tail.strip_prefix('g')
                && let Some((gen_text, _)) = gen_text.split_once('/')
                && let Ok(generation) = u64::from_str_radix(gen_text, 16)
            {
                let entry = max_gen.entry(name.to_string()).or_insert(0);
                *entry = (*entry).max(generation);
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
        let mut aborted: std::collections::HashSet<u64> = std::collections::HashSet::new();
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
                        let entry = max_gen.entry(name.clone()).or_insert(0);
                        *entry = (*entry).max(batch.base_gen);
                        committed.extend(batch.committed.iter().copied());
                        aborted.extend(batch.aborted.iter().map(|&(txn, _)| txn));
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
        if !stale.is_empty() && !read_only {
            let _ = store.delete(stale).await;
        }
        max_txn = max_txn.max(committed.iter().copied().max().unwrap_or(0));
        max_txn = max_txn.max(aborted.iter().copied().max().unwrap_or(0));
        // A rollback overrides a declaration: the declaration is a put that
        // reported failure and landed anyway. See `DeltaBatch::aborted`.
        committed.retain(|txn| !aborted.contains(txn));

        // Every delta that will be applied holds records some declaration
        // elsewhere may be keeping alive. Registered now rather than as each
        // file opens: a file nobody has opened yet still depends on another
        // file's declarations.
        let ledger = Arc::new(Ledger::default());
        let mut recovered: HashMap<String, FileRecovery> = HashMap::new();
        for (name, batches) in per_file {
            let meta = metas.remove(&name).unwrap_or_default();
            let shared: Arc<str> = Arc::from(name.as_str());
            for batch in batches.iter().filter(|b| b.base_gen == meta.base_gen) {
                ledger.register(&shared, batch.seq, &DeltaTxns::of(batch).records);
            }
            recovered.insert(
                name,
                FileRecovery {
                    meta,
                    batches,
                    next_seq: 0,
                    max_gen: 0,
                },
            );
        }
        for (name, meta) in metas {
            recovered.insert(
                name,
                FileRecovery {
                    meta,
                    batches: Vec::new(),
                    next_seq: 0,
                    max_gen: 0,
                },
            );
        }
        for (name, next) in max_seq {
            if let Some(entry) = recovered.get_mut(&name) {
                entry.next_seq = next;
            }
        }
        for (name, generation) in max_gen {
            if let Some(entry) = recovered.get_mut(&name) {
                entry.max_gen = generation;
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
            ledger,
            erasing: dashmap::DashMap::new(),
            maintenance: Arc::new(tokio::sync::RwLock::new(())),
            declaring: Arc::new(tokio::sync::RwLock::new(())),
        }))
    }

    /// Which store and prefix this family is: two families with one identity
    /// are the same files.
    pub fn identity(&self) -> String {
        identity_of(self.store.as_ref(), &self.config.root)
    }

    /// Every file the store held at open or that has been opened since.
    pub fn file_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.recovered.lock().keys().cloned().collect();
        names.extend(self.files.iter().map(|e| e.key().clone()));
        names.sort_unstable();
        names.dedup();
        names
    }

    /// Holds every rewrite and delete of this family's objects off until the
    /// guard drops; waits for the ones in flight. See `maintenance`.
    pub async fn pause_maintenance(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        self.maintenance.clone().write_owned().await
    }

    /// Holds commits off until the guard drops; waits for the ones in
    /// flight. See `declaring`.
    pub async fn hold_commits(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        self.declaring.clone().write_owned().await
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
        let _declaring = self.declaring.read().await;
        let others: Vec<Arc<KvFile>> = self
            .open_files()
            .into_iter()
            .filter(|f| !Arc::ptr_eq(f, last) && f.holds(txn))
            .collect();

        // Concurrently: they are independent objects and the commit's latency is
        // one round trip plus the slowest of these, not their sum.
        let results = futures::future::join_all(others.iter().map(|f| f.flush(None))).await;
        for result in results {
            if let Err(e) = result {
                // Nothing declared it, so it is rolled back, definitely.
                self.abort(txn);
                return Err(e);
            }
        }
        match last.flush_commit(txn).await {
            Ok(()) => {
                // The transaction is over everywhere, so no file's compaction
                // has to keep avoiding the range it was writing. Every open
                // file, not just the ones flushed here: one that spilled its
                // records earlier is still holding the transaction open.
                for file in self.open_files() {
                    let mut state = file.state.write();
                    state.open_txns.remove(&txn);
                    state.undo.remove(&txn);
                }
                Ok(())
            }
            Err(CommitFailure {
                error,
                rolled_back,
            }) => {
                // Rolled back and forgotten, so it no longer holds compaction
                // and retirement off its files for the life of the process.
                // In doubt, it stays open: folding its bytes could make
                // durable what the caller was told failed, and forgetting it
                // could lose what in fact committed.
                if rolled_back {
                    self.abort(txn);
                }
                Err(error)
            }
        }
    }

    /// Rolls `txn` back in every open file. For a transaction nothing has
    /// declared committed, or whose declaration a stored rollback overrides.
    fn abort(&self, txn: u64) {
        for file in self.open_files() {
            file.abort_local(txn);
        }
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
    /// What each delta in `deltas` holds and declares, for the [`Ledger`].
    delta_txns: HashMap<u64, DeltaTxns>,
    /// The lowest length a truncate cut the file to while a compaction was in
    /// flight. The compaction's base length was worked out before it, so it is
    /// clamped to this at publish; otherwise the truncated tail comes back as
    /// base bytes the moment anything extends the file past it.
    truncated_during_compaction: Option<u64>,
    /// Rollbacks to declare in the next delta object. See `DeltaBatch::aborted`.
    aborting: Vec<(u64, u64)>,
    /// How to take each open transaction's writes back out of the live
    /// overlay, should it be rolled back. Dropped when it commits.
    undo: HashMap<u64, Vec<Undo>>,
}

/// One write of an open transaction, as [`KvFile::abort_local`] reverses it.
enum Undo {
    /// An append into space the live overlay had nothing in. Removing it
    /// uncovers what was beneath: nothing anyone references. Contiguous
    /// appends of one transaction share an entry.
    Fresh { start: u64, end: u64 },
    /// A write over bytes that may matter: put back what the live overlay held
    /// there (`prior`, clipped spans; a gap is base or frozen bytes), but only
    /// if the range still holds what this write put there (`crc`) — a newer
    /// write by someone else wins.
    Overwrite {
        start: u64,
        end: u64,
        crc: u32,
        prior: Vec<(u64, Vec<u8>)>,
    },
}

pub(crate) struct KvFile {
    store: Arc<dyn KvStore>,
    config: Arc<KvConfig>,
    cache: Arc<ChunkCache>,
    ledger: Arc<Ledger>,
    id: u64,
    name: String,
    /// `name`, shared with the [`Ledger`] entries this file registers.
    ledger_name: Arc<str>,
    /// Highest generation handed out or found in the store. Shared with any
    /// scratch file made from this one.
    gen_floor: Arc<AtomicU64>,
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
    /// The family's `maintenance` gate, read around every rewrite or delete
    /// of an object.
    maintenance: Arc<tokio::sync::RwLock<()>>,
    /// Shared by readers, because every read of the file takes it — once per
    /// row on the scan path — and a scan's workers serialising on a mutex
    /// here were parking each other on a cache-resident table. Nothing reads
    /// through it with interior mutability: the overlays' read methods take
    /// `&self`.
    state: RwLock<State>,
    /// Serialises delta writes so sequence order is store order.
    flush: tokio::sync::Mutex<()>,
    compacting: AtomicBool,
    service: AtomicBool,
    dropped: AtomicBool,
    /// End of the last read, for spotting a sequential scan.
    read_cursor: AtomicU64,
    /// Highest offset read-ahead has already asked for.
    prefetched_to: AtomicU64,
    /// Times the base under the overlays changed: each compaction that
    /// published, bumped under the state lock as the frozen overlay is retired,
    /// and each `adopt`, bumped under it as the generation is replaced. See
    /// [`KvFile::read_at_inner`].
    folds: AtomicU64,
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
        let (meta, batches, recovered_next_seq, max_gen) = match recovery {
            Some(r) => (r.meta, r.batches, r.next_seq, r.max_gen),
            None => (Meta::default(), Vec::new(), 0, 0),
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
            delta_txns: HashMap::new(),
            truncated_during_compaction: None,
            aborting: Vec::new(),
            undo: HashMap::new(),
        };

        let generation = meta.base_gen;
        let mut len = meta.base_len;
        let mut base_len = meta.base_len;
        let mut next_seq = meta.compacted_upto.max(recovered_next_seq);

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
            // Already registered in the ledger by `KvFamily::open`.
            state.delta_txns.insert(batch.seq, DeltaTxns::of(batch));
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
            ledger: family.ledger.clone(),
            maintenance: family.maintenance.clone(),
            id,
            name: name.to_string(),
            ledger_name: Arc::from(name),
            gen_floor: Arc::new(AtomicU64::new(generation.max(max_gen))),
            root: ArcSwap::from_pointee(root),
            generation: AtomicU64::new(generation),
            len: AtomicU64::new(len),
            base_len: AtomicU64::new(base_len),
            next_seq: Arc::new(AtomicU64::new(next_seq)),
            is_temp: false,
            state: RwLock::new(state),
            flush: tokio::sync::Mutex::new(()),
            compacting: AtomicBool::new(false),
            service: AtomicBool::new(false),
            dropped: AtomicBool::new(false),
            read_cursor: AtomicU64::new(u64::MAX),
            prefetched_to: AtomicU64::new(0),
            folds: AtomicU64::new(0),
        }))
    }

    /// Whether this file is holding unstored records for `txn`.
    fn holds(&self, txn: u64) -> bool {
        let state = self.state.read();
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
    ///
    /// Every fetch goes through [`ChunkCache::fetch`], so a page some other
    /// reader — or the read-ahead — is already fetching is waited for rather
    /// than asked for again. A chunk already on its way covers every page of
    /// it, so it is joined even when this batch alone would have asked for a
    /// few pages by range.
    async fn pages(&self, wanted: &[u64], base_len: u64) -> io::Result<HashMap<u64, Arc<Vec<u8>>>> {
        let generation = self.generation.load(Ordering::Acquire);
        let mut out: HashMap<u64, Arc<Vec<u8>>> = HashMap::with_capacity(wanted.len());
        let mut missing_by_chunk: HashMap<u64, Vec<u64>> = HashMap::new();

        let ids = wanted.iter().map(|&page| ChunkId {
            file: self.id,
            generation,
            index: page,
        });
        self.cache.get_each(ids, |id, found| match found {
            Some(bytes) => {
                out.insert(id.index, bytes);
            }
            None => missing_by_chunk
                .entry(self.chunk_of_page(id.index))
                .or_default()
                .push(id.index),
        });
        if missing_by_chunk.is_empty() {
            return Ok(out);
        }

        let root = self.root();
        let pages_per_chunk = self.config.pages_per_chunk();

        enum Fetch {
            /// A fetch already in flight for this chunk, which covers the pages
            /// wanted from it.
            Join(PendingFetch),
            /// The whole chunk object, splitting into pages on arrival.
            Chunk { chunk: u64 },
            /// One page, as a ranged read.
            Page { chunk: u64, page: u64 },
        }

        let mut work: Vec<Fetch> = Vec::new();
        for (chunk, mut pages) in missing_by_chunk {
            let whole = FetchKey {
                file: self.id,
                generation,
                index: chunk,
                whole: true,
            };
            if let Some(pending) = self.cache.in_flight(whole) {
                work.push(Fetch::Join(pending));
                continue;
            }
            pages.sort_unstable();
            if pages.len() as u64 >= self.config.chunk_fetch_pages.min(pages_per_chunk) {
                work.push(Fetch::Chunk { chunk });
            } else {
                for page in pages {
                    work.push(Fetch::Page { chunk, page });
                }
            }
        }

        // Started a group at a time, so one batch never has more than the
        // store's width of requests of its own in flight.
        let width = self.store.concurrency().max(1);
        let mut work = work.into_iter().peekable();
        while work.peek().is_some() {
            let group: Vec<PendingFetch> = work
                .by_ref()
                .take(width)
                .map(|item| match item {
                    Fetch::Join(pending) => pending,
                    Fetch::Chunk { chunk } => self.fetch_chunk(&root, generation, chunk, base_len),
                    Fetch::Page { chunk, page } => self.fetch_page(&root, generation, chunk, page),
                })
                .collect();
            for outcome in futures::future::join_all(group).await {
                let fetched = outcome.map_err(|e| io::Error::new(e.kind(), e.to_string()))?;
                for (page, bytes) in fetched.iter() {
                    out.insert(*page, bytes.clone());
                }
            }
        }
        Ok(out)
    }

    /// Starts, or joins, the fetch of a whole chunk object.
    fn fetch_chunk(&self, root: &str, generation: u64, chunk: u64, base_len: u64) -> PendingFetch {
        let key = FetchKey {
            file: self.id,
            generation,
            index: chunk,
            whole: true,
        };
        self.cache.fetch(
            key,
            chunk_request(
                self.store.clone(),
                layout::chunk_key(root, generation, chunk),
                chunk * self.config.pages_per_chunk(),
                self.config.page_size,
                base_len,
            ),
        )
    }

    /// Starts, or joins, the ranged fetch of one page.
    fn fetch_page(&self, root: &str, generation: u64, chunk: u64, page: u64) -> PendingFetch {
        let key = FetchKey {
            file: self.id,
            generation,
            index: page,
            whole: false,
        };
        let store = self.store.clone();
        let object = layout::chunk_key(root, generation, chunk);
        let page_size = self.config.page_size;
        let within = (page - chunk * self.config.pages_per_chunk()) * page_size;
        self.cache.fetch(key, async move {
            Ok(match store.get_range(&object, within, page_size as usize).await? {
                Some(bytes) => vec![(page, Arc::new(bytes))],
                None => Vec::new(),
            })
        })
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

        // A compaction can finish between fetching the base and applying the
        // overlays. Base bytes fetched before its chunks were rewritten, with
        // the frozen overlay that covered them retired before they are
        // applied, are bytes the file no longer holds — so if one published in
        // between, read again. The retry cannot see the old base: the fold
        // invalidated those pages, and a fetch that was already in flight is
        // neither joined nor cached after that (see `ChunkCache`).
        //
        // `adopt` replaces the whole file the same way, so the length is read
        // under the lock too: a read sized by the replaced file would return
        // its tail.
        let (n, end) = loop {
            let (folds, n, end) = {
                let state = self.state.read();
                let len = self.len.load(Ordering::Acquire);
                if position >= len {
                    return Ok(0);
                }
                let n = dst.len().min((len - position) as usize);
                let end = position + n as u64;
                // The common case for a freshly appended row: the live
                // overlay has all of it, so nothing is asked of the store.
                if state.frozen.is_none() && state.live.covers(position, end) {
                    state.live.apply(position, &mut dst[..n]);
                    return Ok(n);
                }
                (self.folds.load(Ordering::Acquire), n, end)
            };

            self.fill_from_base(position, &mut dst[..n]).await?;
            let state = self.state.read();
            if self.folds.load(Ordering::Acquire) != folds {
                continue;
            }
            if let Some(frozen) = &state.frozen {
                frozen.apply(position, &mut dst[..n]);
            }
            state.live.apply(position, &mut dst[..n]);
            break (n, end);
        };
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
            let state = self.state.read();
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
    ///
    /// The engine's row fetch tries this first for every row, so it is the hot
    /// path of a scan over a resident table. One read of the state lock and one
    /// of the cache's for the whole batch: it used to take the cache's lock
    /// twice per page — once to check, once to copy — and with eight workers on
    /// one table those shared lock words were most of the cost of a row.
    fn try_read_many_sync(&self, reads: &mut [(u64, &mut [u8])]) -> bool {
        let page_size = self.config.page_size;
        let state = self.state.read();
        // Under the lock, so they describe the same file as the overlays:
        // `adopt` replaces all of them inside it.
        let len = self.len.load(Ordering::Acquire);
        let base_len = self.base_len.load(Ordering::Acquire);
        let generation = self.generation.load(Ordering::Acquire);

        // Checked in full before anything is copied, so the ordinary "not
        // resident" answer leaves the caller's buffers untouched.
        let mut ids: smallvec::SmallVec<[ChunkId; 8]> = smallvec::SmallVec::new();
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
            for page in self.page_of(*position)..=self.page_of(covered_end - 1) {
                let id = ChunkId {
                    file: self.id,
                    generation,
                    index: page,
                };
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
        }

        // Every page the batch needs, borrowed under one read of the cache's
        // lock, so none can be evicted between the check and the copy.
        self.cache
            .read_resident(&ids, |pages| {
                let page_bytes = |page: u64| -> &[u8] {
                    let at = ids
                        .iter()
                        .position(|id| id.index == page)
                        .expect("every page a read needs was collected above");
                    pages[at]
                };
                for (position, buf) in reads.iter_mut() {
                    if buf.is_empty() {
                        continue;
                    }
                    let end = *position + buf.len() as u64;
                    // Wholly in the live overlay: the base underneath is
                    // irrelevant, and was not required to be resident.
                    if !(state.frozen.is_none() && state.live.covers(*position, end)) {
                        let covered_end = end.min(base_len);
                        if *position < covered_end {
                            for page in self.page_of(*position)..=self.page_of(covered_end - 1) {
                                let page_start = page * page_size;
                                let copy_start = page_start.max(*position);
                                let copy_end = (page_start + page_size).min(covered_end);
                                if copy_end <= copy_start {
                                    continue;
                                }
                                let dst_at = (copy_start - *position) as usize;
                                let n = (copy_end - copy_start) as usize;
                                let bytes = page_bytes(page);
                                let src_at = (copy_start - page_start) as usize;
                                let available = bytes.len().saturating_sub(src_at);
                                let copied = n.min(available);
                                buf[dst_at..dst_at + copied]
                                    .copy_from_slice(&bytes[src_at..src_at + copied]);
                                if copied < n {
                                    // Shorter than the page: a hole, which a
                                    // file reads back as zeros.
                                    buf[dst_at + copied..dst_at + n].fill(0);
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
                    }
                    state.live.apply(*position, buf);
                }
            })
            .is_some()
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
                // Skipped only when every page still ahead of the reader in
                // this chunk is resident. Testing the chunk's first page alone
                // skipped the chunk the reader was already in whenever its first
                // page had come in by a ranged read, and the reader then fetched
                // the rest of it one page per request.
                let first_page = from.max(chunk * chunk_size) / page_size;
                let end_page = ((chunk + 1) * chunk_size).min(base_len).div_ceil(page_size);
                let resident = (first_page..end_page).all(|page| {
                    cache.contains(ChunkId {
                        file,
                        generation,
                        index: page,
                    })
                });
                if resident {
                    continue;
                }
                // Through the cache's fetch, so a reader that reaches this chunk
                // while it is on its way waits for it instead of fetching it
                // again — and a read-ahead that reaches a chunk a reader is
                // already fetching waits for that. One chunk at a time, as
                // before: read-ahead is a trickle ahead of the reader, not a
                // burst. A failure is the reader's to see, not this task's.
                let fetch = cache.fetch(
                    FetchKey {
                        file,
                        generation,
                        index: chunk,
                        whole: true,
                    },
                    chunk_request(
                        store.clone(),
                        layout::chunk_key(&root, generation, chunk),
                        chunk * pages_per_chunk,
                        page_size,
                        base_len,
                    ),
                );
                let _ = fetch.await;
            }
        });
    }

    // ---- writes ------------------------------------------------------------

    /// Records a change and returns the transaction it belongs to, which is the
    /// token [`StorageIO::commit_durable`] takes.
    fn stage(&self, txn: u64, record: Record, fresh: bool) -> u64 {
        let mut state = self.state.write();
        if record.kind == RecordKind::Write {
            if txn != 0 && !record.payload.is_empty() {
                let start = record.offset;
                let end = start + record.payload.len() as u64;
                let prior = state.live.spans_within(start, end);
                let list = state.undo.entry(txn).or_default();
                if fresh && prior.is_empty() {
                    match list.last_mut() {
                        Some(Undo::Fresh { end: last, .. }) if *last == start => *last = end,
                        _ => list.push(Undo::Fresh { start, end }),
                    }
                } else {
                    list.push(Undo::Overwrite {
                        start,
                        end,
                        crc: crc32fast::hash(&record.payload),
                        prior,
                    });
                }
            }
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
            self.stage(txn, Record::write(txn, offset, bytes.to_vec()), true);
        }
        offset
    }

    fn write_at(&self, txn: u64, position: u64, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let end = position + bytes.len() as u64;
        self.stage(txn, Record::write(txn, position, bytes.to_vec()), false);
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
        self.write_batch(commit).await.map_err(|f| f.error)
    }

    /// Writes the batch that declares `txn` committed, and says, if it fails,
    /// whether the transaction is now definitely rolled back.
    async fn flush_commit(&self, txn: u64) -> Result<(), CommitFailure> {
        let _serialised = self.flush.lock().await;
        self.write_batch(Some(txn)).await
    }

    /// The body of [`Self::flush_locked`]. Caller holds the flush lock.
    async fn write_batch(&self, commit: Option<u64>) -> Result<(), CommitFailure> {
        let (records, committed, aborted, seq, base_gen, reopen) = {
            let mut state = self.state.write();
            let mut reopen = None;
            if let Some(txn) = commit {
                state.committing.push(txn);
                reopen = state.open_txns.remove(&txn).map(|low| (txn, low));
            }
            if state.pending.is_empty()
                && state.committing.is_empty()
                && state.aborting.is_empty()
            {
                return Ok(());
            }
            let records = std::mem::take(&mut state.pending);
            state.pending_bytes = 0;
            let committed = std::mem::take(&mut state.committing);
            let aborted = std::mem::take(&mut state.aborting);
            // Allocated under the flush lock, so sequence order is store order.
            let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
            (
                records,
                committed,
                aborted,
                seq,
                self.generation.load(Ordering::Acquire),
                reopen,
            )
        };

        let batch = DeltaBatch {
            seq,
            base_gen,
            committed,
            records,
            aborted,
        };
        let key = layout::delta_key(&self.root(), seq);
        let encoded = batch.encode();
        let put = self.store.put(&key, encoded).await;
        // A put that reported failure may have landed anyway: the request
        // reached the store and only the answer was lost. For a batch that
        // declares a commit that is the difference between a committed
        // transaction and a rolled-back one, so ask the store. Still under the
        // flush lock, so no compaction can freeze the transaction's bytes
        // while it is in neither state.
        let mut rolled_back = false;
        let put = match (put, commit) {
            (Err(e), Some(txn)) => match self.resolve_in_doubt(&key, seq, txn, base_gen).await {
                InDoubt::Landed => Ok(()),
                InDoubt::RolledBack => {
                    rolled_back = true;
                    Err(e)
                }
                InDoubt::Unknown => Err(e),
            },
            (put, _) => put,
        };
        match put {
            Ok(()) => {
                let txns = DeltaTxns::of(&batch);
                self.ledger.register(&self.ledger_name, seq, &txns.records);
                let mut state = self.state.write();
                state.deltas.push(seq);
                state.delta_txns.insert(seq, txns);
                Ok(())
            }
            Err(e) => {
                // Nothing was made durable, so the records go back on the queue
                // and the transactions go back to being uncommitted. Reporting
                // success here is the failure mode this whole trait exists to
                // prevent.
                //
                // Uncommitted means the declarations are *not* queued again.
                // They used to be, so the next flush — the background service's,
                // a few hundred milliseconds later — declared committed a
                // transaction whose commit had just returned an error, and
                // whose space the caller had already handed back. And the
                // transaction is open again, so no compaction folds its bytes
                // into a base chunk, which would make them durable the same way.
                //
                // Reopened even when it is definitely rolled back: the caller
                // takes its bytes back out (`KvFile::abort_local`) after this
                // returns, and until then a compaction must not fold them.
                let mut state = self.state.write();
                let mut restored = batch.records;
                restored.append(&mut state.pending);
                state.pending_bytes = restored.iter().map(|r| r.weight()).sum();
                state.pending = restored;
                let mut aborting = batch.aborted;
                aborting.append(&mut state.aborting);
                state.aborting = aborting;
                if let Some((txn, low)) = reopen {
                    let entry = state.open_txns.entry(txn).or_insert(low);
                    *entry = (*entry).min(low);
                }
                Err(CommitFailure {
                    error: e,
                    rolled_back,
                })
            }
        }
    }

    /// After the batch at `seq` declaring `txn` failed to put: did it land?
    ///
    /// If it is there, the transaction is committed. If it is not, it may
    /// still arrive — the store can finish a request the client gave up on —
    /// so the rollback is made durable in a batch of its own, which recovery
    /// lets override the declaration. Only once that is stored is the
    /// transaction definitely rolled back; failing either request leaves it
    /// unknown, and the transaction stays open, as it did before this existed.
    async fn resolve_in_doubt(&self, key: &str, seq: u64, txn: u64, base_gen: u64) -> InDoubt {
        match self.store.get(key).await {
            Ok(Some(bytes)) => {
                if let Ok(Some(found)) = DeltaBatch::decode(&bytes)
                    && found.seq == seq
                    && found.committed.contains(&txn)
                {
                    return InDoubt::Landed;
                }
            }
            Ok(None) => {}
            Err(e) => {
                warn!(
                    "kv: commit of transaction {txn} on {} is in doubt; it stays open: {e}",
                    self.name
                );
                return InDoubt::Unknown;
            }
        }
        let marker_seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
        let marker = DeltaBatch {
            seq: marker_seq,
            base_gen,
            committed: Vec::new(),
            records: Vec::new(),
            aborted: vec![(txn, seq)],
        };
        match self
            .store
            .put(&layout::delta_key(&self.root(), marker_seq), marker.encode())
            .await
        {
            Ok(()) => {
                let mut state = self.state.write();
                state.deltas.push(marker_seq);
                state.delta_txns.insert(marker_seq, DeltaTxns::of(&marker));
                InDoubt::RolledBack
            }
            Err(e) => {
                warn!(
                    "kv: could not record the rollback of transaction {txn} on {}; it stays \
                     open: {e}",
                    self.name
                );
                InDoubt::Unknown
            }
        }
    }

    /// Takes every trace of `txn` out of this file: its unstored records, its
    /// open-transaction entry (so compaction and retirement stop waiting on
    /// it), and its bytes in the live overlay, which go back to what they were.
    ///
    /// Records it already wrote out (spilled) stay in their deltas, inert —
    /// nothing ever declares the transaction — and retire with them.
    fn abort_local(&self, txn: u64) {
        let mut state = self.state.write();
        state.pending.retain(|r| r.txn != txn);
        state.pending_bytes = state.pending.iter().map(|r| r.weight()).sum();
        state.committing.retain(|&t| t != txn);
        state.open_txns.remove(&txn);
        let Some(undo) = state.undo.remove(&txn) else {
            return;
        };
        for entry in undo.into_iter().rev() {
            match entry {
                Undo::Fresh { start, end } => state.live.remove_range(start, end),
                Undo::Overwrite {
                    start,
                    end,
                    crc,
                    prior,
                } => {
                    let still_ours = state.live.covers(start, end) && {
                        let mut now = vec![0u8; (end - start) as usize];
                        state.live.apply(start, &mut now);
                        crc32fast::hash(&now) == crc
                    };
                    if !still_ours {
                        // A later write by someone else; it stands.
                        continue;
                    }
                    state.live.remove_range(start, end);
                    for (at, bytes) in prior {
                        state.live.insert(at, &bytes);
                    }
                }
            }
        }
        // A reader that fetched base bytes before this and applies the overlay
        // after it reads again, as it does across a compaction.
        self.folds.fetch_add(1, Ordering::AcqRel);
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
        // Before the flag, so a compaction waiting out a pause holds nothing
        // `adopt` waits on.
        let _maintenance = self.maintenance.read().await;
        if self.compacting.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let result = self.compact_inner().await;
        if result.is_err() {
            self.abandon_freeze();
        }
        self.compacting.store(false, Ordering::Release);
        result
    }

    /// Undoes the freeze of a compaction that failed: everything frozen goes
    /// back to the live overlay, under anything written since, so the next
    /// pass can try again.
    ///
    /// A failure used to leave the frozen overlay installed, and every later
    /// compaction of the file then declined for the life of the process while
    /// the overlay and the delta count grew. Unfreezing loses nothing: the
    /// frozen bytes are committed, the deltas holding them are still in the
    /// store (retiring them is the last step, after the overlay is gone), and a
    /// chunk already rewritten holds the same bytes.
    fn abandon_freeze(&self) {
        let mut state = self.state.write();
        let Some(frozen) = state.frozen.take() else {
            return;
        };
        state.reserved_seq = None;
        // The truncate already cut `base_len` itself; nothing to clamp.
        state.truncated_during_compaction = None;
        let newer = std::mem::replace(&mut state.live, frozen);
        for (start, bytes) in newer.spans_iter() {
            state.live.insert(start, bytes);
        }
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
        let (safe_end, generation, base_len, len, reserved_seq, may_retire) = {
            let _serialised = self.flush.lock().await;
            let mut state = self.state.write();
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
            // Whether the deltas written so far may be retired by this pass:
            // only if the freeze took every byte they describe. Whatever it
            // left in the live overlay — an open transaction's bytes, and the
            // committed bytes above them — exists in those deltas and nowhere
            // else. This used to be decided at publish, from whether any
            // transaction was open *then*; one that committed in between
            // retired the deltas still holding the bytes left behind here, and
            // a restart lost them.
            let may_retire = state.live.is_empty();
            state.frozen = Some(frozen);
            state.truncated_during_compaction = None;
            let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
            state.reserved_seq = Some(seq);
            (
                len.min(lowest_open),
                self.generation.load(Ordering::Acquire),
                self.base_len.load(Ordering::Acquire),
                len,
                seq,
                may_retire,
            )
        };

        // ---- choose ----
        let mut candidates: Vec<u64> = Vec::new();
        {
            let state = self.state.read();
            let frozen = state.frozen.as_ref().expect("just installed");
            // Every chunk a span touches, once. Sorted and deduplicated rather
            // than `contains`-checked per push, which was quadratic in the
            // number of chunks; the order is free to change because nothing
            // below depends on it and `candidates` is sorted at the end.
            let mut seen: Vec<u64> = Vec::new();
            for (start, end) in frozen.span_bounds() {
                let first = start / chunk_size;
                let last = (end - 1) / chunk_size;
                seen.extend(first..=last);
            }
            seen.sort_unstable();
            seen.dedup();
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
        // Ranges now in base chunks. They stay in the frozen overlay until the
        // publish below; see there.
        let mut folded: Vec<(u64, u64)> = Vec::new();
        for chunk in &candidates {
            let start = chunk * chunk_size;
            let chunk_end = (start + chunk_size).min(len);
            if chunk_end <= start {
                continue;
            }
            let mut buffer = vec![0u8; (chunk_end - start) as usize];
            self.fill_from_base(start, &mut buffer).await?;
            {
                let state = self.state.read();
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
            folded.push((start, chunk_end));
        }

        // ---- publish ----
        //
        // Order is load-bearing. The leftovers go out first, then the metadata
        // that makes them authoritative, and only then are the old deltas
        // deleted. Dying anywhere in here leaves a state that recovers: base
        // chunks written but not published are simply re-covered by the deltas
        // that are still there, because a record is physical redo and applying
        // it twice is applying it once.
        //
        // The frozen overlay stays in place — and readable — until the very
        // end, when the base it was folded into is published. It used to be
        // emptied as each chunk was folded and taken out entirely here, which
        // for the two round trips below left folded bytes past the old
        // `base_len` in neither the overlay nor the base (a read saw zeros),
        // and the leftovers in neither the overlay nor any object (a read saw
        // the older bytes they replaced). Keeping it costs nothing: a reader
        // applies it over the base, and over either the old base or the new
        // one that gives the same bytes.
        let _serialised = self.flush.lock().await;
        let (leftovers, carried) = {
            let state = self.state.read();
            let leftovers = unfolded(state.frozen.as_ref().expect("held"), &folded);
            // Declarations the retired deltas make that another file's
            // records still depend on. See [`Ledger`].
            let carried = if may_retire {
                let declared = state
                    .deltas
                    .iter()
                    .filter(|&&s| s < reserved_seq)
                    .filter_map(|s| state.delta_txns.get(s))
                    .flat_map(|d| d.declared.iter().copied());
                let name = &*self.ledger_name;
                self.ledger
                    .still_needed(declared, |n, s| n == name && s < reserved_seq)
            } else {
                Vec::new()
            };
            // A truncate since the freeze cut the file below what the chunks
            // above were built to describe.
            if let Some(cut) = state.truncated_during_compaction {
                new_base_len = new_base_len.min(cut);
            }
            (leftovers, carried)
        };
        let wrote_leftovers = !leftovers.is_empty() || !carried.is_empty();

        if wrote_leftovers {
            // Transaction 0: already committed, by definition — everything in
            // the frozen overlay was committed before the freeze took it.
            let records: Vec<Record> = leftovers
                .spans_iter()
                .map(|(start, bytes)| Record::write(0, start, bytes.to_vec()))
                .collect();
            let batch = DeltaBatch {
                seq: reserved_seq,
                base_gen: generation,
                committed: carried.clone(),
                records,
                aborted: Vec::new(),
            };
            self.store
                .put(&layout::delta_key(&root, reserved_seq), batch.encode())
                .await?;
        }

        self.base_len.store(new_base_len, Ordering::Release);
        let retire = may_retire;
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
                self.state.read().compacted_upto
            },
        };
        self.store
            .put(&layout::meta_key(&root), meta.encode())
            .await?;

        let retired: Vec<String> = {
            let mut state = self.state.write();
            // An older delta may still hold records of a transaction that is
            // open right now. Deleting it would leave a commit declaration with
            // nothing to commit, so retirement waits for a quiet moment; the
            // chunks and the leftovers are published either way, so the pass
            // still made progress.
            //
            // Decided at the freeze, not here: see `may_retire`.
            let (old, keep): (Vec<u64>, Vec<u64>) = if retire {
                state.deltas.iter().partition(|&&s| s < reserved_seq)
            } else {
                (Vec::new(), state.deltas.clone())
            };
            state.deltas = keep;
            for seq in &old {
                if let Some(txns) = state.delta_txns.remove(seq) {
                    self.ledger.retire(&self.ledger_name, *seq, &txns.records);
                }
            }
            if wrote_leftovers {
                state.deltas.push(reserved_seq);
                state.delta_txns.insert(
                    reserved_seq,
                    DeltaTxns {
                        records: Vec::new(),
                        declared: carried,
                        aborted: Vec::new(),
                    },
                );
            }
            state.truncated_during_compaction = None;
            state.deltas.sort_unstable();
            state.deltas.dedup();
            if retire {
                state.compacted_upto = reserved_seq;
            }
            // The base now describes every folded byte, so the frozen overlay
            // can go. Whatever it holds beyond those stays readable: it goes
            // back to the live overlay, under anything written since the
            // freeze. Taken from the overlay as it is now rather than from the
            // copy written out above, because a truncate in between has cut
            // this one and must not be undone by merging the copy back.
            let frozen = state.frozen.take().expect("held");
            state.reserved_seq = None;
            let remaining = unfolded(&frozen, &folded);
            // A reader that fetched its base bytes before the fold and applies
            // the overlays after this point would find the folded bytes in
            // neither. `read_at_inner` sees this count move and reads again.
            self.folds.fetch_add(1, Ordering::AcqRel);
            if !remaining.is_empty() {
                let live = std::mem::replace(&mut state.live, Overlay::new());
                let mut rebuilt = remaining;
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
        // It deletes the chunks past the new end.
        let _maintenance = self.maintenance.read().await;
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
        // Held across the cut and the write-out, so a compaction's publish —
        // which takes it too — happens wholly before this (and its base length
        // is then clamped here) or wholly after (and clamps itself to
        // `truncated_during_compaction`). Without it, a publish storing the
        // base length it computed before the truncate undid the clamp, and the
        // truncated tail read back as base bytes once the file grew past it.
        let _serialised = self.flush.lock().await;
        let generation = self.generation.load(Ordering::Acquire);
        let root = self.root();
        let chunk_size = self.config.chunk_size;
        let base_len = self.base_len.load(Ordering::Acquire);

        {
            let mut state = self.state.write();
            state.live.truncate(new_len);
            if let Some(frozen) = &mut state.frozen {
                frozen.truncate(new_len);
                let cut = state.truncated_during_compaction.map_or(new_len, |c| c.min(new_len));
                state.truncated_during_compaction = Some(cut);
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
        self.flush_locked(None).await?;
        drop(_serialised);

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
    ///
    /// Above everything handed out before, not just above the live one: two
    /// scratch files made in a row — the first abandoned, its deltas left in
    /// the store — used to share a generation, and adopting the second made
    /// the first one's deltas describe the live file.
    fn next_generation(&self) -> u64 {
        let live = self.generation.load(Ordering::Acquire);
        self.gen_floor.fetch_max(live, Ordering::AcqRel);
        self.gen_floor.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Makes `temp`'s generation the live one.
    ///
    /// The metadata object is written *before* the old generation's chunks are
    /// deleted. Dying in between orphans objects, which costs storage; dying the
    /// other way round would delete the chunks the live metadata still points
    /// at, which costs the table.
    async fn adopt(&self, temp: &Arc<KvFile>) -> io::Result<()> {
        // No compaction of either file may be in flight. One that froze the
        // old generation and publishes after this would write a META naming
        // that generation — whose chunks this is about to delete — and store
        // its base length over the new one: a table that reads correctly until
        // the restart and then comes back as the replaced bytes, or as zeros.
        // Taken before the flush locks, in the order `compact` takes them,
        // and the maintenance gate before both, as `compact` takes it.
        let _maintenance = self.maintenance.read().await;
        let _quiet = CompactionGuard::acquire(self).await;
        let _quiet_temp = CompactionGuard::acquire(temp).await;
        let _one = self.flush.lock().await;
        let _two = temp.flush.lock().await;

        // The old generation's deltas are about to be deleted, and with them
        // any commit declaration they make. The ones another file's records
        // still depend on go out again in the scratch file's batch, which
        // becomes this file's. See [`Ledger`].
        let carried = {
            let state = self.state.read();
            let declared = state
                .delta_txns
                .values()
                .flat_map(|d| d.declared.iter().copied());
            let name = &*self.ledger_name;
            self.ledger
                .still_needed(declared, |n, s| n == name && state.deltas.contains(&s))
        };
        if !carried.is_empty() {
            temp.state.write().committing.extend(carried);
        }
        // Rollbacks too: the batch each one overrides may still arrive, and
        // the new META's watermark says nothing about the old sequences.
        let rollbacks: Vec<(u64, u64)> = self
            .state
            .read()
            .delta_txns
            .values()
            .flat_map(|d| d.aborted.iter().copied())
            .collect();
        if !rollbacks.is_empty() {
            temp.state.write().aborting.extend(rollbacks);
        }
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
            compacted_upto: temp.state.read().compacted_upto,
        };
        self.store
            .put(&layout::meta_key(&root), meta.encode())
            .await?;

        // The generation, the lengths and the overlays change together, under
        // the state lock, and `folds` moves with them. A reader holds that lock
        // while it decides what to read and again while it applies the
        // overlays, so it sees all of the old file or all of the new one, and
        // one whose base fetch straddled the swap reads again. Stored outside
        // the lock, a reader could pair the old generation's base bytes with
        // the new file's overlays — the new rows patched over the replaced
        // ones — or read the old file to its old, longer length.
        let (dead_deltas, dead_txns) = {
            let mut mine = self.state.write();
            let mut theirs = temp.state.write();
            // Read before the swap, or this names the scratch file's deltas —
            // the ones that must survive — instead of the originals.
            let dead = mine.deltas.clone();
            let dead_txns = std::mem::take(&mut mine.delta_txns);
            let old_len = self.len.load(Ordering::Acquire);
            let old_base_len = self.base_len.load(Ordering::Acquire);
            self.generation.store(new_generation, Ordering::Release);
            self.len
                .store(temp.len.load(Ordering::Acquire), Ordering::Release);
            self.base_len.store(base_len, Ordering::Release);
            std::mem::swap(&mut *mine, &mut *theirs);
            self.folds.fetch_add(1, Ordering::AcqRel);
            // The scratch handle now describes the replaced file, all of it.
            // Left naming the new generation, erasing it — what dropping a
            // scratch handle does — would sweep the live file's chunks.
            temp.generation.store(old_generation, Ordering::Release);
            temp.len.store(old_len, Ordering::Release);
            temp.base_len.store(old_base_len, Ordering::Release);
            temp.folds.fetch_add(1, Ordering::AcqRel);
            (dead, dead_txns)
        };
        for (seq, txns) in dead_txns {
            self.ledger.retire(&self.ledger_name, seq, &txns.records);
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
        let _maintenance = self.maintenance.read().await;
        let root = self.root();
        let prefix = if self.is_temp {
            // Scratch shares the real file's prefix. Sweeping it would take the
            // live file's objects with it.
            layout::generation_prefix(&root, self.generation.load(Ordering::Acquire))
        } else {
            format!("{root}/")
        };
        let listed = self.store.list(&prefix).await?;
        let mut keys: Vec<String> = listed.into_iter().map(|o| o.key).collect();
        if self.is_temp {
            // Its deltas are under the shared `d/` prefix; these are its own.
            let state = self.state.read();
            keys.extend(state.deltas.iter().map(|&s| layout::delta_key(&root, s)));
        }
        if !keys.is_empty() {
            self.store.delete(keys).await?;
        }
        let gone = std::mem::take(&mut self.state.write().delta_txns);
        for (seq, txns) in gone {
            self.ledger.retire(&self.ledger_name, seq, &txns.records);
        }
        self.cache.invalidate_file(self.id);
        Ok(())
    }
}

/// A failed commit: the error, and whether the transaction is definitely
/// rolled back (true) or might yet turn out committed (false).
struct CommitFailure {
    error: io::Error,
    rolled_back: bool,
}

/// What a store says about a declaring put that reported failure.
enum InDoubt {
    /// It is there: the transaction committed.
    Landed,
    /// It is not, and a durable rollback now overrides it if it ever arrives.
    RolledBack,
    /// The store could not be asked, or the rollback could not be stored.
    Unknown,
}

/// Holds a file's `compacting` flag for something that must not overlap a
/// compaction. While it is held, [`KvFile::compact`] declines rather than
/// waits, the same as it does for a compaction already running.
struct CompactionGuard<'a> {
    file: &'a KvFile,
}

impl<'a> CompactionGuard<'a> {
    /// Waits out a compaction in flight, then holds the flag. Polled: `adopt`
    /// is the only caller, it is rare, and a compaction is round trips long.
    async fn acquire(file: &'a KvFile) -> Self {
        while file.compacting.swap(true, Ordering::AcqRel) {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        Self { file }
    }
}

impl Drop for CompactionGuard<'_> {
    fn drop(&mut self) {
        self.file.compacting.store(false, Ordering::Release);
    }
}

/// `frozen` without the ranges a compaction folded into base chunks.
fn unfolded(frozen: &Overlay, folded: &[(u64, u64)]) -> Overlay {
    let mut rest = Overlay::new();
    for (start, bytes) in frozen.spans_iter() {
        rest.insert(start, bytes);
    }
    for &(start, end) in folded {
        rest.remove_range(start, end);
    }
    rest
}

/// The request behind a whole-chunk fetch: one GET, split into pages.
///
/// Pages at or past `base_len` are left out, as they always were: the object
/// can run past the base it describes after a truncate, and those bytes are not
/// the file's. Built as a free function so the read-ahead task, which outlives
/// any borrow of the file, can start the same fetch a reader would.
fn chunk_request(
    store: Arc<dyn KvStore>,
    object: String,
    first_page: u64,
    page_size: u64,
    base_len: u64,
) -> impl std::future::Future<Output = io::Result<Vec<(u64, Arc<Vec<u8>>)>>> + Send + 'static {
    async move {
        let Some(bytes) = store.get(&object).await? else {
            return Ok(Vec::new());
        };
        let mut pages = Vec::with_capacity(bytes.len().div_ceil(page_size as usize));
        // Split so a later point read of a neighbouring row is a cache hit
        // rather than a request.
        for (i, slice) in bytes.chunks(page_size as usize).enumerate() {
            let page = first_page + i as u64;
            if page * page_size >= base_len {
                break;
            }
            pages.push((page, Arc::new(slice.to_vec())));
        }
        Ok(pages)
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
            let state = self.file.state.read();
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

    fn defers_in_place_writes(&self) -> bool {
        // Staged under the transaction and stored by its commit.
        true
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

    async fn abort_txn(&self, token: u64) {
        if token != 0 {
            self.family.abort(token);
        }
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
        // The overlay check, the lengths, the generation and the page lookup
        // under one hold of the state lock: `adopt` swaps all of them inside
        // it, and a page of the new generation checked against the old
        // overlays (or the reverse) is a borrow of bytes the file does not
        // hold.
        let bytes = {
            let state = self.file.state.read();
            if end > self.file.len.load(Ordering::Acquire)
                || end > self.file.base_len.load(Ordering::Acquire)
            {
                return None;
            }
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
            let generation = self.file.generation.load(Ordering::Acquire);
            self.file.cache.get(ChunkId {
                file: self.file.id,
                generation,
                index: page,
            })?
        };
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
            ledger: file.ledger.clone(),
            maintenance: file.maintenance.clone(),
            id: self.family.next_file_id.fetch_add(1, Ordering::Relaxed),
            name: file.name.clone(),
            ledger_name: file.ledger_name.clone(),
            gen_floor: file.gen_floor.clone(),
            root: ArcSwap::from_pointee((*file.root()).clone()),
            generation: AtomicU64::new(generation),
            len: AtomicU64::new(0),
            base_len: AtomicU64::new(0),
            next_seq: file.next_seq.clone(),
            is_temp: true,
            state: RwLock::new(State {
                live: Overlay::new(),
                frozen: None,
                pending: Vec::new(),
                pending_bytes: 0,
                committing: Vec::new(),
                open_txns: HashMap::new(),
                deltas: Vec::new(),
                compacted_upto: 0,
                reserved_seq: None,
                delta_txns: HashMap::new(),
                truncated_during_compaction: None,
                aborting: Vec::new(),
                undo: HashMap::new(),
            }),
            flush: tokio::sync::Mutex::new(()),
            compacting: AtomicBool::new(false),
            service: AtomicBool::new(false),
            dropped: AtomicBool::new(false),
            read_cursor: AtomicU64::new(u64::MAX),
            prefetched_to: AtomicU64::new(0),
            folds: AtomicU64::new(0),
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
        if self.file.is_temp {
            // A scratch file shares the live file's *name*, and is in neither
            // the registry nor the erasing gates. Removing the name from the
            // registry here unregistered the live file — the next open of it
            // built a second, diverging copy — and a gate under the name would
            // hold up reopening a table nobody dropped. Its erase touches only
            // its own generation and deltas.
            let file = self.file.clone();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    if let Err(e) = file.erase().await {
                        error!("kv: could not erase scratch of {}: {e}", file.name);
                    }
                });
            }
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
                    let state = file.state.read();
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
