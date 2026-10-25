//! A table's B-tree, hash and path indexes at open: loaded from the
//! snapshot's INDEX sections when the open's proof passed, built from rows
//! otherwise (docs/persistent-indexes.md §4; slice B17).
//!
//! # Which it is
//!
//! [`plan`] decides at the open, from the proof and the setting
//! `index_snapshot.index_storage`:
//! - **load** when the setting is `snapshot`, the proof passed, the table
//!   has B-tree, hash or path indexes and no gin index (gin waits for phase
//!   3: such a table writes no INDEX section and rebuilds), and the snapshot
//!   holds one INDEX section per index, ordinals `0..n` in `for_schema`
//!   order;
//! - **build** otherwise, exactly as before B17.
//!
//! # The load
//!
//! Started by the open on a task of its own, single-flight per instance.
//! - **Writes wait for it.** The open raises the file set's [`LoadBarrier`]
//!   before it returns, and `gate::ticket` waits for the barrier before its
//!   slow path. A load runs only on a word the proof found `Clean`, so every
//!   write's first ticket takes the slow path, and none is issued before the
//!   load has finished or failed: the snapshot equals the data at open,
//!   and stays a valid base after `DIRTY` (§4).
//! - **Readers wait for it** up to `index_snapshot.load_wait_ms` (600), then
//!   scan (`Table::build_candidates`).
//! - **Loads queue** on one process-wide queue of
//!   `index_snapshot.max_concurrent_loads` slots, granted by rebuild cost
//!   (slots × indexes), the highest first: at startup every table and
//!   partition opens, and queues its load, before the first slot frees.
//! - **Each section** is read alone against its crc32c
//!   ([`snapshot::read_section`]), decoded ([`index_section::decode`]) into
//!   a checked stream, and only once every section decoded is any index
//!   touched: `LiveIndex::load_sorted` per index (hook C9), then
//!   `TableIndexes::set_ready_at(epoch)`.
//! - **Any error** (a read, a crc32c, a section's version or bytes, a
//!   refused stream, a missing or unexpected section) clears whatever was
//!   loaded, releases the barrier, and builds from rows as before
//!   (`build_indexes` under the open's `BuildToken`). The build is one pass
//!   for every index, so "unique and implicit first" (§4) has nothing to
//!   order: readiness is the table's, all at once.
//! - **Vouching** (§3 rule 2): a successful load vouches for the word when
//!   the open's FILTER load did and the word is still `Clean`; a failed one
//!   leaves it unvouched, so the next clean point rewrites the snapshot,
//!   and a version bump rebuilds once, then loads. A seal waits for a load
//!   in flight before it looks ([`super::seal::SealTarget::settle`]).
//!
//! # Superset
//!
//! A load either installs every captured entry of every index, or none and
//! the build runs. `load_sorted` adds and removes nothing, so beside a
//! write it is the union (B16), though writes wait. The captured stream
//! carries a presence bit only for a slot whose entries it carries (C3 reads
//! the bits before the sweep, B17), so a loaded index holds no slot it
//! cannot find.

use std::collections::BinaryHeap;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex;

use super::file_gate::{FileGate, blocking};
use super::gate::{BuildToken, Proof};
use super::gate_word::GateState;
use super::index_section;
use super::open_proof::Proven;
use super::snapshot::{self, SectionEntry, SectionKind};
use crate::core::index::TableIndexes;

/// The settings of the load (`index_snapshot.*`), process-wide, changed by
/// `SET CONFIG index_snapshot.<key> = …` and the server's flags.
pub mod settings {
    use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
    use std::time::Duration;

    /// What an open does with a snapshot's INDEX sections, and whether a
    /// seal writes them.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum IndexStorage {
        /// Indexes are rebuilt from rows at every open; seals write FILTER
        /// only. The default until phase 2's gate (owner's Q3).
        Rebuild,
        /// Seals write an INDEX section per B-tree, hash and path index, and
        /// an open whose proof passes loads them.
        Snapshot,
    }

    impl IndexStorage {
        pub fn parse(s: &str) -> Option<IndexStorage> {
            match s.trim().trim_matches('\'').to_ascii_lowercase().as_str() {
                "rebuild" => Some(IndexStorage::Rebuild),
                "snapshot" => Some(IndexStorage::Snapshot),
                _ => None,
            }
        }

        pub fn name(self) -> &'static str {
            match self {
                IndexStorage::Rebuild => "rebuild",
                IndexStorage::Snapshot => "snapshot",
            }
        }
    }

    static STORAGE: AtomicU8 = AtomicU8::new(0);
    static MAX_CONCURRENT_LOADS: AtomicU64 = AtomicU64::new(0);
    static LOAD_WAIT_MS: AtomicU64 = AtomicU64::new(600);

    /// `index_snapshot.index_storage`: `rebuild` (the default) or
    /// `snapshot`.
    pub fn index_storage() -> IndexStorage {
        match STORAGE.load(Ordering::Relaxed) {
            1 => IndexStorage::Snapshot,
            _ => IndexStorage::Rebuild,
        }
    }

    pub fn set_index_storage(s: IndexStorage) {
        STORAGE.store(s as u8, Ordering::Relaxed);
    }

    /// The setting for a file set in `dir`: the process's, or a test's
    /// override for its directory.
    pub fn index_storage_for(dir: Option<&std::path::Path>) -> IndexStorage {
        #[cfg(test)]
        if let Some(d) = dir
            && let Some(s) = super::storage_override::get(d)
        {
            return s;
        }
        let _ = dir;
        index_storage()
    }

    /// How many loads run at once, process-wide. 0 (unset) is half the
    /// cores, at least two.
    pub fn max_concurrent_loads() -> usize {
        match MAX_CONCURRENT_LOADS.load(Ordering::Relaxed) {
            0 => std::thread::available_parallelism().map_or(2, |n| (n.get() / 2).max(2)),
            n => n as usize,
        }
    }

    pub fn set_max_concurrent_loads(n: u64) {
        MAX_CONCURRENT_LOADS.store(n, Ordering::Relaxed);
        super::regrant();
    }

    /// How long a reader waits for a load in flight before it scans.
    pub fn load_wait() -> Duration {
        Duration::from_millis(LOAD_WAIT_MS.load(Ordering::Relaxed))
    }

    pub fn set_load_wait_ms(ms: u64) {
        LOAD_WAIT_MS.store(ms, Ordering::Relaxed);
    }
}

/// Test-only: `index_storage` for tables under one directory, so a test can
/// load snapshots without changing what every other test runs under.
#[cfg(test)]
pub(crate) mod storage_override {
    use std::path::{Path, PathBuf};

    use parking_lot::Mutex;

    use super::settings::IndexStorage;

    static OVERRIDES: Mutex<Vec<(PathBuf, IndexStorage)>> = Mutex::new(Vec::new());

    /// Removes the override when dropped.
    pub(crate) struct Guard(PathBuf);

    impl Drop for Guard {
        fn drop(&mut self) {
            OVERRIDES.lock().retain(|(d, _)| *d != self.0);
        }
    }

    pub(crate) fn set(dir: &Path, s: IndexStorage) -> Guard {
        OVERRIDES.lock().push((dir.to_path_buf(), s));
        Guard(dir.to_path_buf())
    }

    pub(super) fn get(dir: &Path) -> Option<IndexStorage> {
        OVERRIDES.lock().iter().find(|(d, _)| dir.starts_with(d)).map(|(_, s)| *s)
    }
}

// ---------------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------------

static LOADED_TOTAL: AtomicU64 = AtomicU64::new(0);
static BUILT_TOTAL: AtomicU64 = AtomicU64::new(0);
static FAILED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// `rdb_index_snapshot_index_loads_total`: opens whose indexes were loaded
/// from INDEX sections.
pub fn loaded_total() -> u64 {
    LOADED_TOTAL.load(Ordering::Relaxed)
}

/// `rdb_index_builds_total`: opens whose indexes were built from rows (no
/// snapshot to load, or a load that failed).
pub fn built_total() -> u64 {
    BUILT_TOTAL.load(Ordering::Relaxed)
}

/// `rdb_index_snapshot_load_failed_total`: loads that fell back to a build.
pub fn failed_total() -> u64 {
    FAILED_TOTAL.load(Ordering::Relaxed)
}

/// How an instance's indexes came to be, for tests and diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// Nothing yet: a load or a build is under way (or the table has none).
    Pending,
    /// Loaded from the snapshot's INDEX sections.
    Loaded,
    /// Built from rows: nothing to load.
    Built,
    /// Built from rows after a load failed.
    FellBack,
}

impl Origin {
    pub fn of(v: u8) -> Origin {
        match v {
            1 => Origin::Loaded,
            2 => Origin::Built,
            3 => Origin::FellBack,
            _ => Origin::Pending,
        }
    }
}

// ---------------------------------------------------------------------------
// The barrier writes and readers wait at
// ---------------------------------------------------------------------------

/// A file set's loads in flight. Writes wait for none to be left before
/// their first ticket's slow path (`gate::ticket`); readers wait up to
/// `load_wait`. Shared by the gates a RENAME or MOVE carries the word to.
#[derive(Debug, Default)]
pub struct LoadBarrier {
    inflight: AtomicUsize,
    done: tokio::sync::Notify,
}

/// One load in flight on a [`LoadBarrier`]; released on drop, on every path
/// (a panic included), so nothing waits on a load that is gone.
#[derive(Debug)]
pub struct LoadGuard(StdArc<LoadBarrier>);

impl Drop for LoadGuard {
    fn drop(&mut self) {
        if self.0.inflight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.done.notify_waiters();
        }
    }
}

impl LoadBarrier {
    /// A load starts: raised before the open returns.
    pub fn raise(self: &StdArc<Self>) -> LoadGuard {
        self.inflight.fetch_add(1, Ordering::AcqRel);
        LoadGuard(self.clone())
    }

    /// Whether a load is in flight.
    pub fn loading(&self) -> bool {
        self.inflight.load(Ordering::Acquire) > 0
    }

    /// Waits until no load is in flight.
    pub async fn settled(&self) {
        loop {
            let notified = self.done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.loading() {
                return;
            }
            notified.await;
        }
    }

    /// Waits until no load is in flight, up to `limit`. Whether none is.
    pub async fn settled_within(&self, limit: Duration) -> bool {
        if !self.loading() {
            return true;
        }
        tokio::time::timeout(limit, self.settled()).await.is_ok()
    }
}

// ---------------------------------------------------------------------------
// The queue loads take a slot on
// ---------------------------------------------------------------------------

struct Waiter {
    cost: u64,
    seq: u64,
    grant: tokio::sync::oneshot::Sender<()>,
}

impl PartialEq for Waiter {
    fn eq(&self, other: &Self) -> bool {
        (self.cost, self.seq) == (other.cost, other.seq)
    }
}
impl Eq for Waiter {}
impl PartialOrd for Waiter {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Waiter {
    /// The costliest first; among equals, the first to queue.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.cost.cmp(&other.cost).then(other.seq.cmp(&self.seq))
    }
}

struct Queue {
    running: usize,
    seq: u64,
    waiting: BinaryHeap<Waiter>,
}

/// A queue of loads: at most `limit()` hold a slot at once; a freed slot
/// goes to the costliest waiter.
pub(crate) struct LoadQueue {
    inner: Mutex<Queue>,
    limit: fn() -> usize,
    /// The most that ever held a slot at once.
    peak: AtomicUsize,
}

/// The process's queue, sized by `index_snapshot.max_concurrent_loads`.
static QUEUE: LoadQueue = LoadQueue::new(settings::max_concurrent_loads);

impl LoadQueue {
    pub(crate) const fn new(limit: fn() -> usize) -> LoadQueue {
        LoadQueue {
            inner: Mutex::new(Queue {
                running: 0,
                seq: 0,
                waiting: BinaryHeap::new(),
            }),
            limit,
            peak: AtomicUsize::new(0),
        }
    }

    /// Grants free slots to the costliest waiters. A waiter whose load was
    /// dropped meanwhile is skipped.
    fn grant_locked(&self, q: &mut Queue) {
        let limit = (self.limit)().max(1);
        while q.running < limit {
            let Some(w) = q.waiting.pop() else { break };
            if w.grant.send(()).is_ok() {
                q.running += 1;
                self.peak.fetch_max(q.running, Ordering::Relaxed);
            }
        }
    }

    fn regrant(&self) {
        self.grant_locked(&mut self.inner.lock());
    }

    /// Waits for a slot; `cost` orders the waiters.
    pub(crate) async fn admit(&self, cost: u64) -> Slot<'_> {
        let rx = {
            let mut q = self.inner.lock();
            let (tx, rx) = tokio::sync::oneshot::channel();
            q.seq += 1;
            let seq = q.seq;
            q.waiting.push(Waiter { cost, seq, grant: tx });
            self.grant_locked(&mut q);
            rx
        };
        // A load dropped while it waits (its runtime ending) must not keep
        // a slot it was granted and never saw.
        let mut pending = Pending { queue: self, rx: Some(rx) };
        let granted = pending.rx.as_mut().expect("set above").await;
        pending.rx = None;
        match granted {
            Ok(()) => Slot(self),
            // The queue drops a sender only by sending on it.
            Err(_) => unreachable!("a load's slot sender was dropped unsent"),
        }
    }

    fn release(&self) {
        let mut q = self.inner.lock();
        q.running -= 1;
        self.grant_locked(&mut q);
    }

    /// The most that held a slot at once since the last call.
    #[cfg(test)]
    pub(crate) fn peak_and_reset(&self) -> usize {
        self.peak.swap(0, Ordering::Relaxed)
    }
}

/// Re-reads the limit after `max_concurrent_loads` changed.
fn regrant() {
    QUEUE.regrant();
}

/// A slot on a queue, given back on drop.
pub(crate) struct Slot<'a>(&'a LoadQueue);

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// A wait for a slot. Dropped before it saw its grant, it closes the
/// channel, so no grant comes later, and gives back one that came already.
struct Pending<'a> {
    queue: &'a LoadQueue,
    rx: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if let Some(mut rx) = self.rx.take() {
            rx.close();
            if rx.try_recv().is_ok() {
                self.queue.release();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The plan and the load
// ---------------------------------------------------------------------------

/// What the open decided.
pub enum Plan {
    /// Load these INDEX sections from the snapshot at `path`, entry per
    /// index ordinal.
    Load { path: PathBuf, sections: Vec<SectionEntry> },
    /// Build from rows.
    Build,
    /// Build from rows; a snapshot was proven, but its INDEX sections are
    /// not what this open would load (none, or another set).
    BuildInstead(&'static str),
}

/// Whether a seal of `indexes` writes INDEX sections, and an open loads
/// them, under `storage`: B-tree, hash and path indexes, and no gin index.
pub fn captures(indexes: &TableIndexes, storage: settings::IndexStorage) -> bool {
    storage == settings::IndexStorage::Snapshot && !indexes.list.is_empty() && indexes.gin.is_empty()
}

/// The open's decision (module documentation). `proven` is the proof's
/// result, `None` when it rejected the snapshot.
pub fn plan(indexes: &TableIndexes, storage: settings::IndexStorage, proven: Option<&Proven>) -> Plan {
    let Some(p) = proven else {
        return Plan::Build;
    };
    if !captures(indexes, storage) {
        return Plan::Build;
    }
    if p.index.is_empty() {
        return Plan::BuildInstead("the snapshot has no INDEX sections");
    }
    if p.has_gin {
        return Plan::BuildInstead("the snapshot has a GIN section");
    }
    let n = indexes.list.len();
    let mut sections = Vec::with_capacity(n);
    for ordinal in 0..n {
        match p.index.iter().find(|e| e.kind == SectionKind::Index && e.ordinal as usize == ordinal) {
            Some(e) => sections.push(*e),
            None => return Plan::BuildInstead("an INDEX section is missing"),
        }
    }
    if p.index.len() != n {
        return Plan::BuildInstead("the snapshot has more INDEX sections than indexes");
    }
    Plan::Load {
        path: p.path.clone(),
        sections,
    }
}

/// One instance's indexes at open, and what their load reports to.
pub struct OpenIndexes {
    /// The physical name, for logs.
    pub name: String,
    pub indexes: StdArc<TableIndexes>,
    /// `indexes.epoch()` at open: readiness only if no renumbering began or
    /// ended since.
    pub epoch: u64,
    /// The proven slot count: what a section's slots must stay below.
    pub n_slots: u64,
    pub gate: StdArc<FileGate>,
    /// Raised at the open for a load; `None` for a build.
    pub barrier: Option<LoadGuard>,
    /// Whether a successful load may vouch for the word (the open's FILTER
    /// load did, and covered every slot).
    pub vouch: bool,
    /// Set when the snapshot this instance stands on holds its INDEX
    /// sections (the seal's "already clean" asks it, §3 rule 2).
    pub sections_in_snapshot: StdArc<AtomicBool>,
    /// [`Origin`], as a byte.
    pub origin: StdArc<AtomicU8>,
}

/// Why a load did not install.
#[derive(Debug)]
enum LoadError {
    Read(u16, io::Error),
    Section(u16, index_section::SectionError),
    Refused(u16, crate::core::index::sorted::SortedReject),
    Renumbered,
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadError::Read(i, e) => write!(f, "INDEX section {i} could not be read: {e}"),
            LoadError::Section(i, e) => write!(f, "INDEX section {i}: {e}"),
            LoadError::Refused(i, r) => write!(f, "INDEX section {i} was refused by its index: {r:?}"),
            LoadError::Renumbered => write!(f, "a renumbering began or ended during the load"),
        }
    }
}

/// Test-only hooks into a load: hold it before it reads (to watch readers
/// and writers wait), and write sections under another version (to play
/// an older binary's snapshot).
#[cfg(test)]
pub(crate) mod hooks {
    use std::path::{Path, PathBuf};

    use parking_lot::Mutex;

    static HELD: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());
    static VERSION: Mutex<Vec<(PathBuf, u16)>> = Mutex::new(Vec::new());

    /// Loads of file sets under `dir` wait while this lives.
    pub(crate) struct Hold(PathBuf);

    impl Drop for Hold {
        fn drop(&mut self) {
            HELD.lock().retain(|d| *d != self.0);
        }
    }

    pub(crate) fn hold(dir: &Path) -> Hold {
        HELD.lock().push(dir.to_path_buf());
        Hold(dir.to_path_buf())
    }

    pub(super) fn held(dir: &Path) -> bool {
        HELD.lock().iter().any(|d| dir.starts_with(d))
    }

    /// Seals of file sets under `dir` write sections claiming `ver`.
    pub(crate) struct Version(PathBuf);

    impl Drop for Version {
        fn drop(&mut self) {
            VERSION.lock().retain(|(d, _)| *d != self.0);
        }
    }

    pub(crate) fn write_version(dir: &Path, ver: u16) -> Version {
        VERSION.lock().push((dir.to_path_buf(), ver));
        Version(dir.to_path_buf())
    }

    pub(super) fn version(dir: &Path) -> Option<u16> {
        VERSION.lock().iter().find(|(d, _)| dir.starts_with(d)).map(|(_, v)| *v)
    }
}

/// Reads, decodes and installs every section, or none. Returns the time
/// spent reading and decoding, and installing.
fn load_now(
    o: &OpenIndexes,
    path: &std::path::Path,
    sections: &[SectionEntry],
    proof: Proof<'_>,
) -> Result<(Duration, Duration), LoadError> {
    let started = std::time::Instant::now();
    // Every section decoded and checked before any index is touched; one
    // thread per section, as the indexes are independent.
    let n_slots = o.n_slots;
    let decode_one = |e: &SectionEntry| -> Result<_, LoadError> {
        let bytes = snapshot::read_section(path, e).map_err(|err| LoadError::Read(e.ordinal, err))?;
        index_section::decode(&bytes, n_slots).map_err(|err| LoadError::Section(e.ordinal, err))
    };
    let streams: Vec<_> = std::thread::scope(|scope| {
        let handles: Vec<_> = sections.iter().map(|e| scope.spawn(move || decode_one(e))).collect();
        handles.into_iter().map(|h| h.join().expect("a section decode panicked")).collect()
    });
    let streams = streams.into_iter().collect::<Result<Vec<_>, _>>()?;
    let decoded = started.elapsed();
    // Then each index from its stream, each on its own thread.
    let installed: Vec<Result<(), LoadError>> = std::thread::scope(|scope| {
        let handles: Vec<_> = streams
            .into_iter()
            .zip(o.indexes.list.iter())
            .zip(sections)
            .map(|((stream, ix), e)| {
                scope.spawn(move || ix.load_sorted(proof, stream).map_err(|r| LoadError::Refused(e.ordinal, r)))
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("an index load panicked")).collect()
    });
    installed.into_iter().collect::<Result<Vec<()>, _>>()?;
    if !o.indexes.set_ready_at(proof, o.epoch) {
        return Err(LoadError::Renumbered);
    }
    Ok((decoded, started.elapsed() - decoded))
}

/// The indexes of one instance at open: loaded, or built by `rebuild`
/// (module documentation). Runs on the open's task; `build` is the open's
/// token.
pub async fn open_indexes<F, Fut>(build: &BuildToken, mut o: OpenIndexes, plan: Plan, rebuild: F)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = io::Result<u64>>,
{
    let proof = Proof::from(build);
    let fell_back = match plan {
        Plan::Build => false,
        Plan::BuildInstead(why) => {
            log::info!("{}: the index snapshot's sections are not loaded ({why}); rebuilding", o.name);
            false
        }
        Plan::Load { path, sections } => {
            let cost = o.n_slots.saturating_mul(o.indexes.list.len() as u64);
            let slot = QUEUE.admit(cost).await;
            #[cfg(test)]
            if let Some(dir) = o.gate.dir() {
                while hooks::held(dir) {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            }
            let started = std::time::Instant::now();
            let loaded = blocking(|| load_now(&o, &path, &sections, proof));
            drop(slot);
            match loaded {
                Ok((decoded, installed)) => {
                    LOADED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    o.sections_in_snapshot.store(true, Ordering::Release);
                    o.origin.store(1, Ordering::Release);
                    if o.vouch && o.gate.state() == GateState::Clean {
                        o.gate.set_vouched(true);
                    }
                    log::info!(
                        "{}: loaded {} index(es) from the snapshot over {} slots in {:.3} s (read and decode \
                         {:.3} s, install {:.3} s); no rebuild",
                        o.name,
                        o.indexes.len(),
                        o.n_slots,
                        started.elapsed().as_secs_f64(),
                        decoded.as_secs_f64(),
                        installed.as_secs_f64()
                    );
                    drop(o.barrier.take());
                    return;
                }
                Err(e) => {
                    FAILED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    log::warn!("{}: the index snapshot did not load ({e}); rebuilding from rows", o.name);
                    // Nothing a load installed is kept: the build below
                    // starts from empty indexes. No write has touched them,
                    // since writes wait for the barrier.
                    o.indexes.clear(proof);
                    true
                }
            }
        }
    };
    // Writes may go ahead into the unready indexes while the build runs, as
    // they always have.
    drop(o.barrier.take());
    BUILT_TOTAL.fetch_add(1, Ordering::Relaxed);
    match rebuild().await {
        Ok(n) if o.indexes.set_ready_at(proof, o.epoch) => {
            o.origin.store(if fell_back { 3 } else { 2 }, Ordering::Release);
            log::info!("Built {} index(es) on {} over {} rows", o.indexes.len(), o.name, n);
        }
        Ok(_) => log::info!("Index build on {} overlapped a compaction; its result is not used", o.name),
        Err(e) => log::error!("Index build on {} failed; queries scan instead: {}", o.name, e),
    }
}

/// The INDEX sections of `indexes`, in `for_schema` order (§3 step 5): each
/// index through C3 into [`index_section`] bytes, `CANCEL` checked every
/// block. `Ok(None)` when the seal was cancelled meanwhile.
pub fn capture_sections(indexes: &TableIndexes, gate: &FileGate) -> io::Result<Option<Vec<(SectionKind, u16, Vec<u8>)>>> {
    blocking(|| {
        let mut out = Vec::with_capacity(indexes.list.len());
        for (i, ix) in indexes.list.iter().enumerate() {
            let mut w = index_section::SectionWriter::new();
            let mut parts = 0usize;
            let streamed = ix.for_each_sorted(|part| {
                parts += 1;
                if parts % crate::core::index::sorted::SORTED_BLOCK == 0 && gate.word().load().cancelled() {
                    return Err(None);
                }
                w.push(part).map_err(Some)
            });
            match streamed {
                Ok(()) => {}
                Err(None) => return Ok(None),
                Err(Some(r)) => return Err(io::Error::other(format!("index {i} streamed out of order: {r:?}"))),
            }
            #[allow(unused_mut)]
            let mut bytes = w.finish().map_err(|r| io::Error::other(format!("index {i}: {r:?}")))?;
            #[cfg(test)]
            if let Some(v) = gate.dir().and_then(hooks::version) {
                bytes[..2].copy_from_slice(&v.to_le_bytes());
            }
            out.push((SectionKind::Index, i as u16, bytes));
        }
        Ok(Some(out))
    })
}

#[cfg(test)]
#[path = "load_tests.rs"]
mod tests;
