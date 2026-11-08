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
//! - **Correct whether or not a write lands during it.** The open (or a
//!   scan-only open's upgrade, phase 3 P5b-2b) raises the file set's
//!   [`LoadBarrier`] before it returns, and `gate::ticket` waits for the
//!   barrier on its slow path. On a word the proof found `Clean` every
//!   write's first ticket takes that slow path and waits. A scan-only
//!   instance's deferred load can run on a word that is already `Dirty` (a
//!   forget sweep or a vacuum tick wrote meanwhile, which adds no slot), and
//!   there a write's ticket takes the fast path and writes during the load.
//!   That is sound because a load is a union, never a replacement:
//!   `LiveIndex::load_sorted` (and `GinIndex`'s) adds the snapshot's entries
//!   beside whatever a write inserted and removes nothing; a paged index's
//!   adopted run goes in as the store's oldest (`PagedStore::adopt`, inserted
//!   at the front of the runs), and a paged lookup takes the union of every
//!   run and the delta, so the write's entries and the snapshot's are both
//!   found; and a load that fails clears the indexes and rebuilds them from the
//!   rows, which hold every write. The snapshot is a valid base after
//!   `DIRTY` (§4): its entries are a superset of the rows it was sealed over,
//!   and the open proved no slot was renumbered or added since.
//! - **Readers wait for it** up to `index_snapshot.load_wait_ms` (600), then
//!   scan (`Table::build_candidates`).
//! - **Loads queue** on one process-wide queue of
//!   `index_snapshot.max_concurrent_loads` slots, granted by rebuild cost
//!   (slots × indexes), the highest first: at startup every table and
//!   partition opens, and queues its load, before the first slot frees.
//! - **A load someone waits for goes first** (the priority bump, B18). A
//!   write whose first ticket meets a load not yet granted, or a reader
//!   about to wait for one, marks it urgent ([`LoadBarrier::bump`]); the
//!   next free slot goes to the earliest marked, ahead of every costlier
//!   load. Only the order changes: the slots stay `max_concurrent_loads`,
//!   and the write still waits for the load to finish, so nothing writes
//!   an index before its load installs and a load still meets exactly the
//!   snapshot (no union, no write let through).
//! - **Each section** is read alone against its crc32c
//!   ([`snapshot::read_section`]), decoded ([`index_section::decode`]) into
//!   a checked stream and installed by `LiveIndex::load_sorted` (hook C9),
//!   each index on its own thread from read to install, so a fast section
//!   installs while a slow one decodes; then, once every index has,
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
//! A load either installs every captured entry of every index, or clears
//! whatever it installed and the build runs. `load_sorted` adds and removes nothing, so beside a
//! write it is the union (B16), though writes wait. The captured stream
//! carries a presence bit only for a slot whose entries it carries (C3 reads
//! the bits before the sweep, B17), so a loaded index holds no slot it
//! cannot find.

use std::cmp::Reverse;
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
    static PRIORITY_BUMP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

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

    /// Whether a load someone waits for is moved to the front of the queue
    /// (the module documentation's priority bump). On; turned off only to
    /// measure what it saves (`examples/index_rebuild_bench.rs`).
    pub fn priority_bump() -> bool {
        PRIORITY_BUMP.load(Ordering::Relaxed)
    }

    #[doc(hidden)]
    pub fn set_priority_bump(on: bool) {
        PRIORITY_BUMP.store(on, Ordering::Relaxed);
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
static BUMPED_TOTAL: AtomicU64 = AtomicU64::new(0);

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

/// `rdb_index_snapshot_load_bumped_total`: loads moved to the front of the
/// queue because a write or a reader waited for them.
pub fn bumped_total() -> u64 {
    BUMPED_TOTAL.load(Ordering::Relaxed)
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
    /// 0, or the order in which a waiter marked this file set's queued load
    /// urgent ([`LoadBarrier::bump`]): the queue grants the lowest first,
    /// ahead of every unmarked load. Cleared when the load is granted and
    /// when the last load ends.
    urgent: AtomicU64,
}

/// The order of bumps, process-wide: the earliest marked is granted first.
static BUMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// One load in flight on a [`LoadBarrier`]; released on drop, on every path
/// (a panic included), so nothing waits on a load that is gone.
#[derive(Debug)]
pub struct LoadGuard(StdArc<LoadBarrier>);

impl Drop for LoadGuard {
    fn drop(&mut self) {
        if self.0.inflight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.urgent.store(0, Ordering::Release);
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

    /// A reader about to wait: marks a load still queued urgent, then waits
    /// until no load is in flight, up to `limit`. Whether none is.
    pub async fn settled_within(&self, limit: Duration) -> bool {
        if !self.loading() {
            return true;
        }
        self.bump();
        tokio::time::timeout(limit, self.settled()).await.is_ok()
    }

    /// A write's first ticket: marks a load still queued urgent, then waits
    /// until no load is in flight (`gate::ticket`, before its slow path).
    pub async fn settled_for_write(&self) {
        if self.loading() {
            self.bump();
        }
        self.settled().await
    }

    /// Marks this file set's load urgent, if one is in flight: when it is
    /// still queued, the next free slot is its, ahead of every costlier
    /// load (the module documentation's priority bump). A load already
    /// running is unaffected. The earliest marked goes first among several.
    pub fn bump(&self) {
        if !settings::priority_bump() || !self.loading() {
            return;
        }
        let seq = BUMP_SEQ.fetch_add(1, Ordering::Relaxed) + 1;
        if self.urgent.compare_exchange(0, seq, Ordering::AcqRel, Ordering::Acquire).is_ok() {
            BUMPED_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Whether a waiter marked the load in flight urgent and it has not been
    /// granted yet. For tests and diagnostics.
    pub fn bumped(&self) -> bool {
        self.urgent.load(Ordering::Acquire) != 0
    }
}

// ---------------------------------------------------------------------------
// The queue loads take a slot on
// ---------------------------------------------------------------------------

struct Waiter {
    cost: u64,
    seq: u64,
    /// The load's file set barrier, which a waiting write or reader marks
    /// urgent; `None` for an audit's scratch build, which nobody waits for.
    barrier: Option<StdArc<LoadBarrier>>,
    grant: tokio::sync::oneshot::Sender<()>,
}

impl Waiter {
    /// The order slots are granted in, highest first: a load marked urgent
    /// before any other (the earliest marked first); then the costliest;
    /// among equals, the first to queue.
    fn rank(&self) -> (bool, Reverse<u64>, u64, Reverse<u64>) {
        let urgent = self.barrier.as_ref().map_or(0, |b| b.urgent.load(Ordering::Acquire));
        (urgent != 0, Reverse(urgent), self.cost, Reverse(self.seq))
    }
}

struct Queue {
    running: usize,
    seq: u64,
    /// Ranked at each grant, since a waiter's urgency changes while it
    /// waits. Few at a time (the opens that queued), so a vector.
    waiting: Vec<Waiter>,
}

/// A queue of loads: at most `limit()` hold a slot at once; a freed slot
/// goes to the waiter a write or a reader marked urgent first, else to the
/// costliest.
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
                waiting: Vec::new(),
            }),
            limit,
            peak: AtomicUsize::new(0),
        }
    }

    /// Grants free slots to the highest-ranked waiters ([`Waiter::rank`]).
    /// A waiter whose load was dropped meanwhile is skipped.
    fn grant_locked(&self, q: &mut Queue) {
        let limit = (self.limit)().max(1);
        while q.running < limit {
            let Some(best) = (0..q.waiting.len()).max_by_key(|&i| q.waiting[i].rank()) else { break };
            let w = q.waiting.swap_remove(best);
            if let Some(b) = &w.barrier {
                b.urgent.store(0, Ordering::Release);
            }
            if w.grant.send(()).is_ok() {
                q.running += 1;
                self.peak.fetch_max(q.running, Ordering::Relaxed);
            }
        }
    }

    fn regrant(&self) {
        self.grant_locked(&mut self.inner.lock());
    }

    /// Waits for a slot; `cost` orders the waiters. Nobody can mark it
    /// urgent.
    pub(crate) async fn admit(&self, cost: u64) -> Slot<'_> {
        self.admit_with(cost, None).await
    }

    /// Waits for a slot for the load behind `barrier`: `cost` orders the
    /// waiters, and a write or a reader waiting on `barrier` moves it to
    /// the front ([`LoadBarrier::bump`]).
    pub(crate) async fn admit_load(&self, cost: u64, barrier: StdArc<LoadBarrier>) -> Slot<'_> {
        self.admit_with(cost, Some(barrier)).await
    }

    async fn admit_with(&self, cost: u64, barrier: Option<StdArc<LoadBarrier>>) -> Slot<'_> {
        let rx = {
            let mut q = self.inner.lock();
            let (tx, rx) = tokio::sync::oneshot::channel();
            q.seq += 1;
            let seq = q.seq;
            q.waiting.push(Waiter {
                cost,
                seq,
                barrier,
                grant: tx,
            });
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

/// A slot on the process's queue for an audit's scratch build (B18): the
/// scratch copy doubles a table's index memory while it runs, so it counts
/// against `max_concurrent_loads` as a load does. Lowest priority: no
/// write or reader waits for an audit.
pub(crate) async fn admit_audit() -> Slot<'static> {
    QUEUE.admit(0).await
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
    // A paged index (docs/disk-indexes.md) is captured too: its section names
    // a sealed run rather than holding the entries (`LiveIndex::seal_run`).
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
    /// A paged index's sealed run could not be adopted.
    Run(u16, String),
    Section(u16, index_section::SectionError),
    Refused(u16, crate::core::index::sorted::SortedReject),
    Renumbered,
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadError::Read(i, e) => write!(f, "INDEX section {i} could not be read: {e}"),
            LoadError::Run(i, e) => write!(f, "INDEX section {i}'s sealed run: {e}"),
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
    static QUEUES: Mutex<Vec<(PathBuf, &'static super::LoadQueue)>> = Mutex::new(Vec::new());
    static GRANTS: Mutex<Vec<(PathBuf, String)>> = Mutex::new(Vec::new());

    /// Loads of file sets under `dir` queue on `queue` instead of the
    /// process's, while this lives; each grant is logged ([`grants`]).
    pub(crate) struct Queue(PathBuf);

    impl Drop for Queue {
        fn drop(&mut self) {
            QUEUES.lock().retain(|(d, _)| *d != self.0);
            GRANTS.lock().retain(|(d, _)| !d.starts_with(&self.0));
        }
    }

    pub(crate) fn queue(dir: &Path, queue: &'static super::LoadQueue) -> Queue {
        QUEUES.lock().push((dir.to_path_buf(), queue));
        Queue(dir.to_path_buf())
    }

    pub(super) fn queue_for(dir: &Path) -> Option<&'static super::LoadQueue> {
        QUEUES.lock().iter().find(|(d, _)| dir.starts_with(d)).map(|(_, q)| *q)
    }

    pub(super) fn note_grant(dir: &Path, name: &str) {
        if QUEUES.lock().iter().any(|(d, _)| dir.starts_with(d)) {
            GRANTS.lock().push((dir.to_path_buf(), name.to_string()));
        }
    }

    /// The loads under `dir` granted a slot so far, in order.
    pub(crate) fn grants(dir: &Path) -> Vec<String> {
        GRANTS.lock().iter().filter(|(d, _)| d.starts_with(dir)).map(|(_, n)| n.clone()).collect()
    }

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

    /// A version 2 section's bytes as `ver` would carry them: version 1's
    /// are version 2's without the chunk table (what a binary before 2
    /// wrote); any other is the version field alone changed.
    pub(super) fn as_version(mut bytes: Vec<u8>, ver: u16) -> Vec<u8> {
        if ver == 1 && bytes.len() >= 4 {
            let table = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().expect("four bytes")) as usize;
            bytes.truncate(bytes.len() - 4 - table);
        }
        bytes[..2].copy_from_slice(&ver.to_le_bytes());
        bytes
    }

    pub(super) fn version(dir: &Path) -> Option<u16> {
        VERSION.lock().iter().find(|(d, _)| dir.starts_with(d)).map(|(_, v)| *v)
    }
}

/// One INDEX section, decoded: entries for an index to load, or a paged
/// index's reference to its sealed run.
enum Decoded {
    Stream(crate::core::index::sorted::SortedLoad<crate::core::index::KeyBuf>),
    RunRef(Vec<u8>),
}

/// Reads, decodes and installs every section, each index on a thread of its
/// own from its read to its install: the indexes are independent, so one
/// whose section decodes fast installs while a slower one is still decoding.
/// An error anywhere fails the whole load, once every thread has stopped,
/// and the caller clears what the others installed (`TableIndexes::clear`)
/// before it rebuilds; nothing is ready until `set_ready_at`, so no reader
/// meets a partly loaded set. Returns the longest read and decode, and the
/// longest install, of any one index.
fn load_now(
    o: &OpenIndexes,
    path: &std::path::Path,
    sections: &[SectionEntry],
    proof: Proof<'_>,
) -> Result<(Duration, Duration), LoadError> {
    let n_slots = o.n_slots;
    let decode_one = |e: &SectionEntry| -> Result<_, LoadError> {
        let at = std::time::Instant::now();
        let bytes = snapshot::read_section(path, e).map_err(|err| LoadError::Read(e.ordinal, err))?;
        let read = at.elapsed();
        // A paged index's run reference is checked by the index that takes
        // it; an in-memory index meets it as a version it refuses, below.
        if bytes.starts_with(crate::core::index::paged::RUN_REF_MAGIC) {
            return Ok(Decoded::RunRef(bytes));
        }
        let stream = index_section::decode(&bytes, n_slots).map_err(|err| LoadError::Section(e.ordinal, err))?;
        log::debug!(
            "{}: INDEX section {} ({} bytes): read and checked {:.3} s, decoded {:.3} s",
            o.name,
            e.ordinal,
            e.len,
            read.as_secs_f64(),
            (at.elapsed() - read).as_secs_f64()
        );
        Ok(Decoded::Stream(stream))
    };
    let one = |ix: &crate::core::index::LiveIndex, e: &SectionEntry| -> Result<(Duration, Duration), LoadError> {
        let at = std::time::Instant::now();
        let stream = decode_one(e)?;
        let decoded = at.elapsed();
        let loaded = match stream {
            Decoded::Stream(stream) => ix.load_sorted(proof, stream).map_err(|r| LoadError::Refused(e.ordinal, r)),
            Decoded::RunRef(bytes) => ix.adopt_run(proof, &bytes, n_slots).map_err(|r| LoadError::Run(e.ordinal, r)),
        };
        let installed = at.elapsed() - decoded;
        log::debug!("{}: index {} installed in {:.3} s", o.name, ix.decl.name, installed.as_secs_f64());
        loaded.map(|()| (decoded, installed))
    };
    let results: Vec<Result<(Duration, Duration), LoadError>> = std::thread::scope(|scope| {
        let handles: Vec<_> = o
            .indexes
            .list
            .iter()
            .zip(sections)
            .map(|(ix, e)| scope.spawn(move || one(ix, e)))
            .collect();
        handles.into_iter().map(|h| h.join().expect("an index load panicked")).collect()
    });
    let mut longest = (Duration::ZERO, Duration::ZERO);
    for r in results {
        let (decoded, installed) = r?;
        longest = (longest.0.max(decoded), longest.1.max(installed));
    }
    if !o.indexes.set_ready_at(proof, o.epoch) {
        return Err(LoadError::Renumbered);
    }
    Ok(longest)
}

/// Gives back to the OS what a big load freed. The load decodes and
/// installs on worker threads, and glibc keeps what a thread frees in that
/// thread's arena: the sections' bytes, the hash index's dealt buckets, the
/// runs a hash index was filled from. That held a 1.5M-row table's process
/// at ~550 MiB after its load, against ~375 MiB when the load ran on one
/// thread (docs/persistent-indexes.md §4). `malloc_trim` walks every arena;
/// it runs on a thread of its own, after the barrier is down, and at most
/// one at a time, as a startup loads every table at once.
fn release_load_memory(n_slots: u64) {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        static TRIMMING: AtomicBool = AtomicBool::new(false);
        // Below a chunk the load ran on one thread, in its own arena.
        if n_slots < index_section::CHUNK_ENTRIES || TRIMMING.swap(true, Ordering::AcqRel) {
            return;
        }
        let spawned = std::thread::Builder::new().name("rdb-load-trim".into()).spawn(|| {
            crate::release_freed_memory();
            TRIMMING.store(false, Ordering::Release);
        });
        if spawned.is_err() {
            TRIMMING.store(false, Ordering::Release);
        }
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    let _ = n_slots;
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
            #[cfg(test)]
            let queue = o.gate.dir().and_then(hooks::queue_for).unwrap_or(&QUEUE);
            #[cfg(not(test))]
            let queue = &QUEUE;
            let slot = queue.admit_load(cost, o.gate.loads().clone()).await;
            #[cfg(test)]
            if let Some(dir) = o.gate.dir() {
                hooks::note_grant(dir, &o.name);
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
                        "{}: loaded {} index(es) from the snapshot over {} slots in {:.3} s (longest read and \
                         decode {:.3} s, longest install {:.3} s); no rebuild",
                        o.name,
                        o.indexes.len(),
                        o.n_slots,
                        started.elapsed().as_secs_f64(),
                        decoded.as_secs_f64(),
                        installed.as_secs_f64()
                    );
                    drop(o.barrier.take());
                    release_load_memory(o.n_slots);
                    return;
                }
                Err(e) => {
                    FAILED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    log::warn!("{}: the index snapshot did not load ({e}); rebuilding from rows", o.name);
                    // Nothing a load installed is kept: the build below
                    // starts from empty indexes and reads every row, so a
                    // write that landed during the load (a deferred load on
                    // a `Dirty` word, module docs) is rebuilt with the rest,
                    // and writes after the clear insert as always.
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
            // A paged index's section is a reference to a sealed run, written
            // here whole and durable (phase 2), not its entries.
            if ix.is_paged() {
                match ix.seal_run(&|| gate.word().load().cancelled())? {
                    #[allow(unused_mut)]
                    Some(mut bytes) => {
                        // A run reference has no section layout: a binary
                        // before INDEX_SECTION_VER 2 wrote it as this one does,
                        // so playing version 1 leaves it whole. Any other
                        // version is written over its first bytes, a
                        // reference this build refuses.
                        #[cfg(test)]
                        if let Some(v) = gate.dir().and_then(hooks::version).filter(|&v| v != 1) {
                            bytes[..2].copy_from_slice(&v.to_le_bytes());
                        }
                        out.push((SectionKind::Index, i as u16, bytes));
                        continue;
                    }
                    None => return Ok(None),
                }
            }
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
                bytes = hooks::as_version(bytes, v);
            }
            out.push((SectionKind::Index, i as u16, bytes));
        }
        Ok(Some(out))
    })
}

#[cfg(test)]
#[path = "load_tests.rs"]
mod tests;
