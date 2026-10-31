//! The open budget and the sharded CLOCK that chooses what closes
//! (docs/partitioning.md §7.1, §7.2; phase 3 P5b-2).
//!
//! # The budget
//!
//! At most [`Budget::limit`] partitions are open per process, across tables
//! and databases: `min(4096, (RLIMIT_NOFILE − 2048) / 10)`, read from the
//! soft limit at each open that may need room (so a server that raises its
//! limit after starting gets the larger budget), at least 16, or the value
//! `SET CONFIG partition.open_budget` set. The design's divisor is 10 (two
//! providers × five descriptors, persistent-index v1 having dropped
//! `_flt.db`); its term for open plain tables is left out, and the 2,048
//! reserve stands for it. Running out of descriptors anyway is handled where
//! it happens (`Parts::open_slow`: evict, retry once, then `53000`).
//!
//! **The budget is soft.** An open that finds it full closes partitions
//! nobody holds until there is room, and opens anyway when every open
//! partition is held (by a statement, a job, or a transaction's kept
//! ticket). Refusing or waiting would make an answer depend on this node's
//! open count (§12: a write is never refused for the budget), and waiting
//! could deadlock: a transaction holding partition A open by its ticket that
//! then writes B would wait for itself.
//!
//! # The CLOCK
//!
//! Each open partition is an entry in one of [`SHARDS`] rings, the shard
//! chosen by its pid and table, each under its own mutex: an open or close
//! locks one shard, and an access locks none. An access sets its slot's
//! reference bit (`Slot::touch`, a load and at most one store); the hand
//! clears set bits as it passes and offers the first partition whose bit
//! was already clear. **Scan-resistant**: only a routed access (one that
//! reaches a single partition) sets the bit, and a fan-out opens with it
//! clear, so an unrouted `SELECT` wider than the budget evicts what it
//! opened itself before anything a routed workload keeps using.
//!
//! The same bit drives idle close (`Parts::close_idle_round`): a partition
//! not touched by a routed access for a whole `partition.idle_close_secs`
//! closes at the next board round.
//!
//! # Accounting
//!
//! An open partition's place in a ring is a [`Token`] its slot holds beside
//! the instance and drops when the instance leaves the slot: closed, or its
//! table instance dropped (a schema change's replaced instance, a `DROP`).
//! So the open count is exact whatever path an instance leaves by.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc as StdArc, OnceLock, Weak};

use super::table::Parts;

/// Rings in the CLOCK.
pub const SHARDS: usize = 16;
/// The derived budget's ceiling (§7.1).
pub const MAX_OPEN: usize = 4096;
/// The derived budget's floor.
pub const MIN_OPEN: usize = 16;
/// Descriptors reserved for everything that is not a partition.
const RESERVE: u64 = 2048;
/// Descriptors per open partition.
const PER_PARTITION: u64 = 10;

/// `SET CONFIG partition.open_budget` (0: derived) and `partition.idle_close_secs`.
pub mod settings {
    use std::sync::atomic::{AtomicU64, Ordering};

    static OPEN_BUDGET: AtomicU64 = AtomicU64::new(0);
    static IDLE_CLOSE_SECS: AtomicU64 = AtomicU64::new(300);

    /// The configured budget; 0 derives it from the descriptor limit.
    pub fn open_budget() -> u64 {
        OPEN_BUDGET.load(Ordering::Relaxed)
    }

    pub fn set_open_budget(n: u64) {
        OPEN_BUDGET.store(n, Ordering::Relaxed);
    }

    /// How long a partition must go without a routed access before a board
    /// round closes it (300 s). 0 turns idle close off.
    pub fn idle_close_secs() -> u64 {
        IDLE_CLOSE_SECS.load(Ordering::Relaxed)
    }

    pub fn set_idle_close_secs(secs: u64) {
        IDLE_CLOSE_SECS.store(secs, Ordering::Relaxed);
    }
}

/// One open partition in a ring.
struct Entry {
    id: u64,
    parts: Weak<Parts>,
    pid: u32,
}

#[derive(Default)]
struct Ring {
    entries: Vec<Entry>,
    hand: usize,
}

/// A process's open budget and CLOCK (module docs). One per process, or
/// one per directory a test isolated ([`isolate`]).
pub struct Budget {
    /// Fixed for this budget (tests); 0 reads the settings.
    fixed: usize,
    /// The idle-close period fixed for this budget's tables (tests); `None`
    /// reads the settings.
    idle_secs: Option<u64>,
    open: AtomicUsize,
    rings: [parking_lot::Mutex<Ring>; SHARDS],
    next_id: AtomicU64,
    /// Where the next eviction starts looking, so evictions spread over the
    /// rings.
    next_ring: AtomicUsize,
    /// Partitions closed to make room, and opens that went past the budget
    /// because nothing could close.
    evictions: AtomicU64,
    overruns: AtomicU64,
}

/// An open partition's place in the CLOCK: dropped when the instance leaves
/// its slot.
pub struct Token {
    budget: StdArc<Budget>,
    ring: usize,
    id: u64,
}

impl Drop for Token {
    fn drop(&mut self) {
        let mut ring = self.budget.rings[self.ring].lock();
        if let Some(at) = ring.entries.iter().position(|e| e.id == self.id) {
            ring.entries.swap_remove(at);
            if ring.hand > at {
                ring.hand -= 1;
            }
        }
        drop(ring);
        self.budget.open.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Budget {
    fn new(fixed: usize, idle_secs: Option<u64>) -> Self {
        Budget {
            fixed,
            idle_secs,
            open: AtomicUsize::new(0),
            rings: std::array::from_fn(|_| parking_lot::Mutex::new(Ring::default())),
            next_id: AtomicU64::new(1),
            next_ring: AtomicUsize::new(0),
            evictions: AtomicU64::new(0),
            overruns: AtomicU64::new(0),
        }
    }

    /// Partitions this budget allows open at once (module docs).
    pub fn limit(&self) -> usize {
        if self.fixed > 0 {
            return self.fixed;
        }
        match settings::open_budget() {
            0 => derived_limit(),
            n => n as usize,
        }
    }

    /// `partition.idle_close_secs` for this budget's tables.
    pub fn idle_close_secs(&self) -> u64 {
        self.idle_secs.unwrap_or_else(settings::idle_close_secs)
    }

    /// Partitions open under this budget now.
    pub fn open(&self) -> usize {
        self.open.load(Ordering::Acquire)
    }

    /// Partitions closed to make room so far.
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Acquire)
    }

    /// Opens that found the budget full and nothing to close.
    pub fn overruns(&self) -> u64 {
        self.overruns.load(Ordering::Acquire)
    }

    /// Counts partition `pid` of `parts` open, in the ring its key picks.
    pub(crate) fn enter(self: &StdArc<Self>, parts: Weak<Parts>, uid: u64, pid: u32) -> Token {
        let ring = ring_of(uid, pid);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.rings[ring].lock().entries.push(Entry { id, parts, pid });
        self.open.fetch_add(1, Ordering::AcqRel);
        Token { budget: self.clone(), ring, id }
    }

    /// Before an open: closes partitions nobody holds, by the CLOCK, until
    /// one more fits, giving up (and counting an overrun) once every entry
    /// has been offered twice. `skip` is the partition being opened, whose
    /// own lifecycle lock the caller holds.
    pub(crate) async fn make_room(&self, skip: (usize, u32)) {
        let limit = self.limit();
        if self.open() < limit {
            return;
        }
        let mut offers = 2 * self.open() + SHARDS;
        while self.open() >= limit {
            if offers == 0 {
                self.overruns.fetch_add(1, Ordering::AcqRel);
                return;
            }
            offers -= 1;
            let Some((parts, pid)) = self.victim(skip) else {
                self.overruns.fetch_add(1, Ordering::AcqRel);
                return;
            };
            if parts.close_for_budget(pid).await {
                self.evictions.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    /// Closes up to `n` partitions nobody holds, by the CLOCK, whatever the
    /// count: what an open that ran out of descriptors does before its one
    /// retry. Returns how many closed.
    pub(crate) async fn evict(&self, n: usize, skip: (usize, u32)) -> usize {
        let mut closed = 0;
        let mut offers = 2 * self.open() + SHARDS;
        while closed < n && offers > 0 {
            offers -= 1;
            let Some((parts, pid)) = self.victim(skip) else { break };
            if parts.close_for_budget(pid).await {
                closed += 1;
                self.evictions.fetch_add(1, Ordering::AcqRel);
            }
        }
        closed
    }

    /// The CLOCK's next offer: the first entry, from the rings' hands,
    /// whose reference bit was already clear, each set bit passed cleared.
    /// One lap over every ring first, so a ring whose entries all have their
    /// bits set never gives one up while another ring holds an unreferenced
    /// partition; a second lap only when the first found none (the bits it
    /// cleared make every entry an offer then, unless touched meanwhile).
    /// Entries whose table instance is gone are passed over.
    fn victim(&self, skip: (usize, u32)) -> Option<(StdArc<Parts>, u32)> {
        let start = self.next_ring.fetch_add(1, Ordering::Relaxed);
        for _lap in 0..2 {
            for i in 0..SHARDS {
                let mut ring = self.rings[(start + i) % SHARDS].lock();
                let mut steps = ring.entries.len();
                while steps > 0 {
                    steps -= 1;
                    if ring.hand >= ring.entries.len() {
                        ring.hand = 0;
                    }
                    let entry = &ring.entries[ring.hand];
                    let (parts, pid) = (entry.parts.upgrade(), entry.pid);
                    ring.hand += 1;
                    // An entry whose table instance is gone is left by a
                    // token not yet dropped: passed over.
                    let Some(parts) = parts else { continue };
                    if (StdArc::as_ptr(&parts) as usize, pid) == skip || parts.is_retired() {
                        continue;
                    }
                    if parts.take_reference(pid) {
                        continue;
                    }
                    return Some((parts, pid));
                }
            }
        }
        None
    }
}

fn ring_of(uid: u64, pid: u32) -> usize {
    let h = uid.rotate_left(17) ^ (pid as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    (h ^ (h >> 29)) as usize % SHARDS
}

/// `min(4096, (soft RLIMIT_NOFILE − 2048) / 10)`, at least 16.
fn derived_limit() -> usize {
    #[cfg(unix)]
    {
        let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        // SAFETY: a valid out-pointer to an `rlimit`.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0 {
            let soft = limit.rlim_cur as u64;
            let n = (soft.saturating_sub(RESERVE) / PER_PARTITION) as usize;
            return n.clamp(MIN_OPEN, MAX_OPEN);
        }
    }
    MAX_OPEN
}

/// The process's budget.
fn process() -> &'static StdArc<Budget> {
    static PROCESS: OnceLock<StdArc<Budget>> = OnceLock::new();
    PROCESS.get_or_init(|| StdArc::new(Budget::new(0, None)))
}

static ISOLATED: parking_lot::Mutex<Vec<(PathBuf, StdArc<Budget>)>> = parking_lot::Mutex::new(Vec::new());

/// The budget a partitioned table opened in `root` counts against: the
/// process's, unless [`isolate`] gave `root` (or a directory above it) one of
/// its own.
pub(crate) fn for_root(root: &Path) -> StdArc<Budget> {
    let isolated = ISOLATED.lock();
    if let Some((_, b)) = isolated.iter().find(|(d, _)| root.starts_with(d)) {
        return b.clone();
    }
    drop(isolated);
    process().clone()
}

/// Gives tables opened under `dir` from now on a budget of their own, of
/// `limit` partitions: what a test or a measurement that needs a small
/// budget uses, so it neither sees nor evicts other tests' partitions.
#[doc(hidden)]
pub fn isolate(dir: &Path, limit: usize) -> StdArc<Budget> {
    isolate_with(dir, limit, None)
}

/// [`isolate`], with the idle-close period fixed for `dir`'s tables too
/// (`Some(0)` turns idle close off there).
#[doc(hidden)]
pub fn isolate_with(dir: &Path, limit: usize, idle_secs: Option<u64>) -> StdArc<Budget> {
    let budget = StdArc::new(Budget::new(limit.max(1), idle_secs));
    let mut isolated = ISOLATED.lock();
    isolated.retain(|(d, _)| d != dir);
    isolated.push((dir.to_path_buf(), budget.clone()));
    budget
}

/// Undoes [`isolate`] for `dir`.
#[doc(hidden)]
pub fn release(dir: &Path) {
    ISOLATED.lock().retain(|(d, _)| d != dir);
}

/// The process budget's figures, for `SHOW DATABASE STATS` and the
/// measurements: (limit, open, evictions, overruns).
pub fn process_figures() -> (usize, usize, u64, u64) {
    let b = process();
    (b.limit(), b.open(), b.evictions(), b.overruns())
}
