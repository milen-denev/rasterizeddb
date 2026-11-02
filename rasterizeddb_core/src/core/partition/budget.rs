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
pub const PER_PARTITION: u64 = 10;
/// Partitions an open closes to make room, at most; the board rounds trim
/// the rest of an overrun.
pub const CLOSES_PER_OPEN: usize = 2;
/// Partitions one board round closes to trim an overrun, at most.
pub const TRIM_PER_ROUND: usize = 8;

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
            ring.remove_at(at);
        }
        drop(ring);
        self.budget.open.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Ring {
    /// Removes the entry at `at`, keeping the others' order, so the hand
    /// still points at the entry it would have visited next: the entries
    /// after `at` move down one, and so does a hand past it. (`swap_remove`
    /// moved the last entry into `at`, behind a hand past it, which then
    /// skipped it for a lap and revisited another; a ring holds a few
    /// hundred entries at most.)
    fn remove_at(&mut self, at: usize) {
        self.entries.remove(at);
        if self.hand > at {
            self.hand -= 1;
        }
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
    /// one more fits, at most [`CLOSES_PER_OPEN`] of them: each is a sealing
    /// close inside the statement that is opening, so an overrun left by a
    /// statement that held many partitions (one multi-tenant `INSERT` keeps a
    /// ticket on each until it commits) is trimmed by the board rounds
    /// ([`Self::trim`]), not by the next statement alone (review B: a
    /// single-row `INSERT` after a 200-tenant one took 7.4 s on ext4). Gives
    /// up once every entry has been offered twice. An open that finds no
    /// room counts an overrun. `skip` is the partition being opened, whose
    /// own lifecycle lock the caller holds.
    pub(crate) async fn make_room(&self, skip: (usize, u32)) {
        let limit = self.limit();
        if self.open() < limit {
            return;
        }
        let mut offers = 2 * self.open() + SHARDS;
        let mut closed = 0;
        while self.open() >= limit {
            if offers == 0 || closed == CLOSES_PER_OPEN {
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
                closed += 1;
            }
        }
    }

    /// A board round's share of trimming an overrun: closes up to `n`
    /// partitions nobody holds, by the CLOCK, while more are open than the
    /// budget allows. Returns how many closed.
    pub(crate) async fn trim(&self, n: usize) -> usize {
        let limit = self.limit();
        let mut closed = 0;
        let mut offers = 2 * self.open() + SHARDS;
        while self.open() > limit && closed < n && offers > 0 {
            offers -= 1;
            let Some((parts, pid)) = self.victim((0, u32::MAX)) else { break };
            if parts.close_for_budget(pid).await {
                closed += 1;
                self.evictions.fetch_add(1, Ordering::AcqRel);
            }
        }
        closed
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
        // Every table instance upgraded here is dropped after the ring's
        // guard, never under it: the last reference to one (a table dropped
        // or replaced meanwhile) drops its slots' tokens, and a token's drop
        // locks its ring, which parking_lot does not let this thread lock
        // twice (review B, reproduced by review A: a hang holding the
        // opener's lifecycle lock).
        let mut passed: Vec<StdArc<Parts>> = Vec::new();
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
                    #[cfg(test)]
                    victim_hooks::upgraded(&parts);
                    if (StdArc::as_ptr(&parts) as usize, pid) == skip || parts.is_retired() || parts.take_reference(pid) {
                        passed.push(parts);
                        continue;
                    }
                    drop(ring);
                    drop(passed);
                    return Some((parts, pid));
                }
            }
        }
        drop(passed);
        None
    }
}

/// Test-only: a pause inside [`Budget::victim`] once it has upgraded an
/// entry of a table under a directory, holding that ring's guard, so a test
/// can drop the table meanwhile and leave the upgrade the last reference.
#[cfg(test)]
pub(crate) mod victim_hooks {
    use std::path::{Path, PathBuf};
    use std::sync::mpsc::{Receiver, Sender};

    use super::{Parts, StdArc};

    type Park = (PathBuf, Sender<()>, Receiver<()>);
    static PARKS: parking_lot::Mutex<Vec<Park>> = parking_lot::Mutex::new(Vec::new());

    /// The next upgrade of an entry under `dir` signals the returned
    /// receiver, then blocks until the returned sender sends.
    pub(crate) fn park(dir: &Path) -> (Receiver<()>, Sender<()>) {
        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        PARKS.lock().push((dir.to_path_buf(), reached_tx, resume_rx));
        (reached_rx, resume_tx)
    }

    pub(super) fn upgraded(parts: &StdArc<Parts>) {
        let found = {
            let mut parks = PARKS.lock();
            let root = parts.root_dir();
            parks.iter().position(|(d, _, _)| root.starts_with(d)).map(|at| parks.remove(at))
        };
        if let Some((_, reached, resume)) = found {
            let _ = reached.send(());
            let _ = resume.recv_timeout(std::time::Duration::from_secs(30));
        }
    }
}

fn ring_of(uid: u64, pid: u32) -> usize {
    let h = uid.rotate_left(17) ^ (pid as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    (h ^ (h >> 29)) as usize % SHARDS
}

/// `min(4096, (soft RLIMIT_NOFILE − 2048 − the descriptors in use that are
/// not partitions') / 10)`, at least 16 (§7.1's plain-table term, counted
/// rather than estimated): the descriptors in use are read from
/// `/proc/self/fd` at most once a second ([`descriptors_in_use`]), and the
/// open partitions' share (ten each) taken off. A fixed charge per plain
/// table drifted: a table holds 40 with only its primary key's index, 60
/// and 80 with one and two more, and more again after schema changes or a
/// restart (review B of P5b-2b measured 40 to 180).
fn derived_limit() -> usize {
    #[cfg(unix)]
    {
        let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        // SAFETY: a valid out-pointer to an `rlimit`.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0 {
            let others = others_in_use();
            let n = limit_for(limit.rlim_cur as u64, others);
            // On a server, a budget short of the largest is first raised
            // toward it within the hard limit (`raise_descriptor_limit`).
            if n < MAX_OPEN && RAISE_AS_TABLES_OPEN.load(Ordering::Relaxed) {
                let (_, now) = raise_to_target();
                return limit_for(now, others);
            }
            return n;
        }
    }
    MAX_OPEN
}

/// The budget a soft descriptor limit of `soft` leaves when `others`
/// descriptors are in use by everything but the open partitions.
pub fn limit_for(soft: u64, others: u64) -> usize {
    let n = soft.saturating_sub(RESERVE).saturating_sub(others) / PER_PARTITION;
    (n as usize).clamp(MIN_OPEN, MAX_OPEN)
}

/// How often the descriptors in use are counted, at most.
pub(crate) const RECOUNT_EVERY: std::time::Duration = std::time::Duration::from_secs(1);

/// The last count; `u64::MAX` before the first.
static COUNTED: AtomicU64 = AtomicU64::new(u64::MAX);
/// When it was taken or a refresh claimed it ([`claim_due`]), and whether
/// a refresh is under way.
static COUNTED_AT: parking_lot::Mutex<Option<std::time::Instant>> = parking_lot::Mutex::new(None);
static COUNTING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// `/proc/self/fd`'s entries now: one directory read, about 3 ms at ten
/// thousand descriptors and 58 ms at fifty thousand (review B of P5b-2b);
/// 0 where it cannot be read.
fn count_now() -> u64 {
    #[cfg(test)]
    {
        LISTINGS.fetch_add(1, Ordering::Relaxed);
        while LISTING_HELD.load(Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
    std::fs::read_dir("/proc/self/fd").map_or(0, |d| d.count() as u64)
}

/// Test-only: a listing waits while this is set, so a test can cancel a
/// refresh mid-listing.
#[cfg(test)]
static LISTING_HELD: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Listings made, for the test that the budget's path makes none.
#[cfg(test)]
static LISTINGS: AtomicU64 = AtomicU64::new(0);

fn store_count(n: u64) {
    COUNTED.store(n, Ordering::Release);
    *COUNTED_AT.lock() = Some(std::time::Instant::now());
}

/// The descriptors this process has open, as last counted: an atomic read,
/// never a directory listing, so an open that asks the budget pays nothing
/// for it (the listing held a global lock on the open path, review B). The
/// first call counts, synchronously, so a server's first budget is right
/// (`raise_descriptor_limit` makes it at the start); the board rounds keep
/// it fresh ([`refresh_descriptors`]).
pub fn descriptors_in_use() -> u64 {
    match COUNTED.load(Ordering::Acquire) {
        u64::MAX => {
            let n = count_now();
            store_count(n);
            n
        }
        n => n,
    }
}

/// Recounts the descriptors in use if the last count is older than
/// [`RECOUNT_EVERY`] and no other recount is under way, on a blocking
/// thread. A scheduler's board round calls it, off every statement's path;
/// a caller meanwhile reads the previous count.
pub(crate) async fn refresh_descriptors() {
    if claim_due() {
        recount().await;
    }
}

/// Recounts unless another recount is under way; whether this one ran.
async fn recount() -> bool {
    if COUNTING.swap(true, Ordering::AcqRel) {
        return false;
    }
    // Cleared however this ends: a refresh dropped mid-listing (its caller
    // cancelled, a timeout) must not leave the flag set, which would stop
    // every later refresh for good (P5b-2b review).
    let _counting = CountingGuard;
    if let Ok(n) = tokio::task::spawn_blocking(count_now).await {
        store_count(n);
    }
    true
}

/// Clears [`COUNTING`] when the refresh that set it ends, by any path.
struct CountingGuard;

impl Drop for CountingGuard {
    fn drop(&mut self) {
        COUNTING.store(false, Ordering::Release);
    }
}

/// Claims a due refresh: when the last count is older than
/// [`RECOUNT_EVERY`], stamps it now and says so, so the next period's
/// callers find it fresh. Stamped before the listing rather than after, or
/// every plain open in the listing's window (a migration opening tables
/// one after another) would find it still due and spawn a refresh of its
/// own, each of them then returning at the in-progress flag (P5b-3).
fn claim_due() -> bool {
    let mut at = COUNTED_AT.lock();
    if at.is_some_and(|t| t.elapsed() < RECOUNT_EVERY) {
        return false;
    }
    *at = Some(std::time::Instant::now());
    true
}

/// When the count was last taken: tests.
#[cfg(test)]
pub(crate) fn counted_at() -> Option<std::time::Instant> {
    *COUNTED_AT.lock()
}

/// Recounts now, whatever the last count's age: tests.
#[cfg(test)]
pub(crate) fn refresh_descriptors_now() {
    store_count(count_now());
}

/// The descriptors in use by everything but the process budget's open
/// partitions (plain tables, the logs, connections, sockets).
pub fn others_in_use() -> u64 {
    descriptors_in_use().saturating_sub(process().open() as u64 * PER_PARTITION)
}

/// Set by [`raise_descriptor_limit`] (a server's start): from then on each
/// plain table that opens raises the soft limit as far as the budget's
/// target needs, within the hard limit. A library embedder's limit is left
/// alone.
static RAISE_AS_TABLES_OPEN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// A plain table opened (`Table`): the count is refreshed if it is due, and
/// then, on a server, the soft limit follows what the budget needs
/// ([`descriptor_target`]). The refresh is here as well as on the board
/// rounds because a database with no partitioned table runs no scheduler,
/// and its count would otherwise stay the start's for good (P5b-2b review).
/// On a task of its own: the open does not wait for a listing.
pub(crate) fn note_plain_open() {
    // No runtime, no refresh: nothing is claimed that would not follow.
    let rt = tokio::runtime::Handle::try_current().ok();
    let Some(rt) = rt.filter(|_| claim_due()) else {
        if RAISE_AS_TABLES_OPEN.load(Ordering::Relaxed) {
            raise_to_target();
        }
        return;
    };
    rt.spawn(async {
        recount().await;
        if RAISE_AS_TABLES_OPEN.load(Ordering::Relaxed) {
            raise_to_target();
        }
    });
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

/// The soft descriptor limit a server raises itself to: as far as the
/// largest open budget needs (`MAX_OPEN` partitions of ten descriptors over
/// the 2,048 reserve and `others`, the descriptors in use that are not
/// partitions'), within the hard limit, and never lower than it is. `None` when the soft limit is already there or the hard limit leaves
/// no room. pgwire raises it further if its connection cap needs more. Not
/// to the hard limit wholesale, for pgwire's reason (`plan_descriptors`:
/// anything still using `select()` breaks on a descriptor above 1,024, and a
/// larger table is not free).
pub fn descriptor_target(soft: u64, hard: u64, plain: u64) -> Option<u64> {
    let wanted = RESERVE + plain + MAX_OPEN as u64 * PER_PARTITION;
    (soft < wanted && hard > soft).then(|| wanted.min(hard))
}

/// Raises the soft `RLIMIT_NOFILE` to [`descriptor_target`] for the
/// descriptors in use now; the soft limit before and after.
fn raise_to_target() -> (u64, u64) {
    #[cfg(unix)]
    {
        let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        // SAFETY: a valid out-pointer to an `rlimit`.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0 {
            let before = limit.rlim_cur as u64;
            if let Some(to) = descriptor_target(before, limit.rlim_max as u64, others_in_use()) {
                let raised = libc::rlimit { rlim_cur: to as libc::rlim_t, rlim_max: limit.rlim_max };
                // SAFETY: a valid pointer to an `rlimit`; a soft limit at or
                // below the hard one is always permitted.
                if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) } == 0 {
                    return (before, to);
                }
            }
            return (before, before);
        }
    }
    (0, 0)
}

/// Raises the soft `RLIMIT_NOFILE` to [`descriptor_target`] and returns the
/// soft limit before and after, and the open budget it derives (logged by
/// the server at start). A stock systemd unit's soft limit is 1,024, whose
/// budget is the floor of 16; the hard limit is usually far higher
/// (docs/partitioning.md, "Phase 3 P5b-2a as built").
pub fn raise_descriptor_limit() -> (u64, u64, usize) {
    RAISE_AS_TABLES_OPEN.store(true, Ordering::Relaxed);
    let (before, after) = raise_to_target();
    (before, after, process().limit())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stock systemd unit (soft 1,024) under a high hard limit is raised to
    /// what the largest budget needs; a soft limit already there, or a hard
    /// limit at it, is left alone; a hard limit below the target caps it.
    #[test]
    fn the_descriptor_target_reaches_the_largest_budget_within_the_hard_limit() {
        let wanted = RESERVE + MAX_OPEN as u64 * PER_PARTITION;
        assert_eq!(descriptor_target(1024, 524_288, 0), Some(wanted));
        assert_eq!(descriptor_target(1024, 4096, 0), Some(4096));
        assert_eq!(descriptor_target(wanted, 524_288, 0), None);
        assert_eq!(descriptor_target(524_288, 524_288, 0), None);
        assert_eq!(descriptor_target(2256, 2256, 0), None);
        // Open plain tables raise the target by what they hold.
        assert_eq!(descriptor_target(wanted, 524_288, 6_000), Some(wanted + 6_000));
    }

    /// The count the budget reads is the last one taken, not a listing per
    /// call (review B of P5b-2b: listing `/proc/self/fd` under a global
    /// lock on every open cost 58 ms at fifty thousand descriptors): a
    /// thousand reads make no listing (other tests' board rounds may make
    /// one or two meanwhile, at most one a second), and a refresh does.
    /// A scheduler's board round keeps the count fresh: after one, the
    /// count is at most a second (and a little) old, whether this round
    /// recounted or another round, in any database, just had (they share
    /// the count, and one recount at a time runs).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_board_round_refreshes_the_count() {
        let _ = descriptors_in_use();
        // Two periods, so any count taken before (another test's) is older
        // than the bound below.
        tokio::time::sleep(2 * RECOUNT_EVERY + std::time::Duration::from_millis(100)).await;
        crate::core::partition::maintenance::Scheduler::new().run_board_round().await;
        let age = || COUNTED_AT.lock().map_or(std::time::Duration::MAX, |t| t.elapsed());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        // A recount another round started just before may still be landing.
        while age() > RECOUNT_EVERY + std::time::Duration::from_millis(200) {
            assert!(std::time::Instant::now() < deadline, "a board round left the count {:?} old", age());
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// A refresh dropped mid-listing (its caller cancelled) does not leave
    /// the in-progress flag set: the next refresh recounts. (Without the
    /// guard the flag stayed set and every later refresh returned at once,
    /// freezing the count; review of P5b-2b reproduced it 20 of 20 with a
    /// 2 ms timeout at forty thousand descriptors.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_refresh_dropped_mid_listing_leaves_the_next_free_to_recount() {
        // `recount`, not `refresh_descriptors`: in a full run other tests'
        // opens refresh the count every period, so one this test asks for
        // would rarely be due.
        let mut cancelled = false;
        for _ in 0..500 {
            LISTING_HELD.store(true, Ordering::Release);
            // Dropped while its listing is held (unless another test's
            // recount was under way, which returns at once: tried again).
            let ran = tokio::time::timeout(std::time::Duration::from_millis(50), recount()).await;
            LISTING_HELD.store(false, Ordering::Release);
            if ran.is_err() {
                cancelled = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(cancelled, "no recount was ever cancelled mid-listing");
        // Another test's recount may hold the flag a moment; a stuck flag
        // holds it for good.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !recount().await {
            assert!(
                std::time::Instant::now() < deadline,
                "a cancelled recount left the in-progress flag set: no later one ran"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// A plain open that finds the count due claims the refresh before it
    /// spawns it, so the opens after it in the same period, before that
    /// refresh has listed anything, find the count fresh and spawn nothing
    /// (P5b-3: stamped only when the listing landed, every open in the
    /// window spawned a refresh, each then returning at the in-progress
    /// flag). On a current-thread runtime the spawned refresh cannot run
    /// before the check.
    ///
    /// Mutation: stamping the count only when the listing lands.
    #[tokio::test(flavor = "current_thread")]
    async fn a_plain_open_claims_a_due_refresh_before_spawning_it() {
        let _ = descriptors_in_use();
        // Due, unless another test's refresh claims it meanwhile (then the
        // claim below is that one's, and fresh all the same).
        std::thread::sleep(RECOUNT_EVERY + std::time::Duration::from_millis(100));
        note_plain_open();
        let age = counted_at().map_or(std::time::Duration::MAX, |t| t.elapsed());
        assert!(age < RECOUNT_EVERY, "a plain open left a due count unclaimed ({age:?} old)");
    }

    #[test]
    fn the_budget_reads_the_last_count_without_listing() {
        let _ = descriptors_in_use();
        let listings = LISTINGS.load(Ordering::Relaxed);
        let mut sum = 0u64;
        for _ in 0..1000 {
            sum = sum.wrapping_add(descriptors_in_use());
        }
        assert!(sum > 0);
        let made = LISTINGS.load(Ordering::Relaxed) - listings;
        assert!(made < 10, "a thousand reads of the budget's count made {made} listings");
        refresh_descriptors_now();
        assert!(LISTINGS.load(Ordering::Relaxed) > listings + made, "a refresh made no listing");
    }

    /// §7.1's budget: the descriptors in use that are not partitions' come
    /// off the partitions' share, within the floor and the ceiling.
    #[test]
    fn open_plain_tables_shrink_the_derived_budget() {
        assert_eq!(limit_for(20_000, 0), ((20_000 - RESERVE) / PER_PARTITION) as usize);
        assert_eq!(limit_for(20_000, 12_000), ((20_000 - RESERVE - 12_000) / PER_PARTITION) as usize);
        assert_eq!(limit_for(524_288, 60_000), MAX_OPEN);
        assert_eq!(limit_for(2_256, 400), MIN_OPEN);
        // Review B's case: 500 indexed tables (60 descriptors each) under a
        // soft limit of 43,008 leave room for about 1,100 partitions, not
        // the 4,096 the old charge of 25 a table claimed.
        assert_eq!(limit_for(43_008, 500 * 60), ((43_008 - RESERVE - 500 * 60) / PER_PARTITION) as usize);
    }

    /// Removing an entry keeps the others' order and the hand on the entry
    /// it would have visited next (review A: `swap_remove` revisited one and
    /// skipped the moved one for a lap).
    #[test]
    fn removing_an_entry_keeps_the_hand_on_the_next_one() {
        let mut ring = Ring::default();
        for id in 0..6 {
            ring.entries.push(Entry { id, parts: Weak::new(), pid: id as u32 });
        }
        ring.hand = 3;
        ring.remove_at(1);
        assert_eq!(ring.entries.iter().map(|e| e.id).collect::<Vec<_>>(), vec![0, 2, 3, 4, 5]);
        assert_eq!(ring.entries[ring.hand].id, 3, "the hand moved off the entry it was to visit next");
        ring.remove_at(4);
        assert_eq!(ring.entries[ring.hand].id, 3, "removing an entry past the hand moved it");
        ring.remove_at(ring.hand);
        assert_eq!(ring.entries[ring.hand].id, 4, "removing the hand's own entry did not leave it on the next");
    }
}
