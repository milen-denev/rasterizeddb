//! Shared maintenance for partitions (docs/partitioning.md §7.3): one
//! scheduler per database, driven by a dirty queue that writes push, in place
//! of each partition's own services, flush, filter, seal and vacuum tasks;
//! and the [`WriteWatch`] every write path of a partition enters first,
//! which is also the slot's side of the forget pin's first-write edge
//! (§7.2 point 1).
//!
//! # `MaintenanceMode`
//!
//! A `Table` is opened [`MaintenanceMode::Own`] (every plain table, as
//! before) or [`MaintenanceMode::External`] (a partition, from phase 3 P5a).
//! An `External` table spawns no timer of its own: no provider service and
//! its two-second drop poll, no vacuum loop, no idle-checkpoint watcher. Its
//! open hands back a [`Unit`], which this database's [`Scheduler`] services:
//! it lands staged appends (`StorageIO::land_background`), runs the vacuum
//! service's tick once per vacuum interval, and looks for an idle checkpoint,
//! for as long as any of them has something to do, then drops the unit from
//! its round until the next write pushes it again. An idle partition costs
//! no wakeups. One-shot work at open (the index build or load, the filter
//! backfill, the load audit) still runs on tasks of its own, which end.
//! Neither does it register in the forget registry: its forget horizon is
//! its slot's pin, kept by its `PartitionedTable` (`partition::pins`).
//!
//! # The write watch
//!
//! Every write path of an `External` partition enters its watch before its
//! write ticket, and holds the guard to its end (`Table::enter_write`). The
//! watch does three things:
//!
//! - **The first-write edge.** While the slot is pinned (`pinned`, mirroring
//!   a durable pin in the map log), entering is two atomic increments and a
//!   load. A clean slot's first writer takes the slow path, which makes
//!   `DIRTY(pid, floor)` durable and lowers the table's forget handle to
//!   `floor` before any stamp (`PinBoard::first_write`).
//! - **The sweep's clean proof.** `active` counts writers inside, and `seq`
//!   every entry; a writer raises `active` before `seq`, and a sweep reads
//!   `seq` before `active`, so a sweep that saw `active == 0` and finds `seq`
//!   unchanged after it cleared `pinned` knows no write ran or began
//!   meanwhile, and one that begins afterwards takes the slow path
//!   (`PinBoard::sweep`).
//! - **The dirty queue.** The first entry after the scheduler last serviced
//!   the unit pushes it.
//!
//! The scheduler holds units and boards weakly: a partition dropped, or
//! retired by a schema change, leaves its round at the next look.

use std::collections::HashSet;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc as StdArc, Weak};
use std::time::Duration;

use futures::future::BoxFuture;

use super::pins::PinBoard;
use crate::core::mvcc::TransactionManager;
use crate::core::mvcc::transaction::TxId;
use crate::core::vacuum::NeutralizeReport;

/// Who runs a table's background maintenance.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MaintenanceMode {
    /// Its own tasks, one set per table: every plain table.
    #[default]
    Own,
    /// Its database's one [`Scheduler`], through the [`Unit`] its open hands
    /// back: a partition (§7.3).
    External,
}

/// What a round of servicing left a unit with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Service {
    /// Retired or dropped: leave the round for good.
    Gone,
    /// Something is left (staged bytes, a vacuum that is not settled, a seal
    /// pending): service it again next round.
    Busy,
    /// Nothing to do until the next write.
    Settled,
}

/// One `External` partition's maintenance, as the scheduler drives it.
pub trait Unit: Send + Sync {
    /// One round: land staged appends, the vacuum tick when its interval has
    /// passed, an idle-checkpoint look.
    fn service(&self) -> BoxFuture<'_, Service>;
    /// A forget sweep of this partition's records from horizon `from`
    /// (`vacuum::neutralize_aborted_from`), with the permit and tick a
    /// table's own service would take. Moves no horizon: the caller does,
    /// after the map log's `SWEPT`.
    fn neutralize_from<'a>(
        &'a self,
        manager: &'a StdArc<TransactionManager>,
        from: TxId,
    ) -> BoxFuture<'a, io::Result<NeutralizeReport>>;
}

/// The guard a partition's write path holds from its entry to its end.
pub struct WriteGuard<'a>(&'a WriteWatch);

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        // Again at the end: what the write changed (its mutation count,
        // bumped at the statement's end) is news to a round that serviced
        // the unit while it ran and let it go as settled.
        self.0.push();
    }
}

/// One partition slot's watch over its writers (module docs). Shared by the
/// slot, the open `Table` and its unit; outlives a schema change's reopen.
pub struct WriteWatch {
    pid: u32,
    /// The slot is pinned in memory, and its pin is durable in the map log.
    pinned: AtomicBool,
    /// Every entry, and the writers inside: the sweep's clean proof.
    seq: AtomicU64,
    active: AtomicU64,
    /// Pushed to the scheduler since it last serviced the unit.
    queued: AtomicBool,
    board: Weak<PinBoard>,
    scheduler: Weak<Scheduler>,
    unit: parking_lot::Mutex<Option<Weak<dyn Unit>>>,
}

impl WriteWatch {
    pub(crate) fn new(pid: u32, pinned: bool, board: Weak<PinBoard>, scheduler: Weak<Scheduler>) -> StdArc<Self> {
        StdArc::new(WriteWatch {
            pid,
            pinned: AtomicBool::new(pinned),
            seq: AtomicU64::new(0),
            active: AtomicU64::new(0),
            queued: AtomicBool::new(false),
            board,
            scheduler,
            unit: parking_lot::Mutex::new(None),
        })
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Whether the slot is pinned, as the fast path reads it.
    pub fn is_pinned(&self) -> bool {
        self.pinned.load(Ordering::SeqCst)
    }

    pub(crate) fn set_pinned(&self, pinned: bool) {
        self.pinned.store(pinned, Ordering::SeqCst);
    }

    /// `(seq, active)`, read in the order the clean proof needs.
    pub(crate) fn arm(&self) -> (u64, u64) {
        let seq = self.seq.load(Ordering::SeqCst);
        let active = self.active.load(Ordering::SeqCst);
        (seq, active)
    }

    pub(crate) fn seq(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }

    /// The unit the scheduler services for this slot's open instance.
    pub(crate) fn attach(&self, unit: Weak<dyn Unit>) {
        *self.unit.lock() = Some(unit);
        self.queued.store(false, Ordering::SeqCst);
        self.push();
    }

    pub(crate) fn unit(&self) -> Option<StdArc<dyn Unit>> {
        self.unit.lock().as_ref().and_then(Weak::upgrade)
    }

    /// Clears the queued flag before a round services the unit, so a write
    /// during the round pushes it again.
    pub(crate) fn serviced(&self) {
        self.queued.store(false, Ordering::SeqCst);
    }

    fn push(&self) {
        if self.queued.swap(true, Ordering::SeqCst) {
            return;
        }
        let unit = self.unit.lock().clone();
        match (self.scheduler.upgrade(), unit) {
            (Some(s), Some(u)) => s.push(u),
            _ => self.queued.store(false, Ordering::SeqCst),
        }
    }

    /// A forget sweep of this slot now, through its table's board
    /// (`PinBoard::sweep`): what `Table::neutralize_aborted_now` does for a
    /// partition opened `External`.
    pub async fn sweep_now(&self) -> io::Result<NeutralizeReport> {
        let board = self
            .board
            .upgrade()
            .ok_or_else(|| io::Error::other(format!("partition {} was swept after its table closed", self.pid)))?;
        board.sweep(self.pid).await
    }

    /// Entered by every write path before its ticket; the guard is held to
    /// its end. Errs, before anything is written, when the first-write edge
    /// cannot make the pin durable.
    pub async fn enter(&self) -> io::Result<WriteGuard<'_>> {
        // `active` before `seq`: see the module docs.
        self.active.fetch_add(1, Ordering::SeqCst);
        let guard = WriteGuard(self);
        self.seq.fetch_add(1, Ordering::SeqCst);
        if !self.pinned.load(Ordering::SeqCst) {
            let board = self.board.upgrade().ok_or_else(|| {
                io::Error::other(format!("partition {} was written after its table closed", self.pid))
            })?;
            // Boxed: the slow path runs once per clean-to-dirty edge, and
            // every write path's future would otherwise carry it.
            Box::pin(board.first_write(self.pid)).await?;
        }
        if let Some(board) = self.board.upgrade() {
            board.cover_serials().await?;
        }
        self.push();
        Ok(guard)
    }
}

/// Units serviced at once in a round.
const CONCURRENT_UNITS: usize = 4;
/// Pinned partitions one table sweeps per round, at most.
const SWEEPS_PER_ROUND: usize = 4;
/// How often a broken table that holds forgetting back is named in the log.
const BROKEN_WARNING_EVERY: Duration = Duration::from_secs(600);

#[derive(Default)]
struct Queue {
    units: Vec<Weak<dyn Unit>>,
    boards: Vec<Weak<PinBoard>>,
    /// Broken tables holding forgetting back (`PartitionedTable::broken_pin`):
    /// the name, and a token that lives as long as the table does.
    broken: Vec<(String, Weak<()>)>,
    running: bool,
}

/// One database's maintenance scheduler (§7.3). Inert until a partition
/// opens `External`; then two tasks, whatever the partition count: one
/// services the units every [`Self::set_period`] period, the other runs each
/// table's forget round and compaction check on the same period, so a slow
/// unit never holds a table's forgetting back. Both stop when the database
/// drops the scheduler, and neither stops on a panic: each unit and each
/// board is supervised on its own (counted in [`Self::panics`], the unit
/// backed off).
pub struct Scheduler {
    queue: parking_lot::Mutex<Queue>,
    period: parking_lot::Mutex<Duration>,
    /// Rounds run and units serviced, for tests and the measurements.
    rounds: AtomicU64,
    board_rounds: AtomicU64,
    serviced: AtomicU64,
    /// Panics caught in a unit's service or a board's round.
    panics: AtomicU64,
    /// Serialises unit rounds: the task's and a test's [`Self::run_round`].
    round_lock: tokio::sync::Mutex<RoundState>,
    /// Serialises board rounds.
    /// And when broken tables were last named in the log.
    board_lock: tokio::sync::Mutex<Option<std::time::Instant>>,
}

#[derive(Default)]
struct RoundState {
    active: Vec<Weak<dyn Unit>>,
    /// Units whose service panicked: panics in a row, and when to look
    /// again.
    backoff: std::collections::HashMap<usize, (u32, std::time::Instant)>,
}

fn key_of(u: &Weak<dyn Unit>) -> usize {
    Weak::as_ptr(u) as *const () as usize
}

impl Scheduler {
    pub fn new() -> StdArc<Self> {
        StdArc::new(Scheduler {
            queue: parking_lot::Mutex::new(Queue::default()),
            period: parking_lot::Mutex::new(Duration::from_secs(1)),
            rounds: AtomicU64::new(0),
            board_rounds: AtomicU64::new(0),
            serviced: AtomicU64::new(0),
            panics: AtomicU64::new(0),
            round_lock: tokio::sync::Mutex::new(RoundState::default()),
            board_lock: tokio::sync::Mutex::new(None),
        })
    }

    /// The period between rounds. One second by default.
    pub fn set_period(&self, period: Duration) {
        *self.period.lock() = period.max(Duration::from_millis(1));
    }

    /// Unit rounds run.
    pub fn rounds(&self) -> u64 {
        self.rounds.load(Ordering::Acquire)
    }

    /// Board rounds (forget and compaction) run.
    pub fn board_rounds(&self) -> u64 {
        self.board_rounds.load(Ordering::Acquire)
    }

    /// Units serviced, over every round.
    pub fn serviced(&self) -> u64 {
        self.serviced.load(Ordering::Acquire)
    }

    /// Panics caught in a unit's service or a board's round.
    pub fn panics(&self) -> u64 {
        self.panics.load(Ordering::Acquire)
    }

    /// Units waiting for the next round, and units in it.
    pub async fn queued_units(&self) -> usize {
        let queued = self.queue.lock().units.len();
        queued + self.round_lock.lock().await.active.len()
    }

    pub(crate) fn push(self: &StdArc<Self>, unit: Weak<dyn Unit>) {
        let mut q = self.queue.lock();
        q.units.push(unit);
        self.start(&mut q);
    }

    /// Registers a table's pins for the forget rounds.
    pub(crate) fn register_board(self: &StdArc<Self>, board: Weak<PinBoard>) {
        let mut q = self.queue.lock();
        q.boards.retain(|b| b.strong_count() > 0);
        q.boards.push(board);
        self.start(&mut q);
    }

    /// Registers a broken table that holds forgetting back, to be named in
    /// the log every [`BROKEN_WARNING_EVERY`] while `token` lives.
    pub(crate) fn register_broken(self: &StdArc<Self>, name: &str, token: Weak<()>) {
        let mut q = self.queue.lock();
        q.broken.retain(|(_, t)| t.strong_count() > 0);
        q.broken.push((name.to_string(), token));
        self.start(&mut q);
    }

    /// The broken partitioned tables holding the forgotten horizon back now.
    pub fn broken_pins(&self) -> Vec<String> {
        let mut q = self.queue.lock();
        q.broken.retain(|(_, t)| t.strong_count() > 0);
        q.broken.iter().map(|(n, _)| n.clone()).collect()
    }

    fn start(self: &StdArc<Self>, q: &mut Queue) {
        if q.running {
            return;
        }
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return;
        };
        q.running = true;
        for boards in [false, true] {
            let weak = StdArc::downgrade(self);
            rt.spawn(async move {
                loop {
                    let period = match weak.upgrade() {
                        Some(s) => *s.period.lock(),
                        None => break,
                    };
                    tokio::time::sleep(period).await;
                    let Some(s) = weak.upgrade() else {
                        break;
                    };
                    // Each unit and board is supervised inside; this is the
                    // backstop that keeps the task, and so every partition's
                    // maintenance, alive whatever escapes.
                    let round = std::panic::AssertUnwindSafe(async {
                        if boards {
                            s.run_board_round().await
                        } else {
                            s.run_unit_round().await
                        }
                    });
                    if let Err(payload) = futures::FutureExt::catch_unwind(round).await {
                        s.panics.fetch_add(1, Ordering::AcqRel);
                        ::log::error!(
                            "partition maintenance round panicked: {}",
                            crate::core::vacuum::panic_message(&*payload)
                        );
                    }
                }
            });
        }
    }

    /// One unit round, then one board round: what the two tasks run each
    /// period. Tests call it to drive maintenance deterministically.
    pub async fn run_round(&self) {
        self.run_unit_round().await;
        self.run_board_round().await;
    }

    /// Services every queued and active unit, [`CONCURRENT_UNITS`] in flight
    /// at a time, each next one starting as soon as any finishes: a slow
    /// unit holds up one slot, not the rest of the round.
    pub async fn run_unit_round(&self) {
        use futures::StreamExt;
        let mut round = self.round_lock.lock().await;
        let pushed = std::mem::take(&mut self.queue.lock().units);
        let now = std::time::Instant::now();
        let mut seen: HashSet<usize> = HashSet::new();
        let mut waiting: Vec<Weak<dyn Unit>> = Vec::new();
        let mut units: Vec<(Weak<dyn Unit>, StdArc<dyn Unit>)> = Vec::new();
        let active = std::mem::take(&mut round.active);
        for u in active.into_iter().chain(pushed) {
            if !seen.insert(key_of(&u)) {
                continue;
            }
            if round.backoff.get(&key_of(&u)).is_some_and(|(_, at)| *at > now) {
                waiting.push(u);
                continue;
            }
            if let Some(unit) = u.upgrade() {
                units.push((u, unit));
            }
        }
        let n = units.len();
        let mut inflight = futures::stream::FuturesUnordered::new();
        let mut pending = units.into_iter();
        let mut done: Vec<(Weak<dyn Unit>, Result<Service, String>)> = Vec::with_capacity(n);
        loop {
            while inflight.len() < CONCURRENT_UNITS {
                let Some((weak, unit)) = pending.next() else {
                    break;
                };
                inflight.push(async move {
                    let service = std::panic::AssertUnwindSafe(unit.service());
                    let result = futures::FutureExt::catch_unwind(service)
                        .await
                        .map_err(|p| crate::core::vacuum::panic_message(&*p).to_string());
                    (weak, result)
                });
            }
            match inflight.next().await {
                Some(r) => done.push(r),
                None => break,
            }
        }
        let mut still = waiting;
        for (weak, result) in done {
            let key = key_of(&weak);
            match result {
                Ok(Service::Busy) => {
                    round.backoff.remove(&key);
                    still.push(weak);
                }
                Ok(_) => {
                    round.backoff.remove(&key);
                }
                Err(why) => {
                    // Kept, as busy, and looked at again after a backoff
                    // that doubles per panic in a row.
                    self.panics.fetch_add(1, Ordering::AcqRel);
                    let in_a_row = round.backoff.get(&key).map_or(1, |(n, _)| n + 1);
                    let wait = crate::core::vacuum::panic_backoff(*self.period.lock(), in_a_row);
                    ::log::error!("a partition's maintenance panicked ({in_a_row} in a row): {why}; again after {wait:?}");
                    round.backoff.insert(key, (in_a_row, std::time::Instant::now() + wait));
                    still.push(weak);
                }
            }
        }
        let kept: HashSet<usize> = still.iter().map(key_of).collect();
        round.backoff.retain(|k, _| kept.contains(k));
        round.active = still;
        self.serviced.fetch_add(n as u64, Ordering::AcqRel);
        self.rounds.fetch_add(1, Ordering::AcqRel);
    }

    /// Each table's forget round and compaction check, each supervised;
    /// and, every [`BROKEN_WARNING_EVERY`], a warning naming each broken table
    /// that holds forgetting back.
    pub async fn run_board_round(&self) {
        let mut last_warned = self.board_lock.lock().await;
        let (boards, broken) = {
            let mut q = self.queue.lock();
            q.boards.retain(|b| b.strong_count() > 0);
            q.broken.retain(|(_, t)| t.strong_count() > 0);
            (q.boards.clone(), q.broken.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>())
        };
        for board in boards {
            let Some(board) = board.upgrade() else {
                continue;
            };
            let work = std::panic::AssertUnwindSafe(async {
                board.forget_round(SWEEPS_PER_ROUND).await;
                if let Err(e) = board.maybe_compact().await {
                    ::log::error!("compacting the partition map log of \"{}\" failed: {e}", board.name);
                }
            });
            if let Err(payload) = futures::FutureExt::catch_unwind(work).await {
                self.panics.fetch_add(1, Ordering::AcqRel);
                ::log::error!(
                    "the forget round of \"{}\" panicked: {}",
                    board.name,
                    crate::core::vacuum::panic_message(&*payload)
                );
            }
        }
        if !broken.is_empty() && last_warned.is_none_or(|t| t.elapsed() >= BROKEN_WARNING_EVERY) {
            *last_warned = Some(std::time::Instant::now());
            for name in broken {
                ::log::warn!(
                    "partitioned table \"{name}\" cannot be opened, so it holds the database's forgotten \
                     horizon where it was refused: the transaction status log grows until it opens or is dropped"
                );
            }
        }
        self.board_rounds.fetch_add(1, Ordering::AcqRel);
    }
}
