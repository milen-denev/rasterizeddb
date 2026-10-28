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

/// Units in one round at once.
const CONCURRENT_UNITS: usize = 4;
/// Pinned partitions one table sweeps per round, at most.
const SWEEPS_PER_ROUND: usize = 4;

#[derive(Default)]
struct Queue {
    units: Vec<Weak<dyn Unit>>,
    boards: Vec<Weak<PinBoard>>,
    running: bool,
}

/// One database's maintenance scheduler (§7.3). Inert until a partition
/// opens `External`; its one task then runs a round every [`Self::round`]
/// period (one timer per database, whatever the partition count), and stops
/// when the database drops it.
pub struct Scheduler {
    queue: parking_lot::Mutex<Queue>,
    period: parking_lot::Mutex<Duration>,
    /// Rounds run and units serviced, for tests and the measurements.
    rounds: AtomicU64,
    serviced: AtomicU64,
    /// Serialises rounds: the task's and a test's [`Self::run_round`].
    round_lock: tokio::sync::Mutex<RoundState>,
}

#[derive(Default)]
struct RoundState {
    active: Vec<Weak<dyn Unit>>,
}

impl Scheduler {
    pub fn new() -> StdArc<Self> {
        StdArc::new(Scheduler {
            queue: parking_lot::Mutex::new(Queue::default()),
            period: parking_lot::Mutex::new(Duration::from_secs(1)),
            rounds: AtomicU64::new(0),
            serviced: AtomicU64::new(0),
            round_lock: tokio::sync::Mutex::new(RoundState::default()),
        })
    }

    /// The period between rounds. One second by default.
    pub fn set_period(&self, period: Duration) {
        *self.period.lock() = period.max(Duration::from_millis(1));
    }

    pub fn rounds(&self) -> u64 {
        self.rounds.load(Ordering::Acquire)
    }

    /// Units serviced, over every round.
    pub fn serviced(&self) -> u64 {
        self.serviced.load(Ordering::Acquire)
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

    fn start(self: &StdArc<Self>, q: &mut Queue) {
        if q.running {
            return;
        }
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return;
        };
        q.running = true;
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
                s.run_round().await;
            }
        });
    }

    /// One round now: every queued and active unit serviced, then each
    /// table's forget round. What the task runs each period; tests call it
    /// to drive maintenance deterministically.
    pub async fn run_round(&self) {
        let mut round = self.round_lock.lock().await;
        let (pushed, boards) = {
            let mut q = self.queue.lock();
            q.boards.retain(|b| b.strong_count() > 0);
            (std::mem::take(&mut q.units), q.boards.clone())
        };
        let mut seen: HashSet<usize> = HashSet::new();
        let mut units: Vec<Weak<dyn Unit>> = Vec::new();
        for u in round.active.drain(..).chain(pushed) {
            if seen.insert(Weak::as_ptr(&u) as *const () as usize) {
                units.push(u);
            }
        }
        // A few at a time: a slow vacuum sweep of one partition delays the
        // rest by at most its chunk.
        let mut still: Vec<Weak<dyn Unit>> = Vec::new();
        let mut n = 0usize;
        for chunk in units.chunks(CONCURRENT_UNITS) {
            let live: Vec<(Weak<dyn Unit>, StdArc<dyn Unit>)> =
                chunk.iter().filter_map(|w| w.upgrade().map(|u| (w.clone(), u))).collect();
            let results = futures::future::join_all(live.iter().map(|(_, u)| u.service())).await;
            n += live.len();
            for ((weak, _), result) in live.into_iter().zip(results) {
                if result == Service::Busy {
                    still.push(weak);
                }
            }
        }
        round.active = still;
        self.serviced.fetch_add(n as u64, Ordering::AcqRel);
        for board in boards {
            if let Some(board) = board.upgrade() {
                board.forget_round(SWEEPS_PER_ROUND).await;
                if let Err(e) = board.maybe_compact().await {
                    ::log::error!("compacting the partition map log of \"{}\" failed: {e}", board.name);
                }
            }
        }
        self.rounds.fetch_add(1, Ordering::AcqRel);
    }
}
