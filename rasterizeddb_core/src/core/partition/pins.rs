//! A partitioned table's forget pins (docs/partitioning.md §7.2 point 1).
//!
//! Getting this wrong silently resurrects rows: below the database's
//! forgotten horizon every id reads as committed, so a partition holding an
//! unswept stamp of an aborted transaction X must keep that horizon at or
//! below X, open or closed, before and after a restart. A partition's
//! `Table` therefore takes no place in the forget registry
//! (`MaintenanceMode::External`); its **pin**, kept in the map log's state
//! (`log::PartState::pin`), says unswept aborted stamps may exist at or above
//! it, and the [`PinBoard`] holds the table's **one** `ForgetHandle` at the
//! minimum of its partitions' pins (`MapState::forget_pin`; `TxId::MAX`, no
//! pin at all, when every partition is clean).
//!
//! Each change of a pin is a map-log record, applied to the state and then
//! appended under the state's lock, and the handle follows:
//!
//! - **`MATERIALIZE(pid, floor, loc)`** pins a new partition at
//!   `forgotten_below()`, durable before its first row; the handle is
//!   lowered to it before that row (`PartitionedTable::materialize`).
//! - **`DIRTY(pid, floor)`**: the first write to a clean partition, in
//!   [`PinBoard::first_write`], before its first stamp: `floor =
//!   forgotten_below()`, made durable (`append_durable`: log order alone
//!   fails, since an aborted writer has no outcome and writeback can land its
//!   stamp with no record ordering it), then the handle lowered to it. Sound:
//!   the writer's id is at least `oldest_active`, which the forgotten horizon
//!   never passes. `ForgetHandle::lower_to`, since forgetting never moves
//!   back.
//! - **`SWEPT(pid, swept_to, clean)`**, in [`PinBoard::sweep`]: after a
//!   forget sweep to the settle bound `h` proved the partition's records free
//!   of every non-committed id below `h`, durably (its rewrites synced through
//!   the location's log, `vacuum::neutralize_aborted_from`), and every reader
//!   that predates them drained. The pin rises to `h`, or clears when the
//!   sweep was clean: it saw no stamp at or above `h` and no write ran or
//!   began meanwhile (the write watch's proof, `maintenance`). Group commit:
//!   a lost `SWEPT` only pins lower.
//!
//! After a restart the replayed state is the rule §7.2 gives: a partition
//! whose last pin record is `MATERIALIZE`, `DIRTY` or an unclean `SWEPT`
//! pins at its last `SWEPT` or `MATERIALIZE` floor, a clean `SWEPT` pins
//! nothing; the handle is set to the minimum before any partition opens, under
//! the open's `forget_pause`.

use std::io;
use std::path::PathBuf;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use rclite::Arc;

use super::files::{self, MapLogFile};
use super::log::{MapState, Record};
use super::maintenance::{Scheduler, WriteWatch};
use crate::core::mvcc::TransactionManager;
use crate::core::mvcc::transaction::{ForgetHandle, TxId};
use crate::core::storage_providers::backend::DbStorageProvider;
use crate::core::storage_providers::traits::StorageIO;
use crate::core::vacuum::{FORGET_STEP, NeutralizeReport};

/// A map log compacts once it is past twice its generation's snapshot plus
/// this (§3.2): a fixed threshold would undercut one snapshot of 65,536
/// partitions and compact on every append.
pub const COMPACT_SLACK: u64 = 1 << 20;

fn invalid(e: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

/// One partitioned table's pins, map log and forget handle, shared by every
/// instance a schema change reopens it as.
pub struct PinBoard {
    pub(crate) name: String,
    pub(crate) log: StdArc<MapLogFile>,
    /// The replayed log, kept current: a record is applied first and
    /// appended only if it applies, and put back if the append fails, so
    /// the file never holds a record replay would refuse.
    pub(crate) state: StdArc<tokio::sync::Mutex<MapState>>,
    handle: ForgetHandle,
    pub(crate) transactions: StdArc<TransactionManager>,
    /// By pid (pids are `0..n` until reshapes, phase 5).
    watches: Box<[StdArc<WriteWatch>]>,
    /// One sweep at a time per table, so a sweep's `SWEPT` is applied over
    /// the pin it started from.
    sweeping: tokio::sync::Mutex<()>,
    cursor: AtomicUsize,
    /// Ids a pin must lag the settle bound by before a forget round sweeps
    /// it, as a table's own service waits `FORGET_STEP`.
    step: std::sync::atomic::AtomicU64,
    pub(crate) serials: super::serial::SerialSource,
    /// Where the map log lives, and what creates a generation's provider.
    root: PathBuf,
    uid: u64,
    seed: Arc<DbStorageProvider>,
    /// The log had a torn tail, or still has its evidence: no compaction
    /// moves the records away from the generation the evidence names.
    torn: bool,
    /// Bytes of the live generation's opening records (its `SNAPSHOT`'s
    /// generation; 0 for generation 0), and the slack past twice that.
    snapshot_len: AtomicU64,
    slack: AtomicU64,
    compacting: tokio::sync::Mutex<()>,
}

/// What [`PinBoard::new`] builds a board over, beside the log and state.
pub(crate) struct BoardFiles {
    pub root: PathBuf,
    pub uid: u64,
    pub seed: Arc<DbStorageProvider>,
    pub torn: bool,
}

impl PinBoard {
    /// The board for a table whose log replayed to `state`: its handle
    /// registered, then set to the replayed pins' minimum (§7.2's restart
    /// rule). The caller holds a `forget_pause`.
    pub(crate) fn new(
        name: &str,
        log: StdArc<MapLogFile>,
        state: MapState,
        transactions: &StdArc<TransactionManager>,
        scheduler: &StdArc<Scheduler>,
        serials: super::serial::SerialSource,
        files: BoardFiles,
    ) -> StdArc<Self> {
        let handle = transactions.register_table();
        handle.set(state.forget_pin().unwrap_or(TxId::MAX));
        let pins: Vec<(u32, bool)> = state.map().entries().iter().map(|&(_, pid)| (pid, state.part(pid).is_some_and(|p| p.pin.is_some()))).collect();
        let board = StdArc::new_cyclic(|me| {
            let mut watches: Vec<StdArc<WriteWatch>> = Vec::with_capacity(pins.len());
            for (i, &(pid, pinned)) in pins.iter().enumerate() {
                debug_assert_eq!(pid as usize, i, "dense pids until reshapes");
                watches.push(WriteWatch::new(pid, pinned, me.clone(), StdArc::downgrade(scheduler)));
            }
            PinBoard {
                name: name.to_string(),
                log,
                state: StdArc::new(tokio::sync::Mutex::new(state)),
                handle,
                transactions: transactions.clone(),
                watches: watches.into_boxed_slice(),
                sweeping: tokio::sync::Mutex::new(()),
                cursor: AtomicUsize::new(0),
                step: std::sync::atomic::AtomicU64::new(FORGET_STEP),
                serials,
                root: files.root,
                uid: files.uid,
                seed: files.seed,
                torn: files.torn,
                snapshot_len: AtomicU64::new(0),
                slack: AtomicU64::new(COMPACT_SLACK),
                compacting: tokio::sync::Mutex::new(()),
            }
        });
        scheduler.register_board(StdArc::downgrade(&board));
        board
    }

    pub(crate) fn watch(&self, pid: u32) -> io::Result<&StdArc<WriteWatch>> {
        self.watches
            .get(pid as usize)
            .filter(|w| w.pid() == pid)
            .ok_or_else(|| io::Error::other(format!("internal error: no write watch for partition {pid}")))
    }

    /// This table's place in the forget registry, as `try_forget` reads it:
    /// `TxId::MAX` when no partition pins.
    pub fn handle_horizon(&self) -> TxId {
        self.handle.horizon()
    }

    /// The ids a forget round waits for, per pin. Lowered by tests.
    pub fn set_step(&self, ids: u64) {
        self.step.store(ids.max(1), Ordering::Release);
    }

    /// The handle at the minimum of `state`'s pins.
    pub(crate) fn follow(&self, state: &MapState) {
        self.handle.set(state.forget_pin().unwrap_or(TxId::MAX));
    }

    /// After a durable `MATERIALIZE(pid, floor)`, before the partition's
    /// first row: the handle covers the new pin, and the slot is pinned.
    pub(crate) fn materialized(&self, pid: u32, floor: TxId) -> io::Result<()> {
        self.handle.lower_to(floor);
        self.watch(pid)?.set_pinned(true);
        Ok(())
    }

    /// The first-write edge (§7.2): a clean partition's first writer makes
    /// `DIRTY` durable and lowers the handle before its first stamp. Every
    /// writer that found the slot unpinned waits here; the first does the
    /// work, the rest find it done.
    pub(crate) async fn first_write(&self, pid: u32) -> io::Result<()> {
        let watch = self.watch(pid)?;
        let mut state = self.state.lock().await;
        if watch.is_pinned() {
            return Ok(());
        }
        let saved = *state
            .part(pid)
            .ok_or_else(|| io::Error::other(format!("internal error: the map has no partition {pid}")))?;
        if saved.pin.is_none() {
            let floor = self.transactions.status().forgotten_below();
            let rec = Record::Dirty { pid, floor };
            state.apply(&rec).map_err(invalid)?;
            if let Err(e) = self.log.append_durable(&rec).await {
                state.restore_part(pid, saved);
                return Err(io::Error::new(
                    e.kind(),
                    format!("partition {pid} of \"{}\" could not record its first write since its last forget sweep: {e}", self.name),
                ));
            }
            self.handle.lower_to(floor);
        }
        watch.set_pinned(true);
        Ok(())
    }

    /// A forget sweep of partition `pid` (§7.2), when it is open and pinned:
    /// its records proven free of non-committed ids below the settle bound,
    /// then `SWEPT`, the pin raised (or cleared, when clean) and the handle
    /// moved, and the registry asked to forget. Returns the sweep's report;
    /// `advanced_to` is `None` when nothing moved.
    pub(crate) async fn sweep(&self, pid: u32) -> io::Result<NeutralizeReport> {
        let _one = self.sweeping.lock().await;
        let watch = self.watch(pid)?;
        let Some(unit) = watch.unit() else {
            return Ok(NeutralizeReport::default());
        };
        let Some(from) = self.state.lock().await.part(pid).and_then(|p| p.pin) else {
            return Ok(NeutralizeReport::default());
        };
        let (seq0, active0) = watch.arm();
        let report = unit.neutralize_from(&self.transactions, from).await?;
        let Some(bound) = report.advanced_to else {
            return Ok(report);
        };
        let mut state = self.state.lock().await;
        let Some(saved) = state.part(pid).copied() else {
            return Ok(NeutralizeReport { advanced_to: None, ..report });
        };
        // Clean only with no stamp at or above the bound and no writer
        // inside at the start; `pinned` is cleared before `seq` is read
        // again, so a writer arriving after the read takes the slow path
        // and waits for this lock (`maintenance` module docs).
        let mut clean = false;
        if report.newer == 0 && active0 == 0 {
            watch.set_pinned(false);
            clean = watch.seq() == seq0;
            if !clean {
                watch.set_pinned(true);
            }
        }
        let rec = Record::Swept { pid, swept_to: bound, clean };
        if let Err(e) = state.apply(&rec) {
            watch.set_pinned(true);
            return Err(invalid(e));
        }
        if let Err(e) = self.log.append(&rec).await {
            state.restore_part(pid, saved);
            watch.set_pinned(true);
            return Err(e);
        }
        self.follow(&state);
        drop(state);
        self.transactions.try_forget();
        Ok(report)
    }

    /// The forget round for this table (§7.3): up to `budget` pinned
    /// partitions whose pin lags the settle bound by the step, round-robin.
    /// Clean ones are never swept. Returns how many advanced.
    pub(crate) async fn forget_round(&self, budget: usize) -> usize {
        if !self.transactions.status().settles() {
            return 0;
        }
        let bound = self.transactions.settle_bound();
        let step = self.step.load(Ordering::Acquire);
        let due: Vec<u32> = {
            let state = self.state.lock().await;
            state
                .parts()
                .filter(|(_, p)| !p.retired && p.pin.is_some_and(|pin| bound.saturating_sub(pin) >= step))
                .map(|(pid, _)| pid)
                .collect()
        };
        if due.is_empty() {
            return 0;
        }
        let start = self.cursor.fetch_add(budget, Ordering::AcqRel) % due.len();
        let mut advanced = 0;
        for &pid in due.iter().cycle().skip(start).take(budget.min(due.len())) {
            match self.sweep(pid).await {
                Ok(r) if r.advanced_to.is_some() => advanced += 1,
                Ok(_) => {}
                Err(e) => ::log::error!("forget sweep of partition {pid} of \"{}\" failed: {e}", self.name),
            }
        }
        advanced
    }

    /// The slack past twice the snapshot a log compacts at. Lowered by
    /// tests.
    pub fn set_compact_slack(&self, bytes: u64) {
        self.slack.store(bytes, Ordering::Release);
    }

    pub(crate) fn set_snapshot_len(&self, bytes: u64) {
        self.snapshot_len.store(bytes, Ordering::Release);
    }

    /// The map log's live generation.
    pub fn generation(&self) -> u64 {
        self.log.generation()
    }

    /// Compacts the map log when it is past twice its snapshot plus the
    /// slack (§3.2). The scheduler asks each round.
    pub(crate) async fn maybe_compact(&self) -> io::Result<bool> {
        let len = self.log.io().get_len().await;
        let limit = self.snapshot_len.load(Ordering::Acquire).saturating_mul(2).saturating_add(self.slack.load(Ordering::Acquire));
        if len <= limit {
            return Ok(false);
        }
        self.compact().await
    }

    /// Compacts the map log into its next generation (§3.2): (1) under the
    /// state's lock, clone the state and start capturing appends; release;
    /// (2) write `HEADER`, `LOCATIONS` and a `SNAPSHOT` of the clone to the
    /// new generation, created exclusively with a directory fsync; (3)
    /// retake the lock, append the records captured since the clone, check
    /// the new generation replays to the state, append `SEALED` durably and
    /// switch appends to it; (4) delete the old generation. A failure before
    /// the switch deletes the new generation and leaves the old one live; a
    /// crash before `SEALED` is durable leaves a generation no open
    /// chooses, which the reaper deletes; one after leaves the old
    /// generation, which the reaper deletes. Returns whether it compacted.
    pub async fn compact(&self) -> io::Result<bool> {
        if self.torn {
            return Ok(false);
        }
        let _one = self.compacting.lock().await;
        let (clone, generation) = {
            let state = self.state.lock().await;
            if state.dropped() {
                return Ok(false);
            }
            self.log.begin_capture();
            (state.clone(), self.log.generation())
        };
        let next = generation + 1;
        let fresh = match self.write_generation(&clone, next).await {
            Ok(fresh) => fresh,
            Err(e) => {
                self.log.end_capture();
                let _ = files::remove_map_generation(&self.root, self.uid, next);
                return Err(e);
            }
        };
        #[cfg(test)]
        test_hooks::pause_after_written(&self.root).await;
        #[cfg(test)]
        crate::core::power_loss::failpoint::hit("test.pmap.compact_written", &self.root)?;
        let opening = fresh.io().get_len().await;
        let mut state = self.state.lock().await;
        let tail = self.log.end_capture();
        let sealed = async {
            for rec in &tail {
                fresh.append(rec).await?;
            }
            // The new generation must say exactly what the state does before
            // anything makes it the one an open chooses.
            let replay = fresh.replay().await?;
            if replay.torn.is_some()
                || replay.state.header() != state.header()
                || replay.state.locations() != state.locations()
                || replay.state.snapshot() != state.snapshot()
                || replay.state.dropped() != state.dropped()
            {
                return Err(io::Error::other(format!(
                    "the compacted map log generation {next} of \"{}\" does not replay to the table's state",
                    self.name
                )));
            }
            fresh.append_durable(&Record::Sealed).await
        }
        .await;
        if let Err(e) = sealed {
            drop(state);
            drop(fresh);
            let _ = files::remove_map_generation(&self.root, self.uid, next);
            return Err(e);
        }
        #[cfg(test)]
        crate::core::power_loss::failpoint::hit("test.pmap.compact_sealed", &self.root)?;
        let old = self.log.adopt(fresh, next).await?;
        if !state.sealed() {
            state.apply(&Record::Sealed).map_err(invalid)?;
        }
        self.snapshot_len.store(opening, Ordering::Release);
        drop(state);
        drop(old);
        #[cfg(test)]
        crate::core::power_loss::failpoint::hit("test.pmap.compact_switched", &self.root)?;
        files::remove_map_generation(&self.root, self.uid, generation)?;
        ::log::info!(
            "compacted the partition map log of \"{}\" into generation {next} ({opening} bytes of snapshot)",
            self.name
        );
        Ok(true)
    }

    async fn write_generation(&self, clone: &MapState, generation: u64) -> io::Result<MapLogFile> {
        files::create_map_generation(&self.root, self.uid, generation)?;
        let io = StdArc::new(self.seed.create_new(files::map_file(self.uid, generation)).await);
        let log = MapLogFile::create(io).await?;
        log.append(&Record::Header(clone.header().clone())).await?;
        log.append(&Record::Locations(clone.locations().to_vec())).await?;
        log.append(&Record::Snapshot(clone.snapshot())).await?;
        Ok(log)
    }

    /// Covers the table's `SERIAL` counters durably before a write
    /// (`serial::SerialSource::cover`); one atomic look per counter when
    /// nothing passed its reservation.
    pub(crate) async fn cover_serials(&self) -> io::Result<()> {
        self.serials.cover().await
    }
}

/// Test-only: holds a compaction between writing its snapshot and taking
/// the state's lock again, so a test can append records the new generation
/// must take from the capture.
#[cfg(test)]
pub(crate) mod test_hooks {
    use std::path::{Path, PathBuf};
    use std::sync::Arc as StdArc;

    use tokio::sync::Notify;

    type Pause = (PathBuf, StdArc<Notify>, StdArc<Notify>);
    static PAUSES: parking_lot::Mutex<Vec<Pause>> = parking_lot::Mutex::new(Vec::new());

    /// Arms a pause for compactions under `dir`: the first `Notify` is
    /// signalled when one reaches it, and it goes on once the second is.
    pub(crate) fn arm(dir: &Path) -> (StdArc<Notify>, StdArc<Notify>) {
        let (reached, resume) = (StdArc::new(Notify::new()), StdArc::new(Notify::new()));
        PAUSES.lock().push((dir.to_path_buf(), reached.clone(), resume.clone()));
        (reached, resume)
    }

    pub(super) async fn pause_after_written(root: &Path) {
        let found = {
            let mut pauses = PAUSES.lock();
            let at = pauses.iter().position(|(d, _, _)| root.starts_with(d));
            at.map(|i| pauses.remove(i))
        };
        if let Some((_, reached, resume)) = found {
            reached.notify_one();
            resume.notified().await;
        }
    }
}
