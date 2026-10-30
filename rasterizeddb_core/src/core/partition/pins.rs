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
//!   began meanwhile (the write watch's proof, `maintenance`). A lost
//!   `SWEPT` would only pin lower, but every map-log record is written ahead
//!   of the log and synced (`MapLogFile::append_all`), since an unsynced one
//!   could tear the file in a power cut; a forget round's `SWEPT`s share one
//!   transaction and one sync ([`PinBoard::sweep_all`]), and the pins move
//!   only once it has committed.
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
    /// By pid: held by every open, materialize and close of the partition
    /// (phase 3 P5b, `table::Parts`), so at most one instance of a file set
    /// is live and no open builds providers over files a close still lands
    /// into (I14). Here rather than in a table instance's slots, so a schema
    /// change's new instance shares it.
    lifecycle: Box<[tokio::sync::Mutex<()>]>,
    /// The table's live instance's slots, through which a forget round opens
    /// a closed, pinned partition to sweep it (§7.2); replaced by a schema
    /// change's reopen. Weak: the instance owns this board.
    parts: parking_lot::Mutex<std::sync::Weak<super::table::Parts>>,
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
    /// Whether a refused compaction has named `torn` in the log yet.
    torn_warned: std::sync::atomic::AtomicBool,
    /// Bytes of the live generation's opening records (its `SNAPSHOT`'s
    /// generation; 0 for generation 0), and the slack past twice that.
    snapshot_len: AtomicU64,
    slack: AtomicU64,
    /// The highest generation number a compaction of this table tried: never
    /// tried again, even after a failed attempt deleted its file, whose
    /// committed appends a crash could replay onto a file of the same name.
    compacting: tokio::sync::Mutex<u64>,
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
                lifecycle: (0..pins.len()).map(|_| tokio::sync::Mutex::new(())).collect(),
                parts: parking_lot::Mutex::new(std::sync::Weak::new()),
                watches: watches.into_boxed_slice(),
                sweeping: tokio::sync::Mutex::new(()),
                cursor: AtomicUsize::new(0),
                step: std::sync::atomic::AtomicU64::new(FORGET_STEP),
                serials,
                root: files.root,
                uid: files.uid,
                seed: files.seed,
                torn: files.torn,
                torn_warned: std::sync::atomic::AtomicBool::new(false),
                snapshot_len: AtomicU64::new(0),
                slack: AtomicU64::new(COMPACT_SLACK),
                compacting: tokio::sync::Mutex::new(0),
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

    /// Partition `pid`'s lifecycle lock (see the field).
    pub(crate) fn lifecycle(&self, pid: u32) -> io::Result<&tokio::sync::Mutex<()>> {
        self.lifecycle
            .get(pid as usize)
            .ok_or_else(|| io::Error::other(format!("internal error: no lifecycle lock for partition {pid}")))
    }

    /// The live instance's slots, for the forget rounds.
    pub(crate) fn set_parts(&self, parts: std::sync::Weak<super::table::Parts>) {
        *self.parts.lock() = parts;
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
    /// [`Self::sweep_all`] of one partition. Returns the sweep's report;
    /// `advanced_to` is `None` when nothing moved.
    pub(crate) async fn sweep(&self, pid: u32) -> io::Result<NeutralizeReport> {
        let mut done = self.sweep_all(&[pid]).await?;
        done.pop().map_or(Ok(NeutralizeReport::default()), |(_, r)| r)
    }

    /// Forget sweeps of partitions `pids` (§7.2), each when it is open and
    /// pinned: each one's records proven free of non-committed ids below the
    /// settle bound, durably (`vacuum::neutralize_aborted_from` syncs its
    /// rewrites), then every `SWEPT` appended as **one** map-log transaction,
    /// one log sync for the batch however many partitions advanced
    /// (`MapLogFile::append_all`, which writes ahead of the log, so a crash
    /// never tears the map), and only once that has committed the pins raised
    /// (or cleared, when clean), the handle moved and the registry asked to
    /// forget. A failed append puts every pin back as it was.
    ///
    /// Returns each partition's report, in order, or its own error (its
    /// sweep failed, or the batch's append did); `Err` only for a partition
    /// with no write watch.
    pub(crate) async fn sweep_all(&self, pids: &[u32]) -> io::Result<Vec<(u32, io::Result<NeutralizeReport>)>> {
        let _one = self.sweeping.lock().await;
        // A closed partition keeps pinning (§7.2, phase 3 P5b): one that is
        // pinned is opened for its sweep, held open through it, and closed
        // again afterwards if nothing else took it meanwhile. Otherwise a
        // partition pinned at a crash, or closed dirty, would hold
        // forgetting back until a statement happened to open it.
        let (held, opened_here) = self.open_closed_for_sweep(pids).await;
        let done = self.sweep_open(pids).await;
        drop(held);
        self.close_after_sweep(opened_here).await;
        done
    }

    /// Opens each partition of `pids` that is materialized, closed and
    /// pinned, through the live instance's slots: the instances, held by
    /// the caller through the sweep, and the pids opened. One that does not
    /// open is left unswept (its pin stays) and logged.
    async fn open_closed_for_sweep(
        &self,
        pids: &[u32],
    ) -> (Vec<Arc<crate::core::row::table::Table<DbStorageProvider>>>, Vec<u32>) {
        let mut held = Vec::new();
        let mut opened = Vec::new();
        // A replaced instance (a schema change between its retirement and
        // its successor's `set_parts`) opens nothing: those partitions wait
        // for the next round, swept through the new instance.
        let Some(parts) = self.parts.lock().upgrade().filter(|p| !p.is_retired()) else {
            return (held, opened);
        };
        for &pid in pids {
            if !parts.is_materialized(pid) || parts.open_now(pid).is_some() {
                continue;
            }
            if self.state.lock().await.part(pid).and_then(|p| p.pin).is_none() {
                continue;
            }
            match parts.open(pid).await {
                Ok(table) => {
                    held.push(table);
                    opened.push(pid);
                }
                Err(e) => ::log::error!("partition {pid} of \"{}\" did not open for its forget sweep: {e}", self.name),
            }
        }
        (held, opened)
    }

    /// Closes again each partition of `pids` a sweep opened, if nothing else
    /// holds it now.
    async fn close_after_sweep(&self, pids: Vec<u32>) {
        if pids.is_empty() {
            return;
        }
        let Some(parts) = self.parts.lock().upgrade() else {
            return;
        };
        for pid in pids {
            if let Err(e) = parts.close_after_sweep(pid).await {
                ::log::error!("partition {pid} of \"{}\" did not close after its forget sweep: {e}", self.name);
            }
        }
    }

    /// [`Self::sweep_all`] over the partitions open now; a closed one is
    /// skipped, its pin unmoved.
    async fn sweep_open(&self, pids: &[u32]) -> io::Result<Vec<(u32, io::Result<NeutralizeReport>)>> {
        // Each partition's proof, before the state's lock: the scans and
        // their rewrites take as long as they take, and writers' first-write
        // edges wait on that lock.
        let mut swept: Vec<(u32, io::Result<NeutralizeReport>, (u64, u64))> = Vec::with_capacity(pids.len());
        for &pid in pids {
            let watch = self.watch(pid)?;
            let Some(unit) = watch.unit() else {
                swept.push((pid, Ok(NeutralizeReport::default()), (0, 0)));
                continue;
            };
            let Some(from) = self.state.lock().await.part(pid).and_then(|p| p.pin) else {
                swept.push((pid, Ok(NeutralizeReport::default()), (0, 0)));
                continue;
            };
            let armed = watch.arm();
            let report = unit.neutralize_from(&self.transactions, from).await;
            swept.push((pid, report, armed));
        }

        let mut state = self.state.lock().await;
        let mut records = Vec::new();
        // The partitions whose `SWEPT` is in `records`, with their state
        // before it, to put back if the append fails.
        let mut applied: Vec<(usize, super::log::PartState)> = Vec::new();
        for (at, (pid, report, (seq0, active0))) in swept.iter_mut().enumerate() {
            let pid = *pid;
            let Ok(r) = report else {
                continue;
            };
            let Some(bound) = r.advanced_to else {
                continue;
            };
            let watch = self.watch(pid)?;
            let Some(saved) = state.part(pid).copied() else {
                r.advanced_to = None;
                continue;
            };
            // Clean only with no stamp at or above the bound and no writer
            // inside at the start; `pinned` is cleared before `seq` is read
            // again, so a writer arriving after the read takes the slow path
            // and waits for this lock (`maintenance` module docs), which is
            // held until the batch has committed or been put back.
            let mut clean = false;
            if r.newer == 0 && *active0 == 0 {
                watch.set_pinned(false);
                clean = watch.seq() == *seq0;
                if !clean {
                    watch.set_pinned(true);
                }
            }
            let rec = Record::Swept { pid, swept_to: bound, clean };
            if let Err(e) = state.apply(&rec) {
                watch.set_pinned(true);
                *report = Err(invalid(e));
                continue;
            }
            records.push(rec);
            applied.push((at, saved));
        }
        if records.is_empty() {
            return Ok(swept.into_iter().map(|(pid, r, _)| (pid, r)).collect());
        }
        if let Err(e) = self.log.append_all(&records).await {
            for &(at, saved) in &applied {
                let pid = swept[at].0;
                state.restore_part(pid, saved);
                self.watch(pid)?.set_pinned(true);
                swept[at].1 = Err(io::Error::new(e.kind(), e.to_string()));
            }
            return Ok(swept.into_iter().map(|(pid, r, _)| (pid, r)).collect());
        }
        self.follow(&state);
        drop(state);
        self.transactions.try_forget();
        Ok(swept.into_iter().map(|(pid, r, _)| (pid, r)).collect())
    }

    /// The forget round for this table (§7.3): up to `budget` pinned
    /// partitions whose pin lags the settle bound by the step, round-robin,
    /// swept as one batch ([`Self::sweep_all`]: one map-log sync for the
    /// round). Clean ones are never swept. Returns how many advanced.
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
        let batch: Vec<u32> = due.iter().cycle().skip(start).take(budget.min(due.len())).copied().collect();
        let done = match self.sweep_all(&batch).await {
            Ok(done) => done,
            Err(e) => {
                ::log::error!("forget sweep of \"{}\" failed: {e}", self.name);
                return 0;
            }
        };
        let mut advanced = 0;
        for (pid, r) in done {
            match r {
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

    /// Whether compaction is held off by torn-tail evidence (`torn`).
    pub fn compaction_held(&self) -> bool {
        self.torn
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
            // Not silently: the log grows for as long as the evidence stays,
            // so it is named once here and for as long as it lasts in `SHOW
            // DATABASE STATS` (`partition_compaction_held_by`).
            if !self.torn_warned.swap(true, Ordering::AcqRel) {
                ::log::warn!(
                    "the partition map log of \"{}\" is not compacted while its torn-tail evidence ({}{}) exists; \
                     the log grows until an operator examines and removes that file",
                    self.name,
                    files::map_file(self.uid, self.log.generation()),
                    files::TORN,
                );
            }
            return Ok(false);
        }
        let mut tried = self.compacting.lock().await;
        let (clone, generation) = {
            let state = self.state.lock().await;
            if state.dropped() {
                return Ok(false);
            }
            self.log.begin_capture();
            (state.clone(), self.log.generation())
        };
        let next = match files::next_map_generation(&self.root, self.uid, generation.max(*tried)) {
            Ok(next) => next,
            Err(e) => {
                self.log.end_capture();
                return Err(e);
            }
        };
        *tried = next;
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
        if state.dropped() {
            // Dropped meanwhile: its files go with the table.
            drop(state);
            drop(fresh);
            let _ = files::remove_map_generation(&self.root, self.uid, next);
            return Ok(false);
        }
        let sealed = async {
            // One transaction, one sync, for the records taken since the
            // clone.
            fresh.append_all(&tail).await?;
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
        // One transaction, one sync: nothing chooses the generation until
        // its `SEALED`, so these need no order of their own.
        log.append_all(&[
            Record::Header(clone.header().clone()),
            Record::Locations(clone.locations().to_vec()),
            Record::Snapshot(clone.snapshot()),
        ])
        .await?;
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
