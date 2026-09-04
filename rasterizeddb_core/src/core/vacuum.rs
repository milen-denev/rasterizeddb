//! Space reclamation for the row heap.
//!
//! A `DELETE` tombstones a pointer record and an `UPDATE` that outgrows its
//! slot tombstones the old one and appends a replacement. Neither touches the
//! row bytes, so the rows file only ever grows: a table that deletes and
//! reinserts the same working set doubles on disk every cycle even though its
//! live size never changes.
//!
//! This module gives that space back. A background task periodically scans the
//! pointer file, works out which byte ranges of the rows file no pointer refers
//! to any more, and publishes them as a free list that the write path allocates
//! from before it falls back to appending.
//!
//! # What is *not* reclaimed
//!
//! Tombstoned **pointer records** are left in place. Their offsets are slot
//! addresses for the filter index (`slot = offset / ROW_POINTER_RECORD_LEN`),
//! so compacting the pointer file would renumber every slot after the first
//! hole and invalidate the index wholesale. Reusing a slot in place is no
//! better: a reader decoding a 21-byte record while the write path rewrites it
//! could pair a new row's id with the old row's position, which — unlike the
//! single-byte tombstone flip the engine already does — is not a benign tear.
//! Pointer compaction therefore belongs to an offline pass that can rebuild the
//! filter, and is deliberately out of scope here.
//!
//! # Why a reused extent cannot be read out from under a query
//!
//! A query decodes a pointer and then fetches the row bytes it names, so an
//! extent must not be handed to a writer while some in-flight query still holds
//! a pointer into it. Two mechanisms cover that:
//!
//! * **Reader epochs.** Every query takes a monotonically increasing ticket for
//!   its duration. A batch of candidate extents is stamped with the ticket
//!   counter as it stood *after* the scan that found them; it is published only
//!   once every still-active query holds a ticket at or beyond that stamp. Such
//!   a query started after all the tombstones the scan relied on were durable,
//!   so it cannot have obtained a pointer into any of those extents.
//!
//! * **In-flight allocations.** An extent handed to a writer is recorded until
//!   a later scan actually observes the live pointer that now owns it. A scan
//!   that raced the append would otherwise see no owner and free the extent a
//!   second time. Candidates are filtered against that record at publish time,
//!   by interval subtraction rather than exact match, so a partially consumed
//!   extent contributes only its unused remainder.

use branches::likely;
use cacheguard::CacheGuard;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use log::info;
use parking_lot::Mutex;

use crate::core::filter::TableFilters;
use crate::core::mvcc::transaction::{FROZEN_TX, NO_TX};
use crate::core::mvcc::visibility::Visibility;
use crate::core::mvcc::{TransactionManager, TxId, TxOutcome};
use crate::core::row::row_pointer::{ROW_POINTER_RECORD_LEN, RowPointer};
use crate::core::storage_providers::traits::StorageIO;

/// A half-open byte range `[start, start + len)` of the rows file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Extent {
    pub start: u64,
    pub len: u64,
}

impl Extent {
    /// Saturating, so a pointer record with a nonsense length cannot wrap the
    /// arithmetic into an extent that overlaps live data.
    #[inline]
    pub fn end(&self) -> u64 {
        self.start.saturating_add(self.len)
    }
}

/// Below this, a leftover is not worth tracking: the bookkeeping costs more
/// than the bytes, and no row is that small anyway.
const MIN_SPLIT_BYTES: u64 = 24;

/// Ceiling on the free list. A table with millions of holes would otherwise
/// spend more on the list than the space is worth; the largest extents are the
/// ones worth keeping, so the tail is dropped.
const MAX_FREE_EXTENTS: usize = 1 << 16;

// ---------------------------------------------------------------------------
// Reader epochs
// ---------------------------------------------------------------------------

/// Hands every read a ticket so reclamation can tell which queries predate a
/// batch of freed extents.
#[derive(Debug, Default)]
pub struct ReaderRegistry {
    next: AtomicU64,
    active: Mutex<BTreeSet<u64>>,
}

impl ReaderRegistry {
    pub fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
            active: Mutex::new(BTreeSet::new()),
        }
    }

    /// Registers a read. The returned guard must outlive every row fetch the
    /// read performs, including ones running on worker tasks.
    pub fn enter(self: &Arc<Self>) -> ReadTicket {
        let ticket = self.next.fetch_add(1, Ordering::AcqRel);
        self.active.lock().insert(ticket);
        ReadTicket {
            registry: Some(self.clone()),
            ticket,
        }
    }

    /// The value a read entering now would receive.
    #[inline]
    pub fn barrier(&self) -> u64 {
        self.next.load(Ordering::Acquire)
    }

    /// Ticket of the oldest read still running, if any.
    #[inline]
    pub fn oldest_active(&self) -> Option<u64> {
        self.active.lock().iter().next().copied()
    }

    /// Whether every read that was running when `barrier` was taken has ended.
    #[inline]
    pub fn drained_past(&self, barrier: u64) -> bool {
        match self.oldest_active() {
            Some(oldest) => oldest >= barrier,
            None => true,
        }
    }
}

/// Guard returned by [`ReaderRegistry::enter`]. Dropping it retires the ticket.
pub struct ReadTicket {
    registry: Option<Arc<ReaderRegistry>>,
    ticket: u64,
}

impl ReadTicket {
    /// A guard that registers nothing, for when reclamation is switched off.
    #[inline]
    pub fn inert() -> Self {
        Self {
            registry: None,
            ticket: 0,
        }
    }
}

impl Drop for ReadTicket {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.take() {
            registry.active.lock().remove(&self.ticket);
        }
    }
}

// ---------------------------------------------------------------------------
// Free list
// ---------------------------------------------------------------------------

/// The list and the loans drawn from it, under one lock.
///
/// Deliberately not two locks. Publishing has to read the loans and install the
/// new list as one indivisible step: with separate locks, a writer could take an
/// extent from the old list in the window between publication reading the loans
/// and installing the list, and the extent would then be both lent out and on
/// the new list — handed to two rows, which write over each other.
#[derive(Debug, Default)]
struct FreeState {
    /// Keyed `(len, start)`, so the best fit for a request of `n` bytes is the
    /// first entry at or after `(n, 0)`. A `BTreeSet` rather than a sorted
    /// `Vec`: a bulk insert allocates once per row, and shifting a
    /// sixty-thousand-entry vector on each of those is the kind of cost that
    /// only shows up under exactly the workload this is meant to help.
    extents: BTreeSet<(u64, u64)>,
    /// Handed to a writer, but not yet seen owned by a live pointer.
    ///
    /// Keyed by start offset: an extent is lent out whole or from its front, so
    /// two live loans can never share a start.
    in_flight: HashMap<u64, u64>,
}

/// Reclaimed byte ranges of one table's rows file, and the ranges currently
/// lent out to writers.
#[derive(Debug, Default)]
pub struct FreeSpace {
    state: Mutex<FreeState>,
    /// Mirrors `state.in_flight.len()` so a scan can skip the lock per live row.
    ///
    /// On its own cache line: this is the read-mostly load that every scanned
    /// row performs, while the writers that change it are a different set of
    /// threads entirely. Packed next to the reuse counters below, a single
    /// allocating writer invalidated it for every scanning thread at once.
    in_flight_count: CacheGuard<AtomicUsize>,
    /// Extents a run found but could not publish, each with the barrier it was
    /// stamped with. See [`FreeSpace::quarantine`].
    quarantined: Mutex<Vec<(u64, Vec<Extent>)>>,
    /// Deliberately *not* individually padded. All three are touched by the
    /// same thread in the same breath — [`FreeSpace::take`] does a `fetch_sub`
    /// on `free_bytes` and a `fetch_add` on each of the others — so they want to
    /// be on one line, not three. Splitting them would turn one exclusive-line
    /// acquisition per allocation into three, and every writer contends for all
    /// three either way, so nothing is won in exchange.
    free_bytes: AtomicU64,
    reused_bytes: AtomicU64,
    reused_rows: AtomicU64,
    /// Error path only; shares the line above because it is never hot.
    leaked_loans: AtomicU64,
    /// Own line for the same reason as `in_flight_count`: [`FreeSpace::is_enabled`]
    /// is `#[inline]` and runs per row, while `set_enabled` fires approximately
    /// never. It should sit in each scanning core's L1 undisturbed.
    enabled: CacheGuard<AtomicBool>,
}

impl FreeSpace {
    pub fn new(enabled: bool) -> Self {
        Self {
            state: Mutex::new(FreeState::default()),
            in_flight_count: AtomicUsize::new(0).into(),
            quarantined: Mutex::new(Vec::new()),
            free_bytes: AtomicU64::new(0),
            reused_bytes: AtomicU64::new(0),
            reused_rows: AtomicU64::new(0),
            leaked_loans: AtomicU64::new(0),
            enabled: AtomicBool::new(enabled).into(),
        }
    }

    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn free_bytes(&self) -> u64 {
        self.free_bytes.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn reused_bytes(&self) -> u64 {
        self.reused_bytes.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn reused_rows(&self) -> u64 {
        self.reused_rows.load(Ordering::Relaxed)
    }

    /// Number of extents currently on the list. Diagnostics only.
    pub fn len(&self) -> usize {
        self.state.lock().extents.len()
    }

    /// Whether the list has nothing to hand out.
    pub fn is_empty(&self) -> bool {
        self.state.lock().extents.is_empty()
    }

    /// Extents lent out but not yet observed owned by a live pointer.
    #[inline]
    pub fn loans_outstanding(&self) -> usize {
        self.in_flight_count.load(Ordering::Acquire)
    }

    /// Extents abandoned because the write that claimed them failed. They come
    /// back on the scan after the one that observes their absence.
    #[inline]
    pub fn leaked_loans(&self) -> u64 {
        self.leaked_loans.load(Ordering::Relaxed)
    }

    /// Claims `need` bytes, returning the offset to write them at.
    ///
    /// Best fit: the smallest extent that still fits, so large holes stay
    /// available for large rows. A leftover worth keeping goes back on the list
    /// as its own extent; anything smaller is absorbed into the loan and lost
    /// until the next scan finds it again as an interior gap.
    pub fn alloc(&self, need: u64) -> Option<u64> {
        if need == 0 || !self.is_enabled() {
            return None;
        }

        // Choosing the extent and recording the loan happen under one lock, so
        // publication can never observe the extent as neither taken nor lent.
        let start = {
            let state = &mut *self.state.lock();
            let (chosen_len, chosen_start) = *state.extents.range((need, 0)..).next()?;
            state.extents.remove(&(chosen_len, chosen_start));

            let leftover = chosen_len - need;
            let taken = if leftover >= MIN_SPLIT_BYTES {
                state.extents.insert((leftover, chosen_start + need));
                need
            } else {
                // The tail is too small to be worth a list entry. It stays part
                // of the loan and reappears as an interior gap next scan, where
                // it is filtered out for the same reason.
                chosen_len
            };

            state.in_flight.insert(chosen_start, taken);
            self.in_flight_count
                .store(state.in_flight.len(), Ordering::Release);

            // Under the same lock as the list it describes. Outside it, this
            // subtraction could land after a concurrent publish stored the new
            // total and wrap the counter past zero — a metric reading 1.8e19 is
            // worse than one that is merely approximate.
            self.free_bytes.fetch_sub(taken, Ordering::Relaxed);
            chosen_start
        };

        self.reused_bytes.fetch_add(need, Ordering::Relaxed);
        self.reused_rows.fetch_add(1, Ordering::Relaxed);
        Some(start)
    }

    /// A scan saw a live pointer at `start`: whatever loan covered it is now
    /// accounted for by that pointer and no longer needs shielding.
    #[inline]
    fn retire_loan(&self, start: u64) {
        // Called once per live row of a full scan, and almost always with
        // nothing outstanding, so the lock is worth avoiding. Hinted `likely`
        // to make the early return the fall-through, which is the same claim
        // the `CacheGuard` on `in_flight_count` rests on: this load is the
        // whole function on nearly every call, so it should be an L1 hit
        // followed by a not-taken branch and nothing else.
        if likely(self.in_flight_count.load(Ordering::Acquire) == 0) {
            return;
        }
        let state = &mut *self.state.lock();
        if state.in_flight.remove(&start).is_some() {
            self.in_flight_count
                .store(state.in_flight.len(), Ordering::Release);
        }
    }

    /// Gives back an extent whose write failed.
    ///
    /// Without this the loan would shield those bytes from every future scan
    /// and the space would be lost for the life of the process. The extent is
    /// not put straight back on the list: the failed write may have left
    /// partial bytes there, and a scan is what establishes that nothing owns
    /// them.
    pub fn release_failed_loan(&self, start: u64) {
        let state = &mut *self.state.lock();
        if state.in_flight.remove(&start).is_some() {
            self.in_flight_count
                .store(state.in_flight.len(), Ordering::Release);
            self.leaked_loans.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Holds a batch back until every query older than `barrier` has finished.
    ///
    /// Only used when a run times out waiting. Keeping the batch means the next
    /// run does not have to rediscover it, and — more to the point — that a
    /// table with a query always in flight still reclaims, one run behind,
    /// rather than never.
    fn quarantine(&self, candidates: Vec<Extent>, barrier: u64) {
        if candidates.is_empty() {
            return;
        }
        let mut quarantined = self.quarantined.lock();
        // One batch per barrier is enough: a later run's findings supersede an
        // earlier one's, since both derive from the same authoritative sweep.
        quarantined.clear();
        quarantined.push((barrier, candidates));
    }

    /// Takes the quarantined batches whose barrier has since been passed.
    fn take_matured(&self, readers: &ReaderRegistry) -> Vec<Extent> {
        let mut quarantined = self.quarantined.lock();
        if quarantined.is_empty() {
            return Vec::new();
        }
        let mut matured = Vec::new();
        quarantined.retain(|(barrier, extents)| {
            if readers.drained_past(*barrier) {
                matured.extend_from_slice(extents);
                false
            } else {
                true
            }
        });
        matured
    }

    /// Extents held back by a run that timed out waiting for older queries.
    pub fn quarantined_extents(&self) -> usize {
        self.quarantined.lock().iter().map(|(_, e)| e.len()).sum()
    }

    /// Replaces the free list with `candidates`, minus anything still on loan.
    ///
    /// A full replacement rather than a merge: the scan derives the list from
    /// authoritative storage, so anything it did not find is either live or
    /// on loan, and either way must not stay on the list.
    fn publish(&self, mut candidates: Vec<Extent>) {
        // Both halves under one lock. See `FreeState`.
        let state = &mut *self.state.lock();

        if !state.in_flight.is_empty() {
            let loans: Vec<Extent> = state
                .in_flight
                .iter()
                .map(|(&start, &len)| Extent { start, len })
                .collect();
            candidates = subtract_ranges(candidates, &loans);
        }

        candidates.retain(|e| e.len >= MIN_SPLIT_BYTES);

        if candidates.len() > MAX_FREE_EXTENTS {
            // Keep the biggest: they satisfy the most requests per entry.
            candidates.sort_unstable_by_key(|e| std::cmp::Reverse(e.len));
            candidates.truncate(MAX_FREE_EXTENTS);
        }

        let total: u64 = candidates.iter().map(|e| e.len).sum();
        state.extents = candidates.into_iter().map(|e| (e.len, e.start)).collect();
        self.free_bytes.store(total, Ordering::Relaxed);
    }

    /// Drops every extent. Loans are left alone: they describe bytes a writer
    /// is using, which is true whether or not the list exists.
    pub fn clear(&self) {
        self.state.lock().extents.clear();
        self.free_bytes.store(0, Ordering::Relaxed);
    }

    /// Turns reclamation on or off for this table at runtime.
    ///
    /// Switching off drops the list, so nothing further is handed out. Reads
    /// stop registering epochs on the next query, which is safe in that
    /// direction only — extents already published were cleared by the same
    /// call, and no new ones can appear while it stays off.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
        if !enabled {
            self.clear();
        }
    }
}

/// `ranges` minus `holes`, both half-open. Neither input need be sorted.
fn subtract_ranges(ranges: Vec<Extent>, holes: &[Extent]) -> Vec<Extent> {
    let mut sorted_holes: Vec<Extent> = holes.to_vec();
    sorted_holes.sort_unstable_by_key(|h| h.start);

    let mut out = Vec::with_capacity(ranges.len());
    for range in ranges {
        let mut cursor = range.start;
        let end = range.end();
        // Only holes overlapping [cursor, end) matter.
        let first = sorted_holes.partition_point(|h| h.end() <= cursor);
        for hole in &sorted_holes[first..] {
            if hole.start >= end {
                break;
            }
            if hole.start > cursor {
                out.push(Extent {
                    start: cursor,
                    len: hole.start - cursor,
                });
            }
            cursor = cursor.max(hole.end());
            if cursor >= end {
                break;
            }
        }
        if cursor < end {
            out.push(Extent {
                start: cursor,
                len: end - cursor,
            });
        }
    }
    out
}

// ---------------------------------------------------------------------------
// What counts as dead
// ---------------------------------------------------------------------------

/// What the transaction system can vouch for, sampled once per sweep.
///
/// Reclamation frees bytes on the strength of "nothing will ever read this
/// again". Without versioning that is the `deleted` flag. With it, the flag is
/// not enough: a row whose `xmax` is set is invisible to *newer* snapshots but
/// still visible to older ones, and freeing its extent lets a later insert
/// overwrite bytes an open transaction is about to read.
///
/// `oldest_active` is a **bound, not a sample**. Nothing below it can still be
/// running, and a transaction starting from here on gets a higher id, so it
/// cannot see a row that a committed transaction below the bound deleted. That
/// is what makes a decision taken during the sweep still true when the sweep
/// ends.
#[derive(Clone)]
pub struct VersionHorizon {
    manager: Option<Arc<TransactionManager>>,
    oldest_active: TxId,
}

impl VersionHorizon {
    /// The horizon for a table with no versioning attached: `deleted` is the
    /// whole story, exactly as before MVCC existed.
    pub fn unversioned() -> Self {
        Self {
            manager: None,
            oldest_active: NO_TX,
        }
    }

    /// Takes the bound once, for the whole sweep.
    pub fn sample(manager: &Arc<TransactionManager>) -> Self {
        Self {
            manager: Some(manager.clone()),
            oldest_active: manager.oldest_active(),
        }
    }

    #[inline]
    pub fn is_versioned(&self) -> bool {
        self.manager.is_some()
    }

    /// The bound this horizon was taken at. Diagnostics only.
    #[inline]
    pub fn oldest_active(&self) -> TxId {
        self.oldest_active
    }

    /// Whether a version is dead for every snapshot that will ever be taken.
    ///
    /// The three conjuncts each rule out a way of being wrong:
    ///
    /// * `xmax != NO_TX` — nothing deleted it, so it is simply live.
    /// * committed — an *aborted* delete did not happen, so the row is still
    ///   live. Treating an abort as a delete would discard a row nobody
    ///   removed, which is the worst failure available here.
    /// * below the bound — a transaction older than `xmax` may still be running
    ///   and still able to see the version.
    #[inline]
    pub fn is_reclaimable(&self, deleted: bool, xmin: TxId, xmax: TxId) -> bool {
        let Some(manager) = self.manager.as_ref() else {
            return deleted;
        };

        // A version whose *creator* aborted was never visible to anything and
        // never will be, so unlike a dead-but-once-live version there is no
        // snapshot to outlive: no bound check applies. `FROZEN_TX` and `NO_TX`
        // are not real transactions to ask about — they mean "older than
        // versioning", which is the opposite of aborted.
        //
        // Without this, every rolled-back `INSERT` leaked its bytes
        // permanently: `xmax` is `NO_TX` and `deleted` is false, so the checks
        // below both decline it and no later event ever sets either field.
        if xmin != FROZEN_TX && xmin != NO_TX && manager.outcome(xmin) == TxOutcome::Aborted {
            return true;
        }

        if xmax != NO_TX {
            return manager.outcome(xmax) == TxOutcome::Committed && xmax < self.oldest_active;
        }

        // No `xmax`, but marked deleted: the row predates versioning, so no
        // snapshot that could exist is older than versioning itself.
        //
        // This is only sound while nothing sets `deleted` *instead of* stamping
        // `xmax`. Attaching a manager to a table whose `DELETE` still writes the
        // flag would have this free rows an open snapshot can see — which is why
        // `Table::enable_versioning` documents that ordering as its
        // precondition rather than leaving it to be discovered.
        deleted
    }

    /// The horizon implied by a reader's visibility rule.
    ///
    /// Scans need this to skip versions whose extents may already have been
    /// freed. They cannot use `Visibility::visible` for that: the query cache is
    /// keyed by *predicate*, so the predicate has to be evaluated for rows this
    /// snapshot cannot see, or one snapshot's cache entry would not serve
    /// another's. `is_reclaimable` is the strictly weaker test that is safe
    /// there — by `mvcc::visibility::reclamation_lemma` a reclaimable version is
    /// invisible to every snapshot that can still be taken, so skipping it
    /// removes nothing any cache entry could need.
    pub fn for_visibility(visibility: &Visibility) -> Self {
        match visibility {
            Visibility::AsOf { manager, .. } => VersionHorizon::sample(manager),
            // No manager, so nothing can be known to be dead; the caller's own
            // `deleted` check remains the whole rule, as it was before versions.
            Visibility::All => VersionHorizon::unversioned(),
        }
    }
}

/// What a sweep needs beyond the storage it reads.
///
/// A struct rather than three more positional parameters, and it keeps the two
/// pieces of state a sweep mutates — the filter and the clearing bound — next to
/// the horizon that decides when to touch them.
pub struct ReclaimContext<'a> {
    pub horizon: &'a VersionHorizon,
    /// Cleared for versions that have just become dead for everyone. `None`
    /// when the table has no filter.
    pub filters: Option<&'a TableFilters>,
    /// Bound at the last sweep that cleared fingerprints.
    ///
    /// `oldest_active` only ever moves forward, so a version becomes
    /// reclaimable exactly once — when the bound crosses its `xmax`. Comparing
    /// against the previous bound turns "has this slot been cleared already"
    /// into one integer test, instead of a set of every slot ever cleared.
    pub cleared_below: &'a AtomicU64,
}

impl ReclaimContext<'_> {
    /// Whether this sweep is the one that should clear `xmax`'s fingerprints.
    #[inline]
    fn should_clear(&self, xmax: TxId, previous_bound: TxId) -> bool {
        // Unversioned tables clear at delete time, and re-clearing every sweep
        // would churn the filter's write counter into constant checkpoints.
        self.horizon.is_versioned() && self.filters.is_some() && xmax >= previous_bound
    }
}

impl std::fmt::Debug for VersionHorizon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionHorizon")
            .field("versioned", &self.manager.is_some())
            .field("oldest_active", &self.oldest_active)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

/// What a full compaction moved and gave back.
#[derive(Debug, Default, Clone, Copy)]
pub struct CompactReport {
    pub live_rows: u64,
    pub dead_rows: u64,
    /// Live rows whose bytes were moved down to close a gap.
    pub rows_moved: u64,
    /// Live pointer records moved down to close a tombstone.
    pub slots_moved: u64,
    /// Bytes the rows file gave back to the filesystem.
    pub rows_bytes_freed: u64,
    /// Bytes the pointer file gave back.
    pub pointer_bytes_freed: u64,
    /// Set when the provider cannot truncate: everything was still compacted
    /// into the front of the files, but the tail stays allocated.
    pub truncation_unsupported: bool,
}

/// One live row's pointer, and where its record sits.
#[derive(Debug, Clone)]
pub struct LiveRecord {
    pub slot: u64,
    pub pointer: RowPointer,
}

/// Every live pointer, in slot order.
///
/// Compaction needs the whole set before it moves anything: it assigns targets
/// by walking rows in position order, which cannot be done incrementally.
pub async fn collect_live<S: StorageIO>(
    io_pointers: &S,
    rows_len: u64,
    horizon: &VersionHorizon,
) -> std::io::Result<(Vec<LiveRecord>, u64)> {
    let mut live = Vec::new();
    let mut slot = 0u64;
    let mut dead = 0u64;

    sweep_pointers(io_pointers, |pointer| {
        // Compaction *discards* what it does not collect, so a version still
        // visible to an open snapshot has to be carried across even though it is
        // invisible to newer ones. Same predicate as the sweep, for the same
        // reason, with a worse failure mode if it is wrong.
        //
        if horizon.is_reclaimable(pointer.deleted, pointer.xmin, pointer.xmax) {
            dead += 1;
        } else if clamp_to_file(pointer.position, pointer.length as u64, rows_len).is_some() {
            live.push(LiveRecord {
                slot,
                pointer: pointer.clone(),
            });
        } else {
            // A record that points outside the rows file describes nothing that
            // can be read. Carrying it through compaction would give it a fresh
            // slot and make it permanent.
            log::warn!(
                "Compaction dropping pointer at slot {}: position {} length {} lies outside a \
                 {}-byte rows file",
                slot,
                pointer.position,
                pointer.length,
                rows_len
            );
            dead += 1;
        }
        slot += 1;
    })
    .await?;

    Ok((live, dead))
}

/// Plans where each row's bytes go, packed from offset zero in position order.
///
/// Returns the moves that actually change anything, and the compacted length.
/// Rows are visited in increasing source position and every target is at or
/// below its source, so a move can never overwrite bytes a later move still
/// needs to read — which is what lets compaction run in place, with no second
/// copy of the table on disk.
pub fn plan_row_moves(live: &[LiveRecord]) -> (Vec<(usize, u64)>, u64) {
    let mut order: Vec<usize> = (0..live.len()).collect();
    order.sort_unstable_by_key(|&i| live[i].pointer.position);

    let mut moves = Vec::new();
    let mut cursor = 0u64;
    for i in order {
        let pointer = &live[i].pointer;
        debug_assert!(
            pointer.position >= cursor,
            "live rows overlap: {} < {}",
            pointer.position,
            cursor
        );
        if pointer.position != cursor {
            moves.push((i, cursor));
        }
        cursor += pointer.length as u64;
    }
    (moves, cursor)
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// What the last scan found, plus running totals. All fields are bytes unless
/// named otherwise.
#[derive(Debug, Default, Clone, Copy)]
pub struct VacuumReport {
    /// Pointer records examined, tombstones included.
    pub slots: u64,
    pub live_rows: u64,
    pub dead_rows: u64,
    pub live_bytes: u64,
    /// Bytes below the append frontier that no live pointer refers to. Counts
    /// tombstoned rows and the slack an in-place shrinking update leaves behind.
    pub dead_bytes: u64,
    /// Bytes actually published to the free list. Lower than `dead_bytes` when
    /// candidates were still on loan or too small to be worth tracking.
    pub reclaimed_bytes: u64,
    pub extents: u64,
    /// Set when a batch was computed but withheld because an older query was
    /// still running.
    pub deferred: bool,
    /// Filter slots whose fingerprints this sweep cleared, having just
    /// established that no snapshot can reach their version.
    pub fingerprints_cleared: u64,
}

impl VacuumReport {
    /// Fraction of the heap below the append frontier that is dead.
    pub fn dead_ratio(&self) -> f64 {
        let total = self.live_bytes + self.dead_bytes;
        if total == 0 {
            0.0
        } else {
            self.dead_bytes as f64 / total as f64
        }
    }
}

/// Cumulative counters for one table's reclamation, readable at any time.
#[derive(Debug, Default)]
pub struct VacuumStats {
    pub runs: AtomicU64,
    pub compactions: AtomicU64,
    pub deferrals: AtomicU64,
    pub last_dead_bytes: AtomicU64,
    pub last_live_bytes: AtomicU64,
    pub last_reclaimed_bytes: AtomicU64,
    pub total_reclaimed_bytes: AtomicU64,
}

impl VacuumStats {
    fn record(&self, report: &VacuumReport) {
        self.runs.fetch_add(1, Ordering::Relaxed);
        self.last_dead_bytes
            .store(report.dead_bytes, Ordering::Relaxed);
        self.last_live_bytes
            .store(report.live_bytes, Ordering::Relaxed);
        self.last_reclaimed_bytes
            .store(report.reclaimed_bytes, Ordering::Relaxed);
        if report.deferred {
            self.deferrals.fetch_add(1, Ordering::Relaxed);
        }
        if report.reclaimed_bytes > 0 {
            self.compactions.fetch_add(1, Ordering::Relaxed);
            self.total_reclaimed_bytes
                .fetch_add(report.reclaimed_bytes, Ordering::Relaxed);
        }
    }
}

// ---------------------------------------------------------------------------
// The scan
// ---------------------------------------------------------------------------

/// Pointer records read per I/O. Sized to keep the read count low without
/// holding a large buffer for the lifetime of the scan.
const SCAN_RECORDS_PER_CHUNK: usize = 4096;

/// Above this many live rows the scan stops building a map of the whole heap.
/// At 16 bytes an entry, tracking every row of a hundred-million-row table
/// would cost more memory than the space it is trying to give back.
const MAX_TRACKED_LIVE_EXTENTS: usize = 2 << 20;

/// Enough re-reads to catch up with a table under write load; past this the
/// tail is left for the next run rather than spinning here.
///
/// Stopping early cannot free a live extent. A row appended since the sweep
/// began sits past the frontier the sweep observed, and one written into a
/// reclaimed extent is still shielded by its loan, which only a sweep that
/// *sees* its pointer retires.
const MAX_CATCHUP_PASSES: usize = 8;

struct ScanResult {
    slots: u64,
    live_rows: u64,
    dead_rows: u64,
    live_bytes: u64,
    live: Vec<Extent>,
    /// Extents of tombstoned rows. Always collected: they are the starting
    /// point for the capped path.
    dead: Vec<Extent>,
    fingerprints_cleared: u64,
    /// Set when the live map was abandoned for `max_tracked_live_extents`, so
    /// dead space is derived from tombstones rather than from the gaps between
    /// live rows — which does not recover the slack an in-place shrinking
    /// update leaves, and needs the second pass in `run_once` to be sound.
    live_map_dropped: bool,
    /// Highest end offset seen across every pointer, live or dead. Anything
    /// past it belongs to the append frontier and is not ours to hand out.
    frontier: u64,
}

/// Walks every pointer record of `io`, tombstones included, calling `visit` on
/// each.
///
/// Deliberately not `RowPointerIterator`: that skips tombstones, and a tombstone
/// is exactly what marks an extent dead. It also stops at the length it
/// snapshotted on construction, whereas this re-reads the length each pass so a
/// pointer appended mid-sweep is still observed — without which a row written
/// into a just-allocated extent could be missed.
async fn sweep_pointers<S: StorageIO, F: FnMut(&RowPointer)>(
    io: &S,
    mut visit: F,
) -> std::io::Result<()> {
    const RECORD: usize = ROW_POINTER_RECORD_LEN;

    let mut buffer = vec![0u8; RECORD * SCAN_RECORDS_PER_CHUNK];
    let mut scanned_to: u64 = 0;
    let mut caught_up = false;

    for _ in 0..MAX_CATCHUP_PASSES {
        let len = io.get_len().await;
        let boundary = len - (len % RECORD as u64);
        if boundary <= scanned_to {
            caught_up = true;
            break;
        }

        while scanned_to < boundary {
            let want = ((boundary - scanned_to) as usize).min(buffer.len());
            let chunk = &mut buffer[..want];
            let mut position = scanned_to;
            match io.read_data_into_buffer(&mut position, chunk).await {
                Ok(()) => {}
                // A concurrent append can leave the length ahead of the bytes
                // for an instant. Whatever is missing is past our frontier.
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e),
            }

            for record in chunk.chunks_exact(RECORD) {
                visit(&RowPointer::from_slice_unknown(record));
            }

            scanned_to += want as u64;
        }
    }

    if !caught_up {
        log::debug!(
            "Vacuum sweep stopped at {} bytes with the pointer file still growing; \
             the tail is left for the next run",
            scanned_to
        );
    }

    Ok(())
}

/// Reads every pointer record and reports the live and dead extents.
async fn scan_pointers<S: StorageIO>(
    io_pointers: &S,
    free_space: &FreeSpace,
    rows_len: u64,
    max_tracked_live: usize,
    ctx: &ReclaimContext<'_>,
) -> std::io::Result<ScanResult> {
    let horizon = ctx.horizon;
    let previous_bound = ctx.cleared_below.load(Ordering::Acquire);
    let mut result = ScanResult {
        slots: 0,
        live_rows: 0,
        dead_rows: 0,
        live_bytes: 0,
        live: Vec::new(),
        dead: Vec::new(),
        live_map_dropped: false,
        fingerprints_cleared: 0,
        frontier: 0,
    };

    sweep_pointers(io_pointers, |pointer| {
        let slot = result.slots;
        result.slots += 1;

        // Nothing beyond the rows file can be reclaimed, and nothing beyond it
        // should be believed. Handing out an offset past the end would have the
        // write path extend the file — sparsely on disk, and by a `Vec::resize`
        // in memory — to satisfy a length field that a torn or corrupt record
        // made up.
        let extent = clamp_to_file(pointer.position, pointer.length as u64, rows_len);
        result.frontier = result.frontier.max(extent.map_or(0, |e| e.end()));

        // A loan shields an extent until a *record* owns it, and a tombstone is
        // a record. Retiring only on a live pointer leaked the space of any row
        // that was written into a reclaimed extent and then deleted before the
        // next sweep — the loan outlived the row and shielded those bytes from
        // every scan thereafter. Under delete-and-refill churn that is the whole
        // working set, once per cycle.
        free_space.retire_loan(pointer.position);

        if horizon.is_reclaimable(pointer.deleted, pointer.xmin, pointer.xmax) {
            result.dead_rows += 1;
            if let Some(extent) = extent {
                result.dead.push(extent);
            }

            // The filter keeps a dead version's fingerprints until nothing can
            // reach it — clearing them earlier makes a scan prune the slot
            // *before decoding the pointer*, so the row goes unreachable
            // underneath the visibility rule rather than invisible through it.
            // Establishing that nothing can reach it is exactly what this sweep
            // just did, so clearing them is now its job.
            //
            // Safe even if this run is later deferred: deferral is about handing
            // out bytes, and this says only that no snapshot sees the version.
            if let Some(filters) = ctx
                .filters
                .filter(|_| ctx.should_clear(pointer.xmax, previous_bound))
            {
                filters.on_delete(slot);
                result.fingerprints_cleared += 1;
            }
        } else {
            result.live_rows += 1;
            result.live_bytes += pointer.length as u64;
            if !result.live_map_dropped {
                if let Some(extent) = extent {
                    result.live.push(extent);
                }
                if result.live.len() > max_tracked_live {
                    result.live = Vec::new();
                    result.live_map_dropped = true;
                }
            }
        }
    })
    .await?;

    Ok(result)
}

/// Second pass for the capped path: the live extents that overlap `dead`.
///
/// A tombstone is not proof its extent is free. Once an extent has been
/// reclaimed and written into, the tombstone that released it is still on disk
/// pointing at bytes a live row now owns — so a tombstone-derived free set would
/// hand out an occupied extent, and the row that landed there would be
/// overwritten.
///
/// The gap-derived path never has this problem: an extent a live row occupies is
/// not a gap. Only the capped path, which starts from tombstones, needs this.
/// Conflicts are limited to extents this table has previously reclaimed, so the
/// result is small even where the live map was too large to hold.
async fn live_extents_overlapping<S: StorageIO>(
    io_pointers: &S,
    dead: &[Extent],
    rows_len: u64,
    horizon: &VersionHorizon,
) -> std::io::Result<Vec<Extent>> {
    let mut conflicts = Vec::new();
    if dead.is_empty() {
        return Ok(conflicts);
    }

    sweep_pointers(io_pointers, |pointer| {
        if horizon.is_reclaimable(pointer.deleted, pointer.xmin, pointer.xmax) {
            return;
        }
        let Some(extent) = clamp_to_file(pointer.position, pointer.length as u64, rows_len) else {
            return;
        };
        // `dead` is sorted and disjoint, so at most two entries can overlap:
        // the last one starting at or before this extent, and the first one
        // starting after it.
        let idx = dead.partition_point(|d| d.start <= extent.start);
        let overlaps = (idx > 0 && dead[idx - 1].end() > extent.start)
            || (idx < dead.len() && dead[idx].start < extent.end());
        if overlaps {
            conflicts.push(extent);
        }
    })
    .await?;

    Ok(conflicts)
}

/// The part of `[position, position + len)` that lies inside a file of
/// `file_len` bytes, or `None` if none of it does.
#[inline]
fn clamp_to_file(position: u64, len: u64, file_len: u64) -> Option<Extent> {
    if len == 0 || position >= file_len {
        return None;
    }
    let end = position.saturating_add(len).min(file_len);
    Some(Extent {
        start: position,
        len: end - position,
    })
}

/// Coalesces a list of extents into the smallest equivalent set.
///
/// Tombstoned extents never overlap in practice — each row owns its bytes — but
/// adjacent ones are common after a bulk delete, and merging them turns a run of
/// unusably small holes into one extent a row can actually be placed in.
fn merge_overlapping(mut extents: Vec<Extent>) -> Vec<Extent> {
    if extents.is_empty() {
        return extents;
    }
    extents.sort_unstable_by_key(|e| e.start);

    let mut out: Vec<Extent> = Vec::with_capacity(extents.len());
    for extent in extents {
        match out.last_mut() {
            Some(last) if extent.start <= last.end() => {
                let end = last.end().max(extent.end());
                last.len = end - last.start;
            }
            _ => out.push(extent),
        }
    }
    out
}

/// Byte ranges below `frontier` that no live row occupies.
///
/// Live extents cannot overlap — each row owns its bytes outright — so sorting
/// by start and walking the gaps is exact. In-place updates that shrank a row
/// show up here too: their slack is simply a gap no pointer claims.
fn dead_extents(mut live: Vec<Extent>, frontier: u64) -> Vec<Extent> {
    live.sort_unstable_by_key(|e| e.start);

    let mut gaps = Vec::new();
    let mut cursor = 0u64;
    for extent in &live {
        if extent.start > cursor {
            // Adjacent holes have already merged: `cursor` only advances past a
            // live row, so each gap runs from one live row to the next.
            gaps.push(Extent {
                start: cursor,
                len: extent.start - cursor,
            });
        }
        cursor = cursor.max(extent.end());
    }
    if frontier > cursor {
        gaps.push(Extent {
            start: cursor,
            len: frontier - cursor,
        });
    }
    gaps
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Thresholds for the background service.
#[derive(Debug, Clone, Copy)]
pub struct VacuumConfig {
    /// How long the service sleeps between scans.
    pub interval: std::time::Duration,
    /// Skip publishing below this much dead space: a scan of a table with a few
    /// stale rows is not worth the free-list churn.
    pub min_dead_bytes: u64,
    /// …or below this fraction of the heap.
    pub min_dead_ratio: f64,
    /// How long to keep re-checking whether older queries have drained before
    /// giving up on a batch and leaving it for the next run.
    pub drain_timeout: std::time::Duration,
    /// Live rows above which the scan stops mapping the whole heap and derives
    /// dead space from tombstones instead. Exposed so the capped path can be
    /// exercised without building a table large enough to trip it naturally.
    pub max_tracked_live_extents: usize,
}

impl Default for VacuumConfig {
    fn default() -> Self {
        Self {
            interval: std::time::Duration::from_secs(crate::vacuum_interval_secs().max(1)),
            min_dead_bytes: crate::vacuum_min_dead_bytes(),
            min_dead_ratio: crate::vacuum_min_dead_ratio(),
            drain_timeout: std::time::Duration::from_secs(crate::vacuum_drain_timeout_secs()),
            max_tracked_live_extents: MAX_TRACKED_LIVE_EXTENTS,
        }
    }
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// Runs one scan and, if it is worth it, publishes the result.
///
/// `force` bypasses the `min_dead_*` thresholds, for an explicit request.
pub async fn run_once<S: StorageIO>(
    io_pointers: &S,
    io_rows: &S,
    free_space: &FreeSpace,
    readers: &ReaderRegistry,
    ctx: &ReclaimContext<'_>,
    config: &VacuumConfig,
    force: bool,
) -> std::io::Result<VacuumReport> {
    let horizon = ctx.horizon;
    // Read before the pointer sweep. A row appended during the scan lands past
    // this, so clamping to it can only ever exclude bytes, never include ones a
    // pointer has since claimed.
    let rows_len = io_rows.get_len().await;
    let scan = scan_pointers(
        io_pointers,
        free_space,
        rows_len,
        config.max_tracked_live_extents,
        ctx,
    )
    .await?;

    // Only after the sweep has finished, so a crash part-way leaves the bound
    // where it was and the next run re-clears rather than skipping.
    ctx.cleared_below
        .fetch_max(horizon.oldest_active(), Ordering::AcqRel);
    let live_bytes = scan.live_bytes;

    let candidates = if scan.live_map_dropped {
        // Too many live rows to map the heap, so dead space comes from
        // tombstones. That needs a second pass: a tombstone whose extent has
        // since been reclaimed and written into still points at bytes a live
        // row now owns, and handing those out would overwrite it.
        // Info, not warn: this is the normal operating mode for a large table,
        // and it fires every tick. What it costs is the in-place-update slack,
        // not correctness.
        info!(
            "Vacuum: {} live rows exceeds the tracking limit of {}; deriving dead space from \
             tombstones, so slack from in-place updates is left in place",
            scan.live_rows, config.max_tracked_live_extents
        );
        let dead = merge_overlapping(scan.dead);
        let conflicts = live_extents_overlapping(io_pointers, &dead, rows_len, horizon).await?;
        if !conflicts.is_empty() {
            log::debug!(
                "Vacuum: {} tombstoned extents have since been reused; withholding them",
                conflicts.len()
            );
        }
        subtract_ranges(dead, &conflicts)
    } else {
        dead_extents(scan.live, scan.frontier)
    };
    let dead_bytes: u64 = candidates.iter().map(|e| e.len).sum();

    let mut report = VacuumReport {
        slots: scan.slots,
        live_rows: scan.live_rows,
        dead_rows: scan.dead_rows,
        live_bytes,
        dead_bytes,
        reclaimed_bytes: 0,
        extents: candidates.len() as u64,
        deferred: false,
        fingerprints_cleared: scan.fingerprints_cleared,
    };

    if !force {
        let ratio = report.dead_ratio();
        if dead_bytes < config.min_dead_bytes && ratio < config.min_dead_ratio {
            return Ok(report);
        }
    }

    // Stamped after the scan: a query holding a ticket at or beyond this one
    // started after every tombstone the scan relied on was durable, so it
    // cannot be holding a pointer into any of these extents.
    let barrier = readers.barrier();
    let deadline = std::time::Instant::now() + config.drain_timeout;
    let mut drained = true;
    while !readers.drained_past(barrier) {
        if std::time::Instant::now() >= deadline {
            drained = false;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    if !drained {
        // A table whose queries always overlap the scan would otherwise never
        // reclaim anything at all, however long its dead space had been dead.
        //
        // Extents carried over from an earlier run are already safe: they were
        // stamped with a barrier that has since been passed, which is a fact
        // about the past that a query starting now cannot undo. So publish
        // those and hold back only the ones this run just found.
        let ready = free_space.take_matured(readers);
        report.deferred = true;
        if ready.is_empty() {
            log::warn!(
                "Vacuum deferred after {:?}: a query older than epoch {} is still running, \
                 leaving {} bytes unreclaimed",
                config.drain_timeout,
                barrier,
                dead_bytes
            );
            free_space.quarantine(candidates, barrier);
            return Ok(report);
        }

        log::info!(
            "Vacuum publishing {} extents held over from an earlier run; a query older than \
             epoch {} is still running, so this run's findings wait",
            ready.len(),
            barrier
        );
        free_space.publish(ready);
        free_space.quarantine(candidates, barrier);
        report.reclaimed_bytes = free_space.free_bytes();
        report.extents = free_space.len() as u64;
        return Ok(report);
    }

    // Anything a previous run had to hold back is safe now too: this run's
    // barrier is at least as recent as theirs.
    let mut candidates = candidates;
    candidates.extend(free_space.take_matured(readers));
    free_space.publish(candidates);
    report.reclaimed_bytes = free_space.free_bytes();
    report.extents = free_space.len() as u64;
    Ok(report)
}

/// Everything the per-table reclamation service needs to run.
///
/// A struct rather than nine positional parameters: most of them are `Arc`s of
/// similar shape, and at a call site they would be indistinguishable.
pub struct VacuumService<S: StorageIO> {
    pub table_name: String,
    pub io_pointers: rclite::Arc<S>,
    pub io_rows: rclite::Arc<S>,
    pub free_space: Arc<FreeSpace>,
    pub readers: Arc<ReaderRegistry>,
    pub stats: Arc<VacuumStats>,
    /// Cleared when the table is dropped; the loop stops on the next slice.
    pub alive: Arc<CacheGuard<AtomicBool>>,
    /// Bumped by every committed `DELETE` and `UPDATE`. Nothing else can create
    /// dead space, so an unchanged value means there is nothing new to find.
    pub mutations: Arc<CacheGuard<AtomicU64>>,
    /// Picked up whenever it is set: the service starts before versioning can
    /// be wired, so it cannot take a horizon once and keep it.
    pub versions: Arc<arc_swap::ArcSwapOption<TransactionManager>>,
    /// Cleared for versions that have just become unreachable. Shared with the
    /// table so an explicit `VACUUM` and the service agree on what is already
    /// cleared.
    pub filters: Option<Arc<TableFilters>>,
    pub cleared_below: Arc<CacheGuard<AtomicU64>>,
    pub config: VacuumConfig,
}

impl<S: StorageIO> VacuumService<S> {
    /// Starts the service on its own task.
    ///
    /// Returns immediately; the loop stops on its own once `alive` goes false,
    /// which `Table::drop` does, so the task cannot outlive the storage it
    /// scans.
    pub fn spawn(self) {
        tokio::spawn(async move {
            // No value of the counter can equal this, so the first tick always
            // scans — a table opened with tombstones already on disk has dead
            // space that no mutation of ours will ever announce.
            let mut scanned_at_mutation = u64::MAX;

            loop {
                if !sleep_until_due(self.config.interval, &self.alive).await {
                    break;
                }
                if !self.free_space.is_enabled() {
                    continue;
                }

                // A table that is only read, or only appended to, gains no dead
                // space. Re-reading its whole pointer file every tick would be
                // pure cost — and on a large table, not a small one. Outstanding
                // loans still force a scan: they are what retires them.
                let mutations_now = self.mutations.load(Ordering::Acquire);
                if mutations_now == scanned_at_mutation && self.free_space.loans_outstanding() == 0
                {
                    continue;
                }

                let horizon = match self.versions.load_full() {
                    Some(manager) => VersionHorizon::sample(&manager),
                    None => VersionHorizon::unversioned(),
                };
                let ctx = ReclaimContext {
                    horizon: &horizon,
                    filters: self.filters.as_deref(),
                    cleared_below: &self.cleared_below,
                };

                match run_once(
                    &*self.io_pointers,
                    &*self.io_rows,
                    &self.free_space,
                    &self.readers,
                    &ctx,
                    &self.config,
                    false,
                )
                .await
                {
                    Ok(report) => {
                        self.stats.record(&report);
                        // Losing this to a crash costs pruning quality, never
                        // correctness: the mask stays a superset, so a
                        // checkpoint predating the clearing is merely less
                        // selective.
                        if let Some(filters) = self
                            .filters
                            .as_ref()
                            .filter(|_| report.fingerprints_cleared > 0)
                        {
                            filters.request_checkpoint();
                        }
                        // Only a run that got as far as publishing has actually
                        // seen everything; a deferred one must try again.
                        if !report.deferred {
                            scanned_at_mutation = mutations_now;
                        }
                        if report.reclaimed_bytes > 0 {
                            info!(
                                "Vacuum on {}: {} dead rows, {} bytes reclaimable across {} extents ({:.1}% of heap)",
                                self.table_name,
                                report.dead_rows,
                                report.reclaimed_bytes,
                                report.extents,
                                report.dead_ratio() * 100.0
                            );
                        }
                    }
                    Err(e) => {
                        log::error!("Vacuum scan failed for {}: {}", self.table_name, e);
                    }
                }
            }

            log::debug!("Vacuum service for {} stopped", self.table_name);
        });
    }
}

/// Sleeps for `interval`, waking early if the table goes away.
///
/// Returns `false` when the service should stop. Sleeping the whole interval in
/// one call would keep a dropped table's storage alive for up to that long, and
/// have the next tick scan files that are being removed underneath it.
async fn sleep_until_due(interval: std::time::Duration, alive: &AtomicBool) -> bool {
    const SLICE: std::time::Duration = std::time::Duration::from_millis(250);

    let deadline = std::time::Instant::now() + interval;
    loop {
        if !alive.load(Ordering::Relaxed) {
            return false;
        }
        let now = std::time::Instant::now();
        if now >= deadline {
            return alive.load(Ordering::Relaxed);
        }
        tokio::time::sleep(SLICE.min(deadline - now)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(start: u64, len: u64) -> Extent {
        Extent { start, len }
    }

    #[test]
    fn gaps_between_live_extents_are_dead() {
        let live = vec![e(0, 10), e(30, 10), e(60, 10)];
        assert_eq!(dead_extents(live, 70), vec![e(10, 20), e(40, 20)]);
    }

    #[test]
    fn trailing_slack_below_the_frontier_is_dead() {
        // A shrinking in-place update leaves the tail of its old extent unowned.
        let live = vec![e(0, 10)];
        assert_eq!(dead_extents(live, 25), vec![e(10, 15)]);
    }

    #[test]
    fn a_fully_live_heap_has_no_gaps() {
        let live = vec![e(0, 10), e(10, 10)];
        assert!(dead_extents(live, 20).is_empty());
    }

    #[test]
    fn subtraction_splits_a_partially_lent_extent() {
        let ranges = vec![e(100, 200)];
        let loans = vec![e(100, 60)];
        assert_eq!(subtract_ranges(ranges, &loans), vec![e(160, 140)]);
    }

    #[test]
    fn subtraction_removes_a_fully_lent_extent() {
        assert!(subtract_ranges(vec![e(0, 50)], &[e(0, 50)]).is_empty());
    }

    #[test]
    fn subtraction_handles_multiple_holes_in_one_range() {
        let out = subtract_ranges(vec![e(0, 100)], &[e(10, 10), e(50, 10)]);
        assert_eq!(out, vec![e(0, 10), e(20, 30), e(60, 40)]);
    }

    #[test]
    fn best_fit_prefers_the_smallest_sufficient_extent() {
        let fs = FreeSpace::new(true);
        fs.publish(vec![e(0, 1000), e(2000, 100), e(3000, 500)]);
        // 90 fits in the 100-byte extent; the split leftover is under
        // MIN_SPLIT_BYTES so the whole extent is consumed.
        assert_eq!(fs.alloc(90), Some(2000));
        assert_eq!(fs.alloc(400), Some(3000));
        assert_eq!(fs.alloc(900), Some(0));
        assert_eq!(fs.alloc(10_000), None);
    }

    #[test]
    fn a_split_returns_its_remainder_to_the_list() {
        let fs = FreeSpace::new(true);
        fs.publish(vec![e(0, 500)]);
        assert_eq!(fs.alloc(100), Some(0));
        // 400 bytes left at offset 100.
        assert_eq!(fs.alloc(400), Some(100));
        assert_eq!(fs.alloc(1), None);
    }

    #[test]
    fn a_disabled_list_never_allocates() {
        let fs = FreeSpace::new(false);
        fs.publish(vec![e(0, 500)]);
        assert_eq!(fs.alloc(10), None);
    }

    #[test]
    fn publishing_excludes_extents_still_on_loan() {
        let fs = FreeSpace::new(true);
        fs.publish(vec![e(0, 500)]);
        assert_eq!(fs.alloc(100), Some(0));
        // A scan that raced the write sees the extent as unowned and offers it
        // again; the loan must keep it out of the new list.
        fs.publish(vec![e(0, 500)]);
        // Only the 400 bytes past the loan come back.
        assert_eq!(fs.free_bytes(), 400);
        assert_eq!(fs.alloc(400), Some(100));
        assert_eq!(
            fs.alloc(1),
            None,
            "the lent 100 bytes must not be handed out twice"
        );
    }

    #[test]
    fn a_retired_loan_stops_shielding_its_extent() {
        let fs = FreeSpace::new(true);
        fs.publish(vec![e(0, 500)]);
        assert_eq!(fs.alloc(100), Some(0));
        fs.retire_loan(0);
        fs.publish(vec![e(0, 500)]);
        assert_eq!(fs.alloc(500), Some(0));
    }

    #[test]
    fn merging_coalesces_adjacent_holes() {
        // A bulk delete leaves a run of touching extents; as separate entries
        // none of them may fit a row that all of them together would.
        let merged = merge_overlapping(vec![e(0, 10), e(10, 10), e(20, 10)]);
        assert_eq!(merged, vec![e(0, 30)]);
    }

    #[test]
    fn merging_keeps_separated_holes_apart() {
        let merged = merge_overlapping(vec![e(30, 10), e(0, 10)]);
        assert_eq!(merged, vec![e(0, 10), e(30, 10)]);
    }

    #[test]
    fn merging_absorbs_a_contained_extent() {
        assert_eq!(
            merge_overlapping(vec![e(0, 100), e(20, 10)]),
            vec![e(0, 100)]
        );
    }

    #[test]
    fn merging_an_empty_list_is_empty() {
        assert!(merge_overlapping(Vec::new()).is_empty());
    }

    #[test]
    fn an_extent_past_the_end_of_the_file_is_dropped() {
        assert_eq!(clamp_to_file(0, 10, 100), Some(e(0, 10)));
        assert_eq!(clamp_to_file(90, 10, 100), Some(e(90, 10)));
        // A length field that runs past the end is trusted only up to the end.
        assert_eq!(clamp_to_file(90, 1_000, 100), Some(e(90, 10)));
        // A position past the end is not trusted at all.
        assert_eq!(clamp_to_file(200, 10, 100), None);
        assert_eq!(clamp_to_file(100, 10, 100), None);
        assert_eq!(clamp_to_file(0, 0, 100), None);
        // And nothing wraps.
        assert_eq!(clamp_to_file(u64::MAX - 1, u64::MAX, 100), None);
        assert_eq!(clamp_to_file(50, u64::MAX, 100), Some(e(50, 50)));
    }

    #[test]
    fn a_saturating_end_cannot_wrap_into_live_data() {
        assert_eq!(
            Extent {
                start: u64::MAX - 5,
                len: 100
            }
            .end(),
            u64::MAX
        );
    }

    #[test]
    fn a_failed_write_gives_its_extent_back() {
        let fs = FreeSpace::new(true);
        fs.publish(vec![e(0, 500)]);
        let start = fs.alloc(500).unwrap();
        assert_eq!(fs.loans_outstanding(), 1);

        // The loan would otherwise shield those bytes from every future scan.
        fs.release_failed_loan(start);
        assert_eq!(fs.loans_outstanding(), 0);
        assert_eq!(fs.leaked_loans(), 1);

        // Not straight back on the list — a scan is what re-establishes that
        // nothing owns them.
        assert_eq!(fs.alloc(1), None);
        fs.publish(vec![e(0, 500)]);
        assert_eq!(fs.alloc(500), Some(0));
    }

    #[test]
    fn releasing_a_loan_that_was_never_taken_does_nothing() {
        let fs = FreeSpace::new(true);
        fs.release_failed_loan(1234);
        assert_eq!(fs.leaked_loans(), 0);
    }

    #[test]
    fn disabling_at_runtime_empties_the_list() {
        let fs = FreeSpace::new(true);
        fs.publish(vec![e(0, 500)]);
        fs.set_enabled(false);
        assert!(fs.is_empty());
        assert_eq!(fs.alloc(10), None);
        // And re-enabling does not resurrect what was dropped.
        fs.set_enabled(true);
        assert_eq!(fs.alloc(10), None);
    }

    /// Concurrent allocation must never hand the same byte to two rows.
    ///
    /// The failure this guards against is a torn heap, not a panic: two rows
    /// written over each other read back as plausible values.
    #[test]
    fn concurrent_allocation_never_overlaps() {
        use std::sync::atomic::AtomicUsize;

        const EXTENT: u64 = 4096;
        const ROW: u64 = 64;
        const THREADS: usize = 8;

        let fs = Arc::new(FreeSpace::new(true));
        fs.publish(vec![e(0, EXTENT)]);

        let claimed = Arc::new(Mutex::new(Vec::<u64>::new()));
        let exhausted = Arc::new(AtomicUsize::new(0));

        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                let fs = fs.clone();
                let claimed = claimed.clone();
                let exhausted = exhausted.clone();
                scope.spawn(move || {
                    loop {
                        match fs.alloc(ROW) {
                            Some(start) => claimed.lock().push(start),
                            None => {
                                exhausted.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                        }
                    }
                });
            }
        });

        let mut starts = claimed.lock().clone();
        let handed_out = starts.len();
        starts.sort_unstable();
        starts.dedup();
        assert_eq!(starts.len(), handed_out, "an offset was handed out twice");

        // Every claim is a distinct ROW-sized slice of the one extent.
        assert_eq!(handed_out as u64, EXTENT / ROW);
        for pair in starts.windows(2) {
            assert!(
                pair[1] - pair[0] >= ROW,
                "claims at {} and {} overlap",
                pair[0],
                pair[1]
            );
        }
        assert_eq!(exhausted.load(Ordering::Relaxed), THREADS);
    }

    /// Publishing while writers allocate must not put a lent extent back on the
    /// list. With the list and the loans under separate locks this raced: a
    /// writer could claim an extent in the window between publication reading
    /// the loans and installing the list.
    #[test]
    fn publishing_concurrently_with_allocation_never_double_hands_an_extent() {
        const EXTENT: u64 = 8192;
        const ROW: u64 = 64;

        for _ in 0..200 {
            let fs = Arc::new(FreeSpace::new(true));
            fs.publish(vec![e(0, EXTENT)]);

            let claimed = Arc::new(Mutex::new(Vec::<u64>::new()));

            std::thread::scope(|scope| {
                for _ in 0..4 {
                    let fs = fs.clone();
                    let claimed = claimed.clone();
                    scope.spawn(move || {
                        for _ in 0..16 {
                            if let Some(start) = fs.alloc(ROW) {
                                claimed.lock().push(start);
                            }
                        }
                    });
                }
                // A scan that raced every one of those writers and saw the
                // whole extent as unowned.
                let fs = fs.clone();
                scope.spawn(move || {
                    for _ in 0..16 {
                        fs.publish(vec![e(0, EXTENT)]);
                    }
                });
            });

            let mut starts = claimed.lock().clone();
            let handed_out = starts.len();
            starts.sort_unstable();
            starts.dedup();
            assert_eq!(starts.len(), handed_out, "an offset was handed out twice");
        }
    }

    #[test]
    fn a_quarantined_batch_waits_for_its_barrier_then_matures() {
        let reg = Arc::new(ReaderRegistry::new());
        let fs = FreeSpace::new(true);

        let long_running = reg.enter();
        let barrier = reg.barrier();
        fs.quarantine(vec![e(0, 500)], barrier);
        assert_eq!(fs.quarantined_extents(), 1);

        // The query that predates the barrier is still going.
        assert!(fs.take_matured(&reg).is_empty());
        assert_eq!(fs.quarantined_extents(), 1);

        // A query starting now does not hold the batch back — it cannot have a
        // pointer into an extent that was already dead before it began.
        let _later = reg.enter();
        assert!(fs.take_matured(&reg).is_empty());

        drop(long_running);
        assert_eq!(fs.take_matured(&reg), vec![e(0, 500)]);
        assert_eq!(fs.quarantined_extents(), 0);
    }

    #[test]
    fn a_later_batch_supersedes_an_earlier_one() {
        let reg = Arc::new(ReaderRegistry::new());
        let fs = FreeSpace::new(true);
        let _held = reg.enter();

        // Both derive from the same authoritative sweep, so the newer one is a
        // strictly better answer than the older.
        fs.quarantine(vec![e(0, 100)], reg.barrier());
        fs.quarantine(vec![e(0, 100), e(200, 100)], reg.barrier());
        assert_eq!(fs.quarantined_extents(), 2);
    }

    #[test]
    fn quarantining_nothing_stores_nothing() {
        let reg = Arc::new(ReaderRegistry::new());
        let fs = FreeSpace::new(true);
        fs.quarantine(Vec::new(), reg.barrier());
        assert_eq!(fs.quarantined_extents(), 0);
        assert!(fs.take_matured(&reg).is_empty());
    }

    // -----------------------------------------------------------------------
    // Reclamation under versioning
    // -----------------------------------------------------------------------

    use crate::core::mvcc::TransactionManager;
    use crate::core::mvcc::transaction::{FROZEN_TX, NO_TX};
    use crate::core::mvcc::visibility::Visibility;

    /// Pins the defaults, because nothing else does.
    ///
    /// These were once deleted by an over-broad edit to `lib.rs` and rebuilt
    /// from their call sites — which give names and types but not values, so
    /// every default had to be guessed and three were wrong. A wrong drain
    /// timeout or dead-bytes threshold surfaces as a performance regression or
    /// a flaky test, never as an obvious config error, which is exactly the kind
    /// of thing that survives a long time.
    ///
    /// Changing a value here on purpose is a one-line edit and a note in §9 of
    /// `docs/space-reclamation.md`. Changing one by accident now fails.
    ///
    /// **If this test fails, do not re-derive the number — find out where it
    /// came from.** Most of these are policy rather than measurement and the
    /// justification is in §9. The two that rest on numbers are the interval,
    /// which is set against a sweep cost of ~4.5 ns a row (§15, and re-runnable
    /// with `cargo bench --bench vacuum_benchmarks -- sweep`), and
    /// `max_tracked_live_extents`, which is a memory bound at 16 bytes an entry
    /// rather than a speed one. A threshold that drifts because someone
    /// reasoned about it afresh is the failure this is here to prevent; that is
    /// how the reclamation defaults were once lost and guessed back, three of
    /// seven wrong.
    #[test]
    fn the_reclamation_defaults_are_what_the_documentation_says() {
        assert!(crate::vacuum_enabled(), "reclamation is on by default");
        assert_eq!(crate::vacuum_interval_secs(), 30);
        assert_eq!(crate::vacuum_min_dead_bytes(), 8 * 1024 * 1024);
        assert_eq!(crate::vacuum_min_dead_ratio(), 0.20);
        assert_eq!(crate::vacuum_drain_timeout_secs(), 30);

        // Durability knobs, for the same reason: `core::wal` has no test that
        // pins them either.
        assert!(crate::wal_enabled(), "logging is on by default");
        assert_eq!(crate::wal_checkpoint_bytes(), 64 * 1024 * 1024);

        // And the per-run knob, which is not a static but is just as unpinned.
        let config = VacuumConfig::default();
        assert_eq!(config.interval, std::time::Duration::from_secs(30));
        assert_eq!(config.min_dead_bytes, 8 * 1024 * 1024);
        assert_eq!(config.min_dead_ratio, 0.20);
        assert_eq!(config.drain_timeout, std::time::Duration::from_secs(30));
        assert_eq!(config.max_tracked_live_extents, 2 << 20);
    }

    #[test]
    fn without_versioning_the_deleted_flag_is_the_whole_truth() {
        let horizon = VersionHorizon::unversioned();
        assert!(!horizon.is_versioned());
        assert!(horizon.is_reclaimable(true, FROZEN_TX, NO_TX));
        assert!(!horizon.is_reclaimable(false, FROZEN_TX, NO_TX));
        // Even a stamped `xmax` means nothing without a manager to judge it.
        assert!(!horizon.is_reclaimable(false, FROZEN_TX, 7));
        assert!(horizon.is_reclaimable(true, FROZEN_TX, 7));
    }

    #[test]
    fn a_committed_delete_below_the_bound_is_reclaimable() {
        let manager = Arc::new(TransactionManager::in_memory());
        let deleter = manager.begin();
        manager.commit(deleter);

        // Nothing running, so the bound is past the deleter.
        let horizon = VersionHorizon::sample(&manager);
        assert!(horizon.oldest_active() > deleter);
        assert!(horizon.is_reclaimable(false, FROZEN_TX, deleter));
    }

    #[test]
    fn an_older_running_transaction_holds_the_version() {
        let manager = Arc::new(TransactionManager::in_memory());
        // Begins first, so its id is below the deleter's and it may still be
        // able to see the version.
        let reader = manager.begin();
        let deleter = manager.begin();
        manager.commit(deleter);

        let horizon = VersionHorizon::sample(&manager);
        assert_eq!(horizon.oldest_active(), reader);
        assert!(
            !horizon.is_reclaimable(false, FROZEN_TX, deleter),
            "freed a version an older transaction can still see"
        );

        // Once it finishes, the bound moves past the deleter.
        manager.commit(reader);
        let horizon = VersionHorizon::sample(&manager);
        assert!(horizon.is_reclaimable(false, FROZEN_TX, deleter));
    }

    #[test]
    fn an_uncommitted_or_aborted_delete_reclaims_nothing() {
        let manager = Arc::new(TransactionManager::in_memory());

        let in_progress = manager.begin();
        let horizon = VersionHorizon::sample(&manager);
        assert!(
            !horizon.is_reclaimable(false, FROZEN_TX, in_progress),
            "freed a version whose delete has not committed"
        );

        // An aborted delete did not happen: the row is still live, and treating
        // the abort as a delete would discard a row nobody removed.
        let aborted = manager.begin();
        manager.abort(aborted);
        manager.commit(in_progress);
        let horizon = VersionHorizon::sample(&manager);
        assert!(
            !horizon.is_reclaimable(false, FROZEN_TX, aborted),
            "an aborted DELETE reclaimed the row it failed to delete"
        );
    }

    #[test]
    fn a_horizon_taken_from_a_readers_visibility_can_still_find_rows_dead() {
        // The scan gate is only as good as this: if `for_visibility` handed back
        // an unversioned horizon for a versioned reader, `is_reclaimable` would
        // reduce to the `deleted` flag — which under MVCC is never set — and the
        // gate would be a silent no-op that looks completely correct at the call
        // site. That is the failure this pins.
        let manager = Arc::new(TransactionManager::in_memory());
        let writer = manager.begin();
        manager.commit(writer);
        let deleter = manager.begin();
        manager.commit(deleter);

        let reader = manager.begin();
        let visibility = Visibility::for_snapshot(manager.snapshot(reader), manager.clone());
        let horizon = VersionHorizon::for_visibility(&visibility);
        assert!(
            horizon.is_reclaimable(false, writer, deleter),
            "a reader's horizon could not tell a committed delete was dead, so the \
             scan gate would never fire"
        );

        // And `All` must stay inert: it is the rule vacuum's own scan uses and a
        // database with no manager at all, where nothing can be known to be dead.
        let horizon = VersionHorizon::for_visibility(&Visibility::All);
        assert!(
            !horizon.is_reclaimable(false, writer, deleter),
            "an unversioned horizon claimed to know a row was dead"
        );
        assert!(
            horizon.is_reclaimable(true, writer, NO_TX),
            "the tombstone flag stopped meaning what it means without a manager"
        );
    }

    #[test]
    fn a_row_whose_insert_aborted_is_reclaimed() {
        let manager = Arc::new(TransactionManager::in_memory());

        // A rolled-back `INSERT`: the bytes and the pointer are durably on
        // disk, stamped with a transaction that then aborted. Nothing has
        // deleted the row, so `deleted` is false and `xmax` is `NO_TX` — the
        // two fields reclamation used to consult both say "live".
        let inserter = manager.begin();
        manager.abort(inserter);
        let horizon = VersionHorizon::sample(&manager);
        assert!(
            horizon.is_reclaimable(false, inserter, NO_TX),
            "a rolled-back INSERT leaked its row forever"
        );

        // Still running is not aborted: the transaction may yet commit, and
        // freeing its row underneath it would lose a committed write.
        let running = manager.begin();
        let horizon = VersionHorizon::sample(&manager);
        assert!(
            !horizon.is_reclaimable(false, running, NO_TX),
            "freed a row whose INSERT had not finished"
        );

        // And an open reader does not protect it. Unlike a dead-but-once-live
        // version, a row from an aborted insert was never visible to *any*
        // snapshot, so there is no snapshot to wait out.
        let reader = manager.begin();
        let horizon = VersionHorizon::sample(&manager);
        assert!(
            horizon.is_reclaimable(false, inserter, NO_TX),
            "held an aborted INSERT for a reader that could never see it"
        );
        manager.commit(reader);
        manager.commit(running);
    }

    #[test]
    fn the_sentinel_transactions_are_never_read_as_aborted() {
        // `NO_TX` and `FROZEN_TX` are not transactions to ask about — they mean
        // "older than versioning". Asking anyway is actively dangerous: on a
        // status log with no bits yet, `outcome(NO_TX)` reads off the end of
        // the bitmap and reports `Aborted`, which would hand every live
        // pre-versioning row to reclamation at once.
        //
        // A manager that has never begun a transaction is exactly the state a
        // table is in the moment versioning is switched on, so this is the
        // first sweep after `enable_versioning`, not a corner.
        let manager = Arc::new(TransactionManager::in_memory());
        assert_eq!(
            manager.outcome(NO_TX),
            TxOutcome::Aborted,
            "the hazard this guards has changed shape; re-check the guard"
        );

        let horizon = VersionHorizon::sample(&manager);
        assert!(
            !horizon.is_reclaimable(false, NO_TX, NO_TX),
            "reclaimed a live row that predates versioning"
        );
        assert!(
            !horizon.is_reclaimable(false, FROZEN_TX, NO_TX),
            "reclaimed a live frozen row"
        );
    }

    #[test]
    fn a_row_that_predates_versioning_is_reclaimable_when_marked() {
        let manager = Arc::new(TransactionManager::in_memory());
        let _running = manager.begin();
        let horizon = VersionHorizon::sample(&manager);

        // No `xmax`: nothing running can hold a snapshot older than versioning
        // itself, so the flag still means what it always did.
        assert!(horizon.is_reclaimable(true, FROZEN_TX, NO_TX));
        assert!(!horizon.is_reclaimable(false, FROZEN_TX, NO_TX));
        // And a frozen `xmin` is not an `xmax` — it says nothing about death.
        assert!(!horizon.is_reclaimable(false, FROZEN_TX, NO_TX));
    }

    #[test]
    fn the_bound_is_taken_once_and_does_not_drift() {
        let manager = Arc::new(TransactionManager::in_memory());
        let reader = manager.begin();
        let deleter = manager.begin();
        manager.commit(deleter);

        // Sampled while the reader is still running.
        let horizon = VersionHorizon::sample(&manager);
        assert!(!horizon.is_reclaimable(false, FROZEN_TX, deleter));

        // The reader finishing mid-sweep must not change what this sweep
        // decided: a horizon is a snapshot of the bound, not a live view.
        manager.commit(reader);
        assert!(
            !horizon.is_reclaimable(false, FROZEN_TX, deleter),
            "the horizon tracked the bound instead of holding it"
        );
    }

    #[test]
    fn epochs_report_the_oldest_running_read() {
        let reg = Arc::new(ReaderRegistry::new());
        let barrier_before = reg.barrier();
        let first = reg.enter();
        let second = reg.enter();
        assert!(!reg.drained_past(reg.barrier()));
        assert_eq!(reg.oldest_active(), Some(barrier_before));
        drop(first);
        assert_eq!(reg.oldest_active(), Some(barrier_before + 1));
        drop(second);
        assert!(reg.drained_past(reg.barrier()));
    }

    #[test]
    fn a_read_started_after_the_barrier_does_not_block_publication() {
        let reg = Arc::new(ReaderRegistry::new());
        let barrier = reg.barrier();
        let _later = reg.enter();
        assert!(reg.drained_past(barrier));
    }

    #[test]
    fn an_inert_ticket_registers_nothing() {
        let reg = Arc::new(ReaderRegistry::new());
        let barrier = reg.barrier();
        let _t = ReadTicket::inert();
        assert!(reg.drained_past(barrier));
        assert_eq!(reg.oldest_active(), None);
    }
}
