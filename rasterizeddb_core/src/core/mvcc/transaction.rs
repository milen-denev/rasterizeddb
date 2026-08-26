//! Transaction identity, outcome, and the snapshot that decides visibility.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use cacheguard::CacheGuard;

use parking_lot::RwLock;

use super::status::StatusLog;

/// A transaction identifier.
///
/// Monotonic and never reused, so ordering two of them answers "which happened
/// first" without consulting anything else.
///
/// Zero is reserved and means "no transaction": as an `xmin` it marks a row that
/// predates versioning, and as an `xmax` it marks a row nothing has deleted.
pub type TxId = u64;

/// Reserved: not a transaction.
pub const NO_TX: TxId = 0;

/// The `xmin` given to rows written before this table had version metadata.
///
/// Every snapshot must see them — they are the table's existing contents, not
/// the work of some transaction that might yet abort — so they take an id that
/// is committed by definition and older than every real one.
pub const FROZEN_TX: TxId = 1;

/// The first id handed to a real transaction.
pub const FIRST_TX: TxId = 2;

/// How a transaction ended, or that it has not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxOutcome {
    /// Still running. Its writes are visible only to itself.
    InProgress,
    /// Ended successfully. Its writes are visible to any snapshot taken after.
    Committed,
    /// Ended without effect. Its writes are visible to nobody, ever.
    Aborted,
}

/// The set of transactions a statement considers to have already happened.
///
/// A snapshot is deliberately a value rather than a lock: taking one is the only
/// synchronisation a reader does, and from then on it decides visibility
/// entirely on its own. That is what lets readers run without blocking writers.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// One past the highest id assigned when this snapshot was taken. Anything
    /// at or above it began later and is invisible regardless of outcome.
    pub xmax: TxId,
    /// Transactions below `xmax` that were still running. Invisible even if they
    /// commit afterwards — otherwise a statement would see a transaction appear
    /// midway through its own execution.
    running: Arc<BTreeSet<TxId>>,
    /// The reading transaction, whose own writes it must see even though they
    /// are not committed. `NO_TX` for a snapshot taken outside any transaction.
    pub own: TxId,
    /// Every id belonging to the reading transaction, itself and each savepoint
    /// it has opened.
    ///
    /// A savepoint is a sub-transaction with an id of its own, so "is this write
    /// mine?" stops being an equality test the moment savepoints exist. A write
    /// made before a savepoint carries the outer id and must stay visible after
    /// it; a write made inside one that was rolled back must not.
    family: Arc<BTreeSet<TxId>>,
}

impl Snapshot {
    /// A snapshot that sees every committed transaction and nothing in flight.
    pub fn at(xmax: TxId, running: BTreeSet<TxId>, own: TxId) -> Self {
        let mut family = BTreeSet::new();
        if own != NO_TX {
            family.insert(own);
        }
        Snapshot { xmax, running: Arc::new(running), own, family: Arc::new(family) }
    }

    /// Same, with every id the reading transaction owns.
    pub fn at_with_family(
        xmax: TxId,
        running: BTreeSet<TxId>,
        own: TxId,
        family: BTreeSet<TxId>,
    ) -> Self {
        Snapshot { xmax, running: Arc::new(running), own, family: Arc::new(family) }
    }

    /// Whether `tx` is this transaction or one of its savepoints.
    #[inline]
    fn is_mine(&self, tx: TxId) -> bool {
        self.own != NO_TX && self.family.contains(&tx)
    }

    /// True when nothing was in flight, so every version below `xmax` is either
    /// committed or aborted and no version can change visibility later.
    ///
    /// This is the fast path, and it is the common case: with no concurrent
    /// writer, every live row is visible and every dead one is not — exactly
    /// what a non-versioned scan already computes. A caller seeing this may skip
    /// the version file entirely.
    pub fn sees_everything(&self) -> bool {
        self.running.is_empty()
    }

    /// Whether the transaction that wrote a version had already happened, as far
    /// as this snapshot is concerned.
    fn started_before(&self, tx: TxId) -> bool {
        tx < self.xmax && !self.running.contains(&tx)
    }

    /// Whether a row version with these stamps is visible.
    ///
    /// `xmin` is the transaction that created the version and `xmax` the one
    /// that removed it, or [`NO_TX`] if none has. `outcome` resolves an id to
    /// its fate.
    pub fn sees(&self, xmin: TxId, xmax: TxId, outcome: &impl Fn(TxId) -> TxOutcome) -> bool {
        // Did this version's creation happen, from here?
        let created = if xmin == FROZEN_TX {
            true
        } else if self.is_mine(xmin) {
            // A transaction always sees its own writes, committed or not —
            // otherwise an INSERT followed by a SELECT in the same transaction
            // would not find the row it just wrote.
            //
            // Except a savepoint that was rolled back: those writes are its own
            // and must be gone, which is the whole point of rolling back to a
            // savepoint. `outcome` reports such a sub-transaction as aborted
            // while the outer one is still running.
            outcome(xmin) != TxOutcome::Aborted
        } else if !self.started_before(xmin) {
            false
        } else {
            outcome(xmin) == TxOutcome::Committed
        };

        if !created {
            return false;
        }

        // Did its deletion happen? An aborted deleter leaves the row alive.
        if xmax == NO_TX {
            return true;
        }
        let deleted = if self.is_mine(xmax) {
            // Same asymmetry: a delete made inside a rolled-back savepoint did
            // not happen, so the row is alive again.
            outcome(xmax) != TxOutcome::Aborted
        } else if !self.started_before(xmax) {
            false
        } else {
            outcome(xmax) == TxOutcome::Committed
        };

        !deleted
    }
}

/// Assigns transaction ids, tracks which are running, and records how they
/// ended.
///
/// One per database. The id counter and the running set are separate locks'
/// worth of contention in principle, but a snapshot needs both to agree — an id
/// assigned after `xmax` was read but before the running set was, would be
/// missing from both and wrongly visible — so they are taken together.
#[derive(Debug)]
pub struct TransactionManager {
    inner: RwLock<Inner>,
    /// Read without the lock by callers that only need a bound, e.g. deciding
    /// whether a version predates every possible snapshot.
    ///
    /// Padded onto its own cache line, which is the whole point of the field:
    /// it exists to be readable *without* touching `inner`, and unpadded it
    /// shared a line with that `RwLock`'s own atomic word. Every snapshot
    /// acquired or released invalidated the bound for every other core, so the
    /// lock-free read paid a coherence miss caused by exactly the lock it was
    /// added to avoid.
    next_id: CacheGuard<AtomicU64>,
    status: StatusLog,
    /// In-flight in-place overwrites, and the parking spot [`begin`] waits on
    /// while any are outstanding. See [`TransactionManager::try_begin_in_place`].
    ///
    /// [`begin`]: TransactionManager::begin
    in_place: std::sync::Mutex<usize>,
    in_place_done: std::sync::Condvar,
    /// How many savepoint bookkeeping entries [`TransactionManager::outcome`]
    /// would have to walk: `parent.len() + rolled_back.len()`, republished
    /// under the write lock by everything that touches either.
    ///
    /// Zero — every database that has never opened a savepoint, and every one
    /// whose savepoints have retired — means `outcome` can go straight to the
    /// status log rather than take `inner` for a lookup that can only miss.
    /// It is called once or twice for **every row a scan looks at**, and a
    /// shared `RwLock` read is a write to the lock word, so eight workers were
    /// ping-ponging one line between them per row.
    savepoints: AtomicUsize,
    /// Last id passed to [`TransactionManager::mark_wrote`].
    ///
    /// Marking happens per row written, not per statement, so the hot path has
    /// to be cheap: a statement writing ten thousand rows takes the lock for the
    /// first and then only loads this. Correctness never rests on it — a stale
    /// read just takes the lock and re-inserts.
    ///
    /// Padded for the same reason as `next_id`, and it matters more here because
    /// the read is per *row*: unpadded it sat with `in_place` and its `Condvar`,
    /// so an unrelated in-place overwrite starting or finishing knocked the
    /// ten-thousand-row fast path back out to L3 mid-statement.
    last_writer: CacheGuard<AtomicU64>,
}

/// Held for the duration of an in-place overwrite. While one exists,
/// [`TransactionManager::begin`] waits.
///
/// See [`TransactionManager::try_begin_in_place`] for why.
pub struct InPlaceGuard {
    manager: std::sync::Arc<TransactionManager>,
}

impl Drop for InPlaceGuard {
    fn drop(&mut self) {
        let mut in_place = lock(&self.manager.in_place);
        *in_place -= 1;
        if *in_place == 0 {
            self.manager.in_place_done.notify_all();
        }
    }
}

impl std::fmt::Debug for InPlaceGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InPlaceGuard")
    }
}

/// A poisoned lock here means a writer panicked mid-update. The count it left
/// behind is still meaningful — it is a `usize`, not a half-built structure —
/// and refusing to start any transaction ever again is a worse answer than
/// carrying on.
#[inline]
fn lock(mutex: &std::sync::Mutex<usize>) -> std::sync::MutexGuard<'_, usize> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Debug, Default)]
struct Inner {
    next_id: TxId,
    running: BTreeSet<TxId>,
    /// Snapshots held for a whole transaction rather than a statement.
    ///
    /// `READ COMMITTED` takes a fresh snapshot per statement, so it appears
    /// here not at all. `REPEATABLE READ` and `SERIALIZABLE` take one at the
    /// start and reuse it, which is the entire difference between them and the
    /// default — a repeated read has to give the same answer.
    ///
    /// Kept here rather than on the session so that nothing between the session
    /// and the scan has to carry a snapshot: `snapshot(own)` finds the pinned
    /// one by id.
    pinned: std::collections::BTreeMap<TxId, Snapshot>,
    /// Transactions that end when their statement does.
    ///
    /// An autocommit statement's transaction commits the moment the statement
    /// returns, so its writes can never need undoing afterwards. A transaction
    /// opened by `BEGIN` can be rolled back at any later point, and that is the
    /// difference the in-place fast path turns on.
    statement_scoped: BTreeSet<TxId>,
    /// Sub-transaction → the transaction that opened it.
    ///
    /// A savepoint is a transaction of its own so its writes can be discarded
    /// separately, but it has no independent outcome: it commits when its parent
    /// does. Resolving through this chain is what makes that true.
    parent: std::collections::BTreeMap<TxId, TxId>,
    /// The reverse of `parent`, so collecting a family is proportional to the
    /// family rather than to every savepoint the process has ever opened.
    children: std::collections::BTreeMap<TxId, Vec<TxId>>,
    /// Sub-transactions rolled back while their parent runs.
    ///
    /// Recorded separately from the status log because the log is keyed by id
    /// and a rolled-back savepoint is aborted *now*, while its parent is still
    /// undecided. Writing "aborted" to the log would be correct but would also
    /// have to be undone if the id were ever reused — ids are not reused, so
    /// this is really about keeping the log's meaning single: it records how a
    /// *top-level* transaction ended.
    rolled_back: BTreeSet<TxId>,
    /// Transactions that have stamped a row version.
    ///
    /// A transaction that wrote nothing needs no durable outcome record:
    /// nothing on disk carries its id, so a restart that finds no record of it
    /// and reads it as aborted is reading it correctly. That is what lets a
    /// `SELECT` skip the `fdatasync` its outcome would otherwise cost.
    ///
    /// Ids are removed as their family retires, so this holds one entry per
    /// writing transaction currently in flight rather than a history.
    wrote: BTreeSet<TxId>,
}

impl TransactionManager {
    /// A manager whose outcomes are recorded in `status`.
    pub fn new(status: StatusLog) -> Self {
        // Ids resume above anything the previous run assigned, so an id is never
        // reused with a different meaning.
        let next_id = status.highest_recorded().max(FIRST_TX - 1) + 1;
        TransactionManager {
            inner: RwLock::new(Inner {
                next_id,
                running: BTreeSet::new(),
                pinned: std::collections::BTreeMap::new(),
                statement_scoped: BTreeSet::new(),
                parent: std::collections::BTreeMap::new(),
                children: std::collections::BTreeMap::new(),
                rolled_back: BTreeSet::new(),
                wrote: BTreeSet::new(),
            }),
            next_id: AtomicU64::new(next_id).into(),
            status,
            in_place: std::sync::Mutex::new(0),
            in_place_done: std::sync::Condvar::new(),
            savepoints: AtomicUsize::new(0),
            last_writer: AtomicU64::new(NO_TX).into(),
        }
    }

    /// A manager with no durable record, for tests and in-memory databases.
    pub fn in_memory() -> Self {
        TransactionManager::new(StatusLog::in_memory())
    }

    /// Claims the right to overwrite a version in place, if nothing can tell.
    ///
    /// An in-place overwrite rewrites bytes of a version that stays visible to
    /// exactly the same snapshots — it changes no `xmin` or `xmax`, so nothing
    /// distinguishes before from after. That is only safe when no snapshot
    /// exists at all, and checking that separately from doing it is not enough:
    ///
    /// ```text
    /// 1. T begins, snapshots, reads row R          -> old bytes
    /// 2. an autocommit UPDATE overwrites R in place
    /// 3. T reads R again                           -> new bytes
    /// ```
    ///
    /// T is entitled to see R unchanged for its whole life, so that is a
    /// repeatable-read violation, and no transaction has to be uncommitted for
    /// it to happen. The check and the write have to be one step.
    ///
    /// So this takes the same latch [`begin`] does, verifies nothing is
    /// running, and holds it raised until the returned guard drops. `begin`
    /// waits meanwhile. `None` means decline — the caller must write a new
    /// version instead.
    ///
    /// The guard is deliberately cheap to hold across an `await`: it owns an
    /// `Arc` and touches a `usize`, so it never holds a lock while suspended.
    ///
    /// [`begin`]: TransactionManager::begin
    pub fn try_begin_in_place(self: &std::sync::Arc<Self>) -> Option<InPlaceGuard> {
        self.try_begin_in_place_for(NO_TX)
    }

    /// As [`try_begin_in_place`], for a writer running under `own`.
    ///
    /// What the latch has to establish is that nobody *else* holds a snapshot —
    /// the writer's own transaction cannot be surprised by its own write. The
    /// distinction is the difference between the fast path being rare and being
    /// unreachable: every statement now runs under a transaction, including
    /// autocommit ones, so `running` is never empty and a test for emptiness
    /// declines always.
    ///
    /// Correct by construction rather than by argument, because `SELECT` goes
    /// through the same path: a concurrent reader is genuinely in `running`, so
    /// there is no case where one exists and this says yes.
    ///
    /// [`try_begin_in_place`]: TransactionManager::try_begin_in_place
    pub fn try_begin_in_place_for(self: &std::sync::Arc<Self>, own: TxId) -> Option<InPlaceGuard> {
        let mut in_place = lock(&self.in_place);
        // Read `running` while holding the latch, so no `begin` can land
        // between observing it empty and raising the count.
        let inner = self.inner.read();
        if inner.running.iter().any(|id| *id != own) {
            return None;
        }
        // Two separate conditions, and the second is easy to miss. "Nobody else
        // is running" makes the overwrite safe to *read* — no other snapshot can
        // tell before from after. It says nothing about whether the write might
        // have to be **undone**: an overwrite keeps no previous version, so a
        // transaction that is rolled back later cannot restore what it replaced.
        //
        // Only a statement-scoped transaction is safe on that count, because it
        // commits as its statement returns. A `BEGIN` block can roll back at any
        // point, and `a_rolled_back_update_to_a_fixed_width_column_is_undone` is
        // what happens when this check is missing: the rollback reports success
        // and the new value stays.
        // `NO_TX` and `FROZEN_TX` both mean "not inside a transaction", so
        // there is nothing that could later undo the write.
        let can_roll_back =
            own != NO_TX && own != FROZEN_TX && !inner.statement_scoped.contains(&own);
        if can_roll_back {
            return None;
        }
        drop(inner);
        *in_place += 1;
        Some(InPlaceGuard {
            manager: std::sync::Arc::clone(self),
        })
    }

    /// Starts a transaction that ends with the statement that opened it.
    ///
    /// This is autocommit. Marking it matters because such a transaction can
    /// never be rolled back after its statement returns, which is what makes the
    /// in-place fast path safe for it and unsafe for a `BEGIN` block.
    pub fn begin_autocommit(&self) -> TxId {
        let id = self.begin();
        self.inner.write().statement_scoped.insert(id);
        id
    }

    /// Republishes the count [`TransactionManager::outcome`]'s lock-free path
    /// tests. Call with the write lock held, after any change to `parent` or
    /// `rolled_back` — those two are the whole of what `outcome` consults.
    #[inline]
    fn publish_savepoints(&self, inner: &Inner) {
        self.savepoints
            .store(inner.parent.len() + inner.rolled_back.len(), Ordering::Release);
    }

    /// Takes `tx` and every savepoint under it out of the running set.
    ///
    /// A savepoint left running would hold `oldest_active` down for the life of
    /// the process, and reclamation would quietly stop — the failure looks like
    /// a disk-space leak rather than a transaction leak, so it is worth doing
    /// here rather than trusting every caller to unwind its own stack.
    ///
    /// Their *outcomes* need no record: a sub-transaction resolves through its
    /// parent, whose outcome was just written.
    fn retire_family(&self, tx: TxId) {
        let mut inner = self.inner.write();
        let family = Self::collect_family(&inner, tx);

        // Each sub-transaction's outcome is written down before its parent link
        // is dropped. Without this the link is the *only* record of how it ended,
        // and the link is in memory: after a restart `outcome(sub)` would find
        // nothing and report not-committed, so every row written inside a
        // savepoint of a committed transaction would silently disappear.
        // `a_savepoints_outcome_survives_a_restart` is that failure.
        let parent_outcome = self.status.outcome(tx);
        for id in &family {
            if *id == tx {
                continue;
            }
            let resolved = if inner.rolled_back.contains(id) {
                TxOutcome::Aborted
            } else {
                parent_outcome
            };
            self.status.record(*id, resolved);
        }

        // Now the bookkeeping can go. Keeping it would make `parent` and
        // `rolled_back` grow for the life of the process, one entry per
        // savepoint ever opened.
        for id in family {
            inner.running.remove(&id);
            inner.pinned.remove(&id);
            inner.statement_scoped.remove(&id);
            inner.rolled_back.remove(&id);
            inner.wrote.remove(&id);
            if let Some(parent) = inner.parent.remove(&id) {
                if let Some(kids) = inner.children.get_mut(&parent) {
                    kids.retain(|k| *k != id);
                }
            }
            inner.children.remove(&id);
        }
        self.publish_savepoints(&inner);
    }

    /// Opens a savepoint inside `parent` and returns the sub-transaction id that
    /// writes should now carry.
    ///
    /// The sub-transaction inherits `parent`'s snapshot: a savepoint changes what
    /// can be *discarded*, never what can be *seen*.
    pub fn begin_savepoint(&self, parent: TxId) -> TxId {
        let id = self.begin();
        let mut inner = self.inner.write();
        inner.parent.insert(id, parent);
        inner.children.entry(parent).or_default().push(id);
        // Share the parent's pinned snapshot, if it has one, so a repeatable-read
        // transaction does not silently start seeing new commits after a
        // savepoint.
        if let Some(snapshot) = inner.pinned.get(&parent).cloned() {
            inner.pinned.insert(id, snapshot);
        }
        // A savepoint is as rollback-able as its parent, so it never qualifies
        // for the in-place fast path even if the parent somehow did.
        inner.statement_scoped.remove(&id);
        self.publish_savepoints(&inner);
        id
    }

    /// Discards `tx` and everything it contains, leaving its parent running.
    ///
    /// The writes stay on disk; marking them rolled back is what makes them
    /// invisible, and reclamation removes them once no snapshot can reach them —
    /// the same mechanism as a whole-transaction abort.
    pub fn rollback_savepoint(&self, tx: TxId) {
        let mut inner = self.inner.write();
        // Everything opened after `tx` inside the same family goes with it: a
        // savepoint nested inside the one being discarded cannot outlive it.
        let doomed: Vec<TxId> = inner
            .parent
            .keys()
            .copied()
            .filter(|id| *id >= tx)
            .collect();
        for id in doomed {
            inner.rolled_back.insert(id);
            inner.running.remove(&id);
            inner.pinned.remove(&id);
        }
        inner.rolled_back.insert(tx);
        inner.running.remove(&tx);
        inner.pinned.remove(&tx);
        self.publish_savepoints(&inner);
    }

    /// Merges `tx` into its parent: its writes now stand or fall with it.
    ///
    /// Nothing has to move, because a sub-transaction already resolves through
    /// its parent. Releasing only means it can no longer be rolled back on its
    /// own, which the caller enforces by dropping it from the savepoint stack.
    pub fn release_savepoint(&self, tx: TxId) {
        self.inner.write().running.remove(&tx);
    }

    /// Every id belonging to `tx`: itself, and every savepoint opened under it.
    pub fn family_of(&self, tx: TxId) -> BTreeSet<TxId> {
        let inner = self.inner.read();
        Self::collect_family(&inner, tx)
    }

    /// `tx` and everything opened under it, transitively.
    ///
    /// Walks the child map, so the cost is the size of this family. The earlier
    /// version swept `parent` to a fixpoint, which made every commit — savepoints
    /// or not — proportional to every savepoint the process had ever opened.
    fn collect_family(inner: &Inner, tx: TxId) -> BTreeSet<TxId> {
        let mut family = BTreeSet::new();
        if tx == NO_TX {
            return family;
        }
        let mut stack = vec![tx];
        while let Some(id) = stack.pop() {
            if !family.insert(id) {
                continue;
            }
            if let Some(kids) = inner.children.get(&id) {
                stack.extend(kids.iter().copied());
            }
        }
        family
    }

    /// Starts a transaction and returns its id.
    ///
    /// Waits for any in-place overwrite to land first: a snapshot taken in the
    /// middle of one would read the new bytes for a row it is entitled to see
    /// unchanged. See [`TransactionManager::try_begin_in_place`].
    pub fn begin(&self) -> TxId {
        let mut in_place = lock(&self.in_place);
        while *in_place > 0 {
            in_place = self
                .in_place_done
                .wait(in_place)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        // Held past the `inner` lock below, so a `try_begin_in_place` cannot
        // observe an empty `running` set that this call is about to fill.
        let _latched = in_place;

        let mut inner = self.inner.write();
        let id = inner.next_id;
        inner.next_id += 1;
        inner.running.insert(id);
        self.next_id.store(inner.next_id, Ordering::Release);
        self.status.record(id, TxOutcome::InProgress);
        id
    }

    /// Records that `tx` has stamped a row version, so its outcome has to be
    /// made durable when it ends.
    ///
    /// Called from the write paths themselves rather than inferred from the
    /// shape of the statement: a `SELECT` carrying a data-modifying CTE writes
    /// rows, and classifying it by its purpose would mark it read-only and drop
    /// the outcome record for a transaction whose rows are on disk. Those rows
    /// would come back after a restart as debris.
    pub fn mark_wrote(&self, tx: TxId) {
        // Neither is a real transaction: `FROZEN_TX` stamps rows that predate
        // versioning and outlive every snapshot, and nothing ever retires
        // either, so an entry for them would never be removed.
        if tx == NO_TX || tx == FROZEN_TX {
            return;
        }
        if self.last_writer.load(Ordering::Acquire) == tx {
            return;
        }
        self.inner.write().wrote.insert(tx);
        // Stored after the insert and with release ordering, so a thread that
        // takes the fast path above also sees the entry the hint stands for.
        self.last_writer.store(tx, Ordering::Release);
    }

    /// True when `tx`, or any savepoint under it, stamped a row version.
    ///
    /// Ask *before* committing: `commit` retires the family and clears this.
    pub fn wrote_family(&self, tx: TxId) -> bool {
        let inner = self.inner.read();
        if inner.wrote.contains(&tx) {
            return true;
        }
        // A savepoint's writes are the transaction's writes. Checking only the
        // top-level id would call a block read-only because everything in it
        // was written inside a `SAVEPOINT`.
        Self::collect_family(&inner, tx)
            .iter()
            .any(|id| inner.wrote.contains(id))
    }

    /// Ends `tx` successfully.
    ///
    /// The durable record is written **before** the transaction leaves the
    /// running set. In the other order a reader could take a snapshot that
    /// excludes `tx` from `running` — and so consults the status log — in the
    /// window before the log says it committed, and would read its rows as
    /// aborted.
    pub fn commit(&self, tx: TxId) {
        self.status.record(tx, TxOutcome::Committed);
        self.retire_family(tx);
    }

    /// Ends `tx` without effect. Its versions stay on disk until vacuumed, but
    /// no snapshot will ever see them.
    pub fn abort(&self, tx: TxId) {
        self.status.record(tx, TxOutcome::Aborted);
        self.retire_family(tx);
    }

    /// How `tx` ended.
    pub fn outcome(&self, tx: TxId) -> TxOutcome {
        if tx == FROZEN_TX {
            return TxOutcome::Committed;
        }

        // Nothing to walk: `rolled_back` cannot contain `tx` and `parent`
        // cannot name it, so the answer is whatever the log says.
        //
        // Racing with a savepoint being opened is not a new hazard. The locked
        // version resolved the chain as it stood when it took the lock, and a
        // reader that got there first read the same absent parent this reads.
        // Nor can a reader reach this with a sub-transaction id it has not
        // already seen stamped on a row — and that row was written after the
        // savepoint was published, through storage that synchronises them.
        if self.savepoints.load(Ordering::Acquire) == 0 {
            return self.status.outcome(tx);
        }

        // A sub-transaction has no outcome of its own: rolled back explicitly,
        // or whatever its parent turns out to be. Resolving here rather than at
        // each call site means every reader — visibility, reclamation, conflict
        // detection — treats a savepoint the same way without knowing about them.
        let mut current = tx;
        loop {
            let inner = self.inner.read();
            if inner.rolled_back.contains(&current) {
                return TxOutcome::Aborted;
            }
            match inner.parent.get(&current).copied() {
                Some(parent) => {
                    drop(inner);
                    current = parent;
                }
                None => {
                    drop(inner);
                    return self.status.outcome(current);
                }
            }
        }
    }

    /// A snapshot for a statement running under `own`, or outside any
    /// transaction if `own` is [`NO_TX`].
    pub fn snapshot(&self, own: TxId) -> Snapshot {
        let inner = self.inner.read();
        // A transaction that pinned one reuses it, however many statements it
        // runs. This is what makes a repeated read repeatable.
        if let Some(pinned) = inner.pinned.get(&own) {
            return pinned.clone();
        }
        Self::snapshot_now_locked(&inner, own)
    }

    /// A snapshot taken now for `own`, ignoring any it has pinned.
    ///
    /// For work that is physical rather than logical: a table rewrite copies
    /// every row that exists, and must not be narrowed to what a `REPEATABLE
    /// READ` transaction happened to see at its first statement — rows others
    /// committed since are just as real, and a rewrite that read through the
    /// pin would drop them. `own`'s uncommitted writes are included as they
    /// are in [`Self::snapshot`].
    pub fn snapshot_now(&self, own: TxId) -> Snapshot {
        let inner = self.inner.read();
        Self::snapshot_now_locked(&inner, own)
    }

    fn snapshot_now_locked(inner: &Inner, own: TxId) -> Snapshot {
        // A transaction does not count itself as running: it sees its own
        // writes, which `Snapshot::sees` handles through `own`, and listing it
        // here would only make the fast path fire less often.
        let mut family = BTreeSet::new();
        if own != NO_TX {
            family.insert(own);
            loop {
                let before = family.len();
                for (child, parent) in inner.parent.iter() {
                    if family.contains(parent) {
                        family.insert(*child);
                    }
                }
                // A savepoint's *ancestors* are its own too: a write made before
                // the savepoint was opened carries the outer id and must stay
                // visible inside it.
                let mut ancestor = own;
                while let Some(parent) = inner.parent.get(&ancestor).copied() {
                    family.insert(parent);
                    ancestor = parent;
                }
                if family.len() == before {
                    break;
                }
            }
        }
        let running: BTreeSet<TxId> =
            inner.running.iter().copied().filter(|id| !family.contains(id)).collect();
        Snapshot::at_with_family(inner.next_id, running, own, family)
    }

    /// Fixes `tx`'s snapshot for its whole life, rather than one per statement.
    ///
    /// This is `REPEATABLE READ`. Taken at the point it is called rather than at
    /// `begin`, matching PostgreSQL: a transaction's snapshot is established by
    /// its first statement, so `BEGIN` followed by a long pause does not freeze
    /// the database's view at the pause.
    pub fn pin_snapshot(&self, tx: TxId) {
        let snapshot = self.snapshot(tx);
        self.inner.write().pinned.insert(tx, snapshot);
    }

    /// Whether `tx` holds a pinned snapshot.
    pub fn has_pinned_snapshot(&self, tx: TxId) -> bool {
        self.inner.read().pinned.contains_key(&tx)
    }

    /// The oldest transaction still running, or the next id to be assigned if
    /// none is.
    ///
    /// Nothing below this can still be in flight, so a version deleted by a
    /// transaction older than it is dead for every snapshot that will ever be
    /// taken — which is what makes it safe to reclaim.
    pub fn oldest_active(&self) -> TxId {
        let inner = self.inner.read();
        inner.running.iter().next().copied().unwrap_or(inner.next_id)
    }

    /// One past the highest id assigned so far.
    pub fn next_id(&self) -> TxId {
        self.next_id.load(Ordering::Acquire)
    }

    /// How many savepoint bookkeeping entries are outstanding.
    ///
    /// Exposed so a test can assert this returns to zero. These maps are
    /// consulted on the commit path, so an entry that is never removed costs
    /// more than memory — it shows up as a server that gets gradually slower and
    /// nothing pointing at why.
    pub fn bookkeeping_entries(&self) -> usize {
        let inner = self.inner.read();
        // The published count is what `outcome` trusts instead of this lock, so
        // every test that looks at the bookkeeping also checks it did not drift.
        debug_assert_eq!(
            self.savepoints.load(Ordering::Acquire),
            inner.parent.len() + inner.rolled_back.len(),
            "savepoint count drifted from the maps it mirrors"
        );
        inner.parent.len() + inner.children.len() + inner.rolled_back.len()
    }

    /// True when no transaction is running.
    pub fn quiescent(&self) -> bool {
        self.inner.read().running.is_empty()
    }

    /// Flushes the durable record of outcomes.
    pub fn flush(&self) -> std::io::Result<()> {
        self.status.flush()
    }

    /// Writes the outcome records without syncing them.
    ///
    /// The sync is owed to the log, and the caller must take it — normally via
    /// [`crate::core::database::Database::sync_durable`], which then covers the
    /// statement's rows and its outcome with one `fdatasync` instead of two.
    /// See [`StatusLog::flush_deferred`].
    pub fn flush_deferred(&self) -> std::io::Result<()> {
        self.status.flush_deferred()
    }

    /// Copies staged outcome bytes into the file, once the log holds them.
    ///
    /// Call after the sync that [`TransactionManager::flush_deferred`] left
    /// owed. See [`StatusLog::apply_staged`].
    pub fn apply_staged(&self) -> std::io::Result<()> {
        self.status.apply_staged()
    }

    /// Width of the outcome range waiting to be written.
    ///
    /// Read-only transactions do not flush, so this grows until a writing one
    /// does. See [`StatusLog::dirty_bytes`].
    pub fn dirty_outcome_bytes(&self) -> usize {
        self.status.dirty_bytes()
    }

    pub fn status(&self) -> &StatusLog {
        &self.status
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The committed/aborted lookup a snapshot needs, built from a manager.
    fn resolver(m: &TransactionManager) -> impl Fn(TxId) -> TxOutcome + '_ {
        move |id| m.outcome(id)
    }

    #[test]
    fn a_row_from_a_committed_transaction_is_visible() {
        let m = TransactionManager::in_memory();
        let writer = m.begin();
        m.commit(writer);

        let s = m.snapshot(NO_TX);
        assert!(s.sees(writer, NO_TX, &resolver(&m)));
    }

    /// A transaction that stamped nothing owes no durable outcome, which is
    /// what takes the `fdatasync` off a `SELECT`.
    #[test]
    fn a_transaction_that_wrote_nothing_is_not_a_writer() {
        let m = TransactionManager::in_memory();
        let reader = m.begin_autocommit();
        assert!(!m.wrote_family(reader), "a transaction that wrote nothing counted as a writer");
    }

    #[test]
    fn a_transaction_that_stamped_a_version_is_a_writer() {
        let m = TransactionManager::in_memory();
        let writer = m.begin_autocommit();
        m.mark_wrote(writer);
        assert!(m.wrote_family(writer));
    }

    /// Marking is per row written, so it has to survive being repeated and has
    /// to stay right when two transactions interleave — the fast path is a hint
    /// on the last id marked, and a stale read must only cost a lock, never an
    /// answer.
    #[test]
    fn interleaved_writers_are_each_recorded() {
        let m = TransactionManager::in_memory();
        let a = m.begin();
        let b = m.begin();
        let c = m.begin();

        m.mark_wrote(a);
        m.mark_wrote(a);
        m.mark_wrote(b);
        m.mark_wrote(a);

        assert!(m.wrote_family(a));
        assert!(m.wrote_family(b));
        assert!(!m.wrote_family(c), "a transaction that never wrote counted as a writer");
    }

    /// Writes made inside a `SAVEPOINT` carry the sub-transaction's id, so a
    /// block whose every write happened inside one would look read-only if only
    /// the top-level id were consulted — and its outcome record would be
    /// dropped while its rows sat on disk.
    #[test]
    fn a_write_inside_a_savepoint_makes_its_parent_a_writer() {
        let m = TransactionManager::in_memory();
        let parent = m.begin();
        let sub = m.begin_savepoint(parent);

        m.mark_wrote(sub);

        assert!(m.wrote_family(parent), "the parent of a writing savepoint read as read-only");
    }

    /// The neither-real ids are never retired, so an entry for one would never
    /// be removed.
    #[test]
    fn the_sentinel_ids_are_not_recorded_as_writers() {
        let m = TransactionManager::in_memory();
        m.mark_wrote(FROZEN_TX);
        m.mark_wrote(NO_TX);
        assert!(!m.wrote_family(FROZEN_TX));
        assert!(!m.wrote_family(NO_TX));
    }

    /// The record is per transaction in flight, not a history: an id that has
    /// ended must not keep an entry for the life of the process.
    #[test]
    fn retiring_a_transaction_forgets_that_it_wrote() {
        let m = TransactionManager::in_memory();
        let writer = m.begin();
        m.mark_wrote(writer);
        assert!(m.wrote_family(writer));

        m.commit(writer);
        assert!(!m.wrote_family(writer), "the entry outlived the transaction");
    }

    #[test]
    fn a_row_from_an_aborted_transaction_is_visible_to_nobody() {
        let m = TransactionManager::in_memory();
        let writer = m.begin();
        m.abort(writer);

        let s = m.snapshot(NO_TX);
        assert!(!s.sees(writer, NO_TX, &resolver(&m)));
    }

    /// The whole point: a writer in flight must not be half-visible.
    #[test]
    fn a_row_from_a_running_transaction_is_invisible_to_others() {
        let m = TransactionManager::in_memory();
        let writer = m.begin();
        let reader = m.snapshot(NO_TX);

        assert!(!reader.sees(writer, NO_TX, &resolver(&m)));
        // ...and stays invisible after it commits, or the statement would see
        // rows appear underneath it.
        m.commit(writer);
        assert!(!reader.sees(writer, NO_TX, &resolver(&m)));
    }

    #[test]
    fn a_transaction_sees_its_own_uncommitted_writes() {
        let m = TransactionManager::in_memory();
        let me = m.begin();
        let s = m.snapshot(me);

        assert!(s.sees(me, NO_TX, &resolver(&m)), "an INSERT must be visible to the SELECT after it");
        assert!(!s.sees(me, me, &resolver(&m)), "and a row it then deleted must not be");
    }

    #[test]
    fn a_snapshot_taken_after_a_commit_sees_it() {
        let m = TransactionManager::in_memory();
        let writer = m.begin();
        m.commit(writer);

        assert!(m.snapshot(NO_TX).sees(writer, NO_TX, &resolver(&m)));
    }

    /// A deletion by a transaction that then aborts must leave the row alive.
    #[test]
    fn an_aborted_delete_leaves_the_row_visible() {
        let m = TransactionManager::in_memory();
        let writer = m.begin();
        m.commit(writer);
        let deleter = m.begin();
        m.abort(deleter);

        let s = m.snapshot(NO_TX);
        assert!(s.sees(writer, deleter, &resolver(&m)));
    }

    #[test]
    fn a_committed_delete_hides_the_row() {
        let m = TransactionManager::in_memory();
        let writer = m.begin();
        m.commit(writer);
        let deleter = m.begin();
        m.commit(deleter);

        assert!(!m.snapshot(NO_TX).sees(writer, deleter, &resolver(&m)));
    }

    /// A delete that is still running has not happened yet for anyone else.
    #[test]
    fn a_running_delete_leaves_the_row_visible_to_others() {
        let m = TransactionManager::in_memory();
        let writer = m.begin();
        m.commit(writer);
        let reader = m.snapshot(NO_TX);
        let deleter = m.begin();

        assert!(reader.sees(writer, deleter, &resolver(&m)));
    }

    /// Rows that predate versioning belong to the table, not to a transaction.
    #[test]
    fn frozen_rows_are_visible_to_every_snapshot() {
        let m = TransactionManager::in_memory();
        let _running = m.begin();

        assert!(m.snapshot(NO_TX).sees(FROZEN_TX, NO_TX, &resolver(&m)));
    }

    /// The fast path has to be exactly "nothing is concurrent", or it would skip
    /// the version file in a case where the file changes the answer.
    #[test]
    fn the_fast_path_fires_only_when_nothing_is_in_flight() {
        let m = TransactionManager::in_memory();
        assert!(m.snapshot(NO_TX).sees_everything());

        let running = m.begin();
        assert!(!m.snapshot(NO_TX).sees_everything(), "someone else is mid-write");
        // A transaction does not block its own fast path.
        assert!(m.snapshot(running).sees_everything());

        m.commit(running);
        assert!(m.snapshot(NO_TX).sees_everything());
    }

    #[test]
    fn ids_are_monotonic_and_never_reused() {
        let m = TransactionManager::in_memory();
        let a = m.begin();
        let b = m.begin();
        m.abort(b);
        let c = m.begin();

        assert!(a < b && b < c, "{} {} {}", a, b, c);
        assert!(a >= FIRST_TX);
    }

    /// Nothing below `oldest_active` can still be running, which is what lets
    /// vacuum decide a version is dead for good.
    #[test]
    fn oldest_active_bounds_what_can_still_be_running() {
        let m = TransactionManager::in_memory();
        assert_eq!(m.oldest_active(), m.next_id(), "nothing running");

        let first = m.begin();
        let second = m.begin();
        assert_eq!(m.oldest_active(), first);

        m.commit(first);
        assert_eq!(m.oldest_active(), second, "the bound advances as the oldest ends");

        m.commit(second);
        assert_eq!(m.oldest_active(), m.next_id());
    }
}

#[cfg(test)]
mod savepoint_tests {
    use super::*;

    fn resolver(m: &TransactionManager) -> impl Fn(TxId) -> TxOutcome + '_ {
        move |id| m.outcome(id)
    }

    /// `outcome` skips the lock when no savepoint bookkeeping is outstanding,
    /// so the count it tests has to follow the maps through every step — and
    /// come back to zero, or the fast path is lost for the life of the process.
    ///
    /// `bookkeeping_entries` asserts the two agree, so calling it is the check.
    #[test]
    fn the_published_savepoint_count_follows_the_maps() {
        let m = TransactionManager::in_memory();
        assert_eq!(m.savepoints.load(Ordering::Acquire), 0);

        let outer = m.begin();
        let sp = m.begin_savepoint(outer);
        m.bookkeeping_entries();
        assert_eq!(m.savepoints.load(Ordering::Acquire), 1, "one parent link");

        let inner_sp = m.begin_savepoint(sp);
        m.bookkeeping_entries();
        m.rollback_savepoint(inner_sp);
        m.bookkeeping_entries();
        assert_eq!(
            m.savepoints.load(Ordering::Acquire),
            3,
            "two parent links and one rollback"
        );

        m.commit(outer);
        m.bookkeeping_entries();
        assert_eq!(
            m.savepoints.load(Ordering::Acquire),
            0,
            "retiring the family restores the fast path"
        );
    }

    /// A savepoint's writes are visible to its own transaction, like any other
    /// write it made.
    #[test]
    fn a_savepoints_writes_are_visible_inside_the_transaction() {
        let m = TransactionManager::in_memory();
        let outer = m.begin();
        let sp = m.begin_savepoint(outer);

        let s = m.snapshot(sp);
        assert!(s.sees(sp, NO_TX, &resolver(&m)), "its own write");
        assert!(s.sees(outer, NO_TX, &resolver(&m)), "and one made before the savepoint");
    }

    /// The point of the whole thing: rolling back to a savepoint discards what
    /// happened after it and keeps what happened before.
    #[test]
    fn rolling_back_a_savepoint_keeps_the_work_before_it() {
        let m = TransactionManager::in_memory();
        let outer = m.begin();
        let sp = m.begin_savepoint(outer);
        m.rollback_savepoint(sp);

        let s = m.snapshot(outer);
        assert!(!s.sees(sp, NO_TX, &resolver(&m)), "the savepoint's write is gone");
        assert!(s.sees(outer, NO_TX, &resolver(&m)), "the earlier write is not");
    }

    /// A delete made inside a rolled-back savepoint did not happen, so the row
    /// it removed is alive again.
    #[test]
    fn rolling_back_undoes_a_delete_made_inside_it() {
        let m = TransactionManager::in_memory();
        let writer = m.begin();
        m.commit(writer);

        let outer = m.begin();
        let sp = m.begin_savepoint(outer);
        m.rollback_savepoint(sp);

        assert!(m.snapshot(outer).sees(writer, sp, &resolver(&m)));
    }

    /// A savepoint nested inside one that is discarded cannot outlive it.
    #[test]
    fn rolling_back_discards_savepoints_nested_inside_it() {
        let m = TransactionManager::in_memory();
        let outer = m.begin();
        let first = m.begin_savepoint(outer);
        let second = m.begin_savepoint(first);
        m.rollback_savepoint(first);

        let s = m.snapshot(outer);
        assert!(!s.sees(first, NO_TX, &resolver(&m)));
        assert!(!s.sees(second, NO_TX, &resolver(&m)), "the nested one went with it");
        assert!(s.sees(outer, NO_TX, &resolver(&m)));
    }

    /// A savepoint has no outcome of its own — it commits when its parent does,
    /// and not before.
    #[test]
    fn a_savepoint_commits_with_its_parent() {
        let m = TransactionManager::in_memory();
        let outer = m.begin();
        let sp = m.begin_savepoint(outer);
        m.release_savepoint(sp);

        // Still nothing for an outside reader: the parent has not committed.
        assert!(!m.snapshot(NO_TX).sees(sp, NO_TX, &resolver(&m)));

        m.commit(outer);
        assert!(m.snapshot(NO_TX).sees(sp, NO_TX, &resolver(&m)), "released work commits with it");
    }

    /// And it aborts with its parent, released or not.
    #[test]
    fn a_savepoint_aborts_with_its_parent() {
        let m = TransactionManager::in_memory();
        let outer = m.begin();
        let sp = m.begin_savepoint(outer);
        m.release_savepoint(sp);
        m.abort(outer);

        assert_eq!(m.outcome(sp), TxOutcome::Aborted);
        assert!(!m.snapshot(NO_TX).sees(sp, NO_TX, &resolver(&m)));
    }

    /// An outside reader must not see a savepoint's writes while its parent
    /// runs, exactly as for the parent's own.
    #[test]
    fn a_savepoint_is_invisible_outside_its_transaction() {
        let m = TransactionManager::in_memory();
        let outer = m.begin();
        let sp = m.begin_savepoint(outer);

        let s = m.snapshot(NO_TX);
        assert!(!s.sees(sp, NO_TX, &resolver(&m)));
        assert!(!s.sees(outer, NO_TX, &resolver(&m)));
    }

    /// A savepoint inherits the pinned snapshot, so opening one inside a
    /// repeatable-read transaction does not quietly widen what it can see.
    #[test]
    fn a_savepoint_inherits_a_pinned_snapshot() {
        let m = TransactionManager::in_memory();
        let outer = m.begin();
        m.pin_snapshot(outer);
        let before = m.snapshot(outer).xmax;

        let other = m.begin();
        m.commit(other);

        let sp = m.begin_savepoint(outer);
        assert_eq!(m.snapshot(sp).xmax, before, "the savepoint widened the view");
        assert!(!m.snapshot(sp).sees(other, NO_TX, &resolver(&m)));
    }

    /// The family is what makes "is this write mine?" work once ids multiply.
    #[test]
    fn the_family_covers_ancestors_and_descendants() {
        let m = TransactionManager::in_memory();
        let outer = m.begin();
        let first = m.begin_savepoint(outer);
        let second = m.begin_savepoint(first);

        let family = m.family_of(outer);
        assert!(family.contains(&outer) && family.contains(&first) && family.contains(&second));

        // From inside the innermost savepoint, the outer ids are still mine.
        let s = m.snapshot(second);
        assert!(s.sees(outer, NO_TX, &resolver(&m)));
        assert!(s.sees(first, NO_TX, &resolver(&m)));
    }
}

#[cfg(test)]
mod savepoint_leaks {
    use super::*;

    /// A block that ends must take its savepoints with it. One left running
    /// holds `oldest_active` down for the life of the process, and reclamation
    /// stops — which shows up as a disk-space leak, not a transaction leak.
    #[test]
    fn ending_a_transaction_retires_its_savepoints() {
        for end_with_commit in [true, false] {
            let m = TransactionManager::in_memory();
            let outer = m.begin();
            let first = m.begin_savepoint(outer);
            let _second = m.begin_savepoint(first);
            assert_eq!(m.oldest_active(), outer);

            if end_with_commit {
                m.commit(outer);
            } else {
                m.abort(outer);
            }

            assert!(m.quiescent(), "savepoints outlived their transaction");
            assert_eq!(m.oldest_active(), m.next_id(), "the reclamation bound is stuck");
        }
    }

    /// And a savepoint rolled back mid-block does not hold the bound either.
    #[test]
    fn a_rolled_back_savepoint_does_not_hold_the_bound() {
        let m = TransactionManager::in_memory();
        let outer = m.begin();
        let sp = m.begin_savepoint(outer);
        m.rollback_savepoint(sp);
        m.commit(outer);

        assert!(m.quiescent());
        assert_eq!(m.oldest_active(), m.next_id());
    }
}

#[cfg(test)]
mod savepoint_durability {
    use super::*;

    /// A savepoint's writes must still be visible after a restart.
    ///
    /// A sub-transaction resolves its outcome through its parent, and the parent
    /// map is in memory. If nothing durable records how the sub-transaction
    /// ended, then after a restart `outcome(sub)` finds no record and reports
    /// not-committed — so every row written inside a savepoint of a committed
    /// transaction silently disappears.
    #[test]
    fn a_savepoints_outcome_survives_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("status");

        let sub = {
            let m = TransactionManager::new(StatusLog::open(&path).expect("open"));
            let outer = m.begin();
            let sub = m.begin_savepoint(outer);
            m.commit(outer);
            m.flush().expect("flush");
            assert_eq!(m.outcome(sub), TxOutcome::Committed, "before the restart");
            sub
        };

        let m = TransactionManager::new(StatusLog::open(&path).expect("reopen"));
        assert_eq!(
            m.outcome(sub),
            TxOutcome::Committed,
            "the savepoint's rows became invisible across a restart"
        );
    }

    /// And one rolled back stays rolled back, rather than being resurrected by
    /// its parent's commit.
    #[test]
    fn a_rolled_back_savepoint_stays_rolled_back_across_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("status");

        let sub = {
            let m = TransactionManager::new(StatusLog::open(&path).expect("open"));
            let outer = m.begin();
            let sub = m.begin_savepoint(outer);
            m.rollback_savepoint(sub);
            m.commit(outer);
            m.flush().expect("flush");
            assert_eq!(m.outcome(sub), TxOutcome::Aborted, "before the restart");
            sub
        };

        let m = TransactionManager::new(StatusLog::open(&path).expect("reopen"));
        assert_eq!(
            m.outcome(sub),
            TxOutcome::Aborted,
            "a rolled-back savepoint came back after a restart"
        );
    }
}

#[cfg(test)]
mod savepoint_bookkeeping {
    use super::*;

    /// The savepoint maps must return to empty. They are read on the commit
    /// path, so an entry that is never removed is not just memory — it makes
    /// every later commit do more work, and presents as a server that gets
    /// slowly worse with no obvious cause.
    #[test]
    fn the_bookkeeping_returns_to_empty() {
        let m = TransactionManager::in_memory();
        assert_eq!(m.bookkeeping_entries(), 0);

        for round in 0..50 {
            let outer = m.begin();
            let first = m.begin_savepoint(outer);
            let second = m.begin_savepoint(first);
            if round % 3 == 0 {
                m.rollback_savepoint(second);
            }
            if round % 2 == 0 {
                m.commit(outer);
            } else {
                m.abort(outer);
            }
        }

        assert_eq!(
            m.bookkeeping_entries(),
            0,
            "savepoint bookkeeping accumulated across transactions"
        );
        assert!(m.quiescent());
    }

    /// And it is still empty when savepoints are released rather than rolled
    /// back or left standing.
    #[test]
    fn releasing_also_clears_the_bookkeeping() {
        let m = TransactionManager::in_memory();
        for _ in 0..20 {
            let outer = m.begin();
            let sp = m.begin_savepoint(outer);
            m.release_savepoint(sp);
            m.commit(outer);
        }
        assert_eq!(m.bookkeeping_entries(), 0);
    }
}
