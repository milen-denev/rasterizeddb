//! Row locks taken by `SELECT … FOR UPDATE`.
//!
//! A lock is held by a transaction and released when that transaction ends, so
//! the lock table is keyed by `(table, row key)` and holds the owning [`TxId`].
//!
//! # Why a key rather than a position
//!
//! A row's position in the file moves when it is updated, so a lock keyed by
//! position would be released against a row that no longer lives there. The key
//! is the value of the table's unique column, which is what the statement
//! taking the lock is addressing the row by in the first place.
//!
//! # What a lock is worth
//!
//! Only if writers consult it. `UPDATE` and `DELETE` take the same lock before
//! rewriting a row, so a row another transaction holds `FOR UPDATE` cannot be
//! written underneath it — which is the whole point of taking one.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use smallvec::SmallVec;

use crate::core::mvcc::TxId;
use crate::core::mvcc::transaction::TransactionManager;
use crate::core::rql::row_lock::LockStrength;

/// Who holds one row's lock.
#[derive(Debug, Clone)]
enum Holder {
    /// One transaction, exclusively.
    Exclusive(TxId),
    /// Any number of transactions, none of which may write.
    Shared(SmallVec<[TxId; 2]>),
}

/// The outcome of asking for a lock.
#[derive(Debug, PartialEq, Eq)]
pub enum Acquired {
    /// The caller now holds it.
    Yes,
    /// Someone else does. The transaction named is the one to wait for.
    Held(TxId),
}

/// A table-level lock taken by `LOCK TABLE`.
///
/// PostgreSQL distinguishes eight modes whose conflicts form a matrix. They are
/// mapped onto two here — shared and exclusive — chosen so the engine never
/// permits what PostgreSQL would refuse. A mode that only conflicts with itself
/// in PostgreSQL therefore conflicts a little more widely here, which costs
/// concurrency and never costs correctness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableLockMode {
    /// `ACCESS SHARE`, `ROW SHARE`, `SHARE` — readers coexist, writers wait.
    Shared,
    /// `ROW EXCLUSIVE` through `ACCESS EXCLUSIVE` — one holder, nobody else.
    Exclusive,
}

/// A row key, inline up to 16 bytes: an integer or UUID key never allocates.
type Key = SmallVec<[u8; 16]>;
/// A table name (or `table\0column` for a referenced-key lock), interned.
type NameId = u32;

/// Which of the three lock spaces an owned entry lives in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Space {
    /// `SELECT … FOR UPDATE / FOR SHARE` and the writers that honour it,
    /// keyed by the table's identifying column.
    Row,
    /// Referenced keys, taken by foreign-key checks; see
    /// [`RowLocks::acquire_referenced`].
    Referenced,
    /// `LOCK TABLE`.
    Table,
}

#[derive(Default)]
struct Inner {
    /// Every name a lock has been taken under, so the maps key on a `u32`
    /// rather than an owned `String` per lock.
    ///
    /// Cleared whenever every map is empty, which bounds it by the names in use
    /// at once rather than by every name ever locked — CTE materialisation
    /// creates a uniquely named relation per statement.
    names: HashMap<Box<str>, NameId>,
    rows: HashMap<(NameId, Key), Holder>,
    referenced: HashMap<(NameId, Key), Holder>,
    tables: HashMap<NameId, Holder>,
    /// What each transaction holds, so ending one costs its own locks rather
    /// than a walk of every lock in the database under the global mutex.
    ///
    /// May name an entry the transaction no longer holds — a dead holder's
    /// lock is taken over lazily — so release checks before removing.
    owned: HashMap<TxId, Vec<(Space, NameId, Key)>>,
}

impl Inner {
    fn intern(&mut self, name: &str) -> NameId {
        if let Some(id) = self.names.get(name) {
            return *id;
        }
        let id = self.names.len() as NameId;
        self.names.insert(name.into(), id);
        id
    }

    fn map(&mut self, space: Space) -> &mut HashMap<(NameId, Key), Holder> {
        match space {
            Space::Row => &mut self.rows,
            Space::Referenced => &mut self.referenced,
            Space::Table => unreachable!("table locks are keyed by name alone"),
        }
    }

    /// Drops `tx` from one entry. True if it was there.
    fn drop_holder(holder: &mut Holder, tx: TxId) -> (bool, bool) {
        // (was a holder, entry now empty)
        match holder {
            Holder::Exclusive(owner) => {
                if *owner == tx {
                    (true, true)
                } else {
                    (false, false)
                }
            }
            Holder::Shared(owners) => {
                let before = owners.len();
                owners.retain(|o| *o != tx);
                (owners.len() != before, owners.is_empty())
            }
        }
    }

    /// Everything `tx` holds, gone. Returns how many row and table entries
    /// were removed, for the lock-free emptiness counters.
    fn release(&mut self, tx: TxId) -> (usize, usize) {
        let Some(list) = self.owned.remove(&tx) else {
            return (0, 0);
        };
        let (mut rows_removed, mut tables_removed) = (0, 0);
        for (space, name, key) in list {
            match space {
                Space::Table => {
                    if let Some(holder) = self.tables.get_mut(&name) {
                        if let (_, true) = Self::drop_holder(holder, tx) {
                            self.tables.remove(&name);
                            tables_removed += 1;
                        }
                    }
                }
                Space::Row | Space::Referenced => {
                    let map = self.map(space);
                    let entry = (name, key);
                    if let Some(holder) = map.get_mut(&entry) {
                        if let (_, true) = Self::drop_holder(holder, tx) {
                            map.remove(&entry);
                            if space == Space::Row {
                                rows_removed += 1;
                            }
                        }
                    }
                }
            }
        }
        (rows_removed, tables_removed)
    }

    /// Gives back memory a burst left behind. A single large `FOR UPDATE`
    /// would otherwise pin its high-water capacity for the life of the process.
    fn shrink(&mut self) {
        fn shrink_map<K: std::hash::Hash + Eq, V>(map: &mut HashMap<K, V>) {
            if map.capacity() > 64 && map.len() < map.capacity() / 4 {
                map.shrink_to(map.len() * 2);
            }
        }
        shrink_map(&mut self.rows);
        shrink_map(&mut self.referenced);
        shrink_map(&mut self.tables);
        shrink_map(&mut self.owned);
        if self.rows.is_empty() && self.referenced.is_empty() && self.tables.is_empty() {
            self.names.clear();
            shrink_map(&mut self.names);
        }
    }
}

#[derive(Default)]
pub struct RowLocks {
    /// One mutex for every space: a lock is taken once per claimed row or
    /// referenced key and held for a transaction, so this is nowhere near the
    /// per-row hot path, and one lock keeps "is it still held" answerable
    /// without a second acquisition.
    inner: Mutex<Inner>,
    /// `inner.rows.len()` and `inner.tables.len()`, readable without the
    /// mutex. Every UPDATE and DELETE asks both questions, and nearly always
    /// the answer is "none", so it should not cost a lock round trip.
    row_count: AtomicUsize,
    table_count: AtomicUsize,
    /// Resolves a savepoint's id to the transaction that owns it. A lock is
    /// held by a transaction, not by one of its savepoints: keyed by the
    /// savepoint id it was never released by the transaction's end, and the
    /// transaction then conflicted with its own savepoint's lock.
    transactions: Option<std::sync::Arc<TransactionManager>>,
    /// Who each long-waiting transaction is waiting for: the wait-for graph a
    /// deadlock is a cycle in. Only waits that have already lasted
    /// [`DEADLOCK_TIMEOUT`] are entered, as PostgreSQL's `deadlock_timeout`
    /// does, so the graph stays empty unless something is actually stuck. A
    /// mutex of its own: lock operations never take it.
    waits: Mutex<HashMap<TxId, SmallVec<[TxId; 4]>>>,
}

/// See [`RowLocks::waiting_guard`].
pub struct WaitingGuard<'a> {
    locks: &'a RowLocks,
    waiter: TxId,
}

impl Drop for WaitingGuard<'_> {
    fn drop(&mut self) {
        self.locks.clear_waiting(self.waiter);
    }
}

/// How long a lock wait lasts before it is checked for a deadlock.
pub const DEADLOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

impl RowLocks {
    /// Locks whose holders are resolved through `transactions`, so a
    /// savepoint's locks belong to its transaction.
    pub fn for_transactions(transactions: std::sync::Arc<TransactionManager>) -> Self {
        RowLocks {
            transactions: Some(transactions),
            ..Default::default()
        }
    }

    #[inline]
    fn owner_of(&self, tx: TxId) -> TxId {
        match &self.transactions {
            Some(manager) => manager.top_level(tx),
            None => tx,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("row lock map poisoned")
    }

    fn publish_counts(&self, inner: &Inner) {
        self.row_count.store(inner.rows.len(), Ordering::Release);
        self.table_count.store(inner.tables.len(), Ordering::Release);
    }

    /// The shared acquire, for the row and referenced-key spaces.
    fn acquire_in(
        &self,
        space: Space,
        name: &str,
        key: &[u8],
        tx: TxId,
        strength: LockStrength,
        is_live: &dyn Fn(TxId) -> bool,
    ) -> Acquired {
        let tx = self.owner_of(tx);
        // `FROZEN_TX` is not a transaction: nothing ever commits or aborts it,
        // so a lock recorded under it is never released and wedges the row for
        // the life of the process. A statement running outside a transaction
        // cannot hold a lock past its own execution anyway, so it only ever
        // *checks*.
        if tx == crate::core::mvcc::transaction::FROZEN_TX {
            return match self.conflict_in(space, name, key, tx, strength, is_live) {
                Some(owner) => Acquired::Held(owner),
                None => Acquired::Yes,
            };
        }

        let mut inner = self.lock();
        let name_id = inner.intern(name);
        let entry_key = (name_id, Key::from_slice(key));
        let mut dead: SmallVec<[TxId; 2]> = SmallVec::new();
        let exclusive = matches!(strength, LockStrength::Update);
        let decision = decide(
            inner.map(space).get_mut(&entry_key),
            tx,
            exclusive,
            is_live,
            &mut dead,
        );
        let outcome = match decision {
            Decision::Return(outcome) => outcome,
            Decision::Joined { newly } => {
                if newly {
                    inner
                        .owned
                        .entry(tx)
                        .or_default()
                        .push((space, entry_key.0, entry_key.1));
                }
                Acquired::Yes
            }
            Decision::Insert { already_mine } => {
                let holder = if exclusive {
                    Holder::Exclusive(tx)
                } else {
                    Holder::Shared(smallvec::smallvec![tx])
                };
                inner.map(space).insert(entry_key.clone(), holder);
                if !already_mine {
                    inner
                        .owned
                        .entry(tx)
                        .or_default()
                        .push((space, entry_key.0, entry_key.1));
                }
                Acquired::Yes
            }
        };
        // Holders found dead lose everything they hold, so an owner whose
        // transaction ended without running cleanup does not leave its index
        // entry behind. Done after the entry above was rewritten: `release`
        // only removes what the dead owner still holds, and this is now `tx`'s.
        for owner in dead {
            inner.release(owner);
        }
        self.publish_counts(&inner);
        outcome
    }

    fn conflict_in(
        &self,
        space: Space,
        name: &str,
        key: &[u8],
        tx: TxId,
        strength: LockStrength,
        is_live: &dyn Fn(TxId) -> bool,
    ) -> Option<TxId> {
        let tx = self.owner_of(tx);
        let mut inner = self.lock();
        let name_id = *inner.names.get(name)?;
        let map = inner.map(space);
        match map.get(&(name_id, Key::from_slice(key)))? {
            Holder::Exclusive(owner) if *owner != tx && is_live(*owner) => Some(*owner),
            Holder::Shared(owners) if matches!(strength, LockStrength::Update) => {
                owners.iter().find(|o| **o != tx && is_live(**o)).copied()
            }
            _ => None,
        }
    }

    /// Takes `strength` on one row for `tx`, or reports who has it.
    ///
    /// Re-taking a lock the caller already holds succeeds: the claim idiom
    /// selects rows `FOR UPDATE` and then updates them in the same
    /// transaction, which asks for the same lock twice.
    pub fn acquire(
        &self,
        table: &str,
        key: &[u8],
        tx: TxId,
        strength: LockStrength,
        is_live: &dyn Fn(TxId) -> bool,
    ) -> Acquired {
        self.acquire_in(Space::Row, table, key, tx, strength, is_live)
    }

    /// Who holds a conflicting lock on this row, without taking one.
    ///
    /// The read-only half of [`RowLocks::acquire`], for callers that must not
    /// record ownership.
    pub fn conflict(
        &self,
        table: &str,
        key: &[u8],
        tx: TxId,
        strength: LockStrength,
        is_live: &dyn Fn(TxId) -> bool,
    ) -> Option<TxId> {
        if self.row_count.load(Ordering::Acquire) == 0 {
            return None;
        }
        self.conflict_in(Space::Row, table, key, tx, strength, is_live)
    }

    /// Locks a referenced key — the value `column` of `table` holds — for a
    /// foreign-key check or for the write that would remove it.
    ///
    /// PostgreSQL's `FOR KEY SHARE` and its conflict with a `DELETE`: a child
    /// insert takes [`LockStrength::Share`] on the parent key it references and
    /// holds it to commit, and a `DELETE` of the parent takes
    /// [`LockStrength::Update`] on the same key before deciding what its
    /// referencing rows mean. Whichever comes second waits for the first, so
    /// the two can never both commit and leave an orphan.
    ///
    /// A space of its own rather than the row space: `FOR UPDATE` locks are
    /// keyed by the table's identifying column, which need not be the
    /// referenced one, and an `UPDATE` of a non-key column must not wait for a
    /// child insert — `FOR NO KEY UPDATE` does not conflict with `FOR KEY
    /// SHARE` either. `key` is the value's *stored* bytes, so both sides name
    /// one key however the literal that found it was spelled.
    pub fn acquire_referenced(
        &self,
        table: &str,
        column: &str,
        key: &[u8],
        tx: TxId,
        strength: LockStrength,
        is_live: &dyn Fn(TxId) -> bool,
    ) -> Acquired {
        let name = referenced_name(table, column);
        self.acquire_in(Space::Referenced, &name, key, tx, strength, is_live)
    }

    /// Takes a whole-table lock for `tx`, or reports who holds it.
    pub fn acquire_table(
        &self,
        table: &str,
        tx: TxId,
        mode: TableLockMode,
        is_live: &dyn Fn(TxId) -> bool,
    ) -> Acquired {
        let tx = self.owner_of(tx);
        // As in `acquire`: a lock under `FROZEN_TX` would never be released.
        if tx == crate::core::mvcc::transaction::FROZEN_TX {
            return match self.table_conflict(table, tx, true, is_live) {
                Some(owner) => Acquired::Held(owner),
                None => Acquired::Yes,
            };
        }

        let mut inner = self.lock();
        let name_id = inner.intern(table);
        let mut dead: SmallVec<[TxId; 2]> = SmallVec::new();
        let exclusive = matches!(mode, TableLockMode::Exclusive);
        let decision = decide(inner.tables.get_mut(&name_id), tx, exclusive, is_live, &mut dead);
        let outcome = match decision {
            Decision::Return(outcome) => outcome,
            Decision::Joined { newly } => {
                if newly {
                    inner
                        .owned
                        .entry(tx)
                        .or_default()
                        .push((Space::Table, name_id, Key::new()));
                }
                Acquired::Yes
            }
            Decision::Insert { already_mine } => {
                let holder = if exclusive {
                    Holder::Exclusive(tx)
                } else {
                    Holder::Shared(smallvec::smallvec![tx])
                };
                inner.tables.insert(name_id, holder);
                if !already_mine {
                    inner
                        .owned
                        .entry(tx)
                        .or_default()
                        .push((Space::Table, name_id, Key::new()));
                }
                Acquired::Yes
            }
        };
        for owner in dead {
            inner.release(owner);
        }
        self.publish_counts(&inner);
        outcome
    }

    /// Who, other than `tx`, holds a table lock that a write would conflict
    /// with.
    ///
    /// A shared lock blocks writers but not readers; an exclusive one blocks
    /// both. `for_write` says which question is being asked.
    pub fn table_conflict(
        &self,
        table: &str,
        tx: TxId,
        for_write: bool,
        is_live: &dyn Fn(TxId) -> bool,
    ) -> Option<TxId> {
        if self.table_count.load(Ordering::Acquire) == 0 {
            return None;
        }
        let tx = self.owner_of(tx);
        let inner = self.lock();
        let name_id = inner.names.get(table)?;
        match inner.tables.get(name_id)? {
            Holder::Exclusive(owner) if *owner != tx && is_live(*owner) => Some(*owner),
            Holder::Shared(owners) if for_write => {
                owners.iter().find(|o| **o != tx && is_live(**o)).copied()
            }
            _ => None,
        }
    }

    /// Releases everything `tx` holds. Called when a transaction ends, however
    /// it ends.
    ///
    /// Proportional to what `tx` holds, not to every lock in the database: it
    /// runs on every commit, under the one mutex every lock operation takes.
    pub fn release_all(&self, tx: TxId) {
        let tx = self.owner_of(tx);
        self.clear_waiting(tx);
        let mut inner = self.lock();
        if !inner.owned.contains_key(&tx) {
            return;
        }
        inner.release(tx);
        inner.shrink();
        self.publish_counts(&inner);
    }

    fn blockers_of(holder: Option<&Holder>, tx: TxId, exclusive: bool, is_live: &dyn Fn(TxId) -> bool) -> SmallVec<[TxId; 4]> {
        match holder {
            Some(Holder::Exclusive(owner)) if *owner != tx && is_live(*owner) => {
                smallvec::smallvec![*owner]
            }
            Some(Holder::Shared(owners)) if exclusive => owners
                .iter()
                .copied()
                .filter(|o| *o != tx && is_live(*o))
                .collect(),
            _ => SmallVec::new(),
        }
    }

    fn blockers_in(
        &self,
        space: Space,
        name: &str,
        key: &[u8],
        tx: TxId,
        strength: LockStrength,
        is_live: &dyn Fn(TxId) -> bool,
    ) -> SmallVec<[TxId; 4]> {
        let tx = self.owner_of(tx);
        let mut inner = self.lock();
        let Some(name_id) = inner.names.get(name).copied() else {
            return SmallVec::new();
        };
        let exclusive = matches!(strength, LockStrength::Update);
        Self::blockers_of(
            inner.map(space).get(&(name_id, Key::from_slice(key))),
            tx,
            exclusive,
            is_live,
        )
    }

    /// Every live transaction a row lock request by `tx` is blocked by — all
    /// sharers, not just the first, so no cycle through any of them is missed.
    pub fn row_blockers(
        &self,
        table: &str,
        key: &[u8],
        tx: TxId,
        strength: LockStrength,
        is_live: &dyn Fn(TxId) -> bool,
    ) -> SmallVec<[TxId; 4]> {
        self.blockers_in(Space::Row, table, key, tx, strength, is_live)
    }

    /// [`Self::row_blockers`] for a referenced-key lock.
    pub fn referenced_blockers(
        &self,
        table: &str,
        column: &str,
        key: &[u8],
        tx: TxId,
        strength: LockStrength,
        is_live: &dyn Fn(TxId) -> bool,
    ) -> SmallVec<[TxId; 4]> {
        let name = referenced_name(table, column);
        self.blockers_in(Space::Referenced, &name, key, tx, strength, is_live)
    }

    /// [`Self::row_blockers`] for a table lock.
    pub fn table_blockers(
        &self,
        table: &str,
        tx: TxId,
        mode: TableLockMode,
        is_live: &dyn Fn(TxId) -> bool,
    ) -> SmallVec<[TxId; 4]> {
        let tx = self.owner_of(tx);
        let inner = self.lock();
        let Some(name_id) = inner.names.get(table) else {
            return SmallVec::new();
        };
        Self::blockers_of(
            inner.tables.get(name_id),
            tx,
            matches!(mode, TableLockMode::Exclusive),
            is_live,
        )
    }

    /// Records that `waiter` is waiting for `blockers`. Called by a wait loop
    /// once its wait has lasted [`DEADLOCK_TIMEOUT`], and on every poll after
    /// that, so the edge follows the holder as it changes.
    pub fn set_waiting(&self, waiter: TxId, blockers: &[TxId]) {
        let waiter = self.owner_of(waiter);
        if waiter == crate::core::mvcc::transaction::FROZEN_TX {
            return;
        }
        let mut waits = self.waits.lock().expect("wait graph poisoned");
        if blockers.is_empty() {
            waits.remove(&waiter);
        } else {
            waits.insert(waiter, blockers.iter().map(|b| self.owner_of(*b)).collect());
        }
    }

    /// `waiter` is no longer waiting — it got the lock, gave up, or ended.
    pub fn clear_waiting(&self, waiter: TxId) {
        let waiter = self.owner_of(waiter);
        let mut waits = self.waits.lock().expect("wait graph poisoned");
        waits.remove(&waiter);
        if waits.is_empty() && waits.capacity() > 64 {
            waits.shrink_to(0);
        }
    }

    /// Whether `waiter` is on a cycle of the wait-for graph **and** is the
    /// one transaction on that cycle that should give up: the youngest, the
    /// highest id, as the least work is lost. Every participant asks; exactly
    /// one is told yes, so the others go on waiting and get their locks once
    /// the victim's transaction ends.
    pub fn deadlock_victim(&self, waiter: TxId, is_live: &dyn Fn(TxId) -> bool) -> bool {
        let waiter = self.owner_of(waiter);
        let waits = self.waits.lock().expect("wait graph poisoned");
        if !waits.contains_key(&waiter) {
            return false;
        }
        // Depth-first from the waiter, looking for a path back to it.
        let mut path: Vec<(TxId, usize)> = vec![(waiter, 0)];
        let mut visited: std::collections::HashSet<TxId> = std::collections::HashSet::new();
        visited.insert(waiter);
        while let Some((node, next)) = path.last().copied() {
            let edges = waits.get(&node).map(|e| e.as_slice()).unwrap_or(&[]);
            if next >= edges.len() {
                path.pop();
                continue;
            }
            path.last_mut().expect("non-empty").1 += 1;
            let to = edges[next];
            if to == waiter {
                let victim = path.iter().map(|(n, _)| *n).max().unwrap_or(waiter);
                return victim == waiter;
            }
            // A finished holder is not waiting for anything; nor is a
            // transaction with no entry.
            if !is_live(to) || !waits.contains_key(&to) || !visited.insert(to) {
                continue;
            }
            path.push((to, 0));
        }
        false
    }

    /// Clears `waiter`'s wait-for edge when dropped, however the wait ends —
    /// granted, timed out, cancelled. An edge left behind by a wait that
    /// stopped would let a later waiter find a cycle that is not there.
    pub fn waiting_guard(&self, waiter: TxId) -> WaitingGuard<'_> {
        WaitingGuard {
            locks: self,
            waiter,
        }
    }

    /// How many rows are locked. Diagnostic.
    pub fn len(&self) -> usize {
        self.row_count.load(Ordering::Acquire)
    }

    /// True when no row is locked `FOR UPDATE`/`FOR SHARE`. Referenced-key
    /// locks are not counted: they never block an ordinary row write.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many referenced keys are locked. Diagnostic.
    pub fn referenced_len(&self) -> usize {
        self.lock().referenced.len()
    }

    /// Capacity the maps are holding, for tests of the shrink. Diagnostic.
    pub fn capacity(&self) -> usize {
        let inner = self.lock();
        inner.rows.capacity() + inner.referenced.capacity() + inner.owned.capacity()
    }
}

/// What an acquire should do, given the entry as it stands.
enum Decision {
    /// Answered without changing the entry's holders (beyond pruning the dead).
    Return(Acquired),
    /// Added to an existing shared entry; `newly` if it was not already there.
    Joined { newly: bool },
    /// Write a fresh holder. `already_mine` when the caller held the entry
    /// before (an upgrade), so the owner index already names it.
    Insert { already_mine: bool },
}

/// The conflict rules, in one place for rows, referenced keys and tables.
///
/// Holders whose transaction has ended are pruned and reported in `dead`: a
/// lock whose owner has finished is nobody's — transactions end in ways that
/// do not run cleanup, a crashed session, an aborted block — and a lock that
/// outlived its holder would wedge the row until restart.
fn decide(
    holder: Option<&mut Holder>,
    tx: TxId,
    exclusive: bool,
    is_live: &dyn Fn(TxId) -> bool,
    dead: &mut SmallVec<[TxId; 2]>,
) -> Decision {
    match holder {
        None => Decision::Insert {
            already_mine: false,
        },
        Some(Holder::Exclusive(owner)) => {
            // Re-taking a lock already held succeeds, whatever strength is
            // asked for: exclusive covers shared.
            if *owner == tx {
                return Decision::Return(Acquired::Yes);
            }
            if is_live(*owner) {
                return Decision::Return(Acquired::Held(*owner));
            }
            dead.push(*owner);
            Decision::Insert {
                already_mine: false,
            }
        }
        Some(Holder::Shared(owners)) => {
            owners.retain(|owner| {
                let keep = *owner == tx || is_live(*owner);
                if !keep {
                    dead.push(*owner);
                }
                keep
            });
            let mine = owners.contains(&tx);
            if !exclusive {
                if !mine {
                    owners.push(tx);
                }
                return Decision::Joined { newly: !mine };
            }
            match owners.iter().find(|owner| **owner != tx) {
                Some(other) => Decision::Return(Acquired::Held(*other)),
                // Only this transaction shares it, so it may upgrade.
                None => Decision::Insert { already_mine: mine },
            }
        }
    }
}

/// The name a referenced-key lock is taken under. Case-folded: a foreign key
/// records the parent's name as its DDL spelled it, and a `DELETE` names the
/// table as its statement did.
fn referenced_name(table: &str, column: &str) -> String {
    let mut name = String::with_capacity(table.len() + column.len() + 1);
    name.push_str(&table.to_ascii_lowercase());
    name.push('\0');
    name.push_str(&column.to_ascii_lowercase());
    name
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIVE: &dyn Fn(TxId) -> bool = &|_| true;
    const DEAD: &dyn Fn(TxId) -> bool = &|_| false;

    #[test]
    fn an_exclusive_lock_excludes_everyone_else() {
        let locks = RowLocks::default();
        assert_eq!(
            locks.acquire("t", b"1", 10, LockStrength::Update, LIVE),
            Acquired::Yes
        );
        assert_eq!(
            locks.acquire("t", b"1", 20, LockStrength::Update, LIVE),
            Acquired::Held(10)
        );
        // A different row is unaffected.
        assert_eq!(
            locks.acquire("t", b"2", 20, LockStrength::Update, LIVE),
            Acquired::Yes
        );
        // As is the same key in a different table.
        assert_eq!(
            locks.acquire("u", b"1", 20, LockStrength::Update, LIVE),
            Acquired::Yes
        );
    }

    #[test]
    fn a_holder_may_take_its_own_lock_again() {
        // The claim idiom locks rows and then updates them in the same
        // transaction, which asks for the same lock a second time. Refusing
        // that would deadlock a statement against itself.
        let locks = RowLocks::default();
        assert_eq!(
            locks.acquire("t", b"1", 10, LockStrength::Update, LIVE),
            Acquired::Yes
        );
        assert_eq!(
            locks.acquire("t", b"1", 10, LockStrength::Update, LIVE),
            Acquired::Yes
        );
    }

    #[test]
    fn releasing_a_transaction_frees_every_row_it_held() {
        let locks = RowLocks::default();
        locks.acquire("t", b"1", 10, LockStrength::Update, LIVE);
        locks.acquire("t", b"2", 10, LockStrength::Update, LIVE);
        assert_eq!(locks.len(), 2);
        locks.release_all(10);
        assert!(locks.is_empty());
        assert_eq!(
            locks.acquire("t", b"1", 20, LockStrength::Update, LIVE),
            Acquired::Yes
        );
    }

    #[test]
    fn a_statement_outside_a_transaction_never_records_a_lock() {
        // Nothing commits or aborts `FROZEN_TX`, so a lock recorded under it is
        // released by nothing and wedges the row until the process restarts.
        // One `SELECT … FOR UPDATE` run outside a transaction block did exactly
        // that, and every later write to the row was refused as "locked by
        // transaction 1".
        use crate::core::mvcc::transaction::FROZEN_TX;
        let locks = RowLocks::default();
        assert_eq!(
            locks.acquire("t", b"1", FROZEN_TX, LockStrength::Update, LIVE),
            Acquired::Yes
        );
        assert!(locks.is_empty(), "nothing may be recorded");
        // And a real transaction is still free to take it afterwards.
        assert_eq!(
            locks.acquire("t", b"1", 20, LockStrength::Update, LIVE),
            Acquired::Yes
        );
        // It still *sees* a lock somebody else holds, so SKIP LOCKED and
        // NOWAIT keep working outside a transaction.
        assert_eq!(
            locks.acquire("t", b"1", FROZEN_TX, LockStrength::Update, LIVE),
            Acquired::Held(20)
        );
    }

    #[test]
    fn a_lock_whose_owner_has_ended_is_not_held() {
        // Transactions end in ways that run no cleanup. A lock that outlived
        // its owner would wedge the row until the process restarted.
        let locks = RowLocks::default();
        locks.acquire("t", b"1", 10, LockStrength::Update, LIVE);
        assert_eq!(
            locks.acquire("t", b"1", 20, LockStrength::Update, DEAD),
            Acquired::Yes
        );
    }

    #[test]
    fn a_table_lock_excludes_other_transactions() {
        let locks = RowLocks::default();
        assert_eq!(
            locks.acquire_table("t", 10, TableLockMode::Exclusive, LIVE),
            Acquired::Yes
        );
        assert_eq!(
            locks.acquire_table("t", 20, TableLockMode::Exclusive, LIVE),
            Acquired::Held(10)
        );
        // It blocks readers as well as writers, which is what ACCESS EXCLUSIVE
        // means.
        assert_eq!(locks.table_conflict("t", 20, false, LIVE), Some(10));
        assert_eq!(locks.table_conflict("t", 20, true, LIVE), Some(10));
        // Its own holder is never blocked by it.
        assert_eq!(locks.table_conflict("t", 10, true, LIVE), None);
        // Another table is unaffected.
        assert_eq!(locks.table_conflict("u", 20, true, LIVE), None);
    }

    #[test]
    fn a_shared_table_lock_admits_readers_and_blocks_writers() {
        let locks = RowLocks::default();
        locks.acquire_table("t", 10, TableLockMode::Shared, LIVE);
        assert_eq!(
            locks.acquire_table("t", 20, TableLockMode::Shared, LIVE),
            Acquired::Yes
        );
        assert_eq!(locks.table_conflict("t", 30, false, LIVE), None);
        assert_eq!(locks.table_conflict("t", 30, true, LIVE), Some(10));
    }

    #[test]
    fn ending_a_transaction_releases_its_table_locks_too() {
        // A stranded table lock would wedge the whole table until restart.
        let locks = RowLocks::default();
        locks.acquire_table("t", 10, TableLockMode::Exclusive, LIVE);
        locks.acquire("t", b"1", 10, LockStrength::Update, LIVE);
        locks.release_all(10);
        assert_eq!(
            locks.acquire_table("t", 20, TableLockMode::Exclusive, LIVE),
            Acquired::Yes
        );
        assert!(locks.is_empty());
    }

    #[test]
    fn shared_locks_coexist_but_exclude_a_writer() {
        let locks = RowLocks::default();
        assert_eq!(
            locks.acquire("t", b"1", 10, LockStrength::Share, LIVE),
            Acquired::Yes
        );
        assert_eq!(
            locks.acquire("t", b"1", 20, LockStrength::Share, LIVE),
            Acquired::Yes
        );
        assert_eq!(
            locks.acquire("t", b"1", 30, LockStrength::Update, LIVE),
            Acquired::Held(10)
        );
        // Once the other sharer is gone, the remaining one may upgrade.
        locks.release_all(20);
        assert_eq!(
            locks.acquire("t", b"1", 10, LockStrength::Update, LIVE),
            Acquired::Yes
        );
    }

    #[test]
    fn a_referenced_key_lock_is_its_own_space() {
        // A child insert's key-share lock must not make every UPDATE and
        // DELETE in the database pay for the row-lock lookup, and must not
        // collide with a FOR UPDATE keyed by a different column.
        let locks = RowLocks::default();
        assert_eq!(
            locks.acquire_referenced("p", "id", b"1", 10, LockStrength::Share, LIVE),
            Acquired::Yes
        );
        assert!(locks.is_empty(), "row space untouched");
        assert_eq!(locks.referenced_len(), 1);
        assert_eq!(
            locks.acquire("p", b"1", 20, LockStrength::Update, LIVE),
            Acquired::Yes
        );
        // Children share; a delete of the key waits for them.
        assert_eq!(
            locks.acquire_referenced("p", "id", b"1", 11, LockStrength::Share, LIVE),
            Acquired::Yes
        );
        assert_eq!(
            locks.acquire_referenced("P", "ID", b"1", 30, LockStrength::Update, LIVE),
            Acquired::Held(10),
            "names are case-folded: a DELETE names the table as its statement spells it"
        );
        // A different column of the same table is a different key.
        assert_eq!(
            locks.acquire_referenced("p", "alt", b"1", 30, LockStrength::Update, LIVE),
            Acquired::Yes
        );
        locks.release_all(10);
        locks.release_all(11);
        assert_eq!(
            locks.acquire_referenced("p", "id", b"1", 30, LockStrength::Update, LIVE),
            Acquired::Yes
        );
        // And a child now waits for the delete.
        assert_eq!(
            locks.acquire_referenced("p", "id", b"1", 12, LockStrength::Share, LIVE),
            Acquired::Held(30)
        );
    }

    #[test]
    fn a_shared_holder_upgrading_keeps_one_index_entry() {
        let locks = RowLocks::default();
        locks.acquire("t", b"1", 10, LockStrength::Share, LIVE);
        locks.acquire("t", b"1", 10, LockStrength::Update, LIVE);
        locks.acquire("t", b"1", 10, LockStrength::Share, LIVE);
        assert_eq!(locks.lock().owned.get(&10).map(|v| v.len()), Some(1));
        locks.release_all(10);
        assert!(locks.is_empty());
        assert!(locks.lock().owned.is_empty());
    }

    #[test]
    fn a_dead_owner_found_on_acquire_loses_all_its_locks() {
        // A transaction whose end ran no cleanup must not keep its index entry
        // — or its other locks — for the life of the process.
        let locks = RowLocks::default();
        locks.acquire("t", b"1", 10, LockStrength::Update, LIVE);
        locks.acquire("t", b"2", 10, LockStrength::Update, LIVE);
        let only_20: &dyn Fn(TxId) -> bool = &|tx| tx == 20;
        assert_eq!(
            locks.acquire("t", b"1", 20, LockStrength::Update, only_20),
            Acquired::Yes
        );
        assert_eq!(locks.len(), 1, "10's other lock went with it");
        assert!(!locks.lock().owned.contains_key(&10));
    }

    #[test]
    fn a_large_claim_gives_its_memory_back() {
        let locks = RowLocks::default();
        for i in 0u32..50_000 {
            locks.acquire("t", &i.to_le_bytes(), 10, LockStrength::Update, LIVE);
        }
        let peak = locks.capacity();
        assert!(peak >= 50_000, "{}", peak);
        locks.release_all(10);
        assert!(locks.is_empty());
        assert!(
            locks.capacity() < peak / 100,
            "capacity {} kept after release (peak {})",
            locks.capacity(),
            peak
        );
        assert!(locks.lock().names.is_empty(), "interned names cleared when idle");
    }

    #[test]
    fn release_touches_only_the_releasing_transactions_locks() {
        let locks = RowLocks::default();
        for i in 0u32..1_000 {
            locks.acquire("t", &i.to_le_bytes(), 10, LockStrength::Update, LIVE);
        }
        locks.acquire("t", b"x", 20, LockStrength::Update, LIVE);
        locks.release_all(20);
        assert_eq!(locks.len(), 1_000);
        locks.release_all(99); // holds nothing
        assert_eq!(locks.len(), 1_000);
    }

    #[test]
    fn a_savepoints_lock_belongs_to_its_transaction() {
        let manager = std::sync::Arc::new(TransactionManager::in_memory());
        let locks = RowLocks::for_transactions(manager.clone());
        let top = manager.begin();
        let sub = manager.begin_savepoint(top);
        let live: &dyn Fn(TxId) -> bool = &|tx| tx == top || tx == sub;

        assert_eq!(
            locks.acquire("t", b"1", top, LockStrength::Update, live),
            Acquired::Yes
        );
        // Not somebody else's lock from inside the savepoint.
        assert_eq!(
            locks.acquire("t", b"1", sub, LockStrength::Update, live),
            Acquired::Yes
        );
        assert_eq!(
            locks.acquire("t", b"2", sub, LockStrength::Update, live),
            Acquired::Yes
        );
        // Ending the transaction releases what the savepoint took.
        locks.release_all(top);
        assert!(locks.is_empty());
    }
}
