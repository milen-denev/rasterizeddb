//! Table locks: the one owner-keyed lock manager that statements and DDL go
//! through (docs/persistent-indexes.md §7, W3 option C).
//!
//! # Wired (B3b)
//!
//! `RowLocks` owns the manager and delegates its table space to it, so
//! table, row and referenced-key waits share one graph
//! (`RowLocks::table_deadlock_check`, `RowLocks::deadlock_victim`). Every
//! table a statement executes against is taken through
//! `Database::table_for` / `relation_for` / `tables_for`
//! (`core/database/table_for.rs`) for the statement's [`LockOwner`], and
//! `LOCK TABLE` maps its mode through [`PgLockMode::table_mode`].
//!
//! # Modes
//!
//! PostgreSQL's eight table modes, narrowed to the four this engine takes. A
//! PostgreSQL mode with no exact match maps to a stricter one, never a weaker
//! one ([`PgLockMode::table_mode`]):
//!
//! | Mode | Taken by | Conflicts with |
//! |---|---|---|
//! | AS (ACCESS SHARE) | SELECT; the FK parent check | AE |
//! | RX (ROW EXCLUSIVE) | INSERT, UPDATE, DELETE, `FOR UPDATE`, replication apply, VACUUM | S, AE |
//! | S (SHARE) | `LOCK TABLE … SHARE` | RX, AE |
//! | AE (ACCESS EXCLUSIVE) | every reloading DDL; `LOCK TABLE` in the exclusive modes | all |
//!
//! Restricted to these four, the matrix is PostgreSQL's own (the tests check
//! it against PostgreSQL's full table).
//!
//! # Holders hold a mode *set*
//!
//! An owner can hold several modes on one table at once (finding L5): a block
//! that inserted (RX) and then ran a rewriting ALTER (AE) must be able to give
//! back only the AE when the DDL statement ends and keep its RX, or the
//! REPEATABLE READ protection §7 relies on is lost with it. So each holder is
//! `(owner, ModeSet)`, [`TableLocks::release_mode`] drops one mode and
//! [`TableLocks::downgrade`] swaps one for another without a gap.
//!
//! # Granting (PostgreSQL's ProcSleep rule, keyed by owner)
//!
//! A request is granted when it conflicts with no mode *another* owner holds,
//! and no request of another owner that conflicts with it waits ahead of its
//! place in the queue:
//! - (a) every conflicting waiter counts, weak ones included;
//! - (b) the requester's place is just ahead of the first waiter that a mode
//!   it already holds conflicts with (that waiter waits for it, so queueing
//!   behind it would deadlock), and otherwise the end of the queue.
//!
//! This is PostgreSQL's own rule (`ProcSleep` and `ProcLockWakeup`, keyed by
//! owner). Rule (a) is what stops a stream of requests starving a waiter they
//! conflict with: a waiting ALTER behind readers, and also a waiting RX
//! behind a stream of S holders (S does not conflict with S, so a rule that
//! counted only S and AE waiters let every new S pass the RX). Rule (b) is
//! what keeps a block that already holds the table from queueing behind an
//! ALTER that is itself waiting for that block (R1: `BEGIN; INSERT`, a
//! waiting `CREATE INDEX`, and the block's next statement and COMMIT go
//! through), and lets it pass only the waiters it blocks: a rule that let any
//! holder go ahead let a stream of AS holders requesting RX pass a waiting S
//! they do not block, delaying it for as long as the traffic lasted. An
//! owner's own modes never conflict, so an upgrade (RX to AE) waits only for
//! other owners.
//!
//! # The weak-mode fast path
//!
//! Every statement takes and releases a lock per table, readers included, so
//! AS and RX must not serialise on one mutex. As in PostgreSQL:
//! - [`FAST_PATH_SHARDS`] shards keyed by owner, each a mutex over that
//!   owner's weak holdings;
//! - [`STRONG_SLOTS`] counters of strong (S or AE) requests held or pending,
//!   keyed by a hash of the table name; two tables sharing a slot only send
//!   each other's weak requests down the main path, never the reverse;
//! - a weak request reads its table's counter **under its shard's lock**, and
//!   with the counter at zero records itself there and returns;
//! - a strong request increments the counter, then moves every fast-path
//!   entry for its table, from every shard, into the main table, each under
//!   that shard's lock, and only then looks at the main table.
//!
//! Either the weak request's shard section runs before the strong request
//! locks that shard, and the migration finds the entry, or after, and the
//! read sees the increment, which happened before the strong request took
//! the shard's lock. Either way no AE is granted beside a fast-path RX. The
//! counter stays raised while the strong mode is held or awaited and drops
//! when it is released or the wait is given up.
//!
//! # Waits, the wait-for graph and victims
//!
//! A queued request is granted by whoever frees what it waited for (release,
//! downgrade, an abandoned or failed wait ahead of it), in queue order, and
//! woken through its [`Wait`]. Its blockers ([`TableLocks::blockers`]) are the
//! other owners holding a conflicting mode plus the conflicting waiters ahead
//! of its place (rule (b) above). After
//! [`TableLockConfig::deadlock_timeout`] a waiter looks for a cycle through
//! itself ([`TableLocks::deadlock_check`]); the victim is chosen by
//! [`choose_victim`], whatever member did the looking, and its wait fails
//! with 40P01:
//! 1. a DDL that has not begun retirement, the younger of two (no wait
//!    happens after retirement begins, so every waiting DDL qualifies);
//! 2. otherwise any S or AE requester, `LOCK TABLE` included;
//! 3. otherwise the youngest member that is not an autocommit reader.
//!
//! An autocommit reader is never chosen: AS and RX conflict only with S and
//! AE, so the member whose edge points at a reader (it conflicts with what
//! the reader holds or requests) requests S or AE, and every cycle through a
//! reader contains an S or AE requester.
//!
//! # lock_timeout
//!
//! [`LockTimeout::from_setting_ms`] reads PostgreSQL's `lock_timeout`: `0`,
//! the default, means no bound at all (decision 12), never "fail at once".
//! Past a positive bound the wait fails with 55P03; cancellation ends it with
//! 57014.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use smallvec::SmallVec;

use crate::core::mvcc::TxId;
use crate::core::mvcc::transaction::{FROZEN_TX, NO_TX};

/// Hashes a [`LockOwner`] (a transaction id) with one multiply: ids are
/// sequential, so the golden-ratio multiply spreads them over the table's
/// top bits, where hashbrown looks. The owner-keyed maps are on every
/// statement's path (a weak request and its release), and SipHash cost more
/// than the rest of the fast path.
#[derive(Default, Clone, Copy)]
pub(crate) struct OwnerHasher(u64);

impl std::hash::Hasher for OwnerHasher {
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 = (self.0 ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3);
        }
    }

    fn write_u64(&mut self, n: u64) {
        self.0 = (self.0 ^ n).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

type OwnerMap<V> = HashMap<LockOwner, V, std::hash::BuildHasherDefault<OwnerHasher>>;
type TableMap<V> = HashMap<Arc<str>, V, ahash::RandomState>;

/// The four table-lock modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum TableMode {
    /// ACCESS SHARE.
    AccessShare = 0,
    /// ROW EXCLUSIVE.
    RowExclusive = 1,
    /// SHARE.
    Share = 2,
    /// ACCESS EXCLUSIVE.
    AccessExclusive = 3,
}

const AS: u8 = 1 << TableMode::AccessShare as u8;
const RX: u8 = 1 << TableMode::RowExclusive as u8;
const S: u8 = 1 << TableMode::Share as u8;
const AE: u8 = 1 << TableMode::AccessExclusive as u8;
const STRONG: u8 = S | AE;

/// Row `m`: the modes `m` conflicts with. Symmetric, as PostgreSQL's is.
const CONFLICTS: [u8; 4] = [
    AE,                // AS
    S | AE,            // RX
    RX | AE,           // S
    AS | RX | S | AE,  // AE
];

impl TableMode {
    pub const ALL: [TableMode; 4] = [
        TableMode::AccessShare,
        TableMode::RowExclusive,
        TableMode::Share,
        TableMode::AccessExclusive,
    ];

    const fn bit(self) -> u8 {
        1 << self as u8
    }

    /// Whether a lock in `self` and one in `other`, held by two owners,
    /// conflict.
    pub const fn conflicts_with(self, other: TableMode) -> bool {
        CONFLICTS[self as usize] & other.bit() != 0
    }

    /// S and AE: the modes that take the main path and raise the table's
    /// strong counter.
    pub const fn is_strong(self) -> bool {
        self.bit() & STRONG != 0
    }

    /// The abbreviation §7 uses.
    pub const fn short(self) -> &'static str {
        match self {
            TableMode::AccessShare => "AS",
            TableMode::RowExclusive => "RX",
            TableMode::Share => "S",
            TableMode::AccessExclusive => "AE",
        }
    }
}

/// The modes one owner holds on one table.
#[derive(Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct ModeSet(u8);

impl ModeSet {
    pub const EMPTY: ModeSet = ModeSet(0);

    pub const fn of(mode: TableMode) -> ModeSet {
        ModeSet(mode.bit())
    }

    pub const fn contains(self, mode: TableMode) -> bool {
        self.0 & mode.bit() != 0
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub fn insert(&mut self, mode: TableMode) {
        self.0 |= mode.bit();
    }

    pub fn remove(&mut self, mode: TableMode) {
        self.0 &= !mode.bit();
    }

    pub const fn union(self, other: ModeSet) -> ModeSet {
        ModeSet(self.0 | other.0)
    }

    /// Whether any mode in the set conflicts with `mode`.
    pub const fn conflicts_with(self, mode: TableMode) -> bool {
        CONFLICTS[mode as usize] & self.0 != 0
    }

    /// How many of S and AE the set holds: each raised the strong counter
    /// once when it was requested.
    fn strong_count(self) -> u32 {
        (self.0 & STRONG).count_ones()
    }

    pub fn iter(self) -> impl Iterator<Item = TableMode> {
        TableMode::ALL.into_iter().filter(move |m| self.contains(*m))
    }
}

impl fmt::Debug for ModeSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter().map(TableMode::short)).finish()
    }
}

/// PostgreSQL's eight table modes, as `LOCK TABLE` names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgLockMode {
    AccessShare,
    RowShare,
    RowExclusive,
    ShareUpdateExclusive,
    Share,
    ShareRowExclusive,
    Exclusive,
    AccessExclusive,
}

impl PgLockMode {
    pub const ALL: [PgLockMode; 8] = [
        PgLockMode::AccessShare,
        PgLockMode::RowShare,
        PgLockMode::RowExclusive,
        PgLockMode::ShareUpdateExclusive,
        PgLockMode::Share,
        PgLockMode::ShareRowExclusive,
        PgLockMode::Exclusive,
        PgLockMode::AccessExclusive,
    ];

    /// The mode a `LOCK TABLE … IN <self> MODE` takes here: the same mode
    /// where one exists, otherwise a stricter one, never a weaker one.
    pub const fn table_mode(self) -> TableMode {
        match self {
            PgLockMode::AccessShare => TableMode::AccessShare,
            PgLockMode::RowShare | PgLockMode::RowExclusive => TableMode::RowExclusive,
            PgLockMode::Share => TableMode::Share,
            PgLockMode::ShareUpdateExclusive
            | PgLockMode::ShareRowExclusive
            | PgLockMode::Exclusive
            | PgLockMode::AccessExclusive => TableMode::AccessExclusive,
        }
    }
}

/// Who holds and requests table locks: a block's root transaction, or an
/// autocommit statement's owner O (§7 Owner).
///
/// Never `FROZEN_TX`: that id is shared by every session, so a lock held
/// under it would let any session's request pass rule (b) and would never be
/// released. The constructors refuse it, and nothing converts a bare `TxId`.
/// The id orders owners by age: a larger id is younger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LockOwner(TxId);

impl LockOwner {
    /// A block's owner: its root transaction. `None` for `FROZEN_TX` or
    /// `NO_TX`.
    pub fn block(root: TxId) -> Option<LockOwner> {
        LockOwner::new(root)
    }

    /// An autocommit statement's owner O, begun before its first lock.
    /// `None` for `FROZEN_TX` or `NO_TX`.
    pub fn autocommit(o: TxId) -> Option<LockOwner> {
        LockOwner::new(o)
    }

    fn new(id: TxId) -> Option<LockOwner> {
        (id != FROZEN_TX && id != NO_TX).then_some(LockOwner(id))
    }

    pub fn id(self) -> TxId {
        self.0
    }
}

/// What the waiting owner's statement is, for [`choose_victim`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WaitRole {
    /// A reloading, rewriting, renaming, moving or dropping DDL. It waits only
    /// before retirement (§7 step 1), so a waiting one has written nothing.
    Ddl,
    /// An autocommit statement that has written nothing (`!wrote_family(O)`,
    /// finding L8).
    AutocommitReader,
    /// Everything else: blocks, writers, `LOCK TABLE`.
    Other,
}

/// `lock_timeout`, as a wait reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockTimeout {
    /// PostgreSQL's default `0`: wait as long as it takes.
    Unbounded,
    /// Fail with 55P03 once the wait has lasted this long.
    After(Duration),
}

impl LockTimeout {
    /// The session setting in milliseconds; `0` is no bound, as in PostgreSQL.
    pub fn from_setting_ms(ms: u64) -> LockTimeout {
        if ms == 0 {
            LockTimeout::Unbounded
        } else {
            LockTimeout::After(Duration::from_millis(ms))
        }
    }

    pub fn limit(self) -> Option<Duration> {
        match self {
            LockTimeout::Unbounded => None,
            LockTimeout::After(d) => Some(d),
        }
    }
}

/// Why a table-lock wait ended without the lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockError {
    /// The wait was the chosen victim of a cycle.
    Deadlock,
    /// The wait outlasted `lock_timeout`.
    Timeout,
    /// The statement was cancelled while it waited.
    Cancelled,
}

impl LockError {
    pub fn sqlstate(self) -> &'static str {
        match self {
            LockError::Deadlock => "40P01",
            LockError::Timeout => "55P03",
            LockError::Cancelled => "57014",
        }
    }
}

impl fmt::Display for LockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LockError::Deadlock => f.write_str("deadlock detected"),
            LockError::Timeout => f.write_str("canceling statement due to lock timeout"),
            LockError::Cancelled => f.write_str("canceling statement due to user request"),
        }
    }
}

impl std::error::Error for LockError {}

/// Where a queued request stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitOutcome {
    Waiting,
    Granted,
    /// Chosen as a cycle's victim: the wait failed and holds nothing new.
    Deadlock,
    /// Given up by its owner (timeout, cancellation, release).
    Abandoned,
}

const WAITING: u8 = 0;
const GRANTED: u8 = 1;
const DEADLOCK: u8 = 2;
const ABANDONED: u8 = 3;

/// A queued request's outcome cell. Written only under the main mutex, read
/// by its waiter.
struct WaitSlot {
    state: AtomicU8,
    notify: tokio::sync::Notify,
}

impl WaitSlot {
    fn new() -> WaitSlot {
        WaitSlot {
            state: AtomicU8::new(WAITING),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn outcome(&self) -> WaitOutcome {
        match self.state.load(Ordering::Acquire) {
            WAITING => WaitOutcome::Waiting,
            GRANTED => WaitOutcome::Granted,
            DEADLOCK => WaitOutcome::Deadlock,
            _ => WaitOutcome::Abandoned,
        }
    }

    /// Settles the wait and wakes its waiter. `notify_one` keeps a permit if
    /// the waiter is not parked yet, so the wake is never lost.
    fn settle(&self, state: u8) {
        self.state.store(state, Ordering::Release);
        self.notify.notify_one();
    }
}

/// A queued request, held by the owner that made it.
pub struct Wait {
    owner: LockOwner,
    table: Arc<str>,
    mode: TableMode,
    slot: Arc<WaitSlot>,
}

impl Wait {
    pub fn outcome(&self) -> WaitOutcome {
        self.slot.outcome()
    }

    pub fn owner(&self) -> LockOwner {
        self.owner
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn mode(&self) -> TableMode {
        self.mode
    }
}

/// What one request did.
pub enum Request {
    /// Held now (or already held).
    Granted,
    /// Queued; the [`Wait`] says when it is settled.
    Queued(Wait),
}

impl Request {
    pub fn is_granted(&self) -> bool {
        matches!(self, Request::Granted)
    }
}

/// A member of a wait-for cycle, as [`choose_victim`] weighs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CycleMember {
    pub owner: LockOwner,
    /// The table mode it is waiting for; `None` when its edge is a row or
    /// referenced-key wait (once `RowLocks` shares this graph).
    pub mode: Option<TableMode>,
    pub role: WaitRole,
}

/// The member of a cycle whose wait fails (§7 Victims), or `None` when every
/// member is an autocommit reader, which the modes make impossible.
pub fn choose_victim(members: &[CycleMember]) -> Option<LockOwner> {
    let youngest = |keep: &dyn Fn(&CycleMember) -> bool| {
        members.iter().filter(|m| keep(m)).map(|m| m.owner).max()
    };
    youngest(&|m| m.role == WaitRole::Ddl)
        .or_else(|| youngest(&|m| m.mode.is_some_and(TableMode::is_strong)))
        .or_else(|| youngest(&|m| m.role != WaitRole::AutocommitReader))
}

/// One queued request.
struct Waiter {
    owner: LockOwner,
    mode: TableMode,
    role: WaitRole,
    /// The fast-path modes the owner held on the table when it queued. They
    /// cannot leave while it waits (only it releases them, and a migration
    /// moves them into `holders`), so this stays a subset of what it holds.
    fast: ModeSet,
    slot: Arc<WaitSlot>,
}

/// One table's main-path state.
#[derive(Default)]
struct TableState {
    holders: SmallVec<[(LockOwner, ModeSet); 4]>,
    /// FIFO.
    waiters: VecDeque<Waiter>,
}

impl TableState {
    fn held_by(&self, owner: LockOwner) -> ModeSet {
        self.holders
            .iter()
            .find(|(o, _)| *o == owner)
            .map_or(ModeSet::EMPTY, |(_, set)| *set)
    }

    /// Whether another owner holds a mode that conflicts with `mode`.
    fn holder_conflict(&self, owner: LockOwner, mode: TableMode) -> bool {
        self.holders
            .iter()
            .any(|(o, set)| *o != owner && set.conflicts_with(mode))
    }

    /// The waiters, among the first `ahead`, that a request for `mode` by
    /// `owner`, holding `held` on the table, queues behind: rules (a) and
    /// (b). Its place is just ahead of the first waiter of another owner that
    /// `held` conflicts with, or `ahead`; every waiter of another owner
    /// before that place whose request conflicts with `mode` holds it back.
    fn waiters_ahead(
        &self,
        owner: LockOwner,
        mode: TableMode,
        held: ModeSet,
        ahead: usize,
    ) -> impl Iterator<Item = &Waiter> {
        self.waiters
            .iter()
            .take(ahead)
            .filter(move |w| w.owner != owner)
            .take_while(move |w| !held.conflicts_with(w.mode))
            .filter(move |w| w.mode.conflicts_with(mode))
    }

    /// The grant rule: no conflicting holder, and no conflicting waiter ahead
    /// of the request's place.
    fn grantable(&self, owner: LockOwner, mode: TableMode, held: ModeSet, ahead: usize) -> bool {
        !self.holder_conflict(owner, mode) && self.waiters_ahead(owner, mode, held, ahead).next().is_none()
    }

    /// Adds `mode` to `owner`'s set. True if the owner held nothing here
    /// before, so the owner index needs the table.
    fn add(&mut self, owner: LockOwner, mode: TableMode) -> bool {
        match self.holders.iter_mut().find(|(o, _)| *o == owner) {
            Some((_, set)) => {
                set.insert(mode);
                false
            }
            None => {
                self.holders.push((owner, ModeSet::of(mode)));
                true
            }
        }
    }

    fn is_idle(&self) -> bool {
        self.holders.is_empty() && self.waiters.is_empty()
    }
}

#[derive(Default)]
struct Main {
    tables: TableMap<TableState>,
    /// The tables each owner holds a main-path mode on, so releasing an owner
    /// costs its own locks, not a walk of every table.
    owned: OwnerMap<SmallVec<[Arc<str>; 4]>>,
    /// The table each queued owner waits on. An owner waits for one lock at a
    /// time: its statement is one task.
    waiting: OwnerMap<Arc<str>>,
}

/// One fast-path shard: weak holdings of the owners hashed to it.
#[derive(Default)]
struct Shard {
    held: OwnerMap<SmallVec<[(Arc<str>, ModeSet); 4]>>,
}

impl Shard {
    fn held_on(&self, owner: LockOwner, table: &str) -> ModeSet {
        self.held
            .get(&owner)
            .and_then(|v| v.iter().find(|(t, _)| &**t == table))
            .map_or(ModeSet::EMPTY, |(_, set)| *set)
    }

    fn record(&mut self, owner: LockOwner, table: &str, mode: TableMode) {
        let list = self.held.entry(owner).or_default();
        match list.iter_mut().find(|(t, _)| &**t == table) {
            Some((_, set)) => set.insert(mode),
            None => list.push((Arc::from(table), ModeSet::of(mode))),
        }
    }

    /// Drops `mode` from `owner`'s entry for `table`. True if it was there.
    fn remove(&mut self, owner: LockOwner, table: &str, mode: TableMode) -> bool {
        let Some(list) = self.held.get_mut(&owner) else {
            return false;
        };
        let Some(pos) = list.iter().position(|(t, _)| &**t == table) else {
            return false;
        };
        let had = list[pos].1.contains(mode);
        list[pos].1.remove(mode);
        if list[pos].1.is_empty() {
            list.swap_remove(pos);
        }
        if list.is_empty() {
            self.held.remove(&owner);
        }
        had
    }

    /// Takes every entry for `table` out of the shard.
    fn take_table(&mut self, table: &str) -> SmallVec<[(LockOwner, ModeSet); 4]> {
        let mut moved = SmallVec::new();
        self.held.retain(|owner, list| {
            if let Some(pos) = list.iter().position(|(t, _)| &**t == table) {
                moved.push((*owner, list.swap_remove(pos).1));
            }
            !list.is_empty()
        });
        moved
    }
}

/// Fast-path shards, keyed by owner.
pub const FAST_PATH_SHARDS: usize = 64;
/// Strong-request counters, keyed by a hash of the table name.
pub const STRONG_SLOTS: usize = 1024;

/// The wait loop's timing.
#[derive(Debug, Clone, Copy)]
pub struct TableLockConfig {
    /// How long a wait lasts before it looks for a cycle, and from then on
    /// looks on every poll.
    pub deadlock_timeout: Duration,
    /// How often a parked wait wakes to check cancellation and, past
    /// `deadlock_timeout`, the graph. A grant wakes it at once.
    pub poll: Duration,
}

impl Default for TableLockConfig {
    fn default() -> Self {
        TableLockConfig {
            deadlock_timeout: crate::core::mvcc::row_locks::DEADLOCK_TIMEOUT,
            poll: Duration::from_millis(10),
        }
    }
}

/// What the fast path decided.
enum Fast {
    Granted,
    /// Take the main path; `fast` is what the owner holds on the table in
    /// its shard.
    Main { fast: ModeSet },
}

/// What the main path decided.
enum Decided {
    /// The owner already held the mode.
    Already,
    Granted,
    Queued(Wait),
}

/// The table-lock manager.
pub struct TableLocks {
    main: Mutex<Main>,
    shards: Box<[Mutex<Shard>]>,
    strong_pending: Box<[AtomicU32]>,
    /// How many owners hold or await anything on the main path (`owned` plus
    /// `waiting`), published under the main mutex after every change. Zero is
    /// the common case (only weak modes, all on the fast path), and lets
    /// [`TableLocks::release_owner`] skip the main mutex, the one lock every
    /// statement would otherwise take at its end. An owner's own main entries
    /// are made by its own requests, or by a migration that holds the owner's
    /// shard lock while it publishes; the release takes that shard lock
    /// first, so it always sees a count that includes them.
    main_busy: AtomicUsize,
    /// Whether an owner's transaction can still be holding (see
    /// [`TableLocks::set_liveness`]); unset, every owner is live.
    liveness: std::sync::OnceLock<Box<dyn Fn(TxId) -> bool + Send + Sync>>,
    config: TableLockConfig,
}

impl Default for TableLocks {
    fn default() -> Self {
        TableLocks::with_config(TableLockConfig::default())
    }
}

fn shard_of(owner: LockOwner) -> usize {
    // Fibonacci hashing: consecutive ids spread over every shard.
    (owner.0.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - FAST_PATH_SHARDS.trailing_zeros())) as usize
}

fn slot_of(table: &str) -> usize {
    (crate::core::hash::xxh64(table.as_bytes(), 0) % STRONG_SLOTS as u64) as usize
}

impl TableLocks {
    pub fn new() -> TableLocks {
        TableLocks::default()
    }

    pub fn with_config(config: TableLockConfig) -> TableLocks {
        TableLocks {
            main: Mutex::new(Main::default()),
            shards: (0..FAST_PATH_SHARDS).map(|_| Mutex::new(Shard::default())).collect(),
            strong_pending: (0..STRONG_SLOTS).map(|_| AtomicU32::new(0)).collect(),
            main_busy: AtomicUsize::new(0),
            liveness: std::sync::OnceLock::new(),
            config,
        }
    }

    pub fn config(&self) -> TableLockConfig {
        self.config
    }

    /// Installs the test of whether an owner can still be holding: `false`
    /// for an owner whose transaction has aborted. Every engine path releases
    /// its owner explicitly (a statement's scope, a block's `OwnerRelease`,
    /// replication's and the importer's owners), so this is the safety net
    /// for one that never did: a holder found dead by a request it blocks is
    /// released then, as the row locks prune theirs, instead of wedging the
    /// table for the life of the process. Committed owners are never pruned:
    /// a block's locks must outlive its commit until its outcome is durable
    /// (finding L4), and only its release says when that is.
    pub fn set_liveness(&self, live: impl Fn(TxId) -> bool + Send + Sync + 'static) {
        let _ = self.liveness.set(Box::new(live));
    }

    fn is_dead(&self, owner: LockOwner) -> bool {
        self.liveness.get().is_some_and(|live| !live(owner.id()))
    }

    /// Releases, on the main path, every holder of `key` that is dead (see
    /// [`TableLocks::set_liveness`]) and that `owner`'s request for `mode`
    /// conflicts with. True if any was.
    fn prune_dead(&self, main: &mut Main, key: &Arc<str>, owner: LockOwner, mode: TableMode) -> bool {
        if self.liveness.get().is_none() {
            return false;
        }
        let dead: SmallVec<[LockOwner; 2]> = match main.tables.get(key) {
            Some(state) => state
                .holders
                .iter()
                .filter(|(o, set)| *o != owner && set.conflicts_with(mode) && self.is_dead(*o))
                .map(|(o, _)| *o)
                .collect(),
            None => return false,
        };
        for o in &dead {
            self.release_main(main, *o);
        }
        !dead.is_empty()
    }

    fn main(&self) -> MutexGuard<'_, Main> {
        self.main.lock().expect("table lock map poisoned")
    }

    /// Publishes [`TableLocks::main_busy`]; called under the main mutex by
    /// everything that changes `owned` or `waiting`.
    fn publish(&self, main: &Main) {
        self.main_busy
            .store(main.owned.len() + main.waiting.len(), Ordering::SeqCst);
    }

    fn shard(&self, index: usize) -> MutexGuard<'_, Shard> {
        self.shards[index].lock().expect("table lock shard poisoned")
    }

    fn strong(&self, table: &str) -> &AtomicU32 {
        &self.strong_pending[slot_of(table)]
    }

    /// One request: granted now, or queued behind what it conflicts with.
    ///
    /// The caller waits on a queued request (see [`TableLocks::acquire`]) and
    /// must settle it: wait for the grant, or [`TableLocks::abandon`] it.
    pub fn request(&self, owner: LockOwner, table: &str, mode: TableMode, role: WaitRole) -> Request {
        if !mode.is_strong() {
            return match self.fast_try(owner, table, mode) {
                Fast::Granted => Request::Granted,
                Fast::Main { fast } => match self.decide(owner, table, mode, role, fast) {
                    Decided::Already | Decided::Granted => Request::Granted,
                    Decided::Queued(wait) => Request::Queued(wait),
                },
            };
        }
        self.strong_begin(table);
        for shard in 0..FAST_PATH_SHARDS {
            self.migrate_shard(shard, table);
        }
        self.strong_finish(owner, table, mode, role)
    }

    /// The weak request's fast path: under the owner's shard lock, already
    /// held, or recorded there while no strong request is held or pending on
    /// the table.
    fn fast_try(&self, owner: LockOwner, table: &str, mode: TableMode) -> Fast {
        let mut shard = self.shard(shard_of(owner));
        let held = shard.held_on(owner, table);
        if held.contains(mode) {
            return Fast::Granted;
        }
        // Read under the shard lock: a strong request raises the counter
        // before it takes this lock to migrate, so either it finds the entry
        // recorded below or this read sees it.
        if self.strong(table).load(Ordering::SeqCst) == 0 {
            shard.record(owner, table, mode);
            return Fast::Granted;
        }
        Fast::Main { fast: held }
    }

    /// A strong request's first step: weak requests on the table take the
    /// main path from here on.
    fn strong_begin(&self, table: &str) {
        self.strong(table).fetch_add(1, Ordering::SeqCst);
    }

    /// Moves `shard`'s entries for `table` into the main table. The shard
    /// lock is held until they are there, so no entry is ever in neither.
    fn migrate_shard(&self, shard: usize, table: &str) {
        let mut guard = self.shard(shard);
        let moved = guard.take_table(table);
        if moved.is_empty() {
            return;
        }
        let mut main = self.main();
        let Main { tables, owned, .. } = &mut *main;
        let key = key_for(tables, table);
        let state = tables.entry(key.clone()).or_default();
        for (owner, set) in moved {
            for mode in set.iter() {
                if state.add(owner, mode) {
                    owned.entry(owner).or_default().push(key.clone());
                }
            }
        }
        self.publish(&main);
    }

    /// A strong request's main-path step, after every shard migrated.
    fn strong_finish(&self, owner: LockOwner, table: &str, mode: TableMode, role: WaitRole) -> Request {
        match self.decide(owner, table, mode, role, ModeSet::EMPTY) {
            Decided::Already => {
                // The held mode already counts once.
                self.strong(table).fetch_sub(1, Ordering::SeqCst);
                Request::Granted
            }
            Decided::Granted => Request::Granted,
            Decided::Queued(wait) => Request::Queued(wait),
        }
    }

    /// The main path: the grant rule against the table's holders and queue.
    fn decide(&self, owner: LockOwner, table: &str, mode: TableMode, role: WaitRole, fast: ModeSet) -> Decided {
        let mut main = self.main();
        let Main { tables, waiting, .. } = &mut *main;
        debug_assert!(!waiting.contains_key(&owner), "{owner:?} requested a lock while it waits for another");
        let key = key_for(tables, table);
        let state = tables.entry(key.clone()).or_default();
        let mine = state.held_by(owner);
        if mine.contains(mode) {
            return Decided::Already;
        }
        if state.holder_conflict(owner, mode) && self.prune_dead(&mut main, &key, owner, mode) {
            // A dead holder was in the way and is gone; decide again.
            drop(main);
            return self.decide(owner, table, mode, role, fast);
        }
        let Main { tables, owned, waiting } = &mut *main;
        let state = tables.entry(key.clone()).or_default();
        if state.grantable(owner, mode, mine.union(fast), state.waiters.len()) {
            if state.add(owner, mode) {
                owned.entry(owner).or_default().push(key);
            }
            self.publish(&main);
            return Decided::Granted;
        }
        let slot = Arc::new(WaitSlot::new());
        state.waiters.push_back(Waiter {
            owner,
            mode,
            role,
            fast,
            slot: slot.clone(),
        });
        waiting.insert(owner, key.clone());
        self.publish(&main);
        Decided::Queued(Wait {
            owner,
            table: key,
            mode,
            slot,
        })
    }

    /// Grants `mode` now or not at all, queueing nothing: the try half of a
    /// reader's try-or-restart for a table found late (§7 Acquisition).
    pub fn try_acquire(&self, owner: LockOwner, table: &str, mode: TableMode) -> bool {
        match self.request(owner, table, mode, WaitRole::AutocommitReader) {
            Request::Granted => true,
            Request::Queued(wait) => {
                // Granted in between is still granted.
                self.abandon(&wait) == WaitOutcome::Granted
            }
        }
    }

    /// Gives up a queued request. Returns how it stood: `Waiting` if it was
    /// still queued (now removed, holding nothing new), otherwise the outcome
    /// it reached first, `Granted` included, which the caller then holds.
    pub fn abandon(&self, wait: &Wait) -> WaitOutcome {
        let mut main = self.main();
        let outcome = wait.slot.outcome();
        if outcome != WaitOutcome::Waiting {
            return outcome;
        }
        let Main { tables, owned, waiting } = &mut *main;
        self.unqueue(tables, owned, waiting, wait.owner, ABANDONED);
        self.publish(&main);
        WaitOutcome::Waiting
    }

    /// Removes `owner`'s queued request, settles it as `state`, lowers the
    /// strong counter it raised and grants what it was holding up.
    fn unqueue(
        &self,
        tables: &mut TableMap<TableState>,
        owned: &mut OwnerMap<SmallVec<[Arc<str>; 4]>>,
        waiting: &mut OwnerMap<Arc<str>>,
        owner: LockOwner,
        state: u8,
    ) {
        let Some(key) = waiting.remove(&owner) else {
            return;
        };
        if let Some(table) = tables.get_mut(&key)
            && let Some(pos) = table.waiters.iter().position(|w| w.owner == owner)
        {
            let w = table.waiters.remove(pos).expect("position is in range");
            if w.mode.is_strong() {
                self.strong(&key).fetch_sub(1, Ordering::SeqCst);
            }
            w.slot.settle(state);
        }
        wake(tables, owned, waiting, &key);
    }

    /// Drops one mode `owner` holds on `table`, keeping the rest of its set.
    /// True if it held it.
    pub fn release_mode(&self, owner: LockOwner, table: &str, mode: TableMode) -> bool {
        let mut found = false;
        if !mode.is_strong() {
            // A weak mode can sit in the shard and, recorded again after a
            // migration, in the main table too: release both.
            found = self.shard(shard_of(owner)).remove(owner, table, mode);
        }
        let mut main = self.main();
        let Main { tables, owned, waiting } = &mut *main;
        let Some(key) = tables.get_key_value(table).map(|(k, _)| k.clone()) else {
            return found;
        };
        let state = tables.get_mut(&key).expect("key was just found");
        let Some(pos) = state.holders.iter().position(|(o, _)| *o == owner) else {
            return found;
        };
        if !state.holders[pos].1.contains(mode) {
            return found;
        }
        state.holders[pos].1.remove(mode);
        if state.holders[pos].1.is_empty() {
            state.holders.remove(pos);
            forget_owned(owned, owner, &key);
        }
        if mode.is_strong() {
            self.strong(&key).fetch_sub(1, Ordering::SeqCst);
        }
        wake(tables, owned, waiting, &key);
        self.publish(&main);
        true
    }

    /// Swaps `from` for `to` in `owner`'s set on `table` with no instant at
    /// which it holds neither: a block's DDL ending AE while keeping RX
    /// (finding L5). False, changing nothing, if it does not hold `from`.
    pub fn downgrade(&self, owner: LockOwner, table: &str, from: TableMode, to: TableMode) -> bool {
        let mut main = self.main();
        let Main { tables, owned, waiting } = &mut *main;
        let Some(key) = tables.get_key_value(table).map(|(k, _)| k.clone()) else {
            return false;
        };
        let state = tables.get_mut(&key).expect("key was just found");
        let Some(pos) = state.holders.iter().position(|(o, _)| *o == owner) else {
            return false;
        };
        let set = &mut state.holders[pos].1;
        if !set.contains(from) || from == to {
            return false;
        }
        if to.is_strong() && !set.contains(to) {
            // Held under `from`, which conflicts with every weak holder that
            // could still be on the fast path, so none is.
            self.strong(&key).fetch_add(1, Ordering::SeqCst);
        }
        set.insert(to);
        set.remove(from);
        if from.is_strong() {
            self.strong(&key).fetch_sub(1, Ordering::SeqCst);
        }
        wake(tables, owned, waiting, &key);
        self.publish(&main);
        true
    }

    /// Releases everything `owner` holds or waits for, on every table.
    pub fn release_owner(&self, owner: LockOwner) {
        // The shard first, then the main table, never both at once: a
        // migration holds a shard lock while it takes the main one. An entry
        // migrated before the shard lock is taken is in `holders` below.
        {
            let mut shard = self.shard(shard_of(owner));
            if !shard.held.is_empty() {
                shard.held.remove(&owner);
            }
        }
        // Nobody holds or awaits anything on the main path, so neither does
        // this owner (see `main_busy`): the common statement end.
        if self.main_busy.load(Ordering::SeqCst) == 0 {
            return;
        }
        let mut main = self.main();
        if !main.owned.contains_key(&owner) && !main.waiting.contains_key(&owner) {
            return;
        }
        self.release_main(&mut main, owner);
    }

    /// The main path's half of [`TableLocks::release_owner`]: `owner`'s
    /// queued request and every main-path holding, with what they held up
    /// granted.
    fn release_main(&self, main: &mut Main, owner: LockOwner) {
        let Main { tables, owned, waiting } = main;
        self.unqueue(tables, owned, waiting, owner, ABANDONED);
        for key in owned.remove(&owner).unwrap_or_default() {
            if let Some(state) = tables.get_mut(&key)
                && let Some(pos) = state.holders.iter().position(|(o, _)| *o == owner)
            {
                let (_, set) = state.holders.remove(pos);
                let strong = set.strong_count();
                if strong > 0 {
                    self.strong(&key).fetch_sub(strong, Ordering::SeqCst);
                }
            }
            wake(tables, owned, waiting, &key);
        }
        self.publish(main);
    }

    /// The modes `owner` holds on `table`, fast path and main.
    pub fn holds(&self, owner: LockOwner, table: &str) -> ModeSet {
        let fast = self.shard(shard_of(owner)).held_on(owner, table);
        let main = self.main();
        fast.union(main.tables.get(table).map_or(ModeSet::EMPTY, |t| t.held_by(owner)))
    }

    /// The tables `owner` holds ACCESS EXCLUSIVE on, in no order. A strong
    /// mode is only ever on the main path, so the shards need no look.
    pub fn exclusive_tables(&self, owner: LockOwner) -> Vec<String> {
        let main = self.main();
        let Some(keys) = main.owned.get(&owner) else {
            return Vec::new();
        };
        keys.iter()
            .filter(|key| {
                main.tables
                    .get(&**key)
                    .is_some_and(|t| t.held_by(owner).contains(TableMode::AccessExclusive))
            })
            .map(|key| key.to_string())
            .collect()
    }

    /// Who `owner`'s queued request waits for: the other owners holding a
    /// conflicting mode and the conflicting requests ahead of its place
    /// (rules (a) and (b)). Empty when it is not waiting.
    pub fn blockers(&self, owner: LockOwner) -> SmallVec<[LockOwner; 4]> {
        edges(&self.main(), owner)
    }

    /// Looks for a wait-for cycle through `owner`'s queued request. On one,
    /// fails the wait of the member [`choose_victim`] picks (it settles as
    /// [`WaitOutcome::Deadlock`] and is woken), grants what that frees, and
    /// returns the victim. `None` when `owner` is not waiting or is on no
    /// cycle.
    pub fn deadlock_check(&self, owner: LockOwner) -> Option<LockOwner> {
        self.deadlock_check_with(owner, &HashMap::new())
    }

    /// [`TableLocks::deadlock_check`] over one graph with the row and
    /// referenced-key waits (`RowLocks`'s edges, keyed by owner id): an owner
    /// waiting here has this table's edges, any other owner `outside`'s. A
    /// waiter outside this manager is weighed as an `Other` member waiting
    /// for no table mode. The victim is [`choose_victim`]'s pick over the
    /// whole cycle; if it waits here its wait fails and it is returned, and
    /// if it waits outside, `None` is returned and the row side, which reads
    /// the same graph and applies the same rule, fails it.
    pub fn deadlock_check_with(
        &self,
        owner: LockOwner,
        outside: &HashMap<TxId, SmallVec<[TxId; 4]>>,
    ) -> Option<LockOwner> {
        let mut main = self.main();
        let cycle = find_cycle(&main, owner, outside)?;
        let members: SmallVec<[CycleMember; 8]> = cycle
            .iter()
            .map(|o| match waiter_of(&main, *o) {
                Some(w) => CycleMember {
                    owner: *o,
                    mode: Some(w.mode),
                    role: w.role,
                },
                None => CycleMember {
                    owner: *o,
                    mode: None,
                    role: WaitRole::Other,
                },
            })
            .collect();
        let victim = choose_victim(&members);
        debug_assert!(victim.is_some(), "a cycle of autocommit readers only: {members:?}");
        let victim = victim?;
        if !main.waiting.contains_key(&victim) {
            return None;
        }
        let Main { tables, owned, waiting } = &mut *main;
        self.unqueue(tables, owned, waiting, victim, DEADLOCK);
        self.publish(&main);
        Some(victim)
    }

    /// The mode and role of `owner`'s queued request here, if it has one.
    pub fn waiting_for(&self, owner: LockOwner) -> Option<(TableMode, WaitRole)> {
        let main = self.main();
        waiter_of(&main, owner).map(|w| (w.mode, w.role))
    }

    /// `owner`'s queued request, if it has one: its wait-for edges with its
    /// mode and role, read under one lock, so a cycle weighed from them
    /// cannot see the edges of a wait and then find it gone (its own check
    /// failing it in between) and misjudge it as a row waiter.
    pub fn wait_edges(&self, owner: LockOwner) -> Option<(SmallVec<[LockOwner; 4]>, TableMode, WaitRole)> {
        let main = self.main();
        let waiter = waiter_of(&main, owner)?;
        let (mode, role) = (waiter.mode, waiter.role);
        Some((edges(&main, owner), mode, role))
    }

    /// Sets the role of `owner`'s queued request, if it has one: the caller
    /// learnt after queueing who the victim rule should take it for (an
    /// autocommit statement's reader-ness, finding L8, is asked only of a
    /// request that queued, since answering costs a walk).
    pub fn set_role(&self, owner: LockOwner, role: WaitRole) {
        let mut main = self.main();
        let Some(key) = main.waiting.get(&owner).cloned() else {
            return;
        };
        if let Some(waiter) = main
            .tables
            .get_mut(&key)
            .and_then(|state| state.waiters.iter_mut().find(|w| w.owner == owner))
        {
            waiter.role = role;
        }
    }

    /// Whether `owner` holds or awaits anything here, on either path.
    pub fn holds_any(&self, owner: LockOwner) -> bool {
        if self.shard(shard_of(owner)).held.contains_key(&owner) {
            return true;
        }
        let main = self.main();
        main.owned.contains_key(&owner) || main.waiting.contains_key(&owner)
    }

    /// Another owner holding a mode on `table` that conflicts with `mode`,
    /// read from the main path alone: exact for a weak `mode` (AS and RX
    /// conflict only with S and AE, which never sit on the fast path), and
    /// for a strong one right after a request for it on this table (which
    /// migrated the fast path). Diagnostic and for tests.
    pub fn conflicting_holder(&self, owner: LockOwner, table: &str, mode: TableMode) -> Option<LockOwner> {
        let main = self.main();
        main.tables.get(table).and_then(|state| {
            state
                .holders
                .iter()
                .find(|(o, set)| *o != owner && set.conflicts_with(mode))
                .map(|(o, _)| *o)
        })
    }

    /// Requests `mode` on `table` and waits for it: through the grant, a
    /// deadlock check on every poll once the wait has lasted
    /// `deadlock_timeout`, `timeout`, and `cancelled` polled each round. On an
    /// error the request is gone and nothing new is held.
    pub async fn acquire(
        &self,
        owner: LockOwner,
        table: &str,
        mode: TableMode,
        role: WaitRole,
        timeout: LockTimeout,
        cancelled: &(dyn Fn() -> bool + Sync),
    ) -> Result<(), LockError> {
        self.acquire_with(owner, table, mode, role, timeout, cancelled, &|o| self.deadlock_check(o))
            .await
    }

    /// [`TableLocks::acquire`] with the deadlock check supplied by the caller
    /// (`RowLocks`'s, which sees the row and referenced-key waits too).
    #[allow(clippy::too_many_arguments)]
    pub async fn acquire_with(
        &self,
        owner: LockOwner,
        table: &str,
        mode: TableMode,
        role: WaitRole,
        timeout: LockTimeout,
        cancelled: &(dyn Fn() -> bool + Sync),
        deadlock_check: &(dyn Fn(LockOwner) -> Option<LockOwner> + Sync),
    ) -> Result<(), LockError> {
        let wait = match self.request(owner, table, mode, role) {
            Request::Granted => return Ok(()),
            Request::Queued(wait) => wait,
        };
        self.wait_for(&wait, timeout, cancelled, deadlock_check).await
    }

    /// Waits for an already queued request (see [`TableLocks::acquire_with`]).
    pub async fn wait_for(
        &self,
        wait: &Wait,
        timeout: LockTimeout,
        cancelled: &(dyn Fn() -> bool + Sync),
        deadlock_check: &(dyn Fn(LockOwner) -> Option<LockOwner> + Sync),
    ) -> Result<(), LockError> {
        // A wait whose future is dropped (its task cancelled) gives up its
        // place in the queue rather than leave a request nobody will settle
        // in front of every later one (rule (a)).
        struct Settle<'a> {
            locks: &'a TableLocks,
            wait: &'a Wait,
        }
        impl Drop for Settle<'_> {
            fn drop(&mut self) {
                if self.wait.outcome() == WaitOutcome::Waiting {
                    self.locks.abandon(self.wait);
                }
            }
        }
        let _settle = Settle { locks: self, wait };
        let owner = wait.owner;
        let start = Instant::now();
        loop {
            // A holder whose transaction aborted without releasing is
            // released now (see `set_liveness`), which may grant this wait.
            self.prune_for(wait);
            match wait.outcome() {
                WaitOutcome::Granted => return Ok(()),
                WaitOutcome::Deadlock => return Err(LockError::Deadlock),
                WaitOutcome::Abandoned => return Err(LockError::Cancelled),
                WaitOutcome::Waiting => {}
            }
            if cancelled() {
                return self.give_up(wait, LockError::Cancelled);
            }
            let waited = start.elapsed();
            if let Some(limit) = timeout.limit()
                && waited >= limit
            {
                return self.give_up(wait, LockError::Timeout);
            }
            if waited >= self.config.deadlock_timeout && deadlock_check(owner).is_some() {
                // Some member's wait failed; if it was this one, the outcome
                // says so at the top.
                continue;
            }
            let mut nap = self.config.poll;
            if let Some(limit) = timeout.limit() {
                nap = nap.min(limit.saturating_sub(waited));
            }
            if waited < self.config.deadlock_timeout {
                nap = nap.min(self.config.deadlock_timeout - waited);
            }
            let _ = tokio::time::timeout(nap, wait.slot.notify.notified()).await;
        }
    }

    /// [`TableLocks::prune_dead`] for a queued request's table.
    fn prune_for(&self, wait: &Wait) {
        if self.liveness.get().is_none() || wait.outcome() != WaitOutcome::Waiting {
            return;
        }
        let mut main = self.main();
        if self.prune_dead(&mut main, &wait.table, wait.owner, wait.mode) {
            let Main { tables, owned, waiting } = &mut *main;
            wake(tables, owned, waiting, &wait.table);
            self.publish(&main);
        }
    }

    /// Ends a wait with `error` unless it was settled first.
    fn give_up(&self, wait: &Wait, error: LockError) -> Result<(), LockError> {
        match self.abandon(wait) {
            WaitOutcome::Granted => Ok(()),
            WaitOutcome::Deadlock => Err(LockError::Deadlock),
            WaitOutcome::Waiting | WaitOutcome::Abandoned => Err(error),
        }
    }
}

/// The map's own `Arc` for `table`, or a new one.
fn key_for(tables: &TableMap<TableState>, table: &str) -> Arc<str> {
    tables.get_key_value(table).map_or_else(|| Arc::from(table), |(k, _)| k.clone())
}

fn forget_owned(owned: &mut OwnerMap<SmallVec<[Arc<str>; 4]>>, owner: LockOwner, key: &Arc<str>) {
    if let Some(list) = owned.get_mut(&owner) {
        list.retain(|k| k != key);
        if list.is_empty() {
            owned.remove(&owner);
        }
    }
}

/// Grants, in queue order, every waiter on `key` the grant rule now admits,
/// and drops the table's state once nothing holds or awaits it.
///
/// One pass suffices: a grant adds a holder, which cannot make an earlier,
/// ungranted waiter grantable, and removes a waiter, which only later
/// waiters (judged after it) can have been queued behind.
fn wake(
    tables: &mut TableMap<TableState>,
    owned: &mut OwnerMap<SmallVec<[Arc<str>; 4]>>,
    waiting: &mut OwnerMap<Arc<str>>,
    key: &Arc<str>,
) {
    let Some(state) = tables.get_mut(key) else {
        return;
    };
    let mut i = 0;
    while i < state.waiters.len() {
        let w = &state.waiters[i];
        let held = w.fast.union(state.held_by(w.owner));
        if state.grantable(w.owner, w.mode, held, i) {
            let w = state.waiters.remove(i).expect("index is in range");
            if state.add(w.owner, w.mode) {
                owned.entry(w.owner).or_default().push(key.clone());
            }
            waiting.remove(&w.owner);
            w.slot.settle(GRANTED);
        } else {
            i += 1;
        }
    }
    if state.is_idle() {
        tables.remove(key);
    }
}

/// `owner`'s wait-for edges, from the main table alone: a queued weak request
/// waits only on S/AE holders and waiters, which are always there; a queued
/// strong one migrated its table's fast-path entries before queueing; and
/// every waiter, weak or strong, is in the main table's queue.
fn edges(main: &Main, owner: LockOwner) -> SmallVec<[LockOwner; 4]> {
    let mut out: SmallVec<[LockOwner; 4]> = SmallVec::new();
    let Some(key) = main.waiting.get(&owner) else {
        return out;
    };
    let Some(state) = main.tables.get(key) else {
        return out;
    };
    let Some(pos) = state.waiters.iter().position(|w| w.owner == owner) else {
        return out;
    };
    let w = &state.waiters[pos];
    let held = w.fast.union(state.held_by(owner));
    for (o, set) in &state.holders {
        if *o != owner && set.conflicts_with(w.mode) && !out.contains(o) {
            out.push(*o);
        }
    }
    for ahead in state.waiters_ahead(owner, w.mode, held, pos) {
        if !out.contains(&ahead.owner) {
            out.push(ahead.owner);
        }
    }
    out
}

/// `owner`'s queued request, if it waits on the main path.
fn waiter_of(main: &Main, owner: LockOwner) -> Option<&Waiter> {
    let key = main.waiting.get(&owner)?;
    main.tables.get(key)?.waiters.iter().find(|w| w.owner == owner)
}

/// A cycle of the wait-for graph through `start`, as its members, or `None`.
/// An owner waiting here has its table edges; any other has `outside`'s
/// (the row and referenced-key waits), which is how a cycle through both
/// kinds of wait is one cycle.
fn find_cycle(
    main: &Main,
    start: LockOwner,
    outside: &HashMap<TxId, SmallVec<[TxId; 4]>>,
) -> Option<SmallVec<[LockOwner; 8]>> {
    if !main.waiting.contains_key(&start) {
        return None;
    }
    let edges_of = |o: LockOwner| -> Option<SmallVec<[LockOwner; 4]>> {
        if main.waiting.contains_key(&o) {
            return Some(edges(main, o));
        }
        outside
            .get(&o.id())
            .map(|e| e.iter().filter_map(|id| LockOwner::new(*id)).collect())
    };
    // Depth-first from `start`, looking for a path back to it.
    let mut path: Vec<(LockOwner, SmallVec<[LockOwner; 4]>, usize)> = vec![(start, edges(main, start), 0)];
    let mut visited = std::collections::HashSet::new();
    visited.insert(start);
    while let Some((_, out, next)) = path.last_mut() {
        if *next >= out.len() {
            path.pop();
            continue;
        }
        let to = out[*next];
        *next += 1;
        if to == start {
            return Some(path.iter().map(|(n, ..)| *n).collect());
        }
        if !visited.insert(to) {
            continue;
        }
        // An owner that is waiting nowhere has no edges.
        let Some(e) = edges_of(to) else {
            continue;
        };
        path.push((to, e, 0));
    }
    None
}

#[cfg(test)]
impl TableLocks {
    /// Fast-path entries across every shard.
    fn fast_path_len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap().held.values().map(|l| l.len()).sum::<usize>())
            .sum()
    }

    fn strong_pending_for(&self, table: &str) -> u32 {
        self.strong(table).load(Ordering::SeqCst)
    }

    fn queued_on(&self, table: &str) -> usize {
        self.main().tables.get(table).map_or(0, |t| t.waiters.len())
    }

    fn main_is_empty(&self) -> bool {
        let main = self.main();
        main.tables.is_empty() && main.owned.is_empty() && main.waiting.is_empty()
    }
}

#[cfg(test)]
#[path = "table_locks_tests.rs"]
mod tests;
