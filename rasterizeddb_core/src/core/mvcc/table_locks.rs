//! Table locks: the one owner-keyed lock manager that statements and DDL go
//! through (docs/persistent-indexes.md §7, W3 option C).
//!
//! # Not wired yet
//!
//! This is slice B1: the lock state, the grant rule, the fast path, the
//! wait-for graph and the wait loop, unit-tested and used by nothing. B2 gives
//! every statement one [`LockOwner`], B3b routes every table lookup through
//! `table_for(name, owner, mode)`, which lands here, and `RowLocks` delegates
//! its table space (`table_blockers`, `set_waiting`) to it so table, row and
//! referenced-key waits share one graph.
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
//! and either
//! - (a) no conflicting S or AE request from another owner waits ahead of it,
//!   or
//! - (b) the requester already holds some mode on the table.
//!
//! Rule (a) is what stops a stream of readers starving a waiting ALTER; rule
//! (b) is what keeps a block that already holds the table from queueing
//! behind an ALTER that is itself waiting for that block (R1: `BEGIN; INSERT`,
//! a waiting `CREATE INDEX`, and the block's next statement and COMMIT go
//! through). An owner's own modes never conflict, so an upgrade (RX to AE)
//! waits only for other owners. A waiting AS or RX delays nobody: a waiter
//! that holds nothing is on no cycle.
//!
//! This is narrower than PostgreSQL in two directions, both deliberate (§7):
//! PostgreSQL also queues a request behind a conflicting weak waiter, and
//! lets a holder jump only the waiters its held modes conflict with, where (b)
//! lets any holder go ahead.
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
//! other owners holding a conflicting mode plus, unless rule (b) lets it pass,
//! the conflicting S/AE waiters ahead of it. After
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
//! AE, so every cycle through a reader contains an S or AE requester.
//!
//! # lock_timeout
//!
//! [`LockTimeout::from_setting_ms`] reads PostgreSQL's `lock_timeout`: `0`,
//! the default, means no bound at all (decision 12), never "fail at once".
//! Past a positive bound the wait fails with 55P03; cancellation ends it with
//! 57014.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use smallvec::SmallVec;

use crate::core::mvcc::TxId;
use crate::core::mvcc::transaction::{FROZEN_TX, NO_TX};

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
    /// The owner held a fast-path mode on the table when it queued. Its
    /// fast-path entries cannot leave while it waits (only it releases them,
    /// and a migration moves them into `holders`), so this stays true.
    holds_fast: bool,
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

    /// Whether a conflicting S or AE request of another owner waits among the
    /// first `ahead` waiters: rule (a).
    fn strong_waits_ahead(&self, owner: LockOwner, mode: TableMode, ahead: usize) -> bool {
        self.waiters
            .iter()
            .take(ahead)
            .any(|w| w.owner != owner && w.mode.is_strong() && w.mode.conflicts_with(mode))
    }

    /// The grant rule: no conflicting holder, and (a) or (b).
    fn grantable(&self, owner: LockOwner, mode: TableMode, holds_any: bool, ahead: usize) -> bool {
        !self.holder_conflict(owner, mode) && (holds_any || !self.strong_waits_ahead(owner, mode, ahead))
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
    tables: HashMap<Arc<str>, TableState>,
    /// The tables each owner holds a main-path mode on, so releasing an owner
    /// costs its own locks, not a walk of every table.
    owned: HashMap<LockOwner, SmallVec<[Arc<str>; 4]>>,
    /// The table each queued owner waits on. An owner waits for one lock at a
    /// time: its statement is one task.
    waiting: HashMap<LockOwner, Arc<str>>,
}

/// One fast-path shard: weak holdings of the owners hashed to it.
#[derive(Default)]
struct Shard {
    held: HashMap<LockOwner, SmallVec<[(Arc<str>, ModeSet); 4]>>,
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
    /// Take the main path; `holds_fast` if the owner holds another weak mode
    /// on the table in its shard.
    Main { holds_fast: bool },
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
            config,
        }
    }

    pub fn config(&self) -> TableLockConfig {
        self.config
    }

    fn main(&self) -> MutexGuard<'_, Main> {
        self.main.lock().expect("table lock map poisoned")
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
                Fast::Main { holds_fast } => match self.decide(owner, table, mode, role, holds_fast) {
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
        Fast::Main {
            holds_fast: !held.is_empty(),
        }
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
    }

    /// A strong request's main-path step, after every shard migrated.
    fn strong_finish(&self, owner: LockOwner, table: &str, mode: TableMode, role: WaitRole) -> Request {
        match self.decide(owner, table, mode, role, false) {
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
    fn decide(&self, owner: LockOwner, table: &str, mode: TableMode, role: WaitRole, holds_fast: bool) -> Decided {
        let mut main = self.main();
        let Main { tables, owned, waiting } = &mut *main;
        debug_assert!(!waiting.contains_key(&owner), "{owner:?} requested a lock while it waits for another");
        let key = key_for(tables, table);
        let state = tables.entry(key.clone()).or_default();
        let mine = state.held_by(owner);
        if mine.contains(mode) {
            return Decided::Already;
        }
        let holds_any = holds_fast || !mine.is_empty();
        if state.grantable(owner, mode, holds_any, state.waiters.len()) {
            if state.add(owner, mode) {
                owned.entry(owner).or_default().push(key);
            }
            return Decided::Granted;
        }
        let slot = Arc::new(WaitSlot::new());
        state.waiters.push_back(Waiter {
            owner,
            mode,
            role,
            holds_fast,
            slot: slot.clone(),
        });
        waiting.insert(owner, key.clone());
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
        WaitOutcome::Waiting
    }

    /// Removes `owner`'s queued request, settles it as `state`, lowers the
    /// strong counter it raised and grants what it was holding up.
    fn unqueue(
        &self,
        tables: &mut HashMap<Arc<str>, TableState>,
        owned: &mut HashMap<LockOwner, SmallVec<[Arc<str>; 4]>>,
        waiting: &mut HashMap<LockOwner, Arc<str>>,
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
        true
    }

    /// Releases everything `owner` holds or waits for, on every table.
    pub fn release_owner(&self, owner: LockOwner) {
        // The shard first, then the main table, never both at once: a
        // migration holds a shard lock while it takes the main one. An entry
        // migrated before the shard lock is taken is in `holders` below.
        self.shard(shard_of(owner)).held.remove(&owner);
        let mut main = self.main();
        let Main { tables, owned, waiting } = &mut *main;
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
    }

    /// The modes `owner` holds on `table`, fast path and main.
    pub fn holds(&self, owner: LockOwner, table: &str) -> ModeSet {
        let fast = self.shard(shard_of(owner)).held_on(owner, table);
        let main = self.main();
        fast.union(main.tables.get(table).map_or(ModeSet::EMPTY, |t| t.held_by(owner)))
    }

    /// Who `owner`'s queued request waits for: the other owners holding a
    /// conflicting mode and, unless rule (b) lets it pass, the conflicting
    /// S/AE requests ahead of it. Empty when it is not waiting.
    pub fn blockers(&self, owner: LockOwner) -> SmallVec<[LockOwner; 4]> {
        edges(&self.main(), owner)
    }

    /// Looks for a wait-for cycle through `owner`'s queued request. On one,
    /// fails the wait of the member [`choose_victim`] picks (it settles as
    /// [`WaitOutcome::Deadlock`] and is woken), grants what that frees, and
    /// returns the victim. `None` when `owner` is not waiting or is on no
    /// cycle.
    pub fn deadlock_check(&self, owner: LockOwner) -> Option<LockOwner> {
        let mut main = self.main();
        let cycle = find_cycle(&main, owner)?;
        let members: SmallVec<[CycleMember; 8]> = cycle
            .iter()
            .map(|o| {
                let key = &main.waiting[o];
                let w = main.tables[key]
                    .waiters
                    .iter()
                    .find(|w| w.owner == *o)
                    .expect("a waiting owner is queued on its table");
                CycleMember {
                    owner: *o,
                    mode: Some(w.mode),
                    role: w.role,
                }
            })
            .collect();
        let victim = choose_victim(&members);
        debug_assert!(victim.is_some(), "a cycle of autocommit readers only: {members:?}");
        let victim = victim?;
        let Main { tables, owned, waiting } = &mut *main;
        self.unqueue(tables, owned, waiting, victim, DEADLOCK);
        Some(victim)
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
        let wait = match self.request(owner, table, mode, role) {
            Request::Granted => return Ok(()),
            Request::Queued(wait) => wait,
        };
        let start = Instant::now();
        loop {
            match wait.outcome() {
                WaitOutcome::Granted => return Ok(()),
                WaitOutcome::Deadlock => return Err(LockError::Deadlock),
                WaitOutcome::Abandoned => return Err(LockError::Cancelled),
                WaitOutcome::Waiting => {}
            }
            if cancelled() {
                return self.give_up(&wait, LockError::Cancelled);
            }
            let waited = start.elapsed();
            if let Some(limit) = timeout.limit()
                && waited >= limit
            {
                return self.give_up(&wait, LockError::Timeout);
            }
            if waited >= self.config.deadlock_timeout && self.deadlock_check(owner).is_some() {
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
fn key_for(tables: &HashMap<Arc<str>, TableState>, table: &str) -> Arc<str> {
    tables.get_key_value(table).map_or_else(|| Arc::from(table), |(k, _)| k.clone())
}

fn forget_owned(owned: &mut HashMap<LockOwner, SmallVec<[Arc<str>; 4]>>, owner: LockOwner, key: &Arc<str>) {
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
/// One pass suffices: a grant only adds a holder, which cannot make an
/// earlier, ungranted waiter grantable, and later waiters are judged after it.
fn wake(
    tables: &mut HashMap<Arc<str>, TableState>,
    owned: &mut HashMap<LockOwner, SmallVec<[Arc<str>; 4]>>,
    waiting: &mut HashMap<LockOwner, Arc<str>>,
    key: &Arc<str>,
) {
    let Some(state) = tables.get_mut(key) else {
        return;
    };
    let mut i = 0;
    while i < state.waiters.len() {
        let w = &state.waiters[i];
        let holds_any = w.holds_fast || !state.held_by(w.owner).is_empty();
        if state.grantable(w.owner, w.mode, holds_any, i) {
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
/// waits only on S/AE holders and waiters, which are always there, and a
/// queued strong one migrated its table's fast-path entries before queueing.
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
    let holds_any = w.holds_fast || !state.held_by(owner).is_empty();
    for (o, set) in &state.holders {
        if *o != owner && set.conflicts_with(w.mode) && !out.contains(o) {
            out.push(*o);
        }
    }
    if !holds_any {
        for ahead in state.waiters.iter().take(pos) {
            if ahead.owner != owner
                && ahead.mode.is_strong()
                && ahead.mode.conflicts_with(w.mode)
                && !out.contains(&ahead.owner)
            {
                out.push(ahead.owner);
            }
        }
    }
    out
}

/// A cycle of the wait-for graph through `start`, as its members, or `None`.
fn find_cycle(main: &Main, start: LockOwner) -> Option<SmallVec<[LockOwner; 8]>> {
    if !main.waiting.contains_key(&start) {
        return None;
    }
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
        // An owner that is not waiting has no edges.
        if !main.waiting.contains_key(&to) || !visited.insert(to) {
            continue;
        }
        let e = edges(main, to);
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
