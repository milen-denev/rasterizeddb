//! The logical partitioned table (§1.1, §3, §4.3, §4.4): its map, one slot per
//! partition, lazy materialization, routing, and the shared row-id source.
//! SQL `INSERT` routes through it (S9), and `SELECT` and `COUNT` read its
//! partitions under one snapshot (S10, `rql::row_source`); every other
//! statement still refuses.
//!
//! # What it holds
//!
//! - the **shell** (`Table::partition_shell`): the schema, and nothing that
//!   reads or writes rows;
//! - the **map epoch**, read with `ArcSwap::load_full` only (§4.4): a plain
//!   `load` guard is invisible to the strong count a reshape's reclamation
//!   will wait on;
//! - one **slot** per partition (phase 3 P5b): a `materialized` flag and,
//!   while the partition is open, its `Table`. A clear flag is proof the
//!   partition has no rows (§4.3): the flag is set before the first row is
//!   written, so a reader whose snapshot sees a committed row, and which
//!   reads the flag after taking that snapshot, finds it set. An empty open
//!   cell proves nothing: the partition may be closed;
//! - the **map log** (§3.2), which replays to the state the slots mirror;
//! - the **row-id source** every partition draws from (§3.5, I12).
//!
//! # Materializing (§3.2)
//!
//! A partition's first write, under its lifecycle lock (`PinBoard::lifecycle`)
//! so one runs per pid: delete any leftover files and create the three files
//! exclusively (one directory fsync); append `MATERIALIZE` with its WAL record
//! issued at append time, in the root log the first row's outcome will follow,
//! and sync that log (one fsync per partition, ever: otherwise a crash could
//! keep the first rows' pages without the record, which the open refuses as
//! unexplained data under a torn map); set the `materialized` flag; open the
//! partition and register it for versioning; put it in its slot. Leftover
//! files are never reused, and a file with no `MATERIALIZE` is never read.
//! While the map log is torn, a non-empty leftover is refused instead, at
//! open and at materialize: the lost tail may have been its `MATERIALIZE`.
//!
//! # Opening
//!
//! Opens no partition (phase 3 P5b): the map is replayed, the slots built,
//! and each materialized partition's two data files checked to exist. A
//! partition opens on demand (`Parts::open`) and closes only exclusively
//! (`Parts::close`); see "Phase 3 P5b-1 as built" in the design. What
//! closes it (P5b-2a): the open budget's CLOCK when an open needs room
//! (`partition::budget`, `Parts::close_for_budget`), idle close on the
//! board round (`Parts::close_idle_round`), a forget sweep's own close, and
//! callers' explicit closes; every close seals the file set first.
//!
//! Never panics. Anything wrong (a missing map, ids or partition file, a map
//! this phase cannot open, a key the schema no longer has) leaves the table
//! **broken**: the shell still answers schema questions, and every data call
//! returns the reason. Missing files are checked by a never-creating open
//! before any provider is built on them (I9), since providers create.
//!
//! # Phase 2a limits, each refused rather than half-served
//!
//! Every partition lives in the root location (index 0) and the map is the
//! one `CREATE` placed: pids `0..n`, no reshape. A log saying otherwise is a
//! later phase's and leaves the table broken. Location 0 is read as "the
//! directory the map log is in", not as the path recorded at `CREATE`, so a
//! copied database directory opens its own files rather than the original's.
//! A pool with backup mirrors is refused at create, open and materialize,
//! read from the pool each time, since a mirror added at runtime never
//! reaches the root seed.

// `Database` builds one (S4); part of the API is for later slices' callers.
#![cfg_attr(not(test), allow(dead_code))]

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc as StdArc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use arc_swap::ArcSwap;
use rclite::Arc;
use smallvec::SmallVec;

use super::files::{self, MapLogFile};
use super::budget::{self, Budget};
use super::guard::RangeGuard;
use super::maintenance::{MaintenanceMode, Scheduler};
use super::pins::{BoardFiles, PinBoard};
use super::serial::SerialSource;
use super::key::KeySpec;
use super::log::{MapHeader, ROUTE_V1, Record};
use super::map::{PointRange, RangeMap, SPACE_END};
use super::route::{Route, route};
use crate::core::filter::sarg;
use crate::core::mvcc::transaction::TxId;
use crate::core::mvcc::{TransactionManager, Visibility};
use crate::core::row::row::RowWrite;
use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
use crate::core::row::table::{IdSource, PartitionOpen, Table};
use crate::core::storage_providers::backend::DbStorageProvider;
use crate::core::storage_providers::traits::StorageIO;

/// Partitions a `PARTITION BY HASH` table gets when it names no count: 64.
/// Phase 2a chose it while every materialized partition stayed open; the
/// plan was 1024 once phase 3's lazy open landed, but P5b-3's measurement
/// (`lazy_resident_tests::default_partition_count_cost`, docs/
/// partitioning.md "Phase 3 P5b-3 as built") kept 64: at 2,000 tenants
/// 1024 partitions made unrouted reads 10 to 20 times slower, and 250
/// times under an open budget of 64, for point reads no faster.
pub const DEFAULT_PARTITIONS: u32 = 64;

/// The most partitions one database may declare across all its tables
/// (§7.1): 2²⁰, the most one map lays out (`map::MAX_INITIAL`), from plan
/// item 4's measurement (`cap_scale_tests`, docs/partitioning.md "The cap"):
/// a declared partition costs about 630 bytes resident whether or not it is
/// ever written, and a first write no longer depends on how many are
/// declared. 65,536 from phase 3 P5b-3; phase 2's 2,048 stood while every
/// materialized partition opened at startup. Per database, not per process
/// or node: a refusal must depend only on replicated state (§12).
pub const MAX_DECLARED_PER_DATABASE: u32 = 1_048_576;

/// The only location index phase 2a writes or opens: the root.
const ROOT_LOC: u32 = 0;

/// Refuses declaring `adding` more partitions in a database that already
/// declares `declared`. "not supported" makes it 0A000.
pub fn check_declared_cap(declared: u64, adding: u32) -> io::Result<()> {
    check_declared_cap_at(declared, adding, MAX_DECLARED_PER_DATABASE)
}

/// [`check_declared_cap`] against `cap`.
pub(crate) fn check_declared_cap_at(declared: u64, adding: u32, cap: u32) -> io::Result<()> {
    let total = declared.saturating_add(adding as u64);
    if total > cap as u64 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "declaring {adding} more partitions would make {total} in this database; more than \
                 {cap} partitions per database is not supported yet"
            ),
        ));
    }
    Ok(())
}

/// One map epoch: what a statement holds, counted, from before its first
/// partition call to after its last (§4.4).
#[derive(Debug)]
pub struct MapEpoch {
    pub epoch: u64,
    pub map: RangeMap,
}

/// One partition's place in the table.
struct Slot {
    pid: u32,
    range: PointRange,
    /// Set once the partition is materialized: from the replayed map at
    /// open, or by [`Parts::materialize`] once its `MATERIALIZE` is durable
    /// and before its first row. Never cleared. A clear flag is proof the
    /// partition holds no row (§4.3): a reader whose snapshot sees a
    /// committed row, and which reads the flag after taking that snapshot,
    /// finds it set.
    materialized: AtomicBool,
    /// Everything else, built on the partition's first use: a declared
    /// partition nobody has written, routed to or opened holds only the
    /// three fields above. A table of 2^20 declared partitions held each
    /// one's open instance, budget token, kept sizes and directory slots
    /// before a row existed (docs/partitioning.md, "What a declared
    /// partition still costs"). Never replaced once built, so every
    /// reference to it stays the partition's.
    live: OnceLock<Box<SlotLive>>,
}

/// A [`Slot`]'s state past its range and flag, built by [`Slot::live`] on
/// first use; [`Slot::peek`] reads it without building it, and a partition
/// without one is closed, unreferenced, never routed, keeps no sizes and
/// lives in the root.
#[derive(Default)]
struct SlotLive {
    /// The open instance; `None` while the partition is closed or
    /// unmaterialized (phase 3 P5b: partitions open on demand). Every access
    /// clones it under this lock; it is put and taken only while the
    /// partition's lifecycle lock is held too (`PinBoard::lifecycle`), so a
    /// close that finds the strong count at 1 here holds the only reference
    /// anyone can reach.
    open: parking_lot::Mutex<Option<Arc<Table<DbStorageProvider>>>>,
    /// The open instance's place in the open budget's CLOCK
    /// (`partition::budget`), put and dropped with the instance, so the
    /// budget's open count follows the instance out of the slot by whatever
    /// path it leaves (closed, or this table instance dropped).
    token: parking_lot::Mutex<Option<budget::Token>>,
    /// The CLOCK's reference bit: set by a routed access, cleared by the
    /// hand and by the idle round as they pass (P5b-2).
    referenced: AtomicBool,
    /// Set by the first routed access or write in this process and never
    /// cleared, across closes: the evidence a fan-out's scan-only open needs
    /// before it loads a proven snapshot at once (`Parts::open_instance`,
    /// P5b-3). A partition only fan-outs reach stays scan-only, its load
    /// deferred to a routed access that may never come. In memory: a start
    /// begins with none.
    routed: AtomicBool,
    /// A closed partition's (rows, pointers, index-store) file lengths, for
    /// the catalogs only (never a DDL's population check, which reads the
    /// files: `PartitionedTable::rows_len_exact`). See [`KeptSizes`].
    sizes: parking_lot::Mutex<KeptSizes>,
    /// The directory its files are in, once it is materialized somewhere
    /// other than the root (phase 4, §6.1): set from the replayed map at
    /// open or by [`Parts::materialize`], before the flag. Unset means the
    /// root. Never changes: a move is a reshape, to a new pid.
    dir: OnceLock<PathBuf>,
}

/// A closed partition's kept file lengths (P5b-2a): what the catalogs read
/// instead of six `stat`s per closed partition per read.
///
/// - **Cleared when the partition opens** (`Slot::put`): the open instance
///   answers for itself, and what it writes makes the old lengths wrong.
/// - **Cleared when a close takes the instance out**, and marked `closing`
///   until the close stores the final lengths (after its seal and landing,
///   under the lifecycle lock, before the lock is released): a reader in
///   between reads the files and keeps nothing, since the files are still
///   changing (review A and B: a close window answered with the previous
///   close's lengths, and an `ALTER` decided by them rewrote no rows).
/// - **Filled from the files' metadata** by a reader only when no open or
///   close began or ended while it read (`epoch` unchanged), so a read that
///   raced a close never overwrites the close's final lengths.
#[derive(Default)]
struct KeptSizes {
    kept: Option<(u64, u64, u64)>,
    closing: bool,
    epoch: u64,
}

impl KeptSizes {
    /// Forgets the kept lengths; `closing` says whether a close is under way.
    fn reset(&mut self, closing: bool) {
        self.kept = None;
        self.closing = closing;
        self.epoch += 1;
    }
}

impl Slot {
    fn new(pid: u32, range: PointRange, materialized: bool, dir: Option<PathBuf>) -> Self {
        let slot = Slot { pid, range, materialized: AtomicBool::new(materialized), live: OnceLock::new() };
        if let Some(dir) = dir {
            slot.set_dir(dir);
        }
        slot
    }

    /// The slot's state, built now if this is its first use.
    fn live(&self) -> &SlotLive {
        self.live.get_or_init(Box::default)
    }

    /// The slot's state if it was ever built.
    fn peek(&self) -> Option<&SlotLive> {
        self.live.get().map(|live| &**live)
    }

    fn open_now(&self) -> Option<Arc<Table<DbStorageProvider>>> {
        self.peek()?.open.lock().clone()
    }

    fn is_open(&self) -> bool {
        self.peek().is_some_and(|live| live.open.lock().is_some())
    }

    /// The directory its files are in: its own, or `root`.
    fn dir<'a>(&'a self, root: &'a Path) -> &'a Path {
        self.dir_get().map_or(root, PathBuf::as_path)
    }

    /// Its own directory, unset for the root.
    fn dir_get(&self) -> Option<&PathBuf> {
        self.peek()?.dir.get()
    }

    /// Sets its own directory, once; a later call is ignored, as the
    /// directory never changes (a move is a reshape, to a new pid).
    fn set_dir(&self, dir: PathBuf) {
        let _ = self.live().dir.set(dir);
    }

    fn referenced_now(&self) -> bool {
        self.peek().is_some_and(|live| live.referenced.load(Ordering::Relaxed))
    }

    /// The reference bit, cleared: the CLOCK hand's and the idle round's
    /// pass.
    fn take_referenced(&self) -> bool {
        self.peek().is_some_and(|live| live.referenced.swap(false, Ordering::Relaxed))
    }

    fn routed_now(&self) -> bool {
        self.peek().is_some_and(|live| live.routed.load(Ordering::Relaxed))
    }

    /// A routed access: the reference bit set, with a store only when it
    /// was clear, so a hot partition's accesses write nothing shared.
    fn touch(&self) {
        let live = self.live();
        if !live.referenced.load(Ordering::Relaxed) {
            live.referenced.store(true, Ordering::Relaxed);
        }
        if !live.routed.load(Ordering::Relaxed) {
            live.routed.store(true, Ordering::Relaxed);
        }
    }

    /// Puts an opened instance in the slot with its budget token. The
    /// caller holds the lifecycle lock.
    fn put(&self, table: Arc<Table<DbStorageProvider>>, token: budget::Token, referenced: bool) {
        let live = self.live();
        live.referenced.store(referenced, Ordering::Relaxed);
        if referenced {
            live.routed.store(true, Ordering::Relaxed);
        }
        *live.token.lock() = Some(token);
        live.sizes.lock().reset(false);
        *live.open.lock() = Some(table);
    }

    /// The token, given back once the instance has left the slot.
    fn release_token(&self) {
        if let Some(live) = self.peek() {
            drop(live.token.lock().take());
        }
    }

    /// This closed partition's (rows, pointers, index-store) lengths, for a
    /// catalog: kept ones, else from its files' metadata, kept from then on
    /// only if no open or close began or ended meanwhile and none is under
    /// way ([`KeptSizes`]).
    fn closed_sizes(&self, root: &Path, uid: u64) -> io::Result<(u64, u64, u64)> {
        let epoch = {
            let sizes = self.live().sizes.lock();
            if let Some(kept) = sizes.kept {
                return Ok(kept);
            }
            (!sizes.closing).then_some(sizes.epoch)
        };
        let read = closed_lens(root, uid, self.pid)?;
        #[cfg(test)]
        kept_sizes_hooks::read(root, self.pid);
        if let Some(epoch) = epoch {
            let mut sizes = self.live().sizes.lock();
            if sizes.epoch == epoch && !sizes.closing && sizes.kept.is_none() {
                sizes.kept = Some(read);
            }
        }
        Ok(read)
    }
}

/// What [`PartitionedTable::close_partition`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Close {
    /// Closed: its maintenance stopped, its staged appends landed, its
    /// providers dropped.
    Closed,
    /// Not open (closed already, or never materialized).
    NotOpen,
    /// Held by a statement or a job (its strong count above 1), or a
    /// transaction keeps a ticket on its write gate: left open.
    InUse,
    /// The table was replaced by a schema change or dropped: this instance
    /// closes nothing.
    Retired,
}

/// Running totals over the materialized partitions, plus the parent files.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Totals {
    /// Every file of the table: the partitions' three each, and the parent
    /// files (schema, map log, ids) once.
    pub bytes: u64,
    /// The partitions' rows files.
    pub data: u64,
    /// The partitions' pointer files.
    pub pointers: u64,
    /// The partitions' filter checkpoints.
    pub filters: u64,
    /// The schema file, read through the shell's provider.
    pub schema: u64,
    /// The map log.
    pub map: u64,
    /// The row-id reservation every partition shares, counted once.
    pub ids: u64,
    /// Pointer records, live and dead.
    pub slots: u64,
    pub ids_handed_out: u64,
    pub materialized: u32,
    pub declared: u32,
    /// The partitions' measured live rows, each clamped to its own slots
    /// (`VacuumStats::live_rows`), summed. `None` unless every materialized
    /// partition is open and has been swept: a sum that skipped one would
    /// read as that partition's rows all being gone.
    pub swept_live: Option<u64>,
    /// Rows the open partitions' `DELETE`s removed since each opened.
    pub deleted_since_open: u64,
}

/// One partition, as introspection shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionInfo {
    pub pid: u32,
    pub lo: u64,
    pub hi: u128,
    /// The directory its files are in; `None` until it is materialized.
    pub location: Option<String>,
    /// `"materialized"` or `"unmaterialized"`.
    pub state: &'static str,
    pub slots: u64,
    pub bytes: u64,
}

/// Whether the database's pool has backup mirrors configured, read at each
/// call: a mirror added while the server runs leaves the root seed a plain
/// file provider, so the seed's kind cannot answer it.
pub(crate) type MirrorsConfigured = StdArc<dyn Fn() -> bool + Send + Sync>;

/// Where a partition's files go (phase 4, docs/partitioning.md §6.1): the
/// database's storage pool, behind a trait so this module builds without
/// `storage_pools`, where every partition stays in the root ([`RootOnly`]).
pub(crate) trait Placement: Send + Sync {
    /// The directory the next partition of `uid` to materialize goes to,
    /// counted there at once (the pool's dealer); `None` for the root.
    fn deal(&self, uid: u64) -> Option<String>;
    /// Takes back a [`Placement::deal`] whose materialization failed.
    fn undeal(&self, uid: u64, dir: &str);
    /// Replaces what is counted for `uid`: its materialized partitions per
    /// directory, the root's included, as the table opens.
    fn register(&self, uid: u64, counts: std::collections::HashMap<String, u64>);
    /// Where a partition recorded at `dir`, not the root, opens now: `dir`
    /// while it is a location of this database's pool, or where a restore
    /// moved it; `None` when neither.
    fn resolve(&self, dir: &str) -> Option<String>;
    /// Every directory other than the root a partition may be in.
    fn others(&self) -> Vec<PathBuf>;
}

/// Every partition in the root.
pub(crate) struct RootOnly;

impl Placement for RootOnly {
    fn deal(&self, _uid: u64) -> Option<String> {
        None
    }
    fn undeal(&self, _uid: u64, _dir: &str) {}
    fn register(&self, _uid: u64, _counts: std::collections::HashMap<String, u64>) {}
    fn resolve(&self, _dir: &str) -> Option<String> {
        None
    }
    fn others(&self) -> Vec<PathBuf> {
        Vec::new()
    }
}

/// What [`PartitionedTable::open`] needs from the database.
pub(crate) struct OpenCtx<'a> {
    /// The database's root provider, a plain file provider: partition
    /// providers are created beside it.
    pub seed: &'a Arc<DbStorageProvider>,
    pub db_id: u64,
    pub transactions: &'a StdArc<TransactionManager>,
    /// The pool's mirrors, not the seed's: see [`MirrorsConfigured`].
    pub mirrored: MirrorsConfigured,
    /// The database's one maintenance scheduler (§7.3), which every
    /// partition, opened `External`, is serviced by.
    pub scheduler: &'a StdArc<Scheduler>,
    /// Where partitions materialize (phase 4).
    pub placement: StdArc<dyn Placement>,
}

/// Everything a table that opened has; a broken one has none of it.
struct Live {
    spec: KeySpec,
    /// The key layout over the whole space: points of rows, for routing.
    router: RangeGuard,
    epoch: ArcSwap<MapEpoch>,
    /// The slots and what opening a partition needs, shared weakly with the
    /// board, whose forget rounds open a closed, pinned partition to sweep
    /// it (§7.2).
    parts: StdArc<Parts>,
    scheduler: StdArc<Scheduler>,
}

/// One table instance's slots, and what opening, materializing and closing
/// a partition needs (phase 3 P5b, §7.2). A partition opens on its first
/// routed access, fan-out read, write or forget sweep after the table opens
/// ([`Parts::open`]), and closes only exclusively ([`Parts::close`]).
///
/// **One live instance per file set** (I14): every open, materialize and
/// close of partition `p` runs under `p`'s lifecycle lock, which lives in
/// the board and so is shared by every instance a schema change reopens the
/// table as; a close holds it until the old instance's providers are idle
/// and its staged appends landed, so no new instance opens over files an old
/// one still writes.
pub(crate) struct Parts {
    name: String,
    uid: u64,
    db_id: u64,
    root: PathBuf,
    transactions: StdArc<TransactionManager>,
    shell: Arc<Table<DbStorageProvider>>,
    spec: KeySpec,
    seed: Arc<DbStorageProvider>,
    ids: StdArc<IdSource<DbStorageProvider>>,
    /// The map log, its replayed state, the forget pins and the table's one
    /// forget handle (§7.2, `partition::pins`), the `SERIAL` reservations and
    /// the partitions' lifecycle locks: shared with the instance a schema
    /// change reopens this one as ([`PartitionedTable::reopened`]), so one
    /// map log has one writer at a time and the pins outlive the reopen.
    board: StdArc<PinBoard>,
    /// One per live partition, in map order (ascending `lo`).
    slots: Box<[Slot]>,
    /// By pid: the index of that pid's slot in `slots`, or `u32::MAX` for a
    /// pid with none (retired by a reshape, or gone). Pids are dense below
    /// the map's `next_pid`, its entries only until a reshape (phase 5).
    index: Box<[u32]>,
    mirrored: MirrorsConfigured,
    torn: Option<String>,
    /// Set when a schema change replaced this instance or a DROP removed the
    /// table: nothing opens, materializes or closes through it from then on.
    retired: AtomicBool,
    /// `SET CONFIG vacuum` for this table (0: not set, 1: on, 2: off),
    /// applied to every partition as it opens, so a partition closed and
    /// opened again keeps it.
    vacuum: AtomicU8,
    /// Opens and closes so far, for tests and the measurements.
    opens: AtomicU64,
    closes: AtomicU64,
    /// The open budget this table's partitions count against (P5b-2).
    budget: StdArc<Budget>,
    /// This instance, for the budget's CLOCK entries.
    me: std::sync::Weak<Parts>,
    /// When the last idle round ran (`Parts::close_idle_round`).
    idle_round_at: parking_lot::Mutex<Option<std::time::Instant>>,
    /// Where partitions materialize (phase 4, §6.1).
    placement: StdArc<dyn Placement>,
}

pub struct PartitionedTable {
    name: String,
    uid: u64,
    root: PathBuf,
    shell: Arc<Table<DbStorageProvider>>,
    db_id: u64,
    transactions: StdArc<TransactionManager>,
    /// `Err` holds why the table is broken.
    live: Result<Live, String>,
    /// Partitions the map declares; `None` when the log did not replay, so
    /// the count is unknown rather than 0.
    declared: Option<u32>,
    /// Set when the map log had a torn tail cut, or still has its evidence
    /// file: reaping must stay off (§12), and a partition's leftover files
    /// prove nothing (see [`refuse_torn_leftovers`]).
    torn: Option<String>,
    mirrored: MirrorsConfigured,
    dropped: AtomicBool,
    /// A broken table's place in the forget registry, held at the horizon
    /// already forgotten when it was refused, for as long as it stays
    /// broken: its partitions' pins are unknown, and any of them may hold an
    /// aborted stamp that forgetting past would bring back (§7.2). The
    /// status log then grows until the table opens or is dropped.
    /// Held, never read: dropping it lets forgetting go.
    #[allow(dead_code)]
    broken_pin: Option<BrokenPin>,
    /// The database's maintenance scheduler, for the instance a schema
    /// change reopens this one as.
    scheduler: StdArc<Scheduler>,
}

/// A broken table's hold on forgetting (see `broken_pin`): its registry
/// handle, and the token the scheduler names it by in the log and in `SHOW
/// DATABASE STATS` (`forgetting_held_by`) for as long as it lives.
struct BrokenPin {
    _handle: crate::core::mvcc::transaction::ForgetHandle,
    _token: StdArc<()>,
}

impl BrokenPin {
    fn new(name: &str, transactions: &TransactionManager, scheduler: &StdArc<Scheduler>) -> Self {
        let token = StdArc::new(());
        scheduler.register_broken(name, StdArc::downgrade(&token));
        ::log::warn!(
            "partitioned table \"{name}\" cannot be opened: it holds the database's forgotten horizon where it is \
             until it opens or is dropped"
        );
        BrokenPin { _handle: transactions.register_table(), _token: token }
    }
}

fn invalid(e: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

/// The directory a file provider is rooted at. Phase 2a partitions only
/// plain files: an in-memory database has no directory to create them in,
/// and a mirrored pool would need the partition files mirrored too.
fn file_root(seed: &DbStorageProvider) -> io::Result<PathBuf> {
    match (seed.kind(), seed.get_location()) {
        ("file", Some(location)) => Ok(PathBuf::from(location)),
        (kind, _) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("partitioned tables on {kind} storage are not supported yet"),
        )),
    }
}

/// Refuses while the pool has backup mirrors: partition files would be
/// written to the root only, and the mirror would silently lack their rows.
fn refuse_mirrored(mirrored: bool) -> io::Result<()> {
    if mirrored {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "partitioned tables in a storage pool with backup mirrors are not supported yet",
        ));
    }
    Ok(())
}

/// With the map log torn, an unmaterialized partition's non-empty file is
/// not a leftover: the lost tail may have been its `MATERIALIZE`, damaged on
/// the media after its rows committed. Deleting it (materialize) or skipping
/// it as rowless (§4.3) would lose those rows silently, so it is refused
/// until an operator has looked. An empty file is a crash between creating
/// the files and appending the record, and is recreated as usual.
fn refuse_torn_leftovers(root: &Path, uid: u64, generation: u64, pid: u32, torn: &str) -> io::Result<()> {
    let mut found = Vec::new();
    for name in files::data_files(uid, pid) {
        match std::fs::metadata(root.join(&name)) {
            Ok(m) if m.len() > 0 => found.push(name),
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(io::Error::new(e.kind(), format!("checking partition file {name}: {e}")));
            }
        }
    }
    if found.is_empty() {
        return Ok(());
    }
    Err(invalid(format!(
        "the map log was torn ({torn}), and partition {pid} has files with data but no MATERIALIZE ({}): \
         they may hold committed rows whose record was lost; examine them and the torn tail in \
         {}{}, and remove that file once resolved",
        found.join(", "),
        files::map_file(uid, generation),
        files::TORN,
    )))
}

/// The directory of a partition recorded at interned location `loc`:
/// `None` for the root (location 0, wherever the database now is), else
/// the interned path, which must be a location of this database's pool. A
/// map copied or restored from elsewhere names another installation's
/// directory, whose files are not this database's to read or write, so the
/// table is refused rather than opened over them.
fn partition_dir(locations: &[String], loc: u32, placement: &dyn Placement) -> Result<Option<PathBuf>, String> {
    if loc == ROOT_LOC {
        return Ok(None);
    }
    let Some(path) = locations.get(loc as usize) else {
        return Err(format!("is at location {loc}, but only {} are interned", locations.len()));
    };
    match placement.resolve(path) {
        Some(now) => Ok(Some(PathBuf::from(now))),
        None => Err(format!(
            "is in {path}, which is not a storage location of this database; add it back with \
             STORAGE ADD LOCATION '{path}', or restore the partition's files there"
        )),
    }
}

/// The slots of `state`'s live partitions, in map order, and the pid index
/// over them (`Parts::index`): each materialized one with the directory its
/// location is in now, checked to hold its two data files (I9; a stat per
/// file, never a provider, which would create it). Also the dealer's count
/// per directory (§6.1).
fn slots_of(
    state: &super::log::MapState,
    root: &Path,
    uid: u64,
    placement: &dyn Placement,
) -> Result<(Box<[Slot]>, Box<[u32]>, std::collections::HashMap<String, u64>), String> {
    let entries = state.map().entries();
    let n = usize::try_from(state.next_pid()).map_err(|e| e.to_string())?;
    let mut index = vec![u32::MAX; n];
    let mut counts: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    let root_key = files::dir_key(&root.to_string_lossy());
    let mut slots = Vec::with_capacity(entries.len());
    for (j, &(lo, pid)) in entries.iter().enumerate() {
        let range = PointRange { lo, hi: entries.get(j + 1).map_or(SPACE_END, |&(next, _)| next as u128) };
        let part = state.part(pid).ok_or_else(|| format!("live partition {pid} has no state"))?;
        let (materialized, dir) = match part.location {
            None => (false, None),
            Some(loc) => {
                let dir = partition_dir(state.locations(), loc, placement).map_err(|why| format!("partition {pid} {why}"))?;
                files::require_existing(dir.as_deref().unwrap_or(root), &files::data_files(uid, pid))
                    .map_err(|e| e.to_string())?;
                let key = dir.as_ref().map_or_else(|| root_key.clone(), |d| files::dir_key(&d.to_string_lossy()));
                *counts.entry(key).or_insert(0) += 1;
                (true, dir)
            }
        };
        *index
            .get_mut(pid as usize)
            .ok_or_else(|| format!("live partition {pid} is past the map's next pid {n}"))? = j as u32;
        slots.push(Slot::new(pid, range, materialized, dir));
    }
    Ok((slots.into_boxed_slice(), index.into_boxed_slice(), counts))
}

/// The partitions a database's partitioned tables declare, for
/// [`check_declared_cap`]. Refused while any table's count is unknown (its
/// map log did not replay): the cap must depend only on replicated state,
/// and counting such a table as 0 would let this node accept what a healthy
/// replica refuses.
pub(crate) fn declared_in<'a>(tables: impl IntoIterator<Item = &'a PartitionedTable>) -> io::Result<u64> {
    let mut total = 0u64;
    for t in tables {
        match t.declared() {
            Some(n) => total += n as u64,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "partitioned table \"{}\" cannot be opened, so this database's declared partitions \
                         are unknown; declaring more is not supported until it opens",
                        t.name
                    ),
                ));
            }
        }
    }
    Ok(total)
}

/// [`PartitionedTable::assignments_keep_point`] for key `spec` and a clause
/// read against `schema`: shared with the partition's own `UPDATE`, which
/// asks the same question of its range guard's key to decide whether the
/// in-place overwrite is open to it (`Table::update_rows_locked`). True only
/// when no key column is assigned, or each one assigned gets a constant that
/// a top-level conjunct `key = v` pins to the same level hash; an expression
/// on a key column, a clause the tokenizer cannot read, or anything else
/// unproven is false.
pub(crate) fn assignments_keep_point(
    spec: &KeySpec,
    schema: &crate::core::row::schema::TableSchema,
    where_clause: &str,
    assigned: &[(u32, &[u8])],
    computed: &[u32],
) -> bool {
    let levels = spec.levels();
    let touched: SmallVec<[usize; 3]> = (0..levels.len())
        .filter(|&i| {
            let wo = levels[i].write_order;
            computed.contains(&wo) || assigned.iter().any(|(w, _)| *w == wo)
        })
        .collect();
    if touched.is_empty() {
        return true;
    }
    if touched.iter().any(|&i| computed.contains(&levels[i].write_order)) {
        return false;
    }
    let Ok(tokens) = crate::core::tokenizer::query_tokenizer::try_tokenize(where_clause, &schema.fields) else {
        return false;
    };
    let keyed: SmallVec<[u32; 3]> = levels.iter().map(|l| l.write_order).collect();
    let Some(program) = sarg::extract_gated(&tokens, &|wo| keyed.contains(&wo), &decimal_scales(schema)) else {
        return false;
    };
    fn conjuncts<'a>(e: &'a sarg::MaskExpr, out: &mut Vec<&'a sarg::SargPredicate>) {
        match e {
            sarg::MaskExpr::Pred(p) => out.push(p),
            sarg::MaskExpr::And(children) => children.iter().for_each(|c| conjuncts(c, out)),
            // An `OR` pins nothing on its own, and `All` is no proof.
            _ => {}
        }
    }
    let mut pinned = Vec::new();
    conjuncts(&program.root, &mut pinned);
    touched.iter().all(|&i| {
        let wo = levels[i].write_order;
        let Some((_, value)) = assigned.iter().find(|(w, _)| *w == wo) else {
            return false;
        };
        let Ok(new) = spec.level_hash_of(i, value) else {
            return false;
        };
        pinned.iter().any(|p| {
            p.op == crate::core::filter::zonemap::SargOp::Eq
                && p.write_order == wo
                && spec.level_hash_of(i, &p.value).is_ok_and(|old| old == new)
        })
    })
}

/// Each `NUMERIC(p, s)` column's non-default scale, as `Table` passes the
/// extractor, so a clause is read exactly as the table's own scan reads it.
fn decimal_scales(schema: &crate::core::row::schema::TableSchema) -> SmallVec<[(u32, u32); 4]> {
    schema
        .fields
        .iter()
        .filter(|f| f.db_type == crate::core::db_type::DbType::DECIMAL)
        .filter_map(|f| {
            // A column that keeps each value's own scale, by its sentinel.
            if crate::core::value_codec::keeps_own_scale(f) {
                return Some((f.write_order as u32, crate::core::value_codec::OWN_SCALE_SENTINEL));
            }
            f.scale
                .filter(|s| *s != crate::core::value_codec::DECIMAL_SCALE)
                .map(|s| (f.write_order as u32, s))
        })
        .collect()
}

/// The key the map's header names, rebuilt from the shell's declared types.
/// Refused when a header column is gone or has changed family: routing on it
/// would put rows where no query looks.
fn spec_from_header(header: &MapHeader, schema: &crate::core::row::schema::TableSchema) -> Result<KeySpec, String> {
    let mut names = Vec::with_capacity(header.key.len());
    for (wo, _) in &header.key {
        let field = schema
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.write_order as u64 == *wo as u64)
            .ok_or_else(|| format!("partition key column {wo} is not in the table's schema"))?;
        names.push(field.name.as_str());
    }
    let spec = KeySpec::from_schema(schema, &names)?;
    let got: Vec<_> = spec.levels().iter().map(|l| (l.write_order, l.family)).collect();
    if got != header.key {
        return Err(format!(
            "the schema's partition key {got:?} disagrees with the map header's {:?}",
            header.key
        ));
    }
    Ok(spec)
}

impl PartitionedTable {
    /// Writes a new table's map log (`HEADER`, `LOCATIONS` with the root,
    /// `PLACE` of `RangeMap::initial(n)`) and its ids file, synced through the
    /// log's last record.
    ///
    /// The caller has already made the parent files with
    /// `files::create_parent_files` and written the schema into the first of
    /// them, since `spec`'s write orders come from that schema; it makes the
    /// schema durable. Both files written here must exist and be empty: a
    /// provider would create a missing one without the directory fsync, and
    /// a populated one belongs to someone else.
    pub(crate) async fn create_files(
        seed: &Arc<DbStorageProvider>,
        mirrored: bool,
        uid: u64,
        spec: &KeySpec,
        n: u32,
    ) -> io::Result<()> {
        refuse_mirrored(mirrored)?;
        let root = file_root(seed)?;
        let root_str = root
            .to_str()
            .ok_or_else(|| invalid(format!("the root {} is not UTF-8", root.display())))?
            .to_string();
        let map = RangeMap::initial(n).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let header = MapHeader {
            uid,
            route_v: ROUTE_V1,
            key: spec.levels().iter().map(|l| (l.write_order, l.family)).collect(),
            widths: spec.widths().iter().map(|&w| w as u8).collect(),
            colocation: 0,
        };
        header.validate().map_err(invalid)?;

        files::require_existing(&root, &[files::map_file(uid, 0), files::ids_file(uid)])?;
        let log = MapLogFile::create(StdArc::new(seed.create_new(files::map_file(uid, 0)).await)).await?;
        // One transaction, one sync, for all three.
        log.append_all_durable(&[
            Record::Header(header),
            Record::Locations(vec![root_str]),
            Record::Place { hi: SPACE_END, entries: map.entries().to_vec() },
        ])
        .await?;
        IdSource::create(Arc::new(seed.create_new(files::ids_file(uid)).await)).await?;
        Ok(())
    }

    /// Opens the table `uid`: replays its map log and builds its slots,
    /// opening no partition (each opens on demand), but checking that every
    /// materialized partition's data files exist (I9). A failure leaves it
    /// broken, never panics.
    pub(crate) async fn open(ctx: OpenCtx<'_>, name: &str, uid: u64, shell: Arc<Table<DbStorageProvider>>) -> Self {
        // Partitions reopen existing files: the forgotten horizon must not
        // move between one dropping out and its reopen registering.
        let _pause = ctx.transactions.forget_pause();
        let mut declared = None;
        let mut torn = None;
        let root = file_root(ctx.seed);
        let live = match (&root, refuse_mirrored((ctx.mirrored)())) {
            (Ok(root), Ok(())) => Self::open_live(&ctx, name, root, uid, &shell, &mut declared, &mut torn).await,
            (Err(e), _) => Err(e.to_string()),
            (_, Err(e)) => Err(e.to_string()),
        }
        .map_err(|e| {
            let why = format!("partitioned table \"{name}\" ({uid:016x}) cannot be opened: {e}");
            ::log::error!("{why}");
            why
        });
        let broken_pin = live.is_err().then(|| BrokenPin::new(name, ctx.transactions, ctx.scheduler));
        PartitionedTable {
            name: name.to_string(),
            uid,
            root: root.unwrap_or_default(),
            shell,
            db_id: ctx.db_id,
            transactions: ctx.transactions.clone(),
            live,
            declared,
            torn,
            mirrored: ctx.mirrored,
            dropped: AtomicBool::new(false),
            broken_pin,
            scheduler: ctx.scheduler.clone(),
        }
    }

    /// A table refused before its map is read, for `why` (its schema file is
    /// missing or unreadable, or the database cannot hold partitions). Every
    /// data call returns `why`, and the cap counts it as unknown. `root` is
    /// where its files are, so a DROP still deletes them; `shell` is whatever
    /// stands in for the schema, never put into `Database.tables`.
    pub(crate) fn refused(
        ctx: OpenCtx<'_>,
        name: &str,
        uid: u64,
        root: PathBuf,
        shell: Arc<Table<DbStorageProvider>>,
        why: &str,
    ) -> Self {
        let why = format!("partitioned table \"{name}\" ({uid:016x}) cannot be opened: {why}");
        ::log::error!("{why}");
        PartitionedTable {
            name: name.to_string(),
            uid,
            root,
            shell,
            db_id: ctx.db_id,
            transactions: ctx.transactions.clone(),
            live: Err(why),
            declared: None,
            torn: None,
            mirrored: ctx.mirrored,
            dropped: AtomicBool::new(false),
            broken_pin: Some(BrokenPin::new(name, ctx.transactions, ctx.scheduler)),
            scheduler: ctx.scheduler.clone(),
        }
    }

    async fn open_live(
        ctx: &OpenCtx<'_>,
        name: &str,
        root: &Path,
        uid: u64,
        shell: &Arc<Table<DbStorageProvider>>,
        declared: &mut Option<u32>,
        torn: &mut Option<String>,
    ) -> Result<Live, String> {
        if !shell.is_shell() {
            return Err("its shell is a table with rows of its own".to_string());
        }
        let e = |e: io::Error| e.to_string();
        // The live generation (§3.2): the highest a compaction sealed.
        let generation = files::live_generation(root, uid)?;
        files::require_existing(root, &[files::map_file(uid, generation)]).map_err(e)?;
        let map_io = StdArc::new(ctx.seed.create_new(files::map_file(uid, generation)).await);
        let (log, replay) = MapLogFile::open(map_io).await.map_err(e)?;
        *torn = replay.torn.as_ref().map(|t| t.reason.clone());
        let state = replay.state;
        *declared = Some(state.map().len() as u32);

        if state.header().uid != uid {
            return Err(format!("its map log belongs to table {:016x}", state.header().uid));
        }
        if state.dropped() {
            return Err("its map log says it was dropped".to_string());
        }
        if let Some(recorded) = state.locations().first()
            && Path::new(recorded) != root
        {
            ::log::warn!(
                "partitioned table {uid:016x} was created in {recorded}; opening its partitions from {}",
                root.display()
            );
        }
        let spec = spec_from_header(state.header(), &shell.schema)?;

        files::require_existing(root, &[files::ids_file(uid)]).map_err(e)?;
        let ids_io = Arc::new(ctx.seed.create_new(files::ids_file(uid)).await);
        let ids = StdArc::new(IdSource::open_strict(ids_io).await.map_err(e)?);
        // The `SERIAL` reservations (§10), created here for a table that
        // has none yet: one made before phase 3 P5a, or a new one.
        files::ensure_seq_file(root, uid).map_err(e)?;
        let seq_io = StdArc::new(ctx.seed.create_new(files::seq_file(uid)).await);
        let serials = SerialSource::open(ctx.db_id, seq_io).await.map_err(e)?;
        // No entry the defaults stopped reading survives an open: a crash
        // after a RENAME's commit and before its reopen pruned it.
        serials.retain_read(&super::serial::sequences_read(&shell.schema)).await.map_err(e)?;
        // The pins and the table's one forget handle, set from the replayed
        // log before any partition opens (§7.2's restart rule).
        let opened_len = log.io().get_len().await;
        let board = PinBoard::new(
            name,
            StdArc::new(log),
            state.clone(),
            ctx.transactions,
            ctx.scheduler,
            serials,
            BoardFiles { root: root.to_path_buf(), uid, seed: ctx.seed.clone(), torn: torn.is_some() },
        );
        // A compacted generation opens with its snapshot: the size the next
        // compaction is measured against (§3.2).
        if generation > 0 {
            board.set_snapshot_len(opened_len);
        }

        // A reshape a crash interrupted (§12): one with only its
        // `RESHAPE_BEGIN` rolls back (its targets hold nothing a snapshot
        // can need: the sources were never written past; the startup reaper
        // deleted their files, which no `MATERIALIZE` names), and a
        // committed one's retired sources go (their files are reaped: a
        // retired partition is never placed). Before any partition opens,
        // so nothing reads either.
        let state = board.finish_reshapes().await.map_err(e)?;
        for (pid, part) in state.parts() {
            if part.location.is_some() || part.retired {
                continue;
            }
            // §4.3's skip is a proof only while the log is whole, and the
            // leftovers may be on any location a crashed materialize was
            // dealt.
            if let Some(why) = torn.as_deref() {
                refuse_torn_leftovers(root, uid, generation, pid, why).map_err(e)?;
                for other in ctx.placement.others() {
                    refuse_torn_leftovers(&other, uid, generation, pid, why).map_err(e)?;
                }
            }
        }
        let map = state.map().clone();
        *declared = Some(map.len() as u32);
        // Nothing opens here (phase 3 P5b: a partition opens on demand), but
        // a materialized partition's missing file still refuses the table
        // now, as it did when every partition opened (I9).
        let (slots, index, counts) = slots_of(&state, root, uid, &*ctx.placement)?;
        // The dealer's counts (§6.1), every location's, the root's included.
        ctx.placement.register(uid, counts);
        let parts = StdArc::new_cyclic(|me| Parts {
            budget: budget::for_root(root),
            me: me.clone(),
            idle_round_at: parking_lot::Mutex::new(None),
            name: name.to_string(),
            uid,
            db_id: ctx.db_id,
            root: root.to_path_buf(),
            transactions: ctx.transactions.clone(),
            shell: shell.clone(),
            spec: spec.clone(),
            seed: ctx.seed.clone(),
            ids,
            board,
            slots,
            index,
            mirrored: ctx.mirrored.clone(),
            placement: ctx.placement.clone(),
            torn: torn.clone(),
            retired: AtomicBool::new(false),
            vacuum: AtomicU8::new(0),
            opens: AtomicU64::new(0),
            closes: AtomicU64::new(0),
        });
        parts.board.set_parts(StdArc::downgrade(&parts));
        Ok(Live {
            router: RangeGuard::new(spec.clone(), PointRange::FULL),
            spec,
            epoch: ArcSwap::from_pointee(MapEpoch { epoch: state.epoch(), map }),
            parts,
            scheduler: ctx.scheduler.clone(),
        })
    }

    /// §11's step 2 for a schema change, under ACCESS EXCLUSIVE on the
    /// logical name: this instance is retired (nothing opens, materializes or
    /// closes through it from here on), then every open partition is retired
    /// (its maintenance stopped and joined, its seals cancelled), so none
    /// writes through its providers once the new instances open over the same
    /// files. Each under its lifecycle lock, so a close in flight finishes
    /// first.
    pub(crate) async fn retire_partitions(&self) {
        if let Ok(live) = &self.live {
            live.parts.retired.store(true, Ordering::SeqCst);
            for slot in live.parts.slots.iter() {
                // A lock never built is held by nothing, and nothing opened
                // the partition through it.
                let Some(lock) = live.parts.board.lifecycle_if_built(slot.pid) else { continue };
                let _life = lock.lock().await;
                if let Some(table) = slot.open_now() {
                    table.retire().await;
                }
            }
        }
    }

    /// This table under a new schema (§11, "Phase 3 P1 as built"): the same
    /// uid, map log, map state and epoch, its row-id source reopened from its
    /// durable reservation (as at a start), with `shell` as its
    /// shell and every partition this one has open opened again over its
    /// files with `shell`'s schema (and so its indexes built from its rows),
    /// under `name`. The caller holds ACCESS EXCLUSIVE on the logical name
    /// and has retired this table's partitions; nothing materializes or
    /// writes meanwhile, so the shared map log has one writer.
    ///
    /// An unmaterialized partition stays so, and takes `shell`'s schema when
    /// it materializes. Never panics: a key the new schema no longer has, or
    /// a partition that does not open, leaves the new table broken with the
    /// reason, as a startup open would.
    pub(crate) async fn reopened(&self, name: &str, shell: Arc<Table<DbStorageProvider>>) -> PartitionedTable {
        // The map's partitions now: a reshape changes them (phase 5).
        let mut declared = self.declared;
        let live = match &self.live {
            Ok(live) => {
                let opened: Result<Live, String> = async {
                    if !shell.is_shell() {
                        return Err("its shell is a table with rows of its own".to_string());
                    }
                    let spec = spec_from_header(live.parts.board.state.lock().await.header(), &shell.schema)?;
                    // The SERIAL entries the new defaults no longer read go
                    // before anything can resume from them (a RENAME).
                    live.parts.board
                        .serials
                        .retain_read(&super::serial::sequences_read(&shell.schema))
                        .await
                        .map_err(|e| e.to_string())?;
                    // The row-id source reopened from its reservation, as a
                    // start (and a plain table's reload, over `_seq.db`) opens
                    // it: ids resume past every id the old one could have
                    // handed out, and `pg_stat_user_tables` counts as a plain
                    // table's would after the same change.
                    let old = &live.parts;
                    // Replaced from here on, whether or not the caller
                    // retired its partitions first: nothing opens a second
                    // instance of a file set through it beside the new one.
                    old.retired.store(true, Ordering::SeqCst);
                    let ids_io = Arc::new(old.seed.create_new(files::ids_file(self.uid)).await);
                    let ids = StdArc::new(IdSource::open_strict(ids_io).await.map_err(|e| e.to_string())?);
                    // The slots from the map as the log has it now: the old
                    // instance's after a schema change, a new layout after a
                    // reshape (phase 5), whose new pids get their watches and
                    // lifecycle locks first. A partition materialized since
                    // the old instance opened is materialized here too: its
                    // `MATERIALIZE` is in the state.
                    let (slots, index, epoch) = {
                        let state = old.board.state.lock().await;
                        declared = Some(state.map().len() as u32);
                        old.board.grow(&state);
                        let (slots, index, counts) = slots_of(&state, &self.root, self.uid, &*old.placement)?;
                        // The dealer's counts from the map as it is now: a
                        // reshape's targets, wherever `TO` put them, count
                        // where they are (the pool's location guards read
                        // these).
                        old.placement.register(self.uid, counts);
                        (slots, index, MapEpoch { epoch: state.epoch(), map: state.map().clone() })
                    };
                    let parts = StdArc::new_cyclic(|me| Parts {
                        budget: old.budget.clone(),
                        me: me.clone(),
                        idle_round_at: parking_lot::Mutex::new(None),
                        name: name.to_string(),
                        uid: self.uid,
                        db_id: self.db_id,
                        root: self.root.clone(),
                        transactions: self.transactions.clone(),
                        shell: shell.clone(),
                        spec: spec.clone(),
                        seed: old.seed.clone(),
                        ids,
                        board: old.board.clone(),
                        slots,
                        index,
                        mirrored: self.mirrored.clone(),
                        placement: old.placement.clone(),
                        torn: self.torn.clone(),
                        retired: AtomicBool::new(false),
                        vacuum: AtomicU8::new(old.vacuum.load(Ordering::Acquire)),
                        opens: AtomicU64::new(0),
                        closes: AtomicU64::new(0),
                    });
                    let next = Live {
                        router: RangeGuard::new(spec.clone(), PointRange::FULL),
                        spec,
                        // The same epoch, unless a reshape moved it: then
                        // the new map, under the epoch its commit recorded.
                        epoch: if live.epoch.load().epoch == epoch.epoch {
                            ArcSwap::new(live.epoch.load_full())
                        } else {
                            ArcSwap::from_pointee(epoch)
                        },
                        parts,
                        scheduler: live.scheduler.clone(),
                    };
                    // The partitions open in the old instance open again now,
                    // under the new schema, so a partition that no longer
                    // opens fails the change as at a start; a closed one stays
                    // closed and opens under the new schema on demand. Each
                    // under its lifecycle lock: the old instance is retired,
                    // so no close of it runs from here on.
                    for new in next.parts.slots.iter() {
                        // A pid the old instance had no slot for is a
                        // reshape's target: it opens on demand.
                        let Ok(old_slot) = old.slot_at(new.pid) else { continue };
                        // Asked before the lock: the old instance is retired,
                        // so nothing opens or closes it now, and a partition
                        // it never opened takes no lock (one would be built
                        // for every declared partition).
                        if old_slot.open_now().is_none() {
                            continue;
                        }
                        let lock = next.parts.board.lifecycle(new.pid).map_err(|e| e.to_string())?;
                        let _life = lock.lock().await;
                        if old_slot.open_now().is_none() {
                            continue;
                        }
                        let table = next.parts.open_instance(new, false).await.map_err(|err| format!("partition {}: {err}", new.pid))?;
                        let token = next.parts.budget.enter(StdArc::downgrade(&next.parts), self.uid, new.pid);
                        #[cfg(test)]
                        instance_count::reopened(&self.root, self.uid, new.pid);
                        new.put(table, token, old_slot.referenced_now());
                        #[cfg(test)]
                        crate::core::power_loss::failpoint::hit("test.pddl.partition_reopened", &self.root)
                            .map_err(|e| e.to_string())?;
                    }
                    next.parts.board.set_parts(StdArc::downgrade(&next.parts));
                    Ok(next)
                }
                .await;
                opened.map_err(|e| {
                    let why = format!("partitioned table \"{name}\" ({:016x}) cannot be opened: {e}", self.uid);
                    ::log::error!("{why}");
                    why
                })
            }
            Err(why) => Err(why.clone()),
        };
        // Registered while this instance still pins: the new one, broken,
        // holds the horizon where it is (see `broken_pin`).
        let broken_pin = live.is_err().then(|| BrokenPin::new(name, &self.transactions, &self.scheduler));
        PartitionedTable {
            name: name.to_string(),
            uid: self.uid,
            root: self.root.clone(),
            shell,
            db_id: self.db_id,
            transactions: self.transactions.clone(),
            live,
            declared,
            torn: self.torn.clone(),
            mirrored: self.mirrored.clone(),
            dropped: AtomicBool::new(false),
            broken_pin,
            scheduler: self.scheduler.clone(),
        }
    }

    /// The partition key's columns, outermost first, by their names in the
    /// shell's schema now (a RENAME COLUMN renames a key column; the hash
    /// only ever sees its write order).
    pub(crate) fn key_columns(&self) -> io::Result<Vec<String>> {
        let live = self.live()?;
        live.spec
            .levels()
            .iter()
            .map(|level| {
                self.shell
                    .schema
                    .fields
                    .iter()
                    .find(|f| !f.is_deleted && f.write_order as u64 == level.write_order as u64)
                    .map(|f| f.name.clone())
                    .ok_or_else(|| invalid(format!("partition key column {} is not in the schema", level.write_order)))
            })
            .collect()
    }

    /// Every materialized partition, by pid, under the current map epoch,
    /// each opened if it was closed: what a schema change validates existing
    /// rows in. Never only the open ones: a closed partition's rows would
    /// escape the check (phase 3 P5b).
    pub(crate) async fn all_partitions(&self) -> io::Result<Vec<(u32, Arc<Table<DbStorageProvider>>)>> {
        let epoch = self.epoch()?;
        self.materialized(&epoch, &Route::All).await
    }

    /// The partitions open now, by pid, opening none: what holds in-memory
    /// state worth reporting or sealing (the catalog's index readiness,
    /// `SHOW` figures, a checkpoint's seals). A closed partition has none:
    /// its next open builds or loads it. Never what a statement over the
    /// table's rows reads, which [`Self::materialized`] opens.
    /// How many slots have built their state, and how many write watches
    /// and lifecycle locks the board has built: the partitions something
    /// used, not every declared one.
    #[cfg(test)]
    pub(crate) fn built_per_partition(&self) -> (usize, usize, usize) {
        let Ok(live) = &self.live else { return (0, 0, 0) };
        let slots = live.parts.slots.iter().filter(|s| s.peek().is_some()).count();
        let (watches, locks) = live.parts.board.built();
        (slots, watches, locks)
    }

    pub fn open_partitions_now(&self) -> Vec<(u32, Arc<Table<DbStorageProvider>>)> {
        let Ok(live) = self.live() else { return Vec::new() };
        live.parts.slots.iter().filter_map(|s| s.open_now().map(|t| (s.pid, t))).collect()
    }

    /// The materialized partitions, by pid, opening none.
    pub fn materialized_pids(&self) -> io::Result<Vec<u32>> {
        let live = self.live()?;
        Ok(live.parts.slots.iter().filter(|s| s.materialized.load(Ordering::SeqCst)).map(|s| s.pid).collect())
    }

    /// How many partitions are open now.
    pub fn open_count(&self) -> usize {
        self.live().map_or(0, |live| live.parts.slots.iter().filter(|s| s.is_open()).count())
    }

    /// `SET CONFIG vacuum` on this table: every open partition now, and
    /// every partition that opens later, closed ones included. Returns how
    /// many partitions are materialized.
    pub(crate) fn set_vacuum_enabled(&self, enabled: bool) -> io::Result<usize> {
        let live = self.live()?;
        live.parts.vacuum.store(if enabled { 1 } else { 2 }, Ordering::SeqCst);
        let mut materialized = 0;
        for slot in live.parts.slots.iter() {
            if let Some(table) = slot.open_now() {
                table.set_vacuum_enabled(enabled)?;
            }
            materialized += slot.materialized.load(Ordering::SeqCst) as usize;
        }
        Ok(materialized)
    }

    /// This table's `SET CONFIG vacuum` value, if one was set.
    pub fn vacuum_setting(&self) -> Option<bool> {
        match self.live().ok()?.parts.vacuum.load(Ordering::SeqCst) {
            1 => Some(true),
            2 => Some(false),
            _ => None,
        }
    }

    /// Partition opens and exclusive closes this instance has made.
    pub fn open_close_counts(&self) -> (u64, u64) {
        self.live().map_or((0, 0), |live| {
            (live.parts.opens.load(Ordering::Acquire), live.parts.closes.load(Ordering::Acquire))
        })
    }

    /// Bytes in the rows files of the materialized partitions, open or
    /// closed: what a plain table's `len` is to its `ALTER` (a table with
    /// none is metadata-only to change). A closed partition counts its rows
    /// file and its staged `.tmp` from their metadata, so a table whose
    /// partitions are all closed is never taken for empty.
    ///
    /// Never from kept sizes (P5b-2a review): a closed partition's rows file
    /// is read from its metadata each time. Taking no lifecycle lock, it can
    /// race a close in flight, whose files already hold every row (partition
    /// writes reach them through the log); a DDL's own check under ACCESS
    /// EXCLUSIVE reads [`Self::rows_len_exact`], which waits it out.
    pub(crate) fn rows_len(&self) -> io::Result<u64> {
        let live = self.live()?;
        let mut total = 0u64;
        for slot in live.parts.slots.iter() {
            match slot.open_now() {
                Some(table) => total += table.data()?.len.load(Ordering::Relaxed),
                None if slot.materialized.load(Ordering::SeqCst) => {
                    total += closed_lens(slot.dir(&self.root), self.uid, slot.pid)?.0;
                }
                None => {}
            }
        }
        Ok(total)
    }

    /// [`Self::rows_len`] with each materialized partition read under its
    /// lifecycle lock, so an open or a close in flight (an idle close or an
    /// eviction, which take no table lock) finishes first: the instance's
    /// length if it is open then, its files' if it is closed. What a DDL
    /// decides "metadata-only" by, under ACCESS EXCLUSIVE on the table.
    pub(crate) async fn rows_len_exact(&self) -> io::Result<u64> {
        let live = self.live()?;
        let mut total = 0u64;
        for slot in live.parts.slots.iter() {
            if !slot.materialized.load(Ordering::SeqCst) {
                continue;
            }
            let lock = live.parts.board.lifecycle(slot.pid)?;
            let _life = lock.lock().await;
            total += match slot.open_now() {
                Some(table) => table.data()?.len.load(Ordering::Relaxed),
                None => closed_lens(slot.dir(&self.root), self.uid, slot.pid)?.0,
            };
        }
        Ok(total)
    }

    fn live(&self) -> io::Result<&Live> {
        if self.dropped.load(Ordering::Acquire) {
            return Err(io::Error::other(format!("partitioned table \"{}\" was dropped", self.name)));
        }
        self.live.as_ref().map_err(|why| io::Error::other(why.clone()))
    }

    /// `Err` with why the table cannot serve data: it is broken, or dropped.
    pub(crate) fn check(&self) -> io::Result<()> {
        self.live().map(|_| ())
    }

    /// Why the table is broken, if it is.
    pub fn broken(&self) -> Option<&str> {
        self.live.as_ref().err().map(String::as_str)
    }

    /// Set when the map log was torn (§12): reaping must stay off.
    pub(crate) fn torn(&self) -> Option<&str> {
        self.torn.as_deref()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn uid(&self) -> u64 {
        self.uid
    }

    /// The schema-only shell. Its data methods refuse.
    pub fn shell(&self) -> &Arc<Table<DbStorageProvider>> {
        &self.shell
    }

    pub fn spec(&self) -> io::Result<&KeySpec> {
        Ok(&self.live()?.spec)
    }

    /// The visibility rule for a statement under `tx`, from the database's
    /// one manager: step 1 of §4.4, taken before [`Self::epoch`].
    pub fn visibility_for(&self, tx: TxId) -> Visibility {
        Visibility::for_snapshot(self.transactions.snapshot(tx), self.transactions.clone())
    }

    /// `Table::statement_visibility` for a partitioned table: the snapshot
    /// its write's statement already read under, if it read first.
    pub fn statement_visibility(&self, tx: TxId) -> Option<Visibility> {
        crate::core::rql::statement_snapshot::target_visibility(&self.transactions, tx, || self.visibility_for(tx))
    }

    /// The rule for `tx` as of now, ignoring a snapshot it has pinned, as
    /// `Table::visibility_now_for` is for a plain table: what a foreign-key
    /// check reads a partitioned parent or child with (phase 3 P4), every
    /// partition under this one value.
    pub fn visibility_now_for(&self, tx: TxId) -> Visibility {
        Visibility::for_snapshot(self.transactions.snapshot_now(tx), self.transactions.clone())
    }

    /// [`Self::visibility_for`] for a query, as `Table::read_visibility_for`
    /// is for a plain table: inside a read-only statement, the snapshot every
    /// read of that statement shares (`statement_snapshot`), else a new one.
    /// A partitioned table is always versioned, so this is never `All`.
    pub fn read_visibility_for(&self, tx: TxId) -> Visibility {
        crate::core::rql::statement_snapshot::get_or_take(&self.transactions, || self.visibility_for(tx))
    }

    /// The current map epoch, counted (`load_full`, never `load`): step 2 of
    /// §4.4, after the statement's visibility.
    pub fn epoch(&self) -> io::Result<StdArc<MapEpoch>> {
        Ok(self.live()?.epoch.load_full())
    }

    /// Where the rows a `WHERE` clause can match live, as the table's own scan
    /// reads the clause: tokenized against the schema, `Zero` only when it is
    /// constant false, and narrowed only by key predicates `extract_gated`
    /// proves (never `extract_gated_with_paths`, whose virtual columns are no
    /// key). A clause the tokenizer cannot read routes everywhere, where the
    /// evaluator reports it.
    pub fn route_where(&self, where_clause: &str) -> io::Result<Route> {
        let live = self.live()?;
        if where_clause.trim().is_empty() {
            return Ok(Route::All);
        }
        let schema = &self.shell.schema;
        let Ok(tokens) = crate::core::tokenizer::query_tokenizer::try_tokenize(where_clause, &schema.fields) else {
            return Ok(Route::All);
        };
        let constant_false = sarg::clause_tokens_are_constant_false(where_clause, &tokens);
        let keyed: SmallVec<[u32; 3]> = live.spec.levels().iter().map(|l| l.write_order).collect();
        let program = sarg::extract_gated(&tokens, &|wo| keyed.contains(&wo), &decimal_scales(schema));
        Ok(route(&live.spec, program.as_ref().map(|p| &p.root), constant_false))
    }

    /// Whether an `UPDATE` matching `where_clause` and assigning `assigned`
    /// (the constants, by write order; an expression's placeholder included)
    /// and `computed` (the write orders an expression sets) provably keeps
    /// every matched row's point (§4.5, §9). True when no key column is
    /// assigned, or when each one assigned gets a constant that a top-level
    /// conjunct `key = v` of the clause already pins it to: `SET tenant_id =
    /// 5 … WHERE tenant_id = 5 AND …`, an ORM's every-column UPDATE. A key
    /// column given an expression, or a constant nothing pins, is unproven,
    /// and phase 2a refuses the statement before it writes anything (a key
    /// change moves a row between partitions: phase 6).
    ///
    /// The conjunct is read by `extract_gated`, coerced exactly as the scan
    /// compares it, so every row the clause matches holds `v` there; the two
    /// values are compared by level hash, so the point is unchanged. Stricter
    /// than the range guard, which still checks every merged row.
    pub fn assignments_keep_point(
        &self,
        where_clause: &str,
        assigned: &[(u32, &[u8])],
        computed: &[u32],
    ) -> io::Result<bool> {
        let live = self.live()?;
        Ok(assignments_keep_point(&live.spec, &self.shell.schema, where_clause, assigned, computed))
    }

    /// The materialized partitions `route` reaches under `epoch`, by pid,
    /// each opened now if it is closed (a routed open, phase 3 P5b). An
    /// unmaterialized one is skipped: it holds no row (§4.3). A partition
    /// that does not open fails the call: it is never skipped, which would
    /// answer as if its rows were not there.
    pub async fn materialized(
        &self,
        epoch: &MapEpoch,
        route: &Route,
    ) -> io::Result<Vec<(u32, Arc<Table<DbStorageProvider>>)>> {
        let planned = self.planned(epoch, route)?;
        let routed = planned.len() == 1;
        let mut out = Vec::with_capacity(planned.len());
        let mut opened: SmallVec<[u32; 4]> = SmallVec::new();
        for (pid, open) in planned {
            let table = match open {
                Some(table) => table,
                None => match self.live()?.parts.open(pid, routed).await {
                    Ok(table) => {
                        opened.push(pid);
                        table
                    }
                    Err(e) => {
                        // What this call opened is closed again, so a failed
                        // open part-way (out of file descriptors) leaves no
                        // partition held for nothing.
                        drop(out);
                        self.close_opened(&opened).await;
                        return Err(e);
                    }
                },
            };
            // What a write, a DDL or a VACUUM reaches is upgraded (P5b-2b).
            table.ensure_full();
            out.push((pid, table));
        }
        Ok(out)
    }

    /// Closes each of `pids` that nothing else holds now: what a statement
    /// that failed part-way through opening partitions gives back. Boxed:
    /// it is the error path of every fan-out, whose futures stay small.
    pub(crate) fn close_opened<'a>(&'a self, pids: &'a [u32]) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            for &pid in pids {
                if let Err(e) = self.close_partition(pid).await {
                    ::log::warn!("partition {pid} of \"{}\" opened by a failed statement did not close: {e}", self.name);
                }
            }
        })
    }

    /// The materialized partitions `route` reaches under `epoch`, by pid,
    /// opening none: each with its open instance, or `None` while it is
    /// closed, for the caller to open with [`Self::open_part`] when it gets
    /// to it (a fan-out that stops early never opens the rest). The flags
    /// are read here, so a caller that took its snapshot first skips only
    /// partitions holding nothing it may see (§4.3, §4.4).
    pub fn planned(&self, epoch: &MapEpoch, route: &Route) -> io::Result<Planned> {
        let live = self.live()?;
        let mut out = Planned::new();
        for pid in route.pids(&epoch.map) {
            let slot = Self::slot(live, pid)?;
            if let Some(table) = slot.open_now() {
                out.push((pid, Some(table)));
            } else if slot.materialized.load(Ordering::SeqCst) {
                out.push((pid, None));
            }
        }
        // A routed access, one partition reached, sets its reference bit; a
        // fan-out's sets none (scan resistance, `partition::budget`).
        if let [(pid, open)] = out.as_slice() {
            Self::slot(live, *pid)?.touch();
            // A routed access upgrades a scan-only instance (P5b-2b).
            if let Some(table) = open {
                table.ensure_full();
            }
        }
        Ok(out)
    }

    /// Materialized partition `pid`'s open instance, opened now if it is
    /// closed, as a routed access (its reference bit set). Errs for a
    /// partition that is not materialized, and when the open fails: never an
    /// empty stand-in.
    pub async fn open_part(&self, pid: u32) -> io::Result<Arc<Table<DbStorageProvider>>> {
        let live = self.live()?;
        live.parts.open(pid, true).await
    }

    /// [`Self::open_part`] for a fan-out over several partitions: one it
    /// opens enters the CLOCK with its reference bit clear, so a scan wider
    /// than the budget evicts what it opened before what routed accesses
    /// keep using (`partition::budget`).
    pub async fn open_part_scan(&self, pid: u32) -> io::Result<Arc<Table<DbStorageProvider>>> {
        let live = self.live()?;
        live.parts.open(pid, false).await
    }

    /// Closes partition `pid` exclusively (§7.2 point 2): only while nothing
    /// but its slot holds the instance (strong count 1) and no transaction
    /// keeps a ticket on its write gate; then its maintenance is stopped and
    /// joined, its staged appends landed, and the instance dropped. Its pin
    /// stays in the slot (the map log's state), so a closed partition keeps
    /// pinning. The next access opens it again.
    pub async fn close_partition(&self, pid: u32) -> io::Result<Close> {
        let live = self.live()?;
        live.parts.close(pid).await
    }

    /// [`Self::close_partition`] of every open partition; returns how many
    /// closed. Those in use stay open.
    pub async fn close_idle(&self) -> io::Result<usize> {
        let live = self.live()?;
        let mut closed = 0;
        for slot in live.parts.slots.iter() {
            if slot.is_open() && live.parts.close(slot.pid).await? == Close::Closed {
                closed += 1;
            }
        }
        Ok(closed)
    }

    /// The point a full key routes to, for a learned prepared statement's
    /// bound key (S16b): `levels` holds, per key level and leading first, the
    /// write order and declared type the key was bound for and the stored
    /// bytes its value compares as. `None`, so the caller routes the rendered
    /// `WHERE` instead, unless `uid` is this table's and `levels` names this
    /// table's key exactly: a table re-created under the name, or a key that
    /// changed columns or types, never routes on a key bound for another.
    /// Hashed through `KeySpec::prefix_range`, as `route_where` and the
    /// insert path hash.
    pub fn keyed_range<V: AsRef<[u8]>>(
        &self,
        uid: u64,
        levels: &[(u32, crate::core::db_type::DbType, V)],
    ) -> Option<PointRange> {
        if uid != self.uid {
            return None;
        }
        let spec = &self.live().ok()?.spec;
        let key = spec.levels();
        if key.len() != levels.len()
            || key.iter().zip(levels).any(|(k, (wo, ty, _))| k.write_order != *wo || k.db_type != *ty)
        {
            return None;
        }
        let values: SmallVec<[&[u8]; 3]> = levels.iter().map(|(_, _, v)| v.as_ref()).collect();
        spec.prefix_range(&values).ok()
    }

    /// [`Self::materialized`] for the route `Points([range])`, as a bound key's
    /// [`Self::keyed_range`] names it: without building the route when one
    /// partition owns the whole range, which a full key's range (one point,
    /// or a 2¹⁶ tenant block for a one-column key) is unless a reshape split
    /// it; through the general path when it did.
    pub fn materialized_in(&self, epoch: &MapEpoch, range: PointRange) -> io::Result<Planned> {
        let Some(pid) = epoch.map.sole_pid(range.lo, range.hi) else {
            return self.planned(epoch, &Route::Points(vec![range]));
        };
        let live = self.live()?;
        let slot = Self::slot(live, pid)?;
        let mut out = Planned::new();
        if let Some(table) = slot.open_now() {
            table.ensure_full();
            out.push((pid, Some(table)));
        } else if slot.materialized.load(Ordering::SeqCst) {
            out.push((pid, None));
        }
        slot.touch();
        Ok(out)
    }

    fn slot(live: &Live, pid: u32) -> io::Result<&Slot> {
        live.parts.slot_at(pid)
    }

    /// The partition owning `point`, materialized first if it never was and
    /// opened if it is closed.
    pub(crate) async fn partition_for_point(&self, point: u64) -> io::Result<Arc<Table<DbStorageProvider>>> {
        let live = self.live()?;
        let epoch = live.epoch.load_full();
        let slot = Self::slot(live, epoch.map.pid_of(point))?;
        if !slot.range.contains(point) {
            return Err(io::Error::other(format!(
                "internal routing error: point {point:#018x} is outside partition {}'s slot",
                slot.pid
            )));
        }
        slot.touch();
        if let Some(table) = slot.open_now() {
            table.ensure_full();
            return Ok(table);
        }
        live.parts.open_slow_erased(slot, true, true).await
    }

    /// Inserts `row` under `tx` into the partition its key routes to,
    /// materializing that partition first if need be. The partition's guard
    /// checks the row's point again (I4).
    pub async fn insert_row_in_txn(&self, row: RowWrite, tx: TxId) -> io::Result<()> {
        let live = self.live()?;
        let point = live.router.point_of_write(&row).map_err(io::Error::other)?;
        if crate::core::notice::tracing_partitions() {
            let map = &live.epoch.load_full().map;
            crate::core::notice::trace_insert(self.name(), || format!("routed by {}", self.key_names()), map.len(), map.pid_of(point));
        }
        self.partition_for_point(point).await?.insert_row_in_txn(row, tx).await
    }

    /// The partition key's columns, as `partition_trace` names them:
    /// `tenant_id` or `(tenant_id, asset_group_id)`.
    pub(crate) fn key_names(&self) -> String {
        let Ok(live) = self.live() else {
            return "the partition key".to_string();
        };
        let names: Vec<&str> = live
            .spec
            .levels()
            .iter()
            .map(|level| {
                self.shell
                    .schema
                    .fields
                    .iter()
                    .find(|f| f.write_order == level.write_order as u64 && !f.is_deleted)
                    .map_or("?", |f| f.name.as_str())
            })
            .collect();
        match names.as_slice() {
            [one] => one.to_string(),
            many => format!("({})", many.join(", ")),
        }
    }

    /// `partition_trace`'s NOTICE for a statement that `verb`s the partitions
    /// `route` reaches under `epoch` (`None`: a prepared statement's bound
    /// key, one partition), and why those.
    pub(crate) fn trace_route(&self, verb: &str, epoch: &MapEpoch, route: Option<&Route>, where_clause: &str) {
        crate::core::notice::trace(|| {
            let declared = epoch.map.len();
            let (routed, why) = match route {
                None => (1, format!("routed by the bound value of {}", self.key_names())),
                Some(Route::Zero) => (0, "the WHERE clause is constant false".to_string()),
                Some(route @ Route::Points(_)) => (route.pids(&epoch.map).len(), format!("routed by {}", self.key_names())),
                Some(Route::All) if where_clause.trim().is_empty() => (declared, "no WHERE clause".to_string()),
                Some(Route::All) => (declared, format!("the WHERE clause does not pin {}", self.key_names())),
            };
            format!("\"{}\": {verb} {routed} of {declared} partitions ({why})", self.name())
        });
    }

    /// The routing point of a row about to be written.
    pub(crate) fn point_of_write(&self, row: &RowWrite) -> io::Result<u64> {
        self.live()?.router.point_of_write(row).map_err(io::Error::other)
    }

    /// Partitions the map declares; what the per-database cap counts (see
    /// [`declared_in`]). `None` when the log did not replay: unknown, not 0.
    pub(crate) fn declared(&self) -> Option<u32> {
        self.declared
    }

    /// A partition's (rows, pointers, filters) file lengths.
    async fn file_lens(table: &Table<DbStorageProvider>) -> io::Result<(u64, u64, u64)> {
        Ok((
            table.io_rows()?.get_len().await,
            table.io_pointers()?.get_len().await,
            table.index_store_bytes()?,
        ))
    }

    async fn sizes(table: &Table<DbStorageProvider>) -> io::Result<(u64, u64)> {
        let (rows, pointers, filters) = Self::file_lens(table).await?;
        Ok((pointers / ROW_POINTER_RECORD_LEN as u64, rows + pointers + filters))
    }

    /// Totals over the materialized partitions, and the parent files. An open
    /// partition's lengths are atomics on local storage; a closed one's are
    /// its files' metadata (phase 3 P5b).
    pub async fn totals(&self) -> io::Result<Totals> {
        let live = self.live()?;
        let mut t = Totals {
            ids_handed_out: live.parts.ids.handed_out(),
            declared: live.parts.slots.len() as u32,
            schema: self.shell.io_schema.get_len().await,
            map: live.parts.board.log.io().get_len().await,
            ids: live.parts.ids.io.get_len().await,
            ..Totals::default()
        };
        let mut swept_live = Some(0u64);
        for slot in live.parts.slots.iter() {
            let open = slot.open_now();
            let lens = match &open {
                Some(table) => Some(Self::file_lens(table).await?),
                // A closed partition, from its files' metadata.
                None if slot.materialized.load(Ordering::SeqCst) => Some(slot.closed_sizes(slot.dir(&self.root), self.uid)?),
                None => None,
            };
            if let Some((rows, pointers, filters)) = lens {
                let slots = pointers / ROW_POINTER_RECORD_LEN as u64;
                t.materialized += 1;
                t.slots += slots;
                t.data += rows;
                t.pointers += pointers;
                t.filters += filters;
                // A closed partition's counts went with its instance.
                let stats = match &open {
                    Some(table) => Some(table.vacuum_stats()?),
                    None => None,
                };
                t.deleted_since_open += stats.map_or(0, |s| s.deleted_rows.load(Ordering::Relaxed));
                swept_live = match (swept_live, stats.and_then(|s| s.live_rows())) {
                    (Some(sum), Some(live)) => Some(sum + live.min(slots)),
                    _ => None,
                };
            }
        }
        t.swept_live = swept_live.filter(|_| t.materialized > 0);
        t.bytes = t.data + t.pointers + t.filters + t.schema + t.map + t.ids;
        Ok(t)
    }

    /// The map header's routing version (§4.1), as replayed.
    pub async fn route_v(&self) -> io::Result<u16> {
        Ok(self.live()?.parts.board.state.lock().await.header().route_v)
    }

    /// Every declared partition, in map order (by pid until a reshape).
    pub async fn partitions(&self) -> io::Result<Vec<PartitionInfo>> {
        let live = self.live()?;
        let mut out = Vec::with_capacity(live.parts.slots.len());
        for slot in live.parts.slots.iter() {
            let dir = slot.dir(&self.root);
            let shown = || Some(dir.to_string_lossy().into_owned());
            let (location, state, slots, bytes) = match slot.open_now() {
                Some(table) => {
                    let (slots, bytes) = Self::sizes(&table).await?;
                    (shown(), "materialized", slots, bytes)
                }
                None if slot.materialized.load(Ordering::SeqCst) => {
                    let (rows, pointers, filters) = slot.closed_sizes(dir, self.uid)?;
                    (shown(), "materialized", pointers / ROW_POINTER_RECORD_LEN as u64, rows + pointers + filters)
                }
                None => (None, "unmaterialized", 0, 0),
            };
            out.push(PartitionInfo {
                pid: slot.pid,
                lo: slot.range.lo,
                hi: slot.range.hi,
                location,
                state,
                slots,
                bytes,
            });
        }
        Ok(out)
    }

    /// The table's pins, map log and forget handle (§7.2).
    pub fn pin_board(&self) -> io::Result<&StdArc<PinBoard>> {
        Ok(&self.live()?.parts.board)
    }

    /// Partition `pid`'s pin as the map log's state holds it: `None` when
    /// clean or unmaterialized.
    pub async fn pin_of(&self, pid: u32) -> io::Result<Option<TxId>> {
        Ok(self.live()?.parts.board.state.lock().await.part(pid).and_then(|p| p.pin))
    }

    /// Seeds `SERIAL` sequence `name` from its durable reservation (§10):
    /// `false` when the table has none for it, and the caller seeds it from
    /// the partitions' maximum and then calls [`Self::track_serial`].
    pub(crate) fn resume_serial(&self, name: &str) -> io::Result<bool> {
        Ok(self.live()?.parts.board.serials.resume(name))
    }

    /// Reserves `SERIAL` sequence `name` from here on, seeded at `last`.
    /// `seeded`: whether the caller's seed was the counter's.
    pub(crate) fn track_serial(&self, name: &str, last: i64, seeded: bool) -> io::Result<()> {
        self.live()?.parts.board.serials.track(name, last, seeded);
        Ok(())
    }

    /// Sequences this instance's table has seeded from the partitions'
    /// maximum rather than its reservations file.
    pub fn serial_max_scans(&self) -> u64 {
        self.live().map_or(0, |l| l.parts.board.serials.max_scans())
    }

    /// The durable reservation of `SERIAL` sequence `name`, if any.
    pub fn serial_reserved(&self, name: &str) -> Option<i64> {
        self.live().ok()?.parts.board.serials.reserved(name)
    }

    /// A forget sweep of partition `pid` now, through its slot
    /// (`PinBoard::sweep`): `SWEPT`, its pin moved, and the registry asked
    /// to forget. For tests and tools.
    pub async fn sweep_pin_now(&self, pid: u32) -> io::Result<crate::core::vacuum::NeutralizeReport> {
        // A closed, pinned partition is opened for its sweep (`sweep_all`).
        self.live()?.parts.board.sweep(pid).await
    }

    /// The clean-shutdown write of the table's `SERIAL` reservations (§10):
    /// the exact last values, synced, so a clean restart leaves no gap.
    pub(crate) async fn record_serial_exact(&self) -> io::Result<()> {
        match &self.live {
            Ok(live) => live.parts.board.serials.record_exact().await,
            Err(_) => Ok(()),
        }
    }

    /// Forget sweeps of every materialized, pinned partition now, each
    /// through its slot (`PinBoard::sweep_all`, which opens a closed one for
    /// its sweep and closes it again after). Returns how many advanced. For tests and tools:
    /// the scheduler sweeps a pin once it lags by `FORGET_STEP`.
    pub async fn sweep_pins_now(&self) -> io::Result<usize> {
        let live = self.live()?;
        let pinned: Vec<u32> = {
            let state = live.parts.board.state.lock().await;
            state.parts().filter(|(_, p)| p.location.is_some() && p.pin.is_some()).map(|(pid, _)| pid).collect()
        };
        let mut advanced = 0;
        for (_, r) in live.parts.board.sweep_all(&pinned).await? {
            if r?.advanced_to.is_some() {
                advanced += 1;
            }
        }
        Ok(advanced)
    }

    /// §12's DROP steps 3 to 5, after the caller has tombstoned the catalogue
    /// row (the commit point) and taken the table out of both maps: `DROPPED`
    /// (skipped for a table whose log never opened), then every file of the
    /// uid, partitions first and the map log last. Works on a broken table.
    pub(crate) async fn drop_files(&self) -> io::Result<()> {
        self.mark_dropped().await?;
        if self.root.as_os_str().is_empty() {
            return Ok(());
        }
        files::remove_all(&self.root, self.uid).map(|_| ())
    }

    /// §12's DROP step 3 alone: every data call refuses from here on, and the
    /// map log ends in a durable `DROPPED` (skipped for a table whose log never
    /// opened). For a caller that must void the log's records for the files
    /// between this and deleting them, which `DROPPED` is itself one of.
    pub(crate) async fn mark_dropped(&self) -> io::Result<()> {
        self.dropped.store(true, Ordering::Release);
        if let Ok(live) = &self.live {
            // Nothing opens, materializes or closes from here on, and none in
            // flight is still creating or landing into the files about to go:
            // each partition's lifecycle lock taken once, after the flag.
            live.parts.retired.store(true, Ordering::SeqCst);
            for slot in live.parts.slots.iter() {
                if let Some(lock) = live.parts.board.lifecycle_if_built(slot.pid) {
                    drop(lock.lock().await);
                }
            }
            let mut state = live.parts.board.state.lock().await;
            if !state.dropped() {
                let mut next = state.clone();
                next.apply(&Record::Dropped).map_err(invalid)?;
                live.parts.board.log.append_durable(&Record::Dropped).await?;
                *state = next;
            }
        }
        Ok(())
    }
}

/// Reshape (docs/partitioning.md §6.2, phase 5; `partition::reshape`).
impl PartitionedTable {
    /// The one range `for_clause` (`k = v [AND …]`, the inside of a `FOR
    /// (…)`) pins: a leading prefix of the key fixed by equality.
    fn pinned_range(&self, for_clause: &str) -> io::Result<PointRange> {
        let clause = for_clause.replace(',', " AND ");
        match self.route_where(&clause)? {
            Route::Points(ranges) if ranges.len() == 1 => Ok(ranges[0]),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot reshape: FOR ({for_clause}) must fix the leading columns of the partition key ({}) by \
                     equality",
                    self.key_names()
                ),
            )),
        }
    }

    /// `dir` if it is a storage location of this database (the root or
    /// another), as the pool now names it.
    fn location_dir(&self, dir: &str) -> io::Result<PathBuf> {
        let live = self.live()?;
        let key = files::dir_key(dir);
        if key == files::dir_key(&self.root.to_string_lossy()) {
            return Ok(self.root.clone());
        }
        live.parts
            .placement
            .others()
            .into_iter()
            .find(|p| files::dir_key(&p.to_string_lossy()) == key)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "cannot reshape: '{dir}' is not a storage location of this database; add it with STORAGE ADD \
                         LOCATION '{dir}'"
                    ),
                )
            })
    }

    /// Plans `op` against the map now: its sources and its targets' starts
    /// and directories. Reads only the map and the statement.
    pub fn plan_reshape(&self, op: &super::reshape::ReshapeOp) -> io::Result<super::reshape::ReshapePlan> {
        use super::reshape::{ReshapeOp, ReshapePlan, starts};
        let live = self.live()?;
        let epoch = live.epoch.load_full();
        let map = &epoch.map;
        let refuse = |why: String| io::Error::new(io::ErrorKind::InvalidInput, format!("cannot reshape: {why}"));
        let owner = |r: PointRange| -> io::Result<(u32, PointRange)> {
            let pid = map.pid_of(r.lo);
            let range = map.range_of(pid).ok_or_else(|| io::Error::other(format!("partition {pid} is not in the map")))?;
            Ok((pid, range))
        };
        let plan = match op {
            ReshapeOp::SplitPartition { at } => {
                let (pid, r) = owner(self.pinned_range(at)?)?;
                let width = r.hi - r.lo as u128;
                if width < 2 {
                    return Err(refuse(format!("partition {pid} holds a single point and cannot be split")));
                }
                let mid = r.lo as u128 + width / 2;
                ReshapePlan { sources: vec![pid], targets: starts(r.lo, r.hi, &[mid]).into_iter().map(|lo| (lo, None)).collect() }
            }
            ReshapeOp::MergePartitions { first, second } => {
                let (a, ra) = owner(self.pinned_range(first)?)?;
                let (b, rb) = owner(self.pinned_range(second)?)?;
                if a == b {
                    return Err(refuse(format!("both FOR clauses name partition {a}; nothing to merge")));
                }
                let (lo, hi, sources) = if ra.lo < rb.lo { (ra, rb, vec![a, b]) } else { (rb, ra, vec![b, a]) };
                if lo.hi != hi.lo as u128 {
                    return Err(refuse(format!(
                        "partitions {} and {} are not adjacent; only adjacent partitions merge",
                        sources[0], sources[1]
                    )));
                }
                ReshapePlan { sources, targets: vec![(lo.lo, None)] }
            }
            ReshapeOp::MovePartition { at, to } => {
                let (pid, r) = owner(self.pinned_range(at)?)?;
                ReshapePlan { sources: vec![pid], targets: vec![(r.lo, Some(self.location_dir(to)?))] }
            }
            ReshapeOp::IsolateKey { key, to } => {
                let block = self.pinned_range(key)?;
                let (pid, r) = owner(block)?;
                if block.hi > r.hi {
                    return Err(refuse(format!(
                        "the rows of ({key}) span more than partition {pid}; MERGE PARTITIONS first"
                    )));
                }
                let to = to.as_deref().map(|d| self.location_dir(d)).transpose()?;
                let lows = starts(r.lo, r.hi, &[block.lo as u128, block.hi]);
                if lows.len() == 1 && to.is_none() {
                    return Err(refuse(format!("({key}) already has partition {pid} to itself")));
                }
                ReshapePlan {
                    sources: vec![pid],
                    targets: lows.into_iter().map(|lo| (lo, if lo == block.lo { to.clone() } else { None })).collect(),
                }
            }
            ReshapeOp::SplitKey { key, into } => {
                let n = *into;
                if !(2..=1024).contains(&n) || !n.is_power_of_two() {
                    return Err(refuse(format!("SPLIT KEY … INTO {n}: the piece count must be a power of two from 2 to 1024")));
                }
                if live.spec.levels().len() < 2 {
                    return Err(refuse(format!(
                        "SPLIT KEY needs a partition key of two or three columns; ({}) is one, and one value's \
                         hash is a single point (ISOLATE KEY gives it a partition of its own)",
                        self.key_names()
                    )));
                }
                let block = self.pinned_range(key)?;
                let width = block.hi - block.lo as u128;
                if width < n as u128 || width % n as u128 != 0 {
                    return Err(refuse(format!(
                        "({key}) fixes every column of the partition key, so it is one point and cannot be split; \
                         name a leading prefix"
                    )));
                }
                let (pid, r) = owner(block)?;
                if block.hi > r.hi {
                    return Err(refuse(format!(
                        "the rows of ({key}) span more than partition {pid}; MERGE PARTITIONS first"
                    )));
                }
                let step = width / n as u128;
                let mut cuts: Vec<u128> = (0..=n as u128).map(|j| block.lo as u128 + j * step).collect();
                cuts.retain(|&c| c > r.lo as u128 && c < r.hi);
                ReshapePlan { sources: vec![pid], targets: starts(r.lo, r.hi, &cuts).into_iter().map(|lo| (lo, None)).collect() }
            }
        };
        if plan.targets.len() > 1 + 1024 + 1 {
            return Err(refuse("a reshape may make at most 1026 partitions".to_string()));
        }
        let declared = map.len() as u64 - plan.sources.len() as u64 + plan.targets.len() as u64;
        if declared > MAX_DECLARED_PER_DATABASE as u64 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("the reshape would give the table {declared} partitions; more than {MAX_DECLARED_PER_DATABASE} is not supported yet"),
            ));
        }
        Ok(plan)
    }

    /// Runs `plan` up to its commit point (`partition::reshape`, steps 1 to
    /// 6). The caller holds ACCESS EXCLUSIVE on the logical name and a
    /// forget pause, and reopens the table afterwards whatever this returns
    /// (this instance is retired either way). `Ok`: the committed sources'
    /// files, for the caller to delete. `Err`: why, and the targets' files,
    /// for the caller to delete (the reshape was aborted, durably if the
    /// append succeeded; the open aborts it otherwise).
    pub(crate) async fn reshape(
        &self,
        plan: &super::reshape::ReshapePlan,
    ) -> Result<super::reshape::ReshapeFiles, (io::Error, super::reshape::ReshapeFiles)> {
        use super::reshape::{CopyTarget, ReshapeFiles, copy, land};
        let none = |e: io::Error| (e, ReshapeFiles::default());
        let live = self.live().map_err(none)?;
        let parts = &live.parts;
        let board = &parts.board;
        let _no_sweeps = board.hold_sweeps().await;

        // 1. The sources, opened, then the instance retired: maintenance
        // stopped and joined, each source's scheduler unit quiesced.
        let mut sources: Vec<(u32, Arc<Table<DbStorageProvider>>)> = Vec::new();
        let mut source_dirs: Vec<(u32, PointRange, Option<PathBuf>)> = Vec::new();
        for &pid in &plan.sources {
            let slot = parts.slot_at(pid).map_err(none)?;
            if slot.materialized.load(Ordering::SeqCst) {
                let table = parts.open(pid, false).await.map_err(none)?;
                sources.push((pid, table));
                source_dirs.push((pid, slot.range, Some(slot.dir(&self.root).to_path_buf())));
            } else {
                source_dirs.push((pid, slot.range, None));
            }
        }
        self.retire_partitions().await;
        for (_, table) in &sources {
            table.close_exclusive(false).await;
        }

        // 2. RESHAPE_BEGIN, durable, its locations interned first.
        let mut state = board.state.lock().await;
        let mut dealt: Vec<String> = Vec::new();
        let mut targets: Vec<(super::log::ReshapeTarget, PointRange, PathBuf)> = Vec::new();
        let span_hi = source_dirs.iter().map(|(_, r, _)| r.hi).max().unwrap_or(0);
        let begun: io::Result<super::log::ReshapeBegin> = async {
            let mut next_pid = u32::try_from(state.next_pid()).map_err(|_| io::Error::other("partition ids exhausted"))?;
            for (i, (lo, dir)) in plan.targets.iter().enumerate() {
                let hi = plan.targets.get(i + 1).map_or(span_hi, |(next, _)| *next as u128);
                let dir = match dir {
                    Some(d) => d.clone(),
                    None => match source_dirs.iter().find(|(_, r, _)| r.contains(*lo)).and_then(|(_, _, d)| d.clone()) {
                        Some(d) => d,
                        None => match parts.placement.deal(self.uid) {
                            Some(d) => {
                                dealt.push(d.clone());
                                PathBuf::from(d)
                            }
                            None => self.root.clone(),
                        },
                    },
                };
                let loc = intern_location(board, &mut state, &self.root, &dir).await?;
                targets.push((super::log::ReshapeTarget { lo: *lo, pid: next_pid, loc }, PointRange { lo: *lo, hi }, dir));
                next_pid = next_pid.checked_add(1).ok_or_else(|| io::Error::other("partition ids exhausted"))?;
            }
            let begin = super::log::ReshapeBegin {
                id: state.last_reshape() + 1,
                sources: plan.sources.clone(),
                targets: targets.iter().map(|(t, _, _)| *t).collect(),
            };
            let rec = Record::ReshapeBegin(begin.clone());
            let mut next = state.clone();
            next.apply(&rec).map_err(invalid)?;
            board.log.append_durable(&rec).await?;
            *state = next;
            board.grow(&state);
            Ok(begin)
        }
        .await;
        let undeal = |dealt: &[String]| {
            for d in dealt {
                parts.placement.undeal(self.uid, d);
            }
        };
        let begin = match begun {
            Ok(b) => b,
            Err(e) => {
                undeal(&dealt);
                return Err(none(e));
            }
        };
        let target_files = ReshapeFiles { doomed: targets.iter().map(|(t, _, d)| (t.pid, d.clone())).collect() };

        // 3 to 5: the targets' files, the copy, the check.
        let copied: io::Result<()> = async {
            let mut copying = Vec::with_capacity(targets.len());
            for (t, range, dir) in &targets {
                files::create_partition_files(dir, self.uid, t.pid)?;
                let physical = files::data_stem(self.uid, t.pid);
                let table = Table::open_partition(PartitionOpen {
                    cache_scope: format!("{}\u{0}{}", self.db_id, physical),
                    physical,
                    seed: parts.seed.clone(),
                    dir: (!super::reshape::is_root(dir, &self.root)).then(|| dir.to_string_lossy().into_owned()),
                    io_schema: self.shell.io_schema.clone(),
                    schema: self.shell.schema.clone(),
                    ids: parts.ids.clone(),
                    guard: RangeGuard::new(parts.spec.clone(), *range),
                    mode: MaintenanceMode::External,
                    watch: Some(board.watch(t.pid)?),
                    scan_only: true,
                    load_if_proven: false,
                })
                .await?;
                table.retire().await;
                copying.push(CopyTarget::new(*range, Arc::new(table)));
            }
            #[cfg(test)]
            reshape_hooks::hit(&self.root, "before_copy")?;
            copy(&sources, &mut copying).await?;
            for t in &copying {
                land(&t.table).await;
                t.table.close_exclusive(false).await;
            }
            #[cfg(test)]
            reshape_hooks::hit(&self.root, "before_commit")?;
            Ok(())
        }
        .await;
        // Test-only: the process "dies" here, before the commit, leaving the
        // open `RESHAPE_BEGIN` and the targets' files for the next open.
        #[cfg(test)]
        if copied.is_ok() && reshape_hooks::crash(&self.root, "crash_before_commit") {
            return Err((io::Error::other("injected crash before the commit"), ReshapeFiles::default()));
        }

        // 6. RESHAPE_COMMIT, durable: the commit point. Each target pinned at
        // the lowest pin of the sources it overlaps.
        let committed: io::Result<()> = match copied {
            Err(e) => Err(e),
            Ok(()) => {
                let pins: Vec<(u32, Option<u64>)> = targets
                    .iter()
                    .map(|(t, range, _)| {
                        let pin = source_dirs
                            .iter()
                            .filter(|(_, r, _)| (r.lo as u128) < range.hi && (range.lo as u128) < r.hi)
                            .filter_map(|(pid, _, _)| state.part(*pid).and_then(|p| p.pin))
                            .min();
                        (t.pid, pin)
                    })
                    .collect();
                let rec = Record::ReshapeCommit { id: begin.id, pins };
                let mut next = state.clone();
                match next.apply(&rec).map_err(invalid) {
                    Err(e) => Err(e),
                    Ok(()) => match board.log.append_durable(&rec).await {
                        Err(e) => Err(e),
                        Ok(()) => {
                            *state = next;
                            board.follow(&state);
                            Ok(())
                        }
                    },
                }
            }
        };
        match committed {
            Ok(()) => Ok(ReshapeFiles {
                doomed: source_dirs.into_iter().filter_map(|(pid, _, d)| d.map(|d| (pid, d))).collect(),
            }),
            Err(e) => {
                let rec = Record::ReshapeAbort { id: begin.id };
                let mut next = state.clone();
                if next.apply(&rec).is_ok() && board.log.append_durable(&rec).await.is_ok() {
                    *state = next;
                }
                undeal(&dealt);
                Err((e, target_files))
            }
        }
    }

    /// Records `pids`, retired by a committed reshape whose files are now
    /// deleted, as gone (`RETIRED_GONE`).
    pub(crate) async fn retired_gone(&self, pids: &[u32]) -> io::Result<()> {
        if pids.is_empty() {
            return Ok(());
        }
        let live = self.live()?;
        let mut state = live.parts.board.state.lock().await;
        let rec = Record::RetiredGone(pids.to_vec());
        let mut next = state.clone();
        next.apply(&rec).map_err(invalid)?;
        live.parts.board.log.append_durable(&rec).await?;
        *state = next;
        Ok(())
    }
}

/// The index of `dir` among `state`'s interned locations, interned by a
/// durable `LOCATIONS` first if it is new: the root is always location 0,
/// whatever path it was interned as (see `Parts::materialize_at`).
async fn intern_location(
    board: &PinBoard,
    state: &mut super::log::MapState,
    root: &Path,
    dir: &Path,
) -> io::Result<u32> {
    let key = files::dir_key(&dir.to_string_lossy());
    if key == files::dir_key(&root.to_string_lossy()) {
        return Ok(ROOT_LOC);
    }
    if let Some(i) = state.locations().iter().skip(1).position(|l| files::dir_key(l) == key) {
        return Ok(i as u32 + 1);
    }
    let mut paths = state.locations().to_vec();
    paths.push(key);
    let rec = Record::Locations(paths);
    let mut next = state.clone();
    next.apply(&rec).map_err(invalid)?;
    board.log.append_durable(&rec).await?;
    *state = next;
    Ok((state.locations().len() - 1) as u32)
}

/// Test-only failpoints inside a reshape: `arm(dir, point)` makes the
/// reshape of a table under `dir` fail at `point` ("before_copy",
/// "before_commit"), once.
#[cfg(test)]
pub(crate) mod reshape_hooks {
    use std::io;
    use std::path::{Path, PathBuf};

    static ARMED: parking_lot::Mutex<Vec<(PathBuf, &'static str)>> = parking_lot::Mutex::new(Vec::new());

    pub(crate) fn arm(dir: &Path, point: &'static str) {
        ARMED.lock().push((dir.to_path_buf(), point));
    }

    /// Whether the armed `point` is reached (and disarms it): a simulated
    /// crash, which the caller follows by doing nothing more.
    pub(crate) fn crash(root: &Path, point: &'static str) -> bool {
        hit(root, point).is_err()
    }

    pub(super) fn hit(root: &Path, point: &'static str) -> io::Result<()> {
        let mut armed = ARMED.lock();
        if let Some(i) = armed.iter().position(|(d, p)| root.starts_with(d) && *p == point) {
            armed.remove(i);
            return Err(io::Error::other(format!("injected reshape failure at {point}")));
        }
        Ok(())
    }
}

/// A routed partition as a fan-out plans it: its open instance, or `None`
/// while it is materialized and closed, for [`PartitionedTable::open_part`]
/// to open when the fan-out reaches it. Inline for the one a routed point
/// read reaches.
pub type Planned = SmallVec<[(u32, Option<Arc<Table<DbStorageProvider>>>); 1]>;

/// A closed partition's (rows, pointers, filters) lengths from its files'
/// metadata: each data file with its staged `.tmp`, which its next open
/// lands, and the index-store files. A missing data file is an error (I9);
/// a missing `.tmp` or index-store file counts 0.
fn closed_lens(root: &Path, uid: u64, pid: u32) -> io::Result<(u64, u64, u64)> {
    let len = |name: String, required: bool| -> io::Result<u64> {
        match std::fs::metadata(root.join(&name)) {
            Ok(m) => Ok(m.len()),
            Err(e) if e.kind() == io::ErrorKind::NotFound && !required => Ok(0),
            Err(e) => Err(io::Error::new(e.kind(), format!("partition {pid}'s file {name}: {e}"))),
        }
    };
    let [rows, pointers] = files::data_files(uid, pid);
    let stem = files::data_stem(uid, pid);
    Ok((
        len(rows.clone(), true)? + len(format!("{rows}.tmp"), false)?,
        len(pointers.clone(), true)? + len(format!("{pointers}.tmp"), false)?,
        len(format!("{stem}.ixs"), false)? + len(format!("{stem}.ixm"), false)?,
    ))
}

/// An open's failure for lack of file descriptors (EMFILE, ENFILE) named as
/// insufficient resources, which a client sees as SQLSTATE 53000, as
/// PostgreSQL reports running out of them; any other failure as it was. The
/// statement fails and the partition stays closed; the open budget that
/// evicts and retries first is P5b-2's (§7.1).
/// A close's hold on its slot from the moment it takes the instance out:
/// dropped at the close's end (or on any early return), it ends the slot's
/// `closing` mark if the close did not store final sizes, and releases the
/// budget's token.
struct Leaving<'a>(&'a Slot);

impl Drop for Leaving<'_> {
    fn drop(&mut self) {
        {
            let mut sizes = self.0.live().sizes.lock();
            if sizes.closing {
                sizes.reset(false);
            }
        }
        self.0.release_token();
    }
}

/// Partitions an open that ran out of descriptors closes before its retry.
const EVICT_ON_EMFILE: usize = 8;

/// Whether `e` is [`out_of_resources`]'s naming of EMFILE or ENFILE.
fn is_out_of_resources(e: &io::Error) -> bool {
    e.to_string().starts_with("insufficient resources: ")
}

fn out_of_resources(e: io::Error, table: &str, pid: u32) -> io::Error {
    match e.raw_os_error() {
        Some(files::EMFILE) | Some(files::ENFILE) => io::Error::new(
            e.kind(),
            format!("insufficient resources: partition {pid} of \"{table}\" could not be opened: {e}"),
        ),
        _ => e,
    }
}

/// A table instance dropped with partitions open (a restart, a schema
/// change's replaced instance, a `DROP`) drops them with it.
#[cfg(test)]
impl Drop for Parts {
    fn drop(&mut self) {
        for slot in self.slots.iter() {
            if slot.is_open() {
                instance_count::closed(&self.root, self.uid, slot.pid);
            }
        }
    }
}

impl Parts {
    fn slot_at(&self, pid: u32) -> io::Result<&Slot> {
        match self.index.get(pid as usize).and_then(|&i| self.slots.get(i as usize)) {
            Some(slot) if slot.pid == pid => Ok(slot),
            _ => Err(io::Error::other(format!("internal routing error: no slot for partition {pid}"))),
        }
    }

    /// The table's root directory.
    pub(crate) fn root_dir(&self) -> &Path {
        &self.root
    }

    /// A board round's share of trimming the budget's overrun
    /// (`Budget::trim`). Returns how many closed.
    pub(crate) async fn trim_budget(&self) -> usize {
        self.budget.trim(budget::TRIM_PER_ROUND).await
    }

    /// Whether a schema change replaced this instance, or a DROP removed the
    /// table: it opens and closes nothing any more.
    pub(crate) fn is_retired(&self) -> bool {
        self.retired.load(Ordering::SeqCst)
    }

    /// Whether partition `pid` is materialized.
    pub(crate) fn is_materialized(&self, pid: u32) -> bool {
        self.slot_at(pid).is_ok_and(|s| s.materialized.load(Ordering::SeqCst))
    }

    /// Partition `pid`'s open instance, if it is open now.
    pub(crate) fn open_now(&self, pid: u32) -> Option<Arc<Table<DbStorageProvider>>> {
        self.slot_at(pid).ok()?.open_now()
    }

    /// Materialized partition `pid`'s open instance, opened now if it is
    /// closed. `routed` sets its reference bit (a routed access); a
    /// fan-out's open leaves it clear (`partition::budget`).
    pub(crate) async fn open(&self, pid: u32, routed: bool) -> io::Result<Arc<Table<DbStorageProvider>>> {
        let slot = self.slot_at(pid)?;
        if routed {
            slot.touch();
        }
        if let Some(table) = slot.open_now() {
            if routed {
                table.ensure_full();
            }
            return Ok(table);
        }
        self.open_slow_erased(slot, false, routed).await
    }

    /// Clears partition `pid`'s reference bit; whether it was set. The
    /// CLOCK's hand, passing.
    pub(crate) fn take_reference(&self, pid: u32) -> bool {
        self.slot_at(pid).is_ok_and(|s| s.take_referenced())
    }

    /// [`Self::open_slow`] behind a type-erased box: an open runs once per
    /// closed partition reached, and every statement that may reach one
    /// awaits this, so the open's state machine (a whole table open) stays
    /// out of those statements' future types and out of the `Send` proof the
    /// deepest of them already strains (`index_store::gate::ticket` erases
    /// its slow path for the same reason).
    fn open_slow_erased<'a>(
        &'a self,
        slot: &'a Slot,
        materialize: bool,
        routed: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<Arc<Table<DbStorageProvider>>>> + Send + 'a>> {
        Box::pin(self.open_slow(slot, materialize, routed))
    }

    fn refuse_retired(&self) -> io::Result<()> {
        if self.retired.load(Ordering::SeqCst) {
            return Err(io::Error::other(format!(
                "partitioned table \"{}\" was changed or dropped while the statement ran; run it again",
                self.name
            )));
        }
        Ok(())
    }

    /// The slow path of an access to a closed (or, with `materialize`, an
    /// unmaterialized) partition, under its lifecycle lock: materialize it
    /// (§3.2) if it never was, then open it. A failure leaves the slot
    /// closed and fails the caller, never handing back a stand-in.
    async fn open_slow(&self, slot: &Slot, materialize: bool, routed: bool) -> io::Result<Arc<Table<DbStorageProvider>>> {
        let lock = self.board.lifecycle(slot.pid)?;
        let _life = lock.lock().await;
        if let Some(table) = slot.open_now() {
            return Ok(table);
        }
        self.refuse_retired()?;
        if !slot.materialized.load(Ordering::SeqCst) && !materialize {
            return Err(io::Error::other(format!(
                "internal routing error: partition {} is not materialized",
                slot.pid
            )));
        }
        // Room in the open budget first: partitions nobody holds closed by
        // the CLOCK, this one's own lifecycle lock skipped (held here). Soft:
        // with every open partition held, this opens anyway (`budget`).
        let me = (self as *const Parts as usize, slot.pid);
        self.budget.make_room(me).await;
        if !slot.materialized.load(Ordering::SeqCst) {
            self.materialize(slot).await?;
        }
        // A fan-out's (or a forget sweep's) open only reads: scan-only
        // (P5b-2b), upgraded by the first routed access or write.
        let scan_only = !routed;
        #[cfg(test)]
        let scan_only = scan_only && !scan_open_hooks::disabled(&self.root);
        let table = match self.open_instance(slot, scan_only).await {
            Ok(table) => table,
            // Out of descriptors whatever the budget said (plain tables,
            // connections, another process's share of the system's): close
            // some partitions nobody holds and try once more; then the
            // statement fails with `53000` (§7.1).
            Err(e) if is_out_of_resources(&e) => {
                let freed = self.budget.evict(EVICT_ON_EMFILE, me).await;
                if freed == 0 {
                    return Err(e);
                }
                ::log::warn!(
                    "partition {} of \"{}\" ran out of file descriptors opening; closed {freed} idle partitions and retried",
                    slot.pid,
                    self.name
                );
                self.open_instance(slot, scan_only).await?
            }
            Err(e) => return Err(e),
        };
        #[cfg(test)]
        instance_count::opened(&self.root, self.uid, slot.pid);
        slot.put(table.clone(), self.budget.enter(self.me.clone(), self.uid, slot.pid), routed);
        // Again once it is in its slot: a `SET CONFIG vacuum` that stored
        // its value after `open_instance` read it walked the slots before
        // this one was there (both sides store, then read the other's).
        self.apply_vacuum(&table)?;
        self.opens.fetch_add(1, Ordering::AcqRel);
        #[cfg(test)]
        crate::core::power_loss::failpoint::hit("test.p5b.reopened", &self.root)?;
        Ok(table)
    }

    /// §3.2's order, under the partition's lifecycle lock, so once per pid
    /// at a time; the flag is set once the record is durable and before the
    /// first row reaches the partition.
    async fn materialize(&self, slot: &Slot) -> io::Result<()> {
        // A mirror added after CREATE stops new partitions rather than let
        // them go unmirrored.
        refuse_mirrored((self.mirrored)())?;
        let pid = slot.pid;
        let recorded = {
            let state = self.board.state.lock().await;
            state.part(pid).map(|p| (p.location, state.locations().to_vec()))
        };
        match recorded {
            None => return Err(io::Error::other(format!("internal routing error: the map has no partition {pid}"))),
            // Recorded, its flag not set: never left so (the flag is set as
            // the record applies), but the files are this partition's, and a
            // second record would be refused at every later open.
            Some((Some(loc), locations)) => {
                let dir = partition_dir(&locations, loc, &*self.placement)
                    .map_err(|why| io::Error::other(format!("partition {pid} {why}")))?;
                files::require_existing(dir.as_deref().unwrap_or(&self.root), &files::data_files(self.uid, pid))
                    .map_err(|e| out_of_resources(e, &self.name, pid))?;
                if let Some(dir) = dir {
                    slot.set_dir(dir);
                }
            }
            Some((None, _)) => {
                // Checked again here, not only at open: deleting is the step
                // that cannot be undone.
                if let Some(why) = self.torn.as_deref() {
                    let generation = self.board.log.generation();
                    refuse_torn_leftovers(&self.root, self.uid, generation, pid, why)?;
                    for other in self.placement.others() {
                        refuse_torn_leftovers(&other, self.uid, generation, pid, why)?;
                    }
                }
                // The pool's dealer (§6.1), which counts it there at once;
                // taken back if anything below fails.
                let dealt = self.placement.deal(self.uid);
                let placed = self.materialize_at(slot, dealt.as_deref()).await;
                if placed.is_err()
                    && let Some(dir) = &dealt
                {
                    self.placement.undeal(self.uid, dir);
                }
                // The new pin, covered by the table's handle before the
                // first row (§7.2); sound since that row's writer is at
                // least `oldest_active`. After the undeal's decision: the
                // partition is recorded where it was dealt by now.
                self.board.materialized(pid, placed?)?;
            }
        }
        slot.materialized.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// [`Self::materialize`]'s steps once a directory is dealt (`None` or
    /// the root's own path for the root): the files there with their
    /// directory synced, the location interned by a durable `LOCATIONS` if
    /// the map does not name it yet, then the durable `MATERIALIZE` naming
    /// it (§3.2). A crash before `MATERIALIZE` leaves files the startup
    /// reaper deletes wherever they are, and an interned location nothing
    /// uses; recorded placement is never recomputed. Returns the recorded
    /// floor, for the caller's pin.
    async fn materialize_at(&self, slot: &Slot, dealt: Option<&str>) -> io::Result<u64> {
        let pid = slot.pid;
        let root_key = files::dir_key(&self.root.to_string_lossy());
        let off_root = dealt.map(files::dir_key).filter(|d| *d != root_key);
        let dir = off_root.as_ref().map(PathBuf::from);
        files::create_partition_files(dir.as_deref().unwrap_or(&self.root), self.uid, pid)
            .map_err(|e| out_of_resources(e, &self.name, pid))?;
        let floor = self.transactions.status().forgotten_below();
        let mut state = self.board.state.lock().await;
        let loc = match &off_root {
            None => ROOT_LOC,
            // Never location 0, whatever path it was interned as: it is the
            // root wherever the database now is, and a directory the root
            // was created in that is now another location is not it.
            Some(path) => match state.locations().iter().skip(1).position(|l| files::dir_key(l) == *path) {
                Some(i) => i as u32 + 1,
                None => {
                    // Interned first, and installed as soon as it is
                    // durable: the log holds it from then on whatever the
                    // `MATERIALIZE` below does, and a second `LOCATIONS`
                    // must extend the list the log has.
                    let mut paths = state.locations().to_vec();
                    paths.push(path.clone());
                    let rec = Record::Locations(paths);
                    let mut next = state.clone();
                    next.apply(&rec).map_err(invalid)?;
                    self.board.log.append_durable(&rec).await?;
                    *state = next;
                    (state.locations().len() - 1) as u32
                }
            },
        };
        let rec = Record::Materialize { pid, floor, loc };
        // Applied in place and put back if the append fails, as
        // `PinBoard::first_write` does with `DIRTY`: under the state lock
        // nobody sees the state in between. A copy of the whole state to
        // apply to made every first write cost the declared count (13 ms a
        // partition at 2²⁰ declared, 0.8 ms at 2¹⁶; plan item 4).
        let saved = *state
            .part(pid)
            .ok_or_else(|| io::Error::other(format!("internal routing error: the map has no partition {pid}")))?;
        state.apply(&rec).map_err(invalid)?;
        #[cfg(test)]
        let injected = materialize_hooks::fails(&self.root);
        #[cfg(not(test))]
        let injected = false;
        // Durable before any row reaches the partition's files, once per
        // partition. Left to the rows' own sync, a crash could lose the
        // record while writeback kept row pages in the new files, and with
        // the map's tail torn the open refuses the whole table for
        // unexplained data (`refuse_torn_leftovers`).
        let appended = if injected {
            Err(io::Error::other("injected MATERIALIZE append failure"))
        } else {
            self.board.log.append_durable(&rec).await
        };
        if let Err(e) = appended {
            state.restore_part(pid, saved);
            return Err(e);
        }
        if let Some(dir) = dir {
            slot.set_dir(dir);
        }
        Ok(floor)
    }

    /// Opens a slot's partition over files that exist, `External` (its
    /// maintenance the scheduler's, its pin the slot's), under this
    /// instance's schema. The caller holds the lifecycle lock.
    async fn open_instance(&self, slot: &Slot, scan_only: bool) -> io::Result<Arc<Table<DbStorageProvider>>> {
        // A scan-only open loads a proven snapshot anyway while the budget
        // is under half full: the partition is likely to stay open for the
        // routed reads after the fan-out, and a load is cheap (P5b-2b review).
        // Only where routed traffic reached it before, in this process
        // (`Slot::routed`): a fan-out over thousands of partitions only
        // fan-outs read would otherwise load every one of their indexes for
        // routed reads that never come (P5b-3).
        let load_if_proven =
            scan_only && slot.routed_now() && self.budget.open() * 2 < self.budget.limit();
        #[cfg(test)]
        let load_if_proven = load_if_proven && !scan_open_hooks::loads_deferred(&self.root);
        // Never a provider over a missing file, which it would create (I9).
        // Out of descriptors here is insufficient resources too: the check
        // opens the files it checks.
        files::require_existing(slot.dir(&self.root), &files::data_files(self.uid, slot.pid))
            .map_err(|e| out_of_resources(e, &self.name, slot.pid))?;
        let physical = files::data_stem(self.uid, slot.pid);
        let table = Table::open_partition(PartitionOpen {
            cache_scope: format!("{}\u{0}{}", self.db_id, physical),
            physical,
            seed: self.seed.clone(),
            dir: slot.dir_get().map(|d| d.to_string_lossy().into_owned()),
            io_schema: self.shell.io_schema.clone(),
            schema: self.shell.schema.clone(),
            ids: self.ids.clone(),
            guard: RangeGuard::new(self.spec.clone(), slot.range),
            mode: MaintenanceMode::External,
            watch: Some(self.board.watch(slot.pid)?.clone()),
            scan_only,
            load_if_proven,
        })
        .await
        .map_err(|e| out_of_resources(e, &self.name, slot.pid))?;
        table.enable_versioning(self.transactions.clone());
        self.apply_vacuum(&table)?;
        #[cfg(test)]
        lifecycle_hooks::opened(&self.root, slot.pid).await;
        Ok(Arc::new(table))
    }

    /// [`Self::close`], for a forget round that opened `pid` for its sweep,
    /// type-erased as [`Self::open_slow_erased`] is: a sweep is reached from
    /// statement futures too (`Table::neutralize_aborted_now`).
    pub(crate) fn close_after_sweep(
        &self,
        pid: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<Close>> + Send + '_>> {
        Box::pin(self.close(pid))
    }

    fn apply_vacuum(&self, table: &Table<DbStorageProvider>) -> io::Result<()> {
        match self.vacuum.load(Ordering::SeqCst) {
            1 => table.set_vacuum_enabled(true),
            2 => table.set_vacuum_enabled(false),
            _ => Ok(()),
        }
    }

    /// The exclusive close of partition `pid` (§7.2 point 2), under its
    /// lifecycle lock. Taken out of the slot only while the slot's is the
    /// one strong reference (statements and jobs hold clones, and none can
    /// take a new one without this slot's lock) and no ticket is counted on
    /// its write gate: a transaction that wrote it keeps one until its
    /// outcome is final (persistent indexes §2), and that ticket must stay
    /// on the word the next seal of this file set reads. Then, owned alone:
    /// retired (maintenance stopped and joined, seals cancelled), its unit's
    /// round in flight waited out, its staged appends landed, dropped. An
    /// instance a schema change already retired is only dropped: what it had
    /// staged is the new instance's to land.
    async fn close(&self, pid: u32) -> io::Result<Close> {
        self.close_detached(pid, true).await
    }

    /// The close's body on a task of its own, which the caller joins: a
    /// close runs inside statements now (an eviction, a failed fan-out's
    /// cleanup), and its seal's file steps are blocking tasks that outlive a
    /// dropped future, so a caller dropped mid-close (a cancelled statement,
    /// a disconnect) must not release the lifecycle lock while a `.ixs`
    /// publish or a `CLEAN` append is still running: a reopen could then see
    /// that `CLEAN` after its own `DIRTY` (review A). The task holds the lock
    /// to the close's end, whatever happens to its caller. `wait` false
    /// takes the lock only if it is free ([`Self::close_for_budget`]).
    async fn close_detached(&self, pid: u32, wait: bool) -> io::Result<Close> {
        let Some(me) = self.me.upgrade() else {
            return self.close_body(pid, wait).await;
        };
        match tokio::spawn(async move { me.close_body(pid, wait).await }).await {
            Ok(closed) => closed,
            Err(e) => Err(io::Error::other(format!("closing partition {pid} of \"{}\" failed: {e}", self.name))),
        }
    }

    fn close_body(
        &self,
        pid: u32,
        wait: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<Close>> + Send + '_>> {
        Box::pin(async move {
            let slot = self.slot_at(pid)?;
            let lock = self.board.lifecycle(pid)?;
            let _life = if wait {
                lock.lock().await
            } else {
                match lock.try_lock() {
                    Ok(guard) => guard,
                    Err(_) => return Ok(Close::InUse),
                }
            };
            self.close_held(slot, pid).await
        })
    }

    /// [`Self::close`] for the open budget and the idle round, which never
    /// wait for a lifecycle lock: an open or a close of the partition in
    /// flight means it is in use, and waiting could deadlock against an open
    /// that holds its own lock while it makes room. Whether it closed.
    pub(crate) async fn close_for_budget(&self, pid: u32) -> bool {
        match self.close_detached(pid, false).await {
            Ok(Close::Closed) => true,
            Ok(_) => false,
            Err(e) => {
                ::log::warn!("closing partition {pid} of \"{}\" for the open budget failed: {e}", self.name);
                false
            }
        }
    }

    /// One idle round (§7.2, P5b-2), at most once per
    /// `partition.idle_close_secs`: every open partition no routed access
    /// touched since the last round closes, if nobody holds it; the rest
    /// have their reference bits cleared for the next. Returns how many
    /// closed. A board round calls it.
    pub(crate) async fn close_idle_round(&self) -> usize {
        let secs = self.budget.idle_close_secs();
        if secs == 0 || self.retired.load(Ordering::SeqCst) {
            return 0;
        }
        {
            let mut at = self.idle_round_at.lock();
            let now = std::time::Instant::now();
            match *at {
                // The first round only starts the clock.
                None => {
                    *at = Some(now);
                    return 0;
                }
                Some(last) if now.duration_since(last) < std::time::Duration::from_secs(secs) => return 0,
                Some(_) => *at = Some(now),
            }
        }
        let mut closed = 0;
        for slot in self.slots.iter() {
            if !slot.is_open() || slot.take_referenced() {
                continue;
            }
            if self.close_for_budget(slot.pid).await {
                closed += 1;
            }
        }
        closed
    }

    /// The exclusive close's body, the lifecycle lock held by the caller.
    async fn close_held(&self, slot: &Slot, pid: u32) -> io::Result<Close> {
        if self.retired.load(Ordering::SeqCst) {
            return Ok(Close::Retired);
        }
        let table = {
            let mut open = slot.live().open.lock();
            match open.as_ref() {
                None => return Ok(Close::NotOpen),
                Some(t) if Arc::strong_count(t) > 1 => return Ok(Close::InUse),
                Some(t) if t.gate().word().load().count() > 0 => return Ok(Close::InUse),
                Some(_) => match open.take() {
                    Some(t) => {
                        // Readers from here to the end read the files and
                        // keep nothing (`KeptSizes`).
                        slot.live().sizes.lock().reset(true);
                        t
                    }
                    None => return Ok(Close::NotOpen),
                },
            }
        };
        // However the close ends from here, the slot stops reading as
        // closing and the budget's token goes, once the instance is gone.
        let leaving = Leaving(slot);
        #[cfg(test)]
        lifecycle_hooks::closing(&self.root, pid).await;
        #[cfg(test)]
        crate::core::power_loss::failpoint::hit("test.p5b.close_taken", &self.root)?;
        // Sealed as it closes (§7.2 point 3): the snapshot the next open
        // loads instead of rebuilding, written only when the instance has
        // something the file set's snapshot lacks (`index_store::seal`'s
        // step 0); then retired and landed.
        table.close_exclusive(true).await;
        #[cfg(test)]
        crate::core::power_loss::failpoint::hit("test.p5b.close_landed", &self.root)?;
        // Its sizes as it leaves, what the catalogs read while it is closed,
        // stored before the lifecycle lock is released.
        let final_sizes = PartitionedTable::file_lens(&table).await.ok();
        {
            let mut sizes = slot.live().sizes.lock();
            sizes.reset(false);
            sizes.kept = final_sizes;
        }
        // Owned alone since the check above; dropping it drops the providers
        // (the scheduler holds the unit weakly).
        #[cfg(test)]
        let table = close_leak_hooks::maybe_leak(&self.root, pid, table);
        if let Err(still) = Arc::try_unwrap(table) {
            ::log::error!(
                "partition {pid} of \"{}\" was referenced again while it closed ({} references)",
                self.name,
                Arc::strong_count(&still)
            );
            #[cfg(test)]
            instance_count::leaked(&self.root, self.uid, pid);
            // Still counted in the budget for as long as it lives, with its
            // descriptors (review A round 2, N1): the token goes with the
            // last reference, which a task waits for.
            let token = slot.live().token.lock().take();
            tokio::spawn(async move {
                while Arc::strong_count(&still) > 1 {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                drop(still);
                drop(token);
            });
        }
        #[cfg(test)]
        instance_count::closed(&self.root, self.uid, pid);
        // Counted closed in the budget only now, once its descriptors are
        // gone (review A: released before the seal, the budget counted it
        // closed while it still held them).
        drop(leaving);
        self.closes.fetch_add(1, Ordering::AcqRel);
        Ok(Close::Closed)
    }
}

/// Test-only: a pause in a catalog's read of a closed partition's sizes,
/// after it read the files and before it keeps what it read.
#[cfg(test)]
pub(crate) mod kept_sizes_hooks {
    use std::path::{Path, PathBuf};
    use std::sync::mpsc::{Receiver, Sender};

    type Park = (PathBuf, u32, Sender<()>, Receiver<()>);
    static PARKS: parking_lot::Mutex<Vec<Park>> = parking_lot::Mutex::new(Vec::new());

    /// The next read of partition `pid`'s sizes under `dir` signals the
    /// returned receiver, then blocks until the returned sender sends.
    pub(crate) fn park(dir: &Path, pid: u32) -> (Receiver<()>, Sender<()>) {
        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        PARKS.lock().push((dir.to_path_buf(), pid, reached_tx, resume_rx));
        (reached_rx, resume_tx)
    }

    pub(super) fn read(root: &Path, pid: u32) {
        let found = {
            let mut parks = PARKS.lock();
            parks.iter().position(|(d, p, _, _)| root.starts_with(d) && *p == pid).map(|at| parks.remove(at))
        };
        if let Some((_, _, reached, resume)) = found {
            let _ = reached.send(());
            let _ = resume.recv_timeout(std::time::Duration::from_secs(30));
        }
    }
}

/// Test-only: fan-out opens made full again under a directory, as before
/// P5b-2b, for the measurement's before and the tests' baselines.
#[cfg(test)]
pub(crate) mod scan_open_hooks {
    use std::path::{Path, PathBuf};

    static DISABLED: parking_lot::Mutex<Vec<PathBuf>> = parking_lot::Mutex::new(Vec::new());

    pub(crate) fn disable(dir: &Path) {
        DISABLED.lock().push(dir.to_path_buf());
    }

    pub(crate) fn enable(dir: &Path) {
        DISABLED.lock().retain(|d| d != dir);
    }

    pub(super) fn disabled(root: &Path) -> bool {
        DISABLED.lock().iter().any(|d| root.starts_with(d))
    }

    static LOADS_DEFERRED: parking_lot::Mutex<Vec<PathBuf>> = parking_lot::Mutex::new(Vec::new());

    /// Scan-only opens under `dir` defer a proven load too, whatever the
    /// budget's room: the tests of deferral itself.
    pub(crate) fn defer_loads(dir: &Path) {
        LOADS_DEFERRED.lock().push(dir.to_path_buf());
    }

    pub(super) fn loads_deferred(root: &Path) -> bool {
        LOADS_DEFERRED.lock().iter().any(|d| root.starts_with(d))
    }
}

/// Test-only: a close that finds its instance referenced again as it
/// drops it (never reached otherwise): the hook keeps a clone, which the
/// test drops later.
#[cfg(test)]
pub(crate) mod close_leak_hooks {
    use std::path::{Path, PathBuf};

    use rclite::Arc;

    use super::{DbStorageProvider, Table};

    type Kept = (PathBuf, u32, Option<Arc<Table<DbStorageProvider>>>);
    static KEEP: parking_lot::Mutex<Vec<Kept>> = parking_lot::Mutex::new(Vec::new());

    /// The next close of `pid` under `dir` keeps a reference to its instance
    /// past its drop, until [`release`].
    pub(crate) fn keep(dir: &Path, pid: u32) {
        KEEP.lock().push((dir.to_path_buf(), pid, None));
    }

    /// Drops the reference [`keep`] kept.
    pub(crate) fn release(dir: &Path) {
        KEEP.lock().retain(|(d, _, _)| d != dir);
    }

    pub(super) fn maybe_leak(root: &Path, pid: u32, table: Arc<Table<DbStorageProvider>>) -> Arc<Table<DbStorageProvider>> {
        let mut keep = KEEP.lock();
        if let Some(entry) = keep.iter_mut().find(|(d, p, kept)| root.starts_with(d) && *p == pid && kept.is_none()) {
            entry.2 = Some(table.clone());
        }
        table
    }
}

/// Test-only: live instances per file set, as `Parts` puts them in a slot
/// and drops them at close (open count; a second open while one is live, or
/// an instance still referenced as its close drops it, is a violation). I14,
/// as the budget storm checks it. Instances a schema change reopens are not
/// counted (the replaced one stays alive, retired, beside it by design).
#[cfg(test)]
pub(crate) mod instance_count {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};

    type Key = (PathBuf, u64, u32);
    static LIVE: parking_lot::Mutex<Option<HashMap<Key, i64>>> = parking_lot::Mutex::new(None);
    static VIOLATIONS: parking_lot::Mutex<Vec<(PathBuf, String)>> = parking_lot::Mutex::new(Vec::new());

    pub(super) fn opened(root: &Path, uid: u64, pid: u32) {
        let mut live = LIVE.lock();
        let n = live.get_or_insert_with(HashMap::new).entry((root.to_path_buf(), uid, pid)).or_insert(0);
        *n += 1;
        if *n > 1 {
            VIOLATIONS.lock().push((root.to_path_buf(), format!("partition {pid}: {n} live instances at an open")));
        }
    }

    /// A schema change's reopen: counted, never a violation (the replaced
    /// instance stays alive, retired, until its table instance drops).
    pub(super) fn reopened(root: &Path, uid: u64, pid: u32) {
        *LIVE.lock().get_or_insert_with(HashMap::new).entry((root.to_path_buf(), uid, pid)).or_insert(0) += 1;
    }

    pub(super) fn closed(root: &Path, uid: u64, pid: u32) {
        if let Some(n) = LIVE.lock().get_or_insert_with(HashMap::new).get_mut(&(root.to_path_buf(), uid, pid)) {
            *n -= 1;
        }
    }

    pub(super) fn leaked(root: &Path, uid: u64, pid: u32) {
        let _ = uid;
        VIOLATIONS.lock().push((root.to_path_buf(), format!("partition {pid}: still referenced as its close dropped it")));
    }

    /// The violations recorded under `dir`.
    pub(crate) fn violations(dir: &Path) -> Vec<String> {
        VIOLATIONS.lock().iter().filter(|(d, _)| d.starts_with(dir)).map(|(_, v)| v.clone()).collect()
    }
}

/// Test-only: a failing `MATERIALIZE` append, per directory.
#[cfg(test)]
pub(crate) mod materialize_hooks {
    use std::path::{Path, PathBuf};

    static FAILING: parking_lot::Mutex<Vec<PathBuf>> = parking_lot::Mutex::new(Vec::new());

    /// The next `MATERIALIZE` append of a table rooted under `dir` fails,
    /// as a full disk would, after the record is applied in memory.
    pub(crate) fn fail_next(dir: &Path) {
        FAILING.lock().push(dir.to_path_buf());
    }

    pub(super) fn fails(root: &Path) -> bool {
        let mut failing = FAILING.lock();
        match failing.iter().position(|d| root.starts_with(d)) {
            Some(i) => {
                failing.remove(i);
                true
            }
            None => false,
        }
    }
}

/// Test-only parks inside an open and a close, per directory, so a test can
/// hold one between its lifecycle lock and its end and race the rest
/// against it.
#[cfg(test)]
pub(crate) mod lifecycle_hooks {
    use std::path::{Path, PathBuf};
    use std::sync::Arc as StdArc;

    use tokio::sync::Notify;

    type Park = (PathBuf, u32, StdArc<Notify>, StdArc<Notify>);
    static OPENS: parking_lot::Mutex<Vec<Park>> = parking_lot::Mutex::new(Vec::new());
    static CLOSES: parking_lot::Mutex<Vec<Park>> = parking_lot::Mutex::new(Vec::new());

    fn arm(list: &parking_lot::Mutex<Vec<Park>>, dir: &Path, pid: u32) -> (StdArc<Notify>, StdArc<Notify>) {
        let (reached, resume) = (StdArc::new(Notify::new()), StdArc::new(Notify::new()));
        list.lock().push((dir.to_path_buf(), pid, reached.clone(), resume.clone()));
        (reached, resume)
    }

    /// The next open of partition `pid` under `dir` waits, once the
    /// instance is built and before it is put in its slot, until the second
    /// `Notify` is signalled; the first is signalled when it gets there.
    pub(crate) fn park_open(dir: &Path, pid: u32) -> (StdArc<Notify>, StdArc<Notify>) {
        arm(&OPENS, dir, pid)
    }

    /// The next close of partition `pid` under `dir` waits, once it has
    /// taken the instance out of its slot and before it retires it.
    pub(crate) fn park_close(dir: &Path, pid: u32) -> (StdArc<Notify>, StdArc<Notify>) {
        arm(&CLOSES, dir, pid)
    }

    async fn hit(list: &parking_lot::Mutex<Vec<Park>>, root: &Path, pid: u32) {
        let found = {
            let mut l = list.lock();
            let at = l.iter().position(|(d, p, _, _)| root.starts_with(d) && *p == pid);
            at.map(|i| l.remove(i))
        };
        if let Some((_, _, reached, resume)) = found {
            reached.notify_one();
            resume.notified().await;
        }
    }

    pub(super) async fn opened(root: &Path, pid: u32) {
        hit(&OPENS, root, pid).await
    }

    pub(super) async fn closing(root: &Path, pid: u32) {
        hit(&CLOSES, root, pid).await
    }
}
