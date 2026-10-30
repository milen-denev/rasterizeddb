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
//! - one **slot** per partition, whose `OnceCell` holds the open `Table` once
//!   the partition is materialized. An empty cell is proof the partition has
//!   no rows (§4.3): the cell is set before the first row is written, so a
//!   reader whose snapshot sees a committed row, and which reads the cell
//!   after taking that snapshot, finds it set;
//! - the **map log** (§3.2), which replays to the state the slots mirror;
//! - the **row-id source** every partition draws from (§3.5, I12).
//!
//! # Materializing (§3.2)
//!
//! A partition's first write, inside its slot's `OnceCell` initialisation so
//! one runs per pid: delete any leftover files and create the three files
//! exclusively (one directory fsync); append `MATERIALIZE` with its WAL record
//! issued at append time, in the root log the first row's outcome will follow,
//! and sync that log (one fsync per partition, ever: otherwise a crash could
//! keep the first rows' pages without the record, which the open refuses as
//! unexplained data under a torn map); open the partition and register it for versioning; set the cell. Leftover
//! files are never reused, and a file with no `MATERIALIZE` is never read.
//! While the map log is torn, a non-empty leftover is refused instead, at
//! open and at materialize: the lost tail may have been its `MATERIALIZE`.
//!
//! # Opening
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
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use arc_swap::ArcSwap;
use rclite::Arc;
use smallvec::SmallVec;

use super::files::{self, MapLogFile};
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

/// Partitions a `PARTITION BY HASH` table gets when it names no count. 64
/// while every materialized partition stays open (phase 2a); 1024 once phase
/// 3's lazy open lands.
pub const DEFAULT_PARTITIONS: u32 = 64;

/// The most partitions one database may declare across all its tables while
/// every materialized partition is opened at startup (§7.1). Per database,
/// not per process: a refusal must depend only on replicated state (§12).
pub const MAX_DECLARED_PER_DATABASE: u32 = 2048;

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
    /// The open instance; `None` while the partition is closed or
    /// unmaterialized (phase 3 P5b: partitions open on demand). Every access
    /// clones it under this lock; it is put and taken only while the
    /// partition's lifecycle lock is held too (`PinBoard::lifecycle`), so a
    /// close that finds the strong count at 1 here holds the only reference
    /// anyone can reach.
    open: parking_lot::Mutex<Option<Arc<Table<DbStorageProvider>>>>,
}

impl Slot {
    fn new(pid: u32, range: PointRange, materialized: bool) -> Self {
        Slot { pid, range, materialized: AtomicBool::new(materialized), open: parking_lot::Mutex::new(None) }
    }

    fn open_now(&self) -> Option<Arc<Table<DbStorageProvider>>> {
        self.open.lock().clone()
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
    /// By pid; phase 2a pids are `0..n`.
    slots: Box<[Slot]>,
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

    /// Opens the table `uid`: replays its map log and opens every
    /// materialized partition. A failure leaves it broken, never panics.
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
        let map = state.map().clone();
        *declared = Some(map.len() as u32);

        if state.header().uid != uid {
            return Err(format!("its map log belongs to table {:016x}", state.header().uid));
        }
        if state.dropped() {
            return Err("its map log says it was dropped".to_string());
        }
        let dense = map.entries().iter().enumerate().all(|(i, &(_, pid))| pid as usize == i);
        if !dense
            || state.next_pid() != map.len() as u64
            || state.reshapes().next().is_some()
            || state.parts().any(|(_, p)| p.retired)
        {
            return Err("its map has been reshaped, which this version cannot open".to_string());
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

        let entries = map.entries();
        let mut materialized = vec![false; entries.len()];
        for (pid, part) in state.parts() {
            let Some(loc) = part.location else {
                // §4.3's skip is a proof only while the log is whole.
                if let Some(why) = torn.as_deref() {
                    refuse_torn_leftovers(root, uid, generation, pid, why).map_err(e)?;
                }
                continue;
            };
            if loc != ROOT_LOC {
                return Err(format!("partition {pid} is at location {loc}, outside the root"));
            }
            // Nothing opens here (phase 3 P5b: a partition opens on demand),
            // but a materialized partition's missing file still refuses the
            // table now, as it did when every partition opened (I9): a stat
            // per file, never a provider, which would create it.
            files::require_existing(root, &files::data_files(uid, pid)).map_err(e)?;
            if let Some(m) = materialized.get_mut(pid as usize) {
                *m = true;
            }
        }
        let slots: Box<[Slot]> = entries
            .iter()
            .enumerate()
            .map(|(j, &(lo, pid))| {
                let range = PointRange { lo, hi: entries.get(j + 1).map_or(SPACE_END, |&(next, _)| next as u128) };
                Slot::new(pid, range, materialized.get(pid as usize).copied().unwrap_or(false))
            })
            .collect();
        let parts = StdArc::new(Parts {
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
            mirrored: ctx.mirrored.clone(),
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
                let Ok(lock) = live.parts.board.lifecycle(slot.pid) else { continue };
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
                    let slots: Box<[Slot]> = old
                        .slots
                        .iter()
                        .map(|s| Slot::new(s.pid, s.range, s.materialized.load(Ordering::SeqCst)))
                        .collect();
                    let parts = StdArc::new(Parts {
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
                        mirrored: self.mirrored.clone(),
                        torn: self.torn.clone(),
                        retired: AtomicBool::new(false),
                        vacuum: AtomicU8::new(old.vacuum.load(Ordering::Acquire)),
                        opens: AtomicU64::new(0),
                        closes: AtomicU64::new(0),
                    });
                    let next = Live {
                        router: RangeGuard::new(spec.clone(), PointRange::FULL),
                        spec,
                        epoch: ArcSwap::new(live.epoch.load_full()),
                        parts,
                        scheduler: live.scheduler.clone(),
                    };
                    // The partitions open in the old instance open again now,
                    // under the new schema, so a partition that no longer
                    // opens fails the change as at a start; a closed one stays
                    // closed and opens under the new schema on demand. Each
                    // under its lifecycle lock: the old instance is retired,
                    // so no close of it runs from here on.
                    for (old_slot, new) in old.slots.iter().zip(next.parts.slots.iter()) {
                        let lock = next.parts.board.lifecycle(new.pid).map_err(|e| e.to_string())?;
                        let _life = lock.lock().await;
                        if old_slot.open_now().is_none() {
                            continue;
                        }
                        let table = next.parts.open_instance(new).await.map_err(|err| format!("partition {}: {err}", new.pid))?;
                        *new.open.lock() = Some(table);
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
            declared: self.declared,
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
        self.live().map_or(0, |live| live.parts.slots.iter().filter(|s| s.open.lock().is_some()).count())
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
    pub(crate) fn rows_len(&self) -> io::Result<u64> {
        let live = self.live()?;
        let mut total = 0u64;
        for slot in live.parts.slots.iter() {
            match slot.open_now() {
                Some(table) => total += table.data()?.len.load(Ordering::Relaxed),
                None if slot.materialized.load(Ordering::SeqCst) => {
                    total += closed_lens(&self.root, self.uid, slot.pid)?.0;
                }
                None => {}
            }
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
        let mut out = Vec::with_capacity(planned.len());
        for (pid, open) in planned {
            let table = match open {
                Some(table) => table,
                None => self.open_part(pid).await?,
            };
            out.push((pid, table));
        }
        Ok(out)
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
        Ok(out)
    }

    /// Materialized partition `pid`'s open instance, opened now if it is
    /// closed. Errs for a partition that is not materialized, and when the
    /// open fails: never an empty stand-in.
    pub async fn open_part(&self, pid: u32) -> io::Result<Arc<Table<DbStorageProvider>>> {
        let live = self.live()?;
        live.parts.open(pid).await
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
            if slot.open.lock().is_some() && live.parts.close(slot.pid).await? == Close::Closed {
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
            out.push((pid, Some(table)));
        } else if slot.materialized.load(Ordering::SeqCst) {
            out.push((pid, None));
        }
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
        if let Some(table) = slot.open_now() {
            return Ok(table);
        }
        live.parts.open_slow_erased(slot, true).await
    }

    /// Inserts `row` under `tx` into the partition its key routes to,
    /// materializing that partition first if need be. The partition's guard
    /// checks the row's point again (I4).
    pub async fn insert_row_in_txn(&self, row: RowWrite, tx: TxId) -> io::Result<()> {
        let live = self.live()?;
        let point = live.router.point_of_write(&row).map_err(io::Error::other)?;
        self.partition_for_point(point).await?.insert_row_in_txn(row, tx).await
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
        for slot in live.parts.slots.iter() {
            let lens = match slot.open_now() {
                Some(table) => Some(Self::file_lens(&table).await?),
                // A closed partition, from its files' metadata.
                None if slot.materialized.load(Ordering::SeqCst) => Some(closed_lens(&self.root, self.uid, slot.pid)?),
                None => None,
            };
            if let Some((rows, pointers, filters)) = lens {
                t.materialized += 1;
                t.slots += pointers / ROW_POINTER_RECORD_LEN as u64;
                t.data += rows;
                t.pointers += pointers;
                t.filters += filters;
            }
        }
        t.bytes = t.data + t.pointers + t.filters + t.schema + t.map + t.ids;
        Ok(t)
    }

    /// The map header's routing version (§4.1), as replayed.
    pub async fn route_v(&self) -> io::Result<u16> {
        Ok(self.live()?.parts.board.state.lock().await.header().route_v)
    }

    /// Every declared partition, by pid.
    pub async fn partitions(&self) -> io::Result<Vec<PartitionInfo>> {
        let live = self.live()?;
        let root = self.root.to_string_lossy().into_owned();
        let mut out = Vec::with_capacity(live.parts.slots.len());
        for slot in live.parts.slots.iter() {
            let (location, state, slots, bytes) = match slot.open_now() {
                Some(table) => {
                    let (slots, bytes) = Self::sizes(&table).await?;
                    (Some(root.clone()), "materialized", slots, bytes)
                }
                None if slot.materialized.load(Ordering::SeqCst) => {
                    let (rows, pointers, filters) = closed_lens(&self.root, self.uid, slot.pid)?;
                    (Some(root.clone()), "materialized", pointers / ROW_POINTER_RECORD_LEN as u64, rows + pointers + filters)
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
                if let Ok(lock) = live.parts.board.lifecycle(slot.pid) {
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

impl Parts {
    fn slot_at(&self, pid: u32) -> io::Result<&Slot> {
        match self.slots.get(pid as usize) {
            Some(slot) if slot.pid == pid => Ok(slot),
            _ => Err(io::Error::other(format!("internal routing error: no slot for partition {pid}"))),
        }
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
    /// closed.
    pub(crate) async fn open(&self, pid: u32) -> io::Result<Arc<Table<DbStorageProvider>>> {
        let slot = self.slot_at(pid)?;
        if let Some(table) = slot.open_now() {
            return Ok(table);
        }
        self.open_slow_erased(slot, false).await
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
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<Arc<Table<DbStorageProvider>>>> + Send + 'a>> {
        Box::pin(self.open_slow(slot, materialize))
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
    async fn open_slow(&self, slot: &Slot, materialize: bool) -> io::Result<Arc<Table<DbStorageProvider>>> {
        let lock = self.board.lifecycle(slot.pid)?;
        let _life = lock.lock().await;
        if let Some(table) = slot.open_now() {
            return Ok(table);
        }
        self.refuse_retired()?;
        if !slot.materialized.load(Ordering::SeqCst) {
            if !materialize {
                return Err(io::Error::other(format!(
                    "internal routing error: partition {} is not materialized",
                    slot.pid
                )));
            }
            self.materialize(slot).await?;
        }
        let table = self.open_instance(slot).await?;
        *slot.open.lock() = Some(table.clone());
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
        let recorded = self.board.state.lock().await.part(pid).map(|p| p.location);
        match recorded {
            None => return Err(io::Error::other(format!("internal routing error: the map has no partition {pid}"))),
            // Recorded, its flag not set: never left so (the flag is set as
            // the record applies), but the files are this partition's, and a
            // second record would be refused at every later open.
            Some(Some(loc)) => {
                if loc != ROOT_LOC {
                    return Err(io::Error::other(format!("partition {pid} is at location {loc}, outside the root")));
                }
                files::require_existing(&self.root, &files::data_files(self.uid, pid))?;
            }
            Some(None) => {
                // Checked again here, not only at open: deleting is the step
                // that cannot be undone.
                if let Some(why) = self.torn.as_deref() {
                    refuse_torn_leftovers(&self.root, self.uid, self.board.log.generation(), pid, why)?;
                }
                files::create_partition_files(&self.root, self.uid, pid)?;
                let floor = self.transactions.status().forgotten_below();
                let rec = Record::Materialize { pid, floor, loc: ROOT_LOC };
                let mut state = self.board.state.lock().await;
                let mut next = state.clone();
                next.apply(&rec).map_err(invalid)?;
                // Durable before any row reaches the partition's files, once
                // per partition. Left to the rows' own sync, a crash could
                // lose the record while writeback kept row pages in the new
                // files, and with the map's tail torn the open refuses the
                // whole table for unexplained data (`refuse_torn_leftovers`).
                self.board.log.append_durable(&rec).await?;
                *state = next;
                // The new pin, covered by the table's handle before the
                // first row (§7.2); sound since that row's writer is at
                // least `oldest_active`.
                self.board.materialized(pid, floor)?;
            }
        }
        slot.materialized.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Opens a slot's partition over files that exist, `External` (its
    /// maintenance the scheduler's, its pin the slot's), under this
    /// instance's schema. The caller holds the lifecycle lock.
    async fn open_instance(&self, slot: &Slot) -> io::Result<Arc<Table<DbStorageProvider>>> {
        // Never a provider over a missing file, which it would create (I9).
        files::require_existing(&self.root, &files::data_files(self.uid, slot.pid))?;
        let physical = files::data_stem(self.uid, slot.pid);
        let table = Table::open_partition(PartitionOpen {
            cache_scope: format!("{}\u{0}{}", self.db_id, physical),
            physical,
            seed: self.seed.clone(),
            io_schema: self.shell.io_schema.clone(),
            schema: self.shell.schema.clone(),
            ids: self.ids.clone(),
            guard: RangeGuard::new(self.spec.clone(), slot.range),
            mode: MaintenanceMode::External,
            watch: Some(self.board.watch(slot.pid)?.clone()),
        })
        .await?;
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
        let slot = self.slot_at(pid)?;
        let lock = self.board.lifecycle(pid)?;
        let _life = lock.lock().await;
        if self.retired.load(Ordering::SeqCst) {
            return Ok(Close::Retired);
        }
        let table = {
            let mut open = slot.open.lock();
            match open.as_ref() {
                None => return Ok(Close::NotOpen),
                Some(t) if Arc::strong_count(t) > 1 => return Ok(Close::InUse),
                Some(t) if t.gate().word().load().count() > 0 => return Ok(Close::InUse),
                Some(_) => match open.take() {
                    Some(t) => t,
                    None => return Ok(Close::NotOpen),
                },
            }
        };
        #[cfg(test)]
        lifecycle_hooks::closing(&self.root, pid).await;
        #[cfg(test)]
        crate::core::power_loss::failpoint::hit("test.p5b.close_taken", &self.root)?;
        table.close_exclusive().await;
        #[cfg(test)]
        crate::core::power_loss::failpoint::hit("test.p5b.close_landed", &self.root)?;
        // Owned alone since the check above; dropping it drops the providers
        // (the scheduler holds the unit weakly).
        if let Err(still) = Arc::try_unwrap(table) {
            ::log::error!(
                "partition {pid} of \"{}\" was referenced again while it closed ({} references)",
                self.name,
                Arc::strong_count(&still)
            );
        }
        self.closes.fetch_add(1, Ordering::AcqRel);
        Ok(Close::Closed)
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
