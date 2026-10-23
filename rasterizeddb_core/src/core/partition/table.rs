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
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwap;
use rclite::Arc;
use smallvec::SmallVec;

use super::files::{self, MapLogFile};
use super::guard::RangeGuard;
use super::key::KeySpec;
use super::log::{MapHeader, MapState, ROUTE_V1, Record};
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
    let total = declared.saturating_add(adding as u64);
    if total > MAX_DECLARED_PER_DATABASE as u64 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "declaring {adding} more partitions would make {total} in this database; more than \
                 {MAX_DECLARED_PER_DATABASE} partitions per database is not supported yet"
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
    /// Set exactly when the partition is materialized and open; never cleared
    /// in phase 2a.
    open: tokio::sync::OnceCell<Arc<Table<DbStorageProvider>>>,
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
}

/// Everything a table that opened has; a broken one has none of it.
struct Live {
    spec: KeySpec,
    /// The key layout over the whole space: points of rows, for routing.
    router: RangeGuard,
    epoch: ArcSwap<MapEpoch>,
    /// By pid; phase 2a pids are `0..n`.
    slots: Box<[Slot]>,
    log: MapLogFile,
    /// The replayed log, kept current: a record is applied to a copy first
    /// and appended only if it applies, so the file never holds a record
    /// replay would refuse.
    state: tokio::sync::Mutex<MapState>,
    seed: Arc<DbStorageProvider>,
    ids: StdArc<IdSource<DbStorageProvider>>,
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
fn refuse_torn_leftovers(root: &Path, uid: u64, pid: u32, torn: &str) -> io::Result<()> {
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
        files::map_file(uid, 0),
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
        // One sync covers all three: the log is synced in order, so the last
        // record durable means the first two are.
        log.append(&Record::Header(header)).await?;
        log.append(&Record::Locations(vec![root_str])).await?;
        log.append_durable(&Record::Place {
            hi: SPACE_END,
            entries: map.entries().to_vec(),
        })
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
            (Ok(root), Ok(())) => Self::open_live(&ctx, root, uid, &shell, &mut declared, &mut torn).await,
            (Err(e), _) => Err(e.to_string()),
            (_, Err(e)) => Err(e.to_string()),
        }
        .map_err(|e| {
            let why = format!("partitioned table \"{name}\" ({uid:016x}) cannot be opened: {e}");
            ::log::error!("{why}");
            why
        });
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
        }
    }

    async fn open_live(
        ctx: &OpenCtx<'_>,
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
        files::require_existing(root, &[files::map_file(uid, 0)]).map_err(e)?;
        let map_io = StdArc::new(ctx.seed.create_new(files::map_file(uid, 0)).await);
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

        let entries = map.entries();
        let slots: Box<[Slot]> = entries
            .iter()
            .enumerate()
            .map(|(j, &(lo, pid))| Slot {
                pid,
                range: PointRange {
                    lo,
                    hi: entries.get(j + 1).map_or(SPACE_END, |&(next, _)| next as u128),
                },
                open: tokio::sync::OnceCell::new(),
            })
            .collect();
        let live = Live {
            router: RangeGuard::new(spec.clone(), PointRange::FULL),
            spec,
            epoch: ArcSwap::from_pointee(MapEpoch {
                epoch: state.epoch(),
                map,
            }),
            slots,
            log,
            state: tokio::sync::Mutex::new(state.clone()),
            seed: ctx.seed.clone(),
            ids,
        };
        for (pid, part) in state.parts() {
            let Some(loc) = part.location else {
                // §4.3's skip is a proof only while the log is whole.
                if let Some(why) = torn.as_deref() {
                    refuse_torn_leftovers(root, uid, pid, why).map_err(e)?;
                }
                continue;
            };
            if loc != ROOT_LOC {
                return Err(format!("partition {pid} is at location {loc}, outside the root"));
            }
            files::require_existing(root, &files::data_files(uid, pid)).map_err(e)?;
            let slot = &live.slots[pid as usize];
            let table = Self::open_partition(&live, uid, ctx.db_id, ctx.transactions, &shell, slot)
                .await
                .map_err(|err| format!("partition {pid}: {err}"))?;
            let _ = slot.open.set(table);
        }
        Ok(live)
    }

    /// Opens a slot's partition over files that exist, and registers it.
    async fn open_partition(
        live: &Live,
        uid: u64,
        db_id: u64,
        transactions: &StdArc<TransactionManager>,
        shell: &Arc<Table<DbStorageProvider>>,
        slot: &Slot,
    ) -> io::Result<Arc<Table<DbStorageProvider>>> {
        let physical = files::data_stem(uid, slot.pid);
        let table = Table::open_partition(PartitionOpen {
            cache_scope: format!("{}\u{0}{}", db_id, physical),
            physical,
            seed: live.seed.clone(),
            io_schema: shell.io_schema.clone(),
            schema: shell.schema.clone(),
            ids: live.ids.clone(),
            guard: RangeGuard::new(live.spec.clone(), slot.range),
        })
        .await?;
        table.enable_versioning(transactions.clone());
        Ok(Arc::new(table))
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
        let levels = live.spec.levels();
        let touched: SmallVec<[usize; 3]> = (0..levels.len())
            .filter(|&i| {
                let wo = levels[i].write_order;
                computed.contains(&wo) || assigned.iter().any(|(w, _)| *w == wo)
            })
            .collect();
        if touched.is_empty() {
            return Ok(true);
        }
        if touched.iter().any(|&i| computed.contains(&levels[i].write_order)) {
            return Ok(false);
        }
        let schema = &self.shell.schema;
        let Ok(tokens) = crate::core::tokenizer::query_tokenizer::try_tokenize(where_clause, &schema.fields) else {
            return Ok(false);
        };
        let keyed: SmallVec<[u32; 3]> = levels.iter().map(|l| l.write_order).collect();
        let Some(program) = sarg::extract_gated(&tokens, &|wo| keyed.contains(&wo), &decimal_scales(schema)) else {
            return Ok(false);
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
        for &i in &touched {
            let wo = levels[i].write_order;
            let Some((_, value)) = assigned.iter().find(|(w, _)| *w == wo) else {
                return Ok(false);
            };
            let Ok(new) = live.spec.level_hash_of(i, value) else {
                return Ok(false);
            };
            let proven = pinned.iter().any(|p| {
                p.op == crate::core::filter::zonemap::SargOp::Eq
                    && p.write_order == wo
                    && live.spec.level_hash_of(i, &p.value).is_ok_and(|old| old == new)
            });
            if !proven {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// The materialized partitions `route` reaches under `epoch`, by pid. An
    /// unmaterialized one is skipped: it holds no row (§4.3).
    pub fn materialized(&self, epoch: &MapEpoch, route: &Route) -> io::Result<Vec<(u32, Arc<Table<DbStorageProvider>>)>> {
        let live = self.live()?;
        let mut out = Vec::new();
        for pid in route.pids(&epoch.map) {
            if let Some(table) = Self::slot(live, pid)?.open.get() {
                out.push((pid, table.clone()));
            }
        }
        Ok(out)
    }

    fn slot(live: &Live, pid: u32) -> io::Result<&Slot> {
        match live.slots.get(pid as usize) {
            Some(slot) if slot.pid == pid => Ok(slot),
            _ => Err(io::Error::other(format!("internal routing error: no slot for partition {pid}"))),
        }
    }

    /// The partition owning `point`, materialized first if it never was.
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
        slot.open
            .get_or_try_init(|| self.materialize(live, slot))
            .await
            .cloned()
    }

    /// §3.2's order. Runs inside the slot's cell initialisation, so once per
    /// pid at a time, and the cell is set only after it returns: before any
    /// row reaches the partition.
    async fn materialize(&self, live: &Live, slot: &Slot) -> io::Result<Arc<Table<DbStorageProvider>>> {
        // A mirror added after CREATE stops new partitions rather than let
        // them go unmirrored.
        refuse_mirrored((self.mirrored)())?;
        let pid = slot.pid;
        let recorded = live.state.lock().await.part(pid).map(|p| p.location);
        match recorded {
            None => return Err(io::Error::other(format!("internal routing error: the map has no partition {pid}"))),
            // An earlier attempt logged `MATERIALIZE` and then failed to open:
            // the files are this partition's, and a second record would be
            // refused at every later open.
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
                    refuse_torn_leftovers(&self.root, self.uid, pid, why)?;
                }
                files::create_partition_files(&self.root, self.uid, pid)?;
                let rec = Record::Materialize {
                    pid,
                    floor: self.transactions.status().forgotten_below(),
                    loc: ROOT_LOC,
                };
                let mut state = live.state.lock().await;
                let mut next = state.clone();
                next.apply(&rec).map_err(invalid)?;
                // Durable before any row reaches the partition's files, once
                // per partition. Left to the rows' own sync, a crash could
                // lose the record while writeback kept row pages in the new
                // files, and with the map's tail torn the open refuses the
                // whole table for unexplained data (`refuse_torn_leftovers`).
                live.log.append_durable(&rec).await?;
                *state = next;
            }
        }
        Self::open_partition(live, self.uid, self.db_id, &self.transactions, &self.shell, slot).await
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
            table.io_filters()?.get_len().await,
        ))
    }

    async fn sizes(table: &Table<DbStorageProvider>) -> io::Result<(u64, u64)> {
        let (rows, pointers, filters) = Self::file_lens(table).await?;
        Ok((pointers / ROW_POINTER_RECORD_LEN as u64, rows + pointers + filters))
    }

    /// Totals over the materialized partitions, and the parent files. Lengths
    /// are atomics on local storage, so this reads no file metadata.
    pub async fn totals(&self) -> io::Result<Totals> {
        let live = self.live()?;
        let mut t = Totals {
            ids_handed_out: live.ids.handed_out(),
            declared: live.slots.len() as u32,
            schema: self.shell.io_schema.get_len().await,
            map: live.log.io().get_len().await,
            ids: live.ids.io.get_len().await,
            ..Totals::default()
        };
        for slot in live.slots.iter() {
            if let Some(table) = slot.open.get() {
                let (rows, pointers, filters) = Self::file_lens(table).await?;
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
        Ok(self.live()?.state.lock().await.header().route_v)
    }

    /// Every declared partition, by pid.
    pub async fn partitions(&self) -> io::Result<Vec<PartitionInfo>> {
        let live = self.live()?;
        let root = self.root.to_string_lossy().into_owned();
        let mut out = Vec::with_capacity(live.slots.len());
        for slot in live.slots.iter() {
            let (location, state, slots, bytes) = match slot.open.get() {
                Some(table) => {
                    let (slots, bytes) = Self::sizes(table).await?;
                    (Some(root.clone()), "materialized", slots, bytes)
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
            let mut state = live.state.lock().await;
            if !state.dropped() {
                let mut next = state.clone();
                next.apply(&Record::Dropped).map_err(invalid)?;
                live.log.append_durable(&Record::Dropped).await?;
                *state = next;
            }
        }
        Ok(())
    }
}
