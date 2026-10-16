use cacheguard::CacheGuard;
use itertools::Itertools;
use log::{error, info};
use smallvec::SmallVec;
use std::io::Result;
use std::sync::atomic::{AtomicBool, AtomicU64};

use rclite::Arc;

use super::schema::TableSchema;
use crate::core::processor::concurrent_processor::ConcurrentProcessor;
use crate::core::rql::lexer_ct::CreateColumnData;

use crate::core::{
    row::{
        row::{Row, RowFetch, RowWrite},
        row_pointer::{RowPointer, RowPointerIterator},
        schema::SchemaField,
    },
    storage_providers::traits::StorageIO,
};

use crate::core::row::row::{ColumnFetchingData, ColumnWritePayload};

use crate::core::db_type::DbType;
use crate::core::filter::{CandidateBitmap, FilterConfig, TableFilters, is_indexable, sarg};
use crate::core::mvcc::TransactionManager;
use crate::core::mvcc::Visibility;
use crate::core::mvcc::transaction::{FROZEN_TX, NO_TX, TxId};
use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
use crate::core::row::schema::SchemaCalculator;
use crate::core::vacuum::{
    CompactReport, FreeSpace, ReadTicket, ReaderRegistry, VacuumConfig, VacuumReport,
    VacuumService, VacuumStats, VersionHorizon, collect_live, plan_row_moves,
};
use arc_swap::ArcSwapOption;
use std::sync::Arc as StdArc;
use std::sync::atomic::Ordering as AtomicOrdering;

/// How many row ids are reserved on disk at once.
///
/// The sequence is persisted ahead of use, so a crash loses at most this many
/// ids. Ids only have to be unique, never gapless, so the cost of a crash is
/// nothing and the cost of the guarantee is one small write per this many
/// inserts.
const ID_RESERVATION_CHUNK: u64 = 1024;

pub struct Table<S: StorageIO> {
    pub schema: TableSchema,
    /// Set when this table was *opened* and its stored schema could not be
    /// read, so `schema` above is a fabricated empty one rather than the
    /// table's own.
    ///
    /// A caller must not put such a table into the live map: every column is
    /// missing, so `SELECT *` succeeds and returns rows with no fields, which
    /// reads as "this table has no columns" rather than as a failure. The
    /// database already refuses to open a table whose *backend* is missing, for
    /// the same reason and with the same remedy — leave it out, and let the
    /// query say the table does not exist.
    pub schema_unreadable: bool,
    pub io_pointers: Arc<S>,
    pub io_rows: Arc<S>,
    pub io_schema: Arc<S>,
    /// Sidecar holding the persisted filter checkpoint.
    pub io_filters: Arc<S>,
    /// Updates that wrote only the assigned columns rather than re-encoding the
    /// whole row. Diagnostic: the ratio against rows updated says how much of a
    /// workload the cheap path is actually reaching.
    in_place_column_updates: CacheGuard<AtomicU64>,
    /// The transaction system, once one is attached.
    ///
    /// `None` until [`Table::enable_versioning`] is called, and reclamation then
    /// behaves exactly as it did before MVCC existed. Shared with the
    /// reclamation service, which is spawned before versioning can be wired, so
    /// it has to be able to pick it up afterwards.
    versions: StdArc<ArcSwapOption<TransactionManager>>,
    /// Transaction bound at the last sweep that cleared filter fingerprints.
    /// Shared with the service so both agree on what is already cleared.
    cleared_below: StdArc<CacheGuard<AtomicU64>>,
    /// Whether this table carries a `UNIQUE` constraint of any kind — a column
    /// flag, or a table-level constraint in the sidecar.
    ///
    /// Enforcing one means an insert has to look before it writes, and the look
    /// and the write have to be one step or two concurrent inserts both pass.
    /// That costs inserts their concurrency, so it is paid only by tables that
    /// actually have a constraint to enforce.
    ///
    /// It must account for the sidecar as well as the flags: it is what decides
    /// whether the write lock is taken at all, so a table whose only constraints
    /// are table-level would otherwise run the check outside the lock — two
    /// concurrent inserts would each look, each find nothing, and each write.
    has_unique: bool,
    /// Sidecar holding the row-id sequence.
    ///
    /// The id counter used to be recovered from the last pointer record, which
    /// worked only while ids ascended with file order. They no longer do: a
    /// relocated row keeps its id, so the largest id can sit anywhere. Reading
    /// the last record would then hand out ids that are already in use.
    pub io_sequence: Arc<S>,
    /// Highest id reserved on disk. Ids past this need a fresh reservation
    /// before they are handed out.
    id_reserved_upto: CacheGuard<AtomicU64>,
    /// Inserts between their reservation check and the id they take. Each
    /// takes at most one, so the reservation must reach past the next id by
    /// this many: two inserts checking at once both see the next id covered
    /// and take it and the one after, and the second is not covered.
    ids_in_flight: AtomicU64,
    /// Serialises raising the reservation, so it only ever goes up: two
    /// writers could otherwise leave the smaller of their values on disk and
    /// the larger in memory.
    reserving: tokio::sync::Mutex<()>,
    pub hard_locked: CacheGuard<AtomicBool>,
    pub len: CacheGuard<AtomicU64>,
    pub last_row_id: CacheGuard<AtomicU64>,
    pub concurrent_processor: ConcurrentProcessor,
    /// Name this table answers to in the process-wide query cache.
    ///
    /// The cache is one map for the whole process, keyed by `(name, clause,
    /// limit)`, while `cache_generation` — the thing that retires an entry — is
    /// per table. Two databases each holding a `users` table would therefore
    /// share slots and never invalidate each other's, so a scan in one could be
    /// served the other's row pointers. Qualifying the key with the owning
    /// database's id keeps the map global and the entries disjoint.
    ///
    /// Unqualified until [`Table::scope_cache_to_database`] says otherwise, so a
    /// table built outside a `Database` — tests, benchmarks — is unaffected.
    cache_scope: String,
    /// Bit-level filter index. `None` when disabled or when no column of this
    /// table has a filterable type.
    ///
    /// Shared with the maintenance task, which owns backfilling and
    /// checkpointing so neither ever runs on a caller's thread.
    pub filters: Option<StdArc<TableFilters>>,
    /// Secondary indexes: those `CREATE INDEX` declared, and the implicit ones
    /// of `PRIMARY KEY` and `UNIQUE`. `None` when the table has none. See
    /// [`crate::core::index`] for why they may only ever over-report.
    pub indexes: Option<StdArc<crate::core::index::TableIndexes>>,
    /// Byte ranges of the rows file that deleted or relocated rows left behind
    /// and the write path may reuse. Populated by the reclamation service.
    pub free_space: StdArc<FreeSpace>,
    /// Epoch tickets for in-flight reads, so reclamation can tell which queries
    /// predate a batch of freed extents.
    pub readers: StdArc<ReaderRegistry>,
    /// Running totals for the reclamation service.
    pub vacuum_stats: StdArc<VacuumStats>,
    /// Serializes the mutating half of `DELETE` and `UPDATE`.
    ///
    /// Both statements scan for matches and then rewrite the pointer records
    /// they found. Without this, two statements matching the same row each act
    /// on their own snapshot: both see it live, both tombstone it, and both
    /// append a replacement, so one logical row becomes two. Scanning stays
    /// concurrent; only the rewrite is serialized, and every rewrite re-reads
    /// the pointer record under the lock so a stale snapshot is caught rather
    /// than acted on.
    ///
    /// Held exclusively by `DELETE`, by an `UPDATE` and by the aborted-write
    /// sweep. A *point* update, one naming its row by a unique key, holds it
    /// shared together with that key's stripe of `unique_lock`: every other
    /// writer of a row with that key holds the same stripe or this lock
    /// exclusively, so the guarantee above holds for the rows it can match,
    /// and point updates of different keys run side by side.
    write_lock: StdArc<tokio::sync::RwLock<()>>,
    /// Makes a uniqueness check atomic with the write it guards.
    ///
    /// Taken by every `INSERT` into a table with a UNIQUE constraint, and by an
    /// `UPDATE` of such a table for its rewrite — but not for its scan.
    ///
    /// Striped by key (`KeyLocks`): an insert takes the stripe of each key it
    /// writes, so inserts of different keys no longer take turns. An `UPDATE`
    /// takes every stripe, since it learns its rows' keys only as it rewrites
    /// them, except a point update, which takes its key's stripe *before* its
    /// scan and the stripes of the row's other keys before its rewrite.
    ///
    /// It used to be `write_lock`, which an `UPDATE` holds across its whole
    /// scan. A multi-row `INSERT` takes the lock once per tuple, so each tuple
    /// queued behind a full update scan: under one session running 500-row
    /// range updates, three inserters spent ~87% of their time waiting for it,
    /// and a 50,000-row statement sat for minutes. An insert does not need to
    /// exclude the scan — on a table without a constraint it never did.
    ///
    /// It does need to exclude the rewrite, even one that assigns no UNIQUE
    /// column. A relocated row's new version gets its filter entry only once
    /// the batch is durable (`PendingFilterOp`), while the old version keeps
    /// the value. A check that sampled the filter in that window, then read the
    /// old version after the update committed, would find neither version and
    /// admit a duplicate of a key nobody changed. Holding this across the
    /// rewrite and the filter application closes the window. Past it, the
    /// in-flight rule in `find_row_with_value` covers the transaction's
    /// remaining life, lock or no lock. Order: `write_lock`, then this.
    unique_lock: StdArc<crate::core::row::key_locks::KeyLocks>,
    /// Held shared by every read and write, exclusively by full compaction.
    ///
    /// Incremental reclamation needs no such gate — it only ever hands out
    /// bytes nothing refers to. Compaction *moves live rows*, so it needs every
    /// other operation stopped, not merely drained: a query that started after
    /// the drain would still be holding a pointer that compaction is about to
    /// invalidate.
    compaction_gate: StdArc<tokio::sync::RwLock<()>>,
    /// This table's place in the forget registry, set when versioning is
    /// enabled; shared with the vacuum service, which advances it.
    forget: StdArc<std::sync::OnceLock<crate::core::mvcc::transaction::ForgetHandle>>,
    /// Bumped once per completed `DELETE` or `UPDATE`, under the write lock.
    ///
    /// Lets a statement tell whether anything could have changed under it
    /// between its scan and its turn at the lock. Unchanged means its snapshot
    /// is still authoritative and the per-row re-read can be skipped — which is
    /// the usual case, and the one where a bulk `DELETE` would otherwise pay a
    /// pointer read per row for a race that never happened.
    ///
    /// Shared with the reclamation service, which uses it for the other half of
    /// the same observation: nothing but a `DELETE` or an `UPDATE` can create
    /// dead space, so an unchanged value means a scan would find nothing new.
    ///
    /// The `CacheGuard` sits *inside* the `StdArc`, which is where it has to be.
    /// An `Arc<AtomicU64>` is a 24-byte heap allocation — two reference counts
    /// then the value — and these counters are allocated one after another
    /// during `Table::new`, so the allocator hands back addresses in the same
    /// small-object page and `write_generation`, `cleared_below` and
    /// `cache_generation` all landed inside one 64-byte line. That put a counter
    /// incremented by every committed mutation on the same line as the counter
    /// every query loads to validate a cache entry: readers and writers of two
    /// deliberately separated values contending as if they were one.
    write_generation: StdArc<CacheGuard<AtomicU64>>,
    /// Row versions the write path has stamped dead: one per row a `DELETE`
    /// removed and per row an `UPDATE` replaced. The vacuum service sweeps only
    /// once enough have accrued since its last sweep
    /// (`VacuumConfig::sweep_threshold`), rather than on every mutation.
    ///
    /// Plain, not padded: it is bumped once per statement beside
    /// `write_generation`, and padding contended counters measured a loss here.
    dead_stamps: StdArc<AtomicU64>,
    /// Bumped once per committed mutation, and stamped onto every query-cache
    /// entry the table produces. A reader compares the two, so a mutation
    /// strands every entry built before it — including one from a scan still in
    /// flight when the mutation committed, which a sweep of the cache could not
    /// reach. Kept separate from `write_generation`, whose value also drives the
    /// pointer re-read that `DELETE`/`UPDATE` fall back on under contention;
    /// inserts have to move this counter but must not push those onto the slow
    /// path.
    cache_generation: StdArc<CacheGuard<AtomicU64>>,
    /// Cleared once the maintenance task has finished its initial backfill.
    /// Until then the filter is correct but not yet selective.
    filters_ready: StdArc<CacheGuard<AtomicBool>>,
    /// Drop signal for the maintenance task.
    maintenance_alive: StdArc<CacheGuard<AtomicBool>>,
}

impl<S: StorageIO> Drop for Table<S> {
    fn drop(&mut self) {
        // Without this the per-table maintenance task would outlive the table
        // and keep ticking against storage that is going away.
        self.maintenance_alive.store(false, AtomicOrdering::Relaxed);
    }
}

unsafe impl<S: StorageIO> Send for Table<S> {}
unsafe impl<S: StorageIO> Sync for Table<S> {}

/// Hands out each `Table`'s starting cache generation. Process-wide, monotonic,
/// never reused.
///
/// Starting every table at 0 was unsound once a table could be dropped and
/// recreated. `cache_scope` is `"<db_id>\0<table_name>"`, so a table recreated
/// under the same name in the same database gets an **identical** key, while the
/// new `Table` begins at generation 0 again — and nothing retires the old
/// entries, since `Database::drop_table` releases the IO handles but never
/// touches the cache. A generation-0 entry cached before the drop therefore
/// matched a lookup after it, and the answer was pointers into the file as it
/// used to be.
///
/// A short fetch abandons the hit and rescans, so an emptied table self-heals;
/// a *repopulated* one does not, and returns rows from the old table's layout.
///
/// **`ALTER TABLE` reaches the same fault by a much shorter route**, and is the
/// reason this matters outside a test. Every `reload_table` caller is an ALTER
/// path (`executor.rs:1532`, `1572`, `1624`, `1656`, `1677`), each reaching it
/// *after* the change is on disk — and `ADD COLUMN` on a populated table
/// rewrites every row under the new layout, drops and recreates the table, and
/// moves the pointers. Entries cached before the ALTER are pointers into the
/// old layout; with the generation restarting at 0 they matched the scan after
/// it. So "ALTER TABLE invalidates the query cache" is a property this counter
/// establishes, not one that was ever true by construction — which is exactly
/// what someone would otherwise assume when reading the ALTER path.
///
/// Making the counter global rather than per-table means a new instance can
/// never collide with a previous one's entries, and costs nothing: entries are
/// matched on exact equality, so a higher floor simply never matches. Same
/// reasoning as `next_database_id`, one level down.
fn next_cache_generation() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, AtomicOrdering::Relaxed)
}

/// Whether a candidate mask earns the per-record test it costs.
///
/// Two independent reasons to keep one, and a mask needs only one of them:
///
/// * it prunes broadly — the classic case, where most of the table is excluded
///   and the saving is spread across the whole scan; or
/// * it prunes *early* and the scan is able to stop early, in which case the
///   cost of the query is how far it walks before its first match rather than
///   what fraction of the table survives.
///
/// The second is the one a global ratio cannot see, and its absence made
/// `WHERE id >= 99995 LIMIT 5` over a million rows fetch ~100,000 rows it
/// immediately discarded: ~90% of the table matched, so the ratio test threw
/// away a mask that would have skipped every one of them.
///
/// `can_stop_early` is what keeps that exception honest. `leading_skipped` is
/// an absolute count, so on a large enough table a lead-in worth one batch is a
/// negligible fraction of the whole — and a scan that reads everything anyway
/// would then pay the per-record test across the entire table to save that one
/// batch of fetches, which is a net loss. A scan that cannot terminate early
/// therefore gets the ratio test and nothing else.
#[inline]
fn mask_is_worth_using(
    ratio: f64,
    max_ratio: f64,
    leading_skipped: u64,
    worth_it: u64,
    can_stop_early: bool,
) -> bool {
    ratio <= max_ratio || (can_stop_early && leading_skipped >= worth_it)
}

/// Slots the mask excludes before its first candidate.
///
/// This is the stretch a scan would otherwise fetch rows for and discard, so it
/// is the part of a mask's value that a global pruning ratio cannot see.
fn leading_excluded(words: &[u64]) -> u64 {
    let mut skipped = 0u64;
    for &word in words {
        if word == 0 {
            skipped += 64;
            continue;
        }
        return skipped + word.trailing_zeros() as u64;
    }
    skipped
}

/// The cache generation to hand a scan.
///
/// The cache used to be opted out of entirely on a versioned table, because an
/// entry held the pointers a scan found *visible* — a set that only the snapshot
/// which built it could answer from. The generation counter could not police
/// that: it bumps on writes, and what changes visibility is a **commit**, which
/// writes nothing.
///
/// The entry now holds the pointers matching the **predicate**, which does not
/// depend on who is asking, and visibility is applied per pointer at serve time.
/// So one entry serves every snapshot correctly, including the case that broke
/// the old design: a transaction that wrote before the entry was cached and
/// commits after is *in* the set, was merely invisible, and becomes visible.
///
/// # Why a reclaimed extent cannot be read through a stale entry
///
/// Entries can now name versions that are dead for everyone, which reclamation
/// does not know about. It is still safe, and the argument is one line:
///
/// > A version is reclaimable only once its `xmax` is committed and below
/// > `oldest_active` — which is exactly the condition that makes it invisible to
/// > every snapshot that can still be taken.
///
/// The serve path tests visibility **before** fetching, so a reclaimed version
/// is skipped rather than read. It is dead weight in the entry, not a hazard.
/// `a_reclaimable_version_is_invisible_to_every_snapshot` pins the lemma; the
/// ordering of the check and the fetch is what depends on it.
fn cache_generation_for(_visibility: &Visibility, generation: u64) -> Option<u64> {
    Some(generation)
}

/// A filter-index change staged until the write it describes is durable.
///
/// The index used to be mutated inside the write loop, before
/// `commit_durable`. Two things went wrong with that. A concurrent scan could
/// see a slot's fingerprints cleared (DELETE) or rewritten to a new value
/// (in-place UPDATE) while the data file still held the old, committed row, so
/// a query for that row was pruned before its pointer was even decoded — a
/// false negative. Worse, `commit_durable` can fail, and nothing rolled the
/// index back, so the false negative became *durable*: the row kept its old
/// value on disk and the index described a different one, unfindable until the
/// next backfill. Staging the change and applying it only after the commit
/// succeeds closes both — a failed commit drops the staged ops with the write.
enum PendingFilterOp {
    /// Clear a slot's fingerprints once its row is gone for everyone.
    Delete(u64),
    /// (Re)fingerprint a slot from the row now durable at it.
    ///
    /// The third field is the slot's secondary-index keys *before* the write,
    /// for an in-place overwrite: the slot keeps its number and changes its
    /// value, so its old keys must go now that the new value is durable.
    /// `None` for a version written at a fresh slot, which has no old keys.
    Insert(u64, RowWrite, Option<Vec<Option<crate::core::index::KeyBuf>>>),
}

impl<S: StorageIO> Table<S> {
    /// Applies filter-index changes staged during a batch, now that the batch's
    /// `commit_durable` has made the writes they describe durable. A no-op with
    /// no filter index, and it always empties `pending`.
    fn apply_pending_filters(&self, pending: &mut Vec<PendingFilterOp>) {
        if pending.is_empty() {
            return;
        }
        for op in pending.drain(..) {
            match op {
                PendingFilterOp::Delete(slot) => {
                    if let Some(filters) = self.filters.as_ref() {
                        filters.on_delete(slot);
                    }
                    // A secondary index keeps the slot: vacuum forgets it once
                    // nothing can reach the version, reading its key back then.
                }
                PendingFilterOp::Insert(slot, row_write, old_keys) => {
                    if let Some(filters) = self.filters.as_ref() {
                        filters.on_insert(slot, &row_write);
                    }
                    if let Some(indexes) = self.indexes.as_ref() {
                        match old_keys {
                            Some(old) => indexes.on_replace(slot, &old, &row_write),
                            None => indexes.on_insert(slot, &row_write),
                        }
                    }
                }
            }
        }
    }

    /// Whether writes have filter or index maintenance to stage.
    #[inline]
    fn maintains_indexes(&self) -> bool {
        self.filters.is_some() || self.indexes.is_some()
    }
}

impl<S: StorageIO> Table<S> {
    /// Retires every query-cache entry this table has produced so far.
    ///
    /// Must be called *after* the mutation is visible: an entry is only safe to
    /// serve if the generation it was stamped with is still current, so the
    /// bump has to follow the write it invalidates.
    ///
    /// This used to sweep the shared slot array instead, which cost a pass over
    /// every slot in the cache — ~460us at the default sizing — on each
    /// mutation, single-row inserts included, and cleared other tables'
    /// entries along the way. It also could not retire an entry that a
    /// concurrent scan had not inserted yet, so a `DELETE` racing a `SELECT`
    /// left the deleted rows cached.
    #[inline]
    fn invalidate_query_caches(&self) {
        self.cache_generation.fetch_add(1, AtomicOrdering::AcqRel);
    }

    /// The generation a scan starting now should stamp its cache entry with.
    #[inline]
    fn cache_generation(&self) -> u64 {
        self.cache_generation.load(AtomicOrdering::Acquire)
    }

    /// Qualifies this table's query-cache key with the database that owns it.
    ///
    /// Called once, by [`crate::core::database::Database`], before the table is
    /// shared. See [`Table::cache_scope`] for why the cache needs it.
    pub fn scope_cache_to_database(&mut self, database_id: u64) {
        // A separator that cannot occur in an identifier, so `1` + `x_y` and
        // `1_x` + `y` cannot produce the same key.
        self.cache_scope = format!("{}\u{0}{}", database_id, self.schema.name);
    }

    pub async fn new(table_name: &str, initial_io: Arc<S>, columns: Vec<CreateColumnData>) -> Self {
        Self::new_with_constraints(table_name, initial_io, columns, &[]).await
    }

    /// As [`Table::new`], but marks the named columns `UNIQUE`.
    ///
    /// Separate from `new` rather than a field on `CreateColumnData` so that
    /// adding constraints does not touch every caller that builds a column list.
    /// The name matching is case-insensitive, as column resolution is elsewhere.
    ///
    /// Uniqueness is enforced from here on: an `INSERT` or `UPDATE` that would
    /// put a duplicate in one of these columns fails instead of writing it.
    pub async fn new_with_constraints(
        table_name: &str,
        initial_io: Arc<S>,
        columns: Vec<CreateColumnData>,
        unique_columns: &[String],
    ) -> Self {
        let io_pointers = Arc::new(
            initial_io
                .create_new(format!("{}_pointers.db", table_name))
                .await,
        );

        let io_rows = Arc::new(initial_io.create_new(format!("{}.db", table_name)).await);

        let io_schema = Arc::new(
            initial_io
                .create_new(format!("{}_schema.db", table_name))
                .await,
        );

        let io_filters = Arc::new(
            initial_io
                .create_new(format!("{}_flt.db", table_name))
                .await,
        );

        let io_sequence = Arc::new(
            initial_io
                .create_new(format!("{}_seq.db", table_name))
                .await,
        );

        let io_pointers_clone = io_pointers.clone();
        let io_rows_clone = io_rows.clone();
        let io_schema_clone = io_schema.clone();
        let io_filters_clone = io_filters.clone();
        let io_sequence_clone = io_sequence.clone();

        // Cleared by `Drop`. Every task spawned for this table watches it, and
        // this one has to as well: the providers' services never return on
        // their own, so a task that only joined them held an `Arc` to each
        // provider — five files' worth of handles, an mmap and a staging
        // buffer — for the life of the process, for every `Table` ever built.
        // A table rebuilt on each new JSON key leaked a generation per key, and
        // the process ran at the descriptor limit within the hour.
        let maintenance_alive = StdArc::new(CacheGuard::new(AtomicBool::new(true)));
        let services_alive = maintenance_alive.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = async {
                    tokio::join!(
                        io_rows_clone.start_service(),
                        io_pointers_clone.start_service(),
                        io_schema_clone.start_service(),
                        io_filters_clone.start_service(),
                        io_sequence_clone.start_service()
                    );
                } => {}
                _ = async {
                    // Polled rather than signalled, so nothing new has to be
                    // threaded through `Drop`. A few seconds of latency on
                    // releasing a dropped table's handles is nothing next to
                    // never.
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        if !services_alive.load(AtomicOrdering::Relaxed) {
                            break;
                        }
                    }
                } => {}
            }
        });

        // An open passes no columns; only `CREATE TABLE` passes them. So a
        // failed load with no columns to build from is a table whose schema
        // should have been there and was not — a recovery failure, not a new
        // table — and fabricating one both answers every later query wrongly
        // and *overwrites* whatever was still on disk.
        let mut schema_unreadable = false;
        let schema = {
            if let Ok(schema) = TableSchema::load(io_schema.clone()).await {
                schema
            } else if columns.is_empty() {
                error!(
                    "table '{}' was opened but its schema could not be read; it is left \
                     unusable rather than answering queries with no columns",
                    table_name
                );
                schema_unreadable = true;
                // Deliberately not saved: the bytes on disk are the only copy
                // of the real schema, and a recovery that could not read them
                // must not replace them with an empty one.
                TableSchema::new(table_name.to_string(), false)
            } else {
                let mut schema = TableSchema::new(table_name.to_string(), false);

                schema
                    .save(io_schema.clone())
                    .await
                    .expect("Failed to save initial schema");

                for column in columns {
                    let is_unique = unique_columns
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&column.name));
                    schema
                        .add_field(io_schema.clone(), column.name, column.data_type, is_unique)
                        .await;
                }

                schema
            }
        };

        info!("Loaded schema: {:?}", schema);

        let mut pointer_iterator = RowPointerIterator::new(io_pointers.clone()).await.unwrap();

        let (atomic_last_id, atomic_table_length) = if let Some(row_pointer) =
            pointer_iterator.read_last().await
        {
            // The persisted sequence is the authority. Falling back to the
            // last record is only correct for a table written before the
            // sequence existed, when ids still ascended with file order —
            // and such a table has no reservation to honour, so the fallback
            // is exact rather than merely safe.
            let atomic_last_id = AtomicU64::new(match Self::load_id_sequence(&io_sequence).await {
                Some(reserved) => reserved,
                None => row_pointer.row_id(),
            });

            // End of the last row, not its start — the start would place the
            // high-water mark inside a row that already exists. The rows
            // file's own length is the floor: a row written into a
            // reclaimed extent sits below the frontier, so the last pointer
            // on its own no longer bounds the file.
            let table_length =
                (row_pointer.position + row_pointer.length as u64).max(io_rows.get_len().await);
            let atomic_table_length = AtomicU64::new(table_length);

            (atomic_last_id, atomic_table_length)
        } else {
            let atomic_last_id =
                AtomicU64::new(Self::load_id_sequence(&io_sequence).await.unwrap_or(0));
            let atomic_table_length = AtomicU64::new(0);
            (atomic_last_id, atomic_table_length)
        };

        // Everything up to the recovered value may already be in use, so the
        // first insert reserves before it allocates.
        let id_reserved_upto = AtomicU64::new(atomic_last_id.load(AtomicOrdering::Acquire));

        let existing_slots = io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64;

        let filter_settings = crate::core::filter::settings::settings();
        let filters = if filter_settings.table_enabled(table_name) {
            match Self::load_filter_checkpoint(&io_filters, &schema.fields, existing_slots).await {
                Some(f) => {
                    info!(
                        "Loaded filter checkpoint; resuming indexing at slot {} of {}",
                        f.resume_slot(),
                        existing_slots
                    );
                    Some(StdArc::new(f))
                }
                None => {
                    let f = TableFilters::for_table(table_name, &schema.fields);
                    if f.is_empty() {
                        None
                    } else {
                        // Rows already on disk predate the filter, so they stay
                        // candidates until a backfill indexes them. Correct from
                        // the first query; just not yet selective.
                        f.set_unindexed_prefix(existing_slots);
                        Some(StdArc::new(f))
                    }
                }
            }
        } else {
            None
        };

        let filters_ready = StdArc::new(CacheGuard::new(AtomicBool::new(true)));
        let free_space = StdArc::new(FreeSpace::new(crate::vacuum_enabled()));
        let readers = StdArc::new(ReaderRegistry::new());

        // Secondary indexes are built from the table on a task of their own,
        // like the filter backfill: `Table::new` returns at once, writes
        // maintain the indexes from this moment, and queries use them once the
        // build has covered every row that was already there.
        let indexes = {
            let indexes = crate::core::index::for_schema(&schema);
            (!indexes.is_empty()).then(|| StdArc::new(indexes))
        };
        if let Some(indexes) = indexes.clone() {
            let io_pointers_task = io_pointers.clone();
            let io_rows_task = io_rows.clone();
            let fields = schema.fields.clone();
            let readers_task = readers.clone();
            let alive = maintenance_alive.clone();
            let table_name_task = table_name.to_string();
            tokio::spawn(async move {
                if !alive.load(AtomicOrdering::Relaxed) {
                    return;
                }
                match build_indexes(&indexes, &io_pointers_task, &io_rows_task, &fields, Some(&readers_task)).await {
                    Ok(n) => {
                        indexes.set_ready(true);
                        info!("Built {} index(es) on {} over {} rows", indexes.len(), table_name_task, n);
                    }
                    Err(e) => error!(
                        "Index build on {} failed; queries scan instead: {}",
                        table_name_task, e
                    ),
                }
            });
        }

        // Everything the filter needs doing after this point — the initial
        // backfill and every checkpoint — happens on this task. No caller ever
        // pays for it: `Table::new` returns immediately and an insert never
        // stalls behind a blob rewrite.
        // Whether the task has anything to backfill is knowable now, so a table
        // with nothing to do reports ready immediately rather than after the
        // task has been scheduled. Otherwise `filters_ready()` races on a fresh
        // table, where the answer is trivially yes.
        let needs_backfill = filters.as_ref().is_some_and(|f| {
            filter_settings.backfill_on_open() && f.resume_slot() < existing_slots
        });

        if let Some(filters) = filters.clone() {
            filters_ready.store(!needs_backfill, AtomicOrdering::Release);

            let ready = filters_ready.clone();
            let alive = maintenance_alive.clone();
            let io_filters_task = io_filters.clone();
            let io_pointers_task = io_pointers.clone();
            let io_rows_task = io_rows.clone();
            let fields = schema.fields.clone();
            // The backfill reads row bytes by pointer, exactly as a query does,
            // so it has to hold a reader epoch for the same reason: an extent it
            // is fetching from must not be reclaimed underneath it. Indexing
            // another row's bytes would not fail — it would silently build a
            // filter that prunes rows that do match.
            let readers_task = readers.clone();

            tokio::spawn(async move {
                if needs_backfill {
                    let resume = filters.resume_slot();
                    if resume < existing_slots && alive.load(AtomicOrdering::Relaxed) {
                        match backfill_into(
                            &filters,
                            &io_pointers_task,
                            &io_rows_task,
                            &fields,
                            resume,
                            Some(&readers_task),
                        )
                        .await
                        {
                            // Checkpoint straight away so the next open resumes
                            // instead of rescanning.
                            Ok(_) => {
                                let n = io_pointers_task.get_len().await
                                    / ROW_POINTER_RECORD_LEN as u64;
                                filters.checkpoint(&*io_filters_task, n).await;
                            }
                            Err(e) => {
                                error!("Filter backfill failed, continuing unfiltered: {}", e)
                            }
                        }
                    }
                }
                // Queries before this point are correct, just unfiltered.
                ready.store(true, AtomicOrdering::Release);

                loop {
                    // Re-read each tick, so `SET FILTER checkpoint_interval_secs`
                    // takes effect without restarting the table.
                    let secs = crate::core::filter::settings::settings().checkpoint_interval_secs();
                    tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
                    if !alive.load(AtomicOrdering::Relaxed) {
                        break;
                    }
                    if filters.should_checkpoint() || filters.checkpoint_requested() {
                        let n = io_pointers_task.get_len().await / ROW_POINTER_RECORD_LEN as u64;
                        filters.checkpoint(&*io_filters_task, n).await;
                    }
                }
            });
        }

        // Reclamation runs on its own task for the same reason filter
        // maintenance does: a scan of the whole pointer file is not something a
        // caller should ever find itself waiting behind.
        let vacuum_stats = StdArc::new(VacuumStats::default());
        let write_generation = StdArc::new(CacheGuard::new(AtomicU64::new(0)));
        let dead_stamps = StdArc::new(AtomicU64::new(0));
        let versions: StdArc<ArcSwapOption<TransactionManager>> =
            StdArc::new(ArcSwapOption::empty());
        let cleared_below = StdArc::new(CacheGuard::new(AtomicU64::new(0)));
        // The sidecar counts as much as the flags: this is what decides whether
        // the write lock is taken, and a table whose only constraints are
        // table-level would otherwise be checked without it.
        let has_unique = schema.fields.iter().any(|f| f.is_unique && !f.is_deleted)
            || !schema.unique_keys.is_empty();
        let cache_generation =
            StdArc::new(CacheGuard::new(AtomicU64::new(next_cache_generation())));
        let schema_name_for_cache = schema.name.clone();
        let write_lock = StdArc::new(tokio::sync::RwLock::new(()));
        let compaction_gate = StdArc::new(tokio::sync::RwLock::new(()));
        let forget = StdArc::new(std::sync::OnceLock::new());

        if crate::vacuum_enabled() {
            VacuumService {
                table_name: schema.name.clone(),
                io_pointers: io_pointers.clone(),
                io_rows: io_rows.clone(),
                free_space: free_space.clone(),
                readers: readers.clone(),
                stats: vacuum_stats.clone(),
                alive: maintenance_alive.clone(),
                mutations: write_generation.clone(),
                dead_stamps: dead_stamps.clone(),
                versions: versions.clone(),
                filters: filters.clone(),
                indexes: indexes
                    .clone()
                    .map(|ix| {
                        let fetch = Self::build_fetch_for(&schema.fields, &ix.columns());
                        (ix, fetch)
                    }),
                cleared_below: cleared_below.clone(),
                config: VacuumConfig::default(),
                write_lock: write_lock.clone(),
                compaction_gate: compaction_gate.clone(),
                cache_generation: cache_generation.clone(),
                forget: forget.clone(),
            }
            .spawn();
        }

        Self {
            schema,
            schema_unreadable,
            io_pointers,
            io_rows,
            io_schema,
            io_filters,
            io_sequence,
            versions,
            cleared_below,
            has_unique,
            in_place_column_updates: AtomicU64::new(0).into(),
            id_reserved_upto: id_reserved_upto.into(),
            ids_in_flight: AtomicU64::new(0),
            reserving: tokio::sync::Mutex::new(()),
            hard_locked: AtomicBool::new(false).into(),
            len: atomic_table_length.into(),
            last_row_id: atomic_last_id.into(),
            concurrent_processor: ConcurrentProcessor::new(),
            cache_scope: schema_name_for_cache,
            filters,
            indexes,
            free_space,
            readers,
            vacuum_stats,
            write_lock,
            unique_lock: StdArc::new(crate::core::row::key_locks::KeyLocks::new()),
            compaction_gate,
            forget,
            write_generation,
            dead_stamps,
            cache_generation,
            filters_ready,
            maintenance_alive,
        }
    }

    /// Reads the persisted id reservation, or `None` if there is none yet.
    ///
    /// A sidecar that is present but unreadable is treated as absent: the
    /// caller's fallback is conservative, and refusing to open a table because
    /// eight bytes went bad would be a worse answer than handing out ids from a
    /// slightly higher floor.
    async fn load_id_sequence(io_sequence: &Arc<S>) -> Option<u64> {
        if io_sequence.get_len().await < 8 {
            return None;
        }
        let mut buffer = [0u8; 8];
        let mut position = 0u64;
        match io_sequence
            .read_data_into_buffer(&mut position, &mut buffer)
            .await
        {
            Ok(()) => Some(u64::from_le_bytes(buffer)),
            Err(e) => {
                error!(
                    "Could not read the id sequence ({}); recovering from the pointer file",
                    e
                );
                None
            }
        }
    }

    /// Reserves the next block of ids on disk.
    ///
    /// Written before any id in the block is used, so a crash can only ever skip
    /// ids, never repeat one. Ids need to be unique, not gapless.
    async fn reserve_ids(&self, upto: u64) {
        let _one = self.reserving.lock().await;
        if upto <= self.id_reserved_upto.load(AtomicOrdering::Acquire) {
            return;
        }
        self.io_sequence.write_data(0, &upto.to_le_bytes()).await;
        self.id_reserved_upto.store(upto, AtomicOrdering::Release);
    }

    /// Makes sure the sequence on disk covers the ids about to be handed out.
    ///
    /// Called before an update relocates, whose new versions draw from the
    /// counter but keep their row's own id, so a gap there is harmless. An
    /// insert, which keeps the id it draws, uses [`Self::reserve_insert_id`].
    async fn ensure_ids_reserved(&self) {
        let next = self.last_row_id.load(AtomicOrdering::Acquire);
        if next < self.id_reserved_upto.load(AtomicOrdering::Acquire) {
            return;
        }
        self.reserve_ids(next + ID_RESERVATION_CHUNK).await;
    }

    /// The highest id the sequence reservation covers, for tests.
    #[cfg(test)]
    pub(crate) fn ids_reserved_upto(&self) -> u64 {
        self.id_reserved_upto.load(AtomicOrdering::Acquire)
    }

    /// Covers on disk the id an insert is about to take, counting every other
    /// insert that has checked but not yet taken one. Hold the guard until the
    /// id is taken.
    ///
    /// Inserts used to take turns here under the table's one uniqueness lock;
    /// with that lock striped by key, two of them check and allocate side by
    /// side, and checking `next < reserved` alone let the second take an id the
    /// disk did not cover, which a crash then handed out twice.
    pub(crate) async fn reserve_insert_id(&self) -> IdsInFlight<'_> {
        let in_flight = self.ids_in_flight.fetch_add(1, AtomicOrdering::AcqRel) + 1;
        let guard = IdsInFlight(&self.ids_in_flight);
        let next = self.last_row_id.load(AtomicOrdering::Acquire);
        if next + in_flight > self.id_reserved_upto.load(AtomicOrdering::Acquire) {
            self.reserve_ids(next + in_flight + ID_RESERVATION_CHUNK).await;
        }
        guard
    }

    /// Registers this read with the epoch registry, so reclamation cannot hand
    /// out an extent the read is about to fetch from.
    ///
    /// Inert when reclamation is off: nothing ever moves, so there is nothing
    /// to shield against and no reason to pay for the bookkeeping.
    #[inline]
    fn read_guard(&self) -> ReadTicket {
        if self.free_space.is_enabled() {
            self.readers.enter()
        } else {
            ReadTicket::inert()
        }
    }

    /// Puts this table under MVCC.
    ///
    /// Until this is called, reclamation treats the `deleted` flag as the whole
    /// truth, which is what it meant before versioning existed.
    ///
    /// # Precondition
    ///
    /// `DELETE` must already stamp `xmax` rather than setting `deleted`. A table
    /// that still writes the flag would have reclamation read it as "deleted
    /// before versioning, so no snapshot can see it" and free rows an open
    /// transaction is about to read. The two changes belong in one step; this is
    /// not a switch to flip ahead of the write path.
    pub fn enable_versioning(&self, manager: StdArc<TransactionManager>) {
        // Registered with the manager's forget registry at the horizon already
        // forgotten — which the files this table opens satisfy, having been
        // part of a registered table when that horizon was reached. Once per
        // table object: a second call would register a second handle.
        let _ = self.forget.set(manager.register_table());
        self.versions.store(Some(manager));
    }

    /// This table's forget horizon, once versioning is enabled.
    pub fn forget_horizon(&self) -> Option<TxId> {
        self.forget.get().map(|h| h.horizon())
    }

    /// Runs a forget sweep now. See [`crate::core::vacuum::neutralize_aborted`].
    pub async fn neutralize_aborted_now(&self) -> std::io::Result<crate::core::vacuum::NeutralizeReport> {
        let (Some(manager), Some(handle)) = (self.versions.load_full(), self.forget.get()) else {
            return Ok(Default::default());
        };
        let parts = crate::core::vacuum::NeutralizeParts {
            io_pointers: &*self.io_pointers,
            write_lock: &self.write_lock,
            compaction_gate: &self.compaction_gate,
            readers: &self.readers,
            free_space: &self.free_space,
            cache_generation: &self.cache_generation,
            dead_stamps: &self.dead_stamps,
            drain_timeout: std::time::Duration::from_secs(crate::vacuum_drain_timeout_secs()),
        };
        crate::core::vacuum::neutralize_aborted(&parts, &manager, handle).await
    }

    /// Notes that `written_by` has stamped a version here.
    ///
    /// Its outcome record has to be on disk before the write is reported as
    /// successful, and only the transactions that reach this need pay for that
    /// — see [`TransactionManager::mark_wrote`]. Called from the write paths
    /// rather than decided from the shape of the statement, because a `SELECT`
    /// with a data-modifying CTE writes rows through exactly these paths.
    fn note_write(&self, written_by: TxId) {
        if let Some(manager) = self.versions.load().as_ref() {
            manager.mark_wrote(written_by);
        }
    }

    /// Appended to a write-conflict message when the statement had applied
    /// NOTHING before it conflicted.
    ///
    /// PostgreSQL does not abort the second writer of a row: under
    /// `READ COMMITTED` it blocks until the first transaction ends and then
    /// re-evaluates **that statement**, leaving earlier statements of the same
    /// transaction untouched. Re-running a statement that has written nothing
    /// yet is exactly equivalent to having blocked, so the executor may do it;
    /// re-running one that had already applied rows would double-apply them,
    /// which is why the marker is conditional. Stripped before the message
    /// reaches a client.
    pub(crate) const CONFLICT_RETRYABLE: &'static str = " [retryable]";

    /// Refuses a write to a version another transaction has already retired.
    ///
    /// Without this, two transactions can update the same row and both succeed:
    /// the second one's snapshot still shows the *old* version, so it relocates
    /// from the same base and appends a second replacement. Once both commit,
    /// two versions of one row are visible and the row is duplicated — and the
    /// update that was overwritten is silently lost.
    ///
    /// First updater wins. The second is told to retry rather than being made to
    /// wait, which is what PostgreSQL does under `REPEATABLE READ`; blocking
    /// until the first transaction ends would need a lock manager this engine
    /// does not have.
    fn check_write_conflict(&self, pointer: &RowPointer, writer: TxId) -> Result<()> {
        if pointer.xmax == NO_TX || pointer.xmax == writer {
            return Ok(());
        }
        let Some(manager) = self.versions.load_full() else {
            return Ok(());
        };
        // An aborted retirement never happened, so the version is still ours to
        // take.
        if manager.outcome(pointer.xmax) == crate::core::mvcc::TxOutcome::Aborted {
            return Ok(());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            format!(
                "could not serialize access due to concurrent update of row {} \
                 of relation \"{}\" (transaction {} got there first)",
                pointer.row_id(),
                self.schema.name,
                pointer.xmax
            ),
        )
        .into())
    }

    /// The visibility rule for a statement running under `tx`.
    ///
    /// An unversioned table admits every untombstoned row, which is what it did
    /// before MVCC existed. `FROZEN_TX` on a *versioned* table still takes a
    /// snapshot: the statement is outside any transaction, but other
    /// transactions may be in flight and their rows must stay hidden.
    pub fn visibility_for(&self, tx: TxId) -> Visibility {
        match self.versions.load_full() {
            Some(manager) => Visibility::for_snapshot(manager.snapshot(tx), manager),
            None => Visibility::All,
        }
    }

    /// The rule for `tx` as of now, ignoring a snapshot it has pinned.
    ///
    /// For a rewrite of the table rather than a query of it: every row that is
    /// committed now, plus `tx`'s own uncommitted ones. See
    /// `TransactionManager::snapshot_now` for why a pinned snapshot is the
    /// wrong lens for that.
    pub fn visibility_now_for(&self, tx: TxId) -> Visibility {
        match self.versions.load_full() {
            Some(manager) => Visibility::for_snapshot(manager.snapshot_now(tx), manager),
            None => Visibility::All,
        }
    }

    /// Samples what the transaction system can vouch for, for one sweep.
    #[inline]
    pub fn version_horizon(&self) -> VersionHorizon {
        match self.versions.load_full() {
            Some(manager) => VersionHorizon::sample(&manager),
            None => VersionHorizon::unversioned(),
        }
    }

    /// Runs a reclamation scan now, ignoring the size thresholds the background
    /// service applies.
    ///
    /// Publishing still waits for queries older than the scan to finish; if
    /// they do not within the configured drain timeout the report comes back
    /// with `deferred` set and nothing is published.
    pub async fn vacuum_now(&self) -> Result<VacuumReport> {
        let config = VacuumConfig::default();
        let horizon = self.version_horizon();
        let index_fetch = self
            .indexes
            .as_ref()
            .map(|ix| Self::build_fetch_for(&self.schema.fields, &ix.columns()));
        let ctx = crate::core::vacuum::ReclaimContext {
            horizon: &horizon,
            filters: self.filters.as_deref(),
            cleared_below: &self.cleared_below,
            indexes: self.indexes.as_deref().zip(index_fetch.as_ref()),
        };
        let report = crate::core::vacuum::run_once(
            &*self.io_pointers,
            &*self.io_rows,
            &self.free_space,
            &self.readers,
            &ctx,
            &config,
            true,
        )
        .await?;

        if report.fingerprints_cleared > 0 {
            if let Some(filters) = self.filters.as_ref() {
                filters.request_checkpoint();
            }
        }
        Ok(report)
    }

    /// Rewrites the table so live rows and live pointer records sit in one
    /// contiguous run at the front of their files, then gives the tail back to
    /// the filesystem.
    ///
    /// This is the counterpart to the background service, which fills holes but
    /// never closes them: a heap fragmented into holes smaller than the rows
    /// being inserted stays large, tombstoned pointer records are scanned
    /// forever, and neither file ever shrinks. Compaction addresses all three,
    /// at the cost of stopping every other operation on the table for its
    /// duration — the same trade `VACUUM FULL` makes.
    ///
    /// # In place, and safe to interrupt
    ///
    /// No temporary copy of the table is made. Rows are visited in increasing
    /// source position and every target is at or below its source, so a move can
    /// never overwrite bytes a later move still needs.
    ///
    /// Each move is its own transaction, and each leaves the table valid rather
    /// than merely leaving it recoverable:
    ///
    /// * A row move writes the bytes at the new offset and updates the pointer
    ///   to match, together. A pointer never names bytes that are not its own.
    /// * A slot move writes the record at its new slot and tombstones the old
    ///   one, together. The record is live at exactly one slot throughout — a
    ///   copy that left the source live would make the row appear twice.
    ///
    /// So a crash mid-compaction loses only the batch in flight. What is left is
    /// a table that is partly compacted and entirely correct, and the next run
    /// picks up where this one stopped.
    ///
    /// Truncation happens last, after the log is checkpointed so that no
    /// unreplayed record still refers to the range being dropped. A provider
    /// that cannot truncate reports `truncation_unsupported` and keeps the
    /// tail — the space is reusable either way, just not returned.
    pub async fn compact(&self) -> Result<CompactReport> {
        // Compaction is the one writer whose effects the log cannot describe:
        // it moves rows and then shortens the file, and a truncation has no
        // redo record. A backup's fuzzy copy is only made consistent by replay,
        // so it holds this gate for its duration and compaction waits.
        #[cfg(feature = "s3_backup")]
        let _backup = crate::core::backup::compaction_permit().await;

        // Exclusive: rows are about to move, so nothing may be holding a pointer
        // to one. Every read and write path takes this shared.
        let _gate = self.compaction_gate.write().await;

        // Nothing on the free list survives compaction — every extent it names
        // is about to be somewhere else.
        self.free_space.clear();

        // The checkpoint on disk describes a slot layout that is about to stop
        // existing. Marking it dirty first means a crash mid-compaction leaves a
        // sidecar that is rejected and rebuilt, rather than one that confidently
        // indexes rows by their former addresses.
        if let Some(filters) = self.filters.as_ref() {
            filters.mark_disk_dirty(&*self.io_filters).await;
        }

        let rows_len = self.io_rows.get_len().await;
        let (live, dead_rows) =
            collect_live(&*self.io_pointers, rows_len, &self.version_horizon()).await?;

        let mut report = CompactReport {
            live_rows: live.len() as u64,
            dead_rows,
            ..Default::default()
        };

        let (moves, compacted_rows_len) = plan_row_moves(&live);
        let mut pointers = live;

        // Every way out of the two move phases ends the open storage
        // transaction. One left open after a failed write holds the log's
        // pending count up, and the log can then never be checkpointed again —
        // which is how a deployed node's log grew without bound.
        macro_rules! or_abort {
            ($result:expr, $txn:expr) => {
                match $result {
                    Ok(value) => value,
                    Err(e) => {
                        self.io_pointers.abort_txn($txn).await;
                        return Err(e.into());
                    }
                }
            };
        }

        // --- Phase 1: pack the row bytes ---
        let mut buffer: Vec<u8> = Vec::new();
        let mut txn = self.io_rows.begin_txn();

        for (index, target) in moves {
            let record = &mut pointers[index];
            let length = record.pointer.length as usize;

            buffer.clear();
            buffer.resize(length, 0);
            let mut read_at = record.pointer.position;
            if let Err(e) = self
                .io_rows
                .read_data_into_buffer(&mut read_at, &mut buffer)
                .await
            {
                error!(
                    "Compaction could not read row at {}: {}; leaving it in place",
                    record.pointer.position, e
                );
                continue;
            }

            // Bytes and pointer in one transaction: a pointer that survived
            // without its bytes would name someone else's row.
            let _ = self.io_rows.write_data_txn(txn, target, &buffer).await;
            record.pointer.position = target;
            let token = or_abort!(
                record
                    .pointer
                    .save_at_in_txn(
                        &*self.io_pointers,
                        record.slot * ROW_POINTER_RECORD_LEN as u64,
                        txn,
                    )
                    .await,
                txn
            );
            let _ = token;

            report.rows_moved += 1;

            // A failed commit has to stop the compaction, not just be noted.
            // Targets are at or below their sources, so the *next* row's move
            // can land on the bytes this one was supposed to have vacated — if
            // this batch is not durable and the next one is, a restart finds a
            // pointer naming bytes that were overwritten. Interrupting is safe;
            // continuing past a failure is not.
            if crate::core::wal::should_break_transaction(&*self.io_rows) {
                or_abort!(self.io_pointers.commit_durable(txn).await, txn);
                txn = self.io_rows.begin_txn();
            }
        }
        or_abort!(self.io_pointers.commit_durable(txn).await, txn);

        // --- Phase 2: pack the pointer records ---
        //
        // `collect_live` returns records in slot order, and the target lags the
        // source by however many tombstones have been passed, so a destination
        // is always at or below its source and always lands on a slot that holds
        // a tombstone. No live record is ever written over.
        //
        // The filter is not updated per move — every slot address changes, so it
        // is rebuilt wholesale at the end. That renumbering is exactly why this
        // cannot run incrementally in the background alongside a live filter.
        let mut txn = self.io_pointers.begin_txn();
        let mut target_slot = 0u64;

        for record in pointers.iter_mut() {
            if record.slot == target_slot {
                target_slot += 1;
                continue;
            }

            let destination = target_slot * ROW_POINTER_RECORD_LEN as u64;
            let source = record.slot * ROW_POINTER_RECORD_LEN as u64;

            // Write the record at its new slot, then tombstone the old one, in
            // one transaction, so a crash cannot leave the row live at both. The
            // log defers both writes until the commit, so the pair is never
            // half-applied on disk; without a log the two land separately, but
            // nothing can observe the gap — compaction holds the gate
            // exclusively — and a crash without a log has no atomicity anywhere.
            let _ = or_abort!(
                record
                    .pointer
                    .save_at_in_txn(&*self.io_pointers, destination, txn)
                    .await,
                txn
            );

            let mut tombstone = record.pointer.clone();
            tombstone.deleted = true;
            #[cfg(feature = "enable_long_row")]
            {
                tombstone.is_active = false;
            }
            let _ = or_abort!(
                tombstone
                    .save_at_in_txn(&*self.io_pointers, source, txn)
                    .await,
                txn
            );

            record.slot = target_slot;
            target_slot += 1;
            report.slots_moved += 1;

            if crate::core::wal::should_break_transaction(&*self.io_pointers) {
                or_abort!(self.io_pointers.commit_durable(txn).await, txn);
                txn = self.io_pointers.begin_txn();
            }
        }
        // Nothing is truncated past this point unless every move is durable.
        or_abort!(self.io_pointers.commit_durable(txn).await, txn);

        // --- Phase 3: give the tails back ---
        //
        // After the log is checkpointed, so no unreplayed record can re-extend
        // a file past the point we are about to cut it at.
        if let Some(wal) = self
            .io_pointers
            .get_location()
            .and_then(|location| crate::core::wal::wal_for(&location))
        {
            {
                // A checkpoint declines while another is running or while any
                // transaction holds an in-place write — both routine under
                // write load, where one declined attempt used to make the whole
                // VACUUM FULL a silent no-op. So it is retried for a while
                // before the tails are kept.
                const ATTEMPTS: u32 = 40;
                let mut outcome = Ok(false);
                for attempt in 0..ATTEMPTS {
                    let w = wal.clone();
                    outcome = tokio::task::spawn_blocking(move || w.checkpoint())
                        .await
                        .unwrap_or_else(|e| Err(std::io::Error::other(e.to_string())));
                    if !matches!(outcome, Ok(false)) {
                        break;
                    }
                    if attempt + 1 < ATTEMPTS {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            25 * (attempt as u64 + 1).min(8),
                        ))
                        .await;
                    }
                }
                match outcome {
                    // Only a checkpoint that actually ran makes truncation safe.
                    // A declined one (`Ok(false)`) leaves live records that
                    // still name offsets past the cut, so a crash would replay
                    // them back beyond the new end — the tail must stay.
                    Ok(true) => {}
                    Ok(false) => {
                        log::warn!(
                            "Compaction: checkpoint declined (log still active); keeping both tails"
                        );
                        report.truncation_unsupported = true;
                        report.truncation_skipped = Some(
                            "the write-ahead log could not be checkpointed: other \
                             transactions kept it busy for the whole attempt",
                        );
                    }
                    Err(e) => {
                        error!(
                            "Compaction could not checkpoint the log ({}); keeping both tails",
                            e
                        );
                        report.truncation_unsupported = true;
                        report.truncation_skipped =
                            Some("the write-ahead log could not be checkpointed (see the server log)");
                    }
                }
            }
        }

        let compacted_pointer_len = target_slot * ROW_POINTER_RECORD_LEN as u64;
        if report.truncation_skipped.is_some() {
            // Counted where `SHOW TABLE STATS` reads it: the statement reports
            // it too, but a scheduled run has no client to tell.
            self.vacuum_stats
                .full_untruncated
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
        if !report.truncation_unsupported {
            report.rows_bytes_freed = self
                .truncate_tail(&self.io_rows, compacted_rows_len, &mut report)
                .await;
            report.pointer_bytes_freed = self
                .truncate_tail(&self.io_pointers, compacted_pointer_len, &mut report)
                .await;
        }

        // The high-water mark and the id sequence both described the old layout.
        self.len.store(compacted_rows_len, AtomicOrdering::Release);
        if let Some(last) = pointers.last() {
            self.last_row_id
                .fetch_max(last.pointer.row_id(), AtomicOrdering::AcqRel);
        }

        // Every slot address changed, so the filter now indexes rows by
        // addresses they no longer have. Rebuilding from storage is the only
        // honest answer, and `backfill_row` re-sets each slot rather than
        // merging into it, so the old bits do not survive the rewrite. Slots
        // past the new end keep stale bits, but a query bounds itself by the
        // pointer file's length and never reads them, and a later insert that
        // reaches one overwrites it.
        if self.filters.is_some() {
            self.filters_ready.store(false, AtomicOrdering::Release);
            if let Err(e) = self.backfill_filters_from(0).await {
                error!("Compaction could not rebuild the filter: {}", e);
            }
            self.filters_ready.store(true, AtomicOrdering::Release);
            self.checkpoint_filters().await;
        }

        // The secondary indexes name slots too, every one of which moved.
        // Rebuilt under the same exclusive gate, so no query can ask a
        // half-built index; one that fails to rebuild stays unready, and
        // queries scan rather than trust slot numbers that no longer exist.
        if let Some(indexes) = self.indexes.as_ref() {
            indexes.set_ready(false);
            indexes.clear();
            match build_indexes(
                indexes,
                &self.io_pointers,
                &self.io_rows,
                &self.schema.fields,
                self.free_space.is_enabled().then_some(&self.readers),
            )
            .await
            {
                Ok(_) => indexes.set_ready(true),
                Err(e) => error!("Compaction could not rebuild the indexes; queries scan: {}", e),
            }
        }

        // Every slot address moved and the filter was rebuilt from storage, so
        // nothing has been cleared with respect to the new numbering.
        self.cleared_below.store(0, AtomicOrdering::Release);

        // Slot addresses moved, so every cached answer keyed by one is stale.
        self.invalidate_query_caches();
        self.write_generation.fetch_add(1, AtomicOrdering::AcqRel);

        info!(
            "Compacted {}: {} live rows kept, {} tombstones dropped, {} rows and {} slots moved, \
             {} + {} bytes returned",
            self.schema.name,
            report.live_rows,
            report.dead_rows,
            report.rows_moved,
            report.slots_moved,
            report.rows_bytes_freed,
            report.pointer_bytes_freed
        );

        Ok(report)
    }

    /// Shortens one file, tolerating a provider that cannot.
    async fn truncate_tail(&self, io: &Arc<S>, new_len: u64, report: &mut CompactReport) -> u64 {
        let before = io.get_len().await;
        if new_len >= before {
            return 0;
        }
        match io.truncate(new_len).await {
            Ok(()) => before - new_len,
            Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {
                report.truncation_unsupported = true;
                0
            }
            Err(e) => {
                error!("Compaction could not truncate to {}: {}", new_len, e);
                0
            }
        }
    }

    /// Cumulative reclamation counters for this table.
    #[inline]
    pub fn vacuum_stats(&self) -> &VacuumStats {
        &self.vacuum_stats
    }

    /// Row versions the write path has stamped dead since the table opened.
    /// See the field for what counts.
    pub fn dead_stamps(&self) -> u64 {
        self.dead_stamps.load(AtomicOrdering::Relaxed)
    }

    /// Bytes currently on the free list, waiting to be written into.
    ///
    /// Not the same as dead space: this counts what the last scan published and
    /// no writer has claimed since. Dead space that a scan has not reached, or
    /// withheld because an older query was running, is not included.
    #[inline]
    pub fn reclaimable_bytes(&self) -> u64 {
        self.free_space.free_bytes()
    }

    /// Whether reclamation is running for this table.
    ///
    /// Per table rather than per database, which is why it is reported per
    /// table: `SHOW CONFIG` can only answer "per table" for `vacuum.enabled`,
    /// and without this there was nowhere to find out which tables that meant.
    pub fn vacuum_enabled(&self) -> bool {
        self.free_space.is_enabled()
    }

    /// Turns reclamation on or off for this table without restarting.
    ///
    /// Switching off drops the free list, so nothing further is handed out, and
    /// queries stop registering epochs from their next call. Switching back on
    /// starts from an empty list; the next scan repopulates it.
    pub fn set_vacuum_enabled(&self, enabled: bool) {
        self.free_space.set_enabled(enabled);
    }

    /// Writes just the assigned columns' bytes, leaving the rest of the row
    /// where it is.
    ///
    /// Valid only when every assignment is fixed-width: a string's bytes live in
    /// the variable-length region after the fixed columns, so changing one moves
    /// everything after it and the row's length with it.
    ///
    /// Returns `false` if it declines, in which case the caller must take the
    /// general path — declining is always safe, taking it wrongly is not.
    async fn write_columns_in_place(
        &self,
        pointer: &RowPointer,
        updates: &smallvec::SmallVec<[ColumnWritePayload; 32]>,
        txn: u64,
        written_by: TxId,
    ) -> bool {
        // The long-row layout stores a checksum over the whole row, which this
        // path does not have the bytes to recompute. Rewriting the row is what
        // keeps that field honest, so leave it to the general path.
        if cfg!(feature = "enable_long_row") {
            return false;
        }

        // Under versioning, overwriting bytes of a version that stays visible to
        // the same snapshots is only safe when there are no snapshots — and the
        // check has to be atomic with the write. A transaction beginning between
        // them gets a snapshot that reads the new bytes for a row it is entitled
        // to see unchanged, which is a repeatable-read violation that needs
        // nothing uncommitted to happen.
        //
        // The guard holds `begin` off until the write lands. It owns an `Arc`
        // and a counter, never a lock, so carrying it across the awaits below
        // costs nothing and cannot deadlock.
        let _in_place = match self.versions.load_full() {
            // The writer's own transaction is not a reader to be protected from
            // its own write. Testing for an empty `running` set instead would
            // decline every time, because an autocommit statement now runs under
            // a transaction of its own.
            Some(manager) => match manager.try_begin_in_place_for(written_by) {
                Some(guard) => Some(guard),
                // Something is running and may be able to tell. Write a new
                // version instead.
                None => return false,
            },
            None => None,
        };

        let calc = SchemaCalculator;
        let mut writes: smallvec::SmallVec<[(u64, &[u8]); 8]> = smallvec::SmallVec::new();

        for upd in updates.iter() {
            let Some(field) = self
                .schema
                .fields
                .iter()
                .find(|f| f.write_order as u32 == upd.write_order && !f.is_deleted)
            else {
                return false;
            };

            let bytes = upd.data.into_slice();
            // `RowPointer::validate` enforces this on the general path; here the
            // check is what keeps a short payload from leaving the column half
            // written with no way to tell afterwards.
            if bytes.len() != field.size as usize {
                return false;
            }

            let (offset, _) = calc.calculate_schema_offset(&field.name, &self.schema.fields);
            let offset = offset as u64;
            let end = offset.saturating_add(field.size as u64);
            if end > pointer.length as u64 {
                return false;
            }
            writes.push((pointer.position + offset, bytes));
        }

        for (position, bytes) in writes {
            let _ = self.io_rows.write_data_txn(txn, position, bytes).await;
        }
        self.in_place_column_updates
            .fetch_add(1, AtomicOrdering::Relaxed);
        true
    }

    /// How many updates so far wrote only their assigned columns.
    #[inline]
    pub fn in_place_column_updates(&self) -> u64 {
        self.in_place_column_updates.load(AtomicOrdering::Relaxed)
    }

    /// Rejects a write that would put a duplicate in a `UNIQUE` column.
    ///
    /// `exclude_id` is the row being updated, which is allowed to keep the value
    /// it already has.
    ///
    /// Probing goes through the filter: a `column = value` predicate narrows the
    /// table to a handful of candidate slots, and each is then compared exactly,
    /// so the answer is never the filter's guess. With no filter — disabled, or
    /// a column type it cannot index — this degrades to a scan, which is slow
    /// but still correct, and says so in the log.
    /// Records a table-level `UNIQUE` constraint on the already-loaded table.
    ///
    /// Also arms `has_unique`. That flag gates whether the write lock is taken
    /// at all, so a constraint that were known but left unarmed would be
    /// checked outside the lock — which is not a check, since two concurrent
    /// inserts would each look, each find nothing, and each write.
    pub fn register_unique_key(&mut self, key: crate::core::rql::unique_key::UniqueKey) {
        self.schema.unique_keys.push(key);
        self.has_unique = true;
    }

    /// The `__null_` companion of `field`, when the column is nullable.
    fn null_companion(&self, field: &SchemaField) -> Option<&SchemaField> {
        let flag_name = crate::core::rql::nullable::flag_column_name(&field.name);
        self.schema
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&flag_name))
    }

    /// Whether the value a write carries for a UNIQUE column is SQL NULL.
    ///
    /// Decided by the companion, never by the bytes alone. Emptiness used to
    /// decide it, and an empty *string* is a value: a UNIQUE text column took
    /// `''` any number of times. A NOT NULL column has no companion and no
    /// NULL. Only a nullable column whose write carries no companion falls back
    /// on emptiness — there it is the only evidence there is.
    fn written_value_is_null(&self, row_write: &RowWrite, field: &SchemaField, value: &[u8]) -> bool {
        let Some(flag) = self.null_companion(field) else {
            return false;
        };
        match row_write
            .columns_writing_data
            .iter()
            .find(|c| c.write_order == flag.write_order as u32)
        {
            Some(c) => crate::core::rql::nullable::flag_is_set(c.data.into_slice()),
            None => value.is_empty(),
        }
    }

    async fn check_unique(
        &self,
        row_write: &RowWrite,
        exclude_id: Option<u64>,
        visibility: &Visibility,
        // Whose write this is, so a value another transaction is still writing
        // is told apart from one that is committed. See the in-flight branch in
        // `find_row_with_value`.
        written_by: TxId,
        // Which columns the statement actually assigns, by `write_order`, or
        // `None` for an insert — where every column is new.
        //
        // A constraint none of whose columns are being assigned cannot be
        // violated by this statement: the row keeps the values it already had,
        // and those already satisfied the constraint. Re-checking them can only
        // produce a false positive, never catch a real violation — which is
        // exactly what it did. An `UPDATE … SET status = …` that never mentions
        // `id` was rejected for a duplicate `id`, on a table where every `id`
        // was unique. PostgreSQL revalidates a constraint only when a column it
        // covers is written, and for the same reason.
        assigned: Option<&std::collections::HashSet<u32>>,
    ) -> Result<()> {
        if !self.has_unique {
            return Ok(());
        }
        let touches = |write_order: u64| -> bool {
            assigned.is_none_or(|set| set.contains(&(write_order as u32)))
        };

        for field in self
            .schema
            .fields
            .iter()
            .filter(|f| f.is_unique && !f.is_deleted && touches(f.write_order))
        {
            let Some(payload) = row_write
                .columns_writing_data
                .iter()
                .find(|c| c.write_order == field.write_order as u32)
            else {
                continue;
            };

            let value = payload.data.into_slice();
            // An absent value is this engine's stand-in for NULL, and SQL lets a
            // unique column hold any number of those.
            //
            // A *fixed-width* NULL is not absent, though: it is that type's
            // zeroes, and only the `__null_` companion says which it is. Left
            // to emptiness alone, a second row with a NULL uuid collided with
            // the first — they share sixteen zero bytes — and a nullable
            // UNIQUE column could hold exactly one NULL.
            if self.written_value_is_null(row_write, field, value) {
                continue;
            }

            if let Some((existing, in_flight)) = self
                .find_row_with_value(field, value, exclude_id, visibility, written_by)
                .await?
            {
                if in_flight {
                    // Another transaction is writing this value and has not
                    // finished. Whether that is a duplicate depends on whether
                    // it commits, and this engine has no lock manager to wait
                    // on it — so the statement is replayed instead, which is
                    // sound here for the reason the autocommit retry already
                    // relies on: the check runs *before* the write, so a
                    // statement rejected by it has applied nothing.
                    //
                    // Reporting `AlreadyExists` instead would be a guess, and
                    // the wrong one whenever the other transaction aborts —
                    // under `ON CONFLICT DO NOTHING` that guess is a silently
                    // skipped row rather than an error anyone would see.
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        format!(
                            "UNIQUE column {:?} is being written by another transaction{}",
                            field.name,
                            Self::CONFLICT_RETRYABLE
                        ),
                    ));
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!(
                        "duplicate value for UNIQUE column {:?} (row {} already holds it)",
                        field.name, existing
                    ),
                ));
            }
        }

        // Table-level constraints, which may span several columns. A composite
        // forbids repeating the whole tuple while leaving each column free to
        // repeat on its own, so it cannot be checked one column at a time.
        for key in self.schema.unique_keys.iter() {
            let mut tuple: Vec<(&SchemaField, &[u8])> = Vec::with_capacity(key.columns.len());
            let mut skip = false;

            for name in key.columns.iter() {
                let Some(field) = self
                    .schema
                    .fields
                    .iter()
                    .find(|f| f.name.eq_ignore_ascii_case(name) && !f.is_deleted)
                else {
                    // The column was dropped out from under the constraint.
                    // Enforcing what is left would be a different, stricter
                    // constraint than the one declared.
                    log::warn!(
                        "UNIQUE constraint {:?} names column {:?}, which no longer exists; not enforced",
                        key.name,
                        name
                    );
                    skip = true;
                    break;
                };

                let Some(payload) = row_write
                    .columns_writing_data
                    .iter()
                    .find(|c| c.write_order == field.write_order as u32)
                else {
                    skip = true;
                    break;
                };

                let value = payload.data.into_slice();
                // PostgreSQL's default is NULLS DISTINCT: a tuple containing a
                // NULL never conflicts with anything, however often it repeats.
                //
                // The companion decides, not the bytes. A fixed-width NULL is
                // that type's zeroes, so a second row with a NULL uuid looked
                // like a repeat of the first and a nullable `UNIQUE (col)` could
                // hold exactly one NULL — which is what stopped an import of a
                // table PostgreSQL was perfectly happy with.
                if self.written_value_is_null(row_write, field, value) {
                    skip = true;
                    break;
                }
                tuple.push((field, value));
            }

            // Same rule as the column flags above: a constraint the statement
            // does not write to cannot be violated by it.
            if skip || tuple.is_empty() || !tuple.iter().any(|(f, _)| touches(f.write_order)) {
                continue;
            }

            if let Some((existing, in_flight)) = self
                .find_row_with_tuple(&tuple, exclude_id, visibility, written_by)
                .await?
            {
                // Another transaction's unfinished write: replayed, not
                // guessed at — the single-column rule above, for the reason
                // given there.
                if in_flight {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        format!(
                            "UNIQUE constraint {} is being written by another transaction{}",
                            key.describe(),
                            Self::CONFLICT_RETRYABLE
                        ),
                    ));
                }
                // The value is named, not just the rows. A report of "row 634
                // duplicates row 625" is unfalsifiable from outside: it cannot
                // be told apart from data that really does hold a duplicate,
                // and a table with no duplicate values at all has produced it.
                // Printing what the two rows are said to share is what makes
                // the next occurrence diagnosable rather than another hunt.
                let shown: Vec<String> = tuple
                    .iter()
                    .map(|(field, value)| {
                        let rendered =
                            crate::core::rql::subquery::value_to_literal(&field.db_type, value)
                                .unwrap_or_else(|| format!("{:02x?}", value));
                        format!("{} = {}", field.name, rendered)
                    })
                    .collect();
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!(
                        "duplicate value for UNIQUE constraint {} (row {} already holds it): {}; \
                         the row being written is {:?}",
                        key.describe(),
                        existing,
                        shown.join(", "),
                        exclude_id
                    ),
                ));
            }
        }

        Ok(())
    }

    /// Id of a live row whose whole tuple equals `tuple`, other than
    /// `exclude_id`.
    ///
    /// The filter can only narrow on one column at a time, so the probe runs on
    /// the first column it indexes and every surviving candidate is then
    /// compared on *all* the columns. The mask is a superset either way, so
    /// which column is probed changes only how many rows are decoded, never the
    /// answer.
    async fn find_row_with_tuple(
        &self,
        tuple: &[(&SchemaField, &[u8])],
        exclude_id: Option<u64>,
        // Only rows this statement can see may block it, exactly as for a
        // single-column check: under MVCC a deleted row keeps its `deleted`
        // flag clear until reclamation, and would otherwise go on reserving the
        // value it held.
        visibility: &Visibility,
        // Whose write this is: see `find_row_with_value`, whose in-flight rule
        // this applies to a whole tuple.
        written_by: TxId,
    ) -> Result<Option<(u64, bool)>> {
        use crate::core::filter::sarg::{MaskExpr, SargPredicate, SargProgram};
        use crate::core::filter::zonemap::SargOp;

        let mut iterator = RowPointerIterator::new_deferred(self.io_pointers.clone())
            .await
            .unwrap();

        // An index over exactly these columns names the candidate slots; the
        // iterator then reads only the chunks holding them, and anything
        // appended since the sample is a candidate by the list's own rule.
        let from_index = match self.index_probe(tuple).await {
            Some((slots, n_slots)) => {
                let slots: Vec<u64> = slots.into_iter().filter(|s| *s < n_slots).collect();
                iterator.set_candidates(Some(Arc::new(CandidateBitmap::sparse(slots, n_slots))));
                true
            }
            None => false,
        };

        if let (false, Some(filters)) = (from_index, self.filters.as_ref()) {
            match tuple
                .iter()
                .find(|(field, _)| filters.indexes(field.write_order as u32))
            {
                Some((field, value)) => {
                    let program = SargProgram {
                        root: MaskExpr::Pred(SargPredicate {
                            write_order: field.write_order as u32,
                            op: SargOp::Eq,
                            value: value.to_vec(),
                        }),
                    };
                    let n_slots = self.io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64;
                    if let Some(bitmap) = filters.evaluate(&program, n_slots) {
                        iterator.set_candidates(Some(Arc::new(bitmap)));
                    }
                }
                None => log::debug!(
                    "UNIQUE check on ({}) cannot use the filter; scanning",
                    tuple.iter().map(|(f, _)| f.name.as_str()).join(", ")
                ),
            }
        }

        let calc = SchemaCalculator;
        let mut columns_fetching_data: SmallVec<[ColumnFetchingData; 32]> = SmallVec::new();
        for (field, _) in tuple.iter() {
            let (column_offset, schema_id) =
                calc.calculate_schema_offset(&field.name, &self.schema.fields);
            columns_fetching_data.push(ColumnFetchingData {
                column_offset,
                column_type: field.db_type.clone(),
                size: field.size,
                schema_id,
            });
        }
        // Kept alongside the fetch so the comparison below can match columns by
        // schema id rather than by position.
        let schema_ids: Vec<u64> = columns_fetching_data.iter().map(|c| c.schema_id).collect();
        // A stored NULL holds the type's zeroes — empty bytes for text — so a
        // row whose companion says NULL must not match a written `''`.
        let mut flag_ids: Vec<u64> = Vec::new();
        for (field, _) in tuple.iter() {
            if let Some(flag) = self.null_companion(field) {
                let (column_offset, schema_id) =
                    calc.calculate_schema_offset(&flag.name, &self.schema.fields);
                columns_fetching_data.push(ColumnFetchingData {
                    column_offset,
                    column_type: flag.db_type.clone(),
                    size: flag.size,
                    schema_id,
                });
                flag_ids.push(schema_id);
            }
        }
        let fetch = RowFetch {
            columns_fetching_data,
        };

        let mut row = Row::default();
        while let Ok(Some(pointer)) = iterator.next_row_pointer().await {
            if Some(pointer.row_id()) == exclude_id {
                continue;
            }
            // The same occupancy rule as a single column's (see
            // `find_row_with_value`): a row another transaction has written but
            // not committed is invisible to every snapshot, so judging a
            // composite key by the snapshot let concurrent inserts of one tuple
            // each find nothing and each write — 950 rows for 300 pairs from
            // four connections, with no error.
            let manager = self.versions.load_full();
            let in_flight = match (&manager, written_by) {
                (Some(manager), writer) if writer != crate::core::mvcc::transaction::NO_TX => {
                    use crate::core::mvcc::transaction::{NO_TX, TxOutcome};
                    let creator = manager.outcome(pointer.xmin);
                    let occupied = !pointer.deleted
                        && creator != TxOutcome::Aborted
                        && (pointer.xmax == NO_TX
                            || manager.outcome(pointer.xmax) != TxOutcome::Committed);
                    if !occupied {
                        continue;
                    }
                    // Our own uncommitted row is a real duplicate; only another
                    // transaction's unfinished write is worth waiting out.
                    creator == TxOutcome::InProgress && pointer.xmin != writer
                }
                _ => {
                    if !visibility.visible(pointer.deleted, pointer.xmin, pointer.xmax) {
                        continue;
                    }
                    false
                }
            };
            if pointer
                .fetch_row_reuse_async(&*self.io_rows, &fetch, &mut row)
                .await
                .is_err()
            {
                continue;
            }

            // A tuple conflicts only when every column matches. Columns are
            // matched by `schema_id` rather than by position, so the comparison
            // does not depend on the fetch preserving the requested order.
            let stored_null = flag_ids.iter().any(|id| {
                row.columns
                    .iter()
                    .find(|c| u64::from(c.schema_id) == *id)
                    .is_some_and(|c| crate::core::rql::nullable::flag_is_set(c.data.into_slice()))
            });
            let all_match = !stored_null
                && tuple.iter().zip(schema_ids.iter()).all(|((_, value), id)| {
                    row.columns
                        .iter()
                        .find(|c| u64::from(c.schema_id) == *id)
                        .is_some_and(|c| c.data.into_slice() == *value)
                });
            if all_match {
                return Ok(Some((pointer.row_id(), in_flight)));
            }
        }

        Ok(None)
    }

    /// The slots that may hold exactly this key, from an index over exactly
    /// these columns, and the table length sampled *before* the index was
    /// asked. A slot at or past that length was appended since and must be
    /// treated as a candidate too; the caller does.
    ///
    /// Sound for a uniqueness check because the check runs under the key's
    /// stripe of `unique_lock`, as does every write that could put that key
    /// under these columns: an earlier writer has finished, its index entry
    /// included, before this one looks.
    async fn index_probe(&self, columns: &[(&SchemaField, &[u8])]) -> Option<(Vec<u64>, u64)> {
        let indexes = self.indexes.as_ref()?;
        let wos: SmallVec<[u32; 4]> = columns.iter().map(|(f, _)| f.write_order as u32).collect();
        let index = indexes.exact(&wos)?;
        let n_slots = self.io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64;
        let key = index.key_of(|wo| {
            columns
                .iter()
                .find(|(f, _)| f.write_order as u32 == wo)
                .map(|(_, v)| *v)
        })?;
        Some((index.slots_for_key(&key), n_slots))
    }

    /// The `unique_lock` stripe for one constraint's key.
    ///
    /// Two values the uniqueness check calls equal must land on one stripe,
    /// so the key is hashed in the canonical form a hash index is proven to
    /// share with the evaluator's equality. A type without one puts its whole
    /// constraint on a single stripe, which is what the one lock was.
    fn constraint_stripe<'a>(constraint: usize, values: impl Iterator<Item = (&'a SchemaField, &'a [u8])>) -> usize {
        let mut canonical = Vec::with_capacity(32);
        let mut one = Vec::with_capacity(16);
        for (field, value) in values {
            one.clear();
            if crate::core::index::key::encode_canonical(&field.db_type, value, &mut one).is_none() {
                return crate::core::row::key_locks::KeyLocks::stripe_of(constraint, None);
            }
            canonical.extend_from_slice(&(one.len() as u32).to_le_bytes());
            canonical.extend_from_slice(&one);
        }
        crate::core::row::key_locks::KeyLocks::stripe_of(constraint, Some(&canonical))
    }

    /// The constraint id `constraint_stripe` takes for a table-level key.
    /// Column constraints use their column's `write_order`.
    fn unique_key_constraint(position: usize) -> usize {
        usize::MAX - position
    }

    /// The columns of a table-level unique key, or `None` when one was
    /// dropped (and the constraint is no longer enforced; see `check_unique`).
    fn unique_key_fields(&self, key: &crate::core::rql::unique_key::UniqueKey) -> Option<SmallVec<[&SchemaField; 4]>> {
        key.columns
            .iter()
            .map(|name| {
                self.schema
                    .fields
                    .iter()
                    .find(|f| f.name.eq_ignore_ascii_case(name) && !f.is_deleted)
            })
            .collect()
    }

    /// The `unique_lock` stripes a row's keys fall on: one per constraint it
    /// holds a key for. A NULL key conflicts with nothing, so it needs none.
    fn unique_stripes(&self, row_write: &RowWrite) -> SmallVec<[usize; 4]> {
        let value_of = |wo: u32| {
            row_write
                .columns_writing_data
                .iter()
                .find(|c| c.write_order == wo)
                .map(|c| c.data.into_slice())
        };
        let mut stripes = SmallVec::new();
        for field in self.schema.fields.iter().filter(|f| f.is_unique && !f.is_deleted) {
            let Some(value) = value_of(field.write_order as u32) else {
                continue;
            };
            if self.written_value_is_null(row_write, field, value) {
                continue;
            }
            stripes.push(Self::constraint_stripe(
                field.write_order as usize,
                std::iter::once((field, value)),
            ));
        }
        for (position, key) in self.schema.unique_keys.iter().enumerate() {
            let Some(fields) = self.unique_key_fields(key) else {
                continue;
            };
            let values: Option<SmallVec<[(&SchemaField, &[u8]); 4]>> = fields
                .iter()
                .map(|f| value_of(f.write_order as u32).map(|v| (*f, v)))
                .collect();
            let Some(values) = values else {
                continue;
            };
            if values.iter().any(|(f, v)| self.written_value_is_null(row_write, f, v)) {
                continue;
            }
            stripes.push(Self::constraint_stripe(
                Self::unique_key_constraint(position),
                values.into_iter(),
            ));
        }
        stripes
    }

    /// When an `UPDATE` names its rows by one unique key's value and assigns
    /// no column of any unique key: the `unique_lock` stripe of that key.
    ///
    /// Every row such a statement can match holds that key, since the
    /// extracted equality is implied by its `WHERE` (the same promise an index
    /// lookup rests on). So holding the stripe from before the scan to after
    /// the rewrite excludes every other writer of the rows it can touch:
    /// inserts and point updates of the key take the stripe, and a statement
    /// that could give some other row this key takes them all.
    fn point_update_stripe(
        &self,
        where_clause: &str,
        updates: &[ColumnWritePayload],
        expressions: &[(u32, crate::core::rql::expr::Expr)],
    ) -> Option<usize> {
        use crate::core::filter::sarg::MaskExpr;
        use crate::core::filter::zonemap::SargOp;

        if !self.has_unique || where_clause.trim().is_empty() {
            return None;
        }
        let mut keyed: SmallVec<[u32; 8]> = self
            .schema
            .fields
            .iter()
            .filter(|f| f.is_unique && !f.is_deleted)
            .map(|f| f.write_order as u32)
            .collect();
        for key in self.schema.unique_keys.iter() {
            keyed.extend(self.unique_key_fields(key)?.iter().map(|f| f.write_order as u32));
        }
        if updates.iter().any(|u| keyed.contains(&u.write_order))
            || expressions.iter().any(|(wo, _)| keyed.contains(wo))
        {
            return None;
        }

        let tokens = crate::core::tokenizer::query_tokenizer::try_tokenize(where_clause, &self.schema.fields).ok()?;
        let program = sarg::extract_gated(&tokens, &|wo| keyed.contains(&wo), &self.decimal_scales())?;
        let conjuncts: &[MaskExpr] = match &program.root {
            MaskExpr::And(children) => children,
            single @ MaskExpr::Pred(_) => std::slice::from_ref(single),
            _ => return None,
        };
        let eq = |wo: u32| {
            conjuncts.iter().find_map(|c| match c {
                MaskExpr::Pred(p) if p.op == SargOp::Eq && p.write_order == wo => Some(p.value.as_slice()),
                _ => None,
            })
        };

        for field in self.schema.fields.iter().filter(|f| f.is_unique && !f.is_deleted) {
            if let Some(value) = eq(field.write_order as u32) {
                return Some(Self::constraint_stripe(
                    field.write_order as usize,
                    std::iter::once((field, value)),
                ));
            }
        }
        for (position, key) in self.schema.unique_keys.iter().enumerate() {
            let fields = self.unique_key_fields(key)?;
            let values: Option<SmallVec<[(&SchemaField, &[u8]); 4]>> =
                fields.iter().map(|f| eq(f.write_order as u32).map(|v| (*f, v))).collect();
            if let Some(values) = values {
                return Some(Self::constraint_stripe(
                    Self::unique_key_constraint(position),
                    values.into_iter(),
                ));
            }
        }
        None
    }

    /// Id of a live row whose `field` equals `value`, other than `exclude_id`.
    async fn find_row_with_value(
        &self,
        field: &SchemaField,
        value: &[u8],
        exclude_id: Option<u64>,
        // Only rows this statement can see may block it. A version whose
        // deleting transaction has committed is gone, even though reclamation
        // has not yet reached it and its `deleted` flag is still clear.
        visibility: &Visibility,
        // The transaction doing the writing, so a row *it* wrote is told apart
        // from one a concurrent transaction wrote. `NO_TX` disables the
        // in-flight rule and leaves the plain snapshot test, which is what a
        // database with no transaction manager wants.
        written_by: TxId,
    ) -> Result<Option<(u64, bool)>> {
        use crate::core::filter::sarg::{MaskExpr, SargPredicate, SargProgram};
        use crate::core::filter::zonemap::SargOp;

        let mut iterator = RowPointerIterator::new(self.io_pointers.clone())
            .await
            .map_err(std::io::Error::from)?;

        // The slots to probe directly, when the filter narrows the table to
        // few enough of them. `None` walks the pointer file instead.
        //
        // Handing the mask to the iterator was correct but no faster than not
        // having one: the iterator still steps through every record of the
        // pointer file, copying each chunk, to test one bit per record. This
        // check runs once per inserted row, so loading a table with a primary
        // key was quadratic — a 5,000-row INSERT took 67 ms into an empty
        // table and 9.3 s into one of 200k rows, and a 2M-row load ran for
        // over half an hour. Reading the few candidate records by slot makes
        // the check O(candidates), plus a pass over the bitmap's words.
        let mut probe_slots: Option<std::vec::IntoIter<u64>> = None;

        // An index over exactly this column names the slots outright: a
        // descent rather than a pass over a bitmap of the whole table, so an
        // insert into a table with a primary key costs the same at a million
        // rows as at a thousand.
        if let Some((slots, n_slots)) = self.index_probe(&[(field, value)]).await {
            let now = self.io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64;
            let mut slots = slots;
            slots.extend(n_slots..now);
            probe_slots = Some(slots.into_iter());
        }

        // The mask is a superset, so a slot it clears genuinely cannot hold the
        // value; every slot it keeps is still compared byte for byte below.
        if let (None, Some(filters)) = (probe_slots.as_ref(), self.filters.as_ref()) {
            if filters.indexes(field.write_order as u32) {
                let program = SargProgram {
                    root: MaskExpr::Pred(SargPredicate {
                        write_order: field.write_order as u32,
                        op: SargOp::Eq,
                        value: value.to_vec(),
                    }),
                };
                let n_slots = self.io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64;
                if let Some(bitmap) = filters.evaluate(&program, n_slots) {
                    // Past `n_slots` the bitmap says nothing, so every slot
                    // appended since it was sampled is a candidate too.
                    let now = self.io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64;
                    let mut slots: Vec<u64> = Vec::new();
                    if !bitmap.is_nothing() {
                        for (w, &word) in bitmap.words().iter().enumerate() {
                            let mut bits = word;
                            while bits != 0 {
                                let slot = w as u64 * 64 + bits.trailing_zeros() as u64;
                                if slot < n_slots {
                                    slots.push(slot);
                                }
                                bits &= bits - 1;
                            }
                        }
                        slots.extend(n_slots..now);
                    }
                    // Many candidates: the sequential walk reads them in
                    // order and is the cheaper of the two.
                    if (slots.len() as u64) <= now / 64 + 64 {
                        probe_slots = Some(slots.into_iter());
                    } else {
                        iterator.set_candidates(Some(Arc::new(bitmap)));
                    }
                }
            } else {
                log::debug!(
                    "UNIQUE check on {:?} cannot use the filter; scanning",
                    field.name
                );
            }
        }

        let calc = SchemaCalculator;
        let (column_offset, schema_id) =
            calc.calculate_schema_offset(&field.name, &self.schema.fields);
        let mut columns_fetching_data: SmallVec<[ColumnFetchingData; 32]> =
            smallvec::smallvec![ColumnFetchingData {
                column_offset,
                column_type: field.db_type.clone(),
                size: field.size,
                schema_id,
            }];
        // A stored NULL holds the type's zeroes — empty bytes for text — and
        // must not match a written `''`: the companion tells them apart.
        let flag_id = self.null_companion(field).map(|flag| {
            let (column_offset, flag_schema_id) =
                calc.calculate_schema_offset(&flag.name, &self.schema.fields);
            columns_fetching_data.push(ColumnFetchingData {
                column_offset,
                column_type: flag.db_type.clone(),
                size: flag.size,
                schema_id: flag_schema_id,
            });
            flag_schema_id
        });
        let fetch = RowFetch {
            columns_fetching_data,
        };

        let mut row = Row::default();
        let mut record = [0u8; ROW_POINTER_RECORD_LEN];
        loop {
            // A read that fails is an error, never the end of the table: ending
            // early answers "no duplicate" for a value that may be there.
            let pointer = match probe_slots.as_mut() {
                Some(slots) => {
                    let Some(slot) = slots.next() else { break };
                    let mut at = slot * ROW_POINTER_RECORD_LEN as u64;
                    match self.io_pointers.read_data_into_buffer(&mut at, &mut record).await {
                        Ok(()) => {}
                        // A slot past the end: an append reserved it and has
                        // not written it yet, exactly what the sequential walk
                        // stops short of too.
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => continue,
                        Err(e) => return Err(e),
                    }
                    let pointer = RowPointer::from_slice(&record);
                    if pointer.deleted {
                        continue;
                    }
                    pointer
                }
                None => match iterator.next_row_pointer().await {
                    Ok(Some(pointer)) => pointer,
                    Ok(None) => break,
                    Err(e) => return Err(e.into()),
                },
            };
            if Some(pointer.row_id()) == exclude_id {
                continue;
            }
            // A uniqueness check is not a query, and a snapshot is the wrong
            // rule for it. A row another transaction has written but not yet
            // committed is invisible to every snapshot, so two concurrent
            // inserts of one key each looked, each found nothing, and each
            // wrote — the constraint did not hold, silently. The write lock
            // around the check cannot help: it serialises the writers, not what
            // they are permitted to see.
            //
            // So a row *occupies* the value when its creator has not aborted
            // and no committed transaction has deleted it. Deliberately a local
            // rule rather than a new `Visibility`: nothing outside this check
            // may adopt it, because seeing uncommitted rows is exactly wrong for
            // a scan, the query cache and reclamation.
            let manager = self.versions.load_full();
            let in_flight = match (&manager, written_by) {
                (Some(manager), writer) if writer != crate::core::mvcc::transaction::NO_TX => {
                    use crate::core::mvcc::transaction::{NO_TX, TxOutcome};
                    let creator = manager.outcome(pointer.xmin);
                    let occupied = !pointer.deleted
                        && creator != TxOutcome::Aborted
                        && (pointer.xmax == NO_TX
                            || manager.outcome(pointer.xmax) != TxOutcome::Committed);
                    if !occupied {
                        continue;
                    }
                    // Our own uncommitted row is a real duplicate, not a race:
                    // retrying would find it again. Only another transaction's
                    // unfinished write is worth waiting out.
                    creator == TxOutcome::InProgress && pointer.xmin != writer
                }
                _ => {
                    if !visibility.visible(pointer.deleted, pointer.xmin, pointer.xmax) {
                        continue;
                    }
                    false
                }
            };
            if pointer
                .fetch_row_reuse_async(&*self.io_rows, &fetch, &mut row)
                .await
                .is_err()
            {
                continue;
            }
            let stored_null = flag_id.is_some_and(|id| {
                row.columns
                    .iter()
                    .find(|c| u64::from(c.schema_id) == id)
                    .is_some_and(|c| crate::core::rql::nullable::flag_is_set(c.data.into_slice()))
            });
            if !stored_null
                && row
                    .columns
                    .iter()
                    .find(|c| u64::from(c.schema_id) == schema_id)
                    .is_some_and(|column| column.data.into_slice() == value)
            {
                return Ok(Some((pointer.row_id(), in_flight)));
            }
        }

        Ok(None)
    }

    /// Finds the live pointer carrying `id`, if the row still exists.
    ///
    /// A row's id is its identity for its whole life: it survives an `UPDATE`
    /// that relocates the row to a new slot, and it survives compaction, which
    /// renumbers every slot. A slot address does neither, so this is the only
    /// handle to a row that stays meaningful across maintenance — what an
    /// external mapping would have to key on.
    ///
    /// Linear in the pointer file. Intended for resolving a handle, not for
    /// query execution, which goes through the filter index.
    pub async fn locate_row_by_id(&self, id: u64) -> Option<(u64, RowPointer)> {
        const RECORD: usize = ROW_POINTER_RECORD_LEN;

        let _gate = self.compaction_gate.read().await;
        let _read = self.read_guard();

        let len = self.io_pointers.get_len().await;
        let end = len - (len % RECORD as u64);
        let mut position = 0u64;

        let mut buffer = [0u8; RECORD];
        while position + RECORD as u64 <= end {
            let mut read_at = position;
            if self
                .io_pointers
                .read_data_into_buffer(&mut read_at, &mut buffer)
                .await
                .is_err()
            {
                break;
            }
            let candidate = RowPointer::from_slice(&buffer);
            // At most one live pointer carries a given id, so the first live
            // match is the answer however many times the row has moved.
            if !candidate.deleted && candidate.row_id() == id {
                return Some((position, candidate));
            }
            position += RECORD as u64;
        }
        None
    }

    /// True once the maintenance task has finished its initial backfill.
    #[inline]
    pub fn filters_ready(&self) -> bool {
        self.filters_ready.load(AtomicOrdering::Acquire)
    }

    /// Await the initial backfill. Queries before it completes are correct —
    /// just not yet narrowed by the filter.
    pub async fn wait_filters_ready(&self) {
        while !self.filters_ready() {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    /// Read and validate the sidecar. Any problem returns `None` and the caller
    /// builds an empty catalog: a rejected checkpoint costs a scan, never a
    /// wrong answer.
    async fn load_filter_checkpoint(
        io_filters: &Arc<S>,
        fields: &SmallVec<[SchemaField; 20]>,
        now_slots: u64,
    ) -> Option<TableFilters> {
        let len = io_filters.get_len().await;
        if len == 0 {
            return None;
        }

        let mut buf = vec![0u8; len as usize];
        let mut pos = 0u64;
        if io_filters
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .is_err()
        {
            return None;
        }

        match TableFilters::from_checkpoint(&buf, fields, FilterConfig::default(), now_slots) {
            Ok(f) => Some(f),
            Err(reason) => {
                info!("Filter checkpoint not usable ({:?}); rebuilding", reason);
                None
            }
        }
    }

    /// Row fetch covering only columns the filter can index.
    fn build_indexed_row_fetch(schema_fields: &SmallVec<[SchemaField; 20]>) -> RowFetch {
        let schema_calc = SchemaCalculator::default();
        let mut columns_fetching_data = smallvec::SmallVec::new();
        for field in schema_fields
            .iter()
            .filter(|f| !f.is_deleted && is_indexable(&f.db_type))
            .sorted_by(|a, b| a.write_order.cmp(&b.write_order))
        {
            let offset = schema_calc.calculate_schema_offset(&field.name, schema_fields);
            columns_fetching_data.push(ColumnFetchingData {
                column_offset: offset.0,
                column_type: field.db_type.physical(),
                size: field.size,
                schema_id: offset.1,
            });
        }
        RowFetch {
            columns_fetching_data,
        }
    }

    /// Index every pre-existing row, then start trusting the filter for them.
    ///
    /// One full scan of the indexed columns. Deleted pointers are skipped, so
    /// their slots stay all-zero — which reads as "matches nothing" and lets the
    /// filter prune tombstoned rows before their pointer is even decoded.
    pub async fn backfill_filters(&self) -> Result<u64> {
        self.backfill_filters_from(0).await
    }

    /// Index rows from `start_slot` onwards. Resuming from a checkpoint's
    /// frontier makes reopening cost work proportional to what was written
    /// since, not to the size of the table.
    pub async fn backfill_filters_from(&self, start_slot: u64) -> Result<u64> {
        let Some(filters) = self.filters.as_ref() else {
            return Ok(0);
        };
        backfill_into(
            filters,
            &self.io_pointers,
            &self.io_rows,
            &self.schema.fields,
            start_slot,
            self.free_space.is_enabled().then_some(&self.readers),
        )
        .await
    }

    /// Persist the current filter state. Normally the maintenance task does
    /// this; call it directly when a checkpoint must be on disk before some
    /// specific point, such as a controlled shutdown.
    pub async fn checkpoint_filters(&self) {
        let Some(filters) = self.filters.as_ref() else {
            return;
        };
        let n_slots = self.io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64;
        filters.checkpoint(&*self.io_filters, n_slots).await;
    }

    /// The slot ceiling that answers `ORDER BY … LIMIT k` from the leading
    /// slots, or `None` when the ordering has to be produced by sorting.
    ///
    /// Every condition here is a *licence to skip work*, so each one that
    /// cannot be established returns `None` and the ordinary sort runs:
    ///
    /// * a limit, since without one the whole table is the answer regardless;
    /// * exactly one `ORDER BY` key, ascending — two keys order by the second
    ///   within ties of the first, which slot position says nothing about, and
    ///   `DESC` wants the *last* slots, which this does not express;
    /// * a live, non-dropped column of that name;
    /// * a zone map showing that column ascends with slot position, covering
    ///   every slot the scan can reach.
    ///
    /// The ceiling starts at the number of rows actually wanted. Deleted and
    /// invisible rows mean a run of `k` slots may hold fewer than `k` live
    /// rows, which the caller detects and widens.
    async fn monotonic_scan_ceiling(
        &self,
        order_by: &Option<String>,
        limit: Option<u64>,
        n_slots: u64,
    ) -> Option<u64> {
        let wanted = limit?;
        if wanted == 0 || n_slots == 0 {
            return None;
        }
        let filters = self.filters.as_ref()?;
        let spec = order_by.as_ref()?;

        let mut keys = crate::core::rql::order_by::parse_spec(spec).into_iter();
        let (name, descending) = keys.next()?;
        if descending || keys.next().is_some() {
            return None;
        }

        let write_order = self
            .schema
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&name))
            .map(|f| f.write_order)?;

        if !filters.column_is_monotonic(write_order as u32, n_slots) {
            return None;
        }

        // Rounded up to whole zones. Ascending zone bounds order the zones
        // against each other and say nothing about the rows inside one, so the
        // `k` smallest values are only known to lie within the first
        // `ceil(k / ZONE_ROWS)` zones — not in the first `k` slots. Bounded at
        // `k` slots, a column ascending across zones but not within them
        // (timestamps inserted out of order inside a batch) returned the wrong
        // rows: `ORDER BY f LIMIT 3` over values descending within each zone
        // answered slots 0, 1, 2 instead of 511, 510, 509.
        let zone = crate::core::filter::kernels::ZONE_ROWS as u64;
        let wanted = wanted.div_ceil(zone).saturating_mul(zone);
        // Already scanning everything: the bound would only add a mask.
        if wanted >= n_slots {
            return None;
        }
        Some(wanted)
    }

    /// Each `NUMERIC(p, s)` column's scale, where it is not the engine
    /// default, so a literal is bounded where the column stores it.
    fn decimal_scales(&self) -> smallvec::SmallVec<[(u32, u32); 4]> {
        self.schema
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

    /// The candidate slots a secondary index gives for a WHERE clause, or
    /// `None` when no index can narrow it, or none is built yet.
    pub(crate) async fn index_candidates(&self, where_clause: &str) -> Option<Arc<CandidateBitmap>> {
        let indexes = self.indexes.as_ref().filter(|i| i.is_ready())?;
        if where_clause.trim().is_empty() {
            return None;
        }
        // Sampled before the index is asked. A version appended after this
        // sits at or past `n_slots`, which the bitmap counts as a candidate;
        // one appended before it was indexed by its writer before it could
        // commit, so a snapshot that can see it finds it here.
        let n_slots = self.io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64;
        let tokens = crate::core::tokenizer::query_tokenizer::try_tokenize(where_clause, &self.schema.fields).ok()?;
        let scales = self.decimal_scales();
        let cap = crate::core::index::candidate_cap(n_slots);
        // Key indexes, with `doc->>'k'` read as the virtual column of a path
        // index that keys that path.
        let resolve = |document: u32, steps: &[crate::core::jsonb::Step]| indexes.path_column(document, steps);
        let from_keys = sarg::extract_gated_with_paths(&tokens, &|wo| indexes.covers(wo), &scales, &resolve)
            .and_then(|program| indexes.plan(&program.root, &self.schema.fields, cap));
        // Inverted indexes, for `@>`, `?`, `?|`, `?&` on a document column.
        let from_gin = if indexes.gin.is_empty() {
            None
        } else {
            crate::core::index::gin::parse_json_where(&tokens).and_then(|e| indexes.plan_json(&e, cap))
        };
        // Both are supersets of the matches, so where both narrow, so does
        // their intersection.
        let mut slots = match (from_keys, from_gin) {
            (Some(a), Some(b)) => crate::core::index::intersect_sorted(&a, &b),
            (Some(a), None) | (None, Some(a)) => a,
            (None, None) => return None,
        };
        slots.retain(|s| *s < n_slots);
        info!(
            "Index narrowed {} slots to {} candidates",
            n_slots,
            slots.len()
        );
        Some(Arc::new(CandidateBitmap::sparse(slots, n_slots)))
    }

    /// The filter index's mask for a clause, and the table length it covers:
    /// every slot that may match, before any judgement of whether a scan is
    /// better off with it.
    async fn filter_mask(&self, where_clause: &str) -> Option<(CandidateBitmap, u64)> {
        let filters = self.filters.as_ref()?;
        if where_clause.trim().is_empty() {
            return None;
        }
        // A predicate the tokenizer cannot read is not a reason to abandon the
        // query: the filter index is an optimisation, so an unreadable clause
        // just means no candidate set and a full scan, which the evaluator will
        // then reject with a proper message.
        let tokens = crate::core::tokenizer::query_tokenizer::try_tokenize(where_clause, &self.schema.fields).ok()?;
        // Each `NUMERIC(p, s)` column's scale, so a literal is bounded where
        // the column stores it — see `sarg::extract_with_scales`.
        let scales = self.decimal_scales();
        let program = sarg::extract_with_scales(&tokens, filters, &scales)?;
        let n_slots = self.io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64;
        if n_slots == 0 {
            return None;
        }
        let bitmap = filters.evaluate(&program, n_slots)?;
        Some((bitmap, n_slots))
    }


    /// Build a candidate mask for a WHERE clause, or `None` to scan normally.
    ///
    /// `pub(crate)` only so `write_path_tests` can assert on what it declines.
    /// The decisions here — the size gate and the ratio test below — are
    /// invisible in query results by design, since both fail open to a full
    /// scan, so nothing outside this method can tell whether they ran.
    pub(crate) async fn build_candidates(
        &self,
        where_clause: &str,
        limit: Option<u64>,
    ) -> Option<Arc<CandidateBitmap>> {
        // A clause no row can satisfy — `1 = 0`, `notnull IS NULL`, `x IN ()`,
        // `id != id` — needs no index and no scan. Decided before the index is
        // consulted, so it holds on a table with no filters too.
        if crate::core::filter::sarg::clause_is_constant_false(where_clause, &self.schema.fields) {
            return Some(Arc::new(CandidateBitmap::nothing()));
        }
        // A secondary index names the candidate slots outright, so it is
        // asked first. When none can answer the clause, or the answer is too
        // broad to be worth a slot list, the filter's mask is the next best.
        //
        // A short answer is taken as it is. A long one is narrowed by the
        // filter as well: an index over `(cat, price)` asked `cat = 5 AND
        // price > 990` without a range on `price` names every `cat = 5` row,
        // while the filter's mask already excludes most of them on `price`.
        // Both are supersets of the matches, so their intersection is too.
        let from_index = self.index_candidates(where_clause).await;
        if let Some(found) = &from_index {
            if found.count() <= INDEX_ANSWER_TAKEN_AS_IS || self.filters.is_none() {
                return from_index;
            }
            let Some((mask, _)) = self.filter_mask(where_clause).await else {
                return from_index;
            };
            let mut narrowed = (**found).clone();
            narrowed.intersect(&mask);
            info!("Filter narrowed {} index candidates to {}", found.count(), narrowed.count());
            return Some(Arc::new(narrowed));
        }
        let filters = self.filters.as_ref()?;
        let (bitmap, n_slots) = self.filter_mask(where_clause).await?;

        // A table small enough to scan outright gains nothing from a mask: the
        // bitmap test per record costs more than the fetches it saves, and
        // building the mask is itself work. Scanning is the cheaper answer, so
        // give the caller no candidates and let it read everything.
        //
        // This is what `min_table_rows` is for. Until it was read here the knob
        // was settable from the CLI and `SET CONFIG` and reported by `SHOW
        // CONFIG` while gating nothing — it accepted a value and had no effect.
        // Slots rather than live rows, because slots are what the scan walks.
        if n_slots < filters.config().min_table_rows {
            return None;
        }

        // A mask that barely prunes costs more (a bitmap test per record) than
        // the fetches it avoids — *when the scan reads everything*. That is the
        // wrong question to ask when the excluded slots sit at the front.
        //
        // `WHERE id >= 99995 LIMIT 5` over a million rows matches ~90% of the
        // table, so the ratio test threw the mask away; the scan then started
        // at slot 0 and fetched row bytes for ~100,000 non-matching rows before
        // its first hit. The mask would have skipped every one of them. What
        // decides the cost of a query like that is not what fraction of the
        // table survives, it is **how far the scan walks before its first
        // match** — and a leading run of excluded slots is exactly that
        // distance, whatever the global ratio says.
        //
        // So a mask is kept when it prunes broadly *or* when it lets the scan
        // start late. The per-record test the comment above worries about is
        // orders of magnitude cheaper than the row fetch it replaces, so a
        // leading run worth a whole batch of fetches pays for the test many
        // times over.
        let ratio = bitmap.count() as f64 / n_slots as f64;
        let max_ratio = crate::core::filter::settings::settings().min_pruning_ratio();
        let skipped = leading_excluded(bitmap.words()).min(n_slots);
        // One batch of avoided fetches is the smallest saving clearly worth
        // paying the per-record test for.
        let worth_it = crate::batch_size() as u64;
        if !mask_is_worth_using(ratio, max_ratio, skipped, worth_it, limit.is_some()) {
            return None;
        }
        if ratio > max_ratio {
            info!(
                "Filter kept despite {:.4} of slots surviving: it skips the leading {} of {} slots",
                ratio, skipped, n_slots
            );
        }

        info!(
            "Filter pruned {} of {} slots ({:.4} remain)",
            n_slots - bitmap.count(),
            n_slots,
            ratio
        );
        Some(Arc::new(bitmap))
    }

    /// Inserts outside any transaction, visible immediately.
    pub async fn insert_row(&self, row_write: RowWrite) -> Result<()> {
        self.insert_row_in_txn(row_write, FROZEN_TX).await
    }

    /// Inserts a row belonging to `written_by`, visible to nobody else until it
    /// commits.
    pub async fn insert_row_in_txn(&self, row_write: RowWrite, written_by: TxId) -> Result<()> {
        // Compaction relies on the live set not changing under it, so an insert
        // holds the gate too. It is shared, so concurrent inserts still run.
        let _gate = self.compaction_gate.read().await;

        self.note_write(written_by);

        // A uniqueness check that is not atomic with the write it guards is not
        // a check: two concurrent inserts would each look, each find nothing,
        // and each write. Tables without a constraint never take this.
        //
        // Not `write_lock`: see `unique_lock` for why an insert must not queue
        // behind an update's scan. Only this row's keys' stripes: inserts of
        // other keys cannot conflict with it.
        let _write = if self.has_unique {
            Some(self.unique_lock.lock(&self.unique_stripes(&row_write)).await)
        } else {
            None
        };
        self.check_unique(
            &row_write,
            None,
            &self.visibility_for(written_by),
            written_by,
            None,
        )
        .await?;

        // The id this row is about to take must already be covered by the
        // reservation on disk, or a crash could hand it out again.
        let _id = self.reserve_insert_id().await;

        info!(
            "Inserting row with {} columns",
            row_write.columns_writing_data.len()
        );

        let result = RowPointer::write_row_with_pointer_record_pos(
            self.io_pointers.clone(),
            self.io_rows.clone(),
            &self.last_row_id,
            &self.len,
            // Clustering is not implemented; every row goes to cluster 0.
            #[cfg(feature = "enable_long_row")]
            0u32,
            &row_write,
            None,
            Some(&self.free_space),
            None,
            written_by,
        )
        .await;

        // A failed write used to be reported as a successful insert: the result
        // was consulted only to decide whether to index the row, and `Ok(())`
        // was returned either way. A client would be told its row had landed
        // when nothing had been written.
        let (record_pos, _) = result.map_err(|e| {
            error!("Failed to insert row: {}", e);
            std::io::Error::from(e)
        })?;

        // Index the new slot. The pointer record position IS the slot address
        // (dense fixed-stride file), so no mapping is needed.
        if let Some(filters) = self.filters.as_ref() {
            filters.on_insert(record_pos / ROW_POINTER_RECORD_LEN as u64, &row_write);
        }
        // Before this returns, and so before the inserting transaction can
        // commit: no snapshot can see the row without its index entry.
        if let Some(indexes) = self.indexes.as_ref() {
            indexes.on_insert(record_pos / ROW_POINTER_RECORD_LEN as u64, &row_write);
        }

        self.invalidate_query_caches();

        Ok(())
    }

    /// Queries outside any transaction. On a versioned table this still takes a
    /// snapshot — other transactions may be in flight, and their uncommitted
    /// rows must stay hidden.
    pub async fn query_row(
        &self,
        query: &str,
        schema_fields: &Vec<SchemaField>,
        query_row_fetch: RowFetch,
        requested_row_fetch: RowFetch,
        limit: Option<u64>,
        order_by: Option<String>,
    ) -> Result<Vec<Row>> {
        self.query_row_in_txn(
            query,
            schema_fields,
            query_row_fetch,
            requested_row_fetch,
            limit,
            order_by,
            FROZEN_TX,
        )
        .await
    }

    /// Queries through `reader`'s snapshot, so it sees its own uncommitted
    /// writes and nobody else's.
    /// How many rows match, for a plan that wants nothing but the count.
    ///
    /// Additive alongside [`Self::query_row_in_txn`] rather than a mode on it:
    /// the two return different things and share only their preamble. See
    /// [`crate::core::rql::aggregate::is_row_count_only`] for which plans may
    /// use it.
    ///
    /// No `limit` and no `order_by` are taken. Both belong to the aggregated
    /// output rather than to its input, so there is nothing here for them to
    /// mean — and the scan-ceiling machinery `query_row_with_visibility` runs
    /// exists to bound an ordered scan, which this never is.
    pub async fn count_rows_in_txn(
        &self,
        query: &str,
        schema_fields: &Vec<SchemaField>,
        query_row_fetch: RowFetch,
        reader: TxId,
    ) -> Result<u64> {
        let visibility = self.visibility_for(reader);

        // The same two guards the row path takes, for the same reason:
        // reclamation may not hand out an extent while a pointer into it is
        // still in play, even though this path never dereferences one.
        let _gate = self.compaction_gate.read().await;
        let _read = self.read_guard();

        let generation = self.cache_generation();
        let base_candidates = self.build_candidates(query, None).await;

        let mut iterator = RowPointerIterator::new_deferred(self.io_pointers.clone())
            .await
            .unwrap();
        iterator.set_candidates(base_candidates);

        self.concurrent_processor
            .process_count(
                &self.cache_scope,
                query,
                query_row_fetch,
                schema_fields,
                self.io_rows.clone(),
                &mut iterator,
                cache_generation_for(&visibility, generation),
                visibility,
            )
            .await
    }

    pub async fn query_row_in_txn(
        &self,
        query: &str,
        schema_fields: &Vec<SchemaField>,
        query_row_fetch: RowFetch,
        requested_row_fetch: RowFetch,
        limit: Option<u64>,
        order_by: Option<String>,
        reader: TxId,
    ) -> Result<Vec<Row>> {
        let visibility = self.visibility_for(reader);
        self.query_row_with_visibility(
            query,
            schema_fields,
            query_row_fetch,
            requested_row_fetch,
            limit,
            order_by,
            visibility,
        )
        .await
    }

    /// Queries under an explicit rule for which row versions count.
    ///
    /// The entry point the two transaction-scoped readers share. It exists so
    /// a caller can read as of *now* on behalf of a transaction that has
    /// pinned an older snapshot — `visibility_now_for` — which a table rewrite
    /// needs and an ordinary query must never do.
    pub async fn query_row_with_visibility(
        &self,
        query: &str,
        schema_fields: &Vec<SchemaField>,
        query_row_fetch: RowFetch,
        requested_row_fetch: RowFetch,
        limit: Option<u64>,
        order_by: Option<String>,
        visibility: Visibility,
    ) -> Result<Vec<Row>> {
        // Implementation for querying a row from the table

        info!("Querying row");

        // Held for the whole query, worker fetches included: reclamation may
        // not hand out an extent while a pointer into it is still in play.
        let _gate = self.compaction_gate.read().await;
        let _read = self.read_guard();

        // Read before the scan, so a mutation that commits while it runs leaves
        // this query's cache entry stamped with a generation nobody will match.
        let generation = self.cache_generation();

        let base_candidates = self.build_candidates(query, limit).await;
        let n_slots = self.io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64;

        // `ORDER BY` on a column that already ascends with slot position does
        // not need the table ordered: the first `k` rows by slot *are* the
        // first `k` by value. So the scan is bounded to a leading run of slots
        // and only that much is sorted.
        //
        // Bounded by *position*, never by a count. The workers finish out of
        // order and `drain_rows` appends chunks as they arrive, so stopping
        // after `k` rows have been collected could return slots 0–50 and
        // 1500–1650 rather than the first 200 — the right number of rows and
        // the wrong ones.
        let mut ceiling = self.monotonic_scan_ceiling(&order_by, limit, n_slots).await;

        let rows = loop {
            let mut iterator = RowPointerIterator::new_deferred(self.io_pointers.clone())
                .await
                .unwrap();
            iterator.set_candidates(match ceiling {
                Some(upto) => {
                    let mut mask = CandidateBitmap::leading(n_slots, upto);
                    if let Some(base) = base_candidates.as_ref() {
                        mask.intersect(base);
                    }
                    Some(Arc::new(mask))
                }
                None => base_candidates.clone(),
            });

            let rows = self
                .concurrent_processor
                .process(
                    &self.cache_scope,
                    query,
                    query_row_fetch.clone(),
                    requested_row_fetch.clone(),
                    schema_fields,
                    self.io_rows.clone(),
                    &mut iterator,
                    limit,
                    order_by.clone(),
                    cache_generation_for(&visibility, generation),
                    visibility.clone(),
                )
                .await?;

            // Short of the limit with slots still unexamined means the ceiling
            // cut the answer, not the data: deleted or invisible rows in the
            // leading run leave fewer live ones than it holds slots. Widen and
            // retry rather than return a short page, which would look exactly
            // like the end of the table.
            match ceiling {
                Some(upto) if upto < n_slots && limit.is_some_and(|k| (rows.len() as u64) < k) => {
                    ceiling = Some(upto.saturating_mul(2).min(n_slots));
                    continue;
                }
                _ => break rows,
            }
        };

        // The limit is logged beside the count because the two together are the
        // whole diagnosis. A count alone cannot distinguish "no limit was
        // parsed" from "the limit was applied and the table really is that
        // small" from "this line belongs to a different statement" — a
        // distinction that cost a round trip to a production log to guess at.
        info!(
            "Query executed, returning {} rows (limit={:?})",
            rows.len(),
            limit
        );

        return Ok(rows);
    }

    /// Bytes-mode counterpart of `query_row`. Workers serialize matched rows
    /// to wire bytes inline; the result is ready for the response with no
    /// extra `rows_into_vec` pass. Used by the SELECT executor hot path.
    pub async fn query_row_bytes(
        &self,
        query: &str,
        schema_fields: &Vec<SchemaField>,
        query_row_fetch: RowFetch,
        requested_row_fetch: RowFetch,
        limit: Option<u64>,
        order_by: Option<String>,
    ) -> Result<Vec<u8>> {
        self.query_row_bytes_in_txn(
            query,
            schema_fields,
            query_row_fetch,
            requested_row_fetch,
            limit,
            order_by,
            FROZEN_TX,
        )
        .await
    }

    /// Bytes-mode counterpart of [`Table::query_row_in_txn`].
    pub async fn query_row_bytes_in_txn(
        &self,
        query: &str,
        schema_fields: &[SchemaField],
        query_row_fetch: RowFetch,
        requested_row_fetch: RowFetch,
        limit: Option<u64>,
        order_by: Option<String>,
        reader: TxId,
    ) -> Result<Vec<u8>> {
        self.query_row_bytes_reporting_in_txn(
            query,
            schema_fields,
            query_row_fetch,
            requested_row_fetch,
            limit,
            order_by,
            reader,
        )
        .await
        .map(|(bytes, _)| bytes)
    }

    /// [`Table::query_row_bytes_in_txn`], also saying whether any row may hold
    /// columns beyond `requested_row_fetch` (see
    /// `ConcurrentProcessor::process_into_bytes`). A caller that skips trimming
    /// on `false` must take the flag from here, never work it out from the
    /// parse: only the scan knows which fetch it emitted.
    pub async fn query_row_bytes_reporting_in_txn(
        &self,
        query: &str,
        schema_fields: &[SchemaField],
        query_row_fetch: RowFetch,
        requested_row_fetch: RowFetch,
        limit: Option<u64>,
        order_by: Option<String>,
        reader: TxId,
    ) -> Result<(Vec<u8>, bool)> {
        info!("Querying row (bytes mode)");

        let _gate = self.compaction_gate.read().await;
        let _read = self.read_guard();

        // The SNAPSHOT is taken first, then the generation — that order is
        // load-bearing, and the row-mode path already had it by construction.
        //
        // A cached entry holds the pointers that matched the *predicate* at
        // the generation it was stamped with, and `query_cache::lookup` serves
        // it to any reader presenting that same generation. Reading the
        // generation first left a window: a write committing between the two
        // lines is visible to this snapshot but its new row version was never
        // in the cached set, while every older version the set does hold has
        // just been retired by that same committed write — so all of them are
        // invisible and the query returns NOTHING for a row that plainly
        // exists. Taking the snapshot first closes it: anything the snapshot
        // can see committed before the generation was read, so its pointer is
        // in the set, or the entry has been retired and the scan runs for
        // real. Still read before the scan, so an entry this query stamps is
        // retired by a mutation that lands while it runs.
        let visibility = self.visibility_for(reader);
        let generation = self.cache_generation();

        let mut iterator = RowPointerIterator::new_deferred(self.io_pointers.clone())
            .await
            .unwrap();
        iterator.set_candidates(self.build_candidates(query, limit).await);

        let (bytes, overfetched) = self
            .concurrent_processor
            .process_into_bytes(
                &self.cache_scope,
                query,
                query_row_fetch,
                requested_row_fetch,
                schema_fields,
                self.io_rows.clone(),
                &mut iterator,
                limit,
                order_by,
                cache_generation_for(&visibility, generation),
                visibility,
            )
            .await?;

        info!(
            "Query executed, returning {} bytes (limit={:?})",
            bytes.len(),
            limit
        );
        Ok((bytes, overfetched))
    }

    /// A fetch of just the columns named by write order, for the index build
    /// and for vacuum reading a reclaimed version's keys.
    pub(crate) fn build_fetch_for(schema_fields: &SmallVec<[SchemaField; 20]>, columns: &[u32]) -> RowFetch {
        let schema_calc = SchemaCalculator::default();
        let mut columns_fetching_data = smallvec::SmallVec::new();
        for field in schema_fields
            .iter()
            .filter(|f| !f.is_deleted && columns.contains(&(f.write_order as u32)))
            .sorted_by(|a, b| a.write_order.cmp(&b.write_order))
        {
            let offset = schema_calc.calculate_schema_offset(&field.name, schema_fields);
            columns_fetching_data.push(ColumnFetchingData {
                column_offset: offset.0,
                column_type: field.db_type.physical(),
                size: field.size,
                schema_id: offset.1,
            });
        }
        RowFetch {
            columns_fetching_data,
        }
    }

    pub(crate) fn build_full_row_fetch(schema_fields: &SmallVec<[SchemaField; 20]>) -> RowFetch {
        let schema_calc = SchemaCalculator::default();
        let mut columns_fetching_data = smallvec::SmallVec::new();
        for field in schema_fields
            .iter()
            .sorted_by(|a, b| a.write_order.cmp(&b.write_order))
        {
            let offset = schema_calc.calculate_schema_offset(&field.name, schema_fields);
            columns_fetching_data.push(ColumnFetchingData {
                column_offset: offset.0,
                column_type: field.db_type.physical(),
                size: field.size,
                schema_id: offset.1,
            });
        }
        RowFetch {
            columns_fetching_data,
        }
    }

    /// Deletes outside any transaction, tombstoning immediately.
    ///
    /// Correct only for an unversioned table. Once [`Table::enable_versioning`]
    /// has been called, use [`Table::delete_rows_in_txn`]: a tombstone tells
    /// reclamation the row predates versioning and may be reused, which is not
    /// true of a row an open snapshot can still see.
    pub async fn delete_rows(
        &self,
        where_clause: &str,
        query_row_fetch: RowFetch,
        limit: Option<u64>,
    ) -> Result<u64> {
        self.delete_rows_in_txn(where_clause, query_row_fetch, limit, FROZEN_TX)
            .await
    }

    /// Deletes the rows `deleted_by` can see, stamping rather than tombstoning.
    pub async fn delete_rows_in_txn(
        &self,
        where_clause: &str,
        query_row_fetch: RowFetch,
        limit: Option<u64>,
        deleted_by: TxId,
    ) -> Result<u64> {
        self.delete_rows_as_of(where_clause, query_row_fetch, limit, deleted_by, None)
            .await
    }

    /// [`Self::delete_rows_in_txn`], matching rows through `visibility` rather
    /// than a snapshot taken here.
    ///
    /// For a statement that has to know, before it deletes, exactly which rows
    /// it will delete: a `DELETE` of a parent table collects the keys it is
    /// removing to apply its foreign keys' actions, and that collection and
    /// this scan must see the same rows. Taken separately, a parent row
    /// committed between the two was deleted without its children ever being
    /// considered. One snapshot for the statement is also what PostgreSQL does.
    pub async fn delete_rows_as_of(
        &self,
        where_clause: &str,
        query_row_fetch: RowFetch,
        limit: Option<u64>,
        deleted_by: TxId,
        visibility: Option<Visibility>,
    ) -> Result<u64> {
        info!("Deleting rows");

        self.note_write(deleted_by);

        let _gate = self.compaction_gate.read().await;
        let _read = self.read_guard();

        // Taken *before* the scan, not between the scan and the rewrite.
        //
        // Scanning outside the lock and re-reading each record under it is not
        // enough. An `UPDATE` that relocates a row tombstones its record and
        // appends the replacement past the end this scan snapshotted, so the
        // scan sees only a tombstone and matches nothing at all — there is no
        // record left for a re-read to correct. The row is then neither deleted
        // nor reported, which is the one outcome a statement must never have.
        //
        // So `DELETE` and `UPDATE` serialize against each other outright. They
        // already serialized for their rewrites; this extends it over the scan,
        // which is the expensive half. `SELECT` and `INSERT` are unaffected —
        // neither rewrites an existing record, and neither takes this lock.
        let _write = self.write_lock.write().await;

        let mut iterator = RowPointerIterator::new_deferred(self.io_pointers.clone())
            .await
            .unwrap();
        iterator.set_candidates(self.build_candidates(where_clause, limit).await);

        // The checkpoint on disk is about to describe rows that no longer exist.
        // One byte, once per statement, and never on the insert path.
        if let Some(filters) = self.filters.as_ref() {
            filters.mark_disk_dirty(&*self.io_filters).await;
        }

        let matches = self
            .concurrent_processor
            .process_row_pointers(
                &self.cache_scope,
                where_clause,
                query_row_fetch,
                &self.schema.fields,
                self.io_rows.clone(),
                &mut iterator,
                limit,
                // A statement may only remove rows it can see. Without this a
                // DELETE would match another transaction's uncommitted rows.
                visibility.unwrap_or_else(|| self.visibility_for(deleted_by)),
            )
            .await?;

        // One transaction for the whole statement: every tombstone lands or
        // none does, and the flush is paid once rather than once per row.
        let mut txn = self.io_pointers.begin_txn();
        let mut deleted_count: u64 = 0;
        // Applied only after each batch's commit — see `PendingFilterOp`.
        let mut pending_filters: Vec<PendingFilterOp> = Vec::new();

        for pwo in matches.into_iter() {
            if pwo.pointer.deleted {
                continue;
            }

            // No re-read: the scan ran under this same lock, so no other
            // statement can have touched this record since.
            let record_pos = pwo.pointer_record_pos;
            let mut pointer = pwo.pointer;

            // Fails the statement rather than skipping the row: a DELETE that
            // silently left a row behind would report a count the caller cannot
            // act on.
            if let Err(e) = self.check_write_conflict(&pointer, deleted_by) {
                // The storage transaction is given up, not left open: one that
                // logged an in-place stamp and never commits holds the log's
                // pending count up, and the log cannot be checkpointed again
                // for the life of the process.
                self.io_pointers.abort_txn(txn).await;
                return Err(if deleted_count == 0 {
                    std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        format!("{}{}", e, Self::CONFLICT_RETRYABLE),
                    )
                    .into()
                } else {
                    e
                });
            }

            if let Err(e) = pointer
                .delete(self.io_pointers.clone(), record_pos, Some(txn), deleted_by)
                .await
            {
                error!("Failed to delete row pointer {:?}: {}", pointer, e);
                continue;
            }

            // Exact removal: clearing the slot's fingerprint bits *improves*
            // the false-positive rate, and lets later scans prune the tombstone
            // before its pointer record is decoded.
            //
            // Only once the row is gone for everyone, though. A transactional
            // DELETE stamps `xmax` and leaves the version readable by older
            // snapshots; pruning its slot would hide it from them. Staged until
            // the tombstone is durable — see `PendingFilterOp`.
            if deleted_by == FROZEN_TX && self.filters.is_some() {
                pending_filters.push(PendingFilterOp::Delete(
                    record_pos / ROW_POINTER_RECORD_LEN as u64,
                ));
            }

            deleted_count = deleted_count.saturating_add(1);

            // An open transaction blocks checkpointing, so a DELETE running over
            // millions of rows would grow the log without bound — the same
            // reason UPDATE breaks its own. Batching costs the statement its
            // all-or-nothing property, which is the price of a reclaimable log.
            if crate::core::wal::should_break_transaction(&*self.io_pointers) {
                self.io_pointers.commit_durable(txn).await?;
                self.apply_pending_filters(&mut pending_filters);
                txn = self.io_pointers.begin_txn();
            }
        }

        // End the statement transaction: this is the single flush the whole
        // DELETE pays, and the point at which its tombstones become durable.
        //
        // Failing the statement rather than reporting rows affected: a tombstone
        // that is not durable is a row that comes back, and telling the caller
        // otherwise is the difference between a failed delete and a silent one.
        self.io_pointers.commit_durable(txn).await?;
        self.apply_pending_filters(&mut pending_filters);

        // Published before the lock is released, so anyone who takes it next
        // sees a generation that already accounts for everything above.
        self.dead_stamps
            .fetch_add(deleted_count, AtomicOrdering::Relaxed);
        self.write_generation.fetch_add(1, AtomicOrdering::AcqRel);

        if deleted_count > 0 {
            self.invalidate_query_caches();
            // Ask the maintenance task to clear the dirty flag. Rewriting the
            // blob here would make a one-row DELETE pay for the whole index.
            if let Some(filters) = self.filters.as_ref() {
                filters.request_checkpoint();
            }
        }

        Ok(deleted_count)
    }

    /// Updates outside any transaction. See [`Table::delete_rows`] for when
    /// that is correct.
    pub async fn update_rows(
        &self,
        where_clause: &str,
        query_row_fetch: RowFetch,
        updates: smallvec::SmallVec<[ColumnWritePayload; 32]>,
        limit: Option<u64>,
    ) -> Result<u64> {
        self.update_rows_in_txn(where_clause, query_row_fetch, updates, limit, FROZEN_TX)
            .await
    }

    /// Updates the rows `written_by` can see, writing a new version of each.
    pub async fn update_rows_in_txn(
        &self,
        where_clause: &str,
        query_row_fetch: RowFetch,
        updates: smallvec::SmallVec<[ColumnWritePayload; 32]>,
        limit: Option<u64>,
        written_by: TxId,
    ) -> Result<u64> {
        self.update_rows_with_expressions(
            where_clause,
            query_row_fetch,
            updates,
            &[],
            limit,
            written_by,
        )
        .await
    }

    /// As `update_rows_in_txn`, but some assignments are expressions evaluated
    /// against each row rather than one value shared by all of them.
    ///
    /// `COALESCE(c, x)` keeps `c` where it has a value and substitutes `x`
    /// where it does not, so the answer differs per row. The full row is
    /// already loaded here to merge the update into, which is what makes this
    /// the right place to evaluate rather than a second read.
    pub async fn update_rows_with_expressions(
        &self,
        where_clause: &str,
        query_row_fetch: RowFetch,
        updates: smallvec::SmallVec<[ColumnWritePayload; 32]>,
        expressions: &[(u32, crate::core::rql::expr::Expr)],
        limit: Option<u64>,
        written_by: TxId,
    ) -> Result<u64> {
        self.update_rows_as_of(
            where_clause,
            query_row_fetch,
            updates,
            expressions,
            limit,
            written_by,
            None,
        )
        .await
    }

    /// [`Self::update_rows_with_expressions`], matching rows through
    /// `visibility` rather than a snapshot taken here — see
    /// [`Self::delete_rows_as_of`] for why a statement needs that.
    pub async fn update_rows_as_of(
        &self,
        where_clause: &str,
        query_row_fetch: RowFetch,
        updates: smallvec::SmallVec<[ColumnWritePayload; 32]>,
        expressions: &[(u32, crate::core::rql::expr::Expr)],
        limit: Option<u64>,
        written_by: TxId,
        visibility: Option<Visibility>,
    ) -> Result<u64> {
        info!("Updating rows");

        // A statement naming its row by a unique key tries the point path
        // first, which lets updates of different keys run side by side. It
        // falls back here, having written nothing, only when another key of
        // the row it found is on a stripe it could not take in order.
        if let Some(stripe) = self.point_update_stripe(where_clause, &updates, expressions) {
            if let Some(n) = self
                .update_rows_locked(
                    where_clause,
                    query_row_fetch.clone(),
                    &updates,
                    expressions,
                    limit,
                    written_by,
                    visibility.clone(),
                    Some(stripe),
                )
                .await?
            {
                return Ok(n);
            }
        }
        self.update_rows_locked(
            where_clause,
            query_row_fetch,
            &updates,
            expressions,
            limit,
            written_by,
            visibility,
            None,
        )
        .await
        .map(|n| n.unwrap_or_default())
    }

    /// A row's columns as a write, every column keyed by `write_order`.
    fn row_write_of(&self, row: &Row) -> RowWrite {
        let mut row_write = RowWrite {
            columns_writing_data: smallvec::SmallVec::new(),
        };
        for field in self
            .schema
            .fields
            .iter()
            .sorted_by(|a, b| a.write_order.cmp(&b.write_order))
        {
            let write_order = field.write_order as u32;
            let col = row
                .columns
                .iter()
                .find(|c| c.schema_id as u32 == write_order);

            row_write.columns_writing_data.push(ColumnWritePayload {
                // Missing columns shouldn't normally happen for a full fetch;
                // treat as empty.
                data: col.map(|c| c.data.clone()).unwrap_or_default(),
                write_order,
                column_type: field.db_type.physical(),
                size: field.size,
            });
        }
        row_write
    }

    /// The body of [`Self::update_rows_as_of`], under the locks `point`
    /// chooses: with `Some(stripe)`, the write lock shared and that key's
    /// stripe; with `None`, the write lock exclusively and, for the rewrite,
    /// every stripe. `Ok(None)` when a point update gave up before writing.
    #[allow(clippy::too_many_arguments)]
    async fn update_rows_locked(
        &self,
        where_clause: &str,
        query_row_fetch: RowFetch,
        updates: &smallvec::SmallVec<[ColumnWritePayload; 32]>,
        expressions: &[(u32, crate::core::rql::expr::Expr)],
        limit: Option<u64>,
        written_by: TxId,
        visibility: Option<Visibility>,
        point: Option<usize>,
    ) -> Result<Option<u64>> {
        // A caller that brings its own snapshot is a statement with work left
        // to do after this write — foreign-key actions — and that work can
        // fail it. An in-place overwrite keeps no previous version, so a
        // failure there would leave the new bytes standing in a statement
        // reported as failed. Such a statement always writes a new version.
        let may_fail_after_write = visibility.is_some();

        self.note_write(written_by);

        let _gate = self.compaction_gate.read().await;
        let _read = self.read_guard();

        // Before the scan. See the note in `delete_rows`: a relocation by a
        // concurrent statement leaves nothing at the old record for a later
        // re-read to find, so a snapshot taken outside this lock can silently
        // match nothing. A point update takes its key's stripe instead, which
        // every writer of a row it could match takes too (see `write_lock`).
        let (_write_exclusive, _write_shared) = match point {
            Some(_) => (None, Some(self.write_lock.read().await)),
            None => (Some(self.write_lock.write().await), None),
        };
        let point_keys = match point {
            Some(stripe) => Some(self.unique_lock.lock(&[stripe]).await),
            None => None,
        };

        let mut iterator = RowPointerIterator::new_deferred(self.io_pointers.clone())
            .await
            .unwrap();
        iterator.set_candidates(self.build_candidates(where_clause, limit).await);

        if let Some(filters) = self.filters.as_ref() {
            filters.mark_disk_dirty(&*self.io_filters).await;
        }

        let matches = self
            .concurrent_processor
            .process_row_pointers(
                &self.cache_scope,
                where_clause,
                query_row_fetch,
                &self.schema.fields,
                self.io_rows.clone(),
                &mut iterator,
                limit,
                // As in `delete_rows_in_txn`: a statement may only change rows
                // it can see.
                visibility.unwrap_or_else(|| self.visibility_for(written_by)),
            )
            .await?;

        let full_fetch = Self::build_full_row_fetch(&self.schema.fields);

        // The rewrite runs under the lock inserts check uniqueness under; the
        // scan above does not. See `unique_lock` for both halves of that.
        //
        // A point update already holds its key's stripe. It needs the
        // stripes of the row's *other* keys too, for two reasons:
        // - a point update through another key of the same row (`WHERE id`
        //   here, `WHERE email` there) holds only that key's stripe. Without
        //   this both scan the same version and both relocate it, and the row
        //   forks into two live versions;
        // - a relocated row's other keys reach the index only after the
        //   rewrite, so an insert checking one of them in that window would
        //   find neither version.
        // The row cannot change those keys under the stripe held, since
        // giving a row a key takes every stripe. Mutation-tested: without
        // this, `point_update_concurrency.rs` forks rows until it stalls.
        let _unique = match point_keys {
            None if self.has_unique => Some(self.unique_lock.lock_all().await),
            None => None,
            Some(mut keys) => {
                let constraints = self.schema.fields.iter().filter(|f| f.is_unique && !f.is_deleted).count()
                    + self.schema.unique_keys.len();
                if constraints > 1 {
                    let mut more: SmallVec<[usize; 8]> = SmallVec::new();
                    let mut row = Row::default();
                    for pwo in matches.iter().filter(|m| !m.pointer.deleted) {
                        if pwo
                            .pointer
                            .fetch_row_reuse_async(&*self.io_rows, &full_fetch, &mut row)
                            .await
                            .is_ok()
                        {
                            more.extend(self.unique_stripes(&self.row_write_of(&row)));
                        }
                    }
                    if !self.unique_lock.lock_more(&mut keys, &more).await {
                        return Ok(None);
                    }
                }
                Some(keys)
            }
        };

        // A relocation allocates from the id counter even though it hands the
        // row back its own id, so the reservation on disk has to cover it.
        self.ensure_ids_reserved().await;

        // One transaction for the statement, subject to the memory bound below.
        let mut txn = self.io_pointers.begin_txn();

        let mut updated_count: u64 = 0;
        let mut row = Row::default();
        // Applied only after each batch's commit — see `PendingFilterOp`.
        let mut pending_filters: Vec<PendingFilterOp> = Vec::new();

        for pwo in matches.into_iter() {
            if pwo.pointer.deleted {
                continue;
            }

            // No re-read: the scan ran under this same lock.
            let record_pos = pwo.pointer_record_pos;
            let mut pointer = pwo.pointer;
            let row_id = pointer.row_id();

            if let Err(e) = self.check_write_conflict(&pointer, written_by) {
                // As in the DELETE: an abandoned storage transaction would hold
                // the log's checkpoint off for good.
                self.io_pointers.abort_txn(txn).await;
                return Err(if updated_count == 0 {
                    std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        format!("{}{}", e, Self::CONFLICT_RETRYABLE),
                    )
                    .into()
                } else {
                    e
                });
            }

            // Load full row so we can merge updates.
            if pointer
                .fetch_row_reuse_async(&*self.io_rows, &full_fetch, &mut row)
                .await
                .is_err()
            {
                continue;
            }

            // Build RowWrite from the existing row (all columns), keyed by write_order.
            let mut row_write = self.row_write_of(&row);

            // The version's index keys as it stands, before the assignments:
            // an in-place overwrite has to take the slot out from under them.
            let old_keys = self.indexes.as_ref().map(|ix| ix.keys_of(&row_write));

            // Apply updates in-place in the RowWrite payloads.
            for upd in updates.iter() {
                if let Some(existing) = row_write
                    .columns_writing_data
                    .iter_mut()
                    .find(|c| c.write_order == upd.write_order)
                {
                    existing.data = upd.data.clone();
                    existing.column_type = upd.column_type.clone();
                    existing.size = upd.size;
                }
            }

            // Expressions are evaluated against the row as it was *before* this
            // statement touched it, which is what `COALESCE(c, x)` means, and
            // then overwrite the placeholder their assignment left behind.
            for (write_order, expression) in expressions.iter() {
                // Expressions compute at the engine scale; a `NUMERIC(p, s)`
                // target stores at its own, so the result is put back there.
                let value =
                    match crate::core::rql::expr::eval(expression, &row, self.schema.fields.as_slice())
                        .and_then(|value| {
                            match self.schema.fields.iter().find(|f| f.write_order as u32 == *write_order as u32) {
                                Some(target) => crate::core::rql::expr::from_engine_scale(value, target),
                                None => Ok(value),
                            }
                        }) {
                        Ok(value) => value,
                        Err(e) => {
                            self.io_pointers.abort_txn(txn).await;
                            return Err(
                                std::io::Error::new(std::io::ErrorKind::InvalidInput, e).into()
                            );
                        }
                    };

                let Some(slot_index) = row_write
                    .columns_writing_data
                    .iter()
                    .position(|c| c.write_order == *write_order)
                else {
                    continue;
                };

                let is_null = matches!(value, crate::core::rql::expr::Value::Null);
                let bytes = match value {
                    crate::core::rql::expr::Value::Null => Vec::new(),
                    crate::core::rql::expr::Value::Data { bytes, .. } => bytes,
                };

                // The companion moves with the value, and per row: an
                // expression may be NULL for one row and not another, which is
                // the whole point of `COALESCE`. Leaving it behind kept a row
                // reading as NULL however real the value written beside it.
                let column_name = self
                    .schema
                    .fields
                    .iter()
                    .find(|f| f.write_order as u32 == *write_order)
                    .map(|f| f.name.clone());
                if let Some(name) = column_name {
                    let flag_name = crate::core::rql::nullable::flag_column_name(&name);
                    // Tombstones excluded. A by-name lookup that does not say
                    // this resolves to a dropped column whenever one still holds
                    // the name, and here that is a *write*: the null flag would
                    // be written at a dead column's write_order, so the row's
                    // nullability silently stops being recorded. Cheap to say,
                    // and it stops depending on a dropped column's name
                    // happening to be unique to it.
                    if let Some(flag) = self
                        .schema
                        .fields
                        .iter()
                        .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&flag_name))
                    {
                        if let Some(flag_payload) = row_write
                            .columns_writing_data
                            .iter_mut()
                            .find(|c| c.write_order == flag.write_order as u32)
                        {
                            let bits = crate::core::rql::nullable::flag_bytes(is_null);
                            let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bits.len());
                            mb.into_slice_mut().copy_from_slice(&bits);
                            flag_payload.data = mb;
                        }
                    }
                }
                // A fixed-width column has to fill its slot however short the
                // value is, exactly as the constant path pads it.
                let (width, variable) = {
                    let existing = &row_write.columns_writing_data[slot_index];
                    (
                        existing.size as usize,
                        existing.column_type.is_variable_width(),
                    )
                };
                let mut mb = crate::memory_pool::MEMORY_POOL.acquire(if variable {
                    bytes.len()
                } else {
                    width
                });
                let slot = mb.into_slice_mut();
                slot.fill(0);
                let n = bytes.len().min(slot.len());
                slot[..n].copy_from_slice(&bytes[..n]);
                row_write.columns_writing_data[slot_index].data = mb;
            }

            // The row keeps the value it already holds, so it is excluded from
            // its own check.
            let assigned: std::collections::HashSet<u32> =
                updates.iter().map(|u| u.write_order).collect();
            if let Err(e) = self
                .check_unique(
                    &row_write,
                    Some(row_id),
                    &self.visibility_for(written_by),
                    written_by,
                    Some(&assigned),
                )
                .await
            {
                // Durability outranks the constraint violation as the thing to
                // report: the caller can retry a rejected UPDATE, but it cannot
                // do anything about rows it was told had landed and had not.
                self.io_pointers.commit_durable(txn).await?;
                self.apply_pending_filters(&mut pending_filters);
                self.dead_stamps
                    .fetch_add(updated_count, AtomicOrdering::Relaxed);
                self.write_generation.fetch_add(1, AtomicOrdering::AcqRel);
                if updated_count > 0 {
                    self.invalidate_query_caches();
                }
                // The marker survives only while nothing has been applied,
                // the same rule `check_write_conflict` above follows. Unlike
                // the insert check, this one is reached once per matched row,
                // with earlier rows already written — so *if* partial work
                // ever became visible to a replay, a non-idempotent assignment
                // like `n = n + 1` would count twice.
                //
                // That case could not be produced. A conflicting multi-row
                // UPDATE currently applies nothing at all — whichever matched
                // row conflicts, and whether or not the caller has its own
                // transaction — because the failed statement's writes never
                // become visible. Pinned by `a_conflicting_update_applies_
                // nothing`. So this is a guard on an invariant the function
                // already keeps, not a fix for a reachable bug; it is here so
                // that a change which starts committing partial work does not
                // silently gain a double-apply.
                //
                // A conflict that *is* replayable is not an error anyone needs
                // to see, for the same reason the executor logs one at `debug!`
                // — it is about to be waited out and retried.
                let retryable =
                    updated_count == 0 && e.to_string().ends_with(Self::CONFLICT_RETRYABLE);
                if retryable {
                    log::debug!("UPDATE replayed for row {}: {}", row_id, e);
                    return Err(e);
                }
                error!("UPDATE rejected for row {}: {}", row_id, e);
                let text = e.to_string();
                return Err(match text.strip_suffix(Self::CONFLICT_RETRYABLE) {
                    Some(trimmed) => std::io::Error::new(e.kind(), trimmed.to_string()),
                    None => e,
                });
            }

            // Fast path: when every assigned column is fixed-width, the row's
            // layout and length are unchanged, so only those columns' bytes
            // differ. The general path re-encodes and rewrites the whole row —
            // for a wide row and a one-column SET, almost all of that work
            // produces bytes identical to the ones already there.
            //
            // The filter is fed the same full `row_write` either way, so this
            // needs nothing from it that the general path does not.
            if !updates.is_empty()
                && !may_fail_after_write
                && updates.iter().all(|u| u.column_type != DbType::STRING)
                && self
                    .write_columns_in_place(&pointer, updates, txn, written_by)
                    .await
            {
                if self.maintains_indexes() {
                    pending_filters.push(PendingFilterOp::Insert(
                        record_pos / ROW_POINTER_RECORD_LEN as u64,
                        row_write,
                        old_keys,
                    ));
                }
                updated_count = updated_count.saturating_add(1);

                if crate::core::wal::should_break_transaction(&*self.io_pointers) {
                    self.io_pointers.commit_durable(txn).await?;
                    self.apply_pending_filters(&mut pending_filters);
                    txn = self.io_pointers.begin_txn();
                }
                continue;
            }

            let update_result = pointer
                .update_with_pointer_record_pos(
                    self.io_pointers.clone(),
                    self.io_rows.clone(),
                    record_pos,
                    &self.last_row_id,
                    &self.len,
                    // Clustering is not implemented; every row goes to cluster 0.
                    #[cfg(feature = "enable_long_row")]
                    0u32,
                    &row_write,
                    Some(txn),
                    Some(&self.free_space),
                    written_by,
                )
                .await;

            if let Ok((_, relocated)) = &update_result {
                if self.maintains_indexes() {
                    let old_slot = record_pos / ROW_POINTER_RECORD_LEN as u64;
                    match relocated {
                        // A fresh pointer was appended for the new version.
                        // Index it — but only clear the old slot when the
                        // version it holds is dead for everyone. Under
                        // versioning it is not: a snapshot older than this
                        // update is still entitled to read it, and clearing the
                        // fingerprint makes the scan prune the slot before it
                        // ever decodes the pointer, so the row vanishes for
                        // exactly the readers that should still see it.
                        // Reclamation clears it once no snapshot can reach it.
                        // Staged until the new version is durable — see
                        // `PendingFilterOp`.
                        Some(new_pos) => {
                            if written_by == FROZEN_TX {
                                pending_filters.push(PendingFilterOp::Delete(old_slot));
                            }
                            pending_filters.push(PendingFilterOp::Insert(
                                *new_pos / ROW_POINTER_RECORD_LEN as u64,
                                row_write,
                                None,
                            ));
                        }
                        // Overwritten in place: same slot, new fingerprints,
                        // and the old index keys go.
                        None => pending_filters.push(PendingFilterOp::Insert(old_slot, row_write, old_keys)),
                    }
                }
            }

            if update_result.is_ok() {
                updated_count = updated_count.saturating_add(1);
            }

            // An open transaction blocks checkpointing, so a statement running
            // over millions of rows would grow the log without bound. Break it
            // into batches once the log is due for a checkpoint: the statement
            // is then atomic per batch rather than as a whole, which is the
            // price of a reclaimable log.
            if crate::core::wal::should_break_transaction(&*self.io_pointers) {
                self.io_pointers.commit_durable(txn).await?;
                self.apply_pending_filters(&mut pending_filters);
                txn = self.io_pointers.begin_txn();
            }
        }

        self.io_pointers.commit_durable(txn).await?;
        self.apply_pending_filters(&mut pending_filters);
        self.dead_stamps
            .fetch_add(updated_count, AtomicOrdering::Relaxed);
        self.write_generation.fetch_add(1, AtomicOrdering::AcqRel);

        if updated_count > 0 {
            self.invalidate_query_caches();
            if let Some(filters) = self.filters.as_ref() {
                filters.request_checkpoint();
            }
        }

        Ok(Some(updated_count))
    }
}

/// One insert counted in `Table::ids_in_flight` until its id is taken.
pub(crate) struct IdsInFlight<'a>(&'a AtomicU64);

impl Drop for IdsInFlight<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, AtomicOrdering::AcqRel);
    }
}

/// An index answer at most this long is scanned as it is; a longer one is
/// intersected with the filter's mask first. Below it, evaluating the mask
/// costs more than the fetches it could save.
const INDEX_ANSWER_TAKEN_AS_IS: u64 = 256;

/// Index rows `[start_slot, ..)` from authoritative storage.
///
/// A free function rather than a `Table` method so the maintenance task can run
/// it without holding a borrow of the table.
///
/// Deleted pointers are skipped, so their slots stay all-zero — which reads as
/// "matches nothing" and lets the filter prune tombstoned rows before their
/// pointer record is even decoded.
async fn backfill_into<S: StorageIO>(
    filters: &TableFilters,
    io_pointers: &Arc<S>,
    io_rows: &Arc<S>,
    fields: &SmallVec<[SchemaField; 20]>,
    start_slot: u64,
    // Epoch registry to register against, or `None` when reclamation is off and
    // nothing can move under the scan.
    readers: Option<&StdArc<ReaderRegistry>>,
) -> Result<u64> {
    // Held for the whole sweep. This reads rows by pointer just as a query does,
    // and is the one such reader that is not a query.
    let _read = match readers {
        Some(readers) => readers.enter(),
        None => ReadTicket::inert(),
    };

    let fetch = Table::<S>::build_indexed_row_fetch(fields);
    if fetch.columns_fetching_data.is_empty() {
        if start_slot == 0 {
            filters.clear_unindexed_prefix();
        }
        return Ok(0);
    }

    let mut iterator = RowPointerIterator::new(io_pointers.clone())
        .await
        .map_err(|e| std::io::Error::other(format!("{e:?}")))?;

    if start_slot > 0 {
        iterator
            .seek_to_byte_position(start_slot * ROW_POINTER_RECORD_LEN as u64)
            .await
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
    }

    let mut row = Row::default();
    let mut indexed: u64 = 0;

    // Rows are handed to the filter in runs so each column's lock is taken once
    // per run rather than once per row. A whole-table backfill is the largest
    // batch the engine has, and per-row locking is where its cost sits — see
    // `TableFilters::on_insert_batch` for the measurement.
    const BACKFILL_BATCH: usize = 256;
    let mut pending: Vec<(u64, SmallVec<[(u32, Vec<u8>); 32]>)> =
        Vec::with_capacity(BACKFILL_BATCH);

    loop {
        let next = iterator
            .next_row_pointer_with_offset()
            .await
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
        let Some((offset, pointer)) = next else { break };

        if pointer
            .fetch_row_reuse_async(&**io_rows, &fetch, &mut row)
            .await
            .is_err()
        {
            continue;
        }

        let cols: SmallVec<[(u32, Vec<u8>); 32]> = row
            .columns
            .iter()
            .map(|c| (c.schema_id as u32, c.data.into_slice().to_vec()))
            .collect();

        pending.push((offset / ROW_POINTER_RECORD_LEN as u64, cols));
        if pending.len() == BACKFILL_BATCH {
            filters.backfill_batch(&pending);
            pending.clear();
        }
        indexed += 1;
    }
    filters.backfill_batch(&pending);

    // Only a scan that started at the beginning can vouch for the prefix.
    if start_slot == 0 {
        filters.clear_unindexed_prefix();
    }
    info!(
        "Filter backfill indexed {} rows from slot {}",
        indexed, start_slot
    );
    Ok(indexed)
}

/// Fills `indexes` from every version on disk.
///
/// Holds a reader epoch for the reason the filter backfill does: it reads row
/// bytes by pointer, and an extent reclaimed underneath it would be keyed
/// with another row's value. Deleted pointers are skipped by the iterator.
/// A version still visible to an old snapshot is indexed like any other,
/// and vacuum forgets it once nothing can reach it.
///
/// Writers maintain the indexes while this runs, so a version written after
/// the scan passed its slot is indexed by its writer. One the scan reaches
/// as well is simply added twice, which a set ignores.
async fn build_indexes<S: StorageIO>(
    indexes: &crate::core::index::TableIndexes,
    io_pointers: &Arc<S>,
    io_rows: &Arc<S>,
    fields: &SmallVec<[SchemaField; 20]>,
    readers: Option<&StdArc<ReaderRegistry>>,
) -> Result<u64> {
    let _read = match readers {
        Some(readers) => readers.enter(),
        None => ReadTicket::inert(),
    };
    let fetch = Table::<S>::build_fetch_for(fields, &indexes.columns());
    let mut iterator = RowPointerIterator::new(io_pointers.clone())
        .await
        .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
    let mut row = Row::default();
    let mut built = 0u64;
    loop {
        let next = iterator
            .next_row_pointer_with_offset()
            .await
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
        let Some((offset, pointer)) = next else { break };
        let slot = offset / ROW_POINTER_RECORD_LEN as u64;
        if pointer
            .fetch_row_reuse_async(&**io_rows, &fetch, &mut row)
            .await
            .is_err()
        {
            // Unreadable here is not unreadable to the query, which may
            // succeed later: kept as a candidate in every index.
            indexes.index_unreadable(slot);
            continue;
        }
        indexes.index_row(slot, &|wo| {
            row.columns
                .iter()
                .find(|c| c.schema_id as u32 == wo)
                .map(|c| c.data.into_slice())
        });
        built += 1;
    }
    Ok(built)
}

#[cfg(test)]
mod cache_generation_tests {
    use super::next_cache_generation;

    /// Two `Table`s over the same files must never share a cache generation.
    ///
    /// Two ways to get a second instance over the same `cache_scope`, and
    /// nothing retires the old entries in either:
    ///
    /// * `DROP TABLE t; CREATE TABLE t;` — rebuilds under an identical scope.
    /// * **`ALTER TABLE`** — every `reload_table` caller is an ALTER path, and
    ///   `ADD COLUMN` rewrites every row under a new layout first, so the
    ///   cached pointers name positions that no longer hold those rows. This is
    ///   the one an application reaches during an ordinary migration.
    ///
    /// If the new instance restarted at 0 it would hit those entries.
    #[test]
    fn every_table_starts_at_a_generation_no_earlier_table_used() {
        let a = next_cache_generation();
        let b = next_cache_generation();
        let c = next_cache_generation();
        assert!(b > a, "{} !> {}", b, a);
        assert!(c > b, "{} !> {}", c, b);
        // Never zero: a stale entry from a table that predates this fix — or a
        // default-constructed generation — must not match a fresh table.
        assert!(a > 0);
    }
}

#[cfg(test)]
mod candidate_mask_tests {
    use super::{leading_excluded, mask_is_worth_using};

    #[test]
    fn leading_excluded_counts_whole_zero_words_then_the_partial_one() {
        // Two empty words, then the first candidate at bit 3 of the third.
        assert_eq!(leading_excluded(&[0, 0, 0b1000, u64::MAX]), 64 + 64 + 3);

        // First slot is a candidate: nothing is skipped.
        assert_eq!(leading_excluded(&[1, u64::MAX]), 0);

        // Nothing survives anywhere: every slot the words cover is skipped.
        assert_eq!(leading_excluded(&[0, 0, 0, 0]), 256);

        // An empty mask has no leading run to speak of.
        assert_eq!(leading_excluded(&[]), 0);

        // The top bit of a word is still found rather than rolled past.
        assert_eq!(leading_excluded(&[1u64 << 63]), 63);
    }

    /// The regression this exists for: a mask that barely prunes overall but
    /// excludes a long leading run must be kept.
    #[test]
    fn a_mask_that_prunes_late_is_kept_even_when_it_barely_prunes_at_all() {
        // The shape of `WHERE id >= 99995 LIMIT 5` over 1M rows: ~90% of the
        // table survives, so the ratio test alone rejects it...
        assert!(!mask_is_worth_using(0.90, 0.5, 0, 10_000, true));
        // ...but the same ratio with ~100k slots skipped up front is exactly
        // the case that was costing a real query 5.6 ms on every repeat.
        assert!(mask_is_worth_using(0.90, 0.5, 99_995, 10_000, true));
    }

    /// The leading-run exception exists for scans that stop early. A scan that
    /// reads the whole table would pay the per-record test across all of it to
    /// save one batch of fetches — so without a limit, only the ratio counts.
    #[test]
    fn a_late_pruning_mask_is_rejected_when_the_scan_cannot_stop_early() {
        // Identical to the accepted case above but for `can_stop_early`.
        assert!(mask_is_worth_using(0.90, 0.5, 99_995, 10_000, true));
        assert!(!mask_is_worth_using(0.90, 0.5, 99_995, 10_000, false));
    }

    #[test]
    fn a_broadly_pruning_mask_is_still_kept_with_no_leading_run() {
        assert!(mask_is_worth_using(0.10, 0.5, 0, 10_000, true));
        // A broad prune needs no help from early termination.
        assert!(mask_is_worth_using(0.10, 0.5, 0, 10_000, false));
    }

    /// A mask that neither prunes broadly nor starts late is still rejected —
    /// the per-record test would not pay for itself.
    #[test]
    fn a_mask_that_does_neither_is_still_rejected() {
        assert!(!mask_is_worth_using(0.99, 0.5, 9_999, 10_000, true));
        // Exactly at the threshold is worth it, one short of it is not.
        assert!(mask_is_worth_using(0.99, 0.5, 10_000, 10_000, true));
    }
}
