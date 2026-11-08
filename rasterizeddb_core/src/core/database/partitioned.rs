//! Partitioned tables in a [`Database`] (`docs/partitioning.md` §3.1, §7.1,
//! §12): CREATE, DROP, the startup open, and the catalogue rules they share.
//!
//! A partitioned table is in two maps: its [`PartitionedTable`] in
//! `Database.partitioned_tables`, and its schema-only shell in a slot under
//! the logical name (`slots.rs`), where every data call refuses. Its catalogue row carries `clusters = uid`
//! (nonzero), and every file it has is `__pt_<uid>_*` in the database
//! directory. No file starts with its logical name: an older binary, which
//! ignores `clusters`, then finds no `<name>_schema.db` and leaves the table
//! out as unreadable rather than opening it empty (the downgrade guard, I18).

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc as StdArc;

use dashmap::mapref::entry::Entry;
use log::{error, info, warn};
use rclite::Arc;

use super::{
    Barrier, Database, ForgetFailure, SchemaChanged, forget_logged_ids, logs_holding, pool_mirrors,
    table_name_refusal,
};
use crate::core::partition::files;
use crate::core::partition::key::KeySpec;
use crate::core::partition::route::Route;
use crate::core::partition::table::{
    MAX_DECLARED_PER_DATABASE, MirrorsConfigured, OpenCtx, PartitionedTable, Placement, check_declared_cap,
    declared_in,
};
#[cfg(not(feature = "storage_pools"))]
use crate::core::partition::table::RootOnly;
use crate::core::row::schema::TableSchema;
use crate::core::row::table::Table;
use crate::core::rql::lexer_ct::CreateTable;
use crate::core::storage_providers::backend::DbStorageProvider;
use crate::core::storage_providers::traits::StorageIO;
use crate::core::vacuum::VacuumReport;

/// A table name held by one CREATE or DROP while it runs. Released on drop.
pub(crate) struct NameClaim<'a> {
    claims: &'a dashmap::DashMap<String, (), ahash::RandomState>,
    name: String,
}

impl Drop for NameClaim<'_> {
    fn drop(&mut self) {
        self.claims.remove(&self.name);
    }
}

/// Why [`Database::discard_uid_files`] kept a uid's files: the barrier
/// voiding their records failed (kept, since deleted with live records a
/// replay would write them back), or deleting them did.
enum DiscardFailure {
    Void(ForgetFailure),
    Delete(io::Error),
}

impl DiscardFailure {
    fn into_error(self) -> io::Error {
        match self {
            DiscardFailure::Void(f) => f.error,
            DiscardFailure::Delete(e) => e,
        }
    }
}

impl std::fmt::Display for DiscardFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiscardFailure::Void(v) => write!(f, "voiding their log records: {}", v.error),
            DiscardFailure::Delete(e) => write!(f, "deleting them: {e}"),
        }
    }
}

fn unsupported(why: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, why.into())
}

/// Refuses a catalogue where two live rows share a name and either is a
/// partitioned table's, or two rows share a uid (§3.1, I18). Startup opens
/// one table per row and the second insert into the map wins, so either
/// table could be the one served, and the other's rows would be invisible.
/// Two plain rows under one name share their files, so they open as one
/// table; that is a leftover of older bugs, logged rather than refused so it
/// cannot stop a database that ran with it.
pub(super) fn catalogue_duplicates(rows: &[(String, u64)]) -> Result<(), String> {
    let mut by_name: HashMap<&str, Vec<u64>> = HashMap::new();
    let mut by_uid: HashMap<u64, Vec<&str>> = HashMap::new();
    for (name, clusters) in rows {
        by_name.entry(name.as_str()).or_default().push(*clusters);
        if *clusters != 0 {
            by_uid.entry(*clusters).or_default().push(name.as_str());
        }
    }
    let clusters_text = |c: &u64| {
        if *c == 0 {
            "clusters = 0 (a plain table)".to_string()
        } else {
            format!("clusters = {c:#018x} (a partitioned table)")
        }
    };
    let mut refusals = Vec::new();
    let mut names: Vec<_> = by_name.into_iter().filter(|(_, c)| c.len() > 1).collect();
    names.sort();
    for (name, clusters) in names {
        let rows = clusters.iter().map(clusters_text).collect::<Vec<_>>().join(" and ");
        if clusters.iter().any(|c| *c != 0) {
            refusals.push(format!("two live catalogue rows name table \"{name}\": {rows}"));
        } else {
            error!(
                "two live catalogue rows name table \"{name}\" ({rows}); they share their files and open \
                 as one table"
            );
        }
    }
    // A partitioned RENAME commits by writing a second row with the same uid
    // (§3.1), and its recovery (`partitioned_ddl::recover_interrupted_rename`,
    // run before the catalogue is read) tombstones the old row. Two rows
    // still sharing a uid here mean that tombstone failed, or no intent says
    // which row is newer: refused rather than guessed at.
    let mut uids: Vec<_> = by_uid.into_iter().filter(|(_, n)| n.len() > 1).collect();
    uids.sort();
    for (uid, names) in uids {
        refusals.push(format!(
            "catalogue rows for tables {} all claim the partitioned files of uid {uid:#018x}",
            names.iter().map(|n| format!("\"{n}\"")).collect::<Vec<_>>().join(", ")
        ));
    }
    if refusals.is_empty() {
        return Ok(());
    }
    Err(format!(
        "{}. Opening would serve one of them and hide the other's rows; a version that ignores \
         partitioned tables can create a plain table over one. Nothing was opened or changed.",
        refusals.join("; ")
    ))
}

/// Why a UNIQUE or PRIMARY KEY over `columns` cannot be declared on a table
/// partitioned by `key` (§1b): uniqueness is checked inside one partition,
/// so a constraint missing a key column would accept equal values stored in
/// two. `None` when it contains every key column. The one wording, for
/// `CREATE … PARTITION BY`, `ADD CONSTRAINT` and `CREATE UNIQUE INDEX`.
pub(crate) fn unique_key_refusal<K: AsRef<str>>(key: &[K], columns: &[String]) -> Option<String> {
    let missing = key.iter().map(AsRef::as_ref).find(|k| !columns.iter().any(|c| c.eq_ignore_ascii_case(k)))?;
    let mut with_key: Vec<&str> = key.iter().map(AsRef::as_ref).collect();
    with_key.extend(
        columns
            .iter()
            .map(String::as_str)
            .filter(|c| !key.iter().any(|k| k.as_ref().eq_ignore_ascii_case(c))),
    );
    Some(format!(
        "UNIQUE or PRIMARY KEY ({}) does not contain partition key column \"{missing}\"; a partitioned table \
         enforces uniqueness per partition, so the constraint must include every partition key column, as in \
         UNIQUE ({})",
        columns.join(", "),
        with_key.join(", ")
    ))
}

/// `VACUUM` of a partitioned table: [`Table::vacuum_now`] on every
/// materialized partition, summed into one report. An unmaterialized one
/// holds no row. A partition's failure fails the whole call, as a broken
/// table does: a sum that skipped one would read as the table vacuumed.
pub(super) async fn vacuum_partitions(table: &PartitionedTable) -> io::Result<VacuumReport> {
    use crate::core::mvcc::transaction::NO_TX;
    let epoch = table.epoch()?;
    let mut sum = VacuumReport {
        held_min_xmax: NO_TX,
        ..VacuumReport::default()
    };
    for (pid, partition) in table.materialized(&epoch, &Route::All).await? {
        let r = partition
            .vacuum_now()
            .await
            .map_err(|e| io::Error::new(e.kind(), format!("partition {pid}: {e}")))?;
        sum.slots += r.slots;
        sum.live_rows += r.live_rows;
        sum.dead_rows += r.dead_rows;
        sum.live_bytes += r.live_bytes;
        sum.dead_bytes += r.dead_bytes;
        sum.reclaimed_bytes += r.reclaimed_bytes;
        sum.extents += r.extents;
        sum.deferred |= r.deferred;
        sum.fingerprints_cleared += r.fingerprints_cleared;
        sum.held_rows += r.held_rows;
        if r.held_min_xmax != NO_TX && (sum.held_min_xmax == NO_TX || r.held_min_xmax < sum.held_min_xmax) {
            sum.held_min_xmax = r.held_min_xmax;
        }
    }
    Ok(sum)
}

/// The startup reaper (§12) for the database at `location`, run after WAL
/// recovery and the catalogue read and before any table opens, so before
/// forgetting resumes. Deletes the `__pt_` files of uids no catalogue row
/// holds and of partitions their map never materialized, after voiding the
/// logs' records for them, so a later replay cannot write them back into a
/// partition materialized again under the same name.
///
/// Deletes nothing when any reason to doubt the catalogue or a map exists:
/// a live map that is missing, torn or not fully understood, or a plain
/// table whose name starts with `__pt_` (older builds allowed it), whose
/// files the pattern could match (I15).
///
/// It deliberately takes no deletion gate (`backup::deletion_permit`),
/// unlike a DROP: it runs inside `Database::new`, before any backup service
/// can exist on this database, so there is no snapshot for the gate to
/// protect, and taking it there is unnecessary and risks a deadlock with a
/// service of another database already holding it.
pub(super) fn reap_at_startup(
    location: &str,
    io_pointers: &Arc<DbStorageProvider>,
    pool: &super::StoragePool,
    rows: &[(String, u64)],
) -> files::ReapReport {
    let root = Path::new(location);
    if let Some((name, _)) = rows.iter().find(|(name, c)| *c == 0 && name.starts_with(files::PREFIX)) {
        let why = format!(
            "plain table \"{name}\" has a name under the partition prefix {}, so its files could \
             look like a partitioned table's",
            files::PREFIX
        );
        warn!("Partition file reaping is off for this start: {why}");
        return files::ReapReport {
            deleted: Vec::new(),
            kept: Some(why),
        };
    }
    let live: HashMap<u64, Result<files::LiveMap, String>> = rows
        .iter()
        .filter(|(_, c)| *c != 0)
        .map(|(_, uid)| (*uid, files::read_live_map(root, *uid)))
        .collect();
    // Other storage locations first (phase 4): partitions before the parent
    // files that say what they were, as a DROP orders them.
    let root_key = files::dir_key(location);
    let mut report = files::ReapReport::default();
    for dir in super::pool_table_paths(pool) {
        if files::dir_key(&dir) == root_key {
            continue;
        }
        let now = |recorded: &str| pool.partition_dir_now(recorded).unwrap_or_else(|| recorded.to_string());
        let at = files::reap_at(Path::new(&dir), &live, &now, |names| {
            let Some(wal) = crate::core::wal::wal_for(&dir) else {
                return Ok(());
            };
            let ids: Vec<u64> = names.iter().map(|n| crate::core::wal::file_id_for_name(n)).collect();
            forget_logged_ids(&[wal], &ids, Barrier::Drop).map_err(|f| f.error)
        });
        report.deleted.extend(at.deleted);
        if report.kept.is_none() {
            report.kept = at.kept.map(|why| format!("in {dir}: {why}"));
        }
    }
    let at_root = files::reap(root, &live, |names| {
        let logs = logs_holding(io_pointers, pool, Some(location));
        let ids: Vec<u64> = names.iter().map(|n| crate::core::wal::file_id_for_name(n)).collect();
        forget_logged_ids(&logs, &ids, Barrier::Drop).map_err(|f| f.error)
    });
    report.deleted.extend(at_root.deleted);
    if report.kept.is_none() {
        report.kept = at_root.kept;
    }
    if let Some(why) = &report.kept {
        warn!("Partition file reaping deleted nothing this start: {why}");
    } else if !report.deleted.is_empty() {
        info!(
            "Reaped {} partition file(s) no table holds: {}",
            report.deleted.len(),
            report.deleted.join(", ")
        );
    }
    report
}

/// The storage pool as partitions see it (phase 4, docs/partitioning.md
/// §6.1): its dealer, its counts, and which directories it names.
#[cfg(feature = "storage_pools")]
struct PoolPlacement(super::StoragePool);

#[cfg(feature = "storage_pools")]
impl Placement for PoolPlacement {
    fn deal(&self, uid: u64) -> Option<String> {
        // Placed by the pool's policy with or without a backup service: a
        // restore into a new directory records where it moved each location
        // (`storage::repoint_restored_pool`'s aliases), and the restored map
        // opens its partitions there (phase 4b).
        self.0.deal_partition(uid, false)
    }

    fn undeal(&self, uid: u64, dir: &str) {
        self.0.undeal_partition(uid, dir)
    }

    fn register(&self, uid: u64, counts: HashMap<String, u64>) {
        self.0.register_partitions(uid, counts)
    }

    fn resolve(&self, dir: &str) -> Option<String> {
        self.0.partition_dir_now(dir)
    }

    fn others(&self) -> Vec<PathBuf> {
        let root = self.0.root().map(files::dir_key);
        super::pool_table_paths(&self.0)
            .into_iter()
            .filter(|p| Some(files::dir_key(p)) != root)
            .map(PathBuf::from)
            .collect()
    }
}

/// Every file name in `dir` under `uid`: what [`files::remove_all`] deletes,
/// so what the log's records must be voided for first.
fn uid_file_names(dir: &Path, uid: u64) -> io::Result<Vec<String>> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let Ok(name) = entry?.file_name().into_string() else {
            continue;
        };
        let parsed = files::parse_name(&name).or_else(|| {
            name.strip_suffix(files::TORN)
                .and_then(files::parse_name)
                .filter(|f| matches!(f, files::PartFile::Map { .. }))
        });
        if parsed.is_some_and(|f| f.uid() == uid) {
            names.push(name);
        }
    }
    Ok(names)
}

impl Database {
    /// Holds `name` against every other CREATE (plain or partitioned), RENAME
    /// (either name) and partitioned DROP until the claim drops. Between a
    /// name check and the map insert, a second writer of the name's row would
    /// otherwise write a second catalogue row, which the next start refuses.
    /// Never waits, so claims taken in any order cannot deadlock.
    pub(crate) fn claim_name(&self, name: &str) -> io::Result<NameClaim<'_>> {
        match self.name_claims.entry(name.to_string()) {
            Entry::Occupied(_) => Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("table \"{name}\" is being created, renamed or dropped by another statement"),
            )),
            Entry::Vacant(slot) => {
                slot.insert(());
                Ok(NameClaim {
                    claims: &self.name_claims,
                    name: name.to_string(),
                })
            }
        }
    }

    /// Whether the pool has backup mirrors now, asked at each call: a mirror
    /// added while the server runs never reaches the root seed's kind.
    fn mirrors_configured(&self) -> MirrorsConfigured {
        let pool = self.storage.clone();
        StdArc::new(move || !pool_mirrors(&pool).is_empty())
    }

    fn open_ctx(&self) -> OpenCtx<'_> {
        OpenCtx {
            seed: &self.db_io_pointers,
            db_id: self.id,
            transactions: &self.transactions,
            mirrored: self.mirrors_configured(),
            scheduler: &self.partition_maintenance,
            placement: self.partition_placement(),
        }
    }

    /// Where this database's partitions materialize (phase 4, §6.1).
    fn partition_placement(&self) -> StdArc<dyn Placement> {
        #[cfg(feature = "storage_pools")]
        {
            StdArc::new(PoolPlacement(self.storage.clone()))
        }
        #[cfg(not(feature = "storage_pools"))]
        {
            StdArc::new(RootOnly)
        }
    }

    /// A clean shutdown's record of every partitioned table's `SERIAL`
    /// high-water marks, exactly (§10): after the sessions have ended, so a
    /// clean restart resumes with no gap. Called by `seal_for_shutdown`;
    /// errors are logged, never fatal: a table left as it was resumes past
    /// its reservation, a gap.
    pub async fn record_clean_shutdown(&self) {
        let tables: Vec<Arc<PartitionedTable>> = self
            .relation_names()
            .into_iter()
            .filter_map(|name| match self.relation(&name) {
                Some(super::Relation::Partitioned(t)) => Some(t),
                _ => None,
            })
            .collect();
        for t in tables {
            if let Err(e) = t.record_serial_exact().await {
                warn!("shutdown: the SERIAL high-water of \"{}\" was not recorded: {e}", t.name());
            }
        }
    }

    /// The scheduler this database's partitions are maintained by (§7.3).
    pub fn partition_maintenance(&self) -> &StdArc<crate::core::partition::maintenance::Scheduler> {
        &self.partition_maintenance
    }

    /// The database directory, where phase 2a keeps every partitioned file.
    fn partition_root(&self) -> io::Result<PathBuf> {
        self.location
            .as_deref()
            .map(PathBuf::from)
            .ok_or_else(|| unsupported("partitioned tables in an in-memory database are not supported yet"))
    }

    /// Why `ct` cannot become a partitioned table over `key` with
    /// `partitions` partitions here, checked before anything is written.
    /// Called with the partitioned DDL lock held, so the cap's count is
    /// current.
    fn partitioned_create_refusal(&self, ct: &CreateTable, key: &[&str], partitions: u32) -> io::Result<()> {
        self.partitioned_name_refusal(ct)?;
        self.partitioned_shape_refusal(ct, key, partitions)
    }

    /// Whether `ct`'s name is free for a new partitioned table.
    fn partitioned_name_refusal(&self, ct: &CreateTable) -> io::Result<()> {
        let name = &ct.table_name;
        if let Some(reason) = table_name_refusal(name) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, reason));
        }
        // As a plain CREATE: kept files under the name still have live log
        // records, and the next open settles them by the names the catalogue
        // holds, this one included.
        self.refuse_held_name(name)?;
        if let Some(reason) = self.table_files_conflict(name) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, reason));
        }
        if self.has_table(name) || self.is_partitioned(name) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "Table already exists"));
        }
        // A reload, MOVE or plain DROP takes the table out of `tables` and
        // may put it (or its row) back, without a claim. On disk its files
        // refuse the name above; on a key/value or memory backend there are
        // none to find, but its placement stays recorded throughout.
        if let Some(spec) = super::pool_spec_of(&self.storage, name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("table \"{name}\" has a storage placement recorded ({spec}), so the name is taken"),
            ));
        }
        Ok(())
    }

    /// Why `ct` cannot be partitioned over `key` into `partitions` here,
    /// whatever its name: what a `CREATE … PARTITION BY` checks past the
    /// name, and what a conversion of an existing table checks
    /// (`partitioned_convert`).
    fn partitioned_shape_refusal(&self, ct: &CreateTable, key: &[&str], partitions: u32) -> io::Result<()> {
        self.partition_root()?;
        if !pool_mirrors(&self.storage).is_empty() {
            return Err(unsupported(
                "partitioned tables in a storage pool with backup mirrors are not supported yet",
            ));
        }
        let cap = self.declared_cap();
        if partitions == 0 || partitions > cap {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("PARTITIONS must be between 1 and {cap}, not {partitions}"),
            ));
        }
        if key.is_empty() || key.len() > 3 {
            return Err(unsupported(format!(
                "a partition key of {} columns is not supported; it takes 1 to 3",
                key.len()
            )));
        }
        // Foreign keys both ways (phase 3 P4): each one it declares checked
        // as a plain CREATE checks it, and any naming it (or itself) must
        // reference its whole partition key, so every check routes.
        if let Some(why) = self.create_foreign_key_refusal(ct, Some(key)) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, why));
        }
        // A shredded JSON column is a shadow table the drop would have to
        // follow; refused rather than half-handled.
        if !ct.json_columns.is_empty() {
            return Err(unsupported("JSON columns stored as shadow tables are not supported on a partitioned table yet"));
        }
        for column in key {
            let flag = crate::core::rql::nullable::flag_column_name(column);
            if ct.columns.iter().any(|c| c.name.eq_ignore_ascii_case(&flag)) {
                return Err(unsupported(format!(
                    "partition key column \"{column}\" is nullable; a partition key column must be NOT NULL"
                )));
            }
            if ct.generated.iter().any(|g| !g.is_deleted && g.column.eq_ignore_ascii_case(column)) {
                return Err(unsupported(format!(
                    "partition key column \"{column}\" is generated, which is not supported"
                )));
            }
        }
        // §1b: uniqueness is checked inside one partition, so a constraint
        // missing a key column would accept equal values stored in two.
        let constraints = ct
            .unique_columns
            .iter()
            .map(|c| std::slice::from_ref(c))
            .chain(ct.unique_keys.iter().filter(|k| !k.is_deleted).map(|k| k.columns.as_slice()))
            .chain(ct.primary_key.iter().map(std::slice::from_ref));
        for columns in constraints {
            if let Some(why) = unique_key_refusal(key, columns) {
                return Err(unsupported(why));
            }
        }
        let open: Vec<Arc<PartitionedTable>> = self.partitioned_tables_all();
        let declared = declared_in(open.iter().map(|t| &**t))?;
        if cap != MAX_DECLARED_PER_DATABASE {
            return crate::core::partition::table::check_declared_cap_at(declared, partitions, cap);
        }
        check_declared_cap(declared, partitions)
    }

    /// The declared-partitions cap (§7.1): [`MAX_DECLARED_PER_DATABASE`],
    /// or in a test a measurement's raised one for its directory
    /// (`cap_hooks`), so phase 3's scale can be measured before the cap is
    /// raised for everyone.
    fn declared_cap(&self) -> u32 {
        #[cfg(test)]
        if let Some(cap) = self.location.as_deref().and_then(|l| cap_hooks::cap_for(Path::new(l))) {
            return cap;
        }
        MAX_DECLARED_PER_DATABASE
    }

    /// Creates a hash-partitioned table (§12's CREATE order): `ct`'s schema
    /// with `partitions` partitions keyed on `key` (declared column names, 1
    /// to 3, outermost first), none materialized.
    ///
    /// Before the catalogue row is durable a failure deletes the uid's files
    /// and leaves no trace. The row is the commit point: once it is written
    /// the table goes into both maps whatever follows, broken if it must be,
    /// so a retried CREATE finds it rather than writing a second row.
    pub async fn create_partitioned_table(&self, mut ct: CreateTable, key: &[&str], partitions: u32) -> io::Result<()> {
        let _ddl = self.partition_ddl.lock().await;
        let _claim = self.claim_name(&ct.table_name)?;
        if let Err(e) = self.partitioned_create_refusal(&ct, key, partitions) {
            warn!("CREATE TABLE {} PARTITION BY refused: {}", ct.table_name, e);
            return Err(e);
        }
        let _changed = SchemaChanged::unless_internal(&self.schema_generation, &ct.table_name);
        let root = self.partition_root()?;
        // A uid no live table holds and no file in the root is under, so
        // §12's step 1 (delete orphans of the uid) has nothing to delete.
        let open = self.partitioned_tables_all();
        let uid = files::new_uid(&root, |u| open.iter().any(|t| t.uid() == u))?;

        let columns = std::mem::take(&mut ct.columns);
        let (io_schema, schema) = match self.write_partitioned_files(&root, &ct, columns, key, partitions, uid).await {
            Ok(written) => written,
            Err(e) => {
                error!("CREATE TABLE {} PARTITION BY failed before its catalogue row: {}", ct.table_name, e);
                if let Err(discard) = self.discard_uid_files_gated(&root, uid).await {
                    error!("... and its files under uid {uid:016x} were kept: {discard}");
                }
                return Err(e);
            }
        };
        #[cfg(test)]
        if let Some((parked, release)) = super::test_hooks::take_partitioned_create_park(&ct.table_name) {
            parked.notify_one();
            release.notified().await;
        }

        if let Err(e) = self.write_catalogue_row(&ct.table_name, uid).await {
            error!("CREATE TABLE {} PARTITION BY could not write its catalogue row: {}", ct.table_name, e);
            if let Err(discard) = self.discard_uid_files_gated(&root, uid).await {
                error!("... and its files under uid {uid:016x} were kept: {discard}");
            }
            return Err(e);
        }
        // Past the commit point, whatever the sync says.
        let synced = self.sync_durable();

        let mut shell = Table::partition_shell(io_schema, schema);
        shell.scope_cache_to_database(self.id);
        shell.enable_versioning(self.transactions.clone());
        let shell = Arc::new(shell);
        let table = Arc::new(PartitionedTable::open(self.open_ctx(), &ct.table_name, uid, shell.clone()).await);
        let broken = table.broken().map(str::to_string);
        self.install_partitioned(ct.table_name.clone(), table, Some(shell));
        synced?;
        if let Some(why) = broken {
            return Err(io::Error::other(why));
        }
        info!("Created partitioned table {} ({uid:016x}), {partitions} partitions", ct.table_name);
        Ok(())
    }

    /// §12's CREATE step 2: the parent files, the schema and its sidecars,
    /// the map log and the ids file, every entry and byte durable. Returns
    /// the schema as written and its provider, the shell's.
    async fn write_partitioned_files(
        &self,
        root: &Path,
        ct: &CreateTable,
        columns: Vec<crate::core::rql::lexer_ct::CreateColumnData>,
        key: &[&str],
        partitions: u32,
        uid: u64,
    ) -> io::Result<(Arc<DbStorageProvider>, TableSchema)> {
        files::create_parent_files(root, uid)?;
        let io_schema = Arc::new(self.db_io_pointers.create_new(format!("{}.db", files::schema_stem(uid))).await);
        // `ct.columns` was taken for `columns`: asked of the columns given.
        let own_scale: Vec<String> = columns
            .iter()
            .filter(|c| c.data_type == crate::core::db_type::DbType::DECIMAL)
            .filter(|c| !ct.type_modifiers.iter().any(|m| !m.is_deleted && m.column.eq_ignore_ascii_case(&c.name)))
            .map(|c| c.name.clone())
            .collect();
        // The slot every earlier build wrote, for tests of such tables.
        #[cfg(test)]
        let own_scale = if self.legacy_numeric_slot.load(std::sync::atomic::Ordering::Relaxed) {
            Vec::new()
        } else {
            own_scale
        };
        if !own_scale.is_empty() {
            self.require_numeric_own_scale()?;
        }
        let mut schema =
            Table::create_schema_files_with(io_schema.clone(), &ct.table_name, columns, &ct.unique_columns, &own_scale)
                .await?;
        super::write_schema_sidecars(io_schema.clone(), &mut schema, ct).await?;
        // From the schema as written: the write orders are the file's.
        let spec = KeySpec::from_schema(&schema, key).map_err(unsupported)?;
        PartitionedTable::create_files(&self.db_io_pointers, !pool_mirrors(&self.storage).is_empty(), uid, &spec, partitions)
            .await?;
        // The sidecars' directory entries; their bytes and the schema's went
        // through the log.
        files::sync_dir(root)?;
        self.sync_durable()?;
        Ok((io_schema, schema))
    }

    /// Voids every log record for `uid`'s files, then deletes them, the
    /// deletions synced (`remove_all`). The void is the plain DROP's
    /// ([`Database::forget_dropped_ids`]): the database's own log synced
    /// first, so the catalogue change ahead of it is durable, then a barrier
    /// in every log the root reaches, whose fence refuses any later record
    /// from a writer that opened these files before it (a statement still
    /// holding a partition). A void that fails keeps the files: deleted with
    /// live records, a replay would write them back as orphans. Either way
    /// nothing names the uid, and the startup reaper finishes the job.
    ///
    /// The caller holds the deletion gate ([`Self::uid_deletion_permit`])
    /// from before the barrier until the deletions are done, as a plain DROP
    /// holds it from before its catalogue tombstone through its sweep: a
    /// restore empties a file at its barrier only when the barrier committed
    /// after the snapshot's copy began, so the barrier and the deletion must
    /// be on the same side of that point.
    ///
    /// Partitions on other storage locations go first (phase 4): voided in
    /// their location's log and deleted there, so the parent files in the
    /// root, which say what the rest were, go last (§12's DROP order).
    fn discard_uid_files(&self, root: &Path, uid: u64) -> Result<(), DiscardFailure> {
        let root_key = files::dir_key(&root.to_string_lossy());
        for dir in super::pool_table_paths(&self.storage) {
            if files::dir_key(&dir) == root_key {
                continue;
            }
            let at = Path::new(&dir);
            let names = match uid_file_names(at, uid) {
                Ok(names) => names,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(DiscardFailure::Delete(e)),
            };
            if names.is_empty() {
                continue;
            }
            let ids: Vec<u64> = names.iter().map(|n| crate::core::wal::file_id_for_name(n)).collect();
            self.forget_dropped_ids_in(&dir, &ids).map_err(DiscardFailure::Void)?;
            files::remove_all(at, uid).map_err(DiscardFailure::Delete)?;
        }
        let names = uid_file_names(root, uid).map_err(DiscardFailure::Delete)?;
        if names.is_empty() {
            return Ok(());
        }
        let ids: Vec<u64> = names.iter().map(|n| crate::core::wal::file_id_for_name(n)).collect();
        self.forget_dropped_ids(&self.db_io_pointers, &ids).map_err(DiscardFailure::Void)?;
        files::remove_all(root, uid).map(|_| ()).map_err(DiscardFailure::Delete)
    }

    /// [`Self::forget_dropped_ids`] for files in `dir`, another storage
    /// location: its own log's barrier, after the database's own log is
    /// synced so the catalogue tombstone ahead of it is durable.
    fn forget_dropped_ids_in(&self, dir: &str, ids: &[u64]) -> Result<(), ForgetFailure> {
        let Some(wal) = crate::core::wal::wal_for(dir) else {
            return Ok(());
        };
        self.sync_own_log().map_err(|error| ForgetFailure {
            error,
            maybe_logged: false,
        })?;
        forget_logged_ids(&[wal], ids, Barrier::Drop)
    }

    /// [`super::deletion_permit`] for a partitioned table's files. The gate
    /// is one for the whole process, so asking with the root's logs covers
    /// the partitions on other locations too. `None` when no log is there (in memory) or without
    /// the `s3_backup` feature.
    async fn uid_deletion_permit(&self) -> Option<impl Sized> {
        let logs = logs_holding(&self.db_io_pointers, &self.storage, self.location.as_deref());
        super::deletion_permit(&logs).await
    }

    /// [`Self::discard_uid_files`] under the deletion gate, for a caller that
    /// does not already hold it: a CREATE cleaning up after itself.
    async fn discard_uid_files_gated(&self, root: &Path, uid: u64) -> Result<(), DiscardFailure> {
        let _snapshot_gate = self.uid_deletion_permit().await;
        self.discard_uid_files(root, uid)
    }

    /// DROP of a partitioned table (§12's DROP order): the catalogue
    /// tombstone is the commit point; then both maps, `DROPPED`, the barrier
    /// voiding the files' records, and the files, partitions first and parent
    /// files last. Works on a broken table. `Ok(false)` when no partitioned
    /// table has the name (a concurrent DROP won).
    ///
    /// A failure after the commit point leaves files with no catalogue row,
    /// which the startup reaper voids and deletes; the table is gone, so the
    /// DROP reports success, as a plain DROP whose barrier may be durable
    /// does. Unless the database's own log is poisoned: then not even the
    /// tombstone is known to be durable, and the DROP reports the error, as a
    /// plain DROP does. Unlike a plain DROP it never puts the table back: by
    /// then `DROPPED` is durable in its map log. A successor under the name
    /// takes a new uid, so no record of these files can replay into it.
    pub(super) async fn drop_partitioned(&self, name: &str) -> io::Result<bool> {
        // The map key DDL mutates (`resolve_key`): where identifier case
        // folding drops in, so every mutator removes the slot a lookup finds.
        let resolved = self.resolve_key(name);
        let name = resolved.as_deref().unwrap_or(name);
        let _ddl = self.partition_ddl.lock().await;
        let _claim = self.claim_name(name)?;
        // Through the deletions below, as a plain DROP holds it (see
        // `deletion_permit`), and taken as that one is, before the table
        // leaves the maps and the catalogue: waiting with the table half gone
        // would hide a table whose files are still on disk.
        let _snapshot_gate = self.uid_deletion_permit().await;
        let Some(table) = self.partitioned_table(name) else {
            return Ok(false);
        };
        // A rename of this table that has not finished keeps its old row
        // live: dropping the files now would leave that row naming none.
        if let Some(why) = self.unfinished_partitioned_rename(Some(table.uid())) {
            return Err(io::Error::other(why));
        }
        if self.held_names.lock().iter().any(|h| h == name) {
            return Err(io::Error::other(format!(
                "\"{name}\" is held by a rename that has not finished; restart before dropping it"
            )));
        }
        if let Err(e) = self.forget_in_catalogue(name).await {
            error!("Could not remove {} from the catalogue: {}", name, e);
            return Err(e);
        }
        self.take_partitioned(name, &table);

        let uid = table.uid();
        #[cfg(feature = "storage_pools")]
        self.storage.forget_partitions(uid);
        let kept = |what: &str, e: &dyn std::fmt::Display| {
            error!(
                "Dropped partitioned table {name} ({uid:016x}), but {what} failed, so its files were kept for \
                 the startup reaper: {e}"
            );
        };
        let unconfirmed = |e: io::Error| -> io::Result<bool> {
            if self.own_log_poisoned() { Err(e) } else { Ok(true) }
        };
        if let Err(e) = table.mark_dropped().await {
            kept("appending DROPPED to its map log", &e);
            return unconfirmed(e);
        }
        match self.partition_root() {
            Ok(root) => {
                if let Err(failure) = self.discard_uid_files(&root, uid) {
                    kept("voiding its log records or deleting its files", &failure);
                    return unconfirmed(failure.into_error());
                }
            }
            Err(e) => kept("finding its directory", &e),
        }
        info!("Dropped partitioned table {name} ({uid:016x})");
        Ok(true)
    }

    /// Opens the partitioned tables the catalogue names (§7.1), after every
    /// plain table: one that cannot open is left broken, in the partitioned
    /// map so a DROP still reaches it, and never stops the rest.
    pub(super) async fn open_partitioned_rows(&self, rows: Vec<(String, u64)>) {
        #[cfg(test)]
        self.forget_before_partitioned_open().await;
        for (name, uid) in rows {
            let (shell, table) = self.open_partitioned(&name, uid).await;
            self.install_partitioned(name, Arc::new(table), shell);
        }
    }

    /// Test-only: what a plain table's vacuum could do in the window before
    /// the partitioned tables register (settle, sweep every plain table,
    /// forget), for a database whose location a test armed with
    /// [`startup_hooks::forget_before_partitioned_open`].
    #[cfg(test)]
    async fn forget_before_partitioned_open(&self) {
        let Some(location) = self.location.as_deref() else {
            return;
        };
        if !startup_hooks::take(Path::new(location)) {
            return;
        }
        self.transactions.settle_now();
        for name in self.table_names() {
            if let Some(t) = self.table_for_tests(&name)
                && !t.is_shell()
            {
                let _ = t.neutralize_aborted_now().await;
            }
        }
        self.transactions.try_forget();
    }

    /// One partitioned table at startup, and its shell when its schema read.
    /// Nothing is created: the schema file is checked by a never-creating
    /// open before a provider is built on it, since providers create.
    async fn open_partitioned(&self, name: &str, uid: u64) -> (Option<Arc<Table<DbStorageProvider>>>, PartitionedTable) {
        match self.load_shell(name, uid).await {
            Ok(shell) => {
                let table = PartitionedTable::open(self.open_ctx(), name, uid, shell.clone()).await;
                (Some(shell), table)
            }
            Err(why) => (None, self.refused_partitioned(name, uid, &why)),
        }
    }

    /// A partitioned table refused before its map is read, for `why`.
    fn refused_partitioned(&self, name: &str, uid: u64, why: &str) -> PartitionedTable {
        // A memory provider stands in for the schema: building a file
        // provider would create the file whose absence is the problem.
        let schema_file = format!("{}.db", files::schema_stem(uid));
        let stand_in = Arc::new(DbStorageProvider::new_memory(&schema_file));
        let shell = Arc::new(Table::partition_shell(stand_in, TableSchema::new(name.to_string(), false)));
        let root = self.location.as_deref().map(PathBuf::from).unwrap_or_default();
        PartitionedTable::refused(self.open_ctx(), name, uid, root, shell, why)
    }

    /// The shell of partitioned table `uid`, read from its schema file under
    /// the catalogue's `name`, scoped and versioned. The catalogue row is
    /// what names the table: a schema header recording another name (a
    /// RENAME cut between its commit point and its header rewrite, §3.1) is
    /// read under the row's, so the name never depends on which of the two
    /// writes a crash kept.
    async fn load_shell(&self, name: &str, uid: u64) -> Result<Arc<Table<DbStorageProvider>>, String> {
        let schema_file = format!("{}.db", files::schema_stem(uid));
        let root = self.partition_root().map_err(|e| e.to_string())?;
        // Before any provider: a mirrored root would create the schema file's
        // copies in the backups.
        if !pool_mirrors(&self.storage).is_empty() {
            return Err("partitioned tables in a storage pool with backup mirrors are not supported yet".to_string());
        }
        files::require_existing(&root, std::slice::from_ref(&schema_file)).map_err(|e| e.to_string())?;
        let io_schema = Arc::new(self.db_io_pointers.create_new(schema_file.clone()).await);
        let mut schema = TableSchema::load(io_schema.clone())
            .await
            .map_err(|e| format!("its schema {schema_file} could not be read: {e}"))?;
        // Fenced before anything touches the table, as a plain table is at
        // open (`incompat::fence_numeric_own_scale`).
        if let Some(location) = self.location.as_deref() {
            super::incompat::fence_numeric_own_scale(Path::new(location), &schema.fields).map_err(|e| {
                format!("the directory could not be marked for its NUMERIC columns ({}): {e}", super::incompat::NUMERIC_OWN_SCALE_V1)
            })?;
        }
        if schema.name != name {
            warn!(
                "partitioned table \"{name}\" ({uid:016x}) has a schema recorded under the name \"{}\"; it is \
                 read under the catalogue's",
                schema.name
            );
            schema.name = name.to_string();
        }
        let mut shell = Table::partition_shell(io_schema, schema);
        shell.scope_cache_to_database(self.id);
        shell.enable_versioning(self.transactions.clone());
        Ok(Arc::new(shell))
    }

    /// The reopen of a partitioned schema change (`reload_partitioned`, §11
    /// step 3): the shell read again from its sidecars, under `name`, and
    /// `old`'s open partitions reopened under it. When the schema does not
    /// read, the table is broken and has no shell, as at startup.
    pub(super) async fn reopen_partitioned(
        &self,
        name: &str,
        old: &PartitionedTable,
    ) -> (Option<Arc<Table<DbStorageProvider>>>, PartitionedTable) {
        match self.load_shell(name, old.uid()).await {
            Ok(shell) => {
                let table = old.reopened(name, shell.clone()).await;
                (Some(shell), table)
            }
            Err(why) => (None, self.refused_partitioned(name, old.uid(), &why)),
        }
    }
}

/// Reshape (docs/partitioning.md §6.2, phase 5; `partition::reshape`).
impl Database {
    /// Runs `op` on partitioned table `name`. The caller holds ACCESS
    /// EXCLUSIVE on the logical name. Returns the statement's summary.
    ///
    /// Under the partitioned DDL lock, the deletion gate (files are deleted
    /// below) and a forget pause, as a schema change's reload: the plan read
    /// from the map, the reshape run to its commit point, then the table
    /// reopened over whatever map the log now holds (the new one, or the old
    /// one after an abort; the old instance is retired either way) and the
    /// prepared statements' generation moved. After a commit the sources'
    /// files are deleted, their log records voided first, then
    /// `RETIRED_GONE`; a failure there leaves them to the startup reaper, and
    /// the statement still succeeds. After a failure, the targets' files.
    pub(crate) async fn reshape_partitioned(
        &self,
        name: &str,
        op: &crate::core::partition::reshape::ReshapeOp,
    ) -> io::Result<String> {
        let resolved = self.resolve_key(name);
        let name = resolved.as_deref().unwrap_or(name);
        let _ddl = self.partition_ddl.lock().await;
        let Some(old) = self.partitioned_table(name) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} needs a partitioned table, and \"{name}\" is not one", op.verb()),
            ));
        };
        old.check()?;
        let plan = old.plan_reshape(op)?;
        let _snapshot_gate = self.uid_deletion_permit().await;
        let _forget_pause = self.transactions.forget_pause();
        let changed = super::SchemaChanged(&self.schema_generation);
        self.schema_generation.fetch_add(1, std::sync::atomic::Ordering::AcqRel);

        let result = old.reshape(&plan).await;
        let (new_shell, table) = self.reopen_partitioned(name, &old).await;
        let broken = table.broken().map(str::to_string);
        let table = Arc::new(table);
        self.swap_partitioned(name, table.clone(), new_shell);
        drop(changed);
        let uid = table.uid();
        match result {
            Ok(sources) => {
                // Test-only: the process "dies" right after the commit point,
                // leaving the retired sources' files for the next open.
                #[cfg(test)]
                if let Ok(root) = self.partition_root()
                    && crate::core::partition::table::reshape_hooks::crash(&root, "crash_after_commit")
                {
                    return Ok("crashed after the commit".to_string());
                }
                let pids: Vec<u32> = sources.doomed.iter().map(|(pid, _)| *pid).collect();
                match self.discard_pid_files(uid, &sources.doomed) {
                    Ok(()) => {
                        if let Err(e) = table.retired_gone(&pids).await {
                            warn!("{} of \"{name}\": recording partitions {pids:?} gone failed; the next open does: {e}", op.verb());
                        }
                    }
                    Err(e) => warn!(
                        "{} of \"{name}\" committed, but deleting the retired partitions' files failed; the startup \
                         reaper deletes them: {e}",
                        op.verb()
                    ),
                }
                if let Some(why) = broken {
                    return Err(io::Error::other(why));
                }
                let made = plan.targets.len();
                info!("{} of \"{name}\" ({uid:016x}): partitions {:?} replaced by {made}", op.verb(), plan.sources);
                Ok(format!(
                    "{}: {} partition(s) replaced by {made}",
                    op.verb(),
                    plan.sources.len()
                ))
            }
            Err((e, targets)) => {
                if let Err(d) = self.discard_pid_files(uid, &targets.doomed) {
                    warn!(
                        "{} of \"{name}\" failed, and deleting its targets' files failed too; the startup reaper \
                         deletes them: {d}",
                        op.verb()
                    );
                }
                Err(e)
            }
        }
    }

    /// Deletes the files of partitions `doomed` (`(pid, directory)`) of
    /// table `uid`, their log records voided first in the log of the
    /// directory they are in (as a DROP voids, `discard_uid_files`), each
    /// directory synced. The caller holds the deletion gate. A failure
    /// leaves the rest for the startup reaper, which deletes every file of a
    /// pid the map does not place.
    fn discard_pid_files(&self, uid: u64, doomed: &[(u32, PathBuf)]) -> io::Result<()> {
        let root = self.partition_root()?;
        let root_key = files::dir_key(&root.to_string_lossy());
        let mut by_dir: HashMap<String, (PathBuf, Vec<u32>)> = HashMap::new();
        for (pid, dir) in doomed {
            by_dir
                .entry(files::dir_key(&dir.to_string_lossy()))
                .or_insert_with(|| (dir.clone(), Vec::new()))
                .1
                .push(*pid);
        }
        for (key, (dir, pids)) in by_dir {
            let names: Vec<String> = uid_file_names(&dir, uid)?
                .into_iter()
                .filter(|n| matches!(files::parse_name(n), Some(files::PartFile::Data { pid, .. }) if pids.contains(&pid)))
                .collect();
            if names.is_empty() {
                continue;
            }
            let ids: Vec<u64> = names.iter().map(|n| crate::core::wal::file_id_for_name(n)).collect();
            if key == root_key {
                self.forget_dropped_ids(&self.db_io_pointers, &ids).map_err(|f| f.error)?;
            } else {
                self.forget_dropped_ids_in(&dir.to_string_lossy(), &ids).map_err(|f| f.error)?;
            }
            for n in &names {
                match std::fs::remove_file(dir.join(n)) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
            }
            files::sync_dir(&dir)?;
        }
        Ok(())
    }
}

#[path = "partitioned_convert.rs"]
mod convert;
pub(super) use convert::recover_interrupted_conversion;

/// Physical ALTER of a partitioned table (docs/partitioning.md "Phase 6 P8
/// as built"): every row rewritten under a new definition, into a new uid.
impl Database {
    /// Rewrites partitioned table `name` under the ACCESS EXCLUSIVE the
    /// caller holds:
    ///
    /// 1. the old table's open partitions retired (maintenance stopped), and
    ///    `read` handed every materialized partition: it returns the new
    ///    definition (a `CreateTable`, columns included) and its plan;
    /// 2. a new uid's files written from the definition, with the old
    ///    table's key (by its columns' names now) and partition count;
    ///    `prepare` stages what must precede the rows into its schema
    ///    (index declarations), and the shell is read back from it;
    /// 3. the new table opened, and `fill` writes every version into it
    ///    (each routed by its own point), all made durable;
    /// 4. **the commit point**: the catalogue row switched from the old uid
    ///    to the new one in one transaction (`swap_catalogue_uid`);
    /// 5. the new table installed, the old one's map log closed with
    ///    `DROPPED` and its files deleted after voiding their log records.
    ///
    /// A failure before 4 deletes the new uid's files and reopens the old
    /// table, unchanged; a crash before 4 leaves the new uid with no
    /// catalogue row and one after it the old, and the startup reaper
    /// deletes whichever is unnamed. A failure deleting the old files after
    /// 4 is logged, the reaper's to finish. Returns `fill`'s answer.
    pub(crate) async fn rewrite_partitioned<P, R, RF, PR, PF, F, FF>(
        &self,
        name: &str,
        read: R,
        prepare: PR,
        fill: F,
    ) -> Result<bool, String>
    where
        R: FnOnce(Arc<Table<DbStorageProvider>>, Vec<Arc<Table<DbStorageProvider>>>) -> RF,
        RF: std::future::Future<Output = Result<(CreateTable, P), String>>,
        PR: FnOnce(&P, Arc<DbStorageProvider>, Vec<crate::core::row::schema::SchemaField>) -> PF,
        PF: std::future::Future<Output = Result<(), String>>,
        F: FnOnce(P, Arc<PartitionedTable>) -> FF,
        FF: std::future::Future<Output = Result<bool, String>>,
    {
        let resolved = self.resolve_key(name);
        let name = resolved.as_deref().unwrap_or(name);
        let _ddl = self.partition_ddl.lock().await;
        let Some(old) = self.partitioned_table(name) else {
            return Err(format!("partitioned table \"{name}\" does not exist"));
        };
        old.check().map_err(|e| e.to_string())?;
        let key = old.key_columns().map_err(|e| e.to_string())?;
        let partitions = old.declared().ok_or("the partition count is unknown")?;
        let root = self.partition_root().map_err(|e| e.to_string())?;
        let _snapshot_gate = self.uid_deletion_permit().await;
        let _forget_pause = self.transactions.forget_pause();
        let changed = super::SchemaChanged(&self.schema_generation);
        self.schema_generation.fetch_add(1, std::sync::atomic::Ordering::AcqRel);

        // 1. Every materialized partition, then the instance retired.
        let parts: Vec<Arc<Table<DbStorageProvider>>> = match old.all_partitions().await {
            Ok(parts) => parts.into_iter().map(|(_, t)| t).collect(),
            Err(e) => return Err(e.to_string()),
        };
        old.retire_partitions().await;
        let reopen_old = || async {
            let (shell, table) = self.reopen_partitioned(name, &old).await;
            self.swap_partitioned(name, Arc::new(table), shell);
        };
        let (mut definition, plan) = match read(old.shell().clone(), parts).await {
            Ok(read) => read,
            Err(e) => {
                reopen_old().await;
                return Err(e);
            }
        };

        // 2 and 3: the new uid, written and filled.
        let open = self.partitioned_tables_all();
        let new_uid = match files::new_uid(&root, |u| open.iter().any(|t| t.uid() == u)) {
            Ok(u) => u,
            Err(e) => {
                reopen_old().await;
                return Err(e.to_string());
            }
        };
        drop(open);
        let built: Result<(bool, Arc<PartitionedTable>, Arc<Table<DbStorageProvider>>), String> = async {
            let key_refs: Vec<&str> = key.iter().map(String::as_str).collect();
            let columns = std::mem::take(&mut definition.columns);
            let (io_schema, schema) = self
                .write_partitioned_files(&root, &definition, columns, &key_refs, partitions, new_uid)
                .await
                .map_err(|e| e.to_string())?;
            prepare(&plan, io_schema.clone(), schema.fields.to_vec()).await?;
            let schema = TableSchema::load(io_schema.clone()).await.map_err(|e| e.to_string())?;
            let mut shell = Table::partition_shell(io_schema, schema);
            shell.scope_cache_to_database(self.id);
            shell.enable_versioning(self.transactions.clone());
            let shell = Arc::new(shell);
            let table = Arc::new(PartitionedTable::open(self.open_ctx(), name, new_uid, shell.clone()).await);
            table.check().map_err(|e| e.to_string())?;
            let wrote_family = fill(plan, table.clone()).await?;
            #[cfg(test)]
            super::rewrite::point(self, "partitioned_before_commit")?;
            self.sync_durable().map_err(|e| e.to_string())?;
            Ok((wrote_family, table, shell))
        }
        .await;
        let (wrote_family, table, shell) = match built {
            Ok(built) => built,
            Err(e) => {
                error!("Rewriting partitioned table {name} failed before its commit point: {e}");
                if let Some(t) = self.partitioned_tables_all().into_iter().find(|t| t.uid() == new_uid) {
                    t.retire_partitions().await;
                }
                #[cfg(feature = "storage_pools")]
                self.storage.forget_partitions(new_uid);
                if let Err(d) = self.discard_uid_files(&root, new_uid) {
                    error!("... and the new files under uid {new_uid:016x} were kept for the startup reaper: {d}");
                }
                reopen_old().await;
                return Err(e);
            }
        };

        // 4. The commit point.
        if let Err(e) = self.swap_catalogue_uid(name, old.uid(), new_uid).await {
            error!("Rewriting partitioned table {name}: the catalogue could not be switched: {e}");
            table.retire_partitions().await;
            drop(table);
            #[cfg(feature = "storage_pools")]
            self.storage.forget_partitions(new_uid);
            if let Err(d) = self.discard_uid_files(&root, new_uid) {
                error!("... and the new files under uid {new_uid:016x} were kept for the startup reaper: {d}");
            }
            reopen_old().await;
            return Err(e.to_string());
        }
        #[cfg(test)]
        if super::rewrite::point(self, "partitioned_after_commit").is_err() {
            // A "crash" right after the commit point: nothing more runs.
            return Ok(wrote_family);
        }

        // 5. Installed; the old uid gone.
        self.swap_partitioned(name, table, Some(shell));
        drop(changed);
        let old_uid = old.uid();
        // The pool's per-location counts for the old uid, as a DROP's: left,
        // they would refuse a location drop for partitions that are gone.
        #[cfg(feature = "storage_pools")]
        self.storage.forget_partitions(old_uid);
        if let Err(e) = old.mark_dropped().await {
            warn!("Rewrote partitioned table {name}; closing the old map log failed, the startup reaper deletes it: {e}");
        } else if let Err(e) = self.discard_uid_files(&root, old_uid) {
            warn!("Rewrote partitioned table {name}; deleting the old files under uid {old_uid:016x} failed, the startup reaper deletes them: {e}");
        }
        info!("Rewrote partitioned table {name}: uid {old_uid:016x} -> {new_uid:016x}");
        Ok(wrote_family)
    }
}

/// Test-only: a raised declared-partitions cap for databases under a
/// directory, for the scale measurements (`lazy_resident_tests`).
#[cfg(test)]
pub(crate) mod cap_hooks {
    use std::path::{Path, PathBuf};

    static CAPS: parking_lot::Mutex<Vec<(PathBuf, u32)>> = parking_lot::Mutex::new(Vec::new());

    pub(crate) fn raise(dir: &Path, cap: u32) {
        CAPS.lock().push((dir.to_path_buf(), cap));
    }

    pub(super) fn cap_for(location: &Path) -> Option<u32> {
        CAPS.lock().iter().find(|(d, _)| location.starts_with(d)).map(|(_, c)| *c)
    }
}

/// Test-only: arms [`Database::forget_before_partitioned_open`].
#[cfg(test)]
pub(crate) mod startup_hooks {
    use std::path::{Path, PathBuf};

    static ARMED: parking_lot::Mutex<Vec<PathBuf>> = parking_lot::Mutex::new(Vec::new());

    /// The next open of the database at `dir` forgets as far as it can just
    /// before its partitioned tables open.
    pub(crate) fn forget_before_partitioned_open(dir: &Path) {
        ARMED.lock().push(dir.to_path_buf());
    }

    pub(super) fn take(location: &Path) -> bool {
        let mut armed = ARMED.lock();
        match armed.iter().position(|d| location.starts_with(d) || d.starts_with(location)) {
            Some(i) => {
                armed.remove(i);
                true
            }
            None => false,
        }
    }
}
