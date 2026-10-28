//! A DDL's writes to one table's files, all or nothing (slice B5,
//! `docs/persistent-indexes.md` §7 "As built (B5)").
//!
//! Two shapes, both run as one reload under the ACCESS EXCLUSIVE the caller
//! holds ([`Database::reload_with_instance`]):
//!
//! - [`Database::rewrite_table`]: an ALTER that rewrites every row (ADD
//!   COLUMN on a table with rows, DROP NOT NULL, a JSON shadow's type
//!   widening). The rows are read from the retired instance **after** the
//!   lock, every version the old files hold that some snapshot can still see,
//!   each with its own `xmin` and `xmax` (R5); the files are recreated under
//!   the new schema, the carried declarations written, the new instance
//!   opened and filled, and only then swapped in. Waiters stay queued on the
//!   name the whole time, and a lockless reader sees the old instance until
//!   the swap.
//! - [`Database::reload_atomic`]: a metadata ALTER whose several sidecar
//!   writes (ADD COLUMN on an empty table, DROP and RENAME COLUMN, SET NOT
//!   NULL, a NUMERIC modifier, RENAME TO's sequence defaults) run as the
//!   reload's work.
//!
//! Either way the table's files (all of them for a rewrite, the `_schema*`
//! files for a metadata change) are first copied aside to a backup under a
//! reserved name, durably, and an **intent** naming the table and its backup
//! is written beside the catalogue. Only then is anything changed. Success
//! makes the change durable, then removes the intent, then the backup. A
//! failure part-way puts the backup back. A crash part-way leaves the intent,
//! and the next open puts the backup back before any table is opened
//! ([`recover_interrupted_rewrites`]). So a crash or a failure leaves the
//! table as it was or as the statement left it, never a mix.
//!
//! The copy costs one pass over the files, the same order as the rewrite
//! itself, and for a metadata change only the schema files.

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};

use log::{error, info, warn};
use rclite::Arc;

use super::reload::ReloadFailure;
use super::{
    Database, KvStores, RENAMED_SUFFIXES, SeedCache, StoragePool, copy_files_between, discard_files_in,
    seed_in,
};
use crate::core::row::table::Table;
use crate::core::rql::lexer_ct::CreateTable;
use crate::core::storage_providers::backend::DbStorageProvider;
use crate::core::storage_providers::traits::StorageIO;

type DbTable = Table<DbStorageProvider>;

/// Table names beginning with this are reserved: a table's backup during a
/// rewrite is `__rwb_<table>`, and a user table of that name would share its
/// files.
pub(crate) const BACKUP_PREFIX: &str = "__rwb_";

/// The backup's name for `table`.
pub(crate) fn backup_name(table: &str) -> String {
    format!("{BACKUP_PREFIX}{table}")
}

/// Which of a table's files a change touches, and so which are held aside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileSet {
    /// Every file: a rewrite recreates them all.
    Whole,
    /// The schema file and its sidecars: a metadata change writes nothing
    /// else.
    Schema,
}

impl FileSet {
    fn suffixes(self) -> Vec<&'static str> {
        match self {
            FileSet::Whole => RENAMED_SUFFIXES.to_vec(),
            FileSet::Schema => RENAMED_SUFFIXES
                .iter()
                .copied()
                .filter(|s| s.starts_with("_schema"))
                .collect(),
        }
    }

    fn tag(self) -> &'static str {
        match self {
            FileSet::Whole => "whole",
            FileSet::Schema => "schema",
        }
    }

    fn from_tag(tag: &str) -> Option<FileSet> {
        match tag {
            "whole" => Some(FileSet::Whole),
            "schema" => Some(FileSet::Schema),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// The intent
// ---------------------------------------------------------------------------

const INTENT_PREFIX: &str = "__rewrite_";
const INTENT_EXTENSION: &str = "intent";

/// One intent file per table, in the database's own directory, named by a
/// hash of the table's name (a name may hold anything a file name may not).
fn intent_path(root: &str, table: &str) -> PathBuf {
    Path::new(root).join(format!(
        "{INTENT_PREFIX}{:016x}.{INTENT_EXTENSION}",
        crate::core::wal::file_id_for_name(table)
    ))
}

/// The intent's bytes: the file set, then each table and its backup, then a
/// checksum over all of it, each line terminated, so a record that is not
/// exactly one intent does not parse.
fn encode_intent(entries: &[(String, String)], set: FileSet) -> String {
    let mut body = set.tag().to_string();
    for (table, backup) in entries {
        body.push_str(&format!("\n{table}\t{backup}"));
    }
    format!("{body}\n{:016x}\n", crate::core::wal::file_id_for_name(&body))
}

/// The tables, their backups and the file set an intent names; `None` for
/// anything not exactly one intent.
fn parse_intent(text: &str) -> Option<(Vec<(String, String)>, FileSet)> {
    let text = text.strip_suffix('\n')?;
    let (body, sum) = text.rsplit_once('\n')?;
    if sum != format!("{:016x}", crate::core::wal::file_id_for_name(body)) {
        return None;
    }
    let mut lines = body.split('\n');
    let set = FileSet::from_tag(lines.next()?)?;
    let mut entries = Vec::new();
    for line in lines {
        let (table, backup) = line.split_once('\t')?;
        if table.is_empty() || backup != backup_name(table) {
            return None;
        }
        entries.push((table.to_string(), backup.to_string()));
    }
    if entries.is_empty() {
        return None;
    }
    Some((entries, set))
}

/// Writes the intent durably: a temporary file, synced, renamed over the
/// intent, and the directory synced.
pub(super) fn write_intent(path: &Path, text: &str) -> io::Result<()> {
    use std::io::Write;
    let temp = path.with_extension("partial");
    {
        #[cfg(test)]
        crate::core::sync_audit::dir_op(crate::core::sync_audit::DirOp::Create(&temp));
        let mut file = std::fs::File::create(&temp)?;
        file.write_all(text.as_bytes())?;
        crate::core::sync_audit::note("rewrite.intent", path);
        file.sync_all()?;
    }
    #[cfg(test)]
    crate::core::sync_audit::dir_op(crate::core::sync_audit::DirOp::Rename { from: &temp, to: path });
    std::fs::rename(&temp, path)?;
    crate::core::wal::sync_parent_dir(path)
}

/// Removes the intent durably.
pub(super) fn remove_intent(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    crate::core::sync_audit::dir_op(crate::core::sync_audit::DirOp::Unlink(path));
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    }
    crate::core::wal::sync_parent_dir(path)
}

// ---------------------------------------------------------------------------
// Test points
// ---------------------------------------------------------------------------

/// The points of a rewrite a test can fail or cut power at: the power-loss
/// failpoint `test.rewrite.<label>`, armed for the database's directory.
/// A no-op in a server build and for an in-memory database.
pub(crate) fn point(database: &Database, label: &str) -> Result<(), String> {
    #[cfg(test)]
    if let Some(root) = database.location.as_deref() {
        let name = format!("test.rewrite.{label}");
        return crate::core::power_loss::failpoint::hit(&name, Path::new(root)).map_err(|e| e.to_string());
    }
    let _ = (database, label);
    Ok(())
}

// ---------------------------------------------------------------------------
// The backup
// ---------------------------------------------------------------------------

/// Tables' file sets held aside while a DDL changes them, under one intent.
/// Ended by [`Database::backup_commit`] or [`Database::backup_restore`].
pub(crate) struct Backup {
    /// Each table and its backup's name.
    entries: Vec<(String, String)>,
    set: FileSet,
    /// `None` for an in-memory database or a table on `memory:`: nothing
    /// survives a crash there, so there is nothing to recover.
    intent: Option<PathBuf>,
}

impl Database {
    /// The provider family `name`'s files resolve from.
    async fn seed_of(&self, name: &str) -> Arc<DbStorageProvider> {
        seed_in(&self.db_io_pointers, &self.storage, &self.kv, &self.seeds, name).await
    }

    /// Whether `table` lives somewhere a crash leaves: the database has a
    /// directory, and the table is not on `memory:`.
    fn survives_a_crash(&self, table: &str) -> bool {
        if self.location.is_none() {
            return false;
        }
        #[cfg(feature = "storage_pools")]
        if let Some(spec) = self.storage.spec_of(table) {
            return !matches!(
                crate::core::storage::TableBackend::parse(&spec),
                crate::core::storage::TableBackend::Memory
            );
        }
        let _ = table;
        true
    }

    /// Discards a backup's `suffixes`, and its placement once they are gone.
    async fn discard_backup(&self, backup: &str, suffixes: &[&str]) -> bool {
        let seed = self.seed_of(backup).await;
        let gone = discard_files_in(&seed, &self.storage, self.location.as_deref(), backup, suffixes).await;
        if gone {
            super::pool_forget(&self.storage, backup);
        }
        gone
    }

    /// Copies each of `tables`' `set` aside, durably, then records one intent
    /// naming them all: from here a crash or a failure puts every copy back,
    /// together. Nothing of the tables is changed. The caller holds ACCESS
    /// EXCLUSIVE on each and has retired the first's instance (the others'
    /// files in `set` are written by nothing but the statement), so nothing
    /// writes the files while they are read. The intent is filed under the
    /// first table, which no other statement can be changing.
    pub(crate) async fn backup_begin(&self, tables: &[String], set: FileSet) -> io::Result<Backup> {
        let suffixes = set.suffixes();
        let mut entries: Vec<(String, String)> = Vec::new();
        for table in tables {
            if entries.iter().any(|(t, _)| t == table) {
                continue;
            }
            let backup = backup_name(table);
            // The backup sits where the table is (the same backend), so the
            // copy and its restore never cross a device.
            super::pool_rename(&self.storage, table, &backup);
            // A backup left by a crash before its intent was written, or kept
            // by a failed discard: it belongs to nobody.
            let backup_seed = self.seed_of(&backup).await;
            let stale_kept = any_file_exists(&backup_seed, &backup)
                && !discard_files_in(
                    &backup_seed,
                    &self.storage,
                    self.location.as_deref(),
                    &backup,
                    RENAMED_SUFFIXES,
                )
                .await;
            let copied = if stale_kept {
                Err(io::Error::other(format!("a stale backup of \"{table}\" could not be removed")))
            } else {
                let table_seed = self.seed_of(table).await;
                copy_files_between(&table_seed, &backup_seed, table, &backup, &suffixes).await
            };
            entries.push((table.clone(), backup));
            if let Err(e) = copied {
                for (_, backup) in &entries {
                    self.discard_backup(backup, &suffixes).await;
                }
                return Err(e);
            }
        }
        let intent = match (self.location.as_deref(), entries.first()) {
            (Some(root), Some((first, _))) if entries.iter().any(|(t, _)| self.survives_a_crash(t)) => {
                let path = intent_path(root, first);
                if let Err(e) = write_intent(&path, &encode_intent(&entries, set)) {
                    let _ = remove_intent(&path);
                    for (_, backup) in &entries {
                        self.discard_backup(backup, &suffixes).await;
                    }
                    return Err(e);
                }
                Some(path)
            }
            _ => None,
        };
        Ok(Backup { entries, set, intent })
    }

    /// The change is durable (the caller synced it): the intent goes, then
    /// the backups. A backup that cannot be removed now is removed by the
    /// table's next backup; the intent is what recovery acts on.
    pub(crate) async fn backup_commit(&self, b: Backup) -> io::Result<()> {
        if let Some(path) = &b.intent {
            remove_intent(path)?;
        }
        let suffixes = b.set.suffixes();
        for (_, backup) in &b.entries {
            self.discard_backup(backup, &suffixes).await;
        }
        Ok(())
    }

    /// Puts every backup back over its table's `set` and ends the backup.
    /// The instance over a changed table's files must be retired first. On a
    /// failure the intent stays, so the next open finishes the restore.
    pub(crate) async fn backup_restore(&self, b: Backup) -> io::Result<()> {
        for (table, backup) in &b.entries {
            restore_from_backup(
                &self.db_io_pointers,
                &self.storage,
                &self.kv,
                &self.seeds,
                self.location.as_deref(),
                table,
                backup,
                b.set,
            )
            .await?;
        }
        if let Some(path) = &b.intent {
            remove_intent(path)?;
        }
        let suffixes = b.set.suffixes();
        for (_, backup) in &b.entries {
            self.discard_backup(backup, &suffixes).await;
        }
        Ok(())
    }

    /// Creates `create`'s files where its table is placed (the catalogue row
    /// and the placement already exist, and the old files are gone), with
    /// every declaration `CREATE TABLE` writes, and returns the instance that
    /// wrote them. That instance is for the caller's further declarations
    /// only: it is retired and dropped, and the table opened again, before
    /// any row goes in.
    async fn create_files(&self, create: CreateTable) -> io::Result<Arc<DbTable>> {
        let mut create = create;
        let columns = std::mem::take(&mut create.columns);
        let seed = self.seed_of(&create.table_name).await;
        let mut table = Table::new_with_constraints(&create.table_name, seed, columns, &create.unique_columns).await;
        table.scope_cache_to_database(self.id);
        super::write_schema_sidecars(table.io_schema.clone(), &mut table.schema, &create).await?;
        Ok(Arc::new(table))
    }

    /// An ALTER that rewrites every row of `table_name`, as one reload under
    /// the ACCESS EXCLUSIVE the caller holds (see the module comment).
    ///
    /// - `read` gets the retired old instance and returns the new table's
    ///   definition and whatever the fill needs (the versions it read);
    /// - `prepare` gets that and the instance that created the new files, to
    ///   write declarations that need the new schema's numbering (index
    ///   declarations);
    /// - `fill` gets the new instance, opened over the new files, and writes
    ///   every version into it.
    ///
    /// None of them may take a table lock. A failure anywhere puts the old
    /// files back and swaps a fresh instance of them in.
    pub(crate) async fn rewrite_table<P, T, R, RFut, Pr, PrFut, Fi, FiFut>(
        &self,
        table_name: &str,
        read: R,
        prepare: Pr,
        fill: Fi,
    ) -> Result<T, ReloadFailure>
    where
        R: FnOnce(Arc<DbTable>) -> RFut,
        RFut: Future<Output = Result<(CreateTable, P), String>>,
        Pr: FnOnce(&P, Arc<DbTable>) -> PrFut,
        PrFut: Future<Output = Result<(), String>>,
        Fi: FnOnce(P, Arc<DbTable>) -> FiFut,
        FiFut: Future<Output = Result<T, String>>,
    {
        // A partitioned table is never rewritten here: per partition it would
        // not be all or nothing (docs/partitioning.md §11, phase 6). Its
        // callers refuse first; this is the backstop, before anything is held.
        if self.is_partitioned(table_name) {
            return Err(ReloadFailure::Reopen(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("rewriting the rows of partitioned table \"{table_name}\" is not supported yet"),
            )));
        }
        let _relocating = super::relocation_permit(self.location.is_some(), table_name).await;
        // The new files are written, and on failure the old ones put back,
        // through the database's seed under the table's own names while the
        // old file set's word is live: a listed exemption (`gated_io`,
        // `Ungated::SchemaRewrite`), under the reload's ticket on that word.
        use crate::core::index_store::gated_io::{Ungated, ungated};
        self.reload_plain_with_instance(table_name, |old| ungated(Ungated::SchemaRewrite, async move {
            // After the lock and the retirement: every committed write is in
            // the files, and nothing writes them now.
            let (create, plan) = read(old).await?;
            if create.table_name != table_name {
                return Err(format!(
                    "internal error: a rewrite of \"{table_name}\" defined \"{}\"",
                    create.table_name
                ));
            }
            let backup = self
                .backup_begin(&[table_name.to_string()], FileSet::Whole)
                .await
                .map_err(|e| format!("could not set \"{table_name}\" aside for the rewrite: {e}"))?;
            point(self, "after_backup")?;

            let mut opened: Option<Arc<DbTable>> = None;
            let changed: Result<T, String> = async {
                let seed = self.seed_of(table_name).await;
                if !discard_files_in(&seed, &self.storage, self.location.as_deref(), table_name, RENAMED_SUFFIXES)
                    .await
                {
                    return Err(format!("the old files of \"{table_name}\" could not be released"));
                }
                point(self, "after_discard")?;
                let created = self
                    .create_files(create)
                    .await
                    .map_err(|e| format!("could not recreate \"{table_name}\": {e}"))?;
                let prepared = prepare(&plan, created.clone()).await;
                created.retire().await;
                drop(created);
                prepared?;
                point(self, "after_create")?;
                let table = self
                    .open_instance(table_name)
                    .await
                    .map_err(|e| format!("could not open the rewritten \"{table_name}\": {e}"))?;
                opened = Some(table.clone());
                let out = fill(plan, table).await?;
                point(self, "after_fill")?;
                self.sync_durable()
                    .map_err(|e| format!("the rewrite of \"{table_name}\" could not be made durable: {e}"))?;
                point(self, "after_sync")?;
                Ok(out)
            }
            .await;

            match changed {
                Ok(out) => {
                    if let Err(e) = self.backup_commit(backup).await {
                        // The rewrite is durable and its intent may still
                        // be there: the next open would put the old files
                        // back. Fail the statement rather than report a
                        // change a restart can undo.
                        if let Some(table) = opened.take() {
                            table.retire().await;
                        }
                        return Err(format!(
                            "the rewrite of \"{table_name}\" finished but its intent could not be removed: {e}"
                        ));
                    }
                    point(self, "after_commit")?;
                    Ok((out, opened))
                }
                Err(e) => {
                    if let Some(table) = opened.take() {
                        table.retire().await;
                    }
                    if let Err(restore) = self.backup_restore(backup).await {
                        error!(
                            "The rewrite of {} failed ({}) and its old files could not be put back yet ({}); \
                             the next open restores them",
                            table_name, e, restore
                        );
                    }
                    Err(e)
                }
            }
        }))
        .await
    }

    /// A metadata ALTER's writes to `table_name`'s schema files as one
    /// reload's work, all or nothing (see the module comment): `work` gets the
    /// retired instance, whose `io_schema` it writes through, as
    /// [`Database::reload_with`]'s does. A failure puts the schema files back.
    pub(crate) async fn reload_atomic<T, F, Fut>(&self, table_name: &str, work: F) -> Result<T, ReloadFailure>
    where
        F: FnOnce(Arc<DbTable>) -> Fut,
        Fut: Future<Output = Result<T, String>>,
    {
        self.reload_atomic_with(table_name, Vec::new(), work).await
    }

    /// [`Database::reload_atomic`] whose work also writes the schema files of
    /// `also` (the FK children a column's declarations reach, which the
    /// statement holds ACCESS EXCLUSIVE on): their files are held aside under
    /// the same intent, so a crash or a failure puts all of them back, never
    /// some. The caller reloads them after a success.
    pub(crate) async fn reload_atomic_with<T, F, Fut>(
        &self,
        table_name: &str,
        also: Vec<String>,
        work: F,
    ) -> Result<T, ReloadFailure>
    where
        F: FnOnce(Arc<DbTable>) -> Fut,
        Fut: Future<Output = Result<T, String>>,
    {
        let _relocating = super::relocation_permit(self.location.is_some(), table_name).await;
        // A partitioned table's schema files are its shell's,
        // `__pt_<uid>_schema*`: held aside under that stem (§11).
        let mut tables = vec![self.schema_files_stem(table_name)];
        // A partitioned child's too (phase 3 P4): its schema files are its
        // shell's, under `__pt_<uid>`, never its logical name.
        tables.extend(also.iter().map(|child| self.schema_files_stem(child)));
        self.reload_with_instance(table_name, |old| async move {
            let backup = self
                .backup_begin(&tables, FileSet::Schema)
                .await
                .map_err(|e| format!("could not set \"{table_name}\"'s schema aside: {e}"))?;
            let worked = async {
                let out = work(old).await?;
                point(self, "metadata_after_work")?;
                self.sync_durable()
                    .map_err(|e| format!("the change to \"{table_name}\" could not be made durable: {e}"))?;
                Ok::<T, String>(out)
            }
            .await;
            match worked {
                Ok(out) => {
                    self.backup_commit(backup).await.map_err(|e| {
                        format!("the change to \"{table_name}\" finished but its intent could not be removed: {e}")
                    })?;
                    Ok((out, None))
                }
                Err(e) => {
                    // The schema backup goes back over the live names
                    // through the database's seed (`Ungated::SchemaRewrite`).
                    let restored = crate::core::index_store::gated_io::ungated(
                        crate::core::index_store::gated_io::Ungated::SchemaRewrite,
                        self.backup_restore(backup),
                    )
                    .await;
                    if let Err(restore) = restored {
                        error!(
                            "A change to {} failed ({}) and its schema could not be put back yet ({}); \
                             the next open restores it",
                            table_name, e, restore
                        );
                    }
                    Err(e)
                }
            }
        })
        .await
    }
}

/// Whether any file of `name` may exist where `seed` keeps files: checked on
/// disk for a directory, assumed for another backend. A stale backup is rare,
/// and voiding the records of files that are not there costs a log sync per
/// log.
fn any_file_exists(seed: &DbStorageProvider, name: &str) -> bool {
    let Some(dir) = seed.get_location() else {
        return true;
    };
    RENAMED_SUFFIXES
        .iter()
        .any(|suffix| Path::new(&dir).join(format!("{name}{suffix}")).exists())
}

/// Discards `table`'s `set` and copies the backup's over it.
#[allow(clippy::too_many_arguments)]
async fn restore_from_backup(
    io_pointers: &Arc<DbStorageProvider>,
    pool: &StoragePool,
    kv: &KvStores,
    seeds: &SeedCache,
    own: Option<&str>,
    table: &str,
    backup: &str,
    set: FileSet,
) -> io::Result<()> {
    let suffixes = set.suffixes();
    let table_seed = seed_in(io_pointers, pool, kv, seeds, table).await;
    if !discard_files_in(&table_seed, pool, own, table, &suffixes).await {
        return Err(io::Error::other(format!(
            "the changed files of \"{table}\" could not be released"
        )));
    }
    let backup_seed = seed_in(io_pointers, pool, kv, seeds, backup).await;
    copy_files_between(&backup_seed, &table_seed, backup, table, &suffixes).await
}

/// Puts back every table a crash left mid-change: for each intent in the
/// database's directory, the backup's files over the table's, then the
/// intent removed and the backup discarded. Called at open, after the logs
/// are replayed and before any table is opened. A restore that fails leaves
/// its intent for the next open, and the table opens as the change left it,
/// which is logged.
pub(super) async fn recover_interrupted_rewrites(
    io_pointers: &Arc<DbStorageProvider>,
    pool: &StoragePool,
    kv: &KvStores,
    seeds: &SeedCache,
) {
    let Some(root) = io_pointers.get_location() else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(&root) else {
        return;
    };
    let ours: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(INTENT_PREFIX))
        })
        .collect();
    // An intent whose write a crash cut before its rename: never in place, so
    // nothing had been changed under it.
    for partial in ours.iter().filter(|p| p.extension().is_some_and(|x| x == "partial")) {
        #[cfg(test)]
        crate::core::sync_audit::dir_op(crate::core::sync_audit::DirOp::Unlink(partial));
        if std::fs::remove_file(partial).is_ok() {
            let _ = crate::core::wal::sync_parent_dir(partial);
        }
    }
    let mut intents: Vec<PathBuf> = ours
        .into_iter()
        .filter(|p| p.extension().is_some_and(|x| x == INTENT_EXTENSION))
        .collect();
    intents.sort();
    for path in intents {
        let Ok(text) = std::fs::read_to_string(&path) else {
            warn!("A rewrite intent {} could not be read; leaving it in place", path.display());
            continue;
        };
        let Some((entries, set)) = parse_intent(&text) else {
            // Written whole by a rename over it, so never torn: one that does
            // not parse was not written by this engine.
            warn!("Malformed rewrite intent {}: {:?}; leaving it in place", path.display(), text);
            continue;
        };
        let mut restored = true;
        for (table, backup) in &entries {
            info!(
                "Restoring {} from {} ({} files): a change to it was interrupted",
                table,
                backup,
                set.tag()
            );
            if let Err(e) = restore_from_backup(io_pointers, pool, kv, seeds, Some(&root), table, backup, set).await {
                error!(
                    "Could not restore {} after an interrupted change: {}; it opens as the change left it, \
                     and the next open tries again",
                    table, e
                );
                restored = false;
                break;
            }
        }
        if !restored {
            continue;
        }
        if let Err(e) = remove_intent(&path) {
            error!("Restored {:?} but could not remove the intent: {}", entries, e);
            continue;
        }
        for (_, backup) in &entries {
            let backup_seed = seed_in(io_pointers, pool, kv, seeds, backup).await;
            if discard_files_in(&backup_seed, pool, Some(&root), backup, RENAMED_SUFFIXES).await {
                super::pool_forget(pool, backup);
            }
        }
    }
}

#[cfg(test)]
mod intent_tests {
    use super::*;

    #[test]
    fn an_intent_round_trips_and_a_torn_one_does_not_parse() {
        let entries = vec![("t".to_string(), backup_name("t")), ("c".to_string(), backup_name("c"))];
        let text = encode_intent(&entries, FileSet::Schema);
        assert_eq!(parse_intent(&text), Some((entries.clone(), FileSet::Schema)));
        for cut in 0..text.len() {
            assert_eq!(parse_intent(&text[..cut]), None, "a cut at {cut} parsed");
        }
        let other = encode_intent(&[("t".to_string(), "__rwb_u".to_string())], FileSet::Whole);
        assert_eq!(parse_intent(&other), None, "an intent naming another table's backup parsed");
    }
}
