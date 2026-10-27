//! Schema changes on partitioned tables (`docs/partitioning.md` §11 and
//! "Phase 3 P1 as built"): what is refused and why, the reads a DDL arm
//! validates through, and `RENAME`.
//!
//! The changes themselves go through the plain table's arms: each writes its
//! sidecars as the work of [`Database::reload_with`] (or `reload_atomic`),
//! which for a partitioned name runs the partitioned reload
//! (`reload.rs`, `reload_partitioned`): the shell's sidecars written once,
//! then every open partition reopened under the one schema, all under ACCESS
//! EXCLUSIVE on the logical name. A partition has no schema file of its own,
//! so no two partitions can hold different schemas, in memory or on disk.
//!
//! What still needs every row rewritten (a populated `ADD COLUMN`, a type
//! change, `DROP NOT NULL` on a table with rows) is refused here before
//! anything is read or written: per partition a rewrite would not be all or
//! nothing (§11, a later version's generational reshape). So are the key's
//! own rules: a key column cannot be dropped, retyped or made nullable
//! (§1.5), a UNIQUE or PRIMARY KEY must contain every key column (§1b), and
//! foreign keys either way wait for their slice (§10). Every refusal reads
//! only the statement and the replicated schema, or the rows-file lengths a
//! plain table's ALTER already branches on (§12).

use std::io;
use std::sync::atomic::Ordering;

use log::{error, info, warn};
use rclite::Arc;

use super::Database;
use super::partitioned::unique_key_refusal;
use super::relation::Relation;
use super::rewrite::FileSet;
use crate::core::partition::files;
use crate::core::partition::table::PartitionedTable;
use crate::core::row::table::Table;
use crate::core::rql::lexer_alter::{AlterAction, AlterTable};
use crate::core::storage_providers::backend::DbStorageProvider;
use crate::core::storage_providers::traits::StorageIO;

type DbTable = Table<DbStorageProvider>;

/// The text every refusal here ends in when its own wording lacks "not
/// supported", so the wire reports 0A000 as `CREATE … PARTITION BY`'s do.
const SUFFIX: &str = "(not supported on a partitioned table)";

fn rewrite_refusal(what: &str, table: &str) -> String {
    format!(
        "{what} on partitioned table \"{table}\" rewrites every row, which is not supported yet: a later \
         version rewrites a partitioned table as a whole (physical ALTER)"
    )
}

impl Database {
    /// The stem `name`'s schema files hang off: the name itself for a plain
    /// table, `__pt_<uid>` for a partitioned one, whose schema is the shell's
    /// `__pt_<uid>_schema*` (§3.1). What a metadata change holds aside.
    pub(crate) fn schema_files_stem(&self, name: &str) -> String {
        match self.partitioned_table(name) {
            Some(table) => files::schema_stem(table.uid())
                .strip_suffix("_schema")
                .map(str::to_string)
                .unwrap_or_else(|| name.to_string()),
            None => name.to_string(),
        }
    }

    /// The bytes in `name`'s rows files: a plain table's `len`, a partitioned
    /// table's summed over its open partitions. What an ALTER decides
    /// "metadata-only" by. The caller holds ACCESS EXCLUSIVE on `name`.
    pub(crate) fn rows_len_held(&self, name: &str) -> io::Result<u64> {
        match self.relation(name) {
            Some(Relation::Partitioned(table)) => table.rows_len(),
            Some(Relation::Plain(table)) => Ok(table.data()?.len.load(Ordering::Relaxed)),
            None => Err(io::Error::new(io::ErrorKind::NotFound, format!("relation '{name}' does not exist"))),
        }
    }

    /// The tables holding `name`'s rows, for a DDL that validates them: a
    /// plain table itself, or each open partition of a partitioned one (an
    /// unmaterialized partition holds none, §4.3). The caller holds ACCESS
    /// EXCLUSIVE on `name`, so the set cannot change while it reads.
    pub(crate) fn row_tables_held(&self, name: &str) -> io::Result<Vec<Arc<DbTable>>> {
        match self.relation(name) {
            Some(Relation::Partitioned(table)) => {
                Ok(table.open_partitions()?.into_iter().map(|(_, partition)| partition).collect())
            }
            Some(Relation::Plain(table)) => {
                table.data()?;
                Ok(vec![table])
            }
            None => Err(io::Error::new(io::ErrorKind::NotFound, format!("relation '{name}' does not exist"))),
        }
    }

    /// Why `plan` cannot run on the partitioned table it names, or `None`
    /// (also for a plain table): asked by the ALTER arm before anything is
    /// read or written. Each reason carries "not supported", so 0A000.
    pub(crate) fn partitioned_alter_refusal(&self, plan: &AlterTable) -> Option<String> {
        let table = self.partitioned_table(&plan.table_name)?;
        // A table that cannot be opened: the arm's own read reports why.
        let key = table.key_columns().ok()?;
        let schema = &table.shell().schema;
        let name_of = |c: &str| {
            schema
                .fields
                .iter()
                .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(c))
                .cloned()
        };
        let is_key = |c: &str| key.iter().any(|k| k.eq_ignore_ascii_case(c));
        let t = plan.table_name.as_str();
        let populated = || table.rows_len().map_or(true, |len| len > 0);
        match &plan.action {
            AlterAction::AddColumn { name, json, .. } => {
                // A column already there is the arm's to answer (IF NOT
                // EXISTS, or "already exists"), as on a plain table.
                if name_of(name).is_some() {
                    return None;
                }
                if *json {
                    return Some(
                        "JSON columns stored as shadow tables are not supported on a partitioned table yet".to_string(),
                    );
                }
                populated().then(|| rewrite_refusal(&format!("ADD COLUMN {name}"), t))
            }
            AlterAction::DropColumn { name, .. } => (is_key(name) && name_of(name).is_some()).then(|| {
                format!(
                    "DROP COLUMN {name}: \"{name}\" is a partition key column of \"{t}\", and dropping one is not \
                     supported"
                )
            }),
            AlterAction::ConstraintClause { drop: false, description } => {
                let upper = description.to_ascii_uppercase();
                if upper.contains("FOREIGN KEY") || upper.contains("REFERENCES") {
                    return Some("FOREIGN KEY on a partitioned table is not supported yet".to_string());
                }
                // As the ALTER arm reads it: `IF NOT EXISTS` out, in front
                // or after `CONSTRAINT`.
                let (clause, _) = crate::core::rql::unique_key::strip_if_not_exists(description);
                match crate::core::rql::unique_key::parse_clause(&clause, t) {
                    Ok(Some(unique)) => unique_key_refusal(&key, &unique.columns).map(|why| format!("{why} {SUFFIX}")),
                    _ => None,
                }
            }
            AlterAction::AlterColumn { name, detail } => {
                let words: Vec<&str> = detail.split_whitespace().collect();
                let first = words.first().map(|w| w.to_ascii_uppercase()).unwrap_or_default();
                let field = name_of(name)?;
                if first == "TYPE" {
                    // `USING`, and a type that does not parse, are the arm's.
                    if words.iter().any(|w| w.eq_ignore_ascii_case("USING")) {
                        return None;
                    }
                    let (db_type, _) = crate::core::rql::lexer_alter::parse_column_type(&words[1..]).ok()?;
                    if db_type == field.db_type {
                        return None;
                    }
                    if is_key(name) {
                        return Some(format!(
                            "ALTER COLUMN {name} TYPE: \"{name}\" is a partition key column of \"{t}\", whose type \
                             decides where every row is stored; changing it is not supported"
                        ));
                    }
                    return Some(rewrite_refusal(&format!("ALTER COLUMN {name} TYPE"), t));
                }
                if first == "DROP" && detail.to_ascii_uppercase().contains("NOT NULL") {
                    if is_key(name) {
                        return Some(format!(
                            "DROP NOT NULL on partition key column \"{name}\" of \"{t}\" is not supported: a \
                             partition key column must be NOT NULL"
                        ));
                    }
                    let nullable = crate::core::rql::nullable::is_nullable(name, &schema.fields);
                    return (!nullable && populated()).then(|| rewrite_refusal(&format!("ALTER COLUMN {name} DROP NOT NULL"), t));
                }
                None
            }
            _ => None,
        }
    }

    /// `ALTER TABLE … RENAME TO` of a partitioned table (§3.1): no file is
    /// copied or moved, since every file is `__pt_<uid>_*`. In order:
    ///
    /// 1. the rename intent (uid, old, new), durable, renamed into place
    ///    whole (its own, not the plain rename's, whose recovery deletes
    ///    `<name>*` files);
    /// 2. a second catalogue row, `new` with the **same** uid: the commit
    ///    point;
    /// 3. the shell's schema header renamed, all or nothing under a schema
    ///    backup; a failure is logged and harmless, since the catalogue row
    ///    names the table (`load_shell` reads the header under the row's
    ///    name);
    /// 4. the old row tombstoned, made durable, then the intent cleared.
    ///
    /// A crash before 2 leaves the old row only, and the intent's recovery
    /// drops the intent; after 2, both rows, and recovery tombstones the old
    /// one before the catalogue is read ([`recover_interrupted_rename`]), so
    /// no start sees two rows with one uid. Recovery deletes no file. In
    /// memory the open partitions are retired first and reopened under the
    /// name the catalogue ended with.
    ///
    /// The caller (`rename_table`) holds ACCESS EXCLUSIVE on both names,
    /// has checked `new_name`, and paused forgetting.
    pub(super) async fn rename_partitioned(&self, old_name: &str, new_name: &str) -> io::Result<()> {
        let _ddl = self.partition_ddl.lock().await;
        let _new_claim = self.claim_name(new_name)?;
        let _old_claim = self.claim_name(old_name)?;
        let Some(old) = self.partitioned_table(old_name) else {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Table not found"));
        };
        old.check()?;
        // One partitioned rename's intent at a time: a second one would
        // overwrite the first's while its old row may still be live.
        if let Some(why) = self.unfinished_partitioned_rename(None) {
            return Err(io::Error::other(why));
        }
        if self.has_table(new_name) || self.is_partitioned(new_name) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "a table of that name already exists"));
        }
        self.refuse_held_name(new_name)?;
        if let Some(held) = self.held_names.lock().first() {
            return Err(io::Error::other(format!(
                "an earlier rename has not finished (the files of \"{held}\" are kept until the next open \
                 discards them); restart before renaming again"
            )));
        }
        if let Some(reason) = self.table_files_conflict(new_name) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, reason));
        }
        if let Some(spec) = super::pool_spec_of(&self.storage, new_name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("table \"{new_name}\" has a storage placement recorded ({spec}), so the name is taken"),
            ));
        }
        let uid = old.uid();

        // Step 1.
        let intent = self
            .location
            .as_deref()
            .map(|root| std::path::Path::new(root).join(RENAME_INTENT))
            .ok_or_else(|| io::Error::other("a partitioned table needs a database directory"))?;
        super::rewrite::write_intent(&intent, &encode_rename(uid, old_name, new_name))?;
        point(self, "rename_after_intent");
        // Nothing writes the partitions now (ACCESS EXCLUSIVE), and none
        // runs maintenance through the instances about to be replaced.
        old.retire_partitions().await;

        // Step 2: the commit point.
        if let Err(e) = self.write_catalogue_row(new_name, uid).await {
            error!("Rename of partitioned table {old_name} to {new_name}: catalogue insert failed: {e}");
            // Kept if it cannot go: the next open sees the old row alone and
            // drops it.
            if let Err(e) = super::rewrite::remove_intent(&intent) {
                error!("... and its intent could not be removed ({e}); the next open removes it");
            }
            self.reinstall_partitioned(old_name, &old, old_name).await;
            return Err(e);
        }
        let synced = self.sync_durable();
        point(self, "rename_after_commit");

        // Step 3, best effort: the row is what names the table.
        if let Err(e) = self.rewrite_partitioned_header(&old, new_name).await {
            warn!(
                "Renamed partitioned table {old_name} to {new_name}, but its schema header still records the old \
                 name ({e}); the catalogue row decides, so it opens as {new_name}"
            );
        }
        point(self, "rename_after_header");

        // Step 4. Durable before the intent goes: an intent cleared ahead of
        // a lost tombstone would leave two rows with one uid and nothing to
        // say which is newer, and the next start refuses that.
        match self.forget_in_catalogue(old_name).await {
            Ok(_) => match self.sync_durable() {
                Ok(()) => {
                    if let Err(e) = super::rewrite::remove_intent(&intent) {
                        error!(
                            "Renamed partitioned table {old_name} to {new_name}, but its intent could not be removed \
                             ({e}); the next open removes it"
                        );
                    }
                }
                Err(e) => error!(
                    "Renamed partitioned table {old_name} to {new_name}, but the old catalogue row's tombstone is \
                     not durable ({e}); the next open finishes the rename"
                ),
            },
            Err(e) => error!(
                "Renamed partitioned table {old_name} to {new_name}, but could not tombstone the old catalogue row \
                 ({e}); the next open finishes the rename"
            ),
        }
        // Unfinished (the intent is still there): both names are held until
        // the next open settles it. A CREATE of either name is refused
        // (`refuse_held_name`), and the intent itself refuses a second
        // partitioned RENAME, and a DROP of this uid, until then.
        if intent.exists() {
            self.hold_intent(old_name, new_name);
        }

        self.reinstall_partitioned(old_name, &old, new_name).await;
        synced?;
        info!("Renamed partitioned table {old_name} to {new_name} ({uid:016x})");
        Ok(())
    }

    /// Why a partitioned RENAME, or a DROP of the partitioned table `uid`
    /// (`Some`), must wait for the next open: a rename's intent is still in
    /// the database directory, so its old catalogue row may still be live.
    /// A second rename would overwrite the intent (and the next open would
    /// then find two rows with one uid, which it refuses); a DROP would delete
    /// the files the surviving row still names. An intent that does not parse
    /// is taken to name every uid. `None` when there is none.
    pub(super) fn unfinished_partitioned_rename(&self, uid: Option<u64>) -> Option<String> {
        let root = self.location.as_deref()?;
        let path = std::path::Path::new(root).join(RENAME_INTENT);
        let text = match std::fs::read(&path) {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return None,
            Err(e) => {
                return Some(format!(
                    "a partitioned rename's intent {} cannot be read ({e}), so it may name this table; a restart \
                     retries it, or check the catalogue holds each partitioned table once and move the file aside",
                    path.display()
                ));
            }
        };
        let named = parse_rename(&text);
        if let (Some(uid), Some((intent_uid, _, _))) = (uid, &named)
            && *intent_uid != uid
        {
            return None;
        }
        Some(match named {
            Some((_, old, new)) => format!(
                "the rename of partitioned table \"{old}\" to \"{new}\" has not finished (its intent {} is kept \
                 until the next open settles it); restart first",
                path.display()
            ),
            None => format!(
                "a partitioned rename's intent {} does not parse, so it may name this table; a restart moves it \
                 aside, or check the catalogue holds each partitioned table once and move the file aside",
                path.display()
            ),
        })
    }

    /// The in-memory half of a partitioned RENAME: `old` (retired) leaves
    /// `old_name`'s entries, and its table reopened from disk under `name`
    /// takes `name`'s. With `name == old_name` it is put back under the old
    /// name, fresh, after a failure.
    async fn reinstall_partitioned(&self, old_name: &str, old: &Arc<PartitionedTable>, name: &str) {
        // The map key the old entries are under (`resolve_key`), as every
        // mutator asks it before it takes a slot out.
        let resolved = self.resolve_key(old_name);
        let old_name = resolved.as_deref().unwrap_or(old_name);
        let (shell, table) = self.reopen_partitioned(name, old).await;
        if let Some(why) = table.broken() {
            error!("partitioned table {name} could not be reopened after its rename: {why}");
        }
        self.take_partitioned(old_name, old);
        self.install_partitioned(name.to_string(), Arc::new(table), shell);
    }

    /// §3.1's step 3: the shell's schema header names `new_name`, written
    /// in place and made durable under a backup of the schema files, so a
    /// failure or a crash leaves the old header or the new one, never a torn
    /// one.
    ///
    /// `backup_restore` asks that the instance over the changed files be
    /// retired first. Here that is the shell, which runs no maintenance and
    /// writes nothing on its own (a shell is retired from its construction:
    /// `Table::partition_shell`), and every partition, which the caller
    /// retired; under ACCESS EXCLUSIVE no statement writes through either.
    /// Retired again here so the precondition is stated, not inferred. The
    /// table is reopened afterwards through new providers (`load_shell`),
    /// which read the restored files.
    async fn rewrite_partitioned_header(&self, table: &PartitionedTable, new_name: &str) -> io::Result<()> {
        table.shell().retire().await;
        let stem = self.schema_files_stem(table.name());
        let backup = self.backup_begin(&[stem], FileSet::Schema).await?;
        let written: io::Result<()> = async {
            let io_schema = table.shell().io_schema.clone();
            let mut schema = table.shell().schema.clone();
            schema.name = new_name.to_string();
            io_schema.write_data(0, &schema.into_vec()).await;
            // A failure after the bytes went in, before they are durable.
            #[cfg(test)]
            if let Some(root) = self.location.as_deref() {
                crate::core::power_loss::failpoint::hit("test.pddl.rename_header_write", std::path::Path::new(root))?;
            }
            let token = io_schema.begin_txn();
            io_schema
                .commit_durable(token)
                .await
                .map_err(|e| io::Error::other(format!("{e:?}")))?;
            self.sync_durable()
        }
        .await;
        match written {
            Ok(()) => self.backup_commit(backup).await,
            Err(e) => {
                // Over the live names through the database's seed, as a
                // plain table's metadata change restores (`reload_atomic`).
                let restored = crate::core::index_store::gated_io::ungated(
                    crate::core::index_store::gated_io::Ungated::SchemaRewrite,
                    self.backup_restore(backup),
                )
                .await;
                if let Err(restore) = restored {
                    error!(
                        "The header rewrite of {} failed ({e}) and its schema could not be put back yet ({restore}); \
                         the next open restores it",
                        table.name()
                    );
                }
                Err(e)
            }
        }
    }
}

/// A partitioned RENAME's intent (§3.1 step 1), in the database directory.
/// One at a time: partitioned RENAMEs hold `partition_ddl`.
const RENAME_INTENT: &str = "__prename.intent";

/// The intent's bytes: the uid and both names, then a checksum over them,
/// each line terminated, so a record that is not exactly one intent does not
/// parse. Written whole, by a rename over the file (`rewrite::write_intent`).
fn encode_rename(uid: u64, old_name: &str, new_name: &str) -> String {
    let body = format!("{uid:016x}\n{old_name}\n{new_name}");
    format!("{body}\n{:016x}\n", crate::core::wal::file_id_for_name(&body))
}

fn parse_rename(text: &str) -> Option<(u64, String, String)> {
    let text = text.strip_suffix('\n')?;
    let (body, sum) = text.rsplit_once('\n')?;
    if sum != format!("{:016x}", crate::core::wal::file_id_for_name(body)) {
        return None;
    }
    let mut lines = body.split('\n');
    let uid = u64::from_str_radix(lines.next()?, 16).ok()?;
    let (old_name, new_name) = (lines.next()?, lines.next()?);
    if lines.next().is_some() || old_name.is_empty() || new_name.is_empty() {
        return None;
    }
    Some((uid, old_name.to_string(), new_name.to_string()))
}

/// Moves a malformed rename intent to the first free
/// `__prename.intent.bad-<n>`, durably, and logs it at error level; a move
/// that fails leaves it, logged too.
fn set_aside_malformed(path: &std::path::Path, text: &str) {
    let mut n = 1u32;
    let aside = loop {
        let candidate = path.with_file_name(format!("{RENAME_INTENT}.bad-{n}"));
        if !candidate.exists() {
            break candidate;
        }
        n += 1;
    };
    match std::fs::rename(path, &aside).and_then(|()| crate::core::wal::sync_parent_dir(&aside)) {
        Ok(()) => error!(
            "Malformed partitioned rename intent {} ({text:?}) moved aside to {}: it does not parse, so nothing \
             was done with it. Check the catalogue holds each partitioned table once, then remove the file.",
            path.display(),
            aside.display()
        ),
        Err(e) => error!(
            "Malformed partitioned rename intent {} ({text:?}) could not be moved aside ({e}); partitioned RENAME \
             and DROP stay refused until it is moved",
            path.display()
        ),
    }
}

/// Finishes or abandons a partitioned RENAME a crash interrupted, before the
/// catalogue is read for the table list: by the catalogue alone, touching no
/// table file (a partitioned table has none under either name). Both rows
/// with the uid: committed, so the old row is tombstoned, durably, before
/// the intent goes; anything else: not committed (or already finished), and
/// the intent goes. An intent that does not parse was not written by this
/// engine (it is renamed into place whole) and is left, logged.
pub(super) async fn recover_interrupted_rename(
    io_pointers: &Arc<DbStorageProvider>,
    io_rows: &Arc<DbStorageProvider>,
    io_schema: &Arc<DbStorageProvider>,
) {
    let Some(root) = io_pointers.get_location() else {
        return;
    };
    let path = std::path::Path::new(&root).join(RENAME_INTENT);
    // A write cut before its rename: never in place, so nothing followed it.
    let partial = path.with_extension("partial");
    if partial.exists() && std::fs::remove_file(&partial).is_ok() {
        let _ = crate::core::wal::sync_parent_dir(&partial);
    }
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return,
        Err(e) => {
            // Unreadable now is not malformed: the next open tries again,
            // and meanwhile RENAME and DROP say where it is.
            error!("A partitioned rename intent {} could not be read ({e}); leaving it in place", path.display());
            return;
        }
    };
    let text = String::from_utf8_lossy(&bytes);
    let Some((uid, old_name, new_name)) = parse_rename(&text) else {
        // Renamed into place whole, so never torn: one that does not parse
        // was not written by this engine, and says nothing to act on. Left
        // in place it would refuse every partitioned RENAME and DROP for
        // good, since nothing removes it, so it is moved aside, kept for an
        // operator.
        set_aside_malformed(&path, &text);
        return;
    };
    let schema = super::load_db_data_schema(io_schema.clone()).await;
    let Ok(mut iterator) = crate::core::row::row_pointer::RowPointerIterator::new(io_pointers.clone()).await else {
        warn!("The catalogue could not be read to finish a partitioned rename; leaving its intent in place");
        return;
    };
    let rows = super::load_db_tables(&schema, &mut iterator, io_rows.clone()).await;
    let has = |name: &str| rows.iter().any(|(n, c)| n == name && *c == uid);
    if has(&old_name) && has(&new_name) {
        info!("Completing an interrupted rename of partitioned table {old_name} to {new_name} ({uid:016x})");
        let tombstoned = super::tombstone_catalogue_row(io_pointers, io_rows, io_schema, &old_name).await;
        let durable = root.as_str();
        let synced = crate::core::wal::wal_for(durable).is_none_or(|wal| wal.sync_pending().is_ok());
        if !matches!(tombstoned, Ok(true)) || !synced {
            error!(
                "Could not finish the rename of partitioned table {old_name} to {new_name} ({tombstoned:?}); the \
                 intent stays for the next open"
            );
            return;
        }
    } else {
        info!(
            "An interrupted rename of partitioned table {old_name} to {new_name} ({uid:016x}) is settled: the \
             catalogue holds {}",
            if has(&new_name) { new_name.as_str() } else { old_name.as_str() }
        );
    }
    if let Err(e) = super::rewrite::remove_intent(&path) {
        error!("Could not remove the partitioned rename intent {}: {e}", path.display());
    }
}

/// A partitioned RENAME's test points: the power-loss failpoint
/// `test.pddl.<label>`, armed for the database's directory. A no-op in a
/// server build and for an in-memory database.
fn point(database: &Database, label: &str) {
    #[cfg(test)]
    if let Some(root) = database.location.as_deref() {
        let _ = crate::core::power_loss::failpoint::hit(&format!("test.pddl.{label}"), std::path::Path::new(root));
    }
    let _ = (database, label);
}
