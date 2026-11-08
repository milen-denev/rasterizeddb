//! Conversion: a plain table becomes a partitioned one under its own name
//! (docs/partitioning.md §11, "Conversion"; `ALTER TABLE t PARTITION BY HASH
//! (…) [PARTITIONS n]`, or `CREATE INDEX … ON t USING partition (…)`).
//!
//! Offline, under the ACCESS EXCLUSIVE the statement holds, in the order a
//! partitioned rewrite (`rewrite_partitioned`) keeps:
//!
//! 1. the plain table taken out of the table map and retired, and `read`
//!    handed it: every version a snapshot can see, and the definition;
//! 2. the definition checked as a `CREATE … PARTITION BY` is (keys, foreign
//!    keys, the cap), then a new uid's files written from it;
//! 3. the new table opened, `prepare` staging what precedes the rows (index
//!    declarations), and `fill` writing every version into it, routed by its
//!    own point, all made durable;
//! 4. the conversion intent (`__convert.intent`: the name and the uid,
//!    checksummed) written whole;
//! 5. **the commit point**: the name's catalogue row switched from the plain
//!    row (uid 0) to the new uid, in one transaction (`swap_catalogue_uid`);
//! 6. the partitioned table installed, the plain table's files released as a
//!    DROP releases them after its catalogue tombstone (log records voided,
//!    then the files swept), and the intent cleared.
//!
//! A failure before 5 deletes the new uid's files and puts the plain table
//! back, unchanged. A crash before 5 leaves the plain row: the startup
//! reaper deletes the uid nothing names, and [`recover_interrupted_conversion`]
//! clears the intent. A crash after 5 leaves the partitioned row and the
//! plain files: recovery finishes 6 before any table opens. DDL is not
//! transactional here; no window is half applied.

use std::path::Path;

use log::{error, info, warn};
use rclite::Arc;

use super::super::{pool_forget, seed_in, sweep_files_durably, Database};
use crate::core::partition::table::PartitionedTable;
use crate::core::row::schema::TableSchema;
use crate::core::row::table::Table;
use crate::core::rql::lexer_ct::CreateTable;
use crate::core::storage_providers::backend::DbStorageProvider;
use crate::core::storage_providers::traits::StorageIO;

/// The intent's file, in the catalogue's directory.
const CONVERT_INTENT_FILE: &str = "__convert.intent";

/// What the intent names the uid as, its second line.
fn uid_line(uid: u64) -> String {
    format!("__pt_{uid:016x}")
}

fn intent_path(io_pointers: &Arc<DbStorageProvider>) -> Option<std::path::PathBuf> {
    io_pointers.get_location().map(|dir| Path::new(&dir).join(CONVERT_INTENT_FILE))
}

/// The table and uid an intent names, or `None` for anything that is not
/// exactly one whole, checksummed intent (`parse_intent`'s rule).
fn parse(text: &str) -> Option<(String, u64)> {
    let (name, uid) = super::super::parse_intent(text)?;
    let uid = uid.strip_prefix("__pt_").filter(|h| h.len() == 16)?;
    Some((name, u64::from_str_radix(uid, 16).ok()?))
}

fn remove_intent(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    crate::core::wal::sync_parent_dir(path)
}

impl Database {
    /// Converts plain table `name` to one partitioned by `key` into
    /// `partitions` partitions (module docs). Returns `fill`'s answer: whether
    /// the caller's transaction family wrote any version copied.
    pub(crate) async fn convert_to_partitioned<P, R, RF, PR, PF, F, FF>(
        &self,
        name: &str,
        key: &[String],
        partitions: u32,
        read: R,
        prepare: PR,
        fill: F,
    ) -> Result<bool, String>
    where
        R: FnOnce(Arc<Table<DbStorageProvider>>) -> RF,
        RF: std::future::Future<Output = Result<(CreateTable, P), String>>,
        PR: FnOnce(&P, Arc<DbStorageProvider>, Vec<crate::core::row::schema::SchemaField>) -> PF,
        PF: std::future::Future<Output = Result<(), String>>,
        F: FnOnce(P, Arc<PartitionedTable>) -> FF,
        FF: std::future::Future<Output = Result<bool, String>>,
    {
        let resolved = self.resolve_key(name);
        let name = resolved.as_deref().unwrap_or(name).to_string();
        let name = name.as_str();
        let _ddl = self.partition_ddl.lock().await;
        if self.is_partitioned(name) {
            return Err(format!("table \"{name}\" is already partitioned (not supported)"));
        }
        // Checked before the table leaves the map, as everything below that
        // depends only on the name and the database.
        let root = self.partition_root().map_err(|e| e.to_string())?;
        // Every file table records where it is; one anywhere but the root (a
        // pool location, a key/value or memory backend) would move.
        let root_key = crate::core::partition::files::dir_key(&root.to_string_lossy());
        if let Some(spec) = super::super::pool_spec_of(&self.storage, name)
            .filter(|spec| crate::core::partition::files::dir_key(spec) != root_key)
        {
            return Err(format!(
                "table \"{name}\" has a storage placement recorded ({spec}); a partitioned table lives in the \
                 database's root, so converting it would move it: move it there first (not supported)"
            ));
        }
        if self.held_names.lock().iter().any(|h| h == name) {
            return Err(format!(
                "\"{name}\" is part of a rename whose old files could not be released yet; restart before \
                 converting it"
            ));
        }
        let Some(table) = self.table_held(name) else {
            return Err(format!("relation \"{name}\" does not exist"));
        };
        if table.is_shell() {
            return Err(format!("table \"{name}\" is already partitioned (not supported)"));
        }
        let logs = table
            .io_rows()
            .map(|io| super::super::logs_holding(io, &self.storage, self.location.as_deref()))
            .map_err(|e| e.to_string())?;
        if super::super::table_for::lock_exempt(name) {
            return Err(format!("\"{name}\" is a statement's temporary relation and cannot be partitioned"));
        }
        drop(table);

        let _snapshot_gate = super::super::deletion_permit(&logs).await;
        let _uid_gate = self.uid_deletion_permit().await;
        let _forget_pause = self.transactions.forget_pause();
        let changed = super::super::SchemaChanged(&self.schema_generation);
        self.schema_generation.fetch_add(1, std::sync::atomic::Ordering::AcqRel);

        // 1. Out of the map, maintenance stopped, read.
        let Some(plain) = self.take_table(name) else {
            return Err(format!("relation \"{name}\" does not exist"));
        };
        plain.retire().await;
        let (mut definition, plan) = match read(plain.clone()).await {
            Ok(read) => read,
            Err(e) => {
                self.put_back_after_failed_drop(name, plain).await;
                return Err(e);
            }
        };

        // 2. Checked as a CREATE … PARTITION BY over this definition.
        let key_refs: Vec<&str> = key.iter().map(String::as_str).collect();
        if let Err(e) = self.partitioned_shape_refusal(&definition, &key_refs, partitions) {
            self.put_back_after_failed_drop(name, plain).await;
            return Err(e.to_string());
        }
        let open = self.partitioned_tables_all();
        let new_uid = match crate::core::partition::files::new_uid(&root, |u| open.iter().any(|t| t.uid() == u)) {
            Ok(u) => u,
            Err(e) => {
                self.put_back_after_failed_drop(name, plain).await;
                return Err(e.to_string());
            }
        };
        drop(open);

        // 2 and 3: written, filled, durable; then 4, the intent.
        let built: Result<(bool, Arc<PartitionedTable>, Arc<Table<DbStorageProvider>>), String> = async {
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
            super::super::rewrite::point(self, "convert_before_intent")?;
            self.sync_durable().map_err(|e| e.to_string())?;
            if let Some(path) = intent_path(&self.db_io_pointers) {
                super::super::rewrite::write_intent(&path, &super::super::encode_intent(name, &uid_line(new_uid)))
                    .map_err(|e| format!("the conversion intent could not be written: {e}"))?;
            }
            #[cfg(test)]
            super::super::rewrite::point(self, "convert_before_commit")?;
            Ok((wrote_family, table, shell))
        }
        .await;
        let (wrote_family, table, shell) = match built {
            Ok(built) => built,
            Err(e) => {
                error!("Converting {name} to a partitioned table failed before its commit point: {e}");
                self.abandon_conversion(&root, new_uid).await;
                self.put_back_after_failed_drop(name, plain).await;
                return Err(e);
            }
        };

        // 5. The commit point.
        if let Err(e) = self.swap_catalogue_uid(name, 0, new_uid).await {
            error!("Converting {name}: the catalogue could not be switched: {e}");
            table.retire_partitions().await;
            drop(table);
            self.abandon_conversion(&root, new_uid).await;
            self.put_back_after_failed_drop(name, plain).await;
            return Err(e.to_string());
        }
        #[cfg(test)]
        if super::super::rewrite::point(self, "convert_after_commit").is_err() {
            // A "crash" right after the commit point: nothing more runs.
            return Ok(wrote_family);
        }

        // 6. Installed; the plain files released; the intent cleared.
        self.install_partitioned(name.to_string(), table, Some(shell));
        drop(changed);
        if self.release_converted_plain(name, plain).await {
            self.clear_conversion_intent();
        }
        info!("Converted {name} to a partitioned table ({new_uid:016x}), {partitions} partitions");
        Ok(wrote_family)
    }

    /// A failed conversion's new uid: its partitions retired and its files
    /// deleted, or left for the startup reaper (no catalogue row names it);
    /// the intent, if written, cleared.
    async fn abandon_conversion(&self, root: &Path, uid: u64) {
        if let Some(t) = self.partitioned_tables_all().into_iter().find(|t| t.uid() == uid) {
            t.retire_partitions().await;
        }
        #[cfg(feature = "storage_pools")]
        self.storage.forget_partitions(uid);
        if let Err(d) = self.discard_uid_files(root, uid) {
            error!("... and the new files under uid {uid:016x} were kept for the startup reaper: {d}");
        }
        self.clear_conversion_intent();
    }

    /// Step 6's release of the plain table's files, past the commit point:
    /// what a DROP does after its catalogue tombstone. Its log records are
    /// voided first, so no later table of the name replays them; a void that
    /// fails is logged and the files go anyway, as a DROP past its barrier
    /// does, since the catalogue no longer names them. Whether every file is
    /// gone, which keeps the intent for the next open when not.
    async fn release_converted_plain(&self, name: &str, plain: Arc<Table<DbStorageProvider>>) -> bool {
        match plain.io_rows() {
            Ok(io) => {
                if let Err(failure) = self.forget_dropped_files(io, name) {
                    error!(
                        "Converted {name}, but could not void the plain table's log records: {}; its files are \
                         released anyway",
                        failure.error
                    );
                }
            }
            Err(e) => warn!("Converted {name}; the plain table had no row storage to release: {e}"),
        }
        if let Ok(data) = plain.data() {
            data.io_rows.drop_io();
            data.io_pointers.drop_io();
            data.ids.io.drop_io();
        }
        plain.io_schema.drop_io();
        drop(plain);
        let seed = seed_in(&self.db_io_pointers, &self.storage, &self.kv, &self.seeds, name).await;
        sweep_files_durably(&seed, &self.storage, self.location.as_deref(), name).await;
        pool_forget(&self.storage, name);
        self.forget_seed(name);
        let left = std::fs::read_dir(&self.partition_root().unwrap_or_default())
            .map(|dir| {
                dir.flatten()
                    .any(|e| super::super::table_owns_file(name, &e.file_name().to_string_lossy()))
            })
            .unwrap_or(false);
        if left {
            warn!("Converted {name}; some of the plain table's files remain, and the next open deletes them");
        }
        !left
    }

    fn clear_conversion_intent(&self) {
        if let Some(path) = intent_path(&self.db_io_pointers) {
            if let Err(e) = remove_intent(&path) {
                error!("Could not delete the conversion intent {}: {e}", path.display());
            }
        }
    }
}

/// Finishes or abandons a conversion a crash interrupted, before any table
/// opens. The catalogue decides: a row naming the intent's uid means the
/// commit happened, and the plain table's files are released (step 6); the
/// plain row still there means it did not, and the uid's files are the
/// startup reaper's. Either way the intent is cleared once settled. One that
/// does not parse whole is set aside and acted on not at all.
pub(in crate::core::database) async fn recover_interrupted_conversion(
    io_pointers: &Arc<DbStorageProvider>,
    io_rows: &Arc<DbStorageProvider>,
    io_schema: &Arc<DbStorageProvider>,
    pool: &super::super::StoragePool,
    kv: &super::super::KvStores,
    seeds: &super::super::SeedCache,
) {
    let Some(path) = intent_path(io_pointers) else { return };
    let partial = path.with_extension("partial");
    if partial.exists() && std::fs::remove_file(&partial).is_ok() {
        let _ = crate::core::wal::sync_parent_dir(&partial);
    }
    let text = match std::fs::read(&path) {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
        Err(e) => {
            warn!("A conversion intent {} exists but could not be read ({e}); leaving it in place", path.display());
            return;
        }
    };
    let Some((name, uid)) = parse(&text) else {
        let aside = path.with_file_name(format!("{CONVERT_INTENT_FILE}.bad"));
        let _ = std::fs::rename(&path, &aside).and_then(|()| crate::core::wal::sync_parent_dir(&aside));
        error!(
            "Conversion intent {} ({text:?}) does not parse as one whole, checksummed intent; nothing was done \
             with it, and it was moved aside to {}",
            path.display(),
            aside.display()
        );
        return;
    };
    let rows = super::super::catalogue_rows(io_pointers, io_rows, io_schema, &name).await;
    if rows.contains(&uid) {
        info!("Completing an interrupted conversion of {name}: the catalogue names uid {uid:016x}");
        let root = io_pointers.get_location();
        let seed = seed_in(io_pointers, pool, kv, seeds, &name).await;
        if !super::super::discard_files(&seed, pool, root.as_deref(), &name).await {
            warn!("The plain files of converted table {name} were kept; the next open tries again");
            return;
        }
        pool_forget(pool, &name);
    } else {
        info!("Abandoning an interrupted conversion of {name}: the catalogue still holds the plain table");
    }
    if let Err(e) = remove_intent(&path) {
        error!("Could not delete the conversion intent {}: {e}", path.display());
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn an_intent_is_read_back_only_whole() {
        let text = super::super::super::encode_intent("acct", &super::uid_line(0xab));
        assert_eq!(super::parse(&text), Some(("acct".to_string(), 0xab)));
        assert_eq!(super::parse(&text[..text.len() - 3]), None);
        assert_eq!(super::parse("acct\n__pt_00000000000000ab\n"), None);
    }
}
