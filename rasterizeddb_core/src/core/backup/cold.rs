//! Tables that are not in a directory, in a snapshot.
//!
//! A snapshot is a copy of directories, so a table on a key/value store used
//! to be in none of it: every snapshot logged a warning, `SHOW BACKUPS` looked
//! healthy, and a restore came back without the table. Now each one is copied
//! out of its store as a crash-consistent cut ([`kv::export`]) into a scratch
//! directory, and its files go into the snapshot under the names they would
//! have in its database's directory. A restore therefore brings it back as an
//! ordinary table there — never onto the store, which a clone sharing with
//! the live server would corrupt — and as of the moment it was copied, since
//! it has no log to replay further.
//!
//! What still cannot be copied — an in-memory table, a store this server
//! cannot reach — is recorded on the snapshot as uncovered, so the catalogue
//! says the backup is incomplete instead of a restore finding out.
//!
//! [`kv::export`]: crate::core::storage_providers::kv::export

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use log::warn;

use super::manifest::{ColdTable, UncoveredTable};
use crate::core::storage::{POOL_FILE, TableBackend};

/// Where cold tables are copied to while a snapshot is taken: inside the
/// backed-up directory, left out of every snapshot by name, emptied before
/// each use and removed after.
pub const COLD_DIR: &str = "backup_cold.tmp";

/// Cold tables copied for one snapshot, and the tables it cannot contain.
/// Dropping it removes the copies.
pub struct ColdCopy {
    /// `(archive name, path)`, as `snapshot_file_list` lists files.
    pub files: Vec<(String, PathBuf)>,
    pub tables: Vec<ColdTable>,
    pub uncovered: Vec<UncoveredTable>,
    /// When the copy was cut, if anything was copied.
    pub copied_at: Option<u64>,
    dir: PathBuf,
}

impl Drop for ColdCopy {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Every database under `location` with a placement: the root, and each
/// database `CREATE DATABASE` made beside it, as `(directory, relative name)`.
fn databases(location: &Path) -> Vec<(PathBuf, String)> {
    let mut out = Vec::new();
    if location.join(POOL_FILE).is_file() {
        out.push((location.to_path_buf(), String::new()));
    }
    if let Ok(entries) = std::fs::read_dir(location) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if path.is_dir() && name != COLD_DIR && path.join(POOL_FILE).is_file() {
                out.push((path, name));
            }
        }
    }
    out.sort_by(|a, b| a.1.cmp(&b.1));
    out
}

/// The `(table, spec)` placements in a database's pool file. Read here rather
/// than through `StorageManager`, whose open installs logs for locations.
fn placements(database: &Path) -> Vec<(String, String)> {
    let Ok(text) = std::fs::read_to_string(database.join(POOL_FILE)) else {
        return Vec::new();
    };
    text.lines()
        .filter_map(|line| match line.split('\t').collect::<Vec<_>>().as_slice() {
            ["table", spec, table] => Some((table.to_string(), spec.to_string())),
            _ => None,
        })
        .collect()
}

/// Copies every table under `location` that is not in a directory, or
/// records why it could not. `now` stamps the copy.
pub async fn copy_cold_tables(location: &Path, now: impl Fn() -> u64) -> ColdCopy {
    let dir = location.join(COLD_DIR);
    let _ = std::fs::remove_dir_all(&dir);
    let mut copy = ColdCopy {
        files: Vec::new(),
        tables: Vec::new(),
        uncovered: Vec::new(),
        copied_at: None,
        dir: dir.clone(),
    };

    for (database_dir, database) in databases(location) {
        // Stores, each with its tables.
        let mut on_store: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (table, spec) in placements(&database_dir) {
            match TableBackend::parse(&spec) {
                TableBackend::Directory(_) => {}
                TableBackend::KeyValue(store) => on_store.entry(store).or_default().push(table),
                TableBackend::Memory => copy.uncovered.push(UncoveredTable {
                    database: database.clone(),
                    table,
                    reason: "an in-memory table: it holds nothing that outlives the server".to_string(),
                }),
            }
        }
        for (store, tables) in on_store {
            let target = dir.join(&database);
            match copy_store(&database_dir, &store, &tables, &target).await {
                Ok(files) => {
                    copy.copied_at.get_or_insert_with(&now);
                    for table in &tables {
                        let mine: Vec<&(String, PathBuf, u64)> =
                            files.iter().filter(|(file, _, _)| crate::core::database::table_owns_file(table, file)).collect();
                        if mine.is_empty() {
                            copy.uncovered.push(UncoveredTable {
                                database: database.clone(),
                                table: table.clone(),
                                reason: format!("store '{store}' holds no files for it"),
                            });
                            continue;
                        }
                        copy.tables.push(ColdTable {
                            database: database.clone(),
                            table: table.clone(),
                            store: store.clone(),
                            bytes: mine.iter().map(|(_, _, bytes)| bytes).sum(),
                        });
                    }
                    for (file, path, _) in files {
                        let name = if database.is_empty() { file } else { format!("{database}/{file}") };
                        copy.files.push((name, path));
                    }
                }
                Err(e) => {
                    warn!("snapshot: could not copy the tables on store '{store}' of {}: {e}", database_dir.display());
                    for table in tables {
                        copy.uncovered.push(UncoveredTable {
                            database: database.clone(),
                            table,
                            reason: format!("store '{store}' could not be copied: {e}"),
                        });
                    }
                }
            }
        }
    }
    copy
}

#[cfg(feature = "kv_storage")]
async fn copy_store(
    database_dir: &Path,
    store: &str,
    tables: &[String],
    target: &Path,
) -> std::io::Result<Vec<(String, PathBuf, u64)>> {
    let stores = crate::core::storage::kv_registry::KvRegistry::open(&database_dir.to_string_lossy());
    let (client, config) = stores.connect(store)?;
    let owned = |file: &str| tables.iter().any(|table| crate::core::database::table_owns_file(table, file));
    let files = crate::core::storage_providers::kv::export::export(client, config, owned, target).await?;
    Ok(files.into_iter().map(|f| (f.name, f.path, f.bytes)).collect())
}

#[cfg(not(feature = "kv_storage"))]
async fn copy_store(
    _database_dir: &Path,
    store: &str,
    _tables: &[String],
    _target: &Path,
) -> std::io::Result<Vec<(String, PathBuf, u64)>> {
    Err(std::io::Error::other(format!(
        "this build has no key/value storage, so store '{store}' cannot be read"
    )))
}
