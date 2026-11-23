//! A plain `CREATE TABLE`, all or nothing across a power cut: the table is
//! catalogued only once every one of its files is durable, and an intent
//! covers the files written before that.
//!
//! # The failure
//!
//! `add_table` used to write the catalogue row first, then the schema and
//! field files, then the constraint sidecars and the primary-key header in one
//! write-ahead transaction. The row and the field records are logged appends
//! (the file first, the log after), and the sidecars are held back until their
//! commit's sync. A cut inside that sync kept the row and the fields and lost
//! the sidecars: the table reopened with every column and no `PRIMARY KEY`,
//! `UNIQUE`, `CHECK` or `FOREIGN KEY`, enforcing none of them, and a retried
//! `CREATE TABLE IF NOT EXISTS` succeeded on it (7 images of 16 in the
//! reviewer's reproduction).
//!
//! # The order
//!
//! 1. the intent, written whole and synced (`rewrite::write_intent`);
//! 2. the placement and every file: schema, fields, sidecars, header;
//! 3. the log synced, so every one of them is durable;
//! 4. the catalogue row, the commit point, and the log synced again;
//! 5. the intent removed.
//!
//! The row comes after the files are durable, so an image holding any byte of
//! it, its page or its log record, holds every file whole (the log replays
//! them). An image without it holds the files or some of them, named by no
//! table, which a later `CREATE` of the name would refuse to take over
//! (`table_files_conflict`). The intent names them: at open, after the logs
//! replay and the catalogue is read ([`recover`]), an intent whose table is
//! catalogued is a create that reached its commit point and is removed; one
//! whose table is not has its files voided in the logs and deleted
//! (`discard_files_in`), the placement it chose forgotten (a pin recorded
//! before it, `STORAGE PLACE TABLE`, stays, so a retry lands where the pin
//! says), and then goes.
//!
//! So the removal in step 5 need not be synced: an intent left by a cut after
//! the row is durable names a catalogued table, which recovery keeps. The row
//! must be durable before the removal can reach the disk, which is why step 4
//! syncs.
//!
//! # Which tables
//!
//! Only a table a crash can leave files of: the database has a directory, the
//! table is not placed on `memory:`, it is not a statement-scoped relation
//! (always in memory, created by every CTE, and kept off the sync path on
//! purpose), and its directory is not being bulk-loaded (an import that dies
//! is discarded whole, and its `CREATE`s must not cost a sync each). A
//! partitioned table needs none: its files are named by a fresh uid, written
//! and synced before its catalogue row, and the startup reaper deletes a uid
//! no row names (`partitioned.rs`).

use std::io;
use std::path::{Path, PathBuf};

use log::{error, info, warn};
use rclite::Arc;

use super::{KvStores, RENAMED_SUFFIXES, SeedCache, StoragePool, discard_files_in, seed_in};
use crate::core::storage_providers::backend::DbStorageProvider;
use crate::core::storage_providers::traits::StorageIO;

const PREFIX: &str = "__create_";
const EXTENSION: &str = "intent";

/// One intent per table name, in the database's own directory, named by a
/// hash of the name (a name may hold anything a file name may not).
fn intent_path(root: &str, table: &str) -> PathBuf {
    Path::new(root).join(format!(
        "{PREFIX}{:016x}.{EXTENSION}",
        crate::core::wal::file_id_for_name(table)
    ))
}

/// The intent's bytes: a tag, whether the table's placement was there before
/// the `CREATE` (`pinned`) or is the one it chose (`placed`), the table's
/// name, then a checksum over all of it, so only exactly one intent parses.
/// The name may hold a line break: the checksum is the last line.
fn encode(table: &str, pinned: bool) -> String {
    let body = format!("create\n{}\n{table}", if pinned { PINNED } else { PLACED });
    format!("{body}\n{:016x}\n", crate::core::wal::file_id_for_name(&body))
}

const PINNED: &str = "pinned";
const PLACED: &str = "placed";

/// The table an intent names and whether its placement is a pin; `None` for
/// anything not exactly one intent. An intent with no placement line (this
/// branch's first form) keeps the placement, the side that loses nothing.
fn parse(text: &str) -> Option<(String, bool)> {
    let text = text.strip_suffix('\n')?;
    let (body, sum) = text.rsplit_once('\n')?;
    if sum != format!("{:016x}", crate::core::wal::file_id_for_name(body)) {
        return None;
    }
    let rest = body.strip_prefix("create\n")?;
    let (table, pinned) = match rest.split_once('\n') {
        Some((PINNED, table)) => (table, true),
        Some((PLACED, table)) => (table, false),
        _ => (rest, true),
    };
    (!table.is_empty()).then(|| (table.to_string(), pinned))
}

/// Writes `table`'s intent durably (step 1) and returns where it is.
/// `pinned`: a placement was recorded before the `CREATE` (`STORAGE PLACE
/// TABLE`), which a discard keeps.
pub(super) fn record(root: &str, table: &str, pinned: bool) -> io::Result<PathBuf> {
    let path = intent_path(root, table);
    super::rewrite::write_intent(&path, &encode(table, pinned))?;
    Ok(path)
}

/// Removes an intent (step 5), unsynced: see the module documentation.
pub(super) fn clear(path: &Path) {
    #[cfg(test)]
    crate::core::sync_audit::dir_op(crate::core::sync_audit::DirOp::Unlink(path));
    if let Err(e) = std::fs::remove_file(path)
        && e.kind() != io::ErrorKind::NotFound
    {
        // Harmless: the table it names is catalogued, which the next open sees.
        warn!("Could not remove the create intent {}: {}", path.display(), e);
    }
}

/// Discards the files of `table`, a create that never reached its catalogue
/// row: their log records voided, the files deleted, and, when
/// `forget_placement`, the placement the `CREATE` chose forgotten. A pin
/// recorded before it (`STORAGE PLACE TABLE`, applied before the table
/// exists, and the only way onto a key/value store) stays: forgotten, a
/// retried `CREATE` went to the database directory instead. False, keeping
/// the files, when that could not be done.
pub(super) async fn discard(
    io_pointers: &Arc<DbStorageProvider>,
    pool: &StoragePool,
    kv: &KvStores,
    seeds: &SeedCache,
    root: &str,
    table: &str,
    forget_placement: bool,
) -> bool {
    let seed = seed_in(io_pointers, pool, kv, seeds, table).await;
    if !discard_files_in(&seed, pool, Some(root), table, RENAMED_SUFFIXES).await {
        return false;
    }
    if forget_placement {
        super::pool_forget(pool, table);
    }
    true
}

/// At open, after the logs replay and the catalogue is read, before any table
/// opens: each create intent is resolved by the catalogue alone (see the
/// module documentation), skipping a name an interrupted rename still holds. One whose files cannot be discarded now stays for
/// the next open; its table is not catalogued, so nothing opens it meanwhile.
pub(super) async fn recover(
    io_pointers: &Arc<DbStorageProvider>,
    pool: &StoragePool,
    kv: &KvStores,
    seeds: &SeedCache,
    catalogued: &[(String, u64)],
    held: &[String],
) {
    let Some(root) = io_pointers.get_location() else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(&root) else {
        return;
    };
    let ours: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with(PREFIX)))
        .collect();
    for path in ours {
        // A write the cut stopped before its rename: never in place, so no
        // file of the table was created under it.
        if path.extension().is_some_and(|x| x == "partial") {
            #[cfg(test)]
            crate::core::sync_audit::dir_op(crate::core::sync_audit::DirOp::Unlink(&path));
            if std::fs::remove_file(&path).is_ok() {
                let _ = crate::core::wal::sync_parent_dir(&path);
            }
            continue;
        }
        if path.extension().is_none_or(|x| x != EXTENSION) {
            continue;
        }
        let Some((table, pinned)) = std::fs::read_to_string(&path).ok().as_deref().and_then(parse) else {
            // Written whole by a rename over it, so never torn: one that does
            // not parse was not written by this engine.
            warn!("A create intent {} could not be read; leaving it in place", path.display());
            continue;
        };
        if catalogued.iter().any(|(name, _)| *name == table) {
            clear(&path);
            continue;
        }
        // A rename interrupted under the name still holds files there until
        // this open finishes (`held_names`): only an intent left by an
        // unsynced removal can name one, after the table was created and then
        // renamed. Left for the next open, when the name is no longer held.
        if held.iter().any(|h| *h == table) {
            continue;
        }
        info!("Discarding the files of {table}: its CREATE TABLE was interrupted before its catalogue row");
        if !discard(io_pointers, pool, kv, seeds, &root, &table, !pinned).await {
            error!(
                "Could not discard the files of {table}, whose CREATE TABLE was interrupted; \
                 the next open tries again"
            );
            continue;
        }
        #[cfg(test)]
        crate::core::sync_audit::dir_op(crate::core::sync_audit::DirOp::Unlink(&path));
        if std::fs::remove_file(&path).is_ok() {
            let _ = crate::core::wal::sync_parent_dir(&path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_intent_round_trips_and_a_torn_one_does_not_parse() {
        for table in ["t", "a b", "line\nbreak", "pinned\nx", "外键"] {
            for pinned in [false, true] {
                let text = encode(table, pinned);
                assert_eq!(parse(&text), Some((table.to_string(), pinned)));
                for cut in 0..text.len() {
                    if text.is_char_boundary(cut) {
                        assert_eq!(parse(&text[..cut]), None, "{table:?} cut at {cut} parsed");
                    }
                }
            }
        }
        assert_eq!(parse(&encode("", false)), None);
        // The first form, no placement line: the placement is kept.
        let body = "create\nt";
        let old = format!("{body}\n{:016x}\n", crate::core::wal::file_id_for_name(body));
        assert_eq!(parse(&old), Some(("t".to_string(), true)));
    }
}
