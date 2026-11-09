//! DDL that rolls back with its block (2026-11-09, phase 1).
//!
//! PostgreSQL's DDL is transactional, and every migration runner leans on it:
//! a migration is one `BEGIN … COMMIT`, so one that fails part-way leaves the
//! schema as it found it. Here a block's `CREATE TABLE` or `ALTER TABLE` was
//! applied at once and stayed through a `ROLLBACK`, and the retry then failed
//! on "already exists" over half a migration (`ddl_in_transactions`).
//!
//! Now, before such a statement runs in a block, the tables it changes are
//! copied aside (every file, the rewrite machinery's own copy,
//! `rewrite::copy_files_between`) under a reserved name, and an **intent**
//! naming the block's transaction is written beside the catalogue. A table
//! the statement creates gets an intent of its own and no copy. Then:
//!
//! - **COMMIT** (once durable): intents removed, copies discarded.
//! - **ROLLBACK**, an error that ends the block, a dropped connection: in
//!   reverse order, each copy put back over its table (the instance retired
//!   and reopened, `Database::reload_with`) and each created table dropped.
//! - **ROLLBACK TO SAVEPOINT**: the same, for the steps registered since the
//!   savepoint was set (`Savepoint::ddl_steps` in pgwire). A step that cannot
//!   be undone stays registered and leaves the block failed, so it cannot
//!   commit over the change.
//! - **A crash** with intents left: at the next open, before any table opens,
//!   the intents of a transaction that did not commit are undone the same
//!   way, and those of one that did are discarded.
//!
//! The block keeps the ACCESS EXCLUSIVE such a statement took until it ends,
//! as PostgreSQL does, rather than handing it back at the statement's end
//! (`executor::BlockDdl`): a change that may yet be undone must not be read
//! or written over by anyone else.
//!
//! Phase 1 covers `CREATE TABLE` and `ALTER TABLE` on plain tables (and the
//! table an `ALTER … REFERENCES` names); a table that does not outlive the
//! process is copied in memory and has no crash intent. Other DDL in
//! a block, a partitioned table, and a table on a key/value store stay as
//! before: applied at once, with the warning that says so.

use std::io;
use std::path::{Path, PathBuf};

use log::{error, info, warn};

use super::{Database, RENAMED_SUFFIXES, copy_files_between, discard_files_in};
use crate::core::mvcc::TxId;
use crate::core::storage_providers::traits::StorageIO;

const INTENT_PREFIX: &str = "__txddl_";
const INTENT_EXTENSION: &str = "intent";
/// Copies of a block's tables are named `__txb_<tx>_<seq>_<table>`.
pub(crate) const COPY_PREFIX: &str = "__txb_";

/// One undo step of a block's DDL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Undo {
    /// Put `copy`'s files back over `table`'s.
    Restore { table: String, copy: String },
    /// Drop `table`: the block created it.
    Drop { table: String },
}

/// A registered step and its intent file (none in memory).
#[derive(Debug, Clone)]
pub(crate) struct Step {
    undo: Undo,
    intent: Option<PathBuf>,
}

fn intent_path(root: &str, tx: TxId, seq: u32) -> PathBuf {
    Path::new(root).join(format!("{INTENT_PREFIX}{tx:020}_{seq:06}.{INTENT_EXTENSION}"))
}

/// `tx`, then the step, then a checksum; anything else does not parse.
fn encode(tx: TxId, undo: &Undo) -> String {
    let body = match undo {
        Undo::Restore { table, copy } => format!("{tx}\nrestore\t{table}\t{copy}"),
        Undo::Drop { table } => format!("{tx}\ndrop\t{table}"),
    };
    format!("{body}\n{:016x}\n", crate::core::wal::file_id_for_name(&body))
}

fn decode(text: &str) -> Option<(TxId, Undo)> {
    let text = text.strip_suffix('\n')?;
    let (body, sum) = text.rsplit_once('\n')?;
    if sum != format!("{:016x}", crate::core::wal::file_id_for_name(body)) {
        return None;
    }
    let (tx, step) = body.split_once('\n')?;
    let tx: TxId = tx.parse().ok()?;
    let mut parts = step.split('\t');
    let undo = match (parts.next()?, parts.next(), parts.next(), parts.next()) {
        ("restore", Some(table), Some(copy), None) if copy.starts_with(COPY_PREFIX) && !table.is_empty() => {
            Undo::Restore { table: table.to_string(), copy: copy.to_string() }
        }
        ("drop", Some(table), None, None) if !table.is_empty() => Undo::Drop { table: table.to_string() },
        _ => return None,
    };
    Some((tx, undo))
}

/// The tables a block DDL statement changes, as `(existing, created)`, or
/// `None` for a statement phase 1 does not make transactional.
pub(crate) fn targets(database: &Database, sql: &str) -> Option<(Vec<String>, Vec<String>)> {
    let words: Vec<&str> = sql.split_whitespace().collect();
    let upper = |i: usize| words.get(i).map(|w| w.to_ascii_uppercase()).unwrap_or_default();
    let name_at = |i: usize| -> Option<String> {
        let raw = words.get(i)?.trim_end_matches([';', '(']);
        let raw = raw.split('(').next()?;
        let part = raw.rsplit('.').next()?;
        // Names arrive folded at the wire's entry; a quoted one keeps its
        // case, its doubled quotes one.
        let name = match part.strip_prefix('"').and_then(|p| p.strip_suffix('"')) {
            Some(inner) => inner.replace("\"\"", "\""),
            None => part.to_string(),
        };
        (!name.is_empty()).then_some(name)
    };
    match (upper(0).as_str(), upper(1).as_str()) {
        ("CREATE", "TABLE") => {
            let mut i = 2;
            if upper(2) == "IF" && upper(3) == "NOT" && upper(4) == "EXISTS" {
                i = 5;
            }
            let name = name_at(i)?;
            if database.relation(&name).is_some() {
                // IF NOT EXISTS over a table that is there changes nothing.
                return Some((Vec::new(), Vec::new()));
            }
            // A table created AS a query, or partitioned, is phase 2.
            let rest = sql.to_ascii_uppercase();
            if rest.contains(" PARTITION BY ") || rest.contains(" AS SELECT") || rest.contains(" AS (SELECT") {
                return None;
            }
            Some((Vec::new(), vec![name]))
        }
        ("ALTER", "TABLE") => {
            let mut i = 2;
            if upper(i) == "IF" && upper(i + 1) == "EXISTS" {
                i += 2;
            }
            if upper(i) == "ONLY" {
                i += 1;
            }
            let name = name_at(i)?;
            let mut existing = vec![name];
            // `RENAME TO` changes the name the undo would restore under.
            let up = sql.to_ascii_uppercase();
            if up.contains(" RENAME TO ") || up.contains(" SET TABLESPACE") || up.contains(" PARTITION ") {
                return None;
            }
            // A foreign key's referenced table holds its side of the key.
            for (n, w) in words.iter().enumerate() {
                if w.eq_ignore_ascii_case("REFERENCES") {
                    if let Some(parent) = name_at(n + 1) {
                        if !existing.contains(&parent) {
                            existing.push(parent);
                        }
                    }
                }
            }
            Some((existing, Vec::new()))
        }
        _ => None,
    }
}

impl Database {
    /// Whether `table` is one phase 1 can copy and put back.
    fn block_copyable(&self, table: &str) -> bool {
        use super::Relation;
        // A table that does not outlive the process is copied too (in
        // memory): only its crash intent is skipped, which `prepare` does.
        matches!(self.relation(table), Some(Relation::Plain(_))) && !self.on_key_value_store(table)
    }

    /// Whether `table` is placed on a key/value store, whose objects phase 1
    /// does not copy aside.
    fn on_key_value_store(&self, table: &str) -> bool {
        #[cfg(feature = "storage_pools")]
        if let Some(spec) = self.storage.spec_of(table) {
            return matches!(
                crate::core::storage::TableBackend::parse(&spec),
                crate::core::storage::TableBackend::KeyValue(_)
            );
        }
        let _ = table;
        false
    }

    /// Drops a table a block created and then rolled back: the block (or, at
    /// open, nothing else) holds it, so `drop_table`'s lock is there.
    pub(super) async fn drop_table_for_rollback(&self, table: &str) -> io::Result<()> {
        self.drop_table(table).await.map(|_| ())
    }

    /// Whether a block's `sql` rolls back with it (what
    /// [`Database::block_ddl_prepare`] would take on), for the warning about
    /// the DDL that does not.
    pub(crate) fn block_ddl_covers(&self, sql: &str) -> bool {
        targets(self, sql).is_some_and(|(existing, _)| existing.iter().all(|t| self.block_copyable(t)))
    }

    /// Before a DDL statement of block `root` runs: copies the tables it
    /// changes aside and records an intent for each step. `Ok(false)` when
    /// the statement is not one phase 1 makes transactional (it then runs as
    /// before); `Ok(true)` when it is, and the block must keep its lock.
    pub(crate) async fn block_ddl_prepare(&self, root: TxId, sql: &str) -> Result<bool, String> {
        let Some((existing, created)) = targets(self, sql) else {
            return Ok(false);
        };
        if existing.is_empty() && created.is_empty() {
            return Ok(true);
        }
        if existing.iter().any(|t| !self.block_copyable(t)) {
            return Ok(false);
        }
        // The statement's own lock, taken first and kept to the block's end:
        // nobody else may write a table while it is copied, nor read the
        // change before the block commits. The statement then finds it held.
        for table in &existing {
            self.relation_for(
                table,
                crate::core::mvcc::owner::current(),
                crate::core::mvcc::table_locks::TableMode::AccessExclusive,
            )
            .await?;
        }
        // Its outcome must reach the disk at COMMIT even when it wrote no
        // row: recovery reads an intent's transaction as uncommitted, and
        // undoes its DDL, unless the commit is recorded.
        self.transactions.mark_wrote(root);
        let mut steps: Vec<Undo> = existing
            .iter()
            .map(|t| Undo::Restore { table: t.clone(), copy: String::new() })
            .collect();
        steps.extend(created.iter().map(|t| Undo::Drop { table: t.clone() }));
        for undo in steps {
            let seq = self.block_ddl.entry(root).or_default().len() as u32;
            let undo = match undo {
                Undo::Restore { table, .. } => {
                    let copy = format!("{COPY_PREFIX}{root}_{seq}_{table}");
                    // The copy's placement first, as the rewrite's backup
                    // (`rewrite.rs`): its files go where the table's are, and
                    // a crash midway leaves the name pointing at them.
                    super::pool_rename(&self.storage, &table, &copy);
                    let table_seed = self.seed_of(&table).await;
                    let copy_seed = self.seed_of(&copy).await;
                    if let Err(e) = copy_files_between(&table_seed, &copy_seed, &table, &copy, RENAMED_SUFFIXES).await {
                        discard_files_in(&copy_seed, &self.storage, self.location.as_deref(), &copy, RENAMED_SUFFIXES).await;
                        return Err(format!("could not keep \"{table}\" aside for the block's rollback: {e}"));
                    }
                    Undo::Restore { table, copy }
                }
                drop => drop,
            };
            // No intent for a table a crash does not leave behind: there is
            // nothing to put back after one.
            let lasting = match &undo {
                Undo::Restore { table, .. } => self.survives_a_crash(table),
                Undo::Drop { .. } => true,
            };
            let intent = match self.location.as_deref().filter(|_| lasting) {
                Some(root_dir) => {
                    let path = intent_path(root_dir, root, seq);
                    super::rewrite::write_intent(&path, &encode(root, &undo))
                        .map_err(|e| format!("could not record the block's rollback: {e}"))?;
                    Some(path)
                }
                None => None,
            };
            self.block_ddl.entry(root).or_default().push(Step { undo, intent });
        }
        Ok(true)
    }

    /// Whether block `root` has DDL that rolls back with it.
    pub(crate) fn block_ddl_pending(&self, root: TxId) -> bool {
        self.block_ddl.get(&root).is_some_and(|s| !s.is_empty())
    }

    /// Block `root` committed durably: its intents go, then its copies.
    pub(crate) async fn block_ddl_commit(&self, root: TxId) {
        let Some((_, steps)) = self.block_ddl.remove(&root) else { return };
        for step in steps {
            if let Some(path) = &step.intent {
                if let Err(e) = super::rewrite::remove_intent(path) {
                    // Left for the next open, which reads the outcome as
                    // committed and discards.
                    warn!("Could not remove a committed block's DDL intent {}: {}", path.display(), e);
                    continue;
                }
            }
            if let Undo::Restore { copy, .. } = &step.undo {
                let seed = self.seed_of(copy).await;
                if discard_files_in(&seed, &self.storage, self.location.as_deref(), copy, RENAMED_SUFFIXES).await {
                    super::pool_forget(&self.storage, copy);
                }
            }
        }
    }

    /// Block `root` is aborting: its DDL undone, newest first, under the
    /// locks it still holds. A step that fails keeps its intent, so the next
    /// open finishes it.
    pub(crate) async fn block_ddl_abort(&self, root: TxId) {
        let Some((_, steps)) = self.block_ddl.remove(&root) else { return };
        let _ = self.undo_steps(steps).await;
    }

    /// How many DDL steps block `root` has registered: what a savepoint set
    /// now rolls back to.
    pub(crate) fn block_ddl_len(&self, root: TxId) -> usize {
        self.block_ddl.get(&root).map_or(0, |s| s.len())
    }

    /// `ROLLBACK TO` a savepoint set when block `root` had `keep` steps: the
    /// steps after it undone, newest first. `Err` when one could not be:
    /// those steps stay registered (and their intents on disk), so the
    /// block's ROLLBACK, or the next open, tries them again; the caller must
    /// not let the block commit over them.
    pub(crate) async fn block_ddl_rollback_to(&self, root: TxId, keep: usize) -> Result<(), String> {
        let undone = match self.block_ddl.get_mut(&root) {
            Some(mut steps) if steps.len() > keep => steps.split_off(keep),
            _ => return Ok(()),
        };
        let failed = self.undo_steps(undone).await;
        if failed.is_empty() {
            return Ok(());
        }
        let tables: Vec<String> = failed
            .iter()
            .map(|s| match &s.undo {
                Undo::Restore { table, .. } | Undo::Drop { table } => table.clone(),
            })
            .collect();
        self.block_ddl.entry(root).or_default().extend(failed);
        Err(format!(
            "could not roll back the DDL on {} since the savepoint; the transaction must be rolled back",
            tables.join(", ")
        ))
    }

    /// Undoes `steps`, newest first; returns those that failed, oldest first.
    async fn undo_steps(&self, steps: Vec<Step>) -> Vec<Step> {
        let mut failed = Vec::new();
        for step in steps.into_iter().rev() {
            let done = match &step.undo {
                Undo::Restore { table, copy } => {
                    let (table_c, copy_c) = (table.clone(), copy.clone());
                    let restored = self
                        .reload_with(table, |_old| async move {
                            self.put_back(&table_c, &copy_c).await.map_err(|e| e.to_string())
                        })
                        .await;
                    match restored {
                        Ok(()) => {
                            let seed = self.seed_of(copy).await;
                            if discard_files_in(&seed, &self.storage, self.location.as_deref(), copy, RENAMED_SUFFIXES).await {
                                super::pool_forget(&self.storage, copy);
                            }
                            true
                        }
                        Err(e) => {
                            error!("Could not roll back the block's DDL on \"{table}\": {e}; the next open finishes it");
                            false
                        }
                    }
                }
                Undo::Drop { table } => match self.drop_table_for_rollback(table).await {
                    Ok(()) => true,
                    Err(e) => {
                        error!("Could not drop \"{table}\", created by a block that rolled back: {e}");
                        false
                    }
                },
            };
            if done {
                if let Some(path) = &step.intent {
                    let _ = super::rewrite::remove_intent(path);
                }
            } else {
                failed.push(step);
            }
        }
        failed.reverse();
        failed
    }

    /// `copy`'s files over `table`'s.
    async fn put_back(&self, table: &str, copy: &str) -> io::Result<()> {
        let table_seed = self.seed_of(table).await;
        let copy_seed = self.seed_of(copy).await;
        if !discard_files_in(&table_seed, &self.storage, self.location.as_deref(), table, RENAMED_SUFFIXES).await {
            return Err(io::Error::other(format!("the changed files of \"{table}\" could not be released")));
        }
        copy_files_between(&copy_seed, &table_seed, copy, table, RENAMED_SUFFIXES).await
    }
}

/// At open, before any table opens: the intents a crash left. Those of a
/// transaction whose commit is recorded are discarded; a copy of one that did
/// not is put back over its table. A table such a transaction created is
/// returned with its intent, to drop once the database is open
/// ([`Database::finish_block_ddl_recovery`]).
pub(super) async fn recover_files(
    io_pointers: &rclite::Arc<crate::core::storage_providers::backend::DbStorageProvider>,
    pool: &super::StoragePool,
    kv: &super::KvStores,
    seeds: &super::SeedCache,
    committed: impl Fn(TxId) -> bool,
) -> Vec<(PathBuf, String)> {
    let mut drops = Vec::new();
    let Some(root) = io_pointers.get_location() else { return drops };
    let Ok(entries) = std::fs::read_dir(&root) else { return drops };
    let mut intents: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with(INTENT_PREFIX))
                && p.extension().is_some_and(|x| x == INTENT_EXTENSION)
        })
        .collect();
    // Newest first: by name, the transaction and then the step.
    intents.sort();
    intents.reverse();
    for path in intents {
        let Some((tx, undo)) = std::fs::read_to_string(&path).ok().and_then(|t| decode(&t)) else {
            warn!("Malformed block DDL intent {}; leaving it in place", path.display());
            continue;
        };
        let done = match (&undo, committed(tx)) {
            (Undo::Restore { copy, .. }, true) => {
                let seed = super::seed_in(io_pointers, pool, kv, seeds, copy).await;
                discard_files_in(&seed, pool, Some(&root), copy, RENAMED_SUFFIXES).await
            }
            (Undo::Drop { .. }, true) => true,
            (Undo::Restore { table, copy }, false) => {
                info!("Restoring \"{table}\": DDL of transaction {tx} did not commit before a crash");
                let table_seed = super::seed_in(io_pointers, pool, kv, seeds, table).await;
                let copy_seed = super::seed_in(io_pointers, pool, kv, seeds, copy).await;
                let released = discard_files_in(&table_seed, pool, Some(&root), table, RENAMED_SUFFIXES).await;
                match released {
                    true => match copy_files_between(&copy_seed, &table_seed, copy, table, RENAMED_SUFFIXES).await {
                        Ok(()) => discard_files_in(&copy_seed, pool, Some(&root), copy, RENAMED_SUFFIXES).await || true,
                        Err(e) => {
                            error!("Could not restore \"{table}\" after a crash: {e}");
                            false
                        }
                    },
                    false => {
                        error!("Could not release the changed files of \"{table}\" after a crash");
                        false
                    }
                }
            }
            (Undo::Drop { table }, false) => {
                drops.push((path.clone(), table.clone()));
                continue;
            }
        };
        if done {
            let _ = super::rewrite::remove_intent(&path);
        }
    }
    drops
}

impl Database {
    /// The second half of [`recover_files`], once the database is open:
    /// each table a block created that did not commit, dropped.
    pub(super) async fn finish_block_ddl_recovery(&self, drops: Vec<(PathBuf, String)>) {
        for (path, table) in drops {
            info!("Dropping \"{table}\": created by a transaction that did not commit before a crash");
            if self.relation(&table).is_some() {
                if let Err(e) = self.drop_table_for_rollback(&table).await {
                    error!("Could not drop \"{table}\" after a crash: {e}");
                    continue;
                }
            }
            let _ = super::rewrite::remove_intent(&path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_intent_round_trips_and_a_torn_one_does_not_parse() {
        for undo in [
            Undo::Restore { table: "t".into(), copy: format!("{COPY_PREFIX}7_0_t") },
            Undo::Drop { table: "new_t".into() },
        ] {
            let text = encode(7, &undo);
            assert_eq!(decode(&text), Some((7, undo.clone())));
            for cut in 0..text.len() {
                assert_eq!(decode(&text[..cut]), None, "a cut at {cut} parsed");
            }
        }
        assert_eq!(decode(&encode(7, &Undo::Restore { table: "t".into(), copy: "elsewhere".into() })), None);
    }
}
