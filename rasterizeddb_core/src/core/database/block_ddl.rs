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
//! Covered: `CREATE TABLE` (also `AS` a query, and `PARTITION BY`), `ALTER
//! TABLE` (and the table an `ALTER … REFERENCES` names), and `CREATE [UNIQUE]
//! INDEX` / `DROP INDEX` (2026-11-12), on plain tables on any backend and on
//! partitioned ones. What is held aside is what the statement can change
//! ([`FileSet`]): every file for an ALTER of a plain table, which may
//! rewrite its rows; the schema files for an index, which only declares; and
//! for a partitioned table always its shell's schema files, `__pt_<uid>_
//! schema*`, since its DDL changes nothing else (`partitioned_ddl.rs`). A
//! key/value store's objects are copied through a seed that refuses an
//! unreachable store rather than copy its empty stand-in. A table that does
//! not outlive the process is copied in memory and has no crash intent.
//!
//! `DROP TABLE` is not copied: it is **deferred** (2026-11-11,
//! `Undo::DropAtCommit`, `pgwire::deferred_drops`), whatever kind of table it
//! names. It is checked and takes its ACCESS EXCLUSIVE when issued, and runs
//! as the block's last statement at COMMIT, so a ROLLBACK, or a crash before
//! COMMIT, never touched the table. A `CREATE TABLE` of the name later in
//! the block runs the pending drop first, and that one is then not undone
//! (the client is warned).
//!
//! Other DDL in a block (`ALTER TABLE … RENAME TO`, a conversion or reshape
//! of a partitioned table) stays as before: applied at once, with the
//! warning that says so, to the client as well as the log. Role statements
//! are the block's writes to the role catalogue (`auth::
//! execute_role_statement_in`), and `CREATE`/`DROP DATABASE` are refused in
//! a block (25001), as in PostgreSQL.

use std::io;
use std::path::{Path, PathBuf};

use log::{error, info, warn};

use super::rewrite::FileSet;
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
    /// Put `copy`'s files back over `table`'s: those of `set` under the stem
    /// `stem`, which is the table's name for a plain table and its shell's
    /// `__pt_<uid>` for a partitioned one (whose DDL changes only the shell's
    /// schema files, `partitioned_ddl.rs`).
    Restore { table: String, copy: String, set: FileSet, stem: String },
    /// Drop `table`: the block created it.
    Drop { table: String },
    /// A `DROP TABLE` the block ran, deferred to its commit (`sql`, the
    /// single-table drop): nothing to undo, the table was never touched.
    /// Never written as an intent — the drop happens inside the block, at
    /// its commit, so a crash before then leaves the table as it was.
    DropAtCommit { table: String, sql: String },
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

/// `tx`, then the step, then a checksum; anything else does not parse. A
/// whole plain table's restore keeps the three-field form the first
/// version wrote, so either binary reads the other's intents.
fn encode(tx: TxId, undo: &Undo) -> String {
    let body = match undo {
        Undo::Restore { table, copy, set: FileSet::Whole, stem } if stem == table => {
            format!("{tx}\nrestore\t{table}\t{copy}")
        }
        Undo::Restore { table, copy, set, stem } => format!("{tx}\nrestore\t{table}\t{copy}\t{}\t{stem}", set.tag()),
        Undo::Drop { table } => format!("{tx}\ndrop\t{table}"),
        Undo::DropAtCommit { table, .. } => format!("{tx}\ndropcommit\t{table}"),
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
    let parts: Vec<&str> = step.split('\t').collect();
    let undo = match parts.as_slice() {
        ["restore", table, copy] if copy.starts_with(COPY_PREFIX) && !table.is_empty() => Undo::Restore {
            table: table.to_string(),
            copy: copy.to_string(),
            set: FileSet::Whole,
            stem: table.to_string(),
        },
        ["restore", table, copy, set, stem] if copy.starts_with(COPY_PREFIX) && !table.is_empty() && !stem.is_empty() => {
            Undo::Restore {
                table: table.to_string(),
                copy: copy.to_string(),
                set: FileSet::from_tag(set)?,
                stem: stem.to_string(),
            }
        }
        ["drop", table] if !table.is_empty() => Undo::Drop { table: table.to_string() },
        _ => return None,
    };
    Some((tx, undo))
}

/// The tables a block DDL statement changes, as `(existing, created)`, or
/// `None` for a statement phase 1 does not make transactional. Each existing
/// table comes with the files the statement can change: all of them for an
/// `ALTER` that may rewrite rows, the schema files for one that only
/// declares (an index). A partitioned table's are always its schema files
/// ([`Database::block_ddl_prepare`]).
pub(crate) fn targets(database: &Database, sql: &str) -> Option<(Vec<(String, FileSet)>, Vec<String>)> {
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
            // Created AS a query or partitioned, it is still undone by
            // dropping it: `drop_table_for_rollback` takes either, and a
            // CREATE TABLE AS whose rows failed is one the statement made
            // (`block_ddl_unmade`).
            Some((Vec::new(), vec![name]))
        }
        // `CREATE [UNIQUE] INDEX … ON [ONLY] t …`: the table's declarations
        // change. A unique index is a constraint here (the statement runs as
        // `ALTER TABLE … ADD CONSTRAINT`), so it is held as an ALTER is.
        ("CREATE", "INDEX") | ("CREATE", "UNIQUE") => {
            let unique = upper(1) == "UNIQUE";
            if unique && upper(2) != "INDEX" {
                return None;
            }
            // `USING partition` is a conversion, which moves every row.
            if sql.to_ascii_uppercase().contains(" USING PARTITION") {
                return None;
            }
            let on = words.iter().position(|w| w.eq_ignore_ascii_case("ON"))?;
            let mut i = on + 1;
            if upper(i) == "ONLY" {
                i += 1;
            }
            let name = name_at(i)?;
            if database.relation(&name).is_none() {
                // The statement refuses it (42P01) and changes nothing.
                return Some((Vec::new(), Vec::new()));
            }
            let set = if unique { FileSet::Whole } else { FileSet::Schema };
            Some((vec![(name, set)], Vec::new()))
        }
        // `DROP INDEX [CONCURRENTLY] [IF EXISTS] i [, …] [CASCADE|RESTRICT]`:
        // index names are database-wide, so each table is found by the
        // declaration that names it.
        ("DROP", "INDEX") => {
            let mut i = 2;
            if upper(i) == "CONCURRENTLY" {
                i += 1;
            }
            if upper(i) == "IF" && upper(i + 1) == "EXISTS" {
                i += 2;
            }
            let rest = words.get(i..)?.join(" ");
            let rest = rest.trim().trim_end_matches(';').trim_end();
            let mut existing: Vec<(String, FileSet)> = Vec::new();
            for raw in rest.split(',') {
                let mut raw = raw.trim();
                for tail in [" CASCADE", " RESTRICT"] {
                    if raw.len() > tail.len() && raw[raw.len() - tail.len()..].eq_ignore_ascii_case(tail) {
                        raw = raw[..raw.len() - tail.len()].trim_end();
                    }
                }
                let part = raw.rsplit('.').next().unwrap_or(raw);
                let index = match part.strip_prefix('"').and_then(|p| p.strip_suffix('"')) {
                    Some(inner) => inner.replace("\"\"", "\""),
                    None => part.to_string(),
                };
                if let Some(table) = database.index_owner(&index) {
                    if !existing.iter().any(|(t, _)| *t == table) {
                        existing.push((table, FileSet::Schema));
                    }
                }
            }
            Some((existing, Vec::new()))
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
            let mut existing = vec![(name, FileSet::Whole)];
            // `RENAME TO` changes the name the undo would restore under.
            let up = sql.to_ascii_uppercase();
            if up.contains(" RENAME TO ") || up.contains(" SET TABLESPACE") || up.contains(" PARTITION ") {
                return None;
            }
            // A foreign key's referenced table holds its side of the key.
            for (n, w) in words.iter().enumerate() {
                if w.eq_ignore_ascii_case("REFERENCES") {
                    if let Some(parent) = name_at(n + 1) {
                        if !existing.iter().any(|(t, _)| *t == parent) {
                            existing.push((parent, FileSet::Whole));
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
    /// Whether `table` is one a block can hold aside and put back: a plain
    /// table on any backend (a key/value store's objects are copied through
    /// its seed, which refuses rather than copy an unreachable store's
    /// in-memory stand-in), or a partitioned one, whose DDL changes only its
    /// shell's schema files. A table that does not outlive the process is
    /// copied too (in memory): only its crash intent is skipped.
    pub(crate) fn block_restorable(&self, table: &str) -> bool {
        self.relation(table).is_some()
    }

    /// The table that declares index `name` (names are database-wide): a
    /// `CREATE INDEX` over columns or a path, or a partial unique key, which
    /// is what a partial `CREATE UNIQUE INDEX` is kept as.
    pub(crate) fn index_owner(&self, name: &str) -> Option<String> {
        let mut owner: Option<String> = None;
        self.each_schema(|table, schema| {
            if owner.is_none()
                && (schema.secondary_indexes.iter().any(|i| !i.is_deleted && i.name.eq_ignore_ascii_case(name))
                    || schema.path_indexes.iter().any(|i| !i.is_deleted && i.name.eq_ignore_ascii_case(name))
                    || schema
                        .unique_keys
                        .iter()
                        .any(|k| !k.is_deleted && k.predicate.is_some() && k.name.eq_ignore_ascii_case(name)))
            {
                owner = Some(table.to_string());
            }
        });
        owner
    }

    /// Drops a table a block created and then rolled back: the block (or, at
    /// open, nothing else) holds it, so `drop_table`'s lock is there.
    pub(super) async fn drop_table_for_rollback(&self, table: &str) -> io::Result<()> {
        // Never made: a statement that failed past its prepare by a way
        // `block_ddl_unmade` did not see (a panic that closed the session).
        if !(self.has_table(table) || self.is_partitioned(table)) {
            return Ok(());
        }
        self.drop_table(table).await.map(|_| ())
    }

    /// Whether a block's `sql` rolls back with it (what
    /// [`Database::block_ddl_prepare`] would take on), for the warning about
    /// the DDL that does not.
    pub(crate) fn block_ddl_covers(&self, sql: &str) -> bool {
        // A DROP TABLE is deferred to the commit (`pgwire::deferred_drops`),
        // whatever kind of table it names.
        let mut words = sql.split_whitespace();
        if words.next().is_some_and(|w| w.eq_ignore_ascii_case("DROP")) && words.next().is_some_and(|w| w.eq_ignore_ascii_case("TABLE")) {
            use crate::core::rql::lexer_alter::{expand_multi_drop, parse_drop_table};
            let trimmed = sql.trim().trim_end_matches(';').trim_end();
            let each = expand_multi_drop(trimmed).unwrap_or_else(|| vec![trimmed.to_string()]);
            return each.iter().all(|one| parse_drop_table(one).is_ok());
        }
        targets(self, sql).is_some_and(|(existing, _)| existing.iter().all(|(t, _)| self.block_restorable(t)))
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
        if existing.iter().any(|(t, _)| !self.block_restorable(t)) {
            return Ok(false);
        }
        // The statement's own lock, taken first and kept to the block's end:
        // nobody else may write a table while it is copied, nor read the
        // change before the block commits. The statement then finds it held.
        for (table, _) in &existing {
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
            .map(|(t, set)| {
                // A partitioned table's DDL writes only its shell's schema
                // files, `__pt_<uid>_schema*` (partitioned_ddl.rs): those are
                // what is held, under that stem.
                let (set, stem) = match self.is_partitioned(t) {
                    true => (FileSet::Schema, self.schema_files_stem(t)),
                    false => (*set, t.clone()),
                };
                Undo::Restore { table: t.clone(), copy: String::new(), set, stem }
            })
            .collect();
        steps.extend(created.iter().map(|t| Undo::Drop { table: t.clone() }));
        for undo in steps {
            let seq = self.block_ddl.entry(root).or_default().len() as u32;
            let undo = match undo {
                Undo::Restore { table, set, stem, .. } => {
                    let copy = format!("{COPY_PREFIX}{root}_{seq}_{stem}");
                    // The copy's placement first, as the rewrite's backup
                    // (`rewrite.rs`): its files go where the table's are, and
                    // a crash midway leaves the name pointing at them.
                    super::pool_rename(&self.storage, &stem, &copy);
                    let copied = async {
                        // A key/value store that cannot be opened gives an
                        // empty in-memory stand-in: copying from it would
                        // keep nothing aside and report success.
                        let stem_seed = super::seed_or_unavailable(
                            &self.db_io_pointers,
                            &self.storage,
                            &self.kv,
                            &self.seeds,
                            &stem,
                        )
                        .await?;
                        let copy_seed =
                            super::seed_or_unavailable(&self.db_io_pointers, &self.storage, &self.kv, &self.seeds, &copy)
                                .await?;
                        copy_files_between(&stem_seed, &copy_seed, &stem, &copy, &set.suffixes()).await
                    }
                    .await;
                    if let Err(e) = copied {
                        let copy_seed = self.seed_of(&copy).await;
                        if discard_files_in(&copy_seed, &self.storage, self.location.as_deref(), &copy, RENAMED_SUFFIXES).await {
                            super::pool_forget(&self.storage, &copy);
                        }
                        return Err(format!("could not keep \"{table}\" aside for the block's rollback: {e}"));
                    }
                    Undo::Restore { table, copy, set, stem }
                }
                drop => drop,
            };
            // No intent for a table a crash does not leave behind: there is
            // nothing to put back after one.
            let lasting = match &undo {
                Undo::Restore { table, .. } => self.survives_a_crash(table),
                Undo::Drop { .. } => true,
                Undo::DropAtCommit { .. } => false,
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

    /// A block's `DROP TABLE`, deferred to its commit: each table locked
    /// ACCESS EXCLUSIVE to the block's end, as PostgreSQL's drop holds it,
    /// and registered with the single-table drop that runs at commit
    /// ([`Database::block_drops_take`]). A `ROLLBACK` (to a savepoint before
    /// it too) then leaves the table as it was; dropped at once, a rolled-back
    /// drop destroyed the table and its rows.
    pub(crate) async fn block_drop_defer(&self, root: TxId, drops: Vec<(String, String)>) -> Result<(), String> {
        for (table, _) in &drops {
            self.relation_for(
                table,
                crate::core::mvcc::owner::current(),
                crate::core::mvcc::table_locks::TableMode::AccessExclusive,
            )
            .await?;
            // The block's own row locks on it would outlive the table, keyed
            // by a name the next table could take: refused now, as the drop
            // itself refuses it (55006), not at the commit.
            if crate::core::mvcc::owner::current().is_some_and(|o| self.row_locks.txn_holds_rows_on(table, o.owner.id())) {
                return Err(format!(
                    "cannot DROP TABLE on \"{table}\" because it is being used by active queries in this session: the \
                     transaction holds row locks on the table, which this would orphan; end the transaction first"
                ));
            }
        }
        let mut steps = self.block_ddl.entry(root).or_default();
        for (table, sql) in drops {
            steps.push(Step { undo: Undo::DropAtCommit { table, sql }, intent: None });
        }
        Ok(())
    }

    /// The tables block `root` has a deferred drop pending for.
    pub(crate) fn block_drops_pending(&self, root: TxId) -> Vec<String> {
        self.block_ddl.get(&root).map_or_else(Vec::new, |steps| {
            steps
                .iter()
                .filter_map(|s| match &s.undo {
                    Undo::DropAtCommit { table, .. } => Some(table.clone()),
                    _ => None,
                })
                .collect()
        })
    }

    /// Block `root`'s deferred drops, taken out to run (`None`: every one;
    /// `Some(table)`: that table's, which a `CREATE TABLE` of its name in the
    /// block needs gone first), as `(table, sql)`, oldest first.
    pub(crate) fn block_drops_take(&self, root: TxId, only: Option<&str>) -> Vec<(String, String)> {
        let Some(mut steps) = self.block_ddl.get_mut(&root) else { return Vec::new() };
        let mut taken = Vec::new();
        steps.retain(|s| match &s.undo {
            Undo::DropAtCommit { table, sql } if only.is_none_or(|o| o.eq_ignore_ascii_case(table)) => {
                taken.push((table.clone(), sql.clone()));
                false
            }
            _ => true,
        });
        taken
    }

    /// `table`'s files moved to another stem (a partitioned table rewritten
    /// into a new uid, `rewrite_partitioned`, or a plain one converted into
    /// a partitioned one), deleting the files they were under: every
    /// block's held copy of it under the old stem can no longer be put back
    /// (put back, it would land as files no table owns, and the change would
    /// stay). Those steps are forgotten, their copies discarded, and the
    /// client warned, as for any DDL that does not roll back; reporting a
    /// ROLLBACK that left the change in place would be the silent version.
    pub(crate) async fn block_ddl_table_moved(&self, table: &str) {
        let stem = self.schema_files_stem(table);
        let mut stale: Vec<Step> = Vec::new();
        for mut entry in self.block_ddl.iter_mut() {
            let steps = entry.value_mut();
            let mut i = 0;
            while i < steps.len() {
                let moved = matches!(&steps[i].undo, Undo::Restore { table: t, stem: s, .. } if t == table && *s != stem);
                if moved {
                    stale.push(steps.remove(i));
                } else {
                    i += 1;
                }
            }
        }
        if stale.is_empty() {
            return;
        }
        for step in stale {
            if let Some(path) = &step.intent {
                let _ = super::rewrite::remove_intent(path);
            }
            if let Undo::Restore { copy, .. } = &step.undo {
                let seed = self.seed_of(copy).await;
                if discard_files_in(&seed, &self.storage, self.location.as_deref(), copy, RENAMED_SUFFIXES).await {
                    super::pool_forget(&self.storage, copy);
                }
            }
        }
        crate::core::notice::push(
            "WARNING",
            "01000",
            format!(
                "table \"{table}\" was rewritten into new files: this transaction's changes to it are applied at once and \
                 will not be undone if the transaction rolls back"
            ),
        );
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

    /// The statement that registered block `root`'s steps past `mark`
    /// failed with `sqlstate`: forgets each table it was to create and did
    /// not, so neither the block's rollback, a `ROLLBACK TO`, nor the next
    /// open drops a table the block never made. Run before either undo.
    ///
    /// Not made: there is no such table (a read-only refusal, a bad column
    /// type), or there is and the CREATE found it there (`42P07`): another
    /// session created it between [`Database::block_ddl_prepare`] and the
    /// statement's own lock, and its table must outlive this block. One the
    /// statement made before failing (a `CREATE TABLE AS` whose rows did not
    /// go in) exists with another code and is still dropped.
    pub(crate) fn block_ddl_unmade(&self, root: TxId, mark: usize, sqlstate: &str) {
        let Some(mut steps) = self.block_ddl.get_mut(&root) else { return };
        if steps.len() <= mark {
            return;
        }
        let tail = steps.split_off(mark);
        for step in tail {
            let unmade = match &step.undo {
                Undo::Drop { table } => {
                    sqlstate == "42P07" || !(self.has_table(table) || self.is_partitioned(table))
                }
                Undo::Restore { .. } | Undo::DropAtCommit { .. } => false,
            };
            if unmade {
                if let Some(path) = &step.intent {
                    let _ = super::rewrite::remove_intent(path);
                }
            } else {
                steps.push(step);
            }
        }
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
                Undo::Restore { table, .. } | Undo::Drop { table } | Undo::DropAtCommit { table, .. } => table.clone(),
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
                Undo::Restore { table, copy, set, stem } => {
                    let (stem_c, copy_c, set_c) = (stem.clone(), copy.clone(), *set);
                    // The instance is retired by the reload (a partitioned
                    // one's every partition with its shell), so nothing
                    // writes the files while they are put back.
                    let restored = self
                        .reload_with(table, |_old| async move {
                            self.put_back(&stem_c, &copy_c, set_c).await.map_err(|e| e.to_string())
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
                // The table was never touched: nothing to undo.
                Undo::DropAtCommit { .. } => true,
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

    /// `copy`'s files of `set` over `stem`'s, as a failed rewrite's backup
    /// goes back (`rewrite::restore_from_backup`): both seeds real, or not
    /// at all.
    async fn put_back(&self, stem: &str, copy: &str, set: FileSet) -> io::Result<()> {
        let restore = super::rewrite::restore_from_backup(
            &self.db_io_pointers,
            &self.storage,
            &self.kv,
            &self.seeds,
            self.location.as_deref(),
            stem,
            copy,
            set,
        );
        match set {
            // The schema files go back over a live file set's names through
            // the database's seed, as a metadata change's backup does
            // (`reload_atomic_with`, `Ungated::SchemaRewrite`).
            FileSet::Schema => {
                crate::core::index_store::gated_io::ungated(
                    crate::core::index_store::gated_io::Ungated::SchemaRewrite,
                    restore,
                )
                .await
            }
            FileSet::Whole => restore.await,
        }
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
            // Never written as an intent; nothing to finish.
            (Undo::DropAtCommit { .. }, _) => true,
            (Undo::Restore { table, copy, set, stem }, false) => {
                info!("Restoring \"{table}\": DDL of transaction {tx} did not commit before a crash");
                match super::rewrite::restore_from_backup(io_pointers, pool, kv, seeds, Some(&root), stem, copy, *set).await {
                    Ok(()) => {
                        let copy_seed = super::seed_in(io_pointers, pool, kv, seeds, copy).await;
                        discard_files_in(&copy_seed, pool, Some(&root), copy, RENAMED_SUFFIXES).await || true
                    }
                    Err(e) => {
                        error!("Could not restore \"{table}\" after a crash: {e}");
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
            Undo::Restore { table: "t".into(), copy: format!("{COPY_PREFIX}7_0_t"), set: FileSet::Whole, stem: "t".into() },
            Undo::Restore { table: "t".into(), copy: format!("{COPY_PREFIX}7_1_t"), set: FileSet::Schema, stem: "t".into() },
            Undo::Restore {
                table: "p".into(),
                copy: format!("{COPY_PREFIX}7_2___pt_9"),
                set: FileSet::Schema,
                stem: "__pt_9".into(),
            },
            Undo::Drop { table: "new_t".into() },
        ] {
            let text = encode(7, &undo);
            assert_eq!(decode(&text), Some((7, undo.clone())));
            for cut in 0..text.len() {
                assert_eq!(decode(&text[..cut]), None, "a cut at {cut} parsed");
            }
        }
        assert_eq!(
            decode(&encode(
                7,
                &Undo::Restore { table: "t".into(), copy: "elsewhere".into(), set: FileSet::Whole, stem: "t".into() }
            )),
            None
        );
    }

    /// An intent the first version wrote (three fields) reads as a whole
    /// plain table's restore, so a crash across the upgrade still undoes it.
    #[test]
    fn a_first_version_intent_reads_as_a_whole_restore() {
        let body = format!("7\nrestore\tt\t{COPY_PREFIX}7_0_t");
        let text = format!("{body}\n{:016x}\n", crate::core::wal::file_id_for_name(&body));
        assert_eq!(
            decode(&text),
            Some((
                7,
                Undo::Restore { table: "t".into(), copy: format!("{COPY_PREFIX}7_0_t"), set: FileSet::Whole, stem: "t".into() }
            ))
        );
        assert_eq!(text, encode(7, &Undo::Restore {
            table: "t".into(),
            copy: format!("{COPY_PREFIX}7_0_t"),
            set: FileSet::Whole,
            stem: "t".into()
        }));
    }
}
