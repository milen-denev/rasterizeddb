//! The database's clean points (docs/persistent-indexes.md §3, slice B13):
//! `rdb_index_checkpoint(regclass)`, fanned out over a partitioned table's
//! materialized partitions, and shutdown's seal of every file set. The
//! third clean point, the idle checkpoint, is each instance's own watcher
//! (`row::table::table_seal`).
//!
//! Beside them, `rdb_index_check(regclass)` (§4 Audit, B18): the audit's
//! check on demand, over the same file sets.

use std::time::Duration;

use futures::StreamExt;
use rclite::Arc;

use super::Database;
use super::relation::Relation;
use crate::core::mvcc::table_locks::TableMode;
use crate::core::index_store::audit::{Findings, Outcome};
use crate::core::index_store::gate_word::SealKind;
use crate::core::index_store::seal::{self, SealOutcome};
use crate::core::mvcc::owner::{self, OwnerKind, StatementOwner};
use crate::core::mvcc::table_locks::LockOwner;
use crate::core::mvcc::transaction::TxId;
use crate::core::row::table::Table;
use crate::core::storage_providers::backend::DbStorageProvider;

/// One file set's result.
#[derive(Debug)]
pub struct Sealed {
    /// The table, or `table (partition pid)`.
    pub name: String,
    pub outcome: SealOutcome,
}

/// What `rdb_index_checkpoint` did.
#[derive(Debug, Default)]
pub struct CheckpointReport {
    pub sealed: Vec<Sealed>,
    /// File sets this transaction holds a ticket on, so not sealed: the
    /// notice §3 asks for instead of waiting `seal_timeout` on itself.
    pub own_writes: Vec<String>,
}

impl CheckpointReport {
    /// How many file sets now end `CLEAN`, of how many were asked.
    pub fn clean(&self) -> (usize, usize) {
        let clean = self.sealed.iter().filter(|s| s.outcome.is_clean()).count();
        (clean, self.sealed.len() + self.own_writes.len())
    }
}

/// One file set's check.
#[derive(Debug)]
pub struct Checked {
    /// The table, or `table (partition pid)`.
    pub name: String,
    pub outcome: Outcome,
}

/// What `rdb_index_check` did, file set by file set.
#[derive(Debug, Default)]
pub struct CheckReport {
    pub checked: Vec<Checked>,
}

impl CheckReport {
    /// Every file set's findings, added up (a partitioned table's).
    pub fn findings(&self) -> Findings {
        let mut all = Findings::default();
        for c in &self.checked {
            if let Some(f) = c.outcome.findings() {
                all.absorb(f);
            }
        }
        all
    }

    /// The worst outcome, as `rdb_index_check`'s `result` column says it: a
    /// mismatch over an abandoned check over a clean one.
    pub fn result(&self) -> String {
        let pick = |want: fn(&Outcome) -> bool| self.checked.iter().find(|c| want(&c.outcome));
        let worst = pick(|o| matches!(o, Outcome::Mismatch { .. }))
            .or_else(|| pick(|o| matches!(o, Outcome::Abandoned(_))))
            .or_else(|| self.checked.first());
        match worst {
            Some(c) if self.checked.len() > 1 && !matches!(c.outcome, Outcome::Clean(_)) => {
                format!("{}: {}", c.name, c.outcome.describe())
            }
            Some(c) => c.outcome.describe(),
            None => "clean: no file sets".into(),
        }
    }
}

/// Why `rdb_index_check` refused, before anything was read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckRefusal {
    /// Nothing is registered under the name (42P01).
    NotFound(String),
    /// The name is an index or a catalogue view, not a table (42809).
    NotATable(String),
    /// Anything else (XX000).
    Other(String),
}

impl std::fmt::Display for CheckRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckRefusal::NotFound(m) | CheckRefusal::NotATable(m) | CheckRefusal::Other(m) => f.write_str(m),
        }
    }
}

/// What shutdown's seal did.
#[derive(Debug, Default)]
pub struct ShutdownReport {
    pub sealed: Vec<Sealed>,
    /// File sets the deadline left unsealed.
    pub skipped: Vec<String>,
}

impl Database {
    /// `rdb_index_checkpoint(name)` (§3): a mandatory seal of the table's
    /// file set, or, for a partitioned table, of each materialized
    /// partition's in turn. Run under `block`'s owner when called inside a
    /// transaction block, else under a statement owner of its own: ACCESS
    /// SHARE on the name, so no DDL retires the instance under the seal and
    /// writers run on. A file set the calling family holds a ticket on is
    /// not sealed (its own ticket would hold the seal for `seal_timeout`):
    /// it is reported in [`CheckpointReport::own_writes`].
    pub async fn index_checkpoint(&self, name: &str, block: Option<TxId>) -> Result<CheckpointReport, String> {
        let run = async {
            let relation = self
                .relation_for(name, owner::ensure(), TableMode::AccessShare)
                .await?
                .ok_or_else(|| format!("relation \"{name}\" does not exist"))?;
            let family = owner::family_for(&self.transactions);
            let mut report = CheckpointReport::default();
            for (label, table) in seal_targets(name, &relation)? {
                if let Some(f) = family
                    && self.transactions.family_tickets().keeps_on(f, table.gate())
                {
                    report.own_writes.push(label);
                    continue;
                }
                let outcome = table.seal_snapshot(SealKind::Mandatory).await;
                report.sealed.push(Sealed { name: label, outcome });
            }
            Ok(report)
        };
        match block {
            Some(root) => owner::under(&self.transactions, root, OwnerKind::Block, run).await,
            None => owner::statement(&self.transactions, &self.row_locks, OwnerKind::Statement, run).await,
        }
    }

    /// `rdb_index_check(name)` (§4 Audit, B18): the audit's check of the
    /// table's file set now, or of each materialized partition's in turn,
    /// with its in-place rebuild on a mismatch. Under the same owner and
    /// ACCESS SHARE lock as [`Database::index_checkpoint`]. A file set the
    /// calling family holds a ticket on is not checked (its own writes would
    /// abandon the check): reported abandoned. Refuses an index, a
    /// catalogue view and an unknown name before reading anything.
    pub async fn index_check(&self, name: &str, block: Option<TxId>) -> Result<CheckReport, CheckRefusal> {
        if crate::core::rql::catalog_views::is_catalog_view(name) {
            return Err(CheckRefusal::NotATable(format!("\"{name}\" is a catalog view, not a table")));
        }
        let run = async {
            let relation = match self.relation_for(name, owner::ensure(), TableMode::AccessShare).await {
                Ok(Some(relation)) => relation,
                Ok(None) => return Err(self.not_a_table(name)),
                Err(e) => return Err(CheckRefusal::Other(e)),
            };
            let family = owner::family_for(&self.transactions);
            let mut report = CheckReport::default();
            for (label, table) in seal_targets(name, &relation).map_err(CheckRefusal::Other)? {
                let outcome = match family {
                    Some(f) if self.transactions.family_tickets().keeps_on(f, table.gate()) => {
                        Outcome::Abandoned("this transaction has written it; run rdb_index_check after it ends".into())
                    }
                    _ => table.index_check().await,
                };
                report.checked.push(Checked { name: label, outcome });
            }
            Ok(report)
        };
        match block {
            Some(root) => owner::under(&self.transactions, root, OwnerKind::Block, run).await,
            None => owner::statement(&self.transactions, &self.row_locks, OwnerKind::Statement, run).await,
        }
    }

    /// The refusal for a name no relation is registered under: an index's
    /// name is not a table's, anything else does not exist.
    fn not_a_table(&self, name: &str) -> CheckRefusal {
        for table in self.relation_names() {
            let Some(relation) = self.relation(&table) else { continue };
            let schema = relation.schema();
            if schema.secondary_indexes.iter().any(|d| d.name == name)
                || schema.path_indexes.iter().any(|p| !p.is_deleted && p.name == name)
            {
                return CheckRefusal::NotATable(format!("\"{name}\" is an index on {table}, not a table"));
            }
        }
        CheckRefusal::NotFound(format!("relation \"{name}\" does not exist"))
    }

    /// Shutdown's seal (§3 Shutdown), after the sessions have ended: every
    /// instance's background writers stop first (vacuum, the filter's
    /// checkpoint loop, the idle watcher, the providers' landing) and
    /// replication is turned off, since each passes the gate and a ticket
    /// queued behind a shutdown seal would re-dirty it; then every file set,
    /// plain tables and partitions alike, gets a mandatory seal, several at a
    /// time, the most rebuild saved per snapshot byte first, under
    /// `deadline`. What the deadline leaves is skipped, counted, and rebuilds
    /// after the restart.
    pub async fn seal_for_shutdown(&self, deadline: Duration) -> ShutdownReport {
        let until = tokio::time::Instant::now() + deadline;
        // The sessions are gone: partitioned tables' SERIAL marks exactly.
        self.record_clean_shutdown().await;
        self.replication.set_active(false);
        for peer in self.replication.peers() {
            peer.stop();
        }
        let mut targets: Vec<(String, Arc<Table<DbStorageProvider>>)> = Vec::new();
        let mut names = self.relation_names();
        names.sort();
        names.dedup();
        for name in names {
            // An owner of its own per name, given back at once: the
            // instances are kept, and no DDL runs once the sessions are gone.
            let Some(own) = LockOwner::autocommit(self.transactions.begin_owner()) else {
                continue;
            };
            let statement = StatementOwner {
                owner: own,
                kind: OwnerKind::Statement,
            };
            let relation = self.relation_for(&name, Some(statement), TableMode::AccessShare).await;
            self.row_locks.release_owner(own);
            self.transactions.end_owner(own.id());
            match relation {
                Ok(Some(relation)) => match seal_targets(&name, &relation) {
                    Ok(found) => targets.extend(found),
                    Err(e) => log::warn!("shutdown: {name} is not sealed: {e}"),
                },
                Ok(None) => {}
                Err(e) => log::warn!("shutdown: {name} is not sealed: {e}"),
            }
        }
        for (_, table) in &targets {
            table.quiesce_for_shutdown().await;
        }
        let mut weighted = Vec::with_capacity(targets.len());
        for (label, table) in targets {
            let (saved, bytes) = table.rebuild_weight().await;
            weighted.push((saved as f64 / bytes as f64, label, table));
        }
        weighted.sort_by(|a, b| b.0.total_cmp(&a.0));
        let parallel = std::thread::available_parallelism().map_or(4, |n| n.get()).clamp(1, 16);
        let results: Vec<(String, Option<SealOutcome>)> = futures::stream::iter(weighted)
            .map(|(_, label, table)| async move {
                if tokio::time::Instant::now() >= until {
                    return (label, None);
                }
                match tokio::time::timeout_at(until, table.seal_snapshot(SealKind::Mandatory)).await {
                    Ok(outcome) => (label, Some(outcome)),
                    Err(_) => (label, None),
                }
            })
            .buffer_unordered(parallel)
            .collect()
            .await;
        let mut report = ShutdownReport::default();
        for (name, outcome) in results {
            match outcome {
                Some(outcome) => report.sealed.push(Sealed { name, outcome }),
                None => report.skipped.push(name),
            }
        }
        seal::note_skipped_shutdown(report.skipped.len() as u64);
        report
    }
}

/// The file sets `relation` stands for: a plain table's own, or each
/// materialized partition's, by pid. A partitioned table's shell has none.
fn seal_targets(
    name: &str,
    relation: &Relation,
) -> Result<Vec<(String, Arc<Table<DbStorageProvider>>)>, String> {
    match relation {
        Relation::Plain(table) => Ok(if table.is_shell() {
            Vec::new()
        } else {
            vec![(name.to_string(), table.clone())]
        }),
        Relation::Partitioned(pt) => {
            let epoch = pt.epoch().map_err(|e| e.to_string())?;
            Ok(pt
                .materialized(&epoch, &crate::core::partition::route::Route::All)
                .map_err(|e| e.to_string())?
                .into_iter()
                .map(|(pid, table)| (format!("{name} (partition {pid})"), table))
                .collect())
        }
    }
}
