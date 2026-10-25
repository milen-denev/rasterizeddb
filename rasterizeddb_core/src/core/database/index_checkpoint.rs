//! The database's clean points (docs/persistent-indexes.md §3, slice B13):
//! `rdb_index_checkpoint(regclass)`, fanned out over a partitioned table's
//! materialized partitions, and shutdown's seal of every file set. The
//! third clean point, the idle checkpoint, is each instance's own watcher
//! (`row::table::table_seal`).

use std::time::Duration;

use futures::StreamExt;
use rclite::Arc;

use super::Database;
use super::relation::Relation;
use crate::core::mvcc::table_locks::TableMode;
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
