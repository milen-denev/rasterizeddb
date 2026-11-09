//! Automatic compaction: `VACUUM FULL` on its own, for the tables that need it.
//!
//! An `UPDATE` writes a new version into a new pointer slot and tombstones the
//! old one, and the reclamation service (`core::vacuum`) gives back row bytes
//! but never slots: their offsets are the filter index's addresses. So a table
//! whose rows are updated and never added to — a heartbeat, a counter, a lease
//! — grows its pointer file forever while its live rows stay put. Measured on
//! a production node: 8 live rows in 38 030 slots, 1 012 in 472 468, every
//! scan of them linear in the slots. Only `VACUUM FULL` (`Table::compact`)
//! rewrites the slots, and nothing ran it.
//!
//! This task runs it, on a table whose slots are at least
//! [`crate::auto_compact_min_slots`] and at least
//! [`crate::auto_compact_ratio`] times its live rows (as the reclamation
//! sweep last measured them, `VacuumStats::live_rows`), no more often than
//! [`crate::auto_compact_cooldown_secs`] per table. It is the statement
//! itself, through the executor: the same ACCESS EXCLUSIVE and the same
//! interaction with backups and replication as an operator's `VACUUM FULL
//! <table>`, nothing of its own. Plain tables only: a partitioned table is
//! left to its operator ([`measure`]).
//!
//! Compaction stops the table while it runs, so a busy table is skipped
//! rather than queued for: the statement runs with a short `lock_timeout`
//! ([`LOCK_WAIT_MS`]), and a table whose lock is not free at once is tried
//! again next interval. Kept small by the threshold, a run is brief (about
//! 0.4 s at 20 000 slots); a table that already holds far more pays one long
//! run the first time.
//!
//! The task holds the catalogue's database map weakly and ends when the
//! catalogue does.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use log::{info, warn};

use crate::core::database::Database;

/// How long the compaction waits for its table's lock before skipping it.
pub const LOCK_WAIT_MS: u64 = 100;

/// Compactions this process ran on its own.
pub static RUNS: AtomicU64 = AtomicU64::new(0);
/// Due compactions skipped because the table's lock was not free.
pub static SKIPPED_BUSY: AtomicU64 = AtomicU64::new(0);

/// A table's slots and live rows, and whether that makes it due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Measure {
    pub slots: u64,
    pub live: u64,
}

impl Measure {
    /// Due when the slots pass the floor and outnumber the live rows by the
    /// ratio. A table whose live rows are unmeasured (no complete sweep yet)
    /// is never due: slots alone cannot tell dead from live.
    pub fn due(&self, min_slots: u64, ratio: u64) -> bool {
        self.slots >= min_slots && self.slots >= ratio.max(1).saturating_mul(self.live.max(1))
    }
}

/// The table's measure, `None` when it cannot be read, its live rows were
/// never measured, or it is partitioned: a partitioned table's `VACUUM FULL`
/// compacts every partition and would open closed ones to do it, which is an
/// operator's call, not this task's.
pub async fn measure(database: &Database, name: &str) -> Option<Measure> {
    use crate::core::database::Relation;
    use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
    use crate::core::storage_providers::traits::StorageIO;
    let Relation::Plain(table) = database.relation(name)? else { return None };
    let data = table.data().ok()?;
    let slots = data.io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64;
    let live = data.vacuum_stats.live_rows()?;
    Some(Measure { slots, live: live.min(slots) })
}

/// Starts the task over `databases` (the catalogue's map, held weakly).
pub fn spawn<M>(databases: std::sync::Weak<M>, all: fn(&M) -> Vec<rclite::Arc<Database>>)
where
    M: Send + Sync + 'static,
{
    if !crate::auto_compact_enabled() {
        return;
    }
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    handle.spawn(async move {
        let mut last: HashMap<(u64, String), Instant> = HashMap::new();
        loop {
            tokio::time::sleep(Duration::from_secs(crate::auto_compact_interval_secs().max(1))).await;
            let Some(map) = databases.upgrade() else { return };
            let all_databases = all(&map);
            drop(map);
            for database in all_databases {
                tick(&database, &mut last).await;
            }
        }
    });
}

/// One pass over one database.
async fn tick(database: &rclite::Arc<Database>, last: &mut HashMap<(u64, String), Instant>) {
    let (min_slots, ratio) = (crate::auto_compact_min_slots(), crate::auto_compact_ratio());
    let cooldown = Duration::from_secs(crate::auto_compact_cooldown_secs());
    let mut names: Vec<String> = database.table_names();
    names.sort();
    names.dedup();
    for name in names {
        // A statement's scratch relation and a rewrite's backup are not the
        // database's tables.
        if name.starts_with(crate::core::rql::cte::TEMP_TABLE_PREFIX)
            || name.starts_with(crate::core::database::rewrite::BACKUP_PREFIX)
        {
            continue;
        }
        let key = (database.id, name.clone());
        if last.get(&key).is_some_and(|at| at.elapsed() < cooldown) {
            continue;
        }
        let Some(m) = measure(database, &name).await else { continue };
        if !m.due(min_slots, ratio) {
            continue;
        }
        match compact(database, &name).await {
            Outcome::Done => {
                RUNS.fetch_add(1, Ordering::Relaxed);
                let after = measure(database, &name).await.map(|m| m.slots);
                info!(
                    "auto-compacted \"{name}\": {} slots for {} live rows, now {}",
                    m.slots,
                    m.live,
                    after.map_or_else(|| "?".to_string(), |s| s.to_string())
                );
                last.insert(key, Instant::now());
            }
            Outcome::Busy => {
                SKIPPED_BUSY.fetch_add(1, Ordering::Relaxed);
            }
            Outcome::Failed(e) => {
                warn!("auto-compaction of \"{name}\" failed: {e}");
                // Not retried every interval: a failure that recurs would
                // otherwise log once a minute for ever.
                last.insert(key, Instant::now());
            }
        }
    }
}

enum Outcome {
    Done,
    Busy,
    Failed(String),
}

/// `VACUUM FULL "<name>"`, as a statement, bounded by [`LOCK_WAIT_MS`].
async fn compact(database: &rclite::Arc<Database>, name: &str) -> Outcome {
    let sql = format!("VACUUM FULL \"{}\"", name.replace('"', "\"\""));
    let Some(purpose) = crate::core::rql::lexer_s1::recognize_query_purpose(&sql) else {
        return Outcome::Failed(format!("could not read {sql}"));
    };
    let cancel = std::sync::Arc::new(crate::core::cancel::StatementCancel::new(None).with_lock_timeout(LOCK_WAIT_MS));
    let response = crate::core::cancel::STATEMENT_CANCEL
        .scope(cancel, crate::core::rql::executor::execute(purpose, database.clone()))
        .await;
    match response.first() {
        Some(3) => {
            let message = String::from_utf8_lossy(&response[1..]).to_string();
            if message.contains("lock timeout") {
                Outcome::Busy
            } else {
                Outcome::Failed(message)
            }
        }
        _ => Outcome::Done,
    }
}

#[cfg(test)]
#[path = "auto_compact_tests.rs"]
mod tests;
