//! The background flush that asynchronous commits rely on.
//!
//! With `synchronous_commit` on, a statement takes its own `fdatasync` and this
//! has nothing to do. With it off, a commit returns as soon as its records are
//! in the page cache and the sync is owed to this task, which pays it every
//! [`crate::wal_writer_delay_ms`] milliseconds.
//!
//! That delay bounds what an **operating system crash or power loss** can take
//! with it. It is not what this process dying costs: the records are in the
//! kernel's cache by then, so a `SIGKILL` loses nothing at all.
//!
//! Losing those transactions is the deal the knob offers. Losing *half* of one
//! is not, and this is where that is kept true — see [`flush_once`] for the
//! ordering the round has to preserve.

use std::sync::Arc as StdArc;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::Mutex;

use crate::core::mvcc::TransactionManager;
use crate::core::wal;

/// One database on the writer's round.
struct Registered {
    /// Weak, so a database that goes away is dropped from the round rather than
    /// kept alive by it. Nothing else prunes this list.
    manager: Weak<TransactionManager>,
    /// The database's own directory. Its log is the one carrying the
    /// transaction outcome records, which is why the round treats it specially.
    location: String,
}

static REGISTERED: std::sync::OnceLock<Mutex<Vec<Registered>>> = std::sync::OnceLock::new();
static STARTED: AtomicBool = AtomicBool::new(false);

fn registry() -> &'static Mutex<Vec<Registered>> {
    REGISTERED.get_or_init(|| Mutex::new(Vec::new()))
}

/// Puts a database on the writer's round, starting the writer if it is not
/// already running.
///
/// Called for every database with a location. An in-memory one has no log and
/// nothing to flush, so it is not registered.
pub fn register(location: &str, manager: &StdArc<TransactionManager>) {
    {
        let mut list = registry().lock();
        // Dropped databases go here rather than in a sweep of their own; the
        // list is short and this is the only place that grows it.
        list.retain(|r| r.manager.strong_count() > 0);
        list.push(Registered {
            manager: StdArc::downgrade(manager),
            location: location.to_string(),
        });
    }
    spawn_once();
}

/// Starts the writer task, once per process.
///
/// Not started until a database with a log exists, so a process that only ever
/// opens in-memory databases — most of the test suite — never spawns it.
fn spawn_once() {
    // There is nothing to spawn onto outside a runtime. The flag is left clear
    // so a later registration from async code still starts the writer; this is
    // for tests and for tools that build a manager without one.
    if tokio::runtime::Handle::try_current().is_err() {
        return;
    }
    if STARTED.swap(true, Ordering::AcqRel) {
        return;
    }
    // A runtime is guaranteed here: databases are opened from async code.
    tokio::spawn(async move {
        loop {
            let delay = Duration::from_millis(crate::wal_writer_delay_ms());
            tokio::time::sleep(delay).await;
            // `fdatasync` blocks, and blocking a worker is what stopped the
            // log's group commit from ever batching. This task is not on the
            // statement path, but it would be holding a worker for milliseconds
            // at a time while statements wait for one.
            let _ = tokio::task::spawn_blocking(flush_once).await;
        }
    });
}

/// The directory keys ([`wal::dir_key`]) of every live database on the round:
/// the directories whose logs hold transaction outcome records. Anything that
/// syncs logs from the process-wide list syncs every other log first, then
/// these, or leaves them to their own database (see [`flush_once`]).
pub(crate) fn outcome_dirs() -> std::collections::HashSet<String> {
    registry()
        .lock()
        .iter()
        .filter(|r| r.manager.strong_count() > 0)
        .map(|r| wal::dir_key(&r.location))
        .collect()
}

/// One round: flush every log that is owed a sync, then let the outcome bytes
/// they now cover reach the status files.
///
/// **The order matters and is the same order `Database::sync_durable` uses.**
/// A database's own log holds its transaction outcome records, and must be
/// synced after every other log it may have written rows to. An outcome that
/// became durable ahead of the rows it validates would, after a crash in
/// between, name a transaction committed whose rows are missing — which is
/// corruption, not the bounded loss the knob promises.
///
/// So the round syncs in two passes: every log that is no database's own
/// first, and only then each database's own. Visiting database by database,
/// syncing "everything but this one's own" each time, is not enough: the first
/// database visited would sync every *other* database's own log in that
/// pass, before those databases' other directories had been reached.
///
/// The order covers only what was written before the round started. Sessions
/// keep appending to the other logs and staging outcomes between its two
/// halves, so it is not what keeps a transaction's outcome behind its rows on
/// another disk: `Database::commit_written_transaction` is, by syncing those
/// rows before the outcome exists to be staged — in `synchronous_commit = off`
/// too, which is the mode that leans on this round.
///
/// Errors are swallowed deliberately. Nothing is waiting on this round, a
/// failed flush leaves the records exactly where they were, and a log that
/// cannot be flushed has already poisoned itself so the next statement to ask
/// for durability fails loudly rather than lying.
fn flush_once() {
    let entries: Vec<(String, StdArc<TransactionManager>)> = {
        let list = registry().lock();
        list.iter()
            .filter_map(|r| r.manager.upgrade().map(|m| (r.location.clone(), m)))
            .collect()
    };
    if entries.is_empty() {
        return;
    }

    let own: std::collections::HashSet<String> =
        entries.iter().map(|(location, _)| wal::dir_key(location)).collect();
    for (dir, log) in wal::registered() {
        if own.contains(&dir) {
            continue;
        }
        let _ = log.sync_pending();
    }
    for (location, manager) in entries {
        if let Some(log) = wal::wal_for(&location) {
            let _ = log.sync_pending();
        }
        // Now that the log holding them is durable, the outcome bytes staged by
        // `flush_deferred` may be copied into the status file.
        let _ = manager.apply_staged();
    }
}

/// Flushes everything the writer would, without waiting for its next round.
///
/// For a caller that has just turned `synchronous_commit` back on and wants the
/// backlog it accumulated to be durable before it says so.
pub fn flush_now() {
    flush_once();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An empty round must not panic or touch anything, because it runs every
    /// tick for the life of the process.
    #[test]
    fn a_round_with_nothing_registered_does_nothing() {
        flush_once();
    }

    /// A database that has gone away must not be kept alive by the round, or
    /// every database ever opened would leak its outcome bitmap.
    #[test]
    fn a_dropped_database_leaves_the_round() {
        let manager = StdArc::new(TransactionManager::in_memory());
        {
            let mut list = registry().lock();
            list.push(Registered {
                manager: StdArc::downgrade(&manager),
                location: "/nonexistent/log-writer-test".to_string(),
            });
        }
        assert!(
            registry()
                .lock()
                .iter()
                .any(|r| r.manager.strong_count() > 0),
            "the entry was not registered"
        );

        drop(manager);
        // Registering anything prunes what has died.
        let live = StdArc::new(TransactionManager::in_memory());
        register("/nonexistent/log-writer-test-2", &live);

        assert!(
            registry()
                .lock()
                .iter()
                .all(|r| r.manager.strong_count() > 0),
            "a dropped database stayed on the round"
        );
    }
}
