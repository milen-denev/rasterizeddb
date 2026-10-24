//! Conflict retries as real waits (slice B6, finding L1;
//! docs/persistent-indexes.md §7 "As built (B6)").
//!
//! A write that meets another transaction's unfinished work (a version it
//! retired, `Table::check_write_conflict`; a key it is writing, the in-flight
//! UNIQUE checks) fails with the `[retryable]` marker, having applied nothing,
//! and the three retry sites replay it: the autocommit loop
//! (`execute_with_sync`), the block loop (`execute_in_txn`) and a multi-row
//! insert's per-tuple loop (`insert_values_batch`). Each used to sleep blind
//! between attempts, outside the wait-for graph, so a cycle through a retry
//! and a table or row lock was invisible: in V10 case 9 the autocommit writer
//! spun out its budget and got 40001 while the block's CREATE INDEX waited on
//! its ROW EXCLUSIVE.
//!
//! Now the failing check names the transaction it met (the **blocker**)
//! through a task-local side channel ([`note_blocker`], read back by
//! [`capture`]; never parsed out of the message), and the retry waits on that
//! transaction as PostgreSQL's `XactLockTableWait` does:
//! - woken when it ends (`TransactionManager::watch_ends`), then replayed;
//! - with a wait-for edge to the blocker's owner once the wait has lasted
//!   `deadlock_timeout`, checked on every poll (`RowLocks::
//!   conflict_deadlock_victim`), so a cycle through it and a table or row
//!   lock ends in one 40P01, to the victim the owner-first rule picks;
//! - ended by cancellation (57014) and by the session's `lock_timeout`
//!   (55P03), as any lock wait.
//!
//! **The budget stays.** PostgreSQL waits as long as the blocker lives. Here
//! a statement's conflict waits are bounded together by
//! [`CONFLICT_WAIT_BUDGET`] from its first conflict, the bound the blind
//! back-off had (about 1.5 s), after which the conflict is reported (40001)
//! as before: a block that conflicts with another block it is itself waited
//! on by its client (`write_conflicts::a_conflict_inside_an_explicit_block_is_
//! reported`) would otherwise hang. A wait on a live blocker is not counted
//! against `MAX_CONFLICT_RETRIES`; a replay whose blocker had already ended
//! (a `REPEATABLE READ` block replaying against a committed version, a race
//! with the commit) is, and runs at once; a conflict that named no blocker
//! falls back to the blind back-off.

use std::cell::Cell;
use std::future::Future;
use std::time::{Duration, Instant};

use crate::core::database::Database;
use crate::core::mvcc::TxId;
use crate::core::mvcc::transaction::{FROZEN_TX, NO_TX, TxOutcome};

/// How long a statement's conflict waits may last together, from its first
/// conflict: the blind back-off's old total (twelve doublings capped at
/// 250 ms, 1.5 s).
pub(crate) const CONFLICT_WAIT_BUDGET: Duration = Duration::from_millis(1500);

/// How often a wait re-checks its blocker, cancellation and the graph when
/// no transaction end wakes it.
const POLL: Duration = Duration::from_millis(5);

/// The retry sites, indexing `RowLocks::conflict_waits`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Site {
    /// `execute_with_sync`'s autocommit loop.
    Autocommit = 0,
    /// `execute_in_txn`'s block loop.
    Block = 1,
    /// `insert_values_batch`'s per-tuple loop.
    Batch = 2,
}

tokio::task_local! {
    static LAST_BLOCKER: Cell<TxId>;
}

/// Records the transaction a write conflict met: the `xmax` of the version it
/// could not retire, or the creator of a key in flight. A no-op outside a
/// [`capture`]. Nothing in a statement spawns a task (`owner` module), so the
/// write paths run in the capturing task.
pub(crate) fn note_blocker(tx: TxId) {
    if tx == NO_TX || tx == FROZEN_TX {
        return;
    }
    let _ = LAST_BLOCKER.try_with(|slot| slot.set(tx));
}

/// Runs `fut` (one attempt), returning its output and the last blocker noted
/// inside it. That blocker is noted in an enclosing capture too, so a retry
/// site further out (a block loop around a batch whose tuple gave up) still
/// knows whom the conflict was with.
pub(crate) async fn capture<F: Future>(fut: F) -> (F::Output, Option<TxId>) {
    let (out, blocker) = LAST_BLOCKER
        .scope(Cell::new(NO_TX), async {
            let out = fut.await;
            (out, LAST_BLOCKER.with(Cell::get))
        })
        .await;
    let blocker = (blocker != NO_TX).then_some(blocker);
    if let Some(b) = blocker {
        note_blocker(b);
    }
    (out, blocker)
}

/// What a retry site does after a conflict, as [`Pacing::pause`] decides.
#[derive(Debug)]
pub(crate) enum Pause {
    /// Replay the statement (or tuple) now.
    Retry,
    /// Report the conflict: the budget or the replay count ran out.
    GiveUp,
    /// Fail with this message instead: a deadlock (40P01), cancellation
    /// (57014) or `lock_timeout` (55P03).
    Fail(String),
}

/// One retry site's pacing across a statement's attempts.
pub(crate) struct Pacing {
    site: Site,
    /// When the first conflict was met: the budget and `deadlock_timeout`
    /// are measured from it.
    since: Option<Instant>,
    /// Replays not preceded by a wait on a live blocker.
    blind: usize,
}

impl Pacing {
    pub(crate) fn new(site: Site) -> Pacing {
        Pacing { site, since: None, blind: 0 }
    }

    /// After a conflict that `waiter` (an owner id: a block's root, an
    /// autocommit statement's O) met with `blocker`: waits for the blocker
    /// to end, or backs off blind when there is none to wait for, and says
    /// what to do next. `max_blind` bounds the replays that waited for
    /// nothing.
    pub(crate) async fn pause(
        &mut self,
        database: &Database,
        waiter: TxId,
        blocker: Option<TxId>,
        max_blind: usize,
    ) -> Pause {
        let since = *self.since.get_or_insert_with(Instant::now);
        let transactions = &database.transactions;
        let live = |tx: TxId| transactions.outcome(tx) == TxOutcome::InProgress;
        // A blocker of the waiter's own family (a savepoint's key in flight,
        // met by a later savepoint of the same block) never ends while the
        // waiter waits.
        let blocker = blocker.filter(|b| database.row_locks.owner_of(*b) != waiter);
        match blocker {
            Some(blocker) if live(blocker) => {
                if since.elapsed() >= CONFLICT_WAIT_BUDGET {
                    return Pause::GiveUp;
                }
                wait_for(database, self.site, waiter, blocker, since).await
            }
            known => {
                self.blind += 1;
                if self.blind > max_blind || since.elapsed() >= CONFLICT_WAIT_BUDGET {
                    return Pause::GiveUp;
                }
                // A blocker that has already ended: the replay can run now.
                // None named: the old blind back-off.
                if known.is_none() {
                    tokio::time::sleep(blind_backoff(self.blind)).await;
                }
                Pause::Retry
            }
        }
    }
}

/// The blind back-off before replay `n`: doubling, capped per sleep.
fn blind_backoff(n: usize) -> Duration {
    Duration::from_millis((1u64 << n.min(8)).min(250))
}

/// Waits for `blocker` to end: see the module comment.
async fn wait_for(database: &Database, site: Site, waiter: TxId, blocker: TxId, since: Instant) -> Pause {
    let transactions = &database.transactions;
    let row_locks = &database.row_locks;
    let live = |tx: TxId| transactions.outcome(tx) == TxOutcome::InProgress;
    row_locks.conflict_waits.waits[site as usize].fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    let started = Instant::now();
    let lock_timeout = crate::core::cancel::lock_timeout();
    let watch = transactions.watch_ends();
    // The edge goes when the wait does, however it ends.
    let _edge = row_locks.waiting_guard(waiter);
    loop {
        let notified = watch.notified();
        tokio::pin!(notified);
        // Enabled before the blocker is checked, so an end between the check
        // and the await still wakes this wait.
        notified.as_mut().enable();
        if !live(blocker) {
            row_locks.conflict_waits.woken[site as usize].fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            return Pause::Retry;
        }
        if let Some(reason) = crate::core::cancel::tripped() {
            return Pause::Fail(reason.eval_error().as_str().to_string());
        }
        if let Some(limit) = lock_timeout.limit()
            && started.elapsed() >= limit
        {
            return Pause::Fail(format!(
                "canceling statement due to lock timeout (waiting for transaction {blocker})"
            ));
        }
        if since.elapsed() >= CONFLICT_WAIT_BUDGET {
            return Pause::GiveUp;
        }
        if since.elapsed() >= crate::core::mvcc::row_locks::DEADLOCK_TIMEOUT {
            row_locks.set_waiting(waiter, &[blocker]);
            if row_locks.conflict_deadlock_victim(waiter, &live) {
                return Pause::Fail(format!(
                    "deadlock detected: transaction {waiter} waits for transaction {blocker}, \
                     which is waiting, through a cycle, for transaction {waiter}"
                ));
            }
        }
        tokio::select! {
            _ = &mut notified => {}
            _ = tokio::time::sleep(POLL) => {}
        }
    }
}

#[cfg(test)]
#[path = "conflict_wait_tests.rs"]
mod tests;
