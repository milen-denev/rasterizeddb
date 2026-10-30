//! The two instants PostgreSQL's date/time functions are read from.
//!
//! `now()`, `CURRENT_TIMESTAMP`, `transaction_timestamp()`, `LOCALTIMESTAMP`
//! and `CURRENT_DATE`/`CURRENT_TIME` are the time the *transaction* started:
//! one value for every row and every statement of it. `statement_timestamp()`
//! is the time the current statement started. Only `clock_timestamp()` reads
//! the wall clock at the call. Every one of them read the wall clock here, so
//! `INSERT … VALUES (now()), (now())` stored a different time in each row and
//! two statements of one `BEGIN` block disagreed about what "now" was.
//!
//! The session sets both instants for each statement it runs ([`scope`]): the
//! open block's start for the transaction, or the statement's own start under
//! autocommit, which is PostgreSQL's answer too. A task-local, as the
//! statement's cancel signal is, because the readers are deep in the
//! evaluator and the default-value code; it does not cross `task::spawn`, so a
//! spawned worker that evaluates expressions takes the statement's clock with
//! it ([`inherit`]). Outside a statement (a test driving the engine directly,
//! a background task) each reads the wall clock, as before.

use std::future::Future;

/// One statement's instants, microseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatementClock {
    /// When the transaction began: `now()`.
    pub transaction_us: i64,
    /// When the statement began: `statement_timestamp()`.
    pub statement_us: i64,
}

impl StatementClock {
    /// A statement starting now, in a transaction that started at
    /// `transaction_us`, or now when it has none of its own yet.
    pub fn starting(transaction_us: Option<i64>) -> StatementClock {
        let statement_us = wall_us();
        StatementClock {
            transaction_us: transaction_us.unwrap_or(statement_us),
            statement_us,
        }
    }
}

tokio::task_local! {
    static CLOCK: Option<StatementClock>;
}

/// The wall clock, microseconds since the Unix epoch: `clock_timestamp()`.
pub fn wall_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// The running statement's clock, if one is set.
pub fn current() -> Option<StatementClock> {
    CLOCK.try_with(|c| *c).ok().flatten()
}

/// `now()`: the transaction's start, or the wall clock outside a statement.
pub fn transaction_us() -> i64 {
    current().map_or_else(wall_us, |c| c.transaction_us)
}

/// `statement_timestamp()`: the statement's start, or the wall clock outside
/// a statement.
pub fn statement_us() -> i64 {
    current().map_or_else(wall_us, |c| c.statement_us)
}

/// Runs `f` as a statement reading `clock`.
pub fn scope<F: Future>(clock: StatementClock, f: F) -> impl Future<Output = F::Output> {
    CLOCK.scope(Some(clock), f)
}

/// `f`, run on a thread of its own (a `std::thread::scope` worker), under
/// `clock`, the statement's as [`current`] read it on the thread that spawned
/// the worker. A task-local is not there otherwise: a parallel aggregate's
/// fold workers read `now()` off the wall clock, so `count(*) FILTER (WHERE t
/// = now())` counted only the rows its own thread folded.
pub fn on_thread<R>(clock: Option<StatementClock>, f: impl FnOnce() -> R) -> R {
    CLOCK.sync_scope(clock, f)
}

/// `f`, to be spawned, under the clock of the statement spawning it (if any):
/// a worker evaluating `now()` for its rows reads the statement's instant.
pub fn inherit<F: Future>(f: F) -> impl Future<Output = F::Output> {
    CLOCK.scope(current(), f)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_statement_reads_its_own_instants_and_a_spawned_worker_inherits_them() {
        let clock = StatementClock { transaction_us: 1_000, statement_us: 2_000 };
        scope(clock, async {
            assert_eq!((transaction_us(), statement_us()), (1_000, 2_000));
            let worker = tokio::spawn(inherit(async { (transaction_us(), statement_us()) }));
            assert_eq!(worker.await.unwrap(), (1_000, 2_000));
            // Without `inherit` a spawned task has no statement.
            let bare = tokio::spawn(async { current() });
            assert_eq!(bare.await.unwrap(), None);
        })
        .await;
        // Outside a statement: the wall clock.
        assert!(transaction_us() > 1_600_000_000_000_000);
        assert_eq!(current(), None);
    }

    #[test]
    fn a_statement_with_no_transaction_of_its_own_starts_one() {
        let c = StatementClock::starting(None);
        assert_eq!(c.transaction_us, c.statement_us);
        let c = StatementClock::starting(Some(5));
        assert_eq!(c.transaction_us, 5);
        assert!(c.statement_us > 5);
    }
}
