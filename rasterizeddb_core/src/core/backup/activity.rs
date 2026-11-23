//! What the backup service last tried, how it went, and when it will try next.
//!
//! The catalogue records what *succeeded*: a snapshot that failed leaves no
//! entry, so a bucket that stopped accepting uploads looks, from the
//! catalogue, like a quiet database. `SHOW BACKUP STATUS` reads this record to
//! tell the two apart — the last attempt of each kind with its outcome, the
//! last error's text, and the next time each schedule fires.
//!
//! In memory only: it describes this process. After a restart every attempt
//! reads "none since start" until the first one runs, and the catalogue still
//! answers what was last achieved.

use std::io;
use std::sync::Mutex;

use super::timeutil;

/// The operations whose attempts are recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// A snapshot, whoever asked for it: `BACKUP DATABASE`, the schedule, or a
    /// rollover (which takes one first).
    Snapshot,
    /// An upload of the staged log: `BACKUP WAL`, the flush interval, the
    /// bundle-size trigger, or the archive step of a snapshot.
    WalFlush,
    /// A rollover: a full snapshot, then deleting every earlier generation.
    Rollover,
    /// Reading the catalogue from the bucket, retried in the background when
    /// the server started without reaching it (`--s3-start-offline`).
    Catalogue,
}

impl Operation {
    pub fn name(self) -> &'static str {
        match self {
            Operation::Snapshot => "snapshot",
            Operation::WalFlush => "wal_flush",
            Operation::Rollover => "rollover",
            Operation::Catalogue => "catalogue_load",
        }
    }
}

/// One attempt. `outcome` is `None` while it runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attempt {
    pub started_at: u64,
    pub finished_at: Option<u64>,
    /// `Ok` carries a summary of what it did, `Err` the error's text.
    pub outcome: Option<Result<String, String>>,
}

/// The newest failure of any operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub at: u64,
    pub operation: Operation,
    pub message: String,
}

/// When a schedule next fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Next {
    /// No task runs this operation: its interval is 0, or the service has not
    /// started its background tasks (a restore, a test).
    #[default]
    NotScheduled,
    /// Unix seconds.
    At(u64),
    /// Running now; the next time is set when it finishes.
    Now,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Activity {
    pub snapshot: Option<Attempt>,
    pub wal_flush: Option<Attempt>,
    pub rollover: Option<Attempt>,
    pub catalogue: Option<Attempt>,
    pub last_error: Option<Failure>,
    pub next_snapshot: Next,
    pub next_wal_flush: Next,
    pub next_rollover: Next,
    pub next_catalogue: Next,
    /// Why the next snapshot is when it is, when that is not the interval:
    /// the base snapshot is being retried after a failure.
    pub next_snapshot_reason: Option<String>,
}

impl Activity {
    pub fn attempt(&self, operation: Operation) -> Option<&Attempt> {
        match operation {
            Operation::Snapshot => self.snapshot.as_ref(),
            Operation::WalFlush => self.wal_flush.as_ref(),
            Operation::Rollover => self.rollover.as_ref(),
            Operation::Catalogue => self.catalogue.as_ref(),
        }
    }

    fn attempt_mut(&mut self, operation: Operation) -> &mut Option<Attempt> {
        match operation {
            Operation::Snapshot => &mut self.snapshot,
            Operation::WalFlush => &mut self.wal_flush,
            Operation::Rollover => &mut self.rollover,
            Operation::Catalogue => &mut self.catalogue,
        }
    }

    pub fn next(&self, operation: Operation) -> Next {
        match operation {
            Operation::Snapshot => self.next_snapshot,
            Operation::WalFlush => self.next_wal_flush,
            Operation::Rollover => self.next_rollover,
            Operation::Catalogue => self.next_catalogue,
        }
    }

    fn next_mut(&mut self, operation: Operation) -> &mut Next {
        match operation {
            Operation::Snapshot => &mut self.next_snapshot,
            Operation::WalFlush => &mut self.next_wal_flush,
            Operation::Rollover => &mut self.next_rollover,
            Operation::Catalogue => &mut self.next_catalogue,
        }
    }
}

/// The record, shared by the service's operations and its background tasks.
#[derive(Debug, Default)]
pub struct ActivityLog {
    inner: Mutex<Activity>,
}

impl ActivityLog {
    pub fn snapshot(&self) -> Activity {
        self.lock().clone()
    }

    /// Marks `operation` as running, and returns the guard that records how it
    /// ended. Call it once the operation holds its own lock, so a caller queued
    /// behind a running one does not overwrite that one's start.
    pub fn begin(&self, operation: Operation) -> AttemptGuard<'_> {
        *self.lock().attempt_mut(operation) = Some(Attempt {
            started_at: timeutil::now_unix(),
            finished_at: None,
            outcome: None,
        });
        AttemptGuard {
            log: self,
            operation,
            done: false,
        }
    }

    pub fn schedule(&self, operation: Operation, next: Next) {
        *self.lock().next_mut(operation) = next;
    }

    pub fn schedule_snapshot(&self, next: Next, reason: Option<String>) {
        let mut activity = self.lock();
        activity.next_snapshot = next;
        activity.next_snapshot_reason = reason;
    }

    fn end(&self, operation: Operation, outcome: Result<String, String>) {
        let now = timeutil::now_unix();
        let mut activity = self.lock();
        if let Err(message) = &outcome {
            activity.last_error = Some(Failure {
                at: now,
                operation,
                message: message.clone(),
            });
        }
        let current = activity.attempt_mut(operation);
        match current {
            Some(attempt) if attempt.outcome.is_none() => {
                attempt.finished_at = Some(now);
                attempt.outcome = Some(outcome);
            }
            // Begun without an attempt is not reachable; record it whole anyway
            // rather than lose an outcome.
            _ => {
                *current = Some(Attempt {
                    started_at: now,
                    finished_at: Some(now),
                    outcome: Some(outcome),
                })
            }
        }
    }

    /// Records a failure that happened outside a guarded attempt but belongs
    /// to `operation`: a log that refused to be cut while the upload after it
    /// went through. The attempt reads as failed, which is what the caller
    /// was told.
    pub fn fail_last(&self, operation: Operation, message: String) {
        let now = timeutil::now_unix();
        let mut activity = self.lock();
        activity.last_error = Some(Failure {
            at: now,
            operation,
            message: message.clone(),
        });
        match activity.attempt_mut(operation) {
            Some(attempt) => {
                attempt.finished_at = Some(now);
                attempt.outcome = Some(Err(message));
            }
            current => {
                *current = Some(Attempt {
                    started_at: now,
                    finished_at: Some(now),
                    outcome: Some(Err(message)),
                })
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Activity> {
        // A panic while holding it leaves a record, not an invariant; the
        // status is still worth reading.
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Records an attempt's outcome. Dropped without [`AttemptGuard::finish`] —
/// the statement running it was cancelled, or the future was dropped at
/// shutdown — it records that, so the attempt does not read "running" for
/// the rest of the process's life.
pub struct AttemptGuard<'a> {
    log: &'a ActivityLog,
    operation: Operation,
    done: bool,
}

impl AttemptGuard<'_> {
    pub fn finish<T>(mut self, result: &io::Result<T>, summary: impl FnOnce(&T) -> String) {
        self.done = true;
        let outcome = match result {
            Ok(value) => Ok(summary(value)),
            Err(e) => Err(e.to_string()),
        };
        self.log.end(self.operation, outcome);
    }
}

impl Drop for AttemptGuard<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.log.end(
                self.operation,
                Err("abandoned before it finished: the statement running it was cancelled, \
                     or the server stopped"
                    .to_string()),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_attempt_reads_running_then_its_outcome() {
        let log = ActivityLog::default();
        assert_eq!(log.snapshot().snapshot, None);
        let guard = log.begin(Operation::Snapshot);
        let running = log.snapshot().snapshot.expect("begun");
        assert_eq!(running.outcome, None);
        assert_eq!(running.finished_at, None);
        guard.finish(&Ok::<u32, io::Error>(7), |n| format!("{n} files"));
        let done = log.snapshot().snapshot.expect("finished");
        assert_eq!(done.outcome, Some(Ok("7 files".to_string())));
        assert!(done.finished_at.is_some());
        assert_eq!(log.snapshot().last_error, None);
    }

    #[test]
    fn a_failure_is_the_last_error_and_survives_a_later_success() {
        let log = ActivityLog::default();
        log.begin(Operation::WalFlush)
            .finish(&Err::<(), _>(io::Error::other("bucket said 403")), |_| String::new());
        let activity = log.snapshot();
        assert_eq!(
            activity.wal_flush.as_ref().and_then(|a| a.outcome.clone()),
            Some(Err("bucket said 403".to_string()))
        );
        let failure = activity.last_error.expect("recorded");
        assert_eq!(failure.operation, Operation::WalFlush);
        assert_eq!(failure.message, "bucket said 403");

        // The attempt turns green; the last error stays, so an operator can
        // still read what went wrong.
        log.begin(Operation::WalFlush).finish(&Ok::<(), io::Error>(()), |_| "ok".to_string());
        let activity = log.snapshot();
        assert_eq!(activity.wal_flush.and_then(|a| a.outcome), Some(Ok("ok".to_string())));
        assert_eq!(activity.last_error.map(|f| f.message), Some("bucket said 403".to_string()));
    }

    #[test]
    fn a_dropped_attempt_is_recorded_as_abandoned() {
        let log = ActivityLog::default();
        drop(log.begin(Operation::Rollover));
        let outcome = log.snapshot().rollover.and_then(|a| a.outcome).expect("ended");
        assert!(outcome.as_ref().is_err_and(|m| m.starts_with("abandoned")), "{outcome:?}");
        assert_eq!(log.snapshot().last_error.map(|f| f.operation), Some(Operation::Rollover));
    }

    #[test]
    fn fail_last_turns_a_green_attempt_red() {
        let log = ActivityLog::default();
        log.begin(Operation::WalFlush).finish(&Ok::<(), io::Error>(()), |_| "uploaded".to_string());
        log.fail_last(Operation::WalFlush, "the log of db2 refused the cut".to_string());
        let activity = log.snapshot();
        assert_eq!(
            activity.wal_flush.and_then(|a| a.outcome),
            Some(Err("the log of db2 refused the cut".to_string()))
        );
        assert_eq!(activity.last_error.map(|f| f.operation), Some(Operation::WalFlush));
    }
}
