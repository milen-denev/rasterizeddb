//! Stopping a statement that should not go on running.
//!
//! Two things can decide that: `statement_timeout` expiring, and the client
//! that asked for the statement going away. Both need the same thing — a signal
//! the scan can see — so they share one.
//!
//! # Why cooperative, and not a dropped future
//!
//! Dropping the future would cancel at the next await point, which is simpler
//! and wrong here. A statement holds things that must be put back: a CTE
//! materialises into temporary tables its caller is obliged to drop on every
//! exit path, and an autocommit statement owns a transaction that must be
//! aborted rather than left `InProgress` holding back reclamation. A dropped
//! future runs none of that cleanup.
//!
//! So the scan is asked to stop, and the statement then fails through the same
//! path any other scan error takes — which already unwinds correctly, because
//! every one of those obligations is discharged on the error path today.
//!
//! # Why an error and never a short answer
//!
//! A cancelled scan must never return the rows it happened to reach. Setting
//! the scan's stop flag alone would do exactly that: the caller cannot tell a
//! scan that finished from one that gave up, so a cancelled `SELECT` would look
//! like a complete result with fewer rows. The trip therefore records an error
//! *before* it sets the stop flag, and the ordering is the point.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

use crate::core::row::error::EvalError;

/// Why a statement was asked to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReason {
    /// `statement_timeout` elapsed.
    Timeout,
    /// The connection that asked for it closed.
    ClientWentAway,
}

impl CancelReason {
    /// The scan-side error this reason is reported as.
    pub const fn eval_error(self) -> EvalError {
        match self {
            CancelReason::Timeout => EvalError::StatementTimeout,
            CancelReason::ClientWentAway => EvalError::ClientWentAway,
        }
    }
}

/// One statement's reason to stop, shared with whatever is executing it.
pub struct StatementCancel {
    /// When this statement stops being allowed to run. `None` disables it,
    /// which is what `statement_timeout = 0` means.
    deadline: Option<Instant>,
    /// Set out of band — by the connection noticing its client left. 0 is
    /// "not cancelled"; otherwise it is a `CancelReason` discriminant.
    flagged: AtomicU8,
    /// Answers "has the client gone?", cheaply and **without blocking**.
    ///
    /// A predicate rather than a socket, so this module stays unaware of how a
    /// connection is made; the wire layer supplies one that peeks at a
    /// duplicate of its own descriptor.
    ///
    /// It has to be synchronous. The obvious design — race the statement
    /// against a future watching the socket — cannot work: a scan served from
    /// memory never returns `Pending`, so the whole statement runs inside a
    /// single poll and the racing future is never polled at all. Asking here,
    /// from inside the scan, is the same place and the same reason the deadline
    /// is checked.
    gone: Option<Box<dyn Fn() -> bool + Send + Sync>>,
}

impl std::fmt::Debug for StatementCancel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementCancel")
            .field("deadline", &self.deadline)
            .field("flagged", &self.flagged)
            .field("watching", &self.gone.is_some())
            .finish()
    }
}

impl StatementCancel {
    pub fn new(deadline: Option<Instant>) -> Self {
        Self {
            deadline,
            flagged: AtomicU8::new(0),
            gone: None,
        }
    }

    /// Also stop when `gone()` reports the client has left.
    pub fn watching(mut self, gone: Box<dyn Fn() -> bool + Send + Sync>) -> Self {
        self.gone = Some(gone);
        self
    }

    /// A signal that never trips, for callers with nothing to enforce.
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self::new(None))
    }

    /// Asks this statement to stop for a reason that is not the clock.
    pub fn flag(&self, reason: CancelReason) {
        let code = match reason {
            CancelReason::Timeout => 1,
            CancelReason::ClientWentAway => 2,
        };
        // First writer wins: the first reason is the true one, and a later
        // deadline should not relabel a client that had already gone.
        let _ = self
            .flagged
            .compare_exchange(0, code, Ordering::AcqRel, Ordering::Acquire);
    }

    /// Why this statement should stop, if it should.
    ///
    /// Cheap enough for a per-batch check: an atomic load, and a clock read
    /// only when a deadline was actually set.
    pub fn tripped(&self) -> Option<CancelReason> {
        match self.flagged.load(Ordering::Acquire) {
            1 => return Some(CancelReason::Timeout),
            2 => return Some(CancelReason::ClientWentAway),
            _ => {}
        }
        if let Some(at) = self.deadline {
            if Instant::now() >= at {
                return Some(CancelReason::Timeout);
            }
        }
        match &self.gone {
            Some(check) if check() => Some(CancelReason::ClientWentAway),
            _ => None,
        }
    }
}

tokio::task_local! {
    /// The cancel signal for the statement this task is running.
    ///
    /// A task-local rather than a parameter threaded through the executor: the
    /// scan is reached through a long chain of calls that would each have to
    /// carry it, and the alternative — a registry keyed by transaction id —
    /// would outlive the statement if any exit path forgot to remove it.
    ///
    /// It does **not** cross `task::spawn`, so anything reading it must do so
    /// in the task executing the statement. The scan's batch dispatch loops run
    /// there, which is why the check lives in them rather than in the workers
    /// they spawn.
    pub static STATEMENT_CANCEL: Arc<StatementCancel>;
}

/// This statement's cancel signal, or `None` outside a statement.
pub fn current() -> Option<Arc<StatementCancel>> {
    STATEMENT_CANCEL.try_with(Arc::clone).ok()
}

/// Why the current statement should stop, if there is a signal and it tripped.
pub fn tripped() -> Option<CancelReason> {
    STATEMENT_CANCEL.try_with(|c| c.tripped()).ok().flatten()
}
