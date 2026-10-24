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
    /// A `CancelRequest` carrying this session's key arrived.
    UserRequest,
}

impl CancelReason {
    /// The scan-side error this reason is reported as.
    pub const fn eval_error(self) -> EvalError {
        match self {
            CancelReason::Timeout => EvalError::StatementTimeout,
            CancelReason::ClientWentAway => EvalError::ClientWentAway,
            CancelReason::UserRequest => EvalError::UserCanceled,
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
    /// The session's `lock_timeout` in milliseconds, for any table-lock wait
    /// the statement makes; `0`, the default, is no bound (PostgreSQL's
    /// meaning, never "fail at once").
    lock_timeout_ms: u64,
    /// The end of the statement's wire reply, held until its lock owner has
    /// released (pgwire's `ReplyHold`); `None` when nothing is held back.
    held_reply: Option<parking_lot::Mutex<HeldReply>>,
}

/// Wire bytes a statement's reply ends with, kept back until the statement's
/// table locks are released (see `pgwire::ReplyHold`). Carried by the
/// statement's signal because the signal is already scoped to exactly the
/// statement, outside its lock owner's scope: a scope of its own would wrap
/// (and copy) the statement's future once more.
#[derive(Debug, Default)]
pub struct HeldReply {
    pub bytes: Vec<u8>,
    /// Holding has begun: every later message is held too.
    pub holding: bool,
    /// An `ErrorResponse` is among the held bytes.
    pub error: bool,
}

impl std::fmt::Debug for StatementCancel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementCancel")
            .field("deadline", &self.deadline)
            .field("flagged", &self.flagged)
            .field("watching", &self.gone.is_some())
            .field("lock_timeout_ms", &self.lock_timeout_ms)
            .finish()
    }
}

impl StatementCancel {
    pub fn new(deadline: Option<Instant>) -> Self {
        Self {
            deadline,
            flagged: AtomicU8::new(0),
            gone: None,
            lock_timeout_ms: 0,
            held_reply: None,
        }
    }

    /// Holds back the end of this statement's reply (see [`HeldReply`]).
    pub fn holding_reply(mut self) -> Self {
        self.held_reply = Some(parking_lot::Mutex::new(HeldReply::default()));
        self
    }

    /// The held reply, when this statement holds one back.
    pub fn held_reply(&self) -> Option<&parking_lot::Mutex<HeldReply>> {
        self.held_reply.as_ref()
    }

    /// Bounds this statement's table-lock waits by `lock_timeout`
    /// (milliseconds; `0` is no bound).
    pub fn with_lock_timeout(mut self, ms: u64) -> Self {
        self.lock_timeout_ms = ms;
        self
    }

    /// This statement's `lock_timeout`, as a wait reads it.
    pub fn lock_timeout(&self) -> crate::core::mvcc::table_locks::LockTimeout {
        crate::core::mvcc::table_locks::LockTimeout::from_setting_ms(self.lock_timeout_ms)
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
            CancelReason::UserRequest => 3,
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
            3 => return Some(CancelReason::UserRequest),
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

/// Runs `fut` as a statement whose table-lock waits are bounded by
/// `lock_timeout = ms`, for tests driving the engine below the wire.
#[cfg(any(test, feature = "test_tokens"))]
pub async fn with_lock_timeout<F: std::future::Future>(ms: u64, fut: F) -> F::Output {
    STATEMENT_CANCEL
        .scope(Arc::new(StatementCancel::new(None).with_lock_timeout(ms)), fut)
        .await
}

/// The current statement's `lock_timeout`; no bound outside a statement.
pub fn lock_timeout() -> crate::core::mvcc::table_locks::LockTimeout {
    STATEMENT_CANCEL
        .try_with(|c| c.lock_timeout())
        .unwrap_or(crate::core::mvcc::table_locks::LockTimeout::Unbounded)
}

// ------------------------------------------------------------ CancelRequest

/// One connection's cancellation target: the secret key it was handed in
/// `BackendKeyData`, and the statement it is running right now, if any.
///
/// The slot holds the running statement's own [`StatementCancel`], never a
/// session-wide flag. A `CancelRequest` flags exactly the statement it found in
/// the slot, so one that arrives after that statement has finished trips a
/// signal nothing reads any more — it can never stop the *next* statement.
pub struct SessionCancel {
    key: [u8; 4],
    current: std::sync::Mutex<Option<Arc<StatementCancel>>>,
}

impl SessionCancel {
    /// Makes `statement` the one a `CancelRequest` for this session stops,
    /// until the returned guard is dropped.
    ///
    /// The guard clears the slot on every way out of the statement — a
    /// return, an error, the future being dropped — and only if the slot
    /// still holds *this* statement, so a guard outliving its statement
    /// cannot clear its successor.
    pub fn publish(self: &Arc<Self>, statement: Arc<StatementCancel>) -> PublishedStatement {
        *self.current.lock().unwrap_or_else(|e| e.into_inner()) = Some(statement.clone());
        PublishedStatement {
            session: self.clone(),
            statement,
        }
    }
}

/// Clears a session's cancel slot when its statement ends. See
/// [`SessionCancel::publish`].
pub struct PublishedStatement {
    session: Arc<SessionCancel>,
    statement: Arc<StatementCancel>,
}

impl Drop for PublishedStatement {
    fn drop(&mut self) {
        let mut slot = self.session.current.lock().unwrap_or_else(|e| e.into_inner());
        if slot.as_ref().is_some_and(|s| Arc::ptr_eq(s, &self.statement)) {
            *slot = None;
        }
    }
}

/// A connection's entry in the cancel registry, removed when dropped.
pub struct CancelRegistration {
    /// The "process id" sent in `BackendKeyData`: unique per live session in
    /// this process, which a real process id is not — every session here
    /// shares one.
    pub pid: i32,
    /// The secret half, as sent.
    pub key: i32,
    pub session: Arc<SessionCancel>,
}

impl Drop for CancelRegistration {
    fn drop(&mut self) {
        registry().lock().unwrap_or_else(|e| e.into_inner()).remove(&self.pid);
    }
}

fn registry() -> &'static std::sync::Mutex<std::collections::HashMap<i32, Arc<SessionCancel>>> {
    static REGISTRY: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<i32, Arc<SessionCancel>>>,
    > = std::sync::OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

/// Registers a new session under a fresh `(pid, key)`. `key` must come from a
/// cryptographic generator: it is all that stands between any client able to
/// open a connection and every other session's running statement.
pub fn register_session(key: [u8; 4]) -> CancelRegistration {
    static NEXT_PID: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(1);
    let session = Arc::new(SessionCancel {
        key,
        current: std::sync::Mutex::new(None),
    });
    let mut map = registry().lock().unwrap_or_else(|e| e.into_inner());
    let pid = loop {
        // Positive and nonzero, and not one a live session already holds.
        let candidate = NEXT_PID.fetch_add(1, Ordering::Relaxed) & 0x7fff_ffff;
        if candidate != 0 && !map.contains_key(&candidate) {
            break candidate;
        }
    };
    map.insert(pid, session.clone());
    CancelRegistration {
        pid,
        key: i32::from_be_bytes(key),
        session,
    }
}

/// Answers a `CancelRequest`: stops the statement session `pid` is running, if
/// `key` is that session's key and a statement is running. Returns whether one
/// was flagged.
///
/// The key is compared in constant time, so the time a refusal takes says
/// nothing about how much of a guess was right. Nothing is reported back to
/// the requester either way, as in PostgreSQL.
pub fn cancel_request(pid: i32, key: i32) -> bool {
    let Some(session) = registry()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&pid)
        .cloned()
    else {
        return false;
    };
    if !keys_match(&session.key, &key.to_be_bytes()) {
        return false;
    }
    let running = session
        .current
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    match running {
        Some(statement) => {
            statement.flag(CancelReason::UserRequest);
            true
        }
        None => false,
    }
}

fn keys_match(a: &[u8; 4], b: &[u8; 4]) -> bool {
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    std::hint::black_box(diff) == 0
}

#[cfg(test)]
mod cancel_request_tests {
    use super::*;

    #[test]
    fn a_request_stops_only_the_statement_running_when_it_arrives() {
        let reg = register_session([1, 2, 3, 4]);
        // Nothing running: nothing to stop.
        assert!(!cancel_request(reg.pid, reg.key));

        let first = Arc::new(StatementCancel::new(None));
        let published = reg.session.publish(first.clone());
        // A wrong key does nothing.
        assert!(!cancel_request(reg.pid, reg.key ^ 1));
        assert_eq!(first.tripped(), None);
        assert!(cancel_request(reg.pid, reg.key));
        assert_eq!(first.tripped(), Some(CancelReason::UserRequest));
        drop(published);

        // The next statement starts clean, and a late request finds nothing.
        let second = Arc::new(StatementCancel::new(None));
        let _p2 = reg.session.publish(second.clone());
        assert_eq!(second.tripped(), None);
        drop(_p2);
        assert!(!cancel_request(reg.pid, reg.key));
        assert_eq!(second.tripped(), None);
    }

    #[test]
    fn a_stale_guard_does_not_clear_its_successor() {
        let reg = register_session([9, 9, 9, 9]);
        let a = Arc::new(StatementCancel::new(None));
        let b = Arc::new(StatementCancel::new(None));
        let guard_a = reg.session.publish(a);
        let _guard_b = reg.session.publish(b.clone());
        drop(guard_a);
        assert!(cancel_request(reg.pid, reg.key), "b is still published");
        assert_eq!(b.tripped(), Some(CancelReason::UserRequest));
    }

    #[test]
    fn pids_are_unique_and_deregistered() {
        let a = register_session([0; 4]);
        let b = register_session([0; 4]);
        assert_ne!(a.pid, b.pid);
        let pid = a.pid;
        let key = a.key;
        drop(a);
        assert!(!cancel_request(pid, key));
    }
}
