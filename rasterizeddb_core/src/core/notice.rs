//! Notices a statement raises for its client without failing.
//!
//! PostgreSQL tells a client things that are not errors — a `VACUUM FULL` that
//! could not return space, a `BEGIN` inside a block — as `NoticeResponse`
//! messages sent before the statement's `CommandComplete`. The engine had no
//! way to do that: a result was rows, a count or an error, so a condition the
//! client should hear about either became an error (breaking maintenance jobs
//! that expect success) or a log line nobody sees at the production level.
//!
//! The collector is a tokio task-local scoped around one statement by the wire
//! layer ([`collect`]); engine code calls [`push`] from anywhere in that task,
//! and the wire layer sends what was collected ([`drain`]) ahead of the
//! statement's completion. Like the cancel signal, it does not cross
//! `task::spawn`. Outside a scope — the native protocol, a background job —
//! a notice goes to the server log instead, at warn.

use std::cell::RefCell;
use std::future::Future;

/// One notice: PostgreSQL's severity (`WARNING`, `NOTICE`, …), SQLSTATE and
/// message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub severity: &'static str,
    pub sqlstate: &'static str,
    pub message: String,
}

tokio::task_local! {
    static NOTICES: RefCell<Vec<Notice>>;
}

/// Runs `fut` with a fresh notice collector.
pub async fn collect<F: Future>(fut: F) -> F::Output {
    NOTICES.scope(RefCell::new(Vec::new()), fut).await
}

/// Raises a notice for the current statement's client. Returns whether a
/// client will receive it; when not, it is logged.
pub fn push(severity: &'static str, sqlstate: &'static str, message: impl Into<String>) -> bool {
    let message = message.into();
    let mut message = Some(message);
    let collected = NOTICES
        .try_with(|n| {
            n.borrow_mut().push(Notice {
                severity,
                sqlstate,
                message: message.take().expect("taken once"),
            })
        })
        .is_ok();
    if !collected {
        log::warn!("{} {}: {}", severity, sqlstate, message.unwrap_or_default());
    }
    collected
}

/// Takes every notice collected so far in this task.
pub fn drain() -> Vec<Notice> {
    NOTICES
        .try_with(|n| std::mem::take(&mut *n.borrow_mut()))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn notices_belong_to_their_scope() {
        assert!(!push("WARNING", "01000", "outside"), "no scope, no client");
        let drained = collect(async {
            assert!(push("WARNING", "01000", "first"));
            assert!(push("NOTICE", "00000", "second"));
            drain()
        })
        .await;
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].message, "first");
        assert_eq!(drained[1].severity, "NOTICE");
        assert!(drain().is_empty());
    }
}
