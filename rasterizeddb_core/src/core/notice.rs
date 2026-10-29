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

/// What one statement has collected: its notices, and the counted ones
/// ([`count`]), each said once with how many times it happened.
#[derive(Default)]
struct Collected {
    notices: Vec<Notice>,
    counted: Vec<(String, String, u64)>,
}

tokio::task_local! {
    static NOTICES: RefCell<Collected>;
}

/// Runs `fut` with a fresh notice collector.
///
/// Not an `async fn`, so `fut` is stored once rather than once as an
/// argument and again where it is awaited: this wraps every statement, and
/// its future is copied whole each time it moves.
pub fn collect<F: Future>(fut: F) -> impl Future<Output = F::Output> {
    NOTICES.scope(RefCell::new(Collected::default()), fut)
}

/// Raises a notice for the current statement's client. Returns whether a
/// client will receive it; when not, it is logged.
pub fn push(severity: &'static str, sqlstate: &'static str, message: impl Into<String>) -> bool {
    let message = message.into();
    let mut message = Some(message);
    let collected = NOTICES
        .try_with(|n| {
            n.borrow_mut().notices.push(Notice {
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

/// Records one occurrence of something said once per statement: a WARNING
/// (01000) `message`, followed by how many times `key` was counted, sent
/// with the statement's other notices and logged once, at warn. A check that
/// left rows unjudged said so per row in the log, naming nothing.
///
/// Outside a statement's scope it is logged at most once a minute per key.
pub fn count(key: &str, message: impl FnOnce() -> String) {
    let mut message = Some(message);
    let counted = NOTICES
        .try_with(|n| {
            let mut n = n.borrow_mut();
            match n.counted.iter_mut().find(|(k, _, _)| k == key) {
                Some(entry) => entry.2 += 1,
                None => n.counted.push((key.to_string(), (message.take().expect("taken once"))(), 1)),
            }
        })
        .is_ok();
    if !counted {
        static LAST: std::sync::Mutex<Vec<(String, std::time::Instant)>> = std::sync::Mutex::new(Vec::new());
        let mut last = LAST.lock().unwrap_or_else(|e| e.into_inner());
        let now = std::time::Instant::now();
        match last.iter_mut().find(|(k, _)| k == key) {
            Some((_, at)) if now.duration_since(*at) < std::time::Duration::from_secs(60) => return,
            Some((_, at)) => *at = now,
            None => {
                if last.len() >= 1024 {
                    last.clear();
                }
                last.push((key.to_string(), now));
            }
        }
        log::warn!("{} (further occurrences within a minute are not logged)", (message.take().expect("taken once"))());
    }
}

/// Takes every notice collected so far in this task, the counted ones last.
pub fn drain() -> Vec<Notice> {
    let collected = NOTICES
        .try_with(|n| std::mem::take(&mut *n.borrow_mut()))
        .unwrap_or_default();
    let mut out = collected.notices;
    for (_, message, n) in collected.counted {
        let message = format!("{message} ({n} {})", if n == 1 { "row" } else { "rows" });
        log::warn!("{message}");
        out.push(Notice { severity: "WARNING", sqlstate: "01000", message });
    }
    out
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

    #[tokio::test]
    async fn a_counted_notice_is_said_once_with_its_count() {
        let drained = collect(async {
            for _ in 0..3 {
                count("t/c", || "check c was not judged".to_string());
            }
            count("t/d", || "check d was not judged".to_string());
            drain()
        })
        .await;
        let messages: Vec<&str> = drained.iter().map(|n| n.message.as_str()).collect();
        assert_eq!(messages, ["check c was not judged (3 rows)", "check d was not judged (1 row)"]);
    }
}
