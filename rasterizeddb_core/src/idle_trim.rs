//! Hands freed heap back to the kernel once the server goes quiet.
//!
//! glibc keeps what a thread frees in that thread's arena, and an arena only
//! shrinks from its top, so after a heavy statement the process holds its
//! high-water mark: measured on a live server, 0.6 GB before a run of large
//! scans and aggregates and 8 GB idle after it, until the next restart.
//! `malloc_trim(0)` returns every arena's free pages, but it walks the heap
//! and takes each arena's lock, so it runs only when no statement has run
//! for a whole tick ([`TICK`]) and only once per burst of work: an idle
//! server is not trimmed again and again, and a busy one never is.
//!
//! `RDB_IDLE_TRIM=0` turns it off.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

/// Statements running now.
static RUNNING: AtomicUsize = AtomicUsize::new(0);
/// Statements finished since the process began.
static FINISHED: AtomicU64 = AtomicU64::new(0);
/// Trims done, for tests and introspection.
static TRIMS: AtomicU64 = AtomicU64::new(0);

/// How often the trimmer looks. A trim follows between one and two ticks
/// after the last statement ends.
pub const TICK: Duration = Duration::from_secs(5);

/// Held for the length of one statement. See [`statement`].
pub struct Running(());

impl Drop for Running {
    fn drop(&mut self) {
        RUNNING.fetch_sub(1, Ordering::AcqRel);
        FINISHED.fetch_add(1, Ordering::AcqRel);
    }
}

/// Marks a statement as running until the guard is dropped.
pub fn statement() -> Running {
    RUNNING.fetch_add(1, Ordering::AcqRel);
    Running(())
}

pub fn trims() -> u64 {
    TRIMS.load(Ordering::Relaxed)
}

/// What the trimmer remembers between ticks.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Seen {
    /// `FINISHED` at the previous tick.
    pub finished: u64,
    /// `FINISHED` when the last trim ran.
    pub trimmed_at: u64,
}

/// Whether to trim now: nothing running, nothing finished since the previous
/// tick (a whole tick of quiet), and something finished since the last trim.
pub fn due(running: usize, finished: u64, seen: Seen) -> bool {
    running == 0 && finished == seen.finished && finished != seen.trimmed_at
}

/// Starts the trimmer on the current runtime. Does nothing off glibc or with
/// `RDB_IDLE_TRIM=0`.
pub fn spawn() {
    if !cfg!(all(target_os = "linux", target_env = "gnu")) {
        return;
    }
    if std::env::var_os("RDB_IDLE_TRIM").is_some_and(|v| v == "0") {
        return;
    }
    tokio::spawn(async move {
        let mut seen = Seen::default();
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let finished = FINISHED.load(Ordering::Acquire);
            let running = RUNNING.load(Ordering::Acquire);
            if due(running, finished, seen) {
                let started = std::time::Instant::now();
                let _ = tokio::task::spawn_blocking(crate::release_freed_memory).await;
                TRIMS.fetch_add(1, Ordering::Relaxed);
                log::debug!("idle: returned freed heap to the kernel in {:?}", started.elapsed());
                seen.trimmed_at = finished;
            }
            seen.finished = finished;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_trim_waits_for_a_quiet_tick_and_happens_once_per_burst() {
        let fresh = Seen::default();
        // Nothing has ever run: nothing to give back.
        assert!(!due(0, 0, fresh));
        // Work finished during the last tick: wait for a quiet one.
        assert!(!due(0, 5, fresh));
        // A whole tick with no statement finishing and none running: trim.
        let quiet = Seen { finished: 5, trimmed_at: 0 };
        assert!(due(0, 5, quiet));
        // Already trimmed after that burst: not again while idle.
        let trimmed = Seen { finished: 5, trimmed_at: 5 };
        assert!(!due(0, 5, trimmed));
        // A long statement still running is not quiet, whatever the count says.
        assert!(!due(1, 5, quiet));
    }

    #[test]
    fn the_guard_counts_a_statement_while_it_runs() {
        let before = FINISHED.load(Ordering::Acquire);
        let guard = statement();
        assert!(RUNNING.load(Ordering::Acquire) >= 1);
        drop(guard);
        assert!(FINISHED.load(Ordering::Acquire) > before);
    }
}
