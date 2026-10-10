//! Process-level shutdown: turning a termination signal into an orderly stop.
//!
//! Without this the server dies mid-connection, which leaves peers stuck in
//! `CLOSE_WAIT`, the listening port in `TIME_WAIT`, and any un-checkpointed
//! WAL records waiting for the next start-up to replay.

use log::{error, info, warn};

/// Resolves on Ctrl+C, and on SIGTERM where the platform has it, so a `kill`
/// from a service manager drains the same way an interactive Ctrl+C does.
pub async fn termination_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(sigterm) => sigterm,
            Err(e) => {
                error!("Could not install SIGTERM handler ({e}); watching Ctrl+C only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Runs `on_signal` when the first termination signal arrives, then exits the
/// process on the second one.
///
/// The escape hatch matters: installing a handler takes Ctrl+C away from the
/// default kill behaviour, so without it an operator whose drain is wedged has
/// no way out short of `kill -9`.
pub fn on_termination<F>(on_signal: F)
where
    F: FnOnce() + Send + 'static,
{
    tokio::spawn(async move {
        termination_signal().await;
        info!("Termination signal received, draining before exit");
        on_signal();

        termination_signal().await;
        warn!("Second termination signal received, exiting immediately");
        std::process::exit(130);
    });
}

/// Last step before the process leaves: flush everything the WAL still owns.
///
/// Skipping this is not corruption - recovery replays the log on the next
/// start - but it makes that start-up slower for no reason.
pub fn finalize() {
    // Before the WAL: a store saving on exit writes its own file, and should
    // not find the process already half gone.
    #[cfg(feature = "kv_cache")]
    crate::core::kv_cache::save_all_on_exit();
    match crate::core::wal::checkpoint_all() {
        Ok(()) => info!("WAL checkpointed on shutdown"),
        Err(e) => error!("WAL checkpoint failed during shutdown: {e}"),
    }
}
