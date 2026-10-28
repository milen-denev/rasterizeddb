//! A partition's maintenance when it is opened `MaintenanceMode::External`
//! (docs/partitioning.md §7.3, `partition::maintenance`): the same work its
//! own tasks would do (the providers' background landing, the vacuum
//! service's tick, the idle-checkpoint watch), held here without a task and
//! driven by its database's scheduler through [`Unit`].

use std::io;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::ops::ControlFlow;

use cacheguard::CacheGuard;
use futures::future::BoxFuture;
use rclite::Arc;

use super::table_seal::{IdleLook, IdleSealer};
use crate::core::mvcc::TransactionManager;
use crate::core::mvcc::transaction::TxId;
use crate::core::partition::maintenance::{Service, Unit, WriteWatch};
use crate::core::storage_providers::traits::StorageIO;
use crate::core::vacuum::{NeutralizeParts, NeutralizeReport, TickState, VacuumService, neutralize_aborted_from, sweep_scheduler};

struct VacuumSlot<S: StorageIO> {
    service: VacuumService<S>,
    state: TickState,
    /// When the last tick ran; ticks are an interval apart, as the service's
    /// own loop sleeps, and further apart after panics.
    last: Option<tokio::time::Instant>,
    /// Ticks in a row that panicked: the backoff, as the service's own loop
    /// keeps it (`vacuum::panic_backoff`).
    consecutive_panics: u32,
}

/// One `External` partition's maintenance. Owned by its `Table`; the
/// scheduler holds it weakly.
pub(crate) struct ExternalUnit<S: StorageIO> {
    io_rows: Arc<S>,
    io_pointers: Arc<S>,
    alive: StdArc<CacheGuard<AtomicBool>>,
    watch: StdArc<WriteWatch>,
    /// `None` with reclamation off: nothing is swept, and nothing forgotten.
    vacuum: Option<tokio::sync::Mutex<VacuumSlot<S>>>,
    sealer: Option<tokio::sync::Mutex<IdleSealer<S>>>,
}

impl<S: StorageIO> ExternalUnit<S> {
    pub(super) fn new(
        io_rows: Arc<S>,
        io_pointers: Arc<S>,
        alive: StdArc<CacheGuard<AtomicBool>>,
        watch: StdArc<WriteWatch>,
        vacuum: Option<VacuumService<S>>,
        sealer: Option<IdleSealer<S>>,
    ) -> Self {
        ExternalUnit {
            io_rows,
            io_pointers,
            alive,
            watch,
            vacuum: vacuum.map(|service| {
                tokio::sync::Mutex::new(VacuumSlot {
                    service,
                    state: TickState::new(),
                    last: None,
                    consecutive_panics: 0,
                })
            }),
            sealer: sealer.map(tokio::sync::Mutex::new),
        }
    }

    async fn service_once(&self) -> Service {
        // First, so a write during this round pushes the unit again.
        self.watch.serviced();
        #[cfg(test)]
        test_hooks::maybe_panic(&*self.io_pointers);
        #[cfg(test)]
        test_hooks::maybe_park(&*self.io_pointers).await;
        if !self.alive.load(Ordering::Acquire) {
            return Service::Gone;
        }
        let mut busy = self.io_rows.land_background().await;
        busy |= self.io_pointers.land_background().await;
        if let Some(slot) = &self.vacuum {
            let mut slot = slot.lock().await;
            let VacuumSlot { service, state, last, consecutive_panics } = &mut *slot;
            let now = tokio::time::Instant::now();
            let interval = test_hooks::interval_for(&*self.io_pointers).unwrap_or(service.config.interval);
            let wait = crate::core::vacuum::panic_backoff(interval, *consecutive_panics);
            if last.is_none_or(|t| now.duration_since(t) >= wait) {
                *last = Some(now);
                // Supervised as the service's own loop supervises it: a
                // panicking tick is counted and backed off, and the
                // partition, and every other one the scheduler services,
                // goes on.
                let tick = std::panic::AssertUnwindSafe(service.tick(state));
                match futures::FutureExt::catch_unwind(tick).await {
                    Ok(ControlFlow::Break(())) => return Service::Gone,
                    Ok(ControlFlow::Continue(())) => *consecutive_panics = 0,
                    Err(payload) => {
                        *consecutive_panics = consecutive_panics.saturating_add(1);
                        let total = service.stats.panics.fetch_add(1, Ordering::Relaxed) + 1;
                        log::error!(
                            "Vacuum of partition {} panicked ({} so far): {}; retrying after {:?}",
                            service.table_name,
                            total,
                            crate::core::vacuum::panic_message(&*payload),
                            crate::core::vacuum::panic_backoff(interval, *consecutive_panics)
                        );
                        return Service::Busy;
                    }
                }
            }
            busy |= !service.settled(state);
        }
        if let Some(sealer) = &self.sealer {
            match sealer.lock().await.look().await {
                IdleLook::Gone => return Service::Gone,
                IdleLook::Pending => busy = true,
                IdleLook::Quiet => {}
            }
        }
        if busy { Service::Busy } else { Service::Settled }
    }

    async fn neutralize(&self, manager: &StdArc<TransactionManager>, from: TxId) -> io::Result<NeutralizeReport> {
        let Some(slot) = &self.vacuum else {
            return Ok(NeutralizeReport::default());
        };
        // Serialised with this partition's ticks, as one service task would.
        let slot = slot.lock().await;
        let svc = &slot.service;
        let Some(_permit) = sweep_scheduler().acquire(1.0, &svc.alive).await else {
            return Ok(NeutralizeReport::default());
        };
        // Joined by `Table::retire`.
        let _tick = svc.tick.lock().await;
        if !svc.is_alive() {
            return Ok(NeutralizeReport::default());
        }
        let Some(file_gate) = svc.gate.upgrade() else {
            return Ok(NeutralizeReport::default());
        };
        let parts = NeutralizeParts {
            io_pointers: &*svc.io_pointers,
            write_lock: &svc.write_lock,
            compaction_gate: &svc.compaction_gate,
            readers: &svc.readers,
            free_space: &svc.free_space,
            cache_generation: &svc.cache_generation,
            dead_stamps: &svc.dead_stamps,
            drain_timeout: svc.config.drain_timeout,
            alive: Some(&svc.alive),
            gate: Some(&file_gate),
        };
        neutralize_aborted_from(&parts, manager, from).await
    }
}

impl<S: StorageIO> Unit for ExternalUnit<S> {
    fn service(&self) -> BoxFuture<'_, Service> {
        Box::pin(self.service_once())
    }

    fn neutralize_from<'a>(
        &'a self,
        manager: &'a StdArc<TransactionManager>,
        from: TxId,
    ) -> BoxFuture<'a, io::Result<NeutralizeReport>> {
        Box::pin(self.neutralize(manager, from))
    }
}

/// Test-only knobs, per directory: a vacuum interval for the partitions
/// under it, and a one-shot panic outside the tick (the scheduler's own
/// supervision).
pub(crate) mod test_hooks {
    use crate::core::storage_providers::traits::StorageIO;

    #[cfg(test)]
    static INTERVALS: parking_lot::Mutex<Vec<(std::path::PathBuf, std::time::Duration)>> =
        parking_lot::Mutex::new(Vec::new());
    #[cfg(test)]
    static PANICS: parking_lot::Mutex<Vec<std::path::PathBuf>> = parking_lot::Mutex::new(Vec::new());

    /// Partitions under `dir` tick every `interval` instead of the vacuum
    /// interval.
    #[cfg(test)]
    pub(crate) fn set_interval(dir: &std::path::Path, interval: std::time::Duration) {
        INTERVALS.lock().push((dir.to_path_buf(), interval));
    }

    /// The next service of a partition under `dir` panics, once.
    #[cfg(test)]
    pub(crate) fn panic_once(dir: &std::path::Path) {
        PANICS.lock().push(dir.to_path_buf());
    }

    pub(super) fn interval_for<S: StorageIO>(_io: &S) -> Option<std::time::Duration> {
        #[cfg(test)]
        {
            let dir = std::path::PathBuf::from(_io.get_location()?);
            return INTERVALS.lock().iter().find(|(d, _)| dir.starts_with(d)).map(|(_, i)| *i);
        }
        #[cfg(not(test))]
        None
    }

    #[cfg(test)]
    static PARKS: parking_lot::Mutex<Vec<(std::path::PathBuf, std::sync::Arc<tokio::sync::Notify>)>> =
        parking_lot::Mutex::new(Vec::new());

    /// The next service of a partition under `dir` waits until the returned
    /// `Notify` is signalled, once.
    #[cfg(test)]
    pub(crate) fn park_once(dir: &std::path::Path) -> std::sync::Arc<tokio::sync::Notify> {
        let n = std::sync::Arc::new(tokio::sync::Notify::new());
        PARKS.lock().push((dir.to_path_buf(), n.clone()));
        n
    }

    #[cfg(test)]
    pub(super) async fn maybe_park<S: StorageIO>(io: &S) {
        let Some(location) = io.get_location() else {
            return;
        };
        let dir = std::path::PathBuf::from(location);
        let found = {
            let mut p = PARKS.lock();
            let at = p.iter().position(|(d, _)| dir.starts_with(d));
            at.map(|i| p.remove(i))
        };
        if let Some((_, n)) = found {
            n.notified().await;
        }
    }

    #[cfg(test)]
    pub(super) fn maybe_panic<S: StorageIO>(io: &S) {
        let Some(location) = io.get_location() else {
            return;
        };
        let dir = std::path::PathBuf::from(location);
        let hit = {
            let mut p = PANICS.lock();
            let at = p.iter().position(|d| dir.starts_with(d));
            at.map(|i| p.remove(i))
        };
        if hit.is_some() {
            panic!("injected panic in a partition's service");
        }
    }
}
