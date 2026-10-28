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
    /// own loop sleeps.
    last: Option<tokio::time::Instant>,
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
                })
            }),
            sealer: sealer.map(tokio::sync::Mutex::new),
        }
    }

    async fn service_once(&self) -> Service {
        // First, so a write during this round pushes the unit again.
        self.watch.serviced();
        if !self.alive.load(Ordering::Acquire) {
            return Service::Gone;
        }
        let mut busy = self.io_rows.land_background().await;
        busy |= self.io_pointers.land_background().await;
        if let Some(slot) = &self.vacuum {
            let mut slot = slot.lock().await;
            let VacuumSlot { service, state, last } = &mut *slot;
            let now = tokio::time::Instant::now();
            if last.is_none_or(|t| now.duration_since(t) >= service.config.interval) {
                *last = Some(now);
                if service.tick(state).await == ControlFlow::Break(()) {
                    return Service::Gone;
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
