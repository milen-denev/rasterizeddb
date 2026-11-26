//! The flusher: one thread per broker that writes what appends put in memory,
//! and makes it durable.
//!
//! Appends never make a syscall. They mark their partition (or subscription
//! log) dirty, and this thread does the rest in rounds:
//!
//! 1. Take every dirty log and write what it holds, one `pwrite` per run.
//! 2. `fdatasync` every file a `sync` log wrote, once per file per round. A
//!    round's sync covers every append made since the last one, on any
//!    connection: that is the group commit. When several files need a sync
//!    they are synced in parallel, so a round costs about one sync's latency
//!    however many partitions it covers.
//! 3. Sync the directory of any file the round created, then move each log's
//!    `durable` watermark, which wakes the producers waiting on it.
//!
//! `async` logs are written in every round and synced every
//! `broker.flush_ms` on this thread's own clock.
//!
//! While a round syncs, new appends gather for the next one. Under load each
//! round is therefore one sync for many batches, and when the server is idle
//! a lone append waits for nothing but its own sync.

use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use rasterizeddb_mq::wire::Durability;

use super::partition::WriteRound;

/// Something the flusher writes and syncs: a partition, or a subscription's log.
pub(super) trait Flushable: Send + Sync {
    /// Writes what is pending. `None` when there was nothing to write.
    fn write_pending(&self) -> io::Result<Option<WriteRound>>;
    /// Everything below `upto` is now durable.
    fn mark_durable(&self, upto: u64);
    fn durability(&self) -> Durability;
    fn poison(&self, e: &io::Error);
}

impl Flushable for super::partition::Partition {
    fn write_pending(&self) -> io::Result<Option<WriteRound>> {
        super::partition::Partition::write_pending(self)
    }
    fn mark_durable(&self, upto: u64) {
        super::partition::Partition::mark_durable(self, upto)
    }
    fn durability(&self) -> Durability {
        self.durability
    }
    fn poison(&self, e: &io::Error) {
        super::partition::Partition::poison(self, e)
    }
}

struct Shared {
    dirty: Vec<Weak<dyn Flushable>>,
    stop: bool,
}

pub struct Flusher {
    shared: Mutex<Shared>,
    wake: Condvar,
    stopped: AtomicBool,
    flush_ms: std::sync::atomic::AtomicU64,
    /// Rounds run and syncs issued, for `BROKER STATS`.
    pub rounds: std::sync::atomic::AtomicU64,
    pub syncs: std::sync::atomic::AtomicU64,
}

impl Flusher {
    /// Starts the thread. It ends when the last `Arc` is dropped or
    /// [`Flusher::stop`] is called.
    pub fn start(name: &str, flush_ms: u64) -> io::Result<Arc<Flusher>> {
        let f = Arc::new(Flusher {
            shared: Mutex::new(Shared { dirty: Vec::new(), stop: false }),
            wake: Condvar::new(),
            stopped: AtomicBool::new(false),
            flush_ms: std::sync::atomic::AtomicU64::new(flush_ms.max(1)),
            rounds: Default::default(),
            syncs: Default::default(),
        });
        let weak = Arc::downgrade(&f);
        std::thread::Builder::new().name(format!("broker-flush-{name}")).spawn(move || run(weak))?;
        Ok(f)
    }

    /// Puts a log on the next round. Called once per clean-to-dirty change.
    pub(super) fn mark_dirty_dyn(&self, log: Weak<dyn Flushable>) {
        let mut s = self.shared.lock();
        let was_empty = s.dirty.is_empty();
        s.dirty.push(log);
        drop(s);
        if was_empty {
            self.wake.notify_one();
        }
    }

    pub(super) fn mark_dirty(&self, p: &super::partition::Partition) {
        if let Some(me) = p.me() {
            self.mark_dirty_dyn(me);
        }
    }

    pub fn set_flush_ms(&self, ms: u64) {
        self.flush_ms.store(ms.max(1), Ordering::Relaxed);
    }

    pub fn stop(&self) {
        self.shared.lock().stop = true;
        self.wake.notify_one();
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }
}

fn run(weak: Weak<Flusher>) {
    // `async` files written since their last sync.
    let mut unsynced: Vec<Arc<File>> = Vec::new();
    let mut last_async_sync = Instant::now();
    loop {
        let Some(f) = weak.upgrade() else { return };
        let flush_ms = f.flush_ms.load(Ordering::Relaxed);
        let batch = {
            let mut s = f.shared.lock();
            if s.dirty.is_empty() && !s.stop {
                // Wake for work, or for the async clock when it has work, or
                // now and then to notice the broker is gone.
                let wait = if unsynced.is_empty() {
                    Duration::from_millis(500)
                } else {
                    Duration::from_millis(flush_ms).saturating_sub(last_async_sync.elapsed())
                };
                f.wake.wait_for(&mut s, wait);
            }
            if s.stop && s.dirty.is_empty() {
                drop(s);
                // The last async writes are synced before the thread ends.
                sync_all(&unsynced);
                f.stopped.store(true, Ordering::Relaxed);
                return;
            }
            std::mem::take(&mut s.dirty)
        };
        if !batch.is_empty() {
            round(&f, batch, &mut unsynced);
        }
        if !unsynced.is_empty() && last_async_sync.elapsed() >= Duration::from_millis(flush_ms) {
            f.syncs.fetch_add(unsynced.len() as u64, Ordering::Relaxed);
            sync_all(&unsynced);
            unsynced.clear();
            last_async_sync = Instant::now();
        }
        drop(f);
    }
}

fn round(f: &Flusher, batch: Vec<Weak<dyn Flushable>>, unsynced: &mut Vec<Arc<File>>) {
    f.rounds.fetch_add(1, Ordering::Relaxed);
    let mut synced: Vec<(Arc<dyn Flushable>, u64)> = Vec::new();
    let mut files: Vec<Arc<File>> = Vec::new();
    let mut dirs: Vec<PathBuf> = Vec::new();
    for w in batch {
        let Some(log) = w.upgrade() else { continue };
        match log.write_pending() {
            Ok(None) => {}
            Ok(Some(r)) => {
                if let Some(d) = r.dir {
                    if !dirs.contains(&d) {
                        dirs.push(d);
                    }
                }
                match log.durability() {
                    Durability::Sync => {
                        for file in r.files {
                            if !files.iter().any(|x| Arc::ptr_eq(x, &file)) {
                                files.push(file);
                            }
                        }
                        synced.push((log, r.upto));
                    }
                    Durability::Async => {
                        for file in r.files {
                            if !unsynced.iter().any(|x| Arc::ptr_eq(x, &file)) {
                                unsynced.push(file);
                            }
                        }
                    }
                    Durability::Memory => {}
                }
            }
            Err(e) => log.poison(&e),
        }
    }
    if files.is_empty() && dirs.is_empty() {
        return;
    }
    f.syncs.fetch_add((files.len() + dirs.len()) as u64, Ordering::Relaxed);
    let failed = sync_all(&files);
    for d in &dirs {
        if let Err(e) = crate::core::sync_path::sync_dir(d) {
            log::warn!("broker: could not sync directory {}: {e}", d.display());
        }
    }
    for (log, upto) in synced {
        match &failed {
            None => log.mark_durable(upto),
            Some(e) => log.poison(&io::Error::new(e.kind(), e.to_string())),
        }
    }
}

/// `fdatasync`s every file, in parallel when there are several. The first
/// error, if any: a failed sync leaves the page cache's state unknowable
/// (PostgreSQL's fsyncgate), so the caller stops the log rather than retry.
fn sync_all(files: &[Arc<File>]) -> Option<io::Error> {
    match files.len() {
        0 => None,
        1 => files[0].sync_data().err(),
        n => {
            let threads = n.min(8);
            let per = n.div_ceil(threads);
            let errors: Vec<io::Error> = std::thread::scope(|s| {
                let handles: Vec<_> = files
                    .chunks(per)
                    .map(|group| s.spawn(move || group.iter().find_map(|f| f.sync_data().err())))
                    .collect();
                handles.into_iter().filter_map(|h| h.join().ok().flatten()).collect()
            });
            errors.into_iter().next()
        }
    }
}
