//! A table's side of seal and publish (docs/persistent-indexes.md §3,
//! slice B13): what [`crate::core::index_store::seal`] needs of one open
//! instance ([`SealParts`]), the seal entry point, retirement's seal cancel,
//! and the idle checkpoint's watcher.
//!
//! The parts are built once per instance in `Table::assemble`, before the
//! `Table` exists, so the idle watcher (a task of the instance, like the
//! filter's and vacuum's) can seal without the `Table`; every part is a
//! shared handle the instance holds too.

use std::io;
use std::path::PathBuf;
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use cacheguard::CacheGuard;
use rclite::Arc;

use super::{Table, TableData};
use crate::core::filter::TableFilters;
use crate::core::index::TableIndexes;
use crate::core::index_store::file_gate::FileGate;
use crate::core::index_store::gate::{Proof, WriteTicket};
use crate::core::index_store::gate_word::{GateState, SealKind, Word};
use crate::core::index_store::identity::{Identity, VersionTuple};
use crate::core::index_store::load;
use crate::core::index_store::seal::{self, Capture, SealOutcome, SealTarget};
use crate::core::index_store::snapshot::SectionKind;
use crate::core::mvcc::TransactionManager;
use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
use crate::core::storage_providers::traits::StorageIO;

/// What a seal reads of one open instance: its file set's gate, its
/// providers, its structures and how ready they are.
pub(crate) struct SealParts<S: StorageIO> {
    /// The physical name, for logs.
    pub(super) name: String,
    pub(super) gate: StdArc<FileGate>,
    pub(super) io_pointers: Arc<S>,
    pub(super) io_rows: Arc<S>,
    /// A plain table's own schema and sequence files, landed with the rest;
    /// a partition's are the logical table's and attach to no word.
    pub(super) own: Option<(Arc<S>, Arc<S>)>,
    pub(super) filters: Option<StdArc<TableFilters>>,
    pub(super) indexes: Option<StdArc<TableIndexes>>,
    pub(super) filters_ready: StdArc<CacheGuard<AtomicBool>>,
    pub(super) versions: StdArc<ArcSwapOption<TransactionManager>>,
    pub(super) incarnation: u128,
    /// `index::shape_of(&schema)` at open (C6).
    pub(super) shape_hash: u64,
    /// Vacuum's clear horizon, which the FILTER section records (§2, B14).
    pub(super) cleared_below: StdArc<CacheGuard<AtomicU64>>,
    /// `<stem>_flt.db` may be another table's rows (a legacy pair, P19):
    /// the evidence row of §4 cannot be read, so no snapshot is taken.
    pub(super) legacy_flt: bool,
    /// `outcome_revocations()` at open (N26).
    pub(super) revocations_at_open: u64,
    /// Set by [`Table::retire`]: a retired instance never snapshots.
    pub(super) retired: AtomicBool,
    /// Whether the snapshot this instance stands on holds its INDEX
    /// sections: set by a load of them, and by a capture that wrote them
    /// (B17). A seal asks it before "already clean" (§3 rule 2).
    pub(super) index_sections: StdArc<AtomicBool>,
    /// How the indexes came to be at open (`index_store::load::Origin`).
    pub(super) index_origin: StdArc<AtomicU8>,
}

impl<S: StorageIO> SealParts<S> {
    fn dir(&self) -> Option<PathBuf> {
        self.gate.dir().map(|d| d.to_path_buf())
    }

    /// Whether a seal of this instance writes INDEX sections now (B17).
    fn captures_indexes(&self) -> bool {
        self.indexes
            .as_ref()
            .is_some_and(|ix| load::captures(ix, load::settings::index_storage_for(self.gate.dir())))
    }
}

impl<S: StorageIO> SealTarget for SealParts<S> {
    fn gate(&self) -> &StdArc<FileGate> {
        &self.gate
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }

    async fn settle(&self) {
        self.gate.loads().settled().await;
    }

    fn snapshot_current(&self) -> bool {
        !self.captures_indexes() || self.index_sections.load(Ordering::Acquire)
    }

    async fn before_seal(&self, ticket: &WriteTicket) {
        // The filter's checkpoint bookkeeping, reset under the seal's
        // transient ticket: §3 step 7 resets it on publish, but nothing may
        // hold a ticket once `CLEAN` is written, and only the retired
        // `_flt.db` checkpoint ever read it (B14).
        if let Some(f) = &self.filters {
            f.mark_checkpointed(Proof::from(ticket));
        }
    }

    async fn land_staged(&self) -> io::Result<()> {
        self.io_pointers.land_staged().await?;
        self.io_rows.land_staged().await?;
        if let Some((schema, ids)) = &self.own {
            schema.land_staged().await?;
            ids.land_staged().await?;
        }
        Ok(())
    }

    fn ineligible(&self) -> Option<String> {
        if self.legacy_flt {
            return Some(format!(
                "{}_flt.db may be another table's rows, so the evidence a snapshot needs cannot be read",
                self.name
            ));
        }
        if !self.gate.epoch_proven() {
            return Some("rdb.ixe could not be read, so no slot epoch is proven".into());
        }
        if let Some(ix) = &self.indexes
            && !ix.is_ready()
        {
            return Some("an index is not ready (building, or demoted by a renumbering)".into());
        }
        if let Some(f) = &self.filters {
            if !self.filters_ready.load(Ordering::Acquire) {
                return Some("the filter is still backfilling".into());
            }
            if crate::core::filter::renumbering(f.renumber_epoch()) {
                return Some("a renumbering is running".into());
            }
            if !f.is_complete() {
                return Some(
                    "the filter has an unindexed prefix (demoted by a failed renumbering, or \
                     filter.backfill_on_open = off)"
                        .into(),
                );
            }
        }
        let dir = self.dir()?;
        let dir = dir.to_string_lossy();
        if crate::core::bulk_load::is_active(&dir) {
            return Some("the directory is in bulk-load mode (unlogged writes)".into());
        }
        if crate::core::wal::wal_for(&dir).is_none() {
            return Some("the directory has no write-ahead log (wal_enabled = false)".into());
        }
        None
    }

    fn revoked_since_open(&self) -> bool {
        match self.versions.load_full() {
            // Its own database's log: a revocation in another database's
            // cannot make a version of this one visible again.
            Some(tm) => tm.status().revoked_since(self.revocations_at_open),
            // Unversioned, nothing decided on an outcome here; the
            // process-wide count, conservatively, as before.
            None => crate::core::mvcc::status::outcome_revocations() != self.revocations_at_open,
        }
    }

    fn barrier(&self) -> io::Result<()> {
        let dir = self.dir().unwrap_or_default();
        seal::failpoint_at("barrier_root_sync_fails", &dir)?;
        if let Some(tm) = self.versions.load_full() {
            // Stage outcomes; the root sync below makes them durable, poison
            // checked first.
            tm.flush()?;
            if let Some(root) = tm.status_wal() {
                root.sync_issued()?;
            }
        }
        seal::failpoint_at("barrier_dir_sync_fails", &dir)?;
        if let Some(own) = crate::core::wal::wal_for(&dir.to_string_lossy()) {
            own.sync_issued()?;
        }
        Ok(())
    }

    fn root_dir(&self) -> Option<PathBuf> {
        let tm = self.versions.load_full()?;
        let wal = tm.status_wal()?;
        wal.path().parent().map(|p| p.to_path_buf())
    }

    async fn capture(&self) -> io::Result<Capture> {
        let pointers_len = self.io_pointers.get_len().await;
        let rows_len = self.io_rows.get_len().await;
        let n_slots = pointers_len / ROW_POINTER_RECORD_LEN as u64;
        let mut sections = Vec::new();
        let mut filter_config_hash = 0;
        if let Some(f) = &self.filters {
            sections.push((
                SectionKind::Filter,
                0u16,
                f.sealed_image(n_slots, self.cleared_below.load(Ordering::Acquire)),
            ));
            filter_config_hash = f.config_hash();
        }
        // The INDEX sections (B17), one per B-tree, hash and path index, in
        // `for_schema` order; none for a table with a gin index.
        let mut with_indexes = false;
        if let Some(ix) = self.indexes.as_ref().filter(|_| self.captures_indexes()) {
            match load::capture_sections(ix, &self.gate)? {
                Some(s) => sections.extend(s),
                None => return Err(io::Error::other("the capture was cancelled")),
            }
            with_indexes = true;
        }
        // Read by the next "already clean" only after this seal published;
        // a seal that fails after it leaves the word unvouched, which asks
        // nothing of this.
        self.index_sections.store(with_indexes, Ordering::Release);
        Ok(Capture {
            identity: Identity {
                incarnation: self.incarnation,
                slot_epoch: self.gate.slot_epoch(),
                n_slots,
                pointers_len,
                rows_len,
                versions: VersionTuple::current(crate::core::filter::persist::FORMAT_VER),
                shape_hash: self.shape_hash,
                filter_config_hash,
            },
            sections,
        })
    }
}

impl<S: StorageIO> Table<S> {
    /// How this instance's indexes came to be at open (B17).
    pub fn index_origin(&self) -> load::Origin {
        match self.data() {
            Ok(d) => load::Origin::of(d.seal.index_origin.load(Ordering::Acquire)),
            Err(_) => load::Origin::Pending,
        }
    }

    /// Waits for a load of the snapshot's INDEX sections in flight, if any
    /// (B17); after it, the indexes are ready, or a build from rows runs.
    pub async fn index_load_settled(&self) {
        self.gate.loads().settled().await;
    }
}

impl<S: StorageIO> TableData<S> {
    /// The parts a seal of this instance reads.
    pub(crate) fn seal_parts(&self) -> &StdArc<SealParts<S>> {
        &self.seal
    }
}

impl<S: StorageIO> Table<S> {
    /// Seals and publishes this table's file set (§3): `Optional` for the
    /// idle checkpoint, `Mandatory` for shutdown and `rdb_index_checkpoint`.
    /// A partition shell has no file set and is excluded.
    pub async fn seal_snapshot(&self, kind: SealKind) -> SealOutcome {
        match self.data() {
            Ok(d) => seal::seal(&**d.seal_parts(), kind).await,
            Err(_) => SealOutcome::Excluded,
        }
    }

    /// Stops this instance's background writers for shutdown (§3
    /// Shutdown, P30): vacuum, the filter's backfill, the idle watcher and the providers' background landing, joining any vacuum
    /// work in flight. Unlike [`Table::retire`] the instance may still be
    /// sealed: shutdown seals it next.
    pub async fn quiesce_for_shutdown(&self) {
        self.maintenance_alive.store(false, Ordering::Release);
        let _joined = self.maintenance_tick.lock().await;
    }

    /// Rows times one plus the index count: a seal's rebuild cost saved,
    /// which shutdown divides by the snapshot's size to order its seals
    /// (§3 Shutdown).
    pub async fn rebuild_weight(&self) -> (u64, u64) {
        let Ok(d) = self.data() else {
            return (0, 1);
        };
        let slots = d.io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64;
        let indexes = d.indexes.as_ref().map_or(0, |ix| ix.len() as u64);
        let bytes = d.filters.as_ref().map_or(0, |f| f.bytes() as u64);
        (slots.saturating_mul(1 + indexes), bytes.max(1))
    }
}

/// The idle checkpoint's decision for one file set (§3, Q6): a seal is due
/// once the word has been `Dirty`, unsealed and with no ticket counted at
/// every look, and the table's committed-mutation counter has not moved,
/// for the whole threshold. Any look that sees otherwise restarts the
/// clock.
#[derive(Debug, Clone, Copy)]
pub(crate) struct IdleWatch {
    generation: u64,
    since: tokio::time::Instant,
}

impl IdleWatch {
    pub(crate) fn new(generation: u64, now: tokio::time::Instant) -> IdleWatch {
        IdleWatch { generation, since: now }
    }

    /// Whether a seal is due at `now`.
    pub(crate) fn due(&mut self, now: tokio::time::Instant, generation: u64, word: Word, threshold: Duration) -> bool {
        let quiet = word.state() == GateState::Dirty && word.count() == 0 && !word.sealed();
        if !quiet || generation != self.generation {
            *self = IdleWatch::new(generation, now);
            return false;
        }
        now.duration_since(self.since) >= threshold
    }

    /// After a seal attempt: the clock starts again whatever it did.
    pub(crate) fn restart(&mut self, now: tokio::time::Instant) {
        self.since = now;
    }
}

/// The idle threshold for a file set in `dir`: `index_snapshot.
/// idle_checkpoint_secs`, or a test's override for its directory; `None`
/// when off.
pub(crate) fn idle_threshold(dir: Option<&std::path::Path>) -> Option<Duration> {
    #[cfg(test)]
    if let Some(d) = dir
        && let Some(t) = idle_override::get(d)
    {
        return Some(t);
    }
    let _ = dir;
    match seal::settings::idle_checkpoint_secs() {
        0 => None,
        secs => Some(Duration::from_secs(secs)),
    }
}

/// Test-only: an idle threshold for tables under one directory, so a test
/// can watch the idle checkpoint without changing the process-wide setting
/// other tests run under.
#[cfg(test)]
pub(crate) mod idle_override {
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use parking_lot::Mutex;

    static OVERRIDES: Mutex<Vec<(PathBuf, Duration)>> = Mutex::new(Vec::new());

    /// Removes the override when dropped.
    pub(crate) struct Guard(PathBuf);

    impl Drop for Guard {
        fn drop(&mut self) {
            OVERRIDES.lock().retain(|(d, _)| *d != self.0);
        }
    }

    pub(crate) fn set(dir: &Path, threshold: Duration) -> Guard {
        OVERRIDES.lock().push((dir.to_path_buf(), threshold));
        Guard(dir.to_path_buf())
    }

    pub(super) fn get(dir: &Path) -> Option<Duration> {
        OVERRIDES
            .lock()
            .iter()
            .find(|(d, _)| dir.starts_with(d))
            .map(|(_, t)| *t)
    }
}

/// Spawns the idle checkpoint's watcher for one instance: it looks every
/// quarter of the threshold (at most every 30 s, at least every 50 ms), and
/// stops with the instance's maintenance. It holds the parts weakly between
/// looks: a strong hold would keep the file set's gate alive after the
/// instance is dropped, and the next open of the file set would then reuse
/// that word instead of deriving it from `.ixm` (§2: only the first open
/// derives it).
pub(super) fn spawn_idle_watcher<S: StorageIO>(
    parts: &StdArc<SealParts<S>>,
    alive: StdArc<CacheGuard<AtomicBool>>,
    generation: StdArc<CacheGuard<AtomicU64>>,
) {
    let Some(mut sealer) = IdleSealer::new(parts, alive, generation) else {
        return;
    };
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(sealer.poll_interval()).await;
            if sealer.look().await == IdleLook::Gone {
                break;
            }
        }
    });
}

/// What one look of an [`IdleSealer`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdleLook {
    /// The instance is retired or dropped: no more looks.
    Gone,
    /// Nothing for the idle checkpoint to do now or later without a write:
    /// the word is not `Dirty`, or idle checkpoints are off.
    Quiet,
    /// The word is `Dirty`: a seal is due once the threshold passes quietly.
    Pending,
}

/// The idle checkpoint's watch over one instance, without a task of its
/// own: [`spawn_idle_watcher`]'s loop looks through one, and so does the
/// shared scheduler for a partition opened `MaintenanceMode::External`
/// (docs/partitioning.md §7.3). Holds the parts weakly, as the watcher
/// always has.
pub(crate) struct IdleSealer<S: StorageIO> {
    parts: std::sync::Weak<SealParts<S>>,
    dir: Option<PathBuf>,
    alive: StdArc<CacheGuard<AtomicBool>>,
    generation: StdArc<CacheGuard<AtomicU64>>,
    watch: IdleWatch,
}

impl<S: StorageIO> IdleSealer<S> {
    /// `None` for a file set with no files to seal.
    pub(super) fn new(
        parts: &StdArc<SealParts<S>>,
        alive: StdArc<CacheGuard<AtomicBool>>,
        generation: StdArc<CacheGuard<AtomicU64>>,
    ) -> Option<Self> {
        if !parts.gate.has_files() {
            return None;
        }
        let watch = IdleWatch::new(generation.load(Ordering::Acquire), tokio::time::Instant::now());
        Some(IdleSealer {
            dir: parts.dir(),
            parts: StdArc::downgrade(parts),
            alive,
            generation,
            watch,
        })
    }

    /// How long a watcher of its own waits between looks.
    pub(crate) fn poll_interval(&self) -> Duration {
        idle_threshold(self.dir.as_deref())
            .map(|t| (t / 4).clamp(Duration::from_millis(50), Duration::from_secs(30)))
            .unwrap_or(Duration::from_secs(30))
    }

    /// One look: seals when the threshold has passed quietly.
    pub(crate) async fn look(&mut self) -> IdleLook {
        let threshold = idle_threshold(self.dir.as_deref());
        let Some(parts) = self.parts.upgrade() else {
            return IdleLook::Gone;
        };
        if !self.alive.load(Ordering::Acquire) || parts.retired() {
            return IdleLook::Gone;
        }
        let Some(threshold) = threshold else {
            return IdleLook::Quiet;
        };
        let now = tokio::time::Instant::now();
        let word = parts.gate.word().load();
        if self.watch.due(now, self.generation.load(Ordering::Acquire), word, threshold) {
            let outcome = seal::seal(&*parts, SealKind::Optional).await;
            log::debug!("idle checkpoint of {}: {}", parts.name, outcome.describe());
            self.watch.restart(tokio::time::Instant::now());
        }
        if parts.gate.word().load().state() == GateState::Dirty {
            IdleLook::Pending
        } else {
            IdleLook::Quiet
        }
    }
}
