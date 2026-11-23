//! A table's side of the audit (docs/persistent-indexes.md §4 Audit, slice
//! B18): the scratch copy read from rows, the verdict, and the in-place
//! rebuild a mismatch takes. What is compared and when a verdict counts are
//! [`crate::core::index_store::audit`]'s.
//!
//! The parts are built in `Table::assemble`, before the `Table` exists, so
//! the audit after a load runs on a task of the instance (like the idle
//! watcher) without the `Table`; `rdb_index_check` builds the same parts
//! from a live instance.

use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use cacheguard::CacheGuard;
use log::{error, info, warn};
use smallvec::SmallVec;

use super::table_seal::SealParts;
use super::{Table, backfill_into, build_indexes};
use crate::core::index_store::audit::{self, Findings, IndexScratch, Kind, Outcome, Point};
use crate::core::index_store::gate::{self, Proof};
use crate::core::index_store::gate_word::GateState;
use crate::core::index_store::load;
use crate::core::mvcc::Visibility;
use crate::core::mvcc::transaction::FROZEN_TX;
use crate::core::row::row::Row;
use crate::core::row::row_pointer::{ROW_POINTER_RECORD_LEN, RowPointerIterator};
use crate::core::row::schema::SchemaField;
use crate::core::storage_providers::traits::StorageIO;
use crate::core::vacuum::ReaderRegistry;

/// What an audit reads of one open instance, beside what a seal reads.
pub(crate) struct AuditParts<S: StorageIO> {
    pub(super) seal: StdArc<SealParts<S>>,
    pub(super) fields: SmallVec<[SchemaField; 20]>,
    pub(super) readers: StdArc<ReaderRegistry>,
    pub(super) alive: StdArc<CacheGuard<AtomicBool>>,
    pub(super) cache_generation: StdArc<CacheGuard<AtomicU64>>,
}

impl<S: StorageIO> AuditParts<S> {
    fn gone(&self) -> bool {
        !self.alive.load(Ordering::Relaxed) || self.seal.retired.load(Ordering::Acquire)
    }
}

/// File sets with a check running, by gate: one at a time each, so two
/// verdicts never rebuild the same structures at once.
static RUNNING: parking_lot::Mutex<Vec<usize>> = parking_lot::Mutex::new(Vec::new());

struct Running(usize);

impl Running {
    fn claim(gate: usize) -> Option<Running> {
        let mut running = RUNNING.lock();
        if running.contains(&gate) {
            return None;
        }
        running.push(gate);
        Some(Running(gate))
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        RUNNING.lock().retain(|g| *g != self.0);
    }
}

/// Called at the end of `Table::assemble`: when this open raised a load,
/// waits for it, and audits 1 in `index_snapshot.audit_rate` loads that
/// installed (a build from rows is the source of truth, and is not
/// audited).
pub(super) fn spawn_after_load<S: StorageIO>(parts: AuditParts<S>) {
    if parts.seal.indexes.is_none() || !parts.seal.gate.loads().loading() {
        return;
    }
    tokio::spawn(async move {
        parts.seal.gate.loads().settled().await;
        let loaded = load::Origin::of(parts.seal.index_origin.load(Ordering::Acquire)) == load::Origin::Loaded;
        if !loaded || parts.gone() || !audit::chosen(parts.seal.gate.dir()) {
            return;
        }
        let outcome = check(&parts, Kind::AfterLoad).await;
        if let Outcome::Abandoned(why) = &outcome {
            info!("{}: the index snapshot audit was abandoned: {why}", parts.seal.name);
        }
    });
}

impl<S: StorageIO> Table<S> {
    /// `rdb_index_check` for this instance (B18): the audit's check now,
    /// after any load in flight, and its in-place rebuild on a mismatch. A
    /// partition shell has nothing to check.
    pub async fn index_check(&self) -> Outcome {
        let Ok(d) = self.data() else {
            return Outcome::Abandoned("a partitioned table's shell has no rows".into());
        };
        self.index_load_settled().await;
        let parts = AuditParts {
            seal: d.seal_parts().clone(),
            fields: self.schema.fields.clone(),
            readers: d.readers.clone(),
            alive: self.maintenance_alive.clone(),
            cache_generation: self.cache_generation.clone(),
        };
        check(&parts, Kind::OnDemand).await
    }
}

/// One check of one instance, counted, recorded and logged.
async fn check<S: StorageIO>(parts: &AuditParts<S>, kind: Kind) -> Outcome {
    audit::note_started();
    let gate_id = StdArc::as_ptr(&parts.seal.gate) as usize;
    let outcome = match Running::claim(gate_id) {
        Some(_running) => run(parts, kind).await,
        None => Outcome::Abandoned("another check of this table is running".into()),
    };
    outcome.count();
    #[cfg(test)]
    audit::hooks::record(parts.seal.gate.dir(), &parts.seal.name, &outcome);
    match &outcome {
        Outcome::Clean(f) => info!(
            "{}: index check clean ({:?}): {} rows, {} index entries, {} filter values",
            parts.seal.name, kind, f.rows, f.entries, f.filter_checked
        ),
        Outcome::Mismatch { findings, rebuilt } => error!(
            "{}: index check found a mismatch ({:?}): {} index entries missing, {} filter values pruned; \
             rebuilt in place: {rebuilt}; {}",
            parts.seal.name,
            kind,
            findings.missing_entries,
            findings.filter_misses,
            findings.examples.join("; ")
        ),
        Outcome::Abandoned(_) => {}
    }
    outcome
}

/// The check (module documentation of `index_store::audit`).
async fn run<S: StorageIO>(parts: &AuditParts<S>, kind: Kind) -> Outcome {
    let seal = &parts.seal;
    let gate = &seal.gate;
    let word = gate.word();
    // The scratch doubles the table's index memory while it lives, so it
    // takes a slot on the load queue, as a load does, at the lowest rank:
    // behind the opens, never ahead of a load someone waits for. Given back
    // with the scratch, before any rebuild: the rebuild's ticket may wait
    // for a load of this file set, which may wait for a slot.
    let queue_slot = load::admit_audit().await;
    if kind == Kind::AfterLoad && gate.state() != GateState::Clean {
        return Outcome::Abandoned("the table was written after its load".into());
    }
    // The stability baseline: nothing held now, nothing taken until the
    // verdict.
    let taken = word.tickets_taken();
    if word.load().count() > 0 {
        return Outcome::Abandoned("a write holds a ticket on the table".into());
    }
    let moved = || word.tickets_taken() != taken || word.load().count() > 0;
    // Every table a database opens has its transactions attached right
    // after the open; the audit after a load may get here first.
    let mut waited = Duration::ZERO;
    let manager = loop {
        if let Some(m) = seal.versions.load_full() {
            break m;
        }
        if waited >= Duration::from_secs(30) || parts.gone() {
            return Outcome::Abandoned("the table has no transactions attached".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        waited += Duration::from_millis(10);
    };
    let visibility = Visibility::for_snapshot(manager.snapshot(FROZEN_TX), manager);

    let mut findings = Findings::default();
    let indexes = seal.indexes.as_ref().filter(|ix| {
        if !ix.is_ready() {
            findings.skipped.push("the indexes are not ready (building, or demoted)".into());
        }
        ix.is_ready()
    });
    if let Some(ix) = indexes
        && !ix.gin.is_empty()
    {
        findings.skipped.push("gin indexes are not compared".into());
    }
    let filters = seal.filters.as_ref().filter(|f| {
        let ready = seal.filters_ready.load(Ordering::Acquire) && !crate::core::filter::renumbering(f.renumber_epoch());
        if !ready {
            findings.skipped.push("the filter is not ready (backfilling, or renumbering)".into());
        }
        ready
    });
    let list = indexes.map(|ix| ix.list.clone()).unwrap_or_default();
    findings.indexes = list.len();

    let mut cols: Vec<u32> = indexes.map(|ix| ix.columns()).unwrap_or_default();
    if let Some(f) = filters {
        cols.extend(
            parts
                .fields
                .iter()
                .filter(|c| !c.is_deleted && f.indexes(c.write_order as u32))
                .map(|c| c.write_order as u32),
        );
    }
    cols.sort_unstable();
    cols.dedup();
    if cols.is_empty() {
        return Outcome::Clean(findings);
    }

    // The scratch copy: every visible row's keys and filtered values, read
    // as the open's build and backfill read them.
    let mut scratch: Vec<IndexScratch> = list.iter().map(|_| IndexScratch::default()).collect();
    {
        let _read = parts.readers.enter();
        let n_slots = seal.io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64;
        let fetch = Table::<S>::build_fetch_for(&parts.fields, &cols);
        let mut iterator = match RowPointerIterator::new(seal.io_pointers.clone()).await {
            Ok(it) => it,
            Err(e) => return Outcome::Abandoned(format!("the pointers could not be read: {e:?}")),
        };
        let mut row = Row::default();
        let mut seen = 0u64;
        loop {
            let next = match iterator.next_row_pointer_with_offset().await {
                Ok(next) => next,
                Err(e) => return Outcome::Abandoned(format!("the pointers could not be read: {e:?}")),
            };
            let Some((offset, pointer)) = next else { break };
            let slot = offset / ROW_POINTER_RECORD_LEN as u64;
            if slot >= n_slots {
                break;
            }
            seen += 1;
            if seen % 4096 == 0 {
                if parts.gone() {
                    return Outcome::Abandoned("the table was dropped or retired".into());
                }
                if moved() {
                    return Outcome::Abandoned("a write overlapped the check".into());
                }
                tokio::task::yield_now().await;
            }
            if !visibility.visible(pointer.deleted, pointer.xmin, pointer.xmax) {
                continue;
            }
            if pointer.fetch_row_reuse_async(&*seal.io_rows, &fetch, &mut row).await.is_err() {
                findings.unreadable += 1;
                continue;
            }
            findings.rows += 1;
            let get = |wo: u32| row.columns.iter().find(|c| c.schema_id as u32 == wo).map(|c| c.data.into_slice());
            for (ix, s) in list.iter().zip(scratch.iter_mut()) {
                match ix.key_of(get) {
                    Some(key) => s.pairs.push((key, slot)),
                    None => s.unkeyed.push(slot),
                }
            }
            if let Some(f) = filters {
                for c in row.columns.iter() {
                    match f.admits_eq(c.schema_id as u32, slot, c.data.into_slice(), n_slots) {
                        Some(true) => findings.filter_checked += 1,
                        Some(false) => {
                            findings.filter_checked += 1;
                            findings.filter_misses += 1;
                            findings.note(format!(
                                "the filter prunes slot {slot}'s own value of column {}",
                                c.schema_id
                            ));
                        }
                        None => {}
                    }
                }
            }
        }
    }
    #[cfg(test)]
    audit::hooks::at(gate.dir(), Point::Scanned).await;
    #[cfg(not(test))]
    let _ = Point::Scanned;

    audit::compare_indexes(&list, scratch, &mut findings);
    drop(queue_slot);

    // The verdict counts only if nothing moved under it.
    if parts.gone() {
        return Outcome::Abandoned("the table was dropped or retired".into());
    }
    if moved() {
        return Outcome::Abandoned("a write overlapped the check".into());
    }
    if !findings.mismatch() {
        return Outcome::Clean(findings);
    }
    #[cfg(test)]
    audit::hooks::at(gate.dir(), Point::Verdict).await;
    let rebuilt = rebuild_in_place(parts, &findings).await;
    Outcome::Mismatch { findings, rebuilt }
}

/// A mismatch (module documentation of `index_store::audit`): a ticket, so
/// `DIRTY` is durable and the bad snapshot never loads again; under it,
/// what failed is demoted and cleared; then it is rebuilt from rows as the
/// open's build and backfill do, into the installed structures, which
/// writes keep feeding meanwhile (a write that lands after the build passed
/// its slot is indexed by its writer). Never the scratch: it lacks those
/// writes. Whether everything rebuilt is ready again.
async fn rebuild_in_place<S: StorageIO>(parts: &AuditParts<S>, findings: &Findings) -> bool {
    let seal = &parts.seal;
    let ticket = match gate::ticket(&seal.gate, None).await {
        Ok(t) => t,
        Err(e) => {
            error!("{}: the audit could not take a write ticket ({e}); the structures stay as they are", seal.name);
            return false;
        }
    };
    if parts.gone() {
        return false;
    }
    let indexes = seal.indexes.as_ref().filter(|_| findings.missing_entries > 0);
    let filters = seal.filters.as_ref().filter(|_| findings.filter_misses > 0);
    let n_slots = seal.io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64;
    // Read before the demotion: only this build may mark the indexes ready,
    // and only if no renumbering began or ended meanwhile.
    let epoch = indexes.map(|ix| ix.epoch());
    if let Some(ix) = indexes {
        ix.set_ready(Proof::from(&ticket), false);
        ix.clear(Proof::from(&ticket));
    }
    if let Some(f) = filters {
        seal.filters_ready.store(false, Ordering::Release);
        f.set_unindexed_prefix(Proof::from(&ticket), n_slots);
    }
    seal.gate.set_vouched(false);
    parts.cache_generation.fetch_add(1, Ordering::AcqRel);
    // `DIRTY` is durable; a seal refuses unready structures, so nothing
    // needs the ticket while the rebuild runs.
    drop(ticket);
    warn!("{}: rebuilding in place after an index check mismatch", seal.name);

    let mut ready = true;
    if let (Some(ix), Some(epoch)) = (indexes, epoch) {
        let build = gate::build_token();
        match build_indexes(ix, &seal.io_pointers, &seal.io_rows, &parts.fields, Some(&parts.readers)).await {
            Ok(_) if ix.set_ready_at(Proof::from(&build), epoch) => {}
            Ok(_) => {
                info!("{}: the audit's rebuild overlapped a compaction; its result is not used", seal.name);
                ready = false;
            }
            Err(e) => {
                error!("{}: the audit's index rebuild failed; queries scan: {e}", seal.name);
                ready = false;
            }
        }
    }
    if let Some(f) = filters {
        let epoch = f.renumber_epoch();
        match backfill_into(f, &seal.io_pointers, &seal.io_rows, &parts.fields, 0, Some(&parts.readers)).await {
            Ok(_) if f.renumber_epoch() == epoch => seal.filters_ready.store(true, Ordering::Release),
            Ok(_) => ready = false,
            Err(e) => {
                error!("{}: the audit's filter rebuild failed; it stays unused: {e}", seal.name);
                ready = false;
            }
        }
    }
    ready
}
