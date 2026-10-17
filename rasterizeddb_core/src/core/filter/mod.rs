//! Bit-level adaptive filter index.
//!
//! A **positional shadow** of the row-pointer file: bit `i` of the filter
//! describes slot `i` of `{table}_pointers.db`. Because that file is a dense
//! fixed-stride array (`slot = byte_offset / ROW_POINTER_RECORD_LEN`), no
//! key-to-slot map is needed and the whole structure costs *bits* per row
//! rather than bytes per entry.
//!
//! Values are stored as `f`-bit fingerprints, bit-sliced across `f` bitplanes
//! per 512-row zone, so one SIMD register tests 512 rows at once. See
//! [`kernels`] for the probe and `docs/bit-bloom-filter-design.md` for the
//! reasoning, cost model and the tiers not built here.
//!
//! # Safety property
//!
//! Every mask this module produces is a **superset** of the rows that actually
//! match. The `WHERE` evaluator is untouched and still decides truth; the
//! filter only decides what not to bother fetching. Anything the filter cannot
//! speak for — slots above the insert frontier, un-backfilled slots, columns it
//! does not index, predicates it does not understand — defaults to "candidate".

pub mod bloom;
pub mod column;
pub mod hash;
pub mod kernels;
pub mod persist;
pub mod sarg;
pub mod settings;
pub mod sql;
pub mod zonemap;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod nan_bounds_tests;
#[cfg(test)]
mod compact_failure_tests;
#[cfg(test)]
mod vacuum_clear_power_loss_tests;
#[cfg(test)]
mod vacuum_clear_runs_tests;

use parking_lot::RwLock;
use smallvec::SmallVec;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use crate::core::db_type::DbType;
use crate::core::row::row::RowWrite;
use crate::core::row::schema::SchemaField;
use crate::core::storage_providers::traits::StorageIO;

pub use column::{FilterConfig, bitmap_words, choose_f};
pub use hash::{HashSeed, canonical_hash, is_fingerprintable, is_indexable};
pub use sarg::SargProgram;
pub use settings::{FilterSettings, Overrides, Resolved, settings};
pub use zonemap::SargOp;

use column::ColumnFilter;

/// Slots [`TableFilters::on_delete_runs_at`] clears under one hold of a
/// column's lock.
const CLEAR_CHUNK: u64 = 4096;

/// A per-slot candidate bitmap. A set bit means "may match — fetch it".
#[derive(Clone, Debug)]
pub struct CandidateBitmap {
    words: Vec<u64>,
    n_slots: u64,
    /// Admits no slot at all — not even one appended after `n_slots` was
    /// sampled. Only a `WHERE` clause that no row can satisfy earns this: a
    /// pruning *index* may exclude only what it has indexed, but a constant
    /// false predicate excludes every row there will ever be. See
    /// [`CandidateBitmap::nothing`].
    nothing: bool,
    /// The candidates as a sorted slot list, from a secondary index, in place
    /// of `words`. A point lookup over a large table is a handful of slots,
    /// and a dense bitmap for it would be one bit per row allocated and
    /// zeroed for every query. Slots at or past `n_slots` are candidates
    /// either way.
    sparse: Option<Box<[u64]>>,
}

impl CandidateBitmap {
    #[inline]
    pub fn all_candidates(n_slots: u64) -> Self {
        let mut words = vec![u64::MAX; bitmap_words(n_slots)];
        column::mask_tail_pub(&mut words, n_slots);
        Self {
            words,
            n_slots,
            nothing: false,
            sparse: None,
        }
    }

    /// Candidates named slot by slot: `slots` sorted and distinct, every one
    /// below `n_slots`, which is the table length sampled *before* the index
    /// was asked, so a version appended since stays a candidate.
    pub fn sparse(slots: Vec<u64>, n_slots: u64) -> Self {
        debug_assert!(slots.windows(2).all(|w| w[0] < w[1]));
        Self {
            words: Vec::new(),
            n_slots,
            nothing: false,
            sparse: Some(slots.into_boxed_slice()),
        }
    }

    /// Candidates as one bit per slot below `n_slots`, from a secondary
    /// index whose answer is too broad for a slot list. `n_slots` is the
    /// table length sampled *before* the index was asked, as for
    /// [`CandidateBitmap::sparse`]: past it every slot is a candidate.
    pub fn dense(mut words: Vec<u64>, n_slots: u64) -> Self {
        debug_assert_eq!(words.len(), bitmap_words(n_slots));
        // Words it did not provide are candidates, never exclusions.
        words.resize(bitmap_words(n_slots), u64::MAX);
        column::mask_tail_pub(&mut words, n_slots);
        Self {
            words,
            n_slots,
            nothing: false,
            sparse: None,
        }
    }

    #[inline]
    pub fn is_sparse(&self) -> bool {
        self.sparse.is_some()
    }

    /// The slot list itself, sorted and distinct, for a caller that walks it
    /// in order with a cursor of its own rather than searching it once per
    /// slot. `None` for a dense mask. It says nothing about slots at or past
    /// `n_slots`, which are candidates whether listed or not.
    #[inline]
    pub fn sparse_slots(&self) -> Option<&[u64]> {
        self.sparse.as_deref()
    }

    /// The first candidate slot at or after `from`. Past `n_slots` every slot
    /// is a candidate, so this is never more than `max(from, n_slots)`.
    pub fn next_candidate(&self, from: u64) -> u64 {
        if self.nothing {
            return u64::MAX;
        }
        if from >= self.n_slots {
            return from;
        }
        match &self.sparse {
            Some(slots) => {
                let i = slots.partition_point(|s| *s < from);
                slots.get(i).copied().unwrap_or(self.n_slots)
            }
            None => {
                let mut slot = from;
                while slot < self.n_slots {
                    let w = (slot / 64) as usize;
                    let word = self.word(w) & (u64::MAX << (slot % 64));
                    if word != 0 {
                        return (w as u64) * 64 + word.trailing_zeros() as u64;
                    }
                    slot = (w as u64 + 1) * 64;
                }
                self.n_slots.max(from)
            }
        }
    }

    /// For a slot list: the last listed candidate below `limit`, so the
    /// iterator can read from one candidate to the last one a chunk would
    /// reach, and not a byte further. `None` for a dense mask.
    pub fn last_candidate_below(&self, limit: u64) -> Option<u64> {
        let slots = self.sparse.as_ref()?;
        let i = slots.partition_point(|s| *s < limit.min(self.n_slots));
        i.checked_sub(1).map(|i| slots[i])
    }

    /// The mask for a clause no row can satisfy — `1 = 0`, `notnull IS NULL`,
    /// `x IN ()`. The pointer iterator ends at once on it, so the scan reads
    /// nothing; see `sarg::constant_truth`.
    pub fn nothing() -> Self {
        Self {
            words: Vec::new(),
            n_slots: 0,
            nothing: true,
            sparse: None,
        }
    }

    /// Whether this is [`CandidateBitmap::nothing`].
    #[inline]
    pub fn is_nothing(&self) -> bool {
        self.nothing
    }

    /// A mask admitting only slots `[0, upto)`.
    ///
    /// Used to bound a scan by *position* rather than by a count. A count-based
    /// cap cannot be applied to a parallel scan without changing the answer:
    /// workers finish out of order, so stopping after `k` rows have arrived can
    /// yield slots 0–50 and 1500–1650 rather than the first 200. A positional
    /// bound has no such hazard — every slot below the ceiling is examined by
    /// whichever worker owns it, and none above it is.
    ///
    /// The iterator already skips a zero word in one step, so the excluded tail
    /// costs about one test per 64 slots rather than one per slot.
    pub fn leading(n_slots: u64, upto: u64) -> Self {
        let upto = upto.min(n_slots);
        let mut words = vec![0u64; bitmap_words(n_slots)];
        let full = (upto / 64) as usize;
        for w in words.iter_mut().take(full) {
            *w = u64::MAX;
        }
        let rem = upto % 64;
        if rem != 0 {
            if let Some(w) = words.get_mut(full) {
                *w = (1u64 << rem) - 1;
            }
        }
        let mut me = Self {
            words,
            n_slots,
            nothing: false,
            sparse: None,
        };
        column::mask_tail_pub(&mut me.words, n_slots);
        me
    }

    /// Keeps only slots set in both masks.
    ///
    /// `other` shorter than `self` clears the uncovered tail rather than
    /// leaving it set: a mask that does not reach a slot has not admitted it.
    pub fn intersect(&mut self, other: &CandidateBitmap) {
        // A slot list on either side: the answer is a slot list, each slot
        // kept when the other side admits it.
        if self.sparse.is_some() || other.sparse.is_some() {
            let (list, keep_by) = match (&self.sparse, &other.sparse) {
                (Some(mine), _) => (mine.to_vec(), other),
                (None, Some(theirs)) => (theirs.to_vec(), &*self),
                _ => unreachable!(),
            };
            let n_slots = self.n_slots;
            let kept: Vec<u64> = list
                .into_iter()
                .filter(|s| *s < n_slots && keep_by.get(*s))
                .collect();
            *self = CandidateBitmap::sparse(kept, n_slots);
            return;
        }
        for (i, w) in self.words.iter_mut().enumerate() {
            *w &= other.words.get(i).copied().unwrap_or(0);
        }
        column::mask_tail_pub(&mut self.words, self.n_slots);
    }

    #[inline]
    pub fn get(&self, slot: u64) -> bool {
        if self.nothing {
            return false;
        }
        // Past what this bitmap was built to cover: be conservative.
        //
        // Two ways a slot can be uncovered, and only one of them used to be
        // handled. `n_slots` is sampled once, before the scan starts, so a row
        // appended after that sample sits at a slot this bitmap says nothing
        // about. When that slot lands in a *later* word the lookup misses the
        // vector and answers "maybe", which is right. When it lands in the tail
        // of the **last** word it does not: `mask_tail_pub` zeroed those bits so
        // that evaluation would not invent matches beyond the table, and a zero
        // there is indistinguishable from "this row cannot match".
        //
        // So a row written concurrently was pruned from the scan for as long as
        // it fell within 64 slots of the end — which, on a small table, is every
        // row. The version being replaced is already invisible to the reader by
        // then, so the scan matched neither and the row vanished for the length
        // of the write. `UPDATE … RETURNING` read zero rows for a row it went on
        // to update, and a plain `SELECT` could miss a live row outright.
        //
        // A filter index may only ever *exclude* what it has actually indexed.
        if slot >= self.n_slots {
            return true;
        }
        if let Some(slots) = &self.sparse {
            return slots.binary_search(&slot).is_ok();
        }
        let w = (slot / 64) as usize;
        match self.words.get(w) {
            // Past the end of what the bitmap covers: be conservative.
            None => true,
            Some(word) => (word >> (slot % 64)) & 1 == 1,
        }
    }

    /// The bitmap word covering slots `[w*64, w*64+64)`. `u64::MAX` past the end.
    ///
    /// The tail of the last covered word reads as set, for the reason given in
    /// [`CandidateBitmap::get`]: this answers the whole-word skip, which
    /// dismisses 64 records without decoding them, and it must not dismiss
    /// slots the bitmap never covered.
    #[inline]
    pub fn word(&self, w: usize) -> u64 {
        if self.nothing {
            return 0;
        }
        let first = (w as u64) * 64;
        if let Some(slots) = &self.sparse {
            let mut word = 0u64;
            let start = slots.partition_point(|s| *s < first);
            for s in slots[start..].iter().take_while(|s| **s < first + 64) {
                word |= 1 << (s - first);
            }
            let covered = self.n_slots.saturating_sub(first);
            return if covered >= 64 { word } else { word | (u64::MAX << covered) };
        }
        match self.words.get(w) {
            None => u64::MAX,
            Some(word) => {
                let first = (w as u64) * 64;
                let covered = self.n_slots.saturating_sub(first);
                if covered >= 64 {
                    *word
                } else {
                    // Bits at and above `covered` are uncovered, so set them.
                    *word | (u64::MAX << covered)
                }
            }
        }
    }

    #[inline]
    pub fn n_slots(&self) -> u64 {
        self.n_slots
    }

    /// Number of candidate slots. Useful for deciding whether pruning paid off.
    pub fn count(&self) -> u64 {
        if let Some(slots) = &self.sparse {
            return slots.len() as u64;
        }
        self.words.iter().map(|w| w.count_ones() as u64).sum()
    }

    /// The dense words. Empty for a slot list, which only a secondary index
    /// produces and none of this method's callers (the filter's own
    /// persistence and ratio tests) ever receives.
    #[inline]
    pub fn words(&self) -> &[u64] {
        debug_assert!(self.sparse.is_none(), "words() of a sparse candidate list");
        &self.words
    }
}

/// The part of a column that never changes, kept outside the lock.
///
/// Every field here is fixed when the column is built, so the maintenance path
/// can find the row's payload for this column and hash it without holding
/// anything. What is left inside the lock is the bit writes and the frontier.
struct ColumnMeta {
    write_order: u32,
    db_type: DbType,
    /// Whether this column has fingerprint planes at all — false for `BOOL`,
    /// which is indexed for its zone bounds only, and for which hashing would
    /// be work thrown away.
    fingerprints: bool,
}

/// All column filters for one table.
pub struct TableFilters {
    cfg: FilterConfig,
    seed: HashSeed,
    columns: Vec<RwLock<ColumnFilter>>,
    /// Parallel to `columns`. Read without locking.
    meta: Vec<ColumnMeta>,
    /// `write_order` -> index into `columns`.
    lookup: Vec<Option<usize>>,
    /// Slot writes since the last checkpoint, and the table size at that
    /// checkpoint. Together they space checkpoints geometrically so total
    /// rewrite cost stays linear in table size.
    /// Neither of these is padded, deliberately: `Table` evaluates
    /// `should_checkpoint() || checkpoint_requested()` as one expression, so
    /// giving them separate lines put two fills on a path that needs one.
    writes_since_checkpoint: AtomicU64,
    slots_at_checkpoint: AtomicU64,
    /// Whether the on-disk copy is known to be stale. Set before a delete or
    /// update, cleared by a checkpoint.
    disk_dirty: AtomicBool,
    /// Held while the dirty byte is written, so a second mutation waits for
    /// the byte rather than seeing the flag set and writing rows first.
    dirty_marking: tokio::sync::Mutex<()>,
    /// Set by mutations that need a checkpoint sooner than the write threshold
    /// would trigger one.
    checkpoint_requested: AtomicBool,
    /// Whether a checkpoint has been written or loaded, i.e. whether there is
    /// anything on disk that a mutation could invalidate.
    has_checkpoint: AtomicBool,
    /// Meaningful bytes in the sidecar: where the next record goes.
    ///
    /// Tracked here rather than asked of the provider. `LocalStorageProvider`
    /// keeps its length in an atomic that only `append_data` updates, so a file
    /// created or extended by `write_data` reports length 0 in-process until the
    /// provider is reopened (§15.15 of the design doc, and the bug that made a
    /// stale checkpoint load). Every byte in this file was written from here, so
    /// counting them here is both cheaper and the only answer that is right.
    disk_len: AtomicU64,
    /// Delta records written since the last base image.
    deltas_since_base: AtomicU32,
    /// Serialises whole checkpoints against each other.
    ///
    /// Two can be in flight at once — the maintenance task ticks on a timer
    /// while `compact` and `checkpoint_filters` call in directly. Without this
    /// they could each read the same `disk_len`, plan records for the same
    /// offset, and land out of order, so the earlier write's 16-byte stopper
    /// would clobber the later one's header. The file survives that (replay
    /// stops at the bad record and falls back to an earlier state, which is
    /// sound), but it silently throws away a checkpoint, and the fix is one
    /// uncontended lock per checkpoint.
    write_gate: tokio::sync::Mutex<()>,
    /// Bumped when slot numbers start changing under the filter and again
    /// when they stop (`Table::compact` phase 2; see
    /// [`TableFilters::begin_renumbering`]). Work that reads slots and then
    /// writes what it learned into the planes (a backfill, vacuum's clears)
    /// reads this first and writes only if it is unchanged, checked under the
    /// column's lock, so nothing learned under one numbering lands under
    /// another.
    renumber_epoch: AtomicU64,
}

impl TableFilters {
    pub fn new(fields: &[SchemaField], cfg: FilterConfig) -> Self {
        Self::build(fields, cfg, None)
    }

    /// Build for a named table, resolving each column through
    /// [`settings`](settings::settings) so `SET FILTER ... FOR COLUMN t.c`
    /// applies. `new` keeps a fixed config, for tests and internal use.
    pub fn for_table(table: &str, fields: &[SchemaField]) -> Self {
        Self::build(fields, FilterConfig::default(), Some(table))
    }

    fn build(fields: &[SchemaField], cfg: FilterConfig, table: Option<&str>) -> Self {
        let seed = HashSeed::default();
        let mut columns = Vec::new();
        let max_wo = fields.iter().map(|f| f.write_order).max().unwrap_or(0) as usize;
        let mut lookup = vec![None; max_wo + 1];

        for field in fields {
            if field.is_deleted || !is_indexable(&field.db_type) {
                continue;
            }
            // A column can be excluded outright, given its own width bounds, or
            // told to skip zone bounds — see `settings`.
            let (col_cfg, zone_bounds, span_summary) = match table {
                Some(t) => {
                    let r = settings::settings().resolve(t, &field.name);
                    if !r.indexed {
                        continue;
                    }
                    (r.cfg, r.zone_bounds, r.bloom)
                }
                None => (cfg, true, false),
            };
            lookup[field.write_order as usize] = Some(columns.len());
            columns.push(RwLock::new(ColumnFilter::with_options(
                field.write_order as u32,
                field.db_type.clone(),
                seed,
                col_cfg,
                zone_bounds,
                span_summary,
            )));
        }
        let cfg = table
            .map(|t| settings::settings().resolve(t, "").cfg)
            .unwrap_or(cfg);
        let meta = Self::meta_of(&columns);

        Self {
            cfg,
            seed,
            columns,
            meta,
            lookup,
            writes_since_checkpoint: AtomicU64::new(0),
            slots_at_checkpoint: AtomicU64::new(0),
            disk_dirty: AtomicBool::new(false),
            dirty_marking: tokio::sync::Mutex::new(()),
            checkpoint_requested: AtomicBool::new(false),
            has_checkpoint: AtomicBool::new(false),
            disk_len: AtomicU64::new(0),
            deltas_since_base: AtomicU32::new(0),
            write_gate: tokio::sync::Mutex::new(()),
            renumber_epoch: AtomicU64::new(0),
        }
    }

    /// Rebuild from a checkpoint, validating it against the live schema.
    ///
    /// Every rejection path returns `Err` and the caller falls back to an
    /// unindexed table — a discarded sidecar costs a scan, never an answer.
    /// `now_slots` is the pointer file's current slot count. A checkpoint that
    /// claims more slots than that cannot be describing this file.
    pub fn from_checkpoint(
        buf: &[u8],
        fields: &[SchemaField],
        cfg: FilterConfig,
        now_slots: u64,
    ) -> Result<Self, persist::LoadReject> {
        let seed = HashSeed::default();
        let expected: Vec<(u32, DbType)> = fields
            .iter()
            .filter(|f| !f.is_deleted && is_indexable(&f.db_type))
            .map(|f| (f.write_order as u32, f.db_type.clone()))
            .collect();

        let loaded = persist::deserialize(buf, seed, &expected, cfg, now_slots)?;
        // The chain is bounded across restarts, not just within one process, so
        // the count carries over from the file.
        let deltas = loaded.deltas;

        let max_wo = fields.iter().map(|f| f.write_order).max().unwrap_or(0) as usize;
        let mut lookup = vec![None; max_wo + 1];
        let mut columns = Vec::with_capacity(loaded.columns.len());
        for col in loaded.columns {
            lookup[col.write_order as usize] = Some(columns.len());
            columns.push(RwLock::new(col));
        }

        let meta = Self::meta_of(&columns);

        Ok(Self {
            cfg,
            seed,
            columns,
            meta,
            lookup,
            writes_since_checkpoint: AtomicU64::new(0),
            slots_at_checkpoint: AtomicU64::new(loaded.n_slots),
            disk_dirty: AtomicBool::new(false),
            dirty_marking: tokio::sync::Mutex::new(()),
            checkpoint_requested: AtomicBool::new(false),
            has_checkpoint: AtomicBool::new(true),
            disk_len: AtomicU64::new(loaded.valid_len),
            deltas_since_base: AtomicU32::new(deltas),
            write_gate: tokio::sync::Mutex::new(()),
            renumber_epoch: AtomicU64::new(0),
        })
    }

    fn meta_of(columns: &[RwLock<ColumnFilter>]) -> Vec<ColumnMeta> {
        columns
            .iter()
            .map(|c| {
                let g = c.read();
                ColumnMeta {
                    write_order: g.write_order,
                    db_type: g.db_type.clone(),
                    fingerprints: g.has_fingerprints(),
                }
            })
            .collect()
    }

    /// Lowest slot any column still needs indexed. A delta backfill from here
    /// brings the whole catalog current.
    pub fn resume_slot(&self) -> u64 {
        self.columns
            .iter()
            .map(|c| {
                let g = c.read();
                if g.unknown_below() > 0 {
                    0
                } else {
                    g.valid_upto()
                }
            })
            .min()
            .unwrap_or(0)
    }

    /// Serialize the current state. Each column records the frontier it can
    /// actually vouch for, not simply how far it has been filled.
    pub fn checkpoint_bytes(&self, n_slots: u64) -> Vec<u8> {
        let guards: Vec<_> = self.columns.iter().map(|c| c.read()).collect();
        let refs: Vec<&ColumnFilter> = guards.iter().map(|g| &**g).collect();
        persist::serialize(&refs, self.seed, n_slots)
    }

    /// Decide between rewriting the base image and appending a delta, produce
    /// the bytes, and mark every column checkpointed — all under one set of
    /// write guards.
    ///
    /// The guards are held across serialisation deliberately. Serialising under
    /// read guards and then re-taking write guards to clear the dirty marks
    /// leaves a window in which a write lands: the checkpoint would not contain
    /// it and the reset would erase the record that it happened, so the next
    /// incremental checkpoint would not re-send it either. Serialisation is a
    /// memcpy; the I/O happens after the guards are gone.
    fn plan(&self, n_slots: u64) -> Option<(u64, Vec<u8>, bool)> {
        let mut guards: Vec<_> = self.columns.iter().map(|c| c.write()).collect();
        let refs: Vec<&ColumnFilter> = guards.iter().map(|g| &**g).collect();

        let at = self.disk_len.load(Ordering::Relaxed);
        let deltas = self.deltas_since_base.load(Ordering::Relaxed);
        // `dirty` here is the on-disk byte, and clearing it means rewriting the
        // superblock — which an append cannot do. It is also exactly the state
        // where a delta would have to span the file anyway, since the byte is
        // set precisely when an already-covered slot is about to change.
        let dirty = self.disk_dirty.load(Ordering::Relaxed);
        let untouched = refs.iter().all(|c| c.touched_min() == u64::MAX);

        // Nothing was written and nothing is stale, so there is nothing to
        // record. Writing an empty delta each tick would grow the file forever
        // for a table that stopped changing.
        if at > 0 && untouched && !dirty {
            return None;
        }

        // No base yet, a dirty superblock to clear, or a chain long enough that
        // replaying it at open would cost more than rewriting it now.
        let mut plan = if at == 0 || dirty || deltas >= persist::MAX_DELTAS_PER_BASE {
            None
        } else {
            let from: Vec<u64> = refs.iter().map(|c| c.delta_from_zone()).collect();
            let bytes = persist::serialize_delta(&refs, &from, n_slots);
            // A delta that is a large fraction of the file is not saving
            // anything, and folding it into a fresh base image shortens the
            // chain. Half is the point where the next full rewrite would have
            // cost the same anyway.
            if bytes.len() as u64 * 2 >= at {
                None
            } else {
                Some(bytes)
            }
        };

        let is_base = plan.is_none();
        let mut bytes = plan
            .take()
            .unwrap_or_else(|| persist::serialize(&refs, self.seed, n_slots));
        let offset = if is_base { 0 } else { at };
        let end = offset + bytes.len() as u64;
        persist::terminate(&mut bytes);

        for g in guards.iter_mut() {
            g.mark_checkpointed();
        }
        // Published while the guards are still held, so the whole decision —
        // what to write, where, and that these columns are now clean — is
        // atomic with respect to another planner.
        self.disk_len.store(end, Ordering::Relaxed);
        if is_base {
            self.deltas_since_base.store(0, Ordering::Relaxed);
        } else {
            self.deltas_since_base.fetch_add(1, Ordering::Relaxed);
        }
        drop(guards);

        Some((offset, bytes, is_base))
    }

    /// Write a checkpoint through `io`, then mark every column clean.
    ///
    /// The sidecar is a base image followed by an append log. A checkpoint
    /// usually only has to describe the zones written since the last one, which
    /// for an append-only table is a small tail rather than the whole index —
    /// the difference between paying for the table and paying for the writes.
    /// It falls back to rewriting the base when there is nothing to append to,
    /// when the chain has grown long enough to slow the next open, or when the
    /// delta is large enough that rewriting costs the same and leaves less to
    /// replay. A `DELETE` reaches that last case by construction: it invalidates
    /// slots near the start, so its "tail" is the whole file.
    pub async fn checkpoint<S: StorageIO>(&self, io: &S, n_slots: u64) {
        // Planning and writing have to be one step: see `write_gate`.
        let _gate = self.write_gate.lock().await;
        if let Some((offset, bytes, _is_base)) = self.plan(n_slots) {
            // Test-only: parks a planned checkpoint before its write.
            #[cfg(test)]
            checkpoint_test_hooks::planned(self).await;
            io.write_data(offset, &bytes).await;
        }
        self.writes_since_checkpoint.store(0, Ordering::Relaxed);
        self.slots_at_checkpoint.store(n_slots, Ordering::Relaxed);
        self.disk_dirty.store(false, Ordering::Relaxed);
        self.checkpoint_requested.store(false, Ordering::Relaxed);
        self.has_checkpoint.store(true, Ordering::Release);
    }

    /// Mark the on-disk copy stale before mutating already-checkpointed rows.
    ///
    /// Idempotent, so a run of deletes costs a single byte of I/O. Skipped
    /// entirely when nothing has been checkpointed yet — there is no stale copy
    /// to invalidate, and writing the byte would create a sidecar with no valid
    /// superblock.
    pub async fn mark_disk_dirty<S: StorageIO>(&self, io: &S) {
        if !self.has_checkpoint.load(Ordering::Acquire) {
            return;
        }
        // Set only once the byte is written. Updates no longer all take
        // turns: one that found the flag set before the byte landed would
        // write rows a crash could pair with a checkpoint still marked clean.
        if self.disk_dirty.load(Ordering::Acquire) {
            return;
        }
        let _marking = self.dirty_marking.lock().await;
        if self.disk_dirty.load(Ordering::Acquire) {
            return;
        }
        persist::mark_dirty_on_disk(io).await;
        self.disk_dirty.store(true, Ordering::Release);
    }

    /// Ask the maintenance task to checkpoint at its next tick.
    ///
    /// Used by deletes and updates: they leave the sidecar marked dirty, and a
    /// dirty sidecar is discarded on the next open. Rewriting the blob inline
    /// would make a one-row `DELETE` pay for the whole index, so the request is
    /// handed to the background task instead.
    #[inline]
    pub fn request_checkpoint(&self) {
        self.checkpoint_requested.store(true, Ordering::Relaxed);
    }

    /// Meaningful bytes in the sidecar — where the next record will be written.
    ///
    /// Excludes the [`persist::terminate`] stopper, which the next record
    /// overwrites.
    #[inline]
    pub fn sidecar_len(&self) -> u64 {
        self.disk_len.load(Ordering::Relaxed)
    }

    /// Delta records appended since the last full base image.
    #[inline]
    pub fn deltas_since_base(&self) -> u32 {
        self.deltas_since_base.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn checkpoint_requested(&self) -> bool {
        self.checkpoint_requested.load(Ordering::Relaxed)
    }

    /// Whether enough has changed to be worth rewriting the sidecar.
    ///
    /// The threshold grows with the table so the rewrite cost stays linear
    /// overall rather than quadratic in the number of checkpoints.
    pub fn should_checkpoint(&self) -> bool {
        let since = self.writes_since_checkpoint.load(Ordering::Relaxed);
        let base = self.slots_at_checkpoint.load(Ordering::Relaxed) / 4;
        since >= base.max(65_536)
    }

    #[inline]
    fn note_writes(&self, n: u64) {
        self.writes_since_checkpoint.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn config(&self) -> &FilterConfig {
        &self.cfg
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    #[inline]
    fn index_of(&self, write_order: u32) -> Option<usize> {
        self.lookup.get(write_order as usize).copied().flatten()
    }

    /// Whether this column's values ascend with slot position across the whole
    /// of `n_slots`.
    ///
    /// When true, slot order *is* sort order for that column, so `ORDER BY it
    /// LIMIT k` can be answered from the leading slots instead of ordering the
    /// table. Ids and append-time stamps satisfy it by construction; anything
    /// written out of order does not, and says so.
    ///
    /// Requires the column's index to reach `n_slots`: a tail nobody has
    /// indexed could hold a value that breaks the order, and the bounds cannot
    /// rule that out. Returns `false` for a column with no zone map, which
    /// includes every non-numeric column.
    pub fn column_is_monotonic(&self, write_order: u32, n_slots: u64) -> bool {
        let Some(idx) = self.index_of(write_order) else {
            return false;
        };
        let column = self.columns[idx].read();
        // The index must cover every slot the scan will see.
        if column.valid_upto() < n_slots {
            return false;
        }
        column
            .zonemap()
            .is_some_and(|zm| zm.is_monotonic_ascending(zonemap::zones_for(n_slots)))
    }

    #[inline]
    pub fn indexes(&self, write_order: u32) -> bool {
        self.index_of(write_order).is_some()
    }

    /// Mark slots `[0, slots)` as predating the filter. They stay candidates
    /// until [`Self::clear_unindexed_prefix`] follows a backfill.
    pub fn set_unindexed_prefix(&self, slots: u64) {
        for c in &self.columns {
            c.write().set_unindexed_prefix(slots);
        }
    }

    pub fn clear_unindexed_prefix(&self) {
        for c in &self.columns {
            c.write().clear_unindexed_prefix();
        }
    }

    /// See [`TableFilters::begin_renumbering`].
    #[inline]
    pub fn renumber_epoch(&self) -> u64 {
        self.renumber_epoch.load(Ordering::Acquire)
    }

    /// Called before the first slot is renumbered. Every slot below `n_slots`
    /// becomes a candidate again, in memory and in every checkpoint written
    /// from here on (the prefix is persisted, so a reopened table backfills
    /// it), until a backfill under the new numbering clears it with
    /// [`TableFilters::clear_unindexed_prefix_at`]. A renumbering that fails
    /// part-way therefore leaves the filter demoted rather than answering by
    /// slot numbers that moved.
    ///
    /// The epoch is bumped before the columns are demoted, and each column's
    /// lock is taken after the bump, so an epoch-checked write that saw the
    /// old epoch has landed before the demotion and before any slot moves.
    pub fn begin_renumbering(&self, n_slots: u64) {
        self.renumber_epoch.fetch_add(1, Ordering::AcqRel);
        for c in &self.columns {
            c.write().demote_below(n_slots);
        }
    }

    /// [`TableFilters::begin_renumbering`], ordered against checkpoints, with
    /// the sidecar's dirty byte written whatever the in-memory flag says.
    ///
    /// Under `write_gate`, so a checkpoint planned before this (a base image
    /// of the old numbering, which then clears `disk_dirty`) has landed before
    /// the byte is written, and one planned after it sees the demotion and
    /// persists it. Without the gate the flag can read true while such a
    /// checkpoint is about to clear it, which is the case
    /// [`TableFilters::mark_disk_dirty`]'s fast path skips; the byte is
    /// written unconditionally here so this does not lean on the flag. A crash
    /// part-way through the renumbering then reopens to a sidecar that is
    /// either rejected or demoted, never one that trusts the old numbering.
    pub async fn begin_renumbering_on_disk<S: StorageIO>(&self, io: &S, n_slots: u64) {
        let _gate = self.write_gate.lock().await;
        self.begin_renumbering(n_slots);
        if self.has_checkpoint.load(Ordering::Acquire) {
            let _marking = self.dirty_marking.lock().await;
            persist::mark_dirty_on_disk(io).await;
            self.disk_dirty.store(true, Ordering::Release);
        }
    }

    /// Called once slot numbers have stopped changing, whether the
    /// renumbering finished or failed: work that read its epoch during it
    /// read a mix of both numberings.
    pub fn end_renumbering(&self) {
        self.renumber_epoch.fetch_add(1, Ordering::AcqRel);
    }

    /// [`TableFilters::clear_unindexed_prefix`], unless the slots were
    /// renumbered since `epoch` was read. Returns whether every column was
    /// cleared.
    pub fn clear_unindexed_prefix_at(&self, epoch: u64) -> bool {
        for c in &self.columns {
            let mut g = c.write();
            if self.renumber_epoch() != epoch {
                return false;
            }
            g.clear_unindexed_prefix();
        }
        true
    }

    /// Clears every slot in `runs` (`(first, count)` pairs), unless the slots
    /// were renumbered since `epoch` was read. Vacuum's deferred clears: the
    /// slot numbers come from a sweep that read `epoch` before it started.
    /// Returns whether the clears were applied.
    ///
    /// Each column's lock is yielded to waiting inserts and queries every
    /// [`CLEAR_CHUNK`] slots, and the epoch checked again after each, so a
    /// large scattered delete does not hold a column for the whole list. Any
    /// clears applied before a `false` return are still sound: each chunk was
    /// checked under the numbering it was written in, and the caller re-clears
    /// on the next sweep.
    pub fn on_delete_runs_at(&self, epoch: u64, runs: &[(u64, u64)]) -> bool {
        for cell in &self.columns {
            let mut g = cell.write();
            if self.renumber_epoch() != epoch {
                return false;
            }
            let mut in_chunk = 0u64;
            for &(first, count) in runs {
                let end = first + count;
                let mut slot = first;
                while slot < end {
                    let stop = end.min(slot + (CLEAR_CHUNK - in_chunk));
                    for s in slot..stop {
                        g.set(s, None);
                    }
                    in_chunk += stop - slot;
                    slot = stop;
                    if in_chunk == CLEAR_CHUNK {
                        in_chunk = 0;
                        parking_lot::RwLockWriteGuard::bump(&mut g);
                        if self.renumber_epoch() != epoch {
                            return false;
                        }
                    }
                }
            }
        }
        self.note_writes(runs.iter().map(|r| r.1).sum());
        true
    }

    pub fn is_complete(&self) -> bool {
        self.columns.iter().all(|c| c.read().is_complete())
    }

    /// Index every column of a freshly written row.
    ///
    /// Columns absent from `row_write` are explicitly cleared rather than left
    /// alone: a slot that is reused by a relocating update must not keep the
    /// previous occupant's fingerprint.
    pub fn on_insert(&self, slot: u64, row_write: &RowWrite) {
        for (i, m) in self.meta.iter().enumerate() {
            // Both of these used to run inside the lock. Finding the payload is
            // a scan of the row's columns, which made maintenance quadratic in
            // column count *while holding it*, and hashing is the dominant cost
            // of the whole operation (§11.1). Neither needs the lock: the column
            // metadata they use is fixed for the column's lifetime.
            let bytes = row_write
                .columns_writing_data
                .iter()
                .find(|c| c.write_order == m.write_order)
                .map(|p| p.data.into_slice());
            let hash = match (m.fingerprints, bytes) {
                (true, Some(b)) => ColumnFilter::hash_of(&m.db_type, self.seed, b),
                _ => None,
            };
            self.columns[i].write().set_prepared(slot, bytes, hash);
        }
        self.note_writes(1);
    }

    /// Index a run of freshly written rows, taking each column's lock once for
    /// the whole run rather than once per row.
    ///
    /// # Why batching and not atomic plane words
    ///
    /// §8 of the design doc specifies an atomic open zone, and measurement says
    /// that is the wrong shape for *this* contention. Concurrent appends all
    /// land in the same open zone, so its words are the contended cache lines
    /// either way; going lock-free replaces one lock acquisition per row with
    /// `2f` atomic read-modify-writes on those same lines — sixteen ping-pongs
    /// instead of one. The design doc concedes as much in the same paragraph
    /// ("8 locked ops/row is worth avoiding") and proposes exactly this batched
    /// fast path beside it.
    ///
    /// Measured with `report_insert_contention` in `benches/filter_benchmarks`:
    /// per-row locking scales *negatively*, from 9.0 M rows/s on one thread to
    /// 2.0 M on eight. Shortening the critical section does not help — hashing
    /// and payload lookup moved out of it for nothing measurable, which is what
    /// says the cost is the acquisition rather than the work inside.
    ///
    /// The other reason the §8 shape does not fit: it assumes sealed zones are
    /// immutable, and here they are not. `UPDATE`, `DELETE`, backfill and
    /// reclamation all write into zones long past the frontier, so there is no
    /// immutable tail to publish by pointer swap.
    pub fn on_insert_batch(&self, rows: &[(u64, &RowWrite)]) {
        if rows.is_empty() {
            return;
        }
        // Reused across columns so a wide table does not allocate per column.
        let mut prepared: Vec<(u64, Option<&[u8]>, Option<u64>)> = Vec::with_capacity(rows.len());

        for (i, m) in self.meta.iter().enumerate() {
            prepared.clear();
            for (slot, row_write) in rows {
                let bytes = row_write
                    .columns_writing_data
                    .iter()
                    .find(|c| c.write_order == m.write_order)
                    .map(|p| p.data.into_slice());
                let hash = match (m.fingerprints, bytes) {
                    (true, Some(b)) => ColumnFilter::hash_of(&m.db_type, self.seed, b),
                    _ => None,
                };
                prepared.push((*slot, bytes, hash));
            }

            let mut f = self.columns[i].write();
            for (slot, bytes, hash) in prepared.iter() {
                f.set_prepared(*slot, *bytes, *hash);
            }
        }
        self.note_writes(rows.len() as u64);
    }

    /// Clear every column at `slot`. Exact — unlike a Bloom filter, deleting
    /// *improves* the false-positive rate rather than degrading it.
    pub fn on_delete(&self, slot: u64) {
        for cell in &self.columns {
            cell.write().set(slot, None);
        }
        self.note_writes(1);
    }

    /// Total heap bytes across all column planes.
    pub fn bytes(&self) -> usize {
        self.columns.iter().map(|c| c.read().bytes()).sum()
    }

    /// Per-column diagnostics: `(write_order, f, estimated distinct, bytes)`.
    pub fn stats(&self) -> Vec<(u32, u8, u64, usize)> {
        self.columns
            .iter()
            .map(|c| {
                let g = c.read();
                // Width 0 means "no fingerprint planes". Two ways to get there:
                // a gated-out column, maintained but not probed, and a
                // bounds-only column that never had planes to begin with.
                let f = if g.is_gated_out() || !g.has_fingerprints() {
                    0
                } else {
                    g.current_f()
                };
                (g.write_order, f, g.estimated_distinct(), g.bytes())
            })
            .collect()
    }

    /// Evaluate a conjunction of predicates into a candidate bitmap.
    ///
    /// Returns `None` when nothing useful could be pruned, in which case the
    /// caller should scan normally rather than pay for a bitmap.
    pub fn evaluate(&self, program: &SargProgram, n_slots: u64) -> Option<CandidateBitmap> {
        if program.is_empty() || n_slots == 0 {
            return None;
        }
        let words = bitmap_words(n_slots);
        let mut scratch = vec![0u64; words];
        self.eval(&program.root, n_slots, &mut scratch)
            .map(|words| CandidateBitmap {
                words,
                n_slots,
                nothing: false,
                sparse: None,
            })
    }

    /// Evaluate one node. `None` means "all ones" — no information — which is
    /// the identity for AND and absorbing for OR.
    fn eval(&self, e: &sarg::MaskExpr, n_slots: u64, scratch: &mut [u64]) -> Option<Vec<u64>> {
        use sarg::MaskExpr;
        match e {
            MaskExpr::All => None,

            MaskExpr::Pred(pred) => {
                let idx = self.index_of(pred.write_order)?;
                let guard = self.columns[idx].read();
                let usable = guard.probe_pred_into(pred.op, &pred.value, n_slots, scratch);
                drop(guard);
                // Nothing this column can say (unhashable value, or a range on a
                // type with no ordered bounds).
                if usable { Some(scratch.to_vec()) } else { None }
            }

            // Intersection: unknown branches simply drop out. Independent
            // predicates compound here, which is why composite multi-column
            // filters are unnecessary.
            MaskExpr::And(branches) => {
                let mut acc: Option<Vec<u64>> = None;
                for b in branches {
                    let Some(m) = self.eval(b, n_slots, scratch) else {
                        continue;
                    };
                    match acc.as_mut() {
                        Some(a) => {
                            for (d, s) in a.iter_mut().zip(m.iter()) {
                                *d &= *s;
                            }
                        }
                        None => acc = Some(m),
                    }
                }
                acc
            }

            // Union: a single unknown branch makes the whole union unknown, so
            // bail as soon as one appears rather than building a full bitmap.
            //
            // An `IN` list arrives as an `Or` of equalities on one column, and
            // gets a path of its own — see `eval_in_list`.
            MaskExpr::Or(branches) if in_list_column(branches).is_some() => {
                self.eval_in_list(branches, n_slots, scratch)
            }
            MaskExpr::Or(branches) => {
                let mut acc: Option<Vec<u64>> = None;
                for b in branches {
                    let m = self.eval(b, n_slots, scratch)?;
                    match acc.as_mut() {
                        Some(a) => {
                            for (d, s) in a.iter_mut().zip(m.iter()) {
                                *d |= *s;
                            }
                        }
                        None => acc = Some(m),
                    }
                }
                acc
            }
        }
    }

    /// The mask for `col IN (v1, …, vN)`: an `Or` whose every branch is
    /// `col = v` on the same column.
    ///
    /// The general `Or` evaluates each branch into a fresh bitmap — a probe and
    /// a full-bitmap copy per value, so a 50,000-value list over a million rows
    /// copied ~6 GB to build one mask. Here a short list ORs its probes into
    /// one accumulator with no copies, and a long one (more than
    /// [`IN_LIST_PROBE_MAX`] values) is bounded by the list's `[min, max]`
    /// instead: every matching row lies in that range, so the zones the range
    /// rules out hold no match. Coarser than the probes, never unsound — and
    /// any value or column the index cannot speak for makes the whole list
    /// "no information", as one unknown branch does for `Or`.
    fn eval_in_list(
        &self,
        branches: &[sarg::MaskExpr],
        n_slots: u64,
        scratch: &mut [u64],
    ) -> Option<Vec<u64>> {
        let write_order = in_list_column(branches)?;
        let values: Vec<&[u8]> = branches
            .iter()
            .filter_map(|b| match b {
                sarg::MaskExpr::Pred(p) => Some(p.value.as_slice()),
                _ => None,
            })
            .collect();
        let idx = self.index_of(write_order)?;
        let guard = self.columns[idx].read();

        if values.len() <= IN_LIST_PROBE_MAX {
            let mut acc = vec![0u64; scratch.len()];
            for v in &values {
                if !guard.probe_pred_into(SargOp::Eq, v, n_slots, scratch) {
                    return None;
                }
                for (d, s) in acc.iter_mut().zip(scratch.iter()) {
                    *d |= *s;
                }
            }
            return Some(acc);
        }

        // The list's bounds in the column's own order — the order the zone
        // bounds were maintained in. A pair the evaluator cannot order gives
        // up rather than guess.
        let ty = guard.db_type.clone();
        let less = |a: &[u8], b: &[u8]| {
            crate::core::row::logical::perform_comparison_operation(
                a,
                b,
                &ty,
                &crate::core::processor::transformer::ComparerOperation::Less,
            )
            .ok()
        };
        let mut lo = values[0];
        let mut hi = values[0];
        for v in &values[1..] {
            if less(v, lo)? {
                lo = v;
            }
            if less(hi, v)? {
                hi = v;
            }
        }
        if !guard.probe_pred_into(SargOp::Ge, lo, n_slots, scratch) {
            return None;
        }
        let mut acc = scratch.to_vec();
        if !guard.probe_pred_into(SargOp::Le, hi, n_slots, scratch) {
            return None;
        }
        for (d, s) in acc.iter_mut().zip(scratch.iter()) {
            *d &= *s;
        }
        Some(acc)
    }

    /// Index a row read back from storage, for backfill. `columns` are
    /// `(write_order, bytes)` pairs.
    pub fn backfill_row(&self, slot: u64, columns: &SmallVec<[(u32, Vec<u8>); 32]>) {
        for (i, m) in self.meta.iter().enumerate() {
            let bytes = columns
                .iter()
                .find(|(w, _)| *w == m.write_order)
                .map(|(_, b)| b.as_slice());
            self.columns[i].write().set_reindex(slot, bytes);
        }
    }

    /// Backfill a run of rows, taking each column's lock once for the run.
    ///
    /// A whole-table backfill is the largest batch the engine ever has, and it
    /// is the one place where lock acquisitions dominate rather than merely
    /// appear — see [`Self::on_insert_batch`] for the measurement.
    pub fn backfill_batch(&self, rows: &[(u64, SmallVec<[(u32, Vec<u8>); 32]>)]) {
        self.backfill_batch_at(self.renumber_epoch(), rows);
    }

    /// [`TableFilters::backfill_batch`], unless the slots were renumbered
    /// since `epoch` was read: rows read under one numbering must not be
    /// written under another. Returns whether the batch was applied.
    pub fn backfill_batch_at(&self, epoch: u64, rows: &[(u64, SmallVec<[(u32, Vec<u8>); 32]>)]) -> bool {
        if rows.is_empty() {
            return self.renumber_epoch() == epoch;
        }
        for (i, m) in self.meta.iter().enumerate() {
            let mut f = self.columns[i].write();
            if self.renumber_epoch() != epoch {
                return false;
            }
            for (slot, columns) in rows {
                let bytes = columns
                    .iter()
                    .find(|(w, _)| *w == m.write_order)
                    .map(|(_, b)| b.as_slice());
                f.set_reindex(*slot, bytes);
            }
        }
        true
    }
}

/// The longest `IN` list whose values are probed one by one; past it the
/// list's `[min, max]` range is used. See `TableFilters::eval_in_list`.
pub const IN_LIST_PROBE_MAX: usize = 64;

/// The column an `Or` tests when every branch is `col = v` on that one column
/// — the shape `x IN (…)` lowers to — with at least two branches.
fn in_list_column(branches: &[sarg::MaskExpr]) -> Option<u32> {
    if branches.len() < 2 {
        return None;
    }
    let mut column = None;
    for b in branches {
        let sarg::MaskExpr::Pred(p) = b else {
            return None;
        };
        if p.op != SargOp::Eq || column.is_some_and(|c| c != p.write_order) {
            return None;
        }
        column = Some(p.write_order);
    }
    column
}

/// Parks one planned checkpoint of a chosen `TableFilters` between its plan
/// and its write, for tests that need something to happen in that window.
#[cfg(test)]
pub(crate) mod checkpoint_test_hooks {
    use std::sync::Arc;

    #[derive(Default)]
    pub(crate) struct Park {
        /// Notified once the checkpoint has planned and is parked.
        pub planned: tokio::sync::Notify,
        /// Notify to let it write.
        pub release: tokio::sync::Notify,
    }

    static PARK: parking_lot::Mutex<Option<(usize, Arc<Park>)>> = parking_lot::Mutex::new(None);

    /// Parks the next checkpoint of `filters` that has something to write.
    pub(crate) fn arm(filters: &super::TableFilters) -> Arc<Park> {
        let park = Arc::new(Park::default());
        *PARK.lock() = Some((filters as *const _ as usize, park.clone()));
        park
    }

    pub(super) async fn planned(filters: &super::TableFilters) {
        let park = {
            let mut g = PARK.lock();
            match g.as_ref() {
                Some((at, _)) if *at == filters as *const _ as usize => g.take().map(|(_, p)| p),
                _ => None,
            }
        };
        if let Some(park) = park {
            park.planned.notify_one();
            park.release.notified().await;
        }
    }
}

#[cfg(test)]
mod leading_mask_tests {
    use super::*;

    fn set_slots(bm: &CandidateBitmap, n: u64) -> Vec<u64> {
        (0..n).filter(|s| bm.get(*s)).collect()
    }

    #[test]
    fn leading_admits_exactly_the_first_slots() {
        let bm = CandidateBitmap::leading(200, 5);
        assert_eq!(set_slots(&bm, 200), vec![0, 1, 2, 3, 4]);
        assert_eq!(bm.count(), 5);
    }

    /// Word boundaries are where an off-by-one would hide.
    #[test]
    fn leading_is_exact_across_word_boundaries() {
        for upto in [0u64, 1, 63, 64, 65, 127, 128, 129] {
            let bm = CandidateBitmap::leading(300, upto);
            assert_eq!(bm.count(), upto, "upto={}", upto);
            assert!(!bm.get(upto), "slot {} must be excluded", upto);
            if upto > 0 {
                assert!(bm.get(upto - 1), "slot {} must be included", upto - 1);
            }
        }
    }

    /// Asking for more than exists is a full mask, not an overflow.
    #[test]
    fn leading_saturates_at_the_slot_count() {
        let bm = CandidateBitmap::leading(70, 1_000);
        assert_eq!(bm.count(), 70);
    }

    #[test]
    fn intersect_keeps_only_what_both_admit() {
        let mut a = CandidateBitmap::leading(200, 100);
        let b = CandidateBitmap::leading(200, 30);
        a.intersect(&b);
        assert_eq!(a.count(), 30);
        assert!(a.get(29) && !a.get(30));
    }

    /// The composition the query path relies on: a WHERE mask narrowed to a
    /// slot ceiling must admit only rows satisfying both.
    /// A filter index may only exclude slots it actually covers.
    ///
    /// `n_slots` is sampled once, before a scan starts, and the file can grow
    /// while the scan runs. A slot past that sample was never indexed, so the
    /// only safe answer for it is "maybe" — it has to be decoded and checked.
    /// Answering "no" prunes a row that was written concurrently, and because
    /// the version it replaces is already invisible to that reader, the row
    /// disappears from the scan altogether.
    ///
    /// The trap is that the tail of the *last covered word* is zeroed by
    /// `mask_tail_pub`, so it reads as a decided "no" rather than falling off
    /// the end of the vector into the conservative answer.
    #[test]
    fn slots_past_the_sampled_count_are_never_excluded() {
        // 100 is not a multiple of 64: slots 100..128 live in the tail of word 1.
        let bm = CandidateBitmap::leading(100, 0);

        // Covered and genuinely excluded.
        assert!(!bm.get(0));
        assert!(!bm.get(99));

        // Never covered — these must not be pruned.
        for slot in [100u64, 101, 127, 128, 5_000] {
            assert!(
                bm.get(slot),
                "slot {} was never covered and must read as a candidate",
                slot
            );
        }

        // The whole-word skip dismisses 64 records without decoding them, so it
        // must not dismiss the word holding uncovered slots.
        assert_ne!(
            bm.word(1),
            0,
            "word 1 holds uncovered slots and must not be skipped wholesale"
        );
    }

    #[test]
    fn intersect_composes_with_a_predicate_mask() {
        let mut predicate = CandidateBitmap::all_candidates(200);
        let ceiling = CandidateBitmap::leading(200, 64);
        predicate.intersect(&ceiling);
        assert_eq!(predicate.count(), 64);
        assert!(predicate.get(63) && !predicate.get(64));
    }
}

/// The `IN`-list mask: the general `Or` (a probe and a full-bitmap copy per
/// value — the path every list took before) against `eval_in_list`.
///
/// `cargo test --release -p rasterizeddb_core --lib -- --ignored in_list_mask_bench --nocapture`
#[cfg(test)]
mod in_list_mask_bench {
    use super::*;
    use smallvec::SmallVec;

    impl TableFilters {
        /// The `Or` arm as it was, kept as the reference for the measurement
        /// and the agreement check below.
        fn eval_general_or(&self, branches: &[sarg::MaskExpr], n_slots: u64) -> Option<Vec<u64>> {
            let mut scratch = vec![0u64; bitmap_words(n_slots)];
            let mut acc: Option<Vec<u64>> = None;
            for b in branches {
                let m = self.eval(b, n_slots, &mut scratch)?;
                match acc.as_mut() {
                    Some(a) => {
                        for (d, s) in a.iter_mut().zip(m.iter()) {
                            *d |= *s;
                        }
                    }
                    None => acc = Some(m),
                }
            }
            acc
        }
    }

    #[test]
    #[ignore]
    fn in_list_mask_bench() {
        const N: u64 = 1_000_000;
        let field = SchemaField::new("x".to_string(), DbType::I64, 8, 0, 0, false);
        let filters = TableFilters::new(&[field], FilterConfig::default());
        // Clustered values (the common case of an id), so ranges can prune.
        let rows: Vec<(u64, SmallVec<[(u32, Vec<u8>); 32]>)> = (0..N)
            .map(|slot| {
                let mut c: SmallVec<[(u32, Vec<u8>); 32]> = SmallVec::new();
                c.push((0, (slot as i64).to_le_bytes().to_vec()));
                (slot, c)
            })
            .collect();
        filters.backfill_batch(&rows);
        filters.clear_unindexed_prefix();
        for n in [10usize, 1_000, 50_000] {
            // Values spread over a tenth of the table.
            let branches: Vec<sarg::MaskExpr> = (0..n)
                .map(|i| {
                    sarg::MaskExpr::Pred(sarg::SargPredicate {
                        write_order: 0,
                        op: SargOp::Eq,
                        value: (((i as u64 * 100_003) % (N / 10)) as i64).to_le_bytes().to_vec(),
                    })
                })
                .collect();
            // The general Or at 50,000 values takes many seconds; once is enough.
            let rounds = if n >= 50_000 { 1 } else { 3 };
            for round in 0..rounds {
                let t = std::time::Instant::now();
                let old = self::TableFilters::eval_general_or(&filters, &branches, N);
                let old_t = t.elapsed();
                let t = std::time::Instant::now();
                let mut scratch = vec![0u64; bitmap_words(N)];
                let new = filters.eval_in_list(&branches, N, &mut scratch);
                let new_t = t.elapsed();
                let (o, w) = (old.unwrap(), new.unwrap());
                // Sound: everything the old mask admitted, the new one does.
                for (a, b) in o.iter().zip(w.iter()) {
                    assert_eq!(a & !b, 0, "the new mask dropped a candidate");
                }
                let ones = |v: &[u64]| v.iter().map(|w| w.count_ones() as u64).sum::<u64>();
                println!(
                    "IN of {n:>6} over {N} slots, round {round}: general Or {old_t:>10.2?} ({} candidates), in-list {new_t:>10.2?} ({} candidates)",
                    ones(&o),
                    ones(&w)
                );
            }
        }
    }
}
