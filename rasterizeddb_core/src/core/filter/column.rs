//! Per-column filter state: generations, adaptive width, and the watermark
//! that makes the whole thing safe under concurrent inserts.

use std::collections::BTreeSet;

use crate::core::db_type::DbType;

use super::bloom::{self, SuperBloom};
use super::hash::{CODE_EMPTY, HashSeed, canonical_hash, fingerprint, is_fingerprintable};
use super::kernels::{MAX_F, MIN_F, ZONE_ROWS, ZONE_WORDS, probe_fn, read_slot, write_slot};
use super::zonemap::{SargOp, ZoneMap, zones_for};

/// Tuning knobs. Defaults are the ones argued for in the design doc.
#[derive(Clone, Copy, Debug)]
pub struct FilterConfig {
    pub min_f: u8,
    pub default_f: u8,
    pub max_f: u8,
    /// Largest fraction of the table an equality predicate may match and still
    /// be worth filtering. A column with `distinct < 1/min_selectivity` values
    /// is not indexed: every probe would return that share of the table anyway.
    ///
    /// Note this threshold is **absolute, not scaled by row count** — 1024
    /// distinct values is 0.1 % selectivity whether the table holds a million
    /// rows or a billion.
    pub min_selectivity: f64,
    /// Tables smaller than this are cheaper to scan outright, so a query
    /// against one does not consult the index at all.
    ///
    /// Enforced in `Table::build_candidates` rather than here: the decision is
    /// about whether a mask is worth building for a scan, not about how wide a
    /// column's fingerprints should be. Gating it in `choose_f` instead would
    /// take each column out of service as it crossed a zone boundary, which
    /// makes a checkpoint's contents depend on the table's size at the moment
    /// it was taken.
    ///
    /// Counted in **slots, not live rows** — the scan walks slots, so that is
    /// the work a mask would be saving.
    pub min_table_rows: u64,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            min_f: MIN_F,
            default_f: 8,
            max_f: MAX_F,
            min_selectivity: 0.02,
            min_table_rows: 4096,
        }
    }
}

/// Choose fingerprint width from observed cardinality.
///
/// ```text
/// false fetches = N_rows * 2^-f        (independent of cardinality)
/// true matches  = N_rows / distinct
/// balanced when 2^-f ~= 1/distinct  =>  f ~= log2(distinct)
/// ```
///
/// Returns `0` when the column should not be indexed at all — the gate that
/// keeps a low-cardinality `status` column from costing a byte per row for no
/// benefit.
pub fn choose_f(distinct: u64, n_rows: u64, cfg: &FilterConfig) -> u8 {
    if n_rows == 0 {
        return cfg.default_f;
    }
    // Selectivity gate. An equality probe returns ~1/distinct of the table, so
    // the threshold depends on `distinct` alone, NOT on how big the table is.
    let min_distinct = (1.0 / cfg.min_selectivity).ceil() as u64;
    if distinct < min_distinct {
        return 0;
    }
    let f = (distinct.max(2) as f64).log2().ceil() as u8;
    f.clamp(cfg.min_f, cfg.max_f)
}

/// 256-register HyperLogLog with linear-counting correction for small
/// cardinalities. 256 bytes per column, ~6.5 % standard error — ample for
/// picking an integer bit width.
#[derive(Clone)]
pub struct Hll {
    regs: Box<[u8; 256]>,
}

impl Default for Hll {
    fn default() -> Self {
        Self {
            regs: Box::new([0u8; 256]),
        }
    }
}

impl Hll {
    #[inline]
    pub fn regs(&self) -> &[u8; 256] {
        &self.regs
    }

    #[inline]
    pub fn from_regs(regs: [u8; 256]) -> Self {
        Self {
            regs: Box::new(regs),
        }
    }

    #[inline]
    pub fn add(&mut self, h: u64) {
        let idx = (h >> 56) as usize;
        // Sentinel low bits cap the rank at 57 and avoid leading_zeros() on 0.
        let rest = (h << 8) | 0xFF;
        let rank = rest.leading_zeros() as u8 + 1;
        let r = &mut self.regs[idx];
        if rank > *r {
            *r = rank;
        }
    }

    pub fn estimate(&self) -> u64 {
        const M: f64 = 256.0;
        let alpha = 0.7213 / (1.0 + 1.079 / M);
        let mut sum = 0.0f64;
        let mut zeros = 0u32;
        for &r in self.regs.iter() {
            sum += 1.0 / ((1u64 << r.min(63)) as f64);
            if r == 0 {
                zeros += 1;
            }
        }
        let raw = alpha * M * M / sum;
        if raw <= 2.5 * M && zeros > 0 {
            // Linear counting is far more accurate in the small range, which is
            // exactly where the index/don't-index gate lives.
            (M * (M / zeros as f64).ln()).round() as u64
        } else {
            raw.round() as u64
        }
    }
}

/// A contiguous run of zones sharing one fingerprint width.
///
/// `f` grows as the table grows (see [`choose_f`]), and rewriting sealed zones
/// to widen them would defeat the point of an append-only structure. Grouping
/// zones into generations lets old zones keep their narrower `f` forever while
/// new zones are sized for the table as it is now. Offsets stay arithmetic
/// *within* a generation, so lookup is still O(1) after an O(#generations) scan.
///
/// A generation with `f == 0` is **plane-less**: its zones carry no
/// fingerprints at all, and a probe treats every slot in them as a candidate,
/// leaving the zone bounds to do the pruning. Two kinds of column get them:
///
/// * a **clustered** one — a serial id, an insert-time timestamp — whose zone
///   bounds do not overlap, so the bounds alone narrow an equality probe to the
///   one zone that can hold the value. Planes would cost `f` bits a row to
///   narrow 512 rows further, and a point probe would stream all of them
///   (20 MB per 10M rows at f = 16) where the bounds are 312 KB;
/// * a **gated-out** one, with too few distinct values to be worth probing,
///   whose planes nothing reads.
///
/// With no words to divide, its zone count is carried explicitly in `zones`.
#[derive(Clone)]
pub struct Generation {
    pub start_zone: u64,
    pub f: u8,
    pub words: Vec<u64>,
    /// Zone count of a plane-less (`f == 0`) generation; unused otherwise,
    /// where the count follows from `words`.
    pub zones: u64,
}

impl Generation {
    /// A generation with planes of width `f`, or a plane-less one for `f == 0`.
    pub fn new(start_zone: u64, f: u8, words: Vec<u64>, zones: u64) -> Self {
        Self {
            start_zone,
            f,
            words,
            zones,
        }
    }

    /// True when this generation carries no fingerprints.
    #[inline]
    pub fn is_planeless(&self) -> bool {
        self.f == 0
    }

    #[inline]
    pub fn zone_count(&self) -> u64 {
        if self.f == 0 {
            return self.zones;
        }
        (self.words.len() / (ZONE_WORDS * self.f as usize)) as u64
    }

    #[inline]
    pub fn contains(&self, zone: u64) -> bool {
        zone >= self.start_zone && zone < self.start_zone + self.zone_count()
    }

    #[inline]
    fn range(&self, zone: u64) -> std::ops::Range<usize> {
        let local = (zone - self.start_zone) as usize;
        let stride = ZONE_WORDS * self.f as usize;
        local * stride..(local + 1) * stride
    }

    #[inline]
    pub fn zone(&self, zone: u64) -> &[u64] {
        &self.words[self.range(zone)]
    }

    #[inline]
    fn zone_mut(&mut self, zone: u64) -> &mut [u64] {
        let r = self.range(zone);
        &mut self.words[r]
    }

    fn grow_to_include(&mut self, zone: u64) {
        if self.f == 0 {
            self.zones = self.zones.max(zone - self.start_zone + 1);
            return;
        }
        let needed = (zone - self.start_zone + 1) as usize * ZONE_WORDS * self.f as usize;
        if self.words.len() < needed {
            // Zeros read as CODE_EMPTY, i.e. "matches nothing" — a grown region
            // is safe before anything is written into it.
            self.words.resize(needed, 0);
        }
    }
}

/// Filter state for one column of one table.
pub struct ColumnFilter {
    pub write_order: u32,
    pub db_type: DbType,
    seed: HashSeed,
    cfg: FilterConfig,

    gens: Vec<Generation>,

    /// Slots below this have never been indexed (rows that existed before the
    /// filter did). They are always treated as candidates until a backfill
    /// lowers this to 0.
    unknown_below: u64,
    /// Contiguous frontier of indexed slots. Slots at or above it are always
    /// candidates — an insert may be in flight.
    valid_upto: u64,
    /// Slots written out of order, waiting for the frontier to reach them.
    pending: BTreeSet<u64>,

    hll: Hll,
    rows_seen: u64,
    next_f: u8,

    /// Lowest already-indexed slot modified since the last checkpoint.
    ///
    /// A checkpoint may only claim to cover slots below this: everything from
    /// here up could hold a fingerprint the on-disk copy does not know about,
    /// and trusting it would prune rows that now match.
    dirty_min: u64,

    /// Lowest slot written since the last checkpoint, by **any** path.
    ///
    /// Not the same question as `dirty_min`, which asks what *invalidates* a
    /// checkpoint and deliberately excludes a backfill: a backfill restores
    /// known-good state rather than diverging from it. This one asks which
    /// on-disk bytes have stopped being current, and a backfill rewrites bytes
    /// just as surely as an update does. It is what bounds an incremental
    /// checkpoint — everything from this slot's zone onward has to be re-sent.
    touched_min: u64,

    /// Set when the selectivity gate has judged this column not worth probing:
    /// its values are too few for an equality predicate to narrow anything.
    ///
    /// Probing stops; maintenance continues, so the decision is reversible when
    /// cardinality grows. Freeing the planes would save the storage too, but
    /// then re-enabling would need a backfill — not worth the complexity until
    /// something asks for it.
    gated_out: bool,

    /// Whether generations started from here on are plane-less: the column is
    /// gated out, or its zone bounds are clustered enough to stand in for
    /// fingerprints. Re-decided at every zone boundary; see [`Generation`].
    planeless: bool,

    /// Per-zone min/max, when the column's type supports ordered comparison.
    /// Covers `<`/`>`/`BETWEEN`, which fingerprints cannot express at all.
    zonemap: Option<ZoneMap>,

    /// Optional per-superzone span summary — the L0/L1 tier of §3.
    ///
    /// Off by default: it reduces how many plane *bytes* a probe streams and
    /// does nothing at all for how many rows it falsely fetches, so it is worth
    /// its 0.25 bits/row only for a column that takes heavy point-lookup traffic
    /// and holds few distinct values per 16 384-row span. See [`super::bloom`]
    /// for the arithmetic and `SET FILTER bloom = on FOR COLUMN t.c` to enable
    /// it.
    bloom: Option<SuperBloom>,

    /// Whether this column carries per-row fingerprint planes at all.
    ///
    /// False for `BOOL`, which is indexed for its zone bounds alone — see
    /// [`is_fingerprintable`]. A bounds-only column allocates no planes, hashes
    /// nothing on insert and never runs the adaptive-width machinery, so its
    /// whole cost is the two bytes per zone. Everything else about it — the
    /// watermark, the dirty frontier, backfill — behaves identically, because
    /// zone bounds go stale in exactly the same ways fingerprints do.
    fingerprints: bool,
}

impl ColumnFilter {
    pub fn new(write_order: u32, db_type: DbType, seed: HashSeed, cfg: FilterConfig) -> Self {
        Self::with_bounds(write_order, db_type, seed, cfg, true)
    }

    /// `zone_bounds = false` keeps the fingerprints and drops range pruning.
    ///
    /// Worth doing for a column whose values have no locality — random UUIDs,
    /// hashes — where every zone's bounds span the space and prune nothing, so
    /// the two values per zone are pure overhead.
    pub fn with_bounds(
        write_order: u32,
        db_type: DbType,
        seed: HashSeed,
        cfg: FilterConfig,
        zone_bounds: bool,
    ) -> Self {
        Self::with_options(write_order, db_type, seed, cfg, zone_bounds, false)
    }

    /// As [`Self::with_bounds`], additionally choosing whether to keep the
    /// per-superzone span summary.
    pub fn with_options(
        write_order: u32,
        db_type: DbType,
        seed: HashSeed,
        cfg: FilterConfig,
        zone_bounds: bool,
        span_summary: bool,
    ) -> Self {
        let db_type_for_zonemap = if zone_bounds {
            Some(db_type.clone())
        } else {
            None
        };
        let fingerprints = is_fingerprintable(&db_type);
        Self {
            write_order,
            db_type,
            seed,
            cfg,
            gens: Vec::new(),
            unknown_below: 0,
            valid_upto: 0,
            pending: BTreeSet::new(),
            hll: Hll::default(),
            rows_seen: 0,
            next_f: cfg.default_f,
            dirty_min: u64::MAX,
            touched_min: u64::MAX,
            gated_out: false,
            planeless: false,
            zonemap: db_type_for_zonemap.and_then(ZoneMap::new),
            // A summary over rows with no fingerprints would summarise nothing:
            // a bounds-only column never hashes, so there is no key to record.
            bloom: (span_summary && fingerprints).then(SuperBloom::new),
            fingerprints,
        }
    }

    /// Rebuild from a persisted checkpoint.
    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        write_order: u32,
        db_type: DbType,
        seed: HashSeed,
        cfg: FilterConfig,
        gens: Vec<Generation>,
        valid_upto: u64,
        unknown_below: u64,
        rows_seen: u64,
        next_f: u8,
        hll: Hll,
        zonemap: Option<ZoneMap>,
        bloom: Option<SuperBloom>,
    ) -> Self {
        let fingerprints = is_fingerprintable(&db_type);
        Self {
            write_order,
            db_type,
            seed,
            cfg,
            gens,
            // The checkpoint records its own unindexed prefix rather than being
            // assumed to have none. Assuming none was a false negative waiting
            // to happen: a table opened with `backfill_on_open = off` keeps its
            // prefix un-indexed forever, and a checkpoint taken after that
            // claimed a frontier covering rows nothing had ever looked at.
            unknown_below,
            valid_upto,
            pending: BTreeSet::new(),
            hll,
            rows_seen,
            next_f: next_f.clamp(cfg.min_f, cfg.max_f),
            dirty_min: u64::MAX,
            touched_min: u64::MAX,
            gated_out: false,
            planeless: false,
            zonemap,
            bloom: bloom.filter(|_| fingerprints),
            fingerprints,
        }
    }

    #[inline]
    pub fn zonemap(&self) -> Option<&ZoneMap> {
        self.zonemap.as_ref()
    }

    /// True when this column can prune range predicates.
    #[inline]
    pub fn has_zonemap(&self) -> bool {
        self.zonemap.is_some()
    }

    /// True when this column carries per-row fingerprints. False for a
    /// bounds-only column, whose effective fingerprint width is 0.
    #[inline]
    pub fn has_fingerprints(&self) -> bool {
        self.fingerprints
    }

    /// The per-superzone span summary, when this column keeps one.
    #[inline]
    pub fn span_summary(&self) -> Option<&SuperBloom> {
        self.bloom.as_ref()
    }

    /// True when this column keeps a span summary.
    #[inline]
    pub fn has_span_summary(&self) -> bool {
        self.bloom.is_some()
    }

    #[inline]
    pub fn rows_seen(&self) -> u64 {
        self.rows_seen
    }

    #[inline]
    pub fn hll_regs(&self) -> &[u8; 256] {
        self.hll.regs()
    }

    #[inline]
    pub fn generations(&self) -> &[Generation] {
        &self.gens
    }

    /// The frontier a checkpoint may safely record: never past a slot that has
    /// been modified since the last one.
    #[inline]
    pub fn persistable_valid_upto(&self) -> u64 {
        self.valid_upto.min(self.dirty_min)
    }

    /// The width bounds this column was built with (`SET FILTER … FOR
    /// COLUMN` included): part of a snapshot's filter configuration hash.
    #[inline]
    pub fn config(&self) -> &FilterConfig {
        &self.cfg
    }

    /// Called after a successful checkpoint: the on-disk copy now includes
    /// every modification made so far.
    ///
    /// Callers must still be holding the same write guard they serialised
    /// under. Releasing it in between would let a write land in the gap and be
    /// forgotten by both the checkpoint that did not see it and the reset that
    /// erased its trace.
    #[inline]
    pub fn mark_checkpointed(&mut self) {
        self.dirty_min = u64::MAX;
        self.touched_min = u64::MAX;
    }

    /// Lowest slot written since the last checkpoint, or `u64::MAX` if none.
    #[inline]
    pub fn touched_min(&self) -> u64 {
        self.touched_min
    }

    /// First zone an incremental checkpoint has to re-send, given what is
    /// already on disk. Clamped to the zone count, so an untouched column sends
    /// nothing at all.
    #[inline]
    pub fn delta_from_zone(&self) -> u64 {
        let zones = self.zone_count();
        if self.touched_min == u64::MAX {
            return zones;
        }
        (self.touched_min / ZONE_ROWS as u64).min(zones)
    }

    /// Zones this column's planes and bounds currently span.
    pub fn zone_count(&self) -> u64 {
        let planes = self
            .gens
            .last()
            .map_or(0, |g| g.start_zone + g.zone_count());
        let bounds = self.zonemap.as_ref().map_or(0, |z| z.zone_count());
        planes.max(bounds)
    }

    /// Declare that slots `[0, slots)` predate the filter and must be treated as
    /// candidates until backfilled.
    pub fn set_unindexed_prefix(&mut self, slots: u64) {
        self.unknown_below = slots;
        self.valid_upto = slots;
        // The prefix is about to be rewritten from storage, so nothing on disk
        // below it can be assumed current.
        if slots > 0 {
            self.touched_min = 0;
        }
    }

    #[inline]
    pub fn valid_upto(&self) -> u64 {
        self.valid_upto
    }

    #[inline]
    pub fn unknown_below(&self) -> u64 {
        self.unknown_below
    }

    /// True once every slot below the frontier is indexed.
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.unknown_below == 0
    }

    pub fn clear_unindexed_prefix(&mut self) {
        self.unknown_below = 0;
    }

    /// Stops vouching for slots `[0, slots)` on a column that was indexing
    /// them: they are about to be renumbered. Unlike
    /// [`Self::set_unindexed_prefix`] the frontier is left alone, since
    /// everything at or past it is a candidate already, and the prefix never
    /// shrinks. The next checkpoint rewrites from zone 0, so the file stops
    /// vouching for those slots too.
    pub fn demote_below(&mut self, slots: u64) {
        self.unknown_below = self.unknown_below.max(slots);
        if slots > 0 {
            self.touched_min = 0;
        }
    }

    #[inline]
    pub fn current_f(&self) -> u8 {
        self.next_f
    }

    pub fn estimated_distinct(&self) -> u64 {
        self.hll.estimate()
    }

    /// Total heap bytes held by the plane store.
    pub fn bytes(&self) -> usize {
        self.gens.iter().map(|g| g.words.len() * 8).sum::<usize>()
            + self.zonemap.as_ref().map_or(0, |z| z.bytes())
            + self.bloom.as_ref().map_or(0, |b| b.bytes())
    }

    fn gen_index_for(&mut self, zone: u64) -> usize {
        if let Some(i) = self.gens.iter().position(|g| g.contains(zone)) {
            return i;
        }

        // Beyond the last generation: extend it, or start a new one if the
        // adaptive width has moved on — or the column has moved between having
        // planes and not.
        let want_f = if self.planeless { 0 } else { self.next_f };
        match self.gens.last_mut() {
            Some(last) if last.f == want_f => {
                last.grow_to_include(zone);
                self.gens.len() - 1
            }
            Some(last) => {
                let next_start = last.start_zone + last.zone_count();
                if zone < next_start {
                    // Falls inside the last generation's span after all.
                    last.grow_to_include(zone);
                    return self.gens.len() - 1;
                }
                let mut g = Generation::new(next_start, want_f, Vec::new(), 0);
                g.grow_to_include(zone);
                self.gens.push(g);
                self.gens.len() - 1
            }
            None => {
                let mut g = Generation::new(0, want_f, Vec::new(), 0);
                g.grow_to_include(zone);
                self.gens.push(g);
                0
            }
        }
    }

    /// Whether `slot` holds a fingerprint other than `CODE_EMPTY`: whether
    /// clearing it would change a plane. Read-only; vacuum asks before it
    /// takes a write ticket for a clear (docs/persistent-indexes.md §2, RV2).
    /// A bounds-only column, a plane-less zone and a zone never written have
    /// nothing to clear. A wrong `false` would cost selectivity only: a
    /// fingerprint left on a dead slot keeps it a candidate.
    pub fn holds_code(&self, slot: u64) -> bool {
        if !self.fingerprints {
            return false;
        }
        let zone = slot / ZONE_ROWS as u64;
        let local = (slot % ZONE_ROWS as u64) as usize;
        match self.gens.iter().find(|g| g.contains(zone)) {
            Some(g) if g.f > 0 => read_slot(g.zone(zone), g.f, local) != CODE_EMPTY,
            _ => false,
        }
    }

    /// Record a value (or `None` to clear the slot, which is how deletes and
    /// the tombstone half of a relocating update are handled).
    pub fn set(&mut self, slot: u64, value: Option<&[u8]>) {
        self.set_value(slot, value, true)
    }

    fn set_value(&mut self, slot: u64, value: Option<&[u8]>, track_dirty: bool) {
        let hash = match (self.fingerprints, value) {
            (true, Some(b)) => canonical_hash(&self.db_type, b, self.seed),
            _ => None,
        };
        self.set_prepared_inner(slot, value, hash, track_dirty);
    }

    /// Record a value whose hash the caller has already computed.
    ///
    /// [`Self::hash_of`] and this pair up so that hashing — the dominant cost of
    /// insert maintenance, per §11.1 of the design doc — happens *outside* the
    /// per-column lock. Under one writer that changes nothing; under eight it
    /// is most of the critical section, and the critical section is what bounds
    /// concurrent throughput.
    ///
    /// `hash` must be what `hash_of` would return for `value`. Passing a hash
    /// from a different value would fingerprint a row as something it is not,
    /// which is a false negative — so this is deliberately not public.
    pub(super) fn set_prepared(&mut self, slot: u64, value: Option<&[u8]>, hash: Option<u64>) {
        self.set_prepared_inner(slot, value, hash, true)
    }

    /// The hash this column would store for `value`, computable without the
    /// lock: it depends only on the column's type and the table's seed, both
    /// fixed for the column's lifetime.
    #[inline]
    pub fn hash_of(db_type: &DbType, seed: HashSeed, value: &[u8]) -> Option<u64> {
        canonical_hash(db_type, value, seed)
    }

    fn set_prepared_inner(
        &mut self,
        slot: u64,
        value: Option<&[u8]>,
        hash: Option<u64>,
        track_dirty: bool,
    ) {
        // Bounds only ever widen. A cleared slot leaves the zone's range
        // conservatively wide rather than shrinking it, because we cannot know
        // whether the removed value was the sole occupant of a bound.
        if let (Some(bytes), Some(zm)) = (value, self.zonemap.as_mut()) {
            zm.observe(slot / ZONE_ROWS as u64, bytes);
        }
        if !self.fingerprints {
            // Bounds-only: there are no planes to write. The frontier
            // bookkeeping still runs — a zone's bounds are trusted only below
            // the watermark, exactly as a fingerprint is.
            if value.is_some() {
                self.rows_seen = self.rows_seen.max(slot + 1);
            }
            self.note_write(slot, track_dirty);
            return;
        }
        self.set_hash_inner(slot, hash, track_dirty);
    }

    /// Re-index a slot from authoritative storage during a backfill.
    ///
    /// Unlike [`Self::set`], this does **not** drag the checkpoint frontier
    /// back: a backfill is restoring known-good state, not diverging from it.
    /// Treating it as a modification would poison `dirty_min` down to slot 0 and
    /// make every subsequent checkpoint claim to cover nothing.
    pub fn set_reindex(&mut self, slot: u64, value: Option<&[u8]>) {
        self.set_value(slot, value, false)
    }

    pub fn set_hash(&mut self, slot: u64, hash: Option<u64>) {
        self.set_hash_inner(slot, hash, true)
    }

    fn set_hash_inner(&mut self, slot: u64, hash: Option<u64>, track_dirty: bool) {
        let zone = slot / ZONE_ROWS as u64;
        let local = (slot % ZONE_ROWS as u64) as usize;

        if let Some(h) = hash {
            self.hll.add(h);
            self.rows_seen = self.rows_seen.max(slot + 1);
            // A Bloom cannot forget, so a cleared slot leaves its key behind —
            // which makes the summary more conservative, never less. Same
            // asymmetry the zone bounds rely on.
            if let Some(b) = self.bloom.as_mut() {
                b.add(bloom::superzone_of(slot), h);
            }
        }

        if !self.fingerprints {
            self.note_write(slot, track_dirty);
            return;
        }

        let gi = self.gen_index_for(zone);
        let f = self.gens[gi].f;
        // A plane-less zone has nothing to write: every slot in it is a
        // candidate whatever it holds.
        if f > 0 {
            let g = match hash {
                Some(h) => fingerprint(h, f),
                None => CODE_EMPTY,
            };
            write_slot(self.gens[gi].zone_mut(zone), f, local, g);
        }

        self.note_write(slot, track_dirty);

        // Re-evaluate the adaptive width at zone boundaries.
        if local == ZONE_ROWS - 1 {
            let est = self.hll.estimate();
            let chosen = choose_f(est, self.rows_seen, &self.cfg);
            // `choose_f` returns 0 for a column whose predicates would match too
            // much of the table to be worth narrowing. Honour that by stopping
            // probing rather than silently keeping the previous width, which
            // made `min_selectivity` a setting with no effect.
            if chosen == 0 {
                self.gated_out = true;
            } else {
                self.gated_out = false;
                self.next_f = chosen.max(self.cfg.min_f);
            }
            // Planes are worth writing only while something will read them and
            // the bounds cannot do their job alone.
            self.planeless = self.gated_out || self.bounds_are_clustered(zone);
        }
    }

    /// Zones a clustering decision looks back over.
    const CLUSTER_WINDOW: u64 = 8;

    /// Whether the zone bounds up to `zone` are clustered: the last
    /// [`Self::CLUSTER_WINDOW`] zones ascend without overlapping, as an id
    /// sequence or an insert-time timestamp does, so an equality probe's
    /// bounds narrow it to one zone. Only fixed-width bounds qualify (see
    /// `ZoneMap::is_ascending_between`). A wrong answer costs selectivity,
    /// never a row: a plane-less zone is always a candidate.
    fn bounds_are_clustered(&self, zone: u64) -> bool {
        if zone + 1 < Self::CLUSTER_WINDOW {
            return false;
        }
        self.zonemap
            .as_ref()
            .is_some_and(|zm| zm.is_ascending_between(zone + 1 - Self::CLUSTER_WINDOW, zone + 1))
    }

    /// The bookkeeping every slot write owes regardless of what it stored.
    ///
    /// Order matters: the dirty test reads `valid_upto` as it was *before* this
    /// write, so it has to run ahead of [`Self::advance_frontier`]. Overwriting a
    /// slot the frontier already covers (an update or a delete) invalidates any
    /// checkpoint taken above it.
    #[inline]
    fn note_write(&mut self, slot: u64, track_dirty: bool) {
        if track_dirty && slot < self.valid_upto && slot < self.dirty_min {
            self.dirty_min = slot;
        }
        if slot < self.touched_min {
            self.touched_min = slot;
        }
        self.advance_frontier(slot);
    }

    /// Advance the contiguous frontier. Out-of-order completions park in
    /// `pending`; a gap only costs selectivity (those slots stay candidates),
    /// never correctness.
    fn advance_frontier(&mut self, slot: u64) {
        if slot < self.valid_upto {
            return;
        }
        if slot == self.valid_upto {
            self.valid_upto += 1;
            while self.pending.remove(&self.valid_upto) {
                self.valid_upto += 1;
            }
        } else {
            self.pending.insert(slot);
        }
    }

    /// Write a candidate bitmap for `value` into `out`.
    ///
    /// `out` must be `ceil(n_slots / 64)` words. A set bit means "this slot may
    /// match" — the mask is always a **superset** of true matches.
    pub fn probe_into(&self, value: &[u8], n_slots: u64, out: &mut [u64]) -> bool {
        match canonical_hash(&self.db_type, value, self.seed) {
            Some(h) => {
                self.probe_hash_into(h, n_slots, out);
                true
            }
            // Un-hashable value: everything stays a candidate.
            None => {
                out.fill(u64::MAX);
                mask_tail(out, n_slots);
                false
            }
        }
    }

    /// Fill `out` with the candidate mask for `col <op> value`.
    ///
    /// Equality consults the fingerprints *and* the zone bounds — the two prune
    /// independently, so intersecting them is strictly better than either.
    /// Range predicates are zone bounds only.
    /// True when the selectivity gate has taken this column out of service.
    #[inline]
    pub fn is_gated_out(&self) -> bool {
        self.gated_out
    }

    pub fn probe_pred_into(&self, op: SargOp, value: &[u8], n_slots: u64, out: &mut [u64]) -> bool {
        // The selectivity gate takes the *fingerprints* out of service: with
        // so few distinct values an equality probe would match too much to be
        // worth streaming the planes. It says nothing about the zone bounds,
        // which are maintained regardless and stay exact. Returning early here
        // used to drop range pruning too, so `created_day > '…'` on a clustered
        // low-cardinality column scanned the whole table.
        //
        // Equality on a column that carries fingerprints consults both
        // structures, since they prune independently.
        if op == SargOp::Eq && self.fingerprints && !self.gated_out {
            let Some(h) = canonical_hash(&self.db_type, value, self.seed) else {
                out.fill(u64::MAX);
                mask_tail(out, n_slots);
                return false;
            };
            self.probe_hash_into(h, n_slots, out);
            self.intersect_zonemap(op, value, n_slots, out);
            mask_tail(out, n_slots);
            return true;
        }

        // Bounds alone from here: a range predicate, which fingerprints cannot
        // express; any predicate on a gated-out column, whose bounds are still
        // good; or *any* predicate on a bounds-only column, where the bounds
        // are the entire index. `BOOL` is the second case — `__null_x = 1` gets
        // its pruning from zones that hold no `1` at all.
        let usable = self
            .zonemap
            .as_ref()
            .is_some_and(|zm| zm.probes_with(value));
        if !usable {
            out.fill(u64::MAX);
            mask_tail(out, n_slots);
            return false;
        }

        out.fill(u64::MAX);
        self.intersect_zonemap(op, value, n_slots, out);
        mask_tail(out, n_slots);
        true
    }

    /// Clear every zone whose bounds rule the predicate out.
    fn intersect_zonemap(&self, op: SargOp, value: &[u8], n_slots: u64, out: &mut [u64]) {
        let Some(zm) = self.zonemap.as_ref() else {
            return;
        };
        let limit = zones_for(self.valid_upto.min(n_slots));
        for zone in 0..limit {
            let first = zone * ZONE_ROWS as u64;
            let last = first + ZONE_ROWS as u64 - 1;
            if last < self.unknown_below {
                continue;
            }
            if zm.zone_may_match(zone, op, value) {
                continue;
            }
            self.reject_zone(out, zone, first, last);
        }
    }

    /// Zero a zone's slots, then hand back the ones we cannot vouch for.
    fn reject_zone(&self, out: &mut [u64], zone: u64, first: u64, last: u64) {
        let base = (zone as usize) * ZONE_WORDS;
        if base >= out.len() {
            return;
        }
        let end = (base + ZONE_WORDS).min(out.len());
        for w in out[base..end].iter_mut() {
            *w = 0;
        }
        if self.unknown_below > first {
            force_range(out, first, self.unknown_below.min(last + 1));
        }
        if self.valid_upto <= last {
            force_range(out, self.valid_upto.max(first), last + 1);
        }
    }

    pub fn probe_hash_into(&self, hash: u64, n_slots: u64, out: &mut [u64]) {
        let probe = probe_fn();

        // Default: candidate. Anything we cannot speak for stays set.
        out.fill(u64::MAX);

        let mut scratch = [0u64; ZONE_WORDS];

        for gn in self.gens.iter() {
            // No fingerprint exists at width 0; a plane-less generation never
            // reaches the plane probe below.
            let g = if gn.is_planeless() { 0 } else { fingerprint(hash, gn.f) };
            let count = gn.zone_count();
            for lz in 0..count {
                let zone = gn.start_zone + lz;
                let first = zone * ZONE_ROWS as u64;
                if first >= self.valid_upto || first >= n_slots {
                    break;
                }
                let last = first + ZONE_ROWS as u64 - 1;
                if last < self.unknown_below {
                    continue;
                }

                // A plane-less zone cannot rule anything out: every slot stays
                // a candidate, for the zone bounds to narrow.
                if gn.is_planeless() {
                    if self
                        .bloom
                        .as_ref()
                        .is_some_and(|b| !b.may_contain(bloom::superzone_of(first), hash))
                    {
                        self.reject_zone(out, zone, first, last);
                    }
                    continue;
                }

                // The span summary, when this column keeps one. A superzone it
                // rules out needs no plane read at all, which is the whole point
                // of the tier: eight word tests in place of `f` cache lines.
                if self
                    .bloom
                    .as_ref()
                    .is_some_and(|b| !b.may_contain(bloom::superzone_of(first), hash))
                {
                    self.reject_zone(out, zone, first, last);
                    continue;
                }

                probe(gn.zone(zone), gn.f, g, &mut scratch);

                let base = (zone as usize) * ZONE_WORDS;
                let end = (base + ZONE_WORDS).min(out.len());
                out[base..end].copy_from_slice(&scratch[..end - base]);

                // Restore "candidate" for untrusted slots that share this zone.
                if self.unknown_below > first {
                    force_range(out, first, self.unknown_below.min(last + 1));
                }
                if self.valid_upto <= last {
                    force_range(out, self.valid_upto.max(first), last + 1);
                }
            }
        }

        mask_tail(out, n_slots);
    }
}

/// The part of one column an incremental checkpoint carries.
///
/// Everything cheap — the frontier, the cardinality sketch, the chosen width —
/// is re-sent whole; only the plane words and zone bounds are sent as a tail,
/// because only those are large enough for the distinction to matter.
pub struct ColumnDelta {
    pub valid_upto: u64,
    pub unknown_below: u64,
    pub rows_seen: u64,
    pub next_f: u8,
    pub hll: Hll,
    /// `(start_zone, f, first_zone_carried, zones, words from that zone on)`.
    /// `zones` is a plane-less generation's whole zone count; for one with
    /// planes it is 0 and the words say it.
    pub gens: Vec<(u64, u8, u64, u64, Vec<u64>)>,
    /// `(first_zone_carried, bound bytes from that zone on, validity bits)`.
    pub zonemap: Option<(u64, Vec<u8>, Vec<u64>)>,
    /// `(first_superzone_carried, summary words from that superzone on)`.
    pub bloom: Option<(u64, Vec<u32>)>,
}

impl ColumnFilter {
    /// Build the delta covering everything written since the last checkpoint.
    pub fn delta(&self, from_zone: u64) -> ColumnDelta {
        let gens = self
            .gens
            .iter()
            .filter(|g| g.start_zone + g.zone_count() > from_zone)
            .map(|g| {
                let first = g.start_zone.max(from_zone);
                let skip = (first - g.start_zone) as usize * ZONE_WORDS * g.f as usize;
                (
                    g.start_zone,
                    g.f,
                    first,
                    if g.f == 0 { g.zones } else { 0 },
                    g.words[skip.min(g.words.len())..].to_vec(),
                )
            })
            .collect();

        ColumnDelta {
            valid_upto: self.persistable_valid_upto(),
            unknown_below: self.unknown_below,
            rows_seen: self.rows_seen,
            next_f: self.next_f,
            hll: self.hll.clone(),
            gens,
            zonemap: self.zonemap.as_ref().map(|z| {
                (
                    from_zone,
                    z.tail_from(from_zone).to_vec(),
                    z.valid_bits().to_vec(),
                )
            }),
            // A superzone spans 32 zones, so the re-send point rounds *down* to
            // the superzone containing `from_zone`: sending half a block would
            // drop the keys recorded in the half left behind.
            bloom: self.bloom.as_ref().map(|b| {
                let from_sz = from_zone / bloom::SUPERZONE_ZONES;
                (from_sz, b.tail_from(from_sz).to_vec())
            }),
        }
    }

    /// Replay a delta onto this column.
    ///
    /// Returns `false` when the delta cannot describe this column — a
    /// generation whose width disagrees, a word count that does not divide, a
    /// tail that starts inside a generation the base never had. The caller stops
    /// replaying there and keeps the state it had, which is a checkpoint of an
    /// earlier moment: sound, just less complete. That is the same position a
    /// torn final append leaves us in, so there is one recovery path rather than
    /// two.
    pub fn apply_delta(&mut self, d: ColumnDelta) -> bool {
        for (start_zone, f, first_zone, zones, words) in d.gens {
            if first_zone < start_zone {
                return false;
            }
            if f == 0 {
                // Plane-less: only its extent to carry.
                if !words.is_empty() {
                    return false;
                }
                match self.gens.iter_mut().find(|g| g.start_zone == start_zone) {
                    Some(g) if g.f == 0 => g.zones = g.zones.max(zones),
                    Some(_) => return false,
                    None => self.gens.push(Generation::new(start_zone, 0, Vec::new(), zones)),
                }
                continue;
            }
            let stride = ZONE_WORDS * f as usize;
            if words.len() % stride != 0 {
                return false;
            }
            let Some(keep) = ((first_zone - start_zone) as usize).checked_mul(stride) else {
                return false;
            };
            match self.gens.iter_mut().find(|g| g.start_zone == start_zone) {
                Some(g) => {
                    if g.f != f {
                        return false;
                    }
                    if g.words.len() < keep {
                        g.words.resize(keep, 0);
                    }
                    g.words.truncate(keep);
                    g.words.extend_from_slice(&words);
                }
                // A generation the base never saw must be carried whole: it was
                // created after the checkpoint, so it starts at or above the
                // first zone this delta covers.
                None if keep == 0 => {
                    self.gens.push(Generation::new(start_zone, f, words, 0));
                }
                None => return false,
            }
        }
        self.gens.sort_by_key(|g| g.start_zone);

        match (d.zonemap, self.zonemap.as_mut()) {
            (Some((from, data, valid)), Some(zm)) => {
                if !zm.apply_tail(from, &data, valid) {
                    return false;
                }
            }
            (None, None) => {}
            // The base and the delta disagree about whether this column has
            // bounds at all, so one of them is describing a different column.
            _ => return false,
        }

        match (d.bloom, self.bloom.as_mut()) {
            (Some((from, words)), Some(b)) => {
                if !b.apply_tail(from, &words) {
                    return false;
                }
            }
            (None, None) => {}
            _ => return false,
        }

        self.valid_upto = d.valid_upto;
        self.unknown_below = d.unknown_below;
        self.rows_seen = d.rows_seen;
        self.next_f = d.next_f.clamp(self.cfg.min_f, self.cfg.max_f);
        self.hll = d.hll;
        true
    }
}

/// Set bits for slots `[from, to)`.
#[inline]
fn force_range(out: &mut [u64], from: u64, to: u64) {
    let mut s = from;
    while s < to {
        let w = (s / 64) as usize;
        if w >= out.len() {
            return;
        }
        let bit_start = (s % 64) as u32;
        let bits_here = (64 - bit_start as u64).min(to - s);
        let m = if bits_here == 64 {
            u64::MAX
        } else {
            (((1u64 << bits_here) - 1) << bit_start) as u64
        };
        out[w] |= m;
        s += bits_here;
    }
}

/// Clear bits past `n_slots` in the final word so popcounts are meaningful.
#[inline]
fn mask_tail(out: &mut [u64], n_slots: u64) {
    let rem = (n_slots % 64) as u32;
    let last = (n_slots / 64) as usize;
    if rem != 0 && last < out.len() {
        out[last] &= (1u64 << rem) - 1;
        for w in out.iter_mut().skip(last + 1) {
            *w = 0;
        }
    } else {
        for w in out.iter_mut().skip(last) {
            *w = 0;
        }
    }
}

/// Words needed to cover `n_slots`.
#[inline]
pub fn bitmap_words(n_slots: u64) -> usize {
    ((n_slots + 63) / 64) as usize
}

/// Public wrapper over [`mask_tail`] for callers building bitmaps directly.
#[inline]
pub fn mask_tail_pub(out: &mut [u64], n_slots: u64) {
    mask_tail(out, n_slots)
}

#[cfg(test)]
mod gated_bounds_tests {
    use super::*;
    use crate::core::filter::hash::HashSeed;

    /// A day-number-like column: few distinct values, inserted in order, so
    /// each 512-row zone spans one or two of them.
    fn clustered(n: u64, rows_per_value: u64) -> ColumnFilter {
        let mut c = ColumnFilter::new(0, DbType::I64, HashSeed::default(), FilterConfig::default());
        for slot in 0..n {
            c.set(slot, Some(&((slot / rows_per_value) as i64).to_le_bytes()));
        }
        c
    }

    fn survivors(bm: &[u64]) -> u64 {
        bm.iter().map(|w| w.count_ones() as u64).sum()
    }

    /// The selectivity gate takes the fingerprints out of service, not the zone
    /// bounds: a range predicate on a gated column still prunes, and never a
    /// matching slot.
    #[test]
    fn a_gated_column_still_prunes_ranges_by_its_bounds() {
        let n = 20_000u64;
        let per = 1_000u64; // 20 distinct values, below the gate's 50
        let c = clustered(n, per);
        assert!(c.is_gated_out(), "the test needs the gate closed");
        let ops = [SargOp::Gt, SargOp::Ge, SargOp::Lt, SargOp::Le, SargOp::Eq];
        for op in ops {
            for pivot in [-1i64, 0, 3, 10, 17, 19, 20] {
                let mut bm = vec![0u64; bitmap_words(n)];
                let usable = c.probe_pred_into(op, &pivot.to_le_bytes(), n, &mut bm);
                let mut matching = 0u64;
                for slot in 0..n {
                    let v = (slot / per) as i64;
                    let hit = match op {
                        SargOp::Gt => v > pivot,
                        SargOp::Ge => v >= pivot,
                        SargOp::Lt => v < pivot,
                        SargOp::Le => v <= pivot,
                        _ => v == pivot,
                    };
                    if hit {
                        matching += 1;
                        assert!(
                            (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1,
                            "slot {} (v={}) pruned for {:?} {}",
                            slot,
                            v,
                            op,
                            pivot
                        );
                    }
                }
                assert!(usable, "{:?} {}: bounds are usable on a gated column", op, pivot);
                // Bounds work at zone granularity: at most the matching rows
                // plus the two partial zones at the edges.
                assert!(
                    survivors(&bm) <= matching + 2 * ZONE_ROWS as u64,
                    "{:?} {}: {} survivors for {} matches",
                    op,
                    pivot,
                    survivors(&bm),
                    matching
                );
            }
        }
    }

    /// The same gated column with no locality: every zone spans every value,
    /// so nothing can be pruned — and nothing is.
    #[test]
    fn an_unclustered_gated_column_prunes_nothing_wrongly() {
        let n = 10_000u64;
        let mut c = ColumnFilter::new(0, DbType::I64, HashSeed::default(), FilterConfig::default());
        for slot in 0..n {
            c.set(slot, Some(&((slot % 7) as i64).to_le_bytes()));
        }
        assert!(c.is_gated_out());
        let mut bm = vec![0u64; bitmap_words(n)];
        c.probe_pred_into(SargOp::Gt, &3i64.to_le_bytes(), n, &mut bm);
        assert_eq!(survivors(&bm), n);
    }

    /// A range probe on a clustered low-cardinality column, 10M rows over 100
    /// days... capped at 49 distinct so the gate closes: rows surviving the
    /// mask, and probe time. `cargo test --release -p rasterizeddb_core --lib
    /// -- --ignored gated_range_probe_bench --nocapture`.
    #[test]
    #[ignore]
    fn gated_range_probe_bench() {
        let n = 10_000_000u64;
        let c = clustered(n, n / 49 + 1);
        assert!(c.is_gated_out());
        let mut bm = vec![0u64; bitmap_words(n)];
        for pivot in [45i64, 40, 25] {
            let t = std::time::Instant::now();
            c.probe_pred_into(SargOp::Gt, &pivot.to_le_bytes(), n, &mut bm);
            let took = t.elapsed();
            println!(
                "day > {pivot}: {} of {n} slots survive ({:.1}%), probe {:?}",
                survivors(&bm),
                survivors(&bm) as f64 * 100.0 / n as f64,
                took
            );
        }
    }
}

#[cfg(test)]
mod planeless_tests {
    use super::*;
    use crate::core::filter::hash::HashSeed;
    use crate::core::filter::persist;

    fn col() -> ColumnFilter {
        ColumnFilter::new(0, DbType::I64, HashSeed::default(), FilterConfig::default())
    }

    fn fill(c: &mut ColumnFilter, slots: std::ops::Range<u64>, value: impl Fn(u64) -> i64) {
        for slot in slots {
            c.set(slot, Some(&value(slot).to_le_bytes()));
        }
    }

    fn bit(bm: &[u64], slot: u64) -> bool {
        (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1
    }

    fn count(bm: &[u64]) -> u64 {
        bm.iter().map(|w| w.count_ones() as u64).sum()
    }

    fn planeless_zones(c: &ColumnFilter) -> u64 {
        c.generations().iter().filter(|g| g.is_planeless()).map(|g| g.zone_count()).sum()
    }

    /// Every slot whose value is `v` must survive an equality probe for `v`.
    fn assert_no_false_negatives(c: &ColumnFilter, n: u64, value: &dyn Fn(u64) -> i64, probes: &[u64]) {
        let mut bm = vec![0u64; bitmap_words(n)];
        for &slot in probes {
            let v = value(slot);
            c.probe_pred_into(SargOp::Eq, &v.to_le_bytes(), n, &mut bm);
            for s in 0..n {
                if value(s) == v {
                    assert!(bit(&bm, s), "slot {} (v={}) pruned", s, v);
                }
            }
        }
    }

    /// A serial id: distinct per row and ascending, so the bounds narrow an
    /// equality probe to one zone and the planes are dropped after the window.
    #[test]
    fn a_clustered_column_stops_writing_planes() {
        let n = 40_000u64;
        let mut clustered = col();
        fill(&mut clustered, 0..n, |s| s as i64 * 3);
        assert!(!clustered.is_gated_out(), "distinct per row: the gate stays open");
        assert!(planeless_zones(&clustered) > 0, "a clustered column went plane-less");

        let mut random = col();
        let scrambled = |s: u64| (s.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 20) as i64;
        fill(&mut random, 0..n, scrambled);
        assert_eq!(planeless_zones(&random), 0, "an unclustered column keeps its planes");
        assert!(
            clustered.bytes() * 4 < random.bytes(),
            "clustered {} B vs random {} B",
            clustered.bytes(),
            random.bytes()
        );

        // Point probes still land on one zone, and never miss.
        let mut bm = vec![0u64; bitmap_words(n)];
        for slot in [0u64, 511, 512, 4_097, 20_000, 39_999] {
            clustered.probe_pred_into(SargOp::Eq, &(slot as i64 * 3).to_le_bytes(), n, &mut bm);
            assert!(bit(&bm, slot), "slot {} pruned", slot);
            assert!(count(&bm) <= ZONE_ROWS as u64, "{} survivors for a point probe", count(&bm));
        }
        assert_no_false_negatives(&clustered, n, &|s| s as i64 * 3, &[7, 30_001]);
        // A value no row holds.
        clustered.probe_pred_into(SargOp::Eq, &(-5i64).to_le_bytes(), n, &mut bm);
        assert_eq!(count(&bm), 0);
    }

    /// Bounds that stop being clustered bring planes back for the zones after,
    /// and every zone, plane-less or not, still answers correctly.
    #[test]
    fn planes_return_when_clustering_ends() {
        let n1 = 20_000u64;
        let n = 40_000u64;
        let value = |s: u64| {
            if s < n1 { s as i64 } else { (s.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 24) as i64 }
        };
        let mut c = col();
        fill(&mut c, 0..n, value);
        let gens = c.generations();
        assert!(gens.iter().any(|g| g.is_planeless()));
        assert!(gens.last().is_some_and(|g| !g.is_planeless()), "planes resumed");
        assert_no_false_negatives(&c, n, &value, &[3, 15_000, 25_000, 39_999]);
    }

    /// A gated-out column writes no planes, and its probes stay supersets.
    #[test]
    fn a_gated_column_writes_no_planes() {
        let n = 20_000u64;
        let value = |s: u64| (s % 9) as i64;
        let mut c = col();
        fill(&mut c, 0..n, value);
        assert!(c.is_gated_out());
        assert!(planeless_zones(&c) > 0);
        let with_planes: usize = c.generations().iter().map(|g| g.words.len() * 8).sum();
        assert!(with_planes <= 4 * ZONE_ROWS, "only the first zone carries planes: {} B", with_planes);
        assert_no_false_negatives(&c, n, &value, &[0, 4, 8]);
    }

    fn reload(c: &ColumnFilter, n: u64, buf: &[u8]) -> ColumnFilter {
        let expected = [(c.write_order, c.db_type.clone())];
        let mut loaded = persist::deserialize(buf, HashSeed::default(), &expected, FilterConfig::default(), n)
            .unwrap_or_else(|e| panic!("load: {:?}", e));
        assert_eq!(loaded.columns.len(), 1);
        loaded.columns.remove(0)
    }

    /// Plane-less generations survive a checkpoint, and so does every answer.
    #[test]
    fn plane_less_generations_round_trip_a_checkpoint_and_a_delta() {
        let n1 = 20_000u64;
        let n = 30_000u64;
        let value = |s: u64| s as i64;
        let mut c = col();
        fill(&mut c, 0..n1, value);
        let base = persist::serialize(&[&c], HashSeed::default(), n1);
        let reopened = reload(&c, n1, &base);
        assert_eq!(planeless_zones(&reopened), planeless_zones(&c));
        assert_eq!(reopened.zone_count(), c.zone_count());
        assert_no_false_negatives(&reopened, n1, &value, &[0, 777, 19_999]);

        // More rows, then an incremental record on top of the base.
        let from = c.delta_from_zone();
        c.mark_checkpointed();
        let from = from.min(c.delta_from_zone());
        fill(&mut c, n1..n, value);
        let mut buf = base.clone();
        buf.extend_from_slice(&persist::serialize_delta(&[&c], &[from.min(n1 / ZONE_ROWS as u64)], n));
        let replayed = reload(&c, n, &buf);
        assert_eq!(planeless_zones(&replayed), planeless_zones(&c));
        assert_no_false_negatives(&replayed, n, &value, &[1, 20_000, 29_999]);
    }

    /// A version-4 file — written before plane-less generations existed —
    /// reads unchanged; one claiming a plane-less generation is refused.
    #[test]
    fn a_version_4_checkpoint_still_loads() {
        let n = 5_000u64;
        let value = |s: u64| (s.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 20) as i64;
        let mut c = col();
        fill(&mut c, 0..n, value);
        assert_eq!(planeless_zones(&c), 0);
        let mut buf = persist::serialize(&[&c], HashSeed::default(), n);
        // The version field follows the 8-byte magic.
        assert_eq!(u16::from_le_bytes([buf[8], buf[9]]), 5);
        buf[8..10].copy_from_slice(&4u16.to_le_bytes());
        let old = reload(&c, n, &buf);
        assert_eq!(old.zone_count(), c.zone_count());
        assert_no_false_negatives(&old, n, &value, &[0, 2_500, 4_999]);

        // v3 and earlier stay refused.
        buf[8..10].copy_from_slice(&3u16.to_le_bytes());
        assert!(matches!(
            persist::deserialize(&buf, HashSeed::default(), &[(0, DbType::I64)], FilterConfig::default(), n),
            Err(persist::LoadReject::Version(3))
        ));

        // A v4 file cannot hold a plane-less generation.
        let mut clustered = col();
        fill(&mut clustered, 0..n * 4, |s| s as i64);
        assert!(planeless_zones(&clustered) > 0);
        let mut buf = persist::serialize(&[&clustered], HashSeed::default(), n * 4);
        buf[8..10].copy_from_slice(&4u16.to_le_bytes());
        assert!(persist::deserialize(&buf, HashSeed::default(), &[(0, DbType::I64)], FilterConfig::default(), n * 4).is_err());
    }

    /// Bytes per row and point-probe cost on a 10M-row serial column, planes
    /// against plane-less. `cargo test --release -p rasterizeddb_core --lib --
    /// --ignored planeless_point_probe_bench --nocapture`.
    #[test]
    #[ignore]
    fn planeless_point_probe_bench() {
        let n = 10_000_000u64;
        let mut clustered = col();
        fill(&mut clustered, 0..n, |s| s as i64);
        // The same data with the clustering decision defeated: one row out of
        // order per zone keeps the bounds overlapping, so planes stay on.
        let mut planes = col();
        fill(&mut planes, 0..n, |s| if s % ZONE_ROWS as u64 == 0 { (n - s) as i64 } else { s as i64 });
        let mut bm = vec![0u64; bitmap_words(n)];
        for (label, c) in [("plane-less", &clustered), ("planes", &planes)] {
            let t = std::time::Instant::now();
            let mut survivors = 0;
            for k in 0..100u64 {
                let v = (k * 99_991 % n) as i64 + 1;
                c.probe_pred_into(SargOp::Eq, &v.to_le_bytes(), n, &mut bm);
                survivors += count(&bm);
            }
            println!(
                "{label:<11} {:>6.1} MB ({:.2} B/row)  100 point probes {:>9.2?}  mean survivors {}",
                c.bytes() as f64 / 1e6,
                c.bytes() as f64 / n as f64,
                t.elapsed(),
                survivors / 100
            );
        }
    }
}
