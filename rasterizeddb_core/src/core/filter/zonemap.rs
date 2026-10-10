//! Per-zone min/max bounds — the companion structure that covers the
//! predicates fingerprints cannot touch at all.
//!
//! A fingerprint answers "might this row hold exactly `v`?". It says nothing
//! about `>`, `<` or `BETWEEN`. Two values per 512-row zone does, at **0.25
//! bits/row for a `u64` column** — a sixteenth of what an `f = 4` fingerprint
//! costs — and for data with any locality at all (auto-increment ids,
//! timestamps, anything appended in order) it prunes far harder.
//!
//! # Bounds only ever widen
//!
//! Deleting a row cannot shrink a zone's range: we would have to know whether
//! the removed value was the sole occupant of the bound. So a delete leaves the
//! range conservatively wide, and an update widens it if the new value falls
//! outside. Both directions are safe — a too-wide range prunes less, never
//! wrongly — and both are corrected by a rebuild. This is the one place the
//! structure behaves like a Bloom filter rather than like the exact
//! fingerprints.
//!
//! # Comparisons come from the evaluator
//!
//! Every bound test goes through `perform_comparison_operation`, the same
//! function the `WHERE` evaluator uses. There is deliberately no second
//! comparison implementation here to drift out of agreement with it — the same
//! discipline [`super::hash`] applies to equality.
//!
//! # Two bound shapes
//!
//! A fixed-width type stores its bounds *exactly*: two values per zone, and the
//! bound is the value. `STRING` cannot, because the array has to have a stride,
//! so it stores the first [`STRING_BOUND_PREFIX`] bytes and accepts that the
//! bounds are looser than the data.
//!
//! Truncation is **asymmetric**, and getting it wrong is the one bug in this
//! module that returns silently wrong answers rather than merely pruning badly.
//! Cutting a string short makes it lexicographically *smaller*:
//!
//! - a `min` bound moves **down**, which widens the range — safe;
//! - a `max` bound also moves down, which *narrows* the range — and a range too
//!   narrow prunes rows that match.
//!
//! So `max` rounds **up** instead: [`round_up`] increments the truncated prefix
//! as a big-endian integer with carry, and reports "no upper bound at all" when
//! the prefix is every byte `0xFF` and there is nothing above it to name. A
//! value that fits in the prefix is not truncated and needs no rounding.

use crate::core::db_type::DbType;
use crate::core::processor::transformer::ComparerOperation;
use crate::core::row::logical::perform_comparison_operation;

use super::kernels::ZONE_ROWS;

/// A predicate shape the filter can act on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SargOp {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
}

impl SargOp {
    /// The equivalent predicate when the column is on the *right* of the
    /// operator, e.g. `5 < col` means `col > 5`.
    pub fn mirrored(self) -> Self {
        match self {
            SargOp::Eq => SargOp::Eq,
            SargOp::Gt => SargOp::Lt,
            SargOp::Ge => SargOp::Le,
            SargOp::Lt => SargOp::Gt,
            SargOp::Le => SargOp::Ge,
        }
    }

    #[inline]
    pub fn is_range(self) -> bool {
        !matches!(self, SargOp::Eq)
    }
}

/// Fixed serialized width for a type whose bounds are stored exactly.
///
/// `None` for `STRING`, which is not excluded from bounds — it simply stores
/// them in the truncated form [`BoundKind::Prefix`] describes rather than as
/// whole values. Callers deciding *whether* a type has bounds want
/// [`ZoneMap::new`]; callers that need the exact stride of a fixed-width type
/// want this.
#[inline]
pub const fn ordered_width(db_type: &DbType) -> Option<usize> {
    Some(match db_type {
        // `BOOL` carries no fingerprints (see `hash::is_fingerprintable`) but is
        // indexed for these bounds alone: a zone with no `1` in it rules out
        // `flag = 1` outright, which is how `IS NULL` prunes.
        DbType::I8 | DbType::U8 | DbType::BOOL => 1,
        DbType::I16 | DbType::U16 => 2,
        DbType::I32 | DbType::U32 | DbType::F32 | DbType::CHAR => 4,
        DbType::I64 | DbType::U64 | DbType::F64 | DbType::DATETIME => 8,
        DbType::I128 | DbType::U128 | DbType::DECIMAL => 16,
        // UUID and INET compare by `memcmp` over their fixed octet layouts,
        // which is a total order, so bounds are meaningful. UUIDv7/ULID keys
        // additionally sort by time, making them strong zone-map candidates —
        // unlike UUIDv4, whose bounds overlap in every zone and prune nothing.
        DbType::UUID => crate::core::value_codec::UUID_LEN,
        DbType::INET => crate::core::value_codec::INET_LEN,
        _ => return None,
    })
}

/// Bytes of a string kept per bound.
///
/// The whole per-zone cost is `2 * (1 + STRING_BOUND_PREFIX)` bytes, so 34 at
/// 16 — **0.53 bits/row** at 512 rows to a zone. Wider prefixes buy tighter
/// bounds on long shared-prefix data (URLs, reverse-DNS names) at a linear cost;
/// 16 covers the short identifiers string columns usually hold, and covers any
/// `LIKE 'abc%'` prefix up to that length without truncating the *query* side at
/// all — a query literal is never truncated, only the stored bounds are.
pub const STRING_BOUND_PREFIX: usize = 16;

/// `max_len` sentinel meaning "this zone has no upper bound".
///
/// Reached when [`round_up`] carries off the end of an all-`0xFF` prefix: there
/// is no representable string above it, so the zone is unbounded and every `>`
/// and `>=` probe has to treat it as a candidate.
const UNBOUNDED: u8 = 0xFF;

/// How one zone's bounds are laid out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BoundKind {
    /// `min || max`, `width` bytes each, both exact.
    Fixed { width: usize },
    /// `min_len | min[k] | max_len | max[k]`, with lengths capped at `k` and
    /// `max_len == UNBOUNDED` marking a zone with no upper bound.
    ///
    /// The length bytes are load-bearing rather than bookkeeping: zero-padding
    /// a short string to `k` would change what it compares as. `"ab"` is *less*
    /// than `"ab\0\0"` under the engine's ordering, which breaks a `min` bound
    /// in the unsafe direction, so the stored length is what makes the slice
    /// handed to `perform_comparison_operation` the value we meant.
    Prefix { k: usize },
}

impl BoundKind {
    /// Bytes one zone occupies.
    #[inline]
    const fn stride(self) -> usize {
        match self {
            BoundKind::Fixed { width } => 2 * width,
            BoundKind::Prefix { k } => 2 * (1 + k),
        }
    }

    #[inline]
    const fn of(db_type: &DbType) -> Option<Self> {
        if matches!(db_type, DbType::STRING) {
            return Some(BoundKind::Prefix {
                k: STRING_BOUND_PREFIX,
            });
        }
        match ordered_width(db_type) {
            Some(0) | None => None,
            Some(width) => Some(BoundKind::Fixed { width }),
        }
    }
}

/// The smallest value that is greater than every string beginning with `value`,
/// stored in at most `k` bytes. `None` when no such value is representable.
///
/// A value short enough to store whole needs no rounding: it *is* its own
/// inclusive upper bound. A longer one is truncated to `k` bytes and then
/// incremented with carry, which is what keeps the bound above the data instead
/// of below it. Trailing `0xFF` bytes roll to `0x00` as the carry propagates;
/// if it propagates past byte zero the prefix was all `0xFF` and nothing above
/// it can be named, so the zone becomes unbounded.
fn round_up(value: &[u8], k: usize) -> Option<Vec<u8>> {
    if value.len() <= k {
        return Some(value.to_vec());
    }
    successor(&value[..k])
}

/// The smallest byte string strictly greater than **every** string beginning
/// with `prefix`. `None` when `prefix` is every byte `0xFF`, where no such
/// string is representable.
///
/// This is the other half of `LIKE 'abc%'`: the predicate is exactly
/// `col >= 'abc' AND col < successor('abc')`, because incrementing the last
/// byte that can be incremented (rolling trailing `0xFF` bytes to `0x00`)
/// produces a value above every extension of the prefix and below nothing else
/// that starts with it. See [`super::sarg`], which does that lowering.
pub fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut p = prefix.to_vec();
    let mut i = p.len();
    loop {
        if i == 0 {
            return None;
        }
        i -= 1;
        if p[i] == 0xFF {
            p[i] = 0;
        } else {
            p[i] += 1;
            return Some(p);
        }
    }
}

/// Comparison for *probing*. An error fails the query in the evaluator anyway,
/// so the filter must not prune on it: answer "possible" and let the evaluator
/// decide.
#[inline]
fn cmp(a: &[u8], b: &[u8], ty: &DbType, op: ComparerOperation) -> bool {
    perform_comparison_operation(a, b, ty, &op).unwrap_or(true)
}

/// Comparison for *maintaining* bounds, where the permissive default above
/// would be actively wrong.
///
/// `observe` widens `min`/`max` by asking "is this value below/above the
/// current bound?". Defaulting an error to `true` there would overwrite both
/// bounds with whatever arrived last, producing a range that no longer brackets
/// the zone's data — and a range that is too *narrow* prunes rows that match.
/// So errors are surfaced and the caller invalidates the zone instead.
#[inline]
fn cmp_strict(a: &[u8], b: &[u8], ty: &DbType, op: ComparerOperation) -> Option<bool> {
    perform_comparison_operation(a, b, ty, &op).ok()
}

/// Min/max bounds for every zone of one column.
#[derive(Clone)]
pub struct ZoneMap {
    db_type: DbType,
    kind: BoundKind,
    /// `zone_count * kind.stride()`: min then max, per zone.
    data: Vec<u8>,
    /// One bit per zone: whether any value has been recorded (NaN included).
    valid: Vec<u64>,
}

impl ZoneMap {
    pub fn new(db_type: DbType) -> Option<Self> {
        let kind = BoundKind::of(&db_type)?;
        Some(Self {
            db_type,
            kind,
            data: Vec::new(),
            valid: Vec::new(),
        })
    }

    pub fn from_parts(db_type: DbType, data: Vec<u8>, valid: Vec<u64>) -> Option<Self> {
        let kind = BoundKind::of(&db_type)?;
        let stride = kind.stride();
        if stride == 0 || data.len() % stride != 0 {
            return None;
        }
        Some(Self {
            db_type,
            kind,
            data,
            valid,
        })
    }

    #[inline]
    fn stride(&self) -> usize {
        self.kind.stride()
    }

    #[inline]
    pub fn zone_count(&self) -> u64 {
        (self.data.len() / self.stride()) as u64
    }

    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    pub fn valid_bits(&self) -> &[u64] {
        &self.valid
    }

    pub fn bytes(&self) -> usize {
        self.data.len() + self.valid.len() * 8
    }

    #[inline]
    fn is_valid(&self, zone: u64) -> bool {
        let w = (zone / 64) as usize;
        self.valid
            .get(w)
            .is_some_and(|v| (v >> (zone % 64)) & 1 == 1)
    }

    #[inline]
    fn clear_valid(&mut self, zone: u64) {
        let w = (zone / 64) as usize;
        if let Some(v) = self.valid.get_mut(w) {
            *v &= !(1 << (zone % 64));
        }
    }

    #[inline]
    fn set_valid(&mut self, zone: u64) {
        let w = (zone / 64) as usize;
        if self.valid.len() <= w {
            self.valid.resize(w + 1, 0);
        }
        self.valid[w] |= 1 << (zone % 64);
    }

    fn ensure_zone(&mut self, zone: u64) {
        let needed = (zone as usize + 1) * self.stride();
        if self.data.len() < needed {
            self.data.resize(needed, 0);
        }
    }

    /// The zone's lower bound. Always present when the zone is valid.
    #[inline]
    fn min_of(&self, zone: u64) -> &[u8] {
        let o = zone as usize * self.stride();
        match self.kind {
            BoundKind::Fixed { width } => &self.data[o..o + width],
            BoundKind::Prefix { k } => {
                let len = (self.data[o] as usize).min(k);
                &self.data[o + 1..o + 1 + len]
            }
        }
    }

    /// Whether the column is already ordered ascending in slot order, across
    /// the first `zones_covered` zones.
    ///
    /// This is the property that makes `ORDER BY col LIMIT k` answerable
    /// without sorting the table: if every zone's values sit at or above the
    /// previous zone's maximum, then slot order *is* sort order, and the first
    /// `k` rows by slot are the first `k` rows by value. A column of assigned
    /// ids or append-time timestamps satisfies it by construction.
    ///
    /// Deliberately conservative — every uncertainty answers `false`, because
    /// the cost of a wrong `true` is rows returned in the wrong order while
    /// reported as sorted, and the cost of a wrong `false` is only the sort we
    /// would have done anyway:
    ///
    /// * **Fixed-width bounds only.** Truncated string prefixes compare as
    ///   their prefix, so `"ab…"` and `"ab…"` in adjacent zones cannot be
    ///   ordered against each other from the bounds alone.
    /// * **Full coverage required.** A zone past `zones_covered`, or one whose
    ///   valid bit is clear, could hold anything — including a value that
    ///   breaks the order.
    /// * **An unbounded zone answers `false`**, since its maximum is unknown.
    /// * **A comparison that errors answers `false`** rather than defaulting
    ///   to "possible" as the probing path does. Here the permissive default
    ///   would be the unsafe direction.
    ///
    /// Note this stays true under deletes: bounds only ever widen, so a delete
    /// can turn a monotonic column into one that no longer *looks* monotonic,
    /// which declines the optimisation. It cannot make a non-monotonic column
    /// look monotonic.
    pub fn is_monotonic_ascending(&self, zones_covered: u64) -> bool {
        if !matches!(self.kind, BoundKind::Fixed { .. }) {
            return false;
        }
        if zones_covered == 0 || zones_covered > self.zone_count() {
            return false;
        }

        let mut previous_max: Option<&[u8]> = None;
        for zone in 0..zones_covered {
            if !self.is_valid(zone) {
                return false;
            }
            let min = self.min_of(zone);
            let Some(max) = self.max_of(zone) else {
                return false;
            };
            if let Some(prev) = previous_max {
                // Adjacent zones may touch but not overlap: the previous
                // zone's largest value must not exceed this one's smallest.
                match cmp_strict(prev, min, &self.db_type, ComparerOperation::LessOrEquals) {
                    Some(true) => {}
                    _ => return false,
                }
            }
            previous_max = Some(max);
        }
        true
    }

    /// Whether zones `[first, end)` ascend without overlapping, under the same
    /// conservative rules as [`Self::is_monotonic_ascending`]: fixed-width
    /// bounds only, every zone valid and bounded, adjacent zones may touch.
    ///
    /// Used to decide whether a column's bounds are clustered enough to stand in
    /// for fingerprint planes. A wrong `true` there costs only selectivity, but
    /// the rules are kept identical so there is one notion of "ascending".
    pub fn is_ascending_between(&self, first: u64, end: u64) -> bool {
        if !matches!(self.kind, BoundKind::Fixed { .. }) {
            return false;
        }
        if first >= end || end > self.zone_count() {
            return false;
        }
        let mut previous_max: Option<&[u8]> = None;
        for zone in first..end {
            if !self.is_valid(zone) {
                return false;
            }
            let min = self.min_of(zone);
            let Some(max) = self.max_of(zone) else {
                return false;
            };
            if let Some(prev) = previous_max {
                match cmp_strict(prev, min, &self.db_type, ComparerOperation::LessOrEquals) {
                    Some(true) => {}
                    _ => return false,
                }
            }
            previous_max = Some(max);
        }
        true
    }

    /// The zone's upper bound, or `None` when it has none — see [`UNBOUNDED`].
    #[inline]
    fn max_of(&self, zone: u64) -> Option<&[u8]> {
        let o = zone as usize * self.stride();
        match self.kind {
            BoundKind::Fixed { width } => Some(&self.data[o + width..o + 2 * width]),
            BoundKind::Prefix { k } => {
                let at = o + 1 + k;
                let raw = self.data[at];
                if raw == UNBOUNDED {
                    return None;
                }
                let len = (raw as usize).min(k);
                Some(&self.data[at + 1..at + 1 + len])
            }
        }
    }

    #[inline]
    fn put_min(&mut self, zone: u64, value: &[u8]) {
        let o = zone as usize * self.stride();
        match self.kind {
            BoundKind::Fixed { width } => self.data[o..o + width].copy_from_slice(value),
            BoundKind::Prefix { k } => {
                // Truncation moves a lower bound *down*, which is the safe
                // direction: it widens the range rather than narrowing it.
                let len = value.len().min(k);
                self.data[o] = len as u8;
                self.data[o + 1..o + 1 + len].copy_from_slice(&value[..len]);
                self.data[o + 1 + len..o + 1 + k].fill(0);
            }
        }
    }

    /// Store an upper bound already rounded by [`round_up`]; `None` is
    /// "unbounded above".
    #[inline]
    fn put_max(&mut self, zone: u64, value: Option<&[u8]>) {
        let o = zone as usize * self.stride();
        match self.kind {
            BoundKind::Fixed { width } => {
                let v = value.expect("a fixed-width bound is never unbounded");
                self.data[o + width..o + 2 * width].copy_from_slice(v);
            }
            BoundKind::Prefix { k } => {
                let at = o + 1 + k;
                match value {
                    None => {
                        self.data[at] = UNBOUNDED;
                        self.data[at + 1..at + 1 + k].fill(0xFF);
                    }
                    Some(v) => {
                        let len = v.len().min(k);
                        self.data[at] = len as u8;
                        self.data[at + 1..at + 1 + len].copy_from_slice(&v[..len]);
                        self.data[at + 1 + len..at + 1 + k].fill(0);
                    }
                }
            }
        }
    }

    /// Widen the zone's bounds to include `value`.
    ///
    /// NaN *is* recorded in the bounds, as the largest value. The comparisons here
    /// go through `perform_comparison_operation`, which orders floats as
    /// PostgreSQL does — NaN equal to NaN and above +Infinity — so a NaN is an
    /// ordinary maximum: `f > x` keeps its zone, `f = 'NaN'` keeps only zones whose
    /// maximum is NaN, and a zone holding NaN never looks ascending against the
    /// next one. It used to be left out, which was right only while comparisons
    /// were IEEE's (where NaN satisfies nothing); under PostgreSQL's order it
    /// pruned NaN rows from `f > x` and let `ORDER BY f LIMIT k` return a NaN among
    /// the smallest values. Sidecars written before this carry NaN-blind bounds,
    /// which is why the persisted format version moved to 4: an older file is
    /// rejected and rebuilt rather than trusted.
    pub fn observe(&mut self, zone: u64, value: &[u8]) {
        if !self.accepts(value) {
            return;
        }
        self.ensure_zone(zone);

        if !self.is_valid(zone) {
            self.put_min(zone, value);
            let rounded = self.rounded_max(value);
            self.put_max(zone, rounded.as_deref());
            self.set_valid(zone);
            return;
        }

        let ty = self.db_type.clone();
        let below = cmp_strict(value, self.min_of(zone), &ty, ComparerOperation::Less);
        // An unbounded zone is already above everything, so there is nothing to
        // widen and nothing to compare against.
        let above = match self.max_of(zone) {
            None => Some(false),
            Some(max) => cmp_strict(value, max, &ty, ComparerOperation::Greater),
        };

        let (Some(below), Some(above)) = (below, above) else {
            // The type cannot be ordered after all. Drop this zone's bounds
            // rather than keep ones we can no longer maintain: an unbounded zone
            // is always a candidate, which costs selectivity, never rows.
            self.clear_valid(zone);
            return;
        };

        if below {
            self.put_min(zone, value);
        }
        if above {
            let rounded = self.rounded_max(value);
            self.put_max(zone, rounded.as_deref());
        }
    }

    /// The upper bound to store for `value`: itself for a type kept exactly, and
    /// the rounded-up prefix for a truncated one.
    #[inline]
    fn rounded_max(&self, value: &[u8]) -> Option<Vec<u8>> {
        match self.kind {
            BoundKind::Fixed { .. } => Some(value.to_vec()),
            BoundKind::Prefix { k } => round_up(value, k),
        }
    }

    /// Whether a value or probe of this length can be compared against these
    /// bounds at all.
    ///
    /// A fixed-width type demands exact width equality, which is what
    /// `perform_comparison_operation` demands of it: a shorter or longer slice
    /// makes it return `OperandSizeMismatch`, and the filter must not prune on
    /// an error the evaluator would itself have raised. A variable-width type
    /// has no such constraint — comparing `"ab"` with `"abcdef"` is ordinary.
    #[inline]
    fn accepts(&self, value: &[u8]) -> bool {
        match self.kind {
            BoundKind::Fixed { width } => value.len() == width,
            BoundKind::Prefix { .. } => true,
        }
    }

    /// Whether a probe value is one these bounds can be compared against.
    #[inline]
    pub fn probes_with(&self, value: &[u8]) -> bool {
        self.accepts(value)
    }

    /// The bound bytes for zones `[from_zone, ..)`, for an incremental
    /// checkpoint. Empty when the zone is past the end.
    pub fn tail_from(&self, from_zone: u64) -> &[u8] {
        let at = (from_zone as usize).saturating_mul(self.stride());
        self.data.get(at..).unwrap_or(&[])
    }

    /// Splice a tail written by [`Self::tail_from`] back in, replacing every
    /// zone from `from_zone` on.
    ///
    /// The validity bits are replaced wholesale rather than spliced: a zone
    /// below `from_zone` can still have had its bit *cleared* by an ordering
    /// failure, and at one bit per zone the whole vector is small enough that
    /// re-sending it costs less than reasoning about which half changed.
    ///
    /// Returns `false` for a tail that cannot belong to this column, which the
    /// caller turns into "stop replaying" rather than a wrong answer.
    pub fn apply_tail(&mut self, from_zone: u64, data: &[u8], valid: Vec<u64>) -> bool {
        let stride = self.stride();
        if data.len() % stride != 0 {
            return false;
        }
        let Some(keep) = (from_zone as usize).checked_mul(stride) else {
            return false;
        };
        if self.data.len() < keep {
            self.data.resize(keep, 0);
        }
        self.data.truncate(keep);
        self.data.extend_from_slice(data);
        self.valid = valid;
        true
    }

    /// Could any row in this zone satisfy `col op value`?
    ///
    /// Conservative by construction: an unbounded zone, a width mismatch or an
    /// out-of-range index all answer `true`.
    pub fn zone_may_match(&self, zone: u64, op: SargOp, value: &[u8]) -> bool {
        if !self.accepts(value) || zone >= self.zone_count() || !self.is_valid(zone) {
            return true;
        }
        let ty = &self.db_type;
        let min = self.min_of(zone);
        let max = self.max_of(zone);

        // A zone with no upper bound can hold anything above `min`, so every
        // test that leans on `max` has to answer "possible".
        let above_min = |op| max.is_none_or(|m| cmp(m, value, ty, op));

        match op {
            // A NaN probe compares as the largest value, which is what
            // PostgreSQL's order makes it: `x = 'NaN'` keeps the zones whose
            // maximum is NaN, `x > 'NaN'` keeps none.
            SargOp::Eq => {
                cmp(min, value, ty, ComparerOperation::LessOrEquals)
                    && above_min(ComparerOperation::GreaterOrEquals)
            }
            SargOp::Gt => above_min(ComparerOperation::Greater),
            SargOp::Ge => above_min(ComparerOperation::GreaterOrEquals),
            SargOp::Lt => cmp(min, value, ty, ComparerOperation::Less),
            SargOp::Le => cmp(min, value, ty, ComparerOperation::LessOrEquals),
        }
    }
}

/// Zones covering `n_slots`.
#[inline]
pub fn zones_for(n_slots: u64) -> u64 {
    n_slots.div_ceil(ZONE_ROWS as u64)
}

#[cfg(test)]
mod monotonic_tests {
    use super::*;

    fn u64_map(zones: &[(u64, u64)]) -> ZoneMap {
        let mut zm = ZoneMap::new(DbType::U64).expect("U64 carries bounds");
        for (zone, &(lo, hi)) in zones.iter().enumerate() {
            zm.observe(zone as u64, &lo.to_le_bytes());
            zm.observe(zone as u64, &hi.to_le_bytes());
        }
        zm
    }

    /// The property the optimisation rests on: ascending, non-overlapping
    /// zones mean slot order is sort order.
    #[test]
    fn ascending_non_overlapping_zones_are_monotonic() {
        let zm = u64_map(&[(0, 511), (512, 1023), (1024, 1535)]);
        assert!(zm.is_monotonic_ascending(3));
    }

    /// Touching at a boundary is still ordered — a duplicate value spanning two
    /// zones does not break "the first k by slot are the first k by value".
    #[test]
    fn zones_that_touch_are_still_monotonic() {
        let zm = u64_map(&[(0, 512), (512, 1023)]);
        assert!(zm.is_monotonic_ascending(2));
    }

    #[test]
    fn overlapping_zones_are_not_monotonic() {
        // The second zone starts below where the first ended.
        let zm = u64_map(&[(0, 900), (512, 1023)]);
        assert!(!zm.is_monotonic_ascending(2));
    }

    #[test]
    fn descending_zones_are_not_monotonic() {
        let zm = u64_map(&[(1024, 1535), (0, 511)]);
        assert!(!zm.is_monotonic_ascending(2));
    }

    /// Coverage is the safety property: a zone nobody has indexed could hold
    /// anything, so claiming order across it would be a guess.
    #[test]
    fn an_unindexed_zone_refuses() {
        let zm = u64_map(&[(0, 511), (512, 1023)]);
        // Asking about more zones than exist must not read past the end.
        assert!(!zm.is_monotonic_ascending(3));
        // Zero zones is not "trivially ordered", it is "nothing known".
        assert!(!zm.is_monotonic_ascending(0));
    }

    /// A zone whose valid bit is clear has recorded no value; ordering across
    /// it would be asserted rather than observed.
    #[test]
    fn a_zone_with_no_recorded_value_refuses() {
        let mut zm = ZoneMap::new(DbType::U64).unwrap();
        zm.observe(0, &0u64.to_le_bytes());
        zm.observe(2, &1024u64.to_le_bytes());
        // Zone 1 exists in the array but nothing was ever recorded in it.
        assert!(!zm.is_monotonic_ascending(3));
    }

    /// Strings carry truncated prefixes, which cannot be ordered against each
    /// other from the bounds alone.
    #[test]
    fn variable_width_bounds_always_refuse() {
        let mut zm = ZoneMap::new(DbType::STRING).expect("STRING carries bounds");
        zm.observe(0, b"aaaa");
        zm.observe(1, b"bbbb");
        assert!(
            !zm.is_monotonic_ascending(2),
            "prefix bounds must never claim a total order"
        );
    }

    /// Signed types must order as signed, not by their little-endian bytes.
    #[test]
    fn negative_values_order_as_numbers() {
        let mut zm = ZoneMap::new(DbType::I64).expect("I64 carries bounds");
        zm.observe(0, &(-100i64).to_le_bytes());
        zm.observe(0, &(-1i64).to_le_bytes());
        zm.observe(1, &0i64.to_le_bytes());
        zm.observe(1, &100i64.to_le_bytes());
        assert!(
            zm.is_monotonic_ascending(2),
            "-100..-1 then 0..100 is ascending"
        );

        let mut back = ZoneMap::new(DbType::I64).unwrap();
        back.observe(0, &0i64.to_le_bytes());
        back.observe(1, &(-1i64).to_le_bytes());
        assert!(!back.is_monotonic_ascending(2));
    }
}
