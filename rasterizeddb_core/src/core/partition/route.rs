//! The router (§4.2): a `WHERE` clause's sarg program to the routing points
//! its matching rows can occupy. Always a superset.
//!
//! The input is what `sarg::extract_gated` returns with a gate admitting only
//! the key's write_orders (not `extract_gated_with_paths`), and whether
//! `sarg::clause_is_constant_false` holds. That flag is the only way to route
//! to nothing: everything else widens when unsure, since pruning without proof
//! is a silent wrong answer and fanning out is only slower.

use std::collections::{BTreeSet, HashSet};

use smallvec::SmallVec;

use crate::core::filter::sarg::MaskExpr;
use crate::core::filter::zonemap::SargOp;

use super::key::{KeySpec, MAX_ARITY};
use super::map::{PointRange, RangeMap};

/// The most points one statement routes to (§1b, "Multi-column IN"). Past it
/// the boxes degrade from the right, which only widens.
pub const POINT_CAP: u64 = 256;

/// Where a statement's rows can be.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Route {
    /// No row can match. Only from `clause_is_constant_false`.
    Zero,
    /// Every partition.
    All,
    /// Sorted, disjoint, non-adjacent half-open intervals. Never empty, and
    /// never the whole space (that is `All`).
    Points(Vec<PointRange>),
}

impl Route {
    /// The partitions to scan under `map`, sorted by pid.
    pub fn pids(&self, map: &RangeMap) -> Vec<u32> {
        let mut pids: Vec<u32> = match self {
            Route::Zero => return Vec::new(),
            Route::All => map.entries().iter().map(|&(_, pid)| pid).collect(),
            Route::Points(ranges) => ranges
                .iter()
                .flat_map(|r| map.pids_overlapping(r.lo, r.hi))
                .collect(),
        };
        pids.sort_unstable();
        pids.dedup();
        pids
    }

    /// Whether `p` is routed to.
    pub fn contains(&self, p: u64) -> bool {
        match self {
            Route::Zero => false,
            Route::All => true,
            Route::Points(ranges) => ranges.iter().any(|r| r.contains(p)),
        }
    }
}

/// Routes a statement on a table keyed by `spec`. `mask` is the root of
/// `extract_gated`'s program (`None`: nothing narrowed), `constant_false`
/// whether `clause_is_constant_false` held.
pub fn route(spec: &KeySpec, mask: Option<&MaskExpr>, constant_false: bool) -> Route {
    if constant_false {
        return Route::Zero;
    }
    let Some(mask) = mask else {
        return Route::All;
    };
    expand(spec, &fold(spec, mask))
}

/// A level's admissible values: sorted and distinct.
type ValueSet = Vec<Vec<u8>>;

/// The rows whose value at each constrained level is in that level's set;
/// `None` is unconstrained.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct KeyBox([Option<ValueSet>; MAX_ARITY]);

impl KeyBox {
    fn any() -> Self {
        KeyBox::default()
    }

    fn one(level: usize, set: ValueSet) -> Self {
        let mut b = KeyBox::any();
        b.0[level] = Some(set);
        b
    }

    fn is_any(&self) -> bool {
        self.0.iter().all(Option::is_none)
    }

    /// Points it expands to at most: the product of its sets' sizes. Counts
    /// every constrained level, including ones expansion will ignore, so it
    /// also bounds the values held.
    fn measure(&self) -> u64 {
        self.0
            .iter()
            .flatten()
            .fold(1u64, |m, s| m.saturating_mul(s.len() as u64))
    }

    fn values(&self) -> usize {
        self.0.iter().flatten().map(Vec::len).sum()
    }

    fn last_constrained(&self) -> Option<usize> {
        self.0.iter().rposition(Option::is_some)
    }

    fn drop_from(&mut self, level: usize) {
        for s in &mut self.0[level.min(MAX_ARITY)..] {
            *s = None;
        }
    }

    /// The box of `self AND other`. Sets for one level are unioned, never
    /// intersected: `t = 1 AND t = 2` routes to both tenants. An empty
    /// intersection would route to nothing without the proof only
    /// `clause_is_constant_false` gives, and would skip the scan that raises
    /// any evaluation error.
    fn meet(&self, other: &KeyBox) -> KeyBox {
        let mut out = KeyBox::any();
        for (o, (a, b)) in out.0.iter_mut().zip(self.0.iter().zip(&other.0)) {
            *o = match (a, b) {
                (Some(a), Some(b)) => {
                    let mut u: ValueSet = a.iter().chain(b).cloned().collect();
                    u.sort_unstable();
                    u.dedup();
                    Some(u)
                }
                (Some(s), None) | (None, Some(s)) => Some(s.clone()),
                (None, None) => None,
            };
        }
        out
    }
}

/// A union of boxes kept at most [`POINT_CAP`] points by degrading as it
/// grows, so memory stays O(cap × levels) whatever the clause.
struct Boxes {
    set: HashSet<KeyBox>,
    /// Sum of the boxes' measures.
    points: u64,
    values: usize,
    /// Levels from here on were dropped by a degrade. Boxes added later drop
    /// them too, so a degraded union does not regrow and degrade again.
    limit: usize,
}

impl Boxes {
    fn new() -> Self {
        Boxes {
            set: HashSet::new(),
            points: 0,
            values: 0,
            limit: MAX_ARITY,
        }
    }

    fn single(b: KeyBox) -> Self {
        let mut out = Boxes::new();
        out.push(b);
        out
    }

    fn any() -> Self {
        Boxes::single(KeyBox::any())
    }

    /// Everything; then it holds only the unconstrained box.
    fn is_any(&self) -> bool {
        self.set.contains(&KeyBox::any())
    }

    fn set_any(&mut self) {
        self.set.clear();
        self.set.insert(KeyBox::any());
        self.points = 1;
        self.values = 0;
    }

    fn push(&mut self, mut b: KeyBox) {
        note_work(1);
        if self.is_any() {
            return;
        }
        b.drop_from(self.limit);
        // One box over the cap degrades on its own; the others keep their
        // levels.
        while b.measure() > POINT_CAP {
            let level = b.last_constrained().expect("an unconstrained box measures 1");
            b.drop_from(level);
        }
        if b.is_any() {
            self.set_any();
            return;
        }
        let (m, v) = (b.measure(), b.values());
        if self.set.insert(b) {
            self.points += m;
            self.values += v;
            note_peak(self.set.len(), self.values);
            self.degrade();
        }
    }

    /// While over the cap, drops the rightmost constrained level from every
    /// box and de-duplicates. Dropping level 0 is everything.
    fn degrade(&mut self) {
        while self.points > POINT_CAP {
            let level = self
                .set
                .iter()
                .filter_map(KeyBox::last_constrained)
                .max()
                .expect("over the cap, so some level is constrained");
            self.limit = self.limit.min(level);
            let old = std::mem::take(&mut self.set);
            self.points = 0;
            self.values = 0;
            note_work(old.len() as u64);
            for mut b in old {
                b.drop_from(level);
                if b.is_any() {
                    self.set_any();
                    return;
                }
                let (m, v) = (b.measure(), b.values());
                if self.set.insert(b) {
                    self.points += m;
                    self.values += v;
                }
            }
        }
    }
}

fn level_of(spec: &KeySpec, write_order: u32) -> Option<usize> {
    // Exact match on a real key column, so a path index's virtual column
    // (counting down from `u32::MAX - 1`) or any other field is unconstrained.
    spec.levels().iter().position(|l| l.write_order == write_order)
}

/// The boxes covering every row `e` can match. An allowlist: whatever it
/// does not name is one unconstrained box.
fn fold(spec: &KeySpec, e: &MaskExpr) -> Boxes {
    match e {
        MaskExpr::Pred(p) => match (p.op, level_of(spec, p.write_order)) {
            (SargOp::Eq, Some(level)) => Boxes::single(KeyBox::one(level, vec![p.value.clone()])),
            _ => Boxes::any(),
        },
        MaskExpr::And(children) => fold_and(spec, children),
        MaskExpr::Or(children) => fold_or(spec, children),
        // `All`, and any variant added later.
        _ => Boxes::any(),
    }
}

fn fold_and(spec: &KeySpec, children: &[MaskExpr]) -> Boxes {
    let mut acc = Boxes::any();
    for child in children {
        let next = fold(spec, child);
        if next.is_any() {
            continue;
        }
        if acc.is_any() {
            acc = next;
            continue;
        }
        // Capped as it is built: two 256-box operands never hold 65,536.
        let mut product = Boxes::new();
        'product: for a in &acc.set {
            for b in &next.set {
                product.push(a.meet(b));
                if product.is_any() {
                    break 'product;
                }
            }
        }
        acc = product;
    }
    acc
}

fn fold_or(spec: &KeySpec, children: &[MaskExpr]) -> Boxes {
    if let Some(b) = lowered_in(spec, children) {
        return Boxes::single(b);
    }
    let mut out = Boxes::new();
    for child in children {
        let next = fold(spec, child);
        // A union with an unconstrained branch is everything.
        if next.is_any() {
            return next;
        }
        for b in next.set {
            out.push(b);
        }
        if out.is_any() {
            return out;
        }
    }
    // An empty `Or` is no proof of anything.
    if out.set.is_empty() { Boxes::any() } else { out }
}

/// `col IN (…)` as the rewriter lowers it, `col = a OR col = b OR …` on one
/// key level, as one box holding the set: an `AND` with a second list then
/// builds one product box, not one per pair.
fn lowered_in(spec: &KeySpec, children: &[MaskExpr]) -> Option<KeyBox> {
    let mut level = None;
    let mut set = BTreeSet::new();
    for child in children {
        let MaskExpr::Pred(p) = child else {
            return None;
        };
        if p.op != SargOp::Eq {
            return None;
        }
        let l = level_of(spec, p.write_order)?;
        if *level.get_or_insert(l) != l {
            return None;
        }
        // One past the cap is enough to know it degrades; the rest are only
        // checked for shape.
        if set.len() as u64 <= POINT_CAP && set.insert(p.value.clone()) {
            note_peak(1, set.len());
        }
    }
    let level = level?;
    if set.len() as u64 > POINT_CAP {
        // Degrading drops its only level.
        return Some(KeyBox::any());
    }
    Some(KeyBox::one(level, set.into_iter().collect()))
}

/// Each box's leading constrained levels as point intervals, merged.
fn expand(spec: &KeySpec, boxes: &Boxes) -> Route {
    let mut ranges: Vec<PointRange> = Vec::new();
    for b in &boxes.set {
        // Only a contiguous prefix from level 0 narrows: a level after an
        // unconstrained one fixes none of the point's high bits. An empty set
        // is never built, and would widen here rather than prune.
        let sets: SmallVec<[&ValueSet; MAX_ARITY]> = b.0[..spec.arity()]
            .iter()
            .map_while(|s| s.as_ref().filter(|s| !s.is_empty()))
            .collect();
        if sets.is_empty() {
            return Route::All;
        }
        // An odometer over the sets' product; the measure bounds it by the cap.
        let mut idx = [0usize; MAX_ARITY];
        'odometer: loop {
            let prefix: SmallVec<[&[u8]; MAX_ARITY]> =
                sets.iter().zip(idx).map(|(s, i)| s[i].as_slice()).collect();
            ranges.push(prefix_or_wider(spec, &prefix));
            let mut l = sets.len();
            loop {
                if l == 0 {
                    break 'odometer;
                }
                l -= 1;
                idx[l] += 1;
                if idx[l] < sets[l].len() {
                    break;
                }
                idx[l] = 0;
            }
        }
    }
    ranges.sort_unstable_by_key(|r| r.lo);
    let mut merged: Vec<PointRange> = Vec::with_capacity(ranges.len());
    for r in ranges {
        match merged.last_mut() {
            Some(last) if r.lo as u128 <= last.hi => last.hi = last.hi.max(r.hi),
            _ => merged.push(r),
        }
    }
    if merged.first() == Some(&PointRange::FULL) {
        Route::All
    } else {
        Route::Points(merged)
    }
}

/// The prefix's interval, or a shorter prefix's when a value does not
/// normalise for its level (a wrong width, which coercion should have
/// prevented): wider, never an error and never nothing.
fn prefix_or_wider(spec: &KeySpec, prefix: &[&[u8]]) -> PointRange {
    (0..=prefix.len())
        .rev()
        .find_map(|n| spec.prefix_range(&prefix[..n]).ok())
        .unwrap_or(PointRange::FULL)
}

#[cfg(test)]
thread_local! {
    /// The most boxes, and the most values, any union or `IN` set held, for
    /// the bounded-memory tests.
    static PEAK: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
    /// Boxes pushed or re-inserted by a degrade: the bounded-time tests count
    /// work, since a wall clock on a loaded box is not evidence.
    static WORK: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[inline]
fn note_work(n: u64) {
    #[cfg(test)]
    WORK.with(|w| w.set(w.get() + n));
    #[cfg(not(test))]
    let _ = n;
}

#[inline]
fn note_peak(boxes: usize, values: usize) {
    #[cfg(test)]
    PEAK.with(|p| {
        let (b, v) = p.get();
        p.set((b.max(boxes), v.max(values)));
    });
    #[cfg(not(test))]
    let _ = (boxes, values);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db_type::DbType;
    use crate::core::filter::sarg::SargPredicate;
    use crate::core::partition::map::SPACE_END;

    // Columns of the test table: a key may use any of T, G, H and U in any
    // order. U shares T's type, so a value routed at the wrong level still
    // normalises there and prunes, rather than widening on a width mismatch.
    const T: u32 = 0; // I32
    const X: u32 = 1; // I32, never a key
    const G: u32 = 2; // I64
    const H: u32 = 3; // STRING
    const U: u32 = 4; // I32
    /// A path index's first virtual column, as `extract_gated_with_paths`
    /// would number it.
    const PATH: u32 = u32::MAX - 1; // STRING

    fn col_type(wo: u32) -> DbType {
        match wo {
            T | X | U => DbType::I32,
            G => DbType::I64,
            H | PATH => DbType::STRING,
            _ => unreachable!(),
        }
    }

    /// `v` as `coerce_literal` stores it at the column's type.
    fn enc(wo: u32, v: i64) -> Vec<u8> {
        match col_type(wo) {
            DbType::I32 => (v as i32).to_le_bytes().to_vec(),
            DbType::I64 => v.to_le_bytes().to_vec(),
            DbType::STRING => format!("k{v}").into_bytes(),
            _ => unreachable!(),
        }
    }

    fn spec(wos: &[u32]) -> KeySpec {
        let cols: Vec<(u32, DbType)> = wos.iter().map(|&wo| (wo, col_type(wo))).collect();
        KeySpec::new(&cols).unwrap()
    }

    fn pred(wo: u32, op: SargOp, value: Vec<u8>) -> MaskExpr {
        MaskExpr::Pred(SargPredicate {
            write_order: wo,
            op,
            value,
        })
    }

    fn eq(wo: u32, v: i64) -> MaskExpr {
        pred(wo, SargOp::Eq, enc(wo, v))
    }

    fn and(v: Vec<MaskExpr>) -> MaskExpr {
        MaskExpr::And(v)
    }

    fn or(v: Vec<MaskExpr>) -> MaskExpr {
        MaskExpr::Or(v)
    }

    fn in_list(wo: u32, vals: impl IntoIterator<Item = i64>) -> MaskExpr {
        or(vals.into_iter().map(|v| eq(wo, v)).collect())
    }

    fn r(s: &KeySpec, e: &MaskExpr) -> Route {
        route(s, Some(e), false)
    }

    /// The interval of fixing the leading key columns to `vals`.
    fn prefix(s: &KeySpec, vals: &[i64]) -> PointRange {
        let bytes: Vec<Vec<u8>> = s.levels().iter().zip(vals).map(|(l, &v)| enc(l.write_order, v)).collect();
        let refs: Vec<&[u8]> = bytes.iter().map(Vec::as_slice).collect();
        s.prefix_range(&refs).unwrap()
    }

    /// The route of exactly `want`, merged as the router merges: a point
    /// inside a block is the block.
    fn points(mut want: Vec<PointRange>) -> Route {
        want.sort_unstable_by_key(|r| r.lo);
        let mut merged: Vec<PointRange> = Vec::new();
        for r in want {
            match merged.last_mut() {
                Some(last) if r.lo as u128 <= last.hi => last.hi = last.hi.max(r.hi),
                _ => merged.push(r),
            }
        }
        Route::Points(merged)
    }

    fn reset_peak() {
        PEAK.with(|p| p.set((0, 0)));
        WORK.with(|w| w.set(0));
    }

    fn work() -> u64 {
        WORK.with(|w| w.get())
    }

    fn peak() -> (usize, usize) {
        PEAK.with(|p| p.get())
    }

    // ---- §4.2's table, row by row ----

    #[test]
    fn every_key_column_fixed_is_one_point() {
        let s = spec(&[T, G]);
        let got = r(&s, &and(vec![eq(T, 5), eq(G, 9)]));
        let p = s.point_of(&[&enc(T, 5), &enc(G, 9)]).unwrap();
        assert_eq!(got, Route::Points(vec![PointRange::point(p)]));
        // Order inside the AND does not matter.
        assert_eq!(r(&s, &and(vec![eq(G, 9), eq(T, 5)])), got);
        let map = RangeMap::initial(1024).unwrap();
        assert_eq!(got.pids(&map), vec![map.pid_of(p)]);

        let s3 = spec(&[H, T, G]);
        let got = r(&s3, &and(vec![eq(T, 1), eq(G, 2), eq(H, 3)]));
        let p = s3.point_of(&[&enc(H, 3), &enc(T, 1), &enc(G, 2)]).unwrap();
        assert_eq!(got, Route::Points(vec![PointRange::point(p)]));
    }

    #[test]
    fn a_leading_prefix_is_its_interval() {
        let s = spec(&[T, G]);
        let got = r(&s, &eq(T, 5));
        assert_eq!(got, points(vec![prefix(&s, &[5])]));
        let Route::Points(v) = &got else { unreachable!() };
        assert_eq!(v[0].hi - v[0].lo as u128, 1 << 16, "a tenant block");

        let s3 = spec(&[T, G, H]);
        let got = r(&s3, &and(vec![eq(G, 2), eq(T, 1)]));
        assert_eq!(got, points(vec![prefix(&s3, &[1, 2])]));
        let Route::Points(v) = &got else { unreachable!() };
        assert_eq!(v[0].hi - v[0].lo as u128, 1 << 8);
    }

    #[test]
    fn a_non_leading_column_alone_is_all() {
        let s = spec(&[T, G]);
        assert_eq!(r(&s, &eq(G, 9)), Route::All);
        let s3 = spec(&[T, G, H]);
        assert_eq!(r(&s3, &and(vec![eq(G, 1), eq(H, 2)])), Route::All);
        // Level 2 after an unconstrained level 1 fixes nothing.
        assert_eq!(r(&s3, &and(vec![eq(T, 1), eq(H, 2)])), points(vec![prefix(&s3, &[1])]));

        // The same with levels of one type, so a value moved to the wrong
        // level would normalise and prune instead of widening.
        assert_eq!(r(&spec(&[T, U]), &eq(U, 9)), Route::All);
        let s3 = spec(&[G, T, U]);
        assert_eq!(r(&s3, &eq(U, 2)), Route::All);
        assert_eq!(r(&s3, &and(vec![eq(G, 1), eq(U, 2)])), points(vec![prefix(&s3, &[1])]));
    }

    #[test]
    fn ranges_and_other_columns_constrain_nothing() {
        let s = spec(&[T, G]);
        for op in [SargOp::Gt, SargOp::Ge, SargOp::Lt, SargOp::Le] {
            assert_eq!(r(&s, &pred(T, op, enc(T, 5))), Route::All, "{op:?}");
            assert_eq!(r(&s, &and(vec![pred(T, op, enc(T, 5)), eq(G, 1)])), Route::All, "{op:?}");
            assert_eq!(
                r(&s, &and(vec![eq(T, 5), pred(G, op, enc(G, 1))])),
                points(vec![prefix(&s, &[5])]),
                "{op:?}"
            );
        }
        assert_eq!(r(&s, &eq(X, 5)), Route::All);
        assert_eq!(r(&s, &and(vec![eq(T, 5), eq(X, 6)])), points(vec![prefix(&s, &[5])]));
        // A path index's virtual column is not a key column.
        assert_eq!(r(&s, &eq(PATH, 5)), Route::All);
        assert_eq!(r(&spec(&[H]), &eq(PATH, 5)), Route::All);
    }

    #[test]
    fn all_and_no_program_are_all() {
        let s = spec(&[T, G]);
        assert_eq!(route(&s, None, false), Route::All);
        assert_eq!(r(&s, &MaskExpr::All), Route::All);
        assert_eq!(r(&s, &and(vec![eq(T, 5), MaskExpr::All])), points(vec![prefix(&s, &[5])]));
        assert_eq!(r(&s, &or(vec![eq(T, 5), MaskExpr::All])), Route::All);
        assert_eq!(r(&s, &and(vec![])), Route::All);
        assert_eq!(r(&s, &or(vec![])), Route::All);
    }

    #[test]
    fn a_lowered_in_is_one_box() {
        let s = spec(&[T, G]);
        let e = in_list(T, [1, 2, 3, 2]);
        let boxes = fold(&s, &e);
        assert_eq!(boxes.set.len(), 1);
        assert_eq!(boxes.values, 3, "duplicates collapse");
        assert_eq!(r(&s, &e), points(vec![prefix(&s, &[1]), prefix(&s, &[2]), prefix(&s, &[3])]));
    }

    #[test]
    fn multi_column_in_is_the_product() {
        let s = spec(&[T, G]);
        let e = and(vec![in_list(T, [1, 2]), in_list(G, [7, 8, 9])]);
        assert_eq!(fold(&s, &e).set.len(), 1, "one product box");
        let want = [1, 2]
            .iter()
            .flat_map(|&t| [7, 8, 9].map(|g| prefix(&s, &[t, g])))
            .collect();
        assert_eq!(r(&s, &e), points(want));
    }

    #[test]
    fn or_otherwise_is_the_union() {
        let s = spec(&[T, G]);
        // Across columns: one branch is unconstrained at level 0.
        assert_eq!(r(&s, &or(vec![eq(T, 1), eq(G, 2)])), Route::All);
        assert_eq!(r(&s, &or(vec![eq(T, 1), eq(X, 2)])), Route::All);
        // Two key columns of one type are still two levels, not one IN.
        let tu = spec(&[T, U]);
        assert_eq!(r(&tu, &or(vec![eq(T, 1), eq(U, 2)])), Route::All);
        assert_eq!(r(&tu, &or(vec![eq(U, 2), eq(T, 1)])), Route::All);
        assert_eq!(r(&s, &or(vec![eq(T, 1), pred(T, SargOp::Gt, enc(T, 9))])), Route::All);
        assert_eq!(
            r(&s, &or(vec![and(vec![eq(T, 1), eq(G, 2)]), and(vec![eq(T, 3), eq(G, 4)])])),
            points(vec![prefix(&s, &[1, 2]), prefix(&s, &[3, 4])])
        );
        assert_eq!(
            r(&s, &or(vec![eq(T, 1), and(vec![eq(T, 3), eq(G, 4)])])),
            points(vec![prefix(&s, &[1]), prefix(&s, &[3, 4])])
        );
        // An AND over an OR keeps the OR's narrow branches.
        assert_eq!(
            r(&s, &and(vec![or(vec![eq(T, 1), eq(G, 2)]), eq(T, 3)])),
            points(vec![prefix(&s, &[1]), prefix(&s, &[3]), prefix(&s, &[3, 2])])
        );
    }

    #[test]
    fn and_unions_a_level_never_intersects() {
        let s = spec(&[T, G]);
        // A contradiction the constant-false check does not see still routes
        // somewhere: the scan, not the router, finds it matches nothing.
        assert_eq!(
            r(&s, &and(vec![eq(T, 1), eq(T, 2)])),
            points(vec![prefix(&s, &[1]), prefix(&s, &[2])])
        );
        assert_eq!(
            r(&s, &and(vec![eq(T, 1), eq(G, 7), eq(G, 8)])),
            points(vec![prefix(&s, &[1, 7]), prefix(&s, &[1, 8])])
        );
        assert_eq!(
            r(&s, &and(vec![in_list(T, [1, 2]), in_list(T, [2, 3])])),
            points(vec![prefix(&s, &[1]), prefix(&s, &[2]), prefix(&s, &[3])])
        );
    }

    #[test]
    fn only_constant_false_routes_to_zero() {
        let s = spec(&[T, G]);
        let map = RangeMap::initial(64).unwrap();
        assert_eq!(route(&s, None, true), Route::Zero);
        assert_eq!(route(&s, Some(&eq(T, 1)), true), Route::Zero);
        assert!(Route::Zero.pids(&map).is_empty());
        assert_eq!(Route::All.pids(&map), (0..64).collect::<Vec<u32>>());
        let block = r(&s, &eq(T, 1));
        assert_eq!(block.pids(&map), vec![map.pid_of(prefix(&s, &[1]).lo)]);
    }

    /// A map with tenant `t`'s block of `s` cut into `pieces` partitions (as
    /// `SPLIT KEY (t = …) INTO pieces` leaves it), the rest in as few as fit.
    fn split_map(s: &KeySpec, tenants: impl IntoIterator<Item = i64>, pieces: u64) -> RangeMap {
        let mut bounds = BTreeSet::from([0u64]);
        for t in tenants {
            let block = prefix(s, &[t]);
            let step = (block.hi - block.lo as u128) as u64 / pieces;
            bounds.extend((0..pieces).map(|i| block.lo + i * step));
            if block.hi < SPACE_END {
                bounds.insert(block.hi as u64);
            }
        }
        RangeMap::new(bounds.into_iter().zip(0..).collect()).unwrap()
    }

    #[test]
    fn a_split_tenant_routes_to_every_piece() {
        let s = spec(&[T, G]);
        let map = split_map(&s, [1], 4);
        let block = prefix(&s, &[1]);
        let pieces: Vec<u32> = map.pids_overlapping(block.lo, block.hi);
        assert_eq!(pieces.len(), 4);

        // The tenant alone is every piece, not the one holding its start.
        assert_eq!(r(&s, &eq(T, 1)).pids(&map), pieces);
        // A full key is the one piece its point lands in.
        let mut seen = BTreeSet::new();
        for g in 0..64 {
            let p = s.point_of(&[&enc(T, 1), &enc(G, g)]).unwrap();
            let got = r(&s, &and(vec![eq(T, 1), eq(G, g)])).pids(&map);
            assert_eq!(got, vec![map.pid_of(p)], "g = {g}");
            seen.insert(got[0]);
        }
        assert_eq!(seen.len(), 4, "64 groups land in every piece");
        // Another tenant's block meets none of them.
        let other = r(&s, &eq(T, 2)).pids(&map);
        assert_eq!(other.len(), 1);
        assert!(!pieces.contains(&other[0]));
        let mut both = pieces.clone();
        both.extend(&other);
        both.sort_unstable();
        assert_eq!(r(&s, &in_list(T, [1, 2])).pids(&map), both);
    }

    // ---- the cap ----

    #[test]
    fn the_cap_degrades_from_the_right() {
        let s = spec(&[T, G]);
        // 3 tenants × 500 groups: the tenant blocks.
        let e = and(vec![in_list(T, [1, 2, 3]), in_list(G, 0..500)]);
        assert_eq!(r(&s, &e), points(vec![prefix(&s, &[1]), prefix(&s, &[2]), prefix(&s, &[3])]));
        // 2 × 200 = 400 points, past the cap: the two blocks.
        let e = and(vec![in_list(T, [1, 2]), in_list(G, 0..200)]);
        assert_eq!(r(&s, &e), points(vec![prefix(&s, &[1]), prefix(&s, &[2])]));
        // 16 × 16 = 256 is at the cap, and kept.
        let e = and(vec![in_list(T, 0..16), in_list(G, 0..16)]);
        let Route::Points(v) = r(&s, &e) else { panic!("narrowed") };
        assert_eq!(v.len(), 256);
        // Level 0 alone: 256 blocks kept, 257 is everything.
        let Route::Points(v) = r(&s, &in_list(T, 0..256)) else { panic!("narrowed") };
        assert_eq!(v.len(), 256);
        assert_eq!(r(&s, &in_list(T, 0..257)), Route::All);

        // Three levels: 2 × 10 × 20 = 400 drops level 2, leaving 20 prefixes.
        let s3 = spec(&[T, G, H]);
        let e = and(vec![in_list(T, [1, 2]), in_list(G, 0..10), in_list(H, 0..20)]);
        let want = [1, 2]
            .iter()
            .flat_map(|&t| (0..10).map(move |g| (t, g)))
            .map(|(t, g)| prefix(&s3, &[t, g]))
            .collect();
        assert_eq!(r(&s3, &e), points(want));
    }

    #[test]
    fn a_union_past_the_cap_degrades_and_stays_degraded() {
        let s = spec(&[T, G]);
        // 300 (t, g) pairs over 3 tenants, not a lowered IN: the 257th drops
        // level 1, and the pairs after it arrive already dropped.
        let e = or((0..300).map(|i| and(vec![eq(T, i % 3), eq(G, i)])).collect());
        assert_eq!(r(&s, &e), points(vec![prefix(&s, &[0]), prefix(&s, &[1]), prefix(&s, &[2])]));
        // 257 tenants, not lowered: level 0 is dropped, which is everything.
        let e = or((0..257).map(|i| and(vec![eq(T, i), eq(X, 0)])).collect());
        assert_eq!(r(&s, &e), Route::All);
    }

    #[test]
    fn values_that_do_not_normalise_widen() {
        let s = spec(&[T, G]);
        let short = pred(T, SargOp::Eq, vec![1, 2, 3]);
        assert_eq!(r(&s, &short), Route::All);
        assert_eq!(r(&s, &or(vec![eq(T, 1), short.clone()])), Route::All);
        let g_short = pred(G, SargOp::Eq, vec![9]);
        assert_eq!(r(&s, &and(vec![eq(T, 5), g_short])), points(vec![prefix(&s, &[5])]));
        let p = r(&s, &and(vec![eq(T, 5), short]));
        assert!(p.contains(prefix(&s, &[5]).lo), "still covers the valid value: {p:?}");
    }

    // ---- I3: a superset, in bounded memory ----

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }

        fn pick<T: Copy>(&mut self, of: &[T]) -> T {
            of[self.below(of.len() as u64) as usize]
        }
    }

    const COLS: [u32; 6] = [T, X, G, H, U, PATH];
    /// Row values are mostly drawn from `0..DOMAIN`, small so trees match
    /// often, and otherwise from the big lists' `0..BIG`, so rows also meet
    /// the boxes the cap degrades.
    const DOMAIN: u64 = 6;
    const BIG: u64 = 400;

    /// A union of 250 to 309 key boxes not shaped as a lowered IN, so it is
    /// built box by box through `Boxes::push`, degrade and limit. A branch is
    /// `k = v AND (X = 0 | c = small)`: one level, or two once `c` is a key.
    fn big_union(rng: &mut Rng) -> MaskExpr {
        let k = rng.pick(&COLS);
        let c = rng.pick(&COLS);
        let n = 250 + rng.below(60);
        or((0..n)
            .map(|_| {
                let second = if rng.below(2) == 0 { eq(X, 0) } else { eq(c, rng.below(DOMAIN) as i64) };
                and(vec![eq(k, rng.below(BIG) as i64), second])
            })
            .collect())
    }

    fn random_tree(rng: &mut Rng, depth: u32) -> MaskExpr {
        let k = rng.below(if depth == 0 { 11 } else { 17 });
        match k {
            0..=4 => {
                let wo = rng.pick(&COLS);
                let op = rng.pick(&[SargOp::Eq, SargOp::Eq, SargOp::Eq, SargOp::Gt, SargOp::Ge, SargOp::Lt, SargOp::Le]);
                let mut value = enc(wo, rng.below(DOMAIN + 2) as i64);
                // Now and then a width coercion would never produce.
                if op == SargOp::Eq && col_type(wo) != DbType::STRING && rng.below(25) == 0 {
                    value.pop();
                }
                pred(wo, op, value)
            }
            5 => MaskExpr::All,
            6..=8 => {
                let wo = rng.pick(&COLS);
                let (n, range) = if rng.below(6) == 0 { (250 + rng.below(60), BIG) } else { (1 + rng.below(5), DOMAIN + 2) };
                in_list(wo, (0..n).map(|_| rng.below(range) as i64))
            }
            9 if rng.below(2) == 0 => {
                let wo = rng.pick(&COLS);
                and(vec![eq(wo, rng.below(DOMAIN) as i64), eq(wo, rng.below(DOMAIN) as i64)])
            }
            // Equalities on two columns, often both keys: not a lowered IN.
            9 => or(vec![
                eq(rng.pick(&COLS), rng.below(DOMAIN) as i64),
                eq(rng.pick(&COLS), rng.below(DOMAIN) as i64),
            ]),
            // Now and then a product of two, which degrades while built.
            10 if rng.below(4) == 0 => and(vec![big_union(rng), big_union(rng)]),
            10 => big_union(rng),
            11..=13 => and((0..2 + rng.below(3)).map(|_| random_tree(rng, depth - 1)).collect()),
            _ => or((0..2 + rng.below(3)).map(|_| random_tree(rng, depth - 1)).collect()),
        }
    }

    /// Brute force. `All` stands for a clause the router could not read, so
    /// it is taken as true: with no NOT, that matches the most rows.
    fn eval(e: &MaskExpr, row: &[i64; COLS.len()]) -> bool {
        match e {
            MaskExpr::All => true,
            MaskExpr::And(v) => v.iter().all(|c| eval(c, row)),
            MaskExpr::Or(v) => v.iter().any(|c| eval(c, row)),
            MaskExpr::Pred(p) => {
                let i = COLS.iter().position(|&w| w == p.write_order).unwrap();
                let have = enc(p.write_order, row[i]);
                if p.op == SargOp::Eq {
                    return have == p.value;
                }
                let ord = match col_type(p.write_order) {
                    DbType::I32 => (row[i] as i32).cmp(&i32::from_le_bytes(p.value[..].try_into().unwrap())),
                    DbType::I64 => row[i].cmp(&i64::from_le_bytes(p.value[..].try_into().unwrap())),
                    _ => have.as_slice().cmp(&p.value),
                };
                match p.op {
                    SargOp::Gt => ord.is_gt(),
                    SargOp::Ge => ord.is_ge(),
                    SargOp::Lt => ord.is_lt(),
                    SargOp::Le => ord.is_le(),
                    SargOp::Eq => unreachable!(),
                }
            }
        }
    }

    #[test]
    fn random_trees_route_every_matching_row() {
        let map = RangeMap::initial(1024).unwrap();
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        let (mut matched, mut narrowed_and_matched, mut big_matched) = (0u64, 0u64, 0u64);
        // Same-typed levels ([T, U], [U, T], [T, U, G]), so a value moved to
        // the wrong level prunes instead of failing to normalise.
        for keys in [&[T][..], &[T, G], &[G, T], &[T, U], &[U, T], &[T, G, H], &[H, T, G], &[T, U, G]] {
            let s = spec(keys);
            // Every small tenant's block in four pieces, so a route must name
            // every partition its intervals meet, not only where they start.
            let split = split_map(&s, 0..DOMAIN as i64, 4);
            for _ in 0..600 {
                let e = random_tree(&mut rng, 3);
                let got = r(&s, &e);
                assert_ne!(got, Route::Zero);
                let pids = got.pids(&map);
                let split_pids = got.pids(&split);
                let mut hit = false;
                for _ in 0..150 {
                    let row: [i64; COLS.len()] = std::array::from_fn(|_| {
                        let range = if rng.below(4) == 0 { BIG } else { DOMAIN };
                        rng.below(range) as i64
                    });
                    if !eval(&e, &row) {
                        continue;
                    }
                    let at = |wo: u32| row[COLS.iter().position(|&w| w == wo).unwrap()];
                    let key: Vec<Vec<u8>> = keys.iter().map(|&wo| enc(wo, at(wo))).collect();
                    let refs: Vec<&[u8]> = key.iter().map(Vec::as_slice).collect();
                    let p = s.point_of(&refs).unwrap();
                    assert!(got.contains(p), "keys {keys:?}, row {row:?} matches {e:?} but routed {got:?}");
                    assert!(pids.binary_search(&map.pid_of(p)).is_ok());
                    assert!(
                        split_pids.binary_search(&split.pid_of(p)).is_ok(),
                        "keys {keys:?}, row {row:?} matches {e:?} but split pids {split_pids:?}"
                    );
                    matched += 1;
                    if got != Route::All && keys.iter().any(|&wo| at(wo) >= DOMAIN as i64) {
                        big_matched += 1;
                    }
                    hit = true;
                }
                if hit && got != Route::All {
                    narrowed_and_matched += 1;
                }
            }
        }
        // Not vacuous: many rows matched, many through a narrowed route, and
        // some through a narrowed route on a big list's value.
        assert!(matched > 20_000, "{matched}");
        assert!(narrowed_and_matched > 200, "{narrowed_and_matched}");
        assert!(big_matched > 200, "{big_matched}");
    }

    #[test]
    fn ten_thousand_value_in_lists_route_bounded() {
        let lists = |wos: &[u32], lowered: bool| -> MaskExpr {
            and(wos
                .iter()
                .map(|&wo| {
                    if lowered {
                        in_list(wo, 0..10_000)
                    } else {
                        // The same list, not shaped as a lowered IN.
                        or((0..10_000).map(|v| and(vec![eq(wo, v), eq(X, 0)])).collect())
                    }
                })
                .collect())
        };
        for (keys, wos) in [(&[T, G][..], &[T, G][..]), (&[T, G, H], &[T, G, H])] {
            let s = spec(keys);
            for lowered in [true, false] {
                reset_peak();
                let got = r(&s, &lists(wos, lowered));
                let (boxes, values) = peak();
                assert!(boxes as u64 <= POINT_CAP + 1, "{boxes} boxes");
                assert!(values as u64 <= 2 * MAX_ARITY as u64 * POINT_CAP, "{values} values");
                // Time, as box operations: a union stops at the box that
                // makes it everything, so each list costs at most its first
                // 257 branches (three pushes folding one, one into the union)
                // and a degrade per level, however long it is.
                let bound = 1 + wos.len() as u64 * (4 + MAX_ARITY as u64) * (POINT_CAP + 1);
                assert!(work() <= bound, "{} box operations, bound {bound}", work());
                // Level 0 alone is past the cap.
                assert_eq!(got, Route::All);
            }
        }
    }

    #[test]
    fn a_product_past_the_cap_is_capped_while_built() {
        // Unions of 200 and 40 boxes (not lowered, so the AND multiplies box
        // lists): the product would hold 8,000 and 64,000 boxes if capped
        // only once built.
        let list = |wo: u32, n: i64| or((0..n).map(|v| and(vec![eq(wo, v), eq(X, 0)])).collect());
        let s = spec(&[T, G]);
        reset_peak();
        let got = r(&s, &and(vec![list(T, 40), list(G, 200)]));
        let (boxes, values) = peak();
        assert!(boxes as u64 <= POINT_CAP + 1, "{boxes} boxes");
        assert!(values as u64 <= 2 * MAX_ARITY as u64 * POINT_CAP, "{values} values");
        assert_eq!(got, points((0..40).map(|t| prefix(&s, &[t])).collect()));

        let s3 = spec(&[T, G, H]);
        reset_peak();
        let got = r(&s3, &and(vec![list(T, 4), list(G, 40), list(H, 40)]));
        let (boxes, _) = peak();
        assert!(boxes as u64 <= POINT_CAP + 1, "{boxes} boxes");
        // 4 × 40 = 160 (t, g) prefixes fit once level 2 goes.
        let want = (0..4).flat_map(|t| (0..40).map(move |g| (t, g))).map(|(t, g)| prefix(&s3, &[t, g])).collect();
        assert_eq!(got, points(want));
        for (t, g, h) in [(0, 0, 0), (3, 39, 39), (2, 17, 5)] {
            let p = s3.point_of(&[&enc(T, t), &enc(G, g), &enc(H, h)]).unwrap();
            assert!(got.contains(p));
        }
    }

    #[test]
    fn a_full_space_interval_is_all() {
        // Merging is by `u128` ends, so the top of the space does not wrap.
        let s = spec(&[T]);
        let Route::Points(v) = r(&s, &in_list(T, 0..200)) else { panic!("narrowed") };
        assert!(v.iter().all(|r| r.hi <= SPACE_END && !r.is_empty()));
        assert!(v.windows(2).all(|w| (w[0].hi) < w[1].lo as u128), "sorted, disjoint, not adjacent");
    }
}
