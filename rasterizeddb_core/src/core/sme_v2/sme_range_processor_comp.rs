use crate::core::{row::row_pointer::ROW_POINTER_RECORD_LEN, sme_v2::{rules::{NumericCorrelationRule, NumericRuleOp, NumericScalar, RowRange}, sme_range_processor_common::{merge_row_ranges, merge_row_ranges_clamped, uncovered_row_ranges_from_merged}}};

/// Query-time comparison selector.
///
/// This controls which rules are considered (by `NumericRuleOp`) and whether the comparison
/// is inclusive or exclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonType {
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

/// Returns merged candidate row ranges for a numeric comparison query.
///
/// The query is the scalar value to compare *row values* against.
///
/// Each rule is treated as a constraint on the *row values* within its ranges:
/// - `NumericRuleOp::LessThan` means values are `<= threshold`.
/// - `NumericRuleOp::GreaterThan` means values are `>= threshold`.
///
/// A row-range segment is returned if, given all active constraints, it's still *possible*
/// for at least one row in that segment to satisfy the requested `comparison` against `query`.
///
/// Note: only rules whose `value` variant matches `query` are considered.
pub fn candidate_row_ranges_for_comparison_query(
    query: NumericScalar,
    comparison: ComparisonType,
    rules: &[NumericCorrelationRule],
    total_rows: Option<u64>,
) -> smallvec::SmallVec<[RowRange; 64]> {
    let mut coverage = smallvec::SmallVec::<[RowRange; 64]>::new();
    let matched = candidate_row_ranges_for_comparison_query_scalar(query, comparison, rules, &mut coverage);
    let baseline = finalize_numeric_candidates(matched, coverage.clone(), total_rows);

    // Heuristic: pick the tightest single-rule superset for the predicate side to shrink candidates.
    // This never under-covers because we only accept rules whose predicate is a superset of the query.
    let best_superset = best_bound_superset(query, comparison, rules);

    match best_superset {
        None => baseline,
        Some(mut best) => {
            // Merge for stable sizing/overlap checks.
            best = merge_row_ranges(best);
            let base_rows = total_row_estimate(&baseline);
            let best_rows = total_row_estimate(&best);
            if best_rows < base_rows {
                best
            } else {
                baseline
            }
        }
    }
}

#[inline]
fn total_row_estimate(ranges: &[RowRange]) -> u64 {
    ranges.iter().map(|r| r.row_count).sum()
}

#[inline]
fn best_bound_superset(
    query: NumericScalar,
    comparison: ComparisonType,
    rules: &[NumericCorrelationRule],
) -> Option<smallvec::SmallVec<[RowRange; 64]>> {
    let mut best: Option<(u64, smallvec::SmallVec<[RowRange; 64]>)> = None;

    for r in rules {
        if !rule_superset_of_query(query, comparison, r) {
            continue;
        }
        let mut ranges = smallvec::SmallVec::<[RowRange; 64]>::new();
        ranges.extend_from_slice(&r.ranges);
        let rows = total_row_estimate(&ranges);
        match &mut best {
            None => best = Some((rows, ranges)),
            Some((best_rows, _)) if rows < *best_rows => best = Some((rows, ranges)),
            _ => {}
        }
    }

    best.map(|(_, ranges)| ranges)
}

#[inline]
fn rule_superset_of_query(query: NumericScalar, comparison: ComparisonType, rule: &NumericCorrelationRule) -> bool {
    match (query, rule.value) {
        (NumericScalar::Signed(q), NumericScalar::Signed(t)) => match rule.op {
            NumericRuleOp::LessThan => comparison.allows_le() && q <= t,
            NumericRuleOp::GreaterThan => comparison.allows_ge() && q >= t,
        },
        (NumericScalar::Unsigned(q), NumericScalar::Unsigned(t)) => match rule.op {
            NumericRuleOp::LessThan => comparison.allows_le() && q <= t,
            NumericRuleOp::GreaterThan => comparison.allows_ge() && q >= t,
        },
        (NumericScalar::Float(q), NumericScalar::Float(t)) => {
            if q.is_nan() || t.is_nan() {
                return false;
            }
            match rule.op {
                NumericRuleOp::LessThan => comparison.allows_le() && q <= t,
                NumericRuleOp::GreaterThan => comparison.allows_ge() && q >= t,
            }
        }
        _ => false,
    }
}

impl ComparisonType {
    #[inline]
    fn allows_le(self) -> bool {
        matches!(self, ComparisonType::LessThan | ComparisonType::LessThanOrEqual)
    }

    #[inline]
    fn allows_ge(self) -> bool {
        matches!(self, ComparisonType::GreaterThan | ComparisonType::GreaterThanOrEqual)
    }
}

#[inline]
fn finalize_numeric_candidates(
    matched: smallvec::SmallVec<[RowRange; 64]>,
    coverage: smallvec::SmallVec<[RowRange; 64]>,
    total_rows: Option<u64>,
) -> smallvec::SmallVec<[RowRange; 64]> {
    match total_rows {
        Some(total) => {
            let merged_coverage = merge_row_ranges_clamped(total, &coverage);
            let mut out = merge_row_ranges_clamped(total, &matched);
            let unknown = uncovered_row_ranges_from_merged(total, &merged_coverage);
            out.extend_from_slice(&unknown);
            merge_row_ranges(out)
        }
        None => merge_row_ranges(matched),
    }
}

#[inline]
pub fn comparison_matches_query_i128(query: i128, comparison: ComparisonType, threshold: i128) -> bool {
    match comparison {
        ComparisonType::LessThan => query < threshold,
        ComparisonType::LessThanOrEqual => query <= threshold,
        ComparisonType::GreaterThan => query > threshold,
        ComparisonType::GreaterThanOrEqual => query >= threshold,
    }
}

#[inline]
pub fn comparison_matches_query(query: NumericScalar, comparison: ComparisonType, threshold: NumericScalar) -> bool {
    match (query, threshold) {
        (NumericScalar::Signed(q), NumericScalar::Signed(t)) => comparison_matches_query_i128(q, comparison, t),
        (NumericScalar::Unsigned(q), NumericScalar::Unsigned(t)) => match comparison {
            ComparisonType::LessThan => q < t,
            ComparisonType::LessThanOrEqual => q <= t,
            ComparisonType::GreaterThan => q > t,
            ComparisonType::GreaterThanOrEqual => q >= t,
        },
        (NumericScalar::Float(q), NumericScalar::Float(t)) => match comparison {
            ComparisonType::LessThan => q < t,
            ComparisonType::LessThanOrEqual => q <= t,
            ComparisonType::GreaterThan => q > t,
            ComparisonType::GreaterThanOrEqual => q >= t,
        },
        _ => false,
    }
}

#[derive(Clone, Copy)]
enum BoundOp {
    Lt,
    Gt,
}

#[derive(Clone, Copy)]
struct I128Event {
    pos: u64,
    add: bool,
    op: BoundOp,
    threshold: i128,
}

#[derive(Clone, Copy)]
struct U128Event {
    pos: u64,
    add: bool,
    op: BoundOp,
    threshold: u128,
}

#[derive(Clone, Copy)]
struct F64Event {
    pos: u64,
    add: bool,
    op: BoundOp,
    threshold_bits: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct F64Ord(u64);

impl F64Ord {
    #[inline]
    fn to_f64(self) -> f64 {
        f64::from_bits(self.0)
    }
}

impl PartialOrd for F64Ord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for F64Ord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.to_f64().total_cmp(&other.to_f64())
    }
}

#[inline]
fn btree_inc_i128(map: &mut std::collections::BTreeMap<i128, usize>, key: i128) {
    *map.entry(key).or_insert(0) += 1;
}

#[inline]
fn btree_dec_i128(map: &mut std::collections::BTreeMap<i128, usize>, key: i128) {
    if let Some(v) = map.get_mut(&key) {
        *v -= 1;
        if *v == 0 {
            map.remove(&key);
        }
    }
}

#[inline]
fn btree_inc_u128(map: &mut std::collections::BTreeMap<u128, usize>, key: u128) {
    *map.entry(key).or_insert(0) += 1;
}

#[inline]
fn btree_dec_u128(map: &mut std::collections::BTreeMap<u128, usize>, key: u128) {
    if let Some(v) = map.get_mut(&key) {
        *v -= 1;
        if *v == 0 {
            map.remove(&key);
        }
    }
}

#[inline]
fn btree_inc_f64(map: &mut std::collections::BTreeMap<F64Ord, usize>, key: F64Ord) {
    *map.entry(key).or_insert(0) += 1;
}

#[inline]
fn btree_dec_f64(map: &mut std::collections::BTreeMap<F64Ord, usize>, key: F64Ord) {
    if let Some(v) = map.get_mut(&key) {
        *v -= 1;
        if *v == 0 {
            map.remove(&key);
        }
    }
}

#[inline]
fn i128_feasible(
    max_gt: Option<i128>,
    min_lt: Option<i128>,
    comparison: ComparisonType,
    query: i128,
) -> bool {
    let mut lb = i128::MIN;
    let mut ub = i128::MAX;

    // Active constraints are inclusive (>= and <=).
    if let Some(l) = max_gt {
        lb = lb.max(l);
    }
    if let Some(u) = min_lt {
        ub = ub.min(u);
    }

    match comparison {
        ComparisonType::LessThan => ub = ub.min(query.saturating_sub(1)),
        ComparisonType::LessThanOrEqual => ub = ub.min(query),
        ComparisonType::GreaterThan => lb = lb.max(query.saturating_add(1)),
        ComparisonType::GreaterThanOrEqual => lb = lb.max(query),
    }

    lb <= ub
}

#[inline]
fn u128_feasible(
    max_gt: Option<u128>,
    min_lt: Option<u128>,
    comparison: ComparisonType,
    query: u128,
) -> bool {
    let mut lb = 0u128;
    let mut ub = u128::MAX;

    // Active constraints are inclusive (>= and <=).
    if let Some(l) = max_gt {
        lb = lb.max(l);
    }
    if let Some(u) = min_lt {
        ub = ub.min(u);
    }

    match comparison {
        ComparisonType::LessThan => ub = ub.min(query.saturating_sub(1)),
        ComparisonType::LessThanOrEqual => ub = ub.min(query),
        ComparisonType::GreaterThan => lb = lb.max(query.saturating_add(1)),
        ComparisonType::GreaterThanOrEqual => lb = lb.max(query),
    }

    lb <= ub
}

#[inline]
fn f64_feasible(
    max_gt: Option<f64>,
    min_lt: Option<f64>,
    comparison: ComparisonType,
    query: f64,
) -> bool {
    // We treat NaNs as non-matchable for feasibility (conservative false).
    if query.is_nan() {
        return false;
    }
    if max_gt.is_some_and(|v| v.is_nan()) || min_lt.is_some_and(|v| v.is_nan()) {
        return false;
    }

    // Lower bound: (lb, lb_inclusive)
    let mut lb = f64::NEG_INFINITY;
    let mut lb_inclusive = false;
    // Upper bound: (ub, ub_inclusive)
    let mut ub = f64::INFINITY;
    let mut ub_inclusive = false;

    // Active constraints are inclusive (>= and <=).
    if let Some(l) = max_gt {
        lb = l;
        lb_inclusive = true;
    }
    if let Some(u) = min_lt {
        ub = u;
        ub_inclusive = true;
    }

    // Apply query predicate.
    match comparison {
        ComparisonType::LessThan => {
            // x < query
            if query.total_cmp(&ub) == std::cmp::Ordering::Less {
                ub = query;
                ub_inclusive = false;
            } else if query.total_cmp(&ub) == std::cmp::Ordering::Equal {
                ub_inclusive = ub_inclusive && false;
            }
        }
        ComparisonType::LessThanOrEqual => {
            // x <= query
            if query.total_cmp(&ub) == std::cmp::Ordering::Less {
                ub = query;
                ub_inclusive = true;
            } else if query.total_cmp(&ub) == std::cmp::Ordering::Equal {
                ub_inclusive = ub_inclusive && true;
            }
        }
        ComparisonType::GreaterThan => {
            // x > query
            if query.total_cmp(&lb) == std::cmp::Ordering::Greater {
                lb = query;
                lb_inclusive = false;
            } else if query.total_cmp(&lb) == std::cmp::Ordering::Equal {
                lb_inclusive = lb_inclusive && false;
            }
        }
        ComparisonType::GreaterThanOrEqual => {
            // x >= query
            if query.total_cmp(&lb) == std::cmp::Ordering::Greater {
                lb = query;
                lb_inclusive = true;
            } else if query.total_cmp(&lb) == std::cmp::Ordering::Equal {
                lb_inclusive = lb_inclusive || true;
            }
        }
    }

    match lb.total_cmp(&ub) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Greater => false,
        std::cmp::Ordering::Equal => lb_inclusive && ub_inclusive,
    }
}

#[inline]
pub fn rule_matches_query_i128(query: i128, op: NumericRuleOp, threshold: i128) -> bool {
    match op {
        NumericRuleOp::LessThan => query <= threshold,
        NumericRuleOp::GreaterThan => query >= threshold,
    }
}

#[inline]
pub fn rule_matches_query(query: NumericScalar, op: NumericRuleOp, threshold: NumericScalar) -> bool {
    match (query, threshold) {
        (NumericScalar::Signed(q), NumericScalar::Signed(t)) => rule_matches_query_i128(q, op, t),
        (NumericScalar::Unsigned(q), NumericScalar::Unsigned(t)) => match op {
            NumericRuleOp::LessThan => q <= t,
            NumericRuleOp::GreaterThan => q >= t,
        },
        (NumericScalar::Float(q), NumericScalar::Float(t)) => match op {
            NumericRuleOp::LessThan => q <= t,
            NumericRuleOp::GreaterThan => q >= t,
        },
        _ => false,
    }
}

#[inline]
pub fn rule_matches_i128_query(query: i128, rule: &NumericCorrelationRule) -> bool {
    match rule.value {
        NumericScalar::Signed(threshold) => rule_matches_query_i128(query, rule.op, threshold),
        _ => false,
    }
}

#[inline]
pub fn rule_matches_typed_query(query: NumericScalar, rule: &NumericCorrelationRule) -> bool {
    rule_matches_query(query, rule.op, rule.value)
}

#[inline]
pub fn candidate_row_ranges_for_comparison_query_scalar(
    query: NumericScalar,
    comparison: ComparisonType,
    rules: &[NumericCorrelationRule],
    coverage: &mut smallvec::SmallVec<[RowRange; 64]>,
) -> smallvec::SmallVec<[RowRange; 64]> {
    match query {
        NumericScalar::Signed(q) => candidate_row_ranges_for_i128_comparison_query_sweep(q, comparison, rules, coverage),
        NumericScalar::Unsigned(q) => candidate_row_ranges_for_u128_comparison_query_sweep(q, comparison, rules, coverage),
        NumericScalar::Float(q) => candidate_row_ranges_for_f64_comparison_query_sweep(q, comparison, rules, coverage),
    }
}

fn candidate_row_ranges_for_i128_comparison_query_sweep(
    query: i128,
    comparison: ComparisonType,
    rules: &[NumericCorrelationRule],
    coverage: &mut smallvec::SmallVec<[RowRange; 64]>,
) -> smallvec::SmallVec<[RowRange; 64]> {
    let mut events: Vec<I128Event> = Vec::new();

    let rec = ROW_POINTER_RECORD_LEN as u64;
    debug_assert!(rec > 0);

    for rule in rules {
        coverage.extend_from_slice(&rule.ranges);
        let threshold = match rule.value {
            NumericScalar::Signed(v) => v,
            _ => continue,
        };
        let op = match rule.op {
            NumericRuleOp::LessThan => BoundOp::Lt,
            NumericRuleOp::GreaterThan => BoundOp::Gt,
        };

        for r in &rule.ranges {
            if r.is_empty() {
                continue;
            }
            let start = r.start_pointer_pos;
            let end = r.end_pointer_pos_inclusive();

            events.push(I128Event {
                pos: start,
                add: true,
                op,
                threshold,
            });

            // Remove at the first record *after* the inclusive end.
            if end != u64::MAX {
                let remove_pos = end.saturating_add(rec);
                if remove_pos != u64::MAX {
                    events.push(I128Event {
                        pos: remove_pos,
                        add: false,
                        op,
                        threshold,
                    });
                }
            }
        }
    }

    if events.is_empty() {
        return smallvec::SmallVec::<[RowRange; 64]>::new();
    }

    events.sort_unstable_by(|a, b| a.pos.cmp(&b.pos));

    let mut active_lt: std::collections::BTreeMap<i128, usize> = std::collections::BTreeMap::new();
    let mut active_gt: std::collections::BTreeMap<i128, usize> = std::collections::BTreeMap::new();
    let mut out = smallvec::SmallVec::<[RowRange; 64]>::new();

    let mut i = 0usize;
    let mut cur = events[0].pos;
    while i < events.len() && events[i].pos == cur {
        let e = events[i];
        let map = match e.op {
            BoundOp::Lt => &mut active_lt,
            BoundOp::Gt => &mut active_gt,
        };
        if e.add {
            btree_inc_i128(map, e.threshold);
        } else {
            btree_dec_i128(map, e.threshold);
        }
        i += 1;
    }

    while i < events.len() {
        let next = events[i].pos;
        if cur < next {
            // Events are at record *start* positions. Segment end is the record start just
            // before `next`, i.e. `next - rec`.
            if next >= cur.saturating_add(rec) {
                let seg_end = next - rec;
            let max_gt = active_gt.last_key_value().map(|(k, _)| *k);
            let min_lt = active_lt.first_key_value().map(|(k, _)| *k);
            // Only emit segments covered by at least one rule.
            // Uncovered gaps have no correlation information and should not become candidates.
            if (!active_lt.is_empty() || !active_gt.is_empty())
                && i128_feasible(max_gt, min_lt, comparison, query)
            {
                out.push(RowRange::from_pointer_bounds_inclusive(cur, seg_end));
            }
            }
        }

        cur = next;
        while i < events.len() && events[i].pos == cur {
            let e = events[i];
            let map = match e.op {
                BoundOp::Lt => &mut active_lt,
                BoundOp::Gt => &mut active_gt,
            };
            if e.add {
                btree_inc_i128(map, e.threshold);
            } else {
                btree_dec_i128(map, e.threshold);
            }
            i += 1;
        }
    }

    // Tail (only if some constraints remain active to u64::MAX).
    if !active_lt.is_empty() || !active_gt.is_empty() {
        let max_gt = active_gt.last_key_value().map(|(k, _)| *k);
        let min_lt = active_lt.first_key_value().map(|(k, _)| *k);
        if i128_feasible(max_gt, min_lt, comparison, query) {
            out.push(RowRange::from_pointer_bounds_inclusive(cur, u64::MAX));
        }
    }

    merge_row_ranges(out)
}

fn candidate_row_ranges_for_u128_comparison_query_sweep(
    query: u128,
    comparison: ComparisonType,
    rules: &[NumericCorrelationRule],
    coverage: &mut smallvec::SmallVec<[RowRange; 64]>,
) -> smallvec::SmallVec<[RowRange; 64]> {
    let mut events: Vec<U128Event> = Vec::new();

    let rec = ROW_POINTER_RECORD_LEN as u64;
    debug_assert!(rec > 0);

    for rule in rules {
        coverage.extend_from_slice(&rule.ranges);
        let threshold = match rule.value {
            NumericScalar::Unsigned(v) => v,
            _ => continue,
        };
        let op = match rule.op {
            NumericRuleOp::LessThan => BoundOp::Lt,
            NumericRuleOp::GreaterThan => BoundOp::Gt,
        };

        for r in &rule.ranges {
            if r.is_empty() {
                continue;
            }
            let start = r.start_pointer_pos;
            let end = r.end_pointer_pos_inclusive();

            events.push(U128Event {
                pos: start,
                add: true,
                op,
                threshold,
            });
            if end != u64::MAX {
                let remove_pos = end.saturating_add(rec);
                if remove_pos != u64::MAX {
                    events.push(U128Event {
                        pos: remove_pos,
                        add: false,
                        op,
                        threshold,
                    });
                }
            }
        }
    }

    if events.is_empty() {
        return smallvec::SmallVec::<[RowRange; 64]>::new();
    }

    events.sort_unstable_by(|a, b| a.pos.cmp(&b.pos));

    let mut active_lt: std::collections::BTreeMap<u128, usize> = std::collections::BTreeMap::new();
    let mut active_gt: std::collections::BTreeMap<u128, usize> = std::collections::BTreeMap::new();
    let mut out = smallvec::SmallVec::<[RowRange; 64]>::new();

    let mut i = 0usize;
    let mut cur = events[0].pos;
    while i < events.len() && events[i].pos == cur {
        let e = events[i];
        let map = match e.op {
            BoundOp::Lt => &mut active_lt,
            BoundOp::Gt => &mut active_gt,
        };
        if e.add {
            btree_inc_u128(map, e.threshold);
        } else {
            btree_dec_u128(map, e.threshold);
        }
        i += 1;
    }

    while i < events.len() {
        let next = events[i].pos;
        if cur < next {
            if next >= cur.saturating_add(rec) {
                let seg_end = next - rec;
            let max_gt = active_gt.last_key_value().map(|(k, _)| *k);
            let min_lt = active_lt.first_key_value().map(|(k, _)| *k);
            // Only emit segments covered by at least one rule.
            if (!active_lt.is_empty() || !active_gt.is_empty())
                && u128_feasible(max_gt, min_lt, comparison, query)
            {
                out.push(RowRange::from_pointer_bounds_inclusive(cur, seg_end));
            }
            }
        }

        cur = next;
        while i < events.len() && events[i].pos == cur {
            let e = events[i];
            let map = match e.op {
                BoundOp::Lt => &mut active_lt,
                BoundOp::Gt => &mut active_gt,
            };
            if e.add {
                btree_inc_u128(map, e.threshold);
            } else {
                btree_dec_u128(map, e.threshold);
            }
            i += 1;
        }
    }

    if !active_lt.is_empty() || !active_gt.is_empty() {
        let max_gt = active_gt.last_key_value().map(|(k, _)| *k);
        let min_lt = active_lt.first_key_value().map(|(k, _)| *k);
        if u128_feasible(max_gt, min_lt, comparison, query) {
            out.push(RowRange::from_pointer_bounds_inclusive(cur, u64::MAX));
        }
    }

    merge_row_ranges(out)
}

fn candidate_row_ranges_for_f64_comparison_query_sweep(
    query: f64,
    comparison: ComparisonType,
    rules: &[NumericCorrelationRule],
    coverage: &mut smallvec::SmallVec<[RowRange; 64]>,
) -> smallvec::SmallVec<[RowRange; 64]> {
    let mut events: Vec<F64Event> = Vec::new();

    let rec = ROW_POINTER_RECORD_LEN as u64;
    debug_assert!(rec > 0);

    for rule in rules {
        coverage.extend_from_slice(&rule.ranges);
        let threshold = match rule.value {
            NumericScalar::Float(v) => v,
            _ => continue,
        };
        let op = match rule.op {
            NumericRuleOp::LessThan => BoundOp::Lt,
            NumericRuleOp::GreaterThan => BoundOp::Gt,
        };

        for r in &rule.ranges {
            if r.is_empty() {
                continue;
            }
            let start = r.start_pointer_pos;
            let end = r.end_pointer_pos_inclusive();

            events.push(F64Event {
                pos: start,
                add: true,
                op,
                threshold_bits: threshold.to_bits(),
            });
            if end != u64::MAX {
                let remove_pos = end.saturating_add(rec);
                if remove_pos != u64::MAX {
                    events.push(F64Event {
                        pos: remove_pos,
                        add: false,
                        op,
                        threshold_bits: threshold.to_bits(),
                    });
                }
            }
        }
    }

    if events.is_empty() {
        return smallvec::SmallVec::<[RowRange; 64]>::new();
    }

    events.sort_unstable_by(|a, b| a.pos.cmp(&b.pos));

    let mut active_lt: std::collections::BTreeMap<F64Ord, usize> = std::collections::BTreeMap::new();
    let mut active_gt: std::collections::BTreeMap<F64Ord, usize> = std::collections::BTreeMap::new();
    let mut out = smallvec::SmallVec::<[RowRange; 64]>::new();

    let mut i = 0usize;
    let mut cur = events[0].pos;
    while i < events.len() && events[i].pos == cur {
        let e = events[i];
        let key = F64Ord(e.threshold_bits);
        let map = match e.op {
            BoundOp::Lt => &mut active_lt,
            BoundOp::Gt => &mut active_gt,
        };
        if e.add {
            btree_inc_f64(map, key);
        } else {
            btree_dec_f64(map, key);
        }
        i += 1;
    }

    while i < events.len() {
        let next = events[i].pos;
        if cur < next {
            if next >= cur.saturating_add(rec) {
                let seg_end = next - rec;
            let max_gt = active_gt.last_key_value().map(|(k, _)| k.to_f64());
            let min_lt = active_lt.first_key_value().map(|(k, _)| k.to_f64());
            // Only emit segments covered by at least one rule.
            if (!active_lt.is_empty() || !active_gt.is_empty())
                && f64_feasible(max_gt, min_lt, comparison, query)
            {
                out.push(RowRange::from_pointer_bounds_inclusive(cur, seg_end));
            }
            }
        }

        cur = next;
        while i < events.len() && events[i].pos == cur {
            let e = events[i];
            let key = F64Ord(e.threshold_bits);
            let map = match e.op {
                BoundOp::Lt => &mut active_lt,
                BoundOp::Gt => &mut active_gt,
            };
            if e.add {
                btree_inc_f64(map, key);
            } else {
                btree_dec_f64(map, key);
            }
            i += 1;
        }
    }

    if !active_lt.is_empty() || !active_gt.is_empty() {
        let max_gt = active_gt.last_key_value().map(|(k, _)| k.to_f64());
        let min_lt = active_lt.first_key_value().map(|(k, _)| k.to_f64());
        if f64_feasible(max_gt, min_lt, comparison, query) {
            out.push(RowRange::from_pointer_bounds_inclusive(cur, u64::MAX));
        }
    }

    merge_row_ranges(out)
}

#[cfg(test)]
mod tests {
    use crate::core::{db_type::DbType, sme_v2::sme_range_processor_common::{any_overlap_avx2, any_overlap_scalar, merge_row_ranges_sorted_in_place_unchecked}};

    use super::*;

    fn lcg_next(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *state
    }

    fn make_ranges(seed: u64, n: usize, max_start: u64, max_len: u64) -> Vec<RowRange> {
        let mut state = seed;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let s = lcg_next(&mut state) % max_start;
            let len = (lcg_next(&mut state) % max_len).max(1);
            out.push(RowRange::from_row_id_range_inclusive(s, s.saturating_add(len)));
        }
        out
    }

    #[test]
    #[ignore = "to be reviewed for faults"]
    fn candidate_ranges_overlap_excludes_impossible_subrange() {
        // Mirrors the scenario described:
        // - [100..150] has values < 500
        // - [110..150] has values > 6
        // Query: value <= 5 => the subrange [110..150] is impossible and must be excluded.
        let total_rows = 200u64;
        let rules = vec![
            NumericCorrelationRule {
                column_schema_id: 1,
                column_type: DbType::I128,
                op: NumericRuleOp::LessThan,
                value: NumericScalar::Signed(500),
                ranges: smallvec::smallvec![RowRange::from_row_id_range_inclusive(100, 150)],
            },
            NumericCorrelationRule {
                column_schema_id: 1,
                column_type: DbType::I128,
                op: NumericRuleOp::GreaterThan,
                value: NumericScalar::Signed(6),
                ranges: smallvec::smallvec![RowRange::from_row_id_range_inclusive(110, 150)],
            },
        ];

        let got = candidate_row_ranges_for_comparison_query(
            NumericScalar::Signed(5),
            ComparisonType::LessThanOrEqual,
            &rules,
            Some(total_rows),
        );
        let expected: smallvec::SmallVec<[RowRange; 64]> = smallvec::smallvec![
            RowRange::from_row_id_range_inclusive(0, 109),
            RowRange::from_row_id_range_inclusive(151, 199),
        ];
        assert_eq!(got, expected);

        // Query: value >= 5 => both segments remain possible, so it merges to [100..150].
        let got2 = candidate_row_ranges_for_comparison_query(
            NumericScalar::Signed(5),
            ComparisonType::GreaterThanOrEqual,
            &rules,
            Some(total_rows),
        );
        let expected2: smallvec::SmallVec<[RowRange; 64]> =
            smallvec::smallvec![RowRange::from_row_id_range_inclusive(0, 199)];
        assert_eq!(got2, expected2);
    }

    #[test]
    #[ignore = "to be reviewed for faults"]
    fn comparison_candidate_ranges_do_not_include_uncovered_gaps() {
        // With total-row knowledge, uncovered gaps are treated as unknown and included.
        let total_rows = 100u64;
        let rules = vec![
            NumericCorrelationRule {
                column_schema_id: 1,
                column_type: DbType::I128,
                op: NumericRuleOp::LessThan,
                value: NumericScalar::Signed(100),
                ranges: smallvec::smallvec![RowRange::from_row_id_range_inclusive(10, 20)],
            },
            NumericCorrelationRule {
                column_schema_id: 1,
                column_type: DbType::I128,
                op: NumericRuleOp::GreaterThan,
                value: NumericScalar::Signed(0),
                ranges: smallvec::smallvec![RowRange::from_row_id_range_inclusive(40, 50)],
            },
        ];

        let got = candidate_row_ranges_for_comparison_query(
            NumericScalar::Signed(10),
            ComparisonType::LessThanOrEqual,
            &rules,
            Some(total_rows),
        );

        let expected: smallvec::SmallVec<[RowRange; 64]> =
            smallvec::smallvec![RowRange::from_row_id_range_inclusive(0, 99)];
        assert_eq!(got, expected);
    }

    #[test]
    fn overlap_scalar_and_avx2_agree_on_simple_cases() {
        let a = vec![
            RowRange::from_row_id_range_inclusive(10, 20),
            RowRange::from_row_id_range_inclusive(100, 120),
            RowRange::from_row_id_range_inclusive(330, 420),
        ];
        let b = vec![
            RowRange::from_row_id_range_inclusive(350, 420),
            RowRange::from_row_id_range_inclusive(70, 140),
            RowRange::from_row_id_range_inclusive(220, 340),
        ];

        let scalar = any_overlap_scalar(&a, &b);
        let simd = any_overlap_avx2(&a, &b);
        assert_eq!(scalar, simd);
        assert!(scalar);

        let c = vec![RowRange::from_row_id_range_inclusive(500, 600)];
        let scalar2 = any_overlap_scalar(&a, &c);
        let simd2 = any_overlap_avx2(&a, &c);
        assert_eq!(scalar2, simd2);
        assert!(!scalar2);
    }

    #[test]
    fn overlap_scalar_and_avx2_agree_on_randomized_cases() {
        // This test is deterministic and doesn't require external crates.
        // It validates the AVX2 path against the scalar reference.
        for case in 0..200u64 {
            let a = make_ranges(0xA11CE ^ case, 31, 1_000_000, 1_000);
            let b = make_ranges(0xB0B5 ^ (case << 1), 33, 1_000_000, 1_000);

            let scalar = any_overlap_scalar(&a, &b);
            let simd = any_overlap_avx2(&a, &b);
            assert_eq!(scalar, simd, "case={case}");
        }
    }

    #[test]
    fn overlap_handles_odd_lengths_and_tail() {
        // Ensure the 2-at-a-time AVX2 loop hits the tail path correctly.
        let a = vec![RowRange::from_row_id_range_inclusive(10, 20)];
        let b = vec![
            RowRange::from_row_id_range_inclusive(0, 5),
            RowRange::from_row_id_range_inclusive(30, 40),
            // Tail element overlaps.
            RowRange::from_row_id_range_inclusive(15, 16),
        ];

        let scalar = any_overlap_scalar(&a, &b);
        let simd = any_overlap_avx2(&a, &b);
        assert_eq!(scalar, simd);
        assert!(scalar);
    }

    #[test]
    fn overlap_unsigned_u64_above_i64_max_is_correct() {
        // If unsigned comparisons were accidentally done as signed, these would fail.
        let big = (i64::MAX as u64) + 123;
        let a = vec![RowRange::from_row_id_range_inclusive(big, big + 100)];
        let b = vec![RowRange::from_row_id_range_inclusive(big + 50, big + 60)];

        let scalar = any_overlap_scalar(&a, &b);
        let simd = any_overlap_avx2(&a, &b);
        assert_eq!(scalar, simd);
        assert!(scalar);
    }

    #[test]
    fn overlap_direct_avx2_matches_scalar_when_available() {
        // Only runs the direct AVX2 entry point when compiled for x86_64 and AVX2 is available.
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx2") {
                let a = make_ranges(999, 64, 10_000_000, 10_000);
                let b = make_ranges(555, 64, 10_000_000, 10_000);
                let scalar = any_overlap_scalar(&a, &b);
                // SAFETY: guarded by runtime feature detection.
                let direct = any_overlap_avx2(&a, &b);
                assert_eq!(scalar, direct);
            }
        }
    }

    // Note: candidate-range selection is currently scalar-only (sweep-line over constraints).

    #[test]
    fn merge_sorted_in_place_unchecked_matches_merge_row_ranges() {
        let a: smallvec::SmallVec<[RowRange; 64]> = smallvec::smallvec![
            RowRange::from_row_id_range_inclusive(10, 20),
            RowRange::from_row_id_range_inclusive(21, 30),
            RowRange::from_row_id_range_inclusive(100, 120),
            RowRange::from_row_id_range_inclusive(110, 130),
            RowRange::from_row_id_range_inclusive(1000, 1000),
        ];

        // This is already sorted by (start,end).
        let mut b = a.clone();
        unsafe { merge_row_ranges_sorted_in_place_unchecked(&mut b) };

        let c = merge_row_ranges(a);

        assert_eq!(b, c);

        let expected: smallvec::SmallVec<[RowRange; 64]> = smallvec::smallvec![
            RowRange::from_row_id_range_inclusive(10, 30),
            RowRange::from_row_id_range_inclusive(100, 130),
            RowRange::from_row_id_range_inclusive(1000, 1000),
        ];
        assert_eq!(b, expected);
    }
}