use crate::core::sme_v2::rules::{NumericCorrelationRule, NumericRuleOp, NumericScalar, RowRange};

/// Returns merged candidate row ranges for a numeric query.
///
/// The query can be one of:
/// - `NumericScalar::Signed(i128)`
/// - `NumericScalar::Unsigned(u128)`
/// - `NumericScalar::Float(f64)`
///
/// Interpretation follows `NumericRuleOp` docs: this is a predicate on the query value.
///
/// Note: rules are only considered when their `value` variant matches the query variant.
pub fn candidate_row_ranges_for_query(
    query: NumericScalar,
    rules: &[NumericCorrelationRule],
) -> smallvec::SmallVec<[RowRange; 64]> {
    // For small rule counts, scalar is typically fastest.
    if rules.len() < 16 {
        return candidate_row_ranges_for_query_scalar(query, rules);
    }

    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            match query {
                NumericScalar::Signed(q) => {
                    // SAFETY: guarded by runtime feature detection.
                    return unsafe { candidate_row_ranges_for_i128_query_avx2(q, rules) };
                }
                NumericScalar::Unsigned(q) => {
                    // SAFETY: guarded by runtime feature detection.
                    return unsafe { candidate_row_ranges_for_u128_query_avx2(q, rules) };
                }
                NumericScalar::Float(_) => {}
            }
        }
    }

    candidate_row_ranges_for_query_scalar(query, rules)
}

#[inline]
pub fn rule_matches_query_i128(query: i128, op: NumericRuleOp, threshold: i128) -> bool {
    match op {
        NumericRuleOp::LessThan => query < threshold,
        NumericRuleOp::GreaterThan => query > threshold,
    }
}

#[inline]
pub fn rule_matches_query(query: NumericScalar, op: NumericRuleOp, threshold: NumericScalar) -> bool {
    match (query, threshold) {
        (NumericScalar::Signed(q), NumericScalar::Signed(t)) => rule_matches_query_i128(q, op, t),
        (NumericScalar::Unsigned(q), NumericScalar::Unsigned(t)) => match op {
            NumericRuleOp::LessThan => q < t,
            NumericRuleOp::GreaterThan => q > t,
        },
        (NumericScalar::Float(q), NumericScalar::Float(t)) => match op {
            NumericRuleOp::LessThan => q < t,
            NumericRuleOp::GreaterThan => q > t,
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
pub fn candidate_row_ranges_for_query_scalar(
    query: NumericScalar,
    rules: &[NumericCorrelationRule],
) -> smallvec::SmallVec<[RowRange; 64]> {
    let mut out = smallvec::SmallVec::<[RowRange; 64]>::new();
    for rule in rules {
        if rule_matches_typed_query(query, rule) {
            out.extend_from_slice(&rule.ranges);
        }
    }
    merge_row_ranges(out)
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn avx2_lane_any(mask: i32, lane: usize) -> bool {
    ((mask >> (lane * 8)) & 0xFF) != 0
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_cmpgt_u64(a: std::arch::x86_64::__m256i, b: std::arch::x86_64::__m256i) -> std::arch::x86_64::__m256i {
    use std::arch::x86_64::*;
    let sign = _mm256_set1_epi64x(i64::MIN);
    let ax = _mm256_xor_si256(a, sign);
    let bx = _mm256_xor_si256(b, sign);
    _mm256_cmpgt_epi64(ax, bx)
}

/// Unsigned u128 compare (a > b) where each u128 is represented as (hi unsigned u64, lo unsigned u64)
/// in 64-bit lanes.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_u128_gt_masks(
    a_hi: std::arch::x86_64::__m256i,
    a_lo: std::arch::x86_64::__m256i,
    b_hi: std::arch::x86_64::__m256i,
    b_lo: std::arch::x86_64::__m256i,
) -> std::arch::x86_64::__m256i {
    use std::arch::x86_64::*;

    let hi_gt = unsafe { avx2_cmpgt_u64(a_hi, b_hi) };
    let hi_lt = unsafe { avx2_cmpgt_u64(b_hi, a_hi) };
    let hi_neq = _mm256_or_si256(hi_gt, hi_lt);
    let ones = _mm256_set1_epi64x(-1);
    let hi_eq = _mm256_andnot_si256(hi_neq, ones);
    let lo_gt = unsafe { avx2_cmpgt_u64(a_lo, b_lo) };
    let hi_eq_and_lo_gt = _mm256_and_si256(hi_eq, lo_gt);
    _mm256_or_si256(hi_gt, hi_eq_and_lo_gt)
}

/// Signed i128 compare (a > b) where each i128 is represented as (hi signed i64, lo unsigned u64)
/// in 64-bit lanes.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_i128_gt_masks(
    a_hi: std::arch::x86_64::__m256i,
    a_lo: std::arch::x86_64::__m256i,
    b_hi: std::arch::x86_64::__m256i,
    b_lo: std::arch::x86_64::__m256i,
) -> std::arch::x86_64::__m256i {
    use std::arch::x86_64::*;
    let hi_gt = _mm256_cmpgt_epi64(a_hi, b_hi);
    let hi_lt = _mm256_cmpgt_epi64(b_hi, a_hi);
    let hi_neq = _mm256_or_si256(hi_gt, hi_lt);
    let ones = _mm256_set1_epi64x(-1);
    let hi_eq = _mm256_andnot_si256(hi_neq, ones);
    let lo_gt = unsafe { avx2_cmpgt_u64(a_lo, b_lo) };
    let hi_eq_and_lo_gt = _mm256_and_si256(hi_eq, lo_gt);
    _mm256_or_si256(hi_gt, hi_eq_and_lo_gt)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn candidate_row_ranges_for_i128_query_avx2(
    query: i128,
    rules: &[NumericCorrelationRule],
) -> smallvec::SmallVec<[RowRange; 64]> {
    use std::arch::x86_64::*;

    // Only accelerates Signed(i128) rules; others fall back to scalar per rule.
    // Split indices by op so each SIMD loop does one comparison direction.
    let mut lt_idx = smallvec::SmallVec::<[usize; 64]>::new();
    let mut gt_idx = smallvec::SmallVec::<[usize; 64]>::new();
    let mut scalar_fallback = smallvec::SmallVec::<[usize; 64]>::new();

    for (i, r) in rules.iter().enumerate() {
        match (r.op, r.value) {
            (NumericRuleOp::LessThan, NumericScalar::Signed(_)) => lt_idx.push(i),
            (NumericRuleOp::GreaterThan, NumericScalar::Signed(_)) => gt_idx.push(i),
            _ => scalar_fallback.push(i),
        }
    }

    let mut out = smallvec::SmallVec::<[RowRange; 64]>::new();

    // Apply scalar fallback rules (non-i128-signed, if any).
    for &i in &scalar_fallback {
        let rule = &rules[i];
        if rule_matches_typed_query(NumericScalar::Signed(query), rule) {
            out.extend_from_slice(&rule.ranges);
        }
    }

    // Decompose query into (hi, lo) words.
    let qb = query.to_le_bytes();
    let q_lo = u64::from_le_bytes(qb[0..8].try_into().unwrap());
    let q_hi_u64 = u64::from_le_bytes(qb[8..16].try_into().unwrap());
    let q_hi = q_hi_u64 as i64; // signed high word

    let q_hi_vec = _mm256_set1_epi64x(q_hi);
    let q_lo_vec = _mm256_set1_epi64x(q_lo as i64);

    // LessThan op: match if query < threshold  <=>  threshold > query
    {
        let mut k = 0usize;
        while k + 4 <= lt_idx.len() {
            let i0 = lt_idx[k];
            let i1 = lt_idx[k + 1];
            let i2 = lt_idx[k + 2];
            let i3 = lt_idx[k + 3];

            let t0 = match rules[i0].value {
                NumericScalar::Signed(v) => v,
                _ => unreachable!(),
            };
            let t1 = match rules[i1].value {
                NumericScalar::Signed(v) => v,
                _ => unreachable!(),
            };
            let t2 = match rules[i2].value {
                NumericScalar::Signed(v) => v,
                _ => unreachable!(),
            };
            let t3 = match rules[i3].value {
                NumericScalar::Signed(v) => v,
                _ => unreachable!(),
            };

            let b0 = t0.to_le_bytes();
            let b1 = t1.to_le_bytes();
            let b2 = t2.to_le_bytes();
            let b3 = t3.to_le_bytes();

            let lo0 = u64::from_le_bytes(b0[0..8].try_into().unwrap());
            let hi0 = u64::from_le_bytes(b0[8..16].try_into().unwrap()) as i64;
            let lo1 = u64::from_le_bytes(b1[0..8].try_into().unwrap());
            let hi1 = u64::from_le_bytes(b1[8..16].try_into().unwrap()) as i64;
            let lo2 = u64::from_le_bytes(b2[0..8].try_into().unwrap());
            let hi2 = u64::from_le_bytes(b2[8..16].try_into().unwrap()) as i64;
            let lo3 = u64::from_le_bytes(b3[0..8].try_into().unwrap());
            let hi3 = u64::from_le_bytes(b3[8..16].try_into().unwrap()) as i64;

            let t_hi = _mm256_setr_epi64x(hi0, hi1, hi2, hi3);
            let t_lo = _mm256_setr_epi64x(lo0 as i64, lo1 as i64, lo2 as i64, lo3 as i64);

            let gt = unsafe { avx2_i128_gt_masks(t_hi, t_lo, q_hi_vec, q_lo_vec) };
            let m = _mm256_movemask_epi8(gt);

            if avx2_lane_any(m, 0) {
                out.extend_from_slice(&rules[i0].ranges);
            }
            if avx2_lane_any(m, 1) {
                out.extend_from_slice(&rules[i1].ranges);
            }
            if avx2_lane_any(m, 2) {
                out.extend_from_slice(&rules[i2].ranges);
            }
            if avx2_lane_any(m, 3) {
                out.extend_from_slice(&rules[i3].ranges);
            }

            k += 4;
        }
        // Tail
        for &i in &lt_idx[k..] {
            let rule = &rules[i];
            if rule_matches_typed_query(NumericScalar::Signed(query), rule) {
                out.extend_from_slice(&rule.ranges);
            }
        }
    }

    // GreaterThan op: match if query > threshold
    {
        let mut k = 0usize;
        while k + 4 <= gt_idx.len() {
            let i0 = gt_idx[k];
            let i1 = gt_idx[k + 1];
            let i2 = gt_idx[k + 2];
            let i3 = gt_idx[k + 3];

            let t0 = match rules[i0].value {
                NumericScalar::Signed(v) => v,
                _ => unreachable!(),
            };
            let t1 = match rules[i1].value {
                NumericScalar::Signed(v) => v,
                _ => unreachable!(),
            };
            let t2 = match rules[i2].value {
                NumericScalar::Signed(v) => v,
                _ => unreachable!(),
            };
            let t3 = match rules[i3].value {
                NumericScalar::Signed(v) => v,
                _ => unreachable!(),
            };

            let b0 = t0.to_le_bytes();
            let b1 = t1.to_le_bytes();
            let b2 = t2.to_le_bytes();
            let b3 = t3.to_le_bytes();

            let lo0 = u64::from_le_bytes(b0[0..8].try_into().unwrap());
            let hi0 = u64::from_le_bytes(b0[8..16].try_into().unwrap()) as i64;
            let lo1 = u64::from_le_bytes(b1[0..8].try_into().unwrap());
            let hi1 = u64::from_le_bytes(b1[8..16].try_into().unwrap()) as i64;
            let lo2 = u64::from_le_bytes(b2[0..8].try_into().unwrap());
            let hi2 = u64::from_le_bytes(b2[8..16].try_into().unwrap()) as i64;
            let lo3 = u64::from_le_bytes(b3[0..8].try_into().unwrap());
            let hi3 = u64::from_le_bytes(b3[8..16].try_into().unwrap()) as i64;

            let t_hi = _mm256_setr_epi64x(hi0, hi1, hi2, hi3);
            let t_lo = _mm256_setr_epi64x(lo0 as i64, lo1 as i64, lo2 as i64, lo3 as i64);

            let gt = unsafe { avx2_i128_gt_masks(q_hi_vec, q_lo_vec, t_hi, t_lo) };
            let m = _mm256_movemask_epi8(gt);

            if avx2_lane_any(m, 0) {
                out.extend_from_slice(&rules[i0].ranges);
            }
            if avx2_lane_any(m, 1) {
                out.extend_from_slice(&rules[i1].ranges);
            }
            if avx2_lane_any(m, 2) {
                out.extend_from_slice(&rules[i2].ranges);
            }
            if avx2_lane_any(m, 3) {
                out.extend_from_slice(&rules[i3].ranges);
            }

            k += 4;
        }
        
        // Tail
        for &i in &gt_idx[k..] {
            let rule = &rules[i];
            if rule_matches_typed_query(NumericScalar::Signed(query), rule) {
                out.extend_from_slice(&rule.ranges);
            }
        }
    }

    merge_row_ranges(out)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn candidate_row_ranges_for_u128_query_avx2(
    query: u128,
    rules: &[NumericCorrelationRule],
) -> smallvec::SmallVec<[RowRange; 64]> {
    use std::arch::x86_64::*;

    // Only accelerates Unsigned(u128) rules; others fall back to scalar per rule.
    // Split indices by op so each SIMD loop does one comparison direction.
    let mut lt_idx = smallvec::SmallVec::<[usize; 64]>::new();
    let mut gt_idx = smallvec::SmallVec::<[usize; 64]>::new();
    let mut scalar_fallback = smallvec::SmallVec::<[usize; 64]>::new();

    for (i, r) in rules.iter().enumerate() {
        match (r.op, r.value) {
            (NumericRuleOp::LessThan, NumericScalar::Unsigned(_)) => lt_idx.push(i),
            (NumericRuleOp::GreaterThan, NumericScalar::Unsigned(_)) => gt_idx.push(i),
            _ => scalar_fallback.push(i),
        }
    }

    let mut out = smallvec::SmallVec::<[RowRange; 64]>::new();

    // Apply scalar fallback rules (non-u128-unsigned, if any).
    for &i in &scalar_fallback {
        let rule = &rules[i];
        if rule_matches_typed_query(NumericScalar::Unsigned(query), rule) {
            out.extend_from_slice(&rule.ranges);
        }
    }

    // Decompose query into (hi, lo) words.
    let qb = query.to_le_bytes();
    let q_lo = u64::from_le_bytes(qb[0..8].try_into().unwrap());
    let q_hi = u64::from_le_bytes(qb[8..16].try_into().unwrap());

    let q_hi_vec = _mm256_set1_epi64x(q_hi as i64);
    let q_lo_vec = _mm256_set1_epi64x(q_lo as i64);

    // LessThan op: match if query < threshold  <=>  threshold > query
    {
        let mut k = 0usize;
        while k + 4 <= lt_idx.len() {
            let i0 = lt_idx[k];
            let i1 = lt_idx[k + 1];
            let i2 = lt_idx[k + 2];
            let i3 = lt_idx[k + 3];

            let t0 = match rules[i0].value {
                NumericScalar::Unsigned(v) => v,
                _ => unreachable!(),
            };
            let t1 = match rules[i1].value {
                NumericScalar::Unsigned(v) => v,
                _ => unreachable!(),
            };
            let t2 = match rules[i2].value {
                NumericScalar::Unsigned(v) => v,
                _ => unreachable!(),
            };
            let t3 = match rules[i3].value {
                NumericScalar::Unsigned(v) => v,
                _ => unreachable!(),
            };

            let b0 = t0.to_le_bytes();
            let b1 = t1.to_le_bytes();
            let b2 = t2.to_le_bytes();
            let b3 = t3.to_le_bytes();

            let lo0 = u64::from_le_bytes(b0[0..8].try_into().unwrap());
            let hi0 = u64::from_le_bytes(b0[8..16].try_into().unwrap());
            let lo1 = u64::from_le_bytes(b1[0..8].try_into().unwrap());
            let hi1 = u64::from_le_bytes(b1[8..16].try_into().unwrap());
            let lo2 = u64::from_le_bytes(b2[0..8].try_into().unwrap());
            let hi2 = u64::from_le_bytes(b2[8..16].try_into().unwrap());
            let lo3 = u64::from_le_bytes(b3[0..8].try_into().unwrap());
            let hi3 = u64::from_le_bytes(b3[8..16].try_into().unwrap());

            let t_hi = _mm256_setr_epi64x(hi0 as i64, hi1 as i64, hi2 as i64, hi3 as i64);
            let t_lo = _mm256_setr_epi64x(lo0 as i64, lo1 as i64, lo2 as i64, lo3 as i64);

            let gt = unsafe { avx2_u128_gt_masks(t_hi, t_lo, q_hi_vec, q_lo_vec) };
            let m = _mm256_movemask_epi8(gt);

            if avx2_lane_any(m, 0) {
                out.extend_from_slice(&rules[i0].ranges);
            }
            if avx2_lane_any(m, 1) {
                out.extend_from_slice(&rules[i1].ranges);
            }
            if avx2_lane_any(m, 2) {
                out.extend_from_slice(&rules[i2].ranges);
            }
            if avx2_lane_any(m, 3) {
                out.extend_from_slice(&rules[i3].ranges);
            }

            k += 4;
        }

        // Tail
        for &i in &lt_idx[k..] {
            let rule = &rules[i];
            if rule_matches_typed_query(NumericScalar::Unsigned(query), rule) {
                out.extend_from_slice(&rule.ranges);
            }
        }
    }

    // GreaterThan op: match if query > threshold
    {
        let mut k = 0usize;
        while k + 4 <= gt_idx.len() {
            let i0 = gt_idx[k];
            let i1 = gt_idx[k + 1];
            let i2 = gt_idx[k + 2];
            let i3 = gt_idx[k + 3];

            let t0 = match rules[i0].value {
                NumericScalar::Unsigned(v) => v,
                _ => unreachable!(),
            };
            let t1 = match rules[i1].value {
                NumericScalar::Unsigned(v) => v,
                _ => unreachable!(),
            };
            let t2 = match rules[i2].value {
                NumericScalar::Unsigned(v) => v,
                _ => unreachable!(),
            };
            let t3 = match rules[i3].value {
                NumericScalar::Unsigned(v) => v,
                _ => unreachable!(),
            };

            let b0 = t0.to_le_bytes();
            let b1 = t1.to_le_bytes();
            let b2 = t2.to_le_bytes();
            let b3 = t3.to_le_bytes();

            let lo0 = u64::from_le_bytes(b0[0..8].try_into().unwrap());
            let hi0 = u64::from_le_bytes(b0[8..16].try_into().unwrap());
            let lo1 = u64::from_le_bytes(b1[0..8].try_into().unwrap());
            let hi1 = u64::from_le_bytes(b1[8..16].try_into().unwrap());
            let lo2 = u64::from_le_bytes(b2[0..8].try_into().unwrap());
            let hi2 = u64::from_le_bytes(b2[8..16].try_into().unwrap());
            let lo3 = u64::from_le_bytes(b3[0..8].try_into().unwrap());
            let hi3 = u64::from_le_bytes(b3[8..16].try_into().unwrap());

            let t_hi = _mm256_setr_epi64x(hi0 as i64, hi1 as i64, hi2 as i64, hi3 as i64);
            let t_lo = _mm256_setr_epi64x(lo0 as i64, lo1 as i64, lo2 as i64, lo3 as i64);

            let gt = unsafe { avx2_u128_gt_masks(q_hi_vec, q_lo_vec, t_hi, t_lo) };
            let m = _mm256_movemask_epi8(gt);

            if avx2_lane_any(m, 0) {
                out.extend_from_slice(&rules[i0].ranges);
            }
            if avx2_lane_any(m, 1) {
                out.extend_from_slice(&rules[i1].ranges);
            }
            if avx2_lane_any(m, 2) {
                out.extend_from_slice(&rules[i2].ranges);
            }
            if avx2_lane_any(m, 3) {
                out.extend_from_slice(&rules[i3].ranges);
            }

            k += 4;
        }

        // Tail
        for &i in &gt_idx[k..] {
            let rule = &rules[i];
            if rule_matches_typed_query(NumericScalar::Unsigned(query), rule) {
                out.extend_from_slice(&rule.ranges);
            }
        }
    }

    merge_row_ranges(out)
}

#[inline]
pub fn row_ranges_overlap(a: RowRange, b: RowRange) -> bool {
    // Inclusive overlap: [a.start, a.end] intersects [b.start, b.end]
    a.start_row_id <= b.end_row_id && b.start_row_id <= a.end_row_id
}

/// Sorts by `start_row_id`, then merges overlapping or directly-adjacent ranges.
pub fn merge_row_ranges(mut ranges: smallvec::SmallVec<[RowRange; 64]>) -> smallvec::SmallVec<[RowRange; 64]> {
    if ranges.len() <= 1 {
        return ranges;
    }

    ranges.sort_unstable_by(|a, b| {
        a.start_row_id
            .cmp(&b.start_row_id)
            .then_with(|| a.end_row_id.cmp(&b.end_row_id))
    });

    // SAFETY: we just sorted by start/end, and RowRange is Copy (no drop hazards).
    unsafe { merge_row_ranges_sorted_in_place_unchecked(&mut ranges) };
    ranges
}

/// Merges overlapping or directly-adjacent ranges **in-place**.
///
/// This is the fast merge step, without sorting. It can be *much* faster than `merge_row_ranges`
/// if your input is already sorted (e.g. if you maintain per-rule ranges sorted/merged).
///
/// # Safety
/// Caller must guarantee:
/// - `ranges` is sorted by `(start_row_id, end_row_id)` ascending.
/// - every element satisfies `start_row_id <= end_row_id`.
///
/// If these invariants are violated, the result may be incorrect.
pub unsafe fn merge_row_ranges_sorted_in_place_unchecked(
    ranges: &mut smallvec::SmallVec<[RowRange; 64]>,
) {
    let len = ranges.len();
    if len <= 1 {
        return;
    }

    // In-place compaction: write merged ranges back into the same buffer.
    let ptr = ranges.as_mut_ptr();
    let mut write = 0usize;
    let mut cur = unsafe { *ptr };

    for read in 1..len {
        let r = unsafe { *ptr.add(read) };
        let cur_end_plus_1 = cur.end_row_id.saturating_add(1);
        if r.start_row_id <= cur_end_plus_1 {
            cur.end_row_id = cur.end_row_id.max(r.end_row_id);
        } else {
            unsafe { *ptr.add(write) = cur };
            write += 1;
            cur = r;
        }
    }

    unsafe { *ptr.add(write) = cur };
    write += 1;

    // SAFETY: write <= len always holds.
    unsafe { ranges.set_len(write) };
}

/// Scalar overlap check between two vectors of row ranges.
///
/// Returns true if any range in `a` overlaps any range in `b`.
#[inline]
pub fn any_overlap_scalar(a: &[RowRange], b: &[RowRange]) -> bool {
    for &ra in a {
        for &rb in b {
            if row_ranges_overlap(ra, rb) {
                return true;
            }
        }
    }
    false
}

/// AVX2 overlap check between two vectors of row ranges.
///
/// - On non-x86_64 targets, or if AVX2 isn't available at runtime, this falls back to scalar.
/// - For correctness with full `u64` range, comparisons are performed as unsigned.
#[inline]
pub fn any_overlap_avx2(a: &[RowRange], b: &[RowRange]) -> bool {
    // Small inputs are typically faster scalar (less overhead).
    if a.is_empty() || b.is_empty() {
        return false;
    }

    #[cfg(all(target_arch = "x86_64"))]
    {
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: guarded by runtime feature detection.
            return unsafe { any_overlap_avx2_with_ranges(a, b) };
        }
    }

    any_overlap_scalar(a, b)
}

#[cfg(all(target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn any_overlap_avx2_with_ranges(a: &[RowRange], b: &[RowRange]) -> bool {
    use std::arch::x86_64::*;

    let n = b.len();
    if n == 0 {
        return false;
    }

    let sign = _mm256_set1_epi64x(i64::MIN);
    let ones = _mm256_set1_epi64x(-1);

    // Helper: unsigned (a > b) per lane mask, using signed compare after XOR with sign bit.
    #[inline(always)]
    unsafe fn cmpgt_u64(a: __m256i, b: __m256i, sign: __m256i) -> __m256i {
        let ax = unsafe { _mm256_xor_si256(a, sign) };
        let bx = unsafe { _mm256_xor_si256(b, sign) };
        unsafe { _mm256_cmpgt_epi64(ax, bx) }
    }

    // Helper: unsigned (a <= b) per lane mask.
    #[inline(always)]
    unsafe fn cmple_u64(a: __m256i, b: __m256i, sign: __m256i, ones: __m256i) -> __m256i {
        let gt = unsafe { cmpgt_u64(a, b, sign) };
        unsafe { _mm256_andnot_si256(gt, ones) }
    }

    for ra in a {
        let a_start = _mm256_set1_epi64x(ra.start_row_id as i64);
        let a_end = _mm256_set1_epi64x(ra.end_row_id as i64);

        let mut i = 0usize;
        // Load 2 ranges at a time (32 bytes): [s0,e0,s1,e1] in 64-bit lanes.
        while i + 2 <= n {
            let v = unsafe { _mm256_loadu_si256(b.as_ptr().add(i) as *const __m256i) };
            // starts = [s0,s1,s0,s1], ends = [e0,e1,e0,e1]
            let b_start = _mm256_permute4x64_epi64(v, 0x88);
            let b_end = _mm256_permute4x64_epi64(v, 0xDD);

            // overlap = (a_start <= b_end) && (b_start <= a_end)
            let c1 = unsafe { cmple_u64(a_start, b_end, sign, ones) };
            let c2 = unsafe { cmple_u64(b_start, a_end, sign, ones) };
            let both = _mm256_and_si256(c1, c2);

            if _mm256_movemask_epi8(both) != 0 {
                return true;
            }
            i += 2;
        }

        // Tail.
        for j in i..n {
            if row_ranges_overlap(*ra, b[j]) {
                return true;
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use crate::core::db_type::DbType;

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
            out.push(RowRange {
                start_row_id: s,
                end_row_id: s.saturating_add(len),
            });
        }
        out
    }

    #[test]
    fn candidate_ranges_example_query_850() {
        let rules = vec![
            NumericCorrelationRule {
                column_schema_id: 1,
                column_type: DbType::I128,
                op: NumericRuleOp::LessThan,
                value: NumericScalar::Signed(1000),
                ranges: vec![
                    RowRange {
                        start_row_id: 10,
                        end_row_id: 20,
                    },
                    RowRange {
                        start_row_id: 100,
                        end_row_id: 120,
                    },
                    RowRange {
                        start_row_id: 330,
                        end_row_id: 420,
                    },
                ],
            },
            NumericCorrelationRule {
                column_schema_id: 1,
                column_type: DbType::I128,
                op: NumericRuleOp::GreaterThan,
                value: NumericScalar::Signed(800),
                ranges: vec![
                    RowRange {
                        start_row_id: 350,
                        end_row_id: 420,
                    },
                    RowRange {
                        start_row_id: 70,
                        end_row_id: 140,
                    },
                    RowRange {
                        start_row_id: 220,
                        end_row_id: 340,
                    },
                ],
            },
            NumericCorrelationRule {
                column_schema_id: 1,
                column_type: DbType::I128,
                op: NumericRuleOp::GreaterThan,
                value: NumericScalar::Signed(900),
                ranges: vec![
                    RowRange {
                        start_row_id: 380,
                        end_row_id: 450,
                    },
                    RowRange {
                        start_row_id: 40,
                        end_row_id: 120,
                    },
                    RowRange {
                        start_row_id: 130,
                        end_row_id: 320,
                    },
                ],
            },
        ];

        let got = candidate_row_ranges_for_query(NumericScalar::Signed(850), &rules);
        let expected: smallvec::SmallVec<[RowRange; 64]> = smallvec::smallvec![
            RowRange {
                start_row_id: 10,
                end_row_id: 20,
            },
            RowRange {
                start_row_id: 70,
                end_row_id: 140,
            },
            RowRange {
                start_row_id: 220,
                end_row_id: 420,
            },
        ];
        assert_eq!(got, expected);
    }

    #[test]
    fn overlap_scalar_and_avx2_agree_on_simple_cases() {
        let a = vec![
            RowRange {
                start_row_id: 10,
                end_row_id: 20,
            },
            RowRange {
                start_row_id: 100,
                end_row_id: 120,
            },
            RowRange {
                start_row_id: 330,
                end_row_id: 420,
            },
        ];
        let b = vec![
            RowRange {
                start_row_id: 350,
                end_row_id: 420,
            },
            RowRange {
                start_row_id: 70,
                end_row_id: 140,
            },
            RowRange {
                start_row_id: 220,
                end_row_id: 340,
            },
        ];

        let scalar = any_overlap_scalar(&a, &b);
        let simd = any_overlap_avx2(&a, &b);
        assert_eq!(scalar, simd);
        assert!(scalar);

        let c = vec![RowRange {
            start_row_id: 500,
            end_row_id: 600,
        }];
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
        let a = vec![RowRange {
            start_row_id: 10,
            end_row_id: 20,
        }];
        let b = vec![
            RowRange {
                start_row_id: 0,
                end_row_id: 5,
            },
            RowRange {
                start_row_id: 30,
                end_row_id: 40,
            },
            // Tail element overlaps.
            RowRange {
                start_row_id: 15,
                end_row_id: 16,
            },
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
        let a = vec![RowRange {
            start_row_id: big,
            end_row_id: big + 100,
        }];
        let b = vec![RowRange {
            start_row_id: big + 50,
            end_row_id: big + 60,
        }];

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

    #[test]
    fn candidate_ranges_avx2_matches_scalar_when_available() {
        // Ensures the AVX2 fast-path in `candidate_row_ranges_for_i128_query` is equivalent.
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx2") {
                // Make enough rules to force the function into the AVX2 path (len >= 16).
                let mut rules = Vec::new();
                for i in 0..32u64 {
                    let op = if i % 2 == 0 {
                        NumericRuleOp::LessThan
                    } else {
                        NumericRuleOp::GreaterThan
                    };
                    let threshold = if i % 3 == 0 { 900i128 } else { 1000i128 };
                    rules.push(NumericCorrelationRule {
                        column_schema_id: 1,
                        column_type: DbType::I128,
                        op,
                        value: NumericScalar::Signed(threshold),
                        ranges: vec![RowRange {
                            start_row_id: i * 10,
                            end_row_id: i * 10 + 5,
                        }],
                    });
                }

                let query = 950i128;
                let scalar = super::candidate_row_ranges_for_query(NumericScalar::Signed(query), &rules);
                let avx2 = unsafe { super::candidate_row_ranges_for_i128_query_avx2(query, &rules) };
                assert_eq!(scalar, avx2);

                // Also check the public entry point (which may select AVX2).
                let public = candidate_row_ranges_for_query(NumericScalar::Signed(query), &rules);
                assert_eq!(scalar, public);
            }
        }
    }

    #[test]
    fn merge_sorted_in_place_unchecked_matches_merge_row_ranges() {
        let a: smallvec::SmallVec<[RowRange; 64]> = smallvec::smallvec![
            RowRange {
                start_row_id: 10,
                end_row_id: 20,
            },
            RowRange {
                start_row_id: 21,
                end_row_id: 30,
            },
            RowRange {
                start_row_id: 100,
                end_row_id: 120,
            },
            RowRange {
                start_row_id: 110,
                end_row_id: 130,
            },
            RowRange {
                start_row_id: 1000,
                end_row_id: 1000,
            },
        ];

        // This is already sorted by (start,end).
        let mut b = a.clone();
        unsafe { merge_row_ranges_sorted_in_place_unchecked(&mut b) };

        let c = merge_row_ranges(a);

        assert_eq!(b, c);

        let expected: smallvec::SmallVec<[RowRange; 64]> = smallvec::smallvec![
            RowRange {
                start_row_id: 10,
                end_row_id: 30,
            },
            RowRange {
                start_row_id: 100,
                end_row_id: 130,
            },
            RowRange {
                start_row_id: 1000,
                end_row_id: 1000,
            },
        ];
        assert_eq!(b, expected);
    }
}
