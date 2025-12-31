// AVX/AVX2/AVX512-specific SIMD implementations for the Semantic Mapping Engine.
//
// This module intentionally contains only the unsafe, target_feature-gated routines.
// The higher-level selection logic (feature detection + scalar fallbacks) lives in
// `semantic_mapping_engine.rs`.

#![allow(clippy::cast_ptr_alignment)]

#[cfg(target_arch = "x86_64")]
use super::semantic_mapping_engine::{
    decode_rule_f64, decode_rule_i64, decode_rule_u64, decode_string_metric_u64,
    interval_f64_matches, interval_i64_matches, interval_u64_matches,
};

#[cfg(target_arch = "x86_64")]
use crate::core::sme::rules::{SemanticRule, SemanticRuleOp};

#[cfg(target_arch = "x86_64")]
#[inline]
pub(super) fn u64_ordered_to_i64(x: u64) -> i64 {
    (x ^ 0x8000_0000_0000_0000u64) as i64
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
pub(super) unsafe fn simd_filter_eq_i64_avx2(rules: &[SemanticRule], val: i64) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;
    let mut out = Vec::new();
    let mut i = 0usize;
    let val_v = _mm256_set1_epi64x(val);
    let idx = _mm_set_epi32(3 * 72, 2 * 72, 1 * 72, 0 * 72);
    let base_ptr = rules.as_ptr() as *const u8;

    while i + 4 <= rules.len() {
        let current_ptr = base_ptr.add(i * 72);
        let lo_v = _mm256_i32gather_epi64(current_ptr.add(24) as *const i64, idx, 1);
        let hi_v = _mm256_i32gather_epi64(current_ptr.add(40) as *const i64, idx, 1);

        let lo_gt_val = _mm256_cmpgt_epi64(lo_v, val_v);
        let val_gt_hi = _mm256_cmpgt_epi64(val_v, hi_v);
        let bad = _mm256_or_si256(lo_gt_val, val_gt_hi);
        let mask = !(_mm256_movemask_pd(_mm256_castsi256_pd(bad)) as u32);

        if mask != 0 {
            for lane in 0..4 {
                if (mask & (1u32 << lane)) != 0 {
                    let r = &rules[i + lane];
                    out.push((r.start_row_id, r.end_row_id));
                }
            }
        }
        i += 4;
    }

    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_i64(r) {
            if val >= lo && val <= hi {
                out.push((r.start_row_id, r.end_row_id));
            }
        } else {
            out.push((r.start_row_id, r.end_row_id));
        }
    }

    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
pub(super) unsafe fn simd_filter_eq_u64_avx2(rules: &[SemanticRule], val: u64) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;
    let mut out = Vec::new();
    let mut i = 0usize;
    let val_i = u64_ordered_to_i64(val);
    let val_v = _mm256_set1_epi64x(val_i);
    let idx = _mm_set_epi32(3 * 72, 2 * 72, 1 * 72, 0 * 72);
    let base_ptr = rules.as_ptr() as *const u8;
    let sign_flip = _mm256_set1_epi64x(i64::MIN);

    while i + 4 <= rules.len() {
        let current_ptr = base_ptr.add(i * 72);
        let lo_raw = _mm256_i32gather_epi64(current_ptr.add(24) as *const i64, idx, 1);
        let hi_raw = _mm256_i32gather_epi64(current_ptr.add(40) as *const i64, idx, 1);

        let lo_v = _mm256_xor_si256(lo_raw, sign_flip);
        let hi_v = _mm256_xor_si256(hi_raw, sign_flip);

        let lo_gt_val = _mm256_cmpgt_epi64(lo_v, val_v);
        let val_gt_hi = _mm256_cmpgt_epi64(val_v, hi_v);
        let bad = _mm256_or_si256(lo_gt_val, val_gt_hi);
        let mask = !(_mm256_movemask_pd(_mm256_castsi256_pd(bad)) as u32);

        if mask != 0 {
            for lane in 0..4 {
                if (mask & (1u32 << lane)) != 0 {
                    let r = &rules[i + lane];
                    out.push((r.start_row_id, r.end_row_id));
                }
            }
        }
        i += 4;
    }

    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_u64(r) {
            if val >= lo && val <= hi {
                out.push((r.start_row_id, r.end_row_id));
            }
        } else {
            out.push((r.start_row_id, r.end_row_id));
        }
    }

    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
pub(super) unsafe fn simd_filter_eq_f64_avx2(rules: &[SemanticRule], val: f64) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;
    let mut out = Vec::new();
    let mut i = 0usize;
    let val_v = _mm256_set1_pd(val);
    let idx = _mm_set_epi32(3 * 72, 2 * 72, 1 * 72, 0 * 72);
    let base_ptr = rules.as_ptr() as *const u8;

    while i + 4 <= rules.len() {
        let current_ptr = base_ptr.add(i * 72);
        let lo_v = _mm256_i32gather_pd(current_ptr.add(24) as *const f64, idx, 1);
        let hi_v = _mm256_i32gather_pd(current_ptr.add(40) as *const f64, idx, 1);

        let lo_le_val = _mm256_cmp_pd(lo_v, val_v, _CMP_LE_OQ);
        let val_le_hi = _mm256_cmp_pd(val_v, hi_v, _CMP_LE_OQ);

        let good = _mm256_and_pd(lo_le_val, val_le_hi);
        let mask = _mm256_movemask_pd(good) as u32;

        if mask != 0 {
            for lane in 0..4 {
                if (mask & (1u32 << lane)) != 0 {
                    let r = &rules[i + lane];
                    out.push((r.start_row_id, r.end_row_id));
                }
            }
        }
        i += 4;
    }

    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_f64(r) {
            if val >= lo && val <= hi {
                out.push((r.start_row_id, r.end_row_id));
            }
        } else {
            out.push((r.start_row_id, r.end_row_id));
        }
    }

    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
pub(super) unsafe fn simd_filter_i64_avx2(
    rules: &[SemanticRule],
    lower: Option<(i64, bool)>,
    upper: Option<(i64, bool)>,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;

    let mut out = Vec::new();
    let mut i = 0usize;
    let idx = _mm_set_epi32(3 * 72, 2 * 72, 1 * 72, 0 * 72);
    let base_ptr = rules.as_ptr() as *const u8;

    while i + 4 <= rules.len() {
        let current_ptr = base_ptr.add(i * 72);
        let lo_v = _mm256_i32gather_epi64(current_ptr.add(24) as *const i64, idx, 1);
        let hi_v = _mm256_i32gather_epi64(current_ptr.add(40) as *const i64, idx, 1);

        let mut mask = !0u32;
        if let Some((b, inc)) = lower {
            let b_v = _mm256_set1_epi64x(b);
            let hi_gt_b = _mm256_cmpgt_epi64(hi_v, b_v);
            let hi_eq_b = _mm256_cmpeq_epi64(hi_v, b_v);
            let lower_ok = if inc { _mm256_or_si256(hi_gt_b, hi_eq_b) } else { hi_gt_b };
            mask &= _mm256_movemask_pd(_mm256_castsi256_pd(lower_ok)) as u32;
        }
        if let Some((b, inc)) = upper {
            let b_v = _mm256_set1_epi64x(b);
            let lo_gt_b = _mm256_cmpgt_epi64(lo_v, b_v);
            let lo_eq_b = _mm256_cmpeq_epi64(lo_v, b_v);
            let lo_le_b = _mm256_xor_si256(_mm256_set1_epi64x(-1), lo_gt_b);
            let upper_ok = if inc { lo_le_b } else { _mm256_andnot_si256(lo_eq_b, lo_le_b) };
            mask &= _mm256_movemask_pd(_mm256_castsi256_pd(upper_ok)) as u32;
        }

        if mask != 0 {
            for lane in 0..4 {
                if (mask & (1u32 << lane)) != 0 {
                    let r = &rules[i + lane];
                    out.push((r.start_row_id, r.end_row_id));
                }
            }
        }

        i += 4;
    }

    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_i64(r) {
            if interval_i64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        }
    }

    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx512f")]
pub(super) unsafe fn simd_filter_i64_avx512(
    rules: &[SemanticRule],
    lower: Option<(i64, bool)>,
    upper: Option<(i64, bool)>,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;

    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 8 <= rules.len() {
        let mut lo_arr = [0i64; 8];
        let mut hi_arr = [0i64; 8];
        for lane in 0..8 {
            if let Some((lo, hi)) = decode_rule_i64(&rules[i + lane]) {
                lo_arr[lane] = lo;
                hi_arr[lane] = hi;
            } else {
                lo_arr[lane] = i64::MIN;
                hi_arr[lane] = i64::MAX;
            }
        }

        let lo_v = _mm512_loadu_si512(lo_arr.as_ptr() as *const _);
        let hi_v = _mm512_loadu_si512(hi_arr.as_ptr() as *const _);

        let mut mask: u8 = 0xFF;
        if let Some((b, inc)) = lower {
            let b_v = _mm512_set1_epi64(b);
            let gt = _mm512_cmpgt_epi64_mask(hi_v, b_v);
            let eq = _mm512_cmpeq_epi64_mask(hi_v, b_v);
            let lower_ok = if inc { gt | eq } else { gt };
            mask &= lower_ok as u8;
        }
        if let Some((b, inc)) = upper {
            let b_v = _mm512_set1_epi64(b);
            let lt = _mm512_cmpgt_epi64_mask(b_v, lo_v);
            let eq = _mm512_cmpeq_epi64_mask(lo_v, b_v);
            let upper_ok = if inc { lt | eq } else { lt };
            mask &= upper_ok as u8;
        }

        for lane in 0..8 {
            if (mask & (1u8 << lane)) != 0 {
                let r = &rules[i + lane];
                out.push((r.start_row_id, r.end_row_id));
            }
        }

        i += 8;
    }

    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_i64(r) {
            if interval_i64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        }
    }

    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
pub(super) unsafe fn simd_filter_u64_avx2(
    rules: &[SemanticRule],
    lower: Option<(u64, bool)>,
    upper: Option<(u64, bool)>,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;

    let mut out = Vec::new();
    let mut i = 0usize;
    let idx = _mm_set_epi32(3 * 72, 2 * 72, 1 * 72, 0 * 72);
    let base_ptr = rules.as_ptr() as *const u8;
    let sign_flip = _mm256_set1_epi64x(i64::MIN);

    while i + 4 <= rules.len() {
        let current_ptr = base_ptr.add(i * 72);
        let lo_raw = _mm256_i32gather_epi64(current_ptr.add(24) as *const i64, idx, 1);
        let hi_raw = _mm256_i32gather_epi64(current_ptr.add(40) as *const i64, idx, 1);

        let lo_v = _mm256_xor_si256(lo_raw, sign_flip);
        let hi_v = _mm256_xor_si256(hi_raw, sign_flip);

        let mut mask = !0u32;
        if let Some((b_u, inc)) = lower {
            let b = u64_ordered_to_i64(b_u);
            let b_v = _mm256_set1_epi64x(b);
            let hi_gt_b = _mm256_cmpgt_epi64(hi_v, b_v);
            let hi_eq_b = _mm256_cmpeq_epi64(hi_v, b_v);
            let lower_ok = if inc { _mm256_or_si256(hi_gt_b, hi_eq_b) } else { hi_gt_b };
            mask &= _mm256_movemask_pd(_mm256_castsi256_pd(lower_ok)) as u32;
        }
        if let Some((b_u, inc)) = upper {
            let b = u64_ordered_to_i64(b_u);
            let b_v = _mm256_set1_epi64x(b);
            let lo_gt_b = _mm256_cmpgt_epi64(lo_v, b_v);
            let lo_eq_b = _mm256_cmpeq_epi64(lo_v, b_v);
            let lo_le_b = _mm256_xor_si256(_mm256_set1_epi64x(-1), lo_gt_b);
            let upper_ok = if inc { lo_le_b } else { _mm256_andnot_si256(lo_eq_b, lo_le_b) };
            mask &= _mm256_movemask_pd(_mm256_castsi256_pd(upper_ok)) as u32;
        }

        if mask != 0 {
            for lane in 0..4 {
                if (mask & (1u32 << lane)) != 0 {
                    let r = &rules[i + lane];
                    out.push((r.start_row_id, r.end_row_id));
                }
            }
        }
        i += 4;
    }

    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_u64(r) {
            if interval_u64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        }
    }

    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx512f")]
pub(super) unsafe fn simd_filter_u64_avx512(
    rules: &[SemanticRule],
    lower: Option<(u64, bool)>,
    upper: Option<(u64, bool)>,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;

    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 8 <= rules.len() {
        let mut lo_arr = [0i64; 8];
        let mut hi_arr = [0i64; 8];
        for lane in 0..8 {
            if let Some((lo, hi)) = decode_rule_u64(&rules[i + lane]) {
                lo_arr[lane] = u64_ordered_to_i64(lo);
                hi_arr[lane] = u64_ordered_to_i64(hi);
            } else {
                lo_arr[lane] = i64::MIN;
                hi_arr[lane] = i64::MAX;
            }
        }

        let lo_v = _mm512_loadu_si512(lo_arr.as_ptr() as *const _);
        let hi_v = _mm512_loadu_si512(hi_arr.as_ptr() as *const _);

        let mut mask: u8 = 0xFF;
        if let Some((b_u, inc)) = lower {
            let b = u64_ordered_to_i64(b_u);
            let b_v = _mm512_set1_epi64(b);
            let gt = _mm512_cmpgt_epi64_mask(hi_v, b_v);
            let eq = _mm512_cmpeq_epi64_mask(hi_v, b_v);
            let lower_ok = if inc { gt | eq } else { gt };
            mask &= lower_ok as u8;
        }
        if let Some((b_u, inc)) = upper {
            let b = u64_ordered_to_i64(b_u);
            let b_v = _mm512_set1_epi64(b);
            let lt = _mm512_cmpgt_epi64_mask(b_v, lo_v);
            let eq = _mm512_cmpeq_epi64_mask(lo_v, b_v);
            let upper_ok = if inc { lt | eq } else { lt };
            mask &= upper_ok as u8;
        }

        for lane in 0..8 {
            if (mask & (1u8 << lane)) != 0 {
                let r = &rules[i + lane];
                out.push((r.start_row_id, r.end_row_id));
            }
        }

        i += 8;
    }

    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_u64(r) {
            if interval_u64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        }
    }

    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
pub(super) unsafe fn simd_filter_f64_avx2(
    rules: &[SemanticRule],
    lower: Option<(f64, bool)>,
    upper: Option<(f64, bool)>,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;
    let mut out = Vec::new();
    let mut i = 0usize;
    let idx = _mm_set_epi32(3 * 72, 2 * 72, 1 * 72, 0 * 72);
    let base_ptr = rules.as_ptr() as *const u8;

    while i + 4 <= rules.len() {
        let current_ptr = base_ptr.add(i * 72);
        let lo_v = _mm256_i32gather_pd(current_ptr.add(24) as *const f64, idx, 1);
        let hi_v = _mm256_i32gather_pd(current_ptr.add(40) as *const f64, idx, 1);

        let mut mask = !0u32;
        if let Some((b, inc)) = lower {
            let b_v = _mm256_set1_pd(b);
            let lower_ok = if inc { _mm256_cmp_pd(hi_v, b_v, _CMP_GE_OQ) } else { _mm256_cmp_pd(hi_v, b_v, _CMP_GT_OQ) };
            mask &= _mm256_movemask_pd(lower_ok) as u32;
        }
        if let Some((b, inc)) = upper {
            let b_v = _mm256_set1_pd(b);
            let upper_ok = if inc { _mm256_cmp_pd(lo_v, b_v, _CMP_LE_OQ) } else { _mm256_cmp_pd(lo_v, b_v, _CMP_LT_OQ) };
            mask &= _mm256_movemask_pd(upper_ok) as u32;
        }

        if mask != 0 {
            for lane in 0..4 {
                if (mask & (1u32 << lane)) != 0 {
                    let r = &rules[i + lane];
                    out.push((r.start_row_id, r.end_row_id));
                }
            }
        }
        i += 4;
    }

    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_f64(r) {
            if interval_f64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        }
    }

    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx512f")]
pub(super) unsafe fn simd_filter_f64_avx512(
    rules: &[SemanticRule],
    lower: Option<(f64, bool)>,
    upper: Option<(f64, bool)>,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 8 <= rules.len() {
        let mut lo_arr = [0f64; 8];
        let mut hi_arr = [0f64; 8];
        for lane in 0..8 {
            if let Some((lo, hi)) = decode_rule_f64(&rules[i + lane]) {
                lo_arr[lane] = lo;
                hi_arr[lane] = hi;
            } else {
                lo_arr[lane] = f64::NEG_INFINITY;
                hi_arr[lane] = f64::INFINITY;
            }
        }

        let lo_v = _mm512_loadu_pd(lo_arr.as_ptr());
        let hi_v = _mm512_loadu_pd(hi_arr.as_ptr());

        let mut mask: u8 = 0xFF;
        if let Some((b, inc)) = lower {
            let b_v = _mm512_set1_pd(b);
            let lower_ok = if inc { _mm512_cmp_pd_mask(hi_v, b_v, _CMP_GE_OQ) } else { _mm512_cmp_pd_mask(hi_v, b_v, _CMP_GT_OQ) };
            mask &= lower_ok as u8;
        }
        if let Some((b, inc)) = upper {
            let b_v = _mm512_set1_pd(b);
            let upper_ok = if inc { _mm512_cmp_pd_mask(lo_v, b_v, _CMP_LE_OQ) } else { _mm512_cmp_pd_mask(lo_v, b_v, _CMP_LT_OQ) };
            mask &= upper_ok as u8;
        }

        for lane in 0..8 {
            if (mask & (1u8 << lane)) != 0 {
                let r = &rules[i + lane];
                out.push((r.start_row_id, r.end_row_id));
            }
        }

        i += 8;
    }

    for r in rules[i..].iter() {
        if let Some((lo, hi)) = decode_rule_f64(r) {
            if interval_f64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        }
    }

    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
pub(super) unsafe fn bitset256_contains_avx2(rule_mask: *const u8, required: *const u8) -> bool {
    use std::arch::x86_64::*;
    let m = _mm256_loadu_si256(rule_mask as *const __m256i);
    let r = _mm256_loadu_si256(required as *const __m256i);
    let and = _mm256_and_si256(m, r);
    let eq = _mm256_cmpeq_epi8(and, r);
    (_mm256_movemask_epi8(eq) as u32) == 0xFFFF_FFFFu32
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx512bw")]
pub(super) unsafe fn bitset256_contains_avx512bw(rule_mask: *const u8, required: *const u8) -> bool {
    use std::arch::x86_64::*;
    let lane_mask: __mmask64 = 0xFFFF_FFFFu64;
    let m = _mm512_maskz_loadu_epi8(lane_mask, rule_mask as *const i8);
    let r = _mm512_maskz_loadu_epi8(lane_mask, required as *const i8);
    let and = _mm512_and_si512(m, r);
    let eq_mask = _mm512_cmpeq_epi8_mask(and, r);
    eq_mask == u64::MAX
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
pub(super) unsafe fn simd_filter_string_metric_u64_avx2(
    rules: &[SemanticRule],
    op: SemanticRuleOp,
    lower: Option<(u64, bool)>,
    upper: Option<(u64, bool)>,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;

    let mut out = Vec::new();
    let mut i = 0usize;
    let idx = _mm_set_epi32(3 * 72, 2 * 72, 1 * 72, 0 * 72);
    let base_ptr = rules.as_ptr() as *const u8;
    let sign_flip = _mm256_set1_epi64x(i64::MIN);

    let lower_i = lower.map(|(v, inc)| (u64_ordered_to_i64(v), inc));
    let upper_i = upper.map(|(v, inc)| (u64_ordered_to_i64(v), inc));

    let target_op = _mm256_set1_epi64x(op as u8 as i64);

    while i + 4 <= rules.len() {
        let current_ptr = base_ptr.add(i * 72);

        let meta_v = _mm256_i32gather_epi64(current_ptr.add(16) as *const i64, idx, 1);
        let op_v = _mm256_srli_epi64(meta_v, 8);
        let op_v = _mm256_and_si256(op_v, _mm256_set1_epi64x(0xFF));

        let op_match = _mm256_cmpeq_epi64(op_v, target_op);

        let lo_raw = _mm256_i32gather_epi64(current_ptr.add(24) as *const i64, idx, 1);
        let hi_raw = _mm256_i32gather_epi64(current_ptr.add(40) as *const i64, idx, 1);

        let lo_v = _mm256_xor_si256(lo_raw, sign_flip);
        let hi_v = _mm256_xor_si256(hi_raw, sign_flip);

        let mut mask = _mm256_movemask_pd(_mm256_castsi256_pd(op_match)) as u32;

        if mask != 0 {
            if let Some((b, inc)) = lower_i {
                let b_v = _mm256_set1_epi64x(b);
                let hi_gt_b = _mm256_cmpgt_epi64(hi_v, b_v);
                let hi_eq_b = _mm256_cmpeq_epi64(hi_v, b_v);
                let lower_ok = if inc { _mm256_or_si256(hi_gt_b, hi_eq_b) } else { hi_gt_b };
                mask &= _mm256_movemask_pd(_mm256_castsi256_pd(lower_ok)) as u32;
            }
            if let Some((b, inc)) = upper_i {
                let b_v = _mm256_set1_epi64x(b);
                let lo_gt_b = _mm256_cmpgt_epi64(lo_v, b_v);
                let lo_eq_b = _mm256_cmpeq_epi64(lo_v, b_v);
                let lo_le_b = _mm256_xor_si256(_mm256_set1_epi64x(-1), lo_gt_b);
                let upper_ok = if inc { lo_le_b } else { _mm256_andnot_si256(lo_eq_b, lo_le_b) };
                mask &= _mm256_movemask_pd(_mm256_castsi256_pd(upper_ok)) as u32;
            }

            if mask != 0 {
                for lane in 0..4 {
                    if (mask & (1u32 << lane)) != 0 {
                        let r = &rules[i + lane];
                        out.push((r.start_row_id, r.end_row_id));
                    }
                }
            }
        }
        i += 4;
    }

    for r in rules[i..].iter() {
        if r.op != op {
            continue;
        }
        if let Some((lo, hi)) = decode_string_metric_u64(r) {
            if interval_u64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        }
    }

    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx512f")]
pub(super) unsafe fn simd_filter_string_metric_u64_avx512(
    rules: &[SemanticRule],
    op: SemanticRuleOp,
    lower: Option<(u64, bool)>,
    upper: Option<(u64, bool)>,
) -> Vec<(u64, u64)> {
    use std::arch::x86_64::*;

    let mut out = Vec::new();
    let mut i = 0usize;

    let lower_i = lower.map(|(v, _)| u64_ordered_to_i64(v));
    let upper_i = upper.map(|(v, _)| u64_ordered_to_i64(v));

    while i + 8 <= rules.len() {
        let mut lo_arr = [0i64; 8];
        let mut hi_arr = [0i64; 8];
        let mut ok_mask: u32 = 0;

        for lane in 0..8 {
            let r = &rules[i + lane];
            if r.op != op {
                continue;
            }
            if let Some((lo_u, hi_u)) = decode_string_metric_u64(r) {
                lo_arr[lane] = u64_ordered_to_i64(lo_u);
                hi_arr[lane] = u64_ordered_to_i64(hi_u);
                ok_mask |= 1u32 << lane;
            }
        }

        if ok_mask != 0 {
            let lo_v = _mm512_loadu_si512(lo_arr.as_ptr() as *const __m512i);
            let hi_v = _mm512_loadu_si512(hi_arr.as_ptr() as *const __m512i);

            let mut lane_ok: u32 = ok_mask;

            if let Some((_, inc)) = lower {
                let b = lower_i.unwrap();
                let b_v = _mm512_set1_epi64(b);
                let hi_lt_b = _mm512_cmpgt_epi64_mask(b_v, hi_v);
                let hi_eq_b = _mm512_cmpeq_epi64_mask(hi_v, b_v);
                let bad = if inc { hi_lt_b } else { hi_lt_b | hi_eq_b };
                lane_ok &= !(bad as u32);
            }

            if let Some((_, inc)) = upper {
                let b = upper_i.unwrap();
                let b_v = _mm512_set1_epi64(b);
                let lo_gt_b = _mm512_cmpgt_epi64_mask(lo_v, b_v);
                let lo_eq_b = _mm512_cmpeq_epi64_mask(lo_v, b_v);
                let bad = if inc { lo_gt_b } else { lo_gt_b | lo_eq_b };
                lane_ok &= !(bad as u32);
            }

            for lane in 0..8 {
                if (lane_ok >> lane) & 1 == 1 {
                    let r = &rules[i + lane];
                    out.push((r.start_row_id, r.end_row_id));
                }
            }
        }

        i += 8;
    }

    for r in rules[i..].iter() {
        if r.op != op {
            continue;
        }
        if let Some((lo, hi)) = decode_string_metric_u64(r) {
            if interval_u64_matches(lo, hi, lower, upper) {
                out.push((r.start_row_id, r.end_row_id));
            }
        }
    }

    out
}
