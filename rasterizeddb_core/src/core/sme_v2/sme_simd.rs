//! SIMD helpers for SME v2 numeric rule selection.
//!
//! These functions accelerate the rule-selection phase (comparing a query scalar
//! against many rule thresholds) using AVX2/AVX512 when available.

use super::rules::NumericRuleRecordDisk;

#[cfg(target_arch = "x86_64")]
#[inline]
pub fn has_avx2() -> bool {
    std::is_x86_feature_detected!("avx2")
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
pub fn has_avx2() -> bool {
    false
}

#[cfg(target_arch = "x86_64")]
#[inline]
pub fn has_avx512f() -> bool {
    std::is_x86_feature_detected!("avx512f")
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
pub fn has_avx512f() -> bool {
    false
}

/// Returns indices of rules where `query < threshold` (signed i64).
///
/// Caller must ensure the records correspond to signed-int thresholds stored in
/// the first 8 bytes of `value`.
#[cfg(target_arch = "x86_64")]
pub fn select_lt_i64(records: &[NumericRuleRecordDisk], query: i64) -> Vec<usize> {
    if has_avx512f() {
        // Safety: guarded by runtime feature detection.
        unsafe { select_lt_i64_avx512(records, query) }
    } else if has_avx2() {
        unsafe { select_lt_i64_avx2(records, query) }
    } else {
        select_lt_i64_scalar(records, query)
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn select_lt_i64(records: &[NumericRuleRecordDisk], query: i64) -> Vec<usize> {
    select_lt_i64_scalar(records, query)
}

#[cfg(target_arch = "x86_64")]
pub fn select_gt_i64(records: &[NumericRuleRecordDisk], query: i64) -> Vec<usize> {
    if has_avx512f() {
        unsafe { select_gt_i64_avx512(records, query) }
    } else if has_avx2() {
        unsafe { select_gt_i64_avx2(records, query) }
    } else {
        select_gt_i64_scalar(records, query)
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn select_gt_i64(records: &[NumericRuleRecordDisk], query: i64) -> Vec<usize> {
    select_gt_i64_scalar(records, query)
}

#[inline]
fn threshold_i64(r: &NumericRuleRecordDisk) -> i64 {
    i64::from_le_bytes(r.value[0..8].try_into().unwrap())
}

fn select_lt_i64_scalar(records: &[NumericRuleRecordDisk], query: i64) -> Vec<usize> {
    let mut out = Vec::new();
    for (i, r) in records.iter().enumerate() {
        if query < threshold_i64(r) {
            out.push(i);
        }
    }
    out
}

fn select_gt_i64_scalar(records: &[NumericRuleRecordDisk], query: i64) -> Vec<usize> {
    let mut out = Vec::new();
    for (i, r) in records.iter().enumerate() {
        if query > threshold_i64(r) {
            out.push(i);
        }
    }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
unsafe fn select_lt_i64_avx2(records: &[NumericRuleRecordDisk], query: i64) -> Vec<usize> {
    use std::arch::x86_64::*;

    let mut out = Vec::new();
    let qv = _mm256_set1_epi64x(query);

    // Gather from record[i].value[0..8].
    const STRIDE: i32 = NumericRuleRecordDisk::BYTE_LEN as i32;
    const VALUE_OFF: i32 = 0;

    let idx = _mm_set_epi32(3 * STRIDE + VALUE_OFF, 2 * STRIDE + VALUE_OFF, 1 * STRIDE + VALUE_OFF, 0 * STRIDE + VALUE_OFF);
    let base_ptr = records.as_ptr() as *const u8;

    let mut i = 0usize;
    while i + 4 <= records.len() {
        let ptr = base_ptr.add(i * NumericRuleRecordDisk::BYTE_LEN);
        let thr = _mm256_i32gather_epi64(ptr as *const i64, idx, 1);

        // query < thr  <=>  thr > query
        let thr_gt_q = _mm256_cmpgt_epi64(thr, qv);
        let mask = _mm256_movemask_pd(_mm256_castsi256_pd(thr_gt_q)) as u32;
        if mask != 0 {
            for lane in 0..4 {
                if ((mask >> lane) & 1) != 0 {
                    out.push(i + lane);
                }
            }
        }
        i += 4;
    }

    for (j, r) in records[i..].iter().enumerate() {
        if query < threshold_i64(r) {
            out.push(i + j);
        }
    }

    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
unsafe fn select_gt_i64_avx2(records: &[NumericRuleRecordDisk], query: i64) -> Vec<usize> {
    use std::arch::x86_64::*;

    let mut out = Vec::new();
    let qv = _mm256_set1_epi64x(query);

    const STRIDE: i32 = NumericRuleRecordDisk::BYTE_LEN as i32;
    const VALUE_OFF: i32 = 0;

    let idx = _mm_set_epi32(3 * STRIDE + VALUE_OFF, 2 * STRIDE + VALUE_OFF, 1 * STRIDE + VALUE_OFF, 0 * STRIDE + VALUE_OFF);
    let base_ptr = records.as_ptr() as *const u8;

    let mut i = 0usize;
    while i + 4 <= records.len() {
        let ptr = base_ptr.add(i * NumericRuleRecordDisk::BYTE_LEN);
        let thr = _mm256_i32gather_epi64(ptr as *const i64, idx, 1);

        // query > thr
        let q_gt_thr = _mm256_cmpgt_epi64(qv, thr);
        let mask = _mm256_movemask_pd(_mm256_castsi256_pd(q_gt_thr)) as u32;
        if mask != 0 {
            for lane in 0..4 {
                if ((mask >> lane) & 1) != 0 {
                    out.push(i + lane);
                }
            }
        }
        i += 4;
    }

    for (j, r) in records[i..].iter().enumerate() {
        if query > threshold_i64(r) {
            out.push(i + j);
        }
    }

    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx512f")]
unsafe fn select_lt_i64_avx512(records: &[NumericRuleRecordDisk], query: i64) -> Vec<usize> {
    use std::arch::x86_64::*;

    let mut out = Vec::new();
    let qv = _mm512_set1_epi64(query);

    let mut i = 0usize;
    while i + 8 <= records.len() {
        // Load thresholds into a local array (avoids complex gathers on stable Rust).
        let mut thr_arr = [0i64; 8];
        for lane in 0..8 {
            thr_arr[lane] = threshold_i64(&records[i + lane]);
        }
        let thr = _mm512_loadu_si512(thr_arr.as_ptr() as *const _);

        // query < thr  <=> thr > query
        let mask: __mmask8 = _mm512_cmpgt_epi64_mask(thr, qv);
        if mask != 0 {
            for lane in 0..8 {
                if ((mask >> lane) & 1) != 0 {
                    out.push(i + lane);
                }
            }
        }
        i += 8;
    }

    for (j, r) in records[i..].iter().enumerate() {
        if query < threshold_i64(r) {
            out.push(i + j);
        }
    }

    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx512f")]
unsafe fn select_gt_i64_avx512(records: &[NumericRuleRecordDisk], query: i64) -> Vec<usize> {
    use std::arch::x86_64::*;

    let mut out = Vec::new();
    let qv = _mm512_set1_epi64(query);

    let mut i = 0usize;
    while i + 8 <= records.len() {
        let mut thr_arr = [0i64; 8];
        for lane in 0..8 {
            thr_arr[lane] = threshold_i64(&records[i + lane]);
        }
        let thr = _mm512_loadu_si512(thr_arr.as_ptr() as *const _);

        let mask: __mmask8 = _mm512_cmpgt_epi64_mask(qv, thr);
        if mask != 0 {
            for lane in 0..8 {
                if ((mask >> lane) & 1) != 0 {
                    out.push(i + lane);
                }
            }
        }
        i += 8;
    }

    for (j, r) in records[i..].iter().enumerate() {
        if query > threshold_i64(r) {
            out.push(i + j);
        }
    }

    out
}
