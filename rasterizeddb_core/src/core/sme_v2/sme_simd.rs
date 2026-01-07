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

#[inline]
fn threshold_u64(r: &NumericRuleRecordDisk) -> u64 {
    u64::from_le_bytes(r.value[0..8].try_into().unwrap())
}

#[inline]
fn threshold_f64(r: &NumericRuleRecordDisk) -> f64 {
    let bits = u64::from_le_bytes(r.value[0..8].try_into().unwrap());
    f64::from_bits(bits)
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

    let mut out = Vec::with_capacity(records.len() / 8 + 8);
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
        let mut mask = _mm256_movemask_pd(_mm256_castsi256_pd(thr_gt_q)) as u32;
        while mask != 0 {
            let lane = mask.trailing_zeros() as usize;
            out.push(i + lane);
            mask &= mask - 1;
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

    let mut out = Vec::with_capacity(records.len() / 8 + 8);
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
        let mut mask = _mm256_movemask_pd(_mm256_castsi256_pd(q_gt_thr)) as u32;
        while mask != 0 {
            let lane = mask.trailing_zeros() as usize;
            out.push(i + lane);
            mask &= mask - 1;
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

    let mut out = Vec::with_capacity(records.len() / 8 + 8);
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
        let mut mask: u32 = _mm512_cmpgt_epi64_mask(thr, qv) as u32;
        while mask != 0 {
            let lane = mask.trailing_zeros() as usize;
            out.push(i + lane);
            mask &= mask - 1;
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

    let mut out = Vec::with_capacity(records.len() / 8 + 8);
    let qv = _mm512_set1_epi64(query);

    let mut i = 0usize;
    while i + 8 <= records.len() {
        let mut thr_arr = [0i64; 8];
        for lane in 0..8 {
            thr_arr[lane] = threshold_i64(&records[i + lane]);
        }
        let thr = _mm512_loadu_si512(thr_arr.as_ptr() as *const _);

        let mut mask: u32 = _mm512_cmpgt_epi64_mask(qv, thr) as u32;
        while mask != 0 {
            let lane = mask.trailing_zeros() as usize;
            out.push(i + lane);
            mask &= mask - 1;
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

// ============================
// Unsigned (u64) selectors
// ============================

#[cfg(target_arch = "x86_64")]
#[inline]
fn u64_ordered_to_i64(x: u64) -> i64 {
    (x ^ 0x8000_0000_0000_0000u64) as i64
}

#[cfg(target_arch = "x86_64")]
pub fn select_lt_u64(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    // query < thr  <=>  thr > query
    if has_avx512f() {
        unsafe { select_lt_u64_avx512(records, query) }
    } else if has_avx2() {
        unsafe { select_lt_u64_avx2(records, query) }
    } else {
        select_lt_u64_scalar(records, query)
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn select_lt_u64(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    select_lt_u64_scalar(records, query)
}

#[cfg(target_arch = "x86_64")]
pub fn select_gt_u64(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    // query > thr  <=>  thr < query
    if has_avx512f() {
        unsafe { select_gt_u64_avx512(records, query) }
    } else if has_avx2() {
        unsafe { select_gt_u64_avx2(records, query) }
    } else {
        select_gt_u64_scalar(records, query)
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn select_gt_u64(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    select_gt_u64_scalar(records, query)
}

#[cfg(target_arch = "x86_64")]
pub fn select_eq_u64(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    if has_avx512f() {
        unsafe { select_eq_u64_avx512(records, query) }
    } else if has_avx2() {
        unsafe { select_eq_u64_avx2(records, query) }
    } else {
        select_eq_u64_scalar(records, query)
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn select_eq_u64(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    select_eq_u64_scalar(records, query)
}

fn select_lt_u64_scalar(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    let mut out = Vec::new();
    for (i, r) in records.iter().enumerate() {
        if query < threshold_u64(r) {
            out.push(i);
        }
    }
    out
}

fn select_gt_u64_scalar(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    let mut out = Vec::new();
    for (i, r) in records.iter().enumerate() {
        if query > threshold_u64(r) {
            out.push(i);
        }
    }
    out
}

fn select_eq_u64_scalar(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    let mut out = Vec::new();
    for (i, r) in records.iter().enumerate() {
        if query == threshold_u64(r) {
            out.push(i);
        }
    }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
unsafe fn select_lt_u64_avx2(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(records.len() / 8 + 8);
    let q = u64_ordered_to_i64(query);
    let qv = _mm256_set1_epi64x(q);
    const STRIDE: i32 = NumericRuleRecordDisk::BYTE_LEN as i32;
    const VALUE_OFF: i32 = 0;
    let idx = _mm_set_epi32(3 * STRIDE + VALUE_OFF, 2 * STRIDE + VALUE_OFF, 1 * STRIDE + VALUE_OFF, 0 * STRIDE + VALUE_OFF);
    let base_ptr = records.as_ptr() as *const u8;
    let sign_flip = _mm256_set1_epi64x(i64::MIN);

    let mut i = 0usize;
    while i + 4 <= records.len() {
        let ptr = base_ptr.add(i * NumericRuleRecordDisk::BYTE_LEN);
        let thr_raw = _mm256_i32gather_epi64(ptr as *const i64, idx, 1);
        let thr = _mm256_xor_si256(thr_raw, sign_flip);
        let thr_gt_q = _mm256_cmpgt_epi64(thr, qv);
        let mut mask = _mm256_movemask_pd(_mm256_castsi256_pd(thr_gt_q)) as u32;
        while mask != 0 { let lane = mask.trailing_zeros() as usize; out.push(i + lane); mask &= mask - 1; }
        i += 4;
    }
    for (j, r) in records[i..].iter().enumerate() {
        if query < threshold_u64(r) { out.push(i + j); }
    }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
unsafe fn select_gt_u64_avx2(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(records.len() / 8 + 8);
    let q = u64_ordered_to_i64(query);
    let qv = _mm256_set1_epi64x(q);
    const STRIDE: i32 = NumericRuleRecordDisk::BYTE_LEN as i32;
    const VALUE_OFF: i32 = 0;
    let idx = _mm_set_epi32(3 * STRIDE + VALUE_OFF, 2 * STRIDE + VALUE_OFF, 1 * STRIDE + VALUE_OFF, 0 * STRIDE + VALUE_OFF);
    let base_ptr = records.as_ptr() as *const u8;
    let sign_flip = _mm256_set1_epi64x(i64::MIN);

    let mut i = 0usize;
    while i + 4 <= records.len() {
        let ptr = base_ptr.add(i * NumericRuleRecordDisk::BYTE_LEN);
        let thr_raw = _mm256_i32gather_epi64(ptr as *const i64, idx, 1);
        let thr = _mm256_xor_si256(thr_raw, sign_flip);
        let q_gt_thr = _mm256_cmpgt_epi64(qv, thr);
        let mut mask = _mm256_movemask_pd(_mm256_castsi256_pd(q_gt_thr)) as u32;
        while mask != 0 { let lane = mask.trailing_zeros() as usize; out.push(i + lane); mask &= mask - 1; }
        i += 4;
    }
    for (j, r) in records[i..].iter().enumerate() { if query > threshold_u64(r) { out.push(i + j); } }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
unsafe fn select_eq_u64_avx2(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(records.len() / 8 + 8);
    let qv = _mm256_set1_epi64x(query as i64);
    const STRIDE: i32 = NumericRuleRecordDisk::BYTE_LEN as i32;
    const VALUE_OFF: i32 = 0;
    let idx = _mm_set_epi32(3 * STRIDE + VALUE_OFF, 2 * STRIDE + VALUE_OFF, 1 * STRIDE + VALUE_OFF, 0 * STRIDE + VALUE_OFF);
    let base_ptr = records.as_ptr() as *const u8;

    let mut i = 0usize;
    while i + 4 <= records.len() {
        let ptr = base_ptr.add(i * NumericRuleRecordDisk::BYTE_LEN);
        let thr = _mm256_i32gather_epi64(ptr as *const i64, idx, 1);
        let eq = _mm256_cmpeq_epi64(thr, qv);
        let mut mask = _mm256_movemask_pd(_mm256_castsi256_pd(eq)) as u32;
        while mask != 0 { let lane = mask.trailing_zeros() as usize; out.push(i + lane); mask &= mask - 1; }
        i += 4;
    }
    for (j, r) in records[i..].iter().enumerate() { if query == threshold_u64(r) { out.push(i + j); } }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx512f")]
unsafe fn select_lt_u64_avx512(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(records.len() / 8 + 8);
    let q = u64_ordered_to_i64(query);
    let qv = _mm512_set1_epi64(q);
    let mut i = 0usize;
    while i + 8 <= records.len() {
        let mut thr_arr = [0i64; 8];
        for lane in 0..8 { thr_arr[lane] = u64_ordered_to_i64(threshold_u64(&records[i + lane])); }
        let thr = _mm512_loadu_si512(thr_arr.as_ptr() as *const _);
        let mut mask: u32 = _mm512_cmpgt_epi64_mask(thr, qv) as u32;
        while mask != 0 { let lane = mask.trailing_zeros() as usize; out.push(i + lane); mask &= mask - 1; }
        i += 8;
    }
    for (j, r) in records[i..].iter().enumerate() { if query < threshold_u64(r) { out.push(i + j); } }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx512f")]
unsafe fn select_gt_u64_avx512(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(records.len() / 8 + 8);
    let q = u64_ordered_to_i64(query);
    let qv = _mm512_set1_epi64(q);
    let mut i = 0usize;
    while i + 8 <= records.len() {
        let mut thr_arr = [0i64; 8];
        for lane in 0..8 { thr_arr[lane] = u64_ordered_to_i64(threshold_u64(&records[i + lane])); }
        let thr = _mm512_loadu_si512(thr_arr.as_ptr() as *const _);
        let mut mask: u32 = _mm512_cmpgt_epi64_mask(qv, thr) as u32;
        while mask != 0 { let lane = mask.trailing_zeros() as usize; out.push(i + lane); mask &= mask - 1; }
        i += 8;
    }
    for (j, r) in records[i..].iter().enumerate() { if query > threshold_u64(r) { out.push(i + j); } }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx512f")]
unsafe fn select_eq_u64_avx512(records: &[NumericRuleRecordDisk], query: u64) -> Vec<usize> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(records.len() / 8 + 8);
    let qv = _mm512_set1_epi64(query as i64);
    let mut i = 0usize;
    while i + 8 <= records.len() {
        let mut thr_arr = [0i64; 8];
        for lane in 0..8 { thr_arr[lane] = threshold_u64(&records[i + lane]) as i64; }
        let thr = _mm512_loadu_si512(thr_arr.as_ptr() as *const _);
        let mut mask: u32 = _mm512_cmpeq_epi64_mask(thr, qv) as u32;
        while mask != 0 { let lane = mask.trailing_zeros() as usize; out.push(i + lane); mask &= mask - 1; }
        i += 8;
    }
    for (j, r) in records[i..].iter().enumerate() { if query == threshold_u64(r) { out.push(i + j); } }
    out
}

// ============================
// Float (f64) selectors
// ============================

#[cfg(target_arch = "x86_64")]
pub fn select_lt_f64(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    if has_avx512f() {
        unsafe { select_lt_f64_avx512(records, query) }
    } else if has_avx2() {
        unsafe { select_lt_f64_avx2(records, query) }
    } else {
        select_lt_f64_scalar(records, query)
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn select_lt_f64(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    select_lt_f64_scalar(records, query)
}

#[cfg(target_arch = "x86_64")]
pub fn select_gt_f64(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    if has_avx512f() {
        unsafe { select_gt_f64_avx512(records, query) }
    } else if has_avx2() {
        unsafe { select_gt_f64_avx2(records, query) }
    } else {
        select_gt_f64_scalar(records, query)
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn select_gt_f64(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    select_gt_f64_scalar(records, query)
}

#[cfg(target_arch = "x86_64")]
pub fn select_eq_f64(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    if has_avx512f() {
        unsafe { select_eq_f64_avx512(records, query) }
    } else if has_avx2() {
        unsafe { select_eq_f64_avx2(records, query) }
    } else {
        select_eq_f64_scalar(records, query)
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn select_eq_f64(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    select_eq_f64_scalar(records, query)
}

fn select_lt_f64_scalar(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    let mut out = Vec::new();
    for (i, r) in records.iter().enumerate() { if query < threshold_f64(r) { out.push(i); } }
    out
}

fn select_gt_f64_scalar(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    let mut out = Vec::new();
    for (i, r) in records.iter().enumerate() { if query > threshold_f64(r) { out.push(i); } }
    out
}

fn select_eq_f64_scalar(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    let mut out = Vec::new();
    let q_bits = query.to_bits();
    for (i, r) in records.iter().enumerate() { if q_bits == threshold_f64(r).to_bits() { out.push(i); } }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
unsafe fn select_lt_f64_avx2(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(records.len() / 8 + 8);
    let qv = _mm256_set1_pd(query);
    const STRIDE: i32 = NumericRuleRecordDisk::BYTE_LEN as i32;
    const VALUE_OFF: i32 = 0;
    let idx = _mm_set_epi32(3 * STRIDE + VALUE_OFF, 2 * STRIDE + VALUE_OFF, 1 * STRIDE + VALUE_OFF, 0 * STRIDE + VALUE_OFF);
    let base_ptr = records.as_ptr() as *const u8;
    let mut i = 0usize;
    while i + 4 <= records.len() {
        let ptr = base_ptr.add(i * NumericRuleRecordDisk::BYTE_LEN);
        let thr = _mm256_i32gather_pd(ptr as *const f64, idx, 1);
        let mut mask = _mm256_movemask_pd(_mm256_cmp_pd(thr, qv, _CMP_GT_OQ)) as u32; // thr > q
        while mask != 0 { let lane = mask.trailing_zeros() as usize; out.push(i + lane); mask &= mask - 1; }
        i += 4;
    }
    for (j, r) in records[i..].iter().enumerate() { if query < threshold_f64(r) { out.push(i + j); } }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
unsafe fn select_gt_f64_avx2(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(records.len() / 8 + 8);
    let qv = _mm256_set1_pd(query);
    const STRIDE: i32 = NumericRuleRecordDisk::BYTE_LEN as i32;
    const VALUE_OFF: i32 = 0;
    let idx = _mm_set_epi32(3 * STRIDE + VALUE_OFF, 2 * STRIDE + VALUE_OFF, 1 * STRIDE + VALUE_OFF, 0 * STRIDE + VALUE_OFF);
    let base_ptr = records.as_ptr() as *const u8;
    let mut i = 0usize;
    while i + 4 <= records.len() {
        let ptr = base_ptr.add(i * NumericRuleRecordDisk::BYTE_LEN);
        let thr = _mm256_i32gather_pd(ptr as *const f64, idx, 1);
        let mut mask = _mm256_movemask_pd(_mm256_cmp_pd(qv, thr, _CMP_GT_OQ)) as u32; // q > thr
        while mask != 0 { let lane = mask.trailing_zeros() as usize; out.push(i + lane); mask &= mask - 1; }
        i += 4;
    }
    for (j, r) in records[i..].iter().enumerate() { if query > threshold_f64(r) { out.push(i + j); } }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx2")]
unsafe fn select_eq_f64_avx2(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(records.len() / 8 + 8);
    let qv = _mm256_set1_pd(query);
    const STRIDE: i32 = NumericRuleRecordDisk::BYTE_LEN as i32;
    const VALUE_OFF: i32 = 0;
    let idx = _mm_set_epi32(3 * STRIDE + VALUE_OFF, 2 * STRIDE + VALUE_OFF, 1 * STRIDE + VALUE_OFF, 0 * STRIDE + VALUE_OFF);
    let base_ptr = records.as_ptr() as *const u8;
    let mut i = 0usize;
    while i + 4 <= records.len() {
        let ptr = base_ptr.add(i * NumericRuleRecordDisk::BYTE_LEN);
        let thr = _mm256_i32gather_pd(ptr as *const f64, idx, 1);
        let mut mask = _mm256_movemask_pd(_mm256_cmp_pd(thr, qv, _CMP_EQ_OQ)) as u32;
        while mask != 0 { let lane = mask.trailing_zeros() as usize; out.push(i + lane); mask &= mask - 1; }
        i += 4;
    }
    let q_bits = query.to_bits();
    for (j, r) in records[i..].iter().enumerate() { if q_bits == threshold_f64(r).to_bits() { out.push(i + j); } }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx512f")]
unsafe fn select_lt_f64_avx512(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(records.len() / 8 + 8);
    let qv = _mm512_set1_pd(query);
    let mut i = 0usize;
    while i + 8 <= records.len() {
        let mut thr_arr = [0f64; 8];
        for lane in 0..8 { thr_arr[lane] = threshold_f64(&records[i + lane]); }
        let thr = _mm512_loadu_pd(thr_arr.as_ptr());
        let mut mask: u32 = _mm512_cmp_pd_mask(thr, qv, _CMP_GT_OQ) as u32; // thr > q
        while mask != 0 { let lane = mask.trailing_zeros() as usize; out.push(i + lane); mask &= mask - 1; }
        i += 8;
    }
    for (j, r) in records[i..].iter().enumerate() { if query < threshold_f64(r) { out.push(i + j); } }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx512f")]
unsafe fn select_gt_f64_avx512(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(records.len() / 8 + 8);
    let qv = _mm512_set1_pd(query);
    let mut i = 0usize;
    while i + 8 <= records.len() {
        let mut thr_arr = [0f64; 8];
        for lane in 0..8 { thr_arr[lane] = threshold_f64(&records[i + lane]); }
        let thr = _mm512_loadu_pd(thr_arr.as_ptr());
        let mut mask: u32 = _mm512_cmp_pd_mask(qv, thr, _CMP_GT_OQ) as u32; // q > thr
        while mask != 0 { let lane = mask.trailing_zeros() as usize; out.push(i + lane); mask &= mask - 1; }
        i += 8;
    }
    for (j, r) in records[i..].iter().enumerate() { if query > threshold_f64(r) { out.push(i + j); } }
    out
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[target_feature(enable = "avx512f")]
unsafe fn select_eq_f64_avx512(records: &[NumericRuleRecordDisk], query: f64) -> Vec<usize> {
    use std::arch::x86_64::*;
    let mut out = Vec::with_capacity(records.len() / 8 + 8);
    let qv = _mm512_set1_pd(query);
    let mut i = 0usize;
    while i + 8 <= records.len() {
        let mut thr_arr = [0f64; 8];
        for lane in 0..8 { thr_arr[lane] = threshold_f64(&records[i + lane]); }
        let thr = _mm512_loadu_pd(thr_arr.as_ptr());
        let mut mask: u32 = _mm512_cmp_pd_mask(thr, qv, _CMP_EQ_OQ) as u32;
        while mask != 0 { let lane = mask.trailing_zeros() as usize; out.push(i + lane); mask &= mask - 1; }
        i += 8;
    }
    let q_bits = query.to_bits();
    for (j, r) in records[i..].iter().enumerate() { if q_bits == threshold_f64(r).to_bits() { out.push(i + j); } }
    out
}
