//! Compare a column of fixed-width values against one constant, many values per
//! instruction, producing a bitmask: bit `i` is set when `values[i] <op> k`.
//!
//! # The answer is the evaluator's, exactly
//!
//! These kernels feed a scan prefilter that skips rows before they are fetched,
//! so an answer that differs from `perform_comparison_operation` is a row that
//! silently goes missing. Integers have one order, so every level is exact for
//! free. Floats are the trap: the engine follows PostgreSQL, where NaN equals
//! NaN and sorts above every other value and -0 equals +0. IEEE comparisons say
//! NaN is unordered, so a plain `x > k` would drop every NaN row. The predicates
//! are picked to agree with PostgreSQL instead:
//!
//! | `x <op> k`, `k` not NaN | predicate | NaN `x` gives |
//! |---|---|---|
//! | `=`  | `EQ_OQ`  | false (NaN ≠ number) |
//! | `>`  | `NLE_UQ` | true  (NaN is largest) |
//! | `>=` | `NLT_UQ` | true |
//! | `<`  | `LT_OQ`  | false |
//! | `<=` | `LE_OQ`  | false |
//!
//! and with `k` NaN: `=` and `>=` are "x is NaN" (`UNORD_Q` of `x` with itself),
//! `<` is "x is not NaN" (`ORD_Q`), `>` is nothing and `<=` is everything.
//! `tests::every_level_matches_the_evaluator` holds each level to
//! `perform_comparison_operation` itself, NaN, ±0 and ±inf included.
//!
//! # The levels
//!
//! * **Scalar** is the reference.
//! * **AVX2** compares 32 bytes at a time and collapses each comparison to bits
//!   with `movemask`. AVX2 has no unsigned compare, so unsigned lanes are
//!   biased by their sign bit first, which maps unsigned order onto signed
//!   order. `>=` and `<=` are the complements of `<` and `>`, since AVX2 has
//!   only `cmpeq` and `cmpgt`. 16-bit lanes are packed with BMI2 `PEXT` where
//!   it is fast (Intel, AMD Zen 3+) and with `packs` elsewhere: see
//!   [`super::cpu::CpuFeatures::fast_pext`].
//! * **AVX-512** (F+BW+VL+DQ) compares 64 bytes at a time straight into a mask
//!   register, with native unsigned compares and all five operators. The tail
//!   is a masked load and a masked compare, so there is no scalar loop and no
//!   read past the end of the slice.
//!
//! [`compare`] routes through [`super::cpu::level`], which is fixed at startup.

use std::cmp::Ordering;

use super::cpu::{self, SimdLevel};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CmpOp {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
}

impl CmpOp {
    #[inline]
    fn holds(self, ord: Ordering) -> bool {
        match self {
            CmpOp::Eq => ord == Ordering::Equal,
            CmpOp::Gt => ord == Ordering::Greater,
            CmpOp::Ge => ord != Ordering::Less,
            CmpOp::Lt => ord == Ordering::Less,
            CmpOp::Le => ord != Ordering::Greater,
        }
    }
}

/// Words of mask for `n` values.
#[inline]
pub const fn words_for(n: usize) -> usize {
    n.div_ceil(64)
}

/// A value type the kernels compare. Sealed: each one needs its own kernels.
pub trait Lane: Copy + private::Sealed {
    /// [`compare`] at an explicit level. A level this CPU cannot run is lowered
    /// to the best one it can, so this is safe to call with any level.
    fn compare_at(level: SimdLevel, values: &[Self], op: CmpOp, k: Self, out: &mut [u64]);
}

mod private {
    pub trait Sealed {}
    impl Sealed for i8 {}
    impl Sealed for u8 {}
    impl Sealed for i16 {}
    impl Sealed for u16 {}
    impl Sealed for i32 {}
    impl Sealed for u32 {}
    impl Sealed for i64 {}
    impl Sealed for u64 {}
    impl Sealed for f32 {}
    impl Sealed for f64 {}
}

/// Set bit `i` of `out` when `values[i] <op> k`, for every `i < values.len()`.
///
/// Writes exactly `words_for(values.len())` words, with the bits past the last
/// value cleared; `out` must be at least that long.
#[inline]
pub fn compare<T: Lane>(values: &[T], op: CmpOp, k: T, out: &mut [u64]) {
    T::compare_at(cpu::level(), values, op, k, out)
}

#[inline]
fn clamp_level(level: SimdLevel) -> SimdLevel {
    level.min(cpu::features().best())
}

fn fill(out: &mut [u64], n: usize, value: bool) {
    let words = words_for(n);
    for w in out[..words].iter_mut() {
        *w = if value { u64::MAX } else { 0 };
    }
    if value && n % 64 != 0 {
        out[words - 1] = (1u64 << (n % 64)) - 1;
    }
}

/// PostgreSQL's float order: NaN equals NaN and is above everything; -0 = +0.
pub trait PgFloat: Copy {
    fn pg_cmp(self, other: Self) -> Ordering;
    fn nan(self) -> bool;
}

macro_rules! pg_float {
    ($t:ty) => {
        impl PgFloat for $t {
            #[inline]
            fn pg_cmp(self, other: Self) -> Ordering {
                match (self.is_nan(), other.is_nan()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    // Neither is NaN, so this is total; -0.0 == 0.0 here.
                    (false, false) => self.partial_cmp(&other).unwrap_or(Ordering::Equal),
                }
            }
            #[inline]
            fn nan(self) -> bool {
                self.is_nan()
            }
        }
    };
}
pg_float!(f32);
pg_float!(f64);

pub(crate) mod scalar {
    use super::*;

    #[inline]
    pub fn int_word<T: Ord + Copy>(chunk: &[T], op: CmpOp, k: T) -> u64 {
        let mut word = 0u64;
        for (i, v) in chunk.iter().enumerate() {
            word |= (op.holds(v.cmp(&k)) as u64) << i;
        }
        word
    }

    #[inline]
    pub fn float_word<T: PgFloat>(chunk: &[T], op: CmpOp, k: T) -> u64 {
        let mut word = 0u64;
        for (i, v) in chunk.iter().enumerate() {
            word |= (op.holds(v.pg_cmp(k)) as u64) << i;
        }
        word
    }

    pub fn ints<T: Ord + Copy>(values: &[T], op: CmpOp, k: T, out: &mut [u64]) {
        for (w, chunk) in values.chunks(64).enumerate() {
            out[w] = int_word(chunk, op, k);
        }
    }

    pub fn floats<T: PgFloat>(values: &[T], op: CmpOp, k: T, out: &mut [u64]) {
        for (w, chunk) in values.chunks(64).enumerate() {
            out[w] = float_word(chunk, op, k);
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
mod avx2 {
    use super::*;
    use std::arch::x86_64::*;

    const EQ: u8 = 0;
    const GT: u8 = 1;
    const LT: u8 = 2;

    // One comparison result to one bit per lane.
    macro_rules! pack8 {
        ($c:expr) => {
            _mm256_movemask_epi8($c) as u32 as u64
        };
    }
    macro_rules! pack32 {
        ($c:expr) => {
            _mm256_movemask_ps(_mm256_castsi256_ps($c)) as u32 as u64
        };
    }
    macro_rules! pack64 {
        ($c:expr) => {
            _mm256_movemask_pd(_mm256_castsi256_pd($c)) as u32 as u64
        };
    }
    // A 16-bit compare sets both bytes of a lane, so the byte mask has every
    // bit twice. `PEXT` keeps every other bit in one instruction.
    macro_rules! pack16_pext {
        ($c:expr) => {
            _pext_u32(_mm256_movemask_epi8($c) as u32, 0x5555_5555) as u64
        };
    }
    // Without a fast `PEXT`: saturate each lane to a byte. `packs` works per
    // 128-bit half, so packing the vector with itself leaves lanes 0..8 in
    // mask bits 0..8 and lanes 8..16 in bits 16..24.
    macro_rules! pack16_shuf {
        ($c:expr) => {{
            let m = _mm256_movemask_epi8(_mm256_packs_epi16($c, $c)) as u32;
            ((m & 0xFF) | ((m >> 8) & 0xFF00)) as u64
        }};
    }

    macro_rules! int_kernel {
        ($name:ident, $t:ty, $lanes:expr, $cast:ty, $bias:expr, $set1:ident,
         $cmpeq:ident, $cmpgt:ident, $pack:ident, $feat:literal) => {
            #[target_feature(enable = $feat)]
            unsafe fn $name<const KIND: u8, const NEG: bool>(
                values: &[$t],
                op: CmpOp,
                k: $t,
                out: &mut [u64],
            ) {
                let n = values.len();
                let full = n / 64;
                let bias = $set1($bias);
                let kv = _mm256_xor_si256($set1(k as $cast), bias);
                let p = values.as_ptr();
                for w in 0..full {
                    let mut word = 0u64;
                    let mut v = 0usize;
                    while v < 64 / $lanes {
                        let x = _mm256_loadu_si256(p.add(w * 64 + v * $lanes) as *const __m256i);
                        let x = _mm256_xor_si256(x, bias);
                        let c = match KIND {
                            EQ => $cmpeq(x, kv),
                            GT => $cmpgt(x, kv),
                            _ => $cmpgt(kv, x),
                        };
                        word |= $pack!(c) << (v * $lanes);
                        v += 1;
                    }
                    *out.get_unchecked_mut(w) = if NEG { !word } else { word };
                }
                if n % 64 != 0 {
                    out[full] = scalar::int_word(&values[full * 64..], op, k);
                }
            }
        };
    }

    int_kernel!(k_i8, i8, 32, i8, 0i8, _mm256_set1_epi8, _mm256_cmpeq_epi8, _mm256_cmpgt_epi8, pack8, "avx2");
    int_kernel!(k_u8, u8, 32, i8, i8::MIN, _mm256_set1_epi8, _mm256_cmpeq_epi8, _mm256_cmpgt_epi8, pack8, "avx2");
    int_kernel!(k_i16_pext, i16, 16, i16, 0i16, _mm256_set1_epi16, _mm256_cmpeq_epi16, _mm256_cmpgt_epi16, pack16_pext, "avx2,bmi2");
    int_kernel!(k_u16_pext, u16, 16, i16, i16::MIN, _mm256_set1_epi16, _mm256_cmpeq_epi16, _mm256_cmpgt_epi16, pack16_pext, "avx2,bmi2");
    int_kernel!(k_i16_shuf, i16, 16, i16, 0i16, _mm256_set1_epi16, _mm256_cmpeq_epi16, _mm256_cmpgt_epi16, pack16_shuf, "avx2");
    int_kernel!(k_u16_shuf, u16, 16, i16, i16::MIN, _mm256_set1_epi16, _mm256_cmpeq_epi16, _mm256_cmpgt_epi16, pack16_shuf, "avx2");
    int_kernel!(k_i32, i32, 8, i32, 0i32, _mm256_set1_epi32, _mm256_cmpeq_epi32, _mm256_cmpgt_epi32, pack32, "avx2");
    int_kernel!(k_u32, u32, 8, i32, i32::MIN, _mm256_set1_epi32, _mm256_cmpeq_epi32, _mm256_cmpgt_epi32, pack32, "avx2");
    int_kernel!(k_i64, i64, 4, i64, 0i64, _mm256_set1_epi64x, _mm256_cmpeq_epi64, _mm256_cmpgt_epi64, pack64, "avx2");
    int_kernel!(k_u64, u64, 4, i64, i64::MIN, _mm256_set1_epi64x, _mm256_cmpeq_epi64, _mm256_cmpgt_epi64, pack64, "avx2");

    // `>=` is "not <" and `<=` is "not >": AVX2 has only eq and gt.
    macro_rules! int_dispatch {
        ($kernel:ident, $values:expr, $op:expr, $k:expr, $out:expr) => {
            match $op {
                CmpOp::Eq => $kernel::<EQ, false>($values, $op, $k, $out),
                CmpOp::Gt => $kernel::<GT, false>($values, $op, $k, $out),
                CmpOp::Le => $kernel::<GT, true>($values, $op, $k, $out),
                CmpOp::Lt => $kernel::<LT, false>($values, $op, $k, $out),
                CmpOp::Ge => $kernel::<LT, true>($values, $op, $k, $out),
            }
        };
    }

    // NEG complements whole words, which sets the bits past `n` too. Only the
    // last word can have them, and only when it is a full one (a partial last
    // word comes from the scalar tail, which never sets them), so nothing
    // needs clearing.

    pub unsafe fn cmp_i8(v: &[i8], op: CmpOp, k: i8, out: &mut [u64]) { unsafe { int_dispatch!(k_i8, v, op, k, out) } }
    pub unsafe fn cmp_u8(v: &[u8], op: CmpOp, k: u8, out: &mut [u64]) { unsafe { int_dispatch!(k_u8, v, op, k, out) } }
    pub unsafe fn cmp_i16(v: &[i16], op: CmpOp, k: i16, out: &mut [u64], pext: bool) {
        unsafe {
            if pext { int_dispatch!(k_i16_pext, v, op, k, out) } else { int_dispatch!(k_i16_shuf, v, op, k, out) }
        }
    }
    pub unsafe fn cmp_u16(v: &[u16], op: CmpOp, k: u16, out: &mut [u64], pext: bool) {
        unsafe {
            if pext { int_dispatch!(k_u16_pext, v, op, k, out) } else { int_dispatch!(k_u16_shuf, v, op, k, out) }
        }
    }
    pub unsafe fn cmp_i32(v: &[i32], op: CmpOp, k: i32, out: &mut [u64]) { unsafe { int_dispatch!(k_i32, v, op, k, out) } }
    pub unsafe fn cmp_u32(v: &[u32], op: CmpOp, k: u32, out: &mut [u64]) { unsafe { int_dispatch!(k_u32, v, op, k, out) } }
    pub unsafe fn cmp_i64(v: &[i64], op: CmpOp, k: i64, out: &mut [u64]) { unsafe { int_dispatch!(k_i64, v, op, k, out) } }
    pub unsafe fn cmp_u64(v: &[u64], op: CmpOp, k: u64, out: &mut [u64]) { unsafe { int_dispatch!(k_u64, v, op, k, out) } }

    macro_rules! float_kernel {
        ($name:ident, $t:ty, $lanes:expr, $set1:ident, $loadu:ident, $cmp:ident, $movemask:ident) => {
            #[target_feature(enable = "avx2")]
            unsafe fn $name<const P: i32, const SELF: bool>(
                values: &[$t],
                op: CmpOp,
                k: $t,
                out: &mut [u64],
            ) {
                let n = values.len();
                let full = n / 64;
                let kv = $set1(k);
                let p = values.as_ptr();
                for w in 0..full {
                    let mut word = 0u64;
                    let mut v = 0usize;
                    while v < 64 / $lanes {
                        let x = $loadu(p.add(w * 64 + v * $lanes));
                        let c = $cmp::<P>(x, if SELF { x } else { kv });
                        word |= ($movemask(c) as u32 as u64) << (v * $lanes);
                        v += 1;
                    }
                    *out.get_unchecked_mut(w) = word;
                }
                if n % 64 != 0 {
                    out[full] = scalar::float_word(&values[full * 64..], op, k);
                }
            }
        };
    }

    float_kernel!(k_f32, f32, 8, _mm256_set1_ps, _mm256_loadu_ps, _mm256_cmp_ps, _mm256_movemask_ps);
    float_kernel!(k_f64, f64, 4, _mm256_set1_pd, _mm256_loadu_pd, _mm256_cmp_pd, _mm256_movemask_pd);

    pub unsafe fn cmp_f32(v: &[f32], op: CmpOp, k: f32, out: &mut [u64]) { unsafe { float_dispatch!(k_f32, v, op, k, out) } }
    pub unsafe fn cmp_f64(v: &[f64], op: CmpOp, k: f64, out: &mut [u64]) { unsafe { float_dispatch!(k_f64, v, op, k, out) } }
}

/// The PostgreSQL float predicates of the module docs, for one kernel family.
/// The NaN literal is decided here, not per lane.
#[cfg(target_arch = "x86_64")]
macro_rules! float_dispatch {
    ($kernel:ident, $values:expr, $op:expr, $k:expr, $out:expr) => {{
        use std::arch::x86_64::{_CMP_EQ_OQ, _CMP_LE_OQ, _CMP_LT_OQ, _CMP_NLE_UQ, _CMP_NLT_UQ, _CMP_ORD_Q, _CMP_UNORD_Q};
        if $k.is_nan() {
            match $op {
                CmpOp::Eq | CmpOp::Ge => $kernel::<_CMP_UNORD_Q, true>($values, $op, $k, $out),
                CmpOp::Lt => $kernel::<_CMP_ORD_Q, true>($values, $op, $k, $out),
                CmpOp::Gt => fill($out, $values.len(), false),
                CmpOp::Le => fill($out, $values.len(), true),
            }
        } else {
            match $op {
                CmpOp::Eq => $kernel::<_CMP_EQ_OQ, false>($values, $op, $k, $out),
                CmpOp::Gt => $kernel::<_CMP_NLE_UQ, false>($values, $op, $k, $out),
                CmpOp::Ge => $kernel::<_CMP_NLT_UQ, false>($values, $op, $k, $out),
                CmpOp::Lt => $kernel::<_CMP_LT_OQ, false>($values, $op, $k, $out),
                CmpOp::Le => $kernel::<_CMP_LE_OQ, false>($values, $op, $k, $out),
            }
        }
    }};
}
#[cfg(target_arch = "x86_64")]
use float_dispatch;

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
mod avx512 {
    use super::*;
    use std::arch::x86_64::*;

    macro_rules! int_kernel {
        ($name:ident, $t:ty, $lanes:expr, $mask:ty, $cast:ty, $set1:ident, $load:ident, $lt:ty, $cmp:ident) => {
            #[target_feature(enable = "avx512f,avx512bw,avx512vl,avx512dq")]
            unsafe fn $name<const P: i32>(values: &[$t], k: $t, out: &mut [u64]) {
                let n = values.len();
                let kv = $set1(k as $cast);
                let p = values.as_ptr();
                let mut i = 0usize;
                let mut w = 0usize;
                while i < n {
                    let mut word = 0u64;
                    let mut v = 0usize;
                    while v < 64 / $lanes {
                        let start = i + v * $lanes;
                        if start >= n {
                            break;
                        }
                        let rem = n - start;
                        // A partial last vector loads only its own lanes: the
                        // masked-off ones are neither read nor compared.
                        let lm: $mask = if rem >= $lanes {
                            <$mask>::MAX
                        } else {
                            ((1u64 << rem) - 1) as $mask
                        };
                        let x = $load(lm, p.add(start) as *const $lt);
                        let m = $cmp::<P>(lm, x, kv);
                        word |= (m as u64) << (v * $lanes);
                        v += 1;
                    }
                    *out.get_unchecked_mut(w) = word;
                    w += 1;
                    i += 64;
                }
            }
        };
    }

    int_kernel!(k_i8, i8, 64, __mmask64, i8, _mm512_set1_epi8, _mm512_maskz_loadu_epi8, i8, _mm512_mask_cmp_epi8_mask);
    int_kernel!(k_u8, u8, 64, __mmask64, i8, _mm512_set1_epi8, _mm512_maskz_loadu_epi8, i8, _mm512_mask_cmp_epu8_mask);
    int_kernel!(k_i16, i16, 32, __mmask32, i16, _mm512_set1_epi16, _mm512_maskz_loadu_epi16, i16, _mm512_mask_cmp_epi16_mask);
    int_kernel!(k_u16, u16, 32, __mmask32, i16, _mm512_set1_epi16, _mm512_maskz_loadu_epi16, i16, _mm512_mask_cmp_epu16_mask);
    int_kernel!(k_i32, i32, 16, __mmask16, i32, _mm512_set1_epi32, _mm512_maskz_loadu_epi32, i32, _mm512_mask_cmp_epi32_mask);
    int_kernel!(k_u32, u32, 16, __mmask16, i32, _mm512_set1_epi32, _mm512_maskz_loadu_epi32, i32, _mm512_mask_cmp_epu32_mask);
    int_kernel!(k_i64, i64, 8, __mmask8, i64, _mm512_set1_epi64, _mm512_maskz_loadu_epi64, i64, _mm512_mask_cmp_epi64_mask);
    int_kernel!(k_u64, u64, 8, __mmask8, i64, _mm512_set1_epi64, _mm512_maskz_loadu_epi64, i64, _mm512_mask_cmp_epu64_mask);

    // AVX-512 has every operator natively, signed and unsigned.
    macro_rules! int_dispatch {
        ($kernel:ident, $values:expr, $op:expr, $k:expr, $out:expr) => {
            match $op {
                CmpOp::Eq => $kernel::<_MM_CMPINT_EQ>($values, $k, $out),
                CmpOp::Gt => $kernel::<_MM_CMPINT_NLE>($values, $k, $out),
                CmpOp::Ge => $kernel::<_MM_CMPINT_NLT>($values, $k, $out),
                CmpOp::Lt => $kernel::<_MM_CMPINT_LT>($values, $k, $out),
                CmpOp::Le => $kernel::<_MM_CMPINT_LE>($values, $k, $out),
            }
        };
    }

    pub unsafe fn cmp_i8(v: &[i8], op: CmpOp, k: i8, out: &mut [u64]) { unsafe { int_dispatch!(k_i8, v, op, k, out) } }
    pub unsafe fn cmp_u8(v: &[u8], op: CmpOp, k: u8, out: &mut [u64]) { unsafe { int_dispatch!(k_u8, v, op, k, out) } }
    pub unsafe fn cmp_i16(v: &[i16], op: CmpOp, k: i16, out: &mut [u64]) { unsafe { int_dispatch!(k_i16, v, op, k, out) } }
    pub unsafe fn cmp_u16(v: &[u16], op: CmpOp, k: u16, out: &mut [u64]) { unsafe { int_dispatch!(k_u16, v, op, k, out) } }
    pub unsafe fn cmp_i32(v: &[i32], op: CmpOp, k: i32, out: &mut [u64]) { unsafe { int_dispatch!(k_i32, v, op, k, out) } }
    pub unsafe fn cmp_u32(v: &[u32], op: CmpOp, k: u32, out: &mut [u64]) { unsafe { int_dispatch!(k_u32, v, op, k, out) } }
    pub unsafe fn cmp_i64(v: &[i64], op: CmpOp, k: i64, out: &mut [u64]) { unsafe { int_dispatch!(k_i64, v, op, k, out) } }
    pub unsafe fn cmp_u64(v: &[u64], op: CmpOp, k: u64, out: &mut [u64]) { unsafe { int_dispatch!(k_u64, v, op, k, out) } }

    macro_rules! float_kernel {
        ($name:ident, $t:ty, $lanes:expr, $mask:ty, $set1:ident, $load:ident, $cmp:ident) => {
            #[target_feature(enable = "avx512f,avx512bw,avx512vl,avx512dq")]
            unsafe fn $name<const P: i32, const SELF: bool>(
                values: &[$t],
                _op: CmpOp,
                k: $t,
                out: &mut [u64],
            ) {
                let n = values.len();
                let kv = $set1(k);
                let p = values.as_ptr();
                let mut i = 0usize;
                let mut w = 0usize;
                while i < n {
                    let mut word = 0u64;
                    let mut v = 0usize;
                    while v < 64 / $lanes {
                        let start = i + v * $lanes;
                        if start >= n {
                            break;
                        }
                        let rem = n - start;
                        let lm: $mask = if rem >= $lanes {
                            <$mask>::MAX
                        } else {
                            ((1u64 << rem) - 1) as $mask
                        };
                        let x = $load(lm, p.add(start));
                        let m = $cmp::<P>(lm, x, if SELF { x } else { kv });
                        word |= (m as u64) << (v * $lanes);
                        v += 1;
                    }
                    *out.get_unchecked_mut(w) = word;
                    w += 1;
                    i += 64;
                }
            }
        };
    }

    float_kernel!(k_f32, f32, 16, __mmask16, _mm512_set1_ps, _mm512_maskz_loadu_ps, _mm512_mask_cmp_ps_mask);
    float_kernel!(k_f64, f64, 8, __mmask8, _mm512_set1_pd, _mm512_maskz_loadu_pd, _mm512_mask_cmp_pd_mask);

    pub unsafe fn cmp_f32(v: &[f32], op: CmpOp, k: f32, out: &mut [u64]) { unsafe { float_dispatch!(k_f32, v, op, k, out) } }
    pub unsafe fn cmp_f64(v: &[f64], op: CmpOp, k: f64, out: &mut [u64]) { unsafe { float_dispatch!(k_f64, v, op, k, out) } }
}

macro_rules! lane_int {
    ($t:ty, $f:ident) => {
        impl Lane for $t {
            #[inline]
            fn compare_at(level: SimdLevel, values: &[Self], op: CmpOp, k: Self, out: &mut [u64]) {
                let out = &mut out[..words_for(values.len())];
                match clamp_level(level) {
                    #[cfg(target_arch = "x86_64")]
                    SimdLevel::Avx512 => unsafe { avx512::$f(values, op, k, out) },
                    #[cfg(target_arch = "x86_64")]
                    SimdLevel::Avx2 => unsafe { avx2::$f(values, op, k, out) },
                    _ => scalar::ints(values, op, k, out),
                }
            }
        }
    };
}

lane_int!(i8, cmp_i8);
lane_int!(u8, cmp_u8);
lane_int!(i32, cmp_i32);
lane_int!(u32, cmp_u32);
lane_int!(i64, cmp_i64);
lane_int!(u64, cmp_u64);

macro_rules! lane_int16 {
    ($t:ty, $f:ident) => {
        impl Lane for $t {
            #[inline]
            fn compare_at(level: SimdLevel, values: &[Self], op: CmpOp, k: Self, out: &mut [u64]) {
                let out = &mut out[..words_for(values.len())];
                match clamp_level(level) {
                    #[cfg(target_arch = "x86_64")]
                    SimdLevel::Avx512 => unsafe { avx512::$f(values, op, k, out) },
                    #[cfg(target_arch = "x86_64")]
                    SimdLevel::Avx2 => unsafe {
                        avx2::$f(values, op, k, out, cpu::features().fast_pext())
                    },
                    _ => scalar::ints(values, op, k, out),
                }
            }
        }
    };
}

lane_int16!(i16, cmp_i16);
lane_int16!(u16, cmp_u16);

macro_rules! lane_float {
    ($t:ty, $f:ident) => {
        impl Lane for $t {
            #[inline]
            fn compare_at(level: SimdLevel, values: &[Self], op: CmpOp, k: Self, out: &mut [u64]) {
                let out = &mut out[..words_for(values.len())];
                match clamp_level(level) {
                    #[cfg(target_arch = "x86_64")]
                    SimdLevel::Avx512 => unsafe { avx512::$f(values, op, k, out) },
                    #[cfg(target_arch = "x86_64")]
                    SimdLevel::Avx2 => unsafe { avx2::$f(values, op, k, out) },
                    _ => scalar::floats(values, op, k, out),
                }
            }
        }
    };
}

lane_float!(f32, cmp_f32);
lane_float!(f64, cmp_f64);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db_type::DbType;
    use crate::core::processor::transformer::ComparerOperation;
    use crate::core::row::logical::perform_comparison_operation;

    const OPS: [CmpOp; 5] = [CmpOp::Eq, CmpOp::Gt, CmpOp::Ge, CmpOp::Lt, CmpOp::Le];

    fn comparer(op: CmpOp) -> ComparerOperation {
        match op {
            CmpOp::Eq => ComparerOperation::Equals,
            CmpOp::Gt => ComparerOperation::Greater,
            CmpOp::Ge => ComparerOperation::GreaterOrEquals,
            CmpOp::Lt => ComparerOperation::Less,
            CmpOp::Le => ComparerOperation::LessOrEquals,
        }
    }

    /// xorshift64*: deterministic, so a failure reproduces.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
    }

    /// Every runnable level, plus the AVX2 16-bit packing this CPU would not
    /// pick, so both get run wherever AVX2 exists.
    fn check<T: Lane + std::fmt::Debug>(
        values: &[T],
        constants: &[T],
        ty: DbType,
        bytes: impl Fn(T) -> Vec<u8>,
    ) {
        for &k in constants {
            for op in OPS {
                let kb = bytes(k);
                let mut expected = vec![0u64; words_for(values.len())];
                for (i, v) in values.iter().enumerate() {
                    let truth =
                        perform_comparison_operation(&bytes(*v), &kb, &ty, &comparer(op)).unwrap();
                    expected[i / 64] |= (truth as u64) << (i % 64);
                }
                for level in cpu::runnable_levels() {
                    // Poisoned, so a word the kernel forgot to write shows.
                    let mut got = vec![0xDEAD_BEEF_u64; words_for(values.len()) + 1];
                    T::compare_at(level, values, op, k, &mut got);
                    assert_eq!(
                        &got[..expected.len()],
                        &expected[..],
                        "{ty:?} {op:?} {k:?} at {level} over {} values",
                        values.len()
                    );
                    assert_eq!(got[expected.len()], 0xDEAD_BEEF, "wrote past the mask");
                }
            }
        }
    }

    macro_rules! int_case {
        ($name:ident, $t:ty, $ty:expr) => {
            #[test]
            fn $name() {
                let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ std::mem::size_of::<$t>() as u64);
                let edges: Vec<$t> = vec![<$t>::MIN, <$t>::MIN + 1, 0 as $t, 1 as $t, <$t>::MAX - 1, <$t>::MAX];
                // Every length through three words, to cover each tail shape.
                for len in (0..=200usize).chain([1000, 4097]) {
                    let mut values: Vec<$t> = (0..len)
                        .map(|_| match rng.next() % 4 {
                            // Clustered, so equality and the boundaries get hits.
                            0 => edges[(rng.next() % edges.len() as u64) as usize],
                            1 => (rng.next() % 7) as $t,
                            _ => rng.next() as $t,
                        })
                        .collect();
                    if len > 3 {
                        values[len / 2] = 3 as $t;
                    }
                    let mut constants = edges.clone();
                    constants.push(3 as $t);
                    constants.push(rng.next() as $t);
                    check(&values, &constants, $ty, |v: $t| v.to_le_bytes().to_vec());
                }
            }
        };
    }

    int_case!(i8_matches_the_evaluator, i8, DbType::I8);
    int_case!(u8_matches_the_evaluator, u8, DbType::U8);
    int_case!(i16_matches_the_evaluator, i16, DbType::I16);
    int_case!(u16_matches_the_evaluator, u16, DbType::U16);
    int_case!(i32_matches_the_evaluator, i32, DbType::I32);
    int_case!(u32_matches_the_evaluator, u32, DbType::U32);
    int_case!(i64_matches_the_evaluator, i64, DbType::I64);
    int_case!(u64_matches_the_evaluator, u64, DbType::U64);

    macro_rules! float_case {
        ($name:ident, $t:ty, $ty:expr) => {
            #[test]
            fn $name() {
                let mut rng = Rng(0xD1B5_4A32_D192_ED03 ^ std::mem::size_of::<$t>() as u64);
                let edges: Vec<$t> = vec![
                    <$t>::NAN,
                    -<$t>::NAN,
                    <$t>::INFINITY,
                    <$t>::NEG_INFINITY,
                    0.0,
                    -0.0,
                    1.0,
                    -1.0,
                    <$t>::MIN_POSITIVE,
                    <$t>::MAX,
                    <$t>::MIN,
                    2.5,
                ];
                for len in (0..=200usize).chain([1000, 4097]) {
                    let values: Vec<$t> = (0..len)
                        .map(|_| match rng.next() % 3 {
                            0 => edges[(rng.next() % edges.len() as u64) as usize],
                            _ => ((rng.next() % 2001) as i64 - 1000) as $t / 4.0,
                        })
                        .collect();
                    check(&values, &edges, $ty, |v: $t| v.to_le_bytes().to_vec());
                }
            }
        };
    }

    float_case!(f32_matches_the_evaluator, f32, DbType::F32);
    float_case!(f64_matches_the_evaluator, f64, DbType::F64);

    /// Both AVX2 packings of a 16-bit compare, whichever this CPU routes to.
    #[cfg(target_arch = "x86_64")]
    #[test]
    fn both_avx2_16_bit_packings_agree() {
        if !cpu::features().supports(SimdLevel::Avx2) || !cpu::features().bmi2 {
            return;
        }
        let mut rng = Rng(42);
        for len in [0usize, 1, 15, 16, 17, 63, 64, 65, 130, 1000] {
            let values: Vec<i16> = (0..len).map(|_| (rng.next() % 9) as i16 - 4).collect();
            let uvalues: Vec<u16> = values.iter().map(|v| *v as u16).collect();
            for op in OPS {
                for k in [-4i16, 0, 3, i16::MIN, i16::MAX] {
                    let mut a = vec![0u64; words_for(len)];
                    let mut b = vec![0u64; words_for(len)];
                    let mut c = vec![0u64; words_for(len)];
                    unsafe {
                        avx2::cmp_i16(&values, op, k, &mut a, true);
                        avx2::cmp_i16(&values, op, k, &mut b, false);
                    }
                    scalar::ints(&values, op, k, &mut c);
                    assert_eq!(a, c, "pext {op:?} {k}");
                    assert_eq!(b, c, "shuf {op:?} {k}");
                    unsafe {
                        avx2::cmp_u16(&uvalues, op, k as u16, &mut a, true);
                        avx2::cmp_u16(&uvalues, op, k as u16, &mut b, false);
                    }
                    scalar::ints(&uvalues, op, k as u16, &mut c);
                    assert_eq!(a, c, "pext u16 {op:?} {k}");
                    assert_eq!(b, c, "shuf u16 {op:?} {k}");
                }
            }
        }
    }

    /// Throughput per level, for the numbers in the docs:
    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// simd_compare_throughput --nocapture`.
    #[test]
    #[ignore]
    fn simd_compare_throughput() {
        use std::hint::black_box;
        use std::time::Instant;
        const N: usize = 10_000;
        const REPS: usize = 20_000;
        let mut rng = Rng(7);
        let i64s: Vec<i64> = (0..N).map(|_| rng.next() as i64).collect();
        let i32s: Vec<i32> = (0..N).map(|_| rng.next() as i32).collect();
        let f64s: Vec<f64> = (0..N).map(|_| (rng.next() % 1000) as f64).collect();
        let mut out = vec![0u64; words_for(N)];
        for level in cpu::runnable_levels() {
            let t = Instant::now();
            for _ in 0..REPS {
                i64::compare_at(level, black_box(&i64s), CmpOp::Gt, 0, &mut out);
                black_box(&out);
            }
            let a = t.elapsed().as_nanos() as f64 / (N * REPS) as f64;
            let t = Instant::now();
            for _ in 0..REPS {
                i32::compare_at(level, black_box(&i32s), CmpOp::Le, 0, &mut out);
                black_box(&out);
            }
            let b = t.elapsed().as_nanos() as f64 / (N * REPS) as f64;
            let t = Instant::now();
            for _ in 0..REPS {
                f64::compare_at(level, black_box(&f64s), CmpOp::Ge, 500.0, &mut out);
                black_box(&out);
            }
            let c = t.elapsed().as_nanos() as f64 / (N * REPS) as f64;
            println!("{level:>7}: i64 {a:.3} ns/value, i32 {b:.3}, f64 {c:.3}");
        }
    }
}
