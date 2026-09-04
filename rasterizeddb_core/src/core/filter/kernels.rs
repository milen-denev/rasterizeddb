//! SIMD probe kernels for bit-sliced fingerprint planes.
//!
//! A zone holds `ZONE_ROWS = 512` rows as `f` **bitplanes** of 512 bits each.
//! Row `i`'s fingerprint is not contiguous: it is bit `i` of each of the `f`
//! planes. That layout is what lets one register test bit `j` of 512 rows at
//! once, and it is why `f = 5` costs exactly 5 bits per row with no padding.
//!
//! Probing value `g` against a zone is, per plane `j`:
//!
//! ```text
//! acc &= (bit j of g) ? plane[j] : !plane[j]
//! ```
//!
//! `acc` starts all-ones and ends as a 512-bit candidate mask. Because
//! `ZONE_ROWS / 64 == 8`, that mask is exactly 8 `u64` words and drops straight
//! into a slot bitmap with no bit shuffling.

use std::sync::OnceLock;

/// Rows covered by one zone. Load-bearing: 512 bits is exactly one cache line
/// and exactly one AVX-512 ZMM register.
pub const ZONE_ROWS: usize = 512;

/// `u64` words per bitplane.
pub const ZONE_WORDS: usize = ZONE_ROWS / 64;

/// Minimum fingerprint width. Below 4 the reserved-code fold (see
/// [`super::hash::fingerprint`]) leaves too few live codes to be useful.
pub const MIN_F: u8 = 4;

/// Maximum fingerprint width. 16 bits costs 2 B/row and gives ~1/65534 FPP.
pub const MAX_F: u8 = 16;

/// `planes` must be exactly `f * ZONE_WORDS` words.
pub type ProbeFn = fn(planes: &[u64], f: u8, g: u32, out: &mut [u64; ZONE_WORDS]);

/// Portable reference implementation. Also the correctness oracle the SIMD
/// paths are differentially tested against.
pub fn probe_zone_scalar(planes: &[u64], f: u8, g: u32, out: &mut [u64; ZONE_WORDS]) {
    // A real check, not a `debug_assert`: release builds turn assertions off
    // and the reads below are raw-pointer, so a short slice was an
    // out-of-bounds read there where the scalar oracle panics.
    assert_eq!(
        planes.len(),
        f as usize * ZONE_WORDS,
        "probe planes must be f * ZONE_WORDS words"
    );
    out.fill(u64::MAX);
    for j in 0..f as usize {
        // 0 when bit j of g is set (keep plane as-is), !0 when clear (invert it).
        let sel = if (g >> j) & 1 == 1 { 0u64 } else { u64::MAX };
        let base = j * ZONE_WORDS;
        for w in 0..ZONE_WORDS {
            out[w] &= planes[base + w] ^ sel;
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn probe_zone_avx2(planes: &[u64], f: u8, g: u32, out: &mut [u64; ZONE_WORDS]) {
    use std::arch::x86_64::*;
    // A real check, not a `debug_assert`: release builds turn assertions off
    // and the reads below are raw-pointer, so a short slice was an
    // out-of-bounds read there where the scalar oracle panics.
    assert_eq!(
        planes.len(),
        f as usize * ZONE_WORDS,
        "probe planes must be f * ZONE_WORDS words"
    );
    unsafe {
        let p = planes.as_ptr();
        let mut lo = _mm256_set1_epi64x(-1);
        let mut hi = _mm256_set1_epi64x(-1);

        for j in 0..f as usize {
            let sel = _mm256_set1_epi64x(if (g >> j) & 1 == 1 { 0 } else { -1 });
            let a = _mm256_loadu_si256(p.add(j * ZONE_WORDS) as *const __m256i);
            let b = _mm256_loadu_si256(p.add(j * ZONE_WORDS + 4) as *const __m256i);
            lo = _mm256_and_si256(lo, _mm256_xor_si256(a, sel));
            hi = _mm256_and_si256(hi, _mm256_xor_si256(b, sel));
        }

        let o = out.as_mut_ptr();
        _mm256_storeu_si256(o as *mut __m256i, lo);
        _mm256_storeu_si256(o.add(4) as *mut __m256i, hi);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn probe_zone_avx512(planes: &[u64], f: u8, g: u32, out: &mut [u64; ZONE_WORDS]) {
    use std::arch::x86_64::*;
    // A real check, not a `debug_assert`: release builds turn assertions off
    // and the reads below are raw-pointer, so a short slice was an
    // out-of-bounds read there where the scalar oracle panics.
    assert_eq!(
        planes.len(),
        f as usize * ZONE_WORDS,
        "probe planes must be f * ZONE_WORDS words"
    );
    unsafe {
        let p = planes.as_ptr();
        let mut acc = _mm512_set1_epi64(-1);

        for j in 0..f as usize {
            // Branchless: 0 or -1 from bit j of g. A branch here would be
            // unpredictable (it depends on the probed value's bits).
            let want = _mm512_set1_epi64((((g >> j) & 1) as i64).wrapping_neg());
            let plane = _mm512_loadu_si512(p.add(j * ZONE_WORDS) as *const __m512i);
            // 0x90 is the imm8 whose bit at index (4A + 2B + C) equals
            // A & (B XNOR C) — bits 4 and 7 set. Re-derived exhaustively in
            // `super::tests::ternlog_imm8_is_correct`, not taken on faith.
            acc = _mm512_ternarylogic_epi64::<0x90>(acc, plane, want);
        }

        _mm512_storeu_si512(out.as_mut_ptr() as *mut __m512i, acc);
    }
}

// `#[target_feature]` functions cannot be coerced to a plain `fn` pointer, so
// each gets a thin safe shim. The CPU check happens once in `probe_fn`; a shim
// is only ever installed after its feature has been confirmed present.
#[cfg(target_arch = "x86_64")]
fn probe_avx2_shim(planes: &[u64], f: u8, g: u32, out: &mut [u64; ZONE_WORDS]) {
    unsafe { probe_zone_avx2(planes, f, g, out) }
}

#[cfg(target_arch = "x86_64")]
fn probe_avx512_shim(planes: &[u64], f: u8, g: u32, out: &mut [u64; ZONE_WORDS]) {
    unsafe { probe_zone_avx512(planes, f, g, out) }
}

static PROBE: OnceLock<ProbeFn> = OnceLock::new();

/// Resolve the best probe kernel for this CPU, once.
#[inline]
pub fn probe_fn() -> ProbeFn {
    *PROBE.get_or_init(|| {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx512f") {
                return probe_avx512_shim as ProbeFn;
            }
            // AVX2 is guaranteed by the crate's `target-cpu=x86-64-v3` baseline,
            // but detect anyway so the binary stays correct if that changes.
            if is_x86_feature_detected!("avx2") {
                return probe_avx2_shim as ProbeFn;
            }
        }
        probe_zone_scalar as ProbeFn
    })
}

/// Name of the active kernel. For benchmarks and diagnostics.
pub fn active_kernel() -> &'static str {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f") {
            return "avx512f";
        }
        if is_x86_feature_detected!("avx2") {
            return "avx2";
        }
    }
    "scalar"
}

/// Write the `f`-bit fingerprint `g` into slot `local` of a zone's planes.
///
/// The same primitive serves insert, update and delete: delete simply writes
/// [`super::hash::CODE_EMPTY`], which makes the slot stop matching everything.
/// Unlike a Bloom filter this is exact — false-positive rate *improves*.
#[inline]
pub fn write_slot(planes: &mut [u64], f: u8, local: usize, g: u32) {
    debug_assert!(local < ZONE_ROWS);
    // A real check, not a `debug_assert`: release builds turn assertions off
    // and the reads below are raw-pointer, so a short slice was an
    // out-of-bounds read there where the scalar oracle panics.
    assert_eq!(
        planes.len(),
        f as usize * ZONE_WORDS,
        "probe planes must be f * ZONE_WORDS words"
    );
    let (w, b) = (local >> 6, local & 63);
    let m = 1u64 << b;
    for j in 0..f as usize {
        let word = &mut planes[j * ZONE_WORDS + w];
        if (g >> j) & 1 == 1 {
            *word |= m;
        } else {
            *word &= !m;
        }
    }
}

/// Read back the fingerprint stored at slot `local`. Used by tests and by
/// generation upgrades.
#[inline]
pub fn read_slot(planes: &[u64], f: u8, local: usize) -> u32 {
    debug_assert!(local < ZONE_ROWS);
    let (w, b) = (local >> 6, local & 63);
    let mut g = 0u32;
    for j in 0..f as usize {
        if (planes[j * ZONE_WORDS + w] >> b) & 1 == 1 {
            g |= 1 << j;
        }
    }
    g
}
