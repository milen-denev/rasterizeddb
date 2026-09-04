use std::arch::x86_64::*;

use crate::core::processor::transformer::ComparerOperation;
use crate::memory_pool::MEMORY_POOL;
use crate::memory_pool::MemoryBlock;

// Helper trait to convert number types to/from little-endian byte slices
pub trait FromLeBytes: Sized {
    fn from_le_bytes(bytes: &[u8]) -> Self;
    fn to_le_bytes(&self) -> MemoryBlock;
}

pub trait FromLeBytesUnsafe: Sized {
    unsafe fn from_le_bytes_unchecked(bytes: &[u8]) -> Self;
}

macro_rules! impl_from_le_bytes {
    ($($type:ty),*) => {
        $(
            impl FromLeBytes for $type {
                #[inline(always)]
                #[track_caller]
                fn from_le_bytes(bytes: &[u8]) -> Self {
                    <$type>::from_le_bytes(bytes.try_into().expect("Invalid byte slice"))
                }

                #[track_caller]
                #[inline(always)]
                fn to_le_bytes(&self) -> MemoryBlock {
                    let len = std::mem::size_of::<$type>();
                    let mut memory = MEMORY_POOL.acquire(len);
                    let slice = memory.into_slice_mut();
                    slice.copy_from_slice(&(*self).to_le_bytes());
                    memory
                }
            }
        )*
    };
}

macro_rules! impl_from_le_bytes_unsafe {
    ($($type:ty),*) => {
        $(
            impl FromLeBytesUnsafe for $type {
                #[inline(always)]
                #[allow(unsafe_op_in_unsafe_fn)]
                #[track_caller]
                unsafe fn from_le_bytes_unchecked(bytes: &[u8]) -> Self {
                    // SAFETY: Caller must ensure bytes has correct length
                    let ptr = bytes.as_ptr() as *const $type;
                    ptr.read_unaligned().into()
                }
            }
        )*
    };
}

// Implement the trait for all required types
impl_from_le_bytes!(i8, u8, i16, u16, i32, u32, i64, u64, i128, u128, f32, f64);

// Implement the trait for all required types with unsafe methods
impl_from_le_bytes_unsafe!(i8, u8, i16, u16, i32, u32, i64, u64, i128, u128, f32, f64);

// Custom trait for converting from f64
pub trait FromF64: Sized {
    fn from_f64(value: f64) -> Self;
}

macro_rules! impl_from_f64 {
    ($($type:ty),*) => {
        $(
            impl FromF64 for $type {

                #[inline(always)]
                fn from_f64(value: f64) -> Self {
                    value.round() as $type
                }
            }
        )*
    };
}

// Implement the FromF64 trait for all numeric types
impl_from_f64!(i8, u8, i16, u16, i32, u32, i64, u64, i128, u128);

impl FromF64 for f32 {
    fn from_f64(value: f64) -> Self {
        value as f32
    }
}

impl FromF64 for f64 {
    fn from_f64(value: f64) -> Self {
        value
    }
}

// Custom trait for converting to f64
pub trait IntoF64 {
    fn into_f64(self) -> f64;
}

macro_rules! impl_into_f64 {
    ($($type:ty),*) => {
        $(
            impl IntoF64 for $type {

                #[inline(always)]
                fn into_f64(self) -> f64 {
                    self as f64
                }
            }
        )*
    };
}

// Implement the IntoF64 trait for all numeric types
impl_into_f64!(i8, u8, i16, u16, i32, u32, f32);

impl IntoF64 for i64 {
    fn into_f64(self) -> f64 {
        self as f64
    }
}

impl IntoF64 for u64 {
    fn into_f64(self) -> f64 {
        self as f64
    }
}

impl IntoF64 for i128 {
    fn into_f64(self) -> f64 {
        self as f64
    }
}

impl IntoF64 for u128 {
    fn into_f64(self) -> f64 {
        self as f64
    }
}

impl IntoF64 for f64 {
    fn into_f64(self) -> f64 {
        self
    }
}

/// Perform SIMD-based string comparisons on two slices of bytes (`&[u8]`).
/// The operation is determined by the `ComparerOperation` enum.
///
/// # Arguments
/// * `input1` - The first slice of bytes representing a string.
/// * `input2` - The second slice of bytes representing a string.
/// * `operation` - The comparison operation to perform.
///
/// # Returns
/// A `bool` indicating the result of the comparison.
///
/// # Panic
/// This function will panic if the input lengths are incompatible for the operation.
///
/// # Safety
/// This function uses unsafe Rust for SIMD intrinsics.
#[inline(always)]
pub fn simd_compare_strings(input1: &[u8], input2: &[u8], operation: &ComparerOperation) -> bool {
    match operation {
        ComparerOperation::Equals => unsafe { simd_equals(input1, input2) },
        ComparerOperation::NotEquals => !unsafe { simd_equals(input1, input2) },
        ComparerOperation::Contains => unsafe { simd_contains(input1, input2) },
        ComparerOperation::StartsWith => unsafe { simd_starts_with(input1, input2) },
        ComparerOperation::EndsWith => unsafe { simd_ends_with(input1, input2) },
        ComparerOperation::GreaterOrEquals => unsafe { simd_greater_or_equal(input1, input2) },
        // `simd_greater_or_equal` is a total lexicographic `>=`, so the other
        // three orderings follow from it exactly by negating or swapping the
        // operands. Deriving them costs one call each and reuses the tested
        // comparison rather than adding three more SIMD paths to keep in
        // agreement.
        //
        // These used to panic. `WHERE name < 'abc'` is ordinary SQL that any
        // ORM emits, and reaching it took down a query worker.
        ComparerOperation::Less => !unsafe { simd_greater_or_equal(input1, input2) },
        // a > b  <=>  !(a <= b)  <=>  !(b >= a)
        ComparerOperation::Greater => !unsafe { simd_greater_or_equal(input2, input1) },
        // a <= b  <=>  b >= a
        ComparerOperation::LessOrEquals => unsafe { simd_greater_or_equal(input2, input1) },
        ComparerOperation::EqualsCaseInsensitive => ci_equals(input1, input2),
        ComparerOperation::ContainsCaseInsensitive => ci_contains(input1, input2),
        ComparerOperation::StartsWithCaseInsensitive => ci_starts_with(input1, input2),
        ComparerOperation::EndsWithCaseInsensitive => ci_ends_with(input1, input2),
        ComparerOperation::Like => like_match(input1, input2, false),
        ComparerOperation::LikeCaseInsensitive => like_match(input1, input2, true),
        // `SIMILAR TO`: a pattern language of its own, matched in
        // `crate::core::similar_to`. A malformed pattern matches nothing here
        // — it was already reported when the predicate was parsed.
        ComparerOperation::SimilarTo => {
            match (std::str::from_utf8(input1), std::str::from_utf8(input2)) {
                (Ok(text), Ok(pattern)) => {
                    crate::core::similar_to::matches_default(text, pattern).unwrap_or(false)
                }
                _ => false,
            }
        }
        // A structural operator is not a string comparison. `@@` is the one
        // that can reach here — a `tsvector` is text — and it is answered by
        // the text-search matcher rather than by a byte compare.
        ComparerOperation::Pg { op, right_type } => crate::core::pg_ops::evaluate(
            *op,
            &crate::core::db_type::DbType::STRING,
            input1,
            right_type,
            input2,
        )
        .unwrap_or(false),
    }
}

// Case-insensitive comparisons, backing `ILIKE`.
//
// Folding is ASCII-only. Postgres folds according to the database collation,
// so a non-ASCII letter compares case-sensitively here where Postgres would
// match it. Handling the general case needs a Unicode case table, and these are
// scalar rather than SIMD because `ILIKE` is not on the hot path — the aim is a
// correct answer, not a fast wrong one.

#[inline]
fn ci_equals(input1: &[u8], input2: &[u8]) -> bool {
    input1.len() == input2.len()
        && input1
            .iter()
            .zip(input2.iter())
            .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

#[inline]
fn ci_starts_with(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.len() >= needle.len() && ci_equals(&haystack[..needle.len()], needle)
}

#[inline]
fn ci_ends_with(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.len() >= needle.len() && ci_equals(&haystack[haystack.len() - needle.len()..], needle)
}

#[inline]
fn ci_contains(haystack: &[u8], needle: &[u8]) -> bool {
    // The empty needle is contained in everything, matching `simd_contains`
    // and SQL's `x ILIKE '%%'`.
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| ci_equals(window, needle))
}

#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn simd_greater_or_equal(input1: &[u8], input2: &[u8]) -> bool {
    let len1 = input1.len();
    let len2 = input2.len();
    let min_len = len1.min(len2);

    let ptr1 = input1.as_ptr();
    let ptr2 = input2.as_ptr();

    let mut i = 0;

    // AVX2 path (process 32 bytes at a time)
    // Assumes AVX2 support is enabled at compile time for these intrinsics to be valid.
    if min_len.saturating_sub(i) >= 32 {
        let v_sign_bit_256 = _mm256_set1_epi8(0x80u8 as i8);
        while i + 32 <= min_len {
            let v1_256 = _mm256_loadu_si256(ptr1.add(i) as *const __m256i);
            let v2_256 = _mm256_loadu_si256(ptr2.add(i) as *const __m256i);

            // Check for equality first
            let eq_cmp_256 = _mm256_cmpeq_epi8(v1_256, v2_256);
            let eq_mask_256 = _mm256_movemask_epi8(eq_cmp_256) as u32;

            if eq_mask_256 != 0xFFFFFFFFu32 {
                // If not all bytes in the vector are equal
                // Transform to signed for correct unsigned comparison using signed greater-than
                // (unsigned a > unsigned b) is equivalent to (signed (a^0x80) > signed (b^0x80))
                let s1_256 = _mm256_xor_si256(v1_256, v_sign_bit_256);
                let s2_256 = _mm256_xor_si256(v2_256, v_sign_bit_256);

                let gt_cmp_256 = _mm256_cmpgt_epi8(s1_256, s2_256); // v1 > v2 (lexicographically)
                let gt_mask_256 = _mm256_movemask_epi8(gt_cmp_256) as u32;

                // Find the first differing byte
                // (eq_mask_256 ^ 0xFFFFFFFFu32) gives a mask of differing bytes
                let diff_mask = eq_mask_256 ^ 0xFFFFFFFFu32;
                let first_diff_bit_idx = diff_mask.trailing_zeros(); // Index (0-31) of the first differing byte

                // Check if at this first differing position, v1 was greater than v2
                return (gt_mask_256 >> first_diff_bit_idx) & 1 != 0;
            }
            i += 32;
        }
    }

    // SSE2 path (process 16 bytes at a time for remaining)
    // Assumes SSE2 support is enabled at compile time.
    if min_len.saturating_sub(i) >= 16 {
        let v_sign_bit_128 = _mm_set1_epi8(0x80u8 as i8);
        while i + 16 <= min_len {
            let v1_128 = _mm_loadu_si128(ptr1.add(i) as *const __m128i);
            let v2_128 = _mm_loadu_si128(ptr2.add(i) as *const __m128i);

            let eq_cmp_128 = _mm_cmpeq_epi8(v1_128, v2_128);
            let eq_mask_128 = _mm_movemask_epi8(eq_cmp_128) as u16;

            if eq_mask_128 != 0xFFFFu16 {
                // If not all bytes in the vector are equal
                let s1_128 = _mm_xor_si128(v1_128, v_sign_bit_128);
                let s2_128 = _mm_xor_si128(v2_128, v_sign_bit_128);

                let gt_cmp_128 = _mm_cmpgt_epi8(s1_128, s2_128); // v1 > v2 (lexicographically)
                let gt_mask_128 = _mm_movemask_epi8(gt_cmp_128) as u16;

                let diff_mask = eq_mask_128 ^ 0xFFFFu16;
                let first_diff_bit_idx = diff_mask.trailing_zeros(); // Index (0-15) of the first differing byte

                return (gt_mask_128 >> first_diff_bit_idx) & 1 != 0;
            }
            i += 16;
        }
    }

    // Scalar path for remaining bytes (if any, or if SIMD paths didn't cover all of min_len)
    while i < min_len {
        let b1 = *ptr1.add(i);
        let b2 = *ptr2.add(i);
        if b1 > b2 {
            return true;
        }
        if b1 < b2 {
            return false;
        }
        i += 1;
    }

    // If all common bytes are equal, the comparison depends on the lengths.
    // e.g., "abc" >= "ab" is true. "ab" >= "abc" is false.
    return len1 >= len2;
}

/// SIMD-based implementation of string equality (`==`) for `&[u8]`.
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn simd_equals(input1: &[u8], input2: &[u8]) -> bool {
    let len = input1.len();
    if len != input2.len() {
        return false;
    }

    let ptr1 = input1.as_ptr();
    let ptr2 = input2.as_ptr();

    // Fast path for very small strings (0-16 bytes)
    if len <= 16 {
        return compare_small(ptr1, ptr2, len);
    }

    // Medium strings path (16-64 bytes)
    if len <= 64 {
        return compare_medium(ptr1, ptr2, len);
    }

    // Large strings path (> 64 bytes)
    compare_large(ptr1, ptr2, len)
}

/// Equality for 0..=16 byte values — the length most string columns actually
/// hold (a name, a department, a status code).
///
/// Uses two overlapping loads per side instead of a byte-at-a-time loop, so the
/// whole comparison is a constant handful of instructions regardless of length.
/// The loop this replaced made a 16-byte compare measurably *slower* than a
/// 32-byte one, which took the SSE path.
///
/// Overlapping is what keeps it branch-light: for any length in a size class,
/// the head load and the tail load together cover every byte, with the middle
/// bytes simply compared twice.
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn compare_small(ptr1: *const u8, ptr2: *const u8, len: usize) -> bool {
    #[inline(always)]
    unsafe fn load_u64(p: *const u8) -> u64 {
        (p as *const u64).read_unaligned()
    }
    #[inline(always)]
    unsafe fn load_u32(p: *const u8) -> u32 {
        (p as *const u32).read_unaligned()
    }

    if len >= 8 {
        // 8..=16 bytes: head and tail 8-byte words cover everything.
        let a1 = load_u64(ptr1);
        let b1 = load_u64(ptr2);
        let a2 = load_u64(ptr1.add(len - 8));
        let b2 = load_u64(ptr2.add(len - 8));
        return ((a1 ^ b1) | (a2 ^ b2)) == 0;
    }

    if len >= 4 {
        // 4..=7 bytes.
        let a1 = load_u32(ptr1);
        let b1 = load_u32(ptr2);
        let a2 = load_u32(ptr1.add(len - 4));
        let b2 = load_u32(ptr2.add(len - 4));
        return ((a1 ^ b1) | (a2 ^ b2)) == 0;
    }

    if len >= 2 {
        // 2..=3 bytes.
        let a1 = (ptr1 as *const u16).read_unaligned();
        let b1 = (ptr2 as *const u16).read_unaligned();
        let a2 = (ptr1.add(len - 2) as *const u16).read_unaligned();
        let b2 = (ptr2.add(len - 2) as *const u16).read_unaligned();
        return ((a1 ^ b1) | (a2 ^ b2)) == 0;
    }

    if len == 1 {
        // Kept as a single byte compare; folding it into the 2..=3 case cost
        // more than it saved for single-character values.
        return *ptr1 == *ptr2;
    }

    true
}

/// Equality for 17..=64 byte values.
///
/// Same overlapping-load idea as `compare_small`, one size class up: a pair of
/// vector loads per side covers the whole value, so there is no loop and no
/// scalar tail to clean up.
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn compare_medium(ptr1: *const u8, ptr2: *const u8, len: usize) -> bool {
    if len <= 32 {
        // 17..=32 bytes: two overlapping 16-byte loads per side.
        let a1 = _mm_loadu_si128(ptr1 as *const __m128i);
        let b1 = _mm_loadu_si128(ptr2 as *const __m128i);
        let a2 = _mm_loadu_si128(ptr1.add(len - 16) as *const __m128i);
        let b2 = _mm_loadu_si128(ptr2.add(len - 16) as *const __m128i);

        let diff = _mm_or_si128(_mm_xor_si128(a1, b1), _mm_xor_si128(a2, b2));
        return _mm_testz_si128(diff, diff) != 0;
    }

    // 33..=64 bytes: two overlapping 32-byte loads per side.
    let a1 = _mm256_loadu_si256(ptr1 as *const __m256i);
    let b1 = _mm256_loadu_si256(ptr2 as *const __m256i);
    let a2 = _mm256_loadu_si256(ptr1.add(len - 32) as *const __m256i);
    let b2 = _mm256_loadu_si256(ptr2.add(len - 32) as *const __m256i);

    let diff = _mm256_or_si256(_mm256_xor_si256(a1, b1), _mm256_xor_si256(a2, b2));
    _mm256_testz_si256(diff, diff) != 0
}

#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn compare_large(ptr1: *const u8, ptr2: *const u8, len: usize) -> bool {
    let mut i = 0;
    const STRIDE: usize = 64;

    // Process 64 bytes at a time using AVX2
    while i + STRIDE <= len {
        let a1 = _mm256_loadu_si256(ptr1.add(i) as *const __m256i);
        let a2 = _mm256_loadu_si256(ptr1.add(i + 32) as *const __m256i);
        let b1 = _mm256_loadu_si256(ptr2.add(i) as *const __m256i);
        let b2 = _mm256_loadu_si256(ptr2.add(i + 32) as *const __m256i);

        let x1 = _mm256_xor_si256(a1, b1);
        let x2 = _mm256_xor_si256(a2, b2);
        let x = _mm256_or_si256(x1, x2);

        // If any byte differs, testz will return 0
        if _mm256_testz_si256(x, x) == 0 {
            return false;
        }

        i += STRIDE;
    }

    // Handle remaining 32 bytes if any
    if i + 32 <= len {
        let a = _mm256_loadu_si256(ptr1.add(i) as *const __m256i);
        let b = _mm256_loadu_si256(ptr2.add(i) as *const __m256i);
        let x = _mm256_xor_si256(a, b);
        if _mm256_testz_si256(x, x) == 0 {
            return false;
        }
        i += 32;
    }

    // Handle remaining 16 bytes if any
    if i + 16 <= len {
        let a = _mm_loadu_si128(ptr1.add(i) as *const __m128i);
        let b = _mm_loadu_si128(ptr2.add(i) as *const __m128i);
        let cmp = _mm_cmpeq_epi8(a, b);
        let mask = _mm_movemask_epi8(cmp);
        if mask != 0xFFFF {
            return false;
        }
        i += 16;
    }

    // Fallback for remaining bytes
    while i < len {
        if *ptr1.add(i) != *ptr2.add(i) {
            return false;
        }
        i += 1;
    }

    true
}

/// SIMD-based implementation to check if `input1` contains `input2`.
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn simd_contains(haystack: &[u8], needle: &[u8]) -> bool {
    let n = haystack.len();
    let m = needle.len();

    if m == 0 {
        return false; // Empty needle is not found by convention
    }

    if m > n {
        return false; // Needle larger than haystack
    }

    match m {
        1 => contains_single_byte(haystack, needle[0]),
        2..=8 => contains_small(haystack, needle),
        _ => contains_big(haystack, needle),
    }
}

/// Optimized search for single-byte needles
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn contains_single_byte(haystack: &[u8], needle_byte: u8) -> bool {
    let n = haystack.len();
    let v_needle = _mm256_set1_epi8(needle_byte as i8);

    let mut i = 0;
    // Process 32 bytes at a time
    while i + 32 <= n {
        let chunk = _mm256_loadu_si256(haystack.as_ptr().add(i) as *const __m256i);
        let cmp = _mm256_cmpeq_epi8(chunk, v_needle);
        let mask = _mm256_movemask_epi8(cmp);
        if mask != 0 {
            return true;
        }
        i += 32;
    }

    // Process 16 bytes at a time
    if i + 16 <= n {
        let chunk = _mm_loadu_si128(haystack.as_ptr().add(i) as *const __m128i);
        let needle_xmm = _mm_set1_epi8(needle_byte as i8);
        let cmp = _mm_cmpeq_epi8(chunk, needle_xmm);
        let mask = _mm_movemask_epi8(cmp);
        if mask != 0 {
            return true;
        }
        i += 16;
    }

    // Tail
    for &b in &haystack[i..] {
        if b == needle_byte {
            return true;
        }
    }
    false
}

/// Compares the interior bytes (`1..m-1`) of a candidate match.
///
/// The first and last bytes are already known to match when this is called, and
/// `m <= 8` here, so this is at most six byte compares.
///
/// # Safety
/// `h` must point at `m` readable bytes and `n` at the `m`-byte needle.
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn middle_matches(h: *const u8, n: *const u8, m: usize) -> bool {
    let mut j = 1;
    while j + 1 < m {
        if *h.add(j) != *n.add(j) {
            return false;
        }
        j += 1;
    }
    true
}

/// Optimized search for small needles (2-8 bytes).
///
/// Scans 32 candidate positions per iteration by testing the needle's first and
/// last byte simultaneously: one vector compare against the block starting at
/// `i` and another against the block starting at `i + m - 1`, ANDed together.
/// Only positions where *both* bytes line up reach the interior comparison,
/// which for a 2-byte needle is already an exact answer.
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn contains_small(haystack: &[u8], needle: &[u8]) -> bool {
    let n = haystack.len();
    let m = needle.len();
    // Callers guarantee `0 < m <= n`.
    let n_minus_m = n - m;

    let hp = haystack.as_ptr();
    let np = needle.as_ptr();

    let first_byte = *np;
    let last_byte = *np.add(m - 1);

    let mut i = 0usize;

    // AVX2: 32 candidate start positions per iteration.
    // The guard `i + 31 <= n_minus_m` keeps both 32-byte loads (the one at `i`
    // and the one at `i + m - 1`) fully inside the haystack.
    if n_minus_m >= 31 {
        let v_first = _mm256_set1_epi8(first_byte as i8);
        let v_last = _mm256_set1_epi8(last_byte as i8);

        while i + 31 <= n_minus_m {
            let block_first = _mm256_loadu_si256(hp.add(i) as *const __m256i);
            let block_last = _mm256_loadu_si256(hp.add(i + m - 1) as *const __m256i);

            let eq = _mm256_and_si256(
                _mm256_cmpeq_epi8(block_first, v_first),
                _mm256_cmpeq_epi8(block_last, v_last),
            );

            let mut mask = _mm256_movemask_epi8(eq) as u32;
            while mask != 0 {
                let offset = mask.trailing_zeros() as usize;
                if middle_matches(hp.add(i + offset), np, m) {
                    return true;
                }
                mask &= mask.wrapping_sub(1);
            }

            i += 32;
        }
    }

    // SSE2: 16 candidate start positions per iteration.
    if i + 15 <= n_minus_m {
        let v_first = _mm_set1_epi8(first_byte as i8);
        let v_last = _mm_set1_epi8(last_byte as i8);

        while i + 15 <= n_minus_m {
            let block_first = _mm_loadu_si128(hp.add(i) as *const __m128i);
            let block_last = _mm_loadu_si128(hp.add(i + m - 1) as *const __m128i);

            let eq = _mm_and_si128(
                _mm_cmpeq_epi8(block_first, v_first),
                _mm_cmpeq_epi8(block_last, v_last),
            );

            let mut mask = _mm_movemask_epi8(eq) as u32;
            while mask != 0 {
                let offset = mask.trailing_zeros() as usize;
                if middle_matches(hp.add(i + offset), np, m) {
                    return true;
                }
                mask &= mask.wrapping_sub(1);
            }

            i += 16;
        }
    }

    // Scalar tail for the last (< 16) candidate positions.
    while i <= n_minus_m {
        if *hp.add(i) == first_byte
            && *hp.add(i + m - 1) == last_byte
            && middle_matches(hp.add(i), np, m)
        {
            return true;
        }
        i += 1;
    }

    false
}

/// Substring search for needles of 9 bytes or more.
///
/// Filters candidate positions on the needle's first *and* last byte at once,
/// the same way `contains_small` does. Filtering on the first byte alone (what
/// this replaced) sends every position whose first byte matches into a full
/// comparison — on text where that byte is common, most of that work is thrown
/// away. Requiring both bytes cuts survivors by roughly another factor of 256.
///
/// Both size classes share this: the only thing that differed between the old
/// medium and large versions was the vector width of the first-byte scan, and
/// this walks 32 positions at a time for both.
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn contains_big(haystack: &[u8], needle: &[u8]) -> bool {
    let n = haystack.len();
    let m = needle.len();
    // Callers guarantee `0 < m <= n`.
    let n_minus_m = n - m;

    let hp = haystack.as_ptr();
    let first_byte = *needle.get_unchecked(0);
    let last_byte = *needle.get_unchecked(m - 1);

    let mut i = 0usize;

    // AVX2: 32 candidate start positions per iteration. The guard keeps both
    // loads (at `i` and at `i + m - 1`) inside the haystack.
    if n_minus_m >= 31 {
        let v_first = _mm256_set1_epi8(first_byte as i8);
        let v_last = _mm256_set1_epi8(last_byte as i8);

        while i + 31 <= n_minus_m {
            let block_first = _mm256_loadu_si256(hp.add(i) as *const __m256i);
            let block_last = _mm256_loadu_si256(hp.add(i + m - 1) as *const __m256i);

            let eq = _mm256_and_si256(
                _mm256_cmpeq_epi8(block_first, v_first),
                _mm256_cmpeq_epi8(block_last, v_last),
            );

            let mut mask = _mm256_movemask_epi8(eq) as u32;
            while mask != 0 {
                let pos = i + mask.trailing_zeros() as usize;
                if simd_equals(std::slice::from_raw_parts(hp.add(pos), m), needle) {
                    return true;
                }
                mask &= mask.wrapping_sub(1);
            }

            i += 32;
        }
    }

    // SSE2: 16 candidate start positions per iteration.
    if i + 15 <= n_minus_m {
        let v_first = _mm_set1_epi8(first_byte as i8);
        let v_last = _mm_set1_epi8(last_byte as i8);

        while i + 15 <= n_minus_m {
            let block_first = _mm_loadu_si128(hp.add(i) as *const __m128i);
            let block_last = _mm_loadu_si128(hp.add(i + m - 1) as *const __m128i);

            let eq = _mm_and_si128(
                _mm_cmpeq_epi8(block_first, v_first),
                _mm_cmpeq_epi8(block_last, v_last),
            );

            let mut mask = _mm_movemask_epi8(eq) as u32;
            while mask != 0 {
                let pos = i + mask.trailing_zeros() as usize;
                if simd_equals(std::slice::from_raw_parts(hp.add(pos), m), needle) {
                    return true;
                }
                mask &= mask.wrapping_sub(1);
            }

            i += 16;
        }
    }

    // Scalar tail for the last (< 16) candidate positions.
    while i <= n_minus_m {
        if *hp.add(i) == first_byte
            && *hp.add(i + m - 1) == last_byte
            && simd_equals(std::slice::from_raw_parts(hp.add(i), m), needle)
        {
            return true;
        }
        i += 1;
    }

    false
}

/// SIMD-based implementation to check if `input1` starts with `input2`.
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn simd_starts_with(input1: &[u8], input2: &[u8]) -> bool {
    let prefix_len = input2.len();
    if prefix_len > input1.len() {
        return false;
    }

    simd_equals(&input1[..prefix_len], input2)
}

/// SIMD-based implementation to check if `input1` ends with `input2`.
#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn simd_ends_with(input1: &[u8], input2: &[u8]) -> bool {
    let suffix_len = input2.len();
    if suffix_len > input1.len() {
        return false;
    }

    simd_equals(&input1[input1.len() - suffix_len..], input2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_compare_strings_equals() {
        let input1 = "hello".as_bytes();
        let input2 = "hello".as_bytes();
        let input3 = "world".as_bytes();

        assert!(simd_compare_strings(
            input1,
            input2,
            &ComparerOperation::Equals
        ));
        assert!(!simd_compare_strings(
            input1,
            input3,
            &ComparerOperation::Equals
        ));
    }

    #[test]
    fn test_simd_compare_strings_not_equals() {
        let input1 = "hello".as_bytes();
        let input2 = "world".as_bytes();

        assert!(simd_compare_strings(
            input1,
            input2,
            &ComparerOperation::NotEquals
        ));
        assert!(!simd_compare_strings(
            input1,
            input1,
            &ComparerOperation::NotEquals
        ));
    }

    #[test]
    fn test_simd_compare_strings_contains() {
        let input1 = "hello world".as_bytes();
        let input2 = "world".as_bytes();
        let input3 = "rust".as_bytes();

        assert!(simd_compare_strings(
            input1,
            input2,
            &ComparerOperation::Contains
        ));
        assert!(!simd_compare_strings(
            input1,
            input3,
            &ComparerOperation::Contains
        ));
    }

    #[test]
    fn test_simd_compare_strings_starts_with() {
        let input1 = "hello world".as_bytes();
        let input2 = "hello".as_bytes();
        let input3 = "world".as_bytes();

        assert!(simd_compare_strings(
            input1,
            input2,
            &ComparerOperation::StartsWith
        ));
        assert!(!simd_compare_strings(
            input1,
            input3,
            &ComparerOperation::StartsWith
        ));
    }

    #[test]
    fn test_simd_compare_strings_ends_with() {
        let input1 = "hello world".as_bytes();
        let input2 = "world".as_bytes();
        let input3 = "hello".as_bytes();

        assert!(simd_compare_strings(
            input1,
            input2,
            &ComparerOperation::EndsWith
        ));
        assert!(!simd_compare_strings(
            input1,
            input3,
            &ComparerOperation::EndsWith
        ));
    }

    /// Every `ComparerOperation` is now handled for strings. This used to
    /// assert that the ordered ones panicked, which made `WHERE name < 'abc'`
    /// a way to kill a query worker from ordinary SQL.
    #[test]
    fn test_simd_compare_strings_handles_every_operation() {
        let a = "hello".as_bytes();
        let b = "world".as_bytes();

        assert!(!simd_compare_strings(a, b, &ComparerOperation::Greater));
        assert!(simd_compare_strings(b, a, &ComparerOperation::Greater));
        assert!(simd_compare_strings(a, b, &ComparerOperation::Less));
        assert!(!simd_compare_strings(b, a, &ComparerOperation::Less));
        assert!(simd_compare_strings(a, b, &ComparerOperation::LessOrEquals));
        assert!(simd_compare_strings(a, a, &ComparerOperation::LessOrEquals));
        assert!(simd_compare_strings(
            a,
            a,
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(simd_compare_strings(a, a, &ComparerOperation::Equals));
        assert!(simd_compare_strings(a, b, &ComparerOperation::NotEquals));
        assert!(simd_compare_strings(
            a,
            "ell".as_bytes(),
            &ComparerOperation::Contains
        ));
        assert!(simd_compare_strings(
            a,
            "he".as_bytes(),
            &ComparerOperation::StartsWith
        ));
        assert!(simd_compare_strings(
            a,
            "lo".as_bytes(),
            &ComparerOperation::EndsWith
        ));
    }

    #[test]
    fn test_simd_compare_strings_equals_edge_cases() {
        // Empty strings
        assert!(simd_compare_strings(
            "".as_bytes(),
            "".as_bytes(),
            &ComparerOperation::Equals
        ));
        assert!(!simd_compare_strings(
            "a".as_bytes(),
            "".as_bytes(),
            &ComparerOperation::Equals
        ));
        assert!(!simd_compare_strings(
            "".as_bytes(),
            "a".as_bytes(),
            &ComparerOperation::Equals
        ));

        // Single char strings
        assert!(simd_compare_strings(
            "a".as_bytes(),
            "a".as_bytes(),
            &ComparerOperation::Equals
        ));
        assert!(!simd_compare_strings(
            "a".as_bytes(),
            "b".as_bytes(),
            &ComparerOperation::Equals
        ));

        // Different lengths
        let s31_1: String = (0..31).map(|_| 'a').collect();
        let s31_2: String = (0..31).map(|_| 'a').collect();
        let s31_3: String = (0..30).map(|_| 'a').collect::<String>() + "b";
        assert!(simd_compare_strings(
            s31_1.as_bytes(),
            s31_2.as_bytes(),
            &ComparerOperation::Equals
        ));
        assert!(!simd_compare_strings(
            s31_1.as_bytes(),
            s31_3.as_bytes(),
            &ComparerOperation::Equals
        ));

        let s32_1: String = (0..32).map(|_| 'a').collect();
        let s32_2: String = (0..32).map(|_| 'a').collect();
        let s32_3: String = (0..31).map(|_| 'a').collect::<String>() + "b";
        assert!(simd_compare_strings(
            s32_1.as_bytes(),
            s32_2.as_bytes(),
            &ComparerOperation::Equals
        ));
        assert!(!simd_compare_strings(
            s32_1.as_bytes(),
            s32_3.as_bytes(),
            &ComparerOperation::Equals
        ));

        let s33_1: String = (0..33).map(|_| 'a').collect();
        let s33_2: String = (0..33).map(|_| 'a').collect();
        let s33_3: String = (0..32).map(|_| 'a').collect::<String>() + "b";
        assert!(simd_compare_strings(
            s33_1.as_bytes(),
            s33_2.as_bytes(),
            &ComparerOperation::Equals
        ));
        assert!(!simd_compare_strings(
            s33_1.as_bytes(),
            s33_3.as_bytes(),
            &ComparerOperation::Equals
        ));

        let s63_1: String = (0..63).map(|_| 'x').collect();
        let s63_2: String = (0..63).map(|_| 'x').collect();
        let s63_3: String = (0..62).map(|_| 'x').collect::<String>() + "y";
        assert!(simd_compare_strings(
            s63_1.as_bytes(),
            s63_2.as_bytes(),
            &ComparerOperation::Equals
        ));
        assert!(!simd_compare_strings(
            s63_1.as_bytes(),
            s63_3.as_bytes(),
            &ComparerOperation::Equals
        ));

        let s64_1: String = (0..64).map(|_| 'x').collect();
        let s64_2: String = (0..64).map(|_| 'x').collect();
        let s64_3: String = (0..63).map(|_| 'x').collect::<String>() + "y";
        assert!(simd_compare_strings(
            s64_1.as_bytes(),
            s64_2.as_bytes(),
            &ComparerOperation::Equals
        ));
        assert!(!simd_compare_strings(
            s64_1.as_bytes(),
            s64_3.as_bytes(),
            &ComparerOperation::Equals
        ));

        let s65_1: String = (0..65).map(|_| 'x').collect();
        let s65_2: String = (0..65).map(|_| 'x').collect();
        let s65_3: String = (0..64).map(|_| 'x').collect::<String>() + "y";
        assert!(simd_compare_strings(
            s65_1.as_bytes(),
            s65_2.as_bytes(),
            &ComparerOperation::Equals
        ));
        assert!(!simd_compare_strings(
            s65_1.as_bytes(),
            s65_3.as_bytes(),
            &ComparerOperation::Equals
        ));

        // Long string, diff at start, mid, end
        let long_a: Vec<u8> = vec![b'a'; 100];
        let mut long_b = long_a.clone();
        assert!(simd_compare_strings(
            &long_a,
            &long_b,
            &ComparerOperation::Equals
        ));

        long_b[0] = b'b'; // Diff at start
        assert!(!simd_compare_strings(
            &long_a,
            &long_b,
            &ComparerOperation::Equals
        ));
        long_b[0] = b'a';

        long_b[50] = b'b'; // Diff at mid
        assert!(!simd_compare_strings(
            &long_a,
            &long_b,
            &ComparerOperation::Equals
        ));
        long_b[50] = b'a';

        long_b[99] = b'b'; // Diff at end
        assert!(!simd_compare_strings(
            &long_a,
            &long_b,
            &ComparerOperation::Equals
        ));
    }

    #[test]
    fn test_simd_compare_strings_contains_edge_cases() {
        // Empty needle (current behavior is false)
        assert!(!simd_compare_strings(
            "abc".as_bytes(),
            "".as_bytes(),
            &ComparerOperation::Contains
        ));
        assert!(!simd_compare_strings(
            "".as_bytes(),
            "".as_bytes(),
            &ComparerOperation::Contains
        ));

        // Empty haystack
        assert!(!simd_compare_strings(
            "".as_bytes(),
            "a".as_bytes(),
            &ComparerOperation::Contains
        ));

        // Needle longer than haystack
        assert!(!simd_compare_strings(
            "a".as_bytes(),
            "ab".as_bytes(),
            &ComparerOperation::Contains
        ));

        // Single byte needle
        assert!(simd_compare_strings(
            "abc".as_bytes(),
            "a".as_bytes(),
            &ComparerOperation::Contains
        )); // Start
        assert!(simd_compare_strings(
            "abc".as_bytes(),
            "b".as_bytes(),
            &ComparerOperation::Contains
        )); // Mid
        assert!(simd_compare_strings(
            "abc".as_bytes(),
            "c".as_bytes(),
            &ComparerOperation::Contains
        )); // End
        assert!(!simd_compare_strings(
            "abc".as_bytes(),
            "d".as_bytes(),
            &ComparerOperation::Contains
        )); // Not found
        assert!(simd_compare_strings(
            "aaaaa".as_bytes(),
            "a".as_bytes(),
            &ComparerOperation::Contains
        ));
        let long_hay_single_needle: String =
            (0..70).map(|i| if i == 35 { 'b' } else { 'a' }).collect();
        assert!(simd_compare_strings(
            long_hay_single_needle.as_bytes(),
            "b".as_bytes(),
            &ComparerOperation::Contains
        ));
        assert!(!simd_compare_strings(
            long_hay_single_needle.as_bytes(),
            "c".as_bytes(),
            &ComparerOperation::Contains
        ));

        // Multi-byte needle
        let haystack1 = "hello world".as_bytes();
        assert!(simd_compare_strings(
            haystack1,
            "hello".as_bytes(),
            &ComparerOperation::Contains
        )); // Start
        assert!(simd_compare_strings(
            haystack1,
            "lo wo".as_bytes(),
            &ComparerOperation::Contains
        )); // Mid
        assert!(simd_compare_strings(
            haystack1,
            "world".as_bytes(),
            &ComparerOperation::Contains
        )); // End
        assert!(!simd_compare_strings(
            haystack1,
            "rust".as_bytes(),
            &ComparerOperation::Contains
        )); // Not found
        assert!(simd_compare_strings(
            haystack1,
            haystack1,
            &ComparerOperation::Contains
        )); // Needle is haystack

        // Needle length 2 (testing LAST_CHAR_CHECK_MIN_LEN boundary)
        assert!(simd_compare_strings(
            "abcdef".as_bytes(),
            "ab".as_bytes(),
            &ComparerOperation::Contains
        ));
        assert!(simd_compare_strings(
            "abcdef".as_bytes(),
            "cd".as_bytes(),
            &ComparerOperation::Contains
        ));
        assert!(simd_compare_strings(
            "abcdef".as_bytes(),
            "ef".as_bytes(),
            &ComparerOperation::Contains
        ));
        assert!(!simd_compare_strings(
            "abcdef".as_bytes(),
            "ax".as_bytes(),
            &ComparerOperation::Contains
        ));
        assert!(!simd_compare_strings(
            "abcdef".as_bytes(),
            "xb".as_bytes(),
            &ComparerOperation::Contains
        ));

        // First char matches, last char matches, middle differs
        assert!(!simd_compare_strings(
            "ab_cde_fg".as_bytes(),
            "a_X_g".as_bytes(),
            &ComparerOperation::Contains
        ));
        assert!(simd_compare_strings(
            "ab_cde_fg".as_bytes(),
            "cde".as_bytes(),
            &ComparerOperation::Contains
        ));

        // First char matches, last char differs (testing optimization)
        assert!(!simd_compare_strings(
            "apple_banana_orange".as_bytes(),
            "applf".as_bytes(),
            &ComparerOperation::Contains
        )); // apple vs applf
        assert!(simd_compare_strings(
            "apple_banana_orange".as_bytes(),
            "apple".as_bytes(),
            &ComparerOperation::Contains
        ));

        // Haystack/needle lengths for loop logic
        let short_hay = "short".as_bytes();
        assert!(simd_compare_strings(
            short_hay,
            "sh".as_bytes(),
            &ComparerOperation::Contains
        ));
        assert!(simd_compare_strings(
            short_hay,
            "ort".as_bytes(),
            &ComparerOperation::Contains
        ));
        assert!(!simd_compare_strings(
            short_hay,
            "longneedle".as_bytes(),
            &ComparerOperation::Contains
        ));

        let long_hay: String = (0..100).map(|i| (b'a' + (i % 26) as u8) as char).collect();
        assert!(simd_compare_strings(
            long_hay.as_bytes(),
            "abc".as_bytes(),
            &ComparerOperation::Contains
        ));
        assert!(simd_compare_strings(
            long_hay.as_bytes(),
            "xyz".as_bytes(),
            &ComparerOperation::Contains
        )); // Assuming 'xyz' is part of the pattern
        assert!(simd_compare_strings(
            long_hay.as_bytes(),
            &long_hay.as_bytes()[50..55],
            &ComparerOperation::Contains
        )); // Substring from middle
        assert!(!simd_compare_strings(
            long_hay.as_bytes(),
            "123".as_bytes(),
            &ComparerOperation::Contains
        ));

        // Test case where first char match is near end of a 32-byte block, but needle extends into next
        let h_boundary = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab_defg".as_bytes(); // 'b' at index 31
        assert!(simd_compare_strings(
            h_boundary,
            "b_d".as_bytes(),
            &ComparerOperation::Contains
        ));
        assert!(!simd_compare_strings(
            h_boundary,
            "b_x".as_bytes(),
            &ComparerOperation::Contains
        ));

        // Test case where first char match is in scalar tail, needle fits
        let h_tail_match = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaXabcde".as_bytes(); // 'X' at 32
        assert!(simd_compare_strings(
            h_tail_match,
            "abc".as_bytes(),
            &ComparerOperation::Contains
        ));
        assert!(!simd_compare_strings(
            h_tail_match,
            "abx".as_bytes(),
            &ComparerOperation::Contains
        ));

        // Test case where first char match is in scalar tail, but needle doesn't fit
        assert!(!simd_compare_strings(
            "aaXab".as_bytes(),
            "Xabc".as_bytes(),
            &ComparerOperation::Contains
        ));

        // Test case specifically for mask = i32::MIN (match at the 31st byte of a 32-byte chunk)
        let mut thirty_one_chars = String::new();
        for i in 0..31 {
            thirty_one_chars.push((b'0' + (i % 10) as u8) as char);
        }
        let h_i32_min_mask = (thirty_one_chars.clone() + "N" + "eedleTail_MoreData").into_bytes(); // 'N' is at index 31
        let n_i32_min_mask = "NeedleTail".as_bytes();
        assert!(simd_compare_strings(
            &h_i32_min_mask,
            n_i32_min_mask,
            &ComparerOperation::Contains
        ));

        let h_i32_min_mask_no_match_end =
            (thirty_one_chars.clone() + "N" + "eedleFail").into_bytes();
        assert!(!simd_compare_strings(
            &h_i32_min_mask_no_match_end,
            n_i32_min_mask,
            &ComparerOperation::Contains
        ));

        let h_i32_min_mask_short_needle = (thirty_one_chars + "N" + "T_MoreData").into_bytes(); // 'N' at 31
        let n_i32_min_mask_short_needle = "NT".as_bytes();
        assert!(simd_compare_strings(
            &h_i32_min_mask_short_needle,
            n_i32_min_mask_short_needle,
            &ComparerOperation::Contains
        ));
    }

    #[test]
    fn test_simd_compare_strings_starts_with_edge_cases() {
        let s = "hello world".as_bytes();
        // Empty prefix
        assert!(simd_compare_strings(
            s,
            "".as_bytes(),
            &ComparerOperation::StartsWith
        )); // Standard library behavior
        assert!(simd_compare_strings(
            "".as_bytes(),
            "".as_bytes(),
            &ComparerOperation::StartsWith
        ));

        // Prefix longer than string
        assert!(!simd_compare_strings(
            "hi".as_bytes(),
            "hello".as_bytes(),
            &ComparerOperation::StartsWith
        ));

        // Prefix is string itself
        assert!(simd_compare_strings(s, s, &ComparerOperation::StartsWith));

        // Various lengths
        assert!(simd_compare_strings(
            s,
            "h".as_bytes(),
            &ComparerOperation::StartsWith
        ));
        assert!(simd_compare_strings(
            s,
            "he".as_bytes(),
            &ComparerOperation::StartsWith
        ));
        assert!(simd_compare_strings(
            s,
            "hello".as_bytes(),
            &ComparerOperation::StartsWith
        ));
        assert!(!simd_compare_strings(
            s,
            "world".as_bytes(),
            &ComparerOperation::StartsWith
        ));

        let s31: String = (0..31).map(|_| 'a').collect();
        assert!(simd_compare_strings(
            s31.as_bytes(),
            s31.as_bytes(),
            &ComparerOperation::StartsWith
        ));
        assert!(simd_compare_strings(
            (s31.clone() + "b").as_bytes(),
            s31.as_bytes(),
            &ComparerOperation::StartsWith
        ));

        let s32: String = (0..32).map(|_| 'b').collect();
        assert!(simd_compare_strings(
            s32.as_bytes(),
            s32.as_bytes(),
            &ComparerOperation::StartsWith
        ));
        assert!(simd_compare_strings(
            (s32.clone() + "c").as_bytes(),
            s32.as_bytes(),
            &ComparerOperation::StartsWith
        ));

        let s64: String = (0..64).map(|_| 'x').collect();
        assert!(simd_compare_strings(
            s64.as_bytes(),
            s64.as_bytes(),
            &ComparerOperation::StartsWith
        ));
        assert!(simd_compare_strings(
            (s64.clone() + "y").as_bytes(),
            s64.as_bytes(),
            &ComparerOperation::StartsWith
        ));
    }

    #[test]
    fn test_simd_compare_strings_ends_with_edge_cases() {
        let s = "hello world".as_bytes();
        // Empty suffix
        assert!(simd_compare_strings(
            s,
            "".as_bytes(),
            &ComparerOperation::EndsWith
        )); // Standard library behavior
        assert!(simd_compare_strings(
            "".as_bytes(),
            "".as_bytes(),
            &ComparerOperation::EndsWith
        ));

        // Suffix longer than string
        assert!(!simd_compare_strings(
            "hi".as_bytes(),
            "world".as_bytes(),
            &ComparerOperation::EndsWith
        ));

        // Suffix is string itself
        assert!(simd_compare_strings(s, s, &ComparerOperation::EndsWith));

        // Various lengths
        assert!(simd_compare_strings(
            s,
            "d".as_bytes(),
            &ComparerOperation::EndsWith
        ));
        assert!(simd_compare_strings(
            s,
            "ld".as_bytes(),
            &ComparerOperation::EndsWith
        ));
        assert!(simd_compare_strings(
            s,
            "world".as_bytes(),
            &ComparerOperation::EndsWith
        ));
        assert!(!simd_compare_strings(
            s,
            "hello".as_bytes(),
            &ComparerOperation::EndsWith
        ));

        let s31: String = (0..31).map(|_| 'a').collect();
        assert!(simd_compare_strings(
            s31.as_bytes(),
            s31.as_bytes(),
            &ComparerOperation::EndsWith
        ));
        assert!(simd_compare_strings(
            ("b".to_string() + &s31).as_bytes(),
            s31.as_bytes(),
            &ComparerOperation::EndsWith
        ));

        let s32: String = (0..32).map(|_| 'b').collect();
        assert!(simd_compare_strings(
            s32.as_bytes(),
            s32.as_bytes(),
            &ComparerOperation::EndsWith
        ));
        assert!(simd_compare_strings(
            ("c".to_string() + &s32).as_bytes(),
            s32.as_bytes(),
            &ComparerOperation::EndsWith
        ));

        let s64: String = (0..64).map(|_| 'x').collect();
        assert!(simd_compare_strings(
            s64.as_bytes(),
            s64.as_bytes(),
            &ComparerOperation::EndsWith
        ));
        assert!(simd_compare_strings(
            ("y".to_string() + &s64).as_bytes(),
            s64.as_bytes(),
            &ComparerOperation::EndsWith
        ));
    }

    #[test]
    fn test_simd_compare_strings_greater_or_equals() {
        // Basic cases
        assert!(simd_compare_strings(
            "hello".as_bytes(),
            "hello".as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(simd_compare_strings(
            "world".as_bytes(),
            "hello".as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(!simd_compare_strings(
            "hello".as_bytes(),
            "world".as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));

        // Length differences
        assert!(simd_compare_strings(
            "abc".as_bytes(),
            "ab".as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(!simd_compare_strings(
            "ab".as_bytes(),
            "abc".as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(simd_compare_strings(
            "abc".as_bytes(),
            "abc".as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));

        // Empty strings
        assert!(simd_compare_strings(
            "".as_bytes(),
            "".as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(simd_compare_strings(
            "a".as_bytes(),
            "".as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(!simd_compare_strings(
            "".as_bytes(),
            "a".as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));

        // Different casing (lexicographical comparison)
        assert!(simd_compare_strings(
            "zebra".as_bytes(),
            "apple".as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(simd_compare_strings(
            "apple".as_bytes(),
            "Apple".as_bytes(),
            &ComparerOperation::GreaterOrEquals
        )); // 'a' > 'A'
        assert!(!simd_compare_strings(
            "Apple".as_bytes(),
            "apple".as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));

        // Test AVX2 path (strings > 32 bytes)
        let s33_a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".as_bytes(); // 33 'a's
        let s33_b = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab".as_bytes(); // 32 'a's and 1 'b'
        let s33_c = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".as_bytes(); // 33 'a's
        assert!(!simd_compare_strings(
            s33_a,
            s33_b,
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(simd_compare_strings(
            s33_b,
            s33_a,
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(simd_compare_strings(
            s33_a,
            s33_c,
            &ComparerOperation::GreaterOrEquals
        ));

        let s65_a: String = (0..65).map(|_| 'a').collect();
        let mut s65_b_vec: Vec<u8> = (0..65).map(|_| b'a').collect();
        s65_b_vec[64] = b'b'; // last char different
        let s65_b = s65_b_vec.as_slice();
        assert!(!simd_compare_strings(
            s65_a.as_bytes(),
            s65_b,
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(simd_compare_strings(
            s65_b,
            s65_a.as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(simd_compare_strings(
            s65_a.as_bytes(),
            s65_a.as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));

        let mut s65_c_vec: Vec<u8> = (0..65).map(|_| b'a').collect();
        s65_c_vec[30] = b'b'; // char in first AVX block different
        let s65_c = s65_c_vec.as_slice();
        assert!(!simd_compare_strings(
            s65_a.as_bytes(),
            s65_c,
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(simd_compare_strings(
            s65_c,
            s65_a.as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));

        // Test SSE2 path (strings > 16 and <=32 bytes, or remainder after AVX2)
        let s17_a = "aaaaaaaaaaaaaaaaa".as_bytes(); // 17 'a's
        let s17_b = "aaaaaaaaaaaaaaaab".as_bytes(); // 16 'a's and 1 'b'
        let s17_c = "aaaaaaaaaaaaaaaaa".as_bytes(); // 17 'a's
        assert!(!simd_compare_strings(
            s17_a,
            s17_b,
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(simd_compare_strings(
            s17_b,
            s17_a,
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(simd_compare_strings(
            s17_a,
            s17_c,
            &ComparerOperation::GreaterOrEquals
        ));

        let s32_a: String = (0..32).map(|_| 'x').collect();
        let mut s32_b_vec: Vec<u8> = (0..32).map(|_| b'x').collect();
        s32_b_vec[31] = b'y';
        let s32_b = s32_b_vec.as_slice();
        assert!(!simd_compare_strings(
            s32_a.as_bytes(),
            s32_b,
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(simd_compare_strings(
            s32_b,
            s32_a.as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(simd_compare_strings(
            s32_a.as_bytes(),
            s32_a.as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));

        let mut s32_c_vec: Vec<u8> = (0..32).map(|_| b'x').collect();
        s32_c_vec[10] = b'y'; // char in first SSE block different
        let s32_c = s32_c_vec.as_slice();
        assert!(!simd_compare_strings(
            s32_a.as_bytes(),
            s32_c,
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(simd_compare_strings(
            s32_c,
            s32_a.as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));

        // Test scalar path (strings <= 16 bytes, or remainder after SIMD)
        assert!(
            simd_compare_strings(
                "apple".as_bytes(),
                "apply".as_bytes(),
                &ComparerOperation::GreaterOrEquals
            ) == ("apple" >= "apply")
        );
        assert!(
            simd_compare_strings(
                "apply".as_bytes(),
                "apple".as_bytes(),
                &ComparerOperation::GreaterOrEquals
            ) == ("apply" >= "apple")
        );
        assert!(
            simd_compare_strings(
                "test".as_bytes(),
                "testing".as_bytes(),
                &ComparerOperation::GreaterOrEquals
            ) == ("test" >= "testing")
        );
        assert!(
            simd_compare_strings(
                "testing".as_bytes(),
                "test".as_bytes(),
                &ComparerOperation::GreaterOrEquals
            ) == ("testing" >= "test")
        );

        // Test strings that are prefixes of each other
        let prefix_long = "abcdefghijklmnopq"; // 17 chars
        let prefix_short = "abcdefghijklmnop"; // 16 chars
        assert!(simd_compare_strings(
            prefix_long.as_bytes(),
            prefix_short.as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(!simd_compare_strings(
            prefix_short.as_bytes(),
            prefix_long.as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));

        let prefix_long_avx = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_"; // 33 chars
        let prefix_short_avx = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; // 32 chars
        assert!(simd_compare_strings(
            prefix_long_avx.as_bytes(),
            prefix_short_avx.as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));
        assert!(!simd_compare_strings(
            prefix_short_avx.as_bytes(),
            prefix_long_avx.as_bytes(),
            &ComparerOperation::GreaterOrEquals
        ));
    }

    /// Cross-check every ordering against Rust's own `[u8]` ordering over
    /// inputs that actually reach the vector paths.
    ///
    /// The existing cases are 5-byte ASCII, which leaves two things unproven.
    /// First, a difference found *inside* a 32- or 16-byte vector never
    /// happens, so the `movemask`/`trailing_zeros` first-difference logic is
    /// never exercised. Second, and more subtly, every byte stays under 0x80,
    /// so the `XOR 0x80` that turns a signed `cmpgt` into an unsigned
    /// comparison is never load-bearing — a string column holding any accented
    /// or CJK character would order wrongly and nothing here would notice.
    ///
    /// Rust's slice ordering is lexicographic with length as the tiebreak,
    /// which is exactly what `simd_greater_or_equal` implements, so it is a
    /// valid oracle rather than an approximation.
    #[test]
    fn simd_string_ordering_matches_rust_ordering_across_widths() {
        // Deterministic LCG: a fixed seed keeps failures reproducible.
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as u32
        };

        // Drawing from a small alphabet makes shared prefixes common, so the
        // comparison usually has to walk past whole equal vectors before it
        // finds a difference. A wide alphabet would differ in byte 0 nearly
        // every time and never leave the first block.
        const ALPHABET: [u8; 6] = [0x00, b'a', b'b', 0x7F, 0x80, 0xFF];

        for _ in 0..4000 {
            // 0..=80 spans every path: scalar-only, one SSE block, one AVX2
            // block, and AVX2 followed by SSE and scalar remainders.
            let len_a = (next() % 81) as usize;
            let len_b = (next() % 81) as usize;

            let mut a = vec![0u8; len_a];
            let mut b = vec![0u8; len_b];
            for byte in a.iter_mut() {
                *byte = ALPHABET[(next() % ALPHABET.len() as u32) as usize];
            }
            // Copy a prefix of `a` into `b` most of the time, then perturb it,
            // so the first difference lands at a varied offset instead of
            // always at index 0.
            let shared = (next() as usize) % (len_a.min(len_b) + 1);
            for (i, byte) in b.iter_mut().enumerate() {
                *byte = if i < shared {
                    a[i]
                } else {
                    ALPHABET[(next() % ALPHABET.len() as u32) as usize]
                };
            }

            let (a, b) = (&a[..], &b[..]);
            let lt = simd_compare_strings(a, b, &ComparerOperation::Less);
            let gt = simd_compare_strings(a, b, &ComparerOperation::Greater);
            let le = simd_compare_strings(a, b, &ComparerOperation::LessOrEquals);
            let ge = simd_compare_strings(a, b, &ComparerOperation::GreaterOrEquals);
            let eq = simd_compare_strings(a, b, &ComparerOperation::Equals);

            assert_eq!(lt, a < b, "Less {:?} vs {:?}", a, b);
            assert_eq!(gt, a > b, "Greater {:?} vs {:?}", a, b);
            assert_eq!(le, a <= b, "LessOrEquals {:?} vs {:?}", a, b);
            assert_eq!(ge, a >= b, "GreaterOrEquals {:?} vs {:?}", a, b);
            assert_eq!(eq, a == b, "Equals {:?} vs {:?}", a, b);
        }
    }

    /// The three derived orderings are defined in terms of
    /// `simd_greater_or_equal`, so they must stay consistent with each other
    /// as a matter of algebra, independent of what the comparison decides.
    ///
    /// This holds even if the underlying comparison were wrong, which is the
    /// point: it isolates a desync between the four from a fault in the one
    /// they are built on.
    #[test]
    fn derived_string_orderings_stay_algebraically_consistent() {
        let cases: [&[u8]; 8] = [
            b"",
            b"a",
            b"ab",
            b"\xFF",
            b"\x80\x00",
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // 33: past one AVX2 block
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab", // differs in the tail
            b"aaaaaaaaaaaaaaaab",                 // differs inside an SSE block
        ];

        for a in cases {
            for b in cases {
                let lt = simd_compare_strings(a, b, &ComparerOperation::Less);
                let gt = simd_compare_strings(a, b, &ComparerOperation::Greater);
                let le = simd_compare_strings(a, b, &ComparerOperation::LessOrEquals);
                let ge = simd_compare_strings(a, b, &ComparerOperation::GreaterOrEquals);
                let eq = simd_compare_strings(a, b, &ComparerOperation::Equals);

                // Exactly one of <, ==, > holds: a total order, no incomparables.
                assert_eq!(
                    [lt, eq, gt].iter().filter(|x| **x).count(),
                    1,
                    "trichotomy broken for {:?} vs {:?}",
                    a,
                    b
                );
                assert_eq!(le, lt || eq, "<= != (< or ==) for {:?} vs {:?}", a, b);
                assert_eq!(ge, gt || eq, ">= != (> or ==) for {:?} vs {:?}", a, b);
                // Antisymmetry across a swap of the operands.
                assert_eq!(
                    lt,
                    simd_compare_strings(b, a, &ComparerOperation::Greater),
                    "a<b != b>a for {:?} vs {:?}",
                    a,
                    b
                );
            }
        }
    }
}

/// Matches `value` against a SQL `LIKE` pattern.
///
/// `%` matches any run of characters including none; `_` matches exactly one.
/// `fold` lowercases ASCII on both sides, which is what `ILIKE` needs.
///
/// Exists because a pattern with an *interior* wildcard — `'a%b'` — cannot be
/// expressed as any conjunction of the prefix/suffix/substring operators.
/// `STARTSWITH 'a' AND CONTAINS 'b'` over-matches: it does not require `b` to
/// follow `a`, and with three or more segments it loses the ordering entirely.
/// Returning rows PostgreSQL would not is worse than refusing, which is why the
/// rewrite refused these until there was a real matcher behind them.
///
/// Iterative with backtracking rather than recursive: the pattern comes from a
/// client, and `%a%a%a%…` against a long value is a stack-overflow shaped input
/// for the naive recursion. This is O(n·m) worst case and O(n) on the patterns
/// anyone actually writes, with no stack growth.
pub fn like_match(value: &[u8], pattern: &[u8], fold: bool) -> bool {
    let norm = |b: u8| if fold { b.to_ascii_lowercase() } else { b };

    let (mut v, mut p) = (0usize, 0usize);
    // Where to resume if the current `%` guess turns out to be wrong.
    let (mut star_p, mut star_v) = (usize::MAX, 0usize);

    while v < value.len() {
        if p < pattern.len() && pattern[p] == b'_' {
            v += 1;
            p += 1;
        } else if p < pattern.len() && pattern[p] == b'%' {
            // Remember this `%` and try matching it against nothing first.
            star_p = p;
            star_v = v;
            p += 1;
        } else if p < pattern.len() && norm(pattern[p]) == norm(value[v]) {
            v += 1;
            p += 1;
        } else if star_p != usize::MAX {
            // Backtrack: let the last `%` swallow one more character.
            star_v += 1;
            v = star_v;
            p = star_p + 1;
        } else {
            return false;
        }
    }

    // Trailing `%`s can match the empty remainder; anything else cannot.
    while p < pattern.len() && pattern[p] == b'%' {
        p += 1;
    }
    p == pattern.len()
}

#[cfg(test)]
mod like_tests {
    use super::like_match;

    fn m(value: &str, pattern: &str) -> bool {
        like_match(value.as_bytes(), pattern.as_bytes(), false)
    }

    /// The shape that motivated this: a wildcard between two literal segments.
    #[test]
    fn an_interior_wildcard_requires_the_segments_in_order() {
        assert!(m("UserTable:42:read", "UserTable:%:%"));
        assert!(m("UserTable::", "UserTable:%:%"));
        assert!(!m("UserTable:42", "UserTable:%:%"), "only one colon");
        assert!(!m("Other:42:read", "UserTable:%:%"));

        // Order matters — the reason `STARTSWITH … AND CONTAINS …` is wrong.
        assert!(m("axxb", "a%b"));
        assert!(!m("bxxa", "a%b"), "segments matched but in the wrong order");
    }

    #[test]
    fn the_familiar_shapes_still_behave() {
        assert!(m("abc", "abc"));
        assert!(!m("abcd", "abc"));
        assert!(m("abcd", "abc%"));
        assert!(m("xabc", "%abc"));
        assert!(m("xabcy", "%abc%"));
        assert!(m("anything", "%"));
        assert!(m("", "%"));
        assert!(m("", ""));
        assert!(!m("a", ""));
    }

    #[test]
    fn underscore_matches_exactly_one_character() {
        assert!(m("abc", "a_c"));
        assert!(!m("ac", "a_c"), "underscore is not optional");
        assert!(!m("abbc", "a_c"), "underscore is not greedy");
        assert!(m("abc", "___"));
        assert!(
            m("a_c", "a_c"),
            "a literal underscore in the value still matches"
        );
    }

    /// Consecutive and trailing wildcards are common in generated SQL.
    #[test]
    fn repeated_and_trailing_wildcards_collapse() {
        assert!(m("abc", "a%%c"));
        assert!(m("abc", "abc%%%"));
        assert!(m("abc", "%%%abc"));
        assert!(m("abc", "%a%b%c%"));
    }

    /// Backtracking must find a match that a greedy first guess would miss.
    #[test]
    fn backtracking_finds_a_late_match() {
        assert!(m("aaaab", "%ab"));
        assert!(m("aaa", "%a"));
        assert!(!m("aaab", "%ac"));
    }

    /// A pattern built to blow a recursive matcher's stack must merely be slow.
    #[test]
    fn a_pathological_pattern_does_not_recurse() {
        let value = "a".repeat(2_000);
        let pattern = format!("{}b", "%a".repeat(40));
        assert!(!like_match(value.as_bytes(), pattern.as_bytes(), false));
    }

    #[test]
    fn folding_is_applied_to_both_sides() {
        assert!(like_match(b"UserTable:X", b"usertable:%", true));
        assert!(!like_match(b"UserTable:X", b"usertable:%", false));
    }
}
