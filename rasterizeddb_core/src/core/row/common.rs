use std::arch::x86_64::*;

use smallvec::SmallVec;

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
        // Case-insensitive: the ASCII folder, unless its answer can differ from
        // full Unicode lowering — see `ci_compare`.
        ComparerOperation::EqualsCaseInsensitive
        | ComparerOperation::ContainsCaseInsensitive
        | ComparerOperation::StartsWithCaseInsensitive
        | ComparerOperation::EndsWithCaseInsensitive
        | ComparerOperation::LikeCaseInsensitive => ci_compare(input1, input2, operation),
        ComparerOperation::Like => like_match(input1, input2, false),
        // `SIMILAR TO`: a pattern language of its own, matched in
        // `crate::core::similar_to`. The scan reaches it through
        // `logical::perform_comparison_operation`, which calls
        // `similar_to_compare` first so that a pattern the matcher refuses
        // fails the statement; this arm only serves callers that want a bool.
        ComparerOperation::SimilarTo => similar_to_compare(input1, input2).unwrap_or(false),
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

/// `value SIMILAR TO pattern`, with a refusal reported rather than answered.
///
/// A malformed pattern was already refused when the predicate was planned, so
/// `InvalidPattern` here is a safety net; `PatternTooComplex` is a value too
/// costly to match against this pattern. Either fails the statement — a
/// `false` would drop the row and look exactly like a non-match. A value or
/// pattern that is not UTF-8 text does not match, as before.
pub(crate) fn similar_to_compare(
    value: &[u8],
    pattern: &[u8],
) -> Result<bool, crate::core::row::error::EvalError> {
    use crate::core::row::error::EvalError;
    use crate::core::similar_to::MatchError;
    match (std::str::from_utf8(value), std::str::from_utf8(pattern)) {
        (Ok(text), Ok(pattern)) => {
            crate::core::similar_to::evaluate(text, pattern, Some('\\')).map_err(|e| match e {
                MatchError::Invalid(_) => EvalError::InvalidPattern,
                MatchError::TooComplex => EvalError::PatternTooComplex,
            })
        }
        _ => Ok(false),
    }
}

// Case-insensitive comparisons, backing `ILIKE`.
//
// The functions below fold ASCII only, and are what runs whenever both sides
// are ASCII. `simd_compare_strings` sends anything else to
// `ci_compare_unicode`, which lowers both sides in full first.
//
// `ILIKE '%x%'` — the ORM search box — becomes `ICONTAINS` and runs once per
// row, so the substring search is vectorised the same way `contains_small`
// is: see `anchor_find`.

/// A case-insensitive operator: `lower(value) op lower(pattern)`, answered by
/// the ASCII folder whenever that is provably the same answer.
///
/// For an ASCII pattern it is, unless lowering a non-ASCII character of the
/// value can produce what the pattern matches. Exactly two characters lower to
/// anything containing ASCII — the Kelvin sign `U+212A` (to `k`) and capital
/// `I` with dot `U+0130` (to `i` + `U+0307`, the only one that changes the
/// character count) — which `lowering_reaches_ascii_exactly_twice` checks over
/// every code point. So the ASCII answer stands when the pattern has no `k`,
/// no `i` and (for `LIKE`) no `_`; or when the value holds neither of those
/// characters' lead bytes (`0xE2`, `0xC4`); or, outside `LIKE` with `_`, when
/// the ASCII folder already said yes (a match among unchanged characters
/// survives lowering).
///
/// That keeps the common case free of any extra pass over the value. Measured
/// on 1M ASCII values of 50-500 bytes against the ASCII-only dispatcher
/// (`ilike_dispatch_ascii_bench`): a pattern without `k`/`i`/`_` costs
/// +3-5 ns a row (classifying the pattern); one with a `k` or `i` adds one
/// `memchr2` over the value on rows the ASCII folder rejected, +11 ns a row.
/// Testing `is_ascii` on both sides first, the obvious version, cost
/// ISTARTSWITH 3-4x.
#[inline(always)]
fn ci_compare(input1: &[u8], input2: &[u8], operation: &ComparerOperation) -> bool {
    match operation {
        ComparerOperation::EqualsCaseInsensitive => ci_compare_op::<CI_EQ>(input1, input2),
        ComparerOperation::StartsWithCaseInsensitive => ci_compare_op::<CI_STARTS>(input1, input2),
        ComparerOperation::EndsWithCaseInsensitive => ci_compare_op::<CI_ENDS>(input1, input2),
        ComparerOperation::ContainsCaseInsensitive => ci_compare_op::<CI_CONTAINS>(input1, input2),
        _ => ci_compare_op::<CI_LIKE>(input1, input2),
    }
}

const CI_EQ: u8 = 0;
const CI_STARTS: u8 = 1;
const CI_ENDS: u8 = 2;
const CI_CONTAINS: u8 = 3;
const CI_LIKE: u8 = 4;

/// [`ci_compare`] for one operator, chosen at compile time so each arm of the
/// dispatcher carries only its own check.
#[inline(always)]
fn ci_compare_op<const OP: u8>(input1: &[u8], input2: &[u8]) -> bool {
    let operation = match OP {
        CI_EQ => &ComparerOperation::EqualsCaseInsensitive,
        CI_STARTS => &ComparerOperation::StartsWithCaseInsensitive,
        CI_ENDS => &ComparerOperation::EndsWithCaseInsensitive,
        CI_CONTAINS => &ComparerOperation::ContainsCaseInsensitive,
        _ => &ComparerOperation::LikeCaseInsensitive,
    };
    let (non_ascii, reach, under) = classify_pattern(input2);
    if non_ascii {
        return ci_compare_unicode(input1, input2, operation);
    }
    let ascii = match OP {
        CI_EQ => ci_equals(input1, input2),
        CI_STARTS => ci_starts_with(input1, input2),
        CI_ENDS => ci_ends_with(input1, input2),
        CI_CONTAINS => ci_contains(input1, input2),
        _ => like_match(input1, input2, true),
    };
    let under = OP == CI_LIKE && under;
    if (ascii && !under) || !(under || reach) {
        return ascii;
    }
    // `window`: the bytes of the value whose lowering can reach the pattern.
    // A character lowers to no less than a third of its bytes (the Kelvin sign,
    // three bytes to one), so a prefix or suffix of `m` lowered bytes comes
    // from at most `3m` (plus a partial character) bytes of the value.
    let n = input1.len();
    let reach_len = (3 * input2.len() + 4).min(n);
    let window = match OP {
        // Longer than `3m`, the value cannot lower to `m` bytes at all.
        CI_EQ if n > 3 * input2.len() => 0..0,
        CI_STARTS => 0..reach_len,
        CI_ENDS => n - reach_len..n,
        _ => 0..n,
    };
    if !holds_ascii_lowering(&input1[window]) {
        return ascii;
    }
    ci_compare_unicode(input1, input2, operation)
}

/// Whether `bytes` hold `İ` (`C4 B0`) or the Kelvin sign (`E2 84 AA`) — the
/// only characters whose lowercase reaches ASCII. Found by their lead bytes and
/// confirmed, so ordinary `0xE2` text (curly quotes, dashes, `€`) does not send
/// a row down the slow path.
#[inline]
fn holds_ascii_lowering(bytes: &[u8]) -> bool {
    memchr::memchr2_iter(0xC4, 0xE2, bytes).any(|at| match bytes[at] {
        0xC4 => bytes.get(at + 1) == Some(&0xB0),
        _ => bytes.get(at + 1..at + 3) == Some(&[0x84, 0xAA][..]),
    })
}

/// One pass over the pattern, eight bytes at a time: is any byte non-ASCII,
/// does it hold a `k`/`i` (either case) that a non-ASCII character lowers to,
/// does it hold a `_`. Runs once per row, so it is SWAR rather than a byte loop.
#[inline(always)]
fn classify_pattern(p: &[u8]) -> (bool, bool, bool) {
    const ONES: u64 = 0x0101_0101_0101_0101;
    const HIGH: u64 = 0x8080_8080_8080_8080;
    // Exact: non-zero iff some byte of `v` is zero.
    let has_zero = |v: u64| v.wrapping_sub(ONES) & !v & HIGH;
    let mut high = 0u64;
    let mut reach = 0u64;
    let mut under = 0u64;
    let mut word = |w: u64| {
        high |= w;
        let l = w | (ONES * 0x20);
        reach |= has_zero(l ^ (ONES * b'k' as u64)) | has_zero(l ^ (ONES * b'i' as u64));
        under |= has_zero(w ^ (ONES * b'_' as u64));
    };
    let n = p.len();
    let u16_at = |i: usize| u16::from_le_bytes([p[i], p[i + 1]]) as u64;
    let u32_at = |i: usize| u32::from_le_bytes(p[i..i + 4].try_into().expect("four bytes")) as u64;
    // Bytes that are none of the three, filling a short pattern's word.
    const PAD: u64 = ONES * b'0' as u64;
    if n >= 8 {
        let mut chunks = p.chunks_exact(8);
        for c in &mut chunks {
            word(u64::from_le_bytes(c.try_into().expect("eight bytes")));
        }
        if !chunks.remainder().is_empty() {
            // The last eight bytes, overlapping what was already read: every
            // test here is "any byte", so reading a byte twice is harmless.
            word(u64::from_le_bytes(p[n - 8..].try_into().expect("eight bytes")));
        }
    } else if n >= 4 {
        word(u32_at(0) | (u32_at(n - 4) << 32));
    } else if n >= 2 {
        word(u16_at(0) | (u16_at(n - 2) << 16) | (PAD << 32));
    } else if n == 1 {
        word(p[0] as u64 | (PAD << 8));
    }
    (high & HIGH != 0, reach != 0, under != 0)
}

/// A case-insensitive operator on text the ASCII folder cannot answer.
///
/// PostgreSQL defines `ILIKE` for a multi-byte encoding as `lower(value) LIKE
/// lower(pattern)`, so that is what this does, with the lowering `lower()`
/// itself uses here (`str::to_lowercase`) — `'É' ILIKE 'é'` and
/// `lower('É') = lower('é')` cannot disagree. The ASCII folders cannot be used
/// for this even when one side is ASCII: lowering changes lengths (`'K'`, the
/// Kelvin sign, is three bytes and lowers to an ASCII `k`), so their
/// length-first shortcuts would answer `false` for a match.
///
/// Bytes that are not UTF-8 are not text to lower; they fold ASCII only, as
/// before.
#[cold]
#[inline(never)]
fn ci_compare_unicode(input1: &[u8], input2: &[u8], operation: &ComparerOperation) -> bool {
    let (Ok(a), Ok(b)) = (std::str::from_utf8(input1), std::str::from_utf8(input2)) else {
        return match operation {
            ComparerOperation::EqualsCaseInsensitive => ci_equals(input1, input2),
            ComparerOperation::ContainsCaseInsensitive => ci_contains(input1, input2),
            ComparerOperation::StartsWithCaseInsensitive => ci_starts_with(input1, input2),
            ComparerOperation::EndsWithCaseInsensitive => ci_ends_with(input1, input2),
            _ => like_match(input1, input2, true),
        };
    };
    let (a, b) = (a.to_lowercase(), b.to_lowercase());
    match operation {
        ComparerOperation::EqualsCaseInsensitive => a == b,
        ComparerOperation::ContainsCaseInsensitive => a.contains(b.as_str()),
        ComparerOperation::StartsWithCaseInsensitive => a.starts_with(b.as_str()),
        ComparerOperation::EndsWithCaseInsensitive => a.ends_with(b.as_str()),
        _ => like_match(a.as_bytes(), b.as_bytes(), false),
    }
}

/// ASCII lowercase, branch-free so a fixed-width loop over it vectorises.
/// Identical to `u8::to_ascii_lowercase`.
#[inline(always)]
fn fold_ascii(b: u8) -> u8 {
    b | (((b.wrapping_sub(b'A') < 26) as u8) << 5)
}

#[inline]
fn ci_equals(input1: &[u8], input2: &[u8]) -> bool {
    if input1.len() != input2.len() {
        return false;
    }
    // Sixteen bytes at a time with no early exit inside a block, which the
    // compiler turns into a handful of vector instructions; the tail is the
    // plain per-byte compare.
    let mut a = input1.chunks_exact(16);
    let mut b = input2.chunks_exact(16);
    for (x, y) in (&mut a).zip(&mut b) {
        let mut diff = 0u8;
        for k in 0..16 {
            diff |= fold_ascii(x[k]) ^ fold_ascii(y[k]);
        }
        if diff != 0 {
            return false;
        }
    }
    a.remainder()
        .iter()
        .zip(b.remainder())
        .all(|(x, y)| x.eq_ignore_ascii_case(y))
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
    // The empty needle is contained in everything, as in `simd_contains` and
    // SQL's `x ILIKE '%%'`.
    anchor_find::<true>(haystack, needle).is_some()
}

/// How one anchor byte of a needle is compared against the haystack: OR the
/// haystack byte with `or`, then test for equality with `target`.
///
/// For an ASCII letter `or` is `0x20` and `target` its lowercase form — the
/// only bytes that OR `0x20` maps onto a lowercase letter are that letter's two
/// cases, so the test is exact. Anything else is compared as is, which keeps
/// non-ASCII bytes case-sensitive, as `ci_equals` does.
#[inline(always)]
fn anchor_of<const FOLD: bool>(b: u8) -> (u8, u8) {
    if FOLD && b.is_ascii_alphabetic() {
        (0x20, b.to_ascii_lowercase())
    } else {
        (0, b)
    }
}

#[inline(always)]
fn anchor_eq<const FOLD: bool>(a: &[u8], b: &[u8]) -> bool {
    if FOLD { ci_equals(a, b) } else { a == b }
}

/// The byte offset of the first occurrence of `needle` in `haystack`; ASCII
/// letters compare case-insensitively when `FOLD`.
///
/// The first/last-byte filter of `contains_small`, generalised to folding: each
/// 32-byte block is loaded at `i` and at `i + m - 1`, OR-ed with the anchor's
/// fold mask, compared, and the two masks ANDed, so only positions where both
/// ends line up reach a full comparison of the interior. The scalar fallback it
/// replaced re-folded both sides at every position, O(n·m) on every row.
///
/// Leftmost-first: candidates are confirmed in ascending order.
#[inline]
pub(crate) fn anchor_find<const FOLD: bool>(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    let n = haystack.len();
    let m = needle.len();
    if m == 0 {
        return Some(0);
    }
    if m > n {
        return None;
    }
    let n_minus_m = n - m;
    let (or_first, first) = anchor_of::<FOLD>(needle[0]);
    let (or_last, last) = anchor_of::<FOLD>(needle[m - 1]);
    // What a candidate still has to confirm: the anchors are already known to
    // match, so only the bytes between them (none when `m <= 2`).
    let inner = if m > 2 { &needle[1..m - 1] } else { &needle[..0] };
    let confirm = |pos: usize| -> bool {
        m <= 2 || anchor_eq::<FOLD>(&haystack[pos + 1..pos + m - 1], inner)
    };

    let hp = haystack.as_ptr();
    let mut i = 0usize;

    // SAFETY: the guards keep both loads — 32 (or 16) bytes at `i` and at
    // `i + m - 1` — inside the haystack, exactly as in `contains_small`. AVX2
    // is assumed, as everywhere in this module (target-cpu=x86-64-v3).
    unsafe {
        if n_minus_m >= 31 {
            let v_or_first = _mm256_set1_epi8(or_first as i8);
            let v_or_last = _mm256_set1_epi8(or_last as i8);
            let v_first = _mm256_set1_epi8(first as i8);
            let v_last = _mm256_set1_epi8(last as i8);
            while i + 31 <= n_minus_m {
                let block_first = _mm256_or_si256(
                    _mm256_loadu_si256(hp.add(i) as *const __m256i),
                    v_or_first,
                );
                let block_last = _mm256_or_si256(
                    _mm256_loadu_si256(hp.add(i + m - 1) as *const __m256i),
                    v_or_last,
                );
                let eq = _mm256_and_si256(
                    _mm256_cmpeq_epi8(block_first, v_first),
                    _mm256_cmpeq_epi8(block_last, v_last),
                );
                let mut mask = _mm256_movemask_epi8(eq) as u32;
                while mask != 0 {
                    let pos = i + mask.trailing_zeros() as usize;
                    if confirm(pos) {
                        return Some(pos);
                    }
                    mask &= mask.wrapping_sub(1);
                }
                i += 32;
            }
        }

        if i + 15 <= n_minus_m {
            let v_or_first = _mm_set1_epi8(or_first as i8);
            let v_or_last = _mm_set1_epi8(or_last as i8);
            let v_first = _mm_set1_epi8(first as i8);
            let v_last = _mm_set1_epi8(last as i8);
            while i + 15 <= n_minus_m {
                let block_first =
                    _mm_or_si128(_mm_loadu_si128(hp.add(i) as *const __m128i), v_or_first);
                let block_last = _mm_or_si128(
                    _mm_loadu_si128(hp.add(i + m - 1) as *const __m128i),
                    v_or_last,
                );
                let eq = _mm_and_si128(
                    _mm_cmpeq_epi8(block_first, v_first),
                    _mm_cmpeq_epi8(block_last, v_last),
                );
                let mut mask = _mm_movemask_epi8(eq) as u32;
                while mask != 0 {
                    let pos = i + mask.trailing_zeros() as usize;
                    if confirm(pos) {
                        return Some(pos);
                    }
                    mask &= mask.wrapping_sub(1);
                }
                i += 16;
            }
        }
    }

    while i <= n_minus_m {
        if (haystack[i] | or_first) == first
            && (haystack[i + m - 1] | or_last) == last
            && confirm(i)
        {
            return Some(i);
        }
        i += 1;
    }
    None
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

    // The empty string is a substring of every string. `LIKE '%%'` reaches here
    // as `CONTAINS ''`, and answering false dropped every row PostgreSQL keeps.
    if m == 0 {
        return true;
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
        // Empty needle: the empty string is in every string, which is what
        // `LIKE '%%'` (rewritten to `CONTAINS ''`) needs to answer.
        assert!(simd_compare_strings(
            "abc".as_bytes(),
            "".as_bytes(),
            &ComparerOperation::Contains
        ));
        assert!(simd_compare_strings(
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
    // The common shapes have no `_`: `'a%b'`, `'%x%y%'`. Those are answered by
    // finding each literal segment with a vectorised search instead of walking
    // the value a byte at a time with backtracking.
    let answer = if pattern.contains(&b'\\') {
        // PostgreSQL's default escape: `\` makes the next character literal.
        // Django escapes every `_` and `%` of a searched value this way.
        if fold {
            like_match_escaped_segments::<true>(value, pattern)
        } else {
            like_match_escaped_segments::<false>(value, pattern)
        }
    } else if !pattern.contains(&b'_') {
        let segments: SmallVec<[&[u8]; 8]> = pattern.split(|&b| b == b'%').collect();
        if fold {
            match_segments::<true>(value, &segments, pattern)
        } else {
            match_segments::<false>(value, &segments, pattern)
        }
    } else {
        None
    };
    match answer {
        Some(answer) => answer,
        None => like_match_stepping(value, pattern, fold),
    }
}

/// [`match_segments`] for a pattern containing escapes: its segments are
/// unescaped first. `None` when the pattern has an unescaped `_`, which needs
/// the stepping matcher. A trailing lone backslash is a literal backslash
/// here, as in [`like_match_stepping`]; the planner refuses such a pattern
/// before it gets this far.
fn like_match_escaped_segments<const FOLD: bool>(value: &[u8], pattern: &[u8]) -> Option<bool> {
    let mut literal: Vec<u8> = Vec::with_capacity(pattern.len());
    let mut bounds: SmallVec<[(usize, usize); 8]> = SmallVec::new();
    let mut start = 0usize;
    let mut p = 0usize;
    while p < pattern.len() {
        match like_token(pattern, p) {
            (LikeToken::Any, _) => return None,
            (LikeToken::Star, width) => {
                bounds.push((start, literal.len()));
                start = literal.len();
                p += width;
            }
            (LikeToken::Lit(b), width) => {
                literal.push(b);
                p += width;
            }
        }
    }
    bounds.push((start, literal.len()));
    let segments: SmallVec<[&[u8]; 8]> = bounds.iter().map(|&(a, b)| &literal[a..b]).collect();
    match_segments::<FOLD>(value, &segments, pattern)
}

/// `LIKE` for a pattern with no `_`: literal `segments` that were separated by
/// `%` (one segment means no `%` at all). `pattern` is the pattern as written,
/// for the UTF-8 check below.
///
/// The first segment must be a prefix and the last a suffix (unless the pattern
/// starts or ends with `%`, which makes them empty); the segments between are
/// found in order, each leftmost-first after the previous one, inside what the
/// prefix and suffix leave. Leftmost-first is optimal for `%`-only patterns:
/// taking an earlier occurrence never leaves less room for what follows.
///
/// `None` means "ask `like_match_stepping`". That is needed only for a match on
/// a value or pattern that is not valid UTF-8: the stepping matcher retries a
/// `%` a whole *character* at a time, so in a malformed sequence it can skip a
/// byte offset this search would accept. Every offset it does try, this tries
/// too, so a `false` here is always its answer as well; only a `true` needs the
/// check, and only then is the value validated.
#[inline]
fn match_segments<const FOLD: bool>(
    value: &[u8],
    segments: &[&[u8]],
    pattern: &[u8],
) -> Option<bool> {
    let [prefix, middle @ .., suffix] = segments else {
        // No wildcard at all: equality.
        return Some(anchor_eq::<FOLD>(value, segments.first().copied().unwrap_or(&[])));
    };
    if value.len() < prefix.len() + suffix.len() {
        return Some(false);
    }
    let tail_at = value.len() - suffix.len();
    if !anchor_eq::<FOLD>(&value[..prefix.len()], prefix)
        || !anchor_eq::<FOLD>(&value[tail_at..], suffix)
    {
        return Some(false);
    }
    let mut cursor = prefix.len();
    for segment in middle {
        if segment.is_empty() {
            continue;
        }
        match anchor_find::<FOLD>(&value[cursor..tail_at], segment) {
            Some(at) => cursor += at + segment.len(),
            None => return Some(false),
        }
    }
    if std::str::from_utf8(value).is_ok() && std::str::from_utf8(pattern).is_ok() {
        Some(true)
    } else {
        None
    }
}

/// One element of a `LIKE` pattern.
#[derive(Clone, Copy, PartialEq, Debug)]
enum LikeToken {
    /// `_`: one character.
    Any,
    /// `%`: any run of characters.
    Star,
    /// A byte that matches itself — including `\_`, `\%` and `\\`.
    Lit(u8),
}

/// The token at `p` and how many pattern bytes it spans. A backslash escapes
/// the next byte; one at the very end stands for itself.
#[inline(always)]
fn like_token(pattern: &[u8], p: usize) -> (LikeToken, usize) {
    match pattern[p] {
        b'_' => (LikeToken::Any, 1),
        b'%' => (LikeToken::Star, 1),
        b'\\' if p + 1 < pattern.len() => (LikeToken::Lit(pattern[p + 1]), 2),
        b => (LikeToken::Lit(b), 1),
    }
}

/// The general matcher, for patterns with `_`, and the reference
/// `match_segments` is tested against.
///
/// After a `%` fails, the retry used to step one character at a time. When the
/// token after the `%` is a literal, every retry that does not land on that
/// literal fails at once, so the retry jumps straight to the next occurrence
/// with the vectorised search instead — on a valid UTF-8 value and pattern,
/// where every character boundary is a retry position and an occurrence of a
/// character's first byte is always on one. Otherwise it steps as before.
/// `stepping_skip_agrees_with_stepping_one_character_at_a_time` pins that.
fn like_match_stepping(value: &[u8], pattern: &[u8], fold: bool) -> bool {
    let norm = |b: u8| if fold { b.to_ascii_lowercase() } else { b };
    // `_` is one *character*, as in PostgreSQL: `'é' LIKE '_'` is true, and
    // `é` is two bytes. A `%` retry steps a whole character too, so `v` stays
    // on a character boundary and a later `_` never starts mid-sequence.
    let char_len = |at: usize| -> usize {
        let len = match value[at] {
            0xF0..=0xFF => 4,
            0xE0..=0xEF => 3,
            0xC0..=0xDF => 2,
            _ => 1,
        };
        len.min(value.len() - at)
    };
    // Whether the jump is allowed; worked out the first time it is wanted.
    let mut can_skip: Option<bool> = None;
    // The literal byte after the current `%`, if the token after it is one —
    // worked out when the `%` is reached rather than on every retry.
    let mut star_next: Option<u8> = None;

    let (mut v, mut p) = (0usize, 0usize);
    // Where to resume if the current `%` guess turns out to be wrong.
    let (mut star_p, mut star_v) = (usize::MAX, 0usize);

    while v < value.len() {
        if p < pattern.len() {
            match like_token(pattern, p) {
                (LikeToken::Any, width) => {
                    v += char_len(v);
                    p += width;
                    continue;
                }
                (LikeToken::Star, width) => {
                    // Remember this `%` and try matching it against nothing
                    // first.
                    star_p = p;
                    star_v = v;
                    p += width;
                    star_next = match (p < pattern.len()).then(|| like_token(pattern, p)) {
                        Some((LikeToken::Lit(b), _)) => Some(b),
                        _ => None,
                    };
                    continue;
                }
                (LikeToken::Lit(b), width) if norm(b) == norm(value[v]) => {
                    v += 1;
                    p += width;
                    continue;
                }
                _ => {}
            }
        }
        if star_p == usize::MAX {
            return false;
        }
        // Backtrack: let the last `%` swallow one more character.
        star_v += char_len(star_v);
        if let Some(b) = star_next {
            let allowed = *can_skip.get_or_insert_with(|| {
                std::str::from_utf8(value).is_ok() && std::str::from_utf8(pattern).is_ok()
            });
            if allowed {
                let found = if fold {
                    anchor_find::<true>(&value[star_v..], &[b])
                } else {
                    anchor_find::<false>(&value[star_v..], &[b])
                };
                match found {
                    Some(at) => star_v += at,
                    // No later position can start the literal, so every
                    // remaining retry fails.
                    None => return false,
                }
            }
        }
        v = star_v;
        p = star_p + 1;
    }

    // Trailing `%`s can match the empty remainder; anything else cannot. An
    // escaped `%` is `\%` here, so it is not mistaken for one.
    while p < pattern.len() && pattern[p] == b'%' {
        p += 1;
    }
    p == pattern.len()
}

/// The stepping matcher as it was before the jump: one character per retry.
/// Kept as the reference the jump is tested against.
#[cfg(test)]
fn like_match_stepping_reference(value: &[u8], pattern: &[u8], fold: bool) -> bool {
    let norm = |b: u8| if fold { b.to_ascii_lowercase() } else { b };
    let char_len = |at: usize| -> usize {
        let len = match value[at] {
            0xF0..=0xFF => 4,
            0xE0..=0xEF => 3,
            0xC0..=0xDF => 2,
            _ => 1,
        };
        len.min(value.len() - at)
    };
    let (mut v, mut p) = (0usize, 0usize);
    let (mut star_p, mut star_v) = (usize::MAX, 0usize);
    while v < value.len() {
        if p < pattern.len() {
            match like_token(pattern, p) {
                (LikeToken::Any, width) => {
                    v += char_len(v);
                    p += width;
                    continue;
                }
                (LikeToken::Star, width) => {
                    star_p = p;
                    star_v = v;
                    p += width;
                    continue;
                }
                (LikeToken::Lit(b), width) if norm(b) == norm(value[v]) => {
                    v += 1;
                    p += width;
                    continue;
                }
                _ => {}
            }
        }
        if star_p == usize::MAX {
            return false;
        }
        star_v += char_len(star_v);
        v = star_v;
        p = star_p + 1;
    }
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

    /// A character, not a byte: `é` is two bytes, `€` three, `😀` four.
    #[test]
    fn underscore_matches_one_multibyte_character() {
        assert!(m("é", "_"));
        assert!(!m("é", "__"), "two bytes are still one character");
        assert!(m("€", "_"));
        assert!(m("😀", "_"));
        assert!(m("aéc", "a_c"));
        assert!(m("crème", "cr_me"));
        assert!(m("日本語", "_本_"));
        assert!(!m("日本語", "____"));
        // A `%` retry must not leave `_` starting inside a sequence: here the
        // first guess for `%` fails and the retries step `é` as a unit.
        assert!(m("éé!", "%_!"));
        assert!(m("xéy", "%_y"));
        assert!(!m("é", "%__"));
    }

    #[test]
    fn only_wildcards_match_everything() {
        // `LIKE '%%'` is rewritten to `CONTAINS ''` before it gets here, but the
        // matcher must agree with that rewrite.
        assert!(m("", "%%"));
        assert!(m("abc", "%%"));
        assert!(super::simd_compare_strings(
            b"abc",
            b"",
            &crate::core::processor::transformer::ComparerOperation::Contains
        ));
        assert!(super::simd_compare_strings(
            b"",
            b"",
            &crate::core::processor::transformer::ComparerOperation::Contains
        ));
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

    /// A small deterministic generator, so a failure names a reproducible input.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self, bound: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            (self.0 % bound as u64) as usize
        }
    }

    /// Byte strings built from pieces chosen to collide: letters in both
    /// cases, non-letters next to them in the ASCII table (`@`, `[`, `` ` ``,
    /// `{` differ from letters only in bit 5), multibyte characters, and
    /// malformed UTF-8 fragments.
    fn generate(rng: &mut Rng, pieces: &[&[u8]], max: usize) -> Vec<u8> {
        let len = rng.next(max + 1);
        let mut out = Vec::new();
        for _ in 0..len {
            out.extend_from_slice(pieces[rng.next(pieces.len())]);
        }
        out
    }

    const VALUE_PIECES: &[&[u8]] = &[
        b"a", b"A", b"b", b"B", b"c", b"z", b"Z", b"@", b"[", b"`", b"{", b" ", b"%",
        b"_", "é".as_bytes(), "É".as_bytes(), "€".as_bytes(), "😀".as_bytes(), b"\xC3",
        b"\xA9", b"\xFF", b"ab", b"AB", b"abc", b"\\",
    ];
    const PATTERN_PIECES: &[&[u8]] = &[
        b"a", b"A", b"b", b"B", b"c", b"Z", b"@", b"`", b"%", b"%", b"%", b"%%", " ".as_bytes(),
        "é".as_bytes(), "€".as_bytes(), b"\xA9", b"ab", b"aB", b"\\", b"\\%", b"\\_",
        b"\\\\", b"\\a",
    ];

    /// The segment search must agree with the stepping matcher on every
    /// `_`-free pattern, folded and not, including on malformed UTF-8 where it
    /// has to hand back to the stepping matcher.
    #[test]
    fn segment_search_agrees_with_the_stepping_matcher() {
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        for _ in 0..300_000 {
            let value = generate(&mut rng, VALUE_PIECES, 12);
            let pattern = generate(&mut rng, PATTERN_PIECES, 7);
            for fold in [false, true] {
                assert_eq!(
                    like_match(&value, &pattern, fold),
                    super::like_match_stepping_reference(&value, &pattern, fold),
                    "value {:?} pattern {:?} fold {}",
                    String::from_utf8_lossy(&value),
                    String::from_utf8_lossy(&pattern),
                    fold
                );
            }
        }
        // Long values, so the vector paths of the search run, with segments
        // drawn from the value itself so most patterns match.
        for _ in 0..20_000 {
            let value = generate(&mut rng, VALUE_PIECES, 120);
            let mut pattern = Vec::new();
            for _ in 0..rng.next(4) + 1 {
                if rng.next(3) == 0 {
                    pattern.push(b'%');
                }
                if !value.is_empty() {
                    let at = rng.next(value.len());
                    let len = rng.next(6).min(value.len() - at);
                    pattern.extend(value[at..at + len].iter().filter(|b| **b != b'_'));
                }
                pattern.push(b'%');
            }
            if rng.next(2) == 0 {
                pattern.pop();
            }
            for fold in [false, true] {
                let mut p = pattern.clone();
                if fold {
                    p.iter_mut().for_each(|b| *b = b.to_ascii_uppercase());
                }
                assert_eq!(
                    like_match(&value, &p, fold),
                    super::like_match_stepping_reference(&value, &p, fold),
                    "value {:?} pattern {:?} fold {}",
                    String::from_utf8_lossy(&value),
                    String::from_utf8_lossy(&p),
                    fold
                );
            }
        }
    }

    /// The stepping matcher's jump to the next occurrence of the literal after
    /// a `%` must give exactly the one-character-at-a-time answer, on patterns
    /// with `_`, escapes, multibyte and malformed UTF-8, folded and not.
    #[test]
    fn stepping_skip_agrees_with_stepping_one_character_at_a_time() {
        let pieces: &[&[u8]] = &[
            b"a", b"A", b"b", b"c", b"%", b"%", b"_", b"_", b"\\_", b"\\%", b"\\\\",
            "é".as_bytes(), "€".as_bytes(), b"\xA9", b"\xC3", b"ab", b"aB", b"\\",
        ];
        let mut rng = Rng(0x0BAD_5EED_1234_5678);
        for round in 0..300_000 {
            let value = generate(&mut rng, VALUE_PIECES, if round % 8 == 0 { 60 } else { 12 });
            let pattern = generate(&mut rng, pieces, 7);
            for fold in [false, true] {
                assert_eq!(
                    super::like_match_stepping(&value, &pattern, fold),
                    super::like_match_stepping_reference(&value, &pattern, fold),
                    "value {:?} pattern {:?} fold {}",
                    String::from_utf8_lossy(&value),
                    String::from_utf8_lossy(&pattern),
                    fold
                );
                // And the dispatcher, which routes escaped and `_`-free
                // patterns elsewhere, agrees with both.
                assert_eq!(
                    like_match(&value, &pattern, fold),
                    super::like_match_stepping_reference(&value, &pattern, fold),
                    "like_match: value {:?} pattern {:?} fold {}",
                    String::from_utf8_lossy(&value),
                    String::from_utf8_lossy(&pattern),
                    fold
                );
            }
        }
    }

    /// The scalar search `ci_contains` used before it was vectorised.
    fn reference_ci_contains(haystack: &[u8], needle: &[u8]) -> bool {
        needle.is_empty()
            || (needle.len() <= haystack.len()
                && haystack
                    .windows(needle.len())
                    .any(|w| w.iter().zip(needle).all(|(a, b)| a.eq_ignore_ascii_case(b))))
    }

    /// `ICONTAINS`, `IEQUALS`, `ISTARTSWITH`, `IENDSWITH` against the scalar
    /// definitions, over haystacks long enough to run the 32- and 16-byte
    /// blocks and their tails, and needles from 0 to 40 bytes — including ones
    /// cut from the haystack with their case flipped, so most searches hit.
    ///
    /// These are the ASCII folders, called directly: the dispatcher sends
    /// non-ASCII text to the Unicode path instead, which
    /// `unicode_fold_tests` holds to its own definition.
    #[test]
    fn case_insensitive_search_agrees_with_the_scalar_definition() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for round in 0..200_000 {
            let hay = generate(&mut rng, VALUE_PIECES, if round % 4 == 0 { 90 } else { 20 });
            let needle = if !hay.is_empty() && rng.next(3) != 0 {
                let at = rng.next(hay.len());
                let len = rng.next(41).min(hay.len() - at);
                hay[at..at + len]
                    .iter()
                    .map(|b| match rng.next(3) {
                        0 => b.to_ascii_uppercase(),
                        1 => b.to_ascii_lowercase(),
                        _ => *b,
                    })
                    .collect()
            } else {
                generate(&mut rng, VALUE_PIECES, 4)
            };
            let ctx = || {
                format!(
                    "hay {:?} needle {:?}",
                    String::from_utf8_lossy(&hay),
                    String::from_utf8_lossy(&needle)
                )
            };
            assert_eq!(
                super::ci_contains(&hay, &needle),
                reference_ci_contains(&hay, &needle),
                "contains: {}",
                ctx()
            );
            let eq = |a: &[u8], b: &[u8]| {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.eq_ignore_ascii_case(y))
            };
            assert_eq!(
                super::ci_equals(&hay, &needle),
                eq(&hay, &needle),
                "equals: {}",
                ctx()
            );
            assert_eq!(
                super::ci_starts_with(&hay, &needle),
                hay.len() >= needle.len() && eq(&hay[..needle.len()], &needle),
                "starts: {}",
                ctx()
            );
            assert_eq!(
                super::ci_ends_with(&hay, &needle),
                hay.len() >= needle.len() && eq(&hay[hay.len() - needle.len()..], &needle),
                "ends: {}",
                ctx()
            );
            // And the position, which the `LIKE` segment search relies on.
            let expect = if needle.is_empty() {
                Some(0)
            } else {
                hay.windows(needle.len()).position(|w| eq(w, &needle))
            };
            assert_eq!(super::anchor_find::<true>(&hay, &needle), expect, "find: {}", ctx());
            let expect_cs = if needle.is_empty() {
                Some(0)
            } else {
                hay.windows(needle.len()).position(|w| w == &needle[..])
            };
            assert_eq!(super::anchor_find::<false>(&hay, &needle), expect_cs, "find cs: {}", ctx());
        }
    }

    /// Every byte value against every other as a one-byte needle: the OR-0x20
    /// anchor must accept exactly the two cases of a letter and nothing else.
    #[test]
    fn the_folded_anchor_accepts_exactly_the_two_cases() {
        for h in 0..=255u8 {
            for n in 0..=255u8 {
                let hay = [h; 40];
                assert_eq!(
                    super::ci_contains(&hay, &[n]),
                    h.eq_ignore_ascii_case(&n),
                    "h={h:#x} n={n:#x}"
                );
            }
        }
    }

    fn bench_corpus(count: usize) -> Vec<Vec<u8>> {
        let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);
        let words: &[&[u8]] = &[
            b"the", b"Quick", b"brown", b"FOX", b"jumps", b"over", b"lazy", b"dog", b"Order",
            b"customer", b"Invoice", b"shipped", b"pending", b"Berlin", b"sofia", b"needlework",
        ];
        (0..count)
            .map(|_| {
                let target = 50 + rng.next(451);
                let mut v = Vec::with_capacity(target + 16);
                while v.len() < target {
                    v.extend_from_slice(words[rng.next(words.len())]);
                    v.push(b' ');
                }
                v
            })
            .collect()
    }

    /// `simd_compare_strings` as it was before the Unicode path: every
    /// operator's arm, the case-insensitive ones straight to the ASCII folders.
    #[inline(always)]
    fn old_dispatch(
        input1: &[u8],
        input2: &[u8],
        operation: &crate::core::processor::transformer::ComparerOperation,
    ) -> bool {
        use crate::core::processor::transformer::ComparerOperation;
        match operation {
            ComparerOperation::EqualsCaseInsensitive => super::ci_equals(input1, input2),
            ComparerOperation::ContainsCaseInsensitive => super::ci_contains(input1, input2),
            ComparerOperation::StartsWithCaseInsensitive => super::ci_starts_with(input1, input2),
            ComparerOperation::EndsWithCaseInsensitive => super::ci_ends_with(input1, input2),
            ComparerOperation::LikeCaseInsensitive => like_match(input1, input2, true),
            other => super::simd_compare_strings(input1, input2, other),
        }
    }

    /// What the Unicode check costs ASCII text: the dispatcher (two `is_ascii`
    /// passes, then the ASCII folder) against the folder called directly, which
    /// is what the dispatcher did before. Interleaved, five rounds.
    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// ilike_dispatch_ascii_bench --nocapture`.
    #[test]
    #[ignore]
    fn ilike_dispatch_ascii_bench() {
        use crate::core::processor::transformer::ComparerOperation as Op;
        use std::hint::black_box;
        let corpus = bench_corpus(1_000_000);
        let time = |label: &str, f: &dyn Fn(&[u8]) -> bool| {
            let t = std::time::Instant::now();
            let hits = corpus.iter().filter(|v| f(black_box(v))).count();
            println!("  {label:<52} {:>9.2?}  ({hits} hits)", t.elapsed());
        };
        for round in 0..5 {
            println!("round {round}");
            for needle in [&b"NEEDLE"[..], b"Customer Invoice"] {
                let name = String::from_utf8_lossy(needle);
                time(&format!("ICONTAINS '{name}' old dispatch"), &|v| {
                    old_dispatch(v, needle, &Op::ContainsCaseInsensitive)
                });
                time(&format!("ISTARTSWITH '{name}' old dispatch"), &|v| {
                    old_dispatch(v, needle, &Op::StartsWithCaseInsensitive)
                });
                time(&format!("ICONTAINS '{name}' direct"), &|v| super::ci_contains(v, needle));
                time(&format!("ICONTAINS '{name}' dispatch"), &|v| {
                    super::simd_compare_strings(v, needle, &Op::ContainsCaseInsensitive)
                });
                time(&format!("ISTARTSWITH '{name}' direct"), &|v| super::ci_starts_with(v, needle));
                time(&format!("ISTARTSWITH '{name}' dispatch"), &|v| {
                    super::simd_compare_strings(v, needle, &Op::StartsWithCaseInsensitive)
                });
            }
            let pattern = &b"%order%berlin%"[..];
            time("ILIKE '%order%berlin%' direct", &|v| like_match(v, pattern, true));
            time("ILIKE '%order%berlin%' dispatch", &|v| {
                super::simd_compare_strings(v, pattern, &Op::LikeCaseInsensitive)
            });
        }
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// ilike_like_bench --nocapture`. 1M values of 50-500 bytes.
    #[test]
    #[ignore]
    fn ilike_like_bench() {
        use std::hint::black_box;
        let corpus = bench_corpus(1_000_000);
        let time = |label: &str, f: &dyn Fn(&[u8]) -> bool| {
            let t = std::time::Instant::now();
            let hits = corpus.iter().filter(|v| f(black_box(v))).count();
            println!("  {label:<46} {:>9.2?}  ({hits} hits)", t.elapsed());
        };
        for round in 0..3 {
            println!("round {round}");
            for needle in [&b"NEEDLE"[..], b"zq", b"Customer Invoice", b"xyzzy-not-there"] {
                let name = String::from_utf8_lossy(needle);
                time(&format!("ILIKE '%{name}%' old"), &|v| reference_ci_contains(v, needle));
                time(&format!("ILIKE '%{name}%' new"), &|v| super::ci_contains(v, needle));
            }
            for pattern in [&b"the%fox%dog"[..], b"%order%berlin%", b"%zq%x%"] {
                let name = String::from_utf8_lossy(pattern);
                time(&format!("LIKE '{name}' old"), &|v| {
                    super::like_match_stepping(v, pattern, false)
                });
                time(&format!("LIKE '{name}' new"), &|v| like_match(v, pattern, false));
                time(&format!("ILIKE '{name}' old"), &|v| {
                    super::like_match_stepping(v, pattern, true)
                });
                time(&format!("ILIKE '{name}' new"), &|v| like_match(v, pattern, true));
            }
            // Patterns with `_` go to the stepping matcher.
            for pattern in [&b"%or_er%berlin%"[..], b"%c_stomer%", b"%zq_x%", b"the%_ox%"] {
                let name = String::from_utf8_lossy(pattern);
                time(&format!("LIKE '{name}' stepping old"), &|v| {
                    super::like_match_stepping_reference(v, pattern, false)
                });
                time(&format!("LIKE '{name}' stepping new"), &|v| like_match(v, pattern, false));
                time(&format!("ILIKE '{name}' stepping old"), &|v| {
                    super::like_match_stepping_reference(v, pattern, true)
                });
                time(&format!("ILIKE '{name}' stepping new"), &|v| like_match(v, pattern, true));
            }
        }
    }

    /// PostgreSQL's default escape: a backslash makes the next character
    /// literal, so `\_`, `\%` and `\\` match themselves. Django escapes every
    /// `_` and `%` in a `contains` lookup this way.
    #[test]
    fn a_backslash_escapes_the_next_character() {
        assert!(m("a_b", "a\\_b"));
        assert!(!m("axb", "a\\_b"), "an escaped underscore is not a wildcard");
        assert!(m("50%", "50\\%"));
        assert!(!m("500", "50\\%"), "an escaped percent is not a wildcard");
        assert!(!m("50%x", "50\\%"));
        assert!(m("a\\b", "a\\\\b"), "an escaped backslash is one backslash");
        assert!(!m("ab", "a\\\\b"));
        assert!(m("xa_by", "%a\\_b%"));
        assert!(!m("xaxby", "%a\\_b%"));
        assert!(m("xa_bzy", "%a\\_b%y"));
        assert!(!m("xaxbzy", "%a\\_b%y"));
        assert!(m("q%r_s", "_\\%_\\__"));
        assert!(!m("q%rxs", "_\\%_\\__"));
        assert!(m("ab", "\\a\\b"), "an escaped ordinary character is itself");
        // ILIKE folds escaped letters too.
        assert!(like_match(b"A_B", b"a\\_b", true));
        assert!(!like_match(b"AxB", b"a\\_b", true));
        assert!(like_match(b"zA_Bz%", b"%a\\_b%\\%", true));
    }

    #[test]
    fn folding_is_applied_to_both_sides() {
        assert!(like_match(b"UserTable:X", b"usertable:%", true));
        assert!(!like_match(b"UserTable:X", b"usertable:%", false));
    }
}

#[cfg(test)]
mod unicode_fold_tests {
    use super::simd_compare_strings;
    use crate::core::processor::transformer::ComparerOperation;

    fn ci(a: &str, b: &str, op: ComparerOperation) -> bool {
        simd_compare_strings(a.as_bytes(), b.as_bytes(), &op)
    }

    /// `ci_compare` trusts the ASCII folder on the strength of this: only two
    /// non-ASCII characters lower to anything with ASCII in it, and only one
    /// lowers to more than one character. Checked over every code point, as
    /// both `char::to_lowercase` and `str::to_lowercase` (which differs for a
    /// final sigma) compute it.
    #[test]
    fn lowering_reaches_ascii_exactly_twice() {
        let mut reaches = Vec::new();
        let mut widens = Vec::new();
        for c in (0x80u32..=0x10FFFF).filter_map(char::from_u32) {
            let lowered: String = c.to_lowercase().collect();
            for l in [lowered.clone(), c.to_string().to_lowercase(), format!("a{}", c).to_lowercase()[1..].to_string()] {
                if l.bytes().any(|b| b.is_ascii()) && !reaches.contains(&c) {
                    reaches.push(c);
                }
                if l.chars().count() != 1 && !widens.contains(&c) {
                    widens.push(c);
                }
            }
        }
        assert_eq!(reaches, vec!['\u{130}', '\u{212A}']);
        assert_eq!(widens, vec!['\u{130}']);
        // And the lead bytes `ci_compare` scans the value for.
        assert_eq!('\u{130}'.to_string().as_bytes()[0], 0xC4);
        assert_eq!('\u{212A}'.to_string().as_bytes()[0], 0xE2);
    }

    #[test]
    fn pattern_classification_matches_a_byte_loop() {
        let mut seed = 0x1234_5678_9ABC_DEF1u64;
        for len in 0..40 {
            for _ in 0..2000 {
                let p: Vec<u8> = (0..len)
                    .map(|_| {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        // Mostly the interesting bytes and their neighbours.
                        let pick = b"kKiIjJlLhH_^`0\x7f\x80\xff\xc4\xe2aZ";
                        if seed % 4 == 0 { (seed >> 8) as u8 } else { pick[(seed >> 8) as usize % pick.len()] }
                    })
                    .collect();
                let want = (
                    p.iter().any(|b| *b >= 0x80),
                    p.iter().any(|b| matches!(b, b'k' | b'K' | b'i' | b'I')),
                    p.contains(&b'_'),
                );
                // `reach` is only consulted for ASCII patterns.
                let got = super::classify_pattern(&p);
                assert_eq!(got.0, want.0, "{:?}", p);
                if !want.0 {
                    assert_eq!(got, want, "{:?}", p);
                }
            }
        }
    }

    /// The cases PostgreSQL answers `true` and the ASCII-only folder answered
    /// `false`: a non-ASCII letter in another case.
    #[test]
    fn non_ascii_letters_fold_like_lower() {
        use ComparerOperation::*;
        assert!(ci("é", "É", EqualsCaseInsensitive));
        assert!(ci("crème brûlée", "CRÈME", StartsWithCaseInsensitive));
        assert!(ci("crème brûlée", "BRÛLÉE", EndsWithCaseInsensitive));
        assert!(ci("Ελληνικά κείμενα", "ΚΕΊΜΕΝΑ", ContainsCaseInsensitive));
        assert!(ci("ПРИВЕТ мир", "привет%", LikeCaseInsensitive));
        assert!(ci("ПРИВЕТ мир", "_РИВЕТ МИР", LikeCaseInsensitive));
        assert!(ci("Ünïcödé", "üNÏCÖDÉ", LikeCaseInsensitive));
        // Lowering changes length: the Kelvin sign is three bytes and lowers to
        // ASCII `k`, so a length-first shortcut would refuse this.
        assert!(ci("\u{212A}elvin", "kelvin", EqualsCaseInsensitive));
        assert!(ci("100\u{212A}", "%k", LikeCaseInsensitive));
        // Still not equal where the lowered forms differ.
        assert!(!ci("é", "e", EqualsCaseInsensitive));
        assert!(!ci("ß", "SS", EqualsCaseInsensitive), "lower() does not case-fold ß");
        assert!(!ci("crème", "CREME", ContainsCaseInsensitive));
        // Escapes and wildcards survive lowering.
        assert!(ci("É_x", "é\\_X", LikeCaseInsensitive));
        assert!(!ci("Éyx", "é\\_X", LikeCaseInsensitive));
    }

    /// For text, every case-insensitive operator must equal its case-sensitive
    /// counterpart applied to `to_lowercase` of both sides — what `ILIKE` is in
    /// PostgreSQL and what `lower()` computes here. ASCII inputs included, so
    /// the fast path is held to the same definition.
    #[test]
    fn every_operator_matches_lowered_comparison() {
        use ComparerOperation::*;
        let alphabet = [
            "a", "A", "é", "É", "ß", "\u{212A}", "k", "K", "İ", "i", "I", "Σ", "ς", "σ", "%", "_", "x",
            "\u{307}", "€", "’",
        ];
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = |n: usize| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % n as u64) as usize
        };
        for _ in 0..100_000 {
            let mut make = |max: usize| -> String {
                let len = next(max + 1);
                (0..len).map(|_| alphabet[next(alphabet.len())]).collect()
            };
            let a = make(12);
            let b = make(3);
            let (la, lb) = (a.to_lowercase(), b.to_lowercase());
            let cases = [
                (EqualsCaseInsensitive, la == lb),
                (ContainsCaseInsensitive, la.contains(lb.as_str())),
                (StartsWithCaseInsensitive, la.starts_with(lb.as_str())),
                (EndsWithCaseInsensitive, la.ends_with(lb.as_str())),
                (LikeCaseInsensitive, super::like_match(la.as_bytes(), lb.as_bytes(), false)),
            ];
            for (op, want) in cases {
                assert_eq!(ci(&a, &b, op.clone()), want, "{:?} {:?} {:?}", a, op, b);
            }
        }
    }
}
