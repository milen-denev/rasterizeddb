//! Canonical value hashing for the bit-sliced filter index.
//!
//! # The invariant this module exists to uphold
//!
//! > For any two byte slices `a` and `b` of a given [`DbType`], if
//! > `perform_comparison_operation(a, b, ty, Equals) == Ok(true)`
//! > then `canonical_hash(ty, a, seed) == canonical_hash(ty, b, seed)`.
//!
//! Violating it produces **false negatives** — rows that satisfy a `WHERE`
//! clause but are pruned before the evaluator ever sees them. That is the one
//! failure mode the whole filter design promises never to produce, so the
//! invariant is asserted directly against `perform_comparison_operation` in
//! `super::tests` rather than being assumed.
//!
//! Two facts about the engine make this tractable:
//!
//! 1. `perform_comparison_operation` performs **no type coercion**. Both
//!    operands must be exactly `size_of::<T>()` bytes and are compared as the
//!    same `T`, so for integers "equal" means "bitwise identical".
//! 2. Only `NULL`, `UNKNOWN`, `START` and `END` are refused outright, and those
//!    are internal sentinels rather than column types — see [`is_indexable`].
//!    `BOOL` is comparable and therefore indexable, but carries zone bounds
//!    only and is never hashed; see [`is_fingerprintable`].
//!
//! The only types where distinct byte patterns compare equal are floats:
//! `±0.0`, and every NaN payload (PostgreSQL's NaN = NaN), both of which
//! [`canonical_hash`] normalises explicitly.

use crate::core::db_type::DbType;

/// Fingerprint reserved for an empty / deleted / not-yet-indexed slot.
///
/// A zeroed region of plane storage therefore reads as "matches nothing",
/// which is what makes a freshly grown zone and a cleared slot safe by default.
pub const CODE_EMPTY: u32 = 0;

/// Fingerprint reserved for SQL `NULL`.
pub const CODE_NULL: u32 = 1;

/// First fingerprint code available to real values.
pub const CODE_FIRST: u32 = 2;

/// Hash seed. Must be **stable across process restarts** — a randomised seed
/// (e.g. `ahash::RandomState::new()`) would silently invalidate every persisted
/// fingerprint on restart and produce false negatives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HashSeed {
    pub k0: u64,
    pub k1: u64,
}

impl Default for HashSeed {
    fn default() -> Self {
        // Fixed constants, not randomness: determinism is the requirement.
        Self {
            k0: 0x243F_6A88_85A3_08D3,
            k1: 0x1319_8A2E_0370_7344,
        }
    }
}

/// Whether a column of this type can be filtered at all.
///
/// The rule is "everything `perform_comparison_operation` can compare".
/// `NULL`, `UNKNOWN`, `START` and `END` are internal sentinels, not column
/// types, and nothing else is left out.
///
/// Being indexable is **not** the same as carrying fingerprints — see
/// [`is_fingerprintable`]. `BOOL` is indexed for its zone bounds alone.
#[inline]
pub const fn is_indexable(db_type: &DbType) -> bool {
    matches!(
        db_type,
        DbType::I8
            | DbType::I16
            | DbType::I32
            | DbType::I64
            | DbType::I128
            | DbType::U8
            | DbType::U16
            | DbType::U32
            | DbType::U64
            | DbType::U128
            | DbType::F32
            | DbType::F64
            | DbType::STRING
            | DbType::DATETIME
            | DbType::CHAR
            | DbType::DECIMAL
            | DbType::UUID
            | DbType::INET
            | DbType::BOOL
    )
}

/// Whether a column of this type earns per-row fingerprint planes.
///
/// `BOOL` is the single exclusion, and the reasoning that excluded it from the
/// index entirely still holds *for fingerprints*: two distinct values can never
/// clear the selectivity gate in [`super::column::choose_f`], so `f` bits per
/// row would buy a mask that returns half the table.
///
/// Zone bounds are a different trade. Two values per 512-row zone is **0.03
/// bits/row** for a one-byte type — a fiftieth of what `f = 4` fingerprints
/// cost — and for a boolean whose true values are *rare* they prune almost
/// everything: a zone holding no `1` has `min = max = 0`, so `flag = 1` rules
/// the whole zone out.
///
/// That case is not hypothetical. Nullability is implemented as a companion
/// `BOOL` column per nullable column ([`crate::core::rql::nullable`]), and
/// `predicate_rewrite` lowers `x IS NULL` to `__null_x = 1`. A sparsely-null
/// column is exactly the shape zone bounds are best at, and it arrives on every
/// nullable column for free.
#[inline]
pub const fn is_fingerprintable(db_type: &DbType) -> bool {
    is_indexable(db_type) && !matches!(db_type, DbType::BOOL)
}

/// Serialized width the comparison path demands for a fixed-width type.
/// `None` for `STRING` (variable) and for non-comparable types.
#[inline]
const fn fixed_width(db_type: &DbType) -> Option<usize> {
    Some(match db_type {
        DbType::I8 | DbType::U8 => 1,
        DbType::I16 | DbType::U16 => 2,
        DbType::I32 | DbType::U32 | DbType::F32 | DbType::CHAR => 4,
        DbType::I64 | DbType::U64 | DbType::F64 | DbType::DATETIME => 8,
        DbType::I128 | DbType::U128 | DbType::DECIMAL => 16,
        DbType::UUID => crate::core::value_codec::UUID_LEN,
        DbType::INET => crate::core::value_codec::INET_LEN,
        _ => return None,
    })
}

#[inline(always)]
fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

/// Deterministic hash over a byte slice, with the length mixed in so that
/// `"ab"` and `"ab\0"` cannot collide structurally.
#[inline]
fn hash_bytes(b: &[u8], seed: HashSeed) -> u64 {
    let mut acc = seed.k0 ^ (b.len() as u64).wrapping_mul(seed.k1);
    let mut rest = b;

    while rest.len() >= 8 {
        let v = u64::from_le_bytes([
            rest[0], rest[1], rest[2], rest[3], rest[4], rest[5], rest[6], rest[7],
        ]);
        acc = mix(acc ^ v).wrapping_add(seed.k1);
        rest = &rest[8..];
    }

    if !rest.is_empty() {
        let mut buf = [0u8; 8];
        buf[..rest.len()].copy_from_slice(rest);
        acc = mix(acc ^ u64::from_le_bytes(buf));
    }

    mix(acc)
}

/// Hash a column value in its canonical form.
///
/// Returns `None` when the value cannot be indexed — a non-comparable type, or
/// a slice whose width does not match the column type. A width mismatch makes
/// `perform_comparison_operation` return `EvalError::OperandSizeMismatch`,
/// which fails the whole query; the filter must not prune on that basis.
#[inline]
pub fn canonical_hash(db_type: &DbType, bytes: &[u8], seed: HashSeed) -> Option<u64> {
    if matches!(db_type, DbType::STRING) {
        // `simd_equals` compares length first, then bytes. Raw bytes are canonical.
        return Some(hash_bytes(bytes, seed));
    }

    let width = fixed_width(db_type)?;
    if bytes.len() != width {
        return None;
    }

    match db_type {
        // IEEE-754: `-0.0 == +0.0` is true, so the two bit patterns MUST hash
        // alike or `WHERE x = 0.0` would prune rows storing `-0.0`.
        //
        // NaN: the evaluator follows PostgreSQL, where every NaN equals every
        // other, so all NaN bit patterns hash as the canonical one. (The sarg
        // builder never probes with a NaN literal either, so a fingerprint
        // persisted before this canonicalisation is never asked about.)
        DbType::F32 => {
            let v = f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            let v = if v == 0.0 { 0.0f32 } else if v.is_nan() { f32::NAN } else { v };
            Some(mix(seed.k0 ^ u64::from(v.to_bits())))
        }
        DbType::F64 => {
            let mut a = [0u8; 8];
            a.copy_from_slice(bytes);
            let v = f64::from_le_bytes(a);
            let v = if v == 0.0 { 0.0f64 } else if v.is_nan() { f64::NAN } else { v };
            Some(mix(seed.k0 ^ v.to_bits()))
        }
        // Everything else — integers, `DATETIME`/`CHAR`/`DECIMAL` (compared as
        // `i64`/`u32`/`i128`), and `UUID`/`INET` (compared by `memcmp`) — is
        // bitwise identical iff equal, so hashing the raw bytes is exact.
        _ => Some(hash_bytes(bytes, seed)),
    }
}

/// Extract an `f`-bit fingerprint from a hash.
///
/// Taken from the **high** half of the hash so the low half stays free for a
/// future L0/L1 block address without the two uses correlating.
///
/// Codes `0` and `1` are reserved (empty/deleted and NULL), so the two lowest
/// live codes are slightly overloaded and the true false-positive rate is
/// `~1/(2^f - 2)` rather than `2^-f`.
#[inline(always)]
pub fn fingerprint(h: u64, f: u8) -> u32 {
    debug_assert!(f >= 4 && f <= 16, "f out of range: {}", f);
    let mask = (1u32 << f) - 1;
    let g = ((h >> 32) as u32) & mask;
    if g < CODE_FIRST { g + CODE_FIRST } else { g }
}
