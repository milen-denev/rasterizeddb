//! Index keys: column values turned into bytes an index can order or hash.
//!
//! # Correct by agreement with the evaluator
//!
//! An index answers "which rows might match", and the scan then re-checks
//! every candidate with the real predicate. So an index may return too much,
//! but never too little: a key that sorts on the wrong side of a bound, or
//! hashes apart from a value the evaluator calls equal, drops a matching row
//! with nothing reporting it.
//!
//! So every encoding here is pinned against
//! [`perform_comparison_operation`], the function `WHERE` itself evaluates
//! with, by the property tests at the bottom of this file. A type gets an
//! encoding only once that agreement is proven. Every other type is refused
//! when the index is declared. It is never indexed approximately.
//!
//! # B-tree: order-preserving bytes
//!
//! `encode_ordered` produces bytes whose `memcmp` order is the evaluator's
//! order, so the B-tree is a plain `BTreeMap<Box<[u8]>, …>`: no per-comparison
//! type dispatch, and composite keys are concatenations.
//!
//! - Signed integers, `DATETIME` and `DECIMAL` (stored little-endian): written
//!   big-endian with the sign bit flipped, so negatives sort first.
//! - Unsigned integers and `BOOL`: big-endian.
//! - Floats: PostgreSQL's order, not IEEE's. `-0` equals `+0`, NaN equals NaN
//!   and sorts above everything. The usual trick (flip every bit of a negative,
//!   flip the sign bit of a positive) after canonicalising both.
//! - `DATE` and `TIME`: stored as PostgreSQL's big-endian wire format,
//!   so only the sign bit needs flipping.
//! - `UUID`, `INET`, `MACADDR`, `MACADDR8`: already big-endian, already `memcmp`
//!   order.
//! - `STRING`: its bytes. A string that is not the last component is escaped
//!   (`0x00` → `0x00 0xFF`) and terminated with `0x00 0x00`, so a shorter
//!   string sorts before its extensions and the next component cannot leak
//!   into the comparison.
//!
//! `CHAR` is deliberately absent. The evaluator orders it as a
//! little-endian `u32`, while the sort path orders it by raw bytes. Until those
//! agree, there is no order an index could keep that both would accept.
//!
//! # Hash: canonical bytes
//!
//! `encode_canonical` produces bytes that are equal exactly when the evaluator
//! calls the values equal. That is byte equality for most types, after
//! canonicalising floats. `INTERVAL` is refused for this reason:
//! `'1 day' = '24 hours'` is true in PostgreSQL with different bytes. So are
//! `TIMETZ` (two spellings of one instant) and the geometric and range types,
//! whose equality is not byte equality either.

use crate::core::db_type::DbType;

/// The version of the bytes [`encode_ordered`] and [`encode_canonical`]
/// produce, `jsonb::canonical_key` included. A persisted index records it and
/// is rebuilt when it differs.
///
/// Bump rule: any change that moves one output byte for any input, a
/// composite key's composition included, bumps this. The golden vectors in
/// `core::index::vectors` fail on such a change; freeze the new outputs as
/// the next set, raise this constant and append that set's history row,
/// leaving the old set and its row as they are (the module doc there has the
/// steps). Vectors that only add coverage go in its `*_ADDED` tables with no
/// bump. A type newly supported by [`btree_supported`] or [`hash_supported`]
/// needs vectors and a property test against the evaluator before the
/// coverage test passes.
pub const ENCODING_VERSION: u32 = 1;

/// Types a B-tree can order: those with a proven order-preserving encoding.
pub fn btree_supported(t: &DbType) -> bool {
    matches!(
        t,
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
            | DbType::BOOL
            | DbType::F32
            | DbType::F64
            | DbType::DECIMAL
            | DbType::DATETIME
            | DbType::DATE
            | DbType::TIME
            | DbType::UUID
            | DbType::INET
            | DbType::MACADDR
            | DbType::MACADDR8
            | DbType::STRING
    )
}

/// Types a hash index can key: those whose equality is canonical-byte equality.
pub fn hash_supported(t: &DbType) -> bool {
    btree_supported(t) || matches!(t, DbType::CHAR | DbType::JSONB)
}

/// Types where only equality is ever asked: an order exists, but nothing
/// queries a range of it (a random UUID, a document, a hardware address).
/// When no method is named, these get a hash index: one probe instead of a
/// descent, and no ordered structure to keep.
pub fn equality_only(t: &DbType) -> bool {
    matches!(
        t,
        DbType::UUID | DbType::JSONB | DbType::MACADDR | DbType::MACADDR8 | DbType::CHAR
    )
}

#[inline]
fn flip_sign_be(bytes_le: &[u8], out: &mut Vec<u8>) {
    let start = out.len();
    out.extend(bytes_le.iter().rev());
    out[start] ^= 0x80;
}

fn canonical_f64(v: f64) -> f64 {
    if v.is_nan() {
        f64::NAN
    } else if v == 0.0 {
        0.0
    } else {
        v
    }
}

fn canonical_f32(v: f32) -> f32 {
    if v.is_nan() {
        f32::NAN
    } else if v == 0.0 {
        0.0
    } else {
        v
    }
}

/// Appends the order-preserving form of one value. `last` says whether
/// another component follows (only strings care). `None` for a type with no
/// proven encoding, or a value of the wrong width.
pub fn encode_ordered(t: &DbType, v: &[u8], last: bool, out: &mut Vec<u8>) -> Option<()> {
    let width = |n: usize| (v.len() == n).then_some(());
    match t {
        DbType::I8 => {
            width(1)?;
            flip_sign_be(v, out)
        }
        DbType::I16 => {
            width(2)?;
            flip_sign_be(v, out)
        }
        DbType::I32 => {
            width(4)?;
            flip_sign_be(v, out)
        }
        DbType::I64 | DbType::DATETIME => {
            width(8)?;
            flip_sign_be(v, out)
        }
        DbType::I128 | DbType::DECIMAL => {
            width(16)?;
            flip_sign_be(v, out)
        }
        DbType::U8 | DbType::BOOL => {
            width(1)?;
            out.push(v[0])
        }
        DbType::U16 => {
            width(2)?;
            out.extend(v.iter().rev())
        }
        DbType::U32 => {
            width(4)?;
            out.extend(v.iter().rev())
        }
        DbType::U64 => {
            width(8)?;
            out.extend(v.iter().rev())
        }
        DbType::U128 => {
            width(16)?;
            out.extend(v.iter().rev())
        }
        DbType::F32 => {
            width(4)?;
            let f = canonical_f32(f32::from_le_bytes(v.try_into().ok()?));
            let bits = f.to_bits();
            let key = if bits & 0x8000_0000 != 0 { !bits } else { bits ^ 0x8000_0000 };
            out.extend_from_slice(&key.to_be_bytes())
        }
        DbType::F64 => {
            width(8)?;
            let f = canonical_f64(f64::from_le_bytes(v.try_into().ok()?));
            let bits = f.to_bits();
            let key = if bits & 0x8000_0000_0000_0000 != 0 {
                !bits
            } else {
                bits ^ 0x8000_0000_0000_0000
            };
            out.extend_from_slice(&key.to_be_bytes())
        }
        DbType::DATE => {
            width(4)?;
            let start = out.len();
            out.extend_from_slice(v);
            out[start] ^= 0x80;
        }
        DbType::TIME => {
            width(8)?;
            let start = out.len();
            out.extend_from_slice(v);
            out[start] ^= 0x80;
        }
        DbType::UUID | DbType::INET | DbType::MACADDR | DbType::MACADDR8 => out.extend_from_slice(v),
        DbType::STRING => {
            if last {
                out.extend_from_slice(v);
            } else {
                for &b in v {
                    out.push(b);
                    if b == 0 {
                        out.push(0xFF);
                    }
                }
                out.extend_from_slice(&[0, 0]);
            }
        }
        _ => return None,
    }
    Some(())
}

/// Appends the canonical (hash) form of one value. Length-prefixed, so a
/// composite key cannot be ambiguous about where one component ends.
pub fn encode_canonical(t: &DbType, v: &[u8], out: &mut Vec<u8>) -> Option<()> {
    if !hash_supported(t) {
        return None;
    }
    match t {
        DbType::F32 if v.len() == 4 => {
            let f = canonical_f32(f32::from_le_bytes(v.try_into().ok()?));
            out.extend_from_slice(&4u32.to_le_bytes());
            out.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        DbType::F64 if v.len() == 8 => {
            let f = canonical_f64(f64::from_le_bytes(v.try_into().ok()?));
            out.extend_from_slice(&8u32.to_le_bytes());
            out.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        DbType::F32 | DbType::F64 => return None,
        // Documents are equal as values (`1` = `1.0`), not as bytes; one the
        // walk cannot read is compared, and so keyed, by its bytes.
        DbType::JSONB => {
            let start = out.len();
            out.extend_from_slice(&[0u8; 4]);
            if crate::core::jsonb::canonical_key(v, out).is_some() {
                let len = (out.len() - start - 4) as u32;
                out[start..start + 4].copy_from_slice(&len.to_le_bytes());
            } else {
                out.truncate(start);
                out.extend_from_slice(&(v.len() as u32).to_le_bytes());
                out.extend_from_slice(v);
            }
        }
        _ => {
            out.extend_from_slice(&(v.len() as u32).to_le_bytes());
            out.extend_from_slice(v);
        }
    }
    Some(())
}

/// The smallest byte string greater than every string that starts with
/// `prefix`, or `None` when `prefix` is all `0xFF` (nothing bounds it above).
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    crate::core::filter::zonemap::successor(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::processor::transformer::ComparerOperation;
    use crate::core::row::logical::perform_comparison_operation;
    use std::cmp::Ordering;

    /// The evaluator's own verdict on `a` against `b`.
    fn evaluator_order(t: &DbType, a: &[u8], b: &[u8]) -> Ordering {
        let less = perform_comparison_operation(a, b, t, &ComparerOperation::Less).unwrap();
        let equal = perform_comparison_operation(a, b, t, &ComparerOperation::Equals).unwrap();
        if less {
            Ordering::Less
        } else if equal {
            Ordering::Equal
        } else {
            Ordering::Greater
        }
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn bytes(&mut self, n: usize) -> Vec<u8> {
            (0..n).map(|_| self.next() as u8).collect()
        }
    }

    /// Random values of a type, biased toward the edges where encodings break:
    /// zero, the sign boundary, the extremes, NaN and -0 for floats.
    fn sample(t: &DbType, r: &mut Rng) -> Vec<u8> {
        let edge = r.next() % 4 == 0;
        let fixed = |n: usize, r: &mut Rng| -> Vec<u8> {
            if edge {
                match r.next() % 4 {
                    0 => vec![0; n],
                    1 => vec![0xFF; n],
                    2 => {
                        let mut v = vec![0; n];
                        v[n - 1] = 0x80;
                        v
                    }
                    _ => {
                        let mut v = vec![0xFF; n];
                        v[n - 1] = 0x7F;
                        v
                    }
                }
            } else {
                r.bytes(n)
            }
        };
        match t {
            DbType::I8 | DbType::U8 => fixed(1, r),
            DbType::BOOL => vec![(r.next() % 2) as u8],
            DbType::I16 | DbType::U16 => fixed(2, r),
            DbType::I32 | DbType::U32 => fixed(4, r),
            DbType::I64 | DbType::U64 | DbType::DATETIME => fixed(8, r),
            DbType::I128 | DbType::U128 | DbType::DECIMAL => fixed(16, r),
            DbType::F32 => {
                let specials = [0.0f32, -0.0, f32::NAN, -f32::NAN, f32::INFINITY, f32::NEG_INFINITY, 1.5, -1.5];
                if edge {
                    specials[(r.next() % specials.len() as u64) as usize].to_le_bytes().to_vec()
                } else {
                    let f = f32::from_bits(r.next() as u32);
                    f.to_le_bytes().to_vec()
                }
            }
            DbType::F64 => {
                let specials = [0.0f64, -0.0, f64::NAN, -f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.5, -1.5];
                if edge {
                    specials[(r.next() % specials.len() as u64) as usize].to_le_bytes().to_vec()
                } else {
                    f64::from_bits(r.next()).to_le_bytes().to_vec()
                }
            }
            DbType::DATE => {
                let mut v = fixed(4, r);
                v.reverse();
                v
            }
            DbType::TIME => {
                let mut v = fixed(8, r);
                v.reverse();
                v
            }
            DbType::UUID => r.bytes(crate::core::value_codec::UUID_LEN),
            DbType::INET => r.bytes(crate::core::value_codec::INET_LEN),
            DbType::MACADDR => r.bytes(6),
            DbType::MACADDR8 => r.bytes(8),
            // A code point, little-endian: mostly from a few, so equal pairs
            // are common, otherwise any four bytes (the evaluator compares
            // the `u32`, valid character or not).
            DbType::CHAR => {
                if r.next() % 2 == 0 {
                    let pick = [0u32, 'a' as u32, 'b' as u32, 'é' as u32, 0x10_FFFF];
                    pick[(r.next() % pick.len() as u64) as usize].to_le_bytes().to_vec()
                } else {
                    fixed(4, r)
                }
            }
            DbType::STRING => {
                // Short strings over a tiny alphabet with 0x00 and 0xFF in it,
                // so prefixes, equal values and escapes all occur often.
                let n = (r.next() % 5) as usize;
                (0..n).map(|_| [0u8, 1, b'a', b'b', 0xFF][(r.next() % 5) as usize]).collect()
            }
            _ => unreachable!(),
        }
    }

    const ORDERED: &[DbType] = &[
        DbType::I8,
        DbType::I16,
        DbType::I32,
        DbType::I64,
        DbType::I128,
        DbType::U8,
        DbType::U16,
        DbType::U32,
        DbType::U64,
        DbType::U128,
        DbType::BOOL,
        DbType::F32,
        DbType::F64,
        DbType::DECIMAL,
        DbType::DATETIME,
        DbType::DATE,
        DbType::TIME,
        DbType::UUID,
        DbType::INET,
        DbType::MACADDR,
        DbType::MACADDR8,
        DbType::STRING,
    ];

    /// Hash-only types, property-tested by
    /// `canonical_encoding_is_equal_exactly_when_the_evaluator_says_equal`.
    const CANONICAL_ONLY: &[DbType] = &[DbType::CHAR];

    /// Supported types whose evaluator agreement is proven elsewhere: JSONB's
    /// canonical key by `jsonb`'s
    /// `document_equality_is_value_equality_and_keys_agree_with_it`.
    const PROPERTY_TESTED_ELSEWHERE: &[DbType] = &[DbType::JSONB];

    /// Every `DbType`. The match is exhaustive, so a new variant does not
    /// compile until it is listed here, and listing it puts it through the
    /// coverage test below.
    macro_rules! every_type {
        ($($v:ident),* $(,)?) => {{
            fn exhaustive(t: &DbType) {
                match t {
                    $(DbType::$v)|* => {}
                    // The one variant with a payload: reporting-only.
                    DbType::BPCHAR(_) => {}
                }
            }
            let _ = exhaustive;
            [$(DbType::$v,)* DbType::BPCHAR(0)]
        }};
    }

    /// Owner's invariant 5, made a gate: a type an index may key must have
    /// golden vectors (so its bytes cannot move unnoticed) and a property test
    /// against the evaluator (so they are right). `CHAR` lacked the second.
    #[test]
    fn every_supported_type_has_vectors_and_a_property_test() {
        use crate::core::index::vectors;
        let all = every_type!(
            I8, I16, I32, I64, I128, U8, U16, U32, U64, U128, F32, F64, BOOL, CHAR, STRING, DATETIME, UUID, INET,
            BYTEA, ARRAY, DECIMAL, JSON, JSONB, INTERVAL, MACADDR, MACADDR8, BIT, VARBIT, DATE, TIME, TIMETZ, POINT,
            LINE, LSEG, BOX, PATH, POLYGON, CIRCLE, INT4RANGE, INT8RANGE, NUMRANGE, TSRANGE, TSTZRANGE, DATERANGE,
            UNKNOWN, NULL, START, END,
        );
        let mut checked = 0;
        for t in &all {
            if btree_supported(t) {
                assert!(ORDERED.contains(t), "{t:?} is btree_supported with no ordering property test");
                assert!(
                    vectors::ORDERED.iter().chain(vectors::ORDERED_ADDED).any(|(v, ..)| v == t),
                    "{t:?} is btree_supported with no encode_ordered vectors"
                );
                checked += 1;
            }
            if hash_supported(t) {
                assert!(
                    ORDERED.contains(t) || CANONICAL_ONLY.contains(t) || PROPERTY_TESTED_ELSEWHERE.contains(t),
                    "{t:?} is hash_supported with no canonical property test"
                );
                let vectored = if *t == DbType::JSONB {
                    !vectors::JSONB_CANONICAL.is_empty() || !vectors::JSONB_CANONICAL_ADDED.is_empty()
                } else {
                    vectors::CANONICAL.iter().chain(vectors::CANONICAL_ADDED).any(|(v, ..)| v == t)
                };
                assert!(vectored, "{t:?} is hash_supported with no encode_canonical vectors");
                checked += 1;
            }
        }
        assert!(checked >= 46, "the walk checked {checked}");
        // The lists name only supported types, so none hides a stale entry.
        for t in CANONICAL_ONLY.iter().chain(PROPERTY_TESTED_ELSEWHERE) {
            assert!(hash_supported(t) && !btree_supported(t), "{t:?}");
        }
    }

    #[test]
    fn every_ordered_type_is_listed_and_supported() {
        for t in ORDERED {
            assert!(btree_supported(t), "{t:?}");
        }
        // Every supported type is tested below.
        for t in [DbType::CHAR, DbType::INTERVAL, DbType::TIMETZ, DbType::JSONB, DbType::ARRAY, DbType::POINT] {
            assert!(!btree_supported(&t), "{t:?} has no proven order");
        }
    }

    #[test]
    fn ordered_encoding_agrees_with_the_evaluator() {
        let mut r = Rng(0x9E37_79B9_7F4A_7C15);
        for t in ORDERED {
            for _ in 0..20_000 {
                let (a, b) = (sample(t, &mut r), sample(t, &mut r));
                for last in [true, false] {
                    let (mut ea, mut eb) = (Vec::new(), Vec::new());
                    encode_ordered(t, &a, last, &mut ea).unwrap();
                    encode_ordered(t, &b, last, &mut eb).unwrap();
                    assert_eq!(
                        ea.cmp(&eb),
                        evaluator_order(t, &a, &b),
                        "{t:?} last={last}: {a:?} vs {b:?} encoded {ea:?} vs {eb:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn composite_keys_order_component_by_component() {
        // (STRING, I32): the string decides first, then the integer, and a
        // string that is a prefix of another sorts first whatever follows it.
        let mut r = Rng(42);
        for _ in 0..20_000 {
            let (s1, s2) = (sample(&DbType::STRING, &mut r), sample(&DbType::STRING, &mut r));
            let (i1, i2) = (sample(&DbType::I32, &mut r), sample(&DbType::I32, &mut r));
            let key = |s: &[u8], i: &[u8]| {
                let mut k = Vec::new();
                encode_ordered(&DbType::STRING, s, false, &mut k).unwrap();
                encode_ordered(&DbType::I32, i, true, &mut k).unwrap();
                k
            };
            let expect = evaluator_order(&DbType::STRING, &s1, &s2)
                .then_with(|| evaluator_order(&DbType::I32, &i1, &i2));
            assert_eq!(key(&s1, &i1).cmp(&key(&s2, &i2)), expect, "{s1:?},{i1:?} vs {s2:?},{i2:?}");
        }
    }

    #[test]
    fn canonical_encoding_is_equal_exactly_when_the_evaluator_says_equal() {
        let mut r = Rng(7);
        for t in ORDERED.iter().chain(CANONICAL_ONLY) {
            for _ in 0..20_000 {
                let (a, b) = (sample(t, &mut r), sample(t, &mut r));
                let (mut ea, mut eb) = (Vec::new(), Vec::new());
                encode_canonical(t, &a, &mut ea).unwrap();
                encode_canonical(t, &b, &mut eb).unwrap();
                let equal = perform_comparison_operation(&a, &b, t, &ComparerOperation::Equals).unwrap();
                assert_eq!(ea == eb, equal, "{t:?}: {a:?} vs {b:?}");
            }
        }
        // -0 and +0, and every NaN, are one key each.
        let (mut a, mut b) = (Vec::new(), Vec::new());
        encode_canonical(&DbType::F64, &(-0.0f64).to_le_bytes(), &mut a).unwrap();
        encode_canonical(&DbType::F64, &0.0f64.to_le_bytes(), &mut b).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn interval_is_refused_because_equal_values_differ_in_bytes() {
        assert!(!hash_supported(&DbType::INTERVAL));
        assert!(!hash_supported(&DbType::TIMETZ));
        assert!(encode_canonical(&DbType::INTERVAL, &[0; 16], &mut Vec::new()).is_none());
    }
}
