//! The invariant everything else depends on:
//!
//! > if `perform_comparison_operation(a, b, ty, Equals)` is true,
//! > then `canonical_hash(ty, a) == canonical_hash(ty, b)`.
//!
//! Violating it produces **false negatives** — rows that match a `WHERE` clause
//! but are pruned before the evaluator sees them. Everything here asserts that
//! one direction only: the converse (a collision) is a false positive, which is
//! allowed and filtered out downstream.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::filter::hash::*;
use rasterizeddb_core::core::processor::transformer::ComparerOperation;
use rasterizeddb_core::core::row::logical::perform_comparison_operation;
use rasterizeddb_filter_tests::*;

#[test]
fn equality_implies_equal_hashes_for_every_indexable_type() {
    let seed = HashSeed::default();
    for ty in INDEXABLE {
        let samples = sample_values(ty, 48);
        for a in &samples {
            for b in &samples {
                let eq = perform_comparison_operation(a, b, ty, &ComparerOperation::Equals)
                    .expect("indexable types must be comparable");
                if !eq {
                    continue;
                }
                let (ha, hb) = (
                    canonical_hash(ty, a, seed),
                    canonical_hash(ty, b, seed),
                );
                assert_eq!(
                    ha, hb,
                    "{:?}: {:?} and {:?} compare Equal but hash differently",
                    ty, a, b
                );
                assert!(ha.is_some(), "{:?}: a comparable value must be hashable", ty);
            }
        }
    }
}

#[test]
fn float_signed_zeros_hash_alike() {
    let seed = HashSeed::default();

    let (p, n) = (0.0f32.to_le_bytes(), (-0.0f32).to_le_bytes());
    assert_ne!(p, n, "the bit patterns must actually differ");
    assert!(perform_comparison_operation(&p, &n, &DbType::F32, &ComparerOperation::Equals).unwrap());
    assert_eq!(
        canonical_hash(&DbType::F32, &p, seed),
        canonical_hash(&DbType::F32, &n, seed)
    );

    let (p, n) = (0.0f64.to_le_bytes(), (-0.0f64).to_le_bytes());
    assert_ne!(p, n);
    assert!(perform_comparison_operation(&p, &n, &DbType::F64, &ComparerOperation::Equals).unwrap());
    assert_eq!(
        canonical_hash(&DbType::F64, &p, seed),
        canonical_hash(&DbType::F64, &n, seed)
    );
}

#[test]
fn every_nan_payload_shares_one_fingerprint() {
    // PostgreSQL semantics: NaN = NaN, and every NaN equals every other NaN
    // whatever its payload or sign bit. A row holding NaN can therefore be a
    // true match for `f = 'NaN'`, so the invariant that keeps the index from
    // pruning it is that all NaN bit patterns hash alike.
    let seed = HashSeed::default();
    let f32s = [f32::NAN, -f32::NAN, f32::from_bits(0x7fc0_0001), f32::from_bits(0xffff_ffff)];
    let f64s = [f64::NAN, -f64::NAN, f64::from_bits(0x7ff8_0000_0000_0001), f64::from_bits(u64::MAX)];
    for (ty, values) in [
        (DbType::F32, f32s.iter().map(|v| v.to_le_bytes().to_vec()).collect::<Vec<_>>()),
        (DbType::F64, f64s.iter().map(|v| v.to_le_bytes().to_vec()).collect::<Vec<_>>()),
    ] {
        for a in &values {
            for b in &values {
                assert!(
                    perform_comparison_operation(a, b, &ty, &ComparerOperation::Equals).unwrap(),
                    "{:?}: NaN must equal NaN, as in PostgreSQL",
                    ty
                );
                assert_eq!(
                    canonical_hash(&ty, a, seed),
                    canonical_hash(&ty, b, seed),
                    "{:?}: equal NaNs must share a fingerprint",
                    ty
                );
            }
        }
    }
}

#[test]
fn string_hash_distinguishes_length() {
    // `simd_equals` checks length before bytes, so "ab" and "ab\0" are distinct
    // values. A hash that ignored length would collide them structurally.
    let seed = HashSeed::default();
    let pairs = [
        ("ab", "ab\0"),
        ("", "\0"),
        ("x", "xx"),
        ("abc", "abc "),
    ];
    for (a, b) in pairs {
        assert!(
            !perform_comparison_operation(
                a.as_bytes(),
                b.as_bytes(),
                &DbType::STRING,
                &ComparerOperation::Equals
            )
            .unwrap(),
            "{:?} vs {:?} should not be equal",
            a,
            b
        );
        assert_ne!(
            canonical_hash(&DbType::STRING, a.as_bytes(), seed),
            canonical_hash(&DbType::STRING, b.as_bytes(), seed),
            "{:?} and {:?} hash alike",
            a,
            b
        );
    }
}

#[test]
fn wrong_width_is_unhashable() {
    // A width mismatch makes the comparison return OperandSizeMismatch, which
    // fails the whole query. The filter must not prune on that basis.
    let seed = HashSeed::default();
    for ty in INDEXABLE {
        if *ty == DbType::STRING {
            continue;
        }
        let w = width(ty);
        assert!(canonical_hash(ty, &vec![0u8; w], seed).is_some(), "{:?}", ty);
        assert!(canonical_hash(ty, &vec![0u8; w + 1], seed).is_none(), "{:?}", ty);
        if w > 1 {
            assert!(canonical_hash(ty, &vec![0u8; w - 1], seed).is_none(), "{:?}", ty);
        }
        assert!(canonical_hash(ty, &[], seed).is_none(), "{:?}", ty);
    }
}

#[test]
fn strings_hash_at_any_length() {
    let seed = HashSeed::default();
    for len in 0..300usize {
        let v: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        assert!(
            canonical_hash(&DbType::STRING, &v, seed).is_some(),
            "length {} should hash",
            len
        );
    }
}

#[test]
fn indexable_set_matches_policy() {
    for ty in INDEXABLE {
        assert!(is_indexable(ty), "{:?} must be indexable", ty);
        assert!(is_fingerprintable(ty), "{:?} must carry fingerprints", ty);
    }
    for ty in NOT_INDEXABLE {
        assert!(!is_indexable(ty), "{:?} must not be indexable", ty);
        assert!(!is_fingerprintable(ty), "{:?} must not be fingerprinted", ty);
    }
    // Indexed for their zone bounds, never hashed.
    for ty in BOUNDS_ONLY {
        assert!(is_indexable(ty), "{:?} must be indexed", ty);
        assert!(!is_fingerprintable(ty), "{:?} must not be fingerprinted", ty);
    }
}

#[test]
fn every_indexable_type_is_comparable_without_panicking() {
    // Indexing a type the evaluator cannot compare would turn a WHERE clause
    // into a panic rather than a query error.
    for ty in INDEXABLE {
        let w = if *ty == DbType::STRING { 4 } else { width(ty) };
        let (a, b) = (vec![0u8; w], vec![0u8; w]);
        let r = std::panic::catch_unwind(|| {
            perform_comparison_operation(&a, &b, ty, &ComparerOperation::Equals)
        });
        assert!(r.is_ok(), "{:?} panics in perform_comparison_operation", ty);
    }
}

#[test]
fn fixed_width_matches_db_type_size() {
    // `hash` keeps its own width table; a new type whose entry disagreed with
    // `get_size()` would make every value unhashable and silently disable
    // filtering for that column with no error anywhere.
    let seed = HashSeed::default();
    for ty in INDEXABLE {
        if *ty == DbType::STRING {
            continue;
        }
        assert!(
            canonical_hash(ty, &vec![0u8; ty.get_size() as usize], seed).is_some(),
            "{:?}: get_size() = {} is not the width canonical_hash accepts",
            ty,
            ty.get_size()
        );
    }
}

#[test]
fn fingerprints_avoid_reserved_codes_and_stay_in_range() {
    for f in 4..=16u8 {
        for _ in 0..5000 {
            let g = fingerprint(fastrand::u64(..), f);
            assert!(g >= CODE_FIRST, "f={} produced reserved code {}", f, g);
            assert!(g < (1u32 << f), "f={} produced out-of-range code {}", f, g);
            assert_ne!(g, CODE_EMPTY);
            assert_ne!(g, CODE_NULL);
        }
    }
}

#[test]
fn fingerprints_are_deterministic() {
    // The seed is fixed precisely so a restart does not invalidate every
    // persisted fingerprint.
    let seed = HashSeed::default();
    for ty in INDEXABLE {
        for v in sample_values(ty, 8) {
            let a = canonical_hash(ty, &v, seed);
            let b = canonical_hash(ty, &v, seed);
            assert_eq!(a, b, "{:?} hashed differently across calls", ty);
            if let Some(h) = a {
                for f in 4..=16u8 {
                    assert_eq!(fingerprint(h, f), fingerprint(h, f));
                }
            }
        }
    }
}

#[test]
fn distinct_seeds_produce_distinct_hashes() {
    let a = HashSeed::default();
    let b = HashSeed { k0: a.k0 ^ 0x5555_5555_5555_5555, k1: a.k1 };
    let v = 1234u64.to_le_bytes();
    assert_ne!(
        canonical_hash(&DbType::U64, &v, a),
        canonical_hash(&DbType::U64, &v, b)
    );
}

#[test]
fn fingerprint_distribution_is_not_degenerate() {
    // A fingerprint that collapsed to a handful of codes would tank the false
    // positive rate without failing any correctness test.
    let seed = HashSeed::default();
    for f in [4u8, 8, 12] {
        let mut seen = std::collections::HashSet::new();
        for i in 0..20_000u64 {
            let h = canonical_hash(&DbType::U64, &i.to_le_bytes(), seed).unwrap();
            seen.insert(fingerprint(h, f));
        }
        let live = (1usize << f) - 2;
        assert!(
            seen.len() >= live * 9 / 10,
            "f={}: only {} of ~{} codes used",
            f,
            seen.len(),
            live
        );
    }
}

#[test]
fn uuid_and_inet_hash_by_exact_bytes() {
    let seed = HashSeed::default();
    for ty in [DbType::UUID, DbType::INET] {
        let base = sample_values(&ty, 0);
        for v in &base {
            for bit in 0..(width(&ty) * 8) {
                let mut other = v.clone();
                other[bit / 8] ^= 1 << (bit % 8);
                let eq =
                    perform_comparison_operation(v, &other, &ty, &ComparerOperation::Equals)
                        .unwrap();
                assert!(!eq, "{:?}: single-bit flip should not be equal", ty);
                assert_ne!(
                    canonical_hash(&ty, v, seed),
                    canonical_hash(&ty, &other, seed),
                    "{:?}: single-bit flip collided",
                    ty
                );
            }
        }
    }
}

#[test]
fn variable_width_types_without_a_comparator_are_excluded() {
    // `BYTEA` and `ARRAY` share `STRING`'s storage mechanism, so it would be
    // easy to index them by accident. The evaluator cannot compare them, so a
    // filter over one would hold values no query can ever match.
    for ty in [DbType::BYTEA, DbType::ARRAY] {
        assert!(!is_indexable(&ty), "{:?} must not be indexable", ty);

        let r = perform_comparison_operation(
            b"a",
            b"a",
            &ty,
            &ComparerOperation::Equals,
        );
        assert!(
            r.is_err(),
            "{:?} gained a comparator — reconsider whether it should be indexed, \
             and update NOT_INDEXABLE if so",
            ty
        );
    }
}
