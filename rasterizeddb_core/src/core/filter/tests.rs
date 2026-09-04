use smallvec::SmallVec;

use super::column::{ColumnFilter, Hll, bitmap_words, choose_f};
use super::hash::*;
use super::kernels::*;
use super::sarg;
use super::zonemap::{SargOp, ZoneMap};
use super::*;
use crate::core::db_type::DbType;
use crate::core::processor::transformer::ComparerOperation;
use crate::core::row::logical::perform_comparison_operation;
use crate::core::row::schema::SchemaField;

const INDEXABLE: &[DbType] = &[
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
    DbType::F32,
    DbType::F64,
    DbType::STRING,
    DbType::DATETIME,
    DbType::CHAR,
    DbType::DECIMAL,
    DbType::UUID,
    DbType::INET,
];

/// Serialized width, taken from `DbType` itself so a new type cannot be added
/// to `INDEXABLE` with a stale width here.
fn width(t: &DbType) -> usize {
    t.get_size() as usize
}

// ---------------------------------------------------------------------------
// The SIMD constant, re-derived rather than trusted.
// ---------------------------------------------------------------------------

#[test]
fn ternlog_imm8_is_correct() {
    // vpternlogq imm8: the bit at index (4A + 2B + C) is the result for those
    // inputs. We need A & (B XNOR C).
    let mut imm = 0u32;
    for a in 0..2u32 {
        for b in 0..2u32 {
            for c in 0..2u32 {
                let r = a & if b == c { 1 } else { 0 };
                if r == 1 {
                    imm |= 1 << (4 * a + 2 * b + c);
                }
            }
        }
    }
    assert_eq!(imm, 0x90, "kernels.rs hardcodes 0x90 for A & (B XNOR C)");
}

// ---------------------------------------------------------------------------
// The safety-critical invariant: equality implies hash equality.
// ---------------------------------------------------------------------------

/// If `perform_comparison_operation(a, b, ty, Equals)` is true then the two
/// values MUST hash alike. The converse is explicitly NOT required — a hash
/// collision is a false positive, which the evaluator filters out.
#[test]
fn hash_agrees_with_comparison() {
    let seed = HashSeed::default();

    for ty in INDEXABLE {
        let mut samples: Vec<Vec<u8>> = Vec::new();

        if *ty == DbType::STRING {
            for s in [
                "",
                "a",
                "ab",
                "ab\0",
                "hello world",
                "hello worle",
                &"x".repeat(200),
            ] {
                samples.push(s.as_bytes().to_vec());
            }
        } else if *ty == DbType::INET {
            // Layout: [family(1)][address(16, big-endian)][prefix_bits(1)].
            // Cover both families, and two values differing ONLY in the prefix
            // byte — if the hash ignored any part of the value, those two would
            // collide while comparing unequal.
            let mut v4 = vec![0u8; 18];
            v4[0] = 4;
            v4[1..5].copy_from_slice(&[192, 168, 0, 1]);
            v4[17] = 24;
            let mut v4_other_prefix = v4.clone();
            v4_other_prefix[17] = 32;
            let mut v4_other_addr = v4.clone();
            v4_other_addr[4] = 2;

            let mut v6 = vec![0u8; 18];
            v6[0] = 6;
            v6[1] = 0x20;
            v6[2] = 0x01;
            v6[16] = 1;
            v6[17] = 64;
            let mut v6_other_prefix = v6.clone();
            v6_other_prefix[17] = 128;

            samples.extend([v4, v4_other_prefix, v4_other_addr, v6, v6_other_prefix]);
            for _ in 0..32 {
                let mut b: Vec<u8> = (0..18).map(|_| fastrand::u8(..)).collect();
                b[0] = if fastrand::bool() { 4 } else { 6 };
                samples.push(b);
            }
        } else {
            let w = width(ty);
            // Structured edge cases plus random fill.
            samples.push(vec![0u8; w]);
            samples.push(vec![0xFFu8; w]);
            let mut hi = vec![0u8; w];
            hi[w - 1] = 0x80; // sign bit / -0.0
            samples.push(hi);
            for _ in 0..64 {
                samples.push((0..w).map(|_| fastrand::u8(..)).collect());
            }
        }

        for a in &samples {
            for b in &samples {
                let eq = perform_comparison_operation(a, b, ty, &ComparerOperation::Equals)
                    .expect("indexable types must be comparable");
                if eq {
                    let ha = canonical_hash(ty, a, seed);
                    let hb = canonical_hash(ty, b, seed);
                    assert_eq!(
                        ha, hb,
                        "{:?}: values compare Equal but hash differently: {:?} vs {:?}",
                        ty, a, b
                    );
                    assert!(ha.is_some(), "{:?}: comparable value must be hashable", ty);
                }
            }
        }
    }
}

#[test]
fn float_zero_signs_hash_alike() {
    let seed = HashSeed::default();
    // The one case where distinct bit patterns compare equal.
    let pos = 0.0f32.to_le_bytes();
    let neg = (-0.0f32).to_le_bytes();
    assert_ne!(pos, neg, "bit patterns must actually differ");
    assert!(
        perform_comparison_operation(&pos, &neg, &DbType::F32, &ComparerOperation::Equals).unwrap()
    );
    assert_eq!(
        canonical_hash(&DbType::F32, &pos, seed),
        canonical_hash(&DbType::F32, &neg, seed)
    );

    let pos = 0.0f64.to_le_bytes();
    let neg = (-0.0f64).to_le_bytes();
    assert_eq!(
        canonical_hash(&DbType::F64, &pos, seed),
        canonical_hash(&DbType::F64, &neg, seed)
    );
}

#[test]
fn indexable_set_matches_policy() {
    // Sentinels are not column types.
    for t in [DbType::NULL, DbType::UNKNOWN, DbType::START, DbType::END] {
        assert!(
            !is_indexable(&t),
            "{:?} is a sentinel, not a column type",
            t
        );
    }

    // BOOL is indexable but never fingerprinted. Two distinct values can never
    // clear the selectivity gate, so `f` bits per row would buy a mask handing
    // back half the table — but two *bytes per zone* is a fiftieth of that and
    // prunes a sparse flag almost perfectly, which is what `__null_x = 1` is.
    assert!(is_indexable(&DbType::BOOL));
    assert!(!is_fingerprintable(&DbType::BOOL));
    assert_eq!(choose_f(2, 1_000_000, &FilterConfig::default()), 0);

    for t in INDEXABLE {
        assert!(is_indexable(t), "{:?} must be indexable", t);
        assert!(is_fingerprintable(t), "{:?} must carry fingerprints", t);
    }

    // Bounds-only is the only split: nothing is fingerprinted without being
    // indexed, so the two sets differ by exactly BOOL.
    for t in [DbType::NULL, DbType::UNKNOWN, DbType::START, DbType::END] {
        assert!(!is_fingerprintable(&t));
    }
}

#[test]
fn wrong_width_is_unhashable() {
    let seed = HashSeed::default();
    // A width mismatch makes the comparison return OperandSizeMismatch, which
    // fails the query. The filter must not prune on that basis.
    assert!(canonical_hash(&DbType::I32, &[1, 2, 3], seed).is_none());
    assert!(canonical_hash(&DbType::I32, &[1, 2, 3, 4, 5], seed).is_none());
    assert!(canonical_hash(&DbType::I32, &[1, 2, 3, 4], seed).is_some());
}

#[test]
fn fingerprint_never_uses_reserved_codes() {
    for f in MIN_F..=MAX_F {
        for _ in 0..2000 {
            let g = fingerprint(fastrand::u64(..), f);
            assert!(g >= CODE_FIRST, "f={} produced reserved code {}", f, g);
            assert!(g < (1u32 << f), "f={} produced out-of-range code {}", f, g);
        }
    }
}

// ---------------------------------------------------------------------------
// Kernels
// ---------------------------------------------------------------------------

#[test]
fn simd_kernels_match_scalar() {
    for f in MIN_F..=MAX_F {
        for _ in 0..200 {
            let planes: Vec<u64> = (0..f as usize * ZONE_WORDS)
                .map(|_| fastrand::u64(..))
                .collect();
            let g = fingerprint(fastrand::u64(..), f);

            let mut want = [0u64; ZONE_WORDS];
            probe_zone_scalar(&planes, f, g, &mut want);

            let mut got = [0u64; ZONE_WORDS];
            probe_fn()(&planes, f, g, &mut got);
            assert_eq!(
                got,
                want,
                "active kernel {} disagrees at f={}",
                active_kernel(),
                f
            );
        }
    }
}

#[test]
fn slot_roundtrip() {
    for f in MIN_F..=MAX_F {
        let mut planes = vec![0u64; f as usize * ZONE_WORDS];
        let mut expect = vec![0u32; ZONE_ROWS];
        for local in 0..ZONE_ROWS {
            let g = fingerprint(fastrand::u64(..), f);
            write_slot(&mut planes, f, local, g);
            expect[local] = g;
        }
        for local in 0..ZONE_ROWS {
            assert_eq!(
                read_slot(&planes, f, local),
                expect[local],
                "f={} slot={}",
                f,
                local
            );
        }
    }
}

#[test]
fn probe_finds_every_written_slot() {
    // No false negatives at the kernel level.
    let f = 8u8;
    let mut planes = vec![0u64; f as usize * ZONE_WORDS];
    let codes: Vec<u32> = (0..ZONE_ROWS)
        .map(|_| fingerprint(fastrand::u64(..), f))
        .collect();
    for (i, g) in codes.iter().enumerate() {
        write_slot(&mut planes, f, i, *g);
    }
    let mut out = [0u64; ZONE_WORDS];
    for (i, g) in codes.iter().enumerate() {
        probe_fn()(&planes, f, *g, &mut out);
        assert!(
            (out[i / 64] >> (i % 64)) & 1 == 1,
            "slot {} lost its own code",
            i
        );
    }
}

// ---------------------------------------------------------------------------
// ColumnFilter
// ---------------------------------------------------------------------------

fn u64_col() -> ColumnFilter {
    ColumnFilter::new(0, DbType::U64, HashSeed::default(), FilterConfig::default())
}

#[test]
fn column_has_no_false_negatives() {
    let mut col = u64_col();
    let n = 5000u64;
    let vals: Vec<u64> = (0..n).map(|_| fastrand::u64(..)).collect();
    for (slot, v) in vals.iter().enumerate() {
        col.set(slot as u64, Some(&v.to_le_bytes()));
    }

    let mut bm = vec![0u64; bitmap_words(n)];
    for (slot, v) in vals.iter().enumerate() {
        col.probe_into(&v.to_le_bytes(), n, &mut bm);
        let s = slot as u64;
        assert!(
            (bm[(s / 64) as usize] >> (s % 64)) & 1 == 1,
            "slot {} pruned despite holding the probed value",
            slot
        );
    }
}

#[test]
fn column_false_positive_rate_is_near_theory() {
    let mut cfg = FilterConfig::default();
    cfg.min_f = 8;
    cfg.max_f = 8;
    cfg.default_f = 8;
    let mut col = ColumnFilter::new(0, DbType::U64, HashSeed::default(), cfg);

    let n = 20_000u64;
    for slot in 0..n {
        col.set(slot, Some(&fastrand::u64(..).to_le_bytes()));
    }

    let mut bm = vec![0u64; bitmap_words(n)];
    let trials = 200u64;
    let mut hits = 0u64;
    for _ in 0..trials {
        // Values essentially certain not to be present.
        col.probe_hash_into(fastrand::u64(..), n, &mut bm);
        hits += bm.iter().map(|w| w.count_ones() as u64).sum::<u64>();
    }
    let observed = hits as f64 / (trials * n) as f64;
    let theory = 1.0 / 254.0;
    assert!(
        observed < theory * 2.0,
        "FPP {:.5} far above theory {:.5} for f=8",
        observed,
        theory
    );
}

#[test]
fn delete_clears_the_slot_exactly() {
    let mut col = u64_col();
    let v = 12345u64.to_le_bytes();
    for slot in 0..1000u64 {
        col.set(slot, Some(&fastrand::u64(..).to_le_bytes()));
    }
    col.set(500, Some(&v));

    let mut bm = vec![0u64; bitmap_words(1000)];
    col.probe_into(&v, 1000, &mut bm);
    assert!((bm[500 / 64] >> (500 % 64)) & 1 == 1);

    // Unlike a Bloom filter, removal is exact.
    col.set(500, None);
    col.probe_into(&v, 1000, &mut bm);
    assert!(
        (bm[500 / 64] >> (500 % 64)) & 1 == 0,
        "cleared slot still matches"
    );
}

#[test]
fn slots_above_frontier_stay_candidates() {
    let mut col = u64_col();
    let v = 999u64.to_le_bytes();
    for slot in 0..600u64 {
        col.set(slot, Some(&v));
    }

    // Claim more slots exist than were ever indexed: an insert may be in flight.
    let n = 2000u64;
    let mut bm = vec![0u64; bitmap_words(n)];
    col.probe_into(&7777u64.to_le_bytes(), n, &mut bm);

    for slot in 600..n {
        assert!(
            (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1,
            "slot {} above the frontier must stay a candidate",
            slot
        );
    }
}

#[test]
fn unindexed_prefix_stays_candidate() {
    let mut col = u64_col();
    col.set_unindexed_prefix(1000);
    assert!(!col.is_complete());
    for slot in 1000..2000u64 {
        col.set(slot, Some(&1u64.to_le_bytes()));
    }

    let n = 2000u64;
    let mut bm = vec![0u64; bitmap_words(n)];
    col.probe_into(&42u64.to_le_bytes(), n, &mut bm);
    for slot in 0..1000u64 {
        assert!(
            (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1,
            "un-backfilled slot {} must stay a candidate",
            slot
        );
    }
}

#[test]
fn out_of_order_inserts_never_lose_rows() {
    // Concurrent writers complete out of order; a gap may cost selectivity but
    // must never prune a row that was actually indexed.
    let mut col = u64_col();
    let n = 3000u64;
    let vals: Vec<u64> = (0..n).map(|_| fastrand::u64(..)).collect();
    let mut order: Vec<u64> = (0..n).collect();
    fastrand::shuffle(&mut order);
    for &slot in &order {
        col.set(slot, Some(&vals[slot as usize].to_le_bytes()));
    }

    let mut bm = vec![0u64; bitmap_words(n)];
    for slot in 0..n {
        col.probe_into(&vals[slot as usize].to_le_bytes(), n, &mut bm);
        assert!(
            (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1,
            "slot {} lost",
            slot
        );
    }
}

#[test]
fn generations_keep_old_zones_readable() {
    // Force f to change mid-stream and confirm both generations still answer.
    let mut cfg = FilterConfig::default();
    cfg.min_f = 4;
    cfg.max_f = 16;
    cfg.default_f = 4;
    let mut col = ColumnFilter::new(0, DbType::U64, HashSeed::default(), cfg);

    let n = 20_000u64;
    let vals: Vec<u64> = (0..n).map(|_| fastrand::u64(..)).collect();
    for slot in 0..n {
        col.set(slot, Some(&vals[slot as usize].to_le_bytes()));
    }
    // Cardinality is ~n, so the adaptive rule should have widened f.
    assert!(
        col.current_f() > 4,
        "f should have grown, got {}",
        col.current_f()
    );

    let mut bm = vec![0u64; bitmap_words(n)];
    for slot in 0..n {
        col.probe_into(&vals[slot as usize].to_le_bytes(), n, &mut bm);
        assert!(
            (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1,
            "slot {} lost across a generation boundary",
            slot
        );
    }
}

#[test]
fn choose_f_gates_low_cardinality() {
    let cfg = FilterConfig::default();
    // 5 distinct values: a predicate matches ~20% of rows however big the table.
    assert_eq!(choose_f(5, 1_000_000, &cfg), 0);
    assert_eq!(choose_f(5, 1_000, &cfg), 0);
    // 1024 distinct values is 0.1% selectivity — worth filtering at any size.
    assert!(choose_f(1024, 1_000_000, &cfg) >= cfg.min_f);
    assert!(choose_f(1024, 10_000, &cfg) >= cfg.min_f);
    // Unique column: clamped to max_f.
    assert_eq!(choose_f(1_000_000, 1_000_000, &cfg), cfg.max_f);
    // Middling cardinality lands between the bounds.
    let f = choose_f(1024, 1_000_000, &cfg);
    assert!(f >= cfg.min_f && f <= cfg.max_f);
}

#[test]
fn hll_is_roughly_right() {
    for &truth in &[100u64, 5_000, 100_000] {
        let mut h = Hll::default();
        for i in 0..truth {
            h.add(canonical_hash(&DbType::U64, &i.to_le_bytes(), HashSeed::default()).unwrap());
        }
        let est = h.estimate() as f64;
        let err = (est - truth as f64).abs() / truth as f64;
        assert!(
            err < 0.25,
            "HLL estimate {} for {} rows (err {:.2})",
            est,
            truth,
            err
        );
    }
}

// ---------------------------------------------------------------------------
// Sargable extraction
// ---------------------------------------------------------------------------

fn schema() -> SmallVec<[SchemaField; 20]> {
    smallvec::smallvec![
        SchemaField::new("id".into(), DbType::U64, 8, 0, 0, false),
        SchemaField::new("name".into(), DbType::STRING, 12, 0, 1, false),
        SchemaField::new("age".into(), DbType::I32, 4, 0, 2, false),
    ]
}

fn filters() -> TableFilters {
    TableFilters::new(&schema(), FilterConfig::default())
}

fn toks(q: &str) -> SmallVec<[crate::core::tokenizer::query_tokenizer::Token; 36]> {
    crate::core::tokenizer::query_tokenizer::tokenize(q, &schema())
}

#[test]
fn sarg_extracts_plain_conjunction() {
    let f = filters();
    let p = sarg::extract(&toks("id = 42 AND age = 7"), &f).expect("should extract");
    assert_eq!(p.all_preds().len(), 2);
    assert_eq!(p.all_preds()[0].value, 42u64.to_le_bytes().to_vec());
    assert_eq!(p.all_preds()[1].value, 7i32.to_le_bytes().to_vec());
}

#[test]
fn sarg_extracts_string_equality() {
    let f = filters();
    let p = sarg::extract(&toks("name = 'bob'"), &f).expect("should extract");
    assert_eq!(p.all_preds().len(), 1);
    assert_eq!(p.all_preds()[0].value, b"bob".to_vec());
}

#[test]
fn sarg_ignores_unfilterable_conjuncts_but_keeps_the_rest() {
    let f = filters();
    // `id != 2` is not sargable; it contributes nothing, `id = 1` still applies.
    let p = sarg::extract(&toks("id = 1 AND id != 2"), &f).expect("should extract");
    assert_eq!(p.all_preds().len(), 1);
    assert_eq!(p.all_preds()[0].op, SargOp::Eq);
}

#[test]
fn sarg_extracts_range_predicates() {
    let f = filters();
    for (q, want) in [
        ("age > 3", SargOp::Gt),
        ("age >= 3", SargOp::Ge),
        ("age < 3", SargOp::Lt),
        ("age <= 3", SargOp::Le),
    ] {
        let p = sarg::extract(&toks(q), &f).unwrap_or_else(|| panic!("{} not extracted", q));
        assert_eq!(p.all_preds().len(), 1, "{}", q);
        assert_eq!(p.all_preds()[0].op, want, "{}", q);
    }
}

#[test]
fn sarg_mirrors_reversed_operands() {
    let f = filters();
    // `3 < age` is `age > 3`, not `age < 3`. Getting this backwards would prune
    // exactly the rows that match.
    let p = sarg::extract(&toks("3 < age"), &f).unwrap();
    assert_eq!(p.all_preds()[0].op, SargOp::Gt);
    let p = sarg::extract(&toks("3 >= age"), &f).unwrap();
    assert_eq!(p.all_preds()[0].op, SargOp::Le);
}

#[test]
fn sarg_handles_parens() {
    let f = filters();
    // `predicate_rewrite` desugars IN to `(a = 1 OR a = 2 ...)` and BETWEEN to
    // `(a >= lo AND a <= hi)`, so paren support is what makes every IN and
    // BETWEEN an ORM emits filterable.
    assert!(sarg::extract(&toks("(id = 1 OR id = 2)"), &f).is_some());
    assert!(sarg::extract(&toks("(id >= 1 AND id <= 9)"), &f).is_some());
    assert!(sarg::extract(&toks("age = 3 AND (id >= 1 AND id <= 9)"), &f).is_some());

    // An OR group beside another term is filterable again. It was not while the
    // evaluator flattened boolean structure — acting on the parens then made
    // the mask narrower than the evaluator's own output, so filtering changed
    // results rather than only their cost.
    assert!(sarg::extract(&toks("age = 3 AND (id = 1 OR id = 2)"), &f).is_some());
    assert!(sarg::extract(&toks("(id = 1 OR id = 2) AND age = 3"), &f).is_some());
    assert!(sarg::extract(&toks("age = 3 AND (id = 1 OR id = 2 OR id = 3)"), &f).is_some());

    // Unbalanced parens mean we misread the structure: decline.
    assert!(sarg::extract(&toks("(id = 1 OR id = 2"), &f).is_none());
}

#[test]
fn sarg_refuses_inequality() {
    let f = filters();
    // probe(v) over-approximates `col = v`, so its complement UNDER-approximates
    // `col != v` and would drop real matches. No structure here can answer it.
    assert!(sarg::extract(&toks("id != 5"), &f).is_none());
    assert!(sarg::extract(&toks("name != 'x'"), &f).is_none());
}

// ---------------------------------------------------------------------------
// TableFilters end-to-end
// ---------------------------------------------------------------------------

#[test]
fn table_filters_conjunction_compounds() {
    let f = filters();
    let n = 4000u64;

    // Row i: id = i, age = i % 10.
    for slot in 0..n {
        let cols: SmallVec<[(u32, Vec<u8>); 32]> = smallvec::smallvec![
            (0u32, slot.to_le_bytes().to_vec()),
            (2u32, ((slot % 10) as i32).to_le_bytes().to_vec()),
        ];
        f.backfill_row(slot, &cols);
    }

    let p = sarg::extract(&toks("id = 1234 AND age = 4"), &f).unwrap();
    let bm = f.evaluate(&p, n).expect("should produce a bitmap");

    // The true match must survive.
    assert!(bm.get(1234), "true match was pruned");
    // And the conjunction should be far more selective than either half.
    assert!(
        bm.count() < 20,
        "expected tight mask, got {} candidates",
        bm.count()
    );
}

#[test]
fn table_filters_never_prune_a_true_match() {
    let f = filters();
    let n = 3000u64;
    let names: Vec<String> = (0..n).map(|i| format!("user{}", i)).collect();

    for slot in 0..n {
        let cols: SmallVec<[(u32, Vec<u8>); 32]> = smallvec::smallvec![
            (0u32, slot.to_le_bytes().to_vec()),
            (1u32, names[slot as usize].as_bytes().to_vec()),
        ];
        f.backfill_row(slot, &cols);
    }

    for slot in (0..n).step_by(7) {
        let q = format!("name = '{}'", names[slot as usize]);
        let p = sarg::extract(&toks(&q), &f).unwrap();
        let bm = f.evaluate(&p, n).unwrap();
        assert!(bm.get(slot), "slot {} pruned despite matching", slot);
    }
}

#[test]
fn all_candidates_bitmap_is_saturated() {
    let bm = CandidateBitmap::all_candidates(100);
    assert_eq!(bm.count(), 100, "tail bits past n_slots must be cleared");
    for s in 0..100 {
        assert!(bm.get(s));
    }
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

use super::persist::{self, LoadReject};

fn persist_schema() -> SmallVec<[SchemaField; 20]> {
    schema()
}

fn filled_filters(n: u64) -> (TableFilters, Vec<u64>) {
    let f = TableFilters::new(&persist_schema(), FilterConfig::default());
    let vals: Vec<u64> = (0..n).map(|_| fastrand::u64(..)).collect();
    for slot in 0..n {
        let cols: SmallVec<[(u32, Vec<u8>); 32]> = smallvec::smallvec![
            (0u32, vals[slot as usize].to_le_bytes().to_vec()),
            (2u32, ((slot % 97) as i32).to_le_bytes().to_vec()),
        ];
        f.backfill_row(slot, &cols);
    }
    (f, vals)
}

#[test]
fn checkpoint_round_trips_without_losing_rows() {
    let n = 4000u64;
    let (f, vals) = filled_filters(n);
    let bytes = f.checkpoint_bytes(n);

    let restored =
        TableFilters::from_checkpoint(&bytes, &persist_schema(), FilterConfig::default(), n)
            .expect("round trip should load");

    // Every value indexed before the checkpoint must still be findable after.
    for slot in 0..n {
        let program =
            sarg::extract(&toks(&format!("id = {}", vals[slot as usize])), &restored).unwrap();
        let bm = restored.evaluate(&program, n).unwrap();
        assert!(bm.get(slot), "slot {} lost across a checkpoint", slot);
    }
}

#[test]
fn checkpoint_preserves_pruning_power() {
    let n = 4000u64;
    let (f, vals) = filled_filters(n);
    let bytes = f.checkpoint_bytes(n);
    let restored =
        TableFilters::from_checkpoint(&bytes, &persist_schema(), FilterConfig::default(), n)
            .unwrap();

    let program = sarg::extract(&toks(&format!("id = {}", vals[1234])), &restored).unwrap();
    let bm = restored.evaluate(&program, n).unwrap();
    assert!(bm.get(1234));
    assert!(
        bm.count() < n / 10,
        "restored filter barely prunes: {}",
        bm.count()
    );
    assert_eq!(
        restored.resume_slot(),
        n,
        "restored frontier should cover all rows"
    );
}

#[test]
fn modification_after_checkpoint_pulls_the_frontier_back() {
    let n = 4000u64;
    let (f, _) = filled_filters(n);
    let _ = f.checkpoint_bytes(n);

    // Simulate a delete of an already-indexed row *after* the checkpoint.
    f.on_delete(1500);

    let bytes = f.checkpoint_bytes(n);
    let restored =
        TableFilters::from_checkpoint(&bytes, &persist_schema(), FilterConfig::default(), n)
            .unwrap();

    // The checkpoint must not claim to cover anything at or above the modified
    // slot: its bytes for that region could be stale.
    assert!(
        restored.resume_slot() <= 1500,
        "frontier {} still covers a slot modified after the checkpoint",
        restored.resume_slot()
    );
}

#[test]
fn dirty_flag_rejects_the_whole_checkpoint() {
    let n = 1000u64;
    let (f, _) = filled_filters(n);
    let mut bytes = f.checkpoint_bytes(n);

    bytes[persist::DIRTY_OFFSET as usize] = 1;
    assert_eq!(
        TableFilters::from_checkpoint(&bytes, &persist_schema(), FilterConfig::default(), n)
            .err()
            .unwrap(),
        LoadReject::Dirty
    );
}

#[test]
fn corrupt_or_incompatible_checkpoints_are_rejected() {
    let n = 1000u64;
    let (f, _) = filled_filters(n);
    let good = f.checkpoint_bytes(n);
    let cfg = FilterConfig::default();

    let load = |b: &[u8]| {
        TableFilters::from_checkpoint(b, &persist_schema(), cfg, n)
            .err()
            .unwrap()
    };

    assert_eq!(load(&[]), LoadReject::Empty);

    let mut bad = good.clone();
    bad[0] = b'X';
    assert_eq!(load(&bad), LoadReject::BadMagic);

    let mut bad = good.clone();
    bad[8..10].copy_from_slice(&99u16.to_le_bytes());
    assert_eq!(load(&bad), LoadReject::Version(99));

    // Zone geometry change: slot-to-zone arithmetic would differ.
    let mut bad = good.clone();
    bad[10..12].copy_from_slice(&256u16.to_le_bytes());
    assert_eq!(load(&bad), LoadReject::Geometry);

    // `enable_long_row` toggled: every fingerprint would describe another row,
    // since `slot = offset / ROW_POINTER_RECORD_LEN`.
    //
    // Perturb the stored length rather than hardcoding "the other" value: 21 and
    // 92 swap roles with the feature flag, so a literal here passes under one
    // build and silently writes the *correct* length under the other, making the
    // checkpoint load cleanly and this assertion panic on an `Ok`.
    let mut bad = good.clone();
    bad[12] = bad[12].wrapping_add(1);
    assert_eq!(load(&bad), LoadReject::RecordLen);

    // Different hash seed: fingerprints are meaningless.
    let mut bad = good.clone();
    bad[16..24].copy_from_slice(&0xDEADBEEFu64.to_le_bytes());
    assert_eq!(load(&bad), LoadReject::Seed);

    // Truncation at every length, including mid-plane.
    for cut in [1usize, 8, 40, 63, 64, 100, good.len() / 2, good.len() - 1] {
        let err = load(&good[..cut]);
        assert!(
            matches!(
                err,
                LoadReject::Truncated | LoadReject::SchemaMismatch | LoadReject::BadMagic
            ),
            "cut at {} gave {:?}",
            cut,
            err
        );
    }
}

#[test]
fn schema_change_rejects_the_checkpoint() {
    let n = 500u64;
    let (f, _) = filled_filters(n);
    let bytes = f.checkpoint_bytes(n);

    // A column removed since the checkpoint was written.
    let narrower: SmallVec<[SchemaField; 20]> = smallvec::smallvec![
        SchemaField::new("id".into(), DbType::U64, 8, 0, 0, false),
        SchemaField::new("name".into(), DbType::STRING, 12, 0, 1, false),
    ];
    assert_eq!(
        TableFilters::from_checkpoint(&bytes, &narrower, FilterConfig::default(), n)
            .err()
            .unwrap(),
        LoadReject::SchemaMismatch
    );

    // A column whose type changed: canonical_hash is type-dispatched, so every
    // stored fingerprint would be for a different hash function.
    let retyped: SmallVec<[SchemaField; 20]> = smallvec::smallvec![
        SchemaField::new("id".into(), DbType::U64, 8, 0, 0, false),
        SchemaField::new("name".into(), DbType::STRING, 12, 0, 1, false),
        SchemaField::new("age".into(), DbType::I64, 8, 0, 2, false),
    ];
    assert_eq!(
        TableFilters::from_checkpoint(&bytes, &retyped, FilterConfig::default(), n)
            .err()
            .unwrap(),
        LoadReject::SchemaMismatch
    );
}

#[test]
fn checkpoint_survives_generation_boundaries() {
    // Force several generations, then confirm all of them serialize and reload.
    let mut cfg = FilterConfig::default();
    cfg.min_f = 4;
    cfg.default_f = 4;
    cfg.max_f = 16;

    let f = TableFilters::new(&persist_schema(), cfg);
    let n = 20_000u64;
    let vals: Vec<u64> = (0..n).map(|_| fastrand::u64(..)).collect();
    for slot in 0..n {
        let cols: SmallVec<[(u32, Vec<u8>); 32]> =
            smallvec::smallvec![(0u32, vals[slot as usize].to_le_bytes().to_vec())];
        f.backfill_row(slot, &cols);
    }
    assert!(f.stats()[0].1 > 4, "expected f to have grown");

    let bytes = f.checkpoint_bytes(n);
    let restored = TableFilters::from_checkpoint(&bytes, &persist_schema(), cfg, n).unwrap();

    for slot in (0..n).step_by(13) {
        let program =
            sarg::extract(&toks(&format!("id = {}", vals[slot as usize])), &restored).unwrap();
        let bm = restored.evaluate(&program, n).unwrap();
        assert!(
            bm.get(slot),
            "slot {} lost across generations + checkpoint",
            slot
        );
    }
}

#[test]
fn checkpoint_size_tracks_bits_per_row() {
    let n = 8192u64;
    let (f, _) = filled_filters(n);
    let bytes = f.checkpoint_bytes(n);
    // Two indexed columns carrying data plus one empty (name), headers and HLLs.
    // The bound catches format bloat regressions.
    let ceiling = (n as usize * 2 * 2) + 4096;
    assert!(
        bytes.len() < ceiling,
        "checkpoint is {} bytes for {} rows, expected under {}",
        bytes.len(),
        n,
        ceiling
    );
}

// ---------------------------------------------------------------------------
// Zone min/max
// ---------------------------------------------------------------------------

fn i64_col_with_zonemap() -> ColumnFilter {
    let c = ColumnFilter::new(0, DbType::I64, HashSeed::default(), FilterConfig::default());
    assert!(c.has_zonemap());
    c
}

#[test]
fn string_columns_carry_truncated_bounds() {
    // Not exact ones: `ZoneMap` is a strided array, so a string keeps only its
    // first `STRING_BOUND_PREFIX` bytes. What matters is the direction each
    // bound is allowed to move under truncation.
    let c = ColumnFilter::new(
        1,
        DbType::STRING,
        HashSeed::default(),
        FilterConfig::default(),
    );
    assert!(c.has_zonemap());
}

#[test]
fn a_truncated_max_bound_rounds_up_not_down() {
    // The one bug in this module that returns wrong answers rather than merely
    // pruning badly. Truncating makes a string *smaller*: fine for `min`, which
    // widens downward, and wrong for `max`, which would then sit below data it
    // is meant to bracket.
    let k = super::zonemap::STRING_BOUND_PREFIX;
    let long: Vec<u8> = b"prefix0123456789TAIL".to_vec();
    assert!(long.len() > k, "the fixture must actually be truncated");

    let mut zm = ZoneMap::new(DbType::STRING).expect("STRING carries bounds");
    zm.observe(0, &long);

    // The stored max must be at or above the value it came from, never below.
    assert!(
        zm.zone_may_match(0, SargOp::Eq, &long),
        "the zone must still admit the value it was built from"
    );
    // And above every extension of that prefix, which is what a truncated bound
    // has to promise.
    let mut longer = long.clone();
    longer.extend_from_slice(b"MORE");
    let mut zm2 = ZoneMap::new(DbType::STRING).unwrap();
    zm2.observe(0, &longer);
    assert!(zm2.zone_may_match(0, SargOp::Eq, &longer));
}

#[test]
fn an_all_ff_prefix_leaves_the_zone_unbounded_above() {
    // `successor` has nothing to return, so the zone must claim no upper bound
    // rather than wrap around to something below the data.
    let k = super::zonemap::STRING_BOUND_PREFIX;
    assert!(super::zonemap::successor(&vec![0xFFu8; k]).is_none());

    let mut v = vec![0xFFu8; k];
    v.extend_from_slice(b"tail");
    let mut zm = ZoneMap::new(DbType::STRING).unwrap();
    zm.observe(0, &v);

    assert!(zm.zone_may_match(0, SargOp::Eq, &v));
    // Unbounded above means every `>` probe has to answer "possible".
    assert!(zm.zone_may_match(0, SargOp::Gt, b"zzzz"));
    assert!(zm.zone_may_match(0, SargOp::Ge, b"zzzz"));
    // The lower bound still works: nothing here is below `0xFF...`.
    assert!(!zm.zone_may_match(0, SargOp::Lt, b"aaa"));
}

#[test]
fn string_bounds_never_lose_a_row() {
    // The property the whole structure exists to hold, over strings that
    // straddle the truncation length in both directions.
    let mut zm = ZoneMap::new(DbType::STRING).unwrap();
    let mut vals: Vec<Vec<u8>> = Vec::new();
    for _ in 0..400 {
        let len = fastrand::usize(0..40);
        vals.push((0..len).map(|_| fastrand::u8(b'a'..=b'e')).collect());
    }
    for v in &vals {
        zm.observe(0, v);
    }
    for v in &vals {
        assert!(zm.zone_may_match(0, SargOp::Eq, v), "lost {:?}", v);
        assert!(zm.zone_may_match(0, SargOp::Ge, v), "lost {:?} on >=", v);
        assert!(zm.zone_may_match(0, SargOp::Le, v), "lost {:?} on <=", v);
    }
}

#[test]
fn range_probe_has_no_false_negatives() {
    let mut col = i64_col_with_zonemap();
    let n = 6000u64;
    let vals: Vec<i64> = (0..n).map(|_| fastrand::i64(-100_000..100_000)).collect();
    for slot in 0..n {
        col.set(slot, Some(&vals[slot as usize].to_le_bytes()));
    }

    let mut bm = vec![0u64; bitmap_words(n)];
    for _ in 0..200 {
        let pivot = fastrand::i64(-100_000..100_000);
        for op in [SargOp::Gt, SargOp::Ge, SargOp::Lt, SargOp::Le] {
            col.probe_pred_into(op, &pivot.to_le_bytes(), n, &mut bm);
            for slot in 0..n {
                let v = vals[slot as usize];
                let truth = match op {
                    SargOp::Gt => v > pivot,
                    SargOp::Ge => v >= pivot,
                    SargOp::Lt => v < pivot,
                    SargOp::Le => v <= pivot,
                    SargOp::Eq => v == pivot,
                };
                if truth {
                    assert!(
                        (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1,
                        "slot {} (v={}) pruned for {:?} {}",
                        slot,
                        v,
                        op,
                        pivot
                    );
                }
            }
        }
    }
}

#[test]
fn ordered_data_prunes_hard() {
    // The case zone maps exist for: an append-ordered column (ids, timestamps).
    let mut col = i64_col_with_zonemap();
    let n = 10_000u64;
    for slot in 0..n {
        col.set(slot, Some(&(slot as i64).to_le_bytes()));
    }

    let mut bm = vec![0u64; bitmap_words(n)];
    col.probe_pred_into(SargOp::Gt, &9500i64.to_le_bytes(), n, &mut bm);
    let survivors: u64 = bm.iter().map(|w| w.count_ones() as u64).sum();
    // Only the zones covering slots 9500.. should survive; zone granularity is
    // 512, so allow a zone's worth of slack.
    assert!(
        survivors < 1200,
        "expected tight pruning on ordered data, {} of {} survived",
        survivors,
        n
    );
    for slot in 9501..n {
        assert!(
            (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1,
            "slot {} lost",
            slot
        );
    }
}

#[test]
fn equality_combines_fingerprint_and_bounds() {
    let mut col = i64_col_with_zonemap();
    let n = 10_000u64;
    for slot in 0..n {
        col.set(slot, Some(&(slot as i64).to_le_bytes()));
    }
    let mut bm = vec![0u64; bitmap_words(n)];
    col.probe_pred_into(SargOp::Eq, &7777i64.to_le_bytes(), n, &mut bm);
    assert!((bm[7777 / 64] >> (7777 % 64)) & 1 == 1, "true match pruned");
    // Bounds alone would leave one zone (512); fingerprints cut that further.
    let survivors: u64 = bm.iter().map(|w| w.count_ones() as u64).sum();
    assert!(
        survivors < 64,
        "expected fingerprint+bounds to compound, got {}",
        survivors
    );
}

#[test]
fn nan_never_poisons_bounds() {
    // A NaN that seeded min/max would stick forever (every comparison against it
    // is false) and prune every range query on that zone.
    let mut col = ColumnFilter::new(0, DbType::F64, HashSeed::default(), FilterConfig::default());
    col.set(0, Some(&f64::NAN.to_le_bytes()));
    for slot in 1..100u64 {
        col.set(slot, Some(&(slot as f64).to_le_bytes()));
    }

    let n = 100u64;
    let mut bm = vec![0u64; bitmap_words(n)];
    col.probe_pred_into(SargOp::Gt, &50.0f64.to_le_bytes(), n, &mut bm);
    for slot in 51..100u64 {
        assert!(
            (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1,
            "slot {} pruned after a NaN entered the zone",
            slot
        );
    }
}

#[test]
fn all_nan_zone_stays_a_candidate() {
    let mut col = ColumnFilter::new(0, DbType::F64, HashSeed::default(), FilterConfig::default());
    for slot in 0..10u64 {
        col.set(slot, Some(&f64::NAN.to_le_bytes()));
    }
    let mut bm = vec![0u64; bitmap_words(10)];
    col.probe_pred_into(SargOp::Gt, &1.0f64.to_le_bytes(), 10, &mut bm);
    // No bounds were ever established, so the zone cannot be ruled out.
    for slot in 0..10u64 {
        assert!(
            (bm[0] >> slot) & 1 == 1,
            "unbounded zone pruned slot {}",
            slot
        );
    }
}

#[test]
fn bounds_widen_on_update_and_never_shrink_on_delete() {
    let mut col = i64_col_with_zonemap();
    for slot in 0..100u64 {
        col.set(slot, Some(&(slot as i64).to_le_bytes()));
    }

    // Update pushes a value outside the recorded range: bounds must widen or the
    // row becomes unfindable.
    col.set(50, Some(&9_999i64.to_le_bytes()));
    let n = 100u64;
    let mut bm = vec![0u64; bitmap_words(n)];
    col.probe_pred_into(SargOp::Gt, &9_000i64.to_le_bytes(), n, &mut bm);
    assert!((bm[50 / 64] >> (50 % 64)) & 1 == 1, "widened value pruned");

    // Delete leaves the range conservatively wide — safe, just less selective.
    col.set(50, None);
    col.probe_pred_into(SargOp::Gt, &9_000i64.to_le_bytes(), n, &mut bm);
    let survivors: u64 = bm.iter().map(|w| w.count_ones() as u64).sum();
    assert!(
        survivors > 0,
        "delete must not shrink bounds into a false negative"
    );
}

#[test]
fn zonemap_survives_checkpoint() {
    let fields: SmallVec<[SchemaField; 20]> = smallvec::smallvec![
        SchemaField::new("id".into(), DbType::U64, 8, 0, 0, false),
        SchemaField::new("age".into(), DbType::I32, 4, 0, 1, false),
    ];
    let f = TableFilters::new(&fields, FilterConfig::default());
    let n = 5000u64;
    for slot in 0..n {
        let cols: SmallVec<[(u32, Vec<u8>); 32]> = smallvec::smallvec![
            (0u32, slot.to_le_bytes().to_vec()),
            (1u32, (slot as i32).to_le_bytes().to_vec()),
        ];
        f.backfill_row(slot, &cols);
    }

    let bytes = f.checkpoint_bytes(n);
    let restored = TableFilters::from_checkpoint(&bytes, &fields, FilterConfig::default(), n)
        .expect("zonemap round trip");

    let toks2 = crate::core::tokenizer::query_tokenizer::tokenize("age > 4800", &fields);
    let program = sarg::extract(&toks2, &restored).expect("range should be sargable");
    let bm = restored.evaluate(&program, n).expect("bitmap");

    for slot in 4801..n {
        assert!(bm.get(slot), "slot {} lost across checkpoint", slot);
    }
    assert!(
        bm.count() < 1200,
        "restored bounds lost their selectivity: {}",
        bm.count()
    );
}

#[test]
fn zonemap_rejects_mismatched_widths() {
    // A bound width that does not divide the data means the file is describing
    // some other column.
    assert!(ZoneMap::from_parts(DbType::I64, vec![0u8; 17], vec![0u64]).is_none());
    assert!(ZoneMap::from_parts(DbType::I64, vec![0u8; 16], vec![0u64]).is_some());
    // A string zone is `2 * (1 + k)` bytes, so a fixed-width stride does not
    // divide it and vice versa.
    let stride = 2 * (1 + super::zonemap::STRING_BOUND_PREFIX);
    assert!(ZoneMap::from_parts(DbType::STRING, vec![0u8; 16], vec![0u64]).is_none());
    assert!(ZoneMap::from_parts(DbType::STRING, vec![0u8; stride], vec![0u64]).is_some());
}

// ---------------------------------------------------------------------------
// Sum-of-products (OR) extraction
// ---------------------------------------------------------------------------

#[test]
fn sarg_groups_by_sql_precedence() {
    use super::sarg::MaskExpr;
    let f = filters();

    let shape = |q: &str| -> MaskExpr { sarg::extract(&toks(q), &f).unwrap().root };

    // `A OR B` -> two branches of one.
    match shape("id = 1 OR id = 2") {
        MaskExpr::Or(v) => {
            assert_eq!(v.len(), 2);
            assert!(v.iter().all(|b| matches!(b, MaskExpr::Pred(_))));
        }
        other => panic!("expected Or, got {:?}", other),
    }

    // `A AND B OR C AND D` -> (A∧B) ∨ (C∧D), matching the engine's evaluator.
    match shape("id = 1 AND age = 2 OR id = 3 AND age = 4") {
        MaskExpr::Or(v) => {
            assert_eq!(v.len(), 2);
            for b in &v {
                match b {
                    MaskExpr::And(inner) => assert_eq!(inner.len(), 2),
                    other => panic!("expected And, got {:?}", other),
                }
            }
        }
        other => panic!("expected Or, got {:?}", other),
    }

    // `A OR B AND C` -> A ∨ (B∧C). Folding left-to-right would give (A∨B)∧C,
    // which is NOT a superset.
    match shape("id = 1 OR id = 2 AND age = 3") {
        MaskExpr::Or(v) => {
            assert_eq!(v.len(), 2);
            assert!(matches!(v[0], MaskExpr::Pred(_)));
            assert!(matches!(&v[1], MaskExpr::And(i) if i.len() == 2));
        }
        other => panic!("expected Or, got {:?}", other),
    }

    // An AND-only group flattens exactly, so it is kept and nested as written.
    match shape("id = 1 AND (age = 2 AND age = 3)") {
        MaskExpr::And(v) => assert_eq!(v.len(), 2),
        other => panic!("expected And, got {:?}", other),
    }
}

#[test]
fn sarg_bails_when_any_or_branch_is_unusable() {
    let f = filters();
    // `id = 1 OR id != 2`: the second branch cannot be narrowed, so the union is
    // everything. Better to report "no pruning" than to build a full bitmap.
    assert!(sarg::extract(&toks("id = 1 OR id != 2"), &f).is_none());
    assert!(sarg::extract(&toks("id != 1 OR id = 2"), &f).is_none());
}

/// The soundness property, over randomly generated boolean expressions
/// *including parentheses*: the mask must contain every row the engine's own
/// evaluator would return.
///
/// This is the test that makes parenthesis support safe. A precedence or
/// grouping mistake shows up here as a row that matches but was pruned.
#[test]
fn masks_are_supersets_of_truth() {
    let fields = persist_schema();
    let f = TableFilters::new(&fields, FilterConfig::default());

    let n = 3000u64;
    let ids: Vec<u64> = (0..n).map(|i| i * 3 % 1500).collect();
    let ages: Vec<i32> = (0..n).map(|i| (i % 40) as i32).collect();
    for slot in 0..n {
        let cols: SmallVec<[(u32, Vec<u8>); 32]> = smallvec::smallvec![
            (0u32, ids[slot as usize].to_le_bytes().to_vec()),
            (2u32, ages[slot as usize].to_le_bytes().to_vec()),
        ];
        f.backfill_row(slot, &cols);
    }

    #[derive(Clone)]
    enum Expr {
        Id(u64),
        AgeEq(i32),
        AgeGt(i32),
        /// Deliberately unfilterable, so unknown branches are exercised too.
        Opaque,
        And(Box<Expr>, Box<Expr>),
        Or(Box<Expr>, Box<Expr>),
    }

    fn build(depth: u32) -> Expr {
        if depth == 0 || fastrand::u32(0..100) < 35 {
            return match fastrand::usize(0..4) {
                0 => Expr::Id(fastrand::u64(0..1500)),
                1 => Expr::AgeEq(fastrand::i32(0..40)),
                2 => Expr::AgeGt(fastrand::i32(0..40)),
                _ => Expr::Opaque,
            };
        }
        let (l, r) = (Box::new(build(depth - 1)), Box::new(build(depth - 1)));
        if fastrand::bool() {
            Expr::And(l, r)
        } else {
            Expr::Or(l, r)
        }
    }

    /// Render with explicit parens around every compound subexpression, so the
    /// text means exactly what the tree means regardless of precedence.
    fn render(e: &Expr, top: bool) -> String {
        match e {
            Expr::Id(v) => format!("id = {}", v),
            Expr::AgeEq(v) => format!("age = {}", v),
            Expr::AgeGt(v) => format!("age > {}", v),
            Expr::Opaque => "age != 999".to_string(),
            Expr::And(l, r) | Expr::Or(l, r) => {
                let op = if matches!(e, Expr::And(..)) {
                    "AND"
                } else {
                    "OR"
                };
                let inner = format!("{} {} {}", render(l, false), op, render(r, false));
                if top { inner } else { format!("({})", inner) }
            }
        }
    }

    fn truth(e: &Expr, id: u64, age: i32) -> bool {
        match e {
            Expr::Id(v) => id == *v,
            Expr::AgeEq(v) => age == *v,
            Expr::AgeGt(v) => age > *v,
            Expr::Opaque => age != 999,
            Expr::And(l, r) => truth(l, id, age) && truth(r, id, age),
            Expr::Or(l, r) => truth(l, id, age) || truth(r, id, age),
        }
    }

    let mut filtered_any = false;
    for _ in 0..400 {
        let e = build(3);
        let text = render(&e, true);

        let Some(program) = sarg::extract(&toks(&text), &f) else {
            continue;
        };
        let Some(bm) = f.evaluate(&program, n) else {
            continue;
        };
        filtered_any = true;

        for slot in 0..n {
            if truth(&e, ids[slot as usize], ages[slot as usize]) {
                assert!(bm.get(slot), "slot {} pruned but matches `{}`", slot, text);
            }
        }
    }
    assert!(
        filtered_any,
        "no generated expression was filterable; test proved nothing"
    );
}

// ---------------------------------------------------------------------------
// Guards for adding new DbTypes to the filter
// ---------------------------------------------------------------------------

/// Every fixed-width indexable type must hash at exactly `DbType::get_size()`
/// bytes, and refuse anything else.
///
/// `hash::fixed_width` keeps its own width table, so a new type whose entry
/// disagrees with `get_size()` would make `canonical_hash` return `None` for
/// every real value — the filter would silently stop pruning that column with
/// no error anywhere. This catches that drift the moment a type is added.
#[test]
fn fixed_width_agrees_with_db_type_size() {
    let seed = HashSeed::default();
    for ty in INDEXABLE {
        if *ty == DbType::STRING {
            continue; // variable width by definition
        }
        let w = ty.get_size() as usize;
        assert!(
            canonical_hash(ty, &vec![0u8; w], seed).is_some(),
            "{:?}: get_size() = {} is not the width canonical_hash accepts",
            ty,
            w
        );
        assert!(
            canonical_hash(ty, &vec![0u8; w + 1], seed).is_none(),
            "{:?}: accepted {} bytes, one more than get_size()",
            ty,
            w + 1
        );
        if w > 1 {
            assert!(
                canonical_hash(ty, &vec![0u8; w - 1], seed).is_none(),
                "{:?}: accepted {} bytes, one fewer than get_size()",
                ty,
                w - 1
            );
        }
    }
}

/// A type that carries zone bounds must use the same width for them as for its
/// fingerprints. A mismatch would make `zone_may_match` bail to "candidate" on
/// every probe — bounds costing storage while pruning nothing.
#[test]
fn ordered_width_agrees_with_db_type_size() {
    for ty in INDEXABLE {
        if let Some(w) = super::zonemap::ordered_width(ty) {
            assert_eq!(
                w,
                ty.get_size() as usize,
                "{:?}: ordered_width {} != get_size() {}",
                ty,
                w,
                ty.get_size()
            );
            assert!(
                ZoneMap::new(ty.clone()).is_some(),
                "{:?} has an ordered width but no ZoneMap",
                ty
            );
        }
    }
}

/// Anything the filter indexes must be something the evaluator can compare.
///
/// `perform_comparison_operation` panics on types it does not handle, so
/// indexing one would turn a `WHERE` clause into a process-level panic rather
/// than a query error.
#[test]
fn every_indexable_type_is_comparable() {
    for ty in INDEXABLE {
        let w = if *ty == DbType::STRING {
            4
        } else {
            ty.get_size() as usize
        };
        let a = vec![0u8; w];
        let b = vec![0u8; w];
        let r = std::panic::catch_unwind(|| {
            perform_comparison_operation(&a, &b, ty, &ComparerOperation::Equals)
        });
        assert!(
            r.is_ok(),
            "{:?} is marked indexable but panics in perform_comparison_operation",
            ty
        );
    }
}

// ---------------------------------------------------------------------------
// Literal coercion in sarg extraction
// ---------------------------------------------------------------------------

fn wide_schema() -> SmallVec<[SchemaField; 20]> {
    smallvec::smallvec![
        SchemaField::new("id".into(), DbType::U64, 8, 0, 0, false),
        SchemaField::new("amount".into(), DbType::DECIMAL, 16, 0, 1, false),
        SchemaField::new("uid".into(), DbType::UUID, 16, 0, 2, false),
    ]
}

#[test]
fn sarg_fires_on_cross_typed_and_reversed_literals() {
    // The tokenizer types a literal from the *preceding* column, so none of
    // these arrive already matching the column's type. Rejecting them left the
    // query correct but completely unfiltered — the silent no-pruning failure.
    let fields = wide_schema();
    let f = TableFilters::new(&fields, FilterConfig::default());

    for (q, want_op) in [
        ("id > 1990", SargOp::Gt),
        ("1990 < id", SargOp::Gt), // reversed: I32 literal vs U64 column
        ("1990 = id", SargOp::Eq),
        ("amount > 200", SargOp::Gt), // I32 literal vs DECIMAL column
        ("uid = '11111111-2222-3333-4444-555555555555'", SargOp::Eq), // text vs UUID
    ] {
        let toks = crate::core::tokenizer::query_tokenizer::tokenize(q, &fields);
        let p = sarg::extract(&toks, &f).unwrap_or_else(|| panic!("`{}` should be sargable", q));
        assert_eq!(p.all_preds()[0].op, want_op, "{}", q);
    }
}

#[test]
fn coerced_literal_bytes_match_what_the_evaluator_compares() {
    // This is the invariant that makes coercion safe. If `sarg` encoded a
    // literal any differently from the comparison path, the fingerprint would
    // not match and the row would be pruned — a false negative.
    let fields = wide_schema();
    let f = TableFilters::new(&fields, FilterConfig::default());

    // Expected bytes come from the same place the evaluator would get them:
    // `encode_literal` for the types with their own text syntax, and the plain
    // little-endian encoding for numerics (which is what numeric->numeric
    // coercion produces).
    let cases: &[(&str, DbType, Vec<u8>)] = &[
        (
            "uid = '11111111-2222-3333-4444-555555555555'",
            DbType::UUID,
            crate::core::value_codec::encode_literal(
                &DbType::UUID,
                "11111111-2222-3333-4444-555555555555",
            )
            .expect("uuid text should encode"),
        ),
        (
            "amount > 200",
            DbType::DECIMAL,
            crate::core::value_codec::coerce_literal(
                &200i32.to_le_bytes(),
                &DbType::I32,
                &DbType::DECIMAL,
            )
            .expect("i32 should coerce to DECIMAL"),
        ),
        ("1990 < id", DbType::U64, 1990u64.to_le_bytes().to_vec()),
    ];

    for (q, ty, expected) in cases {
        let toks = crate::core::tokenizer::query_tokenizer::tokenize(q, &fields);
        let p = sarg::extract(&toks, &f).unwrap();
        assert_eq!(
            &p.all_preds()[0].value,
            expected,
            "`{}`: sarg encoded the literal differently from the value codec",
            q
        );
        // And it must be exactly the width the comparator demands, or every
        // probe silently returns "unhashable" and filtering disables itself.
        assert_eq!(
            p.all_preds()[0].value.len(),
            ty.get_size() as usize,
            "`{}`: coerced literal is the wrong width for {:?}",
            q,
            ty
        );
    }
}

#[test]
fn uncoercible_literals_are_declined_not_guessed() {
    let fields = wide_schema();
    let f = TableFilters::new(&fields, FilterConfig::default());
    // Not a UUID: coercion fails, so the predicate contributes nothing rather
    // than hashing garbage that would prune real rows.
    let toks = crate::core::tokenizer::query_tokenizer::tokenize("uid = 'not-a-uuid'", &fields);
    assert!(sarg::extract(&toks, &f).is_none());
}

/// The filter acts on an **allowlist** of operators, so any operator added
/// later is opaque until someone deliberately teaches this module about it.
///
/// This is what makes case-insensitive comparison safe to add elsewhere.
/// `ILIKE` would break the §5.2 invariant outright — `'ACME'` and `'acme'`
/// compare equal but hash differently, so a mask built from one would prune
/// rows holding the other. Because `op_of` returns `None` for anything it does
/// not recognise, such a predicate collapses to `All` and the query scans in
/// full rather than losing rows.
#[test]
fn unknown_operators_are_opaque_not_guessed() {
    use crate::core::tokenizer::query_tokenizer::Token;
    let f = filters();

    let ident = Token::Ident(("name".to_string(), DbType::STRING, 1));
    let lit = Token::StringLit("acme".to_string());

    for op in [
        "ILIKE",
        "LIKE",
        "~",
        "~*",
        "SIMILAR TO",
        "IS DISTINCT FROM",
        "@>",
        "!=",
    ] {
        let toks = vec![ident.clone(), Token::Op(op.to_string()), lit.clone()];
        assert!(
            sarg::extract(&toks, &f).is_none(),
            "`{}` must be opaque to the filter, not silently acted on",
            op
        );
    }

    // And the allowlist is exactly the five that are sound.
    for (op, _) in [("=", 0), (">", 1), (">=", 2), ("<", 3), ("<=", 4)] {
        let toks = vec![
            Token::Ident(("id".to_string(), DbType::U64, 0)),
            Token::Op(op.to_string()),
            Token::Number(crate::core::tokenizer::query_tokenizer::NumericValue::U64(
                1,
            )),
        ];
        assert!(
            sarg::extract(&toks, &f).is_some(),
            "`{}` should be sargable",
            op
        );
    }
}

/// A case-insensitive column collation would break the hash invariant for `=`
/// itself, not just for a new operator — the failure `unknown_operators_are_
/// opaque_not_guessed` cannot catch.
///
/// Recorded as an executable statement of the invariant: if equality ever stops
/// being byte equality for `STRING`, `canonical_hash` must fold identically or
/// the type must leave `is_indexable`.
#[test]
fn string_equality_is_byte_equality() {
    let seed = HashSeed::default();
    let (lower, upper) = (b"acme".as_slice(), b"ACME".as_slice());

    assert!(
        !perform_comparison_operation(lower, upper, &DbType::STRING, &ComparerOperation::Equals)
            .unwrap(),
        "STRING equality became case-insensitive: canonical_hash must fold case \
         to match, or STRING must be removed from is_indexable"
    );
    assert_ne!(
        canonical_hash(&DbType::STRING, lower, seed),
        canonical_hash(&DbType::STRING, upper, seed)
    );
}
