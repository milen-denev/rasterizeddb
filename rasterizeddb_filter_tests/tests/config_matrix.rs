//! Behaviour across the tuning space, and across data shapes.
//!
//! The safety property must hold at every fingerprint width and for every
//! distribution. Selectivity is allowed to vary wildly — that is the whole
//! point of the knobs — but a matching row is never pruned at any setting.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::filter::column::{ColumnFilter, bitmap_words, choose_f};
use rasterizeddb_core::core::filter::hash::HashSeed;
use rasterizeddb_core::core::filter::sarg;
use rasterizeddb_core::core::filter::zonemap::SargOp;
use rasterizeddb_core::core::filter::{FilterConfig, TableFilters};
use rasterizeddb_core::core::index_store::gate::{Proof, test_mint};
use rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize;
use rasterizeddb_filter_tests::*;

fn cfg(min_f: u8, max_f: u8) -> FilterConfig {
    FilterConfig { min_f, default_f: min_f, max_f, ..Default::default() }
}

fn is_candidate(bm: &[u64], slot: u64) -> bool {
    (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1
}

#[test]
fn no_false_negatives_at_every_fingerprint_width() {
    for f in 4..=16u8 {
        let mut c = ColumnFilter::new(0, DbType::U64, HashSeed::default(), cfg(f, f));
        let n = 6000u64;
        let vals: Vec<u64> = (0..n).map(|_| fastrand::u64(..)).collect();
        for slot in 0..n {
            c.set(slot, Some(&vals[slot as usize].to_le_bytes()));
        }
        assert_eq!(c.current_f(), f, "width was not honoured");

        let mut bm = vec![0u64; bitmap_words(n)];
        for slot in (0..n).step_by(7) {
            c.probe_pred_into(SargOp::Eq, &vals[slot as usize].to_le_bytes(), n, &mut bm);
            assert!(is_candidate(&bm, slot), "f={}: slot {} lost", f, slot);
        }
    }
}

#[test]
fn selectivity_improves_monotonically_with_width() {
    // Wider fingerprints must not prune *worse*. Measured on absent values, so
    // every surviving candidate is a false positive.
    let n = 20_000u64;
    let mut last = u64::MAX;
    for f in [4u8, 6, 8, 10, 12] {
        let mut c = ColumnFilter::new(0, DbType::U64, HashSeed::default(), cfg(f, f));
        for slot in 0..n {
            c.set(slot, Some(&fastrand::u64(..).to_le_bytes()));
        }
        let mut bm = vec![0u64; bitmap_words(n)];
        let mut hits = 0u64;
        for _ in 0..60 {
            c.probe_hash_into(fastrand::u64(..), n, &mut bm);
            hits += bm.iter().map(|w| w.count_ones() as u64).sum::<u64>();
        }
        assert!(
            hits <= last,
            "f={} produced more false positives ({}) than a narrower width ({})",
            f,
            hits,
            last
        );
        last = hits;
    }
}

#[test]
fn storage_scales_linearly_with_width() {
    let n = 50_000u64;
    let mut prev: Option<(u8, usize)> = None;
    for f in [4u8, 8, 12, 16] {
        let mut c = ColumnFilter::new(0, DbType::U64, HashSeed::default(), cfg(f, f));
        for slot in 0..n {
            c.set(slot, Some(&fastrand::u64(..).to_le_bytes()));
        }
        let planes: usize = c.generations().iter().map(|g| g.words.len() * 8).sum();
        if let Some((pf, pb)) = prev {
            let ratio = planes as f64 / pb as f64;
            let want = f as f64 / pf as f64;
            assert!(
                (ratio - want).abs() < 0.05,
                "f {}->{}: storage grew {:.3}x, expected {:.3}x",
                pf,
                f,
                ratio,
                want
            );
        }
        prev = Some((f, planes));
    }
}

#[test]
fn the_selectivity_gate_moves_with_its_setting() {
    // `min_selectivity` is the *largest* fraction an equality predicate may
    // match and still be worth filtering, so the gate is
    // `distinct >= 1 / min_selectivity`. A larger value is therefore the more
    // permissive setting: it indexes columns whose predicates match more rows.
    let permissive = FilterConfig { min_selectivity: 0.5, ..Default::default() };
    let strict = FilterConfig { min_selectivity: 0.001, ..Default::default() };

    // 100 distinct values: a predicate matches ~1% of rows.
    assert!(
        choose_f(100, 1_000_000, &permissive) > 0,
        "1% selectivity should clear a gate that allows up to 50%"
    );
    assert_eq!(
        choose_f(100, 1_000_000, &strict),
        0,
        "1% selectivity should fail a gate that demands 0.1%"
    );

    // 2000 distinct clears both.
    assert!(choose_f(2000, 1_000_000, &permissive) > 0);
    assert!(choose_f(2000, 1_000_000, &strict) > 0);

    // A boolean-shaped column fails even the most permissive setting: with
    // min_selectivity = 0.5 the gate demands at least 2 distinct values, and a
    // predicate matching half the table cannot pay for the bits.
    assert_eq!(choose_f(1, 1_000_000, &permissive), 0);
    assert_eq!(choose_f(2, 1_000_000, &strict), 0);

    // And the gate is absolute, not scaled by table size.
    for rows in [1_000u64, 1_000_000, 1_000_000_000] {
        assert_eq!(choose_f(100, rows, &strict), 0, "rows={}", rows);
        assert!(choose_f(100, rows, &permissive) > 0, "rows={}", rows);
    }
}

#[test]
fn clamping_is_respected_at_both_ends() {
    for (min_f, max_f) in [(4u8, 6u8), (8, 8), (10, 16)] {
        let c = FilterConfig { min_f, default_f: min_f, max_f, ..Default::default() };
        // Unique column: wants log2(n), clamped to max_f.
        assert_eq!(choose_f(1 << 20, 1 << 20, &c), max_f, "max clamp");
        // Just past the gate: clamped up to min_f.
        let f = choose_f(64, 1_000_000, &c);
        assert!(f >= min_f && f <= max_f, "min_f={} max_f={} gave {}", min_f, max_f, f);
    }
}

// ---------------------------------------------------------------------------
// Data shapes
// ---------------------------------------------------------------------------

/// Build a column from a generator and confirm every value is still findable.
fn shape_holds(label: &str, ty: DbType, vals: Vec<Vec<u8>>) {
    let n = vals.len() as u64;
    let mut c = ColumnFilter::new(0, ty.clone(), HashSeed::default(), FilterConfig::default());
    for (slot, v) in vals.iter().enumerate() {
        c.set(slot as u64, Some(v));
    }
    let mut bm = vec![0u64; bitmap_words(n)];
    for (slot, v) in vals.iter().enumerate() {
        c.probe_pred_into(SargOp::Eq, v, n, &mut bm);
        assert!(
            is_candidate(&bm, slot as u64),
            "{}: slot {} lost (f={})",
            label,
            slot,
            c.current_f()
        );
    }
}

#[test]
fn every_value_identical() {
    // One distinct value: the gate declines to index it, and nothing is lost.
    shape_holds(
        "all-same",
        DbType::U64,
        (0..5000).map(|_| 42u64.to_le_bytes().to_vec()).collect(),
    );
}

#[test]
fn every_value_distinct() {
    shape_holds(
        "all-unique",
        DbType::U64,
        (0..5000u64).map(|i| i.to_le_bytes().to_vec()).collect(),
    );
}

#[test]
fn one_dominant_value_with_a_long_tail() {
    // 99% one value, 1% unique — the shape a nullable-ish or status column has.
    shape_holds(
        "skewed",
        DbType::U64,
        (0..8000u64)
            .map(|i| if i % 100 == 0 { i } else { 7 }.to_le_bytes().to_vec())
            .collect(),
    );
}

#[test]
fn strictly_increasing_and_strictly_decreasing() {
    shape_holds(
        "ascending",
        DbType::I64,
        (0..5000i64).map(|i| i.to_le_bytes().to_vec()).collect(),
    );
    shape_holds(
        "descending",
        DbType::I64,
        (0..5000i64).rev().map(|i| i.to_le_bytes().to_vec()).collect(),
    );
}

#[test]
fn values_clustered_into_a_narrow_band() {
    shape_holds(
        "narrow-band",
        DbType::I64,
        (0..5000).map(|_| fastrand::i64(0..8).to_le_bytes().to_vec()).collect(),
    );
}

#[test]
fn alternating_extremes() {
    shape_holds(
        "extremes",
        DbType::I64,
        (0..4000)
            .map(|i| if i % 2 == 0 { i64::MIN } else { i64::MAX }.to_le_bytes().to_vec())
            .collect(),
    );
}

#[test]
fn strings_of_wildly_varying_length() {
    shape_holds(
        "varied-strings",
        DbType::STRING,
        (0..3000)
            .map(|i| {
                let len = match i % 5 {
                    0 => 0,
                    1 => 1,
                    2 => 32,
                    3 => 300,
                    _ => 4000,
                };
                vec![b'a' + (i % 26) as u8; len]
            })
            .collect(),
    );
}

#[test]
fn strings_sharing_long_prefixes() {
    // Hashing must not degrade when values differ only at the end.
    shape_holds(
        "shared-prefix",
        DbType::STRING,
        (0..3000u32)
            .map(|i| format!("{}{:08}", "prefix-".repeat(20), i).into_bytes())
            .collect(),
    );
}

#[test]
fn floats_including_every_special_value() {
    let mut vals: Vec<Vec<u8>> = vec![
        0.0f64, -0.0, 1.0, -1.0, f64::MIN, f64::MAX, f64::MIN_POSITIVE,
        f64::INFINITY, f64::NEG_INFINITY, f64::EPSILON,
    ]
    .into_iter()
    .map(|v| v.to_le_bytes().to_vec())
    .collect();
    for i in 0..2000 {
        vals.push((i as f64 / 3.0).to_le_bytes().to_vec());
    }
    // NaN is excluded: it is not equal to itself, so it carries no guarantee.
    shape_holds("floats", DbType::F64, vals);
}

// ---------------------------------------------------------------------------
// Adaptive width over a growing table
// ---------------------------------------------------------------------------

#[test]
fn width_adapts_as_a_column_grows_and_old_rows_stay_findable() {
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    let c = FilterConfig { min_f: 4, default_f: 4, max_f: 16, ..Default::default() };
    let fields = fields_of(&[("id", DbType::U64)]);
    let f = TableFilters::new(&fields, c);

    let n = 40_000u64;
    let vals: Vec<u64> = (0..n).map(|_| fastrand::u64(..)).collect();
    let mut widths = Vec::new();
    for slot in 0..n {
        let sv: smallvec::SmallVec<[(u32, Vec<u8>); 32]> =
            smallvec::smallvec![(0u32, vals[slot as usize].to_le_bytes().to_vec())];
        f.backfill_row(proof, slot, &sv);
        if slot % 8000 == 0 {
            widths.push(f.stats()[0].1);
        }
    }
    assert!(
        widths.windows(2).all(|w| w[1] >= w[0]),
        "width should not shrink as the table grows: {:?}",
        widths
    );
    assert!(f.stats()[0].1 > 4, "width never adapted: {:?}", widths);

    // Rows written under the narrowest width must still be findable.
    for slot in (0..n).step_by(211) {
        let q = format!("id = {}", vals[slot as usize]);
        let p = sarg::extract(&tokenize(&q, &fields), &f).expect("sargable");
        let bm = f.evaluate(&p, n).expect("bitmap");
        assert!(bm.get(slot), "slot {} lost as the width grew", slot);
    }
}

#[test]
fn a_narrow_width_still_prunes_something() {
    // Even f = 4 (about 7% FPP) should beat scanning everything.
    let n = 20_000u64;
    let mut c = ColumnFilter::new(0, DbType::U64, HashSeed::default(), cfg(4, 4));
    for slot in 0..n {
        c.set(slot, Some(&fastrand::u64(..).to_le_bytes()));
    }
    let mut bm = vec![0u64; bitmap_words(n)];
    c.probe_hash_into(fastrand::u64(..), n, &mut bm);
    let live: u64 = bm.iter().map(|w| w.count_ones() as u64).sum();
    assert!(live < n / 2, "f=4 left {} of {} candidates", live, n);
}
