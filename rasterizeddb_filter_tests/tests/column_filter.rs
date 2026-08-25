//! `ColumnFilter`: the no-false-negative guarantee, generations, the insert
//! frontier, deletes/updates, and adaptive width.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::filter::column::{ColumnFilter, Hll, bitmap_words, choose_f};
use rasterizeddb_core::core::filter::hash::HashSeed;
use rasterizeddb_core::core::filter::zonemap::SargOp;
use rasterizeddb_core::core::filter::FilterConfig;
use rasterizeddb_filter_tests::*;

fn col(ty: DbType) -> ColumnFilter {
    ColumnFilter::new(0, ty, HashSeed::default(), FilterConfig::default())
}

fn col_with_f(ty: DbType, f: u8) -> ColumnFilter {
    let cfg = FilterConfig { min_f: f, default_f: f, max_f: f, ..Default::default() };
    ColumnFilter::new(0, ty, HashSeed::default(), cfg)
}

fn is_candidate(bm: &[u64], slot: u64) -> bool {
    (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1
}

#[test]
fn no_false_negatives_for_every_indexable_type() {
    for ty in INDEXABLE {
        let mut c = col(ty.clone());
        let vals = sample_values(ty, 600);
        let n = vals.len() as u64;
        for (slot, v) in vals.iter().enumerate() {
            c.set(slot as u64, Some(v));
        }
        let mut bm = vec![0u64; bitmap_words(n)];
        for (slot, v) in vals.iter().enumerate() {
            // A value that is not equal to itself (NaN) can never be a true
            // match, so pruning its slot is correct rather than a false
            // negative. Only self-equal values carry the guarantee.
            let self_equal = rasterizeddb_core::core::row::logical::perform_comparison_operation(
                v,
                v,
                ty,
                &rasterizeddb_core::core::processor::transformer::ComparerOperation::Equals,
            )
            .unwrap_or(false);
            if !self_equal {
                continue;
            }
            c.probe_pred_into(SargOp::Eq, v, n, &mut bm);
            assert!(
                is_candidate(&bm, slot as u64),
                "{:?}: slot {} pruned despite holding the probed value",
                ty,
                slot
            );
        }
    }
}

#[test]
fn no_false_negatives_across_many_zones() {
    let mut c = col(DbType::U64);
    let n = 20_000u64; // ~40 zones
    let vals: Vec<u64> = (0..n).map(|_| fastrand::u64(..)).collect();
    for slot in 0..n {
        c.set(slot, Some(&vals[slot as usize].to_le_bytes()));
    }
    let mut bm = vec![0u64; bitmap_words(n)];
    for slot in 0..n {
        c.probe_pred_into(SargOp::Eq, &vals[slot as usize].to_le_bytes(), n, &mut bm);
        assert!(is_candidate(&bm, slot), "slot {} lost", slot);
    }
}

#[test]
fn out_of_order_inserts_never_lose_rows() {
    // Concurrent writers complete out of order; a gap in the frontier may cost
    // selectivity but must never prune an indexed row.
    let mut c = col(DbType::U64);
    let n = 5000u64;
    let vals: Vec<u64> = (0..n).map(|_| fastrand::u64(..)).collect();
    let mut order: Vec<u64> = (0..n).collect();
    fastrand::shuffle(&mut order);
    for &slot in &order {
        c.set(slot, Some(&vals[slot as usize].to_le_bytes()));
    }
    let mut bm = vec![0u64; bitmap_words(n)];
    for slot in 0..n {
        c.probe_pred_into(SargOp::Eq, &vals[slot as usize].to_le_bytes(), n, &mut bm);
        assert!(is_candidate(&bm, slot), "slot {} lost after out-of-order fill", slot);
    }
}

#[test]
fn slots_above_the_frontier_stay_candidates() {
    let mut c = col(DbType::U64);
    let v = 999u64.to_le_bytes();
    for slot in 0..600u64 {
        c.set(slot, Some(&v));
    }
    let n = 4000u64;
    let mut bm = vec![0u64; bitmap_words(n)];
    c.probe_pred_into(SargOp::Eq, &7777u64.to_le_bytes(), n, &mut bm);
    for slot in 600..n {
        assert!(is_candidate(&bm, slot), "slot {} above the frontier was pruned", slot);
    }
}

#[test]
fn unindexed_prefix_stays_candidate_until_cleared() {
    let mut c = col(DbType::U64);
    c.set_unindexed_prefix(1000);
    assert!(!c.is_complete());
    for slot in 1000..2000u64 {
        c.set(slot, Some(&1u64.to_le_bytes()));
    }
    let n = 2000u64;
    let mut bm = vec![0u64; bitmap_words(n)];
    c.probe_pred_into(SargOp::Eq, &42u64.to_le_bytes(), n, &mut bm);
    for slot in 0..1000u64 {
        assert!(is_candidate(&bm, slot), "un-backfilled slot {} was pruned", slot);
    }

    c.clear_unindexed_prefix();
    assert!(c.is_complete());
}

#[test]
fn delete_is_exact_and_local() {
    let mut c = col(DbType::U64);
    let n = 2000u64;
    for slot in 0..n {
        c.set(slot, Some(&(slot as u64).to_le_bytes()));
    }
    let target = 777u64;
    let mut bm = vec![0u64; bitmap_words(n)];

    c.probe_pred_into(SargOp::Eq, &target.to_le_bytes(), n, &mut bm);
    assert!(is_candidate(&bm, target));

    // Unlike a Bloom filter, removal is exact: the FPP improves.
    c.set(target, None);
    c.probe_pred_into(SargOp::Eq, &target.to_le_bytes(), n, &mut bm);
    assert!(!is_candidate(&bm, target), "cleared slot still matches");

    // Neighbours untouched.
    for near in [target - 1, target + 1] {
        c.probe_pred_into(SargOp::Eq, &near.to_le_bytes(), n, &mut bm);
        assert!(is_candidate(&bm, near), "delete leaked into slot {}", near);
    }
}

#[test]
fn update_in_place_replaces_the_old_value() {
    let mut c = col(DbType::U64);
    let n = 1000u64;
    for slot in 0..n {
        c.set(slot, Some(&(slot as u64).to_le_bytes()));
    }
    let mut bm = vec![0u64; bitmap_words(n)];

    c.set(500, Some(&424242u64.to_le_bytes()));

    c.probe_pred_into(SargOp::Eq, &424242u64.to_le_bytes(), n, &mut bm);
    assert!(is_candidate(&bm, 500), "new value not findable");

    c.probe_pred_into(SargOp::Eq, &500u64.to_le_bytes(), n, &mut bm);
    assert!(!is_candidate(&bm, 500), "old value still findable at that slot");
}

#[test]
fn repeated_overwrites_leave_only_the_last_value() {
    let mut c = col(DbType::U64);
    let n = 600u64;
    // Distinct values, not a constant: a column with one distinct value is
    // correctly taken out of service by the selectivity gate, which would make
    // this test measure the gate rather than overwrite behaviour.
    for slot in 0..n {
        c.set(slot, Some(&(slot + 1_000_000).to_le_bytes()));
    }
    let mut bm = vec![0u64; bitmap_words(n)];
    for round in 1..40u64 {
        c.set(123, Some(&round.to_le_bytes()));
        c.probe_pred_into(SargOp::Eq, &round.to_le_bytes(), n, &mut bm);
        assert!(is_candidate(&bm, 123), "round {}: current value lost", round);
        if round > 1 {
            c.probe_pred_into(SargOp::Eq, &(round - 1).to_le_bytes(), n, &mut bm);
            assert!(!is_candidate(&bm, 123), "round {}: previous value survived", round);
        }
    }
}

#[test]
fn false_positive_rate_is_near_theory() {
    for f in [6u8, 8, 10] {
        let mut c = col_with_f(DbType::U64, f);
        let n = 20_000u64;
        for slot in 0..n {
            c.set(slot, Some(&fastrand::u64(..).to_le_bytes()));
        }
        let mut bm = vec![0u64; bitmap_words(n)];
        let trials = 100u64;
        let mut hits = 0u64;
        for _ in 0..trials {
            c.probe_hash_into(fastrand::u64(..), n, &mut bm);
            hits += bm.iter().map(|w| w.count_ones() as u64).sum::<u64>();
        }
        let observed = hits as f64 / (trials * n) as f64;
        let theory = 1.0 / ((1u32 << f) - 2) as f64;
        assert!(
            observed < theory * 2.5,
            "f={}: FPP {:.6} vs theory {:.6}",
            f,
            observed,
            theory
        );
    }
}

#[test]
fn generations_keep_older_zones_readable() {
    // `f` grows with the table, so early zones carry a narrower width. All of
    // them must still answer.
    let cfg = FilterConfig { min_f: 4, default_f: 4, max_f: 16, ..Default::default() };
    let mut c = ColumnFilter::new(0, DbType::U64, HashSeed::default(), cfg);
    let n = 30_000u64;
    let vals: Vec<u64> = (0..n).map(|_| fastrand::u64(..)).collect();
    for slot in 0..n {
        c.set(slot, Some(&vals[slot as usize].to_le_bytes()));
    }
    assert!(c.current_f() > 4, "f should have grown, got {}", c.current_f());
    assert!(c.generations().len() > 1, "expected more than one generation");

    let mut bm = vec![0u64; bitmap_words(n)];
    for slot in 0..n {
        c.probe_pred_into(SargOp::Eq, &vals[slot as usize].to_le_bytes(), n, &mut bm);
        assert!(is_candidate(&bm, slot), "slot {} lost across a generation boundary", slot);
    }
}

#[test]
fn generation_layout_is_contiguous_and_well_formed() {
    let cfg = FilterConfig { min_f: 4, default_f: 4, max_f: 16, ..Default::default() };
    let mut c = ColumnFilter::new(0, DbType::U64, HashSeed::default(), cfg);
    for slot in 0..30_000u64 {
        c.set(slot, Some(&fastrand::u64(..).to_le_bytes()));
    }
    let mut next_zone = 0u64;
    for g in c.generations() {
        assert_eq!(g.start_zone, next_zone, "generations must tile without gaps");
        assert!(g.f >= 4 && g.f <= 16);
        assert_eq!(
            g.words.len() % (8 * g.f as usize),
            0,
            "generation word count must be a whole number of zones"
        );
        next_zone = g.start_zone + g.zone_count();
    }
}

#[test]
fn storage_is_exactly_f_bits_per_row() {
    for f in [4u8, 8, 12, 16] {
        let mut c = col_with_f(DbType::U64, f);
        let n = 100_000u64;
        for slot in 0..n {
            c.set(slot, Some(&fastrand::u64(..).to_le_bytes()));
        }
        // Fingerprint planes only; a U64 column also carries zone bounds, which
        // are counted separately by design.
        //
        // Measure against zone-rounded slots: storage is allocated a whole
        // 512-row zone at a time, so a table that does not end on a zone
        // boundary legitimately carries a partial zone's worth of slack.
        let plane_bytes: usize = c.generations().iter().map(|g| g.words.len() * 8).sum();
        let zones = n.div_ceil(512);
        let allocated_slots = zones * 512;
        let bits_per_slot = plane_bytes as f64 * 8.0 / allocated_slots as f64;
        assert!(
            (bits_per_slot - f as f64).abs() < 0.01,
            "f={}: {:.4} bits per allocated slot",
            f,
            bits_per_slot
        );
        // And the slack from the partial zone must stay small.
        let bits_per_row = plane_bytes as f64 * 8.0 / n as f64;
        assert!(
            bits_per_row < f as f64 * 1.01,
            "f={}: {:.3} bits/row is more than 1% over",
            f,
            bits_per_row
        );
    }
}

#[test]
fn choose_f_gates_low_cardinality_regardless_of_table_size() {
    let cfg = FilterConfig::default();
    // A predicate matching ~20% of rows is not worth pruning, at any scale.
    for rows in [1_000u64, 1_000_000, 1_000_000_000] {
        assert_eq!(choose_f(2, rows, &cfg), 0, "rows={}", rows);
        assert_eq!(choose_f(5, rows, &cfg), 0, "rows={}", rows);
    }
    // 1024 distinct values is 0.1% selectivity — worth filtering at any scale.
    for rows in [10_000u64, 1_000_000] {
        assert!(choose_f(1024, rows, &cfg) >= cfg.min_f, "rows={}", rows);
    }
    // Unique columns clamp to max_f.
    assert_eq!(choose_f(1_000_000, 1_000_000, &cfg), cfg.max_f);
    // And f tracks log2(distinct) between the bounds.
    let mut last = 0u8;
    for d in [64u64, 256, 4096, 65536] {
        let f = choose_f(d, 1_000_000, &cfg);
        assert!(f >= last, "f should be monotonic in distinct count");
        last = f;
    }
}

#[test]
fn hll_estimates_within_tolerance() {
    for truth in [64u64, 1_000, 50_000, 250_000] {
        let mut h = Hll::default();
        for i in 0..truth {
            h.add(
                rasterizeddb_core::core::filter::hash::canonical_hash(
                    &DbType::U64,
                    &i.to_le_bytes(),
                    HashSeed::default(),
                )
                .unwrap(),
            );
        }
        let est = h.estimate() as f64;
        let err = (est - truth as f64).abs() / truth as f64;
        assert!(err < 0.3, "HLL said {} for {} (err {:.3})", est, truth, err);
    }
}

#[test]
fn hll_roundtrips_through_its_registers() {
    let mut h = Hll::default();
    for i in 0..10_000u64 {
        h.add(fastrand::u64(..).wrapping_add(i));
    }
    let restored = Hll::from_regs(*h.regs());
    assert_eq!(h.estimate(), restored.estimate());
}

#[test]
fn probing_an_empty_column_leaves_everything_a_candidate() {
    let c = col(DbType::U64);
    let n = 1000u64;
    let mut bm = vec![0u64; bitmap_words(n)];
    c.probe_pred_into(SargOp::Eq, &1u64.to_le_bytes(), n, &mut bm);
    for slot in 0..n {
        assert!(is_candidate(&bm, slot), "empty column pruned slot {}", slot);
    }
}

#[test]
fn bitmap_tail_bits_beyond_n_slots_are_clear() {
    // Otherwise popcounts overstate how much survived.
    let mut c = col(DbType::U64);
    for slot in 0..100u64 {
        c.set(slot, Some(&slot.to_le_bytes()));
    }
    for n in [1u64, 63, 64, 65, 100, 511, 512, 513] {
        let mut bm = vec![0u64; bitmap_words(n)];
        c.probe_pred_into(SargOp::Eq, &5u64.to_le_bytes(), n, &mut bm);
        let live: u64 = bm.iter().map(|w| w.count_ones() as u64).sum();
        assert!(live <= n, "n={}: {} bits set, more than slots", n, live);
    }
}

#[test]
fn unhashable_probe_value_leaves_everything_a_candidate() {
    let mut c = col(DbType::U64);
    let n = 500u64;
    for slot in 0..n {
        c.set(slot, Some(&slot.to_le_bytes()));
    }
    let mut bm = vec![0u64; bitmap_words(n)];
    // Wrong width: the evaluator would raise OperandSizeMismatch, so the filter
    // must not prune on it.
    let usable = c.probe_pred_into(SargOp::Eq, &[1, 2, 3], n, &mut bm);
    assert!(!usable);
    for slot in 0..n {
        assert!(is_candidate(&bm, slot), "slot {} pruned on an unhashable probe", slot);
    }
}
