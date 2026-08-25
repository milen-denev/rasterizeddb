//! Per-zone min/max bounds — the structure that covers `<`, `>`, `<=`, `>=`,
//! which fingerprints cannot express at all.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::filter::column::{ColumnFilter, bitmap_words};
use rasterizeddb_core::core::filter::hash::HashSeed;
use rasterizeddb_core::core::filter::zonemap::*;
use rasterizeddb_core::core::filter::FilterConfig;
use rasterizeddb_filter_tests::*;

fn col(ty: DbType) -> ColumnFilter {
    ColumnFilter::new(0, ty, HashSeed::default(), FilterConfig::default())
}

fn is_candidate(bm: &[u64], slot: u64) -> bool {
    (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1
}

#[test]
fn every_indexable_type_carries_bounds() {
    // Fixed-width types store their bounds exactly, at `ordered_width` bytes
    // each. STRING stores a truncated prefix instead, so it has no fixed width
    // — that is the difference the two assertions below are pinning.
    assert!(ordered_width(&DbType::STRING).is_none());
    assert!(col(DbType::STRING).has_zonemap(), "STRING carries truncated bounds");

    for ty in INDEXABLE {
        assert!(col(ty.clone()).has_zonemap(), "{:?}", ty);
        assert!(ZoneMap::new(ty.clone()).is_some(), "{:?}", ty);
        if *ty == DbType::STRING {
            continue;
        }
        let w = ordered_width(ty)
            .unwrap_or_else(|| panic!("{:?} should carry ordered bounds", ty));
        assert_eq!(w, width(ty), "{:?}: bound width must match get_size()", ty);
    }
}

#[test]
fn zonemap_rejects_mismatched_widths() {
    assert!(ZoneMap::from_parts(DbType::I64, vec![0u8; 17], vec![0u64]).is_none());
    assert!(ZoneMap::from_parts(DbType::I64, vec![0u8; 16], vec![0u64]).is_some());
    // A string zone is `2 * (1 + STRING_BOUND_PREFIX)` bytes; a stride that does
    // not divide the blob means the file describes a different column.
    assert!(ZoneMap::from_parts(DbType::STRING, vec![0u8; 16], vec![0u64]).is_none());
    let stride = 2 * (1 + STRING_BOUND_PREFIX);
    assert!(ZoneMap::from_parts(DbType::STRING, vec![0u8; stride], vec![0u64]).is_some());
}

#[test]
fn a_truncated_max_bound_rounds_up() {
    // Truncation makes a string smaller. A `min` bound may move down freely; a
    // `max` bound moving down would sit *below* the data it brackets and prune
    // rows that match, so it rounds up instead.
    let k = STRING_BOUND_PREFIX;
    for tail in ["", "x", "zzzzzzzz"] {
        let mut base: Vec<u8> = (0..k).map(|i| b'a' + (i % 5) as u8).collect();
        base.extend_from_slice(tail.as_bytes());
        let mut zm = ZoneMap::new(DbType::STRING).unwrap();
        zm.observe(0, &base);
        assert!(
            zm.zone_may_match(0, SargOp::Eq, &base),
            "tail {:?}: the zone lost the value it was built from",
            tail
        );
    }
}

#[test]
fn successor_is_above_every_extension_of_its_prefix() {
    assert_eq!(successor(b"abc").as_deref(), Some(&b"abd"[..]));
    // Trailing 0xFF bytes roll over as the carry propagates.
    assert_eq!(successor(&[b'a', 0xFF, 0xFF]).as_deref(), Some(&[b'b', 0, 0][..]));
    // Nothing is above an all-0xFF prefix.
    assert!(successor(&[0xFF, 0xFF]).is_none());
    assert!(successor(b"").is_none());

    // The property that makes `LIKE 'abc%'` a range at all.
    for _ in 0..200 {
        let plen = fastrand::usize(1..6);
        let prefix: Vec<u8> = (0..plen).map(|_| fastrand::u8(b'a'..=b'c')).collect();
        let hi = successor(&prefix).expect("a..c prefixes always have a successor");
        let tail_len = fastrand::usize(0..6);
        let mut s = prefix.clone();
        s.extend((0..tail_len).map(|_| fastrand::u8(b'a'..=b'c')));
        assert!(s >= prefix, "{:?} should be at or above its own prefix", s);
        assert!(s < hi, "{:?} should be below successor({:?}) = {:?}", s, prefix, hi);
    }
}

#[test]
fn range_probes_have_no_false_negatives() {
    let mut c = col(DbType::I64);
    let n = 8000u64;
    let vals: Vec<i64> = (0..n).map(|_| fastrand::i64(-100_000..100_000)).collect();
    for slot in 0..n {
        c.set(slot, Some(&vals[slot as usize].to_le_bytes()));
    }
    let mut bm = vec![0u64; bitmap_words(n)];
    for _ in 0..150 {
        let pivot = fastrand::i64(-120_000..120_000);
        for op in [SargOp::Gt, SargOp::Ge, SargOp::Lt, SargOp::Le, SargOp::Eq] {
            c.probe_pred_into(op, &pivot.to_le_bytes(), n, &mut bm);
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
                        is_candidate(&bm, slot),
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
fn range_probes_are_sound_for_every_ordered_type() {
    for ty in INDEXABLE {
        if *ty == DbType::STRING {
            continue;
        }
        let mut c = col(ty.clone());
        let vals = sample_values(ty, 700);
        let n = vals.len() as u64;
        for (slot, v) in vals.iter().enumerate() {
            c.set(slot as u64, Some(v));
        }
        let mut bm = vec![0u64; bitmap_words(n)];
        for _ in 0..25 {
            let pivot = &vals[fastrand::usize(0..vals.len())];
            for op in [SargOp::Gt, SargOp::Ge, SargOp::Lt, SargOp::Le] {
                c.probe_pred_into(op, pivot, n, &mut bm);
                for (slot, v) in vals.iter().enumerate() {
                    let cmp_op = match op {
                        SargOp::Gt => rasterizeddb_core::core::processor::transformer::ComparerOperation::Greater,
                        SargOp::Ge => rasterizeddb_core::core::processor::transformer::ComparerOperation::GreaterOrEquals,
                        SargOp::Lt => rasterizeddb_core::core::processor::transformer::ComparerOperation::Less,
                        SargOp::Le => rasterizeddb_core::core::processor::transformer::ComparerOperation::LessOrEquals,
                        SargOp::Eq => unreachable!(),
                    };
                    let truth = rasterizeddb_core::core::row::logical::perform_comparison_operation(
                        v, pivot, ty, &cmp_op,
                    )
                    .unwrap_or(true);
                    if truth {
                        assert!(
                            is_candidate(&bm, slot as u64),
                            "{:?}: slot {} pruned for {:?}",
                            ty,
                            slot,
                            op
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn ordered_data_prunes_hard_and_random_data_does_not() {
    // The whole point of zone maps, and their whole limitation.
    let n = 20_000u64;

    let mut ordered = col(DbType::I64);
    for slot in 0..n {
        ordered.set(slot, Some(&(slot as i64).to_le_bytes()));
    }
    let mut random = col(DbType::I64);
    for slot in 0..n {
        random.set(slot, Some(&fastrand::i64(..).to_le_bytes()));
    }

    let pivot = (n as i64) * 9 / 10;
    let mut bm = vec![0u64; bitmap_words(n)];

    ordered.probe_pred_into(SargOp::Gt, &pivot.to_le_bytes(), n, &mut bm);
    let ordered_live: u64 = bm.iter().map(|w| w.count_ones() as u64).sum();
    // The true answer is n/10; zone granularity is 512, so allow a zone of slack.
    assert!(
        ordered_live < n / 10 + 600,
        "append-ordered data should prune to ~10%, got {}",
        ordered_live
    );
    for slot in (pivot as u64 + 1)..n {
        assert!(is_candidate(&bm, slot), "slot {} lost", slot);
    }

    random.probe_pred_into(SargOp::Gt, &pivot.to_le_bytes(), n, &mut bm);
    let random_live: u64 = bm.iter().map(|w| w.count_ones() as u64).sum();
    assert!(
        random_live > ordered_live,
        "random data cannot prune by bounds: {} vs {}",
        random_live,
        ordered_live
    );
}

#[test]
fn equality_combines_fingerprints_and_bounds() {
    let n = 20_000u64;
    let mut c = col(DbType::I64);
    for slot in 0..n {
        c.set(slot, Some(&(slot as i64).to_le_bytes()));
    }
    let mut bm = vec![0u64; bitmap_words(n)];
    c.probe_pred_into(SargOp::Eq, &7777i64.to_le_bytes(), n, &mut bm);
    assert!(is_candidate(&bm, 7777), "true match pruned");
    let live: u64 = bm.iter().map(|w| w.count_ones() as u64).sum();
    // Bounds alone leave one zone (512); fingerprints cut that much further.
    assert!(live < 64, "expected bounds and fingerprints to compound, got {}", live);
}

#[test]
fn nan_never_poisons_bounds() {
    // Every IEEE comparison against NaN is false, so a NaN that seeded min/max
    // would stick there permanently and prune every range query on that zone.
    for (ty, nan, mk) in [
        (
            DbType::F64,
            f64::NAN.to_le_bytes().to_vec(),
            (|i: u64| (i as f64).to_le_bytes().to_vec()) as fn(u64) -> Vec<u8>,
        ),
        (
            DbType::F32,
            f32::NAN.to_le_bytes().to_vec(),
            (|i: u64| (i as f32).to_le_bytes().to_vec()) as fn(u64) -> Vec<u8>,
        ),
    ] {
        let mut c = col(ty.clone());
        c.set(0, Some(&nan));
        for slot in 1..400u64 {
            c.set(slot, Some(&mk(slot)));
        }
        let n = 400u64;
        let mut bm = vec![0u64; bitmap_words(n)];
        let pivot = mk(200);
        c.probe_pred_into(SargOp::Gt, &pivot, n, &mut bm);
        for slot in 201..n {
            assert!(
                is_candidate(&bm, slot),
                "{:?}: slot {} pruned after a NaN entered the zone",
                ty,
                slot
            );
        }
    }
}

#[test]
fn an_all_nan_zone_stays_a_candidate() {
    let mut c = col(DbType::F64);
    for slot in 0..50u64 {
        c.set(slot, Some(&f64::NAN.to_le_bytes()));
    }
    let n = 50u64;
    let mut bm = vec![0u64; bitmap_words(n)];
    c.probe_pred_into(SargOp::Gt, &1.0f64.to_le_bytes(), n, &mut bm);
    for slot in 0..n {
        assert!(is_candidate(&bm, slot), "unbounded zone pruned slot {}", slot);
    }
}

#[test]
fn bounds_widen_on_update_and_never_shrink_on_delete() {
    let mut c = col(DbType::I64);
    let n = 400u64;
    for slot in 0..n {
        c.set(slot, Some(&(slot as i64).to_le_bytes()));
    }
    let mut bm = vec![0u64; bitmap_words(n)];

    // An update outside the recorded range must widen it, or the row vanishes.
    c.set(50, Some(&9_999i64.to_le_bytes()));
    c.probe_pred_into(SargOp::Gt, &9_000i64.to_le_bytes(), n, &mut bm);
    assert!(is_candidate(&bm, 50), "widened value pruned");

    // A delete cannot shrink bounds — we cannot know the removed value was the
    // sole occupant. Conservative is safe; too narrow would not be.
    c.set(50, None);
    c.probe_pred_into(SargOp::Gt, &9_000i64.to_le_bytes(), n, &mut bm);
    let live: u64 = bm.iter().map(|w| w.count_ones() as u64).sum();
    assert!(live > 0, "delete shrank bounds into a false negative");
}

#[test]
fn uuid_ordering_follows_stored_byte_order() {
    // Canonical UUID octets are big-endian and compare by memcmp, so a
    // time-ordered key prunes; a random one cannot.
    let n = 20_000u64;

    let mut seq = col(DbType::UUID);
    for slot in 0..n {
        seq.set(slot, Some(&uuid_ordered(slot)));
    }
    let mut rnd = col(DbType::UUID);
    for slot in 0..n {
        rnd.set(slot, Some(&uuid_v4()));
    }

    let pivot = uuid_ordered(n * 9 / 10);
    let mut bm = vec![0u64; bitmap_words(n)];

    seq.probe_pred_into(SargOp::Gt, &pivot, n, &mut bm);
    let seq_live: u64 = bm.iter().map(|w| w.count_ones() as u64).sum();

    rnd.probe_pred_into(SargOp::Gt, &pivot, n, &mut bm);
    let rnd_live: u64 = bm.iter().map(|w| w.count_ones() as u64).sum();

    assert!(
        seq_live < n / 2,
        "time-ordered UUIDs should prune on a range, {} of {} survived",
        seq_live,
        n
    );
    assert!(
        rnd_live >= seq_live,
        "random UUIDs cannot prune better than ordered ones ({} vs {})",
        rnd_live,
        seq_live
    );
}

#[test]
fn zones_for_rounds_up() {
    assert_eq!(zones_for(0), 0);
    assert_eq!(zones_for(1), 1);
    assert_eq!(zones_for(512), 1);
    assert_eq!(zones_for(513), 2);
    assert_eq!(zones_for(1024), 2);
}

#[test]
fn bounds_cost_a_quarter_bit_per_row_for_u64() {
    let mut c = col(DbType::U64);
    let n = 100_000u64;
    for slot in 0..n {
        c.set(slot, Some(&slot.to_le_bytes()));
    }
    let zm = c.zonemap().expect("u64 carries bounds");
    // 2 x 8 bytes per 512-row zone = 0.25 bits/row, plus a validity bit per zone.
    let bits = zm.bytes() as f64 * 8.0 / n as f64;
    assert!(bits < 0.30, "bounds cost {:.4} bits/row", bits);
}

#[test]
fn a_bound_shorter_than_the_prefix_is_not_padded() {
    // Zero-padding a short bound to the full prefix width is the truncation bug
    // wearing different clothes, and it is the one that looks fine: the padded
    // form compares equal on every byte that matters and diverges only on the
    // length tie-break. `simd_greater_or_equal` resolves a tie by `len1 >= len2`,
    // so `"ab" < "ab\0\0"` — a padded `min` sits *above* the value it came from
    // and the zone prunes its own data.
    //
    // Only a value shorter than STRING_BOUND_PREFIX can show it, which is why
    // this is a test of its own rather than a case inside another.
    for short in ["", "a", "ab", "abcdefgh"] {
        assert!(
            short.len() < STRING_BOUND_PREFIX,
            "the fixture must be shorter than the prefix"
        );
        let mut zm = ZoneMap::new(DbType::STRING).unwrap();
        zm.observe(0, short.as_bytes());

        assert!(
            zm.zone_may_match(0, SargOp::Eq, short.as_bytes()),
            "{:?}: the zone lost the only value it holds",
            short
        );
        assert!(
            zm.zone_may_match(0, SargOp::Ge, short.as_bytes()),
            "{:?}: lost on >=",
            short
        );
        assert!(
            zm.zone_may_match(0, SargOp::Le, short.as_bytes()),
            "{:?}: lost on <=",
            short
        );
    }

    // And a zone holding a mix of short and long values keeps all of them.
    let mut zm = ZoneMap::new(DbType::STRING).unwrap();
    let long = "b".repeat(40);
    let vals: Vec<Vec<u8>> = ["", "a", "ab", "b", long.as_str()]
        .iter()
        .map(|v| v.as_bytes().to_vec())
        .collect();
    for v in &vals {
        zm.observe(0, v);
    }
    for v in &vals {
        assert!(zm.zone_may_match(0, SargOp::Eq, v), "lost {:?}", v);
    }
}
