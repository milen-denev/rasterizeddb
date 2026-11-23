//! Sizes and positions where off-by-one errors live: zone edges, word edges,
//! empty and single-row tables, and the first and last slot of everything.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::filter::column::{ColumnFilter, bitmap_words};
use rasterizeddb_core::core::filter::hash::HashSeed;
use rasterizeddb_core::core::filter::sarg;
use rasterizeddb_core::core::filter::zonemap::SargOp;
use rasterizeddb_core::core::filter::{FilterConfig, TableFilters};
use rasterizeddb_core::core::index_store::gate::{Proof, test_mint};
use rasterizeddb_core::core::row::row::RowWrite;
use rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize;
use rasterizeddb_filter_tests::*;
use smallvec::SmallVec;

/// Sizes chosen around the 512-row zone and the 64-slot bitmap word.
const SIZES: &[u64] = &[
    1, 2, 63, 64, 65, 127, 128, 129, 511, 512, 513, 1023, 1024, 1025, 1536, 2048, 2049,
];

fn is_candidate(bm: &[u64], slot: u64) -> bool {
    (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1
}

#[test]
fn every_slot_is_findable_at_every_boundary_size() {
    for &n in SIZES {
        let mut c = ColumnFilter::new(0, DbType::U64, HashSeed::default(), FilterConfig::default());
        for slot in 0..n {
            c.set(slot, Some(&slot.to_le_bytes()));
        }
        let mut bm = vec![0u64; bitmap_words(n)];
        for slot in 0..n {
            c.probe_pred_into(SargOp::Eq, &slot.to_le_bytes(), n, &mut bm);
            assert!(is_candidate(&bm, slot), "n={}: slot {} lost", n, slot);
        }
    }
}

#[test]
fn first_and_last_slot_behave_like_any_other() {
    for &n in SIZES {
        let mut c = ColumnFilter::new(0, DbType::U64, HashSeed::default(), FilterConfig::default());
        for slot in 0..n {
            c.set(slot, Some(&(slot + 1000).to_le_bytes()));
        }
        let mut bm = vec![0u64; bitmap_words(n)];

        for slot in [0u64, n - 1] {
            c.probe_pred_into(SargOp::Eq, &(slot + 1000).to_le_bytes(), n, &mut bm);
            assert!(is_candidate(&bm, slot), "n={}: slot {} lost", n, slot);
        }

        // Deleting the last slot must not disturb the first — except at n = 1,
        // where they are the same slot and losing it is the point.
        c.set(n - 1, None);
        c.probe_pred_into(SargOp::Eq, &(n - 1 + 1000).to_le_bytes(), n, &mut bm);
        assert!(!is_candidate(&bm, n - 1), "n={}: last slot not cleared", n);
        if n > 1 {
            c.probe_pred_into(SargOp::Eq, &1000u64.to_le_bytes(), n, &mut bm);
            assert!(is_candidate(&bm, 0), "n={}: deleting the last slot lost the first", n);
        }
    }
}

#[test]
fn bitmaps_are_sized_and_masked_correctly() {
    for &n in SIZES {
        assert_eq!(bitmap_words(n), ((n + 63) / 64) as usize, "n={}", n);
        let mut c = ColumnFilter::new(0, DbType::U64, HashSeed::default(), FilterConfig::default());
        for slot in 0..n {
            c.set(slot, Some(&slot.to_le_bytes()));
        }
        let mut bm = vec![0u64; bitmap_words(n)];
        c.probe_pred_into(SargOp::Eq, &0u64.to_le_bytes(), n, &mut bm);
        let live: u64 = bm.iter().map(|w| w.count_ones() as u64).sum();
        assert!(live <= n, "n={}: {} bits set, more than there are slots", n, live);
    }
}

#[test]
fn ranges_are_sound_at_every_boundary_size() {
    for &n in SIZES {
        let mut c = ColumnFilter::new(0, DbType::I64, HashSeed::default(), FilterConfig::default());
        for slot in 0..n {
            c.set(slot, Some(&(slot as i64).to_le_bytes()));
        }
        let mut bm = vec![0u64; bitmap_words(n)];
        for pivot in [0i64, 1, (n as i64) / 2, n as i64 - 2, n as i64 - 1, n as i64] {
            c.probe_pred_into(SargOp::Gt, &pivot.to_le_bytes(), n, &mut bm);
            for slot in 0..n {
                if (slot as i64) > pivot {
                    assert!(is_candidate(&bm, slot), "n={} pivot={}: slot {} lost", n, pivot, slot);
                }
            }
        }
    }
}

#[test]
fn a_zone_straddling_the_frontier_keeps_its_tail_as_candidates() {
    // The frontier rarely lands on a zone boundary, so the partial zone has to
    // hand back the slots it cannot vouch for.
    for filled in [1u64, 100, 511, 512, 513, 600] {
        let mut c = ColumnFilter::new(0, DbType::U64, HashSeed::default(), FilterConfig::default());
        for slot in 0..filled {
            c.set(slot, Some(&slot.to_le_bytes()));
        }
        let n = 1024u64;
        let mut bm = vec![0u64; bitmap_words(n)];
        c.probe_pred_into(SargOp::Eq, &999_999u64.to_le_bytes(), n, &mut bm);
        for slot in filled..n {
            assert!(
                is_candidate(&bm, slot),
                "filled={}: slot {} above the frontier was pruned",
                filled,
                slot
            );
        }
    }
}

#[test]
fn an_unindexed_prefix_at_any_offset_stays_candidate() {
    for prefix in [1u64, 63, 64, 511, 512, 513, 700] {
        let mut c = ColumnFilter::new(0, DbType::U64, HashSeed::default(), FilterConfig::default());
        c.set_unindexed_prefix(prefix);
        let n = 1500u64;
        for slot in prefix..n {
            c.set(slot, Some(&slot.to_le_bytes()));
        }
        let mut bm = vec![0u64; bitmap_words(n)];
        c.probe_pred_into(SargOp::Eq, &999_999u64.to_le_bytes(), n, &mut bm);
        for slot in 0..prefix {
            assert!(is_candidate(&bm, slot), "prefix={}: slot {} pruned", prefix, slot);
        }
    }
}

#[test]
fn checkpoints_round_trip_at_every_boundary_size() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    for &n in SIZES {
        let fields = fields_of(&[("id", DbType::U64)]);
        let f = TableFilters::new(&fields, FilterConfig::default());
        let rows: Vec<Vec<(u32, Vec<u8>)>> =
            (0..n as usize).map(|i| vec![(0u32, (i as u64).to_le_bytes().to_vec())]).collect();
        backfill_all(proof, &f, &rows);

        let bytes = f.checkpoint_bytes(n);
        let restored =
            TableFilters::from_checkpoint(&bytes, &fields, FilterConfig::default(), n)
                .unwrap_or_else(|e| panic!("n={}: {:?}", n, e));

        for slot in 0..n {
            let q = format!("id = {}", slot);
            let p = sarg::extract(&tokenize(&q, &fields), &restored).expect("sargable");
            let bm = restored.evaluate(&p, n).expect("bitmap");
            assert!(bm.get(slot), "n={}: slot {} lost across checkpoint", n, slot);
        }
    }
}

#[test]
fn an_empty_catalog_and_zero_slots_are_handled() {
    let fields = fields_of(&[("id", DbType::U64)]);
    let f = TableFilters::new(&fields, FilterConfig::default());
    let p = sarg::extract(&tokenize("id = 1", &fields), &f).expect("sargable");
    // Zero slots: nothing to prune, so no bitmap.
    assert!(f.evaluate(&p, 0).is_none());
    assert_eq!(bitmap_words(0), 0);
}

#[test]
fn a_table_with_no_indexable_columns_is_inert() {
    // `BYTEA` and `ARRAY` are the two column types the evaluator cannot order or
    // compare, so a catalog built from them holds nothing.
    let fields = fields_of(&[("blob", DbType::BYTEA), ("tags", DbType::ARRAY)]);
    let f = TableFilters::new(&fields, FilterConfig::default());
    assert!(f.is_empty());
    assert!(sarg::extract(&tokenize("blob = 1", &fields), &f).is_none());
}

#[test]
fn a_bool_column_is_indexed_for_its_bounds_alone() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    // BOOL earns no fingerprints — two distinct values can never clear the
    // selectivity gate — but two bytes per 512-row zone is a fiftieth of that
    // cost and prunes a *sparse* flag almost perfectly. That case arrives on
    // every nullable column, since `x IS NULL` lowers to `__null_x = 1`.
    let fields = fields_of(&[("flag", DbType::BOOL)]);
    let f = TableFilters::new(&fields, FilterConfig::default());
    assert!(!f.is_empty(), "BOOL is indexed, for its bounds");
    assert_eq!(f.stats()[0].1, 0, "with no fingerprint planes: f = 0");

    // One `1` every 1000 rows, so most 512-row zones hold none at all.
    let n = 4096u64;
    let rows: Vec<Vec<(u32, Vec<u8>)>> = (0..n as usize)
        .map(|i| vec![(0u32, vec![u8::from(i % 1000 == 0)])])
        .collect();
    backfill_all(proof, &f, &rows);

    let p = sarg::extract(&tokenize("flag = 1", &fields), &f).expect("BOOL = 1 is sargable");
    let bm = f.evaluate(&p, n).expect("a sparse flag should prune");

    // No row that matches may be pruned — the whole point of the structure.
    for slot in (0..n).filter(|i| i % 1000 == 0) {
        assert!(bm.get(slot), "slot {} pruned despite holding the flag", slot);
    }
    // And the zones that hold no `1` at all are gone.
    assert!(
        bm.count() <= 5 * 512,
        "expected all-zero zones pruned, {} of {} survived",
        bm.count(),
        n
    );
}

#[test]
fn many_columns_are_all_independently_indexed() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let spec: Vec<(String, DbType)> = (0..20)
        .map(|i| (format!("c{}", i), if i % 2 == 0 { DbType::U64 } else { DbType::I32 }))
        .collect();
    let refs: Vec<(&str, DbType)> =
        spec.iter().map(|(n, t)| (n.as_str(), t.clone())).collect();
    let (fields, f) = filters_for(&refs);

    let n = 2000u64;
    let rows: Vec<Vec<(u32, Vec<u8>)>> = (0..n as usize)
        .map(|i| {
            (0..20u32)
                .map(|c| {
                    let v = (i as u64) * 20 + c as u64;
                    if c % 2 == 0 {
                        (c, v.to_le_bytes().to_vec())
                    } else {
                        (c, (v as i32).to_le_bytes().to_vec())
                    }
                })
                .collect()
        })
        .collect();
    backfill_all(proof, &f, &rows);

    for c in 0..20u32 {
        assert!(f.indexes(c), "column {} not indexed", c);
        let slot = 777usize;
        let v = (slot as u64) * 20 + c as u64;
        let q = if c % 2 == 0 {
            format!("c{} = {}", c, v)
        } else {
            format!("c{} = {}", c, v as i32)
        };
        let p = sarg::extract(&tokenize(&q, &fields), &f)
            .unwrap_or_else(|| panic!("`{}` should be sargable", q));
        let bm = f.evaluate(&p, n).expect("bitmap");
        assert!(bm.get(slot as u64), "column {}: slot {} lost", c, slot);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn tables_at_boundary_sizes_query_correctly() {
    for &n in &[1u64, 511, 512, 513, 1024] {
        let dir = tempfile::tempdir().unwrap();
        let cols = vec![("id", DbType::U64)];
        let t = open_table(dir.path(), &format!("bnd_{}", n), &cols).await;
        for i in 0..n {
            let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
            rw.columns_writing_data.push(payload(&i.to_le_bytes(), 0, DbType::U64));
            t.insert_row(rw).await.unwrap();
        }
        assert_eq!(run(&t, "").await.len() as u64, n, "n={}", n);
        for target in [0u64, n / 2, n - 1] {
            assert_eq!(
                u64s(&run(&t, &format!("id = {}", target)).await, 0),
                vec![target],
                "n={} target={}",
                n,
                target
            );
        }
        assert!(run(&t, &format!("id = {}", n)).await.is_empty(), "n={}", n);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_table_answers_every_predicate_shape() {
    let dir = tempfile::tempdir().unwrap();
    let cols = vec![("id", DbType::U64), ("name", DbType::STRING)];
    let t = open_table(dir.path(), "empty_tbl", &cols).await;

    for q in ["id = 1", "id > 1", "id < 1", "name = 'x'", "id = 1 OR id = 2", ""] {
        assert!(run(&t, q).await.is_empty(), "`{}` on an empty table", q);
    }
}
