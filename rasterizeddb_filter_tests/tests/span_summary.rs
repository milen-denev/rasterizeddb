//! The L0/L1 span summary: one split-block Bloom per 16 384-row superzone.
//!
//! Two things to hold it to. It must never lose a row — it sits in front of the
//! planes and a wrong "absent" is a false negative, the failure the whole crate
//! exists to avoid. And it must actually rule superzones out, or it is 0.25
//! bits/row of nothing.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::filter::bloom::{self, SuperBloom};
use rasterizeddb_core::core::filter::column::{ColumnFilter, bitmap_words};
use rasterizeddb_core::core::filter::hash::{HashSeed, canonical_hash};
use rasterizeddb_core::core::filter::sarg;
use rasterizeddb_core::core::filter::settings::settings;
use rasterizeddb_core::core::filter::zonemap::SargOp;
use rasterizeddb_core::core::filter::{FilterConfig, TableFilters};
use rasterizeddb_core::core::index_store::gate::{Proof, test_mint};
use rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize;
use rasterizeddb_filter_tests::*;

/// Settings are process-wide, so the tests that touch them must not interleave.
/// Same shape as the `tuning` suite's guard, and separate because each test
/// binary is its own process.
static SETTINGS_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct Guard(Option<std::sync::MutexGuard<'static, ()>>);

impl Drop for Guard {
    fn drop(&mut self) {
        settings().reset_all();
        self.0.take();
    }
}

#[must_use]
fn fresh() -> Guard {
    let g = SETTINGS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    init();
    settings().reset_all();
    Guard(Some(g))
}

fn run_sql(s: &str) -> Result<Vec<String>, String> {
    rasterizeddb_core::core::filter::sql::execute(s)
}

fn summarised(ty: DbType) -> ColumnFilter {
    ColumnFilter::with_options(0, ty, HashSeed::default(), FilterConfig::default(), true, true)
}

fn is_candidate(bm: &[u64], slot: u64) -> bool {
    (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1
}

#[test]
fn a_summary_is_opt_in() {
    let plain = ColumnFilter::new(0, DbType::U64, HashSeed::default(), FilterConfig::default());
    assert!(!plain.has_span_summary(), "off unless asked for");
    assert!(summarised(DbType::U64).has_span_summary());
}

#[test]
fn a_bounds_only_column_gets_no_summary() {
    // BOOL is never hashed, so there is no key to record and a summary would
    // summarise nothing.
    let c = summarised(DbType::BOOL);
    assert!(!c.has_fingerprints());
    assert!(!c.has_span_summary());
}

#[test]
fn no_false_negatives_under_random_data() {
    let mut c = summarised(DbType::U64);
    let n = 60_000u64;
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
fn deletes_and_updates_stay_sound() {
    // A Bloom cannot forget. A deleted key stays recorded, which makes the
    // answer more conservative; an updated row's *new* value has to be recorded
    // or the row would go missing.
    let mut c = summarised(DbType::U64);
    let n = 40_000u64;
    for slot in 0..n {
        c.set(slot, Some(&slot.to_le_bytes()));
    }
    for slot in (0..n).step_by(7) {
        c.set(slot, None);
    }
    for slot in (0..n).step_by(11) {
        c.set(slot, Some(&(slot + 1_000_000).to_le_bytes()));
    }

    let mut bm = vec![0u64; bitmap_words(n)];
    for slot in 0..n {
        let updated = slot % 11 == 0;
        let deleted = slot % 7 == 0 && !updated;
        if deleted {
            continue;
        }
        let v = if updated { slot + 1_000_000 } else { slot };
        c.probe_pred_into(SargOp::Eq, &v.to_le_bytes(), n, &mut bm);
        assert!(is_candidate(&bm, slot), "slot {} lost (updated={})", slot, updated);
    }
}

#[test]
fn out_of_order_writes_stay_sound() {
    let mut c = summarised(DbType::U64);
    let n = 40_000u64;
    let mut order: Vec<u64> = (0..n).collect();
    fastrand::shuffle(&mut order);
    for slot in order {
        c.set(slot, Some(&slot.to_le_bytes()));
    }
    let mut bm = vec![0u64; bitmap_words(n)];
    for slot in 0..n {
        c.probe_pred_into(SargOp::Eq, &slot.to_le_bytes(), n, &mut bm);
        assert!(is_candidate(&bm, slot), "slot {} lost", slot);
    }
}

#[test]
fn an_absent_value_is_ruled_out_of_almost_every_superzone() {
    // The payoff, measured where the arithmetic says it should exist: a few
    // hundred distinct values per 16 384-row span. Each superzone the summary
    // rejects is `f` cache lines of planes never read.
    let mut c = summarised(DbType::U64);
    let n = 16u64 * bloom::SUPERZONE_ROWS;
    for slot in 0..n {
        // 250 distinct values per superzone.
        let sz = slot / bloom::SUPERZONE_ROWS;
        c.set(slot, Some(&(sz * 1_000_000 + slot % 250).to_le_bytes()));
    }

    let summary = c.span_summary().expect("summary present");
    let seed = HashSeed::default();
    let superzones = n / bloom::SUPERZONE_ROWS;
    let mut rejected = 0u64;
    let trials = 400u64;
    for i in 0..trials {
        let absent = 900_000_000u64 + i;
        let h = canonical_hash(&DbType::U64, &absent.to_le_bytes(), seed).unwrap();
        rejected += (0..superzones).filter(|sz| !summary.may_contain(*sz, h)).count() as u64;
    }
    let rate = rejected as f64 / (trials * superzones) as f64;
    assert!(rate > 0.95, "only {:.1}% of superzones ruled out", rate * 100.0);
}

#[test]
fn a_saturated_column_still_answers_correctly() {
    // The other end of the arithmetic: every row distinct, so every block fills
    // and the summary can rule nothing out. It must cost selectivity only.
    let mut c = summarised(DbType::U64);
    let n = 4u64 * bloom::SUPERZONE_ROWS;
    for slot in 0..n {
        c.set(slot, Some(&slot.to_le_bytes()));
    }
    let mut bm = vec![0u64; bitmap_words(n)];
    for slot in (0..n).step_by(97) {
        c.probe_pred_into(SargOp::Eq, &slot.to_le_bytes(), n, &mut bm);
        assert!(is_candidate(&bm, slot), "slot {} lost", slot);
    }
}

#[test]
fn the_summary_is_reachable_through_settings() {
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    let _g = fresh();
    let fields = fields_of(&[("id", DbType::U64), ("cat", DbType::I32)]);

    let off = TableFilters::for_table("sumtbl", &fields);
    run_sql("SET FILTER bloom = on FOR COLUMN sumtbl.id").unwrap();
    let on = TableFilters::for_table("sumtbl", &fields);

    let rows: Vec<Vec<(u32, Vec<u8>)>> = (0..40_000usize)
        .map(|i| {
            vec![
                (0u32, ((i % 300) as u64).to_le_bytes().to_vec()),
                (1u32, ((i % 97) as i32).to_le_bytes().to_vec()),
            ]
        })
        .collect();
    backfill_all(proof, &on, &rows);
    backfill_all(proof, &off, &rows);

    assert!(on.bytes() > off.bytes(), "the summary should cost storage");

    // And it must not change any answer.
    let n = rows.len() as u64;
    for v in [0u64, 7, 299] {
        let toks = tokenize(&format!("id = {}", v), &fields);
        let a = sarg::extract(&toks, &on).unwrap();
        let b = sarg::extract(&toks, &off).unwrap();
        let ba = on.evaluate(&a, n).unwrap();
        let bb = off.evaluate(&b, n).unwrap();
        for slot in (0..n).filter(|s| (*s as usize % 300) as u64 == v) {
            assert!(ba.get(slot), "summary lost slot {}", slot);
            assert!(bb.get(slot));
        }
    }
}

#[test]
fn the_summary_survives_a_checkpoint() {
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    let _g = fresh();
    let fields = fields_of(&[("id", DbType::U64)]);
    run_sql("SET FILTER bloom = on FOR COLUMN ckpt_sum.id").unwrap();
    let f = TableFilters::for_table("ckpt_sum", &fields);

    let n = 40_000u64;
    let rows: Vec<Vec<(u32, Vec<u8>)>> = (0..n as usize)
        .map(|i| vec![(0u32, ((i % 250) as u64).to_le_bytes().to_vec())])
        .collect();
    backfill_all(proof, &f, &rows);

    let bytes = f.checkpoint_bytes(n);
    let restored = TableFilters::from_checkpoint(&bytes, &fields, FilterConfig::default(), n)
        .expect("round trip");

    for v in [0u64, 1, 249] {
        let toks = tokenize(&format!("id = {}", v), &fields);
        let p = sarg::extract(&toks, &restored).unwrap();
        let bm = restored.evaluate(&p, n).unwrap();
        for slot in (0..n).filter(|s| (*s as usize % 250) as u64 == v) {
            assert!(bm.get(slot), "slot {} lost across the checkpoint", slot);
        }
    }

    // The restored copy must still be able to rule things out, or the summary
    // came back empty and the round trip only looked like it worked.
    let toks = tokenize("id = 999999", &fields);
    let p = sarg::extract(&toks, &restored).unwrap();
    assert_eq!(restored.evaluate(&p, n).unwrap().count(), 0);
}

#[test]
fn a_summary_block_never_answers_no_for_a_key_it_holds() {
    // Restated directly against the structure, at the size it is actually used
    // at, because everything above depends on it.
    let mut b = SuperBloom::new();
    for sz in 0..4u64 {
        for i in 0..4000u64 {
            b.add(sz, fastrand::u64(..).wrapping_add(i));
        }
    }
    let keys: Vec<(u64, u64)> =
        (0..5000).map(|i| ((i % 4) as u64, fastrand::u64(..))).collect();
    for (sz, k) in &keys {
        b.add(*sz, *k);
    }
    for (sz, k) in &keys {
        assert!(b.may_contain(*sz, *k), "superzone {} lost key {}", sz, k);
    }
}
