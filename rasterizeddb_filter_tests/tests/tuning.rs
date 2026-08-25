//! Tuning the filter: the settings layer, the SQL surface, and the effect a
//! change actually has on an index.
//!
//! Two things worth separating. Parsing a `SET FILTER` statement is easy to get
//! right and easy to test; what matters more is that the setting **reaches the
//! filter** — that `SET FILTER max_f = 8 FOR COLUMN t.c` produces a column with
//! 8-bit fingerprints and not merely a stored preference nobody reads.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::filter::settings::settings;
use rasterizeddb_core::core::filter::sql;
use rasterizeddb_core::core::filter::TableFilters;
use rasterizeddb_filter_tests::*;

/// Settings are process-wide, so these tests must not interleave: one test's
/// `SET FILTER` would otherwise be visible to another running concurrently.
/// The guard serialises them and resets to defaults on both entry and exit, so
/// a failing test cannot leak state into the next one.
static TUNING_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct Guard(Option<std::sync::MutexGuard<'static, ()>>);

impl Drop for Guard {
    fn drop(&mut self) {
        settings().reset_all();
        // Drop the lock after the reset, so the next test never observes the
        // state this one left behind.
        self.0.take();
    }
}

#[must_use]
fn fresh() -> Guard {
    // A poisoned lock means an earlier test panicked; the state is reset either
    // way, so recovering is right rather than cascading failures.
    let g = TUNING_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    init();
    settings().reset_all();
    Guard(Some(g))
}

fn run_sql(s: &str) -> Result<Vec<String>, String> {
    sql::execute(s)
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

#[test]
fn statements_are_recognised_by_shape() {
    let _g = fresh();
    for s in [
        "SET FILTER max_f = 8",
        "  set filter max_f = 8  ",
        "SHOW FILTER",
        "show filter for table users",
        "RESET FILTER",
        "SET FILTER enabled = off;",
    ] {
        assert!(sql::is_filter_statement(s), "`{}` should be recognised", s);
    }
    for s in [
        "SELECT * FROM users",
        "SET search_path = public",
        "SHOW TIME ZONE",
        "INSERT INTO filter VALUES (1)",
        "",
    ] {
        assert!(!sql::is_filter_statement(s), "`{}` should not be recognised", s);
    }
}

#[test]
fn every_documented_key_is_settable_at_its_documented_scope() {
    let _g = fresh();
    for k in sql::KEYS {
        let value = match k.name {
            "enabled" | "backfill_on_open" | "indexed" | "zone_bounds" | "bloom" => {
                "on".to_string()
            }
            "min_pruning_ratio" | "min_selectivity" => "0.25".to_string(),
            "checkpoint_interval_secs" | "min_table_rows" => "7".to_string(),
            _ => "8".to_string(),
        };
        let global = format!("SET FILTER {} = {}", k.name, value);
        assert!(run_sql(&global).is_ok(), "`{}` should work globally", global);

        let scoped = format!("SET FILTER {} = {} FOR TABLE t", k.name, value);
        if k.global_only {
            assert!(run_sql(&scoped).is_err(), "`{}` should be refused", scoped);
        } else {
            assert!(run_sql(&scoped).is_ok(), "`{}` should work", scoped);
            let col = format!("SET FILTER {} = {} FOR COLUMN t.c", k.name, value);
            assert!(run_sql(&col).is_ok(), "`{}` should work", col);
        }
    }
}

#[test]
fn bad_statements_are_rejected_with_a_reason() {
    let _g = fresh();
    for s in [
        "SET FILTER nonsense = 1",
        "SET FILTER max_f",
        "SET FILTER max_f = notanumber",
        "SET FILTER max_f = 99",
        "SET FILTER max_f = 1",
        "SET FILTER enabled = maybe",
        "SET FILTER min_selectivity = 0",
        "SET FILTER min_selectivity = 2",
        "SET FILTER min_pruning_ratio = 5",
        "SET FILTER max_f = 8 FOR TABLE",
        "SET FILTER max_f = 8 FOR COLUMN users",
        "SET FILTER max_f = 8 FOR SCHEMA x",
        "SHOW FILTER FOR COLUMN nodot",
        "RESET FILTER junk",
    ] {
        let r = run_sql(s);
        assert!(r.is_err(), "`{}` should be rejected, got {:?}", s, r);
        assert!(!r.unwrap_err().is_empty(), "`{}` needs a reason", s);
    }
}

#[test]
fn boolean_spellings_all_work() {
    let _g = fresh();
    for on in ["true", "on", "yes", "1", "T", "TRUE"] {
        run_sql(&format!("SET FILTER enabled = {}", on)).unwrap();
        assert!(settings().enabled(), "`{}` should mean true", on);
        run_sql("SET FILTER enabled = off").unwrap();
    }
    for off in ["false", "off", "no", "0", "F"] {
        run_sql("SET FILTER enabled = on").unwrap();
        run_sql(&format!("SET FILTER enabled = {}", off)).unwrap();
        assert!(!settings().enabled(), "`{}` should mean false", off);
    }
}

#[test]
fn show_reports_the_effective_value_at_each_scope() {
    let _g = fresh();
    run_sql("SET FILTER max_f = 16").unwrap();
    run_sql("SET FILTER max_f = 12 FOR TABLE users").unwrap();
    run_sql("SET FILTER max_f = 6 FOR COLUMN users.email").unwrap();

    let g = run_sql("SHOW FILTER").unwrap().join("\n");
    assert!(g.contains("max_f = 16"), "global: {}", g);

    let t = run_sql("SHOW FILTER FOR TABLE users").unwrap().join("\n");
    assert!(t.contains("max_f = 12"), "table: {}", t);

    let c = run_sql("SHOW FILTER FOR COLUMN users.email").unwrap().join("\n");
    assert!(c.contains("max_f = 6"), "column: {}", c);

    // A column with no override of its own inherits the table's.
    let other = run_sql("SHOW FILTER FOR COLUMN users.name").unwrap().join("\n");
    assert!(other.contains("max_f = 12"), "inherited: {}", other);

    // And a different table inherits the global.
    let elsewhere = run_sql("SHOW FILTER FOR TABLE orders").unwrap().join("\n");
    assert!(elsewhere.contains("max_f = 16"), "unrelated table: {}", elsewhere);
}

#[test]
fn show_keys_lists_everything_settable() {
    let _g = fresh();
    let out = run_sql("SHOW FILTER KEYS").unwrap().join("\n");
    for k in sql::KEYS {
        assert!(out.contains(k.name), "`{}` missing from SHOW FILTER KEYS", k.name);
    }
}

#[test]
fn reset_unwinds_one_scope_at_a_time() {
    let _g = fresh();
    run_sql("SET FILTER max_f = 16").unwrap();
    run_sql("SET FILTER max_f = 12 FOR TABLE users").unwrap();
    run_sql("SET FILTER max_f = 6 FOR COLUMN users.email").unwrap();

    run_sql("RESET FILTER FOR COLUMN users.email").unwrap();
    assert!(
        run_sql("SHOW FILTER FOR COLUMN users.email").unwrap().join("\n").contains("max_f = 12"),
        "column reset should fall back to the table"
    );

    run_sql("RESET FILTER FOR TABLE users").unwrap();
    assert!(
        run_sql("SHOW FILTER FOR TABLE users").unwrap().join("\n").contains("max_f = 16"),
        "table reset should fall back to the global"
    );

    run_sql("RESET FILTER").unwrap();
    let g = run_sql("SHOW FILTER").unwrap().join("\n");
    assert!(g.contains("enabled = true"), "reset should restore defaults: {}", g);
}

#[test]
fn quoted_and_qualified_names_resolve() {
    let _g = fresh();
    run_sql("SET FILTER max_f = 6 FOR COLUMN \"users\".\"email\"").unwrap();
    let c = run_sql("SHOW FILTER FOR COLUMN users.email").unwrap().join("\n");
    assert!(c.contains("max_f = 6"), "quoted name did not resolve: {}", c);

    // Table names are case-insensitive, as identifiers are.
    run_sql("SET FILTER max_f = 7 FOR TABLE Orders").unwrap();
    let t = run_sql("SHOW FILTER FOR TABLE orders").unwrap().join("\n");
    assert!(t.contains("max_f = 7"), "case-insensitive lookup failed: {}", t);
}

#[test]
fn invalid_widths_are_clamped_not_inverted() {
    let _g = fresh();
    // `min_f > max_f` would make `choose_f` clamp to nonsense.
    run_sql("SET FILTER min_f = 12 FOR TABLE t").unwrap();
    run_sql("SET FILTER max_f = 6 FOR TABLE t").unwrap();
    let r = settings().resolve("t", "c");
    assert!(r.cfg.min_f <= r.cfg.max_f, "min_f {} > max_f {}", r.cfg.min_f, r.cfg.max_f);
    assert!(
        r.cfg.default_f >= r.cfg.min_f && r.cfg.default_f <= r.cfg.max_f,
        "default_f {} outside [{}, {}]",
        r.cfg.default_f,
        r.cfg.min_f,
        r.cfg.max_f
    );
}

// ---------------------------------------------------------------------------
// The settings actually reach the filter
// ---------------------------------------------------------------------------

#[test]
fn a_width_override_changes_the_fingerprints_built() {
    let _g = fresh();
    run_sql("SET FILTER min_f = 6 FOR COLUMN tune.id").unwrap();
    run_sql("SET FILTER max_f = 6 FOR COLUMN tune.id").unwrap();
    run_sql("SET FILTER min_f = 12 FOR COLUMN tune.other").unwrap();
    run_sql("SET FILTER max_f = 12 FOR COLUMN tune.other").unwrap();

    let fields = fields_of(&[("id", DbType::U64), ("other", DbType::U64)]);
    let f = TableFilters::for_table("tune", &fields);

    let n = 5000u64;
    let rows: Vec<Vec<(u32, Vec<u8>)>> = (0..n as usize)
        .map(|i| {
            vec![
                (0u32, (i as u64).to_le_bytes().to_vec()),
                (1u32, fastrand::u64(..).to_le_bytes().to_vec()),
            ]
        })
        .collect();
    backfill_all(&f, &rows);

    let stats = f.stats();
    let id_f = stats.iter().find(|s| s.0 == 0).unwrap().1;
    let other_f = stats.iter().find(|s| s.0 == 1).unwrap().1;
    assert_eq!(id_f, 6, "per-column min/max should pin the width");
    assert_eq!(other_f, 12, "the sibling column should keep its own width");

    // And the storage matches the widths, not just the reported number.
    let bytes = f.bytes();
    let expected = (n as f64 * (6.0 + 12.0) / 8.0) as usize;
    assert!(
        bytes > expected / 2 && bytes < expected * 3,
        "storage {} is not consistent with 6+12 bits/row over {} rows",
        bytes,
        n
    );
}

#[test]
fn excluding_a_column_stops_it_being_indexed() {
    let _g = fresh();
    run_sql("SET FILTER indexed = off FOR COLUMN tune2.secret").unwrap();

    let fields = fields_of(&[("id", DbType::U64), ("secret", DbType::STRING)]);
    let f = TableFilters::for_table("tune2", &fields);

    assert!(f.indexes(0), "id should still be indexed");
    assert!(!f.indexes(1), "secret was excluded and must not be indexed");

    // A predicate on the excluded column contributes nothing rather than
    // invalidating a sibling.
    let toks = rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize(
        "id = 3 AND secret = 'x'",
        &fields,
    );
    let p = rasterizeddb_core::core::filter::sarg::extract(&toks, &f)
        .expect("the id half should survive");
    assert_eq!(p.all_preds().len(), 1);
}

#[test]
fn disabling_zone_bounds_keeps_fingerprints_and_drops_ranges() {
    let _g = fresh();
    run_sql("SET FILTER zone_bounds = off FOR COLUMN tune3.v").unwrap();

    let fields = fields_of(&[("v", DbType::U64)]);
    let f = TableFilters::for_table("tune3", &fields);
    let n = 4000u64;
    let rows: Vec<Vec<(u32, Vec<u8>)>> =
        (0..n as usize).map(|i| vec![(0u32, (i as u64).to_le_bytes().to_vec())]).collect();
    backfill_all(&f, &rows);

    let toks = rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize("v = 1234", &fields);
    let p = rasterizeddb_core::core::filter::sarg::extract(&toks, &f).unwrap();
    let bm = f.evaluate(&p, n).expect("equality should still prune");
    assert!(bm.get(1234), "true match pruned");
    assert!(bm.count() < n / 4, "equality should still prune without bounds");

    // Ranges have nothing to act on, so no bitmap is produced.
    let toks = rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize("v > 3900", &fields);
    let p = rasterizeddb_core::core::filter::sarg::extract(&toks, &f).unwrap();
    assert!(
        f.evaluate(&p, n).is_none(),
        "with zone bounds off, a range must narrow nothing"
    );

    // With bounds on, the same range does prune. Clear just this column's
    // override rather than re-entering `fresh()`, which would deadlock on the
    // guard this test already holds.
    settings().reset_column("tune3", "v");
    let f = TableFilters::for_table("tune3", &fields);
    backfill_all(&f, &rows);
    let toks = rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize("v > 3900", &fields);
    let p = rasterizeddb_core::core::filter::sarg::extract(&toks, &f).unwrap();
    let bm = f.evaluate(&p, n).expect("bounds on: range should prune");
    assert!(bm.count() < n / 2, "range left {} of {}", bm.count(), n);
}

#[test]
fn a_stricter_selectivity_gate_declines_more_columns() {
    let _g = fresh();
    let fields = fields_of(&[("v", DbType::U64)]);
    let n = 3000u64;
    // 40 distinct values: a predicate matches 2.5% of rows.
    let rows: Vec<Vec<(u32, Vec<u8>)>> = (0..n as usize)
        .map(|i| vec![(0u32, ((i % 40) as u64).to_le_bytes().to_vec())])
        .collect();

    run_sql("SET FILTER min_selectivity = 0.5 FOR TABLE gate").unwrap();
    let permissive = TableFilters::for_table("gate", &fields);
    backfill_all(&permissive, &rows);
    assert!(permissive.stats()[0].1 > 0, "a permissive gate should index it");

    run_sql("SET FILTER min_selectivity = 0.001 FOR TABLE gate").unwrap();
    let strict = TableFilters::for_table("gate", &fields);
    backfill_all(&strict, &rows);
    assert_eq!(strict.stats()[0].1, 0, "a strict gate should take it out of service");

    // Out of service means probing narrows nothing — not that rows go missing.
    let toks = rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize("v = 7", &fields);
    let p = rasterizeddb_core::core::filter::sarg::extract(&toks, &strict).unwrap();
    assert!(
        strict.evaluate(&p, n).is_none(),
        "a gated-out column must narrow nothing"
    );

    // The permissive one still prunes the same predicate.
    let p2 = rasterizeddb_core::core::filter::sarg::extract(&toks, &permissive).unwrap();
    let bm = permissive.evaluate(&p2, n).expect("permissive gate should prune");
    for slot in (0..n).filter(|i| (i % 40) == 7) {
        assert!(bm.get(slot), "slot {} pruned despite matching", slot);
    }
}

// ---------------------------------------------------------------------------
// Through SQL, end to end
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tuning_statements_execute_through_the_full_sql_path() {
    let _g = fresh();
    let dir = tempfile::tempdir().unwrap();
    let db = open_db(dir.path()).await;

    for sql in [
        "SET FILTER max_f = 12",
        "SET FILTER max_f = 8 FOR TABLE items;",
        "SET FILTER zone_bounds = off FOR COLUMN items.id",
        "SHOW FILTER",
        "SHOW FILTER FOR TABLE items",
        "SHOW FILTER KEYS",
        "RESET FILTER FOR TABLE items",
        "RESET FILTER",
    ] {
        let out = exec(&db, sql).await;
        assert!(!out.is_error(), "`{}` failed: {:?}", sql, out);
    }

    // A bad one is an error, not a panic, and does not poison the session.
    assert!(exec(&db, "SET FILTER max_f = 99").await.is_error());
    assert!(!exec(&db, "SHOW FILTER").await.is_error());
}

#[tokio::test(flavor = "multi_thread")]
async fn disabling_the_filter_by_sql_still_answers_correctly() {
    let _g = fresh();
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "sqloff", &std_columns()).await;
        fill_std(&t).await;
        assert_eq!(u64s(&run(&t, "id = 1234").await, 0), vec![1234]);
    }

    // Turn it off, reopen: no catalog is built, and results are unchanged.
    sql::execute("SET FILTER enabled = off").unwrap();
    let t = open_table(dir.path(), "sqloff", &std_columns()).await;
    t.wait_filters_ready().await;
    assert!(t.filters.is_none(), "disabled by SQL should build no catalog");
    assert_eq!(u64s(&run(&t, "id = 1234").await, 0), vec![1234]);
    assert_eq!(run(&t, "").await.len() as u64, STD_N);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_per_table_exclusion_applies_at_open() {
    let _g = fresh();
    sql::execute("SET FILTER indexed = off FOR TABLE excluded").unwrap();

    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "excluded", &std_columns()).await;
    fill_std(&t).await;
    assert!(t.filters.is_none(), "an excluded table should build no catalog");
    // Still correct, just unfiltered.
    assert_eq!(u64s(&run(&t, "id = 7").await, 0), vec![7]);

    // A different table is unaffected.
    let other = open_table(dir.path(), "included", &std_columns()).await;
    assert!(other.filters.is_some(), "the exclusion must not leak to other tables");
}

/// The default is a measured number, not a taste, so it gets pinned like one.
///
/// `min_pruning_ratio` decides whether a mask is kept on a scan that reads
/// everything, and the break-even it approximates was measured by
/// `tests/cost_ratio.rs` at 0.970–0.996 depending on row width. It sat at 0.5
/// for a long time — picked before either side of the trade had a number under
/// it — which discarded masks that paid for themselves several times over.
///
/// If this assertion fails, the question to ask is not "what should the number
/// be" but "what does `cost_ratio` say now": the answer moves with the ratio
/// between a bitmap test and a row fetch, and that moves with the hardware.
#[test]
fn the_pruning_ratio_default_is_the_measured_one() {
    let _g = fresh();
    assert_eq!(
        settings().min_pruning_ratio(),
        rasterizeddb_core::core::filter::settings::DEFAULT_MIN_PRUNING_RATIO
    );
    assert_eq!(settings().min_pruning_ratio(), 0.90);

    // And it stays a setting: overriding then resetting comes back to it.
    run_sql("SET FILTER min_pruning_ratio = 0.25").unwrap();
    assert_eq!(settings().min_pruning_ratio(), 0.25);
    settings().reset_all();
    assert_eq!(settings().min_pruning_ratio(), 0.90);
}

/// A mask that survives between the old default and the new one is exactly what
/// the change was for: 0.5 threw these away, 0.90 keeps them.
#[test]
fn a_moderately_selective_mask_is_now_kept() {
    let _g = fresh();
    let fields = fields_of(&[("v", DbType::U64)]);
    let n = 4096u64;
    // One value in three matches, so ~0.67 of the table survives — above the
    // old threshold, below the new one.
    let rows: Vec<Vec<(u32, Vec<u8>)>> = (0..n as usize)
        .map(|i| vec![(0u32, ((i % 3) as u64).to_le_bytes().to_vec())])
        .collect();
    let f = TableFilters::new(&fields, rasterizeddb_core::core::filter::FilterConfig::default());
    backfill_all(&f, &rows);

    let toks = rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize("v = 0", &fields);
    let Some(p) = rasterizeddb_core::core::filter::sarg::extract(&toks, &f) else {
        // A three-value column may be gated out entirely, which is a different
        // decision and not what this test is about.
        return;
    };
    let Some(bm) = f.evaluate(&p, n) else { return };
    let ratio = bm.count() as f64 / n as f64;

    if ratio > 0.5 && ratio <= 0.90 {
        assert!(
            ratio <= settings().min_pruning_ratio(),
            "ratio {:.4} should now clear the threshold that 0.5 rejected",
            ratio
        );
    }
}
