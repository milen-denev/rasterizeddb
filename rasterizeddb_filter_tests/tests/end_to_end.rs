//! Real tables, real queries. Every assertion compares against a brute-force
//! expectation computed in the test, so a filter bug shows up as a wrong result
//! set rather than as a silently faster-but-incomplete scan.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::filter::sarg;
use rasterizeddb_core::core::row::row::{ColumnWritePayload, RowWrite};
use rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize;
use rasterizeddb_filter_tests::*;
use smallvec::SmallVec;

const N: u64 = STD_N;

fn all_ids() -> Vec<u64> {
    (0..N).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn point_lookups_return_exactly_the_match() {
    let (_d, t) = std_table("e2e_point").await;
    assert!(t.filters.is_some(), "filters should be active");
    for target in [0u64, 1, 511, 512, 513, 1023, 1024, 1234, N - 1] {
        let rows = run(&t, &format!("id = {}", target)).await;
        assert_eq!(u64s(&rows, 0), vec![target], "id = {}", target);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn absent_values_return_nothing() {
    let (_d, t) = std_table("e2e_absent").await;
    for missing in [N, N + 1, 99_999u64] {
        assert!(run(&t, &format!("id = {}", missing)).await.is_empty());
    }
    assert!(run(&t, "label = 'nope'").await.is_empty());
    assert!(run(&t, "cat = 999").await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn string_equality_is_exact() {
    let (_d, t) = std_table("e2e_string").await;
    for target in [0u64, 7, 777, N - 1] {
        let rows = run(&t, &format!("label = 'l{}'", target)).await;
        assert_eq!(u64s(&rows, 0), vec![target]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn float_equality_is_exact() {
    let (_d, t) = std_table("e2e_float").await;
    // score = id / 4.0
    let rows = run(&t, "score = 250").await;
    assert_eq!(u64s(&rows, 0), vec![1000]);
}

#[tokio::test(flavor = "multi_thread")]
async fn low_cardinality_predicates_are_complete() {
    let (_d, t) = std_table("e2e_lowcard").await;
    // 64 distinct values: whether or not the filter decides to prune, the
    // result set must be exact.
    for c in [0i32, 7, 63] {
        let rows = run(&t, &format!("cat = {}", c)).await;
        let expected: Vec<u64> = (0..N).filter(|i| (i % 64) as i32 == c).collect();
        assert_eq!(u64s(&rows, 0), expected, "cat = {}", c);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ranges_are_exact() {
    let (_d, t) = std_table("e2e_range").await;
    for (clause, expected) in [
        ("id > 1990", (1991..N).collect::<Vec<_>>()),
        ("id >= 1990", (1990..N).collect::<Vec<_>>()),
        ("id < 7", (0..7).collect::<Vec<_>>()),
        ("id <= 7", (0..8).collect::<Vec<_>>()),
        ("id > 1999", Vec::new()),
        ("id < 0", Vec::new()),
        ("id >= 0", all_ids()),
    ] {
        let rows = run(&t, clause).await;
        assert_eq!(u64s(&rows, 0), expected, "{}", clause);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conjunctions_are_exact() {
    let (_d, t) = std_table("e2e_and").await;
    // 1234 % 64 == 18
    assert_eq!(u64s(&run(&t, "id = 1234 AND cat = 18").await, 0), vec![1234]);
    assert!(run(&t, "id = 1234 AND cat = 19").await.is_empty());

    let rows = run(&t, "id > 1900 AND cat = 60").await;
    let expected: Vec<u64> = (1901..N).filter(|i| (i % 64) as i32 == 60).collect();
    assert_eq!(u64s(&rows, 0), expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn disjunctions_are_exact() {
    let (_d, t) = std_table("e2e_or").await;
    assert_eq!(u64s(&run(&t, "id = 5 OR id = 9").await, 0), vec![5, 9]);
    assert_eq!(
        u64s(&run(&t, "id = 5 OR id = 9 OR id = 1999").await, 0),
        vec![5, 9, 1999]
    );
    assert_eq!(u64s(&run(&t, "label = 'l7' OR label = 'l8'").await, 0), vec![7, 8]);
    assert!(run(&t, "id = 5000 OR id = 6000").await.is_empty());

    let rows = run(&t, "id < 3 OR id > 1997").await;
    assert_eq!(u64s(&rows, 0), vec![0, 1, 2, 1998, 1999]);
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_precedence_holds_end_to_end() {
    let (_d, t) = std_table("e2e_prec").await;
    // `A OR B AND C` is A ∨ (B∧C). Left-to-right folding would return neither.
    assert_eq!(
        u64s(&run(&t, "id = 7 OR id = 1234 AND cat = 18").await, 0),
        vec![7, 1234]
    );
    assert_eq!(u64s(&run(&t, "id = 7 OR id = 1234 AND cat = 19").await, 0), vec![7]);
    assert_eq!(
        u64s(&run(&t, "id = 100 AND cat = 36 OR id = 200 AND cat = 8").await, 0),
        vec![100, 200]
    );
}

/// The filter must not change what a query returns — even where the evaluator
/// itself is wrong.
///
/// These expectations are copied verbatim from `filter_disabled::PAREN_SHAPES`,
/// which measures the same clauses with filtering off. The two lists are
/// duplicated rather than shared on purpose: any divergence between them means
/// the filter has started changing *results* instead of only their cost.
///
/// Every OR group here has three or more members, or a sibling group. That is
/// deliberate — a two-element group survives boolean flattening by coincidence,
/// which is exactly how an evaluator bug in this area once passed review.
#[tokio::test(flavor = "multi_thread")]
async fn parenthesised_groups_match_the_unfiltered_path() {
    let (_d, t) = std_table("e2e_paren").await;
    let shapes: &[(&str, &[u64])] = &[
        ("cat = 4 AND (id = 4 OR id = 68 OR id = 5)", &[4, 68]),
        ("(id = 4 OR id = 68 OR id = 5) AND cat = 4", &[4, 68]),
        ("cat = 4 AND (id = 5 OR id = 6 OR id = 7 OR id = 68)", &[68]),
        ("cat = 4 AND (id = 4 OR id = 5)", &[4]),
        ("cat = 4 AND (id = 5 OR id = 4)", &[4]),
        ("(id = 5 OR id = 4) AND cat = 4", &[4]),
        ("(id = 1 OR id = 2) AND (cat = 1 OR cat = 2)", &[1, 2]),
        ("(cat = 4 AND id = 4) OR (cat = 5 AND id = 5)", &[4, 5]),
        ("cat = 4 AND ((id = 4 OR id = 5) OR id = 68)", &[4, 68]),
        ("cat = 4 AND id = 4 OR id = 68 OR id = 5", &[4, 5, 68]),
        ("(id = 1 OR id = 2)", &[1, 2]),
        ("(id >= 10 AND id <= 12)", &[10, 11, 12]),
        ("cat = 4 AND (id >= 4 AND id <= 68)", &[4, 68]),
    ];
    for (q, want) in shapes {
        assert_eq!(u64s(&run(&t, q).await, 0), want.to_vec(), "`{}`", q);
    }
    assert_eq!(u64s(&run(&t, "((id = 3))").await, 0), vec![3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn unfilterable_clauses_still_return_correct_results() {
    let (_d, t) = std_table("e2e_unfiltered").await;
    assert_eq!(run(&t, "id != 5").await.len() as u64, N - 1);
    assert_eq!(u64s(&run(&t, "id >= 1990").await, 0), (1990..N).collect::<Vec<_>>());
    assert_eq!(run(&t, "").await.len() as u64, N, "empty WHERE returns everything");
}

#[tokio::test(flavor = "multi_thread")]
async fn in_and_between_are_exact() {
    let (_d, t) = std_table("e2e_in").await;
    assert_eq!(u64s(&run_rewritten(&t, "id IN (5, 9, 1999)").await, 0), vec![5, 9, 1999]);
    assert!(run_rewritten(&t, "id IN (100000, 200000)").await.is_empty());
    assert_eq!(
        u64s(&run_rewritten(&t, "id BETWEEN 10 AND 20").await, 0),
        (10..=20).collect::<Vec<_>>()
    );
    assert_eq!(
        u64s(&run_rewritten(&t, "id BETWEEN 1995 AND 5000").await, 0),
        (1995..N).collect::<Vec<_>>()
    );
    // The shape that needs real grouping rather than stripping outer parens.
    assert_eq!(
        u64s(&run_rewritten(&t, "cat = 4 AND id IN (4, 68, 5)").await, 0),
        vec![4, 68]
    );
    assert_eq!(
        u64s(&run_rewritten(&t, "id IN (4, 68, 5) AND cat = 4").await, 0),
        vec![4, 68]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn negated_forms_are_exact() {
    let (_d, t) = std_table("e2e_not").await;
    assert_eq!(
        u64s(&run_rewritten(&t, "id NOT BETWEEN 10 AND 20").await, 0),
        (0..N).filter(|i| !(10..=20).contains(i)).collect::<Vec<_>>()
    );
    assert_eq!(
        u64s(&run_rewritten(&t, "id NOT IN (5, 9, 1999)").await, 0),
        (0..N).filter(|i| ![5, 9, 1999].contains(i)).collect::<Vec<_>>()
    );
    assert_eq!(
        u64s(&run_rewritten(&t, "cat = 4 AND id NOT IN (4, 68)").await, 0),
        (0..N).filter(|i| (i % 64) as i32 == 4 && ![4, 68].contains(i)).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_filter_actually_prunes() {
    let (_d, t) = std_table("e2e_prunes").await;
    let filters = t.filters.as_ref().unwrap();

    for (q, ceiling) in [
        ("id = 1234", N / 10),
        ("label = 'l500'", N / 10),
        ("id > 1990", N / 2),
        ("id = 5 OR id = 9", N / 10),
    ] {
        let p = sarg::extract(&tokenize(q, &t.schema.fields), filters)
            .unwrap_or_else(|| panic!("`{}` should be sargable", q));
        let bm = filters.evaluate(&p, N).unwrap_or_else(|| panic!("`{}` should prune", q));
        assert!(
            bm.count() < ceiling,
            "`{}` left {} of {} candidates",
            q,
            bm.count(),
            N
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn deletes_are_reflected() {
    let (_d, t) = std_table("e2e_delete").await;
    assert_eq!(
        t.delete_rows("id = 1234", full_fetch(&t.schema.fields), None).await.unwrap(),
        1
    );
    assert!(run(&t, "id = 1234").await.is_empty(), "deleted row still returned");
    for near in [1233u64, 1235] {
        assert_eq!(u64s(&run(&t, &format!("id = {}", near)).await, 0), vec![near]);
    }
    assert_eq!(run(&t, "").await.len() as u64, N - 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn bulk_deletes_are_reflected() {
    let (_d, t) = std_table("e2e_bulk_delete").await;
    let removed = t
        .delete_rows("cat = 4", full_fetch(&t.schema.fields), None)
        .await
        .unwrap();
    let expected_removed = (0..N).filter(|i| (i % 64) as i32 == 4).count() as u64;
    assert_eq!(removed, expected_removed);
    assert!(run(&t, "cat = 4").await.is_empty());
    assert_eq!(run(&t, "").await.len() as u64, N - expected_removed);
}

#[tokio::test(flavor = "multi_thread")]
async fn updates_are_reflected() {
    let (_d, t) = std_table("e2e_update").await;
    let updates: SmallVec<[ColumnWritePayload; 32]> =
        smallvec::smallvec![payload(&424242u64.to_le_bytes(), 0, DbType::U64)];
    assert_eq!(
        t.update_rows("id = 900", full_fetch(&t.schema.fields), updates, None)
            .await
            .unwrap(),
        1
    );
    assert!(run(&t, "id = 900").await.is_empty(), "old value still findable");
    assert_eq!(run(&t, "id = 424242").await.len(), 1, "new value not findable");
    assert_eq!(run(&t, "").await.len() as u64, N, "row count changed");
}

#[tokio::test(flavor = "multi_thread")]
async fn inserts_after_open_are_indexed() {
    let (_d, t) = std_table("e2e_insert").await;
    for i in N..N + 700 {
        let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
        rw.columns_writing_data.push(payload(&i.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data
            .push(payload(&((i % 64) as i32).to_le_bytes(), 1, DbType::I32));
        rw.columns_writing_data
            .push(payload(format!("l{}", i).as_bytes(), 2, DbType::STRING));
        rw.columns_writing_data
            .push(payload(&(i as f64 / 4.0).to_le_bytes(), 3, DbType::F64));
        t.insert_row(rw).await.unwrap();
    }
    for target in [N, N + 1, N + 350, N + 699] {
        assert_eq!(u64s(&run(&t, &format!("id = {}", target)).await, 0), vec![target]);
    }
    assert_eq!(run(&t, "").await.len() as u64, N + 700);
}

// ---------------------------------------------------------------------------
// Persistence across reopen
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn reopen_reuses_the_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "e2e_reopen", &std_columns()).await;
        fill_std(&t).await;
        t.checkpoint_filters().await;
    }
    let t = open_table(dir.path(), "e2e_reopen", &std_columns()).await;
    t.wait_filters_ready().await;
    let f = t.filters.as_ref().unwrap();
    assert_eq!(f.resume_slot(), N, "checkpoint did not cover the table");
    assert!(f.is_complete());

    for target in [0u64, 512, 1234, N - 1] {
        assert_eq!(u64s(&run(&t, &format!("id = {}", target)).await, 0), vec![target]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn reopen_indexes_rows_written_after_the_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "e2e_delta", &std_columns()).await;
        fill_std(&t).await;
        t.checkpoint_filters().await;
        for i in N..N + 300 {
            let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
            rw.columns_writing_data.push(payload(&i.to_le_bytes(), 0, DbType::U64));
            rw.columns_writing_data
                .push(payload(&((i % 64) as i32).to_le_bytes(), 1, DbType::I32));
            rw.columns_writing_data
                .push(payload(format!("l{}", i).as_bytes(), 2, DbType::STRING));
            rw.columns_writing_data
                .push(payload(&(i as f64 / 4.0).to_le_bytes(), 3, DbType::F64));
            t.insert_row(rw).await.unwrap();
        }
    }
    let t = open_table(dir.path(), "e2e_delta", &std_columns()).await;
    t.wait_filters_ready().await;
    for target in [5u64, N - 1, N, N + 150, N + 299] {
        assert_eq!(u64s(&run(&t, &format!("id = {}", target)).await, 0), vec![target]);
    }
    assert_eq!(run(&t, "").await.len() as u64, N + 300);
}

#[tokio::test(flavor = "multi_thread")]
async fn reopen_after_update_finds_the_new_value() {
    // The exact hazard the dirty byte exists for: a stale checkpoint would hold
    // the OLD fingerprint and prune the row when queried by its NEW value.
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "e2e_reopen_upd", &std_columns()).await;
        fill_std(&t).await;
        t.checkpoint_filters().await;
        let updates: SmallVec<[ColumnWritePayload; 32]> =
            smallvec::smallvec![payload(&424242u64.to_le_bytes(), 0, DbType::U64)];
        t.update_rows("id = 900", full_fetch(&t.schema.fields), updates, None)
            .await
            .unwrap();
    }
    let t = open_table(dir.path(), "e2e_reopen_upd", &std_columns()).await;
    t.wait_filters_ready().await;
    assert_eq!(run(&t, "id = 424242").await.len(), 1, "updated value lost after reopen");
    assert!(run(&t, "id = 900").await.is_empty(), "stale value still findable");
}

#[tokio::test(flavor = "multi_thread")]
async fn reopen_after_delete_is_still_correct() {
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "e2e_reopen_del", &std_columns()).await;
        fill_std(&t).await;
        t.checkpoint_filters().await;
        t.delete_rows("id = 1234", full_fetch(&t.schema.fields), None)
            .await
            .unwrap();
    }
    let t = open_table(dir.path(), "e2e_reopen_del", &std_columns()).await;
    t.wait_filters_ready().await;
    assert!(run(&t, "id = 1234").await.is_empty(), "deleted row reappeared");
    assert_eq!(run(&t, "").await.len() as u64, N - 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mutation_marks_the_sidecar_dirty_on_disk() {
    // Regression: the dirty byte must actually reach the file. Guarding this
    // write on `io.get_len()` silently disabled it, because the provider tracks
    // length in an atomic that `write_data` does not update.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("e2e_dirty_flt.db");
    let t = open_table(dir.path(), "e2e_dirty", &std_columns()).await;
    fill_std(&t).await;
    t.checkpoint_filters().await;
    assert_eq!(std::fs::read(&path).unwrap()[13], 0, "fresh checkpoint should be clean");

    t.delete_rows("id = 42", full_fetch(&t.schema.fields), None).await.unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap()[13],
        1,
        "a delete must mark the on-disk checkpoint stale"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_sidecar_falls_back_to_a_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "e2e_corrupt", &std_columns()).await;
        fill_std(&t).await;
        t.checkpoint_filters().await;
    }
    let path = dir.path().join("e2e_corrupt_flt.db");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0..4].copy_from_slice(b"JUNK");
    std::fs::write(&path, &bytes).unwrap();

    let t = open_table(dir.path(), "e2e_corrupt", &std_columns()).await;
    t.wait_filters_ready().await;
    for target in [0u64, 1234, N - 1] {
        assert_eq!(u64s(&run(&t, &format!("id = {}", target)).await, 0), vec![target]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_truncated_sidecar_falls_back_to_a_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "e2e_trunc", &std_columns()).await;
        fill_std(&t).await;
        t.checkpoint_filters().await;
    }
    let path = dir.path().join("e2e_trunc_flt.db");
    let bytes = std::fs::read(&path).unwrap();
    std::fs::write(&path, &bytes[..bytes.len() / 3]).unwrap();

    let t = open_table(dir.path(), "e2e_trunc", &std_columns()).await;
    t.wait_filters_ready().await;
    assert_eq!(u64s(&run(&t, "id = 1234").await, 0), vec![1234]);
    assert_eq!(run(&t, "").await.len() as u64, N);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_sidecar_is_rebuilt() {
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "e2e_gone", &std_columns()).await;
        fill_std(&t).await;
        t.checkpoint_filters().await;
    }
    let _ = std::fs::remove_file(dir.path().join("e2e_gone_flt.db"));

    let t = open_table(dir.path(), "e2e_gone", &std_columns()).await;
    t.wait_filters_ready().await;
    assert_eq!(u64s(&run(&t, "id = 7").await, 0), vec![7]);
    assert_eq!(run(&t, "").await.len() as u64, N);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_explicit_backfill_is_idempotent() {
    let (_d, t) = std_table("e2e_backfill").await;
    assert!(t.filters.as_ref().unwrap().is_complete());
    assert_eq!(t.backfill_filters().await.unwrap(), N);
    assert_eq!(t.backfill_filters().await.unwrap(), N);
    for target in [0u64, 1234, N - 1] {
        assert_eq!(u64s(&run(&t, &format!("id = {}", target)).await, 0), vec![target]);
    }
}

// ---------------------------------------------------------------------------
// Nullability
// ---------------------------------------------------------------------------

/// Rows in the nullable fixture. One `NULL` every 1000, so most 512-row zones
/// hold none at all and the flag column's bounds can rule them out.
const NULL_N: u64 = 4000;

#[inline]
fn is_null_row(i: u64) -> bool {
    i % 1000 == 0
}

/// `id U64`, `country STRING`, and the companion `__null_country BOOL` that
/// makes `country` nullable. Built by hand rather than through `CREATE TABLE`
/// so the fixture states exactly what the storage layer holds.
async fn nullable_table(
    name: &str,
) -> (tempfile::TempDir, rasterizeddb_core::core::row::table::Table<
    rasterizeddb_core::core::storage_providers::file_sync::LocalStorageProvider,
>) {
    let dir = tempfile::tempdir().unwrap();
    let table = open_table(
        dir.path(),
        name,
        &[
            ("id", DbType::U64),
            ("country", DbType::STRING),
            ("__null_country", DbType::BOOL),
        ],
    )
    .await;

    for i in 0..NULL_N {
        let null = is_null_row(i);
        // A NULL row still stores *something* in the value column; the
        // companion is what says the value is not to be believed.
        let country = if null { String::new() } else { format!("c{}", i % 40) };
        let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
        rw.columns_writing_data.push(payload(&i.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data.push(payload(country.as_bytes(), 1, DbType::STRING));
        rw.columns_writing_data.push(payload(
            &rasterizeddb_core::core::rql::nullable::flag_bytes(null),
            2,
            DbType::BOOL,
        ));
        table.insert_row(rw).await.unwrap();
    }
    (dir, table)
}

#[tokio::test(flavor = "multi_thread")]
async fn is_null_returns_exactly_the_null_rows() {
    let (_d, t) = nullable_table("e2e_is_null").await;
    let rows = run_rewritten(&t, "country IS NULL").await;
    let expected: Vec<u64> = (0..NULL_N).filter(|i| is_null_row(*i)).collect();
    assert_eq!(u64s(&rows, 0), expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn is_not_null_returns_exactly_the_rest() {
    // `IS NOT NULL` desugars to `__null_country = 0`, which is an *equality*,
    // not the complement of one. That distinction is what makes it safe: the
    // trap `!=` falls into is over-approximating a set and then negating it,
    // and nothing here negates anything.
    let (_d, t) = nullable_table("e2e_is_not_null").await;
    let rows = run_rewritten(&t, "country IS NOT NULL").await;
    let expected: Vec<u64> = (0..NULL_N).filter(|i| !is_null_row(*i)).collect();
    assert_eq!(u64s(&rows, 0), expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_null_row_does_not_answer_to_the_empty_string() {
    // The collision that makes encoding NULL as an empty value wrong. The
    // fixture stores `''` for a NULL row deliberately, so if the guard conjunct
    // were dropped this would return 4 extra rows.
    let (_d, t) = nullable_table("e2e_null_vs_empty").await;
    let rows = run_rewritten(&t, "country = ''").await;
    assert!(rows.is_empty(), "NULL rows must not match `= ''`");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_guarded_comparison_is_exact() {
    // `country = 'c7'` rewrites to `(__null_country = 0 AND country = 'c7')`.
    // Both conjuncts are now sargable, and their masks intersect.
    let (_d, t) = nullable_table("e2e_null_guard").await;
    let rows = run_rewritten(&t, "country = 'c7'").await;
    let expected: Vec<u64> =
        (0..NULL_N).filter(|i| !is_null_row(*i) && i % 40 == 7).collect();
    assert_eq!(u64s(&rows, 0), expected);
    assert!(!expected.is_empty(), "the fixture should have matches");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_null_flag_actually_prunes() {
    // The point of indexing BOOL at all. Without it `IS NULL` would be a full
    // scan; with it, every zone holding no NULL is skipped outright.
    let (_d, t) = nullable_table("e2e_null_prunes").await;
    let filters = t.filters.as_ref().expect("filters active");
    let n = NULL_N;
    let toks = tokenize("__null_country = 1", &t.schema.fields);
    let p = sarg::extract(&toks, filters).expect("the flag column is sargable");
    let bm = filters.evaluate(&p, n).expect("a sparse flag should prune");

    for slot in (0..n).filter(|i| is_null_row(*i)) {
        assert!(bm.get(slot), "slot {} pruned despite being NULL", slot);
    }

    // Zone bounds are exact at zone granularity: a zone survives iff it holds at
    // least one `1`, and every other zone is gone. Stating it as an equality
    // rather than "fewer than half" is what makes this a test of the structure
    // instead of a test of the fixture's density.
    const ZONE: u64 = 512;
    let live: std::collections::BTreeSet<u64> =
        (0..n).filter(|i| is_null_row(*i)).map(|i| i / ZONE).collect();
    let expected: u64 = live.iter().map(|z| ZONE.min(n - z * ZONE)).sum();
    assert_eq!(
        bm.count(),
        expected,
        "expected only the {} zones holding a NULL to survive",
        live.len()
    );
    assert!(expected < n, "the fixture must leave something to prune");
}

// ---------------------------------------------------------------------------
// Prefix matching and string ranges
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn prefix_matches_are_exact() {
    // `label` is `l{id}`, inserted in id order, so the column has real locality
    // and its zone bounds are tight. The assertion is exactness, though — a
    // prefix that pruned a matching row would show up as a missing result, not
    // as a slow query.
    let (_d, t) = std_table("e2e_prefix").await;
    for prefix in ["l", "l1", "l19", "l199", "l1999", "l2", "zz", "l0"] {
        let rows = run_rewritten(&t, &format!("label LIKE '{}%'", prefix)).await;
        let expected: Vec<u64> =
            (0..N).filter(|i| format!("l{}", i).starts_with(prefix)).collect();
        assert_eq!(u64s(&rows, 0), expected, "label LIKE '{}%'", prefix);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn startswith_written_directly_is_exact() {
    let (_d, t) = std_table("e2e_startswith").await;
    let rows = run(&t, "label STARTSWITH 'l17'").await;
    let expected: Vec<u64> = (0..N).filter(|i| format!("l{}", i).starts_with("l17")).collect();
    assert_eq!(u64s(&rows, 0), expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn case_insensitive_prefixes_are_still_exact() {
    // `ILIKE` lowers to `ISTARTSWITH`, which the filter refuses to lower to a
    // range. Refusing means "everything is a candidate", so the answer must be
    // identical to the case-sensitive path's — just unpruned.
    let (_d, t) = std_table("e2e_iprefix").await;
    let rows = run_rewritten(&t, "label ILIKE 'L17%'").await;
    let expected: Vec<u64> = (0..N).filter(|i| format!("l{}", i).starts_with("l17")).collect();
    assert_eq!(u64s(&rows, 0), expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn string_ranges_are_exact() {
    let (_d, t) = std_table("e2e_string_range").await;
    for clause in ["label >= 'l5'", "label > 'l5'", "label < 'l5'", "label <= 'l5'"] {
        let rows = run(&t, clause).await;
        let op = clause.trim_start_matches("label ").split(' ').next().unwrap().to_string();
        let expected: Vec<u64> = (0..N)
            .filter(|i| {
                let l = format!("l{}", i);
                match op.as_str() {
                    ">=" => l.as_str() >= "l5",
                    ">" => l.as_str() > "l5",
                    "<" => l.as_str() < "l5",
                    _ => l.as_str() <= "l5",
                }
            })
            .collect();
        assert_eq!(u64s(&rows, 0), expected, "{}", clause);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_prefix_match_actually_prunes() {
    // The payoff. Without the range lowering `STARTSWITH` is opaque and the mask
    // stays all-ones; with it, the zone bounds rule out every zone whose labels
    // fall outside `['k017', 'k018')`.
    //
    // Zero-padded labels, so lexicographic order matches insert order. That is
    // the locality zone bounds need and `l{id}` does not have: `l1000` sorts
    // between `l0` and `l2`, so every zone of the standard fixture spans most of
    // the key space. A prefix match on unsorted string data is still *exact* —
    // `prefix_matches_are_exact` covers that — it just prunes nothing.
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(
        dir.path(),
        "e2e_prefix_prunes",
        &[("id", DbType::U64), ("label", DbType::STRING)],
    )
    .await;
    let n = 4000u64;
    for i in 0..n {
        let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
        rw.columns_writing_data.push(payload(&i.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data
            .push(payload(format!("k{:06}", i).as_bytes(), 1, DbType::STRING));
        t.insert_row(rw).await.unwrap();
    }

    let filters = t.filters.as_ref().expect("filters active");
    let toks = tokenize("label STARTSWITH 'k0017'", &t.schema.fields);
    let p = sarg::extract(&toks, filters).expect("STARTSWITH is sargable");
    let bm = filters.evaluate(&p, n).expect("a prefix should prune");

    let matches = |i: u64| format!("k{:06}", i).starts_with("k0017");
    for slot in (0..n).filter(|i| matches(*i)) {
        assert!(bm.get(slot), "slot {} pruned despite matching the prefix", slot);
    }
    // 100 matching rows out of 4000, all inside one 512-row zone.
    assert!(
        bm.count() <= 512,
        "expected all but the one zone holding the prefix to go, {} of {} survived",
        bm.count(),
        n
    );

    // And the query answers exactly.
    let rows = run_rewritten(&t, "label LIKE 'k0017%'").await;
    assert_eq!(u64s(&rows, 0), (0..n).filter(|i| matches(*i)).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_range_mask_excludes_the_whole_dead_prefix() {
    // `WHERE id >= p` matches most of the table, so a guard that judges a mask
    // by what fraction it excludes will throw this one away — and the scan then
    // fetches row bytes for every row below `p` before its first match. What
    // makes the mask worth keeping is not how much it excludes but *where*: the
    // excluded slots are all in front.
    //
    // Deciding that is the scan's call (`build_candidates`). What this pins is
    // the property the filter owes it: the mask really does exclude the prefix,
    // down to zone granularity, so a leading-run test has something to find.
    let (_d, t) = std_table("e2e_dead_prefix").await;
    let filters = t.filters.as_ref().expect("filters active");

    for pivot in [512u64, 1000, 1536, N / 2] {
        let toks = tokenize(&format!("id >= {}", pivot), &t.schema.fields);
        let p = sarg::extract(&toks, filters).expect("a range on id is sargable");
        let bm = filters.evaluate(&p, N).expect("mask built");

        let dead = (0..N).take_while(|s| !bm.get(*s)).count() as u64;

        // Bounds prune whole zones, so the run reaches the start of the zone
        // holding `pivot` and no further.
        let zone_start = (pivot / 512) * 512;
        assert_eq!(
            dead, zone_start,
            "pivot {}: expected the mask to exclude the leading {} slots, got {}",
            pivot, zone_start, dead
        );

        // Excluding whole zones and nothing else, so the surviving count is
        // exactly what is left above the zone boundary.
        assert_eq!(
            bm.count(),
            N - zone_start,
            "pivot {}: the mask should exclude the leading zones and nothing else",
            pivot
        );

        // Soundness, as always: nothing matching may be missing.
        for slot in pivot..N {
            assert!(bm.get(slot), "pivot {}: slot {} pruned despite matching", pivot, slot);
        }
    }

    // The shape that a ratio-only guard declines: a low pivot leaves most of the
    // table surviving, so "how much is excluded" says the mask is worthless
    // while "where the exclusions are" says it skips the entire lead-in.
    let toks = tokenize("id >= 512", &t.schema.fields);
    let p = sarg::extract(&toks, filters).expect("sargable");
    let bm = filters.evaluate(&p, N).expect("mask built");
    assert!(
        bm.count() as f64 / N as f64 > 0.5,
        "the fixture must reproduce the high-survivor case"
    );
    assert_eq!((0..N).take_while(|s| !bm.get(*s)).count(), 512);
}
