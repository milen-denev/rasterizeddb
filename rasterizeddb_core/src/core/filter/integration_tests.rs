//! End-to-end tests: the filter must never change what a query returns, only
//! how much work it takes to return it.
//!
//! Every assertion here compares against a brute-force expectation computed in
//! the test, so a filter bug shows up as a wrong result set rather than as a
//! silently faster-but-incomplete scan.

use itertools::Itertools;
use rclite::Arc;
use smallvec::SmallVec;

use crate::core::db_type::DbType;
use crate::core::row::row::{ColumnFetchingData, ColumnWritePayload, Row, RowFetch, RowWrite};
use crate::core::row::schema::{SchemaCalculator, SchemaField};
use crate::core::row::table::Table;
use crate::core::rql::lexer_ct::CreateColumnData;
use crate::core::storage_providers::file_sync::LocalStorageProvider;
use crate::memory_pool::MEMORY_POOL;

const N: u64 = 2000;

fn init() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    // The query-result cache would short-circuit repeated WHERE clauses and hide
    // what the filter is doing; these tests want the real scan path.
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
}

fn payload(bytes: &[u8], write_order: u32, column_type: DbType) -> ColumnWritePayload {
    let mut mb = MEMORY_POOL.acquire(bytes.len());
    mb.into_slice_mut().copy_from_slice(bytes);
    let size = column_type.get_size();
    ColumnWritePayload { data: mb, write_order, column_type, size }
}

fn full_fetch(fields: &SmallVec<[SchemaField; 20]>) -> RowFetch {
    let calc = SchemaCalculator::default();
    let mut cols = SmallVec::new();
    for f in fields.iter().sorted_by(|a, b| a.write_order.cmp(&b.write_order)) {
        let (column_offset, schema_id) = calc.calculate_schema_offset(&f.name, fields);
        cols.push(ColumnFetchingData {
            column_offset,
            column_type: f.db_type.clone(),
            size: f.size,
            schema_id,
        });
    }
    RowFetch { columns_fetching_data: cols }
}

/// Table with `id U64` (unique), `cat I32` (64 distinct), `label STRING` (unique).
///
/// Backed by a real file provider rather than `MemoryStorageProvider`: the
/// latter's `create_new` hands back an independent buffer per call, so
/// `TableSchema::add_field` never observes previously added fields and every
/// column ends up with `write_order = 0`. The filter keys on `write_order`, as
/// does the tokenizer, so the test needs the production storage path.
async fn build_table(name: &str) -> (tempfile::TempDir, Table<LocalStorageProvider>) {
    let dir = tempfile::tempdir().unwrap();
    let table = open_table(dir.path(), name).await;
    fill(&table).await;
    // TempDir is returned so it outlives the table; dropping it would delete the
    // files out from under the still-running IO service tasks.
    (dir, table)
}

/// Open (or reopen) the table over an existing directory.
async fn open_table(dir: &std::path::Path, name: &str) -> Table<LocalStorageProvider> {
    init();
    let io = Arc::new(LocalStorageProvider::new(dir.to_str().unwrap(), Some(name)).await);
    Table::new(
        name,
        io,
        vec![
            CreateColumnData { name: "id".into(), data_type: DbType::U64 },
            CreateColumnData { name: "cat".into(), data_type: DbType::I32 },
            CreateColumnData { name: "label".into(), data_type: DbType::STRING },
        ],
    )
    .await
}

async fn fill(table: &Table<LocalStorageProvider>) {
    for i in 0..N {
        let label = format!("l{}", i);
        let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
        rw.columns_writing_data.push(payload(&i.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data
            .push(payload(&((i % 64) as i32).to_le_bytes(), 1, DbType::I32));
        rw.columns_writing_data.push(payload(label.as_bytes(), 2, DbType::STRING));
        table.insert_row(rw).await.unwrap();
    }
}

async fn run(table: &Table<LocalStorageProvider>, where_clause: &str) -> Vec<Row> {
    // RowFetch is not Clone, so build both independently.
    table
        .query_row(
            where_clause,
            &table.schema.fields.to_vec(),
            full_fetch(&table.schema.fields),
            full_fetch(&table.schema.fields),
            None,
            None,
        )
        .await
        .unwrap()
}

fn ids_of(rows: &[Row]) -> Vec<u64> {
    rows.iter()
        .map(|r| {
            let c = r.columns.iter().find(|c| c.schema_id == 0).unwrap();
            let b = c.data.into_slice();
            u64::from_le_bytes(b.try_into().unwrap())
        })
        .sorted()
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn point_lookup_returns_exactly_the_match() {
    let (_dir, table) = build_table("flt_point").await;
    assert!(table.filters.is_some(), "filters should be active");

    for target in [0u64, 1, 511, 512, 1234, N - 1] {
        let rows = run(&table, &format!("id = {}", target)).await;
        assert_eq!(ids_of(&rows), vec![target], "id = {}", target);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn string_equality_returns_exactly_the_match() {
    let (_dir, table) = build_table("flt_str").await;
    for target in [0u64, 777, N - 1] {
        let rows = run(&table, &format!("label = 'l{}'", target)).await;
        assert_eq!(ids_of(&rows), vec![target], "label = l{}", target);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn low_cardinality_predicate_is_complete() {
    let (_dir, table) = build_table("flt_cat").await;
    // 64 distinct values over 2000 rows. Whether or not the filter decides this
    // is worth pruning, the result set must be exact.
    let rows = run(&table, "cat = 7").await;
    let expected: Vec<u64> = (0..N).filter(|i| (i % 64) as i32 == 7).collect();
    assert_eq!(ids_of(&rows), expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn conjunction_is_exact() {
    let (_dir, table) = build_table("flt_and").await;
    // 1234 % 64 == 18
    let rows = run(&table, "id = 1234 AND cat = 18").await;
    assert_eq!(ids_of(&rows), vec![1234]);

    // Contradiction: the filter may prune everything, and so must the evaluator.
    let rows = run(&table, "id = 1234 AND cat = 19").await;
    assert!(rows.is_empty(), "contradictory conjunction returned rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn absent_value_returns_nothing() {
    let (_dir, table) = build_table("flt_absent").await;
    for missing in [N, N + 1, 999_999u64] {
        let rows = run(&table, &format!("id = {}", missing)).await;
        assert!(rows.is_empty(), "id = {} matched something", missing);
    }
    let rows = run(&table, "label = 'nope'").await;
    assert!(rows.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn unfilterable_clauses_still_return_correct_results() {
    let (_dir, table) = build_table("flt_unfilterable").await;

    // Inequality: probe(v)'s complement under-approximates, so no filtering.
    let rows = run(&table, "id != 5").await;
    assert_eq!(rows.len() as u64, N - 1);

    // Range: not sargable for a hash filter at all.
    let rows = run(&table, "id >= 1990").await;
    assert_eq!(ids_of(&rows), (1990..N).collect::<Vec<_>>());

    // Parenthesised: structure the extractor does not model.
    let rows = run(&table, "(id = 3) AND cat = 3").await;
    assert_eq!(ids_of(&rows), vec![3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_where_returns_everything() {
    let (_dir, table) = build_table("flt_all").await;
    let rows = run(&table, "").await;
    assert_eq!(rows.len() as u64, N);
}

#[tokio::test(flavor = "multi_thread")]
async fn deletes_are_reflected() {
    let (_dir, table) = build_table("flt_del").await;
    let deleted = table
        .delete_rows("id = 1234", full_fetch(&table.schema.fields), None)
        .await
        .unwrap();
    assert_eq!(deleted, 1);

    let rows = run(&table, "id = 1234").await;
    assert!(rows.is_empty(), "deleted row still returned");

    // Neighbours must survive: a delete clears exactly one slot.
    for near in [1233u64, 1235] {
        let rows = run(&table, &format!("id = {}", near)).await;
        assert_eq!(ids_of(&rows), vec![near], "delete leaked into slot {}", near);
    }

    let rows = run(&table, "").await;
    assert_eq!(rows.len() as u64, N - 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn updates_are_reflected() {
    let (_dir, table) = build_table("flt_upd").await;
    // Same-width update: stays in its slot, fingerprints overwritten in place.
    let updates: SmallVec<[ColumnWritePayload; 32]> =
        smallvec::smallvec![payload(&424242u64.to_le_bytes(), 0, DbType::U64)];
    let n = table
        .update_rows("id = 900", full_fetch(&table.schema.fields), updates, None)
        .await
        .unwrap();
    assert_eq!(n, 1);

    let rows = run(&table, "id = 900").await;
    assert!(rows.is_empty(), "old value still findable after update");

    let rows = run(&table, "id = 424242").await;
    assert_eq!(rows.len(), 1, "new value not findable after update");

    let rows = run(&table, "").await;
    assert_eq!(rows.len() as u64, N, "update changed the row count");
}

#[tokio::test(flavor = "multi_thread")]
async fn filter_actually_prunes() {
    let (_dir, table) = build_table("flt_prune").await;
    let filters = table.filters.as_ref().unwrap();

    // A unique-column point lookup should leave a tiny candidate set.
    let program = super::sarg::extract(
        &crate::core::tokenizer::query_tokenizer::tokenize("id = 1234", &table.schema.fields),
        filters,
    )
    .expect("id = <lit> must be sargable");

    let bm = filters.evaluate(&program, N).expect("should produce a bitmap");
    assert!(bm.get(1234), "true match was pruned");
    assert!(
        bm.count() < N / 10,
        "expected heavy pruning, {} of {} slots survived",
        bm.count(),
        N
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_indexes_preexisting_rows() {
    let (_dir, table) = build_table("flt_backfill").await;
    let filters = table.filters.as_ref().unwrap();
    assert!(filters.is_complete());

    // Re-running a backfill over an already-complete filter must be a no-op
    // for correctness.
    let indexed = table.backfill_filters().await.unwrap();
    assert_eq!(indexed, N);

    for target in [0u64, 1234, N - 1] {
        let rows = run(&table, &format!("id = {}", target)).await;
        assert_eq!(ids_of(&rows), vec![target]);
    }
}

// ---------------------------------------------------------------------------
// Persistence across reopen
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn reopen_reuses_the_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    {
        let table = open_table(dir.path(), "flt_reopen").await;
        fill(&table).await;
        table.checkpoint_filters().await;
    }

    let table = open_table(dir.path(), "flt_reopen").await;
    table.wait_filters_ready().await;
    let filters = table.filters.as_ref().expect("filters should be active");

    // The checkpoint covered every row, so nothing needed rescanning.
    assert_eq!(filters.resume_slot(), N, "checkpoint did not cover the table");
    assert!(filters.is_complete());

    for target in [0u64, 512, 1234, N - 1] {
        let rows = run(&table, &format!("id = {}", target)).await;
        assert_eq!(ids_of(&rows), vec![target], "id = {} after reopen", target);
    }
    let rows = run(&table, "label = 'l999'").await;
    assert_eq!(ids_of(&rows), vec![999]);
}

#[tokio::test(flavor = "multi_thread")]
async fn reopen_indexes_rows_written_after_the_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    {
        let table = open_table(dir.path(), "flt_delta").await;
    table.wait_filters_ready().await;
        fill(&table).await;
        table.checkpoint_filters().await;

        // Rows appended with no checkpoint after them: the sidecar knows
        // nothing about these.
        for i in N..N + 300 {
            let label = format!("l{}", i);
            let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
            rw.columns_writing_data.push(payload(&i.to_le_bytes(), 0, DbType::U64));
            rw.columns_writing_data
                .push(payload(&((i % 64) as i32).to_le_bytes(), 1, DbType::I32));
            rw.columns_writing_data.push(payload(label.as_bytes(), 2, DbType::STRING));
            table.insert_row(rw).await.unwrap();
        }
    }

    // Open runs a delta backfill from the checkpoint frontier.
    let table = open_table(dir.path(), "flt_delta").await;
    table.wait_filters_ready().await;
    for target in [5u64, N - 1, N, N + 150, N + 299] {
        let rows = run(&table, &format!("id = {}", target)).await;
        assert_eq!(ids_of(&rows), vec![target], "id = {} after delta backfill", target);
    }
    let rows = run(&table, "").await;
    assert_eq!(rows.len() as u64, N + 300);
}

#[tokio::test(flavor = "multi_thread")]
async fn reopen_after_delete_is_still_correct() {
    let dir = tempfile::tempdir().unwrap();
    {
        let table = open_table(dir.path(), "flt_reopen_del").await;
    table.wait_filters_ready().await;
        fill(&table).await;
        table.checkpoint_filters().await;
        // A delete marks the sidecar dirty; delete_rows re-checkpoints after.
        table
            .delete_rows("id = 1234", full_fetch(&table.schema.fields), None)
            .await
            .unwrap();
    }

    let table = open_table(dir.path(), "flt_reopen_del").await;
    table.wait_filters_ready().await;
    let rows = run(&table, "id = 1234").await;
    assert!(rows.is_empty(), "deleted row reappeared after reopen");

    for near in [1233u64, 1235] {
        let rows = run(&table, &format!("id = {}", near)).await;
        assert_eq!(ids_of(&rows), vec![near]);
    }
    let rows = run(&table, "").await;
    assert_eq!(rows.len() as u64, N - 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn reopen_after_update_finds_the_new_value() {
    // The exact hazard the dirty byte exists for: a stale checkpoint would hold
    // the OLD fingerprint and prune the row when queried by its NEW value.
    let dir = tempfile::tempdir().unwrap();
    {
        let table = open_table(dir.path(), "flt_reopen_upd").await;
    table.wait_filters_ready().await;
        fill(&table).await;
        table.checkpoint_filters().await;

        let updates: SmallVec<[ColumnWritePayload; 32]> =
            smallvec::smallvec![payload(&424242u64.to_le_bytes(), 0, DbType::U64)];
        table
            .update_rows("id = 900", full_fetch(&table.schema.fields), updates, None)
            .await
            .unwrap();
    }

    let table = open_table(dir.path(), "flt_reopen_upd").await;
    table.wait_filters_ready().await;
    let rows = run(&table, "id = 424242").await;
    assert_eq!(rows.len(), 1, "updated value not findable after reopen");
    let rows = run(&table, "id = 900").await;
    assert!(rows.is_empty(), "stale value still findable after reopen");
}

#[tokio::test(flavor = "multi_thread")]
async fn corrupt_sidecar_falls_back_to_a_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    {
        let table = open_table(dir.path(), "flt_corrupt").await;
    table.wait_filters_ready().await;
        fill(&table).await;
        table.checkpoint_filters().await;
    }

    // Scribble over the magic. A rejected sidecar must cost a rebuild, never
    // a wrong answer.
    let path = dir.path().join("flt_corrupt_flt.db");
    assert!(path.exists(), "sidecar was never written: {:?}", path);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0..4].copy_from_slice(b"JUNK");
    std::fs::write(&path, &bytes).unwrap();

    let table = open_table(dir.path(), "flt_corrupt").await;
    table.wait_filters_ready().await;
    for target in [0u64, 1234, N - 1] {
        let rows = run(&table, &format!("id = {}", target)).await;
        assert_eq!(ids_of(&rows), vec![target], "id = {} after corrupt sidecar", target);
    }
}

// ---------------------------------------------------------------------------
// Range predicates via zone min/max
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn range_queries_are_exact() {
    let (_dir, table) = build_table("flt_range").await;

    for (clause, expected) in [
        ("id > 1990", (1991..N).collect::<Vec<_>>()),
        ("id >= 1990", (1990..N).collect::<Vec<_>>()),
        ("id < 7", (0..7).collect::<Vec<_>>()),
        ("id <= 7", (0..8).collect::<Vec<_>>()),
        ("id > 1999", Vec::new()),
        ("id < 0", Vec::new()),
    ] {
        let rows = run(&table, clause).await;
        assert_eq!(ids_of(&rows), expected, "{}", clause);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn literal_first_comparisons_match_the_same_rows_as_column_first() {
    // `tokenize` still types a numeric literal from the *preceding* column, so
    // in `1990 < id` the literal is typed before `id` is seen. The parser now
    // reconciles the two by re-encoding the literal at the column's type, which
    // makes the reversed form behave exactly like `id > 1990`.
    let (_dir, table) = build_table("flt_rev").await;

    let reversed = run(&table, "1990 < id").await;
    let forward = run(&table, "id > 1990").await;

    assert_eq!(
        ids_of(&reversed),
        ids_of(&forward),
        "`1990 < id` must select the same rows as `id > 1990`"
    );
    assert!(!ids_of(&reversed).is_empty(), "expected the range to match some rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn range_and_equality_compose() {
    let (_dir, table) = build_table("flt_mixed").await;
    // cat = id % 64, so ids > 1900 with cat == 60 are 1916, 1980.
    let rows = run(&table, "id > 1900 AND cat = 60").await;
    let expected: Vec<u64> = (1901..N).filter(|i| (i % 64) as i32 == 60).collect();
    assert_eq!(ids_of(&rows), expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn range_on_string_column_falls_back() {
    let (_dir, table) = build_table("flt_strrange").await;
    // Strings have no ordered bounds; the query must still be answered by the
    // evaluator alone.
    let rows = run(&table, "label >= 'l999'").await;
    assert!(!rows.is_empty(), "string range returned nothing");
    for id in ids_of(&rows) {
        let label = format!("l{}", id);
        assert!(label.as_bytes() >= b"l999".as_slice(), "{} should not match", label);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn range_prunes_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let table = open_table(dir.path(), "flt_range_reopen").await;
    table.wait_filters_ready().await;
        fill(&table).await;
        table.checkpoint_filters().await;
    }

    let table = open_table(dir.path(), "flt_range_reopen").await;
    table.wait_filters_ready().await;
    let rows = run(&table, "id > 1990").await;
    assert_eq!(ids_of(&rows), (1991..N).collect::<Vec<_>>());

    // Bounds must have survived the checkpoint, not just the fingerprints.
    let filters = table.filters.as_ref().unwrap();
    let program = super::sarg::extract(
        &crate::core::tokenizer::query_tokenizer::tokenize("id > 1990", &table.schema.fields),
        filters,
    )
    .unwrap();
    let bm = filters.evaluate(&program, N).unwrap();
    assert!(bm.count() < N / 2, "bounds lost selectivity across reopen: {}", bm.count());
}

// ---------------------------------------------------------------------------
// OR / sum-of-products
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn or_queries_are_exact() {
    let (_dir, table) = build_table("flt_or").await;

    let rows = run(&table, "id = 5 OR id = 9").await;
    assert_eq!(ids_of(&rows), vec![5, 9]);

    let rows = run(&table, "id = 5 OR id = 9 OR id = 1999").await;
    assert_eq!(ids_of(&rows), vec![5, 9, 1999]);

    let rows = run(&table, "label = 'l7' OR label = 'l8'").await;
    assert_eq!(ids_of(&rows), vec![7, 8]);

    // Nothing on either side.
    let rows = run(&table, "id = 5000 OR id = 6000").await;
    assert!(rows.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn sql_precedence_is_respected_end_to_end() {
    let (_dir, table) = build_table("flt_prec").await;

    // `A OR B AND C` is `A ∨ (B∧C)`. 1234 % 64 == 18, so the second group
    // matches only 1234; the first matches only 7. Folding left-to-right as
    // `(A∨B) ∧ C` would return neither.
    let rows = run(&table, "id = 7 OR id = 1234 AND cat = 18").await;
    assert_eq!(ids_of(&rows), vec![7, 1234]);

    // Second group is contradictory, so only the first survives.
    let rows = run(&table, "id = 7 OR id = 1234 AND cat = 19").await;
    assert_eq!(ids_of(&rows), vec![7]);

    // `A AND B OR C AND D`
    let rows = run(&table, "id = 100 AND cat = 36 OR id = 200 AND cat = 8").await;
    assert_eq!(ids_of(&rows), vec![100, 200]);
}

#[tokio::test(flavor = "multi_thread")]
async fn or_mixing_ranges_and_equality_is_exact() {
    let (_dir, table) = build_table("flt_or_range").await;

    let rows = run(&table, "id > 1995 OR id = 3").await;
    let mut expected: Vec<u64> = (1996..N).collect();
    expected.insert(0, 3);
    assert_eq!(ids_of(&rows), expected);

    let rows = run(&table, "id < 3 OR id > 1997").await;
    assert_eq!(ids_of(&rows), vec![0, 1, 2, 1998, 1999]);
}

#[tokio::test(flavor = "multi_thread")]
async fn or_still_prunes() {
    let (_dir, table) = build_table("flt_or_prune").await;
    let filters = table.filters.as_ref().unwrap();

    let program = super::sarg::extract(
        &crate::core::tokenizer::query_tokenizer::tokenize(
            "id = 5 OR id = 9",
            &table.schema.fields,
        ),
        filters,
    )
    .expect("OR of equalities should be sargable");
    assert!(matches!(&program.root, super::sarg::MaskExpr::Or(v) if v.len() == 2));

    let bm = filters.evaluate(&program, N).expect("bitmap");
    assert!(bm.get(5) && bm.get(9), "true matches pruned");
    assert!(bm.count() < N / 10, "union barely pruned: {} of {}", bm.count(), N);
}

#[tokio::test(flavor = "multi_thread")]
async fn mutation_marks_the_sidecar_dirty_on_disk() {
    // Regression: the dirty byte must actually reach the file. Guarding this
    // write on `io.get_len()` silently disabled it — `LocalStorageProvider`
    // tracks length in an atomic that `write_data` does not update, so a
    // sidecar written via `write_data` reports length 0 until reopened. The
    // result was a stale checkpoint loading happily and pruning a row that
    // matched: a false negative.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("flt_dirty_flt.db");

    let table = open_table(dir.path(), "flt_dirty").await;
    fill(&table).await;
    table.checkpoint_filters().await;
    assert_eq!(std::fs::read(&path).unwrap()[13], 0, "fresh checkpoint should be clean");

    table
        .delete_rows("id = 42", full_fetch(&table.schema.fields), None)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap()[13],
        1,
        "a delete must mark the on-disk checkpoint stale"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn maintenance_runs_without_any_explicit_checkpoint() {
    // The service does the work: no caller ever calls checkpoint_filters().
    crate::core::filter::settings::settings().set_checkpoint_interval_secs(1);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("flt_svc_flt.db");
    {
        let table = open_table(dir.path(), "flt_svc").await;
        fill(&table).await;
        table
            .delete_rows("id = 42", full_fetch(&table.schema.fields), None)
            .await
            .unwrap();
        // The delete requested a checkpoint; the background task should honour
        // it without anyone asking again.
        // A generous deadline, not a tight one: this waits on a background
        // timer, and machine load can starve it for far longer than the
        // interval without anything being wrong.
        for _ in 0..600 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if std::fs::read(&path).map(|b| b.len() > 64 && b[13] == 0).unwrap_or(false) {
                break;
            }
        }
        let bytes = std::fs::read(&path).expect("maintenance task never wrote a sidecar");
        assert!(bytes.len() > 64, "sidecar too small: {}", bytes.len());
        assert_eq!(bytes[13], 0, "maintenance task left the sidecar dirty");
    }

    let table = open_table(dir.path(), "flt_svc").await;
    table.wait_filters_ready().await;
    let rows = run(&table, "id = 42").await;
    assert!(rows.is_empty(), "deleted row came back");
    let rows = run(&table, "id = 43").await;
    assert_eq!(ids_of(&rows), vec![43]);
}

// ---------------------------------------------------------------------------
// IN / BETWEEN, as `predicate_rewrite` desugars them
// ---------------------------------------------------------------------------

/// Run a query the way the executor does: desugar first, then evaluate.
async fn run_rewritten(table: &Table<LocalStorageProvider>, clause: &str) -> Vec<Row> {
    let rewritten =
        crate::core::tokenizer::predicate_rewrite::rewrite_where(clause, &table.schema.fields)
            .unwrap_or_else(|e| panic!("rewrite of `{}` failed: {}", clause, e));
    run(table, &rewritten).await
}

#[tokio::test(flavor = "multi_thread")]
async fn in_lists_are_exact_and_filtered() {
    let (_dir, table) = build_table("flt_in").await;

    let rows = run_rewritten(&table, "id IN (5, 9, 1999)").await;
    assert_eq!(ids_of(&rows), vec![5, 9, 1999]);

    let rows = run_rewritten(&table, "id IN (100000, 200000)").await;
    assert!(rows.is_empty());

    // The desugared form is `(id = 5 OR id = 9 OR id = 1999)`; parenthesised, so
    // it used to be refused outright and scanned in full.
    let filters = table.filters.as_ref().unwrap();
    let rewritten = crate::core::tokenizer::predicate_rewrite::rewrite_where(
        "id IN (5, 9, 1999)",
        &table.schema.fields,
    )
    .unwrap();
    let program = super::sarg::extract(
        &crate::core::tokenizer::query_tokenizer::tokenize(&rewritten, &table.schema.fields),
        filters,
    )
    .expect("desugared IN must be sargable");
    let bm = filters.evaluate(&program, N).expect("bitmap");
    for id in [5u64, 9, 1999] {
        assert!(bm.get(id), "IN member {} pruned", id);
    }
    assert!(bm.count() < N / 10, "IN barely pruned: {} of {}", bm.count(), N);
}

#[tokio::test(flavor = "multi_thread")]
async fn between_is_exact_and_filtered() {
    let (_dir, table) = build_table("flt_between").await;

    let rows = run_rewritten(&table, "id BETWEEN 10 AND 20").await;
    assert_eq!(ids_of(&rows), (10..=20).collect::<Vec<_>>());

    let rows = run_rewritten(&table, "id BETWEEN 1995 AND 5000").await;
    assert_eq!(ids_of(&rows), (1995..N).collect::<Vec<_>>());

    // Desugars to `(id >= lo AND id <= hi)` — two range predicates the zone
    // bounds can act on.
    let filters = table.filters.as_ref().unwrap();
    let rewritten = crate::core::tokenizer::predicate_rewrite::rewrite_where(
        "id BETWEEN 10 AND 20",
        &table.schema.fields,
    )
    .unwrap();
    let program = super::sarg::extract(
        &crate::core::tokenizer::query_tokenizer::tokenize(&rewritten, &table.schema.fields),
        filters,
    )
    .expect("desugared BETWEEN must be sargable");
    let bm = filters.evaluate(&program, N).expect("bitmap");
    for id in 10u64..=20 {
        assert!(bm.get(id), "BETWEEN member {} pruned", id);
    }
    assert!(bm.count() < N / 2, "BETWEEN barely pruned: {}", bm.count());
}

#[tokio::test(flavor = "multi_thread")]
async fn in_combined_with_another_predicate_is_exact() {
    // `WHERE a = ? AND b IN (?, ?)` is everyday ORM traffic, and the shape that
    // needs real grouping rather than just stripping outer parens.
    let (_dir, table) = build_table("flt_in_and").await;

    // The evaluator honours the grouping, so both operand orders give the same
    // SQL-correct answer. It previously flattened to
    // `(cat = 4 AND id = 4) OR id = 68 OR id = 5`, which made the result depend
    // on which side the OR group sat.
    //
    // `cat = id % 64`, so of 4, 68 and 5 only 4 and 68 have `cat = 4`.
    let rows = run_rewritten(&table, "cat = 4 AND id IN (4, 68, 5)").await;
    assert_eq!(ids_of(&rows), vec![4, 68]);

    let rows = run_rewritten(&table, "id IN (4, 68, 5) AND cat = 4").await;
    assert_eq!(ids_of(&rows), vec![4, 68], "operand order must not change the answer");
}

#[tokio::test(flavor = "multi_thread")]
async fn negated_forms_are_exact() {
    let (_dir, table) = build_table("flt_not").await;

    // `NOT BETWEEN` desugars to `(x < lo OR x > hi)` — a genuine disjunction of
    // ranges, so it IS filterable.
    let rows = run_rewritten(&table, "id NOT BETWEEN 10 AND 20").await;
    assert_eq!(ids_of(&rows), (0..N).filter(|i| !(10..=20).contains(i)).collect::<Vec<_>>());

    // `NOT IN` desugars to a conjunction of inequalities. `!=` is not sargable —
    // probe(v) over-approximates `= v`, so its complement UNDER-approximates and
    // would drop real rows — so extraction must decline rather than narrow.
    let rows = run_rewritten(&table, "id NOT IN (5, 9, 1999)").await;
    assert_eq!(
        ids_of(&rows),
        (0..N).filter(|i| ![5, 9, 1999].contains(i)).collect::<Vec<_>>()
    );

    // Mixed: the filterable half still applies, the opaque half contributes
    // nothing. `A AND unknown` -> mask(A) is a valid superset.
    let rows = run_rewritten(&table, "cat = 4 AND id NOT IN (4, 68)").await;
    let expected: Vec<u64> = (0..N)
        .filter(|i| (i % 64) as i32 == 4 && ![4, 68].contains(i))
        .collect();
    assert_eq!(ids_of(&rows), expected);

    // `NOT BETWEEN` desugars to an OR group, which the evaluator now keeps
    // bracketed: `cat = 4 AND (id < 10 OR id > 200)`.
    let rows = run_rewritten(&table, "cat = 4 AND id NOT BETWEEN 10 AND 200").await;
    let expected: Vec<u64> = (0..N)
        .filter(|i| (i % 64) as i32 == 4 && (*i < 10 || *i > 200))
        .collect();
    assert_eq!(ids_of(&rows), expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn not_in_declines_rather_than_narrowing() {
    // The soundness point, asserted on the mask itself rather than only on the
    // result set: a conjunction of inequalities must yield no program at all.
    let (_dir, table) = build_table("flt_notin_mask").await;
    let filters = table.filters.as_ref().unwrap();

    let rewritten = crate::core::tokenizer::predicate_rewrite::rewrite_where(
        "id NOT IN (5, 9)",
        &table.schema.fields,
    )
    .unwrap();
    let toks = crate::core::tokenizer::query_tokenizer::tokenize(&rewritten, &table.schema.fields);
    assert!(
        super::sarg::extract(&toks, filters).is_none(),
        "NOT IN must decline; rewritten form was `{}`",
        rewritten
    );

    // NOT BETWEEN, by contrast, is two ranges and should be filterable.
    let rewritten = crate::core::tokenizer::predicate_rewrite::rewrite_where(
        "id NOT BETWEEN 10 AND 20",
        &table.schema.fields,
    )
    .unwrap();
    let toks = crate::core::tokenizer::query_tokenizer::tokenize(&rewritten, &table.schema.fields);
    let p = super::sarg::extract(&toks, filters).expect("NOT BETWEEN should be sargable");
    assert_eq!(p.all_preds().len(), 2);
}
