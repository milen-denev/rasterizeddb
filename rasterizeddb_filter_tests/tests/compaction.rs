//! Compaction versus the filter index.
//!
//! The filter is a **positional shadow** of the pointer file — bit `i` describes
//! slot `i` — so it rests on one assumption: a row keeps its slot for life.
//! Compaction is the operation that breaks it. It moves live records down over
//! tombstones, so after a run a slot that held one row holds a different one,
//! and every fingerprint below the compaction point describes the wrong row.
//!
//! `Table::compact` handles this by marking the on-disk checkpoint dirty before
//! anything moves, then rebuilding the in-memory catalog from scratch and
//! re-checkpointing afterwards. These tests hold it to that, because the
//! failure mode is silent: a stale index prunes rows that match.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::row::row::{ColumnWritePayload, RowWrite};
use rasterizeddb_filter_tests::*;
use smallvec::SmallVec;

const N: u64 = STD_N;

fn upd(p: Vec<ColumnWritePayload>) -> SmallVec<[ColumnWritePayload; 32]> {
    p.into_iter().collect()
}

/// `total` is how many rows should remain; `sample` are ids to probe.
///
/// The two are separate because probing every survivor of a 2000-row table is
/// slow, but the row count must still be checked against the whole table rather
/// than against the sample.
async fn assert_all_findable(
    t: &rasterizeddb_core::core::row::table::Table<
        rasterizeddb_core::core::storage_providers::file_sync::LocalStorageProvider,
    >,
    total: usize,
    sample: &[u64],
) {
    assert_eq!(run(t, "").await.len(), total, "row count");
    for &id in sample {
        assert_eq!(
            u64s(&run(t, &format!("id = {}", id)).await, 0),
            vec![id],
            "id {} not findable after compaction",
            id
        );
        assert_eq!(
            u64s(&run(t, &format!("label = 'l{}'", id)).await, 0),
            vec![id],
            "id {} not findable by label after compaction",
            id
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_after_deletes_keeps_every_survivor_findable() {
    let (_d, t) = std_table("cmp_deletes").await;

    // Remove a quarter of the rows, scattered so tombstones are interleaved.
    let doomed: Vec<u64> = (0..N).filter(|i| i % 4 == 1).collect();
    for id in &doomed {
        t.delete_rows(&format!("id = {}", id), full_fetch(&t.schema.fields), None)
            .await
            .unwrap();
    }

    let report = t.compact().await.expect("compaction should succeed");
    assert!(report.dead_rows > 0, "expected tombstones to reclaim");
    assert_eq!(report.live_rows, N - doomed.len() as u64);

    let survivors: Vec<u64> = (0..N).filter(|i| i % 4 != 1).collect();
    let sample: Vec<u64> = survivors.iter().copied().step_by(17).collect();
    assert_all_findable(&t, survivors.len(), &sample).await;

    // And the deleted ones stay gone.
    for id in doomed.iter().take(40) {
        assert!(
            run(&t, &format!("id = {}", id)).await.is_empty(),
            "deleted id {} came back after compaction",
            id
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_with_nothing_to_reclaim_is_harmless() {
    let (_d, t) = std_table("cmp_noop").await;
    let report = t.compact().await.expect("compaction should succeed");
    assert_eq!(report.dead_rows, 0);
    assert_eq!(report.live_rows, N);
    assert_all_findable(&t, N as usize, &(0..N).step_by(37).collect::<Vec<_>>()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ranges_are_still_exact_after_compaction() {
    // Zone bounds are rebuilt too; a stale bound would prune real matches.
    let (_d, t) = std_table("cmp_ranges").await;
    for id in (0..N).filter(|i| i % 3 == 0) {
        t.delete_rows(&format!("id = {}", id), full_fetch(&t.schema.fields), None)
            .await
            .unwrap();
    }
    t.compact().await.unwrap();

    let survivors: Vec<u64> = (0..N).filter(|i| i % 3 != 0).collect();
    for pivot in [0u64, 7, 500, 1500, 1998] {
        let got = u64s(&run(&t, &format!("id > {}", pivot)).await, 0);
        let want: Vec<u64> = survivors.iter().copied().filter(|i| *i > pivot).collect();
        assert_eq!(got, want, "id > {} after compaction", pivot);
    }
    let got = u64s(&run(&t, "id < 20").await, 0);
    let want: Vec<u64> = survivors.iter().copied().filter(|i| *i < 20).collect();
    assert_eq!(got, want);
}

#[tokio::test(flavor = "multi_thread")]
async fn inserting_after_compaction_lands_at_the_new_frontier() {
    let (_d, t) = std_table("cmp_then_insert").await;
    for id in (0..N).filter(|i| i % 5 == 2) {
        t.delete_rows(&format!("id = {}", id), full_fetch(&t.schema.fields), None)
            .await
            .unwrap();
    }
    t.compact().await.unwrap();

    for i in N..N + 200 {
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

    let mut survivors: Vec<u64> = (0..N).filter(|i| i % 5 != 2).collect();
    survivors.extend(N..N + 200);
    assert_eq!(run(&t, "").await.len(), survivors.len());
    for id in [0u64, 1, N - 1, N, N + 100, N + 199] {
        if survivors.contains(&id) {
            assert_eq!(u64s(&run(&t, &format!("id = {}", id)).await, 0), vec![id], "id {}", id);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_after_relocating_updates() {
    // A growing update tombstones one slot and appends another, so the table is
    // fragmented in both files at once.
    let (_d, t) = std_table("cmp_relocated").await;

    for id in (0..N).step_by(50) {
        let long = format!("l{}{}", id, "x".repeat(300));
        t.update_rows(
            &format!("id = {}", id),
            full_fetch(&t.schema.fields),
            upd(vec![payload(long.as_bytes(), 2, DbType::STRING)]),
            None,
        )
        .await
        .unwrap();
    }

    let report = t.compact().await.unwrap();
    assert_eq!(report.live_rows, N);
    assert_eq!(run(&t, "").await.len() as u64, N);

    for id in (0..N).step_by(50) {
        let long = format!("l{}{}", id, "x".repeat(300));
        assert_eq!(
            u64s(&run(&t, &format!("label = '{}'", long)).await, 0),
            vec![id],
            "relocated row {} lost its label after compaction",
            id
        );
        assert_eq!(
            u64s(&run(&t, &format!("id = {}", id)).await, 0),
            vec![id],
            "relocated row {} lost by id after compaction",
            id
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_compactions_converge() {
    let (_d, t) = std_table("cmp_repeat").await;
    for id in (0..N).filter(|i| i % 7 == 0) {
        t.delete_rows(&format!("id = {}", id), full_fetch(&t.schema.fields), None)
            .await
            .unwrap();
    }

    let first = t.compact().await.unwrap();
    assert!(first.dead_rows > 0);

    // A second run has nothing left to do, and must not disturb anything.
    let second = t.compact().await.unwrap();
    assert_eq!(second.dead_rows, 0, "a compacted table should have no tombstones left");
    assert_eq!(second.slots_moved, 0, "nothing should move on a compacted table");

    let survivors: Vec<u64> = (0..N).filter(|i| i % 7 != 0).collect();
    let sample: Vec<u64> = survivors.iter().copied().step_by(23).collect();
    assert_all_findable(&t, survivors.len(), &sample).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_checkpoint_from_before_compaction_is_rejected_on_reopen() {
    // The sharpest case: a sidecar written against the old slot layout would
    // confidently index rows by their former addresses. It must be discarded,
    // not trusted.
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "cmp_stale_cp", &std_columns()).await;
        fill_std(&t).await;
        t.checkpoint_filters().await;

        for id in (0..N).filter(|i| i % 4 == 3) {
            t.delete_rows(&format!("id = {}", id), full_fetch(&t.schema.fields), None)
                .await
                .unwrap();
        }
        t.compact().await.unwrap();
    }

    let t = open_table(dir.path(), "cmp_stale_cp", &std_columns()).await;
    t.wait_filters_ready().await;

    let survivors: Vec<u64> = (0..N).filter(|i| i % 4 != 3).collect();
    assert_eq!(run(&t, "").await.len(), survivors.len());
    for id in survivors.iter().copied().step_by(29) {
        assert_eq!(
            u64s(&run(&t, &format!("id = {}", id)).await, 0),
            vec![id],
            "id {} lost after compaction + reopen",
            id
        );
    }
    for id in (0..N).filter(|i| i % 4 == 3).take(30) {
        assert!(
            run(&t, &format!("id = {}", id)).await.is_empty(),
            "deleted id {} came back after compaction + reopen",
            id
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_leaves_the_filter_complete_and_checkpointed() {
    let (_d, t) = std_table("cmp_state").await;
    for id in (0..N).filter(|i| i % 6 == 4) {
        t.delete_rows(&format!("id = {}", id), full_fetch(&t.schema.fields), None)
            .await
            .unwrap();
    }
    t.compact().await.unwrap();

    assert!(t.filters_ready(), "compaction must leave the filter ready");
    let f = t.filters.as_ref().expect("filters active");
    assert!(f.is_complete(), "compaction must leave no unindexed prefix");

    // The rebuilt catalog still prunes, rather than degrading to all-candidates.
    let program = rasterizeddb_core::core::filter::sarg::extract(
        &rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize(
            "id = 1234",
            &t.schema.fields,
        ),
        f,
    )
    .expect("sargable");
    let n = (0..N).filter(|i| i % 6 != 4).count() as u64;
    let bm = f.evaluate(&program, n).expect("should still prune after compaction");
    assert!(bm.count() < n / 4, "rebuilt filter barely prunes: {} of {}", bm.count(), n);
}
