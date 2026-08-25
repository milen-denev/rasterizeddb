//! Concurrent writers and readers.
//!
//! The filter's central safety property is that a scanner never prunes a row a
//! writer has already committed. The frontier exists precisely for this: slots
//! at or above it are unconditional candidates, so an insert that is still in
//! flight costs selectivity rather than correctness.
//!
//! These tests hammer that boundary. A gap in the frontier is allowed; a lost
//! row is not.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::filter::sarg;
use rasterizeddb_core::core::filter::{FilterConfig, TableFilters};
use rasterizeddb_core::core::row::row::RowWrite;
use rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize;
use rasterizeddb_filter_tests::*;
use smallvec::SmallVec;
use std::sync::Arc as StdArc;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_inserts_are_all_findable() {
    let dir = tempfile::tempdir().unwrap();
    let cols = vec![("id", DbType::U64), ("cat", DbType::I32)];
    let t = StdArc::new(open_table(dir.path(), "conc_insert", &cols).await);

    let per_task = 250u64;
    let tasks = 8u64;
    let mut handles = Vec::new();
    for task in 0..tasks {
        let t = t.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..per_task {
                let id = task * per_task + i;
                let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
                rw.columns_writing_data.push(payload(&id.to_le_bytes(), 0, DbType::U64));
                rw.columns_writing_data
                    .push(payload(&((id % 64) as i32).to_le_bytes(), 1, DbType::I32));
                t.insert_row(rw).await.unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let total = tasks * per_task;
    assert_eq!(run(&t, "").await.len() as u64, total, "rows went missing");

    // Every id written by any task must still be findable through the filter.
    for id in 0..total {
        let rows = run(&t, &format!("id = {}", id)).await;
        assert_eq!(u64s(&rows, 0), vec![id], "id {} not findable after concurrent inserts", id);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn queries_running_against_concurrent_inserts_never_lose_committed_rows() {
    let dir = tempfile::tempdir().unwrap();
    let cols = vec![("id", DbType::U64), ("cat", DbType::I32)];
    let t = StdArc::new(open_table(dir.path(), "conc_mixed", &cols).await);

    // Seed so there is something to find from the start.
    for id in 0..300u64 {
        let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
        rw.columns_writing_data.push(payload(&id.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data
            .push(payload(&((id % 64) as i32).to_le_bytes(), 1, DbType::I32));
        t.insert_row(rw).await.unwrap();
    }

    let writer = {
        let t = t.clone();
        tokio::spawn(async move {
            for id in 300..1500u64 {
                let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
                rw.columns_writing_data.push(payload(&id.to_le_bytes(), 0, DbType::U64));
                rw.columns_writing_data
                    .push(payload(&((id % 64) as i32).to_le_bytes(), 1, DbType::I32));
                t.insert_row(rw).await.unwrap();
            }
        })
    };

    // While the writer runs, repeatedly look for rows known to be committed.
    let readers: Vec<_> = (0..4)
        .map(|r| {
            let t = t.clone();
            tokio::spawn(async move {
                for round in 0..60u64 {
                    let id = (r * 37 + round) % 300;
                    let rows = run(&t, &format!("id = {}", id)).await;
                    assert_eq!(
                        u64s(&rows, 0),
                        vec![id],
                        "committed row {} vanished during concurrent inserts",
                        id
                    );
                }
            })
        })
        .collect();

    writer.await.unwrap();
    for h in readers {
        h.await.unwrap();
    }

    assert_eq!(run(&t, "").await.len(), 1500);
    for id in [0u64, 299, 300, 900, 1499] {
        assert_eq!(u64s(&run(&t, &format!("id = {}", id)).await, 0), vec![id]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_deletes_and_queries_stay_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let cols = vec![("id", DbType::U64), ("cat", DbType::I32)];
    let t = StdArc::new(open_table(dir.path(), "conc_delete", &cols).await);

    for id in 0..800u64 {
        let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
        rw.columns_writing_data.push(payload(&id.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data
            .push(payload(&((id % 64) as i32).to_le_bytes(), 1, DbType::I32));
        t.insert_row(rw).await.unwrap();
    }

    // Delete the odd half while reading the even half.
    let deleter = {
        let t = t.clone();
        tokio::spawn(async move {
            for id in (1..800u64).step_by(2) {
                t.delete_rows(
                    &format!("id = {}", id),
                    full_fetch(&t.schema.fields),
                    None,
                )
                .await
                .unwrap();
            }
        })
    };
    let reader = {
        let t = t.clone();
        tokio::spawn(async move {
            for id in (0..800u64).step_by(2).take(80) {
                let rows = run(&t, &format!("id = {}", id)).await;
                assert_eq!(u64s(&rows, 0), vec![id], "surviving row {} vanished", id);
            }
        })
    };

    deleter.await.unwrap();
    reader.await.unwrap();

    // Every even id survives, every odd id is gone.
    for id in (0..800u64).step_by(2) {
        assert_eq!(u64s(&run(&t, &format!("id = {}", id)).await, 0), vec![id]);
    }
    for id in (1..800u64).step_by(2) {
        assert!(run(&t, &format!("id = {}", id)).await.is_empty(), "id {} survived", id);
    }
    assert_eq!(run(&t, "").await.len(), 400);
}

#[test]
fn concurrent_probes_against_a_shared_catalog_agree() {
    // `TableFilters` is shared across worker threads; a probe must not observe
    // a torn view of a column's planes.
    let fields = fields_of(&[("id", DbType::U64)]);
    let f = StdArc::new(TableFilters::new(&fields, FilterConfig::default()));

    let n = 8000u64;
    let vals: StdArc<Vec<u64>> = StdArc::new((0..n).map(|_| fastrand::u64(..)).collect());
    for slot in 0..n {
        let sv: SmallVec<[(u32, Vec<u8>); 32]> =
            smallvec::smallvec![(0u32, vals[slot as usize].to_le_bytes().to_vec())];
        f.backfill_row(slot, &sv);
    }

    let mut handles = Vec::new();
    for t in 0..8u64 {
        let f = f.clone();
        let vals = vals.clone();
        let fields = fields.clone();
        handles.push(std::thread::spawn(move || {
            for k in 0..400u64 {
                let slot = (t * 997 + k * 13) % n;
                let q = format!("id = {}", vals[slot as usize]);
                let p = sarg::extract(&tokenize(&q, &fields), &f).expect("sargable");
                let bm = f.evaluate(&p, n).expect("bitmap");
                assert!(bm.get(slot), "thread {}: slot {} lost", t, slot);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn concurrent_writes_and_probes_never_lose_an_indexed_row() {
    // Writers and readers on the same catalog. A reader may see a row as "not
    // yet indexed" (a candidate), but must never see it as "definitely absent".
    let fields = fields_of(&[("id", DbType::U64)]);
    let f = StdArc::new(TableFilters::new(&fields, FilterConfig::default()));
    let n = 6000u64;

    let writer = {
        let f = f.clone();
        std::thread::spawn(move || {
            for slot in 0..n {
                let sv: SmallVec<[(u32, Vec<u8>); 32]> =
                    smallvec::smallvec![(0u32, slot.to_le_bytes().to_vec())];
                f.backfill_row(slot, &sv);
            }
        })
    };

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let f = f.clone();
            let fields = fields.clone();
            std::thread::spawn(move || {
                for _ in 0..500 {
                    let slot = fastrand::u64(0..n);
                    let q = format!("id = {}", slot);
                    if let Some(p) = sarg::extract(&tokenize(&q, &fields), &f) {
                        if let Some(bm) = f.evaluate(&p, n) {
                            // The row is either not yet written (candidate) or
                            // written (must be a candidate). Either way its bit
                            // must be set.
                            assert!(bm.get(slot), "slot {} pruned mid-write", slot);
                        }
                    }
                }
            })
        })
        .collect();

    writer.join().unwrap();
    for h in readers {
        h.join().unwrap();
    }

    // After the writer finishes, everything is findable.
    for slot in (0..n).step_by(7) {
        let q = format!("id = {}", slot);
        let p = sarg::extract(&tokenize(&q, &fields), &f).unwrap();
        let bm = f.evaluate(&p, n).unwrap();
        assert!(bm.get(slot), "slot {} lost after concurrent fill", slot);
    }
}
