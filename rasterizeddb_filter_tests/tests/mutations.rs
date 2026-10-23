//! Mutation paths in detail.
//!
//! The one that deserves its own file is the **relocating update**: when a row
//! grows past its slot, the pointer is tombstoned and a fresh one appended, so
//! the row moves to a new slot. Every column has to be re-indexed there —
//! including the ones that did not change — or the row becomes unfindable by
//! anything except the column that was edited.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::row::row::{ColumnWritePayload, RowWrite};
use rasterizeddb_filter_tests::*;
use smallvec::SmallVec;

const N: u64 = STD_N;

fn upd(payloads: Vec<ColumnWritePayload>) -> SmallVec<[ColumnWritePayload; 32]> {
    payloads.into_iter().collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_growing_update_relocates_and_stays_findable_by_every_column() {
    let (_d, t) = std_table("mut_grow").await;

    // "l900" -> a much longer string, so the row cannot fit its old slot.
    let long = "l900".to_string() + &"x".repeat(400);
    let n = t
        .update_rows(
            "id = 900",
            full_fetch(&t.schema.fields),
            upd(vec![payload(long.as_bytes(), 2, DbType::STRING)]),
            None,
        )
        .await
        .unwrap();
    assert_eq!(n, 1);

    // Findable by the edited column at its new home.
    let rows = run(&t, &format!("label = '{}'", long)).await;
    assert_eq!(u64s(&rows, 0), vec![900], "relocated row lost its new label");

    // And by the columns that did NOT change — this is the part a relocation
    // breaks if the new slot is only indexed for the edited column.
    assert_eq!(u64s(&run(&t, "id = 900").await, 0), vec![900], "lost by id after relocating");
    let cat = (900 % 64) as i32;
    assert!(
        u64s(&run(&t, &format!("cat = {}", cat)).await, 0).contains(&900),
        "lost by cat after relocating"
    );
    assert_eq!(
        u64s(&run(&t, "score = 225").await, 0),
        vec![900],
        "lost by score after relocating"
    );

    // The old value is gone, and the row count is unchanged.
    assert!(run(&t, "label = 'l900'").await.is_empty(), "old label still findable");
    assert_eq!(run(&t, "").await.len() as u64, N, "relocation changed the row count");
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_growing_updates_relocate_each_time() {
    let (_d, t) = std_table("mut_grow_repeat").await;
    let mut current = "l500".to_string();

    for round in 1..6usize {
        let next = format!("l500{}", "y".repeat(round * 120));
        t.update_rows(
            &format!("label = '{}'", current),
            full_fetch(&t.schema.fields),
            upd(vec![payload(next.as_bytes(), 2, DbType::STRING)]),
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            u64s(&run(&t, &format!("label = '{}'", next)).await, 0),
            vec![500],
            "round {}: new label not findable",
            round
        );
        assert!(
            run(&t, &format!("label = '{}'", current)).await.is_empty(),
            "round {}: old label still findable",
            round
        );
        assert_eq!(u64s(&run(&t, "id = 500").await, 0), vec![500], "round {}: lost by id", round);
        assert_eq!(run(&t, "").await.len() as u64, N, "round {}: row count changed", round);
        current = next;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_shrinking_update_stays_in_place() {
    let (_d, t) = std_table("mut_shrink").await;
    // First grow so there is room to shrink back into.
    let long = "l700".to_string() + &"z".repeat(300);
    t.update_rows(
        "id = 700",
        full_fetch(&t.schema.fields),
        upd(vec![payload(long.as_bytes(), 2, DbType::STRING)]),
        None,
    )
    .await
    .unwrap();

    t.update_rows(
        "id = 700",
        full_fetch(&t.schema.fields),
        upd(vec![payload(b"tiny", 2, DbType::STRING)]),
        None,
    )
    .await
    .unwrap();

    assert_eq!(u64s(&run(&t, "label = 'tiny'").await, 0), vec![700]);
    assert!(run(&t, &format!("label = '{}'", long)).await.is_empty());
    assert_eq!(u64s(&run(&t, "id = 700").await, 0), vec![700]);
    assert_eq!(run(&t, "").await.len() as u64, N);
}

#[tokio::test(flavor = "multi_thread")]
async fn updating_several_columns_at_once_reindexes_all_of_them() {
    let (_d, t) = std_table("mut_multi").await;
    t.update_rows(
        "id = 300",
        full_fetch(&t.schema.fields),
        upd(vec![
            payload(&777_777u64.to_le_bytes(), 0, DbType::U64),
            payload(&(-5i32).to_le_bytes(), 1, DbType::I32),
            payload(b"renamed", 2, DbType::STRING),
        ]),
        None,
    )
    .await
    .unwrap();

    assert_eq!(run(&t, "id = 777777").await.len(), 1);
    assert_eq!(run(&t, "label = 'renamed'").await.len(), 1);
    assert_eq!(run(&t, "cat = -5").await.len(), 1);

    // Every old value is gone.
    assert!(run(&t, "id = 300").await.is_empty());
    assert!(run(&t, "label = 'l300'").await.is_empty());
    let old_cat = (300 % 64) as i32;
    assert!(!u64s(&run(&t, &format!("cat = {}", old_cat)).await, 0).contains(&300));
    assert_eq!(run(&t, "").await.len() as u64, N);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bulk_update_touching_many_rows_reindexes_all_of_them() {
    let (_d, t) = std_table("mut_bulk").await;
    let touched = t
        .update_rows(
            "cat = 7",
            full_fetch(&t.schema.fields),
            upd(vec![payload(&(-1i32).to_le_bytes(), 1, DbType::I32)]),
            None,
        )
        .await
        .unwrap();
    let expected = (0..N).filter(|i| (i % 64) as i32 == 7).count() as u64;
    assert_eq!(touched, expected);

    assert!(run(&t, "cat = 7").await.is_empty(), "old category still matches");
    assert_eq!(run(&t, "cat = -1").await.len() as u64, expected);
    assert_eq!(run(&t, "").await.len() as u64, N);

    // The untouched columns of every moved row must still resolve.
    for id in (0..N).filter(|i| (i % 64) as i32 == 7).take(12) {
        assert_eq!(u64s(&run(&t, &format!("id = {}", id)).await, 0), vec![id]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_then_reinsert_the_same_value() {
    // Slots are append-only, so the reinserted row lands at a new slot while
    // the old one keeps its tombstone.
    let (_d, t) = std_table("mut_del_reinsert").await;

    t.delete_rows("id = 1000", full_fetch(&t.schema.fields), None).await.unwrap();
    assert!(run(&t, "id = 1000").await.is_empty());

    let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
    rw.columns_writing_data.push(payload(&1000u64.to_le_bytes(), 0, DbType::U64));
    rw.columns_writing_data.push(payload(&8i32.to_le_bytes(), 1, DbType::I32));
    rw.columns_writing_data.push(payload(b"reborn", 2, DbType::STRING));
    rw.columns_writing_data.push(payload(&1.5f64.to_le_bytes(), 3, DbType::F64));
    t.insert_row(rw).await.unwrap();

    assert_eq!(u64s(&run(&t, "id = 1000").await, 0), vec![1000], "reinserted row not findable");
    assert_eq!(u64s(&run(&t, "label = 'reborn'").await, 0), vec![1000]);
    assert!(run(&t, "label = 'l1000'").await.is_empty(), "deleted row's label came back");
    assert_eq!(run(&t, "").await.len() as u64, N);
}

#[tokio::test(flavor = "multi_thread")]
async fn interleaved_mutations_stay_consistent() {
    let (_d, t) = std_table("mut_interleaved").await;
    let mut expected_rows = N;

    for round in 0..12u64 {
        let victim = round * 37 + 5;
        t.delete_rows(
            &format!("id = {}", victim),
            full_fetch(&t.schema.fields),
            None,
        )
        .await
        .unwrap();
        expected_rows -= 1;

        let new_id = 100_000 + round;
        let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
        rw.columns_writing_data.push(payload(&new_id.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data.push(payload(&9i32.to_le_bytes(), 1, DbType::I32));
        rw.columns_writing_data
            .push(payload(format!("n{}", round).as_bytes(), 2, DbType::STRING));
        rw.columns_writing_data.push(payload(&0.5f64.to_le_bytes(), 3, DbType::F64));
        t.insert_row(rw).await.unwrap();
        expected_rows += 1;

        let target = round * 41 + 11;
        t.update_rows(
            &format!("id = {}", target),
            full_fetch(&t.schema.fields),
            upd(vec![payload(
                format!("u{}{}", round, "w".repeat(200)).as_bytes(),
                2,
                DbType::STRING,
            )]),
            None,
        )
        .await
        .unwrap();

        assert!(run(&t, &format!("id = {}", victim)).await.is_empty(), "round {}", round);
        assert_eq!(
            u64s(&run(&t, &format!("id = {}", new_id)).await, 0),
            vec![new_id],
            "round {}",
            round
        );
        assert_eq!(
            u64s(&run(&t, &format!("id = {}", target)).await, 0),
            vec![target],
            "round {}: relocated row lost by id",
            round
        );
        assert_eq!(run(&t, "").await.len() as u64, expected_rows, "round {}", round);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mutations_survive_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "mut_reopen", &std_columns()).await;
        fill_std(&t).await;
        t.delete_rows("id = 111", full_fetch(&t.schema.fields), None).await.unwrap();
        t.update_rows(
            "id = 222",
            full_fetch(&t.schema.fields),
            upd(vec![payload(
                &("l222".to_string() + &"q".repeat(300)).into_bytes(),
                2,
                DbType::STRING,
            )]),
            None,
        )
        .await
        .unwrap();
        t.checkpoint_filters().await.unwrap();
    }

    let t = open_table(dir.path(), "mut_reopen", &std_columns()).await;
    t.wait_filters_ready().await.unwrap();

    assert!(run(&t, "id = 111").await.is_empty(), "deleted row came back");
    assert_eq!(u64s(&run(&t, "id = 222").await, 0), vec![222], "relocated row lost after reopen");
    assert!(run(&t, "label = 'l222'").await.is_empty(), "stale label findable after reopen");
    assert_eq!(run(&t, "").await.len() as u64, N - 1);
}
