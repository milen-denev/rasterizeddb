//! The background maintenance service.
//!
//! Filter maintenance on the write path is unavoidable — data has to be indexed
//! as it arrives. Everything else runs on a per-table task: the initial
//! backfill. No caller should ever pay for it, and the task must not outlive
//! its table. (The runtime checkpoint the task also ran was retired with
//! `_flt.db` in persistent indexes B14.)

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::row::row::RowWrite;
use rasterizeddb_filter_tests::*;
use smallvec::SmallVec;
use std::time::Duration;

const N: u64 = STD_N;

fn sidecar(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    sidecar_path(dir, name)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fresh_table_is_ready_immediately() {
    // Nothing predates the filter, so there is nothing to backfill.
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "mnt_fresh", &std_columns()).await;
    assert!(t.filters_ready().unwrap(), "a fresh table should need no backfill");
    assert!(t.filters().unwrap().as_ref().unwrap().is_complete());
}

#[tokio::test(flavor = "multi_thread")]
async fn queries_are_correct_before_the_backfill_finishes() {
    // `Table::new` hands back before the scan finishes, so a query can land in
    // the gap. It must be correct there — just not yet narrowed.
    //
    // Deliberately no duration assertion. A wall-clock bound on "opening is
    // fast" can only fail in the false direction: under load the machine can
    // miss any threshold while the code under test is behaving perfectly. What
    // is actually worth asserting is that the answers are right whether or not
    // the backfill has completed, which is a property rather than a stopwatch.
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "mnt_async", &std_columns()).await;
        fill_std(&t).await;
        // Deliberately no checkpoint: the reopen must rebuild.
    }

    let t = open_table(dir.path(), "mnt_async", &std_columns()).await;

    // Before readiness — the backfill may or may not have finished by now, and
    // the answer must not depend on which.
    assert_eq!(u64s(&run(&t, "id = 1234").await, 0), vec![1234]);
    assert_eq!(u64s(&run(&t, "id = 0").await, 0), vec![0]);
    assert_eq!(u64s(&run(&t, "id = 1999").await, 0), vec![1999]);

    // After readiness — same answers, now narrowed.
    t.wait_filters_ready().await.unwrap();
    assert!(t.filters().unwrap().as_ref().unwrap().is_complete());
    assert_eq!(u64s(&run(&t, "id = 1234").await, 0), vec![1234]);
    assert_eq!(run(&t, "").await.len() as u64, N);
}

/// No background checkpoint (persistent indexes B14): with the old
/// interval long past, a delete leaves no sidecar of any kind, and the
/// reopen is still exact.
#[tokio::test(flavor = "multi_thread")]
async fn the_service_writes_no_sidecar() {
    rasterizeddb_core::filter_settings().set_checkpoint_interval_secs(1);
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "mnt_service", &std_columns()).await;
        fill_std(&t).await;
        t.delete_rows("id = 42", full_fetch(&t.schema.fields), None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2_500)).await;
        assert!(!dir.path().join("mnt_service_flt.db").exists(), "the service wrote a sidecar");
    }

    let t = open_table(dir.path(), "mnt_service", &std_columns()).await;
    t.wait_filters_ready().await.unwrap();
    assert!(run(&t, "id = 42").await.is_empty(), "deleted row came back");
    assert_eq!(u64s(&run(&t, "id = 43").await, 0), vec![43]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_checkpoint_taken_during_writes_is_still_loadable() {
    let dir = tempfile::tempdir().unwrap();
    {
        let t = std::sync::Arc::new(
            open_table(dir.path(), "mnt_concurrent_cp", &std_columns()).await,
        );
        fill_std(&t).await;

        let writer = {
            let t = t.clone();
            tokio::spawn(async move {
                for i in N..N + 600 {
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
            })
        };

        // Checkpoint repeatedly while the writer runs. A checkpoint may cover
        // fewer slots than exist, but must never be unloadable.
        for _ in 0..6 {
            t.checkpoint_filters().await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        writer.await.unwrap();
        t.checkpoint_filters().await.unwrap();
    }

    let t = open_table(dir.path(), "mnt_concurrent_cp", &std_columns()).await;
    t.wait_filters_ready().await.unwrap();
    assert_eq!(run(&t, "").await.len() as u64, N + 600);
    for target in [0u64, N - 1, N, N + 599] {
        assert_eq!(u64s(&run(&t, &format!("id = {}", target)).await, 0), vec![target]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_checkpoint_covering_fewer_slots_than_exist_is_safe() {
    // Rows written after a checkpoint sit above its frontier, so a reopen must
    // treat them as candidates and backfill the delta rather than lose them.
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "mnt_partial", &std_columns()).await;
        fill_std(&t).await;
        t.checkpoint_filters().await.unwrap();
        for i in N..N + 900 {
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

    let t = open_table(dir.path(), "mnt_partial", &std_columns()).await;
    t.wait_filters_ready().await.unwrap();
    for target in [0u64, N - 1, N, N + 450, N + 899] {
        assert_eq!(u64s(&run(&t, &format!("id = {}", target)).await, 0), vec![target]);
    }
    assert_eq!(run(&t, "").await.len() as u64, N + 900);
}

#[tokio::test(flavor = "multi_thread")]
async fn many_tables_keep_independent_catalogs() {
    // One process, several tables. A filter must never answer for another.
    let dir = tempfile::tempdir().unwrap();
    let mut tables = Vec::new();
    for k in 0..4u64 {
        let name = format!("mnt_iso_{}", k);
        let t = open_table(dir.path(), &name, &std_columns()).await;
        for i in 0..400u64 {
            // Each table holds a disjoint id range.
            let id = k * 10_000 + i;
            let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
            rw.columns_writing_data.push(payload(&id.to_le_bytes(), 0, DbType::U64));
            rw.columns_writing_data
                .push(payload(&(k as i32).to_le_bytes(), 1, DbType::I32));
            rw.columns_writing_data
                .push(payload(format!("t{}r{}", k, i).as_bytes(), 2, DbType::STRING));
            rw.columns_writing_data.push(payload(&(k as f64).to_le_bytes(), 3, DbType::F64));
            t.insert_row(rw).await.unwrap();
        }
        tables.push(t);
    }

    for (k, t) in tables.iter().enumerate() {
        let k = k as u64;
        assert_eq!(run(t, "").await.len(), 400, "table {}", k);
        // Its own rows resolve.
        for i in [0u64, 200, 399] {
            let id = k * 10_000 + i;
            assert_eq!(u64s(&run(t, &format!("id = {}", id)).await, 0), vec![id], "table {}", k);
        }
        // Another table's rows do not.
        for other in 0..4u64 {
            if other == k {
                continue;
            }
            let foreign = other * 10_000 + 100;
            assert!(
                run(t, &format!("id = {}", foreign)).await.is_empty(),
                "table {} answered for table {}'s row",
                k,
                other
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_table_stops_its_maintenance_task() {
    // Without the Drop signal the task would keep ticking against storage that
    // is going away. Observable proxy: the directory can be removed and
    // reopened repeatedly without anything panicking or resurrecting.
    rasterizeddb_core::filter_settings().set_checkpoint_interval_secs(1);
    for round in 0..3u32 {
        let dir = tempfile::tempdir().unwrap();
        {
            let t = open_table(dir.path(), "mnt_drop", &std_columns()).await;
            fill_std(&t).await;
            t.checkpoint_filters().await.unwrap();
            assert_eq!(run(&t, "id = 5").await.len(), 1, "round {}", round);
        }
        // The table is gone; give any surviving task a chance to misbehave.
        tokio::time::sleep(Duration::from_millis(120)).await;
        drop(dir);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_explicit_checkpoint_is_idempotent_and_stays_loadable() {
    let dir = tempfile::tempdir().unwrap();
    let path = sidecar(dir.path(), "mnt_idem");
    let t = open_table(dir.path(), "mnt_idem", &std_columns()).await;
    fill_std(&t).await;

    let mut sizes = Vec::new();
    for _ in 0..5 {
        checkpoint_sidecar(&t, dir.path()).await;
        sizes.push(std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0));
    }
    assert!(sizes.windows(2).all(|w| w[0] == w[1]), "checkpoint size drifted: {:?}", sizes);
    assert_eq!(
        std::fs::read(&path).unwrap()[13],
        0,
        "a checkpoint must leave the sidecar clean"
    );
    assert_eq!(u64s(&run(&t, "id = 1234").await, 0), vec![1234]);
}

#[tokio::test(flavor = "multi_thread")]
async fn backfilling_from_a_mid_table_slot_is_safe() {
    // The delta path: resume from a frontier rather than rescanning.
    let (_d, t) = std_table("mnt_delta_backfill").await;
    for start in [0u64, 1, 511, 512, 1000, N] {
        let indexed = t.backfill_filters_from(start).await.unwrap();
        assert!(indexed <= N, "start={} indexed {}", start, indexed);
        // Correctness is unaffected wherever the scan began.
        for target in [0u64, 700, N - 1] {
            assert_eq!(
                u64s(&run(&t, &format!("id = {}", target)).await, 0),
                vec![target],
                "start={} target={}",
                start,
                target
            );
        }
    }
}
