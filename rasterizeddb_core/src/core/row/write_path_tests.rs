//! Probe tests for the DELETE / UPDATE write path.

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

fn init() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(512);
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
}

fn payload(bytes: &[u8], write_order: u32, column_type: DbType) -> ColumnWritePayload {
    let mut mb = MEMORY_POOL.acquire(bytes.len());
    mb.into_slice_mut().copy_from_slice(bytes);
    let size = column_type.get_size();
    ColumnWritePayload {
        data: mb,
        write_order,
        column_type,
        size,
    }
}

fn full_fetch(fields: &SmallVec<[SchemaField; 20]>) -> RowFetch {
    let calc = SchemaCalculator;
    let mut cols = SmallVec::new();
    for f in fields
        .iter()
        .sorted_by(|a, b| a.write_order.cmp(&b.write_order))
    {
        let (column_offset, schema_id) = calc.calculate_schema_offset(&f.name, fields);
        cols.push(ColumnFetchingData {
            column_offset,
            column_type: f.db_type.clone(),
            size: f.size,
            schema_id,
        });
    }
    RowFetch {
        columns_fetching_data: cols,
    }
}

async fn open_table(dir: &std::path::Path, name: &str) -> Table<LocalStorageProvider> {
    init();
    let io = Arc::new(LocalStorageProvider::new(dir.to_str().unwrap(), Some(name)).await);
    Table::new(
        name,
        io,
        vec![
            CreateColumnData {
                name: "id".into(),
                data_type: DbType::U64,
            },
            CreateColumnData {
                name: "cat".into(),
                data_type: DbType::I32,
            },
            CreateColumnData {
                name: "label".into(),
                data_type: DbType::STRING,
            },
        ],
    )
    .await
}

async fn insert(t: &Table<LocalStorageProvider>, id: u64, cat: i32, label: &str) {
    let mut rw = RowWrite {
        columns_writing_data: SmallVec::new(),
    };
    rw.columns_writing_data
        .push(payload(&id.to_le_bytes(), 0, DbType::U64));
    rw.columns_writing_data
        .push(payload(&cat.to_le_bytes(), 1, DbType::I32));
    rw.columns_writing_data
        .push(payload(label.as_bytes(), 2, DbType::STRING));
    t.insert_row(rw).await.unwrap();
}

async fn query_all(t: &Table<LocalStorageProvider>) -> Vec<Row> {
    run(t, "").await
}

async fn run(t: &Table<LocalStorageProvider>, w: &str) -> Vec<Row> {
    t.query_row(
        w,
        &t.schema.fields.to_vec(),
        full_fetch(&t.schema.fields),
        full_fetch(&t.schema.fields),
        None,
        None,
    )
    .await
    .unwrap()
}

fn u64_of(r: &Row, sid: u64) -> u64 {
    let c = r.columns.iter().find(|c| u64::from(c.schema_id) == sid).unwrap();
    u64::from_le_bytes(c.data.into_slice().try_into().unwrap())
}
fn str_of(r: &Row, sid: u64) -> String {
    let c = r.columns.iter().find(|c| u64::from(c.schema_id) == sid).unwrap();
    String::from_utf8_lossy(c.data.into_slice()).to_string()
}

fn upd(
    fields: &SmallVec<[SchemaField; 20]>,
    name: &str,
    bytes: &[u8],
) -> SmallVec<[ColumnWritePayload; 32]> {
    let f = fields.iter().find(|f| f.name == name).unwrap();
    let mut mb = MEMORY_POOL.acquire(bytes.len());
    mb.into_slice_mut().copy_from_slice(bytes);
    let mut sv: SmallVec<[ColumnWritePayload; 32]> = SmallVec::new();
    sv.push(ColumnWritePayload {
        data: mb,
        write_order: f.write_order as u32,
        column_type: f.db_type.clone(),
        size: f.size,
    });
    sv
}

/// Growing a string relocates the row. Does anything about the row change that
/// shouldn't?
#[tokio::test(flavor = "multi_thread")]
async fn update_growing_string_keeps_row_intact() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_grow").await;
    for i in 0..5u64 {
        insert(&t, i, i as i32, "s").await;
    }
    t.wait_filters_ready().await.unwrap();

    let before = run(&t, "id = 2").await;
    assert_eq!(before.len(), 1);

    let n = t
        .update_rows(
            "id = 2",
            full_fetch(&t.schema.fields),
            upd(&t.schema.fields, "label", b"a-much-longer-label"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(n, 1, "one row updated");

    let after = run(&t, "id = 2").await;
    assert_eq!(after.len(), 1, "exactly one row after a relocating update");
    assert_eq!(str_of(&after[0], 2), "a-much-longer-label");
    assert_eq!(u64_of(&after[0], 0), 2);

    // Other rows untouched.
    let all = run(&t, "").await;
    assert_eq!(all.len(), 5, "row count unchanged");
    let mut ids = all.iter().map(|r| u64_of(r, 0)).collect::<Vec<_>>();
    ids.sort();
    assert_eq!(ids, vec![0, 1, 2, 3, 4]);
}

/// Shrinking writes in place.
#[tokio::test(flavor = "multi_thread")]
async fn update_shrinking_string_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_shrink").await;
    for i in 0..5u64 {
        insert(&t, i, i as i32, "a-fairly-long-initial-label").await;
    }
    t.wait_filters_ready().await.unwrap();

    let n = t
        .update_rows(
            "id = 3",
            full_fetch(&t.schema.fields),
            upd(&t.schema.fields, "label", b"x"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(n, 1);

    let after = run(&t, "id = 3").await;
    assert_eq!(after.len(), 1);
    assert_eq!(str_of(&after[0], 2), "x");

    let others = run(&t, "id = 4").await;
    assert_eq!(str_of(&others[0], 2), "a-fairly-long-initial-label");
}

/// Repeated growing updates on the same row: the pointer relocates each time.
#[tokio::test(flavor = "multi_thread")]
async fn repeated_growing_updates() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_repeat").await;
    for i in 0..3u64 {
        insert(&t, i, i as i32, "s").await;
    }
    t.wait_filters_ready().await.unwrap();

    for k in 1..6 {
        let label = "y".repeat(k * 5);
        let n = t
            .update_rows(
                "id = 1",
                full_fetch(&t.schema.fields),
                upd(&t.schema.fields, "label", label.as_bytes()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(n, 1, "iteration {}", k);
        let rows = run(&t, "id = 1").await;
        assert_eq!(rows.len(), 1, "iteration {} row count", k);
        assert_eq!(str_of(&rows[0], 2), label, "iteration {}", k);
    }
    assert_eq!(run(&t, "").await.len(), 3);
}

/// Delete then reopen: tombstones must survive.
#[tokio::test(flavor = "multi_thread")]
async fn delete_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "wp_del").await;
        for i in 0..10u64 {
            insert(&t, i, i as i32, "s").await;
        }
        t.wait_filters_ready().await.unwrap();
        let n = t
            .delete_rows("id = 4", full_fetch(&t.schema.fields), None)
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(run(&t, "").await.len(), 9);
    }
    let t2 = open_table(dir.path(), "wp_del").await;
    t2.wait_filters_ready().await.unwrap();
    assert_eq!(run(&t2, "").await.len(), 9, "tombstone lost after reopen");
    assert!(run(&t2, "id = 4").await.is_empty());
}

/// A relocating update followed by a reopen: the row must not be duplicated and
/// the next insert must not reuse an id or clobber bytes.
#[tokio::test(flavor = "multi_thread")]
async fn relocating_update_survives_reopen_and_next_insert() {
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "wp_reloc").await;
        for i in 0..5u64 {
            insert(&t, i, i as i32, "s").await;
        }
        t.wait_filters_ready().await.unwrap();
        t.update_rows(
            "id = 0",
            full_fetch(&t.schema.fields),
            upd(&t.schema.fields, "label", b"grown-a-lot-here"),
            None,
        )
        .await
        .unwrap();
    }
    let t2 = open_table(dir.path(), "wp_reloc").await;
    t2.wait_filters_ready().await.unwrap();
    assert_eq!(
        run(&t2, "").await.len(),
        5,
        "duplicate or lost row after reopen"
    );
    assert_eq!(str_of(&run(&t2, "id = 0").await[0], 2), "grown-a-lot-here");

    insert(&t2, 99, 99, "new-row-after-reopen").await;
    let all = run(&t2, "").await;
    assert_eq!(all.len(), 6);
    assert_eq!(
        str_of(&run(&t2, "id = 99").await[0], 2),
        "new-row-after-reopen"
    );
    // And the relocated row must still read back correctly.
    assert_eq!(str_of(&run(&t2, "id = 0").await[0], 2), "grown-a-lot-here");
}

/// Update rows matched by a predicate that the update itself invalidates.
#[tokio::test(flavor = "multi_thread")]
async fn update_all_rows_of_a_category() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_cat").await;
    for i in 0..20u64 {
        insert(&t, i, (i % 2) as i32, "s").await;
    }
    t.wait_filters_ready().await.unwrap();

    let n = t
        .update_rows(
            "cat = 1",
            full_fetch(&t.schema.fields),
            upd(&t.schema.fields, "cat", &7i32.to_le_bytes()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(n, 10);
    assert_eq!(run(&t, "cat = 7").await.len(), 10);
    assert!(run(&t, "cat = 1").await.is_empty());
    assert_eq!(run(&t, "").await.len(), 20);
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

use crate::core::row::row_pointer::{ROW_POINTER_RECORD_LEN, RowPointer};
use crate::core::storage_providers::traits::StorageIO;

async fn all_pointers(t: &Table<LocalStorageProvider>) -> Vec<RowPointer> {
    let len = t.data().unwrap().io_pointers.get_len().await;
    let mut out = Vec::new();
    let mut pos = 0u64;
    while pos + ROW_POINTER_RECORD_LEN as u64 <= len {
        let mut buf = [0u8; ROW_POINTER_RECORD_LEN];
        let mut p = pos;
        t.data().unwrap().io_pointers
            .read_data_into_buffer(&mut p, &mut buf)
            .await
            .unwrap();
        out.push(RowPointer::from_slice(&buf));
        pos += ROW_POINTER_RECORD_LEN as u64;
    }
    out
}

/// Two updates that match the same row must not each append a replacement.
///
/// Before the write lock and the re-read, four concurrent `UPDATE`s of one row
/// left four live copies of it: each acted on its own pre-lock snapshot, so each
/// saw the row as live, tombstoned it, and appended a new one.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_updates_do_not_duplicate_a_row() {
    let dir = tempfile::tempdir().unwrap();
    let t = Arc::new(open_table(dir.path(), "wp_conc_upd").await);
    for i in 0..50u64 {
        insert(&t, i, i as i32, "s").await;
    }
    t.wait_filters_ready().await.unwrap();

    let mut handles = Vec::new();
    for k in 0..8u64 {
        let t = t.clone();
        handles.push(tokio::spawn(async move {
            // Every label is longer than the original, so each update takes the
            // relocating path — the one that used to duplicate.
            let label = format!("{}-{}", "z".repeat(10 + k as usize * 4), k);
            t.update_rows(
                "id = 7",
                full_fetch(&t.schema.fields),
                upd(&t.schema.fields, "label", label.as_bytes()),
                None,
            )
            .await
        }));
    }
    let mut applied = 0u64;
    for h in handles {
        applied += h.await.unwrap().unwrap();
    }

    let matches = run(&t, "id = 7").await;
    assert_eq!(matches.len(), 1, "id = 7 must still name exactly one row");
    assert_eq!(run(&t, "").await.len(), 50, "table gained or lost rows");
    assert!(applied >= 1, "at least one update has to win");

    // Exactly one live pointer per surviving row.
    let live = all_pointers(&t)
        .await
        .into_iter()
        .filter(|p| !p.deleted)
        .count();
    assert_eq!(live, 50);
}

/// A delete racing an update of the same row must leave the row gone, not
/// resurrected by the update's replacement pointer.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_delete_and_update_do_not_resurrect_a_row() {
    let dir = tempfile::tempdir().unwrap();
    let t = Arc::new(open_table(dir.path(), "wp_conc_del").await);
    for i in 0..30u64 {
        insert(&t, i, i as i32, "s").await;
    }
    t.wait_filters_ready().await.unwrap();

    let deleter = {
        let t = t.clone();
        tokio::spawn(async move {
            t.delete_rows("id = 3", full_fetch(&t.schema.fields), None)
                .await
        })
    };
    let updater = {
        let t = t.clone();
        tokio::spawn(async move {
            t.update_rows(
                "id = 3",
                full_fetch(&t.schema.fields),
                upd(&t.schema.fields, "label", b"a-much-longer-value"),
                None,
            )
            .await
        })
    };
    let deleted = deleter.await.unwrap().unwrap();
    let updated = updater.await.unwrap().unwrap();

    let survivors = run(&t, "id = 3").await;
    // Whichever ran first, the row is either updated once or gone once — never
    // both, and never twice.
    assert!(
        survivors.len() <= 1,
        "id = 3 matched {} rows",
        survivors.len()
    );
    if deleted == 1 && updated == 0 {
        assert!(survivors.is_empty());
        assert_eq!(run(&t, "").await.len(), 29);
    } else {
        assert_eq!(run(&t, "").await.len(), 30 - deleted as usize);
    }
}

/// Concurrent deletes of one row must be counted once, not once per statement.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_deletes_of_one_row_count_once() {
    let dir = tempfile::tempdir().unwrap();
    let t = Arc::new(open_table(dir.path(), "wp_conc_del2").await);
    for i in 0..20u64 {
        insert(&t, i, i as i32, "s").await;
    }
    t.wait_filters_ready().await.unwrap();

    let mut handles = Vec::new();
    for _ in 0..6 {
        let t = t.clone();
        handles.push(tokio::spawn(async move {
            t.delete_rows("id = 11", full_fetch(&t.schema.fields), None)
                .await
        }));
    }
    let mut total = 0u64;
    for h in handles {
        total += h.await.unwrap().unwrap();
    }
    assert_eq!(
        total, 1,
        "six racing DELETEs reported {} rows affected",
        total
    );
    assert_eq!(run(&t, "").await.len(), 19);
}

// ---------------------------------------------------------------------------
// Space reclamation
// ---------------------------------------------------------------------------

/// The whole point: a delete-and-reinsert cycle must not grow the rows file.
#[tokio::test(flavor = "multi_thread")]
async fn reinserting_after_a_delete_reuses_the_freed_space() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_reuse").await;
    for i in 0..400u64 {
        insert(&t, i, i as i32, "a-label-of-a-fixed-moderate-length").await;
    }
    t.wait_filters_ready().await.unwrap();
    let after_first_fill = t.data().unwrap().io_rows.get_len().await;

    t.delete_rows("id < 400", full_fetch(&t.schema.fields), None)
        .await
        .unwrap();
    assert!(run(&t, "").await.is_empty());

    let report = t.vacuum_now().await.unwrap();
    assert_eq!(report.live_rows, 0);
    assert_eq!(report.dead_rows, 400);
    assert!(
        report.reclaimed_bytes > 0,
        "nothing reclaimed from 400 dead rows: {:?}",
        report
    );

    for i in 1000..1400u64 {
        insert(&t, i, i as i32, "a-label-of-a-fixed-moderate-length").await;
    }
    let after_refill = t.data().unwrap().io_rows.get_len().await;

    assert_eq!(run(&t, "").await.len(), 400);
    assert_eq!(
        after_refill, after_first_fill,
        "refill extended the rows file instead of reusing {} reclaimed bytes",
        report.reclaimed_bytes
    );
    assert!(t.data().unwrap().free_space.reused_rows() >= 400);
}

/// Reused extents must hold the row that was written into them, not a mix of
/// old and new bytes.
#[tokio::test(flavor = "multi_thread")]
async fn rows_written_into_reclaimed_extents_read_back_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_reuse_read").await;
    for i in 0..200u64 {
        insert(&t, i, i as i32, &format!("original-label-{}", i)).await;
    }
    t.wait_filters_ready().await.unwrap();

    // Punch holes rather than clearing the table, so live and reclaimed rows
    // are interleaved.
    t.delete_rows("cat < 100", full_fetch(&t.schema.fields), None)
        .await
        .unwrap();
    t.vacuum_now().await.unwrap();

    for i in 1000..1100u64 {
        insert(&t, i, i as i32, &format!("replacement-label-{}", i)).await;
    }

    let rows = run(&t, "").await;
    assert_eq!(rows.len(), 200);
    for row in &rows {
        let id = u64_of(row, 0);
        let label = str_of(row, 2);
        if id >= 1000 {
            assert_eq!(label, format!("replacement-label-{}", id));
        } else {
            assert_eq!(label, format!("original-label-{}", id));
        }
    }
}

/// Reclaimed placements are ordinary writes, so they have to survive a reopen.
#[tokio::test(flavor = "multi_thread")]
async fn reclaimed_rows_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "wp_reuse_reopen").await;
        for i in 0..100u64 {
            insert(&t, i, i as i32, "some-moderately-long-label-here").await;
        }
        t.wait_filters_ready().await.unwrap();
        t.delete_rows("cat < 60", full_fetch(&t.schema.fields), None)
            .await
            .unwrap();
        t.vacuum_now().await.unwrap();
        for i in 1000..1060u64 {
            insert(&t, i, i as i32, "some-moderately-long-label-here").await;
        }
        assert!(t.data().unwrap().free_space.reused_rows() > 0, "no extent was reused");
    }

    let t2 = open_table(dir.path(), "wp_reuse_reopen").await;
    t2.wait_filters_ready().await.unwrap();
    let rows = run(&t2, "").await;
    assert_eq!(rows.len(), 100);
    for row in &rows {
        assert_eq!(str_of(row, 2), "some-moderately-long-label-here");
    }

    // A fresh insert after reopening must not land on top of a live row.
    insert(&t2, 5000, 5000, "post-reopen-label").await;
    assert_eq!(run(&t2, "").await.len(), 101);
    assert_eq!(
        str_of(&run(&t2, "id = 5000").await[0], 2),
        "post-reopen-label"
    );
}

/// The slack an in-place shrinking update leaves behind is dead space too, and
/// the scan has to see it.
#[tokio::test(flavor = "multi_thread")]
async fn shrinking_updates_leave_reclaimable_slack() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_slack").await;
    for i in 0..100u64 {
        insert(&t, i, i as i32, "a-really-quite-long-label-to-shrink-later").await;
    }
    t.wait_filters_ready().await.unwrap();

    let baseline = t.vacuum_now().await.unwrap();
    assert_eq!(
        baseline.dead_bytes, 0,
        "a freshly filled table has no holes"
    );

    t.update_rows(
        "cat < 50",
        full_fetch(&t.schema.fields),
        upd(&t.schema.fields, "label", b"x"),
        None,
    )
    .await
    .unwrap();

    let after = t.vacuum_now().await.unwrap();
    assert_eq!(after.live_rows, 100);
    assert!(
        after.dead_bytes > 0,
        "50 shrinking updates freed no slack: {:?}",
        after
    );
}

/// A query already running must keep its extents until it finishes.
#[tokio::test(flavor = "multi_thread")]
async fn a_running_query_defers_publication() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_defer").await;
    for i in 0..50u64 {
        insert(&t, i, i as i32, "label-long-enough-to-matter").await;
    }
    t.wait_filters_ready().await.unwrap();
    t.delete_rows("cat < 25", full_fetch(&t.schema.fields), None)
        .await
        .unwrap();

    // Stand in for a query that started before the scan and has not finished.
    let ticket = t.data().unwrap().readers.enter();
    let config = VacuumConfig {
        drain_timeout: std::time::Duration::from_millis(50),
        ..Default::default()
    };
    let horizon = t.version_horizon();
    let ctx = crate::core::vacuum::ReclaimContext {
        horizon: &horizon,
        filters: None,
        indexes: None,
        cleared_below: &std::sync::atomic::AtomicU64::new(0),
        compaction_gate: None,
        alive: None,
        gate: None,
    };
    let report = crate::core::vacuum::run_once(
        &*t.data().unwrap().io_pointers,
        &*t.data().unwrap().io_rows,
        &t.data().unwrap().free_space,
        &t.data().unwrap().readers,
        &ctx,
        &config,
        true,
    )
    .await
    .unwrap();
    assert!(
        report.deferred,
        "publication ran while an older read was live"
    );
    assert_eq!(t.data().unwrap().free_space.free_bytes(), 0);

    drop(ticket);
    let report = t.vacuum_now().await.unwrap();
    assert!(!report.deferred);
    assert!(report.reclaimed_bytes > 0);
}

/// A DDL retiring an instance mid-sweep waits for one pointer chunk, not the
/// rest of the sweep (B4 follow-up to RV14): `retire` clears the maintenance
/// flag and joins the sweep, which stops at its next chunk boundary, reports
/// itself deferred and publishes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn retirement_stops_a_sweep_within_one_chunk() {
    use crate::core::vacuum::run_once_test_hooks;
    const CHUNK: usize = 8;
    const ROWS: u64 = 64;
    let dir = tempfile::tempdir().unwrap();
    let t = Arc::new(open_table(dir.path(), "wp_retire_sweep").await);
    for i in 0..ROWS {
        insert(&t, i, i as i32, "label-long-enough-to-matter").await;
    }
    t.wait_filters_ready().await.unwrap();
    t.delete_rows("cat < 32", full_fetch(&t.schema.fields), None)
        .await
        .unwrap();

    let free_space = &t.data().unwrap().free_space;
    run_once_test_hooks::set_chunk_records(free_space, CHUNK);
    let free_before = free_space.free_bytes();
    let park = run_once_test_hooks::arm(free_space, run_once_test_hooks::IN_SCAN);
    let sweeping = t.clone();
    let sweep = tokio::spawn(async move { sweeping.vacuum_now().await });
    tokio::time::timeout(std::time::Duration::from_secs(30), park.parked.notified())
        .await
        .expect("the sweep never reached its first chunk");

    // The DDL's step 2: clears the flag, then waits for the sweep.
    let retiring = t.clone();
    let retire = tokio::spawn(async move { retiring.retire().await });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !t.is_retired() {
        assert!(std::time::Instant::now() < deadline, "retire never cleared the flag");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    park.release.notify_one();

    let report = tokio::time::timeout(std::time::Duration::from_secs(30), sweep)
        .await
        .expect("the sweep did not stop")
        .unwrap()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(30), retire)
        .await
        .expect("retire did not return once the sweep stopped")
        .unwrap();
    assert!(report.deferred, "a sweep stopped by retirement reported itself complete: {report:?}");
    assert!(
        report.slots <= 2 * CHUNK as u64,
        "the sweep read {} of {} slots after its instance was retired; it should stop within one chunk",
        report.slots,
        ROWS
    );
    assert_eq!(free_space.free_bytes(), free_before, "a retired sweep published space");
}

use crate::core::vacuum::VacuumConfig;

/// Inserts, deletes and reclamation running at the same time must never leave a
/// row reading as anything other than what was written into it.
///
/// The failure this guards against is silent: an extent handed out while a
/// query still holds a pointer into it produces a row assembled from two
/// different rows' bytes, which reads back as a plausible value rather than an
/// error.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writes_and_reclamation_keep_rows_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let t = Arc::new(open_table(dir.path(), "wp_stress").await);
    for i in 0..300u64 {
        insert(&t, i, i as i32, &format!("row-{}-original", i)).await;
    }
    t.wait_filters_ready().await.unwrap();

    let mut handles = Vec::new();

    // Deletes a third of the table, in slices, while everything else runs.
    {
        let t = t.clone();
        handles.push(tokio::spawn(async move {
            for slice in 0..5i32 {
                let lo = slice * 20;
                t.delete_rows(
                    &format!("cat >= {} AND cat < {}", lo, lo + 20),
                    full_fetch(&t.schema.fields),
                    None,
                )
                .await
                .unwrap();
            }
        }));
    }

    // Keeps inserting, so the free list is drawn from as it fills.
    {
        let t = t.clone();
        handles.push(tokio::spawn(async move {
            for i in 1000..1100u64 {
                insert(&t, i, i as i32, &format!("row-{}-original", i)).await;
            }
        }));
    }

    // Reads throughout, holding pointers across fetches.
    {
        let t = t.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..40 {
                for row in run(&t, "").await {
                    let id = u64_of(&row, 0);
                    assert_eq!(
                        str_of(&row, 2),
                        format!("row-{}-original", id),
                        "row {} read back as a different row's bytes",
                        id
                    );
                }
            }
        }));
    }

    // And reclamation, as aggressively as it can run.
    {
        let t = t.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..30 {
                t.vacuum_now().await.unwrap();
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // 300 inserted, 100 deleted, 100 more inserted.
    let rows = run(&t, "").await;
    assert_eq!(rows.len(), 300);
    for row in &rows {
        let id = u64_of(row, 0);
        assert_eq!(str_of(row, 2), format!("row-{}-original", id));
    }

    // And it all still holds after a reopen.
    drop(rows);
    let t2 = open_table(dir.path(), "wp_stress").await;
    t2.wait_filters_ready().await.unwrap();
    let rows = run(&t2, "").await;
    assert_eq!(rows.len(), 300);
    for row in &rows {
        let id = u64_of(row, 0);
        assert_eq!(str_of(row, 2), format!("row-{}-original", id));
    }
}

/// A row that outgrows its extent is relocated, and a relocated row is still
/// the same row: an update must not make it look newly created.
#[cfg(feature = "enable_long_row")]
#[tokio::test(flavor = "multi_thread")]
async fn a_relocating_update_preserves_the_rows_history() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_history").await;
    for i in 0..5u64 {
        insert(&t, i, i as i32, "s").await;
    }
    t.wait_filters_ready().await.unwrap();

    let before = all_pointers(&t).await;
    let original = before
        .iter()
        .find(|p| !p.deleted && p.id == 1)
        .unwrap()
        .clone();
    assert!(original.created_at > 0);

    // Force a relocation: the new label cannot fit the old extent.
    t.update_rows(
        "id = 0",
        full_fetch(&t.schema.fields),
        upd(&t.schema.fields, "label", b"a-label-far-too-long-to-fit"),
        None,
    )
    .await
    .unwrap();

    let after = all_pointers(&t).await;
    let replacement = after.last().unwrap();
    assert!(!replacement.deleted);
    assert_eq!(
        replacement.created_at, original.created_at,
        "the update rewrote the row's creation time"
    );
    assert_eq!(
        replacement.version,
        original.version + 1,
        "the relocation restarted the version counter"
    );
    assert!(
        replacement.updated_at >= original.created_at,
        "updated_at was not stamped"
    );

    // The tombstone it replaced records when the old record died, not the row.
    let tombstone = after.iter().find(|p| p.deleted).unwrap();
    assert!(tombstone.deleted_at > 0);
}

/// A table small enough to scan outright must not have a candidate mask built
/// for it, however selective that mask would be.
///
/// This is `filter.min_table_rows`, which was settable from the CLI and `SET
/// CONFIG` and reported by `SHOW CONFIG` while being read by nothing at all —
/// so the assertion that matters is not where the boundary falls but that the
/// configured value is consulted.
#[tokio::test(flavor = "multi_thread")]
async fn a_small_table_is_scanned_rather_than_masked() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_size_gate").await;
    // Over one 512-row zone, so a zone seals and the column carries
    // fingerprints — but well under the 4096-slot default.
    for i in 0..600u64 {
        insert(&t, i, (i % 8) as i32, "s").await;
    }
    t.wait_filters_ready().await.unwrap();

    let filters = t.data().unwrap().filters.as_ref().expect("filters active");
    let n_slots = t.data().unwrap().io_pointers.get_len().await / ROW_POINTER_RECORD_LEN as u64;
    assert!(
        n_slots < filters.config().min_table_rows,
        "test table is not below the gate: {} slots",
        n_slots
    );

    // The mask is perfectly buildable and would prune hard: `id` is unique, so
    // this is not the ratio test declining a mask that barely prunes.
    let toks = crate::core::tokenizer::query_tokenizer::tokenize("id = 300", &t.schema.fields);
    let program = crate::core::filter::sarg::extract(&toks, filters)
        .expect("a point lookup on a unique column is sargable");
    let bm = filters
        .evaluate(&program, n_slots)
        .expect("the mask builds");
    assert!(bm.get(300), "the mask is wrong, not merely declined");
    assert!(
        bm.count() < n_slots / 2,
        "the mask does not prune, so this proves nothing"
    );

    // And the scan declines it anyway, because the table is too small to pay
    // for a bitmap test per record.
    assert!(
        t.build_candidates("id = 300", None).await.unwrap().is_none(),
        "a table under min_table_rows still had a candidate mask built"
    );

    // Declining is an optimisation, not an answer: the query is unchanged.
    let rows = run(&t, "id = 300").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(u64_of(&rows[0], 0), 300);
}

/// `build_candidates` tokenizes a clause once and hands the tokens to the
/// constant judgement, the indexes and the filter. Each must still see them:
/// a constant false clause reads nothing, a long one only when it carries
/// the rewriter's constant, and the filter still prunes either way.
#[tokio::test(flavor = "multi_thread")]
async fn one_tokenization_serves_the_constant_and_the_filter() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_tokens_once").await;
    for i in 0..5000u64 {
        insert(&t, i, (i % 8) as i32, "s").await;
    }
    t.wait_filters_ready().await.unwrap();
    let n_slots = t.data().unwrap().io_pointers.get_len().await / crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN as u64;
    assert!(n_slots >= t.data().unwrap().filters.as_ref().unwrap().config().min_table_rows, "under the size gate");

    let nothing = t.build_candidates("1 = 0", None).await.unwrap().expect("a constant false clause");
    assert!(nothing.is_nothing());

    let point = t.build_candidates("id = 300", None).await.unwrap().expect("the filter prunes a point lookup");
    assert!(point.get(300) && point.count() < n_slots / 2, "{} of {n_slots}", point.count());

    // Past 4096 characters a clause is judged only when it carries the
    // rewriter's constant, and the filter reads it either way.
    let long = vec!["id = 300"; 600].join(" OR ");
    assert!(long.len() > 4096);
    let filtered = t.build_candidates(&long, None).await.unwrap().expect("the filter reads a long clause");
    assert!(filtered.get(300) && !filtered.is_nothing() && filtered.count() < n_slots / 2);
    let long_false = format!("({long}) AND 1 = 0");
    assert!(t.build_candidates(&long_false, None).await.unwrap().expect("constant").is_nothing());
    assert!(run(&t, &long_false).await.is_empty());
    assert_eq!(run(&t, &long).await.len(), 1);
}

/// An `UPDATE` writes a new version of an existing row, so it must not draw a
/// fresh id from the sequence.
///
/// `RowPointer::new` allocated one unconditionally and `inherit_metadata` then
/// overwrote it with the previous version's, so every relocation spent a
/// sequence number on nothing. That made `last_row_id` — which `SHOW TABLE
/// STATS` reports as "highest id handed out" — count pointer records rather
/// than rows: it read 472 401 against about a thousand live rows on the
/// deployment where this was noticed, which is not a figure anything can use.
#[tokio::test(flavor = "multi_thread")]
async fn updates_do_not_consume_row_ids() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_row_ids").await;
    for i in 0..5u64 {
        insert(&t, i, i as i32, "s").await;
    }

    let before = all_pointers(&t).await;
    let original = before.iter().find(|p| !p.deleted).unwrap().clone();
    let after_inserts = t.data().unwrap().ids.last_row_id.load(std::sync::atomic::Ordering::Relaxed);

    // Each label is longer than the last, so every one of these relocates
    // rather than overwriting the extent in place — the path that allocates.
    for n in 0..10usize {
        t.update_rows(
            "id = 0",
            full_fetch(&t.schema.fields),
            upd(&t.schema.fields, "label", "x".repeat(8 + n).as_bytes()),
            None,
        )
        .await
        .unwrap();
    }

    assert_eq!(
        t.data().unwrap().ids.last_row_id.load(std::sync::atomic::Ordering::Relaxed),
        after_inserts,
        "ten updates drew ids from the sequence"
    );

    // The point of the id is that the row keeps answering to it, so the saving
    // must not have come from dropping it.
    let live: Vec<_> = all_pointers(&t)
        .await
        .into_iter()
        .filter(|p| !p.deleted)
        .collect();
    assert_eq!(live.len(), 5, "an update left more than one live version");
    assert_eq!(
        live.iter().filter(|p| p.id == original.id).count(),
        1,
        "the updated row did not keep the id it was inserted with"
    );

    let rows = run(&t, "id = 0").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(str_of(&rows[0], 2), "x".repeat(17));
}

/// The filter backfill reads rows by pointer just as a query does, and is the
/// one such reader that is not a query. Reclaiming an extent underneath it
/// would index another row's bytes — which does not fail, it silently builds a
/// filter that prunes rows that do match.
#[tokio::test(flavor = "multi_thread")]
async fn a_backfill_running_against_reclamation_still_indexes_correctly() {
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "wp_backfill").await;
        for i in 0..300u64 {
            insert(&t, i, i as i32, &format!("label-{}", i)).await;
        }
        t.wait_filters_ready().await.unwrap();
        // Leave the table with plenty of dead space for the reopen to reclaim.
        t.delete_rows("cat < 150", full_fetch(&t.schema.fields), None)
            .await
            .unwrap();
    }

    // Reopening kicks off a backfill over a table that has reclaimable space.
    let t = Arc::new(open_table(dir.path(), "wp_backfill").await);

    let vacuuming = {
        let t = t.clone();
        tokio::spawn(async move {
            for _ in 0..40 {
                t.vacuum_now().await.unwrap();
                tokio::task::yield_now().await;
            }
        })
    };
    let inserting = {
        let t = t.clone();
        tokio::spawn(async move {
            for i in 1000..1150u64 {
                insert(&t, i, i as i32, &format!("label-{}", i)).await;
            }
        })
    };

    t.wait_filters_ready().await.unwrap();
    vacuuming.await.unwrap();
    inserting.await.unwrap();

    // Every surviving row must still be findable by an indexed predicate, and
    // hold its own bytes. A filter built from reclaimed bytes would fail the
    // point lookups, not the full scan.
    let all = run(&t, "").await;
    assert_eq!(all.len(), 300);
    for row in &all {
        let id = u64_of(row, 0);
        assert_eq!(str_of(row, 2), format!("label-{}", id));

        let found = run(&t, &format!("id = {}", id)).await;
        assert_eq!(found.len(), 1, "id = {} was pruned by the filter", id);
        assert_eq!(str_of(&found[0], 2), format!("label-{}", id));
    }
}

/// `insert_row` used to consult the write result only to decide whether to
/// index the row, and return `Ok(())` either way — a client would be told its
/// row had landed when nothing was written.
#[tokio::test(flavor = "multi_thread")]
async fn insert_reports_what_it_actually_wrote() {
    use crate::core::row::error::RowError;

    // The conversion the propagation goes through must not flatten a storage
    // error into something a caller cannot act on.
    let io_error: std::io::Error = RowError::Io(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "denied",
    ))
    .into();
    assert_eq!(io_error.kind(), std::io::ErrorKind::PermissionDenied);
    let io_error: std::io::Error = RowError::NotFound("gone".into()).into();
    assert_eq!(io_error.kind(), std::io::ErrorKind::NotFound);
    let io_error: std::io::Error = RowError::SavingFailed("disk".into()).into();
    assert!(io_error.to_string().contains("disk"));

    // And an insert that reports success has to be readable back, including
    // when it was placed in a reclaimed extent rather than appended.
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_insert_ok").await;
    for i in 0..50u64 {
        insert(&t, i, i as i32, "a-label-with-some-length").await;
    }
    t.wait_filters_ready().await.unwrap();
    t.delete_rows("cat < 40", full_fetch(&t.schema.fields), None)
        .await
        .unwrap();
    t.vacuum_now().await.unwrap();

    for i in 1000..1040u64 {
        let mut rw = RowWrite {
            columns_writing_data: SmallVec::new(),
        };
        rw.columns_writing_data
            .push(payload(&i.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data
            .push(payload(&(i as i32).to_le_bytes(), 1, DbType::I32));
        rw.columns_writing_data
            .push(payload(b"a-label-with-some-length", 2, DbType::STRING));
        t.insert_row(rw).await.expect("insert reported failure");
    }
    assert!(
        t.data().unwrap().free_space.reused_rows() > 0,
        "the reclaimed path was not exercised"
    );
    assert_eq!(run(&t, "").await.len(), 50);
    assert_eq!(t.data().unwrap().free_space.leaked_loans(), 0, "an extent was stranded");
}

/// On a table too large to map, dead space is derived from tombstones — and a
/// tombstone is not proof its extent is free.
///
/// Once an extent has been reclaimed and written into, the tombstone that
/// released it is still on disk pointing at bytes a live row now owns. Without
/// the second pass that withholds those, the extent is handed out again and the
/// row that landed there is overwritten — silently, since the bytes that come
/// back are a well-formed row, just not the right one.
#[tokio::test(flavor = "multi_thread")]
async fn the_capped_path_never_rehands_a_reused_extent() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_capped").await;
    for i in 0..120u64 {
        insert(&t, i, i as i32, &format!("original-{:04}", i)).await;
    }
    t.wait_filters_ready().await.unwrap();

    // Force the tombstone-derived path on a table small enough to test.
    let config = VacuumConfig {
        max_tracked_live_extents: 0,
        ..Default::default()
    };

    let run = async |t: &Table<LocalStorageProvider>| {
        let horizon = t.version_horizon();
        let ctx = crate::core::vacuum::ReclaimContext {
            horizon: &horizon,
            filters: None,
            indexes: None,
        cleared_below: &std::sync::atomic::AtomicU64::new(0),
            compaction_gate: None,
            alive: None,
            gate: None,
        };
        crate::core::vacuum::run_once(
            &*t.data().unwrap().io_pointers,
            &*t.data().unwrap().io_rows,
            &t.data().unwrap().free_space,
            &t.data().unwrap().readers,
            &ctx,
            &config,
            true,
        )
        .await
        .unwrap()
    };

    t.delete_rows("cat < 80", full_fetch(&t.schema.fields), None)
        .await
        .unwrap();

    // First run publishes the freed extents; the refill claims them. The
    // tombstones that released them stay on disk.
    let first = run(&t).await;
    assert!(
        first.reclaimed_bytes > 0,
        "capped path reclaimed nothing: {:?}",
        first
    );
    for i in 1000..1080u64 {
        insert(&t, i, i as i32, &format!("original-{:04}", i)).await;
    }
    assert!(
        t.data().unwrap().free_space.reused_rows() > 0,
        "the refill did not reuse anything"
    );

    // Second run sees exactly the same tombstones. Anything it publishes now is
    // occupied by one of the rows just written.
    let second = run(&t).await;
    for i in 2000..2080u64 {
        insert(&t, i, i as i32, &format!("original-{:04}", i)).await;
    }

    // The ids that must be present, and no others. Comparing each row's label
    // against its own id would not catch this: a re-handed extent makes the
    // pointer read *another row's* bytes, so id and label move together and
    // stay consistent with each other while the set as a whole is wrong.
    let expected: Vec<u64> = (80..120).chain(1000..1080).chain(2000..2080).collect();

    let rows = query_all(&t).await;
    let mut ids: Vec<u64> = rows.iter().map(|r| u64_of(r, 0)).collect();
    ids.sort_unstable();

    assert_eq!(
        ids, expected,
        "an extent was handed out twice: a row is duplicated and another lost \
         (second run: {:?})",
        second
    );
    for row in &rows {
        let id = u64_of(row, 0);
        assert_eq!(str_of(row, 2), format!("original-{:04}", id));
    }
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

/// Compaction must give the space back, not merely make it reusable.
#[tokio::test(flavor = "multi_thread")]
async fn compaction_shrinks_both_files() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_compact").await;
    for i in 0..300u64 {
        insert(&t, i, i as i32, &format!("a-label-of-some-length-{:04}", i)).await;
    }
    t.wait_filters_ready().await.unwrap();
    let full_rows = t.data().unwrap().io_rows.get_len().await;
    let full_pointers = t.data().unwrap().io_pointers.get_len().await;

    // Punch holes throughout rather than at one end, so compaction has to move
    // rows rather than just cut a tail.
    for band in 0..15i32 {
        let lo = band * 20;
        t.delete_rows(
            &format!("cat >= {} AND cat < {}", lo, lo + 10),
            full_fetch(&t.schema.fields),
            None,
        )
        .await
        .unwrap();
    }
    let survivors = run(&t, "").await.len();
    assert_eq!(survivors, 150);

    let report = t.compact().await.unwrap();
    assert_eq!(report.live_rows, 150);
    assert_eq!(report.dead_rows, 150);
    assert!(report.rows_moved > 0, "no row was moved: {:?}", report);
    assert!(report.slots_moved > 0, "no slot was moved: {:?}", report);
    assert!(!report.truncation_unsupported, "{:?}", report);

    let compacted_rows = t.data().unwrap().io_rows.get_len().await;
    let compacted_pointers = t.data().unwrap().io_pointers.get_len().await;
    assert!(compacted_rows < full_rows, "rows file did not shrink");
    assert_eq!(
        compacted_pointers,
        150 * ROW_POINTER_RECORD_LEN as u64,
        "pointer file still holds tombstones"
    );
    assert_eq!(report.rows_bytes_freed, full_rows - compacted_rows);
    assert_eq!(
        report.pointer_bytes_freed,
        full_pointers - compacted_pointers
    );

    // Nothing about the data changed.
    let rows = run(&t, "").await;
    assert_eq!(rows.len(), 150);
    for row in &rows {
        let id = u64_of(row, 0);
        assert_eq!(str_of(row, 2), format!("a-label-of-some-length-{:04}", id));
    }
}

/// Compaction renumbers every slot, so the filter has to be rebuilt or point
/// lookups start pruning rows that do match.
#[tokio::test(flavor = "multi_thread")]
async fn point_lookups_still_work_after_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_compact_filter").await;
    for i in 0..400u64 {
        insert(&t, i, i as i32, &format!("label-{:04}", i)).await;
    }
    t.wait_filters_ready().await.unwrap();
    t.delete_rows("cat < 250", full_fetch(&t.schema.fields), None)
        .await
        .unwrap();

    t.compact().await.unwrap();
    t.wait_filters_ready().await.unwrap();

    for id in 250..400u64 {
        let found = run(&t, &format!("id = {}", id)).await;
        assert_eq!(found.len(), 1, "id = {} was pruned after compaction", id);
        assert_eq!(str_of(&found[0], 2), format!("label-{:04}", id));
    }
    for id in [0u64, 100, 249] {
        assert!(
            run(&t, &format!("id = {}", id)).await.is_empty(),
            "id = {} came back",
            id
        );
    }
    assert!(run(&t, "label = 'label-0300'").await.len() == 1);
}

/// A compacted table has to reopen, and keep working.
#[tokio::test(flavor = "multi_thread")]
async fn a_compacted_table_reopens_and_keeps_writing() {
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "wp_compact_reopen").await;
        for i in 0..200u64 {
            insert(&t, i, i as i32, &format!("label-{:04}", i)).await;
        }
        t.wait_filters_ready().await.unwrap();
        t.delete_rows("cat < 120", full_fetch(&t.schema.fields), None)
            .await
            .unwrap();
        t.compact().await.unwrap();
    }

    let t = open_table(dir.path(), "wp_compact_reopen").await;
    t.wait_filters_ready().await.unwrap();
    assert_eq!(run(&t, "").await.len(), 80);

    // New rows must not land on top of the compacted ones, and ids must not
    // collide with the survivors.
    for i in 1000..1050u64 {
        insert(&t, i, i as i32, &format!("label-{:04}", i)).await;
    }
    let rows = run(&t, "").await;
    assert_eq!(rows.len(), 130);
    for row in &rows {
        let id = u64_of(row, 0);
        assert_eq!(str_of(row, 2), format!("label-{:04}", id));
    }

    // And it can be compacted again.
    t.delete_rows(
        "cat < 1020 AND cat >= 1000",
        full_fetch(&t.schema.fields),
        None,
    )
    .await
    .unwrap();
    let second = t.compact().await.unwrap();
    assert_eq!(second.live_rows, 110);
    assert_eq!(run(&t, "").await.len(), 110);
}

/// Compacting an already-dense table must move nothing and cut nothing.
#[tokio::test(flavor = "multi_thread")]
async fn compacting_a_dense_table_is_a_no_op() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_compact_dense").await;
    for i in 0..100u64 {
        insert(&t, i, i as i32, "label").await;
    }
    t.wait_filters_ready().await.unwrap();
    let rows_len = t.data().unwrap().io_rows.get_len().await;
    let pointers_len = t.data().unwrap().io_pointers.get_len().await;

    let report = t.compact().await.unwrap();
    assert_eq!(report.rows_moved, 0, "{:?}", report);
    assert_eq!(report.slots_moved, 0, "{:?}", report);
    assert_eq!(report.rows_bytes_freed, 0);
    assert_eq!(report.pointer_bytes_freed, 0);
    assert_eq!(t.data().unwrap().io_rows.get_len().await, rows_len);
    assert_eq!(t.data().unwrap().io_pointers.get_len().await, pointers_len);
    assert_eq!(run(&t, "").await.len(), 100);
}

/// Compacting an empty table, and one where everything is dead.
#[tokio::test(flavor = "multi_thread")]
async fn compaction_handles_empty_and_fully_dead_tables() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_compact_empty").await;

    let report = t.compact().await.unwrap();
    assert_eq!(report.live_rows, 0);
    assert_eq!(report.dead_rows, 0);

    for i in 0..50u64 {
        insert(&t, i, i as i32, "label").await;
    }
    t.wait_filters_ready().await.unwrap();
    t.delete_rows("", full_fetch(&t.schema.fields), None)
        .await
        .unwrap();

    let report = t.compact().await.unwrap();
    assert_eq!(report.live_rows, 0);
    assert_eq!(report.dead_rows, 50);
    assert_eq!(
        t.data().unwrap().io_rows.get_len().await,
        0,
        "a fully dead table kept its bytes"
    );
    assert_eq!(t.data().unwrap().io_pointers.get_len().await, 0);
    assert!(run(&t, "").await.is_empty());

    // And it still accepts writes afterwards.
    insert(&t, 999, 999, "after-compaction").await;
    assert_eq!(run(&t, "").await.len(), 1);
    assert_eq!(str_of(&run(&t, "id = 999").await[0], 2), "after-compaction");
}

/// Compaction takes the gate exclusively, so a query cannot observe a row
/// mid-move. Without that, a reader holding a pointer would read the bytes of
/// whatever row had been packed into that offset.
#[tokio::test(flavor = "multi_thread")]
async fn compaction_excludes_concurrent_readers_and_writers() {
    let dir = tempfile::tempdir().unwrap();
    let t = Arc::new(open_table(dir.path(), "wp_compact_conc").await);
    for i in 0..400u64 {
        insert(&t, i, i as i32, &format!("label-{:04}", i)).await;
    }
    t.wait_filters_ready().await.unwrap();
    for band in 0..10i32 {
        let lo = band * 40;
        t.delete_rows(
            &format!("cat >= {} AND cat < {}", lo, lo + 15),
            full_fetch(&t.schema.fields),
            None,
        )
        .await
        .unwrap();
    }
    let survivors = run(&t, "").await.len();

    let mut handles = Vec::new();
    for _ in 0..3 {
        let t = t.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..25 {
                for row in run(&t, "").await {
                    let id = u64_of(&row, 0);
                    assert_eq!(
                        str_of(&row, 2),
                        format!("label-{:04}", id),
                        "row {} read mid-compaction",
                        id
                    );
                }
            }
        }));
    }
    {
        let t = t.clone();
        handles.push(tokio::spawn(async move {
            for i in 1000..1040u64 {
                insert(&t, i, i as i32, &format!("label-{:04}", i)).await;
            }
        }));
    }
    {
        let t = t.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..4 {
                t.compact().await.unwrap();
                tokio::task::yield_now().await;
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let rows = run(&t, "").await;
    assert_eq!(rows.len(), survivors + 40);
    let mut ids: Vec<u64> = rows.iter().map(|r| u64_of(r, 0)).collect();
    ids.sort_unstable();
    let deduped = {
        let mut d = ids.clone();
        d.dedup();
        d
    };
    assert_eq!(ids, deduped, "compaction duplicated a row");
    for row in &rows {
        let id = u64_of(row, 0);
        assert_eq!(str_of(row, 2), format!("label-{:04}", id));
    }
}

// ---------------------------------------------------------------------------
// Literal validation on the write paths
// ---------------------------------------------------------------------------

/// A literal that is not a value of the column's type must be rejected, not
/// quietly turned into a zero.
///
/// Every numeric arm of both encoders ended in `unwrap_or_default()`, so
/// `INSERT INTO t (n) VALUES ('abc')` stored `0` and reported success. Storing a
/// plausible value the caller never wrote is worse than refusing the statement
/// and worse than storing nothing, because nothing downstream can tell it
/// happened.
#[test]
fn a_bad_literal_is_rejected_rather_than_zeroed() {
    use crate::core::row::schema::SchemaField;
    use crate::core::rql::lexer_ir::insert_row_from_query_purpose;
    use crate::core::rql::lexer_s1::{InsertRowData, QueryPurpose};
    use crate::core::rql::lexer_ur::update_row_from_query_purpose;

    let schema: SmallVec<[SchemaField; 20]> = smallvec::smallvec![
        SchemaField::new(
            "id".into(),
            DbType::U64,
            DbType::U64.get_size(),
            0,
            0,
            false
        ),
        SchemaField::new("n".into(), DbType::I32, DbType::I32.get_size(), 1, 8, false),
    ];

    let insert = |sql: &str| {
        insert_row_from_query_purpose(
            &QueryPurpose::InsertRow(InsertRowData {
                table_name: "t".into(),
                query: sql.to_string(),
                on_conflict_do_nothing: false,
                on_conflict_target: Vec::new(),
                on_conflict_update: None,
            }),
            &schema,
        )
    };
    let update = |sql: &str| {
        update_row_from_query_purpose(&QueryPurpose::UpdateRow(sql.to_string()), &schema)
    };

    // Well-formed statements still parse.
    assert!(insert("INSERT INTO t (id, n) VALUES (1, 42);").is_some());
    assert!(update("UPDATE t SET n = 42 WHERE id = 1").is_some());
    // NULL stays acceptable: it means "no value", not "unparsable".
    assert!(insert("INSERT INTO t (id, n) VALUES (1, NULL);").is_some());
    assert!(update("UPDATE t SET n = NULL WHERE id = 1").is_some());

    // And a literal of the wrong type is refused by both.
    for bad in ["'abc'", "'12x'", "''", "'1.5.2'"] {
        assert!(
            insert(&format!("INSERT INTO t (id, n) VALUES (1, {});", bad)).is_none(),
            "INSERT accepted {} for an I32 column",
            bad
        );
        assert!(
            update(&format!("UPDATE t SET n = {} WHERE id = 1", bad)).is_none(),
            "UPDATE accepted {} for an I32 column",
            bad
        );
    }
}

/// The row encoder assumes each column's bytes match its declared size and that
/// the row fits the length field. It used to discover a violation by panicking
/// part-way through building the row, after an id had already been handed out.
#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_row_write_is_refused_before_anything_is_written() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_validate").await;

    // A fixed-width column carrying fewer bytes than it declares.
    let mut rw = RowWrite {
        columns_writing_data: SmallVec::new(),
    };
    rw.columns_writing_data
        .push(payload(&1u64.to_le_bytes(), 0, DbType::U64));
    let mut short = payload(&[1, 2, 3], 1, DbType::I32);
    short.size = 4;
    rw.columns_writing_data.push(short);
    rw.columns_writing_data
        .push(payload(b"ok", 2, DbType::STRING));
    assert!(
        t.insert_row(rw).await.is_err(),
        "a short column was accepted"
    );

    // A STRING column declaring something other than its header width.
    let mut rw = RowWrite {
        columns_writing_data: SmallVec::new(),
    };
    rw.columns_writing_data
        .push(payload(&2u64.to_le_bytes(), 0, DbType::U64));
    rw.columns_writing_data
        .push(payload(&7i32.to_le_bytes(), 1, DbType::I32));
    let mut wrong = payload(b"ok", 2, DbType::STRING);
    wrong.size = u32::MAX;
    rw.columns_writing_data.push(wrong);
    assert!(
        t.insert_row(rw).await.is_err(),
        "a bogus STRING width was accepted"
    );

    // Nothing was written, and the table still works.
    assert!(run(&t, "").await.is_empty());
    insert(&t, 3, 3, "fine").await;
    assert_eq!(run(&t, "").await.len(), 1);
}

// ---------------------------------------------------------------------------
// Row identity
// ---------------------------------------------------------------------------

/// A row keeps its id when an update relocates it, and the id sequence survives
/// a reopen — which it can no longer do by reading the last record, because ids
/// no longer ascend with file order.
#[tokio::test(flavor = "multi_thread")]
async fn a_relocated_row_keeps_its_id_and_the_sequence_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let ids_before: Vec<u64>;
    {
        let t = open_table(dir.path(), "wp_identity").await;
        for i in 0..20u64 {
            insert(&t, i, i as i32, "s").await;
        }
        t.wait_filters_ready().await.unwrap();

        let original = all_pointers(&t)
            .await
            .into_iter()
            .find(|p| !p.deleted)
            .unwrap();

        // Force a relocation.
        t.update_rows(
            "id = 0",
            full_fetch(&t.schema.fields),
            upd(
                &t.schema.fields,
                "label",
                b"a-label-far-too-long-to-fit-in-place",
            ),
            None,
        )
        .await
        .unwrap();

        let live: Vec<_> = all_pointers(&t)
            .await
            .into_iter()
            .filter(|p| !p.deleted)
            .collect();
        assert!(
            live.iter().any(|p| p.id == original.id),
            "the relocated row lost its id"
        );
        ids_before = live.iter().map(RowPointer::row_id).collect();

        // Ids stay unique across the live set.
        let mut sorted = ids_before.clone();
        sorted.sort_unstable();
        let mut deduped = sorted.clone();
        deduped.dedup();
        assert_eq!(sorted, deduped, "duplicate ids among live rows");
    }

    // Reopening must not hand out an id that is already in use. The last record
    // in the file now carries a *low* id, so recovering from it would.
    let t = open_table(dir.path(), "wp_identity").await;
    t.wait_filters_ready().await.unwrap();
    for i in 100..120u64 {
        insert(&t, i, i as i32, "s").await;
    }

    let live: Vec<_> = all_pointers(&t)
        .await
        .into_iter()
        .filter(|p| !p.deleted)
        .collect();
    assert_eq!(live.len(), 40);
    let mut ids: Vec<u64> = live.iter().map(RowPointer::row_id).collect();
    ids.sort_unstable();
    let mut deduped = ids.clone();
    deduped.dedup();
    assert_eq!(
        ids, deduped,
        "a reopened table reused an id that was still live"
    );
    for old in &ids_before {
        assert!(ids.contains(old), "id {} was lost across the reopen", old);
    }
}

/// An UPDATE whose row was relocated by a concurrent UPDATE must still apply,
/// not silently drop.
///
/// Before this, a tombstone meant "gone" and the second statement skipped the
/// row — but a relocating UPDATE tombstones the old record and appends a live
/// replacement, so the row was very much still there.
#[tokio::test(flavor = "multi_thread")]
async fn an_update_follows_a_row_that_was_relocated_under_it() {
    let dir = tempfile::tempdir().unwrap();
    let t = Arc::new(open_table(dir.path(), "wp_follow").await);
    for i in 0..40u64 {
        insert(&t, i, i as i32, "s").await;
    }
    t.wait_filters_ready().await.unwrap();

    // Both grow the row, so both take the relocating path.
    let first = {
        let t = t.clone();
        tokio::spawn(async move {
            t.update_rows(
                "id = 9",
                full_fetch(&t.schema.fields),
                upd(&t.schema.fields, "label", b"first-writer-value-long"),
                None,
            )
            .await
            .unwrap()
        })
    };
    let second = {
        let t = t.clone();
        tokio::spawn(async move {
            t.update_rows(
                "id = 9",
                full_fetch(&t.schema.fields),
                upd(&t.schema.fields, "cat", &777i32.to_le_bytes()),
                None,
            )
            .await
            .unwrap()
        })
    };
    let applied = first.await.unwrap() + second.await.unwrap();
    assert_eq!(applied, 2, "one of the two updates was dropped");

    // Both assignments touched different columns, so both must be visible.
    let rows = run(&t, "id = 9").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(str_of(&rows[0], 2), "first-writer-value-long");
    let cat = rows[0].columns.iter().find(|c| c.schema_id == 1).unwrap();
    assert_eq!(
        i32::from_le_bytes(cat.data.into_slice().try_into().unwrap()),
        777
    );
    assert_eq!(run(&t, "").await.len(), 40);
}

/// The same for DELETE: a row moved out from under it is still the row it
/// matched, and still has to go.
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_follows_a_row_that_was_relocated_under_it() {
    let dir = tempfile::tempdir().unwrap();
    let t = Arc::new(open_table(dir.path(), "wp_follow_del").await);
    for i in 0..40u64 {
        insert(&t, i, i as i32, "s").await;
    }
    t.wait_filters_ready().await.unwrap();

    let updater = {
        let t = t.clone();
        tokio::spawn(async move {
            t.update_rows(
                "id = 13",
                full_fetch(&t.schema.fields),
                upd(&t.schema.fields, "label", b"grown-well-past-its-extent"),
                None,
            )
            .await
            .unwrap()
        })
    };
    let deleter = {
        let t = t.clone();
        tokio::spawn(async move {
            t.delete_rows("id = 13", full_fetch(&t.schema.fields), None)
                .await
                .unwrap()
        })
    };
    let moved = updater.await.unwrap();
    let deleted = deleter.await.unwrap();

    // Whatever the interleaving, the row must be gone exactly once.
    assert!(
        run(&t, "id = 13").await.is_empty(),
        "the row survived its DELETE"
    );
    assert_eq!(run(&t, "").await.len(), 39);
    assert_eq!(
        deleted, 1,
        "the DELETE reported {} rows (update moved: {})",
        deleted, moved
    );
}

/// A table whose queries always overlap the scan must still reclaim — one run
/// behind — rather than never.
#[tokio::test(flavor = "multi_thread")]
async fn a_permanently_busy_table_still_reclaims() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_busy").await;
    for i in 0..120u64 {
        insert(&t, i, i as i32, "a-label-with-enough-length-to-matter").await;
    }
    t.wait_filters_ready().await.unwrap();
    t.delete_rows("cat < 80", full_fetch(&t.schema.fields), None)
        .await
        .unwrap();

    let config = VacuumConfig {
        drain_timeout: std::time::Duration::from_millis(20),
        ..Default::default()
    };
    let run_vacuum = async |t: &Table<LocalStorageProvider>| {
        let horizon = t.version_horizon();
        let ctx = crate::core::vacuum::ReclaimContext {
            horizon: &horizon,
            filters: None,
            indexes: None,
        cleared_below: &std::sync::atomic::AtomicU64::new(0),
            compaction_gate: None,
            alive: None,
            gate: None,
        };
        crate::core::vacuum::run_once(
            &*t.data().unwrap().io_pointers,
            &*t.data().unwrap().io_rows,
            &t.data().unwrap().free_space,
            &t.data().unwrap().readers,
            &ctx,
            &config,
            true,
        )
        .await
        .unwrap()
    };

    // A query that never ends. Every run will time out waiting for it.
    let forever = t.data().unwrap().readers.enter();

    let first = run_vacuum(&t).await;
    assert!(
        first.deferred,
        "the first run should have timed out: {:?}",
        first
    );
    assert_eq!(first.reclaimed_bytes, 0, "nothing is safe on the first run");
    assert!(
        t.data().unwrap().free_space.quarantined_extents() > 0,
        "the batch was thrown away"
    );

    // The long-running query still holds *its* barrier, but the batch's barrier
    // was taken during the first run, and that has now been passed by everything
    // still active except the one query — which predates it.
    drop(forever);
    let blocking_again = t.data().unwrap().readers.enter();

    let second = run_vacuum(&t).await;
    assert!(
        second.deferred,
        "the second run should also time out: {:?}",
        second
    );
    assert!(
        second.reclaimed_bytes > 0,
        "the held-over batch was never published: {:?}",
        second
    );

    drop(blocking_again);
    for i in 1000..1080u64 {
        insert(&t, i, i as i32, "a-label-with-enough-length-to-matter").await;
    }
    assert!(
        t.data().unwrap().free_space.reused_rows() > 0,
        "reclaimed space was never used"
    );
    assert_eq!(run(&t, "").await.len(), 120);
}

// ---------------------------------------------------------------------------
// UNIQUE constraints
// ---------------------------------------------------------------------------

async fn open_unique_table(dir: &std::path::Path, name: &str) -> Table<LocalStorageProvider> {
    init();
    let io = Arc::new(LocalStorageProvider::new(dir.to_str().unwrap(), Some(name)).await);
    Table::new_with_constraints(
        name,
        io,
        vec![
            CreateColumnData {
                name: "id".into(),
                data_type: DbType::U64,
            },
            CreateColumnData {
                name: "cat".into(),
                data_type: DbType::I32,
            },
            CreateColumnData {
                name: "label".into(),
                data_type: DbType::STRING,
            },
        ],
        &["id".to_string(), "label".to_string()],
    )
    .await
}

/// A `UNIQUE` column has to actually be unique. It was parsed, persisted, and
/// then ignored on every write path.
#[tokio::test(flavor = "multi_thread")]
async fn a_duplicate_insert_into_a_unique_column_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_unique_table(dir.path(), "wp_unique").await;

    for i in 0..20u64 {
        insert(&t, i, i as i32, &format!("label-{}", i)).await;
    }
    t.wait_filters_ready().await.unwrap();

    // Duplicate in the numeric unique column.
    let mut rw = RowWrite {
        columns_writing_data: SmallVec::new(),
    };
    rw.columns_writing_data
        .push(payload(&7u64.to_le_bytes(), 0, DbType::U64));
    rw.columns_writing_data
        .push(payload(&99i32.to_le_bytes(), 1, DbType::I32));
    rw.columns_writing_data
        .push(payload(b"a-brand-new-label", 2, DbType::STRING));
    let err = t.insert_row(rw).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists, "{}", err);

    // Duplicate in the string unique column.
    let mut rw = RowWrite {
        columns_writing_data: SmallVec::new(),
    };
    rw.columns_writing_data
        .push(payload(&500u64.to_le_bytes(), 0, DbType::U64));
    rw.columns_writing_data
        .push(payload(&99i32.to_le_bytes(), 1, DbType::I32));
    rw.columns_writing_data
        .push(payload(b"label-3", 2, DbType::STRING));
    let err = t.insert_row(rw).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists, "{}", err);

    // Neither wrote anything, and a genuinely new row still goes in.
    assert_eq!(run(&t, "").await.len(), 20);
    insert(&t, 500, 99, "label-500").await;
    assert_eq!(run(&t, "").await.len(), 21);

    // A non-unique column may repeat freely.
    insert(&t, 501, 3, "label-501").await;
    assert_eq!(run(&t, "cat = 3").await.len(), 2);
}

/// The value a deleted row held is free again.
#[tokio::test(flavor = "multi_thread")]
async fn a_deleted_rows_unique_value_can_be_reused() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_unique_table(dir.path(), "wp_unique_del").await;
    for i in 0..10u64 {
        insert(&t, i, i as i32, &format!("label-{}", i)).await;
    }
    t.wait_filters_ready().await.unwrap();

    t.delete_rows("id = 4", full_fetch(&t.schema.fields), None)
        .await
        .unwrap();
    insert(&t, 4, 400, "label-4-again").await;
    assert_eq!(run(&t, "id = 4").await.len(), 1);
    assert_eq!(str_of(&run(&t, "id = 4").await[0], 2), "label-4-again");
}

/// An UPDATE must not create a duplicate, but a row keeping its own value is
/// not a duplicate of itself.
#[tokio::test(flavor = "multi_thread")]
async fn update_respects_uniqueness_without_colliding_with_itself() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_unique_table(dir.path(), "wp_unique_upd").await;
    for i in 0..10u64 {
        insert(&t, i, i as i32, &format!("label-{}", i)).await;
    }
    t.wait_filters_ready().await.unwrap();

    // Rewriting a row's non-unique column leaves its unique ones untouched.
    let n = t
        .update_rows(
            "id = 2",
            full_fetch(&t.schema.fields),
            upd(&t.schema.fields, "cat", &42i32.to_le_bytes()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(n, 1, "a row collided with its own unique value");

    // Assigning a unique column a value it already has is also fine.
    let n = t
        .update_rows(
            "id = 2",
            full_fetch(&t.schema.fields),
            upd(&t.schema.fields, "label", b"label-2"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(n, 1);

    // But taking another row's value is not.
    let err = t
        .update_rows(
            "id = 2",
            full_fetch(&t.schema.fields),
            upd(&t.schema.fields, "label", b"label-5"),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists, "{}", err);
    assert_eq!(
        str_of(&run(&t, "id = 2").await[0], 2),
        "label-2",
        "the rejected UPDATE still wrote"
    );
    assert_eq!(run(&t, "").await.len(), 10);
}

/// Two concurrent inserts of the same value must not both succeed. Checking
/// outside the lock that guards the write is not a check at all.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_inserts_cannot_both_claim_a_unique_value() {
    let dir = tempfile::tempdir().unwrap();
    let t = Arc::new(open_unique_table(dir.path(), "wp_unique_race").await);
    insert(&t, 0, 0, "seed").await;
    t.wait_filters_ready().await.unwrap();

    let mut handles = Vec::new();
    for k in 0..8u64 {
        let t = t.clone();
        handles.push(tokio::spawn(async move {
            let mut rw = RowWrite {
                columns_writing_data: SmallVec::new(),
            };
            rw.columns_writing_data
                .push(payload(&777u64.to_le_bytes(), 0, DbType::U64));
            rw.columns_writing_data
                .push(payload(&(k as i32).to_le_bytes(), 1, DbType::I32));
            rw.columns_writing_data.push(payload(
                format!("racer-{}", k).as_bytes(),
                2,
                DbType::STRING,
            ));
            t.insert_row(rw).await.is_ok()
        }));
    }
    let mut winners = 0;
    for h in handles {
        if h.await.unwrap() {
            winners += 1;
        }
    }

    assert_eq!(
        winners, 1,
        "{} concurrent inserts claimed the same unique value",
        winners
    );
    assert_eq!(run(&t, "id = 777").await.len(), 1);
    assert_eq!(run(&t, "").await.len(), 2);
}

/// A table with no unique columns must not pay for the constraint.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_without_constraints_still_accepts_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_no_unique").await;
    insert(&t, 1, 1, "same").await;
    insert(&t, 1, 1, "same").await;
    t.wait_filters_ready().await.unwrap();
    assert_eq!(run(&t, "id = 1").await.len(), 2);
}

// ---------------------------------------------------------------------------
// The VACUUM statement
// ---------------------------------------------------------------------------

/// The statement has to be recognised in the shapes a client actually sends,
/// including the option list Postgres allows and this engine has nothing to do
/// with — ignoring it beats rejecting the statement.
#[test]
fn vacuum_statements_parse() {
    use crate::core::rql::lexer_s1::{QueryPurpose, recognize_query_purpose};

    let parse = |sql: &str| match recognize_query_purpose(sql) {
        Some(QueryPurpose::Vacuum(request)) => Some((request.table_name, request.full)),
        _ => None,
    };

    assert_eq!(parse("VACUUM"), Some((None, false)));
    assert_eq!(parse("VACUUM;"), Some((None, false)));
    assert_eq!(parse("vacuum full"), Some((None, true)));
    assert_eq!(parse("VACUUM users"), Some((Some("users".into()), false)));
    assert_eq!(
        parse("VACUUM FULL users;"),
        Some((Some("users".into()), true))
    );
    assert_eq!(
        parse("VACUUM \"users\""),
        Some((Some("users".into()), false))
    );
    // Options this engine has nothing to act on are skipped, not rejected.
    assert_eq!(
        parse("VACUUM (ANALYZE) users"),
        Some((Some("users".into()), false))
    );
    assert_eq!(
        parse("VACUUM (VERBOSE, ANALYZE) users"),
        Some((Some("users".into()), false))
    );
    assert_eq!(
        parse("VACUUM VERBOSE ANALYZE users"),
        Some((Some("users".into()), false))
    );
    assert_eq!(parse("VACUUM FULL VERBOSE"), Some((None, true)));

    // And it must not swallow anything else.
    assert!(parse("SELECT * FROM vacuum_log").is_none());
    assert!(parse("UPDATE t SET n = 1").is_none());
}

/// Repeated delete-and-refill must reach a steady state, not creep upward.
///
/// Best-fit allocation splits extents, and a leftover under `MIN_SPLIT_BYTES`
/// is dropped rather than tracked. If that loss compounded, a table whose live
/// size never changes would still grow with every cycle — which is the exact
/// failure reclamation exists to prevent, just slower.
#[tokio::test(flavor = "multi_thread")]
async fn churn_reaches_a_steady_state() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_churn").await;

    // Varying widths, so allocation has to split and refit rather than always
    // finding an exact match.
    let label_for = |i: u64| "x".repeat(8 + (i % 23) as usize);

    for i in 0..300u64 {
        insert(&t, i, i as i32, &label_for(i)).await;
    }
    t.wait_filters_ready().await.unwrap();

    let mut sizes = Vec::new();
    for cycle in 0..8u64 {
        let lo = 0i32;
        let hi = 200i32;
        t.delete_rows(
            &format!("cat >= {} AND cat < {}", lo, hi),
            full_fetch(&t.schema.fields),
            None,
        )
        .await
        .unwrap();
        t.vacuum_now().await.unwrap();

        for i in 0..200u64 {
            let id = 1000 + cycle * 1000 + i;
            insert(&t, id, i as i32, &label_for(id)).await;
        }
        sizes.push(t.data().unwrap().io_rows.get_len().await);
    }

    assert_eq!(
        run(&t, "").await.len(),
        300,
        "churn lost or duplicated rows"
    );

    // Once the free list covers a cycle's worth of rows, the file must stop
    // growing outright — not merely grow slowly.
    //
    // It did grow, by about a cycle's worth every other cycle, because a loan
    // was retired only when a sweep saw its row *live*. A row written into a
    // reclaimed extent and deleted before the next sweep left its loan behind
    // forever, shielding those bytes from every scan after it. Under this
    // workload that is the entire working set, once per cycle.
    let steady = &sizes[3..];
    assert!(
        steady.iter().all(|len| *len == steady[0]),
        "the rows file was still growing in steady state: {:?}",
        sizes
    );
    // And essentially every refilled row came from the free list rather than the
    // append frontier. Not exactly all of them: the first cycle's list is built
    // from the initial fill, whose widths do not line up perfectly with the
    // refill's, so a handful of rows find no extent that fits.
    let refilled = 8 * 200;
    let reused = t.data().unwrap().free_space.reused_rows();
    assert!(
        reused > refilled - 10,
        "only {} of {} refilled rows reused an extent; sizes: {:?}",
        reused,
        refilled,
        sizes
    );

    // And every row still reads back as itself.
    for row in run(&t, "").await {
        let id = u64_of(&row, 0);
        assert_eq!(str_of(&row, 2), label_for(id), "row {}", id);
    }
}

/// `CREATE TABLE` has to carry `UNIQUE` and `PRIMARY KEY` through to the
/// enforcement. Both were parsed and then dropped on the floor.
///
/// A constraint lands in one of two places, and which one is not arbitrary: a
/// `UNIQUE` written inline on a column becomes a flag on that column's schema
/// field, while a table-level one becomes a named record in the sidecar. The
/// sidecar is what can hold a *tuple*, and what `DROP CONSTRAINT <name>` can
/// find again — a schema flag carries no constraint name.
#[test]
fn create_table_carries_unique_constraints() {
    use crate::core::rql::lexer_ct::create_table_from_query_purpose;
    use crate::core::rql::lexer_s1::QueryPurpose;

    let parsed =
        |sql: &str| create_table_from_query_purpose(&QueryPurpose::CreateTable(sql.to_string()));

    // Inline constraints flag their column.
    let inline_of = |sql: &str| {
        parsed(sql).map(|t| {
            let mut names = t.unique_columns;
            names.sort();
            names
        })
    };
    // Table-level constraints become named records, each with its columns.
    let table_level_of = |sql: &str| {
        parsed(sql).map(|t| {
            t.unique_keys
                .into_iter()
                .map(|k| (k.name, k.columns))
                .collect::<Vec<_>>()
        })
    };

    // Inline, in both spellings.
    assert_eq!(
        inline_of("CREATE TABLE t (id BIGINT PRIMARY KEY, email TEXT UNIQUE, name TEXT)"),
        Some(vec!["email".to_string(), "id".to_string()])
    );
    // Table-level, single column, with and without a constraint name.
    assert_eq!(
        table_level_of("CREATE TABLE t (id BIGINT, email TEXT, UNIQUE (email))"),
        Some(vec![("t_email_key".to_string(), vec!["email".to_string()])])
    );
    assert_eq!(
        table_level_of(
            "CREATE TABLE t (id BIGINT, email TEXT, CONSTRAINT uq_email UNIQUE (email))"
        ),
        Some(vec![("uq_email".to_string(), vec!["email".to_string()])])
    );
    assert_eq!(
        table_level_of("CREATE TABLE t (id BIGINT, email TEXT, PRIMARY KEY (id))"),
        Some(vec![("t_pkey".to_string(), vec!["id".to_string()])])
    );
    assert_eq!(
        table_level_of("CREATE TABLE t (id BIGINT, email TEXT, PRIMARY KEY (\"id\"))"),
        Some(vec![("t_pkey".to_string(), vec!["id".to_string()])])
    );

    // A composite key constrains the *pair*, so it is kept whole rather than
    // split across the columns it spans — enforcing it per column would reject
    // rows the constraint allows.
    assert_eq!(
        table_level_of("CREATE TABLE t (a BIGINT, b BIGINT, UNIQUE (a, b))"),
        Some(vec![(
            "t_a_b_key".to_string(),
            vec!["a".to_string(), "b".to_string()]
        )])
    );
    assert_eq!(
        // Named as PostgreSQL names a primary key, whatever its columns.
        table_level_of("CREATE TABLE t (a BIGINT, b BIGINT, PRIMARY KEY (a, b))"),
        Some(vec![(
            "t_pkey".to_string(),
            vec!["a".to_string(), "b".to_string()]
        )])
    );
    // A composite constrains no column on its own.
    assert_eq!(
        inline_of("CREATE TABLE t (a BIGINT, b BIGINT, UNIQUE (a, b))"),
        Some(Vec::new())
    );

    // And a table that declares nothing gets nothing, in either place.
    assert_eq!(
        inline_of("CREATE TABLE t (id BIGINT, name TEXT)"),
        Some(Vec::new())
    );
    assert_eq!(
        table_level_of("CREATE TABLE t (id BIGINT, name TEXT)"),
        Some(Vec::new())
    );
}

/// End to end: a `UNIQUE` written in SQL has to be enforced by the engine, and
/// a `VACUUM` written in SQL has to reach the reclamation it names.
#[tokio::test(flavor = "multi_thread")]
async fn unique_and_vacuum_work_through_sql() {
    use crate::core::database::Database;
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};

    init();
    let dir = tempfile::tempdir().unwrap();
    let db = rclite::Arc::new(Database::new(dir.path().to_str().unwrap()).await);

    let exec = async |sql: &str| -> Vec<u8> {
        let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("parse: {}", sql));
        executor::execute(purpose, db.clone()).await
    };
    // Status byte 3 is an error; 0 is success.
    let ok = |reply: &[u8]| reply.first() == Some(&0u8);

    assert!(ok(&exec(
        "CREATE TABLE people (id BIGINT PRIMARY KEY, email TEXT UNIQUE, note TEXT)"
    )
    .await));

    assert!(ok(&exec(
        "INSERT INTO people (id, email, note) VALUES (1, 'a@x', 'first')"
    )
    .await));
    assert!(ok(&exec(
        "INSERT INTO people (id, email, note) VALUES (2, 'b@x', 'second')"
    )
    .await));

    // A duplicate in either unique column is refused.
    let dup_id = exec("INSERT INTO people (id, email, note) VALUES (1, 'c@x', 'dup id')").await;
    assert!(!ok(&dup_id), "duplicate PRIMARY KEY was accepted");
    let dup_email =
        exec("INSERT INTO people (id, email, note) VALUES (3, 'a@x', 'dup email')").await;
    assert!(!ok(&dup_email), "duplicate UNIQUE was accepted");

    // A non-unique column may repeat.
    assert!(ok(&exec(
        "INSERT INTO people (id, email, note) VALUES (3, 'c@x', 'first')"
    )
    .await));

    // Deleting frees the value, and VACUUM reaches the reclamation.
    assert!(ok(&exec("DELETE FROM people WHERE id = 2").await));
    assert!(ok(&exec(
        "INSERT INTO people (id, email, note) VALUES (4, 'b@x', 'reused')"
    )
    .await));

    let reply = exec("VACUUM people").await;
    assert!(ok(&reply), "VACUUM reported an error");
    assert_eq!(
        reply.len(),
        9,
        "VACUUM reply should be a status byte plus a u64"
    );

    let reply = exec("VACUUM FULL").await;
    assert!(ok(&reply), "VACUUM FULL reported an error");

    // Naming a table that does not exist is an error, not a silent success.
    assert!(!ok(&exec("VACUUM no_such_table").await));

    // And the table still behaves after all of that.
    assert!(ok(&exec(
        "INSERT INTO people (id, email, note) VALUES (5, 'e@x', 'after vacuum')"
    )
    .await));
    assert!(!ok(&exec(
        "INSERT INTO people (id, email, note) VALUES (6, 'a@x', 'still dup')"
    )
    .await));
}

/// An UPDATE that assigns only fixed-width columns must not rewrite the row.
///
/// The general path re-encodes the whole row and writes it back, which for a
/// wide row and a one-column SET is almost entirely work that reproduces bytes
/// already on disk. The fast path writes just the assigned columns — and must
/// leave everything else, including the row's address and length, exactly where
/// it was.
#[tokio::test(flavor = "multi_thread")]
async fn a_fixed_width_update_writes_only_the_columns_it_changes() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_inplace").await;
    for i in 0..40u64 {
        insert(
            &t,
            i,
            i as i32,
            &format!("a-string-that-must-not-move-{:04}", i),
        )
        .await;
    }
    t.wait_filters_ready().await.unwrap();

    let before: Vec<(u64, u64, u64)> = all_pointers(&t)
        .await
        .into_iter()
        .filter(|p| !p.deleted)
        .map(|p| (p.row_id(), p.position, p.length as u64))
        .collect();
    let rows_len_before = t.data().unwrap().io_rows.get_len().await;
    let pointers_len_before = t.data().unwrap().io_pointers.get_len().await;

    let n = t
        .update_rows(
            "cat < 20",
            full_fetch(&t.schema.fields),
            upd(&t.schema.fields, "cat", &555i32.to_le_bytes()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(n, 20);

    // The long-row layout stores a checksum over the whole row, which this path
    // does not have the bytes to recompute, so it declines there by design.
    let expected_fast = if cfg!(feature = "enable_long_row") {
        0
    } else {
        20
    };
    assert_eq!(
        t.in_place_column_updates(),
        expected_fast,
        "the fixed-width path was not taken as expected"
    );

    // Nothing moved, nothing was appended, nothing changed length.
    let after: Vec<(u64, u64, u64)> = all_pointers(&t)
        .await
        .into_iter()
        .filter(|p| !p.deleted)
        .map(|p| (p.row_id(), p.position, p.length as u64))
        .collect();
    assert_eq!(before, after, "a fixed-width update moved or resized rows");
    assert_eq!(
        t.data().unwrap().io_rows.get_len().await,
        rows_len_before,
        "the rows file grew"
    );
    assert_eq!(
        t.data().unwrap().io_pointers.get_len().await,
        pointers_len_before,
        "a pointer record was appended"
    );

    // The change landed, the untouched columns did not.
    assert_eq!(run(&t, "cat = 555").await.len(), 20);
    for row in run(&t, "").await {
        let id = u64_of(&row, 0);
        assert_eq!(
            str_of(&row, 2),
            format!("a-string-that-must-not-move-{:04}", id)
        );
    }
    // And the filter still finds the new value by point lookup, which only
    // works if the slot was reindexed.
    assert_eq!(run(&t, "cat = 555").await.len(), 20);
    assert!(run(&t, "cat = 0").await.is_empty());
    assert_eq!(run(&t, "cat = 25").await.len(), 1);
}

/// Assigning a string still takes the general path: its bytes live after the
/// fixed columns, so changing one moves everything behind it.
#[tokio::test(flavor = "multi_thread")]
async fn a_string_update_still_takes_the_general_path() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_inplace_str").await;
    for i in 0..10u64 {
        insert(&t, i, i as i32, "short").await;
    }
    t.wait_filters_ready().await.unwrap();

    // Growing a string must relocate the row, which the fast path would not do.
    t.update_rows(
        "id = 4",
        full_fetch(&t.schema.fields),
        upd(
            &t.schema.fields,
            "label",
            b"a-considerably-longer-value-than-before",
        ),
        None,
    )
    .await
    .unwrap();

    let rows = run(&t, "id = 4").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        str_of(&rows[0], 2),
        "a-considerably-longer-value-than-before"
    );
    assert_eq!(run(&t, "").await.len(), 10);

    // Shrinking one writes in place, and must not leave the old tail readable.
    t.update_rows(
        "id = 4",
        full_fetch(&t.schema.fields),
        upd(&t.schema.fields, "label", b"x"),
        None,
    )
    .await
    .unwrap();
    assert_eq!(str_of(&run(&t, "id = 4").await[0], 2), "x");

    assert_eq!(
        t.in_place_column_updates(),
        0,
        "a string assignment took the fixed-width path"
    );
}

// ---------------------------------------------------------------------------
// The latch the in-place path depends on
// ---------------------------------------------------------------------------

/// Overwriting a version in place changes no `xmin`/`xmax`, so nothing
/// distinguishes before from after. Checking that no snapshot exists and then
/// writing is not enough — a transaction beginning in between gets a snapshot
/// that reads the new bytes for a row it is entitled to see unchanged. The
/// check and the write have to be one step, which is what the latch makes them.
#[test]
fn the_in_place_latch_holds_begin_off() {
    use crate::core::mvcc::TransactionManager;
    use std::sync::Arc as StdArc;
    use std::time::Instant;

    let manager = StdArc::new(TransactionManager::in_memory());

    // Nothing running: the claim succeeds.
    let guard = manager
        .try_begin_in_place()
        .expect("quiescent manager declined");

    // A second in-place write is fine — they cannot observe each other, only
    // transactions can observe them.
    let second = manager
        .try_begin_in_place()
        .expect("declined a concurrent in-place write");

    // The waiter reports *when* `begin` returned, not how long it took.
    //
    // Timing the wait from inside the spawned thread looked equivalent and is
    // not: under load the thread can be scheduled tens of milliseconds after
    // `spawn` returns, so it starts its clock late and measures a window
    // shorter than the one the main thread is sleeping through. That failed at
    // 145.9 ms against a 150 ms floor with the latch working perfectly. An
    // ordering assertion has no threshold to miss.
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    // Set the instant `begin` returns, so the main thread can tell "still
    // waiting" from "not there yet".
    let returned = StdArc::new(std::sync::atomic::AtomicBool::new(false));
    let waiter = {
        let manager = manager.clone();
        let returned = returned.clone();
        std::thread::spawn(move || {
            started_tx.send(()).expect("main thread is waiting");
            let tx = manager.begin();
            returned.store(true, std::sync::atomic::Ordering::Release);
            (tx, Instant::now())
        })
    };

    // The thread is running and about to call `begin`; the sleep gives it time
    // to get there and block.
    started_rx.recv().expect("waiter never started");
    std::thread::sleep(std::time::Duration::from_millis(50));

    drop(guard);
    // Still one outstanding, so `begin` must keep waiting.
    std::thread::sleep(std::time::Duration::from_millis(50));

    // Checked *before* the last release, which is what stops this passing
    // vacuously. An ordering assertion alone is satisfied by a thread that was
    // simply too slow to reach `begin` at all — it would report a later instant
    // for the wrong reason. This says it is genuinely blocked right now.
    assert!(
        !returned.load(std::sync::atomic::Ordering::Acquire),
        "begin returned while an in-place write was still outstanding"
    );

    let released_last = Instant::now();
    drop(second);

    let (tx, returned_at) = waiter.join().unwrap();
    assert!(
        returned_at >= released_last,
        "begin returned before the last in-place write was released, so it did not wait"
    );
    assert!(tx >= crate::core::mvcc::transaction::FIRST_TX);
}

/// And the other direction: a transaction already running declines the claim,
/// because that transaction may be able to tell.
#[test]
fn a_running_transaction_declines_an_in_place_write() {
    use crate::core::mvcc::TransactionManager;
    use std::sync::Arc as StdArc;

    let manager = StdArc::new(TransactionManager::in_memory());
    // Statement-scoped, so the only thing that can decline it is someone else.
    // A `BEGIN` block is declined even for its own writer — it can roll back,
    // and an overwrite keeps no version to roll back to.
    let tx = manager.begin_autocommit();

    assert!(
        manager.try_begin_in_place().is_none(),
        "claimed an in-place write while a transaction was running"
    );
    // ...but that transaction's own writer is not something to protect it from.
    assert!(
        manager.try_begin_in_place_for(tx).is_some(),
        "a transaction was blocked from overwriting in place by itself"
    );
    // A block that may still roll back is declined even for its own writer.
    let block = manager.begin();
    assert!(
        manager.try_begin_in_place_for(block).is_none(),
        "a rollback-able transaction claimed an overwrite it could not undo"
    );
    manager.abort(block);
    // A *different* transaction still blocks it.
    assert!(
        manager.try_begin_in_place_for(tx + 1000).is_none(),
        "claimed an in-place write while someone else's transaction was running"
    );

    // Once it ends, the claim succeeds again.
    manager.commit(tx);
    assert!(manager.try_begin_in_place().is_some());
}

/// End to end: the fast path declines on a versioned table with something in
/// flight, and the update still applies through the general path.
#[tokio::test(flavor = "multi_thread")]
async fn a_versioned_table_declines_the_fast_path_while_a_transaction_runs() {
    use crate::core::mvcc::TransactionManager;
    use std::sync::Arc as StdArc;

    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_inplace_mvcc").await;
    for i in 0..20u64 {
        insert(&t, i, i as i32, "a-label-that-stays-put").await;
    }
    t.wait_filters_ready().await.unwrap();

    let manager = StdArc::new(TransactionManager::in_memory());
    t.enable_versioning(manager.clone());

    // Quiescent: the fast path is available and taken.
    let n = t
        .update_rows(
            "cat < 5",
            full_fetch(&t.schema.fields),
            upd(&t.schema.fields, "cat", &111i32.to_le_bytes()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(n, 5);

    // The long-row layout stores a per-row checksum this path cannot recompute
    // from partial bytes, so it declines there by design and every count below
    // stays at zero. The *updates* must still land either way, which is what the
    // row assertions at the end check.
    let fast: u64 = if cfg!(feature = "enable_long_row") {
        0
    } else {
        1
    };

    let taken_while_quiescent = t.in_place_column_updates();
    assert_eq!(
        taken_while_quiescent,
        5 * fast,
        "the fast path was not available when nothing was running"
    );

    // The writer's *own* transaction must not block it: every statement now runs
    // under one, so testing for an empty running set would decline always and
    // the fast path would be unreachable rather than merely rare.
    //
    // Statement-scoped, which is the second half of the condition. A `BEGIN`
    // block is declined even for its own writer, because an overwrite keeps no
    // previous version and the block can still roll back — asserted below.
    let own = manager.begin_autocommit();
    let n = t
        .update_rows_in_txn(
            "cat >= 5 AND cat < 8",
            full_fetch(&t.schema.fields),
            upd(&t.schema.fields, "cat", &333i32.to_le_bytes()),
            None,
            own,
        )
        .await
        .unwrap();
    assert_eq!(n, 3);
    assert_eq!(
        t.in_place_column_updates(),
        taken_while_quiescent + 3 * fast,
        "the writer's own transaction blocked its own in-place write"
    );
    manager.commit(own);
    let taken_while_quiescent = t.in_place_column_updates();

    // A block that can still roll back is declined even though nothing else is
    // running, and the update still lands through the versioning path.
    let block = manager.begin();
    let n = t
        .update_rows_in_txn(
            "cat >= 8 AND cat < 10",
            full_fetch(&t.schema.fields),
            upd(&t.schema.fields, "cat", &444i32.to_le_bytes()),
            None,
            block,
        )
        .await
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(
        t.in_place_column_updates(),
        taken_while_quiescent,
        "a rollback-able transaction overwrote in place, leaving nothing to undo"
    );
    manager.commit(block);
    let taken_while_quiescent = t.in_place_column_updates();

    // With someone *else*'s transaction open, it must decline — and the update
    // must still happen, through the path that writes a new version.
    let reader = manager.begin();
    let n = t
        .update_rows(
            "cat >= 10 AND cat < 15",
            full_fetch(&t.schema.fields),
            upd(&t.schema.fields, "cat", &222i32.to_le_bytes()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        n, 5,
        "the update was dropped rather than taking the slow path"
    );
    assert_eq!(
        t.in_place_column_updates(),
        taken_while_quiescent,
        "the fast path fired while a transaction was running"
    );
    manager.commit(reader);

    // Both updates are visible, and nothing was lost or duplicated.
    assert_eq!(run(&t, "cat = 111").await.len(), 5);
    assert_eq!(run(&t, "cat = 222").await.len(), 5);
    assert_eq!(run(&t, "").await.len(), 20);
    for row in run(&t, "").await {
        assert_eq!(str_of(&row, 2), "a-label-that-stays-put");
    }
}

/// Reclamation end to end under versioning, across a restart.
///
/// This is the join of everything: a `DELETE` stamps `xmax` rather than
/// tombstoning, the outcome has to be durable for that stamp to mean anything,
/// and reclamation has to consult it rather than the flag. Break any one and a
/// versioned table either never reclaims or reclaims a version somebody can
/// still read.
///
/// The restart is the part worth having. A status log that persisted its extent
/// but not its bits left every transaction reading `InProgress` on reopen, which
/// made every row invisible *and* made reclamation decline forever — the same
/// root cause with two very different symptoms.
#[tokio::test(flavor = "multi_thread")]
async fn a_versioned_table_reclaims_across_a_restart() {
    use crate::core::mvcc::TransactionManager;
    use crate::core::mvcc::status::StatusLog;
    use std::sync::Arc as StdArc;

    let dir = tempfile::tempdir().unwrap();
    let status_path = dir.path().join("probe.status");

    let t = open_table(dir.path(), "wp_mvcc_reclaim").await;
    for i in 0..120u64 {
        insert(&t, i, i as i32, "a-label-with-enough-length-to-reclaim").await;
    }
    t.wait_filters_ready().await.unwrap();
    let rows_len = t.data().unwrap().io_rows.get_len().await;

    let manager = StdArc::new(TransactionManager::new(
        StatusLog::open(&status_path).expect("status log"),
    ));
    t.enable_versioning(manager.clone());

    // Begins *first*, so its id is below the deleter's and its snapshot can
    // still reach rows the delete removes. It is what has to hold the space.
    let holder = manager.begin();

    // Delete under a later transaction, and commit it.
    let deleter = manager.begin();
    let n = t
        .delete_rows_in_txn("cat < 80", full_fetch(&t.schema.fields), None, deleter)
        .await
        .unwrap();
    assert_eq!(n, 80);
    manager.commit(deleter);

    // Committed, but not below the bound: `holder` is older and still running.
    let report = t.vacuum_now().await.unwrap();
    assert_eq!(
        report.reclaimed_bytes, 0,
        "reclaimed a version an open transaction can still reach: {:?}",
        report
    );
    manager.commit(holder);

    // With it finished, the bound moves past the deleter and the space is free.
    let report = t.vacuum_now().await.unwrap();
    assert!(
        report.reclaimed_bytes > 0,
        "nothing reclaimed once every reader had finished: {:?}",
        report
    );

    // Now the restart. This models a *clean* shutdown — the flush is what a
    // controlled stop does, and with no log installed here it is what makes the
    // outcomes durable. The crash case is `survives_sigkill_during_in_place_updates`,
    // which is where routing the records through the log earns its keep.
    manager.flush().expect("flush outcomes");
    drop(manager);
    let reopened = StdArc::new(TransactionManager::new(
        StatusLog::open(&status_path).expect("status log reopen"),
    ));
    assert_eq!(
        reopened.outcome(deleter),
        crate::core::mvcc::TxOutcome::Committed,
        "the delete's outcome did not survive the restart"
    );
    assert!(
        reopened.oldest_active() > deleter,
        "a fresh manager put its bound at or below a committed transaction"
    );

    let t2 = open_table(dir.path(), "wp_mvcc_reclaim").await;
    t2.enable_versioning(reopened);
    t2.wait_filters_ready().await.unwrap();

    // The surviving rows are still there and still readable.
    assert_eq!(run(&t2, "").await.len(), 40);

    // And reclamation still works, so the refill reuses rather than extending.
    let report = t2.vacuum_now().await.unwrap();
    assert!(
        report.reclaimed_bytes > 0,
        "reclamation stopped working after a restart: {:?}",
        report
    );
    for i in 1000..1080u64 {
        insert(&t2, i, i as i32, "a-label-with-enough-length-to-reclaim").await;
    }
    assert!(
        t2.data().unwrap().free_space.reused_rows() > 0,
        "the refill extended the file instead"
    );
    assert_eq!(
        t2.data().unwrap().io_rows.get_len().await,
        rows_len,
        "the rows file grew despite reclaimable space"
    );
    assert_eq!(run(&t2, "").await.len(), 120);
}

/// A transactional UPDATE must write a new version rather than overwriting the
/// old one, or there is nothing for a rollback to restore.
#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_update_leaves_the_old_value_readable() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(1024);

    let dir = temp_testdir::TempDir::default();
    let dir_str = dir.to_string_lossy().to_string();
    let db = rclite::Arc::new(crate::core::database::Database::new(&dir_str).await);

    let run = |sql: String| {
        let db = db.clone();
        async move {
            let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&sql)
                .unwrap_or_else(|| panic!("parse {}", sql));
            crate::core::rql::executor::execute(purpose, db).await
        }
    };

    run("CREATE TABLE t (id INT NOT NULL, name TEXT)".to_string()).await;
    run("INSERT INTO t (id, name) VALUES (1, 'before')".to_string()).await;

    let tx = db.transactions.begin();
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(
        "UPDATE t SET name = 'after' WHERE id = 1",
    )
    .expect("parse update");
    let resp = crate::core::rql::executor::execute_in_txn(purpose, db.clone(), tx).await;
    assert_ne!(resp.first(), Some(&3), "update should succeed");

    let table = db.table_for_tests("t").expect("table").clone();
    let mut it = crate::core::row::row_pointer::RowPointerIterator::new(table.data().unwrap().io_pointers.clone())
        .await
        .unwrap();
    let mut stamps = Vec::new();
    loop {
        let batch = it.next_row_pointers().await.unwrap();
        if batch.is_empty() {
            break;
        }
        for p in batch {
            stamps.push((p.id, p.deleted, p.xmin, p.xmax));
        }
    }
    assert_eq!(
        stamps.len(),
        2,
        "the update must append a version rather than overwrite: {:?}",
        stamps
    );

    db.transactions.abort(tx);

    let purpose =
        crate::core::rql::lexer_s1::recognize_query_purpose("SELECT name FROM t WHERE id = 1")
            .expect("parse select");
    let resp = crate::core::rql::executor::execute(purpose, db.clone()).await;
    let text = String::from_utf8_lossy(&resp);
    assert!(
        text.contains("before"),
        "the rolled-back update must leave the old value: stamps={:?} resp={:?}",
        stamps,
        text
    );
    assert!(
        !text.contains("after"),
        "and must not leave the new one: {:?}",
        text
    );
}

/// Reclamation owns clearing filter fingerprints under versioning, and must do
/// it exactly once per slot.
///
/// A transactional `DELETE` keeps the fingerprints, because clearing them makes
/// a scan prune the slot *before decoding the pointer* — the version goes
/// unreachable underneath the visibility rule rather than invisible through it,
/// and older snapshots lose rows they are entitled to. So the clearing moves to
/// the point where reclamation has established nothing can reach the version.
///
/// Once per slot matters: re-clearing every sweep would drive the filter's write
/// counter into a checkpoint on every tick for the life of the table.
#[tokio::test(flavor = "multi_thread")]
async fn reclamation_clears_filter_fingerprints_once() {
    use crate::core::mvcc::TransactionManager;
    use std::sync::Arc as StdArc;

    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_fp_clear").await;
    for i in 0..60u64 {
        insert(&t, i, i as i32, &format!("label-{:04}", i)).await;
    }
    t.wait_filters_ready().await.unwrap();
    assert!(t.data().unwrap().filters.is_some(), "this test needs a filter");

    let manager = StdArc::new(TransactionManager::in_memory());
    t.enable_versioning(manager.clone());

    let deleter = manager.begin();
    let n = t
        .delete_rows_in_txn("cat < 30", full_fetch(&t.schema.fields), None, deleter)
        .await
        .unwrap();
    assert_eq!(n, 30);
    manager.commit(deleter);

    // The rows are gone from every snapshot's view, and the reads still work —
    // which they would not if the fingerprints had been cleared at delete time
    // while an older snapshot could still reach the versions.
    assert_eq!(run(&t, "").await.len(), 30);

    let first = t.vacuum_now().await.unwrap();
    assert_eq!(
        first.fingerprints_cleared, 30,
        "reclamation did not take over clearing: {:?}",
        first
    );

    // Same versions, already cleared. A second sweep must not touch them again.
    let second = t.vacuum_now().await.unwrap();
    assert_eq!(
        second.fingerprints_cleared, 0,
        "cleared the same slots twice; every sweep would checkpoint forever"
    );
    let third = t.vacuum_now().await.unwrap();
    assert_eq!(third.fingerprints_cleared, 0);

    // The survivors are still findable by an indexed predicate, so clearing hit
    // only the slots it was supposed to.
    for id in 30..60u64 {
        let found = run(&t, &format!("id = {}", id)).await;
        assert_eq!(found.len(), 1, "id = {} was pruned by the clearing", id);
        assert_eq!(str_of(&found[0], 2), format!("label-{:04}", id));
    }
    for id in [0u64, 15, 29] {
        assert!(
            run(&t, &format!("id = {}", id)).await.is_empty(),
            "id = {} came back",
            id
        );
    }

    // A second round of deletes gets cleared in its turn.
    let deleter = manager.begin();
    t.delete_rows_in_txn("cat < 45", full_fetch(&t.schema.fields), None, deleter)
        .await
        .unwrap();
    manager.commit(deleter);
    let fourth = t.vacuum_now().await.unwrap();
    assert_eq!(fourth.fingerprints_cleared, 15, "{:?}", fourth);
    assert_eq!(run(&t, "").await.len(), 15);
}

/// An unversioned table clears at delete time, so reclamation must not do it
/// again on every sweep.
#[tokio::test(flavor = "multi_thread")]
async fn an_unversioned_table_is_not_re_cleared_every_sweep() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_fp_unversioned").await;
    for i in 0..40u64 {
        insert(&t, i, i as i32, "label").await;
    }
    t.wait_filters_ready().await.unwrap();
    t.delete_rows("cat < 20", full_fetch(&t.schema.fields), None)
        .await
        .unwrap();

    for _ in 0..3 {
        let report = t.vacuum_now().await.unwrap();
        assert_eq!(
            report.fingerprints_cleared, 0,
            "re-cleared slots a DELETE had already cleared: {:?}",
            report
        );
    }
    assert_eq!(run(&t, "").await.len(), 20);
}

// ---------------------------------------------------------------------------
// Commit durability
// ---------------------------------------------------------------------------

/// A permanent commit failure has to stay distinguishable from a transient one.
///
/// The log poisons itself when a flush fails, because on Linux the kernel may
/// drop the dirty pages after reporting the error — so a retry could return
/// success while the data is gone. That state is permanent by design, and a
/// caller that retries on it never makes progress.
///
/// Nothing consumes the distinction yet; every current caller just fails the
/// statement. It is pinned here for the same reason the reclamation defaults
/// are: an `io::Error` has nowhere structural to carry this, so it rides in the
/// message, and a refactor that tidies the message away breaks a contract with
/// no test standing under it.
#[test]
fn a_poisoned_commit_is_not_mistaken_for_a_transient_one() {
    use crate::core::storage_providers::traits::{POISONED_LOG, is_poisoned};

    let poisoned = std::io::Error::other(format!("{}: flush failed", POISONED_LOG));
    assert!(is_poisoned(&poisoned), "a poisoned log read as retryable");

    // Everything else is transient as far as this classification goes.
    for transient in [
        std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "short read"),
        std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
        std::io::Error::other("WAL commit failed: disk full"),
        std::io::Error::other(""),
    ] {
        assert!(
            !is_poisoned(&transient),
            "a transient failure read as permanent: {}",
            transient
        );
    }
}

/// The trait's default commit is a success, so a provider with no log is not
/// forced to invent one — and every caller can use `?` unconditionally.
#[tokio::test(flavor = "multi_thread")]
async fn a_provider_without_a_log_commits_successfully() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_commit_default").await;

    // These all end in a `commit_durable(...)?`, so a default that returned an
    // error would fail every write on a provider with no log installed.
    for i in 0..10u64 {
        insert(&t, i, i as i32, "label").await;
    }
    t.wait_filters_ready().await.unwrap();
    assert_eq!(
        t.delete_rows("cat < 3", full_fetch(&t.schema.fields), None)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        t.update_rows(
            "cat >= 5",
            full_fetch(&t.schema.fields),
            upd(&t.schema.fields, "cat", &9i32.to_le_bytes()),
            None
        )
        .await
        .unwrap(),
        5
    );
    t.compact().await.unwrap();
    assert_eq!(run(&t, "").await.len(), 7);
}

/// A filter checkpoint reaches disk on a fresh table with a log installed.
///
/// This is the coverage a sketch from another session was reaching for, on the
/// theory that compaction's `checkpoint_filters` — an in-place write at offset
/// zero immediately after a truncation — was being silently dropped. It is not:
/// the sidecar is created up front by `create_new`, and the checkpoint lands.
///
/// The sketch failed for a different reason, which this test deliberately does
/// *not* assert, because it is a bug rather than a contract: after a
/// WAL-deferred in-place write that extends a file, the writing provider's
/// cached length is stale, so it cannot read back what it just wrote. The
/// on-disk bytes are the ground truth here for exactly that reason.
#[tokio::test(flavor = "multi_thread")]
async fn a_filter_checkpoint_reaches_disk_with_a_log_installed() {
    use crate::core::database::Database;
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};

    init();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let db = rclite::Arc::new(Database::new(&path).await);
    assert!(
        crate::core::wal::wal_for(&path).is_some(),
        "this test is only meaningful with a log installed"
    );

    let exec = async |sql: &str| {
        let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("parse: {}", sql));
        executor::execute(purpose, db.clone()).await
    };
    exec("CREATE TABLE flt (id UBIGINT, name VARCHAR);").await;
    for i in 0..200u64 {
        exec(&format!(
            "INSERT INTO flt (id, name) VALUES ({}, 'row-{}');",
            i, i
        ))
        .await;
    }

    {
        let table = db.table_for_tests("flt").expect("table");
        table.wait_filters_ready().await.unwrap();
        assert!(table.data().unwrap().filters.is_some(), "no catalog to checkpoint");
        table.checkpoint_filters().await.unwrap();
    }

    let on_disk = std::fs::metadata(dir.path().join("flt_flt.db"))
        .map(|m| m.len())
        .unwrap_or(0);
    assert!(on_disk > 0, "the filter checkpoint never reached disk");

    // And a reader that opens the file fresh — which is what `Table::new` does —
    // sees it, so the next open resumes from the checkpoint rather than
    // rescanning.
    let reopened = crate::core::storage_providers::file_sync::LocalStorageProvider::new(
        dir.path().to_str().unwrap(),
        Some("flt_flt.db"),
    )
    .await;
    assert_eq!(reopened.get_len().await, on_disk);
}

/// Truncation has to retract the in-place high-water mark, not just the length.
///
/// `write_data_txn` records how far an in-place write reaches in `inplace_end`,
/// and `commit_durable` raises `file_len` to it — that is what lets a provider
/// read back a write it deferred through the log. The mark only ever grows.
///
/// Truncation makes any claim past the new end false. Leaving it meant the
/// *next* in-place commit restored the pre-truncation length, so the provider
/// reported a file longer than it was and `RowPointerIterator` would walk
/// records that do not exist. Reachable in one step: `compact()` truncates the
/// pointer file, and the next `DELETE` is an in-place write that commits.
#[tokio::test(flavor = "multi_thread")]
async fn truncation_retracts_the_in_place_high_water_mark() {
    use crate::core::storage_providers::file_sync::LocalStorageProvider;
    use crate::core::storage_providers::traits::StorageIO;

    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_string_lossy().to_string();
    let wal_path = dir.path().join(crate::core::wal::DEFAULT_WAL_NAME);
    let wal = crate::core::wal::Wal::open(&wal_path).unwrap();
    crate::core::wal::install_for(&dir_str, std::sync::Arc::new(wal));

    let io = LocalStorageProvider::new(&dir_str, Some("probe.db")).await;
    let file = dir.path().join("probe.db");
    let on_disk = || std::fs::metadata(&file).unwrap().len();

    io.append_data(&vec![7u8; 4096], true).await;

    // An in-place write near the end raises the mark to 4096.
    let txn = io.begin_txn();
    let token = io.write_data_txn(txn, 4000, &[1u8; 96]).await;
    io.commit_durable(token).await.unwrap();
    assert_eq!(io.get_len().await, 4096);

    io.truncate(1024).await.unwrap();
    assert_eq!(io.get_len().await, 1024);
    assert_eq!(on_disk(), 1024);

    // The commit that would have restored the stale mark.
    let txn = io.begin_txn();
    let token = io.write_data_txn(txn, 0, &[2u8; 8]).await;
    io.commit_durable(token).await.unwrap();

    assert_eq!(
        io.get_len().await,
        on_disk(),
        "the provider reported a longer file than it has, so a scan would read records that do not exist"
    );
    assert_eq!(io.get_len().await, 1024);

    // And a read past the real end still refuses rather than returning stale
    // mmap bytes.
    let mut buf = [0u8; 16];
    let mut pos = 2048u64;
    assert!(io.read_data_into_buffer(&mut pos, &mut buf).await.is_err());
}

/// A `DROP TABLE` has to survive a restart.
///
/// The table's own files are released immediately, but the row `add_table`
/// wrote into the database catalogue records that the table *exists*, and
/// `Database::new` rebuilds one `Table` per catalogue row. Leaving that row
/// behind resurrects the table on the next open — with its data gone, because
/// the files really were released. Schema comes back, rows do not.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_table_stays_dropped_across_a_restart() {
    use crate::core::database::Database;
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};

    init();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = rclite::Arc::new(Database::new(&path).await);
        let exec = async |sql: &str| {
            let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("parse: {}", sql));
            executor::execute(purpose, db.clone()).await
        };
        exec("CREATE TABLE keep_me (x INT);").await;
        exec("CREATE TABLE drop_me (x INT);").await;
        exec("INSERT INTO drop_me (x) VALUES (1);").await;

        assert!(db.has_table("drop_me"));
        assert!(
            db.drop_table("drop_me").await.unwrap(),
            "drop reported nothing removed"
        );
        assert!(
            !db.has_table("drop_me"),
            "still present in the live catalogue"
        );
    }

    let reopened = Database::new(&path).await;
    assert!(
        reopened.has_table("keep_me"),
        "the drop took a table with it that it should not have"
    );
    assert!(
        !reopened.has_table("drop_me"),
        "the dropped table came back after a restart"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_insert_gives_its_space_back() {
    use crate::core::mvcc::TransactionManager;
    use std::sync::Arc as StdArc;

    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_aborted_insert").await;
    for i in 0..10u64 {
        insert(&t, i, i as i32, "committed-and-staying").await;
    }
    t.wait_filters_ready().await.unwrap();

    let manager = StdArc::new(TransactionManager::in_memory());
    t.enable_versioning(manager.clone());

    // Nothing dead yet, so whatever the sweep reclaims below is attributable to
    // the rolled-back rows rather than to leftovers from the setup.
    let baseline = t.vacuum_now().await.unwrap();
    assert_eq!(
        baseline.reclaimed_bytes, 0,
        "the table was not quiescent before the rolled-back insert"
    );

    // Rows under a transaction that *commits*. The setup rows above carry a
    // sentinel `xmin` that the guard protects on its own, so without these the
    // "did it reclaim too much" assertion below could never fail on the new
    // branch and would be checking nothing.
    let kept = manager.begin();
    for i in 50..60u64 {
        let mut rw = RowWrite {
            columns_writing_data: SmallVec::new(),
        };
        rw.columns_writing_data
            .push(payload(&i.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data
            .push(payload(&(i as i32).to_le_bytes(), 1, DbType::I32));
        rw.columns_writing_data
            .push(payload(b"committed-under-a-txn", 2, DbType::STRING));
        t.insert_row_in_txn(rw, kept).await.unwrap();
    }
    manager.commit(kept);

    // A batch that fails part-way: the rows are written and durable — the WAL
    // commits per row — and then the transaction that stamped them aborts.
    let tx = manager.begin();
    for i in 100..120u64 {
        let mut rw = RowWrite {
            columns_writing_data: SmallVec::new(),
        };
        rw.columns_writing_data
            .push(payload(&i.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data
            .push(payload(&(i as i32).to_le_bytes(), 1, DbType::I32));
        rw.columns_writing_data
            .push(payload(b"rolled-back-payload", 2, DbType::STRING));
        t.insert_row_in_txn(rw, tx).await.unwrap();
    }
    manager.abort(tx);

    // The rows are invisible, which is MVCC doing its job and says nothing
    // about their bytes.
    let visible = run(&t, "id >= 100").await;
    assert!(
        visible.is_empty(),
        "a rolled-back INSERT was visible to a later reader"
    );

    let report = t.vacuum_now().await.unwrap();
    assert!(
        report.reclaimed_bytes > 0,
        "a rolled-back INSERT leaked its bytes: the sweep reclaimed nothing"
    );

    // `vacuum_now` forces, so the assertion above says the predicate works and
    // nothing about whether the background service would ever get here. The
    // trigger reads `dead_bytes` off the scan rather than off a counter that
    // DELETE and UPDATE maintain, which is the only reason rolled-back rows
    // count at all — a counter-based trigger would have left them reclaimable
    // in principle and untouched in practice.
    let cfg = crate::core::vacuum::VacuumConfig::default();
    assert!(
        report.dead_bytes >= cfg.min_dead_bytes || report.dead_ratio() >= cfg.min_dead_ratio,
        "rolled-back rows counted as dead but stayed under both trigger thresholds          ({} bytes, ratio {:.3}), so an unforced sweep would never reclaim them",
        report.dead_bytes,
        report.dead_ratio()
    );

    // The committed rows are the point of the assertion above being a *lower*
    // bound rather than an exact figure: reclaiming too much is the failure
    // that matters, and it shows up here.
    let survivors = run(&t, "id < 10").await;
    assert_eq!(
        survivors.len(),
        10,
        "reclaiming the rolled-back rows took pre-versioning rows with them"
    );
    // Freeing an extent does not disturb the bytes in it, so a row whose space
    // was wrongly handed back still reads correctly until something reuses it.
    // Counting rows here would pass even while the sweep was giving away live
    // extents — it did, when this was written. Forcing the reuse is what turns
    // the mistake into an observable one.
    for i in 200..230u64 {
        insert(&t, i, i as i32, "committed-under-a-txn").await;
    }

    let committed = run(&t, "id >= 50 AND id < 60").await;
    assert_eq!(
        committed.len(),
        10,
        "reclaiming the rolled-back rows took a committed transaction's rows with them"
    );
    for row in &committed {
        let label = str_of(row, 2);
        assert_eq!(
            label, "committed-under-a-txn",
            "a later insert reused an extent still owned by a committed row"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_update_frees_the_new_version_and_keeps_the_old() {
    use crate::core::mvcc::TransactionManager;
    use std::sync::Arc as StdArc;

    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_aborted_update").await;
    for i in 0..10u64 {
        insert(&t, i, i as i32, "short").await;
    }
    t.wait_filters_ready().await.unwrap();

    let manager = StdArc::new(TransactionManager::in_memory());
    t.enable_versioning(manager.clone());

    // A relocating update — the new label does not fit in place — under a
    // transaction that then rolls back. This is the one case that lands on both
    // sides of the predicate at once, and they must not be symmetric:
    //
    //   new version: `xmin` aborted, never visible to anything  -> free it
    //   old version: `xmax` aborted, the delete never happened   -> keep it
    //
    // Getting the second one wrong loses committed data, so the halves are
    // asserted separately rather than through a single row count.
    let tx = manager.begin();
    let n = t
        .update_rows_in_txn(
            "cat < 5",
            full_fetch(&t.schema.fields),
            upd(
                &t.schema.fields,
                "label",
                b"a-much-longer-label-forcing-relocation",
            ),
            None,
            tx,
        )
        .await
        .unwrap();
    assert_eq!(n, 5);
    manager.abort(tx);

    let report = t.vacuum_now().await.unwrap();
    assert!(
        report.reclaimed_bytes > 0,
        "the rolled-back update's new versions leaked their bytes"
    );

    // Force reuse: a wrongly-freed extent still reads correctly until something
    // writes over it, so counting rows here would pass either way.
    for i in 100..130u64 {
        insert(&t, i, i as i32, "filler-to-reuse-the-freed-extents").await;
    }

    let originals = run(&t, "cat < 5").await;
    assert_eq!(
        originals.len(),
        5,
        "an aborted UPDATE reclaimed the rows it failed to update"
    );
    for row in &originals {
        assert_eq!(
            str_of(row, 2),
            "short",
            "an aborted UPDATE left the new version visible, or its extent was reused"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn uniqueness_sees_the_transactions_own_uncommitted_rows() {
    use crate::core::mvcc::TransactionManager;
    use std::sync::Arc as StdArc;

    // The failure this guards is a read path that isn't transaction-aware:
    // `check_unique` scans for an existing value, and if it scanned with a
    // reader that cannot see the transaction's own writes, the second insert
    // of a duplicate inside one transaction would find nothing and be allowed.
    // Both rows would then commit together and the "unique" column would hold
    // a duplicate, with no error at any point.
    let dir = tempfile::tempdir().unwrap();
    let t = open_unique_table(dir.path(), "wp_unique_txn").await;
    for i in 0..10u64 {
        insert(&t, i, i as i32, &format!("label-{}", i)).await;
    }
    t.wait_filters_ready().await.unwrap();

    let manager = StdArc::new(TransactionManager::in_memory());
    t.enable_versioning(manager.clone());

    let tx = manager.begin();

    let mut rw = RowWrite {
        columns_writing_data: SmallVec::new(),
    };
    rw.columns_writing_data
        .push(payload(&100u64.to_le_bytes(), 0, DbType::U64));
    rw.columns_writing_data
        .push(payload(&1i32.to_le_bytes(), 1, DbType::I32));
    rw.columns_writing_data
        .push(payload(b"claimed-inside-the-txn", 2, DbType::STRING));
    t.insert_row_in_txn(rw, tx).await.expect("first insert");

    // Same unique label, same transaction, not yet committed.
    let mut rw = RowWrite {
        columns_writing_data: SmallVec::new(),
    };
    rw.columns_writing_data
        .push(payload(&101u64.to_le_bytes(), 0, DbType::U64));
    rw.columns_writing_data
        .push(payload(&1i32.to_le_bytes(), 1, DbType::I32));
    rw.columns_writing_data
        .push(payload(b"claimed-inside-the-txn", 2, DbType::STRING));
    let err = t
        .insert_row_in_txn(rw, tx)
        .await
        .expect_err("a transaction was allowed to duplicate its own unique value");
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists, "{}", err);

    manager.commit(tx);

    // Exactly one row holds the value after the commit.
    let rows = run(&t, "").await;
    let claimed = rows
        .iter()
        .filter(|r| str_of(r, 2) == "claimed-inside-the-txn")
        .count();
    assert_eq!(claimed, 1, "the unique column ended up holding a duplicate");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_insert_frees_the_unique_value_it_claimed() {
    use crate::core::mvcc::TransactionManager;
    use std::sync::Arc as StdArc;

    // The other direction, and the one that fails the opposite way: if the
    // check counted rows an aborted transaction wrote, a value would stay
    // claimed by a write that never happened and could never be used again.
    // Testing only the direction above would pass with a reader that sees
    // everything, which is the reader that breaks this one.
    let dir = tempfile::tempdir().unwrap();
    let t = open_unique_table(dir.path(), "wp_unique_rollback").await;
    for i in 0..10u64 {
        insert(&t, i, i as i32, &format!("label-{}", i)).await;
    }
    t.wait_filters_ready().await.unwrap();

    let manager = StdArc::new(TransactionManager::in_memory());
    t.enable_versioning(manager.clone());

    let doomed = manager.begin();
    let mut rw = RowWrite {
        columns_writing_data: SmallVec::new(),
    };
    rw.columns_writing_data
        .push(payload(&200u64.to_le_bytes(), 0, DbType::U64));
    rw.columns_writing_data
        .push(payload(&1i32.to_le_bytes(), 1, DbType::I32));
    rw.columns_writing_data
        .push(payload(b"briefly-claimed", 2, DbType::STRING));
    t.insert_row_in_txn(rw, doomed).await.expect("insert");
    manager.abort(doomed);

    // A later transaction may take the value the rollback gave up.
    let winner = manager.begin();
    let mut rw = RowWrite {
        columns_writing_data: SmallVec::new(),
    };
    rw.columns_writing_data
        .push(payload(&201u64.to_le_bytes(), 0, DbType::U64));
    rw.columns_writing_data
        .push(payload(&1i32.to_le_bytes(), 1, DbType::I32));
    rw.columns_writing_data
        .push(payload(b"briefly-claimed", 2, DbType::STRING));
    t.insert_row_in_txn(rw, winner)
        .await
        .expect("a rolled-back insert kept its unique value reserved forever");
    manager.commit(winner);

    let rows = run(&t, "").await;
    let claimed: Vec<u64> = rows
        .iter()
        .filter(|r| str_of(r, 2) == "briefly-claimed")
        .map(|r| u64_of(r, 0))
        .collect();
    assert_eq!(claimed, vec![201], "the wrong row holds the value");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scan_does_not_fetch_pointers_whose_extents_were_reclaimed() {
    use crate::core::mvcc::TransactionManager;
    use std::sync::Arc as StdArc;

    // Under MVCC `RowPointer::delete` stamps `xmax` and deliberately leaves
    // `deleted` false, and nothing sets it afterwards — so the scan's
    // `if pointer.deleted { continue }` gate matched nothing, and every
    // transactionally-deleted row still reached the predicate fetch. Once
    // reclamation freed its extent and an insert reused it, that fetch read
    // another row's bytes. In production this showed as 32k "Invalid string
    // size" failures from 60 statements, every one with `deleted == false` and
    // a non-zero `xmax`.
    //
    // This reproduces the state — dead pointers whose extents have been
    // reclaimed and handed to new rows — and asserts the scan still returns
    // exactly the live set.
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_reclaimed_scan").await;
    for i in 0..40u64 {
        insert(&t, i, i as i32, "identical-width-payload").await;
    }
    t.wait_filters_ready().await.unwrap();

    let manager = StdArc::new(TransactionManager::in_memory());
    t.enable_versioning(manager.clone());

    let deleter = manager.begin();
    let removed = t
        .delete_rows_in_txn("cat < 20", full_fetch(&t.schema.fields), None, deleter)
        .await
        .unwrap();
    assert_eq!(removed, 20);
    manager.commit(deleter);

    // Their pointers still read as live: this is the state the gate has to
    // handle, and asserting it here means the test fails loudly if the delete
    // path ever starts tombstoning instead of stamping.
    let report = t.vacuum_now().await.unwrap();
    assert!(
        report.reclaimed_bytes > 0,
        "nothing was reclaimed, so nothing is reused"
    );

    // Same width, so the freed extents are genuinely reusable and the stale
    // pointers now address bytes belonging to these rows.
    for i in 100..120u64 {
        insert(&t, i, i as i32, "identical-width-payload").await;
    }

    let mut ids: Vec<u64> = run(&t, "").await.iter().map(|r| u64_of(r, 0)).collect();
    ids.sort();
    let expected: Vec<u64> = (20..40u64).chain(100..120u64).collect();
    assert_eq!(
        ids, expected,
        "the scan returned the wrong set over reclaimed-and-reused extents"
    );

    // And a predicate that has to fetch each row to evaluate it, which is the
    // path that was fetching through stale pointers.
    let mut ids: Vec<u64> = run(&t, "label = 'identical-width-payload'")
        .await
        .iter()
        .map(|r| u64_of(r, 0))
        .collect();
    ids.sort();
    assert_eq!(
        ids, expected,
        "the WHERE-filtered scan disagreed with the unfiltered one"
    );
}

#[test]
fn column_lookup_uses_write_order_not_position() {
    use crate::core::row::row::{Column, Row, column_vec_into_vec};
    use crate::core::row::schema::SchemaField;
    use crate::memory_pool::MemoryBlock;
    use std::borrow::Cow;

    // A projection: the caller passes the columns a SELECT asked for, not the
    // whole field list. Their `write_order`s are the table's, so they do not
    // start at zero and are not contiguous — indexing this slice with one is
    // out of range for exactly the reason it panicked a pgwire worker with
    // "len is 22 but the index is 22".
    let mut projected: SmallVec<[SchemaField; 20]> = SmallVec::new();
    projected.push(SchemaField::new(
        "late".into(),
        DbType::I32,
        4,
        0,
        21,
        false,
    ));
    projected.push(SchemaField::new(
        "later".into(),
        DbType::I32,
        4,
        4,
        22,
        false,
    ));

    let mut mb_a = MEMORY_POOL.acquire(4);
    mb_a.into_slice_mut().copy_from_slice(&7i32.to_le_bytes());
    let mut mb_b = MEMORY_POOL.acquire(4);
    mb_b.into_slice_mut().copy_from_slice(&9i32.to_le_bytes());

    let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
    columns.push(Column {
        schema_id: 21,
        data: mb_a.clone(),
        column_type: DbType::I32,
    });
    columns.push(Column {
        schema_id: 22,
        data: mb_b.clone(),
        column_type: DbType::I32,
    });
    let row = Row {
        position: 0,
        length: 8,
        columns,
    };

    // Would have indexed past the end of a two-element slice.
    assert!(
        row.get_column("later", &projected).is_some(),
        "lookup by write_order failed"
    );
    assert!(
        row.get_column("late", &projected).is_some(),
        "lookup by write_order failed"
    );
    assert!(row.get_column("absent", &projected).is_none());

    // And the naming path returns the names those write_orders actually carry,
    // rather than whatever sits at positions 21 and 22.
    let mut out: SmallVec<[(Cow<'_, str>, MemoryBlock); 20]> = SmallVec::new();
    column_vec_into_vec(&mut out, &row.columns.to_vec(), &projected);
    let names: Vec<&str> = out.iter().map(|(n, _)| n.as_ref()).collect();
    assert_eq!(
        names,
        vec!["late", "later"],
        "output columns were named by position"
    );
}

#[test]
fn a_row_view_returns_absent_for_a_column_the_fetch_skipped() {
    use crate::core::processor::concurrent_processor::{RowView, build_write_order_map};
    use crate::core::processor::transformer::RowBlocks;
    use crate::core::row::row::{Column, Row};
    use crate::core::row::schema::SchemaField;

    // Three columns in the schema, one of them not fetched. That leaves the
    // `u8::MAX` "absent" sentinel in the map, and the only thing between the
    // sentinel and `columns.get_unchecked(255)` was a `debug_assert!` — which
    // is compiled out of the release build. So this case was undefined
    // behaviour in production and passed in tests, where the assert fires.
    let mut schema: SmallVec<[SchemaField; 20]> = SmallVec::new();
    schema.push(SchemaField::new("a".into(), DbType::I32, 4, 0, 0, false));
    schema.push(SchemaField::new("b".into(), DbType::I32, 4, 4, 1, false));
    schema.push(SchemaField::new("c".into(), DbType::I32, 4, 8, 2, false));

    let mut mb = MEMORY_POOL.acquire(4);
    mb.into_slice_mut().copy_from_slice(&5i32.to_le_bytes());

    // Only write_order 0 was fetched.
    let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
    columns.push(Column {
        schema_id: 0,
        data: mb,
        column_type: DbType::I32,
    });
    let row = Row {
        position: 0,
        length: 4,
        columns,
    };

    let mut map: SmallVec<[u8; 32]> = SmallVec::new();
    build_write_order_map(&mut map, &row, schema.as_slice());
    let view = RowView::new(&row, &map);

    assert_eq!(view.mb_at(0).into_slice(), &5i32.to_le_bytes());
    assert!(
        view.mb_at(1).into_slice().is_empty(),
        "an unfetched column was not absent"
    );
    assert!(
        view.mb_at(2).into_slice().is_empty(),
        "an unfetched column was not absent"
    );

    // And a write_order past the end of the map — the other unchecked access,
    // reachable whenever a write_order exceeds the field count.
    assert!(
        view.mb_at(99).into_slice().is_empty(),
        "an out-of-range lookup was not absent"
    );
}

// ---------------------------------------------------------------------------
// A schema that cannot be read
// ---------------------------------------------------------------------------

/// Opening a table whose stored schema will not load must not fabricate an
/// empty one.
///
/// The fabricated schema has no columns, so `SELECT *` succeeds and returns
/// rows with no fields — which reads as "this table has no columns" rather than
/// as a failure. Worse, the fallback used to *save* it, replacing the only copy
/// of the real schema on disk. Found via a KV-backed table that came back from
/// a clean restart with its rows intact and no columns.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_whose_schema_will_not_load_is_flagged_rather_than_emptied() {
    init();
    let dir = tempfile::tempdir().unwrap();
    let name = "wreck";

    // A real table, with columns and a row.
    {
        let t = open_unique_table(dir.path(), name).await;
        insert(&t, 1, 7, "kept").await;
        assert_eq!(t.schema.fields.len(), 3);
        assert!(
            !t.schema_unreadable,
            "a table that opened cleanly is not flagged"
        );
    }

    // Truncate the schema, the way a recovery that never stored it leaves it.
    let schema_path = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            let f = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
            f == format!("{name}_schema.db")
        })
        .unwrap_or_else(|| {
            panic!(
                "no schema file for '{name}' in {:?}",
                std::fs::read_dir(dir.path())
                    .unwrap()
                    .filter_map(|e| e.ok())
                    .map(|e| e.file_name())
                    .collect::<Vec<_>>()
            )
        });
    std::fs::write(&schema_path, b"").unwrap();

    // Reopened the way the database opens an existing table: no columns.
    let io = Arc::new(LocalStorageProvider::new(dir.path().to_str().unwrap(), Some(name)).await);
    let reopened = Table::new(name, io, vec![]).await;
    assert!(
        reopened.schema_unreadable,
        "a schema that did not load must be reported, not replaced with an empty one"
    );

    // And the truncated file was not overwritten with a fabricated schema —
    // whatever is still on disk is the only copy there is.
    assert_eq!(
        std::fs::metadata(&schema_path).unwrap().len(),
        0,
        "the failed load must not have written a schema of its own"
    );
}

/// The vacuum service sweeps on dead stamps, not on "something changed", so
/// the write path must count one per row a DELETE removes and per row an
/// UPDATE replaces — and nothing for an insert or a statement matching nothing.
#[tokio::test(flavor = "multi_thread")]
async fn deletes_and_updates_count_their_dead_stamps() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_dead_stamps").await;
    for i in 0..10u64 {
        insert(&t, i, i as i32, "s").await;
    }
    assert_eq!(t.dead_stamps(), 0, "inserts stamp nothing dead");

    t.delete_rows("id < 3", full_fetch(&t.schema.fields), None)
        .await
        .unwrap();
    assert_eq!(t.dead_stamps(), 3);

    t.delete_rows("id = 999", full_fetch(&t.schema.fields), None)
        .await
        .unwrap();
    assert_eq!(t.dead_stamps(), 3, "a delete matching nothing stamps nothing");

    t.update_rows(
        "id >= 8",
        full_fetch(&t.schema.fields),
        upd(&t.schema.fields, "label", b"a much longer label than before"),
        None,
    )
    .await
    .unwrap();
    assert_eq!(t.dead_stamps(), 5);
}

/// A DELETE that stamps some rows and then meets a write conflict fails — and
/// must give its storage transaction up. It used to return with that
/// transaction open: the in-place stamps it had logged stayed registered as
/// pending in the WAL, which keeps `checkpoint` declining for the life of the
/// process, so the log grew without bound after a single conflict.
#[tokio::test(flavor = "multi_thread")]
async fn a_conflicting_write_leaves_no_storage_transaction_open() {
    use crate::core::mvcc::TransactionManager;

    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_string_lossy().to_string();
    let wal_path = dir.path().join(crate::core::wal::DEFAULT_WAL_NAME);
    let wal = std::sync::Arc::new(crate::core::wal::Wal::open(&wal_path).unwrap());
    crate::core::wal::install_for(&dir_str, wal.clone());

    let t = open_table(dir.path(), "wp_abort_txn").await;
    let manager = std::sync::Arc::new(TransactionManager::in_memory());
    t.enable_versioning(manager.clone());
    for i in 0..8u64 {
        let tx = manager.begin_autocommit();
        let mut rw = RowWrite {
            columns_writing_data: SmallVec::new(),
        };
        rw.columns_writing_data
            .push(payload(&i.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data
            .push(payload(&(i as i32).to_le_bytes(), 1, DbType::I32));
        rw.columns_writing_data.push(payload(b"s", 2, DbType::STRING));
        t.insert_row_in_txn(rw, tx).await.unwrap();
        manager.commit(tx);
    }
    assert_eq!(wal.pending_transactions(), 0);

    // Another transaction holds the last row.
    let holder = manager.begin();
    t.delete_rows_in_txn("id = 7", full_fetch(&t.schema.fields), None, holder)
        .await
        .unwrap();

    // This one stamps rows 0..6 and then conflicts on row 7.
    let loser = manager.begin_autocommit();
    let outcome = t
        .delete_rows_in_txn("id >= 0", full_fetch(&t.schema.fields), None, loser)
        .await;
    assert!(outcome.is_err(), "the delete must conflict on the held row");
    manager.abort(loser);
    manager.commit(holder);

    assert_eq!(
        wal.pending_transactions(),
        0,
        "the failed statement's storage transaction is still open in the log"
    );
    assert!(wal.checkpoint().unwrap(), "the log can be checkpointed again");

    // And the update path, conflicting mid-scan after rewriting earlier rows.
    let rows_before = run(&t, "").await.len();
    let tx = manager.begin_autocommit();
    let holder = manager.begin();
    t.delete_rows_in_txn("id = 6", full_fetch(&t.schema.fields), None, holder)
        .await
        .unwrap();
    let r = t
        .update_rows_in_txn(
            "id >= 0",
            full_fetch(&t.schema.fields),
            upd(&t.schema.fields, "label", b"a label long enough to relocate the row"),
            None,
            tx,
        )
        .await;
    assert!(r.is_err());
    manager.abort(tx);
    manager.abort(holder);
    assert_eq!(wal.pending_transactions(), 0, "the failed UPDATE left its transaction open");
    assert_eq!(run(&t, "").await.len(), rows_before, "nothing the failed statements did is visible");
}

/// COUNT(*) over a million rows written by many transactions, with the status
/// log settled and not: what the settled lookup costs a scan.
///
/// `cargo test --release -p rasterizeddb_core --lib -- --ignored count_scan_with_settled_outcomes --nocapture`
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn count_scan_with_settled_outcomes() {
    use crate::core::mvcc::TransactionManager;
    use crate::core::mvcc::status::StatusLog;

    const ROWS: u64 = 1_000_000;
    for settle in [false, true, false, true] {
        let dir = tempfile::tempdir().unwrap();
        let t = open_table(dir.path(), "wp_count_settled").await;
        let manager = std::sync::Arc::new(TransactionManager::new(StatusLog::in_memory_with(settle)));
        t.enable_versioning(manager.clone());
        // Rows in batches of 100 per transaction, with a few hundred thousand
        // read-only transactions between, as a server's ids go.
        let mut i = 0u64;
        while i < ROWS {
            let tx = manager.begin();
            for _ in 0..100 {
                let mut rw = RowWrite {
                    columns_writing_data: SmallVec::new(),
                };
                rw.columns_writing_data
                    .push(payload(&i.to_le_bytes(), 0, DbType::U64));
                rw.columns_writing_data
                    .push(payload(&(i as i32).to_le_bytes(), 1, DbType::I32));
                rw.columns_writing_data.push(payload(b"x", 2, DbType::STRING));
                t.insert_row_in_txn(rw, tx).await.unwrap();
                i += 1;
            }
            manager.commit(tx);
            for _ in 0..30 {
                let r = manager.begin();
                manager.commit(r);
            }
        }
        let settled = manager.settle_now();
        let empty = full_fetch(&smallvec::SmallVec::new());
        for round in 0..3 {
            let reader = manager.begin();
            let start = std::time::Instant::now();
            let n = t
                .count_rows_in_txn("", &t.schema.fields.to_vec(), empty.clone(), reader)
                .await
                .unwrap();
            let took = start.elapsed();
            manager.commit(reader);
            assert_eq!(n, ROWS);
            println!(
                "settle {:>5} (below {:>8} of {:>8}) round {}: COUNT(*) {:?} ({:.1} ns/row)",
                settle,
                settled,
                manager.next_id(),
                round,
                took,
                took.as_nanos() as f64 / ROWS as f64
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Forgetting aborted ids
// ---------------------------------------------------------------------------

fn forget_row(id: u64) -> RowWrite {
    let mut rw = RowWrite {
        columns_writing_data: SmallVec::new(),
    };
    rw.columns_writing_data
        .push(payload(&id.to_le_bytes(), 0, DbType::U64));
    rw.columns_writing_data
        .push(payload(&(id as i32).to_le_bytes(), 1, DbType::I32));
    rw.columns_writing_data.push(payload(b"v", 2, DbType::STRING));
    rw
}

async fn visible_ids(t: &Table<LocalStorageProvider>) -> Vec<u64> {
    let mut ids: Vec<u64> = run(t, "").await.iter().map(|r| u64_of(r, 0)).collect();
    ids.sort();
    ids
}

/// Inserts `n` rows from `from`, one transaction each: every third aborts.
/// Then, for some committed rows, a delete that aborts. Returns the ids that
/// must stay visible.
async fn forget_history(
    t: &Table<LocalStorageProvider>,
    manager: &std::sync::Arc<crate::core::mvcc::TransactionManager>,
    from: u64,
    n: u64,
) -> Vec<u64> {
    let mut committed = Vec::new();
    for id in from..from + n {
        let tx = manager.begin();
        t.insert_row_in_txn(forget_row(id), tx).await.unwrap();
        if id % 3 == 0 {
            manager.abort(tx);
        } else {
            manager.commit(tx);
            committed.push(id);
        }
    }
    for id in committed.iter().copied().filter(|id| id % 10 == 1) {
        let tx = manager.begin();
        let n = t
            .delete_rows_in_txn(&format!("id = {}", id), full_fetch(&t.schema.fields), None, tx)
            .await
            .unwrap();
        assert_eq!(n, 1);
        manager.abort(tx);
    }
    committed
}

/// Aborted inserts and aborted deletes, swept and forgotten: the ids answer
/// "committed" now, and still no aborted row is visible and no row an aborted
/// delete touched has gone — live, and after a reopen of both the table and
/// the status log, with the forgotten horizon read back from the sidecar.
#[tokio::test(flavor = "multi_thread")]
async fn forgetting_aborted_ids_never_makes_a_row_visible_or_invisible() {
    use crate::core::mvcc::TransactionManager;
    use crate::core::mvcc::status::StatusLog;
    use crate::core::mvcc::TxOutcome;

    let dir = tempfile::tempdir().unwrap();
    let status_path = dir.path().join("tx.status");
    let committed;
    let aborted_id;
    {
        let t = open_table(dir.path(), "wp_forget").await;
        let manager = std::sync::Arc::new(TransactionManager::new(
            StatusLog::open_with(&status_path, true).unwrap(),
        ));
        manager.status().set_persist_step(4096);
        t.enable_versioning(manager.clone());
        let before = manager.next_id();
        committed = forget_history(&t, &manager, 0, 3_000).await;
        // An aborted inserter's id, for the outcome check below.
        aborted_id = before;
        assert_eq!(manager.outcome(aborted_id), TxOutcome::Aborted);
        // Push past a block so the history is settled.
        for _ in 0..9_000 {
            let tx = manager.begin();
            manager.commit(tx);
        }
        // As every writing commit does: outcomes reach the file.
        manager.flush().unwrap();
        manager.settle_now();
        assert_eq!(visible_ids(&t).await, committed, "before any sweep");

        let report = t.neutralize_aborted_now().await.unwrap();
        assert_eq!(report.tombstoned, 1_000, "{:?}", report);
        assert_eq!(report.cleared, (committed.iter().filter(|id| *id % 10 == 1).count()) as u64);
        assert!(report.advanced_to.is_some(), "{:?}", report);
        let forgotten = manager.status().forgotten_below();
        assert!(forgotten > aborted_id, "forgotten {} of {}", forgotten, manager.next_id());
        assert_eq!(
            manager.outcome(aborted_id),
            TxOutcome::Committed,
            "a forgotten id reads as committed"
        );
        assert_eq!(visible_ids(&t).await, committed, "after forgetting");
        // A second sweep finds nothing left to rewrite.
        for _ in 0..9_000 {
            let tx = manager.begin();
            manager.commit(tx);
        }
        manager.settle_now();
        let again = t.neutralize_aborted_now().await.unwrap();
        assert_eq!((again.tombstoned, again.cleared), (0, 0));
        manager.flush().unwrap();
        t.data().unwrap().io_pointers.commit_durable(t.data().unwrap().io_pointers.begin_txn()).await.unwrap();
    }

    // Reopened: the sidecar carries the forgotten horizon, and the rows
    // still read as they did.
    let manager = std::sync::Arc::new(TransactionManager::new(
        StatusLog::open_with(&status_path, true).unwrap(),
    ));
    assert!(manager.status().forgotten_below() > aborted_id, "forgotten horizon persisted");
    let t = open_table(dir.path(), "wp_forget").await;
    t.enable_versioning(manager.clone());
    assert_eq!(visible_ids(&t).await, committed, "after reopen");
    // And with forgetting off: the status file still names every abort.
    let plain = std::sync::Arc::new(TransactionManager::new(
        StatusLog::open_with(&status_path, false).unwrap(),
    ));
    assert_eq!(plain.outcome(aborted_id), TxOutcome::Aborted);
}

/// A table that has never been swept pins the database's forgotten horizon,
/// however far another has got.
#[tokio::test(flavor = "multi_thread")]
async fn forgetting_is_pinned_by_a_table_never_swept() {
    use crate::core::mvcc::TransactionManager;
    use crate::core::mvcc::status::StatusLog;

    let dir = tempfile::tempdir().unwrap();
    let manager = std::sync::Arc::new(TransactionManager::new(StatusLog::in_memory_with(true)));
    let a = open_table(dir.path(), "wp_forget_a").await;
    let b = open_table(dir.path(), "wp_forget_b").await;
    a.enable_versioning(manager.clone());
    b.enable_versioning(manager.clone());
    let committed_a = forget_history(&a, &manager, 0, 600).await;
    let committed_b = forget_history(&b, &manager, 0, 600).await;
    for _ in 0..9_000 {
        let tx = manager.begin();
        manager.commit(tx);
    }
    manager.settle_now();

    let swept = a.neutralize_aborted_now().await.unwrap();
    assert!(swept.advanced_to.is_some());
    assert_eq!(
        manager.status().forgotten_below(),
        0,
        "b has never been swept, so nothing may be forgotten"
    );
    assert_eq!(b.forget_horizon(), Some(0));
    assert_eq!(visible_ids(&b).await, committed_b);

    b.neutralize_aborted_now().await.unwrap();
    assert!(manager.status().forgotten_below() > 0, "both swept: it moves");
    assert_eq!(visible_ids(&a).await, committed_a);
    assert_eq!(visible_ids(&b).await, committed_b);

    // A table registered later starts at the forgotten horizon and pins any
    // further advance until it, too, is swept.
    let floor = manager.status().forgotten_below();
    let c = open_table(dir.path(), "wp_forget_c").await;
    c.enable_versioning(manager.clone());
    assert_eq!(c.forget_horizon(), Some(floor));
    for _ in 0..9_000 {
        let tx = manager.begin();
        manager.commit(tx);
    }
    manager.settle_now();
    a.neutralize_aborted_now().await.unwrap();
    b.neutralize_aborted_now().await.unwrap();
    assert_eq!(manager.status().forgotten_below(), floor, "c pins it");
    // And a dropped table no longer does — once its vacuum service, which
    // shares the registration, has noticed the drop and stopped.
    drop(c);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while manager.try_forget() <= floor {
        assert!(std::time::Instant::now() < deadline, "a dropped table still pins the horizon");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Heavy aborts, sweeps and forgets running against readers, checked against
/// a reference model: a reader never sees a row whose insert aborted, never
/// misses one committed before it started, and never loses a row to an
/// aborted delete — before and after forgets, and after a reopen.
///
/// `RDB_FORGET_STRESS_SECS` sets the duration (default 3 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn forgetting_under_concurrent_writes_and_reads_matches_a_reference_model() {
    use crate::core::mvcc::TransactionManager;
    use crate::core::mvcc::status::StatusLog;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
    use std::sync::Arc;

    let secs: u64 = std::env::var("RDB_FORGET_STRESS_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    const CAP: usize = 4_000_000;
    let dir = tempfile::tempdir().unwrap();
    let status_path = dir.path().join("tx.status");
    let manager = Arc::new(TransactionManager::new(StatusLog::open_with(&status_path, true).unwrap()));
    manager.set_settle_window(4096);
    manager.status().set_persist_step(4096);
    let t = Arc::new(open_table(dir.path(), "wp_forget_stress").await);
    t.enable_versioning(manager.clone());

    // Row id -> 0 not yet decided, 1 committed, 2 aborted.
    let model: Arc<Vec<AtomicU8>> = Arc::new((0..CAP).map(|_| AtomicU8::new(0)).collect());
    let next_row = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let forgets = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();

    for w in 0..3u64 {
        let (t, m, model, next_row, stop) =
            (t.clone(), manager.clone(), model.clone(), next_row.clone(), stop.clone());
        tasks.push(tokio::spawn(async move {
            let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ (w + 1);
            let mut xs = || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng
            };
            while !stop.load(Ordering::Relaxed) {
                let id = next_row.fetch_add(1, Ordering::Relaxed);
                if id as usize >= CAP {
                    break;
                }
                let tx = m.begin();
                t.insert_row_in_txn(forget_row(id), tx).await.unwrap();
                if xs() % 10 < 3 {
                    m.abort(tx);
                    model[id as usize].store(2, Ordering::Release);
                } else {
                    m.commit(tx);
                    model[id as usize].store(1, Ordering::Release);
                }
                // An aborted delete of some committed row.
                if xs() % 20 == 0 && id > 10 {
                    let victim = xs() % id;
                    if model[victim as usize].load(Ordering::Acquire) == 1 {
                        let tx = m.begin();
                        match t
                            .delete_rows_in_txn(
                                &format!("id = {}", victim),
                                full_fetch(&t.schema.fields),
                                None,
                                tx,
                            )
                            .await
                        {
                            Ok(_) | Err(_) => m.abort(tx),
                        }
                    }
                }
                // Read-only transactions, as a server's ids go.
                for _ in 0..(xs() % 8) {
                    let r = m.begin();
                    m.commit(r);
                }
            }
        }));
    }
    {
        let (t, m, stop, forgets) = (t.clone(), manager.clone(), stop.clone(), forgets.clone());
        tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                m.settle_now();
                if let Ok(report) = t.neutralize_aborted_now().await {
                    if report.advanced_to.is_some() {
                        forgets.fetch_add(1, Ordering::Relaxed);
                    }
                }
                m.flush().unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }));
    }
    for _ in 0..2 {
        let (t, model, next_row, stop, reads) =
            (t.clone(), model.clone(), next_row.clone(), stop.clone(), reads.clone());
        tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                // Committed before the read starts: must all be there.
                let upto = next_row.load(Ordering::Relaxed).min(CAP as u64);
                let before: Vec<u64> = (0..upto)
                    .filter(|id| model[*id as usize].load(Ordering::Acquire) == 1)
                    .collect();
                let seen = visible_ids(&t).await;
                let seen_set: std::collections::HashSet<u64> = seen.iter().copied().collect();
                for id in &before {
                    assert!(seen_set.contains(id), "committed row {} is missing", id);
                }
                for id in &seen {
                    // Decided as aborted is never visible. Undecided rows are
                    // checked again at the end.
                    assert_ne!(
                        model[*id as usize].load(Ordering::Acquire),
                        2,
                        "aborted row {} is visible",
                        id
                    );
                }
                reads.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
    stop.store(true, Ordering::Relaxed);
    for task in tasks {
        task.await.unwrap();
    }
    manager.settle_now();
    t.neutralize_aborted_now().await.unwrap();
    let rows = next_row.load(Ordering::Relaxed).min(CAP as u64);
    let expected: Vec<u64> = (0..rows)
        .filter(|id| model[*id as usize].load(Ordering::Acquire) == 1)
        .collect();
    assert_eq!(visible_ids(&t).await, expected, "final, live");
    eprintln!(
        "forget stress: {} rows, {} forgets, {} reads, forgotten below {} of {}",
        rows,
        forgets.load(Ordering::Relaxed),
        reads.load(Ordering::Relaxed),
        manager.status().forgotten_below(),
        manager.next_id()
    );
    assert!(forgets.load(Ordering::Relaxed) > 0, "no forget ever happened");
    assert!(manager.status().forgotten_below() > 0);

    manager.flush().unwrap();
    drop(t);
    drop(manager);
    let manager = Arc::new(TransactionManager::new(StatusLog::open_with(&status_path, true).unwrap()));
    let t = open_table(dir.path(), "wp_forget_stress").await;
    t.enable_versioning(manager.clone());
    assert_eq!(visible_ids(&t).await, expected, "final, reopened");
}

/// Concurrent inserts into a keyed table must never take an id the sequence
/// on disk does not cover.
///
/// Inserts into a table with a UNIQUE key used to take turns under one lock;
/// striped by key, they check the reservation and draw ids side by side, and
/// checking only `next < reserved` let two of them take `reserved - 1` and
/// `reserved`. After a crash the uncovered id was handed out again.
///
/// The invariant read here, after each insert returns: the highest id handed
/// out is covered by the reservation, which is raised only once the sequence
/// is written and only ever rises, so a reading below the counter is a gap.
/// The window is a few instructions wide, so this rarely lands in it;
/// `two_inserts_at_the_reservation_boundary_are_both_covered` pins the rule
/// deterministically.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_keyed_inserts_never_outrun_the_id_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let mut t = open_table(dir.path(), "wp_id_reservation").await;
    t.register_unique_key(crate::core::rql::unique_key::UniqueKey::new(
        "wp_id_reservation_id_key".into(),
        vec!["id".into()],
    ));
    let t = Arc::new(t);
    let mut tasks = Vec::new();
    for task in 0..16u64 {
        let t = t.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..300u64 {
                insert(&t, task * 1_000_000 + i, 0, "s").await;
                let handed_out = t.data().unwrap().ids.last_row_id.load(std::sync::atomic::Ordering::Acquire);
                // Raised only after the sequence is written, and only ever up.
                let covered = t.ids_reserved_upto().unwrap();
                assert!(
                    covered >= handed_out,
                    "ids up to {} were handed out but the disk covers only {}",
                    handed_out,
                    covered
                );
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

/// The same invariant without depending on a thread race: two inserts at the
/// boundary of the reservation, each between its check and its id, must leave
/// both of their ids covered. Checking `next < reserved` alone covered one.
#[tokio::test]
async fn two_inserts_at_the_reservation_boundary_are_both_covered() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "wp_id_boundary").await;
    insert(&t, 1, 0, "s").await;
    let reserved = t.ids_reserved_upto().unwrap();
    // The next id is the last one the reservation covers.
    t.data().unwrap().ids.last_row_id.store(reserved - 1, std::sync::atomic::Ordering::Release);
    let first = t.reserve_insert_id().await.unwrap();
    let second = t.reserve_insert_id().await.unwrap();
    let next = t.data().unwrap().ids.last_row_id.load(std::sync::atomic::Ordering::Acquire);
    // The two will take `next` and `next + 1`.
    assert!(
        t.ids_reserved_upto().unwrap() >= next + 2,
        "two inserts in flight from id {} but only {} is covered",
        next,
        t.ids_reserved_upto().unwrap()
    );
    drop((first, second));
}

/// An INSERT whose row bytes could not be flushed must fail, not commit a
/// pointer naming bytes that never reached the disk.
///
/// The row bytes and the pointer live in different files, and the statement's
/// success is decided by the *pointers* file's commit alone. A flush failure
/// on the rows file poisoned only the rows file, so the pointer committed and
/// the INSERT reported success. No log is installed for this directory, which
/// is the path where each append flushes its own file.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_row_bytes_flush_fails_the_insert() {
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "flush_fail").await;
    let location = dir.path().to_str().unwrap();
    assert!(
        crate::core::wal::wal_for(location).is_none(),
        "this test exercises the path without a log"
    );
    insert(&t, 1, 10, "before").await;

    t.data().unwrap().io_rows.fail_next_flush_for_tests();
    let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
    rw.columns_writing_data.push(payload(&2u64.to_le_bytes(), 0, DbType::U64));
    rw.columns_writing_data.push(payload(&20i32.to_le_bytes(), 1, DbType::I32));
    rw.columns_writing_data.push(payload(b"lost", 2, DbType::STRING));
    assert!(
        t.insert_row(rw).await.is_err(),
        "an insert whose row bytes failed to flush reported success"
    );

    // And every later write in the directory refuses too, for the reason the
    // log refuses after its own flush fails: a retry can succeed over pages
    // the kernel already dropped.
    let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
    rw.columns_writing_data.push(payload(&3u64.to_le_bytes(), 0, DbType::U64));
    rw.columns_writing_data.push(payload(&30i32.to_le_bytes(), 1, DbType::I32));
    rw.columns_writing_data.push(payload(b"after", 2, DbType::STRING));
    assert!(t.insert_row(rw).await.is_err());
    assert!(
        crate::core::storage_providers::file_sync::directory_flush_failure(location).is_some()
    );
    // Process-wide state: forgotten so nothing else that reuses the path
    // inherits it.
    crate::core::storage_providers::file_sync::forget_directory_failure(location);
}
