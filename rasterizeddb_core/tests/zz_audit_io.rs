//! TEMPORARY audit reproducers. Deleted before the audit finishes.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::mvcc::TransactionManager;
use rasterizeddb_core::core::row::row::{ColumnFetchingData, ColumnWritePayload, Row, RowFetch, RowWrite};
use rasterizeddb_core::core::row::schema::{SchemaCalculator, SchemaField};
use rasterizeddb_core::core::row::table::Table;
use rasterizeddb_core::core::rql::lexer_ct::CreateColumnData;
use rasterizeddb_core::core::storage_providers::file_sync::LocalStorageProvider;
use rasterizeddb_core::core::storage_providers::traits::StorageIO;
use rasterizeddb_core::core::wal::{Wal, file_id_for_name};
use rasterizeddb_core::memory_pool::MEMORY_POOL;
use rclite::Arc;
use smallvec::SmallVec;
use std::sync::Arc as StdArc;

fn init() {
    let _ = rasterizeddb_core::MAX_PERMITS_THREADS.set(4);
    let _ = rasterizeddb_core::BATCH_SIZE.set(512);
    let _ = rasterizeddb_core::cache::query_cache::ENABLE_CACHE.set(false);
}

fn on_a_big_stack<F: std::future::Future<Output = ()> + Send + 'static>(body: F) {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap()
                .block_on(body);
        })
        .unwrap()
        .join()
        .unwrap();
}

fn payload(bytes: &[u8], write_order: u32, column_type: DbType) -> ColumnWritePayload {
    let mut mb = MEMORY_POOL.acquire(bytes.len());
    mb.into_slice_mut().copy_from_slice(bytes);
    let size = column_type.get_size();
    ColumnWritePayload { data: mb, write_order, column_type, size }
}

fn full_fetch(fields: &SmallVec<[SchemaField; 20]>) -> RowFetch {
    let calc = SchemaCalculator;
    let mut cols = SmallVec::new();
    let mut sorted: Vec<&SchemaField> = fields.iter().collect();
    sorted.sort_by_key(|f| f.write_order);
    for f in sorted {
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

fn row_write(id: u64, cat: i32, label: &str) -> RowWrite {
    let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
    rw.columns_writing_data.push(payload(&id.to_le_bytes(), 0, DbType::U64));
    rw.columns_writing_data.push(payload(&cat.to_le_bytes(), 1, DbType::I32));
    rw.columns_writing_data.push(payload(label.as_bytes(), 2, DbType::STRING));
    rw
}

fn upd(fields: &SmallVec<[SchemaField; 20]>, name: &str, bytes: &[u8]) -> SmallVec<[ColumnWritePayload; 32]> {
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

fn str_of(r: &Row, sid: u64) -> String {
    let c = r.columns.iter().find(|c| u64::from(c.schema_id) == sid).unwrap();
    String::from_utf8_lossy(c.data.into_slice()).to_string()
}

// ---------------------------------------------------------------------------
// A. The reclaimable gate in the filtered worker branch drops a version that
//    the scan's own snapshot can still see.
// ---------------------------------------------------------------------------

#[test]
fn audit_a_reclaimable_gate_hides_a_visible_old_version() {
    let _uncapped = uncapped();
    on_a_big_stack(async {
        let dir = tempfile::tempdir().unwrap();
        let t = open_table(dir.path(), "audit_gate").await;
        let manager = StdArc::new(TransactionManager::in_memory());
        t.enable_versioning(manager.clone());

        let a = manager.begin_autocommit();
        t.insert_row_in_txn(row_write(1, 7, "start"), a).await.unwrap();
        manager.commit(a);
        t.wait_filters_ready().await;

        // T relocates the row (versioned update always relocates).
        let tx_t = manager.begin_autocommit();
        let n = t
            .update_rows_in_txn(
                "id = 1",
                full_fetch(&t.schema.fields),
                upd(&t.schema.fields, "label", b"a-much-longer-label-that-relocates"),
                None,
                tx_t,
            )
            .await
            .unwrap();
        assert_eq!(n, 1);

        // R starts while T is still running and takes its snapshot now.
        let tx_r = manager.begin_autocommit();
        let vis_old = t.visibility_for(tx_r);

        // T commits before R's scan actually spawns its workers.
        manager.commit(tx_t);
        assert!(tx_t < manager.oldest_active(), "T retired and R is the oldest running");

        let fields = t.schema.fields.to_vec();

        // Unfiltered branch: R must see exactly the OLD version.
        let rows_unfiltered = t
            .query_row_with_visibility("", &fields, full_fetch(&t.schema.fields), full_fetch(&t.schema.fields), None, None, vis_old.clone())
            .await
            .unwrap();
        assert_eq!(rows_unfiltered.len(), 1, "unfiltered scan under R's snapshot");
        assert_eq!(str_of(&rows_unfiltered[0], 2), "start");

        // Filtered branch: same snapshot, same expected answer.
        let rows_filtered = t
            .query_row_with_visibility("id = 1", &fields, full_fetch(&t.schema.fields), full_fetch(&t.schema.fields), None, None, vis_old.clone())
            .await
            .unwrap();
        assert_eq!(
            rows_filtered.len(),
            1,
            "filtered scan under R's snapshot lost the row (unfiltered saw {})",
            rows_unfiltered.len()
        );
        assert_eq!(str_of(&rows_filtered[0], 2), "start");

        manager.commit(tx_r);

        // Control: a fresh snapshot sees the new version.
        let tx_c = manager.begin_autocommit();
        let rows_new = t
            .query_row_with_visibility("id = 1", &fields, full_fetch(&t.schema.fields), full_fetch(&t.schema.fields), None, None, t.visibility_for(tx_c))
            .await
            .unwrap();
        assert_eq!(rows_new.len(), 1);
        assert_eq!(str_of(&rows_new[0], 2), "a-much-longer-label-that-relocates");
        manager.commit(tx_c);
    });
}

// ---------------------------------------------------------------------------
// Disk-full simulation via RLIMIT_FSIZE.
// ---------------------------------------------------------------------------

/// RLIMIT_FSIZE is per process, so a cap set by one test applies to every
/// file any test is writing at that moment. Run in parallel, B and C's caps
/// made D's 32 KB staged copy fail with EFBIG — which was where the staging
/// path's `unwrap()` panic came from. A cap holds this exclusively; every test
/// that writes without one holds it shared, and a test that sets one holds it
/// exclusively for its whole body — its own uncapped writes included.
static FSIZE_GATE: std::sync::RwLock<()> = std::sync::RwLock::new(());

fn uncapped() -> std::sync::RwLockReadGuard<'static, ()> {
    FSIZE_GATE.read().unwrap_or_else(|e| e.into_inner())
}

fn capping() -> std::sync::RwLockWriteGuard<'static, ()> {
    FSIZE_GATE.write().unwrap_or_else(|e| e.into_inner())
}

struct FsizeCap {
    prev: libc::rlimit,
}

impl FsizeCap {
    fn set(limit: u64) -> Self {
        unsafe {
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
            let mut prev = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
            assert_eq!(libc::getrlimit(libc::RLIMIT_FSIZE, &mut prev), 0);
            let new = libc::rlimit { rlim_cur: limit as libc::rlim_t, rlim_max: prev.rlim_max };
            assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &new), 0);
            FsizeCap { prev }
        }
    }
}

impl Drop for FsizeCap {
    fn drop(&mut self) {
        unsafe {
            libc::setrlimit(libc::RLIMIT_FSIZE, &self.prev);
        }
    }
}

const HEADER_LEN: u64 = 4 + 1 + 1 + 2 + 8 + 8 + 8 + 4 + 4;

// B. Was: a short write to the log left `write_pos` behind the file cursor, so
//    the next in-place commit read its payload from the wrong offset and
//    applied foreign bytes to the data file. Now a failed write is cut back to
//    `write_pos`: no partial record survives and the next apply is correct.
#[test]
fn audit_b_a_short_wal_write_leaves_no_partial_record() {
    let _capping = capping();
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("rasterized.wal");
    let data_path = dir.path().join("data.db");
    std::fs::write(&data_path, vec![0u8; 64]).unwrap();

    let wal = Wal::open(&wal_path).unwrap();
    let id = file_id_for_name("data.db");
    wal.register_file(id, &data_path).unwrap();

    let t1 = wal.begin();
    wal.log(t1, id, 0, &[1u8; 100]).unwrap();
    wal.commit_txn(t1).unwrap();
    let len_before = std::fs::metadata(&wal_path).unwrap().len();

    // Cap: the next record's header fits and 100 bytes of its payload, no more.
    let t2 = wal.begin();
    let short = {
        let _cap = FsizeCap::set(len_before + HEADER_LEN + 100);
        wal.log(t2, id, 0, &[2u8; 4096])
    };
    assert!(short.is_err(), "the capped write must fail: {:?}", short);
    let len_after = std::fs::metadata(&wal_path).unwrap().len();
    eprintln!("wal len before {} after failed write {}", len_before, len_after);
    assert_eq!(len_after, len_before, "a partial record was left in the log");

    // Now a perfectly ordinary in-place write in a new transaction.
    let t3 = wal.begin();
    wal.log_in_place(t3, id, 0, b"GOOD").unwrap();
    wal.commit_txn(t3).unwrap();

    let data = std::fs::read(&data_path).unwrap();
    assert_eq!(&data[..4], b"GOOD", "the in-place apply wrote foreign bytes: {:?}", &data[..4]);
}

// C. Was: a short append to a data file panicked part-way (swallowed by the
//    task boundary) and left `file_len` behind the on-disk length, so the next
//    append reported an offset that did not describe its bytes. Now the failed
//    append rolls the file back to `file_len` without panicking, and the next
//    append lands where it says.
#[test]
fn audit_c_a_short_append_rolls_back_and_keeps_offsets_true() {
    let _capping = capping();
    on_a_big_stack(async {
        let dir = tempfile::tempdir().unwrap();
        let p = StdArc::new(LocalStorageProvider::new(dir.path().to_str().unwrap(), Some("t.db")).await);
        p.append_data(&[1u8; 4096], true).await;
        assert_eq!(p.get_len().await, 4096);

        let failed = {
            let _cap = FsizeCap::set(4096 + 1000);
            let p2 = p.clone();
            tokio::spawn(async move {
                p2.append_data(&[2u8; 4096], true).await;
            })
            .await
        };
        assert!(failed.is_ok(), "the capped append panicked instead of failing cleanly");
        let on_disk = std::fs::metadata(dir.path().join("t.db")).unwrap().len();
        eprintln!("on disk {} vs provider len {}", on_disk, p.get_len().await);
        assert_eq!(on_disk, 4096, "the failed append left partial bytes on disk");
        assert_eq!(p.get_len().await, 4096, "the failed append moved the offset allocator");

        let off = p.append_data_returning_offset(&[3u8; 4096], true).await;
        eprintln!("third append reported offset {}", off);
        let mut buf = vec![0u8; 4096];
        let mut pos = off;
        p.read_data_into_buffer(&mut pos, &mut buf).await.unwrap();
        assert_eq!(buf, vec![3u8; 4096], "bytes read back at the reported offset are not the bytes appended");
    });
}

// D. Staged appends (`immediate = false`) followed by `commit_durable` — the
//    sequence `copy_table_files` / `copy_between` / `force_resync` call
//    "durable before the catalogue names it" — leave every byte in the `.tmp`
//    sidecar, which nothing has fsynced.
#[test]
fn audit_d_staged_copy_is_not_durable_at_commit_durable() {
    let _uncapped = uncapped();
    on_a_big_stack(async {
        let dir = tempfile::tempdir().unwrap();
        let p = LocalStorageProvider::new(dir.path().to_str().unwrap(), Some("copy.db")).await;
        for _ in 0..8 {
            p.append_data(&[9u8; 4096], false).await;
        }
        let token = p.begin_txn();
        p.commit_durable(token).await.unwrap();
        let logical = p.get_len().await;
        let main_on_disk = std::fs::metadata(dir.path().join("copy.db")).unwrap().len();
        let tmp_on_disk = std::fs::metadata(dir.path().join("copy.db.tmp")).unwrap().len();
        eprintln!("logical {} main {} tmp {}", logical, main_on_disk, tmp_on_disk);
        assert_eq!(logical, 8 * 4096);
        assert_eq!(
            main_on_disk, logical,
            "commit_durable returned Ok with {} bytes still only in the unsynced .tmp sidecar",
            tmp_on_disk
        );
    });
}

// E. Records logged while a checkpoint is between its file-sync pass and its
//    truncation must not be discarded from the log AND reported durable.
//
//    "Reported durable" is only wrong if the record's file was NOT synced by the
//    checkpoint that cut it from the log. Records committed before the
//    checkpoint fixes what it covers are legitimately cut: that pass syncs
//    their file. Counting by time alone (a flag, or a fence just before the
//    call) cannot see that fix point and flagged correct behaviour, so this
//    asserts the real invariant: every record cut and reported durable had its
//    data file synced by the checkpoint. The window itself is pinned
//    deterministically by `wal::tests::a_checkpoint_keeps_records_logged_during_its_sync`.
#[test]
fn audit_e_checkpoint_claims_durability_for_records_it_discarded() {
    let _uncapped = uncapped();
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("rasterized.wal");
    let data_path = dir.path().join("data.db");
    std::fs::write(&data_path, vec![0u8; 64]).unwrap();
    let wal = StdArc::new(Wal::open(&wal_path).unwrap());
    let id = file_id_for_name("data.db");
    wal.register_file(id, &data_path).unwrap();
    // Pad the registered set so the sync pass takes measurable time.
    for i in 0..400 {
        let p = dir.path().join(format!("pad{}.db", i));
        std::fs::write(&p, b"x").unwrap();
        wal.register_file(file_id_for_name(&format!("pad{}.db", i)), &p).unwrap();
    }
    let t1 = wal.begin();
    wal.log(t1, id, 0, &[1u8; 100]).unwrap();
    wal.commit_txn(t1).unwrap();

    let stop = StdArc::new(AtomicBool::new(false));
    let started = StdArc::new(AtomicBool::new(false));
    let fence_seq = StdArc::new(std::sync::atomic::AtomicU64::new(u64::MAX));
    let logger = {
        let wal = wal.clone();
        let stop = stop.clone();
        let started = started.clone();
        let fence_seq = fence_seq.clone();
        std::thread::spawn(move || {
            let mut seqs: Vec<(u64, u64)> = Vec::new(); // (record seq, marker seq) logged after start
            let payload = vec![7u8; 16 * 1024];
            while !stop.load(Ordering::Relaxed) {
                let t = wal.begin();
                let s = wal.log(t, id, 0, &payload).unwrap();
                let m = wal.commit_txn_deferred_seq(t).unwrap();
                if started.load(Ordering::Relaxed) && m > fence_seq.load(Ordering::SeqCst) {
                    seqs.push((s, m));
                }
            }
            seqs
        })
    };
    std::thread::sleep(std::time::Duration::from_millis(5));
    // The fence: everything the logger commits after this marker was issued
    // after the checkpoint was called.
    let tf = wal.begin();
    wal.log(tf, id, 0, &[3u8; 8]).unwrap();
    let fence = wal.commit_txn_deferred_seq(tf).unwrap();
    fence_seq.store(fence, Ordering::SeqCst);
    started.store(true, Ordering::SeqCst);
    let syncs = rasterizeddb_core::core::sync_audit::watch(dir.path());
    let ran = wal.checkpoint().unwrap();
    let synced = syncs.take();
    let durable_after = wal.durable_through();
    stop.store(true, Ordering::SeqCst);
    let seqs = logger.join().unwrap();
    assert!(ran, "checkpoint declined");

    let discarded_but_claimed: Vec<&(u64, u64)> = seqs.iter().filter(|(s, m)| *m <= durable_after && *s > 0).collect();
    let log_len = std::fs::metadata(&wal_path).unwrap().len();
    eprintln!(
        "records logged during checkpoint: {}; claimed durable by the truncation: {}; log len now {}",
        seqs.len(),
        discarded_but_claimed.len(),
        log_len
    );
    // Any record whose marker seq <= durable_after was issued before the
    // truncation (set_len(0)), so it is gone from the log; commit_through on it
    // returns Ok without a sync.
    for (s, m) in discarded_but_claimed.iter().take(3) {
        assert!(wal.commit_through(*m).is_ok());
        eprintln!("record seq {} marker {} -> commit_through Ok, not in the log", s, m);
    }
    let data_synced = synced
        .iter()
        .any(|(tag, p)| *tag == "wal.checkpoint.data_file" && p.ends_with("data.db"));
    assert!(
        discarded_but_claimed.is_empty() || data_synced,
        "{} records were cut from the log and reported durable, but the checkpoint never synced data.db (syncs: {:?})",
        discarded_but_claimed.len(),
        synced
    );
}
