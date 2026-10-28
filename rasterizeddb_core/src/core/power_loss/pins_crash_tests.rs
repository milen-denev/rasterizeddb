//! Phase 3 P5a through power cuts (docs/partitioning.md §7.2, §3.2, §10,
//! §16 I13): crash images, not a process kill, since a kill keeps every
//! unsynced write and so passes every ordering bug.
//!
//! - **The first-write edge.** A partition swept clean takes a write from a
//!   transaction that aborts, its data pages forced to disk as writeback
//!   could; the image is cut before anything else syncs. Every image, once
//!   the database forgets as far as every other table lets it, still pins
//!   the partition and hides the aborted row: its `DIRTY` was durable before
//!   the stamp.
//! - **A sweep before its horizon.** A forget sweep tombstones an aborted
//!   row and forgetting moves past it, persisting the status sidecar; cut
//!   at once. The tombstone is in every image that keeps the horizon.
//! - **Compaction.** Cut after the new generation's snapshot is written,
//!   after its `SEALED`, and after the switch: every image opens, with
//!   every row and every pin as before.
//! - **`SERIAL`.** Cut after acknowledged inserts: a new insert in every
//!   image takes a value no stored row has.

use super::*;
use parking_lot::Mutex;
use std::sync::Arc as StdArc;

use crate::core::database::Relation;
use crate::core::partition::route::Route;
use crate::core::partition::table::PartitionedTable;

const CREATE: &str =
    "CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v TEXT) PARTITION BY HASH (tenant_id) PARTITIONS 4";
const SEEDS: u64 = 10;

fn pt(db: &Database) -> rclite::Arc<PartitionedTable> {
    match db.relation("pt") {
        Some(Relation::Partitioned(t)) => t,
        _ => panic!("pt is not partitioned"),
    }
}

fn pid_of_tenant(t: &PartitionedTable, tenant: i64) -> u32 {
    let epoch = t.epoch().unwrap();
    let pids = t.route_where(&format!("tenant_id = {tenant}")).unwrap().pids(&epoch.map);
    assert_eq!(pids.len(), 1);
    pids[0]
}

fn materialized(t: &PartitionedTable) -> Vec<u32> {
    let epoch = t.epoch().unwrap();
    t.materialized(&epoch, &Route::All).unwrap().into_iter().map(|(pid, _)| pid).collect()
}

async fn pt_ids(db: &Arc<Database>, clause: &str) -> Result<Vec<i64>, String> {
    let mut v: Vec<i64> = decode(&exec(db, &format!("SELECT id FROM pt WHERE {clause}")).await?)?
        .iter()
        .map(|r| i64::from_le_bytes(r.columns[0].data.into_slice().try_into().unwrap_or([0xFF; 8])))
        .collect();
    v.sort();
    Ok(v)
}

fn advance(db: &Database) {
    for _ in 0..9_000 {
        let tx = db.transactions.begin();
        db.transactions.commit(tx);
    }
    db.transactions.settle_now();
}

/// Sweeps every plain table and every materialized partition of `pt` but
/// `skip`, and forgets as far as that allows.
async fn forget_except(db: &Database, skip: &[u32]) -> TxIdOf {
    advance(db);
    for name in db.table_names() {
        if let Some(t) = db.table_for_tests(&name)
            && !t.is_shell()
        {
            t.neutralize_aborted_now().await.unwrap();
        }
    }
    let t = pt(db);
    let board = t.pin_board().unwrap();
    for pid in materialized(&t) {
        if !skip.contains(&pid) {
            board.sweep(pid).await.unwrap();
        }
    }
    db.transactions.try_forget()
}

type TxIdOf = crate::core::mvcc::TxId;

/// Makes the partition's data files durable as they stand, as kernel
/// writeback may at any moment, telling the recorder so.
fn force_out(dir: &Path, uid: u64, pid: u32) {
    for name in crate::core::partition::files::data_files(uid, pid) {
        let path = dir.join(name);
        crate::core::sync_audit::note("file.data", &path);
        std::fs::File::open(&path).unwrap().sync_all().unwrap();
    }
}

/// The first write to a partition swept clean, by a transaction that
/// aborts, with its stamped pages on disk: no image lets forgetting pass the
/// aborted id while the partition is unswept, and the row stays invisible.
///
/// Mutation: `PinBoard::first_write` appending `DIRTY` with `append` (group
/// commit) instead of `append_durable`: images that keep the row's pages
/// lose the record, the partition opens clean, and the row comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_first_write_after_a_clean_sweep_is_pinned_in_every_image() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir, "pins_cut").await.unwrap();
    exec(&db, CREATE).await.unwrap();
    let values: Vec<String> = (1..=8i64).map(|t| format!("({t}, {t}, 'c')")).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await.unwrap();
    let t = pt(&db);
    let uid = t.uid();
    let p = pid_of_tenant(&t, 1);
    forget_except(&db, &[]).await;
    assert_eq!(t.pin_of(p).await.unwrap(), None, "the partition did not sweep clean");
    drop(t);
    db.sync_durable().unwrap();

    let rec = Recorder::start(&dir);
    let y = db.transactions.begin();
    exec_in(&db, y, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 777, 'aborted')").await.unwrap();
    db.abort_transaction(y);
    force_out(&dir, uid, p);
    let cut = rec.cut_quiescent("after an aborted first write").unwrap();
    close(&dir, db);
    drop(rec);

    let img = base.path().join("image");
    let mut problems = Vec::new();
    for seed in 0..SEEDS {
        cut.write_image(seed, &img);
        let db = open(&img, "pins_cut").await.unwrap();
        let forgotten = forget_except(&db, &[p]).await;
        match pt_ids(&db, "tenant_id = 1").await {
            Ok(got) if got == vec![1] => {}
            got => problems.push(format!("seed {seed}: tenant 1 reads {got:?} (forgotten below {forgotten}, the aborted id {y})")),
        }
        close(&img, db);
        let _ = std::fs::remove_dir_all(&img);
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// A forget sweep's tombstones are durable before any horizon moves past
/// them: the status sidecar, persisted as forgetting moves, can outlive an
/// unsynced rewrite in a power cut, and every id below it then reads as
/// committed.
///
/// Mutation: no `sync_pending` after the rewrites in
/// `neutralize_aborted_from`: images that keep the new sidecar and lose the
/// rewrite show the aborted row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sweep_is_durable_before_forgetting_passes_it() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir, "pins_cut").await.unwrap();
    db.transactions.status().set_persist_step(1);
    exec(&db, CREATE).await.unwrap();
    let values: Vec<String> = (1..=8i64).map(|t| format!("({t}, {t}, 'c')")).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await.unwrap();
    let x = db.transactions.begin();
    exec_in(&db, x, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 999, 'aborted')").await.unwrap();
    db.abort_transaction(x);
    let p = pid_of_tenant(&pt(&db), 1);
    // Everything but the sweep of `p` durable before the cut.
    let forgotten = forget_except(&db, &[p]).await;
    assert!(forgotten <= x);
    db.sync_durable().unwrap();
    // The sidecar is a cache the next persist rewrites whole; gone, the one
    // the sweep below writes is new to the recorder, so every image keeps it
    // as written.
    let status = dir.join(STATUS_FILE);
    let mut sidecar = status.file_name().unwrap().to_os_string();
    sidecar.push(".settled");
    let _ = std::fs::remove_file(status.with_file_name(sidecar));

    let rec = Recorder::start(&dir);
    let report = pt(&db).pin_board().unwrap().sweep(p).await.unwrap();
    assert!(report.tombstoned > 0, "the sweep tombstoned nothing: {report:?}");
    let forgotten = db.transactions.try_forget();
    assert!(forgotten > x, "forgetting did not pass the aborted id ({forgotten} <= {x})");
    let cut = rec.cut_quiescent("after a sweep and a forget").unwrap();
    close(&dir, db);
    drop(rec);

    let img = base.path().join("image");
    let mut problems = Vec::new();
    for seed in 0..SEEDS {
        cut.write_image(seed, &img);
        let db = open(&img, "pins_cut").await.unwrap();
        let forgotten = db.transactions.status().forgotten_below();
        match pt_ids(&db, "tenant_id = 1").await {
            Ok(got) if got == vec![1] => {}
            got => problems.push(format!("seed {seed}: tenant 1 reads {got:?} (forgotten below {forgotten}, the aborted id {x})")),
        }
        close(&img, db);
        let _ = std::fs::remove_dir_all(&img);
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// Each partition's pin, by pid.
async fn pins(t: &PartitionedTable) -> Vec<(u32, Option<TxIdOf>)> {
    let mut out = Vec::new();
    for pid in materialized(t) {
        out.push((pid, t.pin_of(pid).await.unwrap()));
    }
    out
}

/// A map-log compaction cut after its snapshot is written, after its
/// `SEALED`, and after the switch: every image opens with every row and
/// every pin as before, and the unswept partition still hides its aborted
/// row once everything else is forgotten.
///
/// Mutation: `live_generation` choosing the highest generation whether or
/// not it is sealed: images cut after the snapshot was written open a
/// generation missing the records appended since, or none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compaction_cut_anywhere_keeps_the_map() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir, "pins_cut").await.unwrap();
    exec(&db, CREATE).await.unwrap();
    let values: Vec<String> = (1..=8i64).map(|t| format!("({t}, {t}, 'c')")).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await.unwrap();
    let x = db.transactions.begin();
    exec_in(&db, x, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 999, 'aborted')").await.unwrap();
    db.abort_transaction(x);
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    forget_except(&db, &[p]).await;
    let want_pins = pins(&t).await;
    let want_rows = pt_ids(&db, "id > 0").await.unwrap();
    db.sync_durable().unwrap();

    let rec = Recorder::start(&dir);
    let sink: StdArc<Mutex<Vec<Result<CrashPoint, String>>>> = StdArc::new(Mutex::new(Vec::new()));
    let mut guards = Vec::new();
    for point in ["test.pmap.compact_written", "test.pmap.compact_sealed", "test.pmap.compact_switched"] {
        let (rec, sink) = (rec.clone(), sink.clone());
        let action: failpoint::Action = StdArc::new(move |_hit| {
            sink.lock().push(rec.cut_quiescent(point));
            failpoint::Outcome::Continue
        });
        guards.push(failpoint::arm(point, &dir, 1, action));
    }
    assert!(t.pin_board().unwrap().compact().await.unwrap());
    for g in &guards {
        assert_eq!(g.fired(), 1, "a compaction cut point was never reached");
    }
    drop(guards);
    drop(t);
    close(&dir, db);
    drop(rec);
    let points: Vec<CrashPoint> = sink.lock().drain(..).filter_map(|p| p.map_err(|why| eprintln!("{why}")).ok()).collect();
    assert!(points.len() >= 2, "too many cuts were disturbed");

    let img = base.path().join("image");
    let mut problems = Vec::new();
    for point in &points {
        for seed in 0..SEEDS {
            point.write_image(seed, &img);
            let label = format!("{} seed {seed}", point.label);
            match open(&img, "pins_cut").await {
                Err(e) => problems.push(format!("{label}: does not open: {e}")),
                Ok(db) => {
                    let t = pt(&db);
                    if let Some(why) = t.broken() {
                        problems.push(format!("{label}: broken: {why}"));
                    } else {
                        let got = pins(&t).await;
                        if got != want_pins {
                            problems.push(format!("{label}: pins {got:?}, want {want_pins:?}"));
                        }
                        drop(t);
                        match pt_ids(&db, "id > 0").await {
                            Ok(rows) if rows == want_rows => {}
                            rows => problems.push(format!("{label}: rows {rows:?}")),
                        }
                        let forgotten = forget_except(&db, &[p]).await;
                        if forgotten > x || pt_ids(&db, "tenant_id = 1").await != Ok(vec![1]) {
                            problems.push(format!("{label}: forgotten below {forgotten}, past the aborted {x}"));
                        }
                    }
                    close(&img, db);
                }
            }
            let _ = std::fs::remove_dir_all(&img);
        }
    }
    assert!(problems.is_empty(), "{} problems:\n{}", problems.len(), problems.join("\n"));
}

/// Acknowledged `SERIAL` inserts, cut: in every image a new insert takes a
/// value no stored row holds, and resumes from the reservation without
/// scanning a partition.
///
/// Mutation: `SerialSource::cover` never raising the reservation: images
/// resume below stored values and repeat one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serial_values_never_repeat_after_a_power_cut() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir, "pins_cut").await.unwrap();
    exec(&db, "CREATE TABLE ser (tenant_id BIGINT NOT NULL, a BIGSERIAL, body TEXT) PARTITION BY HASH (tenant_id) PARTITIONS 4")
        .await
        .unwrap();
    db.sync_durable().unwrap();
    let rec = Recorder::start(&dir);
    for tenant in 1..=6i64 {
        exec(&db, &format!("INSERT INTO ser (tenant_id, body) VALUES ({tenant}, 'x')")).await.unwrap();
    }
    let cut = rec.cut_quiescent("after acknowledged SERIAL inserts").unwrap();
    close(&dir, db);
    drop(rec);
    let values = |out: Vec<u8>| -> Vec<i64> {
        let mut v: Vec<i64> = decode(&out)
            .unwrap()
            .iter()
            .map(|r| i64::from_le_bytes(r.columns[0].data.into_slice().try_into().unwrap()))
            .collect();
        v.sort();
        v
    };
    let img = base.path().join("image");
    let mut problems = Vec::new();
    for seed in 0..SEEDS {
        cut.write_image(seed, &img);
        let db = open(&img, "pins_cut").await.unwrap();
        let before = values(exec(&db, "SELECT a FROM ser").await.unwrap());
        if before != (1..=6).collect::<Vec<i64>>() {
            problems.push(format!("seed {seed}: acknowledged rows {before:?}"));
        }
        exec(&db, "INSERT INTO ser (tenant_id, body) VALUES (9, 'y')").await.unwrap();
        let after = values(exec(&db, "SELECT a FROM ser").await.unwrap());
        let mut dedup = after.clone();
        dedup.dedup();
        if dedup != after {
            problems.push(format!("seed {seed}: a value repeated: {after:?}"));
        }
        let scans = match db.relation("ser") {
            Some(Relation::Partitioned(t)) => t.serial_max_scans(),
            _ => 99,
        };
        if scans != 0 {
            problems.push(format!("seed {seed}: the first insert scanned the partitions"));
        }
        close(&img, db);
        let _ = std::fs::remove_dir_all(&img);
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}
