//! Phase 3 P5b through power cuts (docs/partitioning.md §7.2, §16 I13,
//! I19): crash images, not a process kill, since a kill keeps every
//! unsynced write and so passes every ordering bug.
//!
//! - **A close, then a checkpoint.** Acknowledged rows whose data pages are
//!   not yet synced, one aborted row, then every partition closed and the
//!   log checkpointed: cut inside the closes and after the checkpoint. Every
//!   image opens with every partition, holds every acknowledged row and not
//!   the aborted one (I16: a close never takes a file out of the log's map).
//!   Partition writes go through the log, so a close has nothing staged to
//!   land in this test; the landing is the background one's.
//! - **A reopen.** Every partition closed and the state synced, then writes
//!   that reopen them, acknowledged: cut at each reopen and after the
//!   acknowledgements. Every image holds the rows written before and every
//!   acknowledged row after.
//! - **I13 across a close.** A partition swept clean takes a write from a
//!   transaction that aborts (its `DIRTY` durable first), and closes, its
//!   staged stamps landed and synced; cut. In every image, once every other
//!   table forgets as far as it can, the partition still pins and hides the
//!   aborted row.
//!
//! Run under each index residency (`RASTERIZEDDB_INDEX_RESIDENCY`), as the
//! P5a crash tests are: the paged files change the crash model's walk.

use super::*;
use parking_lot::Mutex;
use std::sync::Arc as StdArc;

use crate::core::database::Relation;
use crate::core::partition::table::PartitionedTable;

const CREATE: &str = "CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v TEXT, PRIMARY KEY (tenant_id, id)) \
                      PARTITION BY HASH (tenant_id) PARTITIONS 4";

fn seeds() -> u64 {
    env_count("RDB_POWER_LOSS_SEEDS", 5, 1024).max(5)
}

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

/// `(tenant_id, id)` of every row, sorted.
async fn keys(db: &Arc<Database>) -> Result<Vec<(i64, i64)>, String> {
    let mut v: Vec<(i64, i64)> = decode(&exec(db, "SELECT tenant_id, id FROM pt").await?)?
        .iter()
        .map(|r| {
            let n = |c: usize| i64::from_le_bytes(r.columns[c].data.into_slice().try_into().unwrap_or([0xFF; 8]));
            (n(0), n(1))
        })
        .collect();
    v.sort();
    Ok(v)
}

/// Every image of `points` opens, holds `want` exactly, every partition
/// opens, and a write lands; each problem described.
async fn check_images(points: &[CrashPoint], img: &Path, want: &[(i64, i64)], problems: &mut Vec<String>) {
    for point in points {
        for seed in 0..seeds() {
            point.write_image(seed, img);
            let label = format!("{} seed {seed}", point.label);
            match open(img, "lazy_cut").await {
                Err(e) => problems.push(format!("{label}: does not open: {e}")),
                Ok(db) => {
                    let t = pt(&db);
                    if let Some(why) = t.broken() {
                        problems.push(format!("{label}: the table is refused: {why}"));
                    } else {
                        match keys(&db).await {
                            Ok(got) if got == want => {}
                            Ok(got) => {
                                let missing: Vec<_> = want.iter().filter(|k| !got.contains(k)).collect();
                                let extra: Vec<_> = got.iter().filter(|k| !want.contains(k)).collect();
                                problems.push(format!("{label}: missing {missing:?}, extra {extra:?}"));
                            }
                            Err(e) => problems.push(format!("{label}: {e}")),
                        }
                        if let Err(e) = t.all_partitions().await {
                            problems.push(format!("{label}: a partition does not open: {e}"));
                        }
                        if let Err(e) = exec(&db, "INSERT INTO pt (tenant_id, id, v) VALUES (2, 900000, 'after')").await {
                            problems.push(format!("{label}: an insert failed: {e}"));
                        }
                    }
                    drop(t);
                    close(img, db);
                }
            }
            let _ = std::fs::remove_dir_all(img);
        }
    }
}

/// Arms a cut at each of `points` under `dir`, into `sink`.
fn arm_cuts(
    rec: &StdArc<Recorder>,
    dir: &Path,
    points: &[(&str, u64)],
    sink: &StdArc<Mutex<Vec<Result<CrashPoint, String>>>>,
) -> Vec<failpoint::Guard> {
    points
        .iter()
        .map(|(name, nth)| failpoint::arm(name, dir, *nth, rec.cut_action(format!("{name} #{nth}"), sink.clone())))
        .collect()
}

fn taken(sink: &StdArc<Mutex<Vec<Result<CrashPoint, String>>>>) -> Vec<CrashPoint> {
    sink.lock().drain(..).filter_map(|p| p.map_err(|why| eprintln!("{why}")).ok()).collect()
}

/// Acknowledged rows whose data pages are written and not yet synced (their
/// log records are), one aborted row, then every partition closed and the
/// log checkpointed: cut once each close has taken its instance, once each
/// has finished, and after the checkpoint. Every image holds exactly the
/// acknowledged rows: a closed partition's files stay in the log's file map
/// (§7.2: a close never unregisters), so the checkpoint syncs them before it
/// drops the records that could replay them.
///
/// Mutation: `Parts::close` taking its partition's files out of the log's
/// map (`Wal::forget_files`): the checkpoint skips them, drops their
/// records, and images whose unsynced pages are dropped lose rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_close_then_a_checkpoint_cut_anywhere_keeps_every_acknowledged_row() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir, "lazy_cut").await.unwrap();
    exec(&db, CREATE).await.unwrap();
    // Every partition materialized and its files synced before the
    // recorder starts, so the rows below follow the page model: a file the
    // scenario created, no recorded sync touching it, is imaged whole.
    let first: Vec<String> = (1..=8i64).map(|t| format!("({t}, {}, 'base')", 10_000 + t)).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", first.join(", "))).await.unwrap();
    let mut want: Vec<(i64, i64)> = (1..=8i64).map(|t| (t, 10_000 + t)).collect();
    db.sync_durable().unwrap();
    let dir_s = dir.to_string_lossy().into_owned();
    let base_checkpoint = crate::core::wal::with_wal_for(&dir_s, |w| w.map(|w| w.checkpoint()));
    assert!(matches!(base_checkpoint, Some(Ok(true))), "the baseline checkpoint: {base_checkpoint:?}");
    let rec = Recorder::start(&dir);
    for round in 0..3i64 {
        let values: Vec<String> = (1..=8i64).map(|t| format!("({t}, {}, 'r{round}')", round * 100 + t)).collect();
        exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await.unwrap();
        want.extend((1..=8i64).map(|t| (t, round * 100 + t)));
    }
    let x = db.transactions.begin();
    exec_in(&db, x, "INSERT INTO pt (tenant_id, id, v) VALUES (3, 777, 'aborted')").await.unwrap();
    db.abort_transaction(x);
    want.sort();
    let t = pt(&db);
    let n = t.materialized_pids().unwrap().len() as u64;

    let sink: StdArc<Mutex<Vec<Result<CrashPoint, String>>>> = StdArc::new(Mutex::new(Vec::new()));
    let points: Vec<(&str, u64)> = vec![
        ("test.p5b.close_taken", 1),
        ("test.p5b.close_landed", 1),
        ("test.p5b.close_taken", n),
        ("test.p5b.close_landed", n),
    ];
    let guards = arm_cuts(&rec, &dir, &points, &sink);
    assert_eq!(t.close_idle().await.unwrap() as u64, n);
    assert_eq!(guards.iter().map(|g| g.fired()).sum::<u64>(), 4, "the close's cut points were never reached");
    drop(guards);
    let checkpointed = crate::core::wal::with_wal_for(&dir_s, |w| w.map(|w| w.checkpoint()));
    assert!(matches!(checkpointed, Some(Ok(true))), "the checkpoint after the close: {checkpointed:?}");
    sink.lock().push(rec.cut_quiescent("after every close and a checkpoint"));
    drop(t);
    close(&dir, db);
    drop(rec);

    let points = taken(&sink);
    assert!(points.len() >= 4, "too many cuts were disturbed: {}", points.len());
    let img = base.path().join("image");
    let mut problems = Vec::new();
    check_images(&points, &img, &want, &mut problems).await;
    assert!(problems.is_empty(), "{} problems:\n{}", problems.len(), problems.join("\n"));
}

/// Every partition closed and synced, then writes that reopen them, cut at
/// each reopen and after the acknowledgements: every image holds the rows
/// from before and every acknowledged one after.
///
/// Mutation: `Parts::open_instance` creating the partition's files afresh
/// (`files::create_partition_files`, as a materialize does): every image
/// loses the rows from before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reopen_cut_anywhere_keeps_every_acknowledged_row() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir, "lazy_cut").await.unwrap();
    exec(&db, CREATE).await.unwrap();
    let values: Vec<String> = (1..=8i64).map(|t| format!("({t}, {t}, 'before')")).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await.unwrap();
    let mut want: Vec<(i64, i64)> = (1..=8i64).map(|t| (t, t)).collect();
    let t = pt(&db);
    let n = t.materialized_pids().unwrap().len() as u64;
    t.close_idle().await.unwrap();
    db.sync_durable().unwrap();

    let rec = Recorder::start(&dir);
    let sink: StdArc<Mutex<Vec<Result<CrashPoint, String>>>> = StdArc::new(Mutex::new(Vec::new()));
    let guards = arm_cuts(&rec, &dir, &[("test.p5b.reopened", 1), ("test.p5b.reopened", n)], &sink);
    for t_ in 1..=8i64 {
        exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES ({t_}, {}, 'after')", 1000 + t_)).await.unwrap();
        want.push((t_, 1000 + t_));
    }
    assert_eq!(guards.iter().map(|g| g.fired()).sum::<u64>(), 2, "the reopens were never reached");
    drop(guards);
    // Everything acknowledged so far must be in every image cut from here.
    let acked_points_from = sink.lock().len();
    sink.lock().push(rec.cut_quiescent("after the acknowledged writes"));
    drop(t);
    close(&dir, db);
    drop(rec);

    want.sort();
    let points = taken(&sink);
    let img = base.path().join("image");
    let mut problems = Vec::new();
    // Cut at a reopen: the rows from before, and any of the writes after
    // that were acknowledged by then; checked as "the rows from before,
    // plus a subset of the later ones".
    let before: Vec<(i64, i64)> = (1..=8i64).map(|t| (t, t)).collect();
    for (i, point) in points.iter().enumerate() {
        for seed in 0..seeds() {
            point.write_image(seed, &img);
            let label = format!("{} seed {seed}", point.label);
            match open(&img, "lazy_cut").await {
                Err(e) => problems.push(format!("{label}: does not open: {e}")),
                Ok(db) => {
                    match keys(&db).await {
                        Ok(got) => {
                            let lost: Vec<_> = before.iter().filter(|k| !got.contains(k)).collect();
                            let foreign: Vec<_> = got.iter().filter(|k| !want.contains(k)).collect();
                            if !lost.is_empty() || !foreign.is_empty() {
                                problems.push(format!("{label}: lost {lost:?}, foreign {foreign:?}"));
                            }
                            if i >= acked_points_from && got != want {
                                problems.push(format!("{label}: holds {} rows, {} were acknowledged", got.len(), want.len()));
                            }
                        }
                        Err(e) => problems.push(format!("{label}: {e}")),
                    }
                    close(&img, db);
                }
            }
            let _ = std::fs::remove_dir_all(&img);
        }
    }
    assert!(problems.is_empty(), "{} problems:\n{}", problems.len(), problems.join("\n"));
}

/// I13 across a close in a power cut: the first write after a clean sweep
/// aborts and its partition closes, landing and syncing its stamped pages;
/// in every image the partition still pins and the row stays hidden once
/// every other table forgets as far as it can.
///
/// Mutation: a close recording its partition clean (`Parts::close`
/// appending `SWEPT(pid, forgotten_below, clean)` after the landing): every
/// image opens it unpinned, forgetting passes the aborted id, and the row
/// comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closed_partition_keeps_its_pin_in_every_image() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir, "lazy_cut").await.unwrap();
    exec(&db, CREATE).await.unwrap();
    let values: Vec<String> = (1..=8i64).map(|t| format!("({t}, {t}, 'c')")).collect();
    exec(&db, &format!("INSERT INTO pt (tenant_id, id, v) VALUES {}", values.join(", "))).await.unwrap();
    let t = pt(&db);
    let p = pid_of_tenant(&t, 1);
    // Every partition swept clean.
    let forget_all = |db: Arc<Database>| async move {
        for _ in 0..9_000 {
            let tx = db.transactions.begin();
            db.transactions.commit(tx);
        }
        db.transactions.settle_now();
        for name in db.table_names() {
            if let Some(t) = db.table_for_tests(&name)
                && !t.is_shell()
            {
                t.neutralize_aborted_now().await.unwrap();
            }
        }
    };
    forget_all(db.clone()).await;
    t.sweep_pins_now().await.unwrap();
    assert_eq!(t.pin_of(p).await.unwrap(), None, "the partition did not sweep clean");
    db.sync_durable().unwrap();

    let rec = Recorder::start(&dir);
    let y = db.transactions.begin();
    exec_in(&db, y, "INSERT INTO pt (tenant_id, id, v) VALUES (1, 777, 'aborted')").await.unwrap();
    db.abort_transaction(y);
    assert!(t.pin_of(p).await.unwrap().is_some(), "the first write did not pin");
    assert_eq!(t.close_partition(p).await.unwrap(), crate::core::partition::table::Close::Closed);
    let cut = rec.cut_quiescent("after an aborted first write and a close").unwrap();
    drop(t);
    close(&dir, db);
    drop(rec);

    let img = base.path().join("image");
    let mut problems = Vec::new();
    for seed in 0..seeds() {
        cut.write_image(seed, &img);
        let db = open(&img, "lazy_cut").await.unwrap();
        forget_all(db.clone()).await;
        let t = pt(&db);
        for pid in t.materialized_pids().unwrap() {
            if pid != p {
                t.sweep_pin_now(pid).await.unwrap();
            }
        }
        let forgotten = db.transactions.try_forget();
        match t.pin_of(p).await {
            Ok(Some(_)) => {}
            got => problems.push(format!("seed {seed}: partition {p} pins at {got:?}")),
        }
        match keys(&db).await {
            Ok(got) if !got.contains(&(1, 777)) => {}
            got => problems.push(format!("seed {seed}: reads {got:?} (forgotten below {forgotten}, the aborted id {y})")),
        }
        drop(t);
        close(&img, db);
        let _ = std::fs::remove_dir_all(&img);
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}
