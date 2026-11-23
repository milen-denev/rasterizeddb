//! A plain `ALTER TABLE … RENAME TO` through a power cut: every image cut
//! anywhere in the rename opens with the table under exactly one of its two
//! names, holding every row, with no intent and no copy left behind.
//!
//! The intent was once a logged file truncated and appended in place, and
//! its older two-line form was accepted without a checksum: an image holding
//! a torn `pt\npt` read as "rename pt to pt", recovery tombstoned the only
//! catalogue row and discarded the table's files.

use super::*;
use parking_lot::Mutex;
use std::sync::Arc as StdArc;

/// The rename's own points, in the order it reaches them.
const POINTS: &[&str] = &[
    "test.rename.after_intent",
    "test.rename.after_copy",
    "test.rename.after_schema",
    "test.rename.after_commit",
    "test.rename.after_tombstone",
    "test.rename.after_discard",
    "test.rename.after_clear",
];

/// Syncs inside the rename to cut at as well: the intent's own, and the
/// copy's first data syncs.
const SYNC_CUTS: &[(&str, u64)] = &[("sync:rewrite.intent", 1), ("sync:file.data", 1), ("sync:file.data", 3)];

fn names_in(dir: &Path, prefix: &str) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with(prefix))
        .collect();
    names.sort();
    names
}

/// Mutation: recovery returning at once (`recover_interrupted_rename`)
/// leaves images cut past the commit with both names, and images cut before
/// it with the copy's files.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rename_cut_anywhere_leaves_the_table_under_one_name() {
    let seeds = env_count("RDB_POWER_LOSS_SEEDS", 5, 1024);
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir, "rename_cut").await.unwrap();
    exec(&db, "CREATE TABLE pt (id BIGINT PRIMARY KEY, n BIGINT, s TEXT)").await.unwrap();
    let values: Vec<String> = (0..40).map(|i| format!("({i}, {}, 'w{i}')", i % 7)).collect();
    exec(&db, &format!("INSERT INTO pt (id, n, s) VALUES {}", values.join(", "))).await.unwrap();
    exec(&db, "DELETE FROM pt WHERE id >= 35").await.unwrap();
    exec(&db, "UPDATE pt SET s = 'updated' WHERE id < 5").await.unwrap();
    db.sync_durable().unwrap();
    let want = rows(&db, "SELECT id, n, s FROM pt").await.unwrap();
    assert_eq!(want.len(), 35);

    let rec = Recorder::start(&dir);
    let sink: StdArc<Mutex<Vec<Result<CrashPoint, String>>>> = StdArc::new(Mutex::new(Vec::new()));
    let mut guards = Vec::new();
    for point in POINTS {
        let (rec, sink, label) = (rec.clone(), sink.clone(), point.to_string());
        let action: failpoint::Action = StdArc::new(move |_hit| {
            sink.lock().push(rec.cut_quiescent(label.clone()));
            failpoint::Outcome::Continue
        });
        guards.push(failpoint::arm(point, &dir, 1, action));
    }
    for (site, nth) in SYNC_CUTS {
        guards.push(failpoint::arm(site, &dir, *nth, rec.cut_action(format!("{site} #{nth}"), sink.clone())));
    }
    exec(&db, "ALTER TABLE pt RENAME TO pt2").await.unwrap();
    for g in &guards {
        assert_eq!(g.fired(), 1, "a cut point was never reached");
    }
    drop(guards);
    assert_eq!(rows(&db, "SELECT id, n, s FROM pt2").await.unwrap(), want);
    close(&dir, db);
    drop(rec);

    let points: Vec<CrashPoint> = sink
        .lock()
        .drain(..)
        .filter_map(|p| p.map_err(|why| eprintln!("{why}")).ok())
        .collect();
    assert!(points.len() >= POINTS.len(), "too many cuts were disturbed: {}", points.len());
    let img = base.path().join("image");
    let (mut old, mut new) = (0, 0);
    let mut problems = Vec::new();
    for point in &points {
        for seed in 0..seeds {
            point.write_image(seed, &img);
            let label = format!("{} seed {seed}", point.label);
            match open(&img, "rename_cut").await {
                Err(e) => problems.push(format!("{label}: does not open: {e}")),
                Ok(db) => {
                    let intents = names_in(&img, "__rename.");
                    if !intents.is_empty() {
                        problems.push(format!("{label}: intents left after the open: {intents:?}"));
                    }
                    let under_old = rows(&db, "SELECT id, n, s FROM pt").await;
                    let under_new = rows(&db, "SELECT id, n, s FROM pt2").await;
                    match (&under_old, &under_new) {
                        (Ok(got), Err(_)) if *got == want => {
                            old += 1;
                            let copy = names_in(&img, "pt2");
                            if !copy.is_empty() {
                                problems.push(format!("{label}: the abandoned copy's files were kept: {copy:?}"));
                            }
                        }
                        (Err(_), Ok(got)) if *got == want => new += 1,
                        (Ok(_), Ok(_)) => problems.push(format!("{label}: both names are tables")),
                        (Err(a), Err(b)) => problems.push(format!("{label}: the table is gone ({a}; {b})")),
                        _ => problems.push(format!(
                            "{label}: the table lost rows (old {:?} rows, new {:?} rows, of {})",
                            under_old.as_ref().map(|r| r.len()),
                            under_new.as_ref().map(|r| r.len()),
                            want.len()
                        )),
                    }
                    close(&img, db);
                }
            }
            let _ = std::fs::remove_dir_all(&img);
        }
    }
    assert!(problems.is_empty(), "{} problems:\n{}", problems.len(), problems.join("\n"));
    assert!(old > 0 && new > 0, "every image fell on one side (old {old}, new {new}); the cuts test nothing");
    eprintln!("rename cut: {} points, {old} old images, {new} new images", points.len());
}

/// Two renames issued at once, cut at each of either's points: one intent
/// file serves both, so they run one after the other, and each image holds
/// each table under exactly one of its names, every row kept, no intent and
/// no copy left. Unserialised, a cut could find one rename's copy beside the
/// other's intent, which recovery cannot see and so never discards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_renames_in_flight_cut_anywhere_leave_each_table_under_one_name() {
    let seeds = env_count("RDB_POWER_LOSS_SEEDS", 5, 1024);
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir, "rename_pair").await.unwrap();
    let mut want = Vec::new();
    for t in ["qa", "qb"] {
        exec(&db, &format!("CREATE TABLE {t} (id BIGINT PRIMARY KEY, s TEXT)")).await.unwrap();
        let values: Vec<String> = (0..20).map(|i| format!("({i}, '{t}{i}')")).collect();
        exec(&db, &format!("INSERT INTO {t} (id, s) VALUES {}", values.join(", "))).await.unwrap();
        want.push(rows(&db, &format!("SELECT id, s FROM {t}")).await.unwrap());
    }
    db.sync_durable().unwrap();

    let rec = Recorder::start(&dir);
    let sink: StdArc<Mutex<Vec<Result<CrashPoint, String>>>> = StdArc::new(Mutex::new(Vec::new()));
    let mut guards = Vec::new();
    for point in POINTS {
        for nth in [1u64, 2] {
            let (rec, sink, label) = (rec.clone(), sink.clone(), format!("{point} #{nth}"));
            let action: failpoint::Action = StdArc::new(move |_hit| {
                sink.lock().push(rec.cut_quiescent(label.clone()));
                failpoint::Outcome::Continue
            });
            guards.push(failpoint::arm(point, &dir, nth, action));
        }
    }
    let (a, b) = tokio::join!(exec(&db, "ALTER TABLE qa RENAME TO qa2"), exec(&db, "ALTER TABLE qb RENAME TO qb2"));
    a.unwrap();
    b.unwrap();
    // Each point twice, once per rename: every armed cut was reached.
    for g in &guards {
        assert_eq!(g.fired(), 1, "a cut point was never reached");
    }
    drop(guards);
    close(&dir, db);
    drop(rec);

    let points: Vec<CrashPoint> = sink
        .lock()
        .drain(..)
        .filter_map(|p| p.map_err(|why| eprintln!("{why}")).ok())
        .collect();
    assert!(points.len() >= POINTS.len(), "too many cuts were disturbed: {} of {}", points.len(), 2 * POINTS.len());
    let img = base.path().join("image");
    let mut problems = Vec::new();
    for point in &points {
        for seed in 0..seeds {
            point.write_image(seed, &img);
            let label = format!("{} seed {seed}", point.label);
            match open(&img, "rename_pair").await {
                Err(e) => problems.push(format!("{label}: does not open: {e}")),
                Ok(db) => {
                    let intents = names_in(&img, "__rename.");
                    if !intents.is_empty() {
                        problems.push(format!("{label}: intents left after the open: {intents:?}"));
                    }
                    for (t, want) in ["qa", "qb"].iter().zip(&want) {
                        let moved = format!("{t}2");
                        let under_old = rows(&db, &format!("SELECT id, s FROM {t}")).await;
                        let under_new = rows(&db, &format!("SELECT id, s FROM {moved}")).await;
                        match (&under_old, &under_new) {
                            (Ok(got), Err(_)) if got == want => {
                                let copy = names_in(&img, &moved);
                                if !copy.is_empty() {
                                    problems.push(format!("{label}: {t}'s abandoned copy was kept: {copy:?}"));
                                }
                            }
                            (Err(_), Ok(got)) if got == want => {}
                            _ => problems.push(format!(
                                "{label}: {t} is not one table with its rows (old {:?}, new {:?})",
                                under_old.as_ref().map(|r| r.len()),
                                under_new.as_ref().map(|r| r.len())
                            )),
                        }
                    }
                    close(&img, db);
                }
            }
            let _ = std::fs::remove_dir_all(&img);
        }
    }
    assert!(problems.is_empty(), "{} problems:\n{}", problems.len(), problems.join("\n"));
    eprintln!("rename pair cut: {} points", points.len());
}
