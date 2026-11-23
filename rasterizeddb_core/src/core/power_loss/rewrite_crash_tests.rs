//! An ALTER rewrite through a power cut (slice B5, docs/persistent-indexes.md
//! §7 "As built (B5)"): every image cut anywhere in the rewrite opens with
//! the table as it was or as the rewrite left it, never a mix.

use super::*;
use parking_lot::Mutex;
use std::sync::Arc as StdArc;

/// The rewrite's own points, in the order it reaches them.
const POINTS: &[&str] = &[
    "test.rewrite.after_backup",
    "test.rewrite.after_discard",
    "test.rewrite.after_create",
    "test.rewrite.mid_fill",
    "test.rewrite.after_fill",
    "test.rewrite.after_sync",
    "test.rewrite.after_commit",
];

/// Syncs inside the rewrite to cut at as well: the backup's copies and the
/// intent's own sync.
const SYNC_CUTS: &[(&str, u64)] = &[("sync:file.data", 1), ("sync:file.data", 3), ("sync:rewrite.intent", 1)];

/// `ALTER TABLE … ADD COLUMN extra … DEFAULT 5` on a table with committed,
/// updated and deleted rows, cut at each of the rewrite's points and inside
/// its syncs; each cut imaged under every seed kind. Each image must open,
/// leave no intent behind, and hold exactly the old rows: in the old shape,
/// or in the new one with every row's `extra` at 5. A table in the new
/// shape missing rows, or in the old shape with some rows gone, is the mix
/// the backup and its intent exist to rule out.
///
/// Mutation: recovery skips the intents (`recover_interrupted_rewrites`
/// returns at once); images cut after the old files were released open
/// with the rows missing or the table empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rewrite_cut_anywhere_leaves_the_old_table_or_the_new() {
    let seeds = env_count("RDB_POWER_LOSS_SEEDS", 5, 1024);
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir, "rewrite_cut").await.unwrap();
    exec(&db, "CREATE TABLE rw (id BIGINT PRIMARY KEY, n BIGINT, s TEXT)").await.unwrap();
    let values: Vec<String> = (0..40).map(|i| format!("({i}, {}, 'w{i}')", i % 7)).collect();
    exec(&db, &format!("INSERT INTO rw (id, n, s) VALUES {}", values.join(", "))).await.unwrap();
    exec(&db, "DELETE FROM rw WHERE id >= 35").await.unwrap();
    exec(&db, "UPDATE rw SET s = 'updated' WHERE id < 5").await.unwrap();
    db.sync_durable().unwrap();
    let old_rows = rows(&db, "SELECT id, n, s FROM rw").await.unwrap();
    assert_eq!(old_rows.len(), 35);

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
    exec(&db, "ALTER TABLE rw ADD COLUMN extra BIGINT DEFAULT 5").await.unwrap();
    for g in &guards {
        assert_eq!(g.fired(), 1, "a cut point was never reached");
    }
    drop(guards);
    let new_rows = rows(&db, "SELECT id, n, s, extra FROM rw").await.unwrap();
    assert_eq!(new_rows.len(), 35);
    assert_eq!(rows(&db, "SELECT id, n, s FROM rw").await.unwrap(), old_rows);
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
            match open(&img, "rewrite_cut").await {
                Err(e) => problems.push(format!("{label}: does not open: {e}")),
                Ok(db) => {
                    let leftover: Vec<_> = std::fs::read_dir(&img)
                        .unwrap()
                        .filter_map(|e| e.ok())
                        .map(|e| e.file_name().to_string_lossy().to_string())
                        .filter(|n| n.starts_with("__rewrite_"))
                        .collect();
                    if !leftover.is_empty() {
                        problems.push(format!("{label}: intents left after the open: {leftover:?}"));
                    }
                    match rows(&db, "SELECT id, n, s, extra FROM rw").await {
                        Ok(got) if got == new_rows => new += 1,
                        Ok(got) => problems.push(format!(
                            "{label}: the new shape with {} rows, not the rewrite's {}",
                            got.len(),
                            new_rows.len()
                        )),
                        Err(_) => match rows(&db, "SELECT id, n, s FROM rw").await {
                            Ok(got) if got == old_rows => old += 1,
                            Ok(got) => problems.push(format!(
                                "{label}: the old shape with {} rows, not the table's {}",
                                got.len(),
                                old_rows.len()
                            )),
                            Err(e) => problems.push(format!("{label}: the table does not read: {e}")),
                        },
                    }
                    close(&img, db);
                }
            }
            let _ = std::fs::remove_dir_all(&img);
        }
    }
    assert!(problems.is_empty(), "{} problems:\n{}", problems.len(), problems.join("\n"));
    assert!(old > 0 && new > 0, "every image fell on one side (old {old}, new {new}); the cuts test nothing");
    eprintln!("rewrite cut: {} points, {old} old images, {new} new images", points.len());
}
