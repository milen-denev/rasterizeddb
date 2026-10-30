//! The crash-image model and the failpoint registry, on plain files.
//!
//! Syncs here are simulated the way every engine site performs them: a
//! `sync_audit::note` naming the file, then the sync, on the same thread.

use std::path::{Path, PathBuf};
use std::sync::Arc as StdArc;
use std::sync::mpsc;

use parking_lot::Mutex;

use super::failpoint::{self, Outcome, failpoint};
use super::image::{PAGE, Recorder, Rng, mix_pages};
use crate::core::sync_audit::{self, DirOp};

/// Seeds by kind (see `image::seed_kind`).
const P0: u64 = 0;
const P1: u64 = 1;
const P1_LOGS_SYNCED: u64 = 2;
const HALF: u64 = 3;

fn sync_file(path: &Path) {
    sync_audit::note("file.data", path);
    std::fs::File::options().write(true).open(path).unwrap().sync_data().unwrap();
}

fn sync_dir_of(path: &Path) {
    sync_audit::note("dir", path);
    std::fs::File::open(path.parent().unwrap()).unwrap().sync_all().unwrap();
}

fn write_at(path: &Path, at: u64, bytes: &[u8]) {
    use std::os::unix::fs::FileExt;
    let f = std::fs::File::options().write(true).create(true).truncate(false).open(path).unwrap();
    f.write_all_at(bytes, at).unwrap();
}

fn rel(name: &str) -> PathBuf {
    PathBuf::from(name)
}

#[test]
fn an_image_holds_each_file_as_of_its_last_completed_sync() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.db");
    std::fs::write(&a, vec![b'A'; 5000]).unwrap();
    let rec = Recorder::start(dir.path());

    write_at(&a, 0, &vec![b'B'; PAGE]);
    sync_file(&a);
    let synced = std::fs::read(&a).unwrap();
    // Unsynced from here: a page overwritten and a tail appended.
    write_at(&a, 0, &vec![b'D'; PAGE]);
    write_at(&a, 5000, &vec![b'C'; 9000]);
    let current = std::fs::read(&a).unwrap();

    let cp = rec.cut_quiescent("pages").unwrap();
    assert_eq!(cp.image(P0)[&rel("a.db")], synced, "p = 0 keeps exactly the synced bytes");
    assert_eq!(cp.image(P1)[&rel("a.db")], current, "p = 1 keeps the page cache");
    assert_eq!(cp.image(HALF), cp.image(HALF), "a seed reproduces");
    // Every byte of a mixed image is the synced or the current byte there,
    // or a zero past the synced length.
    for seed in [HALF, 4, 8, 9] {
        let img = &cp.image(seed)[&rel("a.db")];
        assert!(img.len() >= synced.len().min(current.len()) && img.len() <= current.len().max(synced.len()));
        for (j, b) in img.iter().enumerate() {
            let s = synced.get(j).copied().unwrap_or(0);
            assert!(*b == s || Some(b) == current.get(j), "seed {seed}: byte {j} came from nowhere");
        }
    }
}

#[test]
fn half_keeps_some_unsynced_pages_and_drops_others() {
    let base = vec![1u8; PAGE * 64];
    let current = vec![2u8; PAGE * 64];
    let out = mix_pages(&base, &current, 0.5, true, &mut Rng::new(7));
    let kept = out.chunks(PAGE).filter(|p| p[0] == 2).count();
    assert!(kept > 8 && kept < 56, "{kept} of 64 pages kept at p = 0.5");
    for p in out.chunks(PAGE) {
        assert!(p.iter().all(|b| *b == p[0]), "a page is kept or dropped whole");
    }
}

#[test]
fn a_kept_page_after_a_dropped_one_is_a_torn_tail() {
    // One synced page; three appended. Some seed keeps a later page while
    // dropping an earlier one, which reads as zeros then data.
    let base = vec![9u8; PAGE];
    let current = [vec![9u8; PAGE], vec![5u8; PAGE * 3]].concat();
    let torn = (0..64).any(|seed| {
        let out = mix_pages(&base, &current, 0.5, false, &mut Rng::new(seed));
        let pages: Vec<u8> = out.chunks(PAGE).map(|p| p[0]).collect();
        pages.windows(2).any(|w| w[0] == 0 && w[1] == 5)
    });
    assert!(torn, "no seed produced a torn tail");
}

#[test]
fn a_log_is_cut_at_its_synced_length_for_the_log_seed() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join(crate::core::wal::DEFAULT_WAL_NAME);
    std::fs::write(&log, b"head").unwrap();
    let rec = Recorder::start(dir.path());
    write_at(&log, 4, &vec![b'r'; 3 * PAGE]);
    sync_audit::note("wal.log", &log);
    std::fs::File::options().write(true).open(&log).unwrap().sync_data().unwrap();
    let synced = std::fs::read(&log).unwrap();
    write_at(&log, synced.len() as u64, &vec![b'u'; 2 * PAGE]);
    let cp = rec.cut_quiescent("log").unwrap();
    let name = rel(crate::core::wal::DEFAULT_WAL_NAME);
    assert_eq!(cp.image(P1_LOGS_SYNCED)[&name], synced);
    assert_eq!(cp.image(P1)[&name].len(), synced.len() + 2 * PAGE);
}

#[test]
fn a_sync_still_inside_its_note_makes_nothing_durable() {
    let dir = tempfile::tempdir().unwrap();
    let b = dir.path().join("b.db");
    std::fs::write(&b, b"before").unwrap();
    let rec = Recorder::start(dir.path());

    let (parked_tx, parked_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let go_rx = Mutex::new(go_rx);
    let parked_tx = Mutex::new(parked_tx);
    let _fp = failpoint::arm(
        "sync:file.data",
        dir.path(),
        1,
        StdArc::new(move |_| {
            parked_tx.lock().send(()).unwrap();
            go_rx.lock().recv().unwrap();
            Outcome::Continue
        }),
    );
    let writer = {
        let b = b.clone();
        std::thread::spawn(move || {
            std::fs::write(&b, b"after!").unwrap();
            sync_file(&b);
        })
    };
    parked_rx.recv().unwrap();
    let during = rec.cut_quiescent("during").unwrap();
    assert_eq!(
        during.image(P0)[&rel("b.db")],
        b"before",
        "a sync whose thread is still inside its note was taken as durable"
    );
    assert_eq!(during.in_flight.len(), 1, "{:?}", during.in_flight);
    go_tx.send(()).unwrap();
    writer.join().unwrap();
    let after = rec.cut_quiescent("after").unwrap();
    assert_eq!(after.image(P0)[&rel("b.db")], b"after!", "a returned sync was not taken as durable");
}

#[test]
fn a_sync_on_another_thread_that_returned_is_durable() {
    let dir = tempfile::tempdir().unwrap();
    let c = dir.path().join("c.db");
    std::fs::write(&c, b"old").unwrap();
    let rec = Recorder::start(dir.path());
    let (synced_tx, synced_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let t = {
        let c = c.clone();
        std::thread::spawn(move || {
            std::fs::write(&c, b"new").unwrap();
            sync_file(&c);
            synced_tx.send(()).unwrap();
            // Parked in a futex, not a sync: the sync has returned.
            go_rx.recv().unwrap();
        })
    };
    synced_rx.recv().unwrap();
    let cp = rec.cut_quiescent("returned").unwrap();
    go_tx.send(()).unwrap();
    t.join().unwrap();
    assert_eq!(cp.image(P0)[&rel("c.db")], b"new");
    assert!(cp.in_flight.is_empty(), "{:?}", cp.in_flight);
}

#[test]
fn the_failpoints_own_sync_is_in_flight_and_earlier_ones_are_not() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path().join("d.db");
    std::fs::write(&d, b"0").unwrap();
    let rec = Recorder::start(dir.path());
    let sink = StdArc::new(Mutex::new(Vec::new()));
    let fp = failpoint::arm("sync:file.data", dir.path(), 2, rec.cut_action("second".into(), sink.clone()));
    std::fs::write(&d, b"1").unwrap();
    sync_file(&d);
    std::fs::write(&d, b"2").unwrap();
    sync_file(&d);
    assert_eq!(fp.fired(), 1);
    let cp = sink.lock().pop().unwrap().unwrap();
    assert_eq!(cp.image(P0)[&rel("d.db")], b"1", "the first sync returned, the second was about to issue");
    assert_eq!(cp.image(P1)[&rel("d.db")], b"2");
}

/// A failpoint that is not a sync site cuts with every sync its thread
/// issued already returned: the code reached it past them. The seal's
/// `clean_append_before_fsync` sits after the `.ixs` rename's directory
/// fsync and a `CLEAN` pwrite, with no sync noted between on that thread;
/// the model took the returned directory sync for one in flight, so seeds
/// kept the unsynced `CLEAN` page while undoing the rename it follows (a
/// `CLEAN` over the old `.ixs`, which only the open's footer check caught).
/// Intermittent there because the seal's task can resume on another worker
/// between the two steps, whose syncs the model probes instead.
#[test]
fn a_plain_failpoint_cuts_after_its_threads_returned_syncs() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("s.ixs");
    let log = dir.path().join("s.ixm");
    std::fs::write(&target, b"old snapshot").unwrap();
    std::fs::write(&log, b"LOG").unwrap();
    let rec = Recorder::start(dir.path());
    let sink = StdArc::new(Mutex::new(Vec::new()));
    let fp = failpoint::arm("test.after_dir_sync", dir.path(), 1, rec.cut_action("after".into(), sink.clone()));
    let tmp = dir.path().join("s.ixs.tmp");
    sync_audit::dir_op(DirOp::Create(&tmp));
    std::fs::write(&tmp, b"new snapshot").unwrap();
    sync_file(&tmp);
    sync_audit::dir_op(DirOp::Rename { from: &tmp, to: &target });
    std::fs::rename(&tmp, &target).unwrap();
    sync_dir_of(&target);
    // The CLEAN-like append, unsynced, then the failpoint before its sync.
    write_at(&log, 3, b"+CLEAN");
    failpoint!("test.after_dir_sync", &log).unwrap();
    assert_eq!(fp.fired(), 1);
    let cp = sink.lock().pop().unwrap().unwrap();
    for seed in 0..40 {
        let img = cp.image(seed);
        assert_eq!(
            img[&rel("s.ixs")],
            b"new snapshot",
            "seed {seed}: the rename was undone although its directory sync had returned"
        );
        assert!(!img.contains_key(&rel("s.ixs.tmp")), "seed {seed}");
    }
    assert_eq!(cp.image(P0)[&rel("s.ixm")], b"LOG", "the unsynced append is not durable");
    assert_eq!(cp.image(P1)[&rel("s.ixm")], b"LOG+CLEAN");
}

#[test]
fn announced_directory_operations_without_a_directory_sync_may_be_undone() {
    let dir = tempfile::tempdir().unwrap();
    let old = dir.path().join("old.db");
    let target = dir.path().join("t.db");
    std::fs::write(&old, b"old bytes").unwrap();
    std::fs::write(&target, b"T").unwrap();
    let rec = Recorder::start(dir.path());

    // Created and synced, but its directory entry is not.
    let new = dir.path().join("new.db");
    sync_audit::dir_op(DirOp::Create(&new));
    std::fs::write(&new, b"new bytes").unwrap();
    sync_file(&new);
    // Unlinked, likewise.
    sync_audit::dir_op(DirOp::Unlink(&old));
    std::fs::remove_file(&old).unwrap();
    // A temporary file renamed over a durable one.
    let tmp = dir.path().join("t.db.tmp");
    sync_audit::dir_op(DirOp::Create(&tmp));
    std::fs::write(&tmp, b"replacement").unwrap();
    sync_file(&tmp);
    sync_audit::dir_op(DirOp::Rename { from: &tmp, to: &target });
    std::fs::rename(&tmp, &target).unwrap();

    let cp = rec.cut_quiescent("undurable").unwrap();
    let undone = cp.image(P0);
    assert!(!undone.contains_key(&rel("new.db")), "{:?}", undone.keys());
    assert_eq!(undone[&rel("old.db")], b"old bytes");
    assert_eq!(undone[&rel("t.db")], b"T");
    assert!(!undone.contains_key(&rel("t.db.tmp")));
    let kept = cp.image(P1);
    assert_eq!(kept[&rel("new.db")], b"new bytes");
    assert!(!kept.contains_key(&rel("old.db")));
    assert_eq!(kept[&rel("t.db")], b"replacement");
    // Mixed seeds keep a prefix: never the rename without the create
    // before it, never the unlink undone while the rename after it is kept.
    for seed in 3..40 {
        let img = cp.image(seed);
        if img.get(&rel("t.db")).map(Vec::as_slice) == Some(b"replacement") {
            assert!(img.contains_key(&rel("new.db")) && !img.contains_key(&rel("old.db")), "seed {seed}");
        }
    }

    // A completed sync of the directory makes all three durable.
    sync_dir_of(&new);
    let cp = rec.cut_quiescent("durable").unwrap();
    let img = cp.image(P0);
    assert_eq!(img[&rel("new.db")], b"new bytes");
    assert!(!img.contains_key(&rel("old.db")));
    assert_eq!(img[&rel("t.db")], b"replacement");
}

/// `partition::files::sync_dir` notes the directory itself as
/// `partition_dir`, where `wal::sync_parent_dir` notes a file in it as `dir`:
/// either makes the directory's earlier operations durable.
#[test]
fn a_partition_directory_sync_makes_its_entries_durable() {
    let dir = tempfile::tempdir().unwrap();
    let rec = Recorder::start(dir.path());
    let name = "__pt_0000000000000001_00000000.db";
    let new = dir.path().join(name);
    sync_audit::dir_op(DirOp::Create(&new));
    std::fs::write(&new, b"partition").unwrap();
    sync_file(&new);
    let cp = rec.cut_quiescent("undurable").unwrap();
    assert!(!cp.image(P0).contains_key(&rel(name)), "a created entry was durable before its directory sync");

    crate::core::partition::files::sync_dir(dir.path()).unwrap();
    let cp = rec.cut_quiescent("durable").unwrap();
    assert_eq!(cp.image(P0)[&rel(name)], b"partition", "the partition directory sync made nothing durable");
}

#[test]
fn a_created_file_nothing_synced_or_announced_follows_the_directory() {
    // The documented gap: sites that sync without noting are invisible, so
    // such a file is taken whole rather than lost.
    let dir = tempfile::tempdir().unwrap();
    let rec = Recorder::start(dir.path());
    let w = dir.path().join("w.db");
    std::fs::write(&w, b"unhooked").unwrap();
    let cp = rec.cut_quiescent("unhooked").unwrap();
    assert_eq!(cp.image(P0)[&rel("w.db")], b"unhooked");
    // Once a recorded sync touches it, the page model applies: issued but
    // the bytes after it are not durable.
    sync_file(&w);
    write_at(&w, 0, b"UNHOOKED");
    let cp = rec.cut_quiescent("hooked").unwrap();
    assert_eq!(cp.image(P0)[&rel("w.db")], b"unhooked");
}

#[test]
fn a_failpoint_fires_on_its_nth_hit_only_under_its_directory() {
    let mine = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let hits = StdArc::new(Mutex::new(Vec::new()));
    let seen = hits.clone();
    let fp = failpoint::arm(
        "test.nth",
        mine.path(),
        3,
        StdArc::new(move |h| {
            seen.lock().push(h.nth);
            Outcome::Continue
        }),
    );
    for _ in 0..5 {
        failpoint!("test.nth", &other.path().join("x")).unwrap();
    }
    assert_eq!(fp.fired(), 0, "hit from another directory");
    for _ in 0..5 {
        failpoint!("test.nth", &mine.path().join("x")).unwrap();
    }
    assert_eq!(fp.fired(), 1);
    assert_eq!(*hits.lock(), vec![3]);
    drop(fp);
    failpoint!("test.nth", &mine.path().join("x")).unwrap();
}

#[test]
fn a_failing_failpoint_fails_its_site() {
    let dir = tempfile::tempdir().unwrap();
    let _fp = failpoint::arm(
        "test.fail",
        dir.path(),
        1,
        StdArc::new(|_| Outcome::Fail(std::io::Error::other("injected"))),
    );
    let site = || -> std::io::Result<u8> {
        failpoint!("test.fail", &dir.path().join("y"))?;
        Ok(1)
    };
    assert!(site().is_err());
    assert_eq!(site().unwrap(), 1, "fires once");
}

#[test]
#[should_panic(expected = "no failpoint is named")]
fn an_unknown_failpoint_cannot_be_armed() {
    let dir = tempfile::tempdir().unwrap();
    let _ = failpoint::arm("snapshot_before_fsnyc", dir.path(), 1, StdArc::new(|_| Outcome::Continue));
}

#[test]
fn every_v1_failpoint_and_every_sync_site_can_be_armed() {
    let dir = tempfile::tempdir().unwrap();
    for name in failpoint::V1_NAMES {
        drop(failpoint::arm(name, dir.path(), 1, StdArc::new(|_| Outcome::Continue)));
    }
    for (tag, _) in super::SYNC_TAGS {
        drop(failpoint::arm(&format!("sync:{tag}"), dir.path(), 1, StdArc::new(|_| Outcome::Continue)));
    }
}

/// The model reads each sync by its tag. A new `note` tag it does not list
/// would be a sync it misreads (a byte flush taken for nothing, or the
/// reverse), so the list must match the source exactly.
#[test]
fn the_model_knows_every_noted_sync_tag() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut found = std::collections::BTreeSet::new();
    let mut stack = vec![src];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            if p.extension().is_none_or(|x| x != "rs") || p.ends_with("power_loss/image_tests.rs") {
                continue;
            }
            let text = std::fs::read_to_string(&p).unwrap();
            for call in ["sync_audit::note(\"", "sync_audit::note_file(\""] {
                for (i, _) in text.match_indices(call) {
                    let rest = &text[i + call.len()..];
                    let tag = &rest[..rest.find('"').unwrap()];
                    found.insert(tag.to_string());
                }
            }
            // `wal::write_mark(path, partial, tag, value)` notes the tag it
            // is passed: the second string literal of each call.
            for (i, _) in text.match_indices("write_mark(&") {
                let args = &text[i..i + text[i..].find('\n').unwrap()];
                if let Some(tag) = args.split('"').nth(3) {
                    found.insert(tag.to_string());
                }
            }
        }
    }
    let listed: std::collections::BTreeSet<String> = super::SYNC_TAGS.iter().map(|(t, _)| t.to_string()).collect();
    assert_eq!(found, listed, "power_loss::SYNC_TAGS differs from the tags the source notes");
}

/// `keep_tail_after_rename_fails` on the log itself (design §11, Q13): a
/// keep-tail checkpoint renames a new log, holding only the records its sync
/// pass did not cover, over the old one. Cut right after that rename, before
/// the directory sync, every image — rename kept or reverted, unsynced pages
/// kept or dropped — replays every acknowledged record. When the site then
/// fails, the log is poisoned: the commit that follows is refused, and a cut
/// after it finds the same records and nothing of the refused one.
#[test]
fn a_keep_tail_checkpoint_loses_no_acknowledged_record_at_a_power_cut() {
    use crate::core::wal::{self, Wal, test_hooks};
    for fail in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join(wal::DEFAULT_WAL_NAME);
        let data = dir.path().join("data.db");
        std::fs::write(&data, vec![0u8; 64]).unwrap();
        let log = StdArc::new(Wal::open(&wal_path).unwrap());
        let id = wal::file_id_for_name("data.db");
        log.register_file(id, &data).unwrap();
        let rec = Recorder::start(dir.path());

        // An acknowledged append: the data written, then logged and synced.
        write_at(&data, 0, b"EARLY");
        log.log_and_commit(id, 0, b"EARLY").unwrap();
        // Another, logged and acknowledged during the checkpoint's sync pass,
        // so the checkpoint keeps it as the new log's tail.
        {
            let log = log.clone();
            let data = data.clone();
            test_hooks::AFTER_CHECKPOINT_SYNC.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    write_at(&data, 32, b"LATE");
                    let t = log.begin();
                    log.log(t, id, 32, b"LATE").unwrap();
                    let seq = log.commit_txn_deferred_seq(t).unwrap();
                    log.commit_through(seq).unwrap();
                }));
            });
        }
        let sink = StdArc::new(Mutex::new(Vec::new()));
        let action: failpoint::Action = {
            let rec = rec.clone();
            let sink = sink.clone();
            StdArc::new(move |_| {
                // Nothing of this thread's is in flight: the new log's sync
                // returned before the rename.
                sink.lock().push(rec.cut_quiescent("right after the keep-tail rename"));
                if fail {
                    Outcome::Fail(std::io::Error::other("injected failure after the rename"))
                } else {
                    Outcome::Continue
                }
            })
        };
        let fp = failpoint::arm("keep_tail_after_rename_fails", dir.path(), 1, action);
        let outcome = log.checkpoint();
        test_hooks::AFTER_CHECKPOINT_SYNC.with(|hook| *hook.borrow_mut() = None);
        assert_eq!(fp.fired(), 1, "fail={fail}: the checkpoint kept no tail, so nothing was renamed");
        drop(fp);

        if fail {
            assert!(outcome.is_err(), "the failure after the rename was swallowed");
            assert!(log.is_poisoned(), "a failure after the keep-tail rename left the log unpoisoned");
            assert!(
                log.log_and_commit(id, 48, b"AFTER").is_err(),
                "a commit after the failed checkpoint was acknowledged into the unlinked log"
            );
        } else {
            assert!(outcome.unwrap(), "the checkpoint declined");
        }
        sink.lock().push(rec.cut_quiescent("after the checkpoint"));
        let points: Vec<_> = std::mem::take(&mut *sink.lock());
        drop(rec);
        drop(log);

        let img = dir.path().join("image");
        let mut reverted = 0;
        for point in points {
            let point = point.unwrap_or_else(|why| panic!("fail={fail}: {why}"));
            for seed in 0..40 {
                point.write_image(seed, &img);
                let replayed = img.join("data.db");
                let image_log = img.join(wal::DEFAULT_WAL_NAME);
                // The old log is the only one holding the head's record.
                let kept_old = std::fs::read(&image_log)
                    .map(|b| b.windows(5).any(|w| w == b"EARLY"))
                    .unwrap_or(false);
                reverted += kept_old as usize;
                Wal::recover(&image_log, |f| (f == id).then(|| replayed.clone()))
                    .unwrap_or_else(|e| panic!("fail={fail} {} seed {seed}: replay failed: {e}", point.label));
                let bytes = std::fs::read(&replayed).unwrap();
                let what = format!("fail={fail}, {}, seed {seed}", point.label);
                assert_eq!(&bytes[0..5], b"EARLY", "{what}: an acknowledged record was lost");
                assert_eq!(&bytes[32..36], b"LATE", "{what}: the kept tail was lost");
                assert_ne!(&bytes[48..53], b"AFTER", "{what}: a refused commit was replayed");
                let _ = std::fs::remove_dir_all(&img);
            }
        }
        // Some seeds undid the rename, and replayed the old log whole.
        assert!(reverted > 0, "fail={fail}: no image reverted the rename, so that case went untested");
    }
}

/// A transaction whose commit marker could not be written is uncommitted:
/// its held in-place overwrite is never applied, it holds no claim on the log
/// (so checkpoints keep working), and a power cut right after the failure, or
/// after a later commit has synced its records into the log, replays none of
/// it while keeping every acknowledged write.
///
/// Mutation: `finish_txn` returning the marker's error without `abort_txn`
/// (the claim stays, the checkpoint declines).
#[test]
fn a_failed_commit_marker_is_uncommitted_and_releases_its_claim() {
    use crate::core::wal::{self, Wal, test_hooks};
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join(wal::DEFAULT_WAL_NAME);
    let data = dir.path().join("data.db");
    std::fs::write(&data, vec![b'.'; 64]).unwrap();
    let log = Wal::open(&wal_path).unwrap();
    let id = wal::file_id_for_name("data.db");
    log.register_file(id, &data).unwrap();
    let rec = Recorder::start(dir.path());

    let commit_in_place = |at: u64, bytes: &[u8]| {
        let t = log.begin();
        log.log_in_place(t, id, at, bytes).unwrap();
        log.commit_txn(t).unwrap();
    };
    commit_in_place(0, b"EARLY");

    let failed = log.begin();
    log.log_in_place(failed, id, 16, b"LOST").unwrap();
    test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(true));
    let outcome = log.commit_txn(failed);
    test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(false));
    assert!(outcome.is_err(), "the injected marker failure never fired");
    assert!(!log.is_poisoned(), "a cleanly cut-back marker poisoned the log");
    assert_eq!(log.pending_transactions(), 0, "the failed commit kept its claim on the log");
    assert_eq!(&std::fs::read(&data).unwrap()[16..20], b"....", "an uncommitted overwrite was applied");
    let mut points = vec![rec.cut_quiescent("right after the failed marker")];

    // A later commit syncs the failed transaction's record into the log too.
    commit_in_place(32, b"LATE");
    points.push(rec.cut_quiescent("after a later commit"));
    assert!(log.checkpoint().unwrap(), "the checkpoint declined after a failed marker");
    commit_in_place(48, b"AFTER");
    points.push(rec.cut_quiescent("after the checkpoint and another commit"));
    drop(rec);
    drop(log);

    let img = dir.path().join("image");
    for (n, point) in points.into_iter().enumerate() {
        let point = point.unwrap_or_else(|why| panic!("{why}"));
        for seed in 0..10 {
            point.write_image(seed, &img);
            let replayed = img.join("data.db");
            let image_log = img.join(wal::DEFAULT_WAL_NAME);
            if image_log.exists() {
                Wal::recover(&image_log, |f| (f == id).then(|| replayed.clone()))
                    .unwrap_or_else(|e| panic!("{} seed {seed}: replay failed: {e}", point.label));
            }
            let bytes = std::fs::read(&replayed).unwrap();
            let what = format!("{}, seed {seed}", point.label);
            assert_eq!(&bytes[0..5], b"EARLY", "{what}: an acknowledged write was lost");
            assert_eq!(&bytes[16..20], b"....", "{what}: the uncommitted overwrite was replayed");
            if n >= 1 {
                assert_eq!(&bytes[32..36], b"LATE", "{what}: an acknowledged write was lost");
            }
            if n >= 2 {
                assert_eq!(&bytes[48..53], b"AFTER", "{what}: an acknowledged write was lost");
            }
            let _ = std::fs::remove_dir_all(&img);
        }
    }
}
