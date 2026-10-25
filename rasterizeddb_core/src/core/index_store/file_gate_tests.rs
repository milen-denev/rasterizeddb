//! The file gate on plain files: the state an open derives from `.ixm` (and
//! that it writes nothing), the word's continuity across opens, the slow
//! path's `DIRTY` append in crash images (owner's decision (a)), its
//! fallbacks, and `rdb.ixe`'s torn tail.

use std::path::Path;
use std::sync::Arc as StdArc;

use parking_lot::Mutex;

use super::*;
use crate::core::index_store::gate;
use crate::core::index_store::marker::{MarkerRecord, encode_record, new_log, scan};
use crate::core::power_loss::failpoint::{self, Outcome};
use crate::core::power_loss::image::{CrashPoint, Recorder};

const INC: u128 = 0x5eed_0000_0000_0000_0000_0000_0000_0b11;

fn clean_log(incarnation: u128) -> Vec<u8> {
    let mut log = new_log(incarnation);
    log.extend_from_slice(&encode_record(1, &MarkerRecord::Clean(dummy_clean(incarnation))));
    log
}

fn sync_file(path: &Path) {
    crate::core::sync_audit::note("file.data", path);
    std::fs::File::options().write(true).open(path).unwrap().sync_data().unwrap();
}

/// Every row of the module table, and an open writes nothing: no byte of the
/// log changes, no sync is issued, nothing is created.
#[test]
fn the_state_at_open_is_the_logs_and_an_open_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut with_trailing = clean_log(INC);
    with_trailing.extend_from_slice(&[0xab; 9]);
    let mut clean_then_dirty = clean_log(INC);
    clean_then_dirty.extend_from_slice(&encode_record(2, &MarkerRecord::Dirty));
    let cases: Vec<(&str, Option<Vec<u8>>, GateState)> = vec![
        ("none", None, GateState::Dirty),
        ("header", Some(new_log(INC)), GateState::Dirty),
        ("clean", Some(clean_log(INC)), GateState::Clean),
        ("clean_trailing", Some(with_trailing), GateState::Clean),
        ("clean_dirty", Some(clean_then_dirty), GateState::Dirty),
        ("foreign", Some(clean_log(INC + 1)), GateState::Dirty),
        ("garbage", Some(vec![7u8; 40]), GateState::Unknown),
    ];
    let watch = crate::core::sync_audit::watch(dir.path());
    for (stem, bytes, want) in &cases {
        let path = dir.path().join(format!("{stem}.ixm"));
        if let Some(b) = bytes {
            std::fs::write(&path, b).unwrap();
        }
        let before = std::fs::read(&path).ok();
        let g = FileGate::open(Some(dir.path().to_path_buf()), stem, INC);
        assert_eq!(g.state(), *want, "{stem}");
        assert_eq!(std::fs::read(&path).ok(), before, "{stem}: the open changed the log");
    }
    // A log that cannot be read at all proves nothing.
    std::fs::create_dir(dir.path().join("eisdir.ixm")).unwrap();
    assert_eq!(
        FileGate::open(Some(dir.path().to_path_buf()), "eisdir", INC).state(),
        GateState::Unknown
    );
    assert!(watch.take().is_empty(), "an open issued a sync");
    let names: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".tmp") || n.ends_with(".ixs"))
        .collect();
    assert!(names.is_empty(), "an open created {names:?}");
    // A table on no directory: detached, Dirty, no files.
    let detached = FileGate::open(None, "anything", INC);
    assert!(!detached.has_files());
    assert_eq!(detached.state(), GateState::Dirty);
}

/// Only the first open of a file set derives the word; a later open while it
/// is alive (a reload) gets the same word, count included. Another
/// incarnation of the name is another file set.
#[tokio::test]
async fn a_later_open_of_a_file_set_keeps_its_word_and_count() {
    let dir = tempfile::tempdir().unwrap();
    let d = Some(dir.path().to_path_buf());
    let first = FileGate::open(d.clone(), "t", INC);
    let held = gate::ticket(&first, None).await.unwrap();
    assert_eq!(first.word().load().count(), 1);
    // The log now says Clean; a reopen must not re-derive it while the word
    // lives.
    std::fs::write(dir.path().join("t.ixm"), clean_log(INC)).unwrap();
    let again = FileGate::open(d.clone(), "t", INC);
    assert!(StdArc::ptr_eq(&first, &again), "a reload derived a second word");
    assert_eq!(again.word().load().count(), 1, "the reload lost the count");
    let other = FileGate::open(d.clone(), "t", INC + 1);
    assert!(!StdArc::ptr_eq(&first, &other));
    assert_eq!(other.state(), GateState::Dirty, "another incarnation's log is foreign");
    drop((held, first, again));
    assert_eq!(FileGate::open(d, "t", INC).state(), GateState::Clean, "a first open reads the log");
}

/// The images a crash point yields under many seeds.
fn images(points: &[CrashPoint], seeds: u64) -> Vec<(String, std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>)> {
    let mut out = Vec::new();
    for p in points {
        for seed in 0..seeds {
            out.push((format!("{} seed {seed}", p.label), p.image(seed)));
        }
    }
    out
}

fn arm_cuts(rec: &StdArc<Recorder>, name: &str, scope: &Path, hits: u64, sink: &StdArc<Mutex<Vec<Result<CrashPoint, String>>>>) -> Vec<failpoint::Guard> {
    (1..=hits)
        .map(|nth| failpoint::arm(name, scope, nth, rec.cut_action(format!("{name} #{nth}"), sink.clone())))
        .collect()
}

/// Owner's decision (a): the append is pwrite, fdatasync, ftruncate,
/// fdatasync, nothing issued in between. The log here once read `HEADER`,
/// `CLEAN`, `DIRTY`, `CLEAN` and its `DIRTY` rotted, so the open sees `CLEAN`
/// last with bytes after it. The new `DIRTY` lands where the rotten one was,
/// and until the truncate is durable the stale second `CLEAN` right after it
/// reads as its successor. So in no image may a log that proves a snapshot
/// sit beside a data file that changed.
#[tokio::test(flavor = "multi_thread")]
async fn the_dirty_append_order_holds_in_every_crash_image() {
    let dir = tempfile::tempdir().unwrap();
    let ixm = dir.path().join("t.ixm");
    let data = dir.path().join("t.db");
    let mut log = clean_log(INC);
    let dirty_at = log.len();
    log.extend_from_slice(&encode_record(2, &MarkerRecord::Dirty));
    log.extend_from_slice(&encode_record(3, &MarkerRecord::Clean(dummy_clean(INC))));
    log[dirty_at + 5] ^= 0x40; // the DIRTY's checksum rots
    std::fs::write(&ixm, &log).unwrap();
    let baseline = vec![b'A'; 6000];
    std::fs::write(&data, &baseline).unwrap();
    assert_eq!(scan(&log).unwrap().proven_clean(), None, "the rotten log proves nothing");

    let rec = Recorder::start(dir.path());
    let gate_ = FileGate::open(Some(dir.path().to_path_buf()), "t", INC);
    assert_eq!(gate_.state(), GateState::Clean);
    let sink = StdArc::new(Mutex::new(Vec::new()));
    let _g1 = arm_cuts(&rec, "sync:index_store.marker", dir.path(), 2, &sink);
    let _g2 = arm_cuts(&rec, "sync:file.data", dir.path(), 1, &sink);

    let ticket = gate::ticket(&gate_, None).await.unwrap();
    // The first write the ticket admits.
    std::fs::write(&data, vec![b'B'; 6000]).unwrap();
    sync_file(&data);
    drop(ticket);
    let mut points: Vec<CrashPoint> = sink.lock().drain(..).filter_map(Result::ok).collect();
    points.push(rec.cut_quiescent("after the write").unwrap());
    assert!(points.len() >= 3, "a cut was lost");

    let mut changed = 0;
    for (label, img) in images(&points, 48) {
        let log = img.get(Path::new("t.ixm")).cloned().unwrap_or_default();
        let data_now = img.get(Path::new("t.db")).cloned().unwrap_or_default();
        if data_now != baseline {
            changed += 1;
        }
        if let Ok(s) = scan(&log)
            && s.proven_clean().is_some()
        {
            assert_eq!(data_now, baseline, "{label}: a log proving a snapshot beside changed data");
        }
    }
    assert!(changed > 0, "no image held the write; the test proves nothing");
    let end = std::fs::read(&ixm).unwrap();
    let s = scan(&end).unwrap();
    assert_eq!(s.state(), marker::LogState::Dirty);
    assert_eq!(s.trailing(), 0, "the stale CLEAN was not cut off");
}

/// T1 on a plain log: cuts around `DIRTY`'s fdatasync leave either no write
/// or a durable `DIRTY`, never a write beside a log still ending `CLEAN`.
#[tokio::test(flavor = "multi_thread")]
async fn cuts_around_dirty_leave_no_write_or_a_durable_dirty() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("t.ixm"), clean_log(INC)).unwrap();
    let data = dir.path().join("t.db");
    let baseline = vec![b'A'; 9000];
    std::fs::write(&data, &baseline).unwrap();
    let rec = Recorder::start(dir.path());
    let gate_ = FileGate::open(Some(dir.path().to_path_buf()), "t", INC);
    let sink = StdArc::new(Mutex::new(Vec::new()));
    let _a = arm_cuts(&rec, "dirty_append_before_fsync", dir.path(), 1, &sink);
    let _b = arm_cuts(&rec, "sync:file.data", dir.path(), 1, &sink);
    let ticket = gate::ticket(&gate_, None).await.unwrap();
    std::fs::write(&data, vec![b'C'; 9000]).unwrap();
    sync_file(&data);
    drop(ticket);
    let mut points: Vec<CrashPoint> = sink.lock().drain(..).filter_map(Result::ok).collect();
    points.push(rec.cut_quiescent("after").unwrap());
    assert_eq!(points.len(), 3);
    let mut changed = 0;
    for (label, img) in images(&points, 48) {
        let log = img.get(Path::new("t.ixm")).cloned().unwrap_or_default();
        let data_now = img.get(Path::new("t.db")).cloned().unwrap_or_default();
        let still_clean = scan(&log).map(|s| s.state() == marker::LogState::Clean).unwrap_or(false);
        if data_now != baseline {
            changed += 1;
            assert!(!still_clean, "{label}: a write survived beside a log still ending CLEAN");
        }
    }
    assert!(changed > 0, "no image held the write");
}

/// `DIRTY` that cannot be made durable unlinks `.ixs` instead, and the write
/// proceeds; when that fails too the ticket fails and the state stays.
#[tokio::test]
async fn a_dirty_that_cannot_be_made_durable_unlinks_the_snapshot_or_fails() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("t.ixm"), clean_log(INC)).unwrap();
    std::fs::write(dir.path().join("t.ixs"), b"snapshot").unwrap();
    let g = FileGate::open(Some(dir.path().to_path_buf()), "t", INC);
    let enospc = || -> failpoint::Action {
        StdArc::new(|_: &failpoint::Hit<'_>| Outcome::Fail(std::io::Error::from_raw_os_error(28)))
    };
    {
        let _f = failpoint::arm("dirty_enospc_after_first_write", dir.path(), 1, enospc());
        let t = gate::ticket(&g, None).await.expect("the unlink fallback lets the write proceed");
        drop(t);
    }
    assert!(!dir.path().join("t.ixs").exists(), "the snapshot was left beside an undurable DIRTY");
    assert_eq!(g.state(), GateState::Dirty);

    // Now the unlink fails too: `.ixs` is a non-empty directory.
    std::fs::write(dir.path().join("u.ixm"), clean_log(INC)).unwrap();
    std::fs::create_dir(dir.path().join("u.ixs")).unwrap();
    std::fs::write(dir.path().join("u.ixs").join("x"), b"x").unwrap();
    let u = FileGate::open(Some(dir.path().to_path_buf()), "u", INC);
    let _f = failpoint::arm("dirty_enospc_after_first_write", dir.path(), 1, enospc());
    assert!(gate::ticket(&u, None).await.is_err(), "a ticket with nothing durable");
    assert_eq!(u.state(), GateState::Clean, "the state moved with nothing durable");
    assert_eq!(u.word().load().count(), 0);
}

/// `Unknown` trusts none of the log: the first ticket rewrites it as
/// `HEADER` + `DIRTY` (setting the index-store flag first) before it exists.
#[tokio::test]
async fn unknown_rewrites_the_log_before_the_first_ticket() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("t.ixm"), vec![9u8; 64]).unwrap();
    let g = FileGate::open(Some(dir.path().to_path_buf()), "t", INC);
    assert_eq!(g.state(), GateState::Unknown);
    let t = gate::ticket(&g, None).await.unwrap();
    assert_eq!(std::fs::read(dir.path().join("t.ixm")).unwrap(), marker::compacted_log(INC));
    assert!(!dir.path().join("t.ixm.tmp").exists());
    assert_eq!(g.state(), GateState::Dirty);
    #[cfg(feature = "persistent_indexes")]
    assert_eq!(
        crate::core::database::incompat::flags(dir.path()).unwrap(),
        vec![crate::core::database::incompat::INDEX_STORE_V1.to_string()]
    );
    drop(t);
}

/// Owner's decision (b): `rdb.ixe` is a fixed-record file (header 16,
/// stride 32), cut back to whole records before replay, and never below its
/// durable floor.
#[test]
fn the_epoch_log_has_a_torn_tail_cut_with_a_durable_floor() {
    use crate::core::index_store::epoch_log::{EpochRecord, HEADER_LEN, RECORD_LEN, decode, header};
    use crate::core::torn_tail::{FileIdentity, Floors, classify, cut_torn_tails};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(EPOCH_LOG_FILE);
    let mut file = header().to_vec();
    for e in 1..=2 {
        file.extend_from_slice(&EpochRecord { incarnation: INC, pid: NO_PID, slot_epoch: e }.encode());
    }
    let whole = file.len();
    file.extend_from_slice(&EpochRecord { incarnation: INC, pid: NO_PID, slot_epoch: 3 }.encode()[..11]);
    std::fs::write(&path, &file).unwrap();
    let kind = classify(dir.path(), EPOCH_LOG_FILE).expect("rdb.ixe is a fixed-record file");
    assert_eq!((kind.header, kind.stride), (HEADER_LEN as u64, RECORD_LEN as u64));
    assert!(decode(&std::fs::read(&path).unwrap()).is_err(), "a torn record decodes");
    let cut = cut_torn_tails(dir.path(), &Floors::default()).unwrap();
    assert_eq!(cut.len(), 1);
    assert_eq!(std::fs::metadata(&path).unwrap().len() as usize, whole);
    assert_eq!(decode(&std::fs::read(&path).unwrap()).unwrap().epoch_of(INC, NO_PID), Some(2));

    // A cut that would go below the floor a checkpoint recorded is refused.
    let mut short = file[..whole - RECORD_LEN].to_vec();
    short.extend_from_slice(&[1u8; 5]);
    std::fs::write(&path, &short).unwrap();
    let mut floors = Floors::default();
    floors.set(FileIdentity::of(&std::fs::metadata(&path).unwrap()), EPOCH_LOG_FILE, whole as u64);
    assert!(cut_torn_tails(dir.path(), &floors).is_err(), "a cut below the durable floor");
}
