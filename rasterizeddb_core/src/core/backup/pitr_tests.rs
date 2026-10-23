//! End-to-end tests for the mechanism the whole feature rests on: that a copy
//! taken while writes were in flight, plus the log segments a checkpoint would
//! otherwise have discarded, reconstructs the database exactly — and stops
//! exactly where it is told to.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::core::wal::{SEGMENT_SUFFIX, Wal, file_id_for_name};

/// Applies one committed write through the log, as an append would.
fn commit_write(wal: &Wal, file_id: u64, offset: u64, bytes: &[u8]) {
    let txn = wal.begin();
    wal.log(txn, file_id, offset, bytes).unwrap();
    wal.commit_txn(txn).unwrap();
}

/// Concatenates every staged segment in log order, which is what an archive
/// bundle holds and what a restore replays.
fn concatenate_staged(staging: &Path, out: &Path) -> u32 {
    let mut staged: Vec<PathBuf> = std::fs::read_dir(staging)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(SEGMENT_SUFFIX))
        })
        .collect();
    staged.sort();

    let mut sink = std::fs::File::create(out).unwrap();
    for path in &staged {
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        sink.write_all(&bytes).unwrap();
    }
    staged.len() as u32
}

fn resolver_for(dir: &Path, names: &[&str]) -> HashMap<u64, PathBuf> {
    names
        .iter()
        .map(|name| (file_id_for_name(name), dir.join(name)))
        .collect()
}

#[test]
fn a_checkpoint_stages_the_log_instead_of_losing_it() {
    let root = temp_testdir::TempDir::default();
    let staging = root.join("staging");
    let wal = Wal::open(root.join("rasterized.wal")).unwrap();
    wal.set_archive_dir(Some(staging.clone())).unwrap();

    let id = file_id_for_name("rows.db");
    commit_write(&wal, id, 0, b"abcd");
    wal.checkpoint().unwrap();

    let staged: Vec<_> = std::fs::read_dir(&staging).unwrap().flatten().collect();
    assert_eq!(
        staged.len(),
        1,
        "the checkpoint should have cut one segment"
    );
    assert!(
        staged[0].metadata().unwrap().len() > 0,
        "a staged segment must carry the records the checkpoint discarded"
    );

    // And the live log is empty again, which is the point of checkpointing.
    assert_eq!(wal.bytes_since_checkpoint(), 0);
}

#[test]
fn an_empty_log_is_not_staged() {
    let root = temp_testdir::TempDir::default();
    let staging = root.join("staging");
    let wal = Wal::open(root.join("rasterized.wal")).unwrap();
    wal.set_archive_dir(Some(staging.clone())).unwrap();

    wal.checkpoint().unwrap();
    wal.checkpoint().unwrap();

    // Staging an empty segment would cost an upload and a manifest rewrite to
    // say that nothing happened.
    let staged = std::fs::read_dir(&staging)
        .map(|d| d.flatten().count())
        .unwrap_or(0);
    assert_eq!(staged, 0);
}

#[test]
fn a_declined_checkpoint_stages_nothing_and_still_reports_success() {
    // `Wal::checkpoint` returns `Ok(())` when it *declines* to run, and there
    // are two places it can do so: a cheap early-out at the top, and a re-check
    // under the writer lock once a pending overwrite could have been registered
    // between the two. Staging sits after the second, so a decline there skips
    // it entirely — and the caller is told `Ok`.
    //
    // The snapshot path must therefore never infer "the window was cut" from a
    // successful call; it checks the condition instead. This pins the behaviour
    // that makes checking necessary, so a later simplification back to trusting
    // the return value fails here rather than in someone's backup.
    let root = temp_testdir::TempDir::default();
    let staging = root.join("staging");
    let wal = Wal::open(root.join("rasterized.wal")).unwrap();
    wal.set_archive_dir(Some(staging.clone())).unwrap();

    // An in-place overwrite needs a registered file to resolve against.
    let data_path = root.join("rows.db");
    std::fs::write(&data_path, vec![0u8; 64]).unwrap();
    let id = file_id_for_name("rows.db");
    wal.register_file(id, &data_path).unwrap();

    // A committed append, so the log genuinely holds something worth archiving.
    commit_write(&wal, id, 0, b"aaaa");

    // Now an in-place write whose transaction stays open: this is what makes a
    // checkpoint decline.
    let pending = wal.begin();
    wal.log_in_place(pending, id, 8, b"bbbb").unwrap();

    let before = wal.bytes_since_checkpoint();
    assert!(before > 0);

    // Reports success, and does nothing.
    wal.checkpoint()
        .expect("a declined checkpoint still reports Ok");

    assert_eq!(
        wal.bytes_since_checkpoint(),
        before,
        "a declined checkpoint must not have truncated the log"
    );
    let staged = std::fs::read_dir(&staging)
        .map(|d| d.flatten().count())
        .unwrap_or(0);
    assert_eq!(
        staged, 0,
        "a declined checkpoint must not have staged a segment"
    );

    // Once the transaction ends, the same call does the work.
    wal.commit_txn(pending).unwrap();
    wal.checkpoint().expect("checkpoint");
    assert_eq!(wal.bytes_since_checkpoint(), 0);
    assert_eq!(
        std::fs::read_dir(&staging).unwrap().flatten().count(),
        1,
        "the segment should be staged once nothing is pending"
    );
}

#[test]
fn replaying_archived_segments_reconstructs_the_file() {
    let root = temp_testdir::TempDir::default();
    let staging = root.join("staging");
    let wal = Wal::open(root.join("rasterized.wal")).unwrap();
    wal.set_archive_dir(Some(staging.clone())).unwrap();

    let id = file_id_for_name("rows.db");

    // Two checkpoints, so the history spans more than one segment and the
    // concatenation is doing real work.
    for i in 0..3u8 {
        commit_write(&wal, id, i as u64 * 4, &[i; 4]);
    }
    wal.checkpoint().unwrap();
    for i in 3..6u8 {
        commit_write(&wal, id, i as u64 * 4, &[i; 4]);
    }
    wal.checkpoint().unwrap();

    let replay = root.join("replay.wal");
    assert_eq!(concatenate_staged(&staging, &replay), 2);

    let target = root.join("restored");
    std::fs::create_dir_all(&target).unwrap();
    let resolver = resolver_for(&target, &["rows.db"]);

    let stats = Wal::recover_upto(&replay, |file| resolver.get(&file).cloned(), None).unwrap();
    assert_eq!(stats.applied, 6);

    let restored = std::fs::read(target.join("rows.db")).unwrap();
    let expected: Vec<u8> = (0..6u8).flat_map(|i| [i; 4]).collect();
    assert_eq!(restored, expected);
}

#[test]
fn recovery_stops_exactly_at_the_requested_commit() {
    let root = temp_testdir::TempDir::default();
    let staging = root.join("staging");
    let wal = Wal::open(root.join("rasterized.wal")).unwrap();
    wal.set_archive_dir(Some(staging.clone())).unwrap();

    let id = file_id_for_name("rows.db");
    for i in 0..6u8 {
        commit_write(&wal, id, i as u64 * 4, &[i; 4]);
    }
    // Six commits, so the commit sequence runs 1..=6.
    assert_eq!(wal.commit_lsn(), 6);
    wal.checkpoint().unwrap();

    let replay = root.join("replay.wal");
    concatenate_staged(&staging, &replay);

    // Stopping at LSN 4 must apply the first four writes and none of the rest.
    let target = root.join("restored");
    std::fs::create_dir_all(&target).unwrap();
    let resolver = resolver_for(&target, &["rows.db"]);

    let stats = Wal::recover_upto(&replay, |file| resolver.get(&file).cloned(), Some(4)).unwrap();
    assert_eq!(stats.applied, 4);
    assert_eq!(
        stats.rolled_back, 2,
        "writes past the ceiling are discarded, not applied"
    );

    let restored = std::fs::read(target.join("rows.db")).unwrap();
    let expected: Vec<u8> = (0..4u8).flat_map(|i| [i; 4]).collect();
    assert_eq!(restored, expected);
}

#[test]
fn a_transaction_split_across_a_checkpoint_survives_concatenation() {
    let root = temp_testdir::TempDir::default();
    let staging = root.join("staging");
    let wal = Wal::open(root.join("rasterized.wal")).unwrap();
    wal.set_archive_dir(Some(staging.clone())).unwrap();

    let id = file_id_for_name("rows.db");

    // A transaction whose data record lands before a checkpoint and whose
    // commit marker lands after it. Replayed segment-by-segment this would be
    // dropped for want of a commit; replayed over the concatenation it is
    // whole. This is why the restore path joins the segments first.
    let txn = wal.begin();
    wal.log(txn, id, 0, b"WXYZ").unwrap();
    wal.checkpoint().unwrap();
    wal.commit_txn(txn).unwrap();
    wal.checkpoint().unwrap();

    let replay = root.join("replay.wal");
    assert_eq!(concatenate_staged(&staging, &replay), 2);

    let target = root.join("restored");
    std::fs::create_dir_all(&target).unwrap();
    let resolver = resolver_for(&target, &["rows.db"]);

    let stats = Wal::recover_upto(&replay, |file| resolver.get(&file).cloned(), None).unwrap();
    assert_eq!(stats.applied, 1);
    assert_eq!(std::fs::read(target.join("rows.db")).unwrap(), b"WXYZ");
}

#[test]
fn a_fuzzy_copy_is_made_consistent_by_replay() {
    // The central claim: a snapshot may be taken while writes are in flight,
    // because the archived log fixes whatever the copy caught mid-flight.
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();

    let staging = root.join("staging");
    let wal = Wal::open(live.join("rasterized.wal")).unwrap();
    wal.set_archive_dir(Some(staging.clone())).unwrap();

    let data_path = live.join("rows.db");
    let id = file_id_for_name("rows.db");

    // Writes that landed before the snapshot started.
    let mut data = std::fs::File::create(&data_path).unwrap();
    data.write_all(&[0u8; 8]).unwrap();
    data.sync_all().unwrap();
    wal.checkpoint().unwrap();

    // The snapshot copies the file here — catching only those 8 bytes.
    let snapshot_copy = root.join("snapshot-rows.db");
    std::fs::copy(&data_path, &snapshot_copy).unwrap();

    // Writes that happen *while* the copy is being taken. They reach the live
    // file and the log, but the copy already went out.
    for i in 1..4u8 {
        let offset = i as u64 * 8;
        commit_write(&wal, id, offset, &[i; 8]);
        let mut data = std::fs::OpenOptions::new()
            .write(true)
            .open(&data_path)
            .unwrap();
        use std::io::Seek;
        data.seek(std::io::SeekFrom::Start(offset)).unwrap();
        data.write_all(&[i; 8]).unwrap();
    }

    // The snapshot finishes, and the checkpoint cuts the window's log.
    wal.checkpoint().unwrap();

    // Restore: unpack the fuzzy copy, then replay.
    let target = root.join("restored");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::copy(&snapshot_copy, target.join("rows.db")).unwrap();

    let stale = std::fs::read(target.join("rows.db")).unwrap();
    assert_eq!(stale.len(), 8, "the copy really was taken mid-flight");

    let replay = root.join("replay.wal");
    concatenate_staged(&staging, &replay);
    let resolver = resolver_for(&target, &["rows.db"]);
    Wal::recover_upto(&replay, |file| resolver.get(&file).cloned(), None).unwrap();

    assert_eq!(
        std::fs::read(target.join("rows.db")).unwrap(),
        std::fs::read(&data_path).unwrap(),
        "after replay the restored file must match the live one byte for byte"
    );
}

#[test]
fn a_segment_sequence_resumes_past_what_is_already_staged() {
    let root = temp_testdir::TempDir::default();
    let staging = root.join("staging");
    let id = file_id_for_name("rows.db");

    {
        let wal = Wal::open(root.join("rasterized.wal")).unwrap();
        wal.set_archive_dir(Some(staging.clone())).unwrap();
        commit_write(&wal, id, 0, b"aaaa");
        wal.checkpoint().unwrap();
    }

    // A restart with segments still waiting to be uploaded must not reuse their
    // names, or the second run would overwrite the first run's records.
    {
        let wal = Wal::open(root.join("rasterized.wal")).unwrap();
        wal.set_archive_dir(Some(staging.clone())).unwrap();
        commit_write(&wal, id, 4, b"bbbb");
        wal.checkpoint().unwrap();
    }

    let staged: Vec<_> = std::fs::read_dir(&staging)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_str().unwrap().to_string())
        .collect();
    assert_eq!(staged.len(), 2, "got {staged:?}");
}

#[test]
fn the_archive_survives_a_compression_round_trip() {
    // The snapshot object is an archive inside a zstd frame; this is that whole
    // pipeline end to end, including the long-range settings production uses.
    let root = temp_testdir::TempDir::default();
    let source = root.join("src");
    std::fs::create_dir_all(&source).unwrap();

    // Highly repetitive, like real database files: it also confirms the
    // configured compressor actually finds those repeats.
    let repetitive: Vec<u8> = (0..2_000_000).map(|i| (i % 251) as u8).collect();
    std::fs::write(source.join("rows.db"), &repetitive).unwrap();
    std::fs::write(source.join("schema.db"), b"schema bytes").unwrap();
    std::fs::write(source.join("empty.db"), b"").unwrap();

    let mut compressed = Vec::new();
    {
        let mut encoder = super::new_encoder(&mut compressed, 12, 1, true).unwrap();
        let mut writer = super::archive::ArchiveWriter::new(&mut encoder, 3).unwrap();
        for name in ["empty.db", "rows.db", "schema.db"] {
            writer.add_file(name, &source.join(name)).unwrap();
        }
        writer.finish().unwrap();
        encoder.finish().unwrap();
    }

    assert!(
        compressed.len() < repetitive.len() / 4,
        "repetitive database bytes should compress hard, got {} from {}",
        compressed.len(),
        repetitive.len()
    );

    let target = root.join("out");
    let mut decoder = zstd::stream::read::Decoder::new(compressed.as_slice()).unwrap();
    let entries = super::archive::unpack(&mut decoder, &target).unwrap();

    assert_eq!(entries.len(), 3);
    assert_eq!(std::fs::read(target.join("rows.db")).unwrap(), repetitive);
    assert_eq!(
        std::fs::read(target.join("schema.db")).unwrap(),
        b"schema bytes"
    );
    assert_eq!(std::fs::read(target.join("empty.db")).unwrap().len(), 0);
}

#[test]
fn scan_reports_the_commit_range_without_replaying() {
    let root = temp_testdir::TempDir::default();
    let staging = root.join("staging");
    let wal = Wal::open(root.join("rasterized.wal")).unwrap();
    wal.set_archive_dir(Some(staging.clone())).unwrap();

    let id = file_id_for_name("rows.db");
    for i in 0..5u8 {
        commit_write(&wal, id, i as u64 * 4, &[i; 4]);
    }
    wal.checkpoint().unwrap();

    let replay = root.join("replay.wal");
    concatenate_staged(&staging, &replay);
    let bytes = std::fs::read(&replay).unwrap();

    let (first, last, commits) = crate::core::wal::scan_commit_range(&bytes);
    assert_eq!((first, last, commits), (1, 5, 5));
}

#[test]
fn a_torn_tail_does_not_inflate_the_reported_range() {
    let root = temp_testdir::TempDir::default();
    let staging = root.join("staging");
    let wal = Wal::open(root.join("rasterized.wal")).unwrap();
    wal.set_archive_dir(Some(staging.clone())).unwrap();

    let id = file_id_for_name("rows.db");
    for i in 0..3u8 {
        commit_write(&wal, id, i as u64 * 4, &[i; 4]);
    }
    wal.checkpoint().unwrap();

    let replay = root.join("replay.wal");
    concatenate_staged(&staging, &replay);
    let mut bytes = std::fs::read(&replay).unwrap();

    // Corrupt the middle: the scan must stop there rather than trusting
    // whatever follows.
    let midpoint = bytes.len() / 2;
    bytes[midpoint] ^= 0xff;

    let (_, last, _) = crate::core::wal::scan_commit_range(&bytes);
    let (_, clean_last, _) = crate::core::wal::scan_commit_range(&std::fs::read(&replay).unwrap());
    assert!(
        last < clean_last,
        "a corrupt record must truncate the range, got {last} against {clean_last}"
    );
}

/// A table dropped and created again under its name after the snapshot. The
/// snapshot holds the old table's file; the archived log holds the barrier,
/// then the new table's records, written into a file that started empty.
/// Replay skipped the voided records but wrote the new ones over the old
/// file, so wherever the new file was shorter the old bytes (rows) came back
/// under the new table.
#[test]
fn a_restore_across_a_drop_and_recreate_holds_only_the_new_files_bytes() {
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let staging = root.join("staging");
    let wal = Wal::open(live.join("rasterized.wal")).unwrap();
    wal.set_archive_dir(Some(staging.clone())).unwrap();
    let data_path = live.join("rows.db");
    let id = file_id_for_name("rows.db");

    // The old table, durable and checkpointed before the snapshot.
    let old: &[u8] = b"OLD-ROW-1;OLD-ROW-2;OLD-ROW-3;";
    commit_write(&wal, id, 0, old);
    std::fs::write(&data_path, old).unwrap();
    wal.checkpoint().unwrap();

    // The snapshot: the commit sequence when its copy began, the copy, and
    // the cut that ends its window.
    let window_start = wal.commit_lsn();
    let snapshot_copy = root.join("snapshot-rows.db");
    std::fs::copy(&data_path, &snapshot_copy).unwrap();
    wal.checkpoint().unwrap();

    // After it: dropped, created again, one shorter row.
    wal.forget_files(&[id]).unwrap();
    std::fs::remove_file(&data_path).unwrap();
    let new: &[u8] = b"NEW-ROW-1;";
    commit_write(&wal, id, 0, new);
    std::fs::write(&data_path, new).unwrap();
    wal.checkpoint().unwrap();

    let target = root.join("restored");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::copy(&snapshot_copy, target.join("rows.db")).unwrap();
    let replay = root.join("replay.wal");
    concatenate_staged(&staging, &replay);
    super::restore::replay(&replay, &target, None, window_start).unwrap();

    assert_eq!(
        std::fs::read(target.join("rows.db")).unwrap(),
        new,
        "the restored table holds the dropped table's bytes past its own"
    );
}

/// The other side of the same rule: a file deleted and created again *before*
/// the snapshot began is the new file in the copy, and may hold bytes no
/// record describes (a rename's or a move's staged copy writes none). The
/// barrier is in the replayed window all the same, since the window starts at
/// the last bundle before the copy; emptying the file there would lose them.
#[test]
fn a_restore_keeps_a_file_the_snapshot_copied_after_its_barrier() {
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let staging = root.join("staging");
    let wal = Wal::open(live.join("rasterized.wal")).unwrap();
    wal.set_archive_dir(Some(staging.clone())).unwrap();
    let data_path = live.join("rows.db");
    let id = file_id_for_name("rows.db");

    commit_write(&wal, id, 0, b"OLD-ROW-1;OLD-ROW-2;");
    std::fs::write(&data_path, b"OLD-ROW-1;OLD-ROW-2;").unwrap();
    wal.forget_files(&[id]).unwrap();
    std::fs::remove_file(&data_path).unwrap();
    // The new file: a logged row, then bytes no record describes.
    commit_write(&wal, id, 0, b"NEW-ROW-1;");
    std::fs::write(&data_path, b"NEW-ROW-1;UNLOGGED-COPY;").unwrap();
    wal.checkpoint().unwrap();

    let window_start = wal.commit_lsn();
    let snapshot_copy = root.join("snapshot-rows.db");
    std::fs::copy(&data_path, &snapshot_copy).unwrap();
    wal.checkpoint().unwrap();

    let target = root.join("restored");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::copy(&snapshot_copy, target.join("rows.db")).unwrap();
    let replay = root.join("replay.wal");
    concatenate_staged(&staging, &replay);
    super::restore::replay(&replay, &target, None, window_start).unwrap();

    assert_eq!(
        std::fs::read(target.join("rows.db")).unwrap(),
        b"NEW-ROW-1;UNLOGGED-COPY;",
        "a file the snapshot copied after its barrier lost bytes at the restore"
    );
}

/// The emptying rule's boundary: a barrier that was the last commit before
/// the copy began is before the copy, and the copy holds the file created
/// after it, unlogged bytes included (a move back writes its staged copy with
/// no record). Only a barrier committed *after* the copy began empties.
#[test]
fn a_restore_keeps_a_file_whose_barrier_was_the_last_commit_before_the_copy() {
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let staging = root.join("staging");
    let wal = Wal::open(live.join("rasterized.wal")).unwrap();
    wal.set_archive_dir(Some(staging.clone())).unwrap();
    let data_path = live.join("rows.db");
    let id = file_id_for_name("rows.db");

    commit_write(&wal, id, 0, b"OLD-ROW-1;OLD-ROW-2;");
    std::fs::write(&data_path, b"OLD-ROW-1;OLD-ROW-2;").unwrap();
    wal.forget_files(&[id]).unwrap();
    std::fs::remove_file(&data_path).unwrap();
    // The new file, all of it written with no record, and no commit since.
    std::fs::write(&data_path, b"STAGED-COPY;").unwrap();

    // The barrier's marker carries this very sequence.
    let window_start = wal.commit_lsn();
    let snapshot_copy = root.join("snapshot-rows.db");
    std::fs::copy(&data_path, &snapshot_copy).unwrap();
    wal.checkpoint().unwrap();

    let target = root.join("restored");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::copy(&snapshot_copy, target.join("rows.db")).unwrap();
    let replay = root.join("replay.wal");
    concatenate_staged(&staging, &replay);
    super::restore::replay(&replay, &target, None, window_start).unwrap();

    assert_eq!(
        std::fs::read(target.join("rows.db")).unwrap(),
        b"STAGED-COPY;",
        "a file whose barrier was the last commit before the copy lost its bytes at the restore"
    );
}

/// The transaction-id mark a run leaves for the next. Removed to stand for a
/// database whose last run left none (written before the mark existed, or
/// the best-effort write failed), so that the next run reissues ids.
fn forget_txn_mark(wal_path: &Path) {
    let mut name = wal_path.file_name().unwrap().to_os_string();
    name.push(".txn");
    match std::fs::remove_file(wal_path.with_file_name(name)) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => panic!("{e}"),
    }
}

/// Commits one write and returns the transaction id it took.
fn commit_write_as(wal: &Wal, file_id: u64, offset: u64, bytes: &[u8]) -> u64 {
    let txn = wal.begin();
    wal.log(txn, file_id, offset, bytes).unwrap();
    wal.commit_txn(txn).unwrap();
    txn
}

/// Two runs joined end to end, the second reissuing the first's transaction
/// ids. A drop and a rename onto the name before the snapshot: the barrier
/// committed before the copy began, so the copy holds the renamed table's
/// file, whose staged bytes no record describes. The second run's commit of
/// the barrier's id, after the copy, was taken for the barrier's own, and
/// the restore emptied the file: the table came back empty.
#[test]
fn a_restore_across_runs_resolves_a_barrier_by_its_own_commit() {
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let staging = root.join("staging");
    let wal_path = live.join("rasterized.wal");
    let data_path = live.join("x.db");
    let id = file_id_for_name("x.db");
    let other = file_id_for_name("y.db");

    // The first run: x written, dropped, and y's bytes copied in as x.
    let (window_start, barrier_txn, snapshot_copy) = {
        let wal = Wal::open(&wal_path).unwrap();
        wal.set_archive_dir(Some(staging.clone())).unwrap();
        commit_write_as(&wal, id, 0, b"DROPPED-X;");
        std::fs::write(&data_path, b"DROPPED-X;").unwrap();
        let barrier_txn = wal.begin() + 1;
        wal.forget_files(&[id]).unwrap();
        std::fs::remove_file(&data_path).unwrap();
        std::fs::write(&data_path, b"RENAMED-Y-ROWS;").unwrap();
        wal.checkpoint().unwrap();

        let window_start = wal.commit_lsn();
        let snapshot_copy = root.join("snapshot-x.db");
        std::fs::copy(&data_path, &snapshot_copy).unwrap();
        wal.checkpoint().unwrap();
        (window_start, barrier_txn, snapshot_copy)
    };

    // The second run reissues ids, and commits the barrier's after the copy.
    forget_txn_mark(&wal_path);
    {
        let wal = Wal::open(&wal_path).unwrap();
        wal.set_archive_dir(Some(staging.clone())).unwrap();
        let mut reused = 0;
        let mut offset = 0;
        while reused < barrier_txn {
            reused = commit_write_as(&wal, other, offset, b"y");
            offset += 1;
        }
        assert_eq!(reused, barrier_txn, "the second run did not reissue the barrier's id");
        assert!(wal.commit_lsn() > window_start);
        wal.checkpoint().unwrap();
    }

    let target = root.join("restored");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::copy(&snapshot_copy, target.join("x.db")).unwrap();
    std::fs::write(target.join("y.db"), b"").unwrap();
    let replay = root.join("replay.wal");
    concatenate_staged(&staging, &replay);
    super::restore::replay(&replay, &target, None, window_start).unwrap();

    assert_eq!(
        std::fs::read(target.join("x.db")).unwrap(),
        b"RENAMED-Y-ROWS;",
        "a later run's commit of the barrier's id emptied the renamed table at the restore"
    );
}

/// Two runs joined end to end: a barrier whose marker failed (the drop
/// refused, the table kept) took an id the first run had committed earlier.
/// That marker precedes the barrier, so it cannot be the barrier's commit,
/// yet it counted as one: the restore emptied the live table's file and
/// voided its records.
#[test]
fn a_barrier_that_never_committed_is_not_committed_by_an_earlier_runs_marker() {
    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let staging = root.join("staging");
    let wal_path = live.join("rasterized.wal");
    let data_path = live.join("x.db");
    let id = file_id_for_name("x.db");

    let (window_start, snapshot_copy, earlier) = {
        let wal = Wal::open(&wal_path).unwrap();
        wal.set_archive_dir(Some(staging.clone())).unwrap();
        commit_write_as(&wal, id, 0, b"AAAA");
        std::fs::write(&data_path, b"AAAA").unwrap();
        wal.checkpoint().unwrap();
        let window_start = wal.commit_lsn();
        let snapshot_copy = root.join("snapshot-x.db");
        std::fs::copy(&data_path, &snapshot_copy).unwrap();
        let earlier = commit_write_as(&wal, id, 4, b"BBBB");
        wal.checkpoint().unwrap();
        (window_start, snapshot_copy, earlier)
    };

    forget_txn_mark(&wal_path);
    {
        let wal = Wal::open(&wal_path).unwrap();
        wal.set_archive_dir(Some(staging.clone())).unwrap();
        // Ids up to the one before `earlier` taken, as other work would.
        while wal.begin() + 1 < earlier {}
        // The barrier takes `earlier` and fails at its marker.
        crate::core::wal::test_hooks::FAIL_BARRIER_MARKER.with(|f| f.set(true));
        assert!(wal.forget_files(&[id]).is_err(), "the injected marker failure never fired");
        crate::core::wal::test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(false));
        let after = commit_write_as(&wal, id, 8, b"CCCC");
        assert_eq!(after, earlier + 1, "the failed barrier did not take the earlier run's id");
        wal.checkpoint().unwrap();
    }

    let target = root.join("restored");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::copy(&snapshot_copy, target.join("x.db")).unwrap();
    std::fs::write(target.join("y.db"), b"").unwrap();
    let replay = root.join("replay.wal");
    concatenate_staged(&staging, &replay);
    super::restore::replay(&replay, &target, None, window_start).unwrap();

    assert_eq!(
        std::fs::read(target.join("x.db")).unwrap(),
        b"AAAABBBBCCCC",
        "a barrier that never committed emptied the table or voided its records at the restore"
    );
}

/// A restored database must not hand out an id the source already had: the
/// rows replayed into it can carry any id the source issued up to the
/// recovery point, and a reissued one makes its new owner adopt them. The id
/// reservation is a logged write, so the replay that brings the rows also
/// brings the mark past them, even when the copy caught an older mark.
#[test]
fn a_restore_resumes_transaction_ids_past_every_one_the_window_issued() {
    use crate::core::mvcc::status::{StatusLog, read_reservation, reservation_path};
    use crate::core::mvcc::transaction::TransactionManager;

    let root = temp_testdir::TempDir::default();
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let staging = root.join("staging");
    let wal = std::sync::Arc::new(Wal::open(live.join("rasterized.wal")).unwrap());
    wal.set_archive_dir(Some(staging.clone())).unwrap();

    let status_path = live.join("db_transactions.status");
    let status = StatusLog::open(&status_path).unwrap();
    status.attach_wal(wal.clone()).unwrap();
    let manager = TransactionManager::new(status);
    manager.set_reservation_block(8);
    // Past the block the open reserved at full size, so the window below
    // crosses several small ones.
    while manager.next_id() < manager.reserved_ids_below() {
        let id = manager.begin();
        manager.abort(id);
    }
    manager.flush().unwrap();
    wal.checkpoint().unwrap();

    // The snapshot copies the status file and the mark here.
    let names = ["db_transactions.status", "db_transactions.next"];
    let copied: Vec<(String, Vec<u8>)> = names
        .iter()
        .map(|n| (n.to_string(), std::fs::read(live.join(n)).unwrap()))
        .collect();
    let copied_mark = read_reservation(&status_path).unwrap();

    // Writers during the window, none with a durable outcome: their rows
    // would be in the replayed log, their outcomes not.
    let mut highest = 0;
    for _ in 0..40 {
        highest = manager.begin();
    }
    assert!(highest >= copied_mark, "the window never passed the copied mark");
    wal.checkpoint().unwrap();

    let target = root.join("restored");
    std::fs::create_dir_all(&target).unwrap();
    for (name, bytes) in &copied {
        std::fs::write(target.join(name), bytes).unwrap();
    }
    let replay = root.join("replay.wal");
    concatenate_staged(&staging, &replay);
    let resolver = resolver_for(&target, &names);
    Wal::recover_upto(&replay, |file| resolver.get(&file).cloned(), None).unwrap();

    let restored_status = target.join("db_transactions.status");
    assert!(reservation_path(&restored_status).exists());
    let restored = TransactionManager::new(StatusLog::open(&restored_status).unwrap());
    let next = restored.begin();
    assert!(
        next > highest,
        "the restored database handed out {next}; the source had issued up to {highest}"
    );
}
