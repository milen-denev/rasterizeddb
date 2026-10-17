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
