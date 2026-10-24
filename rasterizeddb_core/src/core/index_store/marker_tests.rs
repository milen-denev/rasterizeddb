//! The marker log codec's tests: frozen bytes, the read-to-first-bad-frame
//! rule at every truncation and every corrupted byte, "proven clean only
//! with nothing after the `CLEAN`", and the append plan's "never shrinks to
//! end at `CLEAN`" as a property over every intermediate state.

use super::super::codec::{digest_hex, from_hex, to_hex};
use super::super::identity::sample;
use super::*;
use crate::core::index::vectors::{HistoryRow, check_history};

fn clean() -> CleanRecord {
    CleanRecord {
        identity: sample(),
        snapshot_len: 123_456,
        footer: 0x1122_3344_5566_7788,
    }
}

/// `HEADER(sample incarnation)`, `DIRTY`, `CLEAN(sample, 123456,
/// 0x1122334455667788)` under `MARKER_FORMAT_VER` 1, frozen.
const LOG_V1: &str = "2500000027f63dab010000000000000000525a49584d524b3101000000103254\
    7698badcfeefcdab8967452301090000001e8f59ab0201000000000000007500\
    000078577a8403020000000000000001001032547698badcfeefcdab89674523\
    0107000000000000009058170000000000d0cc5f0300000000c0388b17000000\
    0001000000010000000100000002000000020000000100000005000df0fecaef\
    beadde78563412dec0ad0b40e20100000000008877665544332211";

fn v1_digest() -> u64 {
    digest_hex(&[LOG_V1])
}

/// `MARKER_FORMAT_VER`'s frozen vectors, oldest first.
const HISTORY: &[HistoryRow] = &[(1, v1_digest, 0xc6ee_d6f2_20ff_05b0)];

fn golden() -> Vec<u8> {
    let mut log = new_log(sample().incarnation);
    log.extend_from_slice(&encode_record(1, &MarkerRecord::Dirty));
    log.extend_from_slice(&encode_record(2, &MarkerRecord::Clean(clean())));
    log
}

/// Where each record of `log` starts, and its end.
fn frame_bounds(log: &[u8]) -> Vec<(usize, usize)> {
    let s = scan(log).unwrap();
    let mut out: Vec<(usize, usize)> = Vec::new();
    for (i, r) in s.records.iter().enumerate() {
        let end = s.records.get(i + 1).map_or(s.good_end as usize, |n| n.offset as usize);
        out.push((r.offset as usize, end));
    }
    out
}

#[test]
fn the_log_encodes_to_its_frozen_bytes() {
    assert_eq!(to_hex(&golden()), to_hex(&from_hex(LOG_V1)), "the marker log layout moved");
}

#[test]
fn marker_vectors_are_pinned_to_marker_format_ver() {
    check_history("marker::MARKER_FORMAT_VER", HISTORY, MARKER_FORMAT_VER);
}

#[test]
fn a_log_reads_back_record_for_record() {
    let s = scan(&golden()).unwrap();
    assert_eq!(s.incarnation, sample().incarnation);
    let kinds: Vec<MarkerRecord> = s.records.iter().map(|r| r.record).collect();
    assert_eq!(
        kinds,
        vec![
            MarkerRecord::Header {
                format_ver: MARKER_FORMAT_VER,
                incarnation: sample().incarnation
            },
            MarkerRecord::Dirty,
            MarkerRecord::Clean(clean()),
        ]
    );
    assert_eq!(s.records.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![0, 1, 2]);
    assert_eq!(s.state(), LogState::Clean);
    assert_eq!(s.proven_clean(), Some(&clean()));
    assert_eq!(s.trailing(), 0);

    let fresh = scan(&new_log(9)).unwrap();
    assert_eq!((fresh.state(), fresh.proven_clean()), (LogState::Dirty, None));
    let compact = scan(&compacted_log(9)).unwrap();
    assert_eq!(compact.records.len(), 2);
    assert_eq!((compact.state(), compact.proven_clean()), (LogState::Dirty, None));
}

#[test]
fn a_clean_with_bytes_after_it_is_clean_but_proves_nothing() {
    let base = golden();
    // The last: a DIRTY torn one byte short (it has no body, so its frame
    // head is the whole record).
    for tail in [vec![0u8], vec![0xff; 3], encode_record(3, &MarkerRecord::Dirty)[..FRAME_HEAD - 1].to_vec()] {
        let mut log = base.clone();
        log.extend_from_slice(&tail);
        let s = scan(&log).unwrap();
        assert_eq!(s.state(), LogState::Clean, "the state is the log's, not the proof's (N21)");
        assert_eq!(s.proven_clean(), None, "{} trailing bytes", tail.len());
        assert_eq!(s.trailing(), tail.len() as u64);
    }
    // A rotted DIRTY after the CLEAN: the same.
    let mut log = base.clone();
    let mut dirty = encode_record(3, &MarkerRecord::Dirty);
    dirty[10] ^= 0x40;
    log.extend_from_slice(&dirty);
    assert_eq!(scan(&log).unwrap().proven_clean(), None);
}

#[test]
fn every_truncation_reads_the_whole_records_before_it() {
    let log = golden();
    let bounds = frame_bounds(&log);
    for n in 0..=log.len() {
        let got = scan(&log[..n]);
        if n < bounds[0].1 {
            assert!(got.is_err(), "a torn HEADER proves nothing ({n} bytes)");
            continue;
        }
        let s = got.unwrap();
        let whole = bounds.iter().filter(|(_, end)| *end <= n).count();
        assert_eq!(s.records.len(), whole, "{n} bytes");
        assert_eq!(s.good_end as usize, bounds[whole - 1].1);
        assert_eq!(s.proven_clean().is_some(), n == log.len(), "{n} bytes");
    }
}

#[test]
fn every_corrupted_byte_ends_the_log_before_its_record() {
    let log = golden();
    let bounds = frame_bounds(&log);
    for i in 0..log.len() {
        let record = bounds.iter().position(|(s, e)| (*s..*e).contains(&i)).unwrap();
        for flip in [0x01u8, 0x80, 0xff] {
            let mut bad = log.clone();
            bad[i] ^= flip;
            let got = scan(&bad);
            if record == 0 {
                assert!(got.is_err(), "byte {i} ^ {flip:#x} of the HEADER");
                continue;
            }
            let s = got.unwrap();
            assert_eq!(s.records.len(), record, "byte {i} ^ {flip:#x}");
            assert_eq!(s.proven_clean(), None, "byte {i} ^ {flip:#x}");
        }
    }
}

#[test]
fn records_out_of_sequence_or_a_second_header_end_the_log() {
    let mut log = new_log(1);
    log.extend_from_slice(&encode_record(2, &MarkerRecord::Clean(clean())));
    assert_eq!(scan(&log).unwrap().records.len(), 1, "a CLEAN that skips a sequence number");

    // A HEADER after the first, numbered as if it followed.
    let mut log = compacted_log(1);
    log.extend_from_slice(&encode_record(
        2,
        &MarkerRecord::Header {
            format_ver: MARKER_FORMAT_VER,
            incarnation: 1,
        },
    ));
    assert_eq!(scan(&log).unwrap().records.len(), 2, "a HEADER after the first");

    // Another log's CLEAN (sequence 2) after a log whose last record is 0.
    // A record whose number does follow would be read: the sequence binds a
    // record to its position, not to its log, which is why compaction
    // renames a whole new file over the old instead of rewriting in place.
    let mut log = new_log(1);
    log.extend_from_slice(&golden()[frame_bounds(&golden())[2].0..]);
    let s = scan(&log).unwrap();
    assert_eq!((s.records.len(), s.proven_clean()), (1, None));

    let mut later = new_log(1);
    later[17 + 8] = 2; // the HEADER's format version
    let crc = crc32c(&[&later[..4], &later[8..]].concat());
    later[4..8].copy_from_slice(&crc.to_le_bytes());
    assert_eq!(
        scan(&later),
        Err(FormatError::UnsupportedVersion { what: WHAT, found: 2 })
    );
}

#[test]
fn arbitrary_bytes_never_panic() {
    let mut rng = fastrand::Rng::with_seed(0x1a3);
    let header = new_log(5);
    for round in 0..20_000 {
        let len = rng.usize(0..200);
        let mut b: Vec<u8> = (0..len).map(|_| rng.u8(..)).collect();
        if round % 2 == 0 {
            // Past a good HEADER, so the frame parser sees the noise.
            let mut h = header.clone();
            h.append(&mut b);
            b = h;
        }
        if let Ok(s) = scan(&b) {
            assert!(s.good_end <= s.file_len);
            assert!(!s.records.is_empty());
        }
    }
}

/// A log that ends `…CLEAN` followed by `tail`.
fn clean_then(tail: &[u8]) -> Vec<u8> {
    let mut log = golden();
    log.extend_from_slice(tail);
    log
}

/// Every intermediate state of the append plan, over many tails after a
/// `CLEAN`: none is proven clean, the file never ends at the `CLEAN`, and
/// the state after every step is `Dirty` with the new record last.
#[test]
fn appending_after_bad_bytes_never_shrinks_the_file_to_end_at_its_clean() {
    let clean_end = golden().len() as u64;
    let mut rng = fastrand::Rng::with_seed(0xc1ea);
    let mut tails: Vec<Vec<u8>> = vec![
        vec![],
        vec![0],
        vec![0xff; FRAME_HEAD - 1],
        encode_record(3, &MarkerRecord::Dirty)[..10].to_vec(),
        encode_record(3, &MarkerRecord::Clean(clean()))[..40].to_vec(),
    ];
    // A durable DIRTY that rotted, then more bytes.
    let mut rotted = encode_record(3, &MarkerRecord::Dirty);
    rotted[12] ^= 1;
    tails.push(rotted.clone());
    rotted.extend_from_slice(&encode_record(4, &MarkerRecord::Dirty));
    tails.push(rotted);
    for _ in 0..500 {
        let len = rng.usize(1..300);
        tails.push((0..len).map(|_| rng.u8(..)).collect());
    }
    for tail in &tails {
        let mut file = clean_then(tail);
        let before = scan(&file).unwrap();
        assert_eq!(before.state(), LogState::Clean);
        let plan = before.plan_append(Append::Dirty);
        for (step, op) in plan.ops.iter().enumerate() {
            op.apply(&mut file);
            let s = scan(&file).unwrap();
            assert_eq!(s.proven_clean(), None, "step {step} of {:?}: a crash here would trust the CLEAN", plan.ops);
            assert!(file.len() as u64 > clean_end, "step {step}: the file ends at the CLEAN");
            assert_eq!(s.state(), LogState::Dirty, "step {step}");
            assert_eq!(s.records[3].record, MarkerRecord::Dirty, "step {step}: the new DIRTY is record 3");
        }
        let s = scan(&file).unwrap();
        assert_eq!(s.trailing(), 0, "the plan leaves nothing after the new record");
        assert_eq!(s.records.len(), 4);
    }
}

#[test]
fn a_clean_appended_to_a_clean_ending_log_is_proven() {
    let mut file = compacted_log(sample().incarnation);
    let plan = scan(&file).unwrap().plan_append(Append::Clean(clean()));
    assert_eq!(plan.ops.len(), 1, "nothing to cut: a plain append");
    for op in &plan.ops {
        op.apply(&mut file);
    }
    assert_eq!(scan(&file).unwrap().proven_clean(), Some(&clean()));
}

#[test]
fn an_append_round_trips_through_a_real_file() {
    use std::io::Write;
    use std::os::unix::fs::FileExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.ixm");
    {
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&clean_then(&[0xAB; 5])).unwrap();
        f.sync_all().unwrap();
    }
    let s = scan(&std::fs::read(&path).unwrap()).unwrap();
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    for op in s.plan_append(Append::Dirty).ops {
        match op {
            MarkerOp::Write { offset, bytes } => f.write_all_at(&bytes, offset).unwrap(),
            MarkerOp::Truncate { len } => f.set_len(len).unwrap(),
        }
        f.sync_data().unwrap();
    }
    let s = scan(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(s.records.last().unwrap().record, MarkerRecord::Dirty);
    assert_eq!((s.state(), s.trailing()), (LogState::Dirty, 0));
}
