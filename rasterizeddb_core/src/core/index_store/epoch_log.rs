//! The `rdb.ixe` slot-epoch log's codec (docs/persistent-indexes.md §1, §2
//! "Deferred work", §6): one per directory, WAL'd, recording each table's
//! or partition's slot epoch, which a VACUUM FULL bumps durably before its
//! first effect.
//!
//! A snapshot records the epoch it was taken at, and an open trusts it only
//! if that equals the WAL-recovered record here: point-in-time replay, a
//! reseed, or a renumbering the marker log never saw all leave them unequal.
//!
//! Pure: this encodes the header and records and decodes a recovered file.
//! The WAL'd appends, and adding the file to the torn-tail cut (header 16,
//! stride 32), are B11's.
//!
//! # Layout, version 1 (little-endian)
//!
//! ```text
//! header (16):  magic "RZIXEPC1" ‖ format_ver u32 ‖ crc32c u32 (of the 12 bytes before it)
//! record (32):  incarnation u128 ‖ pid u32 ‖ slot_epoch u64 ‖ crc32c u32 (of the 28 bytes before it)
//! ```
//!
//! Records are appended; the last for a key wins, and each key's epochs must
//! rise strictly in file order. A plain table's records carry [`NO_PID`].
//! Unlike the marker log nothing is read past a bad record: the file is
//! WAL'd, so a bad record or a partial one after the torn-tail cut is damage,
//! and an epoch that cannot be proven proves no snapshot.

use std::collections::BTreeMap;

use super::codec::{FormatError, Reader, crc32c};

/// The file, in each directory that holds tables.
pub const EPOCH_LOG_FILE: &str = "rdb.ixe";
pub const EPOCH_LOG_MAGIC: [u8; 8] = *b"RZIXEPC1";
pub const EPOCH_LOG_FORMAT_VER: u32 = 1;
pub const HEADER_LEN: usize = 16;
pub const RECORD_LEN: usize = 32;
/// The pid of a plain table's record. Partition ids never reach it.
pub const NO_PID: u32 = u32::MAX;

const WHAT: &str = "slot-epoch log";

/// One record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EpochRecord {
    pub incarnation: u128,
    pub pid: u32,
    pub slot_epoch: u64,
}

impl EpochRecord {
    pub fn encode(&self) -> [u8; RECORD_LEN] {
        let mut out = [0u8; RECORD_LEN];
        out[..16].copy_from_slice(&self.incarnation.to_le_bytes());
        out[16..20].copy_from_slice(&self.pid.to_le_bytes());
        out[20..28].copy_from_slice(&self.slot_epoch.to_le_bytes());
        let crc = crc32c(&out[..28]);
        out[28..].copy_from_slice(&crc.to_le_bytes());
        out
    }
}

/// The file's header.
pub fn header() -> [u8; HEADER_LEN] {
    let mut out = [0u8; HEADER_LEN];
    out[..8].copy_from_slice(&EPOCH_LOG_MAGIC);
    out[8..12].copy_from_slice(&EPOCH_LOG_FORMAT_VER.to_le_bytes());
    let crc = crc32c(&out[..12]);
    out[12..].copy_from_slice(&crc.to_le_bytes());
    out
}

/// A decoded log: the latest epoch per (incarnation, pid).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EpochLog {
    latest: BTreeMap<(u128, u32), u64>,
    records: usize,
}

impl EpochLog {
    /// The epoch recorded for a table or partition; `None` if it never
    /// renumbered.
    pub fn epoch_of(&self, incarnation: u128, pid: u32) -> Option<u64> {
        self.latest.get(&(incarnation, pid)).copied()
    }

    /// The record a renumbering appends: one above the current epoch, or 1.
    pub fn bump(&self, incarnation: u128, pid: u32) -> EpochRecord {
        EpochRecord {
            incarnation,
            pid,
            slot_epoch: self.epoch_of(incarnation, pid).map_or(1, |e| e + 1),
        }
    }

    /// Records in the file, superseded ones included.
    pub fn records(&self) -> usize {
        self.records
    }

    /// The keys with a record.
    pub fn keys(&self) -> usize {
        self.latest.len()
    }

    /// The file rewritten with only each key's latest record, in key order.
    pub fn compacted(&self) -> Vec<u8> {
        let mut out = header().to_vec();
        for (&(incarnation, pid), &slot_epoch) in &self.latest {
            out.extend_from_slice(
                &EpochRecord {
                    incarnation,
                    pid,
                    slot_epoch,
                }
                .encode(),
            );
        }
        out
    }
}

/// Decodes a recovered `rdb.ixe`. An empty file is an empty log; anything
/// else must be a good header and whole good records whose epochs rise per
/// key.
pub fn decode(bytes: &[u8]) -> Result<EpochLog, FormatError> {
    let mut log = EpochLog::default();
    if bytes.is_empty() {
        return Ok(log);
    }
    let mut r = Reader::new(bytes, WHAT);
    let head = r.array::<HEADER_LEN>()?;
    if head[..8] != EPOCH_LOG_MAGIC {
        return Err(FormatError::BadMagic { what: WHAT });
    }
    if crc32c(&head[..12]) != u32::from_le_bytes(head[12..].try_into().expect("four bytes")) {
        return Err(FormatError::BadChecksum { what: WHAT, at: 0 });
    }
    let ver = u32::from_le_bytes(head[8..12].try_into().expect("four bytes"));
    if ver != EPOCH_LOG_FORMAT_VER {
        return Err(FormatError::UnsupportedVersion { what: WHAT, found: ver });
    }
    while r.remaining() > 0 {
        let at = r.pos();
        let rec = r.array::<RECORD_LEN>()?;
        if crc32c(&rec[..28]) != u32::from_le_bytes(rec[28..].try_into().expect("four bytes")) {
            return Err(FormatError::BadChecksum { what: WHAT, at });
        }
        let incarnation = u128::from_le_bytes(rec[..16].try_into().expect("sixteen bytes"));
        let pid = u32::from_le_bytes(rec[16..20].try_into().expect("four bytes"));
        let slot_epoch = u64::from_le_bytes(rec[20..28].try_into().expect("eight bytes"));
        let prev = log.latest.insert((incarnation, pid), slot_epoch);
        if prev.is_some_and(|p| p >= slot_epoch) {
            return Err(FormatError::Malformed {
                what: WHAT,
                at,
                why: "a slot epoch that does not rise",
            });
        }
        log.records += 1;
    }
    Ok(log)
}

#[cfg(test)]
mod tests {
    use super::super::codec::{digest_hex, from_hex, to_hex};
    use super::*;
    use crate::core::index::vectors::{HistoryRow, check_history};

    const INC: u128 = 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210;

    fn sample_records() -> [EpochRecord; 3] {
        [
            EpochRecord {
                incarnation: INC,
                pid: 3,
                slot_epoch: 1,
            },
            EpochRecord {
                incarnation: INC + 1,
                pid: NO_PID,
                slot_epoch: 7,
            },
            EpochRecord {
                incarnation: INC,
                pid: 3,
                slot_epoch: 2,
            },
        ]
    }

    fn sample_file() -> Vec<u8> {
        let mut out = header().to_vec();
        for r in sample_records() {
            out.extend_from_slice(&r.encode());
        }
        out
    }

    /// The header and [`sample_records`] under `EPOCH_LOG_FORMAT_VER` 1,
    /// frozen.
    const LOG_V1: &str = "525a49584550433101000000185290ee1032547698badcfeefcdab8967452301\
        030000000100000000000000644eb1ca1132547698badcfeefcdab8967452301\
        ffffffff0700000000000000818bcc5c1032547698badcfeefcdab8967452301\
        0300000002000000000000000dc9f511";

    fn v1_digest() -> u64 {
        digest_hex(&[LOG_V1])
    }

    /// `EPOCH_LOG_FORMAT_VER`'s frozen vectors, oldest first.
    const HISTORY: &[HistoryRow] = &[(1, v1_digest, 0x6e9a_a590_5f8d_3869)];

    #[test]
    fn the_log_encodes_to_its_frozen_bytes() {
        assert_eq!(to_hex(&sample_file()), to_hex(&from_hex(LOG_V1)), "the rdb.ixe layout moved");
    }

    #[test]
    fn epoch_log_vectors_are_pinned_to_epoch_log_format_ver() {
        check_history("epoch_log::EPOCH_LOG_FORMAT_VER", HISTORY, EPOCH_LOG_FORMAT_VER);
    }

    #[test]
    fn the_latest_record_per_key_wins() {
        let log = decode(&sample_file()).unwrap();
        assert_eq!(log.epoch_of(INC, 3), Some(2));
        assert_eq!(log.epoch_of(INC + 1, NO_PID), Some(7));
        assert_eq!(log.epoch_of(INC, 4), None);
        assert_eq!((log.records(), log.keys()), (3, 2));
        assert_eq!(log.bump(INC, 3).slot_epoch, 3);
        assert_eq!(log.bump(INC, 9).slot_epoch, 1);
        let compact = decode(&log.compacted()).unwrap();
        assert_eq!((compact.records(), compact.keys()), (2, 2));
        assert_eq!(compact.epoch_of(INC, 3), Some(2));
        assert_eq!(decode(&[]).unwrap(), EpochLog::default());
        assert_eq!(decode(&header()).unwrap().keys(), 0);
    }

    #[test]
    fn an_epoch_that_does_not_rise_is_refused() {
        let mut file = sample_file();
        file.extend_from_slice(
            &EpochRecord {
                incarnation: INC,
                pid: 3,
                slot_epoch: 2,
            }
            .encode(),
        );
        assert!(matches!(decode(&file), Err(FormatError::Malformed { .. })));
    }

    #[test]
    fn every_truncation_and_every_corrupted_byte_is_an_error() {
        let file = sample_file();
        for n in 1..file.len() {
            if n == HEADER_LEN + RECORD_LEN || n == HEADER_LEN + 2 * RECORD_LEN || n == HEADER_LEN {
                // A whole-record prefix is a valid, shorter log.
                assert!(decode(&file[..n]).is_ok(), "{n} bytes");
                continue;
            }
            assert!(decode(&file[..n]).is_err(), "{n} bytes");
        }
        for i in 0..file.len() {
            for flip in [0x01u8, 0x80, 0xff] {
                let mut bad = file.clone();
                bad[i] ^= flip;
                assert!(decode(&bad).is_err(), "byte {i} ^ {flip:#x} was accepted");
            }
        }
    }

    #[test]
    fn arbitrary_bytes_never_panic() {
        let mut rng = fastrand::Rng::with_seed(0xe90c);
        for round in 0..20_000 {
            let mut b: Vec<u8> = (0..rng.usize(0..200)).map(|_| rng.u8(..)).collect();
            if round % 2 == 0 {
                let mut h = header().to_vec();
                h.append(&mut b);
                b = h;
            }
            let _ = decode(&b);
        }
    }
}
