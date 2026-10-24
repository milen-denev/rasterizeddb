//! The `.ixm` marker log's codec (docs/persistent-indexes.md §1, finding 11,
//! N33): what a partition's snapshot is worth, as a log of `HEADER`, then
//! `DIRTY` and `CLEAN` records.
//!
//! Pure: this encodes records, reads a log's bytes and plans an append. The
//! I/O (raw `File`, `pwrite`, `fdatasync`, `ftruncate`, the `.ixm.tmp`
//! rename) is B11's, and B13's for `CLEAN`.
//!
//! # Records, version 1 (little-endian)
//!
//! ```text
//! len u32 ‖ crc32c u32 ‖ kind u8 ‖ rec_seq u64 ‖ body
//! ```
//!
//! - `len` counts the bytes after the checksum: `1 + 8 + body.len()`.
//! - `crc32c` covers `len` (its four bytes), `kind`, `rec_seq` and `body`, so
//!   a rotted length is caught like a rotted body.
//! - `rec_seq` is 0 for the `HEADER` and one more for each record after it.
//!   A record whose sequence does not follow its predecessor's is a bad
//!   frame: bytes from before a compaction, or another log's, never pass for
//!   this one's.
//!
//! | Kind | Body |
//! |---|---|
//! | `HEADER` (1) | `RZIXMRK1` ‖ `MARKER_FORMAT_VER` u32 ‖ incarnation u128 (28 bytes) |
//! | `DIRTY` (2) | none |
//! | `CLEAN` (3) | identity (92 bytes, [`super::identity`]) ‖ snapshot length u64 ‖ footer xxh64 u64 |
//!
//! # Reading: to the first bad frame
//!
//! [`scan`] reads records until the first frame that is short, fails its
//! checksum, has a length or body that does not fit its kind, an unknown
//! kind, a second `HEADER`, or a sequence that does not follow. A first
//! record that is not a good `HEADER` proves nothing (`Err`, which the gate
//! reads as `Unknown`). The state is the log's ([`LogState`], N21): `Clean`
//! when the last good record is `CLEAN`, `Dirty` otherwise. The snapshot is
//! **proven clean only if that `CLEAN` is last with no bytes after it**
//! ([`MarkerScan::proven_clean`]): trailing bytes may be a torn tail, or a
//! `DIRTY` that was durable, had writes after it, and then rotted.
//!
//! # Appending: the file never shrinks to end at a `CLEAN`
//!
//! Cutting the file back to the last good record and appending there would,
//! for an instant, leave the `CLEAN` last with nothing after it, and a crash
//! in that instant leaves `CLEAN` trusted over changed data. So
//! [`MarkerScan::plan_append`] writes the new record in place over the bad
//! bytes first, and only then truncates just past it: at every step some
//! bytes follow the `CLEAN`. The caller runs the plan's [`MarkerOp`]s in
//! order, with an fdatasync after each, and lets no write be issued before
//! the last one has returned: until the truncation is durable, stale records
//! past the new one could still be read as its successors.
//!
//! Compaction ([`compacted_log`], `HEADER` + `DIRTY`) is written whole to
//! `.ixm.tmp` and renamed over the log; sequences restart at 0.

use super::codec::{FormatError, Reader, crc32c};
use super::identity::{IDENTITY_LEN, Identity};

/// The marker log's format version, in its `HEADER`.
pub const MARKER_FORMAT_VER: u32 = 1;
/// The `HEADER` body's first bytes.
pub const MARKER_MAGIC: [u8; 8] = *b"RZIXMRK1";
/// `len`, `crc32c`, `kind` and `rec_seq`.
pub const FRAME_HEAD: usize = 4 + 4 + 1 + 8;

const KIND_HEADER: u8 = 1;
const KIND_DIRTY: u8 = 2;
const KIND_CLEAN: u8 = 3;
const HEADER_BODY: usize = 8 + 4 + 16;
const CLEAN_BODY: usize = IDENTITY_LEN + 8 + 8;

const WHAT: &str = "marker log";

/// What a `CLEAN` records: the snapshot it vouches for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanRecord {
    pub identity: Identity,
    /// The `.ixs` file's length.
    pub snapshot_len: u64,
    /// The `.ixs` footer's xxh64.
    pub footer: u64,
}

/// One record, decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerRecord {
    Header { format_ver: u32, incarnation: u128 },
    Dirty,
    Clean(CleanRecord),
}

/// A record that may be appended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Append {
    Dirty,
    Clean(CleanRecord),
}

impl From<Append> for MarkerRecord {
    fn from(a: Append) -> MarkerRecord {
        match a {
            Append::Dirty => MarkerRecord::Dirty,
            Append::Clean(c) => MarkerRecord::Clean(c),
        }
    }
}

/// A good record and where it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScannedRecord {
    pub seq: u64,
    pub offset: u64,
    pub record: MarkerRecord,
}

/// What the log says about the snapshot (N21).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogState {
    /// The last good record is `CLEAN`, whether or not bytes follow it.
    Clean,
    /// The last good record is `DIRTY` or `HEADER`.
    Dirty,
}

/// The encoded record `record` with sequence number `seq`.
pub fn encode_record(seq: u64, record: &MarkerRecord) -> Vec<u8> {
    let (kind, body) = match record {
        MarkerRecord::Header {
            format_ver,
            incarnation,
        } => {
            let mut b = Vec::with_capacity(HEADER_BODY);
            b.extend_from_slice(&MARKER_MAGIC);
            b.extend_from_slice(&format_ver.to_le_bytes());
            b.extend_from_slice(&incarnation.to_le_bytes());
            (KIND_HEADER, b)
        }
        MarkerRecord::Dirty => (KIND_DIRTY, Vec::new()),
        MarkerRecord::Clean(c) => {
            let mut b = Vec::with_capacity(CLEAN_BODY);
            c.identity.encode_into(&mut b);
            b.extend_from_slice(&c.snapshot_len.to_le_bytes());
            b.extend_from_slice(&c.footer.to_le_bytes());
            (KIND_CLEAN, b)
        }
    };
    let len = (1 + 8 + body.len()) as u32;
    let mut out = Vec::with_capacity(FRAME_HEAD + body.len());
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&[0; 4]);
    out.push(kind);
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&body);
    let mut covered = Vec::with_capacity(out.len() - 4);
    covered.extend_from_slice(&out[..4]);
    covered.extend_from_slice(&out[8..]);
    out[4..8].copy_from_slice(&crc32c(&covered).to_le_bytes());
    out
}

/// A new log: its `HEADER` alone.
pub fn new_log(incarnation: u128) -> Vec<u8> {
    encode_record(
        0,
        &MarkerRecord::Header {
            format_ver: MARKER_FORMAT_VER,
            incarnation,
        },
    )
}

/// A compacted log, `HEADER` + `DIRTY`: what a publish writes to `.ixm.tmp`
/// before its `CLEAN`, and the `Unknown` slow path's rewrite (§1, §2).
pub fn compacted_log(incarnation: u128) -> Vec<u8> {
    let mut out = new_log(incarnation);
    out.extend_from_slice(&encode_record(1, &MarkerRecord::Dirty));
    out
}

/// One frame at `off`, which must carry sequence `seq`.
fn frame_at(bytes: &[u8], off: usize, seq: u64) -> Result<(MarkerRecord, usize), FormatError> {
    let at = off as u64;
    let mut r = Reader::new(&bytes[off..], WHAT);
    let len = r.u32()? as usize;
    let crc = r.u32()?;
    if !(9..=9 + CLEAN_BODY.max(HEADER_BODY)).contains(&len) {
        return Err(FormatError::Malformed {
            what: WHAT,
            at,
            why: "record length out of range",
        });
    }
    let rest = r.take(len)?;
    let mut covered = Vec::with_capacity(4 + len);
    covered.extend_from_slice(&(len as u32).to_le_bytes());
    covered.extend_from_slice(rest);
    if crc32c(&covered) != crc {
        return Err(FormatError::BadChecksum { what: WHAT, at });
    }
    let mut r = Reader::new(rest, WHAT);
    let kind = r.u8()?;
    let got = r.u64()?;
    if got != seq {
        return Err(FormatError::Malformed {
            what: WHAT,
            at,
            why: "record sequence does not follow",
        });
    }
    let body = r.take(r.remaining())?;
    let bad_body = |why| FormatError::Malformed { what: WHAT, at, why };
    let record = match kind {
        KIND_HEADER if body.len() == HEADER_BODY => {
            let mut b = Reader::new(body, WHAT);
            if b.array::<8>()? != MARKER_MAGIC {
                return Err(FormatError::BadMagic { what: WHAT });
            }
            MarkerRecord::Header {
                format_ver: b.u32()?,
                incarnation: b.u128()?,
            }
        }
        KIND_DIRTY if body.is_empty() => MarkerRecord::Dirty,
        KIND_CLEAN if body.len() == CLEAN_BODY => {
            let mut b = Reader::new(body, WHAT);
            MarkerRecord::Clean(CleanRecord {
                identity: Identity::read(&mut b)?,
                snapshot_len: b.u64()?,
                footer: b.u64()?,
            })
        }
        KIND_HEADER | KIND_DIRTY | KIND_CLEAN => return Err(bad_body("body length does not fit the kind")),
        _ => return Err(bad_body("unknown record kind")),
    };
    Ok((record, off + 8 + len))
}

/// A log read to its first bad frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkerScan {
    /// The `HEADER`'s incarnation; the caller compares it with `_schema.db`'s.
    pub incarnation: u128,
    /// Every good record, the `HEADER` first.
    pub records: Vec<ScannedRecord>,
    /// The end of the last good record.
    pub good_end: u64,
    /// The bytes read.
    pub file_len: u64,
}

/// One step of an append, in order; each is followed by an fdatasync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkerOp {
    /// `pwrite` `bytes` at `offset`.
    Write { offset: u64, bytes: Vec<u8> },
    /// `ftruncate` to `len`.
    Truncate { len: u64 },
}

impl MarkerOp {
    /// Applies the step to an in-memory file: the model the tests check the
    /// plan against, and what a caller without a file can use.
    pub fn apply(&self, file: &mut Vec<u8>) {
        match self {
            MarkerOp::Write { offset, bytes } => {
                let off = *offset as usize;
                if file.len() < off + bytes.len() {
                    file.resize(off + bytes.len(), 0);
                }
                file[off..off + bytes.len()].copy_from_slice(bytes);
            }
            MarkerOp::Truncate { len } => file.truncate(*len as usize),
        }
    }
}

/// How to append one record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendPlan {
    pub ops: Vec<MarkerOp>,
}

impl MarkerScan {
    /// Bytes after the last good record.
    pub fn trailing(&self) -> u64 {
        self.file_len - self.good_end
    }

    pub fn last(&self) -> &ScannedRecord {
        self.records.last().expect("a scan always holds its HEADER")
    }

    pub fn state(&self) -> LogState {
        match self.last().record {
            MarkerRecord::Clean(_) => LogState::Clean,
            MarkerRecord::Header { .. } | MarkerRecord::Dirty => LogState::Dirty,
        }
    }

    /// The `CLEAN` that proves the snapshot, if any: the last good record,
    /// with no byte after it.
    pub fn proven_clean(&self) -> Option<&CleanRecord> {
        match &self.last().record {
            MarkerRecord::Clean(c) if self.trailing() == 0 => Some(c),
            _ => None,
        }
    }

    /// How to append `record` after the last good one: write it in place
    /// over whatever follows, and only then, if anything still follows it,
    /// truncate just past it.
    pub fn plan_append(&self, record: Append) -> AppendPlan {
        let bytes = encode_record(self.last().seq + 1, &record.into());
        let end = self.good_end + bytes.len() as u64;
        let mut ops = vec![MarkerOp::Write {
            offset: self.good_end,
            bytes,
        }];
        if self.file_len > end {
            ops.push(MarkerOp::Truncate { len: end });
        }
        AppendPlan { ops }
    }
}

/// Reads a log to its first bad frame. `Err` when its first record is not a
/// good `HEADER` of a version this build reads: such a log proves nothing.
pub fn scan(bytes: &[u8]) -> Result<MarkerScan, FormatError> {
    let (header, mut off) = frame_at(bytes, 0, 0)?;
    let MarkerRecord::Header {
        format_ver,
        incarnation,
    } = header
    else {
        return Err(FormatError::Malformed {
            what: WHAT,
            at: 0,
            why: "the first record is not a HEADER",
        });
    };
    if format_ver != MARKER_FORMAT_VER {
        return Err(FormatError::UnsupportedVersion {
            what: WHAT,
            found: format_ver,
        });
    }
    let mut records = vec![ScannedRecord {
        seq: 0,
        offset: 0,
        record: header,
    }];
    loop {
        let seq = records.len() as u64;
        match frame_at(bytes, off, seq) {
            Ok((MarkerRecord::Header { .. }, _)) | Err(_) => break,
            Ok((record, next)) => {
                records.push(ScannedRecord {
                    seq,
                    offset: off as u64,
                    record,
                });
                off = next;
            }
        }
    }
    Ok(MarkerScan {
        incarnation,
        records,
        good_end: off as u64,
        file_len: bytes.len() as u64,
    })
}

#[cfg(test)]
#[path = "marker_tests.rs"]
mod tests;
