//! Key naming and the on-object encodings.
//!
//! # Why a file is not one object
//!
//! An object store has no partial write, so keeping a table's rows in a single
//! object would mean re-uploading the whole table to change one row. Splitting
//! it into fixed-size *base chunks* bounds that to one chunk — but a chunk big
//! enough to make scans cheap (megabytes) is still absurd to rewrite for a
//! 64-byte row-pointer status flip, which is the single most common write this
//! engine makes.
//!
//! So base chunks are not written on the commit path at all. A commit appends a
//! *delta batch*: one object holding the byte ranges that changed. That is one
//! PUT per commit no matter how many chunks were touched and no matter how far
//! apart they are, and the uploaded bytes are the bytes that actually changed.
//! Compaction later folds deltas into base chunks in the background, which is
//! where the read path's simplicity is bought back.
//!
//! ```text
//!   <root>/<file>/META                  current base generation and length
//!   <root>/<file>/g<generation>/c<index>       base chunk, `chunk_size` bytes
//!   <root>/<file>/d/<seq>               delta batch
//! ```
//!
//! Sequence numbers and chunk indexes are zero-padded hex so a lexicographic
//! listing — the only ordering an object store offers — is also the numeric one.

use std::io;

use super::super::CRC;

/// `RDKV`, little-endian.
pub const MAGIC_DELTA: u32 = 0x564B_4452;
/// `RDKM`, little-endian.
pub const MAGIC_META: u32 = 0x4D4B_4452;
pub const FORMAT_VERSION: u16 = 1;

/// The version a delta batch carrying an `aborted` list is written at.
///
/// A build that predates the list reads a version-1 batch, ignores the
/// list, and so would honour an in-doubt batch's commit of a transaction the
/// list declares rolled back — a transaction the engine reported failed and
/// whose space it reused. Such a batch is therefore written as version 2,
/// which that build refuses (its decoder errors on any version but 1) rather
/// than misreads. A batch with no aborts stays version 1 and readable by it.
pub const DELTA_VERSION_WITH_ABORTS: u16 = 2;

/// magic + version + flags + seq + base_gen + rec_count + commit_count + crc
/// + reserved.
pub const DELTA_HEADER_LEN: usize = 40;
/// magic + version + flags + chunk_size + base_gen + base_len + compacted_upto
/// + crc.
pub const META_LEN: usize = 44;

/// What one record in a delta batch does.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecordKind {
    /// Put `payload` at `offset`, extending the file if it ends past the end.
    Write = 0,
    /// Shorten the file to `offset` bytes. Carries no payload.
    Truncate = 1,
}

impl RecordKind {
    fn from_u8(v: u8) -> io::Result<Self> {
        match v {
            0 => Ok(RecordKind::Write),
            1 => Ok(RecordKind::Truncate),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown kv record kind {other}"),
            )),
        }
    }
}

/// One change, as it is held in memory before a batch is written and as it is
/// read back during recovery.
#[derive(Clone, Debug)]
pub struct Record {
    /// The transaction that produced it, or 0 for a write that is its own
    /// transaction. Recovery keeps a record only if its transaction committed.
    pub txn: u64,
    pub kind: RecordKind,
    pub offset: u64,
    pub payload: Vec<u8>,
}

impl Record {
    pub fn write(txn: u64, offset: u64, payload: Vec<u8>) -> Self {
        Self {
            txn,
            kind: RecordKind::Write,
            offset,
            payload,
        }
    }

    pub fn truncate(txn: u64, len: u64) -> Self {
        Self {
            txn,
            kind: RecordKind::Truncate,
            offset: len,
            payload: Vec::new(),
        }
    }

    /// Bytes this record occupies in memory and, near enough, on the wire.
    pub fn weight(&self) -> u64 {
        self.payload.len() as u64 + 21
    }
}

/// A batch of records plus the transactions this batch declares committed.
///
/// The commit list is what makes a batch safe to write before the transaction
/// that produced its records has finished. Records can be spilled early to keep
/// memory bounded during a long statement; nothing applies them until some later
/// batch names their transaction. It is the same bargain the write-ahead log
/// makes, expressed in objects instead of a file.
#[derive(Debug, Default)]
pub struct DeltaBatch {
    pub seq: u64,
    /// The base generation these records are relative to. A batch written before
    /// a `swap_temp` describes bytes that no longer exist afterwards, and
    /// applying it to the new generation would corrupt the file.
    pub base_gen: u64,
    pub committed: Vec<u64>,
    pub records: Vec<Record>,
    /// Transactions this batch declares rolled back, each with the sequence of
    /// the declaring batch whose put failed: `(txn, in_doubt_seq)`.
    ///
    /// A put that reported an error may still land later — the request was
    /// sent, the client gave up on it. If it does, it declares a transaction
    /// the engine was told had failed, and whose space it has since reused. An
    /// abort here overrides any declaration of the same transaction, and is
    /// needed only until the file's `compacted_upto` passes `in_doubt_seq`,
    /// after which recovery discards that batch unread.
    ///
    /// Stored after the records, with its count in the header's formerly
    /// reserved word, so a build that predates it reads such a batch as before
    /// and ignores the list.
    pub aborted: Vec<(u64, u64)>,
}

impl DeltaBatch {
    pub fn encode(&self) -> Vec<u8> {
        let payload_len: usize = self.committed.len() * 8
            + self
                .records
                .iter()
                .map(|r| 21 + r.payload.len())
                .sum::<usize>();
        let mut out = Vec::with_capacity(DELTA_HEADER_LEN + payload_len);
        out.extend_from_slice(&MAGIC_DELTA.to_le_bytes());
        let version = if self.aborted.is_empty() {
            FORMAT_VERSION
        } else {
            DELTA_VERSION_WITH_ABORTS
        };
        out.extend_from_slice(&version.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes()); // flags
        out.extend_from_slice(&self.seq.to_le_bytes());
        out.extend_from_slice(&self.base_gen.to_le_bytes());
        out.extend_from_slice(&(self.records.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.committed.len() as u32).to_le_bytes());
        // CRC slot, filled once the payload is laid down.
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(&(self.aborted.len() as u32).to_le_bytes());
        debug_assert_eq!(out.len(), DELTA_HEADER_LEN);

        for txn in &self.committed {
            out.extend_from_slice(&txn.to_le_bytes());
        }
        for record in &self.records {
            out.push(record.kind as u8);
            out.extend_from_slice(&record.txn.to_le_bytes());
            out.extend_from_slice(&record.offset.to_le_bytes());
            out.extend_from_slice(&(record.payload.len() as u32).to_le_bytes());
            out.extend_from_slice(&record.payload);
        }
        for (txn, seq) in &self.aborted {
            out.extend_from_slice(&txn.to_le_bytes());
            out.extend_from_slice(&seq.to_le_bytes());
        }

        let crc = CRC.checksum(&out[DELTA_HEADER_LEN..]);
        out[32..36].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// Decodes a batch, returning `Ok(None)` for one that is not intact.
    ///
    /// A truncated or corrupt batch is not an error to propagate: an object
    /// store's put is atomic, so the only way to see a partial batch is a store
    /// that broke its contract, and the safe reading of a batch we cannot trust
    /// is that its transactions never committed. Erroring instead would refuse
    /// to open a database over one damaged object.
    pub fn decode(bytes: &[u8]) -> io::Result<Option<Self>> {
        if bytes.len() < DELTA_HEADER_LEN {
            return Ok(None);
        }
        if u32::from_le_bytes(bytes[0..4].try_into().unwrap()) != MAGIC_DELTA {
            return Ok(None);
        }
        let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        if version != FORMAT_VERSION && version != DELTA_VERSION_WITH_ABORTS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "kv delta batch is format version {version}, this build reads \
                     {FORMAT_VERSION} and {DELTA_VERSION_WITH_ABORTS}"
                ),
            ));
        }
        let seq = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let base_gen = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let rec_count = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
        let commit_count = u32::from_le_bytes(bytes[28..32].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(bytes[32..36].try_into().unwrap());
        let abort_count = u32::from_le_bytes(bytes[36..40].try_into().unwrap()) as usize;

        let payload = &bytes[DELTA_HEADER_LEN..];
        if CRC.checksum(payload) != crc {
            return Ok(None);
        }

        let mut cursor = 0usize;
        let mut committed = Vec::with_capacity(commit_count);
        for _ in 0..commit_count {
            if cursor + 8 > payload.len() {
                return Ok(None);
            }
            committed.push(u64::from_le_bytes(
                payload[cursor..cursor + 8].try_into().unwrap(),
            ));
            cursor += 8;
        }

        let mut records = Vec::with_capacity(rec_count);
        for _ in 0..rec_count {
            if cursor + 21 > payload.len() {
                return Ok(None);
            }
            let kind = RecordKind::from_u8(payload[cursor])?;
            let txn = u64::from_le_bytes(payload[cursor + 1..cursor + 9].try_into().unwrap());
            let offset = u64::from_le_bytes(payload[cursor + 9..cursor + 17].try_into().unwrap());
            let len =
                u32::from_le_bytes(payload[cursor + 17..cursor + 21].try_into().unwrap()) as usize;
            cursor += 21;
            if cursor + len > payload.len() {
                return Ok(None);
            }
            records.push(Record {
                txn,
                kind,
                offset,
                payload: payload[cursor..cursor + len].to_vec(),
            });
            cursor += len;
        }

        let mut aborted = Vec::with_capacity(abort_count.min(1024));
        for _ in 0..abort_count {
            if cursor + 16 > payload.len() {
                return Ok(None);
            }
            aborted.push((
                u64::from_le_bytes(payload[cursor..cursor + 8].try_into().unwrap()),
                u64::from_le_bytes(payload[cursor + 8..cursor + 16].try_into().unwrap()),
            ));
            cursor += 16;
        }

        Ok(Some(Self {
            seq,
            base_gen,
            committed,
            records,
            aborted,
        }))
    }
}

/// What is true of a file's base chunks, independent of any delta.
#[derive(Clone, Copy, Debug, Default)]
pub struct Meta {
    pub chunk_size: u64,
    /// Which generation of base chunks is live. Bumped by `swap_temp`, which is
    /// how a compacted file replaces the original without copying an object.
    pub base_gen: u64,
    /// Bytes of the file that live in base chunks. Everything past this exists
    /// only in deltas.
    pub base_len: u64,
    /// Deltas below this sequence have been folded into base chunks and may be
    /// ignored even if the delete that should have removed them did not land.
    pub compacted_upto: u64,
}

impl Meta {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(META_LEN);
        out.extend_from_slice(&MAGIC_META.to_le_bytes());
        out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&self.chunk_size.to_le_bytes());
        out.extend_from_slice(&self.base_gen.to_le_bytes());
        out.extend_from_slice(&self.base_len.to_le_bytes());
        out.extend_from_slice(&self.compacted_upto.to_le_bytes());
        let crc = CRC.checksum(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        debug_assert_eq!(out.len(), META_LEN);
        out
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Option<Self>> {
        if bytes.len() < META_LEN {
            return Ok(None);
        }
        if u32::from_le_bytes(bytes[0..4].try_into().unwrap()) != MAGIC_META {
            return Ok(None);
        }
        let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("kv meta is format version {version}, this build writes {FORMAT_VERSION}"),
            ));
        }
        if CRC.checksum(&bytes[0..META_LEN - 4])
            != u32::from_le_bytes(bytes[META_LEN - 4..META_LEN].try_into().unwrap())
        {
            return Ok(None);
        }
        Ok(Some(Self {
            chunk_size: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            base_gen: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            base_len: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            compacted_upto: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
        }))
    }
}

// ---- key composition -------------------------------------------------------

#[inline]
pub fn meta_key(root: &str) -> String {
    format!("{root}/META")
}

#[inline]
pub fn chunk_key(root: &str, generation: u64, index: u64) -> String {
    format!("{root}/g{generation:08x}/c{index:012x}")
}

#[inline]
pub fn generation_prefix(root: &str, generation: u64) -> String {
    format!("{root}/g{generation:08x}/")
}

#[inline]
pub fn delta_key(root: &str, seq: u64) -> String {
    format!("{root}/d/{seq:016x}")
}

#[inline]
pub fn delta_prefix(root: &str) -> String {
    format!("{root}/d/")
}

/// Parses the sequence back out of a delta key, for recovery's ordering.
pub fn delta_seq(key: &str) -> Option<u64> {
    let last = key.rsplit('/').next()?;
    u64::from_str_radix(last, 16).ok()
}
