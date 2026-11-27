//! A batch of records, in the one layout used on the wire, on disk and in
//! memory (docs/broker.md §2.1).
//!
//! Everything is little-endian. Every record starts on an 8-byte boundary and
//! a batch's length is a multiple of 8, so every fixed-width field can be read
//! where it sits, with no varints and no parse pass.
//!
//! ```text
//! header, 64 bytes
//!    0  magic          b"RQB1"
//!    4  length u32     the whole batch, header included
//!    8  crc u32        CRC-32 of bytes 12..64
//!   12  flags u16      codec (bits 0-2), keys, headers, uniform, txn, control,
//!                      producer_time (bit 8)
//!   14  epoch u16      producer epoch
//!   16  base_offset u64   stamped by the server
//!   24  base_time i64     µs since the epoch; stamped by the server unless
//!                         producer_time
//!   32  count u32
//!   36  max_time_delta u32
//!   40  producer_id u64   0 = not idempotent
//!   48  base_sequence u32
//!   52  records_len u32   bytes after the header
//!   56  raw_len u32       records_len before compression
//!   60  records_crc u32   CRC-32 of bytes 64..length
//!
//! record, 8-byte aligned
//!    0  length u32     this record, padding included
//!    4  time_delta u32 µs after base_time
//!    8  key_len u32    u32::MAX = no key
//!   12  value_len u32
//!   16  header_count u16
//!   18  reserved u16
//!   20  key, value, then (name_len u16, value_len u32, name, value) per header
//!       zero padding to 8
//! ```
//!
//! `crc` covers the header and `records_crc` the records, so stamping the two
//! server-owned fields rehashes 52 bytes and never the records (§2.2).

use std::fmt;

/// The first four bytes of every batch.
pub const MAGIC: [u8; 4] = *b"RQB1";
/// Bytes of batch header.
pub const HEADER: usize = 64;
/// Bytes of record header before its key.
pub const RECORD_HEADER: usize = 20;
/// The alignment of every record and of a batch's length.
pub const ALIGN: usize = 8;
/// `key_len` of a record without a key.
pub const NO_KEY: u32 = u32::MAX;
/// The largest batch either side accepts, whatever a limit says.
pub const MAX_BATCH: usize = 1 << 30;

pub const O_MAGIC: usize = 0;
pub const O_LENGTH: usize = 4;
pub const O_CRC: usize = 8;
pub const O_FLAGS: usize = 12;
pub const O_EPOCH: usize = 14;
pub const O_BASE_OFFSET: usize = 16;
pub const O_BASE_TIME: usize = 24;
pub const O_COUNT: usize = 32;
pub const O_MAX_DELTA: usize = 36;
pub const O_PRODUCER: usize = 40;
pub const O_SEQUENCE: usize = 48;
pub const O_RECORDS_LEN: usize = 52;
pub const O_RAW_LEN: usize = 56;
pub const O_RECORDS_CRC: usize = 60;

pub const CODEC_MASK: u16 = 0b111;
pub const CODEC_NONE: u16 = 0;
pub const CODEC_ZSTD: u16 = 1;
pub const F_KEYS: u16 = 1 << 3;
pub const F_HEADERS: u16 = 1 << 4;
pub const F_UNIFORM: u16 = 1 << 5;
pub const F_TXN: u16 = 1 << 6;
pub const F_CONTROL: u16 = 1 << 7;
pub const F_PRODUCER_TIME: u16 = 1 << 8;
/// Flags a batch may carry; any other bit is a later version's.
pub const KNOWN_FLAGS: u16 = CODEC_MASK | F_KEYS | F_HEADERS | F_UNIFORM | F_TXN | F_CONTROL | F_PRODUCER_TIME;

#[inline]
pub const fn pad8(n: usize) -> usize {
    (n + (ALIGN - 1)) & !(ALIGN - 1)
}

#[inline]
fn u16_at(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([b[at], b[at + 1]])
}
#[inline]
fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}
#[inline]
fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}
#[inline]
fn put_u16(b: &mut [u8], at: usize, v: u16) {
    b[at..at + 2].copy_from_slice(&v.to_le_bytes());
}
#[inline]
fn put_u32(b: &mut [u8], at: usize, v: u32) {
    b[at..at + 4].copy_from_slice(&v.to_le_bytes());
}
#[inline]
fn put_u64(b: &mut [u8], at: usize, v: u64) {
    b[at..at + 8].copy_from_slice(&v.to_le_bytes());
}

/// Why a batch was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchError {
    /// Fewer bytes than a header, or than the header's `length`.
    Short { have: usize, need: usize },
    Magic,
    /// `length` is not a multiple of 8, disagrees with `records_len`, or is
    /// over the limit.
    Length(u32),
    /// Flag bits this version does not know, or an unknown codec.
    Flags(u16),
    /// `count` is zero, or is not the number of records walked.
    Count { header: u32, walked: u32 },
    HeaderCrc,
    RecordsCrc,
    /// Record `index` (from 0) is malformed.
    Record { index: u32, reason: &'static str },
    /// The batch would pass the limit the builder was given.
    TooLarge { limit: usize },
    /// A record's time is too far from the batch's others for a `u32` of
    /// microseconds (about 71 minutes): start another batch.
    TimeRange,
    /// A record with a producer time in a batch whose first record had none,
    /// or the reverse.
    TimeMode,
}

impl fmt::Display for BatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchError::Short { have, need } => write!(f, "batch truncated: {have} bytes, {need} needed"),
            BatchError::Magic => f.write_str("not a batch: bad magic"),
            BatchError::Length(n) => write!(f, "implausible batch length {n}"),
            BatchError::Flags(v) => write!(f, "unknown batch flags {v:#06x}"),
            BatchError::Count { header, walked } => {
                write!(f, "batch says {header} records, {walked} found")
            }
            BatchError::HeaderCrc => f.write_str("batch header CRC mismatch"),
            BatchError::RecordsCrc => f.write_str("batch records CRC mismatch"),
            BatchError::Record { index, reason } => write!(f, "record {index}: {reason}"),
            BatchError::TooLarge { limit } => write!(f, "batch would pass {limit} bytes"),
            BatchError::TimeRange => f.write_str("record time too far from the batch's others"),
            BatchError::TimeMode => f.write_str("a batch's records either all carry a time or none does"),
        }
    }
}

impl std::error::Error for BatchError {}

/// The bytes a record of this shape takes, padding included.
#[inline]
pub fn record_size(key: Option<&[u8]>, value: &[u8], headers: &[(&[u8], &[u8])]) -> usize {
    let mut n = RECORD_HEADER + key.map_or(0, |k| k.len()) + value.len();
    for (name, val) in headers {
        n += 6 + name.len() + val.len();
    }
    pad8(n)
}

/// Builds one batch, record by record, straight into its stored layout.
///
/// ```
/// use rasterizeddb_mq::{BatchBuilder, BatchView};
/// let mut b = BatchBuilder::new();
/// b.push(Some(b"k"), b"hello", &[]).unwrap();
/// b.push(None, b"world", &[(b"trace", b"1")]).unwrap();
/// let bytes = b.finish();
/// let view = BatchView::parse(&bytes).unwrap();
/// assert_eq!(view.count(), 2);
/// let values: Vec<&[u8]> = view.records().map(|r| r.value).collect();
/// assert_eq!(values, [b"hello".as_slice(), b"world"]);
/// ```
#[derive(Debug)]
pub struct BatchBuilder {
    buf: Vec<u8>,
    count: u32,
    flags: u16,
    /// The length every record has had so far, or 0 once two differ.
    uniform_len: u32,
    /// Absolute times, one per record, when the batch carries producer times.
    times: Vec<i64>,
    min_time: i64,
    max_time: i64,
    limit: usize,
    producer_id: u64,
    epoch: u16,
    base_sequence: u32,
}

impl Default for BatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchBuilder {
    pub fn new() -> Self {
        Self::with_capacity(16 * 1024)
    }

    pub fn with_capacity(bytes: usize) -> Self {
        let mut buf = Vec::with_capacity(bytes.max(HEADER));
        buf.resize(HEADER, 0);
        BatchBuilder {
            buf,
            count: 0,
            flags: 0,
            uniform_len: 0,
            times: Vec::new(),
            min_time: i64::MAX,
            max_time: i64::MIN,
            limit: MAX_BATCH,
            producer_id: 0,
            epoch: 0,
            base_sequence: 0,
        }
    }

    /// The largest batch [`push`](Self::push) builds; the record that would
    /// pass it is refused with [`BatchError::TooLarge`], unless it is the
    /// first.
    pub fn set_limit(&mut self, bytes: usize) {
        self.limit = bytes.clamp(HEADER + RECORD_HEADER, MAX_BATCH);
    }

    /// Marks the batch as an idempotent producer's: the server drops a batch
    /// whose `(producer, epoch, sequence)` it has already appended.
    pub fn set_producer(&mut self, producer_id: u64, epoch: u16, base_sequence: u32) {
        self.producer_id = producer_id;
        self.epoch = epoch;
        self.base_sequence = base_sequence;
    }

    pub fn count(&self) -> u32 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Bytes the batch has so far, header included.
    pub fn len_bytes(&self) -> usize {
        self.buf.len()
    }

    /// Appends a record whose time the server stamps.
    #[inline]
    pub fn push(&mut self, key: Option<&[u8]>, value: &[u8], headers: &[(&[u8], &[u8])]) -> Result<(), BatchError> {
        if !self.times.is_empty() {
            return Err(BatchError::TimeMode);
        }
        self.append(key, value, headers, None)
    }

    /// Appends a record with its own time, in µs since the epoch.
    pub fn push_at(
        &mut self,
        key: Option<&[u8]>,
        value: &[u8],
        headers: &[(&[u8], &[u8])],
        time_us: i64,
    ) -> Result<(), BatchError> {
        if self.count > 0 && self.times.is_empty() {
            return Err(BatchError::TimeMode);
        }
        let (lo, hi) = (self.min_time.min(time_us), self.max_time.max(time_us));
        if (hi as i128 - lo as i128) > u32::MAX as i128 {
            return Err(BatchError::TimeRange);
        }
        self.append(key, value, headers, Some(time_us))?;
        self.min_time = lo;
        self.max_time = hi;
        Ok(())
    }

    fn append(
        &mut self,
        key: Option<&[u8]>,
        value: &[u8],
        headers: &[(&[u8], &[u8])],
        time: Option<i64>,
    ) -> Result<(), BatchError> {
        let size = record_size(key, value, headers);
        if self.count > 0 && self.buf.len() + size > self.limit {
            return Err(BatchError::TooLarge { limit: self.limit });
        }
        if size > u32::MAX as usize || headers.len() > u16::MAX as usize {
            return Err(BatchError::Record { index: self.count, reason: "too large" });
        }
        let at = self.buf.len();
        self.buf.resize(at + RECORD_HEADER, 0);
        let b = &mut self.buf;
        put_u32(b, at, size as u32);
        // time_delta is patched in finish() once the batch's earliest time is known.
        put_u32(b, at + 8, key.map_or(NO_KEY, |k| k.len() as u32));
        put_u32(b, at + 12, value.len() as u32);
        put_u16(b, at + 16, headers.len() as u16);
        if let Some(k) = key {
            b.extend_from_slice(k);
            self.flags |= F_KEYS;
        }
        b.extend_from_slice(value);
        for (name, val) in headers {
            if name.len() > u16::MAX as usize || val.len() > u32::MAX as usize {
                b.truncate(at);
                return Err(BatchError::Record { index: self.count, reason: "header too large" });
            }
            b.extend_from_slice(&(name.len() as u16).to_le_bytes());
            b.extend_from_slice(&(val.len() as u32).to_le_bytes());
            b.extend_from_slice(name);
            b.extend_from_slice(val);
        }
        if !headers.is_empty() {
            self.flags |= F_HEADERS;
        }
        b.resize(at + size, 0);
        if self.count == 0 {
            self.uniform_len = size as u32;
        } else if self.uniform_len != size as u32 {
            self.uniform_len = 0;
        }
        if let Some(t) = time {
            self.times.push(t);
        }
        self.count += 1;
        Ok(())
    }

    /// The finished batch. The builder starts over, empty, for the next one.
    pub fn finish(&mut self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.buf.capacity());
        out.resize(HEADER, 0);
        let mut done = std::mem::replace(&mut self.buf, out);
        self.seal(&mut done);
        done
    }

    /// The finished batch appended to `out`; the builder keeps its buffer.
    pub fn finish_into(&mut self, out: &mut Vec<u8>) {
        let mut buf = std::mem::take(&mut self.buf);
        self.seal(&mut buf);
        out.extend_from_slice(&buf);
        buf.clear();
        buf.resize(HEADER, 0);
        self.buf = buf;
    }

    fn seal(&mut self, b: &mut [u8]) {
        let mut flags = self.flags;
        if self.count > 1 && self.uniform_len != 0 {
            flags |= F_UNIFORM;
        }
        let mut base_time = 0i64;
        let mut max_delta = 0u32;
        if !self.times.is_empty() {
            flags |= F_PRODUCER_TIME;
            base_time = self.min_time;
            max_delta = (self.max_time - self.min_time) as u32;
            let mut at = HEADER;
            for &t in &self.times {
                put_u32(b, at + 4, (t - base_time) as u32);
                at += u32_at(b, at) as usize;
            }
        }
        let len = b.len();
        b[O_MAGIC..O_MAGIC + 4].copy_from_slice(&MAGIC);
        put_u32(b, O_LENGTH, len as u32);
        put_u16(b, O_FLAGS, flags);
        put_u16(b, O_EPOCH, self.epoch);
        put_u64(b, O_BASE_OFFSET, 0);
        put_u64(b, O_BASE_TIME, base_time as u64);
        put_u32(b, O_COUNT, self.count);
        put_u32(b, O_MAX_DELTA, max_delta);
        put_u64(b, O_PRODUCER, self.producer_id);
        put_u32(b, O_SEQUENCE, self.base_sequence);
        put_u32(b, O_RECORDS_LEN, (len - HEADER) as u32);
        put_u32(b, O_RAW_LEN, (len - HEADER) as u32);
        put_u32(b, O_RECORDS_CRC, crc32fast::hash(&b[HEADER..]));
        put_u32(b, O_CRC, header_crc(b));

        self.count = 0;
        self.flags = 0;
        self.uniform_len = 0;
        self.times.clear();
        self.min_time = i64::MAX;
        self.max_time = i64::MIN;
        // An idempotent producer's next batch follows on from this one.
        if self.producer_id != 0 {
            let n = u32_at(b, O_COUNT);
            self.base_sequence = self.base_sequence.wrapping_add(n);
        }
    }
}

#[inline]
fn header_crc(b: &[u8]) -> u32 {
    crc32fast::hash(&b[O_FLAGS..HEADER])
}

/// What the server learned from a batch it accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stamped {
    pub count: u32,
    /// The latest record time, µs since the epoch.
    pub max_time: i64,
    pub producer_id: u64,
    pub epoch: u16,
    pub base_sequence: u32,
}

/// Checks a batch a client sent, from its magic to its last record, and
/// stamps the server's two fields: `base_offset`, and `base_time` unless the
/// producer set its own. `buf` must be exactly one batch. Hashes the records
/// once and the header twice (before and after the stamp).
pub fn check_and_stamp(buf: &mut [u8], limit: usize, base_offset: u64, now_us: i64) -> Result<Stamped, BatchError> {
    check(buf, limit, true)?;
    Ok(stamp(buf, base_offset, now_us))
}

/// Stamps a batch built in this process by [`BatchBuilder`]: its records were
/// hashed when it was built and are not hashed again.
pub fn stamp_trusted(buf: &mut [u8], base_offset: u64, now_us: i64) -> Stamped {
    stamp(buf, base_offset, now_us)
}

fn stamp(buf: &mut [u8], base_offset: u64, now_us: i64) -> Stamped {
    let flags = u16_at(buf, O_FLAGS);
    put_u64(buf, O_BASE_OFFSET, base_offset);
    if flags & F_PRODUCER_TIME == 0 {
        put_u64(buf, O_BASE_TIME, now_us as u64);
    }
    let crc = header_crc(buf);
    put_u32(buf, O_CRC, crc);
    let base_time = u64_at(buf, O_BASE_TIME) as i64;
    Stamped {
        count: u32_at(buf, O_COUNT),
        max_time: base_time.saturating_add(u32_at(buf, O_MAX_DELTA) as i64),
        producer_id: u64_at(buf, O_PRODUCER),
        epoch: u16_at(buf, O_EPOCH),
        base_sequence: u32_at(buf, O_SEQUENCE),
    }
}

/// The length a batch's header claims, once at least [`O_LENGTH`] + 4 bytes
/// are in: what a reader of concatenated batches steps by.
#[inline]
pub fn claimed_len(buf: &[u8]) -> Option<usize> {
    (buf.len() >= O_LENGTH + 4).then(|| u32_at(buf, O_LENGTH) as usize)
}

/// Checks one batch: framing, both CRCs when `verify_crc`, and (uncompressed)
/// every record's layout. `buf` must start at the batch; bytes past its
/// `length` are ignored.
pub fn check(buf: &[u8], limit: usize, verify_crc: bool) -> Result<(), BatchError> {
    if buf.len() < HEADER {
        return Err(BatchError::Short { have: buf.len(), need: HEADER });
    }
    if buf[..4] != MAGIC {
        return Err(BatchError::Magic);
    }
    let len = u32_at(buf, O_LENGTH);
    let records_len = u32_at(buf, O_RECORDS_LEN) as usize;
    if (len as usize) < HEADER + RECORD_HEADER
        || len as usize % ALIGN != 0
        || len as usize > limit.min(MAX_BATCH)
        || HEADER + records_len != len as usize
    {
        return Err(BatchError::Length(len));
    }
    if buf.len() < len as usize {
        return Err(BatchError::Short { have: buf.len(), need: len as usize });
    }
    let b = &buf[..len as usize];
    let flags = u16_at(b, O_FLAGS);
    let codec = flags & CODEC_MASK;
    if flags & !KNOWN_FLAGS != 0 || codec > CODEC_ZSTD {
        return Err(BatchError::Flags(flags));
    }
    if verify_crc {
        if header_crc(b) != u32_at(b, O_CRC) {
            return Err(BatchError::HeaderCrc);
        }
        if crc32fast::hash(&b[HEADER..]) != u32_at(b, O_RECORDS_CRC) {
            return Err(BatchError::RecordsCrc);
        }
    }
    let count = u32_at(b, O_COUNT);
    if count == 0 {
        return Err(BatchError::Count { header: 0, walked: 0 });
    }
    if codec != CODEC_NONE {
        // Compressed records are checked by whoever decompresses them; here,
        // only that the size they claim is one a reader may allocate.
        let raw = u32_at(b, O_RAW_LEN) as usize;
        if raw < RECORD_HEADER * count as usize || raw > MAX_BATCH {
            return Err(BatchError::Length(len));
        }
        return Ok(());
    }
    if u32_at(b, O_RAW_LEN) as usize != records_len {
        return Err(BatchError::Length(len));
    }
    walk(&b[HEADER..], count, flags)
}

/// Walks uncompressed records, checking each one's layout.
fn walk(records: &[u8], count: u32, flags: u16) -> Result<(), BatchError> {
    let mut at = 0usize;
    let mut walked = 0u32;
    let first_len = if records.len() >= 4 { u32_at(records, 0) } else { 0 };
    while at < records.len() {
        let index = walked;
        let bad = |reason| BatchError::Record { index, reason };
        if records.len() - at < RECORD_HEADER {
            return Err(bad("truncated header"));
        }
        let r = &records[at..];
        let rlen = u32_at(r, 0) as usize;
        if rlen < RECORD_HEADER || rlen % ALIGN != 0 || rlen > r.len() {
            return Err(bad("bad length"));
        }
        if flags & F_UNIFORM != 0 && rlen as u32 != first_len {
            return Err(bad("not uniform"));
        }
        let key_len = u32_at(r, 8);
        let value_len = u32_at(r, 12) as usize;
        let header_count = u16_at(r, 16) as usize;
        let mut used = RECORD_HEADER;
        if key_len != NO_KEY {
            if flags & F_KEYS == 0 {
                return Err(bad("key in a batch without keys"));
            }
            used += key_len as usize;
        }
        used = used.checked_add(value_len).ok_or(bad("bad value length"))?;
        if used > rlen {
            return Err(bad("key and value run past the record"));
        }
        if header_count > 0 && flags & F_HEADERS == 0 {
            return Err(bad("headers in a batch without headers"));
        }
        for _ in 0..header_count {
            if used + 6 > rlen {
                return Err(bad("header runs past the record"));
            }
            let n = u16_at(r, used) as usize;
            let v = u32_at(r, used + 2) as usize;
            used = used
                .checked_add(6 + n)
                .and_then(|u| u.checked_add(v))
                .ok_or(bad("bad header length"))?;
            if used > rlen {
                return Err(bad("header runs past the record"));
            }
        }
        if pad8(used) != rlen {
            return Err(bad("padding is not the record's tail"));
        }
        at += rlen;
        walked += 1;
        if walked > count {
            break;
        }
    }
    if walked != count {
        return Err(BatchError::Count { header: count, walked });
    }
    Ok(())
}

/// A batch, read where it lies.
#[derive(Clone, Copy, Debug)]
pub struct BatchView<'a> {
    b: &'a [u8],
}

impl<'a> BatchView<'a> {
    /// Checks the batch at the start of `buf` (both CRCs and every record) and
    /// views it. `buf` may run on past it.
    pub fn parse(buf: &'a [u8]) -> Result<Self, BatchError> {
        check(buf, MAX_BATCH, true)?;
        Ok(Self::trusted(buf))
    }

    /// Views a batch already checked, as one read back from storage that
    /// this process wrote.
    pub fn trusted(buf: &'a [u8]) -> Self {
        let len = u32_at(buf, O_LENGTH) as usize;
        BatchView { b: &buf[..len] }
    }

    pub fn bytes(&self) -> &'a [u8] {
        self.b
    }
    pub fn len(&self) -> usize {
        self.b.len()
    }
    pub fn is_empty(&self) -> bool {
        false
    }
    pub fn flags(&self) -> u16 {
        u16_at(self.b, O_FLAGS)
    }
    pub fn codec(&self) -> u16 {
        self.flags() & CODEC_MASK
    }
    pub fn base_offset(&self) -> u64 {
        u64_at(self.b, O_BASE_OFFSET)
    }
    pub fn base_time(&self) -> i64 {
        u64_at(self.b, O_BASE_TIME) as i64
    }
    pub fn max_time(&self) -> i64 {
        self.base_time().saturating_add(u32_at(self.b, O_MAX_DELTA) as i64)
    }
    pub fn count(&self) -> u32 {
        u32_at(self.b, O_COUNT)
    }
    /// One past the last record's offset.
    pub fn next_offset(&self) -> u64 {
        self.base_offset() + self.count() as u64
    }
    pub fn producer_id(&self) -> u64 {
        u64_at(self.b, O_PRODUCER)
    }
    pub fn epoch(&self) -> u16 {
        u16_at(self.b, O_EPOCH)
    }
    pub fn base_sequence(&self) -> u32 {
        u32_at(self.b, O_SEQUENCE)
    }
    pub fn is_control(&self) -> bool {
        self.flags() & F_CONTROL != 0
    }
    pub fn is_transactional(&self) -> bool {
        self.flags() & F_TXN != 0
    }

    /// The records of an uncompressed batch. A compressed batch yields
    /// nothing: decompress its records region first.
    pub fn records(&self) -> Records<'a> {
        let raw = self.codec() == CODEC_NONE;
        Records {
            rest: if raw { &self.b[HEADER..] } else { &[] },
            index: 0,
            base_offset: self.base_offset(),
            base_time: self.base_time(),
        }
    }

    /// Record `i` of an uncompressed batch, in O(1) when the batch is uniform.
    pub fn record(&self, i: u32) -> Option<RecordView<'a>> {
        if i >= self.count() || self.codec() != CODEC_NONE {
            return None;
        }
        if self.flags() & F_UNIFORM != 0 {
            let stride = u32_at(self.b, HEADER) as usize;
            let at = HEADER + i as usize * stride;
            return Some(RecordView::at(&self.b[at..at + stride], i, self.base_offset(), self.base_time()));
        }
        self.records().nth(i as usize)
    }
}

/// One record of a batch.
#[derive(Clone, Copy, Debug)]
pub struct RecordView<'a> {
    pub index: u32,
    pub offset: u64,
    pub time_us: i64,
    pub key: Option<&'a [u8]>,
    pub value: &'a [u8],
    header_count: u16,
    headers: &'a [u8],
}

impl<'a> RecordView<'a> {
    fn at(r: &'a [u8], index: u32, base_offset: u64, base_time: i64) -> Self {
        let key_len = u32_at(r, 8);
        let value_len = u32_at(r, 12) as usize;
        let mut at = RECORD_HEADER;
        let key = (key_len != NO_KEY).then(|| {
            let k = &r[at..at + key_len as usize];
            at += key_len as usize;
            k
        });
        let value = &r[at..at + value_len];
        at += value_len;
        RecordView {
            index,
            offset: base_offset + index as u64,
            time_us: base_time + u32_at(r, 4) as i64,
            key,
            value,
            header_count: u16_at(r, 16),
            headers: &r[at..],
        }
    }

    pub fn header_count(&self) -> u16 {
        self.header_count
    }

    pub fn headers(&self) -> Headers<'a> {
        Headers { rest: self.headers, left: self.header_count }
    }

    /// The first header named `name`.
    pub fn header(&self, name: &[u8]) -> Option<&'a [u8]> {
        self.headers().find(|(n, _)| *n == name).map(|(_, v)| v)
    }
}

pub struct Records<'a> {
    rest: &'a [u8],
    index: u32,
    base_offset: u64,
    base_time: i64,
}

impl<'a> Iterator for Records<'a> {
    type Item = RecordView<'a>;
    #[inline]
    fn next(&mut self) -> Option<RecordView<'a>> {
        if self.rest.len() < RECORD_HEADER {
            return None;
        }
        let len = u32_at(self.rest, 0) as usize;
        let (r, rest) = self.rest.split_at(len);
        self.rest = rest;
        let v = RecordView::at(r, self.index, self.base_offset, self.base_time);
        self.index += 1;
        Some(v)
    }
}

pub struct Headers<'a> {
    rest: &'a [u8],
    left: u16,
}

impl<'a> Iterator for Headers<'a> {
    type Item = (&'a [u8], &'a [u8]);
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let n = u16_at(self.rest, 0) as usize;
        let v = u32_at(self.rest, 2) as usize;
        let name = &self.rest[6..6 + n];
        let value = &self.rest[6 + n..6 + n + v];
        self.rest = &self.rest[6 + n + v..];
        Some((name, value))
    }
}

/// Batches laid end to end, as a fetch returns them and a segment holds them.
/// Stops at the first that does not frame (a short tail of a fetch cut at its
/// byte limit), which [`Batches::rest`] then holds.
pub struct Batches<'a> {
    rest: &'a [u8],
    verify: bool,
}

impl<'a> Batches<'a> {
    /// Batches checked as they are read (both CRCs and every record).
    pub fn checked(buf: &'a [u8]) -> Self {
        Batches { rest: buf, verify: true }
    }
    /// Batches from storage this process wrote; framing only.
    pub fn trusted(buf: &'a [u8]) -> Self {
        Batches { rest: buf, verify: false }
    }
    pub fn rest(&self) -> &'a [u8] {
        self.rest
    }
}

impl<'a> Iterator for Batches<'a> {
    type Item = Result<BatchView<'a>, BatchError>;
    fn next(&mut self) -> Option<Self::Item> {
        let len = claimed_len(self.rest)?;
        if len < HEADER || self.rest.len() < len {
            return None;
        }
        if self.verify {
            if let Err(e) = check(self.rest, MAX_BATCH, true) {
                self.rest = &[];
                return Some(Err(e));
            }
        } else if self.rest[..4] != MAGIC {
            self.rest = &[];
            return Some(Err(BatchError::Magic));
        }
        let (b, rest) = self.rest.split_at(len);
        self.rest = rest;
        Some(Ok(BatchView { b }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_round_trips_and_stamps() {
        let mut b = BatchBuilder::new();
        b.push(Some(b"a"), b"one", &[]).unwrap();
        b.push(None, b"two", &[(b"h".as_slice(), b"v".as_slice())]).unwrap();
        let mut bytes = b.finish();
        assert_eq!(bytes.len() % 8, 0);
        let s = check_and_stamp(&mut bytes, MAX_BATCH, 100, 5_000).unwrap();
        assert_eq!(s.count, 2);
        let v = BatchView::parse(&bytes).unwrap();
        assert_eq!(v.base_offset(), 100);
        assert_eq!(v.base_time(), 5_000);
        let r: Vec<_> = v.records().collect();
        assert_eq!(r[0].offset, 100);
        assert_eq!(r[0].key, Some(b"a".as_slice()));
        assert_eq!(r[1].key, None);
        assert_eq!(r[1].header(b"h"), Some(b"v".as_slice()));
        assert_eq!(r[1].offset, 101);
    }

    #[test]
    fn uniform_batches_seek() {
        let mut b = BatchBuilder::new();
        for i in 0..10u32 {
            b.push(None, &i.to_le_bytes(), &[]).unwrap();
        }
        let bytes = b.finish();
        let v = BatchView::parse(&bytes).unwrap();
        assert!(v.flags() & F_UNIFORM != 0);
        assert_eq!(v.record(7).unwrap().value, 7u32.to_le_bytes());
    }

    #[test]
    fn damage_is_caught() {
        let mut b = BatchBuilder::new();
        b.push(None, b"payload", &[]).unwrap();
        let good = b.finish();
        let mut bad = good.clone();
        *bad.last_mut().unwrap() ^= 0; // padding byte
        let at = HEADER + RECORD_HEADER;
        bad[at] ^= 1;
        assert_eq!(check(&bad, MAX_BATCH, true), Err(BatchError::RecordsCrc));
        let mut bad = good.clone();
        bad[O_COUNT] = 2;
        assert_eq!(check(&bad, MAX_BATCH, true), Err(BatchError::HeaderCrc));
    }

    #[test]
    fn producer_times() {
        let mut b = BatchBuilder::new();
        b.push_at(None, b"x", &[], 1_000).unwrap();
        b.push_at(None, b"y", &[], 900).unwrap();
        assert_eq!(b.push(None, b"z", &[]), Err(BatchError::TimeMode));
        let mut bytes = b.finish();
        check_and_stamp(&mut bytes, MAX_BATCH, 0, 99_999).unwrap();
        let v = BatchView::parse(&bytes).unwrap();
        let t: Vec<i64> = v.records().map(|r| r.time_us).collect();
        assert_eq!(t, [1_000, 900]);
        assert_eq!(v.max_time(), 1_000);
    }
}
