//! Kafka's wire primitives and RecordBatch v2, and the translation between a
//! Kafka batch and the broker's own (docs/broker.md §7).
//!
//! Only the non-flexible request versions are served (see [`super::versions`]),
//! so there are no tagged fields and no compact strings: big-endian integers,
//! `int16`-length strings, `int32`-length bytes and arrays, and the records'
//! zig-zag varints.
//!
//! A produced Kafka batch is decoded (decompressed when it has to be: gzip,
//! snappy in Kafka's framing, lz4 frames, zstd) and rebuilt as a native batch
//! with the same producer id, epoch, sequence and record times. A fetch
//! rebuilds Kafka batches from stored ones, one for one, uncompressed. The
//! native path skips both steps, which is why it is the faster one.

use rasterizeddb_mq::batch::{BatchBuilder, BatchView, Batches, CODEC_NONE};

/// Kafka's CRC-32C (Castagnoli), over a batch from `attributes` to its end.
static CRC32C: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

#[derive(Debug)]
pub struct Malformed(pub &'static str);

pub type R<T> = Result<T, Malformed>;

/// Reads a request body.
pub struct In<'a> {
    b: &'a [u8],
}

impl<'a> In<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        In { b }
    }
    fn take(&mut self, n: usize) -> R<&'a [u8]> {
        if self.b.len() < n {
            return Err(Malformed("request too short"));
        }
        let (h, t) = self.b.split_at(n);
        self.b = t;
        Ok(h)
    }
    pub fn i8(&mut self) -> R<i8> {
        Ok(self.take(1)?[0] as i8)
    }
    pub fn i16(&mut self) -> R<i16> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn i32(&mut self) -> R<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn i64(&mut self) -> R<i64> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn bool(&mut self) -> R<bool> {
        Ok(self.i8()? != 0)
    }
    /// A string; a null one reads as `None`.
    pub fn nstr(&mut self) -> R<Option<&'a str>> {
        let n = self.i16()?;
        if n < 0 {
            return Ok(None);
        }
        std::str::from_utf8(self.take(n as usize)?).map(Some).map_err(|_| Malformed("string is not UTF-8"))
    }
    pub fn str(&mut self) -> R<&'a str> {
        self.nstr()?.ok_or(Malformed("null string"))
    }
    pub fn nbytes(&mut self) -> R<Option<&'a [u8]>> {
        let n = self.i32()?;
        if n < 0 {
            return Ok(None);
        }
        self.take(n as usize).map(Some)
    }
    /// An array's length; `None` for a null array.
    pub fn narray(&mut self) -> R<Option<usize>> {
        let n = self.i32()?;
        if n < 0 {
            return Ok(None);
        }
        if n as usize > self.b.len() {
            return Err(Malformed("array longer than the request"));
        }
        Ok(Some(n as usize))
    }
    pub fn array(&mut self) -> R<usize> {
        Ok(self.narray()?.unwrap_or(0))
    }
    pub fn rest(&self) -> &'a [u8] {
        self.b
    }
}

/// Writes a response body.
pub struct Out<'a>(pub &'a mut Vec<u8>);

impl Out<'_> {
    pub fn i8(&mut self, v: i8) {
        self.0.push(v as u8);
    }
    pub fn i16(&mut self, v: i16) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    pub fn i32(&mut self, v: i32) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    pub fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    pub fn bool(&mut self, v: bool) {
        self.0.push(v as u8);
    }
    pub fn str(&mut self, s: &str) {
        self.i16(s.len().min(i16::MAX as usize) as i16);
        self.0.extend_from_slice(&s.as_bytes()[..s.len().min(i16::MAX as usize)]);
    }
    pub fn nstr(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.str(s),
            None => self.i16(-1),
        }
    }
    pub fn bytes(&mut self, b: &[u8]) {
        self.i32(b.len() as i32);
        self.0.extend_from_slice(b);
    }
    pub fn nbytes(&mut self, b: Option<&[u8]>) {
        match b {
            Some(b) => self.bytes(b),
            None => self.i32(-1),
        }
    }
    pub fn array(&mut self, n: usize) {
        self.i32(n as i32);
    }
}

// ---------------------------------------------------------------------------
// Varints
// ---------------------------------------------------------------------------

fn varint(b: &[u8], at: &mut usize) -> R<i64> {
    let mut v: u64 = 0;
    let mut shift = 0;
    loop {
        let byte = *b.get(*at).ok_or(Malformed("varint runs past the record"))?;
        *at += 1;
        v |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 63 {
            return Err(Malformed("varint too long"));
        }
    }
    Ok(((v >> 1) as i64) ^ -((v & 1) as i64))
}

fn put_varint(out: &mut Vec<u8>, v: i64) {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    while z >= 0x80 {
        out.push((z as u8) | 0x80);
        z >>= 7;
    }
    out.push(z as u8);
}

fn varint_len(v: i64) -> usize {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    let mut n = 1;
    while z >= 0x80 {
        z >>= 7;
        n += 1;
    }
    n
}

// ---------------------------------------------------------------------------
// RecordBatch v2
// ---------------------------------------------------------------------------

/// Bytes of a v2 batch before its records.
const BATCH_HEADER: usize = 61;

pub const COMPRESSION_NONE: i16 = 0;
pub const COMPRESSION_GZIP: i16 = 1;
pub const COMPRESSION_SNAPPY: i16 = 2;
pub const COMPRESSION_LZ4: i16 = 3;
pub const COMPRESSION_ZSTD: i16 = 4;

/// Why a produced batch was refused, as a Kafka error code.
#[derive(Debug)]
pub struct BadBatch {
    pub code: i16,
    pub message: String,
}

fn bad(code: i16, message: impl Into<String>) -> BadBatch {
    BadBatch { code, message: message.into() }
}

/// One Kafka batch's identity, kept for the reply and the idempotence check.
#[derive(Clone, Copy, Debug)]
pub struct KafkaBatchInfo {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub transactional: bool,
    pub control: bool,
    pub count: i32,
}

/// Rebuilds every v2 batch in `records` (a produce request's record set) as
/// native batches. Older message formats (magic 0 and 1) are refused: every
/// client of the versions served writes v2.
pub fn decode_record_set(records: &[u8], max_bytes: usize) -> Result<Vec<(Vec<u8>, KafkaBatchInfo)>, BadBatch> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < records.len() {
        let rest = &records[at..];
        if rest.len() < 17 {
            return Err(bad(2, "a record set ends inside a batch header"));
        }
        let batch_len = i32::from_be_bytes(rest[8..12].try_into().unwrap());
        if batch_len < 49 {
            return Err(bad(2, "implausible batch length"));
        }
        let total = 12 + batch_len as usize;
        if rest.len() < total {
            return Err(bad(2, "a batch runs past its record set"));
        }
        let magic = rest[16] as i8;
        if magic != 2 {
            return Err(bad(43, format!("message format v{magic}; only v2 record batches are accepted")));
        }
        out.push(decode_batch(&rest[..total], max_bytes)?);
        at += total;
    }
    Ok(out)
}

fn decode_batch(b: &[u8], max_bytes: usize) -> Result<(Vec<u8>, KafkaBatchInfo), BadBatch> {
    if b.len() < BATCH_HEADER {
        return Err(bad(2, "batch shorter than its header"));
    }
    let crc = u32::from_be_bytes(b[17..21].try_into().unwrap());
    if CRC32C.checksum(&b[21..]) != crc {
        return Err(bad(2, "batch CRC mismatch"));
    }
    let attributes = i16::from_be_bytes(b[21..23].try_into().unwrap());
    let base_ts = i64::from_be_bytes(b[27..35].try_into().unwrap());
    let info = KafkaBatchInfo {
        producer_id: i64::from_be_bytes(b[43..51].try_into().unwrap()),
        producer_epoch: i16::from_be_bytes(b[51..53].try_into().unwrap()),
        base_sequence: i32::from_be_bytes(b[53..57].try_into().unwrap()),
        transactional: attributes & 0x10 != 0,
        control: attributes & 0x20 != 0,
        count: i32::from_be_bytes(b[57..61].try_into().unwrap()),
    };
    if info.count < 0 {
        return Err(bad(2, "negative record count"));
    }
    let raw = &b[BATCH_HEADER..];
    let decompressed;
    let records: &[u8] = match attributes & 0x7 {
        COMPRESSION_NONE => raw,
        codec => {
            decompressed = decompress(codec, raw, max_bytes)?;
            &decompressed
        }
    };
    let log_append = attributes & 0x8 != 0;
    let mut builder = BatchBuilder::with_capacity(records.len() + 64 * info.count as usize + 64);
    builder.set_limit(max_bytes);
    if info.producer_id >= 0 && info.base_sequence >= 0 {
        builder.set_producer((info.producer_id as u64).max(1), info.producer_epoch as u16, info.base_sequence as u32);
    }
    let mut at = 0;
    let mut headers: Vec<(&[u8], &[u8])> = Vec::new();
    for _ in 0..info.count {
        let len = varint(records, &mut at).map_err(|e| bad(2, e.0))?;
        let start = at;
        let end = start.checked_add(len as usize).filter(|e| *e <= records.len()).ok_or_else(|| bad(2, "record runs past its batch"))?;
        let rec = &records[..end];
        let mut p = start + 1; // attributes
        let ts_delta = varint(rec, &mut p).map_err(|e| bad(2, e.0))?;
        let _offset_delta = varint(rec, &mut p).map_err(|e| bad(2, e.0))?;
        let key = take_nbytes(rec, &mut p)?;
        let value = take_nbytes(rec, &mut p)?.unwrap_or(&[]);
        let n = varint(rec, &mut p).map_err(|e| bad(2, e.0))?;
        headers.clear();
        for _ in 0..n.max(0) {
            let k = take_nbytes(rec, &mut p)?.unwrap_or(&[]);
            let v = take_nbytes(rec, &mut p)?.unwrap_or(&[]);
            headers.push((k, v));
        }
        let pushed = if log_append {
            builder.push(key, value, &headers)
        } else {
            builder.push_at(key, value, &headers, base_ts.saturating_add(ts_delta).saturating_mul(1000))
        };
        if let Err(e) = pushed {
            return Err(bad(10, e.to_string()));
        }
        at = end;
    }
    if builder.is_empty() {
        return Err(bad(87, "a batch with no records"));
    }
    Ok((builder.finish(), info))
}

fn take_nbytes<'a>(rec: &'a [u8], p: &mut usize) -> Result<Option<&'a [u8]>, BadBatch> {
    let n = varint(rec, p).map_err(|e| bad(2, e.0))?;
    if n < 0 {
        return Ok(None);
    }
    let end = p.checked_add(n as usize).filter(|e| *e <= rec.len()).ok_or_else(|| bad(2, "field runs past its record"))?;
    let v = &rec[*p..end];
    *p = end;
    Ok(Some(v))
}

fn decompress(codec: i16, raw: &[u8], max: usize) -> Result<Vec<u8>, BadBatch> {
    use std::io::Read;
    let limit = |v: Vec<u8>| if v.len() > max { Err(bad(10, "decompressed batch too large")) } else { Ok(v) };
    let mut out = Vec::new();
    match codec {
        COMPRESSION_GZIP => {
            flate2::read::GzDecoder::new(raw)
                .take(max as u64 + 1)
                .read_to_end(&mut out)
                .map_err(|e| bad(2, format!("gzip: {e}")))?;
        }
        COMPRESSION_SNAPPY => out = snappy_decode(raw, max)?,
        COMPRESSION_LZ4 => {
            lz4_flex::frame::FrameDecoder::new(raw)
                .take(max as u64 + 1)
                .read_to_end(&mut out)
                .map_err(|e| bad(2, format!("lz4: {e}")))?;
        }
        COMPRESSION_ZSTD => {
            zstd::stream::read::Decoder::new(raw)
                .map_err(|e| bad(2, format!("zstd: {e}")))?
                .take(max as u64 + 1)
                .read_to_end(&mut out)
                .map_err(|e| bad(2, format!("zstd: {e}")))?;
        }
        other => return Err(bad(76, format!("unknown compression codec {other}"))),
    }
    limit(out)
}

/// Snappy as Kafka clients write it: the Java client's xerial framing
/// (`\x82SNAPPY\0`, two version ints, then `int32`-length raw blocks), or a
/// bare raw block (librdkafka can write either).
fn snappy_decode(raw: &[u8], max: usize) -> Result<Vec<u8>, BadBatch> {
    const XERIAL: &[u8] = b"\x82SNAPPY\0";
    let mut dec = snap::raw::Decoder::new();
    let mut one = |block: &[u8], out: &mut Vec<u8>| -> Result<(), BadBatch> {
        let n = snap::raw::decompress_len(block).map_err(|e| bad(2, format!("snappy: {e}")))?;
        if out.len() + n > max {
            return Err(bad(10, "decompressed batch too large"));
        }
        let at = out.len();
        out.resize(at + n, 0);
        dec.decompress(block, &mut out[at..]).map_err(|e| bad(2, format!("snappy: {e}")))?;
        Ok(())
    };
    let mut out = Vec::new();
    if raw.starts_with(XERIAL) && raw.len() >= 16 {
        let mut at = 16;
        while at + 4 <= raw.len() {
            let n = u32::from_be_bytes(raw[at..at + 4].try_into().unwrap()) as usize;
            at += 4;
            let block = raw.get(at..at + n).ok_or_else(|| bad(2, "snappy block runs past the batch"))?;
            one(block, &mut out)?;
            at += n;
        }
    } else {
        one(raw, &mut out)?;
    }
    Ok(out)
}

/// Appends stored batches to `out` as Kafka v2 batches, uncompressed, from
/// offset `from` on. Records before `from` in the first batch are kept: a
/// Kafka consumer skips them itself, as it does after a broker's fetch.
/// Returns the bytes written. A batch compressed by the native side is
/// skipped (none are written today; a native producer compresses nothing).
pub fn encode_fetch(stored: &[u8], out: &mut Vec<u8>, max_bytes: usize) -> usize {
    let start = out.len();
    for b in Batches::trusted(stored).flatten() {
        if b.codec() != CODEC_NONE {
            continue;
        }
        if out.len() > start && out.len() - start >= max_bytes {
            break;
        }
        encode_batch(&b, out);
    }
    out.len() - start
}

fn encode_batch(b: &BatchView<'_>, out: &mut Vec<u8>) {
    let at = out.len();
    let base_ms = b.base_time() / 1000;
    let max_ms = b.max_time() / 1000;
    let producer = b.producer_id();
    out.extend_from_slice(&(b.base_offset() as i64).to_be_bytes());
    out.extend_from_slice(&[0; 4]); // batch length, below
    out.extend_from_slice(&0i32.to_be_bytes()); // partition leader epoch
    out.push(2); // magic
    out.extend_from_slice(&[0; 4]); // crc, below
    let crc_from = out.len();
    let mut attributes: i16 = 0;
    if b.is_transactional() {
        attributes |= 0x10;
    }
    if b.is_control() {
        attributes |= 0x20;
    }
    out.extend_from_slice(&attributes.to_be_bytes());
    out.extend_from_slice(&((b.count() as i32) - 1).to_be_bytes());
    out.extend_from_slice(&base_ms.to_be_bytes());
    out.extend_from_slice(&max_ms.to_be_bytes());
    out.extend_from_slice(&(if producer == 0 { -1i64 } else { producer as i64 }).to_be_bytes());
    out.extend_from_slice(&(if producer == 0 { -1i16 } else { b.epoch() as i16 }).to_be_bytes());
    out.extend_from_slice(&(if producer == 0 { -1i32 } else { b.base_sequence() as i32 }).to_be_bytes());
    out.extend_from_slice(&(b.count() as i32).to_be_bytes());
    for r in b.records() {
        let ts_delta = r.time_us / 1000 - base_ms;
        let offset_delta = r.index as i64;
        let key_len = r.key.map_or(-1, |k| k.len() as i64);
        let mut body = 1 + varint_len(ts_delta) + varint_len(offset_delta) + varint_len(key_len);
        body += r.key.map_or(0, |k| k.len());
        body += varint_len(r.value.len() as i64) + r.value.len();
        body += varint_len(r.header_count() as i64);
        for (k, v) in r.headers() {
            body += varint_len(k.len() as i64) + k.len() + varint_len(v.len() as i64) + v.len();
        }
        put_varint(out, body as i64);
        out.push(0);
        put_varint(out, ts_delta);
        put_varint(out, offset_delta);
        put_varint(out, key_len);
        if let Some(k) = r.key {
            out.extend_from_slice(k);
        }
        put_varint(out, r.value.len() as i64);
        out.extend_from_slice(r.value);
        put_varint(out, r.header_count() as i64);
        for (k, v) in r.headers() {
            put_varint(out, k.len() as i64);
            out.extend_from_slice(k);
            put_varint(out, v.len() as i64);
            out.extend_from_slice(v);
        }
    }
    let len = (out.len() - at - 12) as i32;
    out[at + 8..at + 12].copy_from_slice(&len.to_be_bytes());
    let crc = CRC32C.checksum(&out[crc_from..]);
    out[crc_from - 4..crc_from].copy_from_slice(&crc.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_round_trip() {
        for v in [0i64, 1, -1, 63, -64, 64, 300, -300, i32::MAX as i64, i64::MIN, i64::MAX] {
            let mut b = Vec::new();
            put_varint(&mut b, v);
            assert_eq!(b.len(), varint_len(v));
            let mut at = 0;
            assert_eq!(varint(&b, &mut at).unwrap(), v);
        }
    }

    #[test]
    fn a_batch_survives_both_ways() {
        let mut nb = BatchBuilder::new();
        nb.push_at(Some(b"k"), b"v1", &[(b"h", b"x")], 5_000_000).unwrap();
        nb.push_at(None, b"v2", &[], 5_001_000).unwrap();
        let mut stored = nb.finish();
        rasterizeddb_mq::batch::check_and_stamp(&mut stored, rasterizeddb_mq::batch::MAX_BATCH, 40, 0).unwrap();
        let mut kafka = Vec::new();
        encode_fetch(&stored, &mut kafka, 1 << 20);
        let back = decode_record_set(&kafka, 1 << 20).unwrap();
        assert_eq!(back.len(), 1);
        let v = BatchView::parse(&back[0].0).unwrap();
        let recs: Vec<_> = v.records().collect();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].key, Some(b"k".as_slice()));
        assert_eq!(recs[0].header(b"h"), Some(b"x".as_slice()));
        assert_eq!(recs[1].value, b"v2");
        assert_eq!(recs[1].time_us, 5_001_000);
    }
}
