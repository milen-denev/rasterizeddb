//! The bytes two instances exchange, and how they are framed.
//!
//! `rastcp` already gives a length-prefixed, TLS-protected request/response
//! channel, so this layer does not concern itself with framing on the wire or
//! with confidentiality. What it defines is the shape of one request and one
//! response: a fixed 12-byte header, then a payload that may be compressed.
//!
//! ```text
//! +--------+--------+--------+--------+
//! | 'R' 'R'| ver    | kind   | codec  |   magic, protocol version,
//! +--------+--------+--------+--------+   frame kind, payload codec
//! | flags  |    reserved     |            flags, padding to a 4-byte boundary
//! +--------+-----------------+
//! |            raw_len       |            payload length *before* compression
//! +--------------------------+
//! |         payload ...      |
//! +--------------------------+
//! ```
//!
//! Carrying `raw_len` in the header is what lets the receiver size the
//! decompression target exactly once, and lets it reject an implausible
//! expansion before spending any memory on it.
//!
//! Every integer is little-endian, matching the rest of the engine's on-disk
//! and on-wire encodings, so a decode is a load rather than a byte shuffle on
//! the platforms this runs on.

use std::fmt;

use super::config::{Codec, Compression, CompressionMode, NodeId};
use super::hlc::Hlc;

/// Identifies a replication frame and catches a client that dialled the SQL
/// port by mistake.
pub const MAGIC: [u8; 2] = *b"RR";

/// Bumped only for a change that an older peer could not parse. The handshake
/// compares it and refuses rather than misreading a frame.
pub const VERSION: u8 = 1;

pub const HEADER_LEN: usize = 12;

/// The largest payload a frame may claim, decompressed. Bounds what a peer can
/// make this node allocate from one 12-byte header.
pub const MAX_PAYLOAD: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    /// Opens a connection: who is calling, which cluster, what it can decode.
    Hello = 0,
    /// Answers it: who answered, which codec was chosen, how far it has got.
    HelloAck = 1,
    /// A batch of transactions to apply.
    Push = 2,
    /// What became of each of them.
    PushAck = 3,
    /// "Send me what I missed."
    Pull = 4,
    /// The answer to a `Pull`.
    PullAck = 5,
    /// Liveness, and a chance to exchange clocks.
    Ping = 6,
    Pong = 7,
    /// A refusal, with a reason a human can read.
    Error = 8,
}

impl FrameKind {
    pub fn from_wire(byte: u8) -> Option<FrameKind> {
        Some(match byte {
            0 => FrameKind::Hello,
            1 => FrameKind::HelloAck,
            2 => FrameKind::Push,
            3 => FrameKind::PushAck,
            4 => FrameKind::Pull,
            5 => FrameKind::PullAck,
            6 => FrameKind::Ping,
            7 => FrameKind::Pong,
            8 => FrameKind::Error,
            _ => return None,
        })
    }
}

impl fmt::Display for FrameKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            FrameKind::Hello => "HELLO",
            FrameKind::HelloAck => "HELLO-ACK",
            FrameKind::Push => "PUSH",
            FrameKind::PushAck => "PUSH-ACK",
            FrameKind::Pull => "PULL",
            FrameKind::PullAck => "PULL-ACK",
            FrameKind::Ping => "PING",
            FrameKind::Pong => "PONG",
            FrameKind::Error => "ERROR",
        })
    }
}

/// Set on a `PullAck` whose sender has more to give than fitted in one answer.
pub const FLAG_MORE: u8 = 1 << 0;
/// Set on a record that reached its sender from a third node rather than
/// originating there — a relayed transaction.
pub const FLAG_RELAYED: u8 = 1 << 0;

#[derive(Debug)]
pub enum ProtocolError {
    /// The first bytes were not a replication frame at all.
    NotAFrame,
    UnsupportedVersion(u8),
    UnknownFrame(u8),
    UnknownCodec(u8),
    /// A length field ran past the end of the buffer.
    Truncated,
    /// A payload claimed a size this node will not allocate.
    TooLarge(usize),
    /// A codec named in a frame is not compiled into this build.
    CodecUnavailable(Codec),
    Compression(String),
    /// A string field was not UTF-8.
    NotUtf8,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::NotAFrame => f.write_str("not a replication frame"),
            ProtocolError::UnsupportedVersion(v) => {
                write!(f, "replication protocol version {} is not supported", v)
            }
            ProtocolError::UnknownFrame(k) => write!(f, "unknown frame kind {}", k),
            ProtocolError::UnknownCodec(c) => write!(f, "unknown codec {}", c),
            ProtocolError::Truncated => f.write_str("frame ended in the middle of a field"),
            ProtocolError::TooLarge(len) => {
                write!(f, "payload of {} bytes exceeds the {} byte limit", len, MAX_PAYLOAD)
            }
            ProtocolError::CodecUnavailable(c) => {
                write!(f, "this build cannot decode {} payloads", c)
            }
            ProtocolError::Compression(e) => write!(f, "compression failed: {}", e),
            ProtocolError::NotUtf8 => f.write_str("a text field was not valid UTF-8"),
        }
    }
}

impl std::error::Error for ProtocolError {}

// ---------------------------------------------------------------------------
// Primitive encoding
// ---------------------------------------------------------------------------

#[inline]
fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

#[inline]
fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

#[inline]
fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

#[inline]
fn put_str(out: &mut Vec<u8>, value: &str) {
    put_u32(out, value.len() as u32);
    out.extend_from_slice(value.as_bytes());
}

/// A bounds-checked forward reader.
///
/// Every field read goes through it, so a malformed or truncated frame ends as
/// a `Truncated` error rather than a panic — this parses bytes that arrived
/// over a network, and an index-out-of-range in an accept loop is a way to take
/// the node down.
pub struct Reader<'a> {
    buf: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    #[inline]
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, at: 0 }
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.at
    }

    #[inline]
    fn take(&mut self, len: usize) -> Result<&'a [u8], ProtocolError> {
        let end = self.at.checked_add(len).ok_or(ProtocolError::Truncated)?;
        if end > self.buf.len() {
            return Err(ProtocolError::Truncated);
        }
        let slice = &self.buf[self.at..end];
        self.at = end;
        Ok(slice)
    }

    #[inline]
    pub fn u8(&mut self) -> Result<u8, ProtocolError> {
        Ok(self.take(1)?[0])
    }

    #[inline]
    pub fn u32(&mut self) -> Result<u32, ProtocolError> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    #[inline]
    pub fn u64(&mut self) -> Result<u64, ProtocolError> {
        let bytes = self.take(8)?;
        let mut array = [0u8; 8];
        array.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(array))
    }

    pub fn str(&mut self) -> Result<String, ProtocolError> {
        let len = self.u32()? as usize;
        // A length is checked against what is actually left before it is used
        // to reserve anything, so a bogus one costs nothing.
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes)
            .map(str::to_string)
            .map_err(|_| ProtocolError::NotUtf8)
    }
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

/// One committed transaction, as it travels.
///
/// The payload is the statements themselves rather than the rows they touched.
/// The engine has no physical redo stream that could be shipped instead: its
/// write-ahead log records byte ranges of specific files, and two nodes that
/// both accept writes lay their files out differently, so the same record would
/// mean different things on either side. Statements are the level at which the
/// two nodes agree, and applying one is a call the executor already offers.
///
/// The cost of that choice is determinism: a statement whose result depends on
/// something other than its text — `random()`, a wall-clock default — produces
/// different rows on each node. [`super::manager`] warns when it sees one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnRecord {
    /// The node where this transaction was committed by a client.
    pub origin: NodeId,
    /// Its position in that node's stream. Contiguous and gap-free per origin,
    /// which is what lets a peer ask for "everything after 41".
    pub seq: u64,
    /// When it committed, on the cluster's hybrid clock.
    pub commit_hlc: Hlc,
    /// [`FLAG_RELAYED`] and room for more.
    pub flags: u8,
    /// The statements, in the order they ran.
    pub statements: Vec<String>,
    /// The tables they wrote, deduplicated. Carried rather than re-derived so
    /// the last-writer-wins comparison does not have to re-parse SQL on the
    /// apply path.
    pub tables: Vec<String>,
}

impl TxnRecord {
    /// A cheap upper bound on the encoded size, used to decide when a batch is
    /// full without encoding it twice.
    pub fn encoded_size_hint(&self) -> usize {
        const FIXED: usize = 8 + 8 + 8 + 1 + 4 + 4;
        FIXED
            + self.statements.iter().map(|s| 4 + s.len()).sum::<usize>()
            + self.tables.iter().map(|t| 4 + t.len()).sum::<usize>()
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.reserve(self.encoded_size_hint());
        put_u64(out, self.origin);
        put_u64(out, self.seq);
        put_u64(out, self.commit_hlc);
        put_u8(out, self.flags);
        put_u32(out, self.statements.len() as u32);
        for statement in &self.statements {
            put_str(out, statement);
        }
        put_u32(out, self.tables.len() as u32);
        for table in &self.tables {
            put_str(out, table);
        }
    }

    pub fn decode(reader: &mut Reader<'_>) -> Result<TxnRecord, ProtocolError> {
        let origin = reader.u64()?;
        let seq = reader.u64()?;
        let commit_hlc = reader.u64()?;
        let flags = reader.u8()?;

        let statement_count = reader.u32()? as usize;
        // Cap the pre-allocation by what could possibly still be in the buffer:
        // the smallest statement is a 4-byte length, so more than remaining/4
        // of them cannot be there.
        let mut statements = Vec::with_capacity(statement_count.min(reader.remaining() / 4));
        for _ in 0..statement_count {
            statements.push(reader.str()?);
        }

        let table_count = reader.u32()? as usize;
        let mut tables = Vec::with_capacity(table_count.min(reader.remaining() / 4));
        for _ in 0..table_count {
            tables.push(reader.str()?);
        }

        Ok(TxnRecord { origin, seq, commit_hlc, flags, statements, tables })
    }

    #[inline]
    pub fn is_relayed(&self) -> bool {
        self.flags & FLAG_RELAYED != 0
    }
}

/// Encodes a batch: a count, then the records.
pub fn encode_records<'a, I>(records: I, count: usize, out: &mut Vec<u8>)
where
    I: IntoIterator<Item = &'a TxnRecord>,
{
    put_u32(out, count as u32);
    for record in records {
        record.encode(out);
    }
}

/// Encodes a `Push` payload: who is sending, then the batch.
///
/// The sender's identity is on the frame rather than inferred from the
/// connection because the handler is stateless — one function per frame, with
/// no session behind it — and relaying needs to know which peer a record
/// arrived from so it is not sent straight back.
pub fn encode_push<'a, I>(sender: NodeId, records: I, count: usize, out: &mut Vec<u8>)
where
    I: IntoIterator<Item = &'a TxnRecord>,
{
    put_u64(out, sender);
    encode_records(records, count, out);
}

pub fn decode_push(reader: &mut Reader<'_>) -> Result<(NodeId, Vec<TxnRecord>), ProtocolError> {
    let sender = reader.u64()?;
    let records = decode_records(reader)?;
    Ok((sender, records))
}

pub fn decode_records(reader: &mut Reader<'_>) -> Result<Vec<TxnRecord>, ProtocolError> {
    let count = reader.u32()? as usize;
    // A record cannot be smaller than its fixed part, so this bounds the
    // allocation by what the buffer could actually hold.
    let mut records = Vec::with_capacity(count.min(reader.remaining() / 33 + 1));
    for _ in 0..count {
        records.push(TxnRecord::decode(reader)?);
    }
    Ok(records)
}

// ---------------------------------------------------------------------------
// Handshake and control payloads
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hello {
    pub node_id: NodeId,
    pub cluster_id: u64,
    /// A bitmask of [`Codec::mask_bit`]s: what the caller can decode.
    pub codecs: u8,
    pub clock: Hlc,
    pub token: String,
    /// Where this node's own listener is, so the answerer can dial back
    /// without being configured to.
    pub advertised_addr: String,
}

impl Hello {
    pub fn encode(&self, out: &mut Vec<u8>) {
        put_u64(out, self.node_id);
        put_u64(out, self.cluster_id);
        put_u8(out, self.codecs);
        put_u64(out, self.clock);
        put_str(out, &self.token);
        put_str(out, &self.advertised_addr);
    }

    pub fn decode(reader: &mut Reader<'_>) -> Result<Hello, ProtocolError> {
        Ok(Hello {
            node_id: reader.u64()?,
            cluster_id: reader.u64()?,
            codecs: reader.u8()?,
            clock: reader.u64()?,
            token: reader.str()?,
            advertised_addr: reader.str()?,
        })
    }
}

/// One origin's high-water mark on the answering node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VectorEntry {
    pub origin: NodeId,
    pub seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HelloAck {
    pub node_id: NodeId,
    pub cluster_id: u64,
    /// The codec the answerer chose for what it sends back, and the one the
    /// caller should use from here on. Negotiation is one-shot and the
    /// answerer decides, so the two ends cannot disagree.
    pub codec: Codec,
    pub clock: Hlc,
    /// How far the answerer has applied from every origin it knows about. The
    /// caller uses it to decide what to resend before sending anything new.
    pub applied: Vec<VectorEntry>,
}

impl HelloAck {
    pub fn encode(&self, out: &mut Vec<u8>) {
        put_u64(out, self.node_id);
        put_u64(out, self.cluster_id);
        put_u8(out, self.codec.as_wire());
        put_u64(out, self.clock);
        put_u32(out, self.applied.len() as u32);
        for entry in &self.applied {
            put_u64(out, entry.origin);
            put_u64(out, entry.seq);
        }
    }

    pub fn decode(reader: &mut Reader<'_>) -> Result<HelloAck, ProtocolError> {
        let node_id = reader.u64()?;
        let cluster_id = reader.u64()?;
        let codec_byte = reader.u8()?;
        let codec = Codec::from_wire(codec_byte).ok_or(ProtocolError::UnknownCodec(codec_byte))?;
        let clock = reader.u64()?;

        let count = reader.u32()? as usize;
        let mut applied = Vec::with_capacity(count.min(reader.remaining() / 16));
        for _ in 0..count {
            applied.push(VectorEntry { origin: reader.u64()?, seq: reader.u64()? });
        }

        Ok(HelloAck { node_id, cluster_id, codec, clock, applied })
    }
}

/// What became of one pushed transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ApplyStatus {
    /// Committed on the receiver.
    Applied = 0,
    /// Already had it. Not a failure: a resend after a reconnect is expected
    /// to say this for everything the peer had already taken.
    Duplicate = 1,
    /// Refused by the conflict policy, or superseded under last-writer-wins.
    Conflict = 2,
    /// Would not apply for another reason; the message says which.
    Failed = 3,
    /// Arrived out of order — the receiver is missing an earlier sequence from
    /// this origin and cannot apply this one without creating a gap.
    Gap = 4,
}

impl ApplyStatus {
    pub fn from_wire(byte: u8) -> Option<ApplyStatus> {
        Some(match byte {
            0 => ApplyStatus::Applied,
            1 => ApplyStatus::Duplicate,
            2 => ApplyStatus::Conflict,
            3 => ApplyStatus::Failed,
            4 => ApplyStatus::Gap,
            _ => return None,
        })
    }

    /// Whether the sender may consider the record delivered. A duplicate counts:
    /// the receiver has it.
    #[inline]
    pub fn is_settled(self) -> bool {
        matches!(self, ApplyStatus::Applied | ApplyStatus::Duplicate)
    }
}

impl fmt::Display for ApplyStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ApplyStatus::Applied => "applied",
            ApplyStatus::Duplicate => "duplicate",
            ApplyStatus::Conflict => "conflict",
            ApplyStatus::Failed => "failed",
            ApplyStatus::Gap => "gap",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplyOutcome {
    pub origin: NodeId,
    pub seq: u64,
    pub status: ApplyStatus,
    /// Empty unless something went wrong, so the common case carries 4 bytes.
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushAck {
    pub node_id: NodeId,
    pub clock: Hlc,
    pub outcomes: Vec<ApplyOutcome>,
    /// The receiver's high-water mark per origin after applying this batch,
    /// which is how a sender that fell behind learns what to backfill without
    /// a separate round trip.
    pub applied: Vec<VectorEntry>,
}

impl PushAck {
    pub fn encode(&self, out: &mut Vec<u8>) {
        put_u64(out, self.node_id);
        put_u64(out, self.clock);
        put_u32(out, self.outcomes.len() as u32);
        for outcome in &self.outcomes {
            put_u64(out, outcome.origin);
            put_u64(out, outcome.seq);
            put_u8(out, outcome.status as u8);
            put_str(out, &outcome.message);
        }
        put_u32(out, self.applied.len() as u32);
        for entry in &self.applied {
            put_u64(out, entry.origin);
            put_u64(out, entry.seq);
        }
    }

    pub fn decode(reader: &mut Reader<'_>) -> Result<PushAck, ProtocolError> {
        let node_id = reader.u64()?;
        let clock = reader.u64()?;

        let count = reader.u32()? as usize;
        let mut outcomes = Vec::with_capacity(count.min(reader.remaining() / 21 + 1));
        for _ in 0..count {
            let origin = reader.u64()?;
            let seq = reader.u64()?;
            let status_byte = reader.u8()?;
            let status = ApplyStatus::from_wire(status_byte)
                .ok_or(ProtocolError::UnknownFrame(status_byte))?;
            let message = reader.str()?;
            outcomes.push(ApplyOutcome { origin, seq, status, message });
        }

        let count = reader.u32()? as usize;
        let mut applied = Vec::with_capacity(count.min(reader.remaining() / 16));
        for _ in 0..count {
            applied.push(VectorEntry { origin: reader.u64()?, seq: reader.u64()? });
        }

        Ok(PushAck { node_id, clock, outcomes, applied })
    }
}

/// "Send me everything you have from `origin` after `after_seq`."
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pull {
    pub node_id: NodeId,
    pub origin: NodeId,
    pub after_seq: u64,
    pub max_records: u32,
    pub max_bytes: u32,
}

impl Pull {
    pub fn encode(&self, out: &mut Vec<u8>) {
        put_u64(out, self.node_id);
        put_u64(out, self.origin);
        put_u64(out, self.after_seq);
        put_u32(out, self.max_records);
        put_u32(out, self.max_bytes);
    }

    pub fn decode(reader: &mut Reader<'_>) -> Result<Pull, ProtocolError> {
        Ok(Pull {
            node_id: reader.u64()?,
            origin: reader.u64()?,
            after_seq: reader.u64()?,
            max_records: reader.u32()?,
            max_bytes: reader.u32()?,
        })
    }
}

// ---------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub kind: FrameKind,
    pub codec: Codec,
    pub flags: u8,
    pub raw_len: usize,
}

pub fn parse_header(frame: &[u8]) -> Result<Header, ProtocolError> {
    if frame.len() < HEADER_LEN {
        return Err(ProtocolError::Truncated);
    }
    if frame[0] != MAGIC[0] || frame[1] != MAGIC[1] {
        return Err(ProtocolError::NotAFrame);
    }
    if frame[2] != VERSION {
        return Err(ProtocolError::UnsupportedVersion(frame[2]));
    }

    let kind = FrameKind::from_wire(frame[3]).ok_or(ProtocolError::UnknownFrame(frame[3]))?;
    let codec = Codec::from_wire(frame[4]).ok_or(ProtocolError::UnknownCodec(frame[4]))?;
    let flags = frame[5];
    let raw_len = u32::from_le_bytes([frame[8], frame[9], frame[10], frame[11]]) as usize;

    if raw_len > MAX_PAYLOAD {
        return Err(ProtocolError::TooLarge(raw_len));
    }

    Ok(Header { kind, codec, flags, raw_len })
}

/// Decides whether a payload of this size is worth compressing.
///
/// `recent_ratio` is the compressed/uncompressed ratio this link last observed,
/// or `None` before it has sent anything. Under [`CompressionMode::Auto`] a
/// link whose traffic stopped compressing — already-compressed blobs, say —
/// stops paying for the attempt.
#[inline]
pub fn should_compress(compression: Compression, len: usize, recent_ratio: Option<f64>) -> bool {
    if compression.codec == Codec::None || !compression.codec.is_available() {
        return false;
    }
    if len < compression.min_bytes {
        return false;
    }
    match compression.mode {
        CompressionMode::Off => false,
        CompressionMode::Always => true,
        CompressionMode::Auto => match recent_ratio {
            None => true,
            Some(ratio) => ratio <= compression.min_ratio,
        },
    }
}

/// Holds the compression contexts for one link.
///
/// Zstd's context owns its window and hash tables; creating one per frame would
/// mean an allocation and a table clear on every batch. A peer's sender task
/// keeps one of these for its lifetime, so the steady state allocates only the
/// output buffer — and that comes from the caller.
pub struct FrameCodec {
    level: i32,
    #[cfg(feature = "replication_compression")]
    compressor: Option<zstd::bulk::Compressor<'static>>,
    #[cfg(feature = "replication_compression")]
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
}

impl fmt::Debug for FrameCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrameCodec").field("level", &self.level).finish()
    }
}

impl Default for FrameCodec {
    fn default() -> Self {
        FrameCodec::new(Compression::default().level)
    }
}

impl FrameCodec {
    pub fn new(level: i32) -> Self {
        FrameCodec {
            level,
            #[cfg(feature = "replication_compression")]
            compressor: None,
            #[cfg(feature = "replication_compression")]
            decompressor: None,
        }
    }

    /// Rebuilds the compressor if the level changed under a runtime
    /// `REPLICATION SET COMPRESSION`.
    pub fn set_level(&mut self, level: i32) {
        if level != self.level {
            self.level = level;
            #[cfg(feature = "replication_compression")]
            {
                self.compressor = None;
            }
        }
    }

    /// Writes a complete frame into `out`, which is cleared first.
    ///
    /// `payload` is taken as a slice rather than a `Vec`, so a caller may build
    /// it in a buffer it reuses.
    pub fn encode_frame(
        &mut self,
        kind: FrameKind,
        flags: u8,
        payload: &[u8],
        compression: Compression,
        recent_ratio: Option<f64>,
        out: &mut Vec<u8>,
    ) -> Result<Codec, ProtocolError> {
        if payload.len() > MAX_PAYLOAD {
            return Err(ProtocolError::TooLarge(payload.len()));
        }

        out.clear();
        out.reserve(HEADER_LEN + payload.len());
        out.extend_from_slice(&MAGIC);
        out.push(VERSION);
        out.push(kind as u8);
        // Patched below once the codec is known; compressing straight into
        // `out` past the header is what avoids a second copy.
        out.push(Codec::None.as_wire());
        out.push(flags);
        out.extend_from_slice(&[0u8, 0u8]);
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());

        let mut codec = Codec::None;
        if should_compress(compression, payload.len(), recent_ratio) {
            match self.compress_into(compression.codec, payload, out) {
                Ok(true) => codec = compression.codec,
                // Compression that expanded the payload is not worth sending;
                // fall through and write the original bytes.
                Ok(false) => {}
                Err(e) => return Err(e),
            }
        }

        if codec == Codec::None {
            out.truncate(HEADER_LEN);
            out.extend_from_slice(payload);
        }

        out[4] = codec.as_wire();
        Ok(codec)
    }

    /// Compresses into `out` past the header. Returns false when the result was
    /// not smaller than the input, in which case the caller rewrites `out`.
    #[allow(unused_variables)]
    fn compress_into(
        &mut self,
        codec: Codec,
        payload: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<bool, ProtocolError> {
        match codec {
            Codec::None => Ok(false),
            Codec::Zstd => {
                #[cfg(feature = "replication_compression")]
                {
                    if self.compressor.is_none() {
                        self.compressor = Some(
                            zstd::bulk::Compressor::new(self.level)
                                .map_err(|e| ProtocolError::Compression(e.to_string()))?,
                        );
                    }
                    let compressor = self.compressor.as_mut().expect("just built");

                    let bound = zstd::zstd_safe::compress_bound(payload.len());
                    let start = out.len();
                    out.resize(start + bound, 0);
                    let written = compressor
                        .compress_to_buffer(payload, &mut out[start..])
                        .map_err(|e| ProtocolError::Compression(e.to_string()))?;
                    out.truncate(start + written);

                    Ok(written < payload.len())
                }
                #[cfg(not(feature = "replication_compression"))]
                {
                    Err(ProtocolError::CodecUnavailable(Codec::Zstd))
                }
            }
        }
    }

    /// Reads a frame, decompressing into `scratch` when it needs to.
    ///
    /// The returned slice borrows either the frame itself (uncompressed, so no
    /// copy at all) or `scratch`. That is why `scratch` is a parameter: a
    /// connection reuses one buffer for every frame it reads.
    pub fn decode_frame<'a>(
        &mut self,
        frame: &'a [u8],
        scratch: &'a mut Vec<u8>,
    ) -> Result<(Header, &'a [u8]), ProtocolError> {
        let header = parse_header(frame)?;
        let body = &frame[HEADER_LEN..];

        match header.codec {
            Codec::None => {
                if body.len() != header.raw_len {
                    return Err(ProtocolError::Truncated);
                }
                Ok((header, body))
            }
            Codec::Zstd => {
                #[cfg(feature = "replication_compression")]
                {
                    if self.decompressor.is_none() {
                        self.decompressor = Some(
                            zstd::bulk::Decompressor::new()
                                .map_err(|e| ProtocolError::Compression(e.to_string()))?,
                        );
                    }
                    let decompressor = self.decompressor.as_mut().expect("just built");

                    scratch.clear();
                    scratch.resize(header.raw_len, 0);
                    let written = decompressor
                        .decompress_to_buffer(body, &mut scratch[..])
                        .map_err(|e| ProtocolError::Compression(e.to_string()))?;
                    if written != header.raw_len {
                        // The header is the receiver's only allocation budget,
                        // so a payload that does not match it is a bug or an
                        // attack, not something to accept and carry on with.
                        return Err(ProtocolError::Truncated);
                    }
                    Ok((header, &scratch[..]))
                }
                #[cfg(not(feature = "replication_compression"))]
                {
                    let _ = scratch;
                    Err(ProtocolError::CodecUnavailable(Codec::Zstd))
                }
            }
        }
    }
}

/// Builds a frame with no compression and no reusable state, for the handshake
/// and other one-off control frames where a context would not be reused.
pub fn plain_frame(kind: FrameKind, flags: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.push(kind as u8);
    out.push(Codec::None.as_wire());
    out.push(flags);
    out.extend_from_slice(&[0u8, 0u8]);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// An `Error` frame carrying a reason.
pub fn error_frame(message: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + message.len());
    put_str(&mut payload, message);
    plain_frame(FrameKind::Error, 0, &payload)
}

/// Reads the message out of an `Error` frame's payload.
pub fn decode_error(payload: &[u8]) -> String {
    Reader::new(payload)
        .str()
        .unwrap_or_else(|_| "peer reported an unreadable error".to_string())
}

/// The codec both ends can use, preferring the best available.
pub fn negotiate(local_mask: u8, remote_mask: u8, preferred: Codec) -> Codec {
    let shared = local_mask & remote_mask;

    if preferred.is_available() && shared & preferred.mask_bit() != 0 {
        return preferred;
    }
    for codec in Codec::preference_order() {
        if codec.is_available() && shared & codec.mask_bit() != 0 {
            return *codec;
        }
    }
    Codec::None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(seq: u64) -> TxnRecord {
        TxnRecord {
            origin: 7,
            seq,
            commit_hlc: 0x1234_5678_9abc,
            flags: 0,
            statements: vec![
                "INSERT INTO users (id, name) VALUES (1, 'ada')".to_string(),
                "UPDATE users SET name = 'grace' WHERE id = 1".to_string(),
            ],
            tables: vec!["users".to_string()],
        }
    }

    #[test]
    fn a_record_survives_a_round_trip() {
        let original = record(42);
        let mut buf = Vec::new();
        original.encode(&mut buf);

        let decoded = TxnRecord::decode(&mut Reader::new(&buf)).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn a_batch_survives_a_round_trip() {
        let records: Vec<TxnRecord> = (1..=5).map(record).collect();
        let mut buf = Vec::new();
        encode_records(records.iter(), records.len(), &mut buf);

        assert_eq!(decode_records(&mut Reader::new(&buf)).unwrap(), records);
    }

    #[test]
    fn a_truncated_record_is_an_error_and_not_a_panic() {
        let mut buf = Vec::new();
        record(1).encode(&mut buf);

        for cut in 0..buf.len() {
            let result = TxnRecord::decode(&mut Reader::new(&buf[..cut]));
            assert!(result.is_err(), "a {}-byte prefix decoded as a record", cut);
        }
    }

    #[test]
    fn a_lying_length_prefix_does_not_allocate_what_it_claims() {
        // A statement count of four billion in a small buffer.
        let mut buf = Vec::new();
        buf.extend_from_slice(&7u64.to_le_bytes());
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.push(0);
        buf.extend_from_slice(&u32::MAX.to_le_bytes());

        assert!(matches!(
            TxnRecord::decode(&mut Reader::new(&buf)),
            Err(ProtocolError::Truncated)
        ));
    }

    #[test]
    fn an_uncompressed_frame_is_returned_without_a_copy() {
        let mut codec = FrameCodec::new(1);
        let payload = b"a short payload";
        let mut frame = Vec::new();
        let used = codec
            .encode_frame(
                FrameKind::Push,
                0,
                payload,
                Compression { mode: CompressionMode::Off, ..Default::default() },
                None,
                &mut frame,
            )
            .unwrap();
        assert_eq!(used, Codec::None);

        let mut scratch = Vec::new();
        let (header, body) = codec.decode_frame(&frame, &mut scratch).unwrap();
        assert_eq!(header.kind, FrameKind::Push);
        assert_eq!(body, payload);
        assert!(scratch.is_empty(), "an uncompressed frame should not touch the scratch buffer");
    }

    #[test]
    #[cfg(feature = "replication_compression")]
    fn a_compressed_batch_round_trips_and_is_smaller() {
        let records: Vec<TxnRecord> = (1..=200).map(record).collect();
        let mut payload = Vec::new();
        encode_records(records.iter(), records.len(), &mut payload);

        let mut codec = FrameCodec::new(1);
        let mut frame = Vec::new();
        let used = codec
            .encode_frame(
                FrameKind::Push,
                0,
                &payload,
                Compression { mode: CompressionMode::Always, ..Default::default() },
                None,
                &mut frame,
            )
            .unwrap();

        assert_eq!(used, Codec::Zstd);
        assert!(
            frame.len() < payload.len() / 2,
            "repetitive SQL should compress well: {} -> {}",
            payload.len(),
            frame.len()
        );

        let mut scratch = Vec::new();
        let (header, body) = codec.decode_frame(&frame, &mut scratch).unwrap();
        assert_eq!(header.raw_len, payload.len());
        assert_eq!(decode_records(&mut Reader::new(body)).unwrap(), records);
    }

    #[test]
    fn a_tiny_payload_is_left_uncompressed() {
        assert!(!should_compress(Compression::default(), 8, None));
    }

    #[test]
    fn auto_mode_stops_compressing_once_it_stops_paying() {
        let compression = Compression { mode: CompressionMode::Auto, ..Default::default() };
        assert!(
            !should_compress(compression, 100_000, Some(0.99)),
            "a 1% saving is not worth the CPU"
        );
        if Codec::Zstd.is_available() {
            assert!(
                should_compress(compression, 100_000, Some(0.3)),
                "a 3x saving is worth keeping"
            );
        }
    }

    #[test]
    fn a_frame_from_another_protocol_is_rejected() {
        let mut junk = b"GET / HTTP/1.1\r\n\r\n".to_vec();
        junk.resize(64, 0);
        assert!(matches!(parse_header(&junk), Err(ProtocolError::NotAFrame)));
    }

    #[test]
    fn a_newer_protocol_version_is_refused_rather_than_misread() {
        let mut frame = plain_frame(FrameKind::Ping, 0, &[]);
        frame[2] = VERSION + 1;
        assert!(matches!(
            parse_header(&frame),
            Err(ProtocolError::UnsupportedVersion(_))
        ));
    }

    #[test]
    fn an_absurd_declared_length_is_refused_before_allocating() {
        let mut frame = plain_frame(FrameKind::Push, 0, &[]);
        frame[8..12].copy_from_slice(&(MAX_PAYLOAD as u32 + 1).to_le_bytes());
        assert!(matches!(parse_header(&frame), Err(ProtocolError::TooLarge(_))));
    }

    #[test]
    fn the_handshake_falls_back_to_uncompressed_against_a_peer_that_cannot_compress() {
        let chosen = negotiate(Codec::local_mask(), Codec::None.mask_bit(), Codec::Zstd);
        assert_eq!(chosen, Codec::None);
    }

    #[test]
    fn the_handshake_keeps_the_preferred_codec_when_both_ends_have_it() {
        let chosen = negotiate(Codec::local_mask(), Codec::local_mask(), Codec::Zstd);
        if Codec::Zstd.is_available() {
            assert_eq!(chosen, Codec::Zstd);
        } else {
            assert_eq!(chosen, Codec::None);
        }
    }

    #[test]
    fn handshake_payloads_round_trip() {
        let hello = Hello {
            node_id: 3,
            cluster_id: 99,
            codecs: Codec::local_mask(),
            clock: 1234,
            token: "s3cret".to_string(),
            advertised_addr: "10.0.0.1:61171".to_string(),
        };
        let mut buf = Vec::new();
        hello.encode(&mut buf);
        assert_eq!(Hello::decode(&mut Reader::new(&buf)).unwrap(), hello);

        let ack = HelloAck {
            node_id: 4,
            cluster_id: 99,
            codec: Codec::None,
            clock: 5678,
            applied: vec![VectorEntry { origin: 3, seq: 17 }],
        };
        let mut buf = Vec::new();
        ack.encode(&mut buf);
        assert_eq!(HelloAck::decode(&mut Reader::new(&buf)).unwrap(), ack);
    }

    #[test]
    fn a_push_ack_round_trips_with_its_failures() {
        let ack = PushAck {
            node_id: 2,
            clock: 42,
            outcomes: vec![
                ApplyOutcome { origin: 1, seq: 1, status: ApplyStatus::Applied, message: String::new() },
                ApplyOutcome {
                    origin: 1,
                    seq: 2,
                    status: ApplyStatus::Conflict,
                    message: "duplicate key".to_string(),
                },
            ],
            applied: vec![VectorEntry { origin: 1, seq: 1 }],
        };
        let mut buf = Vec::new();
        ack.encode(&mut buf);
        assert_eq!(PushAck::decode(&mut Reader::new(&buf)).unwrap(), ack);
    }

    #[test]
    fn an_error_frame_carries_its_reason() {
        let frame = error_frame("cluster mismatch");
        let header = parse_header(&frame).unwrap();
        assert_eq!(header.kind, FrameKind::Error);
        assert_eq!(decode_error(&frame[HEADER_LEN..]), "cluster mismatch");
    }
}
