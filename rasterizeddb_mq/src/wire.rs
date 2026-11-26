//! The native broker protocol, `rdbmq/1` (docs/broker.md §6).
//!
//! Every frame is a 12-byte header and a body:
//!
//! ```text
//! len u32 (bytes after this field) | op u8 | flags u8 | reserved u16 | corr u32 | body
//! ```
//!
//! A reply carries its request's `corr` and op, with [`F_REPLY`] (and
//! [`F_ERROR`] when it failed: the body is then [`ErrorBody`]). Replies come
//! back in any order, so one connection carries any number of requests at
//! once. Deliveries the server pushes have op [`op::DELIVER`] and carry the
//! subscription's handle as `corr`.
//!
//! Every message is defined once, here, with its encoder and decoder: the
//! server and the client share these, so they cannot disagree on a layout.
//! Strings are `u16` length + UTF-8, blobs are `u32` length + bytes, and every
//! integer is little-endian.

use std::fmt;

pub const VERSION: u16 = 1;
pub const FRAME_HEADER: usize = 12;
/// The largest frame either side accepts.
pub const MAX_FRAME: usize = (1 << 30) + 4096;
/// The default port of `--broker-port`.
pub const DEFAULT_PORT: u16 = 7171;

pub const F_REPLY: u8 = 1;
pub const F_ERROR: u8 = 2;
pub const F_PUSH: u8 = 4;

pub mod op {
    pub const HELLO: u8 = 1;
    pub const AUTH: u8 = 2;
    pub const PING: u8 = 3;
    pub const TOPIC_CREATE: u8 = 10;
    pub const TOPIC_DELETE: u8 = 11;
    pub const TOPIC_LIST: u8 = 12;
    pub const TOPIC_ALTER: u8 = 13;
    pub const RESOLVE: u8 = 14;
    pub const PRODUCE: u8 = 20;
    pub const FETCH: u8 = 21;
    pub const LIST_OFFSETS: u8 = 22;
    pub const SUBSCRIBE: u8 = 30;
    pub const CREDIT: u8 = 31;
    pub const ACK: u8 = 32;
    pub const NACK: u8 = 33;
    pub const COMMIT: u8 = 34;
    pub const UNSUBSCRIBE: u8 = 35;
    pub const SUB_DELETE: u8 = 36;
    pub const SUB_LIST: u8 = 37;
    pub const EXCHANGE_DECLARE: u8 = 40;
    pub const EXCHANGE_DELETE: u8 = 41;
    pub const BIND: u8 = 42;
    pub const UNBIND: u8 = 43;
    pub const PUBLISH: u8 = 44;
    pub const QUEUE_DECLARE: u8 = 45;
    pub const QUEUE_DELETE: u8 = 46;
    /// Pushed: a message batch for a subscription (`corr` = its handle).
    pub const DELIVER: u8 = 0x80;
    /// Pushed: an ordered subscription's partitions changed (`corr` = its
    /// handle); body: generation u64, count u32, partition u32 × count.
    pub const ASSIGN: u8 = 0x81;
    /// Pushed: the subscription ended (`corr` = its handle); body: [`ErrorBody`].
    pub const CLOSED: u8 = 0x82;
}

/// Error codes a failed reply carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum Code {
    Unknown = 1,
    Protocol = 2,
    AuthRequired = 3,
    AuthFailed = 4,
    Permission = 5,
    NoTopic = 6,
    TopicExists = 7,
    NoPartition = 8,
    BadBatch = 9,
    TooLarge = 10,
    OffsetOutOfRange = 11,
    NoSubscription = 12,
    NoExchange = 13,
    /// Declared again with different settings.
    Mismatch = 14,
    Unroutable = 15,
    Storage = 16,
    ShuttingDown = 17,
    Invalid = 18,
    /// A producer epoch older than the one the partition has seen.
    Fenced = 19,
    /// A sequence gap from an idempotent producer.
    OutOfOrder = 20,
    Unsupported = 21,
    /// A queue at `max_length` with `overflow = reject`.
    Full = 22,
}

impl Code {
    pub fn from_u16(v: u16) -> Code {
        use Code::*;
        match v {
            2 => Protocol,
            3 => AuthRequired,
            4 => AuthFailed,
            5 => Permission,
            6 => NoTopic,
            7 => TopicExists,
            8 => NoPartition,
            9 => BadBatch,
            10 => TooLarge,
            11 => OffsetOutOfRange,
            12 => NoSubscription,
            13 => NoExchange,
            14 => Mismatch,
            15 => Unroutable,
            16 => Storage,
            17 => ShuttingDown,
            18 => Invalid,
            19 => Fenced,
            20 => OutOfOrder,
            21 => Unsupported,
            22 => Full,
            _ => Unknown,
        }
    }
}

/// A frame's header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    /// Bytes of body.
    pub body_len: usize,
    pub op: u8,
    pub flags: u8,
    pub corr: u32,
}

/// The frame at the start of `buf`: `None` until all of it is in, `Err` when
/// its length is impossible. On `Some`, the body is
/// `buf[FRAME_HEADER..FRAME_HEADER + header.body_len]`.
#[inline]
pub fn parse_header(buf: &[u8]) -> Result<Option<Header>, DecodeError> {
    if buf.len() < FRAME_HEADER {
        return Ok(None);
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    if len < FRAME_HEADER - 4 || len > MAX_FRAME {
        return Err(DecodeError("frame length out of range"));
    }
    if buf.len() < 4 + len {
        return Ok(None);
    }
    Ok(Some(Header {
        body_len: len - (FRAME_HEADER - 4),
        op: buf[4],
        flags: buf[5],
        corr: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
    }))
}

/// Bytes the frame at the start of `buf` needs in all, once its length is in.
#[inline]
pub fn frame_need(buf: &[u8]) -> Option<usize> {
    (buf.len() >= 4).then(|| 4 + u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize)
}

/// Starts a frame in `out`; [`end_frame`] fills its length.
#[inline]
pub fn begin_frame(out: &mut Vec<u8>, op: u8, flags: u8, corr: u32) -> usize {
    let start = out.len();
    out.extend_from_slice(&[0, 0, 0, 0, op, flags, 0, 0]);
    out.extend_from_slice(&corr.to_le_bytes());
    start
}

#[inline]
pub fn end_frame(out: &mut [u8], start: usize) {
    let len = (out.len() - start - 4) as u32;
    out[start..start + 4].copy_from_slice(&len.to_le_bytes());
}

/// A whole frame, built by `body`.
pub fn frame(out: &mut Vec<u8>, op: u8, flags: u8, corr: u32, body: impl FnOnce(&mut Writer<'_>)) {
    let start = begin_frame(out, op, flags, corr);
    body(&mut Writer(out));
    end_frame(out, start);
}

/// A failed reply.
pub fn error_frame(out: &mut Vec<u8>, op: u8, corr: u32, code: Code, message: &str) {
    frame(out, op, F_REPLY | F_ERROR, corr, |w| {
        ErrorBody { code, message: message.to_string() }.encode(w)
    });
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeError(pub &'static str);

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "malformed frame: {}", self.0)
    }
}

impl std::error::Error for DecodeError {}

/// Appends little-endian values to a buffer.
pub struct Writer<'a>(pub &'a mut Vec<u8>);

impl Writer<'_> {
    #[inline]
    pub fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    #[inline]
    pub fn u16(&mut self, v: u16) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    pub fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    pub fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    pub fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    pub fn bool(&mut self, v: bool) {
        self.0.push(v as u8);
    }
    /// A string of at most 65535 bytes; longer is cut at a char boundary.
    pub fn str(&mut self, s: &str) {
        let mut n = s.len().min(u16::MAX as usize);
        while !s.is_char_boundary(n) {
            n -= 1;
        }
        self.u16(n as u16);
        self.0.extend_from_slice(&s.as_bytes()[..n]);
    }
    pub fn blob(&mut self, b: &[u8]) {
        self.u32(b.len() as u32);
        self.0.extend_from_slice(b);
    }
    /// Bytes with no length: the rest of the body.
    #[inline]
    pub fn raw(&mut self, b: &[u8]) {
        self.0.extend_from_slice(b);
    }
}

/// Reads little-endian values from a body.
#[derive(Clone, Copy)]
pub struct Reader<'a> {
    b: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Reader { b }
    }
    #[inline]
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.b.len() < n {
            return Err(DecodeError("body too short"));
        }
        let (h, t) = self.b.split_at(n);
        self.b = t;
        Ok(h)
    }
    #[inline]
    pub fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }
    #[inline]
    pub fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    #[inline]
    pub fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    #[inline]
    pub fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    #[inline]
    pub fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(self.u64()? as i64)
    }
    #[inline]
    pub fn bool(&mut self) -> Result<bool, DecodeError> {
        Ok(self.u8()? != 0)
    }
    pub fn str(&mut self) -> Result<&'a str, DecodeError> {
        let n = self.u16()? as usize;
        std::str::from_utf8(self.take(n)?).map_err(|_| DecodeError("string is not UTF-8"))
    }
    pub fn string(&mut self) -> Result<String, DecodeError> {
        self.str().map(str::to_string)
    }
    pub fn blob(&mut self) -> Result<&'a [u8], DecodeError> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    /// Everything left.
    pub fn rest(&mut self) -> &'a [u8] {
        std::mem::take(&mut self.b)
    }
    pub fn is_empty(&self) -> bool {
        self.b.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Settings shared by topics and queues
// ---------------------------------------------------------------------------

/// How a topic's appends are made durable (docs/broker.md §1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum Durability {
    /// Acked once `fdatasync`ed, by a group commit.
    #[default]
    Sync = 0,
    /// Acked once in the page cache; synced every `broker.flush_ms`.
    Async = 1,
    /// Never written: a bounded in-memory ring.
    Memory = 2,
}

impl Durability {
    pub fn from_u8(v: u8) -> Option<Durability> {
        Some(match v {
            0 => Durability::Sync,
            1 => Durability::Async,
            2 => Durability::Memory,
            _ => return None,
        })
    }
    pub fn name(self) -> &'static str {
        match self {
            Durability::Sync => "sync",
            Durability::Async => "async",
            Durability::Memory => "memory",
        }
    }
    pub fn parse(s: &str) -> Option<Durability> {
        Some(match s.trim().to_ascii_lowercase().as_str() {
            "sync" | "durable" => Durability::Sync,
            "async" => Durability::Async,
            "memory" | "transient" => Durability::Memory,
            _ => return None,
        })
    }
}

/// A topic's settings. `-1` in a retention field means unbounded.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopicConfig {
    pub partitions: u32,
    pub durability: Durability,
    pub retention_ms: i64,
    pub retention_bytes: i64,
    pub segment_bytes: u64,
    pub max_batch_bytes: u32,
    /// A `memory` topic's ring per partition.
    pub memory_bytes: u64,
}

impl Default for TopicConfig {
    fn default() -> Self {
        TopicConfig {
            partitions: 1,
            durability: Durability::Sync,
            retention_ms: 7 * 24 * 3600 * 1000,
            retention_bytes: -1,
            segment_bytes: 256 << 20,
            max_batch_bytes: 16 << 20,
            memory_bytes: 64 << 20,
        }
    }
}

impl TopicConfig {
    pub fn encode(&self, w: &mut Writer<'_>) {
        w.u32(self.partitions);
        w.u8(self.durability as u8);
        w.i64(self.retention_ms);
        w.i64(self.retention_bytes);
        w.u64(self.segment_bytes);
        w.u32(self.max_batch_bytes);
        w.u64(self.memory_bytes);
    }
    pub fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(TopicConfig {
            partitions: r.u32()?,
            durability: Durability::from_u8(r.u8()?).ok_or(DecodeError("unknown durability"))?,
            retention_ms: r.i64()?,
            retention_bytes: r.i64()?,
            segment_bytes: r.u64()?,
            max_batch_bytes: r.u32()?,
            memory_bytes: r.u64()?,
        })
    }
}

/// How a subscription hands messages out (docs/broker.md §1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SubMode {
    /// A committed offset per partition; partitions shared among members.
    Ordered = 0,
    /// Competing consumers, an ack per message, redelivery.
    Shared = 1,
    /// A cursor per connection, in memory, from the tail.
    Broadcast = 2,
}

impl SubMode {
    pub fn from_u8(v: u8) -> Option<SubMode> {
        Some(match v {
            0 => SubMode::Ordered,
            1 => SubMode::Shared,
            2 => SubMode::Broadcast,
            _ => return None,
        })
    }
    pub fn name(self) -> &'static str {
        match self {
            SubMode::Ordered => "ordered",
            SubMode::Shared => "shared",
            SubMode::Broadcast => "broadcast",
        }
    }
    pub fn parse(s: &str) -> Option<SubMode> {
        Some(match s.trim().to_ascii_lowercase().as_str() {
            "ordered" | "stream" | "group" => SubMode::Ordered,
            "shared" | "queue" | "work" => SubMode::Shared,
            "broadcast" | "pubsub" => SubMode::Broadcast,
            _ => return None,
        })
    }
}

/// Where a subscription that has no position yet starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Start {
    Earliest,
    Latest,
    Offset(u64),
    /// The first message at or after this time, µs since the epoch.
    Time(i64),
}

impl Start {
    pub fn encode(&self, w: &mut Writer<'_>) {
        match *self {
            Start::Earliest => {
                w.u8(0);
                w.i64(0)
            }
            Start::Latest => {
                w.u8(1);
                w.i64(0)
            }
            Start::Offset(o) => {
                w.u8(2);
                w.u64(o)
            }
            Start::Time(t) => {
                w.u8(3);
                w.i64(t)
            }
        }
    }
    pub fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let kind = r.u8()?;
        let v = r.i64()?;
        Ok(match kind {
            0 => Start::Earliest,
            1 => Start::Latest,
            2 => Start::Offset(v as u64),
            3 => Start::Time(v),
            _ => return Err(DecodeError("unknown start")),
        })
    }
}

/// A shared subscription's settings, fixed when it is first made.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SubConfig {
    pub mode: SubMode,
    pub start: Start,
    /// A delivery not acked within this is redelivered (shared).
    pub ack_timeout_ms: u32,
    /// Deliveries before a message goes to `dead_letter` (shared; 0 = no limit).
    pub max_deliveries: u32,
    /// A topic name, or empty: dropped after `max_deliveries`.
    pub dead_letter: String,
}

impl Default for SubConfig {
    fn default() -> Self {
        SubConfig {
            mode: SubMode::Shared,
            start: Start::Earliest,
            ack_timeout_ms: 30_000,
            max_deliveries: 0,
            dead_letter: String::new(),
        }
    }
}

impl SubConfig {
    pub fn encode(&self, w: &mut Writer<'_>) {
        w.u8(self.mode as u8);
        self.start.encode(w);
        w.u32(self.ack_timeout_ms);
        w.u32(self.max_deliveries);
        w.str(&self.dead_letter);
    }
    pub fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(SubConfig {
            mode: SubMode::from_u8(r.u8()?).ok_or(DecodeError("unknown mode"))?,
            start: Start::decode(r)?,
            ack_timeout_ms: r.u32()?,
            max_deliveries: r.u32()?,
            dead_letter: r.string()?,
        })
    }
}

/// An exchange's routing rule (docs/broker.md §5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ExchangeKind {
    Direct = 0,
    Fanout = 1,
    /// AMQP words: `.`-separated, `*` one word, `#` zero or more.
    Topic = 2,
    Headers = 3,
    /// MQTT filters: `/`-separated, `+` one level, `#` the rest.
    Mqtt = 4,
}

impl ExchangeKind {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => ExchangeKind::Direct,
            1 => ExchangeKind::Fanout,
            2 => ExchangeKind::Topic,
            3 => ExchangeKind::Headers,
            4 => ExchangeKind::Mqtt,
            _ => return None,
        })
    }
    pub fn name(self) -> &'static str {
        match self {
            ExchangeKind::Direct => "direct",
            ExchangeKind::Fanout => "fanout",
            ExchangeKind::Topic => "topic",
            ExchangeKind::Headers => "headers",
            ExchangeKind::Mqtt => "mqtt",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.trim().to_ascii_lowercase().as_str() {
            "direct" => ExchangeKind::Direct,
            "fanout" => ExchangeKind::Fanout,
            "topic" => ExchangeKind::Topic,
            "headers" => ExchangeKind::Headers,
            "mqtt" => ExchangeKind::Mqtt,
            _ => return None,
        })
    }
}

/// A headers binding's rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum XMatch {
    #[default]
    All = 0,
    Any = 1,
}

/// One binding: messages routed by `key` (and `args` for headers) go to
/// `topic`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Binding {
    pub exchange: String,
    pub topic: String,
    pub key: String,
    pub x_match: XMatch,
    pub args: Vec<(String, Vec<u8>)>,
}

impl Binding {
    pub fn encode(&self, w: &mut Writer<'_>) {
        w.str(&self.exchange);
        w.str(&self.topic);
        w.str(&self.key);
        w.u8(self.x_match as u8);
        w.u16(self.args.len() as u16);
        for (k, v) in &self.args {
            w.str(k);
            w.blob(v);
        }
    }
    pub fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let exchange = r.string()?;
        let topic = r.string()?;
        let key = r.string()?;
        let x_match = if r.u8()? == 1 { XMatch::Any } else { XMatch::All };
        let n = r.u16()? as usize;
        let mut args = Vec::with_capacity(n.min(64));
        for _ in 0..n {
            args.push((r.string()?, r.blob()?.to_vec()));
        }
        Ok(Binding { exchange, topic, key, x_match, args })
    }
}

/// A queue: a one-partition topic and a shared subscription of the same name.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QueueConfig {
    pub durability: Durability,
    pub ack_timeout_ms: u32,
    pub max_deliveries: u32,
    pub dead_letter: String,
    /// Messages older than this are dropped at delivery (-1 = never).
    pub message_ttl_ms: i64,
    /// Messages kept (-1 = unbounded); the oldest go first.
    pub max_length: i64,
}

impl Default for QueueConfig {
    fn default() -> Self {
        QueueConfig {
            durability: Durability::Sync,
            ack_timeout_ms: 30_000,
            max_deliveries: 0,
            dead_letter: String::new(),
            message_ttl_ms: -1,
            max_length: -1,
        }
    }
}

impl QueueConfig {
    pub fn encode(&self, w: &mut Writer<'_>) {
        w.u8(self.durability as u8);
        w.u32(self.ack_timeout_ms);
        w.u32(self.max_deliveries);
        w.str(&self.dead_letter);
        w.i64(self.message_ttl_ms);
        w.i64(self.max_length);
    }
    pub fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(QueueConfig {
            durability: Durability::from_u8(r.u8()?).ok_or(DecodeError("unknown durability"))?,
            ack_timeout_ms: r.u32()?,
            max_deliveries: r.u32()?,
            dead_letter: r.string()?,
            message_ttl_ms: r.i64()?,
            max_length: r.i64()?,
        })
    }
}

// ---------------------------------------------------------------------------
// Bodies
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorBody {
    pub code: Code,
    pub message: String,
}

impl ErrorBody {
    pub fn encode(&self, w: &mut Writer<'_>) {
        w.u16(self.code as u16);
        w.str(&self.message);
    }
    pub fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(ErrorBody { code: Code::from_u16(r.u16()?), message: r.string()? })
    }
}

/// How long a producer waits for its reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Ack {
    /// No reply at all: fire and forget.
    None = 0,
    /// Once appended (in memory or the page cache).
    Written = 1,
    /// Once as durable as the topic is: synced for `sync` topics.
    #[default]
    Durable = 2,
}

impl Ack {
    pub fn from_u8(v: u8) -> Option<Ack> {
        Some(match v {
            0 => Ack::None,
            1 => Ack::Written,
            2 => Ack::Durable,
            _ => return None,
        })
    }
}

/// `PRODUCE`'s partition meaning "by the first record's key".
pub const BY_KEY: u32 = u32::MAX;
/// `PRODUCE`'s partition meaning "whichever, round robin".
pub const ANY_PARTITION: u32 = u32::MAX - 1;

/// `PRODUCE`'s fixed fields; the batch is the rest of the body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Produce {
    pub topic: u32,
    pub partition: u32,
    pub ack: Ack,
}

impl Produce {
    pub const LEN: usize = 9;
    pub fn encode(&self, w: &mut Writer<'_>) {
        w.u32(self.topic);
        w.u32(self.partition);
        w.u8(self.ack as u8);
    }
    pub fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Produce {
            topic: r.u32()?,
            partition: r.u32()?,
            ack: Ack::from_u8(r.u8()?).ok_or(DecodeError("unknown ack"))?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Produced {
    pub partition: u32,
    pub base_offset: u64,
    /// An idempotent producer's batch that was already appended: `base_offset`
    /// is where it went the first time.
    pub duplicate: bool,
}

impl Produced {
    pub fn encode(&self, w: &mut Writer<'_>) {
        w.u32(self.partition);
        w.u64(self.base_offset);
        w.bool(self.duplicate);
    }
    pub fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Produced { partition: r.u32()?, base_offset: r.u64()?, duplicate: r.bool()? })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fetch {
    pub topic: u32,
    pub partition: u32,
    pub offset: u64,
    pub max_bytes: u32,
    /// Wait up to this long for `min_bytes` to arrive past `offset`.
    pub max_wait_ms: u32,
    pub min_bytes: u32,
}

impl Fetch {
    pub fn encode(&self, w: &mut Writer<'_>) {
        w.u32(self.topic);
        w.u32(self.partition);
        w.u64(self.offset);
        w.u32(self.max_bytes);
        w.u32(self.max_wait_ms);
        w.u32(self.min_bytes);
    }
    pub fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Fetch {
            topic: r.u32()?,
            partition: r.u32()?,
            offset: r.u64()?,
            max_bytes: r.u32()?,
            max_wait_ms: r.u32()?,
            min_bytes: r.u32()?,
        })
    }
}

/// `FETCH`'s reply before its batches (the rest of the body).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fetched {
    pub partition: u32,
    /// The offset the next append gets.
    pub next_offset: u64,
    /// The first offset retention has kept.
    pub log_start: u64,
}

impl Fetched {
    pub const LEN: usize = 20;
    pub fn encode(&self, w: &mut Writer<'_>) {
        w.u32(self.partition);
        w.u64(self.next_offset);
        w.u64(self.log_start);
    }
    pub fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Fetched { partition: r.u32()?, next_offset: r.u64()?, log_start: r.u64()? })
    }
}

/// `LIST_OFFSETS`' `at`: the next offset.
pub const LATEST: i64 = -1;
/// `LIST_OFFSETS`' `at`: the first kept offset.
pub const EARLIEST: i64 = -2;

/// A run of offsets: `[offset, offset + count)` of `partition`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Range {
    pub partition: u32,
    pub offset: u64,
    pub count: u32,
}

pub fn encode_ranges(w: &mut Writer<'_>, ranges: &[Range]) {
    w.u32(ranges.len() as u32);
    for r in ranges {
        w.u32(r.partition);
        w.u64(r.offset);
        w.u32(r.count);
    }
}

pub fn decode_ranges(r: &mut Reader<'_>, max: usize) -> Result<Vec<Range>, DecodeError> {
    let n = r.u32()? as usize;
    if n > max {
        return Err(DecodeError("too many ranges"));
    }
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(Range { partition: r.u32()?, offset: r.u64()?, count: r.u32()? });
    }
    Ok(out)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subscribe {
    pub topic: u32,
    pub name: String,
    pub config: SubConfig,
    /// Messages the server may push before the client grants more.
    pub credits: u32,
}

impl Subscribe {
    pub fn encode(&self, w: &mut Writer<'_>) {
        w.u32(self.topic);
        w.str(&self.name);
        self.config.encode(w);
        w.u32(self.credits);
    }
    pub fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        Ok(Subscribe {
            topic: r.u32()?,
            name: r.string()?,
            config: SubConfig::decode(r)?,
            credits: r.u32()?,
        })
    }
}

/// `DELIVER`'s fixed part: which records of the batches that follow are this
/// consumer's. Records outside `ranges` belong to another member (shared), or
/// precede the position (ordered, broadcast), and are skipped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Deliver {
    pub partition: u32,
    pub ranges: Vec<Range>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip() {
        let mut out = Vec::new();
        frame(&mut out, op::PRODUCE, 0, 7, |w| {
            Produce { topic: 3, partition: BY_KEY, ack: Ack::Durable }.encode(w);
            w.raw(b"batch");
        });
        let h = parse_header(&out).unwrap().unwrap();
        assert_eq!((h.op, h.corr, h.body_len), (op::PRODUCE, 7, Produce::LEN + 5));
        let mut r = Reader::new(&out[FRAME_HEADER..]);
        assert_eq!(Produce::decode(&mut r).unwrap().partition, BY_KEY);
        assert_eq!(r.rest(), b"batch");
        assert!(parse_header(&out[..out.len() - 1]).unwrap().is_none());
    }

    #[test]
    fn errors_round_trip() {
        let mut out = Vec::new();
        error_frame(&mut out, op::FETCH, 9, Code::NoTopic, "no topic 4");
        let h = parse_header(&out).unwrap().unwrap();
        assert_eq!(h.flags, F_REPLY | F_ERROR);
        let e = ErrorBody::decode(&mut Reader::new(&out[FRAME_HEADER..])).unwrap();
        assert_eq!(e.code, Code::NoTopic);
    }
}
