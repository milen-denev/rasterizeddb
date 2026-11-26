//! MQTT 3.1.1 and 5.0 on `--mqtt-port` (feature `broker_mqtt`, docs/broker.md
//! §7).
//!
//! Built from the broker's own parts:
//!
//! - **A session** is a queue, `$mqtt.s.<client id>`. It is `memory` for a
//!   clean session (deleted at disconnect) and the configured durability for
//!   a persistent one, so a persistent session's messages wait while its
//!   client is away and survive a restart.
//! - **A subscription filter** is a binding of that queue to the built-in
//!   `amq.mqtt` exchange: the MQTT wildcard trie (`+`, `#`, `$` topics
//!   hidden from wildcards). The granted QoS is kept in the binding, so a
//!   persistent session's subscriptions survive a restart too.
//! - **`$share/<group>/<filter>`** binds a shared queue, `$mqtt.g.<group>`,
//!   that every session in the group consumes.
//! - **PUBLISH** routes through `amq.mqtt`: one copy per matching session.
//!   QoS 1 is acknowledged once stored (synced, for `sync` sessions); QoS 2
//!   runs PUBREC/PUBREL/PUBCOMP with the packet id held until PUBREL, so a
//!   resent PUBLISH is stored once.
//! - **Retained messages** are a topic, `$mqtt.retained`, replayed at start
//!   into a map of the last message per topic. An empty payload clears one.
//! - **Delivery** is the session queue's shared subscription, with the
//!   client's receive maximum as its credit. A QoS 1 delivery is acked on
//!   PUBACK and a QoS 2 one on PUBREC. Anything unacked is delivered again
//!   (with DUP) when the session resumes.
//!
//! MQTT 5 properties that belong to a message (payload format, expiry,
//! content type, response topic, correlation data, user properties) travel
//! with it. Topic aliases are resolved on the way in and never used on the way
//! out.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rasterizeddb_mq::batch::{BatchBuilder, Batches};
use rasterizeddb_mq::wire::{self, Binding, Durability, QueueConfig, Range, TopicConfig, XMatch};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;

use super::acl::{self, Access, Kind, Need};
use super::partition::Partition;
use super::sub::Subscription;
use super::{Broker, BrokerError};
use crate::core::database::Database;
use crate::core::listen::{self, Listeners, Sock, SocketSpec};

const EXCHANGE: &str = "amq.mqtt";
/// Topic aliases a client may set (CONNACK's Topic Alias Maximum).
const TOPIC_ALIAS_MAX: u16 = 64;
/// The largest packet before CONNECT has been accepted.
const PRE_CONNECT_PACKET: usize = 1 << 20;
const RETAINED: &str = "$mqtt.retained";
const HDR_QOS: &[u8] = b"mqtt-q";
const HDR_PROPS: &[u8] = b"mqtt-p";
/// Marks a retained copy put on a session's queue at subscribe time.
const HDR_RETAIN: &[u8] = b"mqtt-r";

#[derive(Clone, Debug)]
pub enum MqttAuth {
    Trust,
    Roles,
}

#[derive(Clone)]
pub struct MqttConfig {
    pub addr: String,
    pub port: Option<u16>,
    pub socket: Option<SocketSpec>,
    pub auth: MqttAuth,
    pub threads: Option<usize>,
    pub tls: Option<tokio_rustls::TlsAcceptor>,
    /// How a persistent session's queue is synced.
    pub durability: Durability,
    pub max_packet: usize,
}

impl std::fmt::Debug for MqttConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttConfig").field("addr", &self.addr).field("port", &self.port).field("auth", &self.auth).finish()
    }
}

pub async fn bind(config: &MqttConfig) -> io::Result<Listeners> {
    if config.port.is_none() && config.socket.is_none() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "MQTT needs --mqtt-port"));
    }
    if matches!(config.auth, MqttAuth::Trust) && config.port.is_some() && !listen::is_loopback(&config.addr) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("--mqtt-auth trust on {} would let anyone on the network publish and subscribe; bind 127.0.0.1", config.addr),
        ));
    }
    listen::bind(&config.addr, config.port, config.socket.as_ref()).await
}

#[derive(Clone)]
struct Retained {
    qos: u8,
    props: Vec<u8>,
    payload: Vec<u8>,
}

struct Shared {
    broker: Arc<Broker>,
    database: rclite::Arc<Database>,
    config: MqttConfig,
    main: tokio::runtime::Handle,
    retained: Mutex<BTreeMap<String, Retained>>,
    /// The live connection of each client id, told to go when another takes
    /// the id over.
    live: Mutex<HashMap<String, (u64, crate::core::broker::sub::Outbox)>>,
    next_conn: std::sync::atomic::AtomicU64,
}

pub async fn serve(listeners: Listeners, database: rclite::Arc<Database>, config: MqttConfig) -> io::Result<()> {
    let broker = database.broker.get().map_err(|e| io::Error::other(e.to_string()))?;
    let retained_topic = broker
        .create_topic(RETAINED, TopicConfig { partitions: 1, retention_ms: -1, ..TopicConfig::default() }, true)
        .map_err(|e| io::Error::other(e.to_string()))?;
    let retained = load_retained(&retained_topic);
    log::info!(
        "broker: MQTT 3.1.1/5.0 on {}:{} ({}, {} retained messages)",
        config.addr,
        listeners.tcp_port().unwrap_or(0),
        match config.auth {
            MqttAuth::Trust => "no authentication",
            MqttAuth::Roles => "database roles",
        },
        retained.len()
    );
    let shared = Arc::new(Shared {
        broker,
        database,
        config,
        main: tokio::runtime::Handle::current(),
        retained: Mutex::new(retained),
        live: Mutex::new(HashMap::new()),
        next_conn: std::sync::atomic::AtomicU64::new(1),
    });
    let threads = listen::default_threads(shared.config.threads);
    listen::serve_per_core(listeners, "mqtt", threads, move |socket, peer| {
        let shared = shared.clone();
        async move {
            let Some(socket) = listen::tls_accept(socket, shared.config.tls.as_ref(), &peer, "mqtt").await else {
                return;
            };
            let mut conn = Conn::new(shared);
            let r = conn.run(socket).await;
            conn.cleanup(r.is_ok() && conn.clean_disconnect);
            if let Err(e) = r {
                if !matches!(e.kind(), io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe | io::ErrorKind::UnexpectedEof) {
                    log::debug!("mqtt: connection from {peer} ended: {e}");
                }
            }
        }
    })
    .await
}

fn load_retained(topic: &super::Topic) -> BTreeMap<String, Retained> {
    let mut map = BTreeMap::new();
    let part = &topic.partitions[0];
    let mut at = part.log_start();
    loop {
        let Ok(r) = part.read(at, 4 << 20) else { break };
        if r.bytes.is_empty() {
            break;
        }
        for b in Batches::trusted(&r.bytes).flatten() {
            for rec in b.records() {
                let Some(t) = rec.key.and_then(|k| std::str::from_utf8(k).ok()) else { continue };
                if rec.value.is_empty() {
                    map.remove(t);
                } else {
                    map.insert(
                        t.to_string(),
                        Retained {
                            qos: rec.header(HDR_QOS).and_then(|q| q.first().copied()).unwrap_or(0),
                            props: rec.header(HDR_PROPS).unwrap_or(&[]).to_vec(),
                            payload: rec.value.to_vec(),
                        },
                    );
                }
            }
            at = b.next_offset();
        }
    }
    map
}

// ---------------------------------------------------------------------------
// Packets
// ---------------------------------------------------------------------------

const CONNECT: u8 = 1;
const CONNACK: u8 = 2;
const PUBLISH: u8 = 3;
const PUBACK: u8 = 4;
const PUBREC: u8 = 5;
const PUBREL: u8 = 6;
const PUBCOMP: u8 = 7;
const SUBSCRIBE: u8 = 8;
const SUBACK: u8 = 9;
const UNSUBSCRIBE: u8 = 10;
const UNSUBACK: u8 = 11;
const PINGREQ: u8 = 12;
const PINGRESP: u8 = 13;
const DISCONNECT: u8 = 14;

#[derive(Debug)]
struct Malformed(&'static str);

/// `(first byte, body, bytes used)` of the packet at the start of `buf`.
fn parse_packet(buf: &[u8], max: usize) -> Result<Option<(u8, &[u8], usize)>, Malformed> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let mut len: usize = 0;
    let mut mul = 1usize;
    let mut i = 1;
    loop {
        let Some(&b) = buf.get(i) else { return Ok(None) };
        len += (b & 0x7f) as usize * mul;
        i += 1;
        if b & 0x80 == 0 {
            break;
        }
        mul *= 128;
        if i > 4 {
            return Err(Malformed("remaining length over four bytes"));
        }
    }
    if len > max {
        return Err(Malformed("packet larger than the maximum"));
    }
    if buf.len() < i + len {
        return Ok(None);
    }
    Ok(Some((buf[0], &buf[i..i + len], i + len)))
}

fn put_len(out: &mut Vec<u8>, mut n: usize) {
    loop {
        let mut b = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            b |= 0x80;
        }
        out.push(b);
        if n == 0 {
            break;
        }
    }
}

fn packet(out: &mut Vec<u8>, first: u8, body: &[u8]) {
    out.push(first);
    put_len(out, body.len());
    out.extend_from_slice(body);
}

struct In<'a> {
    b: &'a [u8],
}

impl<'a> In<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], Malformed> {
        if self.b.len() < n {
            return Err(Malformed("packet too short"));
        }
        let (h, t) = self.b.split_at(n);
        self.b = t;
        Ok(h)
    }
    fn u8(&mut self) -> Result<u8, Malformed> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, Malformed> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, Malformed> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn bin(&mut self) -> Result<&'a [u8], Malformed> {
        let n = self.u16()? as usize;
        self.take(n)
    }
    fn str(&mut self) -> Result<&'a str, Malformed> {
        std::str::from_utf8(self.bin()?).map_err(|_| Malformed("string is not UTF-8"))
    }
    fn varint(&mut self) -> Result<u32, Malformed> {
        let mut v: u32 = 0;
        let mut shift = 0;
        loop {
            let b = self.u8()?;
            v |= ((b & 0x7f) as u32) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
            shift += 7;
            if shift > 21 {
                return Err(Malformed("variable integer too long"));
            }
        }
    }
    /// MQTT 5 properties, as their raw bytes.
    fn props(&mut self) -> Result<&'a [u8], Malformed> {
        let n = self.varint()? as usize;
        self.take(n)
    }
}

fn put_str(out: &mut Vec<u8>, s: &[u8]) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s);
}

fn put_varint(out: &mut Vec<u8>, n: usize) {
    put_len(out, n);
}

/// MQTT 5 properties of a PUBLISH that belong to the message, keeping their
/// encoding; the topic alias (0x23) and subscription identifiers (0x0B) are
/// the connection's own and are dropped. Returns `(kept, topic alias)`.
fn message_props(raw: &[u8]) -> Result<(Vec<u8>, Option<u16>, Option<u32>), Malformed> {
    let mut r = In { b: raw };
    let mut kept = Vec::new();
    let mut alias = None;
    let mut expiry = None;
    while !r.b.is_empty() {
        let start = raw.len() - r.b.len();
        let id = r.varint()?;
        match id {
            0x01 => {
                r.u8()?;
            }
            0x02 => expiry = Some(r.u32()?),
            0x03 | 0x08 => {
                r.bin()?;
            }
            0x09 => {
                r.bin()?;
            }
            0x0B => {
                r.varint()?;
                continue;
            }
            0x23 => {
                alias = Some(r.u16()?);
                continue;
            }
            0x26 => {
                r.bin()?;
                r.bin()?;
            }
            _ => return Err(Malformed("unexpected PUBLISH property")),
        }
        let end = raw.len() - r.b.len();
        kept.extend_from_slice(&raw[start..end]);
    }
    Ok((kept, alias, expiry))
}

/// Whether MQTT filter `filter` matches `topic`.
fn filter_matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && (filter.starts_with('+') || filter.starts_with('#')) {
        return false;
    }
    let mut f = filter.split('/');
    let mut t = topic.split('/');
    loop {
        match (f.next(), t.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

fn valid_filter(f: &str) -> bool {
    if f.is_empty() || f.contains('\0') {
        return false;
    }
    let levels: Vec<&str> = f.split('/').collect();
    for (i, l) in levels.iter().enumerate() {
        if l.contains('#') && (*l != "#" || i + 1 != levels.len()) {
            return false;
        }
        if l.contains('+') && *l != "+" {
            return false;
        }
    }
    true
}

fn queue_name(client: &str) -> String {
    if client.len() <= 200 {
        format!("$mqtt.s.{client}")
    } else {
        format!("$mqtt.s.h{:016x}", rasterizeddb_mq::hash::key_hash(client.as_bytes()))
    }
}

// ---------------------------------------------------------------------------
// A connection
// ---------------------------------------------------------------------------

struct Waiting {
    /// Packet id to acknowledge, and with which packet type.
    id: u16,
    kind: u8,
    targets: Vec<(Arc<Partition>, u64)>,
}

/// A delivery in flight to the client, by packet id.
struct Outgoing {
    sub: Arc<Subscription>,
    range: Range,
    qos: u8,
    /// QoS 2: PUBREC came, PUBREL sent, waiting for PUBCOMP.
    released: bool,
}

struct Consumer {
    sub: Arc<Subscription>,
    member: u64,
    queue: String,
}

struct Conn {
    shared: Arc<Shared>,
    id: u64,
    version: u8,
    client: String,
    persistent: bool,
    clean_disconnect: bool,
    keepalive: Duration,
    will: Option<(String, u8, bool, Vec<u8>, Vec<u8>)>,
    /// Filters and their granted QoS, on the session queue.
    filters: Vec<(String, u8)>,
    /// `$share` groups this session consumes.
    shared_groups: Vec<(String, String, u8)>,
    consumers: HashMap<u32, Consumer>,
    next_handle: u32,
    push_tx: super::sub::Outbox,
    push_rx: crate::core::broker::sub::OutboxRx,
    /// Inbound QoS 2 packet ids awaiting PUBREL.
    qos2_in: std::collections::HashSet<u16>,
    outgoing: HashMap<u16, Outgoing>,
    next_packet: u16,
    receive_max: u16,
    aliases: HashMap<u16, String>,
    waiting: Vec<Waiting>,
    connected: bool,
    /// Who the connection is and what it may use (`acl`): publishing needs
    /// PUBLISH on `amq.mqtt`, subscribing BIND on it, and a persistent
    /// session is its creator's (another role's CONNECT with its client id
    /// is refused).
    access: Access,
}

impl Conn {
    fn new(shared: Arc<Shared>) -> Conn {
        let (push_tx, push_rx) = crate::core::broker::sub::outbox();
        let id = shared.next_conn.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Conn {
            shared,
            id,
            version: 4,
            client: String::new(),
            persistent: false,
            clean_disconnect: false,
            keepalive: Duration::ZERO,
            will: None,
            filters: Vec::new(),
            shared_groups: Vec::new(),
            consumers: HashMap::new(),
            next_handle: 1,
            push_tx,
            push_rx,
            qos2_in: Default::default(),
            outgoing: HashMap::new(),
            next_packet: 1,
            receive_max: 64,
            aliases: HashMap::new(),
            waiting: Vec::new(),
            connected: false,
            access: Access::new("", false),
        }
    }

    async fn run(&mut self, mut sock: Sock) -> io::Result<()> {
        let mut buf: Vec<u8> = Vec::with_capacity(16 * 1024);
        let mut out: Vec<u8> = Vec::with_capacity(16 * 1024);
        let mut last_input = Instant::now();
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let connect_deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let mut at = 0;
            loop {
                let max = if self.connected { self.shared.config.max_packet } else { PRE_CONNECT_PACKET };
                let p = match parse_packet(&buf[at..], max) {
                    Ok(Some(p)) => p,
                    Ok(None) => break,
                    Err(e) => {
                        self.disconnect(&mut out, 0x95, e.0);
                        sock.write_all(&out).await?;
                        return Err(io::Error::new(io::ErrorKind::InvalidData, e.0));
                    }
                };
                let (first, body, used) = p;
                last_input = Instant::now();
                let r = self.packet(first, body, &mut out).await;
                at += used;
                if let Err(e) = r {
                    sock.write_all(&out).await?;
                    return match e {
                        End::Clean => Ok(()),
                        End::Protocol(m) => Err(io::Error::new(io::ErrorKind::InvalidData, m)),
                    };
                }
            }
            buf.drain(..at);
            self.release_waiting(&mut out);
            while let Ok(f) = self.push_rx.try_recv() {
                if self.push(&f, &mut out) {
                    sock.write_all(&out).await?;
                    return Ok(());
                }
            }
            if !out.is_empty() {
                sock.write_all(&out).await?;
                out.clear();
            }
            if buf.capacity() - buf.len() < 4096 {
                buf.reserve(16 * 1024);
            }
            let wait = self.wait_future();
            tokio::select! {
                r = sock.read_buf(&mut buf) => {
                    if r? == 0 {
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "closed without DISCONNECT"));
                    }
                }
                Some(f) = self.push_rx.recv() => {
                    if self.push(&f, &mut out) {
                        sock.write_all(&out).await?;
                        return Ok(());
                    }
                }
                _ = wait => {}
                _ = tick.tick() => {
                    if !self.connected && Instant::now() > connect_deadline {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "no CONNECT"));
                    }
                    if !self.keepalive.is_zero() && last_input.elapsed() > self.keepalive * 3 / 2 {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "keep-alive expired"));
                    }
                }
            }
        }
    }

    fn disconnect(&self, out: &mut Vec<u8>, reason: u8, _why: &str) {
        if self.version == 5 && self.connected {
            packet(out, DISCONNECT << 4, &[reason, 0]);
        }
    }

    async fn packet(&mut self, first: u8, body: &[u8], out: &mut Vec<u8>) -> Result<(), End> {
        let kind = first >> 4;
        let flags = first & 0x0f;
        if !self.connected && kind != CONNECT {
            return Err(End::Protocol("the first packet must be CONNECT"));
        }
        if self.connected && matches!(kind, PUBLISH | SUBSCRIBE) {
            if let Err(e) = self.access.refresh(&self.shared.database, &self.shared.main).await {
                log::warn!("mqtt: closing {}'s connection: {}", self.client, e.message);
                self.disconnect(out, 0x87, "not authorized");
                return Err(End::Protocol("the privilege catalogue could not be read"));
            }
        }
        let mut r = In { b: body };
        let m = |e: Malformed| End::Protocol(e.0);
        match kind {
            CONNECT => {
                if self.connected {
                    return Err(End::Protocol("a second CONNECT"));
                }
                return self.connect(body, out).await;
            }
            PUBLISH => self.publish(flags, body, out)?,
            PUBACK => {
                let id = r.u16().map_err(m)?;
                if let Some(o) = self.outgoing.remove(&id) {
                    let _ = o.sub.ack(&[o.range]);
                    o.sub.credit(self.member_of(&o.sub), 1);
                }
            }
            PUBREC => {
                let id = r.u16().map_err(m)?;
                if let Some(o) = self.outgoing.get_mut(&id) {
                    if !o.released {
                        let _ = o.sub.ack(&[o.range]);
                        o.released = true;
                    }
                }
                packet(out, (PUBREL << 4) | 0x02, &id.to_be_bytes());
            }
            PUBREL => {
                let id = r.u16().map_err(m)?;
                self.qos2_in.remove(&id);
                packet(out, PUBCOMP << 4, &id.to_be_bytes());
            }
            PUBCOMP => {
                let id = r.u16().map_err(m)?;
                if let Some(o) = self.outgoing.remove(&id) {
                    o.sub.credit(self.member_of(&o.sub), 1);
                }
            }
            SUBSCRIBE => {
                if flags != 0x02 {
                    return Err(End::Protocol("SUBSCRIBE flags must be 0010"));
                }
                self.subscribe(body, out)?;
            }
            UNSUBSCRIBE => {
                if flags != 0x02 {
                    return Err(End::Protocol("UNSUBSCRIBE flags must be 0010"));
                }
                self.unsubscribe(body, out)?;
            }
            PINGREQ => packet(out, PINGRESP << 4, &[]),
            DISCONNECT => {
                let reason = if self.version == 5 { r.u8().unwrap_or(0) } else { 0 };
                // 0x04: disconnect with the will message.
                if reason != 0x04 {
                    self.will = None;
                }
                self.clean_disconnect = true;
                return Err(End::Clean);
            }
            _ => return Err(End::Protocol("unexpected packet type")),
        }
        Ok(())
    }

    fn member_of(&self, sub: &Arc<Subscription>) -> u64 {
        self.consumers.values().find(|c| Arc::ptr_eq(&c.sub, sub)).map_or(0, |c| c.member)
    }

    async fn connect(&mut self, body: &[u8], out: &mut Vec<u8>) -> Result<(), End> {
        let mut r = In { b: body };
        let m = |e: Malformed| End::Protocol(e.0);
        let name = r.str().map_err(m)?;
        let level = r.u8().map_err(m)?;
        if !matches!((name, level), ("MQTT", 4) | ("MQTT", 5) | ("MQIsdp", 3)) {
            // 0x01 (3.x) / 0x84 (5): unsupported protocol version.
            packet(out, CONNACK << 4, &[0, if level == 5 { 0x84 } else { 0x01 }]);
            return Err(End::Protocol("unsupported protocol version"));
        }
        self.version = if level == 3 { 4 } else { level };
        let flags = r.u8().map_err(m)?;
        if flags & 1 != 0 {
            return Err(End::Protocol("reserved CONNECT flag set"));
        }
        self.keepalive = Duration::from_secs(r.u16().map_err(m)? as u64);
        let clean = flags & 0x02 != 0;
        let mut session_expiry = 0u32;
        if self.version == 5 {
            let props = r.props().map_err(m)?;
            let mut p = In { b: props };
            while !p.b.is_empty() {
                match p.varint().map_err(m)? {
                    0x11 => session_expiry = p.u32().map_err(m)?,
                    0x21 => self.receive_max = p.u16().map_err(m)?.max(1),
                    0x27 | 0x18 => {
                        p.u32().map_err(m)?;
                    }
                    0x22 => {
                        p.u16().map_err(m)?;
                    }
                    0x19 | 0x17 => {
                        p.u8().map_err(m)?;
                    }
                    0x15 | 0x16 => {
                        p.bin().map_err(m)?;
                    }
                    0x26 => {
                        p.bin().map_err(m)?;
                        p.bin().map_err(m)?;
                    }
                    _ => return Err(End::Protocol("unknown CONNECT property")),
                }
            }
        }
        let mut client = r.str().map_err(m)?.to_string();
        if flags & 0x04 != 0 {
            let will_props = if self.version == 5 { r.props().map_err(m)?.to_vec() } else { Vec::new() };
            let topic = r.str().map_err(m)?.to_string();
            let payload = r.bin().map_err(m)?.to_vec();
            let qos = (flags >> 3) & 3;
            let retain = flags & 0x20 != 0;
            // Will properties are the message's, less the will delay.
            let props = message_props_will(&will_props);
            self.will = Some((topic, qos.min(2), retain, payload, props));
        }
        let user = if flags & 0x80 != 0 { Some(r.str().map_err(m)?.to_string()) } else { None };
        let pass = if flags & 0x40 != 0 { Some(String::from_utf8_lossy(r.bin().map_err(m)?).into_owned()) } else { None };
        let refuse = |out: &mut Vec<u8>, v3: u8, v5: u8, version: u8| {
            if version == 5 {
                packet(out, CONNACK << 4, &[0, v5, 0]);
            } else {
                packet(out, CONNACK << 4, &[0, v3]);
            }
        };
        if let MqttAuth::Roles = self.shared.config.auth {
            let (Some(u), Some(p)) = (user, pass) else {
                refuse(out, 4, 0x86, self.version);
                return Err(End::Protocol("no user name or password"));
            };
            match listen::login(&self.shared.database, &self.shared.main, &u, &p, "mqtt").await {
                Ok(superuser) => self.access = Access::new(&u, superuser),
                Err(e) => {
                    let (v3, v5) = if matches!(e, listen::LoginError::Denied) { (4, 0x86) } else { (5, 0x87) };
                    refuse(out, v3, v5, self.version);
                    return Err(End::Protocol("authentication failed"));
                }
            }
        } else {
            self.access = Access::unrestricted("");
        }
        if self.access.refresh(&self.shared.database, &self.shared.main).await.is_err() {
            refuse(out, 5, 0x87, self.version);
            return Err(End::Protocol("the privilege catalogue could not be read"));
        }
        // A will is a publish, made later on the role's behalf.
        if self.will.is_some() && !self.access.may(Kind::Exchange, EXCHANGE, Need::Publish) {
            refuse(out, 5, 0x87, self.version);
            return Err(End::Protocol("the will may not be published"));
        }
        let mut assigned = None;
        if client.is_empty() {
            if self.version == 4 && !clean {
                refuse(out, 2, 0x85, self.version);
                return Err(End::Protocol("an empty client id needs a clean session"));
            }
            client = format!("rdb-{}-{}", self.id, super::now_us());
            assigned = Some(client.clone());
        }
        // Another role's persistent session is not this one's to take over
        // (nor to read what waits in it).
        let queue = queue_name(&client);
        if self.access.has_owner(Kind::Topic, &queue) && !self.access.may(Kind::Topic, &queue, Need::Own) {
            refuse(out, 5, 0x87, self.version);
            return Err(End::Protocol("the client id is another role's session"));
        }
        self.client = client.clone();
        self.persistent = if self.version == 5 { session_expiry > 0 } else { !clean };
        // Session takeover: the earlier connection with this id goes.
        {
            let mut live = self.shared.live.lock();
            if let Some((_, tx)) = live.insert(client.clone(), (self.id, self.push_tx.clone())) {
                let _ = tx.send(Vec::new());
            }
        }
        let broker = self.shared.broker.clone();
        let existed = broker.queue(&queue).is_ok();
        let owned = self.access.has_owner(Kind::Topic, &queue);
        let mut dropped = false;
        if clean && existed {
            dropped |= broker.delete_topic(&queue, true).unwrap_or(false);
        }
        let durability = if self.persistent { self.shared.config.durability } else { Durability::Memory };
        let qc = QueueConfig { durability, ack_timeout_ms: u32::MAX, ..QueueConfig::default() };
        let created = match broker.declare_queue_reporting(&queue, qc, true) {
            Ok((_, created)) => created,
            // A queue left with another durability (the session changed
            // kind): start it over.
            Err(e) if e.code == wire::Code::Mismatch => {
                dropped |= broker.delete_topic(&queue, true).unwrap_or(false);
                let qc = QueueConfig { durability, ack_timeout_ms: u32::MAX, ..QueueConfig::default() };
                broker.declare_queue_reporting(&queue, qc, true).map_err(|_| End::Protocol("could not open the session"))?.1
            }
            Err(_) => {
                refuse(out, 3, 0x88, self.version);
                return Err(End::Protocol("could not open the session"));
            }
        };
        // A persistent session is its role's; a clean one is gone with its
        // connection and leaves no rows (one per connection otherwise).
        if created && self.persistent {
            acl::created(&self.shared.database, &self.shared.main, Kind::Topic, &queue, self.access.user()).await;
        } else if dropped && owned {
            acl::removed(&self.shared.database, &self.shared.main, Kind::Topic, &queue).await;
        }
        let session_present = existed && !clean;
        if session_present {
            // The persistent session's filters, from its bindings.
            for (name, _, _, bindings) in broker.exchanges() {
                if name != EXCHANGE {
                    continue;
                }
                for b in bindings.into_iter().filter(|b| b.topic == queue) {
                    let qos = b.args.iter().find(|(k, _)| k == "qos").and_then(|(_, v)| v.first().copied()).unwrap_or(0);
                    self.filters.push((b.key, qos));
                }
            }
        }
        let mut ack = vec![session_present as u8, 0];
        if self.version == 5 {
            let mut props = Vec::new();
            if let Some(id) = &assigned {
                props.push(0x12);
                put_str(&mut props, id.as_bytes());
            }
            props.extend_from_slice(&[0x21]);
            props.extend_from_slice(&1024u16.to_be_bytes());
            props.push(0x22); // topic alias maximum the client may use
            props.extend_from_slice(&TOPIC_ALIAS_MAX.to_be_bytes());
            props.extend_from_slice(&[0x29, 0]); // no subscription identifiers
            put_varint(&mut ack, props.len());
            ack.extend_from_slice(&props);
        }
        packet(out, CONNACK << 4, &ack);
        self.connected = true;
        self.consume(&queue).map_err(|_| End::Protocol("could not consume the session"))?;
        Ok(())
    }

    fn consume(&mut self, queue: &str) -> Result<(), BrokerError> {
        if self.consumers.values().any(|c| c.queue == queue) {
            return Ok(());
        }
        let (_, sub) = self.shared.broker.queue(queue)?;
        let handle = self.next_handle;
        self.next_handle += 1;
        let (member, _) = sub.join_with(handle, self.receive_max as u32, wire::SUB_DELIVERY_COUNTS, self.push_tx.clone(), &self.shared.main)?;
        self.consumers.insert(handle, Consumer { sub, member, queue: queue.to_string() });
        Ok(())
    }

    fn publish(&mut self, flags: u8, body: &[u8], out: &mut Vec<u8>) -> Result<(), End> {
        let qos = (flags >> 1) & 3;
        let retain = flags & 1 != 0;
        if qos == 3 {
            return Err(End::Protocol("QoS 3"));
        }
        let mut r = In { b: body };
        let m = |e: Malformed| End::Protocol(e.0);
        let mut topic = r.str().map_err(m)?.to_string();
        let id = if qos > 0 { r.u16().map_err(m)? } else { 0 };
        let mut props = Vec::new();
        let mut expiry = None;
        if self.version == 5 {
            let raw = r.props().map_err(m)?;
            let (kept, alias, exp) = message_props(raw).map_err(m)?;
            props = kept;
            expiry = exp;
            if let Some(a) = alias {
                if a == 0 || a > TOPIC_ALIAS_MAX {
                    return Err(End::Protocol("topic alias out of range"));
                }
                if topic.is_empty() {
                    topic = self.aliases.get(&a).cloned().ok_or(End::Protocol("unknown topic alias"))?;
                } else {
                    self.aliases.insert(a, topic.clone());
                }
            }
        }
        if topic.is_empty() || topic.contains(['+', '#']) {
            return Err(End::Protocol("a PUBLISH topic must be a name, not a filter"));
        }
        let payload = r.b;
        // Not allowed: acknowledged with "not authorized" (v5), and not
        // stored; MQTT 3.1.1 has no refusal for a publish, so it is
        // acknowledged and dropped, as brokers' ACLs do.
        if !self.access.may(Kind::Exchange, EXCHANGE, Need::Publish) {
            if qos > 0 {
                let kind = if qos == 1 { PUBACK } else { PUBREC };
                let mut b = id.to_be_bytes().to_vec();
                if self.version == 5 {
                    b.push(0x87);
                }
                packet(out, kind << 4, &b);
            }
            return Ok(());
        }
        if qos == 2 && !self.qos2_in.insert(id) {
            // Sent again before PUBREL: stored already.
            packet(out, PUBREC << 4, &id.to_be_bytes());
            return Ok(());
        }
        let targets = self.store(&topic, qos, retain, &props, payload, expiry);
        match qos {
            0 => {}
            q => {
                let kind = if q == 1 { PUBACK } else { PUBREC };
                if targets.is_empty() {
                    packet(out, kind << 4, &id.to_be_bytes());
                } else {
                    self.waiting.push(Waiting { id, kind, targets });
                }
            }
        }
        Ok(())
    }

    /// Stores a message for every session it matches, and the retained copy.
    /// Returns the partitions a QoS 1/2 acknowledgement waits on.
    fn store(&self, topic: &str, qos: u8, retain: bool, props: &[u8], payload: &[u8], expiry: Option<u32>) -> Vec<(Arc<Partition>, u64)> {
        let broker = &self.shared.broker;
        let mut headers: Vec<(&[u8], &[u8])> = vec![(HDR_QOS, std::slice::from_ref(&qos))];
        if !props.is_empty() {
            headers.push((HDR_PROPS, props));
        }
        let at;
        if let Some(sec) = expiry {
            at = (super::now_us() + sec as i64 * 1_000_000).to_le_bytes();
            headers.push((b"mqtt-expires", &at));
        }
        let mut targets = Vec::new();
        if retain {
            let mut rb = BatchBuilder::with_capacity(payload.len() + 128);
            let _ = rb.push(Some(topic.as_bytes()), payload, &headers);
            let mut bytes = rb.finish();
            if let Some(t) = broker.topic(RETAINED) {
                if let Ok((p, a)) = broker.produce(&t, 0, &mut bytes, false) {
                    targets.push((p, a.next_offset));
                }
            }
            let mut map = self.shared.retained.lock();
            if payload.is_empty() {
                map.remove(topic);
            } else {
                map.insert(topic.to_string(), Retained { qos, props: props.to_vec(), payload: payload.to_vec() });
            }
        }
        let mut b = BatchBuilder::with_capacity(payload.len() + 128);
        let _ = b.push(Some(topic.as_bytes()), payload, &headers);
        let mut bytes = b.finish();
        match broker.publish(EXCHANGE, topic, &mut bytes, false) {
            Ok(placed) => {
                for (p, a) in placed {
                    if qos > 0 && p.durability == Durability::Sync && p.durable_offset() < a.next_offset {
                        targets.push((p, a.next_offset));
                    }
                }
            }
            Err(e) => log::warn!("mqtt: publish to {topic} failed: {e}"),
        }
        targets.retain(|(p, upto)| p.durability == Durability::Sync && p.durable_offset() < *upto);
        targets
    }

    fn subscribe(&mut self, body: &[u8], out: &mut Vec<u8>) -> Result<(), End> {
        let mut r = In { b: body };
        let m = |e: Malformed| End::Protocol(e.0);
        let id = r.u16().map_err(m)?;
        if self.version == 5 {
            r.props().map_err(m)?;
        }
        let broker = self.shared.broker.clone();
        let queue = queue_name(&self.client);
        let mut codes = Vec::new();
        let mut retained_out = Vec::new();
        while !r.b.is_empty() {
            let filter = r.str().map_err(m)?.to_string();
            let opts = r.u8().map_err(m)?;
            let qos = (opts & 3).min(2);
            let retain_handling = (opts >> 4) & 3;
            if !valid_filter(&filter) {
                codes.push(if self.version == 5 { 0x8F } else { 0x80 });
                continue;
            }
            if !self.access.may(Kind::Exchange, EXCHANGE, Need::Bind) {
                codes.push(if self.version == 5 { 0x87 } else { 0x80 });
                continue;
            }
            let granted = if let Some(rest) = filter.strip_prefix("$share/") {
                let Some((group, f)) = rest.split_once('/') else {
                    codes.push(if self.version == 5 { 0x8F } else { 0x80 });
                    continue;
                };
                let gq = format!("$mqtt.g.{group}");
                let qc = QueueConfig { durability: self.shared.config.durability, ack_timeout_ms: 600_000, ..QueueConfig::default() };
                let ok = broker.declare_queue(&gq, qc, true).is_ok()
                    && broker.bind(binding(f, &gq, qos)).is_ok()
                    && self.consume(&gq).is_ok();
                if ok {
                    self.shared_groups.retain(|(g, x, _)| !(g == group && x == f));
                    self.shared_groups.push((group.to_string(), f.to_string(), qos));
                }
                ok
            } else {
                let fresh = !self.filters.iter().any(|(f, _)| *f == filter);
                // A changed QoS replaces the binding.
                if let Some((_, old)) = self.filters.iter().find(|(f, _)| *f == filter) {
                    let _ = broker.unbind(&binding(&filter, &queue, *old));
                }
                let ok = broker.bind(binding(&filter, &queue, qos)).is_ok();
                if ok {
                    self.filters.retain(|(f, _)| *f != filter);
                    self.filters.push((filter.clone(), qos));
                    // Retained messages: on every subscribe (0), on a new one
                    // (1), never (2).
                    if retain_handling == 0 || retain_handling == 1 && fresh {
                        for (t, msg) in self.shared.retained.lock().iter() {
                            if filter_matches(&filter, t) {
                                retained_out.push((t.clone(), msg.clone(), qos));
                            }
                        }
                    }
                }
                ok
            };
            codes.push(if granted { qos } else if self.version == 5 { 0x80 } else { 0x80 });
        }
        let mut ack = id.to_be_bytes().to_vec();
        if self.version == 5 {
            ack.push(0);
        }
        ack.extend_from_slice(&codes);
        packet(out, SUBACK << 4, &ack);
        // Retained messages go onto the session's own queue, marked, so they
        // are delivered (and, at QoS 1/2, acknowledged) like any other.
        if !retained_out.is_empty() {
            if let Some(topic) = broker.topic(&queue) {
                for (t, msg, _granted) in retained_out {
                    let mut headers: Vec<(&[u8], &[u8])> =
                        vec![(HDR_QOS, std::slice::from_ref(&msg.qos)), (HDR_RETAIN, b"1")];
                    if !msg.props.is_empty() {
                        headers.push((HDR_PROPS, &msg.props));
                    }
                    let mut b = BatchBuilder::with_capacity(msg.payload.len() + 128);
                    let _ = b.push(Some(t.as_bytes()), &msg.payload, &headers);
                    if let Err(e) = broker.produce(&topic, 0, &mut b.finish(), false) {
                        log::warn!("mqtt: retained message for {t} not queued: {e}");
                    }
                }
            }
        }
        Ok(())
    }

    fn unsubscribe(&mut self, body: &[u8], out: &mut Vec<u8>) -> Result<(), End> {
        let mut r = In { b: body };
        let m = |e: Malformed| End::Protocol(e.0);
        let id = r.u16().map_err(m)?;
        if self.version == 5 {
            r.props().map_err(m)?;
        }
        let broker = self.shared.broker.clone();
        let queue = queue_name(&self.client);
        let mut codes = Vec::new();
        while !r.b.is_empty() {
            let filter = r.str().map_err(m)?.to_string();
            let found = if let Some(rest) = filter.strip_prefix("$share/") {
                match rest.split_once('/') {
                    Some((g, f)) => {
                        let gq = format!("$mqtt.g.{g}");
                        let had = self.shared_groups.iter().position(|(x, y, _)| x == g && y == f);
                        if let Some(i) = had {
                            let (_, _, q) = self.shared_groups.remove(i);
                            let _ = broker.unbind(&binding(f, &gq, q));
                            if !self.shared_groups.iter().any(|(x, _, _)| x == g) {
                                let h = self.consumers.iter().find(|(_, c)| c.queue == gq).map(|(h, _)| *h);
                                if let Some(h) = h {
                                    let c = self.consumers.remove(&h).expect("present");
                                    c.sub.leave(c.member);
                                }
                            }
                        }
                        had.is_some()
                    }
                    None => false,
                }
            } else {
                match self.filters.iter().position(|(f, _)| *f == filter) {
                    Some(i) => {
                        let (f, q) = self.filters.remove(i);
                        let _ = broker.unbind(&binding(&f, &queue, q));
                        true
                    }
                    None => false,
                }
            };
            codes.push(if found { 0x00 } else { 0x11 });
        }
        let mut ack = id.to_be_bytes().to_vec();
        if self.version == 5 {
            ack.push(0);
            ack.extend_from_slice(&codes);
        }
        packet(out, UNSUBACK << 4, &ack);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn write_publish(&self, out: &mut Vec<u8>, topic: &str, qos: u8, retain: bool, dup: bool, id: u16, props: &[u8], payload: &[u8]) {
        let mut body = Vec::with_capacity(topic.len() + payload.len() + 16);
        put_str(&mut body, topic.as_bytes());
        if qos > 0 {
            body.extend_from_slice(&id.to_be_bytes());
        }
        if self.version == 5 {
            put_varint(&mut body, props.len());
            body.extend_from_slice(props);
        }
        body.extend_from_slice(payload);
        let first = (PUBLISH << 4) | ((dup as u8) << 3) | (qos << 1) | retain as u8;
        packet(out, first, &body);
    }

    /// The QoS a session receives `topic` at: the highest of its matching
    /// filters' grants. `None` when no filter matches (it was unsubscribed
    /// while the message waited).
    fn granted(&self, topic: &str, queue: &str) -> Option<u8> {
        if queue.starts_with("$mqtt.g.") {
            let group = &queue["$mqtt.g.".len()..];
            return self.shared_groups.iter().filter(|(g, f, _)| g == group && filter_matches(f, topic)).map(|(_, _, q)| *q).max();
        }
        self.filters.iter().filter(|(f, _)| filter_matches(f, topic)).map(|(_, q)| *q).max()
    }

    /// A frame a subscription pushed. `true` ends the connection (another
    /// took the client id over).
    fn push(&mut self, frame: &[u8], out: &mut Vec<u8>) -> bool {
        if frame.is_empty() {
            // Session takeover.
            if self.version == 5 {
                packet(out, DISCONNECT << 4, &[0x8E, 0]);
            }
            self.clean_disconnect = false;
            self.will = None;
            return true;
        }
        let Ok(Some(h)) = wire::parse_header(frame) else { return false };
        if h.op != wire::op::DELIVER {
            return false;
        }
        let Some(c) = self.consumers.get(&h.corr) else { return false };
        let (sub, member, queue) = (c.sub.clone(), c.member, c.queue.clone());
        let body = &frame[wire::FRAME_HEADER..wire::FRAME_HEADER + h.body_len];
        let mut r = wire::Reader::new(body);
        let Ok(partition) = r.u32() else { return false };
        let Ok(ranges) = wire::decode_ranges(&mut r, 1 << 20) else { return false };
        let counts = if h.flags & wire::F_COUNTS != 0 {
            let Ok(c) = wire::decode_counts(&mut r, 1 << 20) else { return false };
            c
        } else {
            Vec::new()
        };
        let now = super::now_us();
        let mut done = Vec::new();
        let mut credit = 0u32;
        for b in Batches::trusted(r.rest()).flatten() {
            for rec in b.records() {
                if !ranges.iter().any(|g| rec.offset >= g.offset && rec.offset < g.offset + g.count as u64) {
                    continue;
                }
                let range = Range { partition, offset: rec.offset, count: 1 };
                let topic = rec.key.and_then(|k| std::str::from_utf8(k).ok()).unwrap_or("");
                let expired = rec
                    .header(b"mqtt-expires")
                    .and_then(|v| <[u8; 8]>::try_from(v).ok())
                    .is_some_and(|v| i64::from_le_bytes(v) < now);
                let Some(granted) = self.granted(topic, &queue).filter(|_| !expired) else {
                    done.push(range);
                    credit += 1;
                    continue;
                };
                let published = rec.header(HDR_QOS).and_then(|q| q.first().copied()).unwrap_or(0);
                let qos = published.min(granted);
                let props = rec.header(HDR_PROPS).unwrap_or(&[]);
                let retain = rec.header(HDR_RETAIN).is_some();
                if qos == 0 {
                    self.write_publish(out, topic, 0, retain, false, 0, props, rec.value);
                    done.push(range);
                    credit += 1;
                    continue;
                }
                let id = self.packet_id();
                // A record delivered before (unacked at a disconnect, or past
                // its ack timeout) carries DUP.
                let dup = counts.iter().any(|c| c.offset == rec.offset && c.deliveries > 1);
                self.write_publish(out, topic, qos, retain, dup, id, props, rec.value);
                self.outgoing.insert(id, Outgoing { sub: sub.clone(), range, qos, released: false });
            }
        }
        if !done.is_empty() {
            let _ = sub.ack(&done);
        }
        if credit > 0 {
            sub.credit(member, credit);
        }
        false
    }

    fn packet_id(&mut self) -> u16 {
        loop {
            let id = self.next_packet;
            self.next_packet = self.next_packet.wrapping_add(1).max(1);
            if !self.outgoing.contains_key(&id) {
                return id;
            }
        }
    }

    fn wait_future(&self) -> impl std::future::Future<Output = ()> + 'static {
        let mut rxs: Vec<watch::Receiver<u64>> =
            self.waiting.iter().flat_map(|w| w.targets.iter().map(|(p, _)| p.subscribe_durable())).collect();
        async move {
            if rxs.is_empty() {
                return std::future::pending().await;
            }
            let _ = futures::future::select_all(rxs.iter_mut().map(|r| Box::pin(r.changed()))).await;
        }
    }

    fn release_waiting(&mut self, out: &mut Vec<u8>) {
        let waiting = std::mem::take(&mut self.waiting);
        for w in waiting {
            if w.targets.iter().all(|(p, upto)| p.durable_offset() >= *upto || p.is_poisoned()) {
                packet(out, w.kind << 4, &w.id.to_be_bytes());
            } else {
                self.waiting.push(w);
            }
        }
    }

    /// The connection ended: unacked deliveries go back to the session queue,
    /// a clean session's queue goes, and the will is published unless the
    /// client disconnected cleanly.
    fn cleanup(&mut self, clean: bool) {
        for (_, o) in self.outgoing.drain() {
            if !o.released {
                let _ = o.sub.nack(&[o.range], 0);
            }
            let _ = o.qos;
        }
        for (_, c) in self.consumers.drain() {
            c.sub.leave(c.member);
        }
        {
            let mut live = self.shared.live.lock();
            if live.get(&self.client).is_some_and(|(id, _)| *id == self.id) {
                live.remove(&self.client);
            }
        }
        if !clean || self.will.is_some() {
            if let Some((topic, qos, retain, payload, props)) = self.will.take() {
                if (!clean || self.version == 5) && self.access.may(Kind::Exchange, EXCHANGE, Need::Publish) {
                    self.store(&topic, qos, retain, &props, &payload, None);
                }
            }
        }
        if self.connected && !self.persistent {
            let broker = &self.shared.broker;
            let queue = queue_name(&self.client);
            // Its bindings go with the queue's topic.
            let _ = broker.delete_topic(&queue, true);
        }
    }
}

enum End {
    Clean,
    Protocol(&'static str),
}

/// A will's properties, as a PUBLISH's, without the will delay (0x18).
fn message_props_will(raw: &[u8]) -> Vec<u8> {
    let mut r = In { b: raw };
    let mut kept = Vec::new();
    while !r.b.is_empty() {
        let start = raw.len() - r.b.len();
        let Ok(id) = r.varint() else { break };
        let ok = match id {
            0x01 => r.u8().is_ok(),
            0x02 | 0x18 => r.u32().is_ok(),
            0x03 | 0x08 | 0x09 => r.bin().is_ok(),
            0x26 => r.bin().is_ok() && r.bin().is_ok(),
            _ => false,
        };
        if !ok {
            break;
        }
        if id != 0x18 {
            let end = raw.len() - r.b.len();
            kept.extend_from_slice(&raw[start..end]);
        }
    }
    kept
}

/// The binding that puts messages matching `filter` on `queue`, with the
/// granted QoS kept in its arguments.
fn binding(filter: &str, queue: &str, qos: u8) -> Binding {
    Binding {
        exchange: EXCHANGE.to_string(),
        topic: queue.to_string(),
        key: filter.to_string(),
        x_match: XMatch::All,
        args: vec![("qos".to_string(), vec![qos])],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters() {
        assert!(filter_matches("a/+/c", "a/b/c"));
        assert!(filter_matches("a/#", "a"));
        assert!(filter_matches("#", "x/y"));
        assert!(!filter_matches("#", "$SYS/x"));
        assert!(filter_matches("$SYS/#", "$SYS/x"));
        assert!(!filter_matches("a/+", "a/b/c"));
        assert!(valid_filter("a/+/#"));
        assert!(!valid_filter("a/b#"));
        assert!(!valid_filter("a/#/b"));
    }

    #[test]
    fn remaining_length() {
        for n in [0usize, 127, 128, 16383, 16384, 2_097_151, 2_097_152] {
            let mut v = vec![0x30];
            put_len(&mut v, n);
            v.resize(v.len() + n, 0);
            let (_, body, used) = parse_packet(&v, usize::MAX).unwrap().unwrap();
            assert_eq!(body.len(), n);
            assert_eq!(used, v.len());
        }
    }
}
