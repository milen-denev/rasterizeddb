//! AMQP 0-9-1 on `--amqp-port` (feature `broker_amqp`, docs/broker.md §7):
//! what RabbitMQ's clients speak.
//!
//! - **Queues** are the broker's queues: a one-partition topic and a shared
//!   subscription of the same name. `durable` is `sync`, and a transient
//!   queue is `memory`. `x-message-ttl`, `x-max-length`, `x-delivery-limit`,
//!   `x-dead-letter-exchange`/`-routing-key` and `x-consumer-timeout` map
//!   onto the queue's settings.
//! - **Exchanges** are the broker's exchanges (`direct`, `fanout`, `topic`,
//!   `headers`, the `amq.*` ones, and the default exchange).
//! - **Consumers** are members of the queue's shared subscription.
//!   `basic.qos` is their credit, and an ack gives it back. Unacked messages
//!   go back to the queue when the channel closes, as in RabbitMQ.
//! - **Publisher confirms** are sent at the durable watermark: once synced,
//!   for a durable queue.
//! - A message is stored as one record. Its properties are kept as the
//!   publisher sent them (header `amqp-p`), with the exchange (`amqp-x`) and
//!   the routing key (`amqp-k`), so a consumer receives what was published.
//!
//! Consecutive publishes on a channel to the same exchange and routing key
//! that arrive in one read are appended as one batch, so a publisher that
//! pipelines gets the native path's group commit.
//!
//! Not offered: transactions (`tx.*`), exchange-to-exchange bindings, and
//! priorities (accepted, ignored).

pub mod codec;

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rasterizeddb_mq::batch::{BatchBuilder, Batches};
use rasterizeddb_mq::wire::{self, Binding, Code, Durability, ExchangeKind, QueueConfig, Range, XMatch};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, watch};

use self::codec::{In, Value};
use super::partition::Partition;
use super::sub::Subscription;
use super::{Broker, DdlPolicy, ddl_policy};
use crate::core::database::Database;
use crate::core::listen::{self, Listeners, Sock, SocketSpec};

const HDR_PROPS: &[u8] = b"amqp-p";
const HDR_EXCHANGE: &[u8] = b"amqp-x";
const HDR_KEY: &[u8] = b"amqp-k";

// Reply codes.
const NOT_FOUND: u16 = 404;
const ACCESS_REFUSED: u16 = 403;
const PRECONDITION_FAILED: u16 = 406;
const RESOURCE_LOCKED: u16 = 405;
const NO_ROUTE: u16 = 312;
const COMMAND_INVALID: u16 = 503;
const CHANNEL_ERROR: u16 = 504;
const NOT_IMPLEMENTED: u16 = 540;
const INTERNAL_ERROR: u16 = 541;
const FRAME_ERROR: u16 = 501;

#[derive(Clone, Debug)]
pub enum AmqpAuth {
    /// Anyone, any password: loopback only.
    Trust,
    /// PLAIN / AMQPLAIN against the database's roles.
    Roles,
}

#[derive(Clone)]
pub struct AmqpConfig {
    pub addr: String,
    pub port: Option<u16>,
    pub socket: Option<SocketSpec>,
    pub auth: AmqpAuth,
    pub threads: Option<usize>,
    pub tls: Option<tokio_rustls::TlsAcceptor>,
    pub frame_max: u32,
    pub heartbeat: u16,
    /// How long a delivered message may stay unacked before it is handed to
    /// another consumer (RabbitMQ's `consumer_timeout`).
    pub consumer_timeout_ms: u32,
}

impl std::fmt::Debug for AmqpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpConfig").field("addr", &self.addr).field("port", &self.port).field("auth", &self.auth).finish()
    }
}

pub async fn bind(config: &AmqpConfig) -> io::Result<Listeners> {
    if config.port.is_none() && config.socket.is_none() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "AMQP needs --amqp-port"));
    }
    if matches!(config.auth, AmqpAuth::Trust) && config.port.is_some() && !listen::is_loopback(&config.addr) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("--amqp-auth trust on {} would let anyone on the network use every queue; bind 127.0.0.1", config.addr),
        ));
    }
    listen::bind(&config.addr, config.port, config.socket.as_ref()).await
}

struct Shared {
    broker: Arc<Broker>,
    database: rclite::Arc<Database>,
    config: AmqpConfig,
    main: tokio::runtime::Handle,
}

pub async fn serve(listeners: Listeners, database: rclite::Arc<Database>, config: AmqpConfig) -> io::Result<()> {
    let broker = database.broker.get().map_err(|e| io::Error::other(e.to_string()))?;
    log::info!(
        "broker: AMQP 0-9-1 on {}:{} ({})",
        config.addr,
        listeners.tcp_port().unwrap_or(0),
        match config.auth {
            AmqpAuth::Trust => "no authentication",
            AmqpAuth::Roles => "database roles",
        }
    );
    let shared = Arc::new(Shared { broker, database, config, main: tokio::runtime::Handle::current() });
    let threads = listen::default_threads(shared.config.threads);
    listen::serve_per_core(listeners, "amqp", threads, move |socket, peer| {
        let shared = shared.clone();
        async move {
            let Some(socket) = listen::tls_accept(socket, shared.config.tls.as_ref(), &peer, "amqp").await else {
                return;
            };
            let mut conn = Conn::new(shared);
            if let Err(e) = conn.run(socket).await {
                if !matches!(e.kind(), io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe | io::ErrorKind::UnexpectedEof) {
                    log::warn!("amqp: connection from {peer} ended: {e}");
                }
            }
            conn.cleanup();
        }
    })
    .await
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Stage {
    Start,
    Tune,
    Open,
    Running,
    Closing,
}

/// A message being received: its method, then its header, then its body.
struct Incoming {
    exchange: String,
    key: String,
    mandatory: bool,
    props: Vec<u8>,
    size: u64,
    body: Vec<u8>,
    have_header: bool,
}

struct Unacked {
    sub: Arc<Subscription>,
    /// The consumer it went to, for its credit; `None` for a `basic.get`.
    handle: Option<u32>,
    partition: u32,
    offset: u64,
}

#[derive(Default)]
struct Channel {
    prefetch: u16,
    confirm: bool,
    next_seq: u64,
    next_tag: u64,
    unacked: BTreeMap<u64, Unacked>,
    incoming: Option<Incoming>,
    /// Sent a channel.close; frames are dropped until its close-ok.
    closing: bool,
}

struct Consumer {
    channel: u16,
    tag: String,
    sub: Arc<Subscription>,
    member: u64,
    no_ack: bool,
    /// Credit given back as deliveries are made, not as they are acked: a
    /// no-ack or unlimited-prefetch consumer.
    credit_on_delivery: bool,
    queue: String,
}

/// Publishes gathered from one read, appended as one batch.
struct Group {
    channel: u16,
    exchange: String,
    key: String,
    builder: BatchBuilder,
    /// Confirm sequence numbers of its messages, when the channel confirms.
    seqs: Vec<u64>,
}

struct Parked {
    channel: u16,
    seqs: Vec<u64>,
    targets: Vec<(Arc<Partition>, u64)>,
}

struct Conn {
    shared: Arc<Shared>,
    stage: Stage,
    superuser: bool,
    frame_max: usize,
    heartbeat: u16,
    channels: HashMap<u16, Channel>,
    consumers: HashMap<u32, Consumer>,
    next_handle: u32,
    push_tx: mpsc::UnboundedSender<Vec<u8>>,
    push_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    /// Queues this connection declared exclusive: deleted when it closes.
    exclusive: Vec<String>,
    /// Queues declared auto-delete: deleted once their last consumer goes.
    auto_delete: Vec<String>,
    group: Option<Group>,
    parked: Vec<Parked>,
    last_input: Instant,
}

type Flow = Result<(), ()>;

impl Conn {
    fn new(shared: Arc<Shared>) -> Conn {
        let (push_tx, push_rx) = mpsc::unbounded_channel();
        let frame_max = shared.config.frame_max as usize;
        Conn {
            shared,
            stage: Stage::Start,
            superuser: false,
            frame_max,
            heartbeat: 0,
            channels: HashMap::new(),
            consumers: HashMap::new(),
            next_handle: 1,
            push_tx,
            push_rx,
            exclusive: Vec::new(),
            auto_delete: Vec::new(),
            group: None,
            parked: Vec::new(),
            last_input: Instant::now(),
        }
    }

    async fn run(&mut self, mut sock: Sock) -> io::Result<()> {
        let mut head = [0u8; 8];
        sock.read_exact(&mut head).await?;
        if &head != codec::PROTOCOL_HEADER {
            sock.write_all(codec::PROTOCOL_HEADER).await?;
            return Ok(());
        }
        let mut out = Vec::with_capacity(64 * 1024);
        self.send_start(&mut out);
        sock.write_all(&out).await?;
        out.clear();
        let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
        let mut beat = tokio::time::interval(Duration::from_secs(1));
        let mut last_output = Instant::now();
        loop {
            let mut at = 0;
            let mut stop = false;
            loop {
                let max = self.frame_max.max(4096) + 8;
                let f = match codec::parse_frame(&buf[at..], max) {
                    Ok(Some(f)) => f,
                    Ok(None) => break,
                    Err(e) => {
                        self.connection_close(&mut out, FRAME_ERROR, e.0, 0, 0);
                        stop = true;
                        break;
                    }
                };
                let (kind, channel, payload, used) = f;
                self.last_input = Instant::now();
                let flow = self.frame(kind, channel, payload, &mut out).await;
                at += used;
                if flow.is_err() {
                    stop = true;
                    break;
                }
            }
            buf.drain(..at);
            self.flush_group(&mut out);
            self.release_parked(&mut out);
            while let Ok(f) = self.push_rx.try_recv() {
                self.push(&f, &mut out);
            }
            if !out.is_empty() {
                sock.write_all(&out).await?;
                out.clear();
                last_output = Instant::now();
            }
            if stop || self.stage == Stage::Closing && self.channels.is_empty() {
                return Ok(());
            }
            if let Some(need) = codec::frame_need(&buf) {
                if need > buf.capacity() {
                    buf.reserve(need - buf.len());
                }
            }
            if buf.capacity() - buf.len() < 16 * 1024 {
                buf.reserve(64 * 1024);
            }
            let wait = self.park_wait();
            tokio::select! {
                r = sock.read_buf(&mut buf) => {
                    if r? == 0 {
                        return Ok(());
                    }
                }
                Some(f) = self.push_rx.recv() => self.push(&f, &mut out),
                _ = wait => {}
                _ = beat.tick() => {
                    if self.heartbeat > 0 {
                        let hb = Duration::from_secs(self.heartbeat as u64);
                        if self.last_input.elapsed() > hb * 2 {
                            return Err(io::Error::new(io::ErrorKind::TimedOut, "missed heartbeats"));
                        }
                        if last_output.elapsed() >= hb / 2 {
                            codec::heartbeat(&mut out);
                        }
                    }
                }
            }
        }
    }

    fn send_start(&self, out: &mut Vec<u8>) {
        let mut caps = Vec::new();
        for k in [
            "publisher_confirms",
            "basic.nack",
            "consumer_cancel_notify",
            "per_consumer_qos",
            "authentication_failure_close",
        ] {
            caps.push(k.len() as u8);
            caps.extend_from_slice(k.as_bytes());
            caps.extend_from_slice(b"t\x01");
        }
        let version = env!("CARGO_PKG_VERSION");
        codec::method(out, 0, 10, 10, |w| {
            w.u8(0);
            w.u8(9);
            w.table(&[
                ("product", Value::Str(b"RasterizedDB")),
                ("version", Value::Str(version.as_bytes())),
                ("platform", Value::Str(b"Rust")),
                ("capabilities", Value::Table(codec::Table { raw: &caps })),
            ]);
            w.longstr(b"PLAIN AMQPLAIN");
            w.longstr(b"en_US");
        });
    }

    fn connection_close(&mut self, out: &mut Vec<u8>, code: u16, text: &str, class: u16, method: u16) {
        codec::method(out, 0, 10, 50, |w| {
            w.u16(code);
            w.shortstr(text);
            w.u16(class);
            w.u16(method);
        });
        self.stage = Stage::Closing;
    }

    fn channel_close(&mut self, out: &mut Vec<u8>, ch: u16, code: u16, text: &str, class: u16, method: u16) {
        codec::method(out, ch, 20, 40, |w| {
            w.u16(code);
            w.shortstr(text);
            w.u16(class);
            w.u16(method);
        });
        self.drop_channel_state(ch);
        if let Some(c) = self.channels.get_mut(&ch) {
            c.closing = true;
        }
    }

    /// A channel's consumers leave and its unacked messages go back.
    fn drop_channel_state(&mut self, ch: u16) {
        if self.group.as_ref().is_some_and(|g| g.channel == ch) {
            self.group = None;
        }
        let handles: Vec<u32> = self.consumers.iter().filter(|(_, c)| c.channel == ch).map(|(h, _)| *h).collect();
        for h in handles {
            self.cancel_consumer(h);
        }
        if let Some(c) = self.channels.get_mut(&ch) {
            let unacked = std::mem::take(&mut c.unacked);
            requeue(unacked.into_values());
            c.incoming = None;
        }
        self.parked.retain(|p| p.channel != ch);
    }

    fn cancel_consumer(&mut self, handle: u32) {
        if let Some(c) = self.consumers.remove(&handle) {
            c.sub.leave(c.member);
            if self.auto_delete.contains(&c.queue) && c.sub.member_count() == 0 {
                let _ = self.shared.broker.delete_topic(&c.queue, true);
                self.auto_delete.retain(|q| *q != c.queue);
            }
        }
    }

    fn cleanup(&mut self) {
        let chans: Vec<u16> = self.channels.keys().copied().collect();
        for ch in chans {
            self.drop_channel_state(ch);
        }
        for q in std::mem::take(&mut self.exclusive) {
            let _ = self.shared.broker.delete_topic(&q, true);
        }
    }

    async fn frame(&mut self, kind: u8, ch: u16, payload: &[u8], out: &mut Vec<u8>) -> Flow {
        match kind {
            codec::FRAME_HEARTBEAT => Ok(()),
            codec::FRAME_METHOD => {
                if payload.len() < 4 {
                    self.connection_close(out, FRAME_ERROR, "short method frame", 0, 0);
                    return Err(());
                }
                let class = u16::from_be_bytes([payload[0], payload[1]]);
                let method = u16::from_be_bytes([payload[2], payload[3]]);
                let args = &payload[4..];
                if ch == 0 {
                    return self.connection_method(class, method, args, out).await;
                }
                if self.stage != Stage::Running {
                    self.connection_close(out, CHANNEL_ERROR, "channel used before the connection opened", class, method);
                    return Err(());
                }
                // A publish may join the batch being gathered; anything
                // else on any channel lands after it.
                if !(class == 60 && method == 40) {
                    self.flush_group(out);
                }
                if let Some(c) = self.channels.get(&ch) {
                    if c.closing {
                        if class == 20 && method == 41 {
                            self.channels.remove(&ch);
                        }
                        return Ok(());
                    }
                }
                let r = self.channel_method(ch, class, method, args, out);
                if let Err((code, text)) = r {
                    if code >= 500 && code != NOT_IMPLEMENTED {
                        self.connection_close(out, code, &text, class, method);
                        return Err(());
                    }
                    self.channel_close(out, ch, code, &text, class, method);
                }
                Ok(())
            }
            codec::FRAME_HEADER => {
                let Some(c) = self.channels.get_mut(&ch) else { return Ok(()) };
                if c.closing {
                    return Ok(());
                }
                let Some(inc) = c.incoming.as_mut().filter(|i| !i.have_header) else {
                    self.connection_close(out, 505, "content header without a basic.publish", 0, 0);
                    return Err(());
                };
                if payload.len() < 14 {
                    self.connection_close(out, FRAME_ERROR, "short content header", 0, 0);
                    return Err(());
                }
                inc.size = u64::from_be_bytes(payload[4..12].try_into().unwrap());
                inc.props = payload[12..].to_vec();
                inc.have_header = true;
                if inc.size > 512 << 20 {
                    self.connection_close(out, FRAME_ERROR, "message larger than 512 MiB", 0, 0);
                    return Err(());
                }
                inc.body.reserve(inc.size as usize);
                if inc.size == 0 {
                    self.message_complete(ch, out);
                }
                Ok(())
            }
            codec::FRAME_BODY => {
                let Some(c) = self.channels.get_mut(&ch) else { return Ok(()) };
                if c.closing {
                    return Ok(());
                }
                let Some(inc) = c.incoming.as_mut().filter(|i| i.have_header) else {
                    self.connection_close(out, 505, "content body without a header", 0, 0);
                    return Err(());
                };
                inc.body.extend_from_slice(payload);
                if inc.body.len() as u64 > inc.size {
                    self.connection_close(out, FRAME_ERROR, "body longer than its header said", 0, 0);
                    return Err(());
                }
                if inc.body.len() as u64 == inc.size {
                    self.message_complete(ch, out);
                }
                Ok(())
            }
            _ => {
                self.connection_close(out, FRAME_ERROR, "unknown frame type", 0, 0);
                Err(())
            }
        }
    }

    async fn connection_method(&mut self, class: u16, method: u16, args: &[u8], out: &mut Vec<u8>) -> Flow {
        let mut r = In::new(args);
        match (class, method) {
            (10, 11) if self.stage == Stage::Start => {
                let parsed = (|| -> codec::R<(String, Vec<u8>)> {
                    let _props = r.table()?;
                    let mech = r.shortstr()?.to_string();
                    let resp = r.longstr()?.to_vec();
                    let _locale = r.shortstr()?;
                    Ok((mech, resp))
                })();
                let Ok((mech, resp)) = parsed else {
                    self.connection_close(out, FRAME_ERROR, "malformed start-ok", 10, 11);
                    return Err(());
                };
                let creds = match mech.as_str() {
                    "PLAIN" => {
                        let mut it = resp.split(|b| *b == 0);
                        let _ = it.next();
                        match (it.next(), it.next()) {
                            (Some(u), Some(p)) => Some((String::from_utf8_lossy(u).into_owned(), String::from_utf8_lossy(p).into_owned())),
                            _ => None,
                        }
                    }
                    "AMQPLAIN" => codec::Table::parse(&resp).ok().and_then(|t| {
                        let u = t.get("LOGIN")?.str()?.to_string();
                        let p = t.get("PASSWORD")?.str()?.to_string();
                        Some((u, p))
                    }),
                    _ => None,
                };
                match &self.shared.config.auth {
                    AmqpAuth::Trust => self.superuser = true,
                    AmqpAuth::Roles => {
                        let Some((user, pass)) = creds else {
                            self.connection_close(out, ACCESS_REFUSED, "unsupported mechanism or malformed credentials", 10, 11);
                            return Err(());
                        };
                        match listen::login(&self.shared.database, &self.shared.main, &user, &pass, "amqp").await {
                            Ok(su) => self.superuser = su,
                            Err(listen::LoginError::Denied) => {
                                self.connection_close(out, ACCESS_REFUSED, "ACCESS_REFUSED - login refused", 10, 11);
                                return Err(());
                            }
                            Err(listen::LoginError::NotAdmitted(e)) => {
                                self.connection_close(out, ACCESS_REFUSED, &e, 10, 11);
                                return Err(());
                            }
                        }
                    }
                }
                let frame_max = self.shared.config.frame_max;
                let heartbeat = self.shared.config.heartbeat;
                codec::method(out, 0, 10, 30, |w| {
                    w.u16(2047);
                    w.u32(frame_max);
                    w.u16(heartbeat);
                });
                self.stage = Stage::Tune;
                Ok(())
            }
            (10, 31) if self.stage == Stage::Tune => {
                let _channel_max = r.u16().unwrap_or(0);
                let frame_max = r.u32().unwrap_or(0);
                let heartbeat = r.u16().unwrap_or(0);
                if frame_max != 0 {
                    self.frame_max = (frame_max as usize).clamp(4096, self.shared.config.frame_max as usize);
                }
                self.heartbeat = heartbeat;
                self.stage = Stage::Open;
                Ok(())
            }
            (10, 40) if self.stage == Stage::Open => {
                codec::method(out, 0, 10, 41, |w| w.shortstr(""));
                self.stage = Stage::Running;
                Ok(())
            }
            (10, 50) => {
                codec::method(out, 0, 10, 51, |_| {});
                Err(())
            }
            (10, 51) => Err(()),
            _ => {
                self.connection_close(out, COMMAND_INVALID, "unexpected connection method", class, method);
                Err(())
            }
        }
    }

    fn check_ddl(&self) -> Result<(), (u16, String)> {
        if ddl_policy() == DdlPolicy::Superuser && !self.superuser {
            return Err((ACCESS_REFUSED, "declaring and deleting needs a superuser here".into()));
        }
        Ok(())
    }

    fn channel_method(&mut self, ch: u16, class: u16, method: u16, args: &[u8], out: &mut Vec<u8>) -> Result<(), (u16, String)> {
        let malformed = |e: codec::Malformed| (FRAME_ERROR, format!("malformed arguments: {}", e.0));
        let mut r = In::new(args);
        let broker = self.shared.broker.clone();
        if class == 20 && method == 10 {
            if self.channels.contains_key(&ch) {
                return Err((CHANNEL_ERROR, format!("channel {ch} is already open")));
            }
            self.channels.insert(ch, Channel::default());
            codec::method(out, ch, 20, 11, |w| w.longstr(b""));
            return Ok(());
        }
        if !self.channels.contains_key(&ch) {
            return Err((CHANNEL_ERROR, format!("channel {ch} is not open")));
        }
        match (class, method) {
            (20, 20) => {
                let active = r.bit().map_err(malformed)?;
                codec::method(out, ch, 20, 21, |w| w.bit(active));
            }
            (20, 40) => {
                self.drop_channel_state(ch);
                self.channels.remove(&ch);
                codec::method(out, ch, 20, 41, |_| {});
            }
            (20, 41) => {
                self.channels.remove(&ch);
            }
            // exchange.declare
            (40, 10) => {
                let _ = r.u16().map_err(malformed)?;
                let name = r.shortstr().map_err(malformed)?.to_string();
                let kind = r.shortstr().map_err(malformed)?.to_string();
                let passive = r.bit().map_err(malformed)?;
                let durable = r.bit().map_err(malformed)?;
                let _auto_delete = r.bit().map_err(malformed)?;
                let _internal = r.bit().map_err(malformed)?;
                let no_wait = r.bit().map_err(malformed)?;
                let args = r.table().map_err(malformed)?;
                if passive {
                    if broker.exchange_kind(&name).is_none() {
                        return Err((NOT_FOUND, format!("NOT_FOUND - no exchange '{name}' in vhost '/'")));
                    }
                } else {
                    self.check_ddl()?;
                    if name.is_empty() {
                        return Err((ACCESS_REFUSED, "the default exchange cannot be declared".into()));
                    }
                    let k = ExchangeKind::parse(&kind)
                        .ok_or_else(|| (COMMAND_INVALID, format!("COMMAND_INVALID - unknown exchange type '{kind}'")))?;
                    if name.starts_with("amq.") && broker.exchange_kind(&name).is_none() {
                        return Err((ACCESS_REFUSED, format!("ACCESS_REFUSED - exchange name '{name}' contains reserved prefix 'amq.*'")));
                    }
                    let alternate = args.get("alternate-exchange").and_then(|v| v.str().map(str::to_string));
                    broker
                        .declare_exchange(&name, k, alternate.as_deref(), durable)
                        .map_err(|e| (if e.code == Code::Mismatch { PRECONDITION_FAILED } else { INTERNAL_ERROR }, e.message))?;
                }
                if !no_wait {
                    codec::method(out, ch, 40, 11, |_| {});
                }
            }
            // exchange.delete
            (40, 20) => {
                self.check_ddl()?;
                let _ = r.u16().map_err(malformed)?;
                let name = r.shortstr().map_err(malformed)?.to_string();
                let _if_unused = r.bit().map_err(malformed)?;
                let no_wait = r.bit().map_err(malformed)?;
                broker.delete_exchange(&name, true).map_err(|e| (ACCESS_REFUSED, e.message))?;
                if !no_wait {
                    codec::method(out, ch, 40, 21, |_| {});
                }
            }
            (40, 30) | (40, 40) => return Err((NOT_IMPLEMENTED, "exchange-to-exchange bindings are not supported".into())),
            // queue.declare
            (50, 10) => {
                let _ = r.u16().map_err(malformed)?;
                let mut name = r.shortstr().map_err(malformed)?.to_string();
                let passive = r.bit().map_err(malformed)?;
                let durable = r.bit().map_err(malformed)?;
                let exclusive = r.bit().map_err(malformed)?;
                let auto_delete = r.bit().map_err(malformed)?;
                let no_wait = r.bit().map_err(malformed)?;
                let args = r.table().map_err(malformed)?;
                if name.is_empty() {
                    let mut b = [0u8; 12];
                    let _ = getrandom_fill(&mut b);
                    name = format!("amq.gen-{}", hex(&b));
                }
                let (topic, sub) = if passive {
                    broker.queue(&name).map_err(|_| (NOT_FOUND, format!("NOT_FOUND - no queue '{name}' in vhost '/'")))?
                } else {
                    self.check_ddl()?;
                    if let Some(t) = broker.topic(&name).filter(|t| t.queue.is_none()) {
                        return Err((PRECONDITION_FAILED, format!("PRECONDITION_FAILED - '{}' is a topic, not a queue", t.name)));
                    }
                    let q = queue_config(&args, durable, &self.shared.config, &name);
                    broker.declare_queue(&name, q, true).map_err(|e| match e.code {
                        Code::Mismatch => (PRECONDITION_FAILED, format!("PRECONDITION_FAILED - {}", e.message)),
                        _ => (INTERNAL_ERROR, e.message),
                    })?;
                    if exclusive && !self.exclusive.contains(&name) {
                        self.exclusive.push(name.clone());
                    } else if auto_delete && !self.auto_delete.contains(&name) {
                        self.auto_delete.push(name.clone());
                    }
                    broker.queue(&name).map_err(|e| (INTERNAL_ERROR, e.message))?
                };
                let _ = topic;
                if !no_wait {
                    let ready = sub.backlog().saturating_sub(sub.inflight() as u64);
                    let consumers = sub.member_count();
                    codec::method(out, ch, 50, 11, |w| {
                        w.shortstr(&name);
                        w.u32(ready.min(u32::MAX as u64) as u32);
                        w.u32(consumers as u32);
                    });
                }
            }
            // queue.bind / queue.unbind
            (50, 20) | (50, 50) => {
                self.check_ddl()?;
                let _ = r.u16().map_err(malformed)?;
                let queue = r.shortstr().map_err(malformed)?.to_string();
                let exchange = r.shortstr().map_err(malformed)?.to_string();
                let key = r.shortstr().map_err(malformed)?.to_string();
                let no_wait = if method == 20 { r.bit().map_err(malformed)? } else { false };
                let args = r.table().map_err(malformed)?;
                if exchange.is_empty() {
                    return Err((ACCESS_REFUSED, "ACCESS_REFUSED - operation not permitted on the default exchange".into()));
                }
                let x_match = match args.get("x-match").and_then(|v| v.str().map(str::to_ascii_lowercase)).as_deref() {
                    Some("any") | Some("any-with-x") => XMatch::Any,
                    _ => XMatch::All,
                };
                let mut bargs = Vec::new();
                for e in args.entries().flatten() {
                    if let Some(b) = e.1.match_bytes() {
                        bargs.push((e.0.to_string(), b));
                    }
                }
                let b = Binding { exchange, topic: queue, key, x_match, args: bargs };
                let r = if method == 20 { broker.bind(b) } else { broker.unbind(&b) };
                r.map_err(|e| (NOT_FOUND, format!("NOT_FOUND - {}", e.message)))?;
                if method == 20 && !no_wait {
                    codec::method(out, ch, 50, 21, |_| {});
                } else if method == 50 {
                    codec::method(out, ch, 50, 51, |_| {});
                }
            }
            // queue.purge
            (50, 30) => {
                let _ = r.u16().map_err(malformed)?;
                let queue = r.shortstr().map_err(malformed)?.to_string();
                let no_wait = r.bit().map_err(malformed)?;
                let (_, sub) = broker.queue(&queue).map_err(|_| (NOT_FOUND, format!("NOT_FOUND - no queue '{queue}'")))?;
                let n = sub.purge();
                if !no_wait {
                    codec::method(out, ch, 50, 31, |w| w.u32(n.min(u32::MAX as u64) as u32));
                }
            }
            // queue.delete
            (50, 40) => {
                self.check_ddl()?;
                let _ = r.u16().map_err(malformed)?;
                let queue = r.shortstr().map_err(malformed)?.to_string();
                let if_unused = r.bit().map_err(malformed)?;
                let if_empty = r.bit().map_err(malformed)?;
                let no_wait = r.bit().map_err(malformed)?;
                let n = match broker.queue(&queue) {
                    Ok((_, sub)) => {
                        if if_unused && sub.member_count() > 0 {
                            return Err((PRECONDITION_FAILED, format!("PRECONDITION_FAILED - queue '{queue}' in use")));
                        }
                        let n = sub.backlog();
                        if if_empty && n > 0 {
                            return Err((PRECONDITION_FAILED, format!("PRECONDITION_FAILED - queue '{queue}' not empty")));
                        }
                        broker.delete_topic(&queue, true).map_err(|e| (INTERNAL_ERROR, e.message))?;
                        n
                    }
                    Err(_) => 0,
                };
                self.exclusive.retain(|q| *q != queue);
                if !no_wait {
                    codec::method(out, ch, 50, 41, |w| w.u32(n.min(u32::MAX as u64) as u32));
                }
            }
            // basic.qos
            (60, 10) => {
                let _size = r.u32().map_err(malformed)?;
                let count = r.u16().map_err(malformed)?;
                let _global = r.bit().map_err(malformed)?;
                self.channels.get_mut(&ch).expect("open").prefetch = count;
                codec::method(out, ch, 60, 11, |_| {});
            }
            // basic.consume
            (60, 20) => {
                let _ = r.u16().map_err(malformed)?;
                let queue = r.shortstr().map_err(malformed)?.to_string();
                let mut tag = r.shortstr().map_err(malformed)?.to_string();
                let _no_local = r.bit().map_err(malformed)?;
                let no_ack = r.bit().map_err(malformed)?;
                let exclusive = r.bit().map_err(malformed)?;
                let no_wait = r.bit().map_err(malformed)?;
                let _args = r.table().map_err(malformed)?;
                let (_, sub) = broker.queue(&queue).map_err(|_| (NOT_FOUND, format!("NOT_FOUND - no queue '{queue}' in vhost '/'")))?;
                if exclusive && sub.member_count() > 0 {
                    return Err((RESOURCE_LOCKED, format!("RESOURCE_LOCKED - cannot obtain exclusive access to queue '{queue}'")));
                }
                let handle = self.next_handle;
                self.next_handle += 1;
                if tag.is_empty() {
                    tag = format!("amq.ctag-{handle}");
                }
                if self.consumers.values().any(|c| c.channel == ch && c.tag == tag) {
                    return Err((530, format!("NOT_ALLOWED - attempt to reuse consumer tag '{tag}'")));
                }
                let prefetch = self.channels[&ch].prefetch;
                let credit_on_delivery = no_ack || prefetch == 0;
                let credits = if prefetch == 0 { 1024 } else { prefetch as u32 };
                let (member, _) = sub
                    .join(handle, credits, self.push_tx.clone(), &self.shared.main)
                    .map_err(|e| (INTERNAL_ERROR, e.message))?;
                self.consumers.insert(handle, Consumer { channel: ch, tag: tag.clone(), sub, member, no_ack, credit_on_delivery, queue });
                if !no_wait {
                    codec::method(out, ch, 60, 21, |w| w.shortstr(&tag));
                }
            }
            // basic.cancel
            (60, 30) => {
                let tag = r.shortstr().map_err(malformed)?.to_string();
                let no_wait = r.bit().map_err(malformed)?;
                let h = self.consumers.iter().find(|(_, c)| c.channel == ch && c.tag == tag).map(|(h, _)| *h);
                if let Some(h) = h {
                    self.cancel_consumer(h);
                }
                if !no_wait {
                    codec::method(out, ch, 60, 31, |w| w.shortstr(&tag));
                }
            }
            // basic.publish
            (60, 40) => {
                let _ = r.u16().map_err(malformed)?;
                let exchange = r.shortstr().map_err(malformed)?.to_string();
                let key = r.shortstr().map_err(malformed)?.to_string();
                let mandatory = r.bit().map_err(malformed)?;
                let _immediate = r.bit().map_err(malformed)?;
                let c = self.channels.get_mut(&ch).expect("open");
                c.incoming = Some(Incoming { exchange, key, mandatory, props: Vec::new(), size: 0, body: Vec::new(), have_header: false });
            }
            // basic.get
            (60, 70) => {
                let _ = r.u16().map_err(malformed)?;
                let queue = r.shortstr().map_err(malformed)?.to_string();
                let no_ack = r.bit().map_err(malformed)?;
                let (_, sub) = broker.queue(&queue).map_err(|_| (NOT_FOUND, format!("NOT_FOUND - no queue '{queue}' in vhost '/'")))?;
                let taken = sub.take(1, no_ack, 0).map_err(|e| (INTERNAL_ERROR, e.message))?;
                match taken.into_iter().next() {
                    None => codec::method(out, ch, 60, 72, |w| w.shortstr("")),
                    Some(t) => {
                        let c = self.channels.get_mut(&ch).expect("open");
                        c.next_tag += 1;
                        let tag = c.next_tag;
                        if !no_ack {
                            c.unacked.insert(tag, Unacked { sub: sub.clone(), handle: None, partition: t.partition, offset: t.offset });
                        }
                        let hdr = |n: &[u8]| t.headers.iter().find(|(k, _)| k == n).map(|(_, v)| v.as_slice());
                        let exchange = hdr(HDR_EXCHANGE).map(|v| String::from_utf8_lossy(v).into_owned()).unwrap_or_default();
                        let key = hdr(HDR_KEY).map(|v| String::from_utf8_lossy(v).into_owned()).unwrap_or_else(|| queue.clone());
                        let left = sub.backlog().saturating_sub(sub.inflight() as u64);
                        codec::method(out, ch, 60, 71, |w| {
                            w.u64(tag);
                            w.bit(false);
                            w.shortstr(&exchange);
                            w.shortstr(&key);
                            w.u32(left.min(u32::MAX as u64) as u32);
                        });
                        codec::content(out, ch, hdr(HDR_PROPS).unwrap_or(&[]), &t.value, self.frame_max);
                    }
                }
            }
            // basic.ack
            (60, 80) => {
                let tag = r.u64().map_err(malformed)?;
                let multiple = r.bit().map_err(malformed)?;
                let taken = self.take_tags(ch, tag, multiple)?;
                self.settle(taken, Settle::Ack);
            }
            // basic.reject
            (60, 90) => {
                let tag = r.u64().map_err(malformed)?;
                let requeue = r.bit().map_err(malformed)?;
                let taken = self.take_tags(ch, tag, false)?;
                self.settle(taken, if requeue { Settle::Requeue } else { Settle::Dead });
            }
            // basic.nack
            (60, 120) => {
                let tag = r.u64().map_err(malformed)?;
                let multiple = r.bit().map_err(malformed)?;
                let requeue = r.bit().map_err(malformed)?;
                let taken = self.take_tags(ch, tag, multiple)?;
                self.settle(taken, if requeue { Settle::Requeue } else { Settle::Dead });
            }
            // basic.recover-async / basic.recover
            (60, 100) | (60, 110) => {
                let _requeue = r.bit().map_err(malformed)?;
                let all = std::mem::take(&mut self.channels.get_mut(&ch).expect("open").unacked);
                self.settle(all.into_values().collect(), Settle::Requeue);
                if method == 110 {
                    codec::method(out, ch, 60, 111, |_| {});
                }
            }
            // confirm.select
            (85, 10) => {
                let no_wait = r.bit().map_err(malformed)?;
                let c = self.channels.get_mut(&ch).expect("open");
                c.confirm = true;
                if !no_wait {
                    codec::method(out, ch, 85, 11, |_| {});
                }
            }
            (90, _) => return Err((NOT_IMPLEMENTED, "transactions (tx.*) are not supported; use publisher confirms".into())),
            _ => return Err((COMMAND_INVALID, format!("unexpected method {class}.{method}"))),
        }
        Ok(())
    }

    /// The unacked entries a delivery tag (or every tag up to it) names.
    fn take_tags(&mut self, ch: u16, tag: u64, multiple: bool) -> Result<Vec<Unacked>, (u16, String)> {
        let c = self.channels.get_mut(&ch).expect("open");
        if multiple {
            let keys: Vec<u64> = if tag == 0 { c.unacked.keys().copied().collect() } else { c.unacked.range(..=tag).map(|(k, _)| *k).collect() };
            Ok(keys.into_iter().filter_map(|k| c.unacked.remove(&k)).collect())
        } else {
            match c.unacked.remove(&tag) {
                Some(u) => Ok(vec![u]),
                None => Err((PRECONDITION_FAILED, format!("PRECONDITION_FAILED - unknown delivery tag {tag}"))),
            }
        }
    }

    fn settle(&mut self, taken: Vec<Unacked>, how: Settle) {
        // Grouped per subscription, as ranges, and credit back per consumer.
        let mut per: Vec<(Arc<Subscription>, Vec<Range>)> = Vec::new();
        let mut credit: HashMap<u32, u32> = HashMap::new();
        for u in taken {
            if let Some(h) = u.handle {
                *credit.entry(h).or_default() += 1;
            }
            let r = Range { partition: u.partition, offset: u.offset, count: 1 };
            match per.iter_mut().find(|(s, _)| Arc::ptr_eq(s, &u.sub)) {
                Some((_, v)) => v.push(r),
                None => per.push((u.sub, vec![r])),
            }
        }
        for (sub, ranges) in per {
            match how {
                Settle::Ack => {
                    let _ = sub.ack(&ranges);
                }
                Settle::Requeue => {
                    let _ = sub.nack(&ranges, 0);
                }
                Settle::Dead => sub.reject(&ranges),
            }
        }
        for (h, n) in credit {
            if let Some(c) = self.consumers.get(&h) {
                if !c.credit_on_delivery {
                    c.sub.credit(c.member, n);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Publishing
    // -----------------------------------------------------------------------

    fn message_complete(&mut self, ch: u16, out: &mut Vec<u8>) {
        let c = self.channels.get_mut(&ch).expect("open");
        let Some(inc) = c.incoming.take() else { return };
        let seq = if c.confirm {
            c.next_seq += 1;
            Some(c.next_seq)
        } else {
            None
        };
        let broker = self.shared.broker.clone();
        let kind = if inc.exchange.is_empty() { Some(ExchangeKind::Direct) } else { broker.exchange_kind(&inc.exchange) };
        let Some(kind) = kind else {
            self.channel_close(out, ch, NOT_FOUND, &format!("NOT_FOUND - no exchange '{}' in vhost '/'", inc.exchange), 60, 40);
            return;
        };
        // Headers routing reads each message's own headers; a mandatory
        // message needs its own answer. Neither joins a batch.
        let alone = inc.mandatory || kind == ExchangeKind::Headers;
        let joins = !alone
            && self.group.as_ref().is_some_and(|g| g.channel == ch && g.exchange == inc.exchange && g.key == inc.key);
        if !joins {
            self.flush_group(out);
        }
        let mut extra: Vec<(String, Vec<u8>)> = Vec::new();
        if kind == ExchangeKind::Headers {
            if let Ok(p) = codec::parse_props(&inc.props) {
                if let Some(t) = p.headers {
                    for (k, v) in t.entries().flatten() {
                        if let Some(b) = v.match_bytes() {
                            extra.push((k.to_string(), b));
                        }
                    }
                }
            }
        }
        let g = self.group.get_or_insert_with(|| Group {
            channel: ch,
            exchange: inc.exchange.clone(),
            key: inc.key.clone(),
            builder: BatchBuilder::with_capacity(inc.body.len() + 256),
            seqs: Vec::new(),
        });
        let mut headers: Vec<(&[u8], &[u8])> = Vec::with_capacity(3 + extra.len());
        if inc.props.len() > 2 {
            headers.push((HDR_PROPS, &inc.props));
        }
        if !inc.exchange.is_empty() {
            headers.push((HDR_EXCHANGE, inc.exchange.as_bytes()));
        }
        headers.push((HDR_KEY, inc.key.as_bytes()));
        for (k, v) in &extra {
            headers.push((k.as_bytes(), v));
        }
        if g.builder.push(None, &inc.body, &headers).is_err() {
            // Past the batch limit: send what is gathered and start again.
            self.flush_group(out);
            let g = self.group.get_or_insert_with(|| Group {
                channel: ch,
                exchange: inc.exchange.clone(),
                key: inc.key.clone(),
                builder: BatchBuilder::with_capacity(inc.body.len() + 256),
                seqs: Vec::new(),
            });
            let _ = g.builder.push(None, &inc.body, &headers);
        }
        if let Some(s) = seq {
            self.group.as_mut().expect("group").seqs.push(s);
        }
        if alone {
            let routed = self.flush_group(out);
            if inc.mandatory && routed == Some(0) {
                codec::method(out, ch, 60, 50, |w| {
                    w.u16(NO_ROUTE);
                    w.shortstr("NO_ROUTE");
                    w.shortstr(&inc.exchange);
                    w.shortstr(&inc.key);
                });
                codec::content(out, ch, &inc.props, &inc.body, self.frame_max);
            }
        }
    }

    /// Appends the gathered batch through its exchange. `Some(n)`: it reached
    /// `n` topics.
    fn flush_group(&mut self, out: &mut Vec<u8>) -> Option<usize> {
        let mut g = self.group.take()?;
        if g.builder.is_empty() {
            return Some(0);
        }
        let mut batch = g.builder.finish();
        let ch = g.channel;
        match self.shared.broker.publish(&g.exchange, &g.key, &mut batch, false) {
            Ok(placed) => {
                let n = placed.len();
                if !g.seqs.is_empty() {
                    let targets: Vec<(Arc<Partition>, u64)> = placed
                        .into_iter()
                        .filter(|(p, a)| p.durability == Durability::Sync && p.durable_offset() < a.next_offset)
                        .map(|(p, a)| (p, a.next_offset))
                        .collect();
                    if targets.is_empty() {
                        confirm(out, ch, &g.seqs);
                    } else {
                        self.parked.push(Parked { channel: ch, seqs: std::mem::take(&mut g.seqs), targets });
                    }
                }
                Some(n)
            }
            Err(e) => {
                let code = match e.code {
                    Code::NoExchange => NOT_FOUND,
                    Code::Full => PRECONDITION_FAILED,
                    _ => INTERNAL_ERROR,
                };
                if !g.seqs.is_empty() {
                    // basic.nack for what could not be stored.
                    for s in &g.seqs {
                        codec::method(out, ch, 60, 120, |w| {
                            w.u64(*s);
                            w.bit(false);
                            w.bit(false);
                        });
                    }
                }
                if code == INTERNAL_ERROR {
                    log::warn!("amqp: publish to '{}' failed: {e}", g.exchange);
                } else {
                    self.channel_close(out, ch, code, &e.message, 60, 40);
                }
                None
            }
        }
    }

    fn park_wait(&self) -> impl std::future::Future<Output = ()> + 'static {
        let mut rxs: Vec<watch::Receiver<u64>> = Vec::new();
        for p in &self.parked {
            for (part, _) in &p.targets {
                rxs.push(part.subscribe_durable());
            }
        }
        async move {
            if rxs.is_empty() {
                return std::future::pending().await;
            }
            let _ = futures::future::select_all(rxs.iter_mut().map(|r| Box::pin(r.changed()))).await;
        }
    }

    fn release_parked(&mut self, out: &mut Vec<u8>) {
        if self.parked.is_empty() {
            return;
        }
        let parked = std::mem::take(&mut self.parked);
        for p in parked {
            if p.targets.iter().all(|(part, upto)| part.durable_offset() >= *upto) {
                confirm(out, p.channel, &p.seqs);
            } else if p.targets.iter().any(|(part, upto)| part.is_poisoned() && part.durable_offset() < *upto) {
                for s in &p.seqs {
                    codec::method(out, p.channel, 60, 120, |w| {
                        w.u64(*s);
                        w.bit(false);
                        w.bit(false);
                    });
                }
            } else {
                self.parked.push(p);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Deliveries
    // -----------------------------------------------------------------------

    /// A frame a subscription pushed: native `DELIVER`s become
    /// `basic.deliver`s; `CLOSED` cancels the consumer.
    fn push(&mut self, frame: &[u8], out: &mut Vec<u8>) {
        let Ok(Some(h)) = wire::parse_header(frame) else { return };
        let body = &frame[wire::FRAME_HEADER..wire::FRAME_HEADER + h.body_len];
        let Some(c) = self.consumers.get(&h.corr) else { return };
        let ch = c.channel;
        if h.op == wire::op::CLOSED {
            let tag = c.tag.clone();
            self.consumers.remove(&h.corr);
            codec::method(out, ch, 60, 30, |w| {
                w.shortstr(&tag);
                w.bit(true);
            });
            return;
        }
        if h.op != wire::op::DELIVER {
            return;
        }
        let mut r = wire::Reader::new(body);
        let Ok(partition) = r.u32() else { return };
        let Ok(ranges) = wire::decode_ranges(&mut r, 1 << 20) else { return };
        let data = r.rest();
        let (tag, sub, no_ack, credit_on_delivery, member, queue) =
            (c.tag.clone(), c.sub.clone(), c.no_ack, c.credit_on_delivery, c.member, c.queue.clone());
        let Some(chan) = self.channels.get_mut(&ch) else { return };
        let mut delivered = 0u32;
        let mut acks = Vec::new();
        for b in Batches::trusted(data).flatten() {
            for rec in b.records() {
                if !ranges.iter().any(|g| rec.offset >= g.offset && rec.offset < g.offset + g.count as u64) {
                    continue;
                }
                chan.next_tag += 1;
                let dtag = chan.next_tag;
                if no_ack {
                    acks.push(Range { partition, offset: rec.offset, count: 1 });
                } else {
                    chan.unacked.insert(dtag, Unacked { sub: sub.clone(), handle: Some(h.corr), partition, offset: rec.offset });
                }
                let exchange = rec.header(HDR_EXCHANGE).map(|v| String::from_utf8_lossy(v).into_owned()).unwrap_or_default();
                let key = rec.header(HDR_KEY).map(|v| String::from_utf8_lossy(v).into_owned()).unwrap_or_else(|| queue.clone());
                codec::method(out, ch, 60, 60, |w| {
                    w.shortstr(&tag);
                    w.u64(dtag);
                    w.bit(false);
                    w.shortstr(&exchange);
                    w.shortstr(&key);
                });
                codec::content(out, ch, rec.header(HDR_PROPS).unwrap_or(&[]), rec.value, self.frame_max);
                delivered += 1;
            }
        }
        if !acks.is_empty() {
            let _ = sub.ack(&acks);
        }
        if credit_on_delivery && delivered > 0 {
            sub.credit(member, delivered);
        }
    }
}

#[derive(Clone, Copy)]
enum Settle {
    Ack,
    Requeue,
    Dead,
}

fn requeue(unacked: impl Iterator<Item = Unacked>) {
    for u in unacked {
        let _ = u.sub.nack(&[Range { partition: u.partition, offset: u.offset, count: 1 }], 0);
    }
}

/// `basic.ack` for each confirmed publish. Never `multiple`: an earlier
/// publish may still be waiting for its sync.
fn confirm(out: &mut Vec<u8>, ch: u16, seqs: &[u64]) {
    for s in seqs {
        codec::method(out, ch, 60, 80, |w| {
            w.u64(*s);
            w.bit(false);
        });
    }
}

/// A queue's settings from its declaration.
fn queue_config(args: &codec::Table<'_>, durable: bool, config: &AmqpConfig, name: &str) -> QueueConfig {
    let mut q = QueueConfig {
        durability: if durable { Durability::Sync } else { Durability::Memory },
        ack_timeout_ms: config.consumer_timeout_ms,
        ..QueueConfig::default()
    };
    if let Some(v) = args.get("x-message-ttl").and_then(|v| v.int()) {
        q.message_ttl_ms = v.max(0);
    }
    if let Some(v) = args.get("x-max-length").and_then(|v| v.int()) {
        q.max_length = v.max(0);
    }
    if let Some(v) = args.get("x-delivery-limit").and_then(|v| v.int()) {
        // Redeliveries allowed: deliveries = limit + 1.
        q.max_deliveries = (v.max(0) + 1).min(u32::MAX as i64) as u32;
    }
    if let Some(v) = args.get("x-consumer-timeout").and_then(|v| v.int()) {
        q.ack_timeout_ms = v.clamp(1, u32::MAX as i64) as u32;
    }
    if let Some(dlx) = args.get("x-dead-letter-exchange").and_then(|v| v.str().map(str::to_string)) {
        let key = args.get("x-dead-letter-routing-key").and_then(|v| v.str().map(str::to_string)).unwrap_or_else(|| name.to_string());
        q.dead_letter = if dlx.is_empty() { key } else { format!("@x:{dlx}:{key}") };
    }
    q
}

fn getrandom_fill(b: &mut [u8]) -> Result<(), ()> {
    // The clock and a counter: names need only be unique, not secret.
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let t = super::now_us() as u64;
    for (i, x) in b.iter_mut().enumerate() {
        *x = ((t >> ((i % 8) * 8)) as u8) ^ ((n >> ((i % 8) * 8)) as u8).wrapping_mul(31).wrapping_add(i as u8);
    }
    Ok(())
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

