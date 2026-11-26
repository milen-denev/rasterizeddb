//! The native broker protocol (`rdbmq/1`, docs/broker.md §6) on
//! `--broker-port` and `--broker-socket`.
//!
//! One connection is one task on one I/O thread ([`crate::core::listen`]). It
//! reads whatever the socket has, runs every whole frame in it, and writes
//! every reply with one write. A `PRODUCE` body is stamped where it lies in
//! the read buffer and copied once, into the partition. Replies may go out
//! of order: one that has to wait (a `sync` produce waiting for its group
//! commit, or a long-poll fetch) is parked, and the frames after it run in
//! the meantime. Pushed deliveries come through the connection's outbox and
//! are written with the next replies.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rasterizeddb_mq::batch;
use rasterizeddb_mq::wire::{
    self, Ack, Binding, Code, Durability, ErrorBody, ExchangeKind, F_REPLY, Fetch, Fetched, Produce, Produced,
    QueueConfig, Reader, Subscribe, TopicConfig, Writer, op,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, watch};

use super::partition::Partition;
use super::sub::Subscription;
use super::{Broker, BrokerError, BrokerHandle, DdlPolicy, ddl_policy};
use crate::core::database::Database;
use crate::core::listen::{self, Listeners, Sock, SocketSpec};

/// How clients prove who they are.
#[derive(Clone, Debug)]
pub enum NativeAuth {
    /// Nobody is asked. Only allowed on loopback, and on a socket that only
    /// its owner and group can reach.
    Trust,
    /// The database's roles.
    Roles,
}

#[derive(Clone)]
pub struct NativeConfig {
    pub addr: String,
    pub port: Option<u16>,
    pub socket: Option<SocketSpec>,
    pub auth: NativeAuth,
    pub threads: Option<usize>,
    pub tls: Option<tokio_rustls::TlsAcceptor>,
}

impl std::fmt::Debug for NativeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeConfig")
            .field("addr", &self.addr)
            .field("port", &self.port)
            .field("socket", &self.socket)
            .field("auth", &self.auth)
            .field("threads", &self.threads)
            .field("tls", &self.tls.is_some())
            .finish()
    }
}

/// A frame before `AUTH` may be at most this big.
const UNAUTH_FRAME: usize = 64 * 1024;
/// Output past this is written mid-batch rather than buffered further.
const FLUSH_AT: usize = 1 << 20;
/// The most a single fetch returns.
const MAX_FETCH: usize = 64 << 20;
/// The longest a fetch may wait.
const MAX_WAIT: Duration = Duration::from_secs(60);

/// Binds the listeners, refusing an unauthenticated one anyone can reach.
pub async fn bind(config: &NativeConfig) -> io::Result<Listeners> {
    if config.port.is_none() && config.socket.is_none() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "the broker needs --broker-port, --broker-socket, or both"));
    }
    if matches!(config.auth, NativeAuth::Trust) {
        if config.port.is_some() && !listen::is_loopback(&config.addr) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "--broker-auth trust on {} would let anyone on the network read and write every topic; \
                     bind 127.0.0.1 or authenticate with database roles",
                    config.addr
                ),
            ));
        }
        if let Some(s) = config.socket.as_ref().filter(|s| s.admits_others()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "--broker-auth trust on unix:{} with mode {:o} would let every user on the machine read and \
                     write every topic; use a --socket-mode such as 660, or authenticate with database roles",
                    s.path.display(),
                    s.mode
                ),
            ));
        }
    }
    listen::bind(&config.addr, config.port, config.socket.as_ref()).await
}

/// Serves the native protocol until a listener fails.
pub async fn serve(listeners: Listeners, database: rclite::Arc<Database>, config: NativeConfig) -> io::Result<()> {
    let handle = database.broker.clone();
    // Open the broker now: a damaged log stops the start-up with its reason.
    handle.get().map_err(|e| io::Error::other(e.to_string()))?;
    let auth = match config.auth {
        NativeAuth::Trust => "no authentication",
        NativeAuth::Roles => "database roles",
    };
    if listeners.has_tcp() {
        let tls = if config.tls.is_some() { ", TLS" } else { "" };
        log::info!(
            "broker: native protocol on {}:{} ({auth}{tls})",
            config.addr,
            listeners.tcp_port().unwrap_or(0)
        );
    }
    if let Some(p) = listeners.socket_path() {
        log::info!("broker: native protocol on unix:{} ({auth})", p.display());
    }
    let main = tokio::runtime::Handle::current();
    let config = Arc::new(config);
    let threads = listen::default_threads(config.threads);
    listen::serve_per_core(listeners, "broker", threads, move |socket, peer| {
        let (database, handle, config, main) = (database.clone(), handle.clone(), config.clone(), main.clone());
        async move {
            let Some(socket) = listen::tls_accept(socket, config.tls.as_ref(), &peer, "broker").await else {
                return;
            };
            let trust = matches!(config.auth, NativeAuth::Trust);
            let mut conn = match Conn::new(database, handle, main, trust) {
                Ok(c) => c,
                Err(e) => {
                    log::warn!("broker: refusing {peer}: {e}");
                    return;
                }
            };
            if let Err(e) = conn.run(socket).await {
                if !matches!(e.kind(), io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe | io::ErrorKind::UnexpectedEof) {
                    log::warn!("broker: connection from {peer} ended: {e}");
                }
            }
            conn.cleanup();
        }
    })
    .await
}

/// A reply that waits for something.
enum Parked {
    /// A produce or publish, until every partition named is durable to its
    /// offset.
    Durable { corr: u32, targets: Vec<(Arc<Partition>, u64)>, frame: Vec<u8> },
    /// A fetch, until data arrives or its deadline.
    Fetch { corr: u32, part: Arc<Partition>, req: Fetch, deadline: Instant },
}

pub(crate) struct Conn {
    database: rclite::Arc<Database>,
    broker: Arc<Broker>,
    main: tokio::runtime::Handle,
    authed: bool,
    superuser: bool,
    user: String,
    subs: HashMap<u32, (Arc<Subscription>, u64)>,
    next_handle: u32,
    push_tx: mpsc::UnboundedSender<Vec<u8>>,
    push_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    parked: Vec<Parked>,
}

impl Conn {
    pub(crate) fn new(
        database: rclite::Arc<Database>,
        handle: BrokerHandle,
        main: tokio::runtime::Handle,
        trust: bool,
    ) -> Result<Conn, BrokerError> {
        let broker = handle.get()?;
        let (push_tx, push_rx) = mpsc::unbounded_channel();
        Ok(Conn {
            database,
            broker,
            main,
            authed: trust,
            superuser: trust,
            user: String::new(),
            subs: HashMap::new(),
            next_handle: 1,
            push_tx,
            push_rx,
            parked: Vec::new(),
        })
    }

    async fn run(&mut self, mut sock: Sock) -> io::Result<()> {
        let mut inbuf: Vec<u8> = Vec::with_capacity(64 * 1024);
        let mut out: Vec<u8> = Vec::with_capacity(64 * 1024);
        loop {
            // Frames already in the buffer run before reading more.
            let mut at = 0;
            loop {
                let h = match wire::parse_header(&inbuf[at..]) {
                    Ok(Some(h)) => h,
                    Ok(None) => break,
                    Err(e) => {
                        wire::error_frame(&mut out, 0, 0, Code::Protocol, &e.to_string());
                        sock.write_all(&out).await?;
                        return Ok(());
                    }
                };
                if !self.authed && h.body_len > UNAUTH_FRAME {
                    wire::error_frame(&mut out, h.op, h.corr, Code::AuthRequired, "authenticate first");
                    sock.write_all(&out).await?;
                    return Ok(());
                }
                let body_at = at + wire::FRAME_HEADER;
                let end = body_at + h.body_len;
                let flow = self.handle(h, &mut inbuf[body_at..end], &mut out).await;
                at = end;
                if out.len() >= FLUSH_AT {
                    sock.write_all(&out).await?;
                    out.clear();
                }
                if flow.is_err() {
                    sock.write_all(&out).await?;
                    return Ok(());
                }
            }
            inbuf.drain(..at);
            self.release_parked(&mut out);
            while let Ok(f) = self.push_rx.try_recv() {
                out.extend_from_slice(&f);
                if out.len() >= FLUSH_AT {
                    break;
                }
            }
            if !out.is_empty() {
                sock.write_all(&out).await?;
                out.clear();
            }
            // A frame larger than the buffer's room: make room for all of it.
            if let Some(need) = wire::frame_need(&inbuf) {
                if need > wire::MAX_FRAME {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "frame too large"));
                }
                if need > inbuf.capacity() {
                    inbuf.reserve(need - inbuf.len());
                }
            }
            if inbuf.capacity() - inbuf.len() < 16 * 1024 {
                inbuf.reserve(64 * 1024);
            }
            let wait = self.park_wait();
            tokio::select! {
                r = sock.read_buf(&mut inbuf) => {
                    if r? == 0 {
                        return Ok(());
                    }
                }
                Some(f) = self.push_rx.recv() => out.extend_from_slice(&f),
                _ = wait => {}
            }
        }
    }

    /// Resolves when a parked reply may be ready: a watched watermark moved,
    /// or a fetch's deadline passed. Pending forever when nothing is parked.
    fn park_wait(&self) -> impl std::future::Future<Output = ()> + 'static {
        let mut rxs: Vec<watch::Receiver<u64>> = Vec::new();
        let mut deadline: Option<Instant> = None;
        for p in &self.parked {
            match p {
                Parked::Durable { targets, .. } => {
                    for (part, _) in targets {
                        rxs.push(part.subscribe_durable());
                    }
                }
                Parked::Fetch { part, deadline: d, .. } => {
                    rxs.push(part.subscribe_appends());
                    deadline = Some(deadline.map_or(*d, |x: Instant| x.min(*d)));
                }
            }
        }
        async move {
            if rxs.is_empty() {
                return std::future::pending().await;
            }
            // A receiver made now has seen the current value, so it wakes on
            // the next change; `release_parked` ran just before and checked
            // the current values.
            let changed = futures::future::select_all(rxs.iter_mut().map(|r| Box::pin(r.changed())));
            match deadline {
                Some(d) => {
                    tokio::select! {
                        _ = changed => {}
                        _ = tokio::time::sleep_until(d.into()) => {}
                    }
                }
                None => {
                    let _ = changed.await;
                }
            }
        }
    }

    /// Sends every parked reply whose condition now holds.
    fn release_parked(&mut self, out: &mut Vec<u8>) {
        if self.parked.is_empty() {
            return;
        }
        let now = Instant::now();
        let parked = std::mem::take(&mut self.parked);
        for p in parked {
            match p {
                Parked::Durable { corr, targets, frame } => {
                    if targets.iter().any(|(part, upto)| part.is_poisoned() && part.durable_offset() < *upto) {
                        wire::error_frame(out, op::PRODUCE, corr, Code::Storage, "the write failed; see the server log");
                    } else if targets.iter().all(|(part, upto)| part.durable_offset() >= *upto) {
                        out.extend_from_slice(&frame);
                    } else {
                        self.parked.push(Parked::Durable { corr, targets, frame });
                    }
                }
                Parked::Fetch { corr, part, req, deadline } => {
                    let have = part.next_offset().saturating_sub(req.offset);
                    if have > 0 || now >= deadline {
                        fetch_reply(out, corr, &part, &req);
                    } else {
                        self.parked.push(Parked::Fetch { corr, part, req, deadline });
                    }
                }
            }
        }
    }

    fn cleanup(&mut self) {
        for (_, (sub, member)) in self.subs.drain() {
            sub.leave(member);
        }
    }

    fn check_ddl(&self) -> Result<(), BrokerError> {
        if ddl_policy() == DdlPolicy::Superuser && !self.superuser {
            return Err(BrokerError::new(Code::Permission, "creating and removing broker objects needs a superuser here"));
        }
        Ok(())
    }

    /// Runs one frame. `Err` closes the connection after the output is sent.
    async fn handle(&mut self, h: wire::Header, body: &mut [u8], out: &mut Vec<u8>) -> Result<(), ()> {
        if h.flags & F_REPLY != 0 {
            wire::error_frame(out, h.op, h.corr, Code::Protocol, "a client does not send replies");
            return Err(());
        }
        if !self.authed && !matches!(h.op, op::HELLO | op::AUTH | op::PING) {
            wire::error_frame(out, h.op, h.corr, Code::AuthRequired, "authenticate first");
            return Ok(());
        }
        let r = match h.op {
            op::AUTH => self.auth(h.corr, body, out).await,
            _ => self.dispatch(h, body, out),
        };
        if let Err(e) = r {
            wire::error_frame(out, h.op, h.corr, e.code, &e.message);
        }
        Ok(())
    }

    async fn auth(&mut self, corr: u32, body: &[u8], out: &mut Vec<u8>) -> Result<(), BrokerError> {
        let mut r = Reader::new(body);
        let user = r.string().map_err(proto)?;
        let password = r.string().map_err(proto)?;
        match listen::login(&self.database, &self.main, &user, &password, "broker").await {
            Ok(superuser) => {
                self.authed = true;
                self.superuser = superuser;
                self.user = user;
                wire::frame(out, op::AUTH, F_REPLY, corr, |w| w.bool(superuser));
                Ok(())
            }
            Err(listen::LoginError::Denied) => Err(BrokerError::new(Code::AuthFailed, "invalid user name or password")),
            Err(listen::LoginError::NotAdmitted(e)) => Err(BrokerError::new(Code::Permission, e)),
        }
    }

    fn dispatch(&mut self, h: wire::Header, body: &mut [u8], out: &mut Vec<u8>) -> Result<(), BrokerError> {
        let corr = h.corr;
        match h.op {
            op::HELLO => {
                let mut r = Reader::new(body);
                let version = r.u16().map_err(proto)?;
                if version != wire::VERSION {
                    return Err(BrokerError::new(
                        Code::Unsupported,
                        format!("protocol version {version}; this server speaks {}", wire::VERSION),
                    ));
                }
                wire::frame(out, op::HELLO, F_REPLY, corr, |w| {
                    w.u16(wire::VERSION);
                    w.str(concat!("rasterizeddb ", env!("CARGO_PKG_VERSION")));
                    w.u32(wire::MAX_FRAME as u32);
                    w.u32(batch::MAX_BATCH as u32);
                    w.bool(!self.authed);
                });
            }
            op::PING => wire::frame(out, op::PING, F_REPLY, corr, |_| {}),
            op::PRODUCE => self.produce(corr, body, out)?,
            op::FETCH => {
                let req = Fetch::decode(&mut Reader::new(body)).map_err(proto)?;
                let topic = self.broker.require_topic_id(req.topic)?;
                let part = topic.partition(req.partition)?.clone();
                if req.offset < part.log_start() {
                    return Err(BrokerError::new(
                        Code::OffsetOutOfRange,
                        format!("offset {} is before the log's start {}", req.offset, part.log_start()),
                    ));
                }
                if part.next_offset() > req.offset || req.max_wait_ms == 0 {
                    fetch_reply(out, corr, &part, &req);
                } else {
                    let wait = Duration::from_millis(req.max_wait_ms as u64).min(MAX_WAIT);
                    self.parked.push(Parked::Fetch { corr, part, req, deadline: Instant::now() + wait });
                }
            }
            op::LIST_OFFSETS => {
                let mut r = Reader::new(body);
                let topic = self.broker.require_topic_id(r.u32().map_err(proto)?)?;
                let part = topic.partition(r.u32().map_err(proto)?)?;
                let at = r.i64().map_err(proto)?;
                let offset = match at {
                    wire::LATEST => part.next_offset(),
                    wire::EARLIEST => part.log_start(),
                    t => part.offset_for_time(t)?,
                };
                wire::frame(out, op::LIST_OFFSETS, F_REPLY, corr, |w| {
                    w.u64(offset);
                    w.u64(part.log_start());
                    w.u64(part.next_offset())
                });
            }
            op::RESOLVE => {
                let name = Reader::new(body).string().map_err(proto)?;
                let t = self.broker.require_topic(&name)?;
                wire::frame(out, op::RESOLVE, F_REPLY, corr, |w| {
                    w.u32(t.id);
                    w.u32(t.partition_count());
                    w.u8(t.config().durability as u8);
                    w.bool(t.queue.is_some())
                });
            }
            op::TOPIC_CREATE => {
                self.check_ddl()?;
                let mut r = Reader::new(body);
                let name = r.string().map_err(proto)?;
                let config = TopicConfig::decode(&mut r).map_err(proto)?;
                let if_not_exists = r.bool().map_err(proto)?;
                let t = self.broker.create_topic(&name, config, if_not_exists)?;
                wire::frame(out, op::TOPIC_CREATE, F_REPLY, corr, |w| {
                    w.u32(t.id);
                    w.u32(t.partition_count())
                });
            }
            op::TOPIC_DELETE => {
                self.check_ddl()?;
                let mut r = Reader::new(body);
                let name = r.string().map_err(proto)?;
                let if_exists = r.bool().map_err(proto)?;
                let done = self.broker.delete_topic(&name, if_exists)?;
                wire::frame(out, op::TOPIC_DELETE, F_REPLY, corr, |w| w.bool(done));
            }
            op::TOPIC_ALTER => {
                self.check_ddl()?;
                let mut r = Reader::new(body);
                let name = r.string().map_err(proto)?;
                let c = TopicConfig::decode(&mut r).map_err(proto)?;
                let new = self.broker.alter_topic(&name, |old| {
                    old.retention_ms = c.retention_ms;
                    old.retention_bytes = c.retention_bytes;
                    old.max_batch_bytes = c.max_batch_bytes;
                    old.segment_bytes = c.segment_bytes;
                })?;
                wire::frame(out, op::TOPIC_ALTER, F_REPLY, corr, |w| new.encode(w));
            }
            op::TOPIC_LIST => {
                let topics = self.broker.topics();
                wire::frame(out, op::TOPIC_LIST, F_REPLY, corr, |w| {
                    w.u32(topics.len() as u32);
                    for t in &topics {
                        w.u32(t.id);
                        w.str(&t.name);
                        t.config().encode(w);
                        w.bool(t.queue.is_some());
                        w.u64(t.depth());
                        w.u64(t.bytes());
                    }
                });
            }
            op::SUBSCRIBE => {
                let req = Subscribe::decode(&mut Reader::new(body)).map_err(proto)?;
                let topic = self.broker.require_topic_id(req.topic)?;
                let sub = self.broker.subscription(&topic, &req.name, req.config.clone())?;
                let handle = self.next_handle;
                self.next_handle += 1;
                let (member, assigned) = sub.join(handle, req.credits, self.push_tx.clone(), &self.main)?;
                self.subs.insert(handle, (sub, member));
                wire::frame(out, op::SUBSCRIBE, F_REPLY, corr, |w| {
                    w.u32(handle);
                    w.u32(assigned.len() as u32);
                    for p in &assigned {
                        w.u32(*p);
                    }
                });
            }
            op::CREDIT => {
                let mut r = Reader::new(body);
                let (sub, member) = self.sub(r.u32().map_err(proto)?)?;
                sub.credit(member, r.u32().map_err(proto)?);
            }
            op::ACK => {
                let mut r = Reader::new(body);
                let (sub, _) = self.sub(r.u32().map_err(proto)?)?;
                let ranges = wire::decode_ranges(&mut r, 1 << 16).map_err(proto)?;
                sub.ack(&ranges)?;
            }
            op::NACK => {
                let mut r = Reader::new(body);
                let (sub, _) = self.sub(r.u32().map_err(proto)?)?;
                let delay = r.u32().map_err(proto)?;
                let ranges = wire::decode_ranges(&mut r, 1 << 16).map_err(proto)?;
                sub.nack(&ranges, delay)?;
            }
            op::RENEW => {
                let mut r = Reader::new(body);
                let (sub, _) = self.sub(r.u32().map_err(proto)?)?;
                let extend = r.u32().map_err(proto)?;
                let ranges = wire::decode_ranges(&mut r, 1 << 16).map_err(proto)?;
                let n = sub.renew(&ranges, extend);
                wire::frame(out, op::RENEW, F_REPLY, corr, |w| w.u64(n));
            }
            op::COMMIT => {
                let mut r = Reader::new(body);
                let (sub, _) = self.sub(r.u32().map_err(proto)?)?;
                let p = r.u32().map_err(proto)?;
                let off = r.u64().map_err(proto)?;
                sub.commit(p, off)?;
                wire::frame(out, op::COMMIT, F_REPLY, corr, |_| {});
            }
            op::UNSUBSCRIBE => {
                let handle = Reader::new(body).u32().map_err(proto)?;
                if let Some((sub, member)) = self.subs.remove(&handle) {
                    sub.leave(member);
                }
                wire::frame(out, op::UNSUBSCRIBE, F_REPLY, corr, |_| {});
            }
            op::SUB_DELETE => {
                self.check_ddl()?;
                let mut r = Reader::new(body);
                let topic = self.broker.require_topic_id(r.u32().map_err(proto)?)?;
                let name = r.string().map_err(proto)?;
                let if_exists = r.bool().map_err(proto)?;
                let done = self.broker.delete_subscription(&topic, &name, if_exists)?;
                wire::frame(out, op::SUB_DELETE, F_REPLY, corr, |w| w.bool(done));
            }
            op::SUB_LIST => {
                let topic = self.broker.require_topic_id(Reader::new(body).u32().map_err(proto)?)?;
                let subs = topic.subscriptions();
                wire::frame(out, op::SUB_LIST, F_REPLY, corr, |w| {
                    w.u32(subs.len() as u32);
                    for s in &subs {
                        w.str(&s.name);
                        s.config.encode(w);
                        w.u32(s.member_count() as u32);
                        w.u64(s.backlog());
                        w.u64(s.inflight() as u64);
                    }
                });
            }
            op::EXCHANGE_DECLARE => {
                self.check_ddl()?;
                let mut r = Reader::new(body);
                let name = r.string().map_err(proto)?;
                let kind = ExchangeKind::from_u8(r.u8().map_err(proto)?).ok_or_else(|| BrokerError::invalid("unknown exchange kind"))?;
                let durable = r.bool().map_err(proto)?;
                let alternate = r.string().map_err(proto)?;
                let alt = (!alternate.is_empty()).then_some(alternate.as_str());
                self.broker.declare_exchange(&name, kind, alt, durable)?;
                wire::frame(out, op::EXCHANGE_DECLARE, F_REPLY, corr, |_| {});
            }
            op::EXCHANGE_DELETE => {
                self.check_ddl()?;
                let mut r = Reader::new(body);
                let name = r.string().map_err(proto)?;
                let if_exists = r.bool().map_err(proto)?;
                let done = self.broker.delete_exchange(&name, if_exists)?;
                wire::frame(out, op::EXCHANGE_DELETE, F_REPLY, corr, |w| w.bool(done));
            }
            op::BIND | op::UNBIND => {
                self.check_ddl()?;
                let b = Binding::decode(&mut Reader::new(body)).map_err(proto)?;
                let done = if h.op == op::BIND { self.broker.bind(b)? } else { self.broker.unbind(&b)? };
                wire::frame(out, h.op, F_REPLY, corr, |w| w.bool(done));
            }
            op::PUBLISH => self.publish(corr, body, out)?,
            op::QUEUE_DECLARE => {
                self.check_ddl()?;
                let mut r = Reader::new(body);
                let name = r.string().map_err(proto)?;
                let q = QueueConfig::decode(&mut r).map_err(proto)?;
                let if_not_exists = r.bool().map_err(proto)?;
                let t = self.broker.declare_queue(&name, q, if_not_exists)?;
                wire::frame(out, op::QUEUE_DECLARE, F_REPLY, corr, |w| w.u32(t.id));
            }
            op::QUEUE_DELETE => {
                self.check_ddl()?;
                let mut r = Reader::new(body);
                let name = r.string().map_err(proto)?;
                let if_exists = r.bool().map_err(proto)?;
                if self.broker.topic(&name).is_some_and(|t| t.queue.is_none()) {
                    return Err(BrokerError::invalid(format!("\"{name}\" is a topic, not a queue")));
                }
                let done = self.broker.delete_topic(&name, if_exists)?;
                wire::frame(out, op::QUEUE_DELETE, F_REPLY, corr, |w| w.bool(done));
            }
            other => {
                return Err(BrokerError::new(Code::Unsupported, format!("unknown op {other}")));
            }
        }
        Ok(())
    }

    fn sub(&self, handle: u32) -> Result<(Arc<Subscription>, u64), BrokerError> {
        self.subs
            .get(&handle)
            .map(|(s, m)| (s.clone(), *m))
            .ok_or_else(|| BrokerError::new(Code::NoSubscription, format!("no subscription has handle {handle} here")))
    }

    fn produce(&mut self, corr: u32, body: &mut [u8], out: &mut Vec<u8>) -> Result<(), BrokerError> {
        if body.len() < Produce::LEN {
            return Err(proto(wire::DecodeError("body too short")));
        }
        let req = Produce::decode(&mut Reader::new(&body[..Produce::LEN])).map_err(proto)?;
        let topic = self.broker.require_topic_id(req.topic)?;
        let batch = &mut body[Produce::LEN..];
        let (part, a) = self.broker.produce(&topic, req.partition, batch, true)?;
        let reply = |out: &mut Vec<u8>| {
            wire::frame(out, op::PRODUCE, F_REPLY, corr, |w| {
                Produced { partition: part.index, base_offset: a.base_offset, duplicate: a.duplicate }.encode(w)
            })
        };
        match req.ack {
            Ack::None => {}
            Ack::Written => reply(out),
            Ack::Durable => {
                if part.durability != Durability::Sync || part.durable_offset() >= a.next_offset {
                    reply(out);
                } else {
                    let mut frame = Vec::with_capacity(32);
                    reply(&mut frame);
                    self.parked.push(Parked::Durable { corr, targets: vec![(part.clone(), a.next_offset)], frame });
                }
            }
        }
        Ok(())
    }

    fn publish(&mut self, corr: u32, body: &mut [u8], out: &mut Vec<u8>) -> Result<(), BrokerError> {
        let (exchange, key, ack, mandatory, head) = {
            let mut r = Reader::new(body);
            let exchange = r.string().map_err(proto)?;
            let key = r.string().map_err(proto)?;
            let ack = Ack::from_u8(r.u8().map_err(proto)?).ok_or_else(|| BrokerError::invalid("unknown ack"))?;
            let mandatory = r.bool().map_err(proto)?;
            let head = body.len() - r.rest().len();
            (exchange, key, ack, mandatory, head)
        };
        let batch = &mut body[head..];
        let routed = self.broker.publish(&exchange, &key, batch, true)?;
        if routed.is_empty() && mandatory {
            return Err(BrokerError::new(Code::Unroutable, format!("no route from \"{exchange}\" for \"{key}\"")));
        }
        let count = routed.len() as u32;
        let mut frame = Vec::with_capacity(24);
        wire::frame(&mut frame, op::PUBLISH, F_REPLY, corr, |w| w.u32(count));
        match ack {
            Ack::None => {}
            Ack::Written => out.extend_from_slice(&frame),
            Ack::Durable => {
                let targets: Vec<(Arc<Partition>, u64)> = routed
                    .into_iter()
                    .filter(|(p, a)| p.durability == Durability::Sync && p.durable_offset() < a.next_offset)
                    .map(|(p, a)| (p, a.next_offset))
                    .collect();
                if targets.is_empty() {
                    out.extend_from_slice(&frame);
                } else {
                    self.parked.push(Parked::Durable { corr, targets, frame });
                }
            }
        }
        Ok(())
    }
}

/// A fetch's reply, read straight into the frame.
fn fetch_reply(out: &mut Vec<u8>, corr: u32, part: &Partition, req: &Fetch) {
    let start = wire::begin_frame(out, op::FETCH, F_REPLY, corr);
    let fixed = out.len();
    out.extend_from_slice(&[0u8; Fetched::LEN]);
    let max = (req.max_bytes as usize).clamp(batch::HEADER, MAX_FETCH);
    match part.read_into(req.offset, max, out) {
        Ok((next, log_start)) => {
            let mut head = Vec::with_capacity(Fetched::LEN);
            Fetched { partition: part.index, next_offset: next, log_start }.encode(&mut Writer(&mut head));
            out[fixed..fixed + Fetched::LEN].copy_from_slice(&head);
            wire::end_frame(out, start);
        }
        Err(e) => {
            out.truncate(start);
            wire::error_frame(out, op::FETCH, corr, e.code, &e.message);
        }
    }
}

fn proto(e: wire::DecodeError) -> BrokerError {
    BrokerError::new(Code::Protocol, e.to_string())
}

#[allow(dead_code)]
fn error_body(e: &BrokerError) -> ErrorBody {
    ErrorBody { code: e.code, message: e.message.clone() }
}
