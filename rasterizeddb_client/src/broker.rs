//! The message broker's native client (`rdbmq/1`, docs/broker.md §6).
//!
//! ```no_run
//! # async fn demo() -> Result<(), rasterizeddb_client::broker::BrokerError> {
//! use rasterizeddb_client::broker::*;
//! let broker = Broker::connect("127.0.0.1:7171", Options::password("app", "secret")).await?;
//! let orders = broker.create_topic("orders", &TopicConfig { partitions: 8, ..Default::default() }, true).await?;
//!
//! // A batching producer: records gather per partition into the stored
//! // layout, and one frame carries each batch.
//! let producer = broker.producer(orders.clone(), ProducerOptions::default());
//! let offset = producer.send(Some(b"customer-7"), b"{\"total\": 42}", &[]).await?;
//!
//! // A work queue: competing consumers, an ack per message.
//! let mut sub = broker.subscribe(&orders, "billing", SubConfig::default(), 1024).await?;
//! while let Some(d) = sub.next().await {
//!     let d = d?;
//!     for r in d.records() {
//!         println!("{} {:?}", r.offset, r.value);
//!     }
//!     sub.ack_delivery(&d)?;
//! }
//! # Ok(()) }
//! ```
//!
//! One connection carries any number of requests at once: each request has
//! an id, and replies come back in whatever order the server finishes them.
//! A background task owns the socket. It writes everything queued since it
//! last looked with one write, and routes each reply to its caller and each
//! pushed delivery to its subscription.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

pub use rasterizeddb_mq::batch::{BatchBuilder, BatchView, Batches, RecordView};
pub use rasterizeddb_mq::wire::{
    ANY_PARTITION, Ack, BY_KEY, Binding, Code, Durability, EARLIEST, ExchangeKind, LATEST, QueueConfig, Range, Start,
    SubConfig, SubMode, TopicConfig, XMatch,
};
use rasterizeddb_mq::wire::{self, ErrorBody, F_ERROR, F_PUSH, F_REPLY, Fetched as FetchedHead, Produced, Reader, Writer, op};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};

/// A failure: the server's code and message, or `code: None` for one on this
/// side (the connection, a malformed reply).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerError {
    pub code: Option<Code>,
    pub message: String,
}

impl BrokerError {
    fn local(message: impl Into<String>) -> Self {
        BrokerError { code: None, message: message.into() }
    }
    fn closed() -> Self {
        Self::local("the broker connection is closed")
    }
}

impl std::fmt::Display for BrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.code {
            Some(c) => write!(f, "{} ({c:?})", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for BrokerError {}

impl From<std::io::Error> for BrokerError {
    fn from(e: std::io::Error) -> Self {
        Self::local(e.to_string())
    }
}

impl From<wire::DecodeError> for BrokerError {
    fn from(e: wire::DecodeError) -> Self {
        Self::local(e.to_string())
    }
}

impl From<rasterizeddb_mq::BatchError> for BrokerError {
    fn from(e: rasterizeddb_mq::BatchError) -> Self {
        Self::local(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, BrokerError>;

/// How to connect.
#[derive(Clone, Debug)]
pub struct Options {
    pub user: Option<String>,
    pub password: Option<String>,
    pub client_name: String,
    /// How long a request may wait for its reply.
    pub timeout: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Options { user: None, password: None, client_name: "rasterizeddb_client".into(), timeout: Duration::from_secs(30) }
    }
}

impl Options {
    /// Log in as `user` with `password` (the database's roles).
    pub fn password(user: &str, password: &str) -> Self {
        Options { user: Some(user.into()), password: Some(password.into()), ..Default::default() }
    }
    /// No login: a server started with `--broker-auth trust`.
    pub fn trust() -> Self {
        Self::default()
    }
}

/// A topic, resolved to the id its frames name it by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicRef {
    pub name: String,
    pub id: u32,
    pub partitions: u32,
    pub durability: Durability,
    pub queue: bool,
}

/// A row of [`Broker::list_topics`].
#[derive(Clone, Debug)]
pub struct TopicInfo {
    pub id: u32,
    pub name: String,
    pub config: TopicConfig,
    pub queue: bool,
    pub messages: u64,
    pub bytes: u64,
}

/// What [`Broker::fetch`] returned: whole stored batches.
#[derive(Clone, Debug)]
pub struct Fetched {
    pub partition: u32,
    pub next_offset: u64,
    pub log_start: u64,
    /// Where the fetch asked to start; records before it are skipped by
    /// [`Fetched::records`].
    pub from: u64,
    pub data: Vec<u8>,
}

impl Fetched {
    pub fn batches(&self) -> impl Iterator<Item = BatchView<'_>> {
        Batches::trusted(&self.data).flatten()
    }
    /// Every record at or after the offset asked for.
    pub fn records(&self) -> impl Iterator<Item = RecordView<'_>> {
        let from = self.from;
        self.batches().flat_map(|b| b.records()).filter(move |r| r.offset >= from)
    }
    /// The offset to fetch next.
    pub fn next_fetch(&self) -> u64 {
        self.batches().last().map_or(self.from, |b| b.next_offset())
    }
}

type Reply = std::result::Result<Vec<u8>, BrokerError>;

struct Inner {
    tx: mpsc::UnboundedSender<Vec<u8>>,
    pending: parking_mutex::Mutex<HashMap<u32, oneshot::Sender<Reply>>>,
    subs: parking_mutex::Mutex<HashMap<u32, mpsc::UnboundedSender<Push>>>,
    next: AtomicU32,
    closed: AtomicBool,
    topics: AsyncMutex<HashMap<String, TopicRef>>,
    timeout: Duration,
}

/// A small std mutex wrapper: the client has no parking_lot.
mod parking_mutex {
    pub struct Mutex<T>(std::sync::Mutex<T>);
    impl<T> Mutex<T> {
        pub fn new(v: T) -> Self {
            Mutex(std::sync::Mutex::new(v))
        }
        pub fn lock(&self) -> std::sync::MutexGuard<'_, T> {
            self.0.lock().unwrap_or_else(|p| p.into_inner())
        }
    }
}

/// A connection to the broker. Cloning shares it.
#[derive(Clone)]
pub struct Broker {
    inner: Arc<Inner>,
}

trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

impl Broker {
    /// Connects to `addr` (`host:port`, or `unix:/path/to/socket`), says
    /// hello, and logs in when `options` has a user.
    pub async fn connect(addr: &str, options: Options) -> Result<Broker> {
        let stream: Box<dyn Stream> = if let Some(path) = addr.strip_prefix("unix:") {
            #[cfg(unix)]
            {
                Box::new(tokio::net::UnixStream::connect(path).await?)
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                return Err(BrokerError::local("unix sockets are not supported on this platform"));
            }
        } else {
            let s = tokio::net::TcpStream::connect(addr).await?;
            let _ = s.set_nodelay(true);
            Box::new(s)
        };
        let (rd, wr) = tokio::io::split(stream);
        let (tx, rx) = mpsc::unbounded_channel();
        let inner = Arc::new(Inner {
            tx,
            pending: parking_mutex::Mutex::new(HashMap::new()),
            subs: parking_mutex::Mutex::new(HashMap::new()),
            next: AtomicU32::new(1),
            closed: AtomicBool::new(false),
            topics: AsyncMutex::new(HashMap::new()),
            timeout: options.timeout,
        });
        tokio::spawn(write_loop(wr, rx, Arc::downgrade(&inner)));
        tokio::spawn(read_loop(rd, Arc::downgrade(&inner)));
        let broker = Broker { inner };
        let hello = broker
            .call(op::HELLO, |w| {
                w.u16(wire::VERSION);
                w.str(&options.client_name)
            })
            .await?;
        let mut r = Reader::new(&hello);
        let _version = r.u16()?;
        let _server = r.string()?;
        let _max_frame = r.u32()?;
        let _max_batch = r.u32()?;
        let auth_required = r.bool()?;
        match (&options.user, &options.password) {
            (Some(user), password) => {
                broker
                    .call(op::AUTH, |w| {
                        w.str(user);
                        w.str(password.as_deref().unwrap_or(""))
                    })
                    .await?;
            }
            (None, _) if auth_required => {
                return Err(BrokerError { code: Some(Code::AuthRequired), message: "the server wants a login".into() });
            }
            _ => {}
        }
        Ok(broker)
    }

    fn send_frame(&self, frame: Vec<u8>) -> Result<()> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(BrokerError::closed());
        }
        self.inner.tx.send(frame).map_err(|_| BrokerError::closed())
    }

    fn corr(&self) -> u32 {
        loop {
            let c = self.inner.next.fetch_add(1, Ordering::Relaxed);
            if c != 0 {
                return c;
            }
        }
    }

    /// Sends a request built by `body` and waits for its reply's body.
    async fn call(&self, op: u8, body: impl FnOnce(&mut Writer<'_>)) -> Result<Vec<u8>> {
        let mut frame = Vec::with_capacity(64);
        let corr = self.corr();
        wire::frame(&mut frame, op, 0, corr, body);
        self.call_frame(corr, frame).await
    }

    async fn call_frame(&self, corr: u32, frame: Vec<u8>) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        self.inner.pending.lock().insert(corr, tx);
        if let Err(e) = self.send_frame(frame) {
            self.inner.pending.lock().remove(&corr);
            return Err(e);
        }
        match tokio::time::timeout(self.inner.timeout, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err(BrokerError::closed()),
            Err(_) => {
                self.inner.pending.lock().remove(&corr);
                Err(BrokerError::local(format!("no reply within {:?}", self.inner.timeout)))
            }
        }
    }

    pub async fn ping(&self) -> Result<()> {
        self.call(op::PING, |_| {}).await.map(|_| ())
    }

    // -----------------------------------------------------------------------
    // Topics
    // -----------------------------------------------------------------------

    /// Creates a topic (or, with `if_not_exists`, returns the one there when
    /// its settings match).
    pub async fn create_topic(&self, name: &str, config: &TopicConfig, if_not_exists: bool) -> Result<TopicRef> {
        self.call(op::TOPIC_CREATE, |w| {
            w.str(name);
            config.encode(w);
            w.bool(if_not_exists)
        })
        .await?;
        self.forget(name).await;
        self.topic(name).await
    }

    pub async fn delete_topic(&self, name: &str, if_exists: bool) -> Result<bool> {
        let r = self
            .call(op::TOPIC_DELETE, |w| {
                w.str(name);
                w.bool(if_exists)
            })
            .await?;
        self.forget(name).await;
        Ok(Reader::new(&r).bool()?)
    }

    /// Changes a topic's retention, segment size and batch limit.
    pub async fn alter_topic(&self, name: &str, config: &TopicConfig) -> Result<TopicConfig> {
        let r = self
            .call(op::TOPIC_ALTER, |w| {
                w.str(name);
                config.encode(w)
            })
            .await?;
        Ok(TopicConfig::decode(&mut Reader::new(&r))?)
    }

    /// The topic named `name`, resolved once and remembered.
    pub async fn topic(&self, name: &str) -> Result<TopicRef> {
        if let Some(t) = self.inner.topics.lock().await.get(name) {
            return Ok(t.clone());
        }
        let r = self.call(op::RESOLVE, |w| w.str(name)).await?;
        let mut r = Reader::new(&r);
        let t = TopicRef {
            name: name.to_string(),
            id: r.u32()?,
            partitions: r.u32()?,
            durability: Durability::from_u8(r.u8()?).unwrap_or_default(),
            queue: r.bool()?,
        };
        self.inner.topics.lock().await.insert(name.to_string(), t.clone());
        Ok(t)
    }

    async fn forget(&self, name: &str) {
        self.inner.topics.lock().await.remove(name);
    }

    pub async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        let r = self.call(op::TOPIC_LIST, |_| {}).await?;
        let mut r = Reader::new(&r);
        let n = r.u32()? as usize;
        let mut out = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            out.push(TopicInfo {
                id: r.u32()?,
                name: r.string()?,
                config: TopicConfig::decode(&mut r)?,
                queue: r.bool()?,
                messages: r.u64()?,
                bytes: r.u64()?,
            });
        }
        Ok(out)
    }

    // -----------------------------------------------------------------------
    // Producing and fetching
    // -----------------------------------------------------------------------

    /// Appends one batch (built by [`BatchBuilder`]) to a partition, or by
    /// key ([`BY_KEY`]), or wherever ([`ANY_PARTITION`]). With
    /// [`Ack::None`] nothing comes back and the result is a placeholder.
    pub async fn produce(&self, topic: &TopicRef, partition: u32, batch: &[u8], ack: Ack) -> Result<Produced> {
        let corr = self.corr();
        let mut frame = Vec::with_capacity(wire::FRAME_HEADER + wire::Produce::LEN + batch.len());
        wire::frame(&mut frame, op::PRODUCE, 0, corr, |w| {
            wire::Produce { topic: topic.id, partition, ack }.encode(w);
            w.raw(batch)
        });
        if ack == Ack::None {
            self.send_frame(frame)?;
            return Ok(Produced { partition, base_offset: u64::MAX, duplicate: false });
        }
        let r = self.call_frame(corr, frame).await?;
        Ok(Produced::decode(&mut Reader::new(&r))?)
    }

    /// Queues a produce on the socket now, in call order, and returns the
    /// reply to wait for. What keeps a [`Producer`]'s batches in order.
    fn enqueue_produce(&self, topic: &TopicRef, partition: u32, batch: &[u8], ack: Ack) -> Result<oneshot::Receiver<Reply>> {
        let corr = self.corr();
        let mut frame = Vec::with_capacity(wire::FRAME_HEADER + wire::Produce::LEN + batch.len());
        wire::frame(&mut frame, op::PRODUCE, 0, corr, |w| {
            wire::Produce { topic: topic.id, partition, ack }.encode(w);
            w.raw(batch)
        });
        let (tx, rx) = oneshot::channel();
        self.inner.pending.lock().insert(corr, tx);
        if let Err(e) = self.send_frame(frame) {
            self.inner.pending.lock().remove(&corr);
            return Err(e);
        }
        Ok(rx)
    }

    /// A producer that batches records per partition.
    pub fn producer(&self, topic: TopicRef, options: ProducerOptions) -> Producer {
        Producer::new(self.clone(), topic, options)
    }

    /// Up to `max_bytes` of whole batches from `offset`, waiting up to
    /// `max_wait` for something to arrive when nothing is there yet.
    pub async fn fetch(&self, topic: &TopicRef, partition: u32, offset: u64, max_bytes: u32, max_wait: Duration) -> Result<Fetched> {
        let r = self
            .call(op::FETCH, |w| {
                wire::Fetch {
                    topic: topic.id,
                    partition,
                    offset,
                    max_bytes,
                    max_wait_ms: max_wait.as_millis().min(u32::MAX as u128) as u32,
                    min_bytes: 1,
                }
                .encode(w)
            })
            .await?;
        let mut rd = Reader::new(&r);
        let head = FetchedHead::decode(&mut rd)?;
        Ok(Fetched {
            partition: head.partition,
            next_offset: head.next_offset,
            log_start: head.log_start,
            from: offset,
            data: rd.rest().to_vec(),
        })
    }

    /// `(offset, log_start, next_offset)`: the offset for `at` ([`LATEST`],
    /// [`EARLIEST`], or a time in µs since the epoch).
    pub async fn list_offsets(&self, topic: &TopicRef, partition: u32, at: i64) -> Result<(u64, u64, u64)> {
        let r = self
            .call(op::LIST_OFFSETS, |w| {
                w.u32(topic.id);
                w.u32(partition);
                w.i64(at)
            })
            .await?;
        let mut r = Reader::new(&r);
        Ok((r.u64()?, r.u64()?, r.u64()?))
    }

    // -----------------------------------------------------------------------
    // Subscriptions
    // -----------------------------------------------------------------------

    /// Joins (creating when needed) subscription `name` on `topic`. The
    /// server pushes up to `credits` records ahead; [`Subscription::next`]
    /// grants more as they are consumed.
    pub async fn subscribe(&self, topic: &TopicRef, name: &str, config: SubConfig, credits: u32) -> Result<Subscription> {
        let corr = self.corr();
        let mut frame = Vec::new();
        wire::frame(&mut frame, op::SUBSCRIBE, 0, corr, |w| {
            wire::Subscribe { topic: topic.id, name: name.to_string(), config, credits }.encode(w)
        });
        let r = self.call_frame(corr, frame).await?;
        let mut r = Reader::new(&r);
        let handle = r.u32()?;
        let n = r.u32()? as usize;
        let mut assigned = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            assigned.push(r.u32()?);
        }
        let (tx, rx) = mpsc::unbounded_channel();
        self.inner.subs.lock().insert(handle, tx);
        Ok(Subscription { broker: self.clone(), handle, rx, assigned, window: credits.max(1), consumed: 0 })
    }

    /// Consumes a queue (its shared subscription).
    pub async fn consume(&self, queue: &str, credits: u32) -> Result<Subscription> {
        let t = self.topic(queue).await?;
        let config = SubConfig { mode: SubMode::Shared, ..Default::default() };
        self.subscribe(&t, queue, config, credits).await
    }

    pub async fn delete_subscription(&self, topic: &TopicRef, name: &str, if_exists: bool) -> Result<bool> {
        let r = self
            .call(op::SUB_DELETE, |w| {
                w.u32(topic.id);
                w.str(name);
                w.bool(if_exists)
            })
            .await?;
        Ok(Reader::new(&r).bool()?)
    }

    // -----------------------------------------------------------------------
    // Queues, exchanges, bindings
    // -----------------------------------------------------------------------

    pub async fn declare_queue(&self, name: &str, config: &QueueConfig, if_not_exists: bool) -> Result<TopicRef> {
        self.call(op::QUEUE_DECLARE, |w| {
            w.str(name);
            config.encode(w);
            w.bool(if_not_exists)
        })
        .await?;
        self.forget(name).await;
        self.topic(name).await
    }

    pub async fn delete_queue(&self, name: &str, if_exists: bool) -> Result<bool> {
        let r = self
            .call(op::QUEUE_DELETE, |w| {
                w.str(name);
                w.bool(if_exists)
            })
            .await?;
        self.forget(name).await;
        Ok(Reader::new(&r).bool()?)
    }

    pub async fn declare_exchange(&self, name: &str, kind: ExchangeKind, durable: bool, alternate: Option<&str>) -> Result<()> {
        self.call(op::EXCHANGE_DECLARE, |w| {
            w.str(name);
            w.u8(kind as u8);
            w.bool(durable);
            w.str(alternate.unwrap_or(""))
        })
        .await
        .map(|_| ())
    }

    pub async fn delete_exchange(&self, name: &str, if_exists: bool) -> Result<bool> {
        let r = self
            .call(op::EXCHANGE_DELETE, |w| {
                w.str(name);
                w.bool(if_exists)
            })
            .await?;
        Ok(Reader::new(&r).bool()?)
    }

    pub async fn bind(&self, binding: &Binding) -> Result<bool> {
        let r = self.call(op::BIND, |w| binding.encode(w)).await?;
        Ok(Reader::new(&r).bool()?)
    }

    pub async fn unbind(&self, binding: &Binding) -> Result<bool> {
        let r = self.call(op::UNBIND, |w| binding.encode(w)).await?;
        Ok(Reader::new(&r).bool()?)
    }

    /// Publishes one batch through `exchange` (`""`: to the queue or topic
    /// named `key`). Returns how many topics it reached; `mandatory` makes
    /// reaching none an error.
    pub async fn publish(&self, exchange: &str, key: &str, batch: &[u8], ack: Ack, mandatory: bool) -> Result<u32> {
        let corr = self.corr();
        let mut frame = Vec::with_capacity(64 + batch.len());
        wire::frame(&mut frame, op::PUBLISH, 0, corr, |w| {
            w.str(exchange);
            w.str(key);
            w.u8(ack as u8);
            w.bool(mandatory);
            w.raw(batch)
        });
        if ack == Ack::None {
            self.send_frame(frame)?;
            return Ok(0);
        }
        let r = self.call_frame(corr, frame).await?;
        Ok(Reader::new(&r).u32()?)
    }
}

// ---------------------------------------------------------------------------
// The socket's two tasks
// ---------------------------------------------------------------------------

async fn write_loop<W: AsyncWrite + Unpin>(mut wr: W, mut rx: mpsc::UnboundedReceiver<Vec<u8>>, inner: std::sync::Weak<Inner>) {
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    while let Some(first) = rx.recv().await {
        buf.extend_from_slice(&first);
        while buf.len() < (1 << 20) {
            match rx.try_recv() {
                Ok(f) => buf.extend_from_slice(&f),
                Err(_) => break,
            }
        }
        if wr.write_all(&buf).await.is_err() {
            break;
        }
        buf.clear();
    }
    if let Some(i) = inner.upgrade() {
        fail_all(&i);
    }
}

async fn read_loop<R: AsyncRead + Unpin>(mut rd: R, inner: std::sync::Weak<Inner>) {
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    'outer: loop {
        let mut at = 0;
        loop {
            let h = match wire::parse_header(&buf[at..]) {
                Ok(Some(h)) => h,
                Ok(None) => break,
                Err(_) => break 'outer,
            };
            let body = &buf[at + wire::FRAME_HEADER..at + wire::FRAME_HEADER + h.body_len];
            let Some(i) = inner.upgrade() else { return };
            route(&i, h, body);
            at += wire::FRAME_HEADER + h.body_len;
        }
        buf.drain(..at);
        if let Some(need) = wire::frame_need(&buf) {
            if need > buf.capacity() {
                buf.reserve(need - buf.len());
            }
        }
        if buf.capacity() - buf.len() < 16 * 1024 {
            buf.reserve(64 * 1024);
        }
        match rd.read_buf(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
    }
    if let Some(i) = inner.upgrade() {
        fail_all(&i);
    }
}

fn route(inner: &Inner, h: wire::Header, body: &[u8]) {
    if h.flags & F_PUSH != 0 {
        let push = match h.op {
            op::DELIVER => match Delivery::parse(body) {
                Ok(d) => Push::Deliver(d),
                Err(e) => Push::Closed(e),
            },
            op::ASSIGN => {
                let mut r = Reader::new(body);
                let mut parse = || -> std::result::Result<Push, wire::DecodeError> {
                    let generation = r.u64()?;
                    let n = r.u32()? as usize;
                    let mut partitions = Vec::with_capacity(n.min(4096));
                    for _ in 0..n {
                        partitions.push(r.u32()?);
                    }
                    Ok(Push::Assign { generation, partitions })
                };
                parse().unwrap_or_else(|e| Push::Closed(e.into()))
            }
            op::CLOSED => {
                let e = ErrorBody::decode(&mut Reader::new(body))
                    .map(|e| BrokerError { code: Some(e.code), message: e.message })
                    .unwrap_or_else(|e| e.into());
                Push::Closed(e)
            }
            _ => return,
        };
        let closed = matches!(push, Push::Closed(_));
        let mut subs = inner.subs.lock();
        if let Some(tx) = subs.get(&h.corr) {
            let _ = tx.send(push);
        }
        if closed {
            subs.remove(&h.corr);
        }
        return;
    }
    if h.flags & F_REPLY == 0 {
        return;
    }
    let reply = if h.flags & F_ERROR != 0 {
        Err(ErrorBody::decode(&mut Reader::new(body))
            .map(|e| BrokerError { code: Some(e.code), message: e.message })
            .unwrap_or_else(|e| e.into()))
    } else {
        Ok(body.to_vec())
    };
    if let Some(tx) = inner.pending.lock().remove(&h.corr) {
        let _ = tx.send(reply);
    } else if let Err(e) = reply {
        // A fire-and-forget frame (an ack, a credit) that failed: tell its
        // subscription, if the server named one.
        if let Some(tx) = inner.subs.lock().get(&h.corr) {
            let _ = tx.send(Push::Closed(e));
        }
    }
}

fn fail_all(inner: &Inner) {
    inner.closed.store(true, Ordering::Release);
    for (_, tx) in inner.pending.lock().drain() {
        let _ = tx.send(Err(BrokerError::closed()));
    }
    for (_, tx) in inner.subs.lock().drain() {
        let _ = tx.send(Push::Closed(BrokerError::closed()));
    }
}

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

/// What the server pushes to a subscription.
#[derive(Debug)]
pub enum Push {
    Deliver(Delivery),
    /// An ordered subscription's partitions changed.
    Assign { generation: u64, partitions: Vec<u32> },
    /// It ended (deleted, the server stopping, the connection lost).
    Closed(BrokerError),
}

/// A batch of records pushed to a subscription. `records()` yields only the
/// ones that are this consumer's.
#[derive(Debug)]
pub struct Delivery {
    pub partition: u32,
    pub ranges: Vec<Range>,
    data: Vec<u8>,
}

impl Delivery {
    fn parse(body: &[u8]) -> Result<Delivery> {
        let mut r = Reader::new(body);
        let partition = r.u32()?;
        let ranges = wire::decode_ranges(&mut r, 1 << 20)?;
        Ok(Delivery { partition, ranges, data: r.rest().to_vec() })
    }

    /// The records delivered, in order.
    pub fn records(&self) -> impl Iterator<Item = RecordView<'_>> {
        let ranges = &self.ranges;
        Batches::trusted(&self.data)
            .flatten()
            .flat_map(|b| b.records())
            .filter(move |r| ranges.iter().any(|g| r.offset >= g.offset && r.offset < g.offset + g.count as u64))
    }

    /// Records delivered.
    pub fn count(&self) -> u64 {
        self.ranges.iter().map(|r| r.count as u64).sum()
    }

    /// The stored batches as they came, for a consumer that reads them itself.
    pub fn raw(&self) -> &[u8] {
        &self.data
    }
}

pub struct Subscription {
    broker: Broker,
    handle: u32,
    rx: mpsc::UnboundedReceiver<Push>,
    /// The partitions an ordered subscription gave this member (it changes
    /// with [`Push::Assign`]).
    pub assigned: Vec<u32>,
    window: u32,
    consumed: u32,
}

impl Subscription {
    /// The next delivery. Credit is granted back as deliveries are taken,
    /// so the server keeps about one window of records in flight.
    pub async fn next(&mut self) -> Option<Result<Delivery>> {
        loop {
            match self.rx.recv().await? {
                Push::Deliver(d) => {
                    self.consumed = self.consumed.saturating_add(d.count().min(u32::MAX as u64) as u32);
                    if self.consumed >= self.window / 2 {
                        let n = std::mem::take(&mut self.consumed);
                        let _ = self.credit(n);
                    }
                    return Some(Ok(d));
                }
                Push::Assign { partitions, .. } => self.assigned = partitions,
                Push::Closed(e) => return Some(Err(e)),
            }
        }
    }

    /// The next push of any kind, without granting credit.
    pub async fn next_push(&mut self) -> Option<Push> {
        self.rx.recv().await
    }

    pub fn handle(&self) -> u32 {
        self.handle
    }

    /// Lets the server push `n` more records.
    pub fn credit(&self, n: u32) -> Result<()> {
        let mut f = Vec::with_capacity(24);
        wire::frame(&mut f, op::CREDIT, 0, self.handle, |w| {
            w.u32(self.handle);
            w.u32(n)
        });
        self.broker.send_frame(f)
    }

    /// Acks records: a shared subscription forgets them, an ordered one
    /// commits past them. No reply; a failure arrives as [`Push::Closed`].
    pub fn ack(&self, ranges: &[Range]) -> Result<()> {
        let mut f = Vec::with_capacity(24 + ranges.len() * 16);
        wire::frame(&mut f, op::ACK, 0, self.handle, |w| {
            w.u32(self.handle);
            wire::encode_ranges(w, ranges)
        });
        self.broker.send_frame(f)
    }

    pub fn ack_delivery(&self, d: &Delivery) -> Result<()> {
        self.ack(&d.ranges)
    }

    /// Hands records back to be delivered again after `delay` (shared).
    pub fn nack(&self, ranges: &[Range], delay: Duration) -> Result<()> {
        let mut f = Vec::with_capacity(28 + ranges.len() * 16);
        wire::frame(&mut f, op::NACK, 0, self.handle, |w| {
            w.u32(self.handle);
            w.u32(delay.as_millis().min(u32::MAX as u128) as u32);
            wire::encode_ranges(w, ranges)
        });
        self.broker.send_frame(f)
    }

    /// Sets an ordered partition's committed offset, and waits for the
    /// server to take it.
    pub async fn commit(&self, partition: u32, offset: u64) -> Result<()> {
        self.broker
            .call(op::COMMIT, |w| {
                w.u32(self.handle);
                w.u32(partition);
                w.u64(offset)
            })
            .await
            .map(|_| ())
    }

    /// Leaves the subscription; a shared one's unacked records go to the
    /// other members.
    pub async fn close(self) -> Result<()> {
        let r = self.broker.call(op::UNSUBSCRIBE, |w| w.u32(self.handle)).await.map(|_| ());
        self.broker.inner.subs.lock().remove(&self.handle);
        r
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if self.broker.inner.subs.lock().remove(&self.handle).is_some() {
            let mut f = Vec::with_capacity(16);
            wire::frame(&mut f, op::UNSUBSCRIBE, 0, 0, |w| w.u32(self.handle));
            let _ = self.broker.send_frame(f);
        }
    }
}

// ---------------------------------------------------------------------------
// The batching producer
// ---------------------------------------------------------------------------

/// How a [`Producer`] batches.
#[derive(Clone, Debug)]
pub struct ProducerOptions {
    /// A partition's batch is sent once it reaches this many bytes.
    pub batch_bytes: usize,
    /// ... or this long after its first record, whichever is first.
    pub linger: Duration,
    /// [`Ack::Durable`] (the default) or [`Ack::Written`]; `None` is taken
    /// as `Written`, since a send resolves to its offset.
    pub ack: Ack,
    /// Mark batches as an idempotent producer's, so a batch sent twice is
    /// stored once.
    pub idempotent: bool,
}

impl Default for ProducerOptions {
    fn default() -> Self {
        ProducerOptions { batch_bytes: 256 * 1024, linger: Duration::from_millis(1), ack: Ack::Durable, idempotent: false }
    }
}

struct PartBuf {
    builder: BatchBuilder,
    waiters: Vec<oneshot::Sender<Result<u64>>>,
    /// A linger timer is running for the batch being built.
    armed: bool,
    /// Bumped at every flush, so a stale timer flushes nothing.
    epoch: u64,
}

struct ProducerInner {
    broker: Broker,
    topic: TopicRef,
    options: ProducerOptions,
    parts: Vec<std::sync::Mutex<PartBuf>>,
    rr: AtomicU32,
}

/// Batches records per partition into the stored layout and sends each batch
/// as one frame. Cloning shares it.
#[derive(Clone)]
pub struct Producer {
    inner: Arc<ProducerInner>,
}

impl Producer {
    fn new(broker: Broker, topic: TopicRef, mut options: ProducerOptions) -> Producer {
        if options.ack == Ack::None {
            options.ack = Ack::Written;
        }
        let producer_id = if options.idempotent {
            let mut b = [0u8; 8];
            let _ = getrandom::fill(&mut b);
            u64::from_le_bytes(b) | 1
        } else {
            0
        };
        let parts = (0..topic.partitions.max(1))
            .map(|_| {
                let mut builder = BatchBuilder::with_capacity(options.batch_bytes + 4096);
                if producer_id != 0 {
                    builder.set_producer(producer_id, 0, 0);
                }
                std::sync::Mutex::new(PartBuf { builder, waiters: Vec::new(), armed: false, epoch: 0 })
            })
            .collect();
        Producer { inner: Arc::new(ProducerInner { broker, topic, options, parts, rr: AtomicU32::new(0) }) }
    }

    /// Queues a record; resolves to its offset once its batch is acked.
    /// Records with a key go to the key's partition, the rest round robin.
    pub fn send(
        &self,
        key: Option<&[u8]>,
        value: &[u8],
        headers: &[(&[u8], &[u8])],
    ) -> impl std::future::Future<Output = Result<u64>> + Send + use<> {
        let n = self.inner.topic.partitions.max(1);
        let p = match key {
            Some(k) => rasterizeddb_mq::hash::partition_for(k, n),
            None => self.inner.rr.fetch_add(1, Ordering::Relaxed) % n,
        };
        self.send_to(p, key, value, headers)
    }

    /// [`Producer::send`] to a partition of the caller's choice.
    pub fn send_to(
        &self,
        partition: u32,
        key: Option<&[u8]>,
        value: &[u8],
        headers: &[(&[u8], &[u8])],
    ) -> impl std::future::Future<Output = Result<u64>> + Send + use<> {
        let (tx, rx) = oneshot::channel();
        let p = (partition as usize).min(self.inner.parts.len() - 1);
        let mut arm = None;
        {
            let mut buf = self.inner.parts[p].lock().unwrap_or_else(|e| e.into_inner());
            let mut tx = Some(tx);
            if buf.builder.push(key, value, headers).is_err() {
                // Full (or a record time mismatch): send what is there, then
                // start the next batch with this record.
                let b = take(&mut buf);
                self.send_locked(p, b);
                if let Err(e) = buf.builder.push(key, value, headers) {
                    if let Some(tx) = tx.take() {
                        let _ = tx.send(Err(e.into()));
                    }
                }
            }
            if let Some(tx) = tx {
                buf.waiters.push(tx);
            }
            if buf.builder.len_bytes() >= self.inner.options.batch_bytes {
                let b = take(&mut buf);
                self.send_locked(p, b);
            } else if !buf.armed && !buf.builder.is_empty() {
                buf.armed = true;
                arm = Some(buf.epoch);
            }
        }
        if let Some(epoch) = arm {
            let me = self.clone();
            let linger = self.inner.options.linger;
            tokio::spawn(async move {
                tokio::time::sleep(linger).await;
                let mut buf = me.inner.parts[p].lock().unwrap_or_else(|e| e.into_inner());
                if buf.epoch != epoch || buf.builder.is_empty() {
                    return;
                }
                let b = take(&mut buf);
                me.send_locked(p, b);
            });
        }
        async move { rx.await.unwrap_or_else(|_| Err(BrokerError::closed())) }
    }

    /// Queues a finished batch on the socket. Called with the partition's
    /// lock held, so its batches go out in the order they were built; only
    /// the wait for the reply is spawned.
    fn send_locked(&self, p: usize, (bytes, waiters): (Vec<u8>, Vec<oneshot::Sender<Result<u64>>>)) {
        let rx = match self.inner.broker.enqueue_produce(&self.inner.topic, p as u32, &bytes, self.inner.options.ack) {
            Ok(rx) => rx,
            Err(e) => {
                for w in waiters {
                    let _ = w.send(Err(e.clone()));
                }
                return;
            }
        };
        tokio::spawn(async move {
            let r: Result<u64> = match rx.await {
                Ok(Ok(body)) => Produced::decode(&mut Reader::new(&body)).map(|d| d.base_offset).map_err(Into::into),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(BrokerError::closed()),
            };
            match r {
                Ok(base) => {
                    for (i, w) in waiters.into_iter().enumerate() {
                        let _ = w.send(Ok(base + i as u64));
                    }
                }
                Err(e) => {
                    for w in waiters {
                        let _ = w.send(Err(e.clone()));
                    }
                }
            }
        });
    }

    /// Sends every partly built batch now.
    pub fn flush(&self) {
        for p in 0..self.inner.parts.len() {
            let mut buf = self.inner.parts[p].lock().unwrap_or_else(|e| e.into_inner());
            if buf.builder.is_empty() {
                continue;
            }
            let b = take(&mut buf);
            self.send_locked(p, b);
        }
    }

    pub fn topic(&self) -> &TopicRef {
        &self.inner.topic
    }
}

fn take(buf: &mut PartBuf) -> (Vec<u8>, Vec<oneshot::Sender<Result<u64>>>) {
    buf.armed = false;
    buf.epoch += 1;
    (buf.builder.finish(), std::mem::take(&mut buf.waiters))
}

// ---------------------------------------------------------------------------
// ORM models as messages
// ---------------------------------------------------------------------------

impl From<crate::Error> for BrokerError {
    fn from(e: crate::Error) -> Self {
        BrokerError::local(e.to_string())
    }
}

/// The header that names the model a message carries.
pub const MODEL_HEADER: &[u8] = b"rdb-model";

/// A `#[derive(Model)]` row as a message: the same struct the SQL ORM and
/// [`KvRepo`](crate::kv::KvRepo) map.
///
/// The value is every column by name, `(name_len u16, name, len u32 or
/// u32::MAX for NULL, text form)` per column, so a consumer built from another
/// version of the struct still finds each column by name. The key is the
/// primary key's text forms joined by `:` (`:` and `\` escaped), which keeps
/// every change to one row in one partition, in order. A model without a
/// primary key is spread round robin.
pub fn encode_model<M: crate::orm::Model>(m: &M) -> Result<(Option<Vec<u8>>, Vec<u8>)> {
    let mut value = Vec::with_capacity(64 * M::COLUMNS.len());
    value.extend_from_slice(&(M::COLUMNS.len() as u16).to_le_bytes());
    let mut key: Option<Vec<u8>> = None;
    for (i, c) in M::COLUMNS.iter().enumerate() {
        value.extend_from_slice(&(c.name.len() as u16).to_le_bytes());
        value.extend_from_slice(c.name.as_bytes());
        let mut text = Vec::new();
        let present = m.write_text(i, &mut text)?;
        if present {
            value.extend_from_slice(&(text.len() as u32).to_le_bytes());
            value.extend_from_slice(&text);
        } else {
            value.extend_from_slice(&u32::MAX.to_le_bytes());
        }
        if c.primary_key {
            let k = key.get_or_insert_with(Vec::new);
            if !k.is_empty() {
                k.push(b':');
            }
            for &b in &text {
                if b == b':' || b == b'\\' {
                    k.push(b'\\');
                }
                k.push(b);
            }
        }
    }
    Ok((key, value))
}

/// The row a message made by [`encode_model`] carries.
pub fn decode_model<M: crate::orm::Model>(value: &[u8]) -> Result<M> {
    let bad = || BrokerError::local(format!("a message is not a {} row", M::TABLE));
    let mut r = Reader::new(value);
    let n = r.u16().map_err(|_| bad())? as usize;
    let mut fields: Vec<(&str, Option<&[u8]>)> = Vec::with_capacity(n);
    for _ in 0..n {
        let name = r.str().map_err(|_| bad())?;
        let len = r.u32().map_err(|_| bad())?;
        let v = if len == u32::MAX {
            None
        } else {
            let mut sub = r;
            let all = sub.rest();
            if all.len() < len as usize {
                return Err(bad());
            }
            let v = &all[..len as usize];
            r = Reader::new(&all[len as usize..]);
            Some(v)
        };
        fields.push((name, v));
    }
    let cells: Vec<Option<&[u8]>> = M::COLUMNS
        .iter()
        .map(|c| fields.iter().find(|(n, _)| *n == c.name).and_then(|(_, v)| *v))
        .collect();
    let rows = crate::kv::rows_from_text(&cells)?;
    let row = rows.get(0).ok_or_else(bad)?;
    let mut m = M::from_row(&row)?;
    m.__rdb_after_load();
    Ok(m)
}

impl Delivery {
    /// The delivered records as rows of `M` ([`encode_model`]).
    pub fn models<M: crate::orm::Model>(&self) -> impl Iterator<Item = Result<M>> + '_ {
        self.records().map(|r| decode_model::<M>(r.value))
    }
}

impl Fetched {
    /// The fetched records as rows of `M` ([`encode_model`]).
    pub fn models<M: crate::orm::Model>(&self) -> impl Iterator<Item = Result<M>> + '_ {
        self.records().map(|r| decode_model::<M>(r.value))
    }
}

/// A topic of `M` rows: a [`Producer`] that takes models, named after the
/// model's table unless told otherwise.
///
/// ```no_run
/// # use rasterizeddb_client::prelude::*;
/// # use rasterizeddb_client::broker::*;
/// # #[derive(Model, Clone)] struct Order { #[rdb(primary_key)] id: i64, total: i64 }
/// # async fn demo(broker: &Broker, o: Order) -> rasterizeddb_client::broker::Result<()> {
/// let orders = broker.model_topic::<Order>(8, ProducerOptions::default()).await?;
/// orders.publish(&o).await?;
/// let mut sub = broker.subscribe(orders.topic(), "billing", SubConfig::default(), 256).await?;
/// if let Some(d) = sub.next().await {
///     let d = d?;
///     for order in d.models::<Order>() {
///         let _order: Order = order?;
///     }
///     sub.ack_delivery(&d)?;
/// }
/// # Ok(()) }
/// ```
pub struct ModelTopic<M> {
    producer: Producer,
    _model: std::marker::PhantomData<fn(&M)>,
}

impl<M: crate::orm::Model> ModelTopic<M> {
    /// Publishes a row; resolves to its offset once stored.
    pub fn publish(&self, m: &M) -> impl std::future::Future<Output = Result<u64>> + Send + use<M> {
        let encoded = encode_model(m);
        let sent = encoded.map(|(key, value)| {
            self.producer.send(key.as_deref(), &value, &[(MODEL_HEADER, M::TABLE.as_bytes())])
        });
        async move {
            match sent {
                Ok(f) => f.await,
                Err(e) => Err(e),
            }
        }
    }

    /// Sends every partly built batch now.
    pub fn flush(&self) {
        self.producer.flush();
    }

    pub fn topic(&self) -> &TopicRef {
        self.producer.topic()
    }
}

impl Broker {
    /// The topic named after `M`'s table (created with `partitions` when it
    /// does not exist), as a [`ModelTopic`].
    pub async fn model_topic<M: crate::orm::Model>(&self, partitions: u32, options: ProducerOptions) -> Result<ModelTopic<M>> {
        let config = TopicConfig { partitions: partitions.max(1), ..Default::default() };
        let topic = match self.topic(M::TABLE).await {
            Ok(t) => t,
            Err(e) if e.code == Some(Code::NoTopic) => self.create_topic(M::TABLE, &config, true).await?,
            Err(e) => return Err(e),
        };
        Ok(self.model_topic_on::<M>(topic, options))
    }

    /// `M` rows on a topic of the caller's choice.
    pub fn model_topic_on<M: crate::orm::Model>(&self, topic: TopicRef, options: ProducerOptions) -> ModelTopic<M> {
        ModelTopic { producer: self.producer(topic, options), _model: std::marker::PhantomData }
    }
}
