//! The broker of one database: its topics, subscriptions, exchanges and
//! bindings, and the metadata log that keeps them (docs/broker.md §2).
//!
//! The metadata log (`meta.<gen>.log`, a [`LogFile`]) records every change
//! made by DDL. Each record is synced before the change is visible, so a
//! client told "created" finds the topic after a crash. Topics and
//! subscriptions are named on disk by number (`t<id>/p<n>/`), so a name never
//! reaches the filesystem.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use parking_lot::{Mutex, RwLock};
use rasterizeddb_mq::batch::{self, BatchView};
use rasterizeddb_mq::wire::{
    ANY_PARTITION, BY_KEY, Binding, Code, Durability, ExchangeKind, QueueConfig, Reader, SubConfig, SubMode,
    TopicConfig, Writer,
};

use super::exchange::{Exchange, Route};
use super::flog::{self, LogFile};
use super::flush::Flusher;
use super::partition::{Appended, Partition};
use super::sub::Subscription;
use super::{BrokerError, flush_ms};
use crate::core::framed_log::Format;

const K_TOPIC: u8 = 1;
const K_TOPIC_DROP: u8 = 2;
const K_TOPIC_ALTER: u8 = 3;
const K_SUB: u8 = 4;
const K_SUB_DROP: u8 = 5;
const K_EXCHANGE: u8 = 6;
const K_EXCHANGE_DROP: u8 = 7;
const K_BIND: u8 = 8;
const K_UNBIND: u8 = 9;
const K_IDS: u8 = 10;

pub(super) const META_FORMAT: Format = Format {
    name: "broker metadata log",
    max_record_len: 1 << 20,
    layout: flog::LAYOUT,
    known_kind: |k| matches!(k, 1..=10 | flog::SEALED),
};

/// The metadata log is compacted once it passes this.
const META_COMPACT_AT: u64 = 4 << 20;
/// The longest topic, exchange or subscription name.
pub const MAX_NAME: usize = 255;
/// The most partitions a topic may have.
pub const MAX_PARTITIONS: u32 = 4096;

/// Exchanges every broker has, as RabbitMQ predeclares them.
const BUILTIN_EXCHANGES: &[(&str, ExchangeKind)] = &[
    ("amq.direct", ExchangeKind::Direct),
    ("amq.fanout", ExchangeKind::Fanout),
    ("amq.topic", ExchangeKind::Topic),
    ("amq.headers", ExchangeKind::Headers),
    ("amq.match", ExchangeKind::Headers),
    ("amq.mqtt", ExchangeKind::Mqtt),
];

pub fn check_name(what: &str, name: &str) -> Result<(), BrokerError> {
    if name.is_empty() || name.len() > MAX_NAME {
        return Err(BrokerError::invalid(format!("{what} name must be 1 to {MAX_NAME} bytes")));
    }
    if name.chars().any(|c| c.is_control()) {
        return Err(BrokerError::invalid(format!("{what} name {name:?} contains a control character")));
    }
    Ok(())
}

fn check_config(c: &TopicConfig) -> Result<(), BrokerError> {
    if c.partitions == 0 || c.partitions > MAX_PARTITIONS {
        return Err(BrokerError::invalid(format!("partitions must be 1 to {MAX_PARTITIONS}")));
    }
    if (c.max_batch_bytes as usize) < batch::HEADER + batch::RECORD_HEADER + 4 || c.max_batch_bytes as usize > batch::MAX_BATCH {
        return Err(BrokerError::invalid("max_batch_bytes out of range"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

/// One topic: its partitions and subscriptions.
pub struct Topic {
    pub id: u32,
    pub name: String,
    config: RwLock<TopicConfig>,
    /// Set when this topic is the log of a queue.
    pub queue: Option<QueueConfig>,
    pub partitions: Vec<Arc<Partition>>,
    pub created_us: i64,
    rr: AtomicU32,
    pub(super) subs: RwLock<HashMap<String, Arc<Subscription>>>,
    pub(super) dir: Option<PathBuf>,
    deleted: AtomicBool,
}

impl Topic {
    pub fn config(&self) -> TopicConfig {
        self.config.read().clone()
    }

    pub fn partition_count(&self) -> u32 {
        self.partitions.len() as u32
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted.load(Ordering::Acquire)
    }

    pub fn partition(&self, p: u32) -> Result<&Arc<Partition>, BrokerError> {
        self.partitions.get(p as usize).ok_or_else(|| {
            BrokerError::new(
                Code::NoPartition,
                format!("topic \"{}\" has {} partitions, not {}", self.name, self.partitions.len(), p + 1),
            )
        })
    }

    /// The partition a produce names, resolving [`BY_KEY`] (the first
    /// record's key, or round robin without one) and [`ANY_PARTITION`].
    pub fn pick(&self, partition: u32, batch: &[u8]) -> Result<&Arc<Partition>, BrokerError> {
        let n = self.partitions.len() as u32;
        let p = match partition {
            ANY_PARTITION => self.rr.fetch_add(1, Ordering::Relaxed) % n,
            BY_KEY => match BatchView::trusted(batch).record(0).and_then(|r| r.key) {
                Some(k) => rasterizeddb_mq::hash::partition_for(k, n),
                None => self.rr.fetch_add(1, Ordering::Relaxed) % n,
            },
            p => p,
        };
        self.partition(p)
    }

    pub fn subscription(&self, name: &str) -> Option<Arc<Subscription>> {
        self.subs.read().get(name).cloned()
    }

    pub fn subscriptions(&self) -> Vec<Arc<Subscription>> {
        let mut v: Vec<_> = self.subs.read().values().cloned().collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    /// Messages in all partitions (next minus start).
    pub fn depth(&self) -> u64 {
        self.partitions.iter().map(|p| p.next_offset() - p.log_start()).sum()
    }

    pub fn bytes(&self) -> u64 {
        self.partitions.iter().map(|p| p.bytes.load(Ordering::Relaxed)).sum()
    }
}

// ---------------------------------------------------------------------------
// The metadata state
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct TopicMeta {
    name: String,
    config: TopicConfig,
    queue: Option<QueueConfig>,
    created_us: i64,
}

#[derive(Clone)]
struct SubMeta {
    topic: u32,
    name: String,
    config: SubConfig,
}

#[derive(Clone)]
struct ExchangeMeta {
    kind: ExchangeKind,
    alternate: String,
}

#[derive(Default)]
struct MetaState {
    topics: BTreeMap<u32, TopicMeta>,
    subs: BTreeMap<u32, SubMeta>,
    exchanges: BTreeMap<String, ExchangeMeta>,
    bindings: Vec<Binding>,
    next_topic: u32,
    next_sub: u32,
}

fn enc(f: impl FnOnce(&mut Writer<'_>)) -> Vec<u8> {
    let mut v = Vec::new();
    f(&mut Writer(&mut v));
    v
}

impl MetaState {
    fn apply(&mut self, kind: u8, body: &[u8]) -> Result<(), String> {
        let mut r = Reader::new(body);
        let e = |e: rasterizeddb_mq::wire::DecodeError| e.to_string();
        match kind {
            K_TOPIC => {
                let id = r.u32().map_err(e)?;
                let name = r.string().map_err(e)?;
                let config = TopicConfig::decode(&mut r).map_err(e)?;
                let created_us = r.i64().map_err(e)?;
                let queue = if r.u8().map_err(e)? == 1 { Some(QueueConfig::decode(&mut r).map_err(e)?) } else { None };
                self.next_topic = self.next_topic.max(id + 1);
                self.topics.insert(id, TopicMeta { name, config, queue, created_us });
            }
            K_TOPIC_DROP => {
                let id = r.u32().map_err(e)?;
                if let Some(t) = self.topics.remove(&id) {
                    self.bindings.retain(|b| b.topic != t.name);
                }
                self.subs.retain(|_, s| s.topic != id);
            }
            K_TOPIC_ALTER => {
                let id = r.u32().map_err(e)?;
                let config = TopicConfig::decode(&mut r).map_err(e)?;
                if let Some(t) = self.topics.get_mut(&id) {
                    t.config = config;
                }
            }
            K_SUB => {
                let id = r.u32().map_err(e)?;
                let topic = r.u32().map_err(e)?;
                let name = r.string().map_err(e)?;
                let config = SubConfig::decode(&mut r).map_err(e)?;
                self.next_sub = self.next_sub.max(id + 1);
                self.subs.insert(id, SubMeta { topic, name, config });
            }
            K_SUB_DROP => {
                self.subs.remove(&r.u32().map_err(e)?);
            }
            K_EXCHANGE => {
                let name = r.string().map_err(e)?;
                let kind = ExchangeKind::from_u8(r.u8().map_err(e)?).ok_or("unknown exchange kind")?;
                let alternate = r.string().map_err(e)?;
                self.exchanges.insert(name, ExchangeMeta { kind, alternate });
            }
            K_EXCHANGE_DROP => {
                let name = r.string().map_err(e)?;
                self.exchanges.remove(&name);
                self.bindings.retain(|b| b.exchange != name);
            }
            K_BIND => {
                let b = Binding::decode(&mut r).map_err(e)?;
                if !self.bindings.contains(&b) {
                    self.bindings.push(b);
                }
            }
            K_UNBIND => {
                let b = Binding::decode(&mut r).map_err(e)?;
                self.bindings.retain(|x| *x != b);
            }
            K_IDS => {
                self.next_topic = self.next_topic.max(r.u32().map_err(e)?);
                self.next_sub = self.next_sub.max(r.u32().map_err(e)?);
            }
            other => return Err(format!("unknown record kind {other}")),
        }
        Ok(())
    }

    /// The whole state as records, for a compaction.
    fn snapshot(&self) -> Vec<(u8, Vec<u8>)> {
        let mut out = vec![(K_IDS, enc(|w| {
            w.u32(self.next_topic);
            w.u32(self.next_sub)
        }))];
        for (id, t) in &self.topics {
            out.push((K_TOPIC, topic_record(*id, t)));
        }
        for (id, s) in &self.subs {
            out.push((K_SUB, enc(|w| {
                w.u32(*id);
                w.u32(s.topic);
                w.str(&s.name);
                s.config.encode(w)
            })));
        }
        for (name, x) in &self.exchanges {
            out.push((K_EXCHANGE, exchange_record(name, x)));
        }
        for b in &self.bindings {
            out.push((K_BIND, enc(|w| b.encode(w))));
        }
        out
    }
}

fn topic_record(id: u32, t: &TopicMeta) -> Vec<u8> {
    enc(|w| {
        w.u32(id);
        w.str(&t.name);
        t.config.encode(w);
        w.i64(t.created_us);
        match &t.queue {
            Some(q) => {
                w.u8(1);
                q.encode(w)
            }
            None => w.u8(0),
        }
    })
}

fn exchange_record(name: &str, x: &ExchangeMeta) -> Vec<u8> {
    enc(|w| {
        w.str(name);
        w.u8(x.kind as u8);
        w.str(&x.alternate)
    })
}

struct Meta {
    /// `None` for an in-memory database.
    log: Option<LogFile>,
    state: MetaState,
}

impl Meta {
    /// Records a change, durably, and applies it.
    fn record(&mut self, kind: u8, body: Vec<u8>) -> Result<(), BrokerError> {
        if let Some(log) = self.log.as_mut() {
            log.append_sync(kind, &body).map_err(BrokerError::io)?;
        }
        self.state.apply(kind, &body).map_err(|e| BrokerError::new(Code::Storage, e))?;
        if let Some(log) = self.log.as_mut() {
            if log.len() > META_COMPACT_AT {
                let snap = self.state.snapshot();
                if let Err(e) = log.compact(&snap) {
                    log::warn!("broker: metadata log compaction failed: {e}");
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The broker
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Topics {
    by_name: HashMap<String, Arc<Topic>>,
    by_id: HashMap<u32, Arc<Topic>>,
}

pub struct Broker {
    pub db_name: String,
    dir: Option<PathBuf>,
    meta: Mutex<Meta>,
    topics: RwLock<Topics>,
    exchanges: RwLock<HashMap<String, Exchange>>,
    pub(super) flusher: Option<Arc<Flusher>>,
    stopped: AtomicBool,
    pub(super) next_ephemeral: std::sync::atomic::AtomicU32,
    me: std::sync::Weak<Broker>,
    /// The database's replication, once attached; shared with every
    /// partition (`repl`).
    pub(super) replication: Arc<std::sync::OnceLock<Arc<crate::replication::Replication>>>,
}

impl Broker {
    /// Opens the broker in `dir` (`None`: an in-memory database), replaying
    /// its metadata and opening every topic and subscription.
    pub fn open(dir: Option<&Path>, db_name: &str) -> Result<Arc<Broker>, BrokerError> {
        let mut state = MetaState::default();
        let log = match dir {
            Some(d) => Some(
                LogFile::open(d, "meta", META_FORMAT, |kind, body| state.apply(kind, body)).map_err(BrokerError::io)?,
            ),
            None => None,
        };
        let flusher = match dir {
            Some(_) => Some(Flusher::start(db_name, flush_ms()).map_err(BrokerError::io)?),
            None => None,
        };
        let broker = Arc::new_cyclic(|me| Broker {
            me: me.clone(),
            replication: Arc::new(std::sync::OnceLock::new()),
            db_name: db_name.to_string(),
            dir: dir.map(Path::to_path_buf),
            meta: Mutex::new(Meta { log: None, state: MetaState::default() }),
            topics: RwLock::new(Topics::default()),
            exchanges: RwLock::new(HashMap::new()),
            flusher,
            stopped: AtomicBool::new(false),
            next_ephemeral: std::sync::atomic::AtomicU32::new(1 << 31),
        });
        // Topics.
        let mut topics = Topics::default();
        for (&id, t) in &state.topics {
            let topic = broker.open_topic(id, t, false).map_err(|e| {
                BrokerError::new(Code::Storage, format!("topic \"{}\" (t{id}): {e}", t.name))
            })?;
            topics.by_name.insert(t.name.clone(), topic.clone());
            topics.by_id.insert(id, topic);
        }
        // Subscriptions.
        for (&id, s) in &state.subs {
            let Some(topic) = topics.by_id.get(&s.topic) else { continue };
            let sub = Subscription::open(&broker, id, topic.clone(), &s.name, s.config.clone()).map_err(|e| {
                BrokerError::new(Code::Storage, format!("subscription \"{}\" on \"{}\": {e}", s.name, topic.name))
            })?;
            topic.subs.write().insert(s.name.clone(), sub);
        }
        // Exchanges and their bindings.
        let mut exchanges = HashMap::new();
        for (name, kind) in BUILTIN_EXCHANGES {
            exchanges.insert(name.to_string(), Exchange::new(name, *kind, None, true));
        }
        for (name, x) in &state.exchanges {
            let alt = (!x.alternate.is_empty()).then(|| x.alternate.clone());
            exchanges.insert(name.clone(), Exchange::new(name, x.kind, alt, true));
        }
        for b in &state.bindings {
            if let (Some(x), Some(t)) = (exchanges.get_mut(&b.exchange), topics.by_name.get(&b.topic)) {
                x.bind(b.clone(), t.id);
            }
        }
        *broker.topics.write() = topics;
        *broker.exchanges.write() = exchanges;
        *broker.meta.lock() = Meta { log, state };
        start_housekeeping(&broker);
        Ok(broker)
    }

    /// Records this broker's effects on `replication` from now on.
    pub fn attach_replication(&self, replication: Arc<crate::replication::Replication>) {
        let _ = self.replication.set(replication);
    }

    pub(super) fn weak(&self) -> std::sync::Weak<Broker> {
        self.me.clone()
    }

    fn topic_dir(&self, id: u32) -> Option<PathBuf> {
        self.dir.as_ref().map(|d| d.join(format!("t{id}")))
    }

    pub(super) fn sub_dir(&self, topic: u32) -> Option<PathBuf> {
        self.topic_dir(topic)
    }

    fn open_topic(&self, id: u32, t: &TopicMeta, create: bool) -> io::Result<Arc<Topic>> {
        let dir = self.topic_dir(id);
        let ctx = super::partition::Context { topic_name: Arc::from(t.name.as_str()), hook: self.replication.clone() };
        let mut partitions = Vec::with_capacity(t.config.partitions as usize);
        for p in 0..t.config.partitions {
            let pdir = dir.as_ref().map(|d| d.join(format!("p{p}")));
            let part = match &pdir {
                Some(pd) if t.config.durability != Durability::Memory => {
                    if create {
                        std::fs::create_dir_all(pd)?;
                        Partition::create(id, p, Some(pd.clone()), &t.config, self.flusher.clone(), &ctx)
                    } else {
                        std::fs::create_dir_all(pd)?;
                        Partition::open(id, p, pd.clone(), &t.config, self.flusher.clone(), &ctx)?
                    }
                }
                Some(pd) => {
                    if !create {
                        // Clears what an earlier durability left.
                        let _ = Partition::open(id, p, pd.clone(), &t.config, None, &ctx)?;
                    }
                    Partition::create(id, p, None, &t.config, None, &ctx)
                }
                None => Partition::create(id, p, None, &t.config, None, &ctx),
            };
            partitions.push(part);
        }
        // A memory topic has no directory to make durable.
        if create && t.config.durability != Durability::Memory {
            if let Some(d) = &dir {
                super::sync_dir(d)?;
                if let Some(parent) = d.parent() {
                    super::sync_dir(parent)?;
                }
            }
        }
        Ok(Arc::new(Topic {
            id,
            name: t.name.clone(),
            config: RwLock::new(t.config.clone()),
            queue: t.queue.clone(),
            partitions,
            created_us: t.created_us,
            rr: AtomicU32::new(0),
            subs: RwLock::new(HashMap::new()),
            dir,
            deleted: AtomicBool::new(false),
        }))
    }

    fn check_running(&self) -> Result<(), BrokerError> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(BrokerError::new(Code::ShuttingDown, "the broker is shutting down"));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Topics
    // -----------------------------------------------------------------------

    pub fn topic(&self, name: &str) -> Option<Arc<Topic>> {
        self.topics.read().by_name.get(name).cloned()
    }

    pub fn topic_by_id(&self, id: u32) -> Option<Arc<Topic>> {
        self.topics.read().by_id.get(&id).cloned()
    }

    pub fn require_topic(&self, name: &str) -> Result<Arc<Topic>, BrokerError> {
        self.topic(name).ok_or_else(|| BrokerError::no_topic(name))
    }

    pub fn require_topic_id(&self, id: u32) -> Result<Arc<Topic>, BrokerError> {
        self.topic_by_id(id)
            .ok_or_else(|| BrokerError::new(Code::NoTopic, format!("no topic has id {id}; resolve its name again")))
    }

    pub fn topics(&self) -> Vec<Arc<Topic>> {
        let mut v: Vec<_> = self.topics.read().by_id.values().cloned().collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    /// Creates a topic. With `if_not_exists`, an existing topic of that name
    /// is returned when its settings match, and [`Code::Mismatch`] otherwise.
    pub fn create_topic(&self, name: &str, config: TopicConfig, if_not_exists: bool) -> Result<Arc<Topic>, BrokerError> {
        self.create_topic_inner(name, config, None, if_not_exists)
    }

    fn create_topic_inner(
        &self,
        name: &str,
        mut config: TopicConfig,
        queue: Option<QueueConfig>,
        if_not_exists: bool,
    ) -> Result<Arc<Topic>, BrokerError> {
        self.check_running()?;
        check_name("topic", name)?;
        check_config(&config)?;
        if self.dir.is_none() {
            config.durability = Durability::Memory;
        }
        let mut meta = self.meta.lock();
        if let Some(t) = self.topic(name) {
            if !if_not_exists {
                return Err(BrokerError::new(Code::TopicExists, format!("topic \"{name}\" already exists")));
            }
            let mine = t.config();
            if mine.partitions != config.partitions || mine.durability != config.durability || t.queue.is_some() != queue.is_some() {
                return Err(BrokerError::new(
                    Code::Mismatch,
                    format!("topic \"{name}\" exists with other settings ({} partitions, {})", mine.partitions, mine.durability.name()),
                ));
            }
            return Ok(t);
        }
        let id = meta.state.next_topic;
        let tm = TopicMeta { name: name.to_string(), config, queue, created_us: super::now_us() };
        let topic = self.open_topic(id, &tm, true).map_err(BrokerError::io)?;
        meta.record(K_TOPIC, topic_record(id, &tm))?;
        {
            let mut topics = self.topics.write();
            topics.by_name.insert(name.to_string(), topic.clone());
            topics.by_id.insert(id, topic.clone());
        }
        if let Some(r) = super::repl::capturing(self.replication.get()) {
            super::repl::topic(r, name, &tm.config, tm.queue.as_ref());
        }
        Ok(topic)
    }

    /// Deletes a topic, its subscriptions, its bindings and its files.
    pub fn delete_topic(&self, name: &str, if_exists: bool) -> Result<bool, BrokerError> {
        self.check_running()?;
        let mut meta = self.meta.lock();
        let Some(topic) = self.topic(name) else {
            return if if_exists { Ok(false) } else { Err(BrokerError::no_topic(name)) };
        };
        meta.record(K_TOPIC_DROP, enc(|w| w.u32(topic.id)))?;
        if let Some(r) = super::repl::capturing(self.replication.get()) {
            super::repl::topic_drop(r, name);
        }
        {
            let mut topics = self.topics.write();
            topics.by_name.remove(name);
            topics.by_id.remove(&topic.id);
        }
        topic.deleted.store(true, Ordering::Release);
        for x in self.exchanges.write().values_mut() {
            x.unbind_topic(topic.id);
        }
        let subs: Vec<_> = topic.subs.write().drain().map(|(_, s)| s).collect();
        for s in subs {
            s.close("its topic was deleted");
        }
        if let Some(d) = &topic.dir {
            if let Err(e) = std::fs::remove_dir_all(d) {
                log::warn!("broker: could not remove {}: {e}", d.display());
            }
        }
        Ok(true)
    }

    /// Changes a topic's retention and batch limit. Partitions and durability
    /// are fixed once created.
    pub fn alter_topic(&self, name: &str, f: impl FnOnce(&mut TopicConfig)) -> Result<TopicConfig, BrokerError> {
        self.check_running()?;
        let mut meta = self.meta.lock();
        let topic = self.require_topic(name)?;
        let old = topic.config();
        let mut new = old.clone();
        f(&mut new);
        if new.partitions != old.partitions || new.durability != old.durability {
            return Err(BrokerError::new(
                Code::Unsupported,
                "a topic's partitions and durability are fixed once it is created; create another and move to it",
            ));
        }
        check_config(&new)?;
        meta.record(K_TOPIC_ALTER, enc(|w| {
            w.u32(topic.id);
            new.encode(w)
        }))?;
        *topic.config.write() = new.clone();
        if let Some(r) = super::repl::capturing(self.replication.get()) {
            super::repl::topic_alter(r, name, &new);
        }
        Ok(new)
    }

    // -----------------------------------------------------------------------
    // Queues
    // -----------------------------------------------------------------------

    /// Declares a queue: a one-partition topic and a shared subscription of
    /// the same name.
    pub fn declare_queue(&self, name: &str, q: QueueConfig, if_not_exists: bool) -> Result<Arc<Topic>, BrokerError> {
        // Declaring a queue that exists with other settings is refused, as
        // RabbitMQ refuses an inequivalent declare: returning the old queue
        // silently left a consumer with an ack timeout it never asked for.
        if let Some(existing) = self.topic(name).and_then(|t| t.queue.clone()) {
            let mut q_eff = q.clone();
            if self.dir.is_none() {
                q_eff.durability = Durability::Memory;
            }
            let differs = queue_differences(&existing, &q_eff);
            if !differs.is_empty() {
                return Err(BrokerError::new(
                    Code::Mismatch,
                    format!("queue \"{name}\" exists with other settings: {}", differs.join(", ")),
                ));
            }
        }
        let config = TopicConfig {
            partitions: 1,
            durability: q.durability,
            retention_ms: -1,
            retention_bytes: -1,
            ..TopicConfig::default()
        };
        let topic = self.create_topic_inner(name, config, Some(q.clone()), if_not_exists)?;
        let sub = SubConfig {
            mode: SubMode::Shared,
            start: rasterizeddb_mq::wire::Start::Earliest,
            ack_timeout_ms: q.ack_timeout_ms,
            max_deliveries: q.max_deliveries,
            dead_letter: q.dead_letter.clone(),
        };
        self.subscription(&topic, name, sub)?;
        Ok(topic)
    }

    /// The queue's topic and its shared subscription.
    pub fn queue(&self, name: &str) -> Result<(Arc<Topic>, Arc<Subscription>), BrokerError> {
        let topic = self.topic(name).filter(|t| t.queue.is_some()).ok_or_else(|| {
            BrokerError::new(Code::NoTopic, format!("queue \"{name}\" does not exist"))
        })?;
        let sub = topic
            .subscription(name)
            .ok_or_else(|| BrokerError::new(Code::NoSubscription, format!("queue \"{name}\" has lost its subscription")))?;
        Ok((topic, sub))
    }

    // -----------------------------------------------------------------------
    // Subscriptions
    // -----------------------------------------------------------------------

    /// The named subscription on `topic`, created with `config` when it does
    /// not exist. A broadcast subscription is never stored and gets a fresh
    /// one per call.
    pub fn subscription(&self, topic: &Arc<Topic>, name: &str, config: SubConfig) -> Result<Arc<Subscription>, BrokerError> {
        self.check_running()?;
        if config.mode == SubMode::Broadcast {
            let id = self.next_ephemeral.fetch_add(1, Ordering::Relaxed);
            return Subscription::ephemeral(self, id, topic.clone(), config);
        }
        check_name("subscription", name)?;
        if let Some(s) = topic.subscription(name) {
            if s.config.mode != config.mode {
                return Err(BrokerError::new(
                    Code::Mismatch,
                    format!("subscription \"{name}\" exists as {}, not {}", s.config.mode.name(), config.mode.name()),
                ));
            }
            return Ok(s);
        }
        let mut meta = self.meta.lock();
        if let Some(s) = topic.subscription(name) {
            return Ok(s);
        }
        if topic.is_deleted() {
            return Err(BrokerError::no_topic(&topic.name));
        }
        let id = meta.state.next_sub;
        let sub = Subscription::create(self, id, topic.clone(), name, config.clone()).map_err(BrokerError::io)?;
        meta.record(K_SUB, enc(|w| {
            w.u32(id);
            w.u32(topic.id);
            w.str(name);
            config.encode(w)
        }))?;
        topic.subs.write().insert(name.to_string(), sub.clone());
        if let Some(r) = super::repl::capturing(self.replication.get()) {
            super::repl::sub(r, &topic.name, name, &config);
        }
        Ok(sub)
    }

    pub fn delete_subscription(&self, topic: &Arc<Topic>, name: &str, if_exists: bool) -> Result<bool, BrokerError> {
        self.check_running()?;
        let mut meta = self.meta.lock();
        let Some(sub) = topic.subscription(name) else {
            return if if_exists {
                Ok(false)
            } else {
                Err(BrokerError::new(Code::NoSubscription, format!("subscription \"{name}\" does not exist")))
            };
        };
        meta.record(K_SUB_DROP, enc(|w| w.u32(sub.id)))?;
        if let Some(r) = super::repl::capturing(self.replication.get()) {
            super::repl::sub_drop(r, &topic.name, name);
        }
        topic.subs.write().remove(name);
        sub.close("it was deleted");
        sub.remove_files();
        Ok(true)
    }

    // -----------------------------------------------------------------------
    // Producing
    // -----------------------------------------------------------------------

    /// Appends one batch to `topic`. `verify` checks a batch that came from a
    /// client (both CRCs and every record); one built in this process skips
    /// that.
    pub fn produce(
        &self,
        topic: &Topic,
        partition: u32,
        batch: &mut [u8],
        verify: bool,
    ) -> Result<(Arc<Partition>, Appended), BrokerError> {
        if topic.is_deleted() {
            return Err(BrokerError::no_topic(&topic.name));
        }
        if verify {
            let limit = topic.config.read().max_batch_bytes as usize;
            batch::check(batch, limit, true)?;
        } else if batch.len() < batch::HEADER {
            return Err(BrokerError::new(Code::BadBatch, "batch too short"));
        }
        let p = topic.pick(partition, batch)?;
        let a = p.append(batch)?;
        Ok((p.clone(), a))
    }

    /// Publishes one batch through an exchange (`""` is the default
    /// exchange, which routes to the queue or topic named by the key). The
    /// batch is routed as one message, by `key` and its first record's
    /// headers. Returns where it went; empty when nothing matched.
    pub fn publish(
        &self,
        exchange: &str,
        key: &str,
        batch: &mut [u8],
        verify: bool,
    ) -> Result<Vec<(Arc<Partition>, Appended)>, BrokerError> {
        if verify {
            batch::check(batch, batch::MAX_BATCH, true)?;
        }
        let topics = self.route(exchange, key, batch)?;
        let mut out = Vec::with_capacity(topics.len());
        let n = topics.len();
        for (i, t) in topics.into_iter().enumerate() {
            let Some(topic) = self.topic_by_id(t) else { continue };
            // Each topic stamps its own copy; the last takes the original.
            let r = if i + 1 == n {
                self.produce(&topic, ANY_PARTITION_OR_KEY, batch, false)
            } else {
                let mut copy = batch.to_vec();
                self.produce(&topic, ANY_PARTITION_OR_KEY, &mut copy, false)
            };
            match r {
                Ok(x) => out.push(x),
                Err(e) if e.code == Code::NoTopic => {}
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    /// The topics a publish to `exchange` with `key` reaches.
    pub fn route(&self, exchange: &str, key: &str, batch: &[u8]) -> Result<Route, BrokerError> {
        if exchange.is_empty() {
            let mut r = Route::new();
            if let Some(t) = self.topic(key) {
                r.push(t.id);
            }
            return Ok(r);
        }
        let first = BatchView::trusted(batch).record(0);
        let header = |name: &str| first.and_then(|r| r.header(name.as_bytes()));
        let xs = self.exchanges.read();
        let x = xs
            .get(exchange)
            .ok_or_else(|| BrokerError::new(Code::NoExchange, format!("exchange \"{exchange}\" does not exist")))?;
        let mut r = x.route(key, header);
        if r.is_empty() {
            if let Some(alt) = x.alternate.as_deref().and_then(|a| xs.get(a)) {
                r = alt.route(key, header);
            }
        }
        Ok(r)
    }

    // -----------------------------------------------------------------------
    // Exchanges and bindings
    // -----------------------------------------------------------------------

    pub fn declare_exchange(
        &self,
        name: &str,
        kind: ExchangeKind,
        alternate: Option<&str>,
        durable: bool,
    ) -> Result<(), BrokerError> {
        self.check_running()?;
        check_name("exchange", name)?;
        let mut meta = self.meta.lock();
        let mut xs = self.exchanges.write();
        if let Some(x) = xs.get(name) {
            if x.kind != kind {
                return Err(BrokerError::new(
                    Code::Mismatch,
                    format!("exchange \"{name}\" exists as {}, not {}", x.kind.name(), kind.name()),
                ));
            }
            return Ok(());
        }
        if durable {
            let xm = ExchangeMeta { kind, alternate: alternate.unwrap_or("").to_string() };
            meta.record(K_EXCHANGE, exchange_record(name, &xm))?;
            if let Some(r) = super::repl::capturing(self.replication.get()) {
                super::repl::exchange(r, name, kind, alternate);
            }
        }
        xs.insert(name.to_string(), Exchange::new(name, kind, alternate.map(str::to_string), durable));
        Ok(())
    }

    pub fn delete_exchange(&self, name: &str, if_exists: bool) -> Result<bool, BrokerError> {
        self.check_running()?;
        if name.starts_with("amq.") {
            return Err(BrokerError::new(Code::Permission, format!("exchange \"{name}\" is built in")));
        }
        let mut meta = self.meta.lock();
        let mut xs = self.exchanges.write();
        let Some(x) = xs.remove(name) else {
            return if if_exists {
                Ok(false)
            } else {
                Err(BrokerError::new(Code::NoExchange, format!("exchange \"{name}\" does not exist")))
            };
        };
        if x.durable {
            meta.record(K_EXCHANGE_DROP, enc(|w| w.str(name)))?;
            if let Some(r) = super::repl::capturing(self.replication.get()) {
                super::repl::exchange_drop(r, name);
            }
        }
        Ok(true)
    }

    pub fn exchange_kind(&self, name: &str) -> Option<ExchangeKind> {
        self.exchanges.read().get(name).map(|x| x.kind)
    }

    /// `(name, kind, durable, bindings)` of every exchange.
    pub fn exchanges(&self) -> Vec<(String, ExchangeKind, bool, Vec<Binding>)> {
        let mut v: Vec<_> = self
            .exchanges
            .read()
            .values()
            .map(|x| (x.name.clone(), x.kind, x.durable, x.bindings.iter().map(|(b, _)| b.clone()).collect()))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    pub fn bind(&self, b: Binding) -> Result<bool, BrokerError> {
        self.check_running()?;
        let mut meta = self.meta.lock();
        let topic = self.require_topic(&b.topic)?;
        let mut xs = self.exchanges.write();
        let x = xs
            .get_mut(&b.exchange)
            .ok_or_else(|| BrokerError::new(Code::NoExchange, format!("exchange \"{}\" does not exist", b.exchange)))?;
        if !x.bind(b.clone(), topic.id) {
            return Ok(false);
        }
        if x.durable && topic.config().durability != Durability::Memory {
            if let Err(e) = meta.record(K_BIND, enc(|w| b.encode(w))) {
                x.unbind(&b, topic.id);
                return Err(e);
            }
            if let Some(r) = super::repl::capturing(self.replication.get()) {
                super::repl::bind(r, &b, false);
            }
        }
        Ok(true)
    }

    pub fn unbind(&self, b: &Binding) -> Result<bool, BrokerError> {
        self.check_running()?;
        let mut meta = self.meta.lock();
        let Some(topic) = self.topic(&b.topic) else { return Ok(false) };
        let mut xs = self.exchanges.write();
        let Some(x) = xs.get_mut(&b.exchange) else { return Ok(false) };
        if !x.unbind(b, topic.id) {
            return Ok(false);
        }
        if x.durable {
            meta.record(K_UNBIND, enc(|w| b.encode(w)))?;
            if let Some(r) = super::repl::capturing(self.replication.get()) {
                super::repl::bind(r, b, true);
            }
        }
        Ok(true)
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// Stops taking work, writes and syncs everything pending.
    pub fn shutdown(&self) {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return;
        }
        for t in self.topics() {
            for s in t.subscriptions() {
                s.close("the server is shutting down");
                s.flush_now();
            }
            for p in &t.partitions {
                if let Ok(Some(r)) = p.write_pending() {
                    let ok = r.files.iter().all(|f| super::sync_file(f).is_ok());
                    if ok {
                        p.mark_durable(r.upto);
                    }
                }
            }
        }
        if let Some(f) = &self.flusher {
            f.stop();
        }
    }

    /// Housekeeping: retention, subscription timers and log compaction.
    fn housekeep(&self, tick: u64) {
        // Retention every ten seconds.
        if tick % 100 == 0 {
            for t in self.topics() {
                let c = t.config();
                if c.retention_ms < 0 && c.retention_bytes < 0 {
                    continue;
                }
                let per_part = if c.retention_bytes >= 0 { c.retention_bytes / c.partitions as i64 } else { -1 };
                for p in &t.partitions {
                    p.apply_retention(c.retention_ms, per_part);
                }
            }
        }
        for t in self.topics() {
            for s in t.subscriptions() {
                s.tick();
            }
        }
    }
}

/// The settings in which two queue declarations differ, as `name: have vs asked`.
fn queue_differences(have: &QueueConfig, asked: &QueueConfig) -> Vec<String> {
    let mut d = Vec::new();
    if have.durability != asked.durability {
        d.push(format!("durability {} vs {}", have.durability.name(), asked.durability.name()));
    }
    if have.ack_timeout_ms != asked.ack_timeout_ms {
        d.push(format!("ack_timeout_ms {} vs {}", have.ack_timeout_ms, asked.ack_timeout_ms));
    }
    if have.max_deliveries != asked.max_deliveries {
        d.push(format!("max_deliveries {} vs {}", have.max_deliveries, asked.max_deliveries));
    }
    if have.dead_letter != asked.dead_letter {
        d.push(format!("dead_letter {:?} vs {:?}", have.dead_letter, asked.dead_letter));
    }
    if have.message_ttl_ms != asked.message_ttl_ms {
        d.push(format!("message_ttl_ms {} vs {}", have.message_ttl_ms, asked.message_ttl_ms));
    }
    if have.max_length != asked.max_length {
        d.push(format!("max_length {} vs {}", have.max_length, asked.max_length));
    }
    d
}

/// `publish`'s partition choice: the first record's key, else round robin.
const ANY_PARTITION_OR_KEY: u32 = BY_KEY;

fn start_housekeeping(broker: &Arc<Broker>) {
    let weak = Arc::downgrade(broker);
    let name = broker.db_name.clone();
    let spawned = std::thread::Builder::new().name(format!("broker-house-{name}")).spawn(move || {
        let mut tick = 0u64;
        loop {
            std::thread::sleep(std::time::Duration::from_millis(100));
            let Some(b) = weak.upgrade() else { return };
            if b.is_stopped() {
                return;
            }
            tick += 1;
            b.housekeep(tick);
        }
    });
    if let Err(e) = spawned {
        log::error!("broker: could not start housekeeping: {e}");
    }
}

impl std::fmt::Debug for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Topic").field("id", &self.id).field("name", &self.name).field("partitions", &self.partitions.len()).finish()
    }
}
