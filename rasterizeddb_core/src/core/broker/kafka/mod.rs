//! The Kafka protocol on `--kafka-port` (feature `broker_kafka`,
//! docs/broker.md §7).
//!
//! The server presents itself as a one-broker cluster, node 1, and serves the
//! non-flexible versions of the APIs a producer, a consumer group and an admin
//! client use ([`versions`]). That is the protocol level of Kafka 2.1, which is
//! the floor that current clients (Kafka 4.x, librdkafka 2.x) still require
//! (KIP-896). A client asks ApiVersions first and keeps to what it is told.
//!
//! - A Kafka topic is a topic, and a partition is a partition.
//! - A produced RecordBatch is rebuilt as a native batch ([`codec`]). Its
//!   producer id, epoch and sequence carry over, so idempotent producers are
//!   deduplicated by the partition's own check.
//! - A fetch rebuilds Kafka batches from stored ones.
//! - A consumer group's committed offsets are an ordered subscription named
//!   after the group, one per topic, made at the first commit. The group
//!   rounds run in [`group`].
//!
//! As on a Kafka broker, a connection's requests are answered in order, one at
//! a time. A JoinGroup or a long-poll fetch therefore holds up only its own
//! connection.

pub mod codec;
pub mod group;

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use rasterizeddb_mq::wire::{Code, Durability, SubConfig, SubMode, TopicConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use self::codec::{In, Out};
use self::group::Groups;
use super::{Broker, BrokerError};
use crate::core::database::Database;
use crate::core::listen::{self, Listeners, Sock, SocketSpec};

/// The node id this server is in its one-node cluster.
const NODE: i32 = 1;
const CLUSTER_ID: &str = "rasterizeddb";

/// `(api key, lowest version, highest version)` served.
pub const VERSIONS: &[(i16, i16, i16)] = &[
    (PRODUCE, 3, 8),
    (FETCH, 4, 11),
    (LIST_OFFSETS, 1, 5),
    (METADATA, 1, 8),
    (OFFSET_COMMIT, 2, 7),
    (OFFSET_FETCH, 1, 5),
    (FIND_COORDINATOR, 0, 2),
    (JOIN_GROUP, 2, 5),
    (HEARTBEAT, 0, 3),
    (LEAVE_GROUP, 0, 3),
    (SYNC_GROUP, 0, 3),
    (SASL_HANDSHAKE, 0, 1),
    (API_VERSIONS, 0, 2),
    (CREATE_TOPICS, 2, 4),
    (DELETE_TOPICS, 1, 3),
    (INIT_PRODUCER_ID, 0, 1),
    (DESCRIBE_CONFIGS, 0, 2),
    (SASL_AUTHENTICATE, 0, 1),
];

/// The versions table, for docs and tests.
pub fn versions() -> &'static [(i16, i16, i16)] {
    VERSIONS
}

const PRODUCE: i16 = 0;
const FETCH: i16 = 1;
const LIST_OFFSETS: i16 = 2;
const METADATA: i16 = 3;
const OFFSET_COMMIT: i16 = 8;
const OFFSET_FETCH: i16 = 9;
const FIND_COORDINATOR: i16 = 10;
const JOIN_GROUP: i16 = 11;
const HEARTBEAT: i16 = 12;
const LEAVE_GROUP: i16 = 13;
const SYNC_GROUP: i16 = 14;
const SASL_HANDSHAKE: i16 = 17;
const API_VERSIONS: i16 = 18;
const CREATE_TOPICS: i16 = 19;
const DELETE_TOPICS: i16 = 20;
const INIT_PRODUCER_ID: i16 = 22;
const DESCRIBE_CONFIGS: i16 = 32;
const SASL_AUTHENTICATE: i16 = 36;

// Error codes.
const E_NONE: i16 = 0;
const E_OFFSET_OUT_OF_RANGE: i16 = 1;
const E_CORRUPT: i16 = 2;
const E_UNKNOWN_TOPIC: i16 = 3;
const E_MESSAGE_TOO_LARGE: i16 = 10;
const E_INVALID_TOPIC: i16 = 17;
const E_UNSUPPORTED_SASL: i16 = 33;
const E_ILLEGAL_SASL_STATE: i16 = 34;
const E_UNSUPPORTED_VERSION: i16 = 35;
const E_TOPIC_EXISTS: i16 = 36;
const E_INVALID_PARTITIONS: i16 = 37;
const E_INVALID_REQUEST: i16 = 42;
const E_OUT_OF_ORDER_SEQUENCE: i16 = 45;
const E_INVALID_PRODUCER_EPOCH: i16 = 47;
const E_TRANSACTIONAL_ID_AUTH: i16 = 53;
const E_SASL_FAILED: i16 = 58;
const E_KAFKA_STORAGE: i16 = 56;
const E_INVALID_RECORD: i16 = 87;
const E_UNKNOWN: i16 = -1;

/// How Kafka clients prove who they are.
#[derive(Clone, Debug)]
pub enum KafkaAuth {
    /// No SASL: only on loopback, or a socket only its owner and group reach.
    Trust,
    /// SASL PLAIN against the database's roles (use TLS: PLAIN sends the
    /// password as it is).
    Sasl,
}

#[derive(Clone)]
pub struct KafkaConfig {
    pub addr: String,
    pub port: Option<u16>,
    pub socket: Option<SocketSpec>,
    /// The host clients are told to connect to (Metadata, FindCoordinator).
    pub advertised_host: String,
    pub advertised_port: Option<u16>,
    pub auth: KafkaAuth,
    pub threads: Option<usize>,
    pub tls: Option<tokio_rustls::TlsAcceptor>,
    /// Create a topic a client names in Metadata or Produce, as Kafka's
    /// `auto.create.topics.enable`.
    pub auto_create: bool,
    pub default_partitions: u32,
}

impl std::fmt::Debug for KafkaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaConfig")
            .field("addr", &self.addr)
            .field("port", &self.port)
            .field("advertised_host", &self.advertised_host)
            .field("auth", &self.auth)
            .field("tls", &self.tls.is_some())
            .finish()
    }
}

/// Binds the listeners, refusing an unauthenticated one anyone can reach.
pub async fn bind(config: &KafkaConfig) -> io::Result<Listeners> {
    if config.port.is_none() && config.socket.is_none() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "the Kafka protocol needs --kafka-port"));
    }
    if matches!(config.auth, KafkaAuth::Trust) && config.port.is_some() && !listen::is_loopback(&config.addr) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "--kafka-auth trust on {} would let anyone on the network read and write every topic; \
                 bind 127.0.0.1 or use --kafka-auth sasl",
                config.addr
            ),
        ));
    }
    listen::bind(&config.addr, config.port, config.socket.as_ref()).await
}

struct Shared {
    broker: Arc<Broker>,
    database: rclite::Arc<Database>,
    groups: Groups,
    config: KafkaConfig,
    port: u16,
    main: tokio::runtime::Handle,
    next_producer: AtomicI64,
}

/// Serves the Kafka protocol until a listener fails.
pub async fn serve(listeners: Listeners, database: rclite::Arc<Database>, config: KafkaConfig) -> io::Result<()> {
    let broker = database.broker.get().map_err(|e| io::Error::other(e.to_string()))?;
    if matches!(config.auth, KafkaAuth::Sasl) && config.tls.is_none() && !listen::is_loopback(&config.addr) {
        log::warn!(
            "kafka: SASL PLAIN on {} without --kafka-tls on: passwords cross the network in the clear",
            config.addr
        );
    }
    let port = config.advertised_port.or_else(|| listeners.tcp_port()).unwrap_or(9092);
    log::info!(
        "broker: Kafka protocol on {}:{} (advertised as {}:{port}, {})",
        config.addr,
        listeners.tcp_port().unwrap_or(0),
        config.advertised_host,
        match config.auth {
            KafkaAuth::Trust => "no authentication",
            KafkaAuth::Sasl => "SASL PLAIN, database roles",
        }
    );
    // Producer ids must not repeat across restarts, or a new producer could
    // inherit an old one's sequence: start from the clock.
    let start = (super::now_us().max(1) << 8) & i64::MAX;
    let shared = Arc::new(Shared {
        broker,
        database,
        groups: Groups::default(),
        port,
        main: tokio::runtime::Handle::current(),
        next_producer: AtomicI64::new(start),
        config,
    });
    {
        let weak = Arc::downgrade(&shared);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let Some(s) = weak.upgrade() else { return };
                s.groups.tick();
            }
        });
    }
    let threads = listen::default_threads(shared.config.threads);
    listen::serve_per_core(listeners, "kafka", threads, move |socket, peer| {
        let shared = shared.clone();
        async move {
            let Some(socket) = listen::tls_accept(socket, shared.config.tls.as_ref(), &peer, "kafka").await else {
                return;
            };
            if let Err(e) = connection(socket, shared).await {
                if !matches!(e.kind(), io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe | io::ErrorKind::UnexpectedEof) {
                    log::warn!("kafka: connection from {peer} ended: {e}");
                }
            }
        }
    })
    .await
}

/// The largest request accepted.
const MAX_REQUEST: usize = 100 << 20;
/// A request before SASL may be at most this big.
const UNAUTH_REQUEST: usize = 64 << 10;

struct Conn {
    shared: Arc<Shared>,
    authed: bool,
    /// SaslHandshake v0 was asked: the next frame is a bare SASL token.
    raw_sasl: bool,
    mechanism_ok: bool,
}

async fn connection(mut sock: Sock, shared: Arc<Shared>) -> io::Result<()> {
    let mut conn = Conn { authed: matches!(shared.config.auth, KafkaAuth::Trust), shared, raw_sasl: false, mechanism_ok: false };
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut out: Vec<u8> = Vec::with_capacity(64 * 1024);
    loop {
        while buf.len() < 4 {
            if sock.read_buf(&mut buf).await? == 0 {
                return Ok(());
            }
        }
        let size = i32::from_be_bytes(buf[0..4].try_into().unwrap());
        let limit = if conn.authed { MAX_REQUEST } else { UNAUTH_REQUEST };
        if size < 0 || size as usize > limit {
            return Err(io::Error::new(io::ErrorKind::InvalidData, format!("request of {size} bytes")));
        }
        let total = 4 + size as usize;
        if buf.capacity() < total {
            buf.reserve(total - buf.len());
        }
        while buf.len() < total {
            if sock.read_buf(&mut buf).await? == 0 {
                return Ok(());
            }
        }
        out.clear();
        let keep_going = conn.request(&buf[4..total], &mut out).await;
        buf.drain(..total);
        if !out.is_empty() {
            sock.write_all(&out).await?;
        }
        if !keep_going {
            return Ok(());
        }
    }
}

/// The response frame being built: size, correlation id, body.
fn begin(out: &mut Vec<u8>, corr: i32) -> usize {
    let at = out.len();
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&corr.to_be_bytes());
    at
}

fn end(out: &mut [u8], at: usize) {
    let n = (out.len() - at - 4) as i32;
    out[at..at + 4].copy_from_slice(&n.to_be_bytes());
}

fn code_of(e: &BrokerError) -> i16 {
    match e.code {
        Code::NoTopic | Code::NoPartition => E_UNKNOWN_TOPIC,
        Code::OffsetOutOfRange => E_OFFSET_OUT_OF_RANGE,
        Code::BadBatch => E_CORRUPT,
        Code::TooLarge => E_MESSAGE_TOO_LARGE,
        Code::Fenced => E_INVALID_PRODUCER_EPOCH,
        Code::OutOfOrder => E_OUT_OF_ORDER_SEQUENCE,
        Code::TopicExists => E_TOPIC_EXISTS,
        Code::Invalid => E_INVALID_REQUEST,
        Code::Storage => E_KAFKA_STORAGE,
        _ => E_UNKNOWN,
    }
}

impl Conn {
    /// Handles one request. `false` closes the connection.
    async fn request(&mut self, req: &[u8], out: &mut Vec<u8>) -> bool {
        if self.raw_sasl {
            // SaslHandshake v0: a bare token, answered with a bare frame.
            self.raw_sasl = false;
            let ok = self.sasl_plain(req).await.is_ok();
            if ok {
                out.extend_from_slice(&0i32.to_be_bytes());
            }
            return ok;
        }
        let mut r = In::new(req);
        let parsed = (|| -> codec::R<(i16, i16, i32, String)> {
            let key = r.i16()?;
            let version = r.i16()?;
            let corr = r.i32()?;
            let client = r.nstr()?.unwrap_or("").to_string();
            Ok((key, version, corr, client))
        })();
        let Ok((key, version, corr, client)) = parsed else { return false };
        let supported = VERSIONS.iter().find(|(k, _, _)| *k == key);
        if key == API_VERSIONS {
            // Answered in the v0 layout whatever was asked when the version
            // is not served, so a newer client learns what to ask instead.
            let v = if supported.is_some_and(|(_, lo, hi)| version >= *lo && version <= *hi) { version } else { 0 };
            let err = if v != version { E_UNSUPPORTED_VERSION } else { E_NONE };
            let at = begin(out, corr);
            let mut w = Out(out);
            w.i16(err);
            w.array(VERSIONS.len());
            for (k, lo, hi) in VERSIONS {
                w.i16(*k);
                w.i16(*lo);
                w.i16(*hi);
            }
            if v >= 1 {
                w.i32(0);
            }
            end(out, at);
            return true;
        }
        let Some(&(_, lo, hi)) = supported else {
            log::debug!("kafka: {client} asked for API {key}, which is not served");
            return false;
        };
        if version < lo || version > hi {
            log::debug!("kafka: {client} asked for API {key} v{version}; v{lo}-v{hi} are served");
            return false;
        }
        if !self.authed && !matches!(key, SASL_HANDSHAKE | SASL_AUTHENTICATE) {
            return false;
        }
        let body = r;
        let at = begin(out, corr);
        let result = match key {
            PRODUCE => self.produce(version, body, out).await,
            FETCH => self.fetch(version, body, out).await,
            LIST_OFFSETS => self.list_offsets(version, body, out),
            METADATA => self.metadata(version, body, out),
            OFFSET_COMMIT => self.offset_commit(version, body, out),
            OFFSET_FETCH => self.offset_fetch(version, body, out),
            FIND_COORDINATOR => self.find_coordinator(version, body, out),
            JOIN_GROUP => self.join_group(version, body, out, &client).await,
            HEARTBEAT => self.heartbeat(version, body, out),
            LEAVE_GROUP => self.leave_group(version, body, out),
            SYNC_GROUP => self.sync_group(version, body, out).await,
            SASL_HANDSHAKE => self.sasl_handshake(version, body, out),
            SASL_AUTHENTICATE => self.sasl_authenticate(version, body, out).await,
            CREATE_TOPICS => self.create_topics(version, body, out),
            DELETE_TOPICS => self.delete_topics(version, body, out),
            INIT_PRODUCER_ID => self.init_producer_id(version, body, out),
            DESCRIBE_CONFIGS => self.describe_configs(version, body, out),
            _ => Err(codec::Malformed("unserved API")),
        };
        match result {
            Ok(Reply::Send) => {
                end(out, at);
                true
            }
            // A produce with acks=0 has no response at all.
            Ok(Reply::Silent) => {
                out.truncate(at);
                true
            }
            Ok(Reply::Close) => {
                end(out, at);
                false
            }
            Err(e) => {
                log::debug!("kafka: malformed request {key} v{version} from {client}: {}", e.0);
                false
            }
        }
    }

    // -----------------------------------------------------------------------
    // Topics
    // -----------------------------------------------------------------------

    fn topic_or_create(&self, name: &str) -> Result<Arc<super::Topic>, BrokerError> {
        match self.shared.broker.topic(name) {
            Some(t) => Ok(t),
            None if self.shared.config.auto_create => self.shared.broker.create_topic(
                name,
                TopicConfig { partitions: self.shared.config.default_partitions.max(1), ..TopicConfig::default() },
                true,
            ),
            None => Err(BrokerError::no_topic(name)),
        }
    }

    fn metadata(&mut self, v: i16, mut r: In<'_>, out: &mut Vec<u8>) -> codec::R<Reply> {
        let names: Option<Vec<String>> = match r.narray()? {
            None => None,
            Some(n) => {
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    v.push(r.str()?.to_string());
                }
                Some(v)
            }
        };
        let allow_create = if v >= 4 { r.bool()? } else { true };
        // v0 asked for every topic with an empty list.
        let names = names.filter(|n| !n.is_empty());
        let topics: Vec<(String, Result<Arc<super::Topic>, BrokerError>)> = match &names {
            None => self.shared.broker.topics().into_iter().map(|t| (t.name.clone(), Ok(t))).collect(),
            Some(list) => list
                .iter()
                .map(|n| {
                    let t = if allow_create {
                        self.topic_or_create(n)
                    } else {
                        self.shared.broker.require_topic(n)
                    };
                    (n.clone(), t)
                })
                .collect(),
        };
        let mut w = Out(out);
        if v >= 3 {
            w.i32(0);
        }
        w.array(1);
        w.i32(NODE);
        w.str(&self.shared.config.advertised_host);
        w.i32(self.shared.port as i32);
        if v >= 1 {
            w.nstr(None);
        }
        if v >= 2 {
            w.nstr(Some(CLUSTER_ID));
        }
        if v >= 1 {
            w.i32(NODE);
        }
        w.array(topics.len());
        for (name, t) in &topics {
            match t {
                Ok(t) => {
                    w.i16(E_NONE);
                    w.str(name);
                    if v >= 1 {
                        w.bool(false);
                    }
                    w.array(t.partitions.len());
                    for p in &t.partitions {
                        w.i16(E_NONE);
                        w.i32(p.index as i32);
                        w.i32(NODE);
                        if v >= 7 {
                            w.i32(0);
                        }
                        w.array(1);
                        w.i32(NODE);
                        w.array(1);
                        w.i32(NODE);
                        if v >= 5 {
                            w.array(0);
                        }
                    }
                }
                Err(e) => {
                    w.i16(if e.code == Code::Invalid { E_INVALID_TOPIC } else { E_UNKNOWN_TOPIC });
                    w.str(name);
                    if v >= 1 {
                        w.bool(false);
                    }
                    w.array(0);
                }
            }
            if v >= 8 {
                w.i32(i32::MIN);
            }
        }
        if v >= 8 {
            w.i32(i32::MIN);
        }
        Ok(Reply::Send)
    }

    fn create_topics(&mut self, v: i16, mut r: In<'_>, out: &mut Vec<u8>) -> codec::R<Reply> {
        let n = r.array()?;
        let mut asked = Vec::with_capacity(n);
        for _ in 0..n {
            let name = r.str()?.to_string();
            let partitions = r.i32()?;
            let _replication = r.i16()?;
            let assignments = r.array()?;
            let mut explicit = 0;
            for _ in 0..assignments {
                let _p = r.i32()?;
                let brokers = r.array()?;
                for _ in 0..brokers {
                    r.i32()?;
                }
                explicit += 1;
            }
            let mut config = TopicConfig::default();
            let configs = r.array()?;
            for _ in 0..configs {
                let k = r.str()?.to_string();
                let val = r.nstr()?.map(str::to_string);
                apply_config(&mut config, &k, val.as_deref());
            }
            config.partitions = if partitions > 0 {
                partitions as u32
            } else if explicit > 0 {
                explicit
            } else {
                self.shared.config.default_partitions.max(1)
            };
            asked.push((name, config));
        }
        let _timeout = r.i32()?;
        let validate_only = r.bool()?;
        let mut w = Out(out);
        if v >= 2 {
            w.i32(0);
        }
        w.array(asked.len());
        for (name, config) in asked {
            let (code, message) = if config.partitions == 0 || config.partitions > super::engine::MAX_PARTITIONS {
                (E_INVALID_PARTITIONS, Some(format!("partitions must be 1 to {}", super::engine::MAX_PARTITIONS)))
            } else if self.shared.broker.topic(&name).is_some() {
                (E_TOPIC_EXISTS, Some(format!("topic \"{name}\" already exists")))
            } else if validate_only {
                (E_NONE, None)
            } else {
                match self.shared.broker.create_topic(&name, config, false) {
                    Ok(_) => (E_NONE, None),
                    Err(e) if e.code == Code::Invalid => (E_INVALID_TOPIC, Some(e.message)),
                    Err(e) => (code_of(&e), Some(e.message)),
                }
            };
            w.str(&name);
            w.i16(code);
            if v >= 1 {
                w.nstr(message.as_deref());
            }
        }
        Ok(Reply::Send)
    }

    fn delete_topics(&mut self, v: i16, mut r: In<'_>, out: &mut Vec<u8>) -> codec::R<Reply> {
        let n = r.array()?;
        let mut names = Vec::with_capacity(n);
        for _ in 0..n {
            names.push(r.str()?.to_string());
        }
        let _timeout = r.i32()?;
        let mut w = Out(out);
        if v >= 1 {
            w.i32(0);
        }
        w.array(names.len());
        for name in names {
            let code = match self.shared.broker.delete_topic(&name, false) {
                Ok(_) => E_NONE,
                Err(e) => code_of(&e),
            };
            w.str(&name);
            w.i16(code);
        }
        Ok(Reply::Send)
    }

    fn describe_configs(&mut self, v: i16, mut r: In<'_>, out: &mut Vec<u8>) -> codec::R<Reply> {
        let n = r.array()?;
        let mut resources = Vec::with_capacity(n);
        for _ in 0..n {
            let kind = r.i8()?;
            let name = r.str()?.to_string();
            if let Some(k) = r.narray()? {
                for _ in 0..k {
                    r.str()?;
                }
            }
            resources.push((kind, name));
        }
        let mut w = Out(out);
        w.i32(0);
        w.array(resources.len());
        for (kind, name) in resources {
            // Resource type 2 is a topic; brokers (4) get an empty list.
            let topic = if kind == 2 { self.shared.broker.topic(&name) } else { None };
            let configs: Vec<(&str, String)> = match &topic {
                Some(t) => {
                    let c = t.config();
                    vec![
                        ("cleanup.policy", "delete".into()),
                        ("retention.ms", c.retention_ms.to_string()),
                        ("retention.bytes", c.retention_bytes.to_string()),
                        ("segment.bytes", c.segment_bytes.to_string()),
                        ("max.message.bytes", c.max_batch_bytes.to_string()),
                    ]
                }
                None => Vec::new(),
            };
            let (code, msg) = if kind == 2 && topic.is_none() {
                (E_UNKNOWN_TOPIC, Some("unknown topic"))
            } else {
                (E_NONE, None)
            };
            w.i16(code);
            w.nstr(msg);
            w.i8(kind);
            w.str(&name);
            w.array(configs.len());
            for (k, val) in configs {
                w.str(k);
                w.nstr(Some(&val));
                w.bool(false);
                if v == 0 {
                    w.bool(false);
                } else {
                    w.i8(1); // DYNAMIC_TOPIC_CONFIG
                }
                w.bool(false);
                if v >= 1 {
                    w.array(0);
                }
            }
        }
        Ok(Reply::Send)
    }

    // -----------------------------------------------------------------------
    // Producing
    // -----------------------------------------------------------------------

    fn init_producer_id(&mut self, _v: i16, mut r: In<'_>, out: &mut Vec<u8>) -> codec::R<Reply> {
        let transactional = r.nstr()?;
        let _timeout = r.i32()?;
        let mut w = Out(out);
        w.i32(0);
        if transactional.is_some() {
            w.i16(E_TRANSACTIONAL_ID_AUTH);
            w.i64(-1);
            w.i16(-1);
        } else {
            w.i16(E_NONE);
            w.i64(self.shared.next_producer.fetch_add(1, Ordering::Relaxed));
            w.i16(0);
        }
        Ok(Reply::Send)
    }

    async fn produce(&mut self, v: i16, mut r: In<'_>, out: &mut Vec<u8>) -> codec::R<Reply> {
        let _transactional = r.nstr()?;
        let acks = r.i16()?;
        let timeout = r.i32()?;
        struct PartOut {
            index: i32,
            code: i16,
            base: i64,
            start: i64,
            message: Option<String>,
        }
        let mut results: Vec<(String, Vec<PartOut>)> = Vec::new();
        let mut waits: Vec<(Arc<super::partition::Partition>, u64)> = Vec::new();
        let topics = r.array()?;
        for _ in 0..topics {
            let name = r.str()?.to_string();
            let parts = r.array()?;
            let topic = self.topic_or_create(&name);
            let mut outs = Vec::with_capacity(parts);
            for _ in 0..parts {
                let index = r.i32()?;
                let records = r.nbytes()?.unwrap_or(&[]);
                let mut po = PartOut { index, code: E_NONE, base: -1, start: -1, message: None };
                let t = match &topic {
                    Ok(t) => t,
                    Err(e) => {
                        po.code = code_of(e);
                        outs.push(po);
                        continue;
                    }
                };
                let Ok(part) = t.partition(index.max(0) as u32).cloned() else {
                    po.code = E_UNKNOWN_TOPIC;
                    outs.push(po);
                    continue;
                };
                let max = t.config().max_batch_bytes as usize;
                match codec::decode_record_set(records, max) {
                    Err(b) => {
                        po.code = b.code;
                        po.message = Some(b.message);
                    }
                    Ok(batches) => {
                        for (mut native, info) in batches {
                            if info.transactional || info.control {
                                po.code = E_INVALID_RECORD;
                                po.message = Some("transactions are not supported by this server".into());
                                break;
                            }
                            match self.shared.broker.produce(t, part.index, &mut native, false) {
                                Ok((p, a)) => {
                                    if po.base < 0 {
                                        po.base = a.base_offset as i64;
                                    }
                                    if !a.duplicate && p.durability == Durability::Sync {
                                        waits.push((p, a.next_offset));
                                    }
                                }
                                Err(e) => {
                                    po.code = code_of(&e);
                                    po.message = Some(e.message);
                                    break;
                                }
                            }
                        }
                    }
                }
                po.start = part.log_start() as i64;
                outs.push(po);
            }
            results.push((name, outs));
        }
        if acks == 0 {
            return Ok(Reply::Silent);
        }
        if acks == -1 && !waits.is_empty() {
            // acks=all: as durable as each topic promises.
            let deadline = Duration::from_millis(timeout.clamp(1, 600_000) as u64);
            let waited = tokio::time::timeout(deadline, async {
                for (p, upto) in &waits {
                    let mut rx = p.subscribe_durable();
                    let _ = rx.wait_for(|d| *d >= *upto || p.is_poisoned()).await;
                }
            })
            .await;
            if waited.is_err() || waits.iter().any(|(p, u)| p.durable_offset() < *u) {
                for (_, outs) in results.iter_mut() {
                    for po in outs.iter_mut().filter(|po| po.code == E_NONE) {
                        po.code = 7; // REQUEST_TIMED_OUT
                    }
                }
            }
        }
        let mut w = Out(out);
        w.array(results.len());
        for (name, outs) in &results {
            w.str(name);
            w.array(outs.len());
            for po in outs {
                w.i32(po.index);
                w.i16(po.code);
                w.i64(po.base);
                w.i64(-1); // log_append_time
                if v >= 5 {
                    w.i64(po.start);
                }
                if v >= 8 {
                    w.array(0);
                    w.nstr(po.message.as_deref());
                }
            }
        }
        w.i32(0);
        Ok(Reply::Send)
    }

    // -----------------------------------------------------------------------
    // Fetching
    // -----------------------------------------------------------------------

    async fn fetch(&mut self, v: i16, mut r: In<'_>, out: &mut Vec<u8>) -> codec::R<Reply> {
        let _replica = r.i32()?;
        let max_wait = r.i32()?;
        let min_bytes = r.i32()?;
        let max_bytes = r.i32()?;
        let _isolation = r.i8()?;
        if v >= 7 {
            let _session = r.i32()?;
            let _epoch = r.i32()?;
        }
        struct Want {
            topic: String,
            parts: Vec<(i32, i64, i32)>,
        }
        let n = r.array()?;
        let mut wants = Vec::with_capacity(n);
        for _ in 0..n {
            let topic = r.str()?.to_string();
            let pn = r.array()?;
            let mut parts = Vec::with_capacity(pn);
            for _ in 0..pn {
                let p = r.i32()?;
                if v >= 9 {
                    let _leader_epoch = r.i32()?;
                }
                let offset = r.i64()?;
                if v >= 5 {
                    let _log_start = r.i64()?;
                }
                let max = r.i32()?;
                parts.push((p, offset, max));
            }
            wants.push(Want { topic, parts });
        }
        // forgotten_topics_data (v7+) and rack_id (v11) are not needed.

        let resolve = |w: &Want| self.shared.broker.topic(&w.topic);
        let has_data = || {
            wants.iter().any(|w| {
                resolve(w).is_some_and(|t| {
                    w.parts.iter().any(|(p, off, _)| t.partition(*p as u32).is_ok_and(|x| x.next_offset() as i64 > *off))
                })
            })
        };
        if !has_data() && max_wait > 0 && min_bytes > 0 {
            let mut rxs = Vec::new();
            for w in &wants {
                if let Some(t) = resolve(w) {
                    for (p, _, _) in &w.parts {
                        if let Ok(part) = t.partition(*p as u32) {
                            rxs.push(part.subscribe_appends());
                        }
                    }
                }
            }
            if !rxs.is_empty() {
                let changed = futures::future::select_all(rxs.iter_mut().map(|r| Box::pin(r.changed())));
                let _ = tokio::time::timeout(Duration::from_millis(max_wait.clamp(0, 60_000) as u64), changed).await;
            }
        }

        let mut w = Out(out);
        w.i32(0); // throttle
        if v >= 7 {
            w.i16(E_NONE);
            w.i32(0); // session id: no fetch sessions, every fetch is full
        }
        w.array(wants.len());
        let mut budget = (max_bytes.max(1) as usize).min(64 << 20);
        for want in &wants {
            let topic = resolve(want);
            let w = &mut Out(&mut *w.0);
            w.str(&want.topic);
            w.array(want.parts.len());
            for &(p, offset, pmax) in &want.parts {
                w.i32(p);
                let part = topic.as_ref().and_then(|t| t.partition(p as u32).ok().cloned());
                let Some(part) = part else {
                    w.i16(E_UNKNOWN_TOPIC);
                    w.i64(-1);
                    w.i64(-1);
                    if v >= 5 {
                        w.i64(-1);
                    }
                    w.i32(-1); // aborted transactions: null
                    if v >= 11 {
                        w.i32(-1);
                    }
                    w.i32(-1); // records: null
                    continue;
                };
                let next = part.next_offset() as i64;
                let start = part.log_start() as i64;
                let in_range = offset >= start && offset <= next;
                w.i16(if in_range { E_NONE } else { E_OFFSET_OUT_OF_RANGE });
                w.i64(next);
                w.i64(next); // last stable offset: no transactions
                if v >= 5 {
                    w.i64(start);
                }
                w.i32(-1); // aborted transactions: null
                if v >= 11 {
                    w.i32(-1); // preferred read replica
                }
                let len_at = w.0.len();
                w.i32(0);
                if in_range && offset < next && budget > 0 {
                    let want_bytes = (pmax.max(1) as usize).min(budget);
                    if let Ok(stored) = part.read(offset as u64, want_bytes) {
                        let n = codec::encode_fetch(&stored.bytes, w.0, want_bytes);
                        budget = budget.saturating_sub(n);
                        let n = (w.0.len() - len_at - 4) as i32;
                        w.0[len_at..len_at + 4].copy_from_slice(&n.to_be_bytes());
                    }
                }
            }
        }
        Ok(Reply::Send)
    }

    fn list_offsets(&mut self, v: i16, mut r: In<'_>, out: &mut Vec<u8>) -> codec::R<Reply> {
        let _replica = r.i32()?;
        if v >= 2 {
            let _isolation = r.i8()?;
        }
        let n = r.array()?;
        let mut asked = Vec::with_capacity(n);
        for _ in 0..n {
            let name = r.str()?.to_string();
            let pn = r.array()?;
            let mut parts = Vec::with_capacity(pn);
            for _ in 0..pn {
                let p = r.i32()?;
                if v >= 4 {
                    let _epoch = r.i32()?;
                }
                parts.push((p, r.i64()?));
            }
            asked.push((name, parts));
        }
        let mut w = Out(out);
        if v >= 2 {
            w.i32(0);
        }
        w.array(asked.len());
        for (name, parts) in asked {
            let topic = self.shared.broker.topic(&name);
            w.str(&name);
            w.array(parts.len());
            for (p, at) in parts {
                w.i32(p);
                let part = topic.as_ref().and_then(|t| t.partition(p as u32).ok());
                let found = part.map(|part| match at {
                    -1 => Ok(part.next_offset()),
                    -2 => Ok(part.log_start()),
                    t => part.offset_for_time(t.saturating_mul(1000)),
                });
                match found {
                    Some(Ok(off)) => {
                        w.i16(E_NONE);
                        w.i64(-1);
                        w.i64(off as i64);
                    }
                    Some(Err(e)) => {
                        w.i16(code_of(&e));
                        w.i64(-1);
                        w.i64(-1);
                    }
                    None => {
                        w.i16(E_UNKNOWN_TOPIC);
                        w.i64(-1);
                        w.i64(-1);
                    }
                }
                if v >= 4 {
                    w.i32(0);
                }
            }
        }
        Ok(Reply::Send)
    }

    // -----------------------------------------------------------------------
    // Groups
    // -----------------------------------------------------------------------

    fn find_coordinator(&mut self, v: i16, mut r: In<'_>, out: &mut Vec<u8>) -> codec::R<Reply> {
        let _key = r.str()?;
        if v >= 1 {
            let _key_type = r.i8()?;
        }
        let mut w = Out(out);
        if v >= 1 {
            w.i32(0);
        }
        w.i16(E_NONE);
        if v >= 1 {
            w.nstr(None);
        }
        w.i32(NODE);
        w.str(&self.shared.config.advertised_host);
        w.i32(self.shared.port as i32);
        Ok(Reply::Send)
    }

    async fn join_group(&mut self, v: i16, mut r: In<'_>, out: &mut Vec<u8>, client: &str) -> codec::R<Reply> {
        let group = r.str()?.to_string();
        let session = r.i32()?;
        let rebalance = if v >= 1 { r.i32()? } else { session };
        let member = r.str()?.to_string();
        if v >= 5 {
            let _instance = r.nstr()?;
        }
        let protocol_type = r.str()?.to_string();
        let n = r.array()?;
        let mut protocols = Vec::with_capacity(n);
        for _ in 0..n {
            let name = r.str()?.to_string();
            let meta = r.nbytes()?.unwrap_or(&[]).to_vec();
            protocols.push((name, meta));
        }
        let g = self.shared.groups.get(&group);
        let joined = g
            .join(
                &member,
                client,
                Duration::from_millis(session.max(0) as u64),
                Duration::from_millis(rebalance.max(0) as u64),
                &protocol_type,
                protocols,
                v >= 4,
            )
            .await;
        let mut w = Out(out);
        if v >= 2 {
            w.i32(0);
        }
        w.i16(joined.error);
        w.i32(joined.generation);
        w.str(&joined.protocol);
        w.str(&joined.leader);
        w.str(&joined.member_id);
        w.array(joined.members.len());
        for (id, meta) in &joined.members {
            w.str(id);
            if v >= 5 {
                w.nstr(None);
            }
            w.bytes(meta);
        }
        Ok(Reply::Send)
    }

    async fn sync_group(&mut self, v: i16, mut r: In<'_>, out: &mut Vec<u8>) -> codec::R<Reply> {
        let group = r.str()?.to_string();
        let generation = r.i32()?;
        let member = r.str()?.to_string();
        if v >= 3 {
            let _instance = r.nstr()?;
        }
        let n = r.array()?;
        let mut assignments = Vec::with_capacity(n);
        for _ in 0..n {
            let id = r.str()?.to_string();
            let a = r.nbytes()?.unwrap_or(&[]).to_vec();
            assignments.push((id, a));
        }
        let (code, assignment) = match self.shared.groups.existing(&group) {
            Some(g) => g.sync(&member, generation, assignments).await,
            None => (group::UNKNOWN_MEMBER_ID, Vec::new()),
        };
        let mut w = Out(out);
        if v >= 1 {
            w.i32(0);
        }
        w.i16(code);
        w.bytes(&assignment);
        Ok(Reply::Send)
    }

    fn heartbeat(&mut self, v: i16, mut r: In<'_>, out: &mut Vec<u8>) -> codec::R<Reply> {
        let group = r.str()?.to_string();
        let generation = r.i32()?;
        let member = r.str()?.to_string();
        if v >= 3 {
            let _instance = r.nstr()?;
        }
        let code = match self.shared.groups.existing(&group) {
            Some(g) => g.heartbeat(&member, generation),
            None => group::UNKNOWN_MEMBER_ID,
        };
        let mut w = Out(out);
        if v >= 1 {
            w.i32(0);
        }
        w.i16(code);
        Ok(Reply::Send)
    }

    fn leave_group(&mut self, v: i16, mut r: In<'_>, out: &mut Vec<u8>) -> codec::R<Reply> {
        let group = r.str()?.to_string();
        let mut members = Vec::new();
        if v >= 3 {
            let n = r.array()?;
            for _ in 0..n {
                let id = r.str()?.to_string();
                let _instance = r.nstr()?;
                members.push(id);
            }
        } else {
            members.push(r.str()?.to_string());
        }
        let g = self.shared.groups.existing(&group);
        let codes: Vec<i16> = members
            .iter()
            .map(|m| g.as_ref().map_or(group::UNKNOWN_MEMBER_ID, |g| g.leave(m)))
            .collect();
        let mut w = Out(out);
        if v >= 1 {
            w.i32(0);
        }
        if v >= 3 {
            w.i16(E_NONE);
            w.array(members.len());
            for (m, c) in members.iter().zip(&codes) {
                w.str(m);
                w.nstr(None);
                w.i16(*c);
            }
        } else {
            w.i16(codes.first().copied().unwrap_or(E_NONE));
        }
        Ok(Reply::Send)
    }

    /// The ordered subscription that holds `group`'s offsets on `topic`.
    fn offsets_sub(&self, topic: &Arc<super::Topic>, group: &str, create_at: Option<u64>) -> Option<Arc<super::sub::Subscription>> {
        if let Some(s) = topic.subscription(group) {
            return Some(s);
        }
        let at = create_at?;
        let config = SubConfig {
            mode: SubMode::Ordered,
            start: rasterizeddb_mq::wire::Start::Offset(at),
            ..SubConfig::default()
        };
        self.shared.broker.subscription(topic, group, config).ok()
    }

    fn offset_commit(&mut self, v: i16, mut r: In<'_>, out: &mut Vec<u8>) -> codec::R<Reply> {
        let group = r.str()?.to_string();
        let generation = r.i32()?;
        let member = r.str()?.to_string();
        if (2..=4).contains(&v) {
            let _retention = r.i64()?;
        }
        if v >= 7 {
            let _instance = r.nstr()?;
        }
        // A commit from a group member is checked against its generation; a
        // commit without one (generation -1) is a standalone consumer's.
        let gate = if generation >= 0 {
            self.shared.groups.existing(&group).map_or(group::UNKNOWN_MEMBER_ID, |g| g.check_member(&member, generation))
        } else {
            E_NONE
        };
        let n = r.array()?;
        let mut results = Vec::with_capacity(n);
        for _ in 0..n {
            let name = r.str()?.to_string();
            let pn = r.array()?;
            let topic = self.shared.broker.topic(&name);
            let mut parts = Vec::with_capacity(pn);
            for _ in 0..pn {
                let p = r.i32()?;
                let offset = r.i64()?;
                if v >= 6 {
                    let _epoch = r.i32()?;
                }
                let _meta = r.nstr()?;
                let code = if gate != E_NONE {
                    gate
                } else {
                    match &topic {
                        None => E_UNKNOWN_TOPIC,
                        Some(t) if t.partition(p.max(0) as u32).is_err() => E_UNKNOWN_TOPIC,
                        Some(t) => match self.offsets_sub(t, &group, Some(offset.max(0) as u64)) {
                            Some(s) => match s.commit(p as u32, offset.max(0) as u64) {
                                Ok(()) => E_NONE,
                                Err(e) => code_of(&e),
                            },
                            None => E_UNKNOWN,
                        },
                    }
                };
                parts.push((p, code));
            }
            results.push((name, parts));
        }
        let mut w = Out(out);
        if v >= 3 {
            w.i32(0);
        }
        w.array(results.len());
        for (name, parts) in results {
            w.str(&name);
            w.array(parts.len());
            for (p, code) in parts {
                w.i32(p);
                w.i16(code);
            }
        }
        Ok(Reply::Send)
    }

    fn offset_fetch(&mut self, v: i16, mut r: In<'_>, out: &mut Vec<u8>) -> codec::R<Reply> {
        let group = r.str()?.to_string();
        let asked: Option<Vec<(String, Vec<i32>)>> = match r.narray()? {
            None => None,
            Some(n) => {
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    let name = r.str()?.to_string();
                    let pn = r.array()?;
                    let mut parts = Vec::with_capacity(pn);
                    for _ in 0..pn {
                        parts.push(r.i32()?);
                    }
                    v.push((name, parts));
                }
                Some(v)
            }
        };
        // A null list (v2+) asks for every topic the group has offsets on.
        let asked = asked.unwrap_or_else(|| {
            self.shared
                .broker
                .topics()
                .into_iter()
                .filter(|t| t.subscription(&group).is_some_and(|s| s.config.mode == SubMode::Ordered))
                .map(|t| (t.name.clone(), (0..t.partition_count() as i32).collect()))
                .collect()
        });
        let mut w = Out(out);
        if v >= 3 {
            w.i32(0);
        }
        w.array(asked.len());
        for (name, parts) in asked {
            let topic = self.shared.broker.topic(&name);
            let sub = topic.as_ref().and_then(|t| self.offsets_sub(t, &group, None));
            w.str(&name);
            w.array(parts.len());
            for p in parts {
                w.i32(p);
                let committed = sub.as_ref().and_then(|s| s.committed(p as u32));
                w.i64(committed.map_or(-1, |c| c as i64));
                if v >= 5 {
                    w.i32(-1);
                }
                w.nstr(Some(""));
                w.i16(E_NONE);
            }
        }
        if v >= 2 {
            w.i16(E_NONE);
        }
        Ok(Reply::Send)
    }

    // -----------------------------------------------------------------------
    // SASL
    // -----------------------------------------------------------------------

    fn sasl_handshake(&mut self, v: i16, mut r: In<'_>, out: &mut Vec<u8>) -> codec::R<Reply> {
        let mechanism = r.str()?;
        let ok = mechanism.eq_ignore_ascii_case("PLAIN");
        self.mechanism_ok = ok;
        let mut w = Out(out);
        w.i16(if ok { E_NONE } else { E_UNSUPPORTED_SASL });
        w.array(1);
        w.str("PLAIN");
        if ok && v == 0 {
            self.raw_sasl = true;
        }
        Ok(Reply::Send)
    }

    async fn sasl_authenticate(&mut self, _v: i16, mut r: In<'_>, out: &mut Vec<u8>) -> codec::R<Reply> {
        let token = r.nbytes()?.unwrap_or(&[]).to_vec();
        let (code, message, close) = if !self.mechanism_ok {
            (E_ILLEGAL_SASL_STATE, Some("SaslHandshake first".to_string()), true)
        } else {
            match self.sasl_plain(&token).await {
                Ok(()) => (E_NONE, None, false),
                Err(m) => (E_SASL_FAILED, Some(m), true),
            }
        };
        let mut w = Out(out);
        w.i16(code);
        w.nstr(message.as_deref());
        w.bytes(&[]);
        if _v >= 1 {
            w.i64(0); // session lifetime: none
        }
        Ok(if close { Reply::Close } else { Reply::Send })
    }

    /// `authzid \0 user \0 password`, checked against the database's roles.
    async fn sasl_plain(&mut self, token: &[u8]) -> Result<(), String> {
        let mut parts = token.split(|b| *b == 0);
        let _authz = parts.next();
        let (Some(user), Some(pass)) = (parts.next(), parts.next()) else {
            return Err("malformed PLAIN token".into());
        };
        let user = std::str::from_utf8(user).map_err(|_| "user name is not UTF-8")?.to_string();
        let pass = std::str::from_utf8(pass).map_err(|_| "password is not UTF-8")?.to_string();
        match listen::login(&self.shared.database, &self.shared.main, &user, &pass, "kafka").await {
            Ok(_) => {
                self.authed = true;
                Ok(())
            }
            Err(listen::LoginError::Denied) => Err("authentication failed".into()),
            Err(listen::LoginError::NotAdmitted(e)) => Err(e),
        }
    }
}

enum Reply {
    Send,
    Silent,
    Close,
}

/// A Kafka topic config, where the broker has a matching setting.
fn apply_config(c: &mut TopicConfig, key: &str, value: Option<&str>) {
    let Some(v) = value else { return };
    let num = || v.trim().parse::<i64>().ok();
    match key {
        "retention.ms" => c.retention_ms = num().unwrap_or(c.retention_ms),
        "retention.bytes" => c.retention_bytes = num().unwrap_or(c.retention_bytes),
        "segment.bytes" => c.segment_bytes = num().map_or(c.segment_bytes, |n| n.max(1 << 20) as u64),
        "max.message.bytes" => c.max_batch_bytes = num().map_or(c.max_batch_bytes, |n| n.clamp(1024, u32::MAX as i64) as u32),
        // Not Kafka's own: how the topic is synced (sync, async, memory).
        "rasterizeddb.durability" => {
            if let Some(d) = Durability::parse(v) {
                c.durability = d;
            }
        }
        _ => {}
    }
}
