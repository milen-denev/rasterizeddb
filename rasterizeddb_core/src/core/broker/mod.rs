//! A message broker inside the database: topics, queues, subscriptions and
//! exchanges, served on ports of their own (docs/broker.md).
//!
//! - **The native protocol** (`rdbmq/1`) on `--broker-port` /
//!   `--broker-socket`: the fastest way in. A native producer sends batches in
//!   their stored layout ([`rasterizeddb_mq::batch`]), and a consumer receives
//!   the stored bytes. See [`net`].
//! - **SQL** on the PostgreSQL port: `BROKER CREATE TOPIC orders PARTITIONS 8`,
//!   `BROKER PUBLISH orders 'hello'`, `BROKER READ orders FROM 0`. See [`sql`].
//! - Kafka, AMQP 0-9-1 and MQTT adapters on ports of their own, each behind
//!   its own feature.
//!
//! # What it is built on
//!
//! - [`partition`]: an append-only log of batches. Appends copy into an
//!   in-memory [`chunk`] under a mutex held for the copy, and the
//!   [`flush`]er writes and group-syncs them.
//! - [`segment`]: the files, with a sparse index and torn-tail recovery.
//! - [`flog`]: framed logs with compaction by generation, on the database's
//!   `framed_log`. These hold the metadata (topics, subscriptions, exchanges,
//!   bindings) and each subscription's state.
//! - [`sub`]: ordered, shared and broadcast subscriptions, and their delivery.
//! - [`exchange`] and [`trie`]: routing.
//!
//! Nothing is opened until the first use. A database that never touches the
//! broker carries one empty `OnceLock`.

#[cfg(feature = "broker_amqp")]
pub mod amqp;
pub mod chunk;
pub mod engine;
pub mod exchange;
pub mod flog;
pub mod flush;
#[cfg(feature = "broker_kafka")]
pub mod kafka;
#[cfg(feature = "broker_mqtt")]
pub mod mqtt;
pub mod net;
pub mod partition;
pub mod segment;
pub mod sql;
pub mod sub;
pub mod trie;
#[cfg(test)]
mod tests;

use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

pub use engine::{Broker, Topic};
pub use rasterizeddb_mq::wire::Code;

/// Where a database keeps its broker, beside its tables.
pub const DIR: &str = "broker";

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

static FLUSH_MS: AtomicU64 = AtomicU64::new(10);
static DDL: AtomicU8 = AtomicU8::new(DdlPolicy::Any as u8);

/// Who may create, alter and delete topics, queues, exchanges and bindings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum DdlPolicy {
    /// Any role that logged in, as RabbitMQ's default permissions allow.
    Any = 0,
    /// Superusers only.
    Superuser = 1,
}

impl DdlPolicy {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.trim().to_ascii_lowercase().as_str() {
            "any" | "all" => DdlPolicy::Any,
            "superuser" | "admin" => DdlPolicy::Superuser,
            _ => return None,
        })
    }
}

/// How often `async` topics are synced, in milliseconds.
pub fn flush_ms() -> u64 {
    FLUSH_MS.load(Ordering::Relaxed)
}

pub fn set_flush_ms(ms: u64) {
    FLUSH_MS.store(ms.max(1), Ordering::Relaxed);
}

pub fn ddl_policy() -> DdlPolicy {
    if DDL.load(Ordering::Relaxed) == DdlPolicy::Superuser as u8 { DdlPolicy::Superuser } else { DdlPolicy::Any }
}

pub fn set_ddl_policy(p: DdlPolicy) {
    DDL.store(p as u8, Ordering::Relaxed);
}

/// Microseconds since the epoch.
#[inline]
pub fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as i64)
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// A failure with the wire code a client is told.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerError {
    pub code: Code,
    pub message: String,
}

impl BrokerError {
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        BrokerError { code, message: message.into() }
    }
    pub fn io(e: std::io::Error) -> Self {
        BrokerError::new(Code::Storage, e.to_string())
    }
    pub fn invalid(message: impl Into<String>) -> Self {
        BrokerError::new(Code::Invalid, message)
    }
    pub fn no_topic(name: &str) -> Self {
        BrokerError::new(Code::NoTopic, format!("topic \"{name}\" does not exist"))
    }
    /// The SQLSTATE a `BROKER` statement fails with.
    pub fn sqlstate(&self) -> &'static str {
        match self.code {
            Code::NoTopic | Code::NoSubscription | Code::NoExchange | Code::NoPartition => "42704",
            Code::TopicExists => "42710",
            Code::Permission | Code::AuthRequired | Code::AuthFailed => "42501",
            Code::Invalid | Code::Mismatch | Code::BadBatch | Code::Protocol => "22023",
            Code::TooLarge | Code::Full => "54000",
            Code::Unsupported => "0A000",
            Code::Storage => "58030",
            Code::ShuttingDown => "57P01",
            _ => "XX000",
        }
    }
}

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for BrokerError {}

impl From<rasterizeddb_mq::BatchError> for BrokerError {
    fn from(e: rasterizeddb_mq::BatchError) -> Self {
        BrokerError::new(Code::BadBatch, e.to_string())
    }
}

// ---------------------------------------------------------------------------
// The database's handle
// ---------------------------------------------------------------------------

/// Every broker opened in the process, for the flush at shutdown.
static ALL: parking_lot::Mutex<Vec<std::sync::Weak<Broker>>> = parking_lot::Mutex::new(Vec::new());

/// Writes and syncs every open broker's pending appends and subscription
/// state, and stops their threads: the last step of a clean shutdown.
pub fn shutdown_all() {
    let brokers: Vec<Arc<Broker>> = ALL.lock().iter().filter_map(std::sync::Weak::upgrade).collect();
    for b in brokers {
        b.shutdown();
        log::info!("broker: {} flushed on shutdown", b.db_name);
    }
}

/// The database's broker, opened on first use. Cloning is one `Arc`.
#[derive(Clone)]
pub struct BrokerHandle {
    inner: Arc<HandleInner>,
}

struct HandleInner {
    /// The database's directory; `None` for an in-memory database, whose
    /// topics are all `memory`.
    location: Option<PathBuf>,
    name: String,
    broker: OnceLock<Result<Arc<Broker>, BrokerError>>,
    open_lock: parking_lot::Mutex<()>,
}

impl BrokerHandle {
    pub fn new(location: Option<&str>, name: &str) -> Self {
        BrokerHandle {
            inner: Arc::new(HandleInner {
                location: location.map(PathBuf::from),
                name: name.to_string(),
                broker: OnceLock::new(),
                open_lock: parking_lot::Mutex::new(()),
            }),
        }
    }

    /// The broker, opening it (and recovering its logs) on the first call.
    pub fn get(&self) -> Result<Arc<Broker>, BrokerError> {
        if let Some(r) = self.inner.broker.get() {
            return r.clone();
        }
        let _g = self.inner.open_lock.lock();
        if let Some(r) = self.inner.broker.get() {
            return r.clone();
        }
        let dir = self.inner.location.as_ref().map(|l| l.join(DIR));
        let r = Broker::open(dir.as_deref(), &self.inner.name).map_err(|e| {
            log::error!("broker: could not open {}: {e}", dir.as_deref().map_or("memory".into(), |d| d.display().to_string()));
            e
        });
        if let Ok(b) = &r {
            let mut all = ALL.lock();
            all.retain(|w| w.strong_count() > 0);
            all.push(Arc::downgrade(b));
        }
        let _ = self.inner.broker.set(r.clone());
        r
    }

    /// The broker if something has already opened it.
    pub fn existing(&self) -> Option<Arc<Broker>> {
        self.inner.broker.get().and_then(|r| r.as_ref().ok().cloned())
    }

    /// Flushes and stops the broker, at shutdown.
    pub fn shutdown(&self) {
        if let Some(b) = self.existing() {
            b.shutdown();
        }
    }
}
