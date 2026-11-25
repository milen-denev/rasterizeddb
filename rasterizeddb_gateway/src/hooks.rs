//! The hooks a separately built crate can install (sharding-plan.md §10,
//! D12): the gateway itself is GPL and carries only the trait and the points
//! that enforce its answer.
//!
//! **Per-user shards (G11).** A [`UserRouting`] maps a login to the shard
//! its whole session is locked to. This crate has no implementation beyond
//! [`NoUserRouting`]: the mapping (its catalogue table, `ALTER USER … ROUTE
//! SHARD`, moves) belongs to the dual-licensed `rasterizeddb_gateway_enterprise`
//! crate, whose binary calls [`crate::run_with`] with its own hooks.
//!
//! What the gateway enforces for a pinned session, whoever pinned it:
//! - every statement goes to the pinned shard: no template lookup, no hash;
//! - `SET rdb.route_key`, `SET LOCAL rdb.route_key` and a native `route=`
//!   option are refused (`42501`);
//! - a statement without a key is never fanned out (`unrouted` is `refuse`
//!   for it, with no override);
//! - each virtual session opens with `pinned = 1`, so the shard checks its
//!   own placement list and refuses a stale mapping (`Moved`).

use std::sync::Arc;

/// A login's place.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pin {
    /// Not mapped: statement routing.
    None,
    /// Locked to this shard for the session's life.
    Shard(u32),
    /// Refused at login with this message (`28000`).
    Refuse(String),
}

/// Per-user shards (module docs). Called once per login, after the
/// client's proof is verified; never per statement.
pub trait UserRouting: Send + Sync {
    fn pin(&self, database: &str, role: &str) -> Pin;
    /// The role epoch this answer was made under; a change applies at the
    /// role's next login.
    fn epoch(&self) -> u64 {
        0
    }
}

/// Nobody is pinned.
pub struct NoUserRouting;

impl UserRouting for NoUserRouting {
    fn pin(&self, _database: &str, _role: &str) -> Pin {
        Pin::None
    }
}

/// Who a session is, for an [`Observer`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientInfo {
    /// The session's backend id (its cancel key's pid).
    pub session: i32,
    pub database: String,
    pub role: String,
    /// The client's address, `ip:port`.
    pub address: String,
    /// Its `application_name` startup parameter.
    pub application: String,
}

/// One statement's measures, as the gateway took them: from its first
/// message read to its last reply byte written.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StatementStats {
    pub latency_us: u64,
    /// The first error's SQLSTATE, if the statement failed.
    pub sqlstate: Option<String>,
    /// Bytes read from the client and written to it.
    pub client_bytes_in: u64,
    pub client_bytes_out: u64,
    /// Calls to shards, and the bytes exchanged with them: as framed
    /// (`plain`) and as sent on the wire (`wire`, after compression when
    /// the link compresses).
    pub shard_calls: u32,
    pub shard_bytes_out_plain: u64,
    pub shard_bytes_out_wire: u64,
    pub shard_bytes_in_plain: u64,
    pub shard_bytes_in_wire: u64,
}

/// A table an observer answers a statement with: column names and rows.
pub type Answer = Result<(Vec<String>, Vec<Vec<Option<String>>>), String>;

/// Sees every session and statement (per-client statistics, audit). Called
/// on the gateway's cores, after the reply is written: keep it short.
pub trait Observer: Send + Sync {
    fn session_opened(&self, _client: &ClientInfo) {}
    fn statement(&self, client: &ClientInfo, stats: &StatementStats);
    fn session_closed(&self, _client: &ClientInfo) {}
    /// A statement the observer answers itself (`SHOW CLIENT STATS`), asked
    /// by `client`; `None` for every other.
    fn answer<'a>(&'a self, _client: &'a ClientInfo, _sql: &'a str) -> Option<std::pin::Pin<Box<dyn std::future::Future<Output = Answer> + Send + 'a>>> {
        None
    }
    /// Lines appended to the metrics endpoint (Prometheus text).
    fn metrics(&self) -> String {
        String::new()
    }
}

/// Everything a hook crate can install.
#[derive(Clone)]
pub struct Hooks {
    pub user_routing: Arc<dyn UserRouting>,
    /// False for [`NoUserRouting`]: a configuration asking for per-user
    /// shards from a build without an implementation is refused at start.
    pub has_user_routing: bool,
    /// `None`: nothing is measured per statement beyond the counters.
    pub observer: Option<Arc<dyn Observer>>,
    /// Paths of the configuration API a hook crate serves.
    #[cfg(feature = "config_api")]
    pub api: Option<Arc<dyn crate::api::ApiExtension>>,
}

impl Default for Hooks {
    fn default() -> Self {
        Hooks {
            user_routing: Arc::new(NoUserRouting),
            has_user_routing: false,
            observer: None,
            #[cfg(feature = "config_api")]
            api: None,
        }
    }
}
