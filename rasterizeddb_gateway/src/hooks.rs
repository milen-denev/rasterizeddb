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

/// Everything a hook crate can install.
#[derive(Clone)]
pub struct Hooks {
    pub user_routing: Arc<dyn UserRouting>,
    /// False for [`NoUserRouting`]: a configuration asking for per-user
    /// shards from a build without an implementation is refused at start.
    pub has_user_routing: bool,
}

impl Default for Hooks {
    fn default() -> Self {
        Hooks { user_routing: Arc::new(NoUserRouting), has_user_routing: false }
    }
}
