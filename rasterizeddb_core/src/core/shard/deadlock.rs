//! Deadlock detection across shards (sharding-plan.md B4.2).
//!
//! Each shard sees only its own waits, so a cycle that passes through two
//! shards (a transaction waits on shard A for one that waits on shard B for
//! the first) is no shard's to find. The gateway tags every part of a
//! multi-shard transaction with one id (`SET LOCAL rdb.global_txn = 'g'`);
//! each shard reports the waits between tagged transactions, following a
//! chain through untagged local ones ([`waits`]); the gateway's detector
//! puts every shard's together, finds the cycles, and asks the shards to end
//! one member ([`abort`]), whose waiting statement then fails with 40P01.
//!
//! Without two-phase commit no part is tagged, and a cross-shard wait is
//! bounded by the lock timeout the gateway sets instead.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Weak};

use dashmap::DashMap;

use crate::core::cancel::{CancelReason, SessionCancel};
use crate::core::database::catalog::DatabaseCatalog;
use crate::core::mvcc::{TxId, TxOutcome};

/// (database id, transaction owner) → its global id.
static TAGS: std::sync::LazyLock<DashMap<(u64, TxId), String>> = std::sync::LazyLock::new(DashMap::new);
/// Global id → the session running it here.
static SESSIONS: std::sync::LazyLock<DashMap<String, Weak<SessionCancel>>> = std::sync::LazyLock::new(DashMap::new);

/// Tags transaction `tx` of database `db` with global id `gid`, run by
/// `session`.
/// `session` is what [`abort`] stops; a session with none (an embedding)
/// is reported in waits but cannot be ended from here.
pub fn tag(db: &crate::core::database::Database, tx: TxId, gid: &str, session: Option<&Arc<SessionCancel>>) {
    let owner = db.row_locks.owner_of(tx);
    TAGS.insert((db.id, owner), gid.to_string());
    if let Some(session) = session {
        SESSIONS.insert(gid.to_string(), Arc::downgrade(session));
    }
}

/// The waits between tagged transactions on this shard, as (waiter's
/// global id, holder's global id): a direct wait, or one through untagged
/// local transactions. Tags of ended transactions are dropped on the way.
pub fn waits(catalog: &DatabaseCatalog) -> Vec<(String, String)> {
    let mut out: HashSet<(String, String)> = HashSet::new();
    for name in catalog.names() {
        let Some(db) = catalog.get(&name) else { continue };
        TAGS.retain(|(d, tx), _| *d != db.id || db.transactions.outcome(*tx) == TxOutcome::InProgress);
        let graph: HashMap<TxId, Vec<TxId>> = db.row_locks.wait_graph().into_iter().map(|(w, b)| (w, b.to_vec())).collect();
        if graph.is_empty() {
            continue;
        }
        let gid_of = |tx: TxId| TAGS.get(&(db.id, tx)).map(|g| g.clone());
        for (&waiter, _) in &graph {
            let Some(from) = gid_of(waiter) else { continue };
            let mut seen: HashSet<TxId> = HashSet::from([waiter]);
            let mut queue: VecDeque<TxId> = graph[&waiter].iter().copied().collect();
            while let Some(t) = queue.pop_front() {
                if !seen.insert(t) {
                    continue;
                }
                match gid_of(t) {
                    Some(to) if to != from => {
                        out.insert((from.clone(), to));
                    }
                    Some(_) => {}
                    None => queue.extend(graph.get(&t).into_iter().flatten().copied()),
                }
            }
        }
    }
    SESSIONS.retain(|g, s| s.strong_count() > 0 && TAGS.iter().any(|e| e.value() == g));
    out.into_iter().collect()
}

/// Ends global transaction `gid`'s waiting statement here, if it runs here:
/// it fails with 40P01 and the client rolls the transaction back.
pub fn abort(gid: &str) -> bool {
    SESSIONS.get(gid).and_then(|s| s.upgrade()).is_some_and(|s| s.flag_current(CancelReason::Deadlock))
}

pub use rasterizeddb_router::deadlock::victims;
