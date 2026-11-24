//! The cluster map this shard serves under, for `SHOW SHARDS`,
//! `rdb_shard_of` and (from B1a) the gateway's map fetch.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use rasterizeddb_router::ShardMap;

/// The map and this node's id in it.
#[derive(Clone, Debug)]
pub struct Served {
    pub map: ShardMap,
    pub shard_id: u32,
}

static SERVED: ArcSwapOption<Served> = ArcSwapOption::const_empty();

pub fn install(map: ShardMap, shard_id: u32) {
    SERVED.store(Some(Arc::new(Served { map, shard_id })));
}

/// The map served, or `None` on a node that is not a shard.
pub fn current() -> Option<Arc<Served>> {
    SERVED.load_full()
}
