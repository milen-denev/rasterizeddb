//! A shard's side of sharding (sharding-plan.md §4): its ownership of
//! level-0 block ranges, and (from B1a) admission, classification and the
//! virtual-session endpoint the gateway talks to.
//!
//! Built with the `sharding` feature, dormant until the server starts with
//! `--role shard`.

pub mod classify;
pub mod ddl;
pub mod map_state;
pub mod merge;
pub mod ownership;
#[cfg(feature = "sharding_raft")]
pub mod raft_group;
#[cfg(feature = "sharding_raft")]
pub mod raft_net;
#[cfg(feature = "sharding_raft")]
pub mod raft_store;
pub mod rshard;
pub mod signing;
pub mod start;
