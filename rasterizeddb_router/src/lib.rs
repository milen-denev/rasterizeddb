//! RasterizedDB's routing function and shard map.
//!
//! One copy of route_v1, shared by everything that must agree on where a row
//! lives: the engine's hash partitions (`rasterizeddb_core` re-exports this
//! through `core::hash` and `core::partition::map`), the native client's
//! shard router, and the gateway.
//!
//! - [`xxh64`]: the in-tree XXH64, pinned to the C reference.
//! - [`key`]: families, frames, level hashes, the point layout, and the
//!   client-side [`key::KeyValue`] normaliser.
//! - [`map`]: point ranges and a partition's range map.
//! - [`shard_map`]: level-0 blocks to shards, with its text and binary codecs.
//! - [`balance`]: the auto-balancing planner over a map and measured loads.
//! - [`rshard`]: the gateway↔shard protocol's messages and codec.
//! - [`literals`]: a simple query's shape and literal slots, for routing.
//!
//! The crate has no dependencies, so every byte of a route is computed here.

#![forbid(unsafe_code)]

pub mod balance;
pub mod key;
pub mod literals;
pub mod map;
pub mod rshard;
pub mod shard_map;
pub mod xxh64;

#[cfg(test)]
mod test_buffers;

pub use key::{KeyFamily, KeyValue, ROUTE_V1};
pub use map::{PointRange, RangeMap};
pub use shard_map::{ShardInfo, ShardMap};
