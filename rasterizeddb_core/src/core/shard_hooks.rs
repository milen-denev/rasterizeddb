//! The ownership hooks on every path that stamps a row (sharding-plan.md
//! B0.7, enforced from B1a).
//!
//! A shard must never stamp a row outside the ranges it owns, or is taking in
//! by its own move: the gateway and the client are never trusted for that.
//! Each stamping path calls [`check_stamp`] with the range of the partition it
//! writes (`None` for a plain table) before it writes anything:
//! - an insert, after the partition's own range check;
//! - `insert_version`, a rewrite's copy of a version;
//! - a delete's and an update's statement entry, before the scan;
//! - a reshape's `append` into its targets.
//!
//! Without the `sharding` feature this is an inline `Ok(())`, so the hot paths
//! carry nothing. With it, and no shard role configured, it is one relaxed
//! load (`core::shard::ownership`).

use crate::core::partition::map::PointRange;

/// Which path is stamping, for the refusal's message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StampOp {
    Insert,
    InsertVersion,
    Update,
    Delete,
    ReshapeAppend,
}

/// `Err` when this node may not stamp rows in `range` (module docs).
#[cfg(not(feature = "sharding"))]
#[inline(always)]
pub fn check_stamp(_range: Option<PointRange>, _op: StampOp) -> std::io::Result<()> {
    Ok(())
}

#[cfg(feature = "sharding")]
#[inline]
pub fn check_stamp(range: Option<PointRange>, op: StampOp) -> std::io::Result<()> {
    crate::core::shard::ownership::check_stamp(range, op)
}
