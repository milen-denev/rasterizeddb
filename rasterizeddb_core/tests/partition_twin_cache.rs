//! The twin-table model test (`partition_twin.rs`) with the query cache on:
//! every configuration, few-open ones included, so a cached answer that
//! outlives a partition's close, or any other instance change, diverges
//! from the plain twin. Its own binary because the cache is switched on per
//! process; `PART_TWIN_SEED` and `PART_TWIN_OPS` apply as there.

#[path = "partition_twin.rs"]
mod twin;
