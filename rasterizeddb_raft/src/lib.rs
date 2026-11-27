//! RasterizedDB's Raft (sharding-plan.md B3, decision D3: our own).
//!
//! A deterministic consensus core in the etcd-raft shape: [`RaftNode`]
//! takes messages and ticks and returns, in a [`Ready`], what its caller
//! must persist, send and apply. It does no I/O and reads no clock, so a
//! seeded simulation can drive several nodes through every interleaving a
//! network and its disks produce, and check the safety properties at each
//! step.
//!
//! Built in: leader election with pre-vote and leader stickiness, log
//! replication with flow control, check-quorum, ReadIndex reads, snapshots,
//! single-server membership changes with learners, leadership transfer.
//!
//! The codec for messages and entries ([`codec`]) is the wire and disk
//! format of the shard's Raft transport and log store.

#![forbid(unsafe_code)]

pub mod codec;
pub mod log;
pub mod node;
pub mod types;

pub use log::RaftLog;
pub use node::{RaftNode, Ready, Timing};
pub use types::*;
