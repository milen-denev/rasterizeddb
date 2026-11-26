//! The message broker's batch format and native wire protocol.
//!
//! The engine (`rasterizeddb_core::core::broker`) and the client
//! (`rasterizeddb_client::broker`) both link this crate, so there is exactly
//! one copy of the layout. The design is in docs/broker.md:
//!
//! - [`batch`]: the format of a batch (§2.1). A native producer builds a batch
//!   in its on-disk layout, the server checks it and stamps two fields, and a
//!   consumer reads the stored bytes.
//! - [`wire`]: the frames of the `rdbmq/1` protocol (§6).
//! - [`hash`]: the partitioner a keyed produce uses on both sides.

pub mod batch;
pub mod hash;
pub mod wire;

pub use batch::{BatchBuilder, BatchError, BatchView, RecordView};
