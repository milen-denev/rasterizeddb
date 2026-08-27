//! A storage backend that keeps a database in a key/value store.
//!
//! Behind the off-by-default `kv_storage` feature; the S3 backing store is
//! behind `kv_storage_s3` on top of it.
//!
//! See [`provider`] for the design and [`store`] for what a backing store has
//! to provide. `docs/kv-storage.md` has the operational side.

pub mod cache;
pub mod layout;
pub mod overlay;
pub mod provider;
pub mod store;

#[cfg(all(feature = "kv_storage_s3", feature = "s3_backup"))]
pub mod s3_store;

#[cfg(test)]
mod tests;

pub use provider::{KvConfig, KvFamily, KvStorageProvider};
pub use store::{KvStore, LocalDirStore, MemoryKvStore, ObjectInfo};

#[cfg(all(feature = "kv_storage_s3", feature = "s3_backup"))]
pub use s3_store::S3KvStore;
