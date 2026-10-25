#[cfg(feature = "s3_backup")]
pub mod backup;
pub mod bulk_load;
pub mod cancel;
pub mod notice;
/// Hash partitioning's routing hash and range map; not wired in yet.
pub mod partition;
pub mod sync_audit;
/// Crash images, failpoints and the index twin suites through restarts
/// and power cuts (persistent-indexes design §11). Public only under the
/// test-only `index_store_failpoints` feature, for the compat suites.
#[cfg(any(test, feature = "index_store_failpoints"))]
#[doc(hidden)]
pub mod power_loss;
#[cfg(test)]
mod downgrade_tests;
pub mod database;
pub mod db_type;
pub mod filter;
/// Frozen hashes for anything persisted (in-tree XXH64; rapidhash for GIN items).
pub mod hash;
pub mod helpers;
pub mod introspect;
pub mod jsonb;
pub mod log_writer;
/// One-way import of a live PostgreSQL database, behind the `pg_import`
/// feature.
#[cfg(feature = "pg_import")]
pub mod migrate;
pub mod mock_helpers;
pub mod mock_table;
pub mod mvcc;
pub mod pg_ops;
pub mod pg_types;
pub mod processor;
/// What ran recently and what it cost, behind the `query_stats` feature.
#[cfg(feature = "query_stats")]
pub mod query_stats;
/// Cutting statement text at a secret, for every surface that keeps one.
pub mod redact;
pub mod row;
pub mod rql;
pub mod similar_to;
/// The shared quote-aware scanner, and escape/dollar-string normalisation.
pub mod sql_scan;
pub mod index;
/// Persistent indexes; phase 0 has only the write gate's proofs.
pub mod index_store;
#[cfg(feature = "kv_cache")]
pub mod kv_cache;
/// Several storage directories for one database, behind the `storage_pools`
/// feature.
#[cfg(feature = "storage_pools")]
pub mod storage;
pub mod storage_providers;
pub mod support_types;
pub mod text_search;
/// A fixed-record file torn by a power cut is cut back before the log replays.
pub mod torn_tail;
pub mod tokenizer;
pub mod vacuum;
pub mod value_codec;
pub mod wal;
