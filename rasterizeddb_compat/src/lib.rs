//! PostgreSQL feature-compatibility harness for RasterizedDB.
//!
//! The schema mirrors a real production application (`pms`) so the gap between
//! what Postgres accepts and what RasterizedDB accepts is measured against
//! traffic an ORM actually generates, rather than against hand-picked queries.
//! Two client surfaces are exercised over the same server:
//!
//! * [`entities`] — sea-orm models, driving the extended query protocol with
//!   bound parameters, exactly as the application does.
//! * [`probes`] — raw SQL text, isolating one language feature per probe so a
//!   failure names the missing feature instead of a failing ORM call.
//!
//! JSON columns and index DDL are deliberately absent: RasterizedDB has its own
//! leaf-structure indexing and no JSON type is planned.
//!
//! # Why `AssertSqlSafe` is everywhere here
//!
//! sqlx 0.9 accepts only `&'static str` in `query()`, so any statement built at
//! runtime has to be wrapped and thereby audited. Sending statements built at
//! runtime is precisely what this crate is *for*: a probe is a SQL string, and
//! a sweep generates one per case. Every wrapped statement is composed inside
//! this crate from its own fixtures — no value crosses in from a user, a
//! network peer, or a file — so the assertion is sound, and it is a harness
//! rather than a server in any case.

pub mod entities;
pub mod probes;
pub mod schema;
pub mod wide_types;
pub mod server;

pub use server::TestServer;
