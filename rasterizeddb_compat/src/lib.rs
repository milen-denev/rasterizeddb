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

pub mod entities;
pub mod probes;
pub mod schema;
pub mod server;

pub use server::TestServer;
