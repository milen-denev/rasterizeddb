//! Importing a live PostgreSQL database into RasterizedDB.
//!
//! One direction only, and one shot: this reads a PostgreSQL server and builds
//! a new RasterizedDB database from what it finds. It does not write back, it
//! does not stay in sync, and it will not import into a database that already
//! holds tables.
//!
//! The work is in four stages, each its own module:
//!
//! 1. [`pg_client`] connects and authenticates — a small, hand-rolled client
//!    for version 3 of the PostgreSQL frontend protocol.
//! 2. [`source`] asks the system catalogues what the database contains:
//!    tables, columns, types, nullability, defaults, primary keys, unique
//!    constraints and foreign keys.
//! 3. [`types`] decides what each PostgreSQL type becomes here, and converts
//!    each value. Everything a type cannot carry faithfully is refused rather
//!    than rounded, because a migration that quietly changes a number is worse
//!    than one that stops and says which column it could not carry.
//! 4. [`run`] creates the target database, replays the schema as DDL through
//!    the ordinary executor, and streams every table's rows in with `COPY`.

pub mod copy;
pub mod pg_client;
pub mod plan;
pub mod run;
pub mod source;
pub mod sql;
pub mod types;
