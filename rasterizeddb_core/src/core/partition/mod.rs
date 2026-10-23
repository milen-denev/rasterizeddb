//! Hash partitioning: the routing hash and the range map (design phase 1,
//! `docs/partitioning.md` §1). Pure and unwired: nothing in the engine calls
//! this yet.
//!
//! API summary:
//!
//! - [`xxh64::xxh64`]`(input, seed) -> u64`: in-tree XXH64, pinned to the C
//!   reference.
//! - [`key::family_of`]`(&DbType) -> Result<KeyFamily, String>`: the §1.4
//!   allowlist (integer `0x01`, uuid `0x02`, text `0x03`), refusing all else
//!   with its reason. It takes the declared type, never `physical()` or a
//!   `RowWrite` column type (both turn BYTEA, JSONB, ARRAY, ranges and more
//!   into `STRING`). `STRING` aliases (CITEXT, BPCHAR, NAME, CHAR(n > 1)) must
//!   be refused by the caller from the declared type text, and a shredded JSON
//!   column, declared `UUID`, by its `schema.json_columns` entry.
//! - [`key::route_bytes`]`(&DbType, column) -> Result<Vec<u8>, String>`: a
//!   value's normalised bytes, for insert and query alike.
//! - [`key::frame`], [`key::level_hash`]: `f ‖ len:u32 LE ‖ b`, hashed with
//!   seed `LEVEL_SEED + level`.
//! - [`KeySpec`]: `from_schema(&TableSchema, &[name])`, which takes declared
//!   types and refuses JSON columns itself (prefer it), or
//!   `new(&[(write_order, DbType)])` (1 to 3 columns),
//!   `levels()`, `arity()`, `widths()`, `point_of(&[&[u8]]) -> u64`, and
//!   `prefix_range(&[&[u8]]) -> PointRange` for a fixed leading prefix
//!   (the whole space for none, a 2¹⁶ block for the tenant, one point for all).
//! - [`PointRange`]`{ lo: u64, hi: u128 }`: half-open, `hi` up to 2⁶⁴.
//! - [`RangeMap`]: `new(entries)` and `initial(n)` (both validated),
//!   `validate()`, `entries()`, `pid_of(point)`, `range_of(pid)`,
//!   `pids_overlapping(lo, hi) -> Vec<u32>` (sorted by pid).
//!
//! - [`route::route`]`(&KeySpec, Option<&MaskExpr>, constant_false) -> Route`
//!   (§4.2): `Zero` only for a constant-false clause, else `All` or merged
//!   point intervals, always a superset; [`Route::pids`] maps it to partitions.
//! - [`log`] (§3.2): the map log's record codec and replay, no IO.
//!   [`log::encode`]`(&Record) -> Result<Vec<u8>, String>` frames a record;
//!   [`log::replay`]`(&[u8]) -> Result<Replay, String>` stops at a torn tail
//!   and refuses anything else it cannot apply; [`log::MapState::apply`]
//!   keeps a replayed state current.
//! - [`files`] (§3.1, §3.2, §12): file names ([`files::data_stem`],
//!   [`files::parse_name`]), and, crate-private, exclusive creation with one
//!   directory fsync (`create_partition_files`, `create_parent_files`), the
//!   never-creating `require_existing` (I9), `remove_all`, and `MapLogFile`,
//!   whose appends carry a WAL record at append time, leave nothing behind
//!   when they fail, and whose open keeps a cut torn tail as `.torn`
//!   evidence. The one module here that does IO, and its errors are
//!   `io::Error`s.
//!
//! - [`guard::RangeGuard`] (§4.5, I4): `check_insert(&RowWrite)` errs when a
//!   row's point lies outside the partition's range; `check_update(before,
//!   after)` also refuses any change of point (0A000). Installed on a
//!   partition `Table` by `Table::open_partition`.
//!
//! - [`table::PartitionedTable`] (§1.1, §3, §4.3, §4.4): the logical table.
//!   `create_files` writes a new table's map log and ids file; `open` replays
//!   the log and opens every materialized partition, or leaves the table
//!   broken (every data call returns why); `route_where(clause) -> Route`;
//!   `materialized(epoch, route)` skips unmaterialized partitions by proof;
//!   `insert_row_in_txn` routes a row and materializes its partition on first
//!   write (§3.2's order). [`table::check_declared_cap`] is the 2,048 declared
//!   partitions per database refusal, over `table::declared_in`, which
//!   refuses while any table's count is unknown; [`table::DEFAULT_PARTITIONS`]
//!   is 64. Pools with backup mirrors are refused, read from the pool at
//!   create, open and materialize.
//!
//! Errors are `String`s, like the rest of the engine's DDL paths, except in
//! `files` and `table`, which do IO and return `io::Error`s.

pub mod files;
pub mod guard;
pub mod key;
pub mod log;
pub mod map;
pub mod route;
pub mod table;
#[cfg(test)]
mod table_tests;

pub use key::{KeyFamily, KeyLevel, KeySpec};
pub use map::{PointRange, RangeMap};
pub use route::{Route, route};
