//! What a table name resolves to (`docs/partitioning.md` §3.3): a plain
//! table, or a partitioned table whose shell has a slot under the logical
//! name and whose rows live in its partitions.
//!
//! A name-keyed path asks [`Database::relation`] (in `slots.rs`, built from
//! the slot) before it touches a slot, files or `seed_in`: a shell found by
//! name has no data, and a path that rebuilt the entry from `<name>*` files
//! would replace the partitioned table with an empty plain one.

use std::io;

use rclite::Arc;

use super::Database;
use crate::core::partition::table::PartitionedTable;
use crate::core::row::schema::TableSchema;
use crate::core::row::table::Table;
use crate::core::storage_providers::backend::DbStorageProvider;

#[derive(Clone)]
pub enum Relation {
    Plain(Arc<Table<DbStorageProvider>>),
    Partitioned(Arc<PartitionedTable>),
}

impl Relation {
    /// The declared schema: a plain table's own, a partitioned table's shell's.
    pub fn schema(&self) -> &TableSchema {
        match self {
            Relation::Plain(table) => &table.schema,
            Relation::Partitioned(table) => &table.shell().schema,
        }
    }
}

impl Database {
    /// Errs when `name` is a partitioned table, for the name-keyed paths
    /// that work from `<name>*` files or rebuild the map entry with
    /// `Table::new`: on a partitioned name they would find no files and put
    /// an empty plain table in its place. `what` names the operation in the
    /// refusal. A shell with no partitioned table behind it errs too, with
    /// `Table::data`'s refusal. Called before the map entry is touched; the
    /// map guard is released on return.
    pub(crate) fn refuse_partitioned(&self, name: &str, what: &str) -> io::Result<()> {
        match self.relation(name) {
            Some(Relation::Partitioned(_)) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("{what} is not supported on partitioned table \"{name}\" yet"),
            )),
            Some(Relation::Plain(table)) => table.data().map(|_| ()),
            None => Ok(()),
        }
    }
}
