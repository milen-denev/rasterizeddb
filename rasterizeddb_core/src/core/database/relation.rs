//! What a table name resolves to (`docs/partitioning.md` §3.3): a plain
//! table, or a partitioned table whose shell sits in `Database.tables` and
//! whose rows live in its partitions.
//!
//! A name-keyed path asks [`Database::relation`] before it touches
//! `Database.tables`, files or `seed_in`: a shell found there by name has no
//! data, and a path that rebuilt the entry from `<name>*` files would replace
//! the partitioned table with an empty plain one.

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
    /// What `name` is. The partitioned map is asked first: a partitioned
    /// table's shell is in `tables` too, and resolving it there would hand
    /// out a table whose every data call refuses. A partitioned table whose
    /// schema could not be read has no shell, and still resolves here, so a
    /// DROP can remove it. A shell in `tables` with no partitioned entry
    /// resolves to `Plain(shell)`, whose data calls refuse loudly.
    pub fn relation(&self, name: &str) -> Option<Relation> {
        if let Some(table) = self.partitioned.get(name) {
            return Some(Relation::Partitioned(table.value().clone()));
        }
        self.tables
            .get(name)
            .map(|table| Relation::Plain(table.value().clone()))
    }

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
