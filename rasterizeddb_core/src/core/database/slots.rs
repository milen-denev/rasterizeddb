//! How a database holds its tables, and the only functions that touch the
//! maps (`docs/persistent-indexes.md` §7, "table_for/schema_view with a
//! private tables map"; slice B3a of the W3 plan).
//!
//! - Each table name owns a [`Slot`]: the table's current instance behind an
//!   `ArcSwap`, so a later reload can replace the instance in one store while
//!   a statement still holding the old one keeps working on it.
//! - `Database::table_slots` and `Database::partitioned_tables` are private,
//!   and only the functions in this file name them (pinned by
//!   `slots_source_tests`). A partitioned table's schema-only shell has a slot
//!   under the logical name, like any table, and its `PartitionedTable` sits
//!   beside it in `partitioned_tables`; a partitioned table whose schema could
//!   not be read has no shell and so no slot.
//! - Every lookup by name goes through [`Database::resolve`], the one place a
//!   name becomes a map key, so a rule about which key a name means
//!   (identifier case folding) is written once.
//! - Metadata readers (Describe, parameter inference, the prepared cache,
//!   catalog views, introspection) use [`Database::schema_view`]: the schema
//!   plus a read-only stats accessor, never a scan, a mutator or a `Table`,
//!   and no lock (§7's deliberate deviation; Q3).
//! - Everything that executes still reaches a `Table` through the escape
//!   hatches [`Database::table_unlocked`] and
//!   [`Database::each_table_unlocked`]. Their callers are listed by name in
//!   `slots_source_tests`; B3b replaces each with `table_for`, which takes the
//!   table lock first, and the lists must then be empty.
//!
//! Outside `core::database` the maps do not compile; a crate reaches a table
//! through these functions:
//!
//! ```
//! fn columns(db: &rasterizeddb_core::core::database::Database) -> usize {
//!     db.schema_view("t").map_or(0, |view| view.schema().fields.len())
//! }
//! ```
//!
//! ```compile_fail,E0616
//! fn count(db: &rasterizeddb_core::core::database::Database) -> usize {
//!     db.table_slots.len()
//! }
//! ```
//!
//! ```compile_fail,E0616
//! fn partitioned(db: &rasterizeddb_core::core::database::Database) -> usize {
//!     db.partitioned_tables.len()
//! }
//! ```

use std::io;
use std::sync::atomic::{AtomicU8, Ordering};

use arc_swap::{ArcSwapAny, RefCnt};
use rclite::Arc;

use super::Database;
use super::relation::Relation;
use crate::core::partition::table::PartitionedTable;
use crate::core::row::schema::TableSchema;
use crate::core::row::table::Table;
use crate::core::storage_providers::backend::DbStorageProvider;
use crate::core::storage_providers::traits::StorageIO;

type DbTable = Table<DbStorageProvider>;

/// The `rclite::Arc` the engine passes tables around in, as an `ArcSwap`
/// pointer. `arc_swap` implements its trait for `std::sync::Arc` only, and
/// the orphan rule needs a local type.
#[derive(Clone)]
struct TableRef(Arc<DbTable>);

// SAFETY: `into_ptr` and `as_ptr` return the same pointer, the one
// `rclite::Arc::from_raw` takes back; `into_ptr` forgets its value without
// touching the count and `from_ptr` rebuilds one without touching it, so
// arc_swap's borrowing and debt accounting see exactly a strong reference per
// pointer it holds, which is what it does with `std::sync::Arc`. `rclite::Arc`
// is `Send + Sync` for a `Send + Sync` table, and its pointer is to a
// non-zero-sized allocation, so distinct live values have distinct pointers.
unsafe impl RefCnt for TableRef {
    type Base = DbTable;

    fn into_ptr(me: Self) -> *mut DbTable {
        Arc::into_raw(me.0) as *mut DbTable
    }

    fn as_ptr(me: &Self) -> *mut DbTable {
        Arc::as_ptr(&me.0) as *mut DbTable
    }

    unsafe fn from_ptr(ptr: *const DbTable) -> Self {
        // SAFETY: `ptr` came from `into_ptr` (the trait's contract), so from
        // `rclite::Arc::into_raw`.
        TableRef(unsafe { Arc::from_raw(ptr) })
    }
}

const LIVE: u8 = 0;
const DROPPED: u8 = 1;

/// One table name's place in the database: its current instance, swapped
/// atomically, and whether the name still owns it.
///
/// A reader that loaded the instance keeps it for as long as it holds the
/// `Arc`, whatever is stored into the slot after. B4's reload builds the new
/// instance, then [`Slot::swap`]s it in; until then a reload removes the slot
/// and installs a new one (unchanged behaviour). B11 adds the gate here.
pub(crate) struct Slot {
    table: ArcSwapAny<TableRef>,
    state: AtomicU8,
}

impl Slot {
    pub(super) fn new(table: Arc<DbTable>) -> Self {
        Slot {
            table: ArcSwapAny::new(TableRef(table)),
            state: AtomicU8::new(LIVE),
        }
    }

    /// The current instance.
    #[inline]
    pub(crate) fn load(&self) -> Arc<DbTable> {
        self.table.load().0.clone()
    }

    /// Stores `table` as the current instance and returns the previous one.
    /// A reader sees the old instance or the new one, never neither, and one
    /// that already loaded the old keeps it.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn swap(&self, table: Arc<DbTable>) -> Arc<DbTable> {
        self.table.swap(TableRef(table)).0
    }

    /// False once the name no longer owns this slot (DROP, RENAME, a reload's
    /// removal): B3b's `table_for` re-resolves the name when it finds that
    /// after waiting.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn is_live(&self) -> bool {
        self.state.load(Ordering::Acquire) == LIVE
    }

    fn mark_dropped(&self) {
        self.state.store(DROPPED, Ordering::Release);
    }
}

/// A table's schema and a read-only view of its size, read with no lock and
/// never blocking a writer (§7 "Describe and catalog reads take no lock").
///
/// It holds the instance it was taken from, so its schema and its figures
/// agree with each other: after a reload swaps or replaces the slot, a view
/// taken before keeps the old instance's schema and figures. It offers no
/// scan, no mutator and no `Table`; a partitioned table's view never exposes
/// its shell, only the shell's schema and the partitions' summed figures.
#[derive(Clone)]
pub struct SchemaView {
    of: ViewOf,
}

#[derive(Clone)]
enum ViewOf {
    Plain(Arc<DbTable>),
    Partitioned(Arc<PartitionedTable>),
}

impl SchemaView {
    /// The declared schema: a plain table's own, a partitioned table's (its
    /// shell's).
    #[inline]
    pub fn schema(&self) -> &TableSchema {
        match &self.of {
            ViewOf::Plain(table) => &table.schema,
            ViewOf::Partitioned(table) => &table.shell().schema,
        }
    }

    pub fn is_partitioned(&self) -> bool {
        matches!(self.of, ViewOf::Partitioned(_))
    }

    /// The read-only stats accessor (Q3).
    pub fn stats(&self) -> SchemaStats<'_> {
        SchemaStats { of: &self.of }
    }
}

/// What [`SchemaStats::totals`] reads: one length load per file and one
/// counter, never a scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelationTotals {
    /// Every file the relation owns: rows, pointers, schema sidecar, filter
    /// checkpoint and id sequence (a partitioned table's: its materialized
    /// partitions' files plus its parent files).
    pub bytes: u64,
    /// The id sequence's high-water mark (`last_row_id`).
    pub ids_handed_out: u64,
    /// Pointer records: the pointer file's length over the record length.
    pub slots: u64,
}

/// The stats accessor of a [`SchemaView`].
pub struct SchemaStats<'v> {
    of: &'v ViewOf,
}

impl SchemaStats<'_> {
    /// The relation's file lengths and id counter. Errs on a table that
    /// cannot serve data (a broken partitioned table, a shell with no
    /// partitioned table behind it) rather than reporting it empty.
    pub async fn totals(&self) -> io::Result<RelationTotals> {
        match self.of {
            ViewOf::Partitioned(table) => {
                let t = table.totals().await?;
                Ok(RelationTotals {
                    bytes: t.bytes,
                    ids_handed_out: t.ids_handed_out,
                    slots: t.slots,
                })
            }
            ViewOf::Plain(table) => {
                let data = table.data()?;
                // The counter before the pointer length, as the catalogue
                // always read them.
                let ids_handed_out = data.ids.last_row_id.load(Ordering::Relaxed);
                let pointers = data.io_pointers.get_len().await;
                let bytes = data.io_rows.get_len().await
                    + pointers
                    + table.io_schema.get_len().await
                    + data.io_filters.get_len().await
                    + data.ids.io.get_len().await;
                Ok(RelationTotals {
                    bytes,
                    ids_handed_out,
                    slots: pointers / crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN as u64,
                })
            }
        }
    }
}

impl Database {
    /// The one place a table name is resolved to a map key: `find` is asked
    /// with the key `name` means. Every lookup by name in this file goes
    /// through it. Identifier case folding drops in here (the exact key
    /// first, then the folded unquoted name); today a name is its own key.
    #[inline]
    fn resolve<T>(&self, name: &str, find: impl Fn(&str) -> Option<T>) -> Option<T> {
        find(name)
    }

    /// The slot `name` resolves to (B3b's `table_for` loads through it).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn slot(&self, name: &str) -> Option<Arc<Slot>> {
        self.resolve(name, |key| {
            self.table_slots.get(key).map(|slot| slot.value().clone())
        })
    }

    /// The schema registered under `name`, for metadata readers; `None`
    /// exactly when no table (or shell) is registered under it, including a
    /// partitioned table whose schema could not be read (its error is
    /// [`Database::relation`]'s to give). Takes no lock and never waits.
    pub fn schema_view(&self, name: &str) -> Option<SchemaView> {
        self.resolve(name, |key| {
            // The slot's guard is released before the partitioned map is
            // asked, so no reader holds a guard on both maps.
            let table = self.table_slots.get(key)?.load();
            let of = match self.partitioned_tables.get(key) {
                Some(partitioned) => ViewOf::Partitioned(partitioned.value().clone()),
                None => ViewOf::Plain(table),
            };
            Some(SchemaView { of })
        })
    }

    /// What `name` is. The partitioned map is asked first: a partitioned
    /// table's shell has a slot too, and resolving it there would hand out a
    /// table whose every data call refuses. A partitioned table whose schema
    /// could not be read has no shell, and still resolves here, so a DROP can
    /// remove it. A shell with no partitioned entry resolves to
    /// `Plain(shell)`, whose data calls refuse loudly.
    pub fn relation(&self, name: &str) -> Option<Relation> {
        self.resolve(name, |key| {
            if let Some(table) = self.partitioned_tables.get(key) {
                return Some(Relation::Partitioned(table.value().clone()));
            }
            self.table_slots
                .get(key)
                .map(|slot| Relation::Plain(slot.load()))
        })
    }

    /// The partitioned table `name` resolves to.
    pub fn partitioned_table(&self, name: &str) -> Option<Arc<PartitionedTable>> {
        self.resolve(name, |key| {
            self.partitioned_tables.get(key).map(|t| t.value().clone())
        })
    }

    /// Whether `name` is a partitioned table (broken ones included).
    pub fn is_partitioned(&self, name: &str) -> bool {
        self.resolve(name, |key| {
            self.partitioned_tables.contains_key(key).then_some(())
        })
        .is_some()
    }

    /// Whether a table, or a partitioned table's shell, is registered under
    /// `name` (what `tables.contains_key` answered).
    pub fn has_table(&self, name: &str) -> bool {
        self.resolve(name, |key| self.table_slots.contains_key(key).then_some(()))
            .is_some()
    }

    /// How many slots there are: tables plus partitioned shells.
    pub fn table_count(&self) -> usize {
        self.table_slots.len()
    }

    /// Every slot's name: tables and partitioned shells, in no order.
    pub fn table_names(&self) -> Vec<String> {
        self.table_slots
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Every relation's name: [`Database::table_names`], then each
    /// partitioned table with no shell (its schema could not be read), so a
    /// listing is never a silent subset.
    pub fn relation_names(&self) -> Vec<String> {
        let mut names = self.table_names();
        for entry in self.partitioned_tables.iter() {
            if !self.table_slots.contains_key(entry.key()) {
                names.push(entry.key().clone());
            }
        }
        names
    }

    /// How many partitioned tables there are, broken ones included.
    pub fn partitioned_count(&self) -> usize {
        self.partitioned_tables.len()
    }

    /// Every partitioned table, broken ones included, in no order.
    pub fn partitioned_tables_all(&self) -> Vec<Arc<PartitionedTable>> {
        self.partitioned_tables
            .iter()
            .map(|t| t.value().clone())
            .collect()
    }

    /// Calls `f` with each slot's name and current schema, holding no lock
    /// but the map's own shard guards (a metadata walk, as the FK lookups
    /// do). `f` must not touch the maps.
    pub(crate) fn each_schema(&self, mut f: impl FnMut(&str, &TableSchema)) {
        for entry in self.table_slots.iter() {
            let table = entry.value().table.load();
            f(entry.key(), &table.0.schema);
        }
    }

    /// **Transitional escape hatch.** The instance registered under `name`
    /// (a partitioned table's shell, as `tables.get` gave), taken with no
    /// table lock. Its callers are listed in `slots_source_tests` so B3b can
    /// move each to `table_for`; the list must then be empty.
    #[inline]
    pub(crate) fn table_unlocked(&self, name: &str) -> Option<Arc<DbTable>> {
        self.resolve(name, |key| {
            self.table_slots.get(key).map(|slot| slot.load())
        })
    }

    /// **Transitional escape hatch.** Every registered instance with its
    /// name, taken with no table lock, for the paths that walk every table
    /// (VACUUM, SHOW STORAGE, the index-name lookups). Listed like
    /// [`Database::table_unlocked`].
    pub(crate) fn each_table_unlocked(&self) -> Vec<(String, Arc<DbTable>)> {
        self.table_slots
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().load()))
            .collect()
    }

    /// The instance registered under `name`, for tests, benches and tools
    /// outside the crate, which have no lock owner. Never called by the
    /// engine (pinned by `slots_source_tests`).
    #[doc(hidden)]
    pub fn table_for_tests(&self, name: &str) -> Option<Arc<DbTable>> {
        self.resolve(name, |key| {
            self.table_slots.get(key).map(|slot| slot.load())
        })
    }

    /// Registers `table` under `name` in a new slot, replacing (and marking
    /// dropped) any slot the name had.
    pub(crate) fn install_table(&self, name: String, table: Arc<DbTable>) {
        if let Some(old) = self.table_slots.insert(name, Arc::new(Slot::new(table))) {
            old.mark_dropped();
        }
    }

    /// Removes `name`'s slot and returns its instance.
    pub(crate) fn take_table(&self, name: &str) -> Option<Arc<DbTable>> {
        self.table_slots.remove(name).map(|(_, slot)| {
            slot.mark_dropped();
            slot.load()
        })
    }

    /// Registers a partitioned table under `name`, and its shell's slot when
    /// its schema read (partitioned map first, as CREATE always did).
    pub(crate) fn install_partitioned(
        &self,
        name: String,
        table: Arc<PartitionedTable>,
        shell: Option<Arc<DbTable>>,
    ) {
        self.partitioned_tables.insert(name.clone(), table);
        if let Some(shell) = shell {
            self.install_table(name, shell);
        }
    }

    /// Removes the partitioned table `name`, and its shell's slot if it still
    /// holds `table`'s shell.
    pub(crate) fn take_partitioned(&self, name: &str, table: &Arc<PartitionedTable>) {
        self.partitioned_tables.remove(name);
        if let Some((_, slot)) = self
            .table_slots
            .remove_if(name, |_, slot| Arc::ptr_eq(&slot.load(), table.shell()))
        {
            slot.mark_dropped();
        }
    }
}
