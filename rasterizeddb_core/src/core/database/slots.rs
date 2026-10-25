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
//! - Everything that executes reaches a `Table` through `table_for` /
//!   `relation_for` (`table_for.rs`), which take the table lock first and
//!   only then load the slot; a DDL's own internals (DROP, RENAME, MOVE, a
//!   reload, the FK children a parent's DDL rewrites) through `table_held`,
//!   which asserts the ACCESS EXCLUSIVE their statement took. B4 removed the
//!   escape hatches that took a `Table` with no lock at all.
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
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

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
/// the orphan rule needs a local type. Generic only so its tests can run it
/// over small values without a database; the slots use [`TableRef`].
struct ArcRef<T>(Arc<T>);

impl<T> Clone for ArcRef<T> {
    fn clone(&self) -> Self {
        ArcRef(self.0.clone())
    }
}

type TableRef = ArcRef<DbTable>;

// SAFETY: `RefCnt` asks that `into_ptr`, `as_ptr` and `from_ptr` agree on one
// pointer per value, that `into_ptr` hand over exactly one strong reference
// and `from_ptr` take exactly that one back, and that distinct live values
// have distinct pointers. For `rclite` 0.4.1 each holds by its layout:
//
// - `ArcInner<T>` is `#[repr(C)] { data: UnsafeCell<T>, counter }`, so `data`
//   sits at offset 0 and the allocation's pointer *is* the data pointer.
//   `Arc::as_ptr` returns that pointer cast to `*const T`, and `Arc::into_raw`
//   is `as_ptr` followed by `mem::forget`: the same pointer, the count
//   untouched, one strong reference now owned by the raw pointer.
//   `Arc::from_raw` rebuilds the `Arc` from exactly that pointer (it casts it
//   back to `*mut ArcInner<T>`, no offset arithmetic, which is only right
//   because `data` is first) and touches no count either. So `into_ptr` then
//   `from_ptr` is a move of one strong reference, which is how arc_swap's
//   borrowing and debt accounting treat `std::sync::Arc`.
// - The counter cannot wrap: past a barrier below its maximum, `clone` drops
//   the extra reference and panics ("reference counter overflow"), so a
//   pointer arc_swap holds is never freed under it by a wrapped count.
// - Every `ArcInner` holds a counter, so even a zero-sized `T` has a real,
//   non-zero-sized allocation and two live values never share a pointer
//   (the slots hold `Table`, which is not zero-sized either); `rclite::Arc`
//   is `Send + Sync` for a `Send + Sync` value, as arc_swap needs.
//
// `refcnt_tests` pins the first point: an rclite release that reorders
// `ArcInner` makes `Arc::as_ptr` differ from the value's address and fails
// there, loudly, rather than this impl turning into a use-after-free.
unsafe impl<T> RefCnt for ArcRef<T> {
    type Base = T;

    fn into_ptr(me: Self) -> *mut T {
        Arc::into_raw(me.0) as *mut T
    }

    fn as_ptr(me: &Self) -> *mut T {
        Arc::as_ptr(&me.0) as *mut T
    }

    unsafe fn from_ptr(ptr: *const T) -> Self {
        // SAFETY: `ptr` came from `into_ptr` (the trait's contract), so from
        // `rclite::Arc::into_raw`.
        ArcRef(unsafe { Arc::from_raw(ptr) })
    }
}

/// The layout [`ArcRef`]'s `unsafe impl RefCnt` stands on (see its SAFETY
/// comment), on values of several sizes and alignments, never a database:
/// a pointer bug here must fail an assertion, not crash the test binary.
#[cfg(test)]
mod refcnt_tests {
    use super::*;

    fn check<T>(value: T) {
        let a = Arc::new(value);
        let data = &*a as *const T as usize;
        // The library property: an rclite `Arc`'s pointer is its value's.
        assert_eq!(Arc::as_ptr(&a) as usize, data, "rclite::Arc::as_ptr is the value's address");
        let r = ArcRef(a.clone());
        assert_eq!(Arc::strong_count(&a), 2);
        assert_eq!(<ArcRef<T> as RefCnt>::as_ptr(&r) as usize, data, "ArcRef::as_ptr is the value's address");
        let raw = <ArcRef<T> as RefCnt>::into_ptr(r);
        assert_eq!(raw as usize, data, "into_ptr gives the value's address");
        assert_eq!(Arc::strong_count(&a), 2, "into_ptr keeps the reference it hands over");
        // SAFETY: `raw` came from `into_ptr` just above.
        let back = unsafe { <ArcRef<T> as RefCnt>::from_ptr(raw) };
        assert_eq!(<ArcRef<T> as RefCnt>::as_ptr(&back) as usize, data, "from_ptr rebuilds the same pointer");
        assert_eq!(Arc::strong_count(&a), 2, "from_ptr takes that one reference back, no more");
        drop(back);
        assert_eq!(Arc::strong_count(&a), 1);
    }

    #[test]
    fn rclite_arc_points_at_its_data() {
        check([7u64; 16]);
        check(String::from("x"));
        check(3u8);
        check(0u128);
        check(());
    }
}

const LIVE: u8 = 0;
const DROPPED: u8 = 1;

/// One table name's place in the database: its current instance, swapped
/// atomically, and whether the name still owns it.
///
/// A reader that loaded the instance keeps it for as long as it holds the
/// `Arc`, whatever is stored into the slot after. A reload and a MOVE build
/// the new instance, then [`Slot::swap`] it in (slice B4), so the slot, and
/// the name's place in the map, never go away during the change: a lockless
/// reader sees the old instance or the new one, never 42P01. The file set's
/// write gate lives here too (slice B11).
pub(crate) struct Slot {
    table: ArcSwapAny<TableRef>,
    state: AtomicU8,
    /// The file set's write gate (docs/persistent-indexes.md §2, "the gate
    /// belongs to the file set, not the `Table` instance"): the slot keeps
    /// the word alive across a reload, whatever happens to the instances, so
    /// a reload's open finds it (`FileGate::open`) and keeps its count and
    /// flags. An instance of another file set (an ALTER rewrite's new
    /// incarnation) brings a new word, which the swap installs here.
    gate: parking_lot::Mutex<std::sync::Arc<crate::core::index_store::file_gate::FileGate>>,
    /// The shape epoch this name's instance was installed or last reshaped
    /// under, drawn from the database-wide counter (§7 `shape_epoch`, Q7).
    /// Written only under ACCESS EXCLUSIVE on the name (an install, a swap),
    /// so a reader holding any lock on the name reads the epoch of the
    /// instance it loaded; lockless readers use the database counter.
    shape_epoch: AtomicU64,
}

impl Slot {
    pub(super) fn new(table: Arc<DbTable>, shape_epoch: u64) -> Self {
        let gate = parking_lot::Mutex::new(table.gate().clone());
        Slot {
            table: ArcSwapAny::new(ArcRef(table)),
            state: AtomicU8::new(LIVE),
            gate,
            shape_epoch: AtomicU64::new(shape_epoch),
        }
    }

    /// The shape epoch of the instance this slot holds (see the field).
    #[inline]
    pub(crate) fn shape_epoch(&self) -> u64 {
        self.shape_epoch.load(Ordering::Acquire)
    }

    /// The current instance.
    #[inline]
    pub(crate) fn load(&self) -> Arc<DbTable> {
        self.table.load().0.clone()
    }

    /// Stores `table` as the current instance and returns the previous one.
    /// A reader sees the old instance or the new one, never neither, and one
    /// that already loaded the old keeps it.
    pub(crate) fn swap(&self, table: Arc<DbTable>) -> Arc<DbTable> {
        {
            let mut gate = self.gate.lock();
            if !std::sync::Arc::ptr_eq(&gate, table.gate()) {
                *gate = table.gate().clone();
            }
        }
        self.table.swap(ArcRef(table)).0
    }

    /// The file set's write gate.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn gate(&self) -> std::sync::Arc<crate::core::index_store::file_gate::FileGate> {
        self.gate.lock().clone()
    }

    /// False once the name no longer owns this slot (DROP, the old name of a
    /// RENAME, a reload whose reopen failed): `table_for` re-resolves the
    /// name when it finds that after waiting. A swap keeps it live.
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

    /// Whether the instance is a schema-only shell: a partitioned table's,
    /// or a shell with no partitioned table behind it (whose data calls
    /// refuse).
    pub fn is_shell(&self) -> bool {
        match &self.of {
            ViewOf::Partitioned(_) => true,
            ViewOf::Plain(table) => table.is_shell(),
        }
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

    /// The map key a DDL mutator acts on for `name`: the key [`Database::resolve`]
    /// finds `name` registered under (a table, a shell or a partitioned table),
    /// `None` when nothing is. DROP, RENAME, MOVE and ALTER (`reload_table`)
    /// ask it before they take a slot out, so a mutator always removes the
    /// slot a lookup by the same name would have found, whatever key
    /// identifier case folding later maps that name to. Today the identity.
    pub(crate) fn resolve_key(&self, name: &str) -> Option<String> {
        self.resolve(name, |key| {
            (self.table_slots.contains_key(key) || self.partitioned_tables.contains_key(key))
                .then(|| key.to_string())
        })
    }

    /// The key a table lock on `name` is taken under: the key `name`
    /// resolves to, so two spellings that resolve to one table lock one
    /// entry once case folding lands in [`Database::resolve`]. Today the
    /// name itself, borrowed (no allocation on the statement path).
    #[inline]
    pub(crate) fn lock_key<'n>(&self, name: &'n str) -> std::borrow::Cow<'n, str> {
        std::borrow::Cow::Borrowed(name)
    }

    /// The key a new name is registered under: CREATE (and every reinstall)
    /// stores the canonical spelling, the one [`Database::resolve_key`] maps
    /// lookups to. Today the identity; case folding decides it here.
    #[inline]
    fn canonical_key(name: String) -> String {
        name
    }

    /// The slot `name` resolves to; `table_for` loads it after the lock
    /// (and only it may: `slots_source_tests::only_table_for_loads_a_slot`).
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

    /// The instance registered under `name`, for tests, benches and tools
    /// outside the crate, which have no lock owner. Never called by the
    /// engine (pinned by `slots_source_tests`).
    #[doc(hidden)]
    pub fn table_for_tests(&self, name: &str) -> Option<Arc<DbTable>> {
        self.resolve(name, |key| {
            self.table_slots.get(key).map(|slot| slot.load())
        })
    }

    /// Stores `table` as `name`'s current instance (§7 step 4: a reload's or
    /// a MOVE's swap), returning the instance it replaced. The slot stays,
    /// and stays live, so a statement queued on the name's lock finds the
    /// new instance in it. Registers a new slot when the name has none.
    ///
    /// An instance with another column shape than the one it replaces draws
    /// a new shape epoch for the slot and moves the database counter, after
    /// the store, so a reader that sees the moved counter sees the new
    /// instance (§7 "Generation order"). Index-only changes keep the epoch.
    pub(crate) fn swap_instance(&self, name: &str, table: Arc<DbTable>) -> Option<Arc<DbTable>> {
        match self.resolve(name, |key| self.table_slots.get(key).map(|slot| slot.value().clone())) {
            Some(slot) => {
                let old = slot.swap(table.clone());
                if !same_result_shape(&old.schema, &table.schema) && !super::table_for::lock_exempt(name) {
                    slot.shape_epoch.store(self.draw_shape_epoch(), Ordering::Release);
                    self.shape_epoch.fetch_add(1, Ordering::AcqRel);
                }
                Some(old)
            }
            None => {
                self.install_table(name.to_string(), table);
                None
            }
        }
    }

    /// Registers `table` under `name` in a new slot, replacing (and marking
    /// dropped) any slot the name had. The slot draws a fresh shape epoch
    /// from the database counter, and the counter moves again once the slot
    /// is in place: whatever the name meant before, a statement told its
    /// shape then is compared again. An engine-private relation (a
    /// statement's temporary, a shadow table) is never named by a client and
    /// moves nothing.
    pub(crate) fn install_table(&self, name: String, table: Arc<DbTable>) {
        let name = Self::canonical_key(name);
        let private = super::table_for::lock_exempt(&name);
        let epoch = if private { 0 } else { self.draw_shape_epoch() };
        if let Some(old) = self.table_slots.insert(name, Arc::new(Slot::new(table, epoch))) {
            old.mark_dropped();
        }
        if !private {
            self.shape_epoch.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// A new value of the database-wide shape counter, never handed out
    /// before.
    fn draw_shape_epoch(&self) -> u64 {
        self.shape_epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// The database-wide shape epoch now (§7 `shape_epoch`, Q7). Read before
    /// a Describe computes a shape, and after a statement's result is built
    /// (its locks then held): unchanged means no table changed what it
    /// returns in between, so nothing needs comparing.
    #[inline]
    pub fn shape_epoch(&self) -> u64 {
        self.shape_epoch.load(Ordering::Acquire)
    }

    /// The shape epoch of the instance `name` resolves to now. Exact for a
    /// caller holding a lock on `name` (only ACCESS EXCLUSIVE moves it); a
    /// partitioned table's is its shell's slot, under the logical name.
    pub fn slot_shape_epoch(&self, name: &str) -> Option<u64> {
        self.slot(name).map(|slot| slot.shape_epoch())
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
        let name = Self::canonical_key(name);
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

/// Whether two schemas give a statement the same result columns: the same
/// fields in the same places with the same names, types and declared scales,
/// and the same JSON and type-modifier declarations (both decide the type a
/// column is reported as, and its rendering). Constraints, defaults, checks
/// and indexes do not change a result's shape, so a CREATE INDEX keeps the
/// epoch and discards nothing a client was told. Anything else unequal is a
/// new shape: a spurious difference costs one comparison at the next
/// Execute, a missed one a client decoding against the wrong columns.
pub(crate) fn same_result_shape(a: &TableSchema, b: &TableSchema) -> bool {
    a.fields.len() == b.fields.len()
        && a.fields.iter().zip(b.fields.iter()).all(|(x, y)| {
            x.name == y.name
                && x.db_type == y.db_type
                && x.size == y.size
                && x.offset == y.offset
                && x.write_order == y.write_order
                && x.is_deleted == y.is_deleted
                && x.scale == y.scale
        })
        && format!("{:?}", a.json_columns) == format!("{:?}", b.json_columns)
        && format!("{:?}", a.type_modifiers) == format!("{:?}", b.type_modifiers)
}
