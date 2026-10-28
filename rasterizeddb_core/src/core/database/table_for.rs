//! Locked table access: `table_for(name, owner, mode)` (slice B3b,
//! `docs/persistent-indexes.md` §7 "As built (B3b)").
//!
//! Everything that executes reaches a table here: the lock is acquired (or
//! confirmed: an owner's held mode is granted again at the cost of a
//! fast-path lookup) for the statement's owner first, and only then is the
//! slot loaded, so a statement never reads an instance a DDL holding ACCESS
//! EXCLUSIVE could be replacing. Metadata readers stay on
//! [`Database::schema_view`], which takes no lock (§7's deliberate
//! deviation).
//!
//! - **Owner.** The caller passes the statement's owner
//!   (`owner::ensure()`): a block's root or an autocommit statement's O.
//!   `None` (no owner scope at all: start-up, recovery, maintenance, tests
//!   driving the engine below the statement layer) takes no lock.
//! - **Exempt relations.** Statement-private temporaries (`__cte_…`,
//!   `__cat_…`: unique per execution, so no other owner can name them) and a
//!   JSON column's shadow tables (`__json_…`: reached only through their
//!   parent, whose lock covers them; B5 gives shadow DDL its own internal
//!   lock) take none (finding L12).
//! - **The wait** is the table locks' one loop: woken by a grant, polling
//!   `cancel::tripped()` (57014), bounded by the session's `lock_timeout`
//!   (55P03; `0` waits as long as it takes), and after `deadlock_timeout`
//!   checking the wait-for graph shared with row and referenced-key waits
//!   (40P01 to the victim the owner-first rule picks). On any error the
//!   request is gone and nothing new is held.
//! - **A whole set at once.** [`Database::tables_for`] takes a statement's
//!   known tables in name order, deduplicated with the strongest mode per
//!   name, so two statements taking overlapping sets cannot deadlock on
//!   acquisition order.

use rclite::Arc;

use super::Database;
use super::relation::Relation;
use crate::core::mvcc::owner::{OwnerKind, StatementOwner};
use crate::core::mvcc::table_locks::{LockError, Request, TableMode, WaitRole};
use crate::core::row::table::Table;
use crate::core::storage_providers::backend::DbStorageProvider;

type DbTable = Table<DbStorageProvider>;

/// Prefix of the catalog views' per-statement relations
/// (`catalog_views::temp_name`).
const CATALOG_TEMP_PREFIX: &str = "__cat_";

/// Whether `name` is an engine-private relation that takes no table lock:
/// a statement's own temporary (a CTE's, a derived table's, a catalog
/// view's), which no other owner can name, or a JSON column's shadow table,
/// covered by its parent's lock. See the module comment.
pub(crate) fn lock_exempt(name: &str) -> bool {
    name.starts_with(crate::core::rql::cte::TEMP_TABLE_PREFIX)
        || name.starts_with(CATALOG_TEMP_PREFIX)
        || crate::core::rql::json_map::is_shadow_table(name)
}

/// Who the victim rule takes a waiter for (§7 Victims): ACCESS EXCLUSIVE
/// outside `LOCK TABLE` is a DDL, which has written nothing while it waits;
/// an autocommit statement's ACCESS SHARE is a reader **while its owner has
/// written nothing** (finding L8: a `WITH` whose data-modifying body already
/// wrote under O is no reader, whatever its head says); everything else,
/// blocks and writers alike, is `Other`. `LOCK TABLE` passes its own role.
///
/// This is the provisional role a request is made with: whether the owner
/// wrote is asked only once the request queues ([`refine_role`]), since it
/// walks the owner's attempts and a granted request never needs a role.
pub(crate) fn role_for(owner: StatementOwner, mode: TableMode) -> WaitRole {
    match mode {
        TableMode::AccessExclusive => WaitRole::Ddl,
        TableMode::AccessShare if owner.kind != OwnerKind::Block => WaitRole::AutocommitReader,
        _ => WaitRole::Other,
    }
}

/// The role of a request that queued, given the provisional one
/// [`role_for`] gave it: an autocommit reader whose owner has written is
/// `Other` (finding L8). `wrote` is asked only for a provisional reader.
pub(crate) fn refine_role(provisional: WaitRole, wrote: impl FnOnce() -> bool) -> WaitRole {
    match provisional {
        WaitRole::AutocommitReader if wrote() => WaitRole::Other,
        role => role,
    }
}

/// The message a failed table-lock wait reports, worded so the wire maps it
/// to its SQLSTATE: 55P03 (`lock timeout`), 57014 (`canceling statement`),
/// 40P01 (`deadlock detected`).
pub(crate) fn lock_error_message(error: LockError, table: &str, mode: TableMode) -> String {
    let what = format!("{} lock on relation \"{}\"", mode_name(mode), table);
    match error {
        LockError::Timeout => format!("canceling statement due to lock timeout (waiting for {what})"),
        LockError::Deadlock => format!("deadlock detected while waiting for {what}"),
        LockError::Cancelled => match crate::core::cancel::tripped() {
            Some(reason) => reason.eval_error().as_str().to_string(),
            None => "canceling statement due to user request".to_string(),
        },
    }
}

/// PostgreSQL's name for one of the four modes.
pub(crate) fn mode_name(mode: TableMode) -> &'static str {
    match mode {
        TableMode::AccessShare => "AccessShareLock",
        TableMode::RowExclusive => "RowExclusiveLock",
        TableMode::Share => "ShareLock",
        TableMode::AccessExclusive => "AccessExclusiveLock",
    }
}

/// The stronger of two modes a statement wants on one table. Lock sets hold
/// AS, RX and AE only (S comes from `LOCK TABLE` alone), which this orders
/// AS < RX < AE; S and RX together, which no set asks for, become AE, the
/// one mode that covers both.
pub(crate) fn stronger(a: TableMode, b: TableMode) -> TableMode {
    use TableMode::*;
    match (a, b) {
        (x, y) if x == y => x,
        (AccessExclusive, _) | (_, AccessExclusive) => AccessExclusive,
        (Share, RowExclusive) | (RowExclusive, Share) => AccessExclusive,
        (Share, _) | (_, Share) => Share,
        (RowExclusive, _) | (_, RowExclusive) => RowExclusive,
        _ => AccessShare,
    }
}

impl Database {
    /// Takes `mode` on `name` for `owner`, waiting as §7 says (see the module
    /// comment). Returns at once with no owner or for an exempt relation.
    ///
    /// The grant is decided synchronously; only a request that queues builds
    /// the (boxed) wait, so the common case adds no future state to its
    /// caller's and allocates nothing for one.
    pub(crate) async fn lock_table(
        &self,
        name: &str,
        owner: Option<StatementOwner>,
        mode: TableMode,
    ) -> Result<(), String> {
        let Some(owner) = owner else {
            return Ok(());
        };
        if lock_exempt(name) {
            return Ok(());
        }
        #[cfg(any(test, feature = "test_tokens"))]
        test_hooks::note_request(name);
        #[cfg(any(test, feature = "test_tokens"))]
        if test_hooks::parks(name, mode) {
            Box::pin(test_hooks::park(name, mode)).await;
        }
        let key = self.lock_key(name);
        let role = role_for(owner, mode);
        let granted = match self.row_locks.table_locks().request(owner.owner, &key, mode, role) {
            Request::Granted => Ok(()),
            Request::Queued(wait) => {
                // Finding L8: a reader is an owner that has written nothing.
                let refined = refine_role(role, || self.row_locks.owner_wrote(owner.owner.id()));
                if refined != role {
                    self.row_locks.table_locks().set_role(owner.owner, refined);
                }
                // §7 step 1: "no `table_for` wait happens after step 2(a)".
                // A reload's complete lock set is taken before it retires
                // anything; a wait in its steps would be unbounded under
                // `lock_timeout = 0` and land a 40P01 halfway through.
                debug_assert!(
                    !super::reload::in_reload_steps(),
                    "a table-lock wait for {} on \"{}\" inside a reload's steps 2-4 (§7 step 1)",
                    mode_name(mode),
                    name
                );
                Box::pin(self.wait_queued(wait, mode)).await
            }
        };
        if granted.is_ok() && mode == TableMode::AccessExclusive {
            return Box::pin(self.ticket_exclusive(name)).await;
        }
        granted
    }

    /// A ticket on the file set of the table ACCESS EXCLUSIVE was just
    /// granted on, kept by the statement's family (B12). ACCESS EXCLUSIVE is
    /// what a DDL takes on every table it may change, its own and the FK
    /// children whose sidecars it rewrites (finding L11), so this is where
    /// each of them gets a durable `DIRTY` and a held ticket before the DDL
    /// writes its schema family (T1, §2 layer 2), whichever arm does the
    /// writing. After the lock, as the lock order wants. Given back with
    /// the family's other tickets when the owner is released, after its
    /// outcome is final. A table that does not exist yet (a `CREATE`'s or a
    /// `RENAME`'s target) has no word to ticket.
    async fn ticket_exclusive(&self, name: &str) -> Result<(), String> {
        let Some(slot) = self.slot(name) else {
            return Ok(());
        };
        if !slot.is_live() {
            return Ok(());
        }
        let table = slot.load();
        let Some(family) = table.family() else {
            return Ok(());
        };
        crate::core::index_store::gate::ticket(table.gate(), Some(family))
            .await
            .map(drop)
            .map_err(|e| format!("\"{name}\" could not be marked dirty before it changed: {e}"))
    }

    /// The wait of a request [`Database::lock_table`] queued.
    async fn wait_queued(&self, wait: crate::core::mvcc::table_locks::Wait, mode: TableMode) -> Result<(), String> {
        #[cfg(any(test, feature = "test_tokens"))]
        test_hooks::note_wait(wait.table(), mode);
        let timeout = crate::core::cancel::lock_timeout();
        let cancelled = || crate::core::cancel::tripped().is_some();
        self.row_locks
            .table_locks()
            .wait_for(&wait, timeout, &cancelled, &|o| self.row_locks.table_deadlock_check(o))
            .await
            .map_err(|e| lock_error_message(e, wait.table(), mode))
    }

    /// The table `name` resolves to, after `mode` is held on it for `owner`
    /// (§7: "everything that executes goes through `table_for`"). `Ok(None)`
    /// when no table is registered under the name, before or after the wait
    /// (a DROP the wait queued behind): the caller reports 42P01 as it
    /// always did. A partitioned name locks its logical name and returns its
    /// shell, as the escape hatch it replaces did; data paths that serve a
    /// partitioned table take [`Database::relation_for`].
    pub(crate) async fn table_for(
        &self,
        name: &str,
        owner: Option<StatementOwner>,
        mode: TableMode,
    ) -> Result<Option<Arc<DbTable>>, String> {
        let Some(slot) = self.slot(name) else {
            return Ok(None);
        };
        self.lock_table(name, owner, mode).await?;
        if slot.is_live() {
            return Ok(Some(slot.load()));
        }
        // Dropped (DROP, the old name of a RENAME) while the request waited:
        // a reload or a MOVE swaps the instance and keeps the slot live. The
        // name is resolved again, and that is 42P01 by design, not by luck:
        // the lock is the name's, and a CREATE of the name takes ACCESS
        // EXCLUSIVE on it too, so one that asked after this request queues
        // behind it (rule (a)) and cannot have installed a table in between.
        // One that asked first ran first, and its table is the one the name
        // means now, under the lock this request holds: PostgreSQL's
        // re-resolution after a lock wait.
        Ok(self.slot(name).map(|slot| slot.load()))
    }

    /// [`Database::relation`] after `mode` is held on `name` for `owner`: a
    /// partitioned table is locked by its logical name, never by its
    /// partitions'. `Ok(None)` when nothing is registered under the name.
    pub(crate) async fn relation_for(
        &self,
        name: &str,
        owner: Option<StatementOwner>,
        mode: TableMode,
    ) -> Result<Option<Relation>, String> {
        // Two lookups, as `relation` makes: the partitioned map, then the
        // slot, loaded after the lock (a partitioned table is re-resolved
        // after it, which a drop it waited behind may have removed).
        if self.partitioned_table(name).is_some() {
            self.lock_table(name, owner, mode).await?;
            return Ok(self.relation(name));
        }
        let Some(slot) = self.slot(name) else {
            return Ok(None);
        };
        self.lock_table(name, owner, mode).await?;
        if slot.is_live() {
            return Ok(Some(Relation::Plain(slot.load())));
        }
        Ok(self.relation(name))
    }

    /// [`Database::tables_for`] for a DDL's complete lock set (§7 step 1):
    /// every name is locked whether or not a table is registered under it.
    /// A name a DDL creates (`CREATE TABLE`'s, a `RENAME`'s target) or drops
    /// `IF EXISTS` is locked too, so a concurrent `CREATE`, `RENAME` or
    /// `DROP` of that name is serialised against it instead of racing it.
    /// Exempt relations are skipped. In name order, deduplicated with the
    /// strongest mode per name.
    pub(crate) async fn lock_names(
        &self,
        set: &[(String, TableMode)],
        owner: Option<StatementOwner>,
    ) -> Result<(), String> {
        if owner.is_none() || set.is_empty() {
            return Ok(());
        }
        let mut wanted: smallvec::SmallVec<[(&str, TableMode); 4]> = smallvec::SmallVec::new();
        for (name, mode) in set {
            if lock_exempt(name) || name.is_empty() {
                continue;
            }
            match wanted.iter_mut().find(|(n, _)| *n == name.as_str()) {
                Some((_, m)) => *m = stronger(*m, *mode),
                None => wanted.push((name.as_str(), *mode)),
            }
        }
        wanted.sort_unstable_by(|a, b| a.0.cmp(b.0));
        for (name, mode) in wanted {
            self.lock_table(name, owner, mode).await?;
        }
        Ok(())
    }

    /// Whether the running statement may change `name` as a DDL does: its
    /// owner holds ACCESS EXCLUSIVE on it. True outside every owner scope
    /// (start-up, recovery, tests below the statement layer, which take no
    /// locks) and for an exempt relation, which takes none (finding L12).
    /// What `reload_with`, DROP, RENAME and MOVE assert (§7 step 1).
    pub(crate) fn exclusive_held(&self, name: &str) -> bool {
        if lock_exempt(name) || !crate::core::mvcc::owner::in_scope() {
            return true;
        }
        crate::core::mvcc::owner::current().is_some_and(|o| {
            self.row_locks
                .table_locks()
                .holds(o.owner, &self.lock_key(name))
                .contains(TableMode::AccessExclusive)
        })
    }

    /// The instance registered under `name`, for a DDL's internals that run
    /// under the ACCESS EXCLUSIVE their statement took on it: DROP's,
    /// RENAME's and MOVE's own table, a reload's old instance, and the FK
    /// children a parent's DDL rewrites (finding L11). Never waits; asserts
    /// the lock (a debug assertion: [`Database::exclusive_held`]).
    pub(crate) fn table_held(&self, name: &str) -> Option<Arc<DbTable>> {
        debug_assert!(
            self.exclusive_held(name),
            "\"{name}\" is reached by a DDL that does not hold ACCESS EXCLUSIVE on it (§7 step 1)"
        );
        self.slot(name).map(|slot| slot.load())
    }

    /// The tables whose live foreign keys name `parent` as their parent,
    /// `parent` itself included when it references itself: the children a
    /// DDL on `parent` may rewrite (finding L11). A metadata walk, no lock;
    /// stable once the caller holds ACCESS EXCLUSIVE on `parent`, since
    /// adding a child (`ADD FOREIGN KEY`, `CREATE TABLE … REFERENCES`) takes
    /// SHARE on the parent, which conflicts with it.
    pub(crate) fn fk_children(&self, parent: &str) -> Vec<String> {
        self.fk_children_on(parent, None)
    }

    /// [`Database::fk_children`], narrowed to the children whose keys
    /// reference one of `columns` of `parent` (`None`: any column): the only
    /// children a column's DDL on the parent rewrites or reads, so an ADD,
    /// DROP or RENAME COLUMN waits for no other child's readers.
    ///
    /// Read from the reverse index (`Database::referencing_keys`), not a
    /// walk of every schema.
    pub(crate) fn fk_children_on(&self, parent: &str, columns: Option<&[&str]>) -> Vec<String> {
        self.fk_children_from_reverse(parent, columns)
    }

    /// Takes a statement's whole known table set at once (§7 Acquisition):
    /// deduplicated with the strongest mode per name, in name order, each
    /// held before the next is requested. Name order is what keeps two
    /// statements whose sets overlap from deadlocking on the order they ask
    /// in; a real cycle (through row locks, or a table found late) is the
    /// deadlock check's. Names no table is registered under, and exempt
    /// relations, are skipped: a set is read off the statement text, so it
    /// can name a CTE or an alias.
    pub(crate) async fn tables_for(
        &self,
        set: &[(String, TableMode)],
        owner: Option<StatementOwner>,
    ) -> Result<(), String> {
        if owner.is_none() || set.is_empty() {
            return Ok(());
        }
        let mut wanted: smallvec::SmallVec<[(&str, TableMode); 4]> = smallvec::SmallVec::new();
        for (name, mode) in set {
            if lock_exempt(name) {
                continue;
            }
            match wanted.iter_mut().find(|(n, _)| *n == name.as_str()) {
                Some((_, m)) => *m = stronger(*m, *mode),
                None => wanted.push((name.as_str(), *mode)),
            }
        }
        wanted.sort_unstable_by(|a, b| a.0.cmp(b.0));
        for (name, mode) in wanted {
            if !self.has_table(name) && !self.is_partitioned(name) {
                continue;
            }
            self.lock_table(name, owner, mode).await?;
        }
        Ok(())
    }
}

/// Test probes: which waits a statement made, so a test can tell a wait from
/// a fast grant. Off in every server build.
#[cfg(any(test, feature = "test_tokens"))]
pub mod test_hooks {
    use crate::core::mvcc::table_locks::TableMode;

    static WAITS: parking_lot::Mutex<Vec<(String, TableMode)>> = parking_lot::Mutex::new(Vec::new());

    pub(super) fn note_wait(table: &str, mode: TableMode) {
        let mut waits = WAITS.lock();
        if waits.len() < 4096 {
            waits.push((table.to_string(), mode));
        }
    }

    /// Every queued table-lock request since the last call, in order.
    pub fn take_waits() -> Vec<(String, TableMode)> {
        std::mem::take(&mut *WAITS.lock())
    }

    tokio::task_local! {
        static REQUESTS: std::cell::RefCell<std::collections::HashMap<String, usize>>;
    }

    pub(super) fn note_request(table: &str) {
        let _ = REQUESTS.try_with(|r| *r.borrow_mut().entry(table.to_string()).or_default() += 1);
    }

    /// Runs `fut`, returning how many table-lock requests (granted or
    /// queued) it made per table name. Per task, so tests running beside it
    /// do not count; nothing in a statement spawns a task.
    pub async fn count_requests<F: std::future::Future>(
        fut: F,
    ) -> (F::Output, std::collections::HashMap<String, usize>) {
        REQUESTS
            .scope(std::cell::RefCell::new(std::collections::HashMap::new()), async {
                let out = fut.await;
                (out, REQUESTS.with(|r| std::mem::take(&mut *r.borrow_mut())))
            })
            .await
    }

    /// One parked request: the next table-lock request on its table in a
    /// mode other than ACCESS EXCLUSIVE stops before it is made, says so,
    /// and goes on when released (B7: a statement parked between what it
    /// read before its lock and the lock itself, while a DDL swaps the
    /// table; finding L7).
    pub struct Park {
        table: String,
        arrived: std::sync::Arc<tokio::sync::Notify>,
        release: std::sync::Arc<tokio::sync::Notify>,
    }

    static PARKS: parking_lot::Mutex<Vec<(String, std::sync::Arc<tokio::sync::Notify>, std::sync::Arc<tokio::sync::Notify>)>> =
        parking_lot::Mutex::new(Vec::new());

    /// Parks the next non-exclusive table-lock request on `table`, once.
    pub fn park_next(table: &str) -> Park {
        let arrived = std::sync::Arc::new(tokio::sync::Notify::new());
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        PARKS.lock().push((table.to_string(), arrived.clone(), release.clone()));
        Park { table: table.to_string(), arrived, release }
    }

    impl Park {
        /// Waits until the parked request has stopped.
        pub async fn arrived(&self) {
            self.arrived.notified().await;
        }

        /// Lets the parked request go on (to its lock).
        pub fn release(&self) {
            self.release.notify_one();
        }
    }

    impl Drop for Park {
        fn drop(&mut self) {
            PARKS.lock().retain(|(t, _, _)| *t != self.table);
            self.release.notify_one();
        }
    }

    pub(super) fn parks(table: &str, mode: TableMode) -> bool {
        mode != TableMode::AccessExclusive && PARKS.lock().iter().any(|(t, _, _)| t == table)
    }

    pub(super) async fn park(table: &str, mode: TableMode) {
        if mode == TableMode::AccessExclusive {
            return;
        }
        let taken = {
            let mut parks = PARKS.lock();
            parks.iter().position(|(t, _, _)| t == table).map(|i| parks.remove(i))
        };
        if let Some((_, arrived, release)) = taken {
            arrived.notify_one();
            release.notified().await;
        }
    }
}
