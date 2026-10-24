//! Lock owners: who a statement's table locks belong to
//! (docs/persistent-indexes.md §7 Owner; slice B2).
//!
//! # One owner per statement
//!
//! Every statement and every block has exactly one lock owner, from its first
//! lock to its last durability step:
//! - a block's owner is its root transaction ([`LockOwner::block`]);
//! - an autocommit statement's is **O**, a top-level id begun in the
//!   transaction manager ([`TransactionManager::begin_owner`]) before its first
//!   lock and ended after its last durability step. Each conflict-retry
//!   attempt runs under an id of its own, aliased to O
//!   (`RowLocks::owner_alias`), so the statement's table locks and its node in
//!   the wait-for graph survive its retries. A `WITH` statement's own
//!   transaction *is* O ([`adopt_transaction`]; Q1);
//! - replication apply, login role lookups, the importer and the native
//!   protocol run under named owners ([`OwnerKind`]), never under `FROZEN_TX`,
//!   which [`LockOwner`] cannot be built from: its constructors refuse it,
//!   and nothing converts a bare id.
//!
//! ```compile_fail
//! use rasterizeddb_core::core::mvcc::owner::LockOwner;
//! let owner: LockOwner = 1u64.into();
//! ```
//!
//! The owner is carried in a task-local [`OwnerScope`], so every
//! sub-execution of a statement (CTE and derived-table bodies, catalog
//! relations and their `DROP`, subqueries, `RETURNING` bodies, `ON CONFLICT`'s
//! update, foreign-key checks and cascades, a multi-row insert's tuples, JSON
//! shadow DDL) finds the statement's owner however its call reached it. A
//! nested entry point (`execute`, `execute_in_txn`) joins the scope it finds
//! instead of opening another: two owners in one task deadlock with
//! themselves where no graph can see it (§7). Nothing in a statement spawns a
//! task, so the scope reaches every piece of it.
//!
//! # Lazily minted
//!
//! An autocommit scope begins O the first time something asks for the owner
//! ([`ensure`]): the executor's attempt loop, and from B3b every table lookup.
//! A statement that never reaches the engine (`SET`, `SHOW`, `BEGIN`) costs no
//! id. O is begun before the first attempt, so it is older than every attempt
//! id the statement uses, and the graph ages the statement by it.
//!
//! # Released by space
//!
//! A transaction's end releases its row and referenced-key locks only
//! (`RowLocks::release_rows`). The owner's table locks are released when its
//! scope ends ([`statement`]'s guard), or for a block when the session's
//! block ends, after the last durability step in both cases (finding L4), on
//! every exit path: success, error, cancellation, a departed client, a panic
//! unwinding, the future being dropped.
//!
//! # Representation
//!
//! O is a `TxId` per autocommit statement, as §7 decides, and B3b's perf
//! gate kept it (Q4: O's own cost is B2's ~210 ns, and the table locks on
//! top of it measured 1-4% of a point statement, none of it in O); everything
//! outside this module and [`TransactionManager::begin_owner`] sees only
//! [`StatementOwner`] and [`LockOwner`], so swapping it touches this file.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use super::TxId;
use super::row_locks::RowLocks;
pub use super::table_locks::LockOwner;
use super::transaction::TransactionManager;

/// Who an owner acts for. Diagnostic, and what the tests pin: no path runs
/// under `FROZEN_TX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OwnerKind {
    /// An autocommit statement's O, from the wire or from `execute`.
    Statement,
    /// A `BEGIN` block: its root transaction.
    Block,
    /// A replication apply's transaction.
    Replication,
    /// Login role lookups and the role catalogue's bootstrap.
    Auth,
    /// `IMPORT … FROM POSTGRES`: its DDL statements and batch transactions.
    Importer,
    /// The native protocol's statements.
    Native,
}

/// A resolved owner and what it acts for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatementOwner {
    pub owner: LockOwner,
    pub kind: OwnerKind,
}

/// One statement's (or one block statement's) owner, held in the task-local.
///
/// Dropping an autocommit scope ends it: what O holds is released, then O is
/// ended unless the statement adopted it as its transaction (which it ended
/// itself). The task-local future drops its value when it is dropped, which
/// is right after the statement's future completes, or when it is dropped
/// unfinished (a cancelled task) or unwound through (a panic): every way out.
pub struct OwnerScope {
    kind: OwnerKind,
    /// A block's root or a named transaction's id: nothing to mint or end.
    fixed: Option<LockOwner>,
    /// O, once something asked for it.
    minted: OnceLock<LockOwner>,
    /// O was begun as the statement's own transaction (a `WITH` statement's),
    /// so its outcome is the statement's to decide and the scope's end only
    /// releases its locks.
    adopted: AtomicBool,
    transactions: Arc<TransactionManager>,
    /// Set for an autocommit scope, which releases O when it ends.
    row_locks: Option<Arc<RowLocks>>,
}

impl Drop for OwnerScope {
    fn drop(&mut self) {
        let Some(row_locks) = &self.row_locks else {
            return;
        };
        let Some(owner) = self.minted.get().copied() else {
            return;
        };
        row_locks.release_owner(owner);
        if !self.adopted.load(Ordering::Acquire) {
            self.transactions.end_owner(owner.id());
        }
    }
}

impl OwnerScope {
    fn resolved(&self) -> Option<LockOwner> {
        self.fixed.or_else(|| self.minted.get().copied())
    }

    fn ensure(&self) -> LockOwner {
        if let Some(owner) = self.fixed {
            return owner;
        }
        *self.minted.get_or_init(|| {
            LockOwner::autocommit(self.transactions.begin_owner())
                .expect("a begun id is a real transaction")
        })
    }
}

tokio::task_local! {
    static SCOPE: OwnerScope;
}

/// Whether the caller runs inside an owner scope.
pub fn in_scope() -> bool {
    SCOPE.try_with(|_| ()).is_ok()
}

/// Whether the caller runs inside an owner scope of the database whose
/// transactions `transactions` are: an owner is one database's transaction,
/// so a sub-execution against another database never joins it.
fn in_scope_of(transactions: &Arc<TransactionManager>) -> bool {
    SCOPE.try_with(|scope| Arc::ptr_eq(&scope.transactions, transactions)).unwrap_or(false)
}

/// The scope's owner if it has one already; `None` outside a scope or before
/// a lazy one is minted. Never mints.
pub fn current() -> Option<StatementOwner> {
    SCOPE
        .try_with(|scope| {
            scope.resolved().map(|owner| StatementOwner {
                owner,
                kind: scope.kind,
            })
        })
        .ok()
        .flatten()
}

/// The scope's owner, beginning O now if this is an autocommit scope that has
/// none yet. `None` only outside every scope.
pub fn ensure() -> Option<StatementOwner> {
    SCOPE
        .try_with(|scope| StatementOwner {
            owner: scope.ensure(),
            kind: scope.kind,
        })
        .ok()
}

/// Begins O as the statement's own transaction, for a `WITH` statement whose
/// CTE bodies and outer statement share one (Q1: the `WITH` transaction is
/// O, never a second owner). `None` when the scope is not an autocommit one or
/// O was already begun as a plain owner; the caller then begins its own
/// transaction and aliases it to [`ensure`]'s owner.
pub fn adopt_transaction() -> Option<TxId> {
    SCOPE
        .try_with(|scope| {
            if scope.fixed.is_some() || scope.minted.get().is_some() {
                return None;
            }
            let mut begun = None;
            let owner = *scope.minted.get_or_init(|| {
                let tx = scope.transactions.begin_autocommit();
                begun = Some(tx);
                LockOwner::autocommit(tx).expect("a begun id is a real transaction")
            });
            let tx = begun?;
            debug_assert_eq!(owner.id(), tx);
            scope.adopted.store(true, Ordering::Release);
            Some(tx)
        })
        .ok()
        .flatten()
}

/// Runs `fut` as one autocommit statement of `kind`: under the scope already
/// in place if there is one (a sub-execution joins its statement's owner),
/// otherwise under a new scope whose O is begun lazily and ended, its locks
/// released, when the returned future completes or is dropped.
///
/// Not an `async fn`, so `fut` is stored once, not once as an argument and
/// again inside the scope: the statement futures it wraps are the largest in
/// the process. Whether a scope is in place is decided when this is called,
/// which is where it is awaited.
pub fn statement<F: Future>(
    transactions: &Arc<TransactionManager>,
    row_locks: &Arc<RowLocks>,
    kind: OwnerKind,
    fut: F,
) -> impl Future<Output = F::Output> {
    if in_scope_of(transactions) {
        return futures::future::Either::Left(fut);
    }
    futures::future::Either::Right(SCOPE.scope(
        OwnerScope {
            kind,
            fixed: None,
            minted: OnceLock::new(),
            adopted: AtomicBool::new(false),
            transactions: transactions.clone(),
            row_locks: Some(row_locks.clone()),
        },
        fut,
    ))
}

/// Runs `fut` under `root`'s owner (a block, or a named internal transaction
/// such as a replication apply or an import batch), unless a scope is already
/// in place. Nothing is released when `fut` ends: the transaction outlives
/// the statement, and whoever ends it releases its owner
/// ([`OwnerRelease`]). `FROZEN_TX` has no owner and runs unscoped. Not an
/// `async fn`, for [`statement`]'s reason.
pub fn under<F: Future>(
    transactions: &Arc<TransactionManager>,
    root: TxId,
    kind: OwnerKind,
    fut: F,
) -> impl Future<Output = F::Output> {
    match LockOwner::block(transactions.top_level(root)) {
        Some(owner) if !in_scope_of(transactions) => futures::future::Either::Right(SCOPE.scope(
            OwnerScope {
                kind,
                fixed: Some(owner),
                minted: OnceLock::new(),
                adopted: AtomicBool::new(false),
                transactions: transactions.clone(),
                row_locks: None,
            },
            fut,
        )),
        _ => futures::future::Either::Left(fut),
    }
}

/// Releases a block's (or named transaction's) owner when dropped, or
/// earlier through [`OwnerRelease::release`]: after the transaction's last
/// durability step and before its outcome is reported, on every exit path.
pub struct OwnerRelease {
    row_locks: Arc<RowLocks>,
    owner: Option<LockOwner>,
}

impl OwnerRelease {
    pub fn new(row_locks: &Arc<RowLocks>, owner: LockOwner) -> OwnerRelease {
        OwnerRelease {
            row_locks: row_locks.clone(),
            owner: Some(owner),
        }
    }

    /// For a transaction's root id; holds nothing for `FROZEN_TX` or `NO_TX`.
    pub fn for_root(row_locks: &Arc<RowLocks>, root: TxId) -> OwnerRelease {
        OwnerRelease {
            row_locks: row_locks.clone(),
            owner: LockOwner::block(root),
        }
    }

    pub fn owner(&self) -> Option<LockOwner> {
        self.owner
    }

    /// Releases now.
    pub fn release(mut self) {
        self.release_now();
    }

    fn release_now(&mut self) {
        if let Some(owner) = self.owner.take() {
            self.row_locks.release_owner(owner);
        }
    }
}

impl Drop for OwnerRelease {
    fn drop(&mut self) {
        self.release_now();
    }
}

impl std::fmt::Debug for OwnerRelease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("OwnerRelease").field(&self.owner).finish()
    }
}

/// Test probes: each sub-execution site records the owner it ran under, so a
/// test can check that one statement's pieces all ran under one owner. Off in
/// every server build.
#[cfg(any(test, feature = "test_tokens"))]
pub mod probe {
    use super::*;

    /// Where a piece of a statement ran.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum Site {
        /// `execute_with_context` (any statement, any depth: a cascade is
        /// depth > 0).
        Dispatch { depth: usize },
        /// An autocommit attempt, in `execute_with_sync`'s retry loop.
        Attempt,
        /// A statement run inside a caller's transaction, in
        /// `execute_in_txn`'s retry loop.
        InTxnAttempt,
        /// `substitute_in_subqueries`.
        Subquery,
        /// `run_returning_rows` (a data-modifying CTE body).
        ReturningBody,
        /// `returning_rows_for_write` (a write's `RETURNING` evaluation).
        Returning,
        /// `apply_conflict_update` (`ON CONFLICT DO UPDATE`).
        ConflictUpdate,
        /// A foreign-key check.
        ForeignKeyCheck,
        /// A multi-row insert's tuple.
        BatchTuple,
        /// JSON shadow DDL (`run_ddl`).
        ShadowDdl,
        /// A JSON default's reference.
        JsonDefault,
        /// `cte::materialize`.
        Cte,
        /// `cte::materialize_derived`.
        Derived,
        /// `catalog_views::materialize`.
        Catalog,
        /// `cte::drop_temps`.
        DropTemps,
    }

    /// One record.
    #[derive(Debug, Clone, Copy)]
    pub struct Record {
        pub site: Site,
        /// The scope's owner, resolved (minted if lazy) at the site: what a
        /// lock taken there would be held by.
        pub owner: Option<StatementOwner>,
        /// The transaction the piece read and wrote under.
        pub tx: TxId,
        /// `RowLocks::owner_of(tx)`: whom that transaction's table locks and
        /// edges belong to.
        pub owner_of_tx: TxId,
        /// Whether the owner read `InProgress` at the site.
        pub owner_in_progress: bool,
    }

    tokio::task_local! {
        static RECORDS: parking_lot::Mutex<Vec<Record>>;
    }

    /// Runs `fut`, returning what every probe inside it recorded.
    pub async fn capture<F: Future>(fut: F) -> (F::Output, Vec<Record>) {
        RECORDS
            .scope(parking_lot::Mutex::new(Vec::new()), async {
                let out = fut.await;
                let records = RECORDS.with(|r| std::mem::take(&mut *r.lock()));
                (out, records)
            })
            .await
    }

    /// Records `site`, outside a capture a no-op.
    pub fn note(site: Site, tx: TxId, transactions: &TransactionManager, row_locks: &RowLocks) {
        if RECORDS.try_with(|_| ()).is_err() {
            return;
        }
        let owner = ensure();
        let record = Record {
            site,
            owner,
            tx,
            owner_of_tx: row_locks.owner_of(tx),
            owner_in_progress: owner.is_some_and(|o| {
                transactions.outcome(o.owner.id()) == crate::core::mvcc::TxOutcome::InProgress
            }),
        };
        let _ = RECORDS.try_with(|r| r.lock().push(record));
    }
}

/// [`probe::note`] at a site, compiled out of server builds.
#[macro_export]
#[doc(hidden)]
macro_rules! owner_probe {
    ($site:expr, $tx:expr, $database:expr) => {
        #[cfg(any(test, feature = "test_tokens"))]
        {
            let database = &$database;
            $crate::core::mvcc::owner::probe::note(
                $site,
                $tx,
                &database.transactions,
                &database.row_locks,
            );
        }
    };
}

#[cfg(test)]
#[path = "owner_tests.rs"]
mod tests;
