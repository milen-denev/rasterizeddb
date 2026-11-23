//! The internal lock over a JSON shadow table's DDL (slice B5,
//! docs/persistent-indexes.md §7 "JSON shadow DDL", findings R4, Q4, L9).
//!
//! Shredded documents (`--json-shadow-tables`) extend their shadow tables as
//! keys arrive: `ALTER TABLE <shadow> ADD COLUMN`, and a type widening. Both
//! run inside the writer's INSERT or UPDATE, under its transaction. Shadow
//! tables take no table lock of their own (their parent's covers readers and
//! writers), so before this two writers could rewrite one shadow at once, or
//! one rewrite it while the other appended a row that the rewrite never
//! carried. As table locks they would deadlock document writers on each
//! other and stall every SELECT of the column behind a pending upgrade (R4).
//!
//! So a shadow's DDL is serialised here instead, outside the table-lock
//! manager:
//! - **DDL** takes the shadow's latch exclusively for the one statement (an
//!   ADD COLUMN, a widening, the shadow's CREATE), and a shadow **row write**
//!   takes it shared for the one INSERT. Neither waits for anything else
//!   while holding it, and neither holds it past its statement, so it adds no
//!   edge any cycle could run through: a writer waits at most for one shadow
//!   rewrite, and the rewrite keeps every version's own stamps (`rewrite_rows`),
//!   other writers' uncommitted rows included.
//! - Before the latch, DDL waits while **another owner holds ACCESS EXCLUSIVE
//!   on the parent: granted, never pending** (finding L9). A parent DDL that
//!   holds AE may touch the shadow chain itself. A pending one waits for the
//!   document writer's own ROW EXCLUSIVE on the parent, so yielding to it would
//!   be a cycle no graph sees, unbounded under `lock_timeout = 0`. A granted AE
//!   cannot coexist with a writer's ROW EXCLUSIVE, so in practice only an
//!   owner holding nothing on the parent ever waits here.
//!
//! **Keys first.** A statement's documents are walked once before any shadow
//! row is written ([`scoped`] with `dry`), doing every CREATE, ADD COLUMN and
//! widening they need; the walk that writes the rows then finds every column
//! in place. So one INSERT of `[{"a":1},{"b":2}]` never rewrites a shadow
//! that holds its own rows.

use std::future::Future;
use std::sync::Arc as StdArc;
use std::sync::OnceLock;

use dashmap::DashMap;

use crate::core::database::Database;
use crate::core::mvcc::table_locks::TableMode;

/// What the statement storing documents is doing: whose shadows (the parent
/// table that holds the JSON columns) and which walk.
struct JsonWrite {
    parent: String,
    dry: bool,
}

tokio::task_local! {
    static WRITE: JsonWrite;
}

/// Runs `fut` (a walk storing documents of `parent`'s JSON columns) as the
/// keys-first walk (`dry`: DDL only, no shadow row written) or the writing
/// one.
pub(crate) async fn scoped<F: Future>(parent: &str, dry: bool, fut: F) -> F::Output {
    WRITE
        .scope(
            JsonWrite {
                parent: parent.to_string(),
                dry,
            },
            fut,
        )
        .await
}

/// Whether the walk in progress is the keys-first one, which writes no row.
pub(crate) fn dry() -> bool {
    WRITE.try_with(|w| w.dry).unwrap_or(false)
}

fn parent() -> Option<String> {
    WRITE.try_with(|w| w.parent.clone()).ok()
}

type Latches = DashMap<(u64, String), StdArc<tokio::sync::RwLock<()>>, ahash::RandomState>;

/// One latch per shadow table per database, made on first use. A dropped
/// shadow's entry stays: it is a few words, and a name used again reuses it.
fn latch(database: &Database, shadow: &str) -> StdArc<tokio::sync::RwLock<()>> {
    static LATCHES: OnceLock<Latches> = OnceLock::new();
    LATCHES
        .get_or_init(Latches::default)
        .entry((database.id, shadow.to_ascii_lowercase()))
        .or_default()
        .clone()
}

/// Held by one shadow row write.
pub(crate) async fn writing(database: &Database, shadow: &str) -> tokio::sync::OwnedRwLockReadGuard<()> {
    latch(database, shadow).read_owned().await
}

/// Held by one shadow DDL statement: after any other owner's **granted**
/// ACCESS EXCLUSIVE on the parent is released, never behind a pending one
/// (finding L9). Errs only when the statement is cancelled while waiting.
pub(crate) async fn ddl(database: &Database, shadow: &str) -> Result<tokio::sync::OwnedRwLockWriteGuard<()>, String> {
    if let (Some(parent), Some(me)) = (parent(), crate::core::mvcc::owner::ensure()) {
        let key = database.lock_key(&parent).into_owned();
        // AE is the one mode ACCESS SHARE conflicts with: a holder found this
        // way holds AE. `conflicting_holder` looks at holders only, not at
        // the queue.
        while database
            .row_locks
            .table_locks()
            .conflicting_holder(me.owner, &key, TableMode::AccessShare)
            .is_some()
        {
            if let Some(reason) = crate::core::cancel::tripped() {
                return Err(format!(
                    "canceling statement ({reason:?}) while its JSON shadow change waited for a DDL on \"{parent}\""
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
    Ok(latch(database, shadow).write_owned().await)
}

#[cfg(test)]
mod tests {
    //! Finding L9 at the lock itself: a shadow's DDL waits behind another
    //! owner's granted ACCESS EXCLUSIVE on the parent, never behind a pending
    //! one, and its latch excludes the shadow's row writers.

    use super::*;
    use crate::core::mvcc::owner::{self, OwnerKind};
    use crate::core::mvcc::table_locks::{LockOwner, Request, WaitRole};
    use rclite::Arc;
    use std::time::Duration;

    const SETTLE: Duration = Duration::from_millis(300);

    async fn db() -> Arc<Database> {
        crate::replication::test_support::in_memory_database().await
    }

    fn owner_of(db: &Database) -> LockOwner {
        LockOwner::autocommit(db.transactions.begin_owner()).unwrap()
    }

    /// The shadow DDL of a statement of its own (a fresh owner), as a task.
    fn shadow_ddl(db: &Arc<Database>, parent: &'static str) -> tokio::task::JoinHandle<Result<(), String>> {
        let db = db.clone();
        tokio::spawn(async move {
            let (t, l) = (db.transactions.clone(), db.row_locks.clone());
            owner::statement(&t, &l, OwnerKind::Statement, async {
                scoped(parent, false, async { ddl(&db, "__json_p_doc").await.map(drop) }).await
            })
            .await
        })
    }

    /// Granted: another owner holds AE on the parent; the shadow DDL waits
    /// until it is released, then runs.
    ///
    /// Mutation: the wait for a granted AE removed (the DDL runs at once).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shadow_ddl_waits_behind_a_granted_parent_access_exclusive() {
        let db = db().await;
        let holder = owner_of(&db);
        let locks = db.row_locks.table_locks();
        assert!(matches!(
            locks.request(holder, "p", TableMode::AccessExclusive, WaitRole::Ddl),
            Request::Granted
        ));
        let ddl_task = shadow_ddl(&db, "p");
        tokio::time::sleep(SETTLE).await;
        assert!(!ddl_task.is_finished(), "shadow DDL ran beside another owner's ACCESS EXCLUSIVE on the parent");
        locks.release_owner(holder);
        tokio::time::timeout(Duration::from_secs(10), ddl_task)
            .await
            .expect("shadow DDL did not run once the parent's AE was released")
            .unwrap()
            .unwrap();
    }

    /// Pending: a document writer holds ROW EXCLUSIVE on the parent and a
    /// DDL waits for ACCESS EXCLUSIVE behind it; the writer's shadow DDL
    /// runs at once (yielding would wait for the writer itself, a cycle no
    /// graph sees).
    ///
    /// Mutation: the shadow DDL also yields to a pending AE (it waits while a
    /// fresh ACCESS SHARE could not be had at once); it never runs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shadow_ddl_does_not_queue_behind_a_pending_parent_access_exclusive() {
        let db = db().await;
        let (t, l) = (db.transactions.clone(), db.row_locks.clone());
        let db2 = db.clone();
        owner::statement(&t, &l, OwnerKind::Statement, async move {
            let me = owner::ensure().unwrap().owner;
            let locks = db2.row_locks.table_locks();
            assert!(matches!(
                locks.request(me, "p", TableMode::RowExclusive, WaitRole::Other),
                Request::Granted
            ));
            let ddl_owner = owner_of(&db2);
            let Request::Queued(wait) = locks.request(ddl_owner, "p", TableMode::AccessExclusive, WaitRole::Ddl) else {
                panic!("the parent's AE was granted beside a writer's RX");
            };
            let ran = tokio::time::timeout(
                Duration::from_secs(5),
                scoped("p", false, async { ddl(&db2, "__json_p_doc").await.map(drop) }),
            )
            .await;
            assert!(
                matches!(ran, Ok(Ok(()))),
                "a writer's shadow DDL queued behind a pending AE on the parent: {ran:?}"
            );
            locks.abandon(&wait);
            locks.release_owner(ddl_owner);
        })
        .await;
    }

    /// The latch: a shadow row write waits for a shadow DDL in progress, and
    /// two row writes share it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn row_writes_wait_for_shadow_ddl_and_share_with_each_other() {
        let db = db().await;
        let first = writing(&db, "__json_q_doc").await;
        tokio::time::timeout(Duration::from_secs(1), writing(&db, "__json_q_doc"))
            .await
            .expect("two row writes did not share the latch");
        drop(first);
        let held = ddl(&db, "__json_q_doc").await.unwrap();
        assert!(
            tokio::time::timeout(SETTLE, writing(&db, "__json_q_doc")).await.is_err(),
            "a row write ran beside a shadow DDL"
        );
        drop(held);
        tokio::time::timeout(Duration::from_secs(1), writing(&db, "__json_q_doc"))
            .await
            .expect("a row write waited after the DDL ended");
    }
}
