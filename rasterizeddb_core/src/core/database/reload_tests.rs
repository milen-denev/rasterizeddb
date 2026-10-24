//! A reload under ACCESS EXCLUSIVE (slice B4, `reload.rs`): a statement
//! still holding the old instance reads on while the new one is swapped in
//! and a new reader sees the new schema; the generation moves only after the
//! swap, so no reader sees the new generation with the old instance; the
//! reload asserts the caller's ACCESS EXCLUSIVE, and forbids a lock wait
//! inside its steps; a failing step-3 work still leaves a fresh instance,
//! never the retired one. The wire-level cases (DDL waiting for readers,
//! DROP's waiters, RENAME's two names, FK lock sets, VACUUM modes) are in
//! `pgwire/ddl_lock_tests.rs`.

use std::sync::atomic::Ordering;

use rclite::Arc;

use super::relation_dispatch_tests::{acct, close, insert_plain, open};
use super::*;
use crate::core::mvcc::owner::{self, OwnerKind};
use crate::core::mvcc::table_locks::{LockOwner, Request, TableMode, WaitRole};
use crate::core::rql::column_default::ColumnDefault;

async fn rows_of(table: &Table<DbStorageProvider>) -> usize {
    let fetch = Table::<DbStorageProvider>::build_full_row_fetch(&table.schema.fields);
    table
        .query_row("n >= 0", &table.schema.fields.to_vec(), fetch.clone(), fetch, None, None)
        .await
        .unwrap()
        .len()
}

/// Step 3's work for these tests: a DEFAULT on `n`, so the new instance's
/// schema differs from the old one's.
async fn add_default(old: Arc<Table<DbStorageProvider>>) -> Result<(), String> {
    let entry = ColumnDefault {
        column: "n".to_string(),
        expression: "7".to_string(),
        is_deleted: false,
    };
    crate::core::rql::column_default::append(old.io_schema.clone(), &entry)
        .await
        .map_err(|e| e.to_string())
}

/// A statement that loaded the old instance before the reload keeps
/// reading it, every row, after the swap: retirement stops its maintenance,
/// not its reads. A reader that comes after sees the new instance, with the
/// new schema, live and not retired.
///
/// Mutation: `reload_with` skips `swap_instance` (the new instance is never
/// published; the new reader gets the old, retired schema).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reader_holding_the_old_instance_reads_on_and_a_new_reader_sees_the_new_schema() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    db.add_table(acct("rl_hold")).await.unwrap();
    for n in 0..5 {
        insert_plain(&db, "rl_hold", 1, n).await;
    }
    let held = db.table_for_tests("rl_hold").unwrap();
    assert!(held.schema.defaults.is_empty());

    db.reload_with("rl_hold", add_default).await.unwrap();

    // The holder: retired, and still reading every row with its own schema.
    assert!(held.is_retired(), "the old instance was not retired");
    assert_eq!(rows_of(&held).await, 5, "the old instance stopped reading after its retirement");
    assert!(held.schema.defaults.is_empty(), "the held instance's schema changed under it");

    // A new reader: the new instance, the new schema, alive.
    let fresh = db.table_for_tests("rl_hold").unwrap();
    assert!(!Arc::ptr_eq(&fresh, &held), "a new reader got the old instance");
    assert!(
        fresh.schema.defaults.iter().any(|d| d.column == "n" && !d.is_deleted),
        "a new reader does not see the new schema"
    );
    assert!(!fresh.is_retired(), "the new instance came up retired");
    assert_eq!(rows_of(&fresh).await, 5);
    drop((held, fresh));
    close(db, root);
}

/// The generation is bumped after the swap: at every step of the reload,
/// while the slot still holds the old instance, the generation is short of
/// the value it ends at, and a reader on another thread that sees that
/// final value always finds the new instance in the slot.
///
/// Mutation: `drop(changed)` moved above `swap_instance` (the end bump
/// before the store): the probe before the swap sees the final generation
/// with the old instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_reader_sees_the_new_generation_with_the_old_instance() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = std::sync::Arc::new(open(root).await);
    db.add_table(acct("rl_gen")).await.unwrap();
    insert_plain(&db, "rl_gen", 1, 1).await;
    let old = db.table_for_tests("rl_gen").unwrap();
    let before = db.schema_generation.load(Ordering::Acquire);
    // A reload bumps twice: on entry, and after the swap.
    let last = before + 2;

    let seen = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
    let _watch = {
        let seen = seen.clone();
        reload::hooks::watch("rl_gen", move |s| seen.lock().push(s))
    };

    // A reader racing the reload on another thread.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let racer = {
        let (db, old, stop) = (db.clone(), old.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut checked = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let generation = db.schema_generation.load(Ordering::Acquire);
                let current = db.table_for_tests("rl_gen").unwrap();
                if generation >= last {
                    assert!(
                        !Arc::ptr_eq(&current, &old),
                        "a reader saw generation {generation} with the old instance"
                    );
                    checked += 1;
                }
            }
            checked
        })
    };

    db.reload_with("rl_gen", add_default).await.unwrap();
    // Let the racer see the final generation.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    stop.store(true, Ordering::Relaxed);
    let checked = racer.join().expect("the racing reader failed");
    assert!(checked > 0, "the racing reader never saw the final generation");

    assert_eq!(db.schema_generation.load(Ordering::Acquire), last);
    let seen = seen.lock().clone();
    let steps: Vec<_> = seen.iter().map(|s| s.step).collect();
    use reload::hooks::Step;
    assert_eq!(steps, [Step::Retired, Step::BeforeSwap, Step::Swapped], "{seen:?}");
    for s in &seen {
        assert!(s.old_retired, "{:?}: the old instance was not retired before the work", s.step);
        if s.slot_holds_old {
            assert!(
                s.generation < last,
                "{:?}: the slot held the old instance at the final generation {}",
                s.step,
                s.generation
            );
        }
    }
    assert!(seen[1].slot_holds_old && !seen[2].slot_holds_old, "{seen:?}");
    drop(old);
    let db = std::sync::Arc::try_unwrap(db).ok().expect("the racer kept the database");
    close(db, root);
}

/// Runs `fut` as one autocommit statement, as the wire does.
async fn as_statement<F: std::future::Future>(db: &Database, fut: F) -> F::Output {
    owner::statement(&db.transactions, &db.row_locks, OwnerKind::Statement, fut).await
}

/// With ACCESS EXCLUSIVE held, a reload inside a statement goes through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reload_under_access_exclusive_goes_through() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    db.add_table(acct("rl_ae")).await.unwrap();
    as_statement(&db, async {
        db.lock_table("rl_ae", owner::ensure(), TableMode::AccessExclusive)
            .await
            .unwrap();
        db.reload_table("rl_ae").await.unwrap();
    })
    .await;
    close(db, root);
}

/// `reload_table` asserts the caller's ACCESS EXCLUSIVE (§7 step 1): inside
/// a statement that took no lock, it panics (debug builds) rather than
/// swapping an instance a reader may be using.
///
/// Mutation: the debug assertion in `reload_with` removed (the reload goes
/// through without the lock, and nothing panics).
#[cfg(debug_assertions)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "without ACCESS EXCLUSIVE")]
async fn reload_table_asserts_access_exclusive() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    db.add_table(acct("rl_noae")).await.unwrap();
    as_statement(&db, async {
        // An owner, but no lock on the table.
        assert!(owner::ensure().is_some());
        let _ = db.reload_table("rl_noae").await;
    })
    .await;
}

/// No table-lock wait may happen inside a reload's steps 2-4 (§7 step 1):
/// one that would queue there panics (debug builds) instead of waiting,
/// unbounded and uncancellable, with the old instance retired.
///
/// Mutation: the debug assertion in `lock_table` removed (the request
/// queues, the bounded wait times out, and nothing panics).
#[cfg(debug_assertions)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "inside a reload's steps")]
async fn a_lock_wait_inside_a_reload_panics() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    db.add_table(acct("rl_steps")).await.unwrap();
    db.add_table(acct("rl_other")).await.unwrap();
    // Another owner holds ACCESS EXCLUSIVE on the other table.
    let other = LockOwner::autocommit(db.transactions.begin_owner()).unwrap();
    assert!(matches!(
        db.row_locks
            .table_locks()
            .request(other, "rl_other", TableMode::AccessExclusive, WaitRole::Ddl),
        Request::Granted
    ));
    as_statement(&db, async {
        db.lock_table("rl_steps", owner::ensure(), TableMode::AccessExclusive)
            .await
            .unwrap();
        let _ = db
            .reload_with("rl_steps", |_old| async {
                let wait = db.lock_table("rl_other", owner::ensure(), TableMode::AccessShare);
                let _ = tokio::time::timeout(std::time::Duration::from_secs(2), wait).await;
                Ok(())
            })
            .await;
    })
    .await;
}

/// A step-3 work that fails after the old instance was retired still leaves
/// a fresh instance in the slot, opened from disk, never the retired one
/// (N14, N23); the failure is reported as the work's.
///
/// Mutation: `reload_with` returns the work's error before the reopen (the
/// retired instance stays in the slot).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_work_leaves_a_fresh_instance_not_the_retired_one() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    db.add_table(acct("rl_fail")).await.unwrap();
    for n in 0..3 {
        insert_plain(&db, "rl_fail", 1, n).await;
    }
    let old = db.table_for_tests("rl_fail").unwrap();
    let failed = db
        .reload_with("rl_fail", |_old| async { Err::<(), _>("injected work failure".to_string()) })
        .await;
    match failed {
        Err(reload::ReloadFailure::Work(e)) => assert_eq!(e, "injected work failure"),
        other => panic!("expected the work's failure, got {other:?}"),
    }
    assert!(old.is_retired());
    let now = db.table_for_tests("rl_fail").expect("the table is gone after a failed work");
    assert!(!Arc::ptr_eq(&now, &old), "the retired instance was put back");
    assert!(!now.is_retired(), "the slot holds a retired instance");
    assert_eq!(rows_of(&now).await, 3);
    drop((old, now));
    close(db, root);
}

/// Finding L12: a statement's own temporary (`__cte_…`) is created and
/// dropped inside a statement with no table lock and no retirement: the DROP
/// asserts nothing it cannot hold, and the instance it drops is not retired
/// (no maintenance join on the statement's path).
///
/// Mutations: `exclusive_held` drops its `lock_exempt` clause (the DROP's
/// assertion fires inside the statement); `drop_table_to_depth` retires a
/// temp too (the instance comes back retired).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_statement_private_temp_is_dropped_without_a_lock_or_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let name = format!("{}b4_probe", crate::core::rql::cte::TEMP_TABLE_PREFIX);
    as_statement(&db, async {
        assert!(owner::ensure().is_some());
        db.add_table(acct(&name)).await.unwrap();
        let instance = db.table_for_tests(&name).unwrap();
        assert!(db.drop_table(&name).await.unwrap(), "the temp was not dropped");
        assert!(!instance.is_retired(), "the temp's drop went through retirement");
        let held = crate::core::mvcc::owner::current()
            .map(|o| db.row_locks.table_locks().holds_any(o.owner))
            .unwrap_or(false);
        assert!(!held, "the statement holds a table lock after creating and dropping its temp");
    })
    .await;
    close(db, root);
}
