//! Nothing is handed on, waited out or counted free while a transaction's
//! commit is recorded but not yet visible.
//!
//! `TransactionManager::commit` records the outcome, then retires the
//! transaction from the running set, and only a snapshot taken after the
//! retirement sees the commit. Whatever judged the transaction gone by its
//! recorded outcome, and then read at a snapshot, read the rows as they were
//! before the commit. First seen as the `SKIP LOCKED` job queue handing a
//! finished job to a second worker (compat `composite_row_locks.rs`, "job
//! (4,25) claimed by workers 4 and 5"): a locker kept a row whose writer was
//! in that window, scanned again at a snapshot that could not see the
//! commit, found the row locked by itself already, and returned it as
//! scanned. The same judgment, elsewhere, stored orphans (a referenced-key
//! lock), lost an upsert and showed a key twice (the uniqueness check's
//! in-flight rule), and failed a replay with 40001 (the conflict wait).
//! `TransactionManager::unfinished` is the rule they ask now.
//!
//! Each test parks a `COMMIT` in that window
//! (`commit_test_hooks::after_record`) and runs the second transaction's
//! statement inside it.

use std::sync::{Mutex, mpsc};
use std::time::Duration;

use super::*;
use crate::core::mvcc::owner::LockOwner;
use crate::core::mvcc::transaction::commit_test_hooks;

const BOUND: Duration = Duration::from_secs(20);

const CLAIM: &str = "SELECT tenant_id, id FROM q WHERE state = 'new' LIMIT 1 FOR UPDATE SKIP LOCKED";

async fn queue() -> Arc<Database> {
    let db = crate::replication::test_support::in_memory_database().await;
    for sql in [
        "CREATE TABLE q (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, state TEXT NOT NULL, \
         claimed_by BIGINT, PRIMARY KEY (tenant_id, id))",
        "INSERT INTO q (tenant_id, id, state) VALUES (1, 1, 'new'), (1, 2, 'new')",
    ] {
        let response = execute(recognize_query_purpose(sql).unwrap(), db.clone()).await;
        assert_ne!(response.first(), Some(&3), "{sql}: {}", String::from_utf8_lossy(&response));
    }
    db
}

async fn in_block(db: &Arc<Database>, tx: TxId, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("{sql}: not recognised"));
    let response = execute_in_txn(purpose, db.clone(), tx).await;
    assert_ne!(response.first(), Some(&3), "{sql}: {}", String::from_utf8_lossy(&response));
    response
}

fn int(column: &crate::core::row::row::Column) -> i64 {
    let bytes = column.data.into_slice();
    i64::from_le_bytes(bytes[..8].try_into().expect("a BIGINT cell"))
}

/// The (tenant_id, id) pairs a claim returned.
async fn claim(db: &Arc<Database>, tx: TxId) -> Vec<(i64, i64)> {
    let response = in_block(db, tx, CLAIM).await;
    assert_eq!(response.first(), Some(&2), "{}", String::from_utf8_lossy(&response));
    crate::core::row::row::vec_into_rows(&response[1..])
        .unwrap()
        .iter()
        .map(|row| (int(&row.columns[0]), int(&row.columns[1])))
        .collect()
}

async fn mark(db: &Arc<Database>, tx: TxId, worker: i64, (tenant, id): (i64, i64)) {
    in_block(
        db,
        tx,
        &format!("UPDATE q SET state = 'done', claimed_by = {worker} WHERE tenant_id = {tenant} AND id = {id}"),
    )
    .await;
}

fn end(db: &Arc<Database>, tx: TxId) {
    db.commit_transaction(tx);
    db.release_owner(LockOwner::block(tx).unwrap());
}

/// (state, claimed_by) of every row, by key. Every row is claimed by the
/// time this is read, so `claimed_by` is never NULL.
async fn stored(db: &Arc<Database>) -> Vec<((i64, i64), String, i64)> {
    let sql = "SELECT tenant_id, id, state, claimed_by FROM q ORDER BY tenant_id, id";
    let response = execute(recognize_query_purpose(sql).unwrap(), db.clone()).await;
    assert_eq!(response.first(), Some(&2), "{}", String::from_utf8_lossy(&response));
    crate::core::row::row::vec_into_rows(&response[1..])
        .unwrap()
        .iter()
        .map(|row| {
            let state = String::from_utf8_lossy(row.columns[2].data.into_slice()).to_string();
            ((int(&row.columns[0]), int(&row.columns[1])), state, int(&row.columns[3]))
        })
        .collect()
}

/// Commits `tx` on a thread of its own and parks it between its recorded
/// outcome and its retirement; returns the thread and the sender that lets
/// it go on. Waits until it is parked.
fn park_commit(db: &Arc<Database>, tx: TxId) -> (std::thread::JoinHandle<()>, mpsc::Sender<()>) {
    let (reached, parked) = mpsc::channel::<()>();
    let (go, resume) = mpsc::channel::<()>();
    let resume = Mutex::new(resume);
    commit_test_hooks::after_record(&db.transactions, tx, move || {
        reached.send(()).unwrap();
        resume.lock().unwrap().recv_timeout(BOUND).expect("the test never let the commit go on");
    });
    let committer = {
        let db = db.clone();
        std::thread::spawn(move || end(&db, tx))
    };
    parked.recv_timeout(BOUND).expect("the commit never reached the hook");
    (committer, go)
}

/// Worker A claims job (1,1) and marks it done; its `COMMIT` is parked
/// with the outcome recorded and A still running. Worker B claims inside
/// that window. A's write marked (1,1), and the mark holds until A is
/// retired, so B skips it and is handed (1,2); and what is stored names A
/// for (1,1).
///
/// Mutation: `acquire_row_lock`'s `owner_live` is `owner_live` alone (as it
/// was): B takes A's lock (A's outcome is no longer `InProgress`) and
/// ignores its mark, its locking read scans again at a snapshot that cannot
/// see A's commit, returns (1,1) as `'new'`, and B's mark would overwrite
/// A's: the assertion on B's claim fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lock_is_not_handed_on_before_its_holders_commit_is_visible() {
    let db = queue().await;
    let a = db.transactions.begin();
    assert_eq!(claim(&db, a).await, vec![(1, 1)]);
    mark(&db, a, 1, (1, 1)).await;

    let (committer, go) = park_commit(&db, a);
    let b = db.transactions.begin();
    let got = claim(&db, b).await;
    // B marks what it was handed only once A is retired: a write of a row
    // A wrote would wait on A, which is parked.
    go.send(()).unwrap();
    committer.join().expect("A's commit");
    for job in &got {
        mark(&db, b, 2, *job).await;
    }
    end(&db, b);

    assert_eq!(got, vec![(1, 2)], "B was handed the job A had finished");
    assert_eq!(
        stored(&db).await,
        vec![((1, 1), "done".to_string(), 1), ((1, 2), "done".to_string(), 2)]
    );
}

/// The same window, met through a writer's row mark rather than its lock:
/// A writes (1,1) with no row locked anywhere, so it takes no lock and only
/// marks the row (`RowLocks::mark_row_writes`). Its `COMMIT` is parked as
/// above, and B's claim inside the window skips (1,1), whose writer is
/// still running.
///
/// Mutation: `acquire_row_lock`'s `owner_live` is `owner_live` alone (as it
/// was): B ignores A's mark, and returns (1,1) as `'new'` after A wrote it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writers_mark_holds_its_row_until_its_commit_is_visible() {
    let db = queue().await;
    let a = db.transactions.begin();
    assert!(db.row_locks.is_empty(), "the fast path needs no row locked");
    mark(&db, a, 1, (1, 1)).await;
    assert!(db.row_locks.is_empty(), "A took a row lock: this is not the marked path");

    let (committer, go) = park_commit(&db, a);
    let b = db.transactions.begin();
    let got = claim(&db, b).await;
    // B marks what it was handed only once A is retired: a write of a row
    // A wrote would wait on A, which is parked.
    go.send(()).unwrap();
    committer.join().expect("A's commit");
    for job in &got {
        mark(&db, b, 2, *job).await;
    }
    end(&db, b);

    assert_eq!(got, vec![(1, 2)], "B was handed the row A had written");
    assert_eq!(
        stored(&db).await,
        vec![((1, 1), "done".to_string(), 1), ((1, 2), "done".to_string(), 2)]
    );
}

// ---------------------------------------------------------------------------
// The rest of the family: every other place that judged a transaction gone by
// its recorded outcome and then acted on a read taken next.
// ---------------------------------------------------------------------------

/// How long a statement started inside the window is given to finish there
/// before the parked commit is let go, unless it begins a conflict wait
/// first. One that would act on the stale read does so at once; one that
/// waits for the commit's retirement is still waiting when this runs out.
/// A conflict wait gives up past `CONFLICT_WAIT_BUDGET`, shorter than this,
/// so [`start_in_window`] lets the commit go the moment one begins.
const GRACE: Duration = Duration::from_secs(2);

/// Conflict waits begun on a live blocker, at every retry site.
fn conflict_waits(db: &Arc<Database>) -> usize {
    db.row_locks.conflict_waits.waits.iter().map(|w| w.load(std::sync::atomic::Ordering::Acquire)).sum()
}

/// Starts `sql` in autocommit inside a parked commit's window and returns
/// once it has finished there (with its response), begun a conflict wait,
/// or had [`GRACE`].
async fn start_in_window(db: &Arc<Database>, sql: &str) -> (tokio::task::JoinHandle<Vec<u8>>, Option<Vec<u8>>) {
    let waits = conflict_waits(db);
    let statement = tokio::spawn({
        let (db, sql) = (db.clone(), sql.to_string());
        async move { autocommit(&db, &sql).await }
    });
    let started = std::time::Instant::now();
    while !statement.is_finished() && conflict_waits(db) == waits && started.elapsed() < GRACE {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    if statement.is_finished() {
        return (tokio::spawn(async { Vec::new() }), Some(statement.await.unwrap()));
    }
    (statement, None)
}

/// Lets the parked commit go on, then the statement finish.
async fn release(
    (committer, go): (std::thread::JoinHandle<()>, mpsc::Sender<()>),
    (statement, early): (tokio::task::JoinHandle<Vec<u8>>, Option<Vec<u8>>),
) -> Vec<u8> {
    go.send(()).unwrap();
    tokio::task::spawn_blocking(move || committer.join().expect("the parked commit"))
        .await
        .unwrap();
    match early {
        Some(response) => response,
        None => tokio::time::timeout(BOUND, statement).await.expect("the statement never finished").unwrap(),
    }
}

async fn db_with(statements: &[&str]) -> Arc<Database> {
    let db = crate::replication::test_support::in_memory_database().await;
    for sql in statements {
        let response = execute(recognize_query_purpose(sql).unwrap(), db.clone()).await;
        assert_ne!(response.first(), Some(&3), "{sql}: {}", String::from_utf8_lossy(&response));
    }
    db
}

async fn autocommit(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    execute(recognize_query_purpose(sql).unwrap_or_else(|| panic!("{sql}: not recognised")), db.clone()).await
}

/// One BIGINT (or INT) value per row of `sql`'s result.
async fn ints(db: &Arc<Database>, sql: &str) -> Vec<i64> {
    let response = autocommit(db, sql).await;
    assert_eq!(response.first(), Some(&2), "{sql}: {}", String::from_utf8_lossy(&response));
    crate::core::row::row::vec_into_rows(&response[1..])
        .unwrap()
        .iter()
        .map(|row| {
            let bytes = row.columns[0].data.into_slice();
            match bytes.len() {
                4 => i32::from_le_bytes(bytes[..4].try_into().unwrap()) as i64,
                _ => int(&row.columns[0]),
            }
        })
        .collect()
}

fn text(response: &[u8]) -> String {
    String::from_utf8_lossy(response).to_string()
}

fn row_count(response: &[u8]) -> u64 {
    match response.first() {
        Some(0) | Some(1) if response.len() >= 9 => u64::from_le_bytes(response[1..9].try_into().unwrap()),
        _ => panic!("not a row count: {}", text(response)),
    }
}

/// Runs `sql` in autocommit while a commit is parked in the window
/// ([`start_in_window`]), then lets the commit go on. Returns its response.
async fn in_window(
    db: &Arc<Database>,
    sql: &str,
    parked: (std::thread::JoinHandle<()>, mpsc::Sender<()>),
) -> Vec<u8> {
    let started = start_in_window(db, sql).await;
    release(parked, started).await
}

const PARENT_CHILD: [&str; 3] = [
    "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY)",
    "CREATE TABLE c (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT REFERENCES p (id))",
    "INSERT INTO p (id) VALUES (1), (2)",
];

/// A parent's DELETE has committed, recorded but not retired; a child
/// insert of that parent comes in the window. The DELETE held the parent
/// key's exclusive referenced-key lock: the insert waits for it to be
/// handed on, and its look under the lock then sees the parent gone.
///
/// Mutation: `acquire_referenced_waiting`'s `live` is the outcome alone (as
/// it was): the insert takes the lock as the DELETE's, looks at a snapshot
/// that still shows the parent, and stores an orphan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_child_insert_in_a_parent_deletes_commit_window_leaves_no_orphan() {
    let db = db_with(&PARENT_CHILD).await;
    let d = db.transactions.begin();
    in_block(&db, d, "DELETE FROM p WHERE id = 1").await;
    let parked = park_commit(&db, d);
    let response = in_window(&db, "INSERT INTO c (cid, pid) VALUES (10, 1)", parked).await;

    assert_eq!(ints(&db, "SELECT id FROM p").await, vec![2]);
    assert_eq!(
        ints(&db, "SELECT cid FROM c").await,
        Vec::<i64>::new(),
        "an orphan was stored; the insert said {}",
        text(&response)
    );
    assert_eq!(response.first(), Some(&3), "the insert of an orphan succeeded: {}", text(&response));
}

/// A child insert has committed, recorded but not retired; its parent's
/// DELETE comes in the window. The insert held the parent key's shared
/// referenced-key lock: the DELETE waits for it, and its look at the
/// referencing rows then finds the child, and refuses.
///
/// Mutation: as above: the DELETE takes the key as the insert's, finds no
/// child at a snapshot that cannot see the insert, and deletes the parent
/// under it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parent_delete_in_a_child_inserts_commit_window_leaves_no_orphan() {
    let db = db_with(&PARENT_CHILD).await;
    let i = db.transactions.begin();
    in_block(&db, i, "INSERT INTO c (cid, pid) VALUES (10, 1)").await;
    let parked = park_commit(&db, i);
    let response = in_window(&db, "DELETE FROM p WHERE id = 1", parked).await;

    assert_eq!(ints(&db, "SELECT cid FROM c").await, vec![10]);
    assert_eq!(
        ints(&db, "SELECT id FROM p ORDER BY id").await,
        vec![1, 2],
        "the referenced parent was deleted; the DELETE said {}",
        text(&response)
    );
}

/// A writer's UPDATE has committed, recorded but not retired; another
/// UPDATE of the same row comes in the window, meets the first one's
/// version and conflicts. The conflict waits for the first writer to be
/// retired and replays then, over its value, as `READ COMMITTED` does.
///
/// Mutation: `Pacing::pause` counts a blocker live by its outcome alone
/// (as it did): the replays run at once, inside the window, meet the same
/// version every time, and the statement gives up with 40001 although the
/// row was free a moment later. `wait_for` the same: the wait it begins
/// ends at once, inside the window (the replays then spin on the version
/// until the commit is retired).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflict_met_in_a_commit_window_waits_and_applies_over_the_commit() {
    let db = db_with(&[
        "CREATE TABLE n (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        "INSERT INTO n (id, v) VALUES (1, 1)",
    ])
    .await;
    let a = db.transactions.begin();
    in_block(&db, a, "UPDATE n SET v = v + 1 WHERE id = 1").await;
    let parked = park_commit(&db, a);
    let woken = |db: &Arc<Database>| {
        db.row_locks.conflict_waits.woken.iter().map(|w| w.load(std::sync::atomic::Ordering::Acquire)).sum::<usize>()
    };
    let before = woken(&db);
    let started = start_in_window(&db, "UPDATE n SET v = v + 10 WHERE id = 1").await;
    if started.1.is_none() {
        // The wait has begun; it must not end while A is still committing.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(woken(&db), before, "the conflict wait ended while its blocker's commit was not yet visible");
    }
    let response = release(parked, started).await;

    assert_ne!(response.first(), Some(&3), "the UPDATE failed: {}", text(&response));
    assert_eq!(row_count(&response), 1);
    assert_eq!(ints(&db, "SELECT v FROM n WHERE id = 1").await, vec![12]);
}

/// An insert of key 1 has committed, recorded but not retired; an upsert of
/// key 1 comes in the window. The key's row is there but no snapshot shows
/// it yet: the upsert waits for the insert to be retired, then updates the
/// row it conflicts with.
///
/// Mutation: the uniqueness check's in-flight rule is the creator's
/// outcome alone (as it was): the key reads as taken, the `DO UPDATE` reads
/// at a snapshot that cannot see the row, updates nothing, and the
/// statement reports `INSERT 0 0` with neither row written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upsert_in_an_inserts_commit_window_updates_the_row() {
    let db = db_with(&["CREATE TABLE u (k BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)"]).await;
    let a = db.transactions.begin();
    in_block(&db, a, "INSERT INTO u (k, v) VALUES (1, 1)").await;
    let parked = park_commit(&db, a);
    let response = in_window(
        &db,
        "INSERT INTO u (k, v) VALUES (1, 2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
        parked,
    )
    .await;

    assert_ne!(response.first(), Some(&3), "the upsert failed: {}", text(&response));
    assert_eq!(ints(&db, "SELECT v FROM u WHERE k = 1").await, vec![2], "the upsert said {}", text(&response));
    assert_eq!(row_count(&response), 1);
}

/// A DELETE of key 1 has committed, recorded but not retired; an insert of
/// key 1 comes in the window. The old row still shows to every snapshot
/// taken in the window, so the key is not free yet: the insert waits for the
/// DELETE to be retired, and no reader ever sees two rows of key 1.
///
/// Mutation: the uniqueness check counts a row whose deleter's outcome is
/// recorded as gone (as it did): the insert lands and commits inside the
/// window, and a reader there sees key 1 twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_insert_in_a_deletes_commit_window_never_shows_the_key_twice() {
    let db = db_with(&[
        "CREATE TABLE u (k BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        "INSERT INTO u (k, v) VALUES (1, 1)",
    ])
    .await;
    let a = db.transactions.begin();
    in_block(&db, a, "DELETE FROM u WHERE k = 1").await;
    let parked = park_commit(&db, a);
    let started = start_in_window(&db, "INSERT INTO u (k, v) VALUES (1, 2)").await;
    // Still inside the window, whichever way the insert went.
    let seen_in_window = ints(&db, "SELECT v FROM u WHERE k = 1").await;
    let response = release(parked, started).await;

    assert!(seen_in_window.len() <= 1, "key 1 was visible twice: {seen_in_window:?}");
    assert_ne!(response.first(), Some(&3), "the insert failed: {}", text(&response));
    assert_eq!(ints(&db, "SELECT v FROM u WHERE k = 1").await, vec![2]);
}

/// The holder's lock handover, across lock spaces. D deletes parent 1
/// (holding the key's exclusive referenced-key lock) and locks row 1 of `r`
/// `FOR UPDATE`; its commit is parked in the window. A `FOR UPDATE` of
/// `r`'s row 1 there must not count D as gone either: taking a dead
/// holder's row frees everything the holder holds, its referenced-key lock
/// included, and a child insert of parent 1 that came next found the key
/// free and the parent still showing.
///
/// Mutation: `acquire_row_lock`'s `live` is the outcome alone (as it was):
/// the `FOR UPDATE` takes D's row and with it D's referenced key, and the
/// child insert stores an orphan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn taking_a_committing_holders_row_frees_none_of_its_other_locks() {
    let mut statements = PARENT_CHILD.to_vec();
    statements.extend(["CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY)", "INSERT INTO r (id) VALUES (1)"]);
    let db = db_with(&statements).await;
    let d = db.transactions.begin();
    in_block(&db, d, "SELECT id FROM r WHERE id = 1 FOR UPDATE").await;
    in_block(&db, d, "DELETE FROM p WHERE id = 1").await;
    let parked = park_commit(&db, d);

    // Inside the window: a SKIP LOCKED claim of `r`, which never waits.
    let b = db.transactions.begin();
    let claimed = in_block(&db, b, "SELECT id FROM r FOR UPDATE SKIP LOCKED").await;
    let started = start_in_window(&db, "INSERT INTO c (cid, pid) VALUES (10, 1)").await;
    let response = release(parked, started).await;
    end(&db, b);

    assert_eq!(
        ints(&db, "SELECT cid FROM c").await,
        Vec::<i64>::new(),
        "an orphan was stored; the insert said {}; the claim got {}",
        text(&response),
        text(&claimed)
    );
}
