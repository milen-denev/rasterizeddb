//! Does a transaction id handed out by one run get handed out again by the
//! next?
//!
//! Ids used to resume at the highest *durable outcome* plus one. A writer whose
//! outcome never became durable — a rolled-back block, whose abort is recorded
//! in memory only, or a statement in flight when the process stopped — left
//! row versions on disk stamped with an id above that, so the next run issued
//! the same id to a new transaction. That transaction saw the old rows as its
//! own uncommitted writes (`xmin` is its id, the log says in progress), and its
//! commit committed them: a rolled-back `INSERT` came back after a clean
//! restart, reporting nothing.
//!
//! Every scenario ends the same way: reopen, run a committed write in a new
//! transaction (the step that adopted the old rows), and check that what was
//! rolled back, or never finished, is still not there.

use std::path::{Path, PathBuf};

use rclite::Arc;

use crate::core::database::Database;
use crate::core::mvcc::TxId;

const TABLE: &str = "reuse";

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

async fn open(dir: &Path) -> Arc<Database> {
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::new_named(&path_str(dir), false, "txreuse").await)
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&path_str(dir));
}

/// One autocommit statement, waiting for its sync. Panics on failure: a
/// scenario whose write did not happen tests nothing.
async fn run(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql)
        .unwrap_or_else(|| panic!("could not parse {sql}"));
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    assert!(
        !out.is_empty() && out[0] != 3,
        "{sql} failed: {}",
        String::from_utf8_lossy(&out)
    );
    out
}

/// One statement inside `tx`, which stays open.
async fn run_in(db: &Arc<Database>, tx: TxId, sql: &str) {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql)
        .unwrap_or_else(|| panic!("could not parse {sql}"));
    let out = Box::pin(crate::core::rql::executor::execute_with_context(
        purpose,
        db.clone(),
        0,
        tx,
    ))
    .await;
    assert!(
        !out.is_empty() && out[0] != 3,
        "{sql} in {tx} failed: {}",
        String::from_utf8_lossy(&out)
    );
}

/// Every `id` in the table, sorted.
async fn ids(db: &Arc<Database>) -> Vec<i64> {
    let out = run(db, &format!("SELECT id FROM {TABLE} WHERE id > 0")).await;
    assert_eq!(out.first(), Some(&2), "not a row set: {}", String::from_utf8_lossy(&out));
    let mut ids: Vec<i64> = crate::core::row::row::vec_into_rows(&out[1..])
        .expect("rows decode")
        .iter()
        .map(|r| {
            let b = r.columns[0].data.into_slice();
            i64::from_le_bytes(b.try_into().expect("a BIGINT is 8 bytes"))
        })
        .collect();
    ids.sort_unstable();
    ids
}

async fn create(db: &Arc<Database>) {
    run(db, &format!("CREATE TABLE {TABLE} (id BIGINT NOT NULL, n INTEGER NOT NULL)")).await;
}

async fn insert(db: &Arc<Database>, id: i64) {
    run(db, &format!("INSERT INTO {TABLE} (id, n) VALUES ({id}, 1)")).await;
}

async fn insert_in(db: &Arc<Database>, tx: TxId, id: i64) {
    run_in(db, tx, &format!("INSERT INTO {TABLE} (id, n) VALUES ({id}, 1)")).await;
}

/// Reopens `dir` and checks `want` is exactly the table, before and after a
/// committed write — the write is what used to commit the old rows.
async fn reopen_and_check(dir: &Path, want: &[i64], next: i64) -> Arc<Database> {
    let db = open(dir).await;
    assert_eq!(ids(&db).await, want, "the rows changed across the restart itself");
    insert(&db, next).await;
    let mut with_next = want.to_vec();
    with_next.push(next);
    with_next.sort_unstable();
    assert_eq!(
        ids(&db).await,
        with_next,
        "a write after the restart brought back rows that were never committed"
    );
    // And from a transaction of its own, after a commit: a reused id can be
    // any one of the next few, not only the first.
    let tx = db.transactions.begin();
    assert_eq!(ids(&db).await, with_next);
    db.commit_transaction(tx);
    db
}

/// The plain case: the last thing before a clean close is a rollback, so no
/// later commit carries its outcome to disk.
#[tokio::test]
async fn a_rolled_back_insert_stays_rolled_back_across_a_clean_restart() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    create(&db).await;
    insert(&db, 1).await;
    insert(&db, 2).await;

    let tx = db.transactions.begin();
    insert_in(&db, tx, 999).await;
    db.abort_transaction(tx);
    assert_eq!(ids(&db).await, vec![1, 2]);
    db.sync_durable().unwrap();
    close(&dir, db);

    let db = reopen_and_check(&dir, &[1, 2], 3).await;
    close(&dir, db);
}

/// A rollback that ends after an *older* transaction's commit: the commit's
/// outcome is durable and is the highest one, so the younger id is above it.
#[tokio::test]
async fn a_rollback_after_an_older_transactions_commit_stays_rolled_back() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    create(&db).await;
    insert(&db, 1).await;

    let older = db.transactions.begin();
    let younger = db.transactions.begin();
    assert!(younger > older);
    insert_in(&db, younger, 900).await;
    insert_in(&db, older, 2).await;
    db.commit_transaction(older);
    db.transactions.flush().unwrap();
    db.sync_durable().unwrap();
    insert_in(&db, younger, 901).await;
    db.abort_transaction(younger);
    assert_eq!(ids(&db).await, vec![1, 2]);
    close(&dir, db);

    let db = reopen_and_check(&dir, &[1, 2], 3).await;
    close(&dir, db);
}

/// A rollback followed by commits in other transactions. The commits carry the
/// abort's byte to disk today; kept so that stays true.
#[tokio::test]
async fn a_rollback_followed_by_commits_stays_rolled_back() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    create(&db).await;
    insert(&db, 1).await;

    let tx = db.transactions.begin();
    insert_in(&db, tx, 999).await;
    db.abort_transaction(tx);
    insert(&db, 2).await;
    insert(&db, 3).await;
    assert_eq!(ids(&db).await, vec![1, 2, 3]);
    close(&dir, db);

    let db = reopen_and_check(&dir, &[1, 2, 3], 4).await;
    close(&dir, db);
}

/// Many rollbacks in a row, across several restarts: every id of every one of
/// them has to stay retired, not only the first the next run would reach.
#[tokio::test]
async fn many_rolled_back_inserts_stay_rolled_back_across_restarts() {
    const ABORTS: i64 = 40;
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    create(&db).await;
    insert(&db, 1).await;
    close(&dir, db);

    let mut want = vec![1];
    for round in 0..3i64 {
        let db = open(&dir).await;
        for k in 0..ABORTS {
            let tx = db.transactions.begin();
            insert_in(&db, tx, 1000 * (round + 1) + k).await;
            db.abort_transaction(tx);
        }
        assert_eq!(ids(&db).await, want);
        close(&dir, db);

        let db = open(&dir).await;
        assert_eq!(ids(&db).await, want, "round {round}: the restart itself");
        // More committed writers than there were aborts, so every reused id
        // would have been handed out and committed.
        for k in 0..ABORTS + 5 {
            let id = 100 * (round + 1) + k;
            insert(&db, id).await;
            want.push(id);
        }
        want.sort_unstable();
        assert_eq!(ids(&db).await, want, "round {round}: rolled-back rows came back");
        close(&dir, db);
    }
}

/// A writer still in flight when the process stops leaves no outcome at all.
/// Dropping the database without ending the transaction is that, for the
/// transaction's bookkeeping: its rows are durable, nothing records how it
/// ended.
#[tokio::test]
async fn an_unfinished_insert_stays_invisible_after_the_process_stops() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    create(&db).await;
    insert(&db, 1).await;

    let tx = db.transactions.begin();
    insert_in(&db, tx, 777).await;
    // The rows themselves made durable, as a later commit's sync would have.
    db.sync_durable().unwrap();
    close(&dir, db);

    let db = reopen_and_check(&dir, &[1], 2).await;
    close(&dir, db);
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap().flatten() {
        let target = to.join(entry.file_name());
        let kind = entry.file_type().unwrap();
        if kind.is_dir() {
            copy_tree(&entry.path(), &target);
        } else if kind.is_file() {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// A crash image: the directory as it stands while a writer is in flight, with
/// no checkpoint, no close and no outcome, opened in place of the original.
/// The source is deleted first, so the image cannot reach it.
#[tokio::test]
async fn an_unfinished_insert_stays_invisible_in_a_crash_image() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let image: PathBuf = base.path().join("image");
    let db = open(&dir).await;
    create(&db).await;
    insert(&db, 1).await;
    insert(&db, 2).await;

    let tx = db.transactions.begin();
    insert_in(&db, tx, 555).await;
    insert_in(&db, tx, 556).await;
    db.sync_durable().unwrap();
    copy_tree(&dir, &image);
    // `db_storage.cfg` names absolute paths, which would send the image back
    // to the source directory. Only a pooled build writes one.
    #[cfg(feature = "storage_pools")]
    {
        let pool = image.join(crate::core::storage::POOL_FILE);
        if let Ok(text) = std::fs::read_to_string(&pool) {
            std::fs::write(&pool, text.replace(&path_str(&dir), &path_str(&image))).unwrap();
        }
    }
    db.abort_transaction(tx);
    close(&dir, db);
    std::fs::remove_dir_all(&dir).unwrap();

    let db = reopen_and_check(&image, &[1, 2], 3).await;
    close(&image, db);
}

/// The ids a restart skips must read as aborted, not as writers still in
/// flight. A row stamped by one — a rolled-back insert of a unique key — would
/// otherwise look like another transaction's unfinished write to every later
/// uniqueness check, and the key could never be inserted again: each attempt
/// reports a retryable conflict until it gives up.
#[tokio::test]
async fn a_rolled_back_unique_key_can_be_inserted_after_a_restart() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    run(&db, &format!("CREATE TABLE {TABLE} (id BIGINT NOT NULL UNIQUE, n INTEGER NOT NULL)")).await;
    insert(&db, 1).await;
    let tx = db.transactions.begin();
    insert_in(&db, tx, 42).await;
    db.abort_transaction(tx);
    db.sync_durable().unwrap();
    close(&dir, db);

    let db = open(&dir).await;
    let sql = format!("INSERT INTO {TABLE} (id, n) VALUES (42, 2)");
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(&sql).unwrap();
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    assert!(
        !out.is_empty() && out[0] != 3,
        "the rolled-back key blocks a new insert of it: {}",
        String::from_utf8_lossy(&out)
    );
    assert_eq!(ids(&db).await, vec![1, 42]);
    close(&dir, db);
}

// ---------------------------------------------------------------------------
// A reservation that cannot be written
// ---------------------------------------------------------------------------

/// One autocommit statement, waiting for its sync, whatever its outcome.
async fn try_run(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql)
        .unwrap_or_else(|| panic!("could not parse {sql}"));
    crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await
}

/// Uses up the reservation's block, small for the test, so the next `begin`
/// has to raise the mark; returns the mark.
fn exhaust_reservation(db: &Arc<Database>) -> TxId {
    db.transactions.set_reservation_block(4);
    while db.transactions.next_id() < db.transactions.reserved_ids_below() {
        let id = db.transactions.begin();
        db.abort_transaction(id);
    }
    db.transactions.reserved_ids_below()
}

/// Heals the injected failure however the test ends, so a failed assertion
/// cannot leave it armed for the next test of the directory (there is none,
/// the directory being the test's own, but a leak would still be wrong).
struct Healed(PathBuf);

impl Drop for Healed {
    fn drop(&mut self) {
        crate::core::mvcc::status::reservation_test_hooks::heal(&self.0);
    }
}

/// The mark cannot be raised (a full disk), and `begin` hands out ids above
/// it anyway so reads go on. A write under such an id is refused at commit,
/// in autocommit and at an explicit block's `COMMIT`, and rolled back: a
/// restart resumes at the mark, and the id handed out again would have taken
/// the committed rows for its own. Reads, autocommit and in a block, work
/// throughout. After a restart the rows are not there and ids resume above
/// every one handed out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_is_refused_while_its_id_cannot_be_reserved_and_reads_go_on() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    create(&db).await;
    insert(&db, 1).await;
    let mark = exhaust_reservation(&db);
    let _healed = Healed(dir.clone());
    crate::core::mvcc::status::reservation_test_hooks::fail_in(&dir);

    // Autocommit: refused, saying why, and as a full disk.
    let out = try_run(&db, &format!("INSERT INTO {TABLE} (id, n) VALUES (2, 1)")).await;
    let message = String::from_utf8_lossy(&out).into_owned();
    assert_eq!(out.first(), Some(&3), "a write under an unreserved id committed: {message}");
    assert!(
        message.contains("could not reserve transaction ids") && message.contains("(the disk is full)"),
        "{message}"
    );
    assert!(message.contains("db_transactions.next"), "the message does not name the file: {message}");

    // An explicit block: its statement runs, and its `COMMIT`, which pgwire
    // makes through `commit_written_transaction`, is refused.
    let tx = db.transactions.begin();
    assert!(tx >= mark, "the block's id {tx} is reserved, so this tests nothing");
    insert_in(&db, tx, 3).await;
    let refused = db
        .commit_written_transaction(tx)
        .expect_err("a block that wrote under an unreserved id committed");
    assert_eq!(refused.kind(), std::io::ErrorKind::StorageFull, "{refused}");
    db.abort_transaction(tx);
    db.transactions.flush().unwrap();

    // Reads go on: an autocommit SELECT, and a block that only read, which
    // commits without asking (it wrote nothing that carries its id).
    assert_eq!(ids(&db).await, vec![1]);
    let reader = db.transactions.begin();
    run_in(&db, reader, &format!("SELECT id FROM {TABLE}")).await;
    assert!(!db.transactions.wrote_family(reader));
    db.commit_transaction(reader);

    let issued = db.transactions.next_id();
    db.transactions.flush().unwrap();
    close(&dir, db);
    crate::core::mvcc::status::reservation_test_hooks::heal(&dir);

    let db = open(&dir).await;
    let next = db.transactions.begin();
    db.abort_transaction(next);
    assert!(next >= issued, "id {next} handed out again after the restart (ids below {issued} were issued)");
    assert_eq!(ids(&db).await, vec![1], "a refused write is there after the restart");
    insert(&db, 4).await;
    assert_eq!(ids(&db).await, vec![1, 4]);
    close(&dir, db);
}

/// Once the reservation can be written again, the next `begin` raises the
/// mark and writes commit. A block that began while it could not commits as
/// well: the raise put the mark above its id, so a restart cannot reissue it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_commit_again_once_the_reservation_can_be_written() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir).await;
    create(&db).await;
    insert(&db, 1).await;
    let mark = exhaust_reservation(&db);
    let _healed = Healed(dir.clone());
    crate::core::mvcc::status::reservation_test_hooks::fail_in(&dir);

    let out = try_run(&db, &format!("INSERT INTO {TABLE} (id, n) VALUES (2, 1)")).await;
    assert_eq!(out.first(), Some(&3), "the failure was not injected: {}", String::from_utf8_lossy(&out));
    let early = db.transactions.begin();
    assert!(early >= mark);
    insert_in(&db, early, 5).await;
    assert_eq!(db.transactions.reserved_ids_below(), mark, "the mark moved while writes failed");

    crate::core::mvcc::status::reservation_test_hooks::heal(&dir);
    // The first statement after the disk recovers raises the mark and commits.
    let out = try_run(&db, &format!("INSERT INTO {TABLE} (id, n) VALUES (3, 1)")).await;
    assert_ne!(
        out.first(),
        Some(&3),
        "a write was refused after the reservation could be written again: {}",
        String::from_utf8_lossy(&out)
    );
    assert!(db.transactions.reserved_ids_below() > early, "the mark is not above the early block");
    db.commit_written_transaction(early)
        .expect("a block below the raised mark was refused");
    db.transactions.flush_deferred().unwrap();
    db.sync_own_location().unwrap();
    assert_eq!(ids(&db).await, vec![1, 3, 5]);

    let issued = db.transactions.next_id();
    db.transactions.flush().unwrap();
    close(&dir, db);
    let db = open(&dir).await;
    let next = db.transactions.begin();
    db.abort_transaction(next);
    assert!(next >= issued, "id {next} handed out again after the restart");
    assert_eq!(ids(&db).await, vec![1, 3, 5]);
    close(&dir, db);
}
