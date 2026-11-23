//! A foreign key must hold against two sessions, not only against one.
//!
//! The child-side check used to be a plain `SELECT` of the parent, and the
//! parent-side check a plain `SELECT` of the children, each through the
//! checking transaction's own snapshot and neither taking a lock. So an
//! `INSERT` of a child and a `DELETE` of its parent, run concurrently, each saw
//! the world as it was before the other — and both committed, leaving a child
//! that references nothing. PostgreSQL closes this with `FOR KEY SHARE` on the
//! referenced row: the second of the two waits for the first and then either
//! proceeds or fails. Never both.
//!
//! Every test here interleaves explicit transactions deterministically: one
//! session holds its half open, the other is started and must *wait*, then the
//! first commits and the second's outcome is asserted. The final assertion in
//! each is the invariant itself — no committed child without its parent.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use std::time::Duration;

async fn connect(server: &TestServer) -> sqlx::PgConnection {
    sqlx::PgConnection::connect(&server.url())
        .await
        .expect("connect")
}

async fn exec(conn: &mut sqlx::PgConnection, sql: &str) -> Result<(), String> {
    conn.execute(AssertSqlSafe(sql.to_string()))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

async fn ids(conn: &mut sqlx::PgConnection, sql: &str) -> Vec<i32> {
    let rows = sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(&mut *conn)
        .await
        .expect("select");
    // NULLs reference nothing, so they are left out.
    let mut out: Vec<i32> = rows
        .iter()
        .filter_map(|r| r.get::<Option<i32>, _>(0))
        .collect();
    out.sort();
    out
}

/// `p` with one row, and `c` referencing it under `action`. Nothing in `c`.
async fn schema(server: &TestServer, action: &str) {
    let mut conn = connect(server).await;
    exec(&mut conn, "CREATE TABLE p (id INT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .expect("parent");
    exec(
        &mut conn,
        &format!(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT, \
             CONSTRAINT fk_c FOREIGN KEY (pid) REFERENCES p (id) ON DELETE {})",
            action
        ),
    )
    .await
    .expect("child");
    exec(&mut conn, "INSERT INTO p (id, name) VALUES (1, 'a')")
        .await
        .expect("parent row");
}

/// The invariant: every committed child's parent is committed too.
async fn assert_no_orphan(server: &TestServer) {
    let mut conn = connect(server).await;
    let parents = ids(&mut conn, "SELECT id FROM p").await;
    let children = ids(&mut conn, "SELECT pid FROM c").await;
    for pid in &children {
        assert!(
            parents.contains(pid),
            "orphan: child references parent {} but parents are {:?} — the INSERT and the \
             DELETE both committed",
            pid,
            parents
        );
    }
}

/// Starts `sql` on `conn` in the background, and reports whether it was still
/// running after a pause — it should be, waiting for the other session.
///
/// Reported rather than asserted here, so that on a build that does not wait
/// the test goes on to show what not waiting costs: the orphan.
async fn start_waiting(
    mut conn: sqlx::PgConnection,
    sql: &'static str,
) -> (
    tokio::task::JoinHandle<(sqlx::PgConnection, Result<(), String>)>,
    bool,
) {
    let handle = tokio::spawn(async move {
        let outcome = exec(&mut conn, sql).await;
        (conn, outcome)
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let waited = !handle.is_finished();
    (handle, waited)
}

fn assert_waited(waited: bool, sql: &str) {
    assert!(
        waited,
        "`{}` finished while the other session's transaction was still open; it should have \
         waited for it",
        sql
    );
}

/// Delete first, insert second: the insert waits, then fails because the
/// parent it waited for is gone.
#[tokio::test(flavor = "multi_thread")]
async fn an_insert_waits_for_a_pending_parent_delete_and_then_fails() {
    let server = TestServer::start().await.expect("server");
    schema(&server, "NO ACTION").await;

    let mut deleter = connect(&server).await;
    exec(&mut deleter, "BEGIN").await.expect("begin");
    exec(&mut deleter, "DELETE FROM p WHERE id = 1")
        .await
        .expect("delete");

    let inserter = connect(&server).await;
    let (pending, waited) = start_waiting(inserter, "INSERT INTO c (id, pid) VALUES (10, 1)").await;

    exec(&mut deleter, "COMMIT").await.expect("commit");
    let (_conn, outcome) = pending.await.expect("join");
    // The invariant first: on a build that does not wait, this is the failure.
    assert_no_orphan(&server).await;
    let err = outcome.expect_err("the parent was deleted; the insert must not succeed");
    assert!(err.contains("foreign key"), "{}", err);

    assert_waited(waited, "the second statement");
}

/// Delete first and rolled back: the waiting insert then succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn an_insert_proceeds_when_the_pending_delete_rolls_back() {
    let server = TestServer::start().await.expect("server");
    schema(&server, "NO ACTION").await;

    let mut deleter = connect(&server).await;
    exec(&mut deleter, "BEGIN").await.expect("begin");
    exec(&mut deleter, "DELETE FROM p WHERE id = 1")
        .await
        .expect("delete");

    let inserter = connect(&server).await;
    let (pending, waited) = start_waiting(inserter, "INSERT INTO c (id, pid) VALUES (10, 1)").await;

    exec(&mut deleter, "ROLLBACK").await.expect("rollback");
    let (mut conn, outcome) = pending.await.expect("join");
    // The invariant first: on a build that does not wait, this is the failure.
    assert_no_orphan(&server).await;
    outcome.expect("the parent survived, so the insert is valid");
    assert_eq!(ids(&mut conn, "SELECT pid FROM c").await, vec![1]);
    assert_waited(waited, "the second statement");
}

/// Insert first, delete second (NO ACTION): the delete waits, then fails
/// because the child it waited for now exists.
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_waits_for_a_pending_child_insert_and_then_fails() {
    let server = TestServer::start().await.expect("server");
    schema(&server, "NO ACTION").await;

    let mut inserter = connect(&server).await;
    exec(&mut inserter, "BEGIN").await.expect("begin");
    exec(&mut inserter, "INSERT INTO c (id, pid) VALUES (10, 1)")
        .await
        .expect("insert");

    let deleter = connect(&server).await;
    let (pending, waited) = start_waiting(deleter, "DELETE FROM p WHERE id = 1").await;

    exec(&mut inserter, "COMMIT").await.expect("commit");
    let (_conn, outcome) = pending.await.expect("join");
    // The invariant first: on a build that does not wait, this is the failure.
    assert_no_orphan(&server).await;
    let err = outcome.expect_err("a child now references the parent");
    assert!(err.contains("foreign key"), "{}", err);

    assert_waited(waited, "the second statement");
}

/// Insert first, delete second (CASCADE): the delete waits, then takes the
/// newly committed child with it.
#[tokio::test(flavor = "multi_thread")]
async fn a_cascading_delete_waits_and_then_removes_the_new_child() {
    let server = TestServer::start().await.expect("server");
    schema(&server, "CASCADE").await;

    let mut inserter = connect(&server).await;
    exec(&mut inserter, "BEGIN").await.expect("begin");
    exec(&mut inserter, "INSERT INTO c (id, pid) VALUES (10, 1)")
        .await
        .expect("insert");

    let deleter = connect(&server).await;
    let (pending, waited) = start_waiting(deleter, "DELETE FROM p WHERE id = 1").await;

    exec(&mut inserter, "COMMIT").await.expect("commit");
    let (mut conn, outcome) = pending.await.expect("join");
    // The invariant first: on a build that does not wait, this is the failure.
    assert_no_orphan(&server).await;
    outcome.expect("a cascade has nothing to refuse");
    assert_eq!(ids(&mut conn, "SELECT id FROM p").await, Vec::<i32>::new());
    assert_eq!(ids(&mut conn, "SELECT id FROM c").await, Vec::<i32>::new());
    assert_waited(waited, "the second statement");
}

/// A `REPEATABLE READ` deleter's snapshot predates a child committed since. Its
/// check must not answer from that snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn a_repeatable_read_delete_sees_a_child_committed_after_its_snapshot() {
    let server = TestServer::start().await.expect("server");
    schema(&server, "NO ACTION").await;

    let mut deleter = connect(&server).await;
    exec(&mut deleter, "BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin");
    // Establishes the snapshot.
    assert_eq!(ids(&mut deleter, "SELECT id FROM p").await, vec![1]);

    let mut inserter = connect(&server).await;
    exec(&mut inserter, "INSERT INTO c (id, pid) VALUES (10, 1)")
        .await
        .expect("insert commits");

    let outcome = exec(&mut deleter, "DELETE FROM p WHERE id = 1").await;
    let _ = exec(&mut deleter, "COMMIT").await;
    let _ = exec(&mut deleter, "ROLLBACK").await;
    assert!(outcome.is_err(), "the delete must not succeed over a committed child");

    assert_no_orphan(&server).await;
}

/// A `REPEATABLE READ` inserter's snapshot predates the parent's deletion.
#[tokio::test(flavor = "multi_thread")]
async fn a_repeatable_read_insert_sees_a_parent_deleted_after_its_snapshot() {
    let server = TestServer::start().await.expect("server");
    schema(&server, "NO ACTION").await;

    let mut inserter = connect(&server).await;
    exec(&mut inserter, "BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin");
    assert_eq!(ids(&mut inserter, "SELECT id FROM p").await, vec![1]);

    let mut deleter = connect(&server).await;
    exec(&mut deleter, "DELETE FROM p WHERE id = 1")
        .await
        .expect("delete commits");

    let outcome = exec(&mut inserter, "INSERT INTO c (id, pid) VALUES (10, 1)").await;
    let commit = exec(&mut inserter, "COMMIT").await;
    assert!(
        outcome.is_err() || commit.is_err(),
        "the insert must not commit against a deleted parent"
    );

    assert_no_orphan(&server).await;
}

/// Two parent columns referenced by two different foreign keys. A delete used
/// to collect the values of only the first referenced column and match every
/// child table against them.
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_checks_each_referenced_column_with_its_own_values() {
    let server = TestServer::start().await.expect("server");
    let mut conn = connect(&server).await;
    exec(
        &mut conn,
        "CREATE TABLE p2 (id INT PRIMARY KEY, alt INT UNIQUE NOT NULL)",
    )
    .await
    .expect("parent");
    exec(
        &mut conn,
        "CREATE TABLE by_id (id INT PRIMARY KEY, pid INT, \
         CONSTRAINT fk_by_id FOREIGN KEY (pid) REFERENCES p2 (id) ON DELETE CASCADE)",
    )
    .await
    .expect("child by id");
    exec(
        &mut conn,
        "CREATE TABLE by_alt (id INT PRIMARY KEY, palt INT, \
         CONSTRAINT fk_by_alt FOREIGN KEY (palt) REFERENCES p2 (alt) ON DELETE CASCADE)",
    )
    .await
    .expect("child by alt");
    // Parent 1 has alt 2 and parent 2 has alt 1, so the two columns' values
    // overlap and a mix-up deletes the wrong children rather than erroring.
    exec(&mut conn, "INSERT INTO p2 (id, alt) VALUES (1, 2)").await.expect("p 1");
    exec(&mut conn, "INSERT INTO p2 (id, alt) VALUES (2, 1)").await.expect("p 2");
    exec(&mut conn, "INSERT INTO by_id (id, pid) VALUES (100, 1)").await.expect("c");
    exec(&mut conn, "INSERT INTO by_id (id, pid) VALUES (101, 2)").await.expect("c");
    exec(&mut conn, "INSERT INTO by_alt (id, palt) VALUES (200, 2)").await.expect("c");
    exec(&mut conn, "INSERT INTO by_alt (id, palt) VALUES (201, 1)").await.expect("c");

    // Deletes parent 1, which is (id 1, alt 2): children 100 and 200 go.
    exec(&mut conn, "DELETE FROM p2 WHERE id = 1").await.expect("delete");

    assert_eq!(ids(&mut conn, "SELECT id FROM p2").await, vec![2]);
    assert_eq!(ids(&mut conn, "SELECT id FROM by_id").await, vec![101]);
    assert_eq!(ids(&mut conn, "SELECT id FROM by_alt").await, vec![201]);
}

/// Child inserts that reference the same parent share its lock: neither waits
/// for the other.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_children_of_one_parent_do_not_block_each_other() {
    let server = TestServer::start().await.expect("server");
    schema(&server, "NO ACTION").await;

    let mut a = connect(&server).await;
    let mut b = connect(&server).await;
    exec(&mut a, "BEGIN").await.expect("begin");
    exec(&mut b, "BEGIN").await.expect("begin");
    exec(&mut a, "INSERT INTO c (id, pid) VALUES (10, 1)").await.expect("a");
    tokio::time::timeout(
        Duration::from_secs(3),
        exec(&mut b, "INSERT INTO c (id, pid) VALUES (11, 1)"),
    )
    .await
    .expect("b must not wait for a")
    .expect("b");
    exec(&mut a, "COMMIT").await.expect("commit a");
    exec(&mut b, "COMMIT").await.expect("commit b");

    // And an UPDATE of the parent that leaves its key alone does not wait for
    // a pending child either.
    exec(&mut a, "BEGIN").await.expect("begin");
    exec(&mut a, "INSERT INTO c (id, pid) VALUES (12, 1)").await.expect("a");
    tokio::time::timeout(
        Duration::from_secs(3),
        exec(&mut b, "UPDATE p SET name = 'b' WHERE id = 1"),
    )
    .await
    .expect("a non-key update must not wait for a child insert")
    .expect("update");
    exec(&mut a, "COMMIT").await.expect("commit a");
    assert_eq!(ids(&mut b, "SELECT id FROM c").await, vec![10, 11, 12]);
}

/// A multi-row INSERT checks its tuples' keys together. It must still refuse
/// exactly what the row-by-row check refused, and accept what it accepted.
#[tokio::test(flavor = "multi_thread")]
async fn a_multi_row_insert_checks_every_tuple() {
    let server = TestServer::start().await.expect("server");
    schema(&server, "NO ACTION").await;
    let mut conn = connect(&server).await;
    exec(&mut conn, "INSERT INTO p (id, name) VALUES (2, 'b'), (3, 'c')")
        .await
        .expect("parents");

    // Repeated keys, NULLs and every parent: fine.
    exec(
        &mut conn,
        "INSERT INTO c (id, pid) VALUES (10, 1), (11, 2), (12, 2), (13, NULL), (14, 3)",
    )
    .await
    .expect("every key exists");

    // One missing key among present ones is still refused, by name.
    let err = exec(
        &mut conn,
        "INSERT INTO c (id, pid) VALUES (20, 1), (21, 99), (22, 3)",
    )
    .await
    .expect_err("99 has no parent");
    assert!(err.contains("fk_c") && err.contains("99"), "{}", err);
    assert!(
        !ids(&mut conn, "SELECT id FROM c").await.contains(&21),
        "the orphan was not written"
    );
    assert_no_orphan(&server).await;
}

/// A tuple may reference a row an earlier tuple of the same statement inserted
/// — a tree loaded parent-first. The batched probe cannot see it yet, and must
/// leave it to the per-row check rather than refuse it.
#[tokio::test(flavor = "multi_thread")]
async fn a_multi_row_insert_may_reference_its_own_earlier_tuples() {
    let server = TestServer::start().await.expect("server");
    let mut conn = connect(&server).await;
    exec(
        &mut conn,
        "CREATE TABLE tree (id INT PRIMARY KEY, up INT, \
         CONSTRAINT fk_up FOREIGN KEY (up) REFERENCES tree (id))",
    )
    .await
    .expect("create");
    exec(
        &mut conn,
        "INSERT INTO tree (id, up) VALUES (1, NULL), (2, 1), (3, 2), (4, 2)",
    )
    .await
    .expect("parent-first load");
    assert_eq!(ids(&mut conn, "SELECT id FROM tree").await, vec![1, 2, 3, 4]);
}

/// The batched check takes the same key-share locks: a delete of a parent the
/// batch referenced waits for it.
#[tokio::test(flavor = "multi_thread")]
async fn a_multi_row_insert_holds_its_parents_against_a_delete() {
    let server = TestServer::start().await.expect("server");
    schema(&server, "NO ACTION").await;
    let mut inserter = connect(&server).await;
    exec(&mut inserter, "INSERT INTO p (id, name) VALUES (2, 'b')")
        .await
        .expect("parent");
    exec(&mut inserter, "BEGIN").await.expect("begin");
    exec(
        &mut inserter,
        "INSERT INTO c (id, pid) VALUES (10, 1), (11, 2), (12, 1)",
    )
    .await
    .expect("insert");

    let deleter = connect(&server).await;
    let (pending, waited) = start_waiting(deleter, "DELETE FROM p WHERE id = 2").await;
    exec(&mut inserter, "COMMIT").await.expect("commit");
    let (_conn, outcome) = pending.await.expect("join");
    assert_no_orphan(&server).await;
    let err = outcome.expect_err("a child now references the parent");
    assert!(err.contains("foreign key"), "{}", err);
    assert_waited(waited, "the second statement");
}

/// A parent row committed while a DELETE that matches it is waiting on a
/// key lock. The DELETE's snapshot predates that row, so — as in PostgreSQL —
/// it must not delete it. It used to: the keys whose children were handled were
/// read under one snapshot and the delete scanned under a later one, so the new
/// parent was deleted and the child committed against it was orphaned.
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_never_removes_a_parent_it_did_not_check() {
    let server = TestServer::start().await.expect("server");
    schema(&server, "CASCADE").await;

    // Holds the key-share lock on parent 1, so the DELETE below waits.
    let mut holder = connect(&server).await;
    exec(&mut holder, "BEGIN").await.expect("begin");
    exec(&mut holder, "INSERT INTO c (id, pid) VALUES (10, 1)")
        .await
        .expect("child of 1");

    let deleter = connect(&server).await;
    let (pending, waited) = start_waiting(deleter, "DELETE FROM p WHERE id >= 1").await;

    // While it waits: a new parent, and a child of it, both committed.
    let mut other = connect(&server).await;
    exec(&mut other, "INSERT INTO p (id, name) VALUES (2, 'b')")
        .await
        .expect("new parent");
    exec(&mut other, "INSERT INTO c (id, pid) VALUES (20, 2)")
        .await
        .expect("child of the new parent");

    exec(&mut holder, "COMMIT").await.expect("commit");
    let (_conn, outcome) = pending.await.expect("join");
    assert_no_orphan(&server).await;
    outcome.expect("the cascade has nothing to refuse");
    assert_eq!(ids(&mut other, "SELECT id FROM p").await, vec![2]);
    assert_eq!(ids(&mut other, "SELECT id FROM c").await, vec![20]);
    assert_waited(waited, "the DELETE");
}
