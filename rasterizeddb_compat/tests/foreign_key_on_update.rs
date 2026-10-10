//! `ON UPDATE` foreign-key actions.
//!
//! They were parsed, stored and never enforced: changing a parent's key left
//! every child pointing at a value no parent held, with one session and no
//! error. Each action is asserted here with PostgreSQL's semantics, and then
//! against a second session in the style of `foreign_key_concurrency.rs`.

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

/// `(id, pid)` of every child, sorted; a NULL pid reads as -1.
async fn children(conn: &mut sqlx::PgConnection) -> Vec<(i32, i32)> {
    let rows = sqlx::query(AssertSqlSafe("SELECT id, pid FROM c".to_string()))
        .fetch_all(&mut *conn)
        .await
        .expect("select");
    let mut out: Vec<(i32, i32)> = rows
        .iter()
        .map(|r| (r.get::<i32, _>(0), r.get::<Option<i32>, _>(1).unwrap_or(-1)))
        .collect();
    out.sort();
    out
}

async fn parents(conn: &mut sqlx::PgConnection) -> Vec<i32> {
    let rows = sqlx::query(AssertSqlSafe("SELECT id FROM p".to_string()))
        .fetch_all(&mut *conn)
        .await
        .expect("select");
    let mut out: Vec<i32> = rows.iter().map(|r| r.get::<i32, _>(0)).collect();
    out.sort();
    out
}

/// Parents 0, 1, 2; children 10 -> 1 and 20 -> 2.
async fn schema(server: &TestServer, action: &str, child_default: Option<i32>) -> sqlx::PgConnection {
    let mut conn = connect(server).await;
    exec(&mut conn, "CREATE TABLE p (id INT PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .expect("parent");
    let default = child_default.map(|d| format!(" DEFAULT {}", d)).unwrap_or_default();
    exec(
        &mut conn,
        &format!(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT{}, \
             CONSTRAINT fk_c FOREIGN KEY (pid) REFERENCES p (id) ON UPDATE {})",
            default, action
        ),
    )
    .await
    .expect("child");
    exec(&mut conn, "INSERT INTO p (id, name) VALUES (0, 'z'), (1, 'a'), (2, 'b')")
        .await
        .expect("parents");
    exec(&mut conn, "INSERT INTO c (id, pid) VALUES (10, 1), (20, 2)")
        .await
        .expect("children");
    conn
}

async fn assert_no_orphan(conn: &mut sqlx::PgConnection) {
    let parents = parents(conn).await;
    for (id, pid) in children(conn).await {
        assert!(
            pid == -1 || parents.contains(&pid),
            "orphan: child {} references {} but parents are {:?}",
            id,
            pid,
            parents
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn no_action_refuses_moving_a_referenced_key() {
    let server = TestServer::start().await.expect("server");
    let mut conn = schema(&server, "NO ACTION", None).await;
    let err = exec(&mut conn, "UPDATE p SET id = 5 WHERE id = 1")
        .await
        .expect_err("child 10 references 1");
    assert!(err.contains("fk_c"), "{}", err);
    assert_eq!(parents(&mut conn).await, vec![0, 1, 2]);
    assert_no_orphan(&mut conn).await;

    // An unreferenced key moves freely, and a referenced one may be "moved" to
    // itself or have its other columns changed.
    exec(&mut conn, "UPDATE p SET id = 7 WHERE id = 0").await.expect("unreferenced");
    exec(&mut conn, "UPDATE p SET id = 1 WHERE id = 1").await.expect("to itself");
    exec(&mut conn, "UPDATE p SET name = 'x' WHERE id = 1").await.expect("non-key");
    assert_eq!(parents(&mut conn).await, vec![1, 2, 7]);
}

#[tokio::test(flavor = "multi_thread")]
async fn restrict_refuses_moving_a_referenced_key() {
    let server = TestServer::start().await.expect("server");
    let mut conn = schema(&server, "RESTRICT", None).await;
    let err = exec(&mut conn, "UPDATE p SET id = 5 WHERE id IN (1, 2)")
        .await
        .expect_err("RESTRICT refuses any referenced key changing");
    assert!(err.contains("fk_c"), "{}", err);
    assert_eq!(parents(&mut conn).await, vec![0, 1, 2]);
    exec(&mut conn, "UPDATE p SET id = 5 WHERE id = 0").await.expect("unreferenced");
}

#[tokio::test(flavor = "multi_thread")]
async fn cascade_carries_the_children_along() {
    let server = TestServer::start().await.expect("server");
    let mut conn = schema(&server, "CASCADE", None).await;
    exec(&mut conn, "UPDATE p SET id = 5 WHERE id = 1").await.expect("cascade");
    assert_eq!(parents(&mut conn).await, vec![0, 2, 5]);
    assert_eq!(children(&mut conn).await, vec![(10, 5), (20, 2)]);

    // Every row of a multi-row update, each to its own new key.
    exec(&mut conn, "UPDATE p SET id = id + 100 WHERE id >= 2").await.expect("multi");
    assert_eq!(parents(&mut conn).await, vec![0, 102, 105]);
    assert_eq!(children(&mut conn).await, vec![(10, 105), (20, 102)]);

    // Chained pairs — 105 moves to 106 while 0 moves to 105 — are applied to
    // the children as they were, not one after the other.
    exec(&mut conn, "INSERT INTO c (id, pid) VALUES (30, 0)").await.expect("child of 0");
    exec(&mut conn, "UPDATE p SET id = 106 WHERE id = 105").await.expect("free 105");
    exec(&mut conn, "UPDATE p SET id = CASE WHEN id = 0 THEN 105 ELSE id + 1 END WHERE id IN (0, 106)")
        .await
        .expect("chained");
    assert_eq!(parents(&mut conn).await, vec![102, 105, 107]);
    assert_eq!(children(&mut conn).await, vec![(10, 107), (20, 102), (30, 105)]);
    assert_no_orphan(&mut conn).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn set_null_and_set_default() {
    let server = TestServer::start().await.expect("server");
    let mut conn = schema(&server, "SET NULL", None).await;
    exec(&mut conn, "UPDATE p SET id = 5 WHERE id = 1").await.expect("set null");
    assert_eq!(children(&mut conn).await, vec![(10, -1), (20, 2)]);

    let server = TestServer::start().await.expect("server");
    let mut conn = schema(&server, "SET DEFAULT", Some(0)).await;
    exec(&mut conn, "UPDATE p SET id = 5 WHERE id = 2").await.expect("set default");
    assert_eq!(children(&mut conn).await, vec![(10, 1), (20, 0)]);
    assert_no_orphan(&mut conn).await;
}

/// A cascade into a table that is itself referenced cascades on.
#[tokio::test(flavor = "multi_thread")]
async fn a_cascade_propagates_through_a_chain() {
    let server = TestServer::start().await.expect("server");
    let mut conn = connect(&server).await;
    exec(&mut conn, "CREATE TABLE a (id INT PRIMARY KEY)").await.expect("a");
    exec(
        &mut conn,
        "CREATE TABLE b (id INT PRIMARY KEY, aid INT, \
         CONSTRAINT fk_b FOREIGN KEY (aid) REFERENCES a (id) ON UPDATE CASCADE)",
    )
    .await
    .expect("b");
    exec(
        &mut conn,
        "CREATE TABLE c2 (id INT PRIMARY KEY, bid INT, \
         CONSTRAINT fk_c2 FOREIGN KEY (bid) REFERENCES b (aid) ON UPDATE CASCADE)",
    )
    .await
    .expect("c2");
    exec(&mut conn, "INSERT INTO a (id) VALUES (1)").await.expect("a row");
    exec(&mut conn, "INSERT INTO b (id, aid) VALUES (1, 1)").await.expect("b row");
    exec(&mut conn, "INSERT INTO c2 (id, bid) VALUES (1, 1)").await.expect("c2 row");
    exec(&mut conn, "UPDATE a SET id = 9 WHERE id = 1").await.expect("cascade");
    let bid: i32 = sqlx::query("SELECT bid FROM c2")
        .fetch_one(&mut conn)
        .await
        .expect("c2")
        .get(0);
    assert_eq!(bid, 9, "the grandchild followed");
}

async fn start_waiting(
    mut conn: sqlx::PgConnection,
    sql: &'static str,
) -> (tokio::task::JoinHandle<(sqlx::PgConnection, Result<(), String>)>, bool) {
    let handle = tokio::spawn(async move {
        let outcome = exec(&mut conn, sql).await;
        (conn, outcome)
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    let waited = !handle.is_finished();
    (handle, waited)
}

/// A child inserted, not yet committed, against the key an UPDATE moves.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_update_waits_for_a_pending_child_and_then_refuses() {
    let server = TestServer::start().await.expect("server");
    let _ = schema(&server, "NO ACTION", None).await;
    let mut inserter = connect(&server).await;
    exec(&mut inserter, "BEGIN").await.expect("begin");
    exec(&mut inserter, "INSERT INTO c (id, pid) VALUES (11, 0)").await.expect("child of 0");

    let updater = connect(&server).await;
    let (pending, waited) = start_waiting(updater, "UPDATE p SET id = 9 WHERE id = 0").await;
    exec(&mut inserter, "COMMIT").await.expect("commit");
    let (_c, outcome) = pending.await.expect("join");
    assert_no_orphan(&mut inserter).await;
    assert!(outcome.is_err(), "a child of 0 committed; moving 0 must fail");
    assert!(waited, "the UPDATE should have waited for the child's transaction");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cascading_key_update_waits_and_carries_the_new_child() {
    let server = TestServer::start().await.expect("server");
    let _ = schema(&server, "CASCADE", None).await;
    let mut inserter = connect(&server).await;
    exec(&mut inserter, "BEGIN").await.expect("begin");
    exec(&mut inserter, "INSERT INTO c (id, pid) VALUES (11, 0)").await.expect("child of 0");

    let updater = connect(&server).await;
    let (pending, waited) = start_waiting(updater, "UPDATE p SET id = 9 WHERE id = 0").await;
    exec(&mut inserter, "COMMIT").await.expect("commit");
    let (_c, outcome) = pending.await.expect("join");
    assert_no_orphan(&mut inserter).await;
    outcome.expect("cascade");
    assert!(children(&mut inserter).await.contains(&(11, 9)));
    assert!(waited);
}

/// The other order: the key update first, the child insert of the old key waits
/// and then fails.
#[tokio::test(flavor = "multi_thread")]
async fn a_child_insert_waits_for_a_pending_key_update_and_then_fails() {
    let server = TestServer::start().await.expect("server");
    let _ = schema(&server, "NO ACTION", None).await;
    let mut updater = connect(&server).await;
    exec(&mut updater, "BEGIN").await.expect("begin");
    exec(&mut updater, "UPDATE p SET id = 9 WHERE id = 0").await.expect("move 0");

    let inserter = connect(&server).await;
    let (pending, waited) = start_waiting(inserter, "INSERT INTO c (id, pid) VALUES (11, 0)").await;
    exec(&mut updater, "COMMIT").await.expect("commit");
    let (_c, outcome) = pending.await.expect("join");
    assert_no_orphan(&mut updater).await;
    assert!(outcome.is_err(), "0 is gone");
    assert!(waited);
}
