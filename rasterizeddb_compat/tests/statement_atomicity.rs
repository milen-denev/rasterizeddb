//! A statement that fails part-way leaves nothing behind: not its rows, not
//! its transaction, not its locks.
//!
//! "Its transaction" is the part that used to go wrong. A statement that ended
//! any way other than through the executor's own error path — a data-modifying
//! CTE whose body failed, a panic, a client gone with a block open — left its
//! transaction `InProgress` in the running set. Nothing ever retired it, so
//! `oldest_active`, and with it every vacuum and snapshot horizon, stayed
//! pinned below it for the life of the process. Each test here ends by
//! checking that nothing is left running.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use std::time::{Duration, Instant};

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

async fn count(conn: &mut sqlx::PgConnection, sql: &str) -> i64 {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(&mut *conn)
        .await
        .expect("select")
        .len() as i64
}

/// Nothing is running once every session is idle: no statement left its
/// transaction behind.
async fn assert_nothing_running(server: &TestServer, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let transactions = &server.database().transactions;
    while !transactions.quiescent() {
        assert!(
            Instant::now() < deadline,
            "{}: a transaction is still running (oldest_active {}, next {}) — it holds back \
             every horizon for the life of the process",
            what,
            transactions.oldest_active(),
            transactions.next_id()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(server.database().row_locks.len(), 0, "{}: locks left behind", what);
}

fn values(from: i32, to: i32, dup_at: Option<i32>) -> String {
    (from..to)
        .map(|i| {
            let id = if Some(i) == dup_at { from } else { i };
            format!("({}, {})", id, i)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

async fn setup(server: &TestServer) -> sqlx::PgConnection {
    let mut c = connect(server).await;
    exec(&mut c, "CREATE TABLE p (id INT PRIMARY KEY)").await.unwrap();
    exec(&mut c, "INSERT INTO p (id) VALUES (1)").await.unwrap();
    exec(
        &mut c,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL, pid INT, \
         CONSTRAINT fk_t FOREIGN KEY (pid) REFERENCES p (id))",
    )
    .await
    .unwrap();
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn a_multi_row_insert_failing_late_leaves_nothing() {
    let server = TestServer::start().await.expect("server");
    let mut c = setup(&server).await;

    // UNIQUE on tuple 500.
    let err = exec(
        &mut c,
        &format!("INSERT INTO t (id, v) VALUES {}", values(0, 600, Some(500))),
    )
    .await
    .expect_err("duplicate key");
    assert!(err.contains("duplicate") || err.contains("unique"), "{}", err);
    assert_eq!(count(&mut c, "SELECT id FROM t").await, 0, "UNIQUE: partial rows visible");

    // NOT NULL on tuple 500.
    let mut tuples: Vec<String> = (0..600).map(|i| format!("({}, {})", i, i)).collect();
    tuples[500] = "(500, NULL)".to_string();
    exec(&mut c, &format!("INSERT INTO t (id, v) VALUES {}", tuples.join(", ")))
        .await
        .expect_err("NOT NULL");
    assert_eq!(count(&mut c, "SELECT id FROM t").await, 0, "NOT NULL: partial rows visible");

    // A foreign key on tuple 500.
    let mut tuples: Vec<String> = (0..600).map(|i| format!("({}, {}, 1)", i, i)).collect();
    tuples[500] = "(500, 500, 99)".to_string();
    exec(&mut c, &format!("INSERT INTO t (id, v, pid) VALUES {}", tuples.join(", ")))
        .await
        .expect_err("FK");
    assert_eq!(count(&mut c, "SELECT id FROM t").await, 0, "FK: partial rows visible");

    // The keys are free again: the failed rows really are gone.
    exec(&mut c, &format!("INSERT INTO t (id, v) VALUES {}", values(0, 600, None)))
        .await
        .expect("the same rows, valid now");
    assert_eq!(count(&mut c, "SELECT id FROM t").await, 600);
    assert_nothing_running(&server, "multi-row insert").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_update_failing_mid_scan_changes_nothing() {
    let server = TestServer::start().await.expect("server");
    let mut c = setup(&server).await;
    exec(&mut c, &format!("INSERT INTO t (id, v) VALUES {}", values(0, 300, None)))
        .await
        .unwrap();
    // Every row to one key: the second row collides.
    exec(&mut c, "UPDATE t SET id = 7")
        .await
        .expect_err("UNIQUE mid-scan");
    let rows = sqlx::query("SELECT v FROM t WHERE id = 7")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i32, _>(0), 7, "the row that was id 7 still is, unchanged");
    assert_eq!(count(&mut c, "SELECT id FROM t").await, 300);
    assert_nothing_running(&server, "failed update").await;
}

/// A data-modifying CTE whose body fails part-way. Its transaction was opened
/// by the session for the statement, and the early return that reports the
/// failure never ended it.
#[tokio::test(flavor = "multi_thread")]
async fn a_failing_data_modifying_cte_ends_its_transaction() {
    let server = TestServer::start().await.expect("server");
    let mut c = setup(&server).await;
    exec(
        &mut c,
        "WITH x AS (INSERT INTO t (id, v) VALUES (1, 1), (1, 2) RETURNING id) SELECT id FROM x",
    )
    .await
    .expect_err("the CTE body violates UNIQUE");
    assert_eq!(count(&mut c, "SELECT id FROM t").await, 0, "the CTE's first row is visible");
    assert_nothing_running(&server, "failed CTE").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_statement_in_a_block_is_undone_by_rollback() {
    let server = TestServer::start().await.expect("server");
    let mut c = setup(&server).await;
    exec(&mut c, "BEGIN").await.unwrap();
    exec(&mut c, "INSERT INTO t (id, v) VALUES (1000, 1)").await.unwrap();
    exec(&mut c, &format!("INSERT INTO t (id, v) VALUES {}", values(0, 600, Some(500))))
        .await
        .expect_err("duplicate");
    exec(&mut c, "ROLLBACK").await.unwrap();
    assert_eq!(count(&mut c, "SELECT id FROM t").await, 0);
    assert_nothing_running(&server, "block rolled back").await;
}

/// A client that goes away with a block open — holding a row lock and a
/// key-share lock — leaves no transaction and no lock behind.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_gone_with_a_block_open_leaves_nothing() {
    let server = TestServer::start().await.expect("server");
    let mut c = setup(&server).await;
    exec(&mut c, "INSERT INTO t (id, v) VALUES (1, 1)").await.unwrap();

    let mut gone = connect(&server).await;
    exec(&mut gone, "BEGIN").await.unwrap();
    exec(&mut gone, "INSERT INTO t (id, v, pid) VALUES (2, 2, 1)").await.unwrap();
    exec(&mut gone, "SELECT id FROM t WHERE id = 1 FOR UPDATE").await.unwrap();
    assert_eq!(server.database().row_locks.len(), 1);
    drop(gone);

    assert_nothing_running(&server, "client gone mid-block").await;
    assert_eq!(server.database().row_locks.referenced_len(), 0, "key-share lock left behind");
    assert_eq!(count(&mut c, "SELECT id FROM t").await, 1, "the block's insert is visible");
    // And the row it held is writable at once.
    exec(&mut c, "UPDATE t SET v = 9 WHERE id = 1").await.expect("unlocked");
}

/// A write stopped by `statement_timeout` or by its client leaving is all or
/// nothing, and ends its transaction either way.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_write_is_all_or_nothing() {
    let server = TestServer::start().await.expect("server");
    let mut c = setup(&server).await;
    for chunk in 0..10 {
        exec(&mut c, &format!("INSERT INTO t (id, v) VALUES {}", values(chunk * 2000, chunk * 2000 + 2000, None)))
            .await
            .unwrap();
    }

    // statement_timeout during an UPDATE over every row.
    exec(&mut c, "SET statement_timeout = 1").await.unwrap();
    let outcome = exec(&mut c, "UPDATE t SET v = v + 1000000 WHERE v >= 0").await;
    exec(&mut c, "SET statement_timeout = 0").await.unwrap();
    let changed = count(&mut c, "SELECT id FROM t WHERE v >= 1000000").await;
    match outcome {
        Ok(()) => assert_eq!(changed, 20_000, "reported success, so every row"),
        Err(e) => {
            assert!(e.contains("canceling statement"), "{}", e);
            assert_eq!(changed, 0, "cancelled, so no row");
        }
    }
    let base = changed;

    // The client leaving mid-UPDATE.
    let mut leaving = connect(&server).await;
    let task = tokio::spawn(async move {
        let _ = exec(&mut leaving, "UPDATE t SET v = v + 1 WHERE v >= 0").await;
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    task.abort();
    let _ = task.await;
    assert_nothing_running(&server, "client gone mid-update").await;
    let bumped = count(&mut c, &format!("SELECT id FROM t WHERE v = {} + id", if base == 0 { 1 } else { 1_000_001 })).await;
    assert!(bumped == 0 || bumped == 20_000, "a partial UPDATE is visible: {} rows", bumped);
}
