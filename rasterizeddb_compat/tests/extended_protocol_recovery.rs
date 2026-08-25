//! What a connection does *after* a statement fails.
//!
//! sea-orm/sqlx drive the extended query protocol: Parse/Bind/Describe/
//! Execute/Sync, with bound parameters and binary results. PostgreSQL has a
//! rule for errors in that protocol — once a statement fails, every message
//! is discarded until `Sync`. A server that keeps answering them leaves
//! replies in the socket that the client never asked for, and the next query
//! on that connection reads the leftovers as if they were its own results.
//!
//! That desync is not a clean error. `bytes` panics with "advance out of
//! bounds" as the client reads a 4-byte field off an exhausted buffer, which
//! is what production saw next to the concurrent-update conflicts.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn setup(pool: &sqlx::PgPool) {
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE case_flow_node (id INT PRIMARY KEY, title TEXT NOT NULL, graph_x INT NOT NULL)",
    ))
    .await
    .expect("create");
    pool.execute(sqlx::raw_sql(
        "INSERT INTO case_flow_node (id, title, graph_x) VALUES (1, 'start', 0)",
    ))
    .await
    .expect("insert");
}

/// A failed statement must not poison the connection it ran on.
#[tokio::test]
async fn a_connection_still_works_after_a_failed_statement() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    // Pin one connection so the recovery is observed on the same socket.
    let mut conn = pool.acquire().await.expect("acquire");

    let failed = sqlx::query("UPDATE case_flow_node SET graph_x = $1 WHERE no_such_column = $2")
        .bind(5i32)
        .bind(1i32)
        .execute(&mut *conn)
        .await;
    assert!(failed.is_err(), "the statement must fail");

    // The same connection, extended protocol, binary results.
    let row = sqlx::query("SELECT id, title, graph_x FROM case_flow_node WHERE id = $1")
        .bind(1i32)
        .fetch_one(&mut *conn)
        .await
        .expect("the connection must still answer after an error");

    assert_eq!(row.get::<i32, _>("id"), 1);
    assert_eq!(row.get::<String, _>("title"), "start");
    assert_eq!(row.get::<i32, _>("graph_x"), 0);
}

/// The production shape: concurrent writers in explicit blocks, each reading
/// back through the same pool afterwards.
///
/// Ignored: this reproduces an open engine defect, not a protocol one. A plain
/// `SELECT` for a row that exists transiently returns no rows while other
/// transactions update that row — roughly one run in ten with eight writers on
/// a single row.
///
/// Tracing the visibility test on the uncached scan shows why it is not simply
/// a snapshot being too strict. For one row id the scan sees a well-formed
/// chain — `xmin=4 xmax=8`, `8→9`, `9→10`, `10→11` — *and*, at the same time,
/// two independently live versions: `xmin=5 xmax=0` and `xmin=7 xmax=0`.
/// Neither retires the other. First-updater-wins is supposed to make that
/// impossible, so the chain has forked: two writers each created a version
/// without seeing the other. A reader whose snapshot rejects every branch tip
/// then correctly concludes the row is invisible, and returns nothing.
///
/// So the defect is on the write side, in the window between the conflict check
/// and the version being stamped, and it is a lost update as much as a missed
/// read. `UPDATE … RETURNING` no longer reports it as an empty success — the
/// executor re-reads once the write has landed — but a plain `SELECT` racing a
/// writer still can.
#[ignore = "open: concurrent writers fork the version chain; see the doc comment"]
#[tokio::test]
async fn concurrent_block_writers_then_reads_stay_in_sync() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    let mut handles = Vec::new();
    for i in 0..8i32 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let mut tx = pool.begin().await.expect("begin");
            sqlx::query("UPDATE case_flow_node SET title = $1, graph_x = $2 WHERE id = $3")
                .bind(format!("node-{}", i))
                .bind(i)
                .bind(1i32)
                .execute(&mut *tx)
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())?;

            // Read back on a pooled connection, binary decode of a 4-byte type.
            let row = sqlx::query("SELECT id, title, graph_x FROM case_flow_node WHERE id = $1")
                .bind(1i32)
                .fetch_one(&pool)
                .await
                .map_err(|e| e.to_string())?;
            let _: i32 = row.get("id");
            let _: i32 = row.get("graph_x");
            let _: String = row.get("title");
            Ok::<(), String>(())
        }));
    }

    for h in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => panic!("a concurrent writer failed: {}", e),
            Err(join) => panic!("a client task died (protocol desync): {}", join),
        }
    }
}

/// After an error inside a block, the connection must report the aborted
/// block and then recover cleanly on rollback — not desync.
#[tokio::test]
async fn an_aborted_block_recovers_on_rollback() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    let mut conn = pool.acquire().await.expect("acquire");
    conn.execute(sqlx::raw_sql("BEGIN")).await.expect("begin");

    let failed = sqlx::query("UPDATE case_flow_node SET graph_x = $1 WHERE nope = $2")
        .bind(1i32)
        .bind(1i32)
        .execute(&mut *conn)
        .await;
    assert!(failed.is_err(), "the statement must fail");

    conn.execute(sqlx::raw_sql("ROLLBACK")).await.expect("rollback");

    let row = sqlx::query("SELECT graph_x FROM case_flow_node WHERE id = $1")
        .bind(1i32)
        .fetch_one(&mut *conn)
        .await
        .expect("the connection must recover after rollback");
    assert_eq!(row.get::<i32, _>("graph_x"), 0);
}
