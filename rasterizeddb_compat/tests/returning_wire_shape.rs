//! `UPDATE ... RETURNING` decoded by a real client, through the extended
//! protocol with binary results — the shape sea-orm emits for every update.
//!
//! The client decodes a row using the column types it cached from `Describe`.
//! The server formats values from a *separately derived* list. When those two
//! disagree the row is still well-formed at the protocol level, so nothing
//! errors: the client simply reads a value of the wrong width. For a 4-byte
//! type over an empty value that is a panic inside `bytes`, not a `Result`,
//! which is why production saw "advance out of bounds" rather than an error.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

/// The pms table, including the nullable jsonb the app leaves unset.
async fn setup(pool: &sqlx::PgPool) {
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE case_flow_node (\
           id INT PRIMARY KEY, \
           case_id INT NOT NULL, \
           node_key TEXT NOT NULL, \
           title TEXT NOT NULL, \
           graph_x INT NOT NULL, \
           graph_y INT NOT NULL, \
           config JSONB, \
           notes TEXT)",
    ))
    .await
    .expect("create");
    pool.execute(sqlx::raw_sql(
        "INSERT INTO case_flow_node (id, case_id, node_key, title, graph_x, graph_y, config, notes) \
         VALUES (1, 7, 'start', 'Start', 0, 0, NULL, '')",
    ))
    .await
    .expect("insert");
}

/// Every column read back at its declared type.
#[tokio::test]
async fn update_returning_decodes_at_the_declared_types() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    let row = sqlx::query(
        "UPDATE case_flow_node SET title = $1, graph_x = $2 WHERE id = $3 \
         RETURNING id, case_id, node_key, title, graph_x, graph_y, config, notes",
    )
    .bind("Renamed")
    .bind(42i32)
    .bind(1i32)
    .fetch_one(&pool)
    .await
    .expect("update returning");

    assert_eq!(row.get::<i32, _>("id"), 1);
    assert_eq!(row.get::<i32, _>("case_id"), 7);
    assert_eq!(row.get::<String, _>("node_key"), "start");
    assert_eq!(row.get::<String, _>("title"), "Renamed");
    assert_eq!(row.get::<i32, _>("graph_x"), 42);
    assert_eq!(row.get::<i32, _>("graph_y"), 0);
    assert_eq!(row.get::<Option<serde_json::Value>, _>("config"), None);
    assert_eq!(row.get::<String, _>("notes"), "");
}

/// `RETURNING *` — the description and the value list are derived by different
/// code paths, so the star form is worth pinning separately.
#[tokio::test]
async fn update_returning_star_decodes_at_the_declared_types() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    let row = sqlx::query("UPDATE case_flow_node SET graph_y = $1 WHERE id = $2 RETURNING *")
        .bind(9i32)
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("update returning star");

    assert_eq!(row.get::<i32, _>("id"), 1);
    assert_eq!(row.get::<i32, _>("graph_y"), 9);
    assert_eq!(row.get::<i32, _>("graph_x"), 0);
    assert_eq!(row.get::<String, _>("notes"), "");
}

/// The same statement under contention, which is when production saw it.
#[tokio::test]
async fn concurrent_update_returning_never_desyncs_a_client() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    let mut handles = Vec::new();
    for i in 0..8i32 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let mut tx = pool.begin().await.expect("begin");
            let row = sqlx::query(
                "UPDATE case_flow_node SET title = $1, graph_x = $2 WHERE id = $3 \
                 RETURNING id, case_id, node_key, title, graph_x, graph_y, config, notes",
            )
            .bind(format!("node-{}", i))
            .bind(i)
            .bind(1i32)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;

            // Force the binary decode of every declared type.
            let _: i32 = row.get("id");
            let _: i32 = row.get("case_id");
            let _: i32 = row.get("graph_x");
            let _: i32 = row.get("graph_y");
            let _: String = row.get("title");
            let _: String = row.get("notes");
            let _: Option<serde_json::Value> = row.get("config");

            tx.commit().await.map_err(|e| e.to_string())?;
            Ok::<(), String>(())
        }));
    }

    for h in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => panic!("a concurrent writer failed: {}", e),
            Err(join) => panic!("a client task died decoding a row: {}", join),
        }
    }
}

/// `RETURNING` reports the row as the *transaction* sees it, including writes
/// this transaction has already made and not yet committed.
///
/// The read used to run under a fresh snapshot, which by construction cannot
/// see its own transaction's uncommitted rows. A second write in the same
/// block then reported the pre-block value for every column it did not itself
/// assign, while storage held the right one.
#[tokio::test]
async fn returning_sees_earlier_writes_in_the_same_block() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    let mut tx = pool.begin().await.expect("begin");

    sqlx::query("UPDATE case_flow_node SET graph_x = $1 WHERE id = $2")
        .bind(111i32)
        .bind(1i32)
        .execute(&mut *tx)
        .await
        .expect("first update");

    // Assigns only graph_y, but reports graph_x too.
    let row = sqlx::query("UPDATE case_flow_node SET graph_y = $1 WHERE id = $2 RETURNING graph_x, graph_y")
        .bind(222i32)
        .bind(1i32)
        .fetch_one(&mut *tx)
        .await
        .expect("second update returning");

    assert_eq!(
        row.get::<i32, _>("graph_x"),
        111,
        "RETURNING must see the write this transaction already made"
    );
    assert_eq!(row.get::<i32, _>("graph_y"), 222);

    tx.commit().await.expect("commit");

    let after = sqlx::query("SELECT graph_x, graph_y FROM case_flow_node WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("read back");
    assert_eq!(after.get::<i32, _>("graph_x"), 111);
    assert_eq!(after.get::<i32, _>("graph_y"), 222);
}

/// The same rule for `DELETE … RETURNING`, which shares the read path.
#[tokio::test]
async fn delete_returning_sees_earlier_writes_in_the_same_block() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    let mut tx = pool.begin().await.expect("begin");
    sqlx::query("UPDATE case_flow_node SET title = $1 WHERE id = $2")
        .bind("renamed-in-block")
        .bind(1i32)
        .execute(&mut *tx)
        .await
        .expect("update");

    let row = sqlx::query("DELETE FROM case_flow_node WHERE id = $1 RETURNING id, title")
        .bind(1i32)
        .fetch_one(&mut *tx)
        .await
        .expect("delete returning");

    assert_eq!(row.get::<i32, _>("id"), 1);
    assert_eq!(
        row.get::<String, _>("title"),
        "renamed-in-block",
        "DELETE RETURNING must report the row as this transaction last left it"
    );
    tx.commit().await.expect("commit");
}
