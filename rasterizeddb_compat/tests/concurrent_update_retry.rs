//! Two handlers updating one row at once.
//!
//! PostgreSQL's default isolation is READ COMMITTED: the second writer waits
//! for the first and re-evaluates its predicate. It never raises `40001`.
//! This engine applies first-updater-wins at every level, so an application
//! rendering one page from several concurrent handlers — each touching the
//! same row, none retrying, which is what sqlx and sea-orm do by default —
//! saw its requests fail. An autocommit write now replays the conflict.
//!
//! The replay must never become a silent empty answer: a retry that matches
//! nothing reports the original conflict instead.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    pool.execute(sqlx::raw_sql(sql))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

/// The production shape: several handlers writing the same row at once.
#[tokio::test]
async fn concurrent_handlers_updating_one_row_all_succeed() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE case_flow_node (id INT PRIMARY KEY, title TEXT NOT NULL, graph_x INT NOT NULL)")
        .await
        .expect("create");
    run(&pool, "INSERT INTO case_flow_node (id, title, graph_x) VALUES (1, 'start', 0)")
        .await
        .expect("insert");

    let mut handles = Vec::new();
    for i in 0..8i32 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            sqlx::query(
                "UPDATE case_flow_node SET title = $1, graph_x = $2 WHERE id = $3 \
                 RETURNING id, title, graph_x",
            )
            .bind(format!("t{}", i))
            .bind(i * 10)
            .bind(1i32)
            .fetch_all(&pool)
            .await
            .map(|r| r.len())
            .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
        }));
    }

    // What this test pins is the SERIALIZATION FAILURE: on PostgreSQL none of
    // these handlers errors, and after the replay none of them does here
    // either. A zero-row success is NOT asserted against — that is the
    // separate open reader defect (an autocommit write can match nothing even
    // with no conflict involved), pinned in
    // `write_conflicts.rs::a_concurrent_reader_never_misses_an_existing_row`.
    // Asserting it here would make this file red for a defect it does not
    // describe.
    let mut failures = Vec::new();
    for h in handles {
        if let Err(e) = h.await.expect("task") {
            failures.push(e);
        }
    }
    assert!(
        failures.is_empty(),
        "concurrent handlers must not fail with a serialization error, as they \
         would not on PostgreSQL; got: {:#?}",
        failures
    );

    // One row, holding one of the writes — the conflict check's guarantee.
    let rows = pool
        .fetch_all(sqlx::raw_sql("SELECT id, title FROM case_flow_node"))
        .await
        .expect("read back");
    assert_eq!(rows.len(), 1, "still exactly one row");
    let title: String = rows[0].try_get("title").expect("title");
    assert!(title.starts_with('t'), "holds one of the writes: {}", title);
}

/// Inside an explicit block the client owns the transaction, so a conflict is
/// still reported rather than replayed — replaying one statement of a block
/// would break the atomicity the block was opened for.
#[tokio::test]
async fn a_conflict_inside_a_block_is_still_reported() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE t (id INT PRIMARY KEY, n BIGINT NOT NULL)")
        .await
        .expect("create");
    run(&pool, "INSERT INTO t (id, n) VALUES (1, 0)").await.expect("insert");

    let mut a = pool.begin().await.expect("begin a");
    a.execute(sqlx::raw_sql("UPDATE t SET n = 1 WHERE id = 1"))
        .await
        .expect("first writer wins");

    let mut b = pool.begin().await.expect("begin b");
    let conflict = b.execute(sqlx::raw_sql("UPDATE t SET n = 2 WHERE id = 1")).await;
    let err = conflict.expect_err("a conflict inside a block must be reported");
    // The replay's internal marker must never reach a client.
    let text = err.to_string();
    assert!(
        !text.contains("[retryable]"),
        "internal marker leaked to the client: {}",
        text
    );
    assert!(
        text.contains("could not serialize access"),
        "the conflict is still reported as one: {}",
        text
    );
    let _ = b.rollback().await;
    a.commit().await.expect("commit a");
}

/// The production statement, near verbatim: sea-orm updating one
/// `case_flow_node` row from several handlers rendering the same page, UUID
/// key, `config` set to NULL, and a full `RETURNING` list. On PostgreSQL none
/// of these errors — the second writer waits — and after the replay none of
/// them errors here either.
#[tokio::test]
async fn the_case_flow_node_update_survives_concurrent_handlers() {
    let (_s, pool) = server().await;
    run(
        &pool,
        "CREATE TABLE case_flow_node (id UUID PRIMARY KEY, case_model_id UUID NOT NULL, \
         step_template_id UUID, title TEXT NOT NULL, description TEXT, node_kind INT NOT NULL, \
         merge_policy INT, graph_x INT NOT NULL, graph_y INT NOT NULL, config JSONB, \
         created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ, archived_at TIMESTAMPTZ)",
    )
    .await
    .expect("create");
    run(&pool, "INSERT INTO case_flow_node (id, case_model_id, step_template_id, title, description, node_kind, graph_x, graph_y, config, created_at, updated_at) \
         VALUES ('b58a1b5d-b4be-412d-bb92-191a2146739d', 'a10abf9a-8c93-4edd-86ab-ab20f56762c7', \
         'fa63b184-553c-4303-8e87-5577cad1f82a', 'start', 'start', 0, 0, 0, NULL, now(), now())")
        .await
        .expect("insert");

    // Several handlers reconciling the same node at once, as the edit page does.
    let mut handles = Vec::new();
    for i in 0..6i32 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            sqlx::query(
                "UPDATE \"case_flow_node\" SET \"step_template_id\" = $1, \"title\" = $2, \
                 \"description\" = $3, \"node_kind\" = 0, \"graph_x\" = $4, \"graph_y\" = 160, \
                 \"config\" = NULL, \"updated_at\" = now() \
                 WHERE \"case_flow_node\".\"id\" = $5 \
                 RETURNING \"id\", \"case_model_id\", \"step_template_id\", \"title\", \
                 \"description\", \"node_kind\", \"merge_policy\", \"graph_x\", \"graph_y\", \
                 \"config\", \"created_at\", \"updated_at\", \"archived_at\"",
            )
            .bind(uuid_of("fa63b184-553c-4303-8e87-5577cad1f82a"))
            .bind("test")
            .bind("test")
            .bind(120 + i)
            .bind(uuid_of("b58a1b5d-b4be-412d-bb92-191a2146739d"))
            .fetch_all(&pool)
            .await
            .map(|r| r.len())
            .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
        }));
    }

    let mut failures = Vec::new();
    for h in handles {
        if let Err(e) = h.await.expect("task") {
            failures.push(e);
        }
    }
    assert!(
        failures.is_empty(),
        "the edit page's concurrent handlers must not fail; got: {:#?}",
        failures
    );

    // The row survives intact, with one of the writes.
    let rows = pool
        .fetch_all(sqlx::raw_sql("SELECT id, title FROM case_flow_node"))
        .await
        .expect("read back");
    assert_eq!(rows.len(), 1, "still exactly one node");
    assert_eq!(rows[0].try_get::<String, _>("title").expect("title"), "test");
}

fn uuid_of(s: &str) -> sqlx::types::Uuid {
    sqlx::types::Uuid::parse_str(s).expect("uuid")
}

/// The same contention, but each writer inside an explicit block — which is
/// what sea-orm's `db.begin()` produces, and what pms actually sends. On
/// PostgreSQL READ COMMITTED these all succeed: the second writer blocks on
/// the row, re-reads it and proceeds. The autocommit replay does not reach
/// this case by design, so this is the shape that matters.
#[tokio::test]
async fn concurrent_writers_inside_explicit_blocks_all_succeed() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE t (id INT PRIMARY KEY, n BIGINT NOT NULL)")
        .await
        .expect("create");
    run(&pool, "INSERT INTO t (id, n) VALUES (1, 0)").await.expect("insert");

    let mut handles = Vec::new();
    for i in 0..8i64 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
            let r = sqlx::query("UPDATE t SET n = $1 WHERE id = 1")
                .bind(i)
                .execute(&mut *tx)
                .await
                .map(|d| d.rows_affected())
                .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string());
            match r {
                Ok(n) => {
                    tx.commit().await.map_err(|e| e.to_string())?;
                    Ok(n)
                }
                Err(e) => {
                    let _ = tx.rollback().await;
                    Err(e)
                }
            }
        }));
    }

    let mut failures: Vec<String> = Vec::new();
    for h in handles {
        if let Err(e) = h.await.expect("task") {
            failures.push(e);
        }
    }
    assert!(
        failures.is_empty(),
        "writers inside explicit blocks must not fail, as they would not on \
         PostgreSQL READ COMMITTED; got: {:#?}",
        failures
    );
}

/// A writer must outlast a block that is held open across a round trip.
///
/// The replay budget used to be five attempts with backoff capped at 40 ms —
/// about 120 ms — which outlasts an autocommit winner and nothing else. An
/// application holds its transaction open while it does other work, so the
/// winner's row stays locked until its `COMMIT` arrives, which is a network
/// round trip away. Against that the budget ran out and a `40001` reached a
/// client that would have waited happily; PostgreSQL blocks until the other
/// transaction ends rather than budgeting at all.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_waits_out_a_block_held_across_a_round_trip() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE case_flow_node (id INT PRIMARY KEY, title TEXT NOT NULL)")
        .await
        .expect("create");
    run(&pool, "INSERT INTO case_flow_node (id, title) VALUES (1, 'start')")
        .await
        .expect("insert");

    // One handler opens a block, writes, and then keeps it open for longer
    // than the old budget before committing.
    let holder = {
        let pool = pool.clone();
        tokio::spawn(async move {
            let mut tx = pool.begin().await.expect("begin holder");
            sqlx::query("UPDATE case_flow_node SET title = $1 WHERE id = $2")
                .bind("held")
                .bind(1i32)
                .execute(&mut *tx)
                .await
                .expect("holder write");
            // Longer than 120 ms, shorter than the new budget.
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            tx.commit().await.expect("holder commit");
        })
    };

    // Give the holder time to take the row.
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    // A second handler writes the same row. It should wait, not fail.
    let started = std::time::Instant::now();
    let result = sqlx::query("UPDATE case_flow_node SET title = $1 WHERE id = $2")
        .bind("second")
        .bind(1i32)
        .execute(&pool)
        .await;

    let waited = started.elapsed();
    println!("second writer returned after {:?}", waited);
    holder.await.expect("holder task");

    let err = match result {
        Ok(_) => None,
        Err(e) => Some(e.to_string()),
    };
    assert!(
        err.is_none(),
        "a writer must outlast a block held across a round trip, as it would on \
         PostgreSQL; got: {:?}",
        err
    );

    // And the second write is the one that stands.
    let title: String = sqlx::query("SELECT title FROM case_flow_node WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("read back")
        .get("title");
    assert_eq!(title, "second");
}
