//! Does DDL participate in a transaction's rollback?
//!
//! Every sea-orm migration runs as: BEGIN → statements → record → COMMIT.
//! On PostgreSQL a failed migration therefore leaves the schema untouched.
//! If this engine's DDL persists through an abort, a failed migration leaves
//! PARTIAL schema changes behind on a table the chain believes un-migrated —
//! the exact state the roles table on the production box may be in.
//! These tests document whichever answer the engine gives.

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

#[tokio::test]
async fn a_rolled_back_drop_constraint_reports_what_actually_happened() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE t (id INT NOT NULL, name TEXT NOT NULL)")
        .await
        .expect("create");
    run(&pool, "ALTER TABLE t ADD CONSTRAINT t_name_key UNIQUE (name)")
        .await
        .expect("add unique");
    run(&pool, "INSERT INTO t (id, name) VALUES (1, 'a')").await.expect("insert");

    // The migration shape: BEGIN, drop the constraint, then abort.
    let mut tx = pool.begin().await.expect("begin");
    tx.execute(sqlx::raw_sql("ALTER TABLE t DROP CONSTRAINT t_name_key"))
        .await
        .expect("drop inside txn");
    tx.rollback().await.expect("rollback");

    // Is the constraint still enforced?
    let dup = run(&pool, "INSERT INTO t (id, name) VALUES (2, 'a')").await;
    match dup {
        Err(_) => {
            // PostgreSQL semantics: the abort undid the drop. A failed
            // migration leaves the schema as it found it.
            println!("VERDICT: DDL rolls back — PostgreSQL semantics");
        }
        Ok(()) => {
            // The drop persisted through the rollback: DDL is NOT
            // transactional here. A failed migration leaves partial schema
            // changes behind.
            println!("VERDICT: DDL does NOT roll back — the drop persisted");
        }
    }
}

/// `ALTER TABLE IF EXISTS missing RENAME TO x` is a notice on PostgreSQL, not
/// an error — and with non-transactional DDL every migration chain here
/// depends on that: a retry replays statements over half-changed schema, so
/// each one must tolerate its own work already being done.
#[tokio::test]
async fn alter_table_if_exists_tolerates_a_missing_table() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE \"role\" (id INT NOT NULL, name TEXT NOT NULL)")
        .await
        .expect("create");
    run(&pool, "INSERT INTO \"role\" (id, name) VALUES (1, 'admin')").await.expect("insert");

    // First run: the rename happens.
    run(&pool, "ALTER TABLE IF EXISTS \"role\" RENAME TO roles")
        .await
        .expect("the rename must apply");
    let n: i64 = sqlx::query("SELECT COUNT(*) AS n FROM roles")
        .fetch_one(&pool)
        .await
        .expect("renamed table readable")
        .try_get("n")
        .expect("n");
    assert_eq!(n, 1, "the rows moved with the name");

    // The retry: "role" no longer exists. IF EXISTS makes this a no-op...
    run(&pool, "ALTER TABLE IF EXISTS \"role\" RENAME TO roles")
        .await
        .expect("a replayed rename must be a notice, not an error");
    // ...while the bare form still refuses, so a typo stays loud.
    let bare = run(&pool, "ALTER TABLE \"role\" RENAME TO roles").await;
    assert!(bare.is_err(), "without IF EXISTS a missing table is an error");
}

/// The existence probe a ported app reaches for when `information_schema` is
/// absent: `SELECT 1 FROM t LIMIT 0` — whose FAILING case is the informative
/// one, and which therefore poisons a transaction block unless it runs under
/// a savepoint. This is the exact pattern migration2 uses; pinned so it
/// keeps working.
#[tokio::test]
async fn an_existence_probe_under_a_savepoint_does_not_poison_the_block() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE present (id INT NOT NULL)").await.expect("create");

    let mut tx = pool.begin().await.expect("begin");
    // Positive probe: succeeds, block healthy.
    tx.execute(sqlx::raw_sql("SAVEPOINT __probe")).await.expect("savepoint");
    tx.execute(sqlx::raw_sql("SELECT 1 FROM present LIMIT 0"))
        .await
        .expect("probing an existing table");
    tx.execute(sqlx::raw_sql("RELEASE SAVEPOINT __probe")).await.expect("release");

    // Negative probe: fails — and the savepoint recovers the block.
    tx.execute(sqlx::raw_sql("SAVEPOINT __probe")).await.expect("savepoint");
    let missing = tx.execute(sqlx::raw_sql("SELECT 1 FROM absent_relation LIMIT 0")).await;
    assert!(missing.is_err(), "the failing case is the informative one");
    tx.execute(sqlx::raw_sql("ROLLBACK TO SAVEPOINT __probe"))
        .await
        .expect("the savepoint must recover the block");

    // The block is alive: real work still lands, and commits.
    tx.execute(sqlx::raw_sql("INSERT INTO present (id) VALUES (7)"))
        .await
        .expect("the block survived the failed probe");
    tx.commit().await.expect("commit");
    let n: i64 = sqlx::query("SELECT COUNT(*) AS n FROM present")
        .fetch_one(&pool)
        .await
        .expect("read back")
        .try_get("n")
        .expect("n");
    assert_eq!(n, 1, "the insert after the recovered probe committed");
}
