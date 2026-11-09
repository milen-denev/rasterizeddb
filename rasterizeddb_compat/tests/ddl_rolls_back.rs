//! DDL inside a block rolls back with it, as PostgreSQL's does
//! (`database::block_ddl`, phase 1: CREATE TABLE and ALTER TABLE).
//!
//! Every migration runner wraps a migration in one transaction for exactly
//! this. Before, a block's DDL was applied at once and stayed through its
//! ROLLBACK: a migration that failed part-way left half its schema behind,
//! and the retry failed on "already exists".

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};
use std::time::Duration;

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&server.url()).await.expect("pool");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.map(|_| ()).map_err(|e| e.to_string())
}

async fn exists(pool: &sqlx::PgPool, table: &str) -> bool {
    run(pool, &format!("SELECT * FROM {table} LIMIT 0")).await.is_ok()
}

#[tokio::test]
async fn a_rolled_back_create_table_is_gone_and_can_be_created_again() {
    let (_s, pool) = server().await;
    let mut tx = pool.begin().await.expect("begin");
    tx.execute("CREATE TABLE m1 (id INT PRIMARY KEY, name TEXT NOT NULL)").await.expect("create");
    tx.execute("INSERT INTO m1 VALUES (1, 'a')").await.expect("insert");
    tx.rollback().await.expect("rollback");
    assert!(!exists(&pool, "m1").await, "the table outlived its block's rollback");
    // The retry a migration runner makes.
    run(&pool, "CREATE TABLE m1 (id INT PRIMARY KEY, name TEXT NOT NULL)").await.expect("created again");
    run(&pool, "INSERT INTO m1 VALUES (1, 'a')").await.expect("insert");
}

#[tokio::test]
async fn a_rolled_back_alter_leaves_the_table_as_it_was() {
    let (_s, pool) = server().await;
    for sql in [
        "CREATE TABLE t (id INT NOT NULL, name TEXT NOT NULL)",
        "ALTER TABLE t ADD CONSTRAINT t_name_key UNIQUE (name)",
        "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b')",
    ] {
        run(&pool, sql).await.expect(sql);
    }
    let mut tx = pool.begin().await.expect("begin");
    tx.execute("ALTER TABLE t ADD COLUMN extra INT").await.expect("add column");
    tx.execute("ALTER TABLE t DROP CONSTRAINT t_name_key").await.expect("drop constraint");
    tx.execute("INSERT INTO t (id, name, extra) VALUES (3, 'a', 9)").await.expect("insert into the changed table");
    tx.rollback().await.expect("rollback");

    // The column is not there, the constraint is, and the rows are the ones
    // from before the block.
    assert!(run(&pool, "SELECT extra FROM t").await.is_err(), "the added column outlived the rollback");
    let dup = run(&pool, "INSERT INTO t (id, name) VALUES (4, 'a')").await;
    assert!(dup.is_err(), "the dropped UNIQUE stayed dropped through the rollback");
    let rows = sqlx::query("SELECT id, name FROM t ORDER BY id").fetch_all(&pool).await.expect("read");
    let got: Vec<(i32, String)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(got, vec![(1, "a".to_string()), (2, "b".to_string())]);
}

#[tokio::test]
async fn a_committed_block_keeps_its_ddl() {
    let (_s, pool) = server().await;
    let mut tx = pool.begin().await.expect("begin");
    tx.execute("CREATE TABLE c1 (id INT PRIMARY KEY)").await.expect("create");
    tx.execute("ALTER TABLE c1 ADD COLUMN v TEXT").await.expect("alter");
    tx.execute("INSERT INTO c1 VALUES (1, 'x')").await.expect("insert");
    tx.commit().await.expect("commit");
    let v: String = sqlx::query("SELECT v FROM c1 WHERE id = 1").fetch_one(&pool).await.expect("read").get(0);
    assert_eq!(v, "x");
}

/// A statement that fails ends the block (25P02 until ROLLBACK), and its
/// earlier DDL is undone with it: a migration's third statement failing
/// leaves nothing of its first two.
#[tokio::test]
async fn a_failed_statement_undoes_the_blocks_earlier_ddl() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE base (id INT PRIMARY KEY)").await.expect("base");
    let mut tx = pool.begin().await.expect("begin");
    tx.execute("CREATE TABLE m2 (id INT PRIMARY KEY)").await.expect("create");
    tx.execute("ALTER TABLE base ADD COLUMN flag BOOLEAN").await.expect("alter");
    assert!(tx.execute("INSERT INTO no_such_table VALUES (1)").await.is_err());
    let _ = tx.rollback().await;
    assert!(!exists(&pool, "m2").await, "the created table outlived the failed block");
    assert!(run(&pool, "SELECT flag FROM base").await.is_err(), "the added column outlived the failed block");
}

/// The block keeps the ACCESS EXCLUSIVE its ALTER took, as PostgreSQL does:
/// another session reading the table waits for the block to end, and then
/// sees the table as the block left it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_session_waits_for_the_block_and_sees_its_outcome() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE w (id INT PRIMARY KEY)").await.expect("create");
    run(&pool, "INSERT INTO w VALUES (1)").await.expect("insert");
    let mut a = pool.acquire().await.expect("a");
    a.execute("BEGIN").await.expect("begin");
    a.execute("ALTER TABLE w ADD COLUMN note TEXT").await.expect("alter");
    let p = pool.clone();
    let reader = tokio::spawn(async move { run(&p, "SELECT note FROM w").await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!reader.is_finished(), "a reader saw the block's uncommitted ALTER");
    a.execute("ROLLBACK").await.expect("rollback");
    let seen = tokio::time::timeout(Duration::from_secs(10), reader).await.expect("reader ends").expect("join");
    assert!(seen.is_err(), "after the rollback the column is not there: {seen:?}");
}

/// `ROLLBACK TO SAVEPOINT` undoes the DDL since the savepoint and keeps what
/// came before it, as PostgreSQL's does; so does recovering from a failed
/// statement by rolling back to a savepoint. A released savepoint's DDL is
/// the block's, undone by the block's ROLLBACK. (An ORM's nested
/// transaction is a savepoint, so a nested migration step relies on this.)
#[tokio::test]
async fn rolling_back_to_a_savepoint_undoes_the_ddl_after_it() {
    let (_s, pool) = server().await;
    let mut c = pool.acquire().await.expect("conn");
    for sql in [
        "BEGIN",
        "CREATE TABLE s0 (id INT PRIMARY KEY)",
        "INSERT INTO s0 VALUES (1)",
        "SAVEPOINT a",
        "ALTER TABLE s0 ADD COLUMN x INT",
        "INSERT INTO s0 (id, x) VALUES (2, 2)",
        "CREATE TABLE s1 (id INT)",
        "ROLLBACK TO SAVEPOINT a",
        // A failed statement after a savepoint, recovered from.
        "SAVEPOINT b",
        "ALTER TABLE s0 ADD COLUMN y INT",
    ] {
        c.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.expect(sql);
    }
    assert!(c.execute("INSERT INTO no_such_table VALUES (1)").await.is_err());
    for sql in ["ROLLBACK TO SAVEPOINT b", "ALTER TABLE s0 ADD COLUMN kept INT", "COMMIT"] {
        c.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.expect(sql);
    }
    assert!(run(&pool, "SELECT x FROM s0").await.is_err(), "the column added after savepoint a stayed");
    assert!(run(&pool, "SELECT y FROM s0").await.is_err(), "the column added after savepoint b stayed");
    assert!(!exists(&pool, "s1").await, "the table created after savepoint a stayed");
    let rows = sqlx::query("SELECT id, kept FROM s0 ORDER BY id").fetch_all(&pool).await.expect("read");
    let got: Vec<(i32, Option<i32>)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(got, vec![(1, None)], "the block's work before the savepoints and after the last");

    // RELEASE merges the savepoint's DDL into the block; ROLLBACK undoes it.
    for sql in ["BEGIN", "SAVEPOINT c", "ALTER TABLE s0 ADD COLUMN z INT", "RELEASE SAVEPOINT c", "ROLLBACK"] {
        c.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.expect(sql);
    }
    assert!(run(&pool, "SELECT z FROM s0").await.is_err(), "a released savepoint's column outlived the ROLLBACK");
    run(&pool, "SELECT kept FROM s0").await.expect("the committed column");
}
