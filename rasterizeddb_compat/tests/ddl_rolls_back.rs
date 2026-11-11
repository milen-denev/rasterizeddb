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

/// A foreign key added or dropped in a block that rolls back: both tables'
/// sides of it are as before, on disk and in the engine's map of who
/// references whom. A rolled-back ADD no longer refuses a parent's delete;
/// a rolled-back DROP still does.
#[tokio::test]
async fn a_rolled_back_foreign_key_change_is_undone_on_both_sides() {
    let (_s, pool) = server().await;
    for sql in [
        "CREATE TABLE p (id INT PRIMARY KEY)",
        "CREATE TABLE c (id INT PRIMARY KEY, pid INT)",
        "CREATE TABLE p2 (id INT PRIMARY KEY)",
        "CREATE TABLE c2 (id INT PRIMARY KEY, pid INT REFERENCES p2 (id))",
        "INSERT INTO p VALUES (1), (2)",
        "INSERT INTO c VALUES (1, 1)",
        "INSERT INTO p2 VALUES (1), (2)",
        "INSERT INTO c2 VALUES (1, 1)",
    ] {
        run(&pool, sql).await.expect(sql);
    }
    let mut tx = pool.begin().await.expect("begin");
    tx.execute("ALTER TABLE c ADD CONSTRAINT c_pid_fkey FOREIGN KEY (pid) REFERENCES p (id)").await.expect("add fk");
    assert!(tx.execute("DELETE FROM p WHERE id = 1").await.is_err(), "the added key is enforced inside the block");
    tx.rollback().await.expect("rollback");
    run(&pool, "DELETE FROM p WHERE id = 1").await.expect("the rolled-back key still refused the parent's delete");
    run(&pool, "INSERT INTO c VALUES (2, 99)").await.expect("the rolled-back key still refused a child");

    let constraint: String =
        sqlx::query("SELECT conname::text FROM pg_constraint WHERE contype = 'f' AND conrelid = 'c2'::regclass")
            .fetch_one(&pool)
            .await
            .map(|r| r.get(0))
            .unwrap_or_else(|_| "c2_pid_fkey".to_string());
    let mut tx = pool.begin().await.expect("begin");
    tx.execute(sqlx::raw_sql(AssertSqlSafe(format!("ALTER TABLE c2 DROP CONSTRAINT {constraint}")))).await.expect("drop fk");
    tx.rollback().await.expect("rollback");
    assert!(run(&pool, "DELETE FROM p2 WHERE id = 1").await.is_err(), "the key dropped in a rolled-back block is gone");
    assert!(run(&pool, "INSERT INTO c2 VALUES (2, 99)").await.is_err(), "the key dropped in a rolled-back block is gone");
}

/// A CREATE TABLE that fails leaves nothing for the block's rollback to
/// drop: the table was never made. Its rollback step used to stay, and the
/// rollback dropped a table that did not exist without its lock (a panic
/// that closed the session) or, had another session created the name in
/// between, that session's table.
#[tokio::test]
async fn a_failed_create_table_leaves_nothing_to_roll_back() {
    let (_s, pool) = server().await;
    let mut c = pool.acquire().await.expect("connection");
    // Under a savepoint: ROLLBACK TO revives the block, and what it made
    // before the failure commits.
    c.execute("BEGIN").await.expect("begin");
    c.execute("CREATE TABLE kept (id INT)").await.expect("create");
    c.execute("SAVEPOINT s").await.expect("savepoint");
    let failed = c.execute("CREATE TABLE bad (id INT CHECK (nosuchcol > 0))").await;
    assert!(failed.is_err(), "a CHECK on a missing column is refused");
    c.execute("ROLLBACK TO SAVEPOINT s").await.expect("rollback to");
    c.execute("COMMIT").await.expect("commit");
    assert!(exists(&pool, "kept").await, "the table made before the savepoint was lost");
    assert!(!exists(&pool, "bad").await);

    // With no savepoint the failure ends the block at once; the session
    // goes on.
    c.execute("BEGIN").await.expect("begin");
    let failed = c.execute("CREATE TABLE bad (id INT CHECK (nosuchcol > 0))").await;
    assert!(failed.is_err());
    c.execute("ROLLBACK").await.expect("rollback");
    c.execute("SELECT 1").await.expect("the session survived the failed block");
    assert!(exists(&pool, "kept").await);
}

/// A read-only block's DDL is refused (25006) before it takes anything: its
/// table is not kept aside, nor locked to the block's end, so other sessions
/// go on reading it while the block stays open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_only_blocks_refused_alter_locks_nothing() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE ro (id INT)").await.expect("create");
    run(&pool, "INSERT INTO ro VALUES (1)").await.expect("insert");
    let mut c = pool.acquire().await.expect("connection");
    c.execute("BEGIN READ ONLY").await.expect("begin");
    // A savepoint keeps the failed block open, with whatever it holds.
    c.execute("SAVEPOINT s").await.expect("savepoint");
    let refused = c.execute("ALTER TABLE ro ADD COLUMN extra INT").await.expect_err("refused");
    assert!(refused.to_string().contains("read-only"), "{refused}");

    let mut other = pool.acquire().await.expect("second connection");
    other.execute("SET lock_timeout = '300ms'").await.expect("lock_timeout");
    let read = tokio::time::timeout(Duration::from_secs(5), other.execute("SELECT * FROM ro")).await;
    assert!(matches!(read, Ok(Ok(_))), "a reader waited on the refused ALTER's lock: {read:?}");
    c.execute("ROLLBACK").await.expect("rollback");
}

struct OnDisk(std::path::PathBuf);

impl Drop for OnDisk {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A file-backed server: a partitioned table needs a directory.
async fn on_disk(tag: &str) -> (TestServer, sqlx::PgPool, OnDisk) {
    let root = std::env::temp_dir().join(format!("ddl_rolls_back_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let server = TestServer::start_file_backed(&root.to_string_lossy()).await.expect("server");
    let pool = sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&server.url()).await.expect("pool");
    (server, pool, OnDisk(root))
}

async fn index_names(pool: &sqlx::PgPool, table: &str) -> Vec<String> {
    sqlx::query("SELECT indexname FROM pg_indexes WHERE tablename = $1 ORDER BY indexname")
        .bind(table)
        .fetch_all(pool)
        .await
        .expect("pg_indexes")
        .iter()
        .map(|r| r.get::<String, _>(0))
        .collect()
}

/// `CREATE INDEX`, `CREATE UNIQUE INDEX` and `DROP INDEX` in a block are
/// undone by its ROLLBACK and kept by its COMMIT (2026-11-12). They used to
/// apply at once and stay; a unique index (run as a constraint) was not even
/// warned about, since only the rewritten statement was checked.
#[tokio::test]
async fn index_ddl_rolls_back_with_its_block() {
    let (_s, pool) = server().await;
    for sql in [
        "CREATE TABLE ix (id INT PRIMARY KEY, x INT, name TEXT)",
        "CREATE INDEX ix_x ON ix (x)",
        "INSERT INTO ix VALUES (1, 10, 'a'), (2, 20, 'b')",
    ] {
        run(&pool, sql).await.expect(sql);
    }
    let before = index_names(&pool, "ix").await;
    assert!(before.contains(&"ix_x".to_string()), "{before:?}");

    let mut tx = pool.begin().await.expect("begin");
    tx.execute("CREATE INDEX ix_name ON ix (name)").await.expect("create index");
    tx.execute("CREATE UNIQUE INDEX ix_name_u ON ix (name)").await.expect("create unique index");
    tx.execute("DROP INDEX ix_x").await.expect("drop index");
    assert!(tx.execute("INSERT INTO ix VALUES (3, 30, 'a')").await.is_err(), "the unique index is enforced in the block");
    tx.rollback().await.expect("rollback");
    assert_eq!(index_names(&pool, "ix").await, before, "the index DDL outlived the rollback");
    run(&pool, "INSERT INTO ix VALUES (3, 30, 'a')").await.expect("the unique index is gone with the rollback");

    let mut tx = pool.begin().await.expect("begin");
    tx.execute("CREATE INDEX ix_name ON ix (name)").await.expect("create index");
    tx.execute("DROP INDEX ix_x").await.expect("drop index");
    tx.commit().await.expect("commit");
    let after = index_names(&pool, "ix").await;
    assert!(after.contains(&"ix_name".to_string()) && !after.contains(&"ix_x".to_string()), "{after:?}");
    let n: i64 = sqlx::query("SELECT count(*) FROM ix WHERE x = 20").fetch_one(&pool).await.expect("read").get(0);
    assert_eq!(n, 1);
}

/// A partitioned table's DDL in a block rolls back (2026-11-12): its ALTER
/// (a metadata one: the table has no rows yet; one that rewrites rows into
/// a new uid is warned about and stays, see `block_ddl_table_moved`), its
/// CREATE INDEX, a table created PARTITION BY, and its DROP, which destroyed
/// the table and its rows through a ROLLBACK before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partitioned_tables_ddl_rolls_back_with_its_block() {
    let (_s, pool, _dir) = on_disk("partitioned").await;
    run(&pool, "CREATE TABLE p (id INT NOT NULL, v TEXT) PARTITION BY HASH (id)").await.expect("create");

    let mut tx = pool.begin().await.expect("begin");
    tx.execute("ALTER TABLE p ADD COLUMN w INT").await.expect("alter");
    tx.execute("CREATE INDEX p_v ON p (v)").await.expect("index");
    tx.execute("CREATE TABLE p2 (id INT NOT NULL) PARTITION BY HASH (id)").await.expect("create partitioned");
    tx.rollback().await.expect("rollback");
    assert!(run(&pool, "SELECT w FROM p").await.is_err(), "the added column outlived the rollback");
    assert!(index_names(&pool, "p").await.is_empty(), "the index outlived the rollback");
    assert!(!exists(&pool, "p2").await, "the created partitioned table outlived the rollback");

    run(&pool, "INSERT INTO p VALUES (1, 'a'), (2, 'b'), (3, 'c')").await.expect("insert");
    let mut tx = pool.begin().await.expect("begin");
    tx.execute("DROP TABLE p").await.expect("drop");
    tx.rollback().await.expect("rollback");
    let n: i64 = sqlx::query("SELECT count(*) FROM p").fetch_one(&pool).await.expect("the dropped table is back").get(0);
    assert_eq!(n, 3, "the rows of the table the block dropped");

    let mut tx = pool.begin().await.expect("begin");
    tx.execute("DROP TABLE p").await.expect("drop");
    tx.commit().await.expect("commit");
    assert!(!exists(&pool, "p").await, "a committed drop kept the table");
}

/// `CREATE TABLE … AS` in a block is dropped by its ROLLBACK (2026-11-12).
#[tokio::test]
async fn create_table_as_rolls_back_with_its_block() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE src (id INT PRIMARY KEY, v TEXT)").await.expect("create");
    run(&pool, "INSERT INTO src VALUES (1, 'a'), (2, 'b')").await.expect("insert");
    let mut tx = pool.begin().await.expect("begin");
    tx.execute("CREATE TABLE copied AS SELECT * FROM src").await.expect("ctas");
    let n: i64 = sqlx::query("SELECT count(*) FROM copied").fetch_one(&mut *tx).await.expect("read").get(0);
    assert_eq!(n, 2);
    tx.rollback().await.expect("rollback");
    assert!(!exists(&pool, "copied").await, "the table created AS a query outlived the rollback");
    run(&pool, "CREATE TABLE copied AS SELECT * FROM src").await.expect("created again");
}

/// `CREATE ROLE` and `DROP ROLE` in a block are the block's writes to the
/// role catalogue (2026-11-12): its ROLLBACK takes them back, and the block
/// sees its own. They ran as statements of their own and outlived it.
#[tokio::test]
async fn role_statements_roll_back_with_their_block() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE ROLE kept_role").await.expect("role");
    let mut tx = pool.begin().await.expect("begin");
    tx.execute("CREATE ROLE block_role").await.expect("create role");
    tx.execute("DROP ROLE block_role").await.expect("the block sees the role it created");
    tx.execute("CREATE ROLE block_role").await.expect("and that it dropped it");
    tx.execute("DROP ROLE kept_role").await.expect("drop role");
    tx.rollback().await.expect("rollback");
    run(&pool, "CREATE ROLE block_role").await.expect("the rolled-back role is gone");
    let again = run(&pool, "CREATE ROLE kept_role").await;
    assert!(again.is_err_and(|e| e.contains("already exists")), "the rolled-back DROP ROLE stayed");
}

/// `CREATE DATABASE` and `DROP DATABASE` cannot run in a block (25001), as
/// in PostgreSQL: no ROLLBACK takes a database back. One created in a block
/// used to stay through its ROLLBACK.
#[tokio::test]
async fn create_database_is_refused_in_a_block() {
    let (_s, pool) = server().await;
    let mut c = pool.acquire().await.expect("conn");
    c.execute("BEGIN").await.expect("begin");
    let refused = c.execute("CREATE DATABASE in_a_block").await.expect_err("refused");
    assert_eq!(refused.as_database_error().and_then(|e| e.code()).as_deref(), Some("25001"), "{refused}");
    c.execute("ROLLBACK").await.expect("rollback");
    c.execute("BEGIN").await.expect("begin");
    let refused = c.execute("DROP DATABASE IF EXISTS in_a_block").await.expect_err("refused");
    assert_eq!(refused.as_database_error().and_then(|e| e.code()).as_deref(), Some("25001"), "{refused}");
    c.execute("ROLLBACK").await.expect("rollback");
    let n: i64 = sqlx::query("SELECT count(*) FROM pg_database WHERE datname = 'in_a_block'")
        .fetch_one(&pool)
        .await
        .expect("pg_database")
        .get(0);
    assert_eq!(n, 0);
    // Outside a block it still runs.
    run(&pool, "CREATE DATABASE in_a_block").await.expect("create database");
    run(&pool, "DROP DATABASE in_a_block").await.expect("drop database");
}

async fn ids_of(pool: &sqlx::PgPool, sql: &str) -> Vec<i32> {
    sqlx::query(AssertSqlSafe(sql.to_string())).fetch_all(pool).await.expect(sql).iter().map(|r| r.get::<i32, _>(0)).collect()
}

/// `ALTER TABLE … RENAME TO` in a block is undone by its ROLLBACK and kept
/// by its COMMIT (2026-11-12): the name, the key a child holds on it, and
/// its `SERIAL`, which goes on from its rows. It applied at once and stayed.
#[tokio::test]
async fn a_rename_rolls_back_with_its_block() {
    let (_s, pool) = server().await;
    for sql in [
        "CREATE TABLE rn (id SERIAL PRIMARY KEY, name TEXT)",
        "CREATE TABLE rn_child (id INT PRIMARY KEY, rn_id INT REFERENCES rn(id))",
        "INSERT INTO rn (name) VALUES ('a'), ('b')",
        "INSERT INTO rn_child VALUES (1, 1)",
    ] {
        run(&pool, sql).await.expect(sql);
    }
    let mut tx = pool.begin().await.expect("begin");
    tx.execute("ALTER TABLE rn RENAME TO rn2").await.expect("rename");
    tx.execute("INSERT INTO rn2 (name) VALUES ('c')").await.expect("insert under the new name");
    tx.rollback().await.expect("rollback");
    assert!(exists(&pool, "rn").await && !exists(&pool, "rn2").await, "the rename outlived the rollback");
    run(&pool, "INSERT INTO rn (name) VALUES ('d')").await.expect("insert");
    assert_eq!(ids_of(&pool, "SELECT id FROM rn ORDER BY id").await, vec![1, 2, 4], "the SERIAL went back");
    assert!(run(&pool, "INSERT INTO rn_child VALUES (2, 99)").await.is_err(), "the child's key is not enforced");

    let mut tx = pool.begin().await.expect("begin");
    tx.execute("ALTER TABLE rn RENAME TO rn2").await.expect("rename");
    tx.commit().await.expect("commit");
    assert!(exists(&pool, "rn2").await && !exists(&pool, "rn").await);
    run(&pool, "INSERT INTO rn2 (name) VALUES ('e')").await.expect("insert");
    assert!(run(&pool, "INSERT INTO rn_child VALUES (3, 99)").await.is_err(), "the child's key follows the rename");
    run(&pool, "INSERT INTO rn_child VALUES (4, 2)").await.expect("a key that is there");
}

/// `DROP TABLE t` then `CREATE TABLE t` in one block (2026-11-12, the
/// pattern of a migration that rebuilds a table): its ROLLBACK brings the
/// old table and its rows back, its COMMIT keeps the new one. The drop ran
/// at the CREATE before, and a rolled-back block lost the table.
#[tokio::test]
async fn a_table_dropped_and_created_again_rolls_back_with_its_block() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE re (id SERIAL PRIMARY KEY, name TEXT)").await.expect("create");
    run(&pool, "INSERT INTO re (name) VALUES ('a'), ('b'), ('c')").await.expect("insert");

    let mut tx = pool.begin().await.expect("begin");
    tx.execute("DROP TABLE re").await.expect("drop");
    tx.execute("CREATE TABLE re (id SERIAL PRIMARY KEY, other TEXT)").await.expect("create again");
    tx.execute("INSERT INTO re (other) VALUES ('x')").await.expect("insert into the new table");
    tx.rollback().await.expect("rollback");
    assert_eq!(ids_of(&pool, "SELECT id FROM re ORDER BY id").await, vec![1, 2, 3], "the dropped table's rows");
    assert!(run(&pool, "SELECT name FROM re").await.is_ok(), "the old table's columns");
    run(&pool, "INSERT INTO re (name) VALUES ('d')").await.expect("insert");
    assert_eq!(ids_of(&pool, "SELECT id FROM re WHERE name = 'd'").await, vec![4], "its SERIAL went on from its rows");

    let mut tx = pool.begin().await.expect("begin");
    tx.execute("DROP TABLE re").await.expect("drop");
    tx.execute("CREATE TABLE re (id SERIAL PRIMARY KEY, other TEXT)").await.expect("create again");
    tx.execute("INSERT INTO re (other) VALUES ('x')").await.expect("insert");
    tx.commit().await.expect("commit");
    assert_eq!(ids_of(&pool, "SELECT id FROM re").await, vec![1], "the new table, its SERIAL from 1");
    assert!(run(&pool, "SELECT name FROM re").await.is_err(), "the old table's column");
    let hidden: i64 = sqlx::query("SELECT count(*) FROM information_schema.tables WHERE table_name LIKE '__txd%'")
        .fetch_one(&pool)
        .await
        .expect("pg_tables")
        .get(0);
    assert_eq!(hidden, 0);
}

/// A savepoint between the DROP and the CREATE: rolling back to it undoes
/// the CREATE and keeps the DROP, as PostgreSQL does, so the COMMIT drops
/// the table. The set-aside table's drop had to follow it back to its name.
#[tokio::test]
async fn rolling_back_to_a_savepoint_between_a_drop_and_its_create_keeps_the_drop() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE sp (id INT PRIMARY KEY)").await.expect("create");
    run(&pool, "INSERT INTO sp VALUES (1)").await.expect("insert");
    let mut c = pool.acquire().await.expect("conn");
    for sql in ["BEGIN", "DROP TABLE sp", "SAVEPOINT s", "CREATE TABLE sp (other TEXT)", "ROLLBACK TO SAVEPOINT s", "COMMIT"] {
        c.execute(sql).await.expect(sql);
    }
    assert!(!exists(&pool, "sp").await, "the drop before the savepoint was undone");
    run(&pool, "CREATE TABLE sp (id INT PRIMARY KEY)").await.expect("the name is free");
}

/// A block cannot rename a table it dropped (42P01, as PostgreSQL): the
/// rename would have left the drop at the commit naming nothing.
#[tokio::test]
async fn a_dropped_table_cannot_be_renamed_in_its_block() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE dr (id INT PRIMARY KEY)").await.expect("create");
    let mut c = pool.acquire().await.expect("conn");
    c.execute("BEGIN").await.expect("begin");
    c.execute("DROP TABLE dr").await.expect("drop");
    let e = c.execute("ALTER TABLE dr RENAME TO dr2").await.expect_err("refused");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("42P01"), "{e}");
    c.execute("ROLLBACK").await.expect("rollback");
    assert!(exists(&pool, "dr").await);
}
