//! `DROP TABLE` inside a transaction block rolls back with it, as
//! PostgreSQL's does.
//!
//! It was applied at once: `BEGIN; DROP TABLE t; ROLLBACK;` destroyed the
//! table and its rows, with the only warning in the server log. The drop now
//! runs at the block's COMMIT, under the ACCESS EXCLUSIVE it took when it was
//! issued; a ROLLBACK, a failed statement that ends the block, or a ROLLBACK
//! TO a savepoint before it leaves the table untouched.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) {
    sqlx::raw_sql(AssertSqlSafe(sql.to_string())).execute(pool).await.unwrap_or_else(|e| panic!("`{sql}`: {e}"));
}

async fn count(pool: &sqlx::PgPool, table: &str) -> Option<i64> {
    sqlx::query(AssertSqlSafe(format!("SELECT count(*) FROM {table}"))).fetch_one(pool).await.ok().map(|r| r.get(0))
}

async fn in_block(pool: &sqlx::PgPool, statements: &[&str], commit: bool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    for sql in statements {
        sqlx::raw_sql(AssertSqlSafe(sql.to_string())).execute(&mut *tx).await?;
    }
    if commit { tx.commit().await } else { tx.rollback().await }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_drop_keeps_the_table_and_its_rows() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE zz (a int); INSERT INTO zz VALUES (1), (2);").await;
    in_block(&pool, &["DROP TABLE zz"], false).await.expect("block");
    assert_eq!(count(&pool, "zz").await, Some(2));
    // Two tables at once, and a savepoint rolled back over the drop.
    run(&pool, "CREATE TABLE z2 (a int); INSERT INTO z2 VALUES (3);").await;
    in_block(&pool, &["DROP TABLE zz, z2"], false).await.expect("block");
    assert_eq!((count(&pool, "zz").await, count(&pool, "z2").await), (Some(2), Some(1)));
    in_block(&pool, &["SAVEPOINT s", "DROP TABLE zz", "ROLLBACK TO SAVEPOINT s"], true).await.expect("block");
    assert_eq!(count(&pool, "zz").await, Some(2));
    // A string of statements is one block too: its failure keeps the table.
    assert!(sqlx::raw_sql("DROP TABLE zz; SELECT 1/0").execute(&pool).await.is_err());
    assert_eq!(count(&pool, "zz").await, Some(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_committed_drop_drops() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE zz (a int); CREATE TABLE z2 (a int); CREATE TABLE z3 (b int);").await;
    in_block(&pool, &["DROP TABLE zz", "INSERT INTO z3 VALUES (1)"], true).await.expect("block");
    assert_eq!(count(&pool, "zz").await, None);
    assert_eq!(count(&pool, "z3").await, Some(1));
    in_block(&pool, &["DROP TABLE z2, z3"], true).await.expect("block");
    assert_eq!((count(&pool, "z2").await, count(&pool, "z3").await), (None, None));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_table_is_gone_for_the_rest_of_the_block() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE zz (a int); INSERT INTO zz VALUES (1);").await;
    // A second drop finds nothing; IF EXISTS skips it.
    let e = in_block(&pool, &["DROP TABLE zz", "DROP TABLE zz"], true).await.expect_err("second drop");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("42P01"));
    in_block(&pool, &["DROP TABLE zz", "DROP TABLE IF EXISTS zz"], false).await.expect("block");
    assert_eq!(count(&pool, "zz").await, Some(1));
    // Dropped and created again in one block (the drop runs first there).
    in_block(&pool, &["DROP TABLE zz", "CREATE TABLE zz (b text)", "INSERT INTO zz VALUES ('x')"], true)
        .await
        .expect("block");
    let b: String = sqlx::query("SELECT b FROM zz").fetch_one(&pool).await.expect("read").get(0);
    assert_eq!(b, "x");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_cascade_keeps_the_dependent_key() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE tp (id int PRIMARY KEY); CREATE TABLE tc (id int, p int REFERENCES tp(id)); INSERT INTO tp VALUES (1);")
        .await;
    in_block(&pool, &["DROP TABLE tp CASCADE"], false).await.expect("block");
    let e = sqlx::raw_sql("INSERT INTO tc VALUES (1, 999)").execute(&pool).await.expect_err("the key is still there");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("23503"));
    // Without CASCADE it is refused when issued, as PostgreSQL refuses it.
    let e = in_block(&pool, &["DROP TABLE tp"], true).await.expect_err("dependent");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("2BP01"));
}

#[tokio::test(flavor = "multi_thread")]
async fn tables_keyed_to_each_other_drop_together_at_commit() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE mp (id int PRIMARY KEY); CREATE TABLE mc (id int, p int REFERENCES mp(id));").await;
    in_block(&pool, &["DROP TABLE mp, mc"], true).await.expect("block");
    assert_eq!((count(&pool, "mp").await, count(&pool, "mc").await), (None, None));
}

/// The drop holds ACCESS EXCLUSIVE from when it is issued: another session's
/// write waits for the block, then finds the table gone, as in PostgreSQL. A
/// write let through would have committed rows the commit then dropped.
#[tokio::test(flavor = "multi_thread")]
async fn another_session_waits_for_the_block_then_finds_the_table_gone() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE zz (a int)").await;
    let mut tx = pool.begin().await.expect("begin");
    sqlx::raw_sql("DROP TABLE zz").execute(&mut *tx).await.expect("drop");
    let other = pool.clone();
    let insert = tokio::spawn(async move { sqlx::raw_sql("INSERT INTO zz VALUES (1)").execute(&other).await });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert!(!insert.is_finished(), "the insert went ahead of the block's drop");
    tx.commit().await.expect("commit");
    let e = insert.await.expect("join").expect_err("the table is gone");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("42P01"));
}
