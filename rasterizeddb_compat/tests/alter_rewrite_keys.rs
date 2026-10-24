//! An ALTER that rewrites a table's rows keeps every key it had, and keeps
//! each version's stamps: a duplicate is still refused afterwards, a key a
//! committed DELETE freed is free, and a block that wrote or deleted before
//! its ALTER still gets its rows back, or loses them, on ROLLBACK.
//!
//! B5 recreates the files and writes every version with its own xmin and
//! xmax (`Table::insert_version`), with no uniqueness check of its own.
//! Expected answers are PostgreSQL 17's (which also rolls the ALTER back;
//! DDL is not transactional here, so only the rows are asserted).

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn refused(pool: &sqlx::PgPool, sql: &str, code: &str) {
    match sqlx::query(AssertSqlSafe(sql)).execute(pool).await {
        Ok(done) => panic!("{sql}: PostgreSQL refuses this with {code}, but it affected {} rows", done.rows_affected()),
        Err(e) => assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some(code), "{sql}: {e}"),
    }
}

async fn ids(pool: &sqlx::PgPool) -> Vec<i32> {
    sqlx::query("SELECT id FROM r ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("read ids")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rewritten_table_keeps_its_keys_and_its_versions() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE r (id INT PRIMARY KEY, email TEXT UNIQUE, a INT, b INT, UNIQUE (a, b));
         INSERT INTO r (id, email, a, b) VALUES (1, 'one', 1, 1), (2, 'two', 2, 2), (3, 'three', 3, 3);
         DELETE FROM r WHERE id = 3;",
    ))
    .await
    .expect("schema");

    // A rewrite of a populated table, with a deleted version in it.
    pool.execute("ALTER TABLE r ADD COLUMN note TEXT").await.expect("add column");
    assert_eq!(ids(&pool).await, vec![1, 2], "the rewrite changed which rows are visible");

    // Every key still refuses a duplicate of a live row.
    refused(&pool, "INSERT INTO r (id, email, a, b) VALUES (1, 'x', 9, 9)", "23505").await;
    refused(&pool, "INSERT INTO r (id, email, a, b) VALUES (9, 'one', 9, 9)", "23505").await;
    refused(&pool, "INSERT INTO r (id, email, a, b) VALUES (9, 'x', 2, 2)", "23505").await;
    // The deleted row's keys are free.
    pool.execute("INSERT INTO r (id, email, a, b) VALUES (3, 'three', 3, 3)").await.expect("reuse a deleted key");
    // The arbiter finds the rewritten row.
    let done = pool
        .execute("INSERT INTO r (id, email, a, b) VALUES (1, 'one', 1, 1) ON CONFLICT (id) DO UPDATE SET note = 'u'")
        .await
        .expect("upsert");
    assert_eq!(done.rows_affected(), 1);
    let note: Option<String> = sqlx::query("SELECT note FROM r WHERE id = 1").fetch_one(&pool).await.expect("note").get(0);
    assert_eq!(note.as_deref(), Some("u"));
    assert_eq!(ids(&pool).await, vec![1, 2, 3]);

    // A block's own insert, carried through its ALTER, is gone on ROLLBACK.
    let mut tx = pool.begin().await.expect("begin");
    (&mut *tx).execute("INSERT INTO r (id, email, a, b) VALUES (10, 'ten', 10, 10)").await.expect("insert in block");
    (&mut *tx).execute("ALTER TABLE r ADD COLUMN n2 INT").await.expect("alter in block");
    tx.rollback().await.expect("rollback");
    assert_eq!(ids(&pool).await, vec![1, 2, 3], "a rolled-back insert survived the rewrite");
    pool.execute("INSERT INTO r (id, email, a, b) VALUES (10, 'ten', 10, 10)").await.expect("the rolled-back key is free");

    // A block's own delete, carried through its ALTER, is undone on ROLLBACK,
    // and the revived row still holds its keys.
    let mut tx = pool.begin().await.expect("begin");
    (&mut *tx).execute("DELETE FROM r WHERE id = 2").await.expect("delete in block");
    (&mut *tx).execute("ALTER TABLE r ADD COLUMN n3 INT").await.expect("alter in block");
    let inside: Vec<i32> = sqlx::query("SELECT id FROM r ORDER BY id")
        .fetch_all(&mut *tx)
        .await
        .expect("read in block")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(inside, vec![1, 3, 10], "the block's own delete was undone by its rewrite");
    tx.rollback().await.expect("rollback");
    assert_eq!(ids(&pool).await, vec![1, 2, 3, 10], "a rolled-back delete was kept by the rewrite");
    refused(&pool, "INSERT INTO r (id, email, a, b) VALUES (2, 'y', 8, 8)", "23505").await;
    refused(&pool, "INSERT INTO r (id, email, a, b) VALUES (8, 'two', 8, 8)", "23505").await;

    // A committed delete an older snapshot can still see is carried, with
    // its xmax: deleted for everyone after it, kept for that snapshot. (A
    // block that never read `r` holds no lock the ALTER waits for.)
    pool.execute("CREATE TABLE side (x INT)").await.expect("side table");
    let mut old = pool.acquire().await.expect("connection");
    (&mut *old).execute("BEGIN ISOLATION LEVEL REPEATABLE READ").await.expect("begin");
    (&mut *old).execute("SELECT count(*) FROM side").await.expect("take the snapshot");
    pool.execute("DELETE FROM r WHERE id = 10").await.expect("delete");
    pool.execute("ALTER TABLE r ADD COLUMN n4 INT").await.expect("rewrite under an old snapshot");
    assert_eq!(ids(&pool).await, vec![1, 2, 3], "a committed delete came back through the rewrite");
    // PostgreSQL shows this snapshot an empty table (its rewrite is not
    // MVCC-safe); here it keeps reading what it read before.
    let seen: Vec<i32> = sqlx::query("SELECT id FROM r ORDER BY id")
        .fetch_all(&mut *old)
        .await
        .expect("read under the old snapshot")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(seen, vec![1, 2, 3, 10], "the old snapshot lost a row it could see");
    (&mut *old).execute("COMMIT").await.expect("commit");
}
