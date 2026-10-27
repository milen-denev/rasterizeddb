//! A failed `UPDATE … FROM` or `DELETE … USING` leaves nothing behind.
//!
//! Both run the join and then write each target row back with a statement of
//! its own. A row that failed (a CHECK, a foreign key) failed the statement,
//! but the rows written before it stayed: `p` read `[(1,5),(2,6),(3,0),(4,0)]`
//! after an `UPDATE` that PostgreSQL rejects whole with 23514.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn pairs(pool: &sqlx::PgPool, sql: &str) -> Vec<(i64, i64)> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| (r.get::<i64, _>(0), r.get::<i64, _>(1)))
        .collect()
}

async fn run(pool: &sqlx::PgPool, sql: &str) {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn code(e: &sqlx::Error) -> String {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_default()
}

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    run(
        &pool,
        "CREATE TABLE p (id BIGINT PRIMARY KEY, n BIGINT NOT NULL, CHECK (n < 100));
         INSERT INTO p VALUES (1, 0), (2, 0), (3, 0), (4, 0);
         CREATE TABLE src (id BIGINT PRIMARY KEY, v BIGINT NOT NULL);
         INSERT INTO src VALUES (1, 5), (2, 6), (3, 500), (4, 7);
         CREATE TABLE child (id BIGINT PRIMARY KEY, pid BIGINT REFERENCES p (id));
         INSERT INTO child VALUES (10, 3);",
    )
    .await;
    (server, pool)
}

const BEFORE: [(i64, i64); 4] = [(1, 0), (2, 0), (3, 0), (4, 0)];

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_autocommit_update_from_writes_nothing() {
    let (_s, pool) = seeded().await;
    let e = pool
        .execute("UPDATE p SET n = src.v FROM src WHERE p.id = src.id")
        .await
        .expect_err("row 3 breaks the CHECK");
    assert_eq!(code(&e), "23514", "{e}");
    assert_eq!(pairs(&pool, "SELECT id, n FROM p ORDER BY id").await, BEFORE, "the rows before the failure stayed");
    // The same with a computed value and RETURNING.
    let e = pool
        .execute("UPDATE p SET n = src.v + p.n FROM src WHERE p.id = src.id RETURNING p.id")
        .await
        .expect_err("row 3 breaks the CHECK");
    assert_eq!(code(&e), "23514", "{e}");
    assert_eq!(pairs(&pool, "SELECT id, n FROM p ORDER BY id").await, BEFORE);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_autocommit_delete_using_deletes_nothing() {
    let (_s, pool) = seeded().await;
    let e = pool
        .execute("DELETE FROM p USING src WHERE p.id = src.id")
        .await
        .expect_err("row 3 is referenced");
    assert_eq!(code(&e), "23503", "{e}");
    assert_eq!(pairs(&pool, "SELECT id, n FROM p ORDER BY id").await, BEFORE, "the rows before the failure are gone");
}

/// Inside a block the statement is rolled back to its start, which a
/// savepoint shows, and the block commits only what came before it.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_join_write_in_a_block_rolls_back_to_its_start() {
    let (_s, pool) = seeded().await;
    let mut c = pool.acquire().await.expect("conn");
    c.execute("BEGIN").await.unwrap();
    c.execute("UPDATE p SET n = 50 WHERE id = 4").await.unwrap();
    c.execute("SAVEPOINT a").await.unwrap();
    let e = c
        .execute("UPDATE p SET n = src.v FROM src WHERE p.id = src.id")
        .await
        .expect_err("row 3 breaks the CHECK");
    assert_eq!(code(&e), "23514", "{e}");
    c.execute("ROLLBACK TO SAVEPOINT a").await.unwrap();
    let e = c.execute("DELETE FROM p USING src WHERE p.id = src.id").await.expect_err("row 3 is referenced");
    assert_eq!(code(&e), "23503", "{e}");
    c.execute("ROLLBACK TO SAVEPOINT a").await.unwrap();
    c.execute("COMMIT").await.unwrap();
    drop(c);
    assert_eq!(
        pairs(&pool, "SELECT id, n FROM p ORDER BY id").await,
        [(1, 0), (2, 0), (3, 0), (4, 50)],
        "a failed statement's rows survived its rollback, or the block lost what came before"
    );
}

/// A failed statement in a block with no savepoint ends the block: the
/// writes before the failure never commit.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_join_write_in_a_block_aborts_it() {
    let (_s, pool) = seeded().await;
    let mut c = pool.acquire().await.expect("conn");
    c.execute("BEGIN").await.unwrap();
    let e = c
        .execute("UPDATE p SET n = src.v FROM src WHERE p.id = src.id")
        .await
        .expect_err("row 3 breaks the CHECK");
    assert_eq!(code(&e), "23514", "{e}");
    let e = c.execute("SELECT 1").await.expect_err("the block is aborted");
    assert_eq!(code(&e), "25P02", "{e}");
    let _ = c.execute("COMMIT").await;
    drop(c);
    assert_eq!(pairs(&pool, "SELECT id, n FROM p ORDER BY id").await, BEFORE);
}

/// The other statements that write through statements of their own: a
/// multi-row upsert (each row's conflict becomes an `UPDATE`) and a
/// data-modifying CTE, each failed by a later write.
#[tokio::test(flavor = "multi_thread")]
async fn other_compound_writes_that_fail_write_nothing() {
    let (_s, pool) = seeded().await;
    let e = pool
        .execute("INSERT INTO p VALUES (1, 5), (2, 6), (3, 500) ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n")
        .await
        .expect_err("row 3 breaks the CHECK");
    assert_eq!(code(&e), "23514", "{e}");
    assert_eq!(pairs(&pool, "SELECT id, n FROM p ORDER BY id").await, BEFORE, "the upsert's earlier rows stayed");
    let e = pool
        .execute("WITH u AS (UPDATE p SET n = 5 WHERE id = 1 RETURNING id) UPDATE p SET n = 500 WHERE id = 2")
        .await
        .expect_err("row 2 breaks the CHECK");
    assert_eq!(code(&e), "23514", "{e}");
    assert_eq!(pairs(&pool, "SELECT id, n FROM p ORDER BY id").await, BEFORE, "the CTE's write stayed");
}

/// A data-modifying CTE without `RETURNING` runs once.
#[tokio::test(flavor = "multi_thread")]
async fn a_cte_write_without_returning_runs_once() {
    let (_s, pool) = seeded().await;
    run(&pool, "WITH u AS (UPDATE p SET n = n + 1 WHERE id = 1) SELECT 1").await;
    assert_eq!(pairs(&pool, "SELECT id, n FROM p WHERE id = 1").await, [(1, 1)]);
}
