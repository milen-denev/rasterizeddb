//! The `__cte_` prefix belongs to the temporary tables a statement
//! materialises (CTEs, derived tables). A user table under it was taken for
//! one: memory-backed, so every row was lost at a restart; exempt from table
//! locks; and reaped at startup. Nothing reported any of it. So the prefix is
//! refused at `CREATE TABLE` and as a `RENAME` target, with 42939
//! (`reserved_name`), while every statement that materialises a temporary
//! goes on working.
//!
//! Mutation: `cte::reserved_name_refusal` always `None` (the creates and the
//! rename succeed). And `create_temporary` without its scope (every CTE fails
//! with 42939).

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn code(pool: &sqlx::PgPool, sql: &str) -> Option<String> {
    match pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await {
        Ok(_) => None,
        Err(e) => Some(e.as_database_error().and_then(|d| d.code().map(|c| c.to_string())).unwrap_or_else(|| e.to_string())),
    }
}

async fn run(pool: &sqlx::PgPool, sql: &str) {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn count(pool: &sqlx::PgPool, sql: &str) -> i64 {
    let row = sqlx::query(AssertSqlSafe(sql.to_string())).fetch_one(pool).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    row.try_get::<i64, _>(0).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn the_statement_temporary_prefix_is_refused_and_ctes_still_work() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");

    for create in [
        "CREATE TABLE __cte_orders (id INT NOT NULL, v TEXT)",
        "CREATE TABLE \"__CTE_Mixed\" (id INT)",
        "CREATE TABLE IF NOT EXISTS __cte_x (id INT)",
        "CREATE TABLE __cte_parts (tenant_id BIGINT NOT NULL, id BIGINT) PARTITION BY HASH (tenant_id) PARTITIONS 4",
    ] {
        assert_eq!(code(&pool, create).await.as_deref(), Some("42939"), "{create}");
    }
    assert_eq!(code(&pool, "SELECT id FROM __cte_orders").await.as_deref(), Some("42P01"), "a refused create left a table");

    run(&pool, "CREATE TABLE orders (id INT NOT NULL, v TEXT)").await;
    run(&pool, "INSERT INTO orders (id, v) VALUES (1, 'a'), (2, 'b'), (3, 'c')").await;
    assert_eq!(code(&pool, "ALTER TABLE orders RENAME TO __cte_orders").await.as_deref(), Some("42939"));
    assert_eq!(code(&pool, "ALTER TABLE orders RENAME TO __Cte_orders").await.as_deref(), Some("42939"));
    assert_eq!(count(&pool, "SELECT count(*) FROM orders").await, 3, "a refused rename moved the table");
    // A name merely containing the prefix is an ordinary name.
    run(&pool, "CREATE TABLE my__cte_notes (id INT)").await;
    run(&pool, "ALTER TABLE my__cte_notes RENAME TO notes__cte_").await;

    // Every way a statement materialises a temporary still works.
    assert_eq!(count(&pool, "WITH c AS (SELECT id FROM orders WHERE id > 1) SELECT count(*) FROM c").await, 2);
    assert_eq!(count(&pool, "SELECT count(*) FROM (SELECT id FROM orders WHERE id < 3) d").await, 2);
    assert_eq!(count(&pool, "WITH k AS (SELECT 7 AS a) SELECT count(*) FROM k").await, 1);
    assert_eq!(
        count(&pool, "WITH ins AS (INSERT INTO orders (id, v) VALUES (4, 'd') RETURNING 1) SELECT count(*) FROM ins").await,
        1
    );
    assert_eq!(
        count(
            &pool,
            "WITH a AS (SELECT id FROM orders), b AS (SELECT id FROM a WHERE id > 2) \
             SELECT count(*) FROM b JOIN orders o ON o.id = b.id"
        )
        .await,
        2
    );
    assert_eq!(count(&pool, "SELECT count(*) FROM orders").await, 4);
}
