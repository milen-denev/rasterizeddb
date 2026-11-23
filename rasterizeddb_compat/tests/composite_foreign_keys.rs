//! Composite (multi-column) foreign keys, over a real client.
//!
//! A key over `(a, b)` holds on the tuple: a row references the parent row
//! holding both of its values at once. Every expectation is PostgreSQL 17's
//! answer to the same statements (run against `postgres:17`).

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), (String, String)> {
    sqlx::raw_sql(AssertSqlSafe(sql.to_string())).execute(pool).await.map(|_| ()).map_err(|e| {
        let code = e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_default();
        (code, e.to_string())
    })
}

async fn ok(pool: &sqlx::PgPool, sql: &str) {
    if let Err((code, e)) = run(pool, sql).await {
        panic!("`{sql}` must succeed, failed {code}: {e}");
    }
}

async fn refused(pool: &sqlx::PgPool, sql: &str, code: &str) -> String {
    match run(pool, sql).await {
        Ok(()) => panic!("`{sql}` was accepted; PostgreSQL refuses it with {code}"),
        Err((got, e)) => {
            assert_eq!(got, code, "`{sql}`: {e}");
            e
        }
    }
}

/// Every row of `sql` as `a|b` text, `NULL` for a NULL.
async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"))
        .iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_composite_key_holds_on_the_tuple() {
    let (_s, pool) = server().await;
    ok(&pool, "CREATE TABLE p (x int, y int, z int, PRIMARY KEY (x, y))").await;
    ok(&pool, "INSERT INTO p VALUES (1, 1, 0), (1, 2, 0), (2, 1, 0)").await;
    ok(&pool, "CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a, b) REFERENCES p)").await;
    ok(&pool, "INSERT INTO c VALUES (1, 1, 1), (2, NULL, 9), (3, 1, NULL)").await;
    let e = refused(&pool, "INSERT INTO c VALUES (4, 2, 2)", "23503").await;
    assert!(e.contains("c_a_b_fkey") && e.contains("(a, b)=(2, 2)"), "{e}");
    // Prepared, with parameters: the same check.
    let e = sqlx::query("INSERT INTO c VALUES ($1, $2, $3)")
        .bind(5_i32)
        .bind(2_i32)
        .bind(2_i32)
        .execute(&pool)
        .await
        .expect_err("no parent (2, 2)");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("23503"), "{e}");
    sqlx::query("INSERT INTO c VALUES ($1, $2, $3)").bind(5_i32).bind(2_i32).bind(1_i32).execute(&pool).await.unwrap();
    refused(&pool, "UPDATE c SET b = 2 WHERE id = 5", "23503").await;
    ok(&pool, "UPDATE c SET a = 1, b = 2 WHERE id = 5").await;
    refused(&pool, "DELETE FROM p WHERE x = 1 AND y = 2", "23503").await;
    refused(&pool, "UPDATE p SET y = 3 WHERE x = 1 AND y = 2", "23503").await;
    ok(&pool, "DELETE FROM p WHERE x = 2 AND y = 1").await;
    assert_eq!(
        rows(&pool, "SELECT conname, conkey::text, confkey::text, confmatchtype::text FROM pg_constraint WHERE contype = 'f'").await,
        ["c_a_b_fkey|{2,3}|{1,2}|s"]
    );
    refused(&pool, "CREATE TABLE d (a int, b int, FOREIGN KEY (a, b) REFERENCES p (x))", "42830").await;
    refused(&pool, "CREATE TABLE d (a int, b int, FOREIGN KEY (a, b) REFERENCES p (x, z))", "42830").await;
    refused(&pool, "CREATE TABLE d (a int, b int, FOREIGN KEY (a, b) REFERENCES p MATCH PARTIAL)", "0A000").await;
    ok(&pool, "CREATE TABLE f (a int, b int, FOREIGN KEY (a, b) REFERENCES p MATCH FULL)").await;
    let e = refused(&pool, "INSERT INTO f VALUES (1, NULL)", "23503").await;
    assert!(e.contains("MATCH FULL does not allow mixing of null and nonnull key values"), "{e}");
    ok(&pool, "INSERT INTO f VALUES (NULL, NULL)").await;
}
