//! INSERT with non-ASCII identifiers and values.
//!
//! The tuple parser located each parenthesis by BYTE offset and then walked
//! the statement by CHARACTER count from there (`char_indices().skip(byte)`),
//! so every multi-byte character before the column list or the VALUES tuple
//! moved the walk past the parenthesis it was meant to start at. Separately,
//! the keyword scanners sliced the upper-cased statement at every byte, which
//! panics inside a multi-byte character of an unquoted identifier.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql)))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

async fn pairs(pool: &sqlx::PgPool, sql: &str) -> Vec<(i32, String)> {
    let rows = pool
        .fetch_all(sqlx::raw_sql(AssertSqlSafe(sql)))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out: Vec<(i32, String)> = rows
        .iter()
        .map(|r| {
            (
                r.try_get::<i32, _>(0).expect("int"),
                r.try_get::<String, _>(1).expect("text"),
            )
        })
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn quoted_non_ascii_table_and_columns_single_row() {
    let (_s, pool) = server().await;
    run(&pool, r#"CREATE TABLE "tëst" ("日本" INT PRIMARY KEY, "naïve" TEXT NOT NULL)"#)
        .await
        .expect("create");
    run(&pool, r#"INSERT INTO "tëst" ("日本", "naïve") VALUES (1, 'crème brûlée')"#)
        .await
        .expect("insert");
    // The column list in the other order, and a value holding parentheses and
    // a comma after non-ASCII text.
    run(&pool, r#"INSERT INTO "tëst" ("naïve", "日本") VALUES ('日本語 (a, b)', 2)"#)
        .await
        .expect("insert reordered");
    assert_eq!(
        pairs(&pool, r#"SELECT "日本", "naïve" FROM "tëst""#).await,
        vec![
            (1, "crème brûlée".to_string()),
            (2, "日本語 (a, b)".to_string()),
        ]
    );
}

#[tokio::test]
async fn quoted_non_ascii_table_and_columns_multi_row() {
    let (_s, pool) = server().await;
    run(&pool, r#"CREATE TABLE "tëst" ("日本" INT PRIMARY KEY, "naïve" TEXT NOT NULL)"#)
        .await
        .expect("create");
    run(
        &pool,
        r#"INSERT INTO "tëst" ("naïve", "日本") VALUES ('ä', 1), ('ö''s (x)', 2), ('€', 3)"#,
    )
    .await
    .expect("multi-row insert");
    assert_eq!(
        pairs(&pool, r#"SELECT "日本", "naïve" FROM "tëst""#).await,
        vec![
            (1, "ä".to_string()),
            (2, "ö's (x)".to_string()),
            (3, "€".to_string()),
        ]
    );
}

#[tokio::test]
async fn ascii_names_with_non_ascii_values() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE plain (id INT PRIMARY KEY, label TEXT NOT NULL, note TEXT)")
        .await
        .expect("create");
    run(&pool, "INSERT INTO plain (label, id, note) VALUES ('ünïcödé', 1, 'ß')")
        .await
        .expect("insert");
    run(&pool, "INSERT INTO plain (label, id, note) VALUES ('Ωmega', 2, 'x'), ('🙂', 3, '日本')")
        .await
        .expect("multi-row insert");
    assert_eq!(
        pairs(&pool, "SELECT id, label FROM plain").await,
        vec![
            (1, "ünïcödé".to_string()),
            (2, "Ωmega".to_string()),
            (3, "🙂".to_string()),
        ]
    );
    assert_eq!(
        pairs(&pool, "SELECT id, note FROM plain WHERE id = 3").await,
        vec![(3, "日本".to_string())]
    );
}

#[tokio::test]
async fn unquoted_non_ascii_identifiers() {
    let (_s, pool) = server().await;
    // PostgreSQL accepts non-ASCII letters in unquoted identifiers. Whether or
    // not this engine does, the statement must not take the worker down.
    if run(&pool, "CREATE TABLE tëst2 (id INT PRIMARY KEY, naïve TEXT NOT NULL)")
        .await
        .is_err()
    {
        return;
    }
    let single = run(&pool, "INSERT INTO tëst2 (id, naïve) VALUES (1, 'é')").await;
    let multi = run(&pool, "INSERT INTO tëst2 (naïve, id) VALUES ('ü', 2), ('ø', 3)").await;
    // The connection must still work afterwards.
    run(&pool, "SELECT 1").await.expect("server still answers");
    single.expect("single-row insert");
    multi.expect("multi-row insert");
    assert_eq!(
        pairs(&pool, "SELECT id, naïve FROM tëst2").await,
        vec![
            (1, "é".to_string()),
            (2, "ü".to_string()),
            (3, "ø".to_string()),
        ]
    );
}
