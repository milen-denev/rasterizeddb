//! Escape strings (`E'…'`) and dollar-quoted strings everywhere a statement
//! can carry a literal.
//!
//! Only the predicate rewriter knew that a backslash in `E'…'` escapes the
//! next character. Every other scanner read `E'it\'s%'` as the literal `'it\'`
//! followed by stray text and a quote that swallowed the rest of the
//! statement, so `WHERE c LIKE E'it\'s%' ORDER BY id` failed naming a column
//! called `ORDER`. Both forms are now rewritten into standard strings where
//! statement text enters the engine.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql)))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<(i32, String)> {
    let rows = pool
        .fetch_all(sqlx::raw_sql(AssertSqlSafe(sql)))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.iter()
        .map(|r| (r.try_get::<i32, _>(0).unwrap(), r.try_get::<String, _>(1).unwrap()))
        .collect()
}

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let (s, pool) = server().await;
    run(&pool, "CREATE TABLE es (id INT PRIMARY KEY, t TEXT NOT NULL)").await;
    (s, pool)
}

#[tokio::test]
async fn insert_with_escape_strings() {
    let (_s, pool) = seeded().await;
    run(&pool, r"INSERT INTO es (id, t) VALUES (1, E'it\'s')").await;
    run(&pool, r"INSERT INTO es (id, t) VALUES (2, E'tab\there'), (3, E'back\\slash, comma')").await;
    run(&pool, r"INSERT INTO es (id, t) VALUES (4, E'ét\xc3\xa9 \101')").await;
    run(&pool, "INSERT INTO es (id, t) VALUES (5, $$dollar 'quoted', (paren)$$)").await;
    run(&pool, "INSERT INTO es (id, t) VALUES (6, $tag$a $$ b$tag$)").await;
    assert_eq!(
        rows(&pool, "SELECT id, t FROM es ORDER BY id").await,
        vec![
            (1, "it's".to_string()),
            (2, "tab\there".to_string()),
            (3, r"back\slash, comma".to_string()),
            (4, "été A".to_string()),
            (5, "dollar 'quoted', (paren)".to_string()),
            (6, "a $$ b".to_string()),
        ]
    );
}

#[tokio::test]
async fn where_like_with_an_escaped_quote_keeps_the_rest_of_the_statement() {
    let (_s, pool) = seeded().await;
    run(&pool, "INSERT INTO es (id, t) VALUES (1, 'it''s here'), (2, 'its'), (3, 'it''s there')").await;
    assert_eq!(
        rows(&pool, r"SELECT id, t FROM es WHERE t LIKE E'it\'s%' ORDER BY id DESC").await,
        vec![(3, "it's there".to_string()), (1, "it's here".to_string())]
    );
    assert_eq!(
        rows(&pool, r"SELECT id, t FROM es WHERE t = E'its' OR t = $$it's here$$ ORDER BY id").await,
        vec![(1, "it's here".to_string()), (2, "its".to_string())]
    );
}

#[tokio::test]
async fn update_set_and_returning() {
    let (_s, pool) = seeded().await;
    run(&pool, "INSERT INTO es (id, t) VALUES (1, 'a'), (2, 'b')").await;
    let returned = rows(
        &pool,
        r"UPDATE es SET t = E'x\'y, z' WHERE id = 1 RETURNING id, t",
    )
    .await;
    assert_eq!(returned, vec![(1, "x'y, z".to_string())]);
    let returned = rows(
        &pool,
        r"INSERT INTO es (id, t) VALUES (3, E'r\'s') RETURNING id, t",
    )
    .await;
    assert_eq!(returned, vec![(3, "r's".to_string())]);
    assert_eq!(
        rows(&pool, "SELECT id, t FROM es ORDER BY id").await,
        vec![
            (1, "x'y, z".to_string()),
            (2, "b".to_string()),
            (3, "r's".to_string()),
        ]
    );
}

#[tokio::test]
async fn escape_strings_in_comments_and_identifiers_are_left_alone() {
    let (_s, pool) = seeded().await;
    run(
        &pool,
        "-- a comment with E'it\\'s in it\nINSERT INTO es (id, t) /* E'\\' */ VALUES (1, 'ok')",
    )
    .await;
    // A word ending in E right before a quote is not an escape-string prefix
    // (`ELSE'\'` is the keyword then a standard string); pinned in the
    // `sql_scan` unit tests, since a CASE in this position is its own feature.
    assert_eq!(rows(&pool, "SELECT id, t FROM es").await, vec![(1, "ok".to_string())]);
}

#[tokio::test]
async fn a_bound_parameter_next_to_an_escape_string() {
    let (_s, pool) = seeded().await;
    run(&pool, "INSERT INTO es (id, t) VALUES (1, 'it''s $1')").await;
    // Extended protocol: the statement text holds an E-string with a `$1`
    // inside it, which must stay text, and a real placeholder outside.
    let got: Vec<(i32, String)> = sqlx::query(AssertSqlSafe(r"SELECT id, t FROM es WHERE t = E'it\'s $1' AND id = $1"))
        .bind(1i32)
        .fetch_all(&pool)
        .await
        .expect("query")
        .iter()
        .map(|r| (r.get::<i32, _>(0), r.get::<String, _>(1)))
        .collect();
    assert_eq!(got, vec![(1, "it's $1".to_string())]);
}

#[tokio::test]
async fn an_escape_that_decodes_to_nothing_valid_is_refused() {
    let (_s, pool) = seeded().await;
    let err = pool
        .execute(sqlx::raw_sql(AssertSqlSafe(r"INSERT INTO es (id, t) VALUES (1, E'\xff')")))
        .await
        .expect_err("not UTF-8");
    assert!(err.to_string().contains("invalid byte sequence"), "{err}");
    let n: i64 = pool
        .fetch_one(sqlx::raw_sql(AssertSqlSafe("SELECT COUNT(*) FROM es")))
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0);
}
