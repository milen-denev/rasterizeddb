//! A data-modifying CTE's failure reaches the client with its own SQLSTATE.
//!
//! The CTE wraps a body's error ("the WITH clause 'w' failed: …"), and a
//! 23505 from inside it reached the client as 0A000. A client that branches
//! on 23505 (an upsert retry, a "name taken" message) never saw one.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor};

fn code(e: &sqlx::Error) -> String {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| e.to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cte_writes_failure_keeps_its_sqlstate() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE cw (id BIGINT PRIMARY KEY, n BIGINT UNIQUE, v BIGINT NOT NULL, CHECK (v < 100));
         INSERT INTO cw VALUES (1, 1, 1), (2, 2, 2);",
    ))
    .await
    .unwrap();
    for (sql, want) in [
        ("WITH w AS (UPDATE cw SET n = 2 WHERE id = 1 RETURNING id) SELECT * FROM w", "23505"),
        ("WITH w AS (INSERT INTO cw VALUES (3, 1, 3) RETURNING id) SELECT * FROM w", "23505"),
        ("WITH w AS (UPDATE cw SET v = 500 WHERE id = 1 RETURNING id) SELECT * FROM w", "23514"),
        ("WITH w AS (UPDATE cw SET v = NULL WHERE id = 1 RETURNING id) SELECT * FROM w", "23502"),
        ("WITH w AS (UPDATE cw SET n = 2 WHERE id = 1) SELECT 1", "23505"),
    ] {
        let e = sqlx::query(AssertSqlSafe(sql)).fetch_all(&pool).await.expect_err(sql);
        assert_eq!(code(&e), want, "{sql}: {e}");
    }
}

/// The SQLSTATE comes from the error, not from the text it quotes: a value
/// or a statement quoting a retryable error's words is not that error.
#[tokio::test(flavor = "multi_thread")]
async fn quoted_text_does_not_choose_the_sqlstate() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql("CREATE TABLE qt (id BIGINT PRIMARY KEY, m BIGINT); INSERT INTO qt VALUES (1, 1);"))
        .await
        .unwrap();
    for (sql, want) in [
        ("UPDATE qt SET m = 'deadlock detected' WHERE id = 1", "22P02"),
        ("UPDATE qt SET m = 'could not serialize access' WHERE id = 1", "22P02"),
        ("WITH w AS (UPDATE qt SET m = 'could not serialize access' WHERE id = 1 RETURNING id) SELECT * FROM w", "22P02"),
        ("WITH w AS (UPDATE qt SET m = 'canceling statement' WHERE id = 1 RETURNING id) SELECT * FROM w", "22P02"),
    ] {
        let e = sqlx::query(AssertSqlSafe(sql)).fetch_all(&pool).await.expect_err(sql);
        assert_eq!(code(&e), want, "{sql}: {e}");
    }
}

/// A quote inside the quoted text: the message embedded the statement as it
/// was, so the quote-span detection paired the wrong quotes and read the
/// text between them as the message's own words.
#[tokio::test(flavor = "multi_thread")]
async fn a_quote_inside_quoted_text_does_not_choose_the_sqlstate() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql("CREATE TABLE qq (id BIGINT PRIMARY KEY, m BIGINT); INSERT INTO qq VALUES (1, 1);"))
        .await
        .unwrap();
    for sql in [
        // The statement text quoted whole in "cannot parse subquery '…'".
        "WITH w AS (SELECT '(could not serialize access)'::int AS z) SELECT * FROM w",
        "WITH w AS (SELECT ' deadlock detected'::int AS z) SELECT * FROM w",
        "SELECT * FROM (SELECT '(could not serialize access)'::int AS z) d",
        "SELECT * FROM (SELECT ' deadlock detected'::int AS z) d",
        // A value quoted in "'…' cannot be read as".
        "WITH w AS (SELECT 'x'' deadlock detected'::int AS x FROM qq) SELECT * FROM w",
        "SELECT * FROM (SELECT 'x'' could not serialize access'::int AS x FROM qq) d",
        "WITH w AS (SELECT '(could not serialize access)'::int AS x FROM qq) SELECT * FROM w",
        "WITH w AS (UPDATE qq SET m = 'it''s could not serialize access' WHERE id = 1 RETURNING id) SELECT * FROM w",
        "WITH w AS (UPDATE qq SET m = ''' deadlock detected' WHERE id = 1 RETURNING id) SELECT * FROM w",
    ] {
        let e = sqlx::query(AssertSqlSafe(sql)).fetch_all(&pool).await.expect_err(sql);
        let got = code(&e);
        assert!(!got.starts_with("40"), "{sql}: a quoted value chose {got}: {e}");
    }
}
