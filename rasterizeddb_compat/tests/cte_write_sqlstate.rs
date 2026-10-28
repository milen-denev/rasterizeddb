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

/// A double quote inside a value: "invalid input syntax for type bigint:
/// "a" could not serialize access "b"" paired the value's own quotes with
/// the message's and read what lay between as the message's words.
#[tokio::test(flavor = "multi_thread")]
async fn a_double_quote_inside_a_value_does_not_choose_the_sqlstate() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql("CREATE TABLE dq (id BIGINT PRIMARY KEY, m BIGINT); INSERT INTO dq VALUES (1, 1);"))
        .await
        .unwrap();
    for words in ["could not serialize access", "deadlock detected", "canceling statement"] {
        for sql in [
            format!(r#"UPDATE dq SET m = 'a" {words} "b' WHERE id = 1"#),
            format!(r#"WITH w AS (UPDATE dq SET m = 'a" {words} "b' WHERE id = 1 RETURNING id) SELECT * FROM w"#),
            format!(r#"INSERT INTO dq VALUES (2, 'a" {words} "b')"#),
            format!(r#"WITH w AS (SELECT 'a" {words} "b'::int AS z FROM dq) SELECT * FROM w"#),
        ] {
            let e = sqlx::query(AssertSqlSafe(sql.clone())).fetch_all(&pool).await.expect_err(&sql);
            let got = code(&e);
            assert!(!got.starts_with("40") && got != "57014", "{sql}: a quoted value chose {got}: {e}");
            if sql.contains("UPDATE") || sql.starts_with("INSERT") {
                assert_eq!(got, "22P02", "{sql}: {e}");
            }
        }
    }
    // A bound value is not in the statement's text, so only the message's
    // own escaping keeps it inside its quotes.
    for words in ["could not serialize access", "deadlock detected", "canceling statement"] {
        let value = format!(r#"a" {words} "b"#);
        for sql in ["UPDATE dq SET m = $1 WHERE id = 1", "INSERT INTO dq VALUES (3, $1)"] {
            let e = sqlx::query(sql).bind(&value).execute(&pool).await.expect_err(sql);
            let got = code(&e);
            assert!(!got.starts_with("40") && got != "57014", "{sql} with {value}: a bound value chose {got}: {e}");
        }
    }
    // A stored value, which the failing statement never spells.
    pool.execute(sqlx::raw_sql(
        r#"CREATE TABLE dqs (id BIGINT PRIMARY KEY, t TEXT);
           INSERT INTO dqs VALUES (1, 'a" could not serialize access "b'), (2, 'a" deadlock detected "b'), (3, 'a" canceling statement "b')"#,
    ))
    .await
    .unwrap();
    for id in 1..=3 {
        for sql in [
            format!("UPDATE dq SET m = s.t FROM dqs s WHERE dq.id = 1 AND s.id = {id}"),
            format!("WITH w AS (UPDATE dq SET m = s.t FROM dqs s WHERE dq.id = 1 AND s.id = {id} RETURNING dq.id) SELECT * FROM w"),
            format!("INSERT INTO dq SELECT 10 + id, t FROM dqs WHERE id = {id}"),
        ] {
            let e = sqlx::query(AssertSqlSafe(sql.clone())).fetch_all(&pool).await.expect_err(&sql);
            let got = code(&e);
            assert!(!got.starts_with("40") && got != "57014", "{sql}: a stored value chose {got}: {e}");
        }
    }
    let m: i64 = sqlx::query_scalar("SELECT m FROM dq WHERE id = 1").fetch_one(&pool).await.unwrap();
    assert_eq!(m, 1);
}

/// A quoted identifier holding a quote, echoed unescaped by a message
/// ("column 'x' deadlock detected 'y' does not exist"): its quotes paired
/// with the message's. The statement's own quote-holding texts are blanked
/// before the code is chosen.
#[tokio::test(flavor = "multi_thread")]
async fn a_quote_inside_an_identifier_does_not_choose_the_sqlstate() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql("CREATE TABLE di (id BIGINT PRIMARY KEY, m BIGINT); INSERT INTO di VALUES (1, 1);"))
        .await
        .unwrap();
    for words in ["could not serialize access", "deadlock detected", "canceling statement", "is locked by transaction"] {
        for sql in [
            format!(r#"UPDATE di SET "x' {words} 'y" = 1"#),
            format!(r#"SELECT "x' {words} 'y" FROM di"#),
            format!(r#"WITH w AS (SELECT "x' {words} 'y" FROM di) SELECT * FROM w"#),
            format!(r#"WITH w AS (UPDATE di SET "x' {words} 'y" = 1 RETURNING id) SELECT * FROM w"#),
            format!(r#"SELECT * FROM di ORDER BY "x' {words} 'y""#),
        ] {
            let e = sqlx::query(AssertSqlSafe(sql.clone())).fetch_all(&pool).await.expect_err(&sql);
            let got = code(&e);
            assert!(
                !got.starts_with("40") && !got.starts_with("57") && got != "55P03",
                "{sql}: a quoted identifier chose {got}: {e}"
            );
        }
    }
}
