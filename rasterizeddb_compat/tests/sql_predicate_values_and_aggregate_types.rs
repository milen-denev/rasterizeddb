//! Three gaps found side by side with PostgreSQL 17, each an explicit error
//! or a too-lenient answer:
//!
//! - a predicate in parentheses as a value in a select list, under a cast or
//!   a concatenation: `('abc' LIKE '%b%')::text`, `'x' || (m LIKE '%o%')::text`
//!   (a column named after itself, or "function '' is not supported yet");
//! - a lone parenthesised name in a WHERE — `(n) > 5`, `(n) IS NULL`,
//!   `(t) = 'a'` — and an expression in an IN list, `id IN (n / 10, 2)`, in
//!   SELECT, UPDATE and DELETE (syntax and plan errors);
//! - `sum` and `avg` of text, and `sum`, `avg`, `min`, `max` of a boolean,
//!   which PostgreSQL does not have (42883), answered NULL or a value; and
//!   the json / jsonb refusal reaches the window and join forms.
//!
//! Every expectation is PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn start(setup: &str) -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    if !setup.is_empty() {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(setup.to_string()))).await.expect("setup");
    }
    (server, pool)
}

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

async fn code(pool: &sqlx::PgPool, sql: &str) -> String {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_all(pool).await.expect_err(sql);
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| format!("no code: {e}"))
}

fn v(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_parenthesised_predicate_is_a_boolean_value() {
    let (_server, pool) =
        start("CREATE TABLE t (id int PRIMARY KEY, m text); INSERT INTO t VALUES (1, 'foo'), (2, 'bar'), (3, NULL)")
            .await;
    for (sql, expected) in [
        ("SELECT ('abc' LIKE '%b%')::text", v(&["true"])),
        ("SELECT 'x' || (m LIKE '%o%')::text FROM t ORDER BY id", v(&["xtrue", "xfalse", "NULL"])),
        ("SELECT (m LIKE '%o%')::text FROM t ORDER BY id", v(&["true", "false", "NULL"])),
        ("SELECT (m ILIKE '%O%') FROM t ORDER BY id", v(&["t", "f", "NULL"])),
        ("SELECT (m NOT LIKE '%o%')::text FROM t ORDER BY id", v(&["false", "true", "NULL"])),
        ("SELECT (m SIMILAR TO 'f%')::text FROM t ORDER BY id", v(&["true", "false", "NULL"])),
        ("SELECT (m ~ 'o')::text, (m !~* 'O') FROM t ORDER BY id", v(&["true|f", "false|t", "NULL|NULL"])),
        ("SELECT (id BETWEEN 1 AND 2)::text FROM t ORDER BY id", v(&["true", "true", "false"])),
        ("SELECT (id IN (1, 3))::text FROM t ORDER BY id", v(&["true", "false", "true"])),
        ("SELECT (m IS NULL)::text FROM t ORDER BY id", v(&["false", "false", "true"])),
        ("SELECT 'v' || (id > 1)::text FROM t ORDER BY id", v(&["vfalse", "vtrue", "vtrue"])),
        ("SELECT (m = 'foo') AS eq FROM t ORDER BY id", v(&["t", "f", "NULL"])),
        // Arithmetic in parentheses stays arithmetic.
        ("SELECT (id - 1)::text FROM t ORDER BY id", v(&["0", "1", "2"])),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_parenthesised_name_and_an_in_list_expression_in_a_where() {
    let (_server, pool) = start(
        "CREATE TABLE g (id int PRIMARY KEY, n int, t text); \
         INSERT INTO g VALUES (1, 3, 'a'), (2, 10, 'b'), (3, NULL, 'a'), (4, 40, NULL), (5, 20, 'c')",
    )
    .await;
    for (sql, expected) in [
        ("SELECT id FROM g WHERE (n) > 5 ORDER BY id", v(&["2", "4", "5"])),
        ("SELECT id FROM g WHERE (n) IS NULL ORDER BY id", v(&["3"])),
        ("SELECT id FROM g WHERE (n) BETWEEN 5 AND 15 ORDER BY id", v(&["2"])),
        ("SELECT id FROM g WHERE (t) = 'a' ORDER BY id", v(&["1", "3"])),
        ("SELECT id FROM g WHERE (g.n) >= 20 AND (t) IS NOT NULL ORDER BY id", v(&["5"])),
        ("SELECT id FROM g WHERE id IN (n / 10, 2) ORDER BY id", v(&["2", "4"])),
        ("SELECT id FROM g WHERE id NOT IN (n / 10) ORDER BY id", v(&["1", "2", "5"])),
        ("SELECT id FROM g WHERE id NOT IN (n / 10, NULL) ORDER BY id", v(&[])),
        ("SELECT id FROM g WHERE NOT (id IN (n / 10, 5)) ORDER BY id", v(&["1", "2"])),
        ("SELECT id FROM g WHERE id IN (1, 2) ORDER BY id", v(&["1", "2"])),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
    pool.execute("UPDATE g SET t = 'u' WHERE (n) = 10").await.unwrap();
    assert_eq!(rows(&pool, "SELECT id, t FROM g ORDER BY id").await, v(&["1|a", "2|u", "3|a", "4|NULL", "5|c"]));
    pool.execute("DELETE FROM g WHERE (n) = 40").await.unwrap();
    pool.execute("DELETE FROM g WHERE id IN (n / 10, 99)").await.unwrap();
    assert_eq!(rows(&pool, "SELECT id FROM g ORDER BY id").await, v(&["1", "2", "3", "5"]));
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregates_postgresql_does_not_have_are_refused() {
    let (_server, pool) = start(
        "CREATE TABLE t (id int PRIMARY KEY, m text, b boolean, j jsonb, n int); \
         INSERT INTO t VALUES (1, 'foo', true, '{\"a\":1}', 5), (2, 'bar', false, '[1]', 7), (3, NULL, NULL, NULL, NULL); \
         CREATE TABLE u (id int PRIMARY KEY, tid int); INSERT INTO u VALUES (1, 1), (2, 2)",
    )
    .await;
    for sql in [
        "SELECT sum(m) FROM t",
        "SELECT avg(m) FROM t",
        "SELECT sum(b) FROM t",
        "SELECT avg(b) FROM t",
        "SELECT min(b) FROM t",
        "SELECT max(b) FROM t",
        "SELECT min(j) OVER () FROM t",
        "SELECT sum(m) OVER () FROM t",
        "SELECT max(b) OVER () FROM t",
        "SELECT sum(t.m) FROM t JOIN u ON u.tid = t.id",
        "SELECT min(t.j) FROM t JOIN u ON u.tid = t.id",
        "SELECT max(t.b) FROM t JOIN u ON u.tid = t.id",
    ] {
        assert_eq!(code(&pool, sql).await, "42883", "{sql}");
    }
    // What PostgreSQL does have still answers.
    assert_eq!(
        rows(&pool, "SELECT sum(t.n), min(t.m), count(t.b), bool_and(t.b) FROM t JOIN u ON u.tid = t.id").await,
        v(&["12|bar|2|f"])
    );
    assert_eq!(rows(&pool, "SELECT sum(n), min(m), max(m) FROM t").await, v(&["12|bar|foo"]));
}
