//! `SIMILAR TO` answers PostgreSQL gives, and refusals it reports as errors.
//!
//! A pattern the matcher cannot use fails the statement with `2201B invalid
//! regular expression`, instead of silently matching no row — or, for deep
//! nesting, overflowing a worker's stack. A value too costly to match fails it
//! with `54001`. And the two shapes the old backtracking matcher answered
//! `false` for — an empty-matching star, a long match — answer correctly.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::query("CREATE TABLE codes (id INTEGER NOT NULL, c TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("create");
    for (id, c) in [(1, "1010"), (2, "0101"), (3, "aaa")] {
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO codes (id, c) VALUES ({id}, '{c}')"
        )))
        .execute(&pool)
        .await
        .expect("insert");
    }
    (server, pool)
}

async fn code_of(pool: &sqlx::PgPool, pattern: &str) -> Option<String> {
    match sqlx::query(AssertSqlSafe(format!(
        "SELECT id FROM codes WHERE c SIMILAR TO '{pattern}'"
    )))
    .fetch_all(pool)
    .await
    {
        Ok(_) => None,
        Err(e) => Some(
            e.as_database_error()
                .and_then(|d| d.code().map(|c| c.to_string()))
                .unwrap_or_else(|| format!("not a database error: {e}")),
        ),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pattern_the_matcher_refuses_fails_with_2201b() {
    let (_s, pool) = seeded().await;

    let rows: Vec<i32> = sqlx::query("SELECT id FROM codes WHERE c SIMILAR TO '101%' ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("a valid pattern still works")
        .iter()
        .map(|r| r.get::<i32, _>(0))
        .collect();
    assert_eq!(rows, vec![1]);

    let deep = format!("{}a{}", "(".repeat(10_000), ")".repeat(10_000));
    assert_eq!(code_of(&pool, &deep).await.as_deref(), Some("2201B"), "deep nesting");
    assert_eq!(code_of(&pool, "(abc").await.as_deref(), Some("2201B"), "unbalanced");

    // The server is still up and answering.
    assert_eq!(code_of(&pool, "(a)*").await, None);
}

async fn ids(pool: &sqlx::PgPool, pattern: &str) -> Vec<i32> {
    sqlx::query(AssertSqlSafe(format!(
        "SELECT id FROM codes WHERE c SIMILAR TO '{pattern}' ORDER BY id"
    )))
    .fetch_all(pool)
    .await
    .unwrap_or_else(|e| panic!("SIMILAR TO '{pattern}' failed: {e}"))
    .iter()
    .map(|r| r.get::<i32, _>(0))
    .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_matching_star_matches() {
    let (_s, pool) = seeded().await;
    // Row 3 is 'aaa'.
    for pattern in ["(a*)*", "(a*)+", "(|a)*", "(a?){2,}", "((a*)*)*", "(a?){2,3}"] {
        assert_eq!(ids(&pool, pattern).await, vec![3], "{pattern}");
    }
    assert_eq!(ids(&pool, "(a?){2}").await, Vec::<i32>::new());
    assert_eq!(ids(&pool, "(1|0)*").await, vec![1, 2]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_long_match_is_found_and_an_expensive_one_is_refused() {
    let (_s, pool) = seeded().await;
    let long = "ab".repeat(20_000);
    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO codes (id, c) VALUES (4, '{long}')"
    )))
    .execute(&pool)
    .await
    .expect("insert a long value");
    // Answered by the automaton, one state per character of the literal.
    assert_eq!(ids(&pool, &format!("{}_", &long[..long.len() - 1])).await, vec![4]);
    assert_eq!(ids(&pool, &long).await, vec![4]);
    assert_eq!(ids(&pool, "(ab)+").await, vec![4]);

    // Thousands of live states for every one of 40,000 characters: refused,
    // not answered `false`.
    // (The trailing `[c]` keeps the last `%` from accepting everything early,
    // and, being a class rather than a literal, from the suffix check.)
    let costly = format!("{}[c]", "(a|%)".repeat(3_000));
    assert_eq!(code_of(&pool, &costly).await.as_deref(), Some("54001"));
}
