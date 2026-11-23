//! `LIKE` / `ILIKE` answers that PostgreSQL gives and this engine used not to.
//!
//! Both were silent wrong answers rather than errors: `LIKE '%%'` returned no
//! rows (it is rewritten to `CONTAINS ''`, and an empty needle was "not found"),
//! and `_` matched one *byte*, so it never matched a multi-byte character.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::query("CREATE TABLE words (id INTEGER NOT NULL, w TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("create");
    for (id, w) in [(1, "abc"), (2, "é"), (3, "crème"), (4, "日本語"), (5, "")] {
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO words (id, w) VALUES ({}, '{}')",
            id, w
        )))
        .execute(&pool)
        .await
        .expect("insert");
    }
    (server, pool)
}

async fn ids(pool: &sqlx::PgPool, predicate: &str) -> Vec<i32> {
    sqlx::query(AssertSqlSafe(format!(
        "SELECT id FROM words WHERE {} ORDER BY id",
        predicate
    )))
    .fetch_all(pool)
    .await
    .unwrap_or_else(|e| panic!("{} failed: {}", predicate, e))
    .iter()
    .map(|r| r.get::<i32, _>(0))
    .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn only_wildcards_match_every_row() {
    let (_s, pool) = seeded().await;
    let all = vec![1, 2, 3, 4, 5];
    for predicate in ["w LIKE '%'", "w LIKE '%%'", "w ILIKE '%%'", "w LIKE '%%%'"] {
        assert_eq!(ids(&pool, predicate).await, all, "{}", predicate);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn underscore_matches_one_character_not_one_byte() {
    let (_s, pool) = seeded().await;
    assert_eq!(ids(&pool, "w LIKE '_'").await, vec![2], "é is one character");
    assert_eq!(ids(&pool, "w LIKE '__'").await, Vec::<i32>::new());
    assert_eq!(ids(&pool, "w LIKE 'cr_me'").await, vec![3]);
    assert_eq!(ids(&pool, "w LIKE '_本_'").await, vec![4]);
    assert_eq!(ids(&pool, "w LIKE '___'").await, vec![1, 4]);
    assert_eq!(ids(&pool, "w ILIKE 'CR_ME'").await, vec![3]);
}
