//! `ILIKE` folds case the way `lower()` does, for every letter, not only ASCII.
//!
//! PostgreSQL defines `ILIKE` on a multi-byte encoding as `lower(value) LIKE
//! lower(pattern)`. The engine folded ASCII only, so `'é' ILIKE 'É'` was false
//! while `lower('É') = 'é'` was true: a search box that silently missed every
//! row whose accented letters were in the other case.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::query("CREATE TABLE words (id INTEGER NOT NULL, w TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("create");
    for (id, w) in [
        (1, "abc"),
        (2, "É"),
        (3, "Crème Brûlée"),
        (4, "ПРИВЕТ"),
        (5, "\u{212A}elvin"),
        (6, "e"),
    ] {
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
async fn ilike_folds_non_ascii_letters() {
    let (_s, pool) = seeded().await;
    let cases: &[(&str, &[i32])] = &[
        ("w ILIKE 'é'", &[2]),
        ("w ILIKE 'É'", &[2]),
        ("w ILIKE '%BRÛLÉE'", &[3]),
        ("w ILIKE 'crème%'", &[3]),
        ("w ILIKE '%È%'", &[3]),
        ("w ILIKE 'cr_me%'", &[3]),
        ("w ILIKE 'привет'", &[4]),
        ("w ILIKE '%ивЕ%'", &[4]),
        ("w ILIKE 'kelvin'", &[5]),
        ("w ILIKE 'e'", &[6]),
        ("w ILIKE 'ABC'", &[1]),
    ];
    let mut failures = Vec::new();
    for (predicate, want) in cases {
        let got = ids(&pool, predicate).await;
        if got != *want {
            failures.push(format!("{}: got {:?}, want {:?}", predicate, got, want));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// `lower()` cannot appear in a `WHERE` clause here, so the server's `lower()`
/// of each row is read back as a column.
#[tokio::test(flavor = "multi_thread")]
async fn ilike_agrees_with_lower() {
    let (_s, pool) = seeded().await;
    let lowered: Vec<(i32, String)> = sqlx::query("SELECT id, lower(w) FROM words ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("lower")
        .iter()
        .map(|r| (r.get::<i32, _>(0), r.get::<String, _>(1)))
        .collect();
    for needle in ["é", "BRÛLÉE", "привет", "kelvin", "abc", "E"] {
        let via_ilike = ids(&pool, &format!("w ILIKE '%{}%'", needle)).await;
        // `SELECT lower($1)` without a table is not supported; the server's
        // `lower()` is `str::to_lowercase`, so the needle is lowered with it.
        let n = needle.to_lowercase();
        let via_lower: Vec<i32> =
            lowered.iter().filter(|(_, w)| w.contains(n.as_str())).map(|(id, _)| *id).collect();
        assert_eq!(via_ilike, via_lower, "needle {}", needle);
    }
}
