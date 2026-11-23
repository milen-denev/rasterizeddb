//! `LIKE` / `ILIKE` escapes, as PostgreSQL reads them.
//!
//! A backslash escapes the next character by default, so `\_`, `\%` and `\\`
//! are literals; `ESCAPE 'c'` picks another escape character and `ESCAPE ''`
//! turns escaping off. Django escapes every `_` and `%` of a searched value this
//! way — `name__contains='a_b'` sends `LIKE '%a\_b%'` — and the engine used to
//! read the backslash as a literal, so such a search matched nothing.
//!
//! Each case runs over the simple query protocol and, with the pattern bound as
//! a parameter, over the extended one.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

const WORDS: [(i32, &str); 8] = [
    (1, "a_b"),
    (2, "axb"),
    (3, "50%"),
    (4, "500"),
    (5, "a\\b"),
    (6, "ab"),
    (7, "A_B"),
    (8, "it's 50% off"),
];

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::query("CREATE TABLE words (id INTEGER NOT NULL, w TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("create");
    for (id, w) in WORDS {
        sqlx::query("INSERT INTO words (id, w) VALUES ($1, $2)")
            .bind(id)
            .bind(w)
            .execute(&pool)
            .await
            .expect("insert");
    }
    (server, pool)
}

/// Ids matching `w <op> '<pattern>' <tail>`, over the simple protocol.
async fn simple(pool: &sqlx::PgPool, op: &str, pattern: &str, tail: &str) -> Vec<i32> {
    let literal = pattern.replace('\'', "''");
    let sql = format!("SELECT id FROM words WHERE w {op} '{literal}' {tail} ORDER BY id");
    pool.fetch_all(sqlx::raw_sql(AssertSqlSafe(sql.clone())))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.get::<i32, _>(0))
        .collect()
}

/// The same, with the pattern bound as `$1`.
async fn extended(pool: &sqlx::PgPool, op: &str, pattern: &str, tail: &str) -> Vec<i32> {
    let sql = format!("SELECT id FROM words WHERE w {op} $1 {tail} ORDER BY id");
    sqlx::query(AssertSqlSafe(sql.clone()))
        .bind(pattern)
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql} with {pattern:?}: {e}"))
        .iter()
        .map(|r| r.get::<i32, _>(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn backslash_escapes_match_postgres() {
    let (_s, pool) = seeded().await;
    // (operator, pattern as the server receives it, ESCAPE clause, expected ids)
    let cases: &[(&str, &str, &str, &[i32])] = &[
        // Django's contains / startswith / endswith / exact.
        ("LIKE", "%a\\_b%", "", &[1]),
        ("LIKE", "a\\_%", "", &[1]),
        ("LIKE", "%\\_b", "", &[1]),
        ("LIKE", "a\\_b", "", &[1]),
        ("LIKE", "%50\\%%", "", &[3, 8]),
        ("LIKE", "50\\%", "", &[3]),
        ("LIKE", "%\\%", "", &[3]),
        ("LIKE", "a\\\\b", "", &[5]),
        ("LIKE", "%\\\\%", "", &[5]),
        // Unescaped wildcards still are wildcards.
        ("LIKE", "a_b", "", &[1, 2, 5]),
        ("LIKE", "50%", "", &[3, 4]),
        // Escapes next to an interior wildcard go to the pattern matcher.
        ("LIKE", "a\\_%b", "", &[1]),
        ("LIKE", "%s 50\\%_off", "", &[8]),
        ("LIKE", "_\\_%", "", &[1, 7]),
        // ILIKE folds escaped letters too.
        ("ILIKE", "%a\\_b%", "", &[1, 7]),
        ("ILIKE", "\\A\\_%", "", &[1, 7]),
        // ESCAPE picks another character, or none.
        ("LIKE", "a!_b", "ESCAPE '!'", &[1]),
        ("LIKE", "%!%%", "ESCAPE '!'", &[3, 8]),
        ("LIKE", "a\\b", "ESCAPE '!'", &[5]),
        ("LIKE", "a\\%", "ESCAPE ''", &[5]),
        ("ILIKE", "!a!_%", "ESCAPE '!'", &[1, 7]),
    ];
    for (op, pattern, tail, expect) in cases {
        assert_eq!(
            simple(&pool, op, pattern, tail).await,
            expect.to_vec(),
            "simple: w {op} {pattern:?} {tail}"
        );
        assert_eq!(
            extended(&pool, op, pattern, tail).await,
            expect.to_vec(),
            "extended: w {op} {pattern:?} {tail}"
        );
    }
}

/// `E''` strings resolve their own backslashes first, so the LIKE escape is
/// written doubled.
#[tokio::test(flavor = "multi_thread")]
async fn an_escape_string_pattern_resolves_its_backslashes_first() {
    let (_s, pool) = seeded().await;
    let ids = |sql: &'static str| {
        let pool = pool.clone();
        async move {
            pool.fetch_all(sqlx::raw_sql(sql))
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
                .iter()
                .map(|r| r.get::<i32, _>(0))
                .collect::<Vec<i32>>()
        }
    };
    assert_eq!(ids(r"SELECT id FROM words WHERE w LIKE E'%a\\_b%' ORDER BY id").await, vec![1]);
    // A quote inside an E'' string is written doubled here: the statement's
    // WHERE-clause extraction does not yet know `\'` (see the report).
    assert_eq!(ids(r"SELECT id FROM words WHERE w LIKE E'it''s%' ORDER BY id").await, vec![8]);
}

/// A pattern ending in its escape character is PostgreSQL's 22025; an ESCAPE
/// of more than one character is 22019.
#[tokio::test(flavor = "multi_thread")]
async fn a_dangling_escape_is_refused() {
    let (_s, pool) = seeded().await;
    let code = |sql: &'static str| {
        let pool = pool.clone();
        async move {
            match pool.fetch_all(sqlx::raw_sql(sql)).await {
                Ok(rows) => panic!("{sql} returned {} rows", rows.len()),
                Err(e) => e
                    .as_database_error()
                    .and_then(|d| d.code().map(|c| c.to_string()))
                    .unwrap_or_else(|| format!("not a database error: {e}")),
            }
        }
    };
    assert_eq!(code(r"SELECT id FROM words WHERE w LIKE 'ab\'").await, "22025");
    assert_eq!(code(r"SELECT id FROM words WHERE w LIKE 'ab!' ESCAPE '!'").await, "22025");
    assert_eq!(code(r"SELECT id FROM words WHERE w LIKE 'ab' ESCAPE 'xy'").await, "22019");
}
