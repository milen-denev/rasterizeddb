//! `''` is a value, not NULL, to a UNIQUE constraint.
//!
//! The uniqueness check skipped any value whose bytes were empty, treating
//! emptiness as NULL — so a UNIQUE text column accepted `''` any number of
//! times. A NULL is told by its `__null_` companion, never by its bytes; and a
//! stored NULL text has empty bytes too, so it must not be mistaken for `''`
//! in the other direction either.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn exec(pool: &sqlx::PgPool, sql: &str) -> Result<u64, sqlx::Error> {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .map(|r| r.rows_affected())
}

async fn count(pool: &sqlx::PgPool, sql: &str) -> i64 {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<i64, _>(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_string_is_a_value_to_unique() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for sql in [
        "CREATE TABLE ue (id INT PRIMARY KEY, code TEXT UNIQUE NOT NULL)",
        "CREATE TABLE un (id INT PRIMARY KEY, code TEXT UNIQUE)",
        "CREATE TABLE uc (id INT PRIMARY KEY, a TEXT NOT NULL, b TEXT NOT NULL, UNIQUE (a, b))",
        "CREATE TABLE um (id INT PRIMARY KEY, code TEXT UNIQUE NOT NULL)",
    ] {
        exec(&pool, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    // NOT NULL column: the second '' violates.
    exec(&pool, "INSERT INTO ue (id, code) VALUES (1, '')").await.unwrap();
    assert!(exec(&pool, "INSERT INTO ue (id, code) VALUES (2, '')").await.is_err(), "second '' accepted");
    // ON CONFLICT sees the conflict and skips the row.
    assert_eq!(
        exec(&pool, "INSERT INTO ue (id, code) VALUES (3, '') ON CONFLICT (code) DO NOTHING")
            .await
            .unwrap(),
        0
    );
    // An UPDATE onto '' conflicts too.
    exec(&pool, "INSERT INTO ue (id, code) VALUES (4, 'x')").await.unwrap();
    assert!(exec(&pool, "UPDATE ue SET code = '' WHERE id = 4").await.is_err(), "UPDATE onto '' accepted");
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM ue WHERE code = ''").await, 1);

    // Nullable column: NULLs never conflict, with each other or with ''.
    exec(&pool, "INSERT INTO un (id, code) VALUES (1, NULL)").await.unwrap();
    exec(&pool, "INSERT INTO un (id, code) VALUES (2, NULL)").await.unwrap();
    exec(&pool, "INSERT INTO un (id, code) VALUES (3, '')").await.unwrap();
    assert!(exec(&pool, "INSERT INTO un (id, code) VALUES (4, '')").await.is_err(), "second '' accepted");
    exec(&pool, "INSERT INTO un (id, code) VALUES (5, NULL)").await.unwrap();
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM un WHERE code IS NULL").await, 3);
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM un WHERE code = ''").await, 1);

    // A composite key of empty strings.
    exec(&pool, "INSERT INTO uc (id, a, b) VALUES (1, '', '')").await.unwrap();
    assert!(exec(&pool, "INSERT INTO uc (id, a, b) VALUES (2, '', '')").await.is_err(), "('', '') twice accepted");
    exec(&pool, "INSERT INTO uc (id, a, b) VALUES (3, '', 'b')").await.unwrap();

    // A multi-row INSERT carrying the duplicate inside itself.
    let multi = exec(&pool, "INSERT INTO um (id, code) VALUES (1, ''), (2, '')").await;
    assert!(multi.is_err(), "multi-row duplicate '' accepted");
    assert!(count(&pool, "SELECT COUNT(*) FROM um WHERE code = ''").await <= 1);
}
