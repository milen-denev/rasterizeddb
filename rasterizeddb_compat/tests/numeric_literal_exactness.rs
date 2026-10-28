//! A numeric constant is the number written, or the statement is refused.
//!
//! - A constant the engine scale (nine places, i128) cannot hold was carried
//!   as a float, and arithmetic on it was a float's: on a NUMERIC(38,2),
//!   `SET h = 123456789012345678901234567890123456.50 + 0` stored
//!   123456789012345680000000000000000000.00, and `1e30 + 1` was 1e30.
//! - `CAST('1e400' AS DOUBLE PRECISION)` was Infinity, where PostgreSQL
//!   raises 22003, and so was the literal `'1e400'` stored in a float column.
//! - `WHERE b = 2147483647 + 1` on a `bigint` compared with 2147483648, where
//!   PostgreSQL raises `integer out of range` for the two `int4` literals.
//! - Text computed into a number column was stored as its bytes:
//!   `SET b = '5' || ''` stored 53, the character's code.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

fn code(e: &sqlx::Error) -> String {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| e.to_string())
}

async fn refused(pool: &sqlx::PgPool, sql: &str, want: &str) {
    let e = sqlx::query(AssertSqlSafe(sql)).fetch_all(pool).await.expect_err(sql);
    assert_eq!(code(&e), want, "{sql}: {e}");
}

async fn texts(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    sqlx::query(AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.get::<Option<String>, _>(0).unwrap_or_else(|| "NULL".into()))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn numeric_constants_are_exact_or_refused() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE h (id BIGINT PRIMARY KEY, h NUMERIC(38,2), w NUMERIC(38,0), b BIGINT, i INT, f DOUBLE PRECISION);
         INSERT INTO h VALUES (1, 1.00, 0, 2147483648, 5, 1.0);",
    ))
    .await
    .unwrap();
    let row = || texts(&pool, "SELECT h::text || ' ' || w::text || ' ' || b::text || ' ' || i::text FROM h WHERE id = 1");

    // Wider than the engine scale: refused, never rounded through a float.
    for sql in [
        "UPDATE h SET h = 123456789012345678901234567890123456.50 + 0 WHERE id = 1",
        "UPDATE h SET h = 123456789012345678901234567890123456.50 - 1 WHERE id = 1",
        "UPDATE h SET w = 1e30 + 1 WHERE id = 1",
        "UPDATE h SET w = i + 1e30 WHERE id = 1",
        "INSERT INTO h VALUES (2, 123456789012345678901234567890123456.50 + 0, 0, 0, 0, 0)",
        "SELECT id FROM h WHERE h = 123456789012345678901234567890123456.50 + 0",
    ] {
        refused(&pool, sql, "22003").await;
    }
    assert_eq!(row().await, ["1.00 0 2147483648 5"]);

    // The same value written as itself still stores exactly, and arithmetic
    // whose answer is exact still runs.
    sqlx::query("UPDATE h SET h = 123456789012345678901234567890123456.50, w = i * 1e30 WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(row().await, ["123456789012345678901234567890123456.50 5000000000000000000000000000000 2147483648 5"]);
    // Two constants are computed exactly, whichever form carries them.
    sqlx::query("UPDATE h SET f = 0.0000000001 * 3, w = 1e30 - 1e29 WHERE id = 1").execute(&pool).await.unwrap();
    let r = sqlx::query("SELECT f, w::text AS w FROM h WHERE id = 1").fetch_one(&pool).await.unwrap();
    assert_eq!(r.get::<f64, _>("f"), 3e-10);
    assert_eq!(r.get::<String, _>("w"), "900000000000000000000000000000");
    // Ten places beside a NUMERIC constant: rounded to the engine's nine
    // first, this was 1.1.
    sqlx::query("UPDATE h SET f = 1.0000000001 + 0.1 WHERE id = 1").execute(&pool).await.unwrap();
    let f: f64 = sqlx::query_scalar("SELECT f FROM h WHERE id = 1").fetch_one(&pool).await.unwrap();
    assert_eq!(f, 1.1000000001);
    sqlx::query("UPDATE h SET f = 0.0000000001 * 3 WHERE id = 1").execute(&pool).await.unwrap();

    // A float's range.
    for sql in [
        "SELECT CAST('1e400' AS DOUBLE PRECISION) FROM h",
        "SELECT '-1e400'::float8 FROM h",
        "SELECT CAST('1e-400' AS DOUBLE PRECISION) FROM h",
        "SELECT '1e40'::real FROM h",
        "UPDATE h SET f = '1e400' WHERE id = 1",
        "UPDATE h SET f = CAST('1e400' AS DOUBLE PRECISION) WHERE id = 1",
        "INSERT INTO h VALUES (3, 1, 1, 1, 1, '1e400')",
    ] {
        refused(&pool, sql, "22003").await;
    }
    assert_eq!(
        texts(&pool, "SELECT CAST('-Infinity' AS DOUBLE PRECISION)::text || ' ' || CAST('4.9e-324' AS float8)::text FROM h").await.len(),
        1
    );
    let f: f64 = sqlx::query_scalar("SELECT f FROM h WHERE id = 1").fetch_one(&pool).await.unwrap();
    assert_eq!(f, 3e-10, "a refused float write changed the row");

    // `int4` literal arithmetic overflows as PostgreSQL's does, whatever
    // column it meets; `int8` literals have their own range.
    for sql in [
        "SELECT id FROM h WHERE b = 2147483647 + 1",
        "SELECT id FROM h WHERE b > 0 AND (b = 2147483647 + 1)",
        "DELETE FROM h WHERE b = 2147483647 + 1",
        "UPDATE h SET i = 0 WHERE b = 2147483647 + 1",
        "SELECT * FROM (SELECT id FROM h WHERE b = 2147483647 + 1) d",
        "SELECT id FROM h WHERE b = 9223372036854775807 + 1",
        "SELECT id FROM h WHERE b = 2147483647 * 1 + 1 - 1",
        "SELECT id FROM h WHERE b = 2147483646 + 2",
    ] {
        refused(&pool, sql, "22003").await;
    }
    // An `int8` literal widens the arithmetic, as there.
    let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM h WHERE b = 2147483648 + 0").fetch_all(&pool).await.unwrap();
    assert_eq!(ids, [1]);
    assert_eq!(row().await.len(), 1, "a refused DELETE removed the row");

    // Text computed for a number column is read as a number.
    sqlx::query("UPDATE h SET b = '5' || '', h = '7.25' || '' WHERE id = 1").execute(&pool).await.unwrap();
    let r = sqlx::query("SELECT b, h::text AS h FROM h WHERE id = 1").fetch_one(&pool).await.unwrap();
    assert_eq!((r.get::<i64, _>("b"), r.get::<String, _>("h")), (5, "7.25".to_string()));
    refused(&pool, "UPDATE h SET b = 'x' || '' WHERE id = 1", "22P02").await;
    let b: i64 = sqlx::query_scalar("SELECT b FROM h WHERE id = 1").fetch_one(&pool).await.unwrap();
    assert_eq!(b, 5);
}
