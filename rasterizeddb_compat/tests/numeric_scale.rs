//! Predicates over `NUMERIC(p, s)` columns.
//!
//! A `DECIMAL` is stored as an `i128` of units scaled by the column's own `s`,
//! while a literal used to be encoded at the engine's default scale of 9. Every
//! comparison over a column declared with any other scale was therefore off by
//! a power of ten — and *answered* rather than failing: `WHERE price > 50` over
//! a `NUMERIC(10,2)` column holding 100 and 250 returned no rows at all.
//!
//! The same query is run against four columns whose only difference is the
//! declared scale, and all four must agree. Testing one scale would have passed
//! throughout: the bug is invisible at `NUMERIC` and at `NUMERIC(p, 9)`, which
//! are exactly the shapes a hand-written test reaches for.

use rasterizeddb_compat::TestServer;
use rust_decimal::Decimal;
use sqlx::{AssertSqlSafe, Row};

/// The declared types under test, all holding the same two values.
const DECLARATIONS: &[(&str, &str)] = &[
    ("plain", "NUMERIC"),
    ("scale0", "NUMERIC(12,0)"),
    ("scale2", "NUMERIC(10,2)"),
    ("scale4", "NUMERIC(12,4)"),
    ("scale9", "NUMERIC(20,9)"),
];

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for (table, declared) in DECLARATIONS {
        sqlx::query(AssertSqlSafe(format!(
            "CREATE TABLE {} (id INTEGER NOT NULL, v {} NOT NULL)",
            table, declared
        )))
        .execute(&pool)
        .await
        .unwrap_or_else(|e| panic!("CREATE {} failed: {}", declared, e));
        for (id, value) in [(1, "100"), (2, "250")] {
            sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO {} (id, v) VALUES ({}, {})",
                table, id, value
            )))
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("INSERT into {} failed: {}", table, e));
        }
    }
    (server, pool)
}

async fn ids(pool: &sqlx::PgPool, table: &str, predicate: &str) -> Vec<i32> {
    sqlx::query(AssertSqlSafe(format!(
        "SELECT id FROM {} WHERE {} ORDER BY id",
        table, predicate
    )))
    .fetch_all(pool)
    .await
    .unwrap_or_else(|e| panic!("{} / {} failed: {}", table, predicate, e))
    .iter()
    .map(|r| r.get::<i32, _>(0))
    .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_declared_scale_answers_the_same_predicate_alike() {
    let (_s, pool) = seeded().await;

    for (predicate, expected) in [
        ("v > 50", vec![1, 2]),
        ("v >= 100", vec![1, 2]),
        ("v = 100", vec![1]),
        ("v > 100", vec![2]),
        ("v < 200", vec![1]),
        ("v < 1000", vec![1, 2]),
        ("v <> 100", vec![2]),
        ("v BETWEEN 90 AND 150", vec![1]),
    ] {
        for (table, declared) in DECLARATIONS {
            assert_eq!(
                ids(&pool, table, predicate).await,
                expected,
                "{} over {}",
                predicate,
                declared
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fractional_literal_compares_at_the_column_scale() {
    let (_s, pool) = seeded().await;
    // 100 is not above 100.5, and is above 99.5 — at every declared scale that
    // can represent the distinction. `NUMERIC(12,0)` rounds the literal, as
    // PostgreSQL does, so it is left out of the fractional cases.
    for (table, declared) in DECLARATIONS.iter().filter(|(t, _)| *t != "scale0") {
        assert_eq!(ids(&pool, table, "v > 100.5").await, vec![2], "{}", declared);
        assert_eq!(ids(&pool, table, "v > 99.5").await, vec![1, 2], "{}", declared);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_value_read_back_is_the_value_written() {
    let (_s, pool) = seeded().await;
    for (table, declared) in DECLARATIONS {
        let value: Decimal = sqlx::query(AssertSqlSafe(format!(
            "SELECT v FROM {} WHERE id = 1",
            table
        )))
        .fetch_one(&pool)
        .await
        .expect("select")
        .get(0);
        assert_eq!(value, Decimal::from(100), "{}", declared);
    }
}

/// The seeded production schema uses `NUMERIC(12,4)` throughout, so this is the
/// shape the bug was found in.
#[tokio::test(flavor = "multi_thread")]
async fn the_wide_schema_filters_on_its_decimal_columns() {
    use rasterizeddb_compat::wide_types;
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    wide_types::apply(&pool).await.expect("schema should apply");

    // Subtotals are 100, 250 and 80.
    let mut over = sqlx::query(AssertSqlSafe(
        "SELECT order_number FROM test_ecommerce_orders WHERE subtotal > 90 ORDER BY order_number",
    ))
    .fetch_all(&pool)
    .await
    .expect("select")
    .iter()
    .map(|r| r.get::<Option<String>, _>(0).unwrap_or_default())
    .collect::<Vec<_>>();
    over.sort();
    assert_eq!(over, vec!["ORD-0001", "ORD-0002"]);
}
