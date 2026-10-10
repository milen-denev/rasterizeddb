//! Expressions whose values have mixed numeric widths order as numbers.
//!
//! `COALESCE(bigint_col, int_col)` yields an 8-byte value on one row and a
//! 4-byte one on the next. The comparator read the second operand's bytes as
//! the first operand's type, so `compare(a, b)` and `compare(b, a)` disagreed
//! and the sort came out in an order that depended on where rows started.
//! PostgreSQL resolves the expression to `bigint` and sorts numerically.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute(
        "CREATE TABLE m (id INTEGER NOT NULL, big BIGINT NULL, small INTEGER NOT NULL, \
         f DOUBLE PRECISION NOT NULL, d NUMERIC(12,3) NOT NULL)",
    )
    .await
    .expect("create");
    // (id, big, small, f, d). `big` is NULL on half the rows, so COALESCE
    // alternates between the two widths.
    let rows: &[(i32, Option<i64>, i32, f64, &str)] = &[
        (1, None, 40, 2.5, "7.250"),
        (2, Some(5_000_000_000), 1, -1.0, "0.001"),
        (3, None, -3, 100.0, "-2.000"),
        (4, Some(-7), 9, 0.5, "3.000"),
        (5, None, 12, 12.0, "12.500"),
        (6, Some(30), 0, 30.25, "30.000"),
        (7, None, 2_000_000_000, 1e10, "999999.999"),
        (8, Some(-5_000_000_000), 8, -1e10, "-999999.999"),
    ];
    for (id, big, small, f, d) in rows {
        let big = big.map(|b| b.to_string()).unwrap_or_else(|| "NULL".to_string());
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "INSERT INTO m (id, big, small, f, d) VALUES ({}, {}, {}, {}, {})",
            id, big, small, f, d
        ))))
        .await
        .expect("insert");
    }
    (server, pool)
}

async fn ids(pool: &sqlx::PgPool, sql: &str) -> Vec<i32> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e))
        .iter()
        .map(|r| r.get::<i32, _>(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn order_by_mixed_width_expressions_is_numeric() {
    let (_s, pool) = seeded().await;
    // COALESCE(big, small): 40, 5e9, -3, -7, 12, 30, 2e9, -5e9.
    let cases: &[(&str, &[i32])] = &[
        ("SELECT id FROM m ORDER BY COALESCE(big, small), id", &[8, 4, 3, 5, 6, 1, 7, 2]),
        ("SELECT id FROM m ORDER BY COALESCE(big, small) DESC, id", &[2, 7, 1, 6, 5, 3, 4, 8]),
        // GREATEST(small, big) ignores NULL: 40, 5e9, -3, 9, 12, 30, 2e9, 8.
        ("SELECT id FROM m ORDER BY GREATEST(small, big), id", &[3, 8, 4, 5, 6, 1, 7, 2]),
        // LEAST(big, small): 40, 1, -3, -7, 12, 0, 2e9, -5e9.
        ("SELECT id FROM m ORDER BY LEAST(big, small), id", &[8, 4, 3, 6, 2, 5, 1, 7]),
        // CASE mixing an int and a float: small where it is above 5, else f:
        // 40, -1.0, 100.0, 9, 12, 30.25, 2e9, 8.
        (
            "SELECT id FROM m ORDER BY CASE WHEN small > 5 THEN small ELSE f END, id",
            &[2, 8, 4, 5, 6, 1, 3, 7],
        ),
        // COALESCE(big, d): an integer against NUMERIC(12,3):
        // 7.25, 5e9, -2, -7, 12.5, 30, 999999.999, -5e9.
        ("SELECT id FROM m ORDER BY COALESCE(big, d), id", &[8, 4, 3, 1, 5, 6, 7, 2]),
    ];
    let mut failures = Vec::new();
    for (sql, want) in cases {
        let got = ids(&pool, sql).await;
        if got != *want {
            failures.push(format!("{}\n  got  {:?}\n  want {:?}", sql, got, want));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}
