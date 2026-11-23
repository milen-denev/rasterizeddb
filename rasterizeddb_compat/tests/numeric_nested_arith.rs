//! An integer sub-expression combined with `NUMERIC` in a `WHERE` predicate.
//!
//! `v + 1 * 2` parses as `v + (1 * 2)`: the product is an integer arithmetic
//! result, and the addition runs at the engine decimal scale. The planner had a
//! conversion for an integer *literal* and an integer *column* meeting a
//! NUMERIC, but none for an integer *intermediate*, so the statement failed
//! with "a I32 operand cannot be combined with a NUMERIC one here". Run at
//! every declared scale, because scale bugs here hide at the default.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

const DECLARATIONS: &[(&str, &str)] = &[
    ("plain", "NUMERIC"),
    ("scale0", "NUMERIC(12,0)"),
    ("scale2", "NUMERIC(10,2)"),
    ("scale4", "NUMERIC(12,4)"),
    ("scale9", "NUMERIC(20,9)"),
];

#[tokio::test(flavor = "multi_thread")]
async fn an_integer_subexpression_meets_numeric_at_every_scale() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for (table, declared) in DECLARATIONS {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "CREATE TABLE {table} (id INTEGER NOT NULL, v {declared} NOT NULL)"
        ))))
        .await
        .unwrap();
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "INSERT INTO {table} (id, v) VALUES (1, 100), (2, 250)"
        ))))
        .await
        .unwrap();
    }
    let mut wrong = Vec::new();
    for (predicate, expected) in [
        ("v + 1 * 2 = 102", vec![1]),
        ("v + 1 * 2 > 102", vec![2]),
        ("v - 2 * 3 = 94", vec![1]),
        ("v * 2 + 1 * 3 = 203", vec![1]),
        ("v + id * 2 = 102", vec![1]),
        ("v + id * 2 = 254", vec![2]),
        ("v + (1 + 1) = 102", vec![1]),
        ("1 * 2 + v = 252", vec![2]),
        ("v / (2 * 5) = 10", vec![1]),
        // A NUMERIC literal inside the sub-expression.
        ("v + 0.5 + 0.5 = 101", vec![1]),
        // A float literal times an integer literal, inside NUMERIC arithmetic.
        ("v + 0.5 * 2 = 101", vec![1]),
        ("v + 1 * 2 >= 102 AND id = 2", vec![2]),
    ] {
        for (table, declared) in DECLARATIONS {
            let got = sqlx::query(AssertSqlSafe(format!(
                "SELECT id FROM {table} WHERE {predicate} ORDER BY id"
            )))
            .fetch_all(&pool)
            .await
            .map(|rows| rows.iter().map(|r| r.get::<i32, _>(0)).collect::<Vec<_>>());
            match got {
                Ok(ids) if ids == expected => {}
                Ok(ids) => wrong.push(format!("{declared:14} {predicate}: got {ids:?}")),
                Err(e) => wrong.push(format!("{declared:14} {predicate}: error {e}")),
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}
