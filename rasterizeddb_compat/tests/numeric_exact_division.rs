//! A quotient that ends is exact, and one that lies on a rounding tie
//! rounds as PostgreSQL rounds it.
//!
//! The exact assignment path (6f81378) bounded every quotient's error by at
//! least one unit past the target, even when the division left no remainder,
//! so it could not settle a tie: `price / 2` on 19.99 (9.995) and on 0.05
//! (0.025), `1.0 / 8` (0.125) were refused where PostgreSQL stores 10.00,
//! 0.03 and 0.13. A remainder of zero is exact now; a nonzero one is carried
//! with a sticky digit past the cut, so it is never taken for a tie.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn prices(pool: &sqlx::PgPool) -> Vec<String> {
    sqlx::query("SELECT price::text, p::text FROM u ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("read")
        .iter()
        .map(|r| format!("{} {}", r.get::<String, _>(0), r.get::<String, _>(1)))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn exact_quotients_round_as_postgres_rounds_them() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE u (id INT PRIMARY KEY, price NUMERIC(10,2), qty INT, p NUMERIC(10,2) CHECK (p >= 0.01));
         INSERT INTO u VALUES (1, 19.99, 3, 1.00), (2, 0.05, 5, 2.00), (3, 10.00, 8, 3.00), (4, 7.00, 3, 4.00);",
    ))
    .await
    .expect("setup");
    for sql in [
        "UPDATE u SET price = price / 2 WHERE id IN (1, 2)",
        "UPDATE u SET price = 1.0 / 8 WHERE id = 3",
        "UPDATE u SET price = qty / 8.0 WHERE id = 4",
        "UPDATE u SET p = p / 200 WHERE id = 1",
        // Not exact: carried with its sticky digit, 2.00 / 3.
        "UPDATE u SET p = p / 3 WHERE id = 2",
    ] {
        pool.execute(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(prices(&pool).await, ["10.00 0.01", "0.03 0.67", "0.13 3.00", "0.38 4.00"]);

    // The same through a parameter and an INSERT's constant.
    sqlx::query("UPDATE u SET price = $1 / 2 WHERE id = 3")
        .bind(rust_decimal::Decimal::new(5, 2))
        .execute(&pool)
        .await
        .expect("prepared");
    pool.execute(AssertSqlSafe("INSERT INTO u VALUES (5, 1.0 / 8, 1, 0.5 / 2)".to_string())).await.expect("insert");
    let tail: Vec<String> = prices(&pool).await.into_iter().skip(2).collect();
    assert_eq!(tail, ["0.03 3.00", "0.38 4.00", "0.13 0.25"]);
}
