//! `HAVING` over an aggregate that is NULL — a group whose values are all
//! NULL. PostgreSQL compares NULL as unknown, which HAVING treats as false;
//! the fold used to compare it as `0`, so `HAVING SUM(v) = 0` kept the
//! all-NULL group alongside the one that really sums to zero.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn groups(pool: &sqlx::PgPool, sql: &'static str) -> Result<Vec<i32>, sqlx::Error> {
    let mut out: Vec<i32> = sqlx::query(sql)
        .fetch_all(pool)
        .await?
        .iter()
        .map(|row| row.get::<i32, _>(0))
        .collect();
    out.sort();
    Ok(out)
}

#[tokio::test]
async fn having_treats_a_null_aggregate_as_unknown() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for sql in [
        "CREATE TABLE h (id INT PRIMARY KEY, g INT NOT NULL, v INT)",
        // g = 1: all NULL. g = 2: sums to 0. g = 3: ordinary.
        "INSERT INTO h (id, g, v) VALUES (1, 1, NULL)",
        "INSERT INTO h (id, g, v) VALUES (2, 1, NULL)",
        "INSERT INTO h (id, g, v) VALUES (3, 2, 3)",
        "INSERT INTO h (id, g, v) VALUES (4, 2, -3)",
        "INSERT INTO h (id, g, v) VALUES (5, 3, 5)",
        "INSERT INTO h (id, g, v) VALUES (6, 3, 1)",
    ] {
        pool.execute(sqlx::raw_sql(sql))
            .await
            .unwrap_or_else(|e| panic!("{}: {}", sql, e));
    }

    assert_eq!(
        groups(&pool, "SELECT g FROM h GROUP BY g HAVING SUM(v) = 0").await.unwrap(),
        vec![2],
        "the all-NULL group's SUM is NULL, not 0"
    );
    assert_eq!(
        groups(&pool, "SELECT g FROM h GROUP BY g HAVING SUM(v) <> 0").await.unwrap(),
        vec![3],
        "NULL <> 0 is unknown"
    );
    assert_eq!(
        groups(&pool, "SELECT g FROM h GROUP BY g HAVING COUNT(v) = 0").await.unwrap(),
        vec![1],
        "COUNT is never NULL"
    );
    // `HAVING <aggregate> IS NULL` is outside the HAVING grammar here (one
    // aggregate compared against a constant). It must be refused, never
    // answered as if it said something else.
    match groups(&pool, "SELECT g FROM h GROUP BY g HAVING MAX(v) IS NULL").await {
        Ok(got) => assert_eq!(got, vec![1], "MAX over all-NULL values is NULL"),
        Err(e) => eprintln!("HAVING MAX(v) IS NULL is refused: {}", e),
    }
}
