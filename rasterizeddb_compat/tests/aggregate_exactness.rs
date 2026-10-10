//! Exact aggregates over the wire: a total that does not fit is an error
//! rather than a wrapped number, and `AVG` over a `NUMERIC(p, s)` column keeps
//! the fractional digits PostgreSQL keeps.
//!
//! Both bugs answered rather than failed. `SUM` over a `BIGINT` holding
//! `i64::MAX` and `1` returned `-9223372036854775808`, and `AVG(1.00, 2.00,
//! 2.00)` over a `NUMERIC(10,2)` column returned `1.66` — a value rounded the
//! wrong way, reported as the answer.

use rasterizeddb_compat::TestServer;
use rust_decimal::Decimal;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn run(pool: &sqlx::PgPool, sql: &str) {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .unwrap_or_else(|e| panic!("{}: {}", sql, e));
}

/// `sum(bigint)` is `numeric`, as in PostgreSQL: a total past `i64` is exact.
///
/// This test used to pin `bigint out of range` — the engine kept `sum(int8)`
/// as `bigint` and refused a total it could not hold. The result type is now
/// PostgreSQL's, so the same rows answer the exact total, decoded as a
/// `numeric`; only `sum(int4)` is still a `bigint`.
#[tokio::test(flavor = "multi_thread")]
async fn a_bigint_sum_past_i64_is_an_exact_numeric() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    run(&pool, "CREATE TABLE big (id INT NOT NULL, g INT NOT NULL, n BIGINT NOT NULL, s INT NOT NULL)").await;
    run(&pool, "INSERT INTO big (id, g, n, s) VALUES (1, 1, 9223372036854775807, 2147483647)").await;
    run(&pool, "INSERT INTO big (id, g, n, s) VALUES (2, 1, 1, 2147483647)").await;
    run(&pool, "INSERT INTO big (id, g, n, s) VALUES (3, 2, 5, 1)").await;

    let total: Decimal = "9223372036854775813".parse().unwrap();
    for (sql, want) in [
        ("SELECT SUM(n) FROM big", total),
        ("SELECT g, SUM(n) FROM big WHERE g = 1 GROUP BY g", "9223372036854775808".parse().unwrap()),
        ("SELECT SUM(n) + 0 FROM big", total),
    ] {
        for (protocol, got) in [
            ("binary", sqlx::query(AssertSqlSafe(sql)).fetch_one(&pool).await),
            ("text", sqlx::raw_sql(AssertSqlSafe(sql)).fetch_one(&pool).await),
        ] {
            let row = got.unwrap_or_else(|e| panic!("{protocol} / {sql}: {e}"));
            let v: Decimal = row.try_get(row.len() - 1).unwrap_or_else(|e| panic!("{protocol} / {sql}: {e}"));
            assert_eq!(v, want, "{protocol} / {sql}");
        }
    }

    // In range it is a numeric too; `sum(int4)` stays a bigint, past the
    // `int4` range, as PostgreSQL's is.
    let ok: Decimal = sqlx::query("SELECT SUM(n) FROM big WHERE g = 2")
        .fetch_one(&pool)
        .await
        .expect("in range")
        .get(0);
    assert_eq!(ok, Decimal::from(5));
    let wide: i64 = sqlx::query("SELECT SUM(s) FROM big WHERE g = 1")
        .fetch_one(&pool)
        .await
        .expect("sum(int4)")
        .get(0);
    assert_eq!(wide, 2 * 2147483647i64);
}

const DECLARATIONS: &[(&str, &str)] = &[
    ("plain", "NUMERIC"),
    ("scale2", "NUMERIC(10,2)"),
    ("scale4", "NUMERIC(12,4)"),
    ("scale9", "NUMERIC(20,9)"),
];

/// `AVG(1.00, 2.00, 2.00)` is 1.666…; at PostgreSQL's sixteen places, rounded
/// the way it rounds a numeric quotient, that is 1.6666666666666667 at every
/// declared scale up to sixteen — including in the wire value, which is
/// rendered by a scale chosen separately from the one the fold divides at.
/// (This read 1.666666667, the engine's nine places, before `avg` took
/// PostgreSQL's digits.)
#[tokio::test(flavor = "multi_thread")]
async fn avg_over_a_scaled_numeric_keeps_its_fraction() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for (table, declared) in DECLARATIONS {
        run(&pool, &format!("CREATE TABLE {} (id INT NOT NULL, g INT NOT NULL, v {} NOT NULL)", table, declared)).await;
        for (id, v) in [(1, "1.00"), (2, "2.00"), (3, "2.00")] {
            run(&pool, &format!("INSERT INTO {} (id, g, v) VALUES ({}, 1, {})", table, id, v)).await;
        }
    }

    let expected: Decimal = "1.6666666666666667".parse().unwrap();
    let mut wrong = Vec::new();
    for (table, declared) in DECLARATIONS {
        for (sql, want) in [
            (format!("SELECT AVG(v) FROM {}", table), expected),
            (format!("SELECT g, AVG(v) FROM {} GROUP BY g", table), expected),
            // Computed over the finished average: an expression evaluates at
            // the engine's nine places, so `+ 1` reads the average cut there.
            (format!("SELECT AVG(v) + 1 FROM {}", table), "2.666666666".parse().unwrap()),
            (format!("SELECT SUM(v) FROM {}", table), "5".parse().unwrap()),
        ] {
            for (protocol, row) in [
                ("binary", sqlx::query(AssertSqlSafe(sql.clone())).fetch_one(&pool).await),
                ("text", sqlx::raw_sql(AssertSqlSafe(sql.clone())).fetch_one(&pool).await),
            ] {
                match row.map(|r| r.try_get::<Decimal, _>(r.len() - 1)) {
                    Ok(Ok(v)) if v == want => {}
                    Ok(Ok(v)) => wrong.push(format!("{declared:14} {protocol:6} {sql}: got {v}")),
                    Ok(Err(e)) => wrong.push(format!("{declared:14} {protocol:6} {sql}: decode {e}")),
                    Err(e) => wrong.push(format!("{declared:14} {protocol:6} {sql}: error {e}")),
                }
            }
        }
        // HAVING meets each aggregate at its own scale.
        for (having, keep) in [
            ("SUM(v) > 4.99", true),
            ("SUM(v) > 5", false),
            ("AVG(v) > 1.6666", true),
            ("AVG(v) > 1.67", false),
        ] {
            let sql = format!("SELECT g FROM {} GROUP BY g HAVING {}", table, having);
            match sqlx::query(AssertSqlSafe(sql.clone())).fetch_all(&pool).await {
                Ok(rows) if (rows.len() == 1) == keep => {}
                Ok(rows) => wrong.push(format!("{declared:14} {sql}: {} groups", rows.len())),
                Err(e) => wrong.push(format!("{declared:14} {sql}: error {e}")),
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}
