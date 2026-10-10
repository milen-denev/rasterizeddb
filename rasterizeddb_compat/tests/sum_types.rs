//! `SUM` and `AVG` result types are PostgreSQL's.
//!
//! `sum(int2)`/`sum(int4)` are `bigint`; `sum(int8)` and `sum(numeric)` are
//! `numeric`; `sum(float4)` is `float4`; `avg` of an integer is `numeric`. The
//! described type and the payload must agree on both protocols — a payload of
//! a different width than its description is read as garbage or refused — so
//! every case decodes the value *by the described type*.
//!
//! This is a client-visible change: `sum(bigint)` used to be described as
//! `int8` (and raised `bigint out of range` past i64), and a client that
//! decoded it as `i64` must now decode a `numeric`.

use rasterizeddb_compat::TestServer;
use rust_decimal::Decimal;
use sqlx::{AssertSqlSafe, Column, Executor, Row, TypeInfo};

async fn run(pool: &sqlx::PgPool, sql: &str) {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .unwrap_or_else(|e| panic!("{}: {}", sql, e));
}

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    run(&pool, "CREATE TABLE s (id INT NOT NULL, g INT NOT NULL, n BIGINT NOT NULL, i INT NOT NULL, h SMALLINT NOT NULL, r REAL NOT NULL, d DOUBLE PRECISION NOT NULL)").await;
    for (id, g, n, i, h, r) in [
        (1, 1, "9223372036854775807", 1, 1, "1.5"),
        (2, 1, "1", 2, 2, "2.25"),
        (3, 2, "5", 2, 2, "0.25"),
    ] {
        run(
            &pool,
            &format!("INSERT INTO s (id, g, n, i, h, r, d) VALUES ({id}, {g}, {n}, {i}, {h}, {r}, {r})"),
        )
        .await;
    }
    (server, pool)
}

/// `(described type name, value as text)` of column 0 of every row, over the
/// binary and the text protocol.
async fn typed(pool: &sqlx::PgPool, sql: &str) -> Vec<(&'static str, String, Vec<String>)> {
    let mut out = Vec::new();
    for protocol in ["binary", "text"] {
        let rows = if protocol == "binary" {
            sqlx::query(AssertSqlSafe(sql.to_string())).fetch_all(pool).await
        } else {
            sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_all(pool).await
        }
        .unwrap_or_else(|e| panic!("{protocol} {sql}: {e}"));
        let ty = rows
            .first()
            .map(|r| r.column(0).type_info().name().to_string())
            .unwrap_or_default();
        let values = rows
            .iter()
            .map(|r| match ty.as_str() {
                "INT8" => r.get::<i64, _>(0).to_string(),
                "INT4" => r.get::<i32, _>(0).to_string(),
                // Compared as numbers: the binary encoding may drop trailing
                // zeros the text one keeps, and a computed numeric is at the
                // engine's nine places where PostgreSQL keeps the operands'.
                "NUMERIC" => r.get::<Decimal, _>(0).normalize().to_string(),
                "FLOAT4" => r.get::<f32, _>(0).to_string(),
                "FLOAT8" => r.get::<f64, _>(0).to_string(),
                other => panic!("{sql}: unexpected type {other}"),
            })
            .collect();
        out.push((protocol, ty, values));
    }
    out
}

fn check(got: Vec<(&'static str, String, Vec<String>)>, ty: &str, values: &[&str], sql: &str) {
    for (protocol, got_ty, got_values) in got {
        assert_eq!(got_ty, ty, "{protocol} {sql}: described type");
        assert_eq!(got_values, values, "{protocol} {sql}: values");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sum_of_bigint_is_an_exact_numeric_past_i64() {
    let (_s, pool) = seeded().await;
    // i64::MAX + 1: exact, where it used to be `bigint out of range`.
    for sql in ["SELECT SUM(n) FROM s WHERE g = 1", "SELECT SUM(n) FROM s GROUP BY g HAVING COUNT(*) = 2"] {
        check(typed(&pool, sql).await, "NUMERIC", &["9223372036854775808"], sql);
    }
    let sql = "SELECT SUM(n) FROM s GROUP BY g ORDER BY SUM(n) DESC";
    check(typed(&pool, sql).await, "NUMERIC", &["9223372036854775808", "5"], sql);
    let sql = "SELECT SUM(n) FROM s GROUP BY g HAVING SUM(n) > 9223372036854775807";
    check(typed(&pool, sql).await, "NUMERIC", &["9223372036854775808"], sql);
    // Computed over the sum: still numeric, still exact.
    let sql = "SELECT SUM(n) * 2 FROM s WHERE g = 1";
    check(typed(&pool, sql).await, "NUMERIC", &["18446744073709551616"], sql);
    // A window SUM over int8 is numeric too.
    let sql = "SELECT SUM(n) OVER (PARTITION BY g) FROM s ORDER BY id";
    check(
        typed(&pool, sql).await,
        "NUMERIC",
        &["9223372036854775808", "9223372036854775808", "5"],
        sql,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sum_of_int_and_smallint_is_bigint() {
    let (_s, pool) = seeded().await;
    for sql in ["SELECT SUM(i) FROM s", "SELECT SUM(h) FROM s", "SELECT SUM(i) OVER () FROM s LIMIT 1"] {
        check(typed(&pool, sql).await, "INT8", &["5"], sql);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sum_of_floats_keeps_their_width() {
    let (_s, pool) = seeded().await;
    check(typed(&pool, "SELECT SUM(r) FROM s").await, "FLOAT4", &["4"], "SUM(real)");
    check(typed(&pool, "SELECT SUM(d) FROM s").await, "FLOAT8", &["4"], "SUM(double)");
    check(typed(&pool, "SELECT AVG(r) FROM s").await, "FLOAT8", &["1.3333333333333333"], "AVG(real)");
}

/// `avg(1, 2, 2)` is `1.6666666666666667`, PostgreSQL's sixteen places.
#[tokio::test(flavor = "multi_thread")]
async fn avg_of_integers_is_numeric_with_postgres_digits() {
    let (_s, pool) = seeded().await;
    for sql in ["SELECT AVG(i) FROM s", "SELECT AVG(h) FROM s", "SELECT AVG(i) OVER () FROM s LIMIT 1"] {
        check(typed(&pool, sql).await, "NUMERIC", &["1.6666666666666667"], sql);
    }
    let sql = "SELECT AVG(n) FROM s WHERE g = 2";
    check(typed(&pool, sql).await, "NUMERIC", &["5"], sql);
}

/// Rows that leave one query for another carry no scale; the subquery boundary
/// brings them to the engine's. A `sum(bigint)` (scale 0) and a
/// `NUMERIC(10,2)` column (scale 2) read through a derived table, a CTE or a
/// scalar subquery used to come back a power of ten away from themselves.
#[tokio::test(flavor = "multi_thread")]
async fn decimals_keep_their_value_through_subqueries() {
    let (_s, pool) = seeded().await;
    run(&pool, "CREATE TABLE p (id INT NOT NULL, price NUMERIC(10,2) NOT NULL)").await;
    run(&pool, "INSERT INTO p (id, price) VALUES (1, 1.25)").await;
    run(&pool, "INSERT INTO p (id, price) VALUES (2, 2.50)").await;
    for (sql, want) in [
        ("SELECT q.price FROM (SELECT price FROM p WHERE id = 1) AS q", "1.25"),
        ("SELECT SUM(q.price) FROM (SELECT price FROM p) AS q", "3.75"),
        ("SELECT q.t FROM (SELECT SUM(n) AS t FROM s WHERE g = 2) AS q", "5"),
        ("SELECT AVG(q.t) FROM (SELECT SUM(i) AS t FROM s GROUP BY g) AS q", "2.5"),
        ("WITH c AS (SELECT SUM(n) AS t FROM s WHERE g = 2) SELECT t FROM c", "5"),
        ("SELECT id FROM s WHERE n = (SELECT SUM(n) FROM s WHERE g = 2)", "3"),
        ("SELECT id FROM p WHERE price = (SELECT MAX(price) FROM p)", "2"),
    ] {
        for (protocol, _, values) in typed(&pool, sql).await {
            assert_eq!(values, vec![want.to_string()], "{protocol} {sql}");
        }
    }
}
