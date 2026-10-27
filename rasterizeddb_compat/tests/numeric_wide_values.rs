//! NUMERIC values past the evaluator's nine places, stored and rendered.
//!
//! - NUMERIC(38,0) refused 38-digit values with 22003: the INSERT rounds by
//!   reading the literal one place deeper, which does not fit an i128.
//! - `amt::text` on NUMERIC(20,12) printed 1.000000000000 for
//!   1.000000000001, and on NUMERIC(38,0) failed "out of range for
//!   arithmetic": the cast went through the engine's nine places.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn text(pool: &sqlx::PgPool, sql: &str) -> String {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .get::<String, _>(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn thirty_eight_digits_fit_numeric_38_0() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute("CREATE TABLE n38 (id INT, v NUMERIC(38,0))").await.unwrap();
    for (id, v) in [
        (1, "99999999999999999999999999999999999998"),
        (2, "99999999999999999999999999999999999999"),
        (3, "-99999999999999999999999999999999999999"),
        (4, "12345678901234567890123456789012345678"),
    ] {
        pool.execute(AssertSqlSafe(format!("INSERT INTO n38 VALUES ({id}, {v})"))).await.unwrap_or_else(|e| panic!("{v}: {e}"));
        assert_eq!(text(&pool, &format!("SELECT v::text FROM n38 WHERE id = {id}")).await, v, "stored or rendered wrong");
    }
    // 39 digits, and a 38-digit value with a fraction to round, still overflow.
    for v in ["999999999999999999999999999999999999990", "99999999999999999999999999999999999999.5"] {
        let e = pool.execute(AssertSqlSafe(format!("INSERT INTO n38 VALUES (9, {v})"))).await.expect_err(v);
        assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("22003"), "{v}: {e}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_numeric_cast_to_text_keeps_every_declared_place() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute("CREATE TABLE n12 (id INT, amt NUMERIC(20,12))").await.unwrap();
    pool.execute("INSERT INTO n12 VALUES (1, 1.000000000001), (2, -12345678.123456789012), (3, NULL), (4, 2.5)")
        .await
        .unwrap();
    for (id, want) in [(1, "1.000000000001"), (2, "-12345678.123456789012"), (4, "2.500000000000")] {
        assert_eq!(text(&pool, &format!("SELECT amt::text FROM n12 WHERE id = {id}")).await, want);
        assert_eq!(text(&pool, &format!("SELECT CAST(amt AS TEXT) FROM n12 WHERE id = {id}")).await, want);
    }
    let null: Option<String> = sqlx::query("SELECT amt::text FROM n12 WHERE id = 3").fetch_one(&pool).await.unwrap().get(0);
    assert_eq!(null, None);
    // A NUMERIC(5,2) still shows its two places.
    pool.execute("CREATE TABLE n5 (d NUMERIC(5,2))").await.unwrap();
    pool.execute("INSERT INTO n5 VALUES (2.5)").await.unwrap();
    assert_eq!(text(&pool, "SELECT d::text FROM n5").await, "2.50");
}
