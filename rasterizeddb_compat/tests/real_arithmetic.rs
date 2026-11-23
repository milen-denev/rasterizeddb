//! `real` arithmetic, against PostgreSQL 17's answers.
//!
//! `real op real` and `-real` were computed in double precision and returned
//! as double: `0.1::real * 3::real` printed 0.30000000447034836 described as
//! `double precision`, where PostgreSQL's is the `real` 0.3. A `real` with any
//! other number is double in both.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

async fn code_of(pool: &sqlx::PgPool, sql: &str) -> String {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).execute(pool).await.expect_err(sql);
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread")]
async fn real_with_real_stays_real() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::raw_sql(
        "CREATE TABLE rr (id int, r real, d float8); INSERT INTO rr VALUES (1, 0.1, 0.1), (2, 0.2, 0.2), (3, 1e30, 1e30);",
    )
    .execute(&pool)
    .await
    .expect("setup");
    assert_eq!(
        rows(
            &pool,
            "SELECT 0.1::real * 3::real, pg_typeof(0.1::real * 3::real), 0.1::real + 0.2::real, 1::real / 3::real, \
             0.1::real * 3, pg_typeof(0.1::real * 3), 0.1::real * 3.0, 0.1::real * 0.2::float8, -0.1::real, \
             pg_typeof(-0.1::real), 0.1::real - 0.3::real"
        )
        .await,
        vec!["0.3|real|0.3|0.33333334|0.30000000447034836|double precision|0.30000000447034836|0.020000000298023225|-0.1|real|-0.20000002"]
    );
    assert_eq!(
        rows(&pool, "SELECT id, r * 3, r + r, r * r, -r, pg_typeof(r + r), r * d FROM rr WHERE id < 3 ORDER BY id").await,
        vec![
            "1|0.30000000447034836|0.2|0.010000001|-0.1|real|0.010000000149011612",
            "2|0.6000000089406967|0.4|0.040000003|-0.2|real|0.04000000059604645",
        ]
    );
    assert_eq!(
        rows(&pool, "SELECT sum(r), pg_typeof(sum(r)), avg(r), pg_typeof(avg(r)), max(r), min(r) FROM rr WHERE id < 3").await,
        vec!["0.3|real|0.15000000223517418|double precision|0.2|0.1"]
    );
    assert_eq!(rows(&pool, "SELECT id FROM rr WHERE r + r = 0.4::real").await, vec!["2"]);
    for (sql, code) in [
        ("SELECT 1e30::real * 1e30::real", "22003"),
        ("SELECT 1e-30::real * 1e-30::real", "22003"),
        ("SELECT 1::real / 0::real", "22012"),
        ("SELECT r * r FROM rr WHERE id = 3", "22003"),
    ] {
        assert_eq!(code_of(&pool, sql).await, code, "{sql}");
    }
}
