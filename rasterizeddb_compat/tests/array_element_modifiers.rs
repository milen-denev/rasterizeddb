//! An array column's element modifier, against PostgreSQL 17's answers.
//!
//! `numeric(5,2)[]`, `varchar(3)[]`, `timestamp(0)[]`: the element's
//! modifier was dropped when the column was declared, so `'{1.234}'` was
//! stored as written where PostgreSQL stores `{1.23}`, `'{abcd}'` went into a
//! `varchar(3)[]` PostgreSQL refuses it from, and a `timestamp(0)[]` kept
//! its fractions. A `timestamp(p)[]` was not even typed: the column took the
//! scalar's precision and was an array of text. And a numeric element read
//! back (`a[1]`) lost its places: `{2.00}`'s was 2.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::raw_sql(
        "CREATE TABLE v (id int, a varchar(3)[], b char(3)[], c numeric(5,2)[], d timestamp(0)[], \
         e timestamp(3) with time zone[], f char[], g numeric(4)[], h interval(0)[], i time(1)[], n numeric[]);",
    )
    .execute(&pool)
    .await
    .expect("setup");
    (server, pool)
}

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

async fn error_of(pool: &sqlx::PgPool, sql: &str) -> (String, String) {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).execute(pool).await.expect_err(sql);
    let d = e.as_database_error().expect("a database error");
    (d.code().map(|c| c.to_string()).unwrap_or_default(), d.message().to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn each_element_is_held_to_the_columns_element_modifier() {
    let (_s, pool) = server().await;
    sqlx::raw_sql(
        "INSERT INTO v VALUES (1, '{ab,\"c  \"}', '{abc,c}', '{1.234,2}', '{\"2026-01-01 10:00:00.6\"}', \
         '{\"2026-01-01 10:00:00.12345+02\"}', '{x}', '{12.5}', '{\"1 day 00:00:01.6\"}', '{10:00:00.66}', '{2.00,1.5}')",
    )
    .execute(&pool)
    .await
    .expect("insert");
    assert_eq!(
        rows(&pool, "SELECT a, c, d, e, f, g, h, i FROM v").await,
        vec![
            "{ab,\"c  \"}|{1.23,2.00}|{\"2026-01-01 10:00:01\"}|{\"2026-01-01 08:00:00.123+00\"}|{x}|{13}|\
             {\"1 day 00:00:02\"}|{10:00:00.7}"
        ]
    );
    // A numeric element keeps its places when read back, as an operand too.
    assert_eq!(rows(&pool, "SELECT a[2] || '|', c[1] + 1, c[2], d[1], n[1], n[1] + 1 FROM v").await, vec![
        "c  ||2.23|2.00|2026-01-01 10:00:01|2.00|3.00"
    ]);
    // An UPDATE is held as an INSERT is.
    sqlx::raw_sql("UPDATE v SET c = ARRAY[1.005, 2.5] WHERE id = 1").execute(&pool).await.expect("update");
    assert_eq!(rows(&pool, "SELECT c FROM v").await, vec!["{1.01,2.50}"]);
    // A text past its length only by spaces is cut to it.
    sqlx::raw_sql("INSERT INTO v (id, a) VALUES (2, '{\"ab   \"}')").execute(&pool).await.expect("insert");
    assert_eq!(rows(&pool, "SELECT a FROM v WHERE id = 2").await, vec!["{\"ab \"}"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_element_the_modifier_cannot_hold_is_refused() {
    let (_s, pool) = server().await;
    let overflow = "numeric field overflow: a field with precision 5, scale 2 must round to an absolute value less than 10^3";
    let cases: &[(&str, &str, &str)] = &[
        ("INSERT INTO v (id, a) VALUES (2, '{abcd}')", "22001", "value too long for type character varying(3)"),
        ("INSERT INTO v (id, c) VALUES (3, '{1234.5}')", "22003", overflow),
        // Rounded first, then checked, as PostgreSQL checks it.
        ("INSERT INTO v (id, c) VALUES (3, '{999.995}')", "22003", overflow),
        ("INSERT INTO v (id, b) VALUES (4, '{abcd}')", "22001", "value too long for type character(3)"),
        // A bare `char` is `character(1)`.
        ("INSERT INTO v (id, f) VALUES (4, '{xy}')", "22001", "value too long for type character(1)"),
    ];
    for (sql, code, message) in cases {
        assert_eq!(error_of(&pool, sql).await, (code.to_string(), message.to_string()), "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_text_array_becomes_a_varchar_array_without_using() {
    let (_s, pool) = server().await;
    sqlx::raw_sql("CREATE TABLE w (id int, t text[]); INSERT INTO w VALUES (1, '{ab,abcd}'), (2, '{a}');")
        .execute(&pool)
        .await
        .expect("setup");
    // Text casts to varchar implicitly; a stored element too long refuses it.
    assert_eq!(
        error_of(&pool, "ALTER TABLE w ALTER COLUMN t TYPE varchar(3)[]").await,
        ("22001".to_string(), "value too long for type character varying(3)".to_string())
    );
    sqlx::raw_sql("DELETE FROM w WHERE id = 1; ALTER TABLE w ALTER COLUMN t TYPE varchar(3)[];")
        .execute(&pool)
        .await
        .expect("alter");
    assert_eq!(
        error_of(&pool, "INSERT INTO w VALUES (3, '{abcd}')").await,
        ("22001".to_string(), "value too long for type character varying(3)".to_string())
    );
    assert_eq!(rows(&pool, "SELECT t FROM w ORDER BY id").await, vec!["{a}"]);
}
