//! Multidimensional arrays, against PostgreSQL 17's answers.
//!
//! The array literal was read one level deep: `'{{a,b},{c,d}}'` in a `text[]`
//! column was stored as the four elements `{a`, `b}`, `{c`, `d}` and printed
//! back as `{"{a","b}","{c","d}"}` — no error. And `unnest(ARRAY[ARRAY[1,2],
//! ARRAY[3,4]])` returned the two inner arrays as text.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::raw_sql("CREATE TABLE md (id INT, t TEXT[], i INT[]);").execute(&pool).await.expect("setup");
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

async fn code_of(pool: &sqlx::PgPool, sql: &str) -> String {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).execute(pool).await.expect_err(sql);
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_nested_literal_is_stored_with_its_dimensions() {
    let (_s, pool) = server().await;
    sqlx::raw_sql("INSERT INTO md VALUES (1, '{{a,b},{c,d}}', '{{1,2},{3,4}}')").execute(&pool).await.expect("insert");
    assert_eq!(rows(&pool, "SELECT t, i FROM md").await, ["{{a,b},{c,d}}|{{1,2},{3,4}}"]);
    assert_eq!(
        rows(&pool, "SELECT t[1][2], i[2][1], i[1], array_ndims(i), array_length(i, 1), array_length(i, 2), cardinality(i) FROM md")
            .await,
        ["b|3|NULL|2|2|2|4"]
    );
    assert_eq!(rows(&pool, "SELECT 3 = ANY(i), array_to_string(i, ',') FROM md").await, ["t|1,2,3,4"]);
    // Ragged, and an element the type refuses: PostgreSQL's 22P02.
    assert_eq!(code_of(&pool, "INSERT INTO md (t) VALUES ('{{a},{b,c}}')").await, "22P02");
    assert_eq!(code_of(&pool, "INSERT INTO md (i) VALUES ('{{1,x}}')").await, "22P02");
    // Binary, typed: a two-dimensional integer[] is not a Vec<i32>, but its
    // bytes say two dimensions of int4.
    let row = sqlx::query("SELECT i FROM md").fetch_one(&pool).await.expect("select");
    use sqlx::ValueRef;
    let raw = row.try_get_raw(0).expect("raw");
    let bytes = raw.as_bytes().expect("binary");
    assert_eq!(&bytes[..4], &2i32.to_be_bytes(), "two dimensions");
    assert_eq!(&bytes[8..12], &23i32.to_be_bytes(), "int4 elements");
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_constructors_build_one_array() {
    let (_s, pool) = server().await;
    assert_eq!(
        rows(&pool, "SELECT ARRAY[ARRAY[1,2],ARRAY[3,4]], (ARRAY[ARRAY['a','b'],ARRAY['c','d']])[2][2]").await,
        ["{{1,2},{3,4}}|d"]
    );
    assert_eq!(code_of(&pool, "SELECT ARRAY[ARRAY[1,2],ARRAY[3]]").await, "2202E");
    assert_eq!(rows(&pool, "SELECT x FROM unnest(ARRAY[ARRAY[1,2],ARRAY[3,4]]) x").await, ["1", "2", "3", "4"]);
}
