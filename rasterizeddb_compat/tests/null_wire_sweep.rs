//! Every nullable type, through every path sea-orm uses, decoded in binary.
//!
//! Hunting one failure mode: a **zero-length, non-NULL** field sent under a
//! type the client decodes with a fixed-width read. sqlx does not length-check
//! those — `int4` is `buf.get_i32()` — so the result is a panic inside `bytes`
//! ("advance out of bounds: the len is 0 but advancing by 4") rather than an
//! error. Arrays make it loudest, reading five `i32` headers before any
//! element, but any 4-byte type does it.
//!
//! A panic here surfaces as a test failure naming the column and the path.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

const COLUMNS: &str = "\
    c_int INT, \
    c_bigint BIGINT, \
    c_smallint SMALLINT, \
    c_real REAL, \
    c_double DOUBLE PRECISION, \
    c_numeric NUMERIC(12,2), \
    c_bool BOOLEAN, \
    c_text TEXT, \
    c_uuid UUID, \
    c_date DATE, \
    c_ts TIMESTAMP, \
    c_bytea BYTEA, \
    c_jsonb JSONB, \
    c_text_arr TEXT[], \
    c_int_arr INT[]";

const LIST: &str = "c_int, c_bigint, c_smallint, c_real, c_double, c_numeric, c_bool, c_text, \
                    c_uuid, c_date, c_ts, c_bytea, c_jsonb, c_text_arr, c_int_arr";

/// Decodes every column at its declared type. Panics inside sqlx surface here.
fn decode_all(row: &sqlx::postgres::PgRow, path: &str) {
    let _ = row.get::<Option<i32>, _>("c_int");
    let _ = row.get::<Option<i64>, _>("c_bigint");
    let _ = row.get::<Option<i16>, _>("c_smallint");
    let _ = row.get::<Option<f32>, _>("c_real");
    let _ = row.get::<Option<f64>, _>("c_double");
    let _ = row.get::<Option<rust_decimal::Decimal>, _>("c_numeric");
    let _ = row.get::<Option<bool>, _>("c_bool");
    let _ = row.get::<Option<String>, _>("c_text");
    let _ = row.get::<Option<uuid::Uuid>, _>("c_uuid");
    let _ = row.get::<Option<chrono::NaiveDate>, _>("c_date");
    let _ = row.get::<Option<chrono::NaiveDateTime>, _>("c_ts");
    let _ = row.get::<Option<Vec<u8>>, _>("c_bytea");
    let _ = row.get::<Option<serde_json::Value>, _>("c_jsonb");
    let _ = row.get::<Option<Vec<String>>, _>("c_text_arr");
    let _ = row.get::<Option<Vec<i32>>, _>("c_int_arr");
    println!("decoded every column via {}", path);
}

#[tokio::test]
async fn all_nulls_through_insert_returning() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(&format!(
        "CREATE TABLE sweep (id INT PRIMARY KEY, {})",
        COLUMNS
    )))
    .await
    .expect("create");

    let row = sqlx::query(&format!(
        "INSERT INTO sweep (id) VALUES ($1) RETURNING id, {}",
        LIST
    ))
    .bind(1i32)
    .fetch_one(&pool)
    .await
    .expect("insert returning");
    decode_all(&row, "INSERT RETURNING");
}

#[tokio::test]
async fn all_nulls_through_update_returning_and_select() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(&format!(
        "CREATE TABLE sweep2 (id INT PRIMARY KEY, {})",
        COLUMNS
    )))
    .await
    .expect("create");
    sqlx::query("INSERT INTO sweep2 (id) VALUES ($1)")
        .bind(1i32)
        .execute(&pool)
        .await
        .expect("insert");

    let row = sqlx::query(&format!(
        "UPDATE sweep2 SET c_text = $1 WHERE id = $2 RETURNING id, {}",
        LIST
    ))
    .bind("set")
    .bind(1i32)
    .fetch_one(&pool)
    .await
    .expect("update returning");
    decode_all(&row, "UPDATE RETURNING");

    let row = sqlx::query(&format!("SELECT id, {} FROM sweep2 WHERE id = $1", LIST))
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select");
    decode_all(&row, "SELECT");

    let row = sqlx::query(&format!("SELECT id, {} FROM sweep2", LIST))
        .fetch_one(&pool)
        .await
        .expect("select all");
    decode_all(&row, "SELECT no predicate");
}

/// Empty-but-not-NULL values, which are the ones that ship a zero-length
/// payload legitimately — the decoder must tell them from a NULL.
#[tokio::test]
async fn empty_values_are_not_confused_with_nulls() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE empties (id INT PRIMARY KEY, t TEXT, b BYTEA, a TEXT[], j JSONB)",
    ))
    .await
    .expect("create");

    sqlx::query("INSERT INTO empties (id, t, b, a, j) VALUES ($1, $2, $3, $4, $5)")
        .bind(1i32)
        .bind("")
        .bind(Vec::<u8>::new())
        .bind(Vec::<String>::new())
        .bind(serde_json::json!({}))
        .execute(&pool)
        .await
        .expect("insert empties");

    let row = sqlx::query("SELECT t, b, a, j FROM empties WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select");
    assert_eq!(row.get::<Option<String>, _>("t"), Some(String::new()));
    assert_eq!(row.get::<Option<Vec<u8>>, _>("b"), Some(Vec::new()));
    assert_eq!(row.get::<Option<Vec<String>>, _>("a"), Some(Vec::new()));
    assert_eq!(row.get::<Option<serde_json::Value>, _>("j"), Some(serde_json::json!({})));
}
