//! The same production literal with JSON stored natively (the default).
//!
//! `JSON_SHADOW_TABLES` is a process-global, so this needs its own test binary
//! to run in the opposite mode from `jsonb_cast_literals.rs`.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

#[tokio::test]
async fn a_json_scalar_round_trips_when_stored_natively() {
    // Deliberately left at the default (false = native bytes).
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");

    pool.execute(sqlx::raw_sql(
        "CREATE TABLE utrn_n (row_id INT PRIMARY KEY, c_ref JSONB, c_doc JSONB)",
    ))
    .await
    .expect("create");

    pool.execute(sqlx::raw_sql(
        "INSERT INTO utrn_n (row_id, c_ref, c_doc) VALUES (1, '\"2f3cb2bc-a050-496c-a0be-448ff06021f5\"'::jsonb, '{\"a\": 1}'::jsonb)",
    ))
    .await
    .expect("a JSON string and an object, both with casts");

    let row = sqlx::query("SELECT c_ref, c_doc FROM utrn_n WHERE row_id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select");
    assert_eq!(
        row.get::<serde_json::Value, _>("c_ref"),
        serde_json::json!("2f3cb2bc-a050-496c-a0be-448ff06021f5")
    );
    assert_eq!(row.get::<serde_json::Value, _>("c_doc")["a"], serde_json::json!(1));
}

/// Bound as a parameter with the cast on the placeholder — what pms sends.
#[tokio::test]
async fn a_bound_jsonb_parameter_with_a_cast_round_trips() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");

    pool.execute(sqlx::raw_sql(
        "CREATE TABLE utrn_b (row_id INT PRIMARY KEY, c_doc JSONB)",
    ))
    .await
    .expect("create");

    sqlx::query("INSERT INTO utrn_b (row_id, c_doc) VALUES ($1, $2::jsonb)")
        .bind(1i32)
        .bind("{\"a\": 1, \"b\": [1, 2]}")
        .execute(&pool)
        .await
        .expect("bound document with a ::jsonb cast");

    // And SQL NULL through the same shape.
    sqlx::query("INSERT INTO utrn_b (row_id, c_doc) VALUES ($1, $2::jsonb)")
        .bind(2i32)
        .bind(Option::<String>::None)
        .execute(&pool)
        .await
        .expect("bound NULL with a ::jsonb cast");

    let row = sqlx::query("SELECT c_doc FROM utrn_b WHERE row_id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select doc");
    assert_eq!(row.get::<serde_json::Value, _>("c_doc")["b"][1], serde_json::json!(2));

    let null_row = sqlx::query("SELECT c_doc FROM utrn_b WHERE row_id = $1")
        .bind(2i32)
        .fetch_one(&pool)
        .await
        .expect("select null");
    assert_eq!(null_row.get::<Option<serde_json::Value>, _>("c_doc"), None);
}
