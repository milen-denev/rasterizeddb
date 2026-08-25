//! `::jsonb` on a JSON value, the form pms's Ultra companion writer emits.
//!
//! The shredding path reads the literal itself — it has to, because the
//! document becomes rows in a shadow table before the statement runs. It
//! recovers the JSON text by unquoting a literal that starts and ends with a
//! single quote, and a trailing `::jsonb` means it ends with `b` instead. The
//! whole token, cast included, then went to `serde_json`, which rejected it at
//! the first character:
//!
//!   invalid JSON for column "c_…": expected value at line 1 column 1
//!
//! Verbatim from production, inserting user-table rows through the API.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    // The deployment that hit this shreds JSON columns into shadow tables.
    let _ = rasterizeddb_core::JSON_SHADOW_TABLES.set(true);
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

/// An object — the shape the shredding path is built for.
#[tokio::test]
async fn a_json_object_with_a_cast_is_accepted() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE utrn_j (row_id INT PRIMARY KEY, c_doc JSONB)",
    ))
    .await
    .expect("create");

    pool.execute(sqlx::raw_sql(
        "INSERT INTO utrn_j (row_id, c_doc) VALUES (1, '{\"a\": 1, \"b\": \"x\"}'::jsonb)",
    ))
    .await
    .expect("an object with a ::jsonb cast must be accepted");

    let row = sqlx::query("SELECT c_doc FROM utrn_j WHERE row_id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select");
    let doc: serde_json::Value = row.get("c_doc");
    assert_eq!(doc["a"], serde_json::json!(1));
    assert_eq!(doc["b"], serde_json::json!("x"));
}

/// The exact value from the production log: a JSON *string* holding a UUID.
///
/// This is the ordinary content of an attachment column — a file id — not an
/// exotic shape, so shredding stores it as one row carrying the value under a
/// `s` kind marker, reusing the typed value columns the array-element path
/// already had.
#[tokio::test]
async fn the_production_jsonb_literal_is_accepted() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE utrn_p (row_id INT PRIMARY KEY, c_ref JSONB)",
    ))
    .await
    .expect("create");

    let r = pool
        .execute(sqlx::raw_sql(
            "INSERT INTO utrn_p (row_id, c_ref) VALUES (1, '\"2f3cb2bc-a050-496c-a0be-448ff06021f5\"'::jsonb)",
        ))
        .await;
    match r {
        Ok(_) => {}
        Err(e) => panic!("production literal refused: {}", e),
    }

    let row = sqlx::query("SELECT c_ref FROM utrn_p WHERE row_id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select");
    let doc: serde_json::Value = row.get("c_ref");
    assert_eq!(
        doc,
        serde_json::json!("2f3cb2bc-a050-496c-a0be-448ff06021f5")
    );
}

/// Every JSON shape in one shredded column, including the ones that already
/// worked — a scalar must not disturb how an object or an array reads back.
#[tokio::test]
async fn scalars_objects_and_arrays_share_a_column() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE utrn_mix (row_id INT PRIMARY KEY, c_v JSONB)",
    ))
    .await
    .expect("create");

    let cases: Vec<(i32, &str, serde_json::Value)> = vec![
        (1, "'\"a file id\"'::jsonb", serde_json::json!("a file id")),
        (2, "'42'::jsonb", serde_json::json!(42)),
        (3, "'true'::jsonb", serde_json::json!(true)),
        (4, "'{\"k\": \"v\"}'::jsonb", serde_json::json!({"k": "v"})),
        (5, "'[1, 2, 3]'::jsonb", serde_json::json!([1, 2, 3])),
        (6, "'[]'::jsonb", serde_json::json!([])),
        (7, "'\"\"'::jsonb", serde_json::json!("")),
        (8, "'-1.5'::jsonb", serde_json::json!(-1.5)),
    ];

    for (id, literal, _) in &cases {
        pool.execute(sqlx::raw_sql(&format!(
            "INSERT INTO utrn_mix (row_id, c_v) VALUES ({}, {})",
            id, literal
        )))
        .await
        .unwrap_or_else(|e| panic!("insert {}: {}", literal, e));
    }

    for (id, literal, expected) in &cases {
        let row = sqlx::query("SELECT c_v FROM utrn_mix WHERE row_id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|e| panic!("select {}: {}", literal, e));
        assert_eq!(
            row.get::<serde_json::Value, _>("c_v"),
            *expected,
            "{} must read back unchanged",
            literal
        );
    }
}
