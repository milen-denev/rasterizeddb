//! The `field` table shape from a case step, with sea-orm's `RETURNING *`.
//!
//! `content_string_vec: Option<Vec<String>>` is a nullable `TEXT[]` and
//! `content_attachment: Option<Vec<u8>>` a nullable `BYTEA`. Arrays are the
//! type that makes a wire-shape bug audible: sqlx's array decoder reads five
//! `i32` headers before any element, so a zero-length payload sent under a
//! real array type panics inside `bytes` — "advance out of bounds: the len is
//! 0 but advancing by 4" — instead of returning an error. Every other type
//! decodes an empty payload as an empty value and says nothing.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn create(pool: &sqlx::PgPool) {
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE field (\
           id INT PRIMARY KEY, \
           case_id INT NOT NULL, \
           step_id INT NOT NULL, \
           label TEXT NOT NULL, \
           content_string TEXT, \
           content_string_vec TEXT[], \
           content_attachment BYTEA, \
           content_number INT, \
           content_bool BOOLEAN)",
    ))
    .await
    .expect("create field");
}

/// Insert with everything nullable left NULL, returning every column.
#[tokio::test]
async fn insert_returning_every_column_with_nulls() {
    let (_s, pool) = server().await;
    create(&pool).await;

    let row = sqlx::query(
        "INSERT INTO field (id, case_id, step_id, label) VALUES ($1, $2, $3, $4) \
         RETURNING id, case_id, step_id, label, content_string, content_string_vec, \
                   content_attachment, content_number, content_bool",
    )
    .bind(1i32)
    .bind(7i32)
    .bind(3i32)
    .bind("Notes")
    .fetch_one(&pool)
    .await
    .expect("insert returning");

    assert_eq!(row.get::<i32, _>("id"), 1);
    assert_eq!(row.get::<Option<String>, _>("content_string"), None);
    assert_eq!(row.get::<Option<Vec<String>>, _>("content_string_vec"), None);
    assert_eq!(row.get::<Option<Vec<u8>>, _>("content_attachment"), None);
    assert_eq!(row.get::<Option<i32>, _>("content_number"), None);
    assert_eq!(row.get::<Option<bool>, _>("content_bool"), None);
}

/// The update a case step performs when data is entered, returning everything.
#[tokio::test]
async fn update_returning_every_column_with_nulls() {
    let (_s, pool) = server().await;
    create(&pool).await;
    sqlx::query("INSERT INTO field (id, case_id, step_id, label) VALUES ($1, $2, $3, $4)")
        .bind(1i32)
        .bind(7i32)
        .bind(3i32)
        .bind("Notes")
        .execute(&pool)
        .await
        .expect("insert");

    // Sets only the text content; the array and bytea stay NULL and still come back.
    let row = sqlx::query(
        "UPDATE field SET content_string = $1 WHERE id = $2 \
         RETURNING id, case_id, step_id, label, content_string, content_string_vec, \
                   content_attachment, content_number, content_bool",
    )
    .bind("typed by the user")
    .bind(1i32)
    .fetch_one(&pool)
    .await
    .expect("update returning");

    assert_eq!(row.get::<Option<String>, _>("content_string"), Some("typed by the user".into()));
    assert_eq!(row.get::<Option<Vec<String>>, _>("content_string_vec"), None);
    assert_eq!(row.get::<Option<Vec<u8>>, _>("content_attachment"), None);
}

/// A populated array alongside a NULL one, both directions.
#[tokio::test]
async fn arrays_populated_and_null_round_trip() {
    let (_s, pool) = server().await;
    create(&pool).await;

    sqlx::query(
        "INSERT INTO field (id, case_id, step_id, label, content_string_vec) \
         VALUES ($1, $2, $3, $4, $5) RETURNING content_string_vec, content_attachment",
    )
    .bind(1i32)
    .bind(7i32)
    .bind(3i32)
    .bind("Choices")
    .bind(vec!["a".to_string(), "b".to_string()])
    .fetch_one(&pool)
    .await
    .expect("insert with array")
    ;

    let row = sqlx::query("SELECT content_string_vec, content_attachment FROM field WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select");
    assert_eq!(
        row.get::<Option<Vec<String>>, _>("content_string_vec"),
        Some(vec!["a".to_string(), "b".to_string()])
    );
    assert_eq!(row.get::<Option<Vec<u8>>, _>("content_attachment"), None);
}
