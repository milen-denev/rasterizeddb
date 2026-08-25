//! A zero-length, non-NULL array field is a client panic, not an error.
//!
//! sqlx's `Vec<T>` decoder:
//!
//!     let mut buf = value.as_bytes()?;   // Err for NULL, so Option<Vec<T>> is safe
//!     let ndim = buf.get_i32();          // no length check
//!
//! NULL is length -1 and decodes fine. An *empty* field is length 0, is not
//! NULL, and reaches `get_i32()` with nothing behind it — "advance out of
//! bounds: the len is 0 but advancing by 4". So every path that can render an
//! array column as empty-but-present is a way to panic a client.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn create(pool: &sqlx::PgPool) {
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE arr (id INT PRIMARY KEY, tags TEXT[], nums INT[], label TEXT)",
    ))
    .await
    .expect("create");
}

/// Reading is what panics, so every case ends in a decode.
fn read(row: &sqlx::postgres::PgRow, label: &str) {
    // `try_get`, not `get`: a decode *error* must be reported as one, not
    // unwrapped into a panic that looks like the wire bug being hunted.
    let tags = row.try_get::<Option<Vec<String>>, _>("tags");
    let nums = row.try_get::<Option<Vec<i32>>, _>("nums");
    println!(
        "{:<40} tags={:?} nums={:?}",
        label,
        tags.as_ref().map_err(|e| e.to_string()),
        nums.as_ref().map_err(|e| e.to_string())
    );
    assert!(tags.is_ok(), "{}: text[] must decode: {:?}", label, tags.err());
    // `nums` is an INT[] and is deliberately not asserted here: every array
    // column is advertised as `text[]`, so a typed client cannot decode an
    // integer array at all. `an_int_array_is_advertised_as_text` records that
    // separately — it is a missing element type, not a wire-shape bug, and it
    // is orthogonal to what these cases cover.
}

#[tokio::test]
async fn empty_array_written_as_a_literal() {
    let (_s, pool) = server().await;
    create(&pool).await;
    pool.execute(sqlx::raw_sql(
        "INSERT INTO arr (id, tags, nums, label) VALUES (1, '{}', '{}', 'x')",
    ))
    .await
    .expect("insert empty literals");

    let row = sqlx::query("SELECT tags, nums FROM arr WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select");
    read(&row, "empty array written as '{}' literal");
}

#[tokio::test]
async fn array_emptied_by_an_update() {
    let (_s, pool) = server().await;
    create(&pool).await;
    sqlx::query("INSERT INTO arr (id, tags, nums, label) VALUES ($1, $2, $3, $4)")
        .bind(1i32)
        .bind(vec!["a".to_string()])
        .bind(vec![1i32])
        .bind("x")
        .execute(&pool)
        .await
        .expect("insert populated");

    // The user clears a multi-select.
    sqlx::query("UPDATE arr SET tags = $1, nums = $2 WHERE id = $3")
        .bind(Vec::<String>::new())
        .bind(Vec::<i32>::new())
        .bind(1i32)
        .execute(&pool)
        .await
        .expect("empty the arrays");

    let row = sqlx::query("SELECT tags, nums FROM arr WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select after emptying");
    read(&row, "arrays emptied by UPDATE");
}

/// Emptied and read back through RETURNING, on the same statement.
#[tokio::test]
async fn array_emptied_with_returning() {
    let (_s, pool) = server().await;
    create(&pool).await;
    sqlx::query("INSERT INTO arr (id, tags, nums, label) VALUES ($1, $2, $3, $4)")
        .bind(1i32)
        .bind(vec!["a".to_string()])
        .bind(vec![1i32])
        .bind("x")
        .execute(&pool)
        .await
        .expect("insert populated");

    let row = sqlx::query("UPDATE arr SET tags = $1 WHERE id = $2 RETURNING tags, nums")
        .bind(Vec::<String>::new())
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("update returning");
    read(&row, "array emptied, read via RETURNING");
}

/// An empty array inserted with RETURNING, the sea-orm insert shape.
#[tokio::test]
async fn empty_array_inserted_with_returning() {
    let (_s, pool) = server().await;
    create(&pool).await;

    let row = sqlx::query("INSERT INTO arr (id, tags, nums, label) VALUES ($1, $2, $3, $4) RETURNING tags, nums")
        .bind(1i32)
        .bind(Vec::<String>::new())
        .bind(Vec::<i32>::new())
        .bind("x")
        .fetch_one(&pool)
        .await
        .expect("insert returning empty arrays");
    read(&row, "empty arrays inserted, read via RETURNING");
}

/// Several rows at once, mixing NULL, empty and populated — the shape a page
/// load produces.
#[tokio::test]
async fn mixed_rows_in_one_result() {
    let (_s, pool) = server().await;
    create(&pool).await;

    sqlx::query("INSERT INTO arr (id, tags, nums, label) VALUES ($1, $2, $3, $4)")
        .bind(1i32)
        .bind(Option::<Vec<String>>::None)
        .bind(Option::<Vec<i32>>::None)
        .bind("null row")
        .execute(&pool)
        .await
        .expect("null row");
    sqlx::query("INSERT INTO arr (id, tags, nums, label) VALUES ($1, $2, $3, $4)")
        .bind(2i32)
        .bind(Vec::<String>::new())
        .bind(Vec::<i32>::new())
        .bind("empty row")
        .execute(&pool)
        .await
        .expect("empty row");
    sqlx::query("INSERT INTO arr (id, tags, nums, label) VALUES ($1, $2, $3, $4)")
        .bind(3i32)
        .bind(vec!["a".to_string(), "b".to_string()])
        .bind(vec![7i32])
        .bind("full row")
        .execute(&pool)
        .await
        .expect("full row");

    let rows = sqlx::query("SELECT id, tags, nums FROM arr")
        .fetch_all(&pool)
        .await
        .expect("select all");
    for row in &rows {
        read(row, &format!("row id={}", row.get::<i32, _>("id")));
    }
}

/// An array column loses its element type on the way out.
///
/// Ignored: records an open bug. `DbType::ARRAY` maps to OID 1009 — `text[]` —
/// for every array column whatever it holds, because the schema keeps no
/// element type. A `TEXT[]` therefore works and an `INT[]`, `UUID[]` or
/// `BOOL[]` cannot be decoded by any client that checks types:
///
///   mismatched types; Rust type `Vec<i32>` (as SQL type `INT4[]`) is not
///   compatible with SQL type `TEXT[]`
///
/// The values themselves round-trip — this is the description being wrong, not
/// the data. Fixing it means recording the element type on the column, which
/// is a schema change rather than a formatting one.
#[ignore = "open: array columns are all advertised as text[]"]
#[tokio::test]
async fn an_int_array_is_advertised_as_text() {
    let (_s, pool) = server().await;
    create(&pool).await;
    sqlx::query("INSERT INTO arr (id, nums, label) VALUES ($1, $2, $3)")
        .bind(1i32)
        .bind(vec![1i32, 2, 3])
        .bind("x")
        .execute(&pool)
        .await
        .expect("insert int array");

    let row = sqlx::query("SELECT nums FROM arr WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select");
    assert_eq!(
        row.try_get::<Option<Vec<i32>>, _>("nums").expect("int[] must decode"),
        Some(vec![1, 2, 3])
    );
}
