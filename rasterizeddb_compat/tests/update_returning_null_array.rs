//! A NULL `text[]` in an `UPDATE … RETURNING` projection.
//!
//! From a backtrace: `Vec<String>::decode` panicking inside `bytes`.
//! `Option<T>::decode` short-circuits only on an *actual* NULL — length −1. A
//! value that is non-NULL but **zero-length** falls through to
//! `Vec<String>::decode`, which opens with an unguarded `get_i32()` for the
//! dimension count: four bytes against an empty buffer, "advance out of bounds:
//! the len is 0 but advancing by 4".
//!
//! The differential that localises it: a plain `SELECT` of the same NULL on the
//! same column is correct. It is the RETURNING projection on an UPDATE that
//! emits length 0.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

const ALL: &str = "id, case_id, step_id, label, content_string, content_string_vec, \
                   content_attachment, content_number, content_bool";

async fn setup(pool: &sqlx::PgPool) {
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE field (\
           id INT PRIMARY KEY, case_id INT NOT NULL, step_id INT NOT NULL, \
           label TEXT NOT NULL, content_string TEXT, content_string_vec TEXT[], \
           content_attachment BYTEA, content_number INT, content_bool BOOLEAN)",
    ))
    .await
    .expect("create");
    pool.execute(sqlx::raw_sql(
        "INSERT INTO field (id, case_id, step_id, label) VALUES (1, 7, 3, 'Notes')",
    ))
    .await
    .expect("seed");
}

/// The control: a plain SELECT of the same NULL is correct.
#[tokio::test]
async fn a_plain_select_reports_the_null_array() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    let row = sqlx::query("SELECT content_string_vec FROM field WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("select");
    assert_eq!(row.get::<Option<Vec<String>>, _>("content_string_vec"), None);
}

/// sea-orm's `model.update()` sets **every** column, including the array to
/// NULL, and returns every column.
#[tokio::test]
async fn update_setting_every_column_returns_the_null_array() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    let row = sqlx::query(&format!(
        "UPDATE field SET case_id = $1, step_id = $2, label = $3, content_string = $4, \
         content_string_vec = $5, content_attachment = $6, content_number = $7, \
         content_bool = $8 WHERE id = $9 RETURNING {}",
        ALL
    ))
    .bind(7i32)
    .bind(3i32)
    .bind("Notes")
    .bind(Some("typed"))
    .bind(Option::<Vec<String>>::None)
    .bind(Option::<Vec<u8>>::None)
    .bind(Option::<i32>::None)
    .bind(Option::<bool>::None)
    .bind(1i32)
    .fetch_one(&pool)
    .await
    .expect("update returning");

    assert_eq!(row.get::<Option<Vec<String>>, _>("content_string_vec"), None);
    assert_eq!(row.get::<Option<Vec<u8>>, _>("content_attachment"), None);
}

/// The same, inside an explicit transaction — sea-orm runs it on a
/// `DatabaseTransaction`.
#[tokio::test]
async fn update_returning_inside_a_transaction() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    let mut tx = pool.begin().await.expect("begin");
    let row = sqlx::query(&format!(
        "UPDATE field SET case_id = $1, step_id = $2, label = $3, content_string = $4, \
         content_string_vec = $5, content_attachment = $6, content_number = $7, \
         content_bool = $8 WHERE id = $9 RETURNING {}",
        ALL
    ))
    .bind(7i32)
    .bind(3i32)
    .bind("Notes")
    .bind(Some("typed in a block"))
    .bind(Option::<Vec<String>>::None)
    .bind(Option::<Vec<u8>>::None)
    .bind(Option::<i32>::None)
    .bind(Option::<bool>::None)
    .bind(1i32)
    .fetch_one(&mut *tx)
    .await
    .expect("update returning in a transaction");

    assert_eq!(row.get::<Option<Vec<String>>, _>("content_string_vec"), None);
    tx.commit().await.expect("commit");
}

/// Setting the array to NULL explicitly, which is the assignment that has to
/// travel through the RETURNING overlay.
#[tokio::test]
async fn setting_the_array_to_null_returns_null() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    // Give it a value first, so the update is a real change to NULL.
    sqlx::query("UPDATE field SET content_string_vec = $1 WHERE id = $2")
        .bind(vec!["a".to_string()])
        .bind(1i32)
        .execute(&pool)
        .await
        .expect("populate");

    let row = sqlx::query(
        "UPDATE field SET content_string_vec = $1 WHERE id = $2 RETURNING content_string_vec",
    )
    .bind(Option::<Vec<String>>::None)
    .bind(1i32)
    .fetch_one(&pool)
    .await
    .expect("update to null returning");

    assert_eq!(row.get::<Option<Vec<String>>, _>("content_string_vec"), None);
}

/// `DELETE … RETURNING` shares the read path, and is also used.
#[tokio::test]
async fn delete_returning_reports_the_null_array() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    let row = sqlx::query(&format!("DELETE FROM field WHERE id = $1 RETURNING {}", ALL))
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("delete returning");
    assert_eq!(row.get::<Option<Vec<String>>, _>("content_string_vec"), None);
}
