//! The case-step autosave: an **upsert** returning every column.
//!
//! `autosave_case_fields` writes `field` with `INSERT … ON CONFLICT … DO
//! UPDATE … RETURNING <every column>`, and sea-orm quotes every identifier.
//! Both halves matter: the conflict branch builds its returned row on a
//! different path from a plain insert, and a nullable `TEXT[]` returned as a
//! zero-length payload under a real array type panics sqlx's decoder — five
//! `i32` headers are read before any element — rather than erroring.

use sqlx::AssertSqlSafe;
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

const RETURNING: &str = "\"id\", \"case_id\", \"step_id\", \"label\", \"content_string\", \
                         \"content_string_vec\", \"content_attachment\", \"content_number\", \
                         \"content_bool\"";

fn decode(row: &sqlx::postgres::PgRow, path: &str) {
    let _ = row.get::<i32, _>("id");
    let _ = row.get::<Option<String>, _>("content_string");
    let _ = row.get::<Option<Vec<String>>, _>("content_string_vec");
    let _ = row.get::<Option<Vec<u8>>, _>("content_attachment");
    let _ = row.get::<Option<i32>, _>("content_number");
    let _ = row.get::<Option<bool>, _>("content_bool");
    println!("decoded via {}", path);
}

/// First autosave: the row does not exist, so the insert branch runs.
#[tokio::test]
async fn upsert_insert_branch_returning_nulls() {
    let (_s, pool) = server().await;
    create(&pool).await;

    let row = sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO \"field\" (\"id\", \"case_id\", \"step_id\", \"label\", \"content_string\") \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (\"id\") DO UPDATE SET \"content_string\" = $5 \
         RETURNING {}",
        RETURNING
    )))
    .bind(1i32)
    .bind(7i32)
    .bind(3i32)
    .bind("Notes")
    .bind("first")
    .fetch_one(&pool)
    .await
    .expect("upsert insert branch");
    decode(&row, "upsert (insert branch)");
    assert_eq!(row.get::<Option<Vec<String>>, _>("content_string_vec"), None);
}

/// Second autosave on the same field: the conflict branch runs. This is the
/// keystroke-by-keystroke case — the one a user actually triggers by typing.
#[tokio::test]
async fn upsert_conflict_branch_returning_nulls() {
    let (_s, pool) = server().await;
    create(&pool).await;

    for (n, text) in [(1, "t"), (2, "ty"), (3, "typ"), (4, "type")] {
        let row = sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO \"field\" (\"id\", \"case_id\", \"step_id\", \"label\", \"content_string\") \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (\"id\") DO UPDATE SET \"content_string\" = $5 \
             RETURNING {}",
            RETURNING
        )))
        .bind(1i32)
        .bind(7i32)
        .bind(3i32)
        .bind("Notes")
        .bind(text)
        .fetch_one(&pool)
        .await
        .unwrap_or_else(|e| panic!("autosave #{} failed: {}", n, e));

        decode(&row, &format!("upsert #{} (conflict branch)", n));
        assert_eq!(
            row.get::<Option<String>, _>("content_string"),
            Some(text.to_string()),
            "autosave #{} must return what it stored",
            n
        );
        assert_eq!(
            row.get::<Option<Vec<String>>, _>("content_string_vec"),
            None,
            "autosave #{}: a NULL array must come back NULL",
            n
        );
    }
}

/// DO NOTHING, the other conflict form seen in the production log.
#[tokio::test]
async fn upsert_do_nothing_returning() {
    let (_s, pool) = server().await;
    create(&pool).await;

    sqlx::query("INSERT INTO \"field\" (\"id\", \"case_id\", \"step_id\", \"label\") VALUES ($1, $2, $3, $4)")
        .bind(1i32)
        .bind(7i32)
        .bind(3i32)
        .bind("Notes")
        .execute(&pool)
        .await
        .expect("seed");

    let rows = sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO \"field\" (\"id\", \"case_id\", \"step_id\", \"label\") \
         VALUES ($1, $2, $3, $4) ON CONFLICT (\"id\") DO NOTHING RETURNING {}",
        RETURNING
    )))
    .bind(1i32)
    .bind(7i32)
    .bind(3i32)
    .bind("Notes")
    .fetch_all(&pool)
    .await
    .expect("do nothing returning");

    for row in &rows {
        decode(row, "upsert DO NOTHING");
    }
}
