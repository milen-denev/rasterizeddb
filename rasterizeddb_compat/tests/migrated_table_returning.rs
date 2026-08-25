//! A table that grew the way a real one does: `ALTER TABLE … ADD COLUMN`,
//! and a dropped column, then read back through `RETURNING`.
//!
//! `field` was not created by one `CREATE TABLE`; migrations added columns
//! over time. A column's `write_order` is its identity in a stored row, so a
//! table that has been altered is a different physical shape from the same
//! columns declared at once — and a value landing in the wrong slot, or a
//! slot with no value, is how a zero-length payload reaches a client under a
//! fixed-width type.

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

fn decode(row: &sqlx::postgres::PgRow, path: &str) {
    let _ = row.get::<i32, _>("id");
    let _ = row.get::<Option<String>, _>("content_string");
    let _ = row.get::<Option<Vec<String>>, _>("content_string_vec");
    let _ = row.get::<Option<Vec<u8>>, _>("content_attachment");
    let _ = row.get::<Option<i32>, _>("content_number");
    let _ = row.get::<Option<bool>, _>("content_bool");
    println!("decoded via {}", path);
}

const RET: &str = "\"id\", \"content_string\", \"content_string_vec\", \"content_attachment\", \
                   \"content_number\", \"content_bool\"";

#[tokio::test]
async fn columns_added_by_migration_then_returned() {
    let (_s, pool) = server().await;

    // The original table.
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE field (id INT PRIMARY KEY, case_id INT NOT NULL, label TEXT NOT NULL)",
    ))
    .await
    .expect("create");

    // Rows exist before the migration, as they would in production.
    pool.execute(sqlx::raw_sql(
        "INSERT INTO field (id, case_id, label) VALUES (1, 7, 'Notes')",
    ))
    .await
    .expect("seed pre-migration");

    // Migrations add the content columns one at a time.
    for stmt in [
        "ALTER TABLE field ADD COLUMN content_string TEXT",
        "ALTER TABLE field ADD COLUMN content_string_vec TEXT[]",
        "ALTER TABLE field ADD COLUMN content_attachment BYTEA",
        "ALTER TABLE field ADD COLUMN content_number INT",
        "ALTER TABLE field ADD COLUMN content_bool BOOLEAN",
    ] {
        pool.execute(sqlx::raw_sql(stmt))
            .await
            .unwrap_or_else(|e| panic!("{}: {}", stmt, e));
    }

    // The pre-migration row, read through RETURNING on an update.
    let row = sqlx::query(AssertSqlSafe(format!(
        "UPDATE \"field\" SET \"content_string\" = $1 WHERE \"id\" = $2 RETURNING {}",
        RET
    )))
    .bind("typed")
    .bind(1i32)
    .fetch_one(&pool)
    .await
    .expect("update returning on a pre-migration row");
    decode(&row, "UPDATE RETURNING, row predates the columns");
    assert_eq!(row.get::<Option<Vec<String>>, _>("content_string_vec"), None);

    // A row inserted after the migration.
    let row = sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO \"field\" (\"id\", \"case_id\", \"label\") VALUES ($1, $2, $3) RETURNING {}",
        RET
    )))
    .bind(2i32)
    .bind(7i32)
    .bind("Notes")
    .fetch_one(&pool)
    .await
    .expect("insert returning after migration");
    decode(&row, "INSERT RETURNING, row postdates the columns");

    // And a plain read of both.
    for id in [1i32, 2] {
        let row = sqlx::query(AssertSqlSafe(format!("SELECT {} FROM \"field\" WHERE \"id\" = $1", RET)))
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|e| panic!("select id={}: {}", id, e));
        decode(&row, &format!("SELECT id={}", id));
    }
}

/// A dropped column shifts what follows it.
#[tokio::test]
async fn a_dropped_column_then_returning() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE field2 (id INT PRIMARY KEY, doomed TEXT, content_string_vec TEXT[], content_number INT)",
    ))
    .await
    .expect("create");
    pool.execute(sqlx::raw_sql("INSERT INTO field2 (id, doomed) VALUES (1, 'x')"))
        .await
        .expect("seed");
    pool.execute(sqlx::raw_sql("ALTER TABLE field2 DROP COLUMN doomed"))
        .await
        .expect("drop");

    let row = sqlx::query(
        "UPDATE \"field2\" SET \"content_number\" = $1 WHERE \"id\" = $2 \
         RETURNING \"id\", \"content_string_vec\", \"content_number\"",
    )
    .bind(5i32)
    .bind(1i32)
    .fetch_one(&pool)
    .await
    .expect("update returning after a drop");

    assert_eq!(row.get::<i32, _>("id"), 1);
    assert_eq!(row.get::<Option<Vec<String>>, _>("content_string_vec"), None);
    assert_eq!(row.get::<Option<i32>, _>("content_number"), Some(5));
}
