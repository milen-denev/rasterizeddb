//! Four answers a plain table gave wrongly, found by rasterizeddb-eb's
//! partitioning twin (the twin matched, so they are not partitioning's).
//! Expected answers are PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Column, Executor, Row};

async fn setup() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE src (id INT NOT NULL, doc JSONB, amt NUMERIC(10,2));
         CREATE TABLE dst (id INT NOT NULL, doc JSONB, amt NUMERIC(10,2));
         INSERT INTO src VALUES (1, '{\"a\": 1}', 7.25), (2, '[1, 2]', 0.50);",
    ))
    .await
    .expect("schema");
    (server, pool)
}

/// `INSERT … SELECT` copies a JSONB column.
#[tokio::test(flavor = "multi_thread")]
async fn insert_select_copies_a_jsonb_column() {
    let (_server, pool) = setup().await;
    pool.execute("INSERT INTO dst (id, doc) SELECT id, doc FROM src").await.expect("copy");
    let docs: Vec<String> = sqlx::query("SELECT doc::text FROM dst ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("read")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(docs, vec![r#"{"a": 1}"#.to_string(), "[1, 2]".to_string()]);
    // And through `SELECT *`, whose types come from the source table.
    pool.execute("CREATE TABLE dst2 (id INT NOT NULL, doc JSONB, amt NUMERIC(10,2))").await.expect("dst2");
    pool.execute("INSERT INTO dst2 SELECT * FROM src").await.expect("copy *");
    let docs: Vec<String> = sqlx::query("SELECT doc::text FROM dst2 ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("read *")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(docs, vec![r#"{"a": 1}"#.to_string(), "[1, 2]".to_string()]);
}

/// A RETURNING that returns no rows still describes its columns.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_insert_select_returning_still_describes_its_columns() {
    let (_server, pool) = setup().await;
    // Simple protocol: zero rows, no error.
    let rows = pool
        .fetch_all(sqlx::raw_sql("INSERT INTO dst (id) SELECT id FROM src WHERE id > 100 RETURNING id"))
        .await
        .expect("simple");
    assert!(rows.is_empty());
    // Extended protocol: sqlx describes before executing; zero rows, no error.
    let got = sqlx::query("INSERT INTO dst (id) SELECT id FROM src WHERE id > $1 RETURNING id")
        .bind(100)
        .fetch_all(&pool)
        .await
        .expect("extended");
    assert!(got.is_empty());
}

/// A computed NUMERIC in RETURNING has its operand's scale.
#[tokio::test(flavor = "multi_thread")]
async fn a_computed_numeric_in_returning_keeps_its_scale() {
    let (_server, pool) = setup().await;
    let row = sqlx::query("UPDATE src SET id = id WHERE id = 1 RETURNING (amt + 1)::text")
        .fetch_one(&pool)
        .await
        .expect("update returning");
    assert_eq!(row.get::<String, _>(0), "8.25");
    let row = sqlx::query("INSERT INTO dst (id, amt) VALUES (9, 1.5) RETURNING (amt * 2)::text")
        .fetch_one(&pool)
        .await
        .expect("insert returning");
    assert_eq!(row.get::<String, _>(0), "3.00");
    // Uncast, as a driver decodes it: the scale is the operand's, not 9.
    for (sql, want) in [
        ("UPDATE src SET id = id WHERE id = 1 RETURNING amt + 1", "8.25"),
        ("INSERT INTO dst (id, amt) VALUES (10, 1.5) RETURNING amt * 2", "3.00"),
        ("DELETE FROM src WHERE id = 2 RETURNING amt - 0.25", "0.25"),
    ] {
        let value: rust_decimal::Decimal = sqlx::query(sql).fetch_one(&pool).await.expect(sql).get(0);
        assert_eq!(value.to_string(), want, "{sql}");
    }
}

/// `INSERT … RETURNING col AS alias` returns the column's value under the alias.
#[tokio::test(flavor = "multi_thread")]
async fn insert_returning_an_aliased_column() {
    let (_server, pool) = setup().await;
    let row = sqlx::query(AssertSqlSafe("INSERT INTO dst (id, amt) VALUES (5, 2.75) RETURNING id AS k, amt AS v"))
        .fetch_one(&pool)
        .await
        .expect("insert returning alias");
    let names: Vec<&str> = row.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, ["k", "v"]);
    assert_eq!(row.get::<i32, _>("k"), 5);
    assert!(row.try_get::<Option<rust_decimal::Decimal>, _>("v").unwrap().is_some(), "the aliased value came back NULL");
}
