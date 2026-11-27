//! A quoted name holding dots (`"d.e.colors"`, as SQL Server's
//! `[DenevCloud.ERP.Colors]` arrives) is one relation: its dots are no
//! qualifier. INSERT failed "Table not found", a SELECT read a relation
//! named `colors`, UPDATE and DELETE answered 0 rows and changed nothing,
//! and DROP TABLE went for `colors`. Also: a join over a derived table
//! described a `timestamp` column as `timestamptz`.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Column, Row, TypeInfo};

async fn exec(pool: &sqlx::PgPool, sql: &str) -> u64 {
    sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .rows_affected()
}

async fn ids(pool: &sqlx::PgPool, sql: &str) -> Vec<i32> {
    sqlx::query_scalar(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_quoted_dotted_name_is_one_relation() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    // A table named by the last segment: what the dotted name was misread as.
    exec(&pool, "CREATE TABLE colors (id int PRIMARY KEY)").await;
    exec(&pool, "INSERT INTO colors VALUES (100)").await;
    exec(&pool, r#"CREATE TABLE "d.e.colors" (id int PRIMARY KEY, name text, n int)"#).await;
    assert_eq!(exec(&pool, r#"INSERT INTO "d.e.colors" (id, name, n) VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3)"#).await, 3);
    assert_eq!(ids(&pool, r#"SELECT d.id FROM "d.e.colors" AS d WHERE d.n > 1 ORDER BY d.id"#).await, vec![2, 3]);
    assert_eq!(exec(&pool, r#"UPDATE "d.e.colors" SET name = 'b2' WHERE id = 2"#).await, 1);
    assert_eq!(exec(&pool, r#"UPDATE "d.e.colors" AS d SET n = 20 WHERE d.id = 2"#).await, 1);
    assert_eq!(exec(&pool, r#"DELETE FROM "d.e.colors" AS d WHERE d.id = 3"#).await, 1);
    assert_eq!(ids(&pool, r#"SELECT n FROM "d.e.colors" ORDER BY id"#).await, vec![1, 20]);
    assert_eq!(ids(&pool, r#"SELECT x.id FROM "d.e.colors" x JOIN "d.e.colors" y ON x.id = y.id ORDER BY 1"#).await, vec![1, 2]);
    exec(&pool, r#"ALTER TABLE "d.e.colors" ADD COLUMN z int"#).await;
    exec(&pool, r#"CREATE INDEX "ix_d.e.colors_n" ON "d.e.colors" (n)"#).await;
    exec(&pool, r#"DROP TABLE "d.e.colors""#).await;
    // The table the dots were misread as is untouched.
    assert_eq!(ids(&pool, "SELECT id FROM colors").await, vec![100]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_derived_tables_timestamp_stays_one_through_a_join() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE blogs (id int PRIMARY KEY, created timestamp, url text)").await;
    exec(&pool, "CREATE TABLE posts (id int PRIMARY KEY, blogid int)").await;
    exec(&pool, "INSERT INTO blogs VALUES (1, '2024-01-02 03:04:05', 'a')").await;
    exec(&pool, "INSERT INTO posts VALUES (1, 1)").await;
    let row = sqlx::query(
        "SELECT x.created FROM (SELECT b.id, b.created FROM blogs AS b ORDER BY b.url LIMIT 2) AS x \
         LEFT JOIN posts AS p ON x.id = p.blogid",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.columns()[0].type_info().name(), "TIMESTAMP");
}
