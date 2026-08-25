//! A NULL must leave a join as a NULL.
//!
//! The projected row was built with each column's *declared* type whatever the
//! value, and a NULL here is recorded by a companion column rather than by the
//! bytes — a NULL fixed-width column holds its type's zeroes. So a NULL integer
//! came out of a join as `0` and a NULL text as `''`.
//!
//! `LEFT JOIN` is the case that matters most: the whole unmatched side arrived
//! as zeroes, making "no matching row" indistinguishable from "a matching row
//! full of defaults".

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn setup(pool: &sqlx::PgPool) {
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)",
    ))
    .await
    .expect("t");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE s (id INT PRIMARY KEY, t_id INT NOT NULL, n INT, txt TEXT, flag BOOLEAN, ts TIMESTAMP)",
    ))
    .await
    .expect("s");
    pool.execute(sqlx::raw_sql("INSERT INTO t (id, name) VALUES (1, 'has-match')"))
        .await
        .expect("t1");
    pool.execute(sqlx::raw_sql("INSERT INTO t (id, name) VALUES (2, 'no-match')"))
        .await
        .expect("t2");
    // Row for t=1 only, and its nullable columns are all NULL.
    pool.execute(sqlx::raw_sql("INSERT INTO s (id, t_id) VALUES (1, 1)"))
        .await
        .expect("s1");
}

/// A matched row whose nullable columns are NULL.
#[tokio::test]
async fn nulls_in_a_matched_row_stay_null() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    let row = sqlx::query(
        "SELECT t.id AS id, s.n AS n, s.txt AS txt, s.flag AS flag \
         FROM t JOIN s ON s.t_id = t.id WHERE t.id = 1",
    )
    .fetch_one(&pool)
    .await
    .expect("inner join select");

    assert_eq!(row.get::<i32, _>("id"), 1);
    assert_eq!(row.get::<Option<i32>, _>("n"), None, "a NULL int must not become 0");
    assert_eq!(row.get::<Option<String>, _>("txt"), None, "a NULL text must not become ''");
    assert_eq!(row.get::<Option<bool>, _>("flag"), None, "a NULL bool must not become false");
}

/// The unmatched side of a LEFT JOIN is NULL, not a row of defaults.
#[tokio::test]
async fn an_unmatched_left_join_side_is_null() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    let rows = sqlx::query(
        "SELECT t.id AS id, s.id AS sid, s.n AS n, s.txt AS txt \
         FROM t LEFT JOIN s ON s.t_id = t.id ORDER BY t.id",
    )
    .fetch_all(&pool)
    .await
    .expect("left join select");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i32, _>("id"), 1);
    assert_eq!(rows[0].get::<Option<i32>, _>("sid"), Some(1), "row 1 matched");

    // Row 2 matched nothing at all.
    assert_eq!(rows[1].get::<i32, _>("id"), 2);
    assert_eq!(
        rows[1].get::<Option<i32>, _>("sid"),
        None,
        "an unmatched LEFT JOIN side must be NULL, not 0"
    );
    assert_eq!(rows[1].get::<Option<i32>, _>("n"), None);
    assert_eq!(rows[1].get::<Option<String>, _>("txt"), None);
}
