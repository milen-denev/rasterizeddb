//! `ORDER BY` over a join puts NULLs where PostgreSQL does.
//!
//! PostgreSQL sorts NULL as larger than every value: last under `ASC`, first
//! under `DESC`, unless `NULLS FIRST` / `NULLS LAST` says otherwise. The join's
//! plain-column sort used to decide NULL-ness from the *bytes*: an unmatched
//! outer side (no bytes at all) sorted first under `ASC`, and a matched row's
//! own NULL — stored as its type's zeroes, typed NULL only by its companion —
//! sorted as the value `0` (or `''`), tied with a real zero.
//!
//! Every expectation here is the exact row sequence PostgreSQL returns, so the
//! sort is checked server-side, including where `LIMIT`/`OFFSET` cut it.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn setup(pool: &sqlx::PgPool) {
    for sql in [
        "CREATE TABLE l (id INT PRIMARY KEY, k INT, name TEXT, v INT)",
        "CREATE TABLE r (id INT PRIMARY KEY, k INT, n INT)",
        "INSERT INTO l (id, k, name, v) VALUES (1, 1, 'a', 0)",
        "INSERT INTO l (id, k, name, v) VALUES (2, 2, 'b', NULL)",
        "INSERT INTO l (id, k, name, v) VALUES (3, NULL, 'c', 5)",
        "INSERT INTO l (id, k, name, v) VALUES (4, 4, NULL, -1)",
        "INSERT INTO r (id, k, n) VALUES (10, 1, 0)",
        "INSERT INTO r (id, k, n) VALUES (11, 2, NULL)",
        "INSERT INTO r (id, k, n) VALUES (12, 3, 7)",
        "INSERT INTO r (id, k, n) VALUES (13, 1, 2)",
    ] {
        pool.execute(sqlx::raw_sql(sql))
            .await
            .unwrap_or_else(|e| panic!("{}: {}", sql, e));
    }
}

type Pair = (Option<i32>, Option<i32>);

async fn pairs(pool: &sqlx::PgPool, sql: &'static str) -> Vec<Pair> {
    sqlx::query(sql)
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{}: {}", sql, e))
        .iter()
        .map(|row| {
            (
                row.try_get::<Option<i32>, _>(0).expect("first"),
                row.try_get::<Option<i32>, _>(1).expect("second"),
            )
        })
        .collect()
}

/// `r.n` is NULL both for a matched row (r.11) and for unmatched left rows.
#[tokio::test]
async fn left_join_nulls_sort_last_ascending_and_first_descending() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    assert_eq!(
        pairs(&pool, "SELECT l.id, r.n FROM l LEFT JOIN r ON r.k = l.k ORDER BY r.n, l.id").await,
        vec![
            (Some(1), Some(0)),
            (Some(1), Some(2)),
            (Some(2), None),
            (Some(3), None),
            (Some(4), None),
        ]
    );
    assert_eq!(
        pairs(&pool, "SELECT l.id, r.n FROM l LEFT JOIN r ON r.k = l.k ORDER BY r.n DESC, l.id")
            .await,
        vec![
            (Some(2), None),
            (Some(3), None),
            (Some(4), None),
            (Some(1), Some(2)),
            (Some(1), Some(0)),
        ]
    );
}

#[tokio::test]
async fn right_join_nulls_on_the_left_side_with_limit() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    assert_eq!(
        pairs(&pool, "SELECT l.id, r.id FROM l RIGHT JOIN r ON r.k = l.k ORDER BY l.id, r.id")
            .await,
        vec![
            (Some(1), Some(10)),
            (Some(1), Some(13)),
            (Some(2), Some(11)),
            (None, Some(12)),
        ]
    );
    assert_eq!(
        pairs(
            &pool,
            "SELECT l.id, r.id FROM l RIGHT JOIN r ON r.k = l.k ORDER BY l.id DESC, r.id LIMIT 2"
        )
        .await,
        vec![(None, Some(12)), (Some(2), Some(11))]
    );
}

#[tokio::test]
async fn full_join_nulls_on_both_sides_with_offset() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    assert_eq!(
        pairs(&pool, "SELECT l.id, r.id FROM l FULL JOIN r ON r.k = l.k ORDER BY r.id DESC, l.id")
            .await,
        vec![
            (Some(3), None),
            (Some(4), None),
            (Some(1), Some(13)),
            (None, Some(12)),
            (Some(2), Some(11)),
            (Some(1), Some(10)),
        ]
    );
    assert_eq!(
        pairs(
            &pool,
            "SELECT l.id, r.id FROM l FULL JOIN r ON r.k = l.k ORDER BY r.id DESC, l.id \
             LIMIT 3 OFFSET 1"
        )
        .await,
        vec![(Some(4), None), (Some(1), Some(13)), (None, Some(12))]
    );
}

/// A matched row's own NULL is not the value 0, and a real 0 is not NULL.
#[tokio::test]
async fn a_matched_null_is_not_a_zero() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    // l.1 has v = 0 (twice: it matches r.10 and r.13); l.2 has v NULL.
    assert_eq!(
        pairs(&pool, "SELECT l.id, l.v FROM l JOIN r ON r.k = l.k ORDER BY l.v DESC, l.id").await,
        vec![(Some(2), None), (Some(1), Some(0)), (Some(1), Some(0))]
    );
    assert_eq!(
        pairs(&pool, "SELECT l.id, l.v FROM l JOIN r ON r.k = l.k ORDER BY l.v, l.id").await,
        vec![(Some(1), Some(0)), (Some(1), Some(0)), (Some(2), None)]
    );

    // Text: a NULL name is not ''.
    let rows = sqlx::query(
        "SELECT l.name, r.id FROM l LEFT JOIN r ON r.k = l.k ORDER BY l.name, r.id",
    )
    .fetch_all(&pool)
    .await
    .expect("text order");
    let got: Vec<(Option<String>, Option<i32>)> =
        rows.iter().map(|row| (row.get(0), row.get(1))).collect();
    let s = |v: &str| Some(v.to_string());
    assert_eq!(
        got,
        vec![
            (s("a"), Some(10)),
            (s("a"), Some(13)),
            (s("b"), Some(11)),
            (s("c"), None),
            (None, None),
        ]
    );
}

/// An explicit placement overrides the default in both directions.
#[tokio::test]
async fn explicit_nulls_first_and_last() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    assert_eq!(
        pairs(
            &pool,
            "SELECT l.id, r.n FROM l LEFT JOIN r ON r.k = l.k ORDER BY r.n NULLS FIRST, l.id"
        )
        .await,
        vec![
            (Some(2), None),
            (Some(3), None),
            (Some(4), None),
            (Some(1), Some(0)),
            (Some(1), Some(2)),
        ]
    );
    assert_eq!(
        pairs(
            &pool,
            "SELECT l.id, r.id FROM l RIGHT JOIN r ON r.k = l.k ORDER BY l.id DESC NULLS LAST, r.id"
        )
        .await,
        vec![
            (Some(2), Some(11)),
            (Some(1), Some(10)),
            (Some(1), Some(13)),
            (None, Some(12)),
        ]
    );
}
