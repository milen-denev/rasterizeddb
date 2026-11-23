//! `ORDER BY` over grouped output puts NULLs where PostgreSQL does, and an
//! aggregate over nothing but NULLs *is* NULL.
//!
//! The fold sorts its finished output rows. A NULL group key leaves it as an
//! empty cell typed NULL, so a comparator reading bytes put it before every
//! value under `ASC` — and tied it with `''`, which is also empty. PostgreSQL
//! sorts NULL as larger than every value: last under `ASC`, first under `DESC`,
//! unless `NULLS FIRST` / `NULLS LAST` says otherwise.
//!
//! And `MAX`/`SUM`/`AVG` of a group whose values are all NULL is NULL in
//! PostgreSQL, not `0` — which also decides where that group sorts.
//!
//! Every expectation is the exact sequence PostgreSQL returns for this data.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn setup(pool: &sqlx::PgPool) {
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, g INT, s TEXT, v INT)",
        // g = 0 / s = '' are real values next to the NULL group.
        "INSERT INTO t (id, g, s, v) VALUES (1, 0, '', 5)",
        "INSERT INTO t (id, g, s, v) VALUES (2, 0, '', NULL)",
        "INSERT INTO t (id, g, s, v) VALUES (3, NULL, NULL, 3)",
        "INSERT INTO t (id, g, s, v) VALUES (4, NULL, NULL, NULL)",
        // g = 2: every v is NULL, so MAX(v) and SUM(v) are NULL.
        "INSERT INTO t (id, g, s, v) VALUES (5, 2, 'b', NULL)",
        "INSERT INTO t (id, g, s, v) VALUES (6, 2, 'b', NULL)",
        "INSERT INTO t (id, g, s, v) VALUES (7, -1, 'a', 9)",
        // For the grouped join: l.g groups, r.v is aggregated, and l.3 has no
        // partner so its group's MAX is over an unmatched (NULL) side.
        "CREATE TABLE l (id INT PRIMARY KEY, g INT)",
        "CREATE TABLE r (id INT PRIMARY KEY, lid INT, v INT)",
        "INSERT INTO l (id, g) VALUES (1, 0)",
        "INSERT INTO l (id, g) VALUES (2, NULL)",
        "INSERT INTO l (id, g) VALUES (3, 7)",
        "INSERT INTO l (id, g) VALUES (4, 0)",
        "INSERT INTO r (id, lid, v) VALUES (10, 1, 4)",
        "INSERT INTO r (id, lid, v) VALUES (11, 2, 8)",
        "INSERT INTO r (id, lid, v) VALUES (12, 4, NULL)",
    ] {
        pool.execute(sqlx::raw_sql(sql))
            .await
            .unwrap_or_else(|e| panic!("{}: {}", sql, e));
    }
}

async fn ints(pool: &sqlx::PgPool, sql: &'static str) -> Vec<(Option<i32>, Option<i32>)> {
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

async fn key_and_count(pool: &sqlx::PgPool, sql: &'static str) -> Vec<(Option<i32>, i64)> {
    sqlx::query(sql)
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{}: {}", sql, e))
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

#[tokio::test]
async fn a_null_group_key_sorts_after_zero() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    assert_eq!(
        key_and_count(&pool, "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY g").await,
        vec![(Some(-1), 1), (Some(0), 2), (Some(2), 2), (None, 2)]
    );
    assert_eq!(
        key_and_count(&pool, "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY g DESC").await,
        vec![(None, 2), (Some(2), 2), (Some(0), 2), (Some(-1), 1)]
    );
    assert_eq!(
        key_and_count(&pool, "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY g DESC LIMIT 2").await,
        vec![(None, 2), (Some(2), 2)]
    );
}

#[tokio::test]
async fn a_null_text_group_is_not_the_empty_string() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    let text = |sql: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query(sql)
                .fetch_all(&pool)
                .await
                .unwrap_or_else(|e| panic!("{}: {}", sql, e))
                .iter()
                .map(|row| (row.get::<Option<String>, _>(0), row.get::<i64, _>(1)))
                .collect::<Vec<_>>()
        }
    };
    let s = |v: &str| Some(v.to_string());
    assert_eq!(
        text("SELECT s, COUNT(*) FROM t GROUP BY s ORDER BY s").await,
        vec![(s(""), 2), (s("a"), 1), (s("b"), 2), (None, 2)]
    );
    assert_eq!(
        text("SELECT s, COUNT(*) FROM t GROUP BY s ORDER BY s DESC").await,
        vec![(None, 2), (s("b"), 2), (s("a"), 1), (s(""), 2)]
    );
}

/// `MAX`/`SUM` over a group of NULLs is NULL — and sorts as one.
#[tokio::test]
async fn an_all_null_aggregate_is_null_and_sorts_as_null() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    assert_eq!(
        ints(&pool, "SELECT g, MAX(v) AS m FROM t GROUP BY g ORDER BY m, g").await,
        vec![(None, Some(3)), (Some(0), Some(5)), (Some(-1), Some(9)), (Some(2), None)]
    );
    assert_eq!(
        ints(&pool, "SELECT g, MAX(v) AS m FROM t GROUP BY g ORDER BY m DESC, g").await,
        vec![(Some(2), None), (Some(-1), Some(9)), (Some(0), Some(5)), (None, Some(3))]
    );
    let rows = sqlx::query("SELECT g, SUM(v) AS total FROM t GROUP BY g ORDER BY total, g")
        .fetch_all(&pool)
        .await
        .expect("sum");
    let got: Vec<(Option<i32>, Option<i64>)> =
        rows.iter().map(|row| (row.get(0), row.get(1))).collect();
    assert_eq!(
        got,
        vec![(None, Some(3)), (Some(0), Some(5)), (Some(-1), Some(9)), (Some(2), None)]
    );
}

#[tokio::test]
async fn explicit_nulls_first_and_last_on_grouped_output() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    assert_eq!(
        key_and_count(&pool, "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY g NULLS FIRST").await,
        vec![(None, 2), (Some(-1), 1), (Some(0), 2), (Some(2), 2)]
    );
    assert_eq!(
        key_and_count(&pool, "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY g DESC NULLS LAST")
            .await,
        vec![(Some(2), 2), (Some(0), 2), (Some(-1), 1), (None, 2)]
    );
    assert_eq!(
        ints(&pool, "SELECT g, MAX(v) AS m FROM t GROUP BY g ORDER BY m NULLS FIRST, g").await,
        vec![(Some(2), None), (None, Some(3)), (Some(0), Some(5)), (Some(-1), Some(9))]
    );
}

#[tokio::test]
async fn grouped_join_orders_null_keys_and_null_aggregates() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    // Groups: g=0 -> r.v {4, NULL} max 4; g=NULL -> {8}; g=7 -> unmatched,
    // MAX over the NULL side is NULL.
    assert_eq!(
        ints(
            &pool,
            "SELECT l.g, MAX(r.v) AS m FROM l LEFT JOIN r ON r.lid = l.id GROUP BY l.g ORDER BY l.g"
        )
        .await,
        vec![(Some(0), Some(4)), (Some(7), None), (None, Some(8))]
    );
    assert_eq!(
        ints(
            &pool,
            "SELECT l.g, MAX(r.v) AS m FROM l LEFT JOIN r ON r.lid = l.id GROUP BY l.g \
             ORDER BY l.g DESC"
        )
        .await,
        vec![(None, Some(8)), (Some(7), None), (Some(0), Some(4))]
    );
    assert_eq!(
        ints(
            &pool,
            "SELECT l.g, MAX(r.v) AS m FROM l LEFT JOIN r ON r.lid = l.id GROUP BY l.g ORDER BY m"
        )
        .await,
        vec![(Some(0), Some(4)), (None, Some(8)), (Some(7), None)]
    );
    assert_eq!(
        ints(
            &pool,
            "SELECT l.g, MAX(r.v) AS m FROM l LEFT JOIN r ON r.lid = l.id GROUP BY l.g \
             ORDER BY m DESC NULLS LAST"
        )
        .await,
        vec![(None, Some(8)), (Some(0), Some(4)), (Some(7), None)]
    );
}
