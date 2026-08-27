//! Window functions and interval arithmetic.
//!
//! A window function keeps every row and adds a column computed from the rows
//! around it. What it sees is decided by the frame, and the frame is where the
//! meaning lives: `SUM(x) OVER (ORDER BY t)` is a running total while
//! `SUM(x) OVER ()` is the partition total, and both are spelled almost alike.
//! The values are asserted, not just the absence of an error, because a frame
//! read the wrong way round returns a number rather than a failure.

use rasterizeddb_compat::TestServer;
use rust_decimal::Decimal;
use sqlx::{AssertSqlSafe, Column, Row, TypeInfo};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

/// Six rows over two tenants, with an order that ties in one of them.
async fn sales(pool: &sqlx::PgPool) {
    sqlx::query(AssertSqlSafe(
        "CREATE TABLE sales (
            id INTEGER NOT NULL,
            tenant INTEGER NOT NULL,
            at TIMESTAMPTZ NOT NULL,
            amount NUMERIC(12,2) NOT NULL
        )",
    ))
    .execute(pool)
    .await
    .expect("create");

    for (id, tenant, at, amount) in [
        (1, 1, "2026-01-01 00:00:00+00", "10"),
        (2, 1, "2026-01-02 00:00:00+00", "20"),
        (3, 1, "2026-01-03 00:00:00+00", "30"),
        (4, 2, "2026-01-01 00:00:00+00", "100"),
        (5, 2, "2026-01-02 00:00:00+00", "100"),
        (6, 2, "2026-01-03 00:00:00+00", "300"),
    ] {
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO sales (id, tenant, at, amount) VALUES ({}, {}, '{}', {})",
            id, tenant, at, amount
        )))
        .execute(pool)
        .await
        .expect("insert");
    }
}

/// `(id, window value as text)` for a select of `id` and one window item.
async fn windowed(pool: &sqlx::PgPool, item: &str) -> Vec<(i32, String)> {
    let sql = format!("SELECT id, {} AS w FROM sales ORDER BY id", item);
    // The window column is read through the row's own type, so a wrong result
    // type shows up here rather than being papered over by a text cast.
    sqlx::query(AssertSqlSafe(sql.clone()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e.to_string().lines().next().unwrap_or("")))
        .iter()
        .map(|r| {
            let id: i32 = r.get(0);
            let rendered = r
                .try_get::<Option<i64>, _>(1)
                .map(|v| v.map(|v| v.to_string()).unwrap_or_default())
                .or_else(|_| {
                    r.try_get::<Option<Decimal>, _>(1)
                        .map(|v| v.map(|v| v.normalize().to_string()).unwrap_or_default())
                })
                .or_else(|_| {
                    r.try_get::<Option<f64>, _>(1)
                        .map(|v| v.map(|v| v.to_string()).unwrap_or_default())
                })
                .unwrap_or_else(|e| panic!("decoding the window column: {}", e));
            (id, rendered)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_ranking_functions_number_each_partition() {
    let (_s, pool) = server().await;
    sales(&pool).await;

    assert_eq!(
        windowed(&pool, "ROW_NUMBER() OVER (PARTITION BY tenant ORDER BY at)").await,
        vec![(1, "1".into()), (2, "2".into()), (3, "3".into()),
             (4, "1".into()), (5, "2".into()), (6, "3".into())]
    );
    // Tenant 2 ties on 100: RANK skips to 3, DENSE_RANK does not.
    assert_eq!(
        windowed(&pool, "RANK() OVER (PARTITION BY tenant ORDER BY amount)").await,
        vec![(1, "1".into()), (2, "2".into()), (3, "3".into()),
             (4, "1".into()), (5, "1".into()), (6, "3".into())]
    );
    assert_eq!(
        windowed(&pool, "DENSE_RANK() OVER (PARTITION BY tenant ORDER BY amount)").await,
        vec![(1, "1".into()), (2, "2".into()), (3, "3".into()),
             (4, "1".into()), (5, "1".into()), (6, "2".into())]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_aggregate_over_a_window_keeps_every_row() {
    let (_s, pool) = server().await;
    sales(&pool).await;

    // No ORDER BY: the frame is the whole partition, so every row of a tenant
    // sees that tenant's total.
    assert_eq!(
        windowed(&pool, "SUM(amount) OVER (PARTITION BY tenant)").await,
        vec![(1, "60".into()), (2, "60".into()), (3, "60".into()),
             (4, "500".into()), (5, "500".into()), (6, "500".into())]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_order_by_makes_the_default_frame_a_running_total() {
    let (_s, pool) = server().await;
    sales(&pool).await;

    // The default frame with ORDER BY runs from the start of the partition to
    // the current row's peers — a running total, not the partition total.
    assert_eq!(
        windowed(&pool, "SUM(amount) OVER (PARTITION BY tenant ORDER BY at)").await,
        vec![(1, "10".into()), (2, "30".into()), (3, "60".into()),
             (4, "100".into()), (5, "200".into()), (6, "500".into())]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rows_frame_slides_over_the_partition() {
    let (_s, pool) = server().await;
    sales(&pool).await;

    // Each row and the one before it.
    assert_eq!(
        windowed(
            &pool,
            "SUM(amount) OVER (PARTITION BY tenant ORDER BY at \
             ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"
        )
        .await,
        vec![(1, "10".into()), (2, "30".into()), (3, "50".into()),
             (4, "100".into()), (5, "200".into()), (6, "400".into())]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_frame_wider_than_the_partition_is_the_partition() {
    let (_s, pool) = server().await;
    sales(&pool).await;
    assert_eq!(
        windowed(
            &pool,
            "SUM(amount) OVER (PARTITION BY tenant ORDER BY at \
             ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)"
        )
        .await,
        vec![(1, "10".into()), (2, "30".into()), (3, "60".into()),
             (4, "100".into()), (5, "200".into()), (6, "500".into())]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn avg_over_a_window_keeps_its_fraction() {
    let (_s, pool) = server().await;
    sales(&pool).await;
    // 10, 20, 30 averages to 20; 100, 100, 300 to 500/3.
    let out = windowed(&pool, "AVG(amount) OVER (PARTITION BY tenant)").await;
    assert_eq!(out[0].1, "20");
    assert!(out[3].1.starts_with("166.666"), "unexpected average: {}", out[3].1);
}

#[tokio::test(flavor = "multi_thread")]
async fn lag_and_lead_look_along_the_partition() {
    let (_s, pool) = server().await;
    sales(&pool).await;

    assert_eq!(
        windowed(&pool, "LAG(amount, 1) OVER (PARTITION BY tenant ORDER BY at)").await,
        vec![(1, "".into()), (2, "10".into()), (3, "20".into()),
             (4, "".into()), (5, "100".into()), (6, "100".into())]
    );
    assert_eq!(
        windowed(&pool, "LEAD(amount, 1) OVER (PARTITION BY tenant ORDER BY at)").await,
        vec![(1, "20".into()), (2, "30".into()), (3, "".into()),
             (4, "100".into()), (5, "300".into()), (6, "".into())]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lagged_timestamp_is_still_a_timestamp() {
    let (_s, pool) = server().await;
    sales(&pool).await;
    // The window's result type follows its argument. Reported as `int8` — which
    // it was before the type travelled with the value — a client asking for a
    // timestamp refused the column.
    let rows = sqlx::query(AssertSqlSafe(
        "SELECT LAG(at, 1) OVER (PARTITION BY tenant ORDER BY at) FROM sales",
    ))
    .fetch_all(&pool)
    .await
    .expect("select");
    assert_eq!(rows[0].columns()[0].type_info().name(), "TIMESTAMPTZ");
}

#[tokio::test(flavor = "multi_thread")]
async fn first_and_last_value_read_the_frame() {
    let (_s, pool) = server().await;
    sales(&pool).await;

    // FIRST_VALUE over the default frame is the partition's first row;
    // LAST_VALUE over the same frame is the *current* row, which is the classic
    // surprise and is what PostgreSQL does.
    assert_eq!(
        windowed(&pool, "FIRST_VALUE(amount) OVER (PARTITION BY tenant ORDER BY at)").await,
        vec![(1, "10".into()), (2, "10".into()), (3, "10".into()),
             (4, "100".into()), (5, "100".into()), (6, "100".into())]
    );
    assert_eq!(
        windowed(&pool, "LAST_VALUE(amount) OVER (PARTITION BY tenant ORDER BY at)").await,
        vec![(1, "10".into()), (2, "20".into()), (3, "30".into()),
             (4, "100".into()), (5, "100".into()), (6, "300".into())]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn count_over_a_window_counts_the_frame() {
    let (_s, pool) = server().await;
    sales(&pool).await;
    assert_eq!(
        windowed(&pool, "COUNT(*) OVER (PARTITION BY tenant ORDER BY at)").await,
        vec![(1, "1".into()), (2, "2".into()), (3, "3".into()),
             (4, "1".into()), (5, "2".into()), (6, "3".into())]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_range_frame_with_an_offset_is_refused_rather_than_counted_as_rows() {
    let (_s, pool) = server().await;
    sales(&pool).await;
    // Answering it as though it counted rows would select a different set of
    // rows and report no error.
    let error = sqlx::query(AssertSqlSafe(
        "SELECT SUM(amount) OVER (ORDER BY at RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) \
         FROM sales",
    ))
    .fetch_all(&pool)
    .await
    .expect_err("a RANGE frame with an offset should be refused");
    assert!(error.to_string().contains("RANGE"), "unexpected error: {}", error);
}
