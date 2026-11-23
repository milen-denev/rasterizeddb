//! Arithmetic over timestamps and intervals.
//!
//! The three cases PostgreSQL defines have three different result *types*, and
//! getting the type wrong is not cosmetic: the difference of two timestamps
//! used to be described as `int8`, so a client asking to decode a duration
//! refused the column outright.
//!
//! Months are the reason an interval is not just a microsecond count. Adding
//! one month to 31 January is 28 February, not 3 March, so the months are
//! applied to the calendar rather than multiplied out.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Column, Row, TypeInfo};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::query(AssertSqlSafe(
        "CREATE TABLE spans (
            id INTEGER NOT NULL,
            started TIMESTAMPTZ NOT NULL,
            finished TIMESTAMPTZ,
            target INTERVAL
        )",
    ))
    .execute(&pool)
    .await
    .expect("create");
    for (id, started, finished, target) in [
        (1, "2026-01-31 10:00:00+00", Some("2026-01-31 12:30:00+00"), "INTERVAL '1 hour'"),
        (2, "2026-02-01 09:00:00+00", Some("2026-02-01 09:30:00+00"), "INTERVAL '2 hours'"),
        (3, "2026-03-01 09:00:00+00", None, "INTERVAL '30 minutes'"),
    ] {
        let finished = match finished {
            Some(f) => format!("'{}'", f),
            None => "NULL".to_string(),
        };
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO spans (id, started, finished, target) VALUES ({}, '{}', {}, {})",
            id, started, finished, target
        )))
        .execute(&pool)
        .await
        .expect("insert");
    }
    (server, pool)
}

/// One expression, rendered as text, for the row with this id. `started` is a
/// `timestamptz`, so a timestamp computed from it is shown with its `+00`, as
/// PostgreSQL 17 shows it (the session is UTC).
async fn value(pool: &sqlx::PgPool, expression: &str, id: i32) -> Option<String> {
    sqlx::query(AssertSqlSafe(format!(
        "SELECT ({})::text FROM spans WHERE id = {}",
        expression, id
    )))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| {
        panic!("{} failed: {}", expression, e.to_string().lines().next().unwrap_or(""))
    })
    .get(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_difference_of_two_timestamps_is_an_interval() {
    let (_s, pool) = server().await;
    assert_eq!(value(&pool, "finished - started", 1).await.as_deref(), Some("02:30:00"));
    assert_eq!(value(&pool, "finished - started", 2).await.as_deref(), Some("00:30:00"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_timestamp_difference_advertises_the_interval_type() {
    let (_s, pool) = server().await;
    let rows = sqlx::query(AssertSqlSafe("SELECT finished - started FROM spans WHERE id = 1"))
        .fetch_all(&pool)
        .await
        .expect("select");
    assert_eq!(rows[0].columns()[0].type_info().name(), "INTERVAL");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_timestamp_offset_by_an_interval_is_a_timestamp() {
    let (_s, pool) = server().await;
    assert_eq!(
        value(&pool, "started + INTERVAL '1 day'", 1).await.as_deref(),
        Some("2026-02-01 10:00:00+00")
    );
    assert_eq!(
        value(&pool, "started - INTERVAL '2 hours'", 1).await.as_deref(),
        Some("2026-01-31 08:00:00+00")
    );
    // The interval may come from a column as well as a literal.
    assert_eq!(
        value(&pool, "started + target", 1).await.as_deref(),
        Some("2026-01-31 11:00:00+00")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_month_moves_the_calendar_rather_than_thirty_days() {
    let (_s, pool) = server().await;
    // 31 January plus a month is 28 February — the day is clamped to the target
    // month's last, which is what PostgreSQL does. Thirty days would give
    // 2 March, and a naive 31 would give an impossible date.
    assert_eq!(
        value(&pool, "started + INTERVAL '1 month'", 1).await.as_deref(),
        Some("2026-02-28 10:00:00+00")
    );
    // A year, likewise, is a calendar step.
    assert_eq!(
        value(&pool, "started + INTERVAL '1 year'", 1).await.as_deref(),
        Some("2027-01-31 10:00:00+00")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn intervals_add_without_collapsing_months_into_days() {
    let (_s, pool) = server().await;
    assert_eq!(
        value(&pool, "target + INTERVAL '30 minutes'", 1).await.as_deref(),
        Some("01:30:00")
    );
    assert_eq!(
        value(&pool, "target + INTERVAL '1 month'", 1).await.as_deref(),
        Some("1 mon 01:00:00")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interval_column_is_compared_against_a_literal() {
    let (_s, pool) = server().await;
    let ids: Vec<i32> = sqlx::query(AssertSqlSafe(
        "SELECT id FROM spans WHERE target > INTERVAL '45 minutes' ORDER BY id",
    ))
    .fetch_all(&pool)
    .await
    .expect("select")
    .iter()
    .map(|r| r.get::<i32, _>(0))
    .collect();
    assert_eq!(ids, vec![1, 2]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_computed_span_is_compared_against_a_column() {
    let (_s, pool) = server().await;
    // The shape a service-level report is written in: which rows took longer
    // than their target. Row 1 took 2h30 against 1h; row 2 beat its target;
    // row 3 never finished.
    let ids: Vec<i32> = sqlx::query(AssertSqlSafe(
        "SELECT id FROM spans WHERE finished IS NOT NULL \
         AND (finished - started) > target ORDER BY id",
    ))
    .fetch_all(&pool)
    .await
    .expect("select")
    .iter()
    .map(|r| r.get::<i32, _>(0))
    .collect();
    assert_eq!(ids, vec![1]);
}
