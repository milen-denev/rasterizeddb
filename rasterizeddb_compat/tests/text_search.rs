//! Full-text search and the array-shape functions.
//!
//! `to_tsvector` normalises a document to its stemmed lexemes and `to_tsquery`
//! normalises a boolean query the same way, which is what makes a query for
//! "runs" find a document that said "running". Both are asserted on their
//! output text rather than only on a row count, because a stemmer that did
//! nothing would still match the words that need no stemming.

use rasterizeddb_compat::{TestServer, wide_types};
use sqlx::{AssertSqlSafe, Row};

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    wide_types::apply(&pool).await.expect("the wide schema should apply");
    (server, pool)
}

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<(String, String)> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| {
            panic!("{}\n  failed: {}", sql, e.to_string().lines().next().unwrap_or(""))
        })
        .iter()
        .map(|r| {
            (
                r.get::<Option<String>, _>(0).unwrap_or_default(),
                r.get::<Option<String>, _>(1).unwrap_or_default(),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_document_becomes_its_stemmed_lexemes() {
    let (_s, pool) = seeded().await;

    let mut out = rows(
        &pool,
        "SELECT order_number, to_tsvector('english', order_notes) \
         FROM test_ecommerce_orders WHERE order_notes IS NOT NULL ORDER BY order_number",
    )
    .await;
    out.sort();

    assert_eq!(
        out,
        vec![
            // "urgent delivery please" — stemmed, and positions kept.
            ("ORD-0001".to_string(), "'deliveri':2 'pleas':3 'urgent':1".to_string()),
            // "fragile handle with care" — `with` is a stop word, so `care`
            // keeps position 4 rather than moving up to 3.
            ("ORD-0002".to_string(), "'care':4 'fragil':1 'handl':2".to_string()),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_simple_configuration_does_not_stem() {
    let (_s, pool) = seeded().await;
    let out = rows(
        &pool,
        "SELECT order_number, to_tsvector('simple', order_notes) \
         FROM test_ecommerce_orders WHERE order_number = 'ORD-0001'",
    )
    .await;
    assert_eq!(out[0].1, "'delivery':2 'please':3 'urgent':1");
}

#[tokio::test(flavor = "multi_thread")]
async fn array_length_counts_elements_and_is_null_for_none() {
    let (_s, pool) = seeded().await;
    let out = rows(
        &pool,
        "SELECT firmware_version, array_length(sensor_readings, 1) \
         FROM test_iot_telemetry ORDER BY firmware_version",
    )
    .await;
    assert_eq!(
        out,
        vec![("1.2.3".to_string(), "3".to_string()), ("1.2.4".to_string(), "2".to_string())]
    );
}

/// The engine's arrays are flat, so `array_ndims` answers 1 where PostgreSQL
/// would answer 2 for a column declared `DOUBLE PRECISION[][]`.
///
/// Pinned as a test rather than left undocumented: the declaration is accepted
/// and the values round-trip, but the second dimension is not modelled, and a
/// query that branches on the dimension count would branch differently here.
#[tokio::test(flavor = "multi_thread")]
async fn array_ndims_reports_one_dimension() {
    let (_s, pool) = seeded().await;
    let out = rows(
        &pool,
        "SELECT firmware_version, array_ndims(calibration_matrix) \
         FROM test_iot_telemetry ORDER BY firmware_version",
    )
    .await;
    assert_eq!(out[0].1, "1");
}
