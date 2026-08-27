//! PostgreSQL's structural operators: containment, overlap, network
//! subsumption and text-search match.
//!
//! These ask a question about a value's *shape* rather than its order, and the
//! answer differs by type — `@>` means one thing for a document, another for a
//! range, another for a polygon. Each is asserted against rows that both do and
//! do not satisfy it: an operator that answered `false` for everything would
//! pass a test that only checked the matching row was absent from the
//! non-matching set.

use rasterizeddb_compat::{TestServer, wide_types};
use sqlx::{AssertSqlSafe, Row};

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    wide_types::apply(&pool).await.expect("the wide schema should apply");
    (server, pool)
}

/// The first column of every row, as text.
async fn column(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{}\n  failed: {}", sql, e.to_string().lines().next().unwrap_or("")))
        .iter()
        .map(|r| r.get::<Option<String>, _>(0).unwrap_or_default())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_address_is_matched_against_a_network() {
    let (_s, pool) = seeded().await;

    // 192.168.1.10 is in 192.168.0.0/16; 10.0.0.5 is not.
    assert_eq!(
        column(
            &pool,
            "SELECT firmware_version FROM test_iot_telemetry \
             WHERE client_ip <<= inet '192.168.0.0/16' ORDER BY firmware_version"
        )
        .await,
        vec!["1.2.3"]
    );
    assert_eq!(
        column(
            &pool,
            "SELECT firmware_version FROM test_iot_telemetry \
             WHERE client_ip <<= inet '10.0.0.0/8' ORDER BY firmware_version"
        )
        .await,
        vec!["1.2.4"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_network_column_is_matched_against_an_address() {
    let (_s, pool) = seeded().await;
    assert_eq!(
        column(
            &pool,
            "SELECT firmware_version FROM test_iot_telemetry \
             WHERE network_subnet >>= inet '192.168.1.10'"
        )
        .await,
        vec!["1.2.3"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_range_contains_a_bare_value() {
    let (_s, pool) = seeded().await;

    // The seeded ranges are [50,150) and [200,300).
    assert_eq!(
        column(
            &pool,
            "SELECT firmware_version FROM test_iot_telemetry WHERE sample_frequencies @> 100"
        )
        .await,
        vec!["1.2.3"]
    );
    assert_eq!(
        column(
            &pool,
            "SELECT firmware_version FROM test_iot_telemetry WHERE sample_frequencies @> 250"
        )
        .await,
        vec!["1.2.4"]
    );
    // 175 falls in the gap between them.
    assert!(
        column(
            &pool,
            "SELECT firmware_version FROM test_iot_telemetry WHERE sample_frequencies @> 175"
        )
        .await
        .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn shapes_are_matched_by_overlap_and_containment() {
    let (_s, pool) = seeded().await;

    assert_eq!(
        column(
            &pool,
            "SELECT firmware_version FROM test_iot_telemetry \
             WHERE sensor_coverage && POLYGON '((0,0),(0,10),(10,10),(10,0),(0,0))'"
        )
        .await,
        vec!["1.2.3"]
    );
    assert_eq!(
        column(
            &pool,
            "SELECT firmware_version FROM test_iot_telemetry \
             WHERE focal_point <@ CIRCLE '<(5,5),20>'"
        )
        .await,
        vec!["1.2.3"]
    );
    // The far device's circle is centred at (25,25) with radius 2, so a circle
    // of radius 20 around (5,5) does not reach it.
    assert_eq!(
        column(
            &pool,
            "SELECT firmware_version FROM test_iot_telemetry \
             WHERE focal_point <@ CIRCLE '<(25,25),5>'"
        )
        .await,
        vec!["1.2.4"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_document_is_matched_by_containment() {
    let (_s, pool) = seeded().await;

    // Two of the three seeded orders are not risk-flagged.
    let mut matched = column(
        &pool,
        "SELECT order_number FROM test_ecommerce_orders \
         WHERE raw_payload @> '{\"is_risk_flagged\": false}' ORDER BY order_number",
    )
    .await;
    matched.sort();
    assert_eq!(matched, vec!["ORD-0001", "ORD-0002"]);

    // `false` is a value, not an absence: the flagged order must not match, and
    // neither would an order with no such key at all.
    assert_eq!(
        column(
            &pool,
            "SELECT order_number FROM test_ecommerce_orders \
             WHERE raw_payload @> '{\"is_risk_flagged\": true}'"
        )
        .await,
        vec!["ORD-0003"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_nested_document_path_is_matched_by_containment() {
    let (_s, pool) = seeded().await;
    assert_eq!(
        column(
            &pool,
            "SELECT order_number FROM test_ecommerce_orders \
             WHERE raw_payload @> '{\"payment_details\": {\"brand\": \"visa\"}}'"
        )
        .await,
        vec!["ORD-0001"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn arrays_are_matched_by_containment_and_overlap() {
    let (_s, pool) = seeded().await;

    let mut gifts = column(
        &pool,
        "SELECT order_number FROM test_ecommerce_orders WHERE tags @> '{gift}' \
         ORDER BY order_number",
    )
    .await;
    gifts.sort();
    assert_eq!(gifts, vec!["ORD-0001", "ORD-0002"]);

    assert_eq!(
        column(
            &pool,
            "SELECT order_number FROM test_ecommerce_orders WHERE tags && '{express}'"
        )
        .await,
        vec!["ORD-0001"]
    );
    // Containment wants *every* element; only one order has both.
    assert_eq!(
        column(
            &pool,
            "SELECT order_number FROM test_ecommerce_orders WHERE tags @> '{express,gift}'"
        )
        .await,
        vec!["ORD-0001"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_operator_that_has_no_meaning_for_the_column_is_refused() {
    let (_s, pool) = seeded().await;

    // Answering `false` would return no rows and look exactly like an empty
    // table, so this has to be an error.
    let error = sqlx::query(AssertSqlSafe(
        "SELECT order_number FROM test_ecommerce_orders WHERE tenant_id @> 1",
    ))
    .fetch_all(&pool)
    .await
    .expect_err("@> over a bigint should be refused");
    assert!(
        error.to_string().contains("does not apply"),
        "unexpected error: {}",
        error
    );
}
