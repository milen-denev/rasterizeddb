//! The wide-type schema: two tables that between them declare almost every
//! PostgreSQL type an application is likely to reach for, and the queries that
//! exercise them.
//!
//! The existing [`crate::schema`] mirrors one production application, so it
//! measures the engine against the traffic an ORM generates. This one measures
//! something else: the *breadth* of the type system and of the operators that
//! come with it — geometric, network, range, bit-string, interval — which an
//! ORM never emits but hand-written SQL does.
//!
//! Every statement here is legal PostgreSQL and was written to run against it
//! unchanged. That is the point of the file: a difference between what
//! PostgreSQL does with these and what RasterizedDB does is a finding, not a
//! dialect.

/// The tables, in dependency order.
pub const TABLES: &[&str] = &["test_ecommerce_orders", "test_iot_telemetry"];

/// `CREATE TABLE` for each entry of [`TABLES`], in the same order.
pub const CREATE_TABLES: &[&str] = &[
    r#"CREATE TABLE test_ecommerce_orders (
    order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id BIGINT NOT NULL,
    order_number VARCHAR(64) NOT NULL UNIQUE,
    customer_id INTEGER NOT NULL,
    store_code CHAR(5) NOT NULL,
    is_prime BOOLEAN DEFAULT false,
    order_status SMALLINT NOT NULL CHECK (order_status BETWEEN 1 AND 10),
    payment_method VARCHAR(32),
    currency CHAR(3) DEFAULT 'USD',
    subtotal NUMERIC(12, 4) NOT NULL,
    tax_amount NUMERIC(12, 4) DEFAULT 0.0000,
    discount_amount NUMERIC(12, 4) DEFAULT 0.0000,
    shipping_fee NUMERIC(12, 4) DEFAULT 0.0000,
    total_amount NUMERIC(12, 4) GENERATED ALWAYS AS (subtotal + tax_amount + shipping_fee - discount_amount) STORED,
    raw_payload JSONB,
    order_notes TEXT,
    tags TEXT[],
    internal_flags BIT(8),
    ip_address INET,
    mac_address MACADDR,
    shipping_latitude DOUBLE PRECISION,
    shipping_longitude DOUBLE PRECISION,
    shipping_location POINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    fulfilled_at TIMESTAMP,
    estimated_delivery DATE,
    fulfillment_duration INTERVAL,
    audit_hash BYTEA,
    checkout_session_id UUID,
    metadata JSON
)"#,
    r#"CREATE TABLE test_iot_telemetry (
    reading_id BIGSERIAL PRIMARY KEY,
    device_uuid UUID NOT NULL,
    device_sequence NUMERIC(20, 0),
    firmware_version VARCHAR(20),
    is_active BOOLEAN NOT NULL DEFAULT true,
    sensor_type_code SMALLINT,
    hardware_status BIT VARYING(32),
    sensor_readings REAL[],
    calibration_matrix DOUBLE PRECISION[][],
    sample_frequencies INT4RANGE,
    operational_hours INTERVAL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    server_received_at TIMESTAMP NOT NULL DEFAULT clock_timestamp(),
    log_date DATE NOT NULL DEFAULT CURRENT_DATE,
    time_of_day TIME NOT NULL,
    time_with_offset TIMETZ NOT NULL,
    bounding_box BOX,
    sensor_path PATH,
    sensor_coverage POLYGON,
    focal_point CIRCLE,
    vector_signature LSEG,
    network_subnet CIDR,
    client_ip INET,
    gateway_mac MACADDR8,
    error_log TEXT,
    payload_checksum BYTEA,
    metric_tags JSONB,
    raw_blob BYTEA,
    alert_level INT2,
    correlation_id UUID
)"#,
];

/// Rows covering the shapes the queries below select on.
///
/// Written as literals rather than bound parameters so a failure names the
/// value's *syntax* rather than a driver's encoding of it, and so the same
/// statement can be pasted into `psql` against real PostgreSQL to see what it
/// should have done.
pub const SEED: &[&str] = &[
    // --- orders ------------------------------------------------------------
    // Tenant 1: two orders that satisfy every filter of query 1 — the low bit
    // of internal_flags set, a payload that is not risk-flagged, tags present.
    r#"INSERT INTO test_ecommerce_orders
       (order_id, tenant_id, order_number, customer_id, store_code, is_prime,
        order_status, payment_method, currency, subtotal, tax_amount,
        discount_amount, shipping_fee, raw_payload, order_notes, tags,
        internal_flags, ip_address, mac_address, shipping_latitude,
        shipping_longitude, shipping_location, created_at, updated_at,
        fulfilled_at, estimated_delivery, fulfillment_duration, audit_hash,
        checkout_session_id, metadata)
       VALUES
       ('11111111-1111-4111-8111-111111111111', 1, 'ORD-0001', 501, 'ST001', true,
        3, 'card', 'USD', 100.0000, 10.0000, 5.0000, 7.5000,
        '{"is_risk_flagged": false, "payment_details": {"brand": "visa"}, "items": [{"sku": "A-1"}, {"sku": "A-2"}], "shipping": {"address": {"zipcode": "10115"}}}',
        'urgent delivery please', '{express,gift}',
        B'00000001', '192.168.1.10', '08:00:2b:01:02:03', 52.52, 13.405,
        '(5,5)', '2026-08-20 10:00:00+00', '2026-08-20 10:00:00+00',
        '2026-08-20 12:30:00', '2026-08-25', INTERVAL '1 hour', '\x00ff',
        '22222222-2222-4222-8222-222222222222',
        '{"requires_signature": true}')"#,
    r#"INSERT INTO test_ecommerce_orders
       (order_id, tenant_id, order_number, customer_id, store_code, is_prime,
        order_status, payment_method, currency, subtotal, tax_amount,
        discount_amount, shipping_fee, raw_payload, order_notes, tags,
        internal_flags, ip_address, mac_address, shipping_latitude,
        shipping_longitude, shipping_location, created_at, updated_at,
        fulfilled_at, estimated_delivery, fulfillment_duration, audit_hash,
        checkout_session_id, metadata)
       VALUES
       ('33333333-3333-4333-8333-333333333333', 1, 'ORD-0002', 501, 'ST001', false,
        3, 'card', 'USD', 250.0000, 25.0000, 0.0000, 0.0000,
        '{"is_risk_flagged": false, "payment_details": {"brand": "mastercard"}, "items": [{"sku": "B-9"}], "shipping": {"address": {"zipcode": "20095"}}}',
        'fragile handle with care', '{gift}',
        B'00000011', '192.168.1.11', '08:00:2b:01:02:04', 53.55, 9.99,
        '(6,6)', '2026-08-21 09:00:00+00', '2026-08-21 09:00:00+00',
        '2026-08-21 09:30:00', '2026-08-26', INTERVAL '2 hours', '\x0100',
        '44444444-4444-4444-8444-444444444444',
        '{"requires_signature": false}')"#,
    // Tenant 2: one order, a different currency, and no low bit set — so the
    // bitwise filter of query 1 excludes it.
    r#"INSERT INTO test_ecommerce_orders
       (order_id, tenant_id, order_number, customer_id, store_code, is_prime,
        order_status, payment_method, currency, subtotal, tax_amount,
        discount_amount, shipping_fee, raw_payload, order_notes, tags,
        internal_flags, ip_address, mac_address, shipping_latitude,
        shipping_longitude, shipping_location, created_at, updated_at,
        fulfilled_at, estimated_delivery, fulfillment_duration, audit_hash,
        checkout_session_id, metadata)
       VALUES
       ('55555555-5555-4555-8555-555555555555', 2, 'ORD-0003', 777, 'ST002', false,
        5, 'paypal', 'EUR', 80.0000, 8.0000, 0.0000, 4.0000,
        '{"is_risk_flagged": true, "payment_details": {"brand": "paypal"}, "items": [], "shipping": {"address": {"zipcode": "75001"}}}',
        NULL, '{standard}',
        B'00000010', '10.0.0.5', '08:00:2b:01:02:05', 48.85, 2.35,
        '(80,80)', '2026-08-22 11:00:00+00', '2026-08-22 11:00:00+00',
        NULL, '2026-08-30', INTERVAL '30 minutes', '\xabcd',
        NULL, '{"requires_signature": true}')"#,
    // --- telemetry ---------------------------------------------------------
    r#"INSERT INTO test_iot_telemetry
       (device_uuid, device_sequence, firmware_version, is_active,
        sensor_type_code, hardware_status, sensor_readings, calibration_matrix,
        sample_frequencies, operational_hours, event_timestamp, time_of_day,
        time_with_offset, bounding_box, sensor_path, sensor_coverage,
        focal_point, vector_signature, network_subnet, client_ip, gateway_mac,
        error_log, payload_checksum, metric_tags, raw_blob, alert_level,
        correlation_id)
       VALUES
       ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', 100, '1.2.3', true,
        7, B'1010', '{80.5,90.25,42.0}', '{{1.0,2.0},{3.0,4.0}}',
        '[50,150)', INTERVAL '3 days 4 hours', '2026-08-20 10:02:00+00', '13:45:00',
        '13:45:00+02', '((0,0),(10,10))', '((0,0),(1,1),(2,2))',
        '((0,0),(0,10),(10,10),(10,0),(0,0))', '<(5,5),3>', '[(0,0),(1,1)]',
        '192.168.1.0/24', '192.168.1.10', '08:00:2b:01:02:03:04:05',
        NULL, '\xdead', '{"site": "berlin"}', '\xbeef', 2,
        '66666666-6666-4666-8666-666666666666')"#,
    r#"INSERT INTO test_iot_telemetry
       (device_uuid, device_sequence, firmware_version, is_active,
        sensor_type_code, hardware_status, sensor_readings, calibration_matrix,
        sample_frequencies, operational_hours, event_timestamp, time_of_day,
        time_with_offset, bounding_box, sensor_path, sensor_coverage,
        focal_point, vector_signature, network_subnet, client_ip, gateway_mac,
        error_log, payload_checksum, metric_tags, raw_blob, alert_level,
        correlation_id)
       VALUES
       ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', 101, '1.2.4', true,
        7, B'0101', '{10.0,20.0}', '{{5.0,6.0}}',
        '[200,300)', INTERVAL '1 day', '2026-08-21 09:01:00+00', '08:15:00',
        '08:15:00+00', '((20,20),(30,30))', '[(5,5),(6,6)]',
        '((20,20),(20,30),(30,30),(30,20),(20,20))', '<(25,25),2>', '[(5,5),(6,6)]',
        '10.0.0.0/8', '10.0.0.5', '08:00:2b:01:02:03:04:06',
        'sensor drift', '\xcafe', '{"site": "paris"}', '\xf00d', 5,
        '77777777-7777-4777-8777-777777777777')"#,
];

/// The six queries, each legal PostgreSQL, with the feature families it needs.
pub struct WideQuery {
    pub name: &'static str,
    /// What the query is written to exercise, for the report.
    pub features: &'static [&'static str],
    pub sql: &'static str,
}

pub const QUERIES: &[WideQuery] = &[
    WideQuery {
        name: "cte_jsonb_lateral_unnest_bitwise",
        features: &[
            "CTE",
            "LEFT JOIN LATERAL unnest",
            "array_agg DISTINCT FILTER",
            "jsonb_agg",
            "jsonb ->",
            "jsonb @>",
            "bit-string &",
            "CURRENT_DATE - INTERVAL",
        ],
        sql: r#"WITH order_metrics AS (
    SELECT
        o.tenant_id,
        o.currency,
        o.order_status,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.total_amount) AS gross_revenue,
        AVG(o.shipping_fee) AS avg_shipping,
        array_agg(DISTINCT tag) FILTER (WHERE tag IS NOT NULL) AS combined_tags,
        jsonb_agg(o.raw_payload -> 'payment_details') AS payment_summaries
    FROM test_ecommerce_orders o
    LEFT JOIN LATERAL unnest(o.tags) AS tag ON true
    WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days'
      AND (o.internal_flags & B'00000001') = B'00000001'
      AND o.raw_payload @> '{"is_risk_flagged": false}'
    GROUP BY o.tenant_id, o.currency, o.order_status
)
SELECT
    tenant_id,
    currency,
    order_status,
    total_orders,
    gross_revenue,
    combined_tags,
    payment_summaries
FROM order_metrics
WHERE total_orders > 1
ORDER BY gross_revenue DESC"#,
    },
    WideQuery {
        name: "spatial_range_inet_filtering",
        features: &[
            "inet <<=",
            "range @> element",
            "polygon &&",
            "circle <@",
            "cast to text",
            "SIMILAR TO",
        ],
        sql: r#"SELECT
    device_uuid,
    log_date,
    time_of_day,
    sensor_coverage,
    sample_frequencies,
    gateway_mac
FROM test_iot_telemetry
WHERE client_ip <<= inet '192.168.0.0/16'
  AND sample_frequencies @> 100
  AND sensor_coverage && POLYGON '((0,0),(0,10),(10,10),(10,0),(0,0))'
  AND focal_point <@ CIRCLE '<(5,5),20>'
  AND hardware_status::text SIMILAR TO '101%'
ORDER BY event_timestamp DESC
LIMIT 100"#,
    },
    WideQuery {
        name: "window_frames_and_interval_math",
        features: &[
            "AVG OVER ROWS BETWEEN",
            "DENSE_RANK OVER",
            "LAG OVER",
            "timestamp - timestamp = interval",
            "interval comparison",
        ],
        sql: r#"SELECT
    order_id,
    tenant_id,
    customer_id,
    subtotal,
    created_at,
    fulfilled_at - created_at AS actual_processing_time,
    fulfillment_duration AS target_processing_time,
    AVG(subtotal) OVER (
        PARTITION BY tenant_id
        ORDER BY created_at
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_subtotal,
    DENSE_RANK() OVER (
        PARTITION BY tenant_id
        ORDER BY subtotal DESC
    ) AS revenue_rank_within_tenant,
    LAG(created_at, 1) OVER (
        PARTITION BY customer_id
        ORDER BY created_at
    ) AS previous_customer_order_time
FROM test_ecommerce_orders
WHERE fulfilled_at IS NOT NULL
  AND (fulfilled_at - created_at) > fulfillment_duration"#,
    },
    WideQuery {
        name: "spatial_network_join",
        features: &[
            "join on OR of inet predicates",
            "inet >>=",
            "point(circle)",
            "distance <->",
            "BETWEEN over interval arithmetic",
        ],
        sql: r#"SELECT
    o.order_number,
    o.customer_id,
    t.device_uuid,
    t.event_timestamp,
    (o.shipping_location <-> point(t.focal_point)) AS distance_to_sensor,
    o.ip_address,
    t.client_ip
FROM test_ecommerce_orders o
INNER JOIN test_iot_telemetry t
    ON t.client_ip = o.ip_address
    OR t.network_subnet >>= o.ip_address
WHERE o.created_at BETWEEN t.event_timestamp - INTERVAL '5 minutes' AND t.event_timestamp + INTERVAL '5 minutes'
  AND o.shipping_location IS NOT NULL
  AND t.focal_point IS NOT NULL
  AND (o.shipping_location <-> point(t.focal_point)) < 50.0"#,
    },
    WideQuery {
        name: "array_ndims_and_ordinality",
        features: &[
            "array_ndims",
            "array_length",
            "LATERAL unnest WITH ORDINALITY",
            "multi-dimensional subscript",
        ],
        sql: r#"SELECT
    reading_id,
    device_uuid,
    array_ndims(calibration_matrix) AS matrix_dimensions,
    array_length(sensor_readings, 1) AS reading_count,
    sensor_val,
    val_idx
FROM test_iot_telemetry,
LATERAL unnest(sensor_readings) WITH ORDINALITY AS arr(sensor_val, val_idx)
WHERE array_length(sensor_readings, 1) IS NOT NULL
  AND sensor_val > 75.5
  AND calibration_matrix[1][1] IS NOT NULL"#,
    },
    WideQuery {
        name: "fulltext_jsonpath_and_subquery",
        features: &[
            "to_tsvector / to_tsquery / @@",
            "jsonb_path_query",
            "jsonb_path_query_array",
            "json::jsonb cast",
            "IN (subquery with GROUP BY / HAVING)",
        ],
        sql: r#"SELECT
    o.order_id,
    o.order_number,
    jsonb_path_query_array(o.raw_payload, '$.items[*].sku') AS item_skus,
    jsonb_path_query(o.raw_payload, '$.shipping.address.zipcode') AS zip_code,
    o.order_notes
FROM test_ecommerce_orders o
WHERE to_tsvector('english', COALESCE(o.order_notes, '')) @@ to_tsquery('english', 'urgent & !fragile')
  AND o.metadata::jsonb @> '{"requires_signature": true}'::jsonb
  AND o.order_id IN (
      SELECT checkout_session_id
      FROM test_ecommerce_orders
      WHERE checkout_session_id IS NOT NULL
      GROUP BY checkout_session_id
      HAVING COUNT(*) = 1
  )"#,
    },
];

/// Creates the tables and seeds them.
pub async fn apply(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    use sqlx::AssertSqlSafe;
    for sql in CREATE_TABLES {
        sqlx::query(AssertSqlSafe(*sql)).execute(pool).await?;
    }
    for sql in SEED {
        sqlx::query(AssertSqlSafe(*sql)).execute(pool).await?;
    }
    Ok(())
}
