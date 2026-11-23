//! Typed literals of the network, geometric, range and binary types, and
//! the structural operators over computed values, against PostgreSQL 17's
//! answers.
//!
//! `inet '10.0.0.5'`, `point '(1,2)'`, `int4range '[1,5)'` were read as a
//! column of that name; `jsonb '{"a":1}' ? 'a'`, `x @> y`, `a && b` outside a
//! `WHERE` were refused; and a `tstzrange` printed its bounds without their
//! zone, so a client read the UTC instant as its local time.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_literals_and_structural_operators_answer_as_postgresql_does() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    let cases: &[(&str, &str)] = &[
        ("SELECT inet '10.0.0.5', cidr '10.0.0.0/8', macaddr '08:00:2b:01:02:03', macaddr8 '08:00:2b:01:02:03:04:05'", "10.0.0.5|10.0.0.0/8|08:00:2b:01:02:03|08:00:2b:01:02:03:04:05"),
        ("SELECT point '(1,2)', box '(0,0),(2,2)', circle '<(1,2),3>', lseg '[(0,0),(1,1)]'", "(1,2)|(2,2),(0,0)|<(1,2),3>|[(0,0),(1,1)]"),
        ("SELECT int8range '[1,5)', int4range '[1,5)', numrange '[1.5,2)', daterange '[2026-01-01,2026-02-01)'", "[1,5)|[1,5)|[1.5,2)|[2026-01-01,2026-02-01)"),
        ("SELECT bytea '\\x0102', varbit '1010', bit varying '101', timetz '10:00+02', time with time zone '10:00+02'", "\\x0102|1010|101|10:00:00+02|10:00:00+02"),
        ("SELECT inet '10.0.0.5' << inet '10.0.0.0/8', inet '10.0.0.5' <<= cidr '10.0.0.0/8', cidr '10.0.0.0/8' >>= inet '10.1.0.0/16'", "t|t|t"),
        ("SELECT jsonb '{\"a\":1}' ? 'a', '{\"a\":1}'::jsonb @> '{\"a\":1}', ARRAY[1,2] && ARRAY[2], ARRAY[1,2] @> ARRAY[1]", "t|t|t|t"),
        ("SELECT jsonb '{\"a\":1,\"b\":2}' ?| ARRAY['b','z'], jsonb '{\"a\":1}' ?& ARRAY['a','z'], jsonb '{\"a\":1}' ?| '{x}'", "t|f|f"),
        ("SELECT int4range '[1,5)' @> 3, 3 <@ int4range '[1,5)', int4range '[1,5)' && int4range '[4,9)'", "t|t|t"),
        ("SELECT box '(0,0),(2,2)' @> point '(1,1)'", "t"),
        ("SELECT '[\"2026-01-01 10:00+02\",\"2026-01-02 10:00+00\")'::tstzrange, tstzrange '[2026-01-01 10:00+00,2026-01-03 00:00+00]', tsrange '[2026-01-01 10:00,2026-01-02 10:00)'", "[\"2026-01-01 08:00:00+00\",\"2026-01-02 10:00:00+00\")|[\"2026-01-01 10:00:00+00\",\"2026-01-03 00:00:00+00\"]|[\"2026-01-01 10:00:00\",\"2026-01-02 10:00:00\")"),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&pool, sql).await, vec![want.to_string()], "{sql}");
    }
    // One precedence with `||`, left to right: `(d ? 'a') || 'x'`. Read with
    // `?` first, it tested for the key 'ax'.
    assert_eq!(rows(&pool, "SELECT jsonb '{\"a\":1}' ? 'a' || 'x'").await, vec!["truex"]);
    assert_eq!(rows(&pool, "SELECT jsonb '{\"ax\":1}' ? 'a' || 'x'").await, vec!["falsex"]);
    // A stored tstzrange too, its bounds in UTC with their zone.
    sqlx::raw_sql("CREATE TABLE trz (r tstzrange); INSERT INTO trz VALUES ('[\"2026-01-01 10:00+02\",\"2026-01-02 10:00+00\")')")
        .execute(&pool)
        .await
        .expect("setup");
    assert_eq!(rows(&pool, "SELECT r, r::text FROM trz").await, vec![
        "[\"2026-01-01 08:00:00+00\",\"2026-01-02 10:00:00+00\")|[\"2026-01-01 08:00:00+00\",\"2026-01-02 10:00:00+00\")"
    ]);
}
