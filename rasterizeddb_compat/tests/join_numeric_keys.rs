//! A join key of two numeric types or scales matches by value, in the
//! common type PostgreSQL resolves the `=` in: `numeric = float8` as float8,
//! `numeric = numeric` by value whatever the scales.
//!
//! Join keys were matched by their bytes. A NUMERIC(10,2) 1.50 is 150 units,
//! an unconstrained 1.5 is 1500000000 and a float 1.5 is an f64, so each pair
//! found nothing. Constants and computed values in a derived table or CTE are
//! NUMERIC now (they were floats), which turned `JOIN (SELECT 1.5 AS c) x ON
//! float_col = x.c` from one row to none; the column-to-column pairs were
//! already wrong. Every expectation is PostgreSQL 17's.

use rasterizeddb_compat::TestServer;
use tokio_postgres::{Client, SimpleQueryMessage};

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

async fn ids(c: &Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap_or("").to_string()),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn numeric_join_keys_meet_by_value() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    for sql in [
        "CREATE TABLE jc (id INT PRIMARY KEY, f FLOAT8, u NUMERIC, a NUMERIC(10,1), r REAL)",
        "INSERT INTO jc VALUES (1, 1.5, 1.5, 1.5, 1.5), (2, 2.5, 2.5, 2.5, 2.5)",
        "CREATE TABLE j1 (id INT PRIMARY KEY, a NUMERIC(10,2), w NUMERIC(20,12), u NUMERIC, f FLOAT8, i INT)",
        "CREATE TABLE j2 (id INT PRIMARY KEY, a NUMERIC(10,2), w NUMERIC(20,12), u NUMERIC, f FLOAT8, i INT)",
        "INSERT INTO j1 VALUES (1, 1.50, 1.500000000000, 1.5, 1.5, 2), (2, 2.25, 0.123456789012, 2.25, 2.25, 3)",
        "INSERT INTO j2 VALUES (1, 1.50, 1.500000000000, 1.5, 1.5, 2), (2, 2.25, 0.123456789012, 2.25, 2.25, 3)",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let mut wrong = Vec::new();
    for (sql, pg) in [
        // A derived or CTE constant, now NUMERIC, against each column type.
        ("SELECT jc.id FROM jc JOIN (SELECT 1.5 AS c FROM jc WHERE id = 1) x ON jc.u = x.c", vec!["1"]),
        ("SELECT jc.id FROM jc JOIN (SELECT 1.5 AS c FROM jc WHERE id = 1) x ON jc.a = x.c", vec!["1"]),
        ("SELECT jc.id FROM jc JOIN (SELECT 1.5 AS c FROM jc WHERE id = 1) x ON jc.r = x.c", vec!["1"]),
        ("SELECT jc.id FROM jc JOIN (SELECT 1.5 AS c FROM jc WHERE id = 1) x ON jc.f = x.c", vec!["1"]),
        ("WITH x AS (SELECT 2.5 AS c FROM jc WHERE id = 1) SELECT jc.id FROM jc JOIN x ON jc.f = x.c", vec!["2"]),
        ("SELECT jc.id FROM jc JOIN (SELECT u * 1 AS c FROM jc WHERE id = 1) x ON jc.u = x.c", vec!["1"]),
        ("SELECT jc.id FROM jc JOIN (SELECT a + 0 AS c FROM jc WHERE id = 1) x ON jc.a = x.c", vec!["1"]),
        // Column to column across declared scales and types.
        ("SELECT j1.id FROM j1 JOIN j2 ON j1.a = j2.u ORDER BY 1", vec!["1", "2"]),
        ("SELECT j1.id FROM j1 JOIN j2 ON j1.a = j2.w ORDER BY 1", vec!["1"]),
        ("SELECT j1.id FROM j1 JOIN j2 ON j1.a = j2.f ORDER BY 1", vec!["1", "2"]),
        ("SELECT j1.id FROM j1 JOIN j2 ON j1.i = j2.a ORDER BY 1", vec![]),
        ("SELECT j1.id FROM j1 JOIN (SELECT a FROM j2) x ON j1.u = x.a ORDER BY 1", vec!["1", "2"]),
        ("SELECT j1.id FROM j1 JOIN (SELECT i * 0.75 AS m FROM j2) x ON j1.a = x.m ORDER BY 1", vec!["1", "2"]),
        ("SELECT j1.id FROM j1 JOIN (SELECT a / 1 AS m FROM j2) x ON j1.a = x.m ORDER BY 1", vec!["1", "2"]),
        // Same type and scale still matches by bytes.
        ("SELECT j1.id FROM j1 JOIN j2 ON j1.a = j2.a ORDER BY 1", vec!["1", "2"]),
        ("SELECT j1.id FROM j1 JOIN (SELECT f FROM j2) x ON j1.f = x.f ORDER BY 1", vec!["1", "2"]),
    ] {
        let got = ids(&c, sql).await;
        if got != pg {
            wrong.push(format!("{sql}: got {got:?}, PostgreSQL {pg:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
