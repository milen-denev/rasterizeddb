//! NUMERIC precision as PostgreSQL keeps it: a generated column computed from
//! the value its inputs store, and (C6) an unconstrained NUMERIC holding each
//! value at its own scale. Every expectation is PostgreSQL 17's (`docker run
//! --rm postgres:17`), read as text.

use rasterizeddb_compat::TestServer;
use tokio_postgres::{Client, SimpleQueryMessage};

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

async fn run(c: &Client, sql: &str) {
    c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn rows(c: &Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).unwrap_or("").to_string()).collect::<Vec<_>>().join("|"))
            }
            _ => None,
        })
        .collect()
}

/// A generated column recomputed by an UPDATE or an ON CONFLICT DO UPDATE
/// reads its inputs as stored: `SET n2 = 8.345` on a NUMERIC(5,2) is 8.35 to
/// `n2 * 1` (8.4 at one place, not 8.3) and to `n2::text` ('8.35', not
/// '8.345'), as an INSERT already had it.
#[tokio::test(flavor = "multi_thread")]
async fn a_generated_column_reads_the_value_as_stored() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(
        &c,
        "CREATE TABLE g (id INT PRIMARY KEY, n2 NUMERIC(5,2), g1 NUMERIC(10,1) GENERATED ALWAYS AS (n2 * 1) STORED, \
         gt TEXT GENERATED ALWAYS AS (n2::text) STORED)",
    )
    .await;
    for sql in [
        "INSERT INTO g (id, n2) VALUES (1, 8.345)",
        "INSERT INTO g (id, n2) VALUES (2, '8.345'::real)",
        "INSERT INTO g (id, n2) VALUES (3, '8.345'::float8)",
        "INSERT INTO g (id, n2) VALUES (4, 0), (5, 0), (6, 0), (7, 0)",
        "UPDATE g SET n2 = 8.345 WHERE id = 4",
        "UPDATE g SET n2 = '8.345'::float8 WHERE id = 5",
        "INSERT INTO g (id, n2) VALUES (6, 0) ON CONFLICT (id) DO UPDATE SET n2 = 8.345",
        "INSERT INTO g (id, n2) VALUES (7, 0) ON CONFLICT (id) DO UPDATE SET n2 = '8.345'::float8",
        "INSERT INTO g (id, n2) VALUES (8, 8.345) ON CONFLICT (id) DO NOTHING",
    ] {
        run(&c, sql).await;
    }
    let want: Vec<String> = (1..=8).map(|id| format!("{id}|8.35|8.4|8.35")).collect();
    assert_eq!(rows(&c, "SELECT * FROM g ORDER BY id").await, want);

    // An assigned expression of the row is rounded as its column rounds it.
    run(
        &c,
        "CREATE TABLE g2 (id INT PRIMARY KEY, i INT, p NUMERIC(6,2), gi BIGINT GENERATED ALWAYS AS (i * 10) STORED, \
         gp NUMERIC(12,3) GENERATED ALWAYS AS (p * 3) STORED, gt TEXT GENERATED ALWAYS AS (p::text || '/' || i::text) STORED)",
    )
    .await;
    run(&c, "INSERT INTO g2 (id, i, p) VALUES (1, 1, 1), (2, 2, 2)").await;
    run(&c, "UPDATE g2 SET i = 3, p = p * 1.005 WHERE id = 1").await;
    run(&c, "UPDATE g2 SET p = p / 3, i = i * 1.5 WHERE id = 2").await;
    run(&c, "INSERT INTO g2 (id, i, p) VALUES (2, 0, 0) ON CONFLICT (id) DO UPDATE SET p = g2.p + 0.004").await;
    assert_eq!(
        rows(&c, "SELECT * FROM g2 ORDER BY id").await,
        ["1|3|1.01|30|3.030|1.01/3", "2|3|0.67|30|2.010|0.67/3"]
    );
}
