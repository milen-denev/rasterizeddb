//! A bound finer than a `NUMERIC(p, s)` column's scale, written with an
//! exponent or sent as a float parameter, never prunes the rows it matches.
//!
//! A float's text is PostgreSQL's now (`1e-05`). The filter index decided
//! whether a bound fits the column's scale by counting the digits after its
//! point, and `1e-05` has none: `a < 0.00001` over a NUMERIC(10,2) was
//! bounded as `a < 0`, every zone holding 0.00 was pruned, and the count was
//! 0 where PostgreSQL 17 (and the engine before) counts every 0.00 row. An
//! UPDATE or DELETE with that predicate touched nothing and reported success.

use rasterizeddb_compat::TestServer;
use tokio_postgres::{Client, SimpleQueryMessage};

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

async fn run(c: &Client, sql: &str) -> u64 {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .find_map(|m| match m {
            SimpleQueryMessage::CommandComplete(n) => Some(n),
            _ => None,
        })
        .unwrap_or(0)
}

async fn count(c: &Client, predicate: &str) -> i64 {
    let sql = format!("SELECT count(*) FROM sg WHERE {predicate}");
    c.query_one(sql.as_str(), &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}")).get(0)
}

/// 5000 rows, every other one 0.00 and the rest `id + 0.25`.
async fn seeded(c: &Client) {
    run(c, "CREATE TABLE sg (id INT PRIMARY KEY, a NUMERIC(10,2))").await;
    for chunk in 0..10 {
        let values: Vec<String> = (1..=500)
            .map(|i| {
                let id = chunk * 500 + i;
                if id % 2 == 0 { format!("({id}, 0.00)") } else { format!("({id}, {id}.25)") }
            })
            .collect();
        run(c, &format!("INSERT INTO sg VALUES {}", values.join(","))).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bound_finer_than_the_scale_keeps_its_rows() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    seeded(&c).await;
    // PostgreSQL 17's counts for each.
    for (predicate, pg) in [
        ("a < 0.00001", 2500),
        ("a < 1e-5", 2500),
        ("a < 0.00009", 2500),
        ("a <= 0.00001", 2500),
        ("a > -0.00001", 5000),
        ("a < 0.0000001", 2500),
        ("a BETWEEN -1e-5 AND 1e-5", 2500),
    ] {
        assert_eq!(count(&c, predicate).await, pg, "{predicate}");
    }
    // A float8 parameter, sent in binary, is the same bound.
    let st = c
        .prepare_typed("SELECT count(*) FROM sg WHERE a < $1", &[tokio_postgres::types::Type::FLOAT8])
        .await
        .expect("prepare");
    let n: i64 = c.query_one(&st, &[&0.00001f64]).await.expect("param").get(0);
    assert_eq!(n, 2500, "a < $1 (float8 0.00001)");
    // And a write over it touches the rows it matches.
    assert_eq!(run(&c, "UPDATE sg SET a = a + 1 WHERE a < 0.00001").await, 2500);
    assert_eq!(count(&c, "a = 1").await, 2500);
    assert_eq!(run(&c, "DELETE FROM sg WHERE a > 0.99999 AND a < 1.00001").await, 2500);
    assert_eq!(count(&c, "a >= 0").await, 2500);
}
