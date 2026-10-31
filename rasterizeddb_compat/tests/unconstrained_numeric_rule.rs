//! An unconstrained `NUMERIC` column keeps nine places here (until values
//! are stored at their own precision). The rule meanwhile, the same on every
//! path into one: a value exact at nine places — any digits past them zeros —
//! is stored; one with a nonzero digit past them is refused (0A000), never
//! rounded in silence.
//!
//! Every stored expectation is PostgreSQL 17's value (`docker run --rm
//! postgres:17`); its display keeps the source's places (0.200000000000),
//! which this storage cannot, so values are read back through `::text`.

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

async fn code(c: &Client, sql: &str) -> String {
    let e = c.simple_query(sql).await.expect_err(sql);
    e.code().map(|s| s.code().to_string()).unwrap_or_else(|| e.to_string())
}

/// A NUMERIC(20,12) 0.200000000000 has nothing past nine places but zeros:
/// every path that copies it into an unconstrained column stores 0.2. It
/// was refused with 22003, "does not fit the column's precision".
#[tokio::test(flavor = "multi_thread")]
async fn an_exact_value_with_more_declared_places_is_stored() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE s2 (id INT PRIMARY KEY, w NUMERIC(20,12), x NUMERIC(38,20))").await;
    run(&c, "INSERT INTO s2 VALUES (1, 0.200000000000, 1.5)").await;
    run(&c, "CREATE TABLE d2 (id INT PRIMARY KEY, u NUMERIC)").await;
    for sql in [
        "INSERT INTO d2 (id, u) SELECT 1, w FROM s2",
        "INSERT INTO d2 (id, u) SELECT 2, x FROM s2",
        "INSERT INTO d2 (id, u) VALUES (6, 0.200000000000)",
        "INSERT INTO d2 (id, u) VALUES (7, (SELECT w FROM s2))",
        "INSERT INTO d2 (id, u) SELECT 9, w * 1 FROM s2",
        "INSERT INTO d2 (id, u) SELECT 10, w + 0 FROM s2",
        "INSERT INTO d2 (id, u) VALUES (12, 0)",
        "UPDATE d2 SET u = (SELECT x FROM s2) WHERE id = 12",
        "INSERT INTO d2 (id, u) SELECT 13, w FROM (SELECT w FROM s2) z",
        "WITH c AS (SELECT w FROM s2) INSERT INTO d2 (id, u) SELECT 14, w FROM c",
    ] {
        run(&c, sql).await;
    }
    assert_eq!(
        rows(&c, "SELECT id, u::text FROM d2 ORDER BY id").await,
        ["1|0.2", "2|1.5", "6|0.2", "7|0.2", "9|0.2", "10|0.2", "12|1.5", "13|0.2", "14|0.2"]
    );
}
