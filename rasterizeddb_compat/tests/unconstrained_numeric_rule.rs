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

/// Every path into an unconstrained column answers alike: a value with a
/// nonzero digit past nine places is refused (PostgreSQL 17 stores it whole,
/// which this storage cannot yet), and one exact at nine is stored as
/// PostgreSQL stores it. Several of these paths rounded to nine places in
/// silence: INSERT … SELECT of a float quotient (`f / 3` stored
/// 0.033333333), a float cast (`b::float8`), a float product, a generated
/// column (`x / 3` stored 0.333333333), and an ON CONFLICT … DO UPDATE whose
/// `excluded.u / 3` was folded at nine places before it was stored.
#[tokio::test(flavor = "multi_thread")]
async fn every_path_into_an_unconstrained_column_follows_one_rule() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE src (id INT PRIMARY KEY, b NUMERIC(20,12), f FLOAT8, r REAL)").await;
    run(&c, "INSERT INTO src VALUES (1, 1.123456789012, 0.1, 0.25), (2, 0.5, 0.2, 0.5)").await;
    run(&c, "CREATE TABLE dst (id INT PRIMARY KEY, u NUMERIC)").await;
    run(&c, "INSERT INTO dst VALUES (1, 0), (8, 1)").await;
    // Refused: a nonzero digit past nine places.
    for sql in [
        "INSERT INTO dst (id, u) SELECT 2, f / 3 FROM src WHERE id = 1",
        "INSERT INTO dst (id, u) SELECT 3, b FROM src WHERE id = 1",
        "INSERT INTO dst (id, u) VALUES (4, 0.1::float8 / 3)",
        "INSERT INTO dst (id, u) VALUES (5, 1.0000000000001)",
        "UPDATE dst SET u = (SELECT b FROM src WHERE id = 1) WHERE id = 1",
        "UPDATE dst SET u = (SELECT f * 1.0000000001 FROM src WHERE id = 1) WHERE id = 1",
        "UPDATE dst SET u = (SELECT b::float8 FROM src WHERE id = 1) WHERE id = 1",
        "UPDATE dst SET u = u / 3 WHERE id = 8",
        // Through the older exact reading (a constant carried as a float).
        "UPDATE dst SET u = u * 1e30 / 3e30 WHERE id = 8",
        "INSERT INTO dst (id, u) VALUES (8, 1) ON CONFLICT (id) DO UPDATE SET u = excluded.u / 3",
    ] {
        assert_eq!(code(&c, sql).await, "0A000", "{sql}");
    }
    assert_eq!(rows(&c, "SELECT id, u::text FROM dst ORDER BY id").await, ["1|0", "8|1"], "a refusal wrote");
    // Stored: exact at nine places.
    for sql in [
        "INSERT INTO dst (id, u) SELECT 10, f FROM src WHERE id = 1",
        "INSERT INTO dst (id, u) SELECT 11, b FROM src WHERE id = 2",
        "INSERT INTO dst (id, u) VALUES (12, 0.5::float8)",
        "INSERT INTO dst (id, u) VALUES (13, 1.00000000000000)",
        "UPDATE dst SET u = (SELECT r FROM src WHERE id = 1) WHERE id = 1",
        "INSERT INTO dst (id, u) VALUES (8, 1) ON CONFLICT (id) DO UPDATE SET u = excluded.u / 4",
    ] {
        run(&c, sql).await;
    }
    assert_eq!(
        rows(&c, "SELECT id, u::text FROM dst ORDER BY id").await,
        ["1|0.25", "8|0.25", "10|0.1", "11|0.5", "12|0.5", "13|1"]
    );
    // A generated column: refused where the value has digits past nine, and
    // a declared scale still rounds from every digit.
    run(
        &c,
        "CREATE TABLE gen (id INT PRIMARY KEY, x NUMERIC(10,2), y NUMERIC GENERATED ALWAYS AS (x / 4) STORED, \
         z NUMERIC(20,12) GENERATED ALWAYS AS (x / 3) STORED)",
    )
    .await;
    run(&c, "INSERT INTO gen (id, x) VALUES (1, 3.00)").await;
    assert_eq!(rows(&c, "SELECT y::text, z::text FROM gen").await, ["0.75|1.000000000000"]);
    run(&c, "CREATE TABLE gen3 (id INT PRIMARY KEY, x NUMERIC(10,2), y NUMERIC GENERATED ALWAYS AS (x / 3) STORED)").await;
    assert_eq!(code(&c, "INSERT INTO gen3 (id, x) VALUES (1, 1.00)").await, "0A000");
}
