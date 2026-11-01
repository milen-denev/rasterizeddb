//! An unconstrained `NUMERIC` column keeps each value at its own scale, every
//! digit as PostgreSQL stores it, on every path into it. (A column created
//! before cells kept their scale stays at nine places and refuses a value with
//! a nonzero digit past them until it is converted; those are the core's
//! `numeric_own_scale_tests`.)
//!
//! Every stored expectation is PostgreSQL 17's value (`docker run --rm
//! postgres:17`), read back through `::text`.

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

/// A NUMERIC(20,12) 0.200000000000 copied into an unconstrained column keeps
/// its twelve places on every path, as PostgreSQL shows it.
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
        [
            "1|0.200000000000",
            "2|1.50000000000000000000",
            "6|0.200000000000",
            "7|0.200000000000",
            "9|0.200000000000",
            "10|0.200000000000",
            "12|1.50000000000000000000",
            "13|0.200000000000",
            "14|0.200000000000"
        ]
    );
}

/// Every path into an unconstrained column stores the value PostgreSQL does,
/// every digit of it: INSERT … SELECT of a float quotient, a float cast, a
/// float product, a quotient at `select_div_scale`'s places, a generated
/// column, and an ON CONFLICT … DO UPDATE of `excluded.u / 3`. Each was
/// refused (0A000) while the column kept nine places, or before that rounded
/// to them in silence.
#[tokio::test(flavor = "multi_thread")]
async fn every_path_into_an_unconstrained_column_follows_one_rule() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE src (id INT PRIMARY KEY, b NUMERIC(20,12), f FLOAT8, r REAL)").await;
    run(&c, "INSERT INTO src VALUES (1, 1.123456789012, 0.1, 0.25), (2, 0.5, 0.2, 0.5)").await;
    run(&c, "CREATE TABLE dst (id INT PRIMARY KEY, u NUMERIC)").await;
    run(&c, "INSERT INTO dst VALUES (1, 0), (8, 1)").await;
    for sql in [
        "INSERT INTO dst (id, u) SELECT 2, f / 3 FROM src WHERE id = 1",
        "INSERT INTO dst (id, u) SELECT 3, b FROM src WHERE id = 1",
        "INSERT INTO dst (id, u) VALUES (4, 0.1::float8 / 3)",
        "INSERT INTO dst (id, u) VALUES (5, 1.0000000000001)",
    ] {
        run(&c, sql).await;
    }
    for (sql, id, want) in [
        ("UPDATE dst SET u = (SELECT b FROM src WHERE id = 1) WHERE id = 1", 1, "1.123456789012"),
        ("UPDATE dst SET u = (SELECT f * 1.0000000001 FROM src WHERE id = 1) WHERE id = 1", 1, "0.10000000001"),
        ("UPDATE dst SET u = (SELECT b::float8 FROM src WHERE id = 1) WHERE id = 1", 1, "1.123456789012"),
        ("UPDATE dst SET u = u / 3 WHERE id = 8", 8, "0.33333333333333333333"),
        ("INSERT INTO dst (id, u) VALUES (8, 1) ON CONFLICT (id) DO UPDATE SET u = excluded.u / 3", 8, "0.33333333333333333333"),
    ] {
        run(&c, sql).await;
        assert_eq!(rows(&c, &format!("SELECT u::text FROM dst WHERE id = {id}")).await, [want], "{sql}");
    }
    // Past what this engine carries: `u * 1e30` of a 20-place value has some
    // 50 digits (PostgreSQL's 0.11111111111111111111 goes through them), and
    // is refused whole rather than rounded.
    let sql = "UPDATE dst SET u = u * 1e30 / 3e30 WHERE id = 8";
    let e = c.simple_query(sql).await.expect_err(sql);
    assert_eq!(e.code().map(|s| s.code().to_string()), Some("0A000".to_string()), "{sql}");
    assert_eq!(rows(&c, "SELECT u::text FROM dst WHERE id = 8").await, ["0.33333333333333333333"]);
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
        [
            "1|0.25",
            "2|0.0333333333333333",
            "3|1.123456789012",
            "4|0.0333333333333333",
            "5|1.0000000000001",
            "8|0.25000000000000000000",
            "10|0.1",
            "11|0.500000000000",
            "12|0.5",
            "13|1.00000000000000"
        ]
    );
    // A generated column keeps every digit too, and a declared scale still
    // rounds from every digit.
    run(
        &c,
        "CREATE TABLE gen (id INT PRIMARY KEY, x NUMERIC(10,2), y NUMERIC GENERATED ALWAYS AS (x / 4) STORED, \
         z NUMERIC(20,12) GENERATED ALWAYS AS (x / 3) STORED)",
    )
    .await;
    run(&c, "INSERT INTO gen (id, x) VALUES (1, 3.00)").await;
    assert_eq!(rows(&c, "SELECT y::text, z::text FROM gen").await, ["0.75000000000000000000|1.000000000000"]);
    run(&c, "CREATE TABLE gen3 (id INT PRIMARY KEY, x NUMERIC(10,2), y NUMERIC GENERATED ALWAYS AS (x / 3) STORED)").await;
    run(&c, "INSERT INTO gen3 (id, x) VALUES (1, 1.00)").await;
    assert_eq!(rows(&c, "SELECT y::text FROM gen3").await, ["0.33333333333333333333"]);
}
