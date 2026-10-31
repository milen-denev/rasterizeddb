//! A `double precision` or `real` value's text is PostgreSQL's: the shortest
//! digits that read back as the same float, with an exponent past fifteen
//! digits (six for `real`) or below 10^-4 — `1e+20`, `1e-07`, `3.4e+38` — and
//! `NaN`, `Infinity`, `-Infinity` by name.
//!
//! The engine wrote Rust's rendering, which never uses an exponent: `1e20` was
//! `100000000000000000000` and `3.4e38::real` a 39-digit string, on the wire
//! and in `::text`, so a `varchar(6)` that takes PostgreSQL's `1e+20` refused
//! the value; and `::text` of an infinity was `inf`. Every expectation is
//! PostgreSQL 17's (`docker run --rm postgres:17`), read as text.

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

async fn seeded(c: &Client) {
    for sql in [
        "CREATE TABLE fl (id INT PRIMARY KEY, f FLOAT8, r REAL, t TEXT, vc VARCHAR(6), c CHAR(6))",
        "INSERT INTO fl VALUES (1, 1e20, 3.4e38, 'x', 'x', 'x')",
        "INSERT INTO fl VALUES (2, 1e-7, 1e-7, 'x', 'x', 'x')",
        "INSERT INTO fl VALUES (3, '-0', 'NaN', 'x', 'x', 'x')",
        "INSERT INTO fl VALUES (4, 'Infinity', '-Infinity', 'x', 'x', 'x')",
        "INSERT INTO fl VALUES (5, 0.1, 0.1, 'x', 'x', 'x')",
        "INSERT INTO fl VALUES (6, 123456789012345, 1e6, 'x', 'x', 'x')",
    ] {
        run(c, sql).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn floats_read_as_postgresql_writes_them() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    seeded(&c).await;
    // The wire, `::text`, a typmod cast and a concatenation all agree.
    assert_eq!(
        rows(&c, "SELECT id, f, r, f::text, r::text, f::varchar(6), 'v=' || f FROM fl ORDER BY id").await,
        [
            "1|1e+20|3.4e+38|1e+20|3.4e+38|1e+20|v=1e+20",
            "2|1e-07|1e-07|1e-07|1e-07|1e-07|v=1e-07",
            "3|-0|NaN|-0|NaN|-0|v=-0",
            "4|Infinity|-Infinity|Infinity|-Infinity|Infini|v=Infinity",
            "5|0.1|0.1|0.1|0.1|0.1|v=0.1",
            "6|123456789012345|1e+06|123456789012345|1e+06|123456|v=123456789012345",
        ]
    );
    assert_eq!(rows(&c, "SELECT id FROM fl WHERE f::text = '1e+20'").await, ["1"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_float_stored_as_text_is_postgresqls_text() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    seeded(&c).await;
    run(&c, "UPDATE fl SET t = f WHERE id > 0").await;
    assert_eq!(
        rows(&c, "SELECT id, t FROM fl ORDER BY id").await,
        ["1|1e+20", "2|1e-07", "3|-0", "4|Infinity", "5|0.1", "6|123456789012345"]
    );
    run(&c, "UPDATE fl SET t = r WHERE id > 0").await;
    assert_eq!(
        rows(&c, "SELECT id, t FROM fl ORDER BY id").await,
        ["1|3.4e+38", "2|1e-07", "3|NaN", "4|-Infinity", "5|0.1", "6|1e+06"]
    );
    // Six characters, which `varchar(6)` and `char(6)` take.
    run(&c, "UPDATE fl SET vc = f, c = f WHERE id = 1").await;
    run(&c, "UPDATE fl SET vc = r WHERE id = 2").await;
    assert_eq!(rows(&c, "SELECT id, vc, c FROM fl WHERE id < 3 ORDER BY id").await, ["1|1e+20|1e+20 ", "2|1e-07|x     "]);
    run(&c, "CREATE TABLE ft (id INT PRIMARY KEY, t TEXT)").await;
    run(&c, "INSERT INTO ft SELECT id, f FROM fl").await;
    assert_eq!(
        rows(&c, "SELECT id, t FROM ft ORDER BY id").await,
        ["1|1e+20", "2|1e-07", "3|-0", "4|Infinity", "5|0.1", "6|123456789012345"]
    );
    // And into a float column, the same floats.
    run(&c, "CREATE TABLE ff (id INT PRIMARY KEY, f FLOAT8, i INT)").await;
    run(&c, "INSERT INTO ff SELECT id, f, 2 FROM fl").await;
    assert_eq!(
        rows(&c, "SELECT id, f FROM ff ORDER BY id").await,
        ["1|1e+20", "2|1e-07", "3|-0", "4|Infinity", "5|0.1", "6|123456789012345"]
    );
}

/// A float scalar subquery is a float: substituted back as a bare constant,
/// `(SELECT max(f) …)` over a float8 column became a `numeric` constant, so
/// the column was described as numeric (1700) where PostgreSQL 17 describes
/// float8 (701) and float4 (700), and a driver decoding f64 refused it.
/// (Read through the simple protocol: an extended-protocol Describe of a
/// statement holding a scalar subquery answers NoData, before and after.)
#[tokio::test(flavor = "multi_thread")]
async fn a_float_scalar_subquery_is_described_as_a_float() {
    use sqlx::{AssertSqlSafe, Row};
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE t (id INT PRIMARY KEY, f FLOAT8, r REAL)").await;
    run(&c, "INSERT INTO t VALUES (1, 0.1, 0.25), (2, 1e20, 3.4e38)").await;
    let pool = server.connect_sqlx().await.expect("sqlx");
    let row = sqlx::raw_sql(AssertSqlSafe("SELECT (SELECT max(f) FROM t), (SELECT min(r) FROM t) FROM t WHERE id = 1".to_string()))
        .fetch_one(&pool)
        .await
        .expect("row");
    assert_eq!(row.try_get::<f64, _>(0).expect("described float8"), 1e20);
    assert_eq!(row.try_get::<f32, _>(1).expect("described float4"), 0.25);
    // And it still compares and stores as the float it is.
    assert_eq!(rows(&c, "SELECT id FROM t WHERE f > (SELECT min(f) FROM t)").await, ["2"]);
    run(&c, "UPDATE t SET f = (SELECT max(f) FROM t) WHERE id = 1").await;
    assert_eq!(rows(&c, "SELECT id, f FROM t ORDER BY id").await, ["1|1e+20", "2|1e+20"]);
}
