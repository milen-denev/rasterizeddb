//! An unquoted numeric constant written into a text column is stored as its
//! value's text, as PostgreSQL's assignment renders it — not as it was
//! spelled. Each expectation is PostgreSQL 17's (`docker run --rm
//! postgres:17`).
//!
//! `1e1` was stored as `'1e1'` (PostgreSQL: `'10'`), `+9` as `'+9'` (`'9'`),
//! `007` as `'007'` (`'7'`), and the statement reported success.

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

/// `(k, v)` per row, ordered by `k`.
async fn pairs(c: &Client, sql: &str) -> Vec<(String, String)> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some((r.get(0).unwrap().to_string(), r.get(1).unwrap_or("NULL").to_string())),
            _ => None,
        })
        .collect()
}

/// `(constant as written, what PostgreSQL 17 stores)`.
const CASES: &[(&str, &str)] = &[
    ("1e1", "10"),
    ("+9", "9"),
    ("1.50", "1.50"),
    ("007", "7"),
    ("-0", "0"),
    ("-0.0", "0.0"),
    ("1.5e-3", "0.0015"),
    ("1.50e1", "15.0"),
    (".5", "0.5"),
    ("5.", "5"),
    ("00.10", "0.10"),
    ("-007", "-7"),
    ("1E+2", "100"),
    ("99999999999999999999", "99999999999999999999"),
    ("0x1F", "31"),
    ("1_000", "1000"),
    ("100e-2", "1.00"),
    ("-1e1", "-10"),
];

fn expected() -> Vec<(String, String)> {
    CASES.iter().enumerate().map(|(k, (_, pg))| (k.to_string(), pg.to_string())).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_stores_the_constants_value_as_text() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE t (k INT NOT NULL, v TEXT, w VARCHAR(40))").await;
    // One row at a time, and all of them in one multi-row VALUES.
    for (k, (raw, _)) in CASES.iter().enumerate() {
        run(&c, &format!("INSERT INTO t VALUES ({k}, {raw}, {raw})")).await;
    }
    assert_eq!(pairs(&c, "SELECT k, v FROM t ORDER BY k").await, expected());
    assert_eq!(pairs(&c, "SELECT k, w FROM t ORDER BY k").await, expected());
    run(&c, "DELETE FROM t").await;
    let rows: Vec<String> = CASES.iter().enumerate().map(|(k, (raw, _))| format!("({k}, {raw}, {raw})")).collect();
    run(&c, &format!("INSERT INTO t VALUES {}", rows.join(", "))).await;
    assert_eq!(pairs(&c, "SELECT k, v FROM t ORDER BY k").await, expected());
    // A quoted string is still its spelling.
    run(&c, "INSERT INTO t VALUES (100, '1e1', '+9')").await;
    assert_eq!(pairs(&c, "SELECT v, w FROM t WHERE k = 100").await, [("1e1".to_string(), "+9".to_string())]);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_upsert_and_default_render_it_too() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE t (k INT PRIMARY KEY, v TEXT)").await;
    for (k, _) in CASES.iter().enumerate() {
        run(&c, &format!("INSERT INTO t VALUES ({k}, 'x')")).await;
    }
    for (k, (raw, _)) in CASES.iter().enumerate() {
        run(&c, &format!("UPDATE t SET v = {raw} WHERE k = {k}")).await;
    }
    assert_eq!(pairs(&c, "SELECT k, v FROM t ORDER BY k").await, expected());

    run(&c, "INSERT INTO t VALUES (100, 'x') ON CONFLICT (k) DO UPDATE SET v = 1e1").await;
    run(&c, "INSERT INTO t VALUES (100, 'x') ON CONFLICT (k) DO UPDATE SET v = +9").await;
    run(&c, "INSERT INTO t VALUES (101, 2e1), (102, +3) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v").await;
    assert_eq!(
        pairs(&c, "SELECT k, v FROM t WHERE k >= 100 ORDER BY k").await,
        [("100", "9"), ("101", "20"), ("102", "3")].map(|(k, v)| (k.to_string(), v.to_string()))
    );

    // PostgreSQL 17: `DEFAULT 1e1` and `DEFAULT +9` on text columns store
    // '10' and '9', omitted or written as DEFAULT.
    run(&c, "CREATE TABLE d (k INT, a TEXT DEFAULT 1e1, b TEXT DEFAULT +9, n INT DEFAULT 1e1)").await;
    run(&c, "INSERT INTO d (k) VALUES (1)").await;
    run(&c, "INSERT INTO d VALUES (2, DEFAULT, DEFAULT, DEFAULT)").await;
    assert_eq!(
        pairs(&c, "SELECT a, b FROM d ORDER BY k").await,
        [("10", "9"), ("10", "9")].map(|(a, b)| (a.to_string(), b.to_string()))
    );
    assert_eq!(pairs(&c, "SELECT k, n FROM d ORDER BY k").await, [("1", "10"), ("2", "10")].map(|(a, b)| (a.to_string(), b.to_string())));
}
