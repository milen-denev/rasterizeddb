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
    // '10' and '9', omitted or written as DEFAULT. (A number column's
    // default keeps the plain spellings only: see the test below.)
    run(&c, "CREATE TABLE d (k INT, a TEXT DEFAULT 1e1, b TEXT DEFAULT +9)").await;
    run(&c, "INSERT INTO d (k) VALUES (1)").await;
    run(&c, "INSERT INTO d VALUES (2, DEFAULT, DEFAULT)").await;
    assert_eq!(
        pairs(&c, "SELECT a, b FROM d ORDER BY k").await,
        [("10", "9"), ("10", "9")].map(|(a, b)| (a.to_string(), b.to_string()))
    );
}

/// A number default a column cannot hold is refused, when the table is
/// created or when a row needs it, never stored as zero. These spellings
/// stored 0 on every insert that left the column out (PostgreSQL 17 refuses
/// them with 22003).
#[tokio::test(flavor = "multi_thread")]
async fn a_number_default_the_column_cannot_hold_is_never_a_zero() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    for (i, decl) in [
        "INT DEFAULT 1.5e0",
        "INT DEFAULT 1e10",
        "SMALLINT DEFAULT 1e5",
        "SMALLINT DEFAULT 100000",
        "DOUBLE PRECISION DEFAULT 1e400",
        "NUMERIC(5,2) DEFAULT 1e10",
        "NUMERIC(5,2) DEFAULT 10000000000",
        "SMALLINT DEFAULT 40000.5",
    ]
    .iter()
    .enumerate()
    {
        let t = format!("dz{i}");
        if c.simple_query(&format!("CREATE TABLE {t} (k INT, a {decl})")).await.is_err() {
            continue;
        }
        let stored = c.simple_query(&format!("INSERT INTO {t} (k) VALUES (1)")).await;
        if stored.is_ok() {
            let got = pairs(&c, &format!("SELECT k, a FROM {t}")).await;
            panic!("{decl}: the insert stored {got:?}");
        }
    }
}

/// A fraction defaulting an integer column is rounded, half away from zero,
/// as PostgreSQL's numeric to integer assignment rounds it. PostgreSQL 17:
/// 2, 3, -2, 5, 3, -3.
#[tokio::test(flavor = "multi_thread")]
async fn a_fractional_default_on_an_integer_column_is_rounded() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(
        &c,
        "CREATE TABLE di (k INT, a INT DEFAULT 1.5, b INT DEFAULT 2.5, c INT DEFAULT -1.5, \
         d INT DEFAULT 5.0, e SMALLINT DEFAULT 2.5, f BIGINT DEFAULT -2.5)",
    )
    .await;
    run(&c, "INSERT INTO di (k) VALUES (1)").await;
    run(&c, "INSERT INTO di VALUES (2, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT)").await;
    let rows = c.simple_query("SELECT a, b, c, d, e, f FROM di ORDER BY k").await.expect("select");
    let got: Vec<Vec<String>> = rows
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some((0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect()),
            _ => None,
        })
        .collect();
    let want: Vec<String> = ["2", "3", "-2", "5", "3", "-3"].map(String::from).to_vec();
    assert_eq!(got, vec![want.clone(), want]);
}

// `SET v = DEFAULT` in an UPDATE or an upsert stores what an INSERT's
// default stores: the rounded integer, and the constant's own text in a
// TEXT column. Both used to refuse the fractional default with 22P02.
#[tokio::test]
async fn set_to_default_stores_what_an_insert_would() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE r (k INT PRIMARY KEY, v INT DEFAULT 1.5, s TEXT DEFAULT 007, b BOOLEAN DEFAULT true)").await;
    run(&c, "INSERT INTO r VALUES (1, 9, 'x', false), (2, 9, 'x', false)").await;
    c.simple_query("UPDATE r SET v = DEFAULT, s = DEFAULT, b = DEFAULT WHERE k = 1").await.expect("update set default");
    assert_eq!(pairs(&c, "SELECT k, b FROM r ORDER BY k").await, [("1".to_string(), "t".to_string()), ("2".to_string(), "f".to_string())]);
    c.simple_query("INSERT INTO r VALUES (2, 9, 'x', false) ON CONFLICT (k) DO UPDATE SET v = DEFAULT")
        .await
        .expect("upsert set default");
    assert_eq!(
        pairs(&c, "SELECT v, s FROM r ORDER BY k").await,
        [("2".to_string(), "7".to_string()), ("2".to_string(), "x".to_string())]
    );
}
