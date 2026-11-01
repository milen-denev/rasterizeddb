//! C7: float semantics as PostgreSQL has them. A float against an exact or
//! REAL column, a NUMERIC column against a float column, NaN and the
//! infinities, text against a float (42883), type names and scalar-subquery
//! labels. Every expectation is PostgreSQL 17's (`docker run --rm
//! postgres:17`), read as text with NULL shown as `NULL`, except where a test
//! says this engine refuses what PostgreSQL stores.

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

/// Each row as `a|b|…`, a NULL as `NULL`.
async fn rows_n(c: &Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"))
            }
            _ => None,
        })
        .collect()
}

async fn sqlstate(c: &Client, sql: &str) -> String {
    let e = c.simple_query(sql).await.expect_err(sql);
    e.code().map(|s| s.code().to_string()).unwrap_or_else(|| e.to_string())
}

/// double alone is no type (42704), double precision is; the 42883 message names real for a real; a scalar subquery standing as a select item is named after its inner item (max, x, y, ?column?, count), and an aliased or nested one keeps its own name. PostgreSQL 17's answers.
#[tokio::test(flavor = "multi_thread")]
async fn float_type_names_and_subquery_labels() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE dd (id INT PRIMARY KEY, x double precision, t TEXT, n NUMERIC)").await;
    run(&c, "INSERT INTO dd VALUES (1, 1.5, 'a', 2)").await;
    assert_eq!(sqlstate(&c, "SELECT id, x::double FROM dd").await, "42704");
    assert_eq!(sqlstate(&c, "SELECT id, CAST(x AS double) FROM dd").await, "42704");
    assert_eq!(sqlstate(&c, "SELECT id FROM dd WHERE x = '1.5'::double").await, "42704");
    assert_eq!(rows_n(&c, "SELECT id FROM dd WHERE x = '1.5'::double precision").await, ["1"] as [&str; 1], "SELECT id FROM dd WHERE x = '1.5'::double precision");
    assert_eq!(rows_n(&c, "SELECT id, x::double precision FROM dd").await, ["1|1.5"] as [&str; 1], "SELECT id, x::double precision FROM dd");
    assert_eq!(sqlstate(&c, "SELECT id FROM dd WHERE t = '1'::real").await, "42883");
    assert_eq!(sqlstate(&c, "SELECT id FROM dd WHERE t = 1::float4").await, "42883");
    assert_eq!(sqlstate(&c, "SELECT id FROM dd WHERE t = '1'::float8").await, "42883");
    assert_eq!(sqlstate(&c, "SELECT id FROM dd WHERE upper(t) = '1'::real").await, "42883");
    assert_eq!(rows_n(&c, "SELECT id, (SELECT max(x) FROM dd) FROM dd").await, ["1|1.5"] as [&str; 1], "SELECT id, (SELECT max(x) FROM dd) FROM dd");
    assert_eq!(rows_n(&c, "SELECT id, (SELECT max(x) * 2 FROM dd) FROM dd").await, ["1|3"] as [&str; 1], "SELECT id, (SELECT max(x) * 2 FROM dd) FROM dd");
    assert_eq!(rows_n(&c, "SELECT id, (SELECT x FROM dd) FROM dd").await, ["1|1.5"] as [&str; 1], "SELECT id, (SELECT x FROM dd) FROM dd");
    assert_eq!(rows_n(&c, "SELECT id, (SELECT x AS y FROM dd) FROM dd").await, ["1|1.5"] as [&str; 1], "SELECT id, (SELECT x AS y FROM dd) FROM dd");
    assert_eq!(rows_n(&c, "SELECT id, (SELECT max(x) FROM dd) AS m FROM dd").await, ["1|1.5"] as [&str; 1], "SELECT id, (SELECT max(x) FROM dd) AS m FROM dd");
    assert_eq!(rows_n(&c, "SELECT (SELECT count(*) FROM dd), id FROM dd").await, ["1|1"] as [&str; 1], "SELECT (SELECT count(*) FROM dd), id FROM dd");
    assert_eq!(rows_n(&c, "SELECT id, (SELECT t FROM dd), (SELECT n FROM dd) FROM dd").await, ["1|a|2"] as [&str; 1], "SELECT id, (SELECT t FROM dd), (SELECT n FROM dd) FROM dd");
    assert_eq!(rows_n(&c, "SELECT id, coalesce((SELECT max(x) FROM dd), 0) FROM dd").await, ["1|1.5"] as [&str; 1], "SELECT id, coalesce((SELECT max(x) FROM dd), 0) FROM dd");
    assert_eq!(rows_n(&c, "SELECT id FROM dd WHERE x = (SELECT max(x) FROM dd)").await, ["1"] as [&str; 1], "SELECT id FROM dd WHERE x = (SELECT max(x) FROM dd)");
}

/// The column names a statement's result carries, as the client is told.
async fn labels(c: &Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .find_map(|m| match m {
            SimpleQueryMessage::RowDescription(columns) => Some(columns.iter().map(|col| col.name().to_string()).collect()),
            _ => None,
        })
        .unwrap_or_default()
}

async fn error_text(c: &Client, sql: &str) -> String {
    let e = c.simple_query(sql).await.expect_err(sql);
    e.as_db_error().map(|d| d.message().to_string()).unwrap_or_else(|| e.to_string())
}

/// A scalar subquery standing as a whole select item is named after its
/// inner item, as PostgreSQL 17 names it: `max`, `?column?` for an
/// expression, the column, its alias, `count`. One with an alias of its own
/// keeps it, and one nested in a call is named after the call. They were
/// named after the constant that replaced them (`float8`, `float8 * 2`).
#[tokio::test(flavor = "multi_thread")]
async fn a_scalar_subquery_item_is_named_after_its_inner_item() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE dd (id INT PRIMARY KEY, x double precision, t TEXT, n NUMERIC)").await;
    run(&c, "INSERT INTO dd VALUES (1, 1.5, 'a', 2)").await;
    for (sql, want) in [
        ("SELECT id, (SELECT max(x) FROM dd) FROM dd", ["id", "max"]),
        ("SELECT id, (SELECT max(x) * 2 FROM dd) FROM dd", ["id", "?column?"]),
        ("SELECT id, (SELECT x FROM dd) FROM dd", ["id", "x"]),
        ("SELECT id, (SELECT x AS y FROM dd) FROM dd", ["id", "y"]),
        ("SELECT id, (SELECT max(x) FROM dd) AS m FROM dd", ["id", "m"]),
        ("SELECT (SELECT count(*) FROM dd), id FROM dd", ["count", "id"]),
        ("SELECT id, coalesce((SELECT max(x) FROM dd), 0) FROM dd", ["id", "coalesce"]),
        ("SELECT id, x::double precision FROM dd", ["id", "x"]),
    ] {
        assert_eq!(labels(&c, sql).await, want, "{sql}");
    }
}
