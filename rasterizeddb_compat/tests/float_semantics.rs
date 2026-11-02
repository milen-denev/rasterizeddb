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

/// A float compared with a column finer than a double (NUMERIC(38,20), BIGINT past 2^53) reads the column's nearest double, also in an IN list mixing floats and numbers (float8, the list's type) and its NOT IN; a REAL column against a double or a decimal constant is widened, as PostgreSQL compares it (0.1f is not '0.1'::float8 and is greater). PostgreSQL 17's values.
#[tokio::test(flavor = "multi_thread")]
async fn a_float_list_over_a_finer_column_is_compared_as_float8() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE fn (id INT PRIMARY KEY, w NUMERIC(38,20), b BIGINT, f4 REAL, p NUMERIC(10,2))").await;
    run(&c, "INSERT INTO fn VALUES (1, 0.1, 9007199254740992, 0.1, 0.10), (2, 0.10000000000000000001, 9007199254740993, 0.2, 2.50), (3, 0.30000000000000004441, 9007199254740994, 0.30000001, 0.30), (4, 2.5, 1, 2.5, NULL)").await;
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE w = '0.1'::float8 ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM fn WHERE w = '0.1'::float8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE w IN ('0.1'::float8, '0.3'::float8) ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM fn WHERE w IN ('0.1'::float8, '0.3'::float8) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE w NOT IN ('0.1'::float8, '0.3'::float8) ORDER BY id").await, ["3", "4"] as [&str; 2], "SELECT id FROM fn WHERE w NOT IN ('0.1'::float8, '0.3'::float8) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE w IN ('0.1'::float8, 2.5) ORDER BY id").await, ["1", "2", "4"] as [&str; 3], "SELECT id FROM fn WHERE w IN ('0.1'::float8, 2.5) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE w NOT IN ('0.1'::float8, 2.5) ORDER BY id").await, ["3"] as [&str; 1], "SELECT id FROM fn WHERE w NOT IN ('0.1'::float8, 2.5) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE b = '9007199254740992'::float8 ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM fn WHERE b = '9007199254740992'::float8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE b IN ('9007199254740992'::float8, 5) ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM fn WHERE b IN ('9007199254740992'::float8, 5) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE b NOT IN ('9007199254740992'::float8, 5) ORDER BY id").await, ["3", "4"] as [&str; 2], "SELECT id FROM fn WHERE b NOT IN ('9007199254740992'::float8, 5) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE b IN ('9007199254740992'::float8, '1'::float8) ORDER BY id").await, ["1", "2", "4"] as [&str; 3], "SELECT id FROM fn WHERE b IN ('9007199254740992'::float8, '1'::float8) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE p IN ('0.1'::float8, 2.5) ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM fn WHERE p IN ('0.1'::float8, 2.5) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE p NOT IN ('0.1'::float8, 2.5) ORDER BY id").await, ["3"] as [&str; 1], "SELECT id FROM fn WHERE p NOT IN ('0.1'::float8, 2.5) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE f4 = '0.1'::float8 ORDER BY id").await, [] as [&str; 0], "SELECT id FROM fn WHERE f4 = '0.1'::float8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE f4 > '0.1'::float8 ORDER BY id").await, ["1", "2", "3", "4"] as [&str; 4], "SELECT id FROM fn WHERE f4 > '0.1'::float8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE f4 < '0.3'::float8 ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM fn WHERE f4 < '0.3'::float8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE f4 = 0.1 ORDER BY id").await, [] as [&str; 0], "SELECT id FROM fn WHERE f4 = 0.1 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE f4 = '0.1'::real ORDER BY id").await, ["1"] as [&str; 1], "SELECT id FROM fn WHERE f4 = '0.1'::real ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM fn WHERE f4 IN ('0.1'::float8, 2.5) ORDER BY id").await, ["4"] as [&str; 1], "SELECT id FROM fn WHERE f4 IN ('0.1'::float8, 2.5) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT count(*) FROM fn WHERE f4 = '0.1'::float8").await, ["0"] as [&str; 1], "SELECT count(*) FROM fn WHERE f4 = '0.1'::float8");
    assert_eq!(rows_n(&c, "SELECT count(*) FROM fn WHERE f4 > '0.1'::float8").await, ["4"] as [&str; 1], "SELECT count(*) FROM fn WHERE f4 > '0.1'::float8");
    assert_eq!(rows_n(&c, "SELECT id, f4 = '0.1'::float8, f4 > '0.1'::float8 FROM fn ORDER BY id").await, ["1|f|t", "2|f|t", "3|f|t", "4|f|t"] as [&str; 4], "SELECT id, f4 = '0.1'::float8, f4 > '0.1'::float8 FROM fn ORDER BY id");
}

/// Text (a column, a text function, a CASE of text, ||), varchar, char and timestamp values compared with a float: PostgreSQL has no operator (42883), in WHERE (=, IN, BETWEEN) and in a select list; the message names real for a ::real. PostgreSQL 17's answers.
#[tokio::test(flavor = "multi_thread")]
async fn text_against_a_float_is_42883() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE tx (id INT PRIMARY KEY, t TEXT, v VARCHAR(10), c CHAR(3), ts TIMESTAMP)").await;
    run(&c, "INSERT INTO tx VALUES (1, '1', '1', '1', '2000-01-01')").await;
    assert_eq!(sqlstate(&c, "SELECT id FROM tx WHERE t = '1'::float8").await, "42883");
    assert_eq!(sqlstate(&c, "SELECT id FROM tx WHERE upper(t) = '1'::float8").await, "42883");
    assert_eq!(sqlstate(&c, "SELECT id FROM tx WHERE CASE WHEN id > 0 THEN t ELSE 'x' END = '1'::float8").await, "42883");
    assert_eq!(sqlstate(&c, "SELECT id FROM tx WHERE t IN ('1'::float8, '2'::float8)").await, "42883");
    assert_eq!(sqlstate(&c, "SELECT id FROM tx WHERE t BETWEEN '0'::float8 AND '2'::float8").await, "42883");
    assert_eq!(sqlstate(&c, "SELECT id FROM tx WHERE v = '1'::float8").await, "42883");
    assert_eq!(sqlstate(&c, "SELECT id FROM tx WHERE c = '1'::float8").await, "42883");
    assert_eq!(sqlstate(&c, "SELECT id FROM tx WHERE t = '1'::real").await, "42883");
    assert_eq!(sqlstate(&c, "SELECT id FROM tx WHERE ts = '1'::float8").await, "42883");
    assert_eq!(sqlstate(&c, "SELECT id FROM tx WHERE t || 'a' = '1'::float8").await, "42883");
    assert_eq!(sqlstate(&c, "SELECT t = '1'::float8 FROM tx").await, "42883");
}

/// NaN and the infinities compared with integer, NUMERIC(p,s), unconstrained NUMERIC and FLOAT8 columns, as PostgreSQL orders them (NaN above every number): <, >, =, <>, IN and NOT IN answer rather than failing the plan. PostgreSQL 17's values.
#[tokio::test(flavor = "multi_thread")]
async fn nan_and_infinity_constants_against_exact_columns() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE nx (id INT PRIMARY KEY, i INT, b BIGINT, p NUMERIC(10,2), n NUMERIC, f FLOAT8)").await;
    run(&c, "INSERT INTO nx VALUES (1, 1, 1, 1.5, 1.5, 1.5), (2, -5, -5, -5, -5, -5)").await;
    assert_eq!(rows_n(&c, "SELECT id FROM nx WHERE i < 'Infinity'::float8 ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM nx WHERE i < 'Infinity'::float8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM nx WHERE i > '-Infinity'::float8 ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM nx WHERE i > '-Infinity'::float8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM nx WHERE i = 'NaN'::float8 ORDER BY id").await, [] as [&str; 0], "SELECT id FROM nx WHERE i = 'NaN'::float8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM nx WHERE i < 'NaN'::float8 ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM nx WHERE i < 'NaN'::float8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM nx WHERE i <> 'NaN'::float8 ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM nx WHERE i <> 'NaN'::float8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM nx WHERE p < 'Infinity'::float8 ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM nx WHERE p < 'Infinity'::float8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM nx WHERE p = 'NaN'::float8 ORDER BY id").await, [] as [&str; 0], "SELECT id FROM nx WHERE p = 'NaN'::float8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM nx WHERE n > '-Infinity'::float8 ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM nx WHERE n > '-Infinity'::float8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM nx WHERE n IN ('NaN'::float8, 1.5) ORDER BY id").await, ["1"] as [&str; 1], "SELECT id FROM nx WHERE n IN ('NaN'::float8, 1.5) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM nx WHERE b NOT IN ('Infinity'::float8) ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM nx WHERE b NOT IN ('Infinity'::float8) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM nx WHERE f < 'Infinity'::float8 ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM nx WHERE f < 'Infinity'::float8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM nx WHERE f = 'NaN'::float8 ORDER BY id").await, [] as [&str; 0], "SELECT id FROM nx WHERE f = 'NaN'::float8 ORDER BY id");
}

/// A NUMERIC (unconstrained or NUMERIC(20,18)) column compared with a FLOAT8 or REAL column, either side, in WHERE, a select list and UPDATE … WHERE: both read as double precision, as PostgreSQL compares them. PostgreSQL 17's values.
#[tokio::test(flavor = "multi_thread")]
async fn a_numeric_column_against_a_float_column() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE cc (id INT PRIMARY KEY, n NUMERIC, p NUMERIC(20,18), i INT, b BIGINT, f8 FLOAT8, f4 REAL)").await;
    run(&c, "INSERT INTO cc VALUES (1, 0.1, 0.1, 1, 9007199254740993, 0.1, 0.1), (2, 0.10000000000000000555, 0.100000000000000006, 2, 2, 0.1, 0.1), (3, 2.5, 2.5, 3, 3, 2.5, 2.5), (4, 1, 1, 9, 9007199254740992, 9007199254740992, 1), (5, NULL, NULL, NULL, NULL, NULL, NULL)").await;
    assert_eq!(rows_n(&c, "SELECT id FROM cc WHERE n = f8 ORDER BY id").await, ["1", "2", "3"] as [&str; 3], "SELECT id FROM cc WHERE n = f8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM cc WHERE f8 = n ORDER BY id").await, ["1", "2", "3"] as [&str; 3], "SELECT id FROM cc WHERE f8 = n ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM cc WHERE n < f8 ORDER BY id").await, ["4"] as [&str; 1], "SELECT id FROM cc WHERE n < f8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM cc WHERE p = f8 ORDER BY id").await, ["1", "2", "3"] as [&str; 3], "SELECT id FROM cc WHERE p = f8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM cc WHERE p > f8 ORDER BY id").await, [] as [&str; 0], "SELECT id FROM cc WHERE p > f8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM cc WHERE b = f8 ORDER BY id").await, ["4"] as [&str; 1], "SELECT id FROM cc WHERE b = f8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM cc WHERE i = f8 ORDER BY id").await, [] as [&str; 0], "SELECT id FROM cc WHERE i = f8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM cc WHERE f4 = f8 ORDER BY id").await, ["3"] as [&str; 1], "SELECT id FROM cc WHERE f4 = f8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM cc WHERE n = f4 ORDER BY id").await, ["3", "4"] as [&str; 2], "SELECT id FROM cc WHERE n = f4 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM cc WHERE n <> f8 ORDER BY id").await, ["4"] as [&str; 1], "SELECT id FROM cc WHERE n <> f8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, n = f8, p < f8 FROM cc ORDER BY id").await, ["1|t|f", "2|t|f", "3|t|f", "4|f|t", "5|NULL|NULL"] as [&str; 5], "SELECT id, n = f8, p < f8 FROM cc ORDER BY id");
    run(&c, "UPDATE cc SET i = 0 WHERE n = f8").await;
    assert_eq!(rows_n(&c, "SELECT id, i FROM cc ORDER BY id").await, ["1|0", "2|0", "3|0", "4|9", "5|NULL"] as [&str; 5], "SELECT id, i FROM cc ORDER BY id");
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

/// The 42883 message names the float's own type: `real` for a `::real` or
/// `::float4`, `double precision` for a double.
#[tokio::test(flavor = "multi_thread")]
async fn the_42883_message_names_real_for_a_real() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE tx (id INT PRIMARY KEY, t TEXT)").await;
    run(&c, "INSERT INTO tx VALUES (1, '1')").await;
    for (sql, want) in [
        ("SELECT id FROM tx WHERE t = '1'::real", "operator does not exist: text = real"),
        ("SELECT id FROM tx WHERE t = 1::float4", "operator does not exist: text = real"),
        ("SELECT id FROM tx WHERE t = '1'::float8", "operator does not exist: text = double precision"),
        ("SELECT id FROM tx WHERE upper(t) = '1'::real", "operator does not exist: text = real"),
        ("SELECT t = '1'::float8 FROM tx", "operator does not exist: text = double precision"),
    ] {
        assert_eq!(error_text(&c, sql).await, want, "{sql}");
    }
}

/// A float8 parameter compared with a text column is PostgreSQL's 42883 when
/// the statement is prepared with that parameter type.
#[tokio::test(flavor = "multi_thread")]
async fn a_float8_parameter_against_text_is_42883() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE tx (id INT PRIMARY KEY, t TEXT)").await;
    run(&c, "INSERT INTO tx VALUES (1, '1')").await;
    let outcome = match c.prepare_typed("SELECT id FROM tx WHERE t = $1", &[tokio_postgres::types::Type::FLOAT8]).await {
        Err(e) => Err(e),
        Ok(statement) => c.query(&statement, &[&1.0f64]).await.map(|_| ()),
    };
    let e = outcome.expect_err("text = float8 parameter");
    assert_eq!(e.code().map(|s| s.code().to_string()).as_deref(), Some("42883"), "{e}");
}

/// NaN and the infinities as a NUMERIC: PostgreSQL 17 stores them; this
/// engine has no NUMERIC NaN or infinity and refuses them on every path in,
/// 0A000 "numeric NaN and infinity are not supported" (an infinity into a
/// NUMERIC(p, s) keeps PostgreSQL's own 22003), and stores nothing. They used
/// to be refused with four different errors, two of them XX000. A known
/// difference (docs/postgres-compatibility.md).
#[tokio::test(flavor = "multi_thread")]
async fn numeric_nan_and_infinity_are_refused_on_every_path() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE src (id INT PRIMARY KEY, f FLOAT8, r REAL, t TEXT)").await;
    run(&c, "INSERT INTO src VALUES (1, 'NaN', 'Infinity', 'NaN'), (2, '-Infinity', 'NaN', 'Infinity')").await;
    run(&c, "CREATE TABLE nn (id INT PRIMARY KEY, n NUMERIC, p NUMERIC(10,2))").await;
    run(&c, "INSERT INTO nn VALUES (40, 1, 1)").await;
    let refused = "numeric NaN and infinity are not supported";
    for sql in [
        "INSERT INTO nn VALUES (1, 'NaN', 0)",
        "INSERT INTO nn VALUES (2, 'Infinity', 0)",
        "INSERT INTO nn VALUES (3, '-inf', 0)",
        "INSERT INTO nn VALUES (4, 0, 'NaN')",
        "INSERT INTO nn VALUES (6, 'NaN'::numeric, 0)",
        "INSERT INTO nn VALUES (7, 'NaN'::float8, 0)",
        "INSERT INTO nn VALUES (8, 'Infinity'::real, 0)",
        "INSERT INTO nn SELECT id + 10, f, 0 FROM src WHERE id = 1",
        "INSERT INTO nn SELECT id + 20, r, 0 FROM src WHERE id = 1",
        "UPDATE nn SET n = 'NaN' WHERE id = 40",
        "UPDATE nn SET n = 'NaN'::float8 WHERE id = 40",
        "UPDATE nn SET n = (SELECT f FROM src WHERE id = 2) WHERE id = 40",
        "UPDATE nn SET n = src.f FROM src WHERE src.id = 1 AND nn.id = 40",
        "UPDATE nn SET n = src.t::numeric FROM src WHERE src.id = 2 AND nn.id = 40",
        "INSERT INTO nn VALUES (40, 'NaN', 0) ON CONFLICT (id) DO UPDATE SET n = excluded.n",
        "INSERT INTO nn VALUES (40, 0, 0) ON CONFLICT (id) DO UPDATE SET n = 'Infinity'",
        "CREATE TABLE nd (id INT PRIMARY KEY, n NUMERIC DEFAULT 'NaN')",
    ] {
        assert_eq!(sqlstate(&c, sql).await, "0A000", "{sql}");
        assert!(error_text(&c, sql).await.ends_with(refused), "{sql}");
    }
    // An infinity into a NUMERIC(p, s): PostgreSQL refuses it too, 22003.
    for sql in [
        "INSERT INTO nn VALUES (5, 0, 'Infinity')",
        "INSERT INTO nn VALUES (9, 0, '-Infinity'::float8)",
        "UPDATE nn SET p = 'Infinity'::float8 WHERE id = 40",
    ] {
        assert_eq!(sqlstate(&c, sql).await, "22003", "{sql}");
    }
    // A generated column computed from a float NaN.
    run(&c, "CREATE TABLE ng (id INT PRIMARY KEY, f FLOAT8, n NUMERIC GENERATED ALWAYS AS (f) STORED)").await;
    assert_eq!(sqlstate(&c, "INSERT INTO ng (id, f) VALUES (1, 'NaN')").await, "0A000");
    run(&c, "INSERT INTO ng (id, f) VALUES (2, 1.5)").await;
    assert_eq!(sqlstate(&c, "UPDATE ng SET f = 'Infinity' WHERE id = 2").await, "0A000");
    assert_eq!(rows_n(&c, "SELECT id, n FROM ng ORDER BY id").await, ["2|1.5"]);
    assert_eq!(rows_n(&c, "SELECT id, n, p FROM nn ORDER BY id").await, ["40|1|1.00"]);
}

/// A REAL column against an IN list: two or more plain numbers compare as real (the list's common type); a float8 item, a single constant, and each column item compare in double precision.
#[tokio::test(flavor = "multi_thread")]
async fn a_real_in_list_takes_postgres_common_type() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE rl (id INT PRIMARY KEY, f4 REAL, f8 FLOAT8, n NUMERIC, p NUMERIC(10,2))").await;
    run(&c, "INSERT INTO rl VALUES (1, 0.1, 0.1, 0.1, 0.10), (2, 0.5, 0.5, 0.5, 0.50), (3, 0.3, 0.3, 0.3, 0.30), (4, NULL, NULL, NULL, NULL), (5, 'NaN', 'NaN', 2, 2), (6, 16777216, 16777216, 16777216, 7)").await;
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (0.1, 0.5) ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM rl WHERE f4 IN (0.1, 0.5) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 NOT IN (0.1, 0.5) ORDER BY id").await, ["3", "5", "6"] as [&str; 3], "SELECT id FROM rl WHERE f4 NOT IN (0.1, 0.5) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (0.1) ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 IN (0.1) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 NOT IN (0.1) ORDER BY id").await, ["1", "2", "3", "5", "6"] as [&str; 5], "SELECT id FROM rl WHERE f4 NOT IN (0.1) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (0.1, 0.5, '0.3'::float8) ORDER BY id").await, ["2"] as [&str; 1], "SELECT id FROM rl WHERE f4 IN (0.1, 0.5, '0.3'::float8) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (0.1, '0.5'::float8) ORDER BY id").await, ["2"] as [&str; 1], "SELECT id FROM rl WHERE f4 IN (0.1, '0.5'::float8) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (16777217, 0.5) ORDER BY id").await, ["2", "6"] as [&str; 2], "SELECT id FROM rl WHERE f4 IN (16777217, 0.5) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (16777217) ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 IN (16777217) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (16777217, '1'::float8) ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 IN (16777217, '1'::float8) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (f8, 0.1) ORDER BY id").await, ["2", "5", "6"] as [&str; 3], "SELECT id FROM rl WHERE f4 IN (f8, 0.1) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 NOT IN (f8, 0.1) ORDER BY id").await, ["1", "3"] as [&str; 2], "SELECT id FROM rl WHERE f4 NOT IN (f8, 0.1) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (0.1, f8) ORDER BY id").await, ["2", "5", "6"] as [&str; 3], "SELECT id FROM rl WHERE f4 IN (0.1, f8) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (n, 0.1) ORDER BY id").await, ["2", "6"] as [&str; 2], "SELECT id FROM rl WHERE f4 IN (n, 0.1) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (n, 0.1, 0.5) ORDER BY id").await, ["1", "2", "6"] as [&str; 3], "SELECT id FROM rl WHERE f4 IN (n, 0.1, 0.5) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (f4, 0.1) ORDER BY id").await, ["1", "2", "3", "5", "6"] as [&str; 5], "SELECT id FROM rl WHERE f4 IN (f4, 0.1) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (n, f8) ORDER BY id").await, ["2", "5", "6"] as [&str; 3], "SELECT id FROM rl WHERE f4 IN (n, f8) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE n IN (0.1, 0.5) ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM rl WHERE n IN (0.1, 0.5) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE n IN ('0.1'::float8, 0.5) ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM rl WHERE n IN ('0.1'::float8, 0.5) ORDER BY id");
}

/// REAL BETWEEN and a REAL against an integer constant compare in double precision: 0.1 is not BETWEEN 0 AND 0.1, 16777216 is not 16777217.
#[tokio::test(flavor = "multi_thread")]
async fn a_real_between_and_integer_compare_in_float8() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE rl (id INT PRIMARY KEY, f4 REAL, f8 FLOAT8, n NUMERIC, p NUMERIC(10,2))").await;
    run(&c, "INSERT INTO rl VALUES (1, 0.1, 0.1, 0.1, 0.10), (2, 0.5, 0.5, 0.5, 0.50), (3, 0.3, 0.3, 0.3, 0.30), (4, NULL, NULL, NULL, NULL), (5, 'NaN', 'NaN', 2, 2), (6, 16777216, 16777216, 16777216, 7)").await;
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 BETWEEN 0 AND 0.1 ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 BETWEEN 0 AND 0.1 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 BETWEEN 0.1 AND 0.5 ORDER BY id").await, ["1", "2", "3"] as [&str; 3], "SELECT id FROM rl WHERE f4 BETWEEN 0.1 AND 0.5 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 NOT BETWEEN 0 AND 0.1 ORDER BY id").await, ["1", "2", "3", "5", "6"] as [&str; 5], "SELECT id FROM rl WHERE f4 NOT BETWEEN 0 AND 0.1 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 BETWEEN '0'::float8 AND '0.3'::float8 ORDER BY id").await, ["1"] as [&str; 1], "SELECT id FROM rl WHERE f4 BETWEEN '0'::float8 AND '0.3'::float8 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 BETWEEN 0.3 AND 0.1 ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 BETWEEN 0.3 AND 0.1 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 = 16777217 ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 = 16777217 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 <> 16777217 ORDER BY id").await, ["1", "2", "3", "5", "6"] as [&str; 5], "SELECT id FROM rl WHERE f4 <> 16777217 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 = 16777216 ORDER BY id").await, ["6"] as [&str; 1], "SELECT id FROM rl WHERE f4 = 16777216 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 > 16777215 ORDER BY id").await, ["5", "6"] as [&str; 2], "SELECT id FROM rl WHERE f4 > 16777215 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE 16777217 > f4 ORDER BY id").await, ["1", "2", "3", "6"] as [&str; 4], "SELECT id FROM rl WHERE 16777217 > f4 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 < 16777217 AND f4 > 1 ORDER BY id").await, ["6"] as [&str; 1], "SELECT id FROM rl WHERE f4 < 16777217 AND f4 > 1 ORDER BY id");
}

/// REAL IN (SELECT numeric) is the scalar = ANY, in double precision: a real 0.1 is not numeric 0.1.
#[tokio::test(flavor = "multi_thread")]
async fn a_real_in_subquery_of_numerics_compares_in_float8() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE rl (id INT PRIMARY KEY, f4 REAL, f8 FLOAT8, n NUMERIC, p NUMERIC(10,2))").await;
    run(&c, "INSERT INTO rl VALUES (1, 0.1, 0.1, 0.1, 0.10), (2, 0.5, 0.5, 0.5, 0.50), (3, 0.3, 0.3, 0.3, 0.30), (4, NULL, NULL, NULL, NULL), (5, 'NaN', 'NaN', 2, 2), (6, 16777216, 16777216, 16777216, 7)").await;
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (SELECT n FROM rl WHERE id IN (1, 2)) ORDER BY id").await, ["2"] as [&str; 1], "SELECT id FROM rl WHERE f4 IN (SELECT n FROM rl WHERE id IN (1, 2)) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 NOT IN (SELECT n FROM rl WHERE id IN (1, 2)) ORDER BY id").await, ["1", "3", "5", "6"] as [&str; 4], "SELECT id FROM rl WHERE f4 NOT IN (SELECT n FROM rl WHERE id IN (1, 2)) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE rl.f4 IN (SELECT n FROM rl r2 WHERE r2.id IN (1, 2)) ORDER BY id").await, ["2"] as [&str; 1], "SELECT id FROM rl WHERE rl.f4 IN (SELECT n FROM rl r2 WHERE r2.id IN (1, 2)) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (SELECT f8 FROM rl WHERE id IN (1, 2)) ORDER BY id").await, ["2"] as [&str; 1], "SELECT id FROM rl WHERE f4 IN (SELECT f8 FROM rl WHERE id IN (1, 2)) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (SELECT f4 FROM rl WHERE id IN (1, 2)) ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM rl WHERE f4 IN (SELECT f4 FROM rl WHERE id IN (1, 2)) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (SELECT p FROM rl WHERE id IN (1, 2)) ORDER BY id").await, ["2"] as [&str; 1], "SELECT id FROM rl WHERE f4 IN (SELECT p FROM rl WHERE id IN (1, 2)) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE n IN (SELECT f4 FROM rl WHERE id IN (1, 2)) ORDER BY id").await, ["2"] as [&str; 1], "SELECT id FROM rl WHERE n IN (SELECT f4 FROM rl WHERE id IN (1, 2)) ORDER BY id");
    // A bigint past a real's exact range: as a list of plain numbers, real,
    // 16777217 would round to the real 16777216 and match.
    run(&c, "CREATE TABLE ri (b BIGINT)").await;
    run(&c, "INSERT INTO ri VALUES (16777217), (16777218)").await;
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (SELECT b FROM ri) ORDER BY id").await, [] as [&str; 0]);
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 NOT IN (SELECT b FROM ri) ORDER BY id").await, ["1", "2", "3", "5", "6"]);
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (16777217, 16777218) ORDER BY id").await, ["6"]);
}

/// UPDATE SET p = 'Infinity' (text) into NUMERIC(p,s) is PostgreSQL's 22003, as the literal path is; the row keeps its value.
#[tokio::test(flavor = "multi_thread")]
async fn infinity_text_into_constrained_numeric_is_22003() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE rl (id INT PRIMARY KEY, f4 REAL, f8 FLOAT8, n NUMERIC, p NUMERIC(10,2))").await;
    run(&c, "INSERT INTO rl VALUES (1, 0.1, 0.1, 0.1, 0.10), (2, 0.5, 0.5, 0.5, 0.50), (3, 0.3, 0.3, 0.3, 0.30), (4, NULL, NULL, NULL, NULL), (5, 'NaN', 'NaN', 2, 2), (6, 16777216, 16777216, 16777216, 7)").await;
    assert_eq!(sqlstate(&c, "UPDATE rl SET p = 'Infinity' WHERE id = 1").await, "22003");
    assert_eq!(sqlstate(&c, "UPDATE rl SET p = '-Infinity' WHERE id = 1").await, "22003");
    assert_eq!(sqlstate(&c, "UPDATE rl SET p = 'abc' WHERE id = 1").await, "22P02");
    assert_eq!(rows_n(&c, "SELECT id, p FROM rl WHERE id = 1").await, ["1|0.10"] as [&str; 1], "SELECT id, p FROM rl WHERE id = 1");
}

/// A NULL in a REAL IN list is an item with no type: it counts toward the list (f4 IN (0.1, NULL) is a list of reals) but not toward its type (a float8 item still widens), and keeps IN unknown and NOT IN never true.
#[tokio::test(flavor = "multi_thread")]
async fn a_real_in_list_with_a_null_keeps_the_common_type() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE rl (id INT PRIMARY KEY, f4 REAL, f8 FLOAT8, n NUMERIC, p NUMERIC(10,2))").await;
    run(&c, "INSERT INTO rl VALUES (1, 0.1, 0.1, 0.1, 0.10), (2, 0.5, 0.5, 0.5, 0.50), (3, 0.3, 0.3, 0.3, 0.30), (4, NULL, NULL, NULL, NULL), (5, 'NaN', 'NaN', 2, 2), (6, 16777216, 16777216, 16777216, 7)").await;
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN ('0.1'::float8, NULL) ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 IN ('0.1'::float8, NULL) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (0.1, NULL) ORDER BY id").await, ["1"] as [&str; 1], "SELECT id FROM rl WHERE f4 IN (0.1, NULL) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 NOT IN ('0.1'::float8, NULL) ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 NOT IN ('0.1'::float8, NULL) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 NOT IN (0.1, NULL) ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 NOT IN (0.1, NULL) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 NOT IN ('0.3'::float8, NULL) ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 NOT IN ('0.3'::float8, NULL) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (16777217, NULL) ORDER BY id").await, ["6"] as [&str; 1], "SELECT id FROM rl WHERE f4 IN (16777217, NULL) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (NULL, '0.5'::float8, f8) ORDER BY id").await, ["2", "5", "6"] as [&str; 3], "SELECT id FROM rl WHERE f4 IN (NULL, '0.5'::float8, f8) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (NULL) ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 IN (NULL) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 NOT IN (NULL) ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 NOT IN (NULL) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (f8, NULL) ORDER BY id").await, ["2", "5", "6"] as [&str; 3], "SELECT id FROM rl WHERE f4 IN (f8, NULL) ORDER BY id");
}

/// '0.1'::numeric is a numeric constant: against a REAL it compares in double precision, alone, under IN, NOT IN, BETWEEN and either side of a comparison; two of them in a list are a list of reals.
#[tokio::test(flavor = "multi_thread")]
async fn a_numeric_cast_constant_against_real_compares_in_float8() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE rl (id INT PRIMARY KEY, f4 REAL, f8 FLOAT8, n NUMERIC, p NUMERIC(10,2))").await;
    run(&c, "INSERT INTO rl VALUES (1, 0.1, 0.1, 0.1, 0.10), (2, 0.5, 0.5, 0.5, 0.50), (3, 0.3, 0.3, 0.3, 0.30), (4, NULL, NULL, NULL, NULL), (5, 'NaN', 'NaN', 2, 2), (6, 16777216, 16777216, 16777216, 7)").await;
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN ('0.1'::numeric) ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 IN ('0.1'::numeric) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 = '0.1'::numeric ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 = '0.1'::numeric ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE '0.1'::numeric = f4 ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE '0.1'::numeric = f4 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 <> '0.1'::numeric ORDER BY id").await, ["1", "2", "3", "5", "6"] as [&str; 5], "SELECT id FROM rl WHERE f4 <> '0.1'::numeric ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 NOT IN ('0.1'::numeric) ORDER BY id").await, ["1", "2", "3", "5", "6"] as [&str; 5], "SELECT id FROM rl WHERE f4 NOT IN ('0.1'::numeric) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 > '0.1'::numeric ORDER BY id").await, ["1", "2", "3", "5", "6"] as [&str; 5], "SELECT id FROM rl WHERE f4 > '0.1'::numeric ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN ('0.1'::numeric, '0.5'::numeric) ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM rl WHERE f4 IN ('0.1'::numeric, '0.5'::numeric) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN ('0.1'::numeric, 0.5) ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM rl WHERE f4 IN ('0.1'::numeric, 0.5) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN ('16777217'::numeric) ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 IN ('16777217'::numeric) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 BETWEEN '0'::numeric AND '0.1'::numeric ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 BETWEEN '0'::numeric AND '0.1'::numeric ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 IN (CAST('0.1' AS numeric)) ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 IN (CAST('0.1' AS numeric)) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE f4 = '0.1'::decimal ORDER BY id").await, [] as [&str; 0], "SELECT id FROM rl WHERE f4 = '0.1'::decimal ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE n = '0.1'::numeric ORDER BY id").await, ["1"] as [&str; 1], "SELECT id FROM rl WHERE n = '0.1'::numeric ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM rl WHERE n IN ('0.1'::numeric, '0.5'::numeric) ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM rl WHERE n IN ('0.1'::numeric, '0.5'::numeric) ORDER BY id");
}
