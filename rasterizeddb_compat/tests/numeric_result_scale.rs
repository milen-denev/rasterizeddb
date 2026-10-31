//! A computed `NUMERIC` carries the display scale PostgreSQL gives it, and
//! the digits that go with it.
//!
//! PostgreSQL's `numeric` keeps every place of a sum, difference or product
//! (a product's scale is the sum of its operands'), rounds a quotient at the
//! places `select_div_scale` picks from the operands' values (sixteen
//! significant digits at least), and shows a written constant with the places
//! it was written with. The engine evaluated every expression at nine places:
//! `1.0 / 3` was 0.333333333 for PostgreSQL's 0.33333333333333333333,
//! `0.00001 * 0.00001` was 0, `2.50 * 2` was 5.0, a constant in a select list
//! was a float (`123456789012345678901234` answered 123456789012345690000000),
//! and a scalar subquery's value lost its scale on the way back in.
//!
//! Every expected row is PostgreSQL 17's (`docker run --rm postgres:17`) for
//! the same statements, read as text through the simple protocol.

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

/// Every row as its cells joined by `|`, NULL as the empty string (psql's
/// unaligned form).
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

const SEED: &[&str] = &[
    "CREATE TABLE t (id INT PRIMARY KEY, a NUMERIC(10,2), b NUMERIC(12,4), c NUMERIC, d NUMERIC(20,12), i INT, bi BIGINT)",
    "INSERT INTO t VALUES (1, 2.50, 1.2345, 3.14159, 0.123456789123, 7, 30)",
    "INSERT INTO t VALUES (2, 10.00, 3.0000, 2, 1.000000000001, 3, 41)",
];

/// `(statement, PostgreSQL 17's rows)`.
const SELECTS: &[(&str, &[&str])] = &[
    // Quotients of constants: `select_div_scale`.
    (
        "SELECT id, 1.0 / 3, 10.0 / 4, 100.00 / 7, 0.000001 / 3, 123456789.0 / 7 FROM t WHERE id = 1",
        &["1|0.33333333333333333333|2.5000000000000000|14.2857142857142857|0.000000333333333333333333|17636684.142857142857"],
    ),
    // Products at the sum of the scales, sums at the larger, constants at
    // their written places.
    (
        "SELECT id, 1.5 * 2.25, 0.00001 * 0.00001, 0.1234567 * 0.1234567, 1.50 + 2.5, 2.50 * 2, 7 / 2.0 FROM t WHERE id = 1",
        &["1|3.375|0.0000000001|0.01524155677489|4.00|5.00|3.5000000000000000"],
    ),
    // Over columns: declared scales, an unconstrained column's own places,
    // a column with more places than nine.
    (
        "SELECT id, a / 3, a / b, a * b, c * c, d / 3, i / 3.0, a % 3 FROM t ORDER BY id",
        &[
            "1|0.83333333333333333333|2.0251113811259619|3.086250|9.8695877281|0.04115226304100000000|2.3333333333333333|2.50",
            "2|3.3333333333333333|3.3333333333333333|30.000000|4|0.33333333333366666667|1.00000000000000000000|1.00",
        ],
    ),
    // The branch taken keeps its own scale.
    (
        "SELECT id, coalesce(a, 0) * 1.0, CASE WHEN id = 1 THEN a ELSE b END, greatest(a, b), least(a, b), nullif(a, 2.50), abs(-a) FROM t ORDER BY id",
        &["1|2.500|2.50|2.50|1.2345||2.50", "2|10.000|3.0000|10.00|3.0000|10.00|10.00"],
    ),
    ("SELECT id FROM t ORDER BY a / b DESC", &["2", "1"]),
    // Over finished aggregates: `sum(bigint)` is a numeric at scale 0, and
    // `sum` of an unconstrained column has its values' places.
    (
        "SELECT min(a) + 1, max(b) * 2, sum(bi) + 1, sum(c) + 1, sum(a) / 3, count(*) * 1.5, avg(a) + 1 FROM t",
        &["3.50|6.0000|72|6.14159|4.1666666666666667|3.0|7.2500000000000000"],
    ),
    // Constants in a select list are `numeric`, exactly as written.
    (
        "SELECT id, 2.50, -2.50, 123456789012345678901234, 1e20, 0.83333333333333333333 FROM t WHERE id = 1",
        &["1|2.50|-2.50|123456789012345678901234|100000000000000000000|0.83333333333333333333"],
    ),
    (
        "SELECT id, (1.0 / 3)::text, (2.50 * 2)::text, a::text, (a * 2)::text FROM t WHERE id = 1",
        &["1|0.33333333333333333333|5.00|2.50|5.00"],
    ),
    // A scalar subquery's value comes back at its own scale.
    (
        "SELECT id, (SELECT a FROM t WHERE id = 1), (SELECT avg(a) FROM t), (SELECT a / 3 FROM t WHERE id = 1), (SELECT sum(bi) FROM t) FROM t WHERE id = 2",
        &["2|2.50|6.2500000000000000|0.83333333333333333333|71"],
    ),
    (
        "SELECT x.id, x.a / y.b, x.a * y.b FROM t x JOIN t y ON x.id = y.id ORDER BY x.id",
        &["1|2.0251113811259619|3.086250", "2|3.3333333333333333|30.000000"],
    ),
    ("UPDATE t SET i = i WHERE id = 1 RETURNING a / 3, a * b, 2.50 * 2", &["0.83333333333333333333|3.086250|5.00"]),
    // A condition compares the exact values: cut to nine places the product
    // was 0, and the quotient equal to its nine-place truncation.
    (
        "SELECT id, CASE WHEN 0.00001 * 0.00001 > 0 THEN 'pos' ELSE 'zero' END, \
         CASE WHEN a / 3 = 0.833333333 THEN 'cut' ELSE 'exact' END FROM t ORDER BY id",
        &["1|pos|exact", "2|pos|exact"],
    ),
];

#[tokio::test(flavor = "multi_thread")]
async fn computed_numerics_have_postgresqls_digits_and_scale() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    for sql in SEED {
        run(&c, sql).await;
    }
    let mut wrong = Vec::new();
    for (sql, pg) in SELECTS {
        let got = rows(&c, sql).await;
        if got != *pg {
            wrong.push(format!("{sql}\n  got {got:?}\n  pg  {pg:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// A computed value stored in a `NUMERIC(p, s)` column is PostgreSQL's
/// value rounded once more to the column's scale; one stored in a text column
/// is its text at its display scale.
#[tokio::test(flavor = "multi_thread")]
async fn stored_values_take_postgresqls_scale() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    for sql in SEED {
        run(&c, sql).await;
    }
    run(&c, "CREATE TABLE w (id INT PRIMARY KEY, a NUMERIC(10,2), d NUMERIC(20,12), v TEXT, n NUMERIC(10,3))").await;
    run(&c, "INSERT INTO w VALUES (1, 0, 0, '', 2.500)").await;
    // `(statement, the query that reads it back, PostgreSQL 17's rows)`.
    let cases: &[(&str, &str, &[&str])] = &[
        ("UPDATE w SET a = 2.0 / 3, d = 10 / 7.0 WHERE id = 1", "SELECT a, d FROM w", &["0.67|1.428571428571"]),
        ("UPDATE w SET a = 0.005 * 1, d = 1 / 3.0 WHERE id = 1", "SELECT a, d FROM w", &["0.01|0.333333333333"]),
        ("UPDATE w SET v = n WHERE id = 1", "SELECT v FROM w", &["2.500"]),
        ("UPDATE w SET v = 2.50 * 2 WHERE id = 1", "SELECT v FROM w", &["5.00"]),
        ("UPDATE w SET v = 1.0 / 3 WHERE id = 1", "SELECT v FROM w", &["0.33333333333333333333"]),
        ("UPDATE w SET v = n * 2 WHERE id = 1", "SELECT v FROM w", &["5.000"]),
    ];
    let mut wrong = Vec::new();
    for (statement, read, pg) in cases {
        run(&c, statement).await;
        let got = rows(&c, read).await;
        if got != *pg {
            wrong.push(format!("{statement}\n  got {got:?}\n  pg  {pg:?}"));
        }
    }
    run(&c, "CREATE TABLE txt (id INT PRIMARY KEY, v TEXT)").await;
    for sql in [
        "INSERT INTO txt SELECT id, a FROM t",
        "INSERT INTO txt SELECT id + 10, a * 2 FROM t",
        "INSERT INTO txt SELECT id + 20, a / 3 FROM t",
        "INSERT INTO txt VALUES (30, 2.50 * 2)",
    ] {
        run(&c, sql).await;
    }
    let pg = ["1|2.50", "2|10.00", "11|5.00", "12|20.00", "21|0.83333333333333333333", "22|3.3333333333333333", "30|5.00"];
    let got = rows(&c, "SELECT id, v FROM txt ORDER BY id").await;
    if got != pg {
        wrong.push(format!("INSERT … SELECT into text\n  got {got:?}\n  pg  {pg:?}"));
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// An unconstrained `NUMERIC` column here keeps nine places, where
/// PostgreSQL keeps every place of the value. A computed value with digits
/// past nine is refused, as such a written constant is, rather than stored
/// without them (it stored 0.666666667 for PostgreSQL's
/// 0.66666666666666666667); one exact at nine places is stored.
#[tokio::test(flavor = "multi_thread")]
async fn an_unconstrained_column_refuses_digits_it_cannot_keep() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE u (id INT PRIMARY KEY, c NUMERIC)").await;
    run(&c, "INSERT INTO u VALUES (1, 1)").await;
    let err = c.simple_query("UPDATE u SET c = 2.0 / 3 WHERE id = 1").await.expect_err("digits past nine places");
    assert_eq!(err.code().map(|s| s.code()), Some("0A000"), "{err}");
    assert_eq!(rows(&c, "SELECT c::text FROM u").await, ["1"], "the refused statement changed nothing");
    // PostgreSQL 17 stores 0.25000000000000000000 and shows that; the value
    // is kept here, at the places it needs.
    run(&c, "UPDATE u SET c = 1.0 / 4 WHERE id = 1").await;
    assert_eq!(rows(&c, "SELECT c::text FROM u").await, ["0.25"]);
}

/// An integer constant past `bigint` beside a NUMERIC is that number, and so
/// are a subquery's values written back into the statement as constants.
#[tokio::test(flavor = "multi_thread")]
async fn integers_past_bigint_compare_exactly() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE big (id INT PRIMARY KEY, w NUMERIC(30,0))").await;
    run(&c, "INSERT INTO big VALUES (1, 123456789012345678901234), (2, 2)").await;
    // PostgreSQL 17's rows for each.
    for (sql, pg) in [
        ("SELECT id FROM big WHERE w = 123456789012345678901234", vec!["1"]),
        ("SELECT id FROM big WHERE w IN (SELECT w FROM big) ORDER BY id", vec!["1", "2"]),
        ("SELECT id FROM big WHERE w = (SELECT w FROM big WHERE id = 1)", vec!["1"]),
        ("SELECT id FROM big WHERE w > 123456789012345678901233 ORDER BY id", vec!["1"]),
    ] {
        assert_eq!(rows(&c, sql).await, pg, "{sql}");
    }
}
