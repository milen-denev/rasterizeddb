//! `exp`, `ln`, `log`, `log10` and `power` over NUMERIC, and the `^`
//! operator. Each NUMERIC form was refused ("exp(numeric) is not supported
//! yet: cast the argument to double precision"), `log(2, 8)` too, and `^`
//! was "column '2 ^ 3' does not exist". PostgreSQL shows these results at a
//! scale numeric.c picks for sixteen significant digits, so `exp(1.0)` is
//! `2.7182818284590452`; every expected answer is PostgreSQL 17's.
//!
//! A function over a computed NUMERIC argument also reads it exactly now:
//! `sqrt(u / 3)` used to take the quotient at the engine's nine places and was
//! wrong from the tenth digit.

use rasterizeddb_compat::TestServer;
use tokio_postgres::SimpleQueryMessage;

async fn rows(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
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

async fn code(c: &tokio_postgres::Client, sql: &str) -> String {
    let e = c.simple_query(sql).await.expect_err(sql);
    e.code().map(|s| s.code().to_string()).unwrap_or_else(|| panic!("{sql}: {e}"))
}

async fn server() -> (TestServer, tokio_postgres::Client) {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, n NUMERIC(8,3), u NUMERIC(20,18), f DOUBLE PRECISION, i INT)",
        "INSERT INTO t VALUES (1, 12.345, 0.123456789012345678, -2.5, 3), (2, 0.5, 2, 4, -2), (3, NULL, NULL, NULL, NULL)",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    (server, c)
}

#[tokio::test(flavor = "multi_thread")]
async fn each_form_answers_as_postgresql_does() {
    let (_server, c) = server().await;
    let cases: &[(&str, &str)] = &[
        ("exp(n)", "229808.12486124595"),
        ("ln(n)", "2.5132511227971428"),
        ("log(n)", "1.0914910942679511"),
        ("log10(n)", "1.0914910942679511"),
        ("log(2, n)", "3.6258549313680572"),
        ("log(n, 100)", "1.8323557658904895"),
        ("power(n, 2)", "152.39902500000000"),
        ("power(n, i)", "1881.3659636250000"),
        ("power(n, 0.5)", "3.5135452181521729"),
        ("n ^ 2", "152.39902500000000"),
        ("n ^ i", "1881.3659636250000"),
        // Floats and integers alone stay double precision.
        ("f ^ 2", "6.25"),
        ("i ^ 2", "9"),
        ("power(f, i)", "-15.625"),
        // `^` groups left to right, under a unary sign and over `*`.
        ("2 ^ 3 ^ 2", "64"),
        ("-2 ^ 2", "4"),
        ("2 * 3 ^ 2", "18"),
        ("(2 ^ 3)::int", "8"),
        // A NUMERIC(20,18) column, and quotients of it: exact arguments.
        ("exp(u)", "1.131401114526201518"),
        ("ln(u)", "-2.091864070678393130"),
        ("sqrt(u / 3)", "0.20286020557052392477"),
        ("exp(u / 3)", "1.04201075315468795697"),
        ("ceil(u * 100000000000)", "12345678902"),
        ("power(u, 2)", "0.015241578753238837"),
        // Constants.
        ("exp(1.0)", "2.7182818284590452"),
        ("ln(1e-30)", "-69.077552789821370520539743640531"),
        ("power(2.00, 100)", "1267650600228229401496703205376.00"),
        ("power(1.0001, 10000)", "2.7181459268252249"),
        ("log(2, 8)", "3.0000000000000000"),
        ("power(-2.0, 3)", "-8.0000000000000000"),
        ("pg_typeof(exp(n))", "numeric"),
        ("pg_typeof(log(2, 8))", "numeric"),
        ("pg_typeof(power(2, 0.5))", "numeric"),
        ("pg_typeof(power(f, 2))", "double precision"),
        ("pg_typeof(i ^ 2)", "double precision"),
        ("pg_typeof(n ^ 2)", "numeric"),
    ];
    let mut wrong = Vec::new();
    for (expr, want) in cases {
        let sql = format!("SELECT {expr} FROM t WHERE id = 1");
        match c.simple_query(&sql).await {
            Ok(messages) => {
                let got: Vec<String> = messages
                    .into_iter()
                    .filter_map(|m| match m {
                        SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap_or("NULL").to_string()),
                        _ => None,
                    })
                    .collect();
                if got != [want.to_string()] {
                    wrong.push(format!("{expr}: got {got:?}, want {want:?}"));
                }
            }
            Err(e) => wrong.push(format!("{expr}: {e}")),
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// PostgreSQL's errors with its SQLSTATEs, and the one this engine adds: a
/// result PostgreSQL shows with more than 38 digits or places is refused
/// rather than shown at another scale.
#[tokio::test(flavor = "multi_thread")]
async fn errors_carry_postgresqls_codes() {
    let (_server, c) = server().await;
    for (sql, sqlstate) in [
        ("SELECT ln(0::numeric)", "2201E"),
        ("SELECT ln(-1.5)", "2201E"),
        ("SELECT log(0, 10.0)", "2201E"),
        ("SELECT log(1, 10.0)", "22012"),
        ("SELECT power(0.0, -1)", "2201F"),
        ("SELECT power(-8.0, 0.5)", "2201F"),
        ("SELECT power(0::float8, -1)", "2201F"),
        ("SELECT power(-8::float8, 0.5)", "2201F"),
        ("SELECT exp(7000.0)", "22003"),
        ("SELECT exp(1000::float8)", "22003"),
        ("SELECT exp(-1000::float8)", "22003"),
        ("SELECT power(10::float8, 400)", "22003"),
        // 45 digits and 59 places in PostgreSQL.
        ("SELECT exp(100.0)", "0A000"),
        ("SELECT exp(-100.0)", "0A000"),
    ] {
        assert_eq!(code(&c, sql).await, sqlstate, "{sql}");
    }
}

/// The functions and `^` wherever a query puts them, and over the extended
/// protocol, whose Describe has to announce NUMERIC where the value is one.
#[tokio::test(flavor = "multi_thread")]
async fn every_clause_reads_them() {
    let (_server, c) = server().await;
    assert_eq!(rows(&c, "SELECT id FROM t WHERE ln(n) > 1 ORDER BY id").await, ["1"]);
    assert_eq!(rows(&c, "SELECT id FROM t WHERE n ^ 2 < 1 ORDER BY id").await, ["2"]);
    assert_eq!(rows(&c, "SELECT id FROM t WHERE (n ^ 2) < 1 ORDER BY id").await, ["2"]);
    assert_eq!(
        rows(&c, "SELECT id, power(n, i) FROM t ORDER BY power(n, i) DESC NULLS LAST").await,
        ["1|1881.3659636250000", "2|4.0000000000000000", "3|NULL"]
    );
    assert_eq!(rows(&c, "SELECT id FROM t ORDER BY n ^ 2 NULLS FIRST").await, ["3", "2", "1"]);
    assert_eq!(rows(&c, "SELECT sum(ln(n)) FROM t").await, ["1.8201039422371975"]);
    assert_eq!(
        rows(&c, "SELECT id, exp(n) FROM t ORDER BY id").await,
        ["1|229808.12486124595", "2|1.6487212707001281", "3|NULL"]
    );
    assert_eq!(rows(&c, "UPDATE t SET n = n ^ 2 WHERE id = 2 RETURNING n").await, ["0.250"]);

    let statement = c
        .prepare("SELECT exp(n), log(2, 8), power(n, 2), n ^ 2, i ^ 2, power(f, 2) FROM t WHERE id = 1")
        .await
        .expect("prepare");
    let types: Vec<&str> = statement.columns().iter().map(|col| col.type_().name()).collect();
    assert_eq!(types, ["numeric", "numeric", "numeric", "numeric", "float8", "float8"]);
    let row = c.query_one(&statement, &[]).await.expect("query");
    assert_eq!(row.get::<_, f64>(4), 9.0);
    assert_eq!(row.get::<_, f64>(5), 6.25);
}
