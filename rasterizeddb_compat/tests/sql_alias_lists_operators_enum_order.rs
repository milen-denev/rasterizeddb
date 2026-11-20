//! A column list on a table alias (`FROM t AS x(a, b)`), the `^@` and
//! `OVERLAPS` operators, and enum values ordered by their declared labels in
//! comparisons, `BETWEEN`, `min`/`max` and an `ORDER BY` of an output (in a
//! `DISTINCT` or grouped query too), and psql's `\gdesc` query. Each was
//! refused, or (enums) compared the labels' text. The answers and column names are PostgreSQL 17's
//! (`.claude/reviews/pgdiff`).

use rasterizeddb_compat::TestServer;
use tokio_postgres::SimpleQueryMessage;

async fn rows(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(
                (0..r.len())
                    .map(|i| r.get(i).unwrap_or("NULL").to_string())
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            _ => None,
        })
        .collect()
}

/// The column names a prepared statement describes.
async fn names(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let st = c.prepare(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    st.columns().iter().map(|c| c.name().to_string()).collect()
}

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_alias_names_its_columns() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute(
        "CREATE TABLE ta (id int, name text, v int);
         INSERT INTO ta VALUES (1, 'a', 10), (2, 'b', 20);
         CREATE VIEW tav AS SELECT id, name FROM ta",
    )
    .await
    .expect("setup");
    assert_eq!(rows(&c, "SELECT x.a, x.b FROM ta AS x(a, b) ORDER BY 1").await, ["1|a", "2|b"]);
    // The columns past the list keep their names.
    assert_eq!(rows(&c, "SELECT a, v FROM ta x(a) ORDER BY 1").await, ["1|10", "2|20"]);
    assert_eq!(names(&c, "SELECT * FROM ta x(p)").await, ["p", "name", "v"]);
    assert_eq!(rows(&c, "SELECT p.k, q.k FROM ta p(k) JOIN ta q(k) ON p.k = q.k ORDER BY 1").await, ["1|1", "2|2"]);
    assert_eq!(
        rows(&c, "SELECT k FROM ta t(k) WHERE EXISTS (SELECT 1 FROM ta u WHERE u.id = k) ORDER BY 1").await,
        ["1", "2"]
    );
    // A view's list, as a table's.
    assert_eq!(rows(&c, "SELECT w.p FROM tav w(p) ORDER BY 1").await, ["1", "2"]);
    // psql's `\gdesc`, whose casts name their schema.
    assert_eq!(
        rows(
            &c,
            "SELECT name AS \"Column\", pg_catalog.format_type(tp, tpm) AS \"Type\"\nFROM (VALUES ('p', '23'::pg_catalog.oid, -1)) s(name, tp, tpm)"
        )
        .await,
        ["p|integer"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn starts_with_and_overlaps_operators() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute("CREATE TABLE tw (id int, name text); INSERT INTO tw VALUES (1, 'alpha'), (2, 'beta')")
        .await
        .expect("setup");
    assert_eq!(rows(&c, "SELECT 'alphabet' ^@ 'alph', 'x' ^@ 'y'").await, ["t|f"]);
    assert_eq!(rows(&c, "SELECT id FROM tw WHERE name ^@ 'al'").await, ["1"]);
    let overlaps = [
        ("(DATE '2001-02-16', DATE '2001-12-21') OVERLAPS (DATE '2001-10-30', DATE '2002-10-30')", "t"),
        ("(DATE '2001-02-16', INTERVAL '100 days') OVERLAPS (DATE '2001-10-30', DATE '2002-10-30')", "f"),
        // Half-open: a period ending where the other starts does not overlap.
        ("(DATE '2001-10-29', DATE '2001-10-30') OVERLAPS (DATE '2001-10-30', DATE '2001-10-31')", "f"),
        // A single instant at the other's start does.
        ("(DATE '2001-10-30', DATE '2001-10-30') OVERLAPS (DATE '2001-10-30', DATE '2001-10-31')", "t"),
        // A pair is ordered first; a missing end that does not decide it.
        ("(DATE '2001-12-21', DATE '2001-02-16') OVERLAPS (DATE '2001-10-30', NULL::date)", "t"),
        ("(TIMESTAMP '2001-02-16 10:00', TIMESTAMP '2001-02-16 12:00') OVERLAPS (TIMESTAMP '2001-02-16 11:00', TIMESTAMP '2001-02-16 13:00')", "t"),
    ];
    for (expr, want) in overlaps {
        assert_eq!(rows(&c, &format!("SELECT {expr}")).await, [want], "{expr}");
    }
    assert_eq!(
        names(&c, "SELECT 'a' ^@ 'b', (DATE '2001-01-01', DATE '2001-01-02') OVERLAPS (DATE '2001-01-01', DATE '2001-01-03')").await,
        ["?column?", "overlaps"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn enum_values_order_by_their_labels() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute(
        "CREATE TYPE feel AS ENUM ('sad', 'ok', 'happy');
         CREATE TABLE te (id int, m feel, n feel);
         INSERT INTO te VALUES (1, 'happy', 'sad'), (2, 'sad', 'ok'), (3, 'ok', 'ok'), (4, NULL, 'ok')",
    )
    .await
    .expect("setup");
    let cases: &[(&str, &[&str])] = &[
        ("SELECT id FROM te WHERE m > 'sad' ORDER BY id", &["1", "3"]),
        ("SELECT id FROM te WHERE 'ok' >= m ORDER BY id", &["2", "3"]),
        ("SELECT id FROM te WHERE m < n ORDER BY id", &["2"]),
        ("SELECT id FROM te WHERE m BETWEEN 'sad' AND 'ok' ORDER BY id", &["2", "3"]),
        ("SELECT id FROM te WHERE m NOT BETWEEN 'sad' AND 'ok' ORDER BY id", &["1"]),
        ("SELECT max(m), min(m) FROM te", &["happy|sad"]),
        ("SELECT m FROM te ORDER BY 1", &["sad", "ok", "happy", "NULL"]),
        ("SELECT m AS z FROM te ORDER BY z DESC, id", &["NULL", "happy", "ok", "sad"]),
        ("SELECT DISTINCT m FROM te ORDER BY m", &["sad", "ok", "happy", "NULL"]),
        ("SELECT n, count(*) FROM te GROUP BY n ORDER BY n", &["sad|1", "ok|3"]),
        ("SELECT max(m) AS hi FROM te GROUP BY n ORDER BY hi", &["ok", "happy"]),
        ("SELECT max(m) AS hi FROM te GROUP BY n ORDER BY 1 DESC LIMIT 1", &["happy"]),
        ("SELECT id FROM te ORDER BY n, m DESC", &["1", "4", "3", "2"]),
        ("SELECT id FROM te WHERE m = 'ok'", &["3"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
    assert_eq!(names(&c, "SELECT max(m), min(m) FROM te").await, ["max", "min"]);
    // A parameter compares by the labels too.
    let got: Vec<i32> = c
        .query("SELECT id FROM te WHERE m >= $1 ORDER BY id", &[&"ok"])
        .await
        .expect("parameter")
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(got, [1, 3]);
    let err = c.simple_query("SELECT id FROM te WHERE m > 'glad'").await.expect_err("no label");
    let db = err.as_db_error().expect("db error");
    assert_eq!((db.code().code(), db.message()), ("22P02", "invalid input value for enum feel: \"glad\""));
}
