//! A `timestamp` (without time zone) and a `timestamptz` are one storage type
//! here and two types in PostgreSQL. The evaluator now tells them apart by
//! the expression that makes the value (`expr::yields_timestamp`), and that
//! one answer decides how the value is shown as text (bare, or with `+00`)
//! and how a quoted literal compared with it is read (its zone discarded, or
//! applied). Every expected value is PostgreSQL 17's (`docker run --rm
//! postgres:17`, session `TimeZone` UTC).
//!
//! Before, the answer was recovered from the statement's text, one spelling
//! at a time: a `timestamp` reaching text through any expression but a bare
//! column got a `+00` it does not have, and a literal not written right
//! beside the column was read with its zone applied.

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

/// Every row's cells as text, NULL as "NULL", joined by `|`.
async fn rows(c: &Client, sql: &str) -> Vec<String> {
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

async fn setup(c: &Client) {
    run(c, "CREATE TABLE tx (k INT PRIMARY KEY, a TIMESTAMP, b TIMESTAMPTZ, t TEXT)").await;
    run(
        c,
        "INSERT INTO tx VALUES (1, '2024-01-01 10:00:00', '2024-01-01 10:00:00+02', NULL), \
         (2, '2024-01-02 11:30:00', '2024-01-02 11:30:00+00', NULL)",
    )
    .await;
}

// Review r4 R1: a `timestamp` reaching text through an expression is bare.
#[tokio::test]
async fn a_timestamp_expression_is_shown_without_an_offset() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    setup(&c).await;
    assert_eq!(
        rows(
            &c,
            "SELECT (a + interval '1 hour')::text, a + interval '1 hour', date_trunc('day', a)::text, \
             coalesce(a, '2000-01-01 00:00:00')::text, greatest(a, '2000-01-01 00:00:00'), \
             (CASE WHEN k = 1 THEN a END)::text, b + interval '1 hour' FROM tx WHERE k = 1"
        )
        .await,
        ["2024-01-01 11:00:00|2024-01-01 11:00:00|2024-01-01 00:00:00|2024-01-01 10:00:00|2024-01-01 10:00:00|\
          2024-01-01 10:00:00|2024-01-01 09:00:00+00"]
    );
    assert_eq!(rows(&c, "SELECT max(a)::text, min(b)::text FROM tx").await, ["2024-01-02 11:30:00|2024-01-01 08:00:00+00"]);
    let clocks = rows(&c, "SELECT now()::timestamp::text, LOCALTIMESTAMP::text, now()::text FROM tx WHERE k = 1").await;
    let clocks: Vec<&str> = clocks[0].split('|').collect();
    assert!(!clocks[0].ends_with("+00") && !clocks[1].ends_with("+00") && clocks[2].ends_with("+00"), "{clocks:?}");
    assert_eq!(rows(&c, "SELECT k FROM tx WHERE (a + interval '1 hour')::text = '2024-01-01 11:00:00'").await, ["1"]);
    assert_eq!(rows(&c, "SELECT k FROM tx WHERE date_trunc('day', a)::text = '2024-01-01 00:00:00'").await, ["1"]);
    // Stored into a text column the same way.
    run(&c, "UPDATE tx SET t = a + interval '1 hour' WHERE k = 1").await;
    run(&c, "UPDATE tx SET t = CASE WHEN k = 2 THEN a END WHERE k = 2").await;
    assert_eq!(rows(&c, "SELECT k, t FROM tx ORDER BY k").await, ["1|2024-01-01 11:00:00", "2|2024-01-02 11:30:00"]);
    // A cast back and forth is the type it ends at.
    assert_eq!(
        rows(
            &c,
            "SELECT '2024-01-01 10:00:00+02'::timestamptz::timestamp, '2024-01-01 10:00:00+02'::timestamp::timestamptz, \
             ('2024-01-01 10:00:00+02'::timestamptz::timestamp)::text FROM tx WHERE k = 1"
        )
        .await,
        ["2024-01-01 08:00:00|2024-01-01 10:00:00+00|2024-01-01 08:00:00"]
    );
    // A generated text column over the column. (PostgreSQL refuses this as
    // not immutable; here it is computed, and its text is the select's.)
    run(&c, "CREATE TABLE gt (k INT, a TIMESTAMP, txt TEXT GENERATED ALWAYS AS (a::text) STORED, cat TEXT GENERATED ALWAYS AS ('at ' || a) STORED)").await;
    run(&c, "INSERT INTO gt (k, a) VALUES (1, '2024-01-01 10:00:00')").await;
    assert_eq!(rows(&c, "SELECT txt, cat FROM gt").await, ["2024-01-01 10:00:00|at 2024-01-01 10:00:00"]);
}

// Review r4 R2: a literal compared with a `timestamp` expression, on either
// side and through any expression, is read without its zone.
#[tokio::test]
async fn a_literal_compared_with_a_timestamp_expression_drops_its_zone() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(
        &c,
        "CREATE TABLE g (k INT, a TIMESTAMP, \
         rev BOOLEAN GENERATED ALWAYS AS ('2024-01-01 09:00:00+05' < a) STORED, \
         coal BOOLEAN GENERATED ALWAYS AS (coalesce(a, '2024-01-01 00:00:00') > '2024-01-01 09:00:00+05') STORED, \
         par BOOLEAN GENERATED ALWAYS AS ((a) > '2024-01-01 09:00:00+05') STORED, \
         plus BOOLEAN GENERATED ALWAYS AS (a + interval '1 hour' > '2024-01-01 10:30:00+05') STORED)",
    )
    .await;
    run(
        &c,
        "INSERT INTO g (k, a) VALUES (1, '2024-01-01 10:00:00'), (2, '2024-01-01 08:00:00'), (3, NULL), \
         (4, '2024-01-01 09:30:00.25')",
    )
    .await;
    assert_eq!(
        rows(&c, "SELECT k, rev, coal, par, plus FROM g ORDER BY k").await,
        ["1|t|t|t|t", "2|f|f|f|f", "3|NULL|f|NULL|NULL", "4|t|t|t|t"]
    );
    run(&c, "UPDATE g SET a = '2024-01-01 10:00:00+09' WHERE k = 2").await;
    assert_eq!(rows(&c, "SELECT k, rev, coal, par, plus FROM g WHERE k = 2").await, ["2|t|t|t|t"]);
    run(&c, "UPDATE g SET a = '2024-01-01 08:00:00' WHERE k = 2").await;
    for sql in [
        "SELECT k FROM g WHERE '2024-01-01 09:00:00+05' < a ORDER BY k",
        "SELECT k FROM g WHERE coalesce(a, '2024-01-01 00:00:00') > '2024-01-01 09:00:00+05' ORDER BY k",
    ] {
        assert_eq!(rows(&c, sql).await, ["1", "4"], "{sql}");
    }
}

// A `timestamptz` read by a scalar subquery or an `INSERT … SELECT` keeps its
// `+00` on the way into a text column; a `timestamp` stays bare.
#[tokio::test]
async fn a_timestamptz_from_a_subquery_or_insert_select_keeps_its_offset() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    setup(&c).await;
    run(&c, "CREATE TABLE tx2 (k INT, t TEXT)").await;
    run(&c, "INSERT INTO tx2 (k, t) SELECT k, b FROM tx WHERE k = 1").await;
    run(&c, "INSERT INTO tx2 (k, t) SELECT k + 10, b + interval '1 hour' FROM tx WHERE k = 1").await;
    run(&c, "INSERT INTO tx2 (k, t) SELECT k + 20, a FROM tx WHERE k = 1").await;
    run(&c, "INSERT INTO tx2 (k, t) SELECT k + 30, a + interval '1 hour' FROM tx WHERE k = 1").await;
    assert_eq!(
        rows(&c, "SELECT k, t FROM tx2 ORDER BY k").await,
        ["1|2024-01-01 08:00:00+00", "11|2024-01-01 09:00:00+00", "21|2024-01-01 10:00:00", "31|2024-01-01 11:00:00"]
    );
    run(&c, "UPDATE tx SET t = (SELECT b FROM tx WHERE k = 2) WHERE k = 1").await;
    run(&c, "UPDATE tx SET t = (SELECT a FROM tx WHERE k = 1) WHERE k = 2").await;
    assert_eq!(rows(&c, "SELECT k, t FROM tx ORDER BY k").await, ["1|2024-01-02 11:30:00+00", "2|2024-01-01 10:00:00"]);
    // The value itself is unchanged where a timestamp column takes it.
    run(&c, "CREATE TABLE tx3 (k INT, a TIMESTAMP, b TIMESTAMPTZ)").await;
    run(&c, "INSERT INTO tx3 (k, a, b) SELECT k, b, a FROM tx WHERE k = 1").await;
    assert_eq!(rows(&c, "SELECT a, b FROM tx3").await, ["2024-01-01 08:00:00|2024-01-01 10:00:00+00"]);
}

// A `timestamp` expression in the select list is described as `timestamp`
// (OID 1114), a `timestamptz` one as `timestamptz` (1184).
#[tokio::test]
async fn a_timestamp_expression_is_described_as_timestamp() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    setup(&c).await;
    for (sql, oid) in [
        ("SELECT '2024-01-01 10:00:00+02'::timestamp FROM tx WHERE k = 1", 1114),
        ("SELECT a + interval '1 hour' FROM tx WHERE k = 1", 1114),
        ("SELECT coalesce(a, '2000-01-01 00:00:00') FROM tx WHERE k = 1", 1114),
        ("SELECT LOCALTIMESTAMP FROM tx WHERE k = 1", 1114),
        ("SELECT b + interval '1 hour' FROM tx WHERE k = 1", 1184),
        ("SELECT '2024-01-01 10:00:00+02'::timestamptz FROM tx WHERE k = 1", 1184),
    ] {
        let st = c.prepare(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(st.columns()[0].type_().oid(), oid, "{sql}");
    }
    assert_eq!(rows(&c, "SELECT '2024-01-01 10:00:00+02'::timestamp FROM tx WHERE k = 1").await, ["2024-01-01 10:00:00"]);
}
