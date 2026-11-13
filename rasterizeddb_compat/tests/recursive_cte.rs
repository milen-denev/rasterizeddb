//! `WITH RECURSIVE`, against PostgreSQL 17's answers (`docker run --rm
//! postgres:17`). It was refused outright, and a column list on any CTE
//! (`WITH t (a, b) AS …`) with it.

use rasterizeddb_compat::TestServer;
use tokio_postgres::{Client, SimpleQueryMessage};

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    client
}

/// Each row as `a|b|…`, a NULL as `NULL`.
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
    c.batch_execute(
        "CREATE TABLE cat (id INT PRIMARY KEY, parent INT, name TEXT);
         INSERT INTO cat VALUES (1, NULL, 'root'), (2, 1, 'a'), (3, 1, 'b'), (4, 2, 'a1'), (5, 4, 'a1x'), (6, NULL, 'other');
         CREATE TABLE edge (src INT, dst INT);
         INSERT INTO edge VALUES (1, 2), (2, 3), (3, 1), (3, 4);",
    )
    .await
    .expect("setup");
}

/// Counting, a tree walk with depth and path, UNION ending a walk over a
/// cycle, UNION deduplicating the anchor too, bigint Fibonacci, a recursive
/// CTE after a plain one, aggregates over one, a VALUES anchor, a column
/// list on a plain CTE (fewer names than columns allowed), and an INSERT
/// fed by one.
#[tokio::test(flavor = "multi_thread")]
async fn recursive_ctes_answer_as_postgresql_does() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    setup(&c).await;
    for (sql, want) in [
        ("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 6) SELECT n, n * n FROM r", vec!["1|1", "2|4", "3|9", "4|16", "5|25", "6|36"]),
        (
            "WITH RECURSIVE t AS (SELECT id, name, 0 AS depth FROM cat WHERE parent IS NULL UNION ALL SELECT c.id, c.name, t.depth + 1 FROM cat c JOIN t ON c.parent = t.id) SELECT id, name, depth FROM t ORDER BY id",
            vec!["1|root|0", "2|a|1", "3|b|1", "4|a1|2", "5|a1x|3", "6|other|0"],
        ),
        ("WITH RECURSIVE sub AS (SELECT id FROM cat WHERE id = 2 UNION SELECT c.id FROM cat c JOIN sub s ON c.parent = s.id) SELECT count(*) FROM sub", vec!["3"]),
        ("WITH RECURSIVE reach(node) AS (SELECT 1 UNION SELECT e.dst FROM edge e JOIN reach r ON e.src = r.node) SELECT node FROM reach ORDER BY node", vec!["1", "2", "3", "4"]),
        (
            "WITH RECURSIVE path(id, trail) AS (SELECT id, name FROM cat WHERE id = 1 UNION ALL SELECT c.id, p.trail || '/' || c.name FROM cat c JOIN path p ON c.parent = p.id) SELECT id, trail FROM path ORDER BY trail",
            vec!["1|root", "2|root/a", "4|root/a/a1", "5|root/a/a1/a1x", "3|root/b"],
        ),
        (
            "WITH RECURSIVE f(i, a, b) AS (SELECT 1, 0::bigint, 1::bigint UNION ALL SELECT i + 1, b, a + b FROM f WHERE i < 10) SELECT i, a FROM f ORDER BY i",
            vec!["1|0", "2|1", "3|1", "4|2", "5|3", "6|5", "7|8", "8|13", "9|21", "10|34"],
        ),
        ("WITH RECURSIVE a AS (SELECT 1 AS x), b(y) AS (SELECT x FROM a UNION ALL SELECT y + 1 FROM b WHERE y < 3) SELECT y FROM b ORDER BY y", vec!["1", "2", "3"]),
        ("WITH RECURSIVE dup(n) AS (SELECT 1 UNION SELECT 1 UNION SELECT n FROM dup) SELECT n FROM dup", vec!["1"]),
        ("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT sum(n), max(n) FROM r", vec!["6|3"]),
        ("WITH RECURSIVE r(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r", vec!["1", "2", "3"]),
        (
            "WITH RECURSIVE v AS (VALUES (1, 'a'), (2, 'b') UNION ALL SELECT column1 + 10, column2 FROM v WHERE column1 < 5) SELECT * FROM v ORDER BY column1",
            vec!["1|a", "2|b", "11|a", "12|b"],
        ),
        ("WITH t(a, b) AS (SELECT 1, 'x') SELECT b, a FROM t", vec!["x|1"]),
        ("WITH t(a) AS (SELECT 1, 2) SELECT * FROM t", vec!["1|2"]),
    ] {
        assert_eq!(rows(&c, sql).await, want, "{sql}");
    }
    c.simple_query("INSERT INTO cat (id, parent, name) WITH RECURSIVE r(n) AS (SELECT 100 UNION ALL SELECT n + 1 FROM r WHERE n < 102) SELECT n, 1, 'gen' FROM r")
        .await
        .expect("INSERT … WITH RECURSIVE");
    assert_eq!(rows(&c, "SELECT count(*) FROM cat WHERE name = 'gen'").await, ["3"]);
}

/// The extended protocol describes a recursive CTE by its non-recursive
/// term and a column list's names, as the rows come back. (A parameter
/// compared with a CTE's column is still described as text, as for any CTE
/// or derived table; the rows here are bound to a real table's column.)
#[tokio::test(flavor = "multi_thread")]
async fn prepared_recursive_ctes_are_described() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    setup(&c).await;
    let st = c
        .prepare("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT n, n * 2 AS twice FROM r ORDER BY n")
        .await
        .expect("prepare recursive");
    let described: Vec<(String, tokio_postgres::types::Type)> = st.columns().iter().map(|c| (c.name().to_string(), c.type_().clone())).collect();
    assert_eq!(described, [("n".to_string(), tokio_postgres::types::Type::INT4), ("twice".to_string(), tokio_postgres::types::Type::INT4)]);
    let r = c.query(&st, &[]).await.expect("run recursive");
    assert_eq!(r.iter().map(|row| row.get::<_, i32>(0)).collect::<Vec<_>>(), [1, 2, 3, 4, 5]);
    let r = c
        .query("WITH RECURSIVE t(a, b) AS (SELECT id, name FROM cat WHERE id = $1 UNION ALL SELECT c.id, c.name FROM cat c JOIN t ON c.parent = t.a) SELECT b, a FROM t ORDER BY a", &[&2i32])
        .await
        .expect("prepared column list");
    assert_eq!(r[0].columns().iter().map(|col| col.name()).collect::<Vec<_>>(), ["b", "a"]);
    assert_eq!(r.iter().map(|row| row.get::<_, String>(0)).collect::<Vec<_>>(), ["a", "a1", "a1x"]);
}

/// The forms PostgreSQL refuses, refused with its messages; and a recursion
/// with no end refused rather than run until the memory or the statement
/// timeout gives out (PostgreSQL stops it lazily at an outer LIMIT; this
/// builds the CTE whole).
#[tokio::test(flavor = "multi_thread")]
async fn recursive_cte_refusals() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    setup(&c).await;
    for (sql, needle) in [
        ("WITH t(a, b, c) AS (SELECT 1, 2) SELECT * FROM t", "WITH query \"t\" has 2 columns available but 3 columns specified"),
        (
            "WITH RECURSIVE bad AS (SELECT 1 AS n FROM bad UNION ALL SELECT n FROM bad) SELECT * FROM bad",
            "recursive reference to query \"bad\" must not appear within its non-recursive term",
        ),
        ("WITH RECURSIVE nounion AS (SELECT n FROM nounion) SELECT * FROM nounion", "does not have the form"),
        ("WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t) SELECT n FROM t LIMIT 5", "recursed more than"),
    ] {
        let e = c.simple_query(sql).await.expect_err(sql);
        let text = e.as_db_error().map(|d| d.message().to_string()).unwrap_or_else(|| e.to_string());
        assert!(text.contains(needle), "{sql}: {text}");
    }
}

