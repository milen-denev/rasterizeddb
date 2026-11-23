//! `IN` and `BETWEEN` inside a condition the evaluator reads: `CASE WHEN s
//! IN ('a', 'b')`, `FILTER (WHERE n BETWEEN 1 AND 4)`, `CHECK (a NOT IN (5,
//! 6))`. Every expectation is PostgreSQL 17's (`docker run --rm postgres:17`).
//!
//! The condition reader had neither: an IN list was "not a comparison", and a
//! BETWEEN's AND was taken for a conjunction, so `n BETWEEN 2 AND 4` became
//! `n BETWEEN 2` and `4`. They are the comparisons they mean, under SQL's
//! three values, so a NULL in the list or the value is PostgreSQL's UNKNOWN.

use rasterizeddb_compat::TestServer;
use tokio_postgres::{Client, SimpleQueryMessage};

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

#[tokio::test(flavor = "multi_thread")]
async fn in_and_between_in_conditions_match_postgresql() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE t (id INT, s TEXT, n INT, v NUMERIC(10,2))",
        "INSERT INTO t VALUES (1,'a',1,1.50),(2,'b',2,2.25),(3,'c',NULL,NULL),(4,'d',4,10.00),(5,NULL,5,5.00)",
        "CREATE TABLE ck (a INT, CHECK (a BETWEEN 1 AND 10), CHECK (a NOT IN (5, 6)))",
        "INSERT INTO ck VALUES (1)",
        "INSERT INTO ck VALUES (NULL)",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    for refused in ["INSERT INTO ck VALUES (11)", "INSERT INTO ck VALUES (5)"] {
        let e = c.simple_query(refused).await.expect_err(refused);
        assert_eq!(e.code().map(|c| c.code()), Some("23514"), "{refused}: {e}");
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT id, CASE WHEN s IN ('a', 'b') THEN 'ab' ELSE 'other' END FROM t ORDER BY id", &["1|ab", "2|ab", "3|other", "4|other", "5|other"]),
        ("SELECT id, CASE WHEN n NOT IN (1, 2) THEN 'x' ELSE 'y' END FROM t ORDER BY id", &["1|y", "2|y", "3|y", "4|x", "5|x"]),
        ("SELECT id, CASE WHEN n IN (1, NULL) THEN 't' WHEN n NOT IN (1, NULL) THEN 'f' ELSE 'u' END FROM t ORDER BY id", &["1|t", "2|u", "3|u", "4|u", "5|u"]),
        ("SELECT COUNT(*) FILTER (WHERE s IN ('a', 'c', 'z')) FROM t", &["2"]),
        ("SELECT COUNT(*) FILTER (WHERE n IN (2, 4) OR s = 'a') FROM t", &["3"]),
        ("SELECT id, CASE WHEN n BETWEEN 2 AND 4 THEN 'mid' ELSE '-' END FROM t ORDER BY id", &["1|-", "2|mid", "3|-", "4|mid", "5|-"]),
        ("SELECT id, CASE WHEN v NOT BETWEEN 2 AND 6 THEN 'out' ELSE 'in' END FROM t ORDER BY id", &["1|out", "2|in", "3|in", "4|out", "5|in"]),
        ("SELECT COUNT(*) FILTER (WHERE n BETWEEN 1 AND 4 AND s <> 'b') FROM t", &["2"]),
        ("SELECT COUNT(*) FILTER (WHERE n BETWEEN 1 AND 2 OR n BETWEEN 4 AND 5) FROM t", &["4"]),
        ("SELECT id, CASE WHEN v IN (1.5, 10) THEN 'hit' END FROM t ORDER BY id", &["1|hit", "2|NULL", "3|NULL", "4|hit", "5|NULL"]),
        ("SELECT SUM(CASE WHEN s IN ('a','b','d') THEN v ELSE 0 END) FROM t", &["13.75"]),
        ("SELECT a FROM ck ORDER BY a", &["1", "NULL"]),
        ("SELECT id FROM t WHERE n IN (1, 2) ORDER BY id", &["1", "2"]),
        ("SELECT id FROM t WHERE n BETWEEN 2 AND 4 AND id > 2 ORDER BY id", &["4"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
}
