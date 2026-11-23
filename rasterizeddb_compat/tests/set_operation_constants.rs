//! A set operation with a branch that has no `FROM` (`SELECT n FROM t UNION
//! SELECT 1.5`). Such a branch's types were unknown: a NUMERIC column beside
//! it was refused, an integer column beside `2.5` sent the constant as NULL
//! under the integer's description (a wrong answer), and a first branch with
//! no `FROM`, or a statement with none at all, was not read. The branch is
//! now typed by its constants, as PostgreSQL types them, and the column
//! resolved as PostgreSQL resolves it: `numeric` beside integers, `double
//! precision` beside a float. Every expected answer is PostgreSQL 17's
//! (`.claude/reviews/setc/p1.sql`, `p2.sql`).

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

#[tokio::test(flavor = "multi_thread")]
async fn constant_branches_take_postgresql_types() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, n NUMERIC(6,2), i INT, b BIGINT, s TEXT, f DOUBLE PRECISION)",
        "INSERT INTO t VALUES (1, 1.50, 10, 5000000000, 'a', 0.5), (2, 2.25, 20, 7, 'b', 1.25)",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT n FROM t UNION SELECT 1.5 ORDER BY 1", &["1.50", "2.25"]),
        ("SELECT n FROM t UNION ALL SELECT 3 ORDER BY 1", &["1.50", "2.25", "3"]),
        ("SELECT 1.5 UNION SELECT n FROM t ORDER BY 1", &["1.5", "2.25"]),
        ("SELECT n FROM t UNION SELECT 1.555 ORDER BY 1", &["1.50", "1.555", "2.25"]),
        // The wrong answer: 2.5 went out as NULL under `i`'s description.
        ("SELECT i FROM t UNION SELECT 2.5 ORDER BY 1", &["2.5", "10", "20"]),
        ("SELECT 2.5 UNION SELECT i FROM t ORDER BY 1", &["2.5", "10", "20"]),
        ("SELECT b FROM t UNION SELECT 2.5 ORDER BY 1", &["2.5", "7", "5000000000"]),
        ("SELECT n FROM t EXCEPT SELECT 1.50 ORDER BY 1", &["2.25"]),
        ("SELECT n, s FROM t UNION SELECT 9.99, 'z' ORDER BY 1", &["1.50|a", "2.25|b", "9.99|z"]),
        ("SELECT 1.5 AS v UNION SELECT 2.25 ORDER BY 1", &["1.5", "2.25"]),
        ("SELECT 1 UNION SELECT 2.5 ORDER BY 1", &["1", "2.5"]),
        ("SELECT NULL UNION SELECT n FROM t ORDER BY 1", &["1.50", "2.25", "NULL"]),
        ("SELECT n FROM t UNION SELECT 1e2 ORDER BY 1", &["1.50", "2.25", "100"]),
        ("SELECT 1 AS a, 'x' AS b UNION ALL SELECT 2, 'y' UNION ALL SELECT i, s FROM t ORDER BY a", &["1|x", "2|y", "10|a", "20|b"]),
        ("SELECT f FROM t UNION SELECT 1.5 ORDER BY 1", &["0.5", "1.25", "1.5"]),
        ("SELECT n FROM t UNION SELECT f FROM t ORDER BY 1", &["0.5", "1.25", "1.5", "2.25"]),
        ("SELECT count(*) FROM (SELECT i FROM t UNION SELECT 2.5) q", &["3"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
    // Described as the rows are typed: `numeric` beside an integer, `double
    // precision` beside a float, on both protocols.
    for (sql, oid, count) in [
        ("SELECT i FROM t UNION SELECT 2.5", 1700u32, 3usize),
        // 1.5 and 1.50 are one value.
        ("SELECT 1.5 UNION SELECT n FROM t", 1700, 2),
        ("SELECT n FROM t UNION SELECT f FROM t", 701, 4),
    ] {
        let described = c.prepare(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(described.columns()[0].type_().oid(), oid, "{sql}");
        let got = c.query(sql, &[]).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(got.len(), count, "{sql}");
    }
}
