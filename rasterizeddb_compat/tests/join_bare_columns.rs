//! A column only one relation of a join has, written bare in a select-list
//! or ORDER BY expression: `CASE WHEN revenue > 5 …` over `ev JOIN u`. Every
//! expectation is PostgreSQL 17's (`docker run --rm postgres:17`).
//!
//! The combined row names every column `<label>__<column>`, and only
//! qualified references were renamed to it, so a bare name inside an
//! expression was "'revenue' is neither a column nor a value", although the
//! same bare name worked as a plain select item and in the WHERE. A name two
//! relations have is PostgreSQL's 42702.

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
async fn bare_columns_in_join_expressions_match_postgresql() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE ev (id INT, user_id INT, revenue NUMERIC(10,2), qty INT)",
        "CREATE TABLE u (uid INT, name TEXT, vip BOOLEAN, score INT)",
        "INSERT INTO ev VALUES (1,1,10.50,2),(2,1,NULL,3),(3,2,20.00,NULL),(4,3,5.25,1),(5,9,1.00,1)",
        "INSERT INTO u VALUES (1,'a',true,10),(2,'b',false,NULL),(3,'c',NULL,30)",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT e.id, CASE WHEN revenue > 5 THEN 'big' ELSE 'small' END FROM ev e JOIN u ON e.user_id = u.uid ORDER BY e.id", &["1|big", "2|small", "3|big", "4|big"]),
        ("SELECT e.id, revenue * qty AS tot FROM ev e JOIN u ON e.user_id = u.uid ORDER BY e.id", &["1|21.00", "2|NULL", "3|NULL", "4|5.25"]),
        ("SELECT e.id, COALESCE(score, -1) + qty FROM ev e JOIN u ON e.user_id = u.uid ORDER BY e.id", &["1|12", "2|13", "3|NULL", "4|31"]),
        ("SELECT e.id, CASE WHEN vip THEN name ELSE 'x' END FROM ev e JOIN u ON e.user_id = u.uid ORDER BY e.id", &["1|a", "2|a", "3|x", "4|x"]),
        ("SELECT e.id, name || '-' || qty FROM ev e LEFT JOIN u ON e.user_id = u.uid ORDER BY e.id", &["1|a-2", "2|a-3", "3|NULL", "4|c-1", "5|NULL"]),
        ("SELECT e.id, score IS NULL FROM ev e LEFT JOIN u ON e.user_id = u.uid ORDER BY e.id", &["1|f", "2|f", "3|t", "4|f", "5|t"]),
        ("SELECT e.id, name FROM ev e JOIN u ON e.user_id = u.uid ORDER BY revenue * -1, e.id", &["3|b", "1|a", "4|c", "2|a"]),
        ("SELECT SUM(CASE WHEN vip THEN 1 ELSE 0 END) FROM ev e JOIN u ON e.user_id = u.uid", &["2"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
    let sql = "SELECT e.id, id + 1 FROM ev e JOIN ev f ON e.id = f.id ORDER BY e.id";
    let e = c.simple_query(sql).await.expect_err(sql);
    assert_eq!(e.code().map(|c| c.code()), Some("42702"), "{e}");
}
