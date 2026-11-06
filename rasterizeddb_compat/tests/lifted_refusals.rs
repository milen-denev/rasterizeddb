//! Refusals lifted, each answered as PostgreSQL 17 answers it (`docker run
//! --rm postgres:17`):
//!
//! - `ORDER BY user_id, rnk`: a window function's output after another key
//!   (it had to lead). The whole ORDER BY is sorted at the window stage now,
//!   a window key reading its value and any other key the row as fetched.
//! - Two window functions in one select item: `LEAD(x) OVER w - LAG(x) OVER w`.
//! - `round(v, n)` with the places read from the row, each value at its own
//!   scale.
//! - `version()` inside an expression.
//! - `SELECT DISTINCT ON (k, …)`, single-table (a join still refuses it).
//! - `IN (SELECT …)` inside a CASE or a FILTER condition (pinned; it worked
//!   once the condition reader had IN, the subquery being substituted first).

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
async fn lifted_refusals_match_postgresql() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE ev (id INT, user_id INT, revenue NUMERIC(10,2), qty INT)",
        "CREATE TABLE vip (uid INT)",
        "INSERT INTO ev VALUES (1,1,10.50,2),(2,1,NULL,3),(3,2,20.00,NULL),(4,3,5.25,1),(5,3,7.00,0),(6,2,0,4)",
        "INSERT INTO vip VALUES (1),(3)",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT id, RANK() OVER (ORDER BY revenue DESC) AS rnk FROM ev ORDER BY user_id, rnk, id", &["2|1", "1|3", "3|2", "6|6", "5|4", "4|5"]),
        ("SELECT id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id) AS rn FROM ev ORDER BY user_id DESC, rn DESC", &["5|2", "4|1", "6|2", "3|1", "2|2", "1|1"]),
        ("SELECT id, revenue, RANK() OVER (ORDER BY revenue) AS r FROM ev ORDER BY revenue NULLS FIRST, r LIMIT 4", &["2|NULL|6", "6|0.00|1", "4|5.25|2", "5|7.00|3"]),
        ("SELECT id, ROW_NUMBER() OVER (ORDER BY id) - RANK() OVER (ORDER BY revenue) AS d FROM ev ORDER BY id", &["1|-3", "2|-4", "3|-2", "4|2", "5|2", "6|5"]),
        ("SELECT id, LEAD(revenue) OVER (ORDER BY id) - LAG(revenue) OVER (ORDER BY id) AS d FROM ev ORDER BY id", &["1|NULL", "2|9.50", "3|NULL", "4|-13.00", "5|-5.25", "6|NULL"]),
        ("SELECT id, SUM(qty) OVER (PARTITION BY user_id) * 100 / SUM(qty) OVER () AS pct FROM ev ORDER BY id", &["1|50", "2|50", "3|40", "4|10", "5|10", "6|40"]),
        ("SELECT id, round(revenue, qty) FROM ev ORDER BY id", &["1|10.50", "2|NULL", "3|NULL", "4|5.3", "5|7", "6|0.0000"]),
        ("SELECT id, round(revenue, user_id - 2) FROM ev ORDER BY id", &["1|10", "2|NULL", "3|20", "4|5.3", "5|7.0", "6|0"]),
        ("SELECT version() IS NOT NULL", &["t"]),
        ("SELECT length(version()) > 5", &["t"]),
        ("SELECT id, CASE WHEN user_id IN (SELECT uid FROM vip) THEN 'v' ELSE '-' END FROM ev ORDER BY id", &["1|v", "2|v", "3|-", "4|v", "5|v", "6|-"]),
        ("SELECT COUNT(*) FILTER (WHERE user_id IN (SELECT uid FROM vip)) FROM ev", &["4"]),
        ("SELECT id, CASE WHEN user_id NOT IN (SELECT uid FROM vip) THEN 'x' END FROM ev ORDER BY id", &["1|NULL", "2|NULL", "3|x", "4|NULL", "5|NULL", "6|x"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
}


/// `SELECT DISTINCT ON (k, …)`: the first row of each key in the statement's
/// order, `LIMIT`/`OFFSET` over what is kept, on a plain and a partitioned
/// table; an ORDER BY not led by the keys is PostgreSQL's 42P10. It was
/// refused outright.
#[tokio::test(flavor = "multi_thread")]
async fn distinct_on_matches_postgresql() {
    let root = std::env::temp_dir().join(format!("lifted_distinct_on_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let server = TestServer::start_file_backed(&root.to_string_lossy()).await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE ev (id INT, user_id INT, kind TEXT, ts INT, revenue NUMERIC(10,2))",
        "CREATE TABLE pev (tenant_id BIGINT NOT NULL, id INT, user_id INT, kind TEXT, ts INT, revenue NUMERIC(10,2)) PARTITION BY HASH (tenant_id)",
        "INSERT INTO ev VALUES (1,1,'a',10,1.00),(2,1,'b',30,2.00),(3,1,'a',20,3.00),(4,2,'a',5,4.00),(5,2,'b',50,NULL),(6,NULL,'c',1,6.00),(7,NULL,'c',2,7.00),(8,3,'a',8,8.00)",
        "INSERT INTO pev VALUES (1,1,1,'a',10,1.00),(1,2,1,'b',30,2.00),(1,3,1,'a',20,3.00),(2,4,2,'a',5,4.00),(2,5,2,'b',50,NULL),(3,6,NULL,'c',1,6.00),(3,7,NULL,'c',2,7.00),(4,8,3,'a',8,8.00)",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT DISTINCT ON (user_id) user_id, id, ts FROM ev ORDER BY user_id, ts DESC", &["1|2|30", "2|5|50", "3|8|8", "NULL|7|2"]),
        ("SELECT DISTINCT ON (user_id) id, kind FROM ev ORDER BY user_id, ts", &["1|a", "4|a", "8|a", "6|c"]),
        ("SELECT DISTINCT ON (user_id, kind) user_id, kind, id FROM ev ORDER BY user_id, kind, ts DESC", &["1|a|3", "1|b|2", "2|a|4", "2|b|5", "3|a|8", "NULL|c|7"]),
        ("SELECT DISTINCT ON (kind) kind, revenue FROM ev ORDER BY kind, revenue DESC NULLS LAST", &["a|8.00", "b|2.00", "c|7.00"]),
        ("SELECT DISTINCT ON (user_id) * FROM ev ORDER BY user_id, ts DESC", &["2|1|b|30|2.00", "5|2|b|50|NULL", "8|3|a|8|8.00", "7|NULL|c|2|7.00"]),
        ("SELECT DISTINCT ON (user_id) user_id, id FROM ev WHERE kind = 'a' ORDER BY user_id, id DESC", &["1|3", "2|4", "3|8"]),
        ("SELECT DISTINCT ON (user_id) user_id, ts FROM ev ORDER BY user_id, ts DESC LIMIT 2", &["1|30", "2|50"]),
        ("SELECT DISTINCT ON (user_id) user_id, ts FROM ev ORDER BY user_id, ts DESC LIMIT 2 OFFSET 1", &["2|50", "3|8"]),
        ("SELECT DISTINCT ON (ts % 2) ts % 2 AS parity, id FROM ev ORDER BY ts % 2, id", &["0|1", "1|4"]),
        ("SELECT DISTINCT ON (user_id) user_id, id, ts FROM pev ORDER BY user_id, ts DESC", &["1|2|30", "2|5|50", "3|8|8", "NULL|7|2"]),
        ("SELECT COUNT(*) FROM (SELECT DISTINCT ON (user_id) user_id, id FROM ev) x", &["4"]),
        ("SELECT DISTINCT ON (u) user_id AS u, id FROM ev ORDER BY u, id DESC", &["1|3", "2|5", "3|8", "NULL|7"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
    let sql = "SELECT DISTINCT ON (user_id) user_id, id FROM ev ORDER BY id";
    let e = c.simple_query(sql).await.expect_err(sql);
    assert_eq!(e.code().map(|c| c.code()), Some("42P10"), "{e}");
    drop(c);
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}
