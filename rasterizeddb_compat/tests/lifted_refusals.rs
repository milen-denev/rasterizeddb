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
