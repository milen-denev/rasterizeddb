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
//! - `SELECT DISTINCT ON (k, …)`, single-table; and over a join, a GROUP BY,
//!   an aggregate or a window function, through a derived table
//!   (`core::rql::distinct_on`).
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

/// `SELECT DISTINCT ON` over a join (inner, left, comma), over a GROUP BY,
/// with an aggregate or a window function in the select list or the ORDER BY,
/// under a WITH, with LIMIT/OFFSET and through the extended protocol: each
/// answered as PostgreSQL 17 answers it, with its column names. Each was
/// refused ("not supported yet").
#[tokio::test(flavor = "multi_thread")]
async fn distinct_on_over_joins_and_aggregates_matches_postgresql() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE ev (id INT, user_id INT, kind TEXT, ts INT, revenue NUMERIC(10,2))",
        "CREATE TABLE users (uid INT, name TEXT, tier TEXT)",
        "INSERT INTO ev VALUES (1,1,'a',10,1.00),(2,1,'b',30,2.00),(3,1,'a',20,3.00),(4,2,'a',5,4.00),(5,2,'b',50,NULL),(6,NULL,'c',1,6.00),(7,NULL,'c',2,7.00),(8,3,'a',8,8.00)",
        "INSERT INTO users VALUES (1,'ann','gold'),(2,'bob','silver'),(3,'cy','gold'),(4,'dee',NULL)",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT DISTINCT ON (u.tier) u.tier, u.name, e.ts FROM ev e JOIN users u ON e.user_id = u.uid ORDER BY u.tier, e.ts DESC", &["gold|ann|30", "silver|bob|50"]),
        ("SELECT DISTINCT ON (u.uid) u.uid, e.id FROM users u LEFT JOIN ev e ON e.user_id = u.uid ORDER BY u.uid, e.id DESC NULLS LAST", &["1|3", "2|5", "3|8", "4|NULL"]),
        ("SELECT DISTINCT ON (1) u.name, e.kind FROM users u, ev e WHERE e.user_id = u.uid ORDER BY 1, e.revenue DESC NULLS LAST", &["ann|a", "bob|a", "cy|a"]),
        ("SELECT DISTINCT ON (user_id) user_id, kind, count(*) AS n FROM ev GROUP BY user_id, kind ORDER BY user_id, n DESC, kind", &["1|a|2", "2|a|1", "3|a|1", "NULL|c|2"]),
        ("SELECT DISTINCT ON (user_id) user_id, kind FROM ev GROUP BY user_id, kind ORDER BY user_id, sum(ts) DESC", &["1|a", "2|b", "3|a", "NULL|c"]),
        ("SELECT DISTINCT ON (kind) kind, id, row_number() OVER (PARTITION BY kind ORDER BY ts DESC) AS rn FROM ev ORDER BY kind, id", &["a|1|2", "b|2|2", "c|6|2"]),
        ("SELECT DISTINCT ON (kind) kind, id FROM ev ORDER BY kind, rank() OVER (ORDER BY ts) DESC", &["a|3", "b|5", "c|7"]),
        ("SELECT DISTINCT ON (u.tier) u.tier, u.name, sum(e.ts) AS total FROM ev e JOIN users u ON e.user_id = u.uid GROUP BY u.tier, u.name ORDER BY u.tier NULLS FIRST, total DESC LIMIT 2", &["gold|ann|60", "silver|bob|55"]),
        ("SELECT DISTINCT ON (u.tier) u.tier, e.ts * 2 FROM ev e JOIN users u ON e.user_id = u.uid ORDER BY u.tier, e.ts * 2", &["gold|16", "silver|10"]),
        ("WITH g AS (SELECT * FROM users WHERE tier IS NOT NULL) SELECT DISTINCT ON (g.tier) g.tier, e.id FROM g JOIN ev e ON e.user_id = g.uid ORDER BY g.tier, e.id", &["gold|1", "silver|4"]),
        ("SELECT DISTINCT ON (t) u.tier AS t, e.id FROM ev e JOIN users u ON e.user_id=u.uid ORDER BY t, e.id DESC", &["gold|8", "silver|5"]),
        ("SELECT DISTINCT ON (e.kind) u.name FROM ev e JOIN users u ON e.user_id=u.uid ORDER BY e.kind, u.name", &["ann", "ann"]),
        ("SELECT DISTINCT ON (u.tier) u.tier, e.id FROM ev e JOIN users u ON e.user_id = u.uid WHERE e.ts > 5 ORDER BY u.tier, e.id", &["gold|1", "silver|5"]),
        ("SELECT DISTINCT ON (user_id) user_id, count(*) FILTER (WHERE kind = 'a') AS na, max(ts) FROM ev GROUP BY user_id, kind ORDER BY user_id, max(ts) DESC", &["1|0|30", "2|0|50", "3|1|8", "NULL|0|2"]),
        ("SELECT DISTINCT ON (u.tier) u.tier, e.id FROM ev e JOIN users u ON e.user_id = u.uid ORDER BY u.tier, e.id LIMIT 1 OFFSET 1", &["silver|4"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }

    // Described (extended protocol) with PostgreSQL's names, and run with a
    // parameter: over a join, and with windows.
    let sql = "SELECT DISTINCT ON (u.tier) u.tier, e.ts * 2, e.id FROM ev e JOIN users u ON e.user_id = u.uid \
               WHERE e.ts > $1 ORDER BY u.tier, e.ts * 2";
    let statement = c.prepare(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let names: Vec<&str> = statement.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, ["tier", "?column?", "id"]);
    let got: Vec<(String, i32, i32)> = c
        .query(&statement, &[&5i32])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(got, [("gold".to_string(), 16, 8), ("silver".to_string(), 100, 5)]);
    let sql = "SELECT DISTINCT ON (kind) kind, count(*) OVER (), max(id) OVER () FROM ev WHERE ts > $1 ORDER BY kind, id";
    let statement = c.prepare(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let names: Vec<&str> = statement.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, ["kind", "count", "max"]);
    let got: Vec<(String, i64, i32)> = c
        .query(&statement, &[&5i32])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(got, [("a".to_string(), 5, 8), ("b".to_string(), 5, 8)]);

    // 42P10 over a join, as on one table.
    let sql = "SELECT DISTINCT ON (u.tier) u.tier FROM ev e JOIN users u ON e.user_id = u.uid ORDER BY e.id";
    let e = c.simple_query(sql).await.expect_err(sql);
    assert_eq!(e.code().map(|c| c.code()), Some("42P10"), "{e}");
    // Still refused, never answered wrong: `*` (its columns are not named
    // here), an ambiguous output name (PostgreSQL's 42702), and a window over
    // a join (refused without DISTINCT ON too).
    for sql in [
        "SELECT DISTINCT ON (u.tier) * FROM ev e JOIN users u ON e.user_id = u.uid ORDER BY u.tier",
        "SELECT DISTINCT ON (u.tier) u.tier, count(*) OVER () FROM ev e JOIN users u ON e.user_id = u.uid ORDER BY u.tier",
        "SELECT DISTINCT ON (id) u.uid AS id, e.id FROM ev e JOIN users u ON e.user_id = u.uid ORDER BY id",
    ] {
        assert!(c.simple_query(sql).await.is_err(), "{sql}");
    }
}

/// `date_part(field, x)` with the field read from the row, each value's field
/// checked against its source as a constant one is, a NULL field giving
/// NULL: answered as PostgreSQL 17 answers it. It was refused ("reads its
/// field from a constant here").
#[tokio::test(flavor = "multi_thread")]
async fn date_part_with_a_field_from_the_row_matches_postgresql() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE dp (id INT, unit TEXT, ts TIMESTAMP, d DATE, iv INTERVAL)",
        "INSERT INTO dp VALUES (1,'year','2024-03-15 10:20:30','2024-03-15','1 day 2 hours'),\
         (2,'Month','2024-03-15 10:20:30','2024-03-15','3 months'),(3,'hour','2024-03-15 10:20:30','2024-03-15','5 hours'),\
         (4,NULL,'2024-03-15 10:20:30','2024-03-15','1 day'),(5,'epoch','2024-03-15 10:20:30','2024-03-15','1 minute'),\
         (6,'dow','2024-03-15 10:20:30','2024-03-15',NULL)",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT id, date_part(unit, ts) FROM dp ORDER BY id", &["1|2024", "2|3", "3|10", "4|NULL", "5|1710498030", "6|5"]),
        ("SELECT id, date_part(unit, d) FROM dp ORDER BY id", &["1|2024", "2|3", "3|0", "4|NULL", "5|1710460800", "6|5"]),
        ("SELECT id, date_part(unit, iv) FROM dp ORDER BY id", &["1|0", "2|3", "3|5", "4|NULL", "5|60", "6|NULL"]),
        ("SELECT id FROM dp WHERE date_part(unit, ts) > 10 ORDER BY id", &["1", "5"]),
        ("SELECT id, date_part(upper(unit), ts) FROM dp WHERE id = 1", &["1|2024"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
    for sql in [
        "SELECT date_part(unit, id) FROM dp",
        "SELECT id, date_part('bogus' || unit, ts) FROM dp WHERE id = 1",
        // An interval has no weekday, whichever row names it.
        "SELECT date_part(CASE WHEN id > 0 THEN 'dow' END, iv) FROM dp WHERE id = 1",
    ] {
        assert!(c.simple_query(sql).await.is_err(), "{sql}");
    }
}

/// `SELECT DISTINCT … ORDER BY … NULLS FIRST/LAST`, every placement, with
/// ordinals, an alias, an expression, LIMIT and OFFSET: answered as
/// PostgreSQL 17 answers it. Any placement was refused under DISTINCT ("not
/// supported in this position yet"); the default one is the scan's own, the
/// other is sorted outside a derived table (`core::rql::distinct_on`).
#[tokio::test(flavor = "multi_thread")]
async fn distinct_with_a_nulls_placement_matches_postgresql() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE dn (id INT PRIMARY KEY, n NUMERIC, i INT, t TEXT)",
        "INSERT INTO dn VALUES (1, 0, 0, 'z'), (2, NULL, NULL, NULL), (3, 0.00, 0, 'z'), (4, 1, 1, 'a'), (5, -1, -1, 'b'), (6, NULL, 7, 'b')",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT DISTINCT n FROM dn ORDER BY n NULLS LAST", &["-1", "0", "1", "NULL"]),
        ("SELECT DISTINCT n FROM dn ORDER BY n NULLS FIRST", &["NULL", "-1", "0", "1"]),
        ("SELECT DISTINCT n FROM dn ORDER BY n DESC NULLS FIRST", &["NULL", "1", "0", "-1"]),
        ("SELECT DISTINCT n FROM dn ORDER BY n DESC NULLS LAST", &["1", "0", "-1", "NULL"]),
        ("SELECT DISTINCT i, t FROM dn ORDER BY t NULLS FIRST, i DESC", &["NULL|NULL", "1|a", "7|b", "-1|b", "0|z"]),
        ("SELECT DISTINCT i, t FROM dn ORDER BY 2 DESC NULLS LAST, 1 LIMIT 3", &["0|z", "-1|b", "7|b"]),
        ("SELECT DISTINCT i, t FROM dn ORDER BY 2 DESC NULLS LAST, 1 OFFSET 2", &["7|b", "1|a", "NULL|NULL"]),
        ("SELECT DISTINCT t AS label FROM dn ORDER BY label NULLS FIRST", &["NULL", "a", "b", "z"]),
        ("SELECT DISTINCT n FROM dn WHERE id > 1 ORDER BY n NULLS FIRST", &["NULL", "-1", "0.00", "1"]),
        ("SELECT DISTINCT i * 2 FROM dn ORDER BY i * 2 NULLS FIRST", &["NULL", "-2", "0", "2", "14"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
    // A sort key outside a DISTINCT's select list: refused, as PostgreSQL
    // refuses it.
    let sql = "SELECT DISTINCT n FROM dn ORDER BY i NULLS FIRST";
    assert!(c.simple_query(sql).await.is_err(), "{sql}");
}
