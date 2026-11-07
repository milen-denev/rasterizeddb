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
//! - `CREATE TABLE … AS` (`core::rql::ctas`): it answered "Table already
//!   exists" (42P07) and created nothing.

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

/// SQL's typed literal (`DATE '…'`, `TIMESTAMP WITH TIME ZONE '…'`, `NUMERIC
/// '…'`, `BOOLEAN '…'`, …) as the value it means: into a text column through
/// INSERT and UPDATE, into typed columns, and compared in a WHERE, answered as
/// PostgreSQL 17 answers it. A text column stored the literal's own source
/// characters (`DATE '2024-01-02'`, silently); NUMERIC and BOOLEAN columns
/// refused it; `d = DATE '…'` failed to plan.
#[tokio::test(flavor = "multi_thread")]
async fn typed_literals_are_the_values_they_mean() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE tl (id INT, t TEXT, v VARCHAR(40))",
        "INSERT INTO tl VALUES (1, TIMESTAMPTZ '2024-01-02 03:04:05+00', TIMESTAMP WITH TIME ZONE '2024-01-02 03:04:05+02')",
        "INSERT INTO tl VALUES (2, TIMESTAMP '2024-01-02 03:04:05', TIMESTAMP '2024-01-02 03:04:05.5')",
        "INSERT INTO tl VALUES (3, DATE '2024-01-02', DATE '2024-1-2')",
        "INSERT INTO tl VALUES (4, INTERVAL '1 day 2 hours', INTERVAL '90 minutes')",
        "INSERT INTO tl VALUES (5, NUMERIC '1.50', INTEGER '7')",
        "INSERT INTO tl VALUES (6, BOOLEAN 'yes', TIME '03:04:05')",
        "INSERT INTO tl (id, t) SELECT 8, TIMESTAMPTZ '2024-01-02 03:04:05+00'",
        "INSERT INTO tl VALUES (9, 'DATE ''y''', 'x')",
        "CREATE TABLE ty (id INT, ts TIMESTAMPTZ, d DATE, iv INTERVAL, n NUMERIC(10,2), b BOOLEAN)",
        "INSERT INTO ty VALUES (1, TIMESTAMPTZ '2024-01-02 03:04:05+02', DATE '2024-01-02', INTERVAL '1 day', NUMERIC '1.505', BOOLEAN 'yes')",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cases: &[(&str, &[&str])] = &[
        (
            "SELECT id, t, v FROM tl ORDER BY id",
            &[
                "1|2024-01-02 03:04:05+00|2024-01-02 01:04:05+00",
                "2|2024-01-02 03:04:05|2024-01-02 03:04:05.5",
                "3|2024-01-02|2024-01-02",
                "4|1 day 02:00:00|01:30:00",
                "5|1.50|7",
                "6|true|03:04:05",
                "8|2024-01-02 03:04:05+00|NULL",
                "9|DATE 'y'|x",
            ],
        ),
        ("SELECT * FROM ty", &["1|2024-01-02 01:04:05+00|2024-01-02|1 day|1.51|t"]),
        ("SELECT id FROM ty WHERE d = DATE '2024-01-02' AND ts > TIMESTAMPTZ '2024-01-01 00:00:00+00' AND iv < INTERVAL '2 days'", &["1"]),
        ("SELECT TIMESTAMPTZ '2024-01-02 03:04:05+00'::text, DATE '2024-01-02' || 'x'", &["2024-01-02 03:04:05+00|2024-01-02x"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
    c.simple_query("UPDATE tl SET t = DATE '2024-03-04', v = TIMESTAMPTZ '2024-01-02 03:04:05+00' WHERE id = 1").await.unwrap();
    assert_eq!(rows(&c, "SELECT t, v FROM tl WHERE id = 1").await, ["2024-03-04|2024-01-02 03:04:05+00"]);
}

/// `CREATE TABLE … AS`, each answer PostgreSQL 17's (`docker run --rm
/// postgres:17`): the values and declared types copied, no constraint
/// copied, `SELECT n` as the count, `WITH [NO] DATA`, a column list, `IF NOT
/// EXISTS`, a CTE or derived table in the query, and a failed query leaving
/// no table. Through the extended protocol too.
#[tokio::test(flavor = "multi_thread")]
async fn create_table_as_matches_postgresql() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE src (id INT PRIMARY KEY, n NUMERIC(10,2), v VARCHAR(20), t TEXT, d DATE, ts TIMESTAMPTZ, b BOOLEAN, r REAL, f FLOAT8, j JSONB, c CHAR(3))",
        "INSERT INTO src VALUES (1, 1.50, 'a', 'x', '2024-01-02', '2024-01-02 03:04:05+00', true, 0.1, 0.1, '{\"k\":1}', 'ab'), \
         (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \
         (3, -2.25, 'b', 'y', '2023-12-31', '2023-12-31 23:00:00+00', false, 2.5, 1e100, '[1,2]', 'c')",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let count = |sql: &'static str| {
        let c = &c;
        async move {
            let messages = c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
            messages
                .iter()
                .find_map(|m| match m {
                    SimpleQueryMessage::CommandComplete(n) => Some(*n),
                    _ => None,
                })
                .unwrap()
        }
    };
    // (statement, its count, a read of what it made, that read's rows)
    let cases: &[(&str, u64, &str, &[&str])] = &[
        ("CREATE TABLE c1 AS SELECT * FROM src", 3, "SELECT * FROM c1 ORDER BY id", &[
            "1|1.50|a|x|2024-01-02|2024-01-02 03:04:05+00|t|0.1|0.1|{\"k\": 1}|ab ",
            "2|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL",
            "3|-2.25|b|y|2023-12-31|2023-12-31 23:00:00+00|f|2.5|1e+100|[1, 2]|c  ",
        ]),
        ("CREATE TABLE c2 AS SELECT id, n * 2 AS dbl, upper(t), count(*) OVER () FROM src WHERE id > 1", 2,
         "SELECT * FROM c2 ORDER BY id", &["2|NULL|NULL|2", "3|-4.50|Y|2"]),
        ("CREATE TABLE c3 (a, b) AS SELECT id, t, v FROM src", 3, "SELECT a, b, v FROM c3 ORDER BY a", &["1|x|a", "2|NULL|NULL", "3|y|b"]),
        ("CREATE TABLE IF NOT EXISTS c1 AS SELECT 1", 0, "SELECT count(*) FROM c1", &["3"]),
        ("CREATE TABLE c5 AS SELECT * FROM src WITH NO DATA", 0, "SELECT count(*) FROM c5", &["0"]),
        ("CREATE TABLE c6 AS SELECT * FROM src WHERE id = 1 WITH DATA", 1, "SELECT id FROM c6", &["1"]),
        ("CREATE TABLE c7 AS WITH w AS (SELECT id, t FROM src WHERE id <> 2) SELECT * FROM w", 2,
         "SELECT * FROM c7 ORDER BY id", &["1|x", "3|y"]),
        ("CREATE TABLE c8 AS SELECT x.id, x.t FROM (SELECT * FROM src WHERE id < 3) x", 2, "SELECT * FROM c8 ORDER BY id", &["1|x", "2|NULL"]),
        ("CREATE TABLE c9 AS SELECT 1 AS one, 'a' AS lit, NULL AS nul, 2.5 AS num", 1, "SELECT * FROM c9", &["1|a|NULL|2.5"]),
        ("CREATE TABLE c10 AS SELECT t, count(*) AS c, sum(n) AS s FROM src GROUP BY t", 3,
         "SELECT * FROM c10 ORDER BY t NULLS FIRST", &["NULL|1|NULL", "x|1|1.50", "y|1|-2.25"]),
        ("CREATE TABLE c11 AS SELECT a.id, b.t FROM src a JOIN src b ON a.id = b.id", 3, "SELECT * FROM c11 ORDER BY id", &["1|x", "2|NULL", "3|y"]),
        ("CREATE TABLE c12 AS SELECT id FROM src ORDER BY id DESC LIMIT 2", 2, "SELECT * FROM c12 ORDER BY id", &["2", "3"]),
        ("CREATE TABLE c15 AS SELECT * FROM src WHERE id > 100", 0, "SELECT count(*) FROM c15", &["0"]),
        ("CREATE TABLE c18 AS SELECT DISTINCT t FROM src", 3, "SELECT * FROM c18 ORDER BY t NULLS FIRST", &["NULL", "x", "y"]),
        ("CREATE TABLE c19 AS SELECT id FROM src UNION ALL SELECT id FROM src", 6, "SELECT count(*) FROM c19", &["6"]),
        ("CREATE TABLE c22 AS SELECT DISTINCT ON (t) t, id FROM src ORDER BY t, id DESC", 3,
         "SELECT * FROM c22 ORDER BY t NULLS FIRST", &["NULL|2", "x|1", "y|3"]),
        ("CREATE TABLE c26 AS TABLE src", 3, "SELECT count(*) FROM c26", &["3"]),
        ("CREATE TABLE \"Mixed\" AS SELECT id FROM src", 3, "SELECT count(*) FROM \"Mixed\"", &["3"]),
    ];
    for (create, n, read, want) in cases {
        assert_eq!(count(create).await, *n, "{create}");
        assert_eq!(rows(&c, read).await, *want, "{create} then {read}");
    }
    // No constraint is copied: not the key, not NOT NULL. The declared
    // lengths are the column's type, so they are.
    for sql in ["INSERT INTO c1 (id) VALUES (1)", "INSERT INTO c1 (id) VALUES (NULL)"] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    for (sql, code) in [
        ("INSERT INTO c1 (id, v) VALUES (9, 'abcdefghijklmnopqrstuvwxyz')", "22001"),
        ("INSERT INTO c1 (id, c) VALUES (10, 'abcd')", "22001"),
        ("CREATE TABLE c1 AS SELECT 1", "42P07"),
        ("CREATE TABLE c4 (a, b, c, d) AS SELECT id, t FROM src", "42601"),
        ("CREATE TABLE c13 AS SELECT id, id FROM src", "42701"),
        ("CREATE TABLE c14 AS SELECT 1 + 1, 'z'", "42701"),
        ("CREATE TABLE c16 AS SELECT * FROM nosuch", "42P01"),
        ("CREATE TABLE c17 AS SELECT id, 1 / (id - 2) FROM src", "22012"),
        ("CREATE TABLE c25 AS VALUES (1, 'a')", "0A000"),
    ] {
        let e = c.simple_query(sql).await.expect_err(sql);
        assert_eq!(e.code().map(|s| s.code()), Some(code), "{sql}: {e}");
    }
    for gone in ["c4", "c13", "c14", "c16", "c17", "c25"] {
        let e = c.simple_query(&format!("SELECT 1 FROM {gone}")).await.expect_err(gone);
        assert_eq!(e.code().map(|s| s.code()), Some("42P01"), "{gone} was left behind: {e}");
    }
    // Parse, Describe, Bind, Execute: the count is the rows written.
    assert_eq!(c.execute("CREATE TABLE p1 AS SELECT id, t FROM src WHERE id <> 2", &[]).await.unwrap(), 2);
    assert_eq!(rows(&c, "SELECT * FROM p1 ORDER BY id").await, ["1|x", "3|y"]);
    let prepared = c.prepare("CREATE TABLE p2 AS SELECT id FROM src").await.unwrap();
    assert!(prepared.columns().is_empty());
    assert_eq!(c.execute(&prepared, &[]).await.unwrap(), 3);
}

/// A text value compared with a number or a boolean written without quotes
/// is PostgreSQL's 42883: it types `1` as an integer, `1.5` as a numeric and
/// `true` as a boolean, and has no operator between them and text. The engine
/// read the literal as text instead: `g = 1` matched only the text '1', and
/// `g > 1` compared 'x' with '1' and returned every row. A quoted literal is
/// untyped and takes the other side's type, as in PostgreSQL.
#[tokio::test(flavor = "multi_thread")]
async fn text_against_a_number_is_refused_as_postgresql_refuses_it() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE a (id INT, g TEXT, c CHAR(2), v VARCHAR(5))",
        "CREATE TABLE b (id INT, w INT)",
        "INSERT INTO a VALUES (1, '1', 'x', '5'), (2, 'x', '2', 'y')",
        "INSERT INTO b VALUES (1, 10)",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    for sql in [
        "SELECT id FROM a WHERE g = 1",
        "SELECT id FROM a WHERE g > 1",
        "SELECT id FROM a WHERE 1 = g",
        "SELECT id FROM a WHERE g <> 1.5",
        "SELECT id FROM a WHERE g = true",
        "SELECT id FROM a WHERE v = 5",
        "SELECT id FROM a WHERE c = 2",
        "SELECT id, g = 1 FROM a ORDER BY id",
        "SELECT a.id FROM a JOIN b ON a.g = b.id",
    ] {
        let e = c.simple_query(sql).await.expect_err(sql);
        assert_eq!(e.code().map(|s| s.code()), Some("42883"), "{sql}: {e}");
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT id FROM a WHERE g = '1' ORDER BY id", &["1"]),
        ("SELECT id FROM a WHERE id = '2'", &["2"]),
        ("SELECT id FROM a WHERE g IS NULL OR g = 'x' ORDER BY id", &["2"]),
        ("SELECT id, g = '1' FROM a ORDER BY id", &["1|t", "2|f"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
}

/// `trunc`, a quantified subquery (`= ANY`, `<> ALL`), `IN` as a value, and
/// a number with a fraction assigned to an integer column, each as
/// PostgreSQL 17 answers (`docker run --rm postgres:17`). They were "function
/// 'trunc' is not supported yet", "a subquery used as a value returned 2
/// rows", "function 'id IN' is not supported yet" and "invalid input syntax
/// for type integer". `> ANY (subquery)` is refused as not supported.
#[tokio::test(flavor = "multi_thread")]
async fn trunc_quantified_subqueries_in_values_and_integer_rounding_match_postgresql() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE a (id INT, g TEXT, v NUMERIC(10,2), f FLOAT8)",
        "CREATE TABLE b (id INT, w INT)",
        "INSERT INTO a VALUES (1, 'x', 1.55, 2.5), (2, 'x', -1.55, -2.5), (3, 'y', NULL, NULL), (4, NULL, 10.00, 3.7), (2, 'x', -1.55, -2.5)",
        "INSERT INTO b VALUES (1, 10), (2, 20), (5, 50), (2, 21)",
        "CREATE TABLE ti (i INT, s SMALLINT, l BIGINT)",
        "INSERT INTO ti VALUES (2.5, 2.5, 2.5), (3.5, -2.5, 1.4999)",
        "INSERT INTO ti (i) VALUES (1.5::float8)",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT trunc(v), trunc(f), trunc(v, 1), trunc(-2.567, 2), trunc(v, -1) FROM a ORDER BY id, v", &[
            "1|2|1.5|-2.56|0", "-1|-2|-1.5|-2.56|0", "-1|-2|-1.5|-2.56|0", "NULL|NULL|NULL|-2.56|NULL", "10|3|10.0|-2.56|10",
        ]),
        ("SELECT trunc(1234.5678, -2), trunc(5)", &["1200|5"]),
        ("SELECT id FROM a WHERE id = ANY (SELECT id FROM b) ORDER BY id", &["1", "2", "2"]),
        ("SELECT id FROM a WHERE id <> ALL (SELECT id FROM b) ORDER BY id", &["3", "4"]),
        ("SELECT id, id IN (SELECT id FROM b) AS hit FROM a ORDER BY id, g", &["1|t", "2|t", "2|t", "3|f", "4|f"]),
        ("SELECT id, id IN (1, 3) AS hit, g NOT IN ('x') AS nx FROM a ORDER BY id, g", &[
            "1|t|f", "2|f|f", "2|f|f", "3|t|t", "4|f|NULL",
        ]),
        ("SELECT id, v IN (SELECT w FROM b) FROM a ORDER BY id", &["1|f", "2|f", "2|f", "3|NULL", "4|t"]),
        ("SELECT * FROM ti ORDER BY i", &["2|NULL|NULL", "3|3|3", "4|-3|1"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
    c.simple_query("UPDATE ti SET i = 7.5 WHERE i = 4").await.expect("update");
    assert_eq!(rows(&c, "SELECT * FROM ti ORDER BY i").await, ["2|NULL|NULL", "3|3|3", "8|-3|1"]);
    // A quoted fraction is a string, which an integer refuses as PostgreSQL does.
    let e = c.simple_query("INSERT INTO ti (i) VALUES ('2.5')").await.expect_err("quoted fraction");
    assert_eq!(e.code().map(|s| s.code()), Some("22P02"), "{e}");
    let e = c.simple_query("SELECT id FROM a WHERE id > ANY (SELECT w FROM b)").await.expect_err("> ANY");
    assert_eq!(e.code().map(|s| s.code()), Some("0A000"), "{e}");
}

/// `INTERSECT` and `EXCEPT`, with and without `ALL`, as PostgreSQL 17
/// answers them (`docker run --rm postgres:17`): NULLs equal, bag counts for
/// `ALL`, `INTERSECT` binding tighter than `UNION` and `EXCEPT`, a
/// parenthesised operand, a join branch, and a trailing ORDER BY and LIMIT.
/// They were refused by name ("INTERSECT is not supported").
#[tokio::test(flavor = "multi_thread")]
async fn intersect_and_except_match_postgresql() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE a (id INT, g TEXT, n NUMERIC(10,2), b BIGINT)",
        "CREATE TABLE b (id INT, w INT, g TEXT)",
        "INSERT INTO a VALUES (1, 'x', 1.50, 1), (2, 'x', NULL, 2), (2, 'x', NULL, 2), (3, NULL, 2.00, 3), (4, NULL, NULL, 4), (4, NULL, NULL, 4), (4, NULL, NULL, 4)",
        "INSERT INTO b VALUES (1, 10, 'x'), (2, 20, 'x'), (5, 50, NULL), (2, 21, 'x'), (4, 40, NULL), (4, 41, NULL)",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT id, g FROM a INTERSECT SELECT id, g FROM b ORDER BY id", &["1|x", "2|x", "4|NULL"]),
        ("SELECT id, g FROM a INTERSECT ALL SELECT id, g FROM b ORDER BY id", &["1|x", "2|x", "2|x", "4|NULL", "4|NULL"]),
        ("SELECT id, g FROM a EXCEPT SELECT id, g FROM b ORDER BY id", &["3|NULL"]),
        ("SELECT id, g FROM a EXCEPT ALL SELECT id, g FROM b ORDER BY id", &["3|NULL", "4|NULL"]),
        ("SELECT id FROM a EXCEPT ALL SELECT id FROM b ORDER BY id", &["3", "4"]),
        ("SELECT id FROM b EXCEPT SELECT id FROM a ORDER BY id", &["5"]),
        ("SELECT id FROM a INTERSECT SELECT id FROM b ORDER BY id DESC LIMIT 2", &["4", "2"]),
        ("SELECT id FROM a INTERSECT SELECT id FROM b UNION SELECT 9 ORDER BY 1", &["1", "2", "4", "9"]),
        // INTERSECT first: a UNION (b INTERSECT b), not (a UNION b) INTERSECT b.
        ("SELECT id FROM a UNION SELECT id FROM b INTERSECT SELECT id FROM b ORDER BY 1", &["1", "2", "3", "4", "5"]),
        ("SELECT id FROM a EXCEPT SELECT id FROM b EXCEPT SELECT 3 ORDER BY 1", &[]),
        ("SELECT id FROM a UNION ALL SELECT id FROM b EXCEPT SELECT 4 ORDER BY 1", &["1", "2", "3", "5"]),
        ("SELECT id FROM a EXCEPT (SELECT id FROM b INTERSECT SELECT 1) ORDER BY 1", &["2", "3", "4"]),
        ("SELECT b FROM a INTERSECT SELECT id FROM b ORDER BY 1", &["1", "2", "4"]),
        ("SELECT g FROM a INTERSECT DISTINCT SELECT g FROM b ORDER BY 1", &["x", "NULL"]),
        ("SELECT count(*) FROM (SELECT id FROM a INTERSECT SELECT id FROM b) s", &["3"]),
        ("SELECT id FROM a WHERE id IN (SELECT id FROM a EXCEPT SELECT id FROM b) ORDER BY id", &["3"]),
        ("SELECT a.id FROM a JOIN b ON a.id = b.id INTERSECT SELECT id FROM b WHERE w > 20 ORDER BY 1", &["2", "4"]),
        ("SELECT id FROM a EXCEPT (SELECT id FROM b INTERSECT SELECT id FROM b WHERE id = 1) ORDER BY 1", &["2", "3", "4"]),
        ("SELECT a.id FROM a JOIN b ON a.id = b.id UNION SELECT id FROM b WHERE w > 40 ORDER BY 1", &["1", "2", "4", "5"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
}

/// A window function over a join, as PostgreSQL 17 answers it (`docker run
/// --rm postgres:17`): computed over a derived table of the join's columns
/// (`core::rql::window_join`), with PostgreSQL's output names. It was refused
/// ("could not be read as an expression over this join"). A bare name two
/// relations share stays 42702.
#[tokio::test(flavor = "multi_thread")]
async fn window_functions_over_a_join_match_postgresql() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE a (id INT, g TEXT, v NUMERIC(10,2))",
        "CREATE TABLE b (id INT, w INT)",
        "INSERT INTO a VALUES (1, 'x', 1.55), (2, 'x', -1.55), (3, 'y', NULL), (4, NULL, 10.00), (2, 'x', -1.55)",
        "INSERT INTO b VALUES (1, 10), (2, 20), (5, 50), (2, 21)",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cases: &[(&str, &[&str])] = &[
        ("SELECT a.id, b.w, row_number() OVER (PARTITION BY a.id ORDER BY b.w) AS rn FROM a JOIN b ON a.id = b.id ORDER BY a.id, b.w", &["1|10|1", "2|20|1", "2|20|2", "2|21|3", "2|21|4"]),
        ("SELECT a.g, sum(b.w) OVER (PARTITION BY a.g) AS s FROM a JOIN b ON a.id = b.id ORDER BY 1, 2", &["x|92", "x|92", "x|92", "x|92", "x|92"]),
        ("SELECT a.id, rank() OVER (ORDER BY b.w DESC) FROM a, b WHERE a.id = b.id ORDER BY 2, 1", &["2|1", "2|1", "2|3", "2|3", "1|5"]),
        ("SELECT a.id, b.w, sum(b.w) OVER (ORDER BY b.w ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run FROM a JOIN b ON a.id = b.id WHERE b.w > 10 ORDER BY run", &["2|20|20", "2|20|40", "2|21|61", "2|21|82"]),
        ("SELECT w AS id, lag(w) OVER (ORDER BY w) FROM a JOIN b ON a.id = b.id ORDER BY id DESC LIMIT 3", &["21|20", "21|21", "20|10"]),
        ("SELECT DISTINCT a.g, count(*) OVER (PARTITION BY a.g) FROM a LEFT JOIN b ON a.id = b.id ORDER BY 1 NULLS FIRST", &["NULL|1", "x|5", "y|1"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
    let e = c.simple_query("SELECT id, rank() OVER (ORDER BY w) FROM a JOIN b ON a.id = b.id").await.expect_err("ambiguous");
    assert_eq!(e.code().map(|s| s.code()), Some("42702"), "{e}");
    // Described as it runs, under PostgreSQL's names.
    let prepared = c
        .prepare("SELECT a.id, b.w, row_number() OVER (PARTITION BY a.id ORDER BY b.w) FROM a JOIN b ON a.id = b.id")
        .await
        .expect("prepare");
    let names: Vec<&str> = prepared.columns().iter().map(|col| col.name()).collect();
    assert_eq!(names, ["id", "w", "row_number"]);
}

/// A boolean stored into a text column by `INSERT … SELECT` is PostgreSQL's
/// text for it, `true` or `false`. The rows were rebuilt as a `VALUES` list
/// with the boolean as the bare `1` or `0`, and a text column stored that.
#[tokio::test(flavor = "multi_thread")]
async fn insert_select_stores_a_boolean_as_postgresql_text() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE digit (n INT, one INT)",
        "INSERT INTO digit VALUES (0, 1), (1, 1), (2, 1), (3, 1)",
        "CREATE TABLE tsink (id INT, b TEXT)",
        "CREATE TABLE bsink (id INT, b BOOLEAN)",
        "INSERT INTO tsink (id, b) SELECT n, n = 2 FROM digit",
        "INSERT INTO bsink (id, b) SELECT n, n BETWEEN 1 AND 2 FROM digit",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(rows(&c, "SELECT * FROM tsink ORDER BY id").await, ["0|false", "1|false", "2|true", "3|false"]);
    assert_eq!(rows(&c, "SELECT * FROM bsink ORDER BY id").await, ["0|f", "1|t", "2|t", "3|f"]);
}
