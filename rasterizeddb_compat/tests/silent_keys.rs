//! W2: keys and orders that answered silently wrong. A window beside a
//! constant, values of wire-format types ordered by their bytes, -0 against
//! 0 and NaN against NaN as keys, and bit strings ordered by length. Every
//! expectation is PostgreSQL 17's (`docker run --rm postgres:17`), read as
//! text with NULL shown as `NULL`.

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

/// Each row as `a|b|…`, a NULL as `NULL`.
async fn rows_n(c: &Client, sql: &str) -> Vec<String> {
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

async fn sqlstate(c: &Client, sql: &str) -> String {
    let e = c.simple_query(sql).await.expect_err(sql);
    e.code().map(|s| s.code().to_string()).unwrap_or_else(|| e.to_string())
}

/// Constants and row expressions in a select list with windows: each window reads its own column (a constant before it was summed in its place) and every item lands where it was written, around window-only columns; DISTINCT applies to the rows with their window values.
#[tokio::test(flavor = "multi_thread")]
async fn constants_and_windows_keep_their_places() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE t (id INT PRIMARY KEY, v INT, n NUMERIC, s TEXT)").await;
    run(&c, "INSERT INTO t VALUES (1, 10, 1.5, 'a'), (2, 20, 2.5, 'b'), (3, NULL, NULL, NULL)").await;
    assert_eq!(rows_n(&c, "SELECT 'x', id, sum(id) OVER () FROM t ORDER BY id").await, ["x|1|6", "x|2|6", "x|3|6"] as [&str; 3], "SELECT 'x', id, sum(id) OVER () FROM t ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT 1, id, sum(id) OVER () FROM t ORDER BY id").await, ["1|1|6", "1|2|6", "1|3|6"] as [&str; 3], "SELECT 1, id, sum(id) OVER () FROM t ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, 'x', sum(id) OVER () FROM t ORDER BY id").await, ["1|x|6", "2|x|6", "3|x|6"] as [&str; 3], "SELECT id, 'x', sum(id) OVER () FROM t ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT 'x', 'y', id, sum(v) OVER (), count(*) OVER (PARTITION BY s) FROM t ORDER BY id").await, ["x|y|1|30|1", "x|y|2|30|1", "x|y|3|30|1"] as [&str; 3], "SELECT 'x', 'y', id, sum(v) OVER (), count(*) OVER (PARTITION BY s) FROM t ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT 'x' AS c, id, row_number() OVER (ORDER BY id), sum(n) OVER (ORDER BY id) FROM t ORDER BY id").await, ["x|1|1|1.5", "x|2|2|4.0", "x|3|3|4.0"] as [&str; 3], "SELECT 'x' AS c, id, row_number() OVER (ORDER BY id), sum(n) OVER (ORDER BY id) FROM t ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT 'x', id, sum(id) OVER (), 'z', max(v) OVER () FROM t ORDER BY id").await, ["x|1|6|z|20", "x|2|6|z|20", "x|3|6|z|20"] as [&str; 3], "SELECT 'x', id, sum(id) OVER (), 'z', max(v) OVER () FROM t ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT 2.5, id, lag(s) OVER (ORDER BY id) FROM t ORDER BY id").await, ["2.5|1|NULL", "2.5|2|a", "2.5|3|b"] as [&str; 3], "SELECT 2.5, id, lag(s) OVER (ORDER BY id) FROM t ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT now() IS NOT NULL, id, sum(id) OVER () FROM t ORDER BY id").await, ["t|1|6", "t|2|6", "t|3|6"] as [&str; 3], "SELECT now() IS NOT NULL, id, sum(id) OVER () FROM t ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id * 2, id, sum(id) OVER () FROM t ORDER BY id").await, ["2|1|6", "4|2|6", "6|3|6"] as [&str; 3], "SELECT id * 2, id, sum(id) OVER () FROM t ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT upper(s), id, sum(id) OVER () FROM t ORDER BY id").await, ["A|1|6", "B|2|6", "NULL|3|6"] as [&str; 3], "SELECT upper(s), id, sum(id) OVER () FROM t ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT sum(v) OVER (), 'a', id FROM t ORDER BY id").await, ["30|a|1", "30|a|2", "30|a|3"] as [&str; 3], "SELECT sum(v) OVER (), 'a', id FROM t ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT 'a', max(n) OVER (), 'b', min(s) OVER (), id FROM t ORDER BY id").await, ["a|2.5|b|a|1", "a|2.5|b|a|2", "a|2.5|b|a|3"] as [&str; 3], "SELECT 'a', max(n) OVER (), 'b', min(s) OVER (), id FROM t ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, 'c', lag(v) OVER (ORDER BY id), v + 1 FROM t ORDER BY id").await, ["1|c|NULL|11", "2|c|10|21", "3|c|20|NULL"] as [&str; 3], "SELECT id, 'c', lag(v) OVER (ORDER BY id), v + 1 FROM t ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT 'q', row_number() OVER (ORDER BY v DESC NULLS LAST), id FROM t ORDER BY id").await, ["q|2|1", "q|1|2", "q|3|3"] as [&str; 3], "SELECT 'q', row_number() OVER (ORDER BY v DESC NULLS LAST), id FROM t ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, sum(v) OVER (ORDER BY id), 7 FROM t WHERE id > 1 ORDER BY id").await, ["2|20|7", "3|20|7"] as [&str; 2], "SELECT id, sum(v) OVER (ORDER BY id), 7 FROM t WHERE id > 1 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT 'x', id FROM t ORDER BY id").await, ["x|1", "x|2", "x|3"] as [&str; 3], "SELECT 'x', id FROM t ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT DISTINCT 'a', sum(id) OVER () FROM t").await, ["a|6"] as [&str; 1], "SELECT DISTINCT 'a', sum(id) OVER () FROM t");
    assert_eq!(rows_n(&c, "SELECT DISTINCT sum(id) OVER () FROM t").await, ["6"] as [&str; 1], "SELECT DISTINCT sum(id) OVER () FROM t");
    assert_eq!(rows_n(&c, "SELECT id + 0, count(*) OVER (), NULL, s FROM t ORDER BY id").await, ["1|3|NULL|a", "2|3|NULL|b", "3|3|NULL|NULL"] as [&str; 3], "SELECT id + 0, count(*) OVER (), NULL, s FROM t ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT 'x', id, v * 2 FROM t ORDER BY id").await, ["x|1|20", "x|2|40", "x|3|NULL"] as [&str; 3], "SELECT 'x', id, v * 2 FROM t ORDER BY id");
}

/// DATE, TIME, TIMETZ, INTERVAL, UUID, INET, TIMESTAMP, MACADDR, BIT and VARBIT order by value in GROUP BY, DISTINCT, windows, UNION, ORDER BY and min/max, not by their stored bytes.
#[tokio::test(flavor = "multi_thread")]
async fn wire_format_types_order_as_values() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE od (id INT PRIMARY KEY, d DATE, tm TIME, tz TIMETZ, iv INTERVAL, u UUID, ip INET, ts TIMESTAMP, mc MACADDR, b BIT(4), vb VARBIT(8))").await;
    run(&c, "INSERT INTO od VALUES (1, '1999-01-01', '10:00', '10:00+00', '1 day', '00000000-0000-0000-0000-000000000001', '10.0.0.1', '1999-01-01', '08:00:2b:01:02:03', B'0101', B'1'), (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), (3, '2024-01-01', '23:59', '23:59+00', '-2 days', 'ffffffff-0000-0000-0000-000000000001', '192.168.1.1', '2024-01-01', 'ff:00:2b:01:02:03', B'1111', B'10'), (4, '1960-01-01', '00:01', '00:01+00', '3 hours', '00000000-0000-0000-0000-000000000000', '1.2.3.4', '1960-01-01', '00:00:2b:01:02:03', B'0001', B'0001'), (5, '1960-01-01', '00:01', '00:01+00', '3 hours', '00000000-0000-0000-0000-000000000000', '1.2.3.4', '1960-01-01', '00:00:2b:01:02:03', B'0001', B'0001')").await;
    assert_eq!(rows_n(&c, "SELECT d, count(*) FROM od GROUP BY d ORDER BY d").await, ["1960-01-01|2", "1999-01-01|1", "2024-01-01|1", "NULL|1"] as [&str; 4], "SELECT d, count(*) FROM od GROUP BY d ORDER BY d");
    assert_eq!(rows_n(&c, "SELECT d, count(*) FROM od GROUP BY d ORDER BY d DESC").await, ["NULL|1", "2024-01-01|1", "1999-01-01|1", "1960-01-01|2"] as [&str; 4], "SELECT d, count(*) FROM od GROUP BY d ORDER BY d DESC");
    assert_eq!(rows_n(&c, "SELECT tm, count(*) FROM od GROUP BY tm ORDER BY tm").await, ["00:01:00|2", "10:00:00|1", "23:59:00|1", "NULL|1"] as [&str; 4], "SELECT tm, count(*) FROM od GROUP BY tm ORDER BY tm");
    assert_eq!(rows_n(&c, "SELECT tz, count(*) FROM od GROUP BY tz ORDER BY tz").await, ["00:01:00+00|2", "10:00:00+00|1", "23:59:00+00|1", "NULL|1"] as [&str; 4], "SELECT tz, count(*) FROM od GROUP BY tz ORDER BY tz");
    assert_eq!(rows_n(&c, "SELECT iv, count(*) FROM od GROUP BY iv ORDER BY iv").await, ["-2 days|1", "03:00:00|2", "1 day|1", "NULL|1"] as [&str; 4], "SELECT iv, count(*) FROM od GROUP BY iv ORDER BY iv");
    assert_eq!(rows_n(&c, "SELECT u, count(*) FROM od GROUP BY u ORDER BY u").await, ["00000000-0000-0000-0000-000000000000|2", "00000000-0000-0000-0000-000000000001|1", "ffffffff-0000-0000-0000-000000000001|1", "NULL|1"] as [&str; 4], "SELECT u, count(*) FROM od GROUP BY u ORDER BY u");
    assert_eq!(rows_n(&c, "SELECT ip, count(*) FROM od GROUP BY ip ORDER BY ip").await, ["1.2.3.4|2", "10.0.0.1|1", "192.168.1.1|1", "NULL|1"] as [&str; 4], "SELECT ip, count(*) FROM od GROUP BY ip ORDER BY ip");
    assert_eq!(rows_n(&c, "SELECT ts, count(*) FROM od GROUP BY ts ORDER BY ts").await, ["1960-01-01 00:00:00|2", "1999-01-01 00:00:00|1", "2024-01-01 00:00:00|1", "NULL|1"] as [&str; 4], "SELECT ts, count(*) FROM od GROUP BY ts ORDER BY ts");
    assert_eq!(rows_n(&c, "SELECT mc, count(*) FROM od GROUP BY mc ORDER BY mc").await, ["00:00:2b:01:02:03|2", "08:00:2b:01:02:03|1", "ff:00:2b:01:02:03|1", "NULL|1"] as [&str; 4], "SELECT mc, count(*) FROM od GROUP BY mc ORDER BY mc");
    assert_eq!(rows_n(&c, "SELECT b, count(*) FROM od GROUP BY b ORDER BY b").await, ["0001|2", "0101|1", "1111|1", "NULL|1"] as [&str; 4], "SELECT b, count(*) FROM od GROUP BY b ORDER BY b");
    assert_eq!(rows_n(&c, "SELECT vb, count(*) FROM od GROUP BY vb ORDER BY vb").await, ["0001|2", "1|1", "10|1", "NULL|1"] as [&str; 4], "SELECT vb, count(*) FROM od GROUP BY vb ORDER BY vb");
    assert_eq!(rows_n(&c, "SELECT DISTINCT d FROM od ORDER BY d").await, ["1960-01-01", "1999-01-01", "2024-01-01", "NULL"] as [&str; 4], "SELECT DISTINCT d FROM od ORDER BY d");
    assert_eq!(rows_n(&c, "SELECT DISTINCT tm FROM od ORDER BY tm").await, ["00:01:00", "10:00:00", "23:59:00", "NULL"] as [&str; 4], "SELECT DISTINCT tm FROM od ORDER BY tm");
    assert_eq!(rows_n(&c, "SELECT DISTINCT iv FROM od ORDER BY iv").await, ["-2 days", "03:00:00", "1 day", "NULL"] as [&str; 4], "SELECT DISTINCT iv FROM od ORDER BY iv");
    assert_eq!(rows_n(&c, "SELECT id, rank() OVER (ORDER BY d) FROM od ORDER BY id").await, ["1|3", "2|5", "3|4", "4|1", "5|1"] as [&str; 5], "SELECT id, rank() OVER (ORDER BY d) FROM od ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, rank() OVER (ORDER BY tm DESC) FROM od ORDER BY id").await, ["1|3", "2|1", "3|2", "4|4", "5|4"] as [&str; 5], "SELECT id, rank() OVER (ORDER BY tm DESC) FROM od ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, rank() OVER (ORDER BY iv) FROM od ORDER BY id").await, ["1|4", "2|5", "3|1", "4|2", "5|2"] as [&str; 5], "SELECT id, rank() OVER (ORDER BY iv) FROM od ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, rank() OVER (ORDER BY u) FROM od ORDER BY id").await, ["1|3", "2|5", "3|4", "4|1", "5|1"] as [&str; 5], "SELECT id, rank() OVER (ORDER BY u) FROM od ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT d FROM od UNION SELECT d FROM od ORDER BY 1").await, ["1960-01-01", "1999-01-01", "2024-01-01", "NULL"] as [&str; 4], "SELECT d FROM od UNION SELECT d FROM od ORDER BY 1");
    assert_eq!(rows_n(&c, "SELECT iv FROM od UNION SELECT iv FROM od ORDER BY 1").await, ["-2 days", "03:00:00", "1 day", "NULL"] as [&str; 4], "SELECT iv FROM od UNION SELECT iv FROM od ORDER BY 1");
    assert_eq!(rows_n(&c, "SELECT min(d), max(d), min(tm), max(tm), min(iv), max(iv) FROM od").await, ["1960-01-01|2024-01-01|00:01:00|23:59:00|-2 days|1 day"] as [&str; 1], "SELECT min(d), max(d), min(tm), max(tm), min(iv), max(iv) FROM od");
    assert_eq!(rows_n(&c, "SELECT id FROM od ORDER BY d, id").await, ["4", "5", "1", "3", "2"] as [&str; 5], "SELECT id FROM od ORDER BY d, id");
    assert_eq!(rows_n(&c, "SELECT id FROM od ORDER BY iv, id").await, ["3", "4", "5", "1", "2"] as [&str; 5], "SELECT id FROM od ORDER BY iv, id");
    assert_eq!(rows_n(&c, "SELECT id FROM od ORDER BY tm DESC, id").await, ["2", "3", "1", "4", "5"] as [&str; 5], "SELECT id FROM od ORDER BY tm DESC, id");
    assert_eq!(rows_n(&c, "SELECT d, count(*) FROM od GROUP BY d ORDER BY d LIMIT 2").await, ["1960-01-01|2", "1999-01-01|1"] as [&str; 2], "SELECT d, count(*) FROM od GROUP BY d ORDER BY d LIMIT 2");
    assert_eq!(rows_n(&c, "SELECT id FROM od ORDER BY d LIMIT 2").await, ["4", "5"] as [&str; 2], "SELECT id FROM od ORDER BY d LIMIT 2");
}

