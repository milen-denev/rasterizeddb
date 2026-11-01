//! W1: NULL in window keys. A NULL's cell holds its type's zeroes (the NULL
//! lives in the column's companion), so a key built from the bytes alone put
//! NULL in one partition with 0, 0.00, '' or false, and sorted it among them.
//! Every expectation is PostgreSQL 17's (`docker run --rm postgres:17`), read
//! as text with NULL shown as `NULL`.

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

/// Each row as `a|b|…`, a NULL as `NULL` (so '' and NULL differ).
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

/// A NULL partitions apart from 0, 0.00, '', false, the epoch and {} for every type (INT, BIGINT, NUMERIC, NUMERIC(p,s), TEXT, VARCHAR, CHAR, BOOLEAN, timestamps, DATE, FLOAT8, JSONB, UUID), sorts last ascending and first descending in a window's ORDER BY (peers, ranks, RANGE frames, LAG/LEAD) unless NULLS FIRST/LAST says otherwise, a text '' is a value to COUNT and MIN, and a NULL window value of text type is NULL, not ''. PostgreSQL 17's values; NULL shown as NULL.
#[tokio::test(flavor = "multi_thread")]
async fn window_null_is_its_own_partition_and_sorts_last() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE wt (id INT PRIMARY KEY, i INT, b BIGINT, n NUMERIC, p NUMERIC(10,2), t TEXT, v VARCHAR(10), c CHAR(2), bo BOOLEAN, ts TIMESTAMP, tz TIMESTAMPTZ, d DATE, f FLOAT8, j JSONB, u UUID)").await;
    run(&c, "INSERT INTO wt VALUES (1, 0, 0, 0, 0, '', '', '  ', false, '2000-01-01 00:00:00', '2000-01-01 00:00:00+00', '2000-01-01', 0, '{}', '00000000-0000-0000-0000-000000000000')").await;
    run(&c, "INSERT INTO wt VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)").await;
    run(&c, "INSERT INTO wt VALUES (3, 0, 0, 0.00, 0.00, '', '', '  ', false, '2000-01-01 00:00:00', '2000-01-01 00:00:00+00', '2000-01-01', 0, '{}', '00000000-0000-0000-0000-000000000000')").await;
    run(&c, "INSERT INTO wt VALUES (4, 1, 1, 1, 1, 'a', 'a', 'a', true, '2001-01-01 00:00:00', '2001-01-01 00:00:00+00', '2001-01-01', 1, '{\"a\":1}', '00000000-0000-0000-0000-000000000001')").await;
    run(&c, "INSERT INTO wt VALUES (5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)").await;
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY i), row_number() OVER (PARTITION BY i ORDER BY id) FROM wt ORDER BY id").await, ["1|2|1", "2|2|1", "3|2|2", "4|1|1", "5|2|2"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY i), row_number() OVER (PARTITION BY i ORDER BY id) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY b), sum(b) OVER (PARTITION BY b) FROM wt ORDER BY id").await, ["1|2|0", "2|2|NULL", "3|2|0", "4|1|1", "5|2|NULL"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY b), sum(b) OVER (PARTITION BY b) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY n), sum(n) OVER (PARTITION BY n) FROM wt ORDER BY id").await, ["1|2|0.00", "2|2|NULL", "3|2|0.00", "4|1|1", "5|2|NULL"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY n), sum(n) OVER (PARTITION BY n) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY p), sum(p) OVER (PARTITION BY p) FROM wt ORDER BY id").await, ["1|2|0.00", "2|2|NULL", "3|2|0.00", "4|1|1.00", "5|2|NULL"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY p), sum(p) OVER (PARTITION BY p) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY t), count(t) OVER (PARTITION BY t) FROM wt ORDER BY id").await, ["1|2|2", "2|2|0", "3|2|2", "4|1|1", "5|2|0"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY t), count(t) OVER (PARTITION BY t) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY v), count(v) OVER () FROM wt ORDER BY id").await, ["1|2|3", "2|2|3", "3|2|3", "4|1|3", "5|2|3"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY v), count(v) OVER () FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY c) FROM wt ORDER BY id").await, ["1|2", "2|2", "3|2", "4|1", "5|2"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY c) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY bo) FROM wt ORDER BY id").await, ["1|2", "2|2", "3|2", "4|1", "5|2"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY bo) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY ts) FROM wt ORDER BY id").await, ["1|2", "2|2", "3|2", "4|1", "5|2"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY ts) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY tz) FROM wt ORDER BY id").await, ["1|2", "2|2", "3|2", "4|1", "5|2"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY tz) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY d) FROM wt ORDER BY id").await, ["1|2", "2|2", "3|2", "4|1", "5|2"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY d) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY f) FROM wt ORDER BY id").await, ["1|2", "2|2", "3|2", "4|1", "5|2"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY f) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY j) FROM wt ORDER BY id").await, ["1|2", "2|2", "3|2", "4|1", "5|2"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY j) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY u) FROM wt ORDER BY id").await, ["1|2", "2|2", "3|2", "4|1", "5|2"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY u) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY i, t) FROM wt ORDER BY id").await, ["1|2", "2|2", "3|2", "4|1", "5|2"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY i, t) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, row_number() OVER (ORDER BY i, id) FROM wt ORDER BY id").await, ["1|1", "2|4", "3|2", "4|3", "5|5"] as [&str; 5], "SELECT id, row_number() OVER (ORDER BY i, id) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, row_number() OVER (ORDER BY i DESC, id) FROM wt ORDER BY id").await, ["1|4", "2|1", "3|5", "4|3", "5|2"] as [&str; 5], "SELECT id, row_number() OVER (ORDER BY i DESC, id) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, row_number() OVER (ORDER BY t, id) FROM wt ORDER BY id").await, ["1|1", "2|4", "3|2", "4|3", "5|5"] as [&str; 5], "SELECT id, row_number() OVER (ORDER BY t, id) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, row_number() OVER (ORDER BY n DESC, id) FROM wt ORDER BY id").await, ["1|4", "2|1", "3|5", "4|3", "5|2"] as [&str; 5], "SELECT id, row_number() OVER (ORDER BY n DESC, id) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, row_number() OVER (ORDER BY ts, id) FROM wt ORDER BY id").await, ["1|1", "2|4", "3|2", "4|3", "5|5"] as [&str; 5], "SELECT id, row_number() OVER (ORDER BY ts, id) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, rank() OVER (ORDER BY i) FROM wt ORDER BY id").await, ["1|1", "2|4", "3|1", "4|3", "5|4"] as [&str; 5], "SELECT id, rank() OVER (ORDER BY i) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, rank() OVER (ORDER BY p DESC) FROM wt ORDER BY id").await, ["1|4", "2|1", "3|4", "4|3", "5|1"] as [&str; 5], "SELECT id, rank() OVER (ORDER BY p DESC) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, dense_rank() OVER (ORDER BY t) FROM wt ORDER BY id").await, ["1|1", "2|3", "3|1", "4|2", "5|3"] as [&str; 5], "SELECT id, dense_rank() OVER (ORDER BY t) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, sum(i) OVER (ORDER BY i) FROM wt ORDER BY id").await, ["1|0", "2|1", "3|0", "4|1", "5|1"] as [&str; 5], "SELECT id, sum(i) OVER (ORDER BY i) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (ORDER BY i RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM wt ORDER BY id").await, ["1|2", "2|5", "3|2", "4|3", "5|5"] as [&str; 5], "SELECT id, count(*) OVER (ORDER BY i RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, lag(id) OVER (ORDER BY i, id), lead(id) OVER (ORDER BY i, id) FROM wt ORDER BY id").await, ["1|NULL|3", "2|4|5", "3|1|4", "4|3|2", "5|2|NULL"] as [&str; 5], "SELECT id, lag(id) OVER (ORDER BY i, id), lead(id) OVER (ORDER BY i, id) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, row_number() OVER (ORDER BY i NULLS FIRST, id) FROM wt ORDER BY id").await, ["1|3", "2|1", "3|4", "4|5", "5|2"] as [&str; 5], "SELECT id, row_number() OVER (ORDER BY i NULLS FIRST, id) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, row_number() OVER (ORDER BY i DESC NULLS LAST, id) FROM wt ORDER BY id").await, ["1|2", "2|4", "3|3", "4|1", "5|5"] as [&str; 5], "SELECT id, row_number() OVER (ORDER BY i DESC NULLS LAST, id) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(t) OVER (), min(t) OVER (), max(t) OVER () FROM wt ORDER BY id").await, ["1|3||a", "2|3||a", "3|3||a", "4|3||a", "5|3||a"] as [&str; 5], "SELECT id, count(t) OVER (), min(t) OVER (), max(t) OVER () FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, first_value(t) OVER (ORDER BY id), last_value(i) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM wt ORDER BY id").await, ["1||NULL", "2||NULL", "3||NULL", "4||NULL", "5||NULL"] as [&str; 5], "SELECT id, first_value(t) OVER (ORDER BY id), last_value(i) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM wt ORDER BY i, id").await, ["1", "3", "4", "2", "5"] as [&str; 5], "SELECT id FROM wt ORDER BY i, id");
    assert_eq!(rows_n(&c, "SELECT id FROM wt ORDER BY i DESC, id").await, ["2", "5", "4", "1", "3"] as [&str; 5], "SELECT id FROM wt ORDER BY i DESC, id");
    assert_eq!(rows_n(&c, "SELECT id FROM wt ORDER BY t, id").await, ["1", "3", "4", "2", "5"] as [&str; 5], "SELECT id FROM wt ORDER BY t, id");
    assert_eq!(rows_n(&c, "SELECT id FROM wt ORDER BY t DESC, id").await, ["2", "5", "4", "1", "3"] as [&str; 5], "SELECT id FROM wt ORDER BY t DESC, id");
    assert_eq!(rows_n(&c, "SELECT id FROM wt ORDER BY i NULLS FIRST, id").await, ["2", "5", "1", "3", "4"] as [&str; 5], "SELECT id FROM wt ORDER BY i NULLS FIRST, id");
    assert_eq!(rows_n(&c, "SELECT id FROM wt ORDER BY n DESC NULLS LAST, id").await, ["4", "1", "3", "2", "5"] as [&str; 5], "SELECT id FROM wt ORDER BY n DESC NULLS LAST, id");
    assert_eq!(rows_n(&c, "SELECT id FROM wt ORDER BY bo, id").await, ["1", "3", "4", "2", "5"] as [&str; 5], "SELECT id FROM wt ORDER BY bo, id");
    assert_eq!(rows_n(&c, "SELECT id FROM wt ORDER BY ts DESC, id").await, ["2", "5", "4", "1", "3"] as [&str; 5], "SELECT id FROM wt ORDER BY ts DESC, id");
    assert_eq!(rows_n(&c, "SELECT id, lag(t) OVER (ORDER BY id), lead(t) OVER (ORDER BY id), first_value(t) OVER (ORDER BY id DESC), max(t) OVER (PARTITION BY i) FROM wt ORDER BY id").await, ["1|NULL|NULL|NULL|", "2|||NULL|NULL", "3|NULL|a|NULL|", "4||NULL|NULL|a", "5|a|NULL|NULL|NULL"] as [&str; 5], "SELECT id, lag(t) OVER (ORDER BY id), lead(t) OVER (ORDER BY id), first_value(t) OVER (ORDER BY id DESC), max(t) OVER (PARTITION BY i) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, lag(n) OVER (ORDER BY id), min(p) OVER (PARTITION BY t) FROM wt ORDER BY id").await, ["1|NULL|0.00", "2|0|NULL", "3|NULL|0.00", "4|0.00|1.00", "5|1|NULL"] as [&str; 5], "SELECT id, lag(n) OVER (ORDER BY id), min(p) OVER (PARTITION BY t) FROM wt ORDER BY id");
}

/// The reviewer's probe: PARTITION BY over a nullable NUMERIC, NUMERIC(10,2) and INT, beside GROUP BY, DISTINCT, UNION, joins and IN over the same NULLs. PostgreSQL 17's values.
#[tokio::test(flavor = "multi_thread")]
async fn window_partitions_from_the_review_probe() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE nz (id INT PRIMARY KEY, n NUMERIC, p NUMERIC(10,2), i INT, t TEXT)").await;
    run(&c, "INSERT INTO nz VALUES (1, 0, 0, 0, ''), (2, NULL, NULL, NULL, NULL), (3, 0.00, 0.00, 0, ''), (4, 1, 1, 1, 'a')").await;
    assert_eq!(rows_n(&c, "SELECT id, sum(n) OVER (PARTITION BY n), row_number() OVER (PARTITION BY n ORDER BY id), count(*) OVER (PARTITION BY n) FROM nz ORDER BY id").await, ["1|0.00|1|2", "2|NULL|1|1", "3|0.00|2|2", "4|1|1|1"] as [&str; 4], "SELECT id, sum(n) OVER (PARTITION BY n), row_number() OVER (PARTITION BY n ORDER BY id), count(*) OVER (PARTITION BY n) FROM nz ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, sum(p) OVER (PARTITION BY p), row_number() OVER (PARTITION BY p ORDER BY id), count(*) OVER (PARTITION BY p) FROM nz ORDER BY id").await, ["1|0.00|1|2", "2|NULL|1|1", "3|0.00|2|2", "4|1.00|1|1"] as [&str; 4], "SELECT id, sum(p) OVER (PARTITION BY p), row_number() OVER (PARTITION BY p ORDER BY id), count(*) OVER (PARTITION BY p) FROM nz ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, sum(i) OVER (PARTITION BY i), row_number() OVER (PARTITION BY i ORDER BY id), count(*) OVER (PARTITION BY i) FROM nz ORDER BY id").await, ["1|0|1|2", "2|NULL|1|1", "3|0|2|2", "4|1|1|1"] as [&str; 4], "SELECT id, sum(i) OVER (PARTITION BY i), row_number() OVER (PARTITION BY i ORDER BY id), count(*) OVER (PARTITION BY i) FROM nz ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT count(DISTINCT n), count(DISTINCT p) FROM nz").await, ["2|2"] as [&str; 1], "SELECT count(DISTINCT n), count(DISTINCT p) FROM nz");
    assert_eq!(rows_n(&c, "SELECT x.id, y.id FROM nz x JOIN nz y ON x.n = y.n ORDER BY 1, 2").await, ["1|1", "1|3", "3|1", "3|3", "4|4"] as [&str; 5], "SELECT x.id, y.id FROM nz x JOIN nz y ON x.n = y.n ORDER BY 1, 2");
    assert_eq!(rows_n(&c, "SELECT id FROM nz WHERE n IN (0, 1) ORDER BY id").await, ["1", "3", "4"] as [&str; 3], "SELECT id FROM nz WHERE n IN (0, 1) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM nz WHERE n NOT IN (1) ORDER BY id").await, ["1", "3"] as [&str; 2], "SELECT id FROM nz WHERE n NOT IN (1) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM nz WHERE n IS NULL").await, ["2"] as [&str; 1], "SELECT id FROM nz WHERE n IS NULL");
    assert_eq!(rows_n(&c, "SELECT id FROM nz WHERE n = 0 ORDER BY id").await, ["1", "3"] as [&str; 2], "SELECT id FROM nz WHERE n = 0 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT min(n), max(n), sum(n), avg(n) FROM nz WHERE id = 2").await, ["NULL|NULL|NULL|NULL"] as [&str; 1], "SELECT min(n), max(n), sum(n), avg(n) FROM nz WHERE id = 2");
    run(&c, "CREATE TABLE nu (n NUMERIC UNIQUE)").await;
    run(&c, "INSERT INTO nu VALUES (NULL), (NULL), (0)").await;
    assert_eq!(sqlstate(&c, "INSERT INTO nu VALUES (0.0)").await, "23505");
    assert_eq!(rows_n(&c, "SELECT count(*) FROM nu").await, ["3"] as [&str; 1], "SELECT count(*) FROM nu");
}

/// DISTINCT, count(DISTINCT), count(col), UNION, joins, IN subqueries, GROUP BY and ORDER BY over NULL beside 0, '', false and {}, and a window over a CTE and a derived table: NULL is never a zero value. PostgreSQL 17's values.
#[tokio::test(flavor = "multi_thread")]
async fn null_keys_outside_windows() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE wt (id INT PRIMARY KEY, i INT, n NUMERIC, p NUMERIC(10,2), t TEXT, bo BOOLEAN, f FLOAT8, j JSONB)").await;
    run(&c, "INSERT INTO wt VALUES (1, 0, 0, 0, '', false, 0, '{}'), (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL), (3, 0, 0.00, 0.00, '', false, 0, '{}'), (4, 1, 1, 1, 'a', true, 1, '{\"a\":1}'), (5, NULL, NULL, NULL, NULL, NULL, NULL, NULL)").await;
    assert_eq!(rows_n(&c, "SELECT DISTINCT i, t FROM wt ORDER BY 1, 2").await, ["0|", "1|a", "NULL|NULL"] as [&str; 3], "SELECT DISTINCT i, t FROM wt ORDER BY 1, 2");
    assert_eq!(rows_n(&c, "SELECT DISTINCT bo FROM wt ORDER BY 1").await, ["f", "t", "NULL"] as [&str; 3], "SELECT DISTINCT bo FROM wt ORDER BY 1");
    assert_eq!(rows_n(&c, "SELECT DISTINCT p FROM wt ORDER BY 1").await, ["0.00", "1.00", "NULL"] as [&str; 3], "SELECT DISTINCT p FROM wt ORDER BY 1");
    assert_eq!(rows_n(&c, "SELECT count(DISTINCT t), count(DISTINCT i), count(DISTINCT bo), count(DISTINCT p), count(DISTINCT f) FROM wt").await, ["2|2|2|2|2"] as [&str; 1], "SELECT count(DISTINCT t), count(DISTINCT i), count(DISTINCT bo), count(DISTINCT p), count(DISTINCT f) FROM wt");
    assert_eq!(rows_n(&c, "SELECT count(t), count(i), count(bo), count(p), count(j) FROM wt").await, ["3|3|3|3|3"] as [&str; 1], "SELECT count(t), count(i), count(bo), count(p), count(j) FROM wt");
    assert_eq!(rows_n(&c, "SELECT i FROM wt UNION SELECT i FROM wt ORDER BY 1").await, ["0", "1", "NULL"] as [&str; 3], "SELECT i FROM wt UNION SELECT i FROM wt ORDER BY 1");
    assert_eq!(rows_n(&c, "SELECT t FROM wt UNION SELECT t FROM wt ORDER BY 1").await, ["", "a", "NULL"] as [&str; 3], "SELECT t FROM wt UNION SELECT t FROM wt ORDER BY 1");
    assert_eq!(rows_n(&c, "SELECT i, t FROM wt UNION ALL SELECT i, t FROM wt WHERE id = 2 ORDER BY 1, 2").await, ["0|", "0|", "1|a", "NULL|NULL", "NULL|NULL", "NULL|NULL"] as [&str; 6], "SELECT i, t FROM wt UNION ALL SELECT i, t FROM wt WHERE id = 2 ORDER BY 1, 2");
    assert_eq!(rows_n(&c, "SELECT a.id, b.id FROM wt a JOIN wt b ON a.t = b.t ORDER BY 1, 2").await, ["1|1", "1|3", "3|1", "3|3", "4|4"] as [&str; 5], "SELECT a.id, b.id FROM wt a JOIN wt b ON a.t = b.t ORDER BY 1, 2");
    assert_eq!(rows_n(&c, "SELECT a.id, b.id FROM wt a JOIN wt b ON a.i = b.i ORDER BY 1, 2").await, ["1|1", "1|3", "3|1", "3|3", "4|4"] as [&str; 5], "SELECT a.id, b.id FROM wt a JOIN wt b ON a.i = b.i ORDER BY 1, 2");
    assert_eq!(rows_n(&c, "SELECT a.id, b.id FROM wt a JOIN wt b ON a.bo = b.bo ORDER BY 1, 2").await, ["1|1", "1|3", "3|1", "3|3", "4|4"] as [&str; 5], "SELECT a.id, b.id FROM wt a JOIN wt b ON a.bo = b.bo ORDER BY 1, 2");
    assert_eq!(rows_n(&c, "SELECT a.id, b.id FROM wt a JOIN wt b ON a.i = b.i AND a.t = b.t ORDER BY 1, 2").await, ["1|1", "1|3", "3|1", "3|3", "4|4"] as [&str; 5], "SELECT a.id, b.id FROM wt a JOIN wt b ON a.i = b.i AND a.t = b.t ORDER BY 1, 2");
    assert_eq!(rows_n(&c, "SELECT id FROM wt WHERE t IN (SELECT t FROM wt WHERE id = 2) ORDER BY id").await, [] as [&str; 0], "SELECT id FROM wt WHERE t IN (SELECT t FROM wt WHERE id = 2) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM wt WHERE i NOT IN (SELECT i FROM wt WHERE id = 4) ORDER BY id").await, ["1", "3"] as [&str; 2], "SELECT id FROM wt WHERE i NOT IN (SELECT i FROM wt WHERE id = 4) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM wt WHERE t IN (SELECT t FROM wt WHERE id = 1) ORDER BY id").await, ["1", "3"] as [&str; 2], "SELECT id FROM wt WHERE t IN (SELECT t FROM wt WHERE id = 1) ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT i, t, count(*) FROM wt GROUP BY i, t ORDER BY 1, 2").await, ["0||2", "1|a|1", "NULL|NULL|2"] as [&str; 3], "SELECT i, t, count(*) FROM wt GROUP BY i, t ORDER BY 1, 2");
    assert_eq!(rows_n(&c, "SELECT bo, count(*) FROM wt GROUP BY bo ORDER BY 1").await, ["f|2", "t|1", "NULL|2"] as [&str; 3], "SELECT bo, count(*) FROM wt GROUP BY bo ORDER BY 1");
    assert_eq!(rows_n(&c, "SELECT j, count(*) FROM wt GROUP BY j ORDER BY 2, 1").await, ["{\"a\": 1}|1", "{}|2", "NULL|2"] as [&str; 3], "SELECT j, count(*) FROM wt GROUP BY j ORDER BY 2, 1");
    assert_eq!(rows_n(&c, "WITH c AS (SELECT id, i, t FROM wt) SELECT id, count(*) OVER (PARTITION BY i), count(*) OVER (PARTITION BY t), row_number() OVER (ORDER BY i, id) FROM c ORDER BY id").await, ["1|2|2|1", "2|2|2|4", "3|2|2|2", "4|1|1|3", "5|2|2|5"] as [&str; 5], "WITH c AS (SELECT id, i, t FROM wt) SELECT id, count(*) OVER (PARTITION BY i), count(*) OVER (PARTITION BY t), row_number() OVER (ORDER BY i, id) FROM c ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY i) FROM wt WHERE id < 5 ORDER BY id").await, ["1|2", "2|1", "3|2", "4|1"] as [&str; 4], "SELECT id, count(*) OVER (PARTITION BY i) FROM wt WHERE id < 5 ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY bo), count(*) OVER (PARTITION BY j) FROM wt ORDER BY id").await, ["1|2|2", "2|2|2", "3|2|2", "4|1|1", "5|2|2"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY bo), count(*) OVER (PARTITION BY j) FROM wt ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT id FROM wt ORDER BY p DESC, id").await, ["2", "5", "4", "1", "3"] as [&str; 5], "SELECT id FROM wt ORDER BY p DESC, id");
    assert_eq!(rows_n(&c, "SELECT id FROM wt ORDER BY j, id").await, ["1", "3", "4", "2", "5"] as [&str; 5], "SELECT id FROM wt ORDER BY j, id");
    assert_eq!(rows_n(&c, "WITH c AS (SELECT id, i FROM wt) SELECT id FROM c ORDER BY i, id").await, ["1", "3", "4", "2", "5"] as [&str; 5], "WITH c AS (SELECT id, i FROM wt) SELECT id FROM c ORDER BY i, id");
    assert_eq!(rows_n(&c, "SELECT id, i FROM wt ORDER BY i NULLS FIRST, id LIMIT 3").await, ["2|NULL", "5|NULL", "1|0"] as [&str; 3], "SELECT id, i FROM wt ORDER BY i NULLS FIRST, id LIMIT 3");
    assert_eq!(rows_n(&c, "SELECT id, count(*) OVER (PARTITION BY i) FROM (SELECT id, i FROM wt) d ORDER BY id").await, ["1|2", "2|2", "3|2", "4|1", "5|2"] as [&str; 5], "SELECT id, count(*) OVER (PARTITION BY i) FROM (SELECT id, i FROM wt) d ORDER BY id");
    assert_eq!(rows_n(&c, "SELECT a.id, b.id FROM wt a LEFT JOIN wt b ON a.p = b.p ORDER BY 1, 2").await, ["1|1", "1|3", "2|NULL", "3|1", "3|3", "4|4", "5|NULL"] as [&str; 7], "SELECT a.id, b.id FROM wt a LEFT JOIN wt b ON a.p = b.p ORDER BY 1, 2");
}
