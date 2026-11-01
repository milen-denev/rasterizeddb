//! NUMERIC precision as PostgreSQL keeps it: a generated column computed from
//! the value its inputs store, and (C6) an unconstrained NUMERIC holding each
//! value at its own scale. Every expectation is PostgreSQL 17's (`docker run
//! --rm postgres:17`), read as text.

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

async fn rows(c: &Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).unwrap_or("").to_string()).collect::<Vec<_>>().join("|"))
            }
            _ => None,
        })
        .collect()
}

async fn sqlstate(c: &Client, sql: &str) -> String {
    let e = c.simple_query(sql).await.expect_err(sql);
    e.code().map(|s| s.code().to_string()).unwrap_or_else(|| e.to_string())
}

/// A generated column recomputed by an UPDATE or an ON CONFLICT DO UPDATE
/// reads its inputs as stored: `SET n2 = 8.345` on a NUMERIC(5,2) is 8.35 to
/// `n2 * 1` (8.4 at one place, not 8.3) and to `n2::text` ('8.35', not
/// '8.345'), as an INSERT already had it.
#[tokio::test(flavor = "multi_thread")]
async fn a_generated_column_reads_the_value_as_stored() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(
        &c,
        "CREATE TABLE g (id INT PRIMARY KEY, n2 NUMERIC(5,2), g1 NUMERIC(10,1) GENERATED ALWAYS AS (n2 * 1) STORED, \
         gt TEXT GENERATED ALWAYS AS (n2::text) STORED)",
    )
    .await;
    for sql in [
        "INSERT INTO g (id, n2) VALUES (1, 8.345)",
        "INSERT INTO g (id, n2) VALUES (2, '8.345'::real)",
        "INSERT INTO g (id, n2) VALUES (3, '8.345'::float8)",
        "INSERT INTO g (id, n2) VALUES (4, 0), (5, 0), (6, 0), (7, 0)",
        "UPDATE g SET n2 = 8.345 WHERE id = 4",
        "UPDATE g SET n2 = '8.345'::float8 WHERE id = 5",
        "INSERT INTO g (id, n2) VALUES (6, 0) ON CONFLICT (id) DO UPDATE SET n2 = 8.345",
        "INSERT INTO g (id, n2) VALUES (7, 0) ON CONFLICT (id) DO UPDATE SET n2 = '8.345'::float8",
        "INSERT INTO g (id, n2) VALUES (8, 8.345) ON CONFLICT (id) DO NOTHING",
    ] {
        run(&c, sql).await;
    }
    let want: Vec<String> = (1..=8).map(|id| format!("{id}|8.35|8.4|8.35")).collect();
    assert_eq!(rows(&c, "SELECT * FROM g ORDER BY id").await, want);

    // An assigned expression of the row is rounded as its column rounds it.
    run(
        &c,
        "CREATE TABLE g2 (id INT PRIMARY KEY, i INT, p NUMERIC(6,2), gi BIGINT GENERATED ALWAYS AS (i * 10) STORED, \
         gp NUMERIC(12,3) GENERATED ALWAYS AS (p * 3) STORED, gt TEXT GENERATED ALWAYS AS (p::text || '/' || i::text) STORED)",
    )
    .await;
    run(&c, "INSERT INTO g2 (id, i, p) VALUES (1, 1, 1), (2, 2, 2)").await;
    run(&c, "UPDATE g2 SET i = 3, p = p * 1.005 WHERE id = 1").await;
    run(&c, "UPDATE g2 SET p = p / 3, i = i * 1.5 WHERE id = 2").await;
    run(&c, "INSERT INTO g2 (id, i, p) VALUES (2, 0, 0) ON CONFLICT (id) DO UPDATE SET p = g2.p + 0.004").await;
    assert_eq!(
        rows(&c, "SELECT * FROM g2 ORDER BY id").await,
        ["1|3|1.01|30|3.030|1.01/3", "2|3|0.67|30|2.010|0.67/3"]
    );
}

/// An unconstrained NUMERIC column stores each value at its own scale, every digit, and every reader reads it so: comparisons (a literal past nine places), ORDER BY, aggregates, arithmetic, a join. PostgreSQL 17's values.
#[tokio::test(flavor = "multi_thread")]
async fn an_unconstrained_numeric_keeps_each_value_at_its_own_scale() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE u (id INT PRIMARY KEY, v NUMERIC, w NUMERIC NOT NULL, k INT)").await;
    run(&c, "INSERT INTO u VALUES (1, 1.50, 2, 1), (2, 0.0000000001, 1.5, 1), (3, 123456789012345678901234567890.5, -7.25, 2), (4, NULL, 1.50, 2), (5, 1.5, 3e-20, 3)").await;
    assert_eq!(rows(&c, "SELECT * FROM u ORDER BY id").await, ["1|1.50|2|1", "2|0.0000000001|1.5|1", "3|123456789012345678901234567890.5|-7.25|2", "4||1.50|2", "5|1.5|0.00000000000000000003|3"] as [&str; 5], "SELECT * FROM u ORDER BY id");
    run(&c, "UPDATE u SET v = '1e-10' WHERE id = 1").await;
    run(&c, "UPDATE u SET w = w * 3 WHERE id = 2").await;
    run(&c, "UPDATE u SET v = 1.0 / 3 WHERE id = 5").await;
    assert_eq!(rows(&c, "SELECT id, v, w FROM u ORDER BY id").await, ["1|0.0000000001|2", "2|0.0000000001|4.5", "3|123456789012345678901234567890.5|-7.25", "4||1.50", "5|0.33333333333333333333|0.00000000000000000003"] as [&str; 5], "SELECT id, v, w FROM u ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM u WHERE v = 1.5 ORDER BY id").await, [] as [&str; 0], "SELECT id FROM u WHERE v = 1.5 ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM u WHERE w = 1.50 ORDER BY id").await, ["4"] as [&str; 1], "SELECT id FROM u WHERE w = 1.50 ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM u WHERE w > 1 ORDER BY id").await, ["1", "2", "4"] as [&str; 3], "SELECT id FROM u WHERE w > 1 ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM u WHERE w IN (1.5, 2, 4.5, 99, 100) ORDER BY id").await, ["1", "2", "4"] as [&str; 3], "SELECT id FROM u WHERE w IN (1.5, 2, 4.5, 99, 100) ORDER BY id");
    assert_eq!(rows(&c, "SELECT id, w FROM u ORDER BY w, id").await, ["3|-7.25", "5|0.00000000000000000003", "4|1.50", "1|2", "2|4.5"] as [&str; 5], "SELECT id, w FROM u ORDER BY w, id");
    assert_eq!(rows(&c, "SELECT sum(w), min(w), max(w), avg(w), count(v) FROM u").await, ["0.75000000000000000003|-7.25|4.5|0.15000000000000000001|4"] as [&str; 1], "SELECT sum(w), min(w), max(w), avg(w), count(v) FROM u");
    assert_eq!(rows(&c, "SELECT w, count(*) FROM u GROUP BY w ORDER BY w").await, ["-7.25|1", "0.00000000000000000003|1", "1.50|1", "2|1", "4.5|1"] as [&str; 5], "SELECT w, count(*) FROM u GROUP BY w ORDER BY w");
    assert_eq!(rows(&c, "SELECT DISTINCT w FROM u ORDER BY w").await, ["-7.25", "0.00000000000000000003", "1.50", "2", "4.5"] as [&str; 5], "SELECT DISTINCT w FROM u ORDER BY w");
    assert_eq!(rows(&c, "SELECT id, w + 1, w * 2, v::text FROM u ORDER BY id").await, ["1|3|4|0.0000000001", "2|5.5|9.0|0.0000000001", "3|-6.25|-14.50|123456789012345678901234567890.5", "4|2.50|3.00|", "5|1.00000000000000000003|0.00000000000000000006|0.33333333333333333333"] as [&str; 5], "SELECT id, w + 1, w * 2, v::text FROM u ORDER BY id");
    assert_eq!(rows(&c, "SELECT a.id, b.id FROM u a JOIN u b ON a.w = b.w ORDER BY 1, 2").await, ["1|1", "2|2", "3|3", "4|4", "5|5"] as [&str; 5], "SELECT a.id, b.id FROM u a JOIN u b ON a.w = b.w ORDER BY 1, 2");
}

/// 1.5, 1.50 and 1.500 are one value to GROUP BY, count(DISTINCT), a join, UNIQUE and MIN/MAX (the later one on a tie, as numeric_larger), each still shown as written. PostgreSQL 17's values.
#[tokio::test(flavor = "multi_thread")]
async fn equal_values_at_different_scales_are_one_value() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE a (id INT PRIMARY KEY, v NUMERIC, g INT)").await;
    run(&c, "INSERT INTO a VALUES (1, 1.5, 1), (2, 1.50, 1), (3, 1.500, 2), (4, 2, 2), (5, 2.0, 3), (6, 0.1, 3), (7, NULL, 3)").await;
    assert_eq!(rows(&c, "SELECT v, count(*) FROM a GROUP BY v ORDER BY v").await, ["0.1|1", "1.5|3", "2|2", "|1"] as [&str; 4], "SELECT v, count(*) FROM a GROUP BY v ORDER BY v");
    assert_eq!(rows(&c, "SELECT count(DISTINCT v) FROM a").await, ["3"] as [&str; 1], "SELECT count(DISTINCT v) FROM a");
    assert_eq!(rows(&c, "SELECT g, sum(v), max(v), min(v) FROM a GROUP BY g ORDER BY g").await, ["1|3.00|1.50|1.50", "2|3.500|2|1.500", "3|2.1|2.0|0.1"] as [&str; 3], "SELECT g, sum(v), max(v), min(v) FROM a GROUP BY g ORDER BY g");
    assert_eq!(rows(&c, "SELECT g FROM a GROUP BY g HAVING sum(v) > 3 ORDER BY g").await, ["2"] as [&str; 1], "SELECT g FROM a GROUP BY g HAVING sum(v) > 3 ORDER BY g");
    run(&c, "CREATE TABLE b (id INT PRIMARY KEY, w NUMERIC)").await;
    run(&c, "INSERT INTO b VALUES (10, 1.5000), (11, 2.00), (12, 0.10)").await;
    assert_eq!(rows(&c, "SELECT a.id, b.id FROM a JOIN b ON a.v = b.w ORDER BY 1, 2").await, ["1|10", "2|10", "3|10", "4|11", "5|11", "6|12"] as [&str; 6], "SELECT a.id, b.id FROM a JOIN b ON a.v = b.w ORDER BY 1, 2");
    assert_eq!(rows(&c, "SELECT a.id, b.id FROM a LEFT JOIN b ON a.v = b.w ORDER BY 1, 2").await, ["1|10", "2|10", "3|10", "4|11", "5|11", "6|12", "7|"] as [&str; 7], "SELECT a.id, b.id FROM a LEFT JOIN b ON a.v = b.w ORDER BY 1, 2");
    assert_eq!(rows(&c, "SELECT id FROM a ORDER BY v DESC NULLS LAST, id LIMIT 3").await, ["4", "5", "1"] as [&str; 3], "SELECT id FROM a ORDER BY v DESC NULLS LAST, id LIMIT 3");
    run(&c, "CREATE TABLE uq (id INT PRIMARY KEY, v NUMERIC UNIQUE)").await;
    run(&c, "INSERT INTO uq VALUES (1, 1.5)").await;
    assert_eq!(sqlstate(&c, "INSERT INTO uq VALUES (2, 1.50)").await, "23505");
    assert_eq!(rows(&c, "SELECT * FROM uq ORDER BY id").await, ["1|1.5"] as [&str; 1], "SELECT * FROM uq ORDER BY id");
    assert_eq!(rows(&c, "SELECT id, CASE WHEN v > 1.5 THEN v ELSE 0.000 END, coalesce(v, 9.99) FROM a ORDER BY id").await, ["1|0.000|1.5", "2|0.000|1.50", "3|0.000|1.500", "4|2|2", "5|2.0|2.0", "6|0.000|0.1", "7|0.000|9.99"] as [&str; 7], "SELECT id, CASE WHEN v > 1.5 THEN v ELSE 0.000 END, coalesce(v, 9.99) FROM a ORDER BY id");
}

/// A CTE, a derived table and UNION ALL keep each value's own scale, as do arithmetic, a quotient and values of 38 digits or 30 places. PostgreSQL 17's values.
#[tokio::test(flavor = "multi_thread")]
async fn a_cte_and_a_derived_table_keep_each_values_scale() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE m (id INT PRIMARY KEY, v NUMERIC, w NUMERIC(10,2))").await;
    run(&c, "INSERT INTO m VALUES (1, 1.5, 1.50), (2, 2.250, 2.25), (3, 0.000000000001, 0), (4, 100, 100)").await;
    assert_eq!(rows(&c, "SELECT id, v + 1, v * 2, v - 0.5, -v, abs(v), v / 3 FROM m ORDER BY id").await, ["1|2.5|3.0|1.0|-1.5|1.5|0.50000000000000000000", "2|3.250|4.500|1.750|-2.250|2.250|0.75000000000000000000", "3|1.000000000001|0.000000000002|-0.499999999999|-0.000000000001|0.000000000001|0.00000000000033333333333333333333", "4|101|200|99.5|-100|100|33.3333333333333333"] as [&str; 4], "SELECT id, v + 1, v * 2, v - 0.5, -v, abs(v), v / 3 FROM m ORDER BY id");
    assert_eq!(rows(&c, "SELECT min(v), max(v), sum(v), avg(v) FROM m").await, ["0.000000000001|100|103.750000000001|25.9375000000002500"] as [&str; 1], "SELECT min(v), max(v), sum(v), avg(v) FROM m");
    assert_eq!(rows(&c, "WITH c AS (SELECT id, v FROM m) SELECT id, v FROM c ORDER BY id").await, ["1|1.5", "2|2.250", "3|0.000000000001", "4|100"] as [&str; 4], "WITH c AS (SELECT id, v FROM m) SELECT id, v FROM c ORDER BY id");
    assert_eq!(rows(&c, "WITH c AS (SELECT id, v * 2 AS d FROM m) SELECT id, d FROM c ORDER BY id").await, ["1|3.0", "2|4.500", "3|0.000000000002", "4|200"] as [&str; 4], "WITH c AS (SELECT id, v * 2 AS d FROM m) SELECT id, d FROM c ORDER BY id");
    assert_eq!(rows(&c, "SELECT v FROM m WHERE id < 3 UNION ALL SELECT w FROM m WHERE id < 3 ORDER BY 1").await, ["1.5", "1.50", "2.250", "2.25"] as [&str; 4], "SELECT v FROM m WHERE id < 3 UNION ALL SELECT w FROM m WHERE id < 3 ORDER BY 1");
    assert_eq!(rows(&c, "SELECT id, v FROM (SELECT id, v FROM m WHERE v > 1) s ORDER BY id").await, ["1|1.5", "2|2.250", "4|100"] as [&str; 3], "SELECT id, v FROM (SELECT id, v FROM m WHERE v > 1) s ORDER BY id");
    assert_eq!(rows(&c, "SELECT 1.0 / 3, 2.0 / 3 FROM m WHERE id = 1").await, ["0.33333333333333333333|0.66666666666666666667"] as [&str; 1], "SELECT 1.0 / 3, 2.0 / 3 FROM m WHERE id = 1");
    run(&c, "INSERT INTO m VALUES (7, 1e-30, 1), (8, 12345678901234567890123456789.123456789, 1)").await;
    assert_eq!(rows(&c, "SELECT id, v FROM m WHERE id >= 7 ORDER BY id").await, ["7|0.000000000000000000000000000001", "8|12345678901234567890123456789.123456789"] as [&str; 2], "SELECT id, v FROM m WHERE id >= 7 ORDER BY id");
    assert_eq!(rows(&c, "SELECT id, v FROM m WHERE v = 0.000000000001 OR v = 1e-30 ORDER BY id").await, ["3|0.000000000001", "7|0.000000000000000000000000000001"] as [&str; 2], "SELECT id, v FROM m WHERE v = 0.000000000001 OR v = 1e-30 ORDER BY id");
    assert_eq!(rows(&c, "UPDATE m SET v = v * 10 WHERE id = 7 RETURNING id, v").await, ["7|0.000000000000000000000000000010"] as [&str; 1], "UPDATE m SET v = v * 10 WHERE id = 7 RETURNING id, v");
    assert_eq!(rows(&c, "SELECT avg(v) FROM m WHERE id IN (1, 2)").await, ["1.8750000000000000"] as [&str; 1], "SELECT avg(v) FROM m WHERE id IN (1, 2)");
    assert_eq!(rows(&c, "SELECT id, v::text, v::float8 FROM m WHERE id < 5 ORDER BY id").await, ["1|1.5|1.5", "2|2.250|2.25", "3|0.000000000001|1e-12", "4|100|100"] as [&str; 4], "SELECT id, v::text, v::float8 FROM m WHERE id < 5 ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM m WHERE v + 1 > 2.5 ORDER BY id").await, ["2", "4", "8"] as [&str; 3], "SELECT id FROM m WHERE v + 1 > 2.5 ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM m WHERE v * 2 = 3 ORDER BY id").await, ["1"] as [&str; 1], "SELECT id FROM m WHERE v * 2 = 3 ORDER BY id");
}

/// A constant too large for an i128 at nine places (1e30) is numeric, exact in arithmetic, a comparison, a CASE and an aggregate. PostgreSQL 17's values.
#[tokio::test(flavor = "multi_thread")]
async fn a_constant_past_one_point_seven_e29_is_numeric() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE t (id INT PRIMARY KEY, n NUMERIC(12,2), i BIGINT, f FLOAT8)").await;
    run(&c, "INSERT INTO t VALUES (1, 7.00, 7, 0.5), (2, 1.25, -3, 2.5)").await;
    assert_eq!(rows(&c, "SELECT id, n * 1e30, i * 1e30, n + 1e30, 1e30 / n FROM t ORDER BY id").await, ["1|7000000000000000000000000000000.00|7000000000000000000000000000000|1000000000000000000000000000007.00|142857142857142857142857142857.14", "2|1250000000000000000000000000000.00|-3000000000000000000000000000000|1000000000000000000000000000001.25|800000000000000000000000000000.00"] as [&str; 2], "SELECT id, n * 1e30, i * 1e30, n + 1e30, 1e30 / n FROM t ORDER BY id");
    assert_eq!(rows(&c, "SELECT id, f * 1e30, f + 1e30 FROM t ORDER BY id").await, ["1|5e+29|1e+30", "2|2.5e+30|1e+30"] as [&str; 2], "SELECT id, f * 1e30, f + 1e30 FROM t ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM t WHERE n < 1e30 ORDER BY id").await, ["1", "2"] as [&str; 2], "SELECT id FROM t WHERE n < 1e30 ORDER BY id");
    assert_eq!(rows(&c, "SELECT id, CASE WHEN n > 2 THEN 1e30 ELSE n END FROM t ORDER BY id").await, ["1|1000000000000000000000000000000", "2|1.25"] as [&str; 2], "SELECT id, CASE WHEN n > 2 THEN 1e30 ELSE n END FROM t ORDER BY id");
    assert_eq!(rows(&c, "SELECT sum(n * 1e30), max(i * 1e29) FROM t").await, ["8250000000000000000000000000000.00|700000000000000000000000000000"] as [&str; 1], "SELECT sum(n * 1e30), max(i * 1e29) FROM t");
}

/// 20,000 rows of an unconstrained NUMERIC, at many scales, with and without
/// a secondary index: every equality, IN list, range, aggregate and sort
/// answers as PostgreSQL 17 does. The filter index fingerprinted each
/// seventeen-byte cell as "no value", so an equality pruned every row and
/// answered none, with no error.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_own_scale_column_answers_every_predicate() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE big (id INT PRIMARY KEY, v NUMERIC, n2 NUMERIC(12,2), k INT)").await;
    let values: Vec<String> = (1..=20_000u32)
        .map(|i| {
            let v = if i % 7 == 0 {
                format!("0.{i:010}")
            } else if i % 3 == 0 {
                format!("{}.{:02}0", i / 100, i % 100)
            } else {
                format!("{}.{:02}", i / 100, i % 100)
            };
            format!("({i}, {v}, {}.{:02}, {})", i / 100, i % 100, i % 50)
        })
        .collect();
    for chunk in values.chunks(1000) {
        run(&c, &format!("INSERT INTO big VALUES {}", chunk.join(", "))).await;
    }
    let checks: &[(&str, &[&str])] = &[
        ("SELECT count(*) FROM big WHERE v = 12.34", &["1"]),
        ("SELECT count(*) FROM big WHERE v = 12.340", &["1"]),
        ("SELECT count(*) FROM big WHERE v = 0.0000000070", &["1"]),
        ("SELECT count(*) FROM big WHERE v = 0.000000007", &["1"]),
        ("SELECT count(*) FROM big WHERE v > 150.5", &["4243"]),
        ("SELECT count(*) FROM big WHERE v BETWEEN 10 AND 20", &["858"]),
        ("SELECT count(*) FROM big WHERE v < 0.0000001", &["142"]),
        ("SELECT count(*) FROM big WHERE v IN (1.01, 2.02, 3.030, 4.04, 0.0000000014)", &["5"]),
        ("SELECT count(*), sum(v), min(v), max(v) FROM big WHERE k = 3", &["400|34210.2900570171|0.0000000203|199.530"]),
        (
            "SELECT k, count(*), sum(v) FROM big GROUP BY k ORDER BY k LIMIT 3",
            &["0|400|34314.5000578550", "1|400|34146.4300575757", "2|400|34178.3600572964"],
        ),
        ("SELECT id, v FROM big ORDER BY v DESC, id LIMIT 3", &["20000|200.00", "19998|199.980", "19997|199.97"]),
        ("SELECT id, v FROM big WHERE v >= 199.99 ORDER BY id", &["20000|200.00"]),
    ];
    for (sql, want) in checks {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
    run(&c, "CREATE INDEX big_v ON big (v)").await;
    for (sql, want) in checks {
        assert_eq!(rows(&c, sql).await, *want, "{sql} (indexed)");
    }
}
