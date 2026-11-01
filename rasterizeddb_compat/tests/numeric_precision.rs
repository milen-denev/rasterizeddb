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
    // An average below 1 is at twenty places, past the AVG item's sixteen:
    // a CTE or derived table keeps all of them (it declared the item's).
    run(&c, "CREATE TABLE sm (id INT PRIMARY KEY, w NUMERIC(10,2))").await;
    run(&c, "INSERT INTO sm VALUES (1, 0.10), (2, 0.25)").await;
    assert_eq!(rows(&c, "WITH c AS (SELECT avg(w) AS a FROM sm) SELECT a FROM c").await, ["0.17500000000000000000"]);
    assert_eq!(rows(&c, "SELECT a FROM (SELECT avg(w) AS a FROM sm) d").await, ["0.17500000000000000000"]);
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

/// 20,000 rows of an unconstrained NUMERIC, at many scales, with no index, a
/// B-tree and a hash index: every equality, IN list, range, aggregate and sort
/// answers as PostgreSQL 17 does. The filter index fingerprinted each
/// seventeen-byte cell as "no value", so an equality pruned every row and
/// answered none, with no error; a hash index keyed 0.0000000070 apart from
/// the 0.000000007 it equals.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_own_scale_column_answers_every_predicate() {
    // File-backed, so the filter index is built and asked, as a server runs.
    let root = std::env::temp_dir().join(format!("numeric_precision_large_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("root");
    struct Root(std::path::PathBuf);
    impl Drop for Root {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = Root(root);
    let server = TestServer::start_file_backed(&root.0.to_string_lossy()).await.expect("server");
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
    // The filter (and below, each index) built and asked, as on a server
    // that has had a moment: before it is ready a scan does not consult it.
    let ready = || async {
        let t = server.database().table_for_tests("big").expect("the table");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !t.filters_ready().unwrap() || t.indexes().unwrap().is_some_and(|ix| !ix.is_ready()) {
            assert!(std::time::Instant::now() < deadline, "the filter or the indexes never became ready");
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    };
    ready().await;
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
    ready().await;
    for (sql, want) in checks {
        assert_eq!(rows(&c, sql).await, *want, "{sql} (B-tree)");
    }
    run(&c, "DROP INDEX big_v").await;
    run(&c, "CREATE INDEX big_h ON big USING hash (v)").await;
    ready().await;
    for (sql, want) in checks {
        assert_eq!(rows(&c, sql).await, *want, "{sql} (hash)");
    }
}

/// A literal with more digits than a NUMERIC cell holds (48) against an
/// unconstrained column: bounded at the 38 places a stored value can have,
/// so 0.123456789123 is below it and 0.12345678912300000000000000000000000001
/// above it, as PostgreSQL 17 answers. Bounded at nine places, both were
/// misread.
#[tokio::test(flavor = "multi_thread")]
async fn an_own_scale_column_compares_with_a_literal_past_38_digits() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE lq (id INT PRIMARY KEY, v NUMERIC)").await;
    run(
        &c,
        "INSERT INTO lq VALUES (1, 0.123456789123), (2, 0.123456789124), (3, 0.123456789), \
         (4, 0.12345678912300000000000000000000000001)",
    )
    .await;
    let lit = "0.123456789123000000000000000000000000000000000001";
    for (op, want) in [("=", ""), ("<>", "1,2,3,4"), ("<", "1,3"), ("<=", "1,3"), (">", "2,4"), (">=", "2,4")] {
        let sql = format!("SELECT id FROM lq WHERE v {op} {lit} ORDER BY id");
        assert_eq!(rows(&c, &sql).await.join(","), want, "{sql}");
    }
}

/// An unconstrained NUMERIC compared with a float8 compares the stored value's own digits with the double's: =, <>, ranges (either side), IN, NOT IN, IN with NULL, a list mixing floats and numbers (float8, its common type), subqueries, a CHECK, UPDATE WHERE and DELETE WHERE. PostgreSQL 17's values.
#[tokio::test(flavor = "multi_thread")]
async fn float_comparisons_on_an_own_scale_column() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE fo (id INT PRIMARY KEY, n NUMERIC, f FLOAT8)").await;
    run(&c, "INSERT INTO fo VALUES (1, 0.1, 0.1), (2, 0.10000000000000001, 0.1), (3, 0.10000000000000003, 0.10000000000000003), (4, 0.30000000000000004, 0.30000000000000004), (5, 0.3, 0.3), (6, 1234567890.123456789, 1234567890.123456789), (7, 1234567890.1234567, 1234567890.1234567), (8, 9007199254740993, 9007199254740992), (9, 0.1000000000000000055511151231257827, 0.1), (10, 2.5, 2.5), (11, NULL, NULL)").await;
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE n = '0.1'::float8 ORDER BY id").await, ["1", "2", "9"] as [&str; 3], "SELECT id FROM fo WHERE n = '0.1'::float8 ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE n > '0.1'::float8 ORDER BY id").await, ["3", "4", "5", "6", "7", "8", "10"] as [&str; 7], "SELECT id FROM fo WHERE n > '0.1'::float8 ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE n >= '0.1'::float8 ORDER BY id").await, ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"] as [&str; 10], "SELECT id FROM fo WHERE n >= '0.1'::float8 ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE n < '0.1'::float8 ORDER BY id").await, [] as [&str; 0], "SELECT id FROM fo WHERE n < '0.1'::float8 ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE n <> '0.1'::float8 ORDER BY id").await, ["3", "4", "5", "6", "7", "8", "10"] as [&str; 7], "SELECT id FROM fo WHERE n <> '0.1'::float8 ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE n = '0.30000000000000004'::float8 ORDER BY id").await, ["4"] as [&str; 1], "SELECT id FROM fo WHERE n = '0.30000000000000004'::float8 ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE '0.1'::float8 < n ORDER BY id").await, ["3", "4", "5", "6", "7", "8", "10"] as [&str; 7], "SELECT id FROM fo WHERE '0.1'::float8 < n ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE '0.1'::float8 >= n ORDER BY id").await, ["1", "2", "9"] as [&str; 3], "SELECT id FROM fo WHERE '0.1'::float8 >= n ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE '0.1'::float8 = n ORDER BY id").await, ["1", "2", "9"] as [&str; 3], "SELECT id FROM fo WHERE '0.1'::float8 = n ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE n = '1234567890.123456789'::float8 ORDER BY id").await, ["6", "7"] as [&str; 2], "SELECT id FROM fo WHERE n = '1234567890.123456789'::float8 ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE n = '9007199254740992'::float8 ORDER BY id").await, ["8"] as [&str; 1], "SELECT id FROM fo WHERE n = '9007199254740992'::float8 ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE n IN ('0.1'::float8, '0.30000000000000004'::float8, '2.5'::float8) ORDER BY id").await, ["1", "2", "4", "9", "10"] as [&str; 5], "SELECT id FROM fo WHERE n IN ('0.1'::float8, '0.30000000000000004'::float8, '2.5'::float8) ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE n NOT IN ('0.1'::float8, '0.30000000000000004'::float8) ORDER BY id").await, ["3", "5", "6", "7", "8", "10"] as [&str; 6], "SELECT id FROM fo WHERE n NOT IN ('0.1'::float8, '0.30000000000000004'::float8) ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE n IN ('0.1'::float8, NULL) ORDER BY id").await, ["1", "2", "9"] as [&str; 3], "SELECT id FROM fo WHERE n IN ('0.1'::float8, NULL) ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE n NOT IN ('0.1'::float8, NULL) ORDER BY id").await, [] as [&str; 0], "SELECT id FROM fo WHERE n NOT IN ('0.1'::float8, NULL) ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE n IN ('0.1'::float8, 2.5) ORDER BY id").await, ["1", "2", "9", "10"] as [&str; 4], "SELECT id FROM fo WHERE n IN ('0.1'::float8, 2.5) ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE n NOT IN (2.5, '0.1'::float8, 3) ORDER BY id").await, ["3", "4", "5", "6", "7", "8"] as [&str; 6], "SELECT id FROM fo WHERE n NOT IN (2.5, '0.1'::float8, 3) ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE n IN (SELECT f FROM fo WHERE id IN (1, 4)) ORDER BY id").await, ["1", "2", "4", "9"] as [&str; 4], "SELECT id FROM fo WHERE n IN (SELECT f FROM fo WHERE id IN (1, 4)) ORDER BY id");
    assert_eq!(rows(&c, "SELECT id FROM fo WHERE n = (SELECT f FROM fo WHERE id = 1) ORDER BY id").await, ["1", "2", "9"] as [&str; 3], "SELECT id FROM fo WHERE n = (SELECT f FROM fo WHERE id = 1) ORDER BY id");
    assert_eq!(rows(&c, "SELECT id, n = '0.1'::float8, n > '0.1'::float8 FROM fo ORDER BY id").await, ["1|t|f", "2|t|f", "3|f|t", "4|f|t", "5|f|t", "6|f|t", "7|f|t", "8|f|t", "9|t|f", "10|f|t", "11||"] as [&str; 11], "SELECT id, n = '0.1'::float8, n > '0.1'::float8 FROM fo ORDER BY id");
    run(&c, "CREATE TABLE fc (id INT PRIMARY KEY, n NUMERIC CHECK (n <> '0.1'::float8))").await;
    assert_eq!(sqlstate(&c, "INSERT INTO fc VALUES (1, 0.10000000000000001)").await, "23514");
    run(&c, "INSERT INTO fc VALUES (2, 0.2)").await;
    assert_eq!(rows(&c, "SELECT id FROM fc ORDER BY id").await, ["2"] as [&str; 1], "SELECT id FROM fc ORDER BY id");
    run(&c, "CREATE TABLE fk (id INT PRIMARY KEY, n NUMERIC CHECK (n NOT IN ('0.1'::float8, 7)))").await;
    assert_eq!(sqlstate(&c, "INSERT INTO fk VALUES (1, 0.10000000000000001)").await, "23514");
    run(&c, "INSERT INTO fk VALUES (2, 0.2)").await;
    assert_eq!(rows(&c, "SELECT id FROM fk ORDER BY id").await, ["2"] as [&str; 1], "SELECT id FROM fk ORDER BY id");
    run(&c, "UPDATE fo SET f = 9 WHERE n = '0.1'::float8").await;
    run(&c, "DELETE FROM fo WHERE n > '0.1'::float8 AND n < '0.2'::float8").await;
    assert_eq!(rows(&c, "SELECT id, f FROM fo ORDER BY id").await, ["1|9", "2|9", "4|0.30000000000000004", "5|0.3", "6|1234567890.1234567", "7|1234567890.1234567", "8|9.007199254740992e+15", "9|9", "10|2.5", "11|"] as [&str; 10], "SELECT id, f FROM fo ORDER BY id");
}

/// ON CONFLICT over an unconstrained NUMERIC key: 1.5 and 1.50 are one key (in the table and within one statement, 21000), a key past nine places conflicts with itself written with a trailing zero, DO NOTHING keeps the first of two equal keys, and EXCLUDED of a whole number is numeric (excluded.u / 3 is 0.333…). PostgreSQL 17's values.
#[tokio::test(flavor = "multi_thread")]
async fn on_conflict_over_an_own_scale_key() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE up (k NUMERIC PRIMARY KEY, v NUMERIC, n INT)").await;
    run(&c, "INSERT INTO up VALUES (1.5, 1.50, 1)").await;
    run(&c, "INSERT INTO up VALUES (1.50, 2.250, 2) ON CONFLICT (k) DO UPDATE SET v = excluded.v, n = up.n + 10").await;
    assert_eq!(rows(&c, "SELECT k, v, n FROM up").await, ["1.5|2.250|11"] as [&str; 1], "SELECT k, v, n FROM up");
    run(&c, "INSERT INTO up VALUES (0.0000000000001, 3.0, 3) ON CONFLICT (k) DO UPDATE SET n = 99").await;
    run(&c, "INSERT INTO up VALUES (0.00000000000010, 4.0, 4) ON CONFLICT (k) DO UPDATE SET v = excluded.v + 1, n = 98").await;
    assert_eq!(rows(&c, "SELECT k, v, n FROM up ORDER BY k").await, ["0.0000000000001|5.0|98", "1.5|2.250|11"] as [&str; 2], "SELECT k, v, n FROM up ORDER BY k");
    run(&c, "INSERT INTO up VALUES (7, 7, 7), (7.0, 8, 8) ON CONFLICT (k) DO NOTHING").await;
    assert_eq!(rows(&c, "SELECT k, v, n FROM up ORDER BY k").await, ["0.0000000000001|5.0|98", "1.5|2.250|11", "7|7|7"] as [&str; 3], "SELECT k, v, n FROM up ORDER BY k");
    run(&c, "CREATE TABLE ub (k NUMERIC PRIMARY KEY, n INT)").await;
    assert_eq!(sqlstate(&c, "INSERT INTO ub VALUES (3.5, 1), (3.50, 2) ON CONFLICT (k) DO UPDATE SET n = 0").await, "21000");
    run(&c, "INSERT INTO ub VALUES (3.5, 1), (4, 2) ON CONFLICT (k) DO UPDATE SET n = 0").await;
    run(&c, "INSERT INTO ub VALUES (3.50, 5), (4.0, 6) ON CONFLICT (k) DO UPDATE SET n = excluded.n * 10").await;
    assert_eq!(rows(&c, "SELECT k, n FROM ub ORDER BY k").await, ["3.5|50", "4|60"] as [&str; 2], "SELECT k, n FROM ub ORDER BY k");
    run(&c, "CREATE TABLE ux (id INT PRIMARY KEY, u NUMERIC)").await;
    run(&c, "INSERT INTO ux VALUES (1, 0)").await;
    run(&c, "INSERT INTO ux VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET u = excluded.u / 3").await;
    run(&c, "INSERT INTO ux VALUES (2, 7) ON CONFLICT (id) DO UPDATE SET u = 0").await;
    run(&c, "INSERT INTO ux VALUES (2, 2) ON CONFLICT (id) DO UPDATE SET u = excluded.u + ux.u").await;
    assert_eq!(rows(&c, "SELECT id, u FROM ux ORDER BY id").await, ["1|0.33333333333333333333", "2|9"] as [&str; 2], "SELECT id, u FROM ux ORDER BY id");
}

/// An average PostgreSQL gives more than 38 places with a nonzero digit past
/// them is refused (0A000), as `n / 2` is, by the aggregate, the window
/// function, and on the way into a column (INSERT … SELECT, UPDATE SET
/// (SELECT avg)), which store nothing. It was rounded to 38 places in
/// silence. PostgreSQL 17 gives 0.000…0015000000000000000000 (56 places) for
/// the first.
#[tokio::test(flavor = "multi_thread")]
async fn an_average_past_38_places_is_refused() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE ar (id INT PRIMARY KEY, n NUMERIC, g INT)").await;
    run(
        &c,
        "INSERT INTO ar VALUES (1, 0.00000000000000000000000000000000000001, 1), \
         (2, 0.00000000000000000000000000000000000002, 1), (3, 0.1, 2), (4, 0.2, 2)",
    )
    .await;
    for sql in [
        "SELECT avg(n) FROM ar WHERE id IN (1, 2)",
        "SELECT g, avg(n) FROM ar GROUP BY g ORDER BY g",
        "SELECT avg(n) OVER () FROM ar WHERE id IN (1, 2) ORDER BY id",
        "SELECT id, avg(n) OVER (PARTITION BY g) FROM ar ORDER BY id",
    ] {
        assert_eq!(sqlstate(&c, sql).await, "0A000", "{sql}");
    }
    // Within 38 places, PostgreSQL 17's values: its own scale for the second
    // is 38, so it rounds there too.
    assert_eq!(rows(&c, "SELECT avg(n) FROM ar WHERE id IN (1, 3)").await, ["0.05000000000000000000000000000000000001"]);
    assert_eq!(rows(&c, "SELECT avg(n) FROM ar WHERE g = 2").await, ["0.15000000000000000000"]);
    assert_eq!(rows(&c, "SELECT avg(n) OVER () FROM ar WHERE id = 3").await, ["0.10000000000000000000"]);
    // Past 38 places with only zeros there: given at 38. PostgreSQL 17 shows
    // the same value with 18 more zeros (0.000…0003000000000000000000).
    run(&c, "CREATE TABLE ar3 (id INT PRIMARY KEY, n NUMERIC)").await;
    run(
        &c,
        "INSERT INTO ar3 VALUES (1, 0.00000000000000000000000000000000000002), \
         (2, 0.00000000000000000000000000000000000004)",
    )
    .await;
    assert_eq!(rows(&c, "SELECT avg(n) FROM ar3").await, ["0.00000000000000000000000000000000000003"]);
    run(&c, "CREATE TABLE ar2 (id INT PRIMARY KEY, n NUMERIC)").await;
    assert_eq!(sqlstate(&c, "INSERT INTO ar2 SELECT 1, avg(n) FROM ar WHERE id IN (1, 2)").await, "0A000");
    run(&c, "INSERT INTO ar2 VALUES (2, 7)").await;
    assert_eq!(
        sqlstate(&c, "UPDATE ar2 SET n = (SELECT avg(n) FROM ar WHERE id IN (1, 2)) WHERE id = 2").await,
        "0A000"
    );
    run(&c, "INSERT INTO ar2 SELECT 3, avg(n) FROM ar WHERE g = 2").await;
    assert_eq!(rows(&c, "SELECT id, n FROM ar2 ORDER BY id").await, ["2|7", "3|0.15000000000000000000"]);
}

/// A DEFAULT with more than nine places on an unconstrained NUMERIC is accepted and stored whole, as is a DEFAULT 1.50 at its scale. PostgreSQL 17's values.
#[tokio::test(flavor = "multi_thread")]
async fn a_numeric_default_keeps_its_places() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE d (id INT PRIMARY KEY, v NUMERIC DEFAULT 1.50, w NUMERIC DEFAULT 0.0000000000001)").await;
    run(&c, "INSERT INTO d (id) VALUES (1)").await;
    assert_eq!(rows(&c, "SELECT * FROM d").await, ["1|1.50|0.0000000000001"] as [&str; 1], "SELECT * FROM d");
}

async fn error_text(c: &Client, sql: &str) -> String {
    let e = c.simple_query(sql).await.expect_err(sql);
    e.as_db_error().map(|d| d.message().to_string()).unwrap_or_else(|| e.to_string())
}

/// A float written into NUMERIC columns: an unconstrained one keeps what float8_numeric gives within 38 places, a NUMERIC(p, s) rounds one too small for its scale to zero and overflows on one too large. PostgreSQL 17's values.
#[tokio::test(flavor = "multi_thread")]
async fn float_into_a_numeric_rounds_to_its_scale() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE fz (id INT PRIMARY KEY, n NUMERIC, p NUMERIC(10,2))").await;
    run(&c, "INSERT INTO fz VALUES (1, '1e-12'::float8, '1e-300'::float8)").await;
    run(&c, "INSERT INTO fz VALUES (2, '1e-38'::float8, '-1e-45'::real)").await;
    run(&c, "INSERT INTO fz VALUES (3, '0.1'::real, '1e-40'::float8)").await;
    run(&c, "INSERT INTO fz VALUES (4, 0, 5)").await;
    run(&c, "UPDATE fz SET p = '1e-200'::float8 WHERE id = 4").await;
    assert_eq!(sqlstate(&c, "INSERT INTO fz VALUES (5, 0, '1e40'::float8)").await, "22003");
    assert_eq!(rows(&c, "SELECT id, n, p FROM fz ORDER BY id").await, ["1|0.000000000001|0.00", "2|0.00000000000000000000000000000000000001|0.00", "3|0.1|0.00", "4|0|0.00"] as [&str; 4], "SELECT id, n, p FROM fz ORDER BY id");
}

/// A float too small or too large for the 38 digits a NUMERIC carries here,
/// written into an unconstrained column, is refused (0A000) on every path
/// in, as `f::numeric` is, and nothing is stored: INSERT VALUES (a literal
/// and an expression), INSERT … SELECT, UPDATE SET (a literal, a subquery,
/// a column of UPDATE … FROM). It was stored as 0. PostgreSQL 17 stores
/// `1e-300` whole (a 300-place value).
#[tokio::test(flavor = "multi_thread")]
async fn a_float_past_38_digits_into_an_unconstrained_numeric_is_refused() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE fs (id INT PRIMARY KEY, f FLOAT8)").await;
    run(&c, "INSERT INTO fs VALUES (1, 1e-300), (2, 1e-39), (3, 0.5), (4, 1e40)").await;
    run(&c, "CREATE TABLE fz (id INT PRIMARY KEY, n NUMERIC)").await;
    run(&c, "INSERT INTO fz VALUES (1, 3), (2, 3)").await;
    for sql in [
        "INSERT INTO fz VALUES (10, '1e-300'::float8)",
        "INSERT INTO fz VALUES (11, '1e-20'::float8 * '1e-20'::float8)",
        "INSERT INTO fz VALUES (12, '1e-45'::real)",
        "INSERT INTO fz VALUES (13, '1e40'::float8)",
        "INSERT INTO fz SELECT id + 20, f FROM fs WHERE id = 2",
        "UPDATE fz SET n = '1e-200'::float8 WHERE id = 1",
        "UPDATE fz SET n = (SELECT f FROM fs WHERE id = 1) WHERE id = 1",
        "UPDATE fz SET n = fs.f FROM fs WHERE fs.id = 2 AND fz.id = 2",
        "UPDATE fz SET n = fs.f FROM fs WHERE fs.id = 4 AND fz.id = 2",
    ] {
        assert_eq!(sqlstate(&c, sql).await, "0A000", "{sql}");
    }
    assert_eq!(
        error_text(&c, "INSERT INTO fz VALUES (10, '1e-300'::float8)").await,
        "numeric value \"1e-300\" needs more than the 38 digits or 38 decimal places column \"n\" holds (not supported)"
    );
    // Within 38 places it is stored, as PostgreSQL 17 stores it.
    run(&c, "UPDATE fz SET n = fs.f FROM fs WHERE fs.id = 3 AND fz.id = 2").await;
    assert_eq!(rows(&c, "SELECT id, n FROM fz ORDER BY id").await, ["1|3", "2|0.5"]);
}

/// A value an unconstrained column made by this build cannot hold is refused
/// with the limit it has (38 digits, 38 places), not the nine places and
/// conversion hint of a column made before cells kept their scale.
#[tokio::test(flavor = "multi_thread")]
async fn an_own_scale_refusal_names_its_own_limit() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE w (id INT PRIMARY KEY, n NUMERIC)").await;
    run(&c, "INSERT INTO w VALUES (1, 0)").await;
    let too_long = format!("0.{}1", "0".repeat(38));
    for sql in [
        format!("INSERT INTO w VALUES (2, {too_long})"),
        format!("INSERT INTO w VALUES (2, '{too_long}')"),
        format!("UPDATE w SET n = {too_long} WHERE id = 1"),
        format!("UPDATE w SET n = '{too_long}' WHERE id = 1"),
        format!("UPDATE w SET n = '{too_long}' || '' WHERE id = 1"),
    ] {
        let message = error_text(&c, &sql).await;
        assert_eq!(
            message,
            format!(
                "numeric value \"{too_long}\" needs more than the 38 digits or 38 decimal places column \"n\" holds \
                 (not supported)"
            ),
            "{sql}"
        );
    }
    let message = error_text(&c, &format!("CREATE TABLE w2 (id INT PRIMARY KEY, n NUMERIC DEFAULT {too_long})")).await;
    assert!(message.contains("38 decimal places the column holds"), "{message}");
    assert!(!message.contains("predates") && !message.contains("nine") && !message.contains(" 9 "), "{message}");
}
