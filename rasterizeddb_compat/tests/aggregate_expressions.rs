//! Aggregates inside expressions, a boolean standing alone as a condition, and
//! `round()`. Every expectation is PostgreSQL 17's (`docker run --rm
//! postgres:17`), on a plain and a partitioned table alike.
//!
//! The report that led here was an analytics query, `ROUND(100.0 *
//! SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) / COUNT(*), 2)`, refused
//! three ways at once: a `CASE` argument inside arithmetic was looked up as a
//! column, `WHEN is_conversion` was "not a comparison", and `round` did not
//! exist. Underneath was a silent one: a `FILTER` on an aggregate inside an
//! expression was peeled off and applied to nothing, so `COUNT(*) FILTER
//! (WHERE …) * 100.0 / COUNT(*)` answered 100.

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
                Some((0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"))
            }
            _ => None,
        })
        .collect()
}

/// The statement's SQLSTATE and message.
async fn refused(c: &Client, sql: &str) -> (String, String) {
    let e = c.simple_query(sql).await.expect_err(sql);
    let db = e.as_db_error().unwrap_or_else(|| panic!("{sql}: not a server error: {e}"));
    (db.code().code().to_string(), db.message().to_string())
}

/// A file-backed server, which partitioned tables need; removed on drop.
struct OnDisk(std::path::PathBuf);

impl Drop for OnDisk {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn on_disk(tag: &str) -> (TestServer, OnDisk) {
    let root = std::env::temp_dir().join(format!("aggregate_expressions_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let server = TestServer::start_file_backed(&root.to_string_lossy()).await.expect("server");
    (server, OnDisk(root))
}

const ROWS: &str = "(1,1,'view',0,false,2.5,1.25),(2,1,'buy',10.50,true,3.5,1249.5),(3,2,'view',0,false,-2.5,-0.004),\
                    (4,2,'buy',20.00,true,0.5,99.995),(5,3,'view',NULL,NULL,NULL,NULL),(6,3,'buy',5.25,false,1.25,2.675)";

/// `ev`, plain, and `pev`, the same rows hash-partitioned.
async fn events(c: &Client) {
    run(c, "CREATE TABLE ev (id INT, user_id INT, event_type TEXT, revenue NUMERIC(10,2), is_conversion BOOLEAN, f FLOAT8, u NUMERIC)").await;
    run(c, "CREATE TABLE pev (tenant_id BIGINT NOT NULL, id INT, user_id INT, event_type TEXT, revenue NUMERIC(10,2), \
            is_conversion BOOLEAN, f FLOAT8, u NUMERIC) PARTITION BY HASH (tenant_id)").await;
    run(c, &format!("INSERT INTO ev VALUES {ROWS}")).await;
    // The tenant is the user, so every partition holds some of the rows.
    let partitioned = ROWS.replace("),(", ")\n(").lines().map(|r| {
        let inner = r.trim_start_matches('(').trim_end_matches(')');
        let user = inner.split(',').nth(1).unwrap();
        format!("({user},{inner})")
    }).collect::<Vec<_>>().join(",");
    run(c, &format!("INSERT INTO pev VALUES {partitioned}")).await;
}

/// The reported query and its neighbours: a computed aggregate argument, on
/// its own and inside arithmetic, with and without `round`.
#[tokio::test(flavor = "multi_thread")]
async fn a_case_argument_is_folded_inside_arithmetic() {
    let (server, _dir) = on_disk("case").await;
    let c = client(&server).await;
    events(&c).await;
    for t in ["ev", "pev"] {
        let cases: &[(String, &[&str])] = &[
            (format!("SELECT SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) FROM {t}"), &["2"]),
            (format!("SELECT SUM(CASE WHEN is_conversion THEN 1 END) FROM {t}"), &["2"]),
            (format!("SELECT 100.0 * SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) / COUNT(*) FROM {t}"), &["33.3333333333333333"]),
            (
                format!(
                    "SELECT event_type, ROUND(100.0 * SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) / COUNT(*), 2) AS rate \
                     FROM {t} GROUP BY event_type ORDER BY event_type"
                ),
                &["buy|66.67", "view|0.00"],
            ),
            (format!("SELECT SUM(CASE WHEN is_conversion THEN revenue ELSE 0 END) - SUM(revenue) FROM {t}"), &["-5.25"]),
            (format!("SELECT ROUND(SUM(CASE WHEN is_conversion THEN revenue ELSE 0 END) / SUM(revenue) * 100, 1) FROM {t}"), &["85.3"]),
            (format!("SELECT MAX(CASE WHEN is_conversion THEN id END) - MIN(id) FROM {t}"), &["3"]),
            (format!("SELECT COALESCE(SUM(CASE WHEN user_id = 9 THEN revenue END), 0) + 1 FROM {t}"), &["1"]),
            (format!("SELECT COUNT(CASE WHEN is_conversion THEN 1 END) + COUNT(*) FROM {t}"), &["8"]),
            (format!("SELECT SUM(revenue * 2) + 1 FROM {t}"), &["72.50"]),
            (
                format!(
                    "SELECT event_type, AVG(CASE WHEN is_conversion THEN 1.0 ELSE 0 END) FROM {t} \
                     GROUP BY event_type ORDER BY event_type"
                ),
                &["buy|0.66666666666666666667", "view|0.00000000000000000000"],
            ),
            (
                format!("SELECT event_type FROM {t} GROUP BY event_type HAVING SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) > 0"),
                &["buy"],
            ),
        ];
        for (sql, want) in cases {
            assert_eq!(rows(&c, sql).await, *want, "{sql}");
        }
    }
}

/// A `FILTER` inside an expression belongs to its own call. It was peeled
/// off the whole item and applied to nothing: the first ratio answered 100,
/// the difference 0.
#[tokio::test(flavor = "multi_thread")]
async fn a_filter_inside_an_expression_filters_its_own_call() {
    let (server, _dir) = on_disk("filter").await;
    let c = client(&server).await;
    events(&c).await;
    for t in ["ev", "pev"] {
        let cases: &[(String, &[&str])] = &[
            (format!("SELECT COUNT(*) FILTER (WHERE user_id = 1) * 100.0 / COUNT(*) FROM {t}"), &["33.3333333333333333"]),
            (format!("SELECT COUNT(*) FILTER (WHERE is_conversion) * 100.0 / COUNT(*) AS r FROM {t}"), &["33.3333333333333333"]),
            (
                format!(
                    "SELECT event_type, SUM(revenue) FILTER (WHERE is_conversion) - SUM(revenue) AS d FROM {t} \
                     GROUP BY event_type ORDER BY event_type"
                ),
                &["buy|-5.25", "view|NULL"],
            ),
            (format!("SELECT COUNT(*) FILTER (WHERE user_id = 1) FROM {t}"), &["2"]),
        ];
        for (sql, want) in cases {
            assert_eq!(rows(&c, sql).await, *want, "{sql}");
        }
    }
    // A join's computed path carries no filter yet: refused, not answered.
    run(&c, "CREATE TABLE u (user_id INT, vip BOOLEAN)").await;
    run(&c, "INSERT INTO u VALUES (1, true), (2, false), (3, NULL)").await;
    let (code, message) =
        refused(&c, "SELECT COUNT(*) FILTER (WHERE e.user_id = 1) * 100.0 / COUNT(*) FROM ev e JOIN u ON e.user_id = u.user_id").await;
    assert_eq!(code, "42601", "{message}");
    assert_eq!(
        rows(&c, "SELECT COUNT(*) FILTER (WHERE u.vip) FROM ev e JOIN u ON e.user_id = u.user_id").await,
        ["2"]
    );
}

/// A boolean column alone is a condition wherever one is expected, NULL
/// staying UNKNOWN; and a select-list column is still a column.
#[tokio::test(flavor = "multi_thread")]
async fn a_bare_boolean_is_a_condition() {
    let (server, _dir) = on_disk("bool").await;
    let c = client(&server).await;
    events(&c).await;
    for t in ["ev", "pev"] {
        let cases: &[(String, &[&str])] = &[
            (format!("SELECT COUNT(*) FILTER (WHERE is_conversion) FROM {t}"), &["2"]),
            (format!("SELECT COUNT(*) FILTER (WHERE NOT is_conversion) FROM {t}"), &["3"]),
            (format!("SELECT COUNT(*) FILTER (WHERE is_conversion AND revenue > 15) FROM {t}"), &["1"]),
            (format!("SELECT COUNT(*) FILTER (WHERE is_conversion OR user_id = 3) FROM {t}"), &["4"]),
            (format!("SELECT COUNT(*) FILTER (WHERE true) FROM {t}"), &["6"]),
            (format!("SELECT SUM(is_conversion::int) FROM {t}"), &["2"]),
            (
                format!("SELECT id, CASE WHEN is_conversion THEN 'y' WHEN NOT is_conversion THEN 'n' ELSE '?' END FROM {t} ORDER BY id"),
                &["1|n", "2|y", "3|n", "4|y", "5|?", "6|n"],
            ),
            (format!("SELECT id, is_conversion FROM {t} ORDER BY id"), &["1|f", "2|t", "3|f", "4|t", "5|NULL", "6|f"]),
        ];
        for (sql, want) in cases {
            assert_eq!(rows(&c, sql).await, *want, "{sql}");
        }
    }
    // Not a boolean: still refused (PostgreSQL's 42804, here 42601).
    let (_, message) = refused(&c, "SELECT COUNT(*) FILTER (WHERE event_type) FROM ev").await;
    assert!(message.contains("event_type"), "{message}");
    // A CHECK that is a bare boolean refuses FALSE and passes NULL.
    run(&c, "CREATE TABLE ck (a INT, ok BOOLEAN, CHECK (ok))").await;
    run(&c, "INSERT INTO ck VALUES (1, true)").await;
    assert_eq!(refused(&c, "INSERT INTO ck VALUES (2, false)").await.0, "23514");
    run(&c, "INSERT INTO ck VALUES (3, NULL)").await;
    assert_eq!(rows(&c, "SELECT a FROM ck ORDER BY a").await, ["1", "3"]);
}

/// `round` as PostgreSQL's overloads: NUMERIC half away from zero at the
/// places asked (negative ones included, in one rounding step), an integer or
/// float alone half to even as `double precision`.
#[tokio::test(flavor = "multi_thread")]
async fn round_matches_postgresql() {
    let (server, _dir) = on_disk("round").await;
    let c = client(&server).await;
    events(&c).await;
    run(&c, "CREATE TABLE one (x INT)").await;
    run(&c, "INSERT INTO one VALUES (1)").await;
    let cases: &[(&str, &[&str])] = &[
        (
            "SELECT round(2.5), round(-2.5), round(0.5::numeric), round(1234.5678, -2), round(1250, -2), round(-1250.0, -2) FROM one",
            &["3|-3|1|1200|1300|-1300"],
        ),
        (
            "SELECT round(1.005, 2), round(1.004999, 2), round(2.675, 2), round(1.5, 0), round(1.23456789012345678901, 15) FROM one",
            &["1.01|1.00|2.68|2|1.234567890123457"],
        ),
        ("SELECT round(12.345, 5), round(12.345, 1)::text, round(NULL::numeric, 2), round(1.5, NULL) FROM one", &["12.34500|12.3|NULL|NULL"]),
        ("SELECT round(99.995, 2), round(-0.004, 2), round(0.0, 3), round(5::bigint, -1), round(123.456, 2) * 2 FROM one", &["100.00|0.00|0.000|10|246.92"]),
        ("SELECT round('1.25', 1) FROM one", &["1.3"]),
        ("SELECT round(5), round(5, 1), round(2.5::float8), round(3.5::float8), round(-2.5::float8), round(2.5::real) FROM one", &["5|5.0|2|4|-2|2"]),
        (
            "SELECT id, round(revenue), round(revenue, 1), round(f), round(u, 2), round(u, -1), round(u) FROM ev ORDER BY id",
            &["1|0|0.0|2|1.25|0|1", "2|11|10.5|4|1249.50|1250|1250", "3|0|0.0|-2|0.00|0|0", "4|20|20.0|0|100.00|100|100", "5|NULL|NULL|NULL|NULL|NULL|NULL", "6|5|5.3|1|2.68|0|3"],
        ),
        ("SELECT id FROM ev WHERE round(revenue) = 11 ORDER BY id", &["2"]),
        ("SELECT id FROM ev WHERE round(u, 2) = 100.00 ORDER BY id", &["4"]),
        ("SELECT id FROM ev WHERE round(f) = 2 ORDER BY id", &["1"]),
        ("SELECT SUM(round(revenue)), round(SUM(revenue), 1), round(AVG(revenue), 2), round(AVG(f)) FROM ev", &["36|35.8|7.15|1"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }
    for (sql, code) in [
        ("SELECT round(2.5::float8, 1) FROM one", "42883"),
        ("SELECT round(event_type) FROM ev", "42883"),
        ("SELECT round(1.5, 2.0) FROM one", "42883"),
    ] {
        let (got, message) = refused(&c, sql).await;
        assert_eq!(got, code, "{sql}: {message}");
    }
    // Places read from a column are not carried yet: refused, not guessed.
    let (_, message) = refused(&c, "SELECT round(revenue, id) FROM ev").await;
    assert!(message.contains("not supported"), "{message}");
}
