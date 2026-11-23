//! `AVG` over integers and `numeric`, printed with the digits PostgreSQL
//! prints.
//!
//! PostgreSQL divides the sum by the count with `numeric_div`, whose result
//! scale `select_div_scale` picks *per value*: enough for sixteen significant
//! digits, never less than either input's display scale. So `avg(1,2,2)` is
//! `1.6666666666666667`, `avg(0,1)` is `0.50000000000000000000`, and an
//! average near 10⁴ has four fewer places. Every expected string below was
//! produced by PostgreSQL 18 (`postgres:18-alpine`) from the same rows.

use rasterizeddb_compat::TestServer;
use rust_decimal::Decimal;
use sqlx::{AssertSqlSafe, Executor, Row};
use std::str::FromStr;

async fn seed(pool: &sqlx::PgPool) {
    for sql in [
        "CREATE TABLE ai (g INT NOT NULL, v INT)",
        "INSERT INTO ai (g, v) VALUES (1,1),(1,2),(1,2),(2,0),(2,1),(3,0),(3,0),(4,10000),(4,20001),(5,-1),(5,-2),(5,-2),(6,7),(8,123456789),(8,987654321),(9,-5),(9,3),(10,NULL)",
        "CREATE TABLE ab (g INT NOT NULL, v BIGINT)",
        "INSERT INTO ab (g, v) VALUES (1,100000000000000000),(1,300000000000000001),(2,9000000000000000000),(2,9000000000000000001),(3,-7),(3,8),(3,9)",
        "CREATE TABLE an (g INT NOT NULL, v NUMERIC(10,2))",
        "INSERT INTO an (g, v) VALUES (1,1.25),(1,2.50),(1,3.33),(2,0.01),(2,0.02),(3,-1.10),(3,2.20),(4,12345678.91),(4,0.09),(5,0)",
        "CREATE TABLE au (g INT NOT NULL, v NUMERIC)",
        "INSERT INTO au (g, v) VALUES (1,1.5),(1,2.25),(1,3),(2,0.001),(2,0.002),(3,12345678901.5),(3,1),(4,-0.000123),(4,0.000001),(5,100),(5,200)",
    ] {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    // g = 7: 1..=10000.
    for chunk in 0..10 {
        let values: Vec<String> = (1..=1000).map(|i| format!("(7, {})", chunk * 1000 + i)).collect();
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "INSERT INTO ai (g, v) VALUES {}",
            values.join(",")
        ))))
        .await
        .unwrap();
    }
}

/// `(table, group, PostgreSQL's text)`.
const EXPECTED: &[(&str, i32, Option<&str>)] = &[
    ("ai", 1, Some("1.6666666666666667")),
    ("ai", 2, Some("0.50000000000000000000")),
    ("ai", 3, Some("0.00000000000000000000")),
    ("ai", 4, Some("15000.500000000000")),
    ("ai", 5, Some("-1.6666666666666667")),
    ("ai", 6, Some("7.0000000000000000")),
    ("ai", 7, Some("5000.5000000000000000")),
    ("ai", 8, Some("555555555.00000000")),
    ("ai", 9, Some("-1.00000000000000000000")),
    ("ai", 10, None),
    ("ab", 1, Some("200000000000000001")),
    ("ab", 2, Some("9000000000000000001")),
    ("ab", 3, Some("3.3333333333333333")),
    ("an", 1, Some("2.3600000000000000")),
    ("an", 2, Some("0.01500000000000000000")),
    ("an", 3, Some("0.55000000000000000000")),
    ("an", 4, Some("6172839.500000000000")),
    ("an", 5, Some("0.00000000000000000000")),
    ("au", 1, Some("2.2500000000000000")),
    ("au", 2, Some("0.00150000000000000000")),
    ("au", 3, Some("6172839451.25000000")),
    ("au", 4, Some("-0.000061000000000000000000")),
    ("au", 5, Some("150.0000000000000000")),
];

#[tokio::test(flavor = "multi_thread")]
async fn avg_prints_postgres_digits_over_both_protocols() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    seed(&pool).await;

    let mut wrong = Vec::new();
    for table in ["ai", "ab", "an", "au"] {
        let sql = format!("SELECT g, AVG(v) FROM {table} GROUP BY g ORDER BY g");
        // Text: the simple-query protocol.
        let text: Vec<(i32, Option<String>)> = sqlx::raw_sql(AssertSqlSafe(sql.clone()))
            .fetch_all(&pool)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .iter()
            .map(|r| {
                let g: String = r.try_get_unchecked(0).unwrap();
                (g.parse().unwrap(), r.try_get_unchecked::<Option<String>, _>(1).unwrap())
            })
            .collect();
        // Binary: a prepared statement, decoded as a decimal carrying its scale.
        let binary: Vec<(i32, Option<Decimal>)> = sqlx::query(AssertSqlSafe(sql.clone()))
            .fetch_all(&pool)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        for (t, g, want) in EXPECTED.iter().filter(|(t, ..)| *t == table) {
            let got_text = text.iter().find(|(gg, _)| gg == g).map(|(_, v)| v.clone());
            if got_text != Some(want.map(|s| s.to_string())) {
                wrong.push(format!("{t} g={g} text: got {got_text:?}, want {want:?}"));
            }
            let got_bin = binary.iter().find(|(gg, _)| gg == g).map(|(_, v)| *v);
            let want_bin = want.map(|s| Decimal::from_str(s).unwrap());
            // The string form carries the scale. A zero is compared by value:
            // a binary numeric zero has no digits, and the decoder reads it
            // back as a bare `0` whatever its display scale says — for
            // PostgreSQL's own value as much as for ours.
            let render = |d: Decimal| if d.is_zero() { "0".to_string() } else { d.to_string() };
            if got_bin.map(|v| v.map(render)) != Some(want_bin.map(render)) {
                wrong.push(format!("{t} g={g} binary: got {got_bin:?}, want {want:?}"));
            }
        }
    }

    // Ungrouped, over the whole table.
    for (sql, want) in [
        ("SELECT AVG(v) FROM ai", "115917.551861834881"),
        ("SELECT AVG(v) FROM au", "1122334473.56844345"),
    ] {
        let got: Option<String> = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
            .fetch_one(&pool)
            .await
            .unwrap()
            .try_get_unchecked(0)
            .unwrap();
        if got.as_deref() != Some(want) {
            wrong.push(format!("{sql}: got {got:?}, want {want}"));
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// An `AVG` result that leaves its query — for a CTE, a derived table, a
/// scalar subquery — or is sorted and filtered inside it, keeps its value.
/// (Its cells carry their own scale; every reader has to honour it.)
#[tokio::test(flavor = "multi_thread")]
async fn avg_results_keep_their_value_through_other_queries() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    seed(&pool).await;
    let text = |sql: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
                .fetch_all(&pool)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
                .iter()
                .map(|r| {
                    (0..r.len())
                        .map(|i| r.try_get_unchecked::<Option<String>, _>(i).unwrap().unwrap_or_default())
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect::<Vec<_>>()
        }
    };
    // Ordered by the average: g 9 (-1), 5 (-1.67), ... numeric order, not bytes.
    let ordered: Vec<String> = text("SELECT g, AVG(v) FROM ai WHERE g IN (1, 2, 4, 5, 9) GROUP BY g ORDER BY AVG(v)")
        .await
        .iter()
        .map(|r| r.split('|').next().unwrap().to_string())
        .collect();
    assert_eq!(ordered, vec!["5", "9", "2", "1", "4"]);
    assert_eq!(
        text("SELECT g FROM ai GROUP BY g HAVING AVG(v) > 1000 ORDER BY g").await,
        vec!["4", "7", "8"]
    );
    // Through a scalar subquery: avg of g = 1 is 1.666…, so v > it is v = 2.
    assert_eq!(
        text("SELECT COUNT(*) FROM ai WHERE g = 1 AND v > (SELECT AVG(v) FROM ai WHERE g = 1)").await,
        vec!["2"]
    );
    // Through a CTE and a derived table: the value survives (at the engine's
    // nine places once it is stored in the temporary relation).
    let cte = text("WITH s AS (SELECT g, AVG(v) AS a FROM ai GROUP BY g) SELECT a FROM s WHERE g = 4").await;
    assert_eq!(cte.len(), 1);
    assert!(cte[0].starts_with("15000.5"), "{cte:?}");
    let derived = text("SELECT COUNT(*) FROM (SELECT g, AVG(v) AS a FROM ai GROUP BY g) AS sub WHERE a > 1000").await;
    assert_eq!(derived, vec!["3"]);
}
