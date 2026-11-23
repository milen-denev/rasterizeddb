//! A NUMERIC column compared with a written number is exact, whatever the
//! column's scale and however many places the number has.
//!
//! The two met at the engine's nine places. A literal with more places than
//! the column was lifted there with it and refused ("a literal in NUMERIC
//! arithmetic is not a number", XX000) when it had ten or more; and a long
//! fraction was read as a float first, so `a = 0.12345678912300000001`
//! matched a NUMERIC(20,12) row holding 0.123456789123, and `a =
//! 99999999.999999999998` found nothing although that value is stored.
//! Every answer here is checked against an exact decimal comparison, on a
//! small table and on one large enough for the filter index to prune.

use std::cmp::Ordering;

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

/// A decimal's text as (negative, integer digits, fraction digits), with
/// leading zeros of the integer and trailing zeros of the fraction removed.
fn parts(text: &str) -> (bool, String, String) {
    let text = text.trim().trim_matches('\'');
    let (negative, body) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    let int = int.trim_start_matches('0').to_string();
    let frac = frac.trim_end_matches('0').to_string();
    let zero = int.is_empty() && frac.is_empty();
    (negative && !zero, int, frac)
}

fn compare(a: &str, b: &str) -> Ordering {
    let (an, ai, af) = parts(a);
    let (bn, bi, bf) = parts(b);
    let magnitude = ai.len().cmp(&bi.len()).then_with(|| ai.cmp(&bi)).then_with(|| {
        let width = af.len().max(bf.len());
        format!("{af:0<width$}").cmp(&format!("{bf:0<width$}"))
    });
    match (an, bn) {
        (false, false) => magnitude,
        (true, true) => magnitude.reverse(),
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
    }
}

/// `value` rounded to `scale` places, half away from zero, as an INSERT
/// stores it. Only what the cases here need: no carry past the integer part.
fn stored(value: &str, scale: Option<usize>) -> String {
    let Some(scale) = scale else { return value.to_string() };
    let (negative, int, frac) = parts(value);
    let frac = format!("{frac:0<scale$}");
    assert!(frac.len() == scale, "the cases store exactly: {value}");
    format!("{}{}{}{}", if negative { "-" } else { "" }, if int.is_empty() { "0" } else { &int }, if scale > 0 { "." } else { "" }, frac)
}

const TABLES: &[(&str, &str, Option<usize>, &[&str])] = &[
    ("q2", "NUMERIC(10,2)", Some(2), &["1.25", "2.50", "-3.75", "99999999.99", "0.01"]),
    ("q12", "NUMERIC(20,12)", Some(12), &["0.123456789123", "99999999.999999999998", "-0.000000000001", "1.5", "0.000000000005"]),
    ("q0", "NUMERIC(38,0)", Some(0), &["12345678901234567890123456789012345678", "5", "-7", "0", "100"]),
    ("qu", "NUMERIC", None, &["0.123456789", "1.5", "-2.25", "123456789.987654321", "0.000000001"]),
];

const LITERALS: &[&str] = &[
    "1", "1.5", "0.12", "0.123456789", "0.1234567891", "0.123456789123", "0.1234567891229", "0.1234567891231",
    "0.12345678912300000001", "99999999.999999999998", "99999999.99", "1.25", "1.251", "1.249", "-3.75", "-3.751",
    "-0.0000000000005", "0.000000000005", "0.00000000000500000000", "5.0000000001", "123456789.987654321",
    "0.0000000009", "100.00000000000000000001",
    // Past what an i128 holds (48 digits): exact still, never a float.
    "0.123456789123000000000000000000000000000000000001", "-0.123456789123000000000000000000000000000000000001",
];

async fn ids(pool: &sqlx::PgPool, sql: &str) -> Result<Vec<i32>, String> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .map(|rows| rows.iter().map(|r| r.get::<i32, _>(0)).collect())
        .map_err(|e| e.to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_numeric_column_compares_exactly_with_a_long_literal() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let mut wrong = Vec::new();
    for (table, declared, scale, values) in TABLES {
        let rows: Vec<String> = values.iter().enumerate().map(|(i, v)| format!("({i}, {v})")).collect();
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "CREATE TABLE {table} (id INT PRIMARY KEY, a {declared}); INSERT INTO {table} VALUES {}, ({}, NULL)",
            rows.join(", "),
            values.len()
        ))))
        .await
        .expect("create");
        let kept: Vec<String> = values.iter().map(|v| stored(v, *scale)).collect();
        let want = |keep: &dyn Fn(&str) -> bool| -> Vec<i32> {
            kept.iter().enumerate().filter(|(_, v)| keep(v)).map(|(i, _)| i as i32).collect()
        };
        for lit in LITERALS {
            let cases: [(String, Vec<i32>); 9] = [
                (format!("a = {lit}"), want(&|v| compare(v, lit).is_eq())),
                (format!("a <> {lit}"), want(&|v| compare(v, lit).is_ne())),
                (format!("a < {lit}"), want(&|v| compare(v, lit).is_lt())),
                (format!("a <= {lit}"), want(&|v| compare(v, lit).is_le())),
                (format!("a > {lit}"), want(&|v| compare(v, lit).is_gt())),
                (format!("a >= {lit}"), want(&|v| compare(v, lit).is_ge())),
                // The literal first, and quoted.
                (format!("{lit} > a"), want(&|v| compare(v, lit).is_lt())),
                (format!("{lit} <= a"), want(&|v| compare(v, lit).is_ge())),
                (format!("a = '{lit}'"), want(&|v| compare(v, lit).is_eq())),
            ];
            for (predicate, expected) in cases {
                let sql = format!("SELECT id FROM {table} WHERE {predicate} ORDER BY id");
                let got = ids(&pool, &sql).await;
                if got.as_ref() != Ok(&expected) {
                    wrong.push(format!("{declared}: {predicate}: got {got:?}, want {expected:?}"));
                }
            }
        }
        let lo = "0.1234567891229";
        let hi = "1.251";
        for (predicate, expected) in [
            (format!("a BETWEEN {lo} AND {hi}"), want(&|v| compare(v, lo).is_ge() && compare(v, hi).is_le())),
            ("a BETWEEN -3.751 AND 0.0000000009".to_string(), want(&|v| compare(v, "-3.751").is_ge() && compare(v, "0.0000000009").is_le())),
            (
                "a IN (0.123456789123, 1.5, 0.12345678912300000001, 1.25)".to_string(),
                want(&|v| ["0.123456789123", "1.5", "0.12345678912300000001", "1.25"].iter().any(|l| compare(v, l).is_eq())),
            ),
            (
                "a NOT IN (0.123456789123, 1.5)".to_string(),
                want(&|v| ["0.123456789123", "1.5"].iter().all(|l| compare(v, l).is_ne())),
            ),
        ] {
            let sql = format!("SELECT id FROM {table} WHERE {predicate} ORDER BY id");
            let got = ids(&pool, &sql).await;
            if got.as_ref() != Ok(&expected) {
                wrong.push(format!("{declared}: {predicate}: got {got:?}, want {expected:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// The same bounds on a table the filter index prunes: a literal between
/// two stored units is compared at the column's own scale, so the index
/// can still be used, and must not drop a matching row.
#[tokio::test(flavor = "multi_thread")]
async fn the_filter_index_prunes_a_long_literal_without_losing_rows() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute("CREATE TABLE big (id INT NOT NULL, v NUMERIC(20,12) NOT NULL)").await.expect("create");
    // v = id % 300 + (id % 7) units of 10^-12.
    let value = |id: i64| format!("{}.{:012}", id % 300, id % 7);
    for chunk in 0..10 {
        let rows: Vec<String> = (0..500).map(|i| chunk * 500 + i).map(|id| format!("({id}, {})", value(id))).collect();
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!("INSERT INTO big VALUES {}", rows.join(",")))))
            .await
            .expect("insert");
    }
    let mut wrong = Vec::new();
    for (predicate, keep) in [
        ("v > 50.0000000000035", Box::new(|v: &str| compare(v, "50.0000000000035").is_gt()) as Box<dyn Fn(&str) -> bool>),
        ("v < 7.0000000000025", Box::new(|v: &str| compare(v, "7.0000000000025").is_lt())),
        ("v = 100.0000000000030000001", Box::new(|v: &str| compare(v, "100.0000000000030000001").is_eq())),
        ("v = 100.000000000003", Box::new(|v: &str| compare(v, "100.000000000003").is_eq())),
        ("v BETWEEN 100.0000000000005 AND 100.0000000000045", Box::new(|v: &str| {
            compare(v, "100.0000000000005").is_ge() && compare(v, "100.0000000000045").is_le()
        })),
    ] {
        let want = (0..5000i64).filter(|id| keep(&value(*id))).count();
        match sqlx::query(AssertSqlSafe(format!("SELECT id FROM big WHERE {predicate}"))).fetch_all(&pool).await {
            Ok(rows) if rows.len() == want => {}
            Ok(rows) => wrong.push(format!("{predicate}: {} rows, want {want}", rows.len())),
            Err(e) => wrong.push(format!("{predicate}: {e}")),
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
