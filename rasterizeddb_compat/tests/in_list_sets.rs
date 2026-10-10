//! `IN` / `NOT IN` lists, short and very long, on a table large enough for
//! the filter index to prune.
//!
//! An `IN` list is answered by a hashed set built from the same coerced
//! literals `col = v` uses, and its mask by OR-ed probes (short lists) or the
//! list's `[min, max]` range (long ones). Every count here is computed in the
//! test from the generating formula, never from the engine. Lists past 65,535
//! values are in scope: the old comparison program wrapped its index there.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

const N: i64 = 5000;

fn a_of(id: i64) -> Option<i64> {
    if id % 13 == 0 { None } else { Some(id % 50) }
}
fn s_of(id: i64) -> String {
    format!("k{}", id % 30)
}
fn d_res(id: i64) -> i64 {
    id % 40
}
fn f_of(id: i64) -> String {
    if id % 40 == 20 {
        "'-0'".to_string()
    } else {
        format!("{}", (id % 20) as f64 * 0.5)
    }
}

async fn count(pool: &sqlx::PgPool, predicate: &str) -> Result<i64, sqlx::Error> {
    let rows = sqlx::query(AssertSqlSafe(format!("SELECT id FROM t WHERE {predicate}")))
        .fetch_all(pool)
        .await?;
    Ok(rows.len() as i64)
}

#[tokio::test(flavor = "multi_thread")]
async fn in_lists_answer_exact_counts() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute(
        "CREATE TABLE t (id BIGINT NOT NULL, a BIGINT, s TEXT NOT NULL, d NUMERIC(10,2) NOT NULL, f DOUBLE PRECISION NOT NULL)",
    )
    .await
    .unwrap();
    for chunk in 0..(N / 500) {
        let values: Vec<String> = (0..500)
            .map(|i| {
                let id = chunk * 500 + i;
                format!(
                    "({id}, {}, '{}', {}, {})",
                    a_of(id).map(|v| v.to_string()).unwrap_or("NULL".into()),
                    s_of(id),
                    d_res(id) as f64 * 0.25,
                    f_of(id)
                )
            })
            .collect();
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "INSERT INTO t (id, a, s, d, f) VALUES {}",
            values.join(",")
        ))))
        .await
        .unwrap();
    }

    let expect = |pred: &dyn Fn(i64) -> bool| (0..N).filter(|id| pred(*id)).count() as i64;
    let in_a = |set: Vec<i64>| move |id: i64| a_of(id).is_some_and(|a| set.contains(&a));
    let not_in_a = |set: Vec<i64>| move |id: i64| a_of(id).is_some_and(|a| !set.contains(&a));

    let thousand: Vec<i64> = (0..1000).map(|i| i * 3 - 100).collect();
    let thousand_sql = thousand.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", ");
    let big: Vec<i64> = (0..70_000).map(|i| i * 7 + 3).collect();
    let big_sql = big.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");

    let cases: Vec<(String, i64)> = vec![
        ("a IN (1, 2, 3, 4, 5)".into(), expect(&in_a(vec![1, 2, 3, 4, 5]))),
        ("a IN (1, 2)".into(), expect(&in_a(vec![1, 2]))),
        ("a IN (1, NULL, 2, 3, 4)".into(), expect(&in_a(vec![1, 2, 3, 4]))),
        ("a NOT IN (1, 2, 3, 4)".into(), expect(&not_in_a(vec![1, 2, 3, 4]))),
        ("a NOT IN (1, NULL)".into(), 0),
        ("a IN (NULL)".into(), 0),
        (format!("a IN ({thousand_sql})"), expect(&in_a(thousand.clone()))),
        (format!("a NOT IN ({thousand_sql})"), expect(&not_in_a(thousand.clone()))),
        (
            "s IN ('k1', 'k2', 'k3', 'k4', 'k29', 'nope')".into(),
            expect(&|id| ["k1", "k2", "k3", "k4", "k29"].contains(&s_of(id).as_str())),
        ),
        (
            "d IN (0.25, 1.5, 2.00, 9.75, 400)".into(),
            expect(&|id| [1, 6, 8, 39].contains(&d_res(id))),
        ),
        (
            "f IN (0, 0.5, 1, 'NaN', 77)".into(),
            expect(&|id| [0, 1, 2].contains(&(id % 20))),
        ),
        (
            format!("id IN ({big_sql})"),
            expect(&|id| id >= 3 && (id - 3) % 7 == 0 && id < 70_000 * 7 + 3),
        ),
        (
            format!("id NOT IN ({big_sql})"),
            expect(&|id| !(id >= 3 && (id - 3) % 7 == 0)),
        ),
        (
            format!("(id IN ({big_sql}) OR a = 0) AND s <> 'k3'"),
            expect(&|id| {
                ((id >= 3 && (id - 3) % 7 == 0) || a_of(id) == Some(0)) && s_of(id) != "k3"
            }),
        ),
        ("id IN (10, 20, 30, 40, 4999)".into(), 5),
    ];
    let mut wrong = Vec::new();
    for (predicate, want) in &cases {
        match count(&pool, predicate).await {
            Ok(got) if got == *want => {}
            Ok(got) => wrong.push(format!("{}: {got} rows, want {want}", &predicate[..predicate.len().min(80)])),
            Err(e) => wrong.push(format!("{}: error {e}", &predicate[..predicate.len().min(80)])),
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));

    // A write through the same path reports the rows it touched.
    let want = expect(&in_a(vec![7, 8, 9, 10]));
    let done = pool
        .execute("UPDATE t SET s = 'updated' WHERE a IN (7, 8, 9, 10)")
        .await
        .unwrap();
    assert_eq!(done.rows_affected() as i64, want);
    assert_eq!(count(&pool, "s = 'updated'").await.unwrap(), want);
    let gone = pool
        .execute("DELETE FROM t WHERE a IN (7, 8, 9, 10)")
        .await
        .unwrap();
    assert_eq!(gone.rows_affected() as i64, want);
}
