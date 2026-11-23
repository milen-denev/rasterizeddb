//! Float predicates follow PostgreSQL, not IEEE.
//!
//! PostgreSQL orders floats totally: NaN equals NaN and is larger than every
//! other value, including +Infinity, and -0 equals +0. The scan compared with
//! IEEE `==`/`<`, so `f = 'NaN'` matched nothing and `f > 1e308` never returned
//! a NaN row. And `IS NULL` on a NOT NULL float was rewritten to `f != f` —
//! *true* for NaN under IEEE — so NaN rows came back as NULL and `IS NOT NULL`
//! dropped them.
//!
//! The big-table half exists because the filter index only prunes on a table
//! large enough to have zones, and its bounds never record a NaN.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn ids(pool: &sqlx::PgPool, sql: &str) -> Result<Vec<i32>, sqlx::Error> {
    let mut out: Vec<i32> = sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await?
        .iter()
        .map(|r| r.get::<i32, _>(0))
        .collect();
    out.sort();
    Ok(out)
}

#[tokio::test(flavor = "multi_thread")]
async fn nan_zero_and_infinity_compare_as_postgres_does() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for ty in ["DOUBLE PRECISION", "REAL"] {
        let t = if ty == "REAL" { "f4" } else { "f8" };
        for sql in [
            format!("CREATE TABLE {t} (id INT NOT NULL, f {ty} NOT NULL)"),
            format!("CREATE TABLE {t}n (id INT NOT NULL, f {ty})"),
        ] {
            pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.clone())))
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        for table in [t.to_string(), format!("{t}n")] {
            for (id, v) in [
                (1, "'NaN'"),
                (2, "'-0'"),
                (3, "0"),
                (4, "'Infinity'"),
                (5, "'-Infinity'"),
                (6, "1.5"),
            ] {
                let sql = format!("INSERT INTO {table} (id, f) VALUES ({id}, {v})");
                pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.clone())))
                    .await
                    .unwrap_or_else(|e| panic!("{sql}: {e}"));
            }
        }
        let sql = format!("INSERT INTO {t}n (id, f) VALUES (7, NULL)");
        pool.execute(sqlx::raw_sql(AssertSqlSafe(sql))).await.unwrap();

        let mut wrong = Vec::new();
        for (table, predicate, expected) in [
            (t.to_string(), "f = 'NaN'", vec![1]),
            (t.to_string(), "f <> 'NaN'", vec![2, 3, 4, 5, 6]),
            (t.to_string(), "f > 100000", vec![1, 4]),
            (t.to_string(), "f = '1.5'", vec![6]),
            (t.to_string(), "f > 'Infinity'", vec![1]),
            (t.to_string(), "f >= 'Infinity'", vec![1, 4]),
            (t.to_string(), "f < 'NaN'", vec![2, 3, 4, 5, 6]),
            (t.to_string(), "f <= 'NaN'", vec![1, 2, 3, 4, 5, 6]),
            (t.to_string(), "f = 0", vec![2, 3]),
            (t.to_string(), "f = '-0'", vec![2, 3]),
            (t.to_string(), "f < 0", vec![5]),
            (t.to_string(), "f <= 0", vec![2, 3, 5]),
            (t.to_string(), "f > 0", vec![1, 4, 6]),
            (t.to_string(), "f IS NULL", vec![]),
            (t.to_string(), "f IS NOT NULL", vec![1, 2, 3, 4, 5, 6]),
            (t.to_string(), "f IS DISTINCT FROM NULL", vec![1, 2, 3, 4, 5, 6]),
            (t.to_string(), "f IS NOT DISTINCT FROM NULL", vec![]),
            (t.to_string(), "f IN (0, 'NaN')", vec![1, 2, 3]),
            (t.to_string(), "f NOT IN (0, 1.5)", vec![1, 4, 5]),
            (format!("{t}n"), "f IS NULL", vec![7]),
            (format!("{t}n"), "f IS NOT NULL", vec![1, 2, 3, 4, 5, 6]),
            (format!("{t}n"), "f = 'NaN'", vec![1]),
            (format!("{t}n"), "f > 1", vec![1, 4, 6]),
            (format!("{t}n"), "f <> 0", vec![1, 4, 5, 6]),
            (format!("{t}n"), "f NOT IN (0)", vec![1, 4, 5, 6]),
        ] {
            let sql = format!("SELECT id FROM {table} WHERE {predicate}");
            match ids(&pool, &sql).await {
                Ok(got) if got == expected => {}
                Ok(got) => wrong.push(format!("{ty} {table}: {predicate}: got {got:?}, want {expected:?}")),
                Err(e) => wrong.push(format!("{ty} {table}: {predicate}: error {e}")),
            }
        }
        assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
    }
}

/// On a table big enough for the filter index to prune, a NaN row must still
/// be returned by every predicate it satisfies. Each zone's ordinary values are
/// narrow (`id / 512`), so a zone's bounds sit far below the probe while its
/// NaN rows — which the bounds never record — satisfy `f > x`.
#[tokio::test(flavor = "multi_thread")]
async fn the_filter_index_never_prunes_a_nan_row() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute("CREATE TABLE bigf (id INT NOT NULL, f DOUBLE PRECISION NOT NULL)")
        .await
        .unwrap();
    const N: i32 = 6000;
    let is_nan = |id: i32| id % 777 == 5;
    let value = |id: i32| (id / 512) as f64;
    for chunk in 0..(N / 500) {
        let values: Vec<String> = (0..500)
            .map(|i| {
                let id = chunk * 500 + i;
                if is_nan(id) {
                    format!("({id}, 'NaN')")
                } else {
                    format!("({id}, {})", value(id))
                }
            })
            .collect();
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "INSERT INTO bigf (id, f) VALUES {}",
            values.join(",")
        ))))
        .await
        .unwrap();
    }
    let count = |pred: &dyn Fn(i32) -> bool| (0..N).filter(|id| pred(*id)).count();
    let mut wrong = Vec::new();
    for (predicate, want) in [
        ("f = 'NaN'", count(&|id| is_nan(id))),
        ("f > 8.5", count(&|id| is_nan(id) || value(id) > 8.5)),
        ("f >= 9", count(&|id| is_nan(id) || value(id) >= 9.0)),
        ("8.5 < f", count(&|id| is_nan(id) || value(id) > 8.5)),
        ("f = 3", count(&|id| !is_nan(id) && value(id) == 3.0)),
        ("f < 2", count(&|id| !is_nan(id) && value(id) < 2.0)),
        ("f <= 'NaN'", N as usize),
        ("f IS NOT NULL", N as usize),
        ("f = 3 OR f > 10", count(&|id| is_nan(id) || value(id) == 3.0 || value(id) > 10.0)),
    ] {
        let sql = format!("SELECT id FROM bigf WHERE {predicate}");
        match sqlx::query(AssertSqlSafe(sql)).fetch_all(&pool).await {
            Ok(rows) if rows.len() == want => {}
            Ok(rows) => wrong.push(format!("{predicate}: {} rows, want {want}", rows.len())),
            Err(e) => wrong.push(format!("{predicate}: error {e}")),
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}
