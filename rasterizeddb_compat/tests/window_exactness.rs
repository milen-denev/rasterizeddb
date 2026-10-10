//! Window `SUM`/`AVG` must answer exactly what the grouped aggregate answers.
//!
//! Both bugs answered rather than failed: a window `SUM` whose total left its
//! type came back as NULL — indistinguishable from an all-NULL frame — where
//! `SUM(x)` raises `bigint out of range`; and a window `AVG` truncated where
//! `AVG(x)` rounds half away from zero, at the engine scale or the column's
//! when wider. Every case compares the window column against the aggregate
//! over the same rows, over both wire protocols.

use rasterizeddb_compat::TestServer;
use rust_decimal::Decimal;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn run(pool: &sqlx::PgPool, sql: &str) {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .unwrap_or_else(|e| panic!("{}: {}", sql, e));
}

async fn both(pool: &sqlx::PgPool, sql: &str) -> Vec<(&'static str, Result<Vec<sqlx::postgres::PgRow>, String>)> {
    vec![
        (
            "binary",
            sqlx::query(AssertSqlSafe(sql.to_string()))
                .fetch_all(pool)
                .await
                .map_err(|e| e.to_string()),
        ),
        (
            "text",
            sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
                .fetch_all(pool)
                .await
                .map_err(|e| e.to_string()),
        ),
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn an_overflowing_window_sum_is_an_error_not_a_null() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    // `sum(bigint)` is a numeric, so `n`'s total past i64 is exact (see
    // `sum_types.rs`); only `d`, past what the engine's numeric holds, overflows.
    run(&pool, "CREATE TABLE big (id INT NOT NULL, g INT NOT NULL, n BIGINT NOT NULL, d NUMERIC NOT NULL)").await;
    run(&pool, "INSERT INTO big (id, g, n, d) VALUES (1, 1, 9223372036854775807, 100000000000000000000000000000)").await;
    run(&pool, "INSERT INTO big (id, g, n, d) VALUES (2, 1, 1, 100000000000000000000000000000)").await;

    // The aggregate's wording, for the window as for the fold.
    for (sql, wording) in [
        ("SELECT SUM(d) FROM big", "numeric field overflow"),
        ("SELECT id, SUM(d) OVER () FROM big", "numeric field overflow"),
        ("SELECT id, AVG(d) OVER () FROM big", "numeric field overflow"),
    ] {
        for (protocol, got) in both(&pool, sql).await {
            match got {
                Err(e) => assert!(e.contains(wording), "{} / {}: unexpected error {}", protocol, sql, e),
                Ok(rows) => panic!("{} / {}: an overflowing sum answered {} rows", protocol, sql, rows.len()),
            }
        }
    }
}

const DECLARATIONS: &[(&str, &str)] = &[
    ("plain", "NUMERIC"),
    ("scale2", "NUMERIC(10,2)"),
    ("scale9", "NUMERIC(20,9)"),
    ("scale12", "NUMERIC(24,12)"),
];

/// The window value equals the aggregate over the same rows, at every scale.
#[tokio::test(flavor = "multi_thread")]
async fn window_sum_and_avg_agree_with_the_aggregate_at_every_scale() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for (table, declared) in DECLARATIONS {
        run(&pool, &format!("CREATE TABLE {} (id INT NOT NULL, v {} NOT NULL)", table, declared)).await;
        for (id, v) in [(1, "1.01"), (2, "2.00"), (3, "2.00")] {
            run(&pool, &format!("INSERT INTO {} (id, v) VALUES ({}, {})", table, id, v)).await;
        }
    }

    let mut wrong = Vec::new();
    for (table, declared) in DECLARATIONS {
        for func in ["SUM", "AVG", "MIN", "MAX"] {
            let aggregate = format!("SELECT {}(v) FROM {}", func, table);
            let want: Decimal = sqlx::query(AssertSqlSafe(aggregate.clone()))
                .fetch_one(&pool)
                .await
                .unwrap_or_else(|e| panic!("{}: {}", aggregate, e))
                .get(0);
            let window = format!("SELECT id, {}(v) OVER () FROM {} ORDER BY id", func, table);
            for (protocol, got) in both(&pool, &window).await {
                match got {
                    Ok(rows) => {
                        for row in rows {
                            match row.try_get::<Decimal, _>(1) {
                                Ok(v) if v == want => {}
                                Ok(v) => wrong.push(format!("{declared:14} {protocol:6} {window}: got {v}, aggregate {want}")),
                                Err(e) => wrong.push(format!("{declared:14} {protocol:6} {window}: decode {e}")),
                            }
                        }
                    }
                    Err(e) => wrong.push(format!("{declared:14} {protocol:6} {window}: error {e}")),
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}

/// `AVG(1.00, 2.00, 2.00)` is 1.666…: rounded to PostgreSQL's sixteen places,
/// 1.6666666666666667, not truncated — over every column scale.
#[tokio::test(flavor = "multi_thread")]
async fn a_window_average_rounds_like_the_aggregate() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    let mut wrong = Vec::new();
    for (table, declared, want) in [
        // PostgreSQL's sixteen places, whatever the column's scale up to it.
        ("r2", "NUMERIC(10,2)", "1.6666666666666667"),
        ("r9", "NUMERIC", "1.6666666666666667"),
        ("r12", "NUMERIC(24,12)", "1.6666666666666667"),
        ("ri", "BIGINT", "1.6666666666666667"),
    ] {
        run(&pool, &format!("CREATE TABLE {} (id INT NOT NULL, v {} NOT NULL)", table, declared)).await;
        for (id, v) in [(1, "1"), (2, "2"), (3, "2")] {
            run(&pool, &format!("INSERT INTO {} (id, v) VALUES ({}, {})", table, id, v)).await;
        }
        let want: Decimal = want.parse().unwrap();
        for sql in [
            format!("SELECT AVG(v) FROM {}", table),
            format!("SELECT AVG(v) OVER () FROM {}", table),
            format!("SELECT AVG(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM {}", table),
        ] {
            for (protocol, got) in both(&pool, &sql).await {
                match got.map(|rows| rows.into_iter().map(|r| r.try_get::<Decimal, _>(0)).collect::<Vec<_>>()) {
                    Ok(values) => {
                        for v in values {
                            match v {
                                Ok(v) if v == want => {}
                                Ok(v) => wrong.push(format!("{declared:14} {protocol:6} {sql}: got {v}")),
                                Err(e) => wrong.push(format!("{declared:14} {protocol:6} {sql}: decode {e}")),
                            }
                        }
                    }
                    Err(e) => wrong.push(format!("{declared:14} {protocol:6} {sql}: error {e}")),
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}

/// A NULL argument is skipped by a window aggregate as by a grouped one: it is
/// not a zero that counts towards an average.
#[tokio::test(flavor = "multi_thread")]
async fn a_window_aggregate_skips_nulls_like_the_aggregate() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    run(&pool, "CREATE TABLE nv (id INT NOT NULL, v BIGINT)").await;
    run(&pool, "INSERT INTO nv (id, v) VALUES (1, 3)").await;
    run(&pool, "INSERT INTO nv (id, v) VALUES (2, NULL)").await;
    run(&pool, "INSERT INTO nv (id, v) VALUES (3, 6)").await;
    fn num(row: &sqlx::postgres::PgRow) -> Result<Option<Decimal>, String> {
        row.try_get::<Option<i64>, _>(0)
            .map(|v| v.map(Decimal::from))
            .or_else(|_| row.try_get::<Option<Decimal>, _>(0))
            .map_err(|e| e.to_string())
    }
    let mut wrong = Vec::new();
    for func in ["COUNT", "SUM", "AVG", "MIN", "MAX"] {
        let aggregate = format!("SELECT {}(v) FROM nv", func);
        let want = num(&sqlx::query(AssertSqlSafe(aggregate.clone()))
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|e| panic!("{}: {}", aggregate, e)))
        .unwrap_or_else(|e| panic!("{}: {}", aggregate, e));
        let sql = format!("SELECT {}(v) OVER () FROM nv", func);
        for (protocol, got) in both(&pool, &sql).await {
            let rows = got.unwrap_or_else(|e| panic!("{}: {}", sql, e));
            for row in rows {
                match num(&row) {
                    Ok(v) if v == want => {}
                    other => wrong.push(format!("{} {}: window {:?} vs aggregate {:?}", protocol, func, other, want)),
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}

/// A window is computed over every row `WHERE` kept, before `LIMIT`/`OFFSET`:
/// a page of a windowed query must not change the window's value.
#[tokio::test(flavor = "multi_thread")]
async fn a_window_is_computed_before_limit() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    run(&pool, "CREATE TABLE lw (id INT NOT NULL, v INT NOT NULL)").await;
    for id in 1..=5 {
        run(&pool, &format!("INSERT INTO lw (id, v) VALUES ({}, {})", id, id * 10)).await;
    }
    let mut wrong = Vec::new();
    for (sql, want) in [
        ("SELECT id, SUM(v) OVER () FROM lw ORDER BY id LIMIT 2", vec![(1, 150), (2, 150)]),
        ("SELECT id, SUM(v) OVER () FROM lw ORDER BY id LIMIT 2 OFFSET 2", vec![(3, 150), (4, 150)]),
        ("SELECT id, COUNT(*) OVER () FROM lw WHERE id > 1 ORDER BY id LIMIT 1", vec![(2, 4)]),
        ("SELECT id, ROW_NUMBER() OVER (ORDER BY id DESC) FROM lw ORDER BY id LIMIT 2", vec![(1, 5), (2, 4)]),
    ] {
        for (protocol, got) in both(&pool, sql).await {
            match got {
                Ok(rows) => {
                    let got: Vec<(i32, i64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
                    if got != want {
                        wrong.push(format!("{protocol:6} {sql}: got {got:?}, want {want:?}"));
                    }
                }
                Err(e) => wrong.push(format!("{protocol:6} {sql}: error {e}")),
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}
