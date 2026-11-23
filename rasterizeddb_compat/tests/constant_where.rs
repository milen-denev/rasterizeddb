//! A `WHERE` clause no row can satisfy is answered without a scan, and one
//! every row satisfies still returns every row.
//!
//! `notnull IS NULL`, `x = NULL`, `id != id`, an EXISTS over nothing and a
//! `NOT IN` list holding a NULL all lower to a constant. They used to scan the
//! whole table to return nothing — and `f != f` on a float column was not even
//! constant: under IEEE it was true for NaN, so it returned the NaN rows.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

const N: i64 = 5000;

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute("CREATE TABLE c (id BIGINT NOT NULL, f DOUBLE PRECISION NOT NULL, n BIGINT)")
        .await
        .unwrap();
    for chunk in 0..(N / 500) {
        let values: Vec<String> = (0..500)
            .map(|i| {
                let id = chunk * 500 + i;
                let f = if id % 100 == 0 { "'NaN'".to_string() } else { format!("{}", id % 7) };
                let n = if id % 3 == 0 { "NULL".to_string() } else { (id % 11).to_string() };
                format!("({id}, {f}, {n})")
            })
            .collect();
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "INSERT INTO c (id, f, n) VALUES {}",
            values.join(",")
        ))))
        .await
        .unwrap();
    }
    (server, pool)
}

#[tokio::test(flavor = "multi_thread")]
async fn constant_predicates_answer_without_reading_rows() {
    let (_s, pool) = seeded().await;
    let mut wrong = Vec::new();
    for (predicate, want) in [
        ("id IS NULL", 0),
        ("f IS NULL", 0),
        ("id != id", 0),
        ("f != f", 0),
        ("f < f", 0),
        ("n = NULL", 0),
        ("n NOT IN (1, NULL)", 0),
        ("id = 5 AND 1 = 0", 0),
        ("n != n", 0),
        ("EXISTS (SELECT 1 FROM c WHERE 1 = 0)", 0),
        // Constant true is every row; a nullable column against itself is not
        // constant (NULL rows drop out).
        ("id IS NOT NULL", N),
        ("id = id", N),
        ("f = f", N),
        ("1 = 1", N),
        ("n = n", (0..N).filter(|id| id % 3 != 0).count() as i64),
        ("id = 5 OR 1 = 0", 1),
    ] {
        let sql = format!("SELECT id, f FROM c WHERE {predicate}");
        match sqlx::query(AssertSqlSafe(sql)).fetch_all(&pool).await {
            Ok(rows) if rows.len() as i64 == want => {}
            Ok(rows) => wrong.push(format!("{predicate}: {} rows, want {want}", rows.len())),
            Err(e) => wrong.push(format!("{predicate}: error {e}")),
        }
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));

    // An empty answer still describes its columns.
    let described = (&pool)
        .prepare(sqlx::SqlSafeStr::into_sql_str(AssertSqlSafe("SELECT id, f FROM c WHERE id IS NULL")))
        .await
        .unwrap();
    let names: Vec<&str> = sqlx::Statement::columns(&described)
        .iter()
        .map(|c| sqlx::Column::name(c))
        .collect();
    assert_eq!(names, vec!["id", "f"]);

    let count: i64 = sqlx::query("SELECT COUNT(*) FROM c WHERE id IS NULL")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    let count: i64 = sqlx::query("SELECT COUNT(*) FROM c WHERE id = id")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, N);
}

#[tokio::test(flavor = "multi_thread")]
async fn constant_predicates_in_writes_report_what_they_touched() {
    let (_s, pool) = seeded().await;
    for sql in [
        "UPDATE c SET n = 1 WHERE id IS NULL",
        "UPDATE c SET n = 1 WHERE id != id",
        "DELETE FROM c WHERE f != f",
        "DELETE FROM c WHERE id IS NULL",
    ] {
        let done = pool.execute(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(done.rows_affected(), 0, "{sql}");
    }
    let done = pool.execute("UPDATE c SET n = 2 WHERE id = id").await.unwrap();
    assert_eq!(done.rows_affected() as i64, N);
    let rows = sqlx::query("SELECT id FROM c WHERE n = 2").fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len() as i64, N);
    let done = pool.execute("DELETE FROM c WHERE 1 = 1").await.unwrap();
    assert_eq!(done.rows_affected() as i64, N);
}

/// Wall time of a constant-false scan against an ordinary full scan on the
/// same table. Debug build, so the absolute numbers are not production's; the
/// ratio is the point. `cargo test -p rasterizeddb_compat --test constant_where
/// -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn constant_false_scan_cost() {
    let (_s, pool) = seeded().await;
    for round in 0..3 {
        for predicate in ["id IS NULL", "id + 0 = -1", "id != id"] {
            let start = std::time::Instant::now();
            for _ in 0..20 {
                sqlx::query(AssertSqlSafe(format!("SELECT id FROM c WHERE {predicate}")))
                    .fetch_all(&pool)
                    .await
                    .unwrap();
            }
            println!("round {round} {predicate:12}: {:?} per query", start.elapsed() / 20);
        }
    }
}
