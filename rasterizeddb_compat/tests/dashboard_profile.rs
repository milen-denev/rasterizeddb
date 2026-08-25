//! Where the time goes on a dashboard page load.
//!
//! Run explicitly:
//!   cargo test -p rasterizeddb_compat --test dashboard_profile -- --ignored --nocapture

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::TestServer;
use sqlx::Executor;
use std::time::Instant;

async fn timed(pool: &sqlx::PgPool, label: &str, sql: &str, iters: u32) -> f64 {
    // Warm once so parse/plan caches are not charged to the first iteration.
    let _ = pool.fetch_all(sqlx::raw_sql(AssertSqlSafe(sql))).await;
    let start = Instant::now();
    for _ in 0..iters {
        pool.fetch_all(sqlx::raw_sql(AssertSqlSafe(sql)))
            .await
            .unwrap_or_else(|e| panic!("{} failed: {}", label, e));
    }
    let per = start.elapsed().as_secs_f64() * 1000.0 / iters as f64;
    println!("{:>9.3} ms  {}", per, label);
    per
}

#[ignore = "profiling harness, run explicitly"]
#[tokio::test]
async fn dashboard_query_shapes() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");

    pool.execute(sqlx::raw_sql(
        "CREATE TABLE domain (id INT PRIMARY KEY, name TEXT NOT NULL)",
    ))
    .await
    .expect("create domain");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE case_model (id INT PRIMARY KEY, domain_id INT NOT NULL, name TEXT NOT NULL)",
    ))
    .await
    .expect("create case_model");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE kase (id INT PRIMARY KEY, case_model_id INT NOT NULL, state TEXT NOT NULL, title TEXT NOT NULL)",
    ))
    .await
    .expect("create kase");

    pool.execute(sqlx::raw_sql("INSERT INTO domain (id, name) VALUES (1, 'd')"))
        .await
        .expect("seed domain");
    for i in 0..50i32 {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "INSERT INTO case_model (id, domain_id, name) VALUES ({}, 1, 'm{}')",
            i, i
        ))))
        .await
        .expect("seed case_model");
    }

    timed(&pool, "BASELINE: SELECT 1 (protocol + statement overhead)", "SELECT 1", 100).await;

    for rows in [200i32, 2000] {
        pool.execute(sqlx::raw_sql("DROP TABLE kase")).await.ok();
        pool.execute(sqlx::raw_sql(
            "CREATE TABLE kase (id INT PRIMARY KEY, case_model_id INT NOT NULL, state TEXT NOT NULL, title TEXT NOT NULL)",
        )).await.expect("recreate kase");
        let seed = Instant::now();
        for i in 0..rows {
            pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
                "INSERT INTO kase (id, case_model_id, state, title) VALUES ({}, {}, '{}', 't{}')",
                i,
                i % 50,
                if i % 3 == 0 { "active" } else { "done" },
                i
            ))))
            .await
            .expect("seed kase");
        }
        println!(
            "\n=== {} rows in kase (seeded in {:.1} ms, {:.3} ms/insert) ===",
            rows,
            seed.elapsed().as_secs_f64() * 1000.0,
            seed.elapsed().as_secs_f64() * 1000.0 / rows as f64
        );

        timed(&pool, "point lookup: domain WHERE id = 1 LIMIT 1", "SELECT id, name FROM domain WHERE id = 1 LIMIT 1", 50).await;
        timed(&pool, "COUNT(*) single table: kase", "SELECT COUNT(*) FROM kase", 20).await;
        timed(&pool, "COUNT(*) filtered: kase WHERE state", "SELECT COUNT(*) FROM kase WHERE state = 'active'", 20).await;
        timed(&pool, "COUNT(*) join: kase x case_model", "SELECT COUNT(*) FROM kase k JOIN case_model m ON k.case_model_id = m.id", 20).await;
        timed(&pool, "join ORDER BY LIMIT 5", "SELECT k.id, k.title, k.state, m.name FROM kase k JOIN case_model m ON k.case_model_id = m.id ORDER BY k.id DESC LIMIT 5", 20).await;
        timed(&pool, "full rows: kase (all)", "SELECT id, case_model_id, state, title FROM kase", 10).await;
        timed(&pool, "COUNT(*) on the 1-row domain table (pure setup cost)", "SELECT COUNT(*) FROM domain", 50).await;
        timed(&pool, "PK lookup, FIRST row, LIMIT 1", "SELECT id, title FROM kase WHERE id = 0 LIMIT 1", 50).await;
        timed(&pool, "PK lookup, LAST row, LIMIT 1", &format!("SELECT id, title FROM kase WHERE id = {} LIMIT 1", rows - 1), 50).await;
    }

    // A dashboard does not issue its cards one at a time.
    for concurrency in [16usize, 60] {
        let start = Instant::now();
        let mut handles = Vec::new();
        for i in 0..concurrency {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                let sql = match i % 4 {
                    0 => "SELECT COUNT(*) FROM kase".to_string(),
                    1 => "SELECT COUNT(*) FROM kase WHERE state = 'active'".to_string(),
                    2 => "SELECT id, name FROM domain WHERE id = 1 LIMIT 1".to_string(),
                    _ => "SELECT COUNT(*) FROM case_model".to_string(),
                };
                pool.fetch_all(sqlx::raw_sql(AssertSqlSafe(&*sql))).await.expect("card query");
            }));
        }
        for h in handles {
            h.await.expect("card task");
        }
        println!(
            "{:>9.3} ms  WALL for {} concurrent card queries",
            start.elapsed().as_secs_f64() * 1000.0,
            concurrency
        );
    }

    // The seeding write on the read path.
    let w = Instant::now();
    for i in 10000..10020i32 {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "INSERT INTO kase (id, case_model_id, state, title) VALUES ({}, 1, 'x', 't')",
            i
        ))))
        .await
        .expect("write");
    }
    println!(
        "\n{:>9.3} ms  single INSERT (the ensure_*_seeded write on a read path)",
        w.elapsed().as_secs_f64() * 1000.0 / 20.0
    );
}
