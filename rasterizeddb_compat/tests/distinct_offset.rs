//! `SELECT DISTINCT … OFFSET n` skips `n` *distinct* rows, as PostgreSQL does:
//! the duplicates are removed first, then the page is taken.
//!
//! The planner used to drop `OFFSET` for a `DISTINCT` select altogether (it
//! handed `None` on, alongside the limit it deliberately withholds from the
//! scan), so the clause was silently ignored and page two of a distinct list
//! was page one again.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::query("CREATE TABLE dof (id INTEGER NOT NULL, v INTEGER NOT NULL, tag TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("create");
    // v takes the values 0..10, each three times; tag pairs with v.
    for id in 0..30 {
        let v = (id * 7) % 10;
        sqlx::query("INSERT INTO dof (id, v, tag) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(v)
            .bind(format!("t{v}"))
            .execute(&pool)
            .await
            .expect("insert");
    }
    (server, pool)
}

async fn values(pool: &sqlx::PgPool, sql: &str) -> Vec<i32> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.get::<i32, _>(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn distinct_pages_over_the_distinct_rows() {
    let (_server, pool) = seeded().await;
    let all: Vec<i32> = (0..10).collect();
    assert_eq!(values(&pool, "SELECT DISTINCT v FROM dof ORDER BY v").await, all);

    for (limit, offset) in [(2u32, 1u32), (3, 0), (3, 8), (5, 10), (0, 2), (1, 9)] {
        let sql = format!("SELECT DISTINCT v FROM dof ORDER BY v LIMIT {limit} OFFSET {offset}");
        let from = (offset as usize).min(all.len());
        let to = (from + limit as usize).min(all.len());
        assert_eq!(values(&pool, &sql).await, all[from..to], "{sql}");
    }
    for offset in [1usize, 7, 10, 25] {
        let sql = format!("SELECT DISTINCT v FROM dof ORDER BY v OFFSET {offset}");
        assert_eq!(values(&pool, &sql).await, all[offset.min(all.len())..], "{sql}");
    }

    // Descending, and more than one column in the distinct row.
    let desc: Vec<i32> = (0..10).rev().collect();
    assert_eq!(
        values(&pool, "SELECT DISTINCT v, tag FROM dof ORDER BY v DESC LIMIT 3 OFFSET 2").await,
        desc[2..5]
    );
}
