//! `ORDER BY col LIMIT k` answered from the leading slots when the zone map
//! says the column ascends with slot position.
//!
//! The zone bounds never recorded a NaN, so a float column holding NaN early
//! in the table still looked monotonic — and the NaN, which PostgreSQL sorts
//! after every number, came back among the smallest values. Expected rows are
//! computed from the data in the test.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

const N: i64 = 5000;

async fn load(pool: &sqlx::PgPool, table: &str, value: impl Fn(i64) -> String) {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
        "CREATE TABLE {table} (id BIGINT NOT NULL, f DOUBLE PRECISION NOT NULL)"
    ))))
    .await
    .unwrap();
    for chunk in 0..(N / 500) {
        let values: Vec<String> = (0..500)
            .map(|i| {
                let id = chunk * 500 + i;
                format!("({id}, {})", value(id))
            })
            .collect();
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "INSERT INTO {table} (id, f) VALUES {}",
            values.join(",")
        ))))
        .await
        .unwrap();
    }
}

async fn ids(pool: &sqlx::PgPool, sql: &str) -> Vec<i64> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.get::<i64, _>(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_nan_never_sorts_among_the_smallest() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    // Ascending with slot position, except NaN at ids 2 and 700.
    let nan = |id: i64| id == 2 || id == 700;
    load(&pool, "fm", |id| if nan(id) { "'NaN'".into() } else { format!("{id}") }).await;
    // Run twice: the second is answered after the zone map has seen every row.
    for _ in 0..2 {
        assert_eq!(ids(&pool, "SELECT id FROM fm ORDER BY f LIMIT 3").await, vec![0, 1, 3]);
        assert_eq!(ids(&pool, "SELECT id FROM fm ORDER BY f ASC LIMIT 1").await, vec![0]);
        // DESC puts NaN first, as PostgreSQL does.
        let mut top = ids(&pool, "SELECT id FROM fm ORDER BY f DESC LIMIT 3").await;
        top[..2].sort();
        assert_eq!(top, vec![2, 700, N - 1]);
        assert_eq!(
            ids(&pool, "SELECT id FROM fm ORDER BY f LIMIT 2 OFFSET 1").await,
            vec![1, 3]
        );
    }
}
