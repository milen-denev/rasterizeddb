//! The query cache, switched on, answering from cached pointer sets — and its
//! byte budget, which keeps a large match set out rather than letting the cache
//! grow without bound (one 5M-row match was 240 MB).

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::cache::query_cache;
use sqlx::{AssertSqlSafe, Executor, Row};

fn cache_on() {
    let _ = query_cache::ENABLE_CACHE.set(true);
    query_cache::init(10_000);
}

async fn ids(pool: &sqlx::PgPool, sql: &str) -> Vec<i32> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e))
        .iter()
        .map(|r| r.get::<i32, _>(0))
        .collect()
}

/// One test, because the budget and the counters are process-wide.
#[tokio::test(flavor = "multi_thread")]
async fn cached_answers_are_right_and_the_budget_bounds_them() {
    cache_on();
    assert!(query_cache::enabled());
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute("CREATE TABLE c (id INTEGER NOT NULL, grp INTEGER NOT NULL)")
        .await
        .expect("create");
    let values: Vec<String> = (0..2000).map(|i| format!("({}, {})", i, i % 4)).collect();
    pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
        "INSERT INTO c (id, grp) VALUES {}",
        values.join(", ")
    ))))
    .await
    .expect("insert");

    let want: Vec<i32> = (0..2000).filter(|i| i % 4 == 1).collect();
    let sql = "SELECT id FROM c WHERE grp = 1 ORDER BY id";
    let (_, _, stores_before) = query_cache::stats();
    let first = ids(&pool, sql).await;
    let (hits_before, _, stores_after) = query_cache::stats();
    assert_eq!(first, want);
    assert!(stores_after > stores_before, "the match set was cached");
    let bytes_cached = query_cache::cached_bytes();
    assert!(bytes_cached > 0);

    // Served from the cache, with the same answer.
    assert_eq!(ids(&pool, sql).await, want);
    let (hits_after, _, _) = query_cache::stats();
    assert!(hits_after > hits_before, "the second read was a hit");

    // A write strands the entry; the next read is right, and frees it.
    pool.execute("UPDATE c SET grp = 9 WHERE id = 1").await.expect("update");
    let after_write: Vec<i32> = want.iter().copied().filter(|&i| i != 1).collect();
    assert_eq!(ids(&pool, sql).await, after_write);
    assert_eq!(
        ids(&pool, "SELECT id FROM c WHERE grp = 1 ORDER BY id LIMIT 3").await,
        after_write[..3].to_vec()
    );

    // A budget too small for this match set: it is no longer stored, and the
    // answer is unchanged.
    query_cache::clear();
    assert_eq!(query_cache::cached_bytes(), 0, "clear frees every entry");
    query_cache::set_byte_budget(4096);
    let (_, _, stores_before) = query_cache::stats();
    assert_eq!(ids(&pool, "SELECT id FROM c WHERE grp = 2 ORDER BY id").await.len(), 500);
    let (_, _, stores_after) = query_cache::stats();
    assert_eq!(stores_after, stores_before, "an entry above budget/8 is not stored");
    assert_eq!(query_cache::cached_bytes(), 0);
    query_cache::set_byte_budget(0);
}
