//! A statement with a subquery reads both halves as of one moment.
//!
//! Every id below lives in exactly one of `a` and `b`. One transaction at a
//! time moves an id from `b` to `a`. So no snapshot sees an id in both, and
//! `a WHERE id IN (SELECT id FROM b)` must always be empty. If the subquery
//! is read under one snapshot and the outer table under a later one, a move
//! committing in between shows the id in both.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};
use std::time::{Duration, Instant};

const IDS: i64 = 300;

async fn setup(pool: &sqlx::PgPool) {
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE a (id BIGINT PRIMARY KEY);
         CREATE TABLE b (id BIGINT PRIMARY KEY);",
    ))
    .await
    .expect("schema");
    let ids: Vec<String> = (0..IDS).map(|i| format!("({i})")).collect();
    pool.execute(AssertSqlSafe(format!("INSERT INTO b VALUES {}", ids.join(", "))))
        .await
        .expect("ids");
    // Filler outside the subquery's range, so the inner scan takes a while.
    for batch in 0..40i64 {
        let values: Vec<String> = (0..500i64).map(|i| format!("({})", 100_000 + batch * 500 + i)).collect();
        pool.execute(AssertSqlSafe(format!("INSERT INTO b VALUES {}", values.join(", "))))
            .await
            .expect("filler");
    }
}

fn mover(pool: &sqlx::PgPool) -> tokio::task::JoinHandle<()> {
    let pool = pool.clone();
    tokio::spawn(async move {
        for id in 0..IDS {
            let mut tx = pool.begin().await.expect("begin");
            sqlx::query("DELETE FROM b WHERE id = $1").bind(id).execute(&mut *tx).await.expect("take");
            sqlx::query("INSERT INTO a (id) VALUES ($1)").bind(id).execute(&mut *tx).await.expect("put");
            tx.commit().await.expect("commit");
        }
    })
}

async fn never_both(sql: &'static str) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    setup(&pool).await;
    let mover = mover(&pool);

    let mut seen_in_both: Vec<i64> = Vec::new();
    let mut reads = 0u64;
    let deadline = Instant::now() + Duration::from_secs(120);
    while !mover.is_finished() {
        let rows = sqlx::query(sql).fetch_all(&pool).await.expect(sql);
        seen_in_both.extend(rows.iter().map(|r| r.get::<i64, _>(0)));
        reads += 1;
        assert!(Instant::now() < deadline, "the mover did not finish in time");
    }
    mover.await.expect("mover");
    assert!(
        seen_in_both.is_empty(),
        "{sql}: {} ids seen in both tables over {} reads, e.g. {:?}",
        seen_in_both.len(),
        reads,
        &seen_in_both[..seen_in_both.len().min(5)]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn in_subquery_reads_one_snapshot() {
    never_both("SELECT id FROM a WHERE id IN (SELECT id FROM b WHERE id < 100000)").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn exists_reads_one_snapshot() {
    never_both("SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a.id AND b.id < 100000)").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cte_reads_one_snapshot() {
    never_both("WITH moved AS (SELECT id FROM b WHERE id < 100000) SELECT a.id FROM a JOIN moved ON moved.id = a.id").await;
}
