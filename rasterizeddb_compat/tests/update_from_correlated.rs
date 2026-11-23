//! `UPDATE … FROM` with a correlated subquery in its condition.
//!
//! The subquery used to run once per joined row, and each run saw the writes
//! the statement had already made to earlier rows. PostgreSQL evaluates the
//! whole statement against the snapshot it started with, so every row's
//! subquery sees the table as it was before the `UPDATE` — and a subquery
//! whose substituted text repeats needs running once.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn seeded(tasks: usize, groups: usize) -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute("CREATE TABLE tasks (id INTEGER PRIMARY KEY, grp INTEGER NOT NULL, done INTEGER NOT NULL)")
        .await
        .expect("create tasks");
    pool.execute("CREATE TABLE groups (grp INTEGER NOT NULL, active INTEGER NOT NULL)")
        .await
        .expect("create groups");
    let rows: Vec<String> = (0..tasks).map(|i| format!("({}, {}, 0)", i, i % groups)).collect();
    for chunk in rows.chunks(500) {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            "INSERT INTO tasks (id, grp, done) VALUES {}",
            chunk.join(", ")
        ))))
        .await
        .expect("insert tasks");
    }
    let gs: Vec<String> = (0..groups).map(|g| format!("({}, {})", g, (g % 2 == 0) as i32)).collect();
    pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
        "INSERT INTO groups (grp, active) VALUES {}",
        gs.join(", ")
    ))))
    .await
    .expect("insert groups");
    (server, pool)
}

const CLAIM: &str = "UPDATE tasks t SET done = 1 FROM groups g \
    WHERE t.grp = g.grp AND g.active = 1 \
    AND (SELECT COUNT(*) FROM tasks t2 WHERE t2.grp = t.grp AND t2.done = 1) = 0";

/// Every row is decided against the table as the statement found it: no task
/// was done, so every task of an active group is updated — not just the first
/// of each group, which is what re-reading after each write produced.
#[tokio::test(flavor = "multi_thread")]
async fn correlated_terms_see_the_statement_snapshot() {
    let (_s, pool) = seeded(40, 4).await;
    let result = pool.execute(CLAIM).await.expect("update");
    assert_eq!(result.rows_affected(), 20, "groups 0 and 2 hold 10 tasks each");
    let done: Vec<i32> = sqlx::query("SELECT id FROM tasks WHERE done = 1 ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("read")
        .iter()
        .map(|r| r.get::<i32, _>(0))
        .collect();
    let want: Vec<i32> = (0..40).filter(|i| (i % 4) % 2 == 0).collect();
    assert_eq!(done, want);

    // Run again: now every active group has a done task, so nothing passes.
    let again = pool.execute(CLAIM).await.expect("update again");
    assert_eq!(again.rows_affected(), 0);
}

/// 4 000 joined rows over 4 groups: the old loop ran 4 000 subqueries, each a
/// scan of `tasks`; the cache runs four. `cargo test --release -p
/// rasterizeddb_compat --test update_from_correlated -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn correlated_update_timing() {
    let (_s, pool) = seeded(4000, 4).await;
    let started = std::time::Instant::now();
    let result = pool.execute(CLAIM).await.expect("update");
    println!(
        "UPDATE … FROM with a correlated COUNT over 4000 rows / 4 groups: {:?} ({} rows)",
        started.elapsed(),
        result.rows_affected()
    );
}
