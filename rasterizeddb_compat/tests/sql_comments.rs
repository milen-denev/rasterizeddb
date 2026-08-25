//! Comments are whitespace, and every parser must see them that way.
//!
//! The cold-row retention worker carried a comment block explaining why its
//! statement had been rewritten, and one line of that prose quoted
//! `IN (SELECT ...)`. The subquery extractor found that inside the comment
//! and tried to execute `SELECT ...` — the engine reported "cannot parse
//! subquery 'SELECT ...'", the ellipsis being literal English from the
//! comment. The worker failed every cycle.

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql)))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

async fn count(pool: &sqlx::PgPool, sql: &str) -> i64 {
    pool.fetch_one(sqlx::raw_sql(AssertSqlSafe(sql)))
        .await
        .expect("count")
        .try_get::<i64, _>(0)
        .expect("n")
}

#[tokio::test]
async fn a_comment_quoting_sql_is_not_executed() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE snap (snapshot_id INT PRIMARY KEY, keep BOOL NOT NULL)")
        .await
        .expect("create");
    for (id, keep) in [(1, "true"), (2, "false"), (3, "false")] {
        run(&pool, &format!("INSERT INTO snap (snapshot_id, keep) VALUES ({}, {})", id, keep))
            .await
            .expect("insert");
    }

    // The production shape: a comment block, one line of which quotes SQL,
    // above the statement that actually runs.
    let sql = r#"
                        -- `DELETE ... USING victims v WHERE s.snapshot_id = v.snapshot_id`
                        -- until 2026-08-22. RasterizedDB accepted `USING` and silently
                        -- IGNORED it, so this became an unqualified `DELETE FROM snap`.
                        --
                        -- The `IN (SELECT ...)` rewrite is exactly equivalent -- `victims`
                        -- yields one bounded set of snapshot_ids either way -- and is
                        -- supported on both backends.
                        DELETE FROM snap
                        WHERE snapshot_id IN (SELECT snapshot_id FROM snap WHERE keep = false)
"#;
    run(&pool, sql).await.expect("a commented statement must run");
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM snap").await, 1, "only the kept row remains");
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM snap WHERE snapshot_id = 1").await,
        1,
        "and it is the right one — the comment's prose was not executed"
    );

    // Block comments too, and a comment that quotes a whole statement.
    run(&pool, "INSERT INTO snap (snapshot_id, keep) VALUES (4, false)").await.expect("insert");
    run(
        &pool,
        "/* was: DELETE FROM snap; see (SELECT 1 FROM nowhere) */ \
         DELETE FROM snap WHERE snapshot_id = 4",
    )
    .await
    .expect("a block comment quoting SQL must not run");
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM snap").await, 1);

    // A `--` inside a string literal is DATA, not a comment, and must survive.
    run(&pool, "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT NOT NULL)")
        .await
        .expect("create notes");
    run(&pool, "INSERT INTO notes (id, body) VALUES (1, 'a -- b /* c */ d')")
        .await
        .expect("insert literal");
    let body: String = pool
        .fetch_one(sqlx::raw_sql("SELECT body FROM notes WHERE id = 1"))
        .await
        .expect("read back")
        .try_get(0)
        .expect("body");
    assert_eq!(body, "a -- b /* c */ d", "comment markers inside a literal are data");
}
