//! `DELETE … USING` — the retention worker's shape: a CTE ranks snapshots
//! per (domain, table) with row_number(), a second CTE picks the victims
//! with a correlated NOT EXISTS, and the DELETE joins against it. The
//! single-relation path refuses USING outright (it once degraded into
//! `DELETE FROM t` and destroyed 13,585 rows); this is the join-and-key-back
//! implementation that makes it work.

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
        .expect("count query")
        .try_get::<i64, _>(0)
        .expect("count")
}

/// The core: DELETE with an aliased target joined against a real relation.
#[tokio::test]
async fn delete_using_removes_only_the_joined_rows() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE snap (snapshot_id INT PRIMARY KEY, domain_id TEXT NOT NULL)")
        .await
        .expect("create snap");
    run(&pool, "CREATE TABLE victims (snapshot_id INT NOT NULL)")
        .await
        .expect("create victims");
    for id in 1..=5 {
        run(&pool, &format!("INSERT INTO snap (snapshot_id, domain_id) VALUES ({}, 'd1')", id))
            .await
            .expect("insert");
    }
    // Victims 2 and 4 — 4 listed twice: several source rows matching one
    // target row must delete it once, not fail on the second pass.
    for id in [2, 4, 4] {
        run(&pool, &format!("INSERT INTO victims (snapshot_id) VALUES ({})", id))
            .await
            .expect("insert victim");
    }

    run(&pool, "DELETE FROM snap s USING victims v WHERE s.snapshot_id = v.snapshot_id")
        .await
        .expect("DELETE USING must work");
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM snap").await, 3);
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM snap WHERE snapshot_id = 2").await, 0);
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM snap WHERE snapshot_id = 4").await, 0);
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM snap WHERE snapshot_id = 1").await, 1);

    // Without a WHERE it must refuse, never degrade into DELETE FROM snap.
    let bare = run(&pool, "DELETE FROM snap s USING victims v").await;
    assert!(bare.is_err(), "a USING without WHERE is refused");
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM snap").await, 3, "and deleted nothing");
}

/// The production statement, near verbatim: ranked CTE with row_number(),
/// victims CTE with a correlated NOT EXISTS, DELETE USING the result.
#[tokio::test]
async fn the_retention_cleanup_statement_works() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE data_lake_iceberg_snapshot (snapshot_id INT PRIMARY KEY, domain_id TEXT NOT NULL, user_table_id TEXT NOT NULL, committed_at TIMESTAMPTZ NOT NULL)")
        .await
        .expect("create snapshots");
    run(&pool, "CREATE TABLE data_lake_iceberg_file (file_id INT PRIMARY KEY, snapshot_id INT NOT NULL, file_status TEXT NOT NULL)")
        .await
        .expect("create files");
    // Five snapshots of one table, oldest first. Keep = 2, so 1..3 rank past
    // the cap — but snapshot 2 still has a live file and must survive.
    for (id, ts) in [
        (1, "2026-08-20 10:00:00"),
        (2, "2026-08-21 10:00:00"),
        (3, "2026-08-22 10:00:00"),
        (4, "2026-08-23 10:00:00"),
        (5, "2026-08-23 12:00:00"),
    ] {
        run(&pool, &format!(
            "INSERT INTO data_lake_iceberg_snapshot (snapshot_id, domain_id, user_table_id, committed_at) VALUES ({}, 'd1', 't1', '{}')",
            id, ts))
            .await
            .expect("insert snapshot");
    }
    run(&pool, "INSERT INTO data_lake_iceberg_file (file_id, snapshot_id, file_status) VALUES (1, 2, 'live')")
        .await
        .expect("insert file");

    run(&pool,
        "WITH ranked AS ( \
             SELECT snapshot_id, \
                    row_number() OVER ( \
                        PARTITION BY domain_id, user_table_id \
                        ORDER BY committed_at DESC, snapshot_id DESC \
                    ) AS rn \
             FROM data_lake_iceberg_snapshot \
             WHERE committed_at <= '2026-08-24 00:00:00' \
         ), \
         victims AS ( \
             SELECT r.snapshot_id \
             FROM ranked r \
             WHERE r.rn > 2 \
               AND NOT EXISTS ( \
                   SELECT 1 \
                   FROM data_lake_iceberg_file f \
                   WHERE f.snapshot_id = r.snapshot_id \
                     AND f.file_status = 'live' \
               ) \
             LIMIT 10 \
         ) \
         DELETE FROM data_lake_iceberg_snapshot s \
         USING victims v \
         WHERE s.snapshot_id = v.snapshot_id",
    )
    .await
    .expect("the retention cleanup must work");

    // Newest two (5, 4) kept by rank; 2 kept by its live file; 1 and 3 gone.
    let left: Vec<i64> = pool
        .fetch_all(sqlx::raw_sql("SELECT snapshot_id FROM data_lake_iceberg_snapshot ORDER BY snapshot_id"))
        .await
        .expect("survivors")
        .iter()
        .map(|r| r.try_get::<i32, _>(0).expect("id") as i64)
        .collect();
    assert_eq!(left, vec![2, 4, 5], "rank cap and live-file guard both honoured");
}

/// The retention statement as the worker actually sends it: extended
/// protocol, four bound parameters, and the multi-line formatting of a Rust
/// raw string. The literal-valued simple-protocol form above passed while
/// this one failed in production, so the difference is pinned here.
#[tokio::test]
async fn the_retention_cleanup_works_with_bound_parameters() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE data_lake_iceberg_snapshot (snapshot_id INT PRIMARY KEY, domain_id TEXT NOT NULL, user_table_id TEXT NOT NULL, committed_at TIMESTAMPTZ NOT NULL)")
        .await
        .expect("create snapshots");
    run(&pool, "CREATE TABLE data_lake_iceberg_file (file_id INT PRIMARY KEY, snapshot_id INT NOT NULL, file_status TEXT NOT NULL)")
        .await
        .expect("create files");
    for (id, ts) in [
        (1, "2026-08-20 10:00:00"),
        (2, "2026-08-21 10:00:00"),
        (3, "2026-08-22 10:00:00"),
        (4, "2026-08-23 10:00:00"),
        (5, "2026-08-23 12:00:00"),
    ] {
        run(&pool, &format!(
            "INSERT INTO data_lake_iceberg_snapshot (snapshot_id, domain_id, user_table_id, committed_at) VALUES ({}, 'd1', 't1', '{}')",
            id, ts))
            .await
            .expect("insert snapshot");
    }
    run(&pool, "INSERT INTO data_lake_iceberg_file (file_id, snapshot_id, file_status) VALUES (1, 2, 'live')")
        .await
        .expect("insert file");

    let sql = r#"
                        WITH ranked AS (
                            SELECT snapshot_id,
                                   row_number() OVER (
                                       PARTITION BY domain_id, user_table_id
                                       ORDER BY committed_at DESC, snapshot_id DESC
                                   ) AS rn
                            FROM data_lake_iceberg_snapshot
                            WHERE committed_at <= $1
                        ),
                        victims AS (
                            SELECT r.snapshot_id
                            FROM ranked r
                            WHERE r.rn > $2
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM data_lake_iceberg_file f
                                  WHERE f.snapshot_id = r.snapshot_id
                                    AND f.file_status = $3
                              )
                            LIMIT $4
                        )
                        DELETE FROM data_lake_iceberg_snapshot s
                        USING victims v
                        WHERE s.snapshot_id = v.snapshot_id
"#;

    sqlx::query(sql)
        .bind("2026-08-24 00:00:00")
        .bind(2i64)
        .bind("live")
        .bind(10i64)
        .execute(&pool)
        .await
        .expect("the bound retention cleanup must work");

    let left: Vec<i32> = pool
        .fetch_all(sqlx::raw_sql("SELECT snapshot_id FROM data_lake_iceberg_snapshot ORDER BY snapshot_id"))
        .await
        .expect("survivors")
        .iter()
        .map(|r| r.try_get::<i32, _>(0).expect("id"))
        .collect();
    assert_eq!(left, vec![2, 4, 5], "rank cap and live-file guard both honoured");
}
