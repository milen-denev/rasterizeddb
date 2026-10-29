//! `DELETE FROM t alias WHERE NOT EXISTS (… alias.col)` — the cold-row
//! retention worker's shape. All three of its statements parse and run as
//! `SELECT`s, so the `DELETE` context is the variable; two of them alias the
//! delete target and correlate a `NOT EXISTS` against that alias, and the
//! third does it four times.

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
async fn the_three_retention_deletes_run() {
    let (_s, pool) = server().await;
    for ddl in [
        "CREATE TABLE user_table_row_archive_restore_state (object_id INT PRIMARY KEY, restored_at TIMESTAMPTZ)",
        "CREATE TABLE data_lake_archive_object_metadata (object_id INT PRIMARY KEY, object_key TEXT NOT NULL)",
        "CREATE TABLE user_table_row_archive_manifest (id INT PRIMARY KEY, object_id INT NOT NULL)",
        "CREATE TABLE user_table_row_archive_manifest_file (id INT PRIMARY KEY, object_id INT NOT NULL)",
        "CREATE TABLE data_lake_iceberg_file (file_id INT PRIMARY KEY, object_key TEXT NOT NULL)",
    ] {
        run(&pool, ddl).await.expect(ddl);
    }
    // Object 1 is referenced everywhere and must survive; object 2 is an
    // orphan and must go. Restore state 1 has a live metadata row, 2 does not.
    run(&pool, "INSERT INTO data_lake_archive_object_metadata (object_id, object_key) VALUES (1, 'k1')").await.unwrap();
    run(&pool, "INSERT INTO data_lake_archive_object_metadata (object_id, object_key) VALUES (2, 'k2')").await.unwrap();
    run(&pool, "INSERT INTO user_table_row_archive_manifest (id, object_id) VALUES (1, 1)").await.unwrap();
    run(&pool, "INSERT INTO user_table_row_archive_manifest_file (id, object_id) VALUES (1, 1)").await.unwrap();
    run(&pool, "INSERT INTO data_lake_iceberg_file (file_id, object_key) VALUES (1, 'k1')").await.unwrap();
    run(&pool, "INSERT INTO user_table_row_archive_restore_state (object_id, restored_at) VALUES (1, '2026-08-01 00:00:00')").await.unwrap();
    run(&pool, "INSERT INTO user_table_row_archive_restore_state (object_id, restored_at) VALUES (9, '2026-08-01 00:00:00')").await.unwrap();

    // 1. aliased target, one correlated NOT EXISTS, bound parameter.
    sqlx::query(
        "DELETE FROM user_table_row_archive_restore_state rs \
         WHERE rs.restored_at <= $1 \
           AND NOT EXISTS (SELECT 1 FROM data_lake_archive_object_metadata om \
                            WHERE om.object_id = rs.object_id)",
    )
    .bind("2026-08-24 00:00:00")
    .execute(&pool)
    .await
    .expect("statement 1 must run");
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM user_table_row_archive_restore_state").await,
        1,
        "the orphan restore state went, the referenced one stayed"
    );
    assert_eq!(
        count(&pool, "SELECT COUNT(*) FROM user_table_row_archive_restore_state WHERE object_id = 1").await,
        1,
    );

    // 3. aliased target, FOUR correlated NOT EXISTS clauses.
    sqlx::query(
        "DELETE FROM data_lake_archive_object_metadata m \
         WHERE NOT EXISTS (SELECT 1 FROM user_table_row_archive_manifest manifest \
                            WHERE manifest.object_id = m.object_id) \
           AND NOT EXISTS (SELECT 1 FROM user_table_row_archive_manifest_file manifest_file \
                            WHERE manifest_file.object_id = m.object_id) \
           AND NOT EXISTS (SELECT 1 FROM user_table_row_archive_restore_state restore_state \
                            WHERE restore_state.object_id = m.object_id) \
           AND NOT EXISTS (SELECT 1 FROM data_lake_iceberg_file file \
                            WHERE file.object_key = m.object_key)",
    )
    .execute(&pool)
    .await
    .expect("statement 3 must run");
    let left: Vec<i32> = pool
        .fetch_all(sqlx::raw_sql("SELECT object_id FROM data_lake_archive_object_metadata ORDER BY object_id"))
        .await
        .expect("survivors")
        .iter()
        .map(|r| r.try_get::<i32, _>(0).expect("id"))
        .collect();
    assert_eq!(left, vec![1], "only the fully-referenced object survives");
}

/// Statement 2 in the form the worker actually sends it now: the ranked/
/// victims CTEs feeding `DELETE … WHERE snapshot_id IN (SELECT … FROM
/// victims)`. It was rewritten away from `DELETE … USING` after an earlier
/// truncation bug, so the USING form pinned elsewhere is SQL pms no longer
/// emits.
#[tokio::test]
async fn the_snapshot_retention_delete_in_subquery_runs() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE data_lake_iceberg_snapshot (snapshot_id INT PRIMARY KEY, domain_id TEXT NOT NULL, user_table_id TEXT NOT NULL, committed_at TIMESTAMPTZ NOT NULL)")
        .await
        .expect("create snapshots");
    run(&pool, "CREATE TABLE data_lake_iceberg_file2 (file_id INT PRIMARY KEY, snapshot_id INT NOT NULL, file_status TEXT NOT NULL)")
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
    run(&pool, "INSERT INTO data_lake_iceberg_file2 (file_id, snapshot_id, file_status) VALUES (1, 2, 'live')")
        .await
        .expect("insert file");

    sqlx::query(
        "WITH ranked AS (\n\
           SELECT snapshot_id,\n\
                  row_number() OVER (PARTITION BY domain_id, user_table_id\n\
                                     ORDER BY committed_at DESC, snapshot_id DESC) AS rn\n\
           FROM data_lake_iceberg_snapshot WHERE committed_at <= $1),\n\
         victims AS (\n\
           SELECT r.snapshot_id FROM ranked r\n\
           WHERE r.rn > $2\n\
             AND NOT EXISTS (SELECT 1 FROM data_lake_iceberg_file2 f\n\
                              WHERE f.snapshot_id = r.snapshot_id AND f.file_status = $3)\n\
           LIMIT $4)\n\
         DELETE FROM data_lake_iceberg_snapshot\n\
         WHERE snapshot_id IN (SELECT snapshot_id FROM victims)",
    )
    .bind("2026-08-24 00:00:00")
    .bind(2i64)
    .bind("live")
    .bind(10i64)
    .execute(&pool)
    .await
    .expect("the IN-subquery retention delete must run");

    let left: Vec<i32> = pool
        .fetch_all(sqlx::raw_sql("SELECT snapshot_id FROM data_lake_iceberg_snapshot ORDER BY snapshot_id"))
        .await
        .expect("survivors")
        .iter()
        .map(|r| r.try_get::<i32, _>(0).expect("id"))
        .collect();
    assert_eq!(left, vec![2, 4, 5], "rank cap and live-file guard both honoured");
}

/// A CTE body whose `FROM` starts a line. The select-list scanner searched
/// for the literal `" FROM "`, so a `FROM` preceded by a newline was not
/// found and the body — plainly a SELECT — was refused as "must be a SELECT".
/// Every ORM and every Rust raw-string query formats across lines; the same
/// space-literal trap has been fixed several times elsewhere in the engine.
#[tokio::test]
async fn a_cte_body_may_break_its_lines_anywhere() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE s (id INT PRIMARY KEY, grp TEXT NOT NULL, n BIGINT NOT NULL)")
        .await
        .expect("create");
    for (id, grp, n) in [(1, "a", 10i64), (2, "a", 20), (3, "b", 5)] {
        run(&pool, &format!("INSERT INTO s (id, grp, n) VALUES ({}, '{}', {})", id, grp, n))
            .await
            .expect("insert");
    }

    // `FROM` at the head of its line, with no space before it.
    let rows = pool
        .fetch_all(sqlx::raw_sql(
            "WITH totals AS (\nSELECT grp, SUM(n) AS total\nFROM s\nGROUP BY grp\n)\nSELECT grp, total FROM totals ORDER BY grp",
        ))
        .await
        .expect("a line-broken CTE body must parse");
    assert_eq!(rows.len(), 2);
    // `sum(bigint)` is a numeric, as in PostgreSQL (it used to be an int8).
    assert_eq!(
        rows[0].try_get::<rust_decimal::Decimal, _>("total").expect("total"),
        rust_decimal::Decimal::from(30)
    );

    // And with tabs, which are just as much whitespace as a space.
    let rows = pool
        .fetch_all(sqlx::raw_sql(
            "WITH totals AS (\n\tSELECT grp, SUM(n) AS total\n\tFROM s\n\tGROUP BY grp\n)\nSELECT total FROM totals ORDER BY grp",
        ))
        .await
        .expect("tabs are whitespace too");
    assert_eq!(rows.len(), 2);
}
