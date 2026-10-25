//! `SELECT rdb_index_check('t')` over the wire (docs/persistent-indexes.md
//! §4 Audit, slice B18): one row of findings, its columns described the
//! same through Describe as in the reply, on a plain and a partitioned
//! table; an index, a catalogue view and an unknown name refused with
//! PostgreSQL's codes before anything is read. A table the check finds
//! wanting is the core suite's (`index_store::audit::tests`), which can
//! drop an entry; nothing over the wire can.

use rasterizeddb_compat::TestServer;
use sqlx::{Column, Executor, Row};

const COLUMNS: [&str; 8] = [
    "relation",
    "file_sets",
    "rows_checked",
    "index_entries",
    "missing_entries",
    "filter_values",
    "filter_misses",
    "result",
];

#[tokio::test(flavor = "multi_thread")]
async fn rdb_index_check_answers_one_row_and_refuses_what_is_not_a_table() {
    // File-backed: partitioned tables need it, and a file set's gate is
    // the one production checks against.
    struct Remove(std::path::PathBuf);
    impl Drop for Remove {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = std::env::temp_dir().join(format!("rdb_index_check_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("a temporary directory");
    let _cleanup = Remove(root.clone());
    let server = TestServer::start_file_backed(&root.to_string_lossy()).await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, s TEXT);
         CREATE INDEX t_a ON t (a);
         CREATE INDEX t_s ON t USING hash (s);",
    ))
    .await
    .expect("schema");
    let values: Vec<String> = (0..1200i64).map(|i| format!("({i}, {}, 's{}')", (i * 31) % 500, i % 97)).collect();
    pool.execute(sqlx::AssertSqlSafe(format!("INSERT INTO t (id, a, s) VALUES {}", values.join(", "))))
        .await
        .expect("rows");
    pool.execute("DELETE FROM t WHERE id % 10 = 3").await.expect("delete");

    // Prepared: Describe's columns are the reply's.
    let row = sqlx::query("SELECT rdb_index_check('t')").fetch_one(&pool).await.expect("rdb_index_check");
    let names: Vec<&str> = row.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, COLUMNS);
    let get = |i: usize| row.get::<String, _>(i);
    assert_eq!(get(0), "t");
    assert_eq!(get(1), "1");
    assert_eq!(get(2), "1080", "rows_checked");
    assert_eq!(get(3), (3 * 1080).to_string(), "index_entries: three indexes");
    assert_eq!(get(4), "0", "missing_entries");
    assert_eq!(get(6), "0", "filter_misses");
    assert!(get(5).parse::<u64>().unwrap() >= 3 * 1080, "filter_values {}", get(5));
    assert_eq!(get(7), "clean");

    // Simple query, with the regclass cast.
    let rows = pool.fetch_all(sqlx::raw_sql("SELECT rdb_index_check('t'::regclass)")).await.expect("simple");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>(7), "clean");

    // A partitioned table: each partition.
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE p (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v BIGINT, PRIMARY KEY (tenant_id, id))
             PARTITION BY HASH (tenant_id) PARTITIONS 4",
    ))
    .await
    .expect("partitioned");
    let values: Vec<String> = (0..400i64).map(|i| format!("({}, {i}, {i})", i % 13)).collect();
    pool.execute(sqlx::AssertSqlSafe(format!("INSERT INTO p (tenant_id, id, v) VALUES {}", values.join(", "))))
        .await
        .expect("partitioned rows");
    let row = sqlx::query("SELECT rdb_index_check('p')").fetch_one(&pool).await.expect("partitioned check");
    assert!(row.get::<String, _>(1).parse::<u64>().unwrap() >= 4, "file_sets {}", row.get::<String, _>(1));
    assert_eq!(row.get::<String, _>(2), "400");
    assert_eq!(row.get::<String, _>(7), "clean");

    // Refusals, before anything is read.
    for (arg, code) in [("t_a", "42809"), ("pg_class", "42809"), ("no_such_table", "42P01")] {
        let e = pool
            .execute(sqlx::AssertSqlSafe(format!("SELECT rdb_index_check('{arg}')")))
            .await
            .expect_err(arg);
        assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some(code), "{arg}: {e}");
    }
    let e = pool.execute("SELECT rdb_index_check(t)").await.expect_err("an identifier, not a literal");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("22023"), "{e}");
    // The session is usable after each.
    let row = sqlx::query("SELECT rdb_index_check('t')").fetch_one(&pool).await.expect("again");
    assert_eq!(row.get::<String, _>(7), "clean");
    drop(pool);
    drop(server);
}
