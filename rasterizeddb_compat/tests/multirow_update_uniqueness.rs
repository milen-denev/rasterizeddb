//! A multi-row UPDATE is checked for uniqueness against its own earlier rows.
//!
//! The check probes the table through its index or filter, whose
//! maintenance waits for the statement's commit, and an in-place overwrite
//! reaches the data file only at that commit: a statement could not see its
//! own earlier rows. `UPDATE su SET n = 1000 WHERE id < 5` on a UNIQUE `n`
//! stored five rows holding 1000 and reported success, in memory and on
//! disk, for single-column, composite and primary keys, plain and
//! hash-partitioned. PostgreSQL checks each row as it is written and fails
//! the statement with 23505, leaving nothing.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};
use std::time::Duration;

struct Root(String);

impl Root {
    fn new(tag: &str) -> Self {
        let r = std::env::temp_dir().join(format!("multirow_update_uniqueness_{}_{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&r);
        std::fs::create_dir_all(&r).expect("root");
        Root(r.to_string_lossy().into_owned())
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn code(e: &sqlx::Error) -> String {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| e.to_string())
}

async fn rows(pool: &sqlx::PgPool, table: &str, cols: &str) -> Vec<String> {
    let key = cols.split(", ").map(|c| format!("COALESCE(CAST({c} AS TEXT), '-')")).collect::<Vec<_>>().join(" || '|' || ");
    let mut out: Vec<String> = sqlx::query(AssertSqlSafe(format!("SELECT {key} FROM {table}")))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{table}: {e}"))
        .iter()
        .map(|r| r.get::<String, _>(0))
        .collect();
    out.sort();
    out
}

const PLAIN: &[(&str, &str)] = &[("su", "id, n, t, m"), ("cu", "id, a, b, m"), ("pk", "id, m, t")];

async fn setup_plain(pool: &sqlx::PgPool) {
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE su (id BIGINT PRIMARY KEY, n BIGINT UNIQUE, t TEXT UNIQUE, m BIGINT);
         CREATE TABLE cu (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, m BIGINT, UNIQUE (a, b));
         CREATE TABLE pk (id BIGINT PRIMARY KEY, m BIGINT, t TEXT);",
    ))
    .await
    .unwrap();
    for (table, values) in [
        ("su", (0..20).map(|i| format!("({i}, {i}, 't{i}', 0)")).collect::<Vec<_>>()),
        ("cu", (0..20).map(|i| format!("({i}, {i}, {i}, 0)")).collect()),
        ("pk", (0..20).map(|i| format!("({i}, 0, 'x')")).collect()),
    ] {
        pool.execute(AssertSqlSafe(format!("INSERT INTO {table} VALUES {}", values.join(", ")))).await.unwrap();
    }
}

/// Each statement fails with 23505 and leaves every table as it was.
async fn refused_plain(pool: &sqlx::PgPool, label: &str) {
    let mut before = Vec::new();
    for (t, c) in PLAIN {
        before.push(rows(pool, t, c).await);
    }
    for stmts in [
        &["UPDATE su SET n = 1000 WHERE id < 5"][..],
        &["UPDATE su SET t = 'same' WHERE id < 5"],
        &["UPDATE su SET n = 1001, t = 'same2' WHERE id >= 5 AND id < 8"],
        &["UPDATE cu SET a = 100, b = 100 WHERE id < 5"],
        &["UPDATE pk SET id = 1000 WHERE id < 5"],
        &["UPDATE pk SET id = 1001, t = 'y' WHERE id >= 5 AND id < 8"],
        &["BEGIN", "UPDATE su SET n = 2000 WHERE id >= 10 AND id < 13"],
    ] {
        let mut c = pool.acquire().await.unwrap();
        let last = stmts.len() - 1;
        for (i, s) in stmts.iter().enumerate() {
            let r = c.execute(AssertSqlSafe(*s)).await;
            if i < last {
                r.unwrap_or_else(|e| panic!("{s}: {e}"));
            } else {
                let e = r.expect_err(&format!("{label}: {s} stored a duplicate"));
                assert_eq!(code(&e), "23505", "{label}: {s}: {e}");
            }
        }
        let _ = c.execute("ROLLBACK").await;
        drop(c);
        for ((t, cols), want) in PLAIN.iter().zip(&before) {
            assert_eq!(&rows(pool, t, cols).await, want, "{label}: {stmts:?} changed {t}");
        }
    }
}

/// Updates that keep every key distinct still succeed, in place and as new
/// versions, single-column and composite, including a row taking a value an
/// earlier row of the statement gave up.
async fn allowed_plain(pool: &sqlx::PgPool, label: &str) {
    for (sql, n) in [
        ("UPDATE su SET n = n + 1000 WHERE id < 5", 5),
        ("UPDATE su SET t = t || 'x' WHERE id < 5", 5),
        ("UPDATE cu SET a = a + 500 WHERE id < 10", 10),
        ("UPDATE pk SET id = id + 100 WHERE id >= 15", 5),
        ("UPDATE su SET n = 9000 WHERE id = 19", 1),
    ] {
        let r = pool.execute(sql).await.unwrap_or_else(|e| panic!("{label}: {sql}: {e}"));
        assert_eq!(r.rows_affected(), n, "{label}: {sql}");
    }
    assert_eq!(rows(pool, "su", "n").await.iter().filter(|n| n.as_str() == "1000").count(), 1, "{label}");
    // A row taking the value an earlier row of the same statement gave up
    // (rows are visited in the order they were written: 6, then 7). The
    // earlier row's old version still looked live on disk, and this failed.
    pool.execute("UPDATE su SET n = CASE WHEN id = 6 THEN 5000 ELSE 6 END WHERE id = 6 OR id = 7")
        .await
        .unwrap_or_else(|e| panic!("{label}: a value vacated earlier in the statement: {e}"));
    assert_eq!(
        rows(pool, "su", "id, n").await.into_iter().filter(|r| r.starts_with("6|") || r.starts_with("7|")).collect::<Vec<_>>(),
        ["6|5000", "7|6"],
        "{label}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_multi_row_update_is_checked_against_its_own_rows_in_memory() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    setup_plain(&pool).await;
    refused_plain(&pool, "memory").await;
    allowed_plain(&pool, "memory").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_multi_row_update_is_checked_against_its_own_rows_on_disk_and_after_a_restart() {
    let dir = Root::new("plain");
    let before = {
        let server = TestServer::start_file_backed(&dir.0).await.expect("server");
        let pool = server.connect_sqlx().await.expect("connect");
        setup_plain(&pool).await;
        refused_plain(&pool, "file").await;
        allowed_plain(&pool, "file").await;
        let mut all = Vec::new();
        for (t, c) in PLAIN {
            all.push(rows(&pool, t, c).await);
        }
        all
    };
    let server = TestServer::start_file_backed(&dir.0).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("connect");
    for ((t, cols), want) in PLAIN.iter().zip(&before) {
        assert_eq!(&rows(&pool, t, cols).await, want, "{t} after a restart");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_multi_row_update_is_checked_against_its_own_rows_on_a_hash_partitioned_table() {
    let dir = Root::new("hash");
    let before = {
        let server = TestServer::start_file_backed(&dir.0).await.expect("server");
        let pool = server.connect_sqlx().await.expect("connect");
        pool.execute(
            "CREATE TABLE hp (tenant BIGINT, id BIGINT, n BIGINT, t TEXT, PRIMARY KEY (tenant, id), UNIQUE (tenant, n)) \
             PARTITION BY HASH (tenant) PARTITIONS 4",
        )
        .await
        .unwrap();
        let values: Vec<String> = (0..20).map(|i| format!("({}, {i}, {i}, 't{i}')", i % 2)).collect();
        pool.execute(AssertSqlSafe(format!("INSERT INTO hp VALUES {}", values.join(", ")))).await.unwrap();
        let before = rows(&pool, "hp", "tenant, id, n, t").await;
        for sql in [
            "UPDATE hp SET n = 1000 WHERE tenant = 0 AND id < 10",
            "UPDATE hp SET n = 2000 WHERE id >= 10 AND id < 16",
            "UPDATE hp SET id = 5000 WHERE tenant = 1 AND id < 10",
            "UPDATE hp SET n = 3000, t = 'same' WHERE tenant = 1 AND id >= 10",
        ] {
            let e = pool.execute(sql).await.expect_err(sql);
            assert_eq!(code(&e), "23505", "{sql}: {e}");
            assert_eq!(rows(&pool, "hp", "tenant, id, n, t").await, before, "{sql} changed rows");
        }
        pool.execute("UPDATE hp SET n = n + 100 WHERE tenant = 0").await.expect("distinct keys");
        rows(&pool, "hp", "tenant, id, n, t").await
    };
    let server = TestServer::start_file_backed(&dir.0).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("connect");
    assert_eq!(rows(&pool, "hp", "tenant, id, n, t").await, before, "after a restart");
}

/// Two sessions each setting a different row to one value: one wins, the
/// other fails 23505 once the first commits, and one row holds the value.
#[tokio::test(flavor = "multi_thread")]
async fn two_sessions_cannot_both_take_one_value() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    setup_plain(&pool).await;
    let mut a = pool.acquire().await.unwrap();
    a.execute("BEGIN").await.unwrap();
    a.execute("UPDATE su SET n = 7000 WHERE id = 1").await.unwrap();
    let b = {
        let pool = pool.clone();
        tokio::spawn(async move { pool.execute("UPDATE su SET n = 7000 WHERE id = 2").await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    a.execute("COMMIT").await.unwrap();
    drop(a);
    let e = tokio::time::timeout(Duration::from_secs(60), b).await.expect("b finished").unwrap().expect_err("both took 7000");
    assert_eq!(code(&e), "23505", "{e}");
    assert_eq!(rows(&pool, "su", "n").await.iter().filter(|n| *n == "7000").count(), 1);
}
