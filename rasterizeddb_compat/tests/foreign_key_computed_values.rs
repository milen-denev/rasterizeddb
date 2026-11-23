//! A foreign key is checked against the value the row stores.
//!
//! The check read the statement's text. A value that is no literal went
//! into the parent's lookup as written: `gen_random_uuid()` was read there
//! as a column ("foreign key … could not be checked: unknown column or
//! unsupported keyword 'gen_random_uuid'", 0A000), `nextval('s')` was
//! compared as text and matched no parent, and a key column the statement
//! did not write was not checked at all, so its DEFAULT went in whatever it
//! was. An `UPDATE` computed a volatile assignment once to check it and
//! again to write it: `SET pn = nextval('s')` checked one number and stored
//! the next, which no parent had to hold.
//!
//! Now the value is computed once, checked, and that same value stored.
//! Every expectation is PostgreSQL 17's answer to the same statements.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

const PARENT: &str = "aaaaaaaa-0000-4000-8000-000000000001";

async fn start(setup: &str) -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(AssertSqlSafe(setup.to_string()))).await.expect("setup");
    (server, pool)
}

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

/// The SQLSTATE `sql` fails with.
async fn code(pool: &sqlx::PgPool, sql: &str) -> String {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_all(pool).await.expect_err(sql);
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| format!("no code: {e}"))
}

/// The invariant behind every test: no stored key without its parent.
async fn orphans(pool: &sqlx::PgPool, child: &str, column: &str, parent: &str) -> Vec<String> {
    rows(
        pool,
        &format!(
            "SELECT count(*) FROM {child} c LEFT JOIN {parent} p ON p.id = c.{column} \
             WHERE c.{column} IS NOT NULL AND p.id IS NULL"
        ),
    )
    .await
}

const NUMBERED: &str = "
    CREATE TABLE parn (id int primary key, ts timestamptz unique);
    CREATE TABLE chn (id int primary key, pn int REFERENCES parn(id), pd int DEFAULT 99 REFERENCES parn(id),
                      pts timestamptz REFERENCES parn(ts));
    INSERT INTO parn VALUES (1, '2026-01-01 00:00:00+00'), (2, NULL), (3, NULL);
    CREATE SEQUENCE sq START 1;";

#[tokio::test(flavor = "multi_thread")]
async fn an_insert_checks_a_generated_uuid_as_a_value() {
    let (_server, pool) = start(&format!(
        "CREATE TABLE par (id uuid primary key);
         CREATE TABLE pl_child (id uuid primary key, p uuid REFERENCES par(id));
         INSERT INTO par VALUES ('{PARENT}');"
    ))
    .await;
    // The benchmark's statement: a key no parent holds is 23503, not 0A000.
    assert_eq!(code(&pool, "INSERT INTO pl_child VALUES (gen_random_uuid(), gen_random_uuid())").await, "23503");
    for sql in [
        format!("INSERT INTO pl_child VALUES (gen_random_uuid(), '{PARENT}')"),
        format!("INSERT INTO pl_child VALUES (gen_random_uuid(), '{PARENT}'::uuid), (gen_random_uuid(), NULL)"),
        format!("INSERT INTO pl_child (p, id) VALUES (CAST('{PARENT}' AS uuid), gen_random_uuid())"),
        format!("INSERT INTO pl_child VALUES (gen_random_uuid(), upper('{PARENT}')::uuid)"),
    ] {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.clone()))).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(rows(&pool, "SELECT count(*), count(p), count(DISTINCT id) FROM pl_child").await, ["5|4|5"]);
    assert_eq!(orphans(&pool, "pl_child", "p", "par").await, ["0"]);
    // An UPDATE to a fresh uuid names no parent either, and changes nothing.
    assert_eq!(code(&pool, "UPDATE pl_child SET p = gen_random_uuid()").await, "23503");
    assert_eq!(rows(&pool, "SELECT count(p), count(DISTINCT p) FROM pl_child").await, ["4|1"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_insert_draws_a_sequence_once_and_checks_that_number() {
    let (_server, pool) = start(NUMBERED).await;
    // 1, 2 and 3 have parents; 4 has none. One number per statement.
    for id in 1..=3 {
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!("INSERT INTO chn (id, pn, pd) VALUES ({id}, nextval('sq'), 1)"))))
            .await
            .unwrap_or_else(|e| panic!("row {id}: {e}"));
    }
    assert_eq!(code(&pool, "INSERT INTO chn (id, pn, pd) VALUES (4, nextval('sq'), 1)").await, "23503");
    assert_eq!(rows(&pool, "SELECT id, pn FROM chn ORDER BY id").await, ["1|1", "2|2", "3|3"]);
    assert_eq!(rows(&pool, "SELECT nextval('sq')").await, ["5"]);
    // Arithmetic and a call are their value.
    assert_eq!(rows(&pool, "INSERT INTO chn (id, pn, pd) VALUES (5, 1 + 1, 4 - 1) RETURNING pn, pd").await, ["2|3"]);
    assert_eq!(rows(&pool, "INSERT INTO chn (id, pn, pd) VALUES (7, abs(-2), 1) RETURNING pn").await, ["2"]);
    assert_eq!(code(&pool, "INSERT INTO chn (id, pn, pd) VALUES (6, 1 + 10, 1)").await, "23503");
    // One bad row of several fails them all.
    assert_eq!(
        code(&pool, "INSERT INTO chn (id, pn, pd) VALUES (17, 2, 1), (18, 1 + 2, 1), (19, 1 + 20, 1)").await,
        "23503"
    );
    // The clock names no parent; the parent's own instant does.
    assert_eq!(code(&pool, "INSERT INTO chn (id, pn, pd, pts) VALUES (10, 1, 1, now())").await, "23503");
    assert_eq!(code(&pool, "INSERT INTO chn (id, pn, pd, pts) VALUES (12, 1, 1, CURRENT_TIMESTAMP)").await, "23503");
    pool.execute(sqlx::raw_sql(
        "INSERT INTO chn (id, pn, pd, pts) VALUES (11, 1, 1, '2026-01-01 00:00:00+00'::timestamptz)",
    ))
    .await
    .expect("the parent's instant");
    assert_eq!(rows(&pool, "SELECT id FROM chn ORDER BY id").await, ["1", "2", "3", "5", "7", "11"]);
    assert_eq!(orphans(&pool, "chn", "pn", "parn").await, ["0"]);
}

/// A key column the statement does not write takes its DEFAULT, and that
/// is a value like any other. It went in unchecked: 99, with no parent 99.
#[tokio::test(flavor = "multi_thread")]
async fn a_default_in_a_key_column_is_checked() {
    let (_server, pool) = start(NUMBERED).await;
    assert_eq!(code(&pool, "INSERT INTO chn (id, pn) VALUES (8, 1)").await, "23503");
    assert_eq!(code(&pool, "INSERT INTO chn (id, pn, pd) VALUES (9, 1, DEFAULT)").await, "23503");
    assert_eq!(code(&pool, "INSERT INTO chn (id, pn) VALUES (8, 1), (9, 2)").await, "23503");
    assert_eq!(rows(&pool, "SELECT count(*) FROM chn").await, ["0"]);
    // With its parent there, the default is the row's key.
    pool.execute(sqlx::raw_sql("INSERT INTO parn VALUES (99, NULL)")).await.expect("parent 99");
    assert_eq!(rows(&pool, "INSERT INTO chn (id, pn) VALUES (8, 1) RETURNING pd").await, ["99"]);
    assert_eq!(rows(&pool, "INSERT INTO chn (id, pn, pd) VALUES (9, 1, DEFAULT) RETURNING pd").await, ["99"]);
    // A written NULL is no key, default or not.
    assert_eq!(rows(&pool, "INSERT INTO chn (id, pn, pd) VALUES (10, NULL, NULL) RETURNING pn, pd").await, ["NULL|NULL"]);
    assert_eq!(orphans(&pool, "chn", "pd", "parn").await, ["0"]);
    // And an UPDATE back to it, checked the same way.
    pool.execute(sqlx::raw_sql("UPDATE chn SET pd = 1 WHERE id = 8; DELETE FROM chn WHERE id = 9; DELETE FROM parn WHERE id = 99"))
        .await
        .expect("parent 99 gone");
    assert_eq!(code(&pool, "UPDATE chn SET pd = DEFAULT WHERE id = 8").await, "23503");
    assert_eq!(rows(&pool, "SELECT pd FROM chn WHERE id = 8").await, ["1"]);
}

const UPDATED: &str = "
    CREATE TABLE parn (id int primary key);
    CREATE TABLE chn (id int primary key, pn int REFERENCES parn(id), tag uuid);
    INSERT INTO parn SELECT g FROM generate_series(1, 6) g;
    INSERT INTO chn VALUES (1, 1, NULL), (2, 1, NULL), (3, 1, NULL);";

#[tokio::test(flavor = "multi_thread")]
async fn an_update_stores_the_number_it_checked() {
    let (_server, pool) = start(UPDATED).await;
    pool.execute(sqlx::raw_sql("CREATE SEQUENCE s2 START 1; CREATE SEQUENCE s4 START 5")).await.expect("sequences");
    // One number drawn, and it is the one stored. It was 2, with 1 checked.
    assert_eq!(rows(&pool, "UPDATE chn SET pn = nextval('s2') WHERE id = 1 RETURNING pn").await, ["1"]);
    assert_eq!(rows(&pool, "SELECT nextval('s2')").await, ["2"]);
    pool.execute(sqlx::raw_sql("UPDATE chn SET pn = nextval('s2') WHERE id = 2")).await.expect("3 has a parent");
    assert_eq!(rows(&pool, "SELECT id, pn FROM chn ORDER BY id").await, ["1|1", "2|3", "3|1"]);
    // 5 and 6 have parents, 7 has none: the statement fails and no row of
    // it stays. It stored 7, 8 and 9 before, having checked 4, 5 and 6.
    assert_eq!(code(&pool, "UPDATE chn SET pn = nextval('s4') WHERE id < 4").await, "23503");
    assert_eq!(rows(&pool, "SELECT id, pn FROM chn ORDER BY id").await, ["1|1", "2|3", "3|1"]);
    assert_eq!(orphans(&pool, "chn", "pn", "parn").await, ["0"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_update_in_a_block_leaves_its_rows_as_they_were() {
    let (_server, pool) = start(UPDATED).await;
    pool.execute(sqlx::raw_sql("CREATE SEQUENCE s4 START 5")).await.expect("sequence");
    let mut conn = pool.acquire().await.expect("connection");
    conn.execute(sqlx::raw_sql("BEGIN; UPDATE chn SET tag = gen_random_uuid() WHERE id = 3")).await.expect("block");
    let e = conn.execute(sqlx::raw_sql("UPDATE chn SET pn = nextval('s4') + 100 WHERE id < 4")).await.expect_err("no parent");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("23503"), "{e}");
    conn.execute(sqlx::raw_sql("ROLLBACK")).await.expect("rollback");
    drop(conn);
    assert_eq!(rows(&pool, "SELECT id, pn, tag IS NULL FROM chn ORDER BY id").await, ["1|1|t", "2|1|t", "3|1|t"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_update_checks_a_volatile_expression_by_what_it_wrote() {
    let (_server, pool) = start(UPDATED).await;
    assert_eq!(
        rows(&pool, "UPDATE chn SET pn = CASE WHEN random() >= 0 THEN 4 ELSE 99 END WHERE id = 3 RETURNING pn").await,
        ["4"]
    );
    assert_eq!(code(&pool, "UPDATE chn SET pn = CASE WHEN random() >= 0 THEN 99 ELSE 4 END WHERE id = 3").await, "23503");
    assert_eq!(rows(&pool, "SELECT pn FROM chn WHERE id = 3").await, ["4"]);
    // A NULL it computes is no key.
    assert_eq!(rows(&pool, "UPDATE chn SET pn = CASE WHEN random() < 0 THEN 4 END WHERE id = 3 RETURNING pn").await, ["NULL"]);
    // An expression the row fixes is checked before the write, as it was.
    assert_eq!(rows(&pool, "UPDATE chn SET pn = pn + 1 WHERE id = 1 RETURNING pn").await, ["2"]);
    assert_eq!(code(&pool, "UPDATE chn SET pn = pn + 40 WHERE id = 1").await, "23503");
    assert_eq!(rows(&pool, "UPDATE chn SET pn = 1 + 1 WHERE id = 2 RETURNING pn").await, ["2"]);
    assert_eq!(code(&pool, "UPDATE chn SET pn = 1 + 30 WHERE id = 2").await, "23503");
    assert_eq!(rows(&pool, "SELECT id, pn FROM chn ORDER BY id").await, ["1|2", "2|2", "3|NULL"]);
    assert_eq!(orphans(&pool, "chn", "pn", "parn").await, ["0"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn on_conflict_draws_a_sequence_only_when_it_updates() {
    let (_server, pool) = start(UPDATED).await;
    pool.execute(sqlx::raw_sql("CREATE SEQUENCE s3 START 1; CREATE SEQUENCE s5 START 1")).await.expect("sequences");
    // No conflict: the SET never runs. It drew two numbers before.
    pool.execute(sqlx::raw_sql("INSERT INTO chn VALUES (50, 1, NULL) ON CONFLICT (id) DO UPDATE SET pn = nextval('s5')"))
        .await
        .expect("no conflict");
    assert_eq!(rows(&pool, "SELECT pn FROM chn WHERE id = 50").await, ["1"]);
    assert_eq!(rows(&pool, "SELECT nextval('s5')").await, ["1"]);
    // A conflict: one number, checked and stored.
    assert_eq!(
        rows(&pool, "INSERT INTO chn VALUES (1, 1, NULL) ON CONFLICT (id) DO UPDATE SET pn = nextval('s3') + 1 RETURNING pn")
            .await,
        ["2"]
    );
    assert_eq!(rows(&pool, "SELECT nextval('s3')").await, ["2"]);
    // What the SET computes is checked: 61 and 71 have no parent.
    assert_eq!(code(&pool, "INSERT INTO chn VALUES (1, 1, NULL) ON CONFLICT (id) DO UPDATE SET pn = 60 + 1").await, "23503");
    assert_eq!(
        code(&pool, "INSERT INTO chn VALUES (1, 1, NULL) ON CONFLICT (id) DO UPDATE SET pn = excluded.pn + 70").await,
        "23503"
    );
    assert_eq!(
        rows(&pool, "INSERT INTO chn VALUES (1, 3, NULL) ON CONFLICT (id) DO UPDATE SET pn = excluded.pn RETURNING pn").await,
        ["3"]
    );
    // A constant PostgreSQL refuses to compute is refused with no conflict too.
    assert_eq!(code(&pool, "INSERT INTO chn VALUES (71, 1, NULL) ON CONFLICT (id) DO UPDATE SET pn = 'abc'::int").await, "22P02");
    assert_eq!(rows(&pool, "SELECT count(*) FROM chn WHERE id = 71").await, ["0"]);
    assert_eq!(orphans(&pool, "chn", "pn", "parn").await, ["0"]);
}

/// A file-backed server's directory, which partitioned tables need; removed
/// on drop.
struct OnDisk(std::path::PathBuf);

impl Drop for OnDisk {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The same over a partitioned child, whose rows each partition writes.
#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_child_checks_what_it_stores() {
    let root = std::env::temp_dir().join(format!("foreign_key_computed_values_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let server = TestServer::start_file_backed(&root.to_string_lossy()).await.expect("server");
    let _root = OnDisk(root);
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE parn (id int primary key);
         INSERT INTO parn SELECT g FROM generate_series(1, 6) g;
         CREATE TABLE cp (k int, id int, pn int REFERENCES parn(id), PRIMARY KEY (k, id)) PARTITION BY HASH (k);
         INSERT INTO cp VALUES (1, 1, 1), (2, 2, 1), (3, 3, 1);
         CREATE SEQUENCE s6 START 1;
         CREATE SEQUENCE s7 START 6;",
    ))
    .await
    .expect("setup");
    assert_eq!(rows(&pool, "UPDATE cp SET pn = nextval('s6') WHERE k = 1 RETURNING pn").await, ["1"]);
    assert_eq!(rows(&pool, "SELECT nextval('s6')").await, ["2"]);
    // 6 has a parent, 7 and 8 have none.
    assert_eq!(code(&pool, "UPDATE cp SET pn = nextval('s7')").await, "23503");
    assert_eq!(rows(&pool, "SELECT k, pn FROM cp ORDER BY k").await, ["1|1", "2|1", "3|1"]);
    assert_eq!(code(&pool, "INSERT INTO cp VALUES (4, 4, nextval('s7'))").await, "23503");
    assert_eq!(rows(&pool, "INSERT INTO cp VALUES (5, 5, 1 + 1), (6, 6, abs(-3)) RETURNING k, pn").await, ["5|2", "6|3"]);
    assert_eq!(orphans(&pool, "cp", "pn", "parn").await, ["0"]);
}
