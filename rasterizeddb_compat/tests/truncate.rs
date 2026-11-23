//! `TRUNCATE`, which was "syntax error or unsupported query" in every form,
//! though ORMs and test fixtures send it before nearly every test
//! (`TRUNCATE … RESTART IDENTITY CASCADE`).
//!
//! It is each table's rows deleted in the caller's transaction under ACCESS
//! EXCLUSIVE, so it rolls back as PostgreSQL's does (`executor::
//! execute_truncate`). Every expected answer is PostgreSQL 17's
//! (`.claude/reviews/trunc/p1.sql`, `p2.sql`).

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Connection, Executor, Row};
use tokio_postgres::SimpleQueryMessage;

async fn rows(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"))
            }
            _ => None,
        })
        .collect()
}

async fn code(c: &tokio_postgres::Client, sql: &str) -> String {
    let e = c.simple_query(sql).await.expect_err(sql);
    e.code().map(|s| s.code().to_string()).unwrap_or_else(|| panic!("{sql}: {e}"))
}

async fn run(c: &tokio_postgres::Client, sql: &str) {
    c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Foreign keys: a referenced table alone is refused (0A000), with its
/// referencing table or `CASCADE` both are emptied, and a table referencing
/// itself is emptied whole, its references among the rows deleted with them.
/// A plain `DELETE` of a self-referencing table was refused the same way:
/// the check ran before the delete and found rows the delete was removing.
#[tokio::test(flavor = "multi_thread")]
async fn foreign_keys_are_refused_cascaded_or_emptied_together() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE parent (id INT PRIMARY KEY, v TEXT)",
        "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))",
        "CREATE TABLE sr (id INT PRIMARY KEY, up INT REFERENCES sr(id))",
        "CREATE TABLE rr (id INT PRIMARY KEY, up INT REFERENCES rr(id) ON DELETE RESTRICT)",
        "INSERT INTO parent VALUES (1, 'a'), (2, 'b')",
        "INSERT INTO child VALUES (10, 1), (20, 2)",
        "INSERT INTO sr VALUES (1, NULL), (2, 1), (3, 2)",
        "INSERT INTO rr VALUES (1, NULL), (2, 1), (3, 2)",
    ] {
        run(&c, sql).await;
    }
    assert_eq!(code(&c, "TRUNCATE parent").await, "0A000");
    assert_eq!(code(&c, "TRUNCATE parent RESTRICT").await, "0A000");
    assert_eq!(rows(&c, "SELECT count(*) FROM parent").await, ["2"], "a refused TRUNCATE emptied the table");
    run(&c, "TRUNCATE parent, child").await;
    assert_eq!(rows(&c, "SELECT (SELECT count(*) FROM parent), (SELECT count(*) FROM child)").await, ["0|0"]);
    run(&c, "INSERT INTO parent VALUES (1, 'a')").await;
    run(&c, "INSERT INTO child VALUES (10, 1)").await;
    run(&c, "TRUNCATE TABLE Parent CASCADE").await;
    assert_eq!(rows(&c, "SELECT (SELECT count(*) FROM parent), (SELECT count(*) FROM child)").await, ["0|0"]);

    // Self-references: a key still referenced by a row that stays is refused;
    // rows that go together go, by DELETE as by TRUNCATE.
    assert_eq!(code(&c, "DELETE FROM sr WHERE id = 1").await, "23503");
    run(&c, "DELETE FROM sr WHERE id IN (2, 3)").await;
    assert_eq!(rows(&c, "SELECT id FROM sr").await, ["1"]);
    run(&c, "INSERT INTO sr VALUES (2, 1), (3, 2)").await;
    run(&c, "DELETE FROM sr").await;
    assert_eq!(rows(&c, "SELECT count(*) FROM sr").await, ["0"]);
    assert_eq!(code(&c, "DELETE FROM rr WHERE id = 2").await, "23503");
    run(&c, "TRUNCATE rr").await;
    assert_eq!(rows(&c, "SELECT count(*) FROM rr").await, ["0"]);

    assert_eq!(code(&c, "TRUNCATE nosuch").await, "42P01");
    assert_eq!(code(&c, "TRUNCATE ONLY parent *").await, "42601");
}

/// `RESTART IDENTITY` starts the tables' own sequences again, `CONTINUE
/// IDENTITY` (the default) does not, and both roll back: a restart undone
/// never hands out a value a restored row holds, and one inside the block
/// starts at 1 for the block's own inserts.
#[tokio::test(flavor = "multi_thread")]
async fn identities_restart_and_roll_back() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    run(&c, "CREATE TABLE s (id SERIAL PRIMARY KEY, v TEXT)").await;
    run(&c, "INSERT INTO s (v) VALUES ('x'), ('y'), ('z')").await;
    run(&c, "TRUNCATE s").await;
    assert_eq!(rows(&c, "INSERT INTO s (v) VALUES ('continued') RETURNING id").await, ["4"]);
    run(&c, "TRUNCATE s RESTART IDENTITY").await;
    assert_eq!(rows(&c, "INSERT INTO s (v) VALUES ('restarted') RETURNING id").await, ["1"]);
    run(&c, "INSERT INTO s (v) VALUES ('two'), ('three')").await;

    run(&c, "BEGIN").await;
    run(&c, "TRUNCATE s RESTART IDENTITY").await;
    assert_eq!(rows(&c, "INSERT INTO s (v) VALUES ('in the block') RETURNING id").await, ["1"]);
    run(&c, "ROLLBACK").await;
    assert_eq!(rows(&c, "INSERT INTO s (v) VALUES ('after rollback') RETURNING id").await, ["4"]);
    assert_eq!(
        rows(&c, "SELECT id, v FROM s ORDER BY id").await,
        ["1|restarted", "2|two", "3|three", "4|after rollback"]
    );

    run(&c, "BEGIN").await;
    run(&c, "TRUNCATE s RESTART IDENTITY").await;
    assert_eq!(rows(&c, "INSERT INTO s (v) VALUES ('fixture') RETURNING id").await, ["1"]);
    run(&c, "COMMIT").await;
    assert_eq!(rows(&c, "INSERT INTO s (v) VALUES ('after commit') RETURNING id").await, ["2"]);
    assert_eq!(rows(&c, "SELECT id, v FROM s ORDER BY id").await, ["1|fixture", "2|after commit"]);

    // A plain rollback brings the rows back.
    run(&c, "BEGIN").await;
    run(&c, "TRUNCATE s").await;
    assert_eq!(rows(&c, "SELECT count(*) FROM s").await, ["0"]);
    run(&c, "ROLLBACK").await;
    assert_eq!(rows(&c, "SELECT count(*) FROM s").await, ["2"]);
}

/// The extended protocol (how an ORM sends it), a read-only block (25006),
/// a partitioned table, and the ACCESS EXCLUSIVE lock: a reader's open
/// transaction holds the TRUNCATE off, and a refused TRUNCATE of several
/// tables empties none of them.
#[tokio::test(flavor = "multi_thread")]
async fn drivers_read_only_blocks_partitions_and_locks() {
    let root = std::env::temp_dir().join(format!("truncate_compat_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let server = TestServer::start_file_backed(&root.to_string_lossy()).await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE a (id INT PRIMARY KEY)",
        "CREATE TABLE b (id INT PRIMARY KEY)",
        "CREATE TABLE p (k INT NOT NULL, v TEXT) PARTITION BY HASH (k) PARTITIONS 4",
        "INSERT INTO a VALUES (1), (2)",
        "INSERT INTO b VALUES (1)",
        "INSERT INTO p VALUES (1, 'x'), (2, 'y'), (3, 'z'), (4, 'w')",
    ] {
        run(&c, sql).await;
    }

    let mut x = sqlx::PgConnection::connect(&server.url()).await.expect("sqlx");
    let done = sqlx::query("TRUNCATE p").execute(&mut x).await.expect("TRUNCATE over the extended protocol");
    assert_eq!(done.rows_affected(), 0);
    let left: i64 = sqlx::query("SELECT count(*) FROM p").fetch_one(&mut x).await.expect("count").get(0);
    assert_eq!(left, 0, "the partitioned table kept rows");

    run(&c, "BEGIN READ ONLY").await;
    assert_eq!(code(&c, "TRUNCATE a").await, "25006");
    run(&c, "ROLLBACK").await;

    // A reader holds ACCESS SHARE on b until its block ends.
    x.execute("BEGIN").await.expect("begin");
    x.execute("SELECT count(*) FROM b").await.expect("read b");
    run(&c, "SET lock_timeout = '300ms'").await;
    assert_eq!(code(&c, "TRUNCATE a, b").await, "55P03");
    assert_eq!(rows(&c, "SELECT count(*) FROM a").await, ["2"], "a was emptied by a TRUNCATE that failed");
    x.execute("COMMIT").await.expect("commit");
    run(&c, "TRUNCATE a, b").await;
    assert_eq!(rows(&c, "SELECT (SELECT count(*) FROM a), (SELECT count(*) FROM b)").await, ["0|0"]);
    x.execute(AssertSqlSafe("INSERT INTO a VALUES (5)")).await.expect("a after the TRUNCATE");
    drop(x);
    drop(c);
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}
