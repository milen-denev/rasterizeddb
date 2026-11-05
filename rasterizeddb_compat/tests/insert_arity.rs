//! `INSERT … VALUES` whose tuples do not fit their target columns. Every
//! expectation is PostgreSQL 17's (`docker run --rm postgres:17`): 42601 with
//! its own message, in its order (each tuple held to the first one's length,
//! then against the targets), and nothing stored. The row parser used to
//! give up without a reason, so a plain table answered XX000 and a
//! partitioned one blamed its key column (23502 "null value in column
//! \"tenant_id\""), for an import whose locale wrote `204,60`.
//!
//! `INSERT INTO t VALUES (…)` maps the tuple across every column in order,
//! generated ones included, as PostgreSQL does: a generated column's place
//! takes `DEFAULT`. They used to be left out, so the tuple shifted past one.

use rasterizeddb_compat::TestServer;
use tokio_postgres::{Client, SimpleQueryMessage};

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

async fn run(c: &Client, sql: &str) {
    c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn rows(c: &Client, sql: &str) -> Vec<String> {
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

/// The statement's SQLSTATE and message.
async fn refused(c: &Client, sql: &str) -> (String, String) {
    let e = c.simple_query(sql).await.expect_err(sql);
    let db = e.as_db_error().unwrap_or_else(|| panic!("{sql}: not a server error: {e}"));
    (db.code().code().to_string(), db.message().to_string())
}

const MORE_EXPRESSIONS: &str = "INSERT has more expressions than target columns";
const MORE_TARGETS: &str = "INSERT has more target columns than expressions";
const UNEQUAL: &str = "VALUES lists must all be the same length";

/// A file-backed server, which partitioned tables need; removed on drop.
struct OnDisk(std::path::PathBuf);

impl Drop for OnDisk {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn on_disk(tag: &str) -> (TestServer, OnDisk) {
    let root = std::env::temp_dir().join(format!("insert_arity_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let server = TestServer::start_file_backed(&root.to_string_lossy()).await.expect("server");
    (server, OnDisk(root))
}

/// Each misfit, on a plain and a partitioned table, as PostgreSQL 17
/// answers it; a short tuple without a column list is a row whose last
/// columns take their defaults. Nothing refused is stored.
#[tokio::test(flavor = "multi_thread")]
async fn misfit_tuples_are_refused_as_postgresql_refuses_them() {
    let (server, _dir) = on_disk("misfit").await;
    let c = client(&server).await;
    run(&c, "CREATE TABLE t (a INT, b INT, c TEXT)").await;
    run(&c, "CREATE TABLE p (tenant_id BIGINT NOT NULL, b INT, c TEXT) PARTITION BY HASH (tenant_id)").await;
    let cases: &[(&str, &str)] = &[
        ("INSERT INTO t (a, b) VALUES (1, 2, 3)", MORE_EXPRESSIONS),
        ("INSERT INTO t (a, b, c) VALUES (1, 2)", MORE_TARGETS),
        ("INSERT INTO t VALUES (1, 2, 'x', 4)", MORE_EXPRESSIONS),
        ("INSERT INTO t (a, b) VALUES (7, 8), (9, 10, 11)", UNEQUAL),
        // The first tuple is checked against the targets before the second
        // is compared with it.
        ("INSERT INTO t (a, b) VALUES (12, 13, 14), (15, 16)", MORE_EXPRESSIONS),
        ("INSERT INTO t VALUES (17, 18, 'y'), (19, 20, 'z', 21)", UNEQUAL),
        ("INSERT INTO t (a, b) VALUES (22, 23, 24) RETURNING a", MORE_EXPRESSIONS),
        ("INSERT INTO t (a, b) VALUES (25, 26, 27) ON CONFLICT DO NOTHING", MORE_EXPRESSIONS),
        // A locale's decimal comma: one value written as two.
        ("INSERT INTO t (a, b, c) VALUES (1, 204,60, 'x')", MORE_EXPRESSIONS),
        ("INSERT INTO p (tenant_id, b) VALUES (1, 2, 3)", MORE_EXPRESSIONS),
        ("INSERT INTO p VALUES (1, 2, 'x', 4)", MORE_EXPRESSIONS),
        ("INSERT INTO p (tenant_id, b, c) VALUES (1, 2)", MORE_TARGETS),
        ("INSERT INTO p (tenant_id, b) VALUES (2, 3), (4, 5, 6)", UNEQUAL),
    ];
    for (sql, want) in cases {
        assert_eq!(refused(&c, sql).await, ("42601".to_string(), want.to_string()), "{sql}");
    }
    run(&c, "INSERT INTO t VALUES (5, 6)").await;
    // Commas inside a nested expression or a string are not separators.
    run(&c, "INSERT INTO t (a, b, c) VALUES (abs(-7), (8), 'q,r')").await;
    run(&c, "INSERT INTO p (tenant_id, b) VALUES (1, 2), (3, 4)").await;
    assert_eq!(rows(&c, "SELECT a, b, c FROM t ORDER BY a").await, ["5|6|NULL", "7|8|q,r"]);
    assert_eq!(rows(&c, "SELECT tenant_id, b, c FROM p ORDER BY tenant_id").await, ["1|2|NULL", "3|4|NULL"]);
}

/// A generated column has its place in `INSERT INTO t VALUES (…)`, which
/// takes `DEFAULT`; any other value there is PostgreSQL's 428C9, and a
/// tuple one longer than every column is 42601.
#[tokio::test(flavor = "multi_thread")]
async fn a_generated_column_keeps_its_place_in_the_tuple() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE g (a INT, b INT GENERATED ALWAYS AS (a * 2) STORED, c TEXT)").await;
    run(&c, "INSERT INTO g VALUES (2, DEFAULT, 'y')").await;
    run(&c, "INSERT INTO g VALUES (5, DEFAULT, 'v'), (6, DEFAULT, 'u')").await;
    assert_eq!(refused(&c, "INSERT INTO g VALUES (3, 4, 'z')").await.0, "428C9");
    // PostgreSQL reads `'x'` as `b`'s and refuses it as an integer (22P02)
    // before the generated column's rule; refused either way, never stored
    // as `c`.
    let (code, _) = refused(&c, "INSERT INTO g VALUES (1, 'x')").await;
    assert!(code == "428C9" || code == "22P02", "{code}");
    assert_eq!(
        refused(&c, "INSERT INTO g VALUES (4, DEFAULT, 'w', 5)").await,
        ("42601".to_string(), MORE_EXPRESSIONS.to_string())
    );
    assert_eq!(rows(&c, "SELECT a, b, c FROM g ORDER BY a").await, ["2|4|y", "5|10|v", "6|12|u"]);
}

/// A dropped column has no place: the tuple maps onto the columns left.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_column_has_no_place_in_the_tuple() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE d (a INT, b INT, c TEXT)").await;
    run(&c, "ALTER TABLE d DROP COLUMN b").await;
    run(&c, "INSERT INTO d VALUES (1, 'x')").await;
    assert_eq!(
        refused(&c, "INSERT INTO d VALUES (1, 'x', 3)").await,
        ("42601".to_string(), MORE_EXPRESSIONS.to_string())
    );
    assert_eq!(rows(&c, "SELECT a, c FROM d").await, ["1|x"]);
}

/// The same refusal inside an explicit block, which it aborts as
/// PostgreSQL's does, and through the extended protocol.
#[tokio::test(flavor = "multi_thread")]
async fn a_block_and_a_prepared_insert_are_refused_alike() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    run(&c, "CREATE TABLE t (a INT, b INT, c TEXT)").await;
    run(&c, "BEGIN").await;
    run(&c, "INSERT INTO t VALUES (1, 1, 'kept until the abort')").await;
    assert_eq!(refused(&c, "INSERT INTO t (a, b) VALUES (2, 2, 2)").await.0, "42601");
    assert_eq!(refused(&c, "SELECT 1").await.0, "25P02");
    run(&c, "ROLLBACK").await;

    let sql = "INSERT INTO t (a, b) VALUES ($1, $2, $3)";
    let code = match c.prepare(sql).await {
        Err(e) => e.code().map(|s| s.code().to_string()),
        Ok(stmt) => {
            let e = c.execute(&stmt, &[&1i32, &2i32, &3i32]).await.expect_err(sql);
            e.code().map(|s| s.code().to_string())
        }
    };
    assert_eq!(code.as_deref(), Some("42601"), "{sql}");
    assert_eq!(rows(&c, "SELECT count(*) FROM t").await, ["0"]);
}
