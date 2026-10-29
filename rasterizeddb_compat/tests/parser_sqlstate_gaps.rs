//! Parser and SQLSTATE gaps, each against PostgreSQL 17's answer.
//!
//! - A number's exponent was split from it: `1e20 * 1e-10` read "'1e' is
//!   neither a column nor a value" (XX000) in VALUES, and `f > 1e-10` asked
//!   for a column `e` in a WHERE.
//! - A name that is no column was 42601 ("Column 'x' not found in schema",
//!   "unknown column or unsupported keyword"), where PostgreSQL says 42703;
//!   an ORM tells a stale schema by that code.
//! - Two CHECKs with one written name were both accepted, and the second
//!   silently replaced the first when the table was read back (42710 in
//!   PostgreSQL); a column CHECK lost the name written for it.
//! - `EXCLUDED.col` of a column the INSERT does not write was XX000; it is
//!   the column's DEFAULT, or NULL.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

fn code(e: &sqlx::Error) -> String {
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| e.to_string())
}

async fn run(pool: &sqlx::PgPool, sql: &str) {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn refused(pool: &sqlx::PgPool, sql: &str) -> String {
    let e = pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string()))).await.expect_err(sql);
    code(&e)
}

async fn ids(pool: &sqlx::PgPool, sql: &str) -> Vec<i32> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.get::<i32, _>(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_number_keeps_its_exponent() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    run(&pool, "CREATE TABLE e1 (id INT PRIMARY KEY, f DOUBLE PRECISION, n NUMERIC)").await;
    run(&pool, "INSERT INTO e1 VALUES (1, 1e20 * 1e-10, 1e3), (2, 2.5E+3, 1e-5)").await;
    let r = sqlx::query("SELECT f, n::text FROM e1 WHERE id = 1").fetch_one(&pool).await.expect("row");
    assert_eq!(r.get::<f64, _>(0), 1e10);
    assert_eq!(r.get::<String, _>(1), "1000");
    for (sql, want) in [
        ("SELECT id FROM e1 WHERE f = 1e20 * 1e-10", vec![1]),
        ("SELECT id FROM e1 WHERE f > 1e-10 ORDER BY id", vec![1, 2]),
        ("SELECT id FROM e1 WHERE f < 2.6e+3", vec![2]),
        ("SELECT id FROM e1 WHERE n < 1E-4", vec![2]),
        ("SELECT id FROM e1 WHERE f + 1e-3 > 2500", vec![1, 2]),
    ] {
        assert_eq!(ids(&pool, sql).await, want, "{sql}");
    }
    let f: f64 = sqlx::query_scalar("SELECT f * 1e-3 FROM e1 WHERE id = 2").fetch_one(&pool).await.expect("expr");
    assert_eq!(f, 2.5);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_name_that_is_no_column_is_42703() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    run(&pool, "CREATE TABLE c (id INT PRIMARY KEY, n INT, k INT UNIQUE)").await;
    run(&pool, "INSERT INTO c VALUES (1, 1, 1)").await;
    for sql in [
        "SELECT nosuch FROM c",
        "SELECT id, nosuch FROM c",
        "SELECT c.nosuch FROM c",
        "SELECT id FROM c WHERE nosuch = 1",
        "SELECT id FROM c WHERE n = 1 AND nosuch > 2",
        "SELECT id FROM c ORDER BY nosuch",
        "SELECT count(*) FROM c GROUP BY nosuch",
        "UPDATE c SET n = nosuch + 1",
        "INSERT INTO c VALUES (2, 2, 1) ON CONFLICT (k) DO UPDATE SET n = EXCLUDED.nosuch",
    ] {
        assert_eq!(refused(&pool, sql).await, "42703", "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn check_names_are_unique_and_kept() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    for sql in [
        "CREATE TABLE d2 (id INT, a INT, CONSTRAINT c1 CHECK (a > 0), CONSTRAINT c1 CHECK (a < 10))",
        "CREATE TABLE d3 (id INT, a INT CONSTRAINT c2 CHECK (a > 0), b INT CONSTRAINT c2 CHECK (b > 0))",
        "CREATE TABLE d6 (id INT, a INT CONSTRAINT c3 CHECK (a > 0), CONSTRAINT c3 CHECK (a < 10))",
    ] {
        assert_eq!(refused(&pool, sql).await, "42710", "{sql}");
    }
    // Nothing was created.
    assert_eq!(refused(&pool, "SELECT * FROM d2").await, "42P01");

    // A column CHECK answers to its written name, and is dropped by it.
    run(&pool, "CREATE TABLE d4 (id INT, a INT CONSTRAINT a_pos CHECK (a > 0), CONSTRAINT a_small CHECK (a < 10))").await;
    let e = pool.execute("INSERT INTO d4 VALUES (1, 0)").await.expect_err("a_pos");
    assert!(e.to_string().contains("\"a_pos\""), "{e}");
    let e = pool.execute("INSERT INTO d4 VALUES (1, 11)").await.expect_err("a_small");
    assert!(e.to_string().contains("\"a_small\""), "{e}");
    run(&pool, "ALTER TABLE d4 DROP CONSTRAINT a_pos").await;
    run(&pool, "INSERT INTO d4 VALUES (1, 0)").await;

    // A derived name meeting a written one is numbered on, and both hold.
    run(&pool, "CREATE TABLE d5 (id INT, a INT CHECK (a > 0), CONSTRAINT d5_a_check CHECK (a < 10))").await;
    let e = pool.execute("INSERT INTO d5 VALUES (1, 0)").await.expect_err("derived");
    assert!(e.to_string().contains("\"d5_a_check1\""), "{e}");
    let e = pool.execute("INSERT INTO d5 VALUES (1, 11)").await.expect_err("written");
    assert!(e.to_string().contains("\"d5_a_check\""), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn excluded_of_an_unwritten_column_is_its_default_or_null() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    run(&pool, "CREATE TABLE u1 (id INT PRIMARY KEY, a INT DEFAULT 7, b INT)").await;
    run(&pool, "INSERT INTO u1 (id, a, b) VALUES (1, 1, 1)").await;
    run(&pool, "INSERT INTO u1 (id, b) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET b = EXCLUDED.a").await;
    let r = sqlx::query("SELECT a, b FROM u1 WHERE id = 1").fetch_one(&pool).await.expect("row");
    assert_eq!((r.get::<Option<i32>, _>(0), r.get::<Option<i32>, _>(1)), (Some(1), Some(7)));
    run(&pool, "INSERT INTO u1 (id, a) VALUES (1, 9) ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.b").await;
    let r = sqlx::query("SELECT a, b FROM u1 WHERE id = 1").fetch_one(&pool).await.expect("row");
    assert_eq!((r.get::<Option<i32>, _>(0), r.get::<Option<i32>, _>(1)), (None, Some(7)));

    // A sequence's next value is not the one the INSERT drew: refused by
    // name rather than computed again.
    run(&pool, "CREATE TABLE u2 (id SERIAL PRIMARY KEY, k INT UNIQUE, v INT)").await;
    run(&pool, "INSERT INTO u2 (k, v) VALUES (1, 1)").await;
    assert_eq!(
        refused(&pool, "INSERT INTO u2 (k, v) VALUES (1, 2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.id").await,
        "0A000"
    );
}

/// A column name with a space in it is refused by name. It failed as
/// "column type 'D\"' is not supported yet" (the name split on its space),
/// and the readers that would query it still split names on whitespace, so
/// creating it would make a column that can be written but not read by name.
#[tokio::test(flavor = "multi_thread")]
async fn a_column_name_with_a_space_is_refused_by_name() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let e = pool.execute(r#"CREATE TABLE qs (id INT PRIMARY KEY, "c d" TEXT)"#).await.expect_err("spaced name");
    assert_eq!(code(&e), "0A000");
    assert!(e.to_string().contains("a column name containing a space"), "{e}");
    assert_eq!(refused(&pool, "SELECT * FROM qs").await, "42P01");
    // A quoted name without a space keeps its case, as before.
    run(&pool, r#"CREATE TABLE qm (id INT PRIMARY KEY, "Mixed" INT)"#).await;
    run(&pool, r#"INSERT INTO qm (id, "Mixed") VALUES (1, 5)"#).await;
    assert_eq!(ids(&pool, r#"SELECT "Mixed" FROM qm"#).await, vec![5]);
}

/// The 42703 rule reaches only a name that is no column: a statement the
/// parser could not read stays a syntax error (42601), and a cast to a type
/// that does not exist is 42704.
#[tokio::test(flavor = "multi_thread")]
async fn syntax_errors_stay_42601() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    run(&pool, "CREATE TABLE t (id INT PRIMARY KEY, x TEXT, v INT)").await;
    run(&pool, "INSERT INTO t VALUES (1, 'a', 10)").await;
    for (sql, want) in [
        ("SELECT id + FROM t", "42601"),
        ("SELECT id +* 2 FROM t", "42601"),
        ("SELECT id, x y z FROM t", "42601"),
        ("SELECT id FROM t WHERE x = 'a' ||", "42601"),
        ("SELECT id FROM t WHERE v BETWEN 1 AND 30", "42601"),
        ("SELECT id FROM t WHERE x ILIKEE 'a'", "42601"),
        ("SELECT id FROM t ODER BY id", "42601"),
        ("SELECT id FROM t WHERE id = 1 ODER BY id", "42601"),
        ("SELECT id FROM t GROUPBY id", "42601"),
        ("SELECT id FROM t GROUP BY id HAVNG id > 1", "42601"),
        ("SELECT id FROM t WHERE id = 1 ANDD v = 2", "42601"),
        ("SELECT id FROM t LIMT 5", "42601"),
        ("SELECT 0x10 FROM t", "42601"),
        ("SELECT id::nosuchtype FROM t", "42704"),
        ("SELECT id FROM t WHERE v::nosuchtype = 1", "42704"),
        // And the names that are no column are still 42703.
        ("SELECT nosuch FROM t", "42703"),
        ("SELECT id FROM t WHERE v = 1 AND nosuch = 1", "42703"),
        ("SELECT id FROM t ORDER BY nosuch", "42703"),
    ] {
        assert_eq!(refused(&pool, sql).await, want, "{sql}");
    }
}

/// `EXCLUDED` of an unwritten column: a constant default is taken however
/// it is spelt (`'(none)'` was refused for its parentheses), a computed one
/// (`CURRENT_TIMESTAMP`) is refused rather than computed again, and `IS
/// [NOT] NULL` over a substituted value is a constant (it was 0A000 "NULL
/// literals are not supported").
#[tokio::test(flavor = "multi_thread")]
async fn excluded_defaults_and_null_tests() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    run(
        &pool,
        "CREATE TABLE ex (id INT PRIMARY KEY, c TEXT DEFAULT '(none)', d TIMESTAMP DEFAULT CURRENT_TIMESTAMP, v TEXT)",
    )
    .await;
    run(&pool, "INSERT INTO ex (id, v) VALUES (1, 'a')").await;
    run(&pool, "INSERT INTO ex (id, v) VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.c").await;
    let v: String = sqlx::query_scalar("SELECT v FROM ex").fetch_one(&pool).await.expect("v");
    assert_eq!(v, "(none)");
    assert_eq!(
        refused(&pool, "INSERT INTO ex (id, v) VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.d::text").await,
        "0A000"
    );
    run(&pool, "INSERT INTO ex (id, v) VALUES (1, NULL) ON CONFLICT (id) DO UPDATE SET v = 'was null' WHERE EXCLUDED.v IS NULL").await;
    run(&pool, "INSERT INTO ex (id, v) VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET v = 'not reached' WHERE EXCLUDED.v IS NULL").await;
    run(&pool, "INSERT INTO ex (id, v) VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET c = 'reached' WHERE EXCLUDED.v IS NOT NULL").await;
    let r = sqlx::query("SELECT v, c FROM ex").fetch_one(&pool).await.expect("row");
    assert_eq!((r.get::<String, _>(0), r.get::<String, _>(1)), ("was null".to_string(), "reached".to_string()));
}
