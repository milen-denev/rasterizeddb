//! `ALTER COLUMN … TYPE` between types: refused for every pair ("would
//! rewrite every row … not supported yet"), so `id integer` to `bigint`, the
//! most common migration there is, could not run. Every row is now rewritten
//! with its value converted as PostgreSQL's assignment cast converts it
//! (`executor::retype_by_cast`), or the statement refused as PostgreSQL
//! refuses it. Every expected answer is PostgreSQL 17's
//! (`.claude/reviews/atype/p1.sql`, 146 pairs, and `p2.sql`).

use rasterizeddb_compat::TestServer;
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

/// Each pair converts its values as PostgreSQL does (an integer from a float
/// rounds half to even, from NUMERIC half away from zero), keeps NULL, and
/// refuses what PostgreSQL refuses: a pair with no assignment cast (42804),
/// a value out of range (22003) or too long (22001). A refused change
/// leaves the column and its values as they were.
#[tokio::test(flavor = "multi_thread")]
async fn values_convert_as_postgresql_converts_them() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    let cases: &[(&str, &[&str], &str, &[&str])] = &[
        ("integer", &["1", "-2", "NULL", "70000"], "bigint", &["1", "-2", "NULL", "70000"]),
        ("smallint", &["1", "-2", "NULL", "32767"], "numeric(6,1)", &["1.0", "-2.0", "NULL", "32767.0"]),
        ("real", &["1.5", "-2.5", "NULL", "2.5"], "integer", &["2", "-2", "NULL", "2"]),
        ("numeric(10,2)", &["1.25", "-2.5", "NULL", "3.75"], "integer", &["1", "-3", "NULL", "4"]),
        ("numeric(10,2)", &["1.25", "-2.5", "NULL", "3.75"], "numeric(6,1)", &["1.3", "-2.5", "NULL", "3.8"]),
        ("double precision", &["1.5", "-2.5", "NULL", "0.125"], "numeric", &["1.5", "-2.5", "NULL", "0.125"]),
        ("bigint", &["1", "-2", "NULL", "5000000000"], "real", &["1", "-2", "NULL", "5e+09"]),
        ("integer", &["1", "-2", "NULL", "70000"], "text", &["1", "-2", "NULL", "70000"]),
        ("boolean", &["true", "false", "NULL", "true"], "varchar(5)", &["true", "false", "NULL", "true"]),
        ("date", &["'2024-01-02'", "'1999-12-31'", "NULL", "'2000-02-29'"], "timestamp", &[
            "2024-01-02 00:00:00",
            "1999-12-31 00:00:00",
            "NULL",
            "2000-02-29 00:00:00",
        ]),
        ("timestamp", &["'2024-01-02 10:30:00'", "'1999-12-31 23:59:59.5'", "NULL", "'2000-02-29 00:00:00'"], "date", &[
            "2024-01-02",
            "1999-12-31",
            "NULL",
            "2000-02-29",
        ]),
    ];
    for (i, (from, values, to, want)) in cases.iter().enumerate() {
        let t = format!("v{i}");
        run(&c, &format!("CREATE TABLE {t} (id INT PRIMARY KEY, c {from})")).await;
        let tuples: Vec<String> = values.iter().enumerate().map(|(k, v)| format!("({k}, {v})")).collect();
        run(&c, &format!("INSERT INTO {t} VALUES {}", tuples.join(", "))).await;
        run(&c, &format!("ALTER TABLE {t} ALTER COLUMN c TYPE {to}")).await;
        assert_eq!(rows(&c, &format!("SELECT c::text FROM {t} ORDER BY id")).await, *want, "{from} -> {to}");
    }

    let refused: &[(&str, &str, &str, &str)] = &[
        ("text", "'12'", "integer", "42804"),
        ("integer", "1", "boolean", "42804"),
        ("integer", "70000", "smallint", "22003"),
        ("bigint", "5000000000", "numeric(6,1)", "22003"),
        ("integer", "123", "varchar(2)", "22001"),
    ];
    for (i, (from, value, to, sqlstate)) in refused.iter().enumerate() {
        let t = format!("r{i}");
        run(&c, &format!("CREATE TABLE {t} (id INT PRIMARY KEY, c {from})")).await;
        run(&c, &format!("INSERT INTO {t} VALUES (1, {value})")).await;
        assert_eq!(code(&c, &format!("ALTER TABLE {t} ALTER COLUMN c TYPE {to}")).await, *sqlstate, "{from} -> {to}");
        let kept = rows(&c, &format!("SELECT c::text FROM {t}")).await;
        assert_eq!(kept, [value.trim_matches('\'').to_string()], "{from} -> {to}: the refused change moved the value");
    }
}

/// A key widened under a foreign key, from either side, still references
/// what it did and is still enforced; a serial column keeps counting; an
/// index on the column answers by the new type. What the conversion would
/// break is refused: two values that become one key (23505), a CHECK some
/// converted row fails (23514), a lossy change to a foreign key's column.
/// All of it survives a restart.
#[tokio::test(flavor = "multi_thread")]
async fn keys_checks_indexes_and_a_restart() {
    let root = std::env::temp_dir().join(format!("alter_column_type_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let root_text = root.to_string_lossy().into_owned();
    let mut server = TestServer::start_file_backed(&root_text).await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE par (id INT PRIMARY KEY, v TEXT)",
        "CREATE TABLE chi (id SERIAL PRIMARY KEY, pid INT NOT NULL REFERENCES par(id), n INT DEFAULT 7)",
        "INSERT INTO par VALUES (1, 'a'), (2, 'b'), (70000, 'c')",
        "INSERT INTO chi (pid) VALUES (1), (2), (70000)",
        "ALTER TABLE par ALTER COLUMN id TYPE bigint",
        "ALTER TABLE chi ALTER COLUMN pid TYPE bigint",
        "ALTER TABLE chi ALTER COLUMN id TYPE bigint",
        "CREATE TABLE ix (id INT PRIMARY KEY, k INT)",
        "CREATE INDEX ix_k ON ix (k)",
        "INSERT INTO ix VALUES (1, 10), (2, 20), (3, 70000)",
        "ALTER TABLE ix ALTER COLUMN k TYPE bigint",
        "INSERT INTO ix VALUES (4, 6000000000)",
    ] {
        run(&c, sql).await;
    }
    assert_eq!(rows(&c, "INSERT INTO chi (pid) VALUES (2) RETURNING id, pid, n").await, ["4|2|7"]);
    assert_eq!(code(&c, "INSERT INTO chi (pid) VALUES (99)").await, "23503");
    assert_eq!(code(&c, "DELETE FROM par WHERE id = 1").await, "23503");
    assert_eq!(code(&c, "INSERT INTO chi (pid, n) VALUES (NULL, 1)").await, "23502");
    assert_eq!(code(&c, "ALTER TABLE chi ALTER COLUMN pid TYPE numeric").await, "0A000");
    run(&c, "INSERT INTO par VALUES (5000000000, 'big')").await;

    run(&c, "CREATE TABLE u (id INT PRIMARY KEY, r REAL UNIQUE)").await;
    run(&c, "INSERT INTO u VALUES (1, 1.4), (2, 1.2), (3, 2.6)").await;
    assert_eq!(code(&c, "ALTER TABLE u ALTER COLUMN r TYPE integer").await, "23505");
    assert_eq!(rows(&c, "SELECT r FROM u ORDER BY id").await, ["1.4", "1.2", "2.6"]);
    run(&c, "CREATE TABLE ck (id INT PRIMARY KEY, x NUMERIC(5,2) CHECK (x <> 3))").await;
    run(&c, "INSERT INTO ck VALUES (1, 2.5), (2, 3.4)").await;
    assert_eq!(code(&c, "ALTER TABLE ck ALTER COLUMN x TYPE integer").await, "23514");
    assert_eq!(rows(&c, "SELECT x FROM ck ORDER BY id").await, ["2.50", "3.40"]);

    drop(c);
    for pass in ["before", "after a restart"] {
        assert_eq!(
            rows(&c_of(&server).await, "SELECT c.id, c.pid, p.v FROM chi c JOIN par p ON p.id = c.pid ORDER BY c.id").await,
            ["1|1|a", "2|2|b", "3|70000|c", "4|2|b"],
            "{pass}"
        );
        let c = c_of(&server).await;
        assert_eq!(rows(&c, "SELECT v FROM par WHERE id = 5000000000").await, ["big"], "{pass}");
        assert_eq!(rows(&c, "SELECT id FROM ix WHERE k = 70000").await, ["3"], "{pass}: the index");
        assert_eq!(rows(&c, "SELECT id FROM ix WHERE k = 6000000000").await, ["4"], "{pass}: the index");
        assert_eq!(
            rows(
                &c,
                "SELECT table_name, column_name, data_type FROM information_schema.columns \
                 WHERE (table_name, column_name) IN (('par', 'id'), ('chi', 'pid'), ('ix', 'k')) ORDER BY 1"
            )
            .await,
            ["chi|pid|bigint", "ix|k|bigint", "par|id|bigint"],
            "{pass}"
        );
        if pass == "before" {
            drop(c);
            drop(server);
            server = TestServer::start_file_backed(&root_text).await.expect("restart");
        }
    }
    assert_eq!(rows(&c_of(&server).await, "INSERT INTO chi (pid) VALUES (70000) RETURNING id").await, ["5"]);
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}

async fn c_of(server: &TestServer) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    c
}
