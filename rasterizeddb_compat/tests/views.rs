//! `CREATE VIEW`, `CREATE MATERIALIZED VIEW` and the rest, as PostgreSQL 17
//! answers them.
//!
//! Every statement here was "syntax error or unsupported query". A view is
//! read as the derived table its query is, wherever a relation is read; its
//! definition is a row written in the statement's transaction, so a block's
//! ROLLBACK takes it back. Expected answers and codes are PostgreSQL 17's
//! (`.claude/reviews/pgdiff`, live side by side).

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};
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

async fn setup(c: &tokio_postgres::Client) {
    c.batch_execute(
        "CREATE TABLE vt (id INT PRIMARY KEY, name TEXT, amount INT); \
         INSERT INTO vt VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30); \
         CREATE TABLE vu (id INT PRIMARY KEY, vt_id INT, tag TEXT); \
         INSERT INTO vu VALUES (1, 1, 'x'), (2, 1, 'y'), (3, 3, 'z')",
    )
    .await
    .expect("setup");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_view_is_read_wherever_a_relation_is() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    setup(&c).await;
    c.batch_execute(
        "CREATE VIEW v1 AS SELECT id, name, amount * 2 AS dbl FROM vt WHERE amount > 10; \
         CREATE VIEW v2 (k, label) AS SELECT id, name FROM v1; \
         CREATE VIEW v3 AS SELECT vt.name, count(vu.id) AS n FROM vt LEFT JOIN vu ON vu.vt_id = vt.id GROUP BY vt.name",
    )
    .await
    .expect("views");
    let cases: &[(&str, &[&str])] = &[
        ("SELECT * FROM v1 ORDER BY id", &["2|b|40", "3|c|60"]),
        ("SELECT name FROM public.v1 WHERE dbl = 60", &["c"]),
        ("SELECT v1.name, vu.tag FROM v1 JOIN vu ON vu.vt_id = v1.id ORDER BY 1, 2", &["c|z"]),
        ("SELECT count(*) FROM v1 x WHERE x.id > 0", &["2"]),
        ("SELECT * FROM v2 ORDER BY k", &["2|b", "3|c"]),
        ("SELECT k FROM v2 WHERE k IN (SELECT id FROM v1 WHERE dbl > 50)", &["3"]),
        ("SELECT * FROM v3 ORDER BY name", &["a|2", "b|0", "c|1"]),
        // Inside a CTE body: the read is the statement's own, not a scope
        // of the catalogue lookup's (which once ended it: no rows).
        ("WITH q AS (SELECT * FROM v2) SELECT label FROM q ORDER BY 1", &["b", "c"]),
        ("WITH q AS (SELECT * FROM v1) SELECT count(*) FROM q", &["2"]),
        // A CTE of the view's name is the CTE.
        ("WITH v1 AS (SELECT 7 AS id) SELECT id FROM v1", &["7"]),
        ("SELECT (SELECT max(dbl) FROM v1)", &["60"]),
    ];
    for (sql, expected) in cases {
        assert_eq!(rows(&c, sql).await, *expected, "{sql}");
    }
    // A view reads the table as it is when it is read.
    c.batch_execute("INSERT INTO vt VALUES (4, 'd', 40)").await.expect("insert");
    assert_eq!(rows(&c, "SELECT count(*) FROM v2").await, ["3"]);

    // The extended protocol describes and runs the same statement.
    let pool = server.connect_sqlx().await.expect("sqlx");
    let got: Vec<(String, i32)> = sqlx::query("SELECT label, k FROM v2 WHERE k > $1 ORDER BY k")
        .bind(2)
        .fetch_all(&pool)
        .await
        .expect("bound")
        .iter()
        .map(|r| (r.get::<String, _>("label"), r.get::<i32, _>("k")))
        .collect();
    assert_eq!(got, [("c".to_string(), 3), ("d".to_string(), 4)]);
    let described = c.prepare("SELECT * FROM v1").await.expect("prepare");
    let names: Vec<&str> = described.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, ["id", "name", "dbl"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn view_ddl_answers_as_postgresql_does() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    setup(&c).await;
    c.batch_execute(
        "CREATE VIEW v1 AS SELECT id, name, amount * 2 AS dbl FROM vt; \
         CREATE VIEW v3 AS SELECT name, count(*) AS n FROM vt GROUP BY name; \
         CREATE VIEW v4 AS SELECT * FROM v3",
    )
    .await
    .expect("views");
    for (sql, expected) in [
        ("CREATE VIEW v1 AS SELECT 1", "42P07"),
        ("CREATE TABLE v1 (a INT)", "42P07"),
        ("CREATE VIEW vt AS SELECT 1", "42P07"),
        ("CREATE OR REPLACE VIEW vt AS SELECT 1", "42809"),
        ("CREATE OR REPLACE VIEW v1 AS SELECT id FROM vt", "42P16"),
        ("CREATE OR REPLACE VIEW v1 AS SELECT id, name AS other, 1 AS dbl FROM vt", "42P16"),
        ("CREATE VIEW bad AS SELECT * FROM nosuch", "42P01"),
        ("CREATE VIEW bad (a, b) AS SELECT 1", "42601"),
        ("CREATE VIEW bad AS SELECT 1 AS a, 2 AS a", "42701"),
        ("CREATE VIEW bad AS SELECT 1 WITH CHECK OPTION", "0A000"),
        ("INSERT INTO v3 VALUES ('q', 1)", "55000"),
        ("UPDATE v3 SET n = 1", "55000"),
        ("DELETE FROM v3", "55000"),
        ("DROP VIEW v3", "2BP01"),
        ("DROP TABLE vt", "2BP01"),
        ("DROP TABLE v3", "42809"),
        ("ALTER TABLE v3 ADD COLUMN z INT", "42809"),
        ("DROP VIEW nosuch", "42P01"),
        ("DROP VIEW vt", "42809"),
        ("DROP MATERIALIZED VIEW v1", "42809"),
        ("REFRESH MATERIALIZED VIEW v1", "0A000"),
    ] {
        assert_eq!(code(&c, sql).await, expected, "{sql}");
    }
    // OR REPLACE may add columns after the ones it had.
    c.batch_execute("CREATE OR REPLACE VIEW v1 AS SELECT id, name, amount * 3 AS dbl, 'new' AS extra FROM vt")
        .await
        .expect("replace");
    assert_eq!(rows(&c, "SELECT * FROM v1 WHERE id = 1").await, ["1|a|30|new"]);
    c.batch_execute("DROP VIEW IF EXISTS nosuch").await.expect("if exists");
    // CASCADE takes the dependents with it.
    c.batch_execute("DROP VIEW v3 CASCADE").await.expect("cascade");
    assert_eq!(code(&c, "SELECT * FROM v4").await, "42P01");
    // A renamed view is read under its new name; a renamed table's views
    // follow it.
    c.batch_execute("CREATE VIEW v5 AS SELECT id FROM v1 WHERE id < 3; ALTER VIEW v1 RENAME TO v1b")
        .await
        .expect("rename view");
    assert_eq!(rows(&c, "SELECT count(*) FROM v5").await, ["2"]);
    c.batch_execute("ALTER TABLE vt RENAME TO vt2").await.expect("rename table");
    assert_eq!(rows(&c, "SELECT count(*) FROM v5").await, ["2"]);
    assert_eq!(rows(&c, "SELECT name FROM v1b WHERE id = 3").await, ["c"]);
    // DROP TABLE … CASCADE drops the views that read it.
    c.batch_execute("DROP TABLE vt2 CASCADE").await.expect("drop cascade");
    assert_eq!(code(&c, "SELECT * FROM v5").await, "42P01");
    assert_eq!(code(&c, "SELECT * FROM v1b").await, "42P01");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_view_is_transactional_and_listed_in_the_catalogue() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    let (c2, conn2) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect 2");
    tokio::spawn(conn2);
    setup(&c).await;
    c.batch_execute("BEGIN; CREATE VIEW vb AS SELECT 1 AS one").await.expect("in block");
    assert_eq!(rows(&c, "SELECT * FROM vb").await, ["1"]);
    // Not another session's until it commits.
    assert_eq!(code(&c2, "SELECT * FROM vb").await, "42P01");
    c.batch_execute("ROLLBACK").await.expect("rollback");
    assert_eq!(code(&c, "SELECT * FROM vb").await, "42P01");
    c.batch_execute("BEGIN; CREATE VIEW vb AS SELECT 1 AS one; COMMIT").await.expect("commit");
    assert_eq!(rows(&c2, "SELECT * FROM vb").await, ["1"]);
    c.batch_execute("BEGIN; DROP VIEW vb; ROLLBACK").await.expect("drop rolled back");
    assert_eq!(rows(&c, "SELECT * FROM vb").await, ["1"]);

    c.batch_execute(
        "CREATE VIEW v1 AS SELECT id, name, amount * 2 AS dbl FROM vt; CREATE MATERIALIZED VIEW mv AS SELECT id FROM vt",
    )
    .await
    .expect("views");
    for (sql, expected) in [
        (
            "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' ORDER BY 1",
            &["v1|VIEW", "vb|VIEW", "vt|BASE TABLE", "vu|BASE TABLE"][..],
        ),
        ("SELECT table_name, check_option FROM information_schema.views ORDER BY 1", &["v1|NONE", "vb|NONE"]),
        ("SELECT viewname FROM pg_views ORDER BY 1", &["v1", "vb"]),
        ("SELECT matviewname, ispopulated FROM pg_matviews", &["mv|t"]),
        ("SELECT tablename FROM pg_tables ORDER BY 1", &["vt", "vu"]),
        (
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'v1' ORDER BY ordinal_position",
            &["id|integer", "name|text", "dbl|integer"],
        ),
        ("SELECT relname, relkind FROM pg_class WHERE relname IN ('vt', 'v1', 'mv') ORDER BY 1", &["mv|m", "v1|v", "vt|r"]),
    ] {
        assert_eq!(rows(&c, sql).await, expected, "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_materialized_view_holds_its_rows_until_refreshed() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    setup(&c).await;
    c.batch_execute("CREATE VIEW big AS SELECT * FROM vt WHERE amount >= 20").await.expect("view");
    let created = c.simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM big").await.expect("create");
    let tag = created.iter().find_map(|m| match m {
        SimpleQueryMessage::CommandComplete(n) => Some(*n),
        _ => None,
    });
    assert_eq!(tag, Some(2), "CREATE MATERIALIZED VIEW reports its rows as SELECT n");
    assert_eq!(rows(&c, "SELECT * FROM mv ORDER BY id").await, ["2|b", "3|c"]);
    c.batch_execute("INSERT INTO vt VALUES (4, 'd', 40)").await.expect("insert");
    assert_eq!(rows(&c, "SELECT count(*) FROM mv").await, ["2"]);
    c.batch_execute("REFRESH MATERIALIZED VIEW mv").await.expect("refresh");
    assert_eq!(rows(&c, "SELECT * FROM mv ORDER BY id").await, ["2|b", "3|c", "4|d"]);
    // In a block, the refresh is the block's.
    c.batch_execute("BEGIN; DELETE FROM vt WHERE id = 4; REFRESH MATERIALIZED VIEW mv; ROLLBACK").await.expect("block");
    assert_eq!(rows(&c, "SELECT count(*) FROM mv").await, ["3"]);
    c.batch_execute("CREATE MATERIALIZED VIEW mv2 (a, b) AS SELECT id, name FROM vt WITH NO DATA").await.expect("no data");
    assert_eq!(rows(&c, "SELECT count(*) FROM mv2").await, ["0"]);
    c.batch_execute("REFRESH MATERIALIZED VIEW mv2").await.expect("refresh mv2");
    assert_eq!(rows(&c, "SELECT a, b FROM mv2 WHERE a = 4").await, ["4|d"]);
    for (sql, expected) in [
        ("INSERT INTO mv VALUES (9, 'x')", "42809"),
        ("DELETE FROM mv", "42809"),
        ("UPDATE mv SET name = 'q'", "42809"),
        ("DROP VIEW mv", "42809"),
        ("DROP TABLE mv", "42809"),
        ("CREATE MATERIALIZED VIEW mv AS SELECT 1", "42P07"),
        ("REFRESH MATERIALIZED VIEW vt", "0A000"),
        ("REFRESH MATERIALIZED VIEW mv CONCURRENTLY", "42601"),
    ] {
        assert_eq!(code(&c, sql).await, expected, "{sql}");
    }
    c.batch_execute("CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT 1").await.expect("if not exists");
    c.batch_execute("ALTER MATERIALIZED VIEW mv RENAME TO mv3").await.expect("rename");
    assert_eq!(rows(&c, "SELECT count(*) FROM mv3").await, ["3"]);
    c.batch_execute("DROP MATERIALIZED VIEW mv3, mv2").await.expect("drop");
    assert_eq!(code(&c, "SELECT * FROM mv3").await, "42P01");
}

#[tokio::test(flavor = "multi_thread")]
async fn views_survive_a_restart() {
    let root = std::env::temp_dir().join(format!("views_restart_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let root_text = root.to_string_lossy().into_owned();
    let mut server = TestServer::start_file_backed(&root_text).await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(
        "CREATE TABLE vt (id INT PRIMARY KEY, name TEXT, amount INT); \
         INSERT INTO vt VALUES (1, 'a', 10), (2, 'b', 20); \
         CREATE VIEW v1 AS SELECT id, name FROM vt WHERE amount > 10; \
         CREATE MATERIALIZED VIEW mv AS SELECT id FROM vt",
    )
    .await
    .expect("setup");
    drop(pool);
    drop(server);
    server = TestServer::start_file_backed(&root_text).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("reconnect");
    let name: String = sqlx::query("SELECT name FROM v1").fetch_one(&pool).await.expect("view").get(0);
    assert_eq!(name, "b");
    let n: i64 = sqlx::query("SELECT count(*) FROM mv").fetch_one(&pool).await.expect("matview").get(0);
    assert_eq!(n, 2);
    let e = pool.execute(AssertSqlSafe("INSERT INTO mv VALUES (5)")).await.expect_err("still a matview");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("42809"));
    drop(pool);
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}

/// Inside a block, and in a statement batch (an implicit block), `DROP
/// TABLE` is deferred to the commit before the statement path runs: the view
/// checks must hold there too. They did not, so `DROP TABLE` of a table a
/// view reads went through in a block.
#[tokio::test(flavor = "multi_thread")]
async fn view_checks_hold_inside_a_block_and_a_batch() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    setup(&c).await;
    c.batch_execute("CREATE VIEW v AS SELECT id FROM vt").await.expect("view");
    assert_eq!(code(&c, "BEGIN; DROP TABLE vt").await, "2BP01");
    c.batch_execute("ROLLBACK").await.expect("rollback");
    assert_eq!(code(&c, "BEGIN; DROP TABLE v").await, "42809");
    c.batch_execute("ROLLBACK").await.expect("rollback");
    assert_eq!(code(&c, "DROP TABLE vt; SELECT 1").await, "2BP01");
    assert_eq!(rows(&c, "SELECT count(*) FROM v").await, ["3"]);
    // CASCADE in a block: the view goes, and comes back with a ROLLBACK.
    c.batch_execute("BEGIN; DROP TABLE vt CASCADE; ROLLBACK").await.expect("cascade rolled back");
    assert_eq!(rows(&c, "SELECT count(*) FROM v").await, ["3"]);
    c.batch_execute("BEGIN; DROP TABLE vt CASCADE; COMMIT").await.expect("cascade");
    assert_eq!(code(&c, "SELECT * FROM v").await, "42P01");
}
