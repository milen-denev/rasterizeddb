//! `CREATE TEMP TABLE`, as PostgreSQL 17 answers it: a table only its
//! session sees, shadowing a permanent one of its name, gone with the session.
//! It was refused ("syntax error or unsupported query"); the MySQL and SQL
//! Server front ends emulated it with real tables.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};
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

async fn client(server: &TestServer) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn a_temp_table_is_its_sessions_alone() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    let other = client(&server).await;
    c.batch_execute(
        "CREATE TABLE tt (a INT); INSERT INTO tt VALUES (1); \
         CREATE TEMP TABLE tt (a INT, b TEXT); INSERT INTO tt VALUES (5, 'temp')",
    )
    .await
    .expect("setup");
    for (sql, expected) in [
        ("SELECT * FROM tt", &["5|temp"][..]),
        ("SELECT * FROM public.tt", &["1"]),
        ("SELECT * FROM pg_temp.tt", &["5|temp"]),
        ("SELECT t.b FROM tt t", &["temp"]),
    ] {
        assert_eq!(rows(&c, sql).await, expected, "{sql}");
    }
    // Another session sees the permanent table only.
    assert_eq!(rows(&other, "SELECT * FROM tt").await, ["1"]);
    other.batch_execute("CREATE TEMP TABLE tt (q TEXT); INSERT INTO tt VALUES ('mine')").await.expect("other temp");
    assert_eq!(rows(&other, "SELECT * FROM tt").await, ["mine"]);
    assert_eq!(rows(&c, "SELECT * FROM tt").await, ["5|temp"]);

    c.batch_execute("UPDATE tt SET b = 'changed' WHERE a = 5; CREATE TEMP TABLE agg AS SELECT a * 10 AS ten FROM public.tt")
        .await
        .expect("update and ctas");
    assert_eq!(rows(&c, "SELECT b FROM tt").await, ["changed"]);
    assert_eq!(rows(&c, "SELECT ten FROM agg").await, ["10"]);
    assert_eq!(code(&c, "CREATE TEMP TABLE tt (z INT)").await, "42P07");
    c.batch_execute("CREATE TEMP TABLE IF NOT EXISTS tt (z INT)").await.expect("if not exists");
    assert_eq!(code(&c, "CREATE TEMP TABLE public.x (z INT)").await, "42P16");

    // ON COMMIT DELETE ROWS and ON COMMIT DROP.
    c.batch_execute("CREATE TEMP TABLE odr (x INT) ON COMMIT DELETE ROWS").await.expect("odr");
    assert_eq!(rows(&c, "BEGIN; INSERT INTO odr VALUES (1); SELECT count(*) FROM odr").await, ["1"]);
    c.batch_execute("COMMIT").await.expect("commit");
    assert_eq!(rows(&c, "SELECT count(*) FROM odr").await, ["0"]);
    c.batch_execute("BEGIN; CREATE TEMP TABLE od (x INT) ON COMMIT DROP; INSERT INTO od VALUES (1); COMMIT")
        .await
        .expect("on commit drop");
    assert_eq!(code(&c, "SELECT * FROM od").await, "42P01");
    // A rolled-back create leaves nothing.
    c.batch_execute("BEGIN; CREATE TEMP TABLE rb (x INT); ROLLBACK").await.expect("rollback");
    assert_eq!(code(&c, "SELECT * FROM rb").await, "42P01");
    // Dropping the temporary table uncovers the permanent one.
    c.batch_execute("DROP TABLE tt").await.expect("drop temp");
    assert_eq!(rows(&c, "SELECT * FROM tt").await, ["1"]);
    // DISCARD TEMP drops the rest.
    c.batch_execute("DISCARD TEMP").await.expect("discard");
    assert_eq!(code(&c, "SELECT * FROM agg").await, "42P01");

    // A prepared statement over one, through the extended protocol.
    let pool = server.connect_sqlx().await.expect("sqlx");
    let mut conn = pool.acquire().await.expect("connection");
    conn.execute("CREATE TEMP TABLE p (id INT, v TEXT)").await.expect("create");
    sqlx::query("INSERT INTO p VALUES ($1, $2)").bind(1).bind("one").execute(&mut *conn).await.expect("insert");
    let v: String = sqlx::query("SELECT v FROM p WHERE id = $1").bind(1).fetch_one(&mut *conn).await.expect("select").get(0);
    assert_eq!(v, "one");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_temp_table_goes_with_its_session() {
    let server = TestServer::start().await.expect("server");
    let watcher = client(&server).await;
    {
        let c = client(&server).await;
        c.batch_execute("CREATE TEMP TABLE gone (x INT); INSERT INTO gone VALUES (1)").await.expect("temp");
    }
    // Once the session has closed, its table is gone from the engine (the
    // catalogue relations never list it, so ask the engine itself).
    let held = || server.database().table_names().into_iter().filter(|n| n.contains("tmp_") && n.ends_with("_gone")).count();
    let mut left = held();
    for _ in 0..100 {
        if left == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        left = held();
    }
    assert_eq!(left, 0);
    drop(watcher);
}

/// A block's ROLLBACK takes back the tables a statement makes for another:
/// a temporary table's, a materialized view's. Run as internal statements,
/// both survived it.
#[tokio::test(flavor = "multi_thread")]
async fn a_rolled_back_block_takes_back_internal_tables() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute("CREATE TABLE base (a INT); INSERT INTO base VALUES (1)").await.expect("base");
    c.batch_execute("BEGIN; CREATE TEMP TABLE rb (x INT); CREATE MATERIALIZED VIEW mv AS SELECT a FROM base; ROLLBACK")
        .await
        .expect("rolled back");
    assert_eq!(code(&c, "SELECT * FROM rb").await, "42P01");
    assert_eq!(code(&c, "SELECT * FROM mv").await, "42P01");
    let created = server.database().table_names().into_iter().filter(|n| n == "mv" || n.ends_with("_rb")).count();
    assert_eq!(created, 0, "{:?}", server.database().table_names());
    c.batch_execute("CREATE MATERIALIZED VIEW mv AS SELECT a FROM base").await.expect("mv");
    c.batch_execute("BEGIN; DROP MATERIALIZED VIEW mv; ROLLBACK").await.expect("drop rolled back");
    assert_eq!(rows(&c, "SELECT * FROM mv").await, ["1"]);
}
