//! What an application reads of its session (`current_user`,
//! `current_setting`, `set_config`, `SET`/`SHOW` of a plain parameter), the
//! SQL-level `PREPARE`/`EXECUTE`, `DO` blocks, PostgreSQL's shorthands
//! (`TABLE t`, `AS MATERIALIZED`, `WINDOW w`, `UPDATE … SET (a, b) = …`,
//! `DEFAULT VALUES`, `SELECT … INTO`, `ROW(…)`, `SIMILAR TO`, `= ANY('{…}'::T[])`),
//! the jsonb table functions and the functions beside them. Each was refused
//! or, for `upper('straße')`, `make_interval(days => 3)` and `bytea || bytea`,
//! answered differently from PostgreSQL 17, whose answers these are
//! (`.claude/reviews/pgdiff`).

use rasterizeddb_compat::TestServer;
use tokio_postgres::SimpleQueryMessage;

async fn rows(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(
                (0..r.len())
                    .map(|i| r.get(i).unwrap_or("NULL").to_string())
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            _ => None,
        })
        .collect()
}

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn the_session_reads_back_as_it_was_set() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    let user = rows(&c, "SELECT current_user").await;
    assert_eq!(user.len(), 1);
    assert_eq!(
        rows(&c, "SELECT session_user = current_user, current_database() IS NOT NULL, pg_backend_pid() > 0").await,
        ["t|t|t"]
    );
    assert_eq!(rows(&c, "SELECT current_schemas(true)").await, ["{pg_catalog,public}"]);
    assert_eq!(
        rows(&c, "SELECT current_setting('server_version_num')::int >= 90000").await,
        ["t"]
    );
    // By name, as a client reading the row by column asks for it.
    let row = c
        .query_one("SELECT current_user, current_database()", &[])
        .await
        .expect("extended");
    assert_eq!(row.columns()[0].name(), "current_user");
    assert_eq!(row.columns()[1].name(), "current_database");
    assert_eq!(row.get::<_, String>("current_user"), user[0]);

    let e = c.simple_query("SELECT current_setting('nope')").await.expect_err("missing");
    assert_eq!(e.code().map(|s| s.code()), Some("42704"));
    assert_eq!(rows(&c, "SELECT current_setting('nope', true)").await, ["NULL"]);

    c.batch_execute("SET application_name = 'MyApp'").await.expect("set");
    assert_eq!(rows(&c, "SHOW application_name").await, ["MyApp"]);
    assert_eq!(rows(&c, "SELECT current_setting('application_name')").await, ["MyApp"]);
    assert_eq!(
        rows(&c, "SELECT set_config('myapp.tenant', '42', false)").await,
        ["42"]
    );
    assert_eq!(
        rows(&c, "SELECT current_setting('myapp.tenant')::int + 1").await,
        ["43"]
    );
    // Over the extended protocol too, described as one text column.
    let row = c
        .query_one("SELECT set_config('myapp.tenant', '7', false)", &[])
        .await
        .expect("extended set_config");
    assert_eq!(row.get::<_, String>(0), "7");
    c.batch_execute("SET search_path TO public").await.expect("set");
    assert_eq!(rows(&c, "SHOW search_path").await, ["public"]);
    c.batch_execute("RESET search_path").await.expect("reset");
    assert_eq!(rows(&c, "SHOW search_path").await, ["\"$user\", public"]);
    // `SET LOCAL` lasts its block; a custom parameter stays defined, empty.
    c.batch_execute("BEGIN; SET LOCAL myapp.x = 'a'").await.expect("local");
    assert_eq!(rows(&c, "SHOW myapp.x").await, ["a"]);
    c.batch_execute("COMMIT").await.expect("commit");
    assert_eq!(rows(&c, "SELECT current_setting('myapp.x', true)").await, [""]);
    // A timeout prints in the largest unit that divides it, as PostgreSQL's.
    c.batch_execute("SET statement_timeout = '5s'").await.expect("set");
    assert_eq!(rows(&c, "SHOW statement_timeout").await, ["5s"]);
    c.batch_execute("RESET statement_timeout").await.expect("reset");
}

#[tokio::test(flavor = "multi_thread")]
async fn prepare_execute_and_do() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute("CREATE TABLE pe (id INT PRIMARY KEY, v TEXT); INSERT INTO pe VALUES (1,'a'),(2,'b'),(3,'c')")
        .await
        .expect("setup");
    c.batch_execute("PREPARE byid(int) AS SELECT v FROM pe WHERE id = $1").await.expect("prepare");
    assert_eq!(rows(&c, "EXECUTE byid(2)").await, ["b"]);
    assert_eq!(rows(&c, "EXECUTE byid(3)").await, ["c"]);
    let e = c.simple_query("PREPARE byid AS SELECT 1").await.expect_err("twice");
    assert_eq!(e.code().map(|s| s.code()), Some("42P05"));
    c.batch_execute("DEALLOCATE byid").await.expect("deallocate");
    let e = c.simple_query("EXECUTE byid(1)").await.expect_err("gone");
    assert_eq!(e.code().map(|s| s.code()), Some("26000"));

    // A DO block: variables, IF, dynamic EXECUTE and DDL, as a migration
    // writes one.
    c.batch_execute(
        "DO $$ DECLARE n int := 4; BEGIN \
           IF NOT EXISTS (SELECT 1 FROM pe WHERE id = n) THEN \
             EXECUTE format('INSERT INTO %I VALUES (%s, %L)', 'pe', n, 'd'); \
           END IF; \
           CREATE TABLE pe_log (msg TEXT); \
         END $$",
    )
    .await
    .expect("do");
    assert_eq!(rows(&c, "SELECT v FROM pe WHERE id = 4").await, ["d"]);
    assert_eq!(rows(&c, "SELECT count(*) FROM pe_log").await, ["0"]);
    let e = c
        .simple_query("DO $$ BEGIN RAISE EXCEPTION 'stop %', 1 USING ERRCODE = '22023'; END $$")
        .await
        .expect_err("raise");
    assert_eq!(e.code().map(|s| s.code()), Some("22023"));
}

#[tokio::test(flavor = "multi_thread")]
async fn shorthands_are_what_they_stand_for() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute(
        "CREATE TABLE sh (id INT PRIMARY KEY, g INT, v TEXT DEFAULT 'z'); \
         INSERT INTO sh VALUES (1,1,'a'),(2,1,'b'),(3,2,'c')",
    )
    .await
    .expect("setup");
    assert_eq!(rows(&c, "TABLE sh ORDER BY id LIMIT 1").await, ["1|1|a"]);
    assert_eq!(
        rows(&c, "WITH x AS MATERIALIZED (SELECT id FROM sh WHERE id < 3) SELECT * FROM x ORDER BY id").await,
        ["1", "2"]
    );
    assert_eq!(
        rows(&c, "SELECT id, sum(id) OVER w FROM sh WINDOW w AS (PARTITION BY g ORDER BY id) ORDER BY id").await,
        ["1|1", "2|3", "3|3"]
    );
    let e = c.simple_query("SELECT rank() OVER nope FROM sh").await.expect_err("no window");
    assert_eq!(e.code().map(|s| s.code()), Some("42P20"));
    c.batch_execute("UPDATE sh SET (g, v) = (g + 10, upper(v)) WHERE id = 1").await.expect("row assign");
    c.batch_execute("UPDATE sh SET (g, v) = (SELECT 99, 'q') WHERE id = 2").await.expect("row subquery");
    assert_eq!(rows(&c, "SELECT g, v FROM sh WHERE id < 3 ORDER BY id").await, ["11|A", "99|q"]);
    // An aggregate beside a plain value is one row, even over no rows: split
    // per column, the plain one was read per row ("returned 2 rows").
    c.batch_execute(
        "CREATE TABLE sha (id INT PRIMARY KEY, g INT, v TEXT); INSERT INTO sha VALUES (1, 11, 'A'), (2, 99, 'q'), (3, 2, 'c'); \
         UPDATE sha SET (g, v) = (SELECT max(g), 'm' || count(*) FROM sha WHERE id > 100) WHERE id = 2; \
         UPDATE sha SET (g, v) = (SELECT max(g) AS top, 'top' FROM sha) WHERE id = 3",
    )
    .await
    .expect("aggregate row");
    assert_eq!(rows(&c, "SELECT g, v FROM sha WHERE id > 1 ORDER BY id").await, ["NULL|m0", "11|top"]);
    let e = c.simple_query("UPDATE sh SET (g, v) = (1)").await.expect_err("count");
    assert_eq!(e.code().map(|s| s.code()), Some("42601"));
    c.batch_execute("CREATE TABLE shd (id SERIAL, v TEXT DEFAULT 'z'); INSERT INTO shd DEFAULT VALUES").await.expect("default values");
    assert_eq!(rows(&c, "SELECT id, v FROM shd").await, ["1|z"]);
    c.batch_execute("SELECT id, v INTO TEMP shi FROM sh WHERE id > 1").await.expect("select into");
    assert_eq!(rows(&c, "SELECT count(*) FROM shi").await, ["2"]);
    c.batch_execute("CREATE UNLOGGED TABLE shu (a INT); INSERT INTO shu VALUES (1)").await.expect("unlogged");
    assert_eq!(rows(&c, "SELECT * FROM shu").await, ["1"]);
    assert_eq!(rows(&c, "SELECT ROW(1,2) < ROW(1,3), (1,'a') = (1,'a')").await, ["t|t"]);
    assert_eq!(
        rows(&c, "SELECT 'abc' SIMILAR TO 'a%', 'abc' NOT SIMILAR TO '%z', 'abc' ~* 'B'").await,
        ["t|t|t"]
    );
    assert_eq!(
        rows(&c, "SELECT id FROM sh WHERE v = ANY('{c,q}'::text[]) AND id = ANY(ARRAY[2,3]::int[]) ORDER BY id").await,
        ["2", "3"]
    );
    assert_eq!(
        rows(&c, "WITH d AS (DELETE FROM shu RETURNING *) SELECT count(*) FROM d").await,
        ["1"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn jsonb_table_functions_and_scalar_gaps() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    assert_eq!(
        rows(&c, r#"SELECT * FROM jsonb_each('{"a":1,"b":[2],"cc":null}'::jsonb) ORDER BY key"#).await,
        ["a|1", "b|[2]", "cc|null"]
    );
    // jsonb's key order: shorter first.
    assert_eq!(
        rows(&c, r#"SELECT key FROM jsonb_each('{"bb":1,"a":2,"c":3}')"#).await,
        ["a", "c", "bb"]
    );
    assert_eq!(
        rows(&c, r#"SELECT * FROM jsonb_array_elements_text('[1,{"x":2},"s",null]')"#).await,
        ["1", "{\"x\": 2}", "s", "NULL"]
    );
    assert_eq!(
        rows(&c, r#"SELECT * FROM jsonb_to_recordset('[{"a":1},{"a":2,"b":"z"}]') AS r(a int, b text)"#).await,
        ["1|NULL", "2|z"]
    );
    let e = c
        .simple_query(r#"SELECT * FROM jsonb_array_elements('{"a":1}')"#)
        .await
        .expect_err("object");
    let message = e.as_db_error().map(|d| d.message().to_string()).unwrap_or_default();
    assert_eq!(message, "cannot extract elements from an object");

    assert_eq!(rows(&c, "SELECT upper('straße'), lower('ΟΔΟΣ'), 'STRASSE' ILIKE 'straße'").await, ["STRAßE|οδοσ|f"]);
    assert_eq!(
        rows(&c, "SELECT make_interval(days => 3), make_interval(1, 2, 0, 4, 5, 6, 7.5)").await,
        ["3 days|1 year 2 mons 4 days 05:06:07.5"]
    );
    assert_eq!(
        rows(&c, r"SELECT '\x0102'::bytea || '\x03'::bytea, 'x' || '\x01'::bytea").await,
        [r"\x010203|\x7801"]
    );
    assert_eq!(
        rows(&c, r"SELECT regexp_split_to_array('a,b;c', '[,;]'), regexp_count('aaa', 'a'), regexp_substr('abc123', '\d+')").await,
        ["{a,b,c}|3|123"]
    );
    assert_eq!(
        rows(&c, "SELECT array_position(ARRAY['a','b'], 'b'), array_remove(ARRAY[1,2,1], 1), array_replace(ARRAY[1,2], 2, 9), array_upper(ARRAY[1,2,3], 1), trim_array(ARRAY[1,2,3], 1), array_fill(0, ARRAY[3])").await,
        ["2|{2}|{1,9}|3|{1,2}|{0,0,0}"]
    );
    assert_eq!(
        rows(&c, "SELECT scale(1.230), min_scale(1.230), trim_scale(1.2300), sin(0), atan2(1,1), get_byte('\\x0102'::bytea, 1)").await,
        ["3|2|1.23|0|0.7853981633974483|2"]
    );
}
