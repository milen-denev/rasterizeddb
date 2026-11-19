//! `CREATE FUNCTION`, `CREATE PROCEDURE`/`CALL` and `CREATE TRIGGER`, as
//! PostgreSQL 17 answers them. Each was "syntax error or unsupported query";
//! the expected answers are PostgreSQL 17's (`.claude/reviews/pgdiff`).

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
async fn functions_and_procedures_answer_as_postgresql_does() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE ft (id INT PRIMARY KEY, v TEXT, n INT); INSERT INTO ft VALUES (1, 'a', 10), (2, 'b', 20); \
         CREATE FUNCTION addf(a integer, b integer) RETURNS integer LANGUAGE sql AS $$ SELECT a + b $$; \
         CREATE FUNCTION dbl(int) RETURNS int AS 'SELECT $1 * 2' LANGUAGE sql; \
         CREATE FUNCTION cnt() RETURNS bigint LANGUAGE sql AS 'SELECT count(*) FROM ft'; \
         CREATE FUNCTION rows_of(x int) RETURNS SETOF ft LANGUAGE sql AS 'SELECT * FROM ft WHERE n >= x'; \
         CREATE FUNCTION up(t text) RETURNS text LANGUAGE plpgsql AS $$ BEGIN RETURN upper(t); END $$; \
         CREATE FUNCTION sq(x double precision) RETURNS double precision RETURN x * x; \
         CREATE PROCEDURE ins(i int, t text) LANGUAGE sql AS $$ \
           INSERT INTO ft (id, v, n) VALUES (i, t, i * 10); INSERT INTO ft (id, v, n) VALUES (i + 100, t, 0) $$",
    )
    .await
    .expect("setup");
    for (sql, expected) in [
        ("SELECT addf(1, 2), addf(n, id) FROM ft ORDER BY id", &["3|11", "3|22"][..]),
        ("SELECT id FROM ft WHERE dbl(n) > 25", &["2"]),
        ("SELECT cnt()", &["2"]),
        ("SELECT id, v FROM rows_of(15) r ORDER BY id", &["2|b"]),
        ("SELECT * FROM rows_of(5) ORDER BY 1", &["1|a|10", "2|b|20"]),
        ("SELECT up(v) FROM ft ORDER BY 1", &["A", "B"]),
        ("SELECT sq(3)", &["9"]),
        ("SELECT addf(addf(1, 1), dbl(2))", &["6"]),
    ] {
        assert_eq!(rows(&c, sql).await, expected, "{sql}");
    }
    c.batch_execute("CALL ins(3, 'c')").await.expect("call");
    assert_eq!(rows(&c, "SELECT id FROM ft WHERE v = 'c' ORDER BY id").await, ["3", "103"]);
    for (sql, expected) in [
        ("CREATE FUNCTION addf(a integer, b integer) RETURNS integer LANGUAGE sql AS 'SELECT 1'", "42723"),
        ("CREATE OR REPLACE FUNCTION addf(a integer, b integer) RETURNS text LANGUAGE sql AS 'SELECT 1'", "42P13"),
        ("CREATE FUNCTION c_fn() RETURNS int AS 'lib', 'sym' LANGUAGE c", "0A000"),
        ("CREATE FUNCTION nolang() RETURNS int AS 'SELECT 1'", "42P13"),
        ("DROP FUNCTION nosuch(int)", "42883"),
        ("CALL nosuch()", "42883"),
    ] {
        assert_eq!(code(&c, sql).await, expected, "{sql}");
    }
    c.batch_execute("CREATE OR REPLACE FUNCTION addf(a integer, b integer) RETURNS integer LANGUAGE sql AS 'SELECT a * b'")
        .await
        .expect("replace");
    assert_eq!(rows(&c, "SELECT addf(3, 4)").await, ["12"]);
    c.batch_execute("DROP FUNCTION addf(int, int); DROP FUNCTION IF EXISTS nosuch(int)").await.expect("drop");
    assert!(c.simple_query("SELECT addf(3, 4)").await.is_err());

    // Bound parameters reach the body.
    let pool = server.connect_sqlx().await.expect("sqlx");
    let n: i32 = sqlx::query("SELECT dbl($1)").bind(21).fetch_one(&pool).await.expect("bound").get(0);
    assert_eq!(n, 42);
}

#[tokio::test(flavor = "multi_thread")]
async fn triggers_run_their_plpgsql_for_each_row() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE ft (id INT PRIMARY KEY, v TEXT, n INT, at TIMESTAMPTZ); INSERT INTO ft VALUES (1, 'a', 10, NULL); \
         CREATE TABLE fl (id SERIAL PRIMARY KEY, op TEXT, rid INT, oldv TEXT, newv TEXT); \
         CREATE FUNCTION touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
           NEW.at := '2026-01-01 00:00:00+00'; IF NEW.n IS NULL THEN NEW.n = 0; END IF; RETURN NEW; END; $$; \
         CREATE TRIGGER t_touch BEFORE INSERT OR UPDATE ON ft FOR EACH ROW EXECUTE FUNCTION touch(); \
         CREATE FUNCTION audit() RETURNS trigger AS $$ BEGIN \
           IF TG_OP = 'DELETE' THEN INSERT INTO fl (op, rid, oldv) VALUES (TG_OP, OLD.id, OLD.v); RETURN OLD; END IF; \
           INSERT INTO fl (op, rid, oldv, newv) VALUES (TG_OP, NEW.id, CASE WHEN TG_OP = 'UPDATE' THEN OLD.v END, NEW.v); \
           RETURN NEW; END $$ LANGUAGE plpgsql; \
         CREATE TRIGGER t_audit AFTER INSERT OR UPDATE OR DELETE ON ft FOR EACH ROW EXECUTE PROCEDURE audit()",
    )
    .await
    .expect("setup");
    assert_eq!(
        rows(&c, "INSERT INTO ft (id, v) VALUES (10, 'x'), (11, 'y') RETURNING id, n, at").await,
        ["10|0|2026-01-01 00:00:00+00", "11|0|2026-01-01 00:00:00+00"]
    );
    assert_eq!(
        rows(&c, "UPDATE ft SET v = v || '!' WHERE id >= 10 RETURNING id, v").await,
        ["10|x!", "11|y!"]
    );
    c.batch_execute("DELETE FROM ft WHERE id = 11").await.expect("delete");
    assert_eq!(
        rows(&c, "SELECT op, rid, oldv, newv FROM fl ORDER BY id").await,
        ["INSERT|10|NULL|x", "INSERT|11|NULL|y", "UPDATE|10|x|x!", "UPDATE|11|y|y!", "DELETE|11|y!|NULL"]
    );

    c.batch_execute(
        "CREATE FUNCTION guard() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE lim integer := 0; BEGIN \
           IF NEW.n < lim THEN RAISE EXCEPTION 'n must be positive, got %', NEW.n USING ERRCODE = '23514'; END IF; \
           IF NEW.v = 'skip' THEN RETURN NULL; END IF; RETURN NEW; END $$; \
         CREATE TRIGGER t_guard BEFORE INSERT ON ft FOR EACH ROW WHEN (NEW.id > 100) EXECUTE FUNCTION guard()",
    )
    .await
    .expect("guard");
    assert_eq!(code(&c, "INSERT INTO ft (id, v, n) VALUES (200, 'q', -5)").await, "23514");
    c.batch_execute("INSERT INTO ft (id, v, n) VALUES (201, 'skip', 5), (202, 'keep', 5), (50, 'low', -5)")
        .await
        .expect("skip and when");
    assert_eq!(rows(&c, "SELECT id FROM ft WHERE id IN (50, 200, 201, 202) ORDER BY id").await, ["50", "202"]);
    // A trigger that raises takes the whole statement back.
    assert_eq!(code(&c, "INSERT INTO ft (id, v, n) VALUES (300, 'ok', 1), (301, 'bad', -1)").await, "23514");
    assert_eq!(rows(&c, "SELECT count(*) FROM ft WHERE id IN (300, 301)").await, ["0"]);
    // And a block's rollback takes back what its triggers did.
    c.batch_execute("BEGIN; INSERT INTO ft (id, v) VALUES (400, 'r'); ROLLBACK").await.expect("rolled back");
    assert_eq!(rows(&c, "SELECT count(*) FROM fl WHERE rid = 400").await, ["0"]);

    for (sql, expected) in [
        ("DROP FUNCTION guard()", "2BP01"),
        ("CREATE TRIGGER t_guard BEFORE INSERT ON ft FOR EACH ROW EXECUTE FUNCTION guard()", "42710"),
        ("CREATE TRIGGER x BEFORE INSERT ON nosuch FOR EACH ROW EXECUTE FUNCTION guard()", "42P01"),
        ("CREATE TRIGGER x BEFORE INSERT ON ft FOR EACH ROW EXECUTE FUNCTION nosuch()", "42883"),
        ("CREATE TRIGGER x BEFORE INSERT ON ft FOR EACH ROW EXECUTE FUNCTION up()", "42883"),
        ("DROP TRIGGER nosuch ON ft", "42704"),
    ] {
        assert_eq!(code(&c, sql).await, expected, "{sql}");
    }
    c.batch_execute("DROP TRIGGER t_guard ON ft; DROP TRIGGER IF EXISTS t_guard ON ft; DROP FUNCTION guard()")
        .await
        .expect("drop");

    // The extended protocol fires them too.
    let pool = server.connect_sqlx().await.expect("sqlx");
    let n: i32 = sqlx::query("INSERT INTO ft (id, v) VALUES ($1, $2) RETURNING n")
        .bind(500)
        .bind("p")
        .fetch_one(&pool)
        .await
        .expect("bound insert")
        .get(0);
    assert_eq!(n, 0);
    let logged: i64 = sqlx::query("SELECT count(*) FROM fl WHERE rid = 500").fetch_one(&pool).await.expect("log").get(0);
    assert_eq!(logged, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn routines_and_triggers_survive_a_restart() {
    let root = std::env::temp_dir().join(format!("routines_restart_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let root_text = root.to_string_lossy().into_owned();
    let mut server = TestServer::start_file_backed(&root_text).await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT); \
         CREATE FUNCTION f(x int) RETURNS int LANGUAGE sql AS 'SELECT x + 1'; \
         CREATE FUNCTION up() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.v := upper(NEW.v); RETURN NEW; END $$; \
         CREATE TRIGGER t_up BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION up()",
    )
    .await
    .expect("setup");
    drop(pool);
    drop(server);
    server = TestServer::start_file_backed(&root_text).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("reconnect");
    let n: i32 = sqlx::query("SELECT f(1)").fetch_one(&pool).await.expect("function").get(0);
    assert_eq!(n, 2);
    pool.execute("INSERT INTO t VALUES (1, 'abc')").await.expect("insert");
    let v: String = sqlx::query("SELECT v FROM t").fetch_one(&pool).await.expect("read").get(0);
    assert_eq!(v, "ABC");
    // A dropped table takes its triggers with it.
    pool.execute("DROP TABLE t; CREATE TABLE t (id INT PRIMARY KEY, v TEXT); INSERT INTO t VALUES (1, 'abc')")
        .await
        .expect("recreate");
    let v: String = sqlx::query("SELECT v FROM t").fetch_one(&pool).await.expect("read").get(0);
    assert_eq!(v, "abc");
    drop(pool);
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}
