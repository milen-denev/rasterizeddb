//! `CREATE SEQUENCE` and `nextval`/`currval`/`setval`/`lastval`, as
//! PostgreSQL 17 answers them.
//!
//! Sequences existed only behind `SERIAL`; `CREATE SEQUENCE` was a syntax
//! error and `nextval` worked only as a column default. Expected answers and
//! codes are PostgreSQL 17's (`.claude/reviews/pgdiff`, side by side).

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
async fn sequence_functions_answer_as_postgresql_does() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    for (sql, expected) in [
        ("CREATE SEQUENCE s1", &[][..]),
        ("SELECT nextval('s1'), nextval('s1')", &["1|2"]),
        ("SELECT currval('s1'), lastval()", &["2|2"]),
        ("CREATE SEQUENCE s2 START 100 INCREMENT BY 10 MAXVALUE 120", &[]),
        ("SELECT nextval('s2')", &["100"]),
        ("SELECT nextval('public.s2'::regclass)", &["110"]),
        ("SELECT nextval('S2')", &["120"]),
        ("SELECT setval('s2', 105)", &["105"]),
        ("SELECT nextval('s2')", &["115"]),
        ("SELECT setval('s2', 105, false)", &["105"]),
        ("SELECT nextval('s2')", &["105"]),
        ("CREATE SEQUENCE s3 INCREMENT -1 MINVALUE -3 MAXVALUE -1 CYCLE", &[]),
        ("SELECT nextval('s3'), nextval('s3'), nextval('s3'), nextval('s3')", &["-1|-2|-3|-1"]),
        ("ALTER SEQUENCE s2 RESTART WITH 101", &[]),
        ("SELECT nextval('s2')", &["101"]),
        ("ALTER SEQUENCE s2 RENAME TO s2b", &[]),
        ("SELECT nextval('s2b')", &["111"]),
    ] {
        assert_eq!(rows(&c, sql).await, expected, "{sql}");
    }
    for (sql, expected) in [
        ("SELECT nextval('s2b')", "2200H"),
        ("SELECT setval('s2b', 500)", "22003"),
        ("SELECT nextval('nosuch')", "42P01"),
        ("CREATE SEQUENCE sd INCREMENT 0", "22023"),
        ("CREATE SEQUENCE sd MINVALUE 10 START 1", "22023"),
        ("CREATE SEQUENCE s1", "42P07"),
        ("CREATE TABLE s1 (a INT)", "42P07"),
        ("DROP TABLE s1", "42809"),
        ("DROP SEQUENCE nosuch", "42P01"),
    ] {
        assert_eq!(code(&c, sql).await, expected, "{sql}");
    }
    // currval is the session's own.
    let other = client(&server).await;
    assert_eq!(code(&other, "SELECT currval('s1')").await, "55000");
    assert_eq!(code(&other, "SELECT lastval()").await, "55000");
    c.batch_execute("CREATE SEQUENCE IF NOT EXISTS s1; DROP SEQUENCE IF EXISTS nosuch").await.expect("if [not] exists");

    // A value is not transactional: a rolled-back nextval is not handed out
    // again.
    c.batch_execute("BEGIN; SELECT nextval('s1'); ROLLBACK").await.expect("rolled back");
    assert_eq!(rows(&c, "SELECT nextval('s1')").await, ["4"]);
    // A rolled-back CREATE SEQUENCE leaves nothing.
    c.batch_execute("BEGIN; CREATE SEQUENCE sr; ROLLBACK").await.expect("create rolled back");
    assert_eq!(code(&c, "SELECT nextval('sr')").await, "42P01");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sequence_numbers_rows_once_each_in_every_statement_shape() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE SEQUENCE ids; \
         CREATE TABLE t (id BIGINT DEFAULT nextval('ids') PRIMARY KEY, v TEXT); \
         CREATE TABLE src (v TEXT); INSERT INTO src VALUES ('p'), ('q')",
    )
    .await
    .expect("setup");
    // One value per row, none skipped: a NOT NULL check that evaluated the
    // value first took two per row.
    assert_eq!(rows(&c, "INSERT INTO t (v) VALUES ('a'), ('b') RETURNING id").await, ["1", "2"]);
    assert_eq!(rows(&c, "INSERT INTO t VALUES (nextval('ids'), 'c'), (nextval('ids'), 'd') RETURNING id").await, ["3", "4"]);
    assert_eq!(rows(&c, "SELECT currval('ids')").await, ["4"]);
    assert_eq!(rows(&c, "INSERT INTO t (id, v) SELECT nextval('ids'), v FROM src RETURNING id").await, ["5", "6"]);
    assert_eq!(rows(&c, "INSERT INTO t (v, id) SELECT 'e', nextval('ids') RETURNING id").await, ["7"]);
    assert_eq!(rows(&c, "SELECT nextval('ids') FROM src").await, ["8", "9"]);
    // RETURNING reports what the row holds, not a second evaluation.
    assert_eq!(rows(&c, "UPDATE t SET id = nextval('ids') + 100 WHERE v = 'a' RETURNING id").await, ["110"]);
    assert_eq!(rows(&c, "SELECT id FROM t WHERE v = 'a'").await, ["110"]);
    c.batch_execute("ALTER TABLE t ADD COLUMN tok UUID").await.expect("uuid column");
    let returned = rows(&c, "UPDATE t SET tok = gen_random_uuid() WHERE v = 'b' RETURNING tok").await;
    assert_eq!(rows(&c, "SELECT tok FROM t WHERE v = 'b'").await, returned);

    // A SERIAL's sequence, as migrations reset it.
    c.batch_execute("CREATE TABLE s (id SERIAL PRIMARY KEY, v TEXT); INSERT INTO s (v) VALUES ('x')").await.expect("serial");
    assert_eq!(rows(&c, "SELECT pg_get_serial_sequence('s', 'id')").await, ["public.s_id_seq"]);
    assert_eq!(rows(&c, "SELECT pg_get_serial_sequence('s', 'v')").await, ["NULL"]);
    assert_eq!(rows(&c, "SELECT setval(pg_get_serial_sequence('s', 'id'), 50)").await, ["50"]);
    assert_eq!(rows(&c, "INSERT INTO s (v) VALUES ('y') RETURNING id").await, ["51"]);
    assert_eq!(rows(&c, "SELECT currval('s_id_seq')").await, ["51"]);

    // The extended protocol takes the same calls.
    let pool = server.connect_sqlx().await.expect("sqlx");
    let id: i64 = sqlx::query("SELECT nextval($1)").bind("ids").fetch_one(&pool).await.expect("bound").get(0);
    assert_eq!(id, 11);

    // A column default reads it: dropping it is 2BP01, or with CASCADE the
    // default goes too.
    assert_eq!(code(&c, "DROP SEQUENCE ids").await, "2BP01");
    c.batch_execute("DROP SEQUENCE ids CASCADE").await.expect("cascade");
    assert_eq!(code(&c, "INSERT INTO t (v) VALUES ('z')").await, "23502");
}

#[tokio::test(flavor = "multi_thread")]
async fn sequences_are_listed_and_resume_after_a_restart() {
    let root = std::env::temp_dir().join(format!("sequences_restart_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let root_text = root.to_string_lossy().into_owned();
    let mut server = TestServer::start_file_backed(&root_text).await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE SEQUENCE a AS integer START 10; CREATE SEQUENCE owned OWNED BY t.id; \
         CREATE TABLE t (id SERIAL PRIMARY KEY)",
    )
    .await
    .expect("setup");
    let mut seen = Vec::new();
    for _ in 0..40 {
        seen.push(rows(&c, "SELECT nextval('a')").await[0].parse::<i64>().unwrap());
    }
    for (sql, expected) in [
        (
            "SELECT sequence_name, data_type, start_value, increment FROM information_schema.sequences ORDER BY 1",
            &["a|integer|10|1", "owned|bigint|1|1", "t_id_seq|integer|1|1"][..],
        ),
        ("SELECT sequencename, last_value FROM pg_sequences WHERE sequencename = 'a'", &["a|49"]),
        ("SELECT relkind FROM pg_class WHERE relname = 'a'", &["S"]),
    ] {
        assert_eq!(rows(&c, sql).await, expected, "{sql}");
    }
    drop(c);
    drop(server);
    server = TestServer::start_file_backed(&root_text).await.expect("restart");
    let c = client(&server).await;
    // Past every value handed out before, never one of them again.
    let next: i64 = rows(&c, "SELECT nextval('a')").await[0].parse().unwrap();
    assert!(next > *seen.iter().max().unwrap(), "{next} after {seen:?}");
    // OWNED BY: the sequence goes with its table.
    c.batch_execute("DROP TABLE t").await.expect("drop table");
    assert_eq!(code(&c, "SELECT nextval('owned')").await, "42P01");
    drop(c);
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlx_reads_sequence_values_as_bigint() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("sqlx");
    pool.execute("CREATE SEQUENCE s").await.expect("create");
    let row = sqlx::query("SELECT nextval('s') AS n, currval('s') AS c").fetch_one(&pool).await.expect("select");
    assert_eq!((row.get::<i64, _>("n"), row.get::<i64, _>("c")), (1, 1));
}

/// `nextval` in a condition draws once per row it judges, as PostgreSQL's
/// does: it was folded into one value for the statement. Over both
/// protocols; the extended one binds the rest of the condition.
#[tokio::test(flavor = "multi_thread")]
async fn nextval_in_a_where_draws_for_every_row() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute("CREATE TABLE t (id INT PRIMARY KEY, n INT); INSERT INTO t VALUES (1, 1), (2, 2), (3, 3), (4, NULL); CREATE SEQUENCE q START 1")
        .await
        .expect("setup");
    assert_eq!(rows(&c, "SELECT count(*) FROM t WHERE n < nextval('q') + 100000").await, ["3"]);
    assert_eq!(rows(&c, "SELECT nextval('q')").await, ["5"]);
    let bound = c.prepare("SELECT count(*) FROM t WHERE n < nextval('q') + $1::int4").await.expect("prepare");
    let n: i64 = c.query_one(&bound, &[&100000i32]).await.expect("bound").get(0);
    assert_eq!(n, 3);
    assert_eq!(rows(&c, "SELECT nextval('q')").await, ["10"]);
    assert_eq!(rows(&c, "DELETE FROM t WHERE id = nextval('q') - 100000 RETURNING id").await, Vec::<String>::new());
    assert_eq!(rows(&c, "SELECT nextval('q')").await, ["15"]);
}
