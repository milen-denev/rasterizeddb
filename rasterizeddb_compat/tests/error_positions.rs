//! An error's position (the `P` field, psql's `at character N`) and
//! PostgreSQL's words for an undefined column or relation. Errors carried no
//! position, and an unknown column read "Column 'x' not found in schema",
//! `relation does not exist` named nothing. Each position and message here
//! is PostgreSQL 17's for the same statement (`.claude/reviews/pgdiff`).

use rasterizeddb_compat::TestServer;

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    c
}

/// The SQLSTATE, message and position of `sql`'s error over the simple
/// protocol.
async fn error(c: &tokio_postgres::Client, sql: &str) -> (String, String, Option<u32>) {
    let err = c.simple_query(sql).await.expect_err(sql);
    let db = err.as_db_error().unwrap_or_else(|| panic!("{sql}: {err}"));
    let position = match db.position() {
        Some(tokio_postgres::error::ErrorPosition::Original(p)) => Some(*p),
        _ => None,
    };
    (db.code().code().to_string(), db.message().to_string(), position)
}

#[tokio::test(flavor = "multi_thread")]
async fn errors_point_where_postgresql_points() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;
    c.batch_execute("CREATE TYPE epe AS ENUM ('a', 'b'); CREATE TABLE ep (id int, name text, e epe)")
        .await
        .expect("setup");
    let cases: &[(&str, &str, &str, u32)] = &[
        ("SELECT nosuch FROM ep", "42703", "column \"nosuch\" does not exist", 8),
        ("SELECT id FROM ep WHERE nosuch = 1", "42703", "column \"nosuch\" does not exist", 25),
        ("SELECT ep.zz FROM ep", "42703", "column ep.zz does not exist", 8),
        ("SELECT id FROM ep ORDER BY nosuch", "42703", "column \"nosuch\" does not exist", 28),
        ("SELECT count(nosuch) FROM ep", "42703", "column \"nosuch\" does not exist", 14),
        ("INSERT INTO ep (id, nosuch) VALUES (1, 2)", "42703", "column \"nosuch\" of relation \"ep\" does not exist", 21),
        ("UPDATE ep SET nosuch = 1", "42703", "column \"nosuch\" of relation \"ep\" does not exist", 15),
        ("SELECT * FROM nosuchrel", "42P01", "relation \"nosuchrel\" does not exist", 15),
        ("SELECT x.id FROM ep", "42P01", "missing FROM-clause entry for table \"x\"", 8),
        ("SELECT id FROM ep WHERE e > 'zzz'", "22P02", "invalid input value for enum epe: \"zzz\"", 29),
        ("SELECT id FROM ep WHERE id = 'abc'", "22P02", "invalid input syntax for type integer: \"abc\"", 30),
        ("SELECT * FROM ep TABLESAMPLE foo (1)", "42704", "tablesample method foo does not exist", 30),
        // Characters, not bytes; and counted from the start of the string a
        // client sent, past the statements before.
        ("SELECT 'é', nosuch FROM ep", "42703", "column \"nosuch\" does not exist", 13),
        ("SELECT 1; SELECT nosuch FROM ep", "42703", "column \"nosuch\" does not exist", 18),
    ];
    for (sql, code, message, position) in cases {
        assert_eq!(error(&c, sql).await, (code.to_string(), message.to_string(), Some(*position)), "{sql}");
    }
    // Over the extended protocol too, in the statement's own text.
    let err = c.query("SELECT nosuch FROM ep WHERE id = $1", &[&1i32]).await.expect_err("extended");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.message(), "column \"nosuch\" does not exist");
    assert!(matches!(db.position(), Some(tokio_postgres::error::ErrorPosition::Original(8))), "{:?}", db.position());
}
