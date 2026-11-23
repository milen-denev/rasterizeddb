//! A WHERE expression that fails for a row reports why, with PostgreSQL's
//! SQLSTATE. Scan workers report through a one-byte code, and every
//! evaluator error without a code of its own came out as "an expression in
//! the WHERE clause could not be evaluated", XX000: a bad cast that
//! PostgreSQL 17 reports as 22P02 `invalid input syntax for type integer:
//! "abc"` looked like an internal error. Messages and codes are PostgreSQL
//! 17's (`docker run --rm postgres:17`).

use rasterizeddb_compat::TestServer;

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_where_expression_says_why() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    c.batch_execute("CREATE TABLE et (id INT PRIMARY KEY, s TEXT); INSERT INTO et VALUES (1, '5'), (2, 'abc')")
        .await
        .expect("setup");
    for (sql, code, message) in [
        ("SELECT id FROM et WHERE s::int > 0", "22P02", "invalid input syntax for type integer: \"abc\""),
        ("SELECT id FROM et WHERE s::numeric > 0", "22P02", "invalid input syntax for type numeric: \"abc\""),
        ("SELECT id FROM et WHERE (s || 'x')::int > 0 OR id > 5", "22P02", "invalid input syntax for type integer: \"5x\""),
        ("SELECT count(*) FROM et WHERE s::int > 0", "22P02", "invalid input syntax for type integer: \"abc\""),
        ("UPDATE et SET s = 'y' WHERE s::int > 0", "22P02", "invalid input syntax for type integer: \"abc\""),
        ("DELETE FROM et WHERE s::int > 0", "22P02", "invalid input syntax for type integer: \"abc\""),
        ("SELECT id FROM et WHERE to_date(s, 'YYYY') > '2000-01-01'", "22007", "invalid value \"abc\" for \"YYYY\""),
        ("SELECT id FROM et WHERE length(s) / (id - 1) > 0", "22012", "division by zero"),
    ] {
        let e = c.simple_query(sql).await.expect_err(sql);
        let db = e.as_db_error().unwrap_or_else(|| panic!("{sql}: {e}"));
        assert_eq!((db.code().code(), db.message()), (code, message), "{sql}");
    }
    // Nothing was written by the failed UPDATE and DELETE.
    let rows = c.simple_query("SELECT id, s FROM et ORDER BY id").await.expect("read back");
    let rows: Vec<String> = rows
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(format!("{}|{}", r.get(0).unwrap(), r.get(1).unwrap())),
            _ => None,
        })
        .collect();
    assert_eq!(rows, ["1|5", "2|abc"]);
}
