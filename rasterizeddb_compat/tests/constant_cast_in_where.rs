//! A literal cast to a type its text is not (`'abc'::int`) is an error
//! wherever it stands, as in PostgreSQL 17, which converts it while parsing:
//! whatever the table holds, empty or not, and behind `OR true`. Inside a
//! WHERE clause it was taken as "no match" instead: `WHERE 'abc'::int = 1`
//! counted 0 rows, `WHERE 'abc'::int > 0 OR true` every row, and `WHERE 1 =
//! 'abc'::int` failed with an internal message. Found against a live server.

use rasterizeddb_compat::TestServer;
use tokio_postgres::Client;

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    client
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bad_constant_cast_in_where_is_an_error_as_in_postgresql() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE t (id INT, name TEXT); INSERT INTO t VALUES (1, 'a'), (2, 'b'); CREATE TABLE e (id INT);",
    )
    .await
    .expect("setup");

    // (statement, PostgreSQL 17's SQLSTATE and message)
    let refused = [
        ("SELECT count(*) FROM t WHERE 'abc'::int = 1", "22P02", r#"invalid input syntax for type integer: "abc""#),
        ("SELECT count(*) FROM t WHERE 1 = 'abc'::int", "22P02", r#"invalid input syntax for type integer: "abc""#),
        ("SELECT count(*) FROM t WHERE 'abc'::int > 0 OR true", "22P02", r#"invalid input syntax for type integer: "abc""#),
        ("SELECT count(*) FROM t WHERE name = 'x' AND 'abc'::int = 1", "22P02", r#"invalid input syntax for type integer: "abc""#),
        ("SELECT count(*) FROM e WHERE 'abc'::int = 1", "22P02", r#"invalid input syntax for type integer: "abc""#),
        ("SELECT count(*) FROM t WHERE id = '12x'::bigint", "22P02", r#"invalid input syntax for type bigint: "12x""#),
        ("SELECT count(*) FROM t WHERE 'maybe'::boolean", "22P02", r#"invalid input syntax for type boolean: "maybe""#),
        ("SELECT count(*) FROM t WHERE CAST('abc' AS integer) = 1", "22P02", r#"invalid input syntax for type integer: "abc""#),
    ];
    for (sql, code, message) in refused {
        let e = c.simple_query(sql).await.expect_err(sql);
        let db = e.as_db_error().unwrap_or_else(|| panic!("{sql}: {e}"));
        assert_eq!((db.code().code(), db.message()), (code, message), "{sql}");
    }

    // Good casts answer as PostgreSQL does. The constant-only ones were wrong
    // too: `'1'::int = 1` matched nothing, `1 = '1'::int` and a bare
    // `'true'::boolean` failed with internal messages.
    for (sql, want) in [
        ("SELECT count(*) FROM t WHERE '1'::int = 1", "2"),
        ("SELECT count(*) FROM t WHERE 1 = '1'::int", "2"),
        ("SELECT count(*) FROM t WHERE '1'::int = 2", "0"),
        ("SELECT count(*) FROM t WHERE 'true'::boolean", "2"),
        ("SELECT count(*) FROM t WHERE NOT 'false'::boolean AND id = 1", "1"),
        ("SELECT count(*) FROM t WHERE '2'::int > 1 OR id = 7", "2"),
        ("SELECT count(*) FROM t WHERE id::text = 'abc'", "0"),
        ("SELECT count(*) FROM t WHERE id = '2'::bigint", "1"),
        ("SELECT count(*) FROM t WHERE 'true'::boolean AND id > 1", "1"),
    ] {
        let rows = c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let got = rows.iter().find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
            _ => None,
        });
        assert_eq!(got.as_deref(), Some(want), "{sql}");
    }
}
