//! `string_agg`, `array_agg`, `json_agg`/`jsonb_agg`, `bool_and`/`bool_or`/
//! `every`, and an `ORDER BY` inside an aggregate call: every one was
//! "function '…' is not supported yet", though `string_agg(x, ', ' ORDER BY
//! x)` is in most reporting queries an application sends. Every expected
//! answer is PostgreSQL 17's (`.claude/reviews/agg/p1.sql`, `p2.sql`), with
//! the values chosen so this engine's byte-order collation and PostgreSQL's
//! en_US one agree.

use rasterizeddb_compat::TestServer;
use sqlx::{Connection, Row};
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
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, g INT, s TEXT, b BOOLEAN, n NUMERIC(6,2))",
        "INSERT INTO t VALUES (1, 1, 'a', true, 1.50), (2, 1, 'b', false, 2.25), (3, 2, 'c', true, NULL), \
         (4, 2, NULL, NULL, 4.00), (5, 3, 'e', true, 5.00)",
        "CREATE TABLE u (id INT PRIMARY KEY, tid INT, tag TEXT)",
        "INSERT INTO u VALUES (1, 1, 'x'), (2, 1, 'y'), (3, 2, 'z')",
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn each_aggregate_answers_as_postgresql_does() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    setup(&c).await;
    let cases: &[(&str, &[&str])] = &[
        ("SELECT string_agg(s, ',' ORDER BY id) FROM t", &["a,b,c,e"]),
        ("SELECT g, string_agg(s, ', ' ORDER BY s DESC) FROM t GROUP BY g ORDER BY g", &["1|b, a", "2|c", "3|e"]),
        ("SELECT string_agg(DISTINCT s, '|') FROM t", &["a|b|c|e"]),
        ("SELECT string_agg(DISTINCT s, '|' ORDER BY s DESC) FROM t", &["e|c|b|a"]),
        ("SELECT g, string_agg(s, ',') FILTER (WHERE id > 1) FROM t GROUP BY g ORDER BY g", &["1|b", "2|c", "3|e"]),
        ("SELECT string_agg(s, ',') FROM t WHERE false", &["NULL"]),
        ("SELECT string_agg(s, NULL ORDER BY id) FROM t", &["abce"]),
        ("SELECT g, array_agg(id ORDER BY id DESC) FROM t GROUP BY g ORDER BY g", &["1|{2,1}", "2|{4,3}", "3|{5}"]),
        ("SELECT array_agg(s ORDER BY id) FROM t", &["{a,b,c,NULL,e}"]),
        ("SELECT array_agg(n ORDER BY id) FROM t", &["{1.50,2.25,NULL,4.00,5.00}"]),
        ("SELECT g, bool_and(b), bool_or(b), every(b) FROM t GROUP BY g ORDER BY g", &["1|f|t|f", "2|t|t|t", "3|t|t|t"]),
        ("SELECT bool_and(b), bool_or(b) FROM t WHERE false", &["NULL|NULL"]),
        ("SELECT bool_or(id > 4), bool_and(id > 0), every(g < 2) FROM t", &["t|t|f"]),
        ("SELECT json_agg(s ORDER BY id) FROM t", &[r#"["a", "b", "c", null, "e"]"#]),
        ("SELECT g, jsonb_agg(id ORDER BY id) FROM t GROUP BY g ORDER BY g", &["1|[1, 2]", "2|[3, 4]", "3|[5]"]),
        (
            "SELECT t.id, string_agg(u.tag, ',' ORDER BY u.tag DESC) FROM t JOIN u ON u.tid = t.id GROUP BY t.id ORDER BY t.id",
            &["1|y,x", "2|z"],
        ),
        (
            "SELECT t.id, array_agg(u.tag ORDER BY u.tag) FROM t LEFT JOIN u ON u.tid = t.id GROUP BY t.id ORDER BY t.id",
            &["1|{x,y}", "2|{z}", "3|{NULL}", "4|{NULL}", "5|{NULL}"],
        ),
        (
            "SELECT g, count(*), string_agg(id::text, '-' ORDER BY id) AS ids FROM t GROUP BY g HAVING count(*) > 1 ORDER BY g",
            &["1|2|1-2", "2|2|3-4"],
        ),
        ("SELECT g, COALESCE(string_agg(s, '/' ORDER BY s), '-') FROM t GROUP BY g ORDER BY g", &["1|a/b", "2|c", "3|e"]),
        ("SELECT length(string_agg(s, '' ORDER BY id)) FROM t", &["4"]),
        (
            "WITH c AS (SELECT g, array_agg(id ORDER BY id) AS ids FROM t GROUP BY g) SELECT g, ids FROM c ORDER BY g",
            &["1|{1,2}", "2|{3,4}", "3|{5}"],
        ),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }

    // What PostgreSQL refuses.
    assert_eq!(code(&c, "SELECT string_agg(id, ',') FROM t").await, "42883");
    assert_eq!(code(&c, "SELECT bool_and(id) FROM t").await, "42883");
    assert_eq!(code(&c, "SELECT string_agg(s) FROM t").await, "42883");
    assert_eq!(code(&c, "SELECT string_agg(DISTINCT s, ',' ORDER BY id) FROM t").await, "42P10");
}

/// Array elements are quoted as `array_out` quotes them, delimiters and
/// strings are taken as written, JSON strings escaped, and an ordered list
/// over thousands of rows (folded by several workers, their partial lists
/// merged) comes out in exactly the call's order.
#[tokio::test(flavor = "multi_thread")]
async fn quoting_escaping_and_order_across_workers() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    c.simple_query("CREATE TABLE q (id INT PRIMARY KEY, g INT, s TEXT)").await.expect("create");
    c.simple_query(
        r#"INSERT INTO q VALUES (1, 1, 'a b'), (2, 1, 'x,y'), (3, 1, 'say "hi"'), (4, 2, ''), (5, 2, 'NULL'), (6, 2, NULL), (7, 3, 'back\slash')"#,
    )
    .await
    .expect("rows");
    let cases: &[(&str, &[&str])] = &[
        (
            "SELECT g, array_agg(s ORDER BY id) FROM q GROUP BY g ORDER BY g",
            &[r#"1|{"a b","x,y","say \"hi\""}"#, r#"2|{"","NULL",NULL}"#, r#"3|{"back\\slash"}"#],
        ),
        ("SELECT string_agg(s, ''';''' ORDER BY id) FROM q WHERE g = 1", &[r#"a b';'x,y';'say "hi""#]),
        ("SELECT json_agg(s ORDER BY id) FROM q WHERE g = 1", &[r#"["a b", "x,y", "say \"hi\""]"#]),
        ("SELECT string_agg(s, ',' ORDER BY id NULLS FIRST) FROM q WHERE g = 2", &[",NULL"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&c, sql).await, *want, "{sql}");
    }

    c.simple_query("CREATE TABLE big (id INT PRIMARY KEY, g INT, v TEXT)").await.expect("create");
    let values: Vec<String> = (1..=3000).map(|i| format!("({i}, {}, 'v{i}')", i % 3)).collect();
    c.simple_query(&format!("INSERT INTO big VALUES {}", values.join(", "))).await.expect("rows");
    let got = rows(&c, "SELECT g, string_agg(v, ',' ORDER BY id DESC) FROM big GROUP BY g ORDER BY g").await;
    for (g, line) in got.iter().enumerate() {
        let want: Vec<String> = (1..=3000).rev().filter(|i| i % 3 == g).map(|i| format!("v{i}")).collect();
        assert_eq!(*line, format!("{g}|{}", want.join(",")), "group {g} out of order");
    }
}

/// Over the extended protocol, each is described as PostgreSQL types it
/// where this engine has the type (text, boolean, jsonb), and the array as
/// the one array type it has, `text[]`, whose text is PostgreSQL's.
#[tokio::test(flavor = "multi_thread")]
async fn drivers_read_them_by_type() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    setup(&c).await;
    let mut x = sqlx::PgConnection::connect(&server.url()).await.expect("sqlx");
    let row = sqlx::query(
        "SELECT string_agg(s, ',' ORDER BY id), bool_and(b), jsonb_agg(id ORDER BY id), array_agg(s ORDER BY id) FROM t WHERE g = 1",
    )
    .fetch_one(&mut x)
    .await
    .expect("extended");
    assert_eq!(row.get::<String, _>(0), "a,b");
    assert!(!row.get::<bool, _>(1));
    assert_eq!(row.get::<serde_json::Value, _>(2), serde_json::json!([1, 2]));
    assert_eq!(row.get::<Vec<String>, _>(3), ["a", "b"]);
}
