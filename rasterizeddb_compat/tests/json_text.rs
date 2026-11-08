//! `json`: a document kept as the text it was written as (`DbType::JSONTEXT`,
//! `core::json_text`). A `json` column used to be a `jsonb` one: its text came
//! back normalised (keys sorted, duplicates dropped, whitespace gone),
//! `pg_typeof` said `jsonb`, and `json_build_object` sorted its keys.
//! Every expected answer here is PostgreSQL 17's.

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

async fn server() -> (TestServer, tokio_postgres::Client) {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE jt (id INT PRIMARY KEY, d JSON, b JSONB)",
        r#"INSERT INTO jt VALUES (1, '{"b": 1,  "a": [1, 2], "b": 3}', '{"b": 1,  "a": [1, 2], "b": 3}'),
           (2, ' [1,  {"z" : true}] ', '[1]'), (3, NULL, NULL), (4, '"é"', '"x"')"#,
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    (server, c)
}

#[tokio::test(flavor = "multi_thread")]
async fn json_keeps_its_text_and_reads_it_as_postgresql_does() {
    let (_server, c) = server().await;
    let cases: &[(&str, &[&str])] = &[
        // Stored as written; jsonb beside it normalised.
        (
            "SELECT id, d, b FROM jt ORDER BY id",
            &[
                r#"1|{"b": 1,  "a": [1, 2], "b": 3}|{"a": [1, 2], "b": 3}"#,
                r#"2| [1,  {"z" : true}] |[1]"#,
                "3|NULL|NULL",
                r#"4|"é"|"x""#,
            ],
        ),
        // A member is its own text; of duplicate keys the last is found.
        ("SELECT id, d->'a', d->>'b', b->>'b' FROM jt WHERE id = 1", &["1|[1, 2]|3|3"]),
        (
            r#"SELECT d->1, d->1->>'z', d#>'{1,z}', d#>>'{1,z}' FROM jt WHERE id = 2"#,
            &[r#"{"z" : true}|true|true|true"#],
        ),
        ("SELECT d->>0 FROM jt WHERE id = 4", &["NULL"]),
        ("SELECT pg_typeof(d), pg_typeof(d->'a'), pg_typeof(d->>'a') FROM jt WHERE id = 1", &["json|json|text"]),
        // Casts.
        (r#"SELECT '{"x":  1}'::json, '{"x":  1}'::json::jsonb, '{"b":1, "a":2}'::jsonb::json"#, &[r#"{"x":  1}|{"x": 1}|{"a": 2, "b": 1}"#]),
        ("SELECT d::text FROM jt WHERE id = 1", &[r#"{"b": 1,  "a": [1, 2], "b": 3}"#]),
        ("SELECT d::jsonb FROM jt WHERE id = 1", &[r#"{"a": [1, 2], "b": 3}"#]),
        ("SELECT json_typeof(d) FROM jt WHERE d IS NOT NULL ORDER BY id", &["object", "array", "string"]),
        ("SELECT json_array_length(d) FROM jt WHERE id = 2", &["2"]),
        // Read in a WHERE clause: these matched nothing while the path was
        // read as a stored document.
        ("SELECT id FROM jt WHERE d->>'b' = '3'", &["1"]),
        ("SELECT id FROM jt WHERE (d->>'b')::int > 2", &["1"]),
        ("SELECT id FROM jt WHERE d->1->>'z' = 'true'", &["2"]),
        ("SELECT count(*) FROM jt WHERE d->>'zz' IS NULL", &["4"]),
        // The json builders: PostgreSQL's layout, arguments in order, a json
        // argument as written.
        (
            r#"SELECT json_build_object('k', 1, 's', 'x', 'n', NULL, 'j', '{"x":  1}'::json, 'jb', '{"x":  1}'::jsonb)"#,
            &[r#"{"k" : 1, "s" : "x", "n" : null, "j" : {"x":  1}, "jb" : {"x": 1}}"#],
        ),
        (r#"SELECT json_build_array(1, 'x', NULL, 2.50, '{"x":  1}'::json)"#, &[r#"[1, "x", null, 2.50, {"x":  1}]"#]),
        (r#"SELECT jsonb_build_object('j', '{"x":  1}'::json)"#, &[r#"{"j": {"x": 1}}"#]),
        ("SELECT json_build_object('o', json_build_object('i', 1))", &[r#"{"o" : {"i" : 1}}"#]),
        ("SELECT json_build_object('a', d->'a') FROM jt WHERE id = 1", &[r#"{"a" : [1, 2]}"#]),
        ("SELECT pg_typeof(json_build_object('a', 1)), pg_typeof(to_json(1)), pg_typeof(jsonb_build_object('a', 1))", &["json|json|jsonb"]),
    ];
    let mut wrong = Vec::new();
    for (sql, want) in cases {
        match c.simple_query(sql).await {
            Ok(messages) => {
                let got: Vec<String> = messages
                    .into_iter()
                    .filter_map(|m| match m {
                        SimpleQueryMessage::Row(r) => Some(
                            (0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"),
                        ),
                        _ => None,
                    })
                    .collect();
                if got != *want {
                    wrong.push(format!("{sql}\n   got {got:?}\n  want {want:?}"));
                }
            }
            Err(e) => wrong.push(format!("{sql}: {e}")),
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// Written through UPDATE and ADD COLUMN too, each value checked; what
/// PostgreSQL refuses with its codes.
#[tokio::test(flavor = "multi_thread")]
async fn writes_are_checked_and_refusals_carry_postgresqls_codes() {
    let (_server, c) = server().await;
    c.simple_query(r#"UPDATE jt SET d = '{"k" :  "v"}' WHERE id = 3"#).await.expect("update");
    assert_eq!(rows(&c, "SELECT d FROM jt WHERE id = 3").await, [r#"{"k" :  "v"}"#]);
    // A column added later is the same `json`, not a shredded one.
    c.simple_query("ALTER TABLE jt ADD COLUMN e JSON").await.expect("add column");
    c.simple_query(r#"INSERT INTO jt (id, e) VALUES (6, '{"q":   1}')"#).await.expect("insert");
    assert_eq!(rows(&c, "SELECT e, e->'q', pg_typeof(e) FROM jt WHERE id = 6").await, [r#"{"q":   1}|1|json"#]);
    for (sql, sqlstate) in [
        ("SELECT '[1,2'::json", "22P02"),
        ("INSERT INTO jt VALUES (5, 'nope', NULL)", "22P02"),
        ("UPDATE jt SET d = 'bad' WHERE id = 3", "22P02"),
        ("CREATE INDEX jt_d ON jt (d)", "42704"),
        ("CREATE INDEX jt_d ON jt USING hash (d)", "42704"),
        ("SELECT json_array_length(d) FROM jt WHERE id = 1", "22023"),
    ] {
        assert_eq!(code(&c, sql).await, sqlstate, "{sql}");
    }
    assert_eq!(rows(&c, "SELECT d FROM jt WHERE id = 3").await, [r#"{"k" :  "v"}"#]);
}

/// Over the extended protocol a `json` column, a member of one and a json
/// builder are described as `json` (OID 114), `->>` as text.
#[tokio::test(flavor = "multi_thread")]
async fn the_extended_protocol_describes_json() {
    let (_server, c) = server().await;
    let statement = c
        .prepare("SELECT d, d->'a', d->>'b', b, json_build_object('k', id), d::text FROM jt WHERE id = $1")
        .await
        .expect("prepare");
    let types: Vec<&str> = statement.columns().iter().map(|col| col.type_().name()).collect();
    assert_eq!(types, ["json", "json", "text", "jsonb", "json", "text"]);
    let row = c.query_one(&statement, &[&1i32]).await.expect("query");
    assert_eq!(row.get::<_, String>(2), "3");
    assert_eq!(row.get::<_, String>(5), r#"{"b": 1,  "a": [1, 2], "b": 3}"#);
}
