//! What was left of the JSON comparison with PostgreSQL 17 after
//! `json_path_shapes_pg17.rs`: the `json_*` table functions, a set-returning
//! function alone in a subquery's select list, a document compared or
//! contained where one side is computed or grouped, and the type of a
//! parameter compared with a table function's column.
//!
//! Every statement here failed before its fix, and every expectation is
//! PostgreSQL 17's own answer (`.claude/reviews/pgdiff/jsonpath`).

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

const SETUP: &str = r#"CREATE TABLE jp (id int PRIMARY KEY, k text, jb jsonb, j json, t text, arr jsonb); INSERT INTO jp VALUES (1, 'obj', '{"a":1,"b":{"c":[10,20]},"s":"x","n":null,"0":"zero"}', '{"a":1,"b":{"c":[10,20]},"s":"x","n":null,"0":"zero"}', '{"a":1,"b":{"c":[10,20]},"s":"x","n":null,"0":"zero"}', '[1,2,3]'); INSERT INTO jp VALUES (2, 'arr', '[1,"two",[3,4],{"k":5},null,true]', '[1,"two",[3,4],{"k":5},null,true]', '[1,"two",[3,4],{"k":5},null,true]', '["a","b"]'); INSERT INTO jp VALUES (3, 'str', '"hello"', '"hello"', '"hello"', '[1,2,3]'); INSERT INTO jp VALUES (4, 'num', '42', '42', '42', '["a","b"]'); INSERT INTO jp VALUES (5, 'boo', 'true', 'true', 'true', '[1,2,3]'); INSERT INTO jp VALUES (6, 'nul', 'null', 'null', 'null', '["a","b"]'); INSERT INTO jp VALUES (7, 'eobj', '{}', '{}', '{}', '[1,2,3]'); INSERT INTO jp VALUES (8, 'earr', '[]', '[]', '[]', '["a","b"]'); INSERT INTO jp VALUES (9, 'sqlnull', NULL, NULL, NULL, NULL);"#;

async fn start() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(AssertSqlSafe(SETUP.to_string()))).await.expect("setup");
    (server, pool)
}

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

/// Each statement's rows against PostgreSQL 17's, in order where the
/// statement orders them.
async fn check(pool: &sqlx::PgPool, cases: &[(&str, &[&str])]) {
    for (sql, expected) in cases {
        let mut got = rows(pool, sql).await;
        let mut want: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
        if !sql.contains("ORDER BY") {
            got.sort();
            want.sort();
        }
        assert_eq!(got, want, "{sql}");
    }
}

/// `json_array_elements[_text]`, `json_each[_text]` and `json_object_keys` in a FROM: the text as stored (a
/// value's own spelling, an object's members in the order written, a key written twice both times), where
/// only the `jsonb_*` forms existed ("missing FROM-clause entry", "Table not found").
#[tokio::test(flavor = "multi_thread")]
async fn the_json_table_functions() {
    let (_server, pool) = start().await;
    let cases: &[(&str, &[&str])] = &[
        (r#"SELECT e.value ->> 0, e.value -> 0, e.value ->> 'k' FROM json_array_elements('[1,"two",[3,4],{"k":5},null,true]'::json) AS e(value)"#, &[r#"NULL|NULL|NULL"#, r#"NULL|NULL|NULL"#, r#"3|3|NULL"#, r#"NULL|NULL|5"#, r#"NULL|NULL|NULL"#, r#"NULL|NULL|NULL"#][..]),
        (r#"SELECT value FROM json_array_elements('[1,"two",[3,4],{"k":5},null,true]'::json)"#, &[r#"1"#, r#""two""#, r#"[3,4]"#, r#"{"k":5}"#, r#"null"#, r#"true"#][..]),
        (r#"SELECT value FROM json_array_elements_text('[1,"two",[3,4],{"k":5},null,true]'::json)"#, &[r#"1"#, r#"two"#, r#"[3,4]"#, r#"{"k":5}"#, r#"NULL"#, r#"true"#][..]),
        (r#"SELECT * FROM json_array_elements('[ 1 , {"b" : 2 , "a":1} ]'::json) WITH ORDINALITY"#, &[r#"1|1"#, r#"{"b" : 2 , "a":1}|2"#][..]),
        (r#"SELECT key, value FROM json_each('{"z":1,"a":"x","n":null,"a":[2]}'::json)"#, &[r#"z|1"#, r#"a|"x""#, r#"n|null"#, r#"a|[2]"#][..]),
        (r#"SELECT key, value FROM json_each_text('{"z":1,"a":"x","n":null,"a":[2]}'::json)"#, &[r#"z|1"#, r#"a|x"#, r#"n|NULL"#, r#"a|[2]"#][..]),
        (r#"SELECT * FROM json_object_keys('{"z":1,"a":"x","n":null,"a":[2]}'::json)"#, &[r#"z"#, r#"a"#, r#"n"#, r#"a"#][..]),
        (r#"SELECT k FROM json_object_keys('{"z":1,"a":"x","n":null,"a":[2]}'::json) AS k ORDER BY k"#, &[r#"a"#, r#"a"#, r#"n"#, r#"z"#][..]),
        (r#"SELECT k, e.value FROM jp, json_array_elements(jp.j) AS e WHERE jp.k = 'arr' "#, &[r#"arr|1"#, r#"arr|"two""#, r#"arr|[3,4]"#, r#"arr|{"k":5}"#, r#"arr|null"#, r#"arr|true"#][..]),
        (r#"SELECT k, e.key, e.value FROM jp CROSS JOIN LATERAL json_each(jp.j) AS e WHERE jp.k = 'obj'"#, &[r#"obj|a|1"#, r#"obj|b|{"c":[10,20]}"#, r#"obj|s|"x""#, r#"obj|n|null"#, r#"obj|0|"zero""#][..]),
        (r#"SELECT k, e.key, e.value FROM jp CROSS JOIN LATERAL json_each_text(jp.j) AS e WHERE jp.k = 'obj'"#, &[r#"obj|a|1"#, r#"obj|b|{"c":[10,20]}"#, r#"obj|s|x"#, r#"obj|n|NULL"#, r#"obj|0|zero"#][..]),
        (r#"SELECT k, x FROM jp, json_object_keys(jp.j) AS x WHERE jp.k IN ('obj','eobj') ORDER BY 1, 2"#, &[r#"obj|0"#, r#"obj|a"#, r#"obj|b"#, r#"obj|n"#, r#"obj|s"#][..]),
        (r#"SELECT * FROM json_array_elements(NULL::json)"#, &[][..]),
        (r#"SELECT * FROM json_array_elements('[]'::json)"#, &[][..]),
        (r#"SELECT * FROM json_each('{}'::json)"#, &[][..]),
        (r#"SELECT pg_typeof(value), pg_typeof(key) FROM json_each('{"a":1}'::json)"#, &[r#"json|text"#][..]),
        (r#"SELECT pg_typeof(value) FROM json_array_elements('[1]'::json)"#, &[r#"json"#][..]),
        (r#"SELECT pg_typeof(value) FROM json_array_elements_text('[1]'::json)"#, &[r#"text"#][..]),
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT 1 FROM json_array_elements_text(jp.j) AS e(value) WHERE e.value = 'two') AND jp.k = 'arr'"#, &[r#"2"#][..]),
        (r#"SELECT count(*) FROM jp, json_array_elements(jp.j) e WHERE jp.k IN ('arr','earr')"#, &[r#"6"#][..]),
        (r#"SELECT json_array_elements_text('[1,"b"]'::json)"#, &[r#"1"#, r#"b"#][..]),
    ];
    check(&pool, cases).await;
}

/// `x IN (SELECT jsonb_array_elements_text(t.doc))`, `EXISTS (SELECT unnest(…))`: the function is the
/// subquery's relation. It was a column named for the whole call.
#[tokio::test(flavor = "multi_thread")]
async fn a_set_returning_function_alone_in_a_subquery() {
    let (_server, pool) = start().await;
    let cases: &[(&str, &[&str])] = &[
        (r#"SELECT id FROM jp WHERE '2' IN (SELECT jsonb_array_elements_text(jp.arr)) ORDER BY id"#, &[r#"1"#, r#"3"#, r#"5"#, r#"7"#][..]),
        (r#"SELECT id FROM jp WHERE 'a' IN (SELECT jsonb_array_elements_text(arr)) ORDER BY id"#, &[r#"2"#, r#"4"#, r#"6"#, r#"8"#][..]),
        (r#"SELECT id FROM jp WHERE '9' NOT IN (SELECT jsonb_array_elements_text(jp.arr)) ORDER BY id"#, &[r#"1"#, r#"2"#, r#"3"#, r#"4"#, r#"5"#, r#"6"#, r#"7"#, r#"8"#, r#"9"#][..]),
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT jsonb_array_elements(jp.arr)) ORDER BY id"#, &[r#"1"#, r#"2"#, r#"3"#, r#"4"#, r#"5"#, r#"6"#, r#"7"#, r#"8"#][..]),
        (r#"SELECT id FROM jp WHERE id IN (SELECT generate_series(1, 3)) ORDER BY id"#, &[r#"1"#, r#"2"#, r#"3"#][..]),
        (r#"SELECT id FROM jp WHERE id IN (SELECT unnest(ARRAY[2, 4, jp.id - 7])) ORDER BY id"#, &[r#"2"#, r#"4"#][..]),
        (r#"SELECT id FROM jp WHERE k IN (SELECT jsonb_object_keys('{"obj":1,"num":2}'::jsonb)) ORDER BY id"#, &[r#"1"#, r#"4"#][..]),
        (r#"SELECT id FROM jp WHERE 'k' IN (SELECT jsonb_object_keys(jp.jb -> 3)) ORDER BY id"#, &[r#"2"#][..]),
        (r#"SELECT id, (SELECT count(*) FROM (SELECT jsonb_array_elements(jp.arr)) s) FROM jp WHERE id < 3 ORDER BY id"#, &[r#"1|3"#, r#"2|2"#][..]),
        (r#"SELECT jsonb_array_elements_text('[1,"b"]'::jsonb)"#, &[r#"1"#, r#"b"#][..]),
        (r#"SELECT jsonb_array_elements('[1,"b"]'::jsonb) AS v"#, &[r#"1"#, r#""b""#][..]),
        (r#"SELECT jsonb_object_keys('{"b":1,"a":2}'::jsonb)"#, &[r#"a"#, r#"b"#][..]),
        (r#"SELECT x FROM (SELECT jsonb_array_elements_text('[1,"b"]'::jsonb) AS x) s ORDER BY x"#, &[r#"1"#, r#"b"#][..]),
        (r#"SELECT k FROM jp WHERE 'c' = ANY (ARRAY(SELECT jsonb_object_keys(jb -> 'b'))) ORDER BY id"#, &[r#"obj"#][..]),
    ];
    check(&pool, cases).await;
}

/// A document the row computes, compared (`'[1]'::jsonb || (doc -> 'a') = …`) or contained (`'[20]'::jsonb
/// <@ (doc -> 'b')`, a constant on the left, a group on the right): "yields a JSONB value, which a WHERE
/// clause cannot compare yet", "expected closing parenthesis after grouped expression", "operator <@ does
/// not apply to a STRING value".
#[tokio::test(flavor = "multi_thread")]
async fn a_computed_or_grouped_document_in_a_where() {
    let (_server, pool) = start().await;
    let cases: &[(&str, &[&str])] = &[
        (r#"SELECT k FROM jp WHERE '{"c":[10,20]}'::jsonb <@ (jb -> 'b') ORDER BY id"#, &[r#"obj"#][..]),
        (r#"SELECT k FROM jp WHERE '[1]'::jsonb || (jb -> 'b' -> 'c') = '[1,10,20]' ORDER BY id"#, &[r#"obj"#][..]),
        (r#"SELECT k FROM jp WHERE '[20]'::jsonb <@ (jb -> 'b' -> 'c') AND id < 5 ORDER BY id"#, &[r#"obj"#][..]),
        (r#"SELECT k FROM jp WHERE id < 5 OR '[3]'::jsonb <@ (jb -> 2) ORDER BY id"#, &[r#"obj"#, r#"arr"#, r#"str"#, r#"num"#][..]),
        (r#"SELECT k FROM jp WHERE NOT ('[20]'::jsonb <@ (jb -> 'b' -> 'c')) ORDER BY id"#, &[][..]),
        (r#"SELECT k FROM jp WHERE (jb -> 'b' -> 'c') || '[1]'::jsonb = '[10,20,1]' ORDER BY id"#, &[r#"obj"#][..]),
        (r#"SELECT k FROM jp WHERE jsonb_build_array(jb -> 'a') = '[1]' ORDER BY id"#, &[r#"obj"#][..]),
        (r#"SELECT k FROM jp WHERE jsonb_build_array(jb -> 'a') <> '[1]'::jsonb ORDER BY id"#, &[r#"arr"#, r#"str"#, r#"num"#, r#"boo"#, r#"nul"#, r#"eobj"#, r#"earr"#, r#"sqlnull"#][..]),
        (r#"SELECT k FROM jp WHERE (jb -> 'b') || '{"d":1}'::jsonb @> '{"d":1}' ORDER BY id"#, &[r#"obj"#][..]),
        (r#"SELECT k FROM jp WHERE (jb -> 'a') < (jb -> 's') ORDER BY id"#, &[][..]),
        (r#"SELECT k FROM jp WHERE 5 < (id + 1) AND 'x' = (jb ->> 's') ORDER BY id"#, &[][..]),
        (r#"SELECT k FROM jp WHERE (jb -> 'b') <@ jb ORDER BY id"#, &[][..]),
        (r#"SELECT k FROM jp WHERE '[1,2,3,4]'::jsonb @> arr ORDER BY id"#, &[r#"obj"#, r#"str"#, r#"boo"#, r#"eobj"#][..]),
    ];
    check(&pool, cases).await;
}

/// What the `json` functions refuse, in PostgreSQL's words.
#[tokio::test(flavor = "multi_thread")]
async fn the_json_table_functions_refuse_the_wrong_kind() {
    let (_server, pool) = start().await;
    for (sql, message) in [
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT 1 FROM jsonb_each(jp.jb) AS e WHERE e.key = 'a') ORDER BY id"#, r#"cannot call jsonb_each on a non-object"#),
        (r#"SELECT * FROM json_array_elements('{}'::json)"#, r#"cannot call json_array_elements on a non-array"#),
        (r#"SELECT * FROM json_array_elements('5'::json)"#, r#"cannot call json_array_elements on a scalar"#),
        (r#"SELECT * FROM json_each('[1]'::json)"#, r#"cannot deconstruct an array as an object"#),
        (r#"SELECT * FROM json_each('5'::json)"#, r#"cannot deconstruct a scalar"#),
        (r#"SELECT * FROM json_object_keys('[1]'::json)"#, r#"cannot call json_object_keys on an array"#),
        (r#"SELECT * FROM json_object_keys('"s"'::json)"#, r#"cannot call json_object_keys on a scalar"#),
    ] {
        let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_all(&pool).await.expect_err(sql).to_string();
        assert!(e.contains(message), "{sql}: {e}");
    }
}

/// A parameter with no declared type, compared with a table function's
/// column, is the column's type, as PostgreSQL infers it: a value that looks
/// like a number was written into the statement as one, and `text = integer`
/// has no operator (42883). tokio-postgres declares no types at Parse.
#[tokio::test(flavor = "multi_thread")]
async fn an_untyped_parameter_beside_a_function_column_is_text() {
    let server = TestServer::start().await.expect("server");
    let (client, connection) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(connection);
    client.batch_execute(SETUP).await.expect("setup");
    let cases: &[(&str, &[&str], &[&str])] = &[
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(jp.arr) AS e(value) WHERE e.value = $1::text) ORDER BY id"#, &["2"][..], &[r#"1"#, r#"3"#, r#"5"#, r#"7"#][..]),
        (r#"SELECT id FROM jp WHERE $1::text IN (SELECT e.value FROM jsonb_array_elements_text(jp.arr) AS e(value)) ORDER BY id"#, &["a"][..], &[r#"2"#, r#"4"#, r#"6"#, r#"8"#][..]),
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(jp.arr) AS e(value) WHERE e.value = $1) ORDER BY id"#, &["2"][..], &[r#"1"#, r#"3"#, r#"5"#, r#"7"#][..]),
        (r#"SELECT id FROM jp WHERE $1 IN (SELECT e.value FROM jsonb_array_elements_text(jp.arr) AS e(value)) ORDER BY id"#, &["3"][..], &[r#"1"#, r#"3"#, r#"5"#, r#"7"#][..]),
        (r#"SELECT id FROM jp WHERE $1 NOT IN (SELECT e.value FROM jsonb_array_elements_text(jp.arr) AS e(value)) ORDER BY id"#, &["3"][..], &[r#"2"#, r#"4"#, r#"6"#, r#"8"#, r#"9"#][..]),
        (r#"SELECT id FROM jp WHERE (SELECT count(*) FROM jsonb_array_elements_text(jp.arr) AS e(value) WHERE e.value = $1) > 0 ORDER BY id"#, &["2"][..], &[r#"1"#, r#"3"#, r#"5"#, r#"7"#][..]),
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT 1 FROM jsonb_each_text(jp.jb) AS m WHERE m.value = $1) AND jp.id = 1 ORDER BY id"#, &["1"][..], &[r#"1"#][..]),
        (r#"SELECT id FROM jp, jsonb_array_elements_text(jp.arr) AS e(value) WHERE e.value = $1 ORDER BY id"#, &["2"][..], &[r#"1"#, r#"3"#, r#"5"#, r#"7"#][..]),
        (r#"SELECT e.value FROM jsonb_array_elements_text('[1,2,"x"]'::jsonb) AS e(value) WHERE e.value = $1"#, &["2"][..], &[r#"2"#][..]),
        (r#"SELECT e.value FROM jsonb_array_elements_text('[1,2,"x"]'::jsonb) AS e WHERE value <> $1 ORDER BY 1"#, &["2"][..], &[r#"1"#, r#"x"#][..]),
        (r#"SELECT k FROM jsonb_object_keys('{"1":0,"b":0}'::jsonb) AS k WHERE k = $1"#, &["1"][..], &[r#"1"#][..]),
        (r#"SELECT e.value FROM jp CROSS JOIN LATERAL jsonb_array_elements_text(jp.arr) AS e(value) WHERE e.value = $1 AND jp.id = 1"#, &["2"][..], &[r#"2"#][..]),
    ];
    for (sql, values, expected) in cases {
        // Every parameter as text, whatever type the server reported: the
        // statements below compare them with text, or cast them.
        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = values.iter().map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
        let rows = client.query(*sql, &params).await.unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
        let got: Vec<String> = rows
            .iter()
            .map(|row| {
                (0..row.len())
                    .map(|i| match row.columns()[i].type_().name() {
                        "int4" => row.get::<_, Option<i32>>(i).map(|v| v.to_string()),
                        "int8" => row.get::<_, Option<i64>>(i).map(|v| v.to_string()),
                        _ => row.get::<_, Option<String>>(i),
                    })
                    .map(|v| v.unwrap_or_else(|| "NULL".into()))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect();
        assert_eq!(got, *expected, "{sql}");
    }
}
