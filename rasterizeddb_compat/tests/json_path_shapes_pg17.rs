//! JSON path operators and functions over a row's JSON, where the engine and
//! PostgreSQL 17 disagreed: the empty path (`#> '{}'`, `#>> '{}'`), a path
//! after a parenthesised operand, a negative index in a WHERE, and a table
//! function reading the outer row inside EXISTS and IN.
//!
//! Every statement here failed before its fix, and every expectation is
//! PostgreSQL 17's own answer to it, taken from a side-by-side run of about
//! 740 statements (`.claude/reviews/pgdiff`). One thing that run settled the
//! other way: `'5'::jsonb -> 0` and `->> 0` give the scalar in PostgreSQL too
//! (a jsonb scalar is a one-element array to `->`), while `json` gives NULL;
//! `scalars_are_one_element_arrays_to_an_index` pins both.

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

/// `x #> '{}'` is `x` and `x #>> '{}'` its text (a string's own, NULL for a JSON null), over a column, a
/// group, a literal and a function's value, in a select list, a WHERE, an ORDER BY and a GROUP BY. Each was
/// an error ("neither a column nor a value", "not found in schema", "unexpected character '#'").
#[tokio::test(flavor = "multi_thread")]
async fn the_empty_path_is_the_value_itself() {
    let (_server, pool) = start().await;
    let cases: &[(&str, &[&str])] = &[
        (r#"SELECT k, jb #> '{}' FROM jp ORDER BY id"#, &[r#"obj|{"0": "zero", "a": 1, "b": {"c": [10, 20]}, "n": null, "s": "x"}"#, r#"arr|[1, "two", [3, 4], {"k": 5}, null, true]"#, r#"str|"hello""#, r#"num|42"#, r#"boo|true"#, r#"nul|null"#, r#"eobj|{}"#, r#"earr|[]"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (jb) #> '{}' FROM jp ORDER BY id"#, &[r#"obj|{"0": "zero", "a": 1, "b": {"c": [10, 20]}, "n": null, "s": "x"}"#, r#"arr|[1, "two", [3, 4], {"k": 5}, null, true]"#, r#"str|"hello""#, r#"num|42"#, r#"boo|true"#, r#"nul|null"#, r#"eobj|{}"#, r#"earr|[]"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, j #> '{}' FROM jp ORDER BY id"#, &[r#"obj|{"a":1,"b":{"c":[10,20]},"s":"x","n":null,"0":"zero"}"#, r#"arr|[1,"two",[3,4],{"k":5},null,true]"#, r#"str|"hello""#, r#"num|42"#, r#"boo|true"#, r#"nul|null"#, r#"eobj|{}"#, r#"earr|[]"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #> '{}' FROM jp ORDER BY id"#, &[r#"obj|{"a":1,"b":{"c":[10,20]},"s":"x","n":null,"0":"zero"}"#, r#"arr|[1,"two",[3,4],{"k":5},null,true]"#, r#"str|"hello""#, r#"num|42"#, r#"boo|true"#, r#"nul|null"#, r#"eobj|{}"#, r#"earr|[]"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT '{"a":1,"b":{"c":[10,20]},"s":"x","n":null,"0":"zero"}'::jsonb #> '{}'"#, &[r#"{"0": "zero", "a": 1, "b": {"c": [10, 20]}, "n": null, "s": "x"}"#][..]),
        (r#"SELECT '[1,"two",[3,4],{"k":5},null,true]'::jsonb #> '{}'"#, &[r#"[1, "two", [3, 4], {"k": 5}, null, true]"#][..]),
        (r#"SELECT '"hello"'::jsonb #> '{}'"#, &[r#""hello""#][..]),
        (r#"SELECT '42'::jsonb #> '{}'"#, &[r#"42"#][..]),
        (r#"SELECT 'true'::jsonb #> '{}'"#, &[r#"true"#][..]),
        (r#"SELECT 'null'::jsonb #> '{}'"#, &[r#"null"#][..]),
        (r#"SELECT '{}'::jsonb #> '{}'"#, &[r#"{}"#][..]),
        (r#"SELECT '[]'::jsonb #> '{}'"#, &[r#"[]"#][..]),
        (r#"SELECT '[1,"two",[3,4],{"k":5},null,true]'::json #> '{}'"#, &[r#"[1,"two",[3,4],{"k":5},null,true]"#][..]),
        (r#"SELECT '"hello"'::json #> '{}'"#, &[r#""hello""#][..]),
        (r#"SELECT id FROM jp WHERE (jb #> '{}') IS NOT NULL ORDER BY id"#, &[r#"1"#, r#"2"#, r#"3"#, r#"4"#, r#"5"#, r#"6"#, r#"7"#, r#"8"#][..]),
        (r#"SELECT id FROM jp WHERE jb #> '{}' IS NULL ORDER BY id"#, &[r#"9"#][..]),
        (r#"SELECT k, jb #>> '{}' FROM jp ORDER BY id"#, &[r#"obj|{"0": "zero", "a": 1, "b": {"c": [10, 20]}, "n": null, "s": "x"}"#, r#"arr|[1, "two", [3, 4], {"k": 5}, null, true]"#, r#"str|hello"#, r#"num|42"#, r#"boo|true"#, r#"nul|NULL"#, r#"eobj|{}"#, r#"earr|[]"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (jb) #>> '{}' FROM jp ORDER BY id"#, &[r#"obj|{"0": "zero", "a": 1, "b": {"c": [10, 20]}, "n": null, "s": "x"}"#, r#"arr|[1, "two", [3, 4], {"k": 5}, null, true]"#, r#"str|hello"#, r#"num|42"#, r#"boo|true"#, r#"nul|NULL"#, r#"eobj|{}"#, r#"earr|[]"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, j #>> '{}' FROM jp ORDER BY id"#, &[r#"obj|{"a":1,"b":{"c":[10,20]},"s":"x","n":null,"0":"zero"}"#, r#"arr|[1,"two",[3,4],{"k":5},null,true]"#, r#"str|hello"#, r#"num|42"#, r#"boo|true"#, r#"nul|NULL"#, r#"eobj|{}"#, r#"earr|[]"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #>> '{}' FROM jp ORDER BY id"#, &[r#"obj|{"a":1,"b":{"c":[10,20]},"s":"x","n":null,"0":"zero"}"#, r#"arr|[1,"two",[3,4],{"k":5},null,true]"#, r#"str|hello"#, r#"num|42"#, r#"boo|true"#, r#"nul|NULL"#, r#"eobj|{}"#, r#"earr|[]"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT '{"a":1,"b":{"c":[10,20]},"s":"x","n":null,"0":"zero"}'::jsonb #>> '{}'"#, &[r#"{"0": "zero", "a": 1, "b": {"c": [10, 20]}, "n": null, "s": "x"}"#][..]),
        (r#"SELECT '[1,"two",[3,4],{"k":5},null,true]'::jsonb #>> '{}'"#, &[r#"[1, "two", [3, 4], {"k": 5}, null, true]"#][..]),
        (r#"SELECT '"hello"'::jsonb #>> '{}'"#, &[r#"hello"#][..]),
        (r#"SELECT '42'::jsonb #>> '{}'"#, &[r#"42"#][..]),
        (r#"SELECT 'true'::jsonb #>> '{}'"#, &[r#"true"#][..]),
        (r#"SELECT 'null'::jsonb #>> '{}'"#, &[r#"NULL"#][..]),
        (r#"SELECT '{}'::jsonb #>> '{}'"#, &[r#"{}"#][..]),
        (r#"SELECT '[]'::jsonb #>> '{}'"#, &[r#"[]"#][..]),
        (r#"SELECT '[1,"two",[3,4],{"k":5},null,true]'::json #>> '{}'"#, &[r#"[1,"two",[3,4],{"k":5},null,true]"#][..]),
        (r#"SELECT '"hello"'::json #>> '{}'"#, &[r#"hello"#][..]),
        (r#"SELECT id FROM jp WHERE (jb #>> '{}') IS NOT NULL ORDER BY id"#, &[r#"1"#, r#"2"#, r#"3"#, r#"4"#, r#"5"#, r#"7"#, r#"8"#][..]),
        (r#"SELECT id FROM jp WHERE jb #>> '{}' IS NULL ORDER BY id"#, &[r#"6"#, r#"9"#][..]),
        (r#"SELECT k FROM jp WHERE jb #>> '{}' = '42'"#, &[r#"num"#][..]),
        (r#"SELECT k FROM jp WHERE jb #>> '{}' = 'hello'"#, &[r#"str"#][..]),
        (r#"SELECT k, jb #>> '{}' || '!' FROM jp ORDER BY id"#, &[r#"obj|{"0": "zero", "a": 1, "b": {"c": [10, 20]}, "n": null, "s": "x"}!"#, r#"arr|[1, "two", [3, 4], {"k": 5}, null, true]!"#, r#"str|hello!"#, r#"num|42!"#, r#"boo|true!"#, r#"nul|NULL"#, r#"eobj|{}!"#, r#"earr|[]!"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, length(jb #>> '{}') FROM jp ORDER BY id"#, &[r#"obj|64"#, r#"arr|40"#, r#"str|5"#, r#"num|2"#, r#"boo|4"#, r#"nul|NULL"#, r#"eobj|2"#, r#"earr|2"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (jb -> 'b') #>> '{}' FROM jp ORDER BY id"#, &[r#"obj|{"c": [10, 20]}"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, to_jsonb(t) #>> '{}' FROM jp ORDER BY id"#, &[r#"obj|{"a":1,"b":{"c":[10,20]},"s":"x","n":null,"0":"zero"}"#, r#"arr|[1,"two",[3,4],{"k":5},null,true]"#, r#"str|"hello""#, r#"num|42"#, r#"boo|true"#, r#"nul|null"#, r#"eobj|{}"#, r#"earr|[]"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT e.value ->> 0, e.value -> 0, e.value #>> '{}', e.value #> '{}' FROM jsonb_array_elements('[1,"two",[3,4],{"k":5},null]'::jsonb) AS e(value)"#, &[r#"1|1|1|1"#, r#"two|"two"|two|"two""#, r#"3|3|[3, 4]|[3, 4]"#, r#"NULL|NULL|{"k": 5}|{"k": 5}"#, r#"NULL|null|NULL|null"#][..]),
        (r#"SELECT e.value #>> '{}' AS v FROM jsonb_array_elements('[1,"two",null]'::jsonb) AS e(value) ORDER BY v"#, &[r#"1"#, r#"two"#, r#"NULL"#][..]),
        (r#"SELECT count(*) FROM jp WHERE jb #>> '{}' IS NULL"#, &[r#"2"#][..]),
        (r#"SELECT k FROM jp ORDER BY jb #>> '{}', id"#, &[r#"num"#, r#"arr"#, r#"earr"#, r#"str"#, r#"boo"#, r#"obj"#, r#"eobj"#, r#"nul"#, r#"sqlnull"#][..]),
        (r#"SELECT jb #>> '{}', count(*) FROM jp GROUP BY jb #>> '{}' ORDER BY 1"#, &[r#"42|1"#, r#"[1, "two", [3, 4], {"k": 5}, null, true]|1"#, r#"[]|1"#, r#"hello|1"#, r#"true|1"#, r#"{"0": "zero", "a": 1, "b": {"c": [10, 20]}, "n": null, "s": "x"}|1"#, r#"{}|1"#, r#"NULL|2"#][..]),
        (r#"SELECT e.value #>> '{}', e.value #> '{}', e.value #>> '{0}', e.value #> '{k}' FROM jsonb_array_elements('[1,"two",[3,4],{"k":5},null,true]'::jsonb) AS e(value)"#, &[r#"1|1|NULL|NULL"#, r#"two|"two"|NULL|NULL"#, r#"[3, 4]|[3, 4]|3|NULL"#, r#"{"k": 5}|{"k": 5}|NULL|5"#, r#"NULL|null|NULL|NULL"#, r#"true|true|NULL|NULL"#][..]),
        (r#"SELECT k, s.v ->> 0, s.v -> 0, s.v #>> '{}' FROM (SELECT k, id, jb AS v FROM jp) s ORDER BY s.id"#, &[r#"obj|NULL|NULL|{"0": "zero", "a": 1, "b": {"c": [10, 20]}, "n": null, "s": "x"}"#, r#"arr|1|1|[1, "two", [3, 4], {"k": 5}, null, true]"#, r#"str|hello|"hello"|hello"#, r#"num|42|42|42"#, r#"boo|true|true|true"#, r#"nul|NULL|null|NULL"#, r#"eobj|NULL|NULL|{}"#, r#"earr|NULL|NULL|[]"#, r#"sqlnull|NULL|NULL|NULL"#][..]),
        (r#"WITH c AS MATERIALIZED (SELECT id, k, jb AS v, j AS w FROM jp) SELECT k, v ->> 0, v -> -1, w ->> 0, w -> 0, v #>> '{}', w #>> '{}' FROM c ORDER BY id"#, &[r#"obj|NULL|NULL|NULL|NULL|{"0": "zero", "a": 1, "b": {"c": [10, 20]}, "n": null, "s": "x"}|{"a":1,"b":{"c":[10,20]},"s":"x","n":null,"0":"zero"}"#, r#"arr|1|true|1|1|[1, "two", [3, 4], {"k": 5}, null, true]|[1,"two",[3,4],{"k":5},null,true]"#, r#"str|hello|"hello"|NULL|NULL|hello|hello"#, r#"num|42|42|NULL|NULL|42|42"#, r#"boo|true|true|NULL|NULL|true|true"#, r#"nul|NULL|null|NULL|NULL|NULL|NULL"#, r#"eobj|NULL|NULL|NULL|NULL|{}|{}"#, r#"earr|NULL|NULL|NULL|NULL|[]|[]"#, r#"sqlnull|NULL|NULL|NULL|NULL|NULL|NULL"#][..]),
        (r#"SELECT a.k, a.jb ->> 0, b.jb #>> '{}', b.j -> 0 FROM jp a JOIN jp b ON b.id = a.id ORDER BY a.id"#, &[r#"obj|NULL|{"0": "zero", "a": 1, "b": {"c": [10, 20]}, "n": null, "s": "x"}|NULL"#, r#"arr|1|[1, "two", [3, 4], {"k": 5}, null, true]|1"#, r#"str|hello|hello|NULL"#, r#"num|42|42|NULL"#, r#"boo|true|true|NULL"#, r#"nul|NULL|NULL|NULL"#, r#"eobj|NULL|{}|NULL"#, r#"earr|NULL|[]|NULL"#, r#"sqlnull|NULL|NULL|NULL"#][..]),
        (r#"SELECT k, coalesce(jb #>> '{}', 'nil'), (jb #>> '{}')::text IS NULL, jb #>> '{}' LIKE 'h%' FROM jp ORDER BY id"#, &[r#"obj|{"0": "zero", "a": 1, "b": {"c": [10, 20]}, "n": null, "s": "x"}|f|f"#, r#"arr|[1, "two", [3, 4], {"k": 5}, null, true]|f|f"#, r#"str|hello|f|t"#, r#"num|42|f|f"#, r#"boo|true|f|f"#, r#"nul|nil|t|NULL"#, r#"eobj|{}|f|f"#, r#"earr|[]|f|f"#, r#"sqlnull|nil|t|NULL"#][..]),
        (r#"SELECT k, CASE WHEN jsonb_typeof(jb) = 'number' THEN (jb #>> '{}')::int + 1 END FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|43"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k FROM jp WHERE jb #>> '{}' LIKE '%l%' ORDER BY id"#, &[r#"obj"#, r#"arr"#, r#"str"#][..]),
        (r#"SELECT k FROM jp WHERE jb #> '{}' = '42'::jsonb ORDER BY id"#, &[r#"num"#][..]),
        (r#"SELECT k FROM jp WHERE (jb #> '{}') @> '{"a":1}' ORDER BY id"#, &[r#"obj"#][..]),
        (r#"SELECT k FROM jp WHERE (jb #>> '{}') = '42' OR (jb -> 'a') = '1' ORDER BY id"#, &[r#"obj"#, r#"num"#][..]),
        (r#"SELECT k FROM jp WHERE '42' = (jb #>> '{}') ORDER BY id"#, &[r#"num"#][..]),
    ];
    check(&pool, cases).await;
}

/// A path after a parenthesised operand: `(doc) #> '{a}'` was "function '' is not supported yet", and
/// `(doc -> 'b') @> '…'` in a WHERE "expected comparison operator, got RPar".
#[tokio::test(flavor = "multi_thread")]
async fn a_path_follows_a_group() {
    let (_server, pool) = start().await;
    let cases: &[(&str, &[&str])] = &[
        (r#"SELECT k, (jb) #> '{a}' FROM jp ORDER BY id"#, &[r#"obj|1"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #> '{a}' FROM jp ORDER BY id"#, &[r#"obj|1"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (jb) #>> '{a}' FROM jp ORDER BY id"#, &[r#"obj|1"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #>> '{a}' FROM jp ORDER BY id"#, &[r#"obj|1"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (jb) #> '{b,c,1}' FROM jp ORDER BY id"#, &[r#"obj|20"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #> '{b,c,1}' FROM jp ORDER BY id"#, &[r#"obj|20"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (jb) #>> '{b,c,1}' FROM jp ORDER BY id"#, &[r#"obj|20"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #>> '{b,c,1}' FROM jp ORDER BY id"#, &[r#"obj|20"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (jb) #> '{0}' FROM jp ORDER BY id"#, &[r#"obj|"zero""#, r#"arr|1"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #> '{0}' FROM jp ORDER BY id"#, &[r#"obj|"zero""#, r#"arr|1"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (jb) #>> '{0}' FROM jp ORDER BY id"#, &[r#"obj|zero"#, r#"arr|1"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #>> '{0}' FROM jp ORDER BY id"#, &[r#"obj|zero"#, r#"arr|1"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (jb) #> '{2,0}' FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|3"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #> '{2,0}' FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|3"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (jb) #>> '{2,0}' FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|3"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #>> '{2,0}' FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|3"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (jb) #> '{3,k}' FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|5"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #> '{3,k}' FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|5"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (jb) #>> '{3,k}' FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|5"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #>> '{3,k}' FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|5"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (jb) #> '{zz}' FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #> '{zz}' FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (jb) #>> '{zz}' FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #>> '{zz}' FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (jb) #> '{n}' FROM jp ORDER BY id"#, &[r#"obj|null"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #> '{n}' FROM jp ORDER BY id"#, &[r#"obj|null"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (jb) #>> '{n}' FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #>> '{n}' FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT id FROM jp WHERE (jb -> 'b') @> '{"c":[10]}' ORDER BY id"#, &[r#"1"#][..]),
        (r#"SELECT id FROM jp WHERE (jb #> '{b}') @> '{"c":[10]}' ORDER BY id"#, &[r#"1"#][..]),
        (r#"SELECT k FROM jp WHERE (jb -> 'b' -> 'c') @> '[20]' AND (jb ->> 's') LIKE 'x%' ORDER BY id"#, &[r#"obj"#][..]),
        (r#"SELECT k FROM jp WHERE id > 0 AND (jp.jb -> 'b') ? 'c' ORDER BY id"#, &[r#"obj"#][..]),
        (r#"SELECT k FROM jp WHERE (jb -> 2) @> '[3]' ORDER BY id"#, &[r#"arr"#][..]),
    ];
    check(&pool, cases).await;
}

/// `doc -> -1` in a WHERE: its constant is rewritten `- 1` before the path is read, which then was not one
/// ("unexpected factor: Op(\">\")").
#[tokio::test(flavor = "multi_thread")]
async fn a_negative_index_in_a_where() {
    let (_server, pool) = start().await;
    let cases: &[(&str, &[&str])] = &[
        (r#"SELECT id FROM jp WHERE (jb -> -1) IS NOT NULL ORDER BY id"#, &[r#"2"#, r#"3"#, r#"4"#, r#"5"#, r#"6"#][..]),
        (r#"SELECT id FROM jp WHERE jb -> -1 IS NULL ORDER BY id"#, &[r#"1"#, r#"7"#, r#"8"#, r#"9"#][..]),
        (r#"SELECT id FROM jp WHERE (jb ->> -1) IS NOT NULL ORDER BY id"#, &[r#"2"#, r#"3"#, r#"4"#, r#"5"#][..]),
        (r#"SELECT id FROM jp WHERE jb ->> -1 IS NULL ORDER BY id"#, &[r#"1"#, r#"6"#, r#"7"#, r#"8"#, r#"9"#][..]),
        (r#"SELECT k, (jb) #>> '{b,c,-1}' FROM jp ORDER BY id"#, &[r#"obj|20"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, (j) #>> '{b,c,-1}' FROM jp ORDER BY id"#, &[r#"obj|20"#, r#"arr|NULL"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k FROM jp WHERE jb -> -1 = 'true' ORDER BY id"#, &[r#"arr"#, r#"boo"#][..]),
        (r#"SELECT k FROM jp WHERE jb ->> -1 = 'true' ORDER BY id"#, &[r#"arr"#, r#"boo"#][..]),
        (r#"SELECT k FROM jp WHERE jb -> 'b' -> 'c' ->> -1 = '20' ORDER BY id"#, &[r#"obj"#][..]),
        (r#"SELECT id FROM jp WHERE jb -> - 1 = 'true' ORDER BY id"#, &[r#"2"#, r#"5"#][..]),
        (r#"SELECT k, jb -> - 1, jb ->> -1 FROM jp ORDER BY id"#, &[r#"obj|NULL|NULL"#, r#"arr|true|true"#, r#"str|"hello"|hello"#, r#"num|42|42"#, r#"boo|true|true"#, r#"nul|null|NULL"#, r#"eobj|NULL|NULL"#, r#"earr|NULL|NULL"#, r#"sqlnull|NULL|NULL"#][..]),
    ];
    check(&pool, cases).await;
}

/// A function in a subquery's FROM over the outer row's column, under EXISTS and IN: the subquery was run
/// on its own ("missing FROM-clause entry for table", "column … does not exist"). Qualified or bare, with a
/// string or a number on the left of IN, and NOT IN with PostgreSQL's NULL rule.
#[tokio::test(flavor = "multi_thread")]
async fn a_function_over_the_outer_row_inside_exists_and_in() {
    let (_server, pool) = start().await;
    let cases: &[(&str, &[&str])] = &[
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(jp.arr) AS e(value) WHERE e.value = '2') ORDER BY id"#, &[r#"1"#, r#"3"#, r#"5"#, r#"7"#][..]),
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(jp.arr) AS e WHERE e.value = '"a"') ORDER BY id"#, &[r#"2"#, r#"4"#, r#"6"#, r#"8"#][..]),
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(arr) e) ORDER BY id"#, &[r#"1"#, r#"2"#, r#"3"#, r#"4"#, r#"5"#, r#"6"#, r#"7"#, r#"8"#][..]),
        (r#"SELECT id FROM jp WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(jp.arr) AS e(value) WHERE e.value = '2') ORDER BY id"#, &[r#"2"#, r#"4"#, r#"6"#, r#"8"#, r#"9"#][..]),
        (r#"SELECT id FROM jp WHERE 'a' IN (SELECT e.value FROM jsonb_array_elements_text(jp.arr) AS e(value)) ORDER BY id"#, &[r#"2"#, r#"4"#, r#"6"#, r#"8"#][..]),
        (r#"SELECT id FROM jp WHERE 'x' NOT IN (SELECT e.value FROM jsonb_array_elements_text(jp.arr) AS e(value)) ORDER BY id"#, &[r#"1"#, r#"2"#, r#"3"#, r#"4"#, r#"5"#, r#"6"#, r#"7"#, r#"8"#, r#"9"#][..]),
        (r#"SELECT id FROM jp WHERE id IN (SELECT g FROM generate_series(1, jp.id - 5) AS g) ORDER BY id"#, &[][..]),
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT 1 FROM generate_series(1, jp.id) AS g WHERE g = 7) ORDER BY id"#, &[r#"7"#, r#"8"#, r#"9"#][..]),
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT 1 FROM unnest(ARRAY[jp.id, 100]) AS u(x) WHERE u.x = 3) ORDER BY id"#, &[r#"3"#][..]),
        (r#"SELECT id, EXISTS (SELECT 1 FROM jsonb_array_elements_text(jp.arr) AS e(value) WHERE e.value = '2') FROM jp ORDER BY id"#, &[r#"1|t"#, r#"2|f"#, r#"3|t"#, r#"4|f"#, r#"5|t"#, r#"6|f"#, r#"7|t"#, r#"8|f"#, r#"9|f"#][..]),
        (r#"SELECT id FROM jp WHERE 'b' IN (SELECT e.value FROM jsonb_array_elements_text(jp.arr) AS e(value) WHERE e.value <> 'a') ORDER BY id"#, &[r#"2"#, r#"4"#, r#"6"#, r#"8"#][..]),
        (r#"SELECT id FROM jp WHERE '9' NOT IN (SELECT e.value FROM jsonb_array_elements_text(jp.arr || '[null]'::jsonb) AS e(value)) ORDER BY id"#, &[r#"9"#][..]),
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(jp.arr) AS e(value)) AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(jp.arr) AS f(value) WHERE f.value = 'a') ORDER BY id"#, &[r#"1"#, r#"3"#, r#"5"#, r#"7"#][..]),
        (r#"SELECT count(*) FROM jp WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(jp.arr) AS e(value) WHERE e.value = '3')"#, &[r#"4"#][..]),
        (r#"SELECT id FROM jp x WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(x.arr) AS e(value) WHERE e.value = '3') ORDER BY id"#, &[r#"1"#, r#"3"#, r#"5"#, r#"7"#][..]),
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(jp.arr) WITH ORDINALITY AS e(value, n) WHERE e.n = 3) ORDER BY id"#, &[r#"1"#, r#"3"#, r#"5"#, r#"7"#][..]),
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(arr) AS e(value) WHERE e.value = 'b') ORDER BY id"#, &[r#"2"#, r#"4"#, r#"6"#, r#"8"#][..]),
        (r#"SELECT id FROM jp WHERE 'b' IN (SELECT value FROM jsonb_array_elements_text(arr)) ORDER BY id"#, &[r#"2"#, r#"4"#, r#"6"#, r#"8"#][..]),
        (r#"SELECT id FROM jp WHERE NOT EXISTS (SELECT 1 FROM jsonb_array_elements(arr)) ORDER BY id"#, &[r#"9"#][..]),
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT 1 FROM generate_series(1, id) g WHERE g = 8) ORDER BY id"#, &[r#"8"#, r#"9"#][..]),
        (r#"SELECT id FROM jp WHERE k IN (SELECT 'ob' || 'j') OR 'it''s' IN (SELECT e.value FROM jsonb_array_elements_text(jp.arr) AS e(value)) ORDER BY id"#, &[r#"1"#][..]),
    ];
    check(&pool, cases).await;
}

/// `->` and `->>` with an index over a scalar: the scalar itself for `jsonb`
/// (0 and -1; nothing at any other index), NULL for `json`. PostgreSQL's
/// answers, which the engine already gave: pinned so that neither side is
/// "corrected" into the other.
#[tokio::test(flavor = "multi_thread")]
async fn scalars_are_one_element_arrays_to_an_index() {
    let (_server, pool) = start().await;
    let cases: &[(&str, &[&str])] = &[
        (r#"SELECT '42'::jsonb -> 0"#, &[r#"42"#][..]),
        (r#"SELECT '"hello"'::json -> 0"#, &[r#"NULL"#][..]),
        (r#"SELECT k, jb ->> 0 FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|1"#, r#"str|hello"#, r#"num|42"#, r#"boo|true"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT k, j ->> 0 FROM jp ORDER BY id"#, &[r#"obj|NULL"#, r#"arr|1"#, r#"str|NULL"#, r#"num|NULL"#, r#"boo|NULL"#, r#"nul|NULL"#, r#"eobj|NULL"#, r#"earr|NULL"#, r#"sqlnull|NULL"#][..]),
        (r#"SELECT '"hello"'::jsonb ->> 0"#, &[r#"hello"#][..]),
        (r#"SELECT '42'::jsonb ->> 0"#, &[r#"42"#][..]),
        (r#"SELECT '"hello"'::json ->> 0"#, &[r#"NULL"#][..]),
        (r#"SELECT '"hello"'::jsonb -> -1"#, &[r#""hello""#][..]),
        (r#"SELECT '42'::jsonb -> 9"#, &[r#"NULL"#][..]),
        (r#"SELECT e.value ->> 0 FROM jsonb_array_elements('[1,"two",[3,4],{"k":5},null,true]'::jsonb) AS e(value)"#, &[r#"1"#, r#"two"#, r#"3"#, r#"NULL"#, r#"NULL"#, r#"true"#][..]),
    ];
    check(&pool, cases).await;
}

/// The same subqueries deciding a write.
#[tokio::test(flavor = "multi_thread")]
async fn a_function_over_the_row_decides_a_write() {
    let (_server, pool) = start().await;
    let cases: &[(&str, &[&str])] = &[
        (r#"DELETE FROM jp WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(jp.arr) AS e(value) WHERE e.value = 'b') AND id = 8 RETURNING id"#, &[r#"8"#][..]),
        (r#"UPDATE jp SET t = 'hit' WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(jp.arr) AS e(value) WHERE e.value = '2') RETURNING id"#, &[r#"1"#, r#"3"#, r#"5"#, r#"7"#][..]),
        // What the two writes left.
        (r#"SELECT id, t FROM jp ORDER BY id"#, &[r#"1|hit"#, r#"2|[1,"two",[3,4],{"k":5},null,true]"#, r#"3|hit"#, r#"4|42"#, r#"5|hit"#, r#"6|null"#, r#"7|hit"#, r#"9|NULL"#][..]),
    ];
    check(&pool, cases).await;
}
