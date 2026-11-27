//! A subquery run for each outer row is run only for the rows the
//! statement's own conditions keep, whether those name the outer table
//! (`t.id = 1`) or not (`id = 1`).
//!
//! PostgreSQL filters before it evaluates: `WHERE id = 1 AND EXISTS (SELECT 1
//! FROM jsonb_each(t.doc) …)` never calls `jsonb_each` for another row. Here
//! the function ran for every row unless the condition was written
//! `t.id = 1`, and for every row of an UPDATE or a DELETE whatever was
//! written, so one row holding an array failed the statement ("cannot call
//! jsonb_each on a non-object"). `jp` holds a document of every kind: every
//! statement below reads a function that fails on most of its rows.
//!
//! Each failed before the fix; each expectation is PostgreSQL 17's.

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

/// A bare outer condition narrows the rows, in a WHERE with EXISTS, IN, a
/// scalar subquery, and beside a LATERAL item or a function in the FROM.
#[tokio::test(flavor = "multi_thread")]
async fn a_bare_outer_condition_keeps_the_function_off_other_rows() {
    let (_server, pool) = start().await;
    let cases: &[(&str, &[&str])] = &[
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT 1 FROM jsonb_each_text(jp.jb) AS m WHERE m.value = '1') AND id = 1 ORDER BY id"#, &[r#"1"#][..]),
        (r#"SELECT id FROM jp WHERE id = 1 AND EXISTS (SELECT 1 FROM jsonb_each(jb) AS m WHERE m.key = 'a')"#, &[r#"1"#][..]),
        (r#"SELECT id FROM jp WHERE k = 'arr' AND EXISTS (SELECT 1 FROM json_array_elements_text(jp.j) AS e(value) WHERE e.value = 'two')"#, &[r#"2"#][..]),
        (r#"SELECT id FROM jp WHERE k IN ('arr', 'earr') AND 'two' IN (SELECT jsonb_array_elements_text(jb)) ORDER BY id"#, &[r#"2"#][..]),
        (r#"SELECT id, (SELECT count(*) FROM jsonb_object_keys(jp.jb)) FROM jp WHERE id = 1 OR k = 'eobj' ORDER BY id"#, &[r#"1|5"#, r#"7|0"#][..]),
        (r#"SELECT id, (SELECT count(*) FROM jsonb_each(jp.jb)) FROM jp WHERE k LIKE '%obj' AND id BETWEEN 1 AND 7 ORDER BY id"#, &[r#"1|5"#, r#"7|0"#][..]),
        (r#"SELECT jp.id, m.key FROM jp CROSS JOIN LATERAL jsonb_each(jp.jb) AS m WHERE id = 1 ORDER BY m.key"#, &[r#"1|0"#, r#"1|a"#, r#"1|b"#, r#"1|n"#, r#"1|s"#][..]),
        (r#"SELECT jp.id, m.key FROM jp, jsonb_each_text(jp.jb) AS m WHERE k = 'obj' AND m.key <> 'b' ORDER BY m.key"#, &[r#"1|0"#, r#"1|a"#, r#"1|n"#, r#"1|s"#][..]),
        (r#"SELECT a.id, m.key FROM jp a CROSS JOIN LATERAL jsonb_each(a.jb) AS m WHERE id = 1 AND key = 'a'"#, &[r#"1|a"#][..]),
        (r#"SELECT jp.id, m.key, m.value FROM jp CROSS JOIN LATERAL jsonb_each_text(jp.jb) AS m WHERE id = 1 AND value = 'x'"#, &[r#"1|s|x"#][..]),
        (r#"SELECT id FROM jp WHERE EXISTS (SELECT 1 FROM jsonb_each_text(jp.jb) AS m WHERE m.value = '1') AND id < 2 AND k <> 'zz' ORDER BY id"#, &[r#"1"#][..]),
        (r#"SELECT id FROM jp WHERE NOT EXISTS (SELECT 1 FROM jsonb_each(jp.jb) AS m WHERE m.key = 'zz') AND id IN (1, 7) ORDER BY id"#, &[r#"1"#, r#"7"#][..]),
    ];
    check(&pool, cases).await;
}

/// The same in an UPDATE and a DELETE, the condition qualified or bare.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_evaluates_its_subquery_for_the_rows_it_may_touch() {
    let (_server, pool) = start().await;
    let cases: &[(&str, &[&str])] = &[
        (r#"UPDATE jp SET t = 'obj!' WHERE id = 1 AND EXISTS (SELECT 1 FROM jsonb_each(jp.jb) AS m WHERE m.key = 'a') RETURNING id"#, &[r#"1"#][..]),
        (r#"UPDATE jp SET t = 'q!' WHERE jp.id = 1 AND EXISTS (SELECT 1 FROM jsonb_each(jp.jb) AS m WHERE m.key = 'a') RETURNING id"#, &[r#"1"#][..]),
        (r#"UPDATE jp SET t = 'b!' WHERE id BETWEEN 1 AND 1 AND k = 'obj' AND (SELECT count(*) FROM jsonb_object_keys(jp.jb)) > 2 RETURNING id"#, &[r#"1"#][..]),
        (r#"DELETE FROM jp WHERE k = 'eobj' AND NOT EXISTS (SELECT 1 FROM jsonb_each(jp.jb) AS m) RETURNING id"#, &[r#"7"#][..]),
        (r#"SELECT id, t FROM jp WHERE id IN (1, 7) OR t IS NULL ORDER BY id"#, &[r#"1|b!"#, r#"9|NULL"#][..]),
    ];
    check(&pool, cases).await;
}

/// A condition calling a function that answers the same every time
/// (`lower`, `coalesce`, `substr`, …) or casting narrows the rows too. It
/// was left out of what is pushed, as any call was.
#[tokio::test(flavor = "multi_thread")]
async fn a_condition_over_an_immutable_function_or_a_cast_narrows_too() {
    let (_server, pool) = start().await;
    let cases: &[(&str, &[&str])] = &[
        (r#"SELECT id FROM jp WHERE lower(k) = 'obj' AND EXISTS (SELECT 1 FROM jsonb_each(jp.jb) AS m WHERE m.key = 'a') ORDER BY id"#, &[r#"1"#][..]),
        (r#"SELECT id FROM jp WHERE upper(jp.k) LIKE '%OBJ' AND length(k) < 5 AND (SELECT count(*) FROM jsonb_each(jp.jb)) >= 0 ORDER BY id"#, &[r#"1"#, r#"7"#][..]),
        (r#"SELECT id FROM jp WHERE coalesce(id, 0) + 1 = 2 AND EXISTS (SELECT 1 FROM jsonb_each_text(jp.jb) AS m WHERE m.value = '1')"#, &[r#"1"#][..]),
        (r#"SELECT id FROM jp WHERE CAST(id AS text) = '1' AND 'a' IN (SELECT jsonb_object_keys(jb))"#, &[r#"1"#][..]),
        (r#"SELECT id FROM jp WHERE id::text = '7' AND NOT EXISTS (SELECT 1 FROM jsonb_each(jp.jb) AS m)"#, &[r#"7"#][..]),
        (r#"SELECT jp.id, m.key FROM jp CROSS JOIN LATERAL jsonb_each(jp.jb) AS m WHERE substr(k, 1, 3) = 'obj' ORDER BY m.key"#, &[r#"1|0"#, r#"1|a"#, r#"1|b"#, r#"1|n"#, r#"1|s"#][..]),
        (r#"UPDATE jp SET t = 'c!' WHERE lower(k) = 'obj' AND EXISTS (SELECT 1 FROM jsonb_each(jp.jb) AS m WHERE m.key = 'a') RETURNING id"#, &[r#"1"#][..]),
    ];
    check(&pool, cases).await;
}

/// And one comparing the row with a parameter, which is one value for the
/// whole statement.
#[tokio::test(flavor = "multi_thread")]
async fn a_condition_over_a_parameter_narrows_too() {
    let server = TestServer::start().await.expect("server");
    let (client, connection) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(connection);
    client.batch_execute(SETUP).await.expect("setup");
    let one = client
        .query("SELECT id FROM jp WHERE id = $1 AND EXISTS (SELECT 1 FROM jsonb_each(jp.jb) AS m WHERE m.key = 'a')", &[&1i32])
        .await
        .expect("an integer parameter");
    assert_eq!(one.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(), [1]);
    let two = client
        .query("SELECT id FROM jp WHERE lower(k) = $1 AND EXISTS (SELECT 1 FROM jsonb_each(jp.jb) AS m WHERE m.key = $2)", &[&"obj", &"a"])
        .await
        .expect("two text parameters");
    assert_eq!(two.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(), [1]);
    let counted = client
        .query("SELECT id FROM jp WHERE k = $1 AND (SELECT count(*) FROM jsonb_object_keys(jp.jb)) = 5", &[&"obj"])
        .await
        .expect("a scalar subquery");
    assert_eq!(counted.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(), [1]);
    let written = client
        .query("UPDATE jp SET t = 'd!' WHERE id = $1 AND EXISTS (SELECT 1 FROM jsonb_each(jp.jb) AS m WHERE m.key = 'a') RETURNING id", &[&1i32])
        .await
        .expect("a write");
    assert_eq!(written.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(), [1]);
}
