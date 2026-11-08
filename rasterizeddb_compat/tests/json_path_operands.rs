//! What a JSON path may start from, and what a FROM-less body may hold.
//!
//! Every expected answer is PostgreSQL 17's, probed beside this engine
//! (`.claude/reviews/rbugs/p*.sql`). Before these were fixed:
//!
//! * `t.doc->>'k'` (a qualified column, as every ORM writes it in a join or
//!   under an alias) was looked up as a column named `doc->>'k'`: "not found".
//! * `->`/`->>` on a `text` column searched the text as an encoded document,
//!   found nothing and answered NULL, so `WHERE s->>'a' IS NULL` kept every
//!   row; PostgreSQL has no such operator.
//! * a path over anything but a column was read as part of a cast's type
//!   name (`'…'::jsonb->'a'`), or, after a closing parenthesis, as the item's
//!   alias: `coalesce(b, '{}')->'k'` returned the whole document.
//! * a `WITH` or derived table whose body had no FROM and a cast in it was
//!   refused as "must be a SELECT", and a `json` cast in one was built as a
//!   text column, on which `->` answered NULL.

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

async fn error(c: &tokio_postgres::Client, sql: &str) -> (String, String) {
    let e = c.simple_query(sql).await.expect_err(sql);
    let db = e.as_db_error().unwrap_or_else(|| panic!("{sql}: {e}"));
    (db.code().code().to_string(), db.message().to_string())
}

async fn server() -> (TestServer, tokio_postgres::Client) {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    for sql in [
        "CREATE TABLE ja (id INT PRIMARY KEY, d JSONB, t JSON)",
        "CREATE TABLE jb (id INT PRIMARY KEY, a_id INT, d JSONB, t JSON)",
        r#"INSERT INTO ja VALUES (1, '{"k":"x","n":1,"o":{"p":"q"},"arr":[1,2]}', '{"k":"tx","n":10}'),
           (2, '{"k":"y","n":2}', '{"k":"ty","n":20}')"#,
        r#"INSERT INTO jb VALUES (1, 1, '{"k":"bx","n":5}', '{"k":"btx"}'), (2, 2, '{"k":"by","n":6}', '{"k":"bty"}')"#,
        "CREATE TABLE tt (id INT PRIMARY KEY, s TEXT, v VARCHAR(10), n INT)",
        r#"INSERT INTO tt VALUES (1, '{"a":[1,2],"k":"v"}', '{"a":1}', 5)"#,
    ] {
        c.simple_query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    (server, c)
}

async fn check(c: &tokio_postgres::Client, cases: &[(&str, &[&str])]) {
    for (sql, want) in cases {
        assert_eq!(rows(c, sql).await, *want, "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_qualified_column_starts_a_path_wherever_it_is_written() {
    let (_server, c) = server().await;
    check(
        &c,
        &[
            ("SELECT ja.d->'k' FROM ja ORDER BY id", &[r#""x""#, r#""y""#]),
            ("SELECT ja.t->>'k' FROM ja ORDER BY id", &["tx", "ty"]),
            ("SELECT x.d->'o'->>'p' FROM ja x ORDER BY x.id", &["q", "NULL"]),
            ("SELECT x.d#>>'{o,p}' FROM ja x ORDER BY x.id", &["q", "NULL"]),
            ("SELECT x.d->>'k' AS kk FROM ja x ORDER BY kk", &["x", "y"]),
            ("SELECT (x.d->>'n')::int + 1 FROM ja x ORDER BY x.id", &["2", "3"]),
            ("SELECT upper(x.d->>'k') FROM ja x ORDER BY x.id", &["X", "Y"]),
            ("SELECT x.d->>'k' || '!' FROM ja x ORDER BY x.id", &["x!", "y!"]),
            ("SELECT x.id FROM ja x WHERE x.d->>'k' = 'y'", &["2"]),
            ("SELECT x.id FROM ja x ORDER BY x.d->>'k' DESC", &["2", "1"]),
            ("SELECT x.d->>'k', count(*) FROM ja x GROUP BY x.d->>'k' ORDER BY 1", &["x|1", "y|1"]),
            ("SELECT s.d->>'k' FROM (SELECT d FROM ja) s ORDER BY 1", &["x", "y"]),
            ("SELECT s.t->>'k' FROM (SELECT t FROM ja) s ORDER BY 1", &["tx", "ty"]),
            // Two relations with a `d` each: each reads its own.
            ("SELECT a.d->>'k', b.d->>'k' FROM ja a JOIN jb b ON b.a_id = a.id ORDER BY a.id", &["x|bx", "y|by"]),
            ("SELECT a.t->>'k', b.t->>'k' FROM ja a JOIN jb b ON b.a_id = a.id ORDER BY a.id", &["tx|btx", "ty|bty"]),
            ("SELECT a.id, upper(b.d->>'k') FROM ja a JOIN jb b ON b.a_id = a.id ORDER BY a.id", &["1|BX", "2|BY"]),
            (
                "SELECT a.id, coalesce(a.d->>'zz', b.d->>'k') FROM ja a JOIN jb b ON b.a_id = a.id ORDER BY a.id",
                &["1|bx", "2|by"],
            ),
            // An inner join on two paths is the filtered cross join it is,
            // not a key: `(b.d->>'k')` was resolved as a column of `(b`.
            (
                "SELECT a.id, b.id FROM ja a JOIN jb b ON (b.d->>'k') = ('b' || (a.d->>'k')) ORDER BY a.id",
                &["1|1", "2|2"],
            ),
            ("SELECT a.id, b.id FROM ja a JOIN jb b ON a.d->>'k' = b.d->>'k'", &[]),
            (
                "SELECT a.id, b.id FROM ja a JOIN jb b ON (a.d->>'n')::int + 4 = (b.d->>'n')::int ORDER BY a.id",
                &["1|1", "2|2"],
            ),
        ],
    )
    .await;
    // The qualifier still has to name a relation of the FROM.
    assert_eq!(error(&c, "SELECT zz.d->>'k' FROM ja x").await.0, "42P01");
    // RETURNING, and the row the write left.
    check(
        &c,
        &[
            (r#"UPDATE ja SET d = '{"k":"z"}' WHERE ja.d->>'k' = 'y' RETURNING ja.d->>'k'"#, &["z"]),
            ("SELECT ja.id, ja.d->>'k' FROM ja ORDER BY ja.id", &["1|x", "2|z"]),
        ],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_path_on_a_column_holding_no_document_is_refused() {
    let (_server, c) = server().await;
    for (sql, message) in [
        ("SELECT s->'a' FROM tt", "operator does not exist: text -> unknown"),
        ("SELECT s->>'a' FROM tt", "operator does not exist: text ->> unknown"),
        ("SELECT tt.s->>'a' FROM tt", "operator does not exist: text ->> unknown"),
        ("SELECT v->>'a' FROM tt", "operator does not exist: character varying ->> unknown"),
        ("SELECT n->'a' FROM tt", "operator does not exist: integer -> unknown"),
        ("SELECT s#>>'{a}' FROM tt", "operator does not exist: text #>> unknown"),
        // Kept every row before, and matched none.
        ("SELECT id FROM tt WHERE s->>'a' IS NULL", "operator does not exist: text ->> unknown"),
        ("SELECT id FROM tt WHERE s->>'a' = '1'", "operator does not exist: text ->> unknown"),
        ("SELECT id FROM tt ORDER BY s->>'a'", "operator does not exist: text ->> unknown"),
        ("SELECT ('x'::text)->'a'", "operator does not exist: text -> unknown"),
        ("SELECT (1)->'a'", "operator does not exist: integer -> unknown"),
    ] {
        assert_eq!(error(&c, sql).await, ("42883".to_string(), message.to_string()), "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_path_reads_from_a_cast_a_group_or_a_call() {
    let (_server, c) = server().await;
    check(
        &c,
        &[
            (r#"SELECT '{"a":[1,2]}'::json->'a'"#, &["[1,2]"]),
            (r#"SELECT '{"a":[1,2]}'::jsonb->'a'"#, &["[1, 2]"]),
            (r#"SELECT '{"a":[1,2]}'::jsonb->>'a'"#, &["[1, 2]"]),
            (r#"SELECT ('{"a":[1,2]}'::jsonb)->'a'"#, &["[1, 2]"]),
            (r#"SELECT ('{"a":[1,2]}'::jsonb)->'a'->>1"#, &["2"]),
            (r#"SELECT '{"a":{"b":"c"}}'::jsonb#>>'{a,b}'"#, &["c"]),
            ("SELECT s::jsonb->'a' FROM tt", &["[1, 2]"]),
            ("SELECT (s::jsonb)->'a' FROM tt", &["[1, 2]"]),
            ("SELECT s::json->'a' FROM tt", &["[1,2]"]),
            ("SELECT s::jsonb->>'k' FROM tt", &["v"]),
            ("SELECT pg_typeof(s::json->'a'), pg_typeof(s::jsonb->'a'), pg_typeof(s::jsonb->>'a') FROM tt", &["json|jsonb|text"]),
            ("SELECT id FROM tt WHERE s::jsonb->>'k' = 'v'", &["1"]),
            ("SELECT id FROM tt WHERE (s::jsonb)->>'k' = 'v'", &["1"]),
            ("SELECT id FROM tt ORDER BY s::jsonb->>'k'", &["1"]),
            ("SELECT s::jsonb->'a' AS x FROM tt", &["[1, 2]"]),
            // After a closing parenthesis the path was taken for the alias.
            ("SELECT (d)->'k' FROM ja WHERE id = 1", &[r#""x""#]),
            ("SELECT (d->'o')->>'p' FROM ja WHERE id = 1", &["q"]),
            (r#"SELECT coalesce(d, '{}')->'k' FROM ja WHERE id = 1"#, &[r#""x""#]),
            // The branch taken is the literal's text, read as a document.
            (r#"SELECT coalesce(NULL::jsonb, '{"k":1}')->'k', coalesce(NULL::jsonb, '[5]')->>0"#, &["1|5"]),
            ("SELECT jsonb_build_object('k', 1)->'k'", &["1"]),
            // A scalar is a one-element array to jsonb's `->`, not to `#>`.
            (r#"SELECT '"x"'::jsonb->0, '5'::jsonb->-1, '5'::jsonb->1, '5'::jsonb#>'{0}'"#, &[r#""x"|5|NULL|NULL"#]),
            ("SELECT json_build_object('k', 1)->'k'", &["1"]),
        ],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_body_with_no_from_materialises_whatever_it_selects() {
    let (_server, c) = server().await;
    check(
        &c,
        &[
            ("WITH c AS (SELECT 'x'::text AS d) SELECT d FROM c", &["x"]),
            ("WITH c AS (SELECT 1::int AS n) SELECT n FROM c", &["1"]),
            (r#"WITH c AS (SELECT '{"a":1}'::jsonb AS d) SELECT d->>'a' FROM c"#, &["1"]),
            ("WITH c AS (SELECT CAST('x' AS text) AS d) SELECT d FROM c", &["x"]),
            ("WITH c AS (SELECT 'x'::text AS d, 2::int AS n) SELECT d, n FROM c", &["x|2"]),
            ("WITH c AS (SELECT now()::date IS NOT NULL AS ok) SELECT ok FROM c", &["t"]),
            ("SELECT * FROM (SELECT 'x'::text AS d) s", &["x"]),
            ("WITH c AS (SELECT 1.50::numeric(10,2) AS n) SELECT n FROM c", &["1.50"]),
            ("WITH c AS (SELECT 'a'::text AS d UNION ALL SELECT 'b'::text) SELECT d FROM c ORDER BY d", &["a", "b"]),
            ("WITH c AS (SELECT '2024-01-02'::date AS d) SELECT d + 1 FROM c", &["2024-01-03"]),
            ("WITH c AS (SELECT NULL::int AS n) SELECT n IS NULL FROM c", &["t"]),
            ("WITH c AS (SELECT 1::int AS n WHERE false) SELECT count(*) FROM c", &["0"]),
            ("SELECT s.n FROM (SELECT 5::bigint AS n) s", &["5"]),
            ("WITH c AS (SELECT 'x'::text AS d) SELECT ja.id, c.d FROM ja, c ORDER BY ja.id", &["1|x", "2|x"]),
            // A `json` cast is a `json` column: `->` read NULL off a text one.
            (r#"WITH c AS (SELECT '{"a":[1,2]}'::json AS d) SELECT d->'a', pg_typeof(d) FROM c"#, &["[1,2]|json"]),
            (r#"SELECT d->'a' FROM (SELECT '{"a":[1,2]}'::json AS d) s"#, &["[1,2]"]),
            // Unnamed items take PostgreSQL's names for them.
            ("WITH c AS (SELECT 'x'::text) SELECT * FROM c", &["x"]),
            ("WITH c AS (SELECT 1::int, 'y'::varchar) SELECT * FROM c", &["1|y"]),
            ("WITH c AS (SELECT count(*) FROM ja) SELECT count FROM c", &["2"]),
        ],
    )
    .await;
}
