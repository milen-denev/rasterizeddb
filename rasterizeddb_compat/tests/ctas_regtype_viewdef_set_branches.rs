//! Gaps the compatibility document listed, each as PostgreSQL 17 answers it:
//!
//! - `CREATE TABLE … AS VALUES (…)` ("over anything but a SELECT or TABLE is
//!   not supported"), its columns `column1`, `column2`;
//! - `regtype` from a number (`23::regtype`, `CAST(20 AS regtype)`), from a
//!   name with a modifier (`'numeric(10,2)'`, `'timestamp(3) with time
//!   zone'`), from an enum's name, and 42704 for a name no type has: each was
//!   "cast to 'regtype' is not supported yet";
//! - `pg_get_viewdef`, which no statement could call;
//! - a set operation whose branches are in parentheses: a correlated
//!   subquery or an `EXISTS` in such a branch was refused, a group
//!   (`(a UNION b) INTERSECT c`) was read as a relation with no name, and a
//!   parenthesised query as an `INSERT`'s or a `CREATE TABLE AS`'s source was
//!   read as one value.
//!
//! Every expectation is PostgreSQL 17's answer to the same statement, in the
//! same order, except `pg_get_viewdef`'s text: PostgreSQL prints its own
//! layout of the view's query, and this answers the query as it was written
//! (what `pg_views.definition` shows here).

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Column, Row};

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

async fn error(pool: &sqlx::PgPool, sql: &str) -> (String, String) {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_all(pool).await.expect_err(sql);
    let db = e.as_database_error().unwrap_or_else(|| panic!("`{sql}`: no database error: {e}"));
    (db.code().map(|c| c.to_string()).unwrap_or_default(), db.message().to_string())
}

async fn run_cases(pool: &sqlx::PgPool, cases: Vec<(&str, Vec<&str>)>) {
    for (sql, expected) in cases {
        assert_eq!(rows(pool, sql).await, expected, "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn create_table_as_values_and_regtype_answer_as_postgres() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (r#"CREATE TABLE src (id int PRIMARY KEY, g text, v numeric(10,2), ts timestamptz)"#, vec![]),
        (r#"INSERT INTO src VALUES (1, 'a', 1.50, '2026-01-01 00:00:00+00'), (2, 'a', 2.25, NULL), (3, 'b', NULL, NULL)"#, vec![]),
        (r#"CREATE TABLE cta6 AS VALUES (1, 'x'), (2, 'y')"#, vec![]),
        (r#"SELECT * FROM cta6 ORDER BY 1"#, vec![r#"1|x"#, r#"2|y"#]),
        (r#"SELECT column1, column2 FROM cta6 WHERE column1 = 2"#, vec![r#"2|y"#]),
        (r#"CREATE TABLE cv1 AS VALUES (1.5, true, NULL), (2, false, 'z')"#, vec![]),
        (r#"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'cv1' ORDER BY ordinal_position"#, vec![r#"column1|numeric"#, r#"column2|boolean"#, r#"column3|text"#]),
        (r#"SELECT * FROM cv1 ORDER BY 1"#, vec![r#"1.5|t|NULL"#, r#"2|f|z"#]),
        (r#"CREATE TABLE cv2 (a, b) AS VALUES (1, 'x')"#, vec![]),
        (r#"SELECT a, b FROM cv2"#, vec![r#"1|x"#]),
        (r#"CREATE TABLE cv3 AS VALUES (1), (2) WITH NO DATA"#, vec![]),
        (r#"SELECT count(*) FROM cv3"#, vec![r#"0"#]),
        (r#"CREATE TEMP TABLE cv5 AS VALUES ('t')"#, vec![]),
        (r#"CREATE TABLE IF NOT EXISTS cv2 AS VALUES (9, 'no')"#, vec![]),
        (r#"SELECT a, b FROM cv2"#, vec![r#"1|x"#]),
        (r#"CREATE TABLE cta1 AS SELECT id, g FROM src WHERE id < 3"#, vec![]),
        (r#"SELECT * FROM cta1 ORDER BY id"#, vec![r#"1|a"#, r#"2|a"#]),
        (r#"CREATE TABLE cta2 AS SELECT g, sum(v) AS total, count(*) AS c FROM src GROUP BY g"#, vec![]),
        (r#"SELECT * FROM cta2 ORDER BY g"#, vec![r#"a|3.75|2"#, r#"b|NULL|1"#]),
        (r#"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'cta2' ORDER BY ordinal_position"#, vec![r#"g|text"#, r#"total|numeric"#, r#"c|bigint"#]),
        (r#"CREATE TABLE cta7 AS TABLE src"#, vec![]),
        (r#"SELECT count(*) FROM cta7"#, vec![r#"3"#]),
        (r#"SELECT 'int4'::regtype, 'integer'::regtype::oid, 'text'::regtype = 'varchar'::regtype, 'timestamptz'::regtype::text"#, vec![r#"integer|23|f|timestamp with time zone"#]),
        (r#"SELECT 23::regtype, 1082::regtype::text, 'numeric(10,2)'::regtype"#, vec![r#"integer|date|numeric"#]),
        (r#"SELECT '23'::regtype, 16::regtype, 1700::regtype::text, 'int4[]'::regtype, 'varchar(10)'::regtype, 'timestamp(3) with time zone'::regtype"#, vec![r#"integer|boolean|numeric|integer[]|character varying|timestamp with time zone"#]),
        (r#"SELECT 99999::regtype"#, vec![r#"99999"#]),
        (r#"SELECT CAST('int8' AS regtype), CAST(20 AS regtype)"#, vec![r#"bigint|bigint"#]),
        (r#"SELECT 23::regtype::oid, 'bool'::regtype::oid = 16"#, vec![r#"23|t"#]),
        (r#"SELECT typname FROM pg_type WHERE oid = 'bool'::regtype"#, vec![r#"bool"#]),
        (r#"SELECT typname FROM pg_type WHERE oid = 1700::regtype"#, vec![r#"numeric"#]),
        (r#"SELECT id FROM src WHERE pg_typeof(v) = 'numeric'::regtype ORDER BY id"#, vec![r#"1"#, r#"2"#, r#"3"#]),
        (r#"SELECT format_type(23::regtype, NULL), format_type('numeric(10,2)'::regtype, NULL)"#, vec![r#"integer|numeric"#]),
        (r#"CREATE TYPE mood AS ENUM ('sad', 'ok')"#, vec![]),
        (r#"SELECT 'mood'::regtype, 'mood'::regtype::oid > 0"#, vec![r#"mood|t"#]),
        (r#"SELECT 'public.mood'::regtype::text"#, vec![r#"mood"#]),
        (r#"SELECT 123 AS n, '23' AS t, 5::int, 7::bigint::text"#, vec![r#"123|23|5|7"#]),
    ];
    run_cases(&pool, cases).await;

    // A name no type has: PostgreSQL's code and message.
    assert_eq!(
        error(&pool, "SELECT 'nosuchtype'::regtype").await,
        ("42704".to_string(), r#"type "nosuchtype" does not exist"#.to_string())
    );
    assert_eq!(error(&pool, "SELECT 1 WHERE 23 = 'nosuch(3)'::regtype").await.0, "42704");
    // Rows of different lengths: PostgreSQL's message.
    let (_, message) = error(&pool, "CREATE TABLE cv4 AS VALUES (1, 2), (3)").await;
    assert_eq!(message, "VALUES lists must all be the same length");
    assert_eq!(error(&pool, "SELECT * FROM cv4").await.0, "42P01");
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_get_viewdef_answers_the_views_query() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    for sql in [
        "CREATE TABLE vd_t (id int PRIMARY KEY, v text)",
        "CREATE VIEW vd_v AS SELECT id, v FROM vd_t WHERE id > 1",
        r#"CREATE VIEW "Vd W" AS SELECT v FROM vd_t WHERE v <> 'it''s'"#,
    ] {
        rows(&pool, sql).await;
    }
    let query = "SELECT id, v FROM vd_t WHERE id > 1";
    // By oid (`'v'::regclass`), with PostgreSQL's pretty flag or wrap column,
    // by name, and schema-qualified: one text, the one `pg_views` shows.
    for sql in [
        "SELECT pg_get_viewdef('vd_v'::regclass)",
        "SELECT pg_get_viewdef('vd_v'::regclass, true)",
        "SELECT pg_get_viewdef('vd_v'::regclass, 80)",
        "SELECT pg_get_viewdef('vd_v')",
        "SELECT pg_get_viewdef('public.vd_v')",
        "SELECT pg_catalog.pg_get_viewdef('vd_v'::regclass, true)",
        "SELECT definition FROM pg_views WHERE viewname = 'vd_v'",
    ] {
        assert_eq!(rows(&pool, sql).await, vec![query], "{sql}");
    }
    assert_eq!(rows(&pool, r#"SELECT pg_get_viewdef('"Vd W"'::regclass)"#).await, vec!["SELECT v FROM vd_t WHERE v <> 'it''s'"]);
    // The column is named as PostgreSQL names it.
    let described = sqlx::raw_sql(AssertSqlSafe("SELECT pg_get_viewdef('vd_v'::regclass)".to_string())).fetch_all(&pool).await.expect("viewdef");
    assert_eq!(described[0].columns()[0].name(), "pg_get_viewdef");

    // PostgreSQL 17's answers from here on.
    let cases: Vec<(&str, Vec<&str>)> = vec![
        // A table is no view, nor is an oid nothing has: NULL.
        ("SELECT pg_get_viewdef('vd_t'::regclass)", vec!["NULL"]),
        ("SELECT pg_get_viewdef('vd_t')", vec!["NULL"]),
        ("SELECT pg_get_viewdef(0)", vec!["NULL"]),
        // Over the catalogue's rows, the way a schema browser asks.
        ("SELECT relname, pg_get_viewdef(oid) IS NULL FROM pg_class WHERE relname IN ('vd_t', 'vd_v') ORDER BY 1", vec!["vd_t|t", "vd_v|f"]),
        ("SELECT c.relname FROM pg_class c WHERE c.relkind = 'v' AND pg_get_viewdef(c.oid) LIKE '%vd_t%' ORDER BY 1", vec!["Vd W", "vd_v"]),
        ("SELECT c.relname FROM pg_class c WHERE c.relkind = 'v' AND pg_get_viewdef(c.oid) LIKE '%it''''s%' ORDER BY 1", vec!["Vd W"]),
        ("SELECT length(pg_get_viewdef('vd_v'::regclass)) > 0 AS has", vec!["t"]),
        // The words in a string are not a call.
        ("SELECT 'pg_get_viewdef(1)' AS t", vec!["pg_get_viewdef(1)"]),
    ];
    run_cases(&pool, cases).await;
    assert_eq!(
        error(&pool, "SELECT pg_get_viewdef('vd_nosuch')").await,
        ("42P01".to_string(), r#"relation "vd_nosuch" does not exist"#.to_string())
    );

    // A view replaced, then dropped, is answered as it then is.
    rows(&pool, "CREATE OR REPLACE VIEW vd_v AS SELECT id, v FROM vd_t WHERE id > 2").await;
    assert_eq!(rows(&pool, "SELECT pg_get_viewdef('vd_v')").await, vec!["SELECT id, v FROM vd_t WHERE id > 2"]);
    rows(&pool, "DROP VIEW vd_v").await;
    assert_eq!(error(&pool, "SELECT pg_get_viewdef('vd_v')").await.0, "42P01");
}

#[tokio::test(flavor = "multi_thread")]
async fn parenthesised_set_operation_branches_answer_as_postgres() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (r#"CREATE TABLE src (id int PRIMARY KEY, g text, v numeric(10,2), ts timestamptz)"#, vec![]),
        (r#"INSERT INTO src VALUES (1, 'a', 1.50, '2026-01-01 00:00:00+00'), (2, 'a', 2.25, NULL), (3, 'b', NULL, NULL)"#, vec![]),
        (r#"(SELECT id FROM src s WHERE EXISTS (SELECT 1 FROM src o WHERE o.g = s.g AND o.id <> s.id)) ORDER BY 1"#, vec![r#"1"#, r#"2"#]),
        (r#"(SELECT id FROM src s WHERE EXISTS (SELECT 1 FROM src o WHERE o.g = s.g AND o.id <> s.id)) EXCEPT SELECT 2"#, vec![r#"1"#]),
        (r#"(SELECT id FROM src s WHERE EXISTS (SELECT 1 FROM src o WHERE o.g = s.g)) EXCEPT SELECT 2 ORDER BY 1"#, vec![r#"1"#, r#"3"#]),
        (r#"(SELECT id, (SELECT max(o.v) FROM src o WHERE o.g = s.g) FROM src s) EXCEPT (SELECT 3, 2.25) ORDER BY 1"#, vec![r#"1|2.25"#, r#"2|2.25"#, r#"3|NULL"#]),
        (r#"(SELECT id FROM src s WHERE v = (SELECT max(o.v) FROM src o WHERE o.g = s.g)) UNION ALL SELECT 99 ORDER BY 1"#, vec![r#"2"#, r#"99"#]),
        (r#"(SELECT id FROM src s WHERE v = (SELECT max(o.v) FROM src o WHERE o.g = s.g)) UNION ALL (SELECT 99) ORDER BY 1"#, vec![r#"2"#, r#"99"#]),
        (r#"SELECT 99 UNION ALL (SELECT id FROM src s WHERE v = (SELECT max(o.v) FROM src o WHERE o.g = s.g)) ORDER BY 1"#, vec![r#"2"#, r#"99"#]),
        (r#"SELECT 99 UNION ALL (SELECT id FROM src s WHERE EXISTS (SELECT 1 FROM src o WHERE o.g = s.g AND o.id <> s.id)) ORDER BY 1"#, vec![r#"1"#, r#"2"#, r#"99"#]),
        (r#"(SELECT id FROM src s WHERE EXISTS (SELECT 1 FROM src o WHERE o.g = s.g AND o.id <> s.id)) INTERSECT ALL (SELECT id FROM src s WHERE NOT EXISTS (SELECT 1 FROM src o WHERE o.g = s.g AND o.id > s.id)) ORDER BY 1"#, vec![r#"2"#]),
        (r#"(SELECT g, (SELECT count(*) FROM src o WHERE o.g = s.g) FROM src s) UNION (SELECT 'z', 0) ORDER BY 1"#, vec![r#"a|2"#, r#"b|1"#, r#"z|0"#]),
        (r#"(SELECT id FROM src WHERE id < 3) EXCEPT SELECT id FROM src WHERE id = 1"#, vec![r#"2"#]),
        (r#"(SELECT id FROM src WHERE id IN (SELECT id FROM src WHERE g = 'a')) UNION (SELECT 9) ORDER BY 1"#, vec![r#"1"#, r#"2"#, r#"9"#]),
        (r#"(SELECT id FROM src ORDER BY id DESC LIMIT 1) UNION ALL (SELECT id FROM src ORDER BY id LIMIT 1)"#, vec![r#"3"#, r#"1"#]),
        (r#"(SELECT id FROM src ORDER BY id DESC LIMIT 2)"#, vec![r#"3"#, r#"2"#]),
        (r#"(SELECT id FROM src) ORDER BY id DESC LIMIT 2"#, vec![r#"3"#, r#"2"#]),
        (r#"(SELECT 1 UNION SELECT 2) INTERSECT SELECT 2"#, vec![r#"2"#]),
        (r#"SELECT 2 INTERSECT (SELECT 1 UNION SELECT 2)"#, vec![r#"2"#]),
        (r#"SELECT 1 EXCEPT (SELECT 1 UNION SELECT 2)"#, vec![]),
        (r#"(SELECT 1 UNION SELECT 2) ORDER BY 1 DESC"#, vec![r#"2"#, r#"1"#]),
        (r#"(SELECT 1 UNION SELECT 2) ORDER BY 1"#, vec![r#"1"#, r#"2"#]),
        (r#"(SELECT id FROM src UNION SELECT 9) EXCEPT (SELECT 1 UNION ALL SELECT 2) ORDER BY 1"#, vec![r#"3"#, r#"9"#]),
        (r#"(SELECT 1) UNION (SELECT 2) INTERSECT (SELECT 3) ORDER BY 1"#, vec![r#"1"#]),
        (r#"((SELECT 1) UNION (SELECT 2)) INTERSECT (SELECT 2) ORDER BY 1"#, vec![r#"2"#]),
        (r#"((SELECT id FROM src s WHERE EXISTS (SELECT 1 FROM src o WHERE o.g = s.g AND o.id <> s.id))) ORDER BY 1"#, vec![r#"1"#, r#"2"#]),
        (r#"((SELECT 1)) UNION ALL ((SELECT 2)) ORDER BY 1"#, vec![r#"1"#, r#"2"#]),
        (r#"(SELECT 'a (b) UNION c' AS t) UNION ALL (SELECT ') EXCEPT (') ORDER BY 1"#, vec![r#") EXCEPT ("#, r#"a (b) UNION c"#]),
        (r#"SELECT id FROM src WHERE g IN ('a', 'union') UNION ALL SELECT 7 ORDER BY 1"#, vec![r#"1"#, r#"2"#, r#"7"#]),
        (r#"SELECT count(*) FROM ((SELECT id FROM src) UNION ALL (SELECT id FROM src)) u"#, vec![r#"6"#]),
        (r#"SELECT u.id FROM ((SELECT id FROM src WHERE id < 3) EXCEPT (SELECT 1)) AS u ORDER BY 1"#, vec![r#"2"#]),
        (r#"SELECT s.id FROM src s JOIN ((SELECT 1 AS id) UNION (SELECT 3)) k ON k.id = s.id ORDER BY 1"#, vec![r#"1"#, r#"3"#]),
        (r#"SELECT count(*) FROM ((SELECT id FROM src)) u"#, vec![r#"3"#]),
        (r#"WITH q AS ((SELECT 1 AS a) UNION ALL (SELECT 2)) (SELECT a FROM q) EXCEPT (SELECT 2)"#, vec![r#"1"#]),
        (r#"WITH q AS ((SELECT id FROM src)) SELECT count(*) FROM q"#, vec![r#"3"#]),
        (r#"CREATE TABLE cu AS (SELECT id FROM src WHERE id < 3) UNION ALL (SELECT 50)"#, vec![]),
        (r#"SELECT * FROM cu ORDER BY 1"#, vec![r#"1"#, r#"2"#, r#"50"#]),
        (r#"CREATE TABLE cu2 AS (SELECT id, g FROM src WHERE id = 3)"#, vec![]),
        (r#"SELECT * FROM cu2"#, vec![r#"3|b"#]),
        (r#"CREATE TABLE cu3 (a, b) AS (SELECT id, g FROM src) WITH NO DATA"#, vec![]),
        (r#"SELECT count(*) FROM cu3"#, vec![r#"0"#]),
        (r#"CREATE TABLE cu4 AS (SELECT id FROM src ORDER BY id DESC LIMIT 2)"#, vec![]),
        (r#"SELECT * FROM cu4 ORDER BY 1"#, vec![r#"2"#, r#"3"#]),
        (r#"INSERT INTO src (id, g) (SELECT id + 10, g FROM src WHERE id = 1)"#, vec![]),
        (r#"INSERT INTO src (SELECT id + 30, g, v, ts FROM src WHERE id = 2)"#, vec![]),
        (r#"INSERT INTO src (id, g) (SELECT id + 100, g FROM src WHERE id = 1) UNION ALL (SELECT 200, 'q')"#, vec![]),
        (r#"INSERT INTO src (id, g) (SELECT 200, 'dup') ON CONFLICT (id) DO UPDATE SET g = EXCLUDED.g RETURNING id, g"#, vec![r#"200|dup"#]),
        (r#"INSERT INTO src (id, g) (SELECT 300, 'r') RETURNING id"#, vec![r#"300"#]),
        (r#"INSERT INTO src (id, g) ((SELECT 301, 'r2'))"#, vec![]),
        (r#"INSERT INTO src (id, g) (SELECT o.id + 400, s.g FROM src s JOIN src o ON o.id = s.id WHERE s.id = 1) UNION ALL SELECT k.id + 500, k.g FROM src k JOIN src j ON j.id = k.id WHERE k.id = 3"#, vec![]),
        (r#"INSERT INTO src (id, g) VALUES (600, 'v'), (601, '(SELECT 1)')"#, vec![]),
        (r#"INSERT INTO src (id, g) VALUES (602, (SELECT 'sub'))"#, vec![]),
        (r#"SELECT id, g, v FROM src WHERE id >= 10 ORDER BY 1"#, vec![r#"11|a|NULL"#, r#"32|a|2.25"#, r#"101|a|NULL"#, r#"200|dup|NULL"#, r#"300|r|NULL"#, r#"301|r2|NULL"#, r#"401|a|NULL"#, r#"503|b|NULL"#, r#"600|v|NULL"#, r#"601|(SELECT 1)|NULL"#, r#"602|sub|NULL"#]),
    ];
    run_cases(&pool, cases).await;

    // Parentheses that do group are kept: a branch's own ORDER BY beside
    // the whole's is refused, as PostgreSQL refuses it (42601).
    assert_eq!(error(&pool, "(SELECT id FROM src ORDER BY id LIMIT 2) ORDER BY id DESC").await.0, "42601");

    // The extended protocol describes and runs the same statement.
    let prepared: Vec<i32> = sqlx::query_scalar(
        "(SELECT id FROM src s WHERE EXISTS (SELECT 1 FROM src o WHERE o.g = s.g AND o.id <> s.id AND o.id < $1)) UNION ALL (SELECT 99) ORDER BY 1",
    )
    .bind(10_i32)
    .fetch_all(&pool)
    .await
    .expect("prepared");
    // The rows the INSERTs above added in group `a`, and 503 beside 3.
    assert_eq!(prepared, vec![1, 2, 11, 32, 99, 101, 401, 503]);
}

/// A file-backed server's directory; removed on drop.
struct OnDisk(std::path::PathBuf);

impl Drop for OnDisk {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The document said a partitioned table "is not rewritten yet"; it is. Each
/// answer is PostgreSQL 17's over the same table of four hash partitions.
#[tokio::test(flavor = "multi_thread")]
async fn an_alter_that_rewrites_a_partitioned_table_answers_as_postgres() {
    let root = OnDisk(std::env::temp_dir().join(format!("partitioned_rewrite_{}", std::process::id())));
    let _ = std::fs::remove_dir_all(&root.0);
    let root_text = root.0.to_string_lossy().into_owned();
    let server = TestServer::start_file_backed(&root_text).await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let cases: Vec<(&str, Vec<&str>)> = vec![
        ("CREATE TABLE pt (id int, tenant int, n numeric(10,2), t text, PRIMARY KEY (tenant, id)) PARTITION BY HASH (tenant)", vec![]),
        ("INSERT INTO pt SELECT g, g % 7, g * 1.5, 'r' || g FROM generate_series(1, 40) g", vec![]),
        ("SELECT count(*), sum(n) FROM pt", vec!["40|1230.00"]),
        ("ALTER TABLE pt ALTER COLUMN n TYPE numeric", vec![]),
        ("ALTER TABLE pt ADD COLUMN extra int DEFAULT 7", vec![]),
        ("ALTER TABLE pt ADD COLUMN u numeric", vec![]),
        ("UPDATE pt SET u = 1.123456789012345678 WHERE id = 1", vec![]),
        ("SELECT id, n, extra, u FROM pt WHERE id IN (1, 2) ORDER BY id", vec!["1|1.50|7|1.123456789012345678", "2|3.00|7|NULL"]),
        ("ALTER TABLE pt ALTER COLUMN t TYPE varchar(10)", vec![]),
        ("ALTER TABLE pt ALTER COLUMN extra TYPE bigint", vec![]),
        ("ALTER TABLE pt ALTER COLUMN extra TYPE text USING extra::text || 'x'", vec![]),
        ("ALTER TABLE pt ALTER COLUMN n TYPE numeric(12,4)", vec![]),
        ("ALTER TABLE pt DROP COLUMN u", vec![]),
        ("SELECT id, tenant, n, t, extra FROM pt WHERE id IN (1, 40) ORDER BY id", vec!["1|1|1.5000|r1|7x", "40|5|60.0000|r40|7x"]),
        ("SELECT count(*), sum(n), min(extra), max(t) FROM pt", vec!["40|1230.0000|7x|r9"]),
        ("ALTER TABLE pt ALTER COLUMN n TYPE int USING n::int", vec![]),
        ("SELECT sum(n) FROM pt", vec!["1240"]),
    ];
    run_cases(&pool, cases).await;
    // The partition key's type decides where a row is stored: refused, as
    // PostgreSQL refuses it.
    let (code, _) = error(&pool, "ALTER TABLE pt ALTER COLUMN tenant TYPE bigint").await;
    assert_eq!(code, "42P16");

    // After a restart the rewritten table reads the same.
    drop(pool);
    drop(server);
    let server = TestServer::start_file_backed(&root_text).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("connect");
    assert_eq!(rows(&pool, "SELECT count(*), sum(n), min(extra), max(t) FROM pt").await, vec!["40|1240|7x|r9"]);
    assert_eq!(rows(&pool, "SELECT id, tenant, n, t, extra FROM pt WHERE id = 40").await, vec!["40|5|60|r40|7x"]);
}
