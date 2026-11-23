//! A column whose name begins with a keyword is a column.
//!
//! The scanners that tell a subquery from an expression asked whether the
//! text after a `(` *starts with* `SELECT` (or `WITH`, `VALUES`), not whether
//! its first word *is* one. So `count(selected_branches)`, `abs(selected)`,
//! `(selection)` and `id IN (selected / 10, 2)` were each read as a subquery
//! named after the column and refused: "cannot parse subquery 'selected'"
//! (22P02). A benchmark met it as `count()` of a jsonb column, but the type
//! had nothing to do with it; only the name did.
//!
//! Every expectation here is PostgreSQL 17's answer to the same statement.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn setup() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        r#"CREATE TABLE s1 (id int primary key, selected int, selection text, j jsonb, with_x int, values_n int,
                            withdrawn bool, selected_branches jsonb);
           INSERT INTO s1 VALUES (1, 10, 'a', '[1]', 1, 5, true, '[1,2]'), (2, NULL, 'b', NULL, 2, 6, false, NULL),
                                 (3, 30, NULL, '{"a":2}', 3, 7, NULL, '{"a":1}');"#,
    ))
    .await
    .expect("setup");
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

async fn code(pool: &sqlx::PgPool, sql: &str) -> String {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_all(pool).await.expect_err(sql);
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| format!("no code: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn an_aggregate_over_a_column_named_like_select_counts_it() {
    let (_server, pool) = setup().await;
    for (sql, expected) in [
        // The two statements the benchmark sent, on its column name.
        ("SELECT count(selected_branches) FROM s1", vec!["2"]),
        (
            "SELECT 's1' t, count(*), count(selected_branches) jsonb_nonnull, sum(length(selection)) FROM s1",
            vec!["s1|3|2|2"],
        ),
        ("SELECT count(selected), count(selection) FROM s1", vec!["2|2"]),
        ("SELECT count(DISTINCT selected_branches), count(DISTINCT selected) FROM s1", vec!["2|2"]),
        ("SELECT min(selected), max(selected), sum(selected) FROM s1", vec!["10|30|40"]),
        ("SELECT sum(with_x), max(values_n), count(withdrawn) FROM s1", vec!["6|7|2"]),
        ("SELECT count(selected_branches) FROM s1 WHERE id > 1", vec!["1"]),
        ("SELECT count(selected_branches) FILTER (WHERE id > 1) FROM s1", vec!["1"]),
        ("SELECT id, count(selected_branches) FROM s1 GROUP BY id ORDER BY id", vec!["1|1", "2|0", "3|1"]),
        ("SELECT count(selected_branches->'a'), count(selected_branches::text) FROM s1", vec!["1|2"]),
        ("SELECT bool_and(selected_branches IS NOT NULL), bool_or(withdrawn) FROM s1", vec!["f|t"]),
        ("SELECT string_agg(selection, ',' ORDER BY id) FROM s1", vec!["a,b"]),
        ("SELECT jsonb_agg(selected_branches ORDER BY id) FROM s1", vec![r#"[[1, 2], null, {"a": 1}]"#]),
        ("SELECT array_agg(selected ORDER BY id) FROM s1", vec!["{10,NULL,30}"]),
        (
            "SELECT id, count(selected) OVER (), sum(selected) OVER (ORDER BY id) FROM s1 ORDER BY id",
            vec!["1|2|10", "2|2|10", "3|2|40"],
        ),
        (
            "SELECT selection, count(selected) FROM s1 GROUP BY selection HAVING count(selected) >= 0 ORDER BY selection",
            vec!["a|1", "b|0", "NULL|1"],
        ),
        ("WITH q AS (SELECT selected FROM s1) SELECT count(selected) FROM q", vec!["2"]),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_parenthesis_before_such_a_column_opens_an_expression() {
    let (_server, pool) = setup().await;
    for (sql, expected) in [
        ("SELECT (selected) + 1 FROM s1 ORDER BY id", vec!["11", "NULL", "31"]),
        ("SELECT (selected) FROM s1 ORDER BY id", vec!["10", "NULL", "30"]),
        ("SELECT (with_x), (values_n), (withdrawn) FROM s1 ORDER BY id", vec!["1|5|t", "2|6|f", "3|7|NULL"]),
        ("SELECT abs(selected), coalesce(selected, 0), upper(selection) FROM s1 ORDER BY id", vec![
            "10|10|A",
            "NULL|0|B",
            "30|30|NULL",
        ]),
        ("SELECT greatest(selected, with_x), least(values_n, selected) FROM s1 ORDER BY id", vec!["10|5", "2|6", "30|7"]),
        ("SELECT id, (selected IS NULL) FROM s1 ORDER BY id", vec!["1|f", "2|t", "3|f"]),
        ("SELECT id FROM s1 WHERE selected IN (selected, 2) ORDER BY id", vec!["1", "3"]),
        ("SELECT id FROM s1 WHERE (with_x + 0) = 2", vec!["2"]),
        ("SELECT id FROM s1 WHERE (values_n * 1) = 6", vec!["2"]),
        ("SELECT id FROM s1 WHERE (withdrawn) ORDER BY id", vec!["1"]),
        ("SELECT id, CASE WHEN (selected) > 5 THEN 'big' ELSE 'no' END FROM s1 ORDER BY id", vec![
            "1|big", "2|no", "3|big",
        ]),
        ("SELECT id FROM s1 ORDER BY (selected) NULLS LAST, id", vec!["1", "3", "2"]),
        ("SELECT a.id FROM s1 a JOIN s1 b ON (a.selected = b.selected) ORDER BY 1", vec!["1", "3"]),
        ("SELECT cardinality(array[selected, with_x]) FROM s1 ORDER BY id", vec!["2", "2", "2"]),
        // Writes: the column list, a SET, a RETURNING.
        ("UPDATE s1 SET selected = (selected) + 1 WHERE id = 1 RETURNING selected", vec!["11"]),
        ("INSERT INTO s1 (selected, id) VALUES (40, 4) RETURNING id, (selected)", vec!["4|40"]),
        (
            "INSERT INTO s1 (id, selected) SELECT id + 10, (selected) FROM s1 WHERE id = 1 RETURNING id, selected",
            vec!["11|11"],
        ),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
}

/// The word itself still opens a subquery, however it is spaced or cased.
#[tokio::test(flavor = "multi_thread")]
async fn the_keyword_itself_still_opens_a_subquery() {
    let (_server, pool) = setup().await;
    for (sql, expected) in [
        ("SELECT id FROM s1 WHERE selected = (SELECT max(selected) FROM s1)", vec!["3"]),
        ("SELECT id FROM s1 WHERE selected = (select max(selected) from s1)", vec!["3"]),
        ("SELECT id FROM s1 WHERE selected = ( SELECT max(selected) FROM s1 )", vec!["3"]),
        ("SELECT id FROM s1 WHERE selected IN (SELECT selected FROM s1 WHERE id = 1)", vec!["1"]),
        ("SELECT id FROM s1 WHERE selected IN(SELECT selected FROM s1 WHERE id = 1)", vec!["1"]),
        ("SELECT id, (SELECT max(selected) FROM s1) FROM s1 ORDER BY id", vec!["1|30", "2|30", "3|30"]),
        ("SELECT n FROM (SELECT selected AS n FROM s1) q ORDER BY n", vec!["10", "30", "NULL"]),
        ("SELECT n FROM (VALUES (1), (2)) v(n) ORDER BY n", vec!["1", "2"]),
        ("SELECT array(SELECT id FROM s1 ORDER BY id)", vec!["{1,2,3}"]),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
}

/// A key and a constraint on such a column, and a view over one.
#[tokio::test(flavor = "multi_thread")]
async fn such_a_column_can_be_a_key_a_check_and_a_view_column() {
    let (_server, pool) = setup().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE s2 (selected int primary key, n int);
         INSERT INTO s2 (selected, n) VALUES (1, 1), (2, 2);
         CREATE TABLE s3 (id int primary key, selected int CHECK (selected > 0),
                          sel2 int GENERATED ALWAYS AS (selected * 2) STORED);
         CREATE VIEW v1 AS SELECT id, (selected) AS s FROM s1;",
    ))
    .await
    .expect("objects");
    for (sql, expected) in [
        (
            "INSERT INTO s2 (selected, n) VALUES (1, 5) ON CONFLICT (selected) DO UPDATE SET n = excluded.n RETURNING n",
            vec!["5"],
        ),
        ("SELECT count(selected) FROM s2", vec!["2"]),
        ("INSERT INTO s3 (id, selected) VALUES (1, 5) RETURNING sel2", vec!["10"]),
        ("SELECT count(s) FROM v1", vec!["2"]),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
    assert_eq!(code(&pool, "INSERT INTO s3 (id, selected) VALUES (2, -5)").await, "23514");
}

/// PostgreSQL has no `sum`, `avg`, `min` or `max` of a document (42883).
/// `sum` and `avg` answered NULL, `min` and `max` a document picked by its
/// bytes. The aggregates it does have over one answer as before.
#[tokio::test(flavor = "multi_thread")]
async fn a_document_has_no_sum_or_extreme() {
    let (_server, pool) = setup().await;
    for sql in [
        "SELECT sum(j) FROM s1",
        "SELECT avg(j) FROM s1",
        "SELECT min(j) FROM s1",
        "SELECT max(selected_branches) FROM s1",
        "SELECT id, min(j) FROM s1 GROUP BY id",
        "SELECT sum(j->'a') FROM s1",
    ] {
        assert_eq!(code(&pool, sql).await, "42883", "{sql}");
    }
    for (sql, expected) in [
        ("SELECT count(j), count(DISTINCT j) FROM s1", vec!["2|2"]),
        ("SELECT jsonb_agg(j ORDER BY id) FROM s1", vec![r#"[[1], null, {"a": 2}]"#]),
        ("SELECT min(j::text), max((j->>'a')::int) FROM s1", vec![r#"[1]|2"#]),
    ] {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
}
