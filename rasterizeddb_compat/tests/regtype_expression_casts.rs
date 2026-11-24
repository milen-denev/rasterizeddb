//! `regtype` over an expression, a call or an earlier cast (`(a + 1)::regtype`,
//! `abs(a)::regtype`, `a::int::regtype`, a scalar subquery's), as PostgreSQL
//! 17 answers it: each was "cast to 'regtype' is not supported yet", only a
//! literal and a column were read. Shown it is the type's name; sorted or
//! compared it is the oid; the column is named as PostgreSQL names a cast
//! column.
//!
//! Every expectation is PostgreSQL 17's answer to the same statement, in the
//! same order.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

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

#[tokio::test(flavor = "multi_thread")]
async fn a_regtype_cast_of_an_expression_answers_as_postgres() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (r#"CREATE TABLE ty (id int PRIMARY KEY, t int, big bigint, "Ab" int)"#, vec![]),
        (r#"INSERT INTO ty VALUES (1, 25, 1700, 16), (2, 23, 16, 23), (3, 99999, NULL, NULL), (4, NULL, 1184, 25), (5, 16, 25, 1700), (6, 1700, 23, 20)"#, vec![]),
        (r#"SELECT id, (t - 2)::regtype FROM ty WHERE id IN (1, 2) ORDER BY id"#, vec![r#"1|integer"#, r#"2|smallint"#]),
        (r#"SELECT id, abs(t)::regtype FROM ty ORDER BY id"#, vec![r#"1|text"#, r#"2|integer"#, r#"3|99999"#, r#"4|NULL"#, r#"5|boolean"#, r#"6|numeric"#]),
        (r#"SELECT id, COALESCE(t, 20)::regtype AS n FROM ty ORDER BY id"#, vec![r#"1|text"#, r#"2|integer"#, r#"3|99999"#, r#"4|bigint"#, r#"5|boolean"#, r#"6|numeric"#]),
        (r#"SELECT id, t::int::regtype, big::int::regtype::text FROM ty ORDER BY id"#, vec![r#"1|text|numeric"#, r#"2|integer|boolean"#, r#"3|99999|NULL"#, r#"4|NULL|timestamp with time zone"#, r#"5|boolean|text"#, r#"6|numeric|integer"#]),
        (r#"SELECT id, CAST(t AS int)::regtype FROM ty WHERE id < 3 ORDER BY id"#, vec![r#"1|text"#, r#"2|integer"#]),
        (r#"SELECT id, "Ab"::regtype, ty."Ab"::regtype::text FROM ty ORDER BY id"#, vec![r#"1|boolean|boolean"#, r#"2|integer|integer"#, r#"3|NULL|NULL"#, r#"4|text|text"#, r#"5|numeric|numeric"#, r#"6|bigint|bigint"#]),
        (r#"SELECT id, (CASE WHEN id = 1 THEN 23 ELSE 25 END)::regtype FROM ty WHERE id < 3 ORDER BY id"#, vec![r#"1|integer"#, r#"2|text"#]),
        (r#"SELECT id FROM ty WHERE (t + 0)::regtype = 'integer'::regtype"#, vec![r#"2"#]),
        (r#"SELECT id FROM ty WHERE abs(t)::regtype IN ('text'::regtype, 'bool'::regtype) ORDER BY id"#, vec![r#"1"#, r#"5"#]),
        (r#"SELECT id FROM ty WHERE (t + 0)::regtype::text = 'numeric'"#, vec![r#"6"#]),
        (r#"SELECT id, (t + 0)::regtype AS n FROM ty ORDER BY n DESC NULLS LAST, id"#, vec![r#"3|99999"#, r#"6|numeric"#, r#"1|text"#, r#"2|integer"#, r#"5|boolean"#, r#"4|NULL"#]),
        (r#"SELECT id, abs(big)::regtype FROM ty ORDER BY 2, 1"#, vec![r#"2|boolean"#, r#"6|integer"#, r#"5|text"#, r#"4|timestamp with time zone"#, r#"1|numeric"#, r#"3|NULL"#]),
        (r#"SELECT id FROM ty ORDER BY COALESCE(t, 16)::regtype, id"#, vec![r#"4"#, r#"5"#, r#"2"#, r#"1"#, r#"6"#, r#"3"#]),
        (r#"SELECT id, row_number() OVER (ORDER BY (big + 0)::regtype) FROM ty ORDER BY id"#, vec![r#"1|5"#, r#"2|1"#, r#"3|6"#, r#"4|4"#, r#"5|3"#, r#"6|2"#]),
        (r#"SELECT (x + 1)::regtype FROM (VALUES (22), (24)) q(x) ORDER BY 1"#, vec![r#"integer"#, r#"text"#]),
        (r#"SELECT (SELECT max(t) FROM ty WHERE t < 100)::regtype"#, vec![r#"text"#]),
        (r#"SELECT (20 + 3)::regtype, (16)::regtype"#, vec![r#"integer|boolean"#]),
    ];
    for (sql, expected) in cases {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }

    // The column names PostgreSQL gives: the type for an expression, the
    // function for a call, the column through any cast.
    let named = sqlx::raw_sql(AssertSqlSafe(
        r#"SELECT (t - 2)::regtype, abs(t)::regtype, t::int::regtype, CAST(t AS int)::regtype, "Ab"::regtype, COALESCE(t, 20)::regtype FROM ty WHERE id = 1"#
            .to_string(),
    ))
    .fetch_all(&pool)
    .await
    .expect("names");
    let names: Vec<&str> = named[0].columns().iter().map(sqlx::Column::name).collect();
    assert_eq!(names, vec!["regtype", "abs", "t", "t", "Ab", "coalesce"]);
}
