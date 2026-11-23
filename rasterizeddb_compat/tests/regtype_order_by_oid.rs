//! A `regtype` value sorts and compares by the type's oid, as in PostgreSQL
//! 17: `boolean` (16), `integer` (23), `text` (25), `numeric` (1700), then an
//! oid no type has. A column cast to `regtype` is shown as the type's name,
//! and it was sorted as that text: `99999`, `boolean`, `integer`, `numeric`,
//! `text`, a wrong order with no error. `<` and `>=` between two were
//! "operator does not exist: text < integer".
//!
//! Where the key is a query's own (`ORDER BY x::regtype`, its position or
//! its alias), a window's or an aggregate's, the oid is what is sorted. A
//! grouped query or a set operation can only sort by what its result
//! carries, which is the name: that is refused (0A000) rather than answered
//! in another order.
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
async fn a_regtype_sorts_and_compares_by_its_oid() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (r#"CREATE TABLE ty (id int PRIMARY KEY, t int, big bigint, g text)"#, vec![]),
        (r#"INSERT INTO ty VALUES (1, 25, 1700, 'a'), (2, 23, 16, 'a'), (3, 99999, NULL, 'b'), (4, NULL, 1184, 'b'), (5, 16, 25, 'c'), (6, 1700, 23, 'c')"#, vec![]),
        (r#"SELECT t::regtype FROM ty ORDER BY 1"#, vec![r#"boolean"#, r#"integer"#, r#"text"#, r#"numeric"#, r#"99999"#, r#"NULL"#]),
        (r#"SELECT t::regtype FROM ty ORDER BY t::regtype"#, vec![r#"boolean"#, r#"integer"#, r#"text"#, r#"numeric"#, r#"99999"#, r#"NULL"#]),
        (r#"SELECT t::regtype AS n FROM ty ORDER BY n DESC"#, vec![r#"NULL"#, r#"99999"#, r#"numeric"#, r#"text"#, r#"integer"#, r#"boolean"#]),
        (r#"SELECT t::regtype AS n, id FROM ty ORDER BY 1 DESC NULLS LAST, 2"#, vec![r#"99999|3"#, r#"numeric|6"#, r#"text|1"#, r#"integer|2"#, r#"boolean|5"#, r#"NULL|4"#]),
        (r#"SELECT id, t::regtype FROM ty ORDER BY t::regtype DESC NULLS LAST, id"#, vec![r#"3|99999"#, r#"6|numeric"#, r#"1|text"#, r#"2|integer"#, r#"5|boolean"#, r#"4|NULL"#]),
        (r#"SELECT id, ty.t::regtype FROM ty ORDER BY 2, 1"#, vec![r#"5|boolean"#, r#"2|integer"#, r#"1|text"#, r#"6|numeric"#, r#"3|99999"#, r#"4|NULL"#]),
        (r#"SELECT id FROM ty ORDER BY big::regtype, id"#, vec![r#"2"#, r#"6"#, r#"5"#, r#"4"#, r#"1"#, r#"3"#]),
        (r#"SELECT id, big::regtype FROM ty ORDER BY big::regtype NULLS FIRST, id LIMIT 3"#, vec![r#"3|NULL"#, r#"2|boolean"#, r#"6|integer"#]),
        (r#"SELECT t::regtype::text FROM ty ORDER BY 1"#, vec![r#"99999"#, r#"boolean"#, r#"integer"#, r#"numeric"#, r#"text"#, r#"NULL"#]),
        (r#"SELECT t::regtype::text AS n FROM ty ORDER BY n DESC"#, vec![r#"NULL"#, r#"text"#, r#"numeric"#, r#"integer"#, r#"boolean"#, r#"99999"#]),
        (r#"SELECT id FROM ty ORDER BY t::regtype::text, id"#, vec![r#"3"#, r#"5"#, r#"2"#, r#"6"#, r#"1"#, r#"4"#]),
        (r#"SELECT id FROM ty WHERE t::regtype < 'text'::regtype ORDER BY id"#, vec![r#"2"#, r#"5"#]),
        (r#"SELECT id FROM ty WHERE t::regtype >= 'text'::regtype ORDER BY id"#, vec![r#"1"#, r#"3"#, r#"6"#]),
        (r#"SELECT id FROM ty WHERE 'integer'::regtype < t::regtype AND t::regtype <= 'numeric'::regtype ORDER BY id"#, vec![r#"1"#, r#"6"#]),
        (r#"SELECT id, row_number() OVER (ORDER BY t::regtype) FROM ty ORDER BY id"#, vec![r#"1|3"#, r#"2|2"#, r#"3|5"#, r#"4|6"#, r#"5|1"#, r#"6|4"#]),
        (r#"SELECT id, rank() OVER (PARTITION BY g ORDER BY t::regtype DESC) FROM ty ORDER BY id"#, vec![r#"1|1"#, r#"2|2"#, r#"3|2"#, r#"4|1"#, r#"5|2"#, r#"6|1"#]),
        (r#"SELECT g, array_agg(id ORDER BY t::regtype) FROM ty GROUP BY g ORDER BY g"#, vec![r#"a|{2,1}"#, r#"b|{3,4}"#, r#"c|{5,6}"#]),
        (r#"SELECT DISTINCT t::regtype FROM ty ORDER BY 1"#, vec![r#"boolean"#, r#"integer"#, r#"text"#, r#"numeric"#, r#"99999"#, r#"NULL"#]),
        (r#"SELECT x.t::regtype FROM ty x WHERE x.id IN (SELECT id FROM ty ORDER BY t::regtype LIMIT 3) ORDER BY x.t::regtype"#, vec![r#"boolean"#, r#"integer"#, r#"text"#]),
        (r#"SELECT g, count(*) FROM ty GROUP BY g ORDER BY g"#, vec![r#"a|2"#, r#"b|2"#, r#"c|2"#]),
        (r#"SELECT id, g AS rows FROM ty ORDER BY rows, t::regtype::text, id"#, vec![r#"2|a"#, r#"1|a"#, r#"3|b"#, r#"4|b"#, r#"5|c"#, r#"6|c"#]),
    ];
    for (sql, expected) in cases {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }

    // PostgreSQL sorts these by oid too. Here the key would have to be in
    // the result, which carries the name: refused, never the name's order.
    for sql in [
        "SELECT t::regtype, count(*) FROM ty GROUP BY t::regtype ORDER BY 1",
        "SELECT t::regtype, count(*) FROM ty GROUP BY t::regtype ORDER BY t::regtype",
        "SELECT t::regtype AS n, count(*) FROM ty GROUP BY 1 ORDER BY n DESC",
        "SELECT t::regtype FROM ty UNION SELECT big::regtype FROM ty ORDER BY 1",
    ] {
        let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_all(&pool).await.expect_err(sql);
        let db = e.as_database_error().unwrap_or_else(|| panic!("`{sql}`: {e}"));
        assert_eq!(db.code().as_deref(), Some("0A000"), "{sql}: {}", db.message());
        assert!(db.message().contains("regtype"), "{sql}: {}", db.message());
    }
    // Sorted by another column, a grouped regtype answers.
    assert_eq!(
        rows(&pool, "SELECT t::regtype, count(*) AS c, max(id) AS m FROM ty GROUP BY t::regtype ORDER BY m").await,
        vec!["text|1|1", "integer|1|2", "99999|1|3", "NULL|1|4", "boolean|1|5", "numeric|1|6"]
    );
}
