//! Gaps found while closing the compatibility document's list, each as
//! PostgreSQL 17 answers it:
//!
//! - a derived table, a CTE, a view or a `CREATE TABLE AS` whose set
//!   operation opens with a branch that has no `FROM` (`SELECT 7 AS id UNION
//!   ALL SELECT id FROM t`): its select list was read up to the next branch's
//!   `FROM`, "syntax error at or near \"ALL\"";
//! - a parenthesised set operation used as a value, `x IN ((SELECT 1) UNION
//!   (SELECT 2))`: "column \"SELECT\" does not exist";
//! - a NULL with a type written on it beside a NUMERIC column, `EXCEPT
//!   SELECT 3, NULL::numeric`: refused as a type this cannot tell; and a
//!   branch ending in a call, which lost the call's closing parenthesis;
//! - `column::regtype`: "cast to 'regtype' is not supported yet" outside a
//!   catalogue statement;
//! - the names of unnamed CHECK constraints in a `CREATE TABLE` (`t_check`,
//!   then `t_check1`; `t_<column>_check` for one that reads one column),
//!   and a second `CHECK` written on one column, which was dropped without
//!   a word and never enforced.
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

async fn error(pool: &sqlx::PgPool, sql: &str) -> (String, String) {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_all(pool).await.expect_err(sql);
    let db = e.as_database_error().unwrap_or_else(|| panic!("`{sql}`: no database error: {e}"));
    (db.code().map(|c| c.to_string()).unwrap_or_default(), db.message().to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn constant_branches_nested_set_operations_and_regtype_columns_answer_as_postgres() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (r#"CREATE TABLE ca (id int PRIMARY KEY, g text, n int)"#, vec![]),
        (r#"INSERT INTO ca VALUES (1, 'a', 0), (2, 'a', 0), (3, 'b', 0), (4, 'n', 0), (5, NULL, 0)"#, vec![]),
        (r#"CREATE TABLE src (id int PRIMARY KEY, g text, v numeric(10,2), w numeric)"#, vec![]),
        (r#"INSERT INTO src VALUES (1, 'a', 1.50, 1.123456789012), (2, 'a', 2.25, NULL), (3, 'b', NULL, 7)"#, vec![]),
        (r#"CREATE TABLE ty (id int PRIMARY KEY, t int, big bigint)"#, vec![]),
        (r#"INSERT INTO ty VALUES (1, 23, 1700), (2, 25, 16), (3, 99999, NULL), (4, NULL, 1184)"#, vec![]),
        (r#"SELECT u.id FROM (SELECT 7 AS id UNION ALL SELECT id FROM ca) u ORDER BY 1"#, vec![r#"1"#, r#"2"#, r#"3"#, r#"4"#, r#"5"#, r#"7"#]),
        (r#"SELECT u.id FROM (SELECT 7 AS id UNION ALL SELECT s.id FROM ca s JOIN ca o ON o.id = s.id) u ORDER BY 1"#, vec![r#"1"#, r#"2"#, r#"3"#, r#"4"#, r#"5"#, r#"7"#]),
        (r#"SELECT u.id FROM (SELECT 7 AS id UNION ALL SELECT id FROM ca s WHERE EXISTS (SELECT 1 FROM ca o WHERE o.g = s.g AND o.id <> s.id)) u ORDER BY 1"#, vec![r#"1"#, r#"2"#, r#"7"#]),
        (r#"SELECT u.id FROM (SELECT 7 AS id UNION SELECT id FROM ca s WHERE EXISTS (SELECT 1 FROM ca o WHERE o.g = s.g)) u ORDER BY 1"#, vec![r#"1"#, r#"2"#, r#"3"#, r#"4"#, r#"7"#]),
        (r#"SELECT u.id FROM (SELECT 7 AS id UNION ALL SELECT id FROM ca s WHERE id = (SELECT max(o.id) FROM ca o WHERE o.g = s.g)) u ORDER BY 1"#, vec![r#"2"#, r#"3"#, r#"4"#, r#"7"#]),
        (r#"SELECT u.id FROM (SELECT 7 AS id UNION ALL (SELECT id FROM ca s WHERE EXISTS (SELECT 1 FROM ca o WHERE o.g = s.g AND o.id <> s.id))) u ORDER BY 1"#, vec![r#"1"#, r#"2"#, r#"7"#]),
        (r#"WITH u AS (SELECT 7 AS id UNION ALL SELECT id FROM ca s WHERE EXISTS (SELECT 1 FROM ca o WHERE o.g = s.g AND o.id <> s.id)) SELECT id FROM u ORDER BY 1"#, vec![r#"1"#, r#"2"#, r#"7"#]),
        (r#"WITH u AS (SELECT 7 AS id, 'k' AS t UNION ALL SELECT id, g FROM ca) SELECT id, t FROM u ORDER BY 1"#, vec![r#"1|a"#, r#"2|a"#, r#"3|b"#, r#"4|n"#, r#"5|NULL"#, r#"7|k"#]),
        (r#"SELECT u.a, u.b FROM (SELECT 1.5 AS a, 'x' AS b EXCEPT SELECT n, g FROM ca) u"#, vec![r#"1.5|x"#]),
        (r#"SELECT u.a FROM (SELECT 2 AS a INTERSECT SELECT id FROM ca) u"#, vec![r#"2"#]),
        (r#"SELECT u.x FROM (SELECT 'from' AS x UNION ALL SELECT g FROM ca WHERE g IS NOT NULL) u ORDER BY 1"#, vec![r#"a"#, r#"a"#, r#"b"#, r#"from"#, r#"n"#]),
        (r#"SELECT count(*) FROM (SELECT 1 AS a UNION ALL SELECT 2) u"#, vec![r#"2"#]),
        (r#"CREATE TABLE cu5 AS SELECT 7 AS id UNION ALL SELECT id FROM ca"#, vec![]),
        (r#"SELECT * FROM cu5 ORDER BY 1"#, vec![r#"1"#, r#"2"#, r#"3"#, r#"4"#, r#"5"#, r#"7"#]),
        (r#"CREATE VIEW vu AS SELECT 7 AS id UNION ALL SELECT id FROM ca"#, vec![]),
        (r#"SELECT id FROM vu ORDER BY 1"#, vec![r#"1"#, r#"2"#, r#"3"#, r#"4"#, r#"5"#, r#"7"#]),
        (r#"SELECT id FROM ca WHERE id IN ((SELECT 1) UNION (SELECT 2)) ORDER BY 1"#, vec![r#"1"#, r#"2"#]),
        (r#"SELECT (SELECT max(id) FROM ca) AS m, ((SELECT 1) UNION (SELECT 1)) AS one"#, vec![r#"5|1"#]),
        (r#"SELECT id FROM ca WHERE id IN (SELECT 1 UNION (SELECT id FROM ca WHERE g = 'b')) ORDER BY 1"#, vec![r#"1"#, r#"3"#]),
        (r#"SELECT id FROM ca WHERE id NOT IN ((SELECT 1) UNION ALL (SELECT 2)) ORDER BY 1"#, vec![r#"3"#, r#"4"#, r#"5"#]),
        (r#"SELECT id FROM ca WHERE id IN ((SELECT id FROM ca WHERE g = 'a') EXCEPT ((SELECT 2) UNION (SELECT 9))) ORDER BY 1"#, vec![r#"1"#]),
        (r#"SELECT id FROM ca WHERE id = ANY ((SELECT 3) UNION (SELECT 4)) ORDER BY 1"#, vec![r#"3"#, r#"4"#]),
        (r#"SELECT id FROM ca WHERE id > ALL ((SELECT 3) UNION (SELECT 4)) ORDER BY 1"#, vec![r#"5"#]),
        (r#"SELECT id FROM ca WHERE EXISTS ((SELECT 1 FROM ca WHERE id = 99) UNION ALL (SELECT 1 FROM ca WHERE id = 1)) ORDER BY 1"#, vec![r#"1"#, r#"2"#, r#"3"#, r#"4"#, r#"5"#]),
        (r#"SELECT id FROM ca WHERE NOT EXISTS ((SELECT 1 FROM ca WHERE id = 1) EXCEPT (SELECT 1 FROM ca WHERE id = 2)) ORDER BY 1"#, vec![r#"1"#, r#"2"#, r#"3"#, r#"4"#, r#"5"#]),
        (r#"SELECT id FROM ca WHERE (id = 1) OR ((g = 'b') AND (n = 0)) UNION ALL SELECT 99 ORDER BY 1"#, vec![r#"1"#, r#"3"#, r#"99"#]),
        (r#"SELECT ((SELECT 1) + (SELECT 2)) AS s UNION ALL SELECT 3 ORDER BY 1"#, vec![r#"3"#, r#"3"#]),
        (r#"UPDATE ca SET n = 1 WHERE id IN ((SELECT 1) UNION (SELECT 3))"#, vec![]),
        (r#"DELETE FROM ca WHERE id IN ((SELECT 4) EXCEPT (SELECT 5))"#, vec![]),
        (r#"SELECT id, n FROM ca ORDER BY 1"#, vec![r#"1|1"#, r#"2|0"#, r#"3|1"#, r#"5|0"#]),
        (r#"SELECT id, v FROM src EXCEPT SELECT 3, NULL::numeric ORDER BY 1"#, vec![r#"1|1.50"#, r#"2|2.25"#]),
        (r#"SELECT id, v FROM src UNION ALL SELECT 9, NULL::numeric(10,2) ORDER BY 1"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|NULL"#]),
        (r#"SELECT id, w FROM src UNION SELECT 3, CAST(NULL AS numeric) ORDER BY 1, 2"#, vec![r#"1|1.123456789012"#, r#"2|NULL"#, r#"3|7"#, r#"3|NULL"#]),
        (r#"SELECT id, v FROM src UNION ALL (SELECT 3, CAST(NULL AS numeric)) ORDER BY 1, 2"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#, r#"3|NULL"#]),
        (r#"SELECT 9 AS id, NULL::numeric AS v UNION ALL SELECT id, v FROM src ORDER BY 1"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|NULL"#]),
        (r#"SELECT 9 AS id, CAST(NULL AS numeric) AS v UNION ALL SELECT id, v FROM src ORDER BY 1"#, vec![r#"1|1.50"#, r#"2|2.25"#, r#"3|NULL"#, r#"9|NULL"#]),
        (r#"SELECT id, (SELECT max(o.v) FROM src o WHERE o.g = s.g) FROM src s EXCEPT SELECT 3, NULL::numeric ORDER BY 1"#, vec![r#"1|2.25"#, r#"2|2.25"#]),
        (r#"SELECT id, v FROM src INTERSECT SELECT 3, NULL::numeric"#, vec![r#"3|NULL"#]),
        (r#"SELECT id, v, w FROM src UNION ALL SELECT 8, NULL::numeric, 2.5 ORDER BY 1"#, vec![r#"1|1.50|1.123456789012"#, r#"2|2.25|NULL"#, r#"3|NULL|7"#, r#"8|NULL|2.5"#]),
        (r#"SELECT g, v FROM src UNION ALL SELECT NULL::text, NULL::numeric ORDER BY 1, 2"#, vec![r#"a|1.50"#, r#"a|2.25"#, r#"b|NULL"#, r#"NULL|NULL"#]),
        (r#"SELECT id, w FROM src UNION ALL SELECT 3, NULL ORDER BY 1, 2"#, vec![r#"1|1.123456789012"#, r#"2|NULL"#, r#"3|7"#, r#"3|NULL"#]),
        (r#"SELECT 9 AS id, upper('x') AS u UNION ALL SELECT id, g FROM src ORDER BY 1"#, vec![r#"1|a"#, r#"2|a"#, r#"3|b"#, r#"9|X"#]),
        (r#"SELECT id, g FROM src UNION ALL SELECT 9, concat('a', 'b') ORDER BY 1"#, vec![r#"1|a"#, r#"2|a"#, r#"3|b"#, r#"9|ab"#]),
        (r#"SELECT id, t::regtype, big::regtype FROM ty ORDER BY id"#, vec![r#"1|integer|numeric"#, r#"2|text|boolean"#, r#"3|99999|NULL"#, r#"4|NULL|timestamp with time zone"#]),
        (r#"SELECT t::regtype::text, big::regtype::oid FROM ty WHERE id = 1"#, vec![r#"integer|1700"#]),
        (r#"SELECT t::regtype FROM ty WHERE id = 2"#, vec![r#"text"#]),
        (r#"SELECT id FROM ty WHERE t::regtype = 'integer'::regtype"#, vec![r#"1"#]),
        (r#"SELECT id FROM ty WHERE 'text'::regtype = t::regtype OR big::regtype IN ('bool'::regtype, 'numeric'::regtype) ORDER BY 1"#, vec![r#"1"#, r#"2"#]),
        (r#"SELECT id FROM ty WHERE t::regtype <> 'integer'::regtype ORDER BY 1"#, vec![r#"2"#, r#"3"#]),
        (r#"SELECT id FROM ty WHERE t::regtype::text = 'text'"#, vec![r#"2"#]),
        (r#"SELECT x::regtype FROM (VALUES (23), (25)) q(x) ORDER BY 1"#, vec![r#"integer"#, r#"text"#]),
        (r#"SELECT a.attname, a.atttypid::regtype FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'ty' AND a.attnum > 0 ORDER BY a.attnum"#, vec![r#"id|integer"#, r#"t|integer"#, r#"big|bigint"#]),
        (r#"SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.atttypid::regtype::text FROM pg_attribute a WHERE a.attrelid = 'src'::regclass AND a.attnum > 0 ORDER BY a.attnum"#, vec![r#"id|integer|integer"#, r#"g|text|text"#, r#"v|numeric(10,2)|numeric"#, r#"w|numeric|numeric"#]),
    ];
    for (sql, expected) in cases {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
    // A column cast to `regtype` keeps the column's name.
    let described = sqlx::raw_sql(AssertSqlSafe("SELECT t::regtype, ty.big::regtype FROM ty WHERE id = 1".to_string()))
        .fetch_all(&pool)
        .await
        .expect("regtype columns");
    let names: Vec<&str> = described[0].columns().iter().map(sqlx::Column::name).collect();
    assert_eq!(names, vec!["t", "big"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn unnamed_checks_take_postgres_names_and_every_one_is_enforced() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (r#"CREATE TABLE ck (a int, b int, CHECK (a > b))"#, vec![]),
        (r#"CREATE TABLE ck2 (a int CHECK (a > 0), b int, CHECK (a > b), CHECK (b > a - 100), CHECK (b < 1000))"#, vec![]),
        (r#"CREATE TABLE ck3 (a int, b int CHECK (b > 0) CHECK (b < 9), CONSTRAINT named CHECK (a < b), CHECK (a + b > 0))"#, vec![]),
        (r#"CREATE TABLE ck4 (a int, CHECK (a > 0), b int, a2 int CHECK (a2 > 0), CHECK (a2 < 5), CHECK (a < 9))"#, vec![]),
        (r#"CREATE TABLE ck5 (a int CONSTRAINT ck5_check CHECK (a > 0), b int, CHECK (a > b), CHECK (b > a - 9), CONSTRAINT ck5_b_check CHECK (a = a), CHECK (b > 0))"#, vec![]),
        (r#"CREATE TABLE ck6 (y int CHECK (y > 0), x int, CHECK (x > 0 AND x < 5), CHECK (1 = 1), CHECK (length('x') = 1))"#, vec![]),
        (r#"CREATE TABLE ck7 (b int CHECK (b > 0) CONSTRAINT upper_b CHECK (b < 9) NOT NULL CHECK (b <> 5))"#, vec![]),
        (r#"CREATE TABLE ck8 (a numeric(10,2) DEFAULT 1.5 CONSTRAINT pos CHECK (a > 0) CHECK (a < 100), s text CHECK (s <> ')') CHECK (length(s) < 4) UNIQUE)"#, vec![]),
        (r#"SELECT conrelid::regclass, string_agg(conname, ' ' ORDER BY conname) FROM pg_constraint WHERE contype = 'c' AND conrelid IN ('ck'::regclass, 'ck2'::regclass, 'ck3'::regclass, 'ck4'::regclass, 'ck5'::regclass, 'ck6'::regclass, 'ck7'::regclass, 'ck8'::regclass) GROUP BY conrelid ORDER BY 1"#, vec![r#"ck|ck_check"#, r#"ck2|ck2_a_check ck2_b_check ck2_check ck2_check1"#, r#"ck3|ck3_b_check ck3_b_check1 ck3_check named"#, r#"ck4|ck4_a2_check ck4_a2_check1 ck4_a_check ck4_a_check1"#, r#"ck5|ck5_b_check ck5_b_check1 ck5_check ck5_check1 ck5_check2"#, r#"ck6|ck6_check ck6_check1 ck6_x_check ck6_y_check"#, r#"ck7|ck7_b_check ck7_b_check1 upper_b"#, r#"ck8|ck8_a_check ck8_s_check ck8_s_check1 pos"#]),
        (r#"SELECT conname, pg_get_constraintdef(oid) LIKE '%b < 9%' FROM pg_constraint WHERE conrelid = 'ck3'::regclass ORDER BY 1"#, vec![r#"ck3_b_check|f"#, r#"ck3_b_check1|t"#, r#"ck3_check|f"#, r#"named|f"#]),
        (r#"INSERT INTO ck7 VALUES (4)"#, vec![]),
        (r#"INSERT INTO ck8 (s) VALUES ('ok')"#, vec![]),
        (r#"SELECT a, s FROM ck8"#, vec![r#"1.50|ok"#]),
        (r#"ALTER TABLE ck2 DROP CONSTRAINT ck2_b_check"#, vec![]),
        (r#"ALTER TABLE ck DROP CONSTRAINT ck_check"#, vec![]),
        (r#"INSERT INTO ck VALUES (1, 2)"#, vec![]),
        (r#"ALTER TABLE ck7 DROP CONSTRAINT ck7_b_check1"#, vec![]),
        (r#"INSERT INTO ck7 VALUES (5)"#, vec![]),
        (r#"ALTER TABLE ck ADD CHECK (a < 100)"#, vec![]),
        (r#"ALTER TABLE ck ADD CHECK (a + b < 100)"#, vec![]),
        (r#"SELECT string_agg(conname, ' ' ORDER BY conname) FROM pg_constraint WHERE conrelid = 'ck'::regclass"#, vec![r#"ck_a_check ck_check"#]),
    ];
    // The violations are asserted between the statements below, so the
    // list is run up to the first row written, then by hand.
    let violation = |table: &str, name: &str| {
        ("23514".to_string(), format!("new row for relation \"{table}\" violates check constraint \"{name}\""))
    };
    let mut cases = cases.into_iter();
    for (sql, expected) in cases.by_ref().take(10) {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
    // Each check of each table refuses its own row, under PostgreSQL's name.
    // Each row breaks one check: of several broken at once PostgreSQL names
    // the first by name, and this the first written.
    for (sql, table, name) in [
        ("INSERT INTO ck VALUES (1, 2)", "ck", "ck_check"),
        ("INSERT INTO ck2 VALUES (500, 300)", "ck2", "ck2_check1"),
        ("INSERT INTO ck2 VALUES (-1, -5)", "ck2", "ck2_a_check"),
        ("INSERT INTO ck2 VALUES (5, -200)", "ck2", "ck2_check1"),
        ("INSERT INTO ck2 VALUES (5000, 4950)", "ck2", "ck2_b_check"),
        ("INSERT INTO ck2 VALUES (5, 7)", "ck2", "ck2_check"),
        ("INSERT INTO ck3 VALUES (1, 100)", "ck3", "ck3_b_check1"),
        ("INSERT INTO ck3 VALUES (-5, 0)", "ck3", "ck3_b_check"),
        ("INSERT INTO ck4 VALUES (9, 0, 1)", "ck4", "ck4_a_check1"),
        ("INSERT INTO ck4 VALUES (1, 0, 7)", "ck4", "ck4_a2_check1"),
        ("INSERT INTO ck5 VALUES (5, 0)", "ck5", "ck5_b_check1"),
        ("INSERT INTO ck6 VALUES (1, 7)", "ck6", "ck6_x_check"),
        ("INSERT INTO ck7 VALUES (100)", "ck7", "upper_b"),
        ("INSERT INTO ck7 VALUES (5)", "ck7", "ck7_b_check1"),
        ("INSERT INTO ck7 VALUES (0)", "ck7", "ck7_b_check"),
        ("INSERT INTO ck8 VALUES (500, 'ab')", "ck8", "ck8_a_check"),
        ("INSERT INTO ck8 VALUES (5, 'abcd')", "ck8", "ck8_s_check1"),
        ("INSERT INTO ck8 VALUES (5, ')')", "ck8", "ck8_s_check"),
    ] {
        assert_eq!(error(&pool, sql).await, violation(table, name), "{sql}");
    }
    assert_eq!(error(&pool, "INSERT INTO ck7 VALUES (NULL)").await.0, "23502");
    for (sql, expected) in cases {
        assert_eq!(rows(&pool, sql).await, expected, "{sql}");
    }
    assert_eq!(error(&pool, "INSERT INTO ck8 (s) VALUES ('ok')").await.0, "23505");
}
