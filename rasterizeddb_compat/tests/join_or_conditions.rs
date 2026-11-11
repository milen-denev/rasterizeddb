//! `OR` and other non-key conditions in a join, against PostgreSQL 17's
//! answers.
//!
//! An `ON` holding `OR` anywhere (even inside a string) was refused, as was
//! any outer join whose condition was not a column equality: `LEFT JOIN b ON
//! a.x = b.x OR a.y = b.z`, `LEFT JOIN b ON a.id = b.id AND a.n < b.m`, `LEFT
//! JOIN b ON a.x < b.x`. Such a condition is now decided on each candidate
//! pair as the join makes it, so the rows an outer join preserves still come
//! out NULL-padded. And an inner join's keyless condition was applied after
//! every join, so a later `RIGHT JOIN` lost the rows it had padded; it is
//! decided at its own step when a later join can pad.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::raw_sql(
        "CREATE TABLE ja (id int PRIMARY KEY, x int, y text);
         CREATE TABLE jb (id int PRIMARY KEY, x int, z text);
         INSERT INTO ja VALUES (1, 10, 'a'), (2, 20, 'b'), (3, NULL, 'c'), (4, 40, 'd');
         INSERT INTO jb VALUES (1, 20, 'a'), (2, 10, 'q'), (5, NULL, 'c'), (6, 99, 'zz');",
    )
    .execute(&pool)
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

#[tokio::test(flavor = "multi_thread")]
async fn a_join_condition_that_is_no_key_answers_as_postgresql_does() {
    let (_s, pool) = server().await;
    let cases: &[(&str, &[&str])] = &[
        ("SELECT ja.id, jb.id FROM ja JOIN jb ON ja.x = jb.x OR ja.y = jb.z ORDER BY 1, 2", &["1|1", "1|2", "2|1", "3|5"]),
        ("SELECT ja.id, jb.id FROM ja LEFT JOIN jb ON ja.x = jb.x OR ja.y = jb.z ORDER BY 1, 2", &["1|1", "1|2", "2|1", "3|5", "4|NULL"]),
        ("SELECT ja.id, jb.id FROM ja RIGHT JOIN jb ON ja.x = jb.x OR ja.y = jb.z ORDER BY 1, 2", &["1|1", "1|2", "2|1", "3|5", "NULL|6"]),
        ("SELECT ja.id, jb.id FROM ja JOIN jb ON ja.id = jb.id OR (ja.x = jb.x AND ja.y <> jb.z) ORDER BY 1, 2", &["1|1", "1|2", "2|1", "2|2"]),
        ("SELECT ja.id, jb.id FROM ja JOIN jb ON (ja.x = jb.x OR ja.y = jb.z) AND ja.id < 3 ORDER BY 1, 2", &["1|1", "1|2", "2|1"]),
        ("SELECT ja.id, jb.id FROM ja JOIN jb ON ja.x < jb.x OR ja.id = jb.id ORDER BY 1, 2", &["1|1", "1|6", "2|2", "2|6", "4|6"]),
        ("SELECT ja.id, jb.id FROM ja, jb WHERE ja.x = jb.x OR ja.y = jb.z ORDER BY 1, 2", &["1|1", "1|2", "2|1", "3|5"]),
        ("SELECT count(*) FROM ja JOIN jb ON ja.x = jb.x OR ja.y = jb.z", &["4"]),
        ("SELECT ja.id, jb.id FROM ja JOIN jb ON ja.x = jb.x OR jb.id = 6 ORDER BY 1, 2", &["1|2", "1|6", "2|1", "2|6", "3|6", "4|6"]),
        ("SELECT ja.id, jb.id FROM ja LEFT JOIN jb ON ja.id = jb.id AND (ja.x = jb.x OR ja.y = jb.z) ORDER BY 1, 2", &["1|1", "2|NULL", "3|NULL", "4|NULL"]),
        ("SELECT ja.id, jb.id FROM ja LEFT JOIN jb ON ja.id = jb.id AND ja.x < jb.x + 100 ORDER BY 1, 2", &["1|1", "2|2", "3|NULL", "4|NULL"]),
        ("SELECT ja.id, jb.id FROM ja RIGHT JOIN jb ON ja.id = jb.id AND (ja.x = jb.x OR ja.y = jb.z) ORDER BY 1, 2", &["1|1", "NULL|2", "NULL|5", "NULL|6"]),
        ("SELECT ja.id, jb.id FROM ja LEFT JOIN jb ON ja.x < jb.x ORDER BY 1, 2", &["1|1", "1|6", "2|6", "3|NULL", "4|6"]),
        ("SELECT ja.id, jb.id FROM ja LEFT JOIN jb ON ja.x = jb.x OR ja.y = jb.z WHERE jb.id IS NULL ORDER BY 1, 2", &["4|NULL"]),
        ("SELECT ja.id, jb.id, jb.z FROM ja LEFT JOIN jb ON ja.x = jb.x OR ja.y = jb.z WHERE ja.id < 4 ORDER BY 1, 2", &["1|1|a", "1|2|q", "2|1|a", "3|5|c"]),
        ("SELECT ja.id, count(jb.id) FROM ja LEFT JOIN jb ON ja.x = jb.x OR ja.y = jb.z GROUP BY ja.id ORDER BY 1", &["1|2", "2|1", "3|1", "4|0"]),
        ("SELECT ja.id, jb.id, jc.id FROM ja LEFT JOIN jb ON ja.x = jb.x OR ja.y = jb.z LEFT JOIN jb jc ON jc.id = jb.id + 1 OR jc.z = ja.y ORDER BY 1, 2, 3", &["1|1|1", "1|1|2", "1|2|1", "2|1|2", "3|5|5", "3|5|6", "4|NULL|NULL"]),
        ("SELECT ja.id, jb.id FROM ja JOIN jb ON ja.x = jb.x OR ja.y = jb.z RIGHT JOIN jb jc ON jc.id = jb.id ORDER BY 1, 2", &["1|1", "1|2", "2|1", "3|5", "NULL|NULL"]),
        ("SELECT ja.id, jb.id FROM ja LEFT JOIN jb ON (ja.x = jb.x OR ja.y = jb.z) AND jb.id > 1 ORDER BY 1, 2", &["1|2", "2|NULL", "3|5", "4|NULL"]),
        ("SELECT ja.id, jb.id FROM ja LEFT JOIN jb ON (ja.x = jb.x OR ja.y = jb.z) AND ja.id > 1 ORDER BY 1, 2", &["1|NULL", "2|1", "3|5", "4|NULL"]),
        ("SELECT ja.id, jb.id, jc.id FROM ja JOIN jb ON ja.x < jb.x RIGHT JOIN jb jc ON jc.id = jb.id ORDER BY 3, 1, 2", &["1|1|1", "NULL|NULL|2", "NULL|NULL|5", "1|6|6", "2|6|6", "4|6|6"]),
        ("SELECT ja.id, jb.id, jc.id FROM ja JOIN jb ON ja.x = jb.x OR ja.y = jb.z FULL JOIN jb jc ON jc.id = jb.id ORDER BY 3, 1, 2", &["1|1|1", "2|1|1", "1|2|2", "3|5|5", "NULL|NULL|6"]),
        ("SELECT ja.id, jb.id, jc.id FROM ja CROSS JOIN jb RIGHT JOIN jb jc ON jc.id = jb.id AND jb.id < 2 ORDER BY 3, 1, 2", &["1|1|1", "2|1|1", "3|1|1", "4|1|1", "NULL|NULL|2", "NULL|NULL|5", "NULL|NULL|6"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&pool, sql).await, want.to_vec(), "{sql}");
    }
}

/// An `ON` term over this relation and an earlier one it is not keyed to:
/// decided on each pair too, not as a filter on this relation's scan.
#[tokio::test(flavor = "multi_thread")]
async fn a_term_spanning_this_and_an_earlier_relation_is_decided_on_each_pair() {
    let (_s, pool) = server().await;
    let cases: &[(&str, &[&str])] = &[
        (
            "SELECT ja.id, jb.id, jc.id FROM ja JOIN jb ON ja.id = jb.id LEFT JOIN jb jc ON jc.id = jb.id AND jc.x > ja.x \
             ORDER BY 1, 2, 3",
            &["1|1|1", "2|2|NULL"],
        ),
        (
            "SELECT ja.id, jb.id, jc.id FROM ja JOIN jb ON ja.id = jb.id LEFT JOIN jb jc ON jc.z = ja.y OR jc.x = jb.x + 10 \
             ORDER BY 1, 2, 3",
            &["1|1|1", "2|2|1"],
        ),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&pool, sql).await, want.to_vec(), "{sql}");
    }
}

/// PostgreSQL refuses a FULL JOIN on a condition it cannot merge or hash;
/// this answers it, as a full join means: every pair the condition accepts,
/// then each side's rows that found none.
#[tokio::test(flavor = "multi_thread")]
async fn a_full_join_on_an_or_condition_keeps_both_sides() {
    let (_s, pool) = server().await;
    assert_eq!(
        rows(&pool, "SELECT ja.id, jb.id FROM ja FULL JOIN jb ON ja.x = jb.x OR ja.y = jb.z ORDER BY 1, 2").await,
        vec!["1|1", "1|2", "2|1", "3|5", "4|NULL", "NULL|6"]
    );
}
