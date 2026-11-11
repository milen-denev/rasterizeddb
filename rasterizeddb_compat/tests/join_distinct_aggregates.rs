//! `DISTINCT` inside an aggregate over a join, against PostgreSQL 17's
//! answers.
//!
//! `count(DISTINCT b.id)`, `array_agg(DISTINCT tag)` over a join (a LATERAL
//! one included) were refused: the join's aggregate parser read `DISTINCT`
//! as part of the argument, an expression nothing could read. Over a single
//! table they worked.

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
async fn distinct_aggregates_over_a_join_count_each_value_once() {
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
    let cases: &[(&str, &[&str])] = &[
        ("SELECT ja.id, count(DISTINCT jb.id) FROM ja JOIN jb ON ja.x = jb.x OR ja.y = jb.z GROUP BY ja.id ORDER BY 1", &["1|2", "2|1", "3|1"]),
        ("SELECT count(DISTINCT ja.id) FROM ja JOIN jb ON ja.x = jb.x OR ja.y = jb.z", &["3"]),
        ("SELECT ja.id, count(DISTINCT jb.x), sum(DISTINCT jb.x), count(jb.x) FROM ja JOIN jb ON true GROUP BY ja.id HAVING count(DISTINCT jb.x) > 2 ORDER BY 1", &["1|3|129|3", "2|3|129|3", "3|3|129|3", "4|3|129|3"]),
        ("SELECT ja.id, string_agg(DISTINCT jb.z, ',' ORDER BY jb.z) FROM ja JOIN jb ON true GROUP BY ja.id ORDER BY 1", &["1|a,c,q,zz", "2|a,c,q,zz", "3|a,c,q,zz", "4|a,c,q,zz"]),
    ];
    for (sql, want) in cases {
        assert_eq!(rows(&pool, sql).await, want.to_vec(), "{sql}");
    }
}

/// A group holding the same value twice: DISTINCT counts it once, in the
/// select list and in HAVING alike.
#[tokio::test(flavor = "multi_thread")]
async fn a_repeated_value_counts_once_in_the_list_and_in_having() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::raw_sql(
        "CREATE TABLE ka (id int, g text, v int); CREATE TABLE kb (id int, g text);
         INSERT INTO ka VALUES (1, 'x', 1), (2, 'x', 1), (3, 'x', 2), (4, 'y', 5);
         INSERT INTO kb VALUES (1, 'x'), (2, 'y');",
    )
    .execute(&pool)
    .await
    .expect("setup");
    assert_eq!(
        rows(
            &pool,
            "SELECT kb.g, count(DISTINCT ka.v), count(ka.v) FROM ka JOIN kb ON ka.g = kb.g GROUP BY kb.g \
             HAVING count(DISTINCT ka.v) = 2"
        )
        .await,
        vec!["x|2|3"]
    );
}
