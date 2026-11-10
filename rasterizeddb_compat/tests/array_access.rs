//! Reading into arrays, against PostgreSQL 17's answers: subscripts
//! (`a[1]`, `a[2:3]`), and `x op ANY/ALL (array)` over an array the row holds.
//!
//! All of it was refused: a subscript in a select list as a missing column
//! "i[1]", in a WHERE as "unexpected character '['", and `5 = ANY(tags)` as
//! "unsupported keyword 'ANY'". Arrays could be stored and printed, not used.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::raw_sql(
        "CREATE TABLE su (id INT, i INT[], t TEXT[], b BOOLEAN[]);
         INSERT INTO su VALUES (1, '{10,20,30}', '{a,b,NULL}', '{t,f}'), (2, '{5}', '{z}', NULL), (3, '{1,NULL}', NULL, NULL);",
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
async fn a_subscript_reads_one_element_and_a_slice_a_range() {
    let (_s, pool) = server().await;
    assert_eq!(
        rows(&pool, "SELECT id, i[1], i[3], i[4], i[0], t[2], t[3], b[1] FROM su WHERE id < 3 ORDER BY id").await,
        ["1|10|30|NULL|NULL|b|NULL|t", "2|5|NULL|NULL|NULL|NULL|NULL|NULL"]
    );
    assert_eq!(rows(&pool, "SELECT i[1]+1, i[2] * 2, 1 + i[1], i[1] * 2 + i[2] FROM su WHERE id = 1").await, ["11|40|11|40"]);
    assert_eq!(
        rows(&pool, "SELECT i[2:3], i[:2], i[2:], i[5:9], t[1:1] FROM su WHERE id = 1").await,
        ["{20,30}|{10,20}|{20,30}|{}|{a}"]
    );
    assert_eq!(rows(&pool, "SELECT (ARRAY[4,5,6])[2], (ARRAY['x','y'])[1]").await, ["5|x"]);
    assert_eq!(rows(&pool, "SELECT i[NULL], i[1:NULL] FROM su WHERE id = 1").await, ["NULL|NULL"]);
    assert_eq!(rows(&pool, "SELECT max(i[1]) FROM su").await, ["10"]);
    assert_eq!(rows(&pool, "SELECT id FROM su ORDER BY i[1]").await, ["3", "2", "1"]);
    assert_eq!(rows(&pool, "SELECT pg_typeof(i[1]), pg_typeof(t[1]) FROM su WHERE id = 1").await, ["integer|text"]);
    // In a WHERE, and through a typed client in binary.
    assert_eq!(rows(&pool, "SELECT id FROM su WHERE i[1] = 5").await, ["2"]);
    assert_eq!(rows(&pool, "SELECT id FROM su WHERE i[1] BETWEEN 4 AND 6").await, ["2"]);
    assert_eq!(rows(&pool, "SELECT id FROM su WHERE i[id + 1] = 20").await, ["1"]);
    let typed: (i32, Vec<i32>) = sqlx::query_as("SELECT i[2], i[2:3] FROM su WHERE id = 1").fetch_one(&pool).await.expect("typed");
    assert_eq!(typed, (20, vec![20, 30]));
    // `ARRAY[…][i]` without parentheses is PostgreSQL's syntax error.
    assert!(sqlx::raw_sql("SELECT ARRAY[1,2][1]").execute(&pool).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn any_and_all_compare_with_every_element_of_a_stored_array() {
    let (_s, pool) = server().await;
    assert_eq!(rows(&pool, "SELECT id FROM su WHERE 5 = ANY(i)").await, ["2"]);
    assert_eq!(rows(&pool, "SELECT id FROM su WHERE 20 = ANY(i) ORDER BY id").await, ["1"]);
    assert_eq!(rows(&pool, "SELECT id FROM su WHERE 7 > ALL(i) ORDER BY id").await, ["2"]);
    assert_eq!(rows(&pool, "SELECT id FROM su WHERE 'b' = ANY(t) ORDER BY id").await, ["1"]);
    assert_eq!(rows(&pool, "SELECT id FROM su WHERE 5 = ANY(i) OR id = 1 ORDER BY id").await, ["1", "2"]);
    // A NULL element leaves the answer unknown when nothing else decides it,
    // so NOT keeps the row out, as in PostgreSQL.
    assert_eq!(rows(&pool, "SELECT id FROM su WHERE NOT (5 = ANY(i)) ORDER BY id").await, ["1"]);
    assert_eq!(rows(&pool, "SELECT id FROM su WHERE 5 <> ALL(i) ORDER BY id").await, ["1"]);
    assert_eq!(rows(&pool, "SELECT id FROM su WHERE NOT (3 = ANY(i)) ORDER BY id").await, ["1", "2"]);
    assert_eq!(
        rows(&pool, "SELECT id, 5 = ANY(i), 10 = ANY(i) FROM su ORDER BY id").await,
        ["1|f|t", "2|t|f", "3|NULL|NULL"]
    );
}
