//! A VALUES tuple whose values hold commas of their own.
//!
//! The single-row INSERT parser split the tuple on every comma outside a
//! quote, so `f(1, 2)`, `ARRAY[1,2]` and `ROW(…)` were cut into pieces. When
//! the piece count differed from the column count that was an error; when it
//! happened to match, the pieces were stored in the wrong columns and the
//! statement reported success.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql)))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

async fn texts(pool: &sqlx::PgPool, sql: &str) -> Vec<(i32, Option<String>, Option<String>)> {
    let rows = pool
        .fetch_all(sqlx::raw_sql(AssertSqlSafe(sql)))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out: Vec<_> = rows
        .iter()
        .map(|r| {
            (
                r.try_get::<i32, _>(0).expect("id"),
                r.try_get::<Option<String>, _>(1).expect("a"),
                r.try_get::<Option<String>, _>(2).expect("b"),
            )
        })
        .collect();
    out.sort();
    out
}

/// The silent case: three pieces for three columns.
#[tokio::test]
async fn an_array_with_a_comma_is_one_value_not_two() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE nv (id INT PRIMARY KEY, a TEXT, b TEXT)")
        .await
        .expect("create");
    // `ARRAY[1,2]` is ONE value, so three columns and two values must be
    // refused. Split at its comma it became a = `ARRAY[1`, b = `2]`, id = 3,
    // and the statement reported success.
    let outcome = run(&pool, "INSERT INTO nv (a, b, id) VALUES (ARRAY[1,2], 3)").await;
    let stored = texts(&pool, "SELECT id, a, b FROM nv").await;
    assert!(outcome.is_err(), "three columns, two values must be refused; stored {stored:?}");
    assert!(stored.is_empty(), "a refused statement stored {stored:?}");

    let outcome = run(&pool, "INSERT INTO nv (a, b, id) VALUES (ARRAY['x','y'], 'w', 4)").await;
    let stored = texts(&pool, "SELECT id, a, b FROM nv").await;
    match outcome {
        Ok(()) => {
            assert_eq!(stored.len(), 1, "{stored:?}");
            assert_eq!(stored[0].0, 4);
            assert_eq!(stored[0].2.as_deref(), Some("w"), "b is the second value: {stored:?}");
            assert_ne!(stored[0].1.as_deref(), Some("ARRAY['x'"), "a piece of the array: {stored:?}");
        }
        // Not supporting the value is acceptable; storing pieces of it is not.
        Err(_) => assert!(stored.is_empty(), "a refused statement stored {stored:?}"),
    }
}

#[tokio::test]
async fn brackets_casts_and_quotes_keep_their_commas() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE nv2 (id INT PRIMARY KEY, a TEXT, b TEXT)")
        .await
        .expect("create");
    // A cast on a quoted value, and a quoted comma.
    run(&pool, "INSERT INTO nv2 (id, a, b) VALUES (1, '1,2'::text, 'p, q')")
        .await
        .expect("insert");
    // E-string with an escaped quote and a comma inside.
    run(&pool, r"INSERT INTO nv2 (id, a, b) VALUES (2, E'it\'s, ok', 'z')")
        .await
        .expect("insert E-string");
    // Dollar-quoted value with a comma and a quote inside.
    run(&pool, "INSERT INTO nv2 (id, a, b) VALUES (3, $$a, 'b$$, 'c')")
        .await
        .expect("insert dollar-quoted");
    assert_eq!(
        texts(&pool, "SELECT id, a, b FROM nv2").await,
        vec![
            (1, Some("1,2".to_string()), Some("p, q".to_string())),
            (2, Some("it's, ok".to_string()), Some("z".to_string())),
            (3, Some("a, 'b".to_string()), Some("c".to_string())),
        ]
    );

}
