//! `TEXT[]` columns over the wire.
//!
//! A production page crashed the client task with `bytes`' "advance out of
//! bounds: the len is 0 but advancing by 4" on an `INSERT … RETURNING` whose
//! list contains a `TEXT[]`. An array is decoded by reading five `i32`
//! headers — ndim, flags, element oid, then per-dimension length and lower
//! bound — before any element, so an array field arriving with no payload
//! panics the decoder at exactly that message. Every other type decodes an
//! empty payload as an empty value instead, which is why no other shape ever
//! reproduced it.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    pool.execute(sqlx::raw_sql(sql))
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

#[tokio::test]
async fn an_array_column_survives_insert_returning_and_select() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE step_template (id INT PRIMARY KEY, name TEXT NOT NULL, previous_step_dependencies TEXT[], branch_options JSONB)")
        .await
        .expect("create");

    // A populated array, an EMPTY one, and NULL — the three the form
    // produces. Each is inserted and returned in one statement, which is what
    // sea-orm's insert does.
    let row = sqlx::query(
        "INSERT INTO step_template (id, name, previous_step_dependencies, branch_options) \
         VALUES ($1, $2, '{alpha,beta}', '{\"k\":1}') \
         RETURNING id, name, previous_step_dependencies, branch_options",
    )
    .bind(1i32)
    .bind("populated")
    .fetch_one(&pool)
    .await
    .expect("INSERT … RETURNING with a populated array");
    assert_eq!(row.try_get::<i32, _>("id").expect("id"), 1);
    // Decoding is where the client crashed — a column is only decoded when
    // it is read, so a test that fetches the row without reading the array
    // proves nothing.
    assert_eq!(
        row.try_get::<Vec<String>, _>("previous_step_dependencies").expect("array decodes"),
        vec!["alpha".to_string(), "beta".to_string()]
    );

    let row = sqlx::query(
        "INSERT INTO step_template (id, name, previous_step_dependencies, branch_options) \
         VALUES ($1, $2, '{}', '{}') \
         RETURNING id, name, previous_step_dependencies, branch_options",
    )
    .bind(2i32)
    .bind("empty")
    .fetch_one(&pool)
    .await
    .expect("INSERT … RETURNING with an EMPTY array");
    assert_eq!(row.try_get::<i32, _>("id").expect("id"), 2);
    assert_eq!(
        row.try_get::<Vec<String>, _>("previous_step_dependencies").expect("empty array decodes"),
        Vec::<String>::new()
    );

    let row = sqlx::query(
        "INSERT INTO step_template (id, name, previous_step_dependencies, branch_options) \
         VALUES ($1, $2, NULL, NULL) \
         RETURNING id, name, previous_step_dependencies, branch_options",
    )
    .bind(3i32)
    .bind("null")
    .fetch_one(&pool)
    .await
    .expect("INSERT … RETURNING with a NULL array");
    assert_eq!(row.try_get::<i32, _>("id").expect("id"), 3);
    assert_eq!(
        row.try_get::<Option<Vec<String>>, _>("previous_step_dependencies")
            .expect("NULL array decodes"),
        None
    );

    // And read back — separating RETURNING-specific from array decode in
    // general. `field.content_string_vec` is read this way on every case.
    let rows = sqlx::query("SELECT id, previous_step_dependencies FROM step_template ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("selecting an array column");
    assert_eq!(rows.len(), 3);
    let read_back: Vec<Option<Vec<String>>> = rows
        .iter()
        .map(|r| {
            r.try_get::<Option<Vec<String>>, _>("previous_step_dependencies")
                .expect("array decodes on the read path too")
        })
        .collect();
    assert_eq!(
        read_back,
        vec![
            Some(vec!["alpha".to_string(), "beta".to_string()]),
            Some(Vec::<String>::new()),
            None,
        ]
    );
}

/// The panic's root cause, which is a silent wrong answer on every nullable
/// column: a `RETURNING` row was built from the write payload and typed as
/// the column's declared type, so a stored NULL reached the client as `0` or
/// `""` — while `SELECT` on the same row correctly reported NULL. An array
/// made it loud (a zero-length payload panics a decoder that reads five
/// `i32` headers), which is the only reason it was ever noticed.
#[tokio::test]
async fn returning_reports_null_the_same_way_select_does() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE t (id INT PRIMARY KEY, note TEXT, n BIGINT, at TIMESTAMPTZ, arr TEXT[])")
        .await
        .expect("create");

    // INSERT … RETURNING
    let row = sqlx::query(
        "INSERT INTO t (id, note, n, at, arr) VALUES ($1, NULL, NULL, NULL, NULL) \
         RETURNING id, note, n, at, arr",
    )
    .bind(1i32)
    .fetch_one(&pool)
    .await
    .expect("insert returning");
    assert_eq!(row.try_get::<Option<String>, _>("note").expect("note"), None);
    assert_eq!(row.try_get::<Option<i64>, _>("n").expect("n"), None);
    assert_eq!(
        row.try_get::<Option<Vec<String>>, _>("arr").expect("arr"),
        None,
        "a NULL array must be NULL, not a zero-length array payload"
    );

    // The stored row agrees — asserted separately, since the two disagreed.
    let back = sqlx::query("SELECT note, n, arr FROM t WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("read back");
    assert_eq!(back.try_get::<Option<String>, _>("note").expect("note"), None);
    assert_eq!(back.try_get::<Option<i64>, _>("n").expect("n"), None);
    assert_eq!(back.try_get::<Option<Vec<String>>, _>("arr").expect("arr"), None);

    // UPDATE … RETURNING and DELETE … RETURNING take a different path from
    // INSERT, and a DELETE has no assignments at all — the marking must not
    // depend on there being any.
    let row = sqlx::query("UPDATE t SET id = id WHERE id = $1 RETURNING note, n, arr")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("update returning");
    assert_eq!(row.try_get::<Option<String>, _>("note").expect("note"), None);
    assert_eq!(row.try_get::<Option<i64>, _>("n").expect("n"), None);
    assert_eq!(row.try_get::<Option<Vec<String>>, _>("arr").expect("arr"), None);

    let row = sqlx::query("DELETE FROM t WHERE id = $1 RETURNING note, n, arr")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("delete returning");
    assert_eq!(row.try_get::<Option<String>, _>("note").expect("note"), None);
    assert_eq!(row.try_get::<Option<i64>, _>("n").expect("n"), None);
    assert_eq!(row.try_get::<Option<Vec<String>>, _>("arr").expect("arr"), None);

    // A real value still reports as itself — the marking must not swallow
    // legitimate zeroes and empty strings.
    run(&pool, "INSERT INTO t (id, note, n, arr) VALUES (2, '', 0, '{}')")
        .await
        .expect("insert real values");
    let row = sqlx::query("SELECT note, n, arr FROM t WHERE id = $1")
        .bind(2i32)
        .fetch_one(&pool)
        .await
        .expect("read back");
    assert_eq!(row.try_get::<Option<String>, _>("note").expect("note"), Some(String::new()));
    assert_eq!(row.try_get::<Option<i64>, _>("n").expect("n"), Some(0));
    assert_eq!(
        row.try_get::<Option<Vec<String>>, _>("arr").expect("arr"),
        Some(Vec::<String>::new()),
        "an empty array is a value, not a NULL"
    );
}
