//! `ORDER BY` an expression — the shape the application's list views emit:
//! `ORDER BY NULLIF(data->>'2', '')::date ASC NULLS FIRST, entry_order ASC,
//! id ASC LIMIT 5000 OFFSET 0`, sorting rows by a date stored in a JSON
//! field. The scan can only sort stored columns, so these keys are evaluated
//! per row after it, and the page is cut after the sort.

use rasterizeddb_compat::TestServer;
use sqlx::Row;

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    sqlx::query(sql)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())
}

async fn ids(pool: &sqlx::PgPool, sql: &str) -> Result<Vec<i32>, String> {
    Ok(sqlx::query(sql)
        .fetch_all(pool)
        .await
        .map_err(|e| e.to_string().lines().next().unwrap_or("").to_string())?
        .iter()
        .map(|r| r.try_get::<i32, _>("id").expect("id"))
        .collect())
}

async fn seed(pool: &sqlx::PgPool) {
    run(pool, "CREATE TABLE utr (id INT NOT NULL, domain_id TEXT NOT NULL, user_table_id TEXT NOT NULL, entry_order BIGINT NOT NULL, data JSONB)")
        .await
        .expect("create");
    // The date lives at key "2". One empty string, one missing key — both
    // must become NULL through NULLIF/absence, and sort as NULLs.
    for (id, order, doc) in [
        (1, 10, r#"{"2": "2026-08-24"}"#),
        (2, 20, r#"{"2": "2026-08-20"}"#),
        (3, 30, r#"{"2": ""}"#),
        (4, 40, r#"{}"#),
        (5, 50, r#"{"2": "2026-08-22"}"#),
    ] {
        run(pool, &format!(
            "INSERT INTO utr (id, domain_id, user_table_id, entry_order, data) VALUES ({}, 'd1', 't1', {}, '{}')",
            id, order, doc
        ))
        .await
        .expect("insert");
    }
    // A row the WHERE must exclude, with a date that would sort first.
    run(pool, r#"INSERT INTO utr (id, domain_id, user_table_id, entry_order, data) VALUES (6, 'd2', 't1', 5, '{"2": "2000-01-01"}')"#)
        .await
        .expect("insert");
}

/// The production statement, near verbatim.
#[tokio::test]
async fn sorts_by_a_date_inside_a_document_with_nulls_first() {
    let (_s, pool) = server().await;
    seed(&pool).await;

    let got = ids(
        &pool,
        "SELECT id FROM utr WHERE domain_id = 'd1' AND user_table_id = 't1' \
         ORDER BY NULLIF(data->>'2', '')::date ASC NULLS FIRST, entry_order ASC, id ASC \
         LIMIT 5000 OFFSET 0",
    )
    .await
    .expect("the list-view sort must work");
    // NULLs first (empty string, then missing key, by entry_order), then the
    // dates ascending. The excluded domain never appears.
    assert_eq!(got, vec![3, 4, 2, 5, 1]);
}

/// The same key reversed, NULLs sent to the back, and a page cut after the
/// sort — OFFSET and LIMIT must apply to the sorted order.
#[tokio::test]
async fn reversed_with_nulls_last_and_paged() {
    let (_s, pool) = server().await;
    seed(&pool).await;

    let got = ids(
        &pool,
        "SELECT id FROM utr WHERE domain_id = 'd1' \
         ORDER BY NULLIF(data->>'2', '')::date DESC NULLS LAST, id ASC",
    )
    .await
    .expect("descending sort");
    assert_eq!(got, vec![1, 5, 2, 3, 4]);

    let page = ids(
        &pool,
        "SELECT id FROM utr WHERE domain_id = 'd1' \
         ORDER BY NULLIF(data->>'2', '')::date DESC NULLS LAST, id ASC LIMIT 2 OFFSET 1",
    )
    .await
    .expect("paged sort");
    assert_eq!(page, vec![5, 2], "the page comes from the sorted order");
}

/// A plain nullable column with an explicit placement: the default puts
/// NULLs last ascending, so `NULLS FIRST` must actually move them.
#[tokio::test]
async fn a_plain_column_honours_an_explicit_nulls_placement() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE ev (id INT NOT NULL, seen_at TIMESTAMPTZ)")
        .await
        .expect("create");
    for (id, ts) in [(1, "'2026-08-20 10:00:00'"), (2, "NULL"), (3, "'2026-08-21 10:00:00'")] {
        run(&pool, &format!("INSERT INTO ev (id, seen_at) VALUES ({}, {})", id, ts))
            .await
            .expect("insert");
    }
    let got = ids(&pool, "SELECT id FROM ev ORDER BY seen_at ASC NULLS FIRST, id ASC")
        .await
        .expect("nulls first on a plain column");
    assert_eq!(got, vec![2, 1, 3]);
    let got = ids(&pool, "SELECT id FROM ev ORDER BY seen_at ASC, id ASC")
        .await
        .expect("default placement");
    assert_eq!(got, vec![1, 3, 2], "default ascending puts NULLs last");
}

/// An expression with no JSON at all — the general facility, not a JSON
/// special case.
#[tokio::test]
async fn sorts_by_a_computed_key() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE w (id INT NOT NULL, name TEXT NOT NULL)")
        .await
        .expect("create");
    for (id, name) in [(1, "banana"), (2, "Apple"), (3, "cherry")] {
        run(&pool, &format!("INSERT INTO w (id, name) VALUES ({}, '{}')", id, name))
            .await
            .expect("insert");
    }
    let got = ids(&pool, "SELECT id FROM w ORDER BY LOWER(name) ASC")
        .await
        .expect("lower() sort");
    assert_eq!(got, vec![2, 1, 3], "case-insensitive order");
}
