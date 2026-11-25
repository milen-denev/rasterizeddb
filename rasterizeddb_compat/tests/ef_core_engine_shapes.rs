//! Engine bugs EF Core 10's SQL (over the SQL Server front end) found, in
//! the engine's own SQL:
//!
//! - `x LIKE p ESCAPE e` over an expression (`CAST(c AS text)`, `upper(c)`)
//!   was a syntax error: the escape was folded into the pattern for one kind
//!   of operand only.
//! - A join whose WHERE holds a multibyte literal (`… = 'ЩЕ'`) panicked
//!   slicing two bytes from the end of the text before a word.
//! - A correlated subquery reading two outer relations an INNER JOIN keys
//!   together was refused; it reads one through the join's key.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;

async fn exec(pool: &sqlx::PgPool, sql: &str) {
    sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn ids(pool: &sqlx::PgPool, sql: &str) -> Vec<i32> {
    sqlx::query_scalar(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

async fn texts(pool: &sqlx::PgPool, sql: &str) -> Vec<Option<String>> {
    sqlx::query_scalar(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn like_escape_over_an_expression() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE le (id int PRIMARY KEY, a text)").await;
    exec(&pool, "INSERT INTO le VALUES (1, 'User-0'), (2, 'Us_r'), (3, 'Usxr')").await;
    assert_eq!(ids(&pool, r"SELECT id FROM le WHERE CAST(a AS text) LIKE '%User-0%' ESCAPE '\'").await, vec![1]);
    assert_eq!(ids(&pool, "SELECT id FROM le WHERE upper(a) LIKE 'US!_R' ESCAPE '!'").await, vec![2]);
    assert_eq!(ids(&pool, r"SELECT id FROM le WHERE lower(a) NOT LIKE 'us\_r' ESCAPE '\' ORDER BY id").await, vec![1, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_join_with_a_multibyte_literal() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE v (id int PRIMARY KEY, pid int, code text)").await;
    exec(&pool, "CREATE TABLE p (id int PRIMARY KEY, sku text)").await;
    exec(&pool, "INSERT INTO p VALUES (1, 'ЩЕ-1')").await;
    exec(&pool, "INSERT INTO v VALUES (10, 1, 'SEED0-ЩЕ')").await;
    assert_eq!(
        ids(&pool, "SELECT v.id FROM v INNER JOIN p ON v.pid = p.id WHERE v.code = 'SEED0-ЩЕ' OR p.sku = 'ЩЕ'").await,
        vec![10]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subquery_reading_two_joined_relations() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE products (id uuid PRIMARY KEY, name text)").await;
    exec(&pool, "CREATE TABLE variants (id uuid PRIMARY KEY, product_id uuid, code text)").await;
    exec(&pool, "CREATE TABLE images (id int PRIMARY KEY, product_id uuid, variant_id uuid, url text, ord int)").await;
    exec(
        &pool,
        "INSERT INTO products VALUES ('22222222-2222-2222-2222-222222222222', 'P2'), ('33333333-3333-3333-3333-333333333333', 'P3')",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO variants VALUES ('55555555-5555-5555-5555-555555555555', '22222222-2222-2222-2222-222222222222', 'A'), \
         ('66666666-6666-6666-6666-666666666666', '33333333-3333-3333-3333-333333333333', 'B')",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO images VALUES (1, '22222222-2222-2222-2222-222222222222', NULL, 'u1', 2), \
         (2, '22222222-2222-2222-2222-222222222222', '55555555-5555-5555-5555-555555555555', 'u2', 1)",
    )
    .await;
    // The image chosen for the variant, else the product's first, else a
    // placeholder: each subquery reads `p` and `v`, keyed by the join.
    let urls = texts(
        &pool,
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM images i1 WHERE p.id = i1.product_id) \
         THEN coalesce((SELECT i2.url FROM images i2 WHERE p.id = i2.product_id AND i2.variant_id = v.id ORDER BY i2.ord LIMIT 1), \
                       (SELECT i3.url FROM images i3 WHERE p.id = i3.product_id ORDER BY i3.ord LIMIT 1)) \
         ELSE 'none' END FROM variants v INNER JOIN products p ON v.product_id = p.id ORDER BY v.code",
    )
    .await;
    assert_eq!(urls, vec![Some("u2".into()), Some("none".into())]);

    // Over a LEFT JOIN the key is not equal on a NULL-extended row: no
    // substitution, the statement stays refused rather than reading
    // v.product_id where p.id is NULL.
    exec(&pool, "INSERT INTO variants VALUES ('77777777-7777-7777-7777-777777777777', NULL, 'C')").await;
    let left = sqlx::raw_sql(
        "SELECT (SELECT i.url FROM images i WHERE i.product_id = p.id AND i.variant_id IS DISTINCT FROM v.id ORDER BY i.ord LIMIT 1) \
         FROM variants v LEFT JOIN products p ON v.product_id = p.id ORDER BY v.code",
    )
    .fetch_all(&pool)
    .await;
    match left {
        Err(sqlx::Error::Database(e)) => assert_eq!(e.code().as_deref(), Some("0A000"), "{e}"),
        other => panic!("a LEFT JOIN's key was substituted: {other:?}"),
    }
    // Nor an equality under OR.
    let or = sqlx::raw_sql(
        "SELECT (SELECT i.url FROM images i WHERE i.product_id = p.id AND i.variant_id = v.id LIMIT 1) \
         FROM variants v INNER JOIN products p ON (v.product_id = p.id OR v.code = 'C') ORDER BY v.code",
    )
    .fetch_all(&pool)
    .await;
    assert!(or.is_err(), "an OR'd key was substituted: {or:?}");
}

/// A correlated subquery over an outer WHERE that keeps no row is an
/// empty result: the LATERAL relation built for no outer value had no key
/// column, and the statement failed with "Column '__lat_k1' not found".
#[tokio::test(flavor = "multi_thread")]
async fn a_correlated_subquery_over_no_outer_row() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    exec(&pool, "CREATE TABLE ar (id int PRIMARY KEY, n int)").await;
    exec(&pool, "CREATE TABLE co (id int PRIMARY KEY, aid int, title text)").await;
    exec(&pool, "INSERT INTO ar VALUES (1, 1)").await;
    exec(&pool, "INSERT INTO co VALUES (10, 1, 'T1')").await;
    let none = texts(&pool, "SELECT (SELECT c.title FROM co c WHERE d.id = c.aid LIMIT 1) FROM ar d WHERE d.n = 2").await;
    assert!(none.is_empty());
    assert_eq!(
        ids(&pool, "SELECT d.id FROM ar d LEFT JOIN LATERAL (SELECT c.title FROM co c WHERE c.aid = d.id) x ON true WHERE d.id <> 1").await,
        Vec::<i32>::new()
    );
    let two: Vec<(i32, Option<String>, Option<i32>)> = sqlx::query_as(
        "SELECT d.id, (SELECT c.title FROM co c WHERE d.id = c.aid LIMIT 1), (SELECT c.id FROM co c WHERE d.id = c.aid LIMIT 1) \
         FROM ar d WHERE d.id <> 1",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(two.is_empty());
    assert_eq!(texts(&pool, "SELECT (SELECT c.title FROM co c WHERE d.id = c.aid LIMIT 1) FROM ar d WHERE d.n = 1").await, vec![Some("T1".to_string())]);
}
