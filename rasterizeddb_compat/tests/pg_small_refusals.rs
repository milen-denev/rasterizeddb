//! Three small refusals PostgreSQL does not make.
//!
//! - A braced UUID, `'{6ba7b810-…}'`, which `uuid_in` accepts, was refused
//!   as input and as a comparison literal.
//! - A number on the left of column arithmetic, `12 = v * 1`, failed with
//!   "unexpected Op(*) after the end of the expression"; `v * 1 = 12` worked.
//! - A second constraint of a taken name answered 42P07 (duplicate_table);
//!   PostgreSQL's is 42710 (duplicate_object).

use rasterizeddb_compat::TestServer;
use sqlx::Executor;

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    (server, pool)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_braced_uuid_is_one() {
    let (_server, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE uu (id UUID PRIMARY KEY, n BIGINT);
         INSERT INTO uu VALUES ('{6ba7b810-9dad-11d1-80b4-00c04fd430c8}', 1);",
    ))
    .await
    .unwrap();

    let id: uuid::Uuid = sqlx::query_scalar("SELECT id FROM uu WHERE id = '{6ba7b810-9dad-11d1-80b4-00c04fd430c8}'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(id.to_string(), "6ba7b810-9dad-11d1-80b4-00c04fd430c8");
    let n: i64 = sqlx::query_scalar("SELECT n FROM uu WHERE id = '{6BA7B810-9DAD-11D1-80B4-00C04FD430C8}'::uuid")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 1);
    // An unbalanced brace is still refused.
    let e = sqlx::query("INSERT INTO uu VALUES ('{6ba7b810-9dad-11d1-80b4-00c04fd430c9', 2)")
        .execute(&pool)
        .await
        .expect_err("half a brace");
    assert_eq!(e.as_database_error().and_then(|d| d.code()).as_deref(), Some("22P02"), "{e}");
}
