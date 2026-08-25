//! Binding a document as a parameter.
//!
//! `jsonb`'s binary wire form is a one-byte format version followed by the
//! document text; `json` is the text alone. The outbound side already knew
//! this — a document was rendered back to a client that way — but the inbound
//! side had no arm for either OID, so binding one was refused with
//! "unsupported binary parameter type OID 3802". Every ORM sends that for a
//! JSON column: pms's case-export settings update failed on exactly this.

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::TestServer;
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

#[tokio::test]
async fn a_document_can_be_bound_as_a_parameter() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE case_settings (id INT PRIMARY KEY, config JSONB)")
        .await
        .expect("create");

    let doc = serde_json::json!({
        "export": {"format": "pdf", "pages": 12},
        "tags": ["a", "b"],
        "quoted": "it's \"fine\"",
        "enabled": true,
        "ratio": 0.25
    });

    // INSERT with the document bound — the shape sea-orm sends.
    sqlx::query("INSERT INTO case_settings (id, config) VALUES ($1, $2)")
        .bind(1i32)
        .bind(&doc)
        .execute(&pool)
        .await
        .expect("binding a jsonb parameter must work");

    let back: serde_json::Value = sqlx::query("SELECT config FROM case_settings WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("read back")
        .try_get("config")
        .expect("config");
    assert_eq!(back, doc, "the document round-trips, quotes and all");

    // UPDATE … RETURNING with a bound document — the failing production path.
    let updated = serde_json::json!({"export": {"format": "csv"}, "tags": []});
    let row = sqlx::query("UPDATE case_settings SET config = $1 WHERE id = $2 RETURNING config")
        .bind(&updated)
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("updating with a bound document must work");
    assert_eq!(
        row.try_get::<serde_json::Value, _>("config").expect("config"),
        updated
    );

    // A NULL document parameter stays NULL, not an empty document.
    let none: Option<serde_json::Value> = None;
    sqlx::query("UPDATE case_settings SET config = $1 WHERE id = $2")
        .bind(&none)
        .bind(1i32)
        .execute(&pool)
        .await
        .expect("binding a NULL document");
    let back: Option<serde_json::Value> = sqlx::query("SELECT config FROM case_settings WHERE id = $1")
        .bind(1i32)
        .fetch_one(&pool)
        .await
        .expect("read back")
        .try_get("config")
        .expect("config");
    assert_eq!(back, None, "a NULL document is NULL");

    // A document used in a predicate through a path operator, bound as text.
    sqlx::query("UPDATE case_settings SET config = $1 WHERE id = $2")
        .bind(&doc)
        .bind(1i32)
        .execute(&pool)
        .await
        .expect("restore");
    let found: i64 = pool
        .fetch_one(sqlx::raw_sql(
            "SELECT COUNT(*) FROM case_settings WHERE config->'export'->>'format' = 'pdf'",
        ))
        .await
        .expect("path predicate")
        .try_get(0)
        .expect("n");
    assert_eq!(found, 1, "the bound document is queryable by path");
}
