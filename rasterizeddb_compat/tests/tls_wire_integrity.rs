//! The same reads and writes, over TLS.
//!
//! The deployment runs SCRAM over TLS; every other test here connects in the
//! clear. That matters for wire bugs specifically: a TLS stream buffers writes
//! until flush, so message boundaries land differently, and a framing or
//! truncation fault can appear on one transport and not the other.
//!
//! Anything that corrupts the stream shows up as a client-side panic inside
//! `bytes` rather than an error, because sqlx's `ParameterDescription` and
//! array decoders take 4-byte reads with no length check.

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::auth::AuthMethod;
use rasterizeddb_core::pgwire::TlsMode;
use sqlx::{Executor, Row};

async fn tls_server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start_with(TlsMode::Require, AuthMethod::Trust, None)
        .await
        .expect("tls server should start");
    let pool = server.connect_sqlx().await.expect("should connect over TLS");
    (server, pool)
}

/// Wide rows, many of them, with every nullable type — a page load's worth.
#[tokio::test(flavor = "multi_thread")]
async fn wide_result_sets_survive_tls() {
    let (_s, pool) = tls_server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE field (id INT PRIMARY KEY, case_id INT NOT NULL, label TEXT NOT NULL, \
         content_string TEXT, content_string_vec TEXT[], content_attachment BYTEA, \
         content_number INT, content_bool BOOLEAN, note TEXT)",
    ))
    .await
    .expect("create");

    for i in 0..60i32 {
        sqlx::query(
            "INSERT INTO field (id, case_id, label, content_string, note) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(i)
        .bind(7i32)
        .bind(format!("label-{}", i))
        .bind(if i % 3 == 0 { None } else { Some(format!("value-{}", i)) })
        .bind("x".repeat(200 + (i as usize % 97)))
        .execute(&pool)
        .await
        .unwrap_or_else(|e| panic!("insert {}: {}", i, e));
    }

    let rows = sqlx::query(
        "SELECT id, case_id, label, content_string, content_string_vec, content_attachment, \
                content_number, content_bool, note FROM field",
    )
    .fetch_all(&pool)
    .await
    .expect("wide select over TLS");
    assert_eq!(rows.len(), 60);
    for row in &rows {
        let _ = row.get::<i32, _>("id");
        let _ = row.get::<Option<String>, _>("content_string");
        let _ = row.get::<Option<Vec<String>>, _>("content_string_vec");
        let _ = row.get::<Option<Vec<u8>>, _>("content_attachment");
        let _ = row.get::<Option<i32>, _>("content_number");
        let _ = row.get::<Option<bool>, _>("content_bool");
        let _ = row.get::<String, _>("note");
    }
}

/// Writes with RETURNING, repeated, on one TLS connection.
#[tokio::test(flavor = "multi_thread")]
async fn repeated_returning_writes_survive_tls() {
    let (_s, pool) = tls_server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE f2 (id INT PRIMARY KEY, a TEXT, b TEXT[], c BYTEA, d INT)",
    ))
    .await
    .expect("create");

    let mut conn = pool.acquire().await.expect("acquire");
    for i in 0..40i32 {
        let row = sqlx::query(
            "INSERT INTO f2 (id, a) VALUES ($1, $2) RETURNING id, a, b, c, d",
        )
        .bind(i)
        .bind(format!("v{}", i))
        .fetch_one(&mut *conn)
        .await
        .unwrap_or_else(|e| panic!("insert returning {}: {}", i, e));
        assert_eq!(row.get::<i32, _>("id"), i);
        assert_eq!(row.get::<Option<Vec<String>>, _>("b"), None);

        let row = sqlx::query("UPDATE f2 SET a = $1 WHERE id = $2 RETURNING id, a, b, c, d")
            .bind(format!("u{}", i))
            .bind(i)
            .fetch_one(&mut *conn)
            .await
            .unwrap_or_else(|e| panic!("update returning {}: {}", i, e));
        assert_eq!(row.get::<Option<Vec<u8>>, _>("c"), None);
    }
}

/// Errors interleaved with successes on one TLS connection, which is what a
/// live app produces and what the protocol's error recovery governs.
#[tokio::test(flavor = "multi_thread")]
async fn errors_between_queries_survive_tls() {
    let (_s, pool) = tls_server().await;
    pool.execute(sqlx::raw_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)"))
        .await
        .expect("create");
    pool.execute(sqlx::raw_sql("INSERT INTO t (id, v) VALUES (1, 10)"))
        .await
        .expect("seed");

    let mut conn = pool.acquire().await.expect("acquire");
    for round in 0..20 {
        let bad = sqlx::query("SELECT no_such_column FROM t WHERE id = $1")
            .bind(1i32)
            .fetch_all(&mut *conn)
            .await;
        assert!(bad.is_err(), "round {}", round);

        let row = sqlx::query("SELECT id, v FROM t WHERE id = $1")
            .bind(1i32)
            .fetch_one(&mut *conn)
            .await
            .unwrap_or_else(|e| panic!("round {}: connection unusable after error: {}", round, e));
        assert_eq!(row.get::<Option<i32>, _>("v"), Some(10));
    }
}

/// Concurrent load over TLS.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_tls_clients_stay_in_sync() {
    let (_s, pool) = tls_server().await;
    pool.execute(sqlx::raw_sql("CREATE TABLE c (id INT PRIMARY KEY, s TEXT, a TEXT[])"))
        .await
        .expect("create");
    for i in 0..20i32 {
        sqlx::query("INSERT INTO c (id, s) VALUES ($1, $2)")
            .bind(i)
            .bind("x".repeat(500))
            .execute(&pool)
            .await
            .expect("seed");
    }

    let mut handles = Vec::new();
    for _ in 0..8 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..10 {
                let rows = sqlx::query("SELECT id, s, a FROM c")
                    .fetch_all(&pool)
                    .await
                    .map_err(|e| e.to_string())?;
                for row in &rows {
                    let _: i32 = row.get("id");
                    let _: Option<String> = row.get("s");
                    let _: Option<Vec<String>> = row.get("a");
                }
            }
            Ok::<(), String>(())
        }));
    }
    for h in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => panic!("a TLS client failed: {}", e),
            Err(join) => panic!("a TLS client task died: {}", join),
        }
    }
}
