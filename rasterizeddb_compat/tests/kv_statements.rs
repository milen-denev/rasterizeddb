//! `KV …` statements: the Redis-compatible store, from SQL through a real
//! driver.
//!
//! What a client sees: typed columns described before execution (sqlx uses
//! the extended protocol, so a wrong `Describe` is a decode error), bound
//! parameters inside a `KV` statement, rows shaped by the command, the
//! refusals, the redaction of keys and values on the statement surfaces, and
//! the superuser rule on the commands that administer the server.

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::auth::AuthMethod;
use rasterizeddb_core::pgwire::TlsMode;
use sqlx::{AssertSqlSafe, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

#[tokio::test(flavor = "multi_thread")]
async fn strings_counters_and_missing_keys() {
    let (_s, pool) = server().await;
    let ok: String = sqlx::query_scalar("KV SET greeting 'hello world' EX 3600")
        .fetch_one(&pool)
        .await
        .expect("set");
    assert_eq!(ok, "OK");
    let v: String = sqlx::query_scalar("KV GET greeting").fetch_one(&pool).await.expect("get");
    assert_eq!(v, "hello world");
    // A missing key is no row, not a NULL row.
    let rows = sqlx::query("KV GET nothing-here").fetch_all(&pool).await.expect("get missing");
    assert!(rows.is_empty());
    // Counters are bigint.
    for expected in 1..=3i64 {
        let n: i64 = sqlx::query_scalar("KV INCR hits").fetch_one(&pool).await.expect("incr");
        assert_eq!(n, expected);
    }
    let ttl: i64 = sqlx::query_scalar("KV TTL greeting").fetch_one(&pool).await.expect("ttl");
    assert!((3590..=3600).contains(&ttl), "{ttl}");
    // MGET keeps positions: a NULL where a key is missing.
    let values: Vec<Option<String>> = sqlx::query("KV MGET greeting nope hits")
        .fetch_all(&pool)
        .await
        .expect("mget")
        .iter()
        .map(|r| r.get::<Option<String>, _>(0))
        .collect();
    assert_eq!(values, vec![Some("hello world".into()), None, Some("3".into())]);
}

#[tokio::test(flavor = "multi_thread")]
async fn bound_parameters_carry_any_text() {
    let (_s, pool) = server().await;
    // Quotes, spaces and a semicolon inside a bound value reach the store whole.
    let tricky = "it's; a \"value\" with spaces";
    sqlx::query("KV SET $1 $2")
        .bind("param:key")
        .bind(tricky)
        .execute(&pool)
        .await
        .expect("set with binds");
    let v: String = sqlx::query_scalar("KV GET $1")
        .bind("param:key")
        .fetch_one(&pool)
        .await
        .expect("get with bind");
    assert_eq!(v, tricky);
}

#[tokio::test(flavor = "multi_thread")]
async fn structures_come_back_as_shaped_rows() {
    let (_s, pool) = server().await;
    sqlx::query("KV HSET user:7 name 'Ada' plan 'pro'").execute(&pool).await.expect("hset");
    let mut pairs: Vec<(String, String)> = sqlx::query("KV HGETALL user:7")
        .fetch_all(&pool)
        .await
        .expect("hgetall")
        .iter()
        .map(|r| (r.get("field"), r.get("value")))
        .collect();
    pairs.sort();
    assert_eq!(pairs, vec![("name".into(), "Ada".into()), ("plan".into(), "pro".into())]);

    sqlx::query("KV ZADD board 1500 ada 1320 bob 1700 cy").execute(&pool).await.expect("zadd");
    let top: Vec<(String, f64)> = sqlx::query("KV ZRANGE board 0 1 REV WITHSCORES")
        .fetch_all(&pool)
        .await
        .expect("zrange")
        .iter()
        .map(|r| (r.get("member"), r.get("score")))
        .collect();
    assert_eq!(top, vec![("cy".into(), 1700.0), ("ada".into(), 1500.0)]);
    let score: f64 = sqlx::query_scalar("KV ZSCORE board bob").fetch_one(&pool).await.expect("zscore");
    assert_eq!(score, 1320.0);

    sqlx::query("KV RPUSH jobs a b c").execute(&pool).await.expect("rpush");
    let items: Vec<String> = sqlx::query_scalar("KV LRANGE jobs 0 -1").fetch_all(&pool).await.expect("lrange");
    assert_eq!(items, vec!["a", "b", "c"]);

    for i in 0..30 {
        sqlx::query(AssertSqlSafe(format!("KV SET scan:{i} x")))
            .execute(&pool)
            .await
            .expect("set");
    }
    let mut seen = 0;
    let mut cursor = "0".to_string();
    loop {
        let rows = sqlx::query("KV SCAN $1 MATCH 'scan:*' COUNT 10")
            .bind(&cursor)
            .fetch_all(&pool)
            .await
            .expect("scan");
        let next: String = rows[0].get("cursor");
        seen += rows.iter().filter(|r| r.get::<Option<String>, _>("key").is_some()).count();
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(seen, 30);
}

#[tokio::test(flavor = "multi_thread")]
async fn errors_and_refusals_are_sql_errors() {
    let (_s, pool) = server().await;
    sqlx::query("KV SET s v").execute(&pool).await.expect("set");
    let wrong = sqlx::query("KV LPUSH s x").execute(&pool).await.expect_err("wrongtype");
    assert!(wrong.to_string().contains("WRONGTYPE"), "{wrong}");
    let unknown = sqlx::query("KV NOSUCHCOMMAND a").execute(&pool).await.expect_err("unknown");
    assert!(unknown.to_string().contains("unknown KV command"), "{unknown}");
    let multi = sqlx::query("KV MULTI").execute(&pool).await.expect_err("multi");
    assert!(multi.to_string().contains("--kv-port"), "{multi}");
    // A KV statement is not undone by a rollback: it is said, and true.
    let mut tx = pool.begin().await.expect("begin");
    sqlx::query("KV SET in-tx yes").execute(&mut *tx).await.expect("set in tx");
    tx.rollback().await.expect("rollback");
    let v: String = sqlx::query_scalar("KV GET in-tx").fetch_one(&pool).await.expect("get");
    assert_eq!(v, "yes");
}

#[tokio::test(flavor = "multi_thread")]
async fn keys_and_values_never_reach_the_statement_log() {
    let (_s, pool) = server().await;
    sqlx::query("KV SET session:9f2c41 'secret-token-value'")
        .execute(&pool)
        .await
        .expect("set");
    let recent: Vec<String> = sqlx::query("SHOW QUERIES")
        .fetch_all(&pool)
        .await
        .expect("show queries")
        .iter()
        .filter_map(|r| r.try_get::<String, _>("statement").ok())
        .collect();
    assert!(!recent.is_empty(), "SHOW QUERIES returned no statements");
    for q in &recent {
        assert!(!q.contains("9f2c41") && !q.contains("secret-token-value"), "leaked: {q}");
    }
    assert!(recent.iter().any(|q| q.starts_with("KV SET")), "{recent:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn administration_needs_a_superuser() {
    let server = TestServer::start_with(TlsMode::Disable, AuthMethod::ScramSha256, Some("boot"))
        .await
        .expect("server should start");
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&server.url_as("rasterized", Some("boot")))
        .await
        .expect("bootstrap role connects");
    sqlx::query("CREATE USER app PASSWORD 'app-pass'").execute(&admin).await.expect("create user");
    let app = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&server.url_as("app", Some("app-pass")))
        .await
        .expect("app connects");
    // Data commands are open to any role.
    sqlx::query("KV SET k v").execute(&app).await.expect("an ordinary role may write keys");
    let config = sqlx::query("KV CONFIG GET maxmemory")
        .fetch_all(&app)
        .await
        .expect("reading the settings is not administering them");
    assert_eq!(config.len(), 1);
    assert_eq!(config[0].get::<String, _>("name"), "maxmemory");
    // Administration is not.
    for sql in ["KV FLUSHALL", "KV CONFIG SET maxmemory 1gb", "KV SAVE"] {
        assert!(sqlx::query(sql).execute(&app).await.is_err(), "{sql} ran for an ordinary role");
    }
    let still: String = sqlx::query_scalar("KV GET k").fetch_one(&app).await.expect("get");
    assert_eq!(still, "v", "the refused FLUSHALL emptied nothing");
    sqlx::query("KV FLUSHALL").execute(&admin).await.expect("a superuser may flush");
}
