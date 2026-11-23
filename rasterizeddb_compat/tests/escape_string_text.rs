//! `E'…'` escape strings stored into a TEXT column hold the characters they
//! mean, through both protocols: `E'a\nb'` is three bytes with a newline, and
//! `E'it\'s'` is `it's`.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

const CASES: &[(&str, &str)] = &[
    (r"E'a\nb'", "a\nb"),
    (r"E'it\'s'", "it's"),
    (r"E'tab\there'", "tab\there"),
    (r"E'back\\slash'", "back\\slash"),
    (r"e'é'", "é"),
    (r"E'x''y'", "x'y"),
    // A standard string keeps its backslashes.
    (r"'a\nb'", "a\\nb"),
];

async fn read(pool: &sqlx::PgPool, id: i32) -> Vec<u8> {
    sqlx::query("SELECT t FROM s WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("read {}: {}", id, e))
        .get::<String, _>(0)
        .into_bytes()
}

#[tokio::test(flavor = "multi_thread")]
async fn escape_strings_store_what_they_mean() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute("CREATE TABLE s (id INTEGER NOT NULL, t TEXT NOT NULL)")
        .await
        .expect("create");
    let mut failures = Vec::new();
    for (i, (literal, want)) in CASES.iter().enumerate() {
        // Simple protocol.
        let simple = i as i32;
        let sql = format!("INSERT INTO s (id, t) VALUES ({}, {})", simple, literal);
        if let Err(e) = pool.execute(sqlx::raw_sql(AssertSqlSafe(sql))).await {
            failures.push(format!("simple {}: {}", literal, e));
        } else if read(&pool, simple).await != want.as_bytes() {
            failures.push(format!("simple {}: stored {:?}", literal, String::from_utf8_lossy(&read(&pool, simple).await)));
        }
        // Extended protocol: the literal inside a prepared statement.
        let extended = 100 + i as i32;
        let sql = format!("INSERT INTO s (id, t) VALUES ($1, {})", literal);
        if let Err(e) = sqlx::query(AssertSqlSafe(sql)).bind(extended).execute(&pool).await {
            failures.push(format!("extended {}: {}", literal, e));
        } else if read(&pool, extended).await != want.as_bytes() {
            failures.push(format!("extended {}: stored {:?}", literal, String::from_utf8_lossy(&read(&pool, extended).await)));
        }
    }
    // UPDATE takes the same literals.
    pool.execute(sqlx::raw_sql(AssertSqlSafe(r"UPDATE s SET t = E'up\ndate' WHERE id = 0".to_string())))
        .await
        .expect("update");
    if read(&pool, 0).await != b"up\ndate" {
        failures.push(format!("update: stored {:?}", String::from_utf8_lossy(&read(&pool, 0).await)));
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}
