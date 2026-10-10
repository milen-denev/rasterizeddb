//! Deduplication must tell NULL from an empty value.
//!
//! A NULL's value column holds the type's zeroes — empty bytes for TEXT and
//! BYTEA — and the NULL itself lives in the `__null_` companion or in a cell
//! typed NULL. A dedup key built from the value bytes alone therefore merged
//! `NULL` with `''`, and `SELECT DISTINCT` over {NULL, '', 'a'} answered two
//! rows. PostgreSQL answers three, on every path that removes duplicates.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    for sql in [
        "CREATE TABLE dn (id INTEGER NOT NULL, t TEXT, b BYTEA, n BIGINT)",
        "CREATE TABLE dk (id INTEGER NOT NULL, tag TEXT NOT NULL)",
    ] {
        sqlx::query(sql).execute(&pool).await.expect("create");
    }
    // Each of NULL, '' and 'a' twice; bytea and bigint carry NULL / empty / a
    // value alongside.
    let values: [(Option<&str>, Option<&[u8]>, Option<i64>); 6] = [
        (None, None, None),
        (Some(""), Some(b""), Some(0)),
        (Some("a"), Some(b"a"), Some(1)),
        (None, None, None),
        (Some(""), Some(b""), Some(0)),
        (Some("a"), Some(b"a"), Some(1)),
    ];
    for (id, (t, b, n)) in values.iter().enumerate() {
        sqlx::query("INSERT INTO dn (id, t, b, n) VALUES ($1, $2, $3, $4)")
            .bind(id as i32)
            .bind(*t)
            .bind(*b)
            .bind(*n)
            .execute(&pool)
            .await
            .expect("insert");
        sqlx::query("INSERT INTO dk (id, tag) VALUES ($1, 'k')")
            .bind(id as i32)
            .execute(&pool)
            .await
            .expect("insert");
    }
    (server, pool)
}

/// The first column of each row, NULL as `None`.
async fn texts(pool: &sqlx::PgPool, sql: &str) -> Vec<Option<String>> {
    let mut out: Vec<Option<String>> = sqlx::query(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.get::<Option<String>, _>(0))
        .collect();
    out.sort();
    out
}

fn three() -> Vec<Option<String>> {
    vec![None, Some(String::new()), Some("a".to_string())]
}

#[tokio::test(flavor = "multi_thread")]
async fn distinct_keeps_null_and_empty_apart_on_every_path() {
    let (_server, pool) = seeded().await;
    for sql in [
        // Single table. (DISTINCT with an expression sort or explicit NULLS
        // placement is refused outright, so there is no such path to test.)
        "SELECT DISTINCT t FROM dn",
        "SELECT DISTINCT t FROM dn ORDER BY t",
        "SELECT DISTINCT t FROM dn LIMIT 10",
        // A join.
        "SELECT DISTINCT dn.t FROM dn JOIN dk ON dk.id = dn.id",
        // A derived and a grouped source.
        "SELECT DISTINCT s.t FROM (SELECT t FROM dn) AS s",
        "SELECT DISTINCT g.t FROM (SELECT t, COUNT(*) AS c FROM dn GROUP BY t) AS g",
        "SELECT t FROM dn GROUP BY t",
        // Set operations.
        "SELECT t FROM dn UNION SELECT t FROM dn",
    ] {
        assert_eq!(texts(&pool, sql).await, three(), "{sql}");
    }
    // UNION ALL keeps everything.
    assert_eq!(texts(&pool, "SELECT t FROM dn UNION ALL SELECT t FROM dn").await.len(), 12);

    // Other columns: NULL vs zero-length BYTEA, NULL vs 0 BIGINT.
    for sql in ["SELECT DISTINCT b FROM dn", "SELECT DISTINCT n FROM dn"] {
        let rows = sqlx::query(AssertSqlSafe(sql.to_string()))
            .fetch_all(&pool)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(rows.len(), 3, "{sql}");
        let nulls = rows
            .iter()
            .filter(|r| r.try_get_raw(0).map(|v| sqlx::ValueRef::is_null(&v)).unwrap_or(false))
            .count();
        assert_eq!(nulls, 1, "{sql}");
    }
    // Several columns at once: (NULL, NULL) and ('', '') are different rows.
    let rows = sqlx::query("SELECT DISTINCT t, b FROM dn")
        .fetch_all(&pool)
        .await
        .expect("distinct pair");
    assert_eq!(rows.len(), 3);
}
