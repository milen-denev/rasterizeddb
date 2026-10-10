//! `length`, `char_length`, `character_length` and `octet_length`.
//!
//! These were refused outright ("function 'length' is not supported yet"),
//! though ORMs emit them for validation and search. PostgreSQL counts
//! *characters* for text and bytes for `octet_length` and for `bytea`, returns
//! `integer`, and returns NULL for NULL.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Row};

async fn seeded() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    sqlx::query("CREATE TABLE lf (id INTEGER NOT NULL, t TEXT, b BYTEA)")
        .execute(&pool)
        .await
        .expect("create");
    for sql in [
        "INSERT INTO lf (id, t, b) VALUES (1, 'abc', '\\x010203')",
        "INSERT INTO lf (id, t, b) VALUES (2, 'héllo', '\\x')",
        "INSERT INTO lf (id, t, b) VALUES (3, '日本語', NULL)",
        "INSERT INTO lf (id, t, b) VALUES (4, '', '\\xff')",
        "INSERT INTO lf (id, t, b) VALUES (5, NULL, NULL)",
    ] {
        sqlx::query(AssertSqlSafe(sql)).execute(&pool).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    (server, pool)
}

async fn ints(pool: &sqlx::PgPool, sql: &str) -> Vec<Option<i32>> {
    sqlx::query(AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|r| r.get::<Option<i32>, _>(0))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn length_counts_characters_and_octet_length_counts_bytes() {
    let (_s, pool) = seeded().await;
    let chars = vec![Some(3), Some(5), Some(3), Some(0), None];
    for f in ["length", "char_length", "character_length"] {
        assert_eq!(ints(&pool, &format!("SELECT {f}(t) FROM lf ORDER BY id")).await, chars, "{f}");
    }
    assert_eq!(
        ints(&pool, "SELECT octet_length(t) FROM lf ORDER BY id").await,
        vec![Some(3), Some(6), Some(9), Some(0), None]
    );
    // `length(bytea)` is bytes, as in PostgreSQL.
    assert_eq!(
        ints(&pool, "SELECT length(b) FROM lf ORDER BY id").await,
        vec![Some(3), Some(0), None, Some(1), None]
    );
}

/// ORDER BY goes through the expression evaluator, so it works. A function
/// call in WHERE does not yet (no post-scan expression filter exists for the
/// single-table path), which is a general gap, not specific to `length`.
#[tokio::test(flavor = "multi_thread")]
async fn length_works_in_order_by() {
    let (_s, pool) = seeded().await;
    let ids: Vec<i32> = sqlx::query("SELECT id FROM lf WHERE t IS NOT NULL ORDER BY length(t) DESC, id")
        .fetch_all(&pool)
        .await
        .expect("order by")
        .iter()
        .map(|r| r.get::<i32, _>(0))
        .collect();
    assert_eq!(ids, vec![2, 1, 3, 4]);
}
