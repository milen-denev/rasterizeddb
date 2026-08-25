//! `GROUP BY` over a column that holds NULLs.
//!
//! The fold built its group key by concatenating each key column's bytes and
//! used the empty byte string for a NULL — which is also what an empty string
//! encodes to. `NULL` and `''` were therefore the same group, and the emitted
//! key was empty bytes wearing the *column's* type. For TEXT that read back as
//! `''`: a silent wrong answer. For a non-text column it was worse — a typed
//! client got a zero-length value under an OID that has no zero-length
//! encoding and failed the whole row, which is how this surfaced (a shredded
//! `jsonb` column beside `count(*)`).
//!
//! NULL is now its own key with its own length marker, and a NULL key is
//! emitted as a NULL rather than as empty bytes.

use rasterizeddb_compat::TestServer;
use sqlx::Row;

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn run(pool: &sqlx::PgPool, sql: &str) {
    sqlx::query(sql)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e));
}

#[tokio::test]
async fn a_null_group_key_is_null_and_not_an_empty_string() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE t (id INT NOT NULL, label TEXT)").await;
    run(&pool, "INSERT INTO t (id, label) VALUES (1, NULL)").await;
    run(&pool, "INSERT INTO t (id, label) VALUES (2, NULL)").await;
    run(&pool, "INSERT INTO t (id, label) VALUES (3, '')").await;
    run(&pool, "INSERT INTO t (id, label) VALUES (4, 'x')").await;

    let rows = sqlx::query("SELECT label, count(*) AS n FROM t GROUP BY label")
        .fetch_all(&pool)
        .await
        .expect("the grouped select runs");

    let mut seen: Vec<(Option<String>, i64)> = rows
        .iter()
        .map(|r| {
            (
                r.try_get::<Option<String>, _>("label").expect("label"),
                r.try_get::<i64, _>("n").expect("n"),
            )
        })
        .collect();
    seen.sort();

    // Three groups, not two: NULL and '' must not be merged.
    assert_eq!(
        seen,
        vec![
            (None, 2),
            (Some(String::new()), 1),
            (Some("x".to_string()), 1),
        ],
        "NULL and the empty string must be distinct groups"
    );
}

/// The failure that made this visible: a non-text key column. Empty bytes
/// under an INT8 OID is not a value a driver can decode, so the row failed
/// rather than reading back as a wrong-but-plausible zero.
#[tokio::test]
async fn a_null_group_key_of_a_non_text_type_decodes() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE m (id INT NOT NULL, amount BIGINT)").await;
    run(&pool, "INSERT INTO m (id, amount) VALUES (1, NULL)").await;
    run(&pool, "INSERT INTO m (id, amount) VALUES (2, 7)").await;

    let rows = sqlx::query("SELECT amount, count(*) AS n FROM m GROUP BY amount")
        .fetch_all(&pool)
        .await
        .expect("the grouped select runs");

    let mut seen: Vec<Option<i64>> = rows
        .iter()
        .map(|r| {
            r.try_get::<Option<i64>, _>("amount")
                .expect("a NULL group key must decode as NULL, not as empty bytes")
        })
        .collect();
    seen.sort();
    assert_eq!(seen, vec![None, Some(7)]);
}

/// Teaching the fold to recognise NULLs risks the opposite error: `COUNT(col)`
/// must still skip them while `COUNT(*)` still counts the row. Both had read
/// the same "empty bytes" before, so separating them could have flipped either
/// one. The capability matrix caught this when it flipped; this states it.
#[tokio::test]
async fn count_of_a_column_skips_nulls_and_count_star_does_not() {
    let (_s, pool) = server().await;
    run(&pool, "CREATE TABLE c (id INT NOT NULL, label TEXT)").await;
    run(&pool, "INSERT INTO c (id, label) VALUES (1, 'a')").await;
    run(&pool, "INSERT INTO c (id, label) VALUES (2, NULL)").await;
    run(&pool, "INSERT INTO c (id, label) VALUES (3, '')").await;

    let row = sqlx::query("SELECT count(*) AS all_rows, count(label) AS labelled FROM c")
        .fetch_one(&pool)
        .await
        .expect("the aggregate runs");

    assert_eq!(row.try_get::<i64, _>("all_rows").expect("all_rows"), 3);
    // The empty string is a value; only the NULL is skipped. Postgres counts
    // it too, and an off-by-one here is the kind that reads as plausible.
    assert_eq!(row.try_get::<i64, _>("labelled").expect("labelled"), 2);
}
