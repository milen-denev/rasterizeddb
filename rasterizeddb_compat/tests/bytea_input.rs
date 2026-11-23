//! `bytea` input must store exactly the bytes PostgreSQL's `byteain` stores.
//!
//! The old parser trimmed escape-format input (so `' ab '` stored two bytes,
//! not four), copied backslash escapes through literally (`'\001'` stored four
//! bytes, not one) and refused whitespace between hex pairs. Each was a silent
//! wrong value, never an error: the insert succeeded and the column held other
//! bytes. Every case here reads the stored value back through the binary
//! protocol, so the comparison is on bytes rather than on a rendering.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn setup() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    pool.execute("CREATE TABLE blobs (id INTEGER NOT NULL, b BYTEA NOT NULL)")
        .await
        .expect("create");
    (server, pool)
}

async fn stored(pool: &sqlx::PgPool, id: i32) -> Vec<u8> {
    sqlx::query("SELECT b FROM blobs WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("read back id {}: {}", id, e))
        .get::<Vec<u8>, _>(0)
}

/// `(sql literal, bytes PostgreSQL stores)`. Literals use standard-conforming
/// strings, so a backslash in `'…'` reaches `byteain` as a backslash.
const ACCEPTED: &[(&str, &[u8])] = &[
    // Escape format.
    (r"' ab '", b" ab "),
    (r"'abc'", b"abc"),
    (r"''", b""),
    (r"'\001'", &[1]),
    (r"'\000'", &[0]),
    (r"'\377'", &[0xff]),
    (r"'a\\b'", b"a\\b"),
    (r"'\\'", b"\\"),
    (r"'\101\102C'", b"ABC"),
    (r"'\0012'", &[1, b'2']),
    (r"'it''s'", b"it's"),
    (r"'''ab'''", b"'ab'"),
    ("'é'", "é".as_bytes()),
    // Hex format: whitespace between pairs, either case of digit.
    (r"'\x'", b""),
    (r"'\xdead'", &[0xde, 0xad]),
    (r"'\xDEAD'", &[0xde, 0xad]),
    (r"'\x de ad'", &[0xde, 0xad]),
    ("'\\x\tde\nad\r '", &[0xde, 0xad]),
    // An escape string: the lexer turns `\\` into one backslash first.
    (r"E'\\001'", &[1]),
    (r"E'\\x0102'", &[1, 2]),
    (r"E'\\\\'", b"\\"),
];

#[tokio::test(flavor = "multi_thread")]
async fn bytea_literals_store_what_postgres_stores() {
    let (_s, pool) = setup().await;
    let mut failures = Vec::new();
    for (i, (literal, expected)) in ACCEPTED.iter().enumerate() {
        let id = i as i32;
        let sql = format!("INSERT INTO blobs (id, b) VALUES ({}, {})", id, literal);
        if let Err(e) = pool.execute(sqlx::raw_sql(AssertSqlSafe(sql))).await {
            failures.push(format!("{}: insert failed: {}", literal, e));
            continue;
        }
        let got = stored(&pool, id).await;
        if got != *expected {
            failures.push(format!("{}: stored {:?}, expected {:?}", literal, got, expected));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// Inputs `byteain` refuses. Each must fail the statement rather than store
/// something.
const REFUSED: &[&str] = &[
    r"'\'",       // a lone backslash
    r"'a\b'",     // backslash followed by neither `\` nor three octal digits
    r"'\400'",    // first octal digit above 3
    r"'\01'",     // two digits only
    r"'\x0'",     // odd number of hex digits
    r"'\xd e'",   // whitespace inside a pair
    r"'\xzz'",    // not hex
    r"'\X00'",    // upper-case X is not the hex marker, and `\X` is no escape
];

#[tokio::test(flavor = "multi_thread")]
async fn malformed_bytea_is_refused_not_stored() {
    let (_s, pool) = setup().await;
    let mut failures = Vec::new();
    for (i, literal) in REFUSED.iter().enumerate() {
        let sql = format!("INSERT INTO blobs (id, b) VALUES ({}, {})", 1000 + i, literal);
        if pool.execute(sqlx::raw_sql(AssertSqlSafe(sql))).await.is_ok() {
            let got = stored(&pool, 1000 + i as i32).await;
            failures.push(format!("{}: accepted, stored {:?}", literal, got));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

#[tokio::test(flavor = "multi_thread")]
async fn bound_bytea_parameters_keep_every_byte() {
    let (_s, pool) = setup().await;
    // Binary protocol: sqlx sends `Vec<u8>` as bytea in binary format.
    let all: Vec<u8> = (0..=255u8).collect();
    for (id, value) in [
        (1, all.clone()),
        (2, Vec::new()),
        (3, b"\\".to_vec()),
        (4, b" \\x00 ".to_vec()),
        (5, b"'".to_vec()),
    ] {
        sqlx::query("INSERT INTO blobs (id, b) VALUES ($1, $2)")
            .bind(id)
            .bind(&value)
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("insert {}: {}", id, e));
        assert_eq!(stored(&pool, id).await, value, "id {}", id);
    }
    // A text-format parameter of type bytea is handed to `byteain` as is.
    sqlx::query("INSERT INTO blobs (id, b) VALUES ($1, $2::bytea)")
        .bind(10)
        .bind(r"\x de ad")
        .execute(&pool)
        .await
        .expect("text bytea param");
    assert_eq!(stored(&pool, 10).await, vec![0xde, 0xad]);
    sqlx::query("INSERT INTO blobs (id, b) VALUES ($1, $2::bytea)")
        .bind(11)
        .bind(r"\001 ")
        .execute(&pool)
        .await
        .expect("text bytea param");
    assert_eq!(stored(&pool, 11).await, vec![1, b' ']);
}

#[tokio::test(flavor = "multi_thread")]
async fn update_and_multirow_insert_read_bytea_the_same_way() {
    let (_s, pool) = setup().await;
    pool.execute(sqlx::raw_sql(AssertSqlSafe(
        r"INSERT INTO blobs (id, b) VALUES (1, ' a '), (2, '\001\\'), (3, '\x 01 02')".to_string(),
    )))
    .await
    .expect("multi-row insert");
    assert_eq!(stored(&pool, 1).await, b" a ");
    assert_eq!(stored(&pool, 2).await, vec![1, b'\\']);
    assert_eq!(stored(&pool, 3).await, vec![1, 2]);

    pool.execute(sqlx::raw_sql(AssertSqlSafe(
        r"UPDATE blobs SET b = '\377 ' WHERE id = 1".to_string(),
    )))
    .await
    .expect("update");
    assert_eq!(stored(&pool, 1).await, vec![0xff, b' ']);
    pool.execute(sqlx::raw_sql(AssertSqlSafe(
        r"UPDATE blobs SET b = E'\\x ab' WHERE id = 2".to_string(),
    )))
    .await
    .expect("update with an escape string");
    assert_eq!(stored(&pool, 2).await, vec![0xab]);

    let refused = pool
        .execute(sqlx::raw_sql(AssertSqlSafe(
            r"UPDATE blobs SET b = '\' WHERE id = 3".to_string(),
        )))
        .await;
    assert!(refused.is_err(), "a lone backslash must fail the update");
    assert_eq!(stored(&pool, 3).await, vec![1, 2], "a refused update changes nothing");
}
