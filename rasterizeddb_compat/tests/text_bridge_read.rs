//! The read half of pms's Ultra text bridge: `SELECT "c_x"::text`.
//!
//! Every companion column is read cast to text, so one path serves every type.
//! A cast in a *projection* decides what the server advertises in
//! `RowDescription` as well as what it sends. If those disagree — advertising
//! the column's underlying type while rendering text, or the reverse — the
//! client decodes a value of the wrong width, and a zero-length rendering
//! under a fixed-width type panics inside `bytes` rather than erroring.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn setup(pool: &sqlx::PgPool) {
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE utrn_read (\
           row_id UUID NOT NULL, \
           entry_order INT NOT NULL, \
           c_text TEXT, \
           c_num BIGINT, \
           c_dec NUMERIC(12,2), \
           c_bool BOOLEAN, \
           c_date DATE, \
           c_json JSONB)",
    ))
    .await
    .expect("create");
}

/// Populated row, every column read through `::text`.
///
/// Ignored: records an open bug rather than a passing behaviour. A boolean
/// read through the bridge renders as `1`, where PostgreSQL renders `true` —
/// so a client parsing `"true"`/`"false"` gets a value it cannot read, or
/// reads it wrongly, from every boolean column in a user table.
///
/// The cast is evaluated by `expr::cast`, which has a typed→text fallback via
/// `subquery::value_to_literal` — that renders the storage form `1`/`0`,
/// correctly for the SQL literals it exists to build and wrongly for a value
/// being handed to a client. Adding a `BOOL`→`STRING` arm to `cast` does not
/// fix it: a `BOOLEAN` column's cell does not reach `cast` carrying
/// `DbType::BOOL`, so the arm never fires. Whatever physical type it does
/// carry has to be established first, which is why this is recorded rather
/// than patched.
#[ignore = "open: a boolean cast to text renders 1/0, not true/false"]
#[tokio::test]
async fn every_column_reads_through_a_text_cast() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    pool.execute(sqlx::raw_sql(
        "INSERT INTO utrn_read (row_id, entry_order, c_text, c_num, c_dec, c_bool, c_date, c_json) \
         VALUES ('54aac85c-3242-480b-8770-7345ede67e7d', 1, 'Jordan Patel', 40, 63665.39, true, '2024-02-22', '{\"a\":1}')",
    ))
    .await
    .expect("insert");

    let row = sqlx::query(
        "SELECT \"c_text\"::text AS c_text, \"c_num\"::text AS c_num, \"c_dec\"::text AS c_dec, \
                \"c_bool\"::text AS c_bool, \"c_date\"::text AS c_date, \"c_json\"::text AS c_json \
         FROM utrn_read WHERE entry_order = $1",
    )
    .bind(1i32)
    .fetch_one(&pool)
    .await
    .expect("text-bridge read");

    // Every one decodes as text, because that is what the cast promised.
    assert_eq!(row.get::<Option<String>, _>("c_text"), Some("Jordan Patel".into()));
    assert_eq!(row.get::<Option<String>, _>("c_num"), Some("40".into()));
    assert_eq!(row.get::<Option<String>, _>("c_bool"), Some("true".into()));
    let dec = row.get::<Option<String>, _>("c_dec").expect("decimal as text");
    assert!(dec.starts_with("63665.3"), "decimal rendered as {}", dec);
    let date = row.get::<Option<String>, _>("c_date").expect("date as text");
    assert!(date.starts_with("2024-02-22"), "date rendered as {}", date);
}

/// The same read with every value NULL — the shape that ships nothing.
#[tokio::test]
async fn a_text_cast_over_nulls_decodes_as_null() {
    let (_s, pool) = server().await;
    setup(&pool).await;

    pool.execute(sqlx::raw_sql(
        "INSERT INTO utrn_read (row_id, entry_order) \
         VALUES ('41289372-5b3b-4caa-8b0b-936b65ef5968', 2)",
    ))
    .await
    .expect("insert nulls");

    let row = sqlx::query(
        "SELECT \"c_text\"::text AS c_text, \"c_num\"::text AS c_num, \"c_dec\"::text AS c_dec, \
                \"c_bool\"::text AS c_bool, \"c_date\"::text AS c_date, \"c_json\"::text AS c_json \
         FROM utrn_read WHERE entry_order = $1",
    )
    .bind(2i32)
    .fetch_one(&pool)
    .await
    .expect("text-bridge read of nulls");

    for name in ["c_text", "c_num", "c_dec", "c_bool", "c_date", "c_json"] {
        assert_eq!(
            row.get::<Option<String>, _>(name),
            None,
            "{} must come back NULL, not an empty value",
            name
        );
    }
}
