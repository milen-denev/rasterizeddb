//! A JSON column in the SHREDDED mapping, cast to text.
//!
//! Shredded mode stores the column as a `UUID` pointing at its shadow table
//! and rebuilds the document when it is read. A cast of the column used to
//! format the rebuilt document's bytes as that storage type, so
//! `doc::text` of `{"a":1}` read `7b226122-3a31-7d`: a UUID's dashes over
//! the document's hex. Found by rasterizeddb-eb. PostgreSQL 17's answers.
//!
//! This whole file runs in shredded mode, a process-global set once, so it
//! is a file of its own (see `shredded_json_describe.rs`).

use rasterizeddb_compat::TestServer;
use sqlx::{Column, Executor, Row};

#[tokio::test(flavor = "multi_thread")]
async fn a_shredded_document_cast_to_text_is_its_text() {
    let _ = rasterizeddb_core::JSON_SHADOW_TABLES.set(true);
    assert!(rasterizeddb_core::json_shadow_tables(), "this file must run with the shredded JSON mapping");
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        r#"CREATE TABLE sj (id INT PRIMARY KEY, doc JSONB);
           INSERT INTO sj (id, doc) VALUES (1, '{"a":1}'), (2, '{"b":"x","c":[1,2]}'), (3, NULL);"#,
    ))
    .await
    .expect("schema");

    let mut wrong = Vec::new();
    for (sql, want_name, want) in [
        ("SELECT doc::text FROM sj ORDER BY id", "doc", vec![Some(r#"{"a": 1}"#), Some(r#"{"b": "x", "c": [1, 2]}"#), None]),
        ("SELECT CAST(doc AS TEXT) AS t FROM sj ORDER BY id", "t", vec![Some(r#"{"a": 1}"#), Some(r#"{"b": "x", "c": [1, 2]}"#), None]),
        ("SELECT doc::text AS t FROM sj WHERE id = 1", "t", vec![Some(r#"{"a": 1}"#)]),
    ] {
        for (protocol, rows) in [
            ("extended", sqlx::query(sqlx::AssertSqlSafe(sql)).fetch_all(&pool).await),
            ("simple", sqlx::raw_sql(sqlx::AssertSqlSafe(sql)).fetch_all(&pool).await),
        ] {
            let rows = match rows {
                Ok(rows) => rows,
                Err(e) => {
                    wrong.push(format!("{protocol} {sql}: refused: {e}"));
                    continue;
                }
            };
            let name = rows[0].columns()[0].name().to_string();
            if name != want_name {
                wrong.push(format!("{protocol} {sql}: column named {name:?}, want {want_name:?}"));
            }
            let got: Vec<Option<String>> = rows.iter().map(|r| r.try_get::<Option<String>, _>(0).unwrap_or_else(|e| Some(format!("<{e}>")))).collect();
            let want: Vec<Option<String>> = want.iter().map(|v| v.map(str::to_string)).collect();
            if got != want {
                wrong.push(format!("{protocol} {sql}\n    got  {got:?}\n    want {want:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
