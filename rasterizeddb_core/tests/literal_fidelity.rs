//! What a statement stores has to be what the statement said.
//!
//! These are regressions for a family of faults this engine is prone to: a
//! write that reports success and stores something else. Each case below was
//! found by importing a real PostgreSQL database, where the values are not the
//! well-behaved ones a hand-written test tends to use.

use rasterizeddb_core::core::database::Database;
use rasterizeddb_core::core::row::row::{Row, vec_into_rows};
use rasterizeddb_core::core::rql::executor::execute;
use rasterizeddb_core::core::rql::lexer_s1::recognize_query_purpose;
use rclite::Arc;

async fn open(dir: &std::path::Path) -> Arc<Database> {
    Arc::new(Database::new_with_backend(dir.to_str().unwrap(), false).await)
}

#[track_caller]
fn expect_ok(response: &[u8], sql: &str) {
    if response.first() == Some(&3) {
        panic!("`{}` failed: {}", sql, String::from_utf8_lossy(&response[1..]));
    }
}

async fn run(database: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("unrecognised: {}", sql));
    execute(purpose, database.clone()).await
}

async fn rows(database: &Arc<Database>, sql: &str) -> Vec<Row> {
    let response = run(database, sql).await;
    expect_ok(&response, sql);
    vec_into_rows(&response[1..]).expect("a decodable result set")
}

fn text(row: &Row, column: usize) -> String {
    String::from_utf8_lossy(row.columns[column].data.into_slice()).to_string()
}

/// The elements of an array value, which is stored in PostgreSQL's binary
/// array representation.
fn elements(row: &Row, column: usize) -> Vec<Option<String>> {
    let raw = row.columns[column].data.into_slice();
    let count = i32::from_be_bytes(raw[12..16].try_into().unwrap()) as usize;
    let mut at = 20;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let length = i32::from_be_bytes(raw[at..at + 4].try_into().unwrap());
        at += 4;
        if length < 0 {
            out.push(None);
            continue;
        }
        let length = length as usize;
        out.push(Some(String::from_utf8_lossy(&raw[at..at + length]).to_string()));
        at += length;
    }
    out
}

/// A quote inside a value is written doubled, and every type has to collapse it
/// back to one.
///
/// `STRING` did. `ARRAY` and `BYTEA` did not, so `{"has 'quote'"}` was stored as
/// `{"has ''quote''"}` — an element with two quotes where the source had one,
/// with the statement reporting success. It was found by importing a
/// PostgreSQL table whose text array held an apostrophe.
#[tokio::test]
async fn a_doubled_quote_is_collapsed_for_every_type() {
    let dir = tempfile::tempdir().unwrap();
    let database = open(dir.path()).await;

    let sql = "CREATE TABLE t (id INT, s TEXT, tags TEXT[], blob BYTEA)";
    expect_ok(&run(&database, sql).await, sql);

    let sql = "INSERT INTO t (id, s, tags, blob) VALUES \
               (1, 'has ''quote''', '{\"has ''quote''\",plain}', '\\x2727')";
    expect_ok(&run(&database, sql).await, sql);

    let inserted = rows(&database, "SELECT s, tags, blob FROM t WHERE id = 1").await;
    assert_eq!(inserted.len(), 1);
    assert_eq!(text(&inserted[0], 0), "has 'quote'");
    assert_eq!(
        elements(&inserted[0], 1),
        vec![Some("has 'quote'".to_string()), Some("plain".to_string())]
    );
    assert_eq!(text(&inserted[0], 2), "''");

    // The UPDATE path is a separate encoder and drifted from the INSERT one
    // before; the pair is tested together for that reason.
    let sql = "UPDATE t SET tags = '{\"upd ''q''\"}', s = 'upd ''q''' WHERE id = 1";
    expect_ok(&run(&database, sql).await, sql);

    let updated = rows(&database, "SELECT s, tags FROM t WHERE id = 1").await;
    assert_eq!(text(&updated[0], 0), "upd 'q'");
    assert_eq!(elements(&updated[0], 1), vec![Some("upd 'q'".to_string())]);
}

/// Non-ASCII text used to be re-encoded on its way through the comment
/// stripper, which walked bytes and pushed each one as a `char`. Every
/// statement carrying an accent or an emoji stored mojibake, and nothing
/// reported anything.
#[tokio::test]
async fn non_ascii_text_survives_a_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let database = open(dir.path()).await;

    let sql = "CREATE TABLE t (id INT, s TEXT)";
    expect_ok(&run(&database, sql).await, sql);

    let values = [
        "café",
        "ünïcødé",
        "日本語",
        "😀 emoji",
        "10 € — naïve",
        "Ω≈ç√∫",
    ];
    for (index, value) in values.iter().enumerate() {
        let sql = format!("INSERT INTO t (id, s) VALUES ({}, '{}')", index, value);
        expect_ok(&run(&database, &sql).await, &sql);
    }

    for (index, value) in values.iter().enumerate() {
        let sql = format!("SELECT s FROM t WHERE id = {}", index);
        let found = rows(&database, &sql).await;
        assert_eq!(found.len(), 1, "{}", sql);
        assert_eq!(text(&found[0], 0), *value);
    }
}

/// The shapes that have broken this engine's statement parser before, each
/// carried as ordinary data.
#[tokio::test]
async fn punctuation_inside_a_value_is_data() {
    let dir = tempfile::tempdir().unwrap();
    let database = open(dir.path()).await;

    let sql = "CREATE TABLE t (id INT, s TEXT)";
    expect_ok(&run(&database, sql).await, sql);

    let values = [
        "a),b(",
        "-- not a comment",
        "/* not a comment */",
        "back\\slash",
        "semi;colon",
        "comma,separated",
        "double\"quote",
        "new\nline",
        "tab\there",
        "",
    ];
    for (index, value) in values.iter().enumerate() {
        let sql = format!("INSERT INTO t (id, s) VALUES ({}, '{}')", index, value);
        expect_ok(&run(&database, &sql).await, &sql);
    }

    for (index, value) in values.iter().enumerate() {
        let found = rows(&database, &format!("SELECT s FROM t WHERE id = {}", index)).await;
        assert_eq!(found.len(), 1, "row {} went missing", index);
        assert_eq!(text(&found[0], 0), *value);
    }
}
