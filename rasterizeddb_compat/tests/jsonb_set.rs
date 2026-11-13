//! `jsonb_set` and `jsonb_strip_nulls`, against PostgreSQL 17's answers
//! (`docker run --rm postgres:17`). Both were "not supported yet". An
//! untouched member keeps its stored bytes, so its numbers keep every digit.

use rasterizeddb_compat::TestServer;
use tokio_postgres::{Client, SimpleQueryMessage};

async fn client(server: &TestServer) -> Client {
    let (client, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    client
}

/// Each row as `a|b|…`, a NULL as `NULL`.
async fn rows(c: &Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).unwrap_or("NULL").to_string()).collect::<Vec<_>>().join("|"))
            }
            _ => None,
        })
        .collect()
}

/// Replacing, adding (create_missing or not), array steps from either end
/// and past them, paths that run out, an empty path, a NULL value, a text[]
/// path, key order, nested nulls stripped from objects but not arrays.
#[tokio::test(flavor = "multi_thread")]
async fn jsonb_set_and_strip_nulls_answer_as_postgresql_does() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    for (sql, want) in [
        ("SELECT jsonb_set('{\"a\":1,\"b\":{\"c\":2}}', '{b,c}', '3')", "{\"a\": 1, \"b\": {\"c\": 3}}"),
        ("SELECT jsonb_set('{\"a\":1.10,\"b\":[1,2,3]}', '{b,1}', '\"x\"')", "{\"a\": 1.10, \"b\": [1, \"x\", 3]}"),
        ("SELECT jsonb_set('{\"a\":1}', '{z}', '{\"n\":1.500}')", "{\"a\": 1, \"z\": {\"n\": 1.500}}"),
        ("SELECT jsonb_set('{\"a\":1}', '{z}', '5', false)", "{\"a\": 1}"),
        ("SELECT jsonb_set('[1,2,3]', '{-1}', '9')", "[1, 2, 9]"),
        ("SELECT jsonb_set('[1,2,3]', '{10}', '9')", "[1, 2, 3, 9]"),
        ("SELECT jsonb_set('[1,2,3]', '{-10}', '9')", "[9, 1, 2, 3]"),
        ("SELECT jsonb_set('{\"a\":{\"b\":1}}', '{x,y}', '1')", "{\"a\": {\"b\": 1}}"),
        ("SELECT jsonb_set('{\"a\":[{\"k\":1}]}', '{a,0,k}', '2')", "{\"a\": [{\"k\": 2}]}"),
        ("SELECT jsonb_set('{\"a\":1}', '{}', '2')", "{\"a\": 1}"),
        ("SELECT jsonb_set('{\"a\":1}', '{a}', NULL)", "NULL"),
        ("SELECT jsonb_set('{\"a\":1}', ARRAY['a'], '\"v\"')", "{\"a\": \"v\"}"),
        ("SELECT jsonb_set('{\"b\":1, \"a\":2}', '{aa}', 'true')", "{\"a\": 2, \"b\": 1, \"aa\": true}"),
        ("SELECT jsonb_set('{\"a\":1}', '{a,b}', '1')", "{\"a\": 1}"),
        ("SELECT jsonb_strip_nulls('{\"a\":null,\"b\":{\"c\":null,\"d\":1},\"e\":[null,{\"f\":null}]}')", "{\"b\": {\"d\": 1}, \"e\": [null, {}]}"),
        ("SELECT jsonb_strip_nulls('[null,1]')", "[null, 1]"),
        ("SELECT jsonb_strip_nulls('null')", "null"),
        ("SELECT jsonb_set('{\"a\":1}', '{a}', '1e2')", "{\"a\": 100}"),
    ] {
        assert_eq!(rows(&c, sql).await, [want], "{sql}");
    }
    for (sql, needle) in [
        ("SELECT jsonb_set('5', '{a}', '1')", "cannot set path in scalar"),
        ("SELECT jsonb_set('[1]', '{a}', '1')", "path element at position 1 is not an integer"),
    ] {
        let e = c.simple_query(sql).await.expect_err(sql);
        let text = e.as_db_error().map(|d| d.message().to_string()).unwrap_or_else(|| e.to_string());
        assert!(text.contains(needle), "{sql}: {text}");
    }
}

/// On stored documents, in an UPDATE, a select list and a WHERE: the
/// untouched `10.50` and `3` keep their digits.
#[tokio::test(flavor = "multi_thread")]
async fn jsonb_set_rewrites_stored_documents() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute("CREATE TABLE jd (id INT PRIMARY KEY, doc JSONB); INSERT INTO jd VALUES (1, '{\"price\": 10.50, \"tags\": [\"a\"]}'), (2, '{\"price\": 3}'), (3, NULL)")
        .await
        .expect("setup");
    c.batch_execute("UPDATE jd SET doc = jsonb_set(doc, '{price}', '11.00') WHERE id = 1; UPDATE jd SET doc = jsonb_set(doc, '{tags}', '[\"b\"]') WHERE id = 2")
        .await
        .expect("update");
    assert_eq!(
        rows(&c, "SELECT id, doc, jsonb_set(doc, '{n}', to_jsonb(id)) FROM jd ORDER BY id").await,
        [
            "1|{\"tags\": [\"a\"], \"price\": 11.00}|{\"n\": 1, \"tags\": [\"a\"], \"price\": 11.00}",
            "2|{\"tags\": [\"b\"], \"price\": 3}|{\"n\": 2, \"tags\": [\"b\"], \"price\": 3}",
            "3|NULL|NULL",
        ]
    );
    assert_eq!(rows(&c, "SELECT id FROM jd WHERE jsonb_set(doc, '{price}', '0') ->> 'price' = '0' ORDER BY id").await, ["1", "2"]);
}
