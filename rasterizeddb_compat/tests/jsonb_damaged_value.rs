//! A stored `jsonb` document that cannot be decoded fails the statement.
//!
//! It was logged and sent as NULL, in text and binary alike: a damaged value
//! read back as an absent one and the SELECT succeeded. The value is damaged
//! here by writing bytes directly into the stored files (a string inside the
//! document made invalid UTF-8), and read back after a restart.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

struct Root(String);

impl Root {
    fn new() -> Self {
        let r = std::env::temp_dir().join(format!("jsonb_damaged_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&r);
        std::fs::create_dir_all(&r).expect("root");
        Root(r.to_string_lossy().into_owned())
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

const MARKER: &[u8] = b"DAMAGE-ME-DAMAGE-ME";

/// Every occurrence of `MARKER` under `dir`, each byte replaced by 0xFF.
fn damage(dir: &std::path::Path) -> usize {
    let mut found = 0;
    for entry in std::fs::read_dir(dir).expect("read dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            found += damage(&path);
            continue;
        }
        let Ok(mut bytes) = std::fs::read(&path) else { continue };
        let mut changed = false;
        let mut at = 0;
        while let Some(offset) = bytes[at..].windows(MARKER.len()).position(|w| w == MARKER) {
            let start = at + offset;
            bytes[start..start + MARKER.len()].fill(0xFF);
            at = start + MARKER.len();
            changed = true;
            found += 1;
        }
        if changed {
            std::fs::write(&path, &bytes).expect("write");
        }
    }
    found
}

#[tokio::test(flavor = "multi_thread")]
async fn a_damaged_jsonb_value_fails_the_statement_instead_of_reading_as_null() {
    let dir = Root::new();
    {
        let server = TestServer::start_file_backed(&dir.0).await.expect("server");
        let pool = server.connect_sqlx().await.expect("connect");
        pool.execute(sqlx::raw_sql(AssertSqlSafe(format!(
            r#"CREATE TABLE jd (id INT PRIMARY KEY, doc JSONB);
               INSERT INTO jd VALUES (1, '{{"ok": true}}'), (2, '{{"k": "{}"}}'), (3, NULL)"#,
            std::str::from_utf8(MARKER).unwrap()
        ))))
        .await
        .expect("setup");
    }
    assert!(damage(std::path::Path::new(&dir.0)) > 0, "the document's bytes were not found to damage");

    let server = TestServer::start_file_backed(&dir.0).await.expect("restart");
    let pool = server.connect_sqlx().await.expect("connect");
    // The rows around it still read as they are.
    let r = sqlx::query("SELECT doc::text FROM jd WHERE id = 1").fetch_one(&pool).await.expect("good row");
    assert_eq!(r.get::<String, _>(0), r#"{"ok": true}"#);
    let r = sqlx::query("SELECT doc FROM jd WHERE id = 3").fetch_one(&pool).await.expect("null row");
    assert!(r.get::<Option<serde_json::Value>, _>(0).is_none());

    // Text (simple protocol) and binary (extended) alike: an error, XX001.
    let text = pool.fetch_all(sqlx::raw_sql("SELECT id, doc FROM jd WHERE id = 2")).await;
    let binary = sqlx::query("SELECT id, doc FROM jd WHERE id = 2").fetch_all(&pool).await;
    for (protocol, result) in [("text", text), ("binary", binary)] {
        let e = result.expect_err(protocol);
        let code = e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string());
        assert_eq!(code.as_deref(), Some("XX001"), "{protocol}: {e}");
    }
    // The session is usable afterwards.
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM jd").fetch_one(&pool).await.expect("count");
    assert_eq!(n, 3);
}
