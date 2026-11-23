//! `SET CONFIG` on a read-once key, in a process that never gave the Builder
//! one: saved in the database directory, applied by the next start there. A
//! hot key is applied at once and saved the same way.
//! A process of its own, since a read-once value is set once a process.

use rasterizeddb_embedded::client::Value;
use rasterizeddb_embedded::{Builder, Client, engine};

fn builder(b: Builder) -> Builder {
    if cfg!(unix) { b } else { b.password("app", "embedded-test-password") }
}

fn text(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        other => panic!("not text: {other:?}"),
    }
}

/// `(value, pending, source)` of one `SHOW CONFIG` key.
async fn key(client: &Client, wanted: &str) -> (String, String, String) {
    let rows = client.query("SHOW CONFIG", &[]).await.unwrap();
    rows.iter()
        .find_map(|row| {
            let v = row.values().unwrap();
            (format!("{}.{}", text(&v[0]), text(&v[1])) == wanted).then(|| (text(&v[2]), text(&v[6]), text(&v[8])))
        })
        .unwrap_or_else(|| panic!("no {wanted}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_saved_startup_key_applies_at_the_next_start() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = builder(Builder::open(dir.path())).worker_threads(2).start().await.unwrap();
        let client = db.connect().await.unwrap();
        assert_eq!(key(&client, "server.batch_size").await, ("10000".into(), String::new(), "default".into()));
        for statement in [
            "SET CONFIG server.batch_size = 4321",
            "SET CONFIG server.concurrent_threads = 6",
            "SET CONFIG wal.checkpoint_bytes = 1048576",
            "SET CONFIG cache.enabled = true",
            "SET CONFIG index.residency = hybrid",
            "SET CONFIG cache.size = 5000",
            // A hot key: applied now, and saved for every start after.
            "SET CONFIG partition.idle_close_secs = 600",
        ] {
            client.query(statement, &[]).await.unwrap_or_else(|e| panic!("{statement}: {e}"));
        }
        // Saved, not applied: the running engine keeps what it started with.
        assert_eq!(key(&client, "server.batch_size").await, ("10000".into(), "4321".into(), "default".into()));
        assert_eq!(engine::batch_size(), 10_000);
        assert_eq!(key(&client, "partition.idle_close_secs").await, ("600".into(), String::new(), "saved".into()));
        let saved = std::fs::read_to_string(dir.path().join("rasterizeddb.conf")).unwrap();
        assert!(saved.contains("partition.idle_close_secs = 600"), "{saved}");
        drop(client);
        db.shutdown().await.unwrap();
    }
    {
        let db = builder(Builder::open(dir.path())).worker_threads(2).start().await.unwrap();
        let client = db.connect().await.unwrap();
        assert_eq!(key(&client, "server.batch_size").await, ("4321".into(), String::new(), "saved".into()));
        assert_eq!(engine::batch_size(), 4321);
        assert_eq!(key(&client, "server.concurrent_threads").await.0, "6");
        assert_eq!(engine::max_permits_threads(), 6);
        assert_eq!(key(&client, "wal.checkpoint_bytes").await, ("1048576".into(), String::new(), "saved".into()));
        assert_eq!(key(&client, "cache.enabled").await, ("true".into(), String::new(), "saved".into()));
        assert_eq!(key(&client, "index.residency").await, ("hybrid".into(), String::new(), "saved".into()));
        // The cache was built at the saved size.
        assert_eq!(key(&client, "cache.size").await, ("5000".into(), String::new(), "saved".into()));
        assert_eq!(key(&client, "partition.idle_close_secs").await, ("600".into(), String::new(), "saved".into()));
        // The engine works with them: a repeated query, through the cache.
        client.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT)", &[]).await.unwrap();
        for i in 0..50i64 {
            client.execute(&format!("INSERT INTO t VALUES ({i}, 'row {i}')"), &[]).await.unwrap();
        }
        for _ in 0..3 {
            let rows = client.query("SELECT body FROM t WHERE id = 31", &[]).await.unwrap();
            assert_eq!(rows.iter().next().unwrap().get::<String>(0).unwrap(), "row 31");
        }
        drop(client);
        db.shutdown().await.unwrap();
    }
    // A Builder value wins over the saved one, and says so.
    {
        let db = builder(Builder::open(dir.path()))
            .worker_threads(2)
            .index_residency(rasterizeddb_embedded::IndexResidency::Memory)
            .config("partition.idle_close_secs", "120")
            .start()
            .await
            .unwrap();
        let client = db.connect().await.unwrap();
        assert_eq!(key(&client, "index.residency").await, ("memory".into(), "hybrid".into(), "command line".into()));
        assert_eq!(key(&client, "partition.idle_close_secs").await, ("120".into(), String::new(), "command line".into()));
        let rows = client.query("SELECT body FROM t WHERE id = 31", &[]).await.unwrap();
        assert_eq!(rows.iter().next().unwrap().get::<String>(0).unwrap(), "row 31");
        drop(client);
        db.shutdown().await.unwrap();
    }
}
