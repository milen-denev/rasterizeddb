//! The Builder's settings reach the engine: typed methods, `config`, the
//! settings file, and the refusals. One test, because the settings are the
//! process's: its phases are the starts of one process, in order.

use std::collections::BTreeMap;
use std::time::Duration;

use rasterizeddb_embedded::client::Value;
use rasterizeddb_embedded::{Builder, Client, IndexResidency, KvEviction, engine};

fn builder(b: Builder) -> Builder {
    if cfg!(unix) { b } else { b.password("app", "embedded-test-password") }
}

fn text(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        other => panic!("not text: {other:?}"),
    }
}

/// `SHOW CONFIG` as `section.key -> (value, pending, source)`.
async fn show_config(client: &Client) -> BTreeMap<String, (String, String, String)> {
    let rows = client.query("SHOW CONFIG", &[]).await.unwrap();
    rows.iter()
        .map(|row| {
            let v = row.values().unwrap();
            (
                format!("{}.{}", text(&v[0]), text(&v[1])),
                (text(&v[2]), text(&v[6]), text(&v[8])),
            )
        })
        .collect()
}

async fn one_text(client: &Client, sql: &str) -> Result<String, String> {
    let rows = client.query(sql, &[]).await.map_err(|e| e.to_string())?;
    Ok(text(&rows.iter().next().expect("one row").values().unwrap()[0]))
}

async fn count(client: &Client, sql: &str) -> i64 {
    let rows = client.query(sql, &[]).await.unwrap();
    rows.iter().next().unwrap().get::<i64>(0).unwrap()
}

fn start_error(b: Builder) -> std::io::Error {
    match b.start_blocking() {
        Ok(db) => {
            db.shutdown_blocking().unwrap();
            panic!("the start was not refused");
        }
        Err(e) => e,
    }
}

#[test]
fn settings_reach_the_engine() {
    let dir = tempfile::tempdir().unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    // Phase 1: every kind of setting, given to the Builder.
    let db = builder(Builder::open(dir.path()))
        .worker_threads(2)
        .query_threads(3)
        .batch_size(5000)
        .malloc_arenas(4)
        .index_residency(IndexResidency::Disk)
        .index_cache_mb(32)
        .join_memory_mb(128)
        .wal_checkpoint_bytes(8 * 1024 * 1024)
        .wal_prealloc_kb(512)
        .synchronous_commit(false)
        .wal_writer_delay(Duration::from_millis(50))
        .auto_compact(false)
        .partition_open_budget(32)
        .kv_max_memory(1 << 20)
        .kv_eviction(KvEviction::AllKeysLru)
        .config("filter.max_f", "9")
        // The last word on a key wins, whichever method said it.
        .config("FILTER.max_f", "14")
        .config("auto_compact.interval_secs", "300")
        .config("vacuum.enabled", "off")
        .start_blocking()
        .unwrap();
    rt.block_on(async {
        let client = db.connect().await.unwrap();
        let config = show_config(&client).await;
        let value = |key: &str| config.get(key).unwrap_or_else(|| panic!("no {key}")).0.clone();
        let source = |key: &str| config[key].2.clone();

        assert_eq!(value("server.concurrent_threads"), "3");
        assert_eq!(source("server.concurrent_threads"), "command line");
        assert_eq!(engine::max_permits_threads(), 3);
        assert_eq!(value("server.batch_size"), "5000");
        assert_eq!(engine::batch_size(), 5000);
        if cfg!(all(target_os = "linux", target_env = "gnu")) {
            assert_eq!(value("server.malloc_arenas"), "4");
            assert_eq!(source("server.malloc_arenas"), "command line");
        } else {
            // No such knob: the engine starts without it and says so.
            assert_eq!(value("server.malloc_arenas"), "0");
            assert_eq!(source("server.malloc_arenas"), "default");
        }
        assert_eq!(value("index.residency"), "disk");
        assert_eq!(value("index.cache_mb"), "32");
        assert_eq!(value("index.delta_mb"), "0");
        assert_eq!(source("index.delta_mb"), "default");
        assert_eq!(value("wal.checkpoint_bytes"), (8 * 1024 * 1024).to_string());
        assert_eq!(value("wal.prealloc_kb"), "512");
        assert_eq!(value("wal.synchronous_commit"), "off");
        assert_eq!(value("wal.writer_delay_ms"), "50");
        assert_eq!(value("auto_compact.enabled"), "off");
        assert_eq!(value("auto_compact.interval_secs"), "300");
        assert_eq!(value("partition.open_budget"), "32");
        assert_eq!(value("filter.max_f"), "14");
        assert_eq!(value("cache.enabled"), "false");
        assert_eq!(engine::core::rql::join::join_memory_mb(), 128);
        assert_eq!(engine::core::kv_cache::max_memory(), 1 << 20);
        assert_eq!(engine::core::kv_cache::eviction_policy(), KvEviction::AllKeysLru);

        // The engine answers correctly under them: an index on disk, a join
        // under the join budget.
        client
            .execute("CREATE TABLE items (id BIGINT PRIMARY KEY, grp BIGINT, body TEXT)", &[])
            .await
            .unwrap();
        client.execute("CREATE TABLE groups (grp BIGINT PRIMARY KEY, name TEXT)", &[]).await.unwrap();
        for g in 0..4i64 {
            client.execute(&format!("INSERT INTO groups VALUES ({g}, 'g{g}')"), &[]).await.unwrap();
        }
        for i in 0..200i64 {
            client
                .execute(&format!("INSERT INTO items VALUES ({i}, {}, 'item {i}')", i % 4), &[])
                .await
                .unwrap();
        }
        assert_eq!(count(&client, "SELECT COUNT(*) FROM items").await, 200);
        assert_eq!(one_text(&client, "SELECT body FROM items WHERE id = 137").await.unwrap(), "item 137");
        assert_eq!(
            count(
                &client,
                "SELECT COUNT(*) FROM items i JOIN groups g ON g.grp = i.grp WHERE g.name = 'g3'"
            )
            .await,
            50
        );

        // A hot key still changes on the running engine.
        one_text(&client, "SET CONFIG wal.synchronous_commit = on").await.unwrap();
        assert_eq!(show_config(&client).await["wal.synchronous_commit"].0, "on");

        // A startup key is saved for the next start; one the Builder set
        // says the Builder's value will win.
        let reply = one_text(&client, "SET CONFIG server.batch_size = 6000").await.unwrap();
        assert!(reply.contains("saved") && reply.contains("flag"), "{reply}");
        let reply = one_text(&client, "SET CONFIG index.delta_mb = 8").await.unwrap();
        assert!(reply.contains("saved") && !reply.contains("flag"), "{reply}");
        let config = show_config(&client).await;
        assert_eq!(config["index.delta_mb"].1, "8", "pending");
        assert_eq!(config["server.batch_size"].1, "6000", "pending");
        assert!(dir.path().join("rasterizeddb.conf").exists());
        drop(client);
    });
    db.shutdown_blocking().unwrap();

    // Phase 2: the same directory with nothing given. The saved values
    // apply where they still can; the batch size is read once and this
    // process already runs with 5000, so it stays, and stays pending.
    let db = builder(Builder::open(dir.path())).worker_threads(2).start_blocking().unwrap();
    let startup_keys: Vec<(String, String)> = rt.block_on(async {
        let client = db.connect().await.unwrap();
        let config = show_config(&client).await;
        assert_eq!(config["index.delta_mb"], ("8".to_string(), String::new(), "saved".to_string()));
        assert_eq!(config["server.batch_size"].0, "5000");
        assert_eq!(config["server.batch_size"].1, "6000");
        assert_eq!(config["server.batch_size"].2, "default");
        assert_eq!(engine::batch_size(), 5000);
        // Hot keys are the process's too: they stand as phase 1 left them.
        assert_eq!(config["auto_compact.enabled"].0, "off");
        // What phase 1 wrote, read back through an index reopened on disk.
        assert_eq!(count(&client, "SELECT COUNT(*) FROM items").await, 200);
        assert_eq!(one_text(&client, "SELECT body FROM items WHERE id = 137").await.unwrap(), "item 137");

        // Every key SHOW CONFIG KEYS marks read-once, with its running value.
        let keys = client.query("SHOW CONFIG KEYS", &[]).await.unwrap();
        let startup = keys
            .iter()
            .filter_map(|row| {
                let v = row.values().unwrap();
                let key = format!("{}.{}", text(&v[0]), text(&v[1]));
                (v[3] == Value::Bool(false)).then(|| (key.clone(), config[&key].0.clone()))
            })
            .collect::<Vec<_>>();
        drop(client);
        startup
    });
    db.shutdown_blocking().unwrap();

    // Phase 3: the Builder has a setter for every read-once key the engine
    // lists (a key added to the engine and not here fails this), and giving
    // a key the value it already runs with is no conflict.
    assert!(startup_keys.len() >= 9, "{startup_keys:?}");
    for (key, running) in &startup_keys {
        let b = builder(Builder::in_memory()).worker_threads(2).config(key.clone(), running.clone());
        if key == "wal.enabled" {
            let e = start_error(b);
            assert!(e.to_string().contains("wal.enabled"), "{e}");
            continue;
        }
        let db = b.start_blocking().unwrap_or_else(|e| panic!("{key} = {running}: {e}"));
        db.shutdown_blocking().unwrap();
    }

    // Phase 4: the refusals. A read-once value this process cannot take any
    // more, an unknown key, a value the key refuses.
    let refused = |b: Builder, needle: &str| {
        let e = start_error(builder(b).worker_threads(2));
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput, "{e}");
        assert!(e.to_string().contains(needle), "{e}");
    };
    refused(Builder::in_memory().batch_size(7000), "server.batch_size = 7000 cannot take effect");
    refused(Builder::in_memory().query_threads(5), "server.concurrent_threads");
    refused(Builder::in_memory().join_memory_mb(64), "join_memory_mb");
    refused(Builder::in_memory().join_memory_mb(0), "join_memory_mb");
    refused(Builder::in_memory().config("server.no_such_key", "1"), "unknown config key");
    refused(Builder::in_memory().config("nodot", "1"), "unknown config key");
    refused(Builder::in_memory().batch_size(0), "server.batch_size");
    refused(Builder::in_memory().config("index.residency", "tape"), "residency");
    refused(Builder::in_memory().config("filter.max_f", "banana"), "max_f");
    refused(Builder::in_memory().kv_save_interval(Duration::from_millis(10)), "kv_save_interval");
    refused(Builder::in_memory().query_cache_entries(0), "query_cache_entries");

    // An engine in memory has no settings file, and does not write the one
    // of the directory this process opened before.
    let before = std::fs::read_to_string(dir.path().join("rasterizeddb.conf")).unwrap();
    let db = builder(Builder::in_memory()).worker_threads(2).start_blocking().unwrap();
    rt.block_on(async {
        let client = db.connect().await.unwrap();
        let e = one_text(&client, "SET CONFIG index.cache_mb = 99").await.unwrap_err();
        assert!(e.contains("no data directory"), "{e}");
        drop(client);
    });
    db.shutdown_blocking().unwrap();
    assert_eq!(std::fs::read_to_string(dir.path().join("rasterizeddb.conf")).unwrap(), before);
}
