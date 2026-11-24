//! The low-memory preset and the settings that are not `SHOW CONFIG` keys,
//! on an engine that then answers correctly and reopens. A process of its
//! own: the settings are the process's.

use std::time::Duration;

use rasterizeddb_embedded::client::Value;
use rasterizeddb_embedded::{Builder, Client, IndexResidency, IndexStorage, SimdLevel, engine};

fn builder(b: Builder) -> Builder {
    if cfg!(unix) { b } else { b.password("app", "embedded-test-password") }
}

async fn config(client: &Client, wanted: &str) -> String {
    let rows = client.query("SHOW CONFIG", &[]).await.unwrap();
    rows.iter()
        .find_map(|row| match &row.values().unwrap()[..3] {
            [Value::Text(section), Value::Text(key), Value::Text(value)] if format!("{section}.{key}") == wanted => {
                Some(value.clone())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("no {wanted}"))
}

async fn count(client: &Client, sql: &str) -> i64 {
    let rows = client.query(sql, &[]).await.unwrap();
    rows.iter().next().unwrap().get::<i64>(0).unwrap()
}

fn settings(b: Builder) -> Builder {
    builder(b)
        .worker_threads(2)
        .low_memory()
        // A call after the preset overrides its choice for that setting.
        .join_memory_mb(300)
        .tune_allocator(true)
        .idle_trim(false)
        .simd(SimdLevel::Scalar)
        .scan_prefilter(false)
        .json_shadow_tables(false)
        .settle_status(true)
        .raise_descriptor_limit(true)
        .query_cache(true)
        .query_cache_entries(1000)
        .query_stats(false)
        .index_storage(IndexStorage::Snapshot)
        .kv_persist(true)
        .kv_save_interval(Duration::from_secs(5))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_preset_and_the_switches_reach_the_engine() {
    // A level this CPU cannot run refuses the start; it does not fall back.
    if !engine::simds::cpu::runnable_levels().contains(&SimdLevel::Avx512) {
        let e = match builder(Builder::in_memory()).simd(SimdLevel::Avx512).start().await {
            Ok(_) => panic!("AVX-512 on a CPU without it was accepted"),
            Err(e) => e,
        };
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput, "{e}");
    }

    let dir = tempfile::tempdir().unwrap();
    {
        let db = settings(Builder::open(dir.path())).start().await.unwrap();
        let client = db.connect().await.unwrap();

        if cfg!(all(target_os = "linux", target_env = "gnu")) {
            assert_eq!(config(&client, "server.malloc_arenas").await, "4");
        }
        assert_eq!(config(&client, "index.residency").await, "disk");
        assert_eq!(engine::core::index::paged::residency(), IndexResidency::Disk);
        assert_eq!(engine::core::rql::join::join_memory_mb(), 300);
        assert_eq!(engine::simds::cpu::level(), SimdLevel::Scalar);
        assert!(!engine::core::processor::prefilter::enabled());
        assert_eq!(config(&client, "cache.enabled").await, "true");
        assert_eq!(config(&client, "query_stats.enabled").await, "off");
        assert_eq!(config(&client, "index_snapshot.index_storage").await, "snapshot");
        assert!(engine::core::kv_cache::persist());
        assert_eq!(engine::core::kv_cache::save_interval_secs(), 5);
        // Raised, or already there: at least the floor either way.
        assert!(engine::core::partition::budget::process_figures().0 >= 16);

        client
            .execute("CREATE TABLE events (id BIGINT PRIMARY KEY, kind TEXT, n BIGINT)", &[])
            .await
            .unwrap();
        client.execute("CREATE INDEX events_kind ON events (kind)", &[]).await.unwrap();
        for i in 0..300i64 {
            client
                .execute(&format!("INSERT INTO events VALUES ({i}, 'k{}', {})", i % 5, i * 2), &[])
                .await
                .unwrap();
        }
        // Scalar kernels, no prefilter, the query cache on, indexes on disk.
        for _ in 0..2 {
            assert_eq!(count(&client, "SELECT COUNT(*) FROM events WHERE kind = 'k3'").await, 60);
            assert_eq!(count(&client, "SELECT n FROM events WHERE id = 211").await, 422);
            assert_eq!(count(&client, "SELECT SUM(n) FROM events WHERE n >= 500").await, (250..300).map(|i| i * 2).sum::<i64>());
        }
        client.kv().set(b"greeting", b"hello", None).await.unwrap();
        assert_eq!(client.kv().get(b"greeting").await.unwrap().as_deref(), Some(&b"hello"[..]));

        // `idle_trim(false)`: quiet for longer than a trim takes to come.
        tokio::time::sleep(engine::idle_trim::TICK * 2 + Duration::from_secs(1)).await;
        assert_eq!(engine::idle_trim::trims(), 0);

        drop(client);
        db.shutdown().await.unwrap();
    }
    // The same settings again are no conflict, and the directory reopens:
    // the indexes from their snapshot, the key/value store from its save.
    {
        let db = settings(Builder::open(dir.path())).start().await.unwrap();
        let client = db.connect().await.unwrap();
        assert_eq!(count(&client, "SELECT COUNT(*) FROM events").await, 300);
        assert_eq!(count(&client, "SELECT COUNT(*) FROM events WHERE kind = 'k3'").await, 60);
        assert_eq!(count(&client, "SELECT n FROM events WHERE id = 211").await, 422);
        assert_eq!(client.kv().get(b"greeting").await.unwrap().as_deref(), Some(&b"hello"[..]));
        drop(client);
        db.shutdown().await.unwrap();
    }
}
