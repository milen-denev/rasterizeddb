//! The `--settle-status-at-open` switch reaches the status log a database
//! opens with. Its own test binary: the switch is process-wide.

use rasterizeddb_core::configuration::Configuration;
use rasterizeddb_core::core::database::catalog::DatabaseCatalog;

fn config(settle: Option<bool>) -> Configuration {
    Configuration {
        location: None,
        batch_size: None,
        concurrent_threads: None,
        storage_locations: Vec::new(),
        storage_policy: None,
        storage_min_free: None,
        settle_status_at_open: settle,
    }
}

#[test]
fn the_configured_switch_reaches_the_status_log() {
    let _ = rasterizeddb_core::MAX_PERMITS_THREADS.get_or_init(|| 4);
    let _ = rasterizeddb_core::BATCH_SIZE.get_or_init(|| 1024);
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap().to_string();

    std::thread::Builder::new()
        .stack_size(64 << 20)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(32 << 20)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                // A first run with the default: some transactions, recorded.
                config(None).apply_runtime_switches();
                {
                    let catalog = DatabaseCatalog::open(Some(&root), "settle").await.unwrap();
                    let db = catalog.get("settle").unwrap();
                    assert!(db.transactions.status().settles(), "on by default");
                    for _ in 0..10_000 {
                        let tx = db.transactions.begin();
                        db.transactions.commit(tx);
                    }
                    db.transactions.flush().unwrap();
                }

                // Reopened with the default, which settles at open.
                {
                    let catalog = DatabaseCatalog::open(Some(&root), "settle").await.unwrap();
                    let db = catalog.get("settle").unwrap();
                    assert!(
                        db.transactions.status().settled_below() >= 8_192,
                        "the default did not settle at open"
                    );
                }

                // Explicitly on, as main.rs applies the flag.
                config(Some(true)).apply_runtime_switches();
                {
                    let catalog = DatabaseCatalog::open(Some(&root), "settle").await.unwrap();
                    let db = catalog.get("settle").unwrap();
                    assert!(
                        db.transactions.status().settled_below() >= 8_192,
                        "the switch did not reach StatusLog::open_with"
                    );
                }

                // And the kill switch: the sidecar is ignored, the file read whole.
                config(Some(false)).apply_runtime_switches();
                let catalog = DatabaseCatalog::open(Some(&root), "settle").await.unwrap();
                let db = catalog.get("settle").unwrap();
                assert!(!db.transactions.status().settles());
                assert_eq!(db.transactions.status().settled_below(), 0);
            });
            rt.shutdown_timeout(std::time::Duration::from_secs(2));
        })
        .unwrap()
        .join()
        .unwrap();
}
