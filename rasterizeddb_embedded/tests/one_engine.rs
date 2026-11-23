//! What an embedding process can rely on about the engine's place in it: one
//! engine at a time, its directory locked against every other opener, the
//! host's signals left alone, and nothing of a stopped engine left behind.
//!
//! One test function in ordered phases: each phase depends on the process's
//! state the one before left, which parallel test functions would race on.

use std::io::ErrorKind;

use rasterizeddb_embedded::Builder;
use rasterizeddb_embedded::engine;

fn builder(b: Builder) -> Builder {
    if cfg!(unix) { b } else { b.password("app", "embedded-test-password") }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_embedding_process_has_one_engine_and_keeps_its_signals() {
    let dir = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();

    // The engine's own hooks are on until an embedded start turns them off.
    assert!(engine::shutdown::signal_hooks());
    let db = builder(Builder::open(dir.path())).worker_threads(2).start().await.unwrap();
    assert!(
        !engine::shutdown::signal_hooks(),
        "an embedded engine left the termination hooks on: REPLICATION ENABLE would take the host's Ctrl+C"
    );
    let client = db.connect().await.unwrap();
    client.execute("CREATE TABLE kept (id BIGINT)", &[]).await.unwrap();
    client.execute("INSERT INTO kept VALUES (1), (2)", &[]).await.unwrap();

    // A second engine alongside the first, on any directory, is refused and
    // leaves the first serving.
    let refused = builder(Builder::open(other.path())).start().await.err().expect("a second engine started");
    assert_eq!(refused.kind(), ErrorKind::AlreadyExists, "{refused}");
    let refused = builder(Builder::in_memory()).start().await.err().expect("a second engine started");
    assert_eq!(refused.kind(), ErrorKind::AlreadyExists, "{refused}");
    let rows = client.query("SELECT COUNT(*) FROM kept", &[]).await.unwrap();
    assert_eq!(rows.iter().next().unwrap().get::<i64>(0).unwrap(), 2);

    // Anything else opening the directory, in this process or another (the
    // lock is the operating system's, per open file), is refused before it
    // reads a byte, rather than replaying the running engine's log.
    let opened = engine::core::database::catalog::DatabaseCatalog::open(
        Some(dir.path().to_str().unwrap()),
        engine::core::database::DEFAULT_DATABASE_NAME,
    )
    .await;
    let refused = match opened {
        Err(e) => e,
        Ok(_) => panic!("a catalogue opened a directory an engine is serving"),
    };
    assert!(refused.contains("already open"), "{refused}");

    // Neither the password nor anything secret shows in a Debug print.
    let shown = format!("{:?}", Builder::open(dir.path()).password("app", "hunter2-secret"));
    assert!(!shown.contains("hunter2-secret"), "{shown}");
    assert!(shown.contains("<hidden>"), "{shown}");
    assert!(!format!("{db:?}").is_empty());

    drop(client);
    db.shutdown().await.unwrap();

    // Stopped, it leaves no log open behind it, and the next start, on the
    // same directory or another, is free to run.
    assert!(
        engine::core::wal::registered().is_empty(),
        "a stopped engine left logs registered: {:?}",
        engine::core::wal::registered().iter().map(|(d, _)| d).collect::<Vec<_>>()
    );
    let db = builder(Builder::open(dir.path())).worker_threads(2).start().await.unwrap();
    let client = db.connect().await.unwrap();
    let rows = client.query("SELECT COUNT(*) FROM kept", &[]).await.unwrap();
    assert_eq!(rows.iter().next().unwrap().get::<i64>(0).unwrap(), 2);
    drop(client);
    db.shutdown().await.unwrap();

    let db = builder(Builder::open(other.path())).worker_threads(2).start().await.unwrap();
    db.shutdown().await.unwrap();
}
