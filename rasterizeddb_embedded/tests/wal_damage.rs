//! A log damaged in its middle refuses the start, and
//! `Builder::wal_truncate_damaged` starts past it, for that start only. One
//! test, because the switch is the process's: its phases are the starts of
//! one process, in order.

use std::path::Path;

use rasterizeddb_embedded::{Builder, engine};

fn builder(b: Builder) -> Builder {
    if cfg!(unix) { b } else { b.password("app", "embedded-test-password") }
}

/// A log in `dir` of three committed four-byte writes to `t.db`, the first
/// one's payload overwritten with 0xFF: damage with whole records after it.
fn damaged_log(dir: &Path) {
    use engine::core::wal::{DEFAULT_WAL_NAME, Wal, file_id_for_name};
    let path = dir.join(DEFAULT_WAL_NAME);
    let id = file_id_for_name("t.db");
    {
        let wal = Wal::open(&path).unwrap();
        for i in 0..3u32 {
            wal.log_and_commit(id, 4 * i as u64, &(i + 1).to_le_bytes()).unwrap();
        }
    }
    // Record layout: a 40-byte header, then the payload.
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[40..44].fill(0xFF);
    std::fs::write(&path, &bytes).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_damaged_log_refuses_and_the_builder_option_starts_past_it_once() {
    let dir = tempfile::tempdir().unwrap();
    damaged_log(dir.path());

    let refused = builder(Builder::open(dir.path())).worker_threads(2).start().await.err().expect("a damaged log started");
    let msg = refused.to_string();
    assert!(msg.contains("damaged at byte 0:"), "{msg}");
    assert!(msg.contains("--wal-truncate-damaged"), "{msg}");

    let db = builder(Builder::open(dir.path()))
        .worker_threads(2)
        .wal_truncate_damaged(true)
        .start()
        .await
        .expect("the option did not start it");
    let client = db.connect().await.unwrap();
    client.query("SELECT 1", &[]).await.expect("the started database does not answer");
    drop(client);
    db.shutdown().await.unwrap();

    // The next start without it refuses again: the option was that start's.
    let again = tempfile::tempdir().unwrap();
    damaged_log(again.path());
    let refused = builder(Builder::open(again.path())).worker_threads(2).start().await.err().expect("the option outlived its start");
    assert!(refused.to_string().contains("damaged at byte 0:"), "{refused}");
}
