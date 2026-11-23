//! A WHERE column the select list leaves out never reaches the client, and
//! the decode-and-trim pass that removes it runs only when the scan says the
//! rows hold it. See `support/projection_probes.rs` for the shapes.
//!
//! Run on both storage kinds: the file-backed one reads rows through the
//! synchronous mmap copy, the in-memory one through the async read.

#[path = "support/raw_wire.rs"]
mod raw_wire;
#[path = "support/projection_probes.rs"]
mod projection_probes;

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::pgwire::PREPARED_RUNS;
use std::sync::atomic::Ordering;

/// The count of cached runs is global to the process.
static COUNTED: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn in_memory_projections_carry_only_the_requested_columns() {
    let _counted = COUNTED.lock().await;
    let server = TestServer::start().await.expect("server should start");
    let before = PREPARED_RUNS.load(Ordering::Relaxed);
    projection_probes::run_probes(server.port(), 5).await;
    // Parameter-less statements repeat the same text, so from the third
    // execution on they run from the cached parse; the probes must have
    // reached that path, not only the text one.
    assert!(
        PREPARED_RUNS.load(Ordering::Relaxed) - before >= projection_probes::probes().len() as u64,
        "the prepared probes never ran from a cached parse"
    );
}

#[tokio::test]
async fn file_backed_projections_carry_only_the_requested_columns() {
    let _counted = COUNTED.lock().await;
    let root = format!(
        "{}/projection_overfetch_{}",
        std::env::temp_dir().display(),
        std::process::id()
    );
    let _ = std::fs::remove_dir_all(&root);
    struct Remove(String);
    impl Drop for Remove {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Remove(root.clone());
    let server = TestServer::start_file_backed(&root).await.expect("start");
    projection_probes::run_probes(server.port(), 5).await;
}
