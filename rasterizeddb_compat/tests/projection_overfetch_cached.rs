//! `projection_overfetch.rs` with the query cache ON. A repeated predicate is
//! answered from the cached pointer set, which fetches only the requested
//! columns, so the scan's "nothing to trim" answer has to hold there too.
//!
//! `ENABLE_CACHE` is a process-global set once, so this lives in its own file.

#[path = "support/raw_wire.rs"]
mod raw_wire;
#[path = "support/projection_probes.rs"]
mod projection_probes;

use rasterizeddb_compat::TestServer;

fn cache_on() {
    // Must win the race against the harness's `init_globals`, which sets it
    // false; both use the same `OnceLock`, so first writer wins.
    let _ = rasterizeddb_core::cache::query_cache::ENABLE_CACHE.set(true);
    rasterizeddb_core::cache::query_cache::init(10_000);
}

#[tokio::test]
async fn cached_answers_carry_only_the_requested_columns() {
    cache_on();
    assert!(rasterizeddb_core::cache::query_cache::enabled(), "the cache did not turn on");
    let server = TestServer::start().await.expect("server should start");
    let (hits_before, ..) = rasterizeddb_core::cache::query_cache::stats();
    projection_probes::run_probes(server.port(), 5).await;
    let (hits_after, ..) = rasterizeddb_core::cache::query_cache::stats();
    assert!(
        hits_after - hits_before >= projection_probes::probes().len() as u64,
        "the probes were never answered from the cache ({} hits)",
        hits_after - hits_before
    );
}
