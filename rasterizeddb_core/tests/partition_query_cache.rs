//! The query cache across partitions (`docs/partitioning.md` §8): each
//! partition caches its share under its own scope, and only a statement
//! routed to at most four partitions reads or fills the cache, so a wide
//! fan-out cannot flush it.
//!
//! The cache and its counters are process-wide, so this file is its own test
//! binary with the cache on and one test in it.

mod partition_wire;

use partition_wire::*;
use rasterizeddb_core::cache::query_cache;

#[test]
fn a_narrow_route_uses_the_cache_and_a_wide_one_stores_nothing() {
    query_cache::init(10_000);
    assert!(*query_cache::ENABLE_CACHE.get_or_init(|| true), "something turned the cache off first");
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    c.ok(
        "CREATE TABLE qc (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, n INT, PRIMARY KEY (tenant_id, id)) \
         PARTITION BY HASH (tenant_id) PARTITIONS 16",
    );
    c.ok("CREATE TABLE qc_plain (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, n INT, PRIMARY KEY (tenant_id, id))");
    let rows: Vec<String> = (1..=24i64).flat_map(|t| (1..=4i64).map(move |i| format!("({t}, {i}, {})", t * i))).collect();
    for t in ["qc", "qc_plain"] {
        c.ok(&format!("INSERT INTO {t} (tenant_id, id, n) VALUES {}", rows.join(", ")));
    }
    let pt = server.db.partitioned_table("qc").unwrap();
    let epoch = pt.epoch().unwrap();
    let materialized = pt.materialized_pids().unwrap().len();
    assert!(materialized > 4, "the wide query must reach more than 4 partitions, not {materialized}");

    // The cache works here at all: a plain table's repeated query hits.
    let probe = |c: &mut Client, sql: &str| {
        let (h0, _, s0) = query_cache::stats();
        let r = c.ok(sql);
        let (h1, _, s1) = query_cache::stats();
        (r.rows, h1 - h0, s1 - s0)
    };
    let plain = "SELECT id, n FROM qc_plain WHERE n > 3";
    let (first, _, stored) = probe(&mut c, plain);
    assert!(stored > 0, "the plain table's query stored nothing: the cache is not on");
    let (second, hits, _) = probe(&mut c, plain);
    assert_eq!(first, second);
    assert!(hits > 0, "the plain table's repeated query did not hit");

    // One partition: stored on the first run, a hit on the second.
    let narrow = "SELECT id, n FROM qc WHERE tenant_id = 7 AND n > 3";
    let (first, _, stored) = probe(&mut c, narrow);
    assert!(stored > 0, "a one-partition route stored nothing");
    let (second, hits, _) = probe(&mut c, narrow);
    assert_eq!(first, second);
    assert!(hits > 0, "a one-partition route's second run did not hit the cache");

    // More than four partitions: nothing stored, nothing hit, run twice.
    let wide = "SELECT id, n FROM qc WHERE n > 3";
    for run in 0..2 {
        let (rows, hits, stored) = probe(&mut c, wide);
        assert_eq!(stored, 0, "run {run}: a route over {materialized} partitions stored into the cache");
        assert_eq!(hits, 0, "run {run}: a route over {materialized} partitions hit the cache");
        let twin = c.ok("SELECT id, n FROM qc_plain WHERE n > 3").rows;
        let mut a = rows.clone();
        let mut b = twin;
        a.sort();
        b.sort();
        assert_eq!(a, b, "run {run}");
    }

    // A write retires the narrow entry: the next run answers the new row.
    c.ok("INSERT INTO qc (tenant_id, id, n) VALUES (7, 99, 1000)");
    let (after, _, _) = probe(&mut c, narrow);
    assert!(after.iter().any(|r| r[0].as_deref() == Some("99")), "a cached answer outlived a write: {after:?}");
    drop(epoch);
    drop(pt);
    drop(c);
    drop(server);
}
