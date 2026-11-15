//! Index snapshots of partitions on a key/value store over the wire
//! (docs/kv-partitions.md §2): the server's shutdown seal stores each open
//! partition's FILTER and INDEX sections in its family, and the restart
//! loads them instead of rebuilding, validated by the files' store states;
//! a partition written after the seal rebuilds and still answers through
//! its index. At the store's default tuning, as a deployment runs it.
//!
//! Its own binary: `index_snapshot.index_storage = snapshot` is a
//! process-wide setting, and the other partition tests run under the
//! default.

mod partition_wire;

use std::time::Duration;

use partition_wire::*;
use rasterizeddb_core::core::index_store::load::{self, Origin, settings::IndexStorage};

const P: &str = "access_logs";
const COLUMNS: &str =
    "(domain TEXT NOT NULL, ts BIGINT NOT NULL, seq BIGINT NOT NULL, path TEXT, PRIMARY KEY (domain, ts, seq))";
const DOMAINS: [&str; 6] = ["a.example", "b.example", "c.example", "d.example", "e.example", "f.example"];

/// Every partition opened in full (a fan-out read opens one scan-only, its
/// index start deferred), its index load or build finished, with how its
/// indexes came to be.
fn origins(server: &Server) -> Vec<(u32, Origin)> {
    let pt = server.db.partitioned_table(P).expect("the partitioned table");
    block_on_big(async move {
        let mut out = Vec::new();
        for pid in pt.materialized_pids().expect("its partitions") {
            let part = pt.open_part(pid).await.expect("the partition opens");
            part.index_load_settled().await;
            let deadline = std::time::Instant::now() + Duration::from_secs(60);
            while part.index_origin() == Origin::Pending {
                assert!(std::time::Instant::now() < deadline, "partition {pid}'s indexes never settled");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            out.push((pid, part.index_origin()));
        }
        out.sort_by_key(|(pid, _)| *pid);
        out
    })
}

/// The server's shutdown seal, as `shutdown::seal_all` runs it once the
/// sessions are gone: every seal must end clean.
fn seal_for_shutdown(server: &Server) {
    let db = server.db.clone();
    let report = block_on_big(async move { db.seal_for_shutdown(Duration::from_secs(60)).await });
    assert!(report.skipped.is_empty(), "skipped by the deadline: {:?}", report.skipped);
    for s in &report.sealed {
        assert!(s.outcome.is_clean(), "{}: {}", s.name, s.outcome.describe());
    }
}

fn count(c: &mut Client, sql: &str) -> String {
    let reply = c.ok(sql);
    reply.rows[0][0].clone().expect("a count")
}

#[test]
fn a_restart_loads_store_partitions_sealed_at_shutdown() {
    load::settings::set_index_storage(IndexStorage::Snapshot);
    let dir = tempfile::tempdir().unwrap();
    let kv = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path().to_str().unwrap());
    {
        let mut c = server.connect();
        c.ok(&format!("STORAGE ADD STORE logs DIRECTORY WITH (PATH = '{}', PREFIX = 'logs')", kv.path().display()));
        c.ok(&format!("STORAGE PLACE TABLE {P} AT 'kv:logs'"));
        c.ok(&format!("CREATE TABLE {P} {COLUMNS} PARTITION BY HASH (domain) PARTITIONS 4"));
        c.ok(&format!("CREATE INDEX access_logs_path ON {P} (path)"));
        // Statement by statement, as a logger writes: one delta object per
        // file per commit, and the store's own compactions along the way.
        for i in 0..600 {
            c.ok(&format!(
                "INSERT INTO {P} (domain, ts, seq, path) VALUES ('{}', {i}, 0, '/p{}')",
                DOMAINS[i % 6],
                i % 50
            ));
        }
        c.ok(&format!("DELETE FROM {P} WHERE ts < 60"));
        assert_eq!(count(&mut c, &format!("SELECT count(*) FROM {P}")), "540");
        assert_eq!(count(&mut c, &format!("SELECT count(*) FROM {P} WHERE path = '/p7'")), "10");
        // What oxyredox asks before its CREATE, over pgwire here (the native
        // path is `native_catalog_tests`).
        let exists = c.ok(&format!("SELECT schemaname, tablename FROM pg_tables WHERE tablename = '{P}'"));
        assert_eq!(exists.rows, vec![vec![Some("public".to_string()), Some(P.to_string())]]);
    }
    seal_for_shutdown(&server);
    let server = server.restart();
    {
        let mut c = server.connect();
        assert_eq!(count(&mut c, &format!("SELECT count(*) FROM {P}")), "540");
        let loaded = origins(&server);
        assert!(loaded.len() >= 2, "the domains reach several partitions: {loaded:?}");
        assert!(loaded.iter().all(|(_, o)| *o == Origin::Loaded), "every partition loads its sections: {loaded:?}");
        assert_eq!(count(&mut c, &format!("SELECT count(*) FROM {P} WHERE path = '/p7'")), "10");
        // Written after the restart and never sealed.
        c.ok(&format!("INSERT INTO {P} (domain, ts, seq, path) VALUES ('a.example', 9000, 0, '/late')"));
        c.ok(&format!("UPDATE {P} SET path = '/moved' WHERE domain = 'b.example' AND ts = 61"));
    }
    let server = server.restart();
    {
        let mut c = server.connect();
        assert_eq!(count(&mut c, &format!("SELECT count(*) FROM {P}")), "541");
        let after = origins(&server);
        let rebuilt: Vec<u32> = after.iter().filter(|(_, o)| *o != Origin::Loaded).map(|(p, _)| *p).collect();
        assert!(!rebuilt.is_empty() && rebuilt.len() <= 2, "the written partitions rebuild: {after:?}");
        assert_eq!(count(&mut c, &format!("SELECT count(*) FROM {P} WHERE path = '/late'")), "1");
        assert_eq!(count(&mut c, &format!("SELECT count(*) FROM {P} WHERE path = '/moved'")), "1");
        assert_eq!(count(&mut c, &format!("SELECT count(*) FROM {P} WHERE path = '/p7'")), "10");
    }
    seal_for_shutdown(&server);
    let server = server.restart();
    {
        let mut c = server.connect();
        assert_eq!(count(&mut c, &format!("SELECT count(*) FROM {P}")), "541");
        let again = origins(&server);
        assert!(again.iter().all(|(_, o)| *o == Origin::Loaded), "resealed at shutdown, every partition loads: {again:?}");
        assert_eq!(count(&mut c, &format!("SELECT count(*) FROM {P} WHERE path = '/moved'")), "1");
    }
    server.stop();
}
