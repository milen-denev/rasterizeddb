//! Lazy open and exclusive close through pgwire sessions
//! (`docs/partitioning.md` §7.2, phase 3 P5b): what only a session shows.
//!
//! A transaction block that wrote a partition keeps a ticket on its write
//! gate until its outcome is final (persistent indexes §2), so a close
//! leaves that partition open while the block runs, however idle the
//! instance is between its statements; once the block ends, the close
//! goes ahead, and the next statement reopens it with the block's outcome
//! applied: a committed block's rows, a rolled-back block's none.

mod partition_wire;

use partition_wire::*;
use rasterizeddb_core::core::partition::table::Close;

fn pid_of(server: &Server, tenant: i64) -> u32 {
    let pt = server.db.partitioned_table("lz").expect("partitioned");
    let epoch = pt.epoch().expect("it opens");
    let pids = pt.route_where(&format!("tenant_id = {tenant}")).expect("routes").pids(&epoch.map);
    assert_eq!(pids.len(), 1);
    pids[0]
}

fn close_pid(server: &Server, pid: u32) -> Close {
    let db = server.db.clone();
    block_on_big(async move {
        db.partitioned_table("lz").expect("partitioned").close_partition(pid).await.expect("a close")
    })
}

fn ids(c: &mut Client, tenant: i64) -> Vec<String> {
    let mut v: Vec<String> = c
        .ok(&format!("SELECT id FROM lz WHERE tenant_id = {tenant}"))
        .rows
        .into_iter()
        .map(|r| r[0].clone().unwrap_or_default())
        .collect();
    v.sort();
    v
}

/// Mutation: `Parts::close` without the gate-count check: the partition
/// closes under the open block.
#[test]
fn a_partition_an_open_block_wrote_stays_open_until_the_block_ends() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    c.ok("CREATE TABLE lz (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, PRIMARY KEY (tenant_id, id)) \
          PARTITION BY HASH (tenant_id) PARTITIONS 8");
    c.ok("INSERT INTO lz (tenant_id, id) VALUES (1, 1), (2, 2)");
    let p = pid_of(&server, 1);
    assert_eq!(close_pid(&server, p), Close::Closed);

    for (end, kept) in [("COMMIT", true), ("ROLLBACK", false)] {
        let id = if kept { 10 } else { 20 };
        c.ok("BEGIN");
        c.ok(&format!("INSERT INTO lz (tenant_id, id) VALUES (1, {id})"));
        // Idle between the block's statements, but the block keeps its
        // ticket on the partition's gate.
        assert_eq!(close_pid(&server, p), Close::InUse, "a partition closed under an open block that wrote it");
        let mut other = server.connect();
        assert_eq!(ids(&mut other, 1), if kept { vec!["1"] } else { vec!["1", "10"] });
        c.ok(end);
        assert_eq!(close_pid(&server, p), Close::Closed, "the block ended ({end}) and the partition still would not close");
        let want: Vec<&str> = if kept { vec!["1", "10"] } else { vec!["1", "10"] };
        assert_eq!(ids(&mut other, 1), want, "after {end} and a reopen");
    }
    drop(c);
    server.stop();
}
