//! `UPDATE`, `DELETE` and `VACUUM` on a partitioned table over the wire (S11,
//! `docs/partitioning.md` §4.4, §4.5, §9): cross-partition UPDATE transfers
//! beside SUM readers (I7), blocks that commit or roll back whole, the
//! refusals and their SQLSTATEs, and prepared UPDATE and DELETE that stay
//! right after writes and a restart (I11).
//!
//! Each comparison is with a plain twin holding the same rows. Bounded: fixed
//! row and statement counts, and the concurrent test stops at a fixed count
//! or after five seconds, whichever comes first.

mod partition_wire;

use std::time::{Duration, Instant};

use partition_wire::*;

const COLUMNS: &str = "(tenant_id BIGINT NOT NULL, grp BIGINT NOT NULL, id BIGINT NOT NULL, n INT, s TEXT, \
                       PRIMARY KEY (tenant_id, grp, id))";

/// `p` partitioned by tenant into `partitions`, `q` its plain twin, the same
/// rows in both.
fn twins(c: &mut Client, p: &str, q: &str, partitions: u32) {
    c.ok(&format!("CREATE TABLE {p} {COLUMNS} PARTITION BY HASH (tenant_id) PARTITIONS {partitions}"));
    c.ok(&format!("CREATE TABLE {q} {COLUMNS}"));
    let mut rows = Vec::new();
    for t in 1..=12i64 {
        for g in 1..=3i64 {
            for i in [1i64, 4, 7] {
                let n = if (t + g + i) % 5 == 0 { "NULL".to_string() } else { ((t * 7 + g * 3 + i) % 90).to_string() };
                rows.push(format!("({t}, {g}, {i}, {n}, 't{t}')"));
            }
        }
    }
    for table in [p, q] {
        c.ok(&format!("INSERT INTO {table} (tenant_id, grp, id, n, s) VALUES {}", rows.join(", ")));
    }
}

fn sorted(mut rows: Vec<Vec<Option<String>>>) -> Vec<Vec<Option<String>>> {
    rows.sort();
    rows
}

/// Both tables hold the same rows.
#[track_caller]
fn same_rows(c: &mut Client, p: &str, q: &str, label: &str) {
    let all = |c: &mut Client, t: &str| sorted(c.ok(&format!("SELECT * FROM {t}")).rows);
    assert_eq!(all(c, p), all(c, q), "{label}");
}

/// I7 under real concurrency, with the transfer one cross-partition UPDATE:
/// a writer moves value between two tenants in different partitions while a
/// reader sums every partition again and again. Every sum is the opening
/// total; each transfer lands whole.
#[test]
fn a_cross_partition_update_transfer_is_never_seen_half_applied() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut setup = server.connect();
    setup.ok(
        "CREATE TABLE acct (tenant_id BIGINT NOT NULL, bal BIGINT NOT NULL, PRIMARY KEY (tenant_id)) \
         PARTITION BY HASH (tenant_id) PARTITIONS 64",
    );
    let opening: Vec<String> = (1..=16).map(|t| format!("({t}, 1000)")).collect();
    setup.ok(&format!("INSERT INTO acct (tenant_id, bal) VALUES {}", opening.join(", ")));
    let pt = server.db.partitioned.get("acct").unwrap().value().clone();
    let epoch = pt.epoch().unwrap();
    let pid = |t: i64| epoch.map.pid_of(pt.spec().unwrap().point_of(&[&t.to_le_bytes()[..]]).unwrap());
    // The two tenants in different groups of the fan-out's eight partitions
    // read at once, so a read sees them at two different moments.
    let mut pids: Vec<u32> = (1..=16i64).map(pid).collect();
    pids.sort();
    pids.dedup();
    assert!(pids.len() > 8, "16 tenants reach only {} partitions", pids.len());
    let a = (1..=16i64).find(|t| pid(*t) == pids[0]).unwrap();
    let b = (1..=16i64).find(|t| pid(*t) == pids[pids.len() - 1]).unwrap();
    drop(setup);

    const TRANSFERS: i64 = 150;
    const SUMS: usize = 300;
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut writer = server.connect();
    let mut reader = server.connect();
    let (sums, transfers) = std::thread::scope(|scope| {
        let w = scope.spawn(move || {
            let mut done = 0i64;
            for k in 1..=TRANSFERS {
                if Instant::now() > deadline {
                    break;
                }
                let (from, to) = if k % 2 == 0 { (a, b) } else { (b, a) };
                let sql = format!(
                    "UPDATE acct SET bal = CASE WHEN tenant_id = {from} THEN bal - {k} ELSE bal + {k} END \
                     WHERE tenant_id IN ({from}, {to})"
                );
                // Every third transfer inside a block, with a read of its own
                // write before the commit.
                if k % 3 == 0 {
                    writer.ok("BEGIN");
                    writer.ok(&sql);
                    let r = writer.ok("SELECT SUM(bal) FROM acct");
                    assert_eq!(r.rows, vec![vec![Some("16000".to_string())]], "transfer {k}: its own block");
                    writer.ok("COMMIT");
                } else {
                    let r = writer.ok(&sql);
                    assert_eq!(r.tags, vec!["UPDATE 2".to_string()], "transfer {k}");
                }
                done += 1;
            }
            done
        });
        let r = scope.spawn(move || {
            let mut seen = 0;
            for i in 0..SUMS {
                if Instant::now() > deadline {
                    break;
                }
                let v: i64 = reader.ok("SELECT SUM(bal) FROM acct").rows[0][0].as_deref().unwrap().parse().unwrap();
                assert_eq!(v, 16_000, "sum {i}: a transfer was seen half applied");
                seen += 1;
            }
            seen
        });
        (r.join().unwrap(), w.join().unwrap())
    });
    assert!(sums > 10 && transfers > 10, "too little ran: {sums} sums, {transfers} transfers");
    let mut c = server.connect();
    assert_eq!(c.ok("SELECT SUM(bal) FROM acct").rows, vec![vec![Some("16000".to_string())]]);
    // The transfers alternate, so `a` ends where the arithmetic says.
    let net: i64 = (1..=transfers).map(|k| if k % 2 == 0 { -k } else { k }).sum();
    let bal = c.ok(&format!("SELECT bal FROM acct WHERE tenant_id = {a}")).rows;
    assert_eq!(bal, vec![vec![Some((1000 + net).to_string())]]);
    drop(c);
    drop(epoch);
    drop(pt);
    drop(server);
}

/// A block's fan-out writes commit whole or roll back whole, like the
/// twin's; a statement failing inside one fails the block (25P02), and
/// nothing of it commits.
#[test]
fn blocks_commit_or_roll_back_every_partition_together() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    twins(&mut c, "bp", "bq", 7);
    for t in ["bp", "bq"] {
        c.ok("BEGIN");
        c.ok(&format!("UPDATE {t} SET n = 0 WHERE grp = 2"));
        c.ok(&format!("DELETE FROM {t} WHERE tenant_id IN (2, 5, 9) AND id = 4"));
        c.ok("ROLLBACK");
    }
    same_rows(&mut c, "bp", "bq", "after a rolled-back block");
    for t in ["bp", "bq"] {
        c.ok("BEGIN");
        c.ok(&format!("UPDATE {t} SET n = 0 WHERE grp = 2"));
        c.ok(&format!("DELETE FROM {t} WHERE tenant_id IN (2, 5, 9) AND id = 4"));
        c.ok("COMMIT");
    }
    same_rows(&mut c, "bp", "bq", "after a committed block");
    // A refused statement in a block poisons it; the earlier fan-out goes.
    c.ok("BEGIN");
    c.ok("UPDATE bp SET n = 77 WHERE grp = 1");
    let e = c.simple("UPDATE bp SET tenant_id = 3 WHERE grp = 1").expect_err("a key change");
    assert_eq!(e.code, "0A000", "{e}");
    let e = c.simple("SELECT 1").expect_err("the block is aborted");
    assert_eq!(e.code, "25P02", "{e}");
    let end = c.simple("COMMIT");
    assert!(end.is_ok(), "{end:?}");
    same_rows(&mut c, "bp", "bq", "after a failed block");
    drop(c);
    drop(server);
}

/// The refusals, each before anything is written, with its SQLSTATE: a key
/// change (phase 6), UPDATE … FROM and DELETE … USING (phase 3). Assigning a
/// key the value its WHERE pins is not a change. (RETURNING is built since
/// S17: `partition_returning.rs`.)
#[test]
fn refused_writes_change_nothing() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    twins(&mut c, "rp", "rq", 7);
    c.ok("CREATE TABLE rdim (tenant_id BIGINT NOT NULL, label TEXT)");
    c.ok("INSERT INTO rdim (tenant_id, label) VALUES (1, 'one'), (2, 'two')");
    let before = stored_rows(&server.db, "rp");
    for sql in [
        "UPDATE rp SET tenant_id = 99 WHERE tenant_id = 1",
        "UPDATE rp SET tenant_id = 2 WHERE n > 10",
        "UPDATE rp SET tenant_id = tenant_id + 1 WHERE tenant_id = 1",
        "UPDATE rp SET n = 1 FROM rdim WHERE rp.tenant_id = rdim.tenant_id",
        "DELETE FROM rp USING rdim WHERE rp.tenant_id = rdim.tenant_id",
    ] {
        let e = c.simple(sql).expect_err(sql);
        assert_eq!(e.code, "0A000", "{sql}: {e}");
        assert!(e.message.contains("not supported"), "{sql}: {e}");
        assert_eq!(stored_rows(&server.db, "rp"), before, "{sql} wrote something");
    }
    for t in ["rp", "rq"] {
        let r = c.ok(&format!("UPDATE {t} SET tenant_id = 3, grp = 1, n = 5 WHERE tenant_id = 3 AND grp = 1"));
        assert_eq!(r.tags, vec!["UPDATE 3".to_string()], "{t}");
    }
    same_rows(&mut c, "rp", "rq", "a same-value key assignment");
    drop(c);
    drop(server);
}

/// I11 for writes: a prepared UPDATE and DELETE, run alternately for
/// different tenants, past the third execution, across a write and across a
/// restart, change exactly what the twin's change.
#[test]
fn prepared_updates_and_deletes_stay_right_after_writes_and_a_restart() {
    let dir = tempfile::tempdir().expect("dir");
    let mut server = Server::start(dir.path().to_str().unwrap());
    {
        let mut c = server.connect();
        twins(&mut c, "pp", "pq", 16);
    }
    let update = "UPDATE {t} SET n = $1 WHERE tenant_id = $2 AND grp = $3";
    let delete = "DELETE FROM {t} WHERE tenant_id = $1 AND id = $2";
    let mut next_id = 100;
    for life in 0..2 {
        let mut c = server.connect();
        for (name, sql) in [("u", update), ("d", delete)] {
            c.prepare(name, &sql.replace("{t}", "pp")).expect("prepare");
            c.prepare(&format!("{name}q"), &sql.replace("{t}", "pq")).expect("prepare");
        }
        for run in 0..8usize {
            let tenant = [3, 8, 3, 11, 5, 8, 1, 3][run];
            let grp = (run % 3 + 1) as i64;
            // A write to the very rows the next execution changes.
            for t in ["pp", "pq"] {
                c.ok(&format!("INSERT INTO {t} (tenant_id, grp, id, n, s) VALUES ({tenant}, {grp}, {next_id}, 0, 'w')"));
            }
            next_id += 1;
            let params = [Some((run * 10 + life).to_string()), Some(tenant.to_string()), Some(grp.to_string())];
            let rp = c.execute_prepared("u", &params).unwrap_or_else(|e| panic!("life {life} run {run}: {e}"));
            let rq = c.execute_prepared("uq", &params).expect("the twin");
            assert_eq!(rp.tags, rq.tags, "life {life} run {run}: UPDATE tenant {tenant}");
            assert!(rp.tags[0] != "UPDATE 0", "life {life} run {run}: the update matched nothing");
            let params = [Some(tenant.to_string()), Some((if run % 2 == 0 { 4 } else { next_id - 1 }).to_string())];
            let rp = c.execute_prepared("d", &params).expect("delete");
            let rq = c.execute_prepared("dq", &params).expect("the twin");
            assert_eq!(rp.tags, rq.tags, "life {life} run {run}: DELETE tenant {tenant}");
            same_rows(&mut c, "pp", "pq", &format!("life {life} run {run}"));
        }
        for name in ["u", "uq", "d", "dq"] {
            c.close_prepared(name);
        }
        drop(c);
        if life == 0 {
            server = server.restart();
        }
    }
    drop(server);
}

/// VACUUM and VACUUM FULL on the partitioned table, and bare, leave it
/// answering like its twin, before and after a restart (finding F2: a bare
/// VACUUM failed once a partitioned table existed).
#[test]
fn vacuum_leaves_the_table_like_its_twin() {
    let dir = tempfile::tempdir().expect("dir");
    let mut server = Server::start(dir.path().to_str().unwrap());
    {
        let mut c = server.connect();
        twins(&mut c, "vp", "vq", 7);
        for t in ["vp", "vq"] {
            c.ok(&format!("UPDATE {t} SET s = 'relocated by a longer value' WHERE grp = 2"));
            c.ok(&format!("DELETE FROM {t} WHERE id = 4"));
        }
        for sql in ["VACUUM vp", "VACUUM", "VACUUM FULL vp", "VACUUM FULL"] {
            let r = c.ok(sql);
            assert_eq!(r.tags, vec!["VACUUM".to_string()], "{sql}");
            same_rows(&mut c, "vp", "vq", sql);
        }
        for t in ["vp", "vq"] {
            c.ok(&format!("INSERT INTO {t} (tenant_id, grp, id, n) VALUES (4, 2, 4, 1), (13, 1, 1, 2)"));
            c.ok(&format!("UPDATE {t} SET n = n + 1 WHERE tenant_id IN (4, 13)"));
        }
        same_rows(&mut c, "vp", "vq", "writes after VACUUM FULL");
    }
    server = server.restart();
    let mut c = server.connect();
    same_rows(&mut c, "vp", "vq", "after a restart");
    for t in ["vp", "vq"] {
        let r = c.ok(&format!("SELECT n, s FROM {t} WHERE tenant_id = 4 AND grp = 2 AND id = 4"));
        assert_eq!(r.rows, vec![vec![Some("2".to_string()), None]], "{t}");
    }
    assert!(misplaced_rows(&server.db, "vp").is_empty());
    drop(c);
    drop(server);
}
