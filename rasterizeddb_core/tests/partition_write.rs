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

/// `pg_class.reltuples` and `pg_stat_user_tables`' live and dead counts for
/// `t`: the slot-derived figures the twin compares (finding: a same-value key
/// assignment cost the partitioned table a version the twin overwrote).
fn tuple_stats(c: &mut Client, t: &str) -> Vec<Vec<Option<String>>> {
    let mut out = c.ok(&format!("SELECT reltuples FROM pg_class WHERE relname = '{t}'")).rows;
    out.extend(c.ok(&format!("SELECT n_live_tup, n_dead_tup FROM pg_stat_user_tables WHERE relname = '{t}'")).rows);
    out
}

/// A routed UPDATE assigning key columns the values its WHERE pins, and an
/// `ON CONFLICT … DO UPDATE` assigning the key `EXCLUDED`'s equal value, take
/// the plain table's in-place overwrite: no more versions than the twin, so
/// the same `reltuples`, `n_live_tup` and `n_dead_tup` after each. Before the
/// fix the partitioned side wrote a new version every time (twin seeds
/// 0x5eed1234 and 0x5eed4242 at 700 ops: reltuples 1010 vs 1009).
#[test]
fn a_same_value_key_assignment_writes_no_more_versions_than_the_twin() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    twins(&mut c, "kp", "kq", 7);
    assert_eq!(tuple_stats(&mut c, "kp"), tuple_stats(&mut c, "kq"), "before any update");
    for (i, sql) in [
        "UPDATE {t} SET tenant_id = 3, n = 42 WHERE tenant_id = 3 AND grp = 1 AND id = 4",
        "UPDATE {t} SET tenant_id = 5, grp = 2, n = 43 WHERE tenant_id = 5 AND grp = 2",
        "INSERT INTO {t} (tenant_id, grp, id, n, s) VALUES (7, 3, 7, 44, 'x') \
         ON CONFLICT (tenant_id, grp, id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id, n = EXCLUDED.n",
        "INSERT INTO {t} (tenant_id, grp, id, n, s) VALUES (9, 1, 1, 45, 'x') \
         ON CONFLICT (tenant_id, grp, id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id, grp = EXCLUDED.grp, \
         id = EXCLUDED.id, n = {t}.n + EXCLUDED.n",
    ]
    .into_iter()
    .enumerate()
    {
        let rp = c.ok(&sql.replace("{t}", "kp"));
        let rq = c.ok(&sql.replace("{t}", "kq"));
        assert_eq!(rp.tags, rq.tags, "{sql}");
        same_rows(&mut c, "kp", "kq", sql);
        let (sp, sq) = (tuple_stats(&mut c, "kp"), tuple_stats(&mut c, "kq"));
        assert_eq!(sp, sq, "statement {i} `{sql}`: partitioned vs twin (reltuples; n_live_tup, n_dead_tup)");
        // The comparison is only worth having if the twin overwrote in place.
        // The last statement's `n` is an expression, which writes a version
        // on both sides.
        if i < 3 {
            assert_eq!(sq[1][1].as_deref(), Some("0"), "statement {i}: the twin's dead tuples");
        }
    }
    assert!(misplaced_rows(&server.db, "kp").is_empty());
    drop(c);
    drop(server);
}

/// A partition's own UPDATE assigning a key column a value the WHERE does not
/// pin keeps off the in-place overwrite, so a guard refusal of a later row
/// finds nothing written: refused, rows unchanged, nothing overwritten. The
/// statement pairs a row whose point it keeps with one it would move, in the
/// one partition holding both tenants. The same assignment pinned by the
/// WHERE does overwrite in place, as the plain table's would.
#[test]
fn a_genuine_key_change_is_refused_before_any_partition_write() {
    use rasterizeddb_core::core::mvcc::transaction::FROZEN_TX;
    use rasterizeddb_core::core::partition::Route;
    use rasterizeddb_core::core::rql::lexer_s1::QueryPurpose;
    use rasterizeddb_core::core::rql::lexer_ur::update_row_from_query_purpose;
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    twins(&mut c, "gp", "gq", 7);
    drop(c);
    // Twelve tenants in seven partitions: some partition holds two.
    let (pid, a, b) = stored_rows_by_partition(&server.db, "gp")
        .into_iter()
        .find_map(|(pid, rows)| {
            let mut tenants: Vec<i64> = rows
                .iter()
                .map(|r| {
                    let bytes = &r.iter().find(|(n, _)| n == "tenant_id").expect("tenant_id").1;
                    i64::from_le_bytes(bytes[..8].try_into().unwrap())
                })
                .collect();
            tenants.sort();
            tenants.dedup();
            (tenants.len() >= 2).then(|| (pid, tenants[0], tenants[1]))
        })
        .expect("a partition holding two tenants");
    let pt = server.db.partitioned.get("gp").expect("gp").value().clone();
    let before = stored_rows(&server.db, "gp");
    let (refused, overwrites, after_pinned) = block_on_big(async move {
        let epoch = pt.epoch().unwrap();
        let part = pt
            .materialized(&epoch, &Route::All)
            .unwrap()
            .into_iter()
            .find(|(p, _)| *p == pid)
            .expect("the partition")
            .1;
        let fields = pt.shell().schema.fields.clone();
        let update = |sql: String| update_row_from_query_purpose(&QueryPurpose::UpdateRow(sql.clone()), &fields)
            .unwrap_or_else(|| panic!("cannot parse {sql}"));
        let in_place = part.in_place_column_updates();
        // Row `a` keeps its point and is scanned first; row `b` would move.
        let u = update(format!("UPDATE gp SET tenant_id = {a}, n = 77 WHERE tenant_id IN ({a}, {b}) AND grp = 1 AND id = 1"));
        let refused = part
            .update_rows_as_of(&u.where_clause, u.query_row_fetch, u.updates, &u.expressions, None, FROZEN_TX, None, None)
            .await;
        let overwrites = part.in_place_column_updates() - in_place;
        let u = update(format!("UPDATE gp SET tenant_id = {a}, n = 78 WHERE tenant_id = {a} AND grp = 1 AND id = 1"));
        let pinned = part
            .update_rows_as_of(&u.where_clause, u.query_row_fetch, u.updates, &u.expressions, None, FROZEN_TX, None, None)
            .await
            .expect("a pinned same-value key assignment");
        assert_eq!(pinned, 1, "the pinned update matched its row");
        (refused, overwrites, part.in_place_column_updates() - in_place)
    });
    let e = refused.expect_err("a key change refused by the range guard");
    assert_eq!(e.kind(), std::io::ErrorKind::Unsupported, "{e}");
    assert!(e.to_string().contains("not supported"), "{e}");
    assert_eq!(overwrites, 0, "the refused statement overwrote a row in place before its refusal");
    assert_eq!(after_pinned, 1, "the pinned same-value key assignment did not overwrite in place");
    let after = stored_rows(&server.db, "gp");
    assert_eq!(after.len(), before.len(), "row count");
    let changed: Vec<_> = before.iter().filter(|r| !after.contains(r)).collect();
    assert_eq!(changed.len(), 1, "only the pinned update's row changed: {changed:?}");
    drop(server);
}
