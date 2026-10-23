//! `SELECT` and `COUNT` on a partitioned table over the wire (S10,
//! `docs/partitioning.md` §4, §8): one snapshot per statement under a
//! concurrent writer (I7), no pruning without proof (I3), an empty route that
//! still describes and answers like the twin, and prepared SELECTs that stay
//! right after writes and a restart (I11).
//!
//! Each comparison is with a plain twin holding the same rows. Bounded: a
//! fixed number of rows and statements, and the concurrent test stops at a
//! fixed iteration count or after five seconds, whichever comes first.

mod partition_wire;

use std::time::{Duration, Instant};

use partition_wire::*;

const COLUMNS: &str = "(tenant_id BIGINT NOT NULL, grp BIGINT NOT NULL, id BIGINT NOT NULL, n INT, s TEXT, \
                       f DOUBLE PRECISION, PRIMARY KEY (tenant_id, grp, id))";

/// `p` partitioned by tenant into `partitions`, `q` its plain twin, the same
/// rows in both: tenants 1 to 12, three groups, a few ids each, with NULLs.
fn twins(c: &mut Client, p: &str, q: &str, partitions: u32) {
    c.ok(&format!("CREATE TABLE {p} {COLUMNS} PARTITION BY HASH (tenant_id) PARTITIONS {partitions}"));
    c.ok(&format!("CREATE TABLE {q} {COLUMNS}"));
    let mut rows = Vec::new();
    for t in 1..=12i64 {
        for g in 1..=3i64 {
            for i in [1i64, 4, 7] {
                let n = if (t + g + i) % 5 == 0 { "NULL".to_string() } else { ((t * 7 + g * 3 + i) % 90).to_string() };
                let s = match (t + i) % 4 {
                    0 => "NULL".to_string(),
                    1 => "'alpha'".to_string(),
                    2 => format!("'t{t}'"),
                    _ => "''".to_string(),
                };
                rows.push(format!("({t}, {g}, {i}, {n}, {s}, {})", (i % 8) as f64 * 0.25));
            }
        }
    }
    for table in [p, q] {
        c.ok(&format!("INSERT INTO {table} (tenant_id, grp, id, n, s, f) VALUES {}", rows.join(", ")));
    }
}

fn sorted(mut rows: Vec<Vec<Option<String>>>) -> Vec<Vec<Option<String>>> {
    rows.sort();
    rows
}

/// `template` with `{t}` as `p` and as `q`: the same answer, names and types
/// included, compared as multisets unless `ordered`.
#[track_caller]
fn like_twin(c: &mut Client, template: &str, p: &str, q: &str, ordered: bool) -> Reply {
    let sql_p = template.replace("{t}", p);
    let sql_q = template.replace("{t}", q);
    let rq = c.simple(&sql_q).unwrap_or_else(|e| panic!("the twin failed `{sql_q}`: {e}"));
    let rp = c.simple(&sql_p).unwrap_or_else(|e| panic!("the partitioned table failed `{sql_p}`: {e}"));
    assert_eq!(rp.columns, rq.columns, "`{sql_p}`: column names");
    assert_eq!(rp.types, rq.types, "`{sql_p}`: column types");
    if ordered {
        assert_eq!(rp.rows, rq.rows, "`{sql_p}`: rows (ordered)");
    } else {
        assert_eq!(sorted(rp.rows.clone()), sorted(rq.rows.clone()), "`{sql_p}`: rows");
    }
    assert_eq!(rp.tags, rq.tags, "`{sql_p}`: tags");
    rp
}

/// I7 under a real concurrent writer: one session moves value between
/// tenants in different partitions, a debit and a credit in one explicit
/// transaction, while another sums every partition again and again. Every
/// sum is the opening balance, and every count of the ledger even.
#[test]
fn a_concurrent_transfer_is_never_seen_half_applied() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut setup = server.connect();
    setup.ok(
        "CREATE TABLE ledger (tenant_id BIGINT NOT NULL, seq BIGINT NOT NULL, amt BIGINT NOT NULL, \
         PRIMARY KEY (tenant_id, seq)) PARTITION BY HASH (tenant_id) PARTITIONS 7",
    );
    let opening: Vec<String> = (1..=16).map(|t| format!("({t}, 0, 1000)")).collect();
    setup.ok(&format!("INSERT INTO ledger (tenant_id, seq, amt) VALUES {}", opening.join(", ")));
    // Two tenants in different partitions.
    let pt = server.db.partitioned.get("ledger").unwrap().value().clone();
    let epoch = pt.epoch().unwrap();
    let pid = |t: i64| epoch.map.pid_of(pt.spec().unwrap().point_of(&[&t.to_le_bytes()[..]]).unwrap());
    let a = 1i64;
    let b = (2..=16i64).find(|t| pid(*t) != pid(a)).expect("two partitions");
    drop(setup);

    const TRANSFERS: i64 = 150;
    const SUMS: usize = 300;
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut writer = server.connect();
    let mut reader = server.connect();
    let (sums, transfers) = std::thread::scope(|scope| {
        let w = scope.spawn(move || {
            let mut done = 0;
            for k in 1..=TRANSFERS {
                if Instant::now() > deadline {
                    break;
                }
                let (from, to) = if k % 2 == 0 { (a, b) } else { (b, a) };
                writer.ok("BEGIN");
                writer.ok(&format!("INSERT INTO ledger (tenant_id, seq, amt) VALUES ({from}, {k}, -{k})"));
                writer.ok(&format!("INSERT INTO ledger (tenant_id, seq, amt) VALUES ({to}, {k}, {k})"));
                writer.ok("COMMIT");
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
                let reply = if i % 2 == 0 {
                    reader.ok("SELECT SUM(amt) FROM ledger")
                } else {
                    reader.ok("SELECT COUNT(*) FROM ledger")
                };
                let v: i64 = reply.rows[0][0].as_deref().unwrap().parse().unwrap();
                if i % 2 == 0 {
                    assert_eq!(v, 16_000, "sum {i}: a transfer was seen half applied");
                } else {
                    assert_eq!(v % 2, 0, "count {i}: {v} rows, a transfer seen half applied");
                }
                seen += 1;
            }
            seen
        });
        (r.join().unwrap(), w.join().unwrap())
    });
    assert!(sums > 10 && transfers > 10, "too little ran: {sums} sums, {transfers} transfers");
    let mut c = server.connect();
    assert_eq!(c.ok("SELECT SUM(amt) FROM ledger").rows, vec![vec![Some("16000".to_string())]]);
    let rows = c.ok("SELECT COUNT(*) FROM ledger").rows;
    assert_eq!(rows, vec![vec![Some((16 + 2 * transfers).to_string())]]);
    drop(c);
    drop(server);
}

/// I3 through SQL: predicates that prove nothing about the key fan out and
/// answer like the twin. Each would lose rows if read as a key equality.
#[test]
fn predicates_without_proof_fan_out_and_answer_like_the_twin() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    twins(&mut c, "np_p", "np_q", 16);
    for clause in [
        "tenant_id = grp",
        "tenant_id = grp + 2",
        "tenant_id + 0 = 3",
        "tenant_id::text = '3'",
        "CAST(tenant_id AS TEXT) = '3'",
        "tenant_id = 3 OR n > 50",
        "tenant_id = 3 OR s = 'alpha'",
        "(tenant_id = 3 AND grp = 1) OR id = 4",
        "tenant_id <> 3",
        "NOT (tenant_id = 3)",
        "tenant_id > 9",
        "tenant_id BETWEEN 3 AND 5",
        "s LIKE 't1%'",
        "tenant_id = 3 AND tenant_id = 4 OR grp = 2",
        // An `OR` whose other branch constrains the key, but not to a value.
        "tenant_id = 3 OR tenant_id > 10",
        "tenant_id < 2 OR tenant_id = 7",
        "(tenant_id, grp) > (7, 2)",
        "tenant_id IN (3, 4) OR tenant_id >= 12",
    ] {
        let r = like_twin(&mut c, &format!("SELECT * FROM {{t}} WHERE {clause}"), "np_p", "np_q", false);
        assert!(!r.rows.is_empty(), "`{clause}` matched nothing on either table; the comparison proves nothing");
        like_twin(&mut c, &format!("SELECT COUNT(*), SUM(n), MAX(s) FROM {{t}} WHERE {clause}"), "np_p", "np_q", false);
    }

    // A text key read through LIKE, and a key compared with another column.
    for (p, q) in [("tk_p", "tk_q")] {
        c.ok(&format!("CREATE TABLE {p} (k TEXT NOT NULL, v INT, PRIMARY KEY (k)) PARTITION BY HASH (k) PARTITIONS 8"));
        c.ok(&format!("CREATE TABLE {q} (k TEXT NOT NULL, v INT, PRIMARY KEY (k))"));
        let rows: Vec<String> =
            ["ab", "abc", "abd", "b", "bab", "x", "AB", "ab%"].iter().enumerate().map(|(i, k)| format!("('{k}', {i})")).collect();
        for t in [p, q] {
            c.ok(&format!("INSERT INTO {t} (k, v) VALUES {}", rows.join(", ")));
        }
    }
    for clause in ["k LIKE 'ab%'", "k LIKE '%b'", "k = 'ab' OR v > 5", "k > 'ab'", "k <> 'ab'", "k ILIKE 'AB%'"] {
        let r = like_twin(&mut c, &format!("SELECT * FROM {{t}} WHERE {clause}"), "tk_p", "tk_q", false);
        assert!(!r.rows.is_empty(), "`{clause}` matched nothing");
    }
    // The equality on a text key still routes, and answers alike.
    let r = like_twin(&mut c, "SELECT * FROM {t} WHERE k = 'abd'", "tk_p", "tk_q", false);
    assert_eq!(r.rows.len(), 1);

    // A JSON path is never a key column (`extract_gated`, not the path-aware
    // variant): a document predicate fans out.
    c.ok(
        "CREATE TABLE jd_p (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, doc JSONB, PRIMARY KEY (tenant_id, id)) \
         PARTITION BY HASH (tenant_id) PARTITIONS 8",
    );
    c.ok("CREATE TABLE jd_q (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, doc JSONB, PRIMARY KEY (tenant_id, id))");
    let docs: Vec<String> = (1..=10i64)
        .map(|t| format!("({t}, {}, '{{\"tenant_id\": {}, \"k\": \"v{}\"}}')", t * 10, (t % 3) + 1, t % 4))
        .collect();
    for t in ["jd_p", "jd_q"] {
        c.ok(&format!("INSERT INTO {t} (tenant_id, id, doc) VALUES {}", docs.join(", ")));
    }
    for clause in ["doc->>'tenant_id' = '2'", "doc->>'k' = 'v1'", "(doc->>'tenant_id')::bigint = 3", "doc @> '{\"k\": \"v2\"}'"] {
        let r = like_twin(&mut c, &format!("SELECT tenant_id, id FROM {{t}} WHERE {clause}"), "jd_p", "jd_q", false);
        assert!(!r.rows.is_empty(), "`{clause}` matched nothing");
    }
    drop(c);
    drop(server);
}

/// An empty route (a tenant never written, a contradiction) answers with no
/// rows under the twin's description, and aggregates answer one row; the
/// extended protocol's Describe gives the twin's names and types, before
/// any execution and after.
#[test]
fn an_empty_route_answers_and_describes_like_the_twin() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    twins(&mut c, "er_p", "er_q", 64);
    for (sql, rows) in [
        ("SELECT * FROM {t} WHERE tenant_id = 999", 0),
        ("SELECT tenant_id, n, s, f FROM {t} WHERE tenant_id = 999 ORDER BY n", 0),
        ("SELECT * FROM {t} WHERE tenant_id = 1 AND tenant_id = 2", 0),
        ("SELECT COUNT(*) FROM {t} WHERE tenant_id = 999", 1),
        ("SELECT MAX(n), MIN(s), SUM(f), AVG(f), COUNT(n) FROM {t} WHERE tenant_id = 999", 1),
        ("SELECT COUNT(*), MAX(n) FROM {t} WHERE tenant_id = 1 AND tenant_id = 2", 1),
        ("SELECT grp, COUNT(*) FROM {t} WHERE tenant_id = 999 GROUP BY grp", 0),
        ("SELECT DISTINCT s FROM {t} WHERE tenant_id = 999", 0),
    ] {
        let r = like_twin(&mut c, sql, "er_p", "er_q", false);
        assert_eq!(r.rows.len(), rows, "`{sql}`");
        assert!(!r.columns.is_empty(), "`{sql}` described no columns");
    }
    // Describe, through Parse + Describe(S), then each execution.
    let template = "SELECT tenant_id, grp, id, n, s, f FROM {t} WHERE tenant_id = $1";
    let dp = c.prepare("d", &template.replace("{t}", "er_p")).expect("prepare on the partitioned table");
    let dq = c.prepare("dq", &template.replace("{t}", "er_q")).expect("prepare on the twin");
    assert_eq!((&dp.columns, &dp.types), (&dq.columns, &dq.types), "Describe differs");
    assert_eq!(dp.columns, vec!["tenant_id", "grp", "id", "n", "s", "f"]);
    for tenant in ["999", "3", "998"] {
        let rp = c.execute_prepared("d", &[Some(tenant.to_string())]).expect("execute");
        let rq = c.execute_prepared("dq", &[Some(tenant.to_string())]).expect("execute");
        assert_eq!((&rp.columns, &rp.types), (&rq.columns, &rq.types), "tenant {tenant}");
        assert_eq!(sorted(rp.rows), sorted(rq.rows), "tenant {tenant}");
    }
    c.close_prepared("d");
    c.close_prepared("dq");
    drop(c);
    drop(server);
}

/// I11: a prepared SELECT on a partitioned table runs from its text every
/// time, so each execution routes its own values: right after writes to its
/// partitions, past the executions where a plain table's statement starts
/// running from a learned parse, alternating tenants, and after a restart.
#[test]
fn a_prepared_select_stays_right_after_writes_and_a_restart() {
    let dir = tempfile::tempdir().expect("dir");
    let mut server = Server::start(dir.path().to_str().unwrap());
    {
        let mut c = server.connect();
        twins(&mut c, "pr_p", "pr_q", 16);
    }
    let sql = "SELECT id, n, s FROM {t} WHERE tenant_id = $1 AND grp = $2";
    let count_sql = "SELECT COUNT(*) FROM {t} WHERE tenant_id = $1";
    let mut next_id = 100;
    for life in 0..2 {
        let mut c = server.connect();
        c.prepare("s", &sql.replace("{t}", "pr_p")).expect("prepare");
        c.prepare("sq", &sql.replace("{t}", "pr_q")).expect("prepare");
        c.prepare("n", &count_sql.replace("{t}", "pr_p")).expect("prepare");
        c.prepare("nq", &count_sql.replace("{t}", "pr_q")).expect("prepare");
        for run in 0..8 {
            let tenant = [3, 8, 3, 11, 5, 8, 1, 3][run];
            let grp = (run % 3 + 1) as i64;
            // A write to the very rows the next execution reads.
            for t in ["pr_p", "pr_q"] {
                c.ok(&format!(
                    "INSERT INTO {t} (tenant_id, grp, id, n, s) VALUES ({tenant}, {grp}, {next_id}, {run}, 'w{life}')"
                ));
            }
            next_id += 1;
            let params = [Some(tenant.to_string()), Some(grp.to_string())];
            let rp = c.execute_prepared("s", &params).unwrap_or_else(|e| panic!("life {life} run {run}: {e}"));
            let rq = c.execute_prepared("sq", &params).expect("the twin");
            assert_eq!(sorted(rp.rows.clone()), sorted(rq.rows.clone()), "life {life} run {run}: tenant {tenant}");
            assert!(
                rp.rows.iter().any(|r| r[0].as_deref() == Some(&(next_id - 1).to_string())),
                "life {life} run {run}: the row just written is missing"
            );
            let np = c.execute_prepared("n", &params[..1]).expect("count");
            let nq = c.execute_prepared("nq", &params[..1]).expect("count");
            assert_eq!(np.rows, nq.rows, "life {life} run {run}: count of tenant {tenant}");
        }
        for name in ["s", "sq", "n", "nq"] {
            c.close_prepared(name);
        }
        drop(c);
        if life == 0 {
            server = server.restart();
        }
    }
    drop(server);
}

/// The fold runs once over the merged rows, so the aggregates with traps in
/// a partial merge answer exactly like the twin: AVG (not an average of
/// averages), COUNT(DISTINCT) (a value in two partitions counted once), and
/// NUMERIC at its declared scale; with GROUP BY, HAVING, ORDER BY and paging.
#[test]
fn aggregates_over_partitions_answer_like_the_twin() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    twins(&mut c, "ag_p", "ag_q", 16);
    for t in ["ag_p", "ag_q"] {
        c.ok(&format!(
            "CREATE TABLE {t}_m (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, price NUMERIC(10,2), qty INT, \
             PRIMARY KEY (tenant_id, id)){}",
            if t == "ag_p" { " PARTITION BY HASH (tenant_id) PARTITIONS 8" } else { "" }
        ));
        let rows: Vec<String> = (1..=24i64)
            .map(|i| {
                let price = if i % 7 == 0 { "NULL".to_string() } else { format!("{}.{:02}", i * 3, (i * 17) % 100) };
                format!("({}, {i}, {price}, {})", i % 9 + 1, i % 5)
            })
            .collect();
        c.ok(&format!("INSERT INTO {t}_m (tenant_id, id, price, qty) VALUES {}", rows.join(", ")));
    }
    for (sql, ordered) in [
        ("SELECT COUNT(DISTINCT s) FROM {t}", false),
        ("SELECT COUNT(DISTINCT grp) FROM {t} WHERE tenant_id IN (2, 5, 9)", false),
        ("SELECT AVG(n), AVG(f), SUM(n), COUNT(n), COUNT(*) FROM {t}", false),
        ("SELECT grp, AVG(n), MIN(s), MAX(s) FROM {t} GROUP BY grp ORDER BY grp", true),
        ("SELECT tenant_id, COUNT(*) FROM {t} GROUP BY tenant_id HAVING SUM(n) > 100 ORDER BY tenant_id DESC LIMIT 4 OFFSET 1", true),
        ("SELECT DISTINCT s FROM {t} ORDER BY s LIMIT 3", true),
        ("SELECT tenant_id, id, n FROM {t} ORDER BY n, tenant_id, grp, id LIMIT 7 OFFSET 5", true),
        ("SELECT tenant_id, grp, id, n * 2 + 1 AS x FROM {t} ORDER BY (n * 2 + 1) DESC, tenant_id, grp, id LIMIT 5", true),
        ("SELECT tenant_id, SUM(n) OVER (PARTITION BY tenant_id) FROM {t} WHERE grp = 2", false),
        ("SELECT SUM(price), AVG(price), MIN(price), MAX(price), COUNT(price) FROM {t}_m", false),
        ("SELECT tenant_id, SUM(price), AVG(price) FROM {t}_m GROUP BY tenant_id ORDER BY tenant_id", true),
        ("SELECT id, price, price * qty FROM {t}_m WHERE price > 20.5 ORDER BY price DESC, id LIMIT 6", true),
        ("SELECT SUM(price) FROM {t}_m WHERE tenant_id = 4", false),
    ] {
        like_twin(&mut c, sql, "ag_p", "ag_q", ordered);
    }
    drop(c);
    drop(server);
}
