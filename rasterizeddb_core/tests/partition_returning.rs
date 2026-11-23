//! `RETURNING` on a partitioned table over the wire (S17,
//! `docs/partitioning.md` §9): `INSERT` (one row, several, `INSERT …
//! SELECT`), `UPDATE` and `DELETE` (routed and fanned out), each answering
//! exactly what a plain twin with the same rows answers: the same rows
//! (compared as multisets, since PostgreSQL gives no order without ORDER BY),
//! the same column names and type OIDs (Describe and payload share one
//! source), and the same values for NULLs, JSONB, NUMERIC scales, defaults,
//! generated and SERIAL columns. Simple and prepared, in autocommit and in
//! blocks; a failed statement changes nothing on either; and a prepared
//! `UPDATE … RETURNING` stays right after a write and after a restart.
//!
//! Bounded: a few dozen rows, fixed statement lists.

mod partition_wire;

use partition_wire::*;

/// Every type the comparison must carry: a SERIAL in the key, NUMERIC at a
/// scale other than the engine's, native JSONB, a default, a generated
/// column, NULLs.
const COLUMNS: &str = "(tenant_id BIGINT NOT NULL, id BIGSERIAL, n INT CHECK (n < 100000), amt NUMERIC(10,2), \
                       doc JSONB, s TEXT DEFAULT 'dflt', g BIGINT GENERATED ALWAYS AS (n * 10) STORED, \
                       PRIMARY KEY (tenant_id, id))";
const P: &str = "rp";
const Q: &str = "rq";

fn sorted(mut rows: Vec<Vec<Option<String>>>) -> Vec<Vec<Option<String>>> {
    rows.sort();
    rows
}

/// `p` partitioned by tenant into `partitions`, `q` its plain twin, the same
/// rows written to both in the same order (so their SERIAL ids agree).
fn twins(c: &mut Client, partitions: u32) {
    c.ok(&format!("CREATE TABLE {P} {COLUMNS} PARTITION BY HASH (tenant_id) PARTITIONS {partitions}"));
    c.ok(&format!("CREATE TABLE {Q} {COLUMNS}"));
    c.ok("CREATE TABLE src (tenant_id BIGINT NOT NULL, n INT, amt NUMERIC(10,2), doc JSONB)");
    let mut rows = Vec::new();
    for t in 1..=10i64 {
        for k in 0..3i64 {
            let n = if (t + k) % 4 == 0 { "NULL".to_string() } else { ((t * 7 + k * 3) % 50).to_string() };
            let amt = if (t + k) % 5 == 0 { "NULL".to_string() } else { format!("{}.{}5", t * 3 + k, k) };
            let doc = match (t + k) % 3 {
                0 => "NULL".to_string(),
                1 => format!("'{{\"t\": {t}, \"tags\": [\"a\", \"b\"]}}'"),
                _ => format!("'[{k}, {{\"x\": null}}]'"),
            };
            rows.push(format!("({t}, {n}, {amt}, {doc})"));
        }
    }
    for table in [P, Q] {
        c.ok(&format!("INSERT INTO {table} (tenant_id, n, amt, doc) VALUES {}", rows.join(", ")));
    }
    c.ok("INSERT INTO src (tenant_id, n, amt, doc) VALUES (2, 5, 1.50, '{\"src\": true}'), (8, NULL, NULL, NULL), \
          (11, 7, 0.07, '[1]')");
}

/// Both tables hold the same rows.
#[track_caller]
fn same_rows(c: &mut Client, label: &str) {
    let all = |c: &mut Client, t: &str| sorted(c.ok(&format!("SELECT * FROM {t}")).rows);
    assert_eq!(all(c, P), all(c, Q), "{label}: stored rows");
}

/// The two answers, as compared: columns with their type OIDs, the rows as
/// a multiset, the command tags; or the SQLSTATE.
fn shape(o: &Outcome) -> Result<(Vec<(String, u32)>, Vec<Vec<Option<String>>>, Vec<String>), String> {
    match o {
        Ok(r) => Ok((
            r.columns.iter().cloned().zip(r.types.iter().copied()).collect(),
            sorted(r.rows.clone()),
            r.tags.clone(),
        )),
        Err(e) => Err(e.code.clone()),
    }
}

/// Two sessions, one per table, so a block on one never sees the other's
/// failure.
struct Pair {
    p: Client,
    q: Client,
}

impl Pair {
    fn new(server: &Server) -> Pair {
        Pair { p: server.connect(), q: server.connect() }
    }
    #[track_caller]
    fn ok(&mut self, sql: &str) {
        self.p.ok(sql);
        self.q.ok(sql);
    }
}

/// Runs `template` on both tables (`{t}` is the table), each through its own
/// session, and compares.
#[track_caller]
fn both(c: &mut Pair, template: &str) -> Outcome {
    let p = c.p.simple(&template.replace("{t}", P));
    let q = c.q.simple(&template.replace("{t}", Q));
    assert_eq!(shape(&p), shape(&q), "`{template}`:\n  partitioned: {p:?}\n  twin:        {q:?}");
    p
}

/// [`both`], and the statement must succeed with a row description.
#[track_caller]
fn both_rows(c: &mut Pair, template: &str) -> Reply {
    let r = both(c, template).unwrap_or_else(|e| panic!("`{template}` failed: {e}"));
    assert!(!r.columns.is_empty(), "`{template}` described no columns");
    r
}

/// Every statement shape, simple-query, autocommit and in blocks, with the
/// stored rows compared after each.
#[test]
fn returning_answers_like_the_twin_in_every_statement_shape() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    twins(&mut c, 7);
    let mut pair = Pair::new(&server);

    let shapes: &[(&str, usize)] = &[
        // INSERT: defaults, the SERIAL, the generated column, NULLs, JSONB.
        ("INSERT INTO {t} (tenant_id, n, amt, doc) VALUES (3, 4, 2.50, '{\"k\": [1, 2]}') RETURNING *", 1),
        ("INSERT INTO {t} (tenant_id) VALUES (12) RETURNING id, n, amt, doc, s, g", 1),
        (
            "INSERT INTO {t} (tenant_id, n, amt, s) VALUES (1, 1, 0.01, 'a'), (5, NULL, 99.99, NULL), (9, 3, NULL, 'c') \
             RETURNING tenant_id, id, s, g, amt",
            3,
        ),
        ("INSERT INTO {t} (tenant_id, n, amt) SELECT tenant_id, n, amt FROM src RETURNING *", 3),
        // Aliases and expressions on INSERT (upstream since 3fa2fc3).
        (
            "INSERT INTO {t} (tenant_id, n, amt) VALUES (6, 8, 1.25), (11, NULL, 2.5) \
             RETURNING id AS k, tenant_id AS t, n * 2 AS dbl, amt + 1, COALESCE(s, 'x') AS s2, g",
            2,
        ),
        // UPDATE routed: one tenant, one partition.
        ("UPDATE {t} SET n = n + 1, amt = amt * 2 WHERE tenant_id = 3 RETURNING id, n, amt, g, doc, s", 4),
        ("UPDATE {t} SET n = 2 WHERE tenant_id = 4 AND id = 10 RETURNING *", 1),
        // UPDATE fan-out, with aliases and expressions over set and unset columns.
        (
            "UPDATE {t} SET amt = 7.25 WHERE n > 20 RETURNING tenant_id, id AS k, amt, amt + 1, n * 2, \
             COALESCE(s, 'x') AS s2, UPPER(s), g",
            0,
        ),
        // SET a = b, and NULLs assigned.
        ("UPDATE {t} SET n = tenant_id WHERE tenant_id IN (1, 6, 9) RETURNING tenant_id, n, g", 0),
        ("UPDATE {t} SET doc = NULL, s = NULL WHERE tenant_id IN (2, 7) RETURNING doc, s, n, amt", 0),
        ("UPDATE {t} SET doc = '{\"new\": {\"deep\": [true, null]}}' WHERE tenant_id = 5 RETURNING doc, id", 0),
        // Empty routes: a never-written tenant, and a contradiction.
        ("UPDATE {t} SET n = 1 WHERE tenant_id = 999 RETURNING id", 0),
        ("DELETE FROM {t} WHERE tenant_id = 1 AND tenant_id = 2 RETURNING id", 0),
        // DELETE routed, then fanned out.
        ("DELETE FROM {t} WHERE tenant_id = 6 RETURNING *", 0),
        ("DELETE FROM {t} WHERE n < 3 RETURNING id, amt, doc, g, n - 1 AS m", 0),
    ];
    for (sql, want) in shapes {
        let r = both_rows(&mut pair, sql);
        if *want > 0 {
            assert_eq!(r.rows.len(), *want, "`{sql}`: {:?}", r.rows);
        }
        same_rows(&mut c, sql);
    }
    // An INSERT … SELECT whose source is empty: a row description and no
    // rows, as PostgreSQL answers (upstream since dedd503).
    let r = both_rows(&mut pair, "INSERT INTO {t} (tenant_id, n) SELECT tenant_id, n FROM src WHERE tenant_id > 100 RETURNING id");
    assert!(r.rows.is_empty());
    // The stored scale and JSONB, as returned, are what a SELECT reads.
    let r = both_rows(&mut pair, "UPDATE {t} SET amt = 1.5 WHERE tenant_id = 12 RETURNING amt, doc, s, g");
    assert_eq!(r.rows, vec![vec![Some("1.50".into()), None, Some("dflt".into()), None]]);

    // In blocks: committed and rolled back, several statements each.
    for end in ["COMMIT", "ROLLBACK"] {
        pair.ok("BEGIN");
        both_rows(&mut pair, "UPDATE {t} SET n = n + 5 WHERE amt > 3 RETURNING tenant_id, id, n, g");
        both_rows(&mut pair, "INSERT INTO {t} (tenant_id, n) VALUES (4, 40), (10, 41) RETURNING id, n, g, s");
        both_rows(&mut pair, "DELETE FROM {t} WHERE doc IS NULL RETURNING tenant_id, id");
        pair.ok(end);
        same_rows(&mut c, end);
    }

    // Prepared: the Describe, then every execution, like the twin's.
    for (sql, runs) in [
        (
            "UPDATE {t} SET n = n + $1 WHERE tenant_id = $2 RETURNING id AS v, n, n * 2, amt",
            vec![vec!["1", "3"], vec!["2", "8"], vec!["-1", "3"], vec!["4", "999"]],
        ),
        ("DELETE FROM {t} WHERE tenant_id = $1 RETURNING *", vec![vec!["8"], vec!["999"], vec!["10"]]),
        (
            "INSERT INTO {t} (tenant_id, n, amt) VALUES ($1, $2, $3) RETURNING id, tenant_id, n, amt, g, s",
            vec![vec!["2", "9", "3.10"], vec!["7", "8", "0.5"]],
        ),
    ] {
        let dp = c.prepare("p", &sql.replace("{t}", P));
        let dq = c.prepare("q", &sql.replace("{t}", Q));
        assert_eq!(shape(&dp), shape(&dq), "Describe of `{sql}`");
        assert!(!dp.as_ref().expect("prepared").columns.is_empty(), "`{sql}` described no columns");
        for params in &runs {
            let params: Vec<Option<String>> = params.iter().map(|p| Some(p.to_string())).collect();
            let rp = c.execute_prepared("p", &params);
            let rq = c.execute_prepared("q", &params);
            assert_eq!(shape(&rp), shape(&rq), "`{sql}` with {params:?}:\n  {rp:?}\n  {rq:?}");
            assert!(rp.is_ok(), "`{sql}` with {params:?}: {rp:?}");
            same_rows(&mut c, sql);
        }
        c.close_prepared("p");
        c.close_prepared("q");
    }
    assert!(misplaced_rows(&server.db, P).is_empty());
    drop(c);
    drop(server);
}

/// A statement with RETURNING that fails changes nothing, on either table,
/// and fails with the twin's SQLSTATE: a duplicate key in the second row of
/// a multi-row insert, a CHECK broken by a fan-out update in a later
/// partition, a RETURNING list naming no column. In a block the failure
/// ends the block, and its earlier statements roll back with it.
#[test]
fn a_failed_returning_statement_changes_nothing() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    twins(&mut c, 7);
    let mut pair = Pair::new(&server);
    let snapshot = |c: &mut Client| sorted(c.ok(&format!("SELECT * FROM {P}")).rows);
    let before = snapshot(&mut c);
    for sql in [
        "INSERT INTO {t} (tenant_id, id, n) VALUES (4, 1000, 1), (1, 1, 2) RETURNING id",
        "UPDATE {t} SET n = n + 99990 WHERE n > 30 RETURNING id, n",
        "UPDATE {t} SET n = 1 WHERE tenant_id IN (1, 2) RETURNING nope",
        "DELETE FROM {t} WHERE tenant_id = 3 RETURNING id, nope",
    ] {
        let p = both(&mut pair, sql);
        assert!(p.is_err(), "`{sql}` succeeded: {p:?}");
        assert_eq!(snapshot(&mut c), before, "`{sql}` changed the partitioned table");
        same_rows(&mut c, sql);
    }
    pair.ok("BEGIN");
    both_rows(&mut pair, "UPDATE {t} SET s = 'in block' WHERE tenant_id = 5 RETURNING id, s");
    let p = both(&mut pair, "UPDATE {t} SET n = n + 99990 WHERE n IS NOT NULL RETURNING id");
    assert!(p.is_err());
    pair.ok("ROLLBACK");
    assert_eq!(snapshot(&mut c), before, "a failed block left a change");
    same_rows(&mut c, "after the failed block");
    drop(c);
    drop(server);
}

/// (d) The Describe of `UPDATE … RETURNING x AS v, n * 2` on a partitioned
/// table is the twin's, names and type OIDs: as a prepared statement's
/// Describe, its portal's, and a simple query's row description; and the
/// values decode as described and are right, checked against values worked
/// out from the rows before the statement, not only against the twin.
#[test]
fn describe_of_update_returning_an_alias_and_an_expression_matches_the_twin() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    twins(&mut c, 7);
    let mut pair = Pair::new(&server);
    let sql = "UPDATE {t} SET n = n + 1 WHERE tenant_id = $1 RETURNING id AS v, n * 2";
    let dp = c.prepare("p", &sql.replace("{t}", P)).expect("prepare");
    let dq = c.prepare("q", &sql.replace("{t}", Q)).expect("prepare");
    assert_eq!(dp.columns, vec!["v".to_string(), "?column?".to_string()], "statement Describe");
    assert_eq!((&dp.columns, &dp.types), (&dq.columns, &dq.types), "statement Describe");
    let int8 = 20;
    assert_eq!(dp.types[0], int8, "id is a BIGSERIAL");

    for tenant in [3i64, 8, 3] {
        let pre: Vec<(String, Option<i64>)> = c
            .ok(&format!("SELECT id, n FROM {P} WHERE tenant_id = {tenant}"))
            .rows
            .into_iter()
            .map(|r| (r[0].clone().unwrap(), r[1].as_ref().map(|n| n.parse().unwrap())))
            .collect();
        let params = [Some(tenant.to_string())];
        let rp = c.execute_prepared("p", &params).expect("execute");
        let rq = c.execute_prepared("q", &params).expect("execute");
        assert_eq!((&rp.columns, &rp.types), (&dp.columns, &dp.types), "portal Describe");
        assert_eq!(shape(&Ok(rp.clone())), shape(&Ok(rq)), "tenant {tenant}");
        let want: Vec<Vec<Option<String>>> =
            pre.iter().map(|(id, n)| vec![Some(id.clone()), n.map(|n| ((n + 1) * 2).to_string())]).collect();
        assert_eq!(sorted(rp.rows), sorted(want), "tenant {tenant}: the values");
    }
    let simple = both_rows(&mut pair, "UPDATE {t} SET n = n + 1 WHERE tenant_id = 5 RETURNING id AS v, n * 2");
    assert_eq!((&simple.columns, &simple.types), (&dp.columns, &dp.types), "simple-query row description");
    c.close_prepared("p");
    c.close_prepared("q");
    same_rows(&mut c, "after");
    drop(c);
    drop(server);
}

/// (e) A prepared `UPDATE … RETURNING` stays right across writes to the very
/// rows it changes and across a restart: every execution's rows, names,
/// types and tag are the twin's, and so is what both store.
#[test]
fn prepared_update_returning_stays_right_after_a_write_and_a_restart() {
    let dir = tempfile::tempdir().expect("dir");
    let mut server = Server::start(dir.path().to_str().unwrap());
    {
        let mut c = server.connect();
        twins(&mut c, 16);
    }
    let sql = "UPDATE {t} SET n = n + $1, s = $2 WHERE tenant_id = $3 RETURNING tenant_id, id, n, s, g, amt";
    for life in 0..2 {
        let mut c = server.connect();
        let dp = c.prepare("p", &sql.replace("{t}", P)).expect("prepare");
        let dq = c.prepare("q", &sql.replace("{t}", Q)).expect("prepare");
        assert_eq!(shape(&Ok(dp)), shape(&Ok(dq)), "life {life}: Describe");
        for run in 0..6usize {
            let tenant = [3, 8, 3, 11, 5, 3][run];
            // A write to the very rows the next execution changes.
            for t in [P, Q] {
                c.ok(&format!("INSERT INTO {t} (tenant_id, n, amt) VALUES ({tenant}, {run}, 1.25)"));
                c.ok(&format!("UPDATE {t} SET amt = amt + 1 WHERE tenant_id = {tenant}"));
            }
            let params = [Some((run as i64 - 2).to_string()), Some(format!("life {life} run {run}")), Some(tenant.to_string())];
            let rp = c.execute_prepared("p", &params);
            let rq = c.execute_prepared("q", &params);
            assert_eq!(shape(&rp), shape(&rq), "life {life} run {run}:\n  {rp:?}\n  {rq:?}");
            let rp = rp.unwrap_or_else(|e| panic!("life {life} run {run}: {e}"));
            assert!(!rp.rows.is_empty(), "life {life} run {run}: the update matched nothing");
            assert!(
                rp.rows.iter().all(|r| r[3].as_deref() == Some(&*format!("life {life} run {run}"))),
                "life {life} run {run}: {:?}",
                rp.rows
            );
            same_rows(&mut c, &format!("life {life} run {run}"));
        }
        c.close_prepared("p");
        c.close_prepared("q");
        drop(c);
        if life == 0 {
            server = server.restart();
        }
    }
    drop(server);
}
