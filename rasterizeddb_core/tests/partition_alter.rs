//! Metadata DDL on partitioned tables (`docs/partitioning.md` §11, "Phase 3
//! P1 as built"): each kind against a plain twin over pgwire, read back
//! after a restart, and what stays refused.
//!
//! Every statement runs on `pa` (partitioned) and then on `pb` (plain, the
//! same columns and constraints): both succeed with equal answers, or both
//! fail with one SQLSTATE. What the partitioned table refuses on purpose is
//! checked on its side only, with 0A000 and "not supported", and runs on
//! neither twin, so they stay equal. After each step the rows both store,
//! read below SQL, must be equal, and so must every catalog view a client
//! introspects with; and again after the server restarts, since a schema
//! that is right in memory and wrong on disk shows only at a reopen.

mod partition_wire;

use partition_wire::*;

const PART: &str = "pa";
const PLAIN: &str = "pb";

fn render(template: &str, table: &str) -> String {
    template.replace("{t}", table)
}

fn unrender(text: &str) -> String {
    text.replace(PART, "{t}").replace(PLAIN, "{t}")
}

/// Two sessions, one per side, over one server.
struct Pair {
    server: Option<Server>,
    p: Option<Client>,
    q: Option<Client>,
}

fn normalize(reply: &Reply, ordered: bool) -> (Vec<String>, Vec<Vec<Option<String>>>, Vec<String>) {
    let cols = reply.columns.iter().zip(&reply.types).map(|(c, t)| format!("{}:{t}", unrender(c))).collect();
    let mut rows: Vec<Vec<Option<String>>> =
        reply.rows.iter().map(|r| r.iter().map(|v| v.as_deref().map(unrender)).collect()).collect();
    if !ordered {
        rows.sort();
    }
    (cols, rows, reply.tags.iter().map(|t| unrender(t)).collect())
}

impl Pair {
    fn start(dir: &std::path::Path) -> Pair {
        let server = Server::start(dir.to_str().unwrap());
        let (p, q) = (server.connect(), server.connect());
        Pair { server: Some(server), p: Some(p), q: Some(q) }
    }

    fn server(&self) -> &Server {
        self.server.as_ref().unwrap()
    }

    fn restart(&mut self) {
        self.p = None;
        self.q = None;
        let server = self.server.take().unwrap().restart();
        self.p = Some(server.connect());
        self.q = Some(server.connect());
        self.server = Some(server);
    }

    /// `template` on both sides; equal answers or one SQLSTATE. Returns the
    /// partitioned side's outcome.
    fn both(&mut self, template: &str) -> Outcome {
        self.both_ordered(template, false)
    }

    fn both_ordered(&mut self, template: &str, ordered: bool) -> Outcome {
        let (sp, sq) = (render(template, PART), render(template, PLAIN));
        let a = self.p.as_mut().unwrap().simple(&sp);
        let b = self.q.as_mut().unwrap().simple(&sq);
        match (&a, &b) {
            (Ok(x), Ok(y)) => assert_eq!(normalize(x, ordered), normalize(y, ordered), "answers differ for `{template}`"),
            (Err(x), Err(y)) => assert_eq!(x.code, y.code, "`{template}` failed differently: {x} / {y}"),
            _ => panic!("`{template}`: partitioned {a:?}, plain {b:?}"),
        }
        a
    }

    /// `template` must succeed on both, alike.
    fn ok(&mut self, template: &str) -> Reply {
        match self.both(template) {
            Ok(reply) => reply,
            Err(e) => panic!("`{template}` failed on both: {e}"),
        }
    }

    /// `template` must fail on both with `code`.
    fn fails(&mut self, template: &str, code: &str) {
        match self.both(template) {
            Err(e) => assert_eq!(e.code, code, "`{template}`: {e}"),
            Ok(r) => panic!("`{template}` succeeded on both: {r:?}"),
        }
    }

    /// `template` must fail on both with one SQLSTATE, whichever: where the
    /// plain table's code is not PostgreSQL's, which is not this file's to
    /// fix.
    fn fails_alike(&mut self, template: &str) {
        if let Ok(r) = self.both(template) {
            panic!("`{template}` succeeded on both: {r:?}");
        }
    }

    /// `template` must be refused by the partitioned side alone (0A000,
    /// "not supported", `needle` in the reason), writing nothing; the twin
    /// does not run it.
    fn refused(&mut self, template: &str, needle: &str) {
        let before = self.state();
        let sql = render(template, PART);
        match self.p.as_mut().unwrap().simple(&sql) {
            Err(e) => {
                assert!(e.is_unsupported(), "`{sql}` failed, but not as a refusal: {e}");
                assert!(e.message.contains(needle), "`{sql}`'s refusal does not say {needle:?}: {e}");
            }
            Ok(r) => panic!("`{sql}` was not refused: {r:?}"),
        }
        assert_eq!(self.state(), before, "`{sql}` was refused but changed something");
    }

    /// What a client sees of a table: its rows, its columns, its indexes,
    /// and its rows read below SQL.
    fn state(&mut self) -> Vec<String> {
        let mut out = Vec::new();
        for (table, client) in [(PART, self.p.as_mut().unwrap())] {
            for q in [
                "SELECT * FROM {t} ORDER BY tenant_id, id",
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
                 WHERE table_name = '{t}' ORDER BY ordinal_position",
                "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{t}' ORDER BY indexname",
            ] {
                out.push(format!("{:?}", client.simple(&render(q, table)).map(|r| normalize(&r, true))));
            }
        }
        out.push(format!("{:?}", stored_rows(&self.server().db, PART)));
        out
    }

    /// Both tables read alike, through SQL, the catalogs and below SQL.
    fn compare(&mut self, at: &str) {
        for q in [
            "SELECT * FROM {t} ORDER BY tenant_id, id",
            "SELECT column_name, data_type, is_nullable, ordinal_position FROM \
             information_schema.columns WHERE table_name = '{t}' ORDER BY ordinal_position",
            "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{t}' ORDER BY indexname",
            "SELECT i.indisunique, i.indisprimary, i.indnatts FROM pg_index i JOIN pg_class r ON i.indrelid = r.oid \
             WHERE r.relname = '{t}' ORDER BY 1, 2, 3",
            "SHOW COLUMNS FROM {t}",
            "DESCRIBE {t}",
            "SELECT relname, relkind FROM pg_class WHERE relname = '{t}'",
            "SELECT count(*) FROM {t}",
        ] {
            let out = self.both_ordered(q, true);
            assert!(out.is_ok(), "{at}: `{q}` failed: {out:?}");
        }
        let db = &self.server().db;
        assert_eq!(stored_rows(db, PART), stored_rows(db, PLAIN), "{at}: the tables store different rows");
        assert!(misplaced_rows(db, PART).is_empty(), "{at}: rows outside their partition");
        // Every partition holds the shell's schema, field for field.
        let pt = db.partitioned_table(PART).expect("still partitioned");
        assert!(pt.broken().is_none(), "{at}: {:?}", pt.broken());
    }
}

impl Drop for Pair {
    fn drop(&mut self) {
        self.p = None;
        self.q = None;
    }
}

const COLUMNS: &str = "(tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, n INT, s TEXT, f DOUBLE PRECISION, \
                       j JSONB, PRIMARY KEY (tenant_id, id))";

fn create(pair: &mut Pair, partitions: u32, rows: bool) {
    pair.p.as_mut().unwrap().ok(&format!("CREATE TABLE {PART} {COLUMNS} PARTITION BY HASH (tenant_id) PARTITIONS {partitions}"));
    pair.q.as_mut().unwrap().ok(&format!("CREATE TABLE {PLAIN} {COLUMNS}"));
    if rows {
        let values: Vec<String> = (0..48)
            .map(|i| {
                let n = if i % 7 == 0 { "NULL".to_string() } else { (i % 11).to_string() };
                let s = if i % 5 == 0 { "NULL".to_string() } else { format!("'w{}'", i % 6) };
                format!("({}, {i}, {n}, {s}, {}, '{{\"k\": {}}}')", i % 9 + 1, i as f64 * 0.5, i % 4)
            })
            .collect();
        pair.ok(&format!("INSERT INTO {{t}} (tenant_id, id, n, s, f, j) VALUES {}", values.join(", ")));
    }
}

/// Every metadata DDL kind on a populated table, each read back against the
/// twin, through two restarts.
#[test]
fn every_metadata_ddl_answers_like_a_plain_twin_and_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut pair = Pair::start(dir.path());
    create(&mut pair, 4, true);
    pair.compare("seeded");

    // Indexes: btree, hash, gin, a path; named, unnamed, IF NOT EXISTS.
    pair.ok("CREATE INDEX {t}_n ON {t} (n)");
    pair.ok("CREATE INDEX IF NOT EXISTS {t}_n ON {t} (n)");
    pair.fails("CREATE INDEX {t}_n ON {t} (s)", "42P07");
    pair.ok("CREATE INDEX {t}_s ON {t} USING hash (s)");
    pair.ok("CREATE INDEX ON {t} (f)");
    pair.ok("CREATE INDEX {t}_j ON {t} USING gin (j)");
    pair.ok("CREATE INDEX {t}_jk ON {t} ((j->>'k'))");
    pair.compare("indexes created");
    for q in [
        "SELECT tenant_id, id FROM {t} WHERE n = 3",
        "SELECT tenant_id, id FROM {t} WHERE s = 'w2'",
        "SELECT tenant_id, id FROM {t} WHERE j @> '{\"k\": 1}'",
        "SELECT tenant_id, id FROM {t} WHERE j->>'k' = '2'",
        "SELECT tenant_id, id FROM {t} WHERE f > 10",
    ] {
        pair.ok(q);
    }

    // UNIQUE: with every key column, accepted and enforced; without, refused.
    pair.ok("CREATE UNIQUE INDEX {t}_uq ON {t} (tenant_id, id, n)");
    pair.refused("CREATE UNIQUE INDEX {t}_bad ON {t} (id, n)", "partition key column");
    pair.refused("ALTER TABLE {t} ADD CONSTRAINT {t}_bad2 UNIQUE (s)", "partition key column");
    pair.fails("ALTER TABLE {t} ADD CONSTRAINT {t}_dup UNIQUE (tenant_id, s)", "23505");
    pair.ok("ALTER TABLE {t} ADD CONSTRAINT {t}_u2 UNIQUE (tenant_id, id, s)");
    pair.compare("unique keys");
    pair.restart();
    pair.compare("unique keys, restarted");
    pair.ok("DROP INDEX {t}_n");
    pair.ok("DROP INDEX IF EXISTS {t}_n");
    pair.ok("DROP INDEX {t}_jk");
    pair.ok("ALTER TABLE {t} DROP CONSTRAINT {t}_u2");
    pair.compare("indexes dropped");

    // Defaults, on a value column and on the key.
    pair.ok("ALTER TABLE {t} ALTER COLUMN n SET DEFAULT 42");
    pair.ok("ALTER TABLE {t} ALTER COLUMN tenant_id SET DEFAULT 3");
    pair.ok("INSERT INTO {t} (id, s) VALUES (100, 'defaulted')");
    pair.ok("SELECT tenant_id, id, n FROM {t} WHERE tenant_id = 3 AND id = 100");
    pair.ok("ALTER TABLE {t} ALTER COLUMN n DROP DEFAULT");
    pair.ok("ALTER TABLE {t} ALTER COLUMN tenant_id DROP DEFAULT");
    pair.ok("INSERT INTO {t} (tenant_id, id) VALUES (4, 101)");
    pair.compare("defaults");

    // CHECK: validated over every partition, enforced, dropped.
    pair.fails("ALTER TABLE {t} ADD CONSTRAINT {t}_c1 CHECK (n < 5)", "23514");
    pair.ok("ALTER TABLE {t} ADD CONSTRAINT {t}_c2 CHECK (n IS NULL OR n >= 0)");
    pair.fails("INSERT INTO {t} (tenant_id, id, n) VALUES (5, 102, -1)", "23514");
    pair.fails("UPDATE {t} SET n = -5 WHERE tenant_id = 2", "23514");
    pair.compare("check");
    pair.restart();
    pair.fails("INSERT INTO {t} (tenant_id, id, n) VALUES (6, 103, -1)", "23514");
    pair.ok("ALTER TABLE {t} DROP CONSTRAINT {t}_c2");
    pair.ok("INSERT INTO {t} (tenant_id, id, n) VALUES (6, 103, -1)");
    pair.compare("check dropped");

    // NOT NULL: validated; DROP NOT NULL rewrites a populated table, and a
    // key column must stay NOT NULL.
    // XX000 on both, where PostgreSQL says 23502: the plain table's code.
    pair.fails_alike("ALTER TABLE {t} ALTER COLUMN n SET NOT NULL");
    pair.ok("UPDATE {t} SET n = 0 WHERE n IS NULL");
    pair.ok("ALTER TABLE {t} ALTER COLUMN n SET NOT NULL");
    pair.fails("INSERT INTO {t} (tenant_id, id, n) VALUES (7, 104, NULL)", "23502");
    pair.ok("ALTER TABLE {t} ALTER COLUMN tenant_id SET NOT NULL");
    pair.refused("ALTER TABLE {t} ALTER COLUMN n DROP NOT NULL", "rewrites every row");
    pair.refused("ALTER TABLE {t} ALTER COLUMN tenant_id DROP NOT NULL", "must be NOT NULL");
    pair.compare("not null");

    // Renames: a value column, the key column, back again.
    pair.ok("ALTER TABLE {t} RENAME COLUMN s TO label");
    pair.ok("SELECT tenant_id, id, label FROM {t} WHERE label = 'w3'");
    pair.ok("ALTER TABLE {t} RENAME COLUMN tenant_id TO tid");
    pair.ok("SELECT tid, id, label FROM {t} WHERE tid = 4");
    pair.ok("INSERT INTO {t} (tid, id, n, label) VALUES (8, 105, 1, 'renamed key')");
    pair.ok("UPDATE {t} SET n = n + 1 WHERE tid = 8");
    pair.restart();
    pair.ok("SELECT tid, id, n, label FROM {t} WHERE tid = 8");
    pair.ok("ALTER TABLE {t} RENAME COLUMN tid TO tenant_id");
    pair.ok("ALTER TABLE {t} RENAME COLUMN label TO s");
    pair.compare("renamed back");

    // DROP COLUMN marks; the key cannot go; a populated ADD COLUMN and a type
    // change rewrite.
    pair.ok("ALTER TABLE {t} DROP COLUMN f");
    pair.fails_alike("SELECT f FROM {t}");
    pair.refused("ALTER TABLE {t} DROP COLUMN tenant_id", "partition key column");
    pair.refused("ALTER TABLE {t} ADD COLUMN extra INT", "rewrites every row");
    pair.refused("ALTER TABLE {t} ADD COLUMN extra INT DEFAULT 5", "rewrites every row");
    pair.refused("ALTER TABLE {t} ALTER COLUMN n TYPE BIGINT", "rewrites every row");
    pair.refused("ALTER TABLE {t} ALTER COLUMN tenant_id TYPE INT", "partition key column");
    pair.ok("ALTER TABLE {t} ADD COLUMN IF NOT EXISTS n INT");
    pair.fails("ALTER TABLE {t} ADD COLUMN n INT", "42701");
    pair.compare("columns dropped");

    // RENAME TO and back, across a restart.
    pair.ok("ALTER TABLE {t} RENAME TO {t}_r");
    pair.fails("SELECT * FROM {t}", "42P01");
    pair.ok("SELECT tenant_id, id, n FROM {t}_r WHERE tenant_id = 2");
    pair.ok("INSERT INTO {t}_r (tenant_id, id, n) VALUES (9, 106, 9)");
    pair.restart();
    {
        let db = &pair.server().db;
        assert!(db.partitioned_table("pa_r").is_some() && db.partitioned_table(PART).is_none());
    }
    pair.ok("SELECT tenant_id, id, n FROM {t}_r WHERE tenant_id = 9");
    pair.ok("ALTER TABLE {t}_r RENAME TO {t}");
    pair.compare("renamed table back");
    pair.restart();
    pair.compare("final restart");
    assert_eq!(files_of_table(dir.path(), PART), Vec::<String>::new(), "files under the logical name");
    assert_eq!(files_of_table(dir.path(), "pa_r"), Vec::<String>::new(), "files under the renamed name");
}

/// On a table with no rows, ADD COLUMN in every metadata-only form (and
/// DROP NOT NULL on a value column) runs as the plain table's does, and the
/// rows written after read back alike, across a restart.
#[test]
fn metadata_only_add_column_and_drop_not_null_on_an_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let mut pair = Pair::start(dir.path());
    create(&mut pair, 3, false);
    pair.ok("ALTER TABLE {t} ADD COLUMN extra INT");
    pair.ok("ALTER TABLE {t} ADD COLUMN filled BIGINT NOT NULL DEFAULT 7");
    pair.ok("ALTER TABLE {t} ADD COLUMN dec NUMERIC(10, 2)");
    pair.ok("ALTER TABLE {t} ADD COLUMN twice INT GENERATED ALWAYS AS (n * 2) STORED");
    pair.ok("ALTER TABLE {t} ADD COLUMN must INT NOT NULL");
    pair.ok("ALTER TABLE {t} ALTER COLUMN must DROP NOT NULL");
    pair.ok("ALTER TABLE {t} ALTER COLUMN filled DROP NOT NULL");
    pair.refused("ALTER TABLE {t} ALTER COLUMN tenant_id DROP NOT NULL", "must be NOT NULL");
    pair.refused("ALTER TABLE {t} ADD COLUMN doc JSON", "shadow tables");
    pair.compare("columns added");
    pair.ok(
        "INSERT INTO {t} (tenant_id, id, n, s, extra, dec, must) VALUES (1, 1, 5, 'a', NULL, 12.34, NULL), \
         (2, 2, NULL, NULL, 3, NULL, 4), (3, 3, 1, 'c', 9, 0.5, 1)",
    );
    pair.ok("INSERT INTO {t} (tenant_id, id, filled) VALUES (4, 4, NULL)");
    pair.compare("rows after the columns");
    pair.restart();
    pair.compare("restarted");
    pair.ok("SELECT tenant_id, extra, filled, dec, twice, must FROM {t} WHERE tenant_id = 2");
    // Once it holds rows, the same statement rewrites, and is refused.
    pair.refused("ALTER TABLE {t} ADD COLUMN late INT", "rewrites every row");
}

/// A prepared statement made before a DDL and executed after it answers on
/// the partitioned table as on its twin: the same columns, types and decoded
/// values, or the same SQLSTATE (the shape change, `rdb.shape_change`), and
/// again on the execution after that.
#[test]
fn prepared_statements_answer_like_the_twin_across_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let mut pair = Pair::start(dir.path());
    create(&mut pair, 4, true);
    let statements = [
        ("sel", "SELECT * FROM {t} WHERE tenant_id = $1"),
        ("cnt", "SELECT count(*), sum(n) FROM {t} WHERE tenant_id = $1"),
        ("ins", "INSERT INTO {t} (tenant_id, id, n) VALUES ($1, $2, $3)"),
        ("upd", "UPDATE {t} SET n = n + 1 WHERE tenant_id = $1 RETURNING tenant_id, id, n"),
    ];
    let mut next_id = 1000;
    let mut run_all = |pair: &mut Pair, at: &str| {
        for (name, sql) in statements {
            for tenant in [2, 5] {
                let params: Vec<Option<String>> = if name == "ins" {
                    next_id += 1;
                    vec![Some(tenant.to_string()), Some(next_id.to_string()), Some("3".into())]
                } else {
                    vec![Some(tenant.to_string())]
                };
                let a = pair.p.as_mut().unwrap().execute_prepared(name, &params);
                let b = pair.q.as_mut().unwrap().execute_prepared(name, &params);
                match (&a, &b) {
                    (Ok(x), Ok(y)) => assert_eq!(normalize(x, false), normalize(y, false), "{at}: `{sql}` {params:?}"),
                    (Err(x), Err(y)) => assert_eq!(x.code, y.code, "{at}: `{sql}`: {x} / {y}"),
                    _ => panic!("{at}: `{sql}` {params:?}: partitioned {a:?}, plain {b:?}"),
                }
                if let Ok(x) = &a {
                    if name == "sel" {
                        assert!(!x.rows.is_empty(), "{at}: no rows decoded");
                    }
                }
            }
        }
    };
    for (name, sql) in statements {
        let a = pair.p.as_mut().unwrap().prepare(name, &render(sql, PART)).expect("prepare");
        let b = pair.q.as_mut().unwrap().prepare(name, &render(sql, PLAIN)).expect("prepare");
        assert_eq!(normalize(&a, false), normalize(&b, false), "describe of `{sql}`");
    }
    run_all(&mut pair, "before");
    for ddl in [
        "CREATE INDEX {t}_n ON {t} (n)",
        "ALTER TABLE {t} RENAME COLUMN s TO label",
        "ALTER TABLE {t} ADD CONSTRAINT {t}_c CHECK (n >= 0 OR n IS NULL)",
        "ALTER TABLE {t} ALTER COLUMN n SET DEFAULT 1",
        "ALTER TABLE {t} DROP COLUMN f",
        "DROP INDEX {t}_n",
        "ALTER TABLE {t} RENAME COLUMN label TO s",
    ] {
        pair.ok(ddl);
        run_all(&mut pair, ddl);
        run_all(&mut pair, &format!("{ddl}, again"));
        pair.compare(ddl);
    }
    pair.restart();
    for (name, sql) in statements {
        pair.p.as_mut().unwrap().prepare(name, &render(sql, PART)).expect("prepare");
        pair.q.as_mut().unwrap().prepare(name, &render(sql, PLAIN)).expect("prepare");
    }
    run_all(&mut pair, "after a restart");
    pair.compare("after a restart");
}

fn seed_one(server: &Server, partitions: u32) {
    let mut c = server.connect();
    c.ok(&format!("CREATE TABLE {PART} {COLUMNS} PARTITION BY HASH (tenant_id) PARTITIONS {partitions}"));
    let values: Vec<String> = (0..40).map(|i| format!("({}, {i}, {}, 'w', 1.0)", i % 8 + 1, i % 13)).collect();
    c.ok(&format!("INSERT INTO {PART} (tenant_id, id, n, s, f) VALUES {}", values.join(", ")));
}

/// What a session can see of the table, read by a session holding nothing.
fn snapshot(server: &Server) -> String {
    let mut c = server.connect();
    let mut out = Vec::new();
    for q in [
        "SELECT * FROM pa ORDER BY tenant_id, id",
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'pa' \
         ORDER BY ordinal_position",
        "SELECT indexname FROM pg_indexes WHERE tablename = 'pa' ORDER BY indexname",
    ] {
        out.push(format!("{:?}", c.simple(q).map(|r| r.rows)));
    }
    out.push(format!("{:?}", stored_rows(&server.db, PART)));
    out.join("\n")
}

/// The DDL takes ACCESS EXCLUSIVE on the logical name, so it queues behind
/// a block that read the table or wrote it, as on a plain table: under
/// `lock_timeout` it fails with 55P03, under `statement_timeout` with 57014,
/// both having written nothing; without a bound it waits and then runs, and
/// a reader that arrives while it waits queues behind it (PostgreSQL's
/// rule).
///
/// Mutation: `relation_for` locks a partition's physical name instead of
/// the logical one, so the statements' locks never meet the DDL's: the DDL
/// runs beside the open block.
#[test]
fn a_ddl_queues_behind_the_tables_users_and_a_timeout_writes_nothing() {
    use std::sync::mpsc;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path().to_str().unwrap());
    seed_one(&server, 4);
    let mut a = server.connect();
    let mut b = server.connect();
    let ddls = [
        "CREATE INDEX pa_n ON pa (n)",
        "ALTER TABLE pa ADD CONSTRAINT pa_c CHECK (n >= 0)",
        "ALTER TABLE pa RENAME COLUMN s TO label",
        "ALTER TABLE pa ALTER COLUMN n SET DEFAULT 4",
        "ALTER TABLE pa DROP COLUMN f",
        "ALTER TABLE pa RENAME TO pa2",
        "CREATE UNIQUE INDEX pa_u ON pa (tenant_id, id, n)",
    ];
    let before = snapshot(&server);
    // A reading block, then a writing one.
    for (open, what) in [("SELECT count(*) FROM pa", "a reader"), ("INSERT INTO pa (tenant_id, id) VALUES (3, 999)", "a writer")] {
        a.ok("BEGIN");
        a.ok(open);
        b.ok("SET lock_timeout = '200ms'");
        for ddl in ddls {
            let e = b.simple(ddl).expect_err(ddl);
            assert_eq!(e.code, "55P03", "`{ddl}` beside {what}: {e}");
        }
        b.ok("SET lock_timeout = 0");
        b.ok("SET statement_timeout = '200ms'");
        let e = b.simple(ddls[1]).expect_err("statement_timeout");
        assert_eq!(e.code, "57014", "beside {what}: {e}");
        b.ok("SET statement_timeout = 0");
        a.ok("ROLLBACK");
        assert_eq!(snapshot(&server), before, "a refused DDL beside {what} changed something");
    }

    // Unbounded: the DDL waits for the block, and a reader arriving after it
    // waits for the DDL.
    a.ok("BEGIN");
    a.ok("SELECT count(*) FROM pa");
    let (done_tx, done_rx) = mpsc::channel::<(&str, Outcome)>();
    let tx = done_tx.clone();
    let ddl_thread = std::thread::spawn(move || {
        let r = b.simple("ALTER TABLE pa ADD CONSTRAINT pa_c CHECK (n >= 0)");
        tx.send(("ddl", r)).unwrap();
        b
    });
    assert!(done_rx.recv_timeout(Duration::from_millis(400)).is_err(), "the DDL did not wait for the block");
    let mut c = server.connect();
    let reader = std::thread::spawn(move || {
        let r = c.simple("SELECT count(*) FROM pa");
        done_tx.send(("reader", r)).unwrap();
        c
    });
    assert!(done_rx.recv_timeout(Duration::from_millis(400)).is_err(), "the reader did not queue behind the DDL");
    a.ok("COMMIT");
    let mut seen = Vec::new();
    for _ in 0..2 {
        let (who, r) = done_rx.recv_timeout(Duration::from_secs(60)).expect("both finish");
        assert!(r.is_ok(), "{who}: {r:?}");
        seen.push(who);
    }
    assert_eq!(seen, ["ddl", "reader"], "the reader ran ahead of the queued DDL");
    let _ = (ddl_thread.join().unwrap(), reader.join().unwrap());
    let e = a.simple("INSERT INTO pa (tenant_id, id, n) VALUES (1, 500, -1)").expect_err("the CHECK holds");
    assert_eq!(e.code, "23514");
}

/// Writers on many tenants run through a stream of DDL: each statement
/// waits for the DDL or completes before it, and afterwards (and after a
/// restart) every acknowledged row is stored once with its last
/// acknowledged value, in its partition, and the indexes answer as scans.
#[test]
fn writers_through_a_stream_of_ddl_lose_and_mix_nothing() {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let mut server = Server::start(dir.path().to_str().unwrap());
    seed_one(&server, 6);
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for w in 0..4i64 {
        let mut c = server.connect();
        let stop = stop.clone();
        writers.push(std::thread::spawn(move || {
            let mut model: BTreeMap<(i64, i64), i64> = BTreeMap::new();
            let mut i = 0i64;
            while !stop.load(Ordering::Relaxed) && i < 400 {
                let tenant = 100 + w * 10 + i % 7;
                let id = 10_000 + i;
                c.ok(&format!("INSERT INTO pa (tenant_id, id, n) VALUES ({tenant}, {id}, {i})"));
                model.insert((tenant, id), i);
                if i % 3 == 0 {
                    let prev = (tenant, 10_000 + i / 2);
                    if let Some(n) = model.get_mut(&prev) {
                        let r = c.ok(&format!("UPDATE pa SET n = n + 1 WHERE tenant_id = {} AND id = {}", prev.0, prev.1));
                        assert_eq!(r.tags, ["UPDATE 1"], "{prev:?}");
                        *n += 1;
                    }
                }
                i += 1;
            }
            model
        }));
    }
    let mut d = server.connect();
    let ddls = [
        "CREATE INDEX pa_n ON pa (n)",
        "ALTER TABLE pa ADD CONSTRAINT pa_c CHECK (n >= 0)",
        "ALTER TABLE pa ALTER COLUMN s SET DEFAULT 'dflt'",
        "CREATE UNIQUE INDEX pa_u ON pa (tenant_id, id, n)",
        "ALTER TABLE pa RENAME COLUMN f TO g",
        "ALTER TABLE pa DROP CONSTRAINT pa_u",
        "ALTER TABLE pa ALTER COLUMN s DROP DEFAULT",
        "ALTER TABLE pa RENAME COLUMN g TO f",
        "ALTER TABLE pa DROP CONSTRAINT pa_c",
        "DROP INDEX pa_n",
    ];
    for round in 0..3 {
        for ddl in ddls {
            d.ok(ddl);
            std::thread::sleep(std::time::Duration::from_millis(5 + round));
        }
    }
    stop.store(true, Ordering::Relaxed);
    let mut model: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    for w in writers {
        model.extend(w.join().expect("a writer failed"));
    }
    assert!(model.len() > 50, "the writers wrote too little to test anything: {}", model.len());
    d.ok("CREATE INDEX pa_n ON pa (n)");
    drop(d);
    for pass in ["live", "restarted"] {
        let mut c = server.connect();
        let rows = c.ok("SELECT tenant_id, id, n FROM pa WHERE tenant_id >= 100 ORDER BY tenant_id, id").rows;
        let got: BTreeMap<(i64, i64), i64> = rows
            .iter()
            .map(|r| {
                let v = |i: usize| r[i].as_deref().unwrap().parse::<i64>().unwrap();
                ((v(0), v(1)), v(2))
            })
            .collect();
        assert_eq!(rows.len(), got.len(), "{pass}: a row stored twice");
        assert_eq!(got, model, "{pass}: the stored rows are not the acknowledged ones");
        for n in [0i64, 5, 17, 101, 250] {
            let a = c.ok(&format!("SELECT tenant_id, id FROM pa WHERE n = {n} ORDER BY tenant_id, id")).rows;
            let b = c.ok(&format!("SELECT tenant_id, id FROM pa WHERE n + 0 = {n} ORDER BY tenant_id, id")).rows;
            assert_eq!(a, b, "{pass}: the index on n answers otherwise than a scan for n = {n}");
        }
        assert!(misplaced_rows(&server.db, PART).is_empty(), "{pass}: rows outside their partition");
        drop(c);
        if pass == "live" {
            server = server.restart();
        }
    }
}

/// SET NOT NULL on a column followed by others, then a rename of an
/// earlier column: the tombstoned companion must be the one marked.
#[test]
fn set_not_null_marks_its_own_companion() {
    let dir = tempfile::tempdir().unwrap();
    let mut pair = Pair::start(dir.path());
    let cols = "(tenant_id BIGINT NOT NULL, grp BIGINT NOT NULL, id BIGINT NOT NULL, n INT, s TEXT, f DOUBLE PRECISION, \
                junk INT, PRIMARY KEY (tenant_id, grp, id))";
    pair.p.as_mut().unwrap().ok(&format!("CREATE TABLE {PART} {cols} PARTITION BY HASH (tenant_id) PARTITIONS 7"));
    pair.q.as_mut().unwrap().ok(&format!("CREATE TABLE {PLAIN} {cols}"));
    pair.ok("INSERT INTO {t} (tenant_id, grp, id, n, s, f) VALUES (1, 1, 1, 1, 'a', 1.5), (2, 1, 2, NULL, NULL, 2.5)");
    pair.ok("CREATE INDEX {t}_ix_n ON {t} USING btree (n)");
    pair.ok("ALTER TABLE {t} ALTER COLUMN f SET NOT NULL");
    pair.compare("f set not null");
    pair.ok("ALTER TABLE {t} RENAME COLUMN s TO s_renamed");
    pair.compare("s renamed");
    pair.restart();
    pair.compare("restarted");
}


/// What the studio and other clients introspect with (`SHOW TABLES`, `SHOW
/// COLUMNS`, `SHOW TABLE STATS`, `SHOW SCHEMA MAP`, `SHOW PARTITIONS`,
/// `pg_indexes`, `information_schema.columns`) shows the logical table's new
/// state after DDL, under its new name, before and after a restart, and
/// never a partition.
#[test]
fn introspection_shows_the_new_state_and_never_a_partition() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = Server::start(dir.path().to_str().unwrap());
    seed_one(&server, 5);
    {
        let mut c = server.connect();
        for ddl in [
            "CREATE INDEX pa_n ON pa (n)",
            "ALTER TABLE pa RENAME COLUMN s TO label",
            "ALTER TABLE pa DROP COLUMN f",
            "ALTER TABLE pa RENAME TO pa2",
        ] {
            c.ok(ddl);
        }
    }
    for pass in ["live", "restarted"] {
        let mut c = server.connect();
        let text = |r: &Reply| format!("{:?} {:?}", r.columns, r.rows);
        let tables = c.ok("SHOW TABLES");
        let t = text(&tables);
        assert!(t.contains("pa2") && !t.contains("\"pa\"") && !t.contains("__pt_"), "{pass}: SHOW TABLES {t}");
        let shown = c.ok("SHOW COLUMNS FROM pa2");
        let at = |name: &str| shown.columns.iter().position(|c| c == name).unwrap();
        let (name_at, dropped_at) = (at("column_name"), at("is_dropped"));
        let live: Vec<String> = shown
            .rows
            .iter()
            .filter(|r| r[dropped_at].as_deref() == Some("f"))
            .filter_map(|r| r[name_at].clone())
            .filter(|n| !n.starts_with("__null_"))
            .collect();
        assert_eq!(live, ["tenant_id", "id", "n", "label", "j"], "{pass}: SHOW COLUMNS {:?}", shown.rows);
        let stats = text(&c.ok("SHOW TABLE STATS pa2"));
        assert!(stats.contains("pa2") && !stats.contains("__pt_"), "{pass}: SHOW TABLE STATS {stats}");
        let map = text(&c.ok("SHOW SCHEMA MAP"));
        // Foreign-key edges: none here, and never a partition.
        assert!(!map.contains("__pt_"), "{pass}: SHOW SCHEMA MAP {map}");
        assert_eq!(c.ok("SHOW PARTITIONS pa2").rows.len(), 5, "{pass}: SHOW PARTITIONS");
        assert!(c.simple("SHOW PARTITIONS pa").is_err(), "{pass}: the old name still answers");
        let idx = text(&c.ok("SELECT indexname, tablename FROM pg_indexes WHERE tablename = 'pa2'"));
        assert!(idx.contains("pa_n"), "{pass}: pg_indexes {idx}");
        let all = text(&c.ok("SELECT tablename FROM pg_indexes"));
        assert!(!all.contains("__pt_"), "{pass}: pg_indexes lists a partition: {all}");
        let names: Vec<String> = c
            .ok("SELECT column_name FROM information_schema.columns WHERE table_name = 'pa2' ORDER BY ordinal_position")
            .rows
            .into_iter()
            .map(|r| r[0].clone().unwrap())
            .collect();
        assert_eq!(names, ["tenant_id", "id", "n", "label", "j"], "{pass}: information_schema.columns");
        let classes = text(&c.ok("SELECT relname FROM pg_class"));
        assert!(!classes.contains("__pt_") && classes.contains("pa2"), "{pass}: pg_class {classes}");
        drop(c);
        if pass == "live" {
            server = server.restart();
        }
    }
}

/// Over the wire, through the statement's lock set and the partitioned
/// refusal it asks before any lock: constraint names starting with a
/// multi-byte character, bare and quoted, a quoted name with a space, and a
/// keyword as a quoted name, on both tables alike; the partitioned rule
/// still refuses a UNIQUE without the key, IF NOT EXISTS too. None panics.
#[test]
fn non_ascii_and_quoted_constraint_names_over_the_wire() {
    let dir = tempfile::tempdir().unwrap();
    let mut pair = Pair::start(dir.path());
    create(&mut pair, 3, true);
    for ddl in [
        "ALTER TABLE {t} ADD CONSTRAINT 価格_chk CHECK (id >= 0)",
        "ALTER TABLE {t} ADD CONSTRAINT aé_chk CHECK (id < 100000)",
        "ALTER TABLE {t} ADD CONSTRAINT \"名\" UNIQUE (tenant_id, id, s)",
        "ALTER TABLE {t} ADD CONSTRAINT IF NOT EXISTS \"名2\" UNIQUE (tenant_id, id, n)",
        "CREATE UNIQUE INDEX IF NOT EXISTS {t}_索引 ON {t} (tenant_id, id, f)",
        "ALTER TABLE {t} ADD CONSTRAINT \"my key\" UNIQUE (tenant_id, id, n, s)",
        "ALTER TABLE {t} ADD CONSTRAINT \"unique\" EXCLUDE USING gist (id WITH =)",
    ] {
        pair.ok(ddl);
    }
    pair.fails_alike("INSERT INTO {t} (tenant_id, id) VALUES (1, -5)");
    pair.compare("non-ASCII names");
    pair.refused("ALTER TABLE {t} ADD CONSTRAINT 価格_u UNIQUE (id)", "partition key column");
    pair.refused("CREATE UNIQUE INDEX IF NOT EXISTS {t}_価格 ON {t} (id)", "partition key column");
    pair.refused("ALTER TABLE {t} ADD CONSTRAINT IF NOT EXISTS \"価 格\" UNIQUE (n)", "partition key column");
    pair.restart();
    pair.compare("non-ASCII names, restarted");
}

/// One row-lock case: `create` (with `{t}`), the rows, the row A locks
/// (`lock` is the WHERE that names it), and the DDL A runs in its block.
struct LockCase {
    create: &'static str,
    insert: &'static str,
    lock: &'static str,
    ddl: &'static str,
}

/// A FOR UPDATE row lock taken in a block, then a DDL in the same block
/// that would leave the lock keyed by something no lookup uses: the table's
/// old name (RENAME TO, DROP TABLE) or an identifying key that changed (a
/// PRIMARY KEY added or dropped, NOT NULL set or dropped where that changes
/// the chosen key). Each is refused with 55006, nothing changed, and the
/// locked row stays protected (another session's UPDATE of it is refused
/// while the block is open). A control runs each DDL in a block holding no
/// row lock: it succeeds. A DDL that keeps the key (a CHECK, a UNIQUE the
/// primary key still outranks) runs beside the lock, and the row stays
/// protected. Plain and partitioned tables alike.
///
/// Mutation: the orphan check off in `execute_alter_table_action` and
/// `execute_drop_table` (the DDL succeeds, and B's UPDATE then writes the
/// row A holds: the lost update).
#[test]
fn a_ddl_that_would_orphan_the_blocks_row_locks_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path().to_str().unwrap());
    let part = " PARTITION BY HASH (a) PARTITIONS 4";
    // The key that names a row: the primary key; else the NOT NULL unique
    // key with the fewest columns. `uq` is named by (a, b, v) until c is NOT
    // NULL, then by (a, c); `uq_nn` the other way round.
    let pk = "(a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT, c BIGINT, PRIMARY KEY (a, b))";
    let uq = "(a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, c BIGINT, UNIQUE (a, b, v), UNIQUE (a, c))";
    let uq_nn = "(a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, c BIGINT NOT NULL, UNIQUE (a, b, v), UNIQUE (a, c))";
    let rows = "INSERT INTO {t} (a, b, v, c) VALUES (1, 1, 1, 1), (2, 2, 2, 2)";
    let orphaning = [
        LockCase { create: pk, insert: rows, lock: "a = 1 AND b = 1", ddl: "ALTER TABLE {t} RENAME TO {t}_2" },
        LockCase { create: pk, insert: rows, lock: "a = 1 AND b = 1", ddl: "DROP TABLE {t}" },
        LockCase { create: pk, insert: rows, lock: "a = 1 AND b = 1", ddl: "ALTER TABLE {t} DROP CONSTRAINT {t}_pkey" },
        LockCase {
            create: uq,
            insert: rows,
            lock: "a = 1 AND b = 1",
            ddl: "ALTER TABLE {t} ADD CONSTRAINT {t}_pkey PRIMARY KEY (a, b)",
        },
        LockCase { create: uq, insert: rows, lock: "a = 1 AND b = 1", ddl: "ALTER TABLE {t} ALTER COLUMN c SET NOT NULL" },
    ];
    // Plain only: on a partitioned table with rows, DROP NOT NULL rewrites
    // and is refused (0A000) before this check.
    let plain_only = [LockCase { create: uq_nn, insert: rows, lock: "a = 1 AND c = 1", ddl: "ALTER TABLE {t} ALTER COLUMN c DROP NOT NULL" }];
    let keeping = [
        LockCase { create: pk, insert: rows, lock: "a = 1 AND b = 1", ddl: "ALTER TABLE {t} ADD CONSTRAINT {t}_c CHECK (v >= 0)" },
        LockCase { create: pk, insert: rows, lock: "a = 1 AND b = 1", ddl: "ALTER TABLE {t} ADD CONSTRAINT {t}_u UNIQUE (a, b, v)" },
        LockCase { create: pk, insert: rows, lock: "a = 1 AND b = 1", ddl: "CREATE INDEX {t}_v ON {t} (v)" },
    ];
    let mut n = 0;
    let mut next = |kind: &str| {
        n += 1;
        format!("rl_{kind}_{n}")
    };
    let mut run = |case: &LockCase, partitioned: bool, lock_rows: bool, expect_refused: bool| {
        let t = next(if partitioned { "p" } else { "q" });
        let r = |s: &str| s.replace("{t}", &t);
        let mut setup = server.connect();
        setup.ok(&format!("CREATE TABLE {t} {}{}", r(case.create), if partitioned { part } else { "" }));
        setup.ok(&r(case.insert));
        let mut a = server.connect();
        let mut b = server.connect();
        b.ok("SET lock_timeout = '300ms'");
        a.ok("BEGIN");
        if lock_rows {
            a.ok(&format!("SELECT * FROM {t} WHERE {} FOR UPDATE", case.lock));
        } else {
            a.ok(&format!("SELECT * FROM {t} WHERE {}", case.lock));
        }
        let ddl = r(case.ddl);
        let out = a.simple(&ddl);
        let label = format!("{} `{ddl}` (locks {lock_rows})", if partitioned { "partitioned" } else { "plain" });
        if expect_refused {
            let e = out.expect_err(&label);
            assert_eq!(e.code, "55006", "{label}: {e}");
            assert!(e.message.contains("row locks"), "{label}: {e}");
            a.simple("ROLLBACK").ok();
            // Nothing changed: the table is there under its name, its row
            // too, and B can lock and write it now that A is gone.
            assert_eq!(setup.ok(&format!("SELECT count(*) FROM {t}")).rows, vec![vec![Some("2".to_string())]], "{label}");
            b.ok(&format!("UPDATE {t} SET v = 9 WHERE {}", case.lock));
        } else {
            out.unwrap_or_else(|e| panic!("{label}: {e}"));
            if lock_rows {
                // The lock still holds: B cannot write A's row.
                let target = if ddl.contains("RENAME TO") { format!("{t}_2") } else { t.clone() };
                let e = b.simple(&format!("UPDATE {target} SET v = 9 WHERE {}", case.lock));
                assert!(e.is_err(), "{label}: B wrote a row A holds: {e:?}");
            }
            a.ok("COMMIT");
        }
        drop((a, b, setup));
    };
    for partitioned in [false, true] {
        for case in &orphaning {
            run(case, partitioned, true, true);
            run(case, partitioned, false, false);
        }
        for case in &keeping {
            run(case, partitioned, true, false);
        }
    }
    for case in &plain_only {
        run(case, false, true, true);
        run(case, false, false, false);
    }
    drop(run);
    // Another session's row locks are not this check's: B's RENAME waits for
    // A's ROW EXCLUSIVE and times out (55P03), as in PostgreSQL, and goes
    // through once A ends.
    for partitioned in [false, true] {
        let t = next(if partitioned { "p" } else { "q" });
        let mut setup = server.connect();
        setup.ok(&format!("CREATE TABLE {t} {pk}{}", if partitioned { part } else { "" }));
        setup.ok(&rows.replace("{t}", &t));
        let mut a = server.connect();
        let mut b = server.connect();
        b.ok("SET lock_timeout = '300ms'");
        a.ok("BEGIN");
        a.ok(&format!("SELECT * FROM {t} WHERE a = 1 AND b = 1 FOR UPDATE"));
        let e = b.simple(&format!("ALTER TABLE {t} RENAME TO {t}_2")).expect_err("B's RENAME under A's row locks");
        assert_eq!(e.code, "55P03", "partitioned {partitioned}: B's RENAME under A's row locks: {e}");
        a.ok("COMMIT");
        b.ok(&format!("ALTER TABLE {t} RENAME TO {t}_2"));
        drop((a, b, setup));
    }
}

/// `DROP CONSTRAINT` of a name that is not there is PostgreSQL's 42704,
/// bare or quoted, plain or partitioned; `IF EXISTS` makes it a no-op. It
/// used to report success, so a client that misspelled a key, or named one
/// with a space, believed it gone while it went on refusing rows.
#[test]
fn dropping_a_constraint_that_is_not_there_is_42704() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    for (t, part) in [("dc_q", ""), ("dc_p", " PARTITION BY HASH (a) PARTITIONS 4")] {
        c.ok(&format!("CREATE TABLE {t} (a BIGINT NOT NULL, b BIGINT, PRIMARY KEY (a)){part}"));
        c.ok(&format!("ALTER TABLE {t} ADD CONSTRAINT \"my key\" UNIQUE (a, b)"));
        for name in ["nope", "\"my\"", "\"my key 2\"", "\"名\""] {
            let e = c.simple(&format!("ALTER TABLE {t} DROP CONSTRAINT {name}")).expect_err(name);
            assert_eq!(e.code, "42704", "{t} {name}: {e}");
            let bare = name.trim_matches('"');
            assert_eq!(e.message, format!("constraint \"{bare}\" of relation \"{t}\" does not exist"), "{t}");
            c.ok(&format!("ALTER TABLE {t} DROP CONSTRAINT IF EXISTS {name}"));
        }
        c.ok(&format!("ALTER TABLE {t} DROP CONSTRAINT \"my key\""));
        let e = c.simple(&format!("ALTER TABLE {t} DROP CONSTRAINT \"my key\"")).expect_err("dropped twice");
        assert_eq!(e.code, "42704", "{t}: {e}");
    }
}

/// How a client sees a table's keys: `pg_index`, and per column `SHOW
/// COLUMNS`' name, nullable, is_unique and is_primary_key.
fn keys_of(c: &mut Client, t: &str) -> (Vec<Vec<Option<String>>>, Vec<Vec<Option<String>>>) {
    let index = c
        .ok(&format!(
            "SELECT i.indisunique, i.indisprimary, i.indnatts FROM pg_index i JOIN pg_class r ON i.indrelid = r.oid \
             WHERE r.relname = '{t}' ORDER BY 1, 2, 3"
        ))
        .rows;
    let shown = c.ok(&format!("SHOW COLUMNS FROM {t}"));
    let at = |name: &str| shown.columns.iter().position(|c| c == name).unwrap();
    let picks = [at("column_name"), at("nullable"), at("is_unique"), at("is_primary_key")];
    (index, shown.rows.iter().map(|r| picks.iter().map(|&i| r[i].clone()).collect()).collect())
}

fn is_primary(c: &mut Client, t: &str, column: &str) -> bool {
    let (index, columns) = keys_of(c, t);
    let flagged = columns.iter().find(|r| r[0].as_deref() == Some(column)).unwrap()[3].as_deref() == Some("t");
    let indexed = index.iter().any(|r| r[1].as_deref() == Some("t"));
    assert_eq!(flagged, indexed, "{t}: SHOW COLUMNS and pg_index disagree: {columns:?} {index:?}");
    flagged
}

fn expect_code(c: &mut Client, sql: &str, code: &str) {
    let e = c.simple(sql).expect_err(sql);
    assert_eq!(e.code, code, "{sql}: {e}");
}

/// `ADD [CONSTRAINT n] PRIMARY KEY (…)` makes the key the table's, as in
/// PostgreSQL: NOT NULL (23502 over a NULL), a second one refused (42P16), a
/// one-column one recorded as `CREATE TABLE` records it, and dropped with
/// its constraint. Plain and partitioned read alike, across a restart.
#[test]
fn add_primary_key_makes_the_tables_primary_key() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = Server::start(dir.path().to_str().unwrap());
    let kinds = [("apk_q", ""), ("apk_p", " PARTITION BY HASH (a) PARTITIONS 4")];
    {
        let mut c = server.connect();
        for (t, part) in kinds {
            // A partition key column is NOT NULL; `b` and plain-only `apk_n`
            // below are where NOT NULL is made.
            c.ok(&format!("CREATE TABLE {t} (a BIGINT NOT NULL, b BIGINT, v BIGINT){part}"));
            c.ok(&format!("INSERT INTO {t} (a, b, v) VALUES (1, 1, 1), (2, NULL, 2)"));
            expect_code(&mut c, &format!("ALTER TABLE {t} ADD PRIMARY KEY (a, b)"), "23502");
            c.ok(&format!("ALTER TABLE {t} ADD CONSTRAINT {t}_pkey PRIMARY KEY (a)"));
            assert!(is_primary(&mut c, t, "a"), "{t}");
            expect_code(&mut c, &format!("ALTER TABLE {t} ADD PRIMARY KEY (a, v)"), "42P16");
            expect_code(&mut c, &format!("ALTER TABLE {t} ADD CONSTRAINT other PRIMARY KEY (a)"), "42P16");
        }
        let (q, p) = (keys_of(&mut c, "apk_q"), keys_of(&mut c, "apk_p"));
        assert_eq!(q, p, "plain and partitioned read unlike");
        // A nullable column made the one-column key: NOT NULL from then on.
        c.ok("CREATE TABLE apk_n (x BIGINT, y BIGINT)");
        c.ok("INSERT INTO apk_n (x, y) VALUES (1, 1), (NULL, 2)");
        expect_code(&mut c, "ALTER TABLE apk_n ADD PRIMARY KEY (x)", "23502");
        c.ok("DELETE FROM apk_n WHERE x IS NULL");
        c.ok("ALTER TABLE apk_n ADD PRIMARY KEY (x)");
    }
    server = server.restart();
    let mut c = server.connect();
    for (t, part) in kinds {
        assert!(is_primary(&mut c, t, "a"), "{t} after a restart");
        expect_code(&mut c, &format!("INSERT INTO {t} (a, b, v) VALUES (NULL, 5, 5)"), "23502");
        expect_code(&mut c, &format!("INSERT INTO {t} (a, b, v) VALUES (1, 5, 5)"), "23505");
        // Dropped with its constraint: no longer the key, still NOT NULL.
        c.ok(&format!("ALTER TABLE {t} DROP CONSTRAINT {t}_pkey"));
        assert!(!is_primary(&mut c, t, "a"), "{t} after the DROP");
        c.ok(&format!("INSERT INTO {t} (a, b, v) VALUES (1, 5, 5)"));
        expect_code(&mut c, &format!("INSERT INTO {t} (a, b, v) VALUES (NULL, 5, 5)"), "23502");
        // A composite one, unnamed: `<table>_pkey`, its columns NOT NULL.
        c.ok(&format!("DELETE FROM {t} WHERE b IS NULL"));
        c.ok(&format!("ALTER TABLE {t} ADD PRIMARY KEY (a, b)"));
        expect_code(&mut c, &format!("INSERT INTO {t} (a, b, v) VALUES (9, NULL, 9)"), "23502");
        expect_code(&mut c, &format!("INSERT INTO {t} (a, b, v) VALUES (1, 5, 6)"), "23505");
        expect_code(&mut c, &format!("ALTER TABLE {t} ADD PRIMARY KEY (a)"), "42P16");
        c.ok(&format!("ALTER TABLE {t} DROP CONSTRAINT {t}_pkey"));
        c.ok(&format!("INSERT INTO {t} (a, b, v) VALUES (1, 5, 6)"));
        let _ = part;
    }
    assert!(is_primary(&mut c, "apk_n", "x"), "apk_n after a restart");
    let (_, columns) = keys_of(&mut c, "apk_n");
    assert_eq!(columns[0], vec![Some("x".into()), Some("f".into()), Some("t".into()), Some("t".into())]);
    expect_code(&mut c, "INSERT INTO apk_n (x, y) VALUES (NULL, 3)", "23502");
    let (q, p) = (keys_of(&mut c, "apk_q"), keys_of(&mut c, "apk_p"));
    assert_eq!(q, p, "plain and partitioned read unlike at the end");
}

/// Names over the 128 bytes a sidecar keeps are cut where they are read, so
/// the name created is the name dropped and two that share their first 128
/// bytes are one name; unnamed CHECKs and UNIQUEs are named as PostgreSQL
/// names them, numbered on a collision.
#[test]
fn long_and_unnamed_constraint_names() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    for (t, part) in [("cn_q", ""), ("cn_p", " PARTITION BY HASH (a) PARTITIONS 4")] {
        c.ok(&format!("CREATE TABLE {t} (a BIGINT NOT NULL, b BIGINT, v BIGINT){part}"));
        c.ok(&format!("INSERT INTO {t} (a, b, v) VALUES (1, 1, 1)"));
        let long = "n".repeat(131);
        c.ok(&format!("ALTER TABLE {t} ADD CONSTRAINT {long} CHECK (v > -1)"));
        c.ok(&format!("ALTER TABLE {t} DROP CONSTRAINT {long}"));
        expect_code(&mut c, &format!("ALTER TABLE {t} DROP CONSTRAINT {long}"), "42704");
        let wide = format!("\"{}\"", "é".repeat(65));
        c.ok(&format!("ALTER TABLE {t} ADD CONSTRAINT {wide} UNIQUE (a, v)"));
        c.ok(&format!("ALTER TABLE {t} DROP CONSTRAINT {wide}"));
        let (x, y) = (format!("{}x", "p".repeat(128)), format!("{}y", "p".repeat(128)));
        c.ok(&format!("ALTER TABLE {t} ADD CONSTRAINT {x} UNIQUE (a, v)"));
        let e = c.simple(&format!("ALTER TABLE {t} ADD CONSTRAINT {y} UNIQUE (a, b)")).expect_err("a name the first took");
        assert!(e.message.contains("already exists"), "{t}: {e}");
        c.ok(&format!("ALTER TABLE {t} DROP CONSTRAINT {y}"));
        expect_code(&mut c, &format!("ALTER TABLE {t} DROP CONSTRAINT {x}"), "42704");

        // Unnamed CHECKs: one column read, none or several, a literal that
        // spells a column's name, and a collision.
        for check in ["v > 0", "v > -5", "a > -5 AND v > -5", "1 = 1", "v > -9 OR 'a' = 'a'"] {
            c.ok(&format!("ALTER TABLE {t} ADD CHECK ({check})"));
        }
        for name in ["v_check", "v_check1", "check", "check1", "v_check2"] {
            c.ok(&format!("ALTER TABLE {t} DROP CONSTRAINT {t}_{name}"));
        }
        // Unnamed UNIQUEs: `<table>_<cols>_key`, then numbered.
        c.ok(&format!("ALTER TABLE {t} ADD UNIQUE (a, v)"));
        c.ok(&format!("ALTER TABLE {t} ADD UNIQUE (a, v)"));
        for name in ["a_v_key", "a_v_key1"] {
            c.ok(&format!("ALTER TABLE {t} DROP CONSTRAINT {t}_{name}"));
        }
        expect_code(&mut c, &format!("ALTER TABLE {t} DROP CONSTRAINT {t}_a_v_key"), "42704");
    }
}
