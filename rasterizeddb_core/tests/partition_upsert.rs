//! `INSERT … ON CONFLICT` on a partitioned table over the wire (S18,
//! `docs/partitioning.md` §9): DO NOTHING and DO UPDATE (with EXCLUDED and
//! WHERE), by columns or by constraint name, single-row, multi-row and
//! `INSERT … SELECT`, with and without RETURNING, in autocommit and in
//! blocks, simple and prepared, each answering exactly what a plain twin with
//! the same rows answers, and storing the same rows.
//!
//! Uniqueness is decided inside the routed partition, which is exact because
//! every unique key holds every key column (§1.5). So the target must name
//! them all (42P10 otherwise, as PostgreSQL), and a DO UPDATE must not move
//! the row's point (0A000, phase 6). The concurrent half of the contract is
//! `rasterizeddb_compat/tests/partition_upsert_matrix.rs`.
//!
//! Bounded: a few dozen rows, fixed statement lists.

mod partition_wire;

use partition_wire::*;

const COLUMNS: &str = "(tenant_id BIGINT NOT NULL, grp BIGINT NOT NULL, id BIGINT NOT NULL, email TEXT, n INT, \
                       PRIMARY KEY (tenant_id, grp, id), UNIQUE (tenant_id, grp, email))";
const P: &str = "up";
const Q: &str = "uq";

fn sorted(mut rows: Vec<Vec<Option<String>>>) -> Vec<Vec<Option<String>>> {
    rows.sort();
    rows
}

/// `up` partitioned by `(tenant_id, grp)` into 7, `uq` its plain twin, the
/// same rows in both, over five tenants and two groups.
fn twins(c: &mut Client) {
    c.ok(&format!("CREATE TABLE {P} {COLUMNS} PARTITION BY HASH (tenant_id, grp) PARTITIONS 7"));
    c.ok(&format!("CREATE TABLE {Q} {COLUMNS}"));
    c.ok("CREATE TABLE src (tenant_id BIGINT NOT NULL, grp BIGINT NOT NULL, id BIGINT NOT NULL, email TEXT, n INT)");
    let mut rows = Vec::new();
    for t in 1..=5i64 {
        for g in 1..=2i64 {
            let n = if (t + g) % 4 == 0 { "NULL".to_string() } else { (t * 10 + g).to_string() };
            rows.push(format!("({t}, {g}, 1, 'e{t}{g}', {n})"));
        }
    }
    for table in [P, Q] {
        c.ok(&format!("INSERT INTO {table} (tenant_id, grp, id, email, n) VALUES {}", rows.join(", ")));
    }
    // Half of these collide with stored keys, half are new.
    c.ok("INSERT INTO src VALUES (1, 1, 1, 'x11', 100), (2, 2, 1, 'x22', 200), (3, 1, 9, 'n31', 300), \
          (6, 1, 1, 'n61', 600), (4, 2, 1, 'x42', NULL), (7, 2, 3, 'n72', 700)");
}

fn stored(c: &mut Client, table: &str) -> Vec<Vec<Option<String>>> {
    sorted(c.ok(&format!("SELECT * FROM {table}")).rows)
}

/// Both tables hold the same rows, and every partitioned row sits in the
/// partition that owns its point.
#[track_caller]
fn same_rows(c: &mut Client, server: &Server, label: &str) {
    assert_eq!(stored(c, P), stored(c, Q), "{label}: stored rows");
    assert_eq!(misplaced_rows(&server.db, P), Vec::<String>::new(), "{label}: misplaced rows");
}

/// The answer as compared: columns with their type OIDs, the rows as a
/// multiset, the command tags; or the SQLSTATE.
type Shape = Result<(Vec<(String, u32)>, Vec<Vec<Option<String>>>, Vec<String>), String>;

fn shape(o: &Outcome) -> Shape {
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

#[track_caller]
fn both_ok(c: &mut Pair, template: &str) -> Reply {
    both(c, template).unwrap_or_else(|e| panic!("`{template}` failed: {e}"))
}

#[track_caller]
fn expect_code(out: Outcome, code: &str) -> WireError {
    match out {
        Err(e) if e.code == code => e,
        other => panic!("expected {code}, got {other:?}"),
    }
}

/// Every statement shape, simple-query, compared with the twin and the
/// stored rows compared after each.
#[test]
fn upserts_answer_like_the_twin_in_every_statement_shape() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    twins(&mut c);
    let mut pair = Pair::new(&server);
    let cols = "(tenant_id, grp, id, email, n)";
    let shapes = [
        // DO NOTHING: targetless, by the key's columns in any order, by name.
        format!("INSERT INTO {{t}} {cols} VALUES (1, 1, 1, 'z', 0) ON CONFLICT DO NOTHING"),
        format!("INSERT INTO {{t}} {cols} VALUES (1, 1, 2, 'z', 0) ON CONFLICT (id, grp, tenant_id) DO NOTHING"),
        format!("INSERT INTO {{t}} {cols} VALUES (1, 1, 2, 'z', 5) ON CONFLICT ON CONSTRAINT {{t}}_pkey DO NOTHING"),
        // The composite UNIQUE: a new id under a stored email.
        format!("INSERT INTO {{t}} {cols} VALUES (2, 1, 7, 'e21', 0) ON CONFLICT (tenant_id, grp, email) DO NOTHING"),
        // DO UPDATE with EXCLUDED, by columns and by constraint name.
        format!("INSERT INTO {{t}} {cols} VALUES (2, 2, 1, 'q', 7) ON CONFLICT (tenant_id, grp, id) DO UPDATE SET n = EXCLUDED.n * 3"),
        format!(
            "INSERT INTO {{t}} {cols} VALUES (3, 1, 1, 'q', 8) ON CONFLICT ON CONSTRAINT {{t}}_pkey \
             DO UPDATE SET n = COALESCE({{t}}.n, 0) + EXCLUDED.n, email = 'by name'"
        ),
        format!(
            "INSERT INTO {{t}} {cols} VALUES (3, 2, 4, 'e32', 1) ON CONFLICT (tenant_id, grp, email) \
             DO UPDATE SET n = -1"
        ),
        // DO UPDATE … WHERE: applied, then declined.
        format!(
            "INSERT INTO {{t}} {cols} VALUES (4, 1, 1, 'w', 9) ON CONFLICT (tenant_id, grp, id) \
             DO UPDATE SET n = EXCLUDED.n WHERE {{t}}.n IS NOT NULL AND {{t}}.n <> EXCLUDED.n"
        ),
        format!(
            "INSERT INTO {{t}} {cols} VALUES (4, 2, 1, 'w', 9) ON CONFLICT (tenant_id, grp, id) \
             DO UPDATE SET n = EXCLUDED.n WHERE {{t}}.n > 1000"
        ),
        // A key column assigned EXCLUDED's own value keeps the point.
        format!(
            "INSERT INTO {{t}} {cols} VALUES (5, 1, 1, 'k', 3) ON CONFLICT (tenant_id, grp, id) \
             DO UPDATE SET tenant_id = EXCLUDED.tenant_id, grp = EXCLUDED.grp, n = 51"
        ),
        // RETURNING: DO NOTHING returns only what it inserted; DO UPDATE the
        // post-image; aliases and expressions; a declined update nothing.
        format!(
            "INSERT INTO {{t}} {cols} VALUES (1, 1, 1, 'r', 0), (8, 1, 1, 'r81', 1), (9, 2, 1, 'r92', NULL) \
             ON CONFLICT DO NOTHING RETURNING tenant_id, id, email, n"
        ),
        format!("INSERT INTO {{t}} {cols} VALUES (1, 2, 1, 'r', 0) ON CONFLICT DO NOTHING RETURNING *"),
        format!(
            "INSERT INTO {{t}} {cols} VALUES (1, 2, 1, 'r', 4), (8, 2, 5, 'r825', 2) ON CONFLICT (tenant_id, grp, id) \
             DO UPDATE SET n = COALESCE({{t}}.n, 0) + EXCLUDED.n RETURNING tenant_id AS t, id AS k, n * 2 + 1, \
             COALESCE(email, 'x') AS e"
        ),
        format!(
            "INSERT INTO {{t}} {cols} VALUES (4, 2, 1, 'w', 9) ON CONFLICT (tenant_id, grp, id) \
             DO UPDATE SET n = 0 WHERE {{t}}.n > 1000 RETURNING *"
        ),
        // INSERT … SELECT, each form.
        format!("INSERT INTO {{t}} {cols} SELECT tenant_id, grp, id, email, n FROM src WHERE n > 250 ON CONFLICT DO NOTHING"),
        format!(
            "INSERT INTO {{t}} {cols} SELECT tenant_id, grp, id, email, n FROM src ON CONFLICT (tenant_id, grp, id) \
             DO UPDATE SET n = EXCLUDED.n, email = EXCLUDED.email RETURNING tenant_id, grp, id, n"
        ),
        format!(
            "INSERT INTO {{t}} {cols} SELECT tenant_id, grp, id, email, n FROM src WHERE tenant_id > 100 \
             ON CONFLICT DO NOTHING RETURNING id"
        ),
    ];
    for sql in &shapes {
        both_ok(&mut pair, sql);
        same_rows(&mut c, &server, sql);
    }
    // The answers that matter, beyond agreeing: only the inserted rows, and
    // the post-image.
    let r = both_ok(
        &mut pair,
        &format!("INSERT INTO {{t}} {cols} VALUES (5, 2, 1, 'a', 1), (5, 2, 2, 'a2', 2) ON CONFLICT DO NOTHING RETURNING id"),
    );
    assert_eq!(r.rows, vec![vec![Some("2".to_string())]], "DO NOTHING returns only the row it inserted");
    assert_eq!(r.tags, vec!["INSERT 0 1".to_string()]);
    let r = both_ok(
        &mut pair,
        &format!("INSERT INTO {{t}} {cols} VALUES (5, 2, 2, 'zz', 40) ON CONFLICT (tenant_id, grp, id) DO UPDATE SET n = {{t}}.n + EXCLUDED.n RETURNING n, email"),
    );
    assert_eq!(r.rows, vec![vec![Some("42".to_string()), Some("a2".to_string())]], "the post-image");
    let r = both_ok(&mut pair, &format!("INSERT INTO {{t}} {cols} VALUES (5, 2, 2, 'zz', 40) ON CONFLICT DO NOTHING RETURNING n"));
    assert!(!r.columns.is_empty() && r.rows.is_empty(), "a skipped row: a description and no rows, {r:?}");

    // In blocks: committed, and rolled back.
    for end in ["COMMIT", "ROLLBACK"] {
        pair.ok("BEGIN");
        both_ok(&mut pair, &format!("INSERT INTO {{t}} {cols} VALUES (1, 1, 1, 'b', 1), (11, 1, 1, 'b', 1) ON CONFLICT (tenant_id, grp, id) DO UPDATE SET n = {{t}}.n + 1000 RETURNING *"));
        both_ok(&mut pair, &format!("INSERT INTO {{t}} {cols} VALUES (12, 2, 1, 'b', 1), (2, 1, 1, 'b', 1) ON CONFLICT DO NOTHING"));
        pair.ok(end);
        same_rows(&mut c, &server, end);
    }
    drop(c);
    drop(server);
}

/// The memory's bug class (multirow-insert-loses-clauses), on a partitioned
/// table: a multi-row `INSERT … ON CONFLICT` is re-run as one statement per
/// tuple, so the clause must reach every tuple (a skipped tuple, not a
/// 23505, and the tuples after it still inserted), and the command tag must
/// count rows, not statements. Over several partitions, against the twin
/// and against what is actually stored; and the same for `INSERT … SELECT`
/// and a prepared multi-row upsert.
#[test]
fn a_multi_row_upsert_keeps_its_clause_and_counts_rows() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    twins(&mut c);
    let mut pair = Pair::new(&server);
    let count = |c: &mut Client| c.ok(&format!("SELECT COUNT(*) FROM {P}")).rows[0][0].clone().unwrap().parse::<i64>().unwrap();

    // Tuples 1 and 3 collide with stored keys (tenants 1 and 3), tuple 4
    // repeats tuple 2 within the statement, tuples 2 and 5 are new: two land.
    let before = count(&mut c);
    let r = both_ok(
        &mut pair,
        "INSERT INTO {t} (tenant_id, grp, id, email, n) VALUES (1, 1, 1, 'a', 0), (6, 1, 2, 'b', 0), \
         (3, 2, 1, 'c', 0), (6, 1, 2, 'b', 0), (7, 2, 2, 'd', 0) ON CONFLICT (tenant_id, grp, id) DO NOTHING",
    );
    assert_eq!(r.tags, vec!["INSERT 0 2".to_string()], "the count is of rows inserted, not tuples");
    assert_eq!(count(&mut c) - before, 2, "stored");
    same_rows(&mut c, &server, "multi-row DO NOTHING");

    // The same with RETURNING: the two inserted rows only.
    let r = both_ok(
        &mut pair,
        "INSERT INTO {t} (tenant_id, grp, id, email, n) VALUES (1, 1, 1, 'a', 0), (6, 2, 2, 'b', 0), \
         (8, 1, 3, 'c', 0), (6, 2, 2, 'b', 0) ON CONFLICT DO NOTHING RETURNING tenant_id, id",
    );
    assert_eq!(sorted(r.rows), vec![vec![Some("6".into()), Some("2".into())], vec![Some("8".into()), Some("3".into())]]);
    assert_eq!(r.tags, vec!["INSERT 0 2".to_string()]);

    // DO UPDATE over three partitions: every tuple is applied, two updates and
    // one insert, counted as three.
    let before = count(&mut c);
    let r = both_ok(
        &mut pair,
        "INSERT INTO {t} (tenant_id, grp, id, email, n) VALUES (2, 1, 1, 'u', 5), (9, 1, 1, 'u91', 6), (4, 2, 1, 'u', 7) \
         ON CONFLICT (tenant_id, grp, id) DO UPDATE SET n = EXCLUDED.n + 1000",
    );
    assert_eq!(r.tags, vec!["INSERT 0 3".to_string()]);
    assert_eq!(count(&mut c) - before, 1, "one insert, two updates");
    let n_of = |c: &mut Client, t: i64, g: i64| {
        c.ok(&format!("SELECT n FROM {P} WHERE tenant_id = {t} AND grp = {g} AND id = 1")).rows[0][0].clone()
    };
    assert_eq!(n_of(&mut c, 2, 1).as_deref(), Some("1005"));
    assert_eq!(n_of(&mut c, 4, 2).as_deref(), Some("1007"));
    assert_eq!(n_of(&mut c, 9, 1).as_deref(), Some("6"), "the inserted tuple keeps its own value");
    same_rows(&mut c, &server, "multi-row DO UPDATE");

    // A DO UPDATE … WHERE declining one tuple: that tuple is not counted.
    let r = both_ok(
        &mut pair,
        "INSERT INTO {t} (tenant_id, grp, id, email, n) VALUES (2, 1, 1, 'u', 1), (4, 2, 1, 'u', 1), (10, 1, 1, 'u', 1) \
         ON CONFLICT (tenant_id, grp, id) DO UPDATE SET n = 0 WHERE {t}.n < 1006",
    );
    assert_eq!(r.tags, vec!["INSERT 0 2".to_string()], "one update, one declined, one insert");
    same_rows(&mut c, &server, "multi-row DO UPDATE … WHERE");

    // INSERT … SELECT: the clause survives the rewrite into VALUES.
    let before = count(&mut c);
    let r = both_ok(
        &mut pair,
        "INSERT INTO {t} (tenant_id, grp, id, email, n) SELECT tenant_id, grp, id, email, n FROM src ON CONFLICT DO NOTHING",
    );
    let inserted = count(&mut c) - before;
    assert!(inserted > 0 && inserted < 6, "some of src collides: {inserted}");
    assert_eq!(r.tags, vec![format!("INSERT 0 {inserted}")]);
    same_rows(&mut c, &server, "INSERT … SELECT DO NOTHING");

    // Prepared, several tuples per execution, run twice: the second run
    // collides on every tuple and counts zero.
    for (name, table) in [("mp", P), ("mq", Q)] {
        c.prepare(
            name,
            &format!(
                "INSERT INTO {table} (tenant_id, grp, id, n) VALUES ($1, 1, 50, $2), ($3, 2, 50, $4) \
                 ON CONFLICT (tenant_id, grp, id) DO NOTHING"
            ),
        )
        .expect("prepare");
    }
    for want in ["INSERT 0 2", "INSERT 0 0"] {
        let params: Vec<Option<String>> = ["3", "1", "11", "2"].iter().map(|v| Some(v.to_string())).collect();
        let rp = c.execute_prepared("mp", &params);
        let rq = c.execute_prepared("mq", &params);
        assert_eq!(shape(&rp), shape(&rq));
        assert_eq!(rp.expect("prepared").tags, vec![want.to_string()]);
    }
    same_rows(&mut c, &server, "prepared multi-row");
    drop(c);
    drop(server);
}

/// A DO UPDATE that would move the row to another point is a key change
/// (phase 6): refused with 0A000 before the update writes, in every shape. A
/// statement whose earlier tuples had inserted rows fails whole, so nothing
/// of it is stored; in a block the block fails and its earlier statements
/// roll back with it. The twin (a plain table, where a key change is just an
/// update) is not consulted; the partitioned table must be unchanged.
#[test]
fn a_do_update_that_moves_the_point_is_refused_before_any_write() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    twins(&mut c);
    let before = stored(&mut c, P);
    for sql in [
        // EXCLUDED's value moved, the stored value moved, a literal elsewhere.
        "INSERT INTO up (tenant_id, grp, id, n) VALUES (1, 1, 1, 0) ON CONFLICT (tenant_id, grp, id) \
         DO UPDATE SET tenant_id = EXCLUDED.tenant_id + 100",
        "INSERT INTO up (tenant_id, grp, id, n) VALUES (1, 1, 1, 0) ON CONFLICT (tenant_id, grp, id) \
         DO UPDATE SET grp = up.grp + 1, n = 5",
        "INSERT INTO up (tenant_id, grp, id, n) VALUES (2, 2, 1, 0) ON CONFLICT ON CONSTRAINT up_pkey \
         DO UPDATE SET n = 1, tenant_id = 77 RETURNING *",
        // Tuple 1 inserts a new row in another partition first.
        "INSERT INTO up (tenant_id, grp, id, n) VALUES (20, 1, 1, 0), (3, 1, 1, 0) ON CONFLICT (tenant_id, grp, id) \
         DO UPDATE SET tenant_id = EXCLUDED.tenant_id * 2",
        "INSERT INTO up (tenant_id, grp, id, email, n) SELECT tenant_id, grp, id, email, n FROM src \
         ON CONFLICT (tenant_id, grp, id) DO UPDATE SET grp = EXCLUDED.grp + 10 RETURNING id",
    ] {
        let e = expect_code(c.simple(sql), "0A000");
        assert!(e.is_unsupported(), "{sql}: {e}");
        assert_eq!(stored(&mut c, P), before, "`{sql}` changed the table");
        assert_eq!(misplaced_rows(&server.db, P), Vec::<String>::new());
    }
    // Prepared: the same.
    c.prepare(
        "kp",
        "INSERT INTO up (tenant_id, grp, id, n) VALUES ($1, $2, 1, 0) ON CONFLICT (tenant_id, grp, id) \
         DO UPDATE SET tenant_id = $3",
    )
    .expect("prepare");
    let e = expect_code(c.execute_prepared("kp", &[Some("4".into()), Some("1".into()), Some("5".into())]), "0A000");
    assert!(e.is_unsupported(), "{e}");
    // The key's own value keeps the point, and runs.
    c.execute_prepared("kp", &[Some("4".into()), Some("1".into()), Some("4".into())]).expect("the same value");
    assert_eq!(stored(&mut c, P), before, "an update to the same values");
    // In a block, after statements that wrote in two partitions.
    c.ok("BEGIN");
    c.ok("INSERT INTO up (tenant_id, grp, id, n) VALUES (30, 1, 1, 0), (31, 2, 1, 0)");
    c.ok("UPDATE up SET n = 999 WHERE tenant_id = 5");
    let e = expect_code(
        c.simple("INSERT INTO up (tenant_id, grp, id, n) VALUES (5, 2, 1, 0) ON CONFLICT (tenant_id, grp, id) DO UPDATE SET tenant_id = 6"),
        "0A000",
    );
    assert!(e.is_unsupported(), "{e}");
    assert_eq!(c.status, b'E', "the refusal ends the block");
    c.ok("ROLLBACK");
    assert_eq!(stored(&mut c, P), before, "the block left nothing");
    drop(c);
    drop(server);
}

/// ON CONFLICT inside a block that later rolls back leaves nothing: neither
/// the rows it inserted (in several partitions) nor the updates it applied,
/// whichever form, multi-row and `INSERT … SELECT` included; the same block
/// committed keeps exactly the twin's rows.
#[test]
fn on_conflict_in_a_rolled_back_block_leaves_nothing() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    twins(&mut c);
    let mut pair = Pair::new(&server);
    let block = [
        "INSERT INTO {t} (tenant_id, grp, id, n) VALUES (1, 1, 1, 0), (40, 1, 1, 1), (41, 2, 1, 2) ON CONFLICT DO NOTHING",
        "INSERT INTO {t} (tenant_id, grp, id, n) VALUES (2, 1, 1, 5), (42, 1, 1, 6) ON CONFLICT (tenant_id, grp, id) \
         DO UPDATE SET n = {t}.n + EXCLUDED.n RETURNING *",
        "INSERT INTO {t} (tenant_id, grp, id, email, n) SELECT tenant_id, grp, id, email, n FROM src \
         ON CONFLICT (tenant_id, grp, id) DO UPDATE SET email = 'from src'",
        "INSERT INTO {t} (tenant_id, grp, id, email, n) VALUES (3, 2, 8, 'e32', 1) ON CONFLICT (tenant_id, grp, email) \
         DO UPDATE SET n = 333",
    ];
    let before = stored(&mut c, P);
    pair.ok("BEGIN");
    for sql in block {
        both_ok(&mut pair, sql);
    }
    // Inside the block the session sees its own writes.
    let inside = pair.p.ok(&format!("SELECT COUNT(*) FROM {P} WHERE tenant_id >= 40")).rows;
    assert_eq!(inside, vec![vec![Some("3".to_string())]]);
    pair.ok("ROLLBACK");
    assert_eq!(stored(&mut c, P), before, "the rolled-back block left something");
    same_rows(&mut c, &server, "after ROLLBACK");
    pair.ok("BEGIN");
    for sql in block {
        both_ok(&mut pair, sql);
    }
    pair.ok("COMMIT");
    assert_ne!(stored(&mut c, P), before);
    same_rows(&mut c, &server, "after COMMIT");
    drop(c);
    drop(server);
}

/// The conflict target must name every partition-key column (42P10, as
/// PostgreSQL, whose partitioned unique keys hold the key too); a constraint
/// name resolves to its columns; one naming nothing is 42704 on both tables.
/// Prepared upserts alternate tenants and answer like the twin's.
#[test]
fn targets_constraint_names_and_prepared_upserts() {
    let dir = tempfile::tempdir().expect("dir");
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    twins(&mut c);
    let mut pair = Pair::new(&server);
    let before = stored(&mut c, P);
    for sql in [
        "INSERT INTO up (tenant_id, grp, id) VALUES (1, 1, 1) ON CONFLICT (grp, id) DO NOTHING",
        "INSERT INTO up (tenant_id, grp, id) VALUES (1, 1, 1) ON CONFLICT (tenant_id, id) DO UPDATE SET n = 1",
        "INSERT INTO up (tenant_id, grp, id) VALUES (1, 1, 1) ON CONFLICT (email) DO UPDATE SET n = 1 RETURNING *",
    ] {
        let e = expect_code(c.simple(sql), "42P10");
        assert!(e.message.contains("\"grp\"") || e.message.contains("\"tenant_id\""), "{e}");
    }
    assert_eq!(stored(&mut c, P), before);
    assert_eq!(
        shape(&both(&mut pair, "INSERT INTO {t} (tenant_id, grp, id) VALUES (1, 1, 1) ON CONFLICT ON CONSTRAINT nope DO UPDATE SET n = 1")),
        Err("42704".to_string())
    );
    // The composite UNIQUE by its default name.
    both_ok(
        &mut pair,
        "INSERT INTO {t} (tenant_id, grp, id, email, n) VALUES (2, 2, 9, 'e22', 5) \
         ON CONFLICT ON CONSTRAINT {t}_tenant_id_grp_email_key DO UPDATE SET n = EXCLUDED.n RETURNING id, n",
    );
    same_rows(&mut c, &server, "by constraint name");

    // Prepared, alternating tenants: inserts, then updates of the same keys.
    for (name, table) in [("pp", P), ("pq", Q)] {
        let d = c.prepare(
            name,
            &format!(
                "INSERT INTO {table} (tenant_id, grp, id, n) VALUES ($1, $2, $3, $4) ON CONFLICT (tenant_id, grp, id) \
                 DO UPDATE SET n = EXCLUDED.n + COALESCE({table}.n, 0) RETURNING id AS k, n"
            ),
        );
        assert_eq!(d.expect("prepare").columns, vec!["k".to_string(), "n".to_string()]);
    }
    for i in 0..10i64 {
        let tenant = if i % 2 == 0 { 2 + i % 3 } else { 20 + i % 3 };
        let params: Vec<Option<String>> =
            [tenant, 1 + i % 2, 1, i].iter().map(|v| Some(v.to_string())).collect();
        let rp = c.execute_prepared("pp", &params);
        let rq = c.execute_prepared("pq", &params);
        assert_eq!(shape(&rp), shape(&rq), "run {i} with {params:?}");
        assert_eq!(rp.expect("run").rows.len(), 1);
        same_rows(&mut c, &server, &format!("prepared run {i}"));
    }
    drop(c);
    drop(server);
}
