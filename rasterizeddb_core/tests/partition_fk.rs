//! Foreign keys to and from hash-partitioned tables (`docs/partitioning.md`
//! §10, "Phase 3 P4 as built"), over pgwire.
//!
//! Every scenario runs on four configurations of one parent and its child:
//! both plain (`pp`, the twin every other one is compared with), a plain
//! parent with a partitioned child (`pq`), a partitioned parent with a plain
//! child (`qp`), and both partitioned (`qq`). The statements are the same
//! text with the configuration's suffix in each table name; each answer
//! (rows, columns, tags) must be the plain twin's, or both must fail with one
//! SQLSTATE. What both sides store is compared after the writes and again
//! after a restart, read back through SQL.

mod partition_wire;

use std::time::Duration;

use partition_wire::*;

const CONFIGS: [(&str, bool, bool); 4] = [("pp", false, false), ("pq", false, true), ("qp", true, false), ("qq", true, true)];

fn partitioned(by: &str, yes: bool) -> String {
    if yes { format!(" PARTITION BY HASH ({by}) PARTITIONS 8") } else { String::new() }
}

fn render(template: &str, suffix: &str) -> String {
    template.replace("{p}", &format!("fkp_{suffix}")).replace("{c}", &format!("fkc_{suffix}"))
}

fn unrender(text: &str) -> String {
    let mut out = text.to_string();
    for (s, _, _) in CONFIGS {
        out = out.replace(&format!("fkp_{s}"), "{p}").replace(&format!("fkc_{s}"), "{c}");
    }
    out
}

fn normalize(reply: &Reply) -> (Vec<String>, Vec<Vec<Option<String>>>, Vec<String>) {
    let cols = reply.columns.iter().map(|c| unrender(c)).collect();
    let mut rows: Vec<Vec<Option<String>>> =
        reply.rows.iter().map(|r| r.iter().map(|v| v.as_deref().map(unrender)).collect()).collect();
    rows.sort();
    (cols, rows, reply.tags.iter().map(|t| unrender(t)).collect())
}

/// One session per configuration over one server.
struct Quad {
    server: Option<Server>,
    clients: Vec<Client>,
}

impl Quad {
    fn start(dir: &std::path::Path) -> Quad {
        let server = Server::start(dir.to_str().unwrap());
        let clients = CONFIGS.iter().map(|_| server.connect()).collect();
        Quad { server: Some(server), clients }
    }

    fn restart(&mut self) {
        self.clients.clear();
        let server = self.server.take().unwrap().restart();
        self.clients = CONFIGS.iter().map(|_| server.connect()).collect();
        self.server = Some(server);
    }

    /// `template` on every configuration: each answers as the plain twin
    /// does, or fails with its SQLSTATE. Returns the twin's outcome.
    #[track_caller]
    fn all(&mut self, template: &str) -> Outcome {
        let mut outcomes = Vec::new();
        for (i, (suffix, _, _)) in CONFIGS.iter().enumerate() {
            outcomes.push(self.clients[i].simple(&render(template, suffix)));
        }
        let twin = &outcomes[0];
        for (i, outcome) in outcomes.iter().enumerate().skip(1) {
            match (twin, outcome) {
                (Ok(a), Ok(b)) => assert_eq!(
                    normalize(a),
                    normalize(b),
                    "`{template}`: {} answered unlike the plain twin",
                    CONFIGS[i].0
                ),
                (Err(a), Err(b)) => {
                    assert_eq!(a.code, b.code, "`{template}`: {} failed unlike the twin: {a} / {b}", CONFIGS[i].0)
                }
                _ => panic!("`{template}`: twin {twin:?}, {} {outcome:?}", CONFIGS[i].0),
            }
        }
        outcomes.swap_remove(0)
    }

    #[track_caller]
    fn ok(&mut self, template: &str) -> Reply {
        self.all(template).unwrap_or_else(|e| panic!("`{template}` failed on every configuration: {e}"))
    }

    #[track_caller]
    fn err(&mut self, template: &str, code: &str) {
        let e = self.all(template).expect_err(template);
        assert_eq!(e.code, code, "`{template}`: {e}");
    }

    /// One configuration's statement alone.
    fn on(&mut self, i: usize, template: &str) -> Outcome {
        self.clients[i].simple(&render(template, CONFIGS[i].0))
    }

    /// One table's rows, compared across configurations.
    #[track_caller]
    fn same_rows_of(&mut self, table: &str) {
        self.ok(&format!("SELECT * FROM {table} ORDER BY 1"));
    }

    /// The rows both tables hold, compared across configurations.
    #[track_caller]
    fn same_rows(&mut self) {
        self.ok("SELECT id, label FROM {p} ORDER BY id");
        self.ok("SELECT cid, pid, v FROM {c} ORDER BY cid");
    }
}

fn create_parent(q: &mut Quad) {
    for (i, (_, part_parent, _)) in CONFIGS.iter().enumerate() {
        let sql = format!(
            "CREATE TABLE {{p}} (id BIGINT NOT NULL PRIMARY KEY, label TEXT){}",
            partitioned("id", *part_parent)
        );
        q.on(i, &sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let values: Vec<String> = (1..=24).map(|i| format!("({i}, 'p{i}')")).collect();
    q.ok(&format!("INSERT INTO {{p}} (id, label) VALUES {}", values.join(", ")));
}

fn create_child(q: &mut Quad, clause: &str) {
    for (i, (_, _, part_child)) in CONFIGS.iter().enumerate() {
        let sql = format!(
            "CREATE TABLE {{c}} (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT DEFAULT 7, v INT, \
             CONSTRAINT {{c}}_pid FOREIGN KEY (pid) REFERENCES {{p}} (id) {clause}){}",
            partitioned("cid", *part_child)
        );
        q.on(i, &sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

fn drop_both(q: &mut Quad) {
    q.ok("DROP TABLE {c}");
    q.ok("DROP TABLE {p}");
}

/// Inserts and updates of a child are checked against the parent, whichever
/// side is partitioned: a missing parent is 23503 with nothing written, a
/// NULL is exempt, a multi-row insert is checked per row, and an expression
/// assigned to the key column is checked per row. Then each `ON DELETE`
/// action, routed (`WHERE id = k`) and fanned out (`IN`, a range), with what
/// both tables hold compared, and again after a restart (the reverse index
/// rebuilt at open).
#[test]
fn inserts_updates_and_every_delete_action_answer_like_the_plain_twin() {
    let dir = tempfile::tempdir().unwrap();
    let mut q = Quad::start(dir.path());
    for action in ["NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"] {
        create_parent(&mut q);
        create_child(&mut q, &format!("ON DELETE {action}"));
        q.ok("INSERT INTO {c} (cid, pid, v) VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30), (4, NULL, 40)");
        let values: Vec<String> = (5..=40).map(|i| format!("({i}, {}, {i})", (i % 12) + 1)).collect();
        q.ok(&format!("INSERT INTO {{c}} (cid, pid, v) VALUES {}", values.join(", ")));
        q.err("INSERT INTO {c} (cid, pid, v) VALUES (100, 999, 0)", "23503");
        q.err("INSERT INTO {c} (cid, pid, v) VALUES (101, 5, 0), (102, 998, 0)", "23503");
        q.ok("INSERT INTO {c} (cid, v) VALUES (103, 0)");
        q.err("UPDATE {c} SET pid = 777 WHERE cid = 2", "23503");
        q.ok("UPDATE {c} SET pid = 20 WHERE cid = 2");
        q.err("UPDATE {c} SET pid = pid + 100 WHERE cid IN (1, 3)", "23503");
        q.ok("UPDATE {c} SET pid = pid + 1 WHERE cid IN (1, 3)");
        q.ok("UPDATE {c} SET pid = NULL WHERE cid = 3");
        q.same_rows();

        // A parent with no child goes whatever the action.
        q.ok("DELETE FROM {p} WHERE id = 24");
        let blocking = matches!(action, "NO ACTION" | "RESTRICT");
        for delete in ["DELETE FROM {p} WHERE id = 2", "DELETE FROM {p} WHERE id IN (5, 6)", "DELETE FROM {p} WHERE id >= 20"] {
            if blocking {
                q.err(delete, "23503");
            } else {
                q.ok(delete);
            }
            q.same_rows();
        }
        q.ok("SELECT count(*) FROM {c} WHERE pid IS NOT NULL AND pid NOT IN (SELECT id FROM {p})");
        let orphans = q.ok("SELECT count(*) FROM {c} WHERE pid IS NOT NULL AND pid NOT IN (SELECT id FROM {p})");
        // SET DEFAULT writes 7, whose parent is still there.
        assert_eq!(orphans.rows, vec![vec![Some("0".to_string())]], "{action}: orphans");

        q.restart();
        q.same_rows();
        q.err("INSERT INTO {c} (cid, pid, v) VALUES (200, 999, 0)", "23503");
        if blocking {
            q.err("DELETE FROM {p} WHERE id = 3", "23503");
        } else {
            q.ok("DELETE FROM {p} WHERE id = 3");
        }
        q.same_rows();
        drop_both(&mut q);
    }
}

/// Inside blocks: a violating insert ends the block on every configuration,
/// a cascade lands in the deleting transaction and goes with its rollback.
#[test]
fn blocks_roll_cascades_back_with_their_cause() {
    let dir = tempfile::tempdir().unwrap();
    let mut q = Quad::start(dir.path());
    create_parent(&mut q);
    create_child(&mut q, "ON DELETE CASCADE");
    q.ok("INSERT INTO {c} (cid, pid, v) VALUES (1, 1, 1), (2, 1, 2), (3, 2, 3), (4, 9, 4)");
    q.ok("BEGIN");
    q.ok("DELETE FROM {p} WHERE id = 1");
    q.ok("SELECT cid FROM {c} ORDER BY cid");
    q.ok("ROLLBACK");
    q.same_rows();
    q.ok("BEGIN");
    q.ok("DELETE FROM {p} WHERE id IN (1, 9)");
    q.ok("COMMIT");
    q.same_rows();
    q.ok("BEGIN");
    q.err("INSERT INTO {c} (cid, pid, v) VALUES (10, 404, 0)", "23503");
    q.ok("ROLLBACK");
    q.same_rows();
}

/// `ON UPDATE` from a plain parent into a plain or partitioned child, each
/// action; the child's key column is not its partition key, so a cascade
/// writes it in place.
#[test]
fn on_update_actions_from_a_plain_parent_answer_like_the_twin() {
    let dir = tempfile::tempdir().unwrap();
    let mut q = Quad::start(dir.path());
    for action in ["NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"] {
        create_parent(&mut q);
        create_child(&mut q, &format!("ON UPDATE {action}"));
        q.ok("INSERT INTO {c} (cid, pid, v) VALUES (1, 1, 1), (2, 2, 2), (3, 2, 3), (4, 7, 4)");
        // A key no child references moves on every configuration whose parent
        // allows the key to change; a partitioned parent's key is its
        // partition key, which no UPDATE may change yet (phase 6).
        for (i, (suffix, part_parent, _)) in CONFIGS.iter().enumerate() {
            let moved = q.on(i, "UPDATE {p} SET id = 50 WHERE id = 2");
            if *part_parent {
                let e = moved.expect_err("a key change on a partitioned parent");
                assert!(e.is_unsupported(), "{suffix}: {e}");
            } else if matches!(action, "NO ACTION" | "RESTRICT") {
                assert_eq!(moved.expect_err(action).code, "23503", "{suffix}");
            } else {
                moved.unwrap_or_else(|e| panic!("{suffix} {action}: {e}"));
            }
        }
        // The plain-parent configurations agree with each other.
        let rows = |q: &mut Quad, i: usize| q.on(i, "SELECT cid, pid FROM {c} ORDER BY cid").unwrap().rows;
        assert_eq!(rows(&mut q, 0), rows(&mut q, 1), "{action}: pq unlike pp");
        assert_eq!(rows(&mut q, 2), rows(&mut q, 3), "{action}: qq unlike qp");
        // A same-value assignment of the key changes nothing a child sees.
        q.ok("UPDATE {p} SET id = 1, label = 'one' WHERE id = 1");
        assert_eq!(rows(&mut q, 0), rows(&mut q, 1), "{action}: pq unlike pp");
        assert_eq!(rows(&mut q, 2), rows(&mut q, 3), "{action}: qq unlike qp");
        assert_eq!(rows(&mut q, 2)[1], vec![Some("2".to_string()), Some("2".to_string())], "{action}: qp moved");
        drop_both(&mut q);
    }
}

/// `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY` validates the rows already
/// there, in every partition of a partitioned child (23503 when one breaks
/// it, nothing stored), takes the PostgreSQL name when unnamed, and is
/// enforced after it, through a restart; `DROP CONSTRAINT` by that name
/// lifts it, and a second drop is 42704.
#[test]
fn add_and_drop_foreign_key_answer_like_the_twin() {
    let dir = tempfile::tempdir().unwrap();
    let mut q = Quad::start(dir.path());
    create_parent(&mut q);
    for (i, (_, _, part_child)) in CONFIGS.iter().enumerate() {
        let sql = format!(
            "CREATE TABLE {{c}} (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT, v INT){}",
            partitioned("cid", *part_child)
        );
        q.on(i, &sql).unwrap();
    }
    let values: Vec<String> = (1..=30).map(|i| format!("({i}, {}, {i})", (i % 20) + 1)).collect();
    q.ok(&format!("INSERT INTO {{c}} (cid, pid, v) VALUES {}", values.join(", ")));
    q.ok("INSERT INTO {c} (cid, pid, v) VALUES (31, 444, 0), (32, NULL, 0)");
    q.err("ALTER TABLE {c} ADD FOREIGN KEY (pid) REFERENCES {p} (id)", "23503");
    // Nothing was added: the orphan's twin still goes in.
    q.ok("INSERT INTO {c} (cid, pid, v) VALUES (33, 445, 0)");
    q.ok("DELETE FROM {c} WHERE pid IN (444, 445)");
    q.ok("ALTER TABLE {c} ADD FOREIGN KEY (pid) REFERENCES {p} (id) ON DELETE CASCADE");
    q.err("INSERT INTO {c} (cid, pid, v) VALUES (34, 446, 0)", "23503");
    q.err("ALTER TABLE {c} ADD CONSTRAINT {c}_pid_fkey FOREIGN KEY (pid) REFERENCES {p} (id)", "42710");
    // A second unnamed key on the column is numbered.
    q.ok("ALTER TABLE {c} ADD FOREIGN KEY (pid) REFERENCES {p} (id)");
    q.restart();
    q.err("INSERT INTO {c} (cid, pid, v) VALUES (34, 446, 0)", "23503");
    q.ok("ALTER TABLE {c} DROP CONSTRAINT {c}_pid_fkey1");
    q.ok("DELETE FROM {p} WHERE id = 2");
    q.same_rows();
    q.ok("ALTER TABLE {c} DROP CONSTRAINT {c}_pid_fkey");
    q.err("ALTER TABLE {c} DROP CONSTRAINT {c}_pid_fkey", "42704");
    q.ok("ALTER TABLE {c} DROP CONSTRAINT IF EXISTS {c}_pid_fkey");
    q.ok("INSERT INTO {c} (cid, pid, v) VALUES (34, 446, 0)");
    q.ok("DELETE FROM {p} WHERE id = 3");
    q.same_rows();
    // A name as written: quoted, with a space and non-ASCII letters.
    q.ok("DELETE FROM {c} WHERE pid IS NOT NULL AND pid NOT IN (SELECT id FROM {p})");
    q.ok("ALTER TABLE {c} ADD CONSTRAINT \"外部 キー\" FOREIGN KEY (pid) REFERENCES {p} (id)");
    q.err("INSERT INTO {c} (cid, pid, v) VALUES (35, 447, 0)", "23503");
    q.restart();
    q.err("ALTER TABLE {c} DROP CONSTRAINT \"外部\"", "42704");
    q.ok("ALTER TABLE {c} DROP CONSTRAINT \"外部 キー\"");
    q.ok("INSERT INTO {c} (cid, pid, v) VALUES (35, 447, 0)");
    q.same_rows();
    // NOT VALID (P4 round 2): the orphan 447 stays, new writes are checked,
    // VALIDATE CONSTRAINT finds it in every partition, then passes.
    q.ok("ALTER TABLE {c} ADD CONSTRAINT nv FOREIGN KEY (pid) REFERENCES {p} (id) NOT VALID");
    q.err("INSERT INTO {c} (cid, pid, v) VALUES (36, 448, 0)", "23503");
    q.ok("INSERT INTO {c} (cid, pid, v) VALUES (36, 4, 0)");
    q.err("ALTER TABLE {c} VALIDATE CONSTRAINT nv", "23503");
    q.restart();
    q.err("INSERT INTO {c} (cid, pid, v) VALUES (37, 449, 0)", "23503");
    q.ok("DELETE FROM {c} WHERE pid = 447");
    q.ok("ALTER TABLE {c} VALIDATE CONSTRAINT nv");
    q.same_rows();
}

/// A partitioned parent is referenced by its whole partition key only: any
/// other column is 42830, on CREATE and on ADD, for a plain or partitioned
/// child, and so is a partitioned table created under a name a key already
/// names by another column. A referenced column that is not there is 42703,
/// and ADD naming a missing table 42P01.
#[test]
fn a_partitioned_parent_is_referenced_by_its_partition_key_only() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    c.ok("CREATE TABLE rp (id BIGINT NOT NULL PRIMARY KEY, alt BIGINT NOT NULL, UNIQUE (id, alt)) \
          PARTITION BY HASH (id) PARTITIONS 4");
    c.ok("CREATE TABLE rp2 (t BIGINT NOT NULL, alt BIGINT NOT NULL, UNIQUE (t, alt)) PARTITION BY HASH (t) PARTITIONS 4");
    for part in ["", " PARTITION BY HASH (cid) PARTITIONS 4"] {
        for bad in ["rp (alt)", "rp2 (t)", "rp2 (alt)"] {
            let e = c
                .simple(&format!("CREATE TABLE rc (cid BIGINT NOT NULL PRIMARY KEY, x BIGINT REFERENCES {bad}){part}"))
                .expect_err(bad);
            assert_eq!(e.code, "42830", "{bad}{part}: {e}");
        }
        let e = c
            .simple(&format!("CREATE TABLE rc (cid BIGINT NOT NULL PRIMARY KEY, x BIGINT REFERENCES rp (nope)){part}"))
            .expect_err("missing column");
        assert_eq!(e.code, "42703", "{e}");
        c.ok(&format!("CREATE TABLE rc (cid BIGINT NOT NULL PRIMARY KEY, x BIGINT){part}"));
        let e = c.simple("ALTER TABLE rc ADD FOREIGN KEY (x) REFERENCES rp (alt)").expect_err("ADD");
        assert_eq!(e.code, "42830", "{e}");
        let e = c.simple("ALTER TABLE rc ADD FOREIGN KEY (x) REFERENCES nowhere (id)").expect_err("ADD");
        assert_eq!(e.code, "42P01", "{e}");
        let e = c.simple("ALTER TABLE rc ADD FOREIGN KEY (nope) REFERENCES rp (id)").expect_err("ADD");
        assert_eq!(e.code, "42703", "{e}");
        c.ok("ALTER TABLE rc ADD FOREIGN KEY (x) REFERENCES rp (id)");
        c.ok("DROP TABLE rc");
    }
    // A plain table's key naming a table not created yet is accepted, as it
    // always was; a partitioned table then created under that name must be
    // referenced by its partition key.
    c.ok("CREATE TABLE early (cid BIGINT NOT NULL PRIMARY KEY, x BIGINT REFERENCES later (alt))");
    // No table has the name yet, so nothing depends on it.
    c.ok("DROP TABLE IF EXISTS later");
    let e = c
        .simple("CREATE TABLE later (id BIGINT NOT NULL PRIMARY KEY, alt BIGINT NOT NULL) PARTITION BY HASH (id)")
        .expect_err("later");
    assert_eq!(e.code, "42830", "{e}");
    c.ok("CREATE TABLE later (alt BIGINT NOT NULL PRIMARY KEY) PARTITION BY HASH (alt) PARTITIONS 4");
    c.ok("INSERT INTO later (alt) VALUES (5)");
    c.ok("INSERT INTO early (cid, x) VALUES (1, 5)");
    assert_eq!(c.simple("INSERT INTO early (cid, x) VALUES (2, 6)").unwrap_err().code, "23503");
    // Self-reference on a partitioned table.
    c.ok("CREATE TABLE tree (id BIGINT NOT NULL PRIMARY KEY, up BIGINT REFERENCES tree (id) ON DELETE CASCADE) \
          PARTITION BY HASH (id) PARTITIONS 4");
    c.ok("INSERT INTO tree (id, up) VALUES (1, NULL)");
    c.ok("INSERT INTO tree (id, up) VALUES (2, 1), (3, 2), (4, 1)");
    assert_eq!(c.simple("INSERT INTO tree (id, up) VALUES (5, 99)").unwrap_err().code, "23503");
    c.ok("DELETE FROM tree WHERE id = 1");
    assert_eq!(c.ok("SELECT count(*) FROM tree").rows, vec![vec![Some("0".to_string())]]);
}

/// `DROP TABLE` of a referenced table is refused (2BP01) on every
/// configuration; with `CASCADE` the dependent keys go and the children stay,
/// unconstrained, through a restart. A self-reference never blocks.
#[test]
fn dropping_a_referenced_parent_is_refused_or_cascades_to_the_keys() {
    let dir = tempfile::tempdir().unwrap();
    let mut q = Quad::start(dir.path());
    create_parent(&mut q);
    create_child(&mut q, "ON DELETE CASCADE");
    q.ok("INSERT INTO {c} (cid, pid, v) VALUES (1, 1, 1), (2, 2, 2)");
    q.err("DROP TABLE {p}", "2BP01");
    q.err("DROP TABLE {p} RESTRICT", "2BP01");
    q.same_rows();
    q.ok("DROP TABLE {p} CASCADE");
    q.ok("INSERT INTO {c} (cid, pid, v) VALUES (3, 999, 3)");
    q.restart();
    q.ok("INSERT INTO {c} (cid, pid, v) VALUES (4, 998, 4)");
    q.ok("SELECT cid, pid FROM {c} ORDER BY cid");
    // A table created under the name is no parent of the old keys.
    create_parent(&mut q);
    q.ok("INSERT INTO {c} (cid, pid, v) VALUES (5, 997, 5)");
    q.ok("DROP TABLE {p}");
}

/// RENAME of either side keeps the key working: the parent renamed (the
/// children's records follow, the reverse index with them), then the child.
/// A DROP COLUMN of the child's key column drops the key with it; one of the
/// referenced column is refused (2BP01) while a key names it.
#[test]
fn renames_and_drop_column_keep_the_key_or_take_it_away() {
    let dir = tempfile::tempdir().unwrap();
    let mut q = Quad::start(dir.path());
    create_parent(&mut q);
    create_child(&mut q, "ON DELETE CASCADE");
    q.ok("INSERT INTO {c} (cid, pid, v) VALUES (1, 1, 1), (2, 2, 2), (3, 2, 3)");
    q.ok("ALTER TABLE {p} RENAME TO {p}_r");
    q.err("INSERT INTO {c} (cid, pid, v) VALUES (4, 404, 4)", "23503");
    q.ok("DELETE FROM {p}_r WHERE id = 2");
    q.ok("SELECT cid, pid FROM {c} ORDER BY cid");
    q.restart();
    q.err("INSERT INTO {c} (cid, pid, v) VALUES (4, 404, 4)", "23503");
    q.ok("ALTER TABLE {c} RENAME TO {c}_r");
    q.err("INSERT INTO {c}_r (cid, pid, v) VALUES (4, 404, 4)", "23503");
    q.ok("DELETE FROM {p}_r WHERE id = 1");
    q.ok("SELECT cid, pid FROM {c}_r ORDER BY cid");
    q.err("DROP TABLE {p}_r", "2BP01");
    q.ok("ALTER TABLE {c}_r RENAME TO {c}");
    q.ok("ALTER TABLE {p}_r RENAME TO {p}");
    q.ok("INSERT INTO {c} (cid, pid, v) VALUES (5, 5, 5)");
    q.err("INSERT INTO {c} (cid, pid, v) VALUES (6, 606, 6)", "23503");
    // The referenced column: a key names it (2BP01); on a partitioned parent
    // it is the partition key, which is never dropped (0A000).
    for (i, (suffix, part_parent, _)) in CONFIGS.iter().enumerate() {
        let e = q.on(i, "ALTER TABLE {p} DROP COLUMN id").expect_err("referenced column");
        let want = if *part_parent { "0A000" } else { "2BP01" };
        assert_eq!(e.code, want, "{suffix}: {e}");
    }
    // The child's key column: the key goes with it, and the parent is free.
    q.ok("ALTER TABLE {c} DROP COLUMN pid");
    q.ok("SELECT cid, v FROM {c} ORDER BY cid");
    q.restart();
    q.ok("DELETE FROM {p} WHERE id = 5");
    q.ok("DROP TABLE {p}");
    q.ok("SELECT cid, v FROM {c} ORDER BY cid");
}

/// The race a referenced-key lock closes, as on a plain table, under the
/// parent's logical name whichever side is partitioned: a child inserted in
/// an open block holds its parent's key, so the parent's DELETE waits for
/// it and then refuses (NO ACTION); a parent deleted in an open block holds
/// the key, so the child's INSERT waits and then refuses. Neither order
/// leaves an orphan.
#[test]
fn a_child_insert_racing_a_parent_delete_leaves_no_orphan() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path().to_str().unwrap());
    let port = server.port;
    let name = server.db.name.clone();
    for (suffix, part_parent, part_child) in CONFIGS {
        let mut a = server.connect();
        a.ok(&render(
            &format!("CREATE TABLE {{p}} (id BIGINT NOT NULL PRIMARY KEY, label TEXT){}", partitioned("id", part_parent)),
            suffix,
        ));
        a.ok(&render(
            &format!(
                "CREATE TABLE {{c}} (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT REFERENCES {{p}} (id)){}",
                partitioned("cid", part_child)
            ),
            suffix,
        ));
        let values: Vec<String> = (1..=16).map(|i| format!("({i}, 'p{i}')")).collect();
        a.ok(&render(&format!("INSERT INTO {{p}} (id, label) VALUES {}", values.join(", ")), suffix));

        // The child first: its block holds key 7.
        a.ok("BEGIN");
        a.ok(&render("INSERT INTO {c} (cid, pid) VALUES (1, 7)", suffix));
        let (tx, rx) = std::sync::mpsc::channel();
        let delete = render("DELETE FROM {p} WHERE id = 7", suffix);
        let (n, p) = (name.clone(), port);
        let deleter = std::thread::spawn(move || {
            let mut b = Client::connect(p, &n);
            tx.send(b.simple(&delete)).unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_millis(400)).is_err(), "{suffix}: the DELETE waits for the child's block");
        a.ok("COMMIT");
        let e = rx.recv_timeout(Duration::from_secs(30)).expect("the DELETE ends").expect_err("a child holds it");
        assert_eq!(e.code, "23503", "{suffix}: {e}");
        deleter.join().unwrap();

        // The parent first: its block holds key 8.
        let mut b = Client::connect(port, &name);
        b.ok("BEGIN");
        b.ok(&render("DELETE FROM {p} WHERE id = 8", suffix));
        let (tx, rx) = std::sync::mpsc::channel();
        let insert = render("INSERT INTO {c} (cid, pid) VALUES (2, 8)", suffix);
        let (n, p) = (name.clone(), port);
        let inserter = std::thread::spawn(move || {
            let mut c = Client::connect(p, &n);
            tx.send(c.simple(&insert)).unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_millis(400)).is_err(), "{suffix}: the INSERT waits for the DELETE's block");
        b.ok("COMMIT");
        let e = rx.recv_timeout(Duration::from_secs(30)).expect("the INSERT ends").expect_err("its parent is gone");
        assert_eq!(e.code, "23503", "{suffix}: {e}");
        inserter.join().unwrap();

        let orphans = a.ok(&render(
            "SELECT count(*) FROM {c} WHERE pid IS NOT NULL AND pid NOT IN (SELECT id FROM {p})",
            suffix,
        ));
        assert_eq!(orphans.rows, vec![vec![Some("0".to_string())]], "{suffix}");
        let kept = a.ok(&render("SELECT cid, pid FROM {c} ORDER BY cid", suffix));
        assert_eq!(kept.rows, vec![vec![Some("1".to_string()), Some("7".to_string())]], "{suffix}");
    }
}

/// The value forms an ORM writes into a key column (P4 round 2): a
/// negative id, alone, cast (`-7::bigint`, `CAST(-6 AS BIGINT)`) or quoted;
/// a text key cast (`'x'::text`, `CAST('a' AS TEXT)`) or holding a quote; a
/// UUID in upper case, cast, or without hyphens; a NUMERIC child column
/// referencing a BIGINT key; and each through a prepared statement. The
/// router reads no key point in a signed number, so a partitioned parent's
/// check routes such a value by the bytes an INSERT of it stores; each
/// answer must be the plain twin's.
#[test]
fn every_literal_form_of_a_key_answers_like_the_plain_twin() {
    let dir = tempfile::tempdir().unwrap();
    let mut q = Quad::start(dir.path());
    let uuid = "6f1c0b2e-8a4d-4c3b-9e7a-1d2f3a4b5c6d";
    for (i, (_, part_parent, part_child)) in CONFIGS.iter().enumerate() {
        for (suffix, column) in [("i", "BIGINT"), ("t", "TEXT"), ("u", "UUID")] {
            let parent = format!(
                "CREATE TABLE {{p}}{suffix} (k {column} NOT NULL PRIMARY KEY, label TEXT){}",
                partitioned("k", *part_parent)
            );
            q.on(i, &parent).unwrap_or_else(|e| panic!("{parent}: {e}"));
            let child = format!(
                "CREATE TABLE {{c}}{suffix} (cid BIGINT NOT NULL PRIMARY KEY, k {column} REFERENCES {{p}}{suffix} (k)){}",
                partitioned("cid", *part_child)
            );
            q.on(i, &child).unwrap_or_else(|e| panic!("{child}: {e}"));
        }
        // A NUMERIC column cannot reference a BIGINT key (42804, as in
        // PostgreSQL); an INT one can.
        let child = format!(
            "CREATE TABLE {{c}}n (cid BIGINT NOT NULL PRIMARY KEY, k NUMERIC(10, 0) REFERENCES {{p}}i (k)){}",
            partitioned("cid", *part_child)
        );
        assert_eq!(q.on(i, &child).expect_err("NUMERIC onto BIGINT").code, "42804", "{child}");
        let child = format!(
            "CREATE TABLE {{c}}n (cid BIGINT NOT NULL PRIMARY KEY, k INT REFERENCES {{p}}i (k)){}",
            partitioned("cid", *part_child)
        );
        q.on(i, &child).unwrap_or_else(|e| panic!("{child}: {e}"));
    }
    let ids: Vec<String> = (-12..=12).filter(|k| *k != 0).map(|k| format!("({k}, 'p{k}')")).collect();
    q.ok(&format!("INSERT INTO {{p}}i (k, label) VALUES {}", ids.join(", ")));
    q.ok("INSERT INTO {p}t (k, label) VALUES ('a', 'a'), ('x', 'x'), ('it''s', 'q'), ('-5', 'n')");
    q.ok(&format!("INSERT INTO {{p}}u (k, label) VALUES ('{uuid}', 'u')"));

    for (n, value) in [
        "-7", "-7::bigint", "CAST(-6 AS BIGINT)", "'-5'", "-4::int", "(-3)", "-2::BIGINT", "+2", "-999::bigint",
        "CAST(-998 AS BIGINT)", "'-997'", "11 - 20",
    ]
    .iter()
    .enumerate()
    {
        q.all(&format!("INSERT INTO {{c}}i (cid, k) VALUES ({n}, {value})"));
    }
    for (n, value) in ["'x'::text", "'x'::varchar", "CAST('a' AS TEXT)", "'it''s'", "'-5'", "'nope'::text", "'X'"]
        .iter()
        .enumerate()
    {
        q.all(&format!("INSERT INTO {{c}}t (cid, k) VALUES ({n}, {value})"));
    }
    let upper = uuid.to_ascii_uppercase();
    let bare = uuid.replace('-', "");
    for (n, value) in [
        format!("'{uuid}'"),
        format!("'{upper}'"),
        format!("'{uuid}'::uuid"),
        format!("CAST('{uuid}' AS UUID)"),
        format!("'{bare}'"),
        "'00000000-0000-0000-0000-000000000001'::uuid".to_string(),
    ]
    .iter()
    .enumerate()
    {
        q.all(&format!("INSERT INTO {{c}}u (cid, k) VALUES ({n}, {value})"));
    }
    for (n, value) in ["-7", "-7::int", "CAST(-6 AS INT)", "'-5'", "-999"].iter().enumerate() {
        q.all(&format!("INSERT INTO {{c}}n (cid, k) VALUES ({n}, {value})"));
    }
    // Prepared, as an ORM binds `$n` (text parameters).
    for (i, (suffix, _, _)) in CONFIGS.iter().enumerate() {
        let sql = render("INSERT INTO {c}i (cid, k) VALUES ($1, $2)", suffix);
        q.clients[i].prepare("ins", &sql).unwrap();
    }
    for (cid, k) in [("100", "-8"), ("101", "-12"), ("102", "-13")] {
        let outcomes: Vec<Outcome> = (0..CONFIGS.len())
            .map(|i| q.clients[i].execute_prepared("ins", &[Some(cid.to_string()), Some(k.to_string())]))
            .collect();
        for (i, o) in outcomes.iter().enumerate().skip(1) {
            match (&outcomes[0], o) {
                (Ok(a), Ok(b)) => assert_eq!(a.tags, b.tags, "prepared {k}: {}", CONFIGS[i].0),
                (Err(a), Err(b)) => assert_eq!(a.code, b.code, "prepared {k}: {}", CONFIGS[i].0),
                (a, b) => panic!("prepared {k}: twin {a:?}, {} {b:?}", CONFIGS[i].0),
            }
        }
    }
    for t in ["i", "t", "u", "n"] {
        q.ok(&format!("SELECT cid, k FROM {{c}}{t} ORDER BY cid"));
    }
    // The parents' deletes see those children, routed and fanned out.
    q.all("DELETE FROM {p}i WHERE k = -7");
    q.all("DELETE FROM {p}i WHERE k IN (-1, -9)");
    q.all("DELETE FROM {p}t WHERE k = 'x'");
    q.all(&format!("DELETE FROM {{p}}u WHERE k = '{uuid}'"));
    q.restart();
    q.all("INSERT INTO {c}i (cid, k) VALUES (200, -6::bigint)");
    q.all("INSERT INTO {c}i (cid, k) VALUES (201, -1::bigint)");
    q.same_rows_of("{c}i");
}

/// A key between columns of incompatible types is refused with 42804, as
/// PostgreSQL refuses it ("foreign key constraint … cannot be implemented"),
/// at CREATE TABLE and at ADD CONSTRAINT, on every configuration: integers
/// of any width reference each other, text types each other, a UUID a UUID;
/// across those families nothing. Nothing is written.
#[test]
fn a_key_between_incompatible_types_is_42804() {
    let dir = tempfile::tempdir().unwrap();
    let mut q = Quad::start(dir.path());
    for (i, (_, part_parent, _)) in CONFIGS.iter().enumerate() {
        for (suffix, column) in [("i", "BIGINT"), ("t", "TEXT"), ("u", "UUID")] {
            let parent = format!(
                "CREATE TABLE {{p}}{suffix} (k {column} NOT NULL PRIMARY KEY){}",
                partitioned("k", *part_parent)
            );
            q.on(i, &parent).unwrap_or_else(|e| panic!("{parent}: {e}"));
        }
    }
    for (child_type, parent, ok) in [
        ("TEXT", "i", false),
        ("UUID", "i", false),
        ("NUMERIC(12, 0)", "i", false),
        ("DOUBLE PRECISION", "i", false),
        ("INT", "t", false),
        ("UUID", "t", false),
        ("TEXT", "u", false),
        ("BIGINT", "u", false),
        ("SMALLINT", "i", true),
        ("INT", "i", true),
        ("VARCHAR(20)", "t", true),
        ("UUID", "u", true),
    ] {
        for (i, (_, _, part_child)) in CONFIGS.iter().enumerate() {
            let create = format!(
                "CREATE TABLE {{c}}x (cid BIGINT NOT NULL PRIMARY KEY, k {child_type} REFERENCES {{p}}{parent} (k)){}",
                partitioned("cid", *part_child)
            );
            match (q.on(i, &create), ok) {
                (Ok(_), true) => {}
                (Err(e), false) => assert_eq!(e.code, "42804", "{create}: {e}"),
                (other, _) => panic!("{create}: {other:?}"),
            }
            if ok {
                q.on(i, "DROP TABLE {c}x").unwrap();
            }
            let create = format!(
                "CREATE TABLE {{c}}x (cid BIGINT NOT NULL PRIMARY KEY, k {child_type}){}",
                partitioned("cid", *part_child)
            );
            q.on(i, &create).unwrap_or_else(|e| panic!("{create}: {e}"));
        }
        let add = format!("ALTER TABLE {{c}}x ADD FOREIGN KEY (k) REFERENCES {{p}}{parent} (k)");
        if ok {
            q.ok(&add);
        } else {
            q.err(&add, "42804");
            // Nothing was added: the column takes any value.
            q.ok("SELECT count(*) FROM {c}x");
        }
        q.ok("DROP TABLE {c}x");
    }
}
