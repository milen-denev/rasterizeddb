//! Prepared statements on a partitioned table over the wire (S15,
//! `docs/partitioning.md` §4.1): a learned parse stays right through
//! everything that can make a parse stale, and answers what the same
//! statement answers on a plain twin and what a model of the rows says.
//!
//! Two clients prepare every statement: one that describes each portal and
//! binds text (sqlx's shape), and one that declares its parameter types,
//! binds in binary and never describes after Parse, for which the schema
//! generation is the only thing that can invalidate a learned parse
//! (memory `prepared-select-cache`).
//!
//! The in-process tests (`pgwire::prepared_partition_tests`) pin what the wire
//! cannot see: which partitions each execution read, and that the parse is
//! skipped.

mod partition_wire;

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Mutex;

use partition_wire::*;
use rasterizeddb_core::pgwire::PREPARED_RUNS;

/// `PREPARED_RUNS` is global to the process: every test here holds this.
static COUNTED: Mutex<()> = Mutex::new(());

fn counted() -> std::sync::MutexGuard<'static, ()> {
    COUNTED.lock().unwrap_or_else(|e| e.into_inner())
}

fn runs() -> u64 {
    PREPARED_RUNS.load(Ordering::Relaxed)
}

const PART: &str = "pp";
const PLAIN: &str = "pq";
const KEY: &str = "PRIMARY KEY (tenant_id, id)";

/// The rows both tables hold: (tenant, id) -> (n, m), `n` rendered as the
/// wire renders it.
type Model = BTreeMap<(i64, i64), (String, i64)>;

fn model(f: impl Fn(i64, i64) -> (String, i64)) -> Model {
    let mut m = Model::new();
    for t in 1..=12 {
        for id in 1..=3 {
            m.insert((t, id), f(t, id));
        }
    }
    m
}

/// Creates `table` (partitioned when `partitioned`) with `columns` and fills
/// it from `rows`.
fn create(c: &mut Client, table: &str, columns: &str, partitioned: bool, rows: &Model, numeric_n: bool) {
    let tail = if partitioned { " PARTITION BY HASH (tenant_id) PARTITIONS 8" } else { "" };
    c.ok(&format!("CREATE TABLE {table} ({columns}, {KEY}){tail}"));
    let values: Vec<String> = rows
        .iter()
        .map(|((t, id), (n, m))| {
            let n = if numeric_n { n.clone() } else { format!("'{n}'") };
            format!("({t}, {id}, {n}, {m})")
        })
        .collect();
    c.ok(&format!("INSERT INTO {table} (tenant_id, id, n, m) VALUES {}", values.join(", ")));
}

/// The statements every round runs, by name, with what the model says each
/// value answers. `$1` is a BIGINT throughout.
struct Statement {
    name: &'static str,
    sql: &'static str,
    values: &'static [i64],
    expect: fn(&Model, i64) -> Vec<Vec<Option<String>>>,
}

fn point(m: &Model, t: i64) -> Vec<Vec<Option<String>>> {
    m.iter()
        .filter(|((tt, _), _)| *tt == t)
        .map(|((tt, id), (n, _))| vec![Some(tt.to_string()), Some(id.to_string()), Some(n.clone())])
        .collect()
}

fn count(m: &Model, t: i64) -> Vec<Vec<Option<String>>> {
    vec![vec![Some(m.keys().filter(|(tt, _)| *tt == t).count().to_string())]]
}

fn fan_out(m: &Model, v: i64) -> Vec<Vec<Option<String>>> {
    let hit: Vec<i64> = m.values().filter_map(|(n, _)| n.parse::<i64>().ok()).filter(|n| *n > v).collect();
    let sum = if hit.is_empty() { None } else { Some(hit.iter().sum::<i64>().to_string()) };
    vec![vec![Some(hit.len().to_string()), sum]]
}

/// Distinct values in a row, so each statement learns from its first two
/// executions and answers its last four from the cached parse.
const STATEMENTS: &[Statement] = &[
    Statement {
        name: "point",
        sql: "SELECT tenant_id, id, n FROM {t} WHERE tenant_id = $1",
        values: &[5, 7, 4, 9, 6, 8],
        expect: point,
    },
    Statement {
        name: "count",
        sql: "SELECT COUNT(*) FROM {t} WHERE tenant_id = $1",
        values: &[11, 5, 4, 999, 12, 6],
        expect: count,
    },
    Statement {
        name: "fanout",
        sql: "SELECT COUNT(*), SUM(n) FROM {t} WHERE n > $1",
        values: &[15, 37, 64, 99, 120, 3],
        expect: fan_out,
    },
];

const CACHED_PER_STATEMENT: u64 = 4;

/// The two clients on one table.
struct Sessions {
    describing: Client,
    binary: Client,
}

impl Sessions {
    fn open(server: &Server, table: &str, statements: &[&Statement]) -> Sessions {
        let mut s = Sessions { describing: server.connect(), binary: server.connect() };
        // The repair this test follows a redefinition through is
        // `rowdescription`'s; since B8b the default, `auto`, answers a
        // statement's first stale use with 0A000 instead.
        for client in [&mut s.describing, &mut s.binary] {
            client.ok("SET rdb.shape_change = rowdescription");
        }
        for st in statements {
            let sql = st.sql.replace("{t}", table);
            s.describing.prepare(st.name, &sql).unwrap_or_else(|e| panic!("prepare {sql}: {e}"));
            s.binary.prepare_typed(st.name, &sql, &[20]).unwrap_or_else(|e| panic!("prepare {sql}: {e}"));
        }
        s
    }

    /// Every value of every statement through both clients: the answers, in
    /// order, as (client, statement, value, reply).
    fn round(&mut self, what: &str, statements: &[&Statement]) -> Vec<(String, Reply)> {
        let mut out = Vec::new();
        for st in statements {
            for v in st.values {
                let d = self.describing.execute_prepared(st.name, &[Some(v.to_string())]);
                let b = self.binary.execute_binary(st.name, &[Some(v.to_be_bytes().to_vec())]);
                for (client, reply) in [("describing", d), ("binary", b)] {
                    let mut reply = reply.unwrap_or_else(|e| panic!("{what}: {client} {} {v}: {e}", st.name));
                    reply.rows.sort();
                    out.push((format!("{client} {} ${v}", st.name), reply));
                }
            }
        }
        out
    }
}

/// One step: both tables answer every statement alike, as the model says,
/// and the partitioned side's last four executions of each ran from the
/// cached parse.
fn step(what: &str, part: &mut Sessions, plain: &mut Sessions, m: &Model, statements: &[&Statement]) {
    let before = runs();
    let p = part.round(what, statements);
    let cached = runs() - before;
    let q = plain.round(what, statements);
    let want = CACHED_PER_STATEMENT * 2 * statements.len() as u64;
    assert!(cached >= want, "{what}: {cached} of the partitioned executions ran from a cached parse, want {want}");
    let mut at = 0;
    for st in statements {
        for v in st.values {
            let expect = {
                let mut e = (st.expect)(m, *v);
                e.sort();
                e
            };
            for _client in 0..2 {
                let ((label, rp), (_, rq)) = (&p[at], &q[at]);
                assert_eq!(rp.rows, expect, "{what}: {label} on the partitioned table, against the model");
                assert_eq!(rp.rows, rq.rows, "{what}: {label}, partitioned against plain");
                assert_eq!(
                    (&rp.columns, &rp.types),
                    (&rq.columns, &rq.types),
                    "{what}: {label}: the descriptions differ"
                );
                at += 1;
            }
        }
    }
}

fn numeric(t: i64, id: i64, scale: i64) -> (String, i64) {
    ((t * scale + id).to_string(), -(t * 10 + id))
}

/// A learned parse on a partitioned table stays right, as on a plain table,
/// after writes, after the table is dropped and created again under its name
/// (same shape; columns reordered, which a stale parse would read wrongly;
/// a column's type changed, which needs a fresh RowDescription), after the
/// name changes kind (partitioned to plain and back), and after a restart.
#[test]
fn prepared_reads_follow_writes_redefinitions_and_restarts() {
    let _counted = counted();
    let dir = tempfile::tempdir().unwrap();
    let mut server = Server::start(dir.path().to_str().unwrap());
    let all: Vec<&Statement> = STATEMENTS.iter().collect();
    let cols = "tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, n BIGINT, m BIGINT";
    let mut m = model(|t, id| numeric(t, id, 10));
    let mut setup = server.connect();
    create(&mut setup, PART, cols, true, &m, true);
    create(&mut setup, PLAIN, cols, false, &m, true);

    let mut part = Sessions::open(&server, PART, &all);
    let mut plain = Sessions::open(&server, PLAIN, &all);
    step("learned", &mut part, &mut plain, &m, &all);

    // Writes, committed by another session, seen by the cached parses.
    for table in [PART, PLAIN] {
        setup.ok(&format!("INSERT INTO {table} (tenant_id, id, n, m) VALUES (5, 9, 509, -59)"));
        setup.ok(&format!("UPDATE {table} SET n = n + 1000 WHERE tenant_id = 7"));
        setup.ok(&format!("DELETE FROM {table} WHERE tenant_id = 4 AND id = 1"));
    }
    m.insert((5, 9), ("509".into(), -59));
    for id in 1..=3 {
        let e = m.get_mut(&(7, id)).unwrap();
        e.0 = (e.0.parse::<i64>().unwrap() + 1000).to_string();
    }
    m.remove(&(4, 1));
    step("after writes", &mut part, &mut plain, &m, &all);

    // Dropped and created again under the same name, the same shape, other rows.
    m = model(|t, id| numeric(t, id, 1000));
    for (table, partitioned) in [(PART, true), (PLAIN, false)] {
        setup.ok(&format!("DROP TABLE {table}"));
        create(&mut setup, table, cols, partitioned, &m, true);
    }
    step("re-created, same shape", &mut part, &mut plain, &m, &all);

    // Columns reordered: `n` now sits where `m` was, so a parse kept from
    // before would read `m` under the name `n`.
    let swapped = "tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, m BIGINT, n BIGINT";
    m = model(|t, id| numeric(t, id, 100));
    for (table, partitioned) in [(PART, true), (PLAIN, false)] {
        setup.ok(&format!("DROP TABLE {table}"));
        create(&mut setup, table, swapped, partitioned, &m, true);
    }
    step("re-created, columns reordered", &mut part, &mut plain, &m, &all);

    // `n` becomes TEXT: the result type changes. The describing client is
    // sent a fresh RowDescription (memory `sqlx-ignores-0a000`), on both
    // tables alike. The fan-out compares `n` with a number, which a TEXT
    // column does not answer like a BIGINT one, so it sits this one out.
    let text = "tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, n TEXT, m BIGINT";
    m = model(|t, id| (format!("t{t}/{id}"), t));
    for (table, partitioned) in [(PART, true), (PLAIN, false)] {
        setup.ok(&format!("DROP TABLE {table}"));
        create(&mut setup, table, text, partitioned, &m, false);
    }
    let point_and_count: Vec<&Statement> = STATEMENTS.iter().filter(|s| s.name != "fanout").collect();
    step("re-created, n is TEXT", &mut part, &mut plain, &m, &point_and_count);

    // The name changes kind under the prepared statements: plain, then
    // partitioned again. The twin stays plain throughout.
    m = model(|t, id| numeric(t, id, 3));
    for partitioned in [false, true] {
        for table in [PART, PLAIN] {
            setup.ok(&format!("DROP TABLE {table}"));
            create(&mut setup, table, cols, partitioned && table == PART, &m, true);
        }
        let kind = if partitioned { "partitioned again" } else { "now plain" };
        assert_eq!(server.db.is_partitioned(PART), partitioned, "{kind}");
        step(&format!("{PART} {kind}"), &mut part, &mut plain, &m, &all);
    }

    // A restart: sessions are gone, so the statements are prepared again and
    // learn again from what was reopened.
    drop((part, plain, setup));
    server = server.restart();
    let mut part = Sessions::open(&server, PART, &all);
    let mut plain = Sessions::open(&server, PLAIN, &all);
    step("after a restart", &mut part, &mut plain, &m, &all);
    drop((part, plain));
    drop(server);
}

/// Owner constraint 1: the Describe of a prepared `SELECT` labels its columns
/// by their select-item aliases, matched by position with each item used
/// once (1099cb0), on a partitioned table as on a plain one, and every
/// cached execution's portal Describe keeps those labels.
#[test]
fn prepared_describe_labels_columns_by_alias() {
    let _counted = counted();
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path().to_str().unwrap());
    let cols = "tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, n BIGINT, m BIGINT";
    let m = model(|t, id| numeric(t, id, 10));
    let mut c = server.connect();
    create(&mut c, PART, cols, true, &m, true);
    create(&mut c, PLAIN, cols, false, &m, true);
    for (sql, names) in [
        ("SELECT n AS v, tenant_id AS t FROM {t} WHERE tenant_id = $1 AND id = $2", ["v", "t"]),
        ("SELECT tenant_id AS a, tenant_id AS b FROM {t} WHERE tenant_id = $1 AND id = $2", ["a", "b"]),
    ] {
        let mut answers: Vec<Vec<Reply>> = Vec::new();
        for table in [PART, PLAIN] {
            let text = sql.replace("{t}", table);
            let described = c.prepare("alias", &text).unwrap_or_else(|e| panic!("{text}: {e}"));
            assert_eq!(described.columns, names, "{text}: the statement Describe");
            let before = runs();
            let mut replies = vec![described];
            for (t, id) in [(5i64, 1i64), (7, 2), (4, 3), (9, 1), (6, 2)] {
                let r = c
                    .execute_prepared("alias", &[Some(t.to_string()), Some(id.to_string())])
                    .unwrap_or_else(|e| panic!("{text} ({t}, {id}): {e}"));
                assert_eq!(r.columns, names, "{text} ({t}, {id}): the portal Describe");
                let n = &m[&(t, id)].0;
                let want = if names[0] == "v" {
                    vec![vec![Some(n.clone()), Some(t.to_string())]]
                } else {
                    vec![vec![Some(t.to_string()), Some(t.to_string())]]
                };
                assert_eq!(r.rows, want, "{text} ({t}, {id})");
                replies.push(r);
            }
            assert!(runs() - before >= 3, "{text}: the cached parse never answered");
            c.close_prepared("alias");
            answers.push(replies);
        }
        for (p, q) in answers[0].iter().zip(&answers[1]) {
            assert_eq!((&p.columns, &p.types, &p.rows), (&q.columns, &q.types, &q.rows), "{sql}: partitioned against plain");
        }
    }
    drop(c);
    drop(server);
}
