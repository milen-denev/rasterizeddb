//! ALTER rewrites as one reload, and a block's DDL giving its ACCESS
//! EXCLUSIVE back (slice B5, docs/persistent-indexes.md §7 "As built (B5)";
//! R5, finding L5, V10 case 12).
//!
//! Driven through `execute_recorded` as `ddl_lock_tests` is.

use std::time::Duration;

use super::owner_tests::{Session, ok, send, session};
use super::*;
use tokio::io::AsyncReadExt;

const SETTLE: Duration = Duration::from_millis(400);
const BOUND: Duration = Duration::from_secs(20);

async fn db() -> Arc<Database> {
    crate::replication::test_support::in_memory_database().await
}

type Rows = Vec<Vec<Option<String>>>;

/// Runs one statement and reads its rows as text: `Ok(rows)` or the error.
async fn query(db: &Arc<Database>, s: &mut Session, sql: &str) -> Result<Rows, String> {
    Box::pin(execute_recorded(db, &mut s.state, &mut s.server, sql, &[], false, None))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut rows = Vec::new();
    loop {
        let kind = s.client.read_u8().await.unwrap();
        let len = s.client.read_i32().await.unwrap() as usize;
        let mut body = vec![0u8; len - 4];
        s.client.read_exact(&mut body).await.unwrap();
        match kind {
            b'D' => {
                let n = i16::from_be_bytes([body[0], body[1]]) as usize;
                let mut at = 2;
                let mut row = Vec::with_capacity(n);
                for _ in 0..n {
                    let l = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
                    at += 4;
                    if l < 0 {
                        row.push(None);
                    } else {
                        row.push(Some(String::from_utf8_lossy(&body[at..at + l as usize]).to_string()));
                        at += l as usize;
                    }
                }
                rows.push(row);
            }
            b'C' => return Ok(rows),
            b'E' => return Err(String::from_utf8_lossy(&body).to_string()),
            _ => {}
        }
    }
}

async fn rows(db: &Arc<Database>, s: &mut Session, sql: &str) -> Rows {
    query(db, s, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// `(id, v)` rows as `"id=v"` strings, for readable assertions.
fn pairs(rows: &Rows) -> Vec<String> {
    rows.iter()
        .map(|r| {
            r.iter()
                .map(|c| c.clone().unwrap_or_else(|| "NULL".to_string()))
                .collect::<Vec<_>>()
                .join("=")
        })
        .collect()
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

fn sqlstate_of(error: &str) -> &str {
    error.split('\0').find_map(|f| f.strip_prefix('C')).unwrap_or("")
}

type Running = tokio::task::JoinHandle<(Session, Result<usize, String>)>;

fn spawn(db: &Arc<Database>, s: Session, sql: &str) -> Running {
    let (db, sql) = (db.clone(), sql.to_string());
    tokio::spawn(async move {
        let mut s = s;
        let out = send(&db, &mut s, &sql).await;
        (s, out)
    })
}

async fn finish(what: &str, running: Running) -> (Session, Result<usize, String>) {
    tokio::time::timeout(BOUND, running)
        .await
        .unwrap_or_else(|_| panic!("{what}: did not finish within {BOUND:?}"))
        .expect("the task panicked")
}

/// A session whose waits for a table lock fail after `ms` (55P03).
async fn bounded(db: &Arc<Database>, ms: u64) -> Session {
    let mut s = session(db);
    ok(db, &mut s, &format!("SET lock_timeout = '{ms}ms'")).await;
    s
}

/// R5: an ALTER rewrite keeps each version's visibility.
///
/// Around `ALTER TABLE r5 ADD COLUMN extra INT DEFAULT 7` run by block A,
/// which has deleted committed row 1, updated committed row 2 and inserted
/// row 8: committed rows (whoever committed them) stay; rows deleted or
/// updated by transactions committed before the ALTER stay deleted or
/// updated (row 6, row 4); an aborted transaction's row (7) stays invisible;
/// A sees its own changes; a reader before A's end waits (A keeps the
/// ALTER's ACCESS EXCLUSIVE to its end, `database::block_ddl`). After A's
/// COMMIT, a REPEATABLE READ snapshot taken before all of it, by a block
/// that never touched `r5` (so the ALTER did not wait for it), reads
/// exactly the rows it read before, in the new shape, and a new snapshot
/// reads A's. After A's ROLLBACK the table is the one from before the
/// ALTER, for both (V10 case 12).
///
/// Mutations, each failing an assertion here: every version written frozen
/// (the old snapshot sees A's row 8 after the commit); every version written
/// under the caller's id (the old snapshot loses the committed rows); `xmax`
/// dropped (deleted versions come back); only versions visible now copied
/// (the old snapshot loses row 6).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rewrite_keeps_every_versions_visibility() {
    for end in ["COMMIT", "ROLLBACK"] {
        rewrite_visibility(end).await;
    }
}

async fn rewrite_visibility(end: &str) {
    let db = db().await;
    let mut setup = session(&db);
    for sql in [
        "CREATE TABLE r5 (id INT PRIMARY KEY, v TEXT)",
        "CREATE TABLE r5_side (id INT PRIMARY KEY)",
        "INSERT INTO r5_side (id) VALUES (1)",
    ] {
        ok(&db, &mut setup, sql).await;
    }
    for i in 1..=6 {
        ok(&db, &mut setup, &format!("INSERT INTO r5 (id, v) VALUES ({i}, 'v{i}')")).await;
    }
    let all = "SELECT id, v FROM r5 ORDER BY id";
    let before = strs(&["1=v1", "2=v2", "3=v3", "4=v4", "5=v5", "6=v6"]);

    assert_eq!(pairs(&rows(&db, &mut setup, all).await), before);
    // S: a snapshot older than everything below, taken by reading another
    // table, so S holds nothing on r5 and the ALTER does not wait for it.
    let mut s = session(&db);
    ok(&db, &mut s, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
    ok(&db, &mut s, "SELECT * FROM r5_side").await;

    // C: committed after S's snapshot, before the ALTER.
    ok(&db, &mut setup, "DELETE FROM r5 WHERE id = 6").await;
    ok(&db, &mut setup, "UPDATE r5 SET v = 'c4' WHERE id = 4").await;
    // D: aborted.
    let mut d = session(&db);
    ok(&db, &mut d, "BEGIN").await;
    ok(&db, &mut d, "INSERT INTO r5 (id, v) VALUES (7, 'd7')").await;
    ok(&db, &mut d, "ROLLBACK").await;

    // A: writes, then the rewrite, in one block.
    let mut a = session(&db);
    ok(&db, &mut a, "BEGIN").await;
    ok(&db, &mut a, "DELETE FROM r5 WHERE id = 1").await;
    ok(&db, &mut a, "UPDATE r5 SET v = 'a2' WHERE id = 2").await;
    ok(&db, &mut a, "INSERT INTO r5 (id, v) VALUES (8, 'a8')").await;
    ok(&db, &mut a, "ALTER TABLE r5 ADD COLUMN extra INT DEFAULT 7").await;

    // Before A's end: a reader waits for it.
    let mut r = bounded(&db, 300).await;
    let e = query(&db, &mut r, "SELECT id, v, extra FROM r5 ORDER BY id")
        .await
        .expect_err("a reader went on beside the block's uncommitted ALTER");
    assert_eq!(sqlstate_of(&e), "55P03", "{e}");

    // A sees its own changes.
    assert_eq!(
        pairs(&rows(&db, &mut a, "SELECT id, v, extra FROM r5 ORDER BY id").await),
        strs(&["2=a2=7", "3=v3=7", "4=c4=7", "5=v5=7", "8=a8=7"]),
        "the block's own view after its rewrite"
    );

    ok(&db, &mut a, end).await;
    let committed = strs(&["1=v1", "2=v2", "3=v3", "4=c4", "5=v5"]);
    let mut r = session(&db);
    if end == "COMMIT" {
        // S's snapshot predates C, D and A: the rows it read, in the new shape.
        assert_eq!(pairs(&rows(&db, &mut s, all).await), before, "a snapshot older than the rewrite");
        assert_eq!(
            pairs(&rows(&db, &mut s, "SELECT id, extra FROM r5 WHERE id = 6").await),
            strs(&["6=7"]),
            "the old snapshot's row reads the new column's default"
        );
        assert_eq!(
            pairs(&rows(&db, &mut r, "SELECT id, v, extra FROM r5 ORDER BY id").await),
            strs(&["2=a2=7", "3=v3=7", "4=c4=7", "5=v5=7", "8=a8=7"]),
            "after the block's commit"
        );
    } else {
        // The table from before the ALTER: no column, none of A's changes.
        assert_eq!(pairs(&rows(&db, &mut r, all).await), committed, "after the block's rollback");
        assert!(query(&db, &mut r, "SELECT extra FROM r5").await.is_err(), "the column outlived the rollback");
        assert_eq!(pairs(&rows(&db, &mut r, "SELECT COUNT(*) FROM r5").await), strs(&["5"]));
        assert_eq!(pairs(&rows(&db, &mut s, all).await), before, "the old snapshot after the rollback");
    }
    ok(&db, &mut s, "COMMIT").await;
    let now = if end == "COMMIT" { strs(&["2=a2", "3=v3", "4=c4", "5=v5", "8=a8"]) } else { committed };
    assert_eq!(pairs(&rows(&db, &mut s, all).await), now, "a new snapshot after everything");
}

/// The rewrite reads after its lock: a block's INSERT that commits while
/// the ALTER waits for it is in the rewritten table (V10 case 5).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_insert_committed_while_the_alter_waits_survives() {
    let db = db().await;
    let mut setup = session(&db);
    ok(&db, &mut setup, "CREATE TABLE waits (id INT PRIMARY KEY)").await;
    ok(&db, &mut setup, "INSERT INTO waits (id) VALUES (1)").await;
    let mut b = session(&db);
    ok(&db, &mut b, "BEGIN").await;
    ok(&db, &mut b, "INSERT INTO waits (id) VALUES (10)").await;
    let alter = spawn(&db, session(&db), "ALTER TABLE waits ADD COLUMN extra INT DEFAULT 3");
    tokio::time::sleep(SETTLE).await;
    assert!(!alter.is_finished(), "the ALTER ran beside a block that wrote the table");
    ok(&db, &mut b, "INSERT INTO waits (id) VALUES (11)").await;
    ok(&db, &mut b, "COMMIT").await;
    let (_, out) = finish("ALTER", alter).await;
    out.expect("the ALTER after the block");
    assert_eq!(
        pairs(&rows(&db, &mut setup, "SELECT id, extra FROM waits ORDER BY id").await),
        strs(&["1=3", "10=3", "11=3"])
    );
}

/// A block's DDL keeps its ACCESS EXCLUSIVE to the block's end, as
/// PostgreSQL's does (2026-11-09, `database::block_ddl`): the change may yet
/// roll back, so nobody else reads or writes the table until the block ends.
/// (Finding L5 had it given back at the statement's end, which was only
/// sound while DDL could not roll back.) Whether the block wrote the table,
/// read it first, or locked it itself, another session's reader waits
/// (55P03 at its `lock_timeout`), a NOWAIT ACCESS EXCLUSIVE fails, and a
/// CREATE INDEX waits and builds once the block commits, over the new shape.
///
/// Guards against `BlockDdl::end` running for a transactional DDL (the reader goes
/// on beside the uncommitted ALTER).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_ddl_keeps_its_access_exclusive_to_the_blocks_end() {
    let db = db().await;
    let mut setup = session(&db);
    for t in ["l5_w", "l5_n", "l5_r", "l5_l"] {
        ok(&db, &mut setup, &format!("CREATE TABLE {t} (id INT PRIMARY KEY, x INT)")).await;
        ok(&db, &mut setup, &format!("INSERT INTO {t} (id, x) VALUES (1, 1)")).await;
    }
    let nowait_ae = |t: &str| format!("LOCK TABLE {t} IN ACCESS EXCLUSIVE MODE NOWAIT");
    for (table, before) in [
        ("l5_w", Some("INSERT INTO l5_w (id, x) VALUES (2, 2)")),
        ("l5_n", None),
        ("l5_r", Some("SELECT * FROM l5_r")),
        ("l5_l", Some("LOCK TABLE l5_l IN ACCESS EXCLUSIVE MODE")),
    ] {
        let mut a = session(&db);
        ok(&db, &mut a, "BEGIN").await;
        if let Some(sql) = before {
            ok(&db, &mut a, sql).await;
        }
        ok(&db, &mut a, &format!("ALTER TABLE {table} ADD COLUMN c INT")).await;
        let mut r = bounded(&db, 300).await;
        let e = query(&db, &mut r, &format!("SELECT id FROM {table}"))
            .await
            .expect_err("a reader went on beside the block's uncommitted ALTER");
        assert_eq!(sqlstate_of(&e), "55P03", "{table}: {e}");
        let mut o = session(&db);
        ok(&db, &mut o, "BEGIN").await;
        let e = send(&db, &mut o, &nowait_ae(table)).await.expect_err("AE granted beside the block's ALTER");
        assert_eq!(sqlstate_of(&e), "55P03", "{table}: {e}");
        ok(&db, &mut o, "ROLLBACK").await;
        let index = spawn(&db, session(&db), &format!("CREATE INDEX {table}_c ON {table} (c)"));
        tokio::time::sleep(SETTLE).await;
        assert!(!index.is_finished(), "{table}: CREATE INDEX ran beside the block's uncommitted ALTER");
        ok(&db, &mut a, "COMMIT").await;
        let (_, out) = finish("CREATE INDEX", index).await;
        out.expect("CREATE INDEX after the block, over its new column");
        let mut r = bounded(&db, 2_000).await;
        query(&db, &mut r, &format!("SELECT c FROM {table}")).await.expect("the committed column");
    }
}

/// A table a block creates is the block's until it commits: another
/// session's reader or writer waits for the block (PostgreSQL's does not see
/// the table at all, 42P01; either way nobody else's row can land in a
/// table the block's ROLLBACK drops), and after the ROLLBACK the table is
/// gone; after a COMMIT it is there for everyone.
///
/// Guards against the created table's ACCESS EXCLUSIVE being handed back at the
/// statement's end (the other session's INSERT commits into it, and the
/// ROLLBACK drops a committed row).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_a_block_creates_is_its_own_until_it_ends() {
    for end in ["ROLLBACK", "COMMIT"] {
        let db = db().await;
        let mut a = session(&db);
        ok(&db, &mut a, "BEGIN").await;
        ok(&db, &mut a, "CREATE TABLE made (id INT PRIMARY KEY)").await;
        let mut o = bounded(&db, 300).await;
        for sql in ["SELECT id FROM made", "INSERT INTO made (id) VALUES (1)"] {
            let e = query(&db, &mut o, sql).await.expect_err(&format!("{end}: {sql} went on beside the block's CREATE"));
            assert!(matches!(sqlstate_of(&e), "55P03" | "42P01"), "{end}: {sql}: {e}");
        }
        ok(&db, &mut a, end).await;
        let mut r = session(&db);
        let seen = query(&db, &mut r, "SELECT COUNT(*) FROM made").await;
        if end == "COMMIT" {
            assert_eq!(pairs(&seen.expect("the committed table")), strs(&["0"]));
        } else {
            assert!(seen.is_err(), "the created table outlived the rollback");
        }
    }
}
