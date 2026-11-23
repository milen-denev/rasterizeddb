//! A partitioned table on a key/value store over the wire
//! (docs/kv-partitions.md §1): `STORAGE PLACE TABLE t AT 'kv:<store>'`
//! before `CREATE TABLE t … PARTITION BY HASH`, then routed and fanned-out
//! INSERT, SELECT, UPDATE, DELETE, ON CONFLICT, RETURNING and VACUUM, each
//! answering what a plain twin with the same rows answers; through a
//! restart, idle closes and reopens, `STORAGE MOVE PARTITION` off the store
//! to a directory and back, and a DROP that leaves nothing on the store.
//!
//! The store is a local directory store, which behaves as S3 does. Bounded:
//! a few hundred rows, fixed statement lists, every wait capped.

mod partition_wire;

use std::path::Path;
use std::time::{Duration, Instant};

use partition_wire::*;

const P: &str = "access_logs";
const Q: &str = "access_plain";
const COLUMNS: &str =
    "(domain TEXT NOT NULL, ts BIGINT NOT NULL, seq BIGINT NOT NULL, path TEXT, PRIMARY KEY (domain, ts, seq))";
const DOMAINS: [&str; 6] = ["a.example", "b.example", "c.example", "d.example", "e.example", "f.example"];

fn sorted(mut rows: Vec<Vec<Option<String>>>) -> Vec<Vec<Option<String>>> {
    rows.sort();
    rows
}

/// Runs `sql` (with `{t}` for the table) on both tables and asserts the
/// answers agree: rows (sorted), columns and command tags.
fn both(c: &mut Client, sql: &str) -> Reply {
    let p = c.simple(&sql.replace("{t}", P)).unwrap_or_else(|e| panic!("{P}: {sql}: {e}"));
    let q = c.simple(&sql.replace("{t}", Q)).unwrap_or_else(|e| panic!("{Q}: {sql}: {e}"));
    assert_eq!(sorted(p.rows.clone()), sorted(q.rows.clone()), "{sql}");
    assert_eq!(p.columns, q.columns, "{sql}");
    assert_eq!(p.tags, q.tags, "{sql}");
    p
}

/// Every read the tables are compared by, each of which must answer rows.
fn compare_reads(c: &mut Client, what: &str) {
    let all = both(c, "SELECT domain, ts, seq, path FROM {t}");
    assert!(!all.rows.is_empty(), "{what}: the table is empty");
    both(c, "SELECT count(*) FROM {t}");
    for d in DOMAINS {
        both(c, &format!("SELECT ts, seq, path FROM {{t}} WHERE domain = '{d}'"));
    }
    both(c, "SELECT domain, count(*), max(ts) FROM {t} GROUP BY domain");
    both(c, "SELECT domain, ts, seq FROM {t} WHERE ts > 50 AND path IS NOT NULL");
}

/// Where each materialized partition is, as the table reports it.
fn locations(server: &Server) -> Vec<(u32, String)> {
    let pt = server.db.partitioned_table(P).expect("the partitioned table");
    let pt2 = pt.clone();
    let infos = block_on_big(async move { pt2.partitions().await.expect("its partitions") });
    infos.into_iter().filter_map(|p| p.location.map(|l| (p.pid, l))).collect()
}

fn holds_objects(dir: &Path) -> bool {
    std::fs::read_dir(dir).into_iter().flatten().flatten().any(|e| {
        let p = e.path();
        if p.is_dir() { holds_objects(&p) } else { true }
    })
}

/// The partition files of `uid` holding objects on the store under `kv`.
fn on_store(kv: &Path, uid: u64) -> Vec<String> {
    let prefix = format!("__pt_{uid:016x}_");
    let mut out: Vec<String> = std::fs::read_dir(kv.join("logs"))
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| holds_objects(&e.path()))
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| n.starts_with(&prefix))
        .collect();
    out.sort();
    out
}

/// The partition data files of `uid` in the directory `dir`.
fn in_dir(dir: &Path, uid: u64) -> Vec<String> {
    files_starting(dir, &format!("__pt_{uid:016x}_"))
        .into_iter()
        .filter(|n| {
            let tail = &n[format!("__pt_{uid:016x}_").len()..];
            tail.len() >= 8 && tail[..8].bytes().all(|b| b.is_ascii_hexdigit())
        })
        .collect()
}

fn close_all_partitions(server: &Server) {
    let pt = server.db.partitioned_table(P).expect("the partitioned table");
    block_on_big(async move {
        pt.close_idle().await.expect("closing idle partitions");
    });
}

#[test]
fn a_partitioned_table_on_a_store_answers_like_a_plain_one() {
    let dir = tempfile::tempdir().unwrap();
    let kv = tempfile::tempdir().unwrap();
    let mut server = Server::start(dir.path().to_str().unwrap());
    let started = Instant::now();
    {
        let mut c = server.connect();
        c.ok(&format!("STORAGE ADD STORE logs DIRECTORY WITH (PATH = '{}', PREFIX = 'logs')", kv.path().display()));
        c.ok(&format!("STORAGE PLACE TABLE {P} AT 'kv:logs'"));
        c.ok(&format!("CREATE TABLE {P} {COLUMNS} PARTITION BY HASH (domain) PARTITIONS 8"));
        c.ok(&format!("CREATE TABLE {Q} {COLUMNS}"));
        // Routed single rows and a multi-row insert across every domain.
        for (i, d) in DOMAINS.iter().enumerate() {
            both(&mut c, &format!("INSERT INTO {{t}} (domain, ts, seq, path) VALUES ('{d}', {i}, 0, '/p{i}')"));
        }
        let values: Vec<String> = (0..120)
            .map(|i| format!("('{}', {}, {}, {})", DOMAINS[i % 6], 10 + i, i % 3, if i % 7 == 0 { "NULL".into() } else { format!("'/x{i}'") }))
            .collect();
        both(&mut c, &format!("INSERT INTO {{t}} (domain, ts, seq, path) VALUES {}", values.join(", ")));
        compare_reads(&mut c, "after the inserts");
        // ON CONFLICT and RETURNING.
        both(&mut c, "INSERT INTO {t} (domain, ts, seq, path) VALUES ('a.example', 0, 0, '/dup') ON CONFLICT DO NOTHING RETURNING domain, ts");
        both(
            &mut c,
            "INSERT INTO {t} (domain, ts, seq, path) VALUES ('b.example', 1, 0, '/up'), ('b.example', 999, 0, '/new') \
             ON CONFLICT (domain, ts, seq) DO UPDATE SET path = EXCLUDED.path RETURNING domain, ts, path",
        );
        both(&mut c, "INSERT INTO {t} (domain, ts, seq, path) VALUES ('c.example', 5000, 1, '/r') RETURNING *");
        // UPDATE and DELETE, routed and fanned out, with RETURNING.
        both(&mut c, "UPDATE {t} SET path = '/routed' WHERE domain = 'd.example' AND ts < 40 RETURNING domain, ts, path");
        both(&mut c, "UPDATE {t} SET path = '/fan' WHERE ts % 5 = 0");
        both(&mut c, "DELETE FROM {t} WHERE domain = 'e.example' AND seq = 1 RETURNING domain, ts, seq");
        both(&mut c, "DELETE FROM {t} WHERE path IS NULL");
        // A block that writes several partitions, and one rolled back.
        c.ok("BEGIN");
        c.ok(&format!("INSERT INTO {P} (domain, ts, seq, path) VALUES ('f.example', 7000, 0, '/b'), ('a.example', 7000, 0, '/b')"));
        c.ok(&format!("UPDATE {P} SET path = '/blk' WHERE domain = 'c.example'"));
        c.ok("COMMIT");
        c.ok(&format!("INSERT INTO {Q} (domain, ts, seq, path) VALUES ('f.example', 7000, 0, '/b'), ('a.example', 7000, 0, '/b')"));
        c.ok(&format!("UPDATE {Q} SET path = '/blk' WHERE domain = 'c.example'"));
        c.ok("BEGIN");
        c.ok(&format!("DELETE FROM {P} WHERE domain = 'a.example'"));
        c.ok("ROLLBACK");
        c.ok(&format!("VACUUM {P}"));
        c.ok(&format!("VACUUM {Q}"));
        compare_reads(&mut c, "after the writes");
    }
    let uid = server.db.partitioned_table(P).expect("the partitioned table").uid();
    let placed = locations(&server);
    assert!(!placed.is_empty() && placed.iter().all(|(_, l)| l == "kv:logs"), "{placed:?}");
    assert_eq!(in_dir(dir.path(), uid), Vec::<String>::new(), "partition files in the root");
    assert_eq!(on_store(kv.path(), uid).len(), placed.len() * 2, "{:?}", on_store(kv.path(), uid));

    // Closed partitions reopen from the store.
    close_all_partitions(&server);
    {
        let mut c = server.connect();
        compare_reads(&mut c, "after every partition closed");
        both(&mut c, "UPDATE {t} SET path = '/reopened' WHERE domain = 'b.example'");
    }

    // A restart reads everything back from the store.
    server = server.restart();
    {
        let mut c = server.connect();
        compare_reads(&mut c, "after the restart");
        both(&mut c, "INSERT INTO {t} (domain, ts, seq, path) VALUES ('d.example', 8000, 0, '/after')");
        both(&mut c, "DELETE FROM {t} WHERE domain = 'f.example' AND ts > 100 RETURNING ts");
        compare_reads(&mut c, "writes after the restart");
    }

    // MOVE PARTITION off the store to the root, then back.
    let root = dir.path().to_str().unwrap().to_string();
    {
        let mut c = server.connect();
        c.ok(&format!("STORAGE MOVE PARTITION {P} FOR (domain = 'a.example') TO '{root}'"));
        compare_reads(&mut c, "after a move to the root");
    }
    let moved = locations(&server);
    assert!(moved.iter().any(|(_, l)| l != "kv:logs"), "no partition left the store: {moved:?}");
    assert!(!in_dir(dir.path(), uid).is_empty(), "the moved partition's files are not in the root");
    server = server.restart();
    {
        let mut c = server.connect();
        compare_reads(&mut c, "after a move to the root and a restart");
        c.ok(&format!("STORAGE MOVE PARTITION {P} FOR (domain = 'a.example') TO 'kv:logs'"));
        compare_reads(&mut c, "after a move back to the store");
        both(&mut c, "INSERT INTO {t} (domain, ts, seq, path) VALUES ('a.example', 9000, 0, '/back')");
    }
    let back = locations(&server);
    assert!(back.iter().all(|(_, l)| l == "kv:logs"), "{back:?}");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !in_dir(dir.path(), uid).is_empty() {
        assert!(Instant::now() < deadline, "the moved-back partition's root files stayed: {:?}", in_dir(dir.path(), uid));
        std::thread::sleep(Duration::from_millis(50));
    }
    server = server.restart();
    {
        let mut c = server.connect();
        compare_reads(&mut c, "after a move back and a restart");
        // A move to a store this database does not configure is refused.
        let e = c.simple(&format!("STORAGE MOVE PARTITION {P} FOR (domain = 'b.example') TO 'kv:nowhere'")).unwrap_err();
        assert!(e.message.contains("nowhere"), "{e}");
        compare_reads(&mut c, "after a refused move");
        // DROP takes every partition off the store.
        c.ok(&format!("DROP TABLE {P}"));
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while !on_store(kv.path(), uid).is_empty() {
        assert!(Instant::now() < deadline, "DROP left files on the store: {:?}", on_store(kv.path(), uid));
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(files_starting(dir.path(), &format!("__pt_{uid:016x}_")).is_empty(), "DROP left files in the root");
    assert!(started.elapsed() < Duration::from_secs(300), "the run took {:?}", started.elapsed());
}

/// Bytes of every object under `dir`, recursively.
fn tree_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else { return 0 };
    entries
        .flatten()
        .map(|e| match e.file_type() {
            Ok(t) if t.is_dir() => tree_bytes(&e.path()),
            Ok(_) => std::fs::metadata(e.path()).map(|m| m.len()).unwrap_or(0),
            Err(_) => 0,
        })
        .sum()
}

/// What a logger does to these tables: additive upserts into counters, a
/// retention DELETE of most rows, then `VACUUM FULL`. The answers stay a plain
/// twin's through the compaction and a restart, and the compaction gives the
/// store its bytes back (the truncation deletes the chunk objects past the new
/// end).
#[test]
fn vacuum_full_on_a_store_compacts_and_keeps_the_answers() {
    let dir = tempfile::tempdir().unwrap();
    let kv = tempfile::tempdir().unwrap();
    let mut server = Server::start(dir.path().to_str().unwrap());
    let started = Instant::now();
    {
        let mut c = server.connect();
        c.ok(&format!(
            "STORAGE ADD STORE logs DIRECTORY WITH (PATH = '{}', PREFIX = 'logs', CHUNK_SIZE = '64KiB', PAGE_SIZE = '16KiB', MAX_DELTAS = '4', COMPACT_CHUNK_BYTES = '8KiB')",
            kv.path().display()
        ));
        c.ok(&format!("STORAGE PLACE TABLE {P} AT 'kv:logs'"));
        c.ok(&format!("CREATE TABLE {P} {COLUMNS} PARTITION BY HASH (domain) PARTITIONS 4"));
        c.ok(&format!("CREATE TABLE {Q} {COLUMNS}"));
        // Additive upserts: the counter a charge accumulates into.
        for round in 0..3 {
            for (i, d) in DOMAINS.iter().enumerate() {
                both(
                    &mut c,
                    &format!(
                        "INSERT INTO {{t}} (domain, ts, seq, path) VALUES ('{d}', 1, 0, '{}') \
                         ON CONFLICT (domain, ts, seq) DO UPDATE SET path = {{t}}.path || EXCLUDED.path",
                        i + round
                    ),
                );
            }
        }
        compare_reads(&mut c, "after the additive upserts");
        // A log's worth of rows, most of which retention then deletes.
        let filler = "x".repeat(200);
        for batch in 0..20 {
            let values: Vec<String> = (0..100)
                .map(|i| format!("('{}', {}, {}, '{filler}')", DOMAINS[i % 6], 100 + batch * 100 + i, i % 2))
                .collect();
            both(&mut c, &format!("INSERT INTO {{t}} (domain, ts, seq, path) VALUES {}", values.join(", ")));
        }
        compare_reads(&mut c, "after the log rows");
    }
    let before = tree_bytes(kv.path());
    {
        let mut c = server.connect();
        both(&mut c, "DELETE FROM {t} WHERE ts >= 100 AND ts < 1900");
        c.ok(&format!("VACUUM FULL {P}"));
        c.ok(&format!("VACUUM FULL {Q}"));
        compare_reads(&mut c, "after the retention delete and VACUUM FULL");
        both(&mut c, "INSERT INTO {t} (domain, ts, seq, path) VALUES ('a.example', 50000, 0, '/after-full')");
        both(&mut c, "UPDATE {t} SET path = '/upd' WHERE domain = 'b.example' AND ts >= 1900");
        compare_reads(&mut c, "writes after VACUUM FULL");
    }
    // The truncation's deletes and the store's compaction run behind the
    // statement; give them a bounded while.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut after = tree_bytes(kv.path());
    while after * 2 > before && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
        after = tree_bytes(kv.path());
    }
    assert!(after * 2 <= before, "VACUUM FULL left the store at {after} bytes of {before}");
    server = server.restart();
    {
        let mut c = server.connect();
        compare_reads(&mut c, "after VACUUM FULL and a restart");
    }
    assert!(started.elapsed() < Duration::from_secs(300), "the run took {:?}", started.elapsed());
}
