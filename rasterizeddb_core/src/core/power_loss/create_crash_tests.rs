//! `CREATE TABLE` through a power cut (`database::create_intent`): every
//! image of a cut anywhere in it, under every seed kind, opens with the table
//! absent, and a `CREATE` of the name then succeeding, or present with every
//! column and every constraint, enforced. Never the table without its
//! constraints, which is what a cut inside its one log sync left when the
//! catalogue row went first (the reviewer's reproduction: 7 images of 16).
//! Judged against the catalogue, not only the sidecars, on a plain and a
//! partitioned table.

use super::*;
use parking_lot::Mutex;
use std::sync::Arc as StdArc;

const PARENT: &str = "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)";

const PLAIN: &str = "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, c BIGINT CHECK (c > 0), \
                     p BIGINT REFERENCES parent (id), u BIGINT, CONSTRAINT t_u UNIQUE (u))";

const PARTITIONED: &str = "CREATE TABLE t (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, c BIGINT CHECK (c > 0), \
                           p BIGINT REFERENCES parent (id), u BIGINT, PRIMARY KEY (tenant_id, id), \
                           CONSTRAINT t_u UNIQUE (tenant_id, u)) PARTITION BY HASH (tenant_id) PARTITIONS 4";

/// What a client can tell of `t`: absent, or its columns and every
/// constraint as its schema holds them. For a partitioned table, the
/// shell's.
fn shape(db: &Arc<Database>) -> Option<String> {
    let schema = if let Some(pt) = db.partitioned_table("t") {
        pt.shell().schema.clone()
    } else {
        db.table_held("t")?.schema.clone()
    };
    Some(format!(
        "{:?} pk {:?} uk {:?} chk {:?} fk {:?} partitioned {}",
        schema.fields.iter().filter(|f| !f.is_deleted).map(|f| f.name.clone()).collect::<Vec<_>>(),
        schema.primary_key,
        schema.unique_keys.iter().filter(|k| !k.is_deleted).map(|k| k.name.clone()).collect::<Vec<_>>(),
        schema.checks.iter().filter(|c| !c.is_deleted).map(|c| (c.name.clone(), c.expression.clone())).collect::<Vec<_>>(),
        schema.foreign_keys.iter().filter(|f| !f.is_deleted).map(|f| (f.name.clone(), f.parent_table.clone())).collect::<Vec<_>>(),
        db.is_partitioned("t"),
    ))
}

/// Every constraint enforced, not only declared: each of these is refused.
async fn enforced(db: &Arc<Database>, partitioned: bool) -> Result<(), String> {
    let (cols, key) = if partitioned { ("tenant_id, id", "1, 1") } else { ("id", "1") };
    exec(db, &format!("INSERT INTO t ({cols}, c, p, u) VALUES ({key}, 5, 1, 7)")).await?;
    let refused = [
        ("the CHECK", format!("INSERT INTO t ({cols}, c, p, u) VALUES ({}, -1, 1, 8)", if partitioned { "1, 2" } else { "2" })),
        ("the FOREIGN KEY", format!("INSERT INTO t ({cols}, c, p, u) VALUES ({}, 5, 99, 9)", if partitioned { "1, 3" } else { "3" })),
        ("the PRIMARY KEY", format!("INSERT INTO t ({cols}, c, p, u) VALUES ({key}, 5, 1, 10)")),
        ("the UNIQUE", format!("INSERT INTO t ({cols}, c, p, u) VALUES ({}, 5, 1, 7)", if partitioned { "1, 4" } else { "4" })),
    ];
    for (what, sql) in refused {
        if exec(db, &sql).await.is_ok() {
            return Err(format!("{what} is not enforced: `{sql}` succeeded"));
        }
    }
    Ok(())
}

fn leftover_intents(dir: &Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("__create_"))
        .collect()
}

#[derive(Default, Debug)]
struct Outcomes {
    absent: usize,
    present: usize,
}

/// Runs `create` cut at each of its syncs and after it returns, and checks
/// every image of every cut.
async fn run_case(create: &str, partitioned: bool, seeds: u64) -> Outcomes {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    {
        let db = open(&dir, "create_cut").await.unwrap();
        exec(&db, PARENT).await.unwrap();
        exec(&db, "INSERT INTO parent (id) VALUES (1), (2)").await.unwrap();
        db.sync_durable().unwrap();
        close(&dir, db);
    }
    let db = open(&dir, "create_cut").await.unwrap();
    let rec = Recorder::start(&dir);
    let sink: StdArc<Mutex<Vec<Result<CrashPoint, String>>>> = StdArc::new(Mutex::new(Vec::new()));
    // Inside each sync the CREATE may take: the intent's, each log sync, a
    // data file's and a directory's. Not every one fires for every table.
    let mut guards: Vec<(String, failpoint::Guard)> = Vec::new();
    for (site, n) in [
        ("sync:rewrite.intent", 1u64),
        ("sync:wal.log", 6),
        ("sync:file.data", 2),
        ("sync:dir", 2),
        ("sync:partition_dir", 1),
    ] {
        for nth in 1..=n {
            let label = format!("{site} #{nth}");
            guards.push((label.clone(), failpoint::arm(site, &dir, nth, rec.cut_action(label, sink.clone()))));
        }
    }
    exec(&db, create).await.unwrap_or_else(|e| panic!("{create}: {e}"));
    let fired: Vec<String> = guards.iter().filter(|(_, g)| g.fired() > 0).map(|(l, _)| l.clone()).collect();
    drop(guards);
    sink.lock().push(rec.cut_quiescent("after the CREATE returned"));
    let new = shape(&db).expect("the CREATE left no table");
    close(&dir, db);
    drop(rec);
    assert!(fired.iter().any(|l| l.starts_with("sync:wal.log")), "{create}: no cut fell in a log sync ({fired:?})");

    let points: Vec<CrashPoint> = sink.lock().drain(..).filter_map(|p| p.map_err(|why| eprintln!("{why}")).ok()).collect();
    assert!(points.len() >= 2, "{create}: too many cuts were disturbed: {}", points.len());
    let img = base.path().join("image");
    let mut out = Outcomes::default();
    let mut problems = Vec::new();
    for point in &points {
        for seed in 0..seeds {
            point.write_image(seed, &img);
            let label = format!("{} seed {seed}", point.label);
            match open(&img, "create_cut").await {
                Err(e) => problems.push(format!("{label}: does not open: {e}")),
                Ok(db) => {
                    let intents = leftover_intents(&img);
                    if !intents.is_empty() {
                        problems.push(format!("{label}: intents left after the open: {intents:?}"));
                    }
                    match shape(&db) {
                        None => {
                            out.absent += 1;
                            // No stray file in the way: the CREATE runs again
                            // and leaves the whole table.
                            match exec(&db, create).await {
                                Err(e) => problems.push(format!("{label}: absent, and a CREATE again fails: {e}")),
                                Ok(_) if shape(&db).as_ref() != Some(&new) => problems.push(format!(
                                    "{label}: absent, and a CREATE again leaves {:?}, not {new}",
                                    shape(&db)
                                )),
                                Ok(_) => {}
                            }
                        }
                        Some(got) if got == new => {
                            out.present += 1;
                            if let Err(why) = enforced(&db, partitioned).await {
                                problems.push(format!("{label}: {why}"));
                            }
                        }
                        Some(got) => problems.push(format!("{label}: neither absent nor whole:\n  {got}\n  new {new}")),
                    }
                    if point.label.starts_with("after the CREATE") && shape(&db).is_none() {
                        problems.push(format!("{label}: the CREATE had returned and the table is gone"));
                    }
                    close(&img, db);
                }
            }
            let _ = std::fs::remove_dir_all(&img);
        }
    }
    eprintln!("{create}: cuts {} at {fired:?}: {out:?}", points.len());
    assert!(problems.is_empty(), "{} problems:\n{}", problems.len(), problems.join("\n"));
    out
}

/// A plain and a partitioned `CREATE TABLE` with a `PRIMARY KEY`, a `UNIQUE`,
/// a `CHECK` and a `FOREIGN KEY`, cut inside every sync it takes and after it
/// returns, every image under every seed kind: the table is absent (and a
/// `CREATE` of it succeeds) or present with all of it, enforced. Both happen.
///
/// Mutations: the catalogue row written first again (images keep it with
/// the columns and no constraint); the intent's recovery skipped (an absent
/// table's files block the next `CREATE`); the intent removed before the
/// row is durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_create_table_cut_anywhere_leaves_no_table_or_the_whole_table() {
    let seeds = env_count("RDB_POWER_LOSS_SEEDS", 16, 1024);
    let mut total = Outcomes::default();
    for (create, partitioned) in [(PLAIN, false), (PARTITIONED, true)] {
        let o = run_case(create, partitioned, seeds).await;
        total.absent += o.absent;
        total.present += o.present;
    }
    assert!(total.absent > 0 && total.present > 0, "every image fell on one side ({total:?}): the cuts tell nothing apart");
}
