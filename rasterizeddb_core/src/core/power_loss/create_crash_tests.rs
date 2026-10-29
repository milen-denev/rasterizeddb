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

/// Where `t` goes: the database directory by the policy, or pinned before the
/// `CREATE` (`STORAGE PLACE TABLE`) to a second directory or a key/value
/// store. A pin must outlive a `CREATE` cut before its catalogue row: the
/// retry lands where it says.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Placement {
    Root,
    #[cfg(feature = "storage_pools")]
    Disk,
    #[cfg(all(feature = "storage_pools", feature = "kv_storage"))]
    Kv,
}

/// The spec `t`'s pin must read in a database whose files are under `base`.
fn pinned_spec(placement: Placement, base: &Path) -> Option<String> {
    let _ = base;
    match placement {
        Placement::Root => None,
        #[cfg(feature = "storage_pools")]
        Placement::Disk => Some(path_str(&base.join("disk2"))),
        #[cfg(all(feature = "storage_pools", feature = "kv_storage"))]
        Placement::Kv => Some("kv:local".to_string()),
    }
}

#[cfg(feature = "storage_pools")]
fn spec_of(db: &Arc<Database>) -> Option<String> {
    db.storage.spec_of("t")
}

#[cfg(not(feature = "storage_pools"))]
fn spec_of(_db: &Arc<Database>) -> Option<String> {
    None
}

/// The key/value store `local`, a directory under `base`, for `root`.
#[cfg(all(feature = "storage_pools", feature = "kv_storage"))]
fn write_kv_config(root: &Path, base: &Path) {
    let store = base.join("kvstore");
    std::fs::create_dir_all(&store).unwrap();
    std::fs::create_dir_all(root).unwrap();
    std::fs::write(
        root.join(crate::core::storage::kv_registry::KV_FILE),
        format!("# test stores\nversion\t1\nstore\tlocal\tdir\t{}\trdb\n", path_str(&store)),
    )
    .unwrap();
}

/// An image's store configuration names the store under the image, as the
/// image writer does for the pool file: otherwise the image would read the
/// recorded run's own store.
fn repoint_kv_config(root: &Path, from: &Path, to: &Path) {
    let path = root.join("db_kv.cfg");
    if let Ok(text) = std::fs::read_to_string(&path) {
        std::fs::write(&path, text.replace(&path_str(from), &path_str(to))).unwrap();
    }
}

/// Closes `db` and forgets the log of every location under `base`, not only
/// the root's: an image reuses its paths, and a log still registered under
/// one answers the next image with the last one's offsets.
fn close_all(base: &Path, root: &Path, db: Arc<Database>) {
    close(root, db);
    for location in ["disk2"] {
        crate::core::wal::remove_for(&path_str(&base.join(location)));
    }
}

/// Runs `create` cut at each of its syncs and after it returns, and checks
/// every image of every cut.
async fn run_case(create: &str, partitioned: bool, placement: Placement, seeds: u64) -> Outcomes {
    let base = tempfile::tempdir().unwrap();
    // The recorded directory holds the root, a second location and a store
    // side by side (a location may not nest inside the root).
    let rec_dir = base.path().join("rec");
    let dir = rec_dir.join("db");
    #[cfg(all(feature = "storage_pools", feature = "kv_storage"))]
    if placement == Placement::Kv {
        write_kv_config(&dir, &rec_dir);
    }
    {
        let db = open(&dir, "create_cut").await.unwrap();
        exec(&db, PARENT).await.unwrap();
        exec(&db, "INSERT INTO parent (id) VALUES (1), (2)").await.unwrap();
        match placement {
            Placement::Root => {}
            #[cfg(feature = "storage_pools")]
            Placement::Disk => {
                let disk = rec_dir.join("disk2");
                std::fs::create_dir_all(&disk).unwrap();
                let added = db
                    .storage
                    .add_location(&path_str(&disk), crate::core::storage::LocationRole::Primary, 1, true)
                    .unwrap();
                db.storage.pin("t", &added).unwrap();
            }
            #[cfg(all(feature = "storage_pools", feature = "kv_storage"))]
            Placement::Kv => {
                db.storage
                    .pin_backend("t", &crate::core::storage::TableBackend::KeyValue("local".to_string()))
                    .unwrap();
            }
        }
        db.sync_durable().unwrap();
        close_all(&rec_dir, &dir, db);
    }
    let db = open(&dir, "create_cut").await.unwrap();
    let pin = pinned_spec(placement, &rec_dir);
    assert_eq!(spec_of(&db), pin, "the pin was not recorded");
    let rec = Recorder::start(&rec_dir);
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
            guards.push((label.clone(), failpoint::arm(site, &rec_dir, nth, rec.cut_action(label, sink.clone()))));
        }
    }
    exec(&db, create).await.unwrap_or_else(|e| panic!("{create}: {e}"));
    let fired: Vec<String> = guards.iter().filter(|(_, g)| g.fired() > 0).map(|(l, _)| l.clone()).collect();
    drop(guards);
    sink.lock().push(rec.cut_quiescent("after the CREATE returned"));
    let new = shape(&db).expect("the CREATE left no table");
    close_all(&rec_dir, &dir, db);
    drop(rec);
    assert!(fired.iter().any(|l| l.starts_with("sync:wal.log")), "{create}: no cut fell in a log sync ({fired:?})");

    let points: Vec<CrashPoint> = sink.lock().drain(..).filter_map(|p| p.map_err(|why| eprintln!("{why}")).ok()).collect();
    assert!(points.len() >= 2, "{create}: too many cuts were disturbed: {}", points.len());
    let img_dir = base.path().join("image");
    let img = img_dir.join("db");
    let pin_in_image = pinned_spec(placement, &img_dir);
    let mut out = Outcomes::default();
    let mut problems = Vec::new();
    for point in &points {
        for seed in 0..seeds {
            point.write_image(seed, &img_dir);
            // An image holds files, not directories: a location or a store
            // with nothing in it yet still exists on the disk it is.
            for empty in ["disk2", "kvstore"] {
                if rec_dir.join(empty).is_dir() {
                    std::fs::create_dir_all(img_dir.join(empty)).unwrap();
                }
            }
            repoint_kv_config(&img, &rec_dir, &img_dir);
            let label = format!("{placement:?} {} seed {seed}", point.label);
            match open(&img, "create_cut").await {
                Err(e) => problems.push(format!("{label}: does not open: {e}")),
                Ok(db) => {
                    let intents = leftover_intents(&img);
                    if !intents.is_empty() {
                        problems.push(format!("{label}: intents left after the open: {intents:?}"));
                    }
                    // A pin, whatever the image; a placement the CREATE chose
                    // (the root, recorded too) goes and comes with the table.
                    if pin_in_image.is_some() && spec_of(&db) != pin_in_image {
                        problems.push(format!("{label}: the placement reads {:?}, not the pin {pin_in_image:?}", spec_of(&db)));
                    }
                    match shape(&db) {
                        None => {
                            out.absent += 1;
                            // No stray file in the way: the CREATE runs again
                            // and leaves the whole table, where the pin says.
                            match exec(&db, create).await {
                                Err(e) => problems.push(format!("{label}: absent, and a CREATE again fails: {e}")),
                                Ok(_) if shape(&db).as_ref() != Some(&new) => problems.push(format!(
                                    "{label}: absent, and a CREATE again leaves {:?}, not {new}",
                                    shape(&db)
                                )),
                                Ok(_) if spec_of(&db) != pin_in_image && pin_in_image.is_some() => problems.push(format!(
                                    "{label}: absent, and a CREATE again placed it at {:?}, not the pin {pin_in_image:?}",
                                    spec_of(&db)
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
                    close_all(&img_dir, &img, db);
                }
            }
            let _ = std::fs::remove_dir_all(&img_dir);
        }
    }
    eprintln!("{create} ({placement:?}): cuts {} at {fired:?}: {out:?}", points.len());
    assert!(problems.is_empty(), "{} problems:\n{}", problems.len(), problems.join("\n"));
    out
}

/// A plain and a partitioned `CREATE TABLE` with a `PRIMARY KEY`, a `UNIQUE`,
/// a `CHECK` and a `FOREIGN KEY`, cut inside every sync it takes and after it
/// returns, every image under every seed kind: the table is absent (and a
/// `CREATE` of it succeeds) or present with all of it, enforced. Both happen.
/// The plain one also pinned before the `CREATE` to a second directory and to
/// a key/value store: every image keeps the pin, and an absent table's retry
/// lands there.
///
/// Mutations: the catalogue row written first again (images keep it with
/// the columns and no constraint); the intent's recovery skipped (an absent
/// table's files block the next `CREATE`); the intent removed before the
/// row is durable; the discard forgetting a pin (the image reads no
/// placement, and the retry goes to the database directory).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_create_table_cut_anywhere_leaves_no_table_or_the_whole_table() {
    let seeds = env_count("RDB_POWER_LOSS_SEEDS", 16, 1024);
    let mut cases = vec![(PLAIN, false, Placement::Root), (PARTITIONED, true, Placement::Root)];
    #[cfg(feature = "storage_pools")]
    cases.push((PLAIN, false, Placement::Disk));
    #[cfg(all(feature = "storage_pools", feature = "kv_storage"))]
    cases.push((PLAIN, false, Placement::Kv));
    let mut total = Outcomes::default();
    for (create, partitioned, placement) in cases {
        let o = run_case(create, partitioned, placement, seeds).await;
        total.absent += o.absent;
        total.present += o.present;
    }
    assert!(total.absent > 0 && total.present > 0, "every image fell on one side ({total:?}): the cuts tell nothing apart");
}
