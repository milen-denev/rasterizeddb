//! Metadata DDL on a partitioned table through a power cut
//! (docs/partitioning.md §11, "Phase 3 P1 as built"): every image cut
//! anywhere in the change opens, with every partition, and holds the table
//! as it was or as the change left it, the same in every partition: never a
//! mix, never a partition that refuses to open.
//!
//! The cuts: the shell's sidecar write (`test.rewrite.metadata_after_work`
//! inside the schema backup of an atomic change, its intent's sync), right
//! after the work (`test.pddl.after_work`), after the first and the second
//! partition reopened (`test.pddl.partition_reopened`), and for RENAME TO
//! each of its steps (`test.pddl.rename_after_*`). Each cut is imaged under
//! every seed kind (`RDB_POWER_LOSS_SEEDS`).

use super::*;
use parking_lot::Mutex;
use std::sync::Arc as StdArc;

const CREATE: &str = "CREATE TABLE pt (tenant_id BIGINT NOT NULL, id BIGINT NOT NULL, v BIGINT, s TEXT, \
                      m BIGINT NOT NULL, PRIMARY KEY (tenant_id, id)) PARTITION BY HASH (tenant_id) PARTITIONS 8";

/// One DDL and the cut points it must reach.
struct Case {
    ddl: &'static str,
    points: &'static [(&'static str, u64)],
    /// Run before the recorder starts: what the DDL undoes.
    prep: &'static [&'static str],
    /// No row: the metadata-only forms of ADD COLUMN and DROP NOT NULL.
    empty: bool,
}

const fn case(ddl: &'static str, points: &'static [(&'static str, u64)]) -> Case {
    Case { ddl, points, prep: &[], empty: false }
}

/// Every partition's indexes built or loaded, and ready.
async fn settled(t: &Table<DbStorageProvider>) -> Result<(), String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    tokio::time::timeout(std::time::Duration::from_secs(60), t.index_load_settled())
        .await
        .map_err(|_| "the index load never settled".to_string())?;
    while !t.filters_ready().map_err(|e| e.to_string())?
        || t.indexes().map_err(|e| e.to_string())?.is_some_and(|ix| !ix.is_ready())
    {
        if std::time::Instant::now() > deadline {
            return Err("the indexes never became ready".into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    Ok(())
}

/// Each index's contents, not only its declaration: once every partition's
/// indexes are ready, an equality its index serves answers as the same
/// predicate the index cannot serve (`v + 0`, `s || ''`). A column a change
/// dropped or renamed fails both, alike.
async fn index_contents(db: &Arc<Database>, name: &str, table: &crate::core::partition::table::PartitionedTable) -> Result<(), String> {
    let epoch = table.epoch().map_err(|e| e.to_string())?;
    for (_, part) in table.materialized(&epoch, &crate::core::partition::route::Route::All).map_err(|e| e.to_string())? {
        settled(&part).await?;
    }
    let probes = [0i64, 3, 30, 93, 141].map(|v| (format!("v = {v}"), format!("v + 0 = {v}")));
    let words = ["w0", "w1", "w2", "w3"].map(|w| (format!("s = '{w}'"), format!("s || '' = '{w}'")));
    for (served, scanned) in probes.iter().chain(words.iter()) {
        let a = rows(db, &format!("SELECT id FROM {name} WHERE {served}")).await;
        let b = rows(db, &format!("SELECT id FROM {name} WHERE {scanned}")).await;
        match (a, b) {
            (Ok(a), Ok(b)) if a == b => {}
            (Err(_), Err(_)) => {}
            (a, b) => return Err(format!("`{served}` answers {a:?}, its scan {b:?}")),
        }
    }
    Ok(())
}

const REOPENS: [(&str, u64); 4] = [
    ("test.pddl.before_work", 1),
    ("test.pddl.after_work", 1),
    ("test.pddl.partition_reopened", 1),
    ("test.pddl.partition_reopened", 2),
];

/// What a client can tell apart, for the table under whichever name it has:
/// its name, its schema as the shell holds it (which every partition must
/// share), and its rows.
async fn fingerprint(db: &Arc<Database>) -> Result<String, String> {
    let names: Vec<String> = ["pt", "pt2"].into_iter().filter(|n| db.is_partitioned(n)).map(str::to_string).collect();
    let [name] = names.as_slice() else {
        return Err(format!(
            "the partitioned table is under {names:?}; relations {:?}, {} partitioned",
            db.relation_names(),
            db.partitioned_count()
        ));
    };
    let table = db.partitioned_table(name).unwrap();
    if let Some(why) = table.broken() {
        return Err(format!("broken: {why}"));
    }
    let schema = &table.shell().schema;
    let shell = format!(
        "{} {:?} idx {:?} uk {:?} chk {:?} def {:?}",
        schema.name,
        schema.fields.iter().map(|f| (&f.name, f.write_order, f.is_deleted)).collect::<Vec<_>>(),
        schema.secondary_indexes.iter().filter(|i| !i.is_deleted).map(|i| &i.name).collect::<Vec<_>>(),
        schema.unique_keys.iter().filter(|k| !k.is_deleted).map(|k| &k.name).collect::<Vec<_>>(),
        schema.checks.iter().filter(|c| !c.is_deleted).map(|c| &c.name).collect::<Vec<_>>(),
        schema.defaults.iter().filter(|d| !d.is_deleted).map(|d| (&d.column, &d.expression)).collect::<Vec<_>>(),
    );
    let epoch = table.epoch().map_err(|e| e.to_string())?;
    for (pid, part) in table.materialized(&epoch, &crate::core::partition::route::Route::All).map_err(|e| e.to_string())? {
        if part.schema.fields.len() != schema.fields.len()
            || part.schema.fields.iter().zip(&schema.fields).any(|(a, b)| a.name != b.name || a.is_deleted != b.is_deleted)
            || part.schema.secondary_indexes.len() != schema.secondary_indexes.len()
            || part.schema.checks.len() != schema.checks.len()
        {
            return Err(format!("partition {pid} holds another schema than the shell"));
        }
    }
    index_contents(db, name, &table).await?;
    // Every tenant routes to its partition and reads; the rows, hashed.
    let key = table.key_columns().map_err(|e| e.to_string())?.remove(0);
    let mut per_tenant = Vec::new();
    for tenant in 1..=6 {
        let got = rows(db, &format!("SELECT * FROM {name} WHERE {key} = {tenant}")).await?;
        per_tenant.push((got.len(), crate::core::wal::file_id_for_name(&format!("{got:?}"))));
    }
    Ok(format!("{name} | {shell} | {per_tenant:?}"))
}

async fn populated(dir: &Path, case: &Case) -> Arc<Database> {
    let db = open(dir, "pddl_cut").await.unwrap();
    exec(&db, CREATE).await.unwrap();
    if !case.empty {
        let values: Vec<String> =
            (0..48i64).map(|i| format!("({}, {i}, {}, 'w{}', {i})", i % 6 + 1, i * 3, i % 4)).collect();
        exec(&db, &format!("INSERT INTO pt (tenant_id, id, v, s, m) VALUES {}", values.join(", "))).await.unwrap();
        exec(&db, "DELETE FROM pt WHERE id % 11 = 3").await.unwrap();
    }
    for sql in case.prep {
        exec(&db, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    db.sync_durable().unwrap();
    db
}

/// What one case's images came to: on the old side, on the new side, on a
/// state between two of its statements, and the partitions that loaded
/// their indexes from a persisted snapshot.
#[derive(Default, Debug)]
struct Outcomes {
    old: usize,
    new: usize,
    between: usize,
    loaded: usize,
}

/// Runs `case` with its cuts armed and checks every image of every cut,
/// twice: rebuilding every index from rows (the default), and with
/// `index_storage = snapshot`, loading the INDEX sections sealed before the
/// change where the open's proof accepts them, so a persisted section that
/// no longer matches its index would answer from stale entries.
async fn run_case(case: &Case, seeds: u64) -> Outcomes {
    use crate::core::index_store::load::{Origin, settings::IndexStorage, storage_override};
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    // Sealed before the change, so every image holds a snapshot from then.
    let _sealing = storage_override::set(&dir, IndexStorage::Snapshot);
    {
        let db = populated(&dir, case).await;
        if let Some(pt) = db.partitioned_table("pt") {
            let epoch = pt.epoch().unwrap();
            for (_, part) in pt.materialized(&epoch, &crate::core::partition::route::Route::All).unwrap() {
                settled(&part).await.unwrap();
            }
        }
        let report = db.seal_for_shutdown(std::time::Duration::from_secs(60)).await;
        assert!(report.skipped.is_empty(), "{}: the shutdown seal skipped {:?}", case.ddl, report.skipped);
        close(&dir, db);
    }
    let db = open(&dir, "pddl_cut").await.unwrap();
    let mut states = vec![fingerprint(&db).await.unwrap()];

    let rec = Recorder::start(&dir);
    let sink: StdArc<Mutex<Vec<Result<CrashPoint, String>>>> = StdArc::new(Mutex::new(Vec::new()));
    let mut guards = Vec::new();
    for (point, nth) in case.points {
        let (rec, sink, label) = (rec.clone(), sink.clone(), format!("{point} #{nth}"));
        let action: failpoint::Action = StdArc::new(move |_hit| {
            sink.lock().push(rec.cut_quiescent(label.clone()));
            failpoint::Outcome::Continue
        });
        guards.push(failpoint::arm(point, &dir, *nth, action));
    }
    // Inside the shell's own syncs too, where there are any; not required.
    let optional: Vec<failpoint::Guard> = [("sync:rewrite.intent", 1u64), ("sync:file.data", 1), ("sync:wal.log", 2)]
        .iter()
        .map(|(site, nth)| failpoint::arm(site, &dir, *nth, rec.cut_action(format!("{site} #{nth}"), sink.clone())))
        .collect();
    // A case may be several statements: an image may hold the state after
    // any of them.
    for sql in case.ddl.split("; ") {
        exec(&db, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        states.push(fingerprint(&db).await.unwrap());
    }
    for (g, (point, nth)) in guards.iter().zip(case.points) {
        assert_eq!(g.fired(), 1, "{}: the cut point {point} #{nth} was never reached", case.ddl);
    }
    drop(guards);
    drop(optional);
    let (old, new) = (states[0].clone(), states.last().unwrap().clone());
    assert_ne!(old, new, "{}: the change changed nothing a client sees", case.ddl);
    close(&dir, db);
    drop(rec);

    let points: Vec<CrashPoint> = sink
        .lock()
        .drain(..)
        .filter_map(|p| p.map_err(|why| eprintln!("{why}")).ok())
        .collect();
    assert!(points.len() >= case.points.len(), "{}: too many cuts were disturbed: {}", case.ddl, points.len());
    let img = base.path().join("image");
    let mut out = Outcomes::default();
    let mut problems = Vec::new();
    for snapshot in [false, true] {
        let _storage = snapshot.then(|| storage_override::set(&img, IndexStorage::Snapshot));
        for point in &points {
            for seed in 0..seeds {
                point.write_image(seed, &img);
                let label = format!(
                    "{}: {} seed {seed}{}",
                    case.ddl,
                    point.label,
                    if snapshot { " (index_storage = snapshot)" } else { "" }
                );
                match open(&img, "pddl_cut").await {
                    Err(e) => problems.push(format!("{label}: does not open: {e}")),
                    Ok(db) => {
                        match fingerprint(&db).await {
                            Ok(got) if got == old => out.old += 1,
                            Ok(got) if got == new => out.new += 1,
                            Ok(got) if states.contains(&got) => out.between += 1,
                            Ok(got) => problems.push(format!(
                                "{label}: neither the old table nor the new:\n  {got}\n  old {old}\n  new {new}"
                            )),
                            Err(e) => {
                                let mut files: Vec<String> = std::fs::read_dir(&img)
                                    .unwrap()
                                    .filter_map(|e| e.ok())
                                    .map(|e| format!("{}:{}", e.file_name().to_string_lossy(), e.metadata().map(|m| m.len()).unwrap_or(0)))
                                    .collect();
                                files.sort();
                                problems.push(format!("{label}: {e}\n  files {files:?}"))
                            }
                        }
                        if snapshot && let Some(pt) = ["pt", "pt2"].iter().find_map(|n| db.partitioned_table(n)) {
                            if let Ok(epoch) = pt.epoch() {
                                for (_, part) in pt.materialized(&epoch, &crate::core::partition::route::Route::All).unwrap_or_default() {
                                    out.loaded += (part.index_origin() == Origin::Loaded) as usize;
                                }
                            }
                        }
                        let leftover: Vec<_> = std::fs::read_dir(&img)
                            .unwrap()
                            .filter_map(|e| e.ok())
                            .map(|e| e.file_name().to_string_lossy().to_string())
                            .filter(|n| n.starts_with("__rewrite_"))
                            .collect();
                        // A backup copied before its intent was durable belongs
                        // to nobody and is discarded by the table's next backup,
                        // as a plain table's is; an intent must be gone.
                        if !leftover.is_empty() {
                            problems.push(format!("{label}: intents left after the open: {leftover:?}"));
                        }
                        close(&img, db);
                    }
                }
                let _ = std::fs::remove_dir_all(&img);
            }
        }
    }
    assert!(problems.is_empty(), "{} problems:\n{}", problems.len(), problems.join("\n"));
    out
}

/// Every metadata DDL kind cut at each of its steps: each image opens with
/// every partition and holds the old table or the new one, alike in every
/// partition.
///
/// Mutations: recovery skips the schema intents (`recover_interrupted_rewrites`
/// returns at once): an atomic change cut after its work opens with the
/// sidecars half written, which is neither table; an index build skipping
/// rows (the index-contents check); the snapshot identity unchanged by the
/// index swap (its images, with `index_storage = snapshot`, load the
/// dropped index's sections for the new one, and its answers are stale).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partitioned_ddl_cut_anywhere_leaves_the_old_table_or_the_new() {
    let seeds = env_count("RDB_POWER_LOSS_SEEDS", 5, 1024);
    let atomic: &[(&str, u64)] = &[
        ("test.rewrite.metadata_after_work", 1),
        ("test.pddl.after_work", 1),
        ("test.pddl.partition_reopened", 1),
        ("test.pddl.partition_reopened", 2),
    ];
    // An empty table has no open partition to reopen.
    let atomic_empty: &[(&str, u64)] = &[("test.rewrite.metadata_after_work", 1), ("test.pddl.after_work", 1)];
    let cases = [
        case("CREATE INDEX pt_v ON pt (v)", &REOPENS),
        case("CREATE INDEX pt_s ON pt USING hash (s)", &REOPENS),
        // An index swapped for another, the count kept: only the snapshot
        // identity's shape tells the sealed sections from the new index's.
        Case {
            prep: &["CREATE INDEX pt_ix ON pt (v)"],
            ..case(
                "DROP INDEX pt_ix; CREATE INDEX pt_iy ON pt (s)",
                &[("test.pddl.after_work", 1), ("test.pddl.before_work", 2), ("test.pddl.after_work", 2)],
            )
        },
        Case { prep: &["CREATE INDEX pt_v ON pt (v)"], ..case("DROP INDEX pt_v", &REOPENS) },
        case("ALTER TABLE pt ADD CONSTRAINT pt_c CHECK (v >= 0)", &REOPENS),
        Case {
            prep: &["ALTER TABLE pt ADD CONSTRAINT pt_c CHECK (v >= 0)"],
            ..case("ALTER TABLE pt DROP CONSTRAINT pt_c", &REOPENS)
        },
        case("ALTER TABLE pt ADD CONSTRAINT pt_u UNIQUE (tenant_id, id, v)", &REOPENS),
        case("ALTER TABLE pt ALTER COLUMN s SET DEFAULT 'd'", &REOPENS),
        Case {
            prep: &["ALTER TABLE pt ALTER COLUMN s SET DEFAULT 'd'"],
            ..case("ALTER TABLE pt ALTER COLUMN s DROP DEFAULT", &REOPENS)
        },
        case("ALTER TABLE pt RENAME COLUMN s TO label", atomic),
        case("ALTER TABLE pt RENAME COLUMN tenant_id TO tid", atomic),
        case("ALTER TABLE pt DROP COLUMN s", atomic),
        case("ALTER TABLE pt ALTER COLUMN s SET NOT NULL", atomic),
        // The metadata-only forms, on a table with no row.
        Case { empty: true, ..case("ALTER TABLE pt ADD COLUMN extra INT DEFAULT 5", atomic_empty) },
        Case { empty: true, ..case("ALTER TABLE pt ALTER COLUMN m DROP NOT NULL", atomic_empty) },
        case(
            "ALTER TABLE pt RENAME TO pt2",
            &[
                ("test.pddl.rename_after_intent", 1),
                ("test.pddl.rename_after_commit", 1),
                ("test.pddl.rename_after_header", 1),
                ("test.pddl.partition_reopened", 1),
            ],
        ),
    ];
    let mut total = Outcomes::default();
    for case in &cases {
        let o = run_case(case, seeds).await;
        eprintln!("{}: {o:?}", case.ddl);
        // Per case: a table with rows has open partitions whose sealed
        // indexes the snapshot pass must have loaded, or it compared nothing
        // for this case (the index swap above all).
        assert!(case.empty || o.loaded > 0, "{}: no image loaded a persisted index ({o:?})", case.ddl);
        total.old += o.old;
        total.new += o.new;
        total.between += o.between;
        total.loaded += o.loaded;
    }
    assert!(total.old > 0 && total.new > 0, "every image fell on one side ({total:?}); the cuts test nothing");
    assert!(total.loaded > 0, "no image loaded a persisted index: the snapshot pass compared nothing ({total:?})");
}
