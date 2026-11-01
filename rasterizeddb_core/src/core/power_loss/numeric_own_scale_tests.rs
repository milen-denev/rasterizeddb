//! C6: an unconstrained NUMERIC column created before cells kept their scale
//! (sixteen bytes at nine places) still reads, keeps its refusal of digits
//! past nine places with a hint, and converts — all or nothing, across a
//! power cut — to the seventeen-byte slot that keeps each value's own scale
//! (`value_codec::keeps_own_scale`). The old columns are made through the
//! test-only `Database::legacy_numeric_slot`, the slot every earlier build
//! wrote.

use super::*;
use crate::core::database::Relation;
use parking_lot::Mutex;
use std::sync::Arc as StdArc;
use std::sync::atomic::Ordering;

fn slot(db: &Arc<Database>, table: &str, column: &str) -> u32 {
    match db.relation(table) {
        Some(Relation::Plain(t)) => t.schema.fields.iter().find(|f| f.name == column && !f.is_deleted).expect("column").size,
        _ => panic!("{table} is not a plain table"),
    }
}

fn flagged(dir: &Path) -> bool {
    crate::core::database::incompat::flags(dir)
        .unwrap()
        .iter()
        .any(|f| f == crate::core::database::incompat::NUMERIC_OWN_SCALE_V1)
}

/// A NUMERIC cell's value as text at the places it needs: a sixteen-byte one
/// at the engine's nine, a seventeen-byte one at its own scale; NULL as "-".
fn value(cell: &[u8]) -> String {
    match crate::core::value_codec::split_decimal_cell(cell) {
        None => "-".to_string(),
        Some((mut m, own)) => {
            let mut s = own.unwrap_or(crate::core::value_codec::DECIMAL_SCALE);
            while s > 0 && m % 10 == 0 {
                m /= 10;
                s -= 1;
            }
            crate::core::value_codec::format_decimal_scaled(m, s)
        }
    }
}

/// `(id, the u column's value, its cell width)` for every row of `table`.
async fn column(db: &Arc<Database>, table: &str) -> Vec<(Vec<u8>, String, usize)> {
    rows(db, &format!("SELECT id, u FROM {table}"))
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r[0].clone(), value(&r[1]), r[1].len()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_old_unconstrained_column_reads_refuses_and_converts() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir, "own_scale").await.unwrap();
    db.legacy_numeric_slot.store(true, Ordering::Relaxed);
    exec(&db, "CREATE TABLE t (id BIGINT PRIMARY KEY, u NUMERIC, w NUMERIC(10,2))").await.unwrap();
    exec(&db, "INSERT INTO t VALUES (1, 1.5, 2.25), (2, 0.000000001, 1), (3, NULL, 3), (4, 123456.789, 4)").await.unwrap();
    exec(&db, "CREATE TABLE t2 (id BIGINT PRIMARY KEY, u NUMERIC)").await.unwrap();
    exec(&db, "INSERT INTO t2 VALUES (1, 2.5)").await.unwrap();
    db.legacy_numeric_slot.store(false, Ordering::Relaxed);
    assert_eq!(slot(&db, "t", "u"), 16);
    assert_eq!(slot(&db, "t", "w"), 16, "a NUMERIC(p, s) column is never the own-scale slot");
    assert!(!flagged(&dir), "no own-scale column yet, so nothing to refuse an older binary");

    // It keeps nine places, and says how to keep more.
    for sql in ["UPDATE t SET u = 2.0 / 3 WHERE id = 1", "INSERT INTO t VALUES (5, 0.0000000001, 0)"] {
        let e = exec(&db, sql).await.expect_err(sql);
        assert!(e.contains("ALTER TABLE … ALTER COLUMN \"u\" TYPE NUMERIC"), "{sql}: {e}");
        assert!(!e.ends_with("for column \"u\""), "the column is named in the hint, not after it: {e}");
    }
    let before = column(&db, "t").await;
    assert_eq!(
        before.iter().map(|(_, v, w)| (v.as_str(), *w)).collect::<Vec<_>>(),
        // A NULL's fixed-width slot holds zeros (its companion says NULL).
        [("1.5", 16), ("0.000000001", 16), ("0", 16), ("123456.789", 16)]
    );

    // A query's temporary relation over it (a CTE, a derived table) is made
    // in memory with the slot that keeps each value's scale, and does not mark
    // the database: nothing an older binary could open.
    let made = crate::core::rql::cte::materialize(
        "WITH c AS (SELECT id, u FROM t) SELECT u FROM c",
        &db,
        crate::core::mvcc::transaction::FROZEN_TX,
    )
    .await
    .unwrap()
    .expect("a CTE to materialise");
    crate::core::rql::cte::drop_temps(&db, &made.temps, crate::core::mvcc::transaction::FROZEN_TX).await;
    assert!(!flagged(&dir), "a temporary relation marked the database");

    // A reopen reads it as it was.
    close(&dir, db);
    let db = open(&dir, "own_scale").await.unwrap();
    assert_eq!(slot(&db, "t", "u"), 16);
    assert_eq!(column(&db, "t").await, before);

    // Converted: every value kept, each cell carrying its scale, and the
    // database marked before the first such field was written.
    exec(&db, "ALTER TABLE t ALTER COLUMN u TYPE NUMERIC").await.unwrap();
    assert_eq!(slot(&db, "t", "u"), 17);
    assert_eq!(slot(&db, "t", "w"), 16);
    assert!(flagged(&dir));
    let after = column(&db, "t").await;
    assert_eq!(
        after.iter().map(|(id, v, _)| (id.clone(), v.clone())).collect::<Vec<_>>(),
        before.iter().map(|(id, v, _)| (id.clone(), v.clone())).collect::<Vec<_>>()
    );
    assert!(after.iter().all(|(_, _, w)| *w == 17), "{after:?}");
    exec(&db, "UPDATE t SET u = 2.0 / 3 WHERE id = 1").await.unwrap();
    exec(&db, "INSERT INTO t VALUES (5, 0.0000000001, 0)").await.unwrap();

    // Any rewrite converts: an ADD COLUMN on a table with rows recreates it.
    exec(&db, "ALTER TABLE t2 ADD COLUMN x BIGINT DEFAULT 7").await.unwrap();
    assert_eq!(slot(&db, "t2", "u"), 17);
    assert_eq!(column(&db, "t2").await.iter().map(|(_, v, _)| v.as_str()).collect::<Vec<_>>(), ["2.5"]);

    // And it survives a reopen, as does a new column's own slot.
    exec(&db, "CREATE TABLE t3 (id BIGINT PRIMARY KEY, u NUMERIC)").await.unwrap();
    close(&dir, db);
    let db = open(&dir, "own_scale").await.unwrap();
    assert_eq!((slot(&db, "t", "u"), slot(&db, "t2", "u"), slot(&db, "t3", "u")), (17, 17, 17));
    let got: Vec<String> = column(&db, "t").await.into_iter().map(|(_, v, _)| v).collect();
    assert_eq!(got, ["0.66666666666666666667", "0.000000001", "0", "123456.789", "0.0000000001"]);
    close(&dir, db);
}

/// A float an old column cannot keep at its nine places, or one past the
/// digits it holds, is refused on every path in, with the old column's hint,
/// and nothing is stored: INSERT VALUES (a literal and an expression),
/// INSERT … SELECT, UPDATE SET (a literal, a subquery, a column of UPDATE …
/// FROM). It was stored as 0. One within nine places is stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_old_column_refuses_a_float_it_cannot_keep() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir, "own_scale_float").await.unwrap();
    db.legacy_numeric_slot.store(true, Ordering::Relaxed);
    exec(&db, "CREATE TABLE t (id BIGINT PRIMARY KEY, u NUMERIC)").await.unwrap();
    db.legacy_numeric_slot.store(false, Ordering::Relaxed);
    assert_eq!(slot(&db, "t", "u"), 16);
    exec(&db, "CREATE TABLE fs (id BIGINT PRIMARY KEY, f DOUBLE PRECISION)").await.unwrap();
    exec(&db, "INSERT INTO fs VALUES (1, 1e-300), (2, 1e-39), (3, 0.5), (4, 1e40)").await.unwrap();
    exec(&db, "INSERT INTO t VALUES (1, 3), (2, 3)").await.unwrap();
    for sql in [
        "INSERT INTO t VALUES (10, '1e-300'::float8)",
        "INSERT INTO t VALUES (11, '1e-20'::float8 * '1e-20'::float8)",
        "INSERT INTO t SELECT id + 20, f FROM fs WHERE id = 2",
        "UPDATE t SET u = '1e-200'::float8 WHERE id = 1",
        "UPDATE t SET u = (SELECT f FROM fs WHERE id = 1) WHERE id = 1",
        "UPDATE t SET u = fs.f FROM fs WHERE fs.id = 2 AND t.id = 2",
    ] {
        let e = exec(&db, sql).await.expect_err(sql);
        assert!(e.contains("ALTER COLUMN") && e.contains("TYPE NUMERIC"), "{sql}: {e}");
    }
    for sql in ["INSERT INTO t VALUES (12, '1e40'::float8)", "UPDATE t SET u = fs.f FROM fs WHERE fs.id = 4 AND t.id = 2"] {
        let e = exec(&db, sql).await.expect_err(sql);
        assert!(e.contains("numeric field overflow") || e.contains("overflow"), "{sql}: {e}");
    }
    exec(&db, "UPDATE t SET u = fs.f FROM fs WHERE fs.id = 3 AND t.id = 2").await.unwrap();
    let got: Vec<(Vec<u8>, String, usize)> = column(&db, "t").await;
    assert_eq!(got.iter().map(|(_, v, w)| (v.as_str(), *w)).collect::<Vec<_>>(), [("3", 16), ("0.5", 16)]);
    assert!(!flagged(&dir));
    close(&dir, db);
}

/// `ALTER COLUMN u TYPE NUMERIC(p, s)` over a seventeen-byte column converts
/// every value to sixteen bytes at `s` places and records the declaration
/// with them: it is enforced, it survives a reopen, and a later rewrite
/// (ADD COLUMN) and `TYPE NUMERIC` back both work. The declaration used to
/// be recorded over the old slot, never enforced, and every later rewrite
/// failed with "holds 17 bytes but its type needs 16".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_typed_retype_of_an_own_scale_column_converts_it() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir, "own_scale_retype").await.unwrap();
    exec(&db, "CREATE TABLE t (id BIGINT PRIMARY KEY, u NUMERIC)").await.unwrap();
    exec(&db, "INSERT INTO t VALUES (1, 1.005), (2, 123.4), (3, NULL), (4, -2.675)").await.unwrap();
    assert_eq!(slot(&db, "t", "u"), 17);
    exec(&db, "ALTER TABLE t ALTER COLUMN u TYPE NUMERIC(5,2)").await.unwrap();
    assert_eq!(slot(&db, "t", "u"), 16);
    let e = exec(&db, "INSERT INTO t VALUES (5, 1000)").await.expect_err("past the precision");
    assert!(e.contains("numeric field overflow"), "{e}");
    close(&dir, db);
    let db = open(&dir, "own_scale_retype").await.unwrap();
    assert_eq!(slot(&db, "t", "u"), 16);
    // `rows` gives them sorted by their bytes.
    let shown = |r: Vec<Vec<Vec<u8>>>| -> Vec<String> { r.into_iter().map(|r| String::from_utf8_lossy(&r[0]).into_owned()).collect() };
    assert_eq!(shown(rows(&db, "SELECT u::text FROM t ORDER BY id").await.unwrap()), ["", "-2.68", "1.01", "123.40"]);
    exec(&db, "ALTER TABLE t ADD COLUMN x BIGINT DEFAULT 7").await.unwrap();
    assert_eq!(slot(&db, "t", "u"), 16);
    exec(&db, "ALTER TABLE t ALTER COLUMN u TYPE NUMERIC").await.unwrap();
    assert_eq!(slot(&db, "t", "u"), 17);
    exec(&db, "INSERT INTO t VALUES (6, 0.0000000000001, 0)").await.unwrap();
    close(&dir, db);
    let db = open(&dir, "own_scale_retype").await.unwrap();
    assert_eq!(
        shown(rows(&db, "SELECT u::text FROM t ORDER BY id").await.unwrap()),
        ["", "-2.68", "0.0000000000001", "1.01", "123.40"]
    );
    close(&dir, db);
}

/// The width of `column` in any relation, a partitioned one's shell too.
fn any_slot(db: &Arc<Database>, table: &str, column: &str) -> u32 {
    db.relation(table)
        .expect("relation")
        .schema()
        .fields
        .iter()
        .find(|f| f.name == column && !f.is_deleted)
        .expect("column")
        .size
}

/// A directory holding a seventeen-byte NUMERIC field without its fence (a
/// point-in-time restore brings the field records and not the flag) is
/// fenced when the table is opened, before anything else touches it, for a
/// plain table and a partitioned one, and for a dropped such column, which
/// keeps its slot in the row layout an older binary would misread.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_fences_a_numeric_column_found_without_its_flag() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    for (table, create) in [
        ("t", "CREATE TABLE t (id BIGINT PRIMARY KEY, u NUMERIC)"),
        ("p", "CREATE TABLE p (id BIGINT NOT NULL, u NUMERIC) PARTITION BY HASH (id) PARTITIONS 2"),
    ] {
        let _ = std::fs::remove_dir_all(&dir);
        let db = open(&dir, "own_scale_open_fence").await.unwrap();
        exec(&db, create).await.unwrap();
        exec(&db, &format!("INSERT INTO {table} (id, u) VALUES (1, 0.0000000000001)")).await.unwrap();
        assert_eq!(any_slot(&db, table, "u"), 17);
        close(&dir, db);
        assert!(flagged(&dir));
        std::fs::remove_file(dir.join(crate::core::database::incompat::FILE_NAME)).unwrap();
        assert!(!flagged(&dir));
        let db = open(&dir, "own_scale_open_fence").await.unwrap();
        assert!(flagged(&dir), "{table}: opened without fencing its seventeen-byte column");
        let got = rows(&db, &format!("SELECT u::text FROM {table}")).await.unwrap();
        assert_eq!(got, [[b"0.0000000000001".to_vec()]], "{table}");
        close(&dir, db);
    }
    let _ = std::fs::remove_dir_all(&dir);
    let db = open(&dir, "own_scale_open_fence").await.unwrap();
    exec(&db, "CREATE TABLE d (id BIGINT PRIMARY KEY, u NUMERIC, w BIGINT)").await.unwrap();
    exec(&db, "INSERT INTO d (id, u, w) VALUES (1, 0.0000000000001, 5)").await.unwrap();
    exec(&db, "ALTER TABLE d DROP COLUMN u").await.unwrap();
    close(&dir, db);
    std::fs::remove_file(dir.join(crate::core::database::incompat::FILE_NAME)).unwrap();
    let db = open(&dir, "own_scale_open_fence").await.unwrap();
    assert!(flagged(&dir), "opened without fencing a dropped seventeen-byte column");
    assert_eq!(rows(&db, "SELECT w FROM d").await.unwrap(), [[5i64.to_le_bytes().to_vec()]]);
    close(&dir, db);
}

/// A partitioned table made with the old slot (the test switch reaches its
/// CREATE too): sixteen bytes at nine places, the old refusal with its hint,
/// and no fence, across a reopen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_old_partitioned_column_keeps_its_slot_and_refusal() {
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir, "own_scale_partitioned").await.unwrap();
    db.legacy_numeric_slot.store(true, Ordering::Relaxed);
    exec(&db, "CREATE TABLE p (id BIGINT NOT NULL, u NUMERIC) PARTITION BY HASH (id) PARTITIONS 2").await.unwrap();
    db.legacy_numeric_slot.store(false, Ordering::Relaxed);
    assert_eq!(any_slot(&db, "p", "u"), 16);
    assert!(!flagged(&dir));
    exec(&db, "INSERT INTO p (id, u) VALUES (1, 1.5), (2, 0.000000001), (3, 2.0 / 3)").await.expect_err("past nine places");
    exec(&db, "INSERT INTO p (id, u) VALUES (1, 1.5), (2, 0.000000001)").await.unwrap();
    let e = exec(&db, "UPDATE p SET u = 0.0000000001 WHERE id = 1").await.expect_err("past nine places");
    assert!(e.contains("ALTER TABLE … ALTER COLUMN \"u\" TYPE NUMERIC"), "{e}");
    close(&dir, db);
    let db = open(&dir, "own_scale_partitioned").await.unwrap();
    assert_eq!(any_slot(&db, "p", "u"), 16);
    assert!(!flagged(&dir));
    let got: Vec<String> = rows(&db, "SELECT u::text FROM p").await.unwrap().into_iter().map(|r| String::from_utf8_lossy(&r[0]).into_owned()).collect();
    assert_eq!(got, ["0.000000001", "1.5"]);
    close(&dir, db);
}

/// A restored directory is fenced database by database: a nested `CREATE
/// DATABASE` directory whose table has a seventeen-byte field gets the flag
/// itself, and the root, whose tables have none, does not. It used to be
/// written at the root only, so the nested database got its own when this
/// binary first opened it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restore_fences_each_database_that_needs_it() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("db");
    let nested = root.join("nx");
    let db = open(&root, "own_scale_nested_root").await.unwrap();
    exec(&db, "CREATE TABLE r (id BIGINT PRIMARY KEY, p NUMERIC(10,2))").await.unwrap();
    close(&root, db);
    let db = open(&nested, "own_scale_nested").await.unwrap();
    exec(&db, "CREATE TABLE t (id BIGINT PRIMARY KEY, u NUMERIC)").await.unwrap();
    close(&nested, db);
    assert!(!flagged(&root) && flagged(&nested));
    std::fs::remove_file(nested.join(crate::core::database::incompat::FILE_NAME)).unwrap();
    crate::core::database::incompat::fence_restored_directory(&root).unwrap();
    assert!(flagged(&nested), "the nested database was not fenced");
    assert!(!flagged(&root), "the root, with no seventeen-byte field, was fenced");
}

/// The rewrite's own points, in the order it reaches them.
const POINTS: &[&str] = &[
    "test.rewrite.after_backup",
    "test.rewrite.after_discard",
    "test.rewrite.after_numeric_fence",
    "test.rewrite.after_create",
    "test.rewrite.mid_fill",
    "test.rewrite.after_fill",
    "test.rewrite.after_sync",
    "test.rewrite.after_commit",
];

/// `ALTER TABLE … ALTER COLUMN u TYPE NUMERIC` over an old column with
/// committed, updated and deleted rows, cut at each of the rewrite's points
/// and imaged under every seed kind. Each image must open and hold exactly
/// the old rows' values: in the old slot (every cell sixteen bytes) or in the
/// new one (every cell seventeen), never a table missing rows.
///
/// Mutation: `rewrite_payload` copies the old sixteen bytes into the new
/// slot unconverted; the write fails and the ALTER with it, so no image is
/// new.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_conversion_cut_anywhere_leaves_the_old_column_or_the_new() {
    let seeds = env_count("RDB_POWER_LOSS_SEEDS", 5, 1024);
    let base = tempfile::tempdir().unwrap();
    let dir = base.path().join("db");
    let db = open(&dir, "own_scale_cut").await.unwrap();
    db.legacy_numeric_slot.store(true, Ordering::Relaxed);
    exec(&db, "CREATE TABLE rw (id BIGINT PRIMARY KEY, u NUMERIC, n BIGINT)").await.unwrap();
    let values: Vec<String> = (0..40).map(|i| format!("({i}, {}.{:03}, {})", i * 7, i * 13 % 1000, i % 5)).collect();
    exec(&db, &format!("INSERT INTO rw (id, u, n) VALUES {}", values.join(", "))).await.unwrap();
    exec(&db, "DELETE FROM rw WHERE id >= 35").await.unwrap();
    exec(&db, "UPDATE rw SET u = 0.000000001 WHERE id < 5").await.unwrap();
    db.legacy_numeric_slot.store(false, Ordering::Relaxed);
    db.sync_durable().unwrap();
    let old: Vec<(Vec<u8>, String, usize)> = column(&db, "rw").await;
    assert_eq!(old.len(), 35);
    assert!(old.iter().all(|(_, _, w)| *w == 16));
    let want: Vec<(Vec<u8>, String)> = old.iter().map(|(id, v, _)| (id.clone(), v.clone())).collect();

    let rec = Recorder::start(&dir);
    let sink: StdArc<Mutex<Vec<Result<CrashPoint, String>>>> = StdArc::new(Mutex::new(Vec::new()));
    let mut guards = Vec::new();
    for point in POINTS {
        let (rec, sink, label) = (rec.clone(), sink.clone(), point.to_string());
        let action: failpoint::Action = StdArc::new(move |_hit| {
            sink.lock().push(rec.cut_quiescent(label.clone()));
            failpoint::Outcome::Continue
        });
        guards.push(failpoint::arm(point, &dir, 1, action));
    }
    exec(&db, "ALTER TABLE rw ALTER COLUMN u TYPE NUMERIC").await.unwrap();
    for g in &guards {
        assert_eq!(g.fired(), 1, "a cut point was never reached");
    }
    drop(guards);
    let new: Vec<(Vec<u8>, String, usize)> = column(&db, "rw").await;
    assert!(new.iter().all(|(_, _, w)| *w == 17), "{new:?}");
    close(&dir, db);
    drop(rec);

    let points: Vec<CrashPoint> = sink
        .lock()
        .drain(..)
        .filter_map(|p| p.map_err(|why| eprintln!("{why}")).ok())
        .collect();
    assert!(points.len() >= POINTS.len(), "too many cuts were disturbed: {}", points.len());
    let img = base.path().join("image");
    let (mut olds, mut news) = (0, 0);
    let mut problems = Vec::new();
    for point in &points {
        for seed in 0..seeds {
            point.write_image(seed, &img);
            let label = format!("{} seed {seed}", point.label);
            // Read before the open, which fences a directory missing the flag
            // (`incompat::fence_numeric_own_scale`) and would hide one the
            // conversion left unfenced.
            let fenced = flagged(&img);
            // The point sits after the fence and before the first field
            // record that needs it: the flag is durable there already.
            if point.label.ends_with("after_numeric_fence") && !fenced {
                problems.push(format!("{label}: the fence is not durable before the field records"));
            }
            match open(&img, "own_scale_cut").await {
                Err(e) => problems.push(format!("{label}: does not open: {e}")),
                Ok(db) => {
                    let got = column(&db, "rw").await;
                    let values: Vec<(Vec<u8>, String)> = got.iter().map(|(id, v, _)| (id.clone(), v.clone())).collect();
                    let widths: std::collections::BTreeSet<usize> = got.iter().map(|(_, _, w)| *w).collect();
                    if values != want {
                        problems.push(format!("{label}: {} rows, not the table's {}, or other values", got.len(), want.len()));
                    } else if widths == [16].into() && slot(&db, "rw", "u") == 16 {
                        olds += 1;
                    } else if widths == [17].into() && slot(&db, "rw", "u") == 17 && !fenced {
                        problems.push(format!("{label}: a seventeen-byte column in a directory without {}", crate::core::database::incompat::NUMERIC_OWN_SCALE_V1));
                    } else if widths == [17].into() && slot(&db, "rw", "u") == 17 {
                        news += 1;
                    } else {
                        problems.push(format!("{label}: cells of widths {widths:?} in a {}-byte slot", slot(&db, "rw", "u")));
                    }
                    close(&img, db);
                }
            }
            let _ = std::fs::remove_dir_all(&img);
        }
    }
    assert!(problems.is_empty(), "{} problems:\n{}", problems.len(), problems.join("\n"));
    assert!(olds > 0 && news > 0, "every image fell on one side (old {olds}, new {news}); the cuts test nothing");
    eprintln!("conversion cut: {} points, {olds} old images, {news} new images", points.len());
}
